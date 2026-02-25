//go:build linux

package iouring

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/godzie44/go-uring/uring"
)

// IORequest is a pending I/O operation submitted to the event loop.
type IORequest struct {
	id      uint64
	resultC chan IOResult
}

// IOResult is the completion result of an I/O operation.
type IOResult struct {
	ID    uint64
	N     int32  // bytes transferred (CQE.Res)
	Err   error
}

// EventLoop runs a single io_uring ring on a dedicated OS thread.
// All SQE submissions and CQE harvesting happen on that thread.
// Callers interact via Submit() which returns a channel for the result.
type EventLoop struct {
	ring       *uring.Ring
	bufPool    *PinnedBufferPool
	features   *KernelFeatures

	// pending maps userData → result channel
	pending   map[uint64]chan IOResult
	pendingMu sync.Mutex

	// submission channel: callers send (op, resultChan) pairs
	submitCh  chan loopSubmission

	// lifecycle
	stopCh    chan struct{}
	stoppedCh chan struct{}
	nextID    atomic.Uint64
	started   atomic.Bool
}

type loopSubmission struct {
	op      uring.Operation
	flags   uint8
	resultC chan IOResult
}

// NewEventLoop creates an EventLoop with a new io_uring ring of the given
// queue depth. bufPool may be nil if fixed-buffer operations are not needed.
func NewEventLoop(queueDepth uint32, bufPool *PinnedBufferPool) (*EventLoop, error) {
	features := DetectFeatures()
	if !features.Available {
		return nil, fmt.Errorf("io_uring not available on kernel %d.%d (need >= 5.1)",
			features.Major, features.Minor)
	}

	ring, err := uring.New(queueDepth)
	if err != nil {
		return nil, fmt.Errorf("io_uring setup failed: %w", err)
	}

	// Register fixed buffers if a pool was provided
	if bufPool != nil {
		vecs := bufPool.Iovec()
		if err := ring.RegisterBuffers(vecs); err != nil {
			_ = ring.Close()
			return nil, fmt.Errorf("io_uring buffer registration failed: %w", err)
		}
	}

	el := &EventLoop{
		ring:      ring,
		bufPool:   bufPool,
		features:  features,
		pending:   make(map[uint64]chan IOResult),
		submitCh:  make(chan loopSubmission, int(queueDepth)),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}

	return el, nil
}

// Start launches the event loop goroutine pinned to an OS thread.
// Must be called before Submit().
func (el *EventLoop) Start() {
	if el.started.Swap(true) {
		return // already started
	}
	go el.run()
}

// Submit queues an operation and returns a channel that will receive exactly
// one IOResult when the kernel completes the operation.
func (el *EventLoop) Submit(op uring.Operation, flags uint8) (<-chan IOResult, error) {
	resultC := make(chan IOResult, 1)

	el.pendingMu.Lock()
	// Check if we are stopped while holding the lock to prevent racing with Stop()
	select {
	case <-el.stopCh:
		el.pendingMu.Unlock()
		return nil, fmt.Errorf("event loop stopped")
	default:
		// We are safe to proceed; the lock ensures Stop() hasn't closed stopCh yet.
	}
	el.pendingMu.Unlock()

	select {
	case el.submitCh <- loopSubmission{op: op, flags: flags, resultC: resultC}:
		return resultC, nil
	case <-el.stopCh:
		return nil, fmt.Errorf("event loop stopped")
	}
}

// SubmitAndWait submits an operation and blocks until it completes or ctx is done.
func (el *EventLoop) SubmitAndWait(ctx context.Context, op uring.Operation, flags uint8) (IOResult, error) {
	ch, err := el.Submit(op, flags)
	if err != nil {
		return IOResult{}, err
	}
	select {
	case result := <-ch:
		return result, result.Err
	case <-ctx.Done():
		return IOResult{}, ctx.Err()
	}
}

// Stop signals the event loop to drain and exit. Blocks until fully stopped.
func (el *EventLoop) Stop() {
	if !el.started.Load() {
		return
	}

	el.pendingMu.Lock()
	select {
	case <-el.stopCh:
		el.pendingMu.Unlock()
	default:
		close(el.stopCh)
		el.pendingMu.Unlock()
	}
	<-el.stoppedCh
}

// BufPool returns the pinned buffer pool associated with this event loop.
func (el *EventLoop) BufPool() *PinnedBufferPool { return el.bufPool }

// Features returns the detected kernel feature set.
func (el *EventLoop) Features() *KernelFeatures { return el.features }

// run is the event loop body. It MUST run on a dedicated OS thread because
// io_uring file descriptors are per-thread in terms of SQ/CQ ownership.
func (el *EventLoop) run() {
	// Pin this goroutine to its OS thread for the lifetime of the loop.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(el.stoppedCh)
	defer el.cleanup()

	const pollTimeout = 100 * time.Microsecond
	const batchSize = 32

	cqeBatch := make([]*uring.CQEvent, batchSize)

	for {
		// Drain the submission channel and queue SQEs
		drained := el.drainSubmissions()

		// If we queued anything, submit to the kernel
		if drained > 0 {
			if _, err := el.ring.Submit(); err != nil {
				log.Printf("[iouring] Submit error: %v", err)
			}
		}

		// Harvest completions (non-blocking peek)
		n := el.ring.PeekCQEventBatch(cqeBatch)
		for i := 0; i < n; i++ {
			cqe := cqeBatch[i]
			el.dispatchCQE(cqe)
		}
		if n > 0 {
			el.ring.AdvanceCQ(uint32(n))
		}

		// Check for stop signal
		select {
		case <-el.stopCh:
			// Drain remaining submissions before exiting
			el.drainSubmissions()
			if _, err := el.ring.Submit(); err != nil {
				log.Printf("[iouring] Final submit error: %v", err)
			}
			// Wait for all pending completions with a timeout
			el.drainPending(500 * time.Millisecond)
			return
		default:
		}

		// If nothing to do, wait briefly for a CQE to avoid busy-spinning
		if drained == 0 && n == 0 {
			cqe, err := el.ring.WaitCQEventsWithTimeout(1, pollTimeout)
			if err == nil && cqe != nil {
				el.dispatchCQE(cqe)
				el.ring.SeenCQE(cqe)
			}
		}
	}
}

// drainSubmissions reads all pending submissions from submitCh and queues them
// as SQEs. Returns the number of SQEs queued.
func (el *EventLoop) drainSubmissions() int {
	count := 0
	for {
		select {
		case sub := <-el.submitCh:
			id := el.nextID.Add(1)
			if err := el.ring.QueueSQE(sub.op, sub.flags, id); err != nil {
				// SQ overflow — send error back immediately
				sub.resultC <- IOResult{ID: id, Err: fmt.Errorf("SQ overflow: %w", err)}
				continue
			}
			el.pendingMu.Lock()
			el.pending[id] = sub.resultC
			el.pendingMu.Unlock()
			count++
		default:
			return count
		}
	}
}

// dispatchCQE sends the completion result to the waiting caller.
func (el *EventLoop) dispatchCQE(cqe *uring.CQEvent) {
	el.pendingMu.Lock()
	resultC, ok := el.pending[cqe.UserData]
	if ok {
		delete(el.pending, cqe.UserData)
	}
	el.pendingMu.Unlock()

	if !ok {
		return // spurious CQE (e.g. timeout internal)
	}

	result := IOResult{ID: cqe.UserData, N: cqe.Res}
	if cqe.Res < 0 {
		result.Err = fmt.Errorf("io_uring op failed: errno %d", -cqe.Res)
	}
	resultC <- result
}

// drainPending waits for all in-flight operations to complete, up to timeout.
func (el *EventLoop) drainPending(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	cqeBatch := make([]*uring.CQEvent, 32)

	for time.Now().Before(deadline) {
		el.pendingMu.Lock()
		remaining := len(el.pending)
		el.pendingMu.Unlock()
		if remaining == 0 {
			return
		}

		cqe, err := el.ring.WaitCQEventsWithTimeout(1, 10*time.Millisecond)
		if err == nil && cqe != nil {
			el.dispatchCQE(cqe)
			el.ring.SeenCQE(cqe)
		}

		n := el.ring.PeekCQEventBatch(cqeBatch)
		for i := 0; i < n; i++ {
			el.dispatchCQE(cqeBatch[i])
		}
		if n > 0 {
			el.ring.AdvanceCQ(uint32(n))
		}
	}

	// Cancel any remaining pending with a timeout error
	el.pendingMu.Lock()
	for id, ch := range el.pending {
		ch <- IOResult{ID: id, Err: fmt.Errorf("event loop stopped before completion")}
		delete(el.pending, id)
	}
	el.pendingMu.Unlock()
}

// cleanup deregisters buffers and closes the ring.
func (el *EventLoop) cleanup() {
	if el.bufPool != nil {
		_ = el.ring.UnRegisterBuffers()
		el.bufPool.Close()
	}
	if err := el.ring.Close(); err != nil {
		log.Printf("[iouring] ring close error: %v", err)
	}
}