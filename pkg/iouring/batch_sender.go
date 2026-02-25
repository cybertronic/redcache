//go:build linux

package iouring

import (
	"context"
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/godzie44/go-uring/uring"
)

// ShardSend describes a single shard to be sent to a remote node.
type ShardSend struct {
	// Conn is the TCP connection to the target node.
	// Must implement syscall.Conn for fd extraction.
	Conn net.Conn

	// Data is the shard payload to send.
	Data []byte

	// NodeID is used for error reporting only.
	NodeID string
}

// ShardResult is the result of sending a single shard.
type ShardResult struct {
	NodeID string
	N      int
	Err    error
}

// BatchSender sends multiple shards (e.g. erasure-coded fragments) to
// different nodes using io_uring linked SQEs, so all sends are submitted
// in a single syscall.
//
// On Linux 6.0+: uses IORING_OP_SEND_ZC (zero-copy send).
// On Linux 5.7–5.x: uses IORING_OP_SEND (async, one kernel copy).
// Fallback: concurrent goroutine-based sends via net.Conn.Write.
type BatchSender struct {
	loop     *EventLoop
	features *KernelFeatures
	enabled  bool
}

// NewBatchSender creates a BatchSender backed by the given EventLoop.
func NewBatchSender(loop *EventLoop) *BatchSender {
	features := loop.Features()
	return &BatchSender{
		loop:     loop,
		features: features,
		enabled:  features.NetworkSupport && features.LinkedOps,
	}
}

// SendShards sends all shards concurrently. When io_uring linked ops are
// available, all sends are submitted in a single io_uring_enter() syscall.
// Results are returned in the same order as shards.
func (bs *BatchSender) SendShards(ctx context.Context, shards []ShardSend) []ShardResult {
	if len(shards) == 0 {
		return nil
	}

	if !bs.enabled {
		return bs.sendFallback(ctx, shards)
	}

	return bs.sendLinked(ctx, shards)
}

// sendLinked submits all shard sends as linked SQEs — a single io_uring_enter()
// syscall covers all of them. The kernel executes them in order; if one fails,
// subsequent linked ops receive -ECANCELED.
//
// IMPORTANT: Linked SQE chains require ALL entries to be submitted via io_uring.
// If any shard cannot be submitted (e.g. fd extraction fails), the entire batch
// falls back to the goroutine-based path to avoid corrupting the chain.
func (bs *BatchSender) sendLinked(ctx context.Context, shards []ShardSend) []ShardResult {
	// Phase 1: Extract all file descriptors up front. If any fail, fall back
	// the entire batch to avoid a broken linked SQE chain.
	type preparedShard struct {
		fd   int32
		data []byte
	}
	prepared := make([]preparedShard, len(shards))
	for i, shard := range shards {
		if len(shard.Data) == 0 {
			prepared[i] = preparedShard{fd: -1, data: nil}
			continue
		}
		fd, err := extractFD(shard.Conn)
		if err != nil {
			// Can't extract fd for this shard — fall back entire batch
			return bs.sendFallback(ctx, shards)
		}
		prepared[i] = preparedShard{fd: fd, data: shard.Data}
	}

	// Phase 2: Submit all SQEs as a linked chain
	results := make([]ShardResult, len(shards))
	resultChans := make([]<-chan IOResult, len(shards))

	for i, p := range prepared {
		if p.data == nil {
			results[i] = ShardResult{NodeID: shards[i].NodeID, N: 0}
			continue
		}

		var op uring.Operation
		if bs.features.SendZCSupport {
			op = &sendZCOp{fd: p.fd, buf: p.data, msgFlags: 0}
		} else {
			op = &sendOp{fd: p.fd, buf: p.data, msgFlags: 0}
		}

		// Use SqeIOLinkFlag on all but the last non-empty shard so the kernel
		// submits them as a linked chain — one syscall for all.
		flags := uint8(0)
		if i < len(shards)-1 {
			flags = uring.SqeIOLinkFlag
		}

		ch, err := bs.loop.Submit(op, flags)
		if err != nil {
			// SQ overflow — fall back entire remaining batch
			return bs.sendFallback(ctx, shards)
		}
		resultChans[i] = ch
	}

	// Phase 3: Collect completions
	for i, ch := range resultChans {
		if ch == nil {
			continue // empty shard, already handled
		}
		select {
		case r := <-ch:
			if r.Err != nil {
				results[i] = ShardResult{NodeID: shards[i].NodeID, Err: r.Err}
			} else if r.N < 0 {
				results[i] = ShardResult{
					NodeID: shards[i].NodeID,
					Err:    fmt.Errorf("send shard %d: errno %d", i, -r.N),
				}
			} else {
				results[i] = ShardResult{NodeID: shards[i].NodeID, N: int(r.N)}
			}
		case <-ctx.Done():
			results[i] = ShardResult{NodeID: shards[i].NodeID, Err: ctx.Err()}
		}
	}

	return results
}

// sendFallback sends all shards concurrently using standard net.Conn.Write.
// Used when io_uring linked ops are not available.
func (bs *BatchSender) sendFallback(ctx context.Context, shards []ShardSend) []ShardResult {
	results := make([]ShardResult, len(shards))
	var wg sync.WaitGroup

	for i, shard := range shards {
		wg.Add(1)
		go func(idx int, s ShardSend) {
			defer wg.Done()

			// Use a channel to receive the write result, avoiding a data race
			// between the writer goroutine and the context cancellation path.
			type writeResult struct {
				n   int
				err error
			}
			done := make(chan writeResult, 1)
			go func() {
				n, err := s.Conn.Write(s.Data)
				done <- writeResult{n: n, err: err}
			}()

			select {
			case wr := <-done:
				results[idx] = ShardResult{NodeID: s.NodeID, N: wr.n, Err: wr.err}
			case <-ctx.Done():
				results[idx] = ShardResult{NodeID: s.NodeID, Err: ctx.Err()}
			}
		}(i, shard)
	}

	wg.Wait()
	return results
}

// SendShardsWithTimeout is a convenience wrapper that applies a per-operation timeout.
func (bs *BatchSender) SendShardsWithTimeout(
	shards []ShardSend,
	timeout time.Duration,
) []ShardResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return bs.SendShards(ctx, shards)
}

// Enabled returns true if io_uring linked sends are active.
func (bs *BatchSender) Enabled() bool { return bs.enabled }

// UsesSendZC returns true if IORING_OP_SEND_ZC is being used.
func (bs *BatchSender) UsesSendZC() bool {
	return bs.enabled && bs.features.SendZCSupport
}

// extractFD extracts the raw file descriptor from a net.Conn.
// Returns an error if the conn does not implement syscall.Conn.
func extractFD(conn net.Conn) (int32, error) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return 0, fmt.Errorf("conn does not implement syscall.Conn")
	}
	rawConn, err := sc.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("SyscallConn: %w", err)
	}

	var fd int32
	if err := rawConn.Control(func(f uintptr) {
		fd = int32(f)
	}); err != nil {
		return 0, fmt.Errorf("Control: %w", err)
	}
	if fd <= 0 {
		return 0, fmt.Errorf("invalid fd: %d", fd)
	}
	return fd, nil
}

// sendZCBatch is a lower-level helper that submits a batch of SEND_ZC ops
// for callers that have already extracted file descriptors.
// Returns a slice of result channels in the same order as fds/bufs.
func (bs *BatchSender) sendZCBatch(
	ctx context.Context,
	fds []int32,
	bufs [][]byte,
) ([]<-chan IOResult, error) {
	if len(fds) != len(bufs) {
		return nil, fmt.Errorf("fds and bufs length mismatch")
	}

	channels := make([]<-chan IOResult, len(fds))

	for i := range fds {
		if len(bufs[i]) == 0 {
			// Empty buffer — synthesize an immediate success
			ch := make(chan IOResult, 1)
			ch <- IOResult{N: 0}
			channels[i] = ch
			continue
		}

		var op uring.Operation
		if bs.features.SendZCSupport {
			op = &sendZCOp{
				fd:       fds[i],
				buf:      bufs[i],
				msgFlags: 0,
			}
		} else {
			op = &sendOp{
				fd:       fds[i],
				buf:      bufs[i],
				msgFlags: 0,
			}
		}

		flags := uint8(0)
		if i < len(fds)-1 {
			flags = uring.SqeIOLinkFlag
		}

		ch, err := bs.loop.Submit(op, flags)
		if err != nil {
			return nil, fmt.Errorf("submit shard %d: %w", i, err)
		}
		channels[i] = ch
	}

	return channels, nil
}

// unsafeSlicePtr returns the pointer to the first element of a byte slice.
// Used for SQE addr field population.
func unsafeSlicePtr(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	return uint64(uintptr(unsafe.Pointer(&b[0])))
}