//go:build linux

package iouring

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
)

// PinnedBuffer is a Go heap buffer pinned via runtime.Pinner so the GC will
// not move it while the kernel holds a reference (required for io_uring fixed
// buffer registration).
type PinnedBuffer struct {
	data   []byte
	pinner runtime.Pinner
	index  int // index in the registered buffer table
	inUse  atomic.Bool
}

// Bytes returns the underlying byte slice.
func (b *PinnedBuffer) Bytes() []byte { return b.data }

// Index returns the buffer's index in the io_uring registered buffer table.
func (b *PinnedBuffer) Index() int { return b.index }

// Len returns the capacity of the buffer.
func (b *PinnedBuffer) Len() int { return len(b.data) }

// unpin releases the GC pin. Called when the buffer pool is closed.
func (b *PinnedBuffer) unpin() { b.pinner.Unpin() }

// PinnedBufferPool manages a fixed set of runtime.Pinner-pinned buffers and
// produces the []syscall.Iovec slice needed for io_uring buffer registration.
type PinnedBufferPool struct {
	buffers    []*PinnedBuffer
	free       chan *PinnedBuffer
	bufferSize int
	count      int
	mu         sync.Mutex
	closed     bool
}

// NewPinnedBufferPool allocates count buffers of bufferSize bytes each,
// pins them all, and returns the pool.
func NewPinnedBufferPool(count, bufferSize int) (*PinnedBufferPool, error) {
	if count <= 0 {
		return nil, fmt.Errorf("count must be > 0")
	}
	if bufferSize <= 0 {
		return nil, fmt.Errorf("bufferSize must be > 0")
	}

	p := &PinnedBufferPool{
		buffers:    make([]*PinnedBuffer, count),
		free:       make(chan *PinnedBuffer, count),
		bufferSize: bufferSize,
		count:      count,
	}

	for i := 0; i < count; i++ {
		buf := &PinnedBuffer{
			data:  make([]byte, bufferSize),
			index: i,
		}
		// Pin the first element of the slice — this prevents the GC from
		// moving the backing array while the kernel has a reference to it.
		buf.pinner.Pin(&buf.data[0])
		p.buffers[i] = buf
		p.free <- buf
	}

	return p, nil
}

// Iovec returns a []syscall.Iovec suitable for ring.RegisterBuffers().
// Each entry points to the pinned backing array of the corresponding buffer.
func (p *PinnedBufferPool) Iovec() []syscall.Iovec {
	vecs := make([]syscall.Iovec, p.count)
	for i, buf := range p.buffers {
		vecs[i] = syscall.Iovec{
			Base: &buf.data[0],
		}
		vecs[i].SetLen(p.bufferSize)
	}
	return vecs
}

// Acquire returns a free buffer from the pool, blocking until one is available.
// Returns nil if the pool is closed.
func (p *PinnedBufferPool) Acquire() *PinnedBuffer {
	buf, ok := <-p.free
	if !ok {
		return nil
	}
	buf.inUse.Store(true)
	return buf
}

// TryAcquire returns a free buffer without blocking. Returns nil if none available.
func (p *PinnedBufferPool) TryAcquire() *PinnedBuffer {
	select {
	case buf := <-p.free:
		buf.inUse.Store(true)
		return buf
	default:
		return nil
	}
}

// Release returns a buffer to the pool after use.
// Safe to call concurrently with Close — the mutex prevents send-on-closed-channel.
func (p *PinnedBufferPool) Release(buf *PinnedBuffer) {
	if buf == nil {
		return
	}
	buf.inUse.Store(false)
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.free <- buf
	}
}

// Close unpins all buffers and closes the pool. Must be called after
// io_uring buffer deregistration (ring.UnRegisterBuffers()).
func (p *PinnedBufferPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.free)
	for _, buf := range p.buffers {
		buf.unpin()
	}
}

// Count returns the total number of buffers in the pool.
func (p *PinnedBufferPool) Count() int { return p.count }

// BufferSize returns the size of each buffer in bytes.
func (p *PinnedBufferPool) BufferSize() int { return p.bufferSize }