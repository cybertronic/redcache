//go:build linux

package iouring

import (
	"context"
	"fmt"
	"os"
	"unsafe"

	"github.com/godzie44/go-uring/uring"
)

// readFixedOp is an IORING_OP_READ_FIXED operation.
// It reads from a file descriptor directly into a pre-registered (pinned) buffer,
// eliminating the kernel→user copy that a normal read() would incur.
type readFixedOp struct {
	fd     int32
	buf    []byte
	offset uint64
	bufIdx uint16
}

func (op *readFixedOp) PrepSQE(sqe *uring.SQEntry) {
	sqe.OpCode = uint8(5) // IORING_OP_READ_FIXED = 5 (opReadFixed)
	sqe.Flags = 0
	sqe.IoPrio = 0
	sqe.Fd = op.fd
	sqe.Off = op.offset
	if len(op.buf) > 0 {
		sqe.Addr = uint64(uintptr(unsafe.Pointer(&op.buf[0])))
	} else {
		sqe.Addr = 0
	}
	sqe.Len = uint32(len(op.buf))
	sqe.OpcodeFlags = 0
	sqe.BufIG = op.bufIdx
	sqe.UserData = 0
}

func (op *readFixedOp) Code() uring.OpCode { return uring.OpCode(5) }

// writeFixedOp is an IORING_OP_WRITE_FIXED operation.
// It writes from a pre-registered (pinned) buffer directly to a file descriptor,
// eliminating the user→kernel copy that a normal write() would incur.
type writeFixedOp struct {
	fd     int32
	buf    []byte
	offset uint64
	bufIdx uint16
}

func (op *writeFixedOp) PrepSQE(sqe *uring.SQEntry) {
	sqe.OpCode = uint8(6) // IORING_OP_WRITE_FIXED = 6 (opWriteFixed)
	sqe.Flags = 0
	sqe.IoPrio = 0
	sqe.Fd = op.fd
	sqe.Off = op.offset
	if len(op.buf) > 0 {
		sqe.Addr = uint64(uintptr(unsafe.Pointer(&op.buf[0])))
	} else {
		sqe.Addr = 0
	}
	sqe.Len = uint32(len(op.buf))
	sqe.OpcodeFlags = 0
	sqe.BufIG = op.bufIdx
	sqe.UserData = 0
}

func (op *writeFixedOp) Code() uring.OpCode { return uring.OpCode(6) }

// FilePool provides zero-copy file I/O using io_uring fixed buffers.
// It wraps an EventLoop and a PinnedBufferPool to offer ReadFixed and WriteFixed
// operations that bypass the kernel↔user copy path.
//
// Falls back to standard os.File.ReadAt / WriteAt when io_uring is unavailable.
type FilePool struct {
	loop     *EventLoop
	bufPool  *PinnedBufferPool
	features *KernelFeatures
	enabled  bool
}

// NewFilePool creates a FilePool backed by an io_uring EventLoop.
// queueDepth controls the SQ/CQ ring size; numBuffers and bufferSize control
// the pre-registered buffer pool.
//
// If io_uring is unavailable (kernel < 5.1), the pool is created in fallback
// mode and all operations transparently use standard syscalls.
func NewFilePool(queueDepth uint32, numBuffers, bufferSize int) (*FilePool, error) {
	features := DetectFeatures()

	if !features.Available {
		// Graceful fallback — no io_uring
		return &FilePool{features: features, enabled: false}, nil
	}

	bufPool, err := NewPinnedBufferPool(numBuffers, bufferSize)
	if err != nil {
		return nil, fmt.Errorf("pinned buffer pool: %w", err)
	}

	loop, err := NewEventLoop(queueDepth, bufPool)
	if err != nil {
		bufPool.Close()
		return nil, fmt.Errorf("event loop: %w", err)
	}

	loop.Start()

	return &FilePool{
		loop:     loop,
		bufPool:  bufPool,
		features: features,
		enabled:  true,
	}, nil
}

// ReadAt reads len(dst) bytes from f starting at offset into dst.
//
// When io_uring is available:
//   - Acquires a pinned buffer from the pool
//   - Submits IORING_OP_READ_FIXED (zero kernel→user copy)
//   - Copies result from pinned buffer into dst
//   - Returns the pinned buffer to the pool
//
// When io_uring is unavailable, falls back to f.ReadAt().
func (fp *FilePool) ReadAt(ctx context.Context, f *os.File, dst []byte, offset int64) (int, error) {
	if !fp.enabled {
		return f.ReadAt(dst, offset)
	}

	// If the request is larger than a single buffer, fall back to standard I/O
	if len(dst) > fp.bufPool.BufferSize() {
		return f.ReadAt(dst, offset)
	}

	buf := fp.bufPool.Acquire()
	if buf == nil {
		return f.ReadAt(dst, offset) // pool closed, fallback
	}
	defer fp.bufPool.Release(buf)

	// Slice the pinned buffer to exactly the requested size
	pinnedSlice := buf.Bytes()[:len(dst)]

	op := &readFixedOp{
		fd:     int32(f.Fd()),
		buf:    pinnedSlice,
		offset: uint64(offset),
		bufIdx: uint16(buf.Index()),
	}

	result, err := fp.loop.SubmitAndWait(ctx, op, 0)
	if err != nil {
		// Fallback on io_uring error
		return f.ReadAt(dst, offset)
	}
	if result.Err != nil {
		return 0, result.Err
	}
	if result.N < 0 {
		return 0, fmt.Errorf("read_fixed: errno %d", -result.N)
	}

	n := int(result.N)
	copy(dst, pinnedSlice[:n])
	return n, nil
}

// WriteAt writes len(src) bytes from src to f starting at offset.
//
// When io_uring is available:
//   - Acquires a pinned buffer from the pool
//   - Copies src into the pinned buffer (one copy, unavoidable for GC safety)
//   - Submits IORING_OP_WRITE_FIXED (zero user→kernel copy after this point)
//   - Returns the pinned buffer to the pool
//
// When io_uring is unavailable, falls back to f.WriteAt().
func (fp *FilePool) WriteAt(ctx context.Context, f *os.File, src []byte, offset int64) (int, error) {
	if !fp.enabled {
		return f.WriteAt(src, offset)
	}

	if len(src) > fp.bufPool.BufferSize() {
		return f.WriteAt(src, offset)
	}

	buf := fp.bufPool.Acquire()
	if buf == nil {
		return f.WriteAt(src, offset)
	}
	defer fp.bufPool.Release(buf)

	// Copy src into the pinned buffer — this is the single unavoidable copy.
	// After this, the kernel reads directly from the pinned buffer (no further copy).
	pinnedSlice := buf.Bytes()[:len(src)]
	copy(pinnedSlice, src)

	op := &writeFixedOp{
		fd:     int32(f.Fd()),
		buf:    pinnedSlice,
		offset: uint64(offset),
		bufIdx: uint16(buf.Index()),
	}

	result, err := fp.loop.SubmitAndWait(ctx, op, 0)
	if err != nil {
		return f.WriteAt(src, offset)
	}
	if result.Err != nil {
		return 0, result.Err
	}
	if result.N < 0 {
		return 0, fmt.Errorf("write_fixed: errno %d", -result.N)
	}

	return int(result.N), nil
}

// ReadAtLarge reads large files by splitting into buffer-sized chunks.
// Each chunk uses a fixed-buffer read; chunks are assembled into dst.
func (fp *FilePool) ReadAtLarge(ctx context.Context, f *os.File, dst []byte, offset int64) (int, error) {
	if !fp.enabled || len(dst) <= fp.bufPool.BufferSize() {
		return fp.ReadAt(ctx, f, dst, offset)
	}

	total := 0
	chunkSize := fp.bufPool.BufferSize()

	for total < len(dst) {
		end := total + chunkSize
		if end > len(dst) {
			end = len(dst)
		}
		chunk := dst[total:end]
		n, err := fp.ReadAt(ctx, f, chunk, offset+int64(total))
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			break
		}
	}
	return total, nil
}

// WriteAtLarge writes large buffers by splitting into buffer-sized chunks.
func (fp *FilePool) WriteAtLarge(ctx context.Context, f *os.File, src []byte, offset int64) (int, error) {
	if !fp.enabled || len(src) <= fp.bufPool.BufferSize() {
		return fp.WriteAt(ctx, f, src, offset)
	}

	total := 0
	chunkSize := fp.bufPool.BufferSize()

	for total < len(src) {
		end := total + chunkSize
		if end > len(src) {
			end = len(src)
		}
		chunk := src[total:end]
		n, err := fp.WriteAt(ctx, f, chunk, offset+int64(total))
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// ReadFile reads an entire file using fixed-buffer reads.
func (fp *FilePool) ReadFile(ctx context.Context, path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	if size == 0 {
		GlobalMetrics().FileReadsTotal.Add(1)
		return []byte{}, nil
	}

	dst := make([]byte, size)
	n, err := fp.ReadAtLarge(ctx, f, dst, 0)
	if err != nil {
		GlobalMetrics().FileReadFallbacks.Add(1)
		return nil, err
	}
	GlobalMetrics().FileReadsTotal.Add(1)
	GlobalMetrics().FileReadBytesTotal.Add(int64(n))
	return dst[:n], nil
}

// WriteFile writes data to a file using fixed-buffer writes.
func (fp *FilePool) WriteFile(ctx context.Context, path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer f.Close()

	n, err := fp.WriteAtLarge(ctx, f, data, 0)
	if err != nil {
		GlobalMetrics().FileWriteFallbacks.Add(1)
		return err
	}
	GlobalMetrics().FileWritesTotal.Add(1)
	GlobalMetrics().FileWriteBytesTotal.Add(int64(n))
	return nil
}

// Enabled returns true if io_uring is active (not in fallback mode).
func (fp *FilePool) Enabled() bool { return fp.enabled }

// Features returns the detected kernel feature set.
func (fp *FilePool) Features() *KernelFeatures { return fp.features }

// Close stops the event loop and releases all resources.
func (fp *FilePool) Close() {
	if fp.loop != nil {
		fp.loop.Stop()
	}
}
