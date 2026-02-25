//go:build linux

package iouring

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"time"
	"unsafe"

	"github.com/godzie44/go-uring/uring"
)

// IORING_OP_SEND_ZC opcode (Linux 6.0+).
// Not yet in the godzie44/go-uring constants, so we define it here.
const opSendZC uring.OpCode = 63 // IORING_OP_SEND_ZC

// IORING_SEND_ZC_REPORT_USAGE flag — request notification when kernel is done
// with the buffer (so we can safely reuse it).
const iosqeSendZCReportUsage uint32 = 1 << 0

// sendZCOp is an IORING_OP_SEND_ZC submission queue entry.
// The kernel references the user buffer directly instead of copying it into
// the socket send buffer, eliminating the user→kernel copy on send.
type sendZCOp struct {
	fd       int32
	buf      []byte
	msgFlags uint32
}

func (op *sendZCOp) PrepSQE(sqe *uring.SQEntry) {
	sqe.OpCode = uint8(opSendZC)
	sqe.Flags = 0
	sqe.IoPrio = 0
	sqe.Fd = op.fd
	sqe.Off = 0
	if len(op.buf) > 0 {
		sqe.Addr = uint64(uintptr(unsafe.Pointer(&op.buf[0])))
	} else {
		sqe.Addr = 0
	}
	sqe.Len = uint32(len(op.buf))
	sqe.OpcodeFlags = op.msgFlags
	sqe.UserData = 0
}

func (op *sendZCOp) Code() uring.OpCode { return opSendZC }

// sendOp is a standard IORING_OP_SEND (fallback for kernels < 6.0).
// Still async and batched via io_uring, but involves one kernel copy.
type sendOp struct {
	fd       int32
	buf      []byte
	msgFlags uint32
}

func (op *sendOp) PrepSQE(sqe *uring.SQEntry) {
	sqe.OpCode = uint8(uring.SendCode)
	sqe.Flags = 0
	sqe.IoPrio = 0
	sqe.Fd = op.fd
	sqe.Off = 0
	if len(op.buf) > 0 {
		sqe.Addr = uint64(uintptr(unsafe.Pointer(&op.buf[0])))
	} else {
		sqe.Addr = 0
	}
	sqe.Len = uint32(len(op.buf))
	sqe.OpcodeFlags = op.msgFlags
	sqe.UserData = 0
}

func (op *sendOp) Code() uring.OpCode { return uring.SendCode }

// ZeroCopyConn wraps a net.TCPConn and replaces its Write path with
// io_uring-based sends. On Linux 6.0+, SEND_ZC is used (zero kernel copy).
// On Linux 5.7–5.x, standard IORING_OP_SEND is used (async, one copy).
// On older kernels or when io_uring is unavailable, falls back to net.Conn.Write.
//
// Read operations always delegate to the underlying net.Conn (the Go runtime
// netpoller handles reads efficiently via epoll already).
type ZeroCopyConn struct {
	inner    net.Conn
	loop     *EventLoop
	features *KernelFeatures
	fd       int32
	enabled  bool
}

// NewZeroCopyConn wraps conn with io_uring-based writes.
// loop must be a started EventLoop. conn must be a *net.TCPConn.
func NewZeroCopyConn(conn net.Conn, loop *EventLoop) (*ZeroCopyConn, error) {
	features := loop.Features()

	if !features.NetworkSupport {
		// Kernel < 5.7 — no network io_uring support
		return &ZeroCopyConn{inner: conn, features: features, enabled: false}, nil
	}

	// Extract the raw file descriptor from the TCPConn.
	// We use SyscallConn to get the fd without duplicating it.
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return &ZeroCopyConn{inner: conn, features: features, enabled: false}, nil
	}
	rawConn, err := sc.SyscallConn()
	if err != nil {
		return &ZeroCopyConn{inner: conn, features: features, enabled: false}, nil
	}

	var fd int32
	var ctrlErr error
	ctrlErr = rawConn.Control(func(f uintptr) {
		fd = int32(f)
	})
	if ctrlErr != nil || fd <= 0 {
		return &ZeroCopyConn{inner: conn, features: features, enabled: false}, nil
	}

	return &ZeroCopyConn{
		inner:    conn,
		loop:     loop,
		features: features,
		fd:       fd,
		enabled:  true,
	}, nil
}

// Write sends b over the connection using io_uring.
//
// On Linux 6.0+: IORING_OP_SEND_ZC — kernel references b directly, no copy.
//   Note: b must remain valid until the kernel signals completion. Since we
//   wait for the CQE before returning, this is always safe.
//
// On Linux 5.7–5.x: IORING_OP_SEND — async send with one kernel copy.
//
// Fallback: net.Conn.Write (standard epoll path).
func (zc *ZeroCopyConn) Write(b []byte) (int, error) {
	if !zc.enabled || len(b) == 0 {
		return zc.inner.Write(b)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return zc.WriteContext(ctx, b)
}

// WriteContext sends b with a context for cancellation/timeout.
func (zc *ZeroCopyConn) WriteContext(ctx context.Context, b []byte) (int, error) {
	if !zc.enabled || len(b) == 0 {
		return zc.inner.Write(b)
	}

	var op uring.Operation
	if zc.features.SendZCSupport {
		op = &sendZCOp{fd: zc.fd, buf: b, msgFlags: 0}
	} else {
		op = &sendOp{fd: zc.fd, buf: b, msgFlags: 0}
	}

	result, err := zc.loop.SubmitAndWait(ctx, op, 0)
	if err != nil {
		// Fallback to standard write on io_uring error
		return zc.inner.Write(b)
	}
	if result.Err != nil {
		return 0, result.Err
	}
	if result.N < 0 {
		return 0, fmt.Errorf("send: errno %d", -result.N)
	}

	return int(result.N), nil
}

// Read delegates to the underlying net.Conn (handled by Go's epoll netpoller).
func (zc *ZeroCopyConn) Read(b []byte) (int, error) {
	return zc.inner.Read(b)
}

// Close closes the underlying connection.
func (zc *ZeroCopyConn) Close() error {
	return zc.inner.Close()
}

// LocalAddr returns the local network address.
func (zc *ZeroCopyConn) LocalAddr() net.Addr {
	return zc.inner.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (zc *ZeroCopyConn) RemoteAddr() net.Addr {
	return zc.inner.RemoteAddr()
}

// SetDeadline sets the read and write deadlines.
func (zc *ZeroCopyConn) SetDeadline(t time.Time) error {
	return zc.inner.SetDeadline(t)
}

// SetReadDeadline sets the read deadline.
func (zc *ZeroCopyConn) SetReadDeadline(t time.Time) error {
	return zc.inner.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (zc *ZeroCopyConn) SetWriteDeadline(t time.Time) error {
	return zc.inner.SetWriteDeadline(t)
}

// Enabled returns true if io_uring sends are active.
func (zc *ZeroCopyConn) Enabled() bool { return zc.enabled }

// UsesSendZC returns true if IORING_OP_SEND_ZC (Linux 6.0+) is being used.
func (zc *ZeroCopyConn) UsesSendZC() bool {
	return zc.enabled && zc.features.SendZCSupport
}

// Unwrap returns the underlying net.Conn.
func (zc *ZeroCopyConn) Unwrap() net.Conn { return zc.inner }

// Ensure ZeroCopyConn satisfies net.Conn.
var _ net.Conn = (*ZeroCopyConn)(nil)