//go:build linux

package iouring

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"github.com/godzie44/go-uring/uring"
)

// IORING_OP_SPLICE is opcode 35.
const opSpliceCode = 35

// SpliceRequest defines the parameters for a zero-copy data transfer.
type SpliceRequest struct {
	InFd      int
	InOffset  int64
	OutFd     int
	OutOffset int64
	Length    uint32
	Flags     uint32
}

// spliceOp implements the uring.Operation interface for IORING_OP_SPLICE.
type spliceOp struct {
	req SpliceRequest
}

// Encode implements the uring.Operation interface for older go-uring versions.
func (s *spliceOp) Encode(sqe *uring.SQEntry) {
	s.PrepSQE(sqe)
}

// PrepSQE implements the uring.Operation interface for newer go-uring versions.
func (s *spliceOp) PrepSQE(sqe *uring.SQEntry) {
	sqe.OpCode = uint8(opSpliceCode)
	sqe.Fd = int32(s.req.InFd)
	sqe.Off = uint64(s.req.InOffset)
	sqe.Addr = uint64(s.req.OutFd)
	sqe.Len = s.req.Length
	// Splice flags are stored in the second union (addr2/off2) at offset 24.
	*(*int64)(unsafe.Pointer(uintptr(unsafe.Pointer(sqe)) + 24)) = s.req.OutOffset
}

// Code implements the uring.Operation interface.
func (s *spliceOp) Code() uring.OpCode {
	return uring.OpCode(opSpliceCode)
}

// OpCode implements the uring.Operation interface.
func (s *spliceOp) OpCode() uint8 {
	return uint8(opSpliceCode)
}

// Splice performs a zero-copy transfer using IORING_OP_SPLICE via the event loop.
func (m *Manager) Splice(req SpliceRequest) (<-chan IOResult, error) {
	if !m.Enabled() || m.loop == nil {
		return nil, fmt.Errorf("iouring: manager disabled or loop not started")
	}
	return m.loop.Submit(&spliceOp{req: req}, 0)
}

// Splicer provides a high-level API for zero-copy file-to-socket transfers.
type Splicer struct {
	mgr  *Manager
	pipe [2]int 
}

// NewSplicer initializes a new Splicer with the required kernel pipe.
func NewSplicer(mgr *Manager) (*Splicer, error) {
	var pipe [2]int
	if err := syscall.Pipe2(pipe[:], syscall.O_CLOEXEC|syscall.O_NONBLOCK); err != nil {
		return nil, fmt.Errorf("failed to create splice pipe: %w", err)
	}
	
	return &Splicer{
		mgr:  mgr,
		pipe: pipe,
	}, nil
}

// SendFileToSocket moves data from an open file to a network connection zero-copy.
func (s *Splicer) SendFileToSocket(file *os.File, fd int, offset int64, length uint32) error {
	res1, err := s.mgr.Splice(SpliceRequest{
		InFd:      int(file.Fd()),
		InOffset:  offset,
		OutFd:     s.pipe[1],
		OutOffset: -1,
		Length:    length,
		Flags:     0,
	})
	if err != nil {
		return err
	}
	
	r1 := <-res1
	if r1.Err != nil {
		return fmt.Errorf("splice file-to-pipe failed: %w", r1.Err)
	}

	res2, err := s.mgr.Splice(SpliceRequest{
		InFd:      s.pipe[0],
		InOffset:  -1,
		OutFd:     fd,
		OutOffset: -1,
		Length:    uint32(r1.N),
		Flags:     0,
	})
	if err != nil {
		return err
	}

	r2 := <-res2
	if r2.Err != nil {
		return fmt.Errorf("splice pipe-to-socket failed: %w", r2.Err)
	}

	return nil
}

// Close releases the kernel pipe resources.
func (s *Splicer) Close() error {
	e1 := syscall.Close(s.pipe[0])
	e2 := syscall.Close(s.pipe[1])
	if e1 != nil { return e1 }
	return e2
}
