//go:build linux

package iouring

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// KernelFeatures holds the detected io_uring capabilities of the running kernel.
type KernelFeatures struct {
	// Major.Minor kernel version
	Major int
	Minor int

	// io_uring availability tiers
	Available        bool // >= 5.1: basic io_uring (file read/write)
	NetworkSupport   bool // >= 5.7: accept, connect, send, recv on sockets
	SendZCSupport    bool // >= 6.0: IORING_OP_SEND_ZC (zero-copy send)
	FixedBuffers     bool // >= 5.1: IORING_REGISTER_BUFFERS
	LinkedOps        bool // >= 5.3: SQE_IO_LINK (linked operations)
	SQPollSupport    bool // >= 5.11: SQPOLL without CAP_SYS_NICE
	TimeoutSupport   bool // >= 5.4: IORING_OP_TIMEOUT
	BatchCQE         bool // >= 5.18: io_uring_peek_batch_cqe
}

var (
	detectedFeatures *KernelFeatures
	detectOnce       sync.Once
)

// DetectFeatures returns the io_uring capabilities of the running kernel.
// Detection is performed once and cached.
func DetectFeatures() *KernelFeatures {
	detectOnce.Do(func() {
		detectedFeatures = detect()
	})
	return detectedFeatures
}

// detect performs the actual kernel version detection.
func detect() *KernelFeatures {
	f := &KernelFeatures{}

	var uname syscall.Utsname
	if err := syscall.Uname(&uname); err != nil {
		return f // all false — safe fallback
	}

	release := int8SliceToString(uname.Release[:])
	major, minor, err := parseKernelVersion(release)
	if err != nil {
		return f
	}

	f.Major = major
	f.Minor = minor

	// >= 5.1: basic io_uring
	if atLeast(major, minor, 5, 1) {
		f.Available = true
		f.FixedBuffers = true
	}
	// >= 5.3: linked ops
	if atLeast(major, minor, 5, 3) {
		f.LinkedOps = true
	}
	// >= 5.4: timeout
	if atLeast(major, minor, 5, 4) {
		f.TimeoutSupport = true
	}
	// >= 5.7: full network support
	if atLeast(major, minor, 5, 7) {
		f.NetworkSupport = true
	}
	// >= 5.11: SQPOLL without CAP_SYS_NICE
	if atLeast(major, minor, 5, 11) {
		f.SQPollSupport = true
	}
	// >= 5.18: batch CQE peek
	if atLeast(major, minor, 5, 18) {
		f.BatchCQE = true
	}
	// >= 6.0: SEND_ZC
	if atLeast(major, minor, 6, 0) {
		f.SendZCSupport = true
	}

	return f
}

// atLeast returns true if (major, minor) >= (wantMajor, wantMinor).
func atLeast(major, minor, wantMajor, wantMinor int) bool {
	if major > wantMajor {
		return true
	}
	if major == wantMajor && minor >= wantMinor {
		return true
	}
	return false
}

// parseKernelVersion parses "6.1.155-..." into (6, 1).
func parseKernelVersion(release string) (int, int, error) {
	// Strip everything after the first non-version character
	parts := strings.FieldsFunc(release, func(r rune) bool {
		return r == '-' || r == '+' || r == ' '
	})
	if len(parts) == 0 {
		return 0, 0, fmt.Errorf("empty release string")
	}

	nums := strings.Split(parts[0], ".")
	if len(nums) < 2 {
		return 0, 0, fmt.Errorf("cannot parse version from %q", parts[0])
	}

	major, err := strconv.Atoi(nums[0])
	if err != nil {
		return 0, 0, fmt.Errorf("bad major: %w", err)
	}
	minor, err := strconv.Atoi(nums[1])
	if err != nil {
		return 0, 0, fmt.Errorf("bad minor: %w", err)
	}

	return major, minor, nil
}

// int8SliceToString converts a [65]int8 utsname field to a Go string.
func int8SliceToString(s []int8) string {
	b := make([]byte, 0, len(s))
	for _, v := range s {
		if v == 0 {
			break
		}
		b = append(b, byte(v))
	}
	return string(b)
}

// String returns a human-readable summary of detected features.
func (f *KernelFeatures) String() string {
	return fmt.Sprintf(
		"kernel=%d.%d available=%v network=%v sendZC=%v fixedBufs=%v linkedOps=%v sqpoll=%v",
		f.Major, f.Minor,
		f.Available, f.NetworkSupport, f.SendZCSupport,
		f.FixedBuffers, f.LinkedOps, f.SQPollSupport,
	)
}