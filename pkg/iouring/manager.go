//go:build linux

package iouring

import (
	"fmt"
	"log"
	"net"
	"sync"
)

// Config holds all io_uring configuration options.
// All fields have safe defaults via DefaultConfig().
type Config struct {
	// Enabled controls whether io_uring is used at all.
	// If false, all operations fall back to standard syscalls.
	Enabled bool

	// QueueDepth is the io_uring SQ/CQ ring size (must be power of 2).
	// Default: 256
	QueueDepth uint32

	// NumFixedBuffers is the number of pre-registered (pinned) buffers.
	// Default: 64
	NumFixedBuffers int

	// FixedBufferSize is the size of each pre-registered buffer in bytes.
	// Default: 1MB
	FixedBufferSize int

	// EnableSendZC enables IORING_OP_SEND_ZC on Linux 6.0+.
	// Automatically disabled on older kernels.
	// Default: true
	EnableSendZC bool

	// EnableLinkedSends enables linked SQE batch sends for erasure coding.
	// Default: true
	EnableLinkedSends bool

	// EnableFileIO enables fixed-buffer file I/O for cache storage.
	// Default: true
	EnableFileIO bool

	// EnableNetworkIO enables io_uring-based network sends.
	// Default: true
	EnableNetworkIO bool
}

// DefaultConfig returns a Config with safe production defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:           true,
		QueueDepth:        256,
		NumFixedBuffers:   64,
		FixedBufferSize:   1 * 1024 * 1024, // 1MB
		EnableSendZC:      true,
		EnableLinkedSends: true,
		EnableFileIO:      true,
		EnableNetworkIO:   true,
	}
}

// Manager is the top-level io_uring subsystem for RedCache.
// It owns the EventLoop, FilePool, and BatchSender and exposes them
// through a single lifecycle-managed object.
//
// Usage:
//
//	mgr, err := iouring.NewManager(iouring.DefaultConfig())
//	if err != nil { ... }
//	defer mgr.Close()
//
//	// File I/O
//	data, err := mgr.FilePool().ReadFile(ctx, "/cache/key123")
//
//	// Batch shard send
//	results := mgr.BatchSender().SendShards(ctx, shards)
//
//	// Zero-copy conn
//	zcConn, err := mgr.WrapConn(tcpConn)
type Manager struct {
	config      Config
	features    *KernelFeatures
	loop        *EventLoop
	filePool    *FilePool
	batchSender *BatchSender
	metrics     *Metrics
	mu          sync.RWMutex
	closed      bool
}

// NewManager creates and starts the io_uring subsystem.
// If io_uring is unavailable or disabled, all components operate in
// transparent fallback mode — no errors are returned.
func NewManager(cfg Config) (*Manager, error) {
	features := DetectFeatures()

	m := &Manager{
		config:   cfg,
		features: features,
		metrics:  globalMetrics,
	}

	if !cfg.Enabled || !features.Available {
		log.Printf("[iouring] disabled or unavailable (%s) — using standard I/O", features)
		// Create fallback-mode components (no io_uring, no errors)
		m.filePool = &FilePool{features: features, enabled: false}
		return m, nil
	}

	log.Printf("[iouring] initialising — %s", features)

	// Create the shared event loop + pinned buffer pool
	if cfg.EnableFileIO || cfg.EnableNetworkIO {
		bufPool, err := NewPinnedBufferPool(cfg.NumFixedBuffers, cfg.FixedBufferSize)
		if err != nil {
			return nil, fmt.Errorf("iouring: pinned buffer pool: %w", err)
		}

		loop, err := NewEventLoop(cfg.QueueDepth, bufPool)
		if err != nil {
			bufPool.Close()
			return nil, fmt.Errorf("iouring: event loop: %w", err)
		}
		loop.Start()
		m.loop = loop

		// FilePool shares the same loop
		if cfg.EnableFileIO {
			m.filePool = &FilePool{
				loop:     loop,
				bufPool:  bufPool,
				features: features,
				enabled:  true,
			}
		} else {
			m.filePool = &FilePool{features: features, enabled: false}
		}

		// BatchSender shares the same loop
		if cfg.EnableNetworkIO && cfg.EnableLinkedSends {
			m.batchSender = NewBatchSender(loop)
		}
	}

	log.Printf("[iouring] ready — fileIO=%v networkIO=%v sendZC=%v linkedSends=%v",
		cfg.EnableFileIO && features.Available,
		cfg.EnableNetworkIO && features.NetworkSupport,
		cfg.EnableSendZC && features.SendZCSupport,
		cfg.EnableLinkedSends && features.LinkedOps,
	)

	return m, nil
}

// FilePool returns the fixed-buffer file I/O pool.
// Never nil — returns a fallback-mode pool if io_uring is unavailable.
func (m *Manager) FilePool() *FilePool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.filePool == nil {
		return &FilePool{features: m.features, enabled: false}
	}
	return m.filePool
}

// BatchSender returns the linked-SQE batch sender for erasure shard distribution.
// May be nil if network I/O is disabled or io_uring is unavailable.
func (m *Manager) BatchSender() *BatchSender {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.batchSender
}

// WrapConn wraps a net.Conn with io_uring-based zero-copy sends.
// Returns a ZeroCopyConn that uses SEND_ZC (Linux 6.0+) or IORING_OP_SEND
// (Linux 5.7+), falling back to standard net.Conn.Write on older kernels.
//
// If io_uring network support is unavailable or disabled, returns a
// ZeroCopyConn in fallback mode (all writes delegate to the inner conn).
func (m *Manager) WrapConn(conn net.Conn) (*ZeroCopyConn, error) {
	m.mu.RLock()
	loop := m.loop
	m.mu.RUnlock()

	if loop == nil || !m.config.EnableNetworkIO {
		// Return a disabled ZeroCopyConn — all writes go through net.Conn
		return &ZeroCopyConn{
			inner:    conn,
			features: m.features,
			enabled:  false,
		}, nil
	}

	return NewZeroCopyConn(conn, loop)
}

// Enabled returns true if io_uring is active (not in fallback mode).
func (m *Manager) Enabled() bool {
	return m.config.Enabled && m.features.Available
}

// Features returns the detected kernel feature set.
func (m *Manager) Features() *KernelFeatures { return m.features }

// Metrics returns the io_uring metrics instance.
func (m *Manager) Metrics() *Metrics { return m.metrics }

// Loop returns the underlying EventLoop. May be nil in fallback mode.
func (m *Manager) Loop() *EventLoop {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.loop
}

// Close stops the event loop and releases all resources.
// Safe to call multiple times.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}
	m.closed = true

	if m.loop != nil {
		m.loop.Stop()
		m.loop = nil
	}

	// Mark FilePool as disabled so callers fall back to standard I/O
	if m.filePool != nil {
		m.filePool.enabled = false
	}
	m.batchSender = nil

	log.Printf("[iouring] closed")
}