//go:build linux

package iouring

import (
	"sync/atomic"
)

// Metrics tracks io_uring operation statistics.
// All fields are updated atomically and safe for concurrent access.
type Metrics struct {
	// File I/O
	FileReadsTotal      atomic.Int64
	FileWritesTotal     atomic.Int64
	FileReadBytesTotal  atomic.Int64
	FileWriteBytesTotal atomic.Int64
	FileReadFallbacks   atomic.Int64  // fell back to standard read
	FileWriteFallbacks  atomic.Int64  // fell back to standard write

	// Network sends
	SendsTotal       atomic.Int64
	SendZCTotal      atomic.Int64  // SEND_ZC operations (Linux 6.0+)
	SendBytesTotal   atomic.Int64
	SendFallbacks    atomic.Int64  // fell back to net.Conn.Write

	// Batch (linked SQE) sends
	BatchSendsTotal  atomic.Int64  // number of batch send calls
	ShardsTotal      atomic.Int64  // total shards sent across all batches
	ShardErrors      atomic.Int64

	// Event loop
	SQEsSubmitted    atomic.Int64
	CQEsHarvested    atomic.Int64
	SQOverflows      atomic.Int64
	LoopIterations   atomic.Int64
}

// Snapshot returns a point-in-time copy of all metrics as a map.
func (m *Metrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"iouring_file_reads_total":       m.FileReadsTotal.Load(),
		"iouring_file_writes_total":      m.FileWritesTotal.Load(),
		"iouring_file_read_bytes_total":  m.FileReadBytesTotal.Load(),
		"iouring_file_write_bytes_total": m.FileWriteBytesTotal.Load(),
		"iouring_file_read_fallbacks":    m.FileReadFallbacks.Load(),
		"iouring_file_write_fallbacks":   m.FileWriteFallbacks.Load(),
		"iouring_sends_total":            m.SendsTotal.Load(),
		"iouring_send_zc_total":          m.SendZCTotal.Load(),
		"iouring_send_bytes_total":       m.SendBytesTotal.Load(),
		"iouring_send_fallbacks":         m.SendFallbacks.Load(),
		"iouring_batch_sends_total":      m.BatchSendsTotal.Load(),
		"iouring_shards_total":           m.ShardsTotal.Load(),
		"iouring_shard_errors":           m.ShardErrors.Load(),
		"iouring_sqes_submitted":         m.SQEsSubmitted.Load(),
		"iouring_cqes_harvested":         m.CQEsHarvested.Load(),
		"iouring_sq_overflows":           m.SQOverflows.Load(),
		"iouring_loop_iterations":        m.LoopIterations.Load(),
	}
}

// globalMetrics is the package-level metrics instance.
var globalMetrics = &Metrics{}

// GlobalMetrics returns the package-level metrics instance.
func GlobalMetrics() *Metrics { return globalMetrics }