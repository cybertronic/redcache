package replication

import (
	"sync"
	"sync/atomic"
)

// ReplicationMetrics tracks replication statistics
type ReplicationMetrics struct {
	// Coordination (Raft)
	coordinationReads  atomic.Int64
	coordinationWrites atomic.Int64
	
	// Cache (Gossip)
	cacheReads  atomic.Int64
	cacheWrites atomic.Int64
	
	// Locks
	locksAcquired atomic.Int64
	locksReleased atomic.Int64
	activeLocks   atomic.Int64
	
	// Cluster
	clusterSize   atomic.Int64
	vectorClocks  atomic.Int64
	
	// Database
	databaseSize atomic.Int64
	
	// Deduplication
	deduplicationRatio atomic.Uint64 // stored as float64 bits
	
	// Conflicts
	conflicts         atomic.Int64
	conflictsResolved atomic.Int64
	
	mu sync.RWMutex
}

// NewReplicationMetrics creates a new metrics tracker
func NewReplicationMetrics() *ReplicationMetrics {
	return &ReplicationMetrics{}
}

// IncrementCoordinationReads increments coordination reads
func (m *ReplicationMetrics) IncrementCoordinationReads() {
	m.coordinationReads.Add(1)
}

// IncrementCoordinationWrites increments coordination writes
func (m *ReplicationMetrics) IncrementCoordinationWrites() {
	m.coordinationWrites.Add(1)
}

// IncrementCacheReads increments cache reads
func (m *ReplicationMetrics) IncrementCacheReads() {
	m.cacheReads.Add(1)
}

// IncrementCacheWrites increments cache writes
func (m *ReplicationMetrics) IncrementCacheWrites() {
	m.cacheWrites.Add(1)
}

// IncrementLocksAcquired increments locks acquired
func (m *ReplicationMetrics) IncrementLocksAcquired() {
	m.locksAcquired.Add(1)
	m.activeLocks.Add(1)
}

// IncrementLocksReleased increments locks released
func (m *ReplicationMetrics) IncrementLocksReleased() {
	m.locksReleased.Add(1)
	m.activeLocks.Add(-1)
}

// SetActiveLocks sets the number of active locks
func (m *ReplicationMetrics) SetActiveLocks(count int64) {
	m.activeLocks.Store(count)
}

// SetClusterSize sets the cluster size
func (m *ReplicationMetrics) SetClusterSize(size int64) {
	m.clusterSize.Store(size)
}

// SetVectorClocks sets the number of vector clocks
func (m *ReplicationMetrics) SetVectorClocks(count int64) {
	m.vectorClocks.Store(count)
}

// SetDatabaseSize sets the database size
func (m *ReplicationMetrics) SetDatabaseSize(size int64) {
	m.databaseSize.Store(size)
}

// SetDeduplicationRatio sets the deduplication ratio
func (m *ReplicationMetrics) SetDeduplicationRatio(ratio float64) {
	m.deduplicationRatio.Store(uint64(ratio))
}

// IncrementConflicts increments conflict count
func (m *ReplicationMetrics) IncrementConflicts() {
	m.conflicts.Add(1)
}

// IncrementConflictsResolved increments resolved conflicts
func (m *ReplicationMetrics) IncrementConflictsResolved() {
	m.conflictsResolved.Add(1)
}

// GetSnapshot returns a snapshot of current metrics
func (m *ReplicationMetrics) GetSnapshot() MetricsSnapshot {
	return MetricsSnapshot{
		CoordinationReads:  m.coordinationReads.Load(),
		CoordinationWrites: m.coordinationWrites.Load(),
		CacheReads:         m.cacheReads.Load(),
		CacheWrites:        m.cacheWrites.Load(),
		LocksAcquired:      m.locksAcquired.Load(),
		LocksReleased:      m.locksReleased.Load(),
		ActiveLocks:        m.activeLocks.Load(),
		ClusterSize:        m.clusterSize.Load(),
		VectorClocks:       m.vectorClocks.Load(),
		DatabaseSize:       m.databaseSize.Load(),
		DeduplicationRatio: float64(m.deduplicationRatio.Load()),
		Conflicts:          m.conflicts.Load(),
		ConflictsResolved:  m.conflictsResolved.Load(),
	}
}

// MetricsSnapshot contains a snapshot of metrics
type MetricsSnapshot struct {
	CoordinationReads  int64
	CoordinationWrites int64
	CacheReads         int64
	CacheWrites        int64
	LocksAcquired      int64
	LocksReleased      int64
	ActiveLocks        int64
	ClusterSize        int64
	VectorClocks       int64
	DatabaseSize       int64
	DeduplicationRatio float64
	Conflicts          int64
	ConflictsResolved  int64
}

// Reset resets all metrics
func (m *ReplicationMetrics) Reset() {
	m.coordinationReads.Store(0)
	m.coordinationWrites.Store(0)
	m.cacheReads.Store(0)
	m.cacheWrites.Store(0)
	m.locksAcquired.Store(0)
	m.locksReleased.Store(0)
	m.activeLocks.Store(0)
	m.clusterSize.Store(0)
	m.vectorClocks.Store(0)
	m.databaseSize.Store(0)
	m.deduplicationRatio.Store(0)
	m.conflicts.Store(0)
	m.conflictsResolved.Store(0)
}