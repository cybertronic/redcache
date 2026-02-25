package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"redcache/pkg/dedup"
	"redcache/pkg/distributed"
	"redcache/pkg/monitoring"
	"redcache/pkg/persistence"
	"redcache/pkg/prefetch"
	"redcache/pkg/storage"
)

// Manager coordinates all replication activities
type Manager struct {
	config         Config
	db             *storage.Database
	hybridStore    *storage.HybridReplicator
	memberlist     *memberlist.Memberlist
	
	// Persistent components
	lockManager    *persistence.PersistentLockManager
	deduplicator   *persistence.PersistentDeduplicator
	predictor      *persistence.PersistentPredictor
	analytics      *persistence.PersistentAnalyticsEngine
	
	// Replication state
	vectorClocks   map[string]*distributed.VectorClock
	clockMu        sync.RWMutex
	
	// Metrics
	metrics        *ReplicationMetrics
	
	// Lifecycle
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

// Config configures the replication manager
type Config struct {
	NodeID            string
	DataDir           string
	
	// Storage config
	StorageConfig     storage.DatabaseConfig
	
	// Raft config (for coordination)
	RaftConfig        storage.RaftConfig
	
	// Gossip config (for cache data)
	GossipConfig      storage.GossipConfig
	
	// Memberlist config
	MemberlistConfig  *memberlist.Config
	
	// Component configs
	LockConfig        distributed.LockConfig
	ClusterConfig     *distributed.ClusterConfig
}

// NewManager creates a new replication manager
func NewManager(config Config) (*Manager, error) {
	// Initialize database
	db, err := storage.NewDatabase(config.StorageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}
	
	// Initialize memberlist for cluster membership
	ml, err := memberlist.Create(config.MemberlistConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}
	
	// Initialize hybrid replicator (Raft + Gossip)
	hybridConfig := storage.HybridConfig{
		NodeID:             config.NodeID,
		RaftConfig:         config.RaftConfig,
		GossipConfig:       config.GossipConfig,
		CoordinationPrefix: "coord/",
		CachePrefix:        "cache/",
	}
	
	hybridStore, err := storage.NewHybridReplicator(hybridConfig, db, ml)
	if err != nil {
		return nil, fmt.Errorf("failed to create hybrid replicator: %w", err)
	}
	
	// Initialize persistent components
	lockManager, err := persistence.NewPersistentLockManager(config.LockConfig, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create lock manager: %w", err)
	}
	
	// Create cluster manager for health monitoring
	
	
	m := &Manager{
		config:        config,
		db:            db,
		hybridStore:   hybridStore,
		memberlist:    ml,
		lockManager:   lockManager,
		vectorClocks:  make(map[string]*distributed.VectorClock),
		metrics:       NewReplicationMetrics(),
		stopCh:        make(chan struct{}),
	}
	
	// Start background workers
	m.wg.Add(1)
	go m.syncWorker()
	
	m.wg.Add(1)
	go m.metricsWorker()
	
	return m, nil
}

// JoinCluster joins the memberlist cluster by connecting to the given peer addresses
func (m *Manager) JoinCluster(peers []string) (int, error) {
	return m.memberlist.Join(peers)
}

// AddRaftVoter adds a node as a Raft voter (must be called on the leader)
func (m *Manager) AddRaftVoter(nodeID, raftAddr string) error {
	return m.hybridStore.AddVoter(nodeID, raftAddr)
}

// GetRaftBind returns the Raft bind address for this node
func (m *Manager) GetRaftBind() string {
	return m.config.RaftConfig.RaftBind
}

// InitializeDeduplicator initializes the persistent deduplicator.
// FIX #29: previously always returned "not implemented".
// config must be a dedup.DeduplicationConfig value or pointer.
func (m *Manager) InitializeDeduplicator(config any) error {
	var cfg dedup.DeduplicationConfig
	switch v := config.(type) {
	case dedup.DeduplicationConfig:
		cfg = v
	case *dedup.DeduplicationConfig:
		if v == nil {
			return fmt.Errorf("InitializeDeduplicator: nil config")
		}
		cfg = *v
	default:
		return fmt.Errorf("InitializeDeduplicator: expected dedup.DeduplicationConfig, got %T", config)
	}

	pd, err := persistence.NewPersistentDeduplicator(cfg, m.db)
	if err != nil {
		return fmt.Errorf("failed to create persistent deduplicator: %w", err)
	}
	m.deduplicator = pd
	return nil
}

// InitializePredictor initializes the persistent ML predictor.
// FIX #29: previously always returned "not implemented".
// config must be a prefetch.PredictorConfig value or pointer.
func (m *Manager) InitializePredictor(config any) error {
	var cfg prefetch.PredictorConfig
	switch v := config.(type) {
	case prefetch.PredictorConfig:
		cfg = v
	case *prefetch.PredictorConfig:
		if v == nil {
			return fmt.Errorf("InitializePredictor: nil config")
		}
		cfg = *v
	default:
		return fmt.Errorf("InitializePredictor: expected prefetch.PredictorConfig, got %T", config)
	}

	pp, err := persistence.NewPersistentPredictor(cfg, m.db)
	if err != nil {
		return fmt.Errorf("failed to create persistent predictor: %w", err)
	}
	m.predictor = pp
	return nil
}

// InitializeAnalytics initializes the persistent analytics engine.
// FIX #29: previously always returned "not implemented".
// config must be a monitoring.AnalyticsConfig value or pointer.
func (m *Manager) InitializeAnalytics(config any) error {
	var cfg monitoring.AnalyticsConfig
	switch v := config.(type) {
	case monitoring.AnalyticsConfig:
		cfg = v
	case *monitoring.AnalyticsConfig:
		if v == nil {
			return fmt.Errorf("InitializeAnalytics: nil config")
		}
		cfg = *v
	default:
		return fmt.Errorf("InitializeAnalytics: expected monitoring.AnalyticsConfig, got %T", config)
	}

	pae, err := persistence.NewPersistentAnalyticsEngine(cfg, m.db)
	if err != nil {
		return fmt.Errorf("failed to create persistent analytics engine: %w", err)
	}
	m.analytics = pae
	return nil
}
func (m *Manager) SetCoordination(ctx context.Context, key string, value []byte) error {
	// Attach vector clock
	vc := m.getOrCreateVectorClock(key)
	vc.Increment(m.config.NodeID)
	
	// Store vector clock
	if err := m.storeVectorClock(key, vc); err != nil {
		return err
	}
	
	// Replicate via Raft
	err := m.hybridStore.SetCoordination(ctx, key, value)
	if err == nil {
		m.metrics.IncrementCoordinationWrites()
	}
	
	return err
}

// GetCoordination retrieves coordination data
func (m *Manager) GetCoordination(ctx context.Context, key string) ([]byte, error) {
	value, err := m.hybridStore.GetCoordination(ctx, key)
	if err == nil {
		m.metrics.IncrementCoordinationReads()
	}
	return value, err
}

// SetCache stores cache data with eventual consistency (Gossip)
func (m *Manager) SetCache(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Attach vector clock
	vc := m.getOrCreateVectorClock(key)
	vc.Increment(m.config.NodeID)
	
	// Store vector clock
	if err := m.storeVectorClock(key, vc); err != nil {
		return err
	}
	
	// Replicate via Gossip
	err := m.hybridStore.SetCache(ctx, key, value, ttl)
	if err == nil {
		m.metrics.IncrementCacheWrites()
	}
	
	return err
}

// GetCache retrieves cache data
func (m *Manager) GetCache(ctx context.Context, key string) ([]byte, error) {
	value, err := m.hybridStore.GetCache(ctx, key)
	if err == nil {
		m.metrics.IncrementCacheReads()
	}
	return value, err
}

// AcquireLock acquires a distributed lock
func (m *Manager) AcquireLock(ctx context.Context, key string, lockType distributed.LockType) error {
	err := m.lockManager.Lock(ctx, key, lockType)
	if err == nil {
		m.metrics.IncrementLocksAcquired()
	}
	return err
}

// ReleaseLock releases a distributed lock
func (m *Manager) ReleaseLock(ctx context.Context, key string) error {
	err := m.lockManager.Unlock(ctx, key)
	if err == nil {
		m.metrics.IncrementLocksReleased()
	}
	return err
}

// getOrCreateVectorClock gets or creates a vector clock for a key
func (m *Manager) getOrCreateVectorClock(key string) *distributed.VectorClock {
	m.clockMu.Lock()
	defer m.clockMu.Unlock()
	
	vc, exists := m.vectorClocks[key]
	if !exists {
		vc = distributed.NewVectorClock()
		m.vectorClocks[key] = vc
	}
	
	return vc
}

// storeVectorClock stores a vector clock in the database
func (m *Manager) storeVectorClock(key string, vc *distributed.VectorClock) error {
	clockKey := []byte("vclock/" + key)
	clockData := vc.ToMap()
	return m.db.SetJSON(clockKey, clockData)
}

// loadVectorClock loads a vector clock from the database
func (m *Manager) loadVectorClock(key string) (*distributed.VectorClock, error) {
	clockKey := []byte("vclock/" + key)
	var clockData map[string]int64
	err := m.db.GetJSON(clockKey, &clockData)
	if err != nil {
		return distributed.NewVectorClock(), nil
	}
	return distributed.FromMap(clockData), nil
}

// syncWorker periodically syncs state across nodes
func (m *Manager) syncWorker() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.performSync()
		}
	}
}

// performSync performs a sync operation
func (m *Manager) performSync() {
// }, nil
	m.clockMu.RLock()
	clockCount := len(m.vectorClocks)
	m.clockMu.RUnlock()
	
	m.metrics.SetVectorClocks(int64(clockCount))
	
	// Sync cluster state
	members := m.memberlist.Members()
	m.metrics.SetClusterSize(int64(len(members)))
}

// metricsWorker periodically updates metrics
func (m *Manager) metricsWorker() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.updateMetrics()
		}
	}
}

// updateMetrics updates replication metrics
func (m *Manager) updateMetrics() {
	// Update hybrid store stats
	stats := m.hybridStore.Stats()
	
	_ = stats  // Unused for now
	dbStats := m.db.Stats()
	m.metrics.SetDatabaseSize(dbStats.TotalSize)
	
	// Update lock stats
	if m.lockManager != nil {
		lockStats := m.lockManager.GetStats()
		_ = lockStats  // map[string]any, skip for now
	}
	
	// Update deduplication stats
	if m.deduplicator != nil {
		dedupStats := m.deduplicator.GetStats()
		_ = dedupStats  // map[string]any, skip for now
	}
}

// IsLeader returns true if this node is the Raft leader
func (m *Manager) IsLeader() bool {
	return m.hybridStore.IsLeader()
}

// Leader returns the current Raft leader address
func (m *Manager) Leader() string {
	return m.hybridStore.Leader()
}

// GetClusterMembers returns the list of cluster members
func (m *Manager) GetClusterMembers() []*memberlist.Node {
	return m.memberlist.Members()
}

// GetStats returns replication statistics
func (m *Manager) GetStats() ManagerStats {
	stats := ManagerStats{
		NodeID:         m.config.NodeID,
		LockStats:      make(map[string]any),
		DedupStats:     make(map[string]any),
		PredictorStats: make(map[string]any),
	}

	if m.lockManager != nil {
		stats.LockStats = m.lockManager.GetStats()
	}

	if m.deduplicator != nil {
		stats.DedupStats = m.deduplicator.GetStats()
	}

	if m.predictor != nil {
		stats.PredictorStats = m.predictor.GetStats()
	}

	return stats
}
	

// Close shuts down the replication manager. Safe to call multiple times.
func (m *Manager) Close() error {
	select {
	case <-m.stopCh:
		// Already closed
		return nil
	default:
		close(m.stopCh)
	}
	m.wg.Wait()
	
	// Close components
	if m.lockManager != nil {
		m.lockManager.Close()
	}
	
	if m.deduplicator != nil {
		m.deduplicator.Close()
	}
	
	if m.predictor != nil {
		m.predictor.Close()
	}
	
	if m.analytics != nil {
		m.analytics.Close()
	}
	
	
	// Close hybrid store
	if m.hybridStore != nil {
		m.hybridStore.Close()
	}
	
	// Close database
	if m.db != nil {
		m.db.Close()
	}
	
	// Leave memberlist
	if m.memberlist != nil {
		m.memberlist.Leave(time.Second)
		m.memberlist.Shutdown()
	}
	
	return nil
}


// ManagerStats contains replication manager statistics
type ManagerStats struct {
	NodeID          string
	HybridStats     storage.HybridStats
	LockStats       map[string]any
	DedupStats      map[string]any
	PredictorStats  map[string]any
	AnalyticsStats  persistence.PersistentAnalyticsStats
}
// WaitForLeader waits for a Raft leader to be elected
func (m *Manager) WaitForLeader(timeout time.Duration) error {
	return m.hybridStore.WaitForLeader(timeout)
}

// Barrier ensures all pending operations are committed
func (m *Manager) Barrier(timeout time.Duration) error {
	return m.hybridStore.Barrier(timeout)
}

// Snapshot triggers a Raft snapshot
func (m *Manager) Snapshot() error {
	return m.hybridStore.Snapshot()
}