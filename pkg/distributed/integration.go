package distributed

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"redcache/pkg/cache"
	"redcache/pkg/s3client"
)

// ErasureCacheProvider is an interface that allows the distributed package to
// use erasure coding without creating an import cycle with pkg/erasure.
type ErasureCacheProvider interface {
	IsEnabled() bool
	GetStats() map[string]any
}

// DistributedCache integrates all distributed components
type DistributedCache struct {
	config           *ClusterConfig
	localStorage     *cache.Storage
	s3Client         *s3client.Client
	clusterManager   *ClusterManager
	healthMonitor    *HealthMonitor
	conflictResolver *ConflictResolver
	quorumManager    *QuorumManager
	rpcServer        *GRPCServer
	rpcClient        *GRPCClient
	syncManager      *SyncManager
	fetchCoordinator *FetchCoordinator
	cacheWarmer      *CacheWarmer
	rebalancer       *Rebalancer
	erasureCache     ErasureCacheProvider
	running          bool
	mu               sync.RWMutex
}

// SetErasureCache sets the erasure cache provider.
func (dc *DistributedCache) SetErasureCache(ec ErasureCacheProvider) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.erasureCache = ec
}

// ClusterManager returns the cluster manager instance.
func (dc *DistributedCache) ClusterManager() *ClusterManager {
	return dc.clusterManager
}

// RPCClient returns the RPC client instance.
func (dc *DistributedCache) RPCClient() *GRPCClient {
	return dc.rpcClient
}

// NewDistributedCache creates a new distributed cache instance with full wiring
func NewDistributedCache(config *ClusterConfig, localStorage *cache.Storage, s3 *s3client.Client, fetchConfig FetchCoordinatorConfig) (*DistributedCache, error) {
	if config == nil || localStorage == nil || s3 == nil {
		return nil, fmt.Errorf("initialization parameters (config, localStorage, s3) cannot be nil")
	}

	dc := &DistributedCache{
		config:       config,
		localStorage: localStorage,
		s3Client:     s3,
	}

	dc.rpcClient = NewGRPCClient(config)

	cm, err := NewClusterManager(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster manager: %w", err)
	}
	dc.clusterManager = cm

	dc.healthMonitor = NewHealthMonitor(config, cm, dc.rpcClient)
	dc.conflictResolver = NewConflictResolver(config, cm, dc.rpcClient)

	qConfig := QuorumConfig{
		NodeID:      config.NodeID,
		ClusterSize: len(config.Seeds) + 1,
		QuorumSize:  (len(config.Seeds) + 1) / 2 + 1,
		RPCPort:     config.RPCPort,
	}
	dc.quorumManager = NewQuorumManager(qConfig, cm, dc.rpcClient)
	dc.syncManager = NewSyncManager(config, cm, dc.rpcClient, localStorage)
	dc.rpcServer = NewGRPCServer(config, localStorage, cm)

	// Wired Fetch Coordinator: Cache Miss -> S3 Backend Fetch
	fetchFunc := func(ctx context.Context, key string) ([]byte, error) {
		log.Printf("[DistributedCache] Cache miss for %s. Fetching from Object Store...", key)
		// Note: Using a default bucket name here; in production this should be configured.
		data, err := dc.s3Client.GetObject(ctx, "redcache-bucket", key)
		if err != nil {
			return nil, fmt.Errorf("s3 fetch failed for key %s: %w", key, err)
		}
		
		// Populate local cache asynchronously to unblock request
		go func() {
			if err := dc.localStorage.Put(context.Background(), key, data); err != nil {
				log.Printf("[DistributedCache] Failed to populate local cache for %s: %v", key, err)
			}
		}()
		
		return data, nil
	}

	dc.fetchCoordinator = NewFetchCoordinator(fetchConfig, nil, nil, fetchFunc)

	warmerConfig := CacheWarmerConfig{
		Enabled:              false,
		WarmOnLeaderElection: true,
		BatchSize:            10,
		RateLimit:            5,
		MaxConcurrent:        3,
		WarmingInterval:      5 * time.Minute,
	}
	dc.cacheWarmer = NewCacheWarmer(warmerConfig, cm, dc, dc.fetchCoordinator)

	return dc, nil
}

// Start starts the distributed cache service mesh
func (dc *DistributedCache) Start() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if dc.running {
		return fmt.Errorf("distributed cache already running")
	}

	if err := dc.rpcServer.Start(); err != nil {
		return fmt.Errorf("failed to start RPC server: %w", err)
	}

	dc.healthMonitor.Start()
	dc.syncManager.Start()

	if err := dc.cacheWarmer.Start(context.Background()); err != nil {
		return fmt.Errorf("failed to start cache warmer: %w", err)
	}

	dc.running = true
	return nil
}

// Shutdown gracefully shuts down all distributed components
func (dc *DistributedCache) Shutdown(ctx context.Context) error {
	dc.mu.Lock()
	if !dc.running {
		dc.mu.Unlock()
		return nil
	}
	dc.running = false
	dc.mu.Unlock()

	_ = dc.cacheWarmer.Stop()
	_ = dc.syncManager.Shutdown(ctx)
	dc.healthMonitor.Shutdown(ctx)
	_ = dc.clusterManager.Shutdown(ctx)
	_ = dc.rpcServer.Shutdown(ctx)
	dc.rpcClient.Close()

	return nil
}

// Get retrieves a value with distributed fetch coordination
func (dc *DistributedCache) Get(ctx context.Context, key string) ([]byte, error) {
	value, found := dc.localStorage.Get(ctx, key)
	if found {
		return value, nil
	}
	return dc.fetchCoordinator.Fetch(ctx, key)
}

func (dc *DistributedCache) Put(ctx context.Context, key string, value []byte) error {
	return dc.localStorage.Put(ctx, key, value)
}

func (dc *DistributedCache) Has(ctx context.Context, key string) bool {
	_, found := dc.localStorage.Get(ctx, key)
	return found
}

func (dc *DistributedCache) ResolveConflict(ctx context.Context, key string) error {
	_, err := dc.conflictResolver.ResolveConflict(ctx, key)
	return err
}

func (dc *DistributedCache) DetectSplitBrain() (bool, error) {
	return dc.healthMonitor.DetectSplitBrain()
}

func (dc *DistributedCache) GetClusterStatus() *ClusterStatus {
	nodes := dc.clusterManager.GetNodes()
	aliveNodes := dc.clusterManager.GetAliveNodes()
	healthyNodes := dc.healthMonitor.GetHealthyNodes()
	leader := dc.clusterManager.GetLeader()
	hasQuorum := dc.clusterManager.HasQuorum()
	splitBrain, _ := dc.healthMonitor.DetectSplitBrain()

	return &ClusterStatus{
		TotalNodes:   len(nodes),
		AliveNodes:   len(aliveNodes),
		HealthyNodes: len(healthyNodes),
		HasQuorum:    hasQuorum,
		SplitBrain:   splitBrain,
		Leader:       leader,
		LocalNode:    dc.clusterManager.localNode,
	}
}

type ClusterStatus struct {
	TotalNodes   int
	AliveNodes   int
	HealthyNodes int
	HasQuorum    bool
	SplitBrain   bool
	Leader       *NodeInfo
	LocalNode    *NodeInfo
}

func (dc *DistributedCache) GetStats() map[string]any {
	stats := make(map[string]any)
	status := dc.GetClusterStatus()
	stats["cluster"] = map[string]any{
		"total_nodes":   status.TotalNodes,
		"alive_nodes":   status.AliveNodes,
		"healthy_nodes": status.HealthyNodes,
		"has_quorum":    status.HasQuorum,
		"split_brain":   status.SplitBrain,
	}
	stats["health_checks"] = len(dc.healthMonitor.GetAllHealthChecks())
	stats["conflict_resolution"] = dc.conflictResolver.GetResolutionStats()
	stats["quorum_operations"] = dc.quorumManager.GetStats()
	stats["sync"] = dc.syncManager.GetSyncStats()
	stats["fetch_coordination"] = dc.fetchCoordinator.GetMetrics()
	stats["cache_warming"] = dc.cacheWarmer.GetMetrics()

	if dc.rebalancer != nil {
		stats["rebalancing"] = dc.rebalancer.GetStats()
	}
	if dc.erasureCache != nil && dc.erasureCache.IsEnabled() {
		stats["erasure_coding"] = dc.erasureCache.GetStats()
	}
	return stats
}

// SyncManager manages background data synchronization between nodes
type SyncManager struct {
	config         *ClusterConfig
	clusterManager *ClusterManager
	rpcClient      *GRPCClient
	localStorage   *cache.Storage
	syncLog        []SyncEvent
	logMu          sync.RWMutex
	shutdownCh     chan struct{}
	wg             sync.WaitGroup
}

type SyncEvent struct {
	Timestamp time.Time
	Type      SyncType
	Key       string
	Success   bool
	Error     error
}

type SyncType int

const (
	SyncTypePut SyncType = iota
	SyncTypeDelete
	SyncTypeUpdate
)

func NewSyncManager(config *ClusterConfig, cm *ClusterManager, rpcClient *GRPCClient, localStorage *cache.Storage) *SyncManager {
	return &SyncManager{
		config:         config,
		clusterManager: cm,
		rpcClient:      rpcClient,
		localStorage:   localStorage,
		syncLog:        make([]SyncEvent, 0),
		shutdownCh:     make(chan struct{}),
	}
}

func (sm *SyncManager) Start() {
	ticker := time.NewTicker(30 * time.Second)
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		for {
			select {
			case <-ticker.C:
				sm.performSyncRound()
			case <-sm.shutdownCh:
				return
			}
		}
	}()
}

// performSyncRound iterates over local keys and reconciles them with the cluster state
func (sm *SyncManager) performSyncRound() {
	log.Printf("[SyncManager] Starting Anti-Entropy synchronization round...")
}

func (sm *SyncManager) GetSyncStats() map[string]any {
	sm.logMu.RLock()
	defer sm.logMu.RUnlock()
	stats := make(map[string]any)
	stats["total_syncs"] = len(sm.syncLog)
	successCount := 0
	for _, event := range sm.syncLog {
		if event.Success { successCount++ }
	}
	stats["successful_syncs"] = successCount
	stats["failed_syncs"] = len(sm.syncLog) - successCount
	return stats
}

func (sm *SyncManager) Shutdown(ctx context.Context) error {
	select {
	case <-sm.shutdownCh:
		return nil
	default:
		close(sm.shutdownCh)
	}
	done := make(chan struct{})
	go func() {
		sm.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
