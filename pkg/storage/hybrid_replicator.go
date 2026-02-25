package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// HybridReplicator combines Raft (strong consistency) and Gossip (eventual consistency)
type HybridReplicator struct {
	raft           *RaftStore
	gossip         *GossipReplicator
	db             *Database
	config         HybridConfig
	mu             sync.RWMutex
}

// HybridConfig configures the hybrid replicator
type HybridConfig struct {
	NodeID            string
	RaftConfig        RaftConfig
	GossipConfig      GossipConfig
	CoordinationPrefix string // Prefix for coordination data (uses Raft)
	CachePrefix       string // Prefix for cache data (uses Gossip)
}

// ConsistencyLevel defines the consistency level for operations
type ConsistencyLevel int

const (
	// ConsistencyStrong uses Raft for strong consistency
	ConsistencyStrong ConsistencyLevel = iota
	// ConsistencyEventual uses Gossip for eventual consistency
	ConsistencyEventual
)

// NewHybridReplicator creates a new hybrid replicator
func NewHybridReplicator(config HybridConfig, db *Database, ml *memberlist.Memberlist) (*HybridReplicator, error) {
	// Create Raft store for coordination
	raftStore, err := NewRaftStore(config.RaftConfig, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft store: %w", err)
	}

	// Create Gossip replicator for cache data
	gossipReplicator, err := NewGossipReplicator(config.GossipConfig, db, ml)
	if err != nil {
		return nil, fmt.Errorf("failed to create gossip replicator: %w", err)
	}

	return &HybridReplicator{
		raft:   raftStore,
		gossip: gossipReplicator,
		db:     db,
		config: config,
	}, nil
}

// Set stores a key-value pair with specified consistency level
func (hr *HybridReplicator) Set(ctx context.Context, key string, value []byte, consistency ConsistencyLevel) error {
	return hr.SetWithTTL(ctx, key, value, 0, consistency)
}

// SetWithTTL stores a key-value pair with TTL and specified consistency level
func (hr *HybridReplicator) SetWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration, consistency ConsistencyLevel) error {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	switch consistency {
	case ConsistencyStrong:
		// Use Raft for strong consistency
		return hr.raft.Set([]byte(key), value)

	case ConsistencyEventual:
		// Use Gossip for eventual consistency
		return hr.gossip.Replicate(ctx, key, value, ttl)

	default:
		return fmt.Errorf("unknown consistency level: %v", consistency)
	}
}

// Get retrieves a value
func (hr *HybridReplicator) Get(ctx context.Context, key string) ([]byte, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	// Determine which store to use based on key prefix
	if hr.isCoordinationKey(key) {
		return hr.raft.Get([]byte(key))
	}

	return hr.gossip.Get(ctx, key)
}

// Delete removes a key with specified consistency level
func (hr *HybridReplicator) Delete(ctx context.Context, key string, consistency ConsistencyLevel) error {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	switch consistency {
	case ConsistencyStrong:
		return hr.raft.Delete([]byte(key))

	case ConsistencyEventual:
		return hr.gossip.Delete(ctx, key)

	default:
		return fmt.Errorf("unknown consistency level: %v", consistency)
	}
}

// SetCoordination stores coordination data with strong consistency
func (hr *HybridReplicator) SetCoordination(ctx context.Context, key string, value []byte) error {
	fullKey := hr.config.CoordinationPrefix + key
	return hr.raft.Set([]byte(fullKey), value)
}

// GetCoordination retrieves coordination data
func (hr *HybridReplicator) GetCoordination(ctx context.Context, key string) ([]byte, error) {
	fullKey := hr.config.CoordinationPrefix + key
	return hr.raft.Get([]byte(fullKey))
}

// DeleteCoordination removes coordination data
func (hr *HybridReplicator) DeleteCoordination(ctx context.Context, key string) error {
	fullKey := hr.config.CoordinationPrefix + key
	return hr.raft.Delete([]byte(fullKey))
}

// SetCache stores cache data with eventual consistency
func (hr *HybridReplicator) SetCache(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	fullKey := hr.config.CachePrefix + key
	return hr.gossip.Replicate(ctx, fullKey, value, ttl)
}

// GetCache retrieves cache data
func (hr *HybridReplicator) GetCache(ctx context.Context, key string) ([]byte, error) {
	fullKey := hr.config.CachePrefix + key
	return hr.gossip.Get(ctx, fullKey)
}

// DeleteCache removes cache data
func (hr *HybridReplicator) DeleteCache(ctx context.Context, key string) error {
	fullKey := hr.config.CachePrefix + key
	return hr.gossip.Delete(ctx, fullKey)
}

// isCoordinationKey checks if a key is coordination data
func (hr *HybridReplicator) isCoordinationKey(key string) bool {
	return len(key) >= len(hr.config.CoordinationPrefix) &&
		key[:len(hr.config.CoordinationPrefix)] == hr.config.CoordinationPrefix
}

// IsLeader returns true if this node is the Raft leader
func (hr *HybridReplicator) IsLeader() bool {
	return hr.raft.IsLeader()
}

// Leader returns the current Raft leader address
func (hr *HybridReplicator) Leader() string {
	return hr.raft.Leader()
}

// AddVoter adds a voting member to the Raft cluster
func (hr *HybridReplicator) AddVoter(id, address string) error {
	return hr.raft.AddVoter(id, address)
}

// RemoveServer removes a server from the Raft cluster
func (hr *HybridReplicator) RemoveServer(id string) error {
	return hr.raft.RemoveServer(id)
}

// GetServers returns the list of Raft servers
func (hr *HybridReplicator) GetServers() ([]Server, error) {
	return hr.raft.GetServers()
}

// Stats returns replication statistics
func (hr *HybridReplicator) Stats() HybridStats {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return HybridStats{
		Raft:   hr.raft.Stats(),
		Gossip: hr.gossip.Stats(),
		DB:     hr.db.Stats(),
	}
}

// HybridStats contains hybrid replication statistics
type HybridStats struct {
	Raft   map[string]string
	Gossip GossipStats
	DB     DatabaseStats
}

// Close shuts down the hybrid replicator
func (hr *HybridReplicator) Close() error {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	// Close gossip first
	if err := hr.gossip.Close(); err != nil {
		return err
	}

	// Close raft
	if err := hr.raft.Close(); err != nil {
		return err
	}

	return nil
}

// WaitForLeader waits for a Raft leader to be elected
func (hr *HybridReplicator) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if hr.raft.Leader() != "" {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for leader")
}

// Barrier ensures all pending operations are committed
func (hr *HybridReplicator) Barrier(timeout time.Duration) error {
	if !hr.raft.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	future := hr.raft.raft.Barrier(timeout)
	return future.Error()
}

// Snapshot triggers a Raft snapshot
func (hr *HybridReplicator) Snapshot() error {
	if !hr.raft.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	future := hr.raft.raft.Snapshot()
	return future.Error()
}

// VerifyLeader verifies this node is still the leader
func (hr *HybridReplicator) VerifyLeader() error {
	if !hr.raft.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	future := hr.raft.raft.VerifyLeader()
	return future.Error()
}