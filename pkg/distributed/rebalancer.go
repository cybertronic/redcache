package distributed

import (
	"redcache/pkg/cache"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Rebalancer handles automatic data rebalancing when nodes join or leave the cluster
type Rebalancer struct {
	config         *ClusterConfig
	clusterManager *ClusterManager
	localStorage   *cache.Storage
	rpcClient      *GRPCClient
	
	// State
	rebalancing    bool
	mu             sync.RWMutex
	
	// Metrics
	lastRebalance  time.Time
	keysMovedTotal int64
	rebalanceCount int64
	
	// Configuration
	batchSize      int
	throttle       time.Duration
}

// NewRebalancer creates a new rebalancer instance
func NewRebalancer(config *ClusterConfig, cm *ClusterManager, localStorage *cache.Storage, rpcClient *GRPCClient) *Rebalancer {
	return &Rebalancer{
		config:         config,
		clusterManager: cm,
		localStorage:   localStorage,
		rpcClient:      rpcClient,
		batchSize:      100, // Default batch size
		throttle:       10 * time.Millisecond, // Default throttle
	}
}

// OnNodeJoin handles node join events and rebalances data to the new node
func (r *Rebalancer) OnNodeJoin(nodeID string) error {
	log.Printf("[Rebalancer] Node join detected: %s, starting rebalancing...", nodeID)
	
	r.mu.Lock()
	if r.rebalancing {
		r.mu.Unlock()
		log.Printf("[Rebalancer] Rebalancing already in progress, skipping")
		return fmt.Errorf("rebalancing already in progress")
	}
	r.rebalancing = true
	r.mu.Unlock()
	
	defer func() {
		r.mu.Lock()
		r.rebalancing = false
		r.mu.Unlock()
	}()
	
	startTime := time.Now()
	
	// Calculate which keys should move to the new node
	keysToMove, err := r.calculateKeysToMove(nodeID)
	if err != nil {
		return fmt.Errorf("failed to calculate keys to move: %w", err)
	}
	
	log.Printf("[Rebalancer] Found %d keys to move to node %s", len(keysToMove), nodeID)
	
	// Execute migration
	if err := r.migrateKeys(context.Background(), keysToMove, nodeID); err != nil {
		return fmt.Errorf("failed to migrate keys: %w", err)
	}
	
	duration := time.Since(startTime)
	r.lastRebalance = time.Now()
	r.rebalanceCount++
	
	log.Printf("[Rebalancer] Rebalancing complete for node %s: %d keys moved in %v", 
		nodeID, r.keysMovedTotal, duration)
	
	return nil
}

// OnNodeLeave handles node leave events and re-replicates data
func (r *Rebalancer) OnNodeLeave(nodeID string) error {
	log.Printf("[Rebalancer] Node leave detected: %s, starting re-replication...", nodeID)
	
	r.mu.Lock()
	if r.rebalancing {
		r.mu.Unlock()
		log.Printf("[Rebalancer] Rebalancing already in progress, skipping")
		return fmt.Errorf("rebalancing already in progress")
	}
	r.rebalancing = true
	r.mu.Unlock()
	
	defer func() {
		r.mu.Lock()
		r.rebalancing = false
		r.mu.Unlock()
	}()
	
	startTime := time.Now()
	
	// Find keys that need new replicas
	keysToReplicate := r.findUnderReplicatedKeys(nodeID)
	
	log.Printf("[Rebalancer] Found %d keys that need re-replication", len(keysToReplicate))
	
	// Re-replicate to new nodes
	if err := r.reReplicateKeys(context.Background(), keysToReplicate); err != nil {
		return fmt.Errorf("failed to re-replicate keys: %w", err)
	}
	
	duration := time.Since(startTime)
	r.lastRebalance = time.Now()
	r.rebalanceCount++
	
	log.Printf("[Rebalancer] Re-replication complete for node %s: %d keys re-replicated in %v", 
		nodeID, len(keysToReplicate), duration)
	
	return nil
}

// calculateKeysToMove determines which keys should move to a new node
// This uses consistent hashing to find keys whose new replica set includes the new node
func (r *Rebalancer) calculateKeysToMove(newNodeID string) (map[string][]string, error) {
	keysToMove := make(map[string][]string)
	
	// We need to scan all keys in local storage to determine which should move
	// For efficiency, we'll use the storage's scan functionality
	ctx := context.Background()
	
	// Get all keys from local storage
	keys, err := r.localStorage.GetAllKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get all keys: %w", err)
	}
	
	// Note: the new node may not yet be in the cluster manager when this is called
	// (it's being added), so we don't require it to exist yet.
	
	// Create a hash ring for replica calculation
	// We need to add all nodes to the ring
	ring := NewConsistentHashRing(100)
	
	// Add all alive nodes to the ring
	aliveNodes := r.clusterManager.GetAliveNodes()
	for _, node := range aliveNodes {
		ring.AddNode(node.ID)
	}
	
	// For each key, check if it should be on the new node
	for _, key := range keys {
		// Calculate new replica set with the new node
		replicaSet := ring.GetNodes(key, r.config.ReplicationFactor)
		
		// If the new node is in the replica set and this node is the primary owner
		for _, replicaID := range replicaSet {
			if replicaID == newNodeID {
				// This key should be on the new node
				keysToMove[key] = replicaSet
				break
			}
		}
	}
	
	return keysToMove, nil
}

// migrateKeys migrates keys to the target node in batches
func (r *Rebalancer) migrateKeys(ctx context.Context, keys map[string][]string, targetNodeID string) error {
	// Get target node info
	targetNode, exists := r.clusterManager.GetNode(targetNodeID)
	if !exists {
		return fmt.Errorf("target node not found: %s", targetNodeID)
	}
	
	// Migrate in batches
	batch := make(map[string][]byte)
	movedCount := 0
	
	for key := range keys {
		// Get value from local storage
		value, found := r.localStorage.Get(ctx, key)
		if !found {
			log.Printf("[Rebalancer] Key %s not found locally, skipping", key)
			continue
		}
		
		batch[key] = value
		
		// Send batch when full
		if len(batch) >= r.batchSize {
			if err := r.sendBatch(ctx, targetNode, batch); err != nil {
				return fmt.Errorf("failed to send batch: %w", err)
			}
			
			movedCount += len(batch)
			batch = make(map[string][]byte)
			
			// Throttle to prevent overwhelming the network
			if r.throttle > 0 {
				time.Sleep(r.throttle)
			}
		}
	}
	
	// Send remaining batch
	if len(batch) > 0 {
		if err := r.sendBatch(ctx, targetNode, batch); err != nil {
			return fmt.Errorf("failed to send final batch: %w", err)
		}
		movedCount += len(batch)
	}
	
	r.keysMovedTotal += int64(movedCount)
	return nil
}

// sendBatch sends a batch of keys to a node via RPC
func (r *Rebalancer) sendBatch(ctx context.Context, node *NodeInfo, batch map[string][]byte) error {
	for key, value := range batch {
		err := r.rpcClient.SetValue(ctx, node.Address, node.Port, key, value, 0)
		if err != nil {
			return fmt.Errorf("failed to send key %s to %s:%d: %w", key, node.Address, node.Port, err)
		}
	}
	return nil
}

// findUnderReplicatedKeys finds keys that need more replicas after a node leaves
func (r *Rebalancer) findUnderReplicatedKeys(leavingNodeID string) []string {
	underReplicated := make([]string, 0)
	
	ctx := context.Background()
	
	// Get all keys from local storage
	keys, err := r.localStorage.GetAllKeys(ctx)
	if err != nil {
		log.Printf("[Rebalancer] Failed to get all keys: %v", err)
		return underReplicated
	}
	
	// Create hash ring without the leaving node
	ring := NewConsistentHashRing(100)
	aliveNodes := r.clusterManager.GetAliveNodes()
	
	for _, node := range aliveNodes {
		if node.ID != leavingNodeID {
			ring.AddNode(node.ID)
		}
	}
	
	// For each key, check if it has enough replicas
	for _, key := range keys {
		replicaSet := ring.GetNodes(key, r.config.ReplicationFactor)
		
		// If we have fewer than desired replicas, mark as under-replicated
		if len(replicaSet) < r.config.ReplicationFactor {
			underReplicated = append(underReplicated, key)
		}
	}
	
	return underReplicated
}

// reReplicateKeys creates new replicas for under-replicated keys
func (r *Rebalancer) reReplicateKeys(ctx context.Context, keys []string) error {
	// Create hash ring
	ring := NewConsistentHashRing(100)
	aliveNodes := r.clusterManager.GetAliveNodes()
	
	for _, node := range aliveNodes {
		ring.AddNode(node.ID)
	}
	
	for _, key := range keys {
		// Get value from local storage
		value, found := r.localStorage.Get(ctx, key)
		if !found {
			continue
		}
		
		// Calculate new replica set
		replicaSet := ring.GetNodes(key, r.config.ReplicationFactor)
		
		// Replicate to new nodes
		for _, replicaID := range replicaSet {
			node, exists := r.clusterManager.GetNode(replicaID)
			if !exists {
				continue
			}
			
			// Skip if this is the local node (we already have the data)
			if node.ID == r.config.NodeID {
				continue
			}
			
			// Send to replica
			err := r.rpcClient.SetValue(ctx, node.Address, node.Port, key, value, 0)
			if err != nil {
				log.Printf("[Rebalancer] Failed to replicate key %s to %s:%d: %v",
					key, node.Address, node.Port, err)
				continue
			}
		}
		
		// Throttle between keys
		if r.throttle > 0 {
			time.Sleep(r.throttle)
		}
	}
	
	return nil
}

// GetStats returns rebalancing statistics
func (r *Rebalancer) GetStats() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	return map[string]any{
		"rebalancing":       r.rebalancing,
		"last_rebalance":    r.lastRebalance,
		"keys_moved_total":  r.keysMovedTotal,
		"rebalance_count":   r.rebalanceCount,
		"batch_size":        r.batchSize,
		"throttle":          r.throttle.String(),
	}
}

// IsRebalancing returns whether rebalancing is currently in progress
func (r *Rebalancer) IsRebalancing() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.rebalancing
}

// TriggerManualRebalance allows manual triggering of rebalancing (for testing/admin)
func (r *Rebalancer) TriggerManualRebalance(nodeID string, isJoin bool) error {
	if isJoin {
		return r.OnNodeJoin(nodeID)
	}
	return r.OnNodeLeave(nodeID)
}