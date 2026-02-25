package distributed

import (
	"encoding/json"
	"sync"
	"time"
)

// PlacementDriver coordinates the assignment of Virtual Shards to Physical Nodes.
type PlacementDriver struct {
	mu          sync.RWMutex
	nodes       map[string]*NodeMetadata
	shards      map[ShardID]*VShardPlacement
	numShards   uint32
	replication int // Number of replicas per shard (e.g., 3)

	onTopologyChange func([]*VShardPlacement)
}

// NodeMetadata tracks the health and capacity of a physical node.
type NodeMetadata struct {
	ID            string
	Address       string
	Status        string // "online", "degraded", "offline"
	LastHeartbeat time.Time
	ShardCount    int
	Capacity      int64 // Available storage/memory
}

// NewPlacementDriver creates a new PD with the specified shard count and replication factor.
func NewPlacementDriver(numShards uint32, replication int) *PlacementDriver {
	pd := &PlacementDriver{
		nodes:       make(map[string]*NodeMetadata),
		shards:      make(map[ShardID]*VShardPlacement),
		numShards:   numShards,
		replication: replication,
	}
	return pd
}

// RegisterNode adds or updates a node in the PD's inventory.
func (pd *PlacementDriver) RegisterNode(id, addr string, capacity int64) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	pd.nodes[id] = &NodeMetadata{
		ID:            id,
		Address:       addr,
		Status:        "online",
		LastHeartbeat: time.Now(),
		Capacity:      capacity,
	}
}

// Rebalance calculates a new topology based on current node availability.
func (pd *PlacementDriver) Rebalance() []*VShardPlacement {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	activeNodes := pd.getOnlineNodes()
	if len(activeNodes) < pd.replication {
		return nil 
	}

	updates := make([]*VShardPlacement, 0)

	for i := uint32(0); i < pd.numShards; i++ {
		sid := ShardID(i)
		existing, ok := pd.shards[sid]

		if !ok || pd.needsRebalance(existing, activeNodes) {
			newPlacement := pd.calculatePlacement(sid, activeNodes)
			pd.shards[sid] = newPlacement
			updates = append(updates, newPlacement)
		}
	}

	if pd.onTopologyChange != nil && len(updates) > 0 {
		pd.onTopologyChange(updates)
	}

	return updates
}

func (pd *PlacementDriver) calculatePlacement(sid ShardID, nodes []*NodeMetadata) *VShardPlacement {
	selected := make([]string, 0, pd.replication)
	candidates := make([]*NodeMetadata, len(nodes))
	copy(candidates, nodes)
	
	for i := 0; i < pd.replication; i++ {
		bestIdx := -1
		for j, n := range candidates {
			if n == nil { continue }
			if bestIdx == -1 || n.ShardCount < candidates[bestIdx].ShardCount {
				bestIdx = j
			}
		}
		if bestIdx != -1 {
			selected = append(selected, candidates[bestIdx].ID)
			candidates[bestIdx].ShardCount++
			candidates[bestIdx] = nil
		}
	}

	version := uint64(time.Now().UnixNano())
	if existing, ok := pd.shards[sid]; ok {
		version = existing.Version + 1
	}

	return &VShardPlacement{
		ShardID: sid,
		Peers:   selected,
		Leader:  selected[0],
		Version: version,
	}
}

func (pd *PlacementDriver) needsRebalance(p *VShardPlacement, active []*NodeMetadata) bool {
	nodeMap := make(map[string]bool)
	for _, n := range active {
		nodeMap[n.ID] = true
	}

	for _, peer := range p.Peers {
		if !nodeMap[peer] {
			return true
		}
	}
	return false
}

func (pd *PlacementDriver) getOnlineNodes() []*NodeMetadata {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	online := make([]*NodeMetadata, 0, len(pd.nodes))
	for _, n := range pd.nodes {
		if n.Status == "online" && time.Since(n.LastHeartbeat) < 30*time.Second {
			online = append(online, n)
		}
	}
	return online
}

func (pd *PlacementDriver) EncodeTopology() ([]byte, error) {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	return json.Marshal(pd.shards)
}

func (pd *PlacementDriver) SetOnTopologyChange(fn func([]*VShardPlacement)) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.onTopologyChange = fn
}
