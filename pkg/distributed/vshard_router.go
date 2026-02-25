package distributed

import (
	"fmt"
	"hash/crc32"
	"sync"
)

// ShardID is a unique identifier for a virtual shard (0 to NumVirtualShards-1).
type ShardID uint32

const (
	// DefaultNumVirtualShards is high enough to allow granular scaling to thousands of nodes
	// while keeping the routing table manageable.
	DefaultNumVirtualShards = 1024
)

// ShardState represents the lifecycle status of a virtual shard on a specific node.
type ShardState int

const (
	ShardStateNone ShardState = iota
	ShardStateFollower
	ShardStateCandidate
	ShardStateLeader
	ShardStateMoving // Shard is currently being migrated to another node
)

// VShardPlacement maps a Virtual Shard to the physical nodes hosting its Raft group.
type VShardPlacement struct {
	ShardID ShardID
	Peers   []string // Physical Node IDs (e.g., "node-1:8080")
	Leader  string   // Current known leader for this shard
	Version uint64   // Incrementing version for topology updates
}

// Router handles the deterministic mapping of keys to virtual shards.
type Router struct {
	numShards uint32
	mu        sync.RWMutex
	// topology maps ShardID to its placement details
	topology map[ShardID]*VShardPlacement
}

// NewRouter creates a new virtual shard router.
func NewRouter(numShards uint32) *Router {
	if numShards == 0 {
		numShards = DefaultNumVirtualShards
	}
	return &Router{
		numShards: numShards,
		topology:  make(map[ShardID]*VShardPlacement),
	}
}

// GetShardID returns the virtual shard ID for a given key using CRC32 checksum.
func (r *Router) GetShardID(key string) ShardID {
	checksum := crc32.ChecksumIEEE([]byte(key))
	return ShardID(checksum % r.numShards)
}

// UpdateTopology updates the routing table with new shard placement information.
func (r *Router) UpdateTopology(placements []*VShardPlacement) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, p := range placements {
		existing, ok := r.topology[p.ShardID]
		if !ok || p.Version > existing.Version {
			r.topology[p.ShardID] = p
		}
	}
}

// Locate returns the placement info for a specific key.
func (r *Router) Locate(key string) (*VShardPlacement, error) {
	shardID := r.GetShardID(key)
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	placement, ok := r.topology[shardID]
	if !ok {
		return nil, fmt.Errorf("topology for shard %d not yet initialized", shardID)
	}
	
	return placement, nil
}

// NumShards returns the total configured virtual shards.
func (r *Router) NumShards() uint32 {
	return r.numShards
}
