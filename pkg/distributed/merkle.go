package distributed

import (
	"bytes"
	"crypto/sha256"
	"sort"
	"sync"
)

// MerkleNode represents a node in the hash tree.
type MerkleNode struct {
	Hash []byte
}

// MerkleTree is used to efficiently compare keyspaces between nodes 
// to identify missing or inconsistent shards.
type MerkleTree struct {
	mu    sync.RWMutex
	nodes []MerkleNode
	keys  []string // Sorted keys in this tree
}

// NewMerkleTree builds a tree from a set of shard keys.
func NewMerkleTree(keys []string) *MerkleTree {
	if len(keys) == 0 {
		return &MerkleTree{}
	}

	// 1. Sort keys for determinism
	sort.Strings(keys)
	
	// 2. Leaf hashes
	leaves := make([]MerkleNode, len(keys))
	for i, k := range keys {
		h := sha256.Sum256([]byte(k))
		leaves[i] = MerkleNode{Hash: h[:]}
	}

	return &MerkleTree{
		nodes: buildTree(leaves),
		keys:  keys,
	}
}

func buildTree(leaves []MerkleNode) []MerkleNode {
	if len(leaves) == 0 {
		return nil
	}
	if len(leaves) == 1 {
		return leaves
	}
	
	current := leaves
	for len(current) > 1 {
		var next []MerkleNode
		for i := 0; i < len(current); i += 2 {
			if i+1 < len(current) {
				// Pair exists, hash them together
				h := sha256.Sum256(append(current[i].Hash, current[i+1].Hash...))
				next = append(next, MerkleNode{Hash: h[:]})
			} else {
				// Maintain balance without redundant hashing
				next = append(next, current[i])
			}
		}
		current = next
	}
	return current
}

// Root returns the top-level hash of the tree.
func (mt *MerkleTree) Root() []byte {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	if len(mt.nodes) == 0 {
		return nil
	}
	return mt.nodes[0].Hash
}

// Compare identifies keys that exist in mt but may be different or missing in other.
func (mt *MerkleTree) Compare(otherRoot []byte) bool {
	return bytes.Equal(mt.Root(), otherRoot)
}
