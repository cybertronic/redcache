package distributed

import (
	"fmt"
	"sync"
)

// VectorClock implements a vector clock for distributed versioning
type VectorClock struct {
	mu    sync.RWMutex
	clock map[string]int64
}

// Ordering represents the relationship between two vector clocks
type Ordering int

const (
	OrderingBefore Ordering = iota  // This clock is before the other
	OrderingAfter                    // This clock is after the other
	OrderingConcurrent               // Clocks are concurrent (conflict)
	OrderingEqual                    // Clocks are equal
)

// NewVectorClock creates a new vector clock
func NewVectorClock() *VectorClock {
	return &VectorClock{
		clock: make(map[string]int64),
	}
}

// Increment increments the clock for the given node
func (vc *VectorClock) Increment(nodeID string) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	
	vc.clock[nodeID]++
}

// Get returns the clock value for a node
func (vc *VectorClock) Get(nodeID string) int64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	return vc.clock[nodeID]
}

// Set sets the clock value for a node
func (vc *VectorClock) Set(nodeID string, value int64) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	
	vc.clock[nodeID] = value
}

// Merge merges another vector clock into this one (taking max of each component)
func (vc *VectorClock) Merge(other *VectorClock) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	
	other.mu.RLock()
	defer other.mu.RUnlock()
	
	for nodeID, otherValue := range other.clock {
		if currentValue, exists := vc.clock[nodeID]; !exists || otherValue > currentValue {
			vc.clock[nodeID] = otherValue
		}
	}
}

// Compare compares this vector clock with another
func (vc *VectorClock) Compare(other *VectorClock) Ordering {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	other.mu.RLock()
	defer other.mu.RUnlock()
	
	// Get all node IDs from both clocks
	allNodes := make(map[string]bool)
	for nodeID := range vc.clock {
		allNodes[nodeID] = true
	}
	for nodeID := range other.clock {
		allNodes[nodeID] = true
	}
	
	// Compare each component
	hasGreater := false
	hasLess := false
	
	for nodeID := range allNodes {
		thisValue := vc.clock[nodeID]
		otherValue := other.clock[nodeID]
		
		if thisValue > otherValue {
			hasGreater = true
		} else if thisValue < otherValue {
			hasLess = true
		}
	}
	
	// Determine ordering
	if !hasGreater && !hasLess {
		return OrderingEqual
	} else if hasGreater && !hasLess {
		return OrderingAfter
	} else if !hasGreater && hasLess {
		return OrderingBefore
	} else {
		return OrderingConcurrent
	}
}

// IsConcurrent checks if this clock is concurrent with another (conflict)
func (vc *VectorClock) IsConcurrent(other *VectorClock) bool {
	return vc.Compare(other) == OrderingConcurrent
}

// IsAfter checks if this clock is after another
func (vc *VectorClock) IsAfter(other *VectorClock) bool {
	return vc.Compare(other) == OrderingAfter
}

// IsBefore checks if this clock is before another
func (vc *VectorClock) IsBefore(other *VectorClock) bool {
	return vc.Compare(other) == OrderingBefore
}

// Clone creates a copy of this vector clock
func (vc *VectorClock) Clone() *VectorClock {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	clone := NewVectorClock()
	for nodeID, value := range vc.clock {
		clone.clock[nodeID] = value
	}
	
	return clone
}

// String returns a string representation of the vector clock
func (vc *VectorClock) String() string {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	return fmt.Sprintf("%v", vc.clock)
}

// ToMap returns the clock as a map (for serialization)
func (vc *VectorClock) ToMap() map[string]int64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	clockMap := make(map[string]int64)
	for nodeID, value := range vc.clock {
		clockMap[nodeID] = value
	}
	
	return clockMap
}

// FromMap creates a vector clock from a map
func FromMap(clockMap map[string]int64) *VectorClock {
	vc := NewVectorClock()
	for nodeID, value := range clockMap {
		vc.clock[nodeID] = value
	}
	return vc
}

// Size returns the number of nodes in the clock
func (vc *VectorClock) Size() int {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	return len(vc.clock)
}

// GetAllNodes returns all node IDs in the clock
func (vc *VectorClock) GetAllNodes() []string {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	nodes := make([]string, 0, len(vc.clock))
	for nodeID := range vc.clock {
		nodes = append(nodes, nodeID)
	}
	
	return nodes
}

// Max returns the maximum clock value across all nodes
func (vc *VectorClock) Max() int64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	max := int64(0)
	for _, value := range vc.clock {
		if value > max {
			max = value
		}
	}
	
	return max
}

// Sum returns the sum of all clock values
func (vc *VectorClock) Sum() int64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	sum := int64(0)
	for _, value := range vc.clock {
		sum += value
	}
	
	return sum
}