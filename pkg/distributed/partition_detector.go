package distributed

import (
	"fmt"
	"sync"
)

// PartitionDetector detects network partitions in the cluster
type PartitionDetector struct {
	mu sync.RWMutex
	
	// Node reachability matrix
	// reachability[nodeA][nodeB] = true if nodeA can reach nodeB
	reachability map[string]map[string]bool
	
	// Detected partitions
	partitions []Partition
	
	// Configuration
	nodeID string
}

// Partition represents a network partition
type Partition struct {
	ID       string
	NodeIDs  []string
	HasQuorum bool
	IsPrimary bool
}

// NewPartitionDetector creates a new partition detector
func NewPartitionDetector(nodeID string) *PartitionDetector {
	return &PartitionDetector{
		nodeID:       nodeID,
		reachability: make(map[string]map[string]bool),
		partitions:   make([]Partition, 0),
	}
}

// UpdateReachability updates the reachability status between two nodes
func (pd *PartitionDetector) UpdateReachability(fromNode, toNode string, reachable bool) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	if pd.reachability[fromNode] == nil {
		pd.reachability[fromNode] = make(map[string]bool)
	}
	
	pd.reachability[fromNode][toNode] = reachable
}

// DetectPartitions detects network partitions using graph connectivity
func (pd *PartitionDetector) DetectPartitions(allNodes []string) []Partition {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	visited := make(map[string]bool)
	partitions := make([]Partition, 0)
	partitionID := 0
	
	for _, node := range allNodes {
		if visited[node] {
			continue
		}
		
		// Find all nodes connected to this node
		partition := pd.findConnectedNodes(node, allNodes, visited)
		
		partitions = append(partitions, Partition{
			ID:      fmt.Sprintf("partition-%d", partitionID),
			NodeIDs: partition,
		})
		
		partitionID++
	}
	
	pd.partitions = partitions
	return partitions
}

// findConnectedNodes finds all nodes reachable from startNode using DFS
func (pd *PartitionDetector) findConnectedNodes(startNode string, allNodes []string, visited map[string]bool) []string {
	partition := make([]string, 0)
	stack := []string{startNode}
	
	for len(stack) > 0 {
		// Pop from stack
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		
		if visited[node] {
			continue
		}
		
		visited[node] = true
		partition = append(partition, node)
		
		// Add all reachable nodes to stack
		if reachable, exists := pd.reachability[node]; exists {
			for targetNode, canReach := range reachable {
				if canReach && !visited[targetNode] {
					stack = append(stack, targetNode)
				}
			}
		}
	}
	
	return partition
}

// FindMajorityPartition finds the partition with the most nodes
func (pd *PartitionDetector) FindMajorityPartition(quorumSize int) *Partition {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	
	var majorityPartition *Partition
	maxNodes := 0
	
	for i := range pd.partitions {
		partition := &pd.partitions[i]
		nodeCount := len(partition.NodeIDs)
		
		partition.HasQuorum = nodeCount >= quorumSize
		
		if nodeCount > maxNodes {
			maxNodes = nodeCount
			majorityPartition = partition
		}
	}
	
	if majorityPartition != nil {
		majorityPartition.IsPrimary = true
	}
	
	return majorityPartition
}

// IsPartitioned returns whether network partitions are detected
func (pd *PartitionDetector) IsPartitioned() bool {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	
	return len(pd.partitions) > 1
}

// GetPartitions returns all detected partitions
func (pd *PartitionDetector) GetPartitions() []Partition {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	
	partitions := make([]Partition, len(pd.partitions))
	copy(partitions, pd.partitions)
	
	return partitions
}

// GetMyPartition returns the partition containing this node
func (pd *PartitionDetector) GetMyPartition() *Partition {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	
	for i := range pd.partitions {
		partition := &pd.partitions[i]
		for _, nodeID := range partition.NodeIDs {
			if nodeID == pd.nodeID {
				return partition
			}
		}
	}
	
	return nil
}

// CanReach checks if fromNode can reach toNode
func (pd *PartitionDetector) CanReach(fromNode, toNode string) bool {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	
	if reachable, exists := pd.reachability[fromNode]; exists {
		return reachable[toNode]
	}
	
	return false
}

// GetReachableNodes returns all nodes reachable from the given node
func (pd *PartitionDetector) GetReachableNodes(fromNode string) []string {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	
	reachable := make([]string, 0)
	
	if nodes, exists := pd.reachability[fromNode]; exists {
		for node, canReach := range nodes {
			if canReach {
				reachable = append(reachable, node)
			}
		}
	}
	
	return reachable
}

// Reset clears all partition detection state
func (pd *PartitionDetector) Reset() {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	pd.reachability = make(map[string]map[string]bool)
	pd.partitions = make([]Partition, 0)
}

// GetStats returns partition detection statistics
func (pd *PartitionDetector) GetStats() PartitionStats {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	
	totalNodes := len(pd.reachability)
	reachableCount := 0
	
	if myReachability, exists := pd.reachability[pd.nodeID]; exists {
		for _, canReach := range myReachability {
			if canReach {
				reachableCount++
			}
		}
	}
	
	return PartitionStats{
		TotalNodes:       totalNodes,
		ReachableNodes:   reachableCount,
		PartitionCount:   len(pd.partitions),
		IsPartitioned:    len(pd.partitions) > 1,
	}
}

// PartitionStats contains partition detection statistics
type PartitionStats struct {
	TotalNodes     int
	ReachableNodes int
	PartitionCount int
	IsPartitioned  bool
}