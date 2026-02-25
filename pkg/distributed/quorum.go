package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// QuorumManager manages quorum-based decision making
type QuorumManager struct {
	mu sync.RWMutex
	
	// Configuration
	nodeID       string
	clusterSize  int
	quorumSize   int
	config       *QuorumConfig
	
	// Dependencies
	cm       *ClusterManager
	rpcClient *GRPCClient
	
	// State
	activeNodes  map[string]*NodeInfo
	isInQuorum   bool
	isReadOnly   bool
	partitionID  string
	
	// Callbacks
	onQuorumLost     func()
	onQuorumRegained func()
	
	// Metrics
	quorumLossCount   int64
	lastQuorumCheck   time.Time
	lastQuorumLoss    time.Time
	lastQuorumRegain  time.Time
}

// QuorumConfig configures the quorum manager
type QuorumConfig struct {
	NodeID              string
	ClusterSize         int
	QuorumSize          int  // If 0, defaults to (ClusterSize/2)+1
	WriteQuorum         int  // Defaults to QuorumSize
	ReadQuorum          int  // Defaults to 1
	ReadOnlyOnMinority  bool
	RetryCount          int
	RetryDelay          time.Duration
	Timeout             time.Duration
	RPCPort             int
}

// NewQuorumManager creates a new quorum manager
func NewQuorumManager(config QuorumConfig, cm *ClusterManager, rpcClient *GRPCClient) *QuorumManager {
	quorumSize := config.QuorumSize
	if quorumSize == 0 {
		quorumSize = (config.ClusterSize / 2) + 1
	}
	
	writeQuorum := config.WriteQuorum
	if writeQuorum == 0 {
		writeQuorum = quorumSize
	}
	
	readQuorum := config.ReadQuorum
	if readQuorum == 0 {
		readQuorum = 1
	}
	
	// Set defaults
	if config.RetryCount == 0 {
		config.RetryCount = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 100 * time.Millisecond
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}
	
	config.QuorumSize = quorumSize
	config.WriteQuorum = writeQuorum
	config.ReadQuorum = readQuorum
	
	qm := &QuorumManager{
		nodeID:      config.NodeID,
		clusterSize: config.ClusterSize,
		quorumSize:  quorumSize,
		config:      &config,
		cm:          cm,
		rpcClient:   rpcClient,
		activeNodes: make(map[string]*NodeInfo),
		isInQuorum:  false,
		isReadOnly:  false,
	}
	
	// Add self as active node
	qm.activeNodes[config.NodeID] = &NodeInfo{
		ID:            config.NodeID,
		State:         NodeStateActive,
		LastHeartbeat: time.Now(),
	}
	
	return qm
}

// UpdateNodeStatus updates the status of a node
func (qm *QuorumManager) UpdateNodeStatus(nodeID string, state NodeState) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	node, exists := qm.activeNodes[nodeID]
	if !exists {
		node = &NodeInfo{
			ID:            nodeID,
			State:         state,
			LastHeartbeat: time.Now(),
		}
		qm.activeNodes[nodeID] = node
	} else {
		node.State = state
		node.LastHeartbeat = time.Now()
	}
}

// RemoveNode removes a node from active nodes
func (qm *QuorumManager) RemoveNode(nodeID string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	delete(qm.activeNodes, nodeID)
}

// CheckQuorum checks if we have quorum and updates state
func (qm *QuorumManager) CheckQuorum() bool {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	qm.lastQuorumCheck = time.Now()
	
	// Count active nodes
	activeCount := 0
	for _, node := range qm.activeNodes {
		if node.State == NodeStateActive {
			activeCount++
		}
	}
	
	hasQuorum := activeCount >= qm.quorumSize
	
	// Handle quorum state changes
	if hasQuorum && !qm.isInQuorum {
		// Regained quorum
		qm.isInQuorum = true
		qm.isReadOnly = false
		qm.lastQuorumRegain = time.Now()
		
		if qm.onQuorumRegained != nil {
			go qm.onQuorumRegained()
		}
	} else if !hasQuorum && qm.isInQuorum {
		// Lost quorum
		qm.isInQuorum = false
		qm.isReadOnly = true
		qm.quorumLossCount++
		qm.lastQuorumLoss = time.Now()
		
		if qm.onQuorumLost != nil {
			go qm.onQuorumLost()
		}
	}
	
	return hasQuorum
}

// HasQuorum returns whether we currently have quorum
func (qm *QuorumManager) HasQuorum() bool {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	return qm.isInQuorum
}

// IsReadOnly returns whether we're in read-only mode
func (qm *QuorumManager) IsReadOnly() bool {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	return qm.isReadOnly
}

// GetActiveNodeCount returns the number of active nodes
func (qm *QuorumManager) GetActiveNodeCount() int {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	count := 0
	for _, node := range qm.activeNodes {
		if node.State == NodeStateActive {
			count++
		}
	}
	
	return count
}

// GetQuorumSize returns the required quorum size
func (qm *QuorumManager) GetQuorumSize() int {
	return qm.quorumSize
}

// IsPrimaryPartition returns whether this partition is the primary (has quorum)
func (qm *QuorumManager) IsPrimaryPartition() bool {
	return qm.HasQuorum()
}

// IsPartitioned returns whether a partition is detected
func (qm *QuorumManager) IsPartitioned() bool {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	// If we don't have quorum, we're likely in a minority partition
	return !qm.isInQuorum
}

// GetActiveNodes returns a list of active nodes
func (qm *QuorumManager) GetActiveNodes() []*NodeInfo {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	nodes := make([]*NodeInfo, 0, len(qm.activeNodes))
	for _, node := range qm.activeNodes {
		if node.State == NodeStateActive {
			nodes = append(nodes, node)
		}
	}
	
	return nodes
}

// SetQuorumLostCallback sets a callback for when quorum is lost
func (qm *QuorumManager) SetQuorumLostCallback(callback func()) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	qm.onQuorumLost = callback
}

// SetQuorumRegainedCallback sets a callback for when quorum is regained
func (qm *QuorumManager) SetQuorumRegainedCallback(callback func()) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	qm.onQuorumRegained = callback
}

// EnterReadOnlyMode forces the node into read-only mode
func (qm *QuorumManager) EnterReadOnlyMode() {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	qm.isReadOnly = true
}

// ExitReadOnlyMode exits read-only mode (only if we have quorum)
func (qm *QuorumManager) ExitReadOnlyMode() error {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	if !qm.isInQuorum {
		return fmt.Errorf("cannot exit read-only mode without quorum")
	}
	
	qm.isReadOnly = false
	return nil
}

// GetStats returns quorum statistics
func (qm *QuorumManager) GetStats() QuorumStats {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	activeCount := 0
	for _, node := range qm.activeNodes {
		if node.State == NodeStateActive {
			activeCount++
		}
	}
	
	return QuorumStats{
		ClusterSize:      qm.clusterSize,
		QuorumSize:       qm.quorumSize,
		ActiveNodes:      activeCount,
		HasQuorum:        qm.isInQuorum,
		IsReadOnly:       qm.isReadOnly,
		QuorumLossCount:  qm.quorumLossCount,
		LastQuorumCheck:  qm.lastQuorumCheck,
		LastQuorumLoss:   qm.lastQuorumLoss,
		LastQuorumRegain: qm.lastQuorumRegain,
	}
}

// QuorumStats contains quorum statistics
type QuorumStats struct {
	ClusterSize      int
	QuorumSize       int
	ActiveNodes      int
	HasQuorum        bool
	IsReadOnly       bool
	QuorumLossCount  int64
	LastQuorumCheck  time.Time
	LastQuorumLoss   time.Time
	LastQuorumRegain time.Time
}

// ValidateWrite checks if a write operation is allowed
func (qm *QuorumManager) ValidateWrite(ctx context.Context) error {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	if qm.isReadOnly {
		return fmt.Errorf("node in read-only mode (minority partition)")
	}
	
	if !qm.isInQuorum {
		return fmt.Errorf("no quorum for write operations")
	}
	
	return nil
}

// ValidateRead checks if a read operation is allowed
func (qm *QuorumManager) ValidateRead(ctx context.Context) error {
	// Reads are always allowed, even in read-only mode
	return nil
}

// WaitForQuorum waits until quorum is achieved or context is cancelled
func (qm *QuorumManager) WaitForQuorum(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if qm.HasQuorum() {
				return nil
			}
		}
	}
}

// GetPartitionInfo returns information about the current partition
func (qm *QuorumManager) GetPartitionInfo() PartitionInfo {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	activeNodes := make([]string, 0)
	for _, node := range qm.activeNodes {
		if node.State == NodeStateActive {
			activeNodes = append(activeNodes, node.ID)
		}
	}
	
	return PartitionInfo{
		PartitionID:  qm.partitionID,
		NodeIDs:      activeNodes,
		HasQuorum:    qm.isInQuorum,
		IsPrimary:    qm.isInQuorum,
		IsReadOnly:   qm.isReadOnly,
	}
}

// PartitionInfo contains information about a partition
type PartitionInfo struct {
	PartitionID  string
	NodeIDs      []string
	HasQuorum    bool
	IsPrimary    bool
	IsReadOnly   bool
}

// UpdateHeartbeat updates the last heartbeat time for a node
func (qm *QuorumManager) UpdateHeartbeat(nodeID string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	node, exists := qm.activeNodes[nodeID]
	if !exists {
		node = &NodeInfo{
			ID:            nodeID,
			State:         NodeStateActive,
			LastHeartbeat: time.Now(),
		}
		qm.activeNodes[nodeID] = node
	} else {
		node.LastHeartbeat = time.Now()
		if node.State != NodeStateActive {
			node.State = NodeStateActive
		}
	}
}

// CheckNodeHealth checks node health based on heartbeat timeout
func (qm *QuorumManager) CheckNodeHealth(timeout time.Duration) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	now := time.Now()
	
	for nodeID, node := range qm.activeNodes {
		if nodeID == qm.nodeID {
			// Don't check self
			continue
		}
		
		timeSinceHeartbeat := now.Sub(node.LastHeartbeat)
		
		if timeSinceHeartbeat > timeout {
			if node.State == NodeStateActive {
				node.State = NodeStateSuspect
			} else if node.State == NodeStateSuspect {
				node.State = NodeStateFailed
			}
		}
	}
}

// GetNodeState returns the state of a specific node
func (qm *QuorumManager) GetNodeState(nodeID string) NodeState {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	node, exists := qm.activeNodes[nodeID]
	if !exists {
		return NodeStateFailed
	}
	
	return node.State
}

// SetPartitionID sets the partition ID
func (qm *QuorumManager) SetPartitionID(partitionID string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	qm.partitionID = partitionID
}

// GetPartitionID returns the current partition ID
func (qm *QuorumManager) GetPartitionID() string {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	return qm.partitionID
}