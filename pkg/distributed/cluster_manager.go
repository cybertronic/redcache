package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// ClusterManager manages cluster membership and coordination
type ClusterManager struct {
	config      *ClusterConfig
	memberlist  *memberlist.Memberlist
	localNode   *NodeInfo
	nodes       map[string]*NodeInfo
	nodesMu     sync.RWMutex
	eventCh     chan memberlist.NodeEvent
	shutdownCh  chan struct{}
	wg          sync.WaitGroup
	rebalancer  *Rebalancer
}

// NodeInfo represents information about a cluster node
// NodeInfo represents information about a cluster node
type NodeInfo struct {
	ID            string
	Address       string
	Port          int
	State         NodeState
	LastSeen      time.Time
	LastHeartbeat time.Time
	Metadata      map[string]string
	IsLeader      bool
	Term          uint64
}

// NodeState represents the state of a node
type NodeState int

const (
	NodeStateUnknown NodeState = iota
	NodeStateAlive
	NodeStateSuspect
	NodeStateDead
	NodeStateLeft
	NodeStateActive
	NodeStateFailed
)

// NewClusterManager creates a new cluster manager
func NewClusterManager(config *ClusterConfig) (*ClusterManager, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	cm := &ClusterManager{
		config:     config,
		nodes:      make(map[string]*NodeInfo),
		eventCh:    make(chan memberlist.NodeEvent, 100),
		shutdownCh: make(chan struct{}),
	}

	// Initialize local node info
	cm.localNode = &NodeInfo{
		ID:       config.NodeID,
		Address:  config.BindAddr,
		Port:     config.BindPort,
		State:    NodeStateAlive,
		LastSeen: time.Now(),
		Metadata: make(map[string]string),
	}

	// Configure memberlist
	mlConfig := memberlist.DefaultLANConfig()
	mlConfig.Name = config.NodeID
	mlConfig.BindAddr = config.BindAddr
	mlConfig.BindPort = config.BindPort
	mlConfig.Events = &memberlist.ChannelEventDelegate{Ch: cm.eventCh}

	// Create memberlist
	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}
	cm.memberlist = ml

	// Join cluster if seeds are provided
	if len(config.Seeds) > 0 {
		_, err := ml.Join(config.Seeds)
		if err != nil {
			return nil, fmt.Errorf("failed to join cluster: %w", err)
		}
	}

	// Start event processing
	cm.wg.Add(1)
	go cm.processEvents()

	return cm, nil
}

// processEvents processes cluster membership events
func (cm *ClusterManager) processEvents() {
	defer cm.wg.Done()

	for {
		select {
		case event := <-cm.eventCh:
			cm.handleEvent(event)
		case <-cm.shutdownCh:
			return
		}
	}
}

// handleEvent handles a cluster membership event
func (cm *ClusterManager) handleEvent(event memberlist.NodeEvent) {
	cm.nodesMu.Lock()
	defer cm.nodesMu.Unlock()

	node := event.Node
	nodeID := node.Name

	switch event.Event {
	case memberlist.NodeJoin:
		cm.nodes[nodeID] = &NodeInfo{
			ID:       nodeID,
			Address:  node.Addr.String(),
			Port:     int(node.Port),
			State:    NodeStateAlive,
			LastSeen: time.Now(),
			Metadata: make(map[string]string),
		}
		
		// Trigger rebalancing for new node
		if cm.rebalancer != nil && cm.config.EnableAutoRebalance {
			go cm.rebalancer.OnNodeJoin(nodeID)
		}

	case memberlist.NodeLeave:
		if info, exists := cm.nodes[nodeID]; exists {
			info.State = NodeStateLeft
			info.LastSeen = time.Now()
		}
		
		// Trigger rebalancing for leaving node
		if cm.rebalancer != nil && cm.config.EnableAutoRebalance {
			go cm.rebalancer.OnNodeLeave(nodeID)
		}

	case memberlist.NodeUpdate:
		if info, exists := cm.nodes[nodeID]; exists {
			info.LastSeen = time.Now()
		}
	}
}

// GetNodes returns all known nodes
func (cm *ClusterManager) GetNodes() []*NodeInfo {
	cm.nodesMu.RLock()
	defer cm.nodesMu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(cm.nodes))
	for _, node := range cm.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetAliveNodes returns all alive nodes
func (cm *ClusterManager) GetAliveNodes() []*NodeInfo {
	cm.nodesMu.RLock()
	defer cm.nodesMu.RUnlock()

	nodes := make([]*NodeInfo, 0)
	for _, node := range cm.nodes {
		if node.State == NodeStateAlive {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// GetNode returns information about a specific node
func (cm *ClusterManager) GetNode(nodeID string) (*NodeInfo, bool) {
	cm.nodesMu.RLock()
	defer cm.nodesMu.RUnlock()

	node, exists := cm.nodes[nodeID]
	return node, exists
}

// IsLeader returns whether the local node is the leader
func (cm *ClusterManager) IsLeader() bool {
	return cm.localNode.IsLeader
}

// GetLeader returns the current leader node
func (cm *ClusterManager) GetLeader() *NodeInfo {
	cm.nodesMu.RLock()
	defer cm.nodesMu.RUnlock()

	for _, node := range cm.nodes {
		if node.IsLeader {
			return node
		}
	}
	return nil
}

// PromoteToLeader promotes the local node to leader
func (cm *ClusterManager) PromoteToLeader(term uint64) {
	cm.localNode.IsLeader = true
	cm.localNode.Term = term
}

// DemoteFromLeader demotes the local node from leader
func (cm *ClusterManager) DemoteFromLeader() {
	cm.localNode.IsLeader = false
}

// GetQuorumSize returns the minimum number of nodes needed for quorum
func (cm *ClusterManager) GetQuorumSize() int {
	totalNodes := len(cm.GetAliveNodes()) + 1 // +1 for local node
	return (totalNodes / 2) + 1
}

// HasQuorum checks if we have quorum
func (cm *ClusterManager) HasQuorum() bool {
	aliveNodes := len(cm.GetAliveNodes()) + 1 // +1 for local node
	quorumSize := cm.GetQuorumSize()
	return aliveNodes >= quorumSize
}

// Shutdown gracefully shuts down the cluster manager
func (cm *ClusterManager) Shutdown(ctx context.Context) error {
	select {
	case <-cm.shutdownCh:
		return nil // already closed
	default:
		close(cm.shutdownCh)
	}
	
	// Leave the cluster
	if err := cm.memberlist.Leave(time.Second * 5); err != nil {
		return fmt.Errorf("failed to leave cluster: %w", err)
	}

	// Shutdown memberlist
	if err := cm.memberlist.Shutdown(); err != nil {
		return fmt.Errorf("failed to shutdown memberlist: %w", err)
	}

	// Wait for goroutines
	done := make(chan struct{})
	go func() {
		cm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
// SetRebalancer sets the rebalancer for the cluster manager
func (cm *ClusterManager) SetRebalancer(rebalancer *Rebalancer) {
	cm.nodesMu.Lock()
	defer cm.nodesMu.Unlock()
	cm.rebalancer = rebalancer
}

// GetRebalancer returns the current rebalancer
func (cm *ClusterManager) GetRebalancer() *Rebalancer {
	cm.nodesMu.RLock()
	defer cm.nodesMu.RUnlock()
	return cm.rebalancer
}
