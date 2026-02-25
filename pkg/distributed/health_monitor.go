package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// HealthMonitor monitors the health of cluster nodes
type HealthMonitor struct {
	config         *ClusterConfig
	clusterManager *ClusterManager
	rpcClient      *GRPCClient
	healthChecks   map[string]*HealthCheck
	healthMu       sync.RWMutex
	shutdownCh     chan struct{}
	wg             sync.WaitGroup
}

// HealthCheck represents a health check for a node
type HealthCheck struct {
	NodeID          string
	LastCheck       time.Time
	LastSuccess     time.Time
	ConsecutiveFails int
	Status          HealthStatus
	Latency         time.Duration
}

// HealthStatus represents the health status of a node
type HealthStatus int

const (
	HealthStatusUnknown HealthStatus = iota
	HealthStatusHealthy
	HealthStatusDegraded
	HealthStatusUnhealthy
)

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(config *ClusterConfig, cm *ClusterManager, client *GRPCClient) *HealthMonitor {
	return &HealthMonitor{
		config:         config,
		clusterManager: cm,
		rpcClient:      client,
		healthChecks:   make(map[string]*HealthCheck),
		shutdownCh:     make(chan struct{}),
	}
}

// Start starts the health monitoring
func (hm *HealthMonitor) Start() {
	hm.wg.Add(1)
	go hm.monitorLoop()
}

// monitorLoop runs the health monitoring loop
func (hm *HealthMonitor) monitorLoop() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.performHealthChecks()
		case <-hm.shutdownCh:
			return
		}
	}
}

// performHealthChecks performs health checks on all nodes
func (hm *HealthMonitor) performHealthChecks() {
	nodes := hm.clusterManager.GetAliveNodes()

	for _, node := range nodes {
		hm.checkNode(node)
	}

	// Clean up old health checks
	hm.cleanupHealthChecks(nodes)
}

// checkNode performs a health check on a specific node
func (hm *HealthMonitor) checkNode(node *NodeInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), hm.config.HealthCheckTimeout)
	defer cancel()

	start := time.Now()
	err := hm.rpcClient.PingNode(ctx, node.Address, node.Port)
	latency := time.Since(start)

	hm.healthMu.Lock()
	defer hm.healthMu.Unlock()

	check, exists := hm.healthChecks[node.ID]
	if !exists {
		check = &HealthCheck{
			NodeID: node.ID,
		}
		hm.healthChecks[node.ID] = check
	}

	check.LastCheck = time.Now()
	check.Latency = latency

	if err != nil {
		check.ConsecutiveFails++
		check.Status = hm.determineStatus(check.ConsecutiveFails)
	} else {
		check.LastSuccess = time.Now()
		check.ConsecutiveFails = 0
		check.Status = HealthStatusHealthy
	}
}

// determineStatus determines health status based on consecutive failures
func (hm *HealthMonitor) determineStatus(consecutiveFails int) HealthStatus {
	if consecutiveFails >= 3 {
		return HealthStatusUnhealthy
	} else if consecutiveFails >= 1 {
		return HealthStatusDegraded
	}
	return HealthStatusHealthy
}

// cleanupHealthChecks removes health checks for nodes that no longer exist
func (hm *HealthMonitor) cleanupHealthChecks(currentNodes []*NodeInfo) {
	hm.healthMu.Lock()
	defer hm.healthMu.Unlock()

	nodeMap := make(map[string]bool)
	for _, node := range currentNodes {
		nodeMap[node.ID] = true
	}

	for nodeID := range hm.healthChecks {
		if !nodeMap[nodeID] {
			delete(hm.healthChecks, nodeID)
		}
	}
}

// GetHealthCheck returns the health check for a specific node
func (hm *HealthMonitor) GetHealthCheck(nodeID string) (*HealthCheck, bool) {
	hm.healthMu.RLock()
	defer hm.healthMu.RUnlock()

	check, exists := hm.healthChecks[nodeID]
	return check, exists
}

// GetAllHealthChecks returns all health checks
func (hm *HealthMonitor) GetAllHealthChecks() map[string]*HealthCheck {
	hm.healthMu.RLock()
	defer hm.healthMu.RUnlock()

	checks := make(map[string]*HealthCheck)
	for k, v := range hm.healthChecks {
		checks[k] = v
	}
	return checks
}

// IsNodeHealthy checks if a node is healthy
func (hm *HealthMonitor) IsNodeHealthy(nodeID string) bool {
	check, exists := hm.GetHealthCheck(nodeID)
	if !exists {
		return false
	}
	return check.Status == HealthStatusHealthy
}

// GetHealthyNodes returns all healthy nodes
func (hm *HealthMonitor) GetHealthyNodes() []*NodeInfo {
	nodes := hm.clusterManager.GetAliveNodes()
	healthy := make([]*NodeInfo, 0)

	for _, node := range nodes {
		if hm.IsNodeHealthy(node.ID) {
			healthy = append(healthy, node)
		}
	}

	return healthy
}

// DetectSplitBrain detects potential split-brain scenarios
func (hm *HealthMonitor) DetectSplitBrain() (bool, error) {
	// Get all nodes
	allNodes := hm.clusterManager.GetAliveNodes()
	if len(allNodes) == 0 {
		return false, nil
	}

	// Check if we have quorum
	hasQuorum := hm.clusterManager.HasQuorum()
	if !hasQuorum {
		return true, fmt.Errorf("cluster does not have quorum")
	}

	// Check for network partitions
	healthyNodes := hm.GetHealthyNodes()
	unhealthyCount := len(allNodes) - len(healthyNodes)

	// If more than half of nodes are unhealthy, potential split-brain
	if unhealthyCount > len(allNodes)/2 {
		return true, fmt.Errorf("potential network partition detected: %d/%d nodes unhealthy",
			unhealthyCount, len(allNodes))
	}

	// Check for multiple leaders
	leaderCount := 0
	for _, node := range allNodes {
		if node.IsLeader {
			leaderCount++
		}
	}

	if leaderCount > 1 {
		return true, fmt.Errorf("multiple leaders detected: %d leaders", leaderCount)
	}

	return false, nil
}

// Shutdown gracefully shuts down the health monitor
func (hm *HealthMonitor) Shutdown(ctx context.Context) error {
	select {
	case <-hm.shutdownCh:
		return nil // already closed
	default:
		close(hm.shutdownCh)
	}

	done := make(chan struct{})
	go func() {
		hm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}