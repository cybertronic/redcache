package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ConflictResolver resolves conflicts between cache entries
type ConflictResolver struct {
	config         *ClusterConfig
	clusterManager *ClusterManager
	rpcClient      *GRPCClient
	resolutionLog  []ConflictResolution
	logMu          sync.RWMutex
}

// ConflictResolution represents a resolved conflict
type ConflictResolution struct {
	Key           string
	Timestamp     time.Time
	Strategy      ResolutionStrategy
	WinningNode   string
	WinningValue  []byte
	ConflictNodes []string
}

// ResolutionStrategy represents a conflict resolution strategy
type ResolutionStrategy int

const (
	StrategyLastWriteWins ResolutionStrategy = iota
	StrategyHighestVersion
	StrategyQuorumVote
	StrategyCustom
)

// NewConflictResolver creates a new conflict resolver
func NewConflictResolver(config *ClusterConfig, cm *ClusterManager, client *GRPCClient) *ConflictResolver {
	return &ConflictResolver{
		config:         config,
		clusterManager: cm,
		rpcClient:      client,
		resolutionLog:  make([]ConflictResolution, 0),
	}
}

// ResolveConflict resolves a conflict for a given key
func (cr *ConflictResolver) ResolveConflict(ctx context.Context, key string) (*ConflictResolution, error) {
	// Get all nodes
	nodes := cr.clusterManager.GetAliveNodes()
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}

	// Gather values from all nodes
	values, err := cr.gatherValues(ctx, key, nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to gather values: %w", err)
	}

	// If no conflict, return
	if len(values) <= 1 {
		return nil, nil
	}

	// Resolve based on strategy
	var resolution *ConflictResolution
	switch cr.config.ConflictResolution {
	case "last-write-wins":
		resolution = cr.resolveLastWriteWins(key, values)
	case "highest-version":
		resolution = cr.resolveHighestVersion(key, values)
	case "quorum":
		resolution = cr.resolveQuorum(key, values)
	default:
		resolution = cr.resolveLastWriteWins(key, values)
	}

	// Log the resolution
	cr.logResolution(resolution)

	// Propagate the winning value
	if err := cr.propagateValue(ctx, key, resolution.WinningValue, nodes); err != nil {
		return nil, fmt.Errorf("failed to propagate value: %w", err)
	}

	return resolution, nil
}

// gatherValues gathers values for a key from all nodes
func (cr *ConflictResolver) gatherValues(ctx context.Context, key string, nodes []*NodeInfo) (map[string]*NodeValue, error) {
	values := make(map[string]*NodeValue)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(n *NodeInfo) {
			defer wg.Done()

			value, err := cr.rpcClient.GetValue(ctx, n.Address, n.Port, key)
			if err != nil {
				return
			}

			mu.Lock()
			values[n.ID] = &NodeValue{
				NodeID:    n.ID,
				Value:     value,
				Timestamp: time.Now(),
			}
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	return values, nil
}

// NodeValue represents a value from a specific node
type NodeValue struct {
	NodeID    string
	Value     []byte
	Timestamp time.Time
	Version   uint64
}

// resolveLastWriteWins resolves conflict using last-write-wins strategy
func (cr *ConflictResolver) resolveLastWriteWins(key string, values map[string]*NodeValue) *ConflictResolution {
	var winner *NodeValue
	var winnerNodeID string

	for nodeID, value := range values {
		if winner == nil || value.Timestamp.After(winner.Timestamp) {
			winner = value
			winnerNodeID = nodeID
		}
	}

	conflictNodes := make([]string, 0, len(values))
	for nodeID := range values {
		if nodeID != winnerNodeID {
			conflictNodes = append(conflictNodes, nodeID)
		}
	}

	return &ConflictResolution{
		Key:           key,
		Timestamp:     time.Now(),
		Strategy:      StrategyLastWriteWins,
		WinningNode:   winnerNodeID,
		WinningValue:  winner.Value,
		ConflictNodes: conflictNodes,
	}
}

// resolveHighestVersion resolves conflict using highest version strategy
func (cr *ConflictResolver) resolveHighestVersion(key string, values map[string]*NodeValue) *ConflictResolution {
	var winner *NodeValue
	var winnerNodeID string

	for nodeID, value := range values {
		if winner == nil || value.Version > winner.Version {
			winner = value
			winnerNodeID = nodeID
		}
	}

	conflictNodes := make([]string, 0, len(values))
	for nodeID := range values {
		if nodeID != winnerNodeID {
			conflictNodes = append(conflictNodes, nodeID)
		}
	}

	return &ConflictResolution{
		Key:           key,
		Timestamp:     time.Now(),
		Strategy:      StrategyHighestVersion,
		WinningNode:   winnerNodeID,
		WinningValue:  winner.Value,
		ConflictNodes: conflictNodes,
	}
}

// resolveQuorum resolves conflict using quorum voting strategy
func (cr *ConflictResolver) resolveQuorum(key string, values map[string]*NodeValue) *ConflictResolution {
	// Count votes for each unique value
	votes := make(map[string]int)
	valueMap := make(map[string][]byte)
	nodeMap := make(map[string][]string)

	for nodeID, value := range values {
		valueKey := string(value.Value)
		votes[valueKey]++
		valueMap[valueKey] = value.Value
		nodeMap[valueKey] = append(nodeMap[valueKey], nodeID)
	}

	// Find the value with most votes
	var winningValue []byte
	var winningNodes []string
	maxVotes := 0

	for valueKey, count := range votes {
		if count > maxVotes {
			maxVotes = count
			winningValue = valueMap[valueKey]
			winningNodes = nodeMap[valueKey]
		}
	}

	// Determine conflict nodes
	conflictNodes := make([]string, 0)
	for nodeID := range values {
		found := false
		for _, wn := range winningNodes {
			if nodeID == wn {
				found = true
				break
			}
		}
		if !found {
			conflictNodes = append(conflictNodes, nodeID)
		}
	}

	winnerNodeID := ""
	if len(winningNodes) > 0 {
		winnerNodeID = winningNodes[0]
	}

	return &ConflictResolution{
		Key:           key,
		Timestamp:     time.Now(),
		Strategy:      StrategyQuorumVote,
		WinningNode:   winnerNodeID,
		WinningValue:  winningValue,
		ConflictNodes: conflictNodes,
	}
}

// propagateValue propagates the winning value to all nodes
func (cr *ConflictResolver) propagateValue(ctx context.Context, key string, value []byte, nodes []*NodeInfo) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(nodes))

	for _, node := range nodes {
		wg.Add(1)
		go func(n *NodeInfo) {
			defer wg.Done()

			if err := cr.rpcClient.SetValue(ctx, n.Address, n.Port, key, value, 0); err != nil {
				errCh <- fmt.Errorf("failed to propagate to node %s: %w", n.ID, err)
			}
		}(node)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// logResolution logs a conflict resolution
func (cr *ConflictResolver) logResolution(resolution *ConflictResolution) {
	cr.logMu.Lock()
	defer cr.logMu.Unlock()

	cr.resolutionLog = append(cr.resolutionLog, *resolution)

	// Keep only last 1000 resolutions
	if len(cr.resolutionLog) > 1000 {
		cr.resolutionLog = cr.resolutionLog[len(cr.resolutionLog)-1000:]
	}
}

// GetResolutionLog returns the conflict resolution log
func (cr *ConflictResolver) GetResolutionLog() []ConflictResolution {
	cr.logMu.RLock()
	defer cr.logMu.RUnlock()

	log := make([]ConflictResolution, len(cr.resolutionLog))
	copy(log, cr.resolutionLog)
	return log
}

// GetResolutionStats returns statistics about conflict resolutions
func (cr *ConflictResolver) GetResolutionStats() map[string]any {
	cr.logMu.RLock()
	defer cr.logMu.RUnlock()

	stats := make(map[string]any)
	stats["total_resolutions"] = len(cr.resolutionLog)

	strategyCount := make(map[ResolutionStrategy]int)
	for _, res := range cr.resolutionLog {
		strategyCount[res.Strategy]++
	}

	stats["by_strategy"] = strategyCount
	return stats
}