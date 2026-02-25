package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CoherenceProtocol defines the cache coherence protocol
type CoherenceProtocol int

const (
	ProtocolMESI CoherenceProtocol = iota // Modified, Exclusive, Shared, Invalid
	ProtocolMOESI                          // Modified, Owner, Exclusive, Shared, Invalid
	ProtocolDragon                         // Dragon protocol
)

// CacheState represents the coherence state of a cache line
type CacheState int

const (
	StateInvalid CacheState = iota
	StateShared
	StateExclusive
	StateModified
	StateOwner // For MOESI
)

// CoherenceConfig configures the coherence protocol
type CoherenceConfig struct {
	Protocol          CoherenceProtocol
	NodeID            string
	ClusterSize       int
	HeartbeatInterval time.Duration
	InvalidationTimeout time.Duration
	EnableWriteThrough bool
	EnableWriteBack    bool
	MaxPendingOps     int
}

// CoherenceManager manages cache coherence across nodes
type CoherenceManager struct {
	config CoherenceConfig
	mu     sync.RWMutex
	
	// Cache line states
	cacheLines map[string]*CacheLine
	
	// Node tracking
	nodes      map[string]*NodeInfo
	nodeStates map[string]NodeState
	
	// Message handling
	msgQueue   chan *CoherenceMessage
	pendingOps map[string]*PendingOperation
	
	// Statistics
	stats CoherenceStats
	
	// Shutdown
	stopChan chan struct{}
}

// CacheLine represents a cache line with coherence state
type CacheLine struct {
	Key       string
	State     CacheState
	Version   int64
	Owner     string
	Sharers   map[string]bool
	Modified  bool
	Timestamp time.Time
	LockOwner string
	mu        sync.RWMutex
}

// NodeInfo contains information about a cluster node
// CoherenceMessage represents a coherence protocol message
type CoherenceMessage struct {
	Type      MessageType
	Key       string
	Value     []byte
	Version   int64
	SourceID  string
	TargetID  string
	Timestamp time.Time
	ResponseChan chan *CoherenceResponse
}

// MessageType defines coherence message types
type MessageType int

const (
	MsgReadRequest MessageType = iota
	MsgReadResponse
	MsgWriteRequest
	MsgWriteResponse
	MsgInvalidate
	MsgInvalidateAck
	MsgUpgrade
	MsgDowngrade
	MsgEviction
	MsgHeartbeat
)

// CoherenceResponse represents a response to a coherence message
type CoherenceResponse struct {
	Success   bool
	Value     []byte
	Version   int64
	State     CacheState
	Error     error
}

// PendingOperation tracks a pending coherence operation
type PendingOperation struct {
	Message      *CoherenceMessage
	StartTime    time.Time
	AckCount     int
	RequiredAcks int
	Responses    []*CoherenceResponse
	Done         chan struct{}
}

// CoherenceStats contains coherence protocol statistics
type CoherenceStats struct {
	ReadRequests      int64
	WriteRequests     int64
	Invalidations     int64
	Upgrades          int64
	Downgrades        int64
	Evictions         int64
	CoherenceMisses   int64
	NetworkMessages   int64
	AvgLatency        time.Duration
	StateTransitions  map[CacheState]int64
}

// NewCoherenceManager creates a new coherence manager
func NewCoherenceManager(config CoherenceConfig) *CoherenceManager {
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 1 * time.Second
	}
	if config.InvalidationTimeout == 0 {
		config.InvalidationTimeout = 5 * time.Second
	}
	if config.MaxPendingOps == 0 {
		config.MaxPendingOps = 1000
	}
	
	cm := &CoherenceManager{
		config:     config,
		cacheLines: make(map[string]*CacheLine),
		nodes:      make(map[string]*NodeInfo),
		nodeStates: make(map[string]NodeState),
		msgQueue:   make(chan *CoherenceMessage, 10000),
		pendingOps: make(map[string]*PendingOperation),
		stopChan:   make(chan struct{}),
		stats: CoherenceStats{
			StateTransitions: make(map[CacheState]int64),
		},
	}
	
	// Start message processor
	go cm.processMessages()
	
	// Start heartbeat
	go cm.heartbeatLoop()
	
	return cm
}

// Read performs a coherent read operation
func (cm *CoherenceManager) Read(ctx context.Context, key string) ([]byte, error) {
	cm.mu.RLock()
	line, exists := cm.cacheLines[key]
	cm.mu.RUnlock()
	
	if !exists {
		// Cache miss - request from other nodes
		return cm.handleReadMiss(ctx, key)
	}
	
	line.mu.RLock()
	defer line.mu.RUnlock()
	
	switch line.State {
	case StateInvalid:
		// Need to fetch from owner
		return cm.handleReadMiss(ctx, key)
		
	case StateShared, StateExclusive, StateModified, StateOwner:
		// Can read locally
		cm.stats.ReadRequests++
		return nil, nil // Value would be retrieved from local cache
		
	default:
		return nil, fmt.Errorf("invalid cache state: %v", line.State)
	}
}

// Write performs a coherent write operation
func (cm *CoherenceManager) Write(ctx context.Context, key string, value []byte) error {
	cm.mu.RLock()
	line, exists := cm.cacheLines[key]
	cm.mu.RUnlock()
	
	if !exists {
		// Create new cache line
		return cm.handleWriteMiss(ctx, key, value)
	}
	
	line.mu.Lock()
	defer line.mu.Unlock()
	
	switch line.State {
	case StateInvalid, StateShared:
		// Need to upgrade to exclusive/modified
		return cm.upgradeToModified(ctx, line, value)
		
	case StateExclusive, StateModified:
		// Can write locally
		line.State = StateModified
		line.Modified = true
		line.Version++
		line.Timestamp = time.Now()
		cm.stats.WriteRequests++
		cm.stats.StateTransitions[StateModified]++
		return nil
		
	case StateOwner:
		// Invalidate sharers and transition to modified
		return cm.upgradeToModified(ctx, line, value)
		
	default:
		return fmt.Errorf("invalid cache state: %v", line.State)
	}
}

// handleReadMiss handles a cache miss on read
func (cm *CoherenceManager) handleReadMiss(ctx context.Context, key string) ([]byte, error) {
	// Send read request to all nodes
	msg := &CoherenceMessage{
		Type:         MsgReadRequest,
		Key:          key,
		SourceID:     cm.config.NodeID,
		Timestamp:    time.Now(),
		ResponseChan: make(chan *CoherenceResponse, cm.config.ClusterSize),
	}
	
	// Broadcast to all nodes
	if err := cm.broadcastMessage(msg); err != nil {
		return nil, err
	}
	
	// Wait for response
	select {
	case resp := <-msg.ResponseChan:
		if resp.Success {
			// Create cache line in shared state
			cm.createCacheLine(key, resp.Value, StateShared, resp.Version)
			return resp.Value, nil
		}
		return nil, resp.Error
		
	case <-time.After(cm.config.InvalidationTimeout):
		return nil, fmt.Errorf("read request timeout")
		
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// handleWriteMiss handles a cache miss on write
func (cm *CoherenceManager) handleWriteMiss(ctx context.Context, key string, value []byte) error {
	// Send write request to all nodes
	msg := &CoherenceMessage{
		Type:         MsgWriteRequest,
		Key:          key,
		Value:        value,
		SourceID:     cm.config.NodeID,
		Timestamp:    time.Now(),
		ResponseChan: make(chan *CoherenceResponse, cm.config.ClusterSize),
	}
	
	// Broadcast to all nodes
	if err := cm.broadcastMessage(msg); err != nil {
		return err
	}
	
	// Wait for acknowledgments
	acks := 0
	required := cm.config.ClusterSize - 1
	
	timeout := time.After(cm.config.InvalidationTimeout)
	for acks < required {
		select {
		case resp := <-msg.ResponseChan:
			if resp.Success {
				acks++
			}
			
		case <-timeout:
			return fmt.Errorf("write request timeout")
			
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	
	// Create cache line in modified state
	cm.createCacheLine(key, value, StateModified, 1)
	cm.stats.WriteRequests++
	
	return nil
}

// upgradeToModified upgrades a cache line to modified state
func (cm *CoherenceManager) upgradeToModified(ctx context.Context, line *CacheLine, value []byte) error {
	// Send invalidation to all sharers
	msg := &CoherenceMessage{
		Type:         MsgInvalidate,
		Key:          line.Key,
		SourceID:     cm.config.NodeID,
		Timestamp:    time.Now(),
		ResponseChan: make(chan *CoherenceResponse, len(line.Sharers)),
	}
	
	// Send to all sharers
	for sharerID := range line.Sharers {
		msg.TargetID = sharerID
		if err := cm.sendMessage(msg); err != nil {
			return err
		}
	}
	
	// Wait for acknowledgments
	acks := 0
	required := len(line.Sharers)
	
	timeout := time.After(cm.config.InvalidationTimeout)
	for acks < required {
		select {
		case resp := <-msg.ResponseChan:
			if resp.Success {
				acks++
			}
			
		case <-timeout:
			return fmt.Errorf("invalidation timeout")
			
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	
	// Transition to modified state
	line.State = StateModified
	line.Modified = true
	line.Version++
	line.Sharers = make(map[string]bool)
	line.Timestamp = time.Now()
	
	cm.stats.Upgrades++
	cm.stats.Invalidations += int64(required)
	cm.stats.StateTransitions[StateModified]++
	
	return nil
}

// createCacheLine creates a new cache line
func (cm *CoherenceManager) createCacheLine(key string, value []byte, state CacheState, version int64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	line := &CacheLine{
		Key:       key,
		State:     state,
		Version:   version,
		Owner:     cm.config.NodeID,
		Sharers:   make(map[string]bool),
		Timestamp: time.Now(),
	}
	
	if state == StateShared {
		line.Sharers[cm.config.NodeID] = true
	}
	
	cm.cacheLines[key] = line
	cm.stats.StateTransitions[state]++
}

// Invalidate invalidates a cache line
func (cm *CoherenceManager) Invalidate(ctx context.Context, key string) error {
	cm.mu.RLock()
	line, exists := cm.cacheLines[key]
	cm.mu.RUnlock()
	
	if !exists {
		return nil
	}
	
	line.mu.Lock()
	defer line.mu.Unlock()
	
	line.State = StateInvalid
	line.Sharers = make(map[string]bool)
	cm.stats.Invalidations++
	cm.stats.StateTransitions[StateInvalid]++
	
	return nil
}

// processMessages processes incoming coherence messages
func (cm *CoherenceManager) processMessages() {
	for {
		select {
		case msg := <-cm.msgQueue:
			cm.handleMessage(msg)
			
		case <-cm.stopChan:
			return
		}
	}
}

// handleMessage handles a coherence message
func (cm *CoherenceManager) handleMessage(msg *CoherenceMessage) {
	cm.stats.NetworkMessages++
	
	switch msg.Type {
	case MsgReadRequest:
		cm.handleReadRequest(msg)
		
	case MsgWriteRequest:
		cm.handleWriteRequest(msg)
		
	case MsgInvalidate:
		cm.handleInvalidate(msg)
		
	case MsgHeartbeat:
		cm.handleHeartbeat(msg)
		
	default:
		// Handle other message types
	}
}

// handleReadRequest handles a read request from another node
func (cm *CoherenceManager) handleReadRequest(msg *CoherenceMessage) {
	cm.mu.RLock()
	line, exists := cm.cacheLines[msg.Key]
	cm.mu.RUnlock()
	
	if !exists {
		msg.ResponseChan <- &CoherenceResponse{
			Success: false,
			Error:   fmt.Errorf("key not found"),
		}
		return
	}
	
	line.mu.Lock()
	defer line.mu.Unlock()
	
	// Transition based on current state
	switch line.State {
	case StateModified, StateExclusive:
		// Downgrade to shared
		line.State = StateShared
		line.Sharers[msg.SourceID] = true
		cm.stats.Downgrades++
		cm.stats.StateTransitions[StateShared]++
		
	case StateShared:
		// Add new sharer
		line.Sharers[msg.SourceID] = true
	}
	
	msg.ResponseChan <- &CoherenceResponse{
		Success: true,
		Version: line.Version,
		State:   line.State,
	}
}

// handleWriteRequest handles a write request from another node
func (cm *CoherenceManager) handleWriteRequest(msg *CoherenceMessage) {
	cm.mu.RLock()
	line, exists := cm.cacheLines[msg.Key]
	cm.mu.RUnlock()
	
	if exists {
		line.mu.Lock()
		line.State = StateInvalid
		line.mu.Unlock()
		cm.stats.Invalidations++
	}
	
	msg.ResponseChan <- &CoherenceResponse{
		Success: true,
	}
}

// handleInvalidate handles an invalidation request
func (cm *CoherenceManager) handleInvalidate(msg *CoherenceMessage) {
	cm.Invalidate(context.Background(), msg.Key)
	
	msg.ResponseChan <- &CoherenceResponse{
		Success: true,
	}
}

// handleHeartbeat handles a heartbeat message
func (cm *CoherenceManager) handleHeartbeat(msg *CoherenceMessage) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	if node, exists := cm.nodes[msg.SourceID]; exists {
		node.LastHeartbeat = time.Now()
		node.State = NodeStateActive
	}
}

// broadcastMessage broadcasts a message to all nodes
func (cm *CoherenceManager) broadcastMessage(msg *CoherenceMessage) error {
	cm.mu.RLock()
	nodes := make([]*NodeInfo, 0, len(cm.nodes))
	for _, node := range cm.nodes {
		if node.ID != cm.config.NodeID {
			nodes = append(nodes, node)
		}
	}
	cm.mu.RUnlock()
	
	for _, node := range nodes {
		msg.TargetID = node.ID
		if err := cm.sendMessage(msg); err != nil {
			return err
		}
	}
	
	return nil
}

// sendMessage sends a message to a specific node
func (cm *CoherenceManager) sendMessage(msg *CoherenceMessage) error {
	// In production, this would send over network
	// For now, just queue locally
	select {
	case cm.msgQueue <- msg:
		return nil
	default:
		return fmt.Errorf("message queue full")
	}
}

// heartbeatLoop sends periodic heartbeats
func (cm *CoherenceManager) heartbeatLoop() {
	ticker := time.NewTicker(cm.config.HeartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			cm.sendHeartbeat()
			cm.checkNodeHealth()
			
		case <-cm.stopChan:
			return
		}
	}
}

// sendHeartbeat sends a heartbeat to all nodes
func (cm *CoherenceManager) sendHeartbeat() {
	msg := &CoherenceMessage{
		Type:      MsgHeartbeat,
		SourceID:  cm.config.NodeID,
		Timestamp: time.Now(),
	}
	
	cm.broadcastMessage(msg)
}

// checkNodeHealth checks the health of all nodes
func (cm *CoherenceManager) checkNodeHealth() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	now := time.Now()
	timeout := cm.config.HeartbeatInterval * 3
	
	for _, node := range cm.nodes {
		if now.Sub(node.LastHeartbeat) > timeout {
			if node.State == NodeStateActive {
				node.State = NodeStateSuspect
			} else if node.State == NodeStateSuspect {
				node.State = NodeStateFailed
				cm.handleNodeFailure(node)
			}
		}
	}
}

// handleNodeFailure handles a node failure
func (cm *CoherenceManager) handleNodeFailure(node *NodeInfo) {
	// Invalidate all cache lines owned by failed node
	for _, line := range cm.cacheLines {
		line.mu.Lock()
		if line.Owner == node.ID {
			line.State = StateInvalid
		}
		delete(line.Sharers, node.ID)
		line.mu.Unlock()
	}
}

// GetStats returns coherence statistics
func (cm *CoherenceManager) GetStats() CoherenceStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	
	return cm.stats
}

// Stop stops the coherence manager
func (cm *CoherenceManager) Stop() {
	select {
	case <-cm.stopChan:
		return // already closed
	default:
		close(cm.stopChan)
	}
}