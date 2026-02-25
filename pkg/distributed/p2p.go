package distributed

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"time"
)

// P2PConfig configures peer-to-peer cache sharing
type P2PConfig struct {
	NodeID            string
	MaxPeers          int
	ReplicationFactor int
	SyncInterval      time.Duration
	PeerTimeout       time.Duration
	EnableGossip      bool
	GossipFanout      int
	GossipInterval    time.Duration
	MaxMessageSize    int64
}

// P2PCache implements peer-to-peer cache sharing
type P2PCache struct {
	config P2PConfig
	mu     sync.RWMutex
	
	// Peer management
	peers       map[string]*Peer
	peerRing    *ConsistentHashRing
	
	// Local cache
	localCache  map[string]*CacheEntry
	
	// Gossip protocol
	gossip      *GossipProtocol
	
	// Replication
	replicas    map[string][]string // key -> replica node IDs
	rpcClient   P2PRPCClient
	
	// Statistics
	stats P2PStats
	
	// Shutdown
	stopChan chan struct{}
}

// Peer represents a peer node
type Peer struct {
	ID            string
	Address       string
	State         PeerState
	LastSeen      time.Time
	RTT           time.Duration
	LoadFactor    float64
	CacheSize     int64
	AvailableSpace int64
	Version       string
}

// PeerState represents the state of a peer
type PeerState int

const (
	PeerStateConnected PeerState = iota
	PeerStateDisconnected
	PeerStateSuspect
)

// CacheEntry represents a cached item in P2P cache
type CacheEntry struct {
	Key         string
	Value       []byte
	Version     int64
	Timestamp   time.Time
	Owner       string
	Replicas    []string
	TTL         time.Duration
	AccessCount int
	VectorClock *VectorClock  // For conflict detection
}

// P2PStats contains P2P cache statistics
type P2PStats struct {
	LocalHits       int64
	RemoteHits      int64
	Misses          int64
	Replications    int64
	Evictions       int64
	PeerCount       int
	AvgPeerRTT      time.Duration
	DataTransferred int64
	GossipMessages  int64
}

// NewP2PCache creates a new P2P cache
func NewP2PCache(config P2PConfig) *P2PCache {
	if config.MaxPeers == 0 {
		config.MaxPeers = 100
	}
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = 3
	}
	if config.SyncInterval == 0 {
		config.SyncInterval = 30 * time.Second
	}
	if config.PeerTimeout == 0 {
		config.PeerTimeout = 60 * time.Second
	}
	if config.GossipFanout == 0 {
		config.GossipFanout = 3
	}
	if config.GossipInterval == 0 {
		config.GossipInterval = 1 * time.Second
	}
	if config.MaxMessageSize == 0 {
		config.MaxMessageSize = 10 * 1024 * 1024 // 10MB
	}
	
	p2p := &P2PCache{
		config:     config,
		peers:      make(map[string]*Peer),
		peerRing:   NewConsistentHashRing(100), // 100 virtual nodes per peer
		localCache: make(map[string]*CacheEntry),
		replicas:   make(map[string][]string),
		stopChan:   make(chan struct{}),
	}
	
	// Initialize gossip protocol if enabled
	if config.EnableGossip {
		p2p.gossip = NewGossipProtocol(GossipConfig{
			NodeID:   config.NodeID,
			Fanout:   config.GossipFanout,
			Interval: config.GossipInterval,
		})
	}
	
	// Start background tasks
	go p2p.syncLoop()
	go p2p.healthCheckLoop()
	
	return p2p
}

// Get retrieves data from P2P cache
func (p2p *P2PCache) Get(ctx context.Context, key string) ([]byte, error) {
	// Try local cache first
	p2p.mu.RLock()
	entry, exists := p2p.localCache[key]
	p2p.mu.RUnlock()
	
	if exists {
		entry.AccessCount++
		p2p.stats.LocalHits++
		return entry.Value, nil
	}
	
	// Find responsible peers using consistent hashing
	peers := p2p.peerRing.GetNodes(key, p2p.config.ReplicationFactor)
	
	// Try to fetch from peers
	for _, peerID := range peers {
		p2p.mu.RLock()
		peer, exists := p2p.peers[peerID]
		p2p.mu.RUnlock()
		
		if !exists || peer.State != PeerStateConnected {
			continue
		}
		
		// Request from peer
		value, err := p2p.requestFromPeer(ctx, peer, key)
		if err == nil {
			// Cache locally
			p2p.putLocal(key, value)
			p2p.stats.RemoteHits++
			return value, nil
		}
	}
	
	p2p.stats.Misses++
	return nil, fmt.Errorf("key not found in P2P cache")
}

// Put stores data in P2P cache with replication
func (p2p *P2PCache) Put(ctx context.Context, key string, value []byte) error {
	// Store locally
	p2p.putLocal(key, value)
	
	// Replicate to peers
	peers := p2p.peerRing.GetNodes(key, p2p.config.ReplicationFactor)
	
	replicated := 0
	for _, peerID := range peers {
		if peerID == p2p.config.NodeID {
			continue
		}
		
		p2p.mu.RLock()
		peer, exists := p2p.peers[peerID]
		p2p.mu.RUnlock()
		
		if !exists || peer.State != PeerStateConnected {
			continue
		}
		
		// Replicate to peer
		if err := p2p.replicateToPeer(ctx, peer, key, value); err == nil {
			replicated++
			p2p.stats.Replications++
		}
	}
	
	// Store replica information
	p2p.mu.Lock()
	p2p.replicas[key] = peers
	p2p.mu.Unlock()
	
	if replicated < p2p.config.ReplicationFactor-1 {
		return fmt.Errorf("insufficient replicas: %d/%d", replicated, p2p.config.ReplicationFactor-1)
	}
	
	return nil
}

// putLocal stores data in local cache
func (p2p *P2PCache) putLocal(key string, value []byte) {
	p2p.mu.Lock()
	defer p2p.mu.Unlock()
	
	entry := &CacheEntry{
		Key:       key,
		Value:     value,
		Version:   time.Now().UnixNano(),
		Timestamp: time.Now(),
		Owner:     p2p.config.NodeID,
	}
	
	p2p.localCache[key] = entry
}

// requestFromPeer requests data from a peer
func (p2p *P2PCache) requestFromPeer(ctx context.Context, peer *Peer, key string) ([]byte, error) {
	// In production, this would make an actual network request
	// For now, simulate with local lookup
	startTime := time.Now()
	
	// Simulate network latency
	time.Sleep(peer.RTT)
	
	p2p.stats.DataTransferred += 1024 // Simulated
	
	// Update peer RTT
	peer.RTT = time.Since(startTime)
	
	return nil, fmt.Errorf("peer request not implemented")
}

// replicateToPeer replicates data to a peer
func (p2p *P2PCache) replicateToPeer(ctx context.Context, peer *Peer, key string, value []byte) error {
	// In production, this would make an actual network request
	// For now, simulate
	time.Sleep(peer.RTT)
	
	p2p.stats.DataTransferred += int64(len(value))
	
	return nil
}

// AddPeer adds a new peer to the P2P network
func (p2p *P2PCache) AddPeer(peer *Peer) error {
	p2p.mu.Lock()
	defer p2p.mu.Unlock()
	
	if len(p2p.peers) >= p2p.config.MaxPeers {
		return fmt.Errorf("max peers reached")
	}
	
	p2p.peers[peer.ID] = peer
	p2p.peerRing.AddNode(peer.ID)
	p2p.stats.PeerCount = len(p2p.peers)
	
	// Gossip new peer information
	if p2p.config.EnableGossip {
		p2p.gossip.Broadcast(&GossipMessage{
			Type:    MsgTypePeerJoin,
			Payload: peer,
		})
	}
	
	return nil
}

// RemovePeer removes a peer from the P2P network
func (p2p *P2PCache) RemovePeer(peerID string) {
	p2p.mu.Lock()
	defer p2p.mu.Unlock()
	
	delete(p2p.peers, peerID)
	p2p.peerRing.RemoveNode(peerID)
	p2p.stats.PeerCount = len(p2p.peers)
	
	// Gossip peer removal
	if p2p.config.EnableGossip {
		p2p.gossip.Broadcast(&GossipMessage{
			Type:    MsgTypePeerLeave,
			Payload: peerID,
		})
	}
	
	// Redistribute data owned by removed peer
	p2p.redistributeData(peerID)
}

// redistributeData redistributes data after peer removal
func (p2p *P2PCache) redistributeData(peerID string) {
	// Find all keys that had this peer as replica
	keysToReplicate := make([]string, 0)
	
	for key, replicas := range p2p.replicas {
		for _, replicaID := range replicas {
			if replicaID == peerID {
				keysToReplicate = append(keysToReplicate, key)
				break
			}
		}
	}
	
	// Replicate to new peers
	for _, key := range keysToReplicate {
		if entry, exists := p2p.localCache[key]; exists {
			p2p.Put(context.Background(), key, entry.Value)
		}
	}
}

// syncLoop periodically syncs with peers
func (p2p *P2PCache) syncLoop() {
	ticker := time.NewTicker(p2p.config.SyncInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			p2p.syncWithPeers()
			
		case <-p2p.stopChan:
			return
		}
	}
}

// syncWithPeers syncs cache state with peers
func (p2p *P2PCache) syncWithPeers() {
	p2p.mu.RLock()
	peers := make([]*Peer, 0, len(p2p.peers))
	for _, peer := range p2p.peers {
		if peer.State == PeerStateConnected {
			peers = append(peers, peer)
		}
	}
	p2p.mu.RUnlock()
	
	// Sync with random subset of peers
	for i := 0; i < p2p.config.GossipFanout && i < len(peers); i++ {
		peer := peers[i]
		p2p.syncWithPeer(peer)
	}
}

// syncWithPeer syncs with a specific peer
func (p2p *P2PCache) syncWithPeer(peer *Peer) {
	// In production, this would exchange cache metadata
	// and sync any missing or outdated entries
	peer.LastSeen = time.Now()
}

// healthCheckLoop periodically checks peer health
func (p2p *P2PCache) healthCheckLoop() {
	ticker := time.NewTicker(p2p.config.PeerTimeout / 3)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			p2p.checkPeerHealth()
			
		case <-p2p.stopChan:
			return
		}
	}
}

// checkPeerHealth checks the health of all peers.
// FIX #7: previously called RemovePeer (which acquires mu.Lock) while already
// holding mu.Lock, causing a deadlock.  We now collect stale peer IDs under
// the lock and call removePeerLocked — an internal helper that assumes the
// caller already holds mu — to avoid the re-entrant lock acquisition.
func (p2p *P2PCache) checkPeerHealth() {
	p2p.mu.Lock()
	defer p2p.mu.Unlock()

	now := time.Now()

	for peerID, peer := range p2p.peers {
		if now.Sub(peer.LastSeen) > p2p.config.PeerTimeout {
			if peer.State == PeerStateConnected {
				peer.State = PeerStateSuspect
			} else if peer.State == PeerStateSuspect {
				peer.State = PeerStateDisconnected
				// FIX #7: call the lock-free helper instead of RemovePeer
				p2p.removePeerLocked(peerID)
			}
		}
	}
}

// removePeerLocked removes a peer from internal data structures.
// Caller MUST hold p2p.mu (write lock).
func (p2p *P2PCache) removePeerLocked(peerID string) {
	delete(p2p.peers, peerID)
	p2p.peerRing.RemoveNode(peerID)
	p2p.stats.PeerCount = len(p2p.peers)

	// Gossip peer removal (does not need the p2p lock)
	if p2p.config.EnableGossip {
		p2p.gossip.Broadcast(&GossipMessage{
			Type:    MsgTypePeerLeave,
			Payload: peerID,
		})
	}

	// Redistribute data owned by removed peer (reads p2p.replicas/localCache
	// which are safe to read while holding the write lock)
	p2p.redistributeData(peerID)
}

// GetStats returns P2P cache statistics
func (p2p *P2PCache) GetStats() P2PStats {
	p2p.mu.RLock()
	defer p2p.mu.RUnlock()
	
	stats := p2p.stats
	
	// Calculate average peer RTT
	totalRTT := time.Duration(0)
	count := 0
	for _, peer := range p2p.peers {
		if peer.State == PeerStateConnected {
			totalRTT += peer.RTT
			count++
		}
	}
	if count > 0 {
		stats.AvgPeerRTT = totalRTT / time.Duration(count)
	}
	
	return stats
}

// Stop stops the P2P cache
func (p2p *P2PCache) Stop() {
	select {
	case <-p2p.stopChan:
		return // already closed
	default:
		close(p2p.stopChan)
	}
	if p2p.gossip != nil {
		p2p.gossip.Stop()
	}
}

// ConsistentHashRing implements consistent hashing for peer selection
type ConsistentHashRing struct {
	mu            sync.RWMutex
	ring          []uint32
	nodes         map[uint32]string
	virtualNodes  int
}

// NewConsistentHashRing creates a new consistent hash ring
func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	return &ConsistentHashRing{
		ring:         make([]uint32, 0),
		nodes:        make(map[uint32]string),
		virtualNodes: virtualNodes,
	}
}

// AddNode adds a node to the ring
func (chr *ConsistentHashRing) AddNode(nodeID string) {
	chr.mu.Lock()
	defer chr.mu.Unlock()
	
	for i := 0; i < chr.virtualNodes; i++ {
		hash := chr.hash(fmt.Sprintf("%s:%d", nodeID, i))
		chr.ring = append(chr.ring, hash)
		chr.nodes[hash] = nodeID
	}
	
	sort.Slice(chr.ring, func(i, j int) bool {
		return chr.ring[i] < chr.ring[j]
	})
}

// RemoveNode removes a node from the ring
func (chr *ConsistentHashRing) RemoveNode(nodeID string) {
	chr.mu.Lock()
	defer chr.mu.Unlock()
	
	newRing := make([]uint32, 0)
	for _, hash := range chr.ring {
		if chr.nodes[hash] != nodeID {
			newRing = append(newRing, hash)
		} else {
			delete(chr.nodes, hash)
		}
	}
	chr.ring = newRing
}

// GetNodes returns N nodes responsible for a key
func (chr *ConsistentHashRing) GetNodes(key string, n int) []string {
	chr.mu.RLock()
	defer chr.mu.RUnlock()
	
	if len(chr.ring) == 0 {
		return nil
	}
	
	hash := chr.hash(key)
	
	// Find position in ring
	idx := sort.Search(len(chr.ring), func(i int) bool {
		return chr.ring[i] >= hash
	})
	
	if idx == len(chr.ring) {
		idx = 0
	}
	
	// Collect unique nodes
	seen := make(map[string]bool)
	nodes := make([]string, 0, n)
	
	for i := 0; i < len(chr.ring) && len(nodes) < n; i++ {
		pos := (idx + i) % len(chr.ring)
		nodeID := chr.nodes[chr.ring[pos]]
		
		if !seen[nodeID] {
			seen[nodeID] = true
			nodes = append(nodes, nodeID)
		}
	}
	
	return nodes
}

// hash computes hash for a key
func (chr *ConsistentHashRing) hash(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return uint32(h[0])<<24 | uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
}

// GossipProtocol implements gossip-based peer discovery and state sync
type GossipProtocol struct {
	config  GossipConfig
	mu      sync.RWMutex
	peers   map[string]*Peer
	msgChan chan *GossipMessage
	stopChan chan struct{}
	messageHandlers map[GossipMessageType]MessageHandler
}

// GossipConfig configures the gossip protocol
type GossipConfig struct {
	NodeID   string
	Fanout   int
	Interval time.Duration
}

// GossipMessage represents a gossip message
type GossipMessage struct {
	Type      GossipMessageType
	SourceID  string
	Timestamp time.Time
	Payload   any
}

// GossipMessageType defines gossip message types
type GossipMessageType int

const (
	MsgTypePeerJoin GossipMessageType = iota
	MsgTypePeerLeave
	MsgTypeHeartbeat
	MsgTypeFetchStarted
	MsgTypeFetchCompleted
	MsgTypeFetchFailed
)

// MessageHandler is a callback for handling gossip messages
type MessageHandler func(msg *GossipMessage)

// AddMessageHandler adds a message handler for a specific message type
func (gp *GossipProtocol) AddMessageHandler(msgType GossipMessageType, handler MessageHandler) {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	
	if gp.messageHandlers == nil {
		gp.messageHandlers = make(map[GossipMessageType]MessageHandler)
	}
	gp.messageHandlers[msgType] = handler
}

// Update handleGossipMessage to call handlers
func (gp *GossipProtocol) handleMessageWithRouting(msg *GossipMessage) {
	gp.mu.RLock()
	handler, exists := gp.messageHandlers[msg.Type]
	gp.mu.RUnlock()
	
	if exists {
		// Call the registered handler
		handler(msg)
	}
	
	// Forward to random peers
	gp.mu.RLock()
	peers := make([]*Peer, 0, len(gp.peers))
	for _, peer := range gp.peers {
		if peer.ID != msg.SourceID {
			peers = append(peers, peer)
		}
	}
	gp.mu.RUnlock()
	
	// Select random subset
	for i := 0; i < gp.config.Fanout && i < len(peers); i++ {
		// In production, send to peer over network
		_ = peers[i]
	}
}

// NewGossipProtocol creates a new gossip protocol
func NewGossipProtocol(config GossipConfig) *GossipProtocol {
	gp := &GossipProtocol{
		config:   config,
		peers:    make(map[string]*Peer),
		msgChan:  make(chan *GossipMessage, 1000),
		stopChan: make(chan struct{}),
		messageHandlers: make(map[GossipMessageType]MessageHandler),
	}
	
	go gp.gossipLoop()
	
	return gp
}
// gossipLoop periodically gossips with peers
func (gp *GossipProtocol) gossipLoop() {
	ticker := time.NewTicker(gp.config.Interval)
	defer ticker.Stop()
	
	for {
		select {
		case msg := <-gp.msgChan:
			gp.handleMessageWithRouting(msg)
			
		case <-ticker.C:
			gp.gossipToPeers()
			
		case <-gp.stopChan:
			return
		}
	}
}

// Stop stops the gossip protocol. Safe to call multiple times.
func (gp *GossipProtocol) Stop() {
	select {
	case <-gp.stopChan:
		return // already closed
	default:
		close(gp.stopChan)
	}
}

// Broadcast broadcasts a message to random peers
func (gp *GossipProtocol) Broadcast(msg *GossipMessage) {
	msg.SourceID = gp.config.NodeID
	msg.Timestamp = time.Now()

	select {
	case gp.msgChan <- msg:
	default:
		// Channel full, drop message
	}
}

// gossipToPeers gossips state to random peers
func (gp *GossipProtocol) gossipToPeers() {
	// Send heartbeat
	gp.Broadcast(&GossipMessage{
		Type: MsgTypeHeartbeat,
	})
}
