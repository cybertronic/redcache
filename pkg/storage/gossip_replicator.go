package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"redcache/pkg/distributed"
)

// GossipReplicator provides eventual consistency replication for cache data
type GossipReplicator struct {
	config         GossipConfig
	db             *Database
	memberlist     *memberlist.Memberlist
	updates        chan *Update
	vectorClocks   map[string]*distributed.VectorClock
	clockMu        sync.RWMutex
	conflictResolver *distributed.ConflictResolver
	shutdownCh     chan struct{}
	wg             sync.WaitGroup
}

// GossipConfig configures the gossip replicator
type GossipConfig struct {
	NodeID             string
	Fanout             int           // Number of nodes to replicate to
	SyncInterval       time.Duration // How often to sync
	ConflictResolution string        // Strategy: "lww", "highest_version", "quorum"
	MaxUpdateQueueSize int
	RetryAttempts      int
	RetryDelay         time.Duration
}

// DefaultGossipConfig returns default gossip configuration
func DefaultGossipConfig(nodeID string) GossipConfig {
	return GossipConfig{
		NodeID:             nodeID,
		Fanout:             3,
		SyncInterval:       5 * time.Second,
		ConflictResolution: "lww",
		MaxUpdateQueueSize: 10000,
		RetryAttempts:      3,
		RetryDelay:         1 * time.Second,
	}
}

// Update represents a data update to be replicated
type Update struct {
	Key         string
	Value       []byte
	Version     int64
	NodeID      string
	Timestamp   time.Time
	VectorClock *distributed.VectorClock
	TTL         time.Duration
}

// NewGossipReplicator creates a new gossip replicator
func NewGossipReplicator(config GossipConfig, db *Database, ml *memberlist.Memberlist) (*GossipReplicator, error) {
	// Conflict resolution will be handled with simple timestamp comparison for now
	var resolver *distributed.ConflictResolver = nil

	gr := &GossipReplicator{
		config:           config,
		db:               db,
		memberlist:       ml,
		updates:          make(chan *Update, config.MaxUpdateQueueSize),
		vectorClocks:     make(map[string]*distributed.VectorClock),
		conflictResolver: resolver,
		shutdownCh:       make(chan struct{}),
	}

	// Start replication workers
	for i := 0; i < 3; i++ {
		gr.wg.Add(1)
		go gr.replicationWorker()
	}

	// Start sync worker
	gr.wg.Add(1)
	go gr.syncWorker()

	return gr, nil
}

// Replicate queues an update for replication
func (gr *GossipReplicator) Replicate(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Get or create vector clock for this key
	gr.clockMu.Lock()
	vc, exists := gr.vectorClocks[key]
	if !exists {
		vc = distributed.NewVectorClock()
		gr.vectorClocks[key] = vc
	}
	// Increment our clock
	vc.Increment(gr.config.NodeID)
	gr.clockMu.Unlock()

	update := &Update{
		Key:         key,
		Value:       value,
		Version:     time.Now().UnixNano(),
		NodeID:      gr.config.NodeID,
		Timestamp:   time.Now(),
		VectorClock: vc.Clone(),
		TTL:         ttl,
	}

	// Store locally first
	if ttl > 0 {
		if err := gr.db.SetWithTTL([]byte(key), value, ttl); err != nil {
			return err
		}
	} else {
		if err := gr.db.Set([]byte(key), value); err != nil {
			return err
		}
	}

	// Store vector clock
	if err := gr.storeVectorClock(key, vc); err != nil {
		return err
	}

	// Queue for replication
	select {
	case gr.updates <- update:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("update queue full")
	}
}

// Get retrieves a value with conflict resolution
func (gr *GossipReplicator) Get(ctx context.Context, key string) ([]byte, error) {
	// Get local value
	value, err := gr.db.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	// Get vector clock
	vc, err := gr.loadVectorClock(key)
	if err != nil {
		return value, nil // Return value even if clock load fails
	}

	gr.clockMu.Lock()
	gr.vectorClocks[key] = vc
	gr.clockMu.Unlock()

	return value, nil
}

// Delete removes a key and replicates the deletion
func (gr *GossipReplicator) Delete(ctx context.Context, key string) error {
	// Create tombstone update
	update := &Update{
		Key:       key,
		Value:     nil, // Tombstone
		Version:   time.Now().UnixNano(),
		NodeID:    gr.config.NodeID,
		Timestamp: time.Now(),
	}

	// Delete locally
	if err := gr.db.Delete([]byte(key)); err != nil {
		return err
	}

	// Delete vector clock
	gr.clockMu.Lock()
	delete(gr.vectorClocks, key)
	gr.clockMu.Unlock()
	gr.db.Delete([]byte("vclock/" + key))

	// Queue for replication
	select {
	case gr.updates <- update:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("update queue full")
	}
}

// replicationWorker processes updates and replicates to other nodes
func (gr *GossipReplicator) replicationWorker() {
	defer gr.wg.Done()

	for {
		select {
		case <-gr.shutdownCh:
			return
		case update := <-gr.updates:
			gr.replicateUpdate(update)
		}
	}
}

// replicateUpdate replicates an update to random nodes
func (gr *GossipReplicator) replicateUpdate(update *Update) {
	// Get cluster members
	members := gr.memberlist.Members()
	if len(members) <= 1 {
		return // Only us in the cluster
	}

	// Select random nodes (fanout)
	targets := gr.selectRandomNodes(members, gr.config.Fanout)

	// Replicate to each target
	for _, target := range targets {
		go gr.sendUpdate(target, update)
	}
}

// sendUpdate sends an update to a specific node
func (gr *GossipReplicator) sendUpdate(target *memberlist.Node, update *Update) {
	// Serialize update
	data, err := json.Marshal(update)
	if err != nil {
		return
	}

	// Send via memberlist (would use gRPC in production)
	for attempt := 0; attempt < gr.config.RetryAttempts; attempt++ {
		err := gr.memberlist.SendReliable(target, data)
		if err == nil {
			return
		}
		time.Sleep(gr.config.RetryDelay)
	}
}

// HandleUpdate handles an incoming update from another node
func (gr *GossipReplicator) HandleUpdate(update *Update) error {
	// Get local vector clock
	gr.clockMu.RLock()
	localVC, exists := gr.vectorClocks[update.Key]
	gr.clockMu.RUnlock()

	if !exists {
		// No local version, accept update
		return gr.applyUpdate(update)
	}

	// Compare vector clocks
	if update.VectorClock == nil {
		// No vector clock in update, use timestamp
		return gr.resolveByTimestamp(update, localVC)
	}

	ordering := update.VectorClock.Compare(localVC)

	switch ordering {
	case distributed.OrderingAfter:
		// Remote is newer, accept
		return gr.applyUpdate(update)

	case distributed.OrderingBefore:
		// Local is newer, ignore
		return nil

	case distributed.OrderingConcurrent:
		// Conflict! Use conflict resolution strategy
		return gr.resolveConflict(update, localVC)

	case distributed.OrderingEqual:
		// Same version, ignore
		return nil
	}

	return nil
}

// applyUpdate applies an update to local storage
func (gr *GossipReplicator) applyUpdate(update *Update) error {
	if update.Value == nil {
		// Tombstone - delete
		return gr.db.Delete([]byte(update.Key))
	}

	// Store value
	var err error
	if update.TTL > 0 {
		err = gr.db.SetWithTTL([]byte(update.Key), update.Value, update.TTL)
	} else {
		err = gr.db.Set([]byte(update.Key), update.Value)
	}
	if err != nil {
		return err
	}

	// Persist the wall-clock write timestamp for LWW conflict resolution.
	if tsBytes, err := update.Timestamp.MarshalBinary(); err == nil {
		_ = gr.db.Set([]byte("ts/"+update.Key), tsBytes)
	}

	// Update vector clock
	if update.VectorClock != nil {
		gr.clockMu.Lock()
		gr.vectorClocks[update.Key] = update.VectorClock.Clone()
		gr.clockMu.Unlock()

		return gr.storeVectorClock(update.Key, update.VectorClock)
	}

	return nil
}


// resolveConflict resolves a conflict using timestamp comparison (simplified)
func (gr *GossipReplicator) resolveConflict(update *Update, localVC *distributed.VectorClock) error {
	// FIX #24: use update.Timestamp directly for LWW comparison.
	// The previous code used localVC.Sum() (sum of logical ticks) as a
	// nanosecond wall-clock timestamp, which is semantically wrong.
	// We store the wall-clock write time in the Update struct instead.
	localTime := gr.getLocalTimestamp(update.Key)

	if update.Timestamp.After(localTime) {
		// Remote is newer, apply it
		return gr.applyUpdate(update)
	}

	// Local is newer or equal, keep it
	return nil
}

// resolveByTimestamp resolves conflict using timestamp (fallback).
// FIX #24: uses the stored wall-clock timestamp, not vectorClock.Sum().
func (gr *GossipReplicator) resolveByTimestamp(update *Update, localVC *distributed.VectorClock) error {
	localTime := gr.getLocalTimestamp(update.Key)

	if update.Timestamp.After(localTime) {
		return gr.applyUpdate(update)
	}

	return nil
}

// getLocalTimestamp returns the wall-clock write time for a key by reading
// the stored Update timestamp from the database.  Falls back to the zero
// time (so any remote update wins) if no timestamp is stored.
func (gr *GossipReplicator) getLocalTimestamp(key string) time.Time {
	timestampKey := []byte("ts/" + key)
	data, err := gr.db.Get(timestampKey)
	if err != nil || len(data) == 0 {
		return time.Time{}
	}
	var t time.Time
	if err := t.UnmarshalBinary(data); err != nil {
		return time.Time{}
	}
	return t
}

// syncWorker periodically syncs with other nodes
func (gr *GossipReplicator) syncWorker() {
	defer gr.wg.Done()

	syncInterval := gr.config.SyncInterval
	if syncInterval <= 0 {
		syncInterval = 30 * time.Second // default sync interval
	}
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gr.shutdownCh:
			return
		case <-ticker.C:
			gr.performSync()
		}
	}
}

// performSync performs a sync with random nodes
func (gr *GossipReplicator) performSync() {
	members := gr.memberlist.Members()
	if len(members) <= 1 {
		return
	}

	// Select random node to sync with
	targets := gr.selectRandomNodes(members, 1)
	if len(targets) == 0 {
		return
	}

	// In production, would request keys/versions from target
	// and sync any differences
	// For now, this is a placeholder
}

// selectRandomNodes selects n random nodes from the member list.
// FIX #23: uses math/rand.Shuffle for genuine randomness instead of the
// previous time.Now().UnixNano() % n loop which always selected the same node.
func (gr *GossipReplicator) selectRandomNodes(members []*memberlist.Node, n int) []*memberlist.Node {
	// Filter out self
	others := make([]*memberlist.Node, 0, len(members))
	for _, member := range members {
		if member.Name != gr.config.NodeID {
			others = append(others, member)
		}
	}

	if len(others) == 0 {
		return nil
	}

	if n > len(others) {
		n = len(others)
	}

	// Shuffle a copy so we don't mutate the caller's slice.
	shuffled := make([]*memberlist.Node, len(others))
	copy(shuffled, others)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	return shuffled[:n]
}

// storeVectorClock stores a vector clock in the database
func (gr *GossipReplicator) storeVectorClock(key string, vc *distributed.VectorClock) error {
	clockKey := []byte("vclock/" + key)
	clockData := vc.ToMap()
	return gr.db.SetJSON(clockKey, clockData)
}

// loadVectorClock loads a vector clock from the database
func (gr *GossipReplicator) loadVectorClock(key string) (*distributed.VectorClock, error) {
	clockKey := []byte("vclock/" + key)
	var clockData map[string]int64
	err := gr.db.GetJSON(clockKey, &clockData)
	if err != nil {
		return distributed.NewVectorClock(), nil // Return empty clock on error
	}
	return distributed.FromMap(clockData), nil
}

// Stats returns replication statistics
func (gr *GossipReplicator) Stats() GossipStats {
	gr.clockMu.RLock()
	defer gr.clockMu.RUnlock()

	return GossipStats{
		QueueSize:     len(gr.updates),
		VectorClocks:  len(gr.vectorClocks),
		ClusterSize:   len(gr.memberlist.Members()),
	}
}

// GossipStats contains gossip replication statistics
type GossipStats struct {
	QueueSize    int
	VectorClocks int
	ClusterSize  int
}

// Close shuts down the gossip replicator
func (gr *GossipReplicator) Close() error {
	select {
	case <-gr.shutdownCh:
		return nil // already closed
	default:
		close(gr.shutdownCh)
	}
	gr.wg.Wait()
	return nil
}