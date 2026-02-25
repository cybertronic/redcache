package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// RaftStore provides Raft-based replication for coordination state
type RaftStore struct {
	raft       *raft.Raft
	fsm        *FSM
	config     RaftConfig
	db         *Database
	shutdownCh chan struct{}
}

// RaftConfig configures the Raft store
type RaftConfig struct {
	NodeID           string
	RaftDir          string
	RaftBind         string
	Bootstrap        bool
	JoinAddresses    []string
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration
	MaxAppendEntries int
	SnapshotInterval time.Duration
	SnapshotThreshold uint64
}

// DefaultRaftConfig returns default Raft configuration
func DefaultRaftConfig(nodeID, raftDir, raftBind string) RaftConfig {
	return RaftConfig{
		NodeID:            nodeID,
		RaftDir:           raftDir,
		RaftBind:          raftBind,
		Bootstrap:         false,
		HeartbeatTimeout:  1 * time.Second,
		ElectionTimeout:   1 * time.Second,
		CommitTimeout:     50 * time.Millisecond,
		MaxAppendEntries:  64,
		SnapshotInterval:  120 * time.Second,
		SnapshotThreshold: 8192,
	}
}

// NewRaftStore creates a new Raft store
func NewRaftStore(config RaftConfig, db *Database) (*RaftStore, error) {
	// Create Raft directory
	if err := os.MkdirAll(config.RaftDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create raft directory: %w", err)
	}

	// Create FSM (FIX #36: no mutex in FSM — Raft guarantees serial Apply calls)
	fsm := NewFSM(db)

	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	// Apply configured values, falling back to raft defaults for zero values
	if config.HeartbeatTimeout > 0 {
		raftConfig.HeartbeatTimeout = config.HeartbeatTimeout
	}
	if config.ElectionTimeout > 0 {
		raftConfig.ElectionTimeout = config.ElectionTimeout
	}
	if config.CommitTimeout > 0 {
		raftConfig.CommitTimeout = config.CommitTimeout
	}
	if config.MaxAppendEntries > 0 {
		raftConfig.MaxAppendEntries = config.MaxAppendEntries
	}
	if config.SnapshotInterval > 0 {
		raftConfig.SnapshotInterval = config.SnapshotInterval
	}
	if config.SnapshotThreshold > 0 {
		raftConfig.SnapshotThreshold = config.SnapshotThreshold
	}

	// Create transport
	addr, err := raft.NewTCPTransport(config.RaftBind, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft transport: %w", err)
	}

	// Create log store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	// Create stable store
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(config.RaftDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	store := &RaftStore{
		raft:       r,
		fsm:        fsm,
		config:     config,
		db:         db,
		shutdownCh: make(chan struct{}),
	}

	// Bootstrap cluster if needed
	if config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(config.NodeID),
					Address: addr.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	// Join cluster if addresses provided
	if len(config.JoinAddresses) > 0 {
		go store.joinCluster()
	}

	return store, nil
}

// Set stores a key-value pair with strong consistency
func (rs *RaftStore) Set(key, value []byte) error {
	if rs.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	cmd := Command{
		Op:    "set",
		Key:   key,
		Value: value,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := rs.raft.Apply(data, 10*time.Second)
	return future.Error()
}

// Get retrieves a value (reads from local state)
func (rs *RaftStore) Get(key []byte) ([]byte, error) {
	return rs.db.Get(key)
}

// Delete removes a key with strong consistency
func (rs *RaftStore) Delete(key []byte) error {
	if rs.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	cmd := Command{
		Op:  "delete",
		Key: key,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := rs.raft.Apply(data, 10*time.Second)
	return future.Error()
}

// IsLeader returns true if this node is the leader
func (rs *RaftStore) IsLeader() bool {
	return rs.raft.State() == raft.Leader
}

// Leader returns the current leader address
func (rs *RaftStore) Leader() string {
	addr, _ := rs.raft.LeaderWithID()
	return string(addr)
}

// AddVoter adds a voting member to the cluster
func (rs *RaftStore) AddVoter(id, address string) error {
	if rs.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	future := rs.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, 10*time.Second)
	return future.Error()
}

// RemoveServer removes a server from the cluster
func (rs *RaftStore) RemoveServer(id string) error {
	if rs.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	future := rs.raft.RemoveServer(raft.ServerID(id), 0, 10*time.Second)
	return future.Error()
}

// GetServers returns the list of servers in the cluster
func (rs *RaftStore) GetServers() ([]Server, error) {
	future := rs.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}

	servers := make([]Server, 0)
	for _, server := range future.Configuration().Servers {
		servers = append(servers, Server{
			ID:      string(server.ID),
			Address: string(server.Address),
			Voter:   server.Suffrage == raft.Voter,
		})
	}

	return servers, nil
}

// Server represents a Raft server
type Server struct {
	ID      string
	Address string
	Voter   bool
}

// Stats returns Raft statistics
func (rs *RaftStore) Stats() map[string]string {
	return rs.raft.Stats()
}

// Close shuts down the Raft store
func (rs *RaftStore) Close() error {
	select {
	case <-rs.shutdownCh:
		return nil // already closed
	default:
		close(rs.shutdownCh)
	}
	return rs.raft.Shutdown().Error()
}

// joinCluster attempts to join an existing cluster by contacting each
// join address over HTTP and requesting to be added as a voter.
// FIX #6: replaces the stub (which only did `_ = addr`) with a real
// implementation that POSTs to the leader's /raft/join endpoint.
func (rs *RaftStore) joinCluster() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-rs.shutdownCh:
			return
		case <-ticker.C:
			// If we are already part of the cluster (not shutdown), stop trying.
			if rs.raft.State() != raft.Shutdown {
				return
			}

			for _, addr := range rs.config.JoinAddresses {
				if err := rs.requestJoin(addr); err == nil {
					// Successfully joined — stop the retry loop.
					return
				}
			}
		}
	}
}

// requestJoin sends a join request to the node at leaderAddr.
// The remote node is expected to expose a POST /raft/join endpoint that
// accepts JSON {"id": "<nodeID>", "addr": "<raftBind>"} and calls AddVoter.
func (rs *RaftStore) requestJoin(leaderAddr string) error {
	// Normalise the address: if it has no scheme, assume HTTP.
	if !strings.HasPrefix(leaderAddr, "http://") && !strings.HasPrefix(leaderAddr, "https://") {
		leaderAddr = "http://" + leaderAddr
	}

	joinURL := strings.TrimRight(leaderAddr, "/") + "/raft/join"

	body := fmt.Sprintf(`{"id":%q,"addr":%q}`, rs.config.NodeID, rs.config.RaftBind)
	resp, err := http.Post(joinURL, "application/json", strings.NewReader(body)) //nolint:noctx
	if err != nil {
		return fmt.Errorf("join request to %s failed: %w", joinURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join request to %s returned status %d", joinURL, resp.StatusCode)
	}

	return nil
}

// FSM implements the Raft finite state machine.
// FIX #36: the mutex has been removed. The Raft library guarantees that
// Apply, Snapshot, and Restore are never called concurrently, so the
// extra locking was unnecessary and could mask real concurrency bugs.
type FSM struct {
	db *Database
}

// NewFSM creates a new FSM
func NewFSM(db *Database) *FSM {
	return &FSM{
		db: db,
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) any {
	// FIX #36: no mutex needed — Raft serialises all Apply calls.
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	switch cmd.Op {
	case "set":
		return f.db.Set(cmd.Key, cmd.Value)
	case "delete":
		return f.db.Delete(cmd.Key)
	default:
		return fmt.Errorf("unknown command: %s", cmd.Op)
	}
}

// Snapshot returns a snapshot of the FSM
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// FIX #36: no mutex needed — Raft serialises Snapshot with Apply.
	snapshot := &FSMSnapshot{
		db: f.db,
	}

	return snapshot, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	// FIX #36: no mutex needed — Raft serialises Restore with Apply.
	defer rc.Close()

	// Read snapshot data
	decoder := json.NewDecoder(rc)
	var entries []SnapshotEntry
	if err := decoder.Decode(&entries); err != nil {
		return err
	}

	// Restore entries
	for _, entry := range entries {
		if err := f.db.Set(entry.Key, entry.Value); err != nil {
			return err
		}
	}

	return nil
}

// FSMSnapshot implements the Raft FSM snapshot
type FSMSnapshot struct {
	db *Database
}

// Persist writes the snapshot to the given sink
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	// Collect all entries
	entries := make([]SnapshotEntry, 0)
	err := s.db.Scan([]byte(""), func(key, value []byte) error {
		entries = append(entries, SnapshotEntry{
			Key:   key,
			Value: value,
		})
		return nil
	})
	if err != nil {
		sink.Cancel()
		return err
	}

	// Write entries
	encoder := json.NewEncoder(sink)
	if err := encoder.Encode(entries); err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release releases the snapshot
func (s *FSMSnapshot) Release() {}

// SnapshotEntry represents a snapshot entry
type SnapshotEntry struct {
	Key   []byte
	Value []byte
}

// Command represents a Raft command
type Command struct {
	Op    string
	Key   []byte
	Value []byte
}