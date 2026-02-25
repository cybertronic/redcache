package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"redcache/pkg/distributed"
)

// MultiRaftStore manages multiple Raft instances (virtual shards) within a single process.
type MultiRaftStore struct {
	mu         sync.RWMutex
	shards     map[distributed.ShardID]*RaftStore
	cancels    map[distributed.ShardID]context.CancelFunc // Per-shard lifecycle control
	transport  *raft.NetworkTransport
	db         *Database
	config     RaftConfig
	shutdownCh chan struct{}
}

// NewMultiRaftStore initializes the multi-tenant Raft subsystem.
func NewMultiRaftStore(config RaftConfig, db *Database) (*MultiRaftStore, error) {
	addr, err := raft.NewTCPTransport(config.RaftBind, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create multi-raft transport: %w", err)
	}

	return &MultiRaftStore{
		shards:     make(map[distributed.ShardID]*RaftStore),
		cancels:    make(map[distributed.ShardID]context.CancelFunc),
		transport:  addr,
		db:         db,
		config:     config,
		shutdownCh: make(chan struct{}),
	}, nil
}

// StartShard initializes and starts a specific virtual shard.
func (m *MultiRaftStore) StartShard(shardID distributed.ShardID, peers []string) (err error) {
	m.mu.Lock()
	if _, ok := m.shards[shardID]; ok {
		m.mu.Unlock()
		return nil
	}

	_, cancel := context.WithCancel(context.Background())
	m.cancels[shardID] = cancel
	m.mu.Unlock()

	// Cleanup if startup fails
	defer func() {
		if err != nil {
			m.mu.Lock()
			cancel()
			delete(m.cancels, shardID)
			m.mu.Unlock()
		}
	}()

	// Shard-specific directory
	shardDir := filepath.Join(m.config.RaftDir, fmt.Sprintf("shard-%d", shardID))
	if err = os.MkdirAll(shardDir, 0755); err != nil {
		return err
	}

	fsm := NewFSM(m.db)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(fmt.Sprintf("%s-shard-%d", m.config.NodeID, shardID))
	
	if m.config.HeartbeatTimeout > 0 {
		raftConfig.HeartbeatTimeout = m.config.HeartbeatTimeout
	}
	if m.config.ElectionTimeout > 0 {
		raftConfig.ElectionTimeout = m.config.ElectionTimeout
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(shardDir, "raft-log.db"))
	if err != nil {
		return err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(shardDir, "raft-stable.db"))
	if err != nil {
		return err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(shardDir, 3, os.Stderr)
	if err != nil {
		return err
	}

	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, m.transport)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.shards[shardID] = &RaftStore{
		raft:       r,
		fsm:        fsm,
		config:     m.config,
		db:         m.db,
		shutdownCh: m.shutdownCh,
	}
	m.mu.Unlock()

	return nil
}

// GetShard returns the RaftStore for a specific shard.
func (m *MultiRaftStore) GetShard(shardID distributed.ShardID) (*RaftStore, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.shards[shardID]
	return s, ok
}

// Close shuts down all virtual shards and the transport.
func (m *MultiRaftStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	close(m.shutdownCh)

	for id, cancel := range m.cancels {
		cancel()
		if s, ok := m.shards[id]; ok {
			_ = s.Close()
		}
	}
	
	return m.transport.Close()
}
