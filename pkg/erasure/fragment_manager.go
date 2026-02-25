package erasure

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"redcache/pkg/distributed"
	"redcache/pkg/iouring"
	"redcache/pkg/storage"
)

// FragmentManager manages erasure-coded fragments across the cluster.
type FragmentManager struct {
	encoder        *Encoder
	clusterManager *distributed.ClusterManager
	rpcClient      distributed.RPCClient
	db             *storage.Database // Persistent store for shard metadata

	batchSender *iouring.BatchSender
	connPool    map[string]net.Conn
	connPoolMu  sync.RWMutex

	shardDistribution string
	fragmentCache     map[string][]byte
	cacheMu           sync.RWMutex
	cacheMaxSize      int64
	cacheCurrentSize  int64

	stats   FragmentStats
	statsMu sync.RWMutex

	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

type FragmentStats struct {
	EncodeOperations      int64
	DecodeOperations      int64
	ReconstructOperations int64
	CacheHits             int64
	CacheMisses           int64
	FragmentsDistributed  int64
	FragmentsCollected    int64
	Errors                int64
}

type FragmentMetadata struct {
	Key               string             `json:"key"`
	Encoding          string             `json:"encoding"`
	NumStripes        int                `json:"num_stripes"`
	FragmentLocations []FragmentLocation `json:"locations"`
	CreatedAt         time.Time          `json:"created_at"`
	LastAccessedAt    time.Time          `json:"last_accessed_at"`
}

type FragmentLocation struct {
	StripeIndex int    `json:"stripe"`
	ShardIndex  int    `json:"shard"`
	NodeID      string `json:"node_id"`
	IsDataShard bool   `json:"is_data"`
}

// NewFragmentManager creates a new fragment manager with metadata persistence
func NewFragmentManager(encoder *Encoder, cm *distributed.ClusterManager, rpcClient distributed.RPCClient, db *storage.Database) *FragmentManager {
	fm := &FragmentManager{
		encoder:           encoder,
		clusterManager:    cm,
		rpcClient:         rpcClient,
		db:                db,
		shardDistribution: "consistent_hashing",
		fragmentCache:     make(map[string][]byte),
		connPool:          make(map[string]net.Conn),
		cacheMaxSize:      10 * 1024 * 1024 * 1024,
		shutdownCh:        make(chan struct{}),
	}
	return fm
}

func (fm *FragmentManager) SetBatchSender(bs *iouring.BatchSender) {
	fm.batchSender = bs
}

// storeMetadata persists fragment map to BadgerDB
func (fm *FragmentManager) storeMetadata(key string, metadata *FragmentMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	metaKey := []byte("meta:frag:" + key)
	return fm.db.Set(metaKey, data)
}

// getMetadata retrieves fragment map from BadgerDB
func (fm *FragmentManager) getMetadata(key string) (*FragmentMetadata, error) {
	metaKey := []byte("meta:frag:" + key)
	data, err := fm.db.Get(metaKey)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("shard metadata not found for key: %s", key)
	}

	var metadata FragmentMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal shard metadata: %w", err)
	}
	return &metadata, nil
}

// GetStats returns fragment manager statistics
func (fm *FragmentManager) GetStats() map[string]any {
	fm.statsMu.RLock()
	defer fm.statsMu.RUnlock()

	return map[string]any{
		"encode_operations":      fm.stats.EncodeOperations,
		"decode_operations":      fm.stats.DecodeOperations,
		"reconstruct_operations": fm.stats.ReconstructOperations,
		"cache_hits":             fm.stats.CacheHits,
		"cache_misses":           fm.stats.CacheMisses,
		"fragments_distributed":  fm.stats.FragmentsDistributed,
		"fragments_collected":    fm.stats.FragmentsCollected,
		"errors":                 fm.stats.Errors,
	}
}

func (fm *FragmentManager) IsEnabled() bool {
	return true
}

func (fm *FragmentManager) Shutdown() error {
	return nil
}
