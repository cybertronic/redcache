package replication

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"redcache/pkg/distributed"
	"redcache/pkg/storage"
)

func TestReplicationManager(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Create config
	config := Config{
		NodeID:  "node1",
		DataDir: tmpDir,
		StorageConfig: storage.DatabaseConfig{
			Path:        filepath.Join(tmpDir, "db"),
			MaxMemoryMB: 32,
			SyncWrites:  false,
			Compression: false,
			GCInterval:  1 * time.Minute,
			ValueLogFileSize:  1 << 20, // 1MB (minimum valid size)
			NumVersionsToKeep: 1,
		},
		RaftConfig: storage.RaftConfig{
			NodeID:   "node1",
			RaftDir:  filepath.Join(tmpDir, "raft"),
			RaftBind: "127.0.0.1:0",
			Bootstrap: true,
		},
		GossipConfig: storage.GossipConfig{
			NodeID:             "node1",
			Fanout:             3,
			SyncInterval:       5 * time.Second,
			ConflictResolution: "lww",
		},
		MemberlistConfig: memberlist.DefaultLocalConfig(),
		LockConfig: distributed.LockConfig{
			NodeID:         "node1",
			DefaultTimeout: 10 * time.Second,
		},
		ClusterConfig: &distributed.ClusterConfig{
			NodeID:   "node1",
			BindAddr: "127.0.0.1",
			BindPort: 0,
		},
	}
	
	// Create manager
	manager, err := NewManager(config)
	require.NoError(t, err)
	defer manager.Close()
	
	// Wait for Raft leader election before running subtests
	err = manager.WaitForLeader(10 * time.Second)
	require.NoError(t, err, "Raft leader election timed out")
	
	t.Run("CoordinationOperations", func(t *testing.T) {
		ctx := context.Background()
		
		// Set coordination data
		err := manager.SetCoordination(ctx, "coord_key1", []byte("coord_value1"))
		assert.NoError(t, err)
		
		// Get coordination data
		value, err := manager.GetCoordination(ctx, "coord_key1")
		assert.NoError(t, err)
		assert.Equal(t, []byte("coord_value1"), value)
	})
	
	t.Run("CacheOperations", func(t *testing.T) {
		ctx := context.Background()
		
		// Set cache data
		err := manager.SetCache(ctx, "cache_key1", []byte("cache_value1"), 0)
		assert.NoError(t, err)
		
		// Get cache data
		value, err := manager.GetCache(ctx, "cache_key1")
		assert.NoError(t, err)
		assert.Equal(t, []byte("cache_value1"), value)
	})
	
	t.Run("LockOperations", func(t *testing.T) {
		ctx := context.Background()
		
		// Acquire lock
		err := manager.AcquireLock(ctx, "lock_key1", distributed.LockTypeExclusive)
		assert.NoError(t, err)
		
		// Release lock
		err = manager.ReleaseLock(ctx, "lock_key1")
		assert.NoError(t, err)
		err = manager.ReleaseLock(ctx, "lock_key1")
		assert.NoError(t, err)
	})
	
	t.Run("LeadershipCheck", func(t *testing.T) {
		// Wait for leader election
		err := manager.WaitForLeader(10 * time.Second)
		assert.NoError(t, err)
		
		// Check if we're the leader (should be since we're the only node)
		isLeader := manager.IsLeader()
		assert.True(t, isLeader)
		
		// Get leader address
		leader := manager.Leader()
		assert.NotEmpty(t, leader)
	})
	
	t.Run("ClusterMembers", func(t *testing.T) {
		members := manager.GetClusterMembers()
		assert.Len(t, members, 1) // Only ourselves
	})
	
	t.Run("Statistics", func(t *testing.T) {
	stats := manager.GetStats()
	
	assert.Equal(t, "node1", stats.NodeID)
	
	// Check that stats contain the expected components
	assert.NotNil(t, stats.HybridStats)
	assert.NotNil(t, stats.LockStats)
	assert.NotNil(t, stats.DedupStats)
	assert.NotNil(t, stats.PredictorStats)
	assert.NotNil(t, stats.AnalyticsStats)
	})
}
func TestReplicationMetrics(t *testing.T) {
	metrics := NewReplicationMetrics()
	
	t.Run("CoordinationMetrics", func(t *testing.T) {
		metrics.IncrementCoordinationReads()
		metrics.IncrementCoordinationWrites()
		
		snapshot := metrics.GetSnapshot()
		assert.Equal(t, int64(1), snapshot.CoordinationReads)
		assert.Equal(t, int64(1), snapshot.CoordinationWrites)
	})
	
	t.Run("CacheMetrics", func(t *testing.T) {
		metrics.IncrementCacheReads()
		metrics.IncrementCacheWrites()
		
		snapshot := metrics.GetSnapshot()
		assert.Equal(t, int64(1), snapshot.CacheReads)
		assert.Equal(t, int64(1), snapshot.CacheWrites)
	})
	
	t.Run("LockMetrics", func(t *testing.T) {
		metrics.IncrementLocksAcquired()
		metrics.IncrementLocksAcquired()
		metrics.IncrementLocksReleased()
		
		snapshot := metrics.GetSnapshot()
		assert.Equal(t, int64(2), snapshot.LocksAcquired)
		assert.Equal(t, int64(1), snapshot.LocksReleased)
		assert.Equal(t, int64(1), snapshot.ActiveLocks)
	})
	
	t.Run("ClusterMetrics", func(t *testing.T) {
		metrics.SetClusterSize(5)
		metrics.SetVectorClocks(100)
		
		snapshot := metrics.GetSnapshot()
		assert.Equal(t, int64(5), snapshot.ClusterSize)
		assert.Equal(t, int64(100), snapshot.VectorClocks)
	})
	
	t.Run("ConflictMetrics", func(t *testing.T) {
		metrics.IncrementConflicts()
		metrics.IncrementConflictsResolved()
		
		snapshot := metrics.GetSnapshot()
		assert.Equal(t, int64(1), snapshot.Conflicts)
		assert.Equal(t, int64(1), snapshot.ConflictsResolved)
	})
	
	t.Run("Reset", func(t *testing.T) {
		metrics.Reset()
		
		snapshot := metrics.GetSnapshot()
		assert.Equal(t, int64(0), snapshot.CoordinationReads)
		assert.Equal(t, int64(0), snapshot.CacheReads)
		assert.Equal(t, int64(0), snapshot.LocksAcquired)
	})
}

func TestVectorClockIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	
	config := Config{
		NodeID:  "node1",
		DataDir: tmpDir,
		StorageConfig: storage.DatabaseConfig{
			Path:        filepath.Join(tmpDir, "db"),
			MaxMemoryMB: 32,
		},
		RaftConfig: storage.RaftConfig{
			NodeID:    "node1",
			RaftDir:   filepath.Join(tmpDir, "raft"),
			RaftBind:  "127.0.0.1:0",
			Bootstrap: true,
		},
		GossipConfig: storage.GossipConfig{
			NodeID: "node1",
		},
		MemberlistConfig: memberlist.DefaultLocalConfig(),
		LockConfig: distributed.LockConfig{
			NodeID: "node1",
		},
		ClusterConfig: &distributed.ClusterConfig{
			NodeID:   "node1",
			BindAddr: "127.0.0.1",
			BindPort: 0,
		},
	}
	
	manager, err := NewManager(config)
	require.NoError(t, err)
	defer manager.Close()
	
	ctx := context.Background()
	
	// Set data multiple times
	for i := 0; i < 5; i++ {
		err := manager.SetCache(ctx, "test_key", []byte("value"), 0)
		assert.NoError(t, err)
	}
	
	// Vector clock should have been incremented
	vc, err := manager.loadVectorClock("test_key")
	assert.NoError(t, err)
	assert.Greater(t, vc.Get("node1"), int64(0))
}

func BenchmarkReplicationManager(b *testing.B) {
	tmpDir := b.TempDir()
	
	config := Config{
		NodeID:  "node1",
		DataDir: tmpDir,
		StorageConfig: storage.DatabaseConfig{
			Path:        filepath.Join(tmpDir, "db"),
			MaxMemoryMB: 32,
		},
		RaftConfig: storage.RaftConfig{
			NodeID:    "node1",
			RaftDir:   filepath.Join(tmpDir, "raft"),
			RaftBind:  "127.0.0.1:0",
			Bootstrap: true,
		},
		GossipConfig: storage.GossipConfig{
			NodeID: "node1",
		},
		MemberlistConfig: memberlist.DefaultLocalConfig(),
		LockConfig: distributed.LockConfig{
			NodeID: "node1",
		},
		ClusterConfig: &distributed.ClusterConfig{
			NodeID:   "node1",
			BindAddr: "127.0.0.1",
			BindPort: 0,
		},
	}
	
	manager, err := NewManager(config)
	require.NoError(b, err)
	defer manager.Close()
	
	ctx := context.Background()
	
	b.Run("CacheWrites", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := "key_" + string(rune(i))
			value := []byte("value_" + string(rune(i)))
			manager.SetCache(ctx, key, value, 0)
		}
	})
	
	b.Run("CacheReads", func(b *testing.B) {
		// Prepare data
		for i := 0; i < 1000; i++ {
			key := "key_" + string(rune(i))
			value := []byte("value_" + string(rune(i)))
			manager.SetCache(ctx, key, value, 0)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := "key_" + string(rune(i%1000))
			manager.GetCache(ctx, key)
		}
	})
}