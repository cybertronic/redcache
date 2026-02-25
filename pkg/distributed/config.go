package distributed

import "time"

// ClusterConfig contains configuration for the distributed cluster
type ClusterConfig struct {
	// Node identification
	NodeID   string
	BindAddr string
	BindPort int
	RPCPort  int

	// Cluster membership
	Seeds []string

	// Quorum settings
	EnableQuorum bool

	// Conflict resolution
	ConflictResolution string // "last-write-wins", "highest-version", "quorum"

	// Replication
	ReplicationFactor int // Number of replicas for each key (default: 3)

	// Auto-rebalancing
	EnableAutoRebalance bool
	RebalanceBatchSize  int
	RebalanceThrottle   time.Duration

	// Erasure coding
	EnableErasureCoding bool          // Enable/disable erasure coding (default: false)
	ErasureEncoding     string        // Encoding scheme (e.g., "6+3", "8+4")
	ErasureShardSize    int64         // Size of each shard in bytes (default: 1MB)
	ErasureEnableAVX2   bool          // Enable AVX2 acceleration
	ErasureEnableAVX512 bool          // Enable AVX-512 acceleration

	// Health monitoring
	HealthCheckInterval time.Duration
	HealthCheckTimeout  time.Duration

	// Data synchronization
	SyncInterval time.Duration
	// TLS configuration
	TLSCertFile string
	TLSKeyFile  string
}

// DefaultClusterConfig returns a default cluster configuration
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		NodeID:               "node-1",
		BindAddr:             "127.0.0.1",
		BindPort:             7946,
		RPCPort:              8946,
		Seeds:                []string{},
		EnableQuorum:         true,
		ConflictResolution:   "last-write-wins",
		ReplicationFactor:    3,
		EnableAutoRebalance:  true,
		RebalanceBatchSize:   100,
		RebalanceThrottle:    10 * time.Millisecond,
		EnableErasureCoding:  false, // Disabled by default
		ErasureEncoding:      "6+3",  // 6 data shards + 3 parity shards
		ErasureShardSize:     1048576, // 1MB
		ErasureEnableAVX2:    true,
		ErasureEnableAVX512:  true,
		HealthCheckInterval:  time.Second * 5,
		HealthCheckTimeout:   time.Second * 2,
		SyncInterval:         time.Second * 10,
	}
}
