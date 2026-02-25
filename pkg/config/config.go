package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	Cache       CacheConfig       `yaml:"cache"`
	Proxy       ProxyConfig       `yaml:"proxy"`
	S3          S3Config          `yaml:"s3"`
	Metrics     MetricsConfig     `yaml:"metrics"`
	Prefetch    PrefetchConfig    `yaml:"prefetch"`
	Storage     StorageConfig     `yaml:"storage"`
	Replication ReplicationConfig `yaml:"replication"`
	ZeroCopy    ZeroCopyConfig    `yaml:"zero_copy"`
	Distributed DistributedConfig `yaml:"distributed"`
}

// DistributedConfig contains cluster-wide configuration
type DistributedConfig struct {
	NodeID              string   `yaml:"node_id"`
	BindAddr            string   `yaml:"bind_addr"`
	BindPort            int      `yaml:"bind_port"`
	RPCPort             int      `yaml:"rpc_port"`
	Seeds               []string `yaml:"seeds"`
	NumVirtualShards    uint32   `yaml:"num_virtual_shards"`
	EnableQuorum        bool     `yaml:"enable_quorum"`
	ReplicationFactor   int      `yaml:"replication_factor"`
	ErasureDataShards   int      `yaml:"erasure_data_shards"`
	ErasureParityShards int      `yaml:"erasure_parity_shards"`
	ErasureShardSize    int      `yaml:"erasure_shard_size"`
	TLSCertFile         string   `yaml:"tls_cert_file"`
	TLSKeyFile          string   `yaml:"tls_key_file"`
}

// CacheConfig contains cache-related configuration
type CacheConfig struct {
	Directory           string        `yaml:"directory"`
	MaxSize             int64         `yaml:"max_size"`
	EvictionPolicy      string        `yaml:"eviction_policy"`
	EvictionThreshold   float64       `yaml:"eviction_threshold"`
	MaintenanceInterval time.Duration `yaml:"maintenance_interval"`
}

// ProxyConfig contains proxy server configuration
type ProxyConfig struct {
	ListenAddr     string        `yaml:"listen_addr"`
	ReadTimeout    time.Duration `yaml:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout"`
	MaxRequestSize int64         `yaml:"max_request_size"`
}

// S3Config contains S3 client configuration
type S3Config struct {
	Endpoint        string        `yaml:"endpoint"`
	Region          string        `yaml:"region"`
	AccessKeyID     string        `yaml:"access_key_id"`
	SecretAccessKey string        `yaml:"secret_access_key"`
	Timeout         time.Duration `yaml:"timeout"`
	MaxRetries      int           `yaml:"max_retries"`

	// Resilience
	EnableCircuitBreaker     bool          `yaml:"enable_circuit_breaker"`
	CircuitFailureThreshold  int64         `yaml:"circuit_failure_threshold"`
	CircuitBreakDuration     time.Duration `yaml:"circuit_break_duration"`
	RetryBackoff             time.Duration `yaml:"retry_backoff"`
	RetryMaxBackoff          time.Duration `yaml:"retry_max_backoff"`
}

// MetricsConfig contains metrics configuration
type MetricsConfig struct {
	Namespace      string        `yaml:"namespace"`
	ListenAddr     string        `yaml:"listen_addr"`
	ReportInterval time.Duration `yaml:"report_interval"`
}

// PrefetchConfig contains prefetching configuration
type PrefetchConfig struct {
	Enabled         bool          `yaml:"enabled"`
	WindowSize      int           `yaml:"window_size"`
	PredictionDepth int           `yaml:"prediction_depth"`
	Workers         int           `yaml:"workers"`
	QueueSize       int           `yaml:"queue_size"`
	Interval        time.Duration `yaml:"interval"`
}

// StorageConfig contains storage-related configuration
type StorageConfig struct {
	Database DatabaseConfig `yaml:"database"`
	Backup   BackupConfig   `yaml:"backup"`
}

// DatabaseConfig contains database configuration
type DatabaseConfig struct {
	Path              string        `yaml:"path"`
	MaxMemoryMB       int64         `yaml:"max_memory_mb"`
	SyncWrites        bool          `yaml:"sync_writes"`
	Compression       bool          `yaml:"compression"`
	EncryptionKey     string        `yaml:"encryption_key"`
	GCInterval        time.Duration `yaml:"gc_interval"`
	ValueLogFileSize  int64         `yaml:"value_log_file_size"`
	NumVersionsToKeep int           `yaml:"num_versions_to_keep"`
}

// BackupConfig contains backup configuration
type BackupConfig struct {
	Enabled   bool          `yaml:"enabled"`
	Path      string        `yaml:"path"`
	Interval  time.Duration `yaml:"interval"`
	Retention time.Duration `yaml:"retention"`
}

// ReplicationConfig contains replication configuration
type ReplicationConfig struct {
	Coordination CoordinationReplicationConfig `yaml:"coordination"`
	Cache        CacheReplicationConfig        `yaml:"cache"`
}

// CoordinationReplicationConfig contains coordination replication config
type CoordinationReplicationConfig struct {
	Method            string        `yaml:"method"`
	RaftDir           string        `yaml:"raft_dir"`
	RaftBind          string        `yaml:"raft_bind"`
	Bootstrap         bool          `yaml:"bootstrap"`
	JoinAddresses     []string      `yaml:"join_addresses"`
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	SnapshotInterval  time.Duration `yaml:"snapshot_interval"`
	SnapshotThreshold uint64        `yaml:"snapshot_threshold"`
}

// CacheReplicationConfig contains cache replication config
type CacheReplicationConfig struct {
	Method             string        `yaml:"method"`
	Fanout             int           `yaml:"fanout"`
	SyncInterval       time.Duration `yaml:"sync_interval"`
	ConflictResolution string        `yaml:"conflict_resolution"`
	MaxUpdateQueueSize int           `yaml:"max_update_queue_size"`
	RetryAttempts      int           `yaml:"retry_attempts"`
	RetryDelay         time.Duration `yaml:"retry_delay"`
}

// ZeroCopyConfig contains io_uring zero-copy configuration
type ZeroCopyConfig struct {
	IOUring IOUringConfig `yaml:"io_uring"`
}

// IOUringConfig configures the io_uring subsystem
type IOUringConfig struct {
	Enabled         bool   `yaml:"enabled"`
	Mode            string `yaml:"mode"`
	QueueDepth      uint32 `yaml:"queue_depth"`
	NumFixedBuffers int    `yaml:"num_fixed_buffers"`
	FixedBufferSize int    `yaml:"fixed_buffer_size"`
	SQPoll          bool   `yaml:"sq_poll"`
}

// Load loads configuration from a YAML file
func Load(filepath string) (*Config, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	expandedData := expandEnvVars(string(data))

	var config Config
	if err := yaml.Unmarshal([]byte(expandedData), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	config.setDefaults()
	return &config, nil
}

func expandEnvVars(config string) string {
	re := regexp.MustCompile(`\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)`)
	return re.ReplaceAllStringFunc(config, func(match string) string {
		var varName string
		var defaultValue string
		if strings.HasPrefix(match, "${") {
			content := match[2 : len(match)-1]
			if strings.Contains(content, ":-") {
				parts := strings.SplitN(content, ":-", 2)
				varName = strings.TrimSpace(parts[0])
				defaultValue = parts[1]
			} else {
				varName = content
			}
		} else {
			varName = match[1:]
		}
		value := os.Getenv(varName)
		if value == "" && defaultValue != "" {
			return defaultValue
		}
		if value == "" {
			return ""
		}
		return value
	})
}

func (c *Config) setDefaults() {
	if c.Cache.Directory == "" { c.Cache.Directory = "/var/cache/redcache" }
	if c.Cache.MaxSize == 0 { c.Cache.MaxSize = 10 * 1024 * 1024 * 1024 }
	if c.Cache.EvictionPolicy == "" { c.Cache.EvictionPolicy = "lru" }
	if c.Cache.EvictionThreshold == 0 { c.Cache.EvictionThreshold = 0.9 }
	if c.Cache.MaintenanceInterval == 0 { c.Cache.MaintenanceInterval = 5 * time.Minute }

	if c.Proxy.ListenAddr == "" { c.Proxy.ListenAddr = ":8080" }
	if c.Proxy.ReadTimeout == 0 { c.Proxy.ReadTimeout = 60 * time.Second }
	if c.Proxy.WriteTimeout == 0 { c.Proxy.WriteTimeout = 60 * time.Second }
	if c.Proxy.MaxRequestSize == 0 { c.Proxy.MaxRequestSize = 100 * 1024 * 1024 }

	if c.Metrics.Namespace == "" { c.Metrics.Namespace = "redcache" }
	if c.Metrics.ListenAddr == "" { c.Metrics.ListenAddr = ":9090" }
	if c.Metrics.ReportInterval == 0 { c.Metrics.ReportInterval = 10 * time.Second }

	if c.Distributed.BindAddr == "" { c.Distributed.BindAddr = "127.0.0.1" }
	if c.Distributed.BindPort == 0 { c.Distributed.BindPort = 7946 }
	if c.Distributed.RPCPort == 0 { c.Distributed.RPCPort = 8946 }
	if c.Distributed.NumVirtualShards == 0 { c.Distributed.NumVirtualShards = 1024 }
	if c.Distributed.ReplicationFactor == 0 { c.Distributed.ReplicationFactor = 3 }
	if c.Distributed.ErasureDataShards == 0 { c.Distributed.ErasureDataShards = 6 }
	if c.Distributed.ErasureParityShards == 0 { c.Distributed.ErasureParityShards = 3 }
	if c.Distributed.ErasureShardSize == 0 { c.Distributed.ErasureShardSize = 1048576 }

	if c.S3.RetryBackoff == 0 { c.S3.RetryBackoff = 100 * time.Millisecond }
	if c.S3.RetryMaxBackoff == 0 { c.S3.RetryMaxBackoff = 10 * time.Second }
	if c.S3.CircuitFailureThreshold == 0 { c.S3.CircuitFailureThreshold = 10 }
	if c.S3.CircuitBreakDuration == 0 { c.S3.CircuitBreakDuration = 30 * time.Second }

	if c.ZeroCopy.IOUring.QueueDepth == 0 { c.ZeroCopy.IOUring.QueueDepth = 256 }
	if c.ZeroCopy.IOUring.NumFixedBuffers == 0 { c.ZeroCopy.IOUring.NumFixedBuffers = 64 }
	if c.ZeroCopy.IOUring.FixedBufferSize == 0 { c.ZeroCopy.IOUring.FixedBufferSize = 1 * 1024 * 1024 }
}

func (c *Config) Validate() error {
	if c.Cache.MaxSize <= 0 { return fmt.Errorf("cache.max_size must be positive") }
	if c.Cache.EvictionThreshold <= 0 || c.Cache.EvictionThreshold > 1 { return fmt.Errorf("cache.eviction_threshold must be between 0 and 1") }
	return nil
}
