# RedCache Configuration Guide

## Overview

RedCache supports three configuration files for different use cases:

1. **config.minimal.yaml** - Quick start with essential settings
2. **config.example.yaml** - Complete reference with all options documented
3. **config.production.yaml** - Production-ready configuration

## Configuration Files

### 1. Minimal Configuration (`config.minimal.yaml`)

**Use Case**: Development, testing, single-node deployments

**Features**:
- Essential settings only
- Quick to understand and modify
- Suitable for getting started
- ~50 lines

**Example**:
```yaml
server:
  address: ":8080"
cache:
  directory: "/cache"
  max_size: 10737418240  # 10GB
s3:
  region: "us-east-1"
```

### 2. Complete Reference (`config.example.yaml`)

**Use Case**: Reference documentation, understanding all options

**Features**:
- Every configuration option documented
- Detailed explanations and examples
- Multiple scenario examples
- Performance tuning guidance
- ~600 lines with extensive comments

**Sections**:
- Server Configuration
- Cache Configuration
- S3 Backend
- Prefetching
- Tiered Caching
- Distributed Coordination
- Monitoring & Metrics
- Logging
- Health Checks
- Security
- Performance Tuning

### 3. Production Configuration (`config.production.yaml`)

**Use Case**: Production GPU training clusters

**Features**:
- Optimized for production workloads
- Multi-node cluster support
- Advanced features enabled
- Performance-tuned settings
- ~200 lines

**Optimizations**:
- 1TB cache size
- Adaptive eviction policy
- Aggressive prefetching
- 10GB memory tier
- 3x replication
- Advanced monitoring

## Configuration Sections

### Server Configuration

```yaml
server:
  address: ":8080"              # Listen address
  read_timeout: "30s"           # Request read timeout
  write_timeout: "30s"          # Response write timeout
  idle_timeout: "60s"           # Keep-alive timeout
  max_header_bytes: 1048576     # Max header size (1MB)
```

**Key Settings**:
- `address`: Port to listen on (default: `:8080`)
- `read_timeout`: Increase for large objects (30-60s)
- `write_timeout`: Increase for slow clients (30-60s)

### Cache Configuration

```yaml
cache:
  directory: "/cache"           # Storage directory
  max_size: 10737418240         # 10GB (bytes only - no suffixes)
  eviction_policy: "lru"        # LRU, LFU, ARC, adaptive
  high_water_mark: 0.9          # Start eviction at 90%
  low_water_mark: 0.8           # Stop eviction at 80%
  enable_compression: true
  compression_algorithm: "zstd"
  enable_deduplication: true
```

**Key Settings**:
- `directory`: Use dedicated NVMe mount (e.g., `/mnt/nvme0/cache`)
- `max_size`: Set to 80-90% of available NVMe space (integer bytes only - see SIZE-REFERENCE.md for conversions)
- `eviction_policy`: 
  - `lru`: Simple, fast (default)
  - `adaptive`: Auto-switches between policies (recommended for production)
- `compression_algorithm`:
  - `zstd`: Best compression ratio (recommended)
  - `lz4`: Fastest compression
  - `gzip`: Standard compression

### S3 Backend Configuration

```yaml
s3:
  region: "us-east-1"           # AWS region
  endpoint: ""                  # Custom endpoint (optional)
  max_retries: 3                # Retry attempts
  timeout: "30s"                # Request timeout
  max_concurrent_requests: 100  # Concurrency limit
```

**Key Settings**:
- `region`: Your AWS region
- `endpoint`: Leave empty for AWS S3, or specify for S3-compatible storage
- `max_concurrent_requests`: Increase for high throughput (100-200)

### Prefetching Configuration

```yaml
prefetch:
  enabled: true
  strategy: "adaptive"          # sequential, strided, temporal, ml, adaptive
  prefetch_ahead: 5             # Number of objects to prefetch
  confidence_threshold: 0.7     # ML prediction confidence
  worker_threads: 4             # Concurrent prefetch operations
```

**Key Settings**:
- `strategy`:
  - `sequential`: Best for sequential data access
  - `adaptive`: Auto-selects best strategy (recommended)
- `prefetch_ahead`: Increase for aggressive prefetching (5-10)
- `confidence_threshold`: Lower for more prefetching (0.6-0.8)

### Tiered Caching Configuration

```yaml
tiered_cache:
  enabled: true
  memory_tier_size: 1073741824  # 1GB hot data in memory
  auto_tiering: true
  promotion_threshold: 3        # Promote after N accesses
  demotion_threshold: "5m"      # Demote after idle time
```

**Key Settings**:
- `memory_tier_size`: 10-20% of total cache size
- `promotion_threshold`: Lower for more aggressive promotion (2-5)
- `demotion_threshold`: Adjust based on access patterns (1m-10m)

### Distributed Configuration

```yaml
distributed:
  enabled: true
  node_id: "gpu-node-01"        # Unique node identifier
  bind_addr: "0.0.0.0"          # Bind address for cluster communication
  bind_port: 7946               # Port for gossip protocol (Memberlist)
  rpc_port: 8946                # Port for gRPC communication
  seeds:                        # Seed nodes for cluster discovery
    - "gpu-node-02:7946"        # Use bind_port, not HTTP port
    - "gpu-node-03:7946"
  enable_quorum: true           # Require majority consensus
  conflict_resolution: "last-write-wins"  # Conflict resolution strategy
  health_check_interval: "5s"  # Health check frequency
  health_check_timeout: "2s"   # Health check timeout
  sync_interval: "10s"          # Data sync frequency
  replication_factor: 3         # Number of replicas
  enable_gossip: true           # Enable gossip protocol
  
  # Auto-Rebalancing
  enable_auto_rebalance: true   # Enable automatic data rebalancing
  rebalance_batch_size: 100    # Keys per batch during rebalancing
  rebalance_throttle: "10ms"   # Delay between batches
```

**Key Settings**:
- `node_id`: Must be unique for each node
- `bind_port`: Port for cluster membership (default: 7946)
- `rpc_port`: Port for node-to-node RPC (default: 8946)
- `seeds`: List seed nodes using bind_port (7946), not HTTP port
- `enable_quorum`: Enable quorum-based operations for consistency
- `conflict_resolution`: Strategy for resolving conflicts
- `replication_factor`: 3 for production (fault tolerance)
- `enable_auto_rebalance`: Enable automatic data rebalancing (default: true)
- `rebalance_batch_size`: Keys per batch (default: 100, increase for faster rebalancing)
- `rebalance_throttle`: Delay between batches (default: 10ms, increase to reduce network load)

### Monitoring Configuration

```yaml
metrics:
  enabled: true
  path: "/metrics"              # Prometheus endpoint
  detailed: false               # Detailed per-object metrics

monitoring:
  enable_analytics: true
  enable_anomaly_detection: true
  enable_auto_tuning: true
  enable_cost_optimization: true
```

**Key Settings**:
- `detailed`: Enable for debugging (increases memory usage)
- `enable_auto_tuning`: Automatically optimizes parameters

## Environment Variables

### AWS Credentials

```bash
# Option 1: Environment variables (development)
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1

# Option 2: IAM roles (production - recommended)
# Attach IAM role to EC2 instance or EKS pod

# Option 3: Credentials file
# ~/.aws/credentials
```

### Configuration Override

```bash
# Override config file location
export REDCACHE_CONFIG=/path/to/config.yaml

# Override specific settings
export REDCACHE_CACHE_SIZE=1099511627776  # 1TB
export REDCACHE_LOG_LEVEL=debug
```

## Configuration Examples

### Example 1: Single GPU Node (Development)

```yaml
server:
  address: ":8080"

cache:
  directory: "/tmp/cache"
  max_size: 1073741824          # 1GB

s3:
  region: "us-east-1"

distributed:
  enabled: false

logging:
  level: "debug"
```

### Example 2: Production Cluster (100 nodes)

```yaml
cache:
  directory: "/mnt/nvme0/cache"
  max_size: 1099511627776       # 1TB per node
  eviction_policy: "adaptive"

prefetch:
  enabled: true
  strategy: "adaptive"
  prefetch_ahead: 10

tiered_cache:
  enabled: true
  memory_tier_size: 10737418240 # 10GB

distributed:
  enabled: true
  node_id: "gpu-node-01"
  peers: ["gpu-node-02:8080", "gpu-node-03:8080"]
  replication_factor: 3

monitoring:
  enable_analytics: true
  enable_auto_tuning: true
```

### Example 3: High-Performance Training

```yaml
cache:
  directory: "/mnt/nvme0/cache"
  max_size: 2199023255552       # 2TB
  eviction_policy: "lru"

prefetch:
  enabled: true
  strategy: "sequential"
  prefetch_ahead: 20            # Very aggressive

tiered_cache:
  enabled: true
  memory_tier_size: 21474836480 # 20GB
  promotion_threshold: 2

performance:
  read_buffer_size: 131072      # 128KB
  write_buffer_size: 131072
  max_connections: 20000
```

### Example 4: Cost-Optimized Setup

```yaml
cache:
  directory: "/mnt/nvme0/cache"
  max_size: 536870912000        # 500GB
  enable_compression: true
  compression_algorithm: "zstd"
  compression_level: "best"
  enable_deduplication: true

monitoring:
  enable_cost_optimization: true

s3:
  max_concurrent_requests: 50   # Lower concurrency
```

## Configuration Validation

RedCache validates configuration on startup:

```bash
# Test configuration
./cache-agent --config config.yaml --validate

# Dry run (validate without starting)
./cache-agent --config config.yaml --dry-run
```

**Common Validation Errors**:

1. **Invalid cache size**
   ```
   Error: cache.max_size must be positive
   ```
   Solution: Set `max_size` to a positive integer

2. **Invalid directory**
   ```
   Error: cache.directory does not exist or is not writable
   ```
   Solution: Create directory with proper permissions

3. **Invalid eviction policy**
   ```
   Error: cache.eviction_policy must be one of: lru, lfu, arc, adaptive
   ```
   Solution: Use a valid policy name

4. **Invalid node configuration**
   ```
   Error: distributed.node_id is required when distributed.enabled is true
   ```
   Solution: Set unique `node_id` for each node

## Performance Tuning

### Cache Size

```yaml
# Development: 1-10GB
max_size: 10737418240

# Production: 500GB-2TB
max_size: 1099511627776

# High-performance: 2-4TB
max_size: 2199023255552
```

**Recommendation**: Use 80-90% of available NVMe space

### Eviction Policy

```yaml
# Simple workloads
eviction_policy: "lru"

# Mixed workloads
eviction_policy: "adaptive"

# Repeated access patterns
eviction_policy: "lfu"
```

### Prefetching

```yaml
# Conservative (low overhead)
prefetch_ahead: 3
confidence_threshold: 0.8

# Balanced (recommended)
prefetch_ahead: 5
confidence_threshold: 0.7

# Aggressive (high hit rate)
prefetch_ahead: 10
confidence_threshold: 0.6
```

### Memory Tier

```yaml
# Small (1-2GB)
memory_tier_size: 1073741824

# Medium (5-10GB)
memory_tier_size: 10737418240

# Large (20-50GB)
memory_tier_size: 21474836480
```

**Recommendation**: 10-20% of total cache size

## Monitoring Configuration

### Prometheus Integration

```yaml
metrics:
  enabled: true
  path: "/metrics"
```

**Scrape Configuration**:
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'redcache'
    static_configs:
      - targets: ['gpu-node-01:8080', 'gpu-node-02:8080']
```

### Key Metrics

- `redcache_cache_hit_rate` - Cache hit rate (target: >85%)
- `redcache_cache_hit_latency` - Cache hit latency (target: <100μs)
- `redcache_cache_size_bytes` - Current cache size
- `redcache_prefetch_accuracy` - Prefetch accuracy (target: >80%)
- `redcache_s3_requests_total` - Total S3 requests

## Troubleshooting

### High Cache Miss Rate

```yaml
# Increase cache size
cache:
  max_size: 2199023255552       # 2TB

# Enable aggressive prefetching
prefetch:
  prefetch_ahead: 10
  confidence_threshold: 0.6

# Increase memory tier
tiered_cache:
  memory_tier_size: 21474836480 # 20GB
```

### High Latency

```yaml
# Increase memory tier
tiered_cache:
  memory_tier_size: 21474836480
  promotion_threshold: 2

# Optimize buffers
performance:
  read_buffer_size: 131072
  write_buffer_size: 131072
```

### High Memory Usage

```yaml
# Reduce memory tier
tiered_cache:
  memory_tier_size: 1073741824  # 1GB

# Disable detailed metrics
metrics:
  detailed: false

# Reduce prefetch queue
prefetch:
  max_queue_size: 50
```

## Best Practices

1. **Start with minimal config** - Add features as needed
2. **Use adaptive policies** - Let the system optimize itself
3. **Monitor metrics** - Track hit rate, latency, and throughput
4. **Enable auto-tuning** - Automatically optimize parameters
5. **Use IAM roles** - Don't hardcode AWS credentials
6. **Dedicated NVMe** - Use separate mount for cache
7. **Test configuration** - Validate before deploying
8. **Document changes** - Keep track of configuration changes

## Additional Resources

- **SIZE-REFERENCE.md** - Size conversion reference (bytes calculator)
- **TESTING.md** - Testing and validation guide
- **STANDALONE-DEPLOYMENT.md** - Deployment instructions
- **FILESYSTEM-RECOMMENDATIONS.md** - Storage setup guide
- **MULTI-NVME-SETUP.md** - Multi-device configuration
- **AWS-STYLE-PRD.md** - Product requirements and architecture