# Environment Variables in Configuration

RedCache supports environment variable substitution in configuration files, allowing you to use dynamic values and keep sensitive information out of your config files.

## Syntax

RedCache supports three syntax formats for environment variables:

### 1. Basic Syntax: `${VAR}`
```yaml
distributed:
  node_id: "${HOSTNAME}"
  bind_addr: "${BIND_ADDRESS}"
```

### 2. Short Syntax: `$VAR`
```yaml
distributed:
  node_id: "$HOSTNAME"
  bind_addr: "$BIND_ADDRESS"
```

### 3. Default Value Syntax: `${VAR:-default}`
```yaml
distributed:
  node_id: "${HOSTNAME:-node-1}"
  bind_addr: "${BIND_ADDRESS:-0.0.0.0}"
  bind_port: ${BIND_PORT:-7946}
```

## Common Use Cases

### 1. Node Identification

Use the hostname as the node ID:

```yaml
distributed:
  enabled: true
  node_id: "${HOSTNAME}"  # Automatically uses system hostname
  bind_addr: "0.0.0.0"
  bind_port: 7946
```

Set the environment variable:
```bash
export HOSTNAME=$(hostname)
# or let the system provide it automatically
```

### 2. Dynamic Port Configuration

```yaml
server:
  address: ":${HTTP_PORT:-8080}"

distributed:
  bind_port: ${GOSSIP_PORT:-7946}
  rpc_port: ${RPC_PORT:-8946}
```

Set custom ports:
```bash
export HTTP_PORT=8081
export GOSSIP_PORT=7947
export RPC_PORT=8947
```

### 3. Environment-Specific Settings

```yaml
cache:
  directory: "${CACHE_DIR:-/data/cache}"
  max_size: ${CACHE_SIZE:-107374182400}  # 100GB default

logging:
  level: "${LOG_LEVEL:-info}"
  output: "${LOG_FILE:-/var/log/redcache/cache.log}"
```

Development environment:
```bash
export CACHE_DIR=/tmp/cache
export CACHE_SIZE=1073741824  # 1GB for dev
export LOG_LEVEL=debug
export LOG_FILE=/tmp/redcache.log
```

Production environment:
```bash
export CACHE_DIR=/mnt/nvme/cache
export CACHE_SIZE=1099511627776  # 1TB for production
export LOG_LEVEL=info
export LOG_FILE=/var/log/redcache/cache.log
```

### 4. Sensitive Credentials

Keep AWS credentials out of config files:

```yaml
s3:
  region: "${AWS_REGION:-us-west-2}"
  access_key_id: "${AWS_ACCESS_KEY_ID}"
  secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
  endpoint: "${S3_ENDPOINT}"
```

Set credentials:
```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export S3_ENDPOINT=https://s3.amazonaws.com
```

### 5. Cluster Configuration

```yaml
distributed:
  enabled: true
  node_id: "${NODE_ID:-${HOSTNAME}}"
  bind_addr: "${BIND_ADDR:-0.0.0.0}"
  bind_port: ${BIND_PORT:-7946}
  rpc_port: ${RPC_PORT:-8946}
  seeds:
    - "${SEED_NODE_1}"
    - "${SEED_NODE_2}"
    - "${SEED_NODE_3}"
```

Configure cluster:
```bash
export NODE_ID=gpu-node-01
export BIND_ADDR=10.0.1.10
export SEED_NODE_1=gpu-node-02:7946
export SEED_NODE_2=gpu-node-03:7946
export SEED_NODE_3=gpu-node-04:7946
```

## Complete Example

### Configuration File (`config.yaml`)

```yaml
server:
  address: ":${HTTP_PORT:-8080}"
  read_timeout: "${READ_TIMEOUT:-30s}"
  write_timeout: "${WRITE_TIMEOUT:-30s}"

cache:
  directory: "${CACHE_DIR:-/data/cache}"
  max_size: ${CACHE_SIZE:-107374182400}
  eviction_policy: "${EVICTION_POLICY:-lru}"
  enable_compression: ${ENABLE_COMPRESSION:-true}
  compression_algorithm: "${COMPRESSION_ALGO:-zstd}"

distributed:
  enabled: ${DISTRIBUTED_ENABLED:-false}
  node_id: "${NODE_ID:-${HOSTNAME}}"
  bind_addr: "${BIND_ADDR:-0.0.0.0}"
  bind_port: ${BIND_PORT:-7946}
  rpc_port: ${RPC_PORT:-8946}
  seeds:
    - "${SEED_1}"
    - "${SEED_2}"
  enable_quorum: ${ENABLE_QUORUM:-true}
  conflict_resolution: "${CONFLICT_RESOLUTION:-last-write-wins}"
  health_check_interval: "${HEALTH_CHECK_INTERVAL:-5s}"
  sync_interval: "${SYNC_INTERVAL:-10s}"

s3:
  region: "${AWS_REGION:-us-west-2}"
  access_key_id: "${AWS_ACCESS_KEY_ID}"
  secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
  endpoint: "${S3_ENDPOINT}"

metrics:
  enabled: ${METRICS_ENABLED:-true}
  path: "${METRICS_PATH:-/metrics}"
  port: "${METRICS_PORT:-9090}"

logging:
  level: "${LOG_LEVEL:-info}"
  format: "${LOG_FORMAT:-json}"
  output: "${LOG_FILE:-/var/log/redcache/cache.log}"
```

### Environment File (`.env`)

```bash
# Server Configuration
HTTP_PORT=8080
READ_TIMEOUT=30s
WRITE_TIMEOUT=30s

# Cache Configuration
CACHE_DIR=/mnt/nvme/cache
CACHE_SIZE=1099511627776  # 1TB
EVICTION_POLICY=lru
ENABLE_COMPRESSION=true
COMPRESSION_ALGO=zstd

# Distributed Configuration
DISTRIBUTED_ENABLED=true
NODE_ID=gpu-node-01
BIND_ADDR=10.0.1.10
BIND_PORT=7946
RPC_PORT=8946
SEED_1=gpu-node-02:7946
SEED_2=gpu-node-03:7946
ENABLE_QUORUM=true
CONFLICT_RESOLUTION=last-write-wins
HEALTH_CHECK_INTERVAL=5s
SYNC_INTERVAL=10s

# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
S3_ENDPOINT=https://s3.amazonaws.com

# Metrics Configuration
METRICS_ENABLED=true
METRICS_PATH=/metrics
METRICS_PORT=9090

# Logging Configuration
LOG_LEVEL=info
LOG_FORMAT=json
LOG_FILE=/var/log/redcache/cache.log
```

### Loading Environment Variables

#### Option 1: Export in Shell
```bash
export HTTP_PORT=8080
export NODE_ID=$(hostname)
export CACHE_DIR=/data/cache
./redcache -config config.yaml
```

#### Option 2: Use .env File
```bash
# Load from .env file
set -a
source .env
set +a
./redcache -config config.yaml
```

#### Option 3: Inline with Command
```bash
HTTP_PORT=8080 NODE_ID=node-1 ./redcache -config config.yaml
```

#### Option 4: Systemd Service
```ini
[Service]
Environment="HTTP_PORT=8080"
Environment="NODE_ID=gpu-node-01"
Environment="CACHE_DIR=/data/cache"
EnvironmentFile=/etc/redcache/environment
ExecStart=/usr/local/bin/redcache -config /etc/redcache/config.yaml
```

#### Option 5: Docker/Kubernetes
```yaml
# Docker Compose
services:
  redcache:
    image: redcache:latest
    environment:
      - HTTP_PORT=8080
      - NODE_ID=node-1
      - CACHE_DIR=/data/cache
    env_file:
      - .env

# Kubernetes ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: redcache-env
data:
  HTTP_PORT: "8080"
  NODE_ID: "node-1"
  CACHE_DIR: "/data/cache"
```

## Best Practices

### 1. Use Defaults for Non-Sensitive Values
```yaml
# Good: Provides sensible defaults
distributed:
  node_id: "${NODE_ID:-${HOSTNAME}}"
  bind_port: ${BIND_PORT:-7946}

# Avoid: Requires environment variable
distributed:
  node_id: "${NODE_ID}"  # Fails if NODE_ID not set
```

### 2. Keep Secrets in Environment Variables
```yaml
# Good: Credentials from environment
s3:
  access_key_id: "${AWS_ACCESS_KEY_ID}"
  secret_access_key: "${AWS_SECRET_ACCESS_KEY}"

# Bad: Credentials in config file
s3:
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCY"
```

### 3. Use Descriptive Variable Names
```yaml
# Good: Clear and specific
distributed:
  node_id: "${REDCACHE_NODE_ID:-${HOSTNAME}}"
  bind_addr: "${REDCACHE_BIND_ADDR:-0.0.0.0}"

# Avoid: Generic names that might conflict
distributed:
  node_id: "${ID}"
  bind_addr: "${ADDR}"
```

### 4. Document Required Variables
Create a `.env.example` file:
```bash
# Required Variables
NODE_ID=node-1
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here

# Optional Variables (with defaults)
HTTP_PORT=8080
CACHE_DIR=/data/cache
LOG_LEVEL=info
```

### 5. Validate Environment Variables
```bash
#!/bin/bash
# validate-env.sh

required_vars=(
  "NODE_ID"
  "AWS_ACCESS_KEY_ID"
  "AWS_SECRET_ACCESS_KEY"
)

for var in "${required_vars[@]}"; do
  if [ -z "${!var}" ]; then
    echo "Error: Required environment variable $var is not set"
    exit 1
  fi
done

echo "All required environment variables are set"
```

## Troubleshooting

### Variable Not Expanding

**Problem:** Variable shows as literal `${VAR}` in logs

**Solution:** Check that the variable is exported:
```bash
# Wrong
VAR=value
./redcache -config config.yaml

# Correct
export VAR=value
./redcache -config config.yaml
```

### Empty Values

**Problem:** Configuration uses empty string instead of default

**Solution:** Use the default syntax:
```yaml
# This will be empty if VAR is not set
node_id: "${VAR}"

# This will use "default" if VAR is not set or empty
node_id: "${VAR:-default}"
```

### Special Characters in Values

**Problem:** Values with special characters cause parsing errors

**Solution:** Quote the entire value:
```yaml
# Correct
secret: "${SECRET_KEY}"

# Also correct for complex values
connection_string: "${DB_CONN:-postgresql://user:pass@localhost:5432/db}"
```

## Security Considerations

1. **Never commit `.env` files** with real credentials to version control
2. **Use `.env.example`** with placeholder values for documentation
3. **Restrict file permissions** on environment files:
   ```bash
   chmod 600 .env
   ```
4. **Use secret management** systems in production (Vault, AWS Secrets Manager, etc.)
5. **Rotate credentials** regularly
6. **Audit environment variable** usage in logs (avoid logging sensitive values)

## Summary

Environment variables in RedCache configuration provide:
- ✅ Dynamic configuration based on environment
- ✅ Secure handling of sensitive credentials
- ✅ Easy deployment across different environments
- ✅ Integration with container orchestration systems
- ✅ Simplified configuration management

Use the `${VAR:-default}` syntax for maximum flexibility and reliability!