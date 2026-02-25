# RedCache: High-Performance Distributed S3 Proxy Cache

RedCache is an OCI-grade, transparent S3 proxy cache designed to accelerate data-heavy workloads (AI/ML training, large-scale data processing) while drastically reducing S3 API overhead and latency.

**Transparent Integration:** No code changes required. Simply repoint your existing S3 client's endpoint to the RedCache proxy.

---

## 🚀 Key Features

### 🛠️ Transparent S3 Proxying
- **Drop-in Replacement:** Works with any standard S3 client (AWS SDK, Boto3, Go SDK).
- **Zero Code Changes:** Just update your `endpoint_url` or `S3_ENDPOINT` environment variable.

### ⚡ Hardware-First Performance
- **io_uring Integration:** High-performance, asynchronous I/O for NVMe and networking (Linux 6.0+).
- **Zero-Copy Architecture:** Uses `mmap`, `mlock`, and kernel-level `splice` to move data without CPU-side buffer copies.
- **AVX-512 Acceleration:** Hardware-accelerated Erasure Coding (EC) for sub-millisecond data reconstruction.

### 🛡️ Resilience & Reliability
- **Adaptive Backoff (Full Jitter):** Prevents thundering herds by desynchronizing retries using exponential curves and random jitter.
- **S3 Circuit Breaker:** Protects your S3 quota by automatically tripping and "failing fast" if the object store hits rate limits (503 Slow Down).
- **Request Coalescing:** Uses `singleflight` to ensure only one request per key is ever in-flight to S3, even with thousands of concurrent local requesters.

### 🌐 Distributed Intelligence
- **Virtual Sharding (1,024 slots):** Seamless scaling from 3 to 1,000+ nodes without migration cliffs.
- **Multi-Raft Sharding:** Independent Raft groups per shard for high-concurrency metadata operations.
- **Merkle Tree Anti-Entropy:** Background state synchronization to ensure data durability and consistency across the cluster.
- **Auto-Rebalancing:** Automatically redistributes data when nodes join or leave the cluster.

### 🧠 Advanced Logic
- **ML-Based Prefetching:** Predicts and warms the cache based on observed access patterns.
- **Adaptive Eviction:** Dynamically switches between LRU, LFU, and ARC based on workload characteristics.
- **BadgerDB Meta-Store:** High-performance persistent metadata tracking.

---

## 🛠️ Getting Started

### 1. Configure
Edit `config.production.yaml` to set your NVMe mount point, S3 credentials, and cluster seeds.

### 2. Deploy
Use the provided Makefile for Docker or Kubernetes deployment:
```bash
make build-linux
make deploy
```

### 3. Use
Repoint your application to the RedCache endpoint (default `:8080`):
```python
import boto3
# No code changes beyond the endpoint_url
s3 = boto3.client('s3', endpoint_url='http://redcache-lb:8080')
```

---

## 📊 Monitoring
RedCache exposes Prometheus metrics on `:9090/metrics`, including:
- Cache Hit/Miss ratios
- `io_uring` submission/completion queue depths
- S3 Circuit Breaker trips and blocks
- Distributed shard health status

