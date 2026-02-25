# RedCache Scripts

Operational and testing utilities for the RedCache cluster.

## Configuration Directory (`/config`)
Contains sample production-grade configurations for sharded nodes.

- `node-1.yaml`: Initial bootstrap node.
- `node-2.yaml`, `node-3.yaml`: Cluster peers.

## Deployment Utilities

### `docker-compose.yml`
Launch a local 3-node sharded cluster with `io_uring` capabilities. 
*Note: Requires `CAP_IPC_LOCK` and `CAP_SYS_RESOURCE`.*

### `run-distributed-test.sh`
Performs a full cluster orchestration test:
1. Bootstraps 3 nodes.
2. Verifies VShard routing.
3. Tests zero-copy data transfer.

### `setup-localstack.sh`
Initializes a local S3-compatible backend (MinIO/Localstack) for tiered storage testing.

## Observability

- `prometheus.yml`: Scraper config for RedCache nodes (8946/9090 metrics).
- `grafana-dashboard.yml`: Pre-configured dashboard for VShard health and io_uring throughput.
- `alerts.yml`: Prometheus alerting rules for Raft leader loss or high replication lag.

## Maintenance

- `generate-test-data.sh`: Creates high-entropy tensor data for benchmarking deduplication and erasure coding.
