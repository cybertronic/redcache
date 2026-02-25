# RedCache Deployment Guide

## Hardware Requirements
- **Storage:** NVMe SSDs (formatted with XFS).
- **Network:** 10GbE+ recommended (support for Linux 6.0+ `SEND_ZC`).
- **Memory:** 1GB+ per virtual shard leader.

## Kernel Configuration
To utilize `io_uring` and `mlock` performance:
1. Increase `memlock` limits in `/etc/security/limits.conf`:
   ```
   * soft memlock unlimited
   * hard memlock unlimited
   ```
2. Ensure `CONFIG_IO_URING` is enabled (Standard on Ubuntu 22.04+).

## Deployment Steps
1. **Bootstrap Phase:** Start the first node with `bootstrap: true` in `replication.coordination`.
2. **Expansion:** Start subsequent nodes with the first node's address in `distributed.seeds`.
3. **Validation:** Monitor `/metrics` for `cache_shards_active` count.
