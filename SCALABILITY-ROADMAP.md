# RedCache Scalability Roadmap

This document outlines the strategic phases for scaling RedCache from its current hardened state to petascale operations.

---

## ✅ Phase 1: Virtual Sharding & Multi-Raft (Completed)
*   **Goal:** Decouple coordination from node count.
*   **Implementation:**
    *   1,024-slot Virtual Sharding (VShard) routing.
    *   Multi-tenant Raft groups for sharded state management.
    *   Shared TCP transport for heartbeat multiplexing.

## ✅ Phase 2: Kernel-Level Zero-Copy (Completed)
*   **Goal:** Saturate 100GbE+ networks with minimal CPU overhead.
*   **Implementation:**
    *   `io_uring` implementation for disk and network I/O.
    *   Page-aligned `mmap`/`mlock` buffer management.
    *   `IORING_OP_SPLICE` for direct NVMe-to-NIC handoff.

## 🚀 Phase 3: RDMA & DPDK Integration (Planned)
*   **Goal:** Bypass the Linux kernel networking stack entirely.
*   **Target:** InfiniBand and RoCE v2 environments.
*   **Focus:** Sub-100 microsecond remote shard retrieval.

## 🚀 Phase 4: Tiered Multi-Cloud Backends (Planned)
*   **Goal:** Unified namespace across multiple cloud providers.
*   **Target:** Native drivers for AWS S3, Azure Blob, GCS, and OCI Object Storage.
*   **Feature:** Automatic data migration between cloud tiers based on cost/access patterns.

## 🚀 Phase 5: Global Cache Coherence (Planned)
*   **Goal:** Strictly consistent caching across geographically distributed sites.
*   **Implementation:** Multi-site MESI coherence extension.

---
**Version:** 2.0.0-stable  
**Last Updated:** 2026-02-24
