# YADB (Yet Another DB & Data Platform) — Architecture

`yadb` is a modular distributed storage and database framework inspired by **YDB's BlobStorage + Tablets** architecture and **Kubernetes' declarative control plane**, engineered to run cleanly over standard Linux filesystems without raw `PDisk` block devices.

---

## 1. Layered Architecture

```text
┌─────────────────────────────────────────────────────────────────────────┐
│ Declarative Control Plane (Kubernetes-style YAML: apply / plan / watch) │
│ Kinds: Cluster | StoragePool | Project | Database | ObjectStore | Disk  │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ Watch / Local Auth & Quota Cache
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐           ┌───────────────┐           ┌───────────────┐
│  DB Service   │           │  S3 Gateway   │           │ Block Volume  │
│ (Tablets/SQL) │           │ (Object API)  │           │  (NBD/Disk)   │
└───────┬───────┘           └───────┬───────┘           └───────┬───────┘
        │                           │                           │
        └───────────────────────────┼───────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Kernel SDK: DSProxy (Erasure Coding RS k+m / Mirror-3 + Hedged Reads)   │
│           + MetaKV (Raft-replicated ordered metadata, topology & RBAC)  │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Storage Node: Append-Only Volume Engine (on ext4/xfs, no raw PDisk)     │
│ 128-bit BlobId (Project+Owner+Gen+Step+Part), CRC32, Slice Read, GC     │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ yadb-io: StorageFs Trait (Linux OS Filesystem | Deterministic SimFs)    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core Design Invariants

1. **Immutability at the Blob Layer:** Blobs are written once and never mutated in place. Because mutations do not exist in the blob layer, blob placement and fragment writes require **no distributed consensus**—only quorum/erasure writes and generation barriers.
2. **Append-Only Volumes Instead of One-File-Per-Blob:** To avoid `ext4`/`xfs` inode exhaustion and directory `fsync` stalls, fragments are packed into large append-only segment files (`vol_XXXXXXXX.dat`) with framing headers, CRC32 checksums, `read_slice(id, offset, len)` support, tail crash recovery, and background compaction.
3. **Zero-Overhead Multi-Tenancy (`Project`):**
   - Every `BlobId` and `MetaKV` key is prefixed by `project_id: u32` and `owner_id: u32`.
   - RBAC, quotas, and pool mappings are watched and cached in-memory on gateways/nodes (`~0%` hot-path network overhead).
   - Workloads support `isolation: shared` (virtual multi-tenant, near-zero RAM overhead) and `isolation: dedicated` (isolated OS worker processes).
4. **Simulation-First Verification (`SimFs`):** Every storage invariant (power loss before `fsync`, torn writes, corrupted bytes, missing erasure fragments) is tested deterministically by seed in memory before touching real disks.

---

## 3. Workspace Crates

- `crates/yadb-io` — `StorageFs` & `VirtualFile` traits, real `OsFs` backend, and deterministic fault-injecting `SimFs` (models dirty page cache loss on power cut, torn writes, and deterministic seeded I/O faults).
- `crates/yadb-blob` — 128-bit `BlobId`, `VolumeStore` (append-only slab engine, crash recovery, slice reads, barrier deletion, compaction), and `ErasureCodec` (`Mirror` and `ReedSolomon` over $GF(2^8)$) + `PoolStriper` distributing fragments across failure domains.
- `crates/yadb-control` — Zero-dependency K8s-style YAML manifest parser, resource schemas (`Cluster`, `StoragePool`, `Project`, `Database`, `ObjectStore`, `BlockVolume`), referential validator, and declarative `StateReconciler` (`plan` / `apply` with generations).
- `crates/yadbctl` — Unified CLI (`yadbctl plan`, `yadbctl apply`, `yadbctl status`, `yadbctl auth-check`, `yadbctl sim-crash`, `yadbctl demo-pool`).
