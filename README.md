# YADB — Yet Another DB & Declarative Storage Platform

`yadb` is a modular Rust distributed storage and multi-tenant data platform inspired by **YDB's immutable `BlobStorage` + Tablets** and **Kubernetes' declarative YAML control plane**—designed to run directly on standard Linux filesystems (`ext4`/`xfs`) without raw `PDisk` block devices.

## Highlights

- **Immutable Append-Only `VolumeStore` + Erasure Coding (`yadb-blob`):**
  - Packs fragments into CRC32-verified segment slabs (`vol_XXXXXXXX.dat`) with crash tail recovery, `get_slice` partial reads, generation barrier GC, and segment compaction.
  - Built-in `Mirror` (low-latency WAL) and systematic `ReedSolomon { k, m }` over $GF(2^8)$ (`PoolStriper`) with automatic bit-rot recovery and `repair_blob` self-healing.
- **Raft Consensus + Ordered `MetaKV` (`yadb-metakv`):**
  - Durable `RaftWal`, multi-node elections & quorum log replication (§5.4.2), CAS by per-key `version`, lexicographical prefix scans, and `Watch` streams.
- **Kubernetes-Style Declarative Control Plane & Zero-Hop Multi-Tenancy (`yadb-control`):**
  - Declarative `Cluster`, `StoragePool`, `Project`, `Database`, `ObjectStore`, and `BlockVolume` manifests reconciled via `yadbctl plan` / `yadbctl apply`.
  - Per-project RBAC (`readOnly`, `readWrite`, `admin`) cached locally in memory via `MetaKV` watches (`~0` network hops on the hot path).
- **Three Pluggable Data Services over One Storage Kernel:**
  1. **`yadb-db` (Sharded SQL & Tablets):** Dual-channel YDB-style `DataShardTablet` writing WALs to `walPool` (`Mirror-3`) and flushing sorted SSTables to `dataPool` (`ReedSolomon 4+2`) with CAS generation fencing.
  2. **`yadb-s3` (Object Storage):** Multi-chunk S3 engine with strong read-after-write consistency, `Range` reads, and sorted prefix listing.
  3. **`yadb-block` (Virtual Disks):** Thin-provisioned Copy-on-Write 4 KiB block volumes.

## Quickstart

```bash
# 1. Build & run deterministic simulation + unit tests
cargo test --offline

# 2. Start a local yadbd node
./target/debug/yadbd --listen 127.0.0.1:8420 --data-dir /tmp/yadb-node1 &

# 3. Apply declarative cluster, pools, project RBAC, SQL DB, S3, and BlockVolume
./target/debug/yadbctl -s http://127.0.0.1:8420 apply -f examples/platform.yaml
./target/debug/yadbctl -s http://127.0.0.1:8420 status

# 4. Run sharded SQL queries (Mirror-3 WAL + Reed-Solomon 4+2 SSTables)
./target/debug/yadbctl -s http://127.0.0.1:8420 sql \
  --project billing --db ledger --subject sa:billing-backend \
  "CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT, balance INT, active BOOL)"

./target/debug/yadbctl -s http://127.0.0.1:8420 sql \
  --project billing --db ledger --subject sa:billing-backend \
  "INSERT INTO accounts VALUES (1, 'alice', 5000, true), (2, 'bob', 1900, true)"

./target/debug/yadbctl -s http://127.0.0.1:8420 sql \
  --project billing --db ledger --subject user:auditor \
  "SELECT id, owner, balance FROM accounts ORDER BY balance DESC"

# 5. Store & range-read S3 objects and thin-provisioned block volumes
./target/debug/yadbctl -s http://127.0.0.1:8420 s3-put \
  --project billing --store receipts --bucket invoices-2026 --key nov/inv1.yaml \
  --subject sa:billing-backend -f examples/platform.yaml

./target/debug/yadbctl -s http://127.0.0.1:8420 block-write \
  --project billing --volume vm-root --subject sa:billing-backend \
  --offset 4096 --data "HELLO-YADB-BLOCK-STORE"
```
