# YADB — Yet Another DB & Declarative Storage Platform

Development priorities, reliability work and future features: [ROADMAP.md](ROADMAP.md).

`yadb` is a modular Rust distributed storage and multi-tenant data platform inspired by **YDB's immutable `BlobStorage` + Tablets** and **Kubernetes' declarative YAML control plane**—designed to run directly on standard Linux filesystems (`ext4`/`xfs`) without raw `PDisk` block devices.

## Highlights

- **Immutable Append-Only `VolumeStore` + Distributed `VDisk` Erasure Coding (`yadb-blob`):**
  - Packs fragments into CRC32-verified segment slabs (`vol_XXXXXXXX.dat`) with crash tail recovery, `get_slice` partial reads, generation barrier GC, and segment compaction.
  - Each `StoragePool` chooses **either** `Mirror` (e.g. `3x` replicas) **or** systematic `ReedSolomon { k, m }` over $GF(2^8)$ (e.g. `4+2`).
  - Distributed `PoolSlot` (`Local` disk + `Remote` HTTP/TCP `/internal/vdisk/*` RPC) stripes fragments physically across cluster failure domains & nodes, supports degraded reads & degraded writes when a node dies, and runs an automatic **Self-Healing Scrubber** (`/v1/pools/scrub`, `yadbctl pool-scrub`).
- **Raft Consensus + Ordered `MetaKV` (`yadb-metakv`):**
  - Durable `RaftWal`, automatic election timeouts & leader failover, transparent Follower-to-Leader write forwarding on every node, atomic multi-key transactions guarded by per-key version or global modification revision, lexicographical prefix scans, and `Watch` streams.
- **Kubernetes-Style Declarative Control Plane & Zero-Hop Multi-Tenancy (`yadb-control`):**
  - Declarative `Cluster`, `StoragePool`, `Project`, `Database`, `ObjectStore`, and `BlockVolume` manifests reconciled via `yadbctl plan` / `yadbctl apply`.
  - Per-project RBAC (`readOnly`, `readWrite`, `admin`) cached locally in memory via `MetaKV` watches (`~0` network hops on the hot path).
- **Three Pluggable Data Services over One Storage Kernel:**
  1. **`yadb-db` (Distributed SQL & Tablets):** Dual-channel YDB-style `DataShardTablet` (`walPool` + `dataPool`) supporting cross-shard 2PC transactions (`BEGIN / COMMIT / ROLLBACK`), `UPDATE ... SET col = col +/- N`, secondary indexes (`CREATE INDEX`), `GROUP BY` + `COUNT / SUM / MIN / MAX / AVG`, and SSTable compaction (`COMPACT TABLE`) with cross-node VDisk GC barriers.
  2. **`yadb-s3` (Object Storage):** Multi-chunk S3 engine with strong read-after-write consistency, `Range` reads, and sorted prefix listing.
  3. **`yadb-block` (Virtual Disks):** Thin-provisioned Copy-on-Write 4 KiB block volumes.
- **Zero-Dependency Embedded OKD-Style Web Console (`yadbd`):**
  - Compiled directly into the `yadbd` binary (`GET /` or `/ui`, ~41 KB, zero external JS/CSS/CDN files).
  - Includes Cluster Overview, Project/Namespace switcher, RBAC Subject impersonation bar, `+ Import YAML` (`Plan` dry-run & `Apply`), SQL Query Workbench, S3 Bucket Browser, and 4 KiB BlockVolume Hex/ASCII Inspector.

## Standard client protocols

The optional protocol listeners are disabled unless explicitly configured. They share the
same control-plane RBAC and distributed data engines as the HTTP gateway.

Create a credentials file readable only by the daemon (`chmod 600`):

```json
{
  "users": {
    "backend": {
      "subject": "sa:billing-backend",
      "secret": "replace-with-a-random-secret-at-least-12-chars"
    }
  }
}
```

Start one daemon with the optional listeners (repeat the listener flags on every cluster node):

```bash
./target/debug/yadbd \
  --node-id 1 --listen 10.0.0.11:8420 \
  --peers '1=10.0.0.11:8420,2=10.0.0.12:8420,3=10.0.0.13:8420' \
  --data-dir /var/lib/yadb/node-1 \
  --protocol-credentials /etc/yadb/credentials.json \
  --pg-listen 10.0.0.11:5433 \
  --iscsi-listen 10.0.0.11:3260 \
  --iscsi-portals 10.0.0.11:3260,10.0.0.12:3260,10.0.0.13:3260 \
  --s3-listen 10.0.0.11:9000 --s3-target billing/receipts
```

### PostgreSQL wire protocol

The initial implementation supports PostgreSQL v3 **Simple Query** mode, including
multi-statement SQL and transaction sessions. It intentionally does not claim extended
queries, prepared statements, COPY, or TLS termination yet. Put TLS/mTLS in front of the
listener (or use a private network): the password exchange is cleartext inside the TCP
connection.

The database name is `project/database`, and the PostgreSQL user is the credentials-file
access-key:

```bash
PGPASSWORD='replace-with-a-random-secret-at-least-12-chars' \
  psql 'host=10.0.0.11 port=5433 user=backend dbname=billing/ledger sslmode=disable' \
  -c 'SELECT id, owner, balance FROM accounts ORDER BY id;'
```

For production use, terminate TLS with a sidecar/reverse proxy and restrict the listener
with firewall rules. A transaction whose gateway node dies must be restarted on another
portal; committed rows remain available through Raft and the replicated BlobStorage.

### AWS S3-compatible API

The S3 listener is path-style and binds one declarative `Project/ObjectStore` per listener.
It supports AWS Signature Version 4, `PUT`, `GET`, `HEAD`, `DELETE`, XML `ListObjectsV2`,
conditional reads, byte ranges, continuation tokens, and CRC32/SHA256/Content-MD5 validation. Multipart
upload, server-side copy, ACLs, custom metadata, and streaming chunk signatures are
explicitly rejected until implemented.

```bash
export AWS_ACCESS_KEY_ID=backend
export AWS_SECRET_ACCESS_KEY='replace-with-a-random-secret-at-least-12-chars'
export AWS_DEFAULT_REGION=us-east-1

# Current API buffers uploads, with a 16 MiB request limit and no multipart API.
# Use s3api put-object for a file <= 16 MiB (high-level `s3 cp` can choose multipart).
aws --endpoint-url http://10.0.0.11:9000 s3api put-object \
  --bucket invoices-2026 --key sample.txt --body sample.txt
aws --endpoint-url http://10.0.0.11:9000 s3 ls s3://invoices-2026/ --recursive
aws --endpoint-url http://10.0.0.11:9000 s3api head-object \
  --bucket invoices-2026 --key image.raw
```

Use a separate listener (`--s3-target project/store`) for each tenant ObjectStore, or put
an authenticated proxy in front if host-style virtual buckets are required. Configure
AWS clients for path-style addressing. GET/Range responses are also limited to 16 MiB;
presigned URLs, session tokens, native TLS, and a complete AWS S3 implementation are not
provided yet. Credentials and object bytes require HTTPS termination or a trusted network.

#### S3 failed-upload cleanup and retention

New uploads reserve a durable per-project sequence and persist the complete chunk list
in `/s3-intent/` **before** writing any fragments. New S3 blobs use channel 4, separate
from legacy S3/SQL data and block extents. The allocator must never be deleted/reset;
restore metadata and fragment data together, not from unrelated points in time.

Publishing a manifest, retiring its previous version and removing the upload intent
is one revision-guarded Raft transaction. Revision guards prevent a deleted/recreated
key from accidentally satisfying an old per-key-version check (ABA). If publication
fails or its outcome is unknown, the durable intent is not an immediate rollback.

The pool scrubber reclaims failed uploads only after **both** a five-minute wall-clock
grace and 32 metadata revisions. On an idle project/cluster, cleanup may wait for more
metadata writes. A consensus-confirmed transition to `Aborted` fences publication;
then exact, persistent tombstones fence every physical part, including absent parts,
against delayed PUT/repair. Every target must acknowledge its tombstone, not merely a
write quorum. Failed cleanup keeps the aborted intent for a later retry/restart.
Clock changes can affect cleanup timing, but are not the publication safety fence.
In clustered mode the leader runs scrub periodically; `/v1/pools/scrub` also requests it
explicitly. `orphan_blobs_collected` counts logical blobs, not physical fragments;
`orphan_collection_error` exposes cleanup failures (partial progress is not a total).

**Retention is deliberately conservative:** overwritten/deleted manifests and their
fragments remain stored because an in-flight reader on another node can hold an old
immutable manifest. Cross-node reader retention/fencing is not implemented. Historical
fragments without new-format intents, including legacy intents with collision-prone
IDs, are never deleted just because a scan finds no current manifest. Segment compaction
can reclaim tombstoned payload bytes in sealed segments, but retains fencing markers;
tombstones/retired metadata are not yet bounded by a retention policy.

Deterministic tests cover publication-versus-abort races, failed/torn fragment and
metadata writes, interrupted cleanup/restart, concurrent ID allocation, immutable PUT
conflicts, PUT-versus-GC fencing, compaction-versus-read, and payload bit-rot before later
durable records. These simulations do not prove real ENOSPC, failed `fdatasync`, hardware
power-loss behavior or all failure schedules. Compaction currently serializes writers
per volume for correctness; latency under large compactions is not characterized.

#### Sync errors and recovery

A failed write or sync is not evidence of rollback. After a WAL write/sync error,
the node rejects further Raft proposals, RPCs and elections and emits no new
replication broadcasts until restart/recovery. This prevents a retry from acknowledging
an in-memory follower entry whose WAL persistence was never confirmed. A volume with
an active-record sync error, failed tail rollback or failed segment activation rejects
further mutations and compaction until reopened. Reads can still use its last
published in-memory index; this is not a read-after-failed-write/status guarantee.

Recovery syncs existing volume segments and the WAL (in durable mode), and syncs their
directory entries before exposing recovered state. A process restart alone does not
clear page cache: a valid recovered record must not receive an idempotent success
response until its durability is established. If recovery sync fails, opening fails.
An operation that returned an error may appear after recovery, including a deletion
or S3 publication; clients still need unknown-outcome handling.

`SimFs` tests inject `sync_data` and directory-sync EIO both before persistence and
after full persistence, including WAL retries, process-only restart followed by power
loss, S3 publication/cleanup, segment rollover and compaction. These are two modeled
endpoints, not arbitrary partial/reordered device writeback, real ENOSPC or hardware
power-loss validation. Temporary compaction sync errors do not poison the active writer:
the original file remains in use before rename, and after rename the replacement
file/offsets are installed together even if directory sync fails.

The new `RevisionTxn` Raft/WAL command and volume tombstone records are not understood
by older binaries. Upgrade all nodes together while stopped; mixed-version rolling
upgrades/downgrades are unsupported.

### iSCSI block volumes and failover

New block writes reserve a durable, project-wide 56-bit allocation in
`/block-write-sequence/{project_id}` before any fragment PUT. Channel 5 separates
these IDs from legacy channel-3 extents, SQL and S3; owner/generation encode the
allocation and step identifies each touched block. Volume names/hashes, process
restarts and deletion/recreation of extent maps cannot reuse the new IDs. Allocation
exhaustion fails rather than wrapping; failed writes may consume allocations.
Never delete/reset the sequence, and restore metadata and fragments together.
Legacy extent maps remain readable and are migrated block-by-block on write, not
rewritten in place. `generation` in a new extent map is allocation metadata, not a
volume ownership or GC fence. Stopped, coordinated upgrades are required; mixed
old/new block writers and downgrades are unsupported.

Block RMW and UNMAP publish maps with modification-revision guards from the same
metadata snapshot, rejecting stale publication after delete/recreate (ABA). Tests
cover a concrete CRC32 volume-name collision, independent concurrent allocators,
allocation rollover/exhaustion, legacy extents, fragment failures and ambiguous
reservation/publication sync errors. Old and unpublished block fragments are retained;
safe reclamation and cross-node reader retention are still open. This does not
establish SQL transaction isolation or repair historical aliasing caused by old IDs.

The iSCSI target implements CHAP, discovery, stable SCSI VPD page `0x83` identifiers,
READ/WRITE, cache flush, capacity, and 4 KiB-aligned UNMAP. It is an **ERL=0** target;
the initiator owns reconnect and path failover. For KVM/libvirt, expose all YADB portals
to Linux `open-iscsi` and use `dm-multipath` so all sessions with the same VPD83 WWID are
one device:

```bash
# Configure these in /etc/iscsi/iscsid.conf BEFORE discovery (protect the file):
# discovery.sendtargets.auth.authmethod = CHAP
# discovery.sendtargets.auth.username = backend
# discovery.sendtargets.auth.password = <same secret as the credentials file>
# node.session.auth.authmethod = CHAP
# node.session.auth.username = backend
# node.session.auth.password = <same secret as the credentials file>

systemctl enable --now iscsid multipathd
iscsiadm -m discovery -t sendtargets -p 10.0.0.11:3260
iscsiadm -m node --login
multipath -ll
```

Example device stanza for `/etc/multipath.conf` (adapt timeouts to the host and test
under network failure, not just process termination):

```text
devices {
  device {
    vendor "YADB"
    product "Distributed Disk"
    path_grouping_policy multibus
    path_selector "round-robin 0"
    path_checker tur
    prio const
    no_path_retry 12
  }
}
```

Use the actual `/dev/mapper/<WWID>` reported by `multipath -ll`, or explicitly assign
that WWID the alias `yadb-billing-vm-root` in a `multipaths` stanza. Verify that all three
portal sessions are present; discovery alone does not establish usable alternate paths.

The discovery response advertises the same target IQN and all configured portals, while
each portal performs the same CHAP/RBAC check and serves the same logical `BlockVolume`.
With a correctly configured initiator and healthy storage quorum, multipath can retry
I/O over another live session when one `yadbd` node dies. Detection and retry pauses
depend on the initiator timeouts; zero errors and millisecond failover are not guaranteed.
Losing all paths or the storage quorum eventually causes I/O failure according to that
policy. A single QEMU
`iscsi://node1/...` URL is **not** equivalent to multipath and can still lose its TCP
session when that target node fails.

For libvirt, use the discovered multipath device as a raw disk, with cache disabled:

```xml
<disk type="block" device="disk">
  <driver name="qemu" type="raw" cache="none" io="native"/>
  <source dev="/dev/mapper/yadb-billing-vm-root"/>
  <target dev="vdb" bus="virtio"/>
</disk>
```

The protocol frontends are covered by `python3 tests/protocols.py`, which uses independent
stdlib clients to test SigV4, PostgreSQL framing/session behavior, iSCSI CHAP/SCSI I/O,
UNMAP, negotiated burst limits, and continued operation after killing a Raft leader.

A write timeout, connection loss, or `write outcome is unknown` error is **not a
rollback guarantee**. A Raft proposal can commit after its replication reply is
lost, including after a leader change. Reconcile application state before retrying
non-idempotent operations; there is no general request-ID deduplication/status API
yet. Deterministic MetaKV tests cover lost replies, entry replacement across terms,
and lost commit notifications, but not all end-to-end protocol failure boundaries.

Native-client checks were run on aarch64 Alpine with **psql 17.11**, **AWS CLI 2.22.10**,
and **libiscsi 1.20.3**. Verified cases include SQL queries/commit and bad passwords;
S3 PUT (default Content-MD5 and explicit CRC32), HEAD/GET/LIST, Unicode/escaped keys,
Range and bad credentials; iSCSI CHAP, INQUIRY, READ CAPACITY(16), and a 512 KiB
WRITE(16) using R2T/Data-Out followed by byte-for-byte READ(16) verification.
This is not a claim of complete PostgreSQL/S3/SCSI conformance.

To repeat the optional native checks, install psql and AWS CLI, and install libiscsi
including its development headers and utilities. Build the probe **outside the source
tree** (it overwrites a region of the disposable test volume):

```bash
cc tests/libiscsi_probe.c $(pkg-config --cflags --libs libiscsi) -o /path/to/libiscsi-probe
python3 tests/protocols.py --native-clients \
  --libiscsi-dir /path/to/libiscsi/bin --libiscsi-probe /path/to/libiscsi-probe
```

Actual `open-iscsi` kernel sessions, `dm-multipath`, a VM filesystem workload and failure
of an active target path remain **unverified**. The Android-hosted test environment has
no `/dev/kvm` or `/dev/mapper/control`; installing userspace tools does not remove this
limitation. Run those tests on a dedicated Linux/KVM host before production use.
The current target has no SCSI persistent reservations, ALUA, mutual CHAP, or encrypted
transport. Multipath paths to one VM are supported by the design; sharing the same raw
filesystem read-write across multiple VMs is not safe without additional coordination.
UNMAP drops logical extent references; it does not yet reclaim the old fragments on disk.
Keep the legacy HTTP/control-plane ports private: their existing subject headers are a
trusted internal interface, not a replacement for protocol authentication.

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
