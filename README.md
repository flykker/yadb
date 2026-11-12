# YADB — Yet Another DB & Declarative Storage Platform

Development priorities, reliability work and future features: [ROADMAP.md](ROADMAP.md).

`yadb` is a modular Rust distributed storage and multi-tenant data platform inspired by **YDB's immutable `BlobStorage` + Tablets** and **Kubernetes' declarative YAML control plane**—designed to run directly on standard Linux filesystems (`ext4`/`xfs`) without raw `PDisk` block devices.

## Highlights

- **Immutable Append-Only `VolumeStore` + Distributed `VDisk` Erasure Coding (`yadb-blob`):**
  - Packs fragments into CRC32-verified segment slabs (`vol_XXXXXXXX.dat`) with crash tail recovery, `get_slice` partial reads, generation barrier GC, and segment compaction.
  - Each `StoragePool` chooses **either** `Mirror` (e.g. `3x` replicas) **or** systematic `ReedSolomon { k, m }` over $GF(2^8)$ (e.g. `4+2`).
  - Distributed `PoolSlot` (`Local` disk + `Remote` HTTP/TCP `/internal/vdisk/*` RPC) stripes fragments physically across cluster failure domains & nodes, supports degraded reads & degraded writes when a node dies, and runs an automatic **Self-Healing Scrubber** (`/v1/pools/scrub`, `yadbctl pool-scrub`).
- **Raft Consensus + Ordered `MetaKV` (`yadb-metakv`):**
  - Durable `RaftWal`, automatic election timeouts & instant leader failover, transparent Follower-to-Leader write forwarding on every node, atomic multi-key `Txn` + CAS by `version`, lexicographical prefix scans, and `Watch` streams.
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

### iSCSI block volumes and failover

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
