# YADB — Yet Another DB & Declarative Storage Platform

Development priorities, reliability work and future features: [ROADMAP.md](ROADMAP.md).

OpenRaft client routing for a **new empty nonproduction cluster** is under
implementation: [scope and safeguards](docs/openraft-empty-test-cluster.md).
Legacy remains the default. Multi-group facade/engine tests pass for two projects
on separate data groups. A child-daemon integration test also passed with singleton
groups, and a targeted three-process test covered multi-node HTTP leader routing.
Broader daemon validation remains open; no cluster was deployed.
This work does not authorize reusing existing data or imply production readiness.
See the [production checklist](docs/multiraft-production-todo.md).

The local-cluster generator opts into per-group leader preferences
(control 1 → node 1, data 2 → node 2, data 3 → node 3) and relaxed Raft
timings. These settings bias elections, not placement guarantees or workload
balancing; failover remains enabled. A healthy nonpreferred leader is not
automatically displaced when the preferred node returns. The
`/openraft/status` route reports each group's observed `leader_id`,
`preferred_leader`, and `preference_met` (null when either is unknown). The
ordinary health leader refers only to the control group. All replicas of a
group must use the same preference and timing values; the election maximum is
also the OpenRaft leader-lease duration. Defaults remain unchanged when the
opt-in fields are absent. These local timing values are not a diagnosed fix
for replication issues and require evaluation for each deployment.

For example, for group 2 on **all** its voters, add to the version-1 group
config (choose preferred node 1 for group 1 and 3 for group 3):

```json
"preferred_leader": 2,
"raft_timing": {
  "heartbeat_ms": 250,
  "election_min_ms": 1500,
  "nonpreferred_election_min_ms": 2750,
  "election_max_ms": 3000
}
```

The two election ranges overlap by design because the maximum must remain
common; a nonpreferred voter can legitimately win. Both fields are opt-in
together, and `preferred_leader` must identify a voter.

The status endpoint `GET /openraft/status` is not an unauthenticated admin
interface: when the daemon is started with `--subject-secret` it requires a
verified signed subject and answers `401` otherwise. Without a configured
secret no subject can be verified, so the route stays open like the other
legacy fallbacks. Configure the preference on every replica of the group. A
returning preferred node does not automatically reclaim a healthy leader, and
failover availability remains enabled.

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
   1. **`yadb-db` (Distributed SQL & Tablets):** Dual-channel YDB-style `DataShardTablet` (`walPool` + `dataPool`) with immutable WAL preparation and atomic cross-shard metadata publication (`BEGIN / COMMIT / ROLLBACK`), `UPDATE ... SET col = col +/- N`, secondary indexes (`CREATE INDEX`), `GROUP BY` + `COUNT / SUM / MIN / MAX / AVG`, and logical SSTable compaction (`COMPACT TABLE`). Old fragments are retained for readers; full transaction isolation is not established.
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

The listener implements PostgreSQL v3 **Simple Query** and the **extended query**
protocol (`Parse`, `Bind`, `Describe`, `Execute`, `Close`, `Flush`, `Sync`), including
multi-statement Simple Query scripts, named and unnamed prepared statements and portals,
and transaction sessions. Only **text format** is supported: a `Bind` that requests a
binary parameter or result format is refused with `0A000`.

Parameters are not server-typed. The SQL engine has no `$n` syntax, so `Bind` decodes
each text value using the OID sent by `Parse` and substitutes a SQL literal into the
statement text. Strings and unknown OIDs are quoted with embedded single quotes
doubled; integers, floats and booleans must be valid literals or the statement fails
with `22P02`; a NULL value becomes the `NULL` keyword. When the client sends OID 0
(unspecified), the value is inferred as integer, then float, then boolean, then text.
Substitution is quote/comment aware, so a value cannot become SQL syntax. This literal
binding is a real limitation: it is not PostgreSQL's server-side typing, parameter
inference and plan reuse, and float parameters reach a parser that has no float column
type. Prepared statements and portals are bounded (64 of each, 64 KiB of SQL); exceeding
a bound is a clean error. `Parse` records the statement text and syntax errors surface
at `Execute`.

`Describe` obtains column names only by executing a read-only statement (`SELECT` or
`SHOW`) through the gateway with `NULL` parameters and discarding the rows; a mutating
statement answers `NoData` without being executed. Execute row limits
(`PortalSuspended`) are refused.

`COPY` is implemented in the **text** format only:

- `COPY <table> [(<columns>)] FROM STDIN` reads the client stream, types each field by
  the table's declared column types (obtained with `SHOW COLUMNS FROM <table>`), and
  loads the rows as batched `INSERT`s through the normal transaction path. The text
  escapes (`\t`, `\n`, `\\`, `\b`, `\f`, `\r`, `\v`, octal, `\xHH`) and `\N` for NULL
  are honoured. Outside an explicit `BEGIN`, the whole `COPY` is one transaction, so a
  malformed row, a duplicate key or a failed batch rolls the entire load back; inside
  one, the client's transaction provides that. `COPY ... FROM STDIN` must be the only
  statement in a query, because its data arrives after the response.
- `COPY <table> [(<columns>)] TO STDOUT` and `COPY (<select>) TO STDOUT` stream rows in
  the same text format.
- Server-side files (`COPY ... FROM '/path'`) and any option other than `FORMAT text`
  are refused with `0A000` rather than silently ignored. CSV and binary formats are not
  implemented, and `PortalSuspended` remains unsupported.

`SHOW COLUMNS FROM <table>` (also `SHOW COLUMNS IN <table>`) answers `column`, `type`
and `primary_key` in declaration order. It exists because `COPY` needs the declared
types: inferring them from the values would silently turn a text column that looks
numeric into an integer.

TLS is opt-in per listener. `--pg-tls-cert` and `--pg-tls-key` (PEM or DER) make the
listener answer `SSLRequest` with `S` and complete a TLS handshake; `--pg-tls-client-ca`
additionally requires a client certificate signed by that CA (mTLS), and
`--pg-require-tls` refuses a cleartext startup packet with `28000`. Without these flags
the behaviour is unchanged: `SSLRequest` answers `N`, and the password exchange is
cleartext inside the TCP connection, so keep the listener on a trusted network or put
TLS/mTLS in front of it. Binary formats remain unsupported.

The database name is `project/database`, and the PostgreSQL user is the credentials-file
access-key:

```bash
PGPASSWORD='replace-with-a-random-secret-at-least-12-chars' \
  psql 'host=10.0.0.11 port=5433 user=backend dbname=billing/ledger sslmode=disable' \
  -c 'SELECT id, owner, balance FROM accounts ORDER BY id;'
```

With TLS configured, the same listener accepts `sslmode=verify-full` when the client
trusts the configured certificate:

```bash
PGPASSWORD='...' psql \
  'host=db.internal port=5433 user=backend dbname=billing/ledger sslmode=verify-full sslrootcert=ca.pem' \
  -c 'COPY accounts FROM STDIN'
```

For production use, terminate TLS with a sidecar/reverse proxy and restrict the listener
with firewall rules. A transaction whose gateway node dies must be restarted on another
portal; committed rows remain available through Raft and the replicated BlobStorage.

#### SQL allocation, publication and compaction

New SQL WAL/SST blobs use channels 6/7 and a durable project-wide 56-bit allocation
in `/sql-blob-sequence/{project_id}`, reserved before fragment PUT. Physical IDs no
longer depend on the CRC32 tablet owner, legacy generation/step, or whether a prior
preparation was published. Do not delete/reset the sequence; metadata and fragments must come
from the same point in time. Legacy channel-1/2 references remain readable. Allocation,
legacy generation and row-sequence exhaustion fail instead of wrapping/saturating.
Upgrades require all nodes stopped and coordinated; mixed old/new SQL writers and
downgrades are unsupported. Existing legacy ID aliasing is not repaired automatically.

Takeover, WAL publication, flush, compaction and cross-shard publication use
modification-revision guards; shard caches compare these revisions rather than
per-key versions that can repeat after delete/recreate. Schema guards use revisions
as well. A failed preparation can leave fragments; a failed publication can still
commit. Neither is an automatic rollback or authorization for cleanup.

`COMPACT TABLE` merges live rows into a new SST but no longer issues generation GC
barriers or deletes old WAL/SST fragments. Old snapshots may still need those bytes,
including on another node. Its `evicted_fragments` result is therefore zero. Tests
cover old snapshots, legacy recovery, colliding owners, unpublished preparations,
stale owners/ABA, concurrent allocation and modeled sync errors. Retention and disk
growth remain unbounded until cross-node reader fencing and safe GC are implemented.

Before WAL preparation, commits revalidate their buffered original row values
under the same tablet mutex/view used for preparation. Manifest revision guards
then reject changes after preparation. No-op writes also validate rows and guard
their shard view, including when another shard has actual mutations. Deterministic
independent-engine tests cover update/insert/delete conflicts at both boundaries,
exact secondary-index contents, rejection of mixed-shard commits and recovery
after a simulated power cut. An unrelated row change before preparation can be
preserved alongside the committing update.

UPDATE and DELETE buffer the original row obtained during their collection,
rather than rereading it after collection. A concurrent change before buffering
therefore conflicts at commit. A primary-key move also preserves the observed
absence of its target so a later concurrent insert cannot become an overwrite.
These remain value comparisons, not protection against equal-value ABA.

`CREATE INDEX` backfill records the modification revision of each exact scanned
tablet view, including revision-zero guards for absent shards. Index entries and
schema are published in one transaction guarded by those revisions and the schema
revision. A change to an already scanned shard rejects the whole publication; retry
the statement after a confirmed conflict. A change to a not-yet-scanned shard can
be included in the later scan. Takeover, flush and compaction also change revisions
and can conservatively cause conflicts. This is not a nonblocking online-index
builder: sustained writes can prevent progress, and the full backfill is buffered
and published in one metadata transaction. Errors with an unknown commit outcome
still require checking recovered schema; they do not prove rollback.

Deterministic tests cover concurrent update/delete/insert, absent/empty shards,
manifest/schema ABA, DML prepared before index publication, exact index contents,
retry and recovery. Modeled publication sync errors recover as an absent index or
an atomically published schema and backfill, not a partial index.

#### SQL read consistency and isolation limits

Row collection copies selected metadata under the local Raft-apply mutex: schema,
shard manifests and candidate index hits come from one applied state. Primary-key
lookups copy only their shard reference; other paths copy the table's shard
references and, if used, the matching index-value range, not every project's data.
After releasing the mutex, read-only tablets load WAL/SSTs from those exact copied
manifests, never from newer metadata or a mutable live shard cache. Old fragments
remain retained, so later updates/compaction cannot change this query's view.
An absent shard stays absent for this collection even if a writer creates it.

This replaces global-revision conflict checks: unrelated writes, later schema/index
changes and shard maintenance no longer abort an already captured collection.
The snapshot schema must match the schema used to resolve the statement before
capture; otherwise the statement reports a schema conflict. Inside a transaction
an error still aborts the transaction, so retry the transaction as a whole. This
covers SELECT's buffered rows (including aggregation inputs) and collection used
by UPDATE/DELETE; it is not a snapshot of every later DML validation step. No read-
side metadata writes or automatic retries are performed.

Snapshots are owned metadata copies, not persistent reader leases or safe-GC
authorization. They add copying under the apply mutex and replay WALs in query-local
tablets; large tables/index hit sets can increase memory, CPU and apply latency.
This is not a quorum read barrier: a follower/isolated node may return stale committed
data. Linearizable reads and cross-portal read-your-writes are not promised.

An explicit opt-in is available for a single quorum-confirmed view. Send
`"consistent": true` in the query body:

```bash
curl -sS -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM accounts","consistent":true}' \
  http://10.0.0.11:8080/db/billing/db/query
```

The script must contain only `SELECT`/`SHOW` statements; any other statement, a
`session_id`, or `require_transaction` is rejected with `400` before a snapshot
is taken, so a mutation can never reach this path. The engine takes exactly one
`try_linearizable_snapshot` of the whole `/db/{project}/{db}/` prefix and serves
the schema, shard manifests and secondary-index entries from that snapshot only:
it does not consult the in-memory shard cache, the local applied state, or a
second snapshot, and it does not retry. A table or shard missing from that
snapshot is the same "not found"/empty result as on the default path. The cost
is one quorum round trip plus copying the whole database namespace; omitting the
flag (or sending `false`) keeps the default behaviour above unchanged. With
OpenRaft metadata the request is forwarded to the project-group leader; with
legacy metadata it fails with the documented routing error (`500`) instead of
returning local data.

Deterministic independent-engine tests preserve the complete pre-commit state
through cross-shard writes and index-hit swaps. They cover absent shards, schema
changes, unrelated writes, compaction, dropped indexes and own-write overlays.
Within BEGIN, separate statements may see newer committed values and
new rows; the transaction preserves its own buffered writes as an overlay, not a
snapshot of committed data. Nonrepeatable reads and phantoms between statements
are explicitly demonstrated, not prevented.

These checks do **not** establish serializable or snapshot-isolated SQL transactions.
DML original-row checks compare values, so intervening changes that restore equal
values are not detected as transaction-level row ABA. There is no full transaction
read-set/range validation or general serializability guarantee. The guarded index
backfill and per-collection snapshots do not change those limits.

### AWS S3-compatible API

The S3 listener is path-style and binds one declarative `Project/ObjectStore` per listener.
It supports AWS Signature Version 4, `PUT`, `GET`, `HEAD`, `DELETE`, XML `ListObjectsV2`,
multipart upload, conditional reads, byte ranges, continuation tokens, and
CRC32/SHA256/Content-MD5 validation. Server-side copy, ACLs, custom metadata, and
streaming chunk signatures are explicitly rejected until implemented.

```bash
export AWS_ACCESS_KEY_ID=backend
export AWS_SECRET_ACCESS_KEY='replace-with-a-random-secret-at-least-12-chars'
export AWS_DEFAULT_REGION=us-east-1

# A single request body is limited to 16 MiB; larger objects use multipart.
aws --endpoint-url http://10.0.0.11:9000 s3api put-object \
  --bucket invoices-2026 --key sample.txt --body sample.txt
aws --endpoint-url http://10.0.0.11:9000 s3 ls s3://invoices-2026/ --recursive
aws --endpoint-url http://10.0.0.11:9000 s3api head-object \
  --bucket invoices-2026 --key image.raw
# High-level copy picks multipart automatically above the configured threshold.
aws --endpoint-url http://10.0.0.11:9000 s3 cp image.raw s3://invoices-2026/image.raw
```

Use a separate listener (`--s3-target project/store`) for each tenant ObjectStore, or put
an authenticated proxy in front if host-style virtual buckets are required. Configure
AWS clients for path-style addressing. GET/Range responses are also limited to 16 MiB;
presigned URLs, session tokens, native TLS, and a complete AWS S3 implementation are not
provided yet. Credentials and object bytes require HTTPS termination or a trusted network.

#### Multipart upload

`POST ?uploads`, `PUT ?partNumber=N&uploadId=...`, `POST ?uploadId=...`,
`GET ?uploadId=...` (ListParts) and `DELETE ?uploadId=...` (Abort) are implemented on
the native listener. Each part is limited to **16 MiB** (the same bound as a single
PUT); a larger part is refused with `413 EntityTooLarge` and the upload stays usable.
Part numbers are `1..=10000`. `CompleteMultipartUpload` must list parts in strictly
ascending order with the ETags the upload returned; a wrong ETag or size is
`400 InvalidPart`, a repeated or descending part number is `400 InvalidPartOrder`, and
an unreadable document is `400 MalformedXML`.

Each part is its own durable allocation: its intent is committed in `/s3-intent/`
before any fragment is written, and the upload record names the parts. Completion is a
single revision-guarded transaction that publishes the manifest, retires the previous
version, deletes the upload record and deletes every part intent. The published ETag is
the S3 multipart form `"<md5 of the concatenated part digests>-<part count>"`. If that
transaction loses a guard the response is `503` with `x-yadb-retry: do-not-retry`:
the outcome is ambiguous, so a client must not replay the completion automatically.
Aborting deletes the record and marks every live part intent `Aborted`; the scrubber
then reclaims the fragments after the usual grace. An upload that is never completed or
aborted is collected the same way after the five-minute/32-revision grace, so it cannot
leak.

`GET` and range `GET` accept `?consistent=true`. The object manifest is then read
through exactly one quorum-confirmed snapshot on the project-group leader and the
chunks it names are fetched from the pool; the local applied manifest is never
used as a fallback. A key absent from that snapshot is `404`. A chunk the pinned
manifest names but that has since been collected is reported as `503` (the
manifest is a point-in-time view, not a GC fence), never as zeros or a short
body. The default (no flag) is unchanged: the locally applied manifest. With
legacy metadata the request fails with the documented routing error (`500`)
instead of returning local data.

#### S3 failed-upload cleanup and retention

New uploads reserve a durable per-project sequence and persist the complete chunk list
in `/s3-intent/` **before** writing any fragments. New S3 blobs use channel 4, separate
from legacy S3/SQL data and block extents. The allocator must never be deleted/reset, and
metadata and fragment data must always come from the same point in time: rebuilding
one without the other silently reuses or loses IDs.

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

A multipart upload adds two kinds of `/s3-intent/` record: the upload record (which
names its parts and no fragments) and one ordinary part intent per part. The upload
record refreshes its lease whenever a part is registered, and while it is live it pins
its parts past their own lease, so a slow upload stays completable. An upload that
expires uncompleted is deleted by the same collector; its parts then become unreferenced
intents and are reclaimed normally, so an abandoned multipart upload cannot leak
fragments.
Clock changes can affect cleanup timing, but are not the publication safety fence.
In clustered mode the leader runs scrub periodically; `/v1/pools/scrub` also requests it
explicitly. `orphan_blobs_collected` counts logical blobs, not physical fragments;
`orphan_collection_error` exposes cleanup failures (partial progress is not a total).
These scrub and orphan-GC operations are disabled in Multi-Raft mode;
do not rely on automatic repair or reclamation there.

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
Never delete/reset the sequence, and keep metadata and fragments from the same
point in time.
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
Range and bad credentials; a 40 MiB `aws s3 cp` multipart upload in 8 MiB parts with a
byte-identical download, the multipart ETag, HEAD and a Range read; iSCSI CHAP, INQUIRY,
READ CAPACITY(16), and a 512 KiB WRITE(16) using R2T/Data-Out followed by byte-for-byte
READ(16) verification.
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
`/v1/apply` additionally requires a signed, allowlisted admin subject: start the
daemon with `--subject-secret` and `--admin-subject <subject>`, and pass the same
secret to `yadbctl` via `--subject-secret` (or `YADB_SUBJECT_SECRET`). Unsigned,
forged, or non-allowlisted callers are refused.
A keyed `/v1/apply` is idempotent: send `Idempotency-Key: <1-128 of [A-Za-z0-9._~-]>`
and the daemon commits the response record in the same Raft transaction as the
mutation, so a retry with the same key and payload replays the recorded response
instead of re-applying, a different payload under the same key is refused with 409,
and `POST /v1/idempotency/resolve` with `{"scope":"control","key":"<key>"}` answers
`committed`, `not_committed`, or `unknown`. Keys are not supported on SQL, S3 or
block writes (501), and the record set has no quota yet.

## Local dev cluster

`scripts/yadb-openraft-dev.py` starts a persistent three-node OpenRaft Multi-Raft
cluster on loopback — one control group plus two data groups, three replicas each,
with mandatory mutual TLS between replicas — and enables the PostgreSQL, S3 and
iSCSI listeners so real clients can be used against it. It is the only cluster
launcher, because the daemon has no other metadata runtime. It never wipes a data
directory, never changes daemon defaults, never retries an ambiguous mutation and
never switches an existing root.

Build the daemon once (`cargo build -p yadbd --offline --locked -j 2`; the
`openraft-storage` feature is a no-op, so it is neither needed nor harmful), then:

```bash
# optional: provision a fresh root without starting it
python3 scripts/yadb-openraft-dev.py prepare

# First start also applies examples/platform.yaml (signed) and prints every endpoint,
# the credentials path and the generated password.
python3 scripts/yadb-openraft-dev.py start
python3 scripts/yadb-openraft-dev.py status   # compares state.json with live PIDs and ports
python3 scripts/yadb-openraft-dev.py logs --lines 50

# stop and start again: same data dirs; manifests are not re-applied
python3 scripts/yadb-openraft-dev.py stop
python3 scripts/yadb-openraft-dev.py start

# A control-plane change must be signed, so use apply rather than an unsigned curl.
python3 scripts/yadb-openraft-dev.py apply examples/platform.yaml
```

Ports derive from one `--port-base` (default `23500`): HTTP `base+0..2`, PostgreSQL
`base+200..202`, S3 `base+300..302`, iSCSI `base+400..402`. The base is recorded in
`state.json` on first start and must be reused for that root; use a new `--root` for a
second cluster. The default root is
`/data/data/com.foxdebug.acodefree/cache/opencode/yadb-openraft-dev`. Start refuses if
any derived port is bound, if state says running and the PIDs are alive, or if a
recorded process is missing; the last case needs an explicit `--force`. It never
wipes data, never re-initialises an existing directory and never retries an `apply`
whose outcome is unknown. Credentials live in `<root>/credentials.json` (mode 0600,
user `backend`, database `billing/ledger`); `--admin-subject` and `--subject-secret`
are mandatory, so `/v1/apply` has to be signed and the launcher's `apply` subcommand
is the supported way to do it.

```bash
# psql, including the extended protocol (\bind)
PGPASSWORD=<printed password> PGSSLMODE=disable \
  psql -h 127.0.0.1 -p 23700 -U backend -d billing/ledger -c 'SELECT 1;'
printf 'SELECT id FROM t WHERE id = $1 \\bind 1 \\g\n' | \
  PGPASSWORD=<printed password> PGSSLMODE=disable \
  psql -h 127.0.0.1 -p 23700 -U backend -d billing/ledger

# AWS CLI (path-style addressing is required)
AWS_ACCESS_KEY_ID=backend AWS_SECRET_ACCESS_KEY=<printed password> \
AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true \
  aws --endpoint-url http://127.0.0.1:23800 s3 cp ./file s3://invoices-2026/dev/file
```

The iSCSI target is `iqn.2026-11.io.yadb:billing:vm-root` on `base+400..402` with
CHAP user `backend`; discovery advertises all three portals. These listeners are
cleartext on loopback: keep them local and treat the whole root as secret.

A restarted node may bind its listeners before it has applied a control image.
Until `generation` is nonzero it answers a retryable "not ready" instead of
denying the request: native S3 returns `503 ServiceUnavailable`, the gateway
returns a JSON `503`, PostgreSQL-wire returns `57P03`, and an iSCSI login gets a
target-error status. An iSCSI login during a control-quorum outage gets the same
target-error status rather than an authentication failure. `/v1/apply` and
`/v1/status` are not gated, so bootstrap and diagnosis still work. A cluster that has never had a manifest applied therefore
answers `503` on client paths rather than `403`; once the image is installed the
historical `403`/`404` answers are unchanged.

Reclaiming disk: overwritten blocks, folded WAL blobs, replaced SSTs and
overwritten S3 objects leave their old fragments behind. `POST /v1/pools/gc`
runs one collection pass and returns `blobs_live`, `fragments_deleted`,
`bytes_reclaimed` and either `gc_refusal: null` or an explicit reason with a
`409`; the leader also sweeps on its own every five minutes. A pass refuses
while any node is reading, so wait about 30 s after the last query:

```bash
curl -s -X POST http://127.0.0.1:23500/v1/pools/gc
```

While a pass runs, reads and writes are refused with `503` instead of being
admitted unfenced, and a read whose lease expired mid-flight is refused rather
than answered from fragments a pass may already have reclaimed. Because only the
metadata leader can publish that lease, a read sent to a follower is proxied to
the leader — the same path writes already take, and the reason a follower read
reports `503 node is not Raft leader` when the leader is unreachable. This
covers the HTTP, native S3 and block read paths (the native listener re-verifies
SigV4 first and keeps its XML error format). A native read keeps its local
contract across the hop because the leader decides it from the manifest it
reads: `If-Match`/`If-None-Match` still answer `412`/`304`, an
unsatisfiable range still answers `416 InvalidRange` with `Content-Range: bytes
*/size`, the 16 MiB rules still answer `400 InvalidRequest`, and
`Last-Modified`/`Accept-Ranges` are preserved. The gateway `/s3/...` GET accepts
the same conditional headers. A proxied read is bounded by the
forwarding wire cap (16 MiB of body); larger block or object reads must be sent
to the leader directly. For that reason the gateway `GET /s3/...` path applies
the same 16 MiB rule as the native listener (a larger full read or slice is
`400 InvalidRequest`); before this it had no limit, so this is a deliberate
change, not an accident. Native `HEAD` on a legacy follower is the one read that
stays local: it fetches no fragments, so it takes no lease, and its `ETag`,
size and `Last-Modified` can lag the leader. The forwarded `Range`,
`If-Match` and `If-None-Match` values are copied from validated header values
and carry no explicit size limit of their own; they are bounded by the HTTP
parser's header limits, and a value that could add a header line is rejected
before it reaches the proxy.

Conditional reads are matched as RFC 9110 `*` / `#entity-tag`, not as a byte
comparison of the whole header: `*`, a list of tags (commas inside the quotes
belong to the tag, empty list elements are skipped) and weak comparison for
`If-None-Match` all work, and `If-Match` uses strong comparison. Repeated
field lines are combined into one comma-separated value, as RFC 9110 permits,
and a forwarded request carries the same bytes, so the leader reads the header
exactly as the follower did (a tag's bytes are never converted through text). A
combined value that cannot be read never turns into an unconditional
read: an unparsable `If-Match` answers `412`, while an unparsable
`If-None-Match` names no confirmable version and does not suppress the bytes.

The metadata Raft log is bounded: once `metakv/raft.wal` passes 8 MiB (or 4096
applied entries above its base) each node writes an atomic
`metakv/checkpoint.json` state image and rewrites the log, keeping everything
above a replicated compaction floor. `GET /raft/status` reports `base_index`,
`compaction_floor`, `wal_bytes` and `compaction_error`. A node that falls behind
the compacted base is re-seeded with that same image: the leader streams
`checkpoint.json` in 64 KiB `InstallSnapshot` chunks over the authenticated Raft
RPC (bounded at 64 MiB, resumable per group, leader and snapshot id), and the
receiver validates the group, membership, last included index/term and
whole-image checksum before publishing it with one atomic rename, rewriting its
log to the new base, and only then moving its applied pointer, so a crash leaves
either the old or the new base. After the install it is an ordinary log
follower. The checkpoint is still a full image, so its size tracks live metadata
rather than history.

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

## Testing

`cargo test` runs the MetaKV library target's tests in parallel. The OpenRaft
cluster tests assert progress against real wall-clock deadlines — a quorum
confirmation inside `heartbeat_interval`, a write inside the client bridge
deadline, a snapshot installed before a purge — so they are serialized through a
process-global test gate (`crates/yadb-metakv/src/openraft_test_gate.rs`). A full
run is therefore slower than the test count suggests. A red run in these tests
should be investigated as either a real quorum/leadership problem or CPU
starvation, not retried.
