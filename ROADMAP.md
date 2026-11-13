# YADB development roadmap

This is a working plan, not a statement that the listed guarantees already hold.
YADB uses ordinary filesystems; raw PDisk and NBD are not project goals.

## P0 — correctness and recovery

- [ ] Finish the protocol review: SCSI allocation lengths and residuals, PostgreSQL
  transaction errors, and S3 conditional/concurrent reads. Reproduce suspected bugs
  and add regression tests before claiming fixes.
- [x] Exercise pgwire failed-transaction handling after duplicate-key DML: confirm
  25P02 for subsequent statements, ROLLBACK and connection reuse with no buffered
  changes committed. Exercise iSCSI INQUIRY zero/exact/short responses and residual
  flags alongside existing allocation-length checks for other SCSI commands. This is
  targeted coverage, not a complete protocol or SQLSTATE compatibility audit.
- [ ] Build a repeatable fault-injection suite covering process crashes at write and
  commit boundaries, complete cluster restart, node loss and recovery, network
  partitions, quorum loss, and a stale leader returning.
- [x] Verify ambiguous Raft proposal results at the transport boundary. Tests now
  cover a lost replication response followed by commit, replacement of an
  uncommitted entry after leader change, and a lost commit notification. Proposal
  results are matched by `(log index, term)` so an old caller cannot receive the
  result of a newer leader's command at the same index. An unknown outcome still
  requires caller-level idempotency/status handling.
- [x] Exercise ambiguous SQL and S3 writes through live three-node processes.
  The test drops Raft replies after forwarding requests, forces a majority
  election, and verifies after convergence that each failed request is either
  absent or present with exactly its original value. It does not claim a client
  can determine which outcome occurred without a status/idempotency API.
- [x] Repeat the native-client three-node recovery run with explicit confirmation
  that SQL/S3 publication replies were dropped. After election and convergence,
  record each ambiguous result and verify it survives a full SIGKILL/restart of
  all nodes. This tests process failure with retained filesystem state, not
  hardware power loss or arbitrary partial writeback.
- [x] Investigate intermittent responses observed before fault injection while
  running the three-node E2E through local peer proxies. The failures were caused
  by the test proxy's default TCP listen backlog of 5: concurrent Raft/VDisk
  connections timed out under load. The harness now uses backlog 128 and captures
  proxy/node diagnostics; repeated concurrent and native-client runs pass. This
  does not change production listener limits or prove arbitrary overload behavior.
- [ ] Check durability ordering from filesystem sync through fragment quorum and
  metadata commit; test torn writes, disk-full errors and corruption/repair.
- [ ] Verify tablet generation fencing, concurrent old/new owners, recovery and
  transaction atomicity under failures; document ambiguous commit outcomes.
- [x] Reproduce block fragment-ID collisions for two CRC32-colliding volume names
  on independent engines, and stale extent publication after delete/recreate ABA.
  Use durable project-wide allocations on channel 5 and revision-guarded RMW/UNMAP
  from one metadata snapshot. Test concurrent reservation, rollover/exhaustion,
  legacy reads, failed fragments, restart and ambiguous sync outcomes. Retain old
  and unpublished fragments; safe block GC remains open.
- [x] Reproduce SQL unpublished-preparation ID reuse, colliding tablet owners,
  stale publication after manifest delete/recreate and compaction breaking old
  snapshots. Reserve durable project-wide channel-6/7 WAL/SST IDs; use revision
  guards and revision-based shard-cache identity. Retain old/unpublished SQL
  fragments and report zero physical evictions. Cover legacy recovery, stale
  flush/compaction, boundaries, concurrent allocation and ambiguous sync outcomes.
- [x] Reproduce a lost update between cross-engine row validation and WAL
  preparation. Revalidate buffered original rows in the tablet view used for
  preparation and revision-guard publication, including no-op-only and mixed
  no-op/mutating shards. Deterministic tests cover both scheduling boundaries,
  update/insert/delete, index contents, cross-shard rejection and simulated
  power-cut recovery; unrelated row changes before preparation can survive.
- [x] Reproduce CREATE INDEX publishing stale backfill after independent-engine
  DML. Guard the exact scanned shard revisions, including absent shards, and schema
  revision in one atomic index/schema publication. Test update/delete/insert at
  scan/publication boundaries, manifest/schema ABA, old-schema prepared DML,
  accepted changes before a shard's scan, retry, power-cut recovery and both modeled
  publication sync outcomes. This conservative full-buffered backfill may conflict
  under writes/maintenance and does not provide nonblocking online index building.
- [x] Reproduce a single row collection mixing shards from before/after one
  atomic commit. Validate the local applied metadata revision around complete
  collection (schema/index hits/rows), rejecting changes rather than returning
  mixed data. Cover indexed lookups, range inserts into absent shards and schema
  changes. Check new commits/phantoms between statements and own-write overlays
  inside BEGIN; document these as limits, not snapshot/serializable isolation.
- [x] Replace global-revision read conflicts with owned selected-metadata snapshots
  captured under the local apply mutex. Use exact copied manifests for query-local
  read-only tablets, including absent shards and index hits. Test concurrent writes,
  dropped indexes, compaction, own overlays and atomic metadata snapshot copies.
  Copying/replay overhead remains; snapshots are not persistent GC reader leases.
- [x] Fence UPDATE/DELETE against changes between row collection and buffering:
  retain the collected row as the original value instead of rereading it. For PK
  changes preserve the observed absence of the target. Independent-engine tests
  cover update, delete, PK move and concurrent target insert. Value-equal ABA and
  transaction-wide read/range validation remain outside this protection.
- [ ] Establish quorum freshness/read barriers separately. Per-collection snapshots
  are locally consistent but follower/isolated-node reads can still be stale.
- [ ] Define/test transaction-wide read-set/range validation, write skew and
  serializability before claiming stronger transaction isolation.
- [ ] Prove GC and compaction do not remove live data, including during recovery,
  concurrent operations and failed commits; reclaim unreferenced block fragments.
- [x] Reproduce and fix loss of a durable GC barrier when a sealed volume segment
  is compacted and the node restarts. Compaction now carries barrier markers into
  the replacement segment; a regression test confirms old-generation writes remain
  fenced after recovery.
- [x] Add fenced cleanup for new S3 failed uploads: persist complete intents before
  fragment PUT, allocate non-reused channel-4 IDs, require wall-clock + revision
  grace, abort through consensus before deletion, and retain work until all physical
  parts acknowledge exact tombstones. Tests cover interrupted cleanup/restart,
  failed/torn fragment and publication writes, publish/abort races, and ID allocation.
- [x] Add modification-revision transaction guards for S3 to reject per-key-version
  ABA after delete/recreate, including after WAL recovery. Keep old version-based
  guards for existing callers; SQL/block publication and cache identity now also
  use modification revisions. This is not a complete metadata/isolation audit.
- [x] Serialize volume PUT validation/publication, tombstones, barriers and compaction;
  pin matching file/offset pairs for reads. Test conflicting PUTs, PUT/fence races,
  concurrent read/compaction, and payload bit-rot before later durable records.
- [ ] Implement cross-node reader retention/fencing before reclaiming overwritten
  or deleted S3 versions, SQL WAL/SSTs or old block extents. Retired/old fragments
  and unknown historical blobs are deliberately retained; do not infer garbage
  from absent current references.
- [ ] Bound tombstone/retired metadata growth and validate real ENOSPC/sync errors,
  hardware power loss, pool-placement changes and mixed-version upgrade behavior.
- [x] Inject sync EIO before/after full persistence; fail closed after WAL errors
  and active-volume sync failures. Reproduce follower retry acknowledgement of
  memory-only entries and process-only reopen acknowledgement of unsynced records;
  sync recovered files/directories before serving. Cover S3 publication/tombstones,
  rollover and compaction in simulation. Arbitrary partial writeback and real sync
  errors remain unverified; recovery requires restart, not automatic re-enablement.
- [ ] Run kernel open-iscsi + dm-multipath + KVM guest I/O on a Linux host; kill
  active targets, partition networks, restore paths and verify guest data integrity.
  This is blocked on this Android environment by missing KVM and device mapper.

## P1 — operations and cluster lifecycle

- [x] Select OpenRaft 0.9.25 for the future Multi-Raft consensus engine; compile
  offline and exercise a volatile singleton. This does not replace legacy Raft.
- [ ] Stop development of the experimental hand-written group core. Its uncommitted
  changes are not a production candidate; preserve them pending an explicit cleanup.
- [ ] Exercise real OpenRaft in-process with three nodes: quorum loss, recovery,
  lagging replicas and reordered/lost messages. Memory tests do not prove durability.
  A volatile three-node test covers replication, follower catch-up, quorum loss
  and recovery, leader failover, duplicate AppendEntries and replay of a delayed
  duplicate after a newer write; arbitrary reordering and other scenarios remain open.
- [ ] Build and audit OpenRaft durable storage, snapshots and serialized vote/log
  writes; run OpenRaft's storage suite and crash/fault tests before daemon use.
  A `#[cfg(test)]` file-backed prototype now exercises serialized framed vote/log
  writes, restart recovery, suffix replacement, lock ownership and fail-closed
  corruption handling. It is not production storage: fault injection, stale-lock
  policy, snapshots, state-machine persistence and OpenRaft storage-suite coverage
  remain open.
- [ ] Secure internal Raft RPC and client identity: separate listeners, authenticate
  peers (evaluate rustls/mTLS), bind node and group identity, and do not trust
  client-supplied subject headers. Existing deployment exposure must be audited.
- [ ] Evaluate ready libraries before extending custom parsers: hyper/reqwest for
  HTTP, ring/subtle for secret verification, pgwire, SQL/YAML parsers, S3/SigV4,
  Reed–Solomon, and Linux LIO or SPDK for iSCSI. Check licenses, offline builds,
  existing wire/disk compatibility and migration with golden/interop tests.
- [x] Record the Multi-Raft dependency audit, safety invariants and migration
  gates in [docs/multiraft-rfc.md](docs/multiraft-rfc.md). This is a proposal,
  not an implementation or a commitment to unsupported transaction guarantees.
- [ ] Implement an isolated Multi-Raft runtime with explicit group identities,
  independent storage/elections and fixed membership; preserve existing traffic.
- [ ] Design and test the directory, group lifecycle, cross-group transactions,
  read visibility, safe allocation and coordinated migration before routing
  existing SQL/S3/block data across independently replicated groups.

- [ ] Add a tablet lifecycle manager: durable ownership, failure detection,
  generation fencing, recovery, observability and controlled migration.
- [ ] Design and test Raft snapshots/log compaction and lagging-node recovery;
  safely change cluster membership and storage-pool placement.
- [ ] Implement/test backup and restore, capacity management, disk replacement,
  safe upgrades, operational runbooks and resource/tenant quota enforcement.
- [ ] Secure internal RPC and client connections, define credential rotation,
  expose metrics and alerts for quorum, latency, degradation and capacity.
- [ ] Expand PostgreSQL, S3 and iSCSI compatibility according to actual client
  needs; keep unsupported features explicitly documented.

## Future — scalability and new workloads

- [ ] Sharding: design a shard map and routing, range-based splitting and merging,
  load/size triggers, live migration, balancing and failure-safe reconfiguration.
  Establish transactional and consistency semantics across topology changes.
- [ ] OLAP: evaluate columnar storage, compression, vectorized execution,
  analytical indexes, SQL planning and resource isolation from OLTP and block I/O.
  Define snapshot and transaction semantics before implementation.
- [ ] Topics/streaming: design durable partitioned logs, retention, consumer
  groups, offsets, replay, quotas and ordering. Explore Kafka protocol compatibility
  with explicit API/version coverage; don't promise full Kafka compatibility.
  Evaluate transactions between topics and tables as a later milestone.
- [ ] Consider CDC, asynchronous replication and external/federated queries after
  core durability, recovery and operational foundations are demonstrated.

## Evidence and constraints

Keep tests reproducible on aarch64 with `cargo --offline`; distinguish simulated
failure tests from real hardware, Linux kernel multipath and VM validation. Record
known limits and avoid describing implementation components as production guarantees.
