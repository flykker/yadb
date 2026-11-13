# YADB development roadmap

This is a working plan, not a statement that the listed guarantees already hold.
YADB uses ordinary filesystems; raw PDisk and NBD are not project goals.

## P0 — correctness and recovery

- [ ] Finish the protocol review: SCSI allocation lengths and residuals, PostgreSQL
  transaction errors, and S3 conditional/concurrent reads. Reproduce suspected bugs
  and add regression tests before claiming fixes.
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
- [ ] Test transaction isolation, read-your-writes, conflicts and range/phantom
  behavior; state only the isolation guarantees actually established.
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
  guards for existing callers; other engines still need their own ABA audit.
- [x] Serialize volume PUT validation/publication, tombstones, barriers and compaction;
  pin matching file/offset pairs for reads. Test conflicting PUTs, PUT/fence races,
  concurrent read/compaction, and payload bit-rot before later durable records.
- [ ] Implement cross-node reader retention/fencing before reclaiming overwritten
  or deleted S3 versions. Retired manifests/fragments and unknown historical blobs
  are deliberately retained; do not infer garbage from absent current references.
- [ ] Bound tombstone/retired metadata growth and validate real ENOSPC/sync errors,
  hardware power loss, pool-placement changes and mixed-version upgrade behavior.
- [ ] Run kernel open-iscsi + dm-multipath + KVM guest I/O on a Linux host; kill
  active targets, partition networks, restore paths and verify guest data integrity.
  This is blocked on this Android environment by missing KVM and device mapper.

## P1 — operations and cluster lifecycle

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
