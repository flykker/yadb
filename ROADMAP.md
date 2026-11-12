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
