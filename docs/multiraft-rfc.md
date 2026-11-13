# Multi-Raft for YADB: architecture and staged migration

Status: design proposal; production Multi-Raft is not implemented. This RFC specifies safety gates and
an initial direction; open choices below require implementation-level designs and
tests. Existing single-group behavior and protocol compatibility remain in force.

The isolated experimental module contains group identities, a typed WAL and an
incomplete deterministic memory-only core. The core is not connected to the WAL,
daemon or client traffic; it must not be used as a production consensus engine.
The selected production consensus dependency is OpenRaft 0.9.x. Its integration
boundary is compile-tested, but its durable storage adapter, group transport and
daemon lifecycle are not implemented yet. The OpenRaft storage contracts require
persist-before-return for votes/logs, and YADB must satisfy those contracts rather
than route OpenRaft through the experimental WAL/core. Envelope checks do not
authenticate peers. The runtime and verification gates below remain future work.

## Goals and non-goals

Distribute independent metadata/data publications across independently elected
Raft groups and nodes, without a control-group write on every data operation.
One group has at most one leader in a term; different groups may have leaders on
different nodes. This is not multi-master writes to one group. No throughput or
linearizable-read claim follows merely from creating multiple groups.

Keep PostgreSQL Simple Query, supported path-style S3, and iSCSI clients working.
Use ordinary filesystems; raw PDisk and NBD are not goals. Maintain existing
documented correctness, retention and unknown-outcome boundaries; do not claim
serializable or transaction-wide snapshot isolation without separate work.

## Current atomicity and shared dependencies

* `LocalMetaKv` wraps one `RaftCore`, one local ordered state machine, one WAL,
  and untagged `/raft/rpc` messages. `mod_revision` is presently meaningful in
  this one state machine. Reads are locally applied state, not quorum barriers.
* SQL commit writes immutable shard WALs first, then one `RevisionTxn` guards
  schemas and shard views and publishes all changed manifests and index entries.
  `CREATE INDEX` similarly guards schema and each scanned shard, including absent
  ones, and publishes schema/backfill atomically. Row collection copies selected
  schema, manifests and index hits in one local apply snapshot. Independent groups
  cannot preserve these properties by simply routing each key independently.
* S3 publication atomically guards the object and prepared upload intent, puts the
  manifest and retired version, and removes the intent. Abort/cleanup fences that
  same intent. Bucket listing scans an ordered key prefix. Partitioning intents
  by upload ID and objects by name without a protocol would break publication.
  Revision-based intent grace also assumes a single revision domain.
* Block writes publish a whole volume extent map with one revision guard after
  durable fragment writes; UNMAP uses the same map. Splitting this map changes
  atomicity of operations spanning extents and requires a separate design.
* `/s3-upload-sequence/{project}`, `/block-write-sequence/{project}`, and
  `/sql-blob-sequence/{project}` reserve non-reused 56-bit allocations before
  fragment PUT. Counters must not be reset, cloned, deleted or rolled back while
  fragments from their previous history can exist. Unknown reservations cannot
  authorize PUT. A single allocator per project would remain a hot shared group.
* Old and unpublished SQL/block fragments and retired S3 versions are retained.
  Moving metadata or a group does not by itself establish reader-safe GC.

## Proposed model

Introduce stable non-reused `GroupId`, independent group-local state machines,
WALs, elections and quorum, and a versioned, durable group directory. A small
control group owns group definitions, placement intent and directory epochs; it
is not contacted for each ordinary data publication once routing is cached.
Control-group failure can prevent topology changes and cache refresh; it must not
silently authorize stale topology. Exactly when data operations can continue is
defined by the eventual fencing and routing protocol, not by this RFC alone.

Route each data group over shared RPC using an explicit group identity. Group
identity must bind transport messages, persisted group metadata and WALs. Unknown
or mismatched groups, node membership, incarnation or cluster identity are
rejected; never fall back to another group's log. Versions are scoped: a bare
group-local revision cannot be compared with another group's revision or reused
as a globally unique identity. Migration and group recreation require distinct
incarnation/epoch identities and fencing to prevent ABA. Preserve legacy single-
group RPC and disk format until an explicitly versioned cutover is implemented.

Local read-only group snapshots do not form a cross-group snapshot. Scans, S3
listing and SQL index reads need a separately specified consistency/continuation
contract, including route changes; quorum freshness requires an explicit read
barrier or another proven protocol. Watches need per-group resume positions and
explicit gap/resync handling; global ordering is not assumed.

### Placement and lifecycle

Control records describe immutable IDs, group incarnation, configured voters,
placement intent, route epoch and lifecycle state. A manager reconciles desired
and actual state; each step is durable, restartable and idempotent. Failure
detectors and timeouts are only hints, not authority to bypass quorum or fencing.
Membership changes require a proven Raft reconfiguration protocol, not rewriting
the voters list. Until then use fixed memberships, no live reconfiguration.
Initially use manual placement and leader election; balancing, auto-failover of
placement and split/merge follow only after their safety tests pass. A group's
ordinary Raft leader election is separate from moving/reconfiguring replicas.

### Cross-group transactions

Raft gives atomicity only within one group. Candidate design: durable 2PC over
Raft groups with a replicated transaction record/decision and stable participant
set. Participants durably prepare intents and validate local guards; a coordinator
durably records exactly one decision before participants finalize. Reads must
resolve or wait on relevant prepared intents so no reader observes a partial
commit, including index/range scans and coordinator/participant restarts.
Unreachable decisions may block: a timeout is not permission to abort a possibly
committed transaction. Unknown client outcomes remain unknown until a safe
status/idempotency contract exists. A decision record must not be reclaimed until
all participants and readers can safely resolve it. This is a candidate, not a
complete 2PC specification or an implemented guarantee.

Define exact isolation and snapshot semantics separately: the present SQL buffer
validates written row values and does not validate complete read sets/ranges.
Adding atomic commit alone does not supply serializability or equal-value ABA
protection. Schema/index backfill and cross-group read snapshots need explicit
protocols before SQL metadata is split.

### Allocation

The initial runtime does not change existing allocators. Future allocation may
reserve disjoint ranges from a durable authority and persist the high-water mark
before use, so the control path is amortized, not contacted for each fragment.
This is not yet an approved algorithm: specify namespace encoding and exhaustion,
range ownership, crashes, stale nodes, restore, backup and migration first. No
range is usable after an ambiguous reservation. IDs, counters and all retained
fragments must remain collision-free across cutover and restoration; partial
restore or a cloned cluster must not silently reuse a live namespace.

## Staged implementation and acceptance gates

Independent review found that the legacy core does not bind its WAL to identity
or voters, accepts unknown Raft senders, and lacks exclusive WAL ownership and
complete structural RPC validation. In particular, reopening a multi-voter WAL
as a singleton can apply its uncommitted tail. These are blockers for using it
as a multi-group runtime, not guarantees supplied by a group envelope. The
experimental `multiraft` module validates configuration and message identity and
provides a separately provisioned identity descriptor. This descriptor is not
bound to a WAL and requires externally provided exclusive ownership and durable
directory provisioning; it does not open a WAL, authenticate peers, or dispatch RPCs.
An experimental OS directory lock and locked descriptor wrapper exist, but are
not wired to a group runtime. The lock is advisory and assumes cooperating
processes and a directory that is not renamed or replaced during use. It provides
no exclusion against legacy code that does not acquire it.
An experimental, separately provisioned group WAL header includes the full replica
identity and fixed voters and is verified while holding that OS lock. A separate
bounded framed-log API can append and recover opaque records and fails closed on
malformed frames without truncation. It does not validate Raft state transitions or
connect to the legacy core. An experimental typed wrapper additionally checks
term/vote monotonicity, log indices and committed-prefix replacement before append
and on replay. Overlapping entries with the same term and index must have identical
commands; this local check does not prove Raft's log-matching property across
replicas or across discarded historical suffixes. It does not implement Raft quorum, prove commit decisions, apply
the state machine, or constitute a completed runtime.
Before attaching a core, implement durable identity binding, exclusive ownership,
membership and structural validation, fail-closed recovery, and bounded resource
handling. The existing recovery can truncate a corrupt frame and does not prove
safety for arbitrary durable corruption. Legacy behavior is unchanged.

0. **RFC / dependency audit.** Inventory atomic transactions, revision domains,
   allocators, readers, watchers, and compatibility. Design before switching data.
1. **Isolated runtime.** Test-only/opt-in groups, fixed membership; independent
   WALs, elections, tagged transport and bounded scheduling. Existing production
   traffic remains on legacy group. Reject wrong group and unsafe disk reuse.
2. **Control and directory.** Durable bootstrap, stable group IDs/epochs,
   observability, route cache and restartable lifecycle. Verify no unsafe fallback.
3. **Membership and snapshots.** Design/test durable Raft snapshots, log
   compaction, lagging replica recovery and safe membership changes before live
   reconfiguration or large-scale deployment. Verify crash points and versioning.
4. **Cross-group transaction/read protocol.** Specify and implement durable
   decisions, participant recovery, visibility and scan behavior; test faults at
   every boundary. Do not claim serializable transactions without read/range tests.
5. **Migrate a bounded vertical slice.** Select only after key/transaction audit;
   include allocator, cleanup and client semantics. Compare old/new behaviors.
6. **Migrate SQL, S3 and block incrementally.** Preserve supported client APIs,
   existing atomicity and unknown-outcome behavior. Add manual moves, then
   automated placement and leader balancing. Design split/merge only afterwards.

## Cutover and restore requirements

Before cutover, develop a versioned offline migration with a coordinated stop of
all writers, consistent metadata + blob backup, manifest and counter validation,
and an explicit durable commit point. Import metadata and allocator high-water
marks without reusing existing BlobIds; retain old fragments. Validate routing,
group contents and invariants before opening writes. A failed/ambiguous cutover
must have a documented fail-closed recovery procedure; do not run old and new
writers together. Rollback after new-format writes requires a separately designed
consistent restore, not simply starting old binaries. No rolling upgrade or
downgrade is promised. Restore must coordinate metadata, fragments and allocation
namespaces and address any divergent surviving cluster before reopening writes.

## Verification matrix

* Deterministic multiple groups with distinct leaders, concurrent proposals,
  same `(term,index)` in different groups, wrong/unknown group RPC, log/path
  isolation, group restart and mixed-version rejection.
* Quorum loss in one group while another progresses; old leader rejoin; dropped,
  duplicated, delayed and reordered RPCs; group-local storage sync failure and
  process restart. An error may still mean a committed proposal.
* Transactions: crash or partition before/after every prepare, decision and
  finalize; recovery with unavailable coordinator; duplicate messages; readers,
  index scans and schema change racing with partial finalization; no mixed result.
* Directory/placement: stale routes and incarnations, failure at every lifecycle
  step, snapshot install and membership transitions, restore and cutover faults.
* Allocation: concurrent range reservations, ambiguous outcomes, exhaustion,
  split/move, restart, backup/restore and coexistence with historical BlobIds.
* Existing workspace regressions and three-node native PostgreSQL/S3/iSCSI E2E
  remain required. Simulated persistence endpoints do not prove arbitrary partial
  writeback, real ENOSPC, hardware power loss or kernel multipath/KVM behavior.

## Open decisions

Group sizing and placement granularity; partitioning by table/key/object/volume;
cross-group isolation and scan/list continuation semantics; 2PC coordinator and
decision GC; scalable non-reused ID allocation; leases/read barriers; rolling
membership and snapshot formats; migration validation and rollback authority;
resource scheduling for thousands of groups. Resolve each before its gate.

## Reference, not an equivalence claim

YDB uses tablets on distributed shared storage, with Hive for tablet placement and
specialized distributed transaction machinery; it is not described here as a
Multi-Raft implementation. See [YDB tablets](https://ydb.tech/docs/en/concepts/glossary.md#tablet-implementation),
[Hive](https://ydb.tech/docs/en/contributor/hive.md), and
[distributed transactions](https://ydb.tech/docs/en/contributor/datashard-distributed-txs.md).
