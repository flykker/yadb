# OpenRaft Multi-Raft production checklist

## Current status (2026-11-14)

**Before the test serialization gate described below, the last completed full
gates were GREEN on the authorization-freshness delta plus the nonblocking-refresh
fix and post-review hardening.** Offline, locked `-j 2` runs
with `CARGO_INCREMENTAL=0` and the approved `YADB_TEST_TMPDIR` root passed:
all-features MetaKV **124/124** (68.77 s), yadbd units **27/27**, clients
**2/2**, daemon **2/2** (10.11 s); default MetaKV **124/124** (75.50 s), yadbd
**20/20**. The previously red all-features
MetaKV failure at `crates/yadb-metakv/src/openraft_multigroup_test.rs:245`
(`OpenRaftWriteUnknown`, client write timeout) did not reproduce: that test
passed alone **15/15** and inside both full gates. No causal fix is claimed and
the historical process-failure/election-churn failures remain unexplained.
Logs: `/data/data/com.foxdebug.acodefree/cache/opencode/workspace-all-features-review2-20261114.log`
and `.../workspace-default-review2-20261114.log`. Security tests pass **16/16**,
lifecycle unit tests for `scripts/yadb-local.py` pass **11/11**, and smoke
port-base tests pass **3/3**.

Authorization freshness is now lease-bound. In OpenRaft mode a node accepts a
control snapshot only after a quorum-confirmed linearizable barrier on the
current control-group leader. The leader reads its own applied state after
`ensure_linearizable`; a follower requests the same snapshot through the
internal `GET /internal/control/snapshot` endpoint protected by the Raft token.
The snapshot is installed before the lease is extended, so observing a fresh
lease implies the corresponding control state is present. The background refresher
wakes on a nominal 200 ms interval and leases expire after 2 s; after expiry,
DB/block/S3/PostgreSQL-wire/iSCSI authorization fails closed unless an on-demand
refresh attempt succeeds. A failed
refresh or quorum loss does not immediately invalidate an existing valid lease;
authorization may continue using its snapshot until expiry. Revocation is not
instantaneous cluster-wide. The 2 s TTL is measured at snapshot acceptance, not
at the barrier, so neither a hard 2 s snapshot-age/revocation bound nor a 200 ms
revocation bound is established. The 200 ms interval is a wakeup cadence, not a
completion guarantee, and the fetch has per-operation socket timeouts, not a total
deadline.

The HTTP authorization path no longer performs the blocking fetch on a Tokio
worker: `control_for_authorization_async` and `refresh_openraft_control_async`
delegate to `spawn_blocking`, while the PG/iSCSI connection threads and the
background refresh thread keep the synchronous path. Before this change a stalled
control leader could occupy a runtime worker in a TCP read and hold the refresh
mutex, preventing the node from answering the leader's internal snapshot request.
A `SIGSTOP` of the control leader for about 1.2 s left follower health and 403
denials at 4–14 ms. This is plausible mitigation for historical
`client write timed out`, not a reproduced or causally proven fix. It is also not
starvation-proof: failed refreshes serialize, each expired-lease request can
enqueue a blocking task, and the internal endpoint shares the blocking pool.

The completed final independent GPT-6.1 Sol review of this delta returned ACCEPT
for the narrow local scope only. Its in-scope non-blocking findings
were fixed: empty Raft tokens are rejected instead of matching an absent header,
S3-wire error mapping preserves `x-yadb-write-outcome`/`x-yadb-retry`, the
`--port-base` upper bound is 55527, prepared-state status also checks Raft
listeners, and docs no longer claim more than the code proves. Open findings are
listed in `docs/multiraft-verification.md`; they are not production blockers for
the narrow scope but remain before any broader claim.

The completed final independent Sol leadership review accepted only the narrow
local group-count balance, not production readiness. Earlier
targeted checks also passed: security tests 16/16, state tests 16/16, storage
23/23, S3 19/19 and IO 7/7; `git diff --check` passed. These do not replace the
full gates above. S3 GC is disabled. OpenRaft `consistent=true` block reads are
implemented (linearizable extent-map read on the project-group leader, forwarded
from followers, missing blob is a `503`), so the iSCSI read path is no longer
refused; that map snapshot is not a GC fence. SQL (`"consistent": true` in the
`POST /db/{project}/{db}/query` body, `SELECT`/`SHOW` only) and S3
(`?consistent=true` on object GET/range GET) now have the same explicit opt-in:
one quorum-confirmed snapshot of the SQL database namespace or of the object
manifest, no fallback to local applied state, `MetaError` propagated (legacy
metadata answers the routing error), and a chunk the pinned S3 manifest names
but that is gone is a `503`. Defaults are unchanged. Startup and shutdown cancellation
are both covered: a dropped startup future releases the acquired replica and the
durable lock. The POSIX compaction stale-FD claim was
false; explicit reopen and an append regression are retained only.

`force_leader` is routing preference, not a linearizable-read guarantee. Legacy
remains default, `production_ready: false`, no existing cluster was switched,
old data was untouched, and no migration occurred. Do not claim known
regressions resolved.

Status: **not production-ready**. Legacy Raft remains the daemon default; the
v2 client integration is still being hardened. A local
nonproduction loopback cluster is running at `/public/cluster_data`: three
daemon processes host nine replicas. It is **not empty**: at 21:25:24 on
2026-11-14 an unidentified apply committed exactly `examples/platform.yaml`
(generation `1`, project `billing`), which the orchestrator did not issue and
`yadbctl` did not perform; the state was preserved, not rolled back. Freshness and
token-gated internal-snapshot behavior were exercised live (unknown-project
SQL/S3/block reads return 403, not 503; the internal endpoint returns 401 without
a token, 200 with revision 2 on the control leader and 409 on followers).
Independent Sol accepted only the narrow delta, not production
readiness; see `docs/multiraft-verification.md` for the root, evidence, incident
and election-churn caveat. The
`openraft-storage` feature is opt-in, not an operational release.
Do not enable production routing until the gates below are complete. The user
has separately authorized this new nonproduction test cluster to exercise
real client routing before those gates close; it is not a production release or
authorization to migrate existing data. The leadership-preference,
authorization-freshness and nonblocking-refresh deltas now have passing full gates
and a narrow-scope independent review; broader protocol semantics, starvation
bounds and reliability gates remain open.

## Authorized client-routing work on the nonproduction test cluster

Status: implementation in progress; the new nonproduction cluster is running but
is no longer empty (see current status above), and no existing cluster has been
switched. See `docs/multiraft-verification.md`.

- [ ] Add explicit daemon metadata-runtime selection, preserving legacy default
  and requiring the OpenRaft feature and mandatory-mTLS group configuration.
- [ ] Route existing SQL, S3, block and iSCSI metadata through actual OpenRaft
  client writes across several active groups. Use a dedicated control group and
  an explicit immutable project-ID-to-data-group map with at least two distinct
  data groups. Keep each project's SQL/S3/block metadata in its assigned group,
  retaining atomic multi-key operations and existing local-applied read semantics
  without claiming linearizable reads. A single-group adapter is only a component
  test, not completion of the user's Multi-Raft request.
  The three-group engine integration case passes: control plus two data groups
  serve SQL/S3/block for two projects, with state-placement, cross-group rejection
  and all-group reopen assertions. These are single-node groups; daemon and
  a child-daemon singleton-group integration test has also passed. A targeted in-process
  loopback-mTLS test now covers nine replicas, three nodes and three groups with
  group 2 led by node 1 and group 3 by node 2; it checks per-project `NotLeader`,
  continued group 3 writes after stopping group 2's leader and selected reopen
  state. It does not establish multi-process/multi-host behavior or replacement
  election. A separate targeted three-process daemon test covers SQL/S3 HTTP
  leader routing; the local cluster smoke also covers SQL/S3/block, but broader
  multi-node daemon and protocol validation remain pending.
- [ ] Select the independent group leader for forwarding and protocol transaction
  fencing; do not use the control group's leader/term for every project.
- [ ] Reject transactions spanning groups before submission. Never synthesize a
  global revision or an atomic cross-group snapshot. Use project-group revisions
  for S3 leases/GC; administrative merged scans are non-atomic local observations.
- [ ] Require a new empty data root for creation, bind its runtime/replica identity
  durably, reject mismatched reopen and legacy access to marked OpenRaft roots.
  Do not modify, import or erase old legacy directories; no automatic fallback.
- [ ] Test actual adapter/engine paths, empty-root admission, persistence/reopen,
  follower write rejection and disabled legacy Raft endpoints in OpenRaft mode.
- [ ] Preserve control-plane/RBAC watcher correctness across durable snapshot
  installation: notify installed-state deltas only after successful persistence,
  never on failed installation, and retain subscriptions for later applies.
- [ ] Ensure a lost forwarded mutation response does not trigger automatic
  resubmission in the new runtime; retain explicit unknown-outcome semantics.
- [ ] Complete post-fix combined offline/locked regressions, address the completed
  independent Sol review's blockers, and keep `production_ready: false` until gates close.

Targeted evidence: the orchestrator's offline/locked `openraft_client::tests`
passed 2/2 in 0.19 s after correcting groups 1/2/3; its
`openraft_state_machine::tests` passed 16/16 in 0.73 s, including durable
snapshot watch delta/removal, no notification on failed write and surviving
subscriptions. Both used the approved `YADB_TEST_TMPDIR`, `CARGO_INCREMENTAL=0`
and `-j2`. Terra reported 19/19 `yadb-s3` tests passed offline/locked, with no
doc tests. These do not close the unchecked gates or imply final review.
The `openraft_daemon` child test passed 1/1 in 1.06 s using the feature-enabled
daemon and v2 singleton groups: HTTP apply of two projects and SQL/S3/block writes,
durable state placement, SIGINT/reopen HTTP reads, legacy rejection of the marked
root, changed-map rejection and an untouched legacy sentinel. It predates final
control-cache hardening and full-suite validation and is not multi-node or deployed
cluster evidence. An earlier default full-workspace gate failed: with the approved
test temporary root and `CARGO_INCREMENTAL=0`, `cargo test --offline --locked -j2
--workspace` reported 122 MetaKV passes and one failure in 116.92 s. The new
`openraft_runtime::multigroup_tests::three_nodes_three_groups_route_independent_leaders_over_mtls`
timed out at line 69 after 15 seconds awaiting leader 1; node 1 was Candidate at
term 2 with last log 0, applied None and leader None. Its earlier targeted 1/1 pass
is not reliable combined-gate evidence. Terra is investigating a possible
initialization/immediate manual-election race under concurrent load; no root cause
was established and no retry or timeout increase masked it. Both process tests and
the 17 MiB snapshot test passed in this run, without resolving the historical
process catch-up failure. At that checkpoint final all-features testing and
independent Sol review were pending; see current status above.

The subsequent `openraft_daemon` full file passed 2/2 in 2.39 s; its new test
passed alone 1/1 in 3.58 s. Three actual daemon processes host nine replicas in
three loopback groups, bootstrapped and observed with control/data leaders 1/2/3.
SQL and S3 writes for both projects through node 1 returned the respective
`x-yadb-leader-id` 2/3 and underlying placement persisted after shutdown. This
closes that targeted multi-node HTTP routing check only, not multi-node block,
PostgreSQL-wire or iSCSI coverage, deployment, final hardening or combined review.

Subsequently Terra corrected the independent-leader test setup: initialization
already starts an election, so the duplicate immediate manual trigger was removed.
Only the designated bootstrap node has its normal automatic election timer enabled;
follower timers are disabled, avoiding a stranded no-retry configuration. The trace
is consistent with the duplicate trigger, not proof of why quorum was missed. No
timeout increase or test retry loop was added. The subsequent offline/locked
`-j2 --workspace` default-feature run passed, including 123/123 MetaKV tests and
the concurrent 17 MiB snapshot test; daemon integration files ran zero tests under
default features as expected. This predates final daemon control-cache/barrier edits,
so final combined verification remained necessary. The all-features gate and
independent Sol review were then pending; see current status above. Neither this
pass nor the test setup change closes production gates or resolves the historical
process catch-up regression.

This changes the earlier legacy-only test-routing policy, not the production
acceptance criteria. Existing data migration, durability/security qualification
and multi-group design remain separate incomplete gates.

A second flaky MetaKV test was diagnosed rather than retried.
`openraft_persistent_cluster_recovers_lagging_follower_via_snapshot` failed about
8% of all-features runs. Its `snapshots_delivered > 0` precondition was vacuous:
the counter is global across targets and openraft reports success even when it
skips an install because the snapshot is not newer than the follower's committed
log. Diagnostics showed a leadership change to term 4, node 3. openraft 0.9.25 has
no pre-vote, so the isolated follower inflated its term and deposed the leader on
rejoin; the new leader's unpurged log then let the follower catch up by append
entries, so no snapshot was installed and the final durable-snapshot assertion
failed legitimately. The test now polls the lagging follower's own state machine,
uses 20–25 s election timeouts so leadership is stable for the ~1.5 s run, and
passed 40/40 consecutive runs. The term inflation is a real liveness property of
this Raft configuration and a candidate explanation for historical election churn;
it is not proven to be the cause and no mitigation is implemented.

### Heavy-test serialization gate (2026-11-15)

`crates/yadb-metakv/src/openraft_test_gate.rs` serializes the OpenRaft tests that
assert wall-clock progress: the multigroup independent-leader test, both
three-process mTLS tests, the child-process kill/reopen test and the two persistent
three-node catch-up tests. Each acquires a process-global `tokio::sync::Mutex`
through `exclusive()`; the process tests take it in the parent only. That skip is
not a deadlock fix — the mutex is process-local, so a child re-executing the
binary would get a private copy — it just keeps child mode honest. Both full
workspace gates were red before the gate, with a heartbeat-bound quorum miss, a 5 s
bridge-deadline write timeout and a process-test leadership change. The gate
restores the dedicated-CPU assumption behind the first two and mitigates the
load-aggravated third; it does not add pre-vote to openraft 0.9.25, does not widen
the 50 ms-heartbeat quorum window and does not change the 5 s bridge deadline. Its
scope is one process: other test binaries, other Cargo invocations and the live
cluster still compete for CPU, so it reduces contention rather than guaranteeing a
dedicated core. A post-gate full-workspace run has not finished, so no gate is
green here. What remains unexplained: the historical process catch-up failures and
the historical election churn. Solo passes (the process test 8/8, the multigroup
test 24/24 with eight concurrent copies) do not establish a cause for either.

## 1. Contracts and durable storage

- [ ] Run the OpenRaft 0.9.25 storage suite on the actual adapters. Its test
  harness requires `NodeId: From<u64>`, unlike YADB's `u32`; adapt the harness
  without changing production semantics or implement and document equivalent
  coverage with a case-by-case mapping. Do not claim suite compliance otherwise.
- [ ] Audit vote, append, truncation, purge, callback ordering, concurrent readers,
  restart and bounds against OpenRaft contracts; test each boundary and replay.
- [ ] Bound memory, disk growth, startup replay and snapshot reception; introduce
  compaction and resource limits without losing acknowledged state.
  The checkpoint implementation is under validation: 32 MiB active-log limit,
  1024 replay frames, 4096 retained live entries and an 8 MiB serialized live
  checkpoint. Replacements sync a new file, rename, sync the directory, then
  switch the append handle; failures poison until validated reopen. Temporary
  replacement can increase peak disk usage, and memory/CPU admission across
  groups, end-to-end snapshot capacity and real failure testing remain open.
  Storage hardening's targeted run passed all 21 storage tests and the adapted
  suite after rejecting disconnected prepends before I/O. Contiguous prepends
  survive checkpoint/reopen, and overlap comparison uses bounded serialization.
  These results do not clear the separate process catch-up regression.
  The subsequent purge/checkpoint mismatch is fixed: a purge marker leaving a
  gap before the retained suffix is rejected before I/O. The regression covers
  markers immediately before, within and beyond the suffix, conflicting identity,
  unchanged files after rejection and checkpoint/reopen of accepted states.
  All 22 storage tests and the adapted suite passed after the fix; broader
  storage-contract and combined-regression gates remain open.
- [ ] Ensure file and parent-directory durability for create, rename and reopen,
  including partial bootstrap, directory sync failures and unknown outcomes.
- [ ] Make snapshot identity, membership, log pointers, concurrent build/install,
  watchers and recovery correct; explicitly test stale and conflicting snapshots.
- [ ] Inject write, partial write, file sync, rename and directory sync faults via
  an injectable filesystem; test every outcome and poisoned-handle recovery.
- [ ] Run separate-process crash/kill and OS/filesystem power-loss tests; document
  filesystem and hardware assumptions. In-process fault simulation is not proof.
- [ ] Redundancy of acknowledged writes. `PoolStriper::put_blob` used to ack once
  `min_write_quorum()` fragments landed — `k` for Reed-Solomon 4+2, so an ack could
  leave zero parity on disk. It now requires every fragment: a short write is
  refused with `BlobError::RedundancyNotMet`, counted in
  `PoolHealthSnapshot::refused_puts` / `ScrubSummary::refused_puts`, and mapped to
  `503` on the SQL, S3 and block paths. Nothing is published for a refused write, so
  the caller holds no reference to the fragments that did land; those fragments are
  garbage, not corruption, and the mark-and-sweep collector reclaims them (see the
  garbage-collection item). The consequence is deliberate: a pool with any slot
  offline refuses new writes instead of accepting unprotected ones, so write
  availability now follows full redundancy. The three-node daemon test asserts this:
  a write while one replica is stopped is refused with `503` and stays invisible,
  and the same statement succeeds after that replica returns. Open: no test
  exercises a real multi-node pool; a refused write is still readable by id until
  the next collection pass; and refusal is per pool, so one offline domain stops
  writes for every volume on it.
- [ ] Idempotency and read-consistency contracts. A keyed `POST /v1/apply` is
  implemented (`Idempotency-Key`, the outcome record committed in the same
  `RevisionTxn` as the mutation, `POST /v1/idempotency/resolve`, and a replicated
  `10_000`-record / `8 MiB` admission counter at `/sys/idempotency/_usage` that
  refuses with `429` before planning). Three project-scope data paths are keyed:
  block UNMAP, block write and S3 object PUT/DELETE. For all three the record is
  project-scoped (`/idempotency/<project-id>/…`), the counter is group-scoped
  (`/idempotency/g<group-id>/_usage`) because a transaction cannot span groups,
  and the record, its counter update and the data mutation are appended to *one*
  guarded `RevisionTxn`: `BlockVolumeEngine::unmap_at_with` /
  `write_at_with` and `ObjectStoreEngine::put_object_with` /
  `delete_object_with`. A caller guard that fails aborts the whole apply, so
  there is no state in which the map/object advanced without the record or the
  record exists without the mutation. `resolve` accepts a project scope and
  fences in the project group. Block writes and S3 PUTs stage immutable
  fragments before that transaction; a refused, conflicted or crashed write
  therefore leaves unreferenced fragments (and, for S3, a `Prepared` intent)
  and nothing rolls them back. Only the S3 namespace has an intent collector
  (`ObjectStoreEngine::collect_orphans_all`, disabled on Multi-Raft), so block
  fragments from a refused or replaced write accumulate until a mark-and-sweep
  pass reclaims them; see the garbage-collection item. SQL writes are
  still **not** keyed: `POST /db/{project}/db/query` returns 501 for a keyed
  write script, because a script can mutate any number of tables, shards and
  secondary indexes through `DatabaseEngine::commit_tx_buffer_at_boundaries`,
  and keying it requires threading the record into *that* transaction rather
  than appending ops to one handler-level transaction. That is deliberately a
  separate change.
  Reads mostly still serve the locally applied state:
  `LocalMetaKv::try_linearizable_snapshot` is consumed by the OpenRaft block
  read with `consistent=true`, the SQL `consistent` body flag and the S3
  `?consistent=true` GET (and the iSCSI read path), which forward to the
  project-group leader and fail rather than return a stale or partially collected
  volume. The SQL `consistent` body flag and the S3 `?consistent=true` GET take
  exactly one such snapshot (the whole `/db/{project}/{db}/` prefix for SQL, the
  object manifest key for S3), never mix it with local applied state, and
  propagate `MetaError` instead of falling back; SQL refuses anything that is not
  `SELECT`/`SHOW` and any session/transaction flag with `400` before taking a
  snapshot. Those paths are still opt-in, and the default read remains
  local-applied.
  `docs/multiraft-idempotency-design.md` holds the full contract and the blockers
  that prevented the remaining scopes before this change; the SQL blocker above
  is the one that remains. A linearizable read still needs one pinned snapshot
  per read path. The group-scoped counter also means projects that share a group
  share one budget, which is intended but not yet exercised with a real second
  project on the same group.
- [x] Garbage collection on the legacy metadata runtime. `POST /v1/pools/gc`
  runs one bounded mark-and-sweep pass, and `POST /v1/pools/scrub` runs the same
  pass after its repair scan; the leader's periodic repair scan runs the sweep
  only every five minutes, because a pass holds the exclusive collector lease for
  its whole duration and a per-scan pass would keep an explicit `/gc` refused.
  Both report `blobs_live`, `fragments_deleted`, `bytes_reclaimed`, `leases_live`
  and an explicit `gc_refusal`, and `/gc`
  answers `409` with that reason instead of a `200` that could be read as
  "nothing to do". The mark is every fragment named by `/db/` tablet manifests
  (WAL and SST), `/s3/` object manifests, `/s3-intent/` in-flight uploads and
  `/block/` extent maps, from one applied snapshot. The sweep deletes only ids a
  mounted slot actually holds and the mark does not name: a durable generation
  barrier where the channel's `(owner, generation)` space is monotonic and the
  dead range is provably disjoint from every live generation (S3 channel 4,
  block channel 5), an exact per-fragment tombstone everywhere else (SQL WAL/SST
  channels, whose owner id is name-derived and whose generation restarts at
  zero, so a permanent barrier would fence a recreated tablet). It refuses with
  zero deletions when the node is not the metadata leader, when any reader or
  writer lease is live, when another pass holds the collector lease, when a
  lease record cannot be parsed, when a mounted pool cannot be enumerated, or
  when a manifest is unreadable. Readers publish a 30 s lease before reading and
  writers a 300 s lease between the fragment append and the publication that
  names it; the collector re-checks the fence immediately before its mark, so a
  read or write that started first is seen. A node keeps one reader lease and
  reuses it, renewing it at the start of each read: a fresh record per statement
  would put two Raft entries in the log for every `SELECT`, and the live record
  already blocks a pass. Reuse also means a node stops blocking collection one
  TTL after its last read, so a pass needs about 30 s without reads on each node
  rather than a microsecond gap.
  Admission and collection are mutually exclusive *by transaction*, not by a
  read-then-write sequence: publishing a reader or writer record guards two keys
  at once (the record's own never-reused id slot, and the collector record being
  absent), so a lease either exists before the pass checks the fence or loses its
  CAS. Every record carries the `mod_revision` it was published at as an
  ownership token: renewal, release and drop are CAS-guarded by it, so a holder
  that lost its record cannot extend it, delete it, or delete the record of the
  pass that took over. A pass renews its own collector record before the
  watermark, before the first deletion and before each pool, and stops deleting
  if a renewal loses. A write joins its lease's guard to the transaction that
  publishes the map or manifest, so a writer that stalled past its lease is
  refused instead of publishing fragments a pass has already reclaimed; a read
  verifies its lease after fetching and answers `503` if it did not survive the
  read, because a pass may have reaped the expired record and reclaimed what was
  just read. The pass raises a durable
  `/gc/watermark` to the revision of its mark before it re-checks the fence; a
  reader publishes its lease first and then refuses with `503` if its applied
  revision is behind that watermark, which is the case a lease alone cannot
  cover — a lagging follower can still hold a manifest whose fragments a
  completed pass already reclaimed. Because only the leader can publish a lease,
  a legacy follower no longer serves a read locally: it proxies the request (with
  its `Range` header) to the leader, which is also what keeps default reads
  working instead of failing every statement on a follower. The native S3 wire
  listener does the same for GET after it has verified SigV4, forwarding the
  proven subject through the internal gateway and re-framing a gateway error as
  an S3 XML error; HEAD needs no fence and stays local. A range end past the
  object is clamped to its size (RFC 9110) on both paths, so a proxied
  `bytes=7-999` answers `206` with the clamped slice instead of `416`. A
  read-only statement whose lease was lost is refused with `503`, but a statement
  that committed a write keeps its result: the mutation is already durable, and
  reporting it as failed would make the client retry a write that landed. The
  post-commit auto-flush is best-effort for the same reason. The legacy forwarder
  retries only a definite `409 not Raft leader`; a transport error is retried
  only for `GET`/`HEAD`. `/s3-retired/` is
  deliberately not live: a retired manifest is unreachable to reads, so its
  chunks are exactly what an overwrite or delete made garbage (the records
  themselves are kept).
  Measured in the daemon suite (bytes on disk, before → after): repeated block
  overwrite 9,513,240 → 198,339 (live set 196,608, 2,256 fragments deleted);
  table flush plus compaction 228,765 → 42,369 (live 20,739, 651 fragments);
  S3 overwrite and delete 789,744 → 267 (live 21, 96 fragments); a refused write
  24,798 → 12,534 against a 12,411-byte live-set floor. On a live three-node
  cluster, eight overwrites of one 8 KB block region left 963,124 bytes of pool
  data against a 897,060-byte floor; one pass deleted 42 fragments and reclaimed
  173,418 bytes (963,124 → 905,359), and the region still read back
  byte-identical, while a pass issued straight after a read refused with
  `1 live lease(s) (1 reader, 0 writer)` and freed nothing. A second live
  three-node run (ports 22000-22300) confirmed the proxied read path: SQL, S3 and
  block reads sent to a follower answered `200`/`206` with the exact bytes; a
  pass right after those reads refused with `2 live lease(s) (2 reader, 0
  writer)`, the same request 40 s later reclaimed 48 fragments / 198,192 bytes, a
  rerun reclaimed nothing, and after a clean stop/start the 602 SQL rows, the S3
  object and the block were intact. The native S3 wire listener was then
  exercised on the same cluster: GET and HEAD answered on all three nodes, a
  native Range GET through a follower answered `206` with `Content-Range: bytes
  0-4/11`, and a missing key on a follower answered `404` with the S3 XML error
  rather than the gateway's JSON. Two block overwrites plus those reads left one
  live reader lease; the pass refused, and the next pass reclaimed 96 fragments /
  396,483 bytes with the block, the S3 object and the 602 rows still intact.
  `tests/protocols.py --native-clients` passed 17/17. A crash between the
  durable barrier and the compaction is simulated by dropping the volume after
  the barrier records are written: reopening reaps the expired collector lease,
  the rerun finishes, and the newest generation still reads back byte-identical.
  Open: the collector is legacy-only (the Multi-Raft item below still refuses);
  a pass needs each node to be read-idle for one lease TTL, so a continuously
  busy node refuses rather than waits for a quiet window, and there is no
  grace-period scheduling; `/s3-retired/` records are retained indefinitely; the
  300 s writer TTL is the only bound on a crashed writer blocking collection; a
  legacy follower's proxied read is limited by the forwarding wire cap (16 MiB
  of body, the same limit keyed writes already have), so a larger block or object
  read must go to the leader directly; and no test runs a real multi-node pool,
  so remote enumeration, remote barriers and remote compaction are only exercised
  in-process. The admission fence, ownership-guarded renewal/release, the
  publication guard and the proxied read path are covered by unit tests
  (`admission_is_closed_while_a_pass_holds_the_collector_record`,
  `a_lost_lease_cannot_release_or_guard_its_successors_record`,
  `a_superseded_collector_cannot_renew_delete_or_release_the_record`,
  `a_renewed_lease_keeps_its_ownership_and_extends_its_ttl`,
  `a_write_whose_lease_was_reaped_does_not_publish`,
  `proxy_response_preserves_allowlisted_entity_headers_and_rejects_duplicates`,
  `range_end_past_the_size_is_clamped_but_a_start_past_it_is_refused`).
- [x] Bound the metadata Raft log. The legacy Raft log is now checkpointed and
  compacted: `metakv/checkpoint.json` holds the durable state image (atomic
  write: tmp, `sync_data`, rename, `sync_dir`), `raft.wal` is rewritten through
  a tmp file plus rename, and every rewritten WAL begins with a `HardState`
  frame followed by `WalRecord::Base{index, term}` so compaction can never lose
  the term or vote. `RaftCore` carries `base_index`/`base_term` and every log
  access is an offset above the base.
  Entries are dropped only when two facts hold: they are applied and durable in
  a checkpoint, and no peer can still need them. The bound is a monotone
  `compaction_floor = max(floor, min(commit_index, min peer match_index))` that
  is replicated in every `AppendEntries` (`#[serde(default)]`, so an older peer
  keeps working and a missing field means 0). Each node compacts to
  `min(last_applied, floor)`; a node that never heard a floor does not compact.
  Ordering is checkpoint, then WAL rewrite, then the in-memory base, so a crash
  between the two files is reconciled on open: the checkpoint is authoritative,
  entries at or below its applied index are dropped, and the WAL is rewritten to
  match. `Base > 0` without a readable checkpoint, a checkpoint below the WAL
  base, and a disagreement between the checkpoint term and the base term are all
  explicit startup errors, never a silent re-initialisation. A failed checkpoint
  or rewrite does not fail the mutation that triggered it (its entries are
  already durable); it records `compaction_error`, refuses every later operation
  with `CompactionFailed` (503), and surfaces the reason on `/raft/status`.
  Compaction triggers at 8 MiB of WAL or 4096 applied entries above the base
  (`WAL_COMPACTION_BYTES`/`WAL_COMPACTION_ENTRIES`); `/raft/status` reports
  `base_index`, `compaction_floor`, `wal_bytes` and `compaction_error`.
  Measured on a three-node legacy cluster (ports 21000-21300, 4801 rows through
  pgwire, 19491 Raft entries): `base_index` advanced to 18506, each node's
  `raft.wal` stayed inside a 3.7-8.4 MB band instead of growing with history
  (about 3.8 KB per entry, so the append-only file would have been tens of
  megabytes), and the checkpoint settled at 56 KB. A `stop`/`start` cycle
  recovered every row with no WAL growth, and a node killed with `SIGKILL`
  caught up from the leader's retained suffix.
  Open: there is no install-snapshot path, so a peer that falls below the
  compacted base is refused with `PeerBehindCompactedBase` and adding or
  re-seeding a node needs that protocol first; the checkpoint is a full image,
  not an incremental history, so its size follows live metadata (small here:
  SQL rows live in pool fragments, not in the metadata image); compaction runs
  under the Raft mutex, so one pass briefly stalls proposals; and a single-node
  cluster compacts on its own applied index without a peer bound.
- [ ] Garbage collection on Multi-Raft. `POST /v1/pools/gc` and the sweep inside
  `POST /v1/pools/scrub` both refuse with `garbage collection disabled for
  Multi-Raft: no single atomic cross-group metadata snapshot` and delete
  nothing. The live set is group-scoped while deletion is pool-scoped, and one
  node cannot enumerate every group's manifests, so a Multi-Raft collector needs
  a per-group fence, a per-group barrier it can prove, and a rule for a fragment
  referenced by another group's manifest. None of that exists; a Multi-Raft
  deployment still reclaims nothing.

Backup and restore are **out of scope by owner decision**, not an oversight: no
command, artifact format, fence or validator exists, and none is planned. A lost
data root is a lost deployment. Reintroducing the capability requires its own
design; the removed draft is not kept in the tree.

## 2. Network and security

- [x] Replace the shared-secret envelope with per-node Ed25519 signatures. A
  verifier only holds public keys, so it cannot impersonate another sender.
  The envelope binds group, incarnation, sender, recipient and a per-sender
  monotonic sequence number, and an in-memory replay guard rejects old messages
  until restart; it is not durable replay protection.
  `openraft_transport` covers impersonation, tampering, unknown senders and
  replay.
- [ ] Complete the OpenRaft network factory and listener over TCP with
  length-prefixed frames, request timeouts, connection limits and a payload
  limit. The server verifies the signature against the sender's trusted public
  key and only then dispatches the Raft RPC. Responses are signed and bound to
  a random per-request nonce. Requests bind a fresh server challenge per TCP
  connection, including across server restart; the captured-request test checks
  this. Protocol security still needs adversarial review and lifecycle/resource
  validation; opt-in TLS/mTLS is implemented separately below.
- [ ] Deploy and validate mandatory TLS/mTLS for operational Raft listeners.
  `TransportConfig::with_tls` implements TLS 1.3 with CA/time/hostname validation,
  ALPN, exact leaf SHA-256 pins bound to node IDs and no plaintext fallback.
  TLS resumption/tickets are disabled. In-process three-node replication and
  untrusted/expired/wrong-node/plaintext/hostname/pin rejection tests pass.
  Plaintext mode remains an explicit isolated-experiment API; provisioning,
  deployment and rotation tests are not complete.
- [ ] Define credential provisioning, authorization, rotation, revocation and
  membership changes; enforce message and snapshot size, concurrency and
  timeout limits. Test impersonation, wrong group/node, tampering, replay and
  cancellation.
  `multiraft-security-operations.md` defines the proposed manual offline trust
  procedure and incident response. Dynamic reload, rolling rotation, revocation
  and membership integration are not implemented or validated deployment gates.
- [x] Protect the legacy internal endpoints: `/raft/rpc` and `/raft/elect`
  now require the shared `--raft-token` in clustered mode and the daemon
  refuses to start without it. Client subjects are only trusted with a valid
  `x-yadb-subject-signature` when `--subject-secret` is configured, and the
  signature is forwarded to the leader.
- [x] Authorize control-plane mutations. `/v1/apply` now requires a subject
  proven with `x-yadb-subject-signature` (HMAC-SHA256 over the subject with
  `--subject-secret`) that appears on the `--admin-subject` allowlist. Unsigned
  or forged subjects get 401, a proven non-admin subject gets 403, and an
  unconfigured allowlist or missing secret gets 503. In OpenRaft mode an empty
  allowlist refuses startup, and an allowlist without a secret refuses startup in
  every mode. The check runs before forwarding, and the signature is forwarded so
  the leader verifies the same identity rather than trusting a plain header.
  `yadbctl` signs its requests when `--subject-secret`/`YADB_SUBJECT_SECRET` is
  set. The browser UI cannot hold the secret, so its apply button now fails with
  the daemon's 401 rather than acting as an anonymous administrator.
- [ ] Replace the client-supplied subject header entirely with a verified
  credential once the protocol listeners share one identity model.
- [ ] Split the internal Raft listener from the public client listener so the
  Raft port is not reachable from client networks.
- [x] Protect the remote VDisk RPCs. `PUT|GET|DELETE
  /internal/vdisk/{pool}/{slot}/blob/{blob_hex}`, `GET .../stats` and `POST
  .../barrier` are served on the public listener and were unauthenticated: an
  unauthenticated caller could install a permanent exact-ID tombstone or a
  garbage-collection barrier that evicts live fragments and fences future
  writes, repeated per replica slot to defeat erasure redundancy. All five
  handlers now require the Raft peer token (`x-yadb-raft-token`) and the remote
  client sends it; the token is threaded from `--raft-token` through
  `Slot::remote`. The credential is the peer token rather than an admin
  subject, because this is node-to-node traffic and an admin check would break
  every remote slot call. Two residual exposures remain and are documented in
  `multiraft-security-operations.md`: the token is a shared cluster secret, so
  it authenticates the cluster rather than an individual peer; and the routes
  are still reachable by anything that can reach the listener, so only the
  dedicated internal listener plus verified firewall rules actually bound who
  can attempt them. Separating that listener is the item above.

## 3. Runtime and operations

- [x] Implement a durable group owner with validated identity, exclusive
  directory ownership, explicit bootstrap/open and clean shutdown
  (`openraft_runtime::persistent`). A partial bootstrap is never reinitialized
  automatically. Callers must await shutdown to completion; arbitrary owner drop
  and startup cancellation are not proven safe.
  A versioned replica descriptor also binds bootstrap voters; tests reject
  changed voters on reopen and wrong-identity registry lookup/shutdown.
- [x] Report an ambiguous write outcome as unknown on both runtimes. The OpenRaft
  bridge already answered `OpenRaftWriteUnknown` with `503` plus
  `x-yadb-write-outcome: unknown` / `x-yadb-retry: do-not-retry`, but the legacy
  path's `MetaError::UncommittedIndex` — the same ambiguity, raised when the
  synchronous RPC cascade did not confirm the proposal in time or a new leader
  replaced it — fell through to a plain `500`. `meta_err_response` now maps both.
  Definite errors (`CorruptWal`, `Io`) still map to `500` with no outcome header,
  and a test asserts that, so a client keying retry policy off the header does not
  stop retrying ordinary rejections. Open: `drive_outbound_with` propagates
  `Io`/`StorageUnavailable` from `guard.step(reply)` *after* the proposal was
  appended, which is also ambiguous, and those variants are not distinguished from
  a pre-append failure.
- [ ] Implement a multi-group registry: several independent groups per node,
  per-group directories, per-group failure isolation and bounded resources.
  An in-process registry provides separate directories and a
  group-count limit; it does not yet provide resource isolation, robust
  cancellation/shutdown handling or shared network routing. The in-progress daemon
  v2 implementation instead owns multiple groups with dedicated listeners; its
  integration and failure behavior still require validation.
- [ ] Integrate the runtime behind explicit daemon configuration; provide metrics,
  logging, health/readiness, operator commands and recovery/upgrade runbooks.
  The observational v1 daemon module starts one isolated group with strict bounded
  JSON configuration, explicit create/open/bootstrap, mandatory mTLS and a separate
  listener; its create/shutdown/reopen tests pass. In-progress v2 client routing
  uses a manifest and group-specific listeners; singleton-group child-daemon
  integration passed, while multi-node validation, full observability and cancellation
  coverage remain open. Status reports `production_ready: false`.
  Local certificate
  chain/name/time and server/client-purpose preflight runs before storage mutation;
  invalid hostname, untrusted and expired-certificate rejection cases pass without
  replica creation. A historical full locked all-features run passed with 111 MetaKV
  tests and 11 daemon tests; its then-needed rerun is superseded by the current
  full gates above, not by that historical result.
  Daemon shutdown now borrows its owner and retains the listener join handle
  across cancellation; a deterministic cancellation/retry/reopen test passes
  (all three daemon module tests passed). This does not qualify arbitrary owner
  drop or cancellation during startup.
  `multiraft-runtime-operations.md` records ownership/shutdown and fail-closed
  recovery boundaries. It does not establish implemented operational tooling.
- [x] Test replication over a real TCP transport with authenticated envelopes
  and durable file stores (`openraft_net_test`), including rejection of unknown
  addresses and forged signatures.
- [ ] Test node shutdown/restart, partitions, leadership changes, quorum loss,
  delayed/duplicate messages, snapshots and catch-up with independent
  processes, real filesystems and TLS.

## 4. Migration and release

- [ ] Specify legacy-to-OpenRaft migration, fencing, cutover, rollback limits,
  compatibility and versioned formats. Validate with realistic data.
  The draft in `multiraft-migration.md` defines required manifest fields,
  authority states and a recovery matrix. It is not an implemented migration
  tool; external fencing authority and exact state import remain blockers.
- [ ] Preserve SQL, S3, block and iSCSI semantics and client identity; test legacy
  compatibility and cross-group transaction/read semantics before routing traffic.
- [ ] Run offline locked workspace checks and full tests; run security, durability,
  fault and multi-process integration suites on supported production systems.
- [ ] Obtain independent GPT-6.1 Sol review of consensus, storage, transport,
  security, lifecycle and migration; address blocking findings and rerun gates.
- [ ] Only then enable production routing with an explicit rollout and recovery plan.

## Evidence already available (not completed gates)

OpenRaft singleton and three-node tests use in-process transport. File-backed
test stores cover restart, follower catch-up and snapshot transfer; selected
faults are simulated in process. A persistent group owner and storage adapters
are available behind the `openraft-storage` feature and wired to an opt-in observational
daemon group with mandatory mTLS. The multi-group client daemon path has a bounded
singleton-group child test pass and a narrow local deployment, but is
still under hardening; none of this establishes
OS/filesystem/hardware power-loss safety.

`persistent_singleton_process_kill_reopen` acknowledges three OpenRaft writes
in a child process, kills it without graceful shutdown, verifies exact values
and reopens the replica. This proves that tested process-death scenario only:
the operating system remains running and may retain dirty filesystem pages.
It does not close the OS/filesystem/hardware power-loss gate.

`three_process_mtls_failover_and_quorum_loss` runs three independent durable
replicas over loopback mTLS. It checks leader kill/failover, old-peer reopen,
no acknowledgment without quorum, quorum restoration and exact persisted values.
Its latest targeted run also verifies an offline follower installs a snapshot
after log purge (install counter greater than zero). This is not a multi-host,
network-partition or power-loss test; those gates remain distinct.
Historically, a later default-feature workspace run failed this test at post-snapshot
catch-up (111 MetaKV tests passed, one failed). Diagnosis remains active; earlier
green targeted/all-features results do not override this unresolved regression.
One test-only defect was identified: a fixed purge through index >= 6 did not
necessarily exceed the stopped follower's last log index 7. The improved setup
passed 2/2 targeted process tests in 5.92 s, but a true offline-last-log race is
being fixed again. The setup is not final, and this is not a proven root-cause
fix for the original failure.
See `multiraft-verification.md` for the preserved artifacts.

`three_process_mtls_network_partition` adds six directed parent-owned TCP proxies
that forward encrypted traffic without terminating TLS. It cuts active tunnels
and isolates the old leader in both directions, checks no isolated acknowledgment,
majority progress, healing/catch-up and persisted acknowledged values. Both process
tests passed together in a targeted run. This models connection cuts, not delayed
or duplicated RPCs, multi-host deployments or OS/device power loss.

Transport handlers now use owned `JoinSet` tasks and RAII connection permits;
request vote-owner claims must match the authenticated sender. The client bounds
the challenge frame to 32 bytes before allocating its body. A shared 32 MiB encoded
inbound-request budget is reserved before allocating body buffers; this is not a
bound on decoded JSON, responses, TLS or total process memory. `serve_until` stops
accepts and aborts/drains handlers; dropping the future only requests cancellation.
Byte-budget/cancellation/panic-permit and explicit-drain tests passed in the full
locked all-features run (109 MetaKV tests); later changes need another run.
Signed snapshot streaming now uses a signed manifest and at most 256 KiB raw
chunks on one mTLS connection, bound to sender/group/recipient, sequence,
challenge/nonce, ordinal, total length and SHA-256. The transport caps raw snapshot
data at 64 MiB + 24 bytes, manifest payload at 64 KiB, and separately budgets
assembly bytes; legacy inline snapshot reception is rejected. Targeted malformed,
stalled, cancellation and mTLS tests passed. A 17 MiB transport-only round trip
passed with explicit 180-second deadlines and took about 100 seconds in debug.
This does not establish large transfers under the default five-second server
deadline or durable installation capacity. The 64 MiB state-image limit includes
retained snapshot plus live state, and budgets do not bound total process memory.
End-to-end snapshot capacity remains open.

The adapted upstream storage-suite runner is **not** the unmodified upstream
suite and does not close the contract gate. The latest append/purge audit initially
failed in `get_initial_state_membership_from_log_and_sm`: recovery purged through
index 2 before the case appended index 1, contrary to this adapter's readable-on-
return append requirement. The adapted runner now isolates the empty-log assertion
in a fresh store and passes; this is an additional setup substitution, not proof
of equivalence to the unmodified case. The full workspace run passed with it.
The harness reconstructs the key-log-ID view from public storage methods rather
than inspecting OpenRaft's private cache; the recovery test for a state machine
ahead of the log checks the purge side effect rather than private `RaftState`
fields; and `last_applied_state` inserts a blank entry to avoid an apply gap.
These substitutions do not demonstrate equivalent coverage of the private
state assertions. Purged-prefix append behavior and conflicting boundary
identities require explicit contract reconciliation and regression coverage,
not a production-readiness claim.

The current checkpoint-capable log format is `YORLOG05`; `YORLOG02`,
`YORLOG03` and `YORLOG04` are rejected unchanged because corrected overlap replay
rules, checkpoint capabilities and the self-checking frame header must not
silently reinterpret older records. A `YORLOG04` root therefore cannot be opened
by a `YORLOG05` binary at all; the local `cluster_data` deployment keeps its
pre-`YORLOG05` binary for exactly that reason, and adopting the new format there
would require a replay-and-republish migration, not a reinitialisation. `YORLOG05` covers `magic||len||payload_crc`
with its own CRC, which is what lets a length damaged upward be refused instead
of repaired as a torn tail.
No automatic log migration exists. Preserve old log directories for
explicit analysis; never delete/reinitialize them to bypass a version error.

Compatibility warning: legacy clustered daemon deployments now require
`--raft-token` / `YADB_RAFT_TOKEN`; configure matching tokens on every peer
before upgrading, or the daemon will refuse to start. This is a deliberate
fail-closed security change, not transparent rolling-upgrade compatibility.
In OpenRaft mode the daemon also requires a non-empty `--admin-subject`, and any
allowlist requires a non-empty `--subject-secret` / `YADB_SUBJECT_SECRET`.
Without those flags an upgraded daemon refuses to start rather than serving an
unauthenticated `/v1/apply`.

### Upstream storage-suite mapping and residual differences

The adapted runner executes the upstream-named cases on actual file-backed log
and state adapters with `u32` IDs. The table distinguishes unchanged observable
assertions from replacements; neither the runner nor the extra reopen tests are
claimed to reproduce inaccessible upstream internal assertions.

| OpenRaft 0.9.25 suite cases | YADB evidence |
| --- | --- |
| `initial_logs`, parts of `get_log_state`, `purge_logs_upto_0/5/20` | `contract_log_state_and_purge_boundaries_survive_reopen` |
| `delete_logs_since_0/11`, `append_to_log` | `contract_truncate_and_append_after_purge_survive_reopen` |
| `save_vote`, parts of `get_log_entries` | `contract_vote_and_range_reads_survive_reopen` |
| Membership scans and `get_membership_*` | Adapted suite retains observable membership assertions; numeric node-ID construction and blocking append helper differ |
| `get_initial_state_without_init` | Replaces whole private `RaftState` equality with public vote, membership, snapshot and store log/purge observations; private fields remain uncovered |
| `get_initial_state_with_state`, `get_initial_state_last_log_gt_sm` | Checks store log state and state-machine applied state instead of private recovered-state log/commit fields; not equivalent private-cache evidence |
| `get_initial_state_last_log_lt_sm` | Checks recovery's purge side effect and retained suffix rather than private recovered-state last-log value |
| `get_initial_state_membership_from_log_and_sm` | Moves the initial empty-log recovery to a fresh store so that its purge does not precede the later below-boundary append; sequential setup differs |
| `get_initial_state_log_ids` | Reconstructs key log IDs from public reads; does not inspect OpenRaft's internal cache |
| `get_initial_state_re_apply_committed` | Retains applied-state observation but replaces private `io_applied` assertion; the no-op committed-persistence default also limits this case's coverage |
| Limited reads, single-entry/log-ID lookup, last-ID, snapshot metadata, single/multiple apply, snapshot transfer | Cases run in the adapted runner with public blocking append/node-ID helpers; this does not add process-crash or fault-boundary coverage |
| `last_applied_state` | Adds blank index 4 before index 5 to satisfy this adapter's consecutive-apply rule; input differs |

The direct contract tests above add reopen checks but do not close these private-
state and changed-input differences. Full equivalence requires preserving the
original assertions through an appropriate upstream harness or proving and testing
each replacement independently; a green adapted runner alone is insufficient.
