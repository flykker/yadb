# OpenRaft Multi-Raft production checklist

## Historical status snapshot (2026-11-14)

This section is a dated snapshot, not a claim about the current tree. Later
sections record what has changed since.

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

`force_leader` is routing preference, not a linearizable-read guarantee. At the
time of this snapshot the legacy runtime was still the daemon default,
`production_ready: false`, no existing cluster had been switched, old data was
untouched, and no migration had occurred. Do not claim known regressions resolved.

Status: **not production-ready**. At the time of this snapshot the legacy Raft
runtime was still the daemon default; the v2 client integration was still being
hardened. A local
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
is no longer empty (see the status snapshot above), and no existing cluster has been
switched. See `docs/multiraft-verification.md`.

- [x] Add explicit daemon metadata-runtime selection. **Superseded:** the legacy
  runtime was removed instead of kept as the default; `--metadata-runtime`
  accepts only `openraft`, and `--openraft-config` with a version-2 multi-group
  manifest is mandatory.
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
  - [x] Distinguish "not sent" from "outcome unknown" on the legacy forwarding
    path. `proxy_http_to_peer` returned the same `io::Error` for a failed
    connect (the leader provably never saw the request) and for a lost reply
    after the request left the node, and the caller reported both as
    `unknown`/`do-not-retry`. Under a 24-way concurrent PUT burst through a
    follower, four writes were refused with a spurious `503` even though
    nothing had been delivered. The helper now returns a `ForwardFailure` that
    records whether any byte was written: connect/setup failures are
    `not-sent` and are retried inside the existing attempt loop (a mutation
    that never left the node is safe to send again), while failures after the
    first write stay `unknown`/`do-not-retry`. A final refusal after only
    `not-sent` failures now reports `not-sent` instead of an empty outcome.
    Two unit tests pin both branches. Open: a partially written request still
    counts as sent, which is the conservative choice; the header/body split is
    not transactional.
  - [x] Stop a Leader from reporting a committed concurrent proposal as
    unknown. `RaftCore::propose` returns the replication messages it built, so
    when several threads propose on one Leader a later proposer's messages
    already cover an earlier proposer's entry, and that earlier `propose`
    returns none. `execute_with` sent nothing, found no applied result and
    answered `UncommittedIndex`; four of twenty-four concurrent S3 PUTs through
    a follower were refused as `no confirmed result for proposal at log index N`
    although they committed. `execute_with` now waits for its own
    `(index, term)`: re-check the result, issue a heartbeat round so a peer that
    already holds the entry acknowledges it, and stop on apply, term change, the
    3 s `COMMIT_WAIT` budget, or three consecutive rounds that reach no peer.
    The wait cannot duplicate the command (the entry is already in the log) and
    a unit test injects a fully timed-out first round and still gets `PutOk`.
  - [x] Size the Follower's forwarding reply budget for real load instead of
    guessing. One fixed 5 s `read_timeout` covered every forwarded request, but
    one SQL statement costs ~130 ms and one S3 PUT ~160 ms locally (the engine
    serializes statements behind one lock; every metadata mutation is a
    synchronous two-peer Raft round), so a 24-way burst needs 4–5 s and the
    budget intermittently expired on writes that were committing. Writes now get
    `FORWARD_WRITE_REPLY_BUDGET` (20 s) and reads keep
    `FORWARD_READ_REPLY_BUDGET` (5 s). This is still a client-visible budget,
    not a retry: expiry reports `unknown`/`do-not-retry` and nothing is resent.
    Open: one Leader's throughput stays bounded by the per-statement lock and
    the per-mutation Raft round; 20 s covers the tested 24-way burst, not an
    unbounded queue.
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
independent Sol review were pending; see the status snapshot above.

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
independent Sol review were then pending; see the status snapshot above. Neither this
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
  - [x] Serve the S3 `?consistent=true` read through the gateway on the native
    listener. The listener used to reject the parameter as an unsupported
    subresource while a plain read answered from local applied state, so a
    client asking for the quorum-confirmed read got `501` from its own listener
    and a client that omitted it could get a `404` for an object just
    acknowledged through another node. The listener now delegates to
    `handle_s3_get` (legacy metadata still refuses with `501`), and the
    delegated read keeps the client's `Range`/`If-Match`/`If-None-Match` lines.
    Open: a full consistent read of an object above the 16 MiB cap is refused;
    use `Range`.
- [x] Multipart S3 uploads. `POST ?uploads`, `PUT ?partNumber&uploadId`,
  `GET ?uploadId` (ListParts), `POST ?uploadId` (Complete) and
  `DELETE ?uploadId` (Abort) are implemented. Upload state is a
  `multipart: true` intent committed in MetaKV **before** any part fragment is
  written; each part is an ordinary durable `v2-` allocation whose intent is
  committed first, so parts survive a restart and a leader change and the whole
  object is never buffered. Parts are verified with the same
  `Content-MD5`/CRC32/SHA-256 rule as a single PUT, and completion is one
  `RevisionTxn` guarded on the upload record, every part intent and the manifest
  key: it publishes the new manifest, retires the previous one and deletes the
  record and part intents, or answers `503` with `x-yadb-retry: do-not-retry`
  when a guard is lost. `InvalidPart`, `InvalidPartOrder`, `MalformedXML` and
  `NoSuchUpload` are refused without consuming the upload. Verified live on the
  three-process cluster with the AWS CLI: a 40 MiB object in five 8 MiB parts
  through each of the three listeners (two followers, so forwarded) produced the
  same multipart ETag and byte-identical downloads, and a `?consistent=true`
  range read after a full restart returned the same bytes. The same run also
  covers the multipart fixes: `ListParts` pagination (`part-number-marker`,
  `max-parts` 1..=1000 with an out-of-range page refused as `InvalidArgument`),
  a Complete `Location` that names the object and no longer leaks the
  `uploadId`, and an initiation that carries the client's `Content-Type`.
  An option the object store cannot honour (`x-amz-meta-*`, encryption, ACL) is
  refused at initiate on **both** wire formats: the gateway checks its own
  headers, and the native listener checks the raw client headers before any
  durable upload record exists, because it forwards only a few of them and the
  gateway would otherwise never see the metadata the client asked to store.
  Note that a plain `ListParts`/GET on a follower answers from that node's
  applied state, so a part acknowledged a moment earlier through another node
  can still be missing there; `?consistent=true` is the opt-in that reads the
  project-group leader's snapshot. Open: a part is
  capped at 16 MiB (not 5 GiB), there is no `ListMultipartUploads` and no
  `UploadPartCopy`, and orphan collection stays disabled on Multi-Raft, so
  aborted or expired upload fragments are not reclaimed there.
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
  pass that took over. Ownership is not the only condition: a record whose
  `expires_at_ms` has passed is refused by `ensure_live` and by every renewal
  path, even when it is still present and still carries the holder's token,
  because the collector is entitled to treat it as dead at that moment and has
  not necessarily reaped it yet. A pass renews its own collector record before the
  watermark, before the first deletion and before each pool, re-checks it while a
  pool deletes — before the first deletion, before every barrier group and every
  32 exact tombstones — and stops deleting if a check loses. Renewal itself refuses
  in both directions: the record must be live when checked *and* the write must be
  acknowledged before the deadline it extends, because a compare-and-swap proves
  ownership but not liveness, and a record that expired during the round trip was
  otherwise resurrected. The write is left in place when the acknowledgement is
  late, so admission stays refused until it lapses. A write joins its
  lease's guard to the transaction that
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
  an S3 XML error; HEAD needs no fence and stays local. The native read keeps
  its local contract across that hop, because the node that reads the manifest
  decides it: existence, `If-Match`/`If-None-Match`, the range and the 16 MiB
  rule are all evaluated on the leader against the one manifest whose fragments
  are then fetched, so a follower's lagging applied state can never refuse or
  mis-slice a read the leader can serve. The follower forwards `Range`,
  `If-Match` and `If-None-Match`; a failed precondition is `412`, a matching
  `If-None-Match` is a bodyless `304`, an unsatisfiable range is `416
  InvalidRange` with `Content-Range: bytes */size`, a read over 16 MiB is `400
  InvalidRequest` before any fragment is read, `Last-Modified` and
  `Accept-Ranges` come from that manifest, and the internal hop header is
  stripped. The gateway `/s3/...` GET shares the same decision (it also answers
  conditional requests now) and reads one manifest per request. A range end past
  the object is clamped to its size (RFC 9110) on both paths, so a proxied
  `bytes=7-999` answers `206` with the clamped slice instead of `416`. A
  read-only statement whose lease was lost is refused with `503`, but a statement
  that committed a write keeps its result: the mutation is already durable, and
  reporting it as failed would make the client retry a write that landed. A
  statement refused before it reads anything (a lost transaction answers `409`)
  takes no reader lease, so retrying an invalid statement cannot park leases. The
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
  rather than the gateway's JSON. A third run on the same cluster checked the
  follower contract: the same `200` body, `ETag`, `Last-Modified` and
  `Accept-Ranges: bytes` as the leader and no `x-yadb-leader-id`; a failed
  `If-Match` answered `412`, a matching `If-None-Match` answered a bodyless
  `304`, and `Range: bytes=900-999` on the 11-byte object answered `416
  InvalidRange` with `Content-Range: bytes */11` on both nodes; the gateway
  `/s3/...` path answered the same three statuses. A fourth run checked RFC 9110
  matching on the leader, a follower and the gateway: `If-None-Match: *`,
  `W/"<etag>"` and `"old", "<etag>"` all answered a bodyless `304`, a
  non-matching `If-None-Match` answered `200`, `If-Match: *` and a list
  containing the version answered `200`, and a weak or non-matching `If-Match`
  answered `412`; a pass right after those reads refused on one live reader
  lease. A fifth run checked the list and field-line edges on the leader, a
  follower and the gateway: a trailing or doubled comma kept the rest of the
  condition (`If-Match: "other",` and `If-Match: , "other"` answered `412`,
  `If-None-Match: "<etag>",` and `If-None-Match: "other", , "<etag>"` answered
  `304`), an unparsable `If-Match` answered `412` while an unparsable
  `If-None-Match` answered `200`, and two separate field lines were honoured when
  either matched. A sixth run checked the combined-value policy the forwarding
  hop shares: a well-formed line plus `junk` in one header answered `412` for
  `If-Match` (including `*` plus `junk`) and `200` for `If-None-Match`, the same
  on the leader, a follower and the gateway. Three `require_transaction`
  queries with an unknown session answered `409` and left `leases_live: 0`. Two block
  overwrites plus those reads left one
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
  read must go to the leader directly — the gateway `GET /s3/...` path now
  enforces the same 16 MiB rule (it had no limit before, a deliberate change);
  native `HEAD` on a legacy follower stays local and can return a stale `ETag`,
  size or `Last-Modified`, because it reads no fragments and takes no lease; the
  forwarded `Range`/`If-Match`/`If-None-Match` values are bounded only by the
  HTTP parser's header limits (a value that could add a header line cannot be a
  `HeaderValue`), and repeated conditional field lines are combined into one
  value that both the local evaluator and the forwarded request read, forwarded
  byte for byte, so the leader cannot interpret the header differently; and no
  test runs a real multi-node pool,
  so remote enumeration, remote barriers and remote compaction are only exercised
  in-process. The admission fence, ownership-guarded renewal/release, the
  publication guard and the proxied read path are covered by unit tests
  (`admission_is_closed_while_a_pass_holds_the_collector_record`,
  `a_lost_lease_cannot_release_or_guard_its_successors_record`,
  `a_superseded_collector_cannot_renew_delete_or_release_the_record`,
  `a_renewed_lease_keeps_its_ownership_and_extends_its_ttl`,
  `a_write_whose_lease_was_reaped_does_not_publish`,
  `proxy_response_preserves_allowlisted_entity_headers_and_rejects_duplicates`,
  `range_end_past_the_size_is_clamped_but_a_start_past_it_is_refused`,
  `a_read_applies_the_local_conditional_contract`,
  `a_read_applies_the_local_range_and_size_rules`,
  `proxy_response_accepts_a_bodyless_304_without_content_length`,
  `conditional_reads_follow_rfc_9110_matching`,
  `a_forwarded_read_carries_its_range_and_conditions`,
  `a_forwarded_condition_keeps_every_field_line`,
  `a_forwarded_condition_is_evaluated_like_the_local_one`,
  `gateway_read_enforces_the_16_mib_cap`,
  `map_error_frames_gateway_range_errors_as_s3_errors`,
  `map_error_frames_gateway_bad_requests_as_invalid_request`).
- [x] Bound the metadata Raft log. **Historical: this describes the legacy runtime,
  which has since been removed** (`crates/yadb-metakv/src/raft.rs:4-21`). Nothing
  writes `metakv/checkpoint.json` any more, and the single-node `LocalKv` refuses a
  WAL that starts from a compacted base because it has no image to restore; the
  equivalent capability now lives in the OpenRaft storage adapters as the
  `state.json` image plus the delta log, tracked under "Bound memory, disk growth,
  startup replay and snapshot reception" above. The text below is kept as the
  record of what the removed runtime did, not as a claim about `main`.
  The legacy Raft log is now checkpointed and
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
  A peer that falls behind the compacted base is re-seeded with the same
  image: `InstallSnapshot` carries the `checkpoint.json` bytes in 64 KiB
  chunks over the authenticated Raft RPC, bounded at 64 MiB and resumable per
  (group, leader, snapshot id) through `metakv/snapshot.partial` (discarded on
  open). Every chunk repeats the group id (CRC32 over the sorted voters), the
  membership, the last included index/term, the total length and the content id
  `(crc32 << 32) | len`, so a receiver refuses a wrong group, wrong membership,
  stale index, conflicting term, gap, out-of-range offset or bad whole-image
  checksum before touching its data and keeps serving what it had. The
  assembled image is synced, checksummed, decoded and cross-checked, published
  with one atomic rename plus `sync_dir`, then the WAL is rewritten to the new
  base, and only then do the in-memory applied/base pointers move; nothing is
  acknowledged before both files are durable, and a crash leaves either the old
  or the new base (the checkpoint is authoritative on open). The rejoin then
  continues as an ordinary log follower, with no second catch-up path.
  `checkpoint.json` is version 2 (adds the membership); a version-1 image is
  still read, an unknown version is refused. The leader no longer answers a
  behind peer with `PeerBehindCompactedBase`; that error now means only the
  follower-side refusal of an append whose prefix is below its own base (and the
  impossible case where nothing has been compacted, so no image exists).
  Open: the checkpoint is a full image, not an incremental history, so its size
  follows live metadata (small here: SQL rows live in pool fragments, not in the
  metadata image); compaction runs under the Raft mutex, so one pass briefly
  stalls proposals; a single-node cluster compacts on its own applied index
  without a peer bound; and a failed install retries from offset 0 on the next
  heartbeat rather than keeping a per-snapshot failure state.
- [ ] Garbage collection on Multi-Raft. A per-group, leader-only,
  quorum-confirmed pass was implemented (each group with its own collector lease
  `/gc/g<group>/collector` and writer leases `/gc/g<group>/writer/<id>`) and is
  now **disabled** (fail-closed). `POST /v1/pools/gc` and the sweep inside
  `POST /v1/pools/scrub` return `409` with `MULTIRAFT_GC_REFUSAL`, delete
  nothing, reclaim nothing and leave fragments intact; the periodic task logs
  the same reason. The fence is not safe: a writer lease can be renewed while a
  collector pass is in flight, and collector RPCs can continue past lease
  expiry, so a reader lease alone is not a sufficient fence. Storage-side
  fencing exists end to end on the legacy runtime. A barrier carries the
  collector lease deadline and the target refuses an expired one with `409`
  (`BlobError::BarrierLeaseExpired`), and an unreadable target clock refuses
  with `BarrierClockUnreadable` instead of defaulting to a live deadline. A
  volume keeps a durable, monotonic epoch and stamps every fragment with the
  epoch it was written under, so `delete_fenced(id, epoch)` refuses to unlink a
  fragment from another epoch. A volume also keeps a durable pin table, so
  `advance_epoch` refuses while any pin is unexpired (`EpochPinned`) and
  `delete_collectable(id, epoch)` refuses a fragment that is not older
  (`FragmentNotOlder`) or that any live pin protects (`FragmentPinned`; the rule
  is "any live pin", because only the current epoch can be pinned). Those
  primitives are reachable over `/internal/vdisk/{pool}/{slot}/...` (epoch,
  pins, pin, unpin, advance, and a delete carrying `fence_epoch` or
  `collector_epoch`), and `PoolStriper` uses them: the read path pins a slot
  before every fragment fetch and fails closed when the pin is refused, and the
  legacy `sweep_pool` advances every slot of a pool before its first deletion
  and deletes through `delete_collectable`. What is still missing is the
  Multi-Raft side: `sweep` returns the refusal before reaching any of it, no
  per-group pass exists, no route verifies the signed fence token (the routes
  authenticate with the shared internal Raft token), and the four cluster drills
  have not run. Collection therefore stays refused, and garbage accumulates and
  can fill the disk;
  this is a knowingly accepted temporary measure. The repair (orphan) scan is
  also still refused, because it needs one whole-deployment view.

  The per-group design that was implemented and then disabled, from the
  read-only analysis of the primitives:

  * **Cross-group references are impossible, so the mark can stay group-scoped.**
    Every fragment id carries its `project_id`, and a manifest only names
    fragments of its own project; a project maps to exactly one data group.
    A pass that marks one group therefore cannot orphan another group's
    fragments. The existing refusal is about *atomicity*, not about a shared
    fragment namespace.
  * **Enumeration is already available, atomicity is not.** `scan("/db/")`,
    `scan("/s3/")` and `scan("/block/")` fan out over every data group from
    local applied state (`openraft_client.rs`), so a mark over all groups is
    possible on any node — but it is a set of per-group snapshots at different
    points in time, which is exactly what the legacy fence is meant to exclude.
  * **The missing fence is per group, and only its leader can provide it.**
    A pass must be leader of group G and take a linearizable snapshot of G
    (`try_linearizable_snapshot` / `ensure_linearizable`) before marking G's
    manifests; a follower's applied state can lag a committed manifest and
    would delete a fragment that manifest still references. The collector lease
    (`/gc-lease/collector`) was one control-group key and had to become
    per-group.
  * **In-flight writes need a fence the legacy path gets from a lease.**
    `acquire_writer_if_legacy` returned `None` on OpenRaft, so SQL tablet writes
    and S3 puts had no writer fence there. S3 is covered by the `Prepared`
    intent committed before its fragments plus the intent lease, and the
    collector must treat an unexpired intent as live. SQL has no equivalent
    intent record, so a per-group writer lease (or an intent committed before
    the fragment) is a prerequisite, not an optimization. **This prerequisite is
    now met for the write path:** the function keeps its name but always
    publishes a writer lease (`acquire_writer(...).map(Some)`,
    `crates/yadb-metakv/src/gc.rs:790`), and the SQL and block write paths call
    it unconditionally (`crates/yadb-db/src/lib.rs:844`, the flush and merge
    paths at `crates/yadb-db/src/tablet.rs:916` and
    `crates/yadb-db/src/tablet.rs:982`, and `crates/yadb-block/src/lib.rs:311`);
    S3 keeps using its `Prepared` intent. What is still missing is the collector
    side: the per-group sweep that would honour those leases is unreachable while
    `sweep` refuses.
  * **Order of work:** per-group collector lease and writer fence → leader-only
    per-group sweep with the intent grace → a drill that kills the group leader
    between fragment write and manifest commit and proves nothing is reclaimed.

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
  required the shared `--raft-token` in clustered mode and the daemon refused
  to start without it. **Historical: that runtime has since been removed and
  neither route exists in `crates/yadbd/src/main.rs`.** Client subjects are
  still only trusted with a valid `x-yadb-subject-signature` when
  `--subject-secret` is configured, and the signature is forwarded to the
  leader.
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
  /internal/vdisk/{pool}/{slot}/blob/{blob_hex}`, `GET .../stats`, `POST
  .../barrier`, `GET .../blobs` and `POST .../compact` are served on the public
  listener and were unauthenticated: an unauthenticated caller could install a
  permanent exact-ID tombstone or a garbage-collection barrier that evicts live
  fragments and fences future writes, repeated per replica slot to defeat
  erasure redundancy, and could enumerate a slot's live fragment ids or force a
  compaction. All seven handlers now require the Raft peer token
  (`x-yadb-raft-token`) and the remote client sends it; the token is threaded
  from `--raft-token` through `Slot::remote`. The credential is the peer token
  rather than an admin subject, because this is node-to-node traffic and an
  admin check would break every remote slot call. Two residual exposures remain
  and are documented in `multiraft-security-operations.md`: the token is a
  shared cluster secret, so it authenticates the cluster rather than an
  individual peer; and the routes are still reachable by anything that can
  reach the listener, so only the dedicated internal listener plus verified
  firewall rules actually bound who can attempt them. Separating that listener
  is the item above.

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
- [x] Answer "not ready" while no control image has been applied. A restarted
  node binds its listeners before its applied metadata necessarily contains the
  control image, so authorization could run against an empty `ControlPlaneState`:
  every subject looked like a stranger and S3 clients received `403 AccessDenied`,
  which they do not retry. The shared authorization snapshot now tells `Stale`
  from `NotReady` (`generation == 0`) and maps the latter to `503` / `57P03` /
  iSCSI target error. `/v1/apply` and `/v1/status` are not gated, and a node with
  an installed image keeps the previous `403`/`404` mapping. Evidence: 63 S3 GETs
  during a node restart produced 27 pre-bind refusals, 5 readiness `503`s and 31
  `200`s with no `403`; 170 pgwire attempts produced 2 `57P03`s and 133 logins
  with no `42501`; and 60 concurrent 1 MiB PUTs produced 30 `200`s whose bytes
  were identical and 30 absent keys, including after collection.
- [ ] Implement a multi-group registry: several independent groups per node,
  per-group directories, per-group failure isolation and bounded resources.
  An in-process registry provides separate directories and a
  group-count limit; it does not yet provide resource isolation, robust
  cancellation/shutdown handling or shared network routing. The daemon now
  always starts a version-2 multi-group configuration with one dedicated mTLS
  listener per group; its integration and failure behavior still require
  validation.
- [ ] Integrate the runtime behind explicit daemon configuration; provide metrics,
  logging, health/readiness, operator commands and recovery/upgrade runbooks.
  Each group starts from strict bounded JSON configuration, explicit
  create/open/bootstrap, mandatory mTLS and a separate listener; its
  create/shutdown/reopen tests pass. The daemon now requires a version-2
  multi-group manifest and uses group-specific listeners. The singleton-group
  child-daemon integration passed, while multi-node validation, full
  observability and cancellation coverage remain open. Status reports
  `production_ready: false`. Local certificate chain/name/time and
  server/client-purpose preflight runs before storage mutation; invalid
  hostname, untrusted and expired-certificate rejection cases pass without
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
- [x] Make an unavailable metadata read an answer instead of a panic.
  `LocalMetaKv` now exposes `try_get`, `try_scan_prefix`, `try_revision` and
  `try_snapshot`; each returns the `MetaError` instead of a default, so "cannot
  be read right now" is never reported as "absent". The panicking forms are
  renamed `get_or_panic`, `scan_prefix_or_panic`, `revision_or_panic` and
  `snapshot_or_panic`, are implemented on top of the fallible forms, and are
  used only by tests. Every production read in `yadb-db`, `yadb-s3`,
  `yadb-block`, `yadb-metakv` and `yadbd` is converted: SQL and S3 reads
  propagate the error, the GC pass refuses, the scrub/repair pass refuses
  instead of building a partial live set, and `handle_health` answers `503`.
  `meta_err_response` maps `MetaError::StorageUnavailable` - the "this state
  machine needs recovery" error - to the existing `503` not-ready answer, so
  the SQL, S3 and block/iSCSI handlers surface it through `DbError::Meta`,
  `S3Error::Meta` and `BlockError::Meta` without a new mapping per protocol.
  The inversion test
  `unreadable_state_machine_errors_instead_of_reporting_absence` poisons a real
  OpenRaft-backed state machine and asserts that `try_get` of a present key and
  of an absent key both return `Err(StorageUnavailable)` rather than a value,
  `None` or a panic; `unavailable_metadata_read_is_not_ready_503` pins the
  handler mapping. The request path is not driven end to end from a poisoned
  state machine: the test-only poison hook is not exported across the crate
  boundary, so the error and its mapping are proven in adjacent tests rather
  than in one HTTP test. `watch_prefix` still validates its scope with an
  `expect`; it is a subscription-scope check, not a data read.
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
are always compiled and are wired to the daemon's version-2 multi-group
configuration with mandatory mTLS. The multi-group client daemon path has a
bounded singleton-group child test pass and a narrow local deployment, but is
still under hardening; none of this establishes OS/filesystem/hardware
power-loss safety.

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
