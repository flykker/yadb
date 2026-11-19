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
full gates above. At that snapshot, S3 GC was disabled. OpenRaft `consistent=true` block reads are
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
- [x] Select the independent group leader for forwarding and protocol transaction
  fencing; do not use the control group's leader/term for every project.
  Forwarding, the forwarded-request guard and the PostgreSQL-wire transaction
  term resolve `role_info_for_key("/db/{id}/")`; reader leases resolve
  `gc_group_for_project` and carry that group's applied index; per-group GC uses
  `role_info_for_group`. The control group's `role_info()` remains only for
  control-plane and unkeyed traffic, health/status reporting and the legacy
  runtime, where a single namespace really is what is being asked about. The
  audit found no site that used the control group where the project group was
  right, so this is proof rather than a fix.
  `forwarding_and_fencing_follow_the_project_group_not_the_control_group` pins
  it in-process with a mesh fixture that separates the leaders: the control
  group is led by node 2 while group 2 is led by node 1 and later node 3. A
  control-group change alone neither redirects nor fences a project write, a
  project write this node leads runs locally although the node is a control
  follower, a group-2 change redirects the write to node 3 with
  `x-yadb-leader-id: 3` while control role, term and leader are unchanged, a
  forwarded copy reaching a non-leader is refused naming that group's leader,
  and the reader lease lands in group 2 carrying group 2's applied index with no
  record in group 3. The two keyed routing sites were inverted and the test
  fails at each: forcing either to `role_info()` makes the project write address
  the control leader (`left: "2"`, `right: "3"`) or run locally when it must be
  forwarded. Open: the live multi-process/multi-host drill, which is a separate
  gate, and the fake remote leader only answers the forwarding HTTP, so the test
  proves addressing and refusal rather than a committed write on node 3.
- [x] Reject transactions spanning groups before submission. Never synthesize a
  global revision or an atomic cross-group snapshot. Use project-group revisions
  for S3 leases/GC; administrative merged scans are non-atomic local observations.
  `GroupRouter::group_for` routes every key before any group is used, so a
  spanning scope fails with `Routing` and no Raft core sees it. The test does not
  rely on the error alone: it snapshots every leader's
  `last_log_index`/`last_applied` and asserts they do not move for a rejected
  `Txn`, `RevisionTxn` or snapshot, on both singleton groups and live three-replica
  groups, because an accepted write appends a log entry even if it never applies.
  `try_revision` refuses and `legacy_revision()` is `0` on OpenRaft, so a lease
  carries the group's own revision, is published under
  `gc::group_lease_prefix(group)` and is fenced by `gc::lease_belongs_to_group`;
  the legacy `live_leases`/`reader_is_safe` namespaces refuse on Multi-Raft rather
  than answering "safe". `try_scan` is a merged local observation whose rows carry
  no revision, so it structurally cannot become a `MetaSnapshot`, and every
  production scan call site is scoped to one project or one group. A group's
  leader is selected independently for the lease and snapshot paths
  (`role_info_for_group`, `try_linearizable_group_snapshot`), so a control-group
  leader is not consulted for a project group's fence. This is in-process; the
  live multi-node drill remains the separate gate.
- [x] Require a new empty data root for creation, bind its runtime/replica identity
  durably, reject mismatched reopen and legacy access to marked OpenRaft roots.
  Do not modify, import or erase old legacy directories; no automatic fallback.
  Create admits only a missing or empty root: a missing root is validated without
  being created, then provisioned as the directory plus one
  `.yadb-openraft-runtime.json` marker (written `create_new`, fsync'd, hardlinked
  from `.pending`, then the root directory is fsync'd). An empty subdirectory, a
  stray file, a half-written `.pending` or a file-as-root is refused without
  touching the root (`create_admits_only_a_missing_or_empty_root`); the empty
  subdirectory is the case a naive `read_dir(root)?.next().is_none()` gets wrong.
  The marker binds cluster, group, incarnation, node, voters and the canonical
  root; a reopen that changes group, incarnation, cluster, node or the voter set
  fails with a marker mismatch and does not rewrite the marker
  (`a_changed_group_incarnation_node_or_voter_set_is_refused_on_reopen`). A
  synthetic pre-OpenRaft tree (WAL, descriptor, state image, empty subdirectory,
  dotfile) is refused on both create and open at the single-group and multi-group
  entry points, and a snapshot of every path, type, mode, mtime and byte is
  unchanged with no marker or `.pending` created
  (`a_legacy_directory_is_refused_and_left_byte_identical`,
  `a_legacy_directory_is_refused_by_the_multi_group_root_guard`); a real daemon
  child refuses a legacy `--data-dir` with the root guard's message, never reaches
  "listening on http", and leaves the tree byte-identical
  (`startup_refuses_a_legacy_data_root_and_leaves_it_byte_identical`). The legacy
  runtime is deleted rather than refused (`--metadata-runtime` accepts only
  `openraft`; `/raft/rpc` and `/raft/elect` return `404`), so no fallback path
  exists. A refused open of a directory with no marker now names the guard
  instead of surfacing a bare `ENOENT`, which is a diagnostic change only. Open:
  marker durability is by construction (fsync + hardlink + parent fsync), not
  proven by crash or power-loss injection; the legacy runtime cannot open a
  marked root because that code no longer exists, which is structural rather than
  tested; and a symlinked root is not tested (a link to a non-empty directory is
  refused, a link to an empty one is admitted and then canonicalized).
- [x] Test actual adapter/engine paths, empty-root admission, persistence/reopen,
  follower write rejection and disabled legacy Raft endpoints in OpenRaft mode.
  `openraft_clients.rs` drives the real SQL, S3 and block engines through
  `LocalMetaKv::from_openraft_groups` and reopens each replica set. Empty-root
  admission is covered by `create_admits_only_a_missing_or_empty_root` and by the
  daemon test that starts from a nonexistent `--data-dir`. Persistence/reopen is
  covered by `multigroup_create_write_reopen_and_marker_binding`, the reopen
  loop in `crates/yadbd/tests/openraft_clients.rs` and the daemon SIGINT/reopen
  read. Follower write
  rejection is covered by `three_daemons_forward_http_to_different_group_leaders`:
  a plain follower write forwards to the project-group leader (`x-yadb-leader-id`),
  and a request that already carries `x-yadb-forwarded` on a non-leader is refused
  with `503`, no `x-yadb-write-outcome: unknown`/`committed` and no
  `x-yadb-retry: do-not-retry`; the refused control mutation leaves every node's
  applied pools unchanged. Disabled legacy endpoints are covered by the same
  daemon test asserting `404` for `GET` and `POST` on `/raft/rpc` and
  `/raft/elect`. Open: there is no direct assertion that the follower's own state
  image never received a forwarded entry - replication makes "local commit"
  indistinguishable there - so the strict no-local-effect assertion is on the
  refused path only.
- [x] Preserve control-plane/RBAC watcher correctness across durable snapshot
  installation: notify installed-state deltas only after successful persistence,
  never on failed installation, and retain subscriptions for later applies.
  The behaviour was already correct — `deliver_notifications` runs only after
  `persist_locked` returns `Ok` — but nothing observed the boundary, so the
  ordering is now pinned: `snapshot_install_notifies_watchers_only_after_durable_publication`
  pauses the install after the temporary image is written and before it is
  synced or renamed, asserts the watcher has received nothing there, then
  releases it and asserts the installed delta arrives;
  `snapshot_install_notifies_delta_only_after_durable_success` and
  `a_failed_install_delivers_nothing_and_poisons_before_any_later_notification`
  pin the failed-install silence, the poisoned handle and the subscription that
  still receives later applies; `snapshot_install_notifies_only_the_values_it_changed`
  pins the diff-only contract, so a replayed snapshot does not look like a
  rewrite of every key. Moving the notification before persistence fails the
  ordering test. In-process fault injection only; power loss is the separate
  filesystem item.
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
assert wall-clock progress: the multigroup independent-leader test, the five
three-process mTLS tests (partition, failover/quorum loss, leader kill,
delayed/duplicated RPCs and power loss after ack), the child-process kill/reopen
test and the two persistent three-node catch-up tests - nine call sites. Each
acquires a process-global `tokio::sync::Mutex`
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

- [x] Run the OpenRaft 0.9.25 storage suite on the actual adapters. Its test
  harness requires `NodeId: From<u64>`, unlike YADB's `u32`, so the harness is
  adapted rather than run unmodified, which is the second branch the item
  allowed: documented equivalent coverage with a case-by-case mapping.
  `scripts/test_openraft_suite_mapping.py` machine-checks all 35 upstream
  `Suite::test_store` cases against the adapted functions, pins the case order
  and set, pins `Cargo.lock` to `openraft 0.9.25`, and records every upstream
  assertion that is not observed verbatim together with the reason. It fails
  when the upstream case set changes, a mapped function disappears, a case stops
  being run, one adapted function stands for two cases, or an unchanged case
  loses an assertion. Two adapted cases were strengthened while building the
  mapping: `get_initial_state_without_init` now asserts the public `committed`
  and `server_state` fields, and `get_initial_state_last_log_lt_sm` asserts the
  public `committed` value instead of only the purge side effect. This is
  documented equivalent coverage, not suite compliance.
- [x] Audit vote, append, truncation, purge, callback ordering, concurrent readers,
  restart and bounds against OpenRaft contracts; test each boundary and replay.
  Every row cites OpenRaft 0.9.25 and is pinned in
  `crates/yadb-metakv/src/openraft_storage.rs` or `openraft_state_machine.rs`.
  `save_vote` ordering (equal, greater and committed accepted; a decreasing term
  or node id refused with the durable image unchanged, then surviving reopen) is
  in `contract_save_vote_is_monotonic_and_durable`. The four truncate boundaries
  — at the last index, above it, at the purge marker and below it — are in
  `contract_truncate_boundaries_are_inclusive_and_reject_the_purged_prefix`, and
  a range read spanning a purge boundary returns exactly the retained entries in
  `contract_range_read_spanning_the_purge_boundary_returns_only_retained_entries`.
  A concurrent reader is parked at the sync/publication boundary and cannot
  observe the log between them in
  `concurrent_reader_is_serialised_against_append_publication`; `get_log_state`
  invariants (`last_purged <= last_log`, purge marker agreeing with the log's own
  view, no entry at or below the marker, no index hole) hold after create, vote,
  append, truncate, purge, automatic checkpoint and reopen in
  `contract_log_state_invariants_hold_across_the_whole_lifecycle`. Callback
  ordering through `blocking_append` is in
  `contract_append_callback_reports_only_persisted_entries` — OpenRaft permits
  the callback before or after `append` returns, and this pins that the entries
  are durable at that moment. The adapter's stricter `MAX_LIVE_ENTRIES` and
  16 MiB frame bounds are refused whole, with the file byte-identical and the
  handle usable, in `contract_entry_count_bound_is_refused_without_truncation_or_poison`
  and `contract_frame_payload_bound_is_refused_before_any_io`; OpenRaft states no
  bound here, so this is a local rule, not an upstream one. The state machine
  refuses a non-consecutive apply batch without publishing or appending to the
  delta (`apply_refuses_a_non_consecutive_batch_without_publishing_or_persisting`);
  OpenRaft only ever supplies the consecutive committed prefix, so that too is
  recorded as a stricter adapter invariant rather than a contract. Two boundaries
  are explicitly *not* attributed to OpenRaft: truncating above the last index is
  accepted as a no-op because the source only requires no hole, and
  `save_committed`/`read_committed` are unimplemented defaults, so restart
  coverage comes from the adapted suite and the reopen assertions instead of a
  direct test. Non-contiguous append, purge identity and purge-marker holes stay
  covered by the existing `append_with_a_hole_is_rejected_without_changing_memory`,
  `overlap_conflicts_and_purge_identity_are_rejected_across_reopen` and
  `purge_boundary_survives_reopen_and_rejects_invalid_mutations`. This is
  in-process contract coverage, not power loss or cross-host qualification, and
  it does not add the separate-process crash/kill gate.
- [ ] Bound memory, disk growth, startup replay and snapshot reception; introduce
  compaction and resource limits without losing acknowledged state. **Partial:
  snapshot reception and the in-flight transport budget are bounded and pinned
  at the wire.** The declared snapshot length is checked against
  `MAX_SNAPSHOT_BYTES` before the assembly reservation and therefore before
  `Vec::with_capacity`, and a length that would truncate in `length as u32` is
  refused the same way
  (`an_over_declared_snapshot_length_is_refused_before_the_assembly_reservation`);
  a chunk carrying more bytes than the manifest declares is refused
  (`a_chunk_carrying_more_bytes_than_the_manifest_declares_is_refused`); one
  shared 64 MiB (`2 * MAX_FRAME`) semaphore bounds the sum of live frame bodies
  across every handler rather than per connection
  (`concurrent_frames_share_one_in_flight_byte_budget`), with a per-source-address
  share of half that budget; an exhausted assembly
  budget blocks the manifest before allocation and dispatch
  (`an_exhausted_assembly_budget_blocks_the_manifest_before_allocation_or_dispatch`);
  a truncated stream never dispatches its valid prefix
  (`a_truncated_snapshot_stream_never_dispatches_its_valid_prefix`); and a
  refused chunk fails the connection closed while the same sender's next
  snapshot succeeds
  (`a_refused_snapshot_chunk_fails_closed_and_the_next_snapshot_succeeds`).
  Still open: disk growth, startup replay, general memory and CPU admission
  across groups, the receiver's on-disk staging area, and end-to-end snapshot
  capacity under the default deadlines. The encoded-frame reservation does not
  bound the transient decoded JSON, and `MAX_PAYLOAD` is verified only after a
  `REQUEST` body of up to `MAX_FRAME` has been buffered.
  The checkpoint implementation is under validation: 32 MiB active-log limit,
  1024 replay frames, 4096 retained live entries and an 8 MiB serialized live
  checkpoint. Replacements sync a new file, rename, sync the directory, then
  switch the append handle; failures poison until validated reopen. Temporary
  replacement can increase peak disk usage, and memory/CPU admission across
  groups, end-to-end snapshot capacity and real failure testing remain open.
  Storage hardening's targeted run passed its then 21 storage tests and the
  adapted suite after rejecting disconnected prepends before I/O. Contiguous prepends
  survive checkpoint/reopen, and overlap comparison uses bounded serialization.
  These results do not clear the separate process catch-up regression.
  The subsequent purge/checkpoint mismatch is fixed: a purge marker leaving a
  gap before the retained suffix is rejected before I/O. The regression covers
  markers immediately before, within and beyond the suffix, conflicting identity,
  unchanged files after rejection and checkpoint/reopen of accepted states.
  All then 22 storage tests and the adapted suite passed after that fix (the
  file now holds 45); broader storage-contract and combined-regression gates
  remain open.
- [x] Ensure file and parent-directory durability for create, rename and reopen,
  including partial bootstrap, directory sync failures and unknown outcomes.
  The barrier order is explicit. `create` creates the directory chain and syncs
  the parent of every directory it created — including intermediates of a
  nested path, whose own entry in *its* parent would otherwise stay unsynced,
  which was a real gap: `root/a/b/c` made `c`'s entry durable in `b` while `b`'s
  entry in `a` and `a`'s entry in `root` were never synced, so a crash could
  leave a durable log unreachable — then writes and syncs the temporary header,
  renames it, syncs the directory that holds it, and only then opens the append
  handle. Checkpoint replacement writes and syncs a new file, renames, syncs the
  directory, then rebinds the handle. Reopen creates no entries, so the
  parent-of-directory barrier belongs to `create`; it re-syncs the log and its
  directory and removes a leftover temporary before returning a handle. A failed
  `sync_data` is an unknown outcome: the handle is refused and a reopen either
  accepts the complete frame and re-syncs it — which can make the failed record
  durable after all — or drops it as a torn tail, so a caller must treat the
  append as possibly applied and never infer from the error that the record is
  gone. A real crash is not observable in-process, so the nested-path test proves
  the ancestor sync is on the path before publication by failing it there;
  supported-host power-loss qualification remains separate. Evidence: the
  then-current 37 storage tests, of which
  `create_propagates_a_parent_directory_sync_failure`,
  `create_syncs_each_new_ancestor_before_publishing_the_log`,
  `bootstrap_faults_publish_nothing_or_a_reopenable_log` and
  `sync_errors_poison_writer_and_reopen_resyncs_visible_record` cover the
  ordering and unknown-outcome claims. Dropping the ancestor sync fails the
  nested-path test; moving the parent sync after publication fails the
  single-level one.
- [x] Make snapshot identity, membership, log pointers, concurrent build/install,
  watchers and recovery correct; explicitly test stale and conflicting snapshots.
  `snapshot_identity_binds_group_membership_and_log_pointer` refuses an image
  built in another group and a metadata field that disagrees with the image
  bytes; `stale_snapshot_install_cannot_roll_back_applied_state` and
  `same_applied_snapshot_requires_identical_state_and_membership` pin the stale
  and conflicting refusals; `concurrent_snapshot_build_and_install_serialise_on_the_inner_lock`
  pauses a build inside `persist_locked` and shows an install on another handle
  cannot finish until the build releases the same `Mutex`, then checks both
  images are whole and that an apply continues at the installed log pointer;
  `snapshot_install_simulated_failures_require_reopen` covers recovery at every
  persistence phase. This is in-process serialisation and fault injection, not
  power loss or cross-host install.
- [x] Inject write, partial write, file sync, rename and directory sync faults for
  every durable log operation — bootstrap create, append for vote, append,
  truncate and purge, checkpoint rewrite and frame rotation, reopen — and assert
  the outcome rather than only the error: the exact bytes left on disk, the
  poisoned handle (reads, log state and further writes all refused), and a
  reopen that either recovers the last durable state or refuses. A failed sync
  keeps the complete frame, because a complete frame is never a torn record; a
  failed or partial write is repaired as a tail only when it is the final frame,
  and a length damaged upward is refused through the self-checking header
  instead of being repaired. Bootstrap failures are classified by what they
  publish: before the rename nothing `open` accepts and `create` is retried,
  after the rename a complete log that `open` accepts and re-syncs. Evidence:
  45 storage tests, of which
  `append_path_faults_poison_and_reopen_to_a_complete_state`,
  `checkpoint_faults_poison_and_recover_valid_primary`,
  `bootstrap_faults_publish_nothing_or_a_reopenable_log` and
  `reopen_faults_refuse_and_a_retry_recovers_the_repaired_log` cover the matrix;
  the expected post-fault image comes from an unfaulted control store rather
  than a hand-copied constant. Removing the poison flag, not poisoning a
  half-published checkpoint, or skipping the durable tail truncation each fail
  these tests.
  In `yadb-metakv` injection is a per-call fault point; `yadb-io` now adds a
  syscall seam for lock acquisition and positional reads (next item).
- [x] Make `yadb-io` lock acquisition and open-time reads injectable and
  validated. `crates/yadb-io/src/sys.rs` is a thread-local, one-shot fault seam
  over the directory lock's `open`/`try_lock`/`unlock` and over positional
  reads; unarmed it is the direct `std` call, so production behaviour and cost
  are unchanged. A failed acquire returns a distinguishable `ErrorKind`
  (`WouldBlock` for a lost race, `PermissionDenied` for an unopenable directory,
  `NotFound` for a missing target, `InvalidInput` for a non-directory or an
  unnormalized path), creates no lock file and leaves the directory usable, and
  an immediate retry succeeds because the fault is one-shot; a held lock refuses
  the second acquirer without disturbing the holder's descriptor. On the read
  side `VirtualFile::read_exact_at` retries a legal short read but refuses an
  early end of file with `UnexpectedEof` instead of accepting a shorter,
  silently valid record, and a mid-read error propagates and leaves the file
  unchanged. The truncation test also shows the hazard it guards: the raw
  `read_at` returns 8 of 16 bytes without complaint. In-process injection proves
  the code path, not the disk.
- [ ] Extend the injected-fault coverage to cross-operation sequences, then pair
  it with the separate-process crash/kill gate. The state machine's own recovery
  read now goes through the `yadb-io` seam: `load_image` opens `state.json`
  through `OsFs` and `read_image` reads it with `VirtualFile::read_exact_at`, so
  a positional-read fault reaches that syscall. The seam separates two failures
  that used to be one. A read that stops before the length the filesystem
  reported is `UnexpectedEof` and is refused, never rebuilt: a fault at the
  syscall says nothing about the bytes, and rebuilding would rename a possibly
  intact image aside. A file that really is shorter than its header or declared
  length is `InvalidData`, exactly as before, and is rebuilt only when the log
  coverage proves replaying from an empty image is lossless. Evidence:
  `truncated_image_is_refused_at_every_offset_and_never_rebuilt` cuts the
  injected read mid-header, at the header boundary, mid-payload and one byte
  short, and asserts `UnexpectedEof`, a byte-identical image and no
  `state.json.corrupt`/`state.json.tmp`;
  `really_truncated_image_is_rebuilt_only_when_coverage_permits` covers a file
  really cut on disk (empty, partial header, mid-payload) and asserts a rebuild
  with the bytes preserved as evidence when coverage is `Some(0)` and
  `InvalidData` with an untouched file when it is not;
  `recovery_read_error_is_refused_and_not_rebuilt` covers a plain I/O error;
  `legal_short_read_is_retried_at_recovery` shows a legal short read is retried.
  The seam is compiled only under the `yadb-io` `test-faults` feature, which
  `yadb-metakv` enables as a dev-dependency, so no shipped build contains it.
  Cross-operation sequences (a fault on the second of two appends, a sync
  failure followed by a read failure) are still not expressible as one per-call
  point, and the separate-process crash/kill gate remains open.
- [ ] Make a crash during `DurableMetaStateMachine::create` recoverable. `create`
  makes `state.json` with `create_new`, drops the handle, then reopens with
  `truncate` and writes; a crash in between leaves a 0-byte image in a root the
  admission check already counts as non-empty (`yadbd/src/openraft.rs` refuses a
  non-empty root), so `create` cannot be retried. `open` refuses it too, because
  a fresh log reports `first_index: None` and `LogCoverage::replays_from_start`
  deliberately rejects an empty log — it cannot be told apart from a replica
  whose state came from a snapshot the log no longer repeats, and rebuilding that
  one would come up empty and serve. Pinned by
  `zero_byte_image_on_a_fresh_root_is_refused_because_the_log_is_empty`; the same
  0-byte image rebuilds once coverage reports `Some(0)`. This predates the
  `yadb-io` seam and is not a decoder problem: it needs a durable
  create-completed marker or a `LogCoverage` distinction between "never had
  entries" and "emptied after a snapshot".
- [ ] Run separate-process crash/kill and OS/filesystem power-loss tests; document
  filesystem and hardware assumptions. In-process fault simulation is not proof.
  `three_process_mtls_power_loss_after_ack_keeps_acknowledged_writes` SIGKILLs
  all three replicas after acknowledged writes and reopens every log and state
  machine from the children's own files: each acknowledged key is present, the
  recovered log covers the applied index, and restarting the same directories
  elects and commits again. That is crash consistency, not power-loss safety:
  `SIGKILL` leaves the page cache intact, so the parent cannot distinguish
  "the child synced" from "the kernel still held dirty pages", and OS panic,
  device write-cache loss and a lost synced rename remain unproven. Those need a
  root crash harness (`dm-flakey`/`dm-log-writes`, a forced cache flush), and the
  filesystem and hardware assumptions are still undocumented.
- [ ] Redundancy of acknowledged writes. `PoolStriper::put_blob` used to ack once
  `min_write_quorum()` fragments landed — `k` for Reed-Solomon 4+2, so an ack could
  leave zero parity on disk. It now requires every fragment: a short write is
  refused with `BlobError::RedundancyNotMet`, counted in
  `PoolHealthSnapshot::refused_puts` / `ScrubSummary::refused_puts`, and mapped to
  `503` on the SQL, S3 and block paths. Nothing is published for a refused write, so
  the caller holds no reference to the fragments that did land; those fragments are
  garbage, not corruption: a pool-wide mark-and-sweep pass reclaims them. That
  pass is not S3-scoped and is not disabled on Multi-Raft — `yadbd::gc::sweep`
  dispatches to `sweep_groups`, marks from each group's quorum-confirmed
  snapshot and sweeps every mounted pool, and the background scrubber runs it
  every five minutes on the control-group leader. The narrower S3 intent
  collector is a second mechanism, not the only one. An earlier version of this
  paragraph said the only collector was S3-scoped and disabled on Multi-Raft and
  then that no collector reclaimed them; both were wrong, and
  `gc::tests::openraft_tests::a_refused_block_write_is_reclaimed_by_a_multigroup_pass`
  now pins the multi-group behaviour: a refused block write stages fragments, a
  `sweep_groups` pass deletes every staged logical id (the test compares
  normalized logical ids, not each physical fragment) and leaves the committed
  ids, and the committed bytes still read back byte for byte. The real residual
  is timing, not scope: until a pass runs
  a refused write's fragments stay readable by id, and a pool that is not
  mounted, a group this node does not lead and a project routed to no data group
  are refused and delete nothing. The consequence is deliberate: a pool with any
  slot offline
  refuses new writes instead of accepting unprotected ones, so write
  availability now follows full redundancy. The three-node daemon test asserts
  this: a write while one replica is stopped is refused with `503` and stays
  invisible, and the same statement succeeds after that replica returns.
  `crates/yadb-blob/tests/remote_pool_redundancy.rs` adds the crate-level
  multi-node proof over real TCP: one independent HTTP server per slot, each
  with its own `VolumeStore` in its own directory, and assertions against each
  server's own store - a healthy put acks only when every server holds its own
  verified fragment, one stopped server refuses with `RedundancyNotMet` and
  advances `refused_puts` while the four survivors keep only unreferenced
  fragments, the same put succeeds once it returns, and a pool with
  `min_write_quorum` below `total_parts()` acks one fragment short, counts
  `degraded_puts`, and still reads the exact bytes from the survivors with one
  server down. Open: that fixture runs on loopback, so separate hosts, network
  partitions and power loss remain unproven; a refused write's landed fragments
  are still readable by id until the next collection pass, which
  `a_refused_block_write_is_reclaimed_by_a_multigroup_pass` now shows reclaiming
  them on a multi-group fixture; and refusal is per pool, so one offline domain
  stops writes for every volume on it.
- [ ] Idempotency and read-consistency contracts. A keyed `POST /v1/apply` is
  implemented (`Idempotency-Key`, the outcome record committed in the same
  `RevisionTxn` as the mutation, `POST /v1/idempotency/resolve`, and a replicated
  `10_000`-record / `8 MiB` admission counter at `/sys/idempotency/_usage` that
  refuses with `429` before planning). Four project-scope data paths are keyed:
  block UNMAP, block write, S3 object PUT/DELETE and one autocommit SQL
  `INSERT`/`UPDATE`/`DELETE`. For all four the record is project-scoped
  (`/idempotency/<project-id>/…`), the counter is group-scoped
  (`/idempotency/g<group-id>/_usage`) because a transaction cannot span groups,
  and the record, its counter update and the data mutation are appended to *one*
  guarded `RevisionTxn`: `BlockVolumeEngine::unmap_at_with` /
  `write_at_with`, `ObjectStoreEngine::put_object_with` /
  `delete_object_with` and `DatabaseEngine::execute_sql_single_keyed` (through
  `commit_tx_buffer_at_boundaries_with`). A caller guard that fails aborts the
  whole apply, so
  there is no state in which the map/object/row advanced without the record or
  the record exists without the mutation. `resolve` accepts a project scope and
  fences in the project group. Block writes and S3 PUTs stage immutable
  fragments before that transaction; a refused, conflicted or crashed write
  therefore leaves unreferenced fragments (and, for S3, a `Prepared` intent)
  and nothing rolls them back. Only the S3 namespace has an intent collector
  (`ObjectStoreEngine::collect_orphans_all`, and per group
  `collect_orphans_in_group` on Multi-Raft), so block
  fragments from a refused or replaced write accumulate until a mark-and-sweep
  pass reclaims them; see the garbage-collection item. The block layer now
  proves that contract directly: `crates/yadb-block/src/gc_tests.rs` models the
  pass's decision (`present - live`, epoch advance, collectable delete or
  generation barrier) over real `PoolStriper`/`VolumeStore` primitives for a
  refused, conflicted, crashed, replaced and unmapped write, and asserts both
  halves every time — the garbage is gone *and* the live extent still reads its
  exact bytes — including a pass that runs between a refused write and the next
  live write of the same block. That model reproduces `sweep_pool` rather than
  calling it (the daemon crate is above `yadb-block`), and the daemon-layer
  refused-write case is covered separately in `crates/yadbd/src/gc.rs`: the
  legacy `fragments_of_a_refused_write_are_reclaimed_and_the_old_data_survives`
  and the multi-group
  `openraft_tests::a_refused_block_write_is_reclaimed_by_a_multigroup_pass`,
  which drives `sweep_groups` over three real OpenRaft groups. SQL writes are
  keyed for exactly one autocommit `INSERT`, `UPDATE` or `DELETE`:
  `POST /db/{project}/db/query` with an `Idempotency-Key` builds the record and
  the group-counter update inside the statement's own commit
  (`DatabaseEngine::commit_tx_buffer_at_boundaries_with`), so the rows, index
  entries, shard manifests, record and counter are appended to one guarded
  `RevisionTxn` and a conflict or refusal commits none of them. A statement that
  matched no rows still records its outcome, or a replay would re-execute it.
  A multi-statement script still returns 501: it commits statement by statement,
  and there is no cross-statement transaction to make one record cover every
  effect, so a handler-level record committed separately would reintroduce the
  ambiguity the key exists to remove.
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
  that prevented the remaining scopes before this change; the SQL blocker is now
  narrowed to multi-statement scripts and interactive sessions, since one
  autocommit `INSERT`/`UPDATE`/`DELETE` publishes all its visible effects in one
  transaction. A linearizable read still needs one pinned snapshot
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
  `UploadPartCopy`. Orphan blob collection runs per metadata group (see the
  GC item below); aborted or expired upload fragments are reclaimed by the
  group that owns the project, and a node that does not lead that group reports
  it instead of collecting from a lagging view. An abandoned intent becomes
  collectable only after the intent grace, now the per-engine knob
  `ObjectStoreEngine::set_intent_grace_secs` (default 300 s, unchanged, and
  provisioned to 1 s only by the harness so the collection and its inversion
  can be drilled).
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
  Open: the namespace-wide collector is legacy-only (Multi-Raft uses the
  per-group entry point described below);
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
- [x] Garbage collection on Multi-Raft. `sweep` routes to `sweep_groups`, one
  leader-only, quorum-confirmed pass per data group, each with its own collector
  lease `/gc/g<group>/collector`, writer leases `/gc/g<group>/writer/<id>` and
  reader leases `/gc/g<group>/reader/<id>` (TTL 30 s). A reader lease is
  published by `AppState::begin_read_lease` in the group that owns the project,
  renewed across reads, and carries that group's applied log index as its
  revision; `lease_belongs_to_group` checks the provenance after admission
  closes, and a live record refuses the group. A group that has served a read
  in the last 30 s therefore refuses collection until the lease lapses; that is
  the cost of the reader fence, not a defect.
  `POST /v1/pools/gc` answers `200` when every group and pool completed, and
  `409` with `gc_refusal` naming any that refused; the sweep inside
  `POST /v1/pools/scrub` runs the same pass, after a per-group repair scan.
  `repair_scan_groups` takes one quorum-confirmed snapshot per group over
  `/db/`, `/s3/` and `/block/`, builds that group's live set from the snapshot
  only, and repairs each live blob through `repair_blob`; `repair_error` is
  `None` when every group ran and otherwise names the refused groups. The scrub
  route answers `200` only when `repair_error` and `gc_refusal` are both `None`,
  and `409` otherwise - the `/v1/pools/gc` rule. On a multi-node cluster a node
  leads only some groups, so its scrub is `409` while the groups it leads are
  repaired; that is the expected shape, not a missing capability. A group whose
  leadership, leases or storage fence cannot
  be proved is named in `gc_groups_refused` and deletes nothing; a pool whose
  epoch advance is refused is named in `gc_pools_refused` and deletes nothing.
  Four live drills in `tests/protocols.py` cover a live reader pin, a superseded
  collector epoch, a non-leader refusal and a clean collection. A fifth drill
  covers the reader lease: after a `PUT` + `GET` the same route answers `409`
  with `leases_live: 1`, `fragments_deleted: 0` and a named reader lease in the
  billing group, and with no further read it stops refusing once the 30 s TTL
  lapses. That drill replaced the last `SKIP` in the suite, so the gate reports
  27 PASS and no SKIP. An earlier ad-hoc live run had already observed the same
  shape, including `leases_live: 0` before any read and expiry by +35 s. Three
  further shapes are proved at unit level rather than live, because neither a
  race nor a mid-pass expiry is deterministic there: a second collector on one
  group refuses on the held collector record and deletes nothing (plus a
  two-thread invariant test), a collector fence lost between two barrier ranges
  stops the pool and is never counted, and an S3 `GET` that outlives its reader
  lease answers `503` with the `ensure_live` text instead of the bytes. The
  volume-to-pool map a pass marks against is taken from a control snapshot
  proved against the control group's quorum, not from the local cache, and a
  pass refuses before touching any group when that proof is unavailable. Storage-side fencing exists end to end on both runtimes. A barrier
  carries the collector lease deadline and the target refuses an expired one
  with `409`
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
  and deletes through `delete_collectable`. The per-group path shares that
  `sweep_pool` and additionally refuses a live lease whose provenance it cannot
  verify (`lease_belongs_to_group` checks the record's group and claimed
  revision against the group's applied index). `sweep` reaches this path on
  Multi-Raft, and the four cluster drills run. What is still missing: no route
  verifies a *signed* fence token (the internal routes authenticate with the
  shared Raft token, which is reachable only on the internal listener when
  `--internal-listen` is set and on the public one otherwise), and no
  drill runs two collectors concurrently on one group or expires a collector
  lease in the middle of a pass - the daemon has no injection point for the
  latter, and the storage-level drills cover it instead. The repair scan is no
  longer refused: it runs per group on the same boundary as the sweep, because a
  group's quorum-confirmed snapshot is complete for that group's blobs. The
  volume-to-pool map behind `block_pool_lookup` is now read from a control
  snapshot proved against the control group's quorum, not from this node's cache
  (`AppState::control_for_collection`), and a pass refuses before touching any
  group when that proof is unavailable. The remaining limit is narrower: the map
  is the control group's confirmed applied state, so a volume created after it is
  not seen by that pass, and a node that cannot reach the control group's quorum
  refuses to collect even the groups it leads.

  The per-group design, from the read-only analysis of the primitives, is now
  implemented and runs:

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
    it unconditionally (`crates/yadb-db/src/lib.rs:940`, the flush and merge
    paths at `crates/yadb-db/src/tablet.rs:916` and
    `crates/yadb-db/src/tablet.rs:982`, and `crates/yadb-block/src/lib.rs:311`);
    S3 keeps using its `Prepared` intent. The collector side now honours those
    leases: `sweep_groups` takes each group's leader fence before marking and
    refuses the group if any lease is live.
  * **Order of work:** per-group collector lease and writer fence → leader-only
    per-group sweep with the intent grace → a drill that kills the group leader
    between fragment write and manifest commit and proves nothing is reclaimed.
    The first two are done. The third is proved at sweep level, not on a live
    cluster:
    `a_group_leader_death_before_publication_keeps_the_fragment_leased_and_unreadable`
    and
    `a_group_leader_death_after_publication_keeps_the_committed_blob_readable`
    in `crates/yadbd/src/gc.rs` append/publish through the real engines, stop
    every single-node group and reopen the same replica identities from disk.
    The uncommitted half is absent and lease-protected until the lease expires
    and is then collected; the committed half reads back its exact bytes and
    keeps its fragments through a pass. What is still missing is the live drill
    on a multi-node cluster, where one group leader fails over to a peer while
    the other groups keep serving, and where the death can cut a log entry
    mid-fsync rather than shut down cleanly.

- [x] Orphan blob collection refuses a scope whose `project_ids` is `None`.
  `collect_orphans_in_scopes_with` used to enumerate a group's projects before
  it collected, so a group that covers every project - a single-group Multi-Raft
  deployment - had no enumerable project set and was refused with
  `NamespaceScanUnavailable`. The refusal was deliberate: collecting from a
  partial project list would have left the unlisted projects' orphans behind
  while reporting a completed pass, so the gap was "cannot collect", not
  "collects wrongly". The repair pass never inherited the guard:
  `repair_scan_groups` is prefix-scoped over `/db/`, `/s3/` and `/block/` and
  never enumerates projects.
  Closed by deriving the project set from the pass's own leader-fenced view.
  `require_group_view` now returns the rows of the quorum-confirmed
  `try_linearizable_group_snapshot` instead of discarding them, and a scope
  with `project_ids == None` walks those rows under `/s3/`, `/s3-intent/` and
  `/s3-retired/` with `projects_in_keys`, the same helper the legacy
  namespace-wide pass now shares. A group's state machine holds only that
  group's keys, so the walk is complete for the group; there is no second
  unfenced scan, a scope with `project_ids: Some(...)` still visits exactly its
  listed projects, and a group with no scope is still `Ok(0)`. If the snapshot
  cannot be taken the pass still refuses and deletes nothing.
  Proven in-process by
  `a_scope_covering_every_project_collects_from_its_own_snapshot` in
  `crates/yadbd/src/gc.rs`, on a real single-group OpenRaft deployment with real
  volume directories: it collects a live project's aborted multipart upload and
  a second project's aborted upload that has no manifest at all, reports exactly
  those two fragments, leaves the live object's fragments intact, and then stops
  the group's raft and asserts the pass refuses through the fence with a `Meta`
  error (not `NamespaceScanUnavailable`) and deletes nothing. Restoring the
  `NamespaceScanUnavailable` early return fails that test with
  `called Result::unwrap() on an Err value: NamespaceScanUnavailable`. A
  `yadb-s3` unit test pins the same enumeration over a substituted view. No live
  daemon drill has collected orphans from a `project_ids == None` scope, so the
  live shape stays unobserved even though the in-process shape is proved.

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
- [x] Complete the OpenRaft network factory and listener over TCP with
  length-prefixed frames, request timeouts, connection limits and a payload
  limit. The server verifies the signature against the sender's trusted public
  key and only then dispatches the Raft RPC. Responses are signed and bound to
  a random per-request nonce. Requests bind a fresh server challenge per TCP
  connection, including across server restart. Wire-level tests pin each claim
  separately: the declared-length check runs before the byte reservation (a
  length the in-flight budget would otherwise grant is refused with the
  semaphore untouched), an over-limit frame fails closed and leaves later
  connections usable, the server deadline ends the connection instead of
  reporting success, the client deadline is reported as a timeout rather than a
  reply, a connection above `MAX_CONNECTIONS` is refused with its socket closed
  and the cap recovers once the holders release, and the adversarial set is
  covered: forged sender, tampered body with a valid header, replay on the same
  and on another live connection, a captured request after server restart, a
  request bound to another connection's challenge, and a substituted reply
  (wrong nonce, wrong sequence, untrusted key, tampered payload). Opt-in
  TLS/mTLS is implemented separately below.
- [x] Bound the plaintext listener's exposure to one reachable peer. Two
  per-address sub-caps mirror each other. Handler slots: one source address may
  hold at most `ceil(MAX_CONNECTIONS / (voters - 1))` and never more than
  `MAX_CONNECTIONS_PER_PEER` (half the 64 global slots); a refused connection is
  closed and counted as `connections_peer_cap_dropped`. In-flight frame bytes:
  one source address may hold at most `MAX_IN_FLIGHT_REQUEST_BYTES_PER_PEER`
  (half of the 64 MiB budget, which is exactly `MAX_FRAME`), accounted like the
  slot cap by canonical source IP, created on demand and removed at zero. A
  frame over that share waits for the address's own reservations to drain; it is
  not refused for exceeding its share, because it already passed the `MAX_FRAME`
  and global-budget checks, and a compile-time assertion proves every admissible
  frame fits an empty share. The wait is not a queue: capacity release notifies
  the waiters and they race, and a wait longer than the handler's deadline is
  dropped, so admission is not starvation-free under sustained contention. Waits
  are counted as `request_bytes_peer_waited`
  and logged first-eight-then-powers-of-two. The global budget was raised in
  this change from 32 MiB to `2 * MAX_FRAME` (64 MiB), because half of 32 MiB is
  less than one `MAX_FRAME` and the per-address share would have been unusable;
  peak live frame buffers therefore double, which is the accepted cost, and one
  address can leave at least one maximum frame for another. The ordinary case —
  one maximum frame from one address — is admitted without waiting. Both hazards
  were proved before the fix and pinned after it by
  `one_source_address_cannot_hold_every_handler_slot`,
  `one_source_address_cannot_hold_the_whole_in_flight_byte_budget`,
  `one_maximum_frame_from_one_address_is_admitted_without_waiting` and the
  end-to-end `one_address_stalling_maximum_frames_does_not_starve_another_peer`
  in `crates/yadb-metakv/src/openraft_net.rs`; the existing global-cap recovery
  test now fills the cap from several source addresses. Two colluding source
  addresses can still fill the global 64 MiB budget: that is outside "one
  reachable peer" and is not fixed here. Co-located replicas or several replicas
  behind one NAT share one address's slot and byte shares.
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
- [x] Split the internal listener from the public client listener so the peer
  routes are not reachable from client networks. `--internal-listen <addr>`
  serves every `/internal/...` route on a dedicated listener; the public
  listener then answers axum's own `404` for them, with no token challenge and
  no handler body, so a firewall can bound the peer network. Leaving the flag
  unset keeps the historical single-listener shape, so no existing deployment
  changes. The Raft token check is unchanged and still runs in every handler:
  the listener narrows who can attempt a call, it does not authenticate it. The
  internal listener serves only those routes - `/v1/apply`, the status routes
  and the rest of the client surface are absent from it - so it cannot become
  an unauthenticated path to the control plane, and no request is trusted
  because of the port it arrived on. In a cluster `--internal-peers
  <id=addr,...>` is required alongside the flag: startup refuses a shape where
  a peer RPC or the follower-to-leader control snapshot would 404.
  `crates/yadbd/tests/internal_listener.rs` asserts both halves against real
  daemons - absent on the client port with the flag, present and
  token-protected without it - plus a three-node cluster that replicates over
  the internal listeners. Serving `/internal/...` on both listeners fails two of
  those tests. Residual: the default is still one listener, so a deployment that
  does not set the flag keeps the exposure and still relies on firewall rules.
  This split also fixed a real second caller: `fetch_fresh_control` dialled the
  leader's public port for `/internal/control/snapshot`, so with the flag on
  every follower would have failed closed with no control image and therefore no
  pool materialisation or authorization; it now uses the peer address.
- [x] Protect the remote VDisk RPCs. `PUT|GET|DELETE
  /internal/vdisk/{pool}/{slot}/blob/{blob_hex}`, `GET .../stats`, `POST
  .../barrier`, `GET .../blobs` and `POST .../compact` were served on the public
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
  firewall rules actually bound who can attempt them. That listener now exists
  behind `--internal-listen`; the exposure remains only in the default
  single-listener deployment.

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
- [x] Implement a multi-group registry: several independent groups per node,
  per-group directories, per-group failure isolation and bounded resources.
  The in-process registry enforces the group-count limit before it reserves a
  directory (both `create` and `open` refuse with `Full` and no directory is
  created), rejects a duplicate identity without disturbing the first group,
  isolates a failed create/open so neither an entry nor a partially initialised
  directory survives - a retry then fails for the original reason rather than
  `AlreadyExists`, which was a real defect: `create` used to leave the empty
  directory behind - and retains ownership across a cancelled shutdown until a
  clean shutdown removes the slot. It still does not provide per-group
  memory/disk isolation or shared network routing. The daemon now always starts
  a version-2 multi-group configuration with one dedicated mTLS listener per
  group; its integration and failure behavior still require validation.
- [x] Report the last collection pass and each metadata group's state on the
  read-only health surface. `/v1/pools/health` now carries `last_gc` (the
  recorded `SweepOutcome` of the most recent pass, including a pass that refused
  before scanning) and `groups` (one row per configured metadata group with
  role, term, leader, commit and applied index, and the reader-lease count and
  nearest expiry this node holds for it). Every group field is a local
  observation: nothing contacts a peer, reads a metadata key, or takes or
  renews a lease, and the commit index comes from a bounded in-process query, so
  a wedged core reports the reason in that row instead of hanging the response.
  `last_gc` is separate from `last_scrub` because the repair scan runs every few
  seconds and skips collection most of the time, so its summary would show a
  skip notice rather than the last real pass. `pools`, `last_scrub`, `node_id`,
  `/healthz`, `/v1/status` and `/raft/status` are unchanged. Evidence:
  `pools_health_reports_the_last_collection_pass_and_every_group` runs a real
  pass on the multi-group fixture, asserts the recorded counters and the three
  group rows, proves two health reads neither take nor renew a lease, then
  invalidates control freshness and asserts the refusal replaces the previous
  success; removing either recording line fails the test.
- [ ] Complete the operational surface around that health data: logging,
  alerting and recovery/upgrade runbooks, and validate them across
  multi-node deployments. Metrics export is done (next bullet) but nothing
  consumes it: no alert rules, no dashboard and no scrape config are shipped,
  and the runbooks for recovery, upgrade and group reconfiguration are still
  unwritten.
  - [x] Export that health data for a scraper. `GET /metrics` on the public
    listener renders the same rows `/v1/pools/health` serves — node and runtime,
    one family per pool, one per slot, the last collection pass, the last repair
    scan and one row per metadata group — in the Prometheus text format
    (`version=0.0.4`). Rendering is a pure function over those rows, so it is
    unit-tested without a daemon. Every family is emitted with `HELP` and `TYPE`
    even when its input is absent, and a `*_last_pass_present 0` gauge says "no
    pass has run" rather than letting a dashboard silently stop matching after
    a restart; a refused pass is `yadb_gc_refused 1` with its own counters, not
    a missing family. Label values escape backslash, quote and newline, and
    large counters stay decimal. The handler gathers exactly what the health
    endpoint gathers and adds nothing: no lease is taken, no pass runs and no
    recorded outcome changes. `metrics_endpoint_serves_the_health_surface_as_prometheus_text`
    asserts the last two against the multi-group fixture by comparing the
    recorded pass before and after a scrape and checking that the lease cache is
    empty and that no lease record was published; the stronger "no counter
    moved" wording is not something that test checks, so it is not claimed here.
    `metrics_endpoint_is_served_by_the_daemon` boots the real daemon and scrapes
    twice to prove the route is registered on the public listener (removing the
    route fails it with `404`) and that a scrape never runs a pass. It
    is a read-only surface on the public listener, not a new control plane, and
    it publishes no subject, key or object name.
  Each group starts from strict bounded JSON configuration, explicit
  create/open/bootstrap, mandatory mTLS and a separate listener; its
  create/shutdown/reopen tests pass. The daemon now requires a version-2
  multi-group manifest and uses group-specific listeners. The singleton-group
  child-daemon integration passed, while multi-node validation and cancellation
  coverage remain open. Status reports
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
  recovery boundaries. It does not establish the remaining operational tooling.
- [x] Provide the operator collection commands. `yadbctl pool-gc` and
  `yadbctl pool-scrub` sign their admin request (`--subject`,
  `--subject-secret` / `YADB_SUBJECT_SECRET`) and interpret the daemon's three
  response shapes instead of printing JSON: `200` prints the counters and exits
  `0`; a per-group `409` prints the counters that ran, lists the named groups and
  exits `2` (a partial pass, not a failure); a whole-pass `409` from the
  unavailable control-plane snapshot says nothing was scanned or collected and
  exits `1`. The interpretation is a pure function over `(status, body)` with
  unit tests for all three shapes and for a malformed body, and `pool-gc --json`
  prints the daemon body verbatim. `pool-scrub` was not signed before this
  change, so on any node with a non-empty admin allowlist it could not pass the
  gate at all; that is fixed here rather than worked around.
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
- [x] Test node shutdown/restart, partitions, leadership changes, quorum loss,
  snapshots and catch-up with independent processes, real filesystems and TLS.
  `three_process_mtls_failover_and_quorum_loss` covers stop/restart, quorum
  loss/restoration and a cross-process snapshot install;
  `three_process_mtls_network_partition` covers bidirectional link cuts; and
  `three_process_mtls_leader_kill_elects_a_replacement_and_the_dead_leader_catches_up`
  covers a SIGKILLed leader (the exit status is asserted to be signal 9, so this
  is a kill and not a clean exit), a different survivor elected inside the 20 s
  budget, a post-kill write acknowledged and applied on both survivors, a
  wrong-identity restart refused with `replica identity or bootstrap voters
  mismatch` before storage, and the dead leader reopened on its own directory
  and identity, converging on every key as a follower of the replacement. Each
  test asserts that no acknowledged key was lost, and the kill test reads every
  key back through the durable adapter after all three processes die.
- [ ] Test delayed and duplicated messages between independent processes, and
  run the process tests on more than one host.
  `three_process_mtls_delayed_and_duplicated_rpcs_do_not_break_agreement` puts a
  parent-side mTLS relay on every directed link, delays each Raft RPC by 60 ms
  and re-originates it as a second authenticated RPC, so a receiving child
  really processes each AppendEntries twice. A write is still acknowledged only
  after a follower already holds the index, all three replicas converge, and
  `applied_index == last_log_index` on every replica; after the relay is removed
  the cluster still agrees. A verbatim replayed frame is rejected before the Raft
  handler (the rejected-envelope counter rises while the duplicate counter does
  not move), and applying the same entry twice through the durable adapter is
  rejected as non-consecutive. Open: this is a userspace relay on loopback with
  in-order delay and duplication only, so real network reordering, loss, jitter,
  congestion and a multi-host deployment remain unproven. A killed process is
  also not a power cut: dirty filesystem pages survive `SIGKILL`, so OS and
  device power loss stay with the separate durability gates.

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
the challenge frame to 32 bytes before allocating its body. A shared 64 MiB
(`2 * MAX_FRAME`) encoded
inbound-request budget is reserved before allocating body buffers, with a
per-source-address share of half of it; this is not a
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
ahead of the log checks the public `committed` field and the purge side effect
rather than private `RaftState` fields; and `last_applied_state` inserts a blank
entry to avoid an apply gap. `scripts/test_openraft_suite_mapping.py` records
each substitution and fails when an unchanged case loses an assertion, so a
mapped case cannot silently lose coverage; it does not turn a substitution into
proof of the private assertion it replaced, so these differences remain open as
contract questions. Purged-prefix append behavior and conflicting boundary
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
| `get_initial_state_without_init` | Replaces whole private `RaftState` equality with every observable public field (vote, `committed`, server state, membership, snapshot) plus the store's empty log/purge state; the remaining private fields are uncovered |
| `get_initial_state_with_state`, `get_initial_state_last_log_gt_sm` | Checks the public `committed` field and store log state instead of the private recovered-state log cache; not equivalent private-cache evidence |
| `get_initial_state_last_log_lt_sm` | Checks the public `committed` field (which holds the private last-log value here), the purge side effect and the retained suffix rather than the private cache accessor |
| `get_initial_state_membership_from_log_and_sm` | Moves the initial empty-log recovery to a fresh store so that its purge does not precede the later below-boundary append; sequential setup differs |
| `get_initial_state_log_ids` | Reconstructs key log IDs from public reads; does not inspect OpenRaft's internal cache |
| `get_initial_state_re_apply_committed` | Retains applied-state observation but replaces private `io_applied` assertion; the no-op committed-persistence default also limits this case's coverage |
| Limited reads, single-entry/log-ID lookup, last-ID, snapshot metadata, single/multiple apply, snapshot transfer | Cases run in the adapted runner with public blocking append/node-ID helpers; this does not add process-crash or fault-boundary coverage |
| `last_applied_state` | Adds blank index 4 before index 5 to satisfy this adapter's consecutive-apply rule; input differs |

The direct contract tests above add reopen checks but do not close these private-
state and changed-input differences. `scripts/test_openraft_suite_mapping.py`
machine-checks the case-by-case mapping, the substitutions and every assertion
that is not observed verbatim, so a mapped case cannot silently lose coverage.
Full equivalence requires preserving the original assertions through an
appropriate upstream harness or proving and testing each replacement
independently; a green adapted runner alone is insufficient.
