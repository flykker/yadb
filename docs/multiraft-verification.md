# Multi-Raft verification and release evidence

## Current status (2026-11-14)

**Before the test serialization gate described below, the last completed full
workspace gates were GREEN on the authorization-freshness delta plus the
nonblocking-refresh fix and the post-review hardening.** Offline, locked `-j 2`
runs with `CARGO_INCREMENTAL=0` and the approved `YADB_TEST_TMPDIR` root passed.
All features: MetaKV **124/124** (68.77 s), yadbd units **27/27**, clients **2/2**
and daemon **2/2** (10.11 s), with all other workspace tests passing. Default
features: MetaKV **124/124** (75.50 s), yadbd **20/20**, with feature-gated
integration files running zero as expected. Logs:
`/data/data/com.foxdebug.acodefree/cache/opencode/workspace-all-features-review2-20261114.log`
and `.../workspace-default-review2-20261114.log`. The previously red all-features
failure at `crates/yadb-metakv/src/openraft_multigroup_test.rs:245`
(`OpenRaftWriteUnknown`, client write timeout) did not reproduce: that test
passed alone **15/15** and inside both full gates. No causal fix is claimed, and
the historical process-failure and election-churn failures remain unexplained.

### Test serialization gate (2026-11-15)

`crates/yadb-metakv/src/openraft_test_gate.rs` provides
`pub(crate) async fn exclusive() -> tokio::sync::MutexGuard<'static, ()>`, backed by
a process-global `OnceLock<tokio::sync::Mutex<()>>` and declared as `mod test_gate;`
in `openraft_runtime.rs`. It serializes the heavy cluster tests that assert progress
against real wall-clock deadlines:
`three_nodes_three_groups_route_independent_leaders_over_mtls`,
`three_process_mtls_network_partition`, `three_process_mtls_failover_and_quorum_loss`,
`persistent_singleton_process_kill_reopen`,
`openraft_persistent_three_nodes_restart_and_catch_up_follower` and
`openraft_persistent_cluster_recovers_lagging_follower_via_snapshot`. The two process
tests and the child-process kill/reopen test take the gate in the parent only.
Children return before it. That is not what prevents a cross-process deadlock — the
`OnceLock` is process-local, so a child re-executing the binary holds a private
mutex — it only avoids a meaningless acquisition. The gate's scope is likewise one
process: other test binaries, other Cargo invocations and the live cluster still
compete for CPU, so it reduces contention rather than promising a dedicated core.

Both full workspace gates were red before the gate. Each ran `-p yadb-metakv --lib`
with about 124 tests in parallel on an eight-core machine and produced three distinct
signatures. The multigroup test failed with
`ControlFreshnessUnavailable("linearizable control read failed: not enough for a
quorum, cluster: {voters:[...]}, got: {1}")`; it later failed with
`OpenRaftWriteUnknown("client write timed out")`. The default-features process test
failed at the post-snapshot catch-up check with `leadership changed during snapshot
catch-up`, where the expected leader reported no leader and had voted for a different
node in the same term.

The quorum miss and the bridge timeout are load-induced scheduler artefacts, not
product defects. openraft 0.9.25's `RaftCore::handle_check_is_leader_request` bounds
the quorum confirmation by `heartbeat_interval`, 50 ms by default, and a loopback
mTLS round trip under full-suite load can exceed that; the miss is fail-closed, so no
stale data is served. The 5 s `recv_timeout` in the metadata bridge aborts a client
write whose commit outcome is then unknown to the caller, but the deadline expiring
under load is a scheduling effect. The leadership change is load-aggravated and also
exposes a real product gap: openraft 0.9.25 has no pre-vote, so a rejoining or
isolated replica can inflate its term and depose a healthy leader, and late
heartbeats under CPU starvation make it far more likely. The process test passed
**8/8** solo runs; the multigroup test passed **24/24** when eight copies ran
concurrently without the rest of the suite.

The preserved artifacts of that process-test failure
(`/data/data/com.foxdebug.acodefree/cache/yadb-process-30593-1794690147519986819`,
one `control-<node>-<generation>/request-N` per child RPC) show the mechanism rather
than only the symptom. Node 2 was leader at term 4 and had applied through log index
10 while serving status requests. The next recorded status from node 2 reports term
**10**, a vote for node 3, no leader, and `last_applied` still 10 — so no entry was
lost and no snapshot install failed. Node 3, the node that had been stopped and
restarted, must have run six consecutive unsuccessful elections (terms 5 through 10)
before node 2 granted its vote and stepped down. That is the pre-vote gap operating
end to end: a stalled replica converts a temporary inability to reach a quorum into
a monotonically rising term, and the first vote it wins deposes a leader that had
committed state. The gate removes the starvation trigger, but this amplifier remains
a real availability hazard in the product and is not fixed by it.

The gate makes the test environment match the dedicated-CPU assumption the tests
encode; it fixes only the scheduler-artefact class. It does not fix the missing
pre-vote, does not change the 50 ms-heartbeat-bound quorum check, and does not change
the 5 s bridge deadline. The post-gate full-workspace run has not finished, so no
gate is claimed green here; earlier passes remain historical.

The freshness delta adds unit tests rejecting stale revisions and daemon regressions
for the token-protected internal snapshot endpoint and revocation. The revocation
test waits for caches on all nodes, but asserts denial only through `public[0]`,
not every endpoint. Security tests passed **16/16** inside the full gates,
including new checks that an empty configured Raft token is rejected rather than
treated as a wildcard and that S3-wire error mapping preserves
`x-yadb-write-outcome`/`x-yadb-retry`. Lifecycle unit tests for
`scripts/yadb-local.py` pass **11/11**, including the new `--port-base`
offset/recording, boundary rejection, prepared-state Raft-listener detection and
CLI dispatch checks; `scripts/test_yadb_local_smoke.py` passes **3/3** and proves
smoke requests follow the recorded port base instead of the 8420 default.

The completed final independent GPT-6.1 Sol review of this delta returned
**ACCEPT for the narrow experimental local scope only**, with `production_ready`
still false. It confirmed the lease ordering, revision-regression rejection,
leader-only quorum-confirmed internal endpoint, and absence of a guard held across
`.await` in the reviewed HTTP path. Its non-blocking findings were addressed where
they were in scope: the empty-token wildcard, the dropped S3-wire mutation-outcome
headers, the off-by-eight `--port-base` upper bound, prepared-state Raft-listener
checking, and overstated wording in these docs and `scripts/README-local.md`.
Findings left open are recorded below.

Open findings from that review, not fixed in this delta:

- Blocking-pool starvation is mitigated, not eliminated; failed refreshes
  serialize on the refresh mutex and every expired-lease request can enqueue a
  blocking task.
- Socket timeouts bound individual operations, not total elapsed refresh time.
- iSCSI maps freshness failure to DATA PROTECT/WRITE PROTECTED sense, and
  established PG sessions map later 503s to generic `XX000`; both fail closed but
  are imprecise.
- The revocation regression asserts denial only through `public[0]`, not every
  protocol/endpoint or a post-rejection no-mutation check.
- Port bind checks are pre-launch only and can race a concurrent starter.
- openraft 0.9.25 has no pre-vote. A replica isolated from the majority keeps
  incrementing its term on failed elections and deposes the current leader when it
  rejoins. This was reproduced in-process, not on the live cluster: with the
  default 150–300 ms election timeout,
  `openraft_persistent_cluster_recovers_lagging_follower_via_snapshot` failed about
  8% of runs because the rejoining node's higher term elected node 3, whose
  unpurged log let the lagging follower catch up without installing a snapshot.
  The test now uses 20–25 s election timeouts so leadership stays stable and the
  purged log forces a snapshot; it then passed 40/40 runs. The failure was a test
  assumption, not a storage defect, but the underlying term inflation is real and
  is a candidate explanation for the historical election churn. It is not proven
  to be the cause of those failures, and no mitigation is implemented.
- The linearizable-read primitive hardcodes a 1000 ms bound
  (`OpenRaftClient::linearizable_snapshot`), and openraft bounds the quorum
  confirmation itself by `heartbeat_interval`: `RaftCore::handle_check_is_leader_request`
  sets `let ttl = Duration::from_millis(self.config.heartbeat_interval)` and sends
  empty `AppendEntries` to every voter, requiring a quorum inside that window
  (openraft 0.9.25). A miss is reported as
  `ControlFreshnessUnavailable("... not enough for a quorum ... got: {leader}")` —
  a fail-closed error, not stale data. The live deployment's 200 ms heartbeat
  therefore means a linearizable read can fail under load even though the client
  bound is 1000 ms. openraft 0.9.25 has no separate linearizable-read timeout
  setting; `heartbeat_interval` is the only lever. This was diagnosed from a real
  failure: the multigroup barrier test asserted a linearizable read on a healthy
  three-node group and got a quorum miss under full-suite load with the 50 ms
  default. The test now configures a 500 ms heartbeat and long election timeouts
  because its elections are manual; the product code was not changed, and the
  availability limit remains real.
- The metadata bridge aborts a client write after a 5 s wall-clock deadline
  (`result_rx.recv_timeout(Duration::from_secs(5))` in `openraft_client.rs`) and
  returns `OpenRaftWriteUnknown("client write timed out")`. Because the
  `client_write` task is aborted rather than the Raft entry, the caller cannot tell
  whether the proposal committed; the error is honest but ambiguous. The deadline is
  a product value and was not raised. Test-side, the serialization gate removes the
  competing load that made it expire; the bridge itself is unchanged.
- A keyed apply whose record write conflicts on the *control-state* guard (rather
  than the record guard) returns 409 "control state changed; replan and retry"
  instead of replaying. This is honest — the daemon cannot tell which guard
  failed, because `KvResult::CasConflict` carries only `current_version` — but it
  means a legitimate concurrent admin apply makes a keyed request fail rather than
  succeed. Retrying with the same key is safe and eventually succeeds.
- Idempotency has no replicated quota. Records are capped at 16 KiB each but the
  number of records is unbounded, so a holder of the admin secret can grow the
  control group without limit. This is a deliberate gap, not an oversight.

The nonblocking-refresh fix moves the quorum-confirmed control fetch off the Tokio
worker: HTTP handlers now call `control_for_authorization_async`, which runs
`ensure_control_fresh` in `spawn_blocking`, and post-apply refreshes use
`refresh_openraft_control_async`. The synchronous path remains for the PG/iSCSI
connection threads and the background refresh thread. Previously a blocked
TCP read to a stalled control leader could occupy a runtime worker while holding
the refresh mutex, so a node could fail to answer the leader's own internal
snapshot request. This is a plausible contributor to historical client write
timeouts, **not** a reproduced or causally proven fix.

Control-scope idempotency was verified live on the local three-process deployment
(binary `a10c8f15903b10011f153e5cf99a648a903c4f1ba7b66ff69ab2b97b06360beb`,
generation 1→2), not only in tests. Unsigned `/v1/apply` returned 401, a forged
signature 401, a correctly signed non-admin 403, and the allowlisted subject 200.
A keyed apply returned 200 and advanced `generation` by one; replaying the same key
and payload returned 200 with an unchanged `generation`; the same key with a
different payload returned 409 `idempotency_key_conflict` with `generation`
unchanged; `POST /v1/idempotency/resolve` returned `committed` for the used key and
`not_committed` for a fresh one; an apply carrying a fenced key returned 409
`idempotency_key_fenced`; a non-`control` scope returned 400. The records and the
`live-idem-pool` project survived two clean stop/start cycles. What this does not
establish: no response-loss injection, no leader kill during a keyed apply, no
follower-forwarded keyed apply, and no quota behaviour, because no quota exists.

Project-scope idempotency exists for block UNMAP only, and its live and
integration evidence is narrower than the control scope's. The integration test
`three_daemons_forward_http_to_different_group_leaders` drives it through the
real daemon: an unproven identity gets 401 and a verified non-member 403 before
any record exists; the same key in two projects produces two independent records;
a replay returns 200 and the recorded response; the same key with a different
range or a byte-different JSON body of the same range returns 409
`idempotency_key_conflict`; `resolve` with `scope: project` returns `committed`
from the project group; a fenced key returns `not_committed` and a later keyed
UNMAP with it returns 409 `idempotency_key_fenced`; a control payload carrying
`project`, a project payload without it, and an unknown scope all return 400. The
legacy runtime refuses a keyed UNMAP with 501. What this does not establish: no
crash between map publication and record (they are one transaction by
construction, but no fault injection proves it), no concurrent same-key race
across two nodes, no leader loss during the publication, and no quota exhaustion
on a real project group — the counter's ceiling is covered by unit tests only.

That was also exercised live on the three-process deployment (binary
`b00018a9f092da23d3cfae818bc0e4ac7008dea659abca6336924bfeb7f87ddc`, built from
`e246483`), against a 10 GiB volume with a real 4 KiB pattern written and read
back first so the zeros afterwards prove the discard rather than an empty volume:
keyed UNMAP 200, replay 200, different range 409, byte-different body of the same
range 409, project `resolve` `committed`, fresh `resolve` `not_committed`/`fenced`,
keyed UNMAP after the fence 409 `idempotency_key_fenced`, control-with-`project`
and project-without-`project` 400, unaligned UNMAP 400. An admin subject that is
not a project member is refused with 403 on block read and write, so admin rights
do not imply membership. Issuing the write and the keyed UNMAP to followers
forwarded them to the group leader (`x-yadb-leader-id: 3`) and the discard was
visible from a third node; a replay from a third node also returned 200. After a
clean supervisor stop and restart, the committed key still resolved `committed`,
the fenced key still `not_committed`, the block still read as zeros, and a replay
still returned 200, so the project records and their group counter survive a
restart. This is still not evidence for the cases listed above: no fault
injection, no concurrent same-key race, no leader kill mid-publication, and no
quota exhaustion on a real group.


Evidence for the fix is a `SIGSTOP`/`SIGCONT` experiment on the live cluster:
pausing the control leader for about 1.2 s (below the 1500 ms preferred election
minimum, so no election should occur) left follower `/healthz` at 200 in 5–14 ms
and unknown-project SQL denial at 403 in 4–11 ms on both followers. After
`SIGCONT` and a 2.5 s wait, denials still returned 403 in about 8–12 ms. This
shows the followers' handlers are not blocked while the leader is unscheduled; it
does not prove correctness under longer stalls, and no election or write
recovery is claimed. The review also noted what the fix does not remove: failed
refreshes serialize on the refresh mutex, every expired-lease request can enqueue
a blocking task, the internal endpoint shares the same blocking pool, socket
timeouts are per-read rather than a total deadline, and synchronous lock
contention remains possible. "Bounded refresh" therefore means bounded component
timeouts, not a proven end-to-end latency bound.

Parallel-deployment support was exercised live once. A second disposable root
`/public/cluster_data_b` was prepared with `--port-base 9420` (HTTP 9420–9422,
Raft 19420–19428) while `/public/cluster_data` kept running on 8420–8422, and it
started under supervisor PID **6485** with children **6502**, **6503** and
**6506**. Its own `status` was `consistent`, all nine Raft listeners were open,
`/v1/status` reported no projects and generation `0`, unknown-project SQL/S3/block
returned **403**, the internal endpoint returned **401** without a token, **200**
with revision `0` on its leader 9420 and **409** with `x-yadb-leader-id: 1` on
9421/9422. The primary deployment stayed `consistent` on 8420 throughout. Raft
tokens do not cross deployments: cluster B's token returned **401** on 8420 and
cluster A's token returned **401** on 9420. Subject-secret separation could not be
observed on an empty cluster, because every authenticated endpoint resolves the
project before checking the subject and `/v1/status` ignores subject headers; no
claim is made there. Cluster B was then stopped with SIGINT and recorded
`phase: stopped`, exit codes `[0, 0, 0]`; its artifacts are preserved and it is
not running.

The current nonproduction loopback deployment is `/public/cluster_data` (mode
`0700`), prepared and started with `scripts/yadb-local.py run`. It is running under
supervisor PID **16793**, children **16795**, **16796** and **16808**, with HTTP
on loopback ports 8420–8422 and Raft on 18420–18428. This is a clean restart:
all nine group configs are `mode: open, initialize: false`, not a first create.
The binary SHA-256 is
`5775b7bc278210deb7f6c8b91a3ca42ac94bb26daf3b3aade8b29583b7d85773`
(mode `0700`, 158,373,144 bytes). `metadata_runtime` is `openraft` and
`production_ready` is false.

**The deployment is no longer empty and is not treated as an empty reference.**
At 21:25:24 local on 2026-11-14 the control group committed one apply that exactly
matches `examples/platform.yaml`: generation `1`, `next_project_id` `2`, project
`billing` with members `sa:billing-backend` (readWrite) and `user:auditor`
(readOnly), database `billing/ledger`, object store `billing/receipts` and its
block volume, plus the `wal-fast`/`data-ec` pools. Raft log and state files were
written at 21:25:24–25 on all three nodes and the pool volumes were materialized
at the same time. No apply was issued by the orchestrator; `yadbctl` was not
executed (atime predates the event), `logs/smoke-setup-started` does not exist,
and no smoke evidence file was produced. The actor is not identified from the
available artifacts; the window overlaps the final review session. The state was
preserved, not rolled back or reinitialized, per the no-reinitialization rule.
The external snapshot of the observed state is
`/data/data/com.foxdebug.acodefree/cache/opencode/cluster-data-unexpected-apply-20261114T184500Z.json`.
Consequences: `/v1/status` reports generation `1` and project `billing` on all
three nodes; the internal snapshot reports revision `2`; no claim of an empty
cluster or revision `0` applies to this deployment any more.

Live freshness probes were re-run after the restart. Unknown-project SQL, S3 and
block reads returned **403** on 8420, 8421 and 8422, not 503. The internal
snapshot endpoint returned **401** without a token on every node, **200** with
revision `2` on control leader 8420, and **409** with `x-yadb-leader-id: 1` on
followers 8421/8422. The daemon regression additionally
checks a wrong token returns 401, waits for all nodes' caches to observe revocation,
and asserts denial of the old subject through `public[0]` only.

Preferred leaders are group 1→node 1, 2→node 2 and 3→node 3, with heartbeat
250 ms, preferred election minimum 1500 ms, nonpreferred minimum 2750 ms and
maximum 3000 ms. In one current observation all three nodes reported those leaders
and `preference_met: true`. This probabilistic soft bias is not a placement guarantee
or workload balance, and does not resolve historical election churn.

Relocation was cancelled and no relocation script exists. The prior root
`/data/data/com.foxdebug.acodefree/cache/opencode/yadb-local-20261114T135536Z-54c1c388-188b-4a5b-927a-4dd5cf4a7a62`
was deleted offline after PIDs were verified absent, ports free, 13 directory locks
held and symlinks rejected. Its smoke/test data and root-local evidence files were
deleted; historical smoke, clean reopen, leadership sampling and election churn
observations below do not validate the new cluster. The external pre-delete inventory
`/data/data/com.foxdebug.acodefree/cache/opencode/yadb-relocation-before-beb66119de814f32bfb2d8699fddddae.json`
contains SHA-256 hashes, sizes and modes only, not data. The final independent Sol
review approved explicit-root prepare and the conditional deletion plan, not production.

The final independent combined Sol review did **not accept** the work overall. It accepted
the HTTP unknown-outcome and framing, URL segment encoding and UNMAP 416 changes
at source level. Subsequent green gates do not establish causal fixes or a new
overall acceptance. Earlier entries below are historical checkpoints.

* Earlier offline, locked full workspace runs used `-j 2`, `CARGO_INCREMENTAL=0`
  and the approved `YADB_TEST_TMPDIR` root. All features passed: MetaKV **124/124**
  (73.94 s), yadbd **21/21**, daemon **2/2**, clients **2/2**, S3 **19/19** and
  IO **7/7**, with all other tests passing. The default run also passed: MetaKV
  **124/124** (84.40 s), yadbd **15/15**, all other tests passing, and zero
  feature-gated integration tests as expected.
* A separate targeted process run passed **2/2** (4.58 s), without a demonstrated
  causal fix. The historical process and snapshot catch-up failures remain unresolved.
  The deprecated legacy `open_clustered` warning and expected offline-peer snapshot
  diagnostic remain.
* Historically, the default full run passed only 121/124 MetaKV tests (group 3
  leader timeout and both process tests without acknowledged writes); the
  all-features run passed 122/124 (both process tests without acknowledged writes
  at `crates/yadb-metakv/src/openraft_process_test.rs:151`). A targeted process
  run also failed 2/2 with no acknowledged write or peer replication progress.
  These failures are not explained by the later passes.
* After the latest narrow parser, UNMAP and URL edits, orchestrator security tests
  passed **11/11** and feature-enabled daemon tests passed **2/2**; client tests **2/2**
  (0.81 s). State-machine tests passed **16/16**, storage **23/23**, S3 **19/19**
  and IO **7/7**. `cargo check --offline --locked --workspace --all-features`
  passed with `-j 2`, and `git diff --check` passed. These targeted results do not
  replace the full runs.
* State-machine reopen passed in a package run (124/124) and in both full runs.
  Tests now use exclusive test directories, a weak-reference release assertion,
  and an explicit directory unlock/drop regression. Descriptor inheritance was
  demonstrated as a possible mechanism, not proven as the original failure's root
  cause.
* After fail-closed forwarded tests, orchestrator-targeted daemon and client tests
  each passed 2/2. S3 GC is disabled. OpenRaft `consistent=true` block reads now
  forward to the project-group leader and read the extent map through
  `try_linearizable_snapshot`; a blob collected between the map read and the fetch
  is a `503`, not zeros. That snapshot is not a GC fence.
* Focused single- and multi-group shutdown cancellation tests pass when the caller
  retains the borrowed owner and retries shutdown. Startup cancellation is now
  supervised: dropping the startup future releases the acquired replica and its
  durable lock, covered by `cancelled_start_releases_acquired_replica` and
  `cancelled_multi_start_releases_first_and_second_replicas`. A Tokio runtime
  shutdown can still cancel the detached cleanup task before it finishes.
* The original compaction stale-file-descriptor claim was false under POSIX:
  explicit reopen is retained, with an append regression only, not proof of a fix
  for that claimed failure.

Authorization freshness is now lease-bound. In OpenRaft mode a node accepts
control state only after a quorum-confirmed linearizable barrier on the current
control-group leader. The leader reads its own applied control snapshot; a
follower fetches the same snapshot from that leader through the internal
`GET /internal/control/snapshot` endpoint authenticated by
`x-yadb-raft-token`. The snapshot is installed before the 2 s lease is extended,
so a fresh lease implies the matching control state is present. The refresher
wakes on a nominal 200 ms interval; after expiry
DB/block/S3/PostgreSQL-wire/iSCSI authorization fails
closed unless an on-demand refresh attempt succeeds. A failed refresh or quorum
loss does not immediately invalidate an existing valid lease; authorization may
continue using its snapshot until expiry. Revocation is not instantaneous
cluster-wide. The 2 s TTL is measured at snapshot acceptance, not at the barrier;
neither a hard 2 s snapshot-age/revocation bound nor a 200 ms revocation bound is
established. The interval is a wakeup cadence, not a completion guarantee, and
the fetch uses per-operation socket timeouts rather than a total deadline. The
narrow-scope independent review is recorded above; the open findings there remain.
`force_leader` remains a routing preference, not a linearizable-read guarantee.
**No production deployment.** Legacy remains the default,
`production_ready: false`, no existing cluster was switched and no migration was
performed; the prior disposable test root was deleted as noted above.

Historically, the deleted deployment passed SQL, S3 and block smoke readback for
alpha and beta through all three endpoints, including a clean stop and reopen.
Endpoint readback did not independently establish replica-local apply or durable
replication. Significant election churn occurred within minutes; no root cause was
proven and runtime stability was not established. Two fake-child cleanup unit tests
passed after the P2 cleanup race fix. Independent Sol tooling review accepted only
that narrow historical empty-loopback smoke and clean reopen; the final combined
review rejected production readiness. The deleted root's evidence is no longer available.

The checklist is authoritative for incomplete gates. A successful command below
does not make this experimental runtime production-ready. Record the exact source
revision/diff, Rust/toolchain version, host OS, filesystem/mount options and device
write-cache policy for release evidence; do not reuse results after relevant edits.

## Offline local regressions

Run from the repository root with an already populated Cargo cache. Do not remove
`--offline`/`--locked` to hide dependency-resolution failures.

```sh
CARGO_INCREMENTAL=0 cargo check --offline --locked --workspace --all-features
YADB_TEST_TMPDIR=/data/data/com.foxdebug.acodefree/cache/opencode CARGO_INCREMENTAL=0 cargo test --offline --locked -j 2 --workspace
YADB_TEST_TMPDIR=/data/data/com.foxdebug.acodefree/cache/opencode CARGO_INCREMENTAL=0 cargo test --offline --locked -j 2 --workspace --all-features
```

`YADB_TEST_TMPDIR` is supported only by tests that explicitly consult it; other
tests still use the platform temporary directory. Check the test implementation
before assuming every artifact is located there. No test command should target
an existing production data directory or credential file.

Targeted probes, not substitutes for the full regression:

```sh
cargo test --offline --locked -p yadb-metakv openraft_storage::tests -- --nocapture
cargo test --offline --locked -p yadb-metakv openraft_state_machine::tests -- --nocapture
cargo test --offline --locked -p yadb-metakv adapted_storage_suite_on_durable_adapters -- --nocapture
cargo test --offline --locked -p yadb-metakv openraft_runtime::runtime_tests -- --nocapture
cargo test --offline --locked -p yadb-metakv openraft_runtime::net_tests -- --nocapture
cargo test --offline --locked -p yadb-metakv openraft_net::reply_tests -- --nocapture
cargo test --offline --locked -p yadb-metakv openraft_tls::tests -- --nocapture
```

The suite adapter uses public-state substitutions and one modified apply input.
Passing it is not equivalent to passing every unmodified upstream assertion.
Treat any failing suite case as a failing regression until the exact contract and
adapter difference are explained and covered; do not silently skip the case.

## Historical orchestration checkpoint (2026-11-14)

* `cargo check --offline --locked --workspace --all-features` passed before the
  current daemon service implementation and later transport/storage edits.
* `cargo test --offline --locked -j 2 --workspace` passed with 102 MetaKV tests
  plus all other workspace tests. This includes the first three-process mTLS
  failover/quorum-loss test and the adapted suite. The process test then had an
  unused task binding; further snapshot/observer/lifecycle edits then required a
  rerun, superseded by the later full gates described above.
* The adapted suite pass includes additional recovery-setup substitutions after
  enforcing rejection of appends into a purged prefix. This is still not an
  upstream-equivalence claim; retain the precise adaptation notes in its source.

These entries describe an intermediate dirty working tree, not a tagged release
or final independent review. No OS/device power-loss qualification has run here.

A subsequent full `--offline --locked -j 2 --workspace --all-features` test run
passed: 109 MetaKV tests, 11 daemon tests and all other workspace tests. It includes
the first bounded-log checkpoint tests, explicit listener drain/byte-budget tests,
snapshot catch-up in the three-process test, and experimental daemon create/status/
shutdown/reopen. Later `YORLOG05` format/serialization changes, local-certificate
preflight validation and the TCP-proxy partition scenario then required rerunning
the integrated gates; see the subsequent results and current status above.

The next full locked all-features run passed with **111 MetaKV tests, 11 daemon
tests and all other workspace tests**. This includes `YORLOG04`, the real
frame-count-triggered checkpoint, local certificate preflight and the directed-
proxy partition test alongside process-kill/snapshot recovery. It predates the
subsequent prepend-continuity, borrowed-shutdown and snapshot-streaming work;
those changes then required new regression results; the latest full gates above
supersede that historical rerun requirement. The existing legacy test still
emits a deprecated `open_clustered` warning. No supported-host power-loss test
or final independent Sol review is implied.

**Historical default-feature regression was red.** A later full
`cargo test --offline --locked -j 2 --workspace` run passed 111 MetaKV tests but
failed `three_process_mtls_failover_and_quorum_loss` at the post-snapshot catch-up
wait. Its artifacts were preserved at
`/data/data/com.foxdebug.acodefree/cache/opencode/yadb-process-14445-1794640851199319831`.
Diagnosis is active; earlier successful targeted/all-features runs do not override
this failure. That build also contained incomplete snapshot-streaming work and
emitted unused-variable/dead-code warnings, so it is not a final combined gate.

After that run, daemon borrowed-shutdown hardening passed
`cargo test --offline --locked -p yadbd --features experimental-openraft-storage experimental_openraft::tests`
with **three tests**, including a deterministic cancellation/retry/reopen case.
The caller retains the owner and listener handle when the cleanup future is
cancelled. This targeted result neither clears the process regression nor
qualifies arbitrary owner drop or startup cancellation.

The orchestrator independently repeated that locked daemon module command with
`YADB_TEST_TMPDIR` and `CARGO_INCREMENTAL=0`: **three passed**, no failures
(0.14 s test runtime). It covers invalid configuration before replica creation,
normal create/shutdown/reopen and borrowed shutdown cancellation/retry. It is not
a final combined workspace result or the independent final Sol review.

Storage hardening's final targeted run passed **21 storage tests** and the
adapted suite after the prepend-continuity fix. Disconnected prepends are
rejected before I/O; contiguous prepends survive checkpoint and reopen. Overlap
comparison uses bounded serialization. The 13 state-machine tests passed before
that final storage-only change. These are targeted results, not a final combined
workspace gate or evidence of unmodified upstream-suite equivalence.

The subsequent orchestrator storage-only run passed **22 tests**, including the
new `purge_missing_prefix_preserves_checkpoint_continuity` regression (0.69 s
test runtime). It used offline/locked Cargo with `CARGO_INCREMENTAL=0` and the
configured test temporary root. One existing legacy `open_clustered` deprecation
warning remains. This targeted pass records the new regression's execution;
contract inspection, final agent report and combined workspace checks remain
separate requirements.

The storage agent's final report confirms that purge markers leaving a gap before
the retained suffix are rejected before I/O. The new regression covers rejected
missing-prefix markers, adjacent/within/beyond-suffix markers and conflicting
retained-log identities, including unchanged files and a usable handle after
rejection. Accepted states survive checkpoint/reopen. After this fix, **22/22
storage tests and the adapted suite passed**. The documented suite substitutions
and final combined verification requirements remain unchanged.

### Local environment observed for these runs

* Rust `1.99.1 (58cf8f9c3 2026-10-13)`, host
  `aarch64-unknown-linux-musl`, LLVM 23.1.1.
* Linux `5.10.136-android12-9-00021-g821df8f5bd36-ab9585204`, aarch64.
* The visible root mount reports F2FS with `fsync_mode=nobarrier`, `lazytime`,
  inline encryption and checkpoint merging. Device/controller write-cache and
  real power-cut behavior were not measured or qualified. Local sync/reopen
  success is therefore not evidence of production power-loss guarantees.
* Source remains an uncommitted working tree; these results are not bound to an
  immutable release commit or independently reviewed source digest.

## What each test class establishes

### Subsequent targeted evidence (2026-11-14)

**An earlier default-feature full workspace gate failed.** With
`YADB_TEST_TMPDIR=/data/data/com.foxdebug.acodefree/cache/opencode` and
`CARGO_INCREMENTAL=0`, `cargo test --offline --locked -j2 --workspace` reported
122 MetaKV tests passed and one failed in 116.92 s. The failure was
`openraft_runtime::multigroup_tests::three_nodes_three_groups_route_independent_leaders_over_mtls`
at line 69: a 15-second timeout waiting for leader 1; node 1's metrics showed
Candidate, term 2, last log 0, applied None and leader None. The earlier targeted
1/1 pass does not establish reliability under the combined workload. Terra is
investigating a possible initialization/immediate manual-election race under
concurrent load; no root cause has been established, and no retry or timeout
increase was used to mask it. Both process tests and the 17 MiB snapshot test
passed in this combined run, but the historical process catch-up failure remains
unresolved.

Subsequently, Terra corrected the test election setup: initialization already
starts an election, so the duplicate immediate manual trigger was removed. Only
the designated bootstrap node has the normal automatic election timer enabled;
follower timers are disabled, avoiding a stranded no-retry configuration. The trace
is consistent with the duplicate trigger but does not prove why quorum was missed.
No timeout increase or test retry loop was added. A subsequent
`cargo test --offline --locked -j2 --workspace` **passed**, including 123/123
MetaKV tests alongside the concurrent 17 MiB snapshot test. The default-feature
daemon integration files ran zero tests, as expected. This run predates final
daemon control-cache/barrier edits, so a combined rerun was then necessary; the
latest full gates above supersede that requirement.
At that checkpoint the all-features gate and independent Sol review were pending;
see current status above for their subsequent results. Production gates and the historical process catch-up
regression remain open.

An earlier all-features verification reached 122/123 MetaKV tests before
failing at `crates/yadb-metakv/src/openraft_state_machine.rs:723` during the
reopen/durability regression. The historical process catch-up failure remains
unresolved. These failures do not establish production readiness or deployment
readiness, and this checklist makes no such claim.

A subsequent package-level all-features MetaKV run passed **124/124 tests**,
including the reopen regression, before a test-helper change. That change uses
exclusive temporary-directory creation and asserts the state-machine `Arc` is
fully released before reopening; reopen failures report the fault and path. The
  prior full workspace all-features gate was **red** at that checkpoint; this package run did not
replace it or resolve the historical process catch-up failure.
After that helper change, the all-features `openraft_state_machine::tests` target
passed **16/16** (108 filtered, 0.36 s test runtime), including the reopen test.
The earlier intermittent lock failure was not reproduced or conclusively explained.

The focused daemon cancellation regression subsequently passed **1/1**:
`experimental_openraft::tests::cancelled_shutdown_retains_owner_and_listener_until_retry`
under `yadbd` with `experimental-openraft-storage`, using offline/locked Cargo and
the configured test temporary root. The two daemon integration binaries ran zero
filtered tests. This confirms the existing single-group borrowed-shutdown
behavior, not arbitrary `MultiRunning` cancellation or startup cancellation.

* The orchestrator ran `YADB_TEST_TMPDIR=/data/data/com.foxdebug.acodefree/cache/opencode CARGO_INCREMENTAL=0 cargo test --offline --locked -j2 -p yadb-metakv openraft_client::tests -- --nocapture`:
  **2/2 passed**, 0.19 s, after correcting the test to use groups 1/2/3.
* With the same environment and Cargo options, the orchestrator ran
  `openraft_state_machine::tests`: **16/16 passed**, 0.73 s. Coverage includes
  durable snapshot watch deltas and removals, no notification on failed writes,
  and subscriptions surviving installation.
* Terra reported `cargo test --offline --locked -p yadb-s3`: **19/19 passed**,
  with no doc tests.
* Terra reported `three_nodes_three_groups_route_independent_leaders_over_mtls`:
  **1/1 passed**. Nine replicas across three groups and three nodes used in-process
  loopback TLS. Group 2 led on node 1 and group 3 on node 2; the test checked
  project-specific `NotLeader`, group 3 writes after stopping group 2's leader,
  and selected state on reopen. It is not a multi-host or multi-process test and
  does not establish replacement election after stopping that leader.
* `cargo test --offline --locked -p yadbd --features experimental-openraft-storage --test openraft_daemon -- --nocapture`
  passed **1/1** in 1.06 s. The child-daemon test builds the binary and uses a v2
  manifest with singleton control group 1 and data groups 2/3. It applies two
  projects over HTTP, writes SQL/S3/block data, verifies underlying state placement,
  then SIGINT-shuts down and reopens for HTTP reads. It checks legacy rejection of
  the marked root, rejection of a changed map and an untouched legacy sentinel.
  This precedes final control-cache hardening and the full suite; it does not
  establish multi-node HTTP behavior or a deployed cluster.
* Subsequently, the full `openraft_daemon` test file passed **2/2** in 2.39 s;
  the new test also passed alone **1/1** in 3.58 s. It runs three actual daemon
  processes with nine replicas across three groups on loopback, bootstrapping
  control on node 1, data group 2 on node 2 and data group 3 on node 3, and
  observing leaders 1/2/3 respectively. SQL and S3 writes for both projects via
  node 1 returned `x-yadb-leader-id` 2/3 as appropriate; underlying placement
  persisted after shutdown. This is targeted nonproduction multi-node HTTP routing
  evidence, not a deployment or multi-node block, PostgreSQL-wire or iSCSI coverage.
  At that checkpoint daemon hardening, combined validation and review remained pending.

These targeted results do not clear the historical process catch-up failure. A test-only setup defect was found:
purging through a fixed index >= 6 did not necessarily pass the stopped follower's
last log index 7. The improved precondition's targeted process tests passed 2/2 in
5.92 s, but a true offline-last-log race is being fixed again; the setup is not
final and this is not a proven fix for the original failure's root cause. The
subsequent default full-workspace gate passed as recorded above, but final combined
verification and independent Sol review were then pending; see current status above.

The subsequent requested target is **actual Multi-Raft client routing**, not the
single-group adapter tested below. The facade now has an explicit control group
and immutable project-to-data-group map, with group-scoped revisions/leader
observations and pre-submission cross-group rejection. Multi-group daemon
configuration had the bounded child-daemon result above but was not then deployed; the
independent-leader test has the limited targeted result above. No successful
singleton result should be read as multi-node verification. Final combined runs
were then pending; see current status above for the completed, non-accepting review.

The extended `yadbd --test openraft_clients` run passed **2/2** in 2.46 s.
Its new three-group case uses control group 1 and data groups 2/3, with projects
1/2 respectively. Both projects perform SQL, S3 and block operations; assertions
inspect their underlying group state, reject a cross-group transaction and
selected snapshot without advancing either group revision, and verify all three
groups after shutdown/reopen plus an untouched legacy marker. Each group is
single-node: this establishes active multi-group routing, not multi-node
replication, daemon integration or resolution of the process snapshot failure.

The new feature-gated `yadbd` engine integration test passed:
`cargo test --offline --locked -p yadbd --features experimental-openraft-storage --test openraft_clients -- --nocapture`
— **one passed**, 0.41 s test runtime. SQL table/index and multi-statement
transaction operations, S3 put/get/list, and block write/read/unmap use the same
real singleton OpenRaft-backed facade and retain their results after shutdown and
reopen. This validates the single-group adapter path, not Multi-Raft routing,
daemon protocol authentication, a deployed cluster or multi-node transport. The
separate process snapshot regression remains unresolved. Cargo emitted local
incremental-cache cleanup permission warnings; no final review is implied.

After adding rate-limited snapshot-phase diagnostics, the locked three-process
test run passed **2/2** in 4.85 s (partition and failover/quorum loss with snapshot
catch-up). One snapshot-client connection failure occurred while node 1 was
offline, followed by recovery. The earlier failure did not reproduce; its root
cause remains unresolved. A later adversarial-budget rerun was blocked by
concurrent borrow-check errors in the unfinished client router, so it is not a
completed combined transport gate.

Signed snapshot-streaming targeted validation passed malformed/stalled stream,
shutdown/permit recovery, TLS rejection, four network integration tests and the
three-process failover/quorum scenario. A 17 MiB transport round trip passed with
explicit 180-second client/server deadlines, taking roughly 100 seconds in debug.
It did not install the snapshot durably or validate large transfers under the
default five-second deadline. This targeted process pass does not explain or
clear the prior intermittent catch-up failure.

Borrowed runtime-shutdown validation passed all 12 runtime module tests, including
cancellation ownership tests. The improved process timeout diagnostics now record
per-child Raft metrics and snapshot-install errors. The preserved failed run shows
node 1 stopped at applied index 7 without a snapshot, while leader 2 snapshotted
and purged through index 9 and node 3 applied index 9. The original artifacts do
not identify the transport/Raft/storage root cause; startup cancellation remains
unproven and no timeout was increased or retry added to mask the failure.

| Class | Evidence | Does not establish |
| --- | --- | --- |
| Volatile singleton/cluster | Command integration, sampled quorum/election/message scenarios | Disk durability, real transport or exhaustive consensus proof |
| File-backed in-process | Real file encoding, reopen and selected snapshot/catch-up paths | Independent-process isolation or OS power loss |
| Injected failure flag | Behavior at the explicitly instrumented write/sync/rename boundary | All partial writes, actual ENOSPC/EIO or arbitrary filesystem reorderings |
| Loopback TCP/mTLS | Actual socket/TLS/signature protocol for tested identities | Firewall isolation, credential lifecycle or hostile-network audit |
| Child process kill/reopen | Acknowledged state survives that process death with the OS still running | Dirty page eviction, device loss or hardware power-cut behavior |
| Multi-process failure suite | Only the tested stop/restart/quorum/failover scenarios | Network partitions unless explicitly injected; arbitrary schedules |
| OS/device crash test | Recorded host/filesystem/device/power-failure scenario | Other hardware configurations or unrestricted power-loss guarantees |

### Durability findings S1/S2/S4/S6/S7 (2026-11-15)

Five defects from the durability audit are fixed on `main`. All evidence below is
in-process; none of it is a power-loss or device qualification.

**S2 — corruption reported as absence (default build).**
`PoolStriper::get_blob` discarded every slot error and answered `NotFound` for
`Mirror` and `InsufficientFragments` for Reed-Solomon, so a blob that was present
and failed its CRC32 was indistinguishable from one never written. The first
`CorruptData` is now retained and returned only when decoding cannot produce a
payload from the healthy fragments. A corrupt mirror copy still reads from a good
replica, a corrupt RS fragment within `k` still reconstructs and is still treated
as missing by `repair_blob` so the scrub rewrites it, and an offline replica is
deliberately **not** promoted (that would turn the consistent-read "map is not a
GC fence" `503` into a `500`). Status mapping is unchanged: internal VDisk `422`,
block and object reads `500` with a corruption reason, `404` still means never
written or deleted. Four tests in `yadb-blob`; 21/21 passed.

**S4 — a torn final OpenRaft log frame prevented the group from opening
(experimental build).** `append_frame` writes and syncs before OpenRaft is told the
append succeeded, so a frame cut short by a crash was never acknowledged and can
be dropped. `recover` now distinguishes `End`, `Torn` and `Complete`: a torn tail is
truncated to the last frame boundary, synced, and reported through `TailRepair`.
A complete frame with bad magic, an impossible length or a CRC mismatch is still
`InvalidData` and leaves the file byte-identical. `partial_write_poisoned_until_
reopen_which_fails_closed` asserted the bug and was replaced by
`..._which_repairs_the_torn_tail`; four tests were added. `yadb-metakv` 124 → 128,
all passing. The residual recorded here at the time — a length field corrupted
*upward* in a non-final frame being byte-identical to a torn tail — is closed by
`YORLOG05` below.

**S4 residual closed — the OpenRaft frame header is self-checking
(`YORLOG05`).** `YORLOG04` frames carried `magic`, `len` and `payload_crc` in 12
bytes, so `len` itself was unprotected: grown past the end of the file it looked
exactly like a torn tail, `recover` truncated there, and every acknowledged frame
after it was discarded while the group reported a successful start. That is the
defect S1 fixed in the legacy WAL and in `yadb-blob` segments, left open in the
metadata log itself. Frames now carry a fourth header word, `crc32(magic||len||
payload_crc)`, and the file magic is bumped to `YORLOG05`, so a pre-fix binary
refuses the file instead of truncating it. `read_frame` validates the header
checksum before it trusts `len`, which makes "the payload never reached disk" the
only way to observe a length past EOF. `yadb-metakv` 149 → 150;
`upward_corrupted_length_in_the_middle_is_refused_not_truncated` asserts
`InvalidData` with a message naming the header checksum and a byte-identical file,
and the format-rejection test now covers `YORLOG02`, `03` and `04`. What this does
not prove: `SimFs`/hand-patched bytes are not real device damage, and no migration
from `YORLOG04` exists, so an existing experimental log must be analysed, never
rewritten in place.

Operational consequence, checked against the live root: all nine
`metadata/*/log/openraft.log` files under `/public/cluster_data` begin with
`YORLOG04`, and a `YORLOG05` binary refuses them before any listener binds. That
is the intended fail-closed behaviour, and it means the deployed binary in
`cluster_data/bin/` (`ca07b340…`) must stay on the pre-`YORLOG05` build until that
root is retired. Reinitialising it to adopt the new format is the silent data loss
this project forbids; the migration path, if one is ever wanted, has to replay
`YORLOG04` and republish as `YORLOG05` in a new file with the old bytes preserved.

**S5 — a corrupt OpenRaft state image is rebuilt from the log, or refused.** A
single damaged byte in `state/state.json` made `DurableMetaStateMachine::open`
fail, so the replica could not start even when its log still held every entry;
the only recourse was a manual `rm`, which is the silent reinitialisation this
project forbids. `open_or_rebuild(dir, identity, coverage)` rebuilds **only** on
`InvalidData` from `read_image`, and **only** when the log provably replays from
the start. `LogCoverage` carries both facts and requires both: `last_purged ==
None` **and** `first_index == 0` (`LogCoverage::replays_from_start`,
`openraft_storage.rs:117-126`). A purge marker alone is not enough, because
`prepare_append` permits a fresh, unpurged log to begin at a snapshot boundary
after `InstallSnapshot`; replaying that into an empty machine would silently lose
the prefix, and `apply` would accept the first entry because `expected` is `None`
when `applied` is `None`. Corrupt bytes are never truncated or rewritten: they are
renamed to `state.json.corrupt` (then `.1`, `.2`, … so earlier evidence survives)
with a directory sync, and the empty image is published through `state.json.tmp`
plus rename plus directory sync. A crash between the rename and the publish leaves
no `state.json`, so the next open fails closed with the evidence intact. A corrupt
image with a purged or front-holed log is `Err(InvalidData)` and the file is
byte-identical afterwards. `yadb-metakv` 149 → 150 covers the rebuild, the
refusal, the numbered evidence files and the empty-log case.

**Startup recovery is reported, not silent.** Both repairs above change durable
state during `open`: a torn tail is truncated and a corrupt image is renamed
aside and replaced. Neither was surfaced outside tests, so a daemon could start
looking healthy after recovering and an operator could not tell a clean start from
a repaired one. `PersistentGroup::open` now records what it repaired in a
`RecoveryReport` (empty for `create` and for a clean open), and `yadbd` prints one
line per repair when a group starts — group, node, discarded bytes, and the path
the corrupt image was preserved as. Tests assert a clean reopen reports nothing
and that an appended partial frame header is reported with the exact discarded
length and truncated durably.

**S6 — degraded acknowledged writes (default build).** `put_blob` acks at
`min_write_quorum()`, which is `k` for RS 4+2, so an ack could leave zero parity on
disk while the failed parts were dropped. Short writes are now queued in
`pending_repairs`, counted in `degraded_puts`, repaired in bounded batches by
`repair_pending` and dequeued only when every fragment is intact. The queue is
drained before the manifest scan and on OpenRaft too, since repairing blobs this
process wrote needs no cross-group metadata view.

The first version of that fix returned `Err` from `repair_blob` when the payload
decoded but a fragment rewrite failed, which made the scrub count a *readable*
blob as `blobs_unrecoverable` — a false alarm that conflates lost data with lost
redundancy. Now `repair_blob` answers `Ok` whenever the payload decoded, with
`intact_parts` counting fragments *after* the attempt; `RepairSweep` and
`ScrubSummary` carry `still_degraded` / `degraded_puts_still_degraded` separately
from `unrecoverable`, and the blob stays queued. Three tests; 24/24 passed. Open:
the queue is in-memory and per process, so a restart forgets it, and the live-blob
scrub path (as opposed to the queue) still counts a readable short blob as intact
when no rewrite succeeds.

**S7 — ambiguous legacy write outcome reported as a definite failure.** The
OpenRaft bridge answered `OpenRaftWriteUnknown` with `503` plus
`x-yadb-write-outcome: unknown` and `x-yadb-retry: do-not-retry`, but
`MetaError::UncommittedIndex` on the legacy path — the same ambiguity — mapped to a
plain `500`. A client could not tell "may have committed" from "rejected".
`meta_err_response` now maps both; `CorruptWal` and `Io` still map to `500` with no
outcome header, asserted by a test so the distinction cannot silently collapse.
Three tests in `yadbd`; 31/31 units passed.

**S1 — silent loss of acknowledged entries on WAL replay.** The legacy WAL reader
treated every frame it could not parse as a torn tail and truncated the file there,
so mid-file damage discarded every acknowledged entry after it while the node
started and reported success. Replay now truncates only when a frame provably runs
past EOF; a complete frame with a bad magic, a bad header or payload CRC, or a
payload that is not a `WalRecord` is `CorruptWal`, and no `set_len` runs on any
corruption path. The v2 header is self-checking, which makes a damaged `len`
distinguishable from a payload that never reached disk; the magic is bumped to
`YADBWAL2` so a pre-fix binary cannot read a v2 log as a tail. `yadb-blob` volume
segments had the same defect in a worse form — truncating at a corrupt record
header dropped later `TOMBSTONE`/`BARRIER` records, which can resurrect deleted
blobs — and now refuse to open. `yadb-metakv` 140/140, `yadb-blob` 26/26,
`yadb-block` 20/20. Open: `SimFs::corrupt_byte` proves the reader's decision logic,
not the shapes a real device leaves after a crash; a `YADBWAL1` file keeps the
legacy ambiguous-`len` truncation for its whole life, because `RaftWal` takes the
format from the file magic and appends in that format — nothing rewrites or
compacts `raft.wal`, so "until it is rewritten" is not a bound that ever arrives
for an existing deployment; and the operator-facing path from `CorruptWal` to a
clear refusal at the `yadbd` boundary is not tested.

| Finding | Tests | What it still does not prove |
| --- | --- | --- |
| S1 | `yadb-metakv` 140/140, `yadb-blob` 26/26 | In-memory byte flips, not real crash shapes; `YADBWAL1` keeps the legacy ambiguity permanently, since no rewrite path exists |
| S2 | `yadb-blob` 21/21 | No real multi-node pool; corruption is injected by flipping a byte in `SimFs` |
| S4 | `yadb-metakv` 128/128 | No real power loss; torn tails are hand-cut bytes or `Fault::WritePartial` |
| S6 | `yadb-blob` 24/24 | Queue is never exercised through a real pool or across a restart |
| S7 | `yadbd` bins 31/31 | Only the HTTP mapping; `Io` after the append stays indistinguishable from `Io` before it |

### Live deployment after S1 (2026-11-15)

`main` at `234d49a` was built with `--features experimental-openraft-storage` and
deployed to the three-process root. Every daemon reported three group leaders and
the same applied index per group on all nodes. What was exercised live, not just in
tests:

* block write at offset 0, three consecutive `consistent=true` reads returning the
  same bytes, and the same read from a `readOnly` subject;
* `POST /block/billing/vm-root/unmap` with a 4 KiB-aligned range zeroing the range,
  and a `400` for an unaligned one;
* `/v1/pools/scrub` returning `degraded_puts_still_degraded`, which is the field
  added by the S6 correction — proof the deployed binary is the reviewed one;
* **restart durability**: a marker written at offset 4096, a clean `SIGTERM` stop
  (`exit_codes [0, 0, 0]`, all ports released), a fresh start, and the marker read
  back byte-identical with the unmapped range still zero.

This is a local single-host run. It does not establish power-loss durability,
multi-host behaviour, or that the OpenRaft log and state images survive a kill
rather than a clean stop.

### Live deployment on `YORLOG05` (2026-11-15)

The `YORLOG05` format was never exercised against a real log before this run: the
previous root's nine logs were `YORLOG04`, which the new binary refuses by design.
A fresh root was provisioned from scratch (`scripts/yadb-local.py prepare`, binary
`28a7fe2f…`) rather than migrated, and the following was observed:

* all nine `metadata/*/log/openraft.log` files begin with `YORLOG05`, i.e. every
  replica was created in the new format and the header word is actually written;
* `/v1/apply` of the sample manifest created the cluster, two pools, the project,
  a database, an object store and a block volume — seven entries committed through
  the new frames;
* a 4400-byte block write at offset 0 read back byte-identical from all three
  nodes, three reads each, once the write had propagated;
* clean `SIGTERM` stop with `exit_codes [0, 0, 0]` and all twelve ports released;
* a fresh start logged **no** recovery line, which is the positive control for the
  `RecoveryReport` path: a clean open must stay silent, or the new warning would
  fire on every start and be ignored;
* after that restart the block read back identically on all three nodes, so
  `YORLOG05` frames replay correctly rather than merely being accepted.

Not established: the same run did not kill the process mid-append, so the
`repaired_tail` warning is still only covered by tests, not by a live torn tail on
disk. The first read after the write returned zeros from the writing node before
the entry had applied locally — the documented "reads are local-applied"
limitation, not a `YORLOG05` effect.

## Required failure record format

For each scenario preserve setup identities, voters, initial applied/log/purge
positions, exact injected boundary, command ID, client acknowledgment or unknown
outcome, subsequent leader/quorum observations, recovered state and retained raw
artifacts. Never label a timed-out proposal aborted: it may commit after healing.
Record both integrity and availability outcomes; fail-closed recovery may preserve
safety while leaving the group unavailable for manual repair.

## Supported-host durability qualification (not available here)

Use disposable, isolated machines/VMs with independently controlled virtual disks
or hardware power. A SIGKILL of the daemon alone is insufficient. Verify the test
actually discards the intended OS/device cache and captures the external client
acknowledgment on a different failure domain. Exercise at least:

* Vote/log/state/snapshot create, partial writes, sync before/after persistence,
  rename, parent-directory sync, truncation, purge and eventual log compaction.
* Kill/crash before and after client acknowledgment; retained suffixes and torn
  tails, corrupt lengths/checksums, real ENOSPC and sync EIO, poisoned-handle reopen.
* Entire-cluster restart, one failed disk, follower snapshot recovery, quorum
  loss/recovery, stale leader return and divergent surviving replicas.
* Repeated cutover/backup-restore failure boundaries only after a migration tool
  implements the specified authority protocol.

The acceptance oracle must compare all externally acknowledged commands, respect
atomic multi-key results and classify unknown outcomes without inventing aborts.
Failing artifacts must be preserved, not repaired/truncated by the test harness.
Publish filesystem/hardware assumptions and unsupported recovery states with the
release. Do not claim these tests ran without actual supported-host evidence.
