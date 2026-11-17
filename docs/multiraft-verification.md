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
**ACCEPT for the narrow local scope only**, with `production_ready`
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

Project-scope idempotency exists for block UNMAP, block write and S3 object
PUT/DELETE. Its live and integration evidence is narrower than the control
scope's. The integration test
`three_daemons_forward_http_to_different_group_leaders` drives the UNMAP path
through the real daemon: an unproven identity gets 401 and a verified non-member
403 before any record exists; the same key in two projects produces two
independent records; a replay returns 200 and the recorded response; the same key
with a different range or a byte-different JSON body of the same range returns
409 `idempotency_key_conflict`; `resolve` with `scope: project` returns
`committed` from the project group; a fenced key returns `not_committed` and a
later keyed UNMAP with it returns 409 `idempotency_key_fenced`; a control payload
carrying `project`, a project payload without it, and an unknown scope all return
400. A keyed write on the single-node metadata store is refused with 501,
because that store keeps no record namespace; the daemon never runs on it.

Block write and S3 PUT/DELETE share the same contract and the same record
namespace and counter, but their evidence is in-process only. Block write
publishes through `BlockVolumeEngine::write_at_with`; S3 PUT/DELETE publish or
retire the manifest through `ObjectStoreEngine::put_object_with` /
`delete_object_with`. In both cases the record and the admission counter are
appended to the *same* guarded `RevisionTxn` as the map/manifest mutation, so a
caller guard that cannot be satisfied leaves neither half. The engine tests prove
that by comparing `mod_revision` of the mutation and the record after a
successful keyed call, and by asserting that a never-satisfiable caller guard
leaves the map/object unchanged and no record behind. A write's immutable
fragments are staged before the publication: a refusal, conflict or crash before
the publication leaves them unreferenced, and the orphan scan reclaims them (for
S3 the `Prepared` intent is the collectable handle). No test injects a crash
between the fragment write and the publication, and the tests assert the
contract, not that a partially written fragment is unreadable.

The S3 replay reproduces the gateway's success shape exactly: 200 with the
`etag`/`size`/`chunks`/`pool` JSON body and the `ETag` header for PUT, and 200
with `{"deleted": …}` for DELETE (the wire protocol wraps the same responses as
200 + `ETag` and 204 respectively). What this does not establish: no real S3
client exercised a replay, no crash was injected between map/manifest publication
and the record (they are one transaction by construction), no concurrent same-key
race across two nodes, no leader loss during the publication, and no quota
exhaustion on a real project group — the counter's ceiling is covered by unit
tests only. The keyed S3 wire path additionally depends on a configured
`--subject-secret` (the gateway mints the internal subject signature from the
SigV4-authenticated identity), which no daemon test exercises.

SQL writes are deliberately still not keyed: `POST /db/{project}/db/query` with
an `Idempotency-Key` and a write script returns 501. A script can mutate any
number of tables, shards and secondary indexes through
`DatabaseEngine::commit_tx_buffer_at_boundaries`, and each of those builds its own
metadata transactions; making the script keyed requires threading the record and
its counter into that transaction. A handler-level record committed separately
from the data would reintroduce exactly the ambiguity the key exists to remove,
so the gap is stated rather than papered over. A SQL write *without* a key is
deliberately still accepted: requiring one would stop `psql`, the pgwire
protocol and every existing script from writing at all, and an unkeyed write is
the pre-existing contract rather than a new hole. The review that asked for a
mandatory key is answered here: the key is opt-in for SQL, mandatory nowhere, and
a client that wants an ambiguous write to be resolvable must use the block or S3
path, or supply the key and accept the 501 that says the guarantee is not there
yet.

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
does not make this runtime production-ready. Record the exact source
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
snapshot catch-up in the three-process test, and daemon create/status/
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
`cargo test --offline --locked -p yadbd --features openraft-storage openraft::tests`
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
`openraft::tests::cancelled_shutdown_retains_owner_and_listener_until_retry`
under `yadbd` with `openraft-storage`, using offline/locked Cargo and
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
* `cargo test --offline --locked -p yadbd --features openraft-storage --test openraft_daemon -- --nocapture`
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
`cargo test --offline --locked -p yadbd --features openraft-storage --test openraft_clients -- --nocapture`
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
(opt-in build).** `append_frame` writes and syncs before OpenRaft is told the
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
from `YORLOG04` exists, so an existing log must be analysed, never
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

**S6 — degraded acknowledged writes (default build).** `put_blob` acked at
`min_write_quorum()`, which was `k` for RS 4+2, so an ack could leave zero parity on
disk while the failed parts were dropped. The first correction recorded those short
writes in an in-memory `pending_repairs` queue, which only made the gap observable:
the write was still acked, the caller still learned nothing at write time, and a
restart forgot the backlog.

The contract is now strict instead of observable. `min_write_quorum()` is every
fragment, `put_blob` returns `BlobError::RedundancyNotMet { written, required }`
when any fragment fails, and the refusal is counted in `refused_puts` on both
`PoolHealthSnapshot` and `ScrubSummary`. The SQL, S3 and block paths map it to
`503`, so a client retries rather than treating data as committed. Nothing is
published for a refused write: no manifest is written, the caller holds no
reference, and the fragments that did land are reclaimed by the orphan scan. The
trade-off is explicit — a pool with a slot offline refuses new writes instead of
accepting unprotected ones, so write availability now follows full redundancy.

`repair_blob` is unchanged in what it means: it still answers `Ok` whenever the
payload decoded, with `intact_parts` counting fragments *after* the attempt, so a
readable-but-short blob is never reported as `blobs_unrecoverable`. Three tests
cover the refusal, the unreferenced fragments, and a fully redundant ack; 26/26
`yadb-blob` tests passed.

This changed what the daemon suite may expect, and that is the point: the first
run of the feature gate failed `three_daemons_forward_http_to_different_group_leaders`
with `503 storage pool cannot reach full redundancy; the write was refused` after it
stopped one of the three replicas. A log probe showed the PUT to the stopped node
failing with `Connection refused`, so the write was genuinely unprotected — the old
contract had acked it. The test now asserts the refusal, that the refused row is not
readable, that the same statement succeeds once the replica returns (a `503` refusal
publishes nothing, so retrying it cannot double-apply), and that the returned replica
then serves the committed row. Open: no test exercises a real multi-node pool, and a
partially written blob stays readable by id until the orphan scan runs.

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
| S6 | `yadb-blob` 26/26 | Refusal is never exercised through a real multi-node pool |
| S7 | `yadbd` bins 31/31 | Only the HTTP mapping; `Io` after the append stays indistinguishable from `Io` before it |

### Garbage collection: bounded mark-and-sweep (2026-11-15)

Superseded block fragments, folded WAL blobs, replaced SSTs, overwritten or
deleted S3 chunks and the fragments of a refused write are now reclaimed on the
legacy metadata runtime. `POST /v1/pools/gc` runs one pass; `POST
/v1/pools/scrub` runs the same pass after its repair scan and keeps every
existing response field, adding `blobs_live`, `fragments_deleted`,
`bytes_reclaimed`, `leases_live`, `gc_refusal`, `gc_pools_refused` and
`gc_errors`. A refusal is `409` with the reason in the body, never a `200` with
zero counts. The leader's periodic repair scan runs every four seconds but only
sweeps every five minutes: a pass holds the exclusive collector lease for its
whole duration, so a per-scan pass would make an explicit `/gc` refuse almost
always.

**Fence protocol.** A collector may only delete a fragment that no committed
revision can still read, and the two ways that can fail are a read that pinned a
revision before the mark and a write that appended fragments before publishing
them. Both are recorded as durable leases in the same metadata group as the
manifests: a reader publishes a 30 s `/gc-lease/reader/<id>` record before it
reads the map and releases it after it has fetched the fragments; a writer
publishes a 300 s `/gc-lease/writer/<id>` record after its allocation and before
its first fragment append, and releases it only after the transaction that names
those fragments returns. The collector reads the whole lease namespace (a record
it cannot parse counts as live), refuses the entire pass while any lease of
either kind is live, takes an exclusive `/gc-lease/collector` record by CAS so
two passes cannot interleave, enumerates what every mounted slot holds, then
re-checks the fence and takes its mark snapshot *after* that check. A read that
began before the mark is therefore either still holding its lease (the pass
refuses) or finished, and a write that began before the mark is either still
holding its lease or has already published the fragments it appended. A read
that starts after the mark is covered by the watermark instead: the pass raises a
durable `/gc/watermark` to its mark revision before it re-checks the fence, and a
reader publishes its lease first and then refuses with `503` if its applied
revision is behind that watermark. Without it, a lagging follower could serve a
manifest committed before the mark — one whose fragments the pass already
deleted — because its local applied view is not the leader's. A node reuses one
reader lease, renewing it at the start of each read, so a `SELECT` does not write
two Raft entries each and a node stops blocking collection one TTL after its last
read. There is no grace period and no "recent enough" rule; a fence that cannot
be read is a refusal.

**Admission is a transaction, not a check.** Publishing a reader or writer record
guards two keys at once: its own never-reused id slot, and the collector record
being absent. A lease therefore either exists before the pass re-checks the fence
or loses its CAS, so no read or write can be admitted between the check and the
first deletion. Every record also carries the `mod_revision` it was published at
as an ownership token: renewal, release and `Drop` are CAS-guarded by it, so a
holder that lost its record cannot extend it, delete it, or delete the record of
the pass that took over. A pass renews its collector record before the watermark,
before its first deletion and before each pool, and stops deleting when a renewal
loses. A write joins its lease's guard to the transaction that publishes the map
or manifest, so a writer that stalled past its lease is refused instead of
publishing fragments a pass has already reclaimed; a read verifies its lease
after fetching and answers `503` if it did not survive, because a pass may have
reaped the expired record and reclaimed what was just read. Since only the
metadata leader can publish a lease, a legacy follower proxies reads to the
leader (carrying the caller's `Range` and conditional headers) instead of
serving them unfenced.
Both the gateway and the native S3 listener proxy on a follower: the native
listener re-authenticates the SigV4 request first, forwards the proven subject
through the internal gateway, and re-frames a gateway error as an S3 XML error,
so the wire format does not change. The node that reads the manifest decides the
read: `If-Match`/`If-None-Match`, existence, the range and the 16 MiB rule are
all evaluated against the one manifest whose fragments are then fetched, on the
leader. A follower's applied metadata can lag, so it never answers those
questions itself; it only forwards. A range end past the object is clamped to the
object size (RFC 9110) by both paths, so a proxied `bytes=7-999` still answers
`206` with `Content-Range: bytes 7-8/9` rather than a `416`, an unsatisfiable
range answers the local `416 InvalidRange` with `Content-Range: bytes */size`, a
failed precondition answers `412`, a matching `If-None-Match` answers a bodyless
`304`, and a read over 16 MiB answers `400 InvalidRequest` before any fragment is
fetched. `Last-Modified` and `Accept-Ranges` come from that manifest and the
internal `x-yadb-leader-id` hop header is removed, so a follower response is the
one a local read would have produced. Conditions are matched as RFC 9110
`*` / `#entity-tag` rather than by comparing the whole header value: `*` matches
the representation (which exists, since a manifest was read), a list is split on
commas outside the quotes, empty list elements (a leading, doubled or trailing
comma) are skipped as RFC 9110 requires of a recipient, and `If-Match` uses
strong comparison while `If-None-Match` uses weak comparison (so `W/"a"` matches
`"a"`). Repeated field lines are combined into one comma-separated value, as
RFC 9110 permits, and both the local evaluator and the forwarded request read
that combined value, so the leader cannot interpret the header differently from
the follower; the bytes are forwarded verbatim, because an `entity-tag` may
contain `obs-text` and a lossy text conversion could name another tag. A value
that cannot be read is not guessed at: `If-Match` is true only on a match, so a
combined `If-Match` that cannot be read answers `412` rather than becoming an
unconditional read (a refusal stays an explicit error, and a well-formed line
does not rescue a malformed one, since the combined value is what both hops
read), while a combined `If-None-Match` that cannot be read names no version
that can be confirmed and so does not suppress the bytes. A stored ETag that is
not a well-formed entity-tag is never repaired into a match. The gateway
`/s3/...` GET applies
the same rule, including the 16 MiB cap, which it did not before: that is a
deliberate change, because a follower's proxied read has to fit the forwarding
wire cap. Native `HEAD` on a legacy follower is the one read that stays local —
it fetches no fragments and takes no lease, so its `ETag`, size and
`Last-Modified` can lag the leader. A
read-only statement whose lease did not survive is refused with `503`; a
statement that committed a write keeps its result even if the fence was lost,
because the mutation is already durable and reporting it as failed would make the
client retry a write that landed — the loss is logged and the lease is not
cached. A request refused before it reads anything (a lost transaction answers
`409`) does not take a reader lease at all, so a client cannot leave live leases
behind by retrying an invalid statement. The
post-commit auto-flush is likewise best-effort: the rows are already durable
through the committed manifest and its WAL blobs, so a flush failure is logged
and retried by a later statement instead of failing the committed transaction.
The legacy forwarder retries only a definite `409 not Raft leader`; a transport
error is retried only for `GET`/`HEAD`, because a lost response to a mutation is
not evidence that it did not commit.

Deletion is per pool: an id is swept only when a mounted slot of that pool holds
it and no manifest names it. Where a channel allocates `(owner, generation)`
from a monotonic per-project counter (S3 channel 4, block channel 5) and every
dead generation in the group is strictly below every live one, the collector
writes one durable generation barrier per slot; elsewhere it writes an exact
per-fragment tombstone, because the SQL channels derive the owner id from the
table name and restart the generation at zero, so a permanent barrier would
fence a recreated tablet. An allocation still in flight cannot be caught by a
barrier: a reservation writes no fragment, so it is never part of the dead set,
and the fragments it later appends exist only while its writer lease is live.
Compaction is the last step: the active segment is
sealed only when it carries garbage and is mostly garbage or large enough to
roll soon, then sealed segments are compacted, so the bytes the barriers freed
actually leave the filesystem. Ordering is durable fence → durable
barrier/tombstone → deletion → compaction, every step is idempotent, and a rerun
recomputes the mark from committed metadata.

**Evidence.** Nine acceptance tests in `crates/yadbd/src/gc.rs`, two handler
tests in `yadbd`'s security suite, and five fence tests in `yadb-metakv`'s `gc`
module (`admission_is_closed_while_a_pass_holds_the_collector_record`,
`a_lost_lease_cannot_release_or_guard_its_successors_record`,
`a_superseded_collector_cannot_renew_delete_or_release_the_record`,
`a_renewed_lease_keeps_its_ownership_and_extends_its_ttl`,
`a_write_whose_lease_was_reaped_does_not_publish` in `yadb-block`). The proxied
read path is covered by `proxy_response_preserves_allowlisted_entity_headers_and_rejects_duplicates`,
`proxy_response_accepts_a_bodyless_304_without_content_length`,
`range_end_past_the_size_is_clamped_but_a_start_past_it_is_refused`,
`a_read_applies_the_local_conditional_contract`,
`a_read_applies_the_local_range_and_size_rules`,
`map_error_frames_gateway_range_errors_as_s3_errors` and
`map_error_frames_gateway_bad_requests_as_invalid_request`. Conditional and
range semantics are covered by `conditional_reads_follow_rfc_9110_matching`,
`a_forwarded_read_carries_its_range_and_conditions`,
`a_forwarded_condition_keeps_every_field_line`,
`a_forwarded_condition_is_evaluated_like_the_local_one` and
`gateway_read_enforces_the_16_mib_cap` (a real 16 MiB+1 object through the
gateway handler: full read `400`, slice `206`, oversized slice `400`, small
object `200`, and an empty object `200` / `416` with `Content-Range: bytes */0`
for a range). Byte
counts are real bytes on disk across the three slots of an in-process mirror
pool, measured before and after one pass:

| Scenario | Before | After | Live set | Fragments deleted |
| --- | --- | --- | --- | --- |
| 16 overwrites of one 16-block region | 9,513,240 | 198,339 | 196,608 | 2,256 |
| Table flush + compaction | 228,765 | 42,369 | 20,739 | 651 |
| S3 overwrite + delete | 789,744 | 267 | 21 | 96 |
| Forced refused write | 24,798 | 12,534 | 12,411 | 3 |

The refused-write case is the interesting one: the write fails for lack of full
redundancy, the fragments that landed are 12,387 bytes of garbage, the pass
reclaims exactly those, and the previously committed bytes still read back. A
crash between the barrier and the compaction is simulated by dropping the volume
after the barrier records are written and the collector lease is left expired;
reopening reaps the lease, the rerun compacts, and the newest generation still
reads back byte-identical. Refusals are covered for a live reader lease, an
unparsable lease record and a held collector lease; each asserts zero fragments
deleted and unchanged disk usage. The non-leader refusal is implemented but not
covered by a test, because it needs a multi-node metadata group.

On a live three-node cluster started by `scripts/yadb-dev.py` (the launcher that
existed then; the runtime and that script have since been removed), eight overwrites
of one 8 KB block region through iSCSI left `node-1`'s pools at 963,124 bytes
against a 897,060-byte floor; `POST /v1/pools/gc` then reported
`blobs_live: 419, fragments_deleted: 42, bytes_reclaimed: 173,418,
barriers_applied: 1` and the directory fell to 905,359 bytes, with the region
still reading back byte-identical and the 413 SQL rows intact. A pass issued
immediately after a read refused with `1 live lease(s) (1 reader, 0 writer)` and
freed nothing, which is the intended "each node must be read-idle for one TTL"
behaviour.

The fence revision was re-run live on a second three-node cluster (ports
22000-22300): a `POST /db/billing/ledger/query` and an S3 GET sent to a
*follower* both answered `200` through the proxy (previously `503 node is not
Raft leader`), an S3 range GET through a follower answered `206` with the exact
`Range` slice, and a 64 KiB block written on the leader read back byte-identical
through a follower. A pass issued right after those reads refused with
`2 live lease(s) (2 reader, 0 writer)`; the same request 40 s later reported
`blobs_live: 107, fragments_deleted: 48, bytes_reclaimed: 198,192,
barriers_applied: 1`, and an immediate rerun reported `fragments_deleted: 0,
bytes_reclaimed: 0, barriers_applied: 0`. After a clean stop/start the 602 SQL
rows, the S3 object and the block contents were all intact, and the first pass
after the restart refused on the two pre-restart reader records until they
expired — no manual cleanup was needed. The same cluster then exercised the
native S3 wire listener: GET and HEAD answered on all three nodes, a native
Range GET through a follower answered `206` with `Content-Range: bytes 0-4/11`,
and a missing key on a follower answered `404` with the S3 XML error rather than
the gateway's JSON. Two block overwrites plus those reads left one live reader
lease, the pass refused, and the next pass reclaimed 96 fragments / 396,483
bytes with the block, the S3 object and the 602 rows still intact. A third run on
the same cluster closed the review findings: a native follower GET returned the
same `200` body, `ETag`, `Last-Modified` and `Accept-Ranges: bytes` as the leader
and no internal `x-yadb-leader-id`; a failed `If-Match` answered `412
PreconditionFailed`, a matching `If-None-Match` answered a bodyless `304` with no
`Content-Type`, `Range: bytes=900-999` on the 11-byte object answered `416
InvalidRange` with `Content-Range: bytes */11` on both leader and follower, and a
missing key answered `404 NoSuchKey` with `<Message>object not found</Message>`
instead of the gateway's JSON envelope. The same conditions on the gateway path
(`/s3/...`) answered `304`, `412` and `416` with the same `Content-Range`, so the
gateway and the native listener now share one decision. Three
`require_transaction` queries with an unknown session answered `409` and left
`leases_live: 0`, so a refused statement no
longer parks a reader lease. After one 64 KiB block overwrite plus reads through
all three paths the pass refused on `2 live lease(s) (2 reader, 0 writer)` and
the next pass reclaimed 48 fragments / 198,291 bytes with the block, the S3
object and the 602 rows intact. A fourth run checked RFC 9110 matching on both
the leader and a follower: `If-None-Match: *`, `If-None-Match: W/"630a9e70-11"`
and `If-None-Match: "old", "630a9e70-11"` all answered a bodyless `304`, a
non-matching `If-None-Match` answered `200` with the bytes, `If-Match: *` and
`If-Match: "old", "630a9e70-11"` answered `200`, and `If-Match: W/"630a9e70-11"`
(weak) and a non-matching `If-Match` answered `412`; the gateway `/s3/...` path
agreed on all of them. A fifth run checked the list and field-line edges on the
leader, a follower (so the forwarding path) and the gateway: `If-Match: "other",`
and `If-Match: , "other"` answered `412`, `If-None-Match: "<etag>",` and
`If-None-Match: "other", , "<etag>"` answered a bodyless `304`, an unparsable
`If-Match` answered `412` while an unparsable `If-None-Match` answered `200`,
and two separate `If-Match`/`If-None-Match` field lines were honoured when either
line matched (`200`/`304`). A sixth run checked the combined-value policy that
the forwarding hop now shares: a well-formed line plus `junk` in the same header
answered `412` for `If-Match` (including `If-Match: *` plus `junk`) and `200`
for `If-None-Match`, identically on the leader, a follower and the gateway, and
the trailing/doubled-comma cases still answered `412`/`304`. A pass issued right
after those reads refused with
`1 live lease(s) (1 reader, 0 writer)` and the next pass completed cleanly with
the object and the 602 rows intact. Gates on this tree: `yadbd --bins` **76/76**,
`yadb-metakv` **169/169**, `yadb-block`
**25/25**, `yadb-db` **46/46**, `yadb-s3` **26/26**, `cargo fmt --all --
--check` clean, `tests/protocols.py` **16/16** and
`tests/protocols.py --native-clients` **17/17** on the legacy three-node
cluster.

**Open.** The collector is legacy-only. On Multi-Raft both endpoints return
`409` with `MULTIRAFT_GC_REFUSAL`: a writer lease can be renewed while a
collector pass is in flight, and collector RPCs can continue past lease expiry,
so a reader lease alone is not a sufficient fence. The per-group collector
described in "Per-group garbage collection on Multi-Raft" below was implemented
and then disabled for the same reason; storage-side fencing is not implemented,
so garbage accumulates. The legacy behaviour in this section is unchanged. A
pass needs each node read-idle for one lease TTL (30 s), because a node keeps
its reader lease for reuse instead of deleting it per statement; a
continuously busy node refuses instead of waiting. A read that
outlives its own lease is not refreshed mid-read, so a query longer than 30 s is
refused with `503` after it finishes rather than returning bytes a pass may have
reclaimed; the client must re-issue it. `/s3-retired/` records are kept
indefinitely; the 300 s writer TTL is the only bound on a crashed writer blocking
collection; and the tests use an in-process pool, so remote slot enumeration,
remote barriers and remote compaction run through the same code paths without a
second host. The metadata Raft log is bounded separately by the checkpoint plus
compaction item in the tracker.

### Live deployment after S1 (2026-11-15)

`main` at `234d49a` was built with `--features openraft-storage` and
deployed to the three-process root. Every daemon reported three group leaders and
the same applied index per group on all nodes. What was exercised live, not just in
tests:

* block write at offset 0, three consecutive `consistent=true` reads returning the
  same bytes, and the same read from a `readOnly` subject;
* `POST /block/billing/vm-root/unmap` with a 4 KiB-aligned range zeroing the range,
  and a `400` for an unaligned one;
* `/v1/pools/scrub` returning the S6 correction's field, which is how the deployed
  binary was shown to be the reviewed one;
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

### Renamed OpenRaft surface (2026-11-15)

The `experimental` prefix was removed from the OpenRaft names: the build feature is
`openraft-storage`, the daemon module is `openraft`, the flag is `--openraft-config`
and the status route is `/openraft/status`. A separate disposable root provisioned
from that tree started with `--openraft-config`, `scripts/yadb-local.py verify`
passed on all three nodes with `metadata_runtime: openraft`, the status JSON no
longer carries an `experimental` field, and `GET /experimental/openraft/status`
returns `404`. The cluster is still `production_ready: false`; the rename changes
no runtime behaviour and no routing identity.

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
* Repeated cutover failure boundaries only after a migration tool implements the
  specified authority protocol.

The acceptance oracle must compare all externally acknowledged commands, respect
atomic multi-key results and classify unknown outcomes without inventing aborts.
Failing artifacts must be preserved, not repaired/truncated by the test harness.
Publish filesystem/hardware assumptions and unsupported recovery states with the
release. Do not claim these tests ran without actual supported-host evidence.

## Metadata WAL compaction (2026-11-15)

The legacy Raft log is checkpointed and compacted. `metakv/checkpoint.json`
carries the durable state image plus `applied_index`, `applied_term`,
`commit_index`, `hard_state`, `compaction_floor` and a format version; it is
written tmp/`sync_data`/rename/`sync_dir`. `raft.wal` is rewritten the same way,
always beginning with a `HardState` frame and then `WalRecord::Base{index, term}`.
`RaftCore` addresses the log through `base_index`/`base_term`, and compaction is
bounded by a monotone `compaction_floor` replicated in `AppendEntries` with
`#[serde(default)]`.

Ordering: checkpoint, then WAL rewrite, then the in-memory base. On open the
checkpoint is authoritative; a WAL whose base is older is rewritten to match, so
a crash between the two renames is harmless and idempotent. A `Base > 0` with a
missing or unparsable checkpoint, a checkpoint below the WAL base, and a
checkpoint term disagreeing with the base term all refuse to start.

Tests (`cargo test -p yadb-metakv`, 177 tests):
- `compaction_bounds_the_wal_and_preserves_state_across_reopen`: 51 puts, forced
  threshold, base advances, WAL shrinks, every key reads back, reopen reproduces
  base/floor/revision and does not append.
- `checkpoint_renamed_before_the_wal_rewrite_is_reconciled_on_open`: the crash
  window; the stale WAL is rewritten on open, state and revision are identical,
  and the file shrinks.
- `a_compacted_base_without_a_checkpoint_refuses_to_start`,
  `an_unparsable_checkpoint_refuses_to_start` and
  `a_checkpoint_from_another_membership_refuses_to_start`: explicit `Checkpoint`
  errors.
- `term_and_vote_survive_compaction`: the rewritten WAL itself carries the hard
  state, so replay cannot rewind the term.
- `compaction_never_drops_an_entry_above_a_stopped_peer_match`: 3-node cluster,
  one peer stopped, leader's base and floor stop at that peer's match, and the
  peer catches up after healing.
- `a_peer_below_the_compacted_base_is_sent_the_durable_image`: the leader sends
  one `InstallSnapshot` chunk with the base index/term, the group id and the
  membership instead of refusing, while the follower still refuses an append
  whose prefix is below its own base.
- `a_replica_that_lost_its_log_rejoins_through_the_durable_image`: a replica
  replaced with an empty directory gets the image, publishes
  `checkpoint.json`, has its WAL rewritten, and then receives the entries above
  the base through the ordinary log.
- `a_snapshot_from_another_group_or_membership_is_refused`: wrong group id and
  wrong membership are refused with the state and files untouched.
- `a_corrupted_or_stale_snapshot_is_refused_without_publishing`: a flipped byte
  fails the whole-image checksum, a retried intact image still installs, and an
  image below the base or with another term is refused.
- `a_snapshot_transfer_resumes_without_duplicating_bytes`: 32-byte chunks, a
  retransmitted chunk keeps the offset, an out-of-order chunk is refused with
  the offset the receiver still needs, and the resumed transfer completes.
- `an_install_interrupted_by_a_power_cut_leaves_the_old_state`: two chunks, a
  power cut, and the reopened node keeps base 0 with no partial image.
- `a_crash_after_publishing_the_image_keeps_the_new_base`: the rename reaches
  the disk and the following sync fails; the in-memory base does not move, the
  published image survives the power cut, and the reopened node adopts it and
  rewrites its log.
- WAL layer: `base_marker_replays_and_rewrite_preserves_hard_state`,
  `rewrite_replaces_the_whole_file_and_shrinks_it`, `a_gap_above_the_base_is_corruption`,
  `an_append_below_the_base_is_corruption`.

Live measurement on a three-node legacy cluster (ports 21000-21300, fresh root):
4801 rows written through pgwire in three runs, 19491 Raft entries. `base_index`
advanced to 18506 and `compaction_floor` to 19491; each node's `raft.wal` stayed
in a 3.7-8.4 MB band (about 3.8 KB per entry) instead of growing with the entry
count, and `checkpoint.json` settled at 56 KB. A `stop`/`start` cycle recovered
all 4801 rows with no WAL growth; a node killed with `SIGKILL` while the other
two kept writing was refused for writes (full redundancy is required) and caught
up from the leader's retained suffix when restarted. `tests/protocols.py` passed
16/16, `cargo test -p yadbd --bins` 63 tests, `cargo fmt --all -- --check` clean.

A peer behind the compacted base is re-seeded from the same checkpoint image:
the leader streams it in 64 KiB `InstallSnapshot` chunks (64 MiB bound,
resumable per group/leader/snapshot id), the receiver validates group,
membership, index/term and whole-image checksum, publishes with one atomic
rename, rewrites its log, and only then moves its applied pointer; a crash
leaves the old or the new base. Open: the checkpoint is a full image rather
than incremental history; compaction runs under the Raft mutex, so a pass
briefly stalls proposals; a single-node cluster bounds itself by its own
applied index; and a refused install retries from offset 0 on the next
heartbeat instead of remembering the failure.

## Startup readiness without a control image (2026-11-16)

### Defect

A restarted node bound its client listeners before its control watcher had
installed the persisted image. In that window `ControlPlaneState` was empty
(`generation == 0`), `resolve_s3_target` saw no project, and the node answered
native S3 and gateway requests with `403 AccessDenied` ("project access
denied"). A `403` is terminal for S3 clients: the same request succeeded about
200 ms later, but the client had already given up. The window was reproduced on
the three-node root by restarting one node under a GET loop.

### Fix

`ControlAuthError` now distinguishes `Stale` (the existing quorum-freshness
refusal) from `NotReady` (no image applied yet). `AppState::authorization_snapshot`
returns `NotReady` while `generation == 0`, and each frontend maps it to a
retryable answer:

* native S3: `503 ServiceUnavailable`, message `control plane has not applied an
  image yet; the node is not ready`;
* gateway/HTTP: JSON `503` with the same message;
* pgwire: `57P03` (`cannot_connect_now`);
* iSCSI login and discovery: login status class 3 (target error), detail 0 —
  the initiator retries instead of caching an empty target list. This covers a
  stale control quorum as well as an uninstalled image, so a transient control
  outage is never reported as an authentication failure.

`/v1/apply` and `/v1/status` are not gated, so bootstrap and diagnosis still
work on a node with no image. Once an image is installed the historical `403`
(stranger) and `404` (absent object) answers are unchanged; the new unit test
`an_unapplied_control_image_is_reported_as_not_ready` asserts both directions.
The GC fixture was given `generation = 1`, because it describes a configured
cluster.

The test harness also gained `wait_leader()`: a restarted node binds its
listeners before Raft has elected a leader, and a read forwarded in that window
is correctly refused with "known leader: None". Waiting for the election
removes that race rather than retrying the read.

### Live evidence

One node of the migrated three-node root was restarted and hammered throughout
the restart:

* native S3 GET churn, 63 requests in 2.5 s: 27 connection refusals (listener
  not bound), 5 readiness `503`s, 31 `200`s, **0 `403`s**;
* pgwire, 170 attempts: 35 refusals, 2 `57P03`, 133 successful logins, **no
  permission error**;
* 60 concurrent 1 MiB PUTs to the restarting node: 29 refusals, 1 `503`, 30
  `200`. Every acknowledged key read back byte-identical and every non-`200`
  key was absent (`404`), before and after a collection pass — no partial
  object, no acknowledged-then-missing write.

A leader `SIGKILL` during an 8 MiB PUT through a follower left the client with
`503`, the object absent, and node 3 as the new leader (term 47). Node 1
returned, all three converged at revision 2046 / commit 2059 with
`compaction_error: null` on every node, and the 1002 SQL rows, the 1 MiB and
8 MiB objects and the surviving race keys were intact after GC.

The aborted 8 MiB PUT left a prepared `/s3-intent/` record naming eight
reserved chunks; a GC pass counts intents as live, which is why the pass
immediately after the kill reported `blobs_live: 199` against the 191-blob live
set. Expiry is enforced by the collector: after the 300 s / 32-revision grace,
the leader's periodic maintenance (the same `scrub_all_pools` pass, every four
seconds, with collection due every 300 s) aborts an expired intent and then
deletes its record and any unreachable chunks. The explicit
`POST /v1/pools/scrub` that followed reported `orphan_blobs_collected: 0` with
no error, and its own sweep already found `blobs_live: 191`: the expired intent
and its eight reserved chunk ids were no longer part of the live set, and no
fragment remained. The next explicit GC reported `blobs_live: 191`,
`fragments_deleted: 0`, `bytes_reclaimed: 0` again, and the repair scan checked
191 blobs and found 191 intact. `dev_load` still had 1002
rows, `accounts` 602, the 1 KiB, 1 MiB and 8 MiB objects and the probe were
byte-identical, and the 60 race keys still split 30 present / 30 absent.

### Gates

`cargo test -p yadbd --bins` 77/77; `yadb-metakv` 169/169 (one transient
failure when run in the same process group as the other crates did not
reproduce solo); `yadb-block` 25, `yadb-db` 46, `yadb-s3` 26, `yadb-control` 1;
`cargo fmt --all -- --check` clean. `tests/protocols.py` passed 16/16 four
times in a row and `--native-clients` 17/17 with the readiness gate in place.

Open: the readiness gate keys on `generation == 0`, so a cluster whose most
recent manifest set is empty by construction would report `503` forever; the
test cluster always applies at least the platform manifest. A node that has an
image but whose control snapshot is corrupt takes the existing fail-closed
stale path, not `NotReady`.

## pgwire COPY/TLS and forwarded-outcome classification (2026-11-16)

### COPY in text format

`COPY <table> [(columns)] FROM STDIN`, `COPY <table> [(columns)] TO STDOUT` and
`COPY (<select>) TO STDOUT` are implemented in the pgwire frontend. A `COPY`
load is typed by the target table's declared column types, read with the new
`SHOW COLUMNS FROM <table>` statement, so a text column whose values look
numeric stays text. Rows are sent through the existing gateway transaction path
as batched `INSERT`s (500 rows or 32 KiB), and the whole load is one
transaction when the client is not already inside one. Server-side files and
every option other than `FORMAT text` are refused with `0A000` instead of being
ignored.

Live evidence on a three-node legacy cluster (port base 24000): `\copy` of a
4-row file with a tab, an escaped newline, a backslash, an octal and a hex
escape and a NULL loaded and read back with the expected values; `\copy ... to`
produced `1\talpha\tt\n2\tline\nbreak\tf\n...` byte for byte; `COPY (SELECT ...)
TO STDOUT` streamed the same escaping; 5000 rows loaded in one `\copy` and read
back (`row-3999` for id 4999); inline `COPY ... FROM STDIN` terminated with `\.`
loaded 2 rows. Failures rolled back completely: a non-integer id, a non-boolean
value, a duplicate primary key and a short row each left the table unchanged
(0 rows for the refused id), and `BEGIN; \copy ...; ROLLBACK` left no row. A
`COPY` combined with another statement in one query is refused with `0A000`.

### TLS and mTLS

`--pg-tls-cert`/`--pg-tls-key` (PEM or DER) make the listener answer
`SSLRequest` with `S`; `--pg-tls-client-ca` requires a client certificate;
`--pg-require-tls` refuses cleartext startup. Without the flags the listener is
byte-for-byte the previous cleartext one, so `sslmode=prefer` keeps working.

Live evidence on the same cluster: `psql` negotiated TLSv1.3
(`TLS_AES_256_GCM_SHA384`), `sslmode=verify-full` with the configured CA
connected, a client certificate from a foreign CA was rejected with `unknown
ca`, a missing client certificate with `certificate required`, and
`sslmode=disable` against `--pg-require-tls` was refused with `28000`/`TLS is
required by this server`.

### Concurrent proposals: a Leader now waits for its own entry

The concurrent S3 PUT burst exposed two independent defects, both of which
refused writes that were committing normally.

`RaftCore::propose` returns the replication messages it built, and the old
`LocalMetaKv::execute_with` sent them once and immediately looked for the
result. When several threads propose on the same Leader, a later proposer
builds messages whose `next_index` already covers an earlier proposer's entry,
so that earlier `propose` returned no messages at all and its entry was never
acknowledged by the proposer itself. `execute_with` then read
`take_applied_result(idx)` before any round trip, found nothing and answered
`MetaError::UncommittedIndex` — reported to the client as
`no confirmed result for proposal at log index N; write outcome is unknown`.
Four of twenty-four concurrent PUTs failed that way.

`execute_with` now waits for the entry it appended: it re-checks the result,
issues a heartbeat round so a peer that already holds the entry acknowledges
it, and stops only when the entry is applied, its term no longer matches the
proposed `(index, term)` (a new leader replaced it), the budget expires, or
three consecutive rounds reach no peer at all (no quorum to wait for, so the
refusal is immediate rather than a full-budget poll of refused connections).
The wait is not a retry of the command: the entry is already in the log and can
commit at most once, so the wait only decides whether the client sees the
committed result or an explicit unknown outcome. `COMMIT_WAIT` is 3 s, below
the forwarding Follower's reply budget so the Leader answers first. A unit test
injects a first round where both peers time out and asserts the write still
returns `PutOk`.

### Forwarded writes: budget and outcome classification

The second defect was the Follower's reply budget. `proxy_http_to_peer` used a
single fixed 5 s `read_timeout` for every request. Measured on a three-node
local cluster, one SQL statement costs ~130 ms and one S3 PUT ~160 ms: the
engine serializes statements behind a single lock and each metadata mutation
costs a synchronous two-peer Raft round. A 24-way burst through a Follower
therefore needs 4–5 s, so the 5 s budget intermittently expired on writes that
were committing normally and reported them as
`reading leader response failed: Resource temporarily unavailable (os error 11)`
with `x-yadb-write-outcome: unknown`.

The budget is now split: `FORWARD_WRITE_REPLY_BUDGET` is 20 s and
`FORWARD_READ_REPLY_BUDGET` stays 5 s. This is not a retry policy — an expired
budget still reports `unknown`/`do-not-retry` and the request is never resent —
so the larger write budget only widens the window in which a slow success is
reported as a success. Reads keep the short budget because a read that is
merely slow can be failed over and retried.

`proxy_http_to_peer` also used to return one `io::Error` for every failure, so
a refused connect — where the leader provably never received the request — was
reported with the same `unknown`/`do-not-retry` as a lost reply. The helper now
returns a `ForwardFailure` that records whether any byte was written: connect
and socket-setup failures are `not-sent` and are retried inside the existing
attempt loop, failures after the first write stay `unknown`/`do-not-retry`, and
a final refusal after only `not-sent` failures reports `not-sent`. Two unit
tests cover a closed port (`not-sent`) and a peer that accepts the request and
closes without answering (`unknown`).

After both fixes, `tests/protocols.py` passed 16/16 on five consecutive runs
and `--native-clients` 17/17; a direct 24-way burst measured 1.5 s wall time
with no failures through both the Leader's and a Follower's S3 listener.

### Gates

`cargo test -p yadbd --bins` 83/83; `cargo test -p yadb-metakv --lib` 170/170;
`cargo test -p yadb-db --lib` 46/46; `cargo fmt --all -- --check` clean;
`tests/protocols.py` 16/16 on five consecutive runs and `--native-clients`
17/17.

Open: `COPY` supports only the text format, has no `FREEZE`/`WHERE`/CSV/binary
options, and its implicit transaction still goes through the gateway's
`BEGIN`/`COMMIT` rather than a dedicated bulk path, so a large load costs one
HTTP round trip per batch. A partially written forwarded request counts as
`sent`, which is the conservative choice. The Leader's per-statement
serialization and one-Raft-round-per-metadata-mutation design means throughput
on one Leader is bounded by that round trip; the 20 s forwarding budget is sized
for the 24-way burst this test exercises, not for an unbounded queue.

## Live three-process Multi-Raft run (2026-11-16)

A fresh root was provisioned and run with `scripts/yadb-openraft-dev.py` at port
base 23500: three daemon processes, each hosting control group 1 and data groups
2 and 3 (three replicas per group), mutual TLS, `--metadata-runtime openraft`,
and the PostgreSQL, S3 and iSCSI listeners enabled. Every node reported
`metadata_runtime: openraft`, `client_traffic: multiraft` and
`production_ready: false`. `metadata_runtime` names the runtime, and the daemon
emits only `openraft` or `legacy` there; `multiraft` is the topology
(one control group plus data groups) and appears only in `client_traffic`.
Initial leaders were g1 = node 1, g2 = node 2,
g3 = node 3 at term 1.

### Control plane

A signed `POST /v1/apply` of `examples/platform.yaml` returned HTTP 200 with
generation 1; a second, different manifest returned HTTP 200 with generation 2.
All three nodes served the same `/v1/status` (`generation: 2`, projects `billing`
and `beta`) and the same control-group leader.

### Protocols

* PostgreSQL through pgwire (`psql` on 23700/23701/23702): `CREATE TABLE`,
  `INSERT` ×3 and `SELECT count(*), sum(id), min(note)` returned `3|6|alpha` on
  every node.
* Native S3 through the gateway on 23800/23801/23802: a 64 KiB PUT and GET were
  byte-identical on all three nodes (`cmp` clean, SHA-256 prefix `de3f3404`).
* Block: a 4 KiB PUT returned `{"bytes_written":4096,"offset":0}`; the
  consistent GET on 23500/23501/23502 returned 200 with matching bytes. Writes
  to a follower returned `x-yadb-leader-id` naming the data-group leader.
* iSCSI through the target on 23900/23901: CHAP login reached full feature phase,
  `WRITE(16)` and `READ(16)` both returned SCSI status `0x00`, and the 4096 bytes
  read back were identical to the written pattern. A kernel initiator was not
  available (`iscsi_tcp` could not be inserted), so the initiator was a
  hand-written RFC 7143 login/SCSI client; this proves the target's wire
  behaviour, not kernel-initiator interoperability.

### GC and scrub refusal

With Multi-Raft metadata, `POST /v1/pools/gc` and `POST /v1/pools/scrub` return
**409** with `gc_refusal` set to `MULTIRAFT_GC_REFUSAL`: a writer lease can be
renewed while a collector pass is in flight and collector RPCs can outlive their
lease, so a reader lease alone does not fence collection. Both report zero
deletions and zero reclaimed bytes and leave fragments intact.

(At `234d49a` the reason was `garbage collection disabled for Multi-Raft: no
single atomic cross-group metadata snapshot`, and scrub answered 200. The
per-group collector described below replaced that, then was itself disabled;
current behaviour is the fail-closed refusal above.)

### Failure drills

| Drill | Observed |
| --- | --- |
| Graceful restart of node 1 (control leader) | g1 re-elected node 2 at term 2 within one poll; after node 1 returned it followed node 2. SQL, S3 and block readback unchanged. |
| `SIGKILL` of node 2 (g2 leader) | g2 had no leader for one poll, then elected node 3 at term 2. Writes through nodes 1 and 3 returned `503 "storage pool cannot reach full redundancy; the write was refused"`; consistent reads returned 200 with matching bytes. |
| One node down (reads vs writes) | Reads kept serving from the remaining replicas; writes were refused with the documented `503` full-redundancy error instead of being accepted with fewer copies. |
| `SIGKILL` of a leader during a 256 KiB block write (20 ms in) | The client got a closed connection (unknown outcome, no false success). After recovery the target range was entirely the old value: no torn bytes. |
| `SIGKILL` of a leader during a 256 KiB block write (100 ms in) | The client got `503 "OpenRaft forwarding failed; write outcome unknown, do not automatically retry"`. After recovery the range was again entirely the old value. |
| Full process-tree restart (all three killed externally) | `start --force` reopened every group from disk (`mode: open`), re-elected leaders, and reported `generation: 2`. SQL `3|6|alpha`, the 64 KiB S3 object and both block ranges were intact. |
| Keyed block write + replay | Two `PUT`s with the same `Idempotency-Key` both returned 200 with the recorded body, and the volume changed once. |

### Defects found and fixed by this run

1. **pgwire/iSCSI bridge sent an unsigned subject.** `protocols::http` forwarded
   `x-yadb-subject` without a signature, so every session authorized as
   `anonymous` and got `403 access denied for 'anonymous' on project 'billing'`.
   The bridge now signs with the daemon's subject secret; call sites in
   `protocols/pg.rs` and `protocols/iscsi.rs` pass it.
2. **SQL writes panicked with "multi-group metadata has no global revision".**
   `gc::acquire_writer_if_legacy(..., self.meta.revision(), ...)` evaluated
   `revision()` eagerly, and `revision()` asserts on multi-group metadata even
   when the legacy branch is not taken. Added `LocalMetaKv::legacy_revision()`
   (returns 0 on OpenRaft) and used it in `yadb-db` and `yadb-block`.
3. **S3 gateway returned `403 access denied for 'anonymous'`.** The
   `x-yadb-subject-signature` header was only added for keyed PUT/DELETE, so an
   unkeyed PUT/GET proved no subject. The SigV4-verified subject is now always
   signed.
4. **Plain S3 reads panicked on Multi-Raft.** `handle_s3_get` took the legacy GC
   reader lease unconditionally, and `begin_read_lease` calls
   `meta.revision()`, which asserts on multi-group metadata. The SQL and block
   read paths already skipped the lease when `meta.is_openraft()`; the S3 path
   now does too, and skips the corresponding release. Without this, a
   non-`consistent=true` S3 GET answered `500 task ... panicked with message
   "multi-group metadata has no global revision"`.
5. **A stale daemon-test assertion.** `openraft_daemon` expected `501` for a
   keyed block write, from before keyed block writes were implemented. On the
   base commit the same assertion saw `500` (defect 2); with defect 2 fixed the
   request correctly returns `200`. The test now asserts the implemented
   contract: 200, then a replay that returns the recorded response.

### Gates

`cargo fmt` clean; `cargo test -p yadbd --bins` 77/77;
`cargo test -p yadbd --features openraft-storage --test openraft_daemon` 4/4
(the daemon test suite failed at `ff335e1` at the keyed-block-write assertion,
returning 500 instead of the expected 501).

### Open items

* Per-group GC on Multi-Raft is refused by design and not implemented; the
  refusal is the contract, not a gap in this evidence.
* `production_ready` stays false: this run proves the runtime works on three
  processes, not that the durability and authorization gates for production are
  met.
* After the kill drills all three groups ended up led by node 1, so the soft
  leadership preference for groups 2 and 3 was not met. That is allowed and
  documented; a preferred node does not reclaim leadership automatically.
* A kernel iSCSI initiator was unavailable in this environment, so iSCSI was
  exercised with a hand-written initiator.

## Multipart S3, consistent reads and the merged-tree run (2026-11-16)

This run is on merged `main` after the pgwire/COPY block and the Multi-Raft
block were integrated. Root `/data/data/com.foxdebug.acodefree/cache/opencode/yadb-or-merge`,
base 23500, same layout as the run above; the cluster was stopped after each
drill and its data kept.

### Multipart uploads

`POST /bucket/key?uploads` initiates, `PUT ...?partNumber=N&uploadId=...` uploads
a part, `GET ...?uploadId=...` lists, `POST ...?uploadId=...` completes and
`DELETE ...?uploadId=...` aborts. Parts are ordinary durable blob-pool
allocations referenced by a `multipart: true` intent committed before any
fragment is written, so the object is never buffered in memory. Completion is a
single `RevisionTxn` that publishes the manifest, retires the previous one,
deletes the upload record and deletes every part intent; a lost guard answers
`503` with `x-yadb-retry: do-not-retry` rather than retrying a publication whose
outcome is unknown. A wrong ETag (`InvalidPart`), a repeated or descending part
number (`InvalidPartOrder`), a malformed document (`MalformedXML`) and an
unknown upload (`NoSuchUpload`) are refused without consuming the upload.

Live evidence with the real AWS CLI (2.22.10, `--only-show-errors`), a 40 MiB
random file uploaded in five 8 MiB parts **through each of the three native S3
listeners in turn** (two of them are followers, so the requests were forwarded):

| Listener | Result |
| --- | --- |
| 23800 (node 1) | 41943040 bytes, ETag `"5da9de09…-5"`, download SHA-256 identical |
| 23801 (node 2) | same size and ETag, download SHA-256 identical |
| 23802 (node 3) | same size and ETag, download SHA-256 identical |

The upload predates a full cluster stop/start; after the restart a
`?consistent=true` range read of it returned the same bytes on all three nodes,
so the completed object survived the restart and not only its metadata did.

### Consistent reads through the native listener

`?consistent=true` is the opt-in quorum-confirmed read; without it a read is the
documented local applied-state read. The gateway route supported the parameter,
but the native listener rejected it as an unsupported subresource and, worse,
answered a plain read from local state — exactly the stale read the option
exists to refuse. Observed: a PUT acknowledged through one node, then a
`?consistent=true` GET on the node that had not applied it answered `404`.

The listener now accepts the parameter and delegates to `handle_s3_get`, which
takes the snapshot on the project-group leader; legacy metadata still refuses
with `501 consistent reads require OpenRaft metadata` (asserted in
`tests/protocols.py`). The delegation initially dropped `Range`,
`If-Match` and `If-None-Match`, so a consistent read of a 40 MiB object answered
`400 use Range for objects over 16 MiB` even though a range was supplied. One
helper now builds the gateway header map for both the delegated read and the
legacy leader forwarding, keeping every conditional field line; a unit test
pins two `If-Match` lines surviving as two conditions.

Live after the fix, on the 40 MiB object: `?consistent=true` with
`Range: bytes=33554432-33558527` → `206` and 4096 identical bytes on all three
nodes; the same read with a wrong `If-Match` → `412`; a full `?consistent=true`
read of the object → `400` because of the documented 16 MiB cap.

### Launcher: mode follows the replica directories

A start that failed before the daemon created a replica (an argument error, or a
binary built without `--features openraft-storage`) still recorded a settled
phase. The next start then flipped every group config to `open`, and the daemon
refused with `replica path conflicts with explicit create/open mode` forever.
`sync_replica_modes` now decides from the replica directories on disk: all
present → `open`; none present → `create`; partly present → explicit refusal,
because a partial bootstrap cannot be repaired by guessing. The check runs on
every start instead of only in the `stopped` branch.

### Failure drills on the merged tree

| Drill | Observed |
| --- | --- |
| `SIGKILL` of node 2 (g2 leader), then restart | g2 re-elected node 1 at **term 2**; the restarted node rejoined as a follower. Writes and consistent reads through every listener still returned 200. |
| `stop-node 3` (one of three replicas) | Consistent reads returned 200 with identical bytes on nodes 1 and 2 (majority 2/3); every S3 PUT answered `503 "storage pool cannot reach full redundancy; the write was refused"` and a pgwire `INSERT` failed with a SQL error, so no write was acknowledged with fewer copies. |
| `start-node 3` (recovery) | PUTs returned 200 through all three listeners again, and every object read back with one identical SHA-256 per object from every node. |

### Gates on the merged tree

`cargo fmt --all -- --check` clean; `cargo test -p yadbd --bins` 90/90;
`cargo test -p yadb-s3 --lib` 37/37; `cargo test -p yadb-metakv --lib` 170/170;
`cargo test -p yadb-db --lib` 46/46;
`cargo test -p yadbd --features openraft-storage --test openraft_daemon` 4/4;
`tests/protocols.py` 17/17 on consecutive runs and `--native-clients` 19/19.

### Open items for multipart and consistent reads

* A part is capped at 16 MiB (AWS allows 5 GiB); a larger part is refused, not
  streamed.
* There is no `ListMultipartUploads` and no `UploadPartCopy`/server-side copy.
* Orphan collection stays disabled on Multi-Raft, so an aborted or expired
  upload's fragments are not reclaimed there.
* After an ambiguous `503` completion the client must `HEAD`/`GET` to learn the
  outcome; the server does not resume an upload on its own.
* A full `?consistent=true` read of an object above 16 MiB is refused; use
  `Range`.

## Independent review round: multipart, forwarding and COPY (2026-11-16)

An independent review of `ff335e1..cf614e9` returned **BLOCK** with six blockers
and four major findings. Every finding was fixed with a test rather than a
relaxed assertion, and the tree was re-verified from scratch.

### Blockers and majors

| Finding | Fix |
| --- | --- |
| Part registration did not guard the part's own intent, so a collector that aborted a stalled part could still be acknowledged, and the upload lease was refreshed without the part lease. | Registration is one `RevisionTxn` guarded on both the upload record and the part's `Prepared` intent, refreshing both leases. A lost part guard ends the call; the collector re-reads the record instead of trusting a stale `pinned_parts` snapshot. |
| An initiate could accept options a single PUT refuses, dropping them silently. | `unsupported_object_options` refuses `x-amz-meta-*`, encryption and ACL at initiate, before the durable upload record. The live run below showed the native listener needed its own check as well. |
| The completion parser accepted a truncated or extended document. | A full tokenizer (`xml_token`, `read_complete_part`, `read_element_text`) consumes the whole document, accepts only the expected elements, refuses duplicates and rejects content after the root as `MalformedMultipart`. |
| `ListParts` ignored `part-number-marker`/`max-parts`. | Both are honoured: `max-parts` 1..=1000 (out of range → `InvalidArgument`), `NextPartNumberMarker` emitted, and the gateway answer is read through a typed, fallible, bounded JSON parse. |
| A failed `ROLLBACK` after a broken `COPY` was ignored, leaving an invisible open transaction. | `copy_rollback` surfaces the failure as an `io::Error` and the connection is closed; both `COMMIT` errors are checked. |
| The multipart error mapper dropped `x-yadb-write-outcome`. | `multipart_error` preserves it and maps a local `ConcurrentModification` to `unknown`. |
| A forwarded `HEAD` was parsed with the GET framing rule, so a bodyless `HEAD` with `Content-Length` was rejected as truncated. | `ProxyResponse` carries `content_length`; `parse_proxy_response_for(raw, head)` accepts a bodyless `HEAD` with its declared length, rejects a `HEAD` that carries a body, and `maybe_forward_write` restores `Content-Length` on the forwarded answer. |
| `Location` in a Complete answer leaked the `uploadId`. | `Location` names the object only. |
| The launcher skipped `sync_replica_modes` in `start-node`/`restart-node`, and a stop timeout wrote a compacted PID list. | `sync_replica_modes` runs on the common start path; PIDs stay node-indexed. |

### Defect found by the live run on the merged tree

The live smoke refused to pass an initiate carrying `x-amz-meta-owner`: the
native listener builds a small gateway header set and forwards only
`content-type` and the identity headers, so `handle_s3_create_multipart` never
saw the client's metadata and answered `200` with the metadata silently
dropped. The gateway path refused it, the native path did not. The listener now
applies `unsupported_object_options` to the raw client headers before the
upload record exists, and `tests/protocols.py` asserts `501 NotImplemented` plus
no object for both a metadata and an encryption option.

### Live evidence on the merged tree

Root `yadb-or-merge` at port base 23500, three processes, control and data
groups 1–3, node 1 leader of all three.

| Check | Observed |
| --- | --- |
| Multipart initiate on node 1, three 1 MiB parts on node 2, ListParts on node 3, Complete on node 1 | Same multipart ETag `"…-3"`; a follower's `ListParts` converged on all three parts within the 15 s bound. |
| `ListParts` pagination on node 3 | `max-parts=2` → parts 1–2, `IsTruncated=true`, `NextPartNumberMarker=2`; the marker page → part 3, `IsTruncated=false`; `max-parts=1001` → `400 InvalidArgument`. |
| Complete `Location` | `http://127.0.0.1:23800/invoices-2026/smoke/multi-listener`, no `uploadId`. |
| `HEAD ?consistent=true` of a 20 MiB object (two 10 MiB parts) on node 2, a follower | `200`, `Content-Length: 20971520`, empty body — the forwarded HEAD keeps the leader's declared length. |
| Plain `HEAD` of the same object on nodes 2 and 3 | `200` with `Content-Length: 20971520` and no body. |
| `If-None-Match` on a plain GET (node 2) and on a consistent GET (node 3) | `304` both times, with no representation and no error envelope. |
| Initiate with `x-amz-meta-owner`, and with `x-amz-server-side-encryption` | `501 NotImplemented` on the native listener, no object created. |

pgwire on the same run: `SHOW COLUMNS` reported the declared schema, a `COPY`
from a follower node committed, a `COPY` whose second row failed to parse left
the table at exactly the three committed rows, and a CRLF-terminated line stored
its value without the `\r`.

### Gates

`cargo fmt --all -- --check` clean; `cargo test -p yadbd --bins` 94/94;
`cargo test -p yadbd --bins --features openraft-storage` 103/103;
`cargo test -p yadb-s3 --lib` 38/38; `cargo test -p yadb-metakv --lib` 170/170;
`cargo test -p yadb-db --lib` 46/46; `tests/protocols.py` 17/17 on four
consecutive runs; `tests/protocols.py --native-clients` 19/19 on four
consecutive runs, including the AWS CLI multipart upload and psql 17.

A focused repro of the post-restart VDisk tombstone assertion (`SIGKILL` every
node, restart, re-probe each tombstone) passed 30/30 rounds, so the earlier
single failure was not reproduced; the assertion now reports the status and body
if it ever fails again.

### Still open

* `ListParts` and a plain GET on a follower answer from that node's applied
  state, so a part or object acknowledged a moment earlier through another node
  can be missing until it applies; `?consistent=true` is the opt-in read.
* Per-group GC on Multi-Raft and `InstallSnapshot` were unimplemented at this
  point; both landed later (see the per-group GC section below and the
  compaction section above).

## Second review round: XML framing, pagination and the stop path (2026-11-16)

The re-review of `4016497` returned BLOCK with three issues; two were real
defects introduced by the first round of fixes.

| Issue | Fix |
| --- | --- |
| Opening tags ended at the first `>` even inside an attribute value, and the attribute list was never validated. `<Part ignored="><PartNumber>1</PartNumber><ETag>"<matching>"</ETag></Part>` was read as a normal `<Part>`, so a malformed document could publish an object. | `xml_tag_end` scans quote-aware and `xml_validate_attributes` accepts only whitespace-separated `name="value"`/`name='value'`. Unterminated quotes, unquoted values, a missing name and a `<` inside a value are `MalformedMultipart`; a well-formed `xmlns` list is still accepted. |
| `?max-parts=` and `?part-number-marker=` were silently defaulted: an empty value was treated as an absent parameter. | Only a missing parameter defaults; presence with an unparsable value (including empty) is `InvalidArgument`. |
| The aborted-part test passed a revision captured before the successful registration, so it refused on the stale revision and proved nothing about the state. | The test passes the aborted record's own revision, so only the state check can refuse. |
| The collector's lost-guard branch had no test. | `collect_orphans_from` takes the scan, so a test can hand a pass the stale scan it would hold, refresh the record through a registration in between, and assert the pass re-reads and leaves both parts alive. |
| No automated regression for the stop timeout, the start-node replica-mode reconciliation, `Location`, or COPY cleanup. | `scripts/test_yadb_openraft_dev.py` (3 tests) covers the stop-timeout PID slots and that `start-node`/`restart-node` reconcile replica modes before spawning; `tests/protocols.py` now runs COPY end to end and asserts `Location` carries no `uploadId` and that empty pagination values are refused. |

The new stop-path test found that the first PID-slot fix was still wrong: it
only cleared slots for nodes that were live but had exited, so a node that had
already exited kept a stale PID. Every slot that is not a still-running node is
now cleared.

### Live evidence

Same three-process root at port base 23500.

| Check | Observed |
| --- | --- |
| `GET ?uploadId=…&max-parts=` and `&part-number-marker=` | `400 InvalidArgument` for both, on a real upload with one part. |
| `max-parts=0`, `=1001`, `=abc` | `400 InvalidArgument`. |
| No pagination parameters | `200` with `MaxParts=1000`. |
| Complete with an unterminated attribute quote, an unquoted value, and an attribute with no name, each carrying the part's real ETag | `400 MalformedXML` for all three; the upload survived and the well-formed document then completed it, and the object read back byte-identical. |
| `stop-node --node 3` | `state.json` kept `pids [.., .., null]` with `down [3]`, and `status` reported node 3 as intentionally stopped rather than as corruption. |
| `start-node --node 3` | Node 3 restarted and became HTTP-ready; this is the path that previously failed with "replica path conflicts with explicit create/open mode". A PUT through node 3 then answered `200` and read back through node 1. |

### Gates

`cargo fmt --all -- --check` clean; `cargo test -p yadbd --bins` 94/94;
`cargo test -p yadbd --bins --features openraft-storage` 103/103;
`cargo test -p yadb-s3 --lib` 39/39; `python3 -m unittest
scripts/test_yadb_openraft_dev.py` 3/3; `tests/protocols.py` 18/18;
`tests/protocols.py --native-clients` 20/20.

## Third review round: strict XML framing and a test that proves the guard (2026-11-16)

The re-review of `2fe061a` returned BLOCK with two majors in the completion
tokenizer and one minor in a test. The tokenizer now rejects malformed
documents that the previous version silently normalised.

| Issue | Fix |
| --- | --- |
| An attribute list accepted `a="1"b="2"` (no separating whitespace), repeated names (`a="1" a="2"`) and undeclared entity references (`a="&undeclared;"`), so a strict parser would reject a document that could still publish. | `xml_validate_attributes` requires whitespace before every attribute, rejects a duplicate name, and validates each value with `xml_validate_attribute_value`: no raw `<`, and every `&` must be a declared entity or a `&#N;`/`&#xN;` character reference (`valid_char_reference`). |
| A comment containing `--` or ending in `-`, and an XML declaration inside the root, were skipped as if valid; `</ Part>` was read as `</Part>` because the closing name was trimmed. | Comments are validated (`--` and a trailing `-` are `MalformedMultipart`); `<?xml …?>` is its own `XmlToken::Declaration`, accepted only once and only before the root; a closing tag allows trailing whitespace but no whitespace after `</`. |
| The collector regression still passed with the re-read branch removed, because part 1 kept the revision lease its registration had extended and `u64::MAX` only expires the time lease. | The test expires **both** leases of part 1 before taking the stale scan. With the re-read branch disabled it now fails (`removed == 1`), and with the branch in place the part survives. |

The new regressions all carry a real `<PartNumber>`/`<ETag>` pair inside the
normal root, so accepting any of them would publish an object rather than fail
for an unrelated reason. Well-formed documents with a legal declaration, a
namespace, a comment, entity/character references and newline-separated
attributes are still accepted.

### Gates

`cargo fmt --all -- --check` clean; `cargo test -p yadbd --bins` 94/94;
`cargo test -p yadbd --bins --features openraft-storage` 103/103;
`cargo test -p yadb-s3 --lib` 39/39; `python3 -m unittest
scripts/test_yadb_openraft_dev.py` 3/3; `tests/protocols.py` 18/18 on three
consecutive runs; `tests/protocols.py --native-clients` 20/20.
## Fourth review round: a real XML grammar, and the flake behind it (2026-11-16)

The re-review of `980e7ea` returned BLOCK with three majors, all of the same
kind: the hand-written tokenizer was still laxer than XML in ways that let a
malformed document complete an upload. No XML crate is vendored and `--locked`
forbids adding one, so the tokenizer was replaced rather than patched again.

| Issue | Fix |
| --- | --- |
| Declaration placement and syntax were unchecked: a declaration after a comment or after whitespace was accepted, and `<?xml?>` / `<?xml version="2.0"?>` / `<?xml junk?>` were all skipped as valid. `<?1?>` was skipped too. | `XmlToken::Declaration` is emitted only for the very first token of the document; a comment, whitespace or any other token before it makes the declaration illegal. `xml_validate_declaration` parses the real grammar: mandatory `version` with `1.x`, optional `encoding` (`is_enc_name`) then optional `standalone` (`yes`/`no`), nothing else. A processing-instruction target must be an XML `Name` (`<?1?>` is refused), and any case-variant of `xml` is a reserved target. |
| Character references accepted `&#0;`, `&#xD800;`, `&#x110000;`, `&#X41;` and overflowing digits. | `xml_char_reference` requires lowercase `x`, digits only, no overflow (`u32::from_str_radix`), and the resulting code point must satisfy XML's `Char` production. Literal characters are checked with `is_xml_char` as well, so a raw `\x01` is refused in text, comments, PIs and attribute values. |
| Non-XML whitespace was accepted as an attribute separator and around `=`, because the code used Rust's `trim`, which eats U+00A0. | One predicate, `is_xml_ws` (space, tab, CR, LF), is used for every structural position: attribute separation, `=` spacing, closing-tag tail, inter-element text, declaration pseudo-attributes. `xml_is_ws` decides whether a text token is ignorable, so NBSP is content, not whitespace. |
| `<Part a="1" / >` was accepted as a self-closing tag. | The `/` must be adjacent to `>`; the raw tail is checked before trimming. |
| Element text was decoded with `xml_unescape`, which expanded named entities only and left numeric references alone, so `&#49;` was not `1`. | `read_element_text` decodes through `xml_decode`, which handles the five predefined entities and character references, and refuses undeclared ones. It also treats comments and processing instructions as non-content, so `1<!--x-->2` is `12` and `"ab<?pi?>cd"` is `"abcd"`, while a nested element is still refused. |

`]]>` in character data is refused, comments still reject `--` and a trailing
`-`, and `<!DOCTYPE`/CDATA remain unsupported and explicit. A leading byte-order
mark is accepted as an encoding signature, not content. Every regression
carries a real `<PartNumber>`/`<ETag>` pair inside the normal root, so accepting
one would publish an object. Positive controls keep passing: a legal
declaration with `encoding`/`standalone`, an `xml-stylesheet` PI, a namespace,
comments, entity and character references, and newline/tab-separated
attributes.

### The native-client flake was a lost tombstone, not a timeout

`tests/protocols.py --native-clients` failed about one run in three at the
post-restart VDisk check: a `PUT` of a tombstoned blob answered `200` instead of
`412`. Instrumenting the test showed the tombstone was already gone **before**
the SIGKILL, and the segment file had been rebuilt with live records only.

Cause: `open_or_get_local_vdisk` checked the map under a read lock, released it,
opened the store, then inserted under a write lock. Two concurrent callers (the
`/v1/apply` materialisation and the first internal request, or two internal
requests) both missed the map and opened the same slot directory twice. Each
instance keeps its own `write_pos`, `tombstones` and `barriers`; when the
directory was still empty, the second `init_new_segment` truncated the segment
the first instance had just created. The instance holding the older view then
appended from offset 8 and overwrote the tombstone the other instance had
acknowledged, so a restart recovered a file where the deleted blob was live.

Two changes, both minimal:

* `open_or_get_local_vdisk` now does the check, the open and the insert under
  one write lock. One slot directory has one store per process.
* `init_new_segment` refuses to reinitialise a segment that already has bytes
  (`InvalidSegment: refusing to reinitialize a non-empty segment`) instead of
  truncating it. A lost race is now an explicit error rather than silent loss of
  tombstones and GC barriers.

Regression: `concurrent_local_vdisk_opens_return_one_store` starts eight threads
on a barrier and asserts every returned `Arc` is the same store. With the racy
version restored it fails on all three runs; with the fix it passes.

### Gates

`cargo fmt --all -- --check` clean; `cargo test -p yadbd --bins` 95/95;
`cargo test -p yadbd --bins --features openraft-storage` 104/104;
`cargo test -p yadb-blob --lib` 26/26; `cargo test -p yadb-s3 --lib` 39/39;
`python3 -m unittest scripts/test_yadb_openraft_dev.py` 3/3;
`tests/protocols.py` 18/18; `tests/protocols.py --native-clients` 20/20 on six
consecutive runs after the fix.

## Per-group garbage collection on Multi-Raft (2026-11-17)

Multi-Raft collection no longer refuses as a whole. `POST /v1/pools/gc` and the
sweep phase of `POST /v1/pools/scrub` run one pass per metadata group, and each
pass either completes under a fence it can prove or refuses that group and
deletes nothing for it. Legacy metadata keeps its single-namespace pass byte for
byte; nothing below changes legacy keys, ordering or behaviour.

The design is the one in `multiraft-production-todo.md`: cross-group references
are impossible (a fragment id carries its `project_id`, a manifest only names
fragments of its own project, and a project maps to exactly one data group), so
the live set stays group-scoped and no cross-group snapshot is needed.

The rules and tests in this historical section describe the former per-group
implementation, which is now disabled because of lease-expiry fencing races.
They are not a current safety guarantee. Manual and periodic Multi-Raft
mark-and-sweep now refuse; a live reclamation proof remains open.

| Concern | Per-group rule |
| --- | --- |
| Leadership | The pass runs only on the leader of group G, read through `role_info_for_group`. A follower refuses with the leader it knows. |
| Snapshot | `try_linearizable_group_snapshot(G)` names the group directly and returns a quorum-confirmed snapshot. Followers and unavailable quorums are errors, never a fallback to local applied state. |
| Collector lease | `/gc/g<group>/collector`, one key per group, held for `COLLECTOR_LEASE_MS` (120 s) and renewed before each pool. Two passes over different groups do not block each other. |
| Lease fence | The collector checks `/gc/g<group>/writer/<id>` (TTL 300 s) and `/gc/g<group>/reader/<id>` (TTL 30 s); a live record in G refuses G only. Multi-Raft writers publish group leases, but readers do not currently publish them, so this check does not protect in-flight reads. The fence is checked before and after collector admission closes. |
| S3 intent | An unexpired `Prepared` intent is part of the mark, so chunks appended before publication are kept. |
| Deletion | Only ids whose `project_id` the group's scope covers, and only when the pool enumerated successfully. A refused pool is left untouched for every group. |
| Refusal | Not leader, unreadable fence, collector lease held, snapshot failure or an unprovable live set refuses the group with the reason in `gc_groups_refused`; `/v1/pools/gc` answers `409` with that reason. Other groups are unaffected. |
| Compaction | Physical and fenced by durable barriers, so it runs once after the groups. A pass in which every group refused touches no volume. |

The repair (orphan) scan still refuses on Multi-Raft (`scrub disabled for
Multi-Raft`): it needs one whole-deployment view, which Multi-Raft does not have.

### Tests

| Test | Proves |
| --- | --- |
| `gc::tests::openraft_tests::a_multigroup_pass_reclaims_each_groups_dead_fragments` | Two data groups over projects 1 and 2 each keep exactly the newest generation (16 fragments) and serve their newest bytes after the pass. |
| `gc::tests::openraft_tests::a_follower_group_refuses_and_deletes_nothing` | Group 2 is a two-node cluster whose local node is a follower. The pass refuses group 2, deletes nothing, and the group 2 leader then reclaims the same fragments — so the refusal, not the data, kept them. |
| `gc::tests::openraft_tests::a_group_writer_lease_protects_an_unpublished_fragment` | An unexpired writer lease in group 2 keeps an appended, unpublished fragment alive and refuses group 2, while group 3 still reclaims its own garbage. Releasing the lease makes the fragment provably dead. |
| `gc::tests::openraft_tests::a_prepared_s3_intent_protects_its_chunks` | A `Prepared` intent keeps its chunks through a pass with `fragments_deleted: 0`; deleting the intent makes the same chunks dead. |
| `yadb_metakv::gc::tests::group_leases_are_scoped_to_their_group` | A group 2 writer record lands in `/gc/g2/writer/`, fences group 2, does not fence group 3 and does not appear in the legacy namespace. |
| `yadb_metakv::gc::tests::group_collectors_are_independent_and_close_admission_per_group` | Two groups hold collector records at once; admission closes per group and reopens independently. |

### Sensitivity

Disabling the post-admission fence re-check (`if !live.is_empty()` →
`if false && !live.is_empty()` in both per-group checks) makes
`a_group_writer_lease_protects_an_unpublished_fragment` fail:
`groups_refused` is empty and the pass deletes 483 fragments while a writer
lease is live (`left: 0, right: 1`). The check was restored; the suite is green
with it.

### Gates

`cargo fmt --all` then `cargo fmt --all -- --check` clean;
`cargo test -p yadb-metakv --lib` 172/172;
`cargo test -p yadbd --bins` 96/96;
`cargo test -p yadbd --bins --features openraft-storage` 109/109;
`cargo test -p yadb-s3` 39/39 plus 0 doc-tests. All with `--offline --locked
-j 2`. The full `yadb-metakv` run was green on the second attempt; the first
failed five TLS/process tests with `request timeout` under load, and each
passes when run alone (pre-existing timing sensitivity, not touched here).

### Open

* No live-cluster drill of the per-group path: the evidence is in-process
  fixtures, not a three-node deployment with a killed group leader.
* Readers on Multi-Raft take no reader lease (`begin_read_lease` is still
  legacy-gated), so only the per-group writer lease and the S3 `Prepared` intent
  fence in-flight writes there.
* A pass over a group whose project set is empty still takes and releases its
  collector record; the cost is one round trip, not a correctness issue.

## S3 allocation ids in blocks (2026-11-17)

S3 uploads no longer perform one guarded update of
`/s3-upload-sequence/{project_id}` per upload. A writer's first upload after its
cache empties advances the durable counter by `ALLOCATION_BLOCK` (32) and serves
the rest of the block from a process-local, per-project cache; the next guarded
update happens only when that block is spent.

The contention that motivated it was measured on the 24-writer S3 burst: 48
uploads produced 596 sequence transactions on the single
`/s3-upload-sequence/` key, and individual transactions exceeded the metadata
bridge deadline and surfaced as `503 ... metadata bridge response timed out or
stopped`. With the block allocator the same burst needs one sequence transaction
per process per round instead of one per upload.

The unused tail of a block is not returned to the counter. An allocation id only
has to be unique and increasing; skipping ids is safe, while reusing one would
alias two uploads, including after a restart, because the durable counter already
sits at the end of the block. A crash therefore loses the unused tail of the
current block instead of handing out a duplicate. `reserve_upload` still refuses
to reuse the ids of failed or ambiguous allocations and still returns
`GenerationExhausted` when the counter would pass `MAX_ALLOCATION`
(`(1<<56)-1`).

`generation` in a BlobId is the low 24 bits of the allocation id (`MAX_U24`) and is
masked, not checked, so blocks of 32 make generation values advance in steps of 32
and wrap at the 24-bit boundary. Uniqueness does not rest on `generation` alone:
two allocations 2^24 apart share a generation but differ in `owner_id` (the high
32 bits), and the allocator only stops at `MAX_ALLOCATION`, not at the generation
boundary. There is no generation-exhaustion error to reach 31 ids early.

## Handoff slots, a commit wait that reads silence correctly, and the live drill (2026-11-17)

Historical verification record: the commit-wait and publication fault-injection
details below describe the removed legacy runtime and its then-current tests,
not the present OpenRaft bridge or protocol suite. Current tests cover a dropped
successful S3 response and early quorum-loss refusals, but do not fault S3
manifest or SQL shard publication itself. The historical gates below do not
establish that coverage for OpenRaft.

### Placement

A pool now has `total_parts()` nominal slots plus `handoffSlots` spares, and the
placement rule is documented in `multiraft-runtime-operations.md`. A part whose
nominal slot refuses is written to a spare, in parallel rounds, and the
acknowledgement still means every part of the blob has a durable copy — a
handoff write is not a degraded write and does not appear in `degraded_puts`.
The write quorum itself is unchanged by default (`total_parts()`); `minWriteQuorum`
is an explicit per-pool opt-in bounded by `min_parts_for_read()` and
`total_parts()`.

Tests (`cargo test -p yadb-blob --lib`, 33 tests) add
`a_failed_nominal_slot_is_handed_off_and_the_ack_stays_fully_redundant`,
`without_handoff_a_failed_nominal_slot_refuses_the_write`,
`one_handoff_slot_never_takes_two_parts_of_the_same_blob`,
`delete_tombstones_the_handoff_copy_too`,
`repair_moves_a_handoff_part_back_to_its_nominal_slot`,
`a_spare_that_refuses_once_is_skipped_by_the_next_round` and
`appending_handoff_slots_does_not_move_existing_parts`.

### The 24-way burst: a self-inflicted silence

`tests/protocols.py`'s concurrent S3 burst failed intermittently with fourteen
of twenty-four writes answered `UncommittedIndex` for indices 557-570 while the
leader's `commit_index` was already 588 and every node stayed in term 1. Two
defects in the commit wait, not in replication:

* A waiter counted *rounds* without a peer reply and refused after three. During
  an apply stall those three rounds take tens of milliseconds, so it refused
  entries that committed a moment later.
* Every waiter re-broadcast on each 2 ms poll. Twenty-four waiters produced
  thousands of RPCs per second against the two peers they were waiting for; the
  peers then answered slower than the 500 ms Raft RPC read timeout, and those
  timeouts were counted as a missing quorum. The burst manufactured the silence
  it then refused on.

The wait now re-broadcasts at most once per `COMMIT_WAIT_NUDGE_INTERVAL` (50 ms)
and treats silence as evidence only after it has held for
`COMMIT_WAIT_NO_QUORUM` (1500 ms), which is longer than one round's own timeout.
`drive_round` returns `(replies, attempted)` so a round that had nothing to send
— another proposer already shipped the entry — is never counted as silence.
Regressions: `execute_keeps_waiting_through_silence_shorter_than_the_no_quorum_window`
(five silent rounds then a reply, must commit) and
`execute_refuses_after_sustained_silence_and_before_the_commit_budget` (silence
that holds, refused at or after 1500 ms and before the 3000 ms budget).

### The ambiguous-write drill had stopped being ambiguous

With the wait fixed, the S3 ambiguity scenario returned `200`: the predicate
dropped only the reply to the publication `AppendEntries`, but the next
heartbeat carries no entries, so it was not matched, and its reply reports the
follower's `match_index` — which already includes the entry. That reply confirms
the write as well as the lost one did. The proxy now cuts the whole
leader->follower direction once the proposal has been relayed, which is what
"the follower holding the proposal became unreachable" means. The client sees
`503`/`x-yadb-write-outcome: unknown`, and after the heal the manifest either
survives exactly or is absent — never a different value.

### Live three-node drill (legacy runtime, ports 18000-18300)

Historical record. The legacy runtime and `scripts/yadb-dev.py` no longer exist:
OpenRaft is the only metadata runtime and `scripts/yadb-openraft-dev.py` is the
only cluster launcher. The drill below documents what that runtime did, and is
not a procedure that can be repeated today.

`scripts/yadb-dev.py start` on a fresh root with `examples/platform.yaml`
(three mounts per node, `handoffSlots: 3`), then:

| Step | Observed |
| --- | --- |
| `SIGKILL` node 3, leader stays node 1 | `S3 PUT` -> `200`, etag `"1968494e-15"`; `GET` returns the exact 15 bytes; `INSERT`/`SELECT` through pgwire returns `alpha` |
| Pool scrub on the leader | `degraded_puts: 0` |
| `SIGKILL` node 2 | `S3 PUT` -> `503`, `x-yadb-write-outcome: unknown`; `GET` -> `503 reader lease unavailable`; `INSERT` fails explicitly |
| `stop`, `start`, three nodes converged | the refused object is `404`, the handed-off object is `200` with the same bytes, `drill_live` holds only `('1','alpha')` |

So a two-of-three write is acknowledged with full redundancy through a handoff
slot, a one-of-three write is refused with an explicit unknown outcome, and the
refused mutation left nothing behind. The cluster was stopped immediately after
the run; its data directory is kept.

### Historical gates on the then-merged tree

`cargo fmt --all -- --check` clean; `cargo test -p yadb-metakv --lib` 181/181;
`cargo test -p yadbd --bins` 96/96; `cargo test -p yadbd --bins --features
openraft-storage` 109/109; `cargo test -p yadb-blob --lib` 33/33;
`cargo test -p yadb-s3 --lib` 39/39; `python3 -m unittest
scripts/test_yadb_openraft_dev.py` 3/3; `tests/protocols.py` 18/18 on two
consecutive runs and 18/18 on four more before the merges;
`tests/protocols.py --native-clients` 20/20.

### Open

* No live drill of the per-group GC path or of an actual `InstallSnapshot`
  transfer between processes; both are proven with in-process fixtures.
* A pool with spares tolerates the loss of a whole node only while a spare slot
  lives on a surviving node and takes at most one part of the blob; a second
  simultaneous failure can still refuse the write with `RedundancyNotMet`.
* The commit wait bounds its own RPC rate but does not share one round between
  waiters: twenty-four waiters still issue twenty-four rounds per 50 ms.

## Transport deadline floor and concurrent write dispatch (2026-11-17)

A 24-writer S3 burst on a three-process Multi-Raft cluster (ports 19500-19900,
transport proxies in front of every peer link) took 31.8 s, 57.6 s and 71.2 s per
round and produced `503 ... metadata bridge response timed out or stopped`. Two
independent causes were measured, not inferred.

**The transport abandons appends it could have completed.** Every Raft RPC opens a
fresh TCP connection and performs the pinned mTLS handshake. Measured on loopback
under suite load that round trip is about 120 ms at p50, 285 ms at p90 and 960 ms
at the tail. OpenRaft derives an `AppendEntries` deadline from `heartbeat_interval`
(250 ms with the dev timing), so roughly a fifth of appends were abandoned after
the follower had already applied them; each abandoned append is retried after a
backoff, so a healthy cluster stalled for seconds. `TransportConfig::rpc_ttl_floor`
raises the deadline to one `election_timeout_min` (1500 ms here), on the grounds
that an append older than one election timeout is stale anyway. Snapshot transfers
keep their own, longer deadline.

**The bridge serialised concurrent writers.** `OpenRaftClient::execute` awaited one
`client_write` before dequeuing the next, so N concurrent writers became N serial
round trips and the state machine persisted one entry at a time instead of one
committed batch. It now dispatches each write and lets the runtime coalesce them.
`client_write` is documented as safe to call concurrently, and the response still
arrives only after the entry is applied (`handle_apply_result`), so the outcome
classification is unchanged.

After both changes the same burst completed 24/24 in 8.6-8.9 s for the first round
and about 2.3 s for the second, with zero RPC timeouts in the daemon logs. The
first round is slower because every writer reserves an allocation block; the
second round serves from the per-process cache. This is an improvement, not a
target: per-peer connection reuse remains the real fix for the handshake cost.

**The test proxy added latency of its own.** While chasing the defect below, the
directed relay in `tests/protocols.py` was measured against the direct link with
`proxy_rtt.py`. It relayed with Nagle enabled on both sockets, so a small
request/response pair waited for a delayed ACK on either leg: proxied RTT was
69 ms at the tail against ~1.1 ms direct, and the peers saw a cluster that was
slow only under the harness. `TCP_NODELAY` on both legs brings proxied median to
2.2-2.8 ms and max to 4.2-7.4 ms over four runs. This was a harness defect, not a
daemon one, and it matters because every partition test measures the cluster
through those proxies.

### Post-restart commit stall (resolved)

The protocol suite used to fail one scenario, reproducibly, immediately after a
full-cluster `SIGKILL`/restart:

```
BRIDGE TIMEOUT /s3-upload-sequence/1 after=6000ms
  state=Leader leader=Some(2) term=1 last_log=Some(281) applied=Some(280)
```

The leader had appended entry 281 and `running_state` was `Ok(())`, while
`last_applied` stayed at 280 for the whole deadline and `AppendEntries` to both
peers kept succeeding at 60-250 ms. Because OpenRaft answers `client_write` from
`handle_apply_result`, this isolated the stall to the apply path, not replication
or commit. Ruled out by inspection: no `inner.lock()` guard in the state machine
is held across an `await`, and watch notifications use an unbounded
`std::sync::mpsc::channel()`, so `Watcher::tx.send` cannot block the apply loop.

The cause was the second candidate: every apply batch republished the whole
`state.json` — serialize the full image, write a temp file, `sync_data`, rename,
`sync_all` the directory. On this filesystem the rename plus directory sync alone
was measured at up to 1.6 s under concurrent writers, so with several batches
pending the apply pointer fell behind faster than it advanced, and the 6 s bridge
deadline expired while the log was already committed.

The fix is a delta log. `apply` appends one framed record holding the batch to
`state.delta` and `sync_data`s it *before* mutating memory, so a crash can never
leave memory ahead of the file; a torn final frame is truncated on the next open
and cannot drop an acknowledged entry. The full `state.json` image is published
only when the delta exceeds 4096 entries or 8 MiB, and when a snapshot is built or
installed. An apply now costs one file append and one data sync, with no rename
and no directory sync.

Verification after the change: `tests/protocols.py` 3/3 consecutive clean runs on
a binary built from a tree with no instrumentation, each 20 PASS / 4 SKIP, zero
`BRIDGE TIMEOUT`, zero `TLS CONNECT FAIL`, zero `REPLY WRITE FAILED`.

Residual cost, not a defect: `sync_data` still runs on a runtime worker thread
inside `async fn apply`, because OpenRaft's state-machine trait is async and the
call is not offloaded to a blocking pool. It is bounded to one sync per apply
batch (not per entry) and no longer includes a directory sync. It has not starved
the runtime on this 7-core host; a dedicated blocking pool is the change to make
if apply latency ever shows up under a real fsync-heavy disk.
