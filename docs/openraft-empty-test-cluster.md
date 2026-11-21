# OpenRaft client routing for a new empty test cluster

## Current status (2026-11-14)

**Latest full workspace gates are GREEN on the authorization-freshness delta plus
the nonblocking-refresh fix and post-review hardening.** Offline, locked `-j 2`
runs with `CARGO_INCREMENTAL=0` and the approved `YADB_TEST_TMPDIR` root passed.
All features: MetaKV **124/124** (68.77 s), yadbd units **27/27**, clients
**2/2**, daemon **2/2** (10.11 s) and all other workspace tests. Default features:
MetaKV **124/124** (75.50 s), yadbd **20/20**, with feature-gated integration
files running zero as expected. Logs:
`/data/data/com.foxdebug.acodefree/cache/opencode/workspace-all-features-review2-20261114.log`
and `.../workspace-default-review2-20261114.log`. The previously red failure at
`crates/yadb-metakv/src/openraft_multigroup_test.rs:245`
(`OpenRaftWriteUnknown`, client write timeout) passed alone **15/15** and in both
full gates. No causal fix is claimed; historical process-failure and
election-churn failures remain unexplained. Security tests passed **16/16**,
including the authorization-freshness checks, empty-token rejection and S3-wire
outcome-header preservation.

The current nonproduction loopback deployment is `/public/cluster_data` (mode
`0700`), prepared and started with `scripts/yadb-local.py run`. It is running under
supervisor PID **16793**, children **16795**, **16796** and **16808**, with HTTP
on loopback ports 8420–8422 and Raft on 18420–18428. This is a clean restart:
all nine group configs are `mode: open, initialize: false`, not a first create.
The binary SHA-256 is
`5775b7bc278210deb7f6c8b91a3ca42ac94bb26daf3b3aade8b29583b7d85773`.
`metadata_runtime` is `openraft`; the daemon answered `production_ready: false`
at that snapshot, a field since replaced by the scoped `readiness` object. The deployment
is **no longer empty**: at 21:25:24 on 2026-11-14 an unidentified apply committed
exactly `examples/platform.yaml` (generation `1`, project `billing`, database
`billing/ledger`, object store `billing/receipts`, pools `wal-fast`/`data-ec`).
No apply came from the orchestrator, `yadbctl` was not executed, and
`logs/smoke-setup-started` does not exist. The state was preserved rather than
rolled back. See `multiraft-verification.md` for evidence and consequences; no
claim of an empty cluster or revision `0` applies any more.

Live freshness probes were re-run. Unknown-project SQL,
S3 and block reads returned **403** on 8420, 8421 and 8422, not 503. The internal
snapshot endpoint returned **401** without a token on every node, **200** with
revision `2` on control leader 8420, and **409** with
`x-yadb-leader-id: 1` on followers 8421/8422. The daemon regression additionally
checks a wrong token returns 401, waits for all nodes' caches to observe revocation,
and asserts denial of the old subject through `public[0]` only. Unit tests cover
rejected stale revisions.

Authorization freshness is lease-bound. In OpenRaft mode a node accepts control
state only after a quorum-confirmed linearizable barrier on the current
control-group leader. A follower fetches that leader's snapshot through the
internal `GET /internal/control/snapshot` endpoint authenticated by
`x-yadb-raft-token`. The snapshot is installed before the 2 s lease is extended,
so a fresh lease implies the matching control state is present. The background
refresh thread wakes on a nominal 200 ms interval; after expiry,
DB/block/S3/PostgreSQL-wire/iSCSI authorization fails
closed unless an on-demand refresh attempt succeeds. A failed refresh or quorum
loss does not immediately invalidate an existing valid lease; authorization may
continue using its snapshot until expiry. Revocation is not instantaneous
cluster-wide. The 2 s TTL is measured at snapshot acceptance, not at the barrier;
neither a hard 2 s snapshot-age/revocation bound nor a 200 ms revocation bound is
established. The 200 ms interval is a wakeup cadence, not a completion guarantee,
and the fetch uses per-operation socket timeouts rather than a total deadline.

`/v1/apply` has its own authorization: the daemon accepts the request only from a
subject proven by `x-yadb-subject-signature` (HMAC-SHA256 under
`--subject-secret`) and listed in `--admin-subject`; unsigned, forged and
non-allowlisted subjects get 401/403, and an unconfigured allowlist gets 503. An
OpenRaft daemon refuses to start with an empty allowlist, and any allowlist
requires a non-empty secret. `scripts/yadb-local.py run` now starts every node
with `--admin-subject local-operator` and exports the subject secret from
`credentials/secrets.json`; `scripts/yadb-local-smoke.py` already signs its apply
with that identity.

The HTTP authorization path does not perform the blocking fetch on a Tokio worker:
`control_for_authorization_async` and `refresh_openraft_control_async` delegate to
`spawn_blocking`, so a stalled control leader does not hold a runtime worker in a
TCP read. That removes the identified direct mechanism; it does not make the path
starvation-proof. Failed refreshes serialize on the refresh mutex, expired-lease
requests can each enqueue a blocking task, the internal snapshot endpoint shares
the same blocking pool, and synchronous lock contention remains possible. The
synchronous path is kept for the PG/iSCSI connection threads and the
background refresh thread. A `SIGSTOP` of the control leader for about 1.2 s kept
follower health and 403 denial responses in the 4–14 ms range. This is supporting
evidence only; it is not a causal fix for historical client write timeouts.

Preferred leaders are group 1→node 1, 2→node 2 and 3→node 3. The root described
in this document was provisioned with heartbeat 250 ms, preferred election
minimum 1500 ms, nonpreferred minimum 2750 ms and maximum 3000 ms, and in one
observation all three nodes reported those leaders and `preference_met: true`.
The launcher now provisions heartbeat 750 ms / election 2250-3000 / maximum
3500 ms, because the heartbeat is also OpenRaft's append budget (see
`docs/multiraft-verification.md`); this root is unchanged on disk. This
probabilistic soft bias is not a placement guarantee or workload balance, and
does not resolve historical election churn.

Relocation was cancelled and no relocation script exists. The prior root
`/data/data/com.foxdebug.acodefree/cache/opencode/yadb-local-20261114T135536Z-54c1c388-188b-4a5b-927a-4dd5cf4a7a62`
was deleted offline after PIDs were verified absent, ports free, 13 directory locks
held and symlinks rejected. Its smoke/test data and root-local evidence files were
deleted; historical smoke, clean reopen, leadership sampling and election churn
observations below do not validate the new cluster. The external pre-delete inventory
`/data/data/com.foxdebug.acodefree/cache/opencode/yadb-relocation-before-beb66119de814f32bfb2d8699fddddae.json`
contains SHA-256 hashes, sizes and modes only, not data. The final independent Sol
review approved explicit-root prepare and the conditional deletion plan, not production.

The final independent combined Sol review did **not accept** the work overall. That
review predates this freshness delta. It accepted the HTTP unknown-outcome and
framing, URL segment encoding and UNMAP 416 changes at source level. Later green
tests do not establish causal fixes or a new overall acceptance. The freshness
delta and nonblocking-refresh fix separately received a narrow-scope ACCEPT with
open findings, recorded in `multiraft-verification.md`; production readiness
remains unaccepted. At the time of this snapshot the legacy runtime was still the
default, `production_ready: false`, no existing cluster had been switched and no
migration occurred; the prior disposable test root was deleted as noted above.
That runtime has since been removed.

Earlier full gates were green historically: all-features MetaKV **124/124**
(73.94 s), yadbd **21/21**, clients **2/2**, daemon **2/2**, S3 **19/19**, IO
**7/7**; default MetaKV **124/124** (84.40 s), yadbd **15/15**; and a targeted
process run passed **2/2** (4.58 s) without a demonstrated causal fix. The legacy
`open_clustered` deprecation warning and expected offline-peer snapshot diagnostic
remain. Historically, full runs failed at default MetaKV 121/124 and all-features
122/124; both process tests reported no acknowledged writes, and the default run
also had a group 3 leader timeout. A targeted process run failed 2/2 with no
acknowledged write or peer replication progress. These failures and the earlier
process snapshot catch-up failure remain unexplained by the later passes.

Historically, the deleted deployment passed SQL, S3 and block smoke readback for
alpha and beta through all three endpoints, including a clean stop and reopen.
Endpoint readback did not independently establish replica-local apply or durable
replication. Significant election churn occurred within minutes; no root cause was
proven and runtime stability was not established. Two fake-child cleanup unit tests
passed after the P2 cleanup race fix. Independent Sol tooling review accepted only
that narrow historical empty-loopback smoke and clean reopen; the final combined
review rejected production readiness. The deleted root's evidence is no longer available.

`force_leader` only expresses a routing preference and does not provide
linearizable reads. OpenRaft block reads with `consistent=true` forward to the
project-group leader and read the extent map through a quorum-confirmed snapshot;
a blob collected before the fetch is a `503` rather than zeros, so that snapshot
is not a GC fence. S3 GC remains disabled. Shutdown cancellation passes only
when the borrowed owner is retained and shutdown is retried; startup cancellation
is supervised and releases an unclaimed replica, though a Tokio runtime shutdown
can cancel that detached cleanup. The POSIX stale-FD compaction claim was false, so only explicit
reopen and an append regression remain. The lease bound above closes the previous
indefinite-staleness authorization bug but is not production qualification.

Status: **implementation in progress, not production-ready**. This document
records the explicitly authorized test-routing scope, not a completed rollout.
The production checklist remains `multiraft-production-todo.md`.

## Scope

The requested target is **multiple active client-serving groups**, not a
single-group OpenRaft replacement. Use a dedicated control group plus at least
two distinct data groups, with an explicit immutable project-ID-to-group map.
All SQL, S3, block and iSCSI metadata for one project stays in its assigned group.
Existing atomic metadata transactions must not be split across groups. Blob fragments
continue to use the existing storage engines and pools; OpenRaft is the metadata
consensus engine, not a replacement for fragment storage.

The explicit selection being implemented is `--metadata-runtime openraft` with the
`openraft-storage` build feature and a mandatory-mTLS group config.
At the time of this snapshot the legacy runtime was still the default; it has
since been removed, and `--metadata-runtime` now accepts only `openraft`.
A child-daemon integration test passed with three
singleton groups, before final control-cache hardening and full-suite validation.
A targeted three-process test also covered multi-node HTTP leader routing for
SQL/S3 writes; broader daemon validation remains open. The current loopback
deployment (not empty since the 21:25:24 apply) and historical smoke evidence from
the deleted deployment are described in current status above.

## Version 2 manifest and staged setup

The current implementation reads a version-2 manifest for client mode. This
illustrative manifest contains paths only, not credentials or runnable configs:

```json
{
  "version": 2,
  "control_group": 1,
  "project_groups": {"1": 2, "2": 3},
  "groups": [
    "/path/to/node-1/group-1.json",
    "/path/to/node-1/group-2.json",
    "/path/to/node-1/group-3.json"
  ]
}
```

Each absolute path names a separate version-1 group configuration described in
`multiraft-runtime-operations.md`; it is not an embedded object. Configure 3–16
unique groups, with a control group and at least two distinct data groups. Every
non-control group must be referenced by at least one nonzero numeric project ID,
and every route must name a configured data group. IDs and routes are immutable on
reopen. Each node's group configs must share cluster ID, node ID, voter IDs and
create/open mode, but use distinct group identities, replica paths and dedicated
listeners. Corresponding voter inventories and addresses must agree across nodes.

1. Preserve all existing data. Provision a separate new or empty client data
   root per node, and existing protected replica-root directories outside those
   client roots. Keep configs and independently provisioned credentials outside
   both. Choose unique group-specific mTLS listener addresses and a separate public
   HTTP peer map whose node IDs exactly match the voters.
2. For **initial creation**, set `mode: "create"` in every group config on
   every node. For each group choose exactly one bootstrap node and set that
   group's `initialize: true` only there; set it false on its other replicas.
   The chosen node may differ between groups. Do not initialize a group twice.
3. For a separately authorized test, the intended per-node selection is
   `--metadata-runtime openraft --openraft-config /absolute/path/to/manifest.json`,
   plus the node's explicit `--data-dir`, `--node-id`, `--listen` and `--peers`
   public HTTP map. Use a feature-enabled build. The manifest and group configs
   are node-specific; the routing map must agree throughout the test cluster.
   To keep the token-authenticated `/internal/...` peer routes off the client
   network, add `--internal-listen <addr>` and the matching
   `--internal-peers <id=addr,...>` map: the public listener then answers `404`
   for those routes, and startup refuses a shape where a peer RPC or the
   follower-to-leader control snapshot would 404. Without the flags the
   historical single-listener shape is kept, so the exposure is bounded only by
   firewall rules.
4. For a normal restart, first stop cleanly, then set **each** group config to
   `mode: "open", initialize: false`; retain the same root, identities, voters,
   replica paths and route map. Open is not create. A partial create or marker
   mismatch requires diagnosis; do not delete artifacts or retry as a new cluster.

This is a staging guide for implementation still under validation, not authorization to
start against existing deployments. The separate empty loopback test is described above.

## Data isolation

* Allocate a new empty data root for each test node. Do not point it at a legacy
  directory, copy legacy WALs into it, or delete old data to make admission pass.
* Keep signing/TLS credentials and configuration outside that new empty root.
  Public test certificates and deterministic test keys are not deployment keys.
* Bind the new root to its runtime, replica identity and canonical backing path
  with a durable versioned marker. A missing, partial or conflicting marker on
  reopen is an error, not permission to create a different runtime.
* An OpenRaft root must never be opened as legacy, and an OpenRaft startup error
  must never fall back to creating a legacy WAL.
* Persist the complete group identities and project mapping with the root marker.
  Changing a mapping on reopen is not a routing tweak: it requires a separately
  implemented data-movement protocol and is rejected in this version.
* Create once per replica; restart every group with explicit open mode and initialization disabled.
  Partial creation remains fail-closed and needs diagnosis.

The marker is a guard for a fresh test deployment, not an export/import format,
cluster-wide writer fence or migration/cutover authority.

## Addresses and identity

Provision separate addresses for the mandatory-mTLS Raft listener and the public
HTTP gateway. The public peer map used for forwarding must agree on node IDs
with the fixed voters, but must not contain the Raft listener addresses. The
daemon node ID must match its replica config. Never initialize every replica
independently; choose one operator-selected bootstrap replica for each group.
Each group has independent election state and its own storage. Public forwarding
must select the addressed project's group leader, not assume the control group's
leader serves all projects. Dedicated group listeners are acceptable initially;
shared transport demultiplexing is not implied.

Actual startup requires node identities, addresses, protected fresh roots and
properly provisioned keys/certificates. No existing deployment has been changed
merely by agreeing to this work or running local integration tests.

## Semantics and remaining limitations

* Successful writes must come from OpenRaft `client_write`, never direct local
  state-machine application or a shadow legacy instance.
* Reject transactions whose guards/operations cross group boundaries before
  submission. No distributed transaction protocol is implemented; projects
  colocated in one group still share that group's atomic metadata domain, not a
  cross-group guarantee. Broad administrative
  scans may merge local per-group observations, but are not atomic snapshots.
* Revisions are group-local. S3 leases and collection must use the owning project's
  group revision; there is no meaningful max/sum/global revision clock.
* Project IDs are allocated by the control plane, currently in project creation
  order. The explicit route map must cover every admitted project ID before its
  control-plane state is persisted. Predeclare routes for intended test projects;
  adding a project must not silently choose a group by hashing or remap data.
* Reads remain local-applied reads. Selected-key/range snapshots copy one applied
  state, but do not establish quorum freshness, linearizability or cross-node
  read-your-writes. An isolated follower can return stale data.
* A timed-out write or lost forwarded response has an unknown outcome. Do not
  automatically resubmit a mutation after an ambiguous response; inspect state
  or use application-level idempotency.
* Pool scrub and its orphan-collection/GC work are disabled in
  Multi-Raft mode; do not assume automatic repair or reclamation. This does not
  make direct low-level deletion or barrier endpoints safe to use.
* Protocol write behavior on a follower must be tested separately from HTTP
  forwarding; engine-level tests do not establish transparent portal failover.
* Readiness is a scoped claim, not a bare flag. The old advice to keep
  `production_ready: false` was superseded on 2026-11-21: the field became the
  `readiness` object, whose `limits` array names what a green run does not
  prove. Changing the selected client runtime is still not production
  qualification. Snapshot capacity/deadlines, the prior process catch-up
  regression, resource isolation, lifecycle, security and host durability gates
  remain tracked separately.
* Returning to legacy after OpenRaft writes is not a rollback. Legacy has not
  received those writes; use a separately reviewed migration procedure or
  discard only explicitly disposable test data after authorization.

## Required verification before calling the test switch complete

Exercise real metadata writes, guarded multi-key transactions, selected reads,
watches and persistence/reopen through the facade. Exercise SQL transaction/index
paths, S3 put/get/list and block write/read/unmap against that same facade. Test
both data groups with real project operations and inspect their independent
durable state; merely starting extra idle groups does not count. Test
empty-root admission, unchanged legacy data, disabled legacy Raft endpoints and
accurate backend status. The latest offline/locked full gates passed, but that is
not a production or multi-host qualification. Address the independent Sol review's
blockers and remaining qualification, recording failures rather than hiding them
with retries or larger timeouts. Local tests are not a deployed multi-host cluster.
