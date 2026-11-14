# Experimental runtime operations and recovery boundaries

## Current status (2026-11-14)

The latest offline, locked `-j 2` full workspace gates are **GREEN** on the
authorization-freshness delta, the nonblocking-refresh fix and post-review
hardening. With `CARGO_INCREMENTAL=0` and the approved
`YADB_TEST_TMPDIR` root, all-features passed MetaKV **124/124** (68.77 s), yadbd
units **27/27**, clients **2/2**, daemon **2/2** (10.11 s) and all other tests;
default passed MetaKV **124/124** (75.50 s) and yadbd **20/20**. The previously
red `openraft_multigroup_test.rs:245` timeout passed alone **15/15** and inside
both full gates. No causal fix is claimed; historical process-failure and
election-churn failures remain unexplained. Logs:
`/data/data/com.foxdebug.acodefree/cache/opencode/workspace-all-features-review2-20261114.log`
and `.../workspace-default-review2-20261114.log`.

After the freshness delta and hardening, security tests pass **16/16** and the
feature-enabled daemon tests **2/2**, covering the token-protected internal
snapshot endpoint and revocation. Unit tests cover rejected stale revisions, empty
Raft-token rejection and S3-wire outcome-header preservation. The revocation test
waits for caches on all nodes but asserts denial only through `public[0]`. State
tests pass 16/16, storage 23/23, S3 19/19 and IO 7/7. These targeted results are
included in the full gates above, not substitutes for them.

Authorization freshness is now lease-bound. In OpenRaft mode a node accepts
control state only after a quorum-confirmed linearizable barrier on the current
control-group leader; a follower obtains that snapshot from the leader through
the internal `GET /internal/control/snapshot` endpoint authenticated by
`x-yadb-raft-token`. The snapshot is installed before the 2 s lease is extended,
so a fresh lease implies the matching control state is present. The refresher
wakes on a nominal 200 ms interval; after expiry,
DB/block/S3/PostgreSQL-wire/iSCSI authorization fails
closed unless an on-demand refresh attempt succeeds. A failed refresh or quorum
loss does not immediately invalidate an existing valid lease; authorization may
continue using its snapshot until expiry. Revocation is not instantaneous
cluster-wide. The 2 s TTL is measured at snapshot acceptance, not at the barrier;
neither a hard 2 s snapshot-age/revocation bound nor a 200 ms revocation bound is
established. The 200 ms interval is a wakeup cadence, not a completion guarantee,
and the fetch has per-operation socket timeouts, not a total deadline.

`force_leader` is routing preference, not a linearizable-read guarantee. OpenRaft
`consistent=true` block reads are served by a quorum-confirmed extent-map read on
the project-group leader (iSCSI reads use it too), and a blob collected between
the map read and the fetch is a `503` rather than zeros; SQL, S3 and status reads
remain locally applied. S3 GC is
disabled. `/v1/apply` requires a signed subject on the `--admin-subject` allowlist;
an OpenRaft daemon refuses to start without one, and any allowlist requires a
non-empty `--subject-secret`. Unsigned or forged subjects get 401, a proven
non-admin subject gets 403. This is a shared-secret check, not per-operator
credentials: anyone holding the secret can sign any allowlisted subject.
Focused shutdown cancellation passes retain the borrowed owner and retry;
startup cancellation remains unsafe. The POSIX compaction stale-FD claim was false;
explicit reopen and an append regression are retained only. Legacy remains default,
no existing cluster was switched, old data is untouched, no migration occurred, and
`production_ready: false`.

Status: **not production-ready**. Legacy remains the default; no existing or
production cluster has been switched. The user has authorized implementation of
actual OpenRaft client routing for a new nonproduction cluster only. That
local loopback cluster is running at `/public/cluster_data` under supervisor
PID **16793** (children **16795**, **16796**, **16808**), all nine group configs
in `mode: open, initialize: false`, binary SHA-256
`5775b7bc278210deb7f6c8b91a3ca42ac94bb26daf3b3aade8b29583b7d85773`.
The deployment is **not empty**: at 21:25:24 on 2026-11-14 an unidentified apply
committed exactly `examples/platform.yaml` (generation `1`, project `billing`);
the orchestrator did not issue it and `yadbctl` was not executed. The state was
preserved, not rolled back. Live freshness probes and the internal
snapshot token gate passed with revision `2`. It is an experimental test
deployment, not production
routing or migration. The feature flag alone does not authorize migration,
production routing, rolling upgrades or disaster recovery by editing voters. The
final independent GPT-6.1 Sol review of the freshness delta and the
nonblocking-refresh fix returned ACCEPT for the narrow experimental local scope
only, with open findings listed in `multiraft-verification.md`.

## Daemon opt-in interface

The requested client-routing target is now **Multi-Raft**, with a dedicated
control group and at least two actively used data groups. An immutable explicit
project-ID-to-group map keeps all metadata transactions for each project in one
group. The version-2 manifest and staged create/open guide are in
`openraft-empty-test-cluster.md`. A child-daemon integration test passed with
singleton groups before final hardening; a targeted three-process HTTP leader-routing
test also passed. Broader daemon validation remains open; the only running cluster
is the local nonproduction loopback deployment described above (not empty; see
current status). The
version-1 standalone interface below remains observational, not a client-routing
configuration.

The daemon's `experimental-openraft-storage` build feature forwards the MetaKV
experimental feature. `--experimental-openraft-config <path>` is rejected before
data initialization when the feature is absent. In legacy mode the version-1
configuration starts one isolated observational group on a dedicated mandatory-mTLS
listener while legacy MetaKV serves clients. The in-progress v2 client mode instead
uses one dedicated listener per group and does not create a legacy MetaKV WAL or
fall back to legacy on failure.

OpenRaft `consistent=true` block reads are supported: the extent map is read
through a quorum-confirmed snapshot on the project-group leader and the blobs it
names are immutable and published before the map, so the returned bytes reflect a
committed state. That snapshot is **not** a GC fence: if a newer write replaced a
block and a barrier collected the old blob, the read fails with `503`
(`BlockError::Inconsistent`) rather than returning zeros, and the engine does not
retry. A follower forwards the read to the leader instead of serving its local
view; `NotLeader` is an error, not a stale fallback. Legacy mode retains its
existing behavior.

Version-1 JSON configuration fields:

| Field | Meaning |
| --- | --- |
| `version` | Exactly `1`; unknown fields are rejected |
| `root` | Existing, durably provisioned experimental root directory |
| `mode` | Explicit `create` or `open`; never automatic |
| `initialize` | Defaults false; true only with create, on one operator-selected bootstrap replica |
| `cluster`, `group`, `incarnation`, `node` | 16-byte cluster array and nonzero group/incarnation/node identity |
| `listen` | Dedicated socket address; nonzero port, local peer address must agree |
| `signing_key_der` | Local Ed25519 PKCS#8 file; public key must match local peer inventory |
| `tls.roots_der` | Nonempty list of DER CA certificate paths |
| `tls.chain_der`, `tls.key_der` | Nonempty local leaf-first DER chain and private key |
| `peers` | Sorted fixed peer list, including local node, at most 32 |
| Each peer | `id`, `address`, `public_key` (32-byte array), `server_name`, `cert_sha256` (32-byte exact DER leaf pin) |

Config input is limited to 256 KiB; each DER input is limited to 1 MiB and CA/
chain counts to 16. These are parser limits, not total runtime resource isolation.
Credential paths are local operator input; current relative paths resolve from
the process working directory, not automatically from the config file directory.
Use reviewed absolute paths to avoid ambiguity. No credential generation command
or production-ready example secrets are provided.

Before touching replica storage, preparation verifies the local signing public
key, certificate pin, trusted chain, name, current validity and server/client
certificate purposes. This is point-in-time validation, not live revocation or
protection against replacement of credential files by a privileged local actor.

Replica directory names are derived from full cluster/group/incarnation/node
identity under `root`. `create` refuses an existing path, including partial data;
`open` refuses a missing path. Only one explicitly authorized initial replica per
group uses `initialize: true`; restart every group with `mode: "open", initialize: false`.

`GET /experimental/openraft/status` is observational, not a client write API or
readiness/cutover authority. It labels the runtime experimental and
`production_ready: false`; client traffic is labeled legacy for observational v1
and multiraft for v2 client mode. Group node/state/term/leader are cached local
observations. Metrics, authenticated operator controls and daemon integration
validation remain incomplete. Protect the public diagnostics listener according to
the deployment's existing access policy.

## Provision storage before starting

Use ordinary local filesystems with working file/directory sync and cooperating
OS directory locks. Provision a protected root directory first. Do not rename,
replace or share a locked directory with legacy code, which does not participate
in these locks. Network filesystems and hostile local path mutation are outside
the current ownership contract.

`PersistentGroup::create` requires a new replica directory. It reserves that
directory, syncs its parent, writes a versioned `replica.json` binding the exact
cluster/group/incarnation/node and bootstrap voters, then creates separate log
and state directories. `open` requires an existing matching descriptor and valid
stores. Missing, empty, mismatched, unsupported or partially created artifacts
must fail closed, not trigger automatic bootstrap.

Persisted bootstrap voters are immutable configuration provenance, not a live
membership editing interface. In particular, do not reopen a multi-voter group
as a singleton to recover from quorum loss. The registry also rejects a different
voters list for the same group/node key.

## Normal shutdown

Stop accepting RPCs, cancel and **join** owned handlers, then await OpenRaft core
shutdown while the persistent group/directory owner remains alive. Only then
release owners and permit reopen. Dropping or aborting an async future is not an
acknowledgment that its child tasks have finished. Cloned Raft handles and cached
status must not be mistaken for an independent storage owner.

`RaftPeerServer::serve_until` is the explicit stop-and-drain transport interface;
dropping a `serve` future requests handler cancellation but does not synchronously
drain it. Runtime shutdown APIs must be awaited to completion. Cancellation safety
for arbitrary callers and failures at every lifecycle await remain open gates.

The daemon's `Running::shutdown(&mut self)` keeps its group owner and listener
join handle in the caller when the shutdown future is cancelled. Retry shutdown
on the same `Running` and await completion before dropping it. The deterministic
cancellation test covers that borrowed cleanup boundary; dropping `Running`
itself before cleanup finishes, or cancelling startup, remains unqualified.

## Interpreting diagnostics

An observed leader ID, local server state or applied index is a local metric,
not proof of current quorum, freshness, write availability or safe client routing.
Production readiness must remain false while any production checklist gate is
open. Never use `/healthz` or a cached leader observation as a cutover authority.

Useful eventual alerts include fatal storage/raft errors, leader churn, quorum
unavailability, replication/applied lag, snapshot build/install failures, disk and
retained-log capacity, connection/byte-budget exhaustion, certificate expiry and
trust mismatches. Thresholds and operator remediation need supported-host tests;
this document does not imply a complete metrics/alert implementation exists.

`client write timed out` from the metadata bridge means the bridge's 5 s deadline
expired before the client write returned. The commit outcome is unknown to the
caller: the bridge aborts its task, not the Raft entry, so the proposal may still
commit. For a keyed `/v1/apply`, resolve the outcome with
`POST /v1/idempotency/resolve`; for any other write, reconcile application state
before retrying.

## Fail-closed recovery matrix

| Condition | Required action |
| --- | --- |
| Directory lock busy | Identify the current owner; do not bypass locks or replace the directory |
| Partial bootstrap | Preserve artifacts and diagnose; do not auto-create missing files or initialize again |
| Replica identity/voters mismatch | Stop and verify the intended configuration and provenance; never rewrite the descriptor to fit |
| Unsupported experimental log version | Preserve the original bytes; use a separately reviewed migration or explicit disposable-test reprovisioning, not silent replay |
| Corrupt/truncated log tail or state image | Preserve raw evidence, stop serving, and recover from validated replicas/backups through an approved procedure; automatic truncation is not implemented |
| Write/sync/rename/directory-sync error | Treat the operation outcome as unknown; the handle is poisoned, and further participation requires reopen/recovery |
| Quorum unavailable | Restore legitimate peers/connectivity; never reduce voters, accept singleton writes or treat timeout as abort |
| Certificate/pin/signing-key rejection | Keep listener fail-closed; follow the credential incident runbook, never disable TLS verification |
| Snapshot exceeds transport capacity | Stop growth/recover under an approved plan; do not discard retained logs or raise isolated limits without end-to-end capacity tests |

The checkpoint-capable experimental log format is `YORLOG04`. Its fixed limits
are 32 MiB for the active file, 1024 frames, 4096 live entries and 8 MiB for the
serialized retained checkpoint. Replacement temporarily uses extra disk space;
file/directory sync errors poison the store until validated reopen. These limits
can stop an overloaded group and are not a substitute for capacity planning,
backpressure, multi-group isolation or supported-host durability tests.

Reopen can re-sync visible bytes after a process-only failure; it cannot make
missing acknowledged data reappear after a power loss, repair arbitrary damage,
or prove that unknown operations aborted. A validated primary state image may
allow removal of its abandoned temporary image; other temporary artifacts require
their own format-specific recovery rules. Never apply a generic “delete *.tmp”
repair to production data.

## Required release integrations

Daemon config validation/lifecycle tests, resource limits/compaction, consistent
backup and restore, replica replacement, explicit trust rotation, operator
commands, current-quorum/read barriers, multi-process failure coverage and
filesystem/hardware durability qualification remain tracked in
`multiraft-production-todo.md`. See also `multiraft-verification.md`,
`multiraft-security-operations.md`, `multiraft-migration.md`,
`multiraft-idempotency-design.md` (idempotency and the read barrier) and
`multiraft-backup-restore-design.md` (backup/restore design; nothing implemented).

Idempotency is implemented for `POST /v1/apply` and, at project scope, for block
UNMAP (`POST /block/{project}/{volume}/unmap`). A keyed apply commits its outcome
record in the same `RevisionTxn` as the control-state mutation, so a lost response
is resolvable with `POST /v1/idempotency/resolve` rather than by retrying. A
keyed UNMAP does the same with the extent-map publication: `unmap_at_with` appends
the record and the admission counter to the map's guarded transaction, so there is
no state in which the map advanced without the record or the record exists without
the map. Its record is project-scoped (`/idempotency/<project-id>/…`) and its
counter is group-scoped (`/idempotency/g<group-id>/_usage`), because a transaction
cannot span groups; projects sharing a group therefore share one budget. Resolve
takes `{"scope":"control","key":…}` or
`{"scope":"project","project":…,"key":…}`; a control resolve needs an admin
subject, a project resolve needs `readWrite` on that project. SQL, S3 and block PUT
reject a key with 501, as does every keyed write on the legacy runtime.
Admission for control is bounded by a replicated counter
(`/sys/idempotency/_usage`, `10_000` records or `8 MiB`, committed in the same
`RevisionTxn`), which refuses with `429` before planning; the project counter has
the same limits and refuses before the map is read.
The read barrier is consumed by OpenRaft block reads with `consistent=true`
(including iSCSI): the extent map comes from a quorum-confirmed snapshot on the
project-group leader and a missing blob is a `503`, never zeros. SQL, S3 and
`/v1/status` still serve the locally applied state.
