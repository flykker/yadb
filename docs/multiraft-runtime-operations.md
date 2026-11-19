# OpenRaft runtime operations and recovery boundaries

## Status snapshot (2026-11-14), superseded where noted

This section is a dated record, not a statement about the current tree. Since it
was written the legacy multi-node Raft runtime was **removed** rather than kept as
a default: `--metadata-runtime` is pinned to `openraft`, `--openraft-config` is
mandatory, and there is no fallback runtime
(`crates/yadbd/src/main.rs:58`, where the CLI value parser accepts only
`openraft`, and `crates/yadb-metakv/src/raft.rs:4-21`). The
only non-replicated store left is the single-node `LocalKv`, which is a test and
library facility. Statements below about a "legacy default", a "legacy mode" or a
legacy `checkpoint.json` describe the tree as it was on 2026-11-14 and no longer
hold. Where a sentence is still true, it has been left alone.

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

Local three-process verification on 2026-11-17: `tests/protocols.py` passed 3/3
consecutive runs, each 20 PASS / 4 SKIP, on a binary built from a tree with no
instrumentation, with zero `BRIDGE TIMEOUT`, `TLS CONNECT FAIL` and `REPLY WRITE
FAILED` events. That run covers the delta-log apply path, the transport deadline
floor and concurrent dispatch. It does not cover per-group GC, which was refused
at that snapshot, or an `InstallSnapshot` transfer between processes, which
stays a `SKIP`.

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
the map read and the fetch is a `503` rather than zeros. At the time of this
snapshot SQL, S3 and status reads were still served from local applied state;
SQL and S3 `consistent` opt-in reads were added later (see below). At that
snapshot, S3 GC was disabled. `/v1/apply` requires a signed subject on the `--admin-subject` allowlist;
an OpenRaft daemon refuses to start without one, and any allowlist requires a
non-empty `--subject-secret`. Unsigned or forged subjects get 401, a proven
non-admin subject gets 403. This is a shared-secret check, not per-operator
credentials: anyone holding the secret can sign any allowlisted subject.
Focused shutdown cancellation passes retain the borrowed owner and retry;
startup cancellation remains unsafe. The POSIX compaction stale-FD claim was false;
explicit reopen and an append regression are retained only. At the time of this
snapshot the legacy runtime was still the default, no existing cluster had been
switched, old data was untouched, no migration had occurred, and
`production_ready: false`.

Status: **not production-ready**. At the time of this snapshot the legacy runtime
was the default; no existing or production cluster had been switched. The user
had authorized implementation of actual OpenRaft client routing for a new
nonproduction cluster only. That
local loopback cluster is running at `/public/cluster_data` under supervisor
PID **16793** (children **16795**, **16796**, **16808**), all nine group configs
in `mode: open, initialize: false`, binary SHA-256
`5775b7bc278210deb7f6c8b91a3ca42ac94bb26daf3b3aade8b29583b7d85773`.
The deployment is **not empty**: at 21:25:24 on 2026-11-14 an unidentified apply
committed exactly `examples/platform.yaml` (generation `1`, project `billing`);
the orchestrator did not issue it and `yadbctl` was not executed. The state was
preserved, not rolled back. Live freshness probes and the internal
snapshot token gate passed with revision `2`. It is a test
deployment, not production
routing or migration. The feature flag alone does not authorize migration,
production routing, rolling upgrades or disaster recovery by editing voters. The
final independent GPT-6.1 Sol review of the freshness delta and the
nonblocking-refresh fix returned ACCEPT for the narrow local scope
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
the status snapshot). The
version-1 standalone interface below remains observational, not a client-routing
configuration.

The daemon's `openraft-storage` build feature is a retained no-op: the OpenRaft
adapters are always compiled in and there is no other metadata runtime, so an
invocation that still passes `--features openraft-storage` keeps working and one
that omits it is not weaker. `--openraft-config <path>` is mandatory and must
name a version-2 multi-group manifest. These names previously carried an
`experimental` prefix; the old spellings (`--experimental-openraft-config`, the
`experimental-openraft-storage` feature and `/experimental/openraft/status`) are no
longer accepted. There is no version-1 single-group daemon mode and no fallback:
the daemon creates one dedicated mTLS listener per group and never creates a
single-node metadata WAL.

OpenRaft `consistent=true` block reads are supported: the extent map is read
through a quorum-confirmed snapshot on the project-group leader and the blobs it
names are immutable and published before the map, so the returned bytes reflect a
committed state. That snapshot is **not** a GC fence: if a newer write replaced a
block and a barrier collected the old blob, the read fails with `503`
(`BlockError::Inconsistent`) rather than returning zeros, and the engine does not
retry. A follower forwards the read to the leader instead of serving its local
view; `NotLeader` is an error, not a stale fallback. There is no legacy daemon
mode to keep behaviour for: `--metadata-runtime` is pinned to `openraft` and a
daemon without `--openraft-config` refuses to start.

Version-1 JSON configuration fields:

| Field | Meaning |
| --- | --- |
| `version` | Exactly `1`; unknown fields are rejected |
| `root` | Existing, durably provisioned root directory |
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

`GET /openraft/status` is observational, not a client write API or
readiness/cutover authority. When the daemon runs with `--subject-secret` the
route requires a verified signed subject (`x-yadb-subject` together with a valid
`x-yadb-subject-signature`) and returns `401` otherwise; without a configured
secret no subject can be verified, so the route stays open. `--subject-secret` is
mandatory, so in a supported deployment the route is always authenticated. It
reports `production_ready: false`; `client_traffic` is `multiraft` for the
multi-group owner and `single-group` for a single-group one. Group node/state/term/
leader are cached local observations, alongside `last_log_index`,
`last_applied`, `millis_since_quorum_ack` and (on a leader) `replication`, which
maps each peer to the highest log id it has acknowledged. These narrow a stalled
write; they do not attribute one. `last_log_index` ahead of `last_applied` means
this node holds entries it has not applied, and equality means nothing is pending
locally, so the stall is upstream — but the endpoint does not know which index a
given client write received, so it cannot prove that *that* write is the one
waiting. A peer whose `replication` entry is `None` has not acknowledged anything
in the current term, which is the shape a lost or timed-out append leaves behind.
The `transport` object reports
process-wide counters for events that otherwise leave no trace — connections
dropped because all handler slots were busy (`connections_cap_dropped`),
connections that ended before dispatch or while reading the request frame
(`connections_failed`), envelopes rejected by decode, signature or challenge
(`envelopes_rejected`), outgoing RPCs that ended without a verified reply
(`outgoing_rpc_failures`), and snapshot transfers that failed validation or
timed out (`snapshot_failures`). `outgoing_rpc_failures` counts a cancelled RPC
too: OpenRaft's own heartbeat deadline drops the future mid-flight, which is
precisely the append timeout worth seeing. A snapshot failure is counted once, in
`snapshot_failures`, and not a second time in `envelopes_rejected` or
`connections_failed`, so the two numbers can be added without double counting.
They are monotonic
since process start and are not a health verdict: a nonzero `outgoing_rpc_failures`
is expected under churn, while a nonzero `connections_cap_dropped` means the
64-handler cap was reached and a peer was refused rather than delayed. Metrics,
authenticated operator controls and
daemon integration validation remain incomplete. Protect the public diagnostics
listener according to the deployment's existing access policy.

OpenRaft reports replication failures, election changes and commit events through
`tracing`. `yadbd` installs a subscriber only when `RUST_LOG` is set, so a normal
start writes nothing extra:

```
RUST_LOG=openraft=info yadbd --openraft-config ...
```

`RUST_LOG=openraft=debug` adds per-append detail and is very verbose — one suite
run produced 310 MB — so use it for a single targeted reproduction, not for a
long-lived daemon. Without the subscriber those events are discarded, which is
why a stalled commit once left no daemon-side trace at all.

## Provision storage before starting

Use ordinary local filesystems with working file/directory sync and cooperating
OS directory locks. Provision a protected root directory first. Do not rename,
replace or share a locked directory with legacy code, which does not participate
in these locks. Network filesystems and hostile local path mutation are outside
the current ownership contract.

`scripts/yadb-openraft-dev.py` does this provisioning for a disposable
three-process cluster: `prepare` writes credentials, per-node group configs and
the version-2 manifests, and `start` refuses to reuse a root that holds node data
without a matching `state.json`. Use it when you
need the PostgreSQL, S3 and iSCSI frontends as well; `scripts/yadb-local.py`
provisions a Multi-Raft cluster without the client protocol listeners.

Multi-Raft mark-and-sweep runs per metadata group. Each pass is leader-only and
marks from a quorum-confirmed snapshot of its group; a live writer or reader
lease refuses that group, and a pool whose epoch advance is refused (a live
pin, or an unreadable slot) is refused by name and deletes nothing. Readers
publish a per-group reader lease as well as pinning each fragment at the
target, so a group that has served a read in the last 30 s refuses collection
until that lease lapses.

Storage-side fencing crosses the wire. A barrier carries the collector lease
deadline, and a target refuses an expired one with `409` instead of evicting a
whole generation range for a pass that lost its fence (an unreadable target clock
refuses the same way). A volume keeps a durable epoch and stamps each fragment
with the epoch it was written under, so `delete_fenced` refuses a fragment from
another epoch. A volume keeps a durable pin table, so an epoch advance refuses
while an unexpired pin exists and `delete_collectable` refuses a fragment that is
not older or whose epoch is still pinned. A remote slot exposes all of it on the
internal VDisk routes, and the collector advances every slot of a pool before its
first deletion, so a pin taken concurrently either stops the advance or names the
new epoch. The plan and the four live drills are in
`multiraft-gc-fencing-plan.md` and `tests/protocols.py`.

The metadata half of the lease is now stricter than the paragraph above
suggests, and the difference matters when reading the plan. `ensure_live` refuses
a record whose `expires_at_ms` has passed even when the record is still present
and still carries the holder's `mod_revision`; all three renewal paths
(`gc::renew`, `GcLeaseGuard::renew`, `CollectorGuard::renew`) share one
implementation and refuse to extend such a record instead of resurrecting it.
That implementation also refuses a renewal whose write is acknowledged at or
after the deadline it was extending, because the compare-and-swap proves
ownership but not liveness: a record that expired during the round trip was
otherwise extended and the holder continued under a fence that had already
lapsed. The write is left in place when that happens, so admission stays refused
until the record expires on its own. A reader or writer therefore cannot
act under a lease the collector already considers dead, and `begin_read_lease`
publishes a fresh record instead of reusing a dead one. What remains unproven is
exactly item 3 of the plan: the record lives in metadata, the deletion happens on
a storage target that never sees it. The reader lease added on Multi-Raft does
not change that: it is a back-off signal the collector consults before its mark,
while the pin is the target-side fence. The durable target epoch above is a
target-side fact, not a lease the target can evaluate; it only says which epoch a
fragment belongs to.
Manual GC runs per metadata group: it answers `200` when every group and pool
completed, and `409` with `gc_refusal` naming any that refused. Scrub runs the
same collection and then its own per-group repair scan: it answers `200` only
when `repair_error` and `gc_refusal` are both `None`, and otherwise `409` with
the summary body, the same rule `/v1/pools/gc` uses.
Both report `blobs_live`, `fragments_deleted`, `bytes_reclaimed`, `leases_live`,
`gc_groups_refused`, `gc_pools_refused` and an explicit `gc_refusal`. A group
whose leadership, leases or storage fence cannot be proved is refused by name
and deletes nothing; a pool whose epoch advance is refused (a live reader pin,
or an unreadable slot) is refused by name and deletes nothing. The four live
drills in `tests/protocols.py` cover a live pin, a superseded collector epoch,
a non-leader refusal and a clean collection.

Scrub's repair half runs per group too. `gc::repair_scan_groups` takes a
quorum-confirmed snapshot of one group over `/db/`, `/s3/` and `/block/`, builds
the live set from that snapshot only (never `try_scan_prefix`) and runs
`repair_blob` on each live blob of the group. That boundary is exact, not a
compromise: every data namespace routes a project id to exactly one data group,
so a project's manifests are never split across groups, and a blob id embeds its
project id, so one group's snapshot is the complete live set for that group's
blobs. Repair deletes nothing except the handoff copy of a part already written
to its nominal slot, and a tombstoned id refuses that write, so a pass cannot
resurrect a collected blob. A follower group, or a group without a quorum, is
refused by name in `repair_error`; one group's refusal does not stop another
group's pass, so on a multi-node cluster a node leads only some data groups,
repairs those, and answers `409` naming the ones it does not lead. That `409`
is not a failed pass: read the counters for the groups this node leads, and run
the same request on each named group's leader to cover the rest. `yadbctl
pool-scrub` cannot express that distinction — it prints the body and exits `1`
on any non-`200` — so a scripted scrub must inspect `repair_error` rather than
the exit status.

Three limits stay stated. `block_pool_lookup` comes from the local control-plane
cache, so a stale cache can make a pass skip a blob; it never repairs or deletes
the wrong one. A blob published after the snapshot is not repaired by that pass
— repair is idempotent, not atomic with publication. A group leader dying
between a fragment write and its manifest commit still has no drill.

The upload-intent grace is a per-engine knob,
`ObjectStoreEngine::set_intent_grace_secs`, default 300 s (unchanged) and
provisioned only by the test harness, which sets 1 s so an abandoned intent is
collectable inside a run. A live drill leaves an abandoned intent through a pass
inside the grace and observes it collected after it. Orphan collection also runs
per group; a group the node does not lead is named in `orphan_collection_error`
rather than counted as collected. Orphan collection still needs an enumerable
project set: a scope that covers every project (`project_ids == None`, as in a
single-group deployment) is refused instead of scanned partially.

Multi-Raft readers publish a *metadata* read lease, renewed across reads and
scoped to the project's metadata group, as well as pinning the fragment at the
target. `AppState::begin_read_lease` resolves the owning group with
`gc_group_for_project` and caches one record per group (a map keyed by
`Option<u64>`, `None` being the legacy namespace); the record carries the
group's applied log index as its revision, and `lease_belongs_to_group` checks
that provenance when the collector closes admission. The target-side pin is
still the finer fence: `read_part` reads the slot epoch and pins it, and
`delete_collectable` refuses while any pin is live, so a read that outlives its
metadata lease is still fenced per fetch by the pin at the point of use.

The operational consequence is deliberate: `POST /v1/pools/gc` and the
collection half of `POST /v1/pools/scrub` refuse a group whose reader lease is
live, for up to the 30 s TTL after the last read in that group. A collector
against a recently read group therefore refuses rather than collects; run it
after a quiet window, or read the refusal and retry. The lease is coarser than the pin — it
fences the whole group, not the fragment being read — and it buys a
collector-visible back-off signal that is consulted before the mark is taken.

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

## Pool placement: nominal slots, handoff slots and the write quorum

A pool is striped over `total_parts()` *nominal* slots, and part `i` of a blob
lives on nominal slot `(hash(blob) + i) % total_parts()`. Adding slots to a pool
therefore never moves an existing part: the modulus is the nominal count, not the
slot count.

A `StoragePool` may declare `handoffSlots`. Those are spare slots appended after
the nominal ones and used only when a nominal slot does not answer, following
YDB's handoff disks: the part is written to a spare on a *different* node and the
write is still acknowledged, because every part is still durable. Without
handoff slots a single unreachable slot refuses the write
(`RedundancyNotMet`), which is the historical default and is unchanged.

Two rules keep a handoff placement honest:

* one spare holds at most one part of a given blob, because YDB counts effective
  replicas per disk, and two parts on one disk are one replica;
* a spare on the node whose nominal slot failed is skipped — that node is the
  one that did not answer.

A slot that *answers* with a refusal is not a slot that did not answer. An
`ImmutableConflict` (the id already holds different bytes), a `FencedByBarrier`
or a `Tombstoned` refusal, and a `StorageUnavailable` volume, are all permanent:
the write is refused with that exact error, nothing is retried on a spare, and
nothing is acknowledged, because a handoff copy would describe bytes that reads
never return — or resurrect a fenced blob from a copy the fence did not cover.
Only an I/O failure is handoff-eligible: a connection failure or an unexpected
HTTP status on a remote slot, or an I/O failure on a local volume.

The handoff search ends only when no unplaced part has an untried eligible spare
left, never merely because a round made no progress: one unreachable spare must
not end the search while a healthy spare beside it is still untried. Every round
consumes at least one spare, so the search is bounded by `handoffSlots`.

A handoff ack keeps the *erasure* layout whole (all `total_parts()` fragments
exist), but it does not keep the nominal spread across `failureDomains` until the
parts are assimilated back: a scrub pass moves a handoff part to its nominal slot
and only then deletes the spare copy. Size `handoffSlots` so the declared mount
count covers `total_parts + handoffSlots`, and expect a degraded write window
after a node loss rather than assuming domain-level redundancy throughout it.
The daemon exposes the backlog as `degraded_puts` in `/v1/pools/health`; it stays
zero for a handoff write, because a handoff write is not degraded — it is fully
placed, just not on the nominal slots.

`minWriteQuorum` is a separate, explicit opt-out from full placement. Its floor is
the pool's read threshold, because a lower quorum could acknowledge a write the
pool cannot read back, and its ceiling is `total_parts()`. A pool that sets it
below `total_parts()` accepts writes it cannot serve with full redundancy and
counts them in `degraded_puts`; the default is `total_parts()`, so an
acknowledgement promises every fragment.

## Apply path: checkpoint plus delta log

A replica's state directory holds two files. `state.json` is the full image, and
`state.delta` is an append-only log of the entries applied since that image was
written. Every apply batch appends one framed record to `state.delta` and syncs
it *before* the in-memory view changes, so memory is never ahead of the file. The
full image is republished only when the delta passes 4096 entries or 8 MiB, and
when a snapshot is built or installed.

This exists because republishing the image per apply batch was the post-restart
commit stall: each batch serialized the whole map, wrote a temp file, synced it,
renamed it and synced the directory, and the rename plus directory sync alone was
measured at up to 1.6 s under concurrent writers. The apply pointer then fell
behind the committed log faster than it advanced, and the 6 s bridge deadline
expired on a cluster that was replicating normally.

Consequences for an operator:

* A `state.json` older than `state.delta` is normal, not damage. The delta is
  replayed on open, and frames at or below the checkpoint's applied index are
  skipped so a failed fold is harmless.
* A torn *final* frame is truncated on open — in both `state.delta` and the Raft
  log. That is the only automatic repair in the runtime, and it is safe by
  construction: an append writes one whole frame per `write_all`, the directory
  lock admits a single writer, and the sync happens before the entry is
  acknowledged, so a frame running past the end of the file is an
  unacknowledged append. Nothing else about a damaged state directory is
  repaired automatically: a bad header, a checksum mismatch or a gap in the
  delta is refused, never truncated.
* A failed append truncates the file back to its previous length before
  returning the error, so an acknowledged entry is never absent from disk.
* Copying a replica's state directory for evidence means copying both files.

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

`client write timed out` from the metadata bridge means the bridge's 6 s deadline
expired before the client write returned. The commit outcome is unknown to the
caller: the bridge aborts its task, not the Raft entry, so the proposal may still
commit. For a keyed `/v1/apply`, resolve the outcome with
`POST /v1/idempotency/resolve`; for any other write, reconcile application state
before retrying.

A bridge timeout is not by itself evidence that the entry failed to replicate.
OpenRaft answers `client_write` from `handle_apply_result`, i.e. only after the
state machine has applied the entry, so `last_log_index` ahead of `last_applied`
while the transport reports successful `AppendEntries` means the apply path is
behind, not that the proposal was lost. Read both indices from
`/openraft/status` before deciding whether to reconcile.

Two runtime behaviours are required for the bridge to keep up with concurrent
writers and are set explicitly rather than inherited:

* **RPC deadline floor.** Every Raft RPC opens a fresh connection and performs
  the pinned mTLS handshake, which measured about 120 ms at p50 and near 960 ms
  in the tail on loopback. OpenRaft derives the `AppendEntries` deadline from
  `heartbeat_interval`, and 0.9.25 wraps the call in
  `C::timeout(heartbeat_interval, ...)` itself, so the transport's own deadline
  cannot extend an append: a miss becomes `RPCError::Timeout`, which is not
  retried. OpenRaft reports it as a progress error, and for a heartbeat request
  that error resets nothing, so no retry follows until a later event. `heartbeat_ms` is
  therefore the real append budget and must be sized above the transport's
  worst-case handshake; both launchers use 750 ms against a measured 1.8 ms
  median / 2.4 ms max proxied round trip, with election timeouts at
  2250/3000/3500 ms (the validator requires
  `election_min_ms >= 3 x heartbeat_ms`, which bounds the heartbeat from above,
  not below). `TransportConfig::rpc_ttl_floor`
  raises the deadline for the RPC types OpenRaft does not wrap, and the daemon
  sets it to one `election_timeout_min`. `None` keeps OpenRaft's own deadline,
  which the in-process test transports rely on.
  Snapshot transfers use `install_snapshot_timeout` (200 ms by default) as their
  hard TTL, which the same floor raises: a snapshot cannot cross a fresh mTLS
  handshake in 200 ms either.

  Guarded for new roots: `prepare` refuses a `mode: create` config whose
  `heartbeat_ms` is below `MIN_HEARTBEAT_MS` (500 ms), because a budget shorter
  than the transport round trip abandons appends the follower already applied.
  The number is anchored on measurement, not derived: 250 ms produced 668
  abandoned `AppendEntries` and a stalled commit index, 750 ms produced none.
  An existing root (`mode: open`) with a shorter value still starts, with a
  warning on stderr, so a live deployment is not turned into an outage; treat
  such a config as needing migration to 750 ms. The floor rejects gross
  misconfiguration and is not a proof of adequacy — it is not derived from the
  transport's worst-case handshake, and `election_min_ms >= 3 x heartbeat_ms`
  bounds the heartbeat from above, not from below. Both launchers provision
  750 ms.
* **Concurrent dispatch.** The bridge hands each write to the runtime without
  waiting for the previous one to finish. `client_write` is safe to call
  concurrently, and it is the only way the runtime can batch entries into one
  apply and one state-image write; awaiting each write first turns N concurrent
  writers into N serial round trips and persists one entry at a time.

Connection reuse per peer remains a legitimate follow-up. The floor only makes
the existing per-RPC cost tolerable; it does not remove it.

S3 upload allocation ids are handed out in blocks of 32. A writer's first upload
after its cache empties advances `/s3-upload-sequence/{project_id}` by 32 in one
guarded write and serves the rest of the block locally, so a burst performs one
sequence transaction per process per round instead of one per upload. A process
that exits with ids unused loses that tail; the ids are never reused. The ids
handed out are consecutive within a block (`previous+1`, `previous+2`, ...), so
the durable counter advances by 32 per block while `generation` advances by 1 per
upload. A BlobId takes `owner_id` from the high 32 bits of the allocation id and
`generation` from the low 24, so `generation` wraps at the 24-bit boundary while
`owner_id` increments; uniqueness comes from the pair, not from `generation`
alone. `MAX_ALLOCATION` is `2^56-1`, so `owner_id` still fits 32 bits.

## Fail-closed recovery matrix

| Condition | Required action |
| --- | --- |
| Directory lock busy | Identify the current owner; do not bypass locks or replace the directory |
| Partial bootstrap | Preserve artifacts and diagnose; do not auto-create missing files or initialize again |
| Replica identity/voters mismatch | Stop and verify the intended configuration and provenance; never rewrite the descriptor to fit |
| Unsupported log version | Preserve the original bytes; use a separately reviewed migration or explicit disposable-test reprovisioning, not silent replay |
| Corrupt/truncated log tail or state image | Preserve raw evidence, stop serving, and recover from validated replicas through an approved procedure. The only automatic truncation is a torn *final* frame in the Raft log or in `state.delta`, whose unacknowledged status follows from the append order; a bad header, checksum mismatch, delta gap or any mid-file damage is refused, not repaired |
| Write/sync/rename/directory-sync error | Treat the operation outcome as unknown; the handle is poisoned, and further participation requires reopen/recovery |
| Quorum unavailable | Restore legitimate peers/connectivity; never reduce voters, accept singleton writes or treat timeout as abort |
| Certificate/pin/signing-key rejection | Keep listener fail-closed; follow the credential incident runbook, never disable TLS verification |
| Snapshot exceeds transport capacity | Stop growth/recover under an approved plan; do not discard retained logs or raise isolated limits without end-to-end capacity tests |

The checkpoint-capable log format is `YORLOG05`. Its fixed limits
are 32 MiB for the active file, 1024 frames, 4096 live entries and 8 MiB for the
serialized retained checkpoint. Replacement temporarily uses extra disk space;
file/directory sync errors poison the store until validated reopen. These limits
can stop an overloaded group and are not a substitute for capacity planning,
backpressure, multi-group isolation or supported-host durability tests.

The single-node metadata WAL distinguishes a torn tail from damage. A frame is
truncated only when it provably runs past the end of the file: fewer bytes remain
than a header needs, a short read at EOF, or a declared length past EOF. A
complete frame with a bad magic, a bad header or payload checksum, or a payload
that is not a `WalRecord` is `CorruptWal`, and the file is left byte-identical for
inspection. Volume segments follow the same rule: a complete record header that
fails its CRC or an unknown record kind refuses to open, because truncating there
would drop later `TOMBSTONE`/`BARRIER` records and could resurrect deleted blobs.

Two operational consequences are deliberate and not defects:

* **The format is now `YADBWAL2`.** `YADBWAL1` files still replay under the old
  tail rule and keep appending in their own format, so no existing file changes
  behaviour on upgrade. A **rollback to a binary older than this change is
  destructive**: it does not recognise `YADBWAL2` and will treat the whole log as a
  tail and truncate it. Do not roll back a node that has written a `V2` log.
* **A zero-filled tail now fails closed.** Some filesystems leave allocated but
  unwritten zeros after a crash. Such a region is not one of the provable torn-tail
  shapes, so the node refuses to start instead of truncating. That is the safe
  direction — nothing is silently discarded — but it means a crash can require
  operator action rather than automatic recovery.

Reopen can re-sync visible bytes after a process-only failure; it cannot make
missing acknowledged data reappear after a power loss, repair arbitrary damage,
or prove that unknown operations aborted. A validated primary state image may
allow removal of its abandoned temporary image; other temporary artifacts require
their own format-specific recovery rules. Never apply a generic “delete *.tmp”
repair to production data.

Blob fragments follow the same rule at the data layer: a fragment that is present
but fails its CRC32 is reported as corruption (`BlobError::CorruptData`, `422` on
the internal VDisk route, `500` on the public object and block routes), never as
`not found`. When enough healthy fragments remain the blob is reconstructed
transparently and `POST /v1/pools/scrub` rewrites the damaged fragment; corruption
is surfaced only when reconstruction is impossible, and then as corruption rather
than as a fragment shortfall. Unreachable replicas are deliberately not promoted
this way, because an offline node says nothing about the data and a promoted
error would turn the consistent-read “map is not a GC fence” `503` into a `500`.
Operationally: `404`/`NoSuchKey` means the object was never written or was
deleted, while `422`/`500` with a corruption reason means it exists and must be
repaired from a replica — never re-created silently.

## Required release integrations

Daemon config validation/lifecycle tests, resource limits/compaction, replica
replacement, explicit trust rotation, operator commands, current-quorum/read
barriers, multi-process failure coverage and filesystem/hardware durability
qualification remain tracked in `multiraft-production-todo.md`. See also
`multiraft-verification.md`, `multiraft-security-operations.md`,
`multiraft-migration.md` and `multiraft-idempotency-design.md` (idempotency and
the read barrier).

Idempotency is implemented for `POST /v1/apply` and, at project scope, for block
UNMAP (`POST /block/{project}/{volume}/unmap`), block write
(`BlockVolumeEngine::write_at_with`) and S3 object PUT/DELETE
(`ObjectStoreEngine::put_object_with` / `delete_object_with`). A keyed apply
commits its outcome record in the same `RevisionTxn` as the control-state
mutation, so a lost response is resolvable with
`POST /v1/idempotency/resolve` rather than by retrying. A keyed UNMAP does the
same with the extent-map publication: `unmap_at_with` appends the record and the
admission counter to the map's guarded transaction, so there is no state in
which the map advanced without the record or the record exists without the map.
Block write and S3 PUT/DELETE use the same guarded-transaction shape with the
engine methods named above. The record is project-scoped
(`/idempotency/<project-id>/…`) and the counter is group-scoped
(`/idempotency/g<group-id>/_usage`), because a transaction cannot span groups;
projects sharing a group therefore share one budget. Resolve takes
`{"scope":"control","key":…}` or
`{"scope":"project","project":…,"key":…}`; a control resolve needs an admin
subject, a project resolve needs `readWrite` on that project. SQL writes reject a
key with 501, as does every keyed write on a single-node metadata store.
Admission for control is bounded by a replicated counter
(`/sys/idempotency/_usage`, `10_000` records or `8 MiB`, committed in the same
`RevisionTxn`), which refuses with `429` before planning; the project counter has
the same limits and refuses before the map is read.
The read barrier is consumed by OpenRaft block reads with `consistent=true`
(including iSCSI), by the SQL `consistent` body flag and by S3
`?consistent=true` GET: the block extent map, SQL namespace or object manifest
comes from a quorum-confirmed snapshot on the project-group leader, and a block
blob collected between the map read and the fetch is a `503`, never zeros. Those
reads are opt-in; default SQL/S3 reads and `/v1/status` still serve the locally
applied state.
