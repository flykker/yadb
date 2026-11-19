# OpenRaft transport: credential and incident runbook

Status: **not a production deployment guide**. The adapter implements pinned
TLS 1.3 mTLS plus Ed25519-signed envelopes; provisioning, rotation and revocation
are manual, offline operations and still need integration and failure tests.
No plaintext downgrade or production client routing is authorized by this guide.

## Trust boundaries

Each configured peer needs two separately provisioned identities:

1. A TLS leaf certificate/private key. Validate its CA chain, validity interval,
   server name and exact leaf SHA-256 pin. One leaf pin must map to exactly one
   node ID. The TLS session's peer ID must match the signed envelope sender.
2. An Ed25519 signing key with an out-of-band authenticated public-key registry.
   The registry is scoped to the exact cluster/group/incarnation and fixed voters.
   Never distribute signing seeds or TLS private keys to other peers.

An RPC additionally binds recipient, group/incarnation, per-connection random
server challenge, request sequence and client nonce. This prevents a captured
request being accepted on a fresh connection; it does not protect a compromised
authorized node, prevent denial of service or supply Byzantine consensus. The
standalone in-memory `ReplayGuard` is not restart-persistent replay protection.

TLS resumption and tickets are disabled. New connections perform certificate
validation; an already authenticated handler is not automatically revalidated
when time advances or files change. Replacing files does not update in-memory
trust. TLS and envelope public-key revocation require stopping listeners and
draining/aborting handlers, then restarting with validated replacement config.

The `testdata/tls` certificates and private keys are public **TEST ONLY** data.
Never provision them outside tests, never copy the deterministic test signing
seeds, and never treat a fixture's long validity period as a credential policy.

## Internal VDisk RPCs

`PUT|GET|DELETE /internal/vdisk/<pool>/<slot>/blob/<blob_hex>`,
`GET .../stats`, `POST .../barrier`, `GET .../blobs` and `POST .../compact` are
served on the same public listener as the client API unless `--internal-listen`
moves them to a dedicated peer listener, which then makes the public listener
answer `404` for every one of them. They are peer traffic: a slot whose data
lives on another node is reached through them by
`crates/yadb-blob/src/pool.rs`. Two of them are
destructive by design — `DELETE` installs a permanent exact-ID tombstone, and
`barrier` durably fences writes at or below a generation and evicts matching live
fragments (`crates/yadb-blob/src/volume.rs`).

Because `barrier` evicts a whole generation range in one record, its request also
carries the deadline of the collector lease that issued it
(`lease_expires_at_ms`). The target compares that deadline with its own clock
immediately before writing the barrier and refuses with `409` when it has passed,
so a barrier that waited in a queue or crossed a paused peer cannot delete for a
pass that lost its fence. A target that cannot read its own clock also refuses
(`BarrierClockUnreadable`, same `409`) instead of assuming the lease is live. The
request field is required, not defaulted: a caller
that omits it is rejected at deserialization rather than silently getting an
unfenced barrier.

That requirement makes a mixed-version rollout asymmetric, which is worth knowing
before an upgrade: a new collector sending a barrier to an older target succeeds
and is *not* checked (the old target ignores the unknown field and behaves as it
did before this change), while an older collector sending to a new target is
rejected with `422` naming the missing field. Both directions fail closed with
respect to *safety* only in the second case, so do not treat the presence of the
field as a deployment-wide guarantee until every target runs this version.

**They were unauthenticated until this delta.** Any caller able to reach the
listener could delete or fence a hosted slot without an admin subject and without
project authorization, could enumerate the slot's live fragment ids, or could
force a segment compaction, and could repeat the destructive calls across the
replica slots to defeat erasure redundancy. Pool names and slot indices are disclosed by the
unauthenticated `/v1/pools/health` and `/v1/status` routes. This bypassed the
control-plane authorization described above at the data layer; it did not alter
control state or invalidate an idempotency record.

**Fixed with the Raft peer token, not the admin subject.** All seven handlers now
call `raft_token_matches` before touching the store and answer `401` otherwise,
and the remote client sends `x-yadb-raft-token` on every call. The credential is
deliberately the peer token: an admin check would break every remote slot call and
would also misdescribe the trust relationship, which is node-to-node. The token is
threaded from `--raft-token` through `Slot::remote` into the request, so there is
no path where a remote slot is built without it. `raft_token_matches` returns
`true` only when no token is configured, which is the legacy single-node case; an
explicitly empty token is rejected rather than treated as a wildcard. In OpenRaft
mode a non-empty `--raft-token` is mandatory at startup, so a multi-node
deployment cannot reach the unauthenticated branch.

## Provisioning checklist (required before any operational deployment)

* Assign immutable cluster/group/incarnation and nonzero node IDs first. Require
  a separately authorized operation to change membership; do not edit voters to
  recover quorum. OpenRaft consensus membership and transport trust are separate
  controls and must agree before allowing RPCs.
* Generate keys on the intended node or an approved secret-management system;
  never use command-line secret values or log secret file contents. Use an
  offline/controlled CA and record issuance, ownership, expiry and incident owner.
* Bind server names to the expected nodes and verify the exact DER leaf pin and
  Ed25519 public key through an authenticated inventory. Reject duplicates,
  missing voters, mismatched local signing keys and unapproved identities.
* Provision secret files and locked storage directories with restrictive owner
  permissions and protected parent directories; verify symlink and replacement
  protections. Current adapters assume cooperating local processes:
  advisory locks do not defend against a privileged attacker or replaced paths.
* Use a dedicated internal listener reachable only by intended peer networks.
  Bind addresses explicitly, apply firewall rules and verify client networks
  cannot reach it. TLS is not a substitute for connection/memory/disk limits.
* Verify clock synchronization and certificate expiry alarms. Never bypass time
  or hostname verification to recover from an expired certificate or bad clock.
* Before enabling any runtime, validate CA, name, pin, local key matching,
  membership and on-disk replica identity. A validation error must not trigger
  plaintext fallback, a new bootstrap or silent replacement of existing data.
* Configure control-plane administration explicitly. `/v1/apply` requires a
  subject proven by an HMAC-SHA256 `x-yadb-subject-signature` under
  `--subject-secret` and listed in `--admin-subject`. An OpenRaft daemon with an
  empty allowlist refuses to start, as does any daemon whose allowlist has no
  secret. This is a shared-secret check, not per-operator credentials: every
  holder of the secret can sign any allowlisted subject, so distribute it only to
  the administrative client hosts and rotate it with the offline procedure below.
* A keyed `/v1/apply` records a durable outcome under
  `/sys/idempotency/<sha256(subject)>/<sha256(key)>` in the control group, so an
  operator who loses a response can call `POST /v1/idempotency/resolve` instead of
  retrying blindly. Two consequences for operations: the record stores the
  response body (bounded to 8 KiB, base64) and its end-to-end headers, so do not
  put secrets in an apply response; and admission is bounded by a replicated
  counter at `/sys/idempotency/_usage` (`10_000` records or `8 MiB` of encoded
  records, committed in the same guarded `RevisionTxn`). Exhaustion is a clean
  `429` before planning, which matters because the durable state image is capped at
  64 MiB and a committed entry that cannot be serialized within that cap would make
  the control group stop advancing rather than reject the write. The byte budget is
  deliberately far below that cap. Treat the key namespace as attacker-reachable
  whenever the admin secret is reachable; an attacker can still exhaust the quota
  (denying new keys, not replay or resolve of existing ones), and a full `/sys/`
  remains a control-group outage.
* Internal VDisk RPCs are peer traffic, not client traffic: they require the
  `--raft-token`, and the token is the credential that protects `DELETE` and
  `barrier` on a hosted slot. See "Internal VDisk RPCs" above.
* A keyed block UNMAP records its outcome in the project's own group under
  `/idempotency/<project-id>/<sha256(subject)>/<sha256(key)>`, and its admission
  counter is group-scoped at `/idempotency/g<group-id>/_usage`. Both halves are
  hashed for the same reason as control: a group image and a Raft snapshot are
  readable by anyone who can inspect storage, and a record must not disclose which
  keys or subjects a project used. Authorization differs from control by design —
  `resolve` for a project record needs `readWrite` on that project, not the admin
  allowlist, because the record exists only because that identity was allowed to
  mutate the project. Projects that share a Raft group share one counter, so one
  project can exhaust the record budget of another in the same group; that is the
  intended consequence of the counter having to live in the group it bounds.
* `POST /v1/pools/scrub` mutates (it collects orphan blobs, rewrites fragments and
  records a summary) and therefore requires the same signed, allowlisted admin
  subject as `/v1/apply`. There is no unauthenticated deployment to preserve: a
  daemon with an empty `--admin-subject` refuses to start, so every reachable
  scrub is checked. (`crates/yadbd/src/main.rs:7912` enforces the allowlist; the
  comment above the handler states that `require_admin`'s empty-allowlist branch
  is kept for library callers and tests, not as a reachable daemon mode.)

## Current rotation procedure: coordinated offline restart only

The current exact-pin map does not implement overlapping certificates, dynamic
reload or a dual-key epoch. Therefore **no safe rolling rotation is promised**.

1. Obtain approval for an inventory-bound new credential set and keep recoverable
   copies of the previous configuration under secret-management policy.
2. Quiesce clients of any separately authorized new-empty-test Multi-Raft cluster,
   then stop its listeners and await all handler and Raft shutdowns on every peer.
   There is no legacy Raft runtime any more, so a rollback to it is not merely
   discouraged: it does not exist. The previous configuration must therefore be
   kept recoverable for its own sake, not as a fallback path.
3. Provision all new CA chains, leaf pins, names, envelope public keys and local
   secrets consistently. Do not change replica identity, voters or data paths.
4. Validate every peer's configuration offline, including local certificate/key
   ownership and exact peer maps. Record inventory digests without recording keys.
5. Reopen existing replicas explicitly; never bootstrap after a rotation error.
   Check authenticated connectivity, leader election and applied-state progress.
6. If any peer rejects credentials, stop the runtime and investigate.
   Reverting trust is allowed only if the previous credentials are not compromised
   and operator policy explicitly authorizes it. Never fall back to plaintext.

Before replacing this with rolling rotation, implement versioned trust epochs,
overlap/retirement rules, crash recovery and tests proving removed credentials
cannot authenticate through stale listeners, resumed sessions or old handlers.

## Compromise or revocation incident

1. Network-isolate the affected node and stop the listeners on all
   peers; preserve logs, configuration digests and a copy of the affected data
   directories as evidence before any recovery step.
2. Do not assume revoking a TLS leaf alone is sufficient: the attacker may possess
   the envelope signing key, an active session or write access to local files.
3. Remove/replace all compromised trust material consistently using the offline
   procedure. The adapter does not consult live CRL/OCSP or implement key epochs.
   If the subject secret may be exposed, rotate `--subject-secret` and the
   `--admin-subject` allowlist on every node in the same offline restart; a stale
   secret keeps signing valid admin requests until it is replaced.
4. Review acknowledged/unknown proposals and replica integrity. Raft is not BFT:
   do not assume an authorized malicious peer could not corrupt application state.
5. Membership removal requires a validated OpenRaft reconfiguration protocol or
   a separately approved, offline state import. Deleting a voter from the transport
   map is not a consensus membership change and may destroy availability.
6. If quorum or provenance cannot be established, remain fail-closed and recover
   under the migration authority protocol; never force a stale singleton.

## Evidence still required

Missing-client-cert and wrong CA/name/pin/node tests, replay across restart,
listener cancellation and overload tests are necessary but do not complete this
runbook's deployment gate. Add automated provisioning validation, clock/expiry
scenarios, full-cluster offline rotation, compromised-key revocation, stale
handlers, membership/trust disagreement, secret permissions and rollback tests
on supported deployment systems. Independent security review remains mandatory.
