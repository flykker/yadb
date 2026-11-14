# Offline Multi-Raft migration and recovery protocol (draft v1)

Status: **specification only; migration is not implemented or authorized**.
No command currently exports/imports this format, enforces its writer fence or
performs cutover. For existing deployments, keep all SQL, S3, block, iSCSI and
legacy Raft traffic on the legacy runtime. The separately authorized fresh,
empty test cluster is outside that restriction; it does not authorize importing
or reusing existing legacy data. An experimental OpenRaft
group is not a migration destination
until the [production checklist](multiraft-production-todo.md) is complete.

## Scope and invariants

The first supported migration must preserve the entire metadata transaction
domain in one OpenRaft group. Splitting SQL rows, index manifests, allocation
counters or multi-key operations into different groups requires a separate
cross-group transaction/read protocol; it is not part of v1.

* Exactly one runtime may accept writes. A process-local lock, stopped leader or
  loss of quorum is not a cluster-wide fence. Fence every legacy writer, public
  listener, administrative tool and background maintenance path, including old
  binaries that cannot read a new-format fence record. Require an external
  deployment/network fence and verification that fenced processes are stopped.
* A metadata-only backup is insufficient. Preserve referenced fragments, retained
  historical fragments, volume tombstones/barriers and allocator high-water
  marks together. Never classify unknown blobs as garbage during migration.
* Source revisions, per-key versions and modification revisions must survive
  import unchanged. Replaying every key as a new `Put` does not preserve these
  semantics. A future import API must validate an exact state image; normal
  client writes are not an import interface.
* Do not copy legacy Raft terms, votes, uncommitted log tails or membership into
  OpenRaft logs. Establish a new group/incarnation and bootstrap consensus using
  OpenRaft's supported protocol. Bind the imported application state to that
  new group's committed import record and snapshot metadata.
* An error after a write/sync/rename or a lost acknowledgment has an unknown
  outcome until recovery verifies durable artifacts. Never infer rollback from
  a timeout or automatically initialize a missing/partial destination.

## Proposed manifest v1

An eventual strict, bounded decoder must reject unknown versions and fields,
duplicate identities/paths, invalid numbers, unsafe paths, missing files and
inconsistent checksums before allowing an import. SHA-256 detects corruption;
it is not authorization. Authenticate the manifest and backup provenance with
an operator-controlled signing key and separately provisioned trust roots.

Required fields (not an implemented serialization contract):

| Field | Required meaning |
| --- | --- |
| `format_version`, `migration_id` | Version `1`, unique immutable operation ID |
| `source` | Cluster identity, exact binary/schema versions, all replica identities and accepted committed/applied boundary |
| `destination` | New cluster/group/incarnation, node identities, bootstrap voters, transport protocol and storage format versions |
| `fence` | External fence authority/reference, monotonic epoch and inventory of every blocked writer/listener |
| `metadata` | Exact revision, key count, ordered key/value/version/mod-revision image, length and SHA-256 |
| `allocators` | Every namespace/channel/counter and high-water mark, including retained/unpublished allocations |
| `fragments` | Pool/VDisk identities and generations; exact fragment inventory, lengths, hashes, tombstones and barrier positions |
| `validation` | Invariant-check version and reports, missing/unavailable fragments and explicit rejection result |
| `backup` | Complete immutable backup ID, locations and verified restore-test report |
| `authorization` | Operator approval of this exact manifest digest, fence epoch and destination identity |

Choose maximum manifest size, inventory sharding and canonical encoding before
implementation. Hash raw persisted bytes or a specified canonical encoding;
ordinary JSON object ordering is not a signing contract. Inventory paths must
be relative, normalized and confined to a locked backup root; reject symlinks
and path traversal rather than trust file names in a signed manifest.

## State transitions and durable authority

The future migration journal must use bounded, checksummed versioned records
with operation ID, previous-record digest and fence epoch. Persist file contents
and the containing directory before acknowledging a transition. Hold exclusive
ownership throughout. The journal records evidence, not an independent right
to open a listener: the external fence must remain in force.

1. **PREPARED** — inventory source and destination; reject unsupported versions,
   degraded/missing data or failed prerequisites. Test the backup restore before
   authorizing cutover. No source mutation and no destination client traffic.
2. **SOURCE_FENCED** — block and stop all writers/maintenance, including stale
   peers; verify all reachable source replicas agree on the committed/applied
   state used for export. An uncertain source commit requires investigation,
   not selection of the largest local log. Keep uncommitted tails out of export.
3. **BACKUP_VERIFIED** — capture consistent metadata and immutable fragment
   inventory while fenced, validate checksums and allocator bounds, persist the
   signed manifest and independent backup. Missing evidence blocks progress.
4. **IMPORTED** — initialize only explicitly reserved empty destination paths,
   import the exact application state through the future offline import API,
   and retain the source fence. Any partial destination is quarantined; resume
   only with operation-ID/checksum verification, never a fresh bootstrap guess.
5. **VALIDATED** — all destination voters recover and validate the imported
   state, exact revisions/counters, referenced fragments, tombstones/barriers
   and expected membership. Perform read-only protocol comparisons. Validation
   reports bind to the manifest digest and unchanged fence epoch.
6. **CUTOVER_COMMITTED** — persist the sole authoritative runtime selection,
   route epoch, manifest digest and irreversible-cutover approval in durable
   deployment authority; ensure stale source writers cannot return. This is
   the cutover commit point, not an HTTP route update or a leader election.
7. **DESTINATION_WRITABLE** — only after re-reading committed authority, start
   destination client listeners. Initially preserve one transaction domain.
   Record the first potentially accepted destination write; unknown outcomes
   count as writes for rollback purposes. Keep source data immutable.

Every transition needs before/after crash tests, idempotent restart validation
and explicit handling for file sync, directory sync and rename errors. A local
journal cannot atomically update external routing and fencing; the deployment
authority protocol and its failure recovery remain design blockers.

## Recovery and rollback

| Observed durable state | Allowed recovery |
| --- | --- |
| Before `SOURCE_FENCED` | Cancel preparation; source remains authoritative |
| Fenced, no committed cutover | Keep destination closed; verify journal, manifest and external authority before explicitly reopening the unchanged source |
| Unknown or disagreeing cutover authority | Keep both runtimes write-disabled; recover the authority, do not choose by wall clock or highest local state |
| Committed cutover, no proven destination write | Do not automatically reopen source; an explicit, separately fenced reversal protocol is still required |
| Any possible destination write | No binary downgrade or old-directory restart; stop/fence destination and restore a new consistent metadata+fragment backup using a separately validated recovery procedure |

Restoring a backup to a cloned cluster must allocate a new cluster/incarnation
and enforce a fragment allocation namespace that cannot collide with surviving
writers. Resetting counters or changing only a hostname is unsafe. Never restore
metadata alone over live fragment pools, delete retained data, or allow a
divergent surviving cluster to share allocation authority.

## Required realistic-data acceptance evidence

* SQL schemas, indexes, multi-key DML, transactions and unpublished WAL/SSTs;
  compare supported query results, errors and conflict/unknown-outcome behavior.
* S3 buckets, versions, multipart intents and aborted/partially published uploads;
  compare list/read results and preserve cleanup fencing and retained fragments.
* Block/iSCSI volumes with sparse extents, overwrite/UNMAP, tombstones/barriers,
  old and new fragment-ID schemes; verify full readback and allocator exhaustion.
* Key delete/recreate, equal-value ABA, exact per-key versions/mod-revisions,
  global revision bounds and schema/control-plane generations.
* Multi-key atomic publication and same-request retry semantics. Legacy watch
  subscriptions must disconnect/resync explicitly; no fabricated continuous
  global watch sequence across runtime or group changes.
* Kill/restart at each transition; interrupted import, stale source process,
  network partition, quorum loss, replayed approval, changed manifest, lost sync
  acknowledgment and failed rollback. Distinguish process-kill from actual
  power-loss evidence on each supported filesystem/hardware configuration.

The release gate remains open until a tool implements this protocol, independent
review approves its authority/fencing model, and the evidence above is recorded.
