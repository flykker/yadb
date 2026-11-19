# Record: closing the two remaining `tests/protocols.py` SKIPs

**Status: a dated record, 2026-11-19, revised the same day.** This document
began as a plan written against `main` at 23 PASS / 2 SKIP. It is kept, and its
reasoning is not deleted, because it is the argument for why *per group* is the
correct boundary for repair. Its other conclusion — that the metadata read lease
is the wrong mechanism to add — was reversed after the plan was written:
`feat/reader-lease` (`e4555c5`) implements it, and Multi-Raft readers publish a
per-group lease as well as pinning the fragment at the target. The sections
below are the argument as it was made, annotated with what was implemented,
what was reversed, and what remains unproven. The implementation is merged into
`main` (`7dd822c`, with the trial branch's test and a fix as `1254c84`).

## Status at the close of this record

1. **Per-group repair — done.** `gc::repair_scan_groups` sits next to
   `sweep_groups` in `crates/yadbd/src/gc.rs`; it takes a quorum-confirmed
   snapshot per group over `/db/`, `/s3/` and `/block/`, builds the live set
   from that snapshot only, and calls `repair_blob` for each live blob.
   `ScrubSummary.repair_error` is `None` when every group ran, and otherwise
   names the refused groups. `SCRUB_MULTIRAFT_REFUSAL` is deleted.
2. **Routing rule — done.** `POST /v1/pools/scrub` answers `200` only when
   `repair_error` is `None` **and** `gc_refusal` is `None`, and `409`
   otherwise — the rule `/v1/pools/gc` already used. On a multi-node cluster a
   node leads only some groups, so its scrub is `409` while the groups it does
   lead are repaired; the refusal names the groups, not a missing capability.
3. **Live repair drill and its inversion — done, observed.** The drill and the
   node-still-down inversion described below ran in the full gate on
   2026-11-19 and printed `PASS Multi-Raft repair scan repairs per group,
   refuses only groups this node does not lead`. That run reported 26 PASS / 1
   SKIP; the run with the lease implemented reports 27 PASS / 0 SKIP.
4. **Intent grace knob and orphan drill — done, observed.** The grace is now
   the per-engine knob `ObjectStoreEngine::set_intent_grace_secs`, default 300 s
   unchanged, provisioned only by the harness (1 s), so the orphan-collection
   drill and its inversion (the same intent preserved inside its grace) are
   possible without changing a shipped default. The drill printed
   `PASS Multi-Raft orphan-blob collection keeps a live upload intent, then
   collects its chunks once the grace lapses` in the same run.
5. **Reader lease — closed by implementation, after the earlier decision was
   reversed.** Multi-Raft readers now publish (and renew) a reader lease in the
   metadata group that owns the project, in addition to the target-side pin.
   `AppState::begin_read_lease` resolves the owning group with
   `gc_group_for_project` and caches one record per group in a map keyed by
   `Option<u64>`, `None` being the legacy namespace; a record in another group
   is not mistaken for a fence. The group-scoped record carries that group's
   applied log index as its revision, because Multi-Raft has no
   deployment-wide revision. `yadb_metakv::gc::lease_belongs_to_group` checks
   the record's group and that claimed index after admission closes, and
   `sweep_group` refuses a group whose record names another group or claims an
   index the group has not applied. On legacy metadata `reader_is_safe` still
   runs; on Multi-Raft the lease publication is itself a committed group write,
   so that check is skipped. The TTL is 30 s (`DEFAULT_TTL_MS`). A live
   three-node run observed the refusal and its expiry (see "SKIP 2" below).
   `feat/reader-lease` (`e4555c5`) implements it; the option the record
   rejected is now the implemented one, merged into `main`. Two defects were
   found while validating it against `main` and are fixed in the merge: the WIP
   had collapsed the legacy and Multi-Raft consistent block read into one call
   to `read_at_consistent`, which asks for a quorum-confirmed snapshot legacy
   metadata cannot produce, so every legacy `consistent=true` block read (what
   the iSCSI READ path always sends) failed; and four test fixtures still built
   the cache as `Mutex::new(None)` after it became a map. The per-group cache
   had no test at all — the only lease test removed the `None` key and never
   resolved a group — so one was added that runs two data groups and asserts
   the record names its group, carries that group's applied index, is renewed
   rather than republished, and frees only its own group when released.
6. **Guards and docs — done.** `scripts/test_protocols_review.py` passes 21
   tests, `scripts/test_doc_anchors.py` passes 9, and the offline refusal
   contract in `tests/test_gc_refusal_protocol.py` passes 15, after the harness
   change and the re-resolved citations.

### The gate run (2026-11-19)

`python3 tests/protocols.py` ran against the built daemon on the documented
fixed bases and reported **27 PASS / 0 SKIP**, exit 0. The single remaining
`SKIP` — the narrowed `reader lease` one, which named the target-side pin and
the drill that proves it instead of claiming the runtime has no fence at all —
is replaced by a live drill that prints `PASS Multi-Raft S3 GET publishes a
per-group reader lease and the collector refuses until it expires`. The 27 is
the previous 26 PASS / 1 SKIP with that one scenario converted: no scenario was
removed and no assertion was relaxed to reach it. The three drills that report
separately in that run are the three new `PASS` lines.

The run also found a defect the offline guards could not: the orphan drill
passed the `/blobs` route's hex strings to a helper that formats an integer
(`{blob:032x}`), so it died with `Unknown format code 'x'` before asserting
anything. Fixed in the harness and re-run.

The first run *after* the lease was implemented failed in the orphan drill
rather than in the new one, and that failure is the fence working: the repair
drill ends with `s3_visible` GETs, those leave a cached reader lease in the
billing group, and the next drill's first scan compares the sweep's refused
*set* against the groups the node does not lead — so a live lease named a group
the node leads. The harness now records the last S3 `GET` in `signed()` and
drains from that timestamp, instead of each drill threading its own, and the
orphan drill drains *before* it uploads its part, because its first pass has to
run inside the one-second intent grace. The set comparison was not weakened.

### Second block: the shapes this record still did not claim (2026-11-19)

The list below this section was the next target, and four of its items were
closed in a second block. Three are proved at unit level rather than live,
because the live cluster cannot produce them deterministically; the fourth was
a real defect and is fixed in the daemon.

* **A reader lease expiring mid-read** — the guard existed and nothing observed
  it. `s3_get_refuses_bytes_when_the_read_outlives_its_reader_lease` runs the
  real S3 `GET` over the real multi-group metadata store, writes a 4 MiB object
  (1024 fragments at the engine's 4096-byte chunk size) into the project's
  group, and shrinks the test-only `AppState::reader_lease_ttl_ms` to 1 ms. The
  handler answers `503` with the `ensure_live` text and not the bytes, and the
  same `GET` at the shipped 30 s default answers `200` with the exact bytes.
  `reader_lease_ttl_ms` defaults to `DEFAULT_TTL_MS` at every construction site
  and has no CLI flag and no route: it exists only so a test can shrink it.
* **Two concurrent collectors on one group** — `a_held_group_collector_lease_refuses_the_second_pass_and_deletes_nothing`
  holds the group's collector record and observes the second pass refuse by
  name, delete and barrier nothing, and leave the same fragments collectable
  once the record is released. `two_concurrent_passes_on_one_group_delete_each_fragment_at_most_once`
  runs two real passes on separate threads and asserts the invariant: at most
  one reports a deletion, every dead fragment is deleted exactly once in total,
  a refusal names the collector lease, no live fragment disappears. The race
  alone can be satisfied by running the passes back to back, so the held-record
  test is the proof and the race test is a guard.
* **A collector lease expiring mid-pass** — `a_collector_fence_lost_mid_pool_stops_deleting_and_is_never_counted`
  fails the fence closure `sweep_pool` receives between two barrier ranges: the
  range applied before the loss is evicted and counted, the range after it is
  present and uncounted, the outcome names the stop, and a live fence
  afterwards reclaims exactly the refused range.
* **`block_pool_lookup` from the local cache** — this one was not a missing
  test but a real hole, and the earlier wording understated it. The pass read
  the `project/volume` to pool map from this node's local cache, and a stale
  map is not merely conservative: after a volume is remapped it marks the old
  pool live and the new pool's fragments garbage, so a pass on the cache can
  delete data that is still referenced. `AppState::control_for_collection` now
  hands a pass only a control snapshot proved against the control group's
  quorum, and a pass refuses before touching any group when that proof is
  unavailable — `409` from `POST /v1/pools/gc`, a `repair_error` from scrub.
  `a_collection_pass_reads_the_control_map_from_a_confirmed_snapshot` proves
  both halves.

## What this record still does not claim

The following are not claimed at live-cluster level, and the text below must not
be read as proving them there. Four were subsequently proved elsewhere: three at
sweep level against real single-node OpenRaft groups and real volume directories,
and one in-process at the S3 handler over the real multi-group metadata store.
One more (the volume-to-pool map) was closed in the daemon by taking its map from
a confirmed control snapshot. Each entry says what the proof is and what the live
cluster still lacks.

* a collector lease expiring in the middle of a live cluster pass — the daemon
  still has no injection point, so the live cluster cannot produce it. The
  shape is proved at sweep level with a real single-node OpenRaft group and
  real volume directories: `a_collector_fence_lost_mid_pool_stops_deleting_and_is_never_counted`
  fails the fence closure `sweep_pool` receives between two barrier ranges and
  observes that the earlier range is evicted and counted, the later range is
  untouched and uncounted, and the outcome names the stop. What stays unproven
  is the same failure through `POST /v1/pools/gc` on a cluster;
* a reader lease expiring in the middle of a *live cluster* read — proved
  in-process instead, at the handler: `s3_get_refuses_bytes_when_the_read_outlives_its_reader_lease`
  in `crates/yadbd/src/main.rs` runs the real S3 `GET` over the real multi-group
  metadata store, writes an object through the engine into the group that owns
  the project, and shrinks the test-only `AppState::reader_lease_ttl_ms` to
  1 ms. The fenced fetch outlives the lease, `end_read_lease` refuses, and the
  handler answers `503` carrying the `ensure_live` text (`expired at …
  unreaped`) instead of the bytes — not the acquire path's `reader lease
  unavailable`. The same `GET` at the shipped 30 s default answers `200` with
  the exact bytes. The field exists only so a test can shrink the TTL: no CLI
  flag, no internal route, default unchanged. A live proof would need a read
  longer than 30 s or a runtime TTL knob, and neither was added because that
  would put a new operator surface in the hot read path; the live run only
  observed the record block a pass and then expire with no read in flight.
  `end_read_lease_refuses_an_expired_lease_and_does_not_cache_it` covers the
  same fence at the method level and that the dead record is not cached;
* two concurrent collectors on one group on a live cluster — a concurrent
  `POST /v1/pools/gc` pair is inherently racy, so there is no deterministic
  drill. The shape is proved at sweep level against a real single-node
  OpenRaft group: `a_held_group_collector_lease_refuses_the_second_pass_and_deletes_nothing`
  holds the group's collector record and observes the second pass refuse by
  name, delete nothing and leave the same fragments collectable once the record
  is released; `two_concurrent_passes_on_one_group_delete_each_fragment_at_most_once`
  runs two real passes on separate threads and asserts that at most one
  reports a deletion, every dead fragment is deleted exactly once in total, a
  refusal names the collector lease, and no live fragment disappears. Both
  passes succeeding back to back satisfies the same invariant, so the race
  alone is not the proof — the held-record test is the deterministic half;
* a group leader dying between a fragment write and its manifest commit — proved
  in-process at sweep level, not on the live cluster. Two tests in
  `crates/yadbd/src/gc.rs` (`gc::tests::openraft_tests`) use three real
  single-node OpenRaft groups over real volume directories, write through the
  real engines, stop every group, and reopen the same replica identities from
  their durable log and state-machine directories.
  `a_group_leader_death_before_publication_keeps_the_fragment_leased_and_unreadable`
  appends a fragment for project 1 (group 2), publishes the writer lease with
  `yadb_metakv::gc::acquire(..., LeaseKind::Writer, ...)` exactly as
  `acquire_writer_if_legacy` does, and dies before any manifest names it. After
  the restart a quorum-confirmed read is `ObjectNotFound` (not a partial
  object), the live lease still refuses group 2 and deletes nothing, and once
  the record is expired the pass reaps it, collects the fragment and reports
  the deletion. This is the block/SQL write shape, where the writer lease is
  the only fence. An S3 upload in flight has its prepared intent instead;
  `a_prepared_s3_intent_protects_its_chunks` proves that fence at the same
  sweep level, but does not restart the group.
  `a_group_leader_death_after_publication_keeps_the_committed_blob_readable`
  publishes an S3 object through `put_object`, stops the groups, reopens them,
  and reads the exact bytes with `get_object_consistent`; a pass after recovery
  keeps every chunk the committed manifest names, and a plain read still
  returns the same bytes. What stays unproven is the same death on a multi-node
  cluster: the test restarts a one-node deployment (all three single-node
  groups) with a clean shutdown, not one group leader failing over to a peer,
  and it does not cut a log entry mid-fsync.

`block_pool_lookup` used to be on this list: the pass read the
`project/volume` to pool map from this node's local control-plane cache rather
than from a snapshot of the control group, so a stale map could name the wrong
pool for a volume. A pass now takes that map only from a control snapshot this
node has proved against the control group's quorum
(`AppState::control_for_collection`), and refuses before touching any group when
that proof is unavailable — `409` from `POST /v1/pools/gc`, a `repair_error`
from scrub — instead of sweeping on an unproven map.
`a_collection_pass_reads_the_control_map_from_a_confirmed_snapshot` proves both
halves: a pass is handed the control group's newer map while the cache still
holds the old one, and both routes refuse and delete nothing when no snapshot can
be confirmed. The remaining limit is narrower: the map is the control group's
confirmed applied state, so a volume created after that snapshot is not seen by
that pass, and a node that cannot reach the control group's quorum refuses to
collect even the groups it leads.

The two SKIPs were the only remaining record of a capability the runtime does
not have. They are not removed by deletion: either the capability is
implemented, or the SKIP is narrowed to the capability that is genuinely absent.
That rule was followed — SKIP 1 was closed by implementation, and SKIP 2 was
first narrowed, then closed by implementing the lease after the decision was
reversed.

## Where the two SKIPs stood

| Scenario | What it refused | Closable? |
|---|---|---|
| `Multi-Raft repair scan` | The repair half of `POST /v1/pools/scrub` | Yes, per group |
| `reader lease` | A metadata read lease on Multi-Raft | Closed by implementing the lease; the earlier "the pin suffices" answer was reversed |

## SKIP 1: `Multi-Raft repair scan`

### What the refusal said

The route answered `409` for every Multi-Raft deployment, and the summary
carried `repair_error` from a `SCRUB_MULTIRAFT_REFUSAL` constant that is now
deleted. The reason given was that repair
"needs one whole-deployment view of live manifests".

### Why that reason was wider than repair needs

The refusal conflated two questions:

1. **Which blobs should exist?** This is answered from live manifests.
2. **What is wrong with one blob?** This is answered from the fragments alone.

`repair_blob` (`crates/yadb-blob/src/pool.rs:1590`) only does (2). It reads the
parts of one logical id, rebuilds the ones that are missing from the surviving
parts, and moves a part that lives on a handoff slot back to its nominal slot.
It never consults a manifest. The whole-deployment view is needed only for (1),
and (1) is not a cross-group question either:

* every data namespace (`/db/`, `/s3/`, `/block/`) routes by project id to
  exactly one data group (`crates/yadb-metakv/src/openraft_client.rs:157`), so
  a project's manifests are never split across groups;
* a blob id embeds the project id, so the blobs of one group's projects are
  exactly the blobs whose manifests that group holds;
* therefore a quorum-confirmed snapshot of one group over those three prefixes
  yields the *complete* live set for that group's blobs, not a partial one.

Deletion is the operation that needs a cross-group decision, and it already
makes that decision per group (`sweep_groups` at `crates/yadbd/src/gc.rs:634`).
Repair deletes nothing except the handoff copy of a part it has already written
to the nominal slot, and a tombstoned id refuses that write
(`crates/yadb-blob/src/volume.rs:634`), so a pass cannot resurrect a blob a
collector removed.

The conclusion is that per-group repair is the correct boundary, not a
compromise: a group's snapshot is complete for that group's blobs, and nothing
in the repair path crosses groups.

### The steps that were taken

1. **`crates/yadbd/src/gc.rs`** — `repair_scan_groups(meta, pools, control)`
   was added next to `sweep_groups`, returning a `RepairOutcome` with per-group
   refusals and the counters `ScrubSummary` already has. For each
   `GcGroupScope` it:
   * takes `try_linearizable_group_snapshot(group, &[], ["/db/", "/s3/",
     "/block/"])` (`crates/yadb-metakv/src/raft.rs:378`). A follower or a group
     without a quorum is a refusal for that group; there is no fallback to local
     applied state, and one group's refusal does not stop another group's pass,
     exactly as `sweep_groups` behaves;
   * builds the live set from the returned rows only, never from
     `try_scan_prefix`, so a lagging applied state cannot be mistaken for a
     complete view;
   * repairs each live blob through `repair_blob`.
   * **Does not copy the `project_ids.is_some()` guard** from
     `collect_orphans_in_scopes_with` (`crates/yadb-s3/src/lib.rs:1612`). That
     guard existed because orphan collection enumerated projects; this pass is
     prefix-scoped, so a `project_ids == None` scope is not a blocker. The
     doc comment says this, so the next reader does not copy the refusal. The
     guard itself was real for orphan collection at the time; it has since been
     replaced by snapshot-derived enumeration, so a `project_ids == None` scope
     collects from its own fenced view instead of refusing (see "The two gaps
     the SKIP 1 reason used to record").
2. **`crates/yadbd/src/main.rs`** — the OpenRaft branch of `scrub_all_pools`
   reports the real counters instead of `repair_error: Some(...)`.
   `repair_error` is `Some` only when a group refused, naming the group.
   `SCRUB_MULTIRAFT_REFUSAL` is deleted, together with its
   `#[allow(dead_code)]` twin in `gc.rs` if nothing else anchored on it.
3. **`handle_pools_scrub`** — the unconditional `is_openraft() → 409` was
   replaced with the rule `/v1/pools/gc` already uses: `200` only when every
   group ran, otherwise `409` with the refusal in the body. Otherwise the route
   would keep claiming a failure after the capability exists.
4. **Tests** (each checked by inversion, per the repository rule):
   * unit, `SimFs` fixture: a blob of group A is repaired while a blob of group
     B is untouched by A's pass; a follower group refuses and repairs nothing;
     a tombstoned blob is reported unrecoverable, never repaired.
   * live drill, no injection point needed: take one node down, write an S3
     object so its parts land on handoff slots, restart the node, run
     `POST /v1/pools/scrub`, and assert `fragments_repaired >= 1` — the parts
     have returned to their nominal slots. The harness already produces this
     placement in the quorum-loss drill.
   * inversion: run the same scrub while the node is still down. Nothing is
     moved (`fragments_repaired == 0`), and a blob whose nominal slots sit on
     the down node is counted `blobs_unrecoverable` because its parts cannot be
     pinned anywhere — its absence is not proved, so the pass does not claim it
     healed one. The object just written *does* read back from its handoff
     copies and is counted intact, which is the honest shape: the route reports
     no repair it did not make, and it does not call a readable blob lost.
     (Observed on a live three-node run: 4 blobs checked, 3 intact, 1
     unrecoverable, 0 fragments repaired.)
5. **Guards and docs** — the scenario is dropped from `EXPECTED_SKIPS` and
   `REQUIRED_SUBSTRINGS` in `scripts/test_protocols_review.py`,
   `assert_multiraft_scrub_repair_refusal` is replaced with a positive
   assertion, and `multiraft-verification.md`, `multiraft-runtime-operations.md`,
   `multiraft-production-todo.md` and `multiraft-gc-fencing-plan.md` are updated
   to match, all in the same change set. Steps 1–3 are implemented. The unit
   tests and the live drills in step 4 belong to the implementation and harness
   changes; all four passed in the gate run recorded above.

### What this still does not prove

* The volume-to-pool map is now taken from a control snapshot this node has
  proved against the control group's quorum, not from the local cache, so a
  stale cache can no longer name the wrong pool for a volume
  (`AppState::control_for_collection`, proven by
  `a_collection_pass_reads_the_control_map_from_a_confirmed_snapshot`). What
  remains is that the snapshot is the control group's confirmed applied state:
  a volume created after it is not seen by that pass, and a node that cannot
  reach the control group's quorum refuses to collect at all.
* A blob published after the snapshot is not repaired by that pass; the next
  pass sees it. Repair is idempotent, not atomic with publication.
* There is still no *live-cluster* drill for a group leader dying between a
  fragment write and its manifest commit. The shape is now proved at sweep
  level across a real restart by
  `a_group_leader_death_before_publication_keeps_the_fragment_leased_and_unreadable`
  and
  `a_group_leader_death_after_publication_keeps_the_committed_blob_readable`
  (both in `crates/yadbd/src/gc.rs`): the uncommitted half stays absent and
  lease-protected until the lease expires and is then collected, and the
  committed half reads back its exact bytes and keeps its fragments through a
  pass. The live cluster still lacks the same death with peers, where one
  group's leader fails over while the other groups keep serving. That hole
  exists independently of this SKIP.

## SKIP 2: `reader lease`

### What it said

The SKIP recorded that `begin_read_lease` was gated by
`!st.app.meta.is_openraft()` in the S3 read handler, so on Multi-Raft no
metadata read lease was acquired, renewed or expired. That gate is gone: the
handler now publishes a lease for every `GET` in both metadata modes. The
original wording and the reasons it gave are kept below because they were the
decision of record until the reversal.

### Why the mechanism was absent, and why that was the decision at the time

The lease exists to stop a collector deleting fragments under a reader. On
Multi-Raft that job is done on the storage target instead: `read_part` reads
the slot's epoch and then pins it, and a collectable delete is refused while
any pin is live. The fence is therefore per fragment, at the point of use, and
the target evaluates it when the delete arrives rather than trusting a record
it never sees. The plan called this strictly stronger than a metadata lease.
The defensible part of that claim is narrower: the pin covers the fetch in
progress and a failed pin is reported as a failure, not as an absent fragment.
The pin has its own TTL (`READ_PIN_TTL_MS`, the target's maximum), so a read
that outlives it is no longer protected by it either; the difference is that
the gap is per fetch rather than per group.

The branch `feat/reader-lease` (then at `a6828ea`) was a WIP implementation of
the metadata lease. It was later rebased onto `main` as `e4555c5`; the decision
below was reversed and that implementation is the one this record describes.

### Two options, and which one was taken then

**A. Keep the SKIP, narrow it, and prove the pin instead — taken at the time.**

The old wording read as "Multi-Raft does not fence reads". That is false: the
read path fences itself. The narrowed wording said that no *metadata* lease
existed by design, because the fragment pin already fenced the read, and cited
the pin as the mechanism. The SKIP stayed as the record that the metadata
mechanism was absent.

The positive drill was added rather than only a note, and it uses the shipped
surfaces only: write a 12 MiB object (large enough to stripe, under the 16 MiB
body cap), read it while one thread per slot polls
`/internal/vdisk/{pool}/{slot}/pins` with the Raft token, assert at least one
non-empty observation with integer `pin_id`/`epoch`/`expires_at_ms`, then assert
every slot's pin list drains after the read. It does not stall a response
through the transport proxy: the pin window is a few milliseconds per fragment,
and a 12 MiB read widens it enough to observe without touching the proxy. The
drill passed in the gate run recorded above.

**B. Merge `feat/reader-lease` — rejected at the time.**

This removes the SKIP by implementing the mechanism, at the cost of roughly 567
changed lines in `main.rs`, a lease-provenance check on every collection pass,
and a second fence that duplicates the pin. It still would not produce a
cluster-level PASS for the interesting case: a collector losing its lease
mid-pass is not injectable in the daemon, which `multiraft-gc-fencing-plan.md`
already records as an open gap. Option B buys a removed SKIP, not a stronger
proof.

### The reversal

The decision was reversed on 2026-11-19 and option B was implemented. The
reasons the old text gave for rejecting it are still true: the change adds a
second fence, the lease is coarser than the pin (it fences the whole group, not
the fragment being read), and a collector losing its lease mid-pass is still
not injectable. What changed is the judgement about that cost:

* the pin protocol now exists, so the lease is an additional coarse fence
  rather than the only one. The pin remains the finer, point-of-use fence and
  is still proved by its own live drill;
* the per-group collector now exists and consults the reader records before it
  takes its mark, so the lease is not published and ignored: a collector can be
  told to back off without inspecting every target;
* the branch had been rebased onto `main` and kept the two pieces the old text
  said the collector would need: per-group keying and the
  `lease_belongs_to_group` provenance check.

Mechanics as implemented. `AppState::begin_read_lease` resolves the owning
group with `gc_group_for_project` and caches one record per group in a map
keyed by `Option<u64>` (`None` is the legacy namespace), so a record in another
group is not mistaken for a fence. A group-scoped lease carries the owning
group's applied log index as its revision instead of a deployment-wide
revision, because Multi-Raft has no global revision.
`yadb_metakv::gc::lease_belongs_to_group` checks that provenance after
admission closes, and `sweep_group` refuses a group whose record names another
group or claims an index that group has not applied. On legacy metadata the
durable sweep-watermark check (`reader_is_safe`) still runs; on Multi-Raft the
lease publication is itself a committed group write, so that check is skipped.
The lease is cached and renewed across reads; the TTL is 30 s
(`DEFAULT_TTL_MS`).

The honest trade-off:

* a live reader lease blocks `POST /v1/pools/gc` and the scrub's collection
  half for up to the 30 s TTL after the last read in that group, so a collector
  against a recently read group refuses rather than collects;
* the metadata lease is coarser than the pin: it fences the whole group, not
  the fragment being read;
* the pin remains the finer, point-of-use fence and is still proved by its own
  live drill.

So the change buys a collector that can be told to back off without inspecting
every target, and a lease that the collection path consults before it takes its
mark; it costs collection availability after reads. It is not a pure win.

### The live run that was observed

On a live three-node cluster with the new daemon, `POST /v1/pools/gc` before
any read reported `leases_live: 0`. After a `PUT` and a `GET`, the same request
answered `409` with `leases_live: 1`, `fragments_deleted: 0`, and a
`gc_refusal` containing `group 2 refused: 1 live lease(s) (1 reader, 0 writer)
in metadata group 2; no fragment of that group was deleted`. The lease was
still live at +15 s and had expired by +35 s with no intervening reads, after
which the reader-lease clause disappeared from the refusal. This is one
observed run. It does not establish behaviour under concurrent collectors, and
it does not cover a pass that starts before the lease and continues after it
expires.

## The two gaps the SKIP 1 reason used to record

The `Multi-Raft repair scan` reason was the only place that recorded two facts.
Removing it would silently delete them, so both were handled:

1. **Orphan collection runs per group but had no live drill**, because an upload
   intent only becomes collectable after `INTENT_GRACE_SECS = 300`
   (`crates/yadb-s3/src/lib.rs:21`) and no run waits that long. Closed by making
   the grace a per-engine configuration value,
   `ObjectStoreEngine::set_intent_grace_secs`, whose default stays 300 s and
   which is provisioned only from the test harness (1 s), plus a drill. The
   drill's inversion is the same intent on the same deployment before and after
   its grace lapses: the first pass runs inside the grace, collects nothing and
   leaves every chunk of the abandoned part readable; the second runs after 40
   commits in the project group (well past the 32-revision lease) and the wall
   clock, and collects the intent and its chunks. It does **not** run a second
   deployment at the 300 s default — that would cost five minutes of wall time —
   so "the shipped default protects an intent for 300 s" rests on the knob's
   unit test (`intent_grace_is_configurable_and_protects_a_fresh_intent_until_it_expires`)
   and on the constant being unchanged, not on a live observation.
2. **A scope with `project_ids == None` cannot be collected** — that was a
   single-group Multi-Raft deployment, where `collect_orphans_in_scopes_with`
   refused with `NamespaceScanUnavailable` because the group covered every
   project and its project set could not be enumerated. This was a real,
   separate gap, unrelated to the repair scan, and it became its own item in
   `multiraft-production-todo.md`. The repair pass did **not** inherit the
   guard: it is prefix-scoped and never enumerates projects, so a
   `project_ids == None` scope repaired normally while orphan collection over
   the same scope refused. That asymmetry was deliberate and is why the gap
   needed its own record rather than dying with the SKIP reason. The gap is now
   closed: the project set is derived from the group's own quorum-confirmed
   snapshot under the three S3 manifest prefixes, which is complete for that
   group, and a scope with `project_ids == None` collects from it instead of
   refusing.

## Order taken, and the projected effect on the gate

1. Per-group repair (`gc.rs`), with unit tests and the follower refusal.
2. Routing and the `200`/`409` rule (`main.rs`).
3. Live repair drill plus its inversion.
4. `intent_grace_secs` knob and the orphan drill plus its inversion.
5. Reader-pin drill; narrowed `reader lease` wording.
6. Reversal: implement `feat/reader-lease`, re-point the `reader lease`
   scenario from a narrowed SKIP to a positive lease drill, and keep the pin
   drill.
7. Guards, docs, and a full gate run under the shared `cargo` lock.

The projection below was made from the 23 PASS / 2 SKIP the plan was written
against, and it describes the run *before* the reversal. It is kept as the
record of that run, not as the count after the implementation lands. Closing a
SKIP does not by itself add a PASS line: the scrub scenario already printed one,
and its text changes from "refuses its repair half" to "repairs per group". Each
new drill that reports separately adds one:

| Step | PASS | SKIP |
|---|---|---|
| Repair scan closes (rewritten scrub scenario) | 23 | 1 |
| Live repair drill reported separately | 24 | 1 |
| Orphan drill (step 4) | 25 | 1 |
| Reader-pin drill (step 5) | 26 | 1 |
| Reader-lease drill (step 6) | 27 | 0 |

The floor of that sequence was 23 PASS / 1 SKIP: the repair scan implemented,
the `reader lease` SKIP narrowed, and no new drill. A drill that turns out not
to be injectable is recorded as such here and in the SKIP reason; it is never
counted as a PASS. Every row above was reached in the gate runs recorded under
"The gate run (2026-11-19)"; the last row is the 27 PASS / 0 SKIP run.

The count after the implementation lands is no longer open: closing the last
SKIP added one PASS line and removed the only SKIP, so the inventory is 27 PASS
and no SKIP. The number comes from the run recorded in
`multiraft-verification.md`, not from the arithmetic.

## What must not change

* No default is altered to make a drill pass: the snapshot knobs, the intent
  grace and the collection interval keep their shipped values, and the harness
  overrides them explicitly.
* No fallback to locally applied state, no automatic re-initialisation, no retry
  of an ambiguous mutation.
* No SKIP is deleted to make the inventory shorter; it is either implemented or
  narrowed to what is genuinely absent. SKIP 2 was narrowed first and then
  implemented; both steps are recorded above.
* TLS/mTLS, routing identities and the internal token surface are untouched.
