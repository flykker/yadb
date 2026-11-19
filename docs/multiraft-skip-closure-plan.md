# Record: closing the two remaining `tests/protocols.py` SKIPs

**Status: a dated record, 2026-11-19, revised the same day.** This document
began as a plan written against `main` at 23 PASS / 2 SKIP. It is kept, and its
reasoning is not deleted, because it is the argument for why *per group* is the
correct boundary for repair. Its other conclusion — that the metadata read lease
is the wrong mechanism to add — was reversed after the plan was written:
`feat/reader-lease` (`e4555c5`) implements it, and Multi-Raft readers publish a
per-group lease as well as pinning the fragment at the target. The sections
below are the argument as it was made, annotated with what was implemented,
what was reversed, and what remains unproven. The implementation is on that
branch; it is not merged into the tree this record was written on.

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
   refuses only groups this node does not lead`. The gate reported 26 PASS / 1
   SKIP.
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
   three-node drill observed the refusal and its expiry (see "SKIP 2" below).
   `feat/reader-lease` (`e4555c5`) implements it; the option the record
   rejected is now the implemented one, on that branch.
6. **Guards and docs — done.** `scripts/test_protocols_review.py` passes 21
   tests, `scripts/test_doc_anchors.py` passes 9, and the offline refusal
   contract in `tests/test_gc_refusal_protocol.py` passes 15, after the harness
   change and the re-resolved citations.

### The gate run (2026-11-19)

`python3 tests/protocols.py` ran against the built daemon on the documented
fixed bases and reported **26 PASS / 1 SKIP**. That is the run *before* the
reader-lease reversal: the single remaining `SKIP` was the narrowed `reader
lease` one, which named the target-side pin and the drill that proves it
instead of claiming the runtime has no fence at all. The count is kept as the
last observation of the narrowed record, not as the current inventory. The
three drills that report separately in that run are the three new `PASS` lines.

The count after the implementation lands is not projected here. Closing the
`reader lease` SKIP by implementation should remove the last SKIP, but the
number is established only by the run recorded in
`multiraft-verification.md`; no count is asserted before that run exists.

The run also found a defect the offline guards could not: the orphan drill
passed the `/blobs` route's hex strings to a helper that formats an integer
(`{blob:032x}`), so it died with `Unknown format code 'x'` before asserting
anything. Fixed in the harness and re-run; the 26/1 result above is from the
run after that fix.

## What this record still does not claim

These stay unproven, and the text below must not be read as proving them:

* a collector lease expiring in the middle of a pass — the daemon has no
  injection point, so only the storage-level drill covers the shape;
* a reader lease expiring in the middle of a read — the live drill observed the
  record block a pass and then expire with no read in flight, not a read that
  outlived its lease;
* two concurrent collectors on one group;
* a group leader dying between a fragment write and its manifest commit;
* `block_pool_lookup` resolving through the local control-plane cache rather
  than the group snapshot: a stale cache can make the pass skip a blob, and can
  never make it repair or delete the wrong one.

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
  exactly one data group (`crates/yadb-metakv/src/openraft_client.rs:129`), so
  a project's manifests are never split across groups;
* a blob id embeds the project id, so the blobs of one group's projects are
  exactly the blobs whose manifests that group holds;
* therefore a quorum-confirmed snapshot of one group over those three prefixes
  yields the *complete* live set for that group's blobs, not a partial one.

Deletion is the operation that needs a cross-group decision, and it already
makes that decision per group (`sweep_groups` at `crates/yadbd/src/gc.rs:614`).
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
     "/block/"])` (`crates/yadb-metakv/src/raft.rs:361`). A follower or a group
     without a quorum is a refusal for that group; there is no fallback to local
     applied state, and one group's refusal does not stop another group's pass,
     exactly as `sweep_groups` behaves;
   * builds the live set from the returned rows only, never from
     `try_scan_prefix`, so a lagging applied state cannot be mistaken for a
     complete view;
   * repairs each live blob through `repair_blob`.
   * **Does not copy the `project_ids.is_some()` guard** from
     `collect_orphans_in_scopes_with` (`crates/yadb-s3/src/lib.rs:1618`). That
     guard exists because orphan collection enumerates projects; this pass is
     prefix-scoped, so a `project_ids == None` scope is not a blocker. The
     doc comment says this, so the next reader does not copy the refusal. The
     guard itself is still real for orphan collection; see "The two gaps the
     SKIP 1 reason used to record".
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

* `block_pool_lookup` comes from the local control-plane cache, not from the
  group snapshot. A stale cache makes the pass skip a blob; it can never make
  it repair or delete the wrong one. The code says this instead of implying
  the whole pass is quorum-derived.
* A blob published after the snapshot is not repaired by that pass; the next
  pass sees it. Repair is idempotent, not atomic with publication.
* There is still no drill for a group leader dying between a fragment write and
  its manifest commit. That hole exists independently of this SKIP.

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

### The drill that was observed

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
   (`crates/yadb-s3/src/lib.rs:17`) and no run waits that long. Closed by making
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
2. **A scope with `project_ids == None` cannot be collected** — that is a
   single-group Multi-Raft deployment, where `collect_orphans_in_scopes_with`
   refuses with `NamespaceScanUnavailable` because the group covers every
   project and its project set cannot be enumerated. This is a real, separate
   gap, unrelated to the repair scan, and it is now its own open item in
   `multiraft-production-todo.md`. The repair pass does **not** inherit the
   guard: it is prefix-scoped and never enumerates projects, so a
   `project_ids == None` scope repairs normally while orphan collection over the
   same scope still refuses. That asymmetry is deliberate and is why the gap
   needed its own record rather than dying with the SKIP reason.

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

The floor of that sequence was 23 PASS / 1 SKIP: the repair scan implemented,
the `reader lease` SKIP narrowed, and no new drill. A drill that turns out not
to be injectable is recorded as such here and in the SKIP reason; it is never
counted as a PASS. Every row above was reached in the gate run recorded under
"The gate run (2026-11-19)".

The count after the implementation lands is left open. Removing the last SKIP
should add a PASS line for the lease drill, but the number is established only
by the run recorded in `multiraft-verification.md`; no count is asserted here
before that run exists.

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
