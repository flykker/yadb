# Record: closing the two remaining `tests/protocols.py` SKIPs

**Status: a dated record, 2026-11-19.** This document began as a plan written
against `main` at 23 PASS / 2 SKIP. It is kept, and its reasoning is not
deleted, because it is the argument for why *per group* is the correct boundary
for repair and why the metadata read lease is the wrong mechanism to add. The
sections below are the argument as it was made, annotated with what was
implemented and what remains unproven.

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
3. **Live repair drill and its inversion — exists; conditional on the harness
   run.** The drill and the node-still-down inversion described below are in the
   harness change. No run was observed from the worktree that wrote this record,
   so no PASS is claimed here.
4. **Intent grace knob and orphan drill — knob done; drill exists, conditional
   on the harness run.** The grace is now the per-engine knob
   `ObjectStoreEngine::set_intent_grace_secs`, default 300 s unchanged,
   provisioned only by the harness (1 s), so the orphan-collection drill and its
   inversion (same intent preserved at the default) are possible without
   changing a shipped default.
5. **Reader lease — not implemented; narrowed instead.** Multi-Raft has no
   *metadata* read lease by design: `read_part` pins the slot epoch at the point
   of use and `delete_collectable` refuses while any pin is live, so the fence
   is per fragment on the target rather than a record the target never sees. A
   positive drill observes a live pin during an S3 `GET` and its drain;
   conditional on the harness run. `feat/reader-lease` stays unmerged and option
   B stays rejected.
6. **Guards and docs.** The `tests/protocols.py` and
   `scripts/test_protocols_review.py` edits belong to the harness change; this
   record does not restate their result.

The gate counts are deliberately not restated as achieved. The projection in
"Order taken, and the projected effect on the gate" is the argument for what the
change should add, and the harness run is the record of what it did add.

## What this record still does not claim

These stay unproven, and the text below must not be read as proving them:

* a collector lease expiring in the middle of a pass — the daemon has no
  injection point, so only the storage-level drill covers the shape;
* two concurrent collectors on one group;
* a group leader dying between a fragment write and its manifest commit;
* `block_pool_lookup` resolving through the local control-plane cache rather
  than the group snapshot: a stale cache can make the pass skip a blob, and can
  never make it repair or delete the wrong one.

The two SKIPs were the only remaining record of a capability the runtime does
not have. They are not removed by deletion: either the capability is
implemented, or the SKIP is narrowed to the capability that is genuinely absent.
That rule was followed — SKIP 1 is closed by implementation, SKIP 2 is narrowed.

## Where the two SKIPs stood

| Scenario | What it refused | Closable? |
|---|---|---|
| `Multi-Raft repair scan` | The repair half of `POST /v1/pools/scrub` | Yes, per group |
| `reader lease` | A metadata read lease on Multi-Raft | No; reads are fenced by a target-side pin instead |

## SKIP 1: `Multi-Raft repair scan`

### What the refusal said

The route answered `409` for every Multi-Raft deployment, and the summary
carried `repair_error` from `SCRUB_MULTIRAFT_REFUSAL`
(`crates/yadbd/src/main.rs:348`, set at `crates/yadbd/src/main.rs:923`, status
chosen at `crates/yadbd/src/main.rs:7661`). The reason given was that repair
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
makes that decision per group (`sweep_groups` at `crates/yadbd/src/gc.rs:431`).
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
     `collect_orphans_in_scopes_with` (`crates/yadb-s3/src/lib.rs:1563`). That
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
   * inversion: run the same scrub while the node is still down. The nominal
     slot is unreachable, so the blob is counted `blobs_unrecoverable`, nothing
     is moved, the object still reads back from the handoff copies, and the
     route does not report a repair it did not make.
5. **Guards and docs** — the scenario is dropped from `EXPECTED_SKIPS` and
   `REQUIRED_SUBSTRINGS` in `scripts/test_protocols_review.py`,
   `assert_multiraft_scrub_repair_refusal` is replaced with a positive
   assertion, and `multiraft-verification.md`, `multiraft-runtime-operations.md`,
   `multiraft-production-todo.md` and `multiraft-gc-fencing-plan.md` are updated
   to match, all in the same change set. Steps 1–3 are implemented. The unit
   tests and the live drills in step 4 belong to the implementation and harness
   changes; none of them was observed running from the worktree that wrote this
   record, so step 4 is conditional on that run. Step 5 is split across the
   harness change and this documentation change.

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

`begin_read_lease` (`crates/yadbd/src/main.rs:539`) is gated by
`!st.app.meta.is_openraft()` (`crates/yadbd/src/protocols/s3.rs:683`), so on
Multi-Raft no metadata read lease is acquired, renewed or expired.

### Why the mechanism is absent

The lease exists to stop a collector deleting fragments under a reader. On
Multi-Raft that job is done on the storage target instead: `read_part`
(`crates/yadb-blob/src/pool.rs:1136`) reads the slot's epoch and then pins it,
and a collectable delete is refused while any pin is live. The fence is
therefore per fragment, at the point of use, and it is strictly stronger than a
metadata record the target never sees: it cannot expire mid-read without the
read failing closed, and it does not depend on the reader re-checking a record
around each fetch.

The branch `feat/reader-lease` (`a6828ea`) is a WIP implementation of the
metadata lease. It is not merged.

### Two options, and which one was taken

**A. Keep the SKIP, narrow it, and prove the pin instead — taken.**

The old wording read as "Multi-Raft does not fence reads". That is false: the
read path fences itself. The narrowed wording says that no *metadata* lease
exists by design, because the fragment pin already fences the read, and cites
the pin as the mechanism. The SKIP stays as the record that the metadata
mechanism is absent.

The positive drill was added rather than only a note: stall one slot's response
through the harness transport proxy while an S3 `GET` is in flight, assert that
the slot's `/pins` surface shows a live pin, then release the response and
assert the pin drains and the object is intact. The proxy machinery and the
`/pins` route both exist; `assert_vdisk_fence_rpc` already drives that route
directly. The drill exists in the harness change; it has not been observed
green from the worktree that wrote this record, so its PASS is conditional on
that run.

**B. Merge `feat/reader-lease` — rejected.**

This removes the SKIP by implementing the mechanism, at the cost of roughly 567
changed lines in `main.rs`, a lease-provenance check on every collection pass,
and a second fence that duplicates the pin. It still would not produce a
cluster-level PASS for the interesting case: a collector losing its lease
mid-pass is not injectable in the daemon, which `multiraft-gc-fencing-plan.md`
already records as an open gap. Option B buys a removed SKIP, not a stronger
proof.

## The two gaps the SKIP 1 reason used to record

The `Multi-Raft repair scan` reason was the only place that recorded two facts.
Removing it would silently delete them, so both were handled:

1. **Orphan collection runs per group but had no live drill**, because an upload
   intent only becomes collectable after `INTENT_GRACE_SECS = 300`
   (`crates/yadb-s3/src/lib.rs:17`) and no run waits that long. Closed by making
   the grace a per-engine configuration value,
   `ObjectStoreEngine::set_intent_grace_secs`, whose default stays 300 s and
   which is provisioned only from the test harness (1 s), plus a drill: with the
   grace at one second, an abandoned intent is collected; with the default, the
   same intent is preserved. The second half is the inversion that shows the
   grace is what protects it. The drill is conditional on the harness run.
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
6. Guards, docs, and a full gate run under the shared `cargo` lock.

The projection below was made from the 23 PASS / 2 SKIP the plan was written
against. Closing a SKIP does not by itself add a PASS line: the scrub scenario
already printed one, and its text changes from "refuses its repair half" to
"repairs per group". Each new drill that reports separately adds one:

| Step | PASS | SKIP |
|---|---|---|
| Repair scan closes (rewritten scrub scenario) | 23 | 1 |
| Live repair drill reported separately | 24 | 1 |
| Orphan drill (step 4) | 25 | 1 |
| Reader-pin drill (step 5) | 26 | 1 |

The floor is 23 PASS / 1 SKIP: the repair scan implemented, the `reader lease`
SKIP narrowed, and no new drill. A drill that turns out not to be injectable is
recorded as such here and in the SKIP reason; it is never counted as a PASS.
Which rows the harness run actually reaches is not asserted by this record.

## What must not change

* No default is altered to make a drill pass: the snapshot knobs, the intent
  grace and the collection interval keep their shipped values, and the harness
  overrides them explicitly.
* No fallback to locally applied state, no automatic re-initialisation, no retry
  of an ambiguous mutation.
* No SKIP is deleted to make the inventory shorter; it is either implemented or
  narrowed to what is genuinely absent.
* TLS/mTLS, routing identities and the internal token surface are untouched.
