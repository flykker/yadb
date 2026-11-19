# Plan: closing the two remaining `tests/protocols.py` SKIPs

**Status: a plan, not a claim about the tree.** This document records what the
two SKIPs refuse today, why one of them can be closed by implementing the
capability and the other cannot be closed without inventing a mechanism nothing
needs, and what each step must prove. It is written against `main` as of
2026-11-19, where the gate is 23 PASS / 2 SKIP.

The two SKIPs are the only remaining record of a capability the runtime does not
have. They are not removed by deletion: either the capability is implemented, or
the SKIP is narrowed to the capability that is genuinely absent.

## Where the two SKIPs stand

| Scenario | What it refuses | Closable? |
|---|---|---|
| `Multi-Raft repair scan` | The repair half of `POST /v1/pools/scrub` | Yes, per group |
| `reader lease` | A metadata read lease on Multi-Raft | No; reads are fenced by a target-side pin instead |

## SKIP 1: `Multi-Raft repair scan`

### What the refusal says today

The route answers `409` for every Multi-Raft deployment, and the summary carries
`repair_error` from `SCRUB_MULTIRAFT_REFUSAL`
(`crates/yadbd/src/main.rs:348`, set at `crates/yadbd/src/main.rs:923`, status
chosen at `crates/yadbd/src/main.rs:7661`). The reason given is that repair
"needs one whole-deployment view of live manifests".

### Why that reason is wider than repair needs

The refusal conflates two questions:

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

### Steps

1. **`crates/yadbd/src/gc.rs`** — add `repair_scan_groups(meta, pools, control)`
   next to `sweep_groups`, returning a `RepairOutcome` with per-group refusals
   and the counters `ScrubSummary` already has. For each `GcGroupScope`:
   * take `try_linearizable_group_snapshot(group, &[], ["/db/", "/s3/",
     "/block/"])` (`crates/yadb-metakv/src/raft.rs:361`). A follower or a group
     without a quorum is a refusal for that group; there is no fallback to local
     applied state, and one group's refusal does not stop another group's pass,
     exactly as `sweep_groups` behaves;
   * build the live set from the returned rows only, never from
     `try_scan_prefix`, so a lagging applied state cannot be mistaken for a
     complete view;
   * repair each live blob through `repair_blob`.
   * **Do not copy the `project_ids.is_some()` guard** from
     `collect_orphans_in_scopes_with` (`crates/yadb-s3/src/lib.rs:1563`). That
     guard exists because orphan collection enumerates projects; this pass is
     prefix-scoped, so a `project_ids == None` scope is not a blocker. The
     doc comment must say this, or the next reader will copy the refusal.
2. **`crates/yadbd/src/main.rs`** — the OpenRaft branch of `scrub_all_pools`
   reports the real counters instead of `repair_error: Some(...)`. `repair_error`
   becomes `Some` only when a group refused, naming the group.
   `SCRUB_MULTIRAFT_REFUSAL` is then deleted, together with its
   `#[allow(dead_code)]` twin in `gc.rs` if nothing else anchors on it.
3. **`handle_pools_scrub`** — replace the unconditional `is_openraft() → 409`
   with the rule `/v1/pools/gc` already uses: `200` only when every group ran,
   otherwise `409` with the refusal in the body. Otherwise the route keeps
   claiming a failure after the capability exists.
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
5. **Guards and docs** — drop the scenario from `EXPECTED_SKIPS` and
   `REQUIRED_SUBSTRINGS` in `scripts/test_protocols_review.py`, replace
   `assert_multiraft_scrub_repair_refusal` with a positive assertion, and update
   `multiraft-verification.md`, `multiraft-runtime-operations.md`,
   `multiraft-production-todo.md` and `multiraft-gc-fencing-plan.md`.

### What this would still not prove

* `block_pool_lookup` comes from the local control-plane cache, not from the
  group snapshot. A stale cache makes the pass skip a blob; it can never make
  it repair or delete the wrong one. The code must say this instead of implying
  the whole pass is quorum-derived.
* A blob published after the snapshot is not repaired by that pass; the next
  pass sees it. Repair is idempotent, not atomic with publication.
* There is still no drill for a group leader dying between a fragment write and
  its manifest commit. That hole exists independently of this SKIP.

## SKIP 2: `reader lease`

### What it says today

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

### Two options

**A. Keep the SKIP, narrow it, and prove the pin instead (recommended).**

The current wording reads as "Multi-Raft does not fence reads". That is false:
the read path fences itself. The wording should say that no *metadata* lease
exists by design, because the fragment pin already fences the read, and cite the
pin as the mechanism. The SKIP stays as the record that the metadata mechanism
is absent.

Add a positive drill rather than only a note: stall one slot's response through
the harness transport proxy while an S3 `GET` is in flight, assert that the
slot's `/pins` surface shows a live pin, then release the response and assert
the pin drains and the object is intact. The proxy machinery and the `/pins`
route both exist; `assert_vdisk_fence_rpc` already drives that route directly.

**B. Merge `feat/reader-lease`.**

This removes the SKIP by implementing the mechanism, at the cost of roughly 567
changed lines in `main.rs`, a lease-provenance check on every collection pass,
and a second fence that duplicates the pin. It still would not produce a
cluster-level PASS for the interesting case: a collector losing its lease
mid-pass is not injectable in the daemon, which
`multiraft-gc-fencing-plan.md` already records as an open gap. Option B buys a
removed SKIP, not a stronger proof.

## Two gaps recorded only inside SKIP 1

The `Multi-Raft repair scan` reason is currently the only place that records two
facts. Removing it would silently delete them, so both must be handled in the
same change:

1. **Orphan collection runs per group but has no live drill**, because an upload
   intent only becomes collectable after `INTENT_GRACE_SECS = 300`
   (`crates/yadb-s3/src/lib.rs:17`) and no run waits that long. Close it by
   making the grace a configuration value whose default stays 300, provisioning
   it only from the test harness, and adding a drill: with the grace at one
   second, an abandoned intent is collected; with the default, the same intent
   is preserved. The second half is the inversion that shows the grace is what
   protects it.
2. **A scope with `project_ids == None` cannot be collected** — that is a
   single-group Multi-Raft deployment, where `collect_orphans_in_scopes_with`
   refuses. This is a real gap, unrelated to the repair scan, and needs its own
   record in `multiraft-runtime-operations.md`.

## Order and effect on the gate

1. Per-group repair (`gc.rs`), with unit tests and the follower refusal.
2. Routing and the `200`/`409` rule (`main.rs`).
3. Live repair drill plus its inversion.
4. `intent_grace_secs` knob and the orphan drill plus its inversion.
5. Reader-pin drill; narrowed `reader lease` wording.
6. Guards, docs, and a full gate run under the shared `cargo` lock.

Expected counts, from today's 23 PASS / 2 SKIP. Closing a SKIP does not by itself
add a PASS line: the scrub scenario already prints one, and its text changes from
"refuses its repair half" to "repairs per group". Each new drill that reports
separately adds one:

| Step | PASS | SKIP |
|---|---|---|
| Repair scan closes (rewritten scrub scenario) | 23 | 1 |
| Live repair drill reported separately | 24 | 1 |
| Orphan drill (step 4) | 25 | 1 |
| Reader-pin drill (step 5) | 26 | 1 |

The floor is 23 PASS / 1 SKIP: the repair scan implemented, the `reader lease`
SKIP narrowed, and no new drill. A drill that turns out not to be injectable is
recorded as such here and in the SKIP reason; it is never counted as a PASS.

## What must not change

* No default is altered to make a drill pass: the snapshot knobs, the intent
  grace and the collection interval keep their shipped values, and the harness
  overrides them explicitly.
* No fallback to locally applied state, no automatic re-initialisation, no retry
  of an ambiguous mutation.
* No SKIP is deleted to make the inventory shorter; it is either implemented or
  narrowed to what is genuinely absent.
* TLS/mTLS, routing identities and the internal token surface are untouched.
