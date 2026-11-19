# Plan: storage-side fencing for Multi-Raft garbage collection

**Status: the mechanism below is implemented on both metadata runtimes.** On
Multi-Raft, collection runs per data group through `sweep_groups`, and the four
drills in "What would prove it" are exercised by `tests/protocols.py`; drill 3
(a collector losing its lease mid-pass) is not injectable at the cluster level
and stays covered by the storage-level drill only. Six pieces exist in `main`: a barrier carries
the collector lease deadline and the target refuses an expired one; a target
keeps a durable epoch it can refuse an exact delete against (see "What the target
epoch now establishes"); a target keeps a durable pin table it can refuse an
epoch advance and a collectable delete against (see "What the pin table now
establishes"); a remote slot reaches all of it over the internal VDisk surface
(see "What the fence transport now establishes"); the read path pins a slot
before every fragment fetch and fails closed when the pin is refused (see "The
read path now pins"); and the legacy sweep advances every slot of a pool before
its first deletion and deletes through the collectable form (see "The collector
pass now advances first").

Multi-Raft mark-and-sweep runs: `POST /v1/pools/gc` calls `sweep`, which routes
to `sweep_groups`, one leader-only pass per data group, reaching all of the
above; the route answers `409` only when a group or pool refused.
`POST /v1/pools/scrub` runs a per-group repair scan and then the same sweep. It
answers `200` only when `repair_error` and `gc_refusal` are both `None`, and
`409` otherwise - the `/v1/pools/gc` rule. A node leads only some groups on a
multi-node cluster, so its scrub is `409` while the groups it leads are
repaired; the refusal names the groups rather than a missing capability. The
repair pass takes no collector lease (see "The repair pass takes no collector
lease"); what the plan does not yet establish is a collector lease expiring
mid-pass at cluster level, which the daemon cannot inject.

## Why a reader lease alone is not a fence

A reader lease is published as a metadata record and a collector refuses to run
while one is live. That is not a fence, for three reasons found in review:

1. The collector reaps an expired record and then continues its pass. A reader
   whose lease expired mid-read is not stopped, because the reader does not
   re-check the lease around each fragment fetch.
2. `ensure_live` verifies that the record is still present at the revision it was
   published at. It does not compare `expires_at_ms` with the current time, so a
   reader can act on a lease the collector already considers expired but has not
   yet reaped.
3. A lease is a *metadata* record. The deletion happens on a *storage target*
   that never sees it, so nothing on the target refuses a delete that races a
   read in flight.

Only (3) is fundamental. A lease can be made airtight inside the metadata group
— that is a local ordering problem — but it cannot reach the process that
actually unlinks the fragment. Collection is only safe if the target itself can
tell a safe delete from an unsafe one. This is why the plan did not rely on a
reader lease alone; it is not why the lease is forbidden. The lease implemented
later (see "The reader lease: rejected first, then implemented") is an additional
coarse fence the collector consults, while the pin is what the target can
evaluate.

### What has since been closed

Item 2 no longer holds. `ensure_live` refuses a record whose `expires_at_ms` has
passed, even when the record is still present and still owned, and all three
renewal paths refuse to extend an expired record rather than resurrecting it
(`crates/yadb-metakv/src/gc.rs`). A reader or writer cannot act under a lease the
collector already considers dead, and `begin_read_lease` publishes a fresh record
instead of reusing a dead one.

The renewal race is closed too. The three renewal paths are now one function
(`extend`), and it refuses in both directions: the record must be live when it is
checked, and the write must be acknowledged before the deadline it was extending.
A compare-and-swap proves ownership, not liveness, so without the second check a
record that expired during the round trip was extended and the holder went on
deleting under a fence that had already lapsed. The write still lands in that
case, which is deliberate: the record keeps its extended deadline and expires on
its own, so admission stays refused until it lapses rather than being admitted
under a fence the holder is abandoning. The cost is that a holder cannot renew
with less than one round trip of its TTL left, and it stops instead of guessing.

Item 1 is partly closed. A collector pass now re-proves the fence *while* it
deletes: `sweep_pool` checks before its first deletion, before every barrier
group and every 32 exact tombstones, and stops the pool and the pass on a failed
check. The overshoot after expiry is therefore bounded to a few dozen tombstones
per pool instead of the whole dead set.

The barrier half of item 1 is closed to the same standard. A barrier is the one
deletion the check interval cannot bound — one durable record evicts a whole
generation range — so the deadline a successful check proves now travels with the
request: `BarrierRequest.lease_expires_at_ms`, through `apply_barrier` into
`collect_garbage_barrier`, which compares it with its own clock immediately before
the barrier record and the eviction and refuses with
`BlobError::BarrierLeaseExpired`. `handle_vdisk_barrier` answers that refusal with
`409`, not `500`: the request was well formed and refused on purpose, and the
caller must stop rather than retry. A barrier that waited in a queue, or crossed a
paused peer, therefore evicts nothing instead of deleting for a pass that lost its
fence.

A target that cannot read its own clock refuses as well, with
`BlobError::BarrierClockUnreadable` and the same `409`. The check is a guard on
deletion, and the tempting default — substitute `0` for an unreadable clock — makes
every deadline look live, which is the direction that deletes. The two refusals
stay distinct so an operator can tell "the pass was too slow" from "this target's
clock is broken".

What remains open for a barrier is a window, not a missing check: the deadline is
read, then the barrier record is written and fsynced, and a lease that lapses
inside that write-and-fsync still lets the eviction through. The window is the
target's own write latency, and it widens by the skew between the collector's
clock and the target's — the two are compared directly, so a target clock running
behind extends it. This is narrower than the previous "no check at all" by the
whole duration of a queued or retried request, and it is not zero.

The fix does not reduce the weight of item 3. The remaining gap is unchanged:
metadata can be airtight and the target still deletes bytes a reader is holding.
Do not read it as permission to enable collection.

## What the target epoch now establishes

The first increment of item 3 was its durable, single-target half, in
`crates/yadb-blob` (`VolumeStore`); the sections that follow extend it over the
wire and into the collector. At that increment nothing crossed a process
boundary.

What exists now:

* Each volume keeps a durable `epoch: u64`. On open it is derived as the maximum
  over every record in the volume's own log; it is never invented, and it is
  never advanced by open, recovery, a put, a delete, a barrier or a compaction.
* An advance is an appended `REC_KIND_EPOCH` record plus its sync, through
  `advance_epoch(expected_epoch)`. The method refuses a caller whose expected
  epoch is not the target's current one, so a superseded collector cannot move
  the epoch past a newer one. Because the value is the maximum over records and
  the record is never rewritten in place, a torn advance can only fail to raise
  the epoch, never reset it, and an acknowledged advance is always one that
  survived. A sync that fails after persisting leaves the higher value behind:
  the epoch can over-advance, which fences more, never less.
* Every PUT record carries the epoch it was written under. The previous 33-byte
  record header is still read and its records belong to epoch 0, so a volume
  written before this increment keeps opening. Compaction preserves each
  fragment's epoch and rewrites the current epoch record, so neither is lost when
  a segment is reclaimed.
* `delete_fenced(id, expected_epoch)` refuses to unlink a fragment recorded under
  any other epoch, with `BlobError::FencedByEpoch`, which names both epochs. The
  refusal happens before any record is appended, so a refused delete leaves no
  tombstone. The unfenced `delete` remains, documented as unfenced, and the
  epoch comparison is equality — the strict form.

These properties are covered by unit tests in `crates/yadb-blob/src/lib.rs`,
including the torn-advance case driven through `SimFs` fault injection
(`torn_write_prob`) and a legacy 33-byte record built by hand.

What it does **not** establish, and why the invariant still does not hold:

* Equality, not "older than". A collector that advanced to N+1 cannot collect an
  epoch-N fragment through this path, because nothing on the target says whether
  an epoch-N reader still holds it. That relaxation is only sound with pins, and
  it belongs to the increment that adds them.
* At this increment nothing carried the epoch over the wire: `PoolSlot::delete`
  was the unfenced delete, there was no fence header or signed token on the
  fragment endpoint, and a barrier carried the collector lease deadline, not an
  epoch. The internal VDisk routes below now carry it, and `delete_collectable`
  is the collector's delete.
* The epoch alone cannot answer "is this epoch still readable?", and the pin
  table that can (see "What the pin table now establishes") had no client at
  this increment; the read path and the collector now use it.
* At this increment no collector advanced an epoch and GC was still refused.
  Both are now false: `sweep_pool` advances every slot and the collector runs on
  Multi-Raft. The unfenced `delete` is still callable.

### On-disk format: the upgrade is one-way

The record header grew from 33 to 41 bytes and its magic changed from `YBRC` to
`YBRD`, because the epoch had to be covered by the header CRC rather than appended
outside it. The direction of compatibility is deliberate and only one way:

* an older volume (all `YBRC` records) opens on this code, with every record in
  epoch 0;
* a volume written by this code does **not** open on an older binary. The old
  recovery compares the magic, does not recognise `YBRD`, and refuses with
  `InvalidSegment("corrupt record header")` instead of skipping the record.

That is the fail-closed direction — an old binary cannot read a fragment, let
alone delete one — but it means a rollback after an upgrade requires restoring the
volume from a backup taken before it, not just swapping the binary back. Plan the
upgrade as irreversible per volume, and do not mix binaries across a rollback of
the same directory.

## What the pin table now establishes

The second increment of item 3 is in place, and again only the durable,
single-target half, in `crates/yadb-blob` (`VolumeStore`). Nothing crosses a
process boundary yet.

What exists now:

* A pin is a durable record (`REC_KIND_PIN`) carrying a pin id, the epoch it
  names and `expires_at_ms`; a release is a second record (`REC_KIND_UNPIN`)
  naming the id. Neither is rewritten in place. Recovery replays them in segment
  order, so the later record wins, and drops the pins whose deadline has passed.
  Compaction copies every unexpired pin into the replacement segment; expired
  pins may be dropped.
* `pin(epoch, ttl_ms)` refuses any epoch but the current one
  (`PinEpochNotCurrent`, naming both), refuses `ttl_ms == 0`, and refuses a TTL
  above `MAX_PIN_TTL_MS` (five minutes) rather than clamping it. The deadline it
  returns is the target's own clock plus the TTL.
* `unpin(pin_id)` is idempotent and one-directional: `Ok(true)` only when a live
  pin was removed, `Ok(false)` for an unknown or already expired id, and never a
  resurrection or an extension.
* `advance_epoch` keeps its expected-epoch refusal and additionally refuses while
  any unexpired pin exists, with `EpochPinned`, which names the epoch and the
  number of pins. Both are stop-the-pass.
* `delete_collectable(id, collector_epoch)` implements "fragment epoch <
  collector epoch, and no live pin on the slot". A fragment that is not older is
  refused with `FragmentNotOlder`; one protected by a live pin is refused with
  `FragmentPinned`; both name the fragment's epoch and the collector's, and both
  happen before any record is appended. The pin rule is deliberately "any live
  pin", not "a pin that names this fragment's epoch". `pin` accepts only the
  target's current epoch, so a pin naming an older fragment's epoch cannot exist
  - and the first version of this rule, which asked for one, therefore admitted
  deleting the very fragments it was meant to protect. The cost of the wider rule
  is that one live pin pauses every collectable delete on the slot until its TTL,
  which is the direction that deletes less. `delete_fenced` is unchanged: still
  the strict equality primitive, still documented as such.
* Liveness is `now < expires_at_ms`. Recovery and every read of the table go
  through one helper, and an unreadable clock refuses there
  (`PinClockUnreadable`) instead of reading as zero - the same failure direction
  as the barrier clock. Compaction is the one exception, and deliberately the
  other way: with no readable clock it keeps every pin, because dropping a live
  one is the worse error, and it drops only pins the clock proves expired.
  Segment replacement now selects pins through a pure helper,
  `select_pins_to_keep(pins, &Result<u64, String>)`, so both clock outcomes are
  tested without going through `SystemTime::now()`:
  `a_segment_replacement_copies_exactly_the_pins_live_on_its_clock` pins the
  strict `now < expires_at_ms` boundary and
  `an_unreadable_clock_keeps_every_pin_including_expired_ones` pins the
  fail-safe direction. Both were verified by inversion: `<=` at the boundary
  and an empty result on clock failure each turn the corresponding test red.
  The seam is the selection function, not `target_clock()` itself; the store
  still feeds it the real clock, and no test presents a broken clock to
  `SystemTime::now()`.

What it does **not** establish, and why the invariant still does not hold:

* No route verifies the signed fence token. The pin routes authenticate the
  connection with the shared internal Raft token, so "a superseded collector
  cannot delete" still rests on the epoch compare-and-swap, not on a token.
* A pin is taken per candidate fetch, not per client request. A multi-part read
  takes and releases one pin per candidate it tries, and a pin cannot be
  extended: the TTL is the maximum the target accepts, so a fetch that outlives
  five minutes loses its protection. No read path can extend a pin.
* The collector path that advances epochs and calls `delete_collectable` runs on
  both runtimes: the per-group functions share the same `sweep_pool`, and `sweep`
  routes to `sweep_groups` on Multi-Raft. The unfenced `delete` remains callable
  from any caller that has a `PoolSlot`.
* GC runs on Multi-Raft (`/v1/pools/gc`; `409` only when a group or pool
  refuses), and scrub runs the per-group repair scan and then the same sweep,
  answering `200` only when neither refused. Three of the four live
  drills in "What would prove it" run at cluster level; the fourth is not
  injectable there.
* `crates/yadb-blob/tests/epoch_fence_drills.rs` is the storage-side precursor
  of those drills. It drives `VolumeStore` directly and proves four storage
  invariants: a live pin refuses the advance and the collectable delete of every
  fragment older than the collector, including one written under an older epoch
  than the pin names; a superseded collector's epoch is refused by the advance
  and by the exact-epoch delete without changing the volume; a pass that stops at
  its first refusal leaves the later fragments intact; and a tombstone survives
  seal, compaction and a power-cut reopen, so a late write cannot resurrect a
  collected fragment. It proves no wire format, no collector loop, no reader
  path and nothing about a cluster, and it does not stand in for the four cluster
  drills.

The atomicity the collector protocol depends on is target-side, not caller-side:
`advance_epoch` re-reads the pin table and compares the expected epoch under the
same lock it appends under, so "no pins" and "epoch advanced" are one step from
the target's point of view. A pin that lands first makes the advance refuse; a
pin that lands afterwards names the new epoch and is covered by the "any live
pin" delete rule. The caller brackets two operations rather than holding a
directory lock across both.

## The read path now pins

`PoolStriper::get_blob` and `repair_blob` fetch every candidate part through
`read_part`, which for each candidate slot:

1. reads the slot's durable epoch,
2. takes a pin on exactly that epoch with `READ_PIN_TTL_MS` (the maximum the
   target accepts), and
3. fetches the fragment under a guard that releases the pin in `Drop`.

The epoch is read before it is named on purpose: a collector that advances in
between makes `pin` fail with `PinEpochNotCurrent`, and the read treats that as
a refusal rather than pinning an epoch the target no longer holds.

A failed pin is never "the fragment is absent". `read_part` returns the failure
to the caller, `get_blob` reports it instead of `NotFound` or
`InsufficientFragments`, and `repair_blob` stops instead of reconstructing a
fragment it could not prove missing. Reading around a down replica is unchanged:
the failure is only reported when no other candidate supplied the part, and a
successful reconstruction never consults it. This includes transport failures -
an unreachable or poisoned target cannot prove absence either.

What this does not establish: there is no *request-level* pin (one pin covering a
whole S3 GET or SQL scan) and no pin renewal. A per-group reader lease is now
published on Multi-Raft (see "The reader lease: rejected first, then implemented"),
but it is a metadata back-off signal, not a substitute for the pin: the pin
protects one candidate fetch at a time, which is what the delete rule needs.

## The collector pass now advances first

`crates/yadbd/src/gc.rs::sweep_pool` is generic over the storage filesystem and,
on the legacy runtime, does the following before its first deletion:

1. `advance_pool_epochs` reads every slot's epoch, then advances every slot with
   the value it read. A read failure or an advance refusal records a pool
   refusal in `pools_refused` and `errors` and stops the pool with nothing
   deleted. There is no retry and no "advance the others and continue".
2. Exact deletions go through `delete_collectable_parts`, which asks every slot
   of the pool for every part with that slot's new epoch. `FragmentNotOlder`,
   `FragmentPinned` and `FencedByEpoch` stop the pool immediately; only an
   unavailable target (`Io`) is recorded and stepped over, matching how the pool
   treats a down slot elsewhere.
3. Barriers still carry the collector lease deadline and not the epoch, and the
   fence is still checked before the first deletion, before every barrier group
   and every 32 exact tombstones.

The per-group path (`sweep_groups` -> `sweep_group`) uses the same `sweep_pool`,
so it inherits the advance-then-delete ordering. It additionally checks that
every live lease it found could have been published by the group it is sweeping:
`lease_belongs_to_group` compares the record's `group` field and its claimed
`revision` against the group's applied index, re-read after admission closed, and
refuses the group on a mismatch. Neither field is self-enforcing - both are
written by the leaseholder - so trusting them would let a foreign or forged
record act as a fence.

What this does not establish: the ordering is enforced structurally (the advance
is the first statement), not observed as a write sequence; no test has a
concurrent reader fetching while a pass runs; and no drill runs two collectors on
one group. The provenance check does have a runtime caller now: `sweep_groups`
runs it on Multi-Raft.

## The repair pass takes no collector lease

`repair_scan_groups` takes no collector lease and publishes no lease of its own.
It advances no epoch and never calls `delete_collectable`; the only pin in its
path is the one `read_part` takes around each candidate fetch, exactly as a
client read does. It is not fenced by the collector lease, and nothing here
should be read as saying it is. What makes it safe next to the collection fence
is narrower and structural:

* **It reads under the same target-side fence as any client.** Every candidate
  part is fetched through `read_part`, which reads the slot's epoch and pins it
  before the fetch. A slot that refuses the pin stops the repair instead of
  being treated as a missing part, so repair never reads bytes a collector is
  reclaiming, and it never reconstructs a part it could not prove missing.
* **Its only deletion is a duplicate.** For each part, repair writes the bytes
  to the part's nominal slot and, only after that `put` succeeded, removes the
  handoff copy. The deleted fragment is the same part id that is already durable
  at its nominal slot, so the delete can only take redundancy from two copies to
  one - never the last good copy. The delete is the unfenced `delete`, its
  result is ignored, and a collector that removed the handoff copy first is
  indistinguishable from success: the object still reads back, and the next pass
  retries whatever is left. This is assimilation, not collection.
* **It cannot resurrect a blob a collector removed.** A tombstoned id refuses
  the nominal write, so the handoff delete is not reached; and a blob whose
  manifest the group's snapshot does not name is never handed to `repair_blob`
  at all.
* **It needs no lease because it has no append-then-publish window.** A writer
  holds a writer lease between the fragment append and the publication that
  names it, because a mark taken before that publication would omit the blob. A
  repair writes fragments only for blobs that are *already published* in the
  quorum-confirmed snapshot of the group that owns them. `sweep_group` checks the
  writer fence and only then takes the group's collector lease before it marks,
  so a concurrent pass either sees the publication's writer lease live and
  refuses, or has already closed admission - and a publication attempted after
  that loses its CAS on the collector record and is refused. A blob repair can
  see is therefore in every mark a concurrent pass can compute, and the repaired
  fragments are named live rather than swept. The worst case runs the other way:
  the manifest is deleted after repair's snapshot, so the next mark omits the
  blob and collects the fragments repair just wrote - deleting more, not less.

Two repair passes over one group are also safe without coordination: the write
target is a deterministic nominal slot for a content-addressed part, so the
writes are identical and the handoff deletes are idempotent.

What this does not establish: no drill runs a repair pass concurrently with a
sweep, so the interleavings above are reasoned from the ordering of the
statements, not observed; and no drill runs two repair passes on one group at
once. The handoff delete is unfenced by design, so "repair cannot delete a
fragment a live pin protects" is not a claim this plan makes - the claim is only
that the fragment it deletes is a duplicate of one already durable at the
nominal slot.

## The reader lease: rejected first, then implemented

This section was written before the reversal and is kept as the record of the
decision. It said the parked branch (`feat/reader-lease`, rebased onto `main`
and building) should not be merged before the pin protocol existed. The branch
publishes a per-group reader lease on Multi-Raft: `reader_lease` becomes a map
keyed by metadata group, the record carries the group's applied log index
instead of a deployment-wide revision, and reads forward to the group leader so
they can publish one. The reasons to wait were:

* **It is the mechanism this plan demotes, not the one it depends on.** The
  component table demotes the lease to an optimisation once pins exist. Building
  the lease client first puts the weaker mechanism in the hot path and leaves the
  correctness mechanism still without a client.
* **It costs a committed write per read and buys nothing yet.** `renew` is a CAS
  through `extend`, so every fenced read is a Raft round trip; and every read is
  forced onto the group leader, which is the read-scaling property Multi-Raft
  exists to provide. The only consumer of a reader lease was `sweep_group`, and
  `sweep` returned its refusal before reaching it, so on Multi-Raft the record
  was published and never consulted.

Both reasons were true when written, and the second was the decisive one: a
record nobody consults is not a fence. They stopped being decisive when the
per-group collector was enabled. `sweep_group` now reaches the lease check,
consults the group's reader records after closing admission, and refuses the
group while one is live; the lease is therefore a collector-visible back-off
signal, not a write that is ignored. The branch was rebased, its per-group
keying and `lease_belongs_to_group` provenance check were kept, and the
implementation is `e4555c5`, merged into `main` on 2026-11-19.

What the reversal does not change: the pin is still the correctness mechanism,
and the lease is still coarser. It fences the whole metadata group rather than
the fragment being read, and it blocks collection for up to its 30 s TTL after
the last read. The reasons above are the cost, and the cost was accepted
because the collector can now be told to back off without inspecting every
target. A collector that loses its lease mid-pass is still not injectable at
cluster level; that gap is unchanged.


## Invariant to establish

> A fragment is deleted only if no reader can still hold a reference to it, and
> a reader that holds a reference can always complete or fail cleanly, never
> read bytes that were reclaimed underneath it.

Equivalently: for each fragment, the target must be able to answer "is this
epoch still readable?" with a value that cannot be stale for longer than the
reader's own pin.

## Design: epochs on the target, pins carried by readers

Three pieces, all of them required.

### 1. A target-side epoch

Each storage target keeps a durable `epoch: u64`, monotonic, incremented only by
the collector under the target's directory lock. Every fragment write records the
epoch it was created under, so a target can always say which epoch a fragment
belongs to.

The storage half of this is implemented (see "What the target epoch now
establishes"). When this design was written, the collector side was not: no
pass took the directory lock, read a pin set, or advanced an epoch. That is no
longer the state of the tree — "The collector pass now advances first" records
the collector side — but the ordering argument below is the one that made the
advance a requirement.

### 2. A delete that names its epoch

Replace the current delete with a versioned RPC that carries the collector's
fence epoch:

```
DELETE /internal/fragment/{blob}/{part}
x-yadb-fence-epoch: <u64>
x-yadb-fence-token: <signed by the group's node key>
```

The target refuses (a distinct `409 stale fence`, not a generic failure) when the
carried epoch is older than its own, and refuses when the token does not verify
against the group identity. A collector that was superseded therefore cannot
delete, even if it is still running with a lease it believes is live. This is the
property a metadata lease cannot provide, because it is enforced where the bytes
are.

### 3. A reader pin with a bounded lifetime

A reader that fetches fragments takes a pin on the epoch it is reading:

```
POST /internal/vdisk/{pool}/{slot}/pin    {epoch, ttl_ms} -> {pin_id, epoch, expires_at_ms}
POST /internal/vdisk/{pool}/{slot}/unpin  {pin_id}        -> {released}
```

The target refuses to advance its epoch while any pin is unexpired, and refuses
to delete any fragment older than the collector while any pin is live. Pins are
held by the *reader's* process, not by the metadata group, so no metadata
ordering is involved: the target is the single arbiter.

Pins are taken before the fragment is fetched and released as soon as the bytes
are copied. A read that cannot take a pin fails closed instead of reading
unfenced. (Implemented: see "The read path now pins".)

#### Exact semantics the target must implement

These are the decisions that a later increment must not improvise; they follow from
the ordering above and from the same fail-closed rule the barrier uses.

* **Pins are durable, not in-memory.** A pin record is appended and synced like
  any other record. If the target forgets a pin across a restart, a collector can
  advance past a reader that is still holding bytes — the one direction that must
  not happen. A pin that outlives its reader only blocks collection until its TTL
  runs out, which is the safe direction.
* **A pin names one epoch, and it must be the current one.** A pin for an older
  epoch is refused: the reader would be pinning an epoch the collector may already
  have advanced past, so the pin would prove nothing. A pin for a newer epoch is
  also refused — that epoch does not exist yet. (Implemented: `pin` returns
  `PinEpochNotCurrent` naming both epochs.)
* **`advance_epoch` refuses while any unexpired pin exists**, with a distinct
  error, in addition to the existing expected-epoch check. Both refusals are "stop
  this pass", never retryable.
* **Liveness is `now < expires_at_ms`, and an unreadable clock refuses.** The
  comparison must not default to `0`: that would make every pin look expired and
  let the advance through. This is the same failure direction as the barrier clock.
* **TTL is bounded.** `ttl_ms` of zero is refused, and a TTL above a documented
  maximum is clamped or refused rather than honoured, so one buggy reader cannot
  hold collection off indefinitely.
* **The delete rule becomes "older, and unpinned"**: a collector at epoch `C` may
  delete a fragment of epoch `f` only when `f < C` and no unexpired pin is live
  on the slot. Not "no pin names `f`": only the current epoch can be pinned, so
  the narrower form admits deleting the fragments a reader is actually holding.
  This is the step that makes the epoch useful at all — the equality rule in
  `delete_fenced` refuses everything older, so a collector using it would delete
  nothing. Keep `delete_fenced` as the strict, equality-checked primitive and add
  the collectable form next to it rather than loosening it.
* **Compaction keeps unexpired pins and may drop expired ones.** Dropping a live
  pin while rewriting a segment would be the same loss as forgetting it on restart.
* **Recovery drops expired pins and keeps the rest**, and never invents a pin.

#### How one pin protects fragments the reader did not write

A reader pins the target's current epoch, but the fragment it fetches may have
been written under an older one. A per-fragment rule of "no unexpired pin names
`f`" does not cover that case: nothing can pin an older epoch, so a fragment from
epoch `N-1` has no pin of its own and would look collectable at collector epoch
`N+1`. That was the first implementation's rule, and the hole was real: a reader
pinned epoch 1 and the collector deleted the epoch-0 fragment underneath it.

The rule is therefore "any live pin on the slot", which covers the older fragment
directly, plus the advance refusal as a second layer:

1. A reader reads epoch `N` and pins it. A collector's `advance_epoch(N)` is now
   refused, so the collector's pass stops with nothing deleted — including the
   older fragments, which were never reached.
2. If the collector advanced to `N+1` first, the reader's `pin(N)` is refused as
   not-current, and the read fails closed rather than fetch unfenced.
3. If the pin lands after the advance, it names `N+1`, and the "any live pin"
   delete rule refuses every fragment older than `N+1` — which is every fragment
   the reader could be holding.

So the ordering is load-bearing in both directions, and a reader that treats a
refused pin as "retry with the new epoch" would silently read a fragment the
collector may already have deleted. A retry is only safe if it re-reads the
fragment under the new pin, which is a different operation from resuming the one
in flight.

## Epoch advance protocol (collector)

1. Read the group's applied index and the target list.
2. For each target, read the epoch, then advance it with that value. The target
   re-reads its pin table and refuses if any pin is unexpired, so "no pins" and
   "epoch advanced" is one target-side step. Abort the pass and delete nothing
   on any refusal.
3. Mark-and-sweep using deletes that carry the new epoch.
4. Fragments of older epochs are now collectable, and the target refuses any
   fragment that is not older or that any live pin still protects.

The check-and-advance being one target-side step is the whole point: a pin taken
concurrently either lands before it (the advance refuses, the pass aborts) or
after it (it names the new epoch, and the "any live pin" delete rule then
protects everything older than that epoch).

Implemented for the legacy runtime in `advance_pool_epochs`; see "The collector
pass now advances first". The plan's earlier "take the target's directory lock"
is not how it is done: the caller does not hold a lock across two operations, it
issues one operation the target makes atomic. That is a narrower interface and
the same guarantee.

The unit of the protocol is one VDisk slot, not one pool. Each slot has its own
durable epoch and pin table, so the collector reads and advances every slot that
can hold a dead fragment, and it advances all of them before the first deletion
in that group. A refusal on any one slot - a live pin, a stale expected epoch, an
unreadable clock - stops the group's pass with nothing deleted: deleting on the
slots that did advance while another slot still had a pinned reader would be
exactly the partial fence this protocol exists to prevent. Handoff copies make
the set a superset of the nominal slots: every candidate slot the pool can place
a part on is advanced too. A barrier carries the collector lease deadline but not
the epoch; after the advance has proved that no pin exists on any epoch and
admission is closed, a barrier can only evict fragments the mark has already
proved dead, so it does not need a separate epoch field.

## What the fence transport now establishes

A remote slot can now reach a target's epoch and pin table. `PoolSlot` exposes
`epoch`, `live_pins`, `pin`, `unpin`, `advance_epoch`, `delete_fenced` and
`delete_collectable` for a slot backed by another process, and the daemon serves
them on the internal VDisk surface:

| Route | Effect |
| --- | --- |
| `GET /internal/vdisk/{pool}/{slot}/epoch` | the durable epoch |
| `GET /internal/vdisk/{pool}/{slot}/pins` | every unexpired pin |
| `POST /internal/vdisk/{pool}/{slot}/pin` | take a pin on one epoch for a TTL |
| `POST /internal/vdisk/{pool}/{slot}/unpin` | release a pin id, idempotently |
| `POST /internal/vdisk/{pool}/{slot}/advance` | raise the epoch from an expected value |
| `DELETE /internal/vdisk/{pool}/{slot}/blob/{blob_hex}?fence_epoch=` | delete only if the fragment's epoch equals the named one |
| `DELETE …?collector_epoch=` | delete only if the fragment is older than the named epoch and unpinned |

Every handler requires the internal Raft token, and a refusal is a `409` with a
stable code (`epoch_mismatch`, `epoch_pinned`, `fenced_by_epoch`,
`fragment_not_older`, `fragment_pinned`, `epoch_exhausted`, …). The client maps
`storage_unavailable` to `StorageUnavailable` and every other non-200 to
`RemoteFenceRefused`; only a transport failure becomes `Io`. That distinction is
the point: "the target refused" and "the target could not be asked" have
different consequences for a collector, and neither may be read as success.

What this does **not** establish:

* **No route verifies a fence token.** The handlers authenticate the connection
  with the shared internal Raft token, not the operation with a group-key
  signature. `yadb_metakv::fence_token` exists as a signed primitive - group,
  sender, recipient, operation, epoch, deadline, under a domain prefix separate
  from the Raft envelope - but it has no consumer. So "a superseded collector
  cannot delete" rests today on the target's epoch compare-and-swap alone. The
  token would add the property that the *operation*, not just the connection,
  was issued by a voter of that group; it does not exist yet.
* **These routes now have callers.** The collector advances an epoch and calls
  `delete_collectable` through `sweep_pool` on Multi-Raft, and the read path takes
  a pin before every fragment fetch. Three of the four drills in "What would
  prove it" run at cluster level; the mid-pass lease loss is not injectable
  there.

## What has to change, by component

| Component | Change |
| --- | --- |
| `crates/yadb-metakv/src/gc.rs` | Keep the admission CAS and the provenance check, but demote the lease to an optimisation: correctness no longer depends on it |
| `crates/yadbd/src/gc.rs` | Sweep only with a fence epoch obtained per target; treat `stale fence` as "stop this pass", never as retryable |
| storage target (blob volume + the daemon's fragment endpoint) | Durable epoch and pin table with expiry (done for the volume), versioned delete RPC (done, see above), signed fence token (primitive only, no route verifies it) |
| reader path (S3/DB/iSCSI fragment fetch) | Pin before fetch, unpin after; fail closed when a pin is refused |
| `crates/yadb-metakv/src/openraft_transport.rs` | Sign the fence token with the existing per-node key; do not invent a second identity (done: `sign`/`verify_signature` are shared) |
| `tests/protocols.py` | A live drill: start a pass, hold a read open, assert the fragment survives; then assert it is collected after the pin is released |

## What would prove it

1. A reader pinned on epoch N reads while a collector advances to N+1; the
   fragment is not deleted and the read returns the exact bytes.
2. A superseded collector carrying epoch N-1 gets `409 stale fence` from every
   target and deletes nothing.
3. A collector that loses its lease mid-pass stops at the first `stale fence`
   and reports partial progress without retrying.
4. After all pins expire, the same fragments are collected and `degraded_puts`
   stays zero.

Drills 1, 2 and 4 run in `tests/protocols.py` on a three-process cluster
(`assert_multiraft_gc_drills`). Drill 3 is not injectable at the cluster level:
the daemon has no way to expire a collector lease in the middle of a pass, so
only the storage-level drill below covers it, and no PASS is claimed for the
cluster-level form.

`crates/yadb-blob/tests/epoch_fence_drills.rs` covers the storage half of
drills 1, 2 and 4 and the "stop, do not retry" shape of drill 3, against a
deterministic `SimFs`. It is a precursor, not a substitute: it never starts a
daemon, never carries an epoch or a pin over a wire, and never runs a collector
loop. A green run there does not move any of the four drills above to "proven".

## Deliberately out of scope

* Reclaiming disk space already lost while collection is refused.
* Cross-group or cross-target atomicity: each target fences itself, and a pass
  that succeeds on two targets and fails on the third is a partial sweep, which
  is safe (it deletes less, never more).
* Any automatic repair of a target whose epoch file is damaged. That is a
  fail-closed stop, like every other damaged-artifact case.
