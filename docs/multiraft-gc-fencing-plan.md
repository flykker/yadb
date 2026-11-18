# Plan: storage-side fencing for Multi-Raft garbage collection

**Status: the mechanism below is design only and is not implemented**, apart from
two narrow pieces of storage-side fencing: a barrier carries the collector lease
deadline and the target refuses an expired one, and a target now keeps a durable
epoch it can refuse an exact delete against (see "What the target epoch now
establishes"). Neither piece establishes the invariant, and Multi-Raft
mark-and-sweep stays refused (`409` on `/v1/pools/gc` and `/v1/pools/scrub`,
periodic task logged and skipped) until the mechanism below exists and is proven.
Garbage accumulates in the meantime; that cost was accepted deliberately.

## Why the current refusal exists

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
tell a safe delete from an unsafe one.

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

The first increment of item 3 is in place, and only its durable, single-target
half, in `crates/yadb-blob` (`VolumeStore`). Nothing crosses a process boundary
yet.

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
* Nothing carries the epoch over the wire. `PoolSlot::delete` is still the
  unfenced delete, there is no fence header or signed token on the fragment
  endpoint, and `BarrierRequest`, `apply_barrier` and `collect_garbage_barrier`
  are unchanged. A remote collector cannot be fenced by any of this yet.
* There is no pin table, so a target can still only answer "which epoch does this
  fragment belong to?", not "is this epoch still readable?".
* No collector advances an epoch, and GC enablement is still refused. Until the
  delete RPC is replaced, nothing forces the collector path to use
  `delete_fenced` at all; the unfenced `delete` is still callable.

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
establishes"). What is not implemented is the collector side: no pass takes the
directory lock, reads a pin set, or advances an epoch, so nothing advances an
epoch today except a test.

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
POST /internal/pin  {epoch, lease_id, ttl_ms}  -> {pin_id, expires_at_ms}
POST /internal/unpin {pin_id}
```

The target refuses to advance its epoch while any pin on the current epoch is
unexpired, and refuses to delete fragments of an epoch that still has a live pin.
Pins are held by the *reader's* process, not by the metadata group, so no
metadata ordering is involved: the target is the single arbiter.

Pins must be taken before the fragment is fetched and released after the last
byte is handed to the client. A read that cannot take a pin fails closed with the
existing "reader lease unavailable" class of error rather than reading unfenced.

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
  also refused — that epoch does not exist yet.
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
  delete a fragment of epoch `f` only when `f < C` and no unexpired pin names `f`.
  This is the step that makes the epoch useful at all — the equality rule now in
  `delete_fenced` refuses everything older, so a collector using it would delete
  nothing. Keep `delete_fenced` as the strict, equality-checked primitive and add
  the collectable form next to it rather than loosening it.
* **Compaction keeps unexpired pins and may drop expired ones.** Dropping a live
  pin while rewriting a segment would be the same loss as forgetting it on restart.
* **Recovery drops expired pins and keeps the rest**, and never invents a pin.

## Epoch advance protocol (collector)

1. Read the group's applied index and the target list.
2. Take the target's directory lock, then re-read the pin set. If any pin is
   unexpired, abort the pass and delete nothing.
3. Increment `epoch`, sync it durably, release the lock.
4. Mark-and-sweep using deletes that carry the new epoch.
5. Fragments of older epochs that no pin referenced are now collectable.

The lock-then-read ordering is the whole point: it makes "no pins" and "epoch
advanced" a single atomic step from the target's point of view. A pin taken
concurrently either lands before the check (pass aborts) or after the advance
(it names the new epoch and fences only the new epoch's fragments).

## What has to change, by component

| Component | Change |
| --- | --- |
| `crates/yadb-metakv/src/gc.rs` | Keep the admission CAS and the provenance check, but demote the lease to an optimisation: correctness no longer depends on it |
| `crates/yadbd/src/gc.rs` | Sweep only with a fence epoch obtained per target; treat `stale fence` as "stop this pass", never as retryable |
| storage target (blob volume + the daemon's fragment endpoint) | Durable epoch (done for the volume, see above), pin table with expiry, versioned delete RPC, signed fence token |
| reader path (S3/DB/iSCSI fragment fetch) | Pin before fetch, unpin after; fail closed when a pin is refused |
| `crates/yadb-metakv/src/openraft_transport.rs` | Sign the fence token with the existing per-node key; do not invent a second identity |
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

Until all four pass in `tests/protocols.py` on a three-process cluster, the
refusal stays.

## Deliberately out of scope

* Reclaiming disk space already lost while collection is refused.
* Cross-group or cross-target atomicity: each target fences itself, and a pass
  that succeeds on two targets and fails on the third is a partial sweep, which
  is safe (it deletes less, never more).
* Any automatic repair of a target whose epoch file is damaged. That is a
  fail-closed stop, like every other damaged-artifact case.
