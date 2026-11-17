# Plan: storage-side fencing for Multi-Raft garbage collection

**Status: design only. Nothing in this document is implemented.** Multi-Raft
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
| storage target (blob volume + the daemon's fragment endpoint) | Durable epoch, pin table with expiry, versioned delete RPC, signed fence token |
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
