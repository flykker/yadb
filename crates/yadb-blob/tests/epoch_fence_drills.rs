//! Storage-level drills for the GC epoch/pin/delete fence.
//!
//! These are precursor drills, not the cluster drills. They drive
//! [`VolumeStore`]'s public epoch, pin and delete primitives directly on a
//! deterministic [`SimFs`], and prove only storage-side invariants:
//!
//! * a live reader pin blocks an epoch advance and every collectable delete on
//!   the slot, including fragments written under an older epoch than the pin
//!   names, because a reader can only pin the current epoch;
//! * a superseded collector's epoch is refused by both the advance and the
//!   exact-epoch delete, and a refusal leaves the volume unchanged;
//! * a collector pass stops at its first refusal and deletes nothing after it;
//! * a tombstone survives sealing, compaction and a power-cut reopen, so a
//!   collected fragment cannot be resurrected by a late write.
//!
//! They do not prove the wire protocol, the collector loop, a reader path, or
//! anything about a cluster. There is no RPC carrying an epoch or a pin, no
//! reader that takes a pin before a fetch, no collector that advances an epoch,
//! and no reader that fails closed when a pin is refused. The four cluster
//! drills in `tests/protocols.py` remain unrun; this file neither replaces them
//! nor claims they pass.

use yadb_blob::{BlobError, BlobId, VolumeConfig, VolumeStore};
use yadb_io::SimFs;

/// A fragment at epoch 0, then a reader pin on epoch 0. The pin must refuse the
/// advance and the collectable delete, the bytes must still be readable, and
/// once the pin is released both the advance and the delete must go through.
#[test]
fn drill_one_a_pinned_reader_blocks_the_advance_and_the_delete() {
    let fs = SimFs::new(0xE1_0001);
    let dir = "epoch_fence_drill_one";
    let store = VolumeStore::open(fs.clone(), dir, VolumeConfig::default()).unwrap();
    let id = BlobId::new(41, 1, 1, 1, 2, 0);
    let bytes = b"drill one: bytes held by a pinned reader";

    store.put(id, bytes).unwrap();
    assert_eq!(store.epoch(), 0, "a fresh volume starts at epoch 0");
    assert_eq!(store.fragment_epoch(id), Some(0));

    // A 60 s TTL is far longer than this test, so the pin is live throughout.
    let grant = store.pin(0, 60_000).unwrap();
    assert_eq!(grant.epoch, 0);

    match store.advance_epoch(0) {
        Err(BlobError::EpochPinned { epoch, live_pins }) => {
            assert_eq!(epoch, 0);
            assert_eq!(live_pins, 1);
        }
        other => panic!("a live pin must refuse the advance, got {other:?}"),
    }
    assert_eq!(store.epoch(), 0, "a refused advance writes nothing");

    // The collector names the epoch it tried to advance to (1), even though
    // that advance was refused. The same pin must refuse the delete.
    match store.delete_collectable(id, 1) {
        Err(BlobError::FragmentPinned {
            id: named,
            fragment_epoch,
            collector_epoch,
            live_pins,
        }) => {
            assert_eq!(named, id);
            assert_eq!(fragment_epoch, 0);
            assert_eq!(collector_epoch, 1);
            assert_eq!(live_pins, 1);
        }
        other => panic!("a live pin must refuse the collectable delete, got {other:?}"),
    }
    assert_eq!(
        store.get(id).unwrap(),
        bytes,
        "the pinned read still returns the exact bytes"
    );

    // Releasing the pin is what admits both steps; neither was retried before.
    assert!(store.unpin(grant.pin_id).unwrap());
    assert_eq!(store.advance_epoch(0).unwrap(), 1);
    assert!(store.delete_collectable(id, 1).unwrap());
    assert!(matches!(store.get(id), Err(BlobError::NotFound(missing)) if missing == id));
}

/// A collector that still believes it is at epoch 0 is refused by both the
/// advance and the exact-epoch delete, and neither refusal changes the volume.
#[test]
fn drill_two_a_superseded_collector_is_refused_by_every_primitive() {
    let fs = SimFs::new(0xE2_0002);
    let dir = "epoch_fence_drill_two";
    let store = VolumeStore::open(fs.clone(), dir, VolumeConfig::default()).unwrap();

    assert_eq!(store.advance_epoch(0).unwrap(), 1);

    // A superseded collector must not move the epoch past the newer one.
    match store.advance_epoch(0) {
        Err(BlobError::EpochMismatch {
            expected_epoch,
            actual_epoch,
        }) => {
            assert_eq!(expected_epoch, 0);
            assert_eq!(actual_epoch, 1);
        }
        other => panic!("a superseded advance must be refused, got {other:?}"),
    }

    let id = BlobId::new(42, 1, 1, 1, 2, 0);
    let bytes = b"drill two: written under the collector's new epoch";
    store.put(id, bytes).unwrap();
    assert_eq!(store.fragment_epoch(id), Some(1));

    // The exact-epoch delete names the old epoch, so it may not unlink a
    // fragment recorded under the new one.
    match store.delete_fenced(id, 0) {
        Err(BlobError::FencedByEpoch {
            id: named,
            recorded_epoch,
            expected_epoch,
        }) => {
            assert_eq!(named, id);
            assert_eq!(recorded_epoch, 1);
            assert_eq!(expected_epoch, 0);
        }
        other => panic!("a superseded fenced delete must be refused, got {other:?}"),
    }

    // The failed calls left the volume exactly where it was.
    assert_eq!(store.epoch(), 1, "refused calls leave the epoch alone");
    assert_eq!(
        store.get(id).unwrap(),
        bytes,
        "the refused delete unlinks nothing"
    );
    assert_eq!(
        store.fragment_epoch(id),
        Some(1),
        "the fragment is still recorded under epoch 1"
    );
}

/// A pass collects an unpinned fragment, then a reader arrives, and the pass
/// stops at its first refusal without deleting anything after it.
///
/// The ordering is the point. A pin is taken on the target's *current* epoch,
/// so it cannot be taken before the fragment the reader will fetch is written.
/// A collector therefore collects what is collectable while no pin is live, and
/// from the moment a pin lands every remaining delete is refused - including
/// fragments older than the pinned epoch. An earlier version of this drill
/// pinned first and then expected the epoch-0 fragment to be collectable; that
/// expectation was the hole, because the reader pinning epoch 1 may fetch a
/// fragment written at epoch 0.
#[test]
fn drill_three_a_pass_stops_at_the_first_refusal_and_deletes_nothing_after_it() {
    let fs = SimFs::new(0xE3_0003);
    let dir = "epoch_fence_drill_three";
    let store = VolumeStore::open(fs.clone(), dir, VolumeConfig::default()).unwrap();

    let a = BlobId::new(43, 1, 1, 1, 2, 0);
    let b = BlobId::new(43, 1, 1, 2, 2, 0);
    let c = BlobId::new(43, 1, 1, 3, 2, 0);
    let a_bytes = b"drill three: fragment A, collected before the pin";
    let b_bytes = b"drill three: fragment B, older than the pin";
    let c_bytes = b"drill three: fragment C, written at the pinned epoch";

    // A and B belong to epoch 0; C is written after the advance, at epoch 1.
    store.put(a, a_bytes).unwrap();
    store.put(b, b_bytes).unwrap();
    assert_eq!(store.advance_epoch(0).unwrap(), 1);
    store.put(c, c_bytes).unwrap();

    // Equality is not "older": a collector at C's own epoch may not collect C.
    // This is the boundary the `>=` comparison in the Collectable arm enforces,
    // and it holds whether or not a pin exists.
    match store.delete_collectable(c, 1) {
        Err(BlobError::FragmentNotOlder {
            id: named,
            fragment_epoch,
            collector_epoch,
        }) => {
            assert_eq!(named, c);
            assert_eq!(fragment_epoch, 1);
            assert_eq!(collector_epoch, 1);
        }
        other => panic!("an equal-epoch fragment is not collectable, got {other:?}"),
    }

    // No pin is live yet, so the pass collects A. This is the only delete the
    // pass will make; everything after this point has to be refused.
    assert!(
        store.delete_collectable(a, 2).unwrap(),
        "A is older than the fence and no pin is live"
    );
    assert!(matches!(store.get(a), Err(BlobError::NotFound(id)) if id == a));

    // A reader arrives and pins the epoch the target is on now, 1. It cannot
    // pin epoch 0, where B was written - that is exactly why B still has to be
    // protected by this pin.
    let grant = store.pin(1, 60_000).unwrap();
    assert_eq!(grant.epoch, 1);
    assert!(matches!(
        store.pin(0, 60_000),
        Err(BlobError::PinEpochNotCurrent {
            requested_epoch: 0,
            actual_epoch: 1,
        })
    ));

    // The pass reaches B first and is refused. B is an epoch-0 fragment and the
    // pin names epoch 1: the refusal is not "this pin names this fragment", it
    // is "a reader may still be holding a fragment on this slot".
    let refuse_b = || match store.delete_collectable(b, 2) {
        Err(BlobError::FragmentPinned {
            id: named,
            fragment_epoch,
            collector_epoch,
            live_pins,
        }) => {
            assert_eq!(named, b);
            assert_eq!(fragment_epoch, 0);
            assert_eq!(collector_epoch, 2);
            assert_eq!(live_pins, 1);
        }
        other => panic!("a live pin must refuse the older fragment B, got {other:?}"),
    };
    refuse_b();

    // A real pass stops at the first refusal: it does not retry and does not
    // continue to C. C is a dead epoch-1 fragment, but this caller leaves it
    // intact because the pass refused at B.
    assert_eq!(
        store.get(c).unwrap(),
        c_bytes,
        "C was never attempted and is still intact"
    );
    assert_eq!(store.fragment_epoch(c), Some(1));

    // Continuing would not have helped: the same pin refuses C too. This is the
    // pair that makes one pin a fence over every fragment on the slot.
    assert!(matches!(
        store.delete_collectable(c, 2),
        Err(BlobError::FragmentPinned {
            id: named,
            fragment_epoch: 1,
            collector_epoch: 2,
            live_pins: 1,
        }) if named == c
    ));

    // The refusal is not retryable in shape: the identical call fails
    // identically while the pin is live.
    refuse_b();
    assert_eq!(store.get(b).unwrap(), b_bytes);
    assert_eq!(
        store.get(c).unwrap(),
        c_bytes,
        "C is still readable while its reader holds the pin"
    );

    // Releasing the pin is what makes the rest collectable - the pass resumes
    // only because the reader finished, never because it retried through the
    // refusal.
    assert!(store.unpin(grant.pin_id).unwrap());
    assert!(store.delete_collectable(b, 2).unwrap());
    assert!(store.delete_collectable(c, 2).unwrap());
    assert!(matches!(store.get(b), Err(BlobError::NotFound(id)) if id == b));
    assert!(matches!(store.get(c), Err(BlobError::NotFound(id)) if id == c));
}

/// A collected fragment's tombstone survives sealing, compaction and a
/// power-cut reopen, and a late write of the same bytes is refused.
#[test]
fn drill_four_collection_survives_compaction_and_reopen() {
    let fs = SimFs::new(0xE4_0004);
    let dir = "epoch_fence_drill_four";
    let store = VolumeStore::open(fs.clone(), dir, VolumeConfig::default()).unwrap();

    let id = BlobId::new(44, 1, 1, 1, 2, 0);
    let bytes = b"drill four: collected bytes that must not come back";

    store.put(id, bytes).unwrap();
    assert_eq!(store.epoch(), 0);
    assert_eq!(store.advance_epoch(0).unwrap(), 1);
    assert!(store.delete_collectable(id, 1).unwrap());
    assert_eq!(
        store.fragment_epoch(id),
        None,
        "a collected fragment has no epoch"
    );

    // Seal and compact so the tombstone is rewritten into a replacement
    // segment rather than left in the active one.
    assert!(
        store.seal_and_compact().unwrap() > 0,
        "the collected fragment's bytes must be reclaimed"
    );

    drop(store);
    fs.simulate_power_cut();

    let reopened = VolumeStore::open(fs.clone(), dir, VolumeConfig::default()).unwrap();
    assert_eq!(
        reopened.epoch(),
        1,
        "the epoch survives compaction and reopen"
    );
    assert!(
        matches!(reopened.get(id), Err(BlobError::NotFound(missing)) if missing == id),
        "the tombstone survives compaction and reopen"
    );
    assert!(
        reopened.live_pins().unwrap().is_empty(),
        "recovery invented no pin"
    );
    assert_eq!(reopened.fragment_epoch(id), None);

    // No resurrection: the tombstone is consulted before the idempotent-write
    // check, so even the exact same bytes are refused.
    match reopened.put(id, bytes) {
        Err(BlobError::Tombstoned(named)) => assert_eq!(named, id),
        other => panic!("a late put must not resurrect a collected fragment, got {other:?}"),
    }
    assert_eq!(reopened.fragment_epoch(id), None);
}
