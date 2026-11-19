//! Storage-level drills for the GC epoch/pin/delete fence.
//!
//! These are precursor drills, not the cluster drills. They drive
//! [`VolumeStore`]'s public epoch, pin and delete primitives directly on a
//! deterministic [`SimFs`], and prove only storage-side invariants:
//!
//! * a live reader pin blocks an epoch advance and a collectable delete of the
//!   fragment that pin protects;
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

/// A pass deletes one collectable fragment, hits a pinned fragment, and stops.
/// The remaining dead fragment is untouched, and the same refusal repeats if
/// the caller retries the identical call.
#[test]
fn drill_three_a_pass_stops_at_the_first_refusal_and_deletes_nothing_after_it() {
    let fs = SimFs::new(0xE3_0003);
    let dir = "epoch_fence_drill_three";
    let store = VolumeStore::open(fs.clone(), dir, VolumeConfig::default()).unwrap();

    let a = BlobId::new(43, 1, 1, 1, 2, 0);
    let b = BlobId::new(43, 1, 1, 2, 2, 0);
    let c = BlobId::new(43, 1, 1, 3, 2, 0);
    let a_bytes = b"drill three: fragment A, collectable";
    let b_bytes = b"drill three: fragment B, never attempted";
    let c_bytes = b"drill three: fragment C, held by a reader";

    // A and B belong to epoch 0; C is written after the advance, at epoch 1.
    store.put(a, a_bytes).unwrap();
    store.put(b, b_bytes).unwrap();
    assert_eq!(store.advance_epoch(0).unwrap(), 1);
    store.put(c, c_bytes).unwrap();
    let grant = store.pin(1, 60_000).unwrap();
    assert_eq!(grant.epoch, 1);

    // Equality is not "older": a collector at C's own epoch may not collect C.
    // This is the boundary the `>=` comparison in the Collectable arm enforces.
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

    // The pass is fenced at the epoch it tried to advance to, 2. That advance
    // was refused by the pin on epoch 1 (the refusal drill one asserts), and
    // the same pin must refuse the collectable delete of C. A is older than the
    // fence and unpinned, so the pass collects it first.
    assert!(
        store.delete_collectable(a, 2).unwrap(),
        "A is older than the fence and unpinned"
    );

    let refuse_c = || match store.delete_collectable(c, 2) {
        Err(BlobError::FragmentPinned {
            id: named,
            fragment_epoch,
            collector_epoch,
            live_pins,
        }) => {
            assert_eq!(named, c);
            assert_eq!(fragment_epoch, 1);
            assert_eq!(collector_epoch, 2);
            assert_eq!(live_pins, 1);
        }
        other => panic!("the pinned epoch must refuse C, got {other:?}"),
    };
    refuse_c();

    // A real pass stops at the first refusal: it does not retry and does not
    // continue to B. B is a dead epoch-0 fragment, but this caller leaves it
    // intact because the pass refused at C. The primitive's repeated refusal
    // is what makes "stop, do not retry" the only safe rule.
    assert_eq!(
        store.get(b).unwrap(),
        b_bytes,
        "B was never attempted and is still intact"
    );
    assert_eq!(store.fragment_epoch(b), Some(0));

    // The refusal is not retryable in shape: the identical call fails
    // identically while the pin is live.
    refuse_c();
    assert_eq!(store.get(b).unwrap(), b_bytes);
    assert_eq!(
        store.get(c).unwrap(),
        c_bytes,
        "C is still readable while its reader holds the pin"
    );
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
