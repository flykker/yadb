//! Block-side proofs for the orphan-fragment contract of `write_at_with`.
//!
//! A block write appends immutable fragments *before* the guarded publication
//! that names them. A refused, conflicted or crashed write therefore leaves
//! fragments no committed extent map references. The daemon's mark-and-sweep
//! pass (`yadbd::gc`) is what reclaims them, and it lives in a crate that
//! depends on this one, so these tests model its decision exactly:
//!
//! * the live set is the `/block/` mark, i.e. the ids a committed
//!   [`VolumeExtentMap`] names;
//! * every id a mounted slot holds and the mark does not name is dead;
//! * the pass advances every slot's epoch and then deletes each dead id through
//!   `PoolSlot::delete_collectable`, exactly as `yadbd::gc::sweep_pool` does.
//!
//! The model uses exact tombstones except in the one test that is specifically
//! about the generation barrier the real pass would write when the dead range is
//! disjoint from every live generation. An exact tombstone is narrower than a
//! barrier, so using it elsewhere can only delete less, never a live fragment.
//!
//! What these tests do **not** prove: a real process crash (the crash case
//! leaves a durable writer lease behind, power-cuts the simulated filesystem and
//! deletes the expired lease record the way the collector's reap would), a real
//! multi-node pool (every slot is a local `VolumeStore` on `SimFs`), or the
//! daemon's scheduling of the pass.
//!
//! The load-bearing tests assert both halves: the garbage is gone *and* the live
//! object still reads back its exact bytes. A collector that deletes everything
//! fails the second half.

use std::collections::BTreeSet;
use std::sync::Arc;

use yadb_blob::{ErasureScheme, VolumeConfig, VolumeStore};
use yadb_io::SimFs;

use super::*;

const BS: u32 = 4096;

fn make_pool(fs: &SimFs) -> Arc<PoolStriper<SimFs>> {
    let domains = (0..3)
        .map(|i| {
            VolumeStore::open(fs.clone(), format!("gc-d{i}"), VolumeConfig::default()).unwrap()
        })
        .collect();
    Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap())
}

fn make_engine(
    meta: &LocalMetaKv<SimFs>,
    pool: &Arc<PoolStriper<SimFs>>,
) -> BlockVolumeEngine<SimFs> {
    let engine = BlockVolumeEngine::new(meta.clone(), BS);
    engine.register_pool("data", pool.clone());
    engine
}

/// Every logical id a mounted slot of this pool holds. Physical parts collapse
/// to the logical id, which is the unit the mark set uses.
fn present(pool: &PoolStriper<SimFs>) -> BTreeSet<BlobId> {
    let mut ids = BTreeSet::new();
    for slot in pool.slots() {
        for id in slot.local_store().unwrap().live_ids() {
            ids.insert(id.with_part(0));
        }
    }
    ids
}

/// Whether any slot still holds a part of `id`.
fn fragment_present(pool: &PoolStriper<SimFs>, id: BlobId) -> bool {
    pool.slots().iter().any(|slot| {
        slot.local_store()
            .unwrap()
            .live_ids()
            .iter()
            .any(|present| present.with_part(0) == id)
    })
}

/// The live set of one volume: the ids its committed extent map names.
fn map_live(engine: &BlockVolumeEngine<SimFs>, volume: &str) -> BTreeSet<BlobId> {
    engine
        .load_map(1, volume)
        .unwrap()
        .blocks
        .values()
        .copied()
        .collect()
}

/// One collection pass over a pool, using the real collector's decision:
/// `present - live` is dead, every slot's epoch is advanced, and each dead id is
/// deleted through the target's collectable delete. Returns the number of
/// physical fragments removed.
fn collect(pool: &PoolStriper<SimFs>, live: &BTreeSet<BlobId>) -> usize {
    let dead: Vec<BlobId> = present(pool).difference(live).copied().collect();
    if dead.is_empty() {
        return 0;
    }
    let epochs: Vec<u64> = pool
        .slots()
        .iter()
        .map(|slot| {
            let epoch = slot.epoch().expect("a test slot's epoch is readable");
            slot.advance_epoch(epoch)
                .expect("a test slot's epoch advances")
        })
        .collect();
    let mut deleted = 0;
    for id in dead {
        for part in 1..=pool.scheme().total_parts() {
            let part_id = id.with_part(part as u8);
            for (slot, epoch) in pool.slots().iter().zip(&epochs) {
                if slot
                    .delete_collectable(part_id, *epoch)
                    .expect("a collectable delete must not be refused")
                {
                    deleted += 1;
                }
            }
        }
    }
    deleted
}

/// A guard that can never be satisfied: the record is absent, so revision 1
/// never matches. Every one of the engine's 32 attempts stages its fragments and
/// then loses the same CAS, which is the "refused write" state.
fn never_satisfiable(record: &str) -> MapPublication {
    MapPublication {
        guards: vec![RevisionGuard {
            key: record.to_string(),
            expected_mod_revision: 1,
        }],
        ops: vec![TxnOp::Put {
            key: record.to_string(),
            value: b"done".to_vec(),
        }],
    }
}

// ---------------------------------------------------------------------------
// Case: the publication transaction is refused (an unsatisfiable caller guard)
// ---------------------------------------------------------------------------

#[test]
fn refused_write_stages_garbage_a_pass_reclaims_without_touching_the_live_block() {
    let fs = SimFs::new(9101);
    let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
    let pool = make_pool(&fs);
    let engine = make_engine(&meta, &pool);

    engine
        .write_at(1, "disk", "data", 8192, 0, b"committed-before")
        .unwrap();
    let live = engine.load_map(1, "disk").unwrap().blocks[&0];
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 16).unwrap(),
        b"committed-before"
    );

    // Every attempt's `prepare` sees the map it would publish, so the id under
    // block 0 is that attempt's staged fragment.
    let mut staged = BTreeSet::new();
    let result = engine.write_at_with(1, "disk", "data", 8192, 0, b"never-published", |map| {
        staged.insert(map.blocks[&0]);
        Ok(never_satisfiable("/block-write/1/disk/never"))
    });
    assert!(
        matches!(result, Err(BlockError::Conflict)),
        "an unsatisfiable caller guard must end in a definite non-commit: {result:?}"
    );
    assert_eq!(
        staged.len(),
        32,
        "the engine must retry to its bound, one fresh allocation per attempt"
    );
    assert!(
        staged.iter().all(|id| *id != live),
        "a refused attempt must never reuse the live id"
    );
    assert_eq!(
        engine.load_map(1, "disk").unwrap().blocks[&0],
        live,
        "the refused publication changed the map"
    );
    // A normal block read consults only the map, so the staged bytes are not
    // returned even though they exist on the pool.
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 16).unwrap(),
        b"committed-before",
        "a normal read returned bytes from an unreferenced fragment"
    );

    // The documented residual: the garbage is still readable by its physical
    // id until a pass reclaims it. This is what a pass must remove.
    for id in &staged {
        let bytes = pool
            .get_blob(*id)
            .expect("a fully written refused fragment is readable by id");
        assert_eq!(&bytes[..15], b"never-published");
        assert!(!bytes.starts_with(b"committed-before"));
    }

    let deleted = collect(&pool, &map_live(&engine, "disk"));
    assert!(
        deleted >= staged.len() * pool.scheme().total_parts(),
        "the pass deleted {deleted} fragments, fewer than the {staged:?} refused ids"
    );
    for id in &staged {
        assert!(
            !fragment_present(&pool, *id),
            "a pass must reclaim the refused fragment {id}"
        );
        assert!(
            pool.get_blob(*id).is_err(),
            "the reclaimed fragment {id} is still readable by id"
        );
    }
    // The live half: the exact bytes are still there, from the id the map names.
    assert!(fragment_present(&pool, live));
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 16).unwrap(),
        b"committed-before",
        "the pass deleted or corrupted the live block"
    );

    // Repeating the pass finds nothing dead, so it deletes nothing.
    assert_eq!(
        collect(&pool, &map_live(&engine, "disk")),
        0,
        "a repeated pass deleted a live fragment"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 16).unwrap(),
        b"committed-before",
        "a repeated pass changed the live block"
    );

    // The pass's tombstones are exact: they name the refused ids, not the
    // volume or the block index. A later write must still publish and read back.
    engine
        .write_at(1, "disk", "data", 8192, 0, b"after-pass")
        .unwrap();
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 10).unwrap(),
        b"after-pass",
        "a pass poisoned a later write of the same block"
    );
}

// ---------------------------------------------------------------------------
// Case: the publication transaction conflicts (a concurrent map revision)
// ---------------------------------------------------------------------------

#[test]
fn conflicted_attempt_is_garbage_and_only_the_published_attempt_stays_live() {
    let fs = SimFs::new(9102);
    let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
    let pool = make_pool(&fs);
    let engine = make_engine(&meta, &pool);
    let map_key = BlockVolumeEngine::<SimFs>::meta_key(1, "disk");

    engine
        .write_at(1, "disk", "data", 8192, 0, b"old-generation")
        .unwrap();
    let old = engine.load_map(1, "disk").unwrap().blocks[&0];

    let mut first_attempt = None;
    let mut attempts = 0u32;
    engine
        .write_at_with(1, "disk", "data", 8192, 0, b"new-generation", |map| {
            attempts += 1;
            if attempts == 1 {
                first_attempt = Some(map.blocks[&0]);
                // Bump the map key between the engine's read and its CAS. The
                // attempt's guard then fails as a definite non-commit, exactly
                // like a concurrent writer that committed first.
                let current = meta.get_or_panic(&map_key).unwrap();
                meta.execute(KvCommand::Put {
                    key: map_key.clone(),
                    value: current.value,
                })
                .unwrap();
            }
            Ok(MapPublication::default())
        })
        .unwrap();
    assert_eq!(attempts, 2, "the first attempt must lose its CAS and retry");

    let first = first_attempt.expect("the first attempt staged a fragment");
    let published = engine.load_map(1, "disk").unwrap().blocks[&0];
    assert_ne!(first, old);
    assert_ne!(
        first, published,
        "the conflicted attempt's fragment became the live one"
    );
    // Both attempts write the same payload, so content alone cannot tell which
    // fragment a read used. The map names the retry's id, and that is the only
    // id a read can consult.
    assert_eq!(
        engine.load_map(1, "disk").unwrap().blocks[&0],
        published,
        "the map does not name the retry's fragment"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 14).unwrap(),
        b"new-generation"
    );
    // The conflicted attempt's bytes exist, but no read can reach them.
    assert_eq!(&pool.get_blob(first).unwrap()[..14], b"new-generation");
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 14).unwrap(),
        b"new-generation"
    );

    let deleted = collect(&pool, &map_live(&engine, "disk"));
    assert!(deleted > 0);
    assert!(
        !fragment_present(&pool, first),
        "the conflicted attempt's fragment survived the pass"
    );
    assert!(
        !fragment_present(&pool, old),
        "the superseded generation survived the pass"
    );
    assert!(
        fragment_present(&pool, published),
        "the pass deleted the published fragment"
    );
    assert!(
        pool.get_blob(first).is_err(),
        "the reclaimed conflicted fragment is still readable by id"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 14).unwrap(),
        b"new-generation",
        "the pass lost the live generation"
    );

    // A second pass has nothing dead left, so it deletes nothing and the live
    // object is unchanged. The pass is idempotent on this state.
    let again = collect(&pool, &map_live(&engine, "disk"));
    assert_eq!(again, 0, "a second pass deleted {again} fragments");
    assert!(
        fragment_present(&pool, published),
        "a second pass deleted the published fragment"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 14).unwrap(),
        b"new-generation",
        "a second pass changed the live generation"
    );
}

// ---------------------------------------------------------------------------
// Case: the process dies between staging and publication
// ---------------------------------------------------------------------------

#[test]
fn crash_between_staging_and_publication_leaves_only_garbage() {
    let fs = SimFs::new(9103);
    let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
    let pool = make_pool(&fs);
    let engine = make_engine(&meta, &pool);

    engine
        .write_at(1, "disk", "data", 8192, 0, b"survivor")
        .unwrap();
    let live = engine.load_map(1, "disk").unwrap().blocks[&0];

    // Stage exactly what a write does after its allocation and lease and before
    // its publication, then "die": forget the lease so its durable record stays
    // behind, as a killed process leaves it.
    let allocation = engine.reserve_allocation(1).unwrap();
    let staged = BlobId::new(
        1,
        (allocation >> 24) as u32,
        (allocation & BlobId::MAX_U24 as u64) as u32,
        1,
        BLOCK_CHANNEL,
        0,
    );
    let lease_key = {
        let lease = yadb_metakv::gc::acquire_writer(
            &meta,
            1,
            meta.revision_or_panic(),
            yadb_metakv::gc::WRITER_TTL_MS,
        )
        .unwrap();
        let key = lease.key().to_string();
        pool.put_blob(staged, &vec![b'x'; BS as usize]).unwrap();
        std::mem::forget(lease);
        key
    };
    assert_ne!(staged, live);
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 8).unwrap(),
        b"survivor",
        "a staged-but-unpublished fragment was returned by a normal read"
    );

    drop(engine);
    drop(pool);
    drop(meta);
    fs.simulate_power_cut();

    let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
    let pool = make_pool(&fs);
    let engine = make_engine(&meta, &pool);
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 8).unwrap(),
        b"survivor",
        "recovery lost the committed block"
    );
    assert!(
        fragment_present(&pool, staged),
        "the crash case did not leave the staged fragment behind"
    );
    assert!(
        fragment_present(&pool, live),
        "recovery lost the committed block's fragment"
    );

    // The collector refuses while the crashed writer's lease is live; after its
    // TTL the collector reaps it. Deleting the record is the post-reap state.
    meta.execute(KvCommand::Delete { key: lease_key }).unwrap();
    let deleted = collect(&pool, &map_live(&engine, "disk"));
    assert!(deleted > 0);
    assert!(
        !fragment_present(&pool, staged),
        "the crashed write's fragment survived the pass"
    );
    assert!(
        fragment_present(&pool, live),
        "the pass deleted the committed block"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 8).unwrap(),
        b"survivor",
        "the pass lost the committed block"
    );
}

// ---------------------------------------------------------------------------
// Case: a newer write replaces the same block
// ---------------------------------------------------------------------------

#[test]
fn replaced_write_is_garbage_and_the_new_generation_reads_back() {
    let fs = SimFs::new(9104);
    let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
    let pool = make_pool(&fs);
    let engine = make_engine(&meta, &pool);

    engine
        .write_at(1, "disk", "data", 8192, 0, b"first-value")
        .unwrap();
    let old = engine.load_map(1, "disk").unwrap().blocks[&0];
    engine
        .write_at(1, "disk", "data", 8192, 0, b"second-value")
        .unwrap();
    let new = engine.load_map(1, "disk").unwrap().blocks[&0];
    assert_ne!(old, new, "a replacement reused the old fragment id");
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 12).unwrap(),
        b"second-value"
    );
    // The old generation is unreferenced but still on the pool.
    assert!(fragment_present(&pool, old));
    assert_eq!(&pool.get_blob(old).unwrap()[..11], b"first-value");

    let deleted = collect(&pool, &map_live(&engine, "disk"));
    assert!(deleted > 0);
    assert!(
        !fragment_present(&pool, old),
        "the replaced generation survived the pass"
    );
    assert!(
        fragment_present(&pool, new),
        "the pass deleted the replacement"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 12).unwrap(),
        b"second-value",
        "the pass lost the replacement's bytes"
    );
}

// ---------------------------------------------------------------------------
// Case: the block is deleted (unmapped) before the pass
// ---------------------------------------------------------------------------

#[test]
fn unmapped_block_is_garbage_and_reads_as_thin_provisioned_zeros() {
    let fs = SimFs::new(9105);
    let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
    let pool = make_pool(&fs);
    let engine = make_engine(&meta, &pool);

    engine
        .write_at(1, "disk", "data", 8192, 0, b"to-be-unmapped")
        .unwrap();
    let id = engine.load_map(1, "disk").unwrap().blocks[&0];
    engine.unmap_at(1, "disk", 8192, 0, BS as u64).unwrap();
    assert!(!engine.load_map(1, "disk").unwrap().blocks.contains_key(&0));
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 14).unwrap(),
        vec![0u8; 14],
        "an unmapped block must read as zeros"
    );
    assert!(fragment_present(&pool, id));

    let deleted = collect(&pool, &map_live(&engine, "disk"));
    assert!(deleted > 0);
    assert!(
        !fragment_present(&pool, id),
        "the unmapped block's fragment survived the pass"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 14).unwrap(),
        vec![0u8; 14],
        "the pass changed what an unmapped block reads as"
    );
}

// ---------------------------------------------------------------------------
// The dangerous interaction: a pass between a refused write and a live write
// ---------------------------------------------------------------------------

/// An empty volume is the case where the real pass takes the barrier path: the
/// dead set has no live generation beside it, so `dead_max < min_live` holds
/// trivially and one durable barrier fences every generation at or below the
/// refused attempt. The next write must reserve a *higher* generation, or the
/// barrier would refuse it (or the pass would have deleted it).
#[test]
fn a_barrier_between_a_refused_and_a_live_write_keeps_the_new_generation() {
    let fs = SimFs::new(9106);
    let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
    let pool = make_pool(&fs);
    let engine = make_engine(&meta, &pool);

    let mut staged = BTreeSet::new();
    let result = engine.write_at_with(1, "disk", "data", 8192, 0, b"refused", |map| {
        staged.insert(map.blocks[&0]);
        Ok(never_satisfiable("/block-write/1/disk/barrier"))
    });
    assert!(matches!(result, Err(BlockError::Conflict)), "{result:?}");
    assert_eq!(staged.len(), 32);
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 7).unwrap(),
        vec![0u8; 7],
        "a refused write on an empty volume must not become readable"
    );

    // What the real pass writes on an empty volume: one barrier up to the
    // highest dead generation of that (project, owner, channel) group.
    let owner = staged.iter().next().unwrap().owner_id();
    assert!(
        staged.iter().all(|id| id.owner_id() == owner),
        "32 attempts must stay in one owner for this barrier shape"
    );
    let up_to = staged
        .iter()
        .map(|id| id.generation())
        .max()
        .expect("attempts staged fragments");
    for slot in pool.slots() {
        let response = slot
            .apply_barrier(1, owner, BLOCK_CHANNEL, up_to, false, u64::MAX)
            .expect("a live collector barrier is accepted");
        assert!(response.evicted > 0, "the barrier evicted nothing");
    }
    for id in &staged {
        assert!(
            !fragment_present(&pool, *id),
            "the barrier left the refused fragment {id}"
        );
    }

    // The live write after the pass. Its allocation is above the barrier, so it
    // must not be refused and must not be swept by a later pass.
    engine
        .write_at(1, "disk", "data", 8192, 0, b"live-after-barrier")
        .unwrap();
    let live = engine.load_map(1, "disk").unwrap().blocks[&0];
    assert!(
        (live.owner_id(), live.generation()) > (owner, up_to),
        "the new write reused a generation the barrier already fenced: {live}"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 18).unwrap(),
        b"live-after-barrier"
    );

    // A later pass must keep the new generation: it is the only live id.
    collect(&pool, &map_live(&engine, "disk"));
    assert!(
        fragment_present(&pool, live),
        "a pass after the live write deleted the new fragment"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 18).unwrap(),
        b"live-after-barrier",
        "the pass lost the live write's bytes"
    );
}

/// The same interaction where the volume already has live data: the refused
/// generation is *above* the live one, so the real pass cannot use a barrier
/// (`dead_max < min_live` is false) and tombstones the refused id exactly. The
/// tombstone names that id, not the block or the volume, so the next write's
/// fragment is unaffected.
#[test]
fn an_exact_tombstone_between_a_refused_and_a_live_write_keeps_the_new_generation() {
    let fs = SimFs::new(9107);
    let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
    let pool = make_pool(&fs);
    let engine = make_engine(&meta, &pool);

    engine
        .write_at(1, "disk", "data", 8192, 0, b"old-live")
        .unwrap();
    let old = engine.load_map(1, "disk").unwrap().blocks[&0];

    let mut staged = BTreeSet::new();
    let result = engine.write_at_with(1, "disk", "data", 8192, 0, b"refused", |map| {
        staged.insert(map.blocks[&0]);
        Ok(never_satisfiable("/block-write/1/disk/exact"))
    });
    assert!(matches!(result, Err(BlockError::Conflict)), "{result:?}");
    let refused = staged.iter().next().copied().unwrap();
    assert!(
        (refused.owner_id(), refused.generation()) > (old.owner_id(), old.generation()),
        "the refused attempt must be newer than the live generation"
    );

    // A pass with the old data live. `dead_max > min_live`, so the real pass
    // uses exact tombstones; the model does the same.
    collect(&pool, &map_live(&engine, "disk"));
    for id in &staged {
        assert!(
            !fragment_present(&pool, *id),
            "the pass left the refused fragment {id}"
        );
    }
    assert!(
        fragment_present(&pool, old),
        "the pass deleted the old live block"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 8).unwrap(),
        b"old-live"
    );

    // The new write is newer than every tombstoned id.
    engine
        .write_at(1, "disk", "data", 8192, 0, b"new-live")
        .unwrap();
    let new = engine.load_map(1, "disk").unwrap().blocks[&0];
    assert!((new.owner_id(), new.generation()) > (refused.owner_id(), refused.generation()));
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 8).unwrap(),
        b"new-live"
    );
    collect(&pool, &map_live(&engine, "disk"));
    assert!(
        fragment_present(&pool, new),
        "the pass after the new write deleted its fragment"
    );
    assert_eq!(
        engine.read_at(1, "disk", "data", 8192, 0, 8).unwrap(),
        b"new-live"
    );
}
