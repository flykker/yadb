pub mod erasure;
pub mod id;
pub mod pool;
pub mod volume;

pub use erasure::{ErasureCodec, ErasureScheme};
pub use id::BlobId;
pub use pool::{
    BarrierRequest, BarrierResponse, BlobIdList, CompactResponse, PoolHealthSnapshot,
    PoolHealthState, PoolSlot, PoolStriper, RepairReport, SlotHealthSnapshot,
};
pub use volume::{VolumeConfig, VolumeStats, VolumeStore};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobError {
    #[error("volume storage failed; restart and recover before further mutations")]
    StorageUnavailable,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("blob not found: {0}")]
    NotFound(BlobId),

    #[error("immutable blob conflict on {0}")]
    ImmutableConflict(BlobId),

    #[error("write to {id} rejected: generation fenced by GC barrier <= {barrier_gen}")]
    FencedByBarrier { id: BlobId, barrier_gen: u32 },

    #[error("write to deleted immutable blob {0} rejected")]
    Tombstoned(BlobId),

    #[error(
        "barrier {project_id}/{owner_id}/ch{channel}<={up_to_generation} refused: the collector \
         lease it was issued under expired at {expires_at_ms} ms and the target read the request \
         at {now_ms} ms, so the range is left intact rather than evicted for a pass that is no \
         longer entitled to delete"
    )]
    BarrierLeaseExpired {
        project_id: u32,
        owner_id: u32,
        channel: u8,
        up_to_generation: u32,
        expires_at_ms: u64,
        now_ms: u64,
    },

    #[error(
        "barrier {project_id}/{owner_id}/ch{channel}<={up_to_generation} refused: the target could \
         not read its own clock ({reason}), so it cannot tell whether the collector lease that \
         issued this barrier has expired; the range is left intact"
    )]
    BarrierClockUnreadable {
        project_id: u32,
        owner_id: u32,
        channel: u8,
        up_to_generation: u32,
        reason: String,
    },

    #[error("blob size {size} exceeds max {max}")]
    BlobTooLarge { size: usize, max: usize },

    #[error("slice [{offset}..{}] out of bounds for {id} (len {blob_len})", offset + len)]
    SliceOutOfBounds {
        id: BlobId,
        offset: usize,
        len: usize,
        blob_len: usize,
    },

    #[error("corrupt blob {id}: {reason}")]
    CorruptData { id: BlobId, reason: &'static str },

    #[error("invalid volume segment: {0}")]
    InvalidSegment(&'static str),

    #[error("invalid erasure configuration: {0}")]
    InvalidErasure(String),

    #[error("insufficient fragments to reconstruct blob: have {available}, need {required}")]
    InsufficientFragments { available: usize, required: usize },

    #[error(
        "write to {id} refused: {written} of {required} fragments landed, so the write would not be redundant"
    )]
    RedundancyNotMet {
        id: BlobId,
        written: usize,
        required: usize,
    },
}

impl BlobError {
    /// Whether this failure only says "this slot could not answer", so the same
    /// part may be placed on another slot.
    ///
    /// Conservative by construction: only `Io` qualifies. A remote slot maps a
    /// connection failure and an unexpected HTTP status to `Io`, and a local
    /// volume maps an I/O failure to `Io`; both mean the slot did not answer.
    ///
    /// Every other variant is a statement about the *data* or the slot's durable
    /// state and must never be bypassed by writing the part elsewhere:
    /// `ImmutableConflict` (the id already holds different bytes),
    /// `FencedByBarrier` and `Tombstoned` are fences that a handoff would
    /// silently defeat, and `StorageUnavailable` means the slot's outcome is
    /// unknown until it is recovered, so acknowledging the write somewhere else
    /// would hide a fault an operator has to see. If in doubt, this is `false`.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, BlobError::Io(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use yadb_io::{FaultConfig, SimFs, StorageFs, SyncFault, VirtualFile};

    #[test]
    fn process_only_reopen_must_sync_recovered_records_before_idempotent_ack() {
        for operation in ["put", "delete", "barrier"] {
            let fs = SimFs::new(1253);
            let cfg = VolumeConfig::default();
            let store = VolumeStore::open(fs.clone(), "reopen", cfg).unwrap();
            let id = BlobId::new(1, 1, 1, 1, 4, 0);
            if operation != "put" {
                store.put(id, b"payload").unwrap();
            }
            fs.set_faults(FaultConfig {
                sync_data_fault: SyncFault::BeforePersist,
                ..Default::default()
            });
            match operation {
                "put" => assert!(store.put(id, b"payload").is_err()),
                "delete" => assert!(store.delete(id).is_err()),
                _ => assert!(store.collect_garbage_barrier(1, 1, 4, 1, u64::MAX).is_err()),
            }
            drop(store);
            // Recovery must not expose the page-cache record while sync is
            // still failing, nor discard it as though failure meant rollback.
            assert!(VolumeStore::open(fs.clone(), "reopen", cfg).is_err());
            fs.set_faults(FaultConfig::default());
            // Process restart, not power loss: page cache still contains the
            // unconfirmed record. Recovered bytes alone are not a sync proof.
            let reopened = VolumeStore::open(fs.clone(), "reopen", cfg).unwrap();
            if operation == "put" {
                reopened.put(id, b"payload").unwrap(); // idempotent acknowledgement
            } else if operation == "delete" {
                assert!(!reopened.delete(id).unwrap()); // idempotent tombstone acknowledgement
            } else {
                assert!(matches!(
                    reopened.put(id, b"payload"),
                    Err(BlobError::FencedByBarrier { .. })
                ));
            }
            drop(reopened);
            fs.simulate_power_cut();
            let durable = VolumeStore::open(fs, "reopen", cfg).unwrap();
            if operation == "put" {
                assert_eq!(durable.get(id).unwrap(), b"payload");
            } else {
                assert!(!durable.contains(id));
                assert!(durable.put(id, b"payload").is_err());
            }
        }
    }

    #[test]
    fn failed_sync_blocks_mutations_and_preserves_unknown_outcome_until_recovery() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            for operation in ["put", "delete", "barrier"] {
                let fs = SimFs::new(1250);
                let cfg = VolumeConfig::default();
                let store = VolumeStore::open(fs.clone(), "sync", cfg).unwrap();
                let old = BlobId::new(1, 1, 1, 1, 4, 0);
                let unknown = BlobId::new(1, 1, 2, 1, 4, 0);
                store.put(old, b"old").unwrap();
                fs.set_faults(FaultConfig {
                    sync_data_fault: fault,
                    ..Default::default()
                });
                let result = match operation {
                    "put" => store.put(unknown, b"unknown"),
                    "delete" => store.delete(old).map(|_| ()),
                    _ => store
                        .collect_garbage_barrier(1, 1, 4, 1, u64::MAX)
                        .map(|_| ()),
                };
                assert!(matches!(result, Err(BlobError::Io(_))));
                fs.set_faults(FaultConfig::default());
                // Even an idempotent PUT/DELETE must not acknowledge from the
                // pre-error view or overwrite the ambiguous record's offset.
                assert!(matches!(
                    store.put(old, b"old"),
                    Err(BlobError::StorageUnavailable)
                ));
                assert!(matches!(
                    store.put(unknown, b"unknown"),
                    Err(BlobError::StorageUnavailable)
                ));
                assert!(matches!(
                    store.delete(old),
                    Err(BlobError::StorageUnavailable)
                ));
                assert!(matches!(
                    store.collect_garbage_barrier(1, 1, 4, 1, u64::MAX),
                    Err(BlobError::StorageUnavailable)
                ));
                assert!(matches!(
                    store.compact_sealed_segments(),
                    Err(BlobError::StorageUnavailable)
                ));
                drop(store);
                fs.simulate_power_cut();
                let recovered = VolumeStore::open(fs.clone(), "sync", cfg).unwrap();
                let persisted = fault == SyncFault::AfterPersist;
                if operation == "put" {
                    assert_eq!(recovered.get(old).unwrap(), b"old");
                    assert_eq!(recovered.contains(unknown), persisted);
                    if persisted {
                        assert_eq!(recovered.get(unknown).unwrap(), b"unknown");
                    }
                } else {
                    assert_eq!(recovered.contains(old), !persisted);
                    if persisted {
                        assert!(recovered.put(old, b"old").is_err());
                    } else {
                        recovered.put(old, b"old").unwrap();
                    }
                }
                let next = BlobId::new(1, 1, 3, 1, 4, 0);
                recovered.put(next, b"next").unwrap();
                drop(recovered);
                fs.simulate_power_cut();
                let again = VolumeStore::open(fs, "sync", cfg).unwrap();
                assert_eq!(again.get(next).unwrap(), b"next");
            }
        }
    }

    #[test]
    fn rollover_sync_failure_never_reinitializes_ambiguous_segment_in_process() {
        for directory in [false, true] {
            for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
                let fs = SimFs::new(1251);
                let cfg = VolumeConfig {
                    max_segment_bytes: 80,
                    ..Default::default()
                };
                let store = VolumeStore::open(fs.clone(), "roll", cfg).unwrap();
                let old = BlobId::new(1, 1, 1, 1, 4, 0);
                let next = BlobId::new(1, 1, 1, 2, 4, 0);
                store.put(old, &[b'o'; 30]).unwrap();
                fs.set_faults(if directory {
                    FaultConfig {
                        sync_dir_fault: fault,
                        ..Default::default()
                    }
                } else {
                    FaultConfig {
                        sync_data_fault: fault,
                        ..Default::default()
                    }
                });
                assert!(matches!(store.put(next, b"next"), Err(BlobError::Io(_))));
                fs.set_faults(FaultConfig::default());
                assert!(matches!(
                    store.put(next, b"next"),
                    Err(BlobError::StorageUnavailable)
                ));
                assert_eq!(store.get(old).unwrap(), vec![b'o'; 30]);
                drop(store);
                fs.simulate_power_cut();
                let recovered = VolumeStore::open(fs, "roll", cfg).unwrap();
                assert_eq!(recovered.get(old).unwrap(), vec![b'o'; 30]);
                assert!(!recovered.contains(next));
                recovered.put(next, b"next").unwrap();
            }
        }
    }

    #[test]
    fn compaction_sync_errors_keep_runtime_and_recovered_reads_consistent() {
        for directory in [false, true] {
            for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
                let fs = SimFs::new(1252);
                let cfg = VolumeConfig {
                    max_segment_bytes: 220,
                    ..Default::default()
                };
                let store = VolumeStore::open(fs.clone(), "compact_sync", cfg).unwrap();
                let dead = BlobId::new(1, 1, 1, 1, 4, 0);
                let live = BlobId::new(1, 1, 1, 2, 4, 0);
                let next = BlobId::new(1, 1, 1, 3, 4, 0);
                store.put(dead, &[b'd'; 60]).unwrap();
                store.put(live, &[b'l'; 60]).unwrap();
                store.put(next, &[b'n'; 60]).unwrap(); // seal first segment
                store.delete(dead).unwrap();
                fs.set_faults(if directory {
                    FaultConfig {
                        sync_dir_fault: fault,
                        ..Default::default()
                    }
                } else {
                    FaultConfig {
                        sync_data_fault: fault,
                        ..Default::default()
                    }
                });
                assert!(store.compact_sealed_segments().is_err());
                assert_eq!(store.get(live).unwrap(), vec![b'l'; 60]);
                assert_eq!(store.get_slice(live, 10, 20).unwrap(), vec![b'l'; 20]);
                assert!(!store.contains(dead));
                drop(store);
                fs.set_faults(FaultConfig::default());
                fs.simulate_power_cut();
                let recovered = VolumeStore::open(fs, "compact_sync", cfg).unwrap();
                assert_eq!(recovered.get(live).unwrap(), vec![b'l'; 60]);
                assert_eq!(recovered.get(next).unwrap(), vec![b'n'; 60]);
                assert!(matches!(
                    recovered.put(dead, &[b'd'; 60]),
                    Err(BlobError::Tombstoned(_))
                ));
                recovered.compact_sealed_segments().unwrap();
            }
        }
    }

    #[test]
    fn reed_solomon_recovers_any_two_lost_fragments() {
        let codec = ErasureCodec::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }).unwrap();
        let original = b"YADB distributed storage engine over standard filesystems without PDisk!";
        let shards = codec.encode(original);
        assert_eq!(shards.len(), 6);

        // Drop every possible pair (i, j) out of 6 shards (all 15 combinations: data+data, data+parity, parity+parity)
        for drop_a in 0..6 {
            for drop_b in (drop_a + 1)..6 {
                let mut partial: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
                partial[drop_a] = None;
                partial[drop_b] = None;
                let recovered = codec.decode(&partial).unwrap();
                assert_eq!(recovered.as_slice(), original);
            }
        }
    }

    #[test]
    fn volume_recovers_from_torn_writes_and_power_cuts() {
        let fs = SimFs::new(777);
        let cfg = VolumeConfig {
            max_segment_bytes: 4096,
            max_blob_bytes: 1024 * 1024,
            sync_on_put: true,
        };

        let store = VolumeStore::open(fs.clone(), "disk0", cfg).unwrap();
        let b1 = BlobId::new(1, 100, 1, 1, 0, 0);
        let b2 = BlobId::new(1, 100, 1, 2, 0, 0);
        store.put(b1, b"durable-alpha").unwrap();
        store.put(b2, b"durable-bravo").unwrap();

        // Inject deterministic 100% torn write on next put, then cut power!
        fs.set_faults(FaultConfig {
            seed: 99,
            torn_write_prob: 1.0,
            read_eio_prob: 0.0,
            write_eio_prob: 0.0,
            ..Default::default()
        });
        let b3 = BlobId::new(1, 100, 1, 3, 0, 0);
        assert!(store.put(b3, b"half-written-payload-during-crash").is_err());

        fs.set_faults(FaultConfig::default());
        fs.simulate_power_cut();

        // Reopen after power cut: b1 & b2 must be intact, b3 cleanly truncated, new b4 writable!
        let reopened = VolumeStore::open(fs, "disk0", cfg).unwrap();
        assert_eq!(reopened.get(b1).unwrap(), b"durable-alpha");
        assert_eq!(reopened.get(b2).unwrap(), b"durable-bravo");
        assert!(matches!(reopened.get(b3), Err(BlobError::NotFound(_))));
        assert_eq!(reopened.get_slice(b2, 8, 5).unwrap(), b"bravo");

        let b4 = BlobId::new(1, 100, 2, 1, 0, 0);
        reopened.put(b4, b"post-recovery-write").unwrap();
        assert_eq!(reopened.get(b4).unwrap(), b"post-recovery-write");
    }

    #[test]
    fn barrier_gc_and_compaction_reclaim_disk_space() {
        let fs = SimFs::new(1234);
        let cfg = VolumeConfig {
            max_segment_bytes: 512, // Force multiple segments
            max_blob_bytes: 1024 * 1024,
            sync_on_put: true,
        };
        let store = VolumeStore::open(fs, "disk_gc", cfg).unwrap();

        // Generation 1 blobs
        for step in 1..=6 {
            let id = BlobId::new(10, 50, 1, step, 2, 0);
            store.put(id, &[b'x'; 180]).unwrap();
        }
        // Generation 2 blob (must survive barrier <= 1)
        let live_id = BlobId::new(10, 50, 2, 1, 2, 0);
        store.put(live_id, &[b'y'; 180]).unwrap();

        let before = store.stats().unwrap();
        assert_eq!(before.live_blobs, 7);

        let evicted = store
            .collect_garbage_barrier(10, 50, 2, 1, u64::MAX)
            .unwrap();
        assert_eq!(evicted, 6);

        let reclaimed = store.compact_sealed_segments().unwrap();
        assert!(
            reclaimed > 0,
            "expected segment compaction to reclaim bytes"
        );
        assert_eq!(store.get(live_id).unwrap(), vec![b'y'; 180]);
    }

    /// The deadline check is a guard on deletion, so its failure directions matter
    /// more than its success direction: an unreadable clock must refuse, not
    /// default to zero (which would make every deadline look live and let the
    /// barrier evict), and an unreadable clock is a distinct refusal from an
    /// expired lease so an operator can tell "clock broken" from "pass too slow".
    #[test]
    fn an_unreadable_target_clock_refuses_a_barrier_instead_of_assuming_it_is_live() {
        let ids = (10, 50, 2, 1);
        // Production used to compute `map_or(0, ...)`, i.e. a broken clock read as
        // the epoch and every lease looked live. That is the direction that
        // deletes, so it is the one asserted against here.
        match volume::check_barrier_deadline(ids, u64::MAX, Err("clock stepped".into())) {
            Err(BlobError::BarrierClockUnreadable {
                project_id,
                owner_id,
                channel,
                up_to_generation,
                reason,
            }) => {
                assert_eq!(
                    (project_id, owner_id, channel, up_to_generation),
                    ids,
                    "the refusal must name the range it left intact"
                );
                assert!(reason.contains("clock stepped"), "{reason}");
            }
            other => panic!("an unreadable clock must refuse, got {other:?}"),
        }

        // The same guard still admits a barrier whose deadline is provably ahead,
        // so the refusal above is the clock and not a blanket rejection.
        assert!(volume::check_barrier_deadline(ids, 1000, Ok(999)).is_ok());
        // Boundary: the deadline is exclusive. At the deadline the lease has
        // lapsed, and that is the case that must refuse.
        assert!(matches!(
            volume::check_barrier_deadline(ids, 1000, Ok(1000)),
            Err(BlobError::BarrierLeaseExpired { .. })
        ));
    }

    #[test]
    fn a_barrier_whose_lease_expired_evicts_nothing_and_does_not_fence() {
        // The barrier is the one deletion a per-tombstone fence interval cannot
        // bound: one durable record evicts a whole generation range. It therefore
        // carries the deadline of the collector lease that issued it, and a target
        // that receives it after that deadline refuses. This is the check that
        // makes the refusal real rather than advisory - if it were missing, the
        // eviction below would still happen.
        let fs = SimFs::new(1236);
        let cfg = VolumeConfig {
            max_segment_bytes: 512,
            max_blob_bytes: 1024 * 1024,
            sync_on_put: true,
        };
        let store = VolumeStore::open(fs, "disk_expired_barrier", cfg).unwrap();
        let mut ids = Vec::new();
        for step in 1..=4 {
            let id = BlobId::new(10, 50, 1, step, 2, 0);
            store.put(id, &[b'x'; 180]).unwrap();
            ids.push(id);
        }
        let before = store.stats().unwrap();
        assert_eq!(before.live_blobs, 4);

        // A deadline in the past: the pass that issued this barrier is no longer
        // entitled to delete, so every fragment must still be readable.
        let error = store
            .collect_garbage_barrier(10, 50, 2, 1, 1)
            .expect_err("an expired barrier must be refused");
        match &error {
            BlobError::BarrierLeaseExpired {
                project_id,
                owner_id,
                channel,
                up_to_generation,
                expires_at_ms,
                now_ms,
            } => {
                assert_eq!(
                    (*project_id, *owner_id, *channel, *up_to_generation),
                    (10, 50, 2, 1)
                );
                assert_eq!(*expires_at_ms, 1);
                assert!(*now_ms >= 1);
            }
            other => panic!("expected BarrierLeaseExpired, got {other:?}"),
        }
        assert_eq!(store.stats().unwrap().live_blobs, 4);
        for id in &ids {
            assert_eq!(store.get(*id).unwrap(), vec![b'x'; 180]);
        }
        // The refusal must also not have left the durable barrier behind: a
        // barrier record written before the check would fence later writes even
        // though nothing was evicted, which is a different, permanent effect.
        // The probe is a generation-1 write, the range the refused barrier named,
        // so a durable record would refuse it with `FencedByBarrier`.
        let extra = BlobId::new(10, 50, 1, 5, 2, 0);
        store.put(extra, b"still writable").unwrap();
        assert_eq!(store.get(extra).unwrap(), b"still writable");

        // The same range with a live deadline is evicted, so the refusal above is
        // the deadline and not a broken barrier path. Five, not four: the probe
        // above is in the same generation and is evicted too.
        let evicted = store
            .collect_garbage_barrier(10, 50, 2, 1, u64::MAX)
            .unwrap();
        assert_eq!(evicted, 5);
        assert!(matches!(store.get(ids[0]), Err(BlobError::NotFound(_))));
        assert!(matches!(store.get(extra), Err(BlobError::NotFound(_))));
    }

    #[test]
    fn compaction_preserves_gc_fencing_after_restart() {
        let fs = SimFs::new(1235);
        let cfg = VolumeConfig {
            max_segment_bytes: 256,
            max_blob_bytes: 1024,
            sync_on_put: true,
        };
        let old = BlobId::new(10, 50, 1, 1, 2, 0);
        let live = BlobId::new(10, 50, 2, 1, 2, 0);
        let store = VolumeStore::open(fs.clone(), "gc_restart", cfg).unwrap();
        store.put(old, &[b'x'; 100]).unwrap();
        store
            .collect_garbage_barrier(10, 50, 2, 1, u64::MAX)
            .unwrap();
        // Seal the segment containing the barrier, then compact it away.
        store.put(live, &[b'y'; 100]).unwrap();
        assert!(store.compact_sealed_segments().unwrap() > 0);
        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "gc_restart", cfg).unwrap();
        assert_eq!(reopened.get(live).unwrap(), vec![b'y'; 100]);
        assert!(matches!(reopened.get(old), Err(BlobError::NotFound(_))));
        assert!(matches!(
            reopened.put(old, b"late orphan fragment"),
            Err(BlobError::FencedByBarrier { barrier_gen: 1, .. })
        ));
    }

    #[test]
    fn exact_delete_tombstone_survives_restart_and_compaction() {
        let fs = SimFs::new(1236);
        let cfg = VolumeConfig {
            max_segment_bytes: 256,
            max_blob_bytes: 1024,
            sync_on_put: true,
        };
        let id = BlobId::new(10, 51, 7, 1, 2, 0);
        let live = BlobId::new(10, 51, 8, 1, 2, 0);
        let store = VolumeStore::open(fs.clone(), "tombstone", cfg).unwrap();
        store.put(id, b"orphan").unwrap();
        assert!(store.delete(id).unwrap());
        store.put(live, &[b'l'; 100]).unwrap();
        store
            .put(BlobId::new(10, 51, 8, 2, 2, 0), &[b'm'; 100])
            .unwrap();
        assert!(store.compact_sealed_segments().unwrap() > 0);
        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "tombstone", cfg).unwrap();
        assert!(matches!(reopened.get(id), Err(BlobError::NotFound(_))));
        assert!(matches!(
            reopened.put(id, b"reused"),
            Err(BlobError::Tombstoned(_))
        ));
    }

    #[test]
    fn concurrent_conflicting_puts_never_acknowledge_two_values() {
        let fs = SimFs::new(1240);
        let store = VolumeStore::open(fs.clone(), "conflicts", VolumeConfig::default()).unwrap();
        for step in 1..=64 {
            let id = BlobId::new(1, 1, 1, step, 4, 0);
            let start = std::sync::Barrier::new(2);
            let (a, b) = std::thread::scope(|scope| {
                let a = scope.spawn(|| {
                    start.wait();
                    store.put(id, b"first")
                });
                let b = scope.spawn(|| {
                    start.wait();
                    store.put(id, b"other")
                });
                (a.join().unwrap(), b.join().unwrap())
            });
            assert_ne!(a.is_ok(), b.is_ok());
            assert_eq!(
                store.get(id).unwrap(),
                if a.is_ok() { b"first" } else { b"other" }
            );
        }
        drop(store);
        fs.simulate_power_cut();
        assert_eq!(
            VolumeStore::open(fs, "conflicts", VolumeConfig::default())
                .unwrap()
                .stats()
                .unwrap()
                .live_blobs,
            64
        );
    }

    #[test]
    fn concurrent_put_and_fence_cannot_resurrect_data_after_recovery() {
        for exact in [false, true] {
            let fs = SimFs::new(1241);
            let store = VolumeStore::open(fs.clone(), "fences", VolumeConfig::default()).unwrap();
            for generation in 1..=32 {
                let id = BlobId::new(1, 1, generation, 1, 4, 0);
                let start = std::sync::Barrier::new(2);
                std::thread::scope(|scope| {
                    let put = scope.spawn(|| {
                        start.wait();
                        store.put(id, b"payload")
                    });
                    let fence = scope.spawn(|| {
                        start.wait();
                        if exact {
                            store.delete(id).map(|_| ())
                        } else {
                            store
                                .collect_garbage_barrier(1, 1, 4, generation, u64::MAX)
                                .map(|_| ())
                        }
                    });
                    let _ = put.join().unwrap();
                    fence.join().unwrap().unwrap();
                });
                assert!(matches!(store.get(id), Err(BlobError::NotFound(_))));
            }
            drop(store);
            fs.simulate_power_cut();
            let recovered = VolumeStore::open(fs, "fences", VolumeConfig::default()).unwrap();
            assert_eq!(recovered.stats().unwrap().live_blobs, 0);
            for generation in 1..=32 {
                assert!(recovered
                    .put(BlobId::new(1, 1, generation, 1, 4, 0), b"late")
                    .is_err());
            }
        }
    }

    #[test]
    fn reads_pin_matching_offsets_while_compaction_replaces_segment_files() {
        let fs = SimFs::new(1242);
        let cfg = VolumeConfig {
            max_segment_bytes: 300,
            ..Default::default()
        };
        let store = VolumeStore::open(fs, "compact_read", cfg).unwrap();
        let dead = BlobId::new(1, 1, 1, 1, 4, 0);
        let live = BlobId::new(1, 1, 1, 2, 4, 0);
        store.put(dead, &[b'd'; 70]).unwrap();
        store.put(live, &[b'l'; 70]).unwrap();
        store
            .put(BlobId::new(1, 1, 1, 3, 4, 0), &[b'z'; 70])
            .unwrap();
        store.delete(dead).unwrap();
        std::thread::scope(|scope| {
            let compactor = scope.spawn(|| {
                for _ in 0..100 {
                    store.compact_sealed_segments().unwrap();
                }
            });
            let readers: Vec<_> = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        for _ in 0..500 {
                            assert_eq!(store.get(live).unwrap(), vec![b'l'; 70]);
                            assert_eq!(store.get_slice(live, 10, 20).unwrap(), vec![b'l'; 20]);
                        }
                    })
                })
                .collect();
            compactor.join().unwrap();
            for reader in readers {
                reader.join().unwrap();
            }
        });
    }

    #[test]
    fn failed_tombstone_and_barrier_do_not_poison_later_acknowledged_writes() {
        for torn in [false, true] {
            let fs = SimFs::new(1243);
            let store =
                VolumeStore::open(fs.clone(), "failed_fence", VolumeConfig::default()).unwrap();
            let id = BlobId::new(1, 1, 1, 1, 4, 0);
            store.put(id, b"old").unwrap();
            fs.set_faults(FaultConfig {
                torn_write_prob: if torn { 1.0 } else { 0.0 },
                write_eio_prob: if torn { 0.0 } else { 1.0 },
                ..Default::default()
            });
            assert!(store.delete(id).is_err());
            assert!(store.collect_garbage_barrier(1, 1, 4, 1, u64::MAX).is_err());
            fs.set_faults(FaultConfig::default());
            let next = BlobId::new(1, 1, 2, 1, 4, 0);
            store.put(next, b"next").unwrap();
            drop(store);
            fs.simulate_power_cut();
            let recovered = VolumeStore::open(fs, "failed_fence", VolumeConfig::default()).unwrap();
            assert_eq!(recovered.get(id).unwrap(), b"old");
            assert_eq!(recovered.get(next).unwrap(), b"next");
        }
    }

    #[test]
    fn payload_bitrot_before_tombstone_does_not_truncate_later_durable_records() {
        let fs = SimFs::new(1244);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "bitrot_recovery", cfg).unwrap();
        let dead = BlobId::new(1, 1, 1, 1, 4, 0);
        let live = BlobId::new(1, 1, 1, 2, 4, 0);
        let corrupt = BlobId::new(1, 1, 1, 3, 4, 0);
        store.put(dead, b"deleted").unwrap();
        store.delete(dead).unwrap();
        store.put(live, b"durable").unwrap();
        store.put(corrupt, b"repair-me").unwrap();
        fs.corrupt_byte("bitrot_recovery/vol_00000001.dat", 8 + 33)
            .unwrap();
        fs.corrupt_byte("bitrot_recovery/vol_00000001.dat", 8 + 40 + 33 + 40 + 33)
            .unwrap();
        drop(store);
        fs.simulate_power_cut();
        let recovered = VolumeStore::open(fs, "bitrot_recovery", cfg).unwrap();
        assert!(matches!(
            recovered.put(dead, b"deleted"),
            Err(BlobError::Tombstoned(_))
        ));
        assert_eq!(recovered.get(live).unwrap(), b"durable");
        assert!(matches!(
            recovered.get(corrupt),
            Err(BlobError::CorruptData { .. })
        ));
        recovered.put(corrupt, b"repair-me").unwrap();
        assert_eq!(recovered.get(corrupt).unwrap(), b"repair-me");
    }

    #[test]
    fn mid_file_corrupt_record_header_refuses_to_start() {
        let fs = SimFs::new(1260);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "hdr_bitrot", cfg).unwrap();
        let dead = BlobId::new(1, 1, 1, 1, 4, 0);
        let live = BlobId::new(1, 1, 1, 2, 4, 0);
        let other = BlobId::new(1, 1, 1, 3, 4, 0);
        store.put(dead, b"deleted").unwrap();
        store.put(live, b"durable").unwrap();
        store.delete(dead).unwrap();
        store.put(other, b"other").unwrap();
        drop(store);

        // Damage the header of the second record. It is followed by the
        // tombstone for `dead`; truncating at the bad header would drop that
        // tombstone and resurrect the deleted blob.
        let path = "hdr_bitrot/vol_00000001.dat";
        fs.corrupt_byte(path, 8 + 40 + 4).unwrap();
        let damaged = {
            let file = fs.open_rw(path, false).unwrap();
            let mut bytes = vec![0u8; file.len().unwrap() as usize];
            file.read_at(0, &mut bytes).unwrap();
            bytes
        };

        match VolumeStore::open(fs.clone(), "hdr_bitrot", cfg) {
            Err(BlobError::InvalidSegment(_)) => {}
            Err(error) => panic!("expected InvalidSegment, got {error:?}"),
            Ok(_) => panic!("a mid-file corrupt header must not be truncated away"),
        }

        let file = fs.open_rw(path, false).unwrap();
        let mut after = vec![0u8; file.len().unwrap() as usize];
        file.read_at(0, &mut after).unwrap();
        assert_eq!(after, damaged, "recovery must not rewrite mid-file damage");
    }

    #[test]
    fn torn_tail_record_header_still_truncates_and_volume_stays_appendable() {
        // A partial header and a complete header with a short payload are both
        // provably the final, never-acknowledged bytes and are still removed.
        for torn_len in [10usize, 33 + 2] {
            let fs = SimFs::new(1261);
            let cfg = VolumeConfig::default();
            let store = VolumeStore::open(fs.clone(), "torn_hdr", cfg).unwrap();
            let a = BlobId::new(1, 1, 1, 1, 4, 0);
            let b = BlobId::new(1, 1, 1, 2, 4, 0);
            store.put(a, b"alpha").unwrap();
            store.put(b, b"bravo").unwrap();
            drop(store);

            let path = "torn_hdr/vol_00000001.dat";
            let file = fs.open_rw(path, false).unwrap();
            let mut bytes = vec![0u8; file.len().unwrap() as usize];
            file.read_at(0, &mut bytes).unwrap();
            let valid_len = bytes.len() as u64;
            // Replay the first record's header (plus part of its payload) at
            // EOF, as a torn append would leave it.
            file.write_at(valid_len, &bytes[8..8 + torn_len]).unwrap();
            file.sync_data().unwrap();
            assert_eq!(file.len().unwrap(), valid_len + torn_len as u64);
            drop(file);

            let reopened = VolumeStore::open(fs.clone(), "torn_hdr", cfg).unwrap();
            assert_eq!(reopened.get(a).unwrap(), b"alpha");
            assert_eq!(reopened.get(b).unwrap(), b"bravo");
            let c = BlobId::new(1, 1, 1, 3, 4, 0);
            reopened.put(c, b"charlie").unwrap();
            drop(reopened);

            let again = VolumeStore::open(fs, "torn_hdr", cfg).unwrap();
            assert_eq!(again.get(a).unwrap(), b"alpha");
            assert_eq!(again.get(b).unwrap(), b"bravo");
            assert_eq!(again.get(c).unwrap(), b"charlie");
        }
    }

    #[test]
    fn remote_delete_sends_peer_token_only_when_configured() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            for expected in [Some("secret"), None] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).unwrap();
                    request.push(byte[0]);
                }
                let request = String::from_utf8(request).unwrap();
                let token = request
                    .lines()
                    .find(|line| line.starts_with("x-yadb-raft-token:"));
                assert_eq!(token, expected.map(|_| "x-yadb-raft-token: secret"));
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabsent",
                    )
                    .unwrap();
            }
        });
        let id = BlobId::new(1, 1, 1, 1, 4, 1);
        for token in [Some("secret".to_string()), None] {
            let slot = PoolSlot::<SimFs>::remote("pool", 0, "remote", 2, peer.clone(), token);
            assert!(!slot.delete(id).unwrap());
        }
        server.join().unwrap();
    }

    #[test]
    fn remote_delete_requires_explicit_tombstone_ack_not_http_404() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            for response in [
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabsent",
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).unwrap();
                    request.push(byte[0]);
                }
                assert!(request.starts_with(b"DELETE /internal/vdisk/pool/0/blob/"));
                stream.write_all(response.as_bytes()).unwrap();
            }
        });
        let slot = PoolSlot::<SimFs>::remote("pool", 0, "remote", 2, peer, None);
        let id = BlobId::new(1, 1, 1, 1, 4, 1);
        assert!(slot.delete(id).is_err()); // missing slot/route is NOT a durable fence
        assert!(!slot.delete(id).unwrap());
        server.join().unwrap();
    }

    #[test]
    fn pool_striper_survives_bitrot_and_repairs_missing_domain() {
        let fs = SimFs::new(2026);
        let mut domains = Vec::new();
        for i in 0..6 {
            domains.push(
                VolumeStore::open(fs.clone(), format!("az{i}"), VolumeConfig::default()).unwrap(),
            );
        }

        let striper = PoolStriper::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }, domains).unwrap();
        let id = BlobId::new(7, 1, 1, 1, 2, 0);
        let secret = b"Multi-tenant erasure-coded tablet SSTable block #0001";
        striper.put_blob(id, secret).unwrap();

        // Corrupt a byte on az0 AND wipe az3's segment completely!
        let files_az0 = fs.list_files("az0").unwrap();
        if let Some(seg) = files_az0.first() {
            fs.corrupt_byte(&format!("az0/{seg}"), 45).unwrap();
        }
        let files_az3 = fs.list_files("az3").unwrap();
        for seg in files_az3 {
            let _ = fs.remove_file(&format!("az3/{seg}"));
        }

        // Transparent read still succeeds via RS(4+2)!
        let read_back = striper.get_blob(id).unwrap();
        assert_eq!(read_back.as_slice(), secret);

        // Now run self-healing Scrubber repair_blob: both the bit-rotted az0 shard AND
        // the missing/corrupt shard should be healed back to 6/6 intact!
        let report = striper.repair_blob(id).unwrap();
        assert!(report.repaired_parts >= 1);
        let second_check = striper.repair_blob(id).unwrap();
        assert_eq!(second_check.intact_parts, 6);
        assert_eq!(second_check.repaired_parts, 0);
    }

    /// Builds a striper over `count` local domains holding one small blob each, so a
    /// test can corrupt or remove individual fragments by domain name.
    fn seeded_striper(
        seed: u64,
        scheme: ErasureScheme,
        count: usize,
        id: BlobId,
        payload: &[u8],
    ) -> (SimFs, PoolStriper<SimFs>) {
        let fs = SimFs::new(seed);
        let mut domains = Vec::new();
        for i in 0..count {
            domains.push(
                VolumeStore::open(fs.clone(), format!("az{i}"), VolumeConfig::default()).unwrap(),
            );
        }
        let striper = PoolStriper::new(scheme, domains).unwrap();
        striper.put_blob(id, payload).unwrap();
        (fs, striper)
    }

    fn corrupt_domain(fs: &SimFs, domain: &str) {
        let files = fs.list_files(domain).unwrap();
        assert_eq!(
            files.len(),
            1,
            "{domain}: expected exactly one segment for the single seeded blob"
        );
        // Offset 45 lands inside the payload CRC region for a short fragment, the
        // same offset the bitrot test above uses; a header byte would truncate
        // instead of corrupting, which is a different finding.
        fs.corrupt_byte(&format!("{domain}/{}", files[0]), 45)
            .unwrap();
    }

    fn wipe_domain(fs: &SimFs, domain: &str) {
        for seg in fs.list_files(domain).unwrap() {
            fs.remove_file(&format!("{domain}/{seg}")).unwrap();
        }
    }

    /// In-memory VDisk for the S6 tests: a real fragment store plus a switchable
    /// PUT failure, so a slot can be failed and then recover without reopening.
    ///
    /// This answers by method and path rather than from a fixed script. A scripted
    /// server couples the test to the client's connection order, and the client
    /// has its own connect/read timeouts and fetches fragments in parallel, so
    /// under load the responses could be paired with the wrong requests and the
    /// test would fail for a reason unrelated to the code under test.
    struct MockVdisk {
        addr: String,
        store: Arc<std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>>,
        fail_puts: Arc<std::sync::atomic::AtomicBool>,
        /// Refuses exactly one PUT and then accepts the next one. Models a spare
        /// that was momentarily unreachable, which the handoff round must survive
        /// by reassigning the part to another spare instead of failing the write.
        fail_once: Arc<std::sync::atomic::AtomicBool>,
        /// HTTP status a PUT answers with while non-zero, e.g. 409 for an
        /// immutable conflict or 412 for a barrier fence. Zero accepts the write.
        /// The other switches model an unreachable slot; this one models a slot
        /// that answers with a *permanent* refusal.
        put_status: Arc<std::sync::atomic::AtomicU16>,
        seen: Arc<std::sync::Mutex<Vec<String>>>,
        server: Option<std::thread::JoinHandle<()>>,
    }

    impl MockVdisk {
        fn start() -> Self {
            use std::io::{Read, Write};
            use std::net::TcpListener;
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let store = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
            let fail_puts = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let fail_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let put_status = Arc::new(std::sync::atomic::AtomicU16::new(0));
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            let (store_c, fail_c, seen_c) = (store.clone(), fail_puts.clone(), seen.clone());
            let fail_once_c = fail_once.clone();
            let put_status_c = put_status.clone();
            let server = std::thread::spawn(move || loop {
                let (mut stream, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(_) => return,
                };
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                let (head, mut body) = loop {
                    let n = match stream.read(&mut buf) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    raw.extend_from_slice(&buf[..n]);
                    if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&raw[..pos]).to_string();
                        let rest = raw[pos + 4..].to_vec();
                        break (head, rest);
                    }
                };
                let mut lines = head.lines();
                let request_line = lines.next().unwrap_or("").to_string();
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts.next().unwrap_or("").to_string();
                let want: usize = lines
                    .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                    .and_then(|l| l.split(':').nth(1))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                while body.len() < want {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => body.extend_from_slice(&buf[..n]),
                    }
                }
                seen_c.lock().unwrap().push(request_line);

                // Read once: a status line must not mix two observations.
                let refused_status = put_status_c.load(Ordering::Relaxed);
                let response: Vec<u8> = match method.as_str() {
                    "PUT" if refused_status != 0 => format!(
                        "HTTP/1.1 {refused_status} Refused\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .into_bytes(),
                    "PUT" if fail_c.load(Ordering::Relaxed) => {
                        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
                    }
                    "PUT" if fail_once_c.swap(false, Ordering::Relaxed) => {
                        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
                    }
                    "PUT" => {
                        store_c.lock().unwrap().insert(path.to_string(), body);
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_vec()
                    }
                    "GET" => match store_c.lock().unwrap().get(&path) {
                        Some(bytes) => {
                            let mut out = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                bytes.len()
                            )
                            .into_bytes();
                            out.extend_from_slice(bytes);
                            out
                        }
                        None => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_vec(),
                    },
                    "DELETE" => {
                        let existed = store_c.lock().unwrap().remove(&path).is_some();
                        let body = if existed { "deleted" } else { "absent" };
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .into_bytes()
                    }
                    _ => b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_vec(),
                };
                let _ = stream.write_all(&response);
                let _ = stream.flush();
            });
            Self {
                addr,
                store,
                fail_puts,
                fail_once,
                put_status,
                seen,
                server: Some(server),
            }
        }

        fn requests(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }

        /// Stops the accept loop and joins it, so a leaked listener cannot keep
        /// serving into the next test.
        fn stop(mut self) {
            let _ = std::net::TcpStream::connect(&self.addr);
            if let Some(server) = self.server.take() {
                let _ = server.join();
            }
        }
    }

    // S6 follow-up: `put_blob` used to ack once `min_write_quorum()` fragments
    // landed — `k` for RS 4+2 — so an ack could leave zero parity on disk and the
    // caller believed the data would survive losing a node. A write that cannot
    // reach full redundancy is now refused instead of acked.
    #[test]
    fn short_write_is_refused_instead_of_acked() {
        let vdisk = MockVdisk::start();
        vdisk.fail_puts.store(true, Ordering::Relaxed);
        let addr = vdisk.addr.clone();
        let fs = SimFs::new(7001);
        let mut slots = Vec::new();
        for i in 0..5 {
            let store = Arc::new(
                VolumeStore::open(fs.clone(), format!("az{i}"), VolumeConfig::default()).unwrap(),
            );
            slots.push(PoolSlot::local(i, format!("az{i}"), 1, None, store));
        }
        slots.push(PoolSlot::<SimFs>::remote("pool", 5, "az5", 2, addr, None));
        let striper =
            PoolStriper::from_slots("pool", ErasureScheme::ReedSolomon { k: 4, m: 2 }, slots)
                .unwrap();

        let id = BlobId::new(11, 1, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";
        // Five of six fragments is above the k=4 read threshold, which is exactly
        // the case that used to be acked with no parity at all.
        match striper.put_blob(id, payload) {
            Err(BlobError::RedundancyNotMet {
                written,
                required,
                id: refused,
            }) => {
                assert_eq!(written, 5);
                assert_eq!(required, 6);
                assert_eq!(refused, id);
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        let health = striper.health_snapshot(false);
        assert_eq!(health.refused_puts, 1);
        assert_eq!(health.min_write_slots, 6);

        // The peer comes back; the same write is then fully redundant.
        vdisk.fail_puts.store(false, Ordering::Relaxed);
        striper.put_blob(id, payload).unwrap();
        assert_eq!(striper.repair_blob(id).unwrap().intact_parts, 6);
        assert_eq!(striper.health_snapshot(false).refused_puts, 1);
        vdisk.stop();
    }

    // A refused write is not rolled back: the fragments that did land stay on
    // disk under an id no manifest will ever reference. That is deliberate — the
    // refusal is reported to the caller before publication — but it means the
    // orphan scan is what reclaims the space, not the refusal itself.
    #[test]
    fn refused_write_leaves_unreferenced_fragments() {
        let vdisk = MockVdisk::start();
        vdisk.fail_puts.store(true, Ordering::Relaxed);
        let addr = vdisk.addr.clone();
        let fs = SimFs::new(7002);
        let mut slots = Vec::new();
        for i in 0..5 {
            let store = Arc::new(
                VolumeStore::open(fs.clone(), format!("az{i}"), VolumeConfig::default()).unwrap(),
            );
            slots.push(PoolSlot::local(i, format!("az{i}"), 1, None, store));
        }
        slots.push(PoolSlot::<SimFs>::remote("pool", 5, "az5", 2, addr, None));
        let striper =
            PoolStriper::from_slots("pool", ErasureScheme::ReedSolomon { k: 4, m: 2 }, slots)
                .unwrap();

        let id = BlobId::new(11, 2, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";
        assert!(striper.put_blob(id, payload).is_err());
        // Five of six fragments are on disk, so the payload still decodes — but
        // the write was reported as failed and no manifest will point at it.
        assert_eq!(striper.get_blob(id).unwrap(), payload);
        assert_eq!(striper.health_snapshot(false).refused_puts, 1);
        vdisk.stop();
    }

    // A fully redundant write must be acked and must not be counted as refused.
    #[test]
    fn full_redundancy_write_is_acked() {
        let id = BlobId::new(11, 3, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";
        let (_fs, striper) = seeded_striper(
            7003,
            ErasureScheme::ReedSolomon { k: 4, m: 2 },
            6,
            id,
            payload,
        );
        let health = striper.health_snapshot(false);
        assert_eq!(health.refused_puts, 0);
        assert_eq!(health.min_write_slots, 6);
    }

    /// A pool whose every slot is a remote `MockVdisk`, so one slot can be
    /// failed and later recovered. Slot `i` sits on node `i % 3 + 1`, mirroring
    /// the daemon's `targets[slot % nodes]` striping: the handoff slots land on
    /// different nodes from the nominal slots they may have to replace.
    fn remote_pool(
        pool: &str,
        scheme: ErasureScheme,
        handoff: usize,
    ) -> (Vec<MockVdisk>, PoolStriper<SimFs>) {
        let nominal = scheme.total_parts();
        let mut vdisks = Vec::with_capacity(nominal + handoff);
        let mut slots = Vec::with_capacity(nominal + handoff);
        for i in 0..(nominal + handoff) {
            let vdisk = MockVdisk::start();
            let node = (i % 3) as u32 + 1;
            slots.push(PoolSlot::<SimFs>::remote(
                pool,
                i,
                format!("az{node}"),
                node,
                vdisk.addr.clone(),
                None,
            ));
            vdisks.push(vdisk);
        }
        let striper = PoolStriper::from_slots(pool, scheme, slots).unwrap();
        (vdisks, striper)
    }

    fn stop_all(vdisks: Vec<MockVdisk>) {
        for vdisk in vdisks {
            vdisk.stop();
        }
    }

    fn stored(vdisks: &[MockVdisk], from: usize, to: usize) -> usize {
        (from..to)
            .map(|i| vdisks[i].store.lock().unwrap().len())
            .sum()
    }

    /// A local-only pool with `handoff` spare slots. Every slot is a real
    /// `VolumeStore`, so a tombstone and a barrier are genuine durable refusals
    /// rather than an HTTP status a mock was told to answer. Slot `i` sits on
    /// node `i % 3 + 1`, mirroring `remote_pool`, so a spare on the failed
    /// nominal slot's node stays distinguishable from one on a healthy node.
    fn local_pool(
        seed: u64,
        scheme: ErasureScheme,
        handoff: usize,
    ) -> (Vec<Arc<VolumeStore<SimFs>>>, PoolStriper<SimFs>) {
        let fs = SimFs::new(seed);
        let mut stores = Vec::new();
        let mut slots = Vec::new();
        for i in 0..(scheme.total_parts() + handoff) {
            let store = Arc::new(
                VolumeStore::open(fs.clone(), format!("az{i}"), VolumeConfig::default()).unwrap(),
            );
            let node = (i % 3) as u32 + 1;
            slots.push(PoolSlot::local(
                i,
                format!("az{i}"),
                node,
                None,
                store.clone(),
            ));
            stores.push(store);
        }
        let striper = PoolStriper::from_slots("pool", scheme, slots).unwrap();
        (stores, striper)
    }

    // YDB's handoff-disk rule: a part whose nominal slot does not answer is
    // written to a spare slot, and the ack still means every part is durable.
    // Without it, losing one node made the whole pool read-only for writes.
    #[test]
    fn a_failed_nominal_slot_is_handed_off_and_the_ack_stays_fully_redundant() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 3);
        let id = BlobId::new(11, 40, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

        // Nominal slot 2 (node 3) stops answering puts.
        vdisks[2].fail_puts.store(true, Ordering::Relaxed);

        striper
            .put_blob(id, payload)
            .expect("handoff must keep the write available");

        let health = striper.health_snapshot(false);
        assert_eq!(health.refused_puts, 0);
        assert_eq!(
            health.degraded_puts, 0,
            "a handoff write is fully redundant: every part is durable on its own slot"
        );
        assert_eq!(health.handoff_slots, 3);
        assert_eq!(health.total_slots, 6);
        assert_eq!(striper.get_blob(id).unwrap(), payload);

        // The failed slot received nothing; exactly one spare took the part.
        assert_eq!(stored(&vdisks, 2, 3), 0);
        assert_eq!(stored(&vdisks, 6, 9), 1);
        stop_all(vdisks);
    }

    // Sensitivity for the test above: with no handoff slots the same single-slot
    // failure refuses the write. Remove the handoff fallback from `put_blob` and
    // the test above fails here instead.
    #[test]
    fn without_handoff_a_failed_nominal_slot_refuses_the_write() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 0);
        let id = BlobId::new(11, 41, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

        vdisks[2].fail_puts.store(true, Ordering::Relaxed);
        match striper.put_blob(id, payload) {
            Err(BlobError::RedundancyNotMet {
                written, required, ..
            }) => {
                assert_eq!(written, 5);
                assert_eq!(required, 6);
            }
            other => panic!("expected RedundancyNotMet, got {other:?}"),
        }
        assert_eq!(striper.health_snapshot(false).refused_puts, 1);
        stop_all(vdisks);
    }

    // YDB counts effective replicas per *disk*, not per part: two parts of one
    // blob on one handoff disk are one replica. So a spare takes at most one
    // part, and two failures with a single spare must be refused rather than
    // acked with a hidden double placement.
    #[test]
    fn one_handoff_slot_never_takes_two_parts_of_the_same_blob() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 1);
        let id = BlobId::new(11, 42, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

        // Two nominal slots, on nodes 2 and 3, fail; the only spare is on node 1.
        vdisks[1].fail_puts.store(true, Ordering::Relaxed);
        vdisks[2].fail_puts.store(true, Ordering::Relaxed);

        match striper.put_blob(id, payload) {
            Err(BlobError::RedundancyNotMet { written, .. }) => assert_eq!(written, 5),
            other => panic!("expected RedundancyNotMet, got {other:?}"),
        }
        assert_eq!(
            stored(&vdisks, 6, 7),
            1,
            "the single spare holds one part, never two"
        );
        stop_all(vdisks);
    }

    // A handoff copy is invisible to the manifest, so deletion has to tombstone
    // every candidate. Tombstoning only the nominal slot would let the blob come
    // back as soon as that slot answered `NotFound`.
    #[test]
    fn delete_tombstones_the_handoff_copy_too() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 3);
        let id = BlobId::new(11, 43, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

        vdisks[2].fail_puts.store(true, Ordering::Relaxed);
        striper.put_blob(id, payload).unwrap();
        assert_eq!(stored(&vdisks, 6, 9), 1);

        // The node returns, but its nominal slot still has no data.
        vdisks[2].fail_puts.store(false, Ordering::Relaxed);

        assert!(striper.delete(id).unwrap());
        assert_eq!(
            stored(&vdisks, 0, 9),
            0,
            "no copy, handoff or nominal, may survive a delete"
        );
        assert!(striper.get_blob(id).is_err());
        stop_all(vdisks);
    }

    // Assimilation: once the nominal slot answers again, a part living on a
    // handoff slot is moved back, and only then is the handoff copy removed.
    #[test]
    fn repair_moves_a_handoff_part_back_to_its_nominal_slot() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 3);
        let id = BlobId::new(11, 44, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

        vdisks[2].fail_puts.store(true, Ordering::Relaxed);
        striper.put_blob(id, payload).unwrap();
        assert_eq!(stored(&vdisks, 6, 9), 1);

        vdisks[2].fail_puts.store(false, Ordering::Relaxed);
        let report = striper.repair_blob(id).unwrap();
        assert_eq!(
            report.repaired_parts, 1,
            "the one handoff part is moved back"
        );
        assert_eq!(report.intact_parts, 6);
        assert_eq!(
            stored(&vdisks, 2, 3),
            1,
            "the part is back on its nominal slot"
        );
        assert_eq!(stored(&vdisks, 6, 9), 0, "the handoff copy is gone");
        assert_eq!(striper.get_blob(id).unwrap(), payload);
        stop_all(vdisks);
    }

    // A spare that refuses one part must not swallow the write: the next round
    // reassigns the part to another spare. This is the loop that keeps a single
    // momentarily unreachable spare from costing the pool its redundancy.
    #[test]
    fn a_spare_that_refuses_once_is_skipped_by_the_next_round() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        // Each iteration arms a different spare, so the spare the round picks
        // first is exercised whichever one the hash order chooses.
        for armed in 6..9 {
            let (vdisks, striper) = remote_pool("pool", scheme, 3);
            let id = BlobId::new(11, 46, 1, 1, 2, armed as u8);
            let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

            vdisks[1].fail_puts.store(true, Ordering::Relaxed);
            vdisks[2].fail_puts.store(true, Ordering::Relaxed);
            vdisks[armed].fail_once.store(true, Ordering::Relaxed);

            striper.put_blob(id, payload).unwrap_or_else(|e| {
                panic!("spare {armed} refusing once must not fail the write: {e:?}")
            });
            assert_eq!(
                stored(&vdisks, 0, 9),
                6,
                "every part is still durable exactly once (spare {armed} armed)"
            );
            assert_eq!(striper.get_blob(id).unwrap(), payload);
            stop_all(vdisks);
        }
    }

    // Defect: `put_blob` reduced every nominal result to `is_ok()`, so a
    // permanent refusal looked exactly like an unreachable slot. The part was
    // then written to a handoff slot and the write was acknowledged, and the ack
    // described bytes that reads never return: the nominal slots still hold the
    // first payload, while the spares hold fragments of the second.
    #[test]
    fn conflicting_nominal_put_refuses_the_write_instead_of_handing_off() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        // Six spares, so the buggy code could place every part and ack.
        let (vdisks, striper) = remote_pool("pool", scheme, 6);
        let id = BlobId::new(13, 1, 1, 1, 2, 0);
        let first = b"Multi-tenant erasure-coded tablet SSTable block #0001";
        let second = b"a different payload for the same immutable id #0002";
        striper.put_blob(id, first).unwrap();

        // Every nominal slot now answers 409: the id already holds other bytes.
        for vdisk in vdisks.iter().take(scheme.total_parts()) {
            vdisk.put_status.store(409, Ordering::Relaxed);
        }

        match striper.put_blob(id, second) {
            // The error is propagated unchanged, so it names the fragment the
            // slot refused, not the logical blob: same blob, part 1.
            Err(BlobError::ImmutableConflict(refused)) => {
                assert_eq!(
                    refused.with_part(0),
                    id,
                    "refusal names a part of this blob"
                )
            }
            other => panic!("a conflicting put must refuse with ImmutableConflict, got {other:?}"),
        }
        assert_eq!(
            stored(&vdisks, 6, 12),
            0,
            "a permanent refusal must never be handed off to a spare"
        );
        assert_eq!(
            striper.get_blob(id).unwrap(),
            first,
            "an ack for `second` would describe bytes that reads never return"
        );
        stop_all(vdisks);
    }

    // A tombstone is a durable fence, not a missing slot. Writing the part to a
    // spare would resurrect a blob the tombstone just deleted, from a copy the
    // tombstone never covered (a delete that reached the nominal slots only).
    #[test]
    fn tombstoned_nominal_slot_refuses_the_write_without_resurrecting() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (stores, striper) = local_pool(7301, scheme, 6);
        let id = BlobId::new(13, 2, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";
        striper.put_blob(id, payload).unwrap();

        // A delete that reached the nominal placement but not the spares: every
        // part id is tombstoned where it nominally lives, and the spares would
        // still accept the same bytes.
        for store in stores.iter().take(scheme.total_parts()) {
            for part in 1..=scheme.total_parts() as u8 {
                store.delete(id.with_part(part)).unwrap();
            }
        }

        match striper.put_blob(id, payload) {
            Err(BlobError::Tombstoned(refused)) => {
                assert_eq!(
                    refused.with_part(0),
                    id,
                    "refusal names a part of this blob"
                )
            }
            other => panic!("a put over a tombstone must refuse with Tombstoned, got {other:?}"),
        }
        for (i, store) in stores.iter().enumerate() {
            assert_eq!(
                store.stats().unwrap().live_blobs,
                0,
                "slot {i} must hold no fragment: a tombstone is not a missing slot"
            );
        }
        // No fragment is readable from anywhere, nominal or spare: the deleted
        // blob is gone, not resurrected on a handoff slot.
        assert!(
            matches!(
                striper.get_blob(id),
                Err(BlobError::InsufficientFragments { available: 0, .. })
            ),
            "the tombstoned blob must not be resurrected, got {:?}",
            striper.get_blob(id)
        );
    }

    // The collector reaches a slot through `apply_barrier`, not the volume
    // directly, so the slot must forward the deadline instead of dropping it. A
    // remote slot sends it to the target; a local one passes it to the store.
    // Without this the check would exist and still be bypassed by the only path
    // the sweep uses.
    #[test]
    fn a_slot_refuses_a_barrier_whose_lease_expired() {
        let scheme = ErasureScheme::Mirror { replicas: 3 };
        let (_, striper) = local_pool(7303, scheme, 3);
        let id = BlobId::new(14, 4, 1, 1, 2, 0);
        striper.put_blob(id, b"unreferenced fragment").unwrap();

        let slot = &striper.slots()[0];
        let error = slot
            .apply_barrier(id.project_id(), id.owner_id(), id.channel(), 1, false, 1)
            .expect_err("an expired barrier must be refused by the slot");
        assert!(
            matches!(error, BlobError::BarrierLeaseExpired { .. }),
            "expected BarrierLeaseExpired, got {error:?}"
        );
        // Nothing was evicted on this slot, and no durable fence was installed.
        assert!(slot
            .local_store()
            .unwrap()
            .live_ids()
            .iter()
            .any(|present| present.with_part(0) == id));

        slot.apply_barrier(
            id.project_id(),
            id.owner_id(),
            id.channel(),
            1,
            false,
            u64::MAX,
        )
        .unwrap();
        assert!(
            !slot
                .local_store()
                .unwrap()
                .live_ids()
                .iter()
                .any(|present| present.with_part(0) == id),
            "a live barrier must still evict"
        );
    }

    // Same rule for a barrier fence: the generation barrier is durable on the
    // nominal slots, and rerouting the part to a spare the barrier does not cover
    // would defeat it.
    #[test]
    fn barrier_fenced_nominal_slot_refuses_the_write_instead_of_handing_off() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (stores, striper) = local_pool(7302, scheme, 6);
        let id = BlobId::new(13, 3, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

        for slot in striper.slots().iter().take(scheme.total_parts()) {
            slot.apply_barrier(
                id.project_id(),
                id.owner_id(),
                id.channel(),
                1,
                false,
                u64::MAX,
            )
            .unwrap();
        }

        match striper.put_blob(id, payload) {
            Err(BlobError::FencedByBarrier {
                id: refused,
                barrier_gen,
            }) => {
                assert_eq!(
                    refused.with_part(0),
                    id,
                    "refusal names a part of this blob"
                );
                assert_eq!(barrier_gen, 1);
            }
            other => panic!("a fenced put must refuse with FencedByBarrier, got {other:?}"),
        }
        for (i, store) in stores.iter().enumerate() {
            assert_eq!(
                store.stats().unwrap().live_blobs,
                0,
                "slot {i} must hold no fragment: a fence is not a missing slot"
            );
        }
        assert!(striper.get_blob(id).is_err());
    }

    // The behaviour the fix must preserve: a slot that could not answer *is*
    // handoff-eligible. One I/O failure on a nominal slot costs nothing but the
    // placement, and the ack still means full redundancy.
    #[test]
    fn a_nominal_slot_that_fails_once_is_handed_off() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 3);
        let id = BlobId::new(13, 4, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

        // Nominal slot 2 misses its first PUT with a 500, which a remote slot
        // maps to `Io`: the only handoff-eligible failure.
        vdisks[2].fail_once.store(true, Ordering::Relaxed);

        striper
            .put_blob(id, payload)
            .expect("a transient I/O failure must still be handed off");
        assert_eq!(stored(&vdisks, 2, 3), 0);
        assert_eq!(stored(&vdisks, 6, 9), 1);
        assert_eq!(stored(&vdisks, 0, 9), 6);
        assert_eq!(striper.get_blob(id).unwrap(), payload);
        let health = striper.health_snapshot(false);
        assert_eq!(health.refused_puts, 0);
        assert_eq!(health.degraded_puts, 0);
        stop_all(vdisks);
    }

    // Defect: the handoff loop broke out as soon as a whole round made no
    // progress. One refusing spare then stopped the search while a healthy spare
    // was still untried, so a write that had a spare available was refused. The
    // search now ends only when no unplaced part has an untried eligible spare.
    #[test]
    fn handoff_tries_the_next_spare_when_the_first_eligible_spare_refuses() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 3);
        let id = BlobId::new(13, 5, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

        // Nominal slot 2 (node 3) stops answering puts.
        vdisks[2].fail_puts.store(true, Ordering::Relaxed);
        let failed_node = striper.slots()[2].node_id();
        // The spare on the failed node is skipped, so two spares are eligible.
        let eligible: Vec<usize> = striper
            .handoff_order_for(id)
            .into_iter()
            .filter(|&slot| striper.slots()[slot].node_id() != failed_node)
            .collect();
        assert!(
            eligible.len() >= 2,
            "the fixture must offer two eligible spares, got {eligible:?}"
        );
        let (first, second) = (eligible[0], eligible[1]);
        vdisks[first].fail_puts.store(true, Ordering::Relaxed);

        striper.put_blob(id, payload).unwrap_or_else(|e| {
            panic!(
                "spare {first} refusing must not stop the search while spare {second} is \
                 untried: {e:?}"
            )
        });
        assert_eq!(
            stored(&vdisks, first, first + 1),
            0,
            "the refusing spare must hold nothing"
        );
        assert_eq!(
            stored(&vdisks, second, second + 1),
            1,
            "the part must land on the next eligible spare"
        );
        assert_eq!(stored(&vdisks, 0, 9), 6);
        assert_eq!(striper.get_blob(id).unwrap(), payload);
        assert_eq!(striper.health_snapshot(false).degraded_puts, 0);
        stop_all(vdisks);
    }

    // Adding handoff slots must not move a single existing part: `select_slot`
    // reduces modulo the nominal count, not the slot count. Using the slot count
    // would make every blob written before the upgrade unreadable.
    #[test]
    fn appending_handoff_slots_does_not_move_existing_parts() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let fs = SimFs::new(9101);
        let id = BlobId::new(11, 45, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";

        let mut slots = Vec::new();
        for i in 0..6 {
            let store = Arc::new(
                VolumeStore::open(fs.clone(), format!("az{i}"), VolumeConfig::default()).unwrap(),
            );
            slots.push(PoolSlot::local(
                i,
                format!("az{i}"),
                (i % 3) as u32 + 1,
                None,
                store,
            ));
        }
        let before = PoolStriper::from_slots("pool", scheme, slots).unwrap();
        before.put_blob(id, payload).unwrap();

        let mut slots = Vec::new();
        let mut extra = Vec::new();
        for i in 0..9 {
            let store = Arc::new(
                VolumeStore::open(fs.clone(), format!("az{i}"), VolumeConfig::default()).unwrap(),
            );
            if i >= 6 {
                extra.push(store.clone());
            }
            slots.push(PoolSlot::local(
                i,
                format!("az{i}"),
                (i % 3) as u32 + 1,
                None,
                store,
            ));
        }
        let after = PoolStriper::from_slots("pool", scheme, slots).unwrap();
        assert_eq!(after.get_blob(id).unwrap(), payload);
        for (i, store) in extra.iter().enumerate() {
            assert_eq!(
                store.stats().unwrap().live_blobs,
                0,
                "handoff slot {} must stay empty: the old parts did not move",
                6 + i
            );
        }
    }

    // S2: a fully present but bit-rotted blob must never be reported as absent.
    // `PoolSlot::get` distinguishes 404 from 422, but `get_blob` used to drop every
    // error with `if let Ok(..)` / `.ok()` and then answer `NotFound`, so an operator
    // could not tell corruption from a missing object.
    #[test]
    fn corrupt_mirror_copies_are_reported_as_corruption_not_absence() {
        let id = BlobId::new(9, 1, 1, 1, 2, 0);
        let payload = b"mirror payload that is present but bit-rotted";
        let (fs, striper) =
            seeded_striper(3101, ErasureScheme::Mirror { replicas: 3 }, 3, id, payload);
        for i in 0..3 {
            corrupt_domain(&fs, &format!("az{i}"));
        }
        match striper.get_blob(id) {
            Err(BlobError::CorruptData { .. }) => {}
            Err(BlobError::NotFound(_)) => panic!(
                "corruption was reported as absence: a present but damaged blob is not a \
                 missing blob"
            ),
            other => panic!("expected CorruptData, got {other:?}"),
        }
    }

    // The other half of S2: surfacing corruption must not cost availability. One
    // damaged replica with a healthy one beside it still reads.
    #[test]
    fn corrupt_mirror_copy_still_reads_from_a_healthy_replica() {
        let id = BlobId::new(9, 2, 1, 1, 2, 0);
        let payload = b"mirror payload with one damaged replica";
        let (fs, striper) =
            seeded_striper(3102, ErasureScheme::Mirror { replicas: 3 }, 3, id, payload);
        corrupt_domain(&fs, "az0");
        assert_eq!(striper.get_blob(id).unwrap(), payload);
    }

    #[test]
    fn corrupt_rs_fragments_beyond_the_reconstruction_threshold_report_corruption() {
        let id = BlobId::new(9, 3, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";
        // RS(4+2): corrupting three fragments leaves three healthy, below k=4, so
        // the payload cannot be reconstructed. The shortfall is not the cause — the
        // observed damage is — so the error must name corruption.
        let (fs, striper) = seeded_striper(
            3103,
            ErasureScheme::ReedSolomon { k: 4, m: 2 },
            6,
            id,
            payload,
        );
        for i in 0..3 {
            corrupt_domain(&fs, &format!("az{i}"));
        }
        match striper.get_blob(id) {
            Err(BlobError::CorruptData { .. }) => {}
            Err(BlobError::InsufficientFragments { .. }) => panic!(
                "damage was reported as a shortfall: the fragments were present and failed \
                 their checksum"
            ),
            other => panic!("expected CorruptData, got {other:?}"),
        }
        // Repair cannot invent the missing shards either, and must not claim the
        // fragments were merely short.
        assert!(matches!(
            striper.repair_blob(id),
            Err(BlobError::CorruptData { .. })
        ));
    }

    #[test]
    fn corrupt_rs_fragment_within_threshold_still_reads_and_repairs() {
        let id = BlobId::new(9, 4, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";
        let (fs, striper) = seeded_striper(
            3104,
            ErasureScheme::ReedSolomon { k: 4, m: 2 },
            6,
            id,
            payload,
        );
        corrupt_domain(&fs, "az0");
        wipe_domain(&fs, "az3");
        assert_eq!(striper.get_blob(id).unwrap(), payload);
        // A corrupt fragment is still treated as missing for reconstruction, so the
        // scrub rewrites it instead of refusing.
        let report = striper.repair_blob(id).unwrap();
        assert!(report.repaired_parts >= 1);
        assert_eq!(striper.repair_blob(id).unwrap().intact_parts, 6);
    }
}
