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
pub use volume::{
    PinGrant, PinId, PinView, VolumeConfig, VolumeStats, VolumeStore, MAX_PIN_TTL_MS,
};

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

    #[error(
        "fenced delete of {id} refused: the fragment was written under target epoch \
         {recorded_epoch}, but the delete names epoch {expected_epoch}"
    )]
    FencedByEpoch {
        id: BlobId,
        recorded_epoch: u64,
        expected_epoch: u64,
    },

    #[error(
        "epoch advance refused: the caller fenced its pass at epoch {expected_epoch}, but this \
         target is at epoch {actual_epoch}"
    )]
    EpochMismatch {
        expected_epoch: u64,
        actual_epoch: u64,
    },

    #[error("epoch advance refused: the target epoch is already u64::MAX and cannot advance")]
    EpochExhausted,

    #[error(
        "pin refused: a pin must name the target's current epoch, but the caller named epoch \
         {requested_epoch} while this target is at epoch {actual_epoch}"
    )]
    PinEpochNotCurrent {
        requested_epoch: u64,
        actual_epoch: u64,
    },

    #[error("pin refused: ttl_ms must be at least 1, so that every pin has a deadline")]
    PinTtlZero,

    #[error(
        "pin refused: ttl_ms {requested_ms} exceeds the maximum pin lifetime of {max_ms} ms; a \
         pin is held across one fragment fetch, so a longer one is a bug rather than a lease"
    )]
    PinTtlTooLong { requested_ms: u64, max_ms: u64 },

    #[error(
        "pin table refused: the target could not read its own clock ({reason}), so it cannot \
         tell whether a pin has expired; no pin was released and nothing was collected"
    )]
    PinClockUnreadable { reason: String },

    #[error(
        "pin refused: the target's next pin id would exceed u64::MAX, so it cannot issue an id \
         that is provably unused"
    )]
    PinIdsExhausted,

    #[error(
        "epoch advance refused: {live_pins} unexpired pin(s) still name epoch {epoch}, and a \
         reader holding bytes of that epoch may not be fenced by an advance"
    )]
    EpochPinned { epoch: u64, live_pins: usize },

    #[error(
        "collectable delete of {id} refused: the fragment was written under epoch \
         {fragment_epoch}, which is not older than the collector's epoch {collector_epoch}"
    )]
    FragmentNotOlder {
        id: BlobId,
        fragment_epoch: u64,
        collector_epoch: u64,
    },

    #[error(
        "collectable delete of {id} refused: the fragment was written under epoch \
         {fragment_epoch}, which {live_pins} unexpired pin(s) still name; the collector's epoch \
         {collector_epoch} does not fence those readers"
    )]
    FragmentPinned {
        id: BlobId,
        fragment_epoch: u64,
        collector_epoch: u64,
        live_pins: usize,
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

    #[error("remote VDisk fence request refused with HTTP {status} ({code}): {detail}")]
    RemoteFenceRefused {
        status: u16,
        code: String,
        detail: String,
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
    /// silently defeat, `FencedByEpoch` says the fragment belongs to a different
    /// collector epoch and must not be deleted at all, and `StorageUnavailable`
    /// means the slot's outcome is
    /// unknown until it is recovered, so acknowledging the write somewhere else
    /// would hide a fault an operator has to see. If in doubt, this is `false`.
    ///
    /// The pin-table refusals (`EpochPinned`, `PinEpochNotCurrent`,
    /// `FragmentPinned`, `FragmentNotOlder`, `PinClockUnreadable`) are also
    /// `false`: each is a "stop this pass" answer about durable state, not a
    /// statement that the slot could not answer.
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

    // ---------------------------------------------------------------------
    // Target epochs: the durable, storage-side half of the GC fence.
    //
    // These cover only what a single target can prove by itself. They say
    // nothing about the RPC that carries the epoch to a remote target, about
    // reader pins, or about the collector that chooses when to advance - none of
    // which exists yet.
    // ---------------------------------------------------------------------

    #[test]
    fn the_epoch_advances_only_through_the_explicit_step() {
        let fs = SimFs::new(8001);
        let cfg = VolumeConfig::default();
        let id = BlobId::new(20, 1, 1, 1, 2, 0);
        let store = VolumeStore::open(fs.clone(), "epoch_step", cfg).unwrap();
        assert_eq!(store.epoch(), 0, "a fresh volume starts at epoch 0");

        // No other operation may move it. Every one of these is a mutation, and
        // an implicit advance would revoke a pass that is still entitled to
        // delete.
        store.put(id, b"payload").unwrap();
        let other = BlobId::new(20, 1, 2, 1, 2, 0);
        store.put(other, b"other").unwrap();
        store.delete(other).unwrap();
        store
            .collect_garbage_barrier(20, 1, 2, 1, u64::MAX)
            .unwrap();
        store.seal_and_compact().unwrap();
        assert_eq!(
            store.epoch(),
            0,
            "no write, delete, barrier or compaction may advance the epoch"
        );

        // Opening is not an advance either, including after a power cut.
        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs.clone(), "epoch_step", cfg).unwrap();
        assert_eq!(reopened.epoch(), 0, "open must not advance the epoch");

        // The one explicit step, and it is durable.
        assert_eq!(reopened.advance_epoch(0).unwrap(), 1);
        assert_eq!(reopened.epoch(), 1);
        let next = BlobId::new(20, 1, 3, 1, 2, 0);
        reopened.put(next, b"next").unwrap();
        assert_eq!(reopened.epoch(), 1, "a write does not advance the epoch");
        drop(reopened);
        fs.simulate_power_cut();
        let again = VolumeStore::open(fs, "epoch_step", cfg).unwrap();
        assert_eq!(again.epoch(), 1, "the advanced epoch is durable");
        assert_eq!(again.get(next).unwrap(), b"next");
    }

    #[test]
    fn each_fragment_records_the_epoch_it_was_written_under() {
        let fs = SimFs::new(8002);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "frag_epoch", cfg).unwrap();
        let first = BlobId::new(21, 1, 1, 1, 2, 0);
        let second = BlobId::new(21, 1, 1, 2, 2, 0);
        let third = BlobId::new(21, 1, 1, 3, 2, 0);
        store.put(first, b"first").unwrap();
        assert_eq!(store.fragment_epoch(first), Some(0));
        store.advance_epoch(0).unwrap();
        store.put(second, b"second").unwrap();
        store.advance_epoch(1).unwrap();
        store.put(third, b"third").unwrap();
        assert_eq!(store.fragment_epoch(second), Some(1));
        assert_eq!(store.fragment_epoch(third), Some(2));
        assert_eq!(
            store.fragment_epoch(BlobId::new(21, 9, 9, 9, 2, 0)),
            None,
            "a fragment the volume does not hold has no epoch"
        );

        // The stamp is on disk, not an in-memory counter: it is read back from
        // each fragment's own record.
        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "frag_epoch", cfg).unwrap();
        assert_eq!(reopened.epoch(), 2);
        assert_eq!(reopened.fragment_epoch(first), Some(0));
        assert_eq!(reopened.fragment_epoch(second), Some(1));
        assert_eq!(reopened.fragment_epoch(third), Some(2));
        assert_eq!(reopened.get(third).unwrap(), b"third");
    }

    #[test]
    fn a_fenced_delete_refuses_a_fragment_from_another_epoch() {
        let fs = SimFs::new(8003);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "fenced_delete", cfg).unwrap();
        let old = BlobId::new(22, 1, 1, 1, 2, 0);
        let new = BlobId::new(22, 1, 1, 2, 2, 0);
        store.put(old, b"old").unwrap();
        store.advance_epoch(0).unwrap();
        store.put(new, b"new").unwrap();

        // A delete carrying the new epoch must not unlink the older fragment.
        match store.delete_fenced(old, 1) {
            Err(BlobError::FencedByEpoch {
                id,
                recorded_epoch,
                expected_epoch,
            }) => {
                assert_eq!(id, old);
                assert_eq!(recorded_epoch, 0);
                assert_eq!(expected_epoch, 1);
            }
            other => panic!("expected FencedByEpoch, got {other:?}"),
        }
        assert_eq!(
            store.get(old).unwrap(),
            b"old",
            "a refused delete unlinks nothing"
        );
        // And the fence is equality, not "older is collectable": a stale epoch
        // must not unlink a fragment written under a newer one.
        assert!(matches!(
            store.delete_fenced(new, 0),
            Err(BlobError::FencedByEpoch {
                recorded_epoch: 1,
                expected_epoch: 0,
                ..
            })
        ));
        assert_eq!(store.get(new).unwrap(), b"new");

        // The epoch each fragment records is the one that deletes it.
        assert!(store.delete_fenced(new, 1).unwrap());
        assert!(!store.contains(new));
        assert!(store.delete_fenced(old, 0).unwrap());
        assert!(!store.contains(old));
        // Idempotent, like the unfenced delete.
        assert!(!store.delete_fenced(old, 0).unwrap());
    }

    #[test]
    fn a_refused_fenced_delete_leaves_no_durable_tombstone() {
        let fs = SimFs::new(8004);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "refused_delete", cfg).unwrap();
        let id = BlobId::new(23, 1, 1, 1, 2, 0);
        store.put(id, b"keep").unwrap();
        store.advance_epoch(0).unwrap(); // the target is at 1, the fragment at 0

        assert!(matches!(
            store.delete_fenced(id, 1),
            Err(BlobError::FencedByEpoch { .. })
        ));
        // A refused delete must not install its permanent fence: the id is still
        // writable, so no tombstone was appended for a delete that was not
        // entitled to happen.
        store.put(id, b"keep").unwrap();

        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs.clone(), "refused_delete", cfg).unwrap();
        assert_eq!(reopened.get(id).unwrap(), b"keep");
        assert_eq!(reopened.fragment_epoch(id), Some(0));
        assert_eq!(
            reopened.epoch(),
            1,
            "a refused delete does not touch the epoch"
        );
        // The entitlement was what was missing, not the delete: with the recorded
        // epoch the same fragment is removed, and that removal is durable.
        assert!(reopened.delete_fenced(id, 0).unwrap());
        drop(reopened);
        fs.simulate_power_cut();
        let durable = VolumeStore::open(fs, "refused_delete", cfg).unwrap();
        assert!(!durable.contains(id));
        assert!(matches!(
            durable.put(id, b"keep"),
            Err(BlobError::Tombstoned(_))
        ));
    }

    #[test]
    fn a_torn_epoch_advance_cannot_reset_the_durable_epoch() {
        // The epoch is what a collector fences its pass with. Losing it during an
        // update would silently re-open the epoch a superseded collector still
        // holds. It is therefore never rewritten in place: an advance appends a
        // record, and recovery takes the maximum over every record, so a torn
        // append can only fail to raise the value.
        let fs = SimFs::new(8005);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "torn_epoch", cfg).unwrap();
        let id = BlobId::new(24, 1, 1, 1, 2, 0);
        store.put(id, b"epoch zero").unwrap();
        assert_eq!(store.advance_epoch(0).unwrap(), 1);
        // No fragment is written under epoch 1 or 2, so only the epoch records
        // carry them. A recovery that ignores them, or that reinitialises the
        // epoch, reports 0 below instead of 2.
        assert_eq!(store.advance_epoch(1).unwrap(), 2);

        fs.set_faults(FaultConfig {
            seed: 31,
            torn_write_prob: 1.0,
            ..Default::default()
        });
        let error = store
            .advance_epoch(2)
            .expect_err("a torn epoch write must not be acknowledged");
        assert!(matches!(error, BlobError::Io(_)), "got {error:?}");
        // An epoch whose write was not proven is not published.
        assert_eq!(store.epoch(), 2);
        fs.set_faults(FaultConfig::default());

        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs.clone(), "torn_epoch", cfg).unwrap();
        assert_eq!(
            reopened.epoch(),
            2,
            "a torn advance must not lower the durable epoch"
        );
        assert_eq!(reopened.fragment_epoch(id), Some(0));
        assert_eq!(reopened.get(id).unwrap(), b"epoch zero");
        // The torn attempt left nothing usable behind, and the next step still
        // starts from the value the volume actually holds.
        assert_eq!(reopened.advance_epoch(2).unwrap(), 3);
    }

    #[test]
    fn a_failed_epoch_sync_publishes_nothing_and_recovers_to_the_durable_value() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(8006);
            let cfg = VolumeConfig::default();
            let store = VolumeStore::open(fs.clone(), "epoch_sync", cfg).unwrap();
            let id = BlobId::new(25, 1, 1, 1, 2, 0);
            store.put(id, b"payload").unwrap();
            assert_eq!(store.advance_epoch(0).unwrap(), 1);

            fs.set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            assert!(matches!(store.advance_epoch(1), Err(BlobError::Io(_))));
            // The caller is never told an epoch the target could not prove
            // durable, and the volume does not build on the ambiguous append.
            assert_eq!(store.epoch(), 1, "fault {fault:?}");
            assert!(matches!(
                store.advance_epoch(1),
                Err(BlobError::StorageUnavailable)
            ));
            assert!(matches!(
                store.put(BlobId::new(25, 1, 1, 2, 2, 0), b"x"),
                Err(BlobError::StorageUnavailable)
            ));
            fs.set_faults(FaultConfig::default());
            drop(store);
            fs.simulate_power_cut();

            let reopened = VolumeStore::open(fs, "epoch_sync", cfg).unwrap();
            // The acknowledged epoch is never lost. A sync that failed after
            // persisting leaves the higher value behind: that over-advances, so it
            // fences more, never less, and the collector that was refused stops.
            let expected = if fault == SyncFault::AfterPersist {
                2
            } else {
                1
            };
            assert_eq!(reopened.epoch(), expected, "fault {fault:?}");
            // Either way the fragment keeps its own epoch, so a delete carrying
            // the epoch the failed advance tried to reach is refused.
            assert_eq!(reopened.fragment_epoch(id), Some(0));
            assert!(matches!(
                reopened.delete_fenced(id, 1),
                Err(BlobError::FencedByEpoch { .. })
            ));
            assert_eq!(reopened.get(id).unwrap(), b"payload");
        }
    }

    #[test]
    fn compaction_preserves_the_epoch_and_each_fragments_epoch() {
        let fs = SimFs::new(8007);
        let cfg = VolumeConfig {
            max_segment_bytes: 160,
            ..Default::default()
        };
        let store = VolumeStore::open(fs.clone(), "epoch_compact", cfg).unwrap();
        let old = BlobId::new(26, 1, 1, 1, 2, 0);
        let new = BlobId::new(26, 1, 1, 2, 2, 0);
        store.put(old, &[b'o'; 60]).unwrap();
        store.advance_epoch(0).unwrap();
        store.put(new, &[b'n'; 60]).unwrap();
        // Two advances with no fragment of their own: only an epoch record
        // carries them, and the segment holding it is about to be rewritten.
        store.advance_epoch(1).unwrap();
        store.advance_epoch(2).unwrap();
        // Roll the active segment so the fragments and epoch records above become
        // sealed and are copied into replacement segments.
        store
            .put(BlobId::new(26, 1, 1, 3, 2, 0), &[b'z'; 60])
            .unwrap();
        assert!(store.compact_sealed_segments().unwrap() > 0);

        assert_eq!(store.epoch(), 3);
        assert_eq!(store.fragment_epoch(old), Some(0));
        assert_eq!(store.fragment_epoch(new), Some(1));
        // Compaction rewrites bytes; it does not create a new fragment, so the
        // epoch a fenced delete compares against is unchanged.
        assert!(matches!(
            store.delete_fenced(old, 1),
            Err(BlobError::FencedByEpoch { .. })
        ));
        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "epoch_compact", cfg).unwrap();
        assert_eq!(reopened.epoch(), 3, "compaction must not lower the epoch");
        assert_eq!(reopened.fragment_epoch(old), Some(0));
        assert_eq!(reopened.fragment_epoch(new), Some(1));
        assert_eq!(reopened.get(old).unwrap(), vec![b'o'; 60]);
        assert!(reopened.delete_fenced(old, 0).unwrap());
    }

    #[test]
    fn sealing_and_compacting_away_the_last_epoch_record_keeps_the_epoch() {
        // A volume whose last operation was an epoch advance has no fragment
        // carrying that epoch: the epoch record is the only copy. Sealing the
        // active segment and compacting it away must therefore rewrite the epoch,
        // or the next open silently reports the epoch of the newest fragment
        // instead - the reset this increment exists to prevent.
        let fs = SimFs::new(8010);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "epoch_seal", cfg).unwrap();
        let id = BlobId::new(28, 1, 1, 1, 2, 0);
        store.put(id, b"fragment at epoch zero").unwrap();
        assert_eq!(store.advance_epoch(0).unwrap(), 1);
        assert_eq!(store.advance_epoch(1).unwrap(), 2);
        // The active segment now holds one fragment at epoch 0 and two epoch
        // records. Sealing rolls it and compaction rewrites it, reclaiming the
        // superseded epoch record.
        assert!(store.seal_and_compact().unwrap() > 0);
        assert_eq!(store.epoch(), 2);
        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "epoch_seal", cfg).unwrap();
        assert_eq!(
            reopened.epoch(),
            2,
            "compacting away the last epoch record must not lower the epoch"
        );
        assert_eq!(reopened.get(id).unwrap(), b"fragment at epoch zero");
        assert_eq!(reopened.fragment_epoch(id), Some(0));
    }

    #[test]
    fn a_legacy_record_without_an_epoch_is_readable_as_epoch_zero() {
        // A volume written before target epochs existed has 33-byte headers. It
        // must keep opening, and its fragments belong to epoch 0 - the epoch every
        // volume starts at - not to an epoch nobody can name.
        let fs = SimFs::new(8008);
        let cfg = VolumeConfig::default();
        let id = BlobId::new(27, 1, 1, 1, 2, 0);
        let payload = b"written before epochs existed";
        {
            fs.create_dir_all("legacy_epoch").unwrap();
            let file = fs.open_rw("legacy_epoch/vol_00000001.dat", true).unwrap();
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"YADBVOL1");
            let mut hdr = [0u8; 33];
            hdr[0..4].copy_from_slice(&0x5942_5243u32.to_le_bytes()); // V1 magic
            hdr[4] = 1; // REC_KIND_PUT
            hdr[5..21].copy_from_slice(&id.to_be_bytes());
            hdr[21..25].copy_from_slice(&(payload.len() as u32).to_le_bytes());
            hdr[25..29].copy_from_slice(&crc32fast::hash(payload).to_le_bytes());
            let hdr_crc = crc32fast::hash(&hdr[0..29]);
            hdr[29..33].copy_from_slice(&hdr_crc.to_le_bytes());
            bytes.extend_from_slice(&hdr);
            bytes.extend_from_slice(payload);
            file.write_at(0, &bytes).unwrap();
            file.sync_data().unwrap();
            fs.sync_dir("legacy_epoch").unwrap();
        }

        let store = VolumeStore::open(fs.clone(), "legacy_epoch", cfg).unwrap();
        assert_eq!(store.epoch(), 0);
        assert_eq!(store.get(id).unwrap(), payload);
        assert_eq!(store.fragment_epoch(id), Some(0));
        assert!(matches!(
            store.delete_fenced(id, 1),
            Err(BlobError::FencedByEpoch {
                recorded_epoch: 0,
                expected_epoch: 1,
                ..
            })
        ));
        assert_eq!(store.get(id).unwrap(), payload);
        assert!(store.delete_fenced(id, 0).unwrap());
        drop(store);

        // The real upgrade path is: open a legacy volume, append current records
        // to the same segment, restart. Recovery has to parse the 33-byte record
        // and land exactly on the 41-byte one that follows, or it truncates the
        // tail and loses the tombstone - which would resurrect a deleted blob.
        let reopened = VolumeStore::open(fs.clone(), "legacy_epoch", cfg).unwrap();
        assert_eq!(reopened.epoch(), 0);
        assert!(
            !reopened.contains(id),
            "the tombstone appended after a legacy record must survive reopen"
        );
        assert!(matches!(
            reopened.put(id, payload),
            Err(BlobError::Tombstoned { .. })
        ));
    }

    #[test]
    fn an_epoch_advance_naming_a_stale_epoch_is_refused() {
        let fs = SimFs::new(8009);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "stale_advance", cfg).unwrap();
        assert_eq!(store.advance_epoch(0).unwrap(), 1);
        // A collector that fenced its pass at epoch 0 has been superseded: it must
        // not move the epoch past the newer one.
        match store.advance_epoch(0) {
            Err(BlobError::EpochMismatch {
                expected_epoch,
                actual_epoch,
            }) => assert_eq!((expected_epoch, actual_epoch), (0, 1)),
            other => panic!("expected EpochMismatch, got {other:?}"),
        }
        assert_eq!(store.epoch(), 1);
        // An advance ahead of the target is refused too: the step is exactly one,
        // from the epoch the caller read.
        assert!(matches!(
            store.advance_epoch(5),
            Err(BlobError::EpochMismatch {
                expected_epoch: 5,
                actual_epoch: 1
            })
        ));
        assert_eq!(store.epoch(), 1);
        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "stale_advance", cfg).unwrap();
        assert_eq!(reopened.epoch(), 1, "a refused advance wrote nothing");
        assert!(
            reopened.live_ids().is_empty(),
            "an epoch record is not a fragment"
        );
        assert_eq!(reopened.advance_epoch(1).unwrap(), 2);
    }

    // ---------------------------------------------------------------------
    // Reader pins: the target-side half of "no unexpired pin names the epoch".
    //
    // These cover only what a single target can prove by itself. There is no
    // RPC that carries a pin, no reader that takes one before a fetch, and no
    // collector that consults the table - none of that exists yet.
    // ---------------------------------------------------------------------

    #[test]
    fn a_pin_is_durable_and_names_the_current_epoch() {
        let fs = SimFs::new(9001);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_durable", cfg).unwrap();
        let before = store.epoch();
        let grant = store.pin(before, 60_000).unwrap();
        assert_eq!(grant.epoch, before);
        assert!(
            grant.expires_at_ms > 0,
            "a pin always carries a real deadline"
        );

        let live = store.live_pins().unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].pin_id, grant.pin_id);
        assert_eq!(live[0].epoch, before);
        assert_eq!(live[0].expires_at_ms, grant.expires_at_ms);

        // The pin is a record, not a field: a restart rebuilds it, with the same
        // id, epoch and deadline, and it still stops an advance.
        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "pin_durable", cfg).unwrap();
        let live = reopened.live_pins().unwrap();
        assert_eq!(
            live,
            vec![PinView {
                pin_id: grant.pin_id,
                epoch: before,
                expires_at_ms: grant.expires_at_ms,
            }],
            "a pin that was acknowledged must survive a power cut"
        );
        assert!(matches!(
            reopened.advance_epoch(before),
            Err(BlobError::EpochPinned { epoch, live_pins: 1 }) if epoch == before
        ));
    }

    #[test]
    fn a_pin_naming_any_epoch_but_the_current_one_is_refused() {
        let fs = SimFs::new(9002);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_epoch", cfg).unwrap();

        // A pin for a newer epoch names an epoch that does not exist yet.
        match store.pin(1, 1000) {
            Err(BlobError::PinEpochNotCurrent {
                requested_epoch,
                actual_epoch,
            }) => assert_eq!((requested_epoch, actual_epoch), (1, 0)),
            other => panic!("expected PinEpochNotCurrent, got {other:?}"),
        }

        store.advance_epoch(0).unwrap();
        // A pin for an older epoch names an epoch the collector may already have
        // advanced past, so it would prove nothing.
        match store.pin(0, 1000) {
            Err(BlobError::PinEpochNotCurrent {
                requested_epoch,
                actual_epoch,
            }) => assert_eq!((requested_epoch, actual_epoch), (0, 1)),
            other => panic!("expected PinEpochNotCurrent, got {other:?}"),
        }
        assert!(
            store.live_pins().unwrap().is_empty(),
            "a refused pin appends no record"
        );
        assert_eq!(store.epoch(), 1);

        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "pin_epoch", cfg).unwrap();
        assert!(reopened.live_pins().unwrap().is_empty());
        assert_eq!(reopened.epoch(), 1, "a refused pin does not move the epoch");
        // The current epoch is the one that works, so the refusals above were
        // about the epoch and not a blanket rejection.
        assert_eq!(reopened.pin(1, 1000).unwrap().epoch, 1);
    }

    #[test]
    fn a_pin_with_no_deadline_or_an_unbounded_one_is_refused() {
        let fs = SimFs::new(9003);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_ttl", cfg).unwrap();

        assert!(matches!(store.pin(0, 0), Err(BlobError::PinTtlZero)));
        match store.pin(0, MAX_PIN_TTL_MS + 1) {
            Err(BlobError::PinTtlTooLong {
                requested_ms,
                max_ms,
            }) => {
                assert_eq!(requested_ms, MAX_PIN_TTL_MS + 1);
                assert_eq!(max_ms, MAX_PIN_TTL_MS);
            }
            other => panic!("expected PinTtlTooLong, got {other:?}"),
        }
        assert!(
            store.live_pins().unwrap().is_empty(),
            "a refused TTL appends no record"
        );
        // Nothing was left behind that could block a pass.
        assert_eq!(store.advance_epoch(0).unwrap(), 1);

        // The documented maximum itself is accepted: the bound is a limit, not a
        // blanket refusal of long pins.
        let grant = store.pin(1, MAX_PIN_TTL_MS).unwrap();
        assert!(grant.expires_at_ms > 0);
        assert_eq!(store.live_pins().unwrap().len(), 1);
        assert!(store.unpin(grant.pin_id).unwrap());
    }

    #[test]
    fn an_unexpired_pin_stops_the_epoch_advance_and_names_the_count() {
        let fs = SimFs::new(9004);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_advance", cfg).unwrap();
        let first = store.pin(0, 60_000).unwrap();
        let second = store.pin(0, 60_000).unwrap();
        assert_ne!(first.pin_id, second.pin_id);

        match store.advance_epoch(0) {
            Err(BlobError::EpochPinned { epoch, live_pins }) => {
                assert_eq!(epoch, 0);
                assert_eq!(live_pins, 2);
            }
            other => panic!("expected EpochPinned, got {other:?}"),
        }
        assert_eq!(store.epoch(), 0, "a refused advance writes nothing");

        // The existing expected-epoch refusal is kept, and it is checked first: a
        // superseded collector is told it is superseded, not that the target is
        // pinned.
        assert!(matches!(
            store.advance_epoch(9),
            Err(BlobError::EpochMismatch {
                expected_epoch: 9,
                actual_epoch: 0
            })
        ));

        assert!(store.unpin(first.pin_id).unwrap());
        // Releasing twice is a no-op, not a second release.
        assert!(!store.unpin(first.pin_id).unwrap());
        assert!(matches!(
            store.advance_epoch(0),
            Err(BlobError::EpochPinned { live_pins: 1, .. })
        ));
        assert!(store.unpin(second.pin_id).unwrap());
        assert_eq!(store.advance_epoch(0).unwrap(), 1);

        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "pin_advance", cfg).unwrap();
        assert_eq!(reopened.epoch(), 1);
        assert!(
            reopened.live_pins().unwrap().is_empty(),
            "a released pin is not resurrected by recovery"
        );
    }

    #[test]
    fn an_unpin_of_an_unknown_or_expired_pin_releases_nothing() {
        let fs = SimFs::new(9005);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_unpin", cfg).unwrap();

        // Unknown ids are not errors: an unpin may arrive twice, or for a pin the
        // target already forgot.
        assert!(!store.unpin(PinId::from_wire(4242)).unwrap());

        let grant = store.pin(0, 1).unwrap();
        // One millisecond of lifetime, then wait it out. The pin is expired by the
        // clock, not by an unpin.
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(
            store.live_pins().unwrap().is_empty(),
            "an expired pin is not live"
        );
        assert!(
            !store.unpin(grant.pin_id).unwrap(),
            "releasing an expired pin removes nothing live"
        );
        // Nothing was extended or resurrected: the advance is admitted.
        assert_eq!(store.advance_epoch(0).unwrap(), 1);

        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "pin_unpin", cfg).unwrap();
        assert!(reopened.live_pins().unwrap().is_empty());
        assert_eq!(reopened.epoch(), 1);
    }

    #[test]
    fn pin_liveness_is_strictly_before_the_deadline_and_a_broken_clock_refuses() {
        // Production used to compute `map_or(0, ...)`, i.e. a broken clock read as
        // "now", which makes every pin look expired and lets a collector advance
        // past a reader. That is the direction that deletes, so it is asserted
        // against here.
        assert!(volume::pin_is_live(1000, &Ok(999)).unwrap());
        // Boundary: the deadline is exclusive. At the deadline the pin is gone.
        assert!(!volume::pin_is_live(1000, &Ok(1000)).unwrap());
        assert!(!volume::pin_is_live(1000, &Ok(1001)).unwrap());
        match volume::pin_is_live(1000, &Err("clock stepped".into())) {
            Err(BlobError::PinClockUnreadable { reason }) => {
                assert!(reason.contains("clock stepped"), "{reason}");
            }
            other => panic!("an unreadable clock must refuse, got {other:?}"),
        }
    }

    #[test]
    fn recovery_drops_expired_pins_and_keeps_the_rest_without_inventing_any() {
        let records = vec![
            volume::PinRecord::Pin {
                pin_id: PinId::from_wire(3),
                epoch: 1,
                expires_at_ms: 500,
            },
            volume::PinRecord::Pin {
                pin_id: PinId::from_wire(4),
                epoch: 2,
                expires_at_ms: 5000,
            },
            // Releasing 3 drops it from the live set, but its id is still spent:
            // an id must not be reissued while a record naming it survives.
            volume::PinRecord::Unpin {
                pin_id: PinId::from_wire(3),
            },
            // An unpin for an id that never had a PIN invents nothing.
            volume::PinRecord::Unpin {
                pin_id: PinId::from_wire(9),
            },
        ];

        let (pins, next) = volume::rebuild_pins(records.clone(), &Ok(100)).unwrap();
        assert_eq!(pins.len(), 1, "only the unexpired, unreleased pin survives");
        assert_eq!(pins.get(&PinId::from_wire(4)).unwrap().epoch, 2);
        assert_eq!(pins.get(&PinId::from_wire(4)).unwrap().expires_at_ms, 5000);
        assert_eq!(next, 10, "the next id is past every id in the log");

        // At the deadline the remaining pin is dropped, and the id is still spent.
        let (pins, next) = volume::rebuild_pins(records.clone(), &Ok(5000)).unwrap();
        assert!(pins.is_empty(), "now == expires_at_ms is expired");
        assert_eq!(next, 10);

        // A broken clock refuses instead of dropping every pin.
        assert!(matches!(
            volume::rebuild_pins(records, &Err("clock stepped".into())),
            Err(BlobError::PinClockUnreadable { .. })
        ));
    }

    #[test]
    fn a_torn_pin_write_never_becomes_a_live_pin() {
        let fs = SimFs::new(9006);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_torn", cfg).unwrap();
        let id = BlobId::new(30, 1, 1, 1, 2, 0);
        store.put(id, b"reader bytes").unwrap();

        fs.set_faults(FaultConfig {
            seed: 41,
            torn_write_prob: 1.0,
            ..Default::default()
        });
        let error = store.pin(0, 60_000).expect_err("a torn pin must fail");
        assert!(matches!(error, BlobError::Io(_)), "got {error:?}");
        assert!(
            store.live_pins().unwrap().is_empty(),
            "a pin whose record was not written is not acknowledged"
        );
        fs.set_faults(FaultConfig::default());

        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "pin_torn", cfg).unwrap();
        // The torn record is a tail, so recovery truncates it. A reader that got
        // an error took no pin, and a half record must not be read as one with a
        // garbage deadline.
        assert!(reopened.live_pins().unwrap().is_empty());
        assert_eq!(
            reopened.advance_epoch(0).unwrap(),
            1,
            "a torn pin must not block the advance it never fenced"
        );
        assert_eq!(reopened.get(id).unwrap(), b"reader bytes");
    }

    #[test]
    fn a_pin_is_acknowledged_only_after_its_record_is_durable() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(9007);
            let cfg = VolumeConfig::default();
            let store = VolumeStore::open(fs.clone(), "pin_sync", cfg).unwrap();
            fs.set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            assert!(
                matches!(store.pin(0, 60_000), Err(BlobError::Io(_))),
                "fault {fault:?}"
            );
            // An ambiguous append is not built on: the volume refuses further
            // mutations until it is reopened.
            assert!(matches!(
                store.pin(0, 60_000),
                Err(BlobError::StorageUnavailable)
            ));
            fs.set_faults(FaultConfig::default());
            drop(store);
            fs.simulate_power_cut();

            let reopened = VolumeStore::open(fs, "pin_sync", cfg).unwrap();
            let live = reopened.live_pins().unwrap();
            if fault == SyncFault::AfterPersist {
                // The record reached the platter before the sync reported an
                // error. Keeping it is the safe direction: it blocks collection
                // until its TTL runs out, and the caller was told the pin failed.
                assert_eq!(live.len(), 1, "fault {fault:?}");
                assert!(matches!(
                    reopened.advance_epoch(0),
                    Err(BlobError::EpochPinned { live_pins: 1, .. })
                ));
            } else {
                assert!(live.is_empty(), "fault {fault:?}");
                assert_eq!(reopened.advance_epoch(0).unwrap(), 1, "fault {fault:?}");
            }
        }
    }

    #[test]
    fn a_live_pin_refuses_a_collectable_delete_of_its_epoch() {
        let fs = SimFs::new(9008);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_delete", cfg).unwrap();
        let id = BlobId::new(31, 1, 1, 1, 2, 0);
        store.put(id, b"held by a reader").unwrap();
        assert_eq!(store.fragment_epoch(id), Some(0));
        let grant = store.pin(0, 60_000).unwrap();

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
            other => panic!("expected FragmentPinned, got {other:?}"),
        }
        assert_eq!(store.get(id).unwrap(), b"held by a reader");
        // The refusal happened before any record was appended, so the id is not
        // tombstoned and a later write of the same bytes still works.
        store.put(id, b"held by a reader").unwrap();

        assert!(store.unpin(grant.pin_id).unwrap());
        assert!(
            store.delete_collectable(id, 1).unwrap(),
            "with the pin released the older fragment is collectable"
        );
        assert!(!store.contains(id));
        assert_eq!(store.fragment_epoch(id), None);
        // Idempotent, like every other delete.
        assert!(!store.delete_collectable(id, 1).unwrap());
    }

    #[test]
    fn a_collectable_delete_needs_the_fragment_to_be_older_than_the_collector() {
        let fs = SimFs::new(9009);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_older", cfg).unwrap();
        let id = BlobId::new(32, 1, 1, 1, 2, 0);
        store.advance_epoch(0).unwrap();
        store.put(id, b"current epoch").unwrap();
        assert_eq!(store.fragment_epoch(id), Some(1));

        // Equality is not "older": a collector that has not advanced past this
        // fragment may not collect it, pinned or not.
        for collector_epoch in [1, 0] {
            match store.delete_collectable(id, collector_epoch) {
                Err(BlobError::FragmentNotOlder {
                    id: named,
                    fragment_epoch,
                    collector_epoch: named_collector,
                }) => {
                    assert_eq!(named, id);
                    assert_eq!(fragment_epoch, 1);
                    assert_eq!(named_collector, collector_epoch);
                }
                other => panic!("expected FragmentNotOlder, got {other:?}"),
            }
        }
        assert_eq!(store.get(id).unwrap(), b"current epoch");
        // No tombstone was left by the refusals.
        store.put(id, b"current epoch").unwrap();

        // A fragment that is already absent is not an error and is tombstoned, so
        // a late PUT cannot resurrect it.
        let absent = BlobId::new(32, 1, 2, 1, 2, 0);
        assert!(!store.delete_collectable(absent, 5).unwrap());

        assert!(store.delete_collectable(id, 2).unwrap());
        assert!(!store.contains(id));
    }

    /// A reader can only pin the target's *current* epoch, so that pin must
    /// protect every fragment older than the collector, not just the fragments
    /// written under the pinned epoch.
    ///
    /// The previous version of this test asserted the opposite - that a pin on
    /// epoch 1 leaves the epoch-0 fragment collectable - and passed. That is the
    /// hole: the reader pins epoch 1, then fetches a fragment written at epoch 0,
    /// and the collector deletes it underneath the live pin.
    #[test]
    fn a_live_pin_on_the_current_epoch_protects_every_older_fragment() {
        let fs = SimFs::new(9013);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_scope", cfg).unwrap();
        let old = BlobId::new(35, 1, 1, 1, 2, 0);
        store.put(old, b"epoch zero").unwrap();
        store.advance_epoch(0).unwrap();
        let current = BlobId::new(35, 1, 1, 2, 2, 0);
        store.put(current, b"epoch one").unwrap();

        // The pin the read path can take: the epoch the target is on now.
        let grant = store.pin(1, 60_000).unwrap();
        // Pinning an older epoch is impossible, which is why the check below
        // cannot be narrowed to "pins that name the fragment's epoch".
        assert!(matches!(
            store.pin(0, 60_000),
            Err(BlobError::PinEpochNotCurrent {
                requested_epoch: 0,
                actual_epoch: 1,
            })
        ));

        // Both the fragment written at the pinned epoch and the one written
        // before it are protected by the same live pin.
        for (id, fragment_epoch) in [(old, 0), (current, 1)] {
            assert!(
                matches!(
                    store.delete_collectable(id, 2),
                    Err(BlobError::FragmentPinned {
                        fragment_epoch: named_fragment,
                        collector_epoch: 2,
                        live_pins: 1,
                        ..
                    }) if named_fragment == fragment_epoch
                ),
                "a live pin on the current epoch must protect the epoch-{fragment_epoch} fragment"
            );
        }
        assert_eq!(store.get(old).unwrap(), b"epoch zero");
        assert_eq!(store.get(current).unwrap(), b"epoch one");

        assert!(store.unpin(grant.pin_id).unwrap());
        // With no pin live, both are older than a collector at epoch 2 and both
        // are collectable: the refusal above was the pin, not the epoch order.
        assert!(store.delete_collectable(old, 2).unwrap());
        assert!(store.delete_collectable(current, 2).unwrap());
        assert!(!store.contains(old));
        assert!(!store.contains(current));
    }

    #[test]
    fn delete_fenced_stays_strict_where_delete_collectable_is_older_and_unpinned() {
        let fs = SimFs::new(9010);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_two_deletes", cfg).unwrap();
        let old = BlobId::new(33, 1, 1, 1, 2, 0);
        store.put(old, b"epoch zero").unwrap();
        store.advance_epoch(0).unwrap();

        // The same call that the strict form refuses, the collectable form admits.
        // That difference is the whole point of the epoch relaxation.
        assert!(matches!(
            store.delete_fenced(old, 1),
            Err(BlobError::FencedByEpoch {
                recorded_epoch: 0,
                expected_epoch: 1,
                ..
            })
        ));
        assert_eq!(store.get(old).unwrap(), b"epoch zero");
        assert!(store.delete_collectable(old, 1).unwrap());
        assert!(!store.contains(old));

        // In the other direction, strict equality deletes a current fragment that
        // the collectable form refuses.
        let current = BlobId::new(33, 1, 1, 2, 2, 0);
        store.put(current, b"epoch one").unwrap();
        assert!(matches!(
            store.delete_collectable(current, 1),
            Err(BlobError::FragmentNotOlder {
                fragment_epoch: 1,
                collector_epoch: 1,
                ..
            })
        ));
        assert!(store.delete_fenced(current, 1).unwrap());
        assert!(!store.contains(current));
    }

    #[test]
    fn compaction_keeps_unexpired_pins_and_never_resurrects_an_expired_one() {
        let fs = SimFs::new(9011);
        let cfg = VolumeConfig {
            max_segment_bytes: 300,
            ..Default::default()
        };
        let store = VolumeStore::open(fs.clone(), "pin_compact", cfg).unwrap();
        let grant = store.pin(0, 60_000).unwrap();
        // One fragment that is deleted (so compaction has something to reclaim)
        // and enough live ones to roll the segment that holds the pin record.
        let dead = BlobId::new(34, 1, 1, 1, 2, 0);
        store.put(dead, &[b'd'; 100]).unwrap();
        store
            .put(BlobId::new(34, 1, 1, 2, 2, 0), &[b'l'; 100])
            .unwrap();
        store.delete(dead).unwrap();
        for step in 3..=4 {
            store
                .put(BlobId::new(34, 1, 1, step, 2, 0), &[b'l'; 100])
                .unwrap();
        }
        assert!(
            store.seal_and_compact().unwrap() > 0,
            "the deleted fragment's bytes must be reclaimed, so the replacement \
             really was rewritten"
        );
        assert_eq!(
            store.live_pins().unwrap(),
            vec![PinView {
                pin_id: grant.pin_id,
                epoch: 0,
                expires_at_ms: grant.expires_at_ms,
            }],
            "compaction must not drop a live pin"
        );
        assert!(matches!(
            store.advance_epoch(0),
            Err(BlobError::EpochPinned { live_pins: 1, .. })
        ));

        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs.clone(), "pin_compact", cfg).unwrap();
        assert_eq!(
            reopened.live_pins().unwrap().len(),
            1,
            "the replacement segment must carry the live pin"
        );
        assert!(matches!(
            reopened.advance_epoch(0),
            Err(BlobError::EpochPinned { live_pins: 1, .. })
        ));

        // An expired pin is dead weight: compaction may drop its record, and it
        // must never be rewritten with a fresh deadline. Rolling the segment that
        // holds it is what gives compaction the chance to get this wrong.
        let short = reopened.pin(0, 1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(!reopened.unpin(short.pin_id).unwrap());
        for step in 5..=6 {
            reopened
                .put(BlobId::new(34, 1, 1, step, 2, 0), &[b'l'; 100])
                .unwrap();
        }
        reopened.seal_and_compact().unwrap();
        let live = reopened.live_pins().unwrap();
        assert_eq!(live.len(), 1, "only the long pin is still live");
        assert_eq!(live[0].pin_id, grant.pin_id);
        assert!(
            live.iter().all(|pin| pin.pin_id != short.pin_id),
            "an expired pin must not be rewritten as a live one"
        );

        // The long pin from before is still live, so release it to reach the
        // advance.
        assert!(reopened.unpin(grant.pin_id).unwrap());
        assert_eq!(reopened.advance_epoch(0).unwrap(), 1);
        drop(reopened);
        fs.simulate_power_cut();
        let durable = VolumeStore::open(fs, "pin_compact", cfg).unwrap();
        assert!(durable.live_pins().unwrap().is_empty());
        assert_eq!(durable.epoch(), 1);
    }

    #[test]
    fn pin_table_refusals_are_not_slot_unavailability() {
        // Every one of these is a statement about durable state, not "this slot
        // could not answer". A caller that treated one as unavailable would
        // place the same write or delete on another slot and bypass the fence.
        let id = BlobId::new(36, 1, 1, 1, 2, 0);
        let errors = [
            BlobError::EpochPinned {
                epoch: 3,
                live_pins: 2,
            },
            BlobError::PinEpochNotCurrent {
                requested_epoch: 2,
                actual_epoch: 3,
            },
            BlobError::FragmentPinned {
                id,
                fragment_epoch: 2,
                collector_epoch: 3,
                live_pins: 1,
            },
            BlobError::FragmentNotOlder {
                id,
                fragment_epoch: 3,
                collector_epoch: 3,
            },
            BlobError::PinClockUnreadable {
                reason: "clock stepped".into(),
            },
            BlobError::PinTtlZero,
            BlobError::PinTtlTooLong {
                requested_ms: 10,
                max_ms: 5,
            },
        ];
        for error in errors {
            assert!(!error.is_unavailable(), "{error:?} must not be retryable");
        }
    }

    #[test]
    fn a_released_pin_id_is_not_handed_out_again_after_reopen() {
        let fs = SimFs::new(9012);
        let cfg = VolumeConfig::default();
        let store = VolumeStore::open(fs.clone(), "pin_ids", cfg).unwrap();
        let first = store.pin(0, 60_000).unwrap();
        assert!(store.unpin(first.pin_id).unwrap());
        drop(store);
        fs.simulate_power_cut();

        let reopened = VolumeStore::open(fs, "pin_ids", cfg).unwrap();
        let second = reopened.pin(0, 60_000).unwrap();
        assert!(
            second.pin_id.get() > first.pin_id.get(),
            "ids come from the durable log, so a reopened target cannot reissue one \
             a live reader may still hold"
        );
        // A late release of the old id must not touch the new pin.
        assert!(!reopened.unpin(first.pin_id).unwrap());
        assert_eq!(reopened.live_pins().unwrap().len(), 1);
        assert_eq!(reopened.live_pins().unwrap()[0].pin_id, second.pin_id);
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
        // Record layout: PUT "deleted" (header + 7), TOMBSTONE, PUT "durable"
        // (header + 7), PUT "repair-me" (header + 9). Damage the first byte of
        // the first payload and of the last payload, never a header.
        let hdr = crate::volume::RECORD_HEADER_LEN;
        fs.corrupt_byte("bitrot_recovery/vol_00000001.dat", 8 + hdr)
            .unwrap();
        fs.corrupt_byte(
            "bitrot_recovery/vol_00000001.dat",
            8 + (hdr + 7) + hdr + (hdr + 7) + hdr,
        )
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
        // tombstone and resurrect the deleted blob. Record 1 is a PUT of 7 bytes.
        let path = "hdr_bitrot/vol_00000001.dat";
        fs.corrupt_byte(path, 8 + crate::volume::RECORD_HEADER_LEN + 7 + 4)
            .unwrap();
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
            // Inside the payload, not the header: a header byte would truncate
            // the tail instead of corrupting a readable fragment.
            fs.corrupt_byte(
                &format!("az0/{seg}"),
                8 + crate::volume::RECORD_HEADER_LEN + 4,
            )
            .unwrap();
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
        fs.corrupt_byte(
            &format!("{domain}/{}", files[0]),
            8 + crate::volume::RECORD_HEADER_LEN + 4,
        )
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
        /// HTTP status the pin route answers with while non-zero. Models a
        /// target that refuses to be read: a read that ignored the refusal and
        /// fetched anyway would report bytes the fence never admitted.
        pin_status: Arc<std::sync::atomic::AtomicU16>,
        seen: Arc<std::sync::Mutex<Vec<String>>>,
        server: Option<std::thread::JoinHandle<()>>,
    }

    /// A minimal JSON response for the mock's fence routes.
    fn json_response(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
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
            let pin_status = Arc::new(std::sync::atomic::AtomicU16::new(0));
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            // The read path pins the target's epoch before every fetch, so the
            // mock has to serve that surface or it models a target that refuses
            // every read. Epoch 0 never advances here: these tests are about
            // placement, not collection.
            let next_pin = Arc::new(std::sync::atomic::AtomicU64::new(1));
            let live_pins = Arc::new(std::sync::Mutex::new(
                std::collections::BTreeSet::new(),
            ));
            let (store_c, fail_c, seen_c) = (store.clone(), fail_puts.clone(), seen.clone());
            let fail_once_c = fail_once.clone();
            let put_status_c = put_status.clone();
            let pin_status_c = pin_status.clone();
            let next_pin_c = next_pin.clone();
            let live_pins_c = live_pins.clone();
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
                    "GET" if path.ends_with("/epoch") => json_response("{\"epoch\":0}"),
                    "GET" if path.ends_with("/pins") => json_response("{\"pins\":[]}"),
                    "POST" if path.ends_with("/pin") && pin_status_c.load(Ordering::Relaxed) != 0 => {
                        let status = pin_status_c.load(Ordering::Relaxed);
                        let body = "{\"error\":\"pin_epoch_not_current\",\"message\":\"mock refusal\"}";
                        format!(
                            "HTTP/1.1 {status} Refused\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .into_bytes()
                    }
                    "POST" if path.ends_with("/pin") => {
                        let pin_id = next_pin_c.fetch_add(1, Ordering::Relaxed);
                        live_pins_c.lock().unwrap().insert(pin_id);
                        json_response(&format!(
                            "{{\"pin_id\":{pin_id},\"epoch\":0,\"expires_at_ms\":{}}}",
                            u64::MAX
                        ))
                    }
                    "POST" if path.ends_with("/unpin") => {
                        // `pin_id` is the only numeric field in the request body.
                        let pin_id = body
                            .split(|b| !b.is_ascii_digit())
                            .filter(|part| !part.is_empty())
                            .next()
                            .and_then(|part| std::str::from_utf8(part).ok())
                            .and_then(|text| text.parse::<u64>().ok());
                        let released = pin_id
                            .is_some_and(|pin_id| live_pins_c.lock().unwrap().remove(&pin_id));
                        json_response(&format!("{{\"released\":{released}}}"))
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
                pin_status,
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

    /// A read takes a pin on every slot it fetches from and releases it before
    /// it returns, so the fence covers the fetch and nothing after it.
    #[test]
    fn a_read_releases_its_pin_on_every_slot_it_touched() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (stores, striper) = local_pool(3105, scheme, 0);
        let id = BlobId::new(12, 43, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0003";

        striper.put_blob(id, payload).unwrap();
        assert_eq!(striper.get_blob(id).unwrap(), payload);

        for (slot, store) in stores.iter().enumerate() {
            // A 300 s pin would still be live here if it had not been released.
            assert!(
                store.live_pins().unwrap().is_empty(),
                "slot {slot} kept a read pin after the fetch returned"
            );
            // The pin is gone, so a collector at the next epoch may now reclaim
            // every fragment this slot holds. A leaked pin would refuse this.
            for part in 1..=scheme.total_parts() as u8 {
                let part_id = id.with_part(part);
                if store.fragment_epoch(part_id).is_some() {
                    assert!(
                        store.delete_collectable(part_id, 1).unwrap(),
                        "slot {slot} part {part} stayed protected after the read"
                    );
                }
            }
        }
    }

    /// The pin is taken before the fragment is fetched, and released after it.
    ///
    /// `seen` records the request lines in the order the mock received them, so
    /// this observes the wire order rather than the order of statements in
    /// `read_part`. A delete admitted between the fetch and a later pin would
    /// already have unlinked the bytes the pin was supposed to protect.
    #[test]
    fn a_read_pins_the_target_before_it_fetches_and_releases_it_after() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 0);
        let id = BlobId::new(12, 44, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0004";

        striper.put_blob(id, payload).unwrap();
        for vdisk in &vdisks {
            vdisk.seen.lock().unwrap().clear();
        }
        assert_eq!(striper.get_blob(id).unwrap(), payload);

        for (slot, vdisk) in vdisks.iter().enumerate() {
            let seen = vdisk.requests();
            let index = |needle: &str| seen.iter().position(|line| line.contains(needle));
            let epoch = index(&format!("/{slot}/epoch "));
            let pin = index(&format!("/{slot}/pin "));
            let fetch = index(&format!("/{slot}/blob/"));
            let unpin = index(&format!("/{slot}/unpin "));
            assert!(
                epoch.is_some() && pin.is_some() && fetch.is_some() && unpin.is_some(),
                "slot {slot} did not see epoch, pin, fetch and unpin: {seen:?}"
            );
            assert!(
                epoch < pin,
                "slot {slot} pinned before it knew the epoch: {seen:?}"
            );
            assert!(
                pin < fetch,
                "slot {slot} fetched before it pinned, so the fetch was unprotected: {seen:?}"
            );
            assert!(
                fetch < unpin,
                "slot {slot} released the pin before the bytes were copied: {seen:?}"
            );
        }
    }

    /// An unreachable target is not evidence that the bytes are gone either.
    ///
    /// The pin is the read's first contact with the target, so a target that
    /// cannot be reached fails there. Reporting that as `NotFound` would answer
    /// a question about the data from a failure that says nothing about it.
    #[test]
    fn an_unreachable_slot_is_not_reported_as_absence() {
        // Port 1 on the loopback interface has no listener: the pin's connection
        // is refused, which is the transport failure a down node produces.
        let slot =
            PoolSlot::<SimFs>::remote("pool", 0, "az0", 1, "127.0.0.1:1".to_string(), None);
        let striper =
            PoolStriper::from_slots("pool", ErasureScheme::Mirror { replicas: 1 }, vec![slot])
                .unwrap();
        let id = BlobId::new(12, 47, 1, 1, 2, 0);
        match striper.get_blob(id) {
            Err(BlobError::Io(_)) => {}
            Err(BlobError::NotFound(_)) => {
                panic!("an unreachable target was reported as absence")
            }
            other => panic!("expected the transport failure, got {other:?}"),
        }
    }

    /// A refused pin is a refusal to read, not evidence that the bytes are gone.
    ///
    /// Every target refuses here, so no part can be read. Reporting
    /// `NotFound`/`InsufficientFragments` would turn "the fence refused" into
    /// "the data does not exist", and the second statement is not one the read
    /// is entitled to make.
    #[test]
    fn a_refused_pin_fails_the_read_closed_instead_of_reporting_absence() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 0);
        let id = BlobId::new(12, 45, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0005";

        striper.put_blob(id, payload).unwrap();
        for vdisk in &vdisks {
            vdisk.pin_status.store(409, Ordering::Relaxed);
        }
        match striper.get_blob(id) {
            Err(BlobError::RemoteFenceRefused { status, code, .. }) => {
                assert_eq!(status, 409);
                assert_eq!(code, "pin_epoch_not_current");
            }
            Err(BlobError::NotFound(_)) => {
                panic!("a refused pin was reported as absence")
            }
            Err(BlobError::InsufficientFragments { .. }) => {
                panic!("a refused pin was reported as a shortfall")
            }
            other => panic!("expected the refusal, got {other:?}"),
        }
        // The refusal consumed nothing: once the targets admit reads again, the
        // payload is intact.
        for vdisk in &vdisks {
            vdisk.pin_status.store(0, Ordering::Relaxed);
        }
        assert_eq!(striper.get_blob(id).unwrap(), payload);
    }

    /// A repair stops when a target refuses to be pinned, instead of treating
    /// the part as missing and rewriting a fragment a reader may be holding.
    ///
    /// Only one target refuses, and the other five are enough to reconstruct,
    /// so "treat as missing" would silently succeed. The absence of any PUT is
    /// what distinguishes stopping from repairing.
    #[test]
    fn a_repair_stops_when_a_target_refuses_to_be_pinned() {
        let scheme = ErasureScheme::ReedSolomon { k: 4, m: 2 };
        let (vdisks, striper) = remote_pool("pool", scheme, 0);
        let id = BlobId::new(12, 46, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0006";

        striper.put_blob(id, payload).unwrap();
        vdisks[0].pin_status.store(409, Ordering::Relaxed);
        for vdisk in &vdisks {
            vdisk.seen.lock().unwrap().clear();
        }
        match striper.repair_blob(id) {
            Err(BlobError::RemoteFenceRefused { status, .. }) => assert_eq!(status, 409),
            other => panic!("a refused pin must stop the repair, got {other:?}"),
        }
        for (slot, vdisk) in vdisks.iter().enumerate() {
            let seen = vdisk.requests();
            assert!(
                !seen.iter().any(|line| line.starts_with("PUT")),
                "slot {slot} was rewritten although the repair could not prove the part missing: {seen:?}"
            );
        }
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
