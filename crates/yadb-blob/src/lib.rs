pub mod erasure;
pub mod id;
pub mod pool;
pub mod volume;

pub use erasure::{ErasureCodec, ErasureScheme};
pub use id::BlobId;
pub use pool::{
    BarrierRequest, BarrierResponse, PoolHealthSnapshot, PoolHealthState, PoolSlot, PoolStriper,
    RepairReport, RepairSweep, SlotHealthSnapshot,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use yadb_io::{FaultConfig, SimFs, StorageFs, SyncFault};

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
                _ => assert!(store.collect_garbage_barrier(1, 1, 4, 1).is_err()),
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
                    _ => store.collect_garbage_barrier(1, 1, 4, 1).map(|_| ()),
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
                    store.collect_garbage_barrier(1, 1, 4, 1),
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

        let evicted = store.collect_garbage_barrier(10, 50, 2, 1).unwrap();
        assert_eq!(evicted, 6);

        let reclaimed = store.compact_sealed_segments().unwrap();
        assert!(
            reclaimed > 0,
            "expected segment compaction to reclaim bytes"
        );
        assert_eq!(store.get(live_id).unwrap(), vec![b'y'; 180]);
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
        store.collect_garbage_barrier(10, 50, 2, 1).unwrap();
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
                                .collect_garbage_barrier(1, 1, 4, generation)
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
            assert!(store.collect_garbage_barrier(1, 1, 4, 1).is_err());
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

    /// Mock VDisk that answers a fixed script of responses, one per request, and
    /// reports the request lines it saw. Used to drive a slot that fails and then
    /// recovers, which `SimFs` faults cannot model: a write fault poisons the
    /// volume until reopen, and a transient peer failure must not require that.
    fn scripted_vdisk(
        responses: Vec<&'static str>,
    ) -> (String, std::thread::JoinHandle<Vec<String>>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut raw = Vec::new();
                let mut buf = [0u8; 1024];
                loop {
                    let n = stream.read(&mut buf).unwrap();
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&buf[..n]);
                    if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let text = String::from_utf8_lossy(&raw).to_string();
                seen.push(text.lines().next().unwrap_or("").to_string());
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
            }
            seen
        });
        (addr, handle)
    }

    // S6: `put_blob` acked with fewer than all fragments and dropped the failures,
    // so a pool that had lost a node kept writing with reduced or absent redundancy
    // and nothing recorded which blobs needed healing.
    #[test]
    fn short_write_is_reported_as_degraded_and_queued_for_repair() {
        let (addr, server) = scripted_vdisk(vec![
            // put_blob: the fragment does not land.
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            // repair_blob: the fragment is absent, so it must be rewritten.
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            // repair_blob: the rewrite lands.
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ]);
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
        // Five of six fragments is still above the k=4 write quorum, so the write is
        // acked — but with zero parity, which is exactly the state S6 describes.
        striper.put_blob(id, payload).unwrap();
        assert_eq!(striper.pending_repair_count(), 1);
        let health = striper.health_snapshot(false);
        assert_eq!(health.degraded_puts, 1);
        assert_eq!(health.pending_repairs, 1);

        let sweep = striper.repair_pending(16);
        assert_eq!(sweep.attempted, 1);
        assert_eq!(sweep.repaired_parts, 1);
        assert_eq!(sweep.unrecoverable, 0);
        assert_eq!(sweep.still_pending, 0);
        assert_eq!(striper.pending_repair_count(), 0);

        let seen = server.join().unwrap();
        assert_eq!(seen.len(), 3);
        assert!(seen[0].starts_with("PUT /internal/vdisk/pool/5/blob/"));
        assert!(seen[1].starts_with("GET /internal/vdisk/pool/5/blob/"));
        assert!(seen[2].starts_with("PUT /internal/vdisk/pool/5/blob/"));
    }

    // The other half: a repair that cannot rewrite the fragment must not be
    // reported as success, and the blob must stay queued rather than be forgotten.
    #[test]
    fn unrepairable_degraded_write_stays_queued() {
        let (addr, server) = scripted_vdisk(vec![
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ]);
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
        striper.put_blob(id, payload).unwrap();
        let sweep = striper.repair_pending(16);
        assert_eq!(sweep.attempted, 1);
        assert_eq!(sweep.repaired_parts, 0);
        assert_eq!(sweep.unrecoverable, 1);
        assert_eq!(sweep.still_pending, 1);
        assert_eq!(striper.pending_repair_count(), 1);
        server.join().unwrap();
    }

    // A fully redundant write must not enter the queue, or the queue would grow
    // with every write and the signal would be worthless.
    #[test]
    fn full_redundancy_write_is_not_queued() {
        let id = BlobId::new(11, 3, 1, 1, 2, 0);
        let payload = b"Multi-tenant erasure-coded tablet SSTable block #0001";
        let (_fs, striper) = seeded_striper(
            7003,
            ErasureScheme::ReedSolomon { k: 4, m: 2 },
            6,
            id,
            payload,
        );
        assert_eq!(striper.pending_repair_count(), 0);
        let health = striper.health_snapshot(false);
        assert_eq!(health.degraded_puts, 0);
        assert_eq!(health.pending_repairs, 0);
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
