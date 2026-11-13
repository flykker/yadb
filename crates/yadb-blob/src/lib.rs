pub mod erasure;
pub mod id;
pub mod pool;
pub mod volume;

pub use erasure::{ErasureCodec, ErasureScheme};
pub use id::BlobId;
pub use pool::{
    BarrierRequest, BarrierResponse, PoolHealthSnapshot, PoolHealthState, PoolSlot, PoolStriper,
    RepairReport, SlotHealthSnapshot,
};
pub use volume::{VolumeConfig, VolumeStats, VolumeStore};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobError {
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
    use yadb_io::{FaultConfig, SimFs, StorageFs};

    #[test]
    fn reed_solomon_recovers_any_two_lost_fragments() {
        let codec = ErasureCodec::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }).unwrap();
        let original = b"YADB distributed storage engine over standard filesystems without PDisk!";
        let shards = codec.encode(original);
        assert_eq!(shards.len(), 6);

        // Drop every possible pair (i, j) out of 6 shards (all 15 combinations: data+data, data+parity, parity+parity)
        for drop_a in 0..6 {
            for drop_b in (drop_a + 1)..6 {
                let mut partial: Vec<Option<Vec<u8>>> =
                    shards.iter().cloned().map(Some).collect();
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
        assert!(reclaimed > 0, "expected segment compaction to reclaim bytes");
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
        assert!(matches!(reopened.put(old, b"late orphan fragment"),
            Err(BlobError::FencedByBarrier { barrier_gen: 1, .. })));
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
        store.put(BlobId::new(10, 51, 8, 2, 2, 0), &[b'm'; 100]).unwrap();
        assert!(store.compact_sealed_segments().unwrap() > 0);
        drop(store);
        fs.simulate_power_cut();
        let reopened = VolumeStore::open(fs, "tombstone", cfg).unwrap();
        assert!(matches!(reopened.get(id), Err(BlobError::NotFound(_))));
        assert!(matches!(reopened.put(id, b"reused"), Err(BlobError::Tombstoned(_))));
    }

    #[test]
    fn concurrent_conflicting_puts_never_acknowledge_two_values() {
        let fs = SimFs::new(1240);
        let store = VolumeStore::open(fs.clone(), "conflicts", VolumeConfig::default()).unwrap();
        for step in 1..=64 {
            let id = BlobId::new(1, 1, 1, step, 4, 0);
            let start = std::sync::Barrier::new(2);
            let (a, b) = std::thread::scope(|scope| {
                let a = scope.spawn(|| { start.wait(); store.put(id, b"first") });
                let b = scope.spawn(|| { start.wait(); store.put(id, b"other") });
                (a.join().unwrap(), b.join().unwrap())
            });
            assert_ne!(a.is_ok(), b.is_ok());
            assert_eq!(store.get(id).unwrap(), if a.is_ok() { b"first" } else { b"other" });
        }
        drop(store);
        fs.simulate_power_cut();
        assert_eq!(VolumeStore::open(fs, "conflicts", VolumeConfig::default()).unwrap().stats().unwrap().live_blobs, 64);
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
                    let put = scope.spawn(|| { start.wait(); store.put(id, b"payload") });
                    let fence = scope.spawn(|| {
                        start.wait();
                        if exact { store.delete(id).map(|_| ()) }
                        else { store.collect_garbage_barrier(1, 1, 4, generation).map(|_| ()) }
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
                assert!(recovered.put(BlobId::new(1, 1, generation, 1, 4, 0), b"late").is_err());
            }
        }
    }

    #[test]
    fn reads_pin_matching_offsets_while_compaction_replaces_segment_files() {
        let fs = SimFs::new(1242);
        let cfg = VolumeConfig { max_segment_bytes: 300, ..Default::default() };
        let store = VolumeStore::open(fs, "compact_read", cfg).unwrap();
        let dead = BlobId::new(1, 1, 1, 1, 4, 0);
        let live = BlobId::new(1, 1, 1, 2, 4, 0);
        store.put(dead, &[b'd'; 70]).unwrap();
        store.put(live, &[b'l'; 70]).unwrap();
        store.put(BlobId::new(1, 1, 1, 3, 4, 0), &[b'z'; 70]).unwrap();
        store.delete(dead).unwrap();
        std::thread::scope(|scope| {
            let compactor = scope.spawn(|| {
                for _ in 0..100 { store.compact_sealed_segments().unwrap(); }
            });
            let readers: Vec<_> = (0..4).map(|_| scope.spawn(|| {
                for _ in 0..500 {
                    assert_eq!(store.get(live).unwrap(), vec![b'l'; 70]);
                    assert_eq!(store.get_slice(live, 10, 20).unwrap(), vec![b'l'; 20]);
                }
            })).collect();
            compactor.join().unwrap();
            for reader in readers { reader.join().unwrap(); }
        });
    }

    #[test]
    fn failed_tombstone_and_barrier_do_not_poison_later_acknowledged_writes() {
        for torn in [false, true] {
            let fs = SimFs::new(1243);
            let store = VolumeStore::open(fs.clone(), "failed_fence", VolumeConfig::default()).unwrap();
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
        fs.corrupt_byte("bitrot_recovery/vol_00000001.dat", 8 + 33).unwrap();
        fs.corrupt_byte("bitrot_recovery/vol_00000001.dat", 8 + 40 + 33 + 40 + 33).unwrap();
        drop(store);
        fs.simulate_power_cut();
        let recovered = VolumeStore::open(fs, "bitrot_recovery", cfg).unwrap();
        assert!(matches!(recovered.put(dead, b"deleted"), Err(BlobError::Tombstoned(_))));
        assert_eq!(recovered.get(live).unwrap(), b"durable");
        assert!(matches!(recovered.get(corrupt), Err(BlobError::CorruptData { .. })));
        recovered.put(corrupt, b"repair-me").unwrap();
        assert_eq!(recovered.get(corrupt).unwrap(), b"repair-me");
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
        let slot = PoolSlot::<SimFs>::remote("pool", 0, "remote", 2, peer);
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

        let striper =
            PoolStriper::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }, domains).unwrap();
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
}
