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
