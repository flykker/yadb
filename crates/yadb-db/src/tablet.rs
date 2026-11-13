use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use yadb_blob::{BlobId, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, KvResult, LocalMetaKv, RevisionGuard, TxnOp};

use crate::sql::SqlValue;
use crate::DbError;

// Channels 1/2 remain readable. New IDs must not overlap legacy hash-based IDs.
pub const CHANNEL_WAL: u8 = 6;
pub const CHANNEL_SST: u8 = 7;
const MAX_ALLOCATION: u64 = (1u64 << 56) - 1;
const AUTO_COMPACT_SST_THRESHOLD: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RowEntry {
    Present { seq: u64, values: Vec<SqlValue> },
    Tombstone { seq: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use yadb_blob::{ErasureScheme, VolumeConfig, VolumeStore};
    use yadb_io::{FaultConfig, SimFs, SyncFault};

    fn setup() -> (SimFs, LocalMetaKv<SimFs>, Arc<PoolStriper<SimFs>>) {
        let fs = SimFs::new(9100);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let domains = (0..3)
            .map(|i| {
                VolumeStore::open(fs.clone(), format!("disk{i}"), VolumeConfig::default()).unwrap()
            })
            .collect();
        let pool =
            Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
        (fs, meta, pool)
    }

    fn open(
        meta: &LocalMetaKv<SimFs>,
        pool: &Arc<PoolStriper<SimFs>>,
        key: &str,
    ) -> DataShardTablet<SimFs> {
        DataShardTablet::open_or_recover(
            meta.clone(),
            key.into(),
            1,
            42,
            "pool".into(),
            "pool".into(),
            pool.clone(),
            pool.clone(),
            1024,
        )
        .unwrap()
    }

    fn row(value: i64) -> Vec<(Vec<u8>, Option<Vec<SqlValue>>)> {
        vec![(b"key".to_vec(), Some(vec![SqlValue::Int(value)]))]
    }

    #[test]
    fn uncommitted_preparations_never_reuse_blob_ids() {
        let (_fs, meta, pool) = setup();
        let tablet = open(&meta, &pool, "shard");
        let first = tablet.prepare_batch(row(1)).unwrap();
        let second = tablet.prepare_batch(row(2)).unwrap();
        assert_ne!(
            first.next_manifest.wal_blobs,
            second.next_manifest.wal_blobs
        );
        assert!(tablet.get_row(b"key").unwrap().is_none());
    }

    #[test]
    fn colliding_tablet_owners_have_disjoint_wal_and_sst_ids() {
        let (_fs, meta, pool) = setup();
        let mut first = open(&meta, &pool, "shard-a");
        let mut second = open(&meta, &pool, "shard-b");
        assert_eq!(first.owner_id(), second.owner_id());
        first.apply_batch(row(1)).unwrap();
        second.apply_batch(row(2)).unwrap();
        first.flush_to_sstable().unwrap();
        second.flush_to_sstable().unwrap();
        first.compact().unwrap();
        assert_eq!(
            second.get_row(b"key").unwrap(),
            Some(vec![SqlValue::Int(2)])
        );
        assert_ne!(
            first.manifest.sst_blobs[0].blob_id,
            second.manifest.sst_blobs[0].blob_id
        );
    }

    #[test]
    fn compaction_preserves_old_snapshot_ssts() {
        let (_fs, meta, pool) = setup();
        let mut tablet = open(&meta, &pool, "shard");
        tablet.apply_batch(row(1)).unwrap();
        tablet.flush_to_sstable().unwrap();
        let snapshot = DataShardTablet::open_readonly_snapshot(
            meta.clone(),
            "shard".into(),
            pool.clone(),
            pool.clone(),
        )
        .unwrap()
        .unwrap();
        tablet.apply_batch(row(2)).unwrap();
        tablet.compact().unwrap();
        assert_eq!(
            tablet.get_row(b"key").unwrap(),
            Some(vec![SqlValue::Int(2)])
        );
        assert_eq!(
            snapshot.get_row(b"key").unwrap(),
            Some(vec![SqlValue::Int(1)])
        );
        assert_eq!(
            snapshot.scan_all_live_rows().unwrap(),
            vec![vec![SqlValue::Int(1)]]
        );
    }

    #[test]
    fn stale_tablet_cannot_publish_after_manifest_delete_recreate() {
        let (_fs, meta, pool) = setup();
        let mut tablet = open(&meta, &pool, "shard");
        let old = meta.get("shard").unwrap();
        meta.execute(KvCommand::Delete {
            key: "shard".into(),
        })
        .unwrap();
        meta.execute(KvCommand::Put {
            key: "shard".into(),
            value: old.value.clone(),
        })
        .unwrap();
        assert_eq!(old.version, meta.get("shard").unwrap().version);
        assert!(matches!(
            tablet.apply_batch(row(1)),
            Err(DbError::StaleTabletGeneration { .. })
        ));
        assert_eq!(meta.get("shard").unwrap().value, old.value);
    }

    #[test]
    fn stale_flush_and_compaction_cannot_publish_or_remove_committed_data() {
        for compact in [false, true] {
            let (_fs, meta, pool) = setup();
            let mut stale = open(&meta, &pool, "shard");
            stale.apply_batch(row(1)).unwrap();
            let mut current = open(&meta, &pool, "shard");
            current.apply_batch(row(2)).unwrap();
            current.flush_to_sstable().unwrap();
            let result = if compact {
                stale.compact().map(|_| ())
            } else {
                stale.flush_to_sstable().map(|_| ())
            };
            assert!(matches!(result, Err(DbError::StaleTabletGeneration { .. })));
            let snapshot =
                DataShardTablet::open_readonly_snapshot(meta, "shard".into(), pool.clone(), pool)
                    .unwrap()
                    .unwrap();
            assert_eq!(
                snapshot.get_row(b"key").unwrap(),
                Some(vec![SqlValue::Int(2)])
            );
        }
    }

    #[test]
    fn allocator_rollover_exhaustion_and_logical_counters_do_not_wrap() {
        let (_fs, meta, pool) = setup();
        let mut tablet = open(&meta, &pool, "shard");
        let key = "/sql-blob-sequence/1".to_string();
        meta.execute(KvCommand::Put {
            key: key.clone(),
            value: serde_json::to_vec(&(BlobId::MAX_U24 as u64)).unwrap(),
        })
        .unwrap();
        let prep = tablet.prepare_batch(row(1)).unwrap();
        let id = prep.next_manifest.wal_blobs[0];
        assert_eq!(
            (id.owner_id(), id.generation(), id.step(), id.channel()),
            (1, 0, 1, CHANNEL_WAL)
        );
        meta.execute(KvCommand::Put {
            key: key.clone(),
            value: serde_json::to_vec(&(MAX_ALLOCATION - 1)).unwrap(),
        })
        .unwrap();
        tablet.apply_batch(row(2)).unwrap();
        assert!(matches!(
            tablet.prepare_batch(row(3)),
            Err(DbError::CounterExhausted)
        ));
        assert_eq!(
            tablet.get_row(b"key").unwrap(),
            Some(vec![SqlValue::Int(2)])
        );
        assert_eq!(
            serde_json::from_slice::<u64>(&meta.get(&key).unwrap().value).unwrap(),
            MAX_ALLOCATION
        );
        tablet.manifest.last_seq = u64::MAX;
        assert!(matches!(
            tablet.prepare_batch(row(4)),
            Err(DbError::CounterExhausted)
        ));
        tablet.manifest.generation = BlobId::MAX_U24;
        assert!(matches!(tablet.compact(), Err(DbError::CounterExhausted)));
        meta.execute(KvCommand::Put {
            key: "shard".into(),
            value: serde_json::to_vec(&tablet.manifest).unwrap(),
        })
        .unwrap();
        assert!(matches!(
            DataShardTablet::open_or_recover(
                meta,
                "shard".into(),
                1,
                42,
                "pool".into(),
                "pool".into(),
                pool.clone(),
                pool,
                1024
            ),
            Err(DbError::CounterExhausted)
        ));
    }

    #[test]
    fn legacy_wal_and_sst_remain_readable_after_compaction_and_restart() {
        let (fs, meta, pool) = setup();
        let mut tablet = open(&meta, &pool, "shard");
        let wal_id = BlobId::new(1, 42, 1, 1, 1, 0);
        let sst_id = BlobId::new(1, 42, 1, 2, 2, 0);
        let entry = RowEntry::Present {
            seq: 1,
            values: vec![SqlValue::Int(10)],
        };
        pool.put_blob(
            sst_id,
            &serde_json::to_vec(&SstImage {
                entries: vec![(b"sst".to_vec(), entry.clone())],
            })
            .unwrap(),
        )
        .unwrap();
        pool.put_blob(
            wal_id,
            &serde_json::to_vec(&WalBatch {
                mutations: vec![(b"wal".to_vec(), entry)],
            })
            .unwrap(),
        )
        .unwrap();
        tablet.manifest.wal_blobs.push(wal_id);
        tablet.manifest.sst_blobs.push(SstRef {
            blob_id: sst_id,
            min_key: b"sst".to_vec(),
            max_key: b"sst".to_vec(),
            row_count: 1,
        });
        tablet.manifest.last_seq = 1;
        tablet.cas_save_manifest().unwrap();
        drop(tablet);
        let mut tablet = open(&meta, &pool, "shard");
        let snapshot = DataShardTablet::open_readonly_snapshot(
            meta.clone(),
            "shard".into(),
            pool.clone(),
            pool.clone(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(tablet.scan_all_live_rows().unwrap().len(), 2);
        tablet.compact().unwrap();
        assert_eq!(tablet.manifest.sst_blobs[0].blob_id.channel(), CHANNEL_SST);
        assert_eq!(snapshot.scan_all_live_rows().unwrap().len(), 2);
        drop(snapshot);
        drop(tablet);
        drop(meta);
        drop(pool);
        fs.simulate_power_cut();
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let domains = (0..3)
            .map(|i| {
                VolumeStore::open(fs.clone(), format!("disk{i}"), VolumeConfig::default()).unwrap()
            })
            .collect();
        let pool =
            Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
        let tablet = open(&meta, &pool, "shard");
        assert_eq!(tablet.scan_all_live_rows().unwrap().len(), 2);
        assert!(pool.get_blob(wal_id).is_ok());
        assert!(pool.get_blob(sst_id).is_ok());
        let before =
            serde_json::from_slice::<u64>(&meta.get("/sql-blob-sequence/1").unwrap().value)
                .unwrap();
        tablet.prepare_batch(row(11)).unwrap();
        assert!(
            serde_json::from_slice::<u64>(&meta.get("/sql-blob-sequence/1").unwrap().value)
                .unwrap()
                > before
        );
    }

    #[test]
    fn ambiguous_reservation_never_writes_fragments_and_recovery_does_not_reuse_ids() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let (fs, meta, pool) = setup();
            let tablet = open(&meta, &pool, "shard");
            fs.set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            assert!(matches!(
                tablet.prepare_batch(row(1)),
                Err(DbError::Meta(_))
            ));
            for slot in pool.slots() {
                assert_eq!(slot.local_store().unwrap().stats().unwrap().live_blobs, 0);
            }
            drop(tablet);
            drop(meta);
            drop(pool);
            fs.set_faults(FaultConfig::default());
            fs.simulate_power_cut();
            let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
            let domains = (0..3)
                .map(|i| {
                    VolumeStore::open(fs.clone(), format!("disk{i}"), VolumeConfig::default())
                        .unwrap()
                })
                .collect();
            let pool =
                Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
            let tablet = open(&meta, &pool, "shard");
            let prep = tablet.prepare_batch(row(2)).unwrap();
            assert_eq!(
                prep.next_manifest.wal_blobs[0].generation(),
                if fault == SyncFault::AfterPersist {
                    2
                } else {
                    1
                }
            );
        }
    }

    #[test]
    fn publication_sync_error_recovers_as_committed_or_unpublished() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let (fs, meta, pool) = setup();
            let mut tablet = open(&meta, &pool, "shard");
            tablet.apply_batch(row(1)).unwrap();
            let prep = tablet.prepare_batch(row(2)).unwrap();
            let id = *prep.next_manifest.wal_blobs.last().unwrap();
            fs.set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            assert!(tablet.publish_prepared(&prep).is_err());
            assert_eq!(
                tablet.get_row(b"key").unwrap(),
                Some(vec![SqlValue::Int(1)])
            );
            drop(tablet);
            drop(meta);
            drop(pool);
            fs.set_faults(FaultConfig::default());
            fs.simulate_power_cut();
            let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
            let domains = (0..3)
                .map(|i| {
                    VolumeStore::open(fs.clone(), format!("disk{i}"), VolumeConfig::default())
                        .unwrap()
                })
                .collect();
            let pool =
                Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
            let mut tablet = open(&meta, &pool, "shard");
            assert_eq!(
                tablet.get_row(b"key").unwrap(),
                Some(vec![SqlValue::Int(if fault == SyncFault::AfterPersist {
                    2
                } else {
                    1
                })])
            );
            tablet.apply_batch(row(3)).unwrap();
            assert_ne!(*tablet.manifest.wal_blobs.last().unwrap(), id);
            assert!(pool.get_blob(id).is_ok()); // No cleanup based on absent references.
        }
    }

    #[test]
    fn failed_fragment_write_and_rejected_preparation_do_not_block_retry() {
        let (_fs, meta, _unused) = setup();
        let data_fs = SimFs::new(9101);
        let domains = (0..3)
            .map(|i| {
                VolumeStore::open(data_fs.clone(), format!("disk{i}"), VolumeConfig::default())
                    .unwrap()
            })
            .collect();
        let pool =
            Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
        let mut tablet = open(&meta, &pool, "shard");
        data_fs.set_faults(FaultConfig {
            write_eio_prob: 1.0,
            ..Default::default()
        });
        assert!(tablet.prepare_batch(row(1)).is_err());
        data_fs.set_faults(FaultConfig::default());
        let first = tablet.prepare_batch(row(2)).unwrap();
        let rejected = tablet.prepare_batch(row(3)).unwrap();
        let revision = tablet.publish_prepared(&first).unwrap();
        tablet.apply_prepared(first, revision).unwrap();
        assert!(matches!(
            tablet.publish_prepared(&rejected),
            Err(DbError::StaleTabletGeneration { .. })
        ));
        tablet.apply_batch(row(4)).unwrap();
        assert_eq!(
            tablet.get_row(b"key").unwrap(),
            Some(vec![SqlValue::Int(4)])
        );
    }

    #[test]
    fn independent_tablets_allocate_disjoint_ids_concurrently() {
        let (_fs, meta, pool) = setup();
        let tablets = (0..4)
            .map(|i| open(&meta, &pool, &format!("shard-{i}")))
            .collect::<Vec<_>>();
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let threads = tablets
            .into_iter()
            .map(|tablet| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    (0..8)
                        .map(|_| tablet.next_blob_id(CHANNEL_WAL).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        let ids = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids.len(), 32);
    }
}

impl RowEntry {
    pub fn seq(&self) -> u64 {
        match self {
            RowEntry::Present { seq, .. } | RowEntry::Tombstone { seq } => *seq,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalBatch {
    pub mutations: Vec<(Vec<u8>, RowEntry)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstRef {
    pub blob_id: BlobId,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub row_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SstImage {
    entries: Vec<(Vec<u8>, RowEntry)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabletManifest {
    pub project_id: u32,
    pub owner_id: u32,
    pub generation: u32,
    /// Legacy channel-1/2 allocator state; unused for new physical IDs.
    pub last_step: u32,
    pub last_seq: u64,
    pub wal_pool: String,
    pub data_pool: String,
    pub wal_blobs: Vec<BlobId>,
    pub sst_blobs: Vec<SstRef>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CompactionReport {
    pub merged_ssts: usize,
    pub live_rows: usize,
    /// Zero until safe cross-node reader retention/reclamation is implemented.
    pub evicted_fragments: usize,
}

/// Prepared immutable WAL write for atomic cross-shard manifest publication.
pub struct PreparedShardCommit {
    pub meta_key: String,
    pub expected_mod_revision: u64,
    pub next_manifest: TabletManifest,
    pub encoded_manifest: Vec<u8>,
    pub mutations: Vec<(Vec<u8>, RowEntry)>,
}

/// `DataShard` storing WAL on `walPool` (channel 6) and SSTs on `dataPool`
/// (channel 7). Legacy channels 1/2 remain readable through manifest references.
pub struct DataShardTablet<FS: StorageFs> {
    meta_key: String,
    manifest_revision: u64,
    manifest: TabletManifest,
    meta: LocalMetaKv<FS>,
    wal_striper: Arc<PoolStriper<FS>>,
    data_striper: Arc<PoolStriper<FS>>,
    memtable: BTreeMap<Vec<u8>, RowEntry>,
    flush_threshold_rows: usize,
}

impl<FS: StorageFs> DataShardTablet<FS> {
    #[allow(clippy::too_many_arguments)]
    pub fn open_or_recover(
        meta: LocalMetaKv<FS>,
        meta_key: String,
        project_id: u32,
        owner_id: u32,
        wal_pool_name: String,
        data_pool_name: String,
        wal_striper: Arc<PoolStriper<FS>>,
        data_striper: Arc<PoolStriper<FS>>,
        flush_threshold_rows: usize,
    ) -> Result<Self, DbError> {
        if let Some(kv) = meta.get(&meta_key) {
            let mut manifest: TabletManifest = serde_json::from_slice(&kv.value)?;
            // The revision, not the bounded legacy generation, fences publication.
            manifest.generation = manifest
                .generation
                .checked_add(1)
                .filter(|n| *n <= BlobId::MAX_U24)
                .ok_or(DbError::CounterExhausted)?;
            manifest.last_step = 0;

            let mut memtable = BTreeMap::new();
            for &wal_id in &manifest.wal_blobs {
                let raw = wal_striper.get_blob(wal_id)?;
                let batch: WalBatch = serde_json::from_slice(&raw)?;
                for (pk, entry) in batch.mutations {
                    if entry.seq() > manifest.last_seq {
                        manifest.last_seq = entry.seq();
                    }
                    memtable.insert(pk, entry);
                }
            }

            let encoded = serde_json::to_vec(&manifest)?;
            let next_revision = match meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: meta_key.clone(),
                    expected_mod_revision: kv.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: meta_key.clone(),
                    value: encoded,
                }],
            })? {
                KvResult::TxnOk { revision, .. } => revision,
                KvResult::CasConflict { .. } => {
                    return Err(DbError::StaleTabletGeneration { owner_id });
                }
                _ => unreachable!(),
            };

            Ok(Self {
                meta_key,
                manifest_revision: next_revision,
                manifest,
                meta,
                wal_striper,
                data_striper,
                memtable,
                flush_threshold_rows: flush_threshold_rows.max(16),
            })
        } else {
            let manifest = TabletManifest {
                project_id,
                owner_id,
                generation: 1,
                last_step: 0,
                last_seq: 0,
                wal_pool: wal_pool_name,
                data_pool: data_pool_name,
                wal_blobs: Vec::new(),
                sst_blobs: Vec::new(),
            };
            let encoded = serde_json::to_vec(&manifest)?;
            let revision = match meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: meta_key.clone(),
                    expected_mod_revision: 0,
                }],
                ops: vec![TxnOp::Put {
                    key: meta_key.clone(),
                    value: encoded,
                }],
            })? {
                KvResult::TxnOk { revision, .. } => revision,
                KvResult::CasConflict { .. } => {
                    return Err(DbError::StaleTabletGeneration { owner_id });
                }
                _ => unreachable!(),
            };

            Ok(Self {
                meta_key,
                manifest_revision: revision,
                manifest,
                meta,
                wal_striper,
                data_striper,
                memtable: BTreeMap::new(),
                flush_threshold_rows: flush_threshold_rows.max(16),
            })
        }
    }

    pub fn generation(&self) -> u32 {
        self.manifest.generation
    }

    pub fn owner_id(&self) -> u32 {
        self.manifest.owner_id
    }

    pub fn manifest_revision(&self) -> u64 {
        self.manifest_revision
    }

    /// Opens a read-only view of a shard from replicated `MetaKV` + distributed `PoolStriper`
    /// without mutating Raft state or bumping the leader's fencing `generation`!
    pub fn open_readonly_snapshot(
        meta: LocalMetaKv<FS>,
        meta_key: String,
        wal_striper: Arc<PoolStriper<FS>>,
        data_striper: Arc<PoolStriper<FS>>,
    ) -> Result<Option<Self>, DbError> {
        let Some(kv) = meta.get(&meta_key) else {
            return Ok(None);
        };
        Ok(Some(Self::open_snapshot_value(
            meta, meta_key, kv, wal_striper, data_striper,
        )?))
    }

    /// Read only the supplied immutable manifest value; never reload metadata.
    /// Old fragments must remain retained for the lifetime of this view.
    pub(crate) fn open_snapshot_value(
        meta: LocalMetaKv<FS>,
        meta_key: String,
        kv: yadb_metakv::KvValue,
        wal_striper: Arc<PoolStriper<FS>>,
        data_striper: Arc<PoolStriper<FS>>,
    ) -> Result<Self, DbError> {
        let manifest: TabletManifest = serde_json::from_slice(&kv.value)?;
        let mut memtable = BTreeMap::new();
        for &wal_id in &manifest.wal_blobs {
            let raw = wal_striper.get_blob(wal_id)?;
            let batch: WalBatch = serde_json::from_slice(&raw)?;
            for (pk, entry) in batch.mutations {
                memtable.insert(pk, entry);
            }
        }
        Ok(Self {
            meta_key,
            manifest_revision: kv.mod_revision,
            manifest,
            meta,
            wal_striper,
            data_striper,
            memtable,
            flush_threshold_rows: usize::MAX,
        })
    }

    pub fn sst_count(&self) -> usize {
        self.manifest.sst_blobs.len()
    }

    pub fn wal_count(&self) -> usize {
        self.manifest.wal_blobs.len()
    }

    // Reserve before PUT, independently of owner hashes, tablet lifetimes and
    // publication outcomes. Never delete/reset this key or restore it separately
    // from fragment storage. Unknown reservation outcomes do not authorize PUT.
    fn next_blob_id(&self, channel: u8) -> Result<BlobId, DbError> {
        let key = format!("/sql-blob-sequence/{}", self.manifest.project_id);
        for _ in 0..32 {
            let current = self.meta.get(&key);
            let revision = current.as_ref().map_or(0, |v| v.mod_revision);
            let previous = match current {
                Some(v) => serde_json::from_slice::<u64>(&v.value)?,
                None => 0,
            };
            let next = previous
                .checked_add(1)
                .filter(|v| *v <= MAX_ALLOCATION)
                .ok_or(DbError::CounterExhausted)?;
            if matches!(
                self.meta.execute(KvCommand::RevisionTxn {
                    guards: vec![RevisionGuard {
                        key: key.clone(),
                        expected_mod_revision: revision
                    }],
                    ops: vec![TxnOp::Put {
                        key: key.clone(),
                        value: serde_json::to_vec(&next)?
                    }],
                })?,
                KvResult::TxnOk { .. }
            ) {
                return Ok(BlobId::new(
                    self.manifest.project_id,
                    (next >> 24) as u32,
                    (next & BlobId::MAX_U24 as u64) as u32,
                    1,
                    channel,
                    0,
                ));
            }
        }
        Err(DbError::StaleTabletGeneration {
            owner_id: self.manifest.owner_id,
        })
    }

    fn cas_save_manifest(&mut self) -> Result<(), DbError> {
        let encoded = serde_json::to_vec(&self.manifest)?;
        match self.meta.execute(KvCommand::RevisionTxn {
            guards: vec![RevisionGuard {
                key: self.meta_key.clone(),
                expected_mod_revision: self.manifest_revision,
            }],
            ops: vec![TxnOp::Put {
                key: self.meta_key.clone(),
                value: encoded,
            }],
        })? {
            KvResult::TxnOk { revision, .. } => {
                self.manifest_revision = revision;
                Ok(())
            }
            KvResult::CasConflict { .. } => Err(DbError::StaleTabletGeneration {
                owner_id: self.manifest.owner_id,
            }),
            _ => unreachable!(),
        }
    }

    /// Phase 1 of a multi-shard transaction: writes the immutable WAL batch to `walPool`
    /// without exposing it in `MetaKV` or `MemTable` yet!
    pub fn prepare_batch(
        &self,
        ops: Vec<(Vec<u8>, Option<Vec<SqlValue>>)>,
    ) -> Result<PreparedShardCommit, DbError> {
        let mut next_manifest = self.manifest.clone();
        let mut mutations = Vec::with_capacity(ops.len());
        for (pk, row_opt) in ops {
            next_manifest.last_seq = next_manifest
                .last_seq
                .checked_add(1)
                .ok_or(DbError::CounterExhausted)?;
            let seq = next_manifest.last_seq;
            let entry = match row_opt {
                Some(values) => RowEntry::Present { seq, values },
                None => RowEntry::Tombstone { seq },
            };
            mutations.push((pk, entry));
        }

        let wal_id = self.next_blob_id(CHANNEL_WAL)?;

        let batch_bytes = serde_json::to_vec(&WalBatch {
            mutations: mutations.clone(),
        })?;
        self.wal_striper.put_blob(wal_id, &batch_bytes)?;

        next_manifest.wal_blobs.push(wal_id);
        let encoded_manifest = serde_json::to_vec(&next_manifest)?;

        Ok(PreparedShardCommit {
            meta_key: self.meta_key.clone(),
            expected_mod_revision: self.manifest_revision,
            next_manifest,
            encoded_manifest,
            mutations,
        })
    }

    /// Post-commit hook: applies the committed manifest revision and MemTable updates.
    pub fn apply_prepared(
        &mut self,
        prep: PreparedShardCommit,
        new_revision: u64,
    ) -> Result<(), DbError> {
        self.manifest = prep.next_manifest;
        self.manifest_revision = new_revision;
        for (pk, entry) in prep.mutations {
            self.memtable.insert(pk, entry);
        }
        if self.memtable.len() >= self.flush_threshold_rows {
            self.flush_to_sstable()?;
        }
        Ok(())
    }

    /// Writes a single-shard batch to `walPool`, publishes with a revision guard,
    /// updates in-memory `MemTable`, and auto-flushes to an erasure-coded SSTable if threshold reached.
    pub fn apply_batch(
        &mut self,
        ops: Vec<(Vec<u8>, Option<Vec<SqlValue>>)>,
    ) -> Result<(), DbError> {
        if ops.is_empty() {
            return Ok(());
        }
        let prep = self.prepare_batch(ops)?;
        let revision = self.publish_prepared(&prep)?;
        self.apply_prepared(prep, revision)
    }

    fn publish_prepared(&self, prep: &PreparedShardCommit) -> Result<u64, DbError> {
        match self.meta.execute(KvCommand::RevisionTxn {
            guards: vec![RevisionGuard {
                key: prep.meta_key.clone(),
                expected_mod_revision: prep.expected_mod_revision,
            }],
            ops: vec![TxnOp::Put {
                key: prep.meta_key.clone(),
                value: prep.encoded_manifest.clone(),
            }],
        })? {
            KvResult::TxnOk { revision, .. } => Ok(revision),
            KvResult::CasConflict { .. } => Err(DbError::StaleTabletGeneration {
                owner_id: self.manifest.owner_id,
            }),
            _ => unreachable!(),
        }
    }

    /// Flushes `MemTable` into an immutable sorted SSTable blob on `dataPool` (`ReedSolomon`),
    /// clears obsoleted WAL blob references, and triggers background compaction if SST count >= 4!
    pub fn flush_to_sstable(&mut self) -> Result<bool, DbError> {
        if self.memtable.is_empty() {
            return Ok(false);
        }

        let entries: Vec<(Vec<u8>, RowEntry)> = self
            .memtable
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let min_key = entries.first().unwrap().0.clone();
        let max_key = entries.last().unwrap().0.clone();
        let row_count = entries.len();

        let sst_id = self.next_blob_id(CHANNEL_SST)?;
        let sst_bytes = serde_json::to_vec(&SstImage { entries })?;
        self.data_striper.put_blob(sst_id, &sst_bytes)?;

        let prev_wals = std::mem::take(&mut self.manifest.wal_blobs);
        self.manifest.sst_blobs.push(SstRef {
            blob_id: sst_id,
            min_key,
            max_key,
            row_count,
        });

        if let Err(e) = self.cas_save_manifest() {
            self.manifest.wal_blobs = prev_wals;
            self.manifest.sst_blobs.pop();
            return Err(e);
        }

        self.memtable.clear();

        if self.manifest.sst_blobs.len() >= AUTO_COMPACT_SST_THRESHOLD {
            let _ = self.compact()?;
        }

        Ok(true)
    }

    /// Merges SSTables + MemTable, dropping tombstones from the new image only.
    /// Old fragments are retained: remote readers can still hold old manifests.
    pub fn compact(&mut self) -> Result<CompactionReport, DbError> {
        if self.manifest.sst_blobs.is_empty() && self.memtable.is_empty() {
            return Ok(CompactionReport::default());
        }

        let merged_ssts = self.manifest.sst_blobs.len();
        let mut merged: BTreeMap<Vec<u8>, RowEntry> = BTreeMap::new();
        for sst_ref in &self.manifest.sst_blobs {
            let raw = self.data_striper.get_blob(sst_ref.blob_id)?;
            let img: SstImage = serde_json::from_slice(&raw)?;
            for (k, entry) in img.entries {
                merged.insert(k, entry);
            }
        }
        for (k, entry) in &self.memtable {
            merged.insert(k.clone(), entry.clone());
        }

        // No older SST remains in the NEW manifest. Retained old snapshots keep
        // their own immutable references and are never merged with this image.
        let live_entries: Vec<(Vec<u8>, RowEntry)> = merged
            .into_iter()
            .filter(|(_, e)| matches!(e, RowEntry::Present { .. }))
            .collect();
        let live_rows = live_entries.len();

        let new_gen = self
            .manifest
            .generation
            .checked_add(1)
            .filter(|n| *n <= BlobId::MAX_U24)
            .ok_or(DbError::CounterExhausted)?;

        let mut new_ssts = Vec::new();
        if !live_entries.is_empty() {
            let min_key = live_entries.first().unwrap().0.clone();
            let max_key = live_entries.last().unwrap().0.clone();
            let sst_id = self.next_blob_id(CHANNEL_SST)?;
            let sst_bytes = serde_json::to_vec(&SstImage {
                entries: live_entries,
            })?;
            self.data_striper.put_blob(sst_id, &sst_bytes)?;
            new_ssts.push(SstRef {
                blob_id: sst_id,
                min_key,
                max_key,
                row_count: live_rows,
            });
        }

        let prev_manifest = self.manifest.clone();
        self.manifest.generation = new_gen;
        self.manifest.last_step = 0;
        self.manifest.wal_blobs.clear();
        self.manifest.sst_blobs = new_ssts;

        if let Err(e) = self.cas_save_manifest() {
            self.manifest = prev_manifest;
            return Err(e);
        }

        self.memtable.clear();

        Ok(CompactionReport {
            merged_ssts,
            live_rows,
            evicted_fragments: 0,
        })
    }

    /// Fast single-row lookup checking `MemTable` first, then newest-to-oldest SSTables
    /// whose `[min_key..=max_key]` range overlaps `pk`.
    pub fn get_row(&self, pk: &[u8]) -> Result<Option<Vec<SqlValue>>, DbError> {
        if let Some(entry) = self.memtable.get(pk) {
            return Ok(match entry {
                RowEntry::Present { values, .. } => Some(values.clone()),
                RowEntry::Tombstone { .. } => None,
            });
        }

        for sst_ref in self.manifest.sst_blobs.iter().rev() {
            if pk < sst_ref.min_key.as_slice() || pk > sst_ref.max_key.as_slice() {
                continue; // Key outside SSTable key bounds -> zero disk I/O!
            }
            let raw = self.data_striper.get_blob(sst_ref.blob_id)?;
            let img: SstImage = serde_json::from_slice(&raw)?;
            if let Ok(idx) = img.entries.binary_search_by(|(k, _)| k.as_slice().cmp(pk)) {
                return Ok(match &img.entries[idx].1 {
                    RowEntry::Present { values, .. } => Some(values.clone()),
                    RowEntry::Tombstone { .. } => None,
                });
            }
        }

        Ok(None)
    }

    /// Merges all SSTables (oldest -> newest) + `MemTable` in sorted primary-key order,
    /// filtering out deleted tombstones.
    pub fn scan_all_live_rows(&self) -> Result<Vec<Vec<SqlValue>>, DbError> {
        let mut merged: BTreeMap<Vec<u8>, RowEntry> = BTreeMap::new();
        for sst_ref in &self.manifest.sst_blobs {
            let raw = self.data_striper.get_blob(sst_ref.blob_id)?;
            let img: SstImage = serde_json::from_slice(&raw)?;
            for (k, entry) in img.entries {
                merged.insert(k, entry);
            }
        }
        for (k, entry) in &self.memtable {
            merged.insert(k.clone(), entry.clone());
        }

        let mut rows = Vec::new();
        for (_, entry) in merged {
            if let RowEntry::Present { values, .. } = entry {
                rows.push(values);
            }
        }
        Ok(rows)
    }
}
