use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use yadb_blob::{BlobId, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, KvResult, LocalMetaKv};

use crate::sql::SqlValue;
use crate::DbError;

pub const CHANNEL_WAL: u8 = 1;
pub const CHANNEL_SST: u8 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RowEntry {
    Present {
        seq: u64,
        values: Vec<SqlValue>,
    },
    Tombstone {
        seq: u64,
    },
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
    pub last_step: u32,
    pub last_seq: u64,
    pub wal_pool: String,
    pub data_pool: String,
    pub wal_blobs: Vec<BlobId>,
    pub sst_blobs: Vec<SstRef>,
}

/// YDB-inspired `DataShard` tablet storing WAL segments on `walPool` (`channel = 1`, e.g. `Mirror-3`)
/// and immutable sorted SSTables on `dataPool` (`channel = 2`, e.g. `ReedSolomon 4+2`).
pub struct DataShardTablet<FS: StorageFs> {
    meta_key: String,
    manifest_ver: u64,
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
            // Bump tablet generation on recovery/takeover and seal in MetaKV via CAS!
            manifest.generation = manifest.generation.saturating_add(1).min(BlobId::MAX_U24);
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
            let next_ver = match meta.execute(KvCommand::CompareAndSwap {
                key: meta_key.clone(),
                expected_version: kv.version,
                new_value: Some(encoded),
            })? {
                KvResult::CasOk { version, .. } => version,
                KvResult::CasConflict { .. } => {
                    return Err(DbError::StaleTabletGeneration { owner_id });
                }
                _ => unreachable!(),
            };

            Ok(Self {
                meta_key,
                manifest_ver: next_ver,
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
            let ver = match meta.execute(KvCommand::CompareAndSwap {
                key: meta_key.clone(),
                expected_version: 0,
                new_value: Some(encoded),
            })? {
                KvResult::CasOk { version, .. } => version,
                KvResult::CasConflict { .. } => {
                    return Err(DbError::StaleTabletGeneration { owner_id });
                }
                _ => unreachable!(),
            };

            Ok(Self {
                meta_key,
                manifest_ver: ver,
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

    pub fn manifest_version(&self) -> u64 {
        self.manifest_ver
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
        let manifest: TabletManifest = serde_json::from_slice(&kv.value)?;
        let mut memtable = BTreeMap::new();
        for &wal_id in &manifest.wal_blobs {
            let raw = wal_striper.get_blob(wal_id)?;
            let batch: WalBatch = serde_json::from_slice(&raw)?;
            for (pk, entry) in batch.mutations {
                memtable.insert(pk, entry);
            }
        }
        Ok(Some(Self {
            meta_key,
            manifest_ver: kv.version,
            manifest,
            meta,
            wal_striper,
            data_striper,
            memtable,
            flush_threshold_rows: usize::MAX,
        }))
    }

    pub fn sst_count(&self) -> usize {
        self.manifest.sst_blobs.len()
    }

    pub fn wal_count(&self) -> usize {
        self.manifest.wal_blobs.len()
    }

    fn next_blob_id(&mut self, channel: u8) -> BlobId {
        self.manifest.last_step = (self.manifest.last_step + 1).min(BlobId::MAX_U24);
        BlobId::new(
            self.manifest.project_id,
            self.manifest.owner_id,
            self.manifest.generation,
            self.manifest.last_step,
            channel,
            0,
        )
    }

    fn cas_save_manifest(&mut self) -> Result<(), DbError> {
        let encoded = serde_json::to_vec(&self.manifest)?;
        match self.meta.execute(KvCommand::CompareAndSwap {
            key: self.meta_key.clone(),
            expected_version: self.manifest_ver,
            new_value: Some(encoded),
        })? {
            KvResult::CasOk { version, .. } => {
                self.manifest_ver = version;
                Ok(())
            }
            KvResult::CasConflict { .. } => Err(DbError::StaleTabletGeneration {
                owner_id: self.manifest.owner_id,
            }),
            _ => unreachable!(),
        }
    }

    /// Writes a batch of row upserts/deletes to `walPool` first, commits the WAL `BlobId` via CAS,
    /// updates in-memory `MemTable`, and auto-flushes to an erasure-coded SSTable if threshold reached.
    pub fn apply_batch(&mut self, ops: Vec<(Vec<u8>, Option<Vec<SqlValue>>)>) -> Result<(), DbError> {
        if ops.is_empty() {
            return Ok(());
        }

        let mut mutations = Vec::with_capacity(ops.len());
        for (pk, row_opt) in ops {
            self.manifest.last_seq += 1;
            let seq = self.manifest.last_seq;
            let entry = match row_opt {
                Some(values) => RowEntry::Present { seq, values },
                None => RowEntry::Tombstone { seq },
            };
            mutations.push((pk, entry));
        }

        let wal_id = self.next_blob_id(CHANNEL_WAL);
        let batch_bytes = serde_json::to_vec(&WalBatch {
            mutations: mutations.clone(),
        })?;
        self.wal_striper.put_blob(wal_id, &batch_bytes)?;

        self.manifest.wal_blobs.push(wal_id);
        if let Err(e) = self.cas_save_manifest() {
            self.manifest.wal_blobs.pop();
            return Err(e);
        }

        for (pk, entry) in mutations {
            self.memtable.insert(pk, entry);
        }

        if self.memtable.len() >= self.flush_threshold_rows {
            self.flush_to_sstable()?;
        }

        Ok(())
    }

    /// Flushes `MemTable` into an immutable sorted SSTable blob on `dataPool` (`ReedSolomon`)
    /// and clears obsoleted WAL blob references!
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

        let sst_id = self.next_blob_id(CHANNEL_SST);
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
        Ok(true)
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
