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
const AUTO_COMPACT_SST_THRESHOLD: usize = 4;

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

#[derive(Debug, Clone, Copy, Default)]
pub struct CompactionReport {
    pub merged_ssts: usize,
    pub live_rows: usize,
    pub evicted_fragments: usize,
}

/// Phase-1 prepared WAL write for atomic cross-shard 2PC commit via `MetaKV::Txn`.
pub struct PreparedShardCommit {
    pub meta_key: String,
    pub expected_version: u64,
    pub next_manifest: TabletManifest,
    pub encoded_manifest: Vec<u8>,
    pub mutations: Vec<(Vec<u8>, RowEntry)>,
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

    pub fn owner_id(&self) -> u32 {
        self.manifest.owner_id
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

    /// Phase 1 of a multi-shard transaction: writes the immutable WAL batch to `walPool`
    /// without exposing it in `MetaKV` or `MemTable` yet!
    pub fn prepare_batch(
        &self,
        ops: Vec<(Vec<u8>, Option<Vec<SqlValue>>)>,
    ) -> Result<PreparedShardCommit, DbError> {
        let mut next_manifest = self.manifest.clone();
        let mut mutations = Vec::with_capacity(ops.len());
        for (pk, row_opt) in ops {
            next_manifest.last_seq += 1;
            let seq = next_manifest.last_seq;
            let entry = match row_opt {
                Some(values) => RowEntry::Present { seq, values },
                None => RowEntry::Tombstone { seq },
            };
            mutations.push((pk, entry));
        }

        next_manifest.last_step = (next_manifest.last_step + 1).min(BlobId::MAX_U24);
        let wal_id = BlobId::new(
            next_manifest.project_id,
            next_manifest.owner_id,
            next_manifest.generation,
            next_manifest.last_step,
            CHANNEL_WAL,
            0,
        );

        let batch_bytes = serde_json::to_vec(&WalBatch {
            mutations: mutations.clone(),
        })?;
        self.wal_striper.put_blob(wal_id, &batch_bytes)?;

        next_manifest.wal_blobs.push(wal_id);
        let encoded_manifest = serde_json::to_vec(&next_manifest)?;

        Ok(PreparedShardCommit {
            meta_key: self.meta_key.clone(),
            expected_version: self.manifest_ver,
            next_manifest,
            encoded_manifest,
            mutations,
        })
    }

    /// Phase 2 post-commit hook: applies the committed manifest version and `MemTable` updates!
    pub fn apply_prepared(
        &mut self,
        prep: PreparedShardCommit,
        new_version: u64,
    ) -> Result<(), DbError> {
        self.manifest = prep.next_manifest;
        self.manifest_ver = new_version;
        for (pk, entry) in prep.mutations {
            self.memtable.insert(pk, entry);
        }
        if self.memtable.len() >= self.flush_threshold_rows {
            self.flush_to_sstable()?;
        }
        Ok(())
    }

    /// Writes a single-shard batch of row upserts/deletes to `walPool`, commits via CAS,
    /// updates in-memory `MemTable`, and auto-flushes to an erasure-coded SSTable if threshold reached.
    pub fn apply_batch(&mut self, ops: Vec<(Vec<u8>, Option<Vec<SqlValue>>)>) -> Result<(), DbError> {
        if ops.is_empty() {
            return Ok(());
        }
        let prep = self.prepare_batch(ops)?;
        match self.meta.execute(KvCommand::CompareAndSwap {
            key: prep.meta_key.clone(),
            expected_version: prep.expected_version,
            new_value: Some(prep.encoded_manifest.clone()),
        })? {
            KvResult::CasOk { version, .. } => self.apply_prepared(prep, version),
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

        if self.manifest.sst_blobs.len() >= AUTO_COMPACT_SST_THRESHOLD {
            let _ = self.compact()?;
        }

        Ok(true)
    }

    /// Merges all SSTables + `MemTable` into a single clean erasure-coded SSTable (purging tombstones
    /// and overwritten versions), advances the tablet `generation`, and issues a GC barrier across all
    /// `walPool` and `dataPool` VDisks (+ segment compaction) to physically reclaim disk space!
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

        // Drop all tombstones since no older SSTable remains underneath!
        let live_entries: Vec<(Vec<u8>, RowEntry)> = merged
            .into_iter()
            .filter(|(_, e)| matches!(e, RowEntry::Present { .. }))
            .collect();
        let live_rows = live_entries.len();

        let old_gen = self.manifest.generation;
        let new_gen = old_gen.saturating_add(1).min(BlobId::MAX_U24);

        let mut new_ssts = Vec::new();
        let mut new_step = 0u32;
        if !live_entries.is_empty() {
            new_step = 1;
            let min_key = live_entries.first().unwrap().0.clone();
            let max_key = live_entries.last().unwrap().0.clone();
            let sst_id = BlobId::new(
                self.manifest.project_id,
                self.manifest.owner_id,
                new_gen,
                new_step,
                CHANNEL_SST,
                0,
            );
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
        self.manifest.last_step = new_step;
        self.manifest.wal_blobs.clear();
        self.manifest.sst_blobs = new_ssts;

        if let Err(e) = self.cas_save_manifest() {
            self.manifest = prev_manifest;
            return Err(e);
        }

        self.memtable.clear();

        // Issue generation GC barrier across all WAL and SST VDisk slots + compact local segments!
        let pid = self.manifest.project_id;
        let oid = self.manifest.owner_id;
        let mut evicted_fragments = 0usize;

        for slot in self.wal_striper.slots() {
            if let Ok(n) = slot.collect_garbage_barrier(pid, oid, CHANNEL_WAL, old_gen) {
                evicted_fragments += n;
            }
            if let Some(local) = slot.local_store() {
                let _ = local.compact_sealed_segments();
            }
        }
        for slot in self.data_striper.slots() {
            if let Ok(n) = slot.collect_garbage_barrier(pid, oid, CHANNEL_SST, old_gen) {
                evicted_fragments += n;
            }
            if let Some(local) = slot.local_store() {
                let _ = local.compact_sealed_segments();
            }
        }

        Ok(CompactionReport {
            merged_ssts,
            live_rows,
            evicted_fragments,
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
