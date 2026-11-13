use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yadb_blob::{BlobError, BlobId, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, KvResult, LocalMetaKv, MetaError, RevisionGuard, TxnOp};

pub const DEFAULT_BLOCK_SIZE: u32 = 4096;
// Separate from legacy block channel 3 and S3 channel 4. Owner + generation
// encode a project-wide allocation; step is the block ordinal in that allocation.
const BLOCK_CHANNEL: u8 = 5;
const MAX_ALLOCATION: u64 = (1u64 << 56) - 1;

#[derive(Debug, Error)]
pub enum BlockError {
    #[error("concurrent volume update; retry the request")]
    Conflict,

    #[error("block blob allocation space exhausted or write has too many blocks")]
    GenerationExhausted,
    #[error("blob engine error: {0}")]
    Blob(#[from] BlobError),

    #[error("metakv error: {0}")]
    Meta(#[from] MetaError),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("storage pool `{0}` is not mounted on this node")]
    PoolNotMounted(String),

    #[error("I/O offset {offset} + len {len} exceeds volume capacity {size_bytes}")]
    OutOfBounds {
        offset: u64,
        len: usize,
        size_bytes: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeExtentMap {
    pub block_size: u32,
    /// Legacy maps used this as a per-volume epoch. New maps store the low
    /// allocation bits; it is not an ownership or garbage-collection fence.
    pub generation: u32,
    /// Sparse mapping from logical `block_index` -> immutable `BlobId` in `PoolStriper`.
    /// Unmapped blocks are thin-provisioned zeros!
    pub blocks: BTreeMap<u64, BlobId>,
}

/// Thin-provisioned Copy-on-Write Virtual Block Device Engine (YDB NBS analog)
/// backed by `PoolStriper` (`Mirror` or `ReedSolomon`) + `MetaKV` extent maps.
pub struct BlockVolumeEngine<FS: StorageFs> {
    meta: LocalMetaKv<FS>,
    pools: Arc<RwLock<BTreeMap<String, Arc<PoolStriper<FS>>>>>,
    block_size: u32,
}

impl<FS: StorageFs> Clone for BlockVolumeEngine<FS> {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            pools: self.pools.clone(),
            block_size: self.block_size,
        }
    }
}

impl<FS: StorageFs> BlockVolumeEngine<FS> {
    pub fn new(meta: LocalMetaKv<FS>, block_size: u32) -> Self {
        Self {
            meta,
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            block_size: block_size.max(512),
        }
    }

    pub fn register_pool(&self, name: impl Into<String>, striper: Arc<PoolStriper<FS>>) {
        self.pools.write().unwrap().insert(name.into(), striper);
    }

    fn get_pool(&self, name: &str) -> Result<Arc<PoolStriper<FS>>, BlockError> {
        self.pools
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| BlockError::PoolNotMounted(name.to_string()))
    }

    fn meta_key(project_id: u32, volume: &str) -> String {
        format!("/block/{project_id}/{volume}")
    }

    fn load_map(&self, project_id: u32, volume: &str) -> Result<VolumeExtentMap, BlockError> {
        Ok(self.load_map_snapshot(project_id, volume)?.1)
    }

    // The guard and data must come from the same read, including the absent case.
    fn load_map_snapshot(
        &self,
        project_id: u32,
        volume: &str,
    ) -> Result<(u64, VolumeExtentMap), BlockError> {
        let key = Self::meta_key(project_id, volume);
        match self.meta.get(&key) {
            Some(kv) => Ok((kv.mod_revision, serde_json::from_slice(&kv.value)?)),
            None => Ok((
                0,
                VolumeExtentMap {
                    block_size: self.block_size,
                    generation: 1,
                    blocks: BTreeMap::new(),
                },
            )),
        }
    }

    fn publish_map(
        &self,
        key: String,
        expected: u64,
        map: &VolumeExtentMap,
    ) -> Result<bool, BlockError> {
        Ok(matches!(
            self.meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: key.clone(),
                    expected_mod_revision: expected
                }],
                ops: vec![TxnOp::Put {
                    key,
                    value: serde_json::to_vec(map)?
                }],
            })?,
            KvResult::TxnOk { .. }
        ))
    }

    /// Copy-on-Write arbitrary byte range write across 4 KiB logical blocks.
    pub fn write_at(
        &self,
        project_id: u32,
        volume: &str,
        pool_name: &str,
        volume_size_bytes: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, BlockError> {
        if offset
            .checked_add(data.len() as u64)
            .is_none_or(|end| end > volume_size_bytes)
        {
            return Err(BlockError::OutOfBounds {
                offset,
                len: data.len(),
                size_bytes: volume_size_bytes,
            });
        }
        if data.is_empty() {
            return Ok(0);
        }

        let pool = self.get_pool(pool_name)?;
        for _ in 0..32 {
            let key = Self::meta_key(project_id, volume);
            let (revision, mut map) = self.load_map_snapshot(project_id, volume)?;
            let bs = map.block_size as u64;
            let blocks = ((offset % bs) as u128 + data.len() as u128).div_ceil(bs as u128);
            if blocks > BlobId::MAX_U24 as u128 {
                return Err(BlockError::GenerationExhausted);
            }
            let allocation = self.reserve_allocation(project_id)?;
            let owner = (allocation >> 24) as u32;
            map.generation = (allocation & BlobId::MAX_U24 as u64) as u32;

            let mut cursor = 0usize;
            let mut step = 0u32;
            while cursor < data.len() {
                let abs_pos = offset + cursor as u64;
                let blk_idx = abs_pos / bs;
                let in_blk_off = (abs_pos % bs) as usize;
                let copy_len = (data.len() - cursor).min(bs as usize - in_blk_off);

                let mut blk_buf = if in_blk_off != 0 || copy_len < bs as usize {
                    if let Some(&prev_id) = map.blocks.get(&blk_idx) {
                        pool.get_blob(prev_id)?
                    } else {
                        vec![0u8; bs as usize]
                    }
                } else {
                    vec![0u8; bs as usize]
                };

                blk_buf[in_blk_off..in_blk_off + copy_len]
                    .copy_from_slice(&data[cursor..cursor + copy_len]);

                step += 1; // bounded above before reserving or writing anything
                let blob_id =
                    BlobId::new(project_id, owner, map.generation, step, BLOCK_CHANNEL, 0);
                pool.put_blob(blob_id, &blk_buf)?;
                map.blocks.insert(blk_idx, blob_id);

                cursor += copy_len;
            }

            if self.publish_map(key, revision, &map)? {
                return Ok(data.len());
            }
        }
        Err(BlockError::Conflict)
    }

    // Persist the allocation BEFORE writing fragments. Failed publications and process restarts
    // must never reuse an immutable BlobId for different bytes.
    // Never delete/reset this key, even when all volume metadata is removed.
    // An ambiguous reservation may consume IDs; it must not be used without a
    // confirmed TxnOk. Allocation is independent of names, pools and map lifetime.
    fn reserve_allocation(&self, pid: u32) -> Result<u64, BlockError> {
        let key = format!("/block-write-sequence/{pid}");
        for _ in 0..32 {
            let cur = self.meta.get(&key);
            let revision = cur.as_ref().map_or(0, |v| v.mod_revision);
            let prev = match cur {
                Some(v) => serde_json::from_slice::<u64>(&v.value)?,
                None => 0,
            };
            let next = prev
                .checked_add(1)
                .filter(|n| *n <= MAX_ALLOCATION)
                .ok_or(BlockError::GenerationExhausted)?;
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
                return Ok(next);
            }
        }
        Err(BlockError::Conflict)
    }

    /// Discard aligned full blocks. Partial-block discard is rejected, not rounded outward.
    pub fn unmap_at(
        &self,
        pid: u32,
        volume: &str,
        size: u64,
        offset: u64,
        len: u64,
    ) -> Result<(), BlockError> {
        if offset.checked_add(len).is_none_or(|end| end > size) {
            return Err(BlockError::OutOfBounds {
                offset,
                len: usize::try_from(len).unwrap_or(usize::MAX),
                size_bytes: size,
            });
        }
        let key = Self::meta_key(pid, volume);
        for _ in 0..32 {
            let (revision, mut map) = self.load_map_snapshot(pid, volume)?;
            let bs = map.block_size as u64;
            if offset % bs != 0 || len % bs != 0 {
                return Err(BlockError::Conflict);
            }
            let end = (offset + len) / bs;
            map.blocks
                .retain(|index, _| *index < offset / bs || *index >= end);
            if self.publish_map(key.clone(), revision, &map)? {
                return Ok(());
            }
        }
        Err(BlockError::Conflict)
    }

    /// Reads arbitrary byte range across logical blocks; unmapped blocks return zero bytes with 0 disk I/O!
    pub fn read_at(
        &self,
        project_id: u32,
        volume: &str,
        pool_name: &str,
        volume_size_bytes: u64,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, BlockError> {
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > volume_size_bytes)
        {
            return Err(BlockError::OutOfBounds {
                offset,
                len,
                size_bytes: volume_size_bytes,
            });
        }
        if len == 0 {
            return Ok(Vec::new());
        }

        let pool = self.get_pool(pool_name)?;
        let map = self.load_map(project_id, volume)?;
        let bs = map.block_size as u64;

        let mut out = vec![0u8; len];
        let mut cursor = 0usize;
        while cursor < len {
            let abs_pos = offset + cursor as u64;
            let blk_idx = abs_pos / bs;
            let in_blk_off = (abs_pos % bs) as usize;
            let copy_len = (len - cursor).min(bs as usize - in_blk_off);

            if let Some(&blob_id) = map.blocks.get(&blk_idx) {
                let blk_data = pool.get_blob(blob_id)?;
                out[cursor..cursor + copy_len]
                    .copy_from_slice(&blk_data[in_blk_off..in_blk_off + copy_len]);
            }
            cursor += copy_len;
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yadb_blob::{ErasureScheme, VolumeConfig, VolumeStore};
    use yadb_io::{FaultConfig, SimFs, SyncFault};

    fn pool(fs: &SimFs) -> Arc<PoolStriper<SimFs>> {
        let domains = (0..3)
            .map(|i| {
                VolumeStore::open(
                    fs.clone(),
                    format!("collision-d{i}"),
                    VolumeConfig::default(),
                )
                .unwrap()
            })
            .collect();
        Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap())
    }

    #[test]
    fn colliding_volume_names_have_independent_fragment_ids() {
        let fs = SimFs::new(8801);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let pool = pool(&fs);
        let a = "disk-1645d3b62cc3efdf";
        let b = "disk-bec5d8b945d60d3e";
        assert_ne!(a, b);
        assert_eq!(
            crc32fast::hash(format!("vol:{a}").as_bytes()),
            crc32fast::hash(format!("vol:{b}").as_bytes())
        );
        let first = BlockVolumeEngine::new(meta.clone(), 4096);
        first.register_pool("data", pool.clone());
        first
            .write_at(1, a, "data", 8192, 0, b"first-volume")
            .unwrap();
        let second = BlockVolumeEngine::new(meta, 4096);
        second.register_pool("data", pool);
        second
            .write_at(1, b, "data", 8192, 0, b"second-volume")
            .unwrap();
        assert_eq!(
            first.read_at(1, a, "data", 8192, 0, 12).unwrap(),
            b"first-volume"
        );
        assert_eq!(
            second.read_at(1, b, "data", 8192, 0, 13).unwrap(),
            b"second-volume"
        );
        assert_ne!(
            first.load_map(1, a).unwrap().blocks[&0],
            second.load_map(1, b).unwrap().blocks[&0]
        );
    }

    #[test]
    fn stale_extent_publication_rejects_delete_recreate_aba() {
        let fs = SimFs::new(8802);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let engine = BlockVolumeEngine::new(meta.clone(), 4096);
        let key = BlockVolumeEngine::<SimFs>::meta_key(1, "disk");
        let old = engine.load_map(1, "disk").unwrap();
        meta.execute(KvCommand::Put {
            key: key.clone(),
            value: serde_json::to_vec(&old).unwrap(),
        })
        .unwrap();
        let observed = meta.get(&key).unwrap();
        assert_eq!(observed.version, observed.mod_revision);
        meta.execute(KvCommand::Delete { key: key.clone() })
            .unwrap();
        let mut replacement = old.clone();
        replacement.blocks.insert(9, BlobId::new(1, 10, 1, 1, 3, 0));
        meta.execute(KvCommand::Put {
            key: key.clone(),
            value: serde_json::to_vec(&replacement).unwrap(),
        })
        .unwrap();
        assert_eq!(meta.get(&key).unwrap().version, observed.version);
        // Both stale RMW and stale UNMAP must fail, even if the per-key version repeats.
        assert!(!engine
            .publish_map(key.clone(), observed.mod_revision, &old)
            .unwrap());
        assert_eq!(
            engine.load_map(1, "disk").unwrap().blocks,
            replacement.blocks
        );
    }

    #[test]
    fn allocation_rollover_exhaustion_and_overflow_fail_without_wrap() {
        let fs = SimFs::new(8803);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let engine = BlockVolumeEngine::new(meta.clone(), 4096);
        engine.register_pool("data", pool(&fs));
        let key = "/block-write-sequence/1".to_string();
        // Seed the boundary directly; reserving millions of IDs is unnecessary.
        meta.execute(KvCommand::Put {
            key: key.clone(),
            value: serde_json::to_vec(&(BlobId::MAX_U24 as u64)).unwrap(),
        })
        .unwrap();
        engine
            .write_at(1, "roll", "data", 8192, 0, b"rollover")
            .unwrap();
        let id = engine.load_map(1, "roll").unwrap().blocks[&0];
        assert_eq!(
            (id.owner_id(), id.generation(), id.step(), id.channel()),
            (1, 0, 1, BLOCK_CHANNEL)
        );
        meta.execute(KvCommand::Put {
            key: key.clone(),
            value: serde_json::to_vec(&(MAX_ALLOCATION - 1)).unwrap(),
        })
        .unwrap();
        engine
            .write_at(1, "last", "data", 8192, 0, b"last")
            .unwrap();
        let id = engine.load_map(1, "last").unwrap().blocks[&0];
        assert_eq!(
            (id.owner_id(), id.generation()),
            (u32::MAX, BlobId::MAX_U24)
        );
        assert!(matches!(
            engine.write_at(1, "last", "data", 8192, 0, b"overflow"),
            Err(BlockError::GenerationExhausted)
        ));
        assert_eq!(
            engine.read_at(1, "last", "data", 8192, 0, 4).unwrap(),
            b"last"
        );
        assert_eq!(
            serde_json::from_slice::<u64>(&meta.get(&key).unwrap().value).unwrap(),
            MAX_ALLOCATION
        );
        assert!(matches!(
            engine.write_at(1, "last", "data", u64::MAX, u64::MAX, b"x"),
            Err(BlockError::OutOfBounds { .. })
        ));
        assert!(matches!(
            engine.read_at(1, "last", "data", u64::MAX, u64::MAX, 1),
            Err(BlockError::OutOfBounds { .. })
        ));
    }

    #[test]
    fn legacy_extents_remain_readable_after_cow_and_recovery() {
        let fs = SimFs::new(8804);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let pool = pool(&fs);
        let legacy_id = BlobId::new(1, 1234, 7, 1, 3, 0);
        pool.put_blob(legacy_id, &[b'l'; 4096]).unwrap();
        let legacy = VolumeExtentMap {
            block_size: 4096,
            generation: 7,
            blocks: [(0, legacy_id), (1, legacy_id)].into_iter().collect(),
        };
        meta.execute(KvCommand::Put {
            key: BlockVolumeEngine::<SimFs>::meta_key(1, "legacy"),
            value: serde_json::to_vec(&legacy).unwrap(),
        })
        .unwrap();
        let engine = BlockVolumeEngine::new(meta, 4096);
        engine.register_pool("data", pool.clone());
        engine
            .write_at(1, "legacy", "data", 8192, 1, b"new")
            .unwrap();
        assert_eq!(
            engine.read_at(1, "legacy", "data", 8192, 0, 5).unwrap(),
            b"lnewl"
        );
        let map = engine.load_map(1, "legacy").unwrap();
        assert_eq!(map.blocks[&0].channel(), BLOCK_CHANNEL);
        assert_eq!(map.blocks[&1], legacy_id);
        let first_id = map.blocks[&0];
        drop(engine);
        drop(pool);
        fs.simulate_power_cut();
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let engine = BlockVolumeEngine::new(meta, 4096);
        engine.register_pool("data", self::pool(&fs));
        assert_eq!(
            engine.read_at(1, "legacy", "data", 8192, 0, 5).unwrap(),
            b"lnewl"
        );
        assert_eq!(
            engine.read_at(1, "legacy", "data", 8192, 4096, 4).unwrap(),
            b"llll"
        );
        engine
            .write_at(1, "legacy", "data", 8192, 1, b"two")
            .unwrap();
        assert_ne!(engine.load_map(1, "legacy").unwrap().blocks[&0], first_id);
    }

    #[test]
    fn failed_fragment_write_consumes_allocation_without_publishing() {
        let meta_fs = SimFs::new(8805);
        let data_fs = SimFs::new(8806);
        let meta = LocalMetaKv::open_single_node(&meta_fs, "meta", 1).unwrap();
        let engine = BlockVolumeEngine::new(meta.clone(), 4096);
        let pool = pool(&data_fs);
        engine.register_pool("data", pool.clone());
        data_fs.set_faults(FaultConfig {
            write_eio_prob: 1.0,
            ..Default::default()
        });
        assert!(engine
            .write_at(1, "disk", "data", 8192, 0, b"failed")
            .is_err());
        assert!(meta
            .get(&BlockVolumeEngine::<SimFs>::meta_key(1, "disk"))
            .is_none());
        assert_eq!(
            serde_json::from_slice::<u64>(&meta.get("/block-write-sequence/1").unwrap().value)
                .unwrap(),
            1
        );
        data_fs.set_faults(FaultConfig::default());
        engine
            .write_at(1, "disk", "data", 8192, 0, b"success")
            .unwrap();
        let old = engine.load_map(1, "disk").unwrap().blocks[&0];
        assert_eq!(old.generation(), 2);
        // Deleting all user metadata must not reset allocation identity.
        meta.execute(KvCommand::Delete {
            key: BlockVolumeEngine::<SimFs>::meta_key(1, "disk"),
        })
        .unwrap();
        engine
            .write_at(1, "disk", "data", 8192, 0, b"recreated")
            .unwrap();
        assert_ne!(engine.load_map(1, "disk").unwrap().blocks[&0], old);
        assert_eq!(&pool.get_blob(old).unwrap()[..7], b"success");
    }

    #[test]
    fn ambiguous_allocation_sync_never_starts_fragment_writes() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let meta_fs = SimFs::new(8807);
            let data_fs = SimFs::new(8808);
            let meta = LocalMetaKv::open_single_node(&meta_fs, "meta", 1).unwrap();
            let engine = BlockVolumeEngine::new(meta, 4096);
            let pool = pool(&data_fs);
            engine.register_pool("data", pool.clone());
            meta_fs.set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            assert!(matches!(
                engine.write_at(1, "disk", "data", 8192, 0, b"unknown"),
                Err(BlockError::Meta(MetaError::Io(_)))
            ));
            for domain in pool.domains() {
                assert_eq!(domain.local_store().unwrap().stats().unwrap().live_blobs, 0);
            }
            drop(engine);
            meta_fs.set_faults(FaultConfig::default());
            meta_fs.simulate_power_cut();
            let meta = LocalMetaKv::open_single_node(&meta_fs, "meta", 1).unwrap();
            let engine = BlockVolumeEngine::new(meta, 4096);
            engine.register_pool("data", pool);
            engine
                .write_at(1, "disk", "data", 8192, 0, b"confirmed")
                .unwrap();
            let id = engine.load_map(1, "disk").unwrap().blocks[&0];
            assert_eq!(
                id.generation(),
                if fault == SyncFault::AfterPersist {
                    2
                } else {
                    1
                }
            );
        }
    }

    #[test]
    fn independent_allocators_reserve_disjoint_ids_concurrently() {
        let fs = SimFs::new(8809);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let threads = (0..4)
            .map(|_| {
                let meta = meta.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let engine = BlockVolumeEngine::new(meta, 4096);
                    barrier.wait();
                    (0..16)
                        .map(|_| engine.reserve_allocation(1).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        let mut ids = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (1..=64).collect::<Vec<_>>());
    }

    #[test]
    fn publication_sync_error_recovers_without_reusing_or_reclaiming_fragments() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let meta_fs = SimFs::new(8810);
            let data_fs = SimFs::new(8811);
            let meta = LocalMetaKv::open_single_node(&meta_fs, "meta", 1).unwrap();
            let engine = BlockVolumeEngine::new(meta, 4096);
            let pool = pool(&data_fs);
            engine.register_pool("data", pool.clone());
            engine.write_at(1, "disk", "data", 8192, 0, b"old").unwrap();
            let (revision, mut map) = engine.load_map_snapshot(1, "disk").unwrap();
            let old_id = map.blocks[&0];
            let allocation = engine.reserve_allocation(1).unwrap();
            let id = BlobId::new(
                1,
                (allocation >> 24) as u32,
                (allocation & BlobId::MAX_U24 as u64) as u32,
                1,
                BLOCK_CHANNEL,
                0,
            );
            pool.put_blob(id, &[b'n'; 4096]).unwrap();
            map.blocks.insert(0, id);
            map.generation = id.generation();
            meta_fs.set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            assert!(engine
                .publish_map(
                    BlockVolumeEngine::<SimFs>::meta_key(1, "disk"),
                    revision,
                    &map
                )
                .is_err());
            drop(engine);
            meta_fs.set_faults(FaultConfig::default());
            meta_fs.simulate_power_cut();
            let meta = LocalMetaKv::open_single_node(&meta_fs, "meta", 1).unwrap();
            let engine = BlockVolumeEngine::new(meta, 4096);
            engine.register_pool("data", pool.clone());
            assert_eq!(
                engine.read_at(1, "disk", "data", 8192, 0, 3).unwrap(),
                if fault == SyncFault::AfterPersist {
                    b"nnn"
                } else {
                    b"old"
                }
            );
            engine
                .write_at(1, "disk", "data", 8192, 0, b"next")
                .unwrap();
            let next_id = engine.load_map(1, "disk").unwrap().blocks[&0];
            assert_ne!(next_id, id);
            assert_ne!(next_id, old_id);
            assert_eq!(&pool.get_blob(old_id).unwrap()[..3], b"old");
            assert_eq!(pool.get_blob(id).unwrap(), vec![b'n'; 4096]);
        }
    }

    #[test]
    fn thin_provisioned_cow_block_volume_reads_zeros_and_spans_blocks() {
        let fs = SimFs::new(4242);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let engine = BlockVolumeEngine::new(meta, 4096);

        let mut doms = Vec::new();
        for i in 0..3 {
            doms.push(
                VolumeStore::open(fs.clone(), format!("m{i}"), VolumeConfig::default()).unwrap(),
            );
        }
        let pool = Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, doms).unwrap());
        engine.register_pool("wal-fast", pool);

        let tib = 1024 * 1024 * 1024 * 1024u64; // 1 TiB thin-provisioned volume!
        let zeros = engine
            .read_at(1, "boot-disk", "wal-fast", tib, 500_000_000, 128)
            .unwrap();
        assert!(zeros.iter().all(|&b| b == 0));

        // Unaligned cross-block write spanning block 0 and block 1 (offset 4090..4102)
        engine
            .write_at(1, "boot-disk", "wal-fast", tib, 4090, b"HELLO-BLOCKS")
            .unwrap();
        let read_back = engine
            .read_at(1, "boot-disk", "wal-fast", tib, 4088, 16)
            .unwrap();
        assert_eq!(&read_back[..2], &[0, 0]);
        assert_eq!(&read_back[2..14], b"HELLO-BLOCKS");
        assert_eq!(&read_back[14..], &[0, 0]);
    }

    #[test]
    fn concurrent_partial_writes_restart_and_unmap() {
        let fs = SimFs::new(777);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let engine = BlockVolumeEngine::new(meta.clone(), 4096);
        let domains = (0..3)
            .map(|i| {
                VolumeStore::open(fs.clone(), format!("d{i}"), VolumeConfig::default()).unwrap()
            })
            .collect();
        let pool =
            Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
        engine.register_pool("data", pool.clone());
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let threads = (0..4)
            .map(|i| {
                let engine = engine.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    engine
                        .write_at(1, "disk", "data", 8192, i * 512, &vec![i as u8 + 1; 512])
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();
        for t in threads {
            t.join().unwrap();
        }
        let expected = (0..4).flat_map(|i| vec![i + 1; 512]).collect::<Vec<_>>();
        assert_eq!(
            engine.read_at(1, "disk", "data", 8192, 0, 2048).unwrap(),
            expected
        );
        let reopened = BlockVolumeEngine::new(meta, 4096);
        reopened.register_pool("data", pool);
        reopened
            .write_at(1, "disk", "data", 8192, 2048, b"restart")
            .unwrap();
        assert_eq!(
            reopened.read_at(1, "disk", "data", 8192, 0, 2048).unwrap(),
            expected
        );
        assert!(reopened.unmap_at(1, "disk", 8192, 512, 4096).is_err());
        reopened.unmap_at(1, "disk", 8192, 0, 4096).unwrap();
        assert_eq!(
            reopened.read_at(1, "disk", "data", 8192, 0, 4096).unwrap(),
            vec![0; 4096]
        );
    }
}
