use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yadb_blob::{BlobError, BlobId, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, KvResult, LocalMetaKv, MetaError};

pub const DEFAULT_BLOCK_SIZE: u32 = 4096;

#[derive(Debug, Error)]
pub enum BlockError {
    #[error("concurrent volume update; retry the request")]
    Conflict,

    #[error("volume blob generation space exhausted")]
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
    step_seq: Arc<AtomicU32>,
}

impl<FS: StorageFs> Clone for BlockVolumeEngine<FS> {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            pools: self.pools.clone(),
            block_size: self.block_size,
            step_seq: self.step_seq.clone(),
        }
    }
}

impl<FS: StorageFs> BlockVolumeEngine<FS> {
    pub fn new(meta: LocalMetaKv<FS>, block_size: u32) -> Self {
        Self {
            meta,
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            block_size: block_size.max(512),
            step_seq: Arc::new(AtomicU32::new(1)),
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

    fn owner_id(volume: &str) -> u32 {
        crc32fast::hash(format!("vol:{volume}").as_bytes()).max(1)
    }

    fn load_map(&self, project_id: u32, volume: &str) -> Result<VolumeExtentMap, BlockError> {
        let key = Self::meta_key(project_id, volume);
        match self.meta.get(&key) {
            Some(kv) => Ok(serde_json::from_slice(&kv.value)?),
            None => Ok(VolumeExtentMap {
                block_size: self.block_size,
                generation: 1,
                blocks: BTreeMap::new(),
            }),
        }
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
        if offset.saturating_add(data.len() as u64) > volume_size_bytes {
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
            let current = self.meta.get(&key);
            let version = current.as_ref().map_or(0, |v| v.version);
            let mut map = match current {
                Some(kv) => serde_json::from_slice::<VolumeExtentMap>(&kv.value)?,
                None => self.load_map(project_id, volume)?,
            };
            map.generation = self.reserve_generation(project_id, volume, map.generation)?;
            let bs = map.block_size as u64;
            let owner = Self::owner_id(volume);

            let mut cursor = 0usize;
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

                let step = (self.step_seq.fetch_add(1, Ordering::Relaxed) % BlobId::MAX_U24).max(1);
                let blob_id = BlobId::new(project_id, owner, map.generation, step, 3, 0);
                pool.put_blob(blob_id, &blk_buf)?;
                map.blocks.insert(blk_idx, blob_id);

                cursor += copy_len;
            }

            let encoded = serde_json::to_vec(&map)?;
            match self.meta.execute(KvCommand::CompareAndSwap {
                key,
                expected_version: version,
                new_value: Some(encoded),
            })? {
                KvResult::CasOk { .. } => return Ok(data.len()),
                KvResult::CasConflict { .. } => continue,
                _ => unreachable!(),
            }
        }
        Err(BlockError::Conflict)
    }

    // Persist the allocation BEFORE writing fragments. Failed CAS writes and process restarts
    // must never reuse an immutable BlobId for different bytes.
    fn reserve_generation(&self, pid: u32, volume: &str, floor: u32) -> Result<u32, BlockError> {
        let key = format!("/block-alloc/{pid}/{volume}");
        for _ in 0..32 {
            let cur = self.meta.get(&key);
            let version = cur.as_ref().map_or(0, |v| v.version);
            let prev = match cur {
                Some(v) => serde_json::from_slice::<u32>(&v.value)?,
                None => floor,
            };
            let next = prev
                .max(floor)
                .checked_add(1)
                .filter(|n| *n <= BlobId::MAX_U24)
                .ok_or(BlockError::GenerationExhausted)?;
            if matches!(
                self.meta.execute(KvCommand::CompareAndSwap {
                    key: key.clone(),
                    expected_version: version,
                    new_value: Some(serde_json::to_vec(&next)?),
                })?,
                KvResult::CasOk { .. }
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
            let cur = self.meta.get(&key);
            let version = cur.as_ref().map_or(0, |v| v.version);
            let mut map = self.load_map(pid, volume)?;
            let bs = map.block_size as u64;
            if offset % bs != 0 || len % bs != 0 {
                return Err(BlockError::Conflict);
            }
            let end = (offset + len) / bs;
            map.blocks
                .retain(|index, _| *index < offset / bs || *index >= end);
            if matches!(
                self.meta.execute(KvCommand::CompareAndSwap {
                    key: key.clone(),
                    expected_version: version,
                    new_value: Some(serde_json::to_vec(&map)?),
                })?,
                KvResult::CasOk { .. }
            ) {
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
        if offset.saturating_add(len as u64) > volume_size_bytes {
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
    use yadb_io::SimFs;

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
