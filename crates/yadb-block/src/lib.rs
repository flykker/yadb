use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yadb_blob::{BlobError, BlobId, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, LocalMetaKv, MetaError};

pub const DEFAULT_BLOCK_SIZE: u32 = 4096;

#[derive(Debug, Error)]
pub enum BlockError {
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
        let mut map = self.load_map(project_id, volume)?;
        map.generation = ((self.meta.revision() as u32)
            .saturating_add(1)
            .max(map.generation.saturating_add(1)))
        .min(BlobId::MAX_U24);
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
        self.meta.execute(KvCommand::Put {
            key: Self::meta_key(project_id, volume),
            value: encoded,
        })?;

        Ok(data.len())
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
        let pool = Arc::new(
            PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, doms).unwrap(),
        );
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
}
