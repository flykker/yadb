use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yadb_blob::{BlobError, BlobId, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, LocalMetaKv, MetaError};

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("blob engine error: {0}")]
    Blob(#[from] BlobError),

    #[error("metakv error: {0}")]
    Meta(#[from] MetaError),

    #[error("manifest serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("storage pool `{0}` is not mounted on this node")]
    PoolNotMounted(String),

    #[error("object not found: s3://{bucket}/{key}")]
    ObjectNotFound { bucket: String, key: String },

    #[error("invalid byte range [{start}..={end}] for object size {size}")]
    InvalidRange { start: u64, end: u64, size: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub blob_id: BlobId,
    pub offset: u64,
    pub len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectManifest {
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub content_type: String,
    pub pool: String,
    pub chunks: Vec<ChunkRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectSummary {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub version: u64,
}

/// Multi-tenant S3 / ObjectStore engine built directly on top of YADB's `MetaKV` + `PoolStriper`.
///
/// - Data chunks are striped across failure domains via `PoolStriper` (e.g. `ReedSolomon 4+2`)
///   without passing payload bytes through Raft.
/// - Only the compact `ObjectManifest` (~hundreds of bytes) is committed in `MetaKV`, providing
///   strong read-after-write consistency and lexicographical `ListObjectsV2` prefix scans.
pub struct ObjectStoreEngine<FS: StorageFs> {
    meta: LocalMetaKv<FS>,
    pools: Arc<RwLock<BTreeMap<String, Arc<PoolStriper<FS>>>>>,
    chunk_size: usize,
    step_seq: Arc<AtomicU32>,
}

impl<FS: StorageFs> Clone for ObjectStoreEngine<FS> {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            pools: self.pools.clone(),
            chunk_size: self.chunk_size,
            step_seq: self.step_seq.clone(),
        }
    }
}

impl<FS: StorageFs> ObjectStoreEngine<FS> {
    pub fn new(meta: LocalMetaKv<FS>, chunk_size: usize) -> Self {
        Self {
            meta,
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            chunk_size: chunk_size.max(4096),
            step_seq: Arc::new(AtomicU32::new(1)),
        }
    }

    pub fn register_pool(&self, name: impl Into<String>, striper: Arc<PoolStriper<FS>>) {
        self.pools.write().unwrap().insert(name.into(), striper);
    }

    pub fn has_pool(&self, name: &str) -> bool {
        self.pools.read().unwrap().contains_key(name)
    }

    fn get_pool(&self, name: &str) -> Result<Arc<PoolStriper<FS>>, S3Error> {
        self.pools
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::PoolNotMounted(name.to_string()))
    }

    fn meta_key(project_id: u32, store: &str, bucket: &str, key: &str) -> String {
        format!("/s3/{project_id}/{store}/{bucket}/{key}")
    }

    fn meta_prefix(project_id: u32, store: &str, bucket: &str, prefix: &str) -> String {
        format!("/s3/{project_id}/{store}/{bucket}/{prefix}")
    }

    fn owner_hash(store: &str, bucket: &str, key: &str) -> u32 {
        crc32fast::hash(format!("{store}/{bucket}/{key}").as_bytes()).max(1)
    }

    /// Writes object chunks to erasure-coded `PoolStriper` first (idempotent, consensus-free),
    /// then atomically commits the `ObjectManifest` in `MetaKV`.
    pub fn put_object(
        &self,
        project_id: u32,
        store: &str,
        pool_name: &str,
        bucket: &str,
        key: &str,
        content_type: &str,
        payload: &[u8],
    ) -> Result<ObjectManifest, S3Error> {
        let pool = self.get_pool(pool_name)?;
        let owner_id = Self::owner_hash(store, bucket, key);

        // Bind blob generation to durable MetaKV revision + 1 so restarts/failovers never collide!
        let mkey = Self::meta_key(project_id, store, bucket, key);
        let next_gen = ((self.meta.revision() as u32).saturating_add(1)).min(BlobId::MAX_U24);

        let mut chunks = Vec::new();
        let mut offset = 0u64;
        for slice in payload.chunks(self.chunk_size) {
            let step = (self.step_seq.fetch_add(1, Ordering::Relaxed) % BlobId::MAX_U24).max(1);
            let blob_id = BlobId::new(project_id, owner_id, next_gen, step, 2, 0);
            pool.put_blob(blob_id, slice)?;
            chunks.push(ChunkRef {
                blob_id,
                offset,
                len: slice.len() as u32,
            });
            offset += slice.len() as u64;
        }

        let crc = crc32fast::hash(payload);
        let etag = format!("\"{crc:08x}-{}\"", payload.len());
        let manifest = ObjectManifest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: payload.len() as u64,
            etag,
            content_type: content_type.to_string(),
            pool: pool_name.to_string(),
            chunks,
        };

        let encoded = serde_json::to_vec(&manifest)?;
        self.meta.execute(KvCommand::Put {
            key: mkey,
            value: encoded,
        })?;

        Ok(manifest)
    }

    pub fn head_object(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectManifest, S3Error> {
        let mkey = Self::meta_key(project_id, store, bucket, key);
        let kv = self.meta.get(&mkey).ok_or_else(|| S3Error::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })?;
        Ok(serde_json::from_slice(&kv.value)?)
    }

    pub fn get_object(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(ObjectManifest, Vec<u8>), S3Error> {
        let manifest = self.head_object(project_id, store, bucket, key)?;
        if manifest.size == 0 {
            return Ok((manifest, Vec::new()));
        }
        let end = manifest.size - 1;
        let bytes = self.read_manifest_range(&manifest, 0, end)?;
        Ok((manifest, bytes))
    }

    /// Reads inclusive byte range `start..=end`, fetching ONLY overlapping chunks from `PoolStriper`.
    pub fn get_object_range(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
        start: u64,
        end: u64,
    ) -> Result<(ObjectManifest, Vec<u8>), S3Error> {
        let manifest = self.head_object(project_id, store, bucket, key)?;
        if start > end || end >= manifest.size {
            return Err(S3Error::InvalidRange {
                start,
                end,
                size: manifest.size,
            });
        }
        let bytes = self.read_manifest_range(&manifest, start, end)?;
        Ok((manifest, bytes))
    }

    fn read_manifest_range(
        &self,
        manifest: &ObjectManifest,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, S3Error> {
        let pool = self.get_pool(&manifest.pool)?;
        let mut out = Vec::with_capacity((end - start + 1) as usize);

        for ch in &manifest.chunks {
            let ch_start = ch.offset;
            let ch_end = ch.offset + ch.len as u64 - 1;
            if ch_end < start || ch_start > end {
                continue; // Skip non-overlapping chunks completely!
            }
            let chunk_bytes = pool.get_blob(ch.blob_id)?;
            let slice_from = start.saturating_sub(ch_start) as usize;
            let slice_to = ((end.min(ch_end) - ch_start) + 1) as usize;
            out.extend_from_slice(&chunk_bytes[slice_from..slice_to]);
        }

        Ok(out)
    }

    pub fn delete_object(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
    ) -> Result<bool, S3Error> {
        let mkey = Self::meta_key(project_id, store, bucket, key);
        let res = self.meta.execute(KvCommand::Delete { key: mkey })?;
        Ok(matches!(
            res,
            yadb_metakv::KvResult::Deleted { existed: true, .. }
        ))
    }

    pub fn list_objects(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<ObjectSummary>, S3Error> {
        let base = format!("/s3/{project_id}/{store}/{bucket}/");
        let full_prefix = Self::meta_prefix(project_id, store, bucket, prefix);
        let rows = self.meta.scan_prefix(&full_prefix);
        let mut out = Vec::with_capacity(rows.len());
        for (full_key, kv) in rows {
            let obj_key = full_key.strip_prefix(&base).unwrap_or(&full_key).to_string();
            let manifest: ObjectManifest = serde_json::from_slice(&kv.value)?;
            out.push(ObjectSummary {
                key: obj_key,
                size: manifest.size,
                etag: manifest.etag,
                version: kv.version,
            });
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
    fn multi_chunk_s3_object_supports_range_read_and_listing_over_rs_4_2() {
        let fs = SimFs::new(5050);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let s3 = ObjectStoreEngine::new(meta, 4096); // 4 KiB chunks to test multi-chunk slicing

        let mut domains = Vec::new();
        for i in 0..6 {
            domains.push(
                VolumeStore::open(fs.clone(), format!("d{i}"), VolumeConfig::default()).unwrap(),
            );
        }
        let pool = Arc::new(
            PoolStriper::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }, domains).unwrap(),
        );
        s3.register_pool("data-ec", pool);

        let payload: Vec<u8> = (0..15_000u32).map(|i| (i % 251) as u8).collect();
        let mf = s3
            .put_object(
                1,
                "receipts",
                "data-ec",
                "invoices-2026",
                "nov/inv-002.bin",
                "application/octet-stream",
                &payload,
            )
            .unwrap();
        assert_eq!(mf.chunks.len(), 4);

        s3.put_object(
            1,
            "receipts",
            "data-ec",
            "invoices-2026",
            "nov/inv-001.bin",
            "application/octet-stream",
            b"small-header",
        )
        .unwrap();

        // Cross-chunk byte range read [4090..=8200]
        let (_, slice) = s3
            .get_object_range(
                1,
                "receipts",
                "invoices-2026",
                "nov/inv-002.bin",
                4090,
                8200,
            )
            .unwrap();
        assert_eq!(slice.as_slice(), &payload[4090..=8200]);

        // Sorted listing
        let listed = s3
            .list_objects(1, "receipts", "invoices-2026", "nov/")
            .unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].key, "nov/inv-001.bin");
        assert_eq!(listed[1].key, "nov/inv-002.bin");
    }
}
