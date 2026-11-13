use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yadb_blob::{BlobError, BlobId, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, KvResult, LocalMetaKv, MetaError, RevisionGuard, TxnOp};

// Keep new S3 ids separate from legacy S3/SQL (channel 2) and block (3).
const S3_CHANNEL: u8 = 4;
const INTENT_GRACE_SECS: u64 = 300;
const INTENT_GRACE_REVISIONS: u64 = 32;
const MAX_ALLOCATION: u64 = (1u64 << 56) - 1;

fn unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("object generation allocation exhausted or contended")]
    GenerationExhausted,
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

    #[error("object was modified concurrently")]
    ConcurrentModification,

    #[error("invalid byte range [{start}..={end}] for object size {size}")]
    InvalidRange { start: u64, end: u64, size: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    #[serde(default)]
    pub last_modified: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectSummary {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub version: u64,
    #[serde(default)]
    pub last_modified: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UploadIntent {
    upload_id: String,
    manifest_key: String,
    generation: u32,
    pool: String,
    chunks: Vec<ChunkRef>,
    state: UploadState,
    created_revision: u64,
    /// Revision grace supplements (but does not replace) wall-clock grace.
    lease_until_revision: u64,
    /// Both the wall-clock and revision grace must elapse before aborting.
    #[serde(default)]
    lease_until_unix_secs: u64,
    /// Only v2 allocations are provably disjoint from historical fragments.
    /// Intents written by earlier engines are retained, never guessed safe.
    #[serde(default)]
    allocation_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum UploadState {
    Prepared,
    Aborted,
}

/// Multi-tenant S3 / ObjectStore engine built directly on top of YADB's `MetaKV` + `PoolStriper`.
///
/// - Data chunks are striped across failure domains via `PoolStriper` (e.g. `ReedSolomon 4+2`)
///   without passing payload bytes through Raft.
/// - Metadata (allocation, upload intent and manifest) is committed through Raft;
///   payload bytes bypass Raft. Reads use the local MetaKV view; protocol handlers
///   must provide any required quorum fence before serving that view.
pub struct ObjectStoreEngine<FS: StorageFs> {
    meta: LocalMetaKv<FS>,
    pools: Arc<RwLock<BTreeMap<String, Arc<PoolStriper<FS>>>>>,
    chunk_size: usize,
}

impl<FS: StorageFs> Clone for ObjectStoreEngine<FS> {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            pools: self.pools.clone(),
            chunk_size: self.chunk_size,
        }
    }
}

impl<FS: StorageFs> ObjectStoreEngine<FS> {
    pub fn new(meta: LocalMetaKv<FS>, chunk_size: usize) -> Self {
        Self {
            meta,
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            chunk_size: chunk_size.max(4096),
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

    fn intent_prefix(project_id: u32) -> String {
        format!("/s3-intent/{project_id}/")
    }

    fn intent_key(project_id: u32, upload_id: &str) -> String {
        format!("/s3-intent/{project_id}/{upload_id}")
    }

    fn retired_key(project_id: u32, store: &str, bucket: &str, key: &str, revision: u64) -> String {
        format!("/s3-retired/{project_id}/{store}/{bucket}/{key}/{revision}")
    }

    /// One non-deleting allocator per project, shared by every writer/key/pool.
    /// Failed/ambiguous allocations are skipped rather than reused.
    fn reserve_upload(&self, project_id: u32) -> Result<u64, S3Error> {
        let key = format!("/s3-upload-sequence/{project_id}");
        for _ in 0..128 {
            let current = self.meta.get(&key);
            let previous = match &current {
                Some(v) => serde_json::from_slice::<u64>(&v.value)?,
                None => 0,
            };
            let next = previous.checked_add(1).filter(|v| *v <= MAX_ALLOCATION)
                .ok_or(S3Error::GenerationExhausted)?;
            if matches!(self.meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: key.clone(),
                    expected_mod_revision: current.as_ref().map_or(0, |v| v.mod_revision),
                }],
                ops: vec![TxnOp::Put { key: key.clone(), value: serde_json::to_vec(&next)? }],
            })?, KvResult::TxnOk { .. }) {
                return Ok(next);
            }
        }
        Err(S3Error::GenerationExhausted)
    }

    /// Persists allocation + complete intent before any fragment write, then
    /// writes erasure-coded chunks and atomically publishes manifest + retirement.
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
        let mkey = Self::meta_key(project_id, store, bucket, key);
        let allocation_id = self.reserve_upload(project_id)?;
        let owner_id = (allocation_id >> 24) as u32;
        let next_gen = (allocation_id & BlobId::MAX_U24 as u64) as u32;
        let upload_id = format!("v2-{allocation_id:014x}");
        let intent_key = Self::intent_key(project_id, &upload_id);
        let intent_lease = self.meta.revision().saturating_add(INTENT_GRACE_REVISIONS);
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        for (index, slice) in payload.chunks(self.chunk_size).enumerate() {
            let step = u32::try_from(index + 1).ok().filter(|v| *v <= BlobId::MAX_U24)
                .ok_or(S3Error::GenerationExhausted)?;
            chunks.push(ChunkRef {
                blob_id: BlobId::new(project_id, owner_id, next_gen, step, S3_CHANNEL, 0),
                offset,
                len: slice.len() as u32,
            });
            offset += slice.len() as u64;
        }
        let intent = UploadIntent {
            upload_id: upload_id.clone(),
            manifest_key: mkey.clone(),
            generation: next_gen,
            pool: pool_name.to_string(),
            chunks: chunks.clone(),
            state: UploadState::Prepared,
            created_revision: self.meta.revision(),
            lease_until_revision: intent_lease,
            lease_until_unix_secs: unix_seconds().saturating_add(INTENT_GRACE_SECS),
            allocation_id: Some(allocation_id),
        };
        let created = self.meta.execute(KvCommand::CompareAndSwap {
            key: intent_key.clone(),
            expected_version: 0,
            new_value: Some(serde_json::to_vec(&intent)?),
        })?;
        if !matches!(created, KvResult::CasOk { .. }) {
            return Err(S3Error::ConcurrentModification);
        }

        for (slice, chunk) in payload.chunks(self.chunk_size).zip(chunks.iter()) {
            let blob_id = chunk.blob_id;
            pool.put_blob(blob_id, slice)?;
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
            last_modified: unix_seconds(),
        };

        self.publish_upload(project_id, store, &manifest, &intent_key)?;
        Ok(manifest)
    }

    fn publish_upload(
        &self, project_id: u32, store: &str, manifest: &ObjectManifest, intent_key: &str,
    ) -> Result<(), S3Error> {
        let mkey = Self::meta_key(project_id, store, &manifest.bucket, &manifest.key);
        // The previous manifest and its guard MUST come from the same read.
        let previous = self.meta.get(&mkey);
        let encoded = serde_json::to_vec(&manifest)?;
        // Publish the manifest and remove the intent in one Raft transaction.
        // Publication and abort guard the same intent revision: only one may
        // win, even if both proposals are in flight after the grace expires.
        let mut ops = vec![
            yadb_metakv::TxnOp::Put {
                key: mkey.clone(),
                value: encoded,
            },
            yadb_metakv::TxnOp::Delete { key: intent_key.to_string() },
        ];
        if let Some(value) = &previous {
            let old: ObjectManifest = serde_json::from_slice(&value.value)?;
            ops.push(yadb_metakv::TxnOp::Put {
                key: Self::retired_key(project_id, store, &manifest.bucket, &manifest.key, value.mod_revision),
                value: serde_json::to_vec(&old)?,
            });
        }
        let current_intent = self
            .meta
            .get(&intent_key)
            .ok_or(S3Error::ConcurrentModification)?;
        let current_intent_state: UploadIntent = serde_json::from_slice(&current_intent.value)?;
        if current_intent_state.state != UploadState::Prepared
            || current_intent_state.manifest_key != mkey
            || current_intent_state.pool != manifest.pool
            || current_intent_state.chunks != manifest.chunks
        {
            return Err(S3Error::ConcurrentModification);
        }
        let result = self.meta.execute(KvCommand::RevisionTxn {
            guards: vec![
                RevisionGuard {
                    key: intent_key.to_string(),
                    expected_mod_revision: current_intent.mod_revision,
                },
                RevisionGuard {
                    key: mkey.clone(),
                    expected_mod_revision: previous.as_ref().map_or(0, |v| v.mod_revision),
                },
            ],
            ops,
        })?;
        if !matches!(result, KvResult::TxnOk { .. }) {
            return Err(S3Error::ConcurrentModification);
        }

        Ok(())
    }

    pub fn head_object(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectManifest, S3Error> {
        let mkey = Self::meta_key(project_id, store, bucket, key);
        let kv = self
            .meta
            .get(&mkey)
            .ok_or_else(|| S3Error::ObjectNotFound {
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
        let bytes = self.read_manifest_range(&manifest, start, end)?;
        Ok((manifest, bytes))
    }

    /// Read the selected immutable manifest, not a newer version of its key.
    /// Callers must obtain the manifest from an authorized object lookup.
    pub fn read_manifest_range(
        &self,
        manifest: &ObjectManifest,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, S3Error> {
        if start > end || end >= manifest.size {
            return Err(S3Error::InvalidRange {
                start,
                end,
                size: manifest.size,
            });
        }
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
        let previous = self.meta.get(&mkey);
        let mut ops = vec![yadb_metakv::TxnOp::Delete { key: mkey.clone() }];
        if let Some(value) = &previous {
            let manifest: ObjectManifest = serde_json::from_slice(&value.value)?;
            let retired = Self::retired_key(
                project_id,
                store,
                bucket,
                key,
                value.mod_revision,
            );
            ops.push(yadb_metakv::TxnOp::Put {
                key: retired,
                value: serde_json::to_vec(&manifest)?,
            });
        }
        match self.meta.execute(KvCommand::RevisionTxn {
            guards: vec![RevisionGuard {
                key: mkey,
                expected_mod_revision: previous.as_ref().map_or(0, |v| v.mod_revision),
            }],
            ops,
        })? {
            KvResult::TxnOk { .. } => Ok(previous.is_some()),
            _ => Err(S3Error::ConcurrentModification),
        }
    }

    /// Reclaims only uniquely allocated, aborted uploads. Retired manifests and
    /// historical/unknown fragments are retained: readers on another node may
    /// still hold an immutable manifest, and there is no reader-fencing protocol.
    pub fn collect_orphans(&self, project_id: u32) -> Result<usize, S3Error> {
        self.collect_orphans_at(project_id, unix_seconds())
    }

    fn collect_orphans_at(&self, project_id: u32, now: u64) -> Result<usize, S3Error> {
        let current_revision = self.meta.revision();
        let mut removed = 0usize;
        let intents = self.meta.scan_prefix(&Self::intent_prefix(project_id));
        for (key, value) in intents {
            let Ok(intent) = serde_json::from_slice::<UploadIntent>(&value.value) else { continue };
            let Some(allocation) = intent.allocation_id.filter(|id| *id > 0 && *id <= MAX_ALLOCATION) else {
                continue;
            };
            if key != Self::intent_key(project_id, &format!("v2-{allocation:014x}"))
                || intent.chunks.len() > BlobId::MAX_U24 as usize
                || intent.chunks.iter().enumerate().any(|(i, chunk)| {
                    chunk.blob_id != BlobId::new(project_id, (allocation >> 24) as u32,
                        (allocation & BlobId::MAX_U24 as u64) as u32, (i + 1) as u32, S3_CHANNEL, 0)
                })
            {
                // Legacy/invalid intent: do not turn a hash collision into data loss.
                continue;
            }
            if intent.state == UploadState::Prepared
                && (intent.lease_until_revision > current_revision || intent.lease_until_unix_secs > now)
            {
                continue;
            }
            // Even an already-Aborted value is re-confirmed through consensus.
            // Local reads may be stale; neither a follower nor an isolated old
            // leader is allowed to delete based only on its local view.
            let aborted = UploadIntent { state: UploadState::Aborted, ..intent.clone() };
            let abort = self.meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard { key: key.clone(), expected_mod_revision: value.mod_revision }],
                ops: vec![TxnOp::Put { key: key.clone(), value: serde_json::to_vec(&aborted)? }],
            })?;
            let KvResult::TxnOk { revision: aborted_revision, .. } = abort else { continue };
            let mut retain = false;
            for chunk in &aborted.chunks {
                if self.is_reachable(project_id, &intent.pool, chunk.blob_id)? {
                    retain = true;
                    continue;
                }
                removed += self.get_pool(&intent.pool)?.delete(chunk.blob_id)? as usize;
            }
            if retain {
                continue;
            }
            self.meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard { key: key.clone(), expected_mod_revision: aborted_revision }],
                ops: vec![TxnOp::Delete { key }],
            })?;
        }
        Ok(removed)
    }

    fn is_reachable(&self, project_id: u32, pool: &str, blob_id: BlobId) -> Result<bool, S3Error> {
        for prefix in [format!("/s3/{project_id}/"), format!("/s3-retired/{project_id}/")] {
            for (_, value) in self.meta.scan_prefix(&prefix) {
                let manifest: ObjectManifest = serde_json::from_slice(&value.value)?;
                if manifest.pool == pool && manifest.chunks.iter().any(|c| c.blob_id == blob_id) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Runs the collector for every project currently represented in MetaKV.
    /// Includes projects with only failed uploads and no published objects.
    /// Malformed unrelated keys are ignored.
    pub fn collect_orphans_all(&self) -> Result<usize, S3Error> {
        let mut projects = std::collections::BTreeSet::new();
        for prefix in ["/s3/", "/s3-intent/", "/s3-retired/"] {
            for (key, _) in self.meta.scan_prefix(prefix) {
                if let Some(project) = key.strip_prefix(prefix).and_then(|v| v.split('/').next()) {
                    if let Ok(project_id) = project.parse::<u32>() {
                        projects.insert(project_id);
                    }
                }
            }
        }
        let mut removed = 0;
        for project_id in projects {
            removed += self.collect_orphans(project_id)?;
        }
        Ok(removed)
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
            let obj_key = full_key
                .strip_prefix(&base)
                .unwrap_or(&full_key)
                .to_string();
            let manifest: ObjectManifest = serde_json::from_slice(&kv.value)?;
            out.push(ObjectSummary {
                key: obj_key,
                size: manifest.size,
                etag: manifest.etag,
                version: kv.version,
                last_modified: manifest.last_modified,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yadb_blob::{ErasureScheme, VolumeConfig, VolumeStore};
    use yadb_io::{FaultConfig, SimFs};

    fn engine(meta_fs: &SimFs, disks: &[SimFs]) -> ObjectStoreEngine<SimFs> {
        let meta = LocalMetaKv::open_single_node(meta_fs, "meta", 1).unwrap();
        let s3 = ObjectStoreEngine::new(meta, 4096);
        let domains = disks.iter().enumerate().map(|(i, fs)| {
            VolumeStore::open(fs.clone(), format!("d{i}"), VolumeConfig::default()).unwrap()
        }).collect();
        s3.register_pool("pool", Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap()));
        s3
    }

    // Stop an upload exactly after intent persistence, before metadata publication.
    fn prepared(s3: &ObjectStoreEngine<SimFs>) -> (String, UploadIntent, ObjectManifest) {
        let allocation = s3.reserve_upload(1).unwrap();
        let upload_id = format!("v2-{allocation:014x}");
        let key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let chunks = vec![ChunkRef {
            blob_id: BlobId::new(1, (allocation >> 24) as u32,
                (allocation & BlobId::MAX_U24 as u64) as u32, 1, S3_CHANNEL, 0),
            offset: 0, len: 7,
        }];
        let intent = UploadIntent {
            upload_id, manifest_key: "/s3/1/store/bucket/key".into(),
            generation: chunks[0].blob_id.generation(), pool: "pool".into(), chunks: chunks.clone(),
            state: UploadState::Prepared, created_revision: s3.meta.revision(),
            lease_until_revision: s3.meta.revision() + INTENT_GRACE_REVISIONS,
            lease_until_unix_secs: 1000, allocation_id: Some(allocation),
        };
        s3.meta.execute(KvCommand::CompareAndSwap {
            key: key.clone(), expected_version: 0, new_value: Some(serde_json::to_vec(&intent).unwrap()),
        }).unwrap();
        let manifest = ObjectManifest {
            bucket: "bucket".into(), key: "key".into(), size: 7, etag: "test".into(),
            content_type: "text/plain".into(), pool: "pool".into(), chunks, last_modified: 0,
        };
        (key, intent, manifest)
    }

    fn advance_revisions(s3: &ObjectStoreEngine<SimFs>) {
        for _ in 0..INTENT_GRACE_REVISIONS {
            s3.meta.execute(KvCommand::Put { key: "/test-tick".into(), value: Vec::new() }).unwrap();
        }
    }

    #[test]
    fn collector_requires_both_graces_and_fences_delayed_publication_and_put() {
        let meta_fs = SimFs::new(6200);
        let disks = [SimFs::new(6201), SimFs::new(6202), SimFs::new(6203)];
        let s3 = engine(&meta_fs, &disks);
        let (key, _, manifest) = prepared(&s3);
        let id = manifest.chunks[0].blob_id;
        let pool = s3.get_pool("pool").unwrap();
        pool.put_blob(id, b"payload").unwrap();
        assert_eq!(s3.collect_orphans_at(1, 1001).unwrap(), 0); // time alone
        advance_revisions(&s3);
        assert_eq!(s3.collect_orphans_at(1, 999).unwrap(), 0); // revisions alone
        assert_eq!(s3.collect_orphans_at(1, 1001).unwrap(), 1);
        assert!(s3.meta.get(&key).is_none());
        assert!(matches!(s3.publish_upload(1, "store", &manifest, &key), Err(S3Error::ConcurrentModification)));
        assert!(pool.put_blob(id, b"payload").is_err());
        assert!(s3.head_object(1, "store", "bucket", "key").is_err());
    }

    #[test]
    fn collector_retries_aborted_intent_after_failed_target_and_restart() {
        let meta_fs = SimFs::new(6210);
        let disks = [SimFs::new(6211), SimFs::new(6212), SimFs::new(6213)];
        let s3 = engine(&meta_fs, &disks);
        let (key, _, manifest) = prepared(&s3);
        let id = manifest.chunks[0].blob_id;
        s3.get_pool("pool").unwrap().put_blob(id, b"payload").unwrap();
        advance_revisions(&s3);
        disks[1].set_faults(FaultConfig { write_eio_prob: 1.0, ..Default::default() });
        assert!(s3.collect_orphans_at(1, 1001).is_err());
        let aborted: UploadIntent = serde_json::from_slice(&s3.meta.get(&key).unwrap().value).unwrap();
        assert_eq!(aborted.state, UploadState::Aborted);
        drop(s3);
        disks[1].set_faults(FaultConfig::default());
        meta_fs.simulate_power_cut();
        for disk in &disks { disk.simulate_power_cut(); }
        let recovered = engine(&meta_fs, &disks);
        // No live object in this project: discovery must use the intent namespace.
        assert_eq!(recovered.collect_orphans_all().unwrap(), 1);
        assert!(recovered.meta.get(&key).is_none());
        assert!(recovered.get_pool("pool").unwrap().put_blob(id, b"payload").is_err());
    }

    #[test]
    fn published_upload_wins_over_late_collector_and_survives_power_cut() {
        let meta_fs = SimFs::new(6220);
        let disks = [SimFs::new(6221), SimFs::new(6222), SimFs::new(6223)];
        let s3 = engine(&meta_fs, &disks);
        let (key, _, manifest) = prepared(&s3);
        s3.get_pool("pool").unwrap().put_blob(manifest.chunks[0].blob_id, b"payload").unwrap();
        s3.publish_upload(1, "store", &manifest, &key).unwrap();
        advance_revisions(&s3);
        assert_eq!(s3.collect_orphans_at(1, 1001).unwrap(), 0);
        drop(s3);
        meta_fs.simulate_power_cut();
        for disk in &disks { disk.simulate_power_cut(); }
        let recovered = engine(&meta_fs, &disks);
        assert_eq!(recovered.get_object(1, "store", "bucket", "key").unwrap().1, b"payload");
    }

    #[test]
    fn unpublishable_upload_cannot_commit_while_aborted_cleanup_is_incomplete() {
        let meta_fs = SimFs::new(6230);
        let disks = [SimFs::new(6231), SimFs::new(6232), SimFs::new(6233)];
        let s3 = engine(&meta_fs, &disks);
        let (key, _, manifest) = prepared(&s3);
        s3.get_pool("pool").unwrap().put_blob(manifest.chunks[0].blob_id, b"payload").unwrap();
        advance_revisions(&s3);
        disks[0].set_faults(FaultConfig { write_eio_prob: 1.0, ..Default::default() });
        assert!(s3.collect_orphans_at(1, 1001).is_err());
        assert!(matches!(s3.publish_upload(1, "store", &manifest, &key), Err(S3Error::ConcurrentModification)));
    }

    #[test]
    fn concurrent_independent_writers_use_disjoint_ids_and_restart_never_reuses_them() {
        let meta_fs = SimFs::new(6240);
        let disks = [SimFs::new(6241), SimFs::new(6242), SimFs::new(6243)];
        let s3 = engine(&meta_fs, &disks);
        let ids = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..12).map(|i| {
                let writer = ObjectStoreEngine::new(s3.meta.clone(), 4096);
                writer.register_pool("pool", s3.get_pool("pool").unwrap());
                scope.spawn(move || writer.put_object(1, "store", "pool", "bucket", &format!("key-{i}"), "text/plain", b"payload").unwrap().chunks[0].blob_id)
            }).collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect::<std::collections::BTreeSet<_>>()
        });
        assert_eq!(ids.len(), 12);
        drop(s3);
        meta_fs.simulate_power_cut();
        for disk in &disks { disk.simulate_power_cut(); }
        let recovered = engine(&meta_fs, &disks);
        let new = recovered.put_object(1, "store", "pool", "bucket", "key-0", "text/plain", b"new").unwrap();
        assert!(!ids.contains(&new.chunks[0].blob_id));
    }

    #[test]
    fn allocator_crosses_generation_boundary_and_stops_before_id_wraparound() {
        let meta_fs = SimFs::new(6245);
        let disks = [SimFs::new(6246), SimFs::new(6247), SimFs::new(6248)];
        let s3 = engine(&meta_fs, &disks);
        s3.meta.execute(KvCommand::Put {
            key: "/s3-upload-sequence/1".into(), value: serde_json::to_vec(&(BlobId::MAX_U24 as u64)).unwrap(),
        }).unwrap();
        let manifest = s3.put_object(1, "store", "pool", "bucket", "key", "text/plain", b"payload").unwrap();
        let id = manifest.chunks[0].blob_id;
        assert_eq!(id.owner_id(), 1);
        assert_eq!(id.generation(), 0);
        assert_eq!(id.channel(), S3_CHANNEL);
        s3.meta.execute(KvCommand::Put {
            key: "/s3-upload-sequence/1".into(), value: serde_json::to_vec(&MAX_ALLOCATION).unwrap(),
        }).unwrap();
        assert!(matches!(s3.reserve_upload(1), Err(S3Error::GenerationExhausted)));
    }

    #[test]
    fn malformed_manifest_stops_cleanup_without_forgetting_the_aborted_intent() {
        let meta_fs = SimFs::new(6249);
        let disks = [SimFs::new(6256), SimFs::new(6257), SimFs::new(6258)];
        let s3 = engine(&meta_fs, &disks);
        let (key, _, manifest) = prepared(&s3);
        let id = manifest.chunks[0].blob_id;
        s3.get_pool("pool").unwrap().put_blob(id, b"payload").unwrap();
        advance_revisions(&s3);
        s3.meta.execute(KvCommand::Put { key: manifest_key(), value: b"invalid-json".to_vec() }).unwrap();
        assert!(matches!(s3.collect_orphans_at(1, 1001), Err(S3Error::Json(_))));
        assert!(s3.meta.get(&key).is_some());
        assert_eq!(s3.get_pool("pool").unwrap().get_blob(id).unwrap(), b"payload");
    }

    #[test]
    fn intent_collision_prevents_any_fragment_put() {
        let meta_fs = SimFs::new(6250);
        let disks = [SimFs::new(6251), SimFs::new(6252), SimFs::new(6253)];
        let s3 = engine(&meta_fs, &disks);
        s3.meta.execute(KvCommand::Put {
            key: ObjectStoreEngine::<SimFs>::intent_key(1, "v2-00000000000001"), value: b"occupied".to_vec(),
        }).unwrap();
        assert!(matches!(s3.put_object(1, "store", "pool", "bucket", "key", "text/plain", b"payload"), Err(S3Error::ConcurrentModification)));
        for slot in s3.get_pool("pool").unwrap().slots() {
            assert_eq!(slot.local_store().unwrap().stats().unwrap().live_blobs, 0);
        }
    }

    #[test]
    fn rejected_or_torn_publication_retains_durable_intent_and_reclaims_after_restart() {
        for torn in [false, true] {
            let meta_fs = SimFs::new(6270);
            let disks = [SimFs::new(6271), SimFs::new(6272), SimFs::new(6273)];
            let s3 = engine(&meta_fs, &disks);
            let (key, _, manifest) = prepared(&s3);
            let id = manifest.chunks[0].blob_id;
            s3.get_pool("pool").unwrap().put_blob(id, b"payload").unwrap();
            meta_fs.set_faults(FaultConfig {
                torn_write_prob: if torn { 1.0 } else { 0.0 },
                write_eio_prob: if torn { 0.0 } else { 1.0 },
                ..Default::default()
            });
            assert!(matches!(s3.publish_upload(1, "store", &manifest, &key), Err(S3Error::Meta(_))));
            assert!(s3.meta.get(&manifest_key()).is_none());
            assert!(s3.meta.get(&key).is_some());
            drop(s3);
            meta_fs.set_faults(FaultConfig::default());
            meta_fs.simulate_power_cut();
            for disk in &disks { disk.simulate_power_cut(); }
            let recovered = engine(&meta_fs, &disks);
            advance_revisions(&recovered);
            assert_eq!(recovered.collect_orphans_at(1, 1001).unwrap(), 1);
            assert!(recovered.meta.get(&key).is_none());
            assert!(recovered.head_object(1, "store", "bucket", "key").is_err());
        }
    }

    fn manifest_key() -> String {
        ObjectStoreEngine::<SimFs>::meta_key(1, "store", "bucket", "key")
    }

    #[test]
    fn collector_and_publication_have_only_two_safe_race_outcomes() {
        let meta_fs = SimFs::new(6280);
        let disks = [SimFs::new(6281), SimFs::new(6282), SimFs::new(6283)];
        let s3 = engine(&meta_fs, &disks);
        for _ in 0..32 {
            let (key, _, manifest) = prepared(&s3);
            let id = manifest.chunks[0].blob_id;
            s3.get_pool("pool").unwrap().put_blob(id, b"payload").unwrap();
            advance_revisions(&s3);
            let start = std::sync::Barrier::new(2);
            let (published, collected) = std::thread::scope(|scope| {
                let publisher = scope.spawn(|| { start.wait(); s3.publish_upload(1, "store", &manifest, &key) });
                let collector = scope.spawn(|| { start.wait(); s3.collect_orphans_at(1, 1001) });
                (publisher.join().unwrap(), collector.join().unwrap().unwrap())
            });
            if published.is_ok() {
                assert_eq!(collected, 0);
                assert_eq!(s3.read_manifest_range(&manifest, 0, 6).unwrap(), b"payload");
            } else {
                assert_eq!(collected, 1);
                assert!(s3.get_pool("pool").unwrap().get_blob(id).is_err());
                if let Ok(current) = s3.head_object(1, "store", "bucket", "key") {
                    assert_ne!(current.chunks[0].blob_id, id);
                }
            }
        }
    }

    #[test]
    fn collector_never_guesses_legacy_or_unknown_fragments_are_safe() {
        let meta_fs = SimFs::new(6260);
        let disks = [SimFs::new(6261), SimFs::new(6262), SimFs::new(6263)];
        let s3 = engine(&meta_fs, &disks);
        let (key, mut intent, _) = prepared(&s3);
        intent.allocation_id = None;
        intent.lease_until_revision = 0;
        intent.lease_until_unix_secs = 0;
        intent.chunks[0].blob_id = BlobId::new(1, 99, 1, 1, 2, 0);
        let pool = s3.get_pool("pool").unwrap();
        pool.put_blob(intent.chunks[0].blob_id, b"payload").unwrap();
        let unknown = BlobId::new(1, 100, 1, 1, 2, 0);
        pool.put_blob(unknown, b"unknown").unwrap();
        s3.meta.execute(KvCommand::Put { key, value: serde_json::to_vec(&intent).unwrap() }).unwrap();
        assert_eq!(s3.collect_orphans_all().unwrap(), 0);
        assert_eq!(pool.get_blob(intent.chunks[0].blob_id).unwrap(), b"payload");
        assert_eq!(pool.get_blob(unknown).unwrap(), b"unknown");
    }

    #[test]
    fn collector_cannot_delete_even_aborted_intents_without_a_consensus_result() {
        let meta_fs = SimFs::new(6290);
        let disks = [SimFs::new(6291), SimFs::new(6292), SimFs::new(6293)];
        let s3 = engine(&meta_fs, &disks);
        let (key, mut intent, manifest) = prepared(&s3);
        let id = manifest.chunks[0].blob_id;
        s3.get_pool("pool").unwrap().put_blob(id, b"payload").unwrap();
        intent.state = UploadState::Aborted;
        s3.meta.execute(KvCommand::Put { key, value: serde_json::to_vec(&intent).unwrap() }).unwrap();
        let term = s3.meta.role_info().2;
        s3.meta.step_rpc(yadb_metakv::RaftRpc::RequestVote {
            term: term + 1, candidate_id: 2, last_log_index: u64::MAX, last_log_term: term + 1,
        }).unwrap();
        assert!(matches!(s3.collect_orphans_at(1, 1001), Err(S3Error::Meta(MetaError::NotLeader { .. }))));
        assert_eq!(s3.get_pool("pool").unwrap().get_blob(id).unwrap(), b"payload");
    }

    #[test]
    fn failed_fragment_quorum_is_reclaimed_but_committed_object_stays_durable() {
        for torn in [false, true] {
            let meta_fs = SimFs::new(6300);
            let disks = [SimFs::new(6301), SimFs::new(6302), SimFs::new(6303)];
            let s3 = engine(&meta_fs, &disks);
            s3.put_object(1, "store", "pool", "bucket", "live", "text/plain", b"live").unwrap();
            for disk in &disks[1..] {
                disk.set_faults(FaultConfig {
                    torn_write_prob: if torn { 1.0 } else { 0.0 },
                    write_eio_prob: if torn { 0.0 } else { 1.0 }, ..Default::default()
                });
            }
            assert!(matches!(s3.put_object(1, "store", "pool", "bucket", "failed", "text/plain", b"orphan"), Err(S3Error::Blob(_))));
            let pending = s3.meta.scan_prefix("/s3-intent/1/");
            assert_eq!(pending.len(), 1);
            let intent: UploadIntent = serde_json::from_slice(&pending[0].1.value).unwrap();
            drop(s3);
            meta_fs.simulate_power_cut();
            for disk in &disks { disk.set_faults(FaultConfig::default()); disk.simulate_power_cut(); }
            let recovered = engine(&meta_fs, &disks);
            advance_revisions(&recovered);
            assert_eq!(recovered.collect_orphans_at(1, intent.lease_until_unix_secs + 1).unwrap(), 1);
            assert_eq!(recovered.get_object(1, "store", "bucket", "live").unwrap().1, b"live");
            assert!(recovered.head_object(1, "store", "bucket", "failed").is_err());
        }
    }

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
        let pool =
            Arc::new(PoolStriper::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }, domains).unwrap());
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

        // A reader holding the original manifest must not switch versions when
        // the object is replaced between metadata lookup and payload read.
        s3.put_object(1, "receipts", "data-ec", "invoices-2026", "nov/inv-002.bin", "text/plain", b"replacement").unwrap();
        assert_eq!(s3.read_manifest_range(&mf, 4090, 8200).unwrap(), payload[4090..=8200]);
        assert!(matches!(s3.read_manifest_range(&mf, 0, mf.size), Err(S3Error::InvalidRange { .. })));

        // Sorted listing
        let listed = s3
            .list_objects(1, "receipts", "invoices-2026", "nov/")
            .unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].key, "nov/inv-001.bin");
        assert_eq!(listed[1].key, "nov/inv-002.bin");
    }

    #[test]
    fn failed_fragment_quorum_leaves_no_manifest_but_retains_orphan_after_restart() {
        let meta_fs = SimFs::new(6001);
        let data_fs = SimFs::new(6002);
        let meta = LocalMetaKv::open_single_node(&meta_fs, "meta", 1).unwrap();
        let s3 = ObjectStoreEngine::new(meta, 4096);
        let mut domains = Vec::new();
        for i in 0..3 {
            domains.push(
                VolumeStore::open(
                    data_fs.clone(),
                    format!("mirror-{i}"),
                    VolumeConfig::default(),
                )
                .unwrap(),
            );
        }
        let pool = Arc::new(PoolStriper::new(
            ErasureScheme::Mirror { replicas: 3 },
            domains,
        )
        .unwrap());
        s3.register_pool("wal-fast", pool.clone());

        // The metadata WAL remains healthy; only fragment writes are faulted.
        // This produces a failed 1/3 mirror write after one durable fragment,
        // but no ObjectManifest is published.
        data_fs.set_faults(FaultConfig {
            // SplitMix64 values for seed 13 make the first mirror write pass,
            // then tear the next two writes deterministically.
            seed: 13,
            torn_write_prob: 0.5,
            read_eio_prob: 0.0,
            write_eio_prob: 0.0,
        });
        let result = s3.put_object(
            1,
            "receipts",
            "wal-fast",
            "invoices-2026",
            "failed-fragment-write",
            "application/octet-stream",
            b"payload whose manifest must not become visible",
        );
        assert!(matches!(result, Err(S3Error::Blob(_))), "{result:?}");
        assert!(matches!(
            s3.head_object(1, "receipts", "invoices-2026", "failed-fragment-write"),
            Err(S3Error::ObjectNotFound { .. })
        ));

        let live_fragments: usize = pool
            .slots()
            .iter()
            .map(|slot| slot.local_store().unwrap().stats().unwrap().live_blobs)
            .sum();
        assert_eq!(live_fragments, 1);
        // A failed fragment PUT is not an atomic rollback. The acknowledged
        // fragment remains on disk, but without a manifest it is not an S3
        // object. The durable intent allows later fenced cleanup.
        drop(s3);
        drop(pool);
        data_fs.set_faults(FaultConfig::default());
        data_fs.simulate_power_cut();
        meta_fs.simulate_power_cut();
        let meta = LocalMetaKv::open_single_node(&meta_fs, "meta", 1).unwrap();
        let recovered = ObjectStoreEngine::new(meta, 4096);
        assert!(matches!(
            recovered.head_object(1, "receipts", "invoices-2026", "failed-fragment-write"),
            Err(S3Error::ObjectNotFound { .. })
        ));
        let remaining: usize = (0..3)
            .map(|i| {
                VolumeStore::open(
                    data_fs.clone(),
                    format!("mirror-{i}"),
                    VolumeConfig::default(),
                )
                .unwrap()
                .stats()
                .unwrap()
                .live_blobs
            })
            .sum();
        assert_eq!(remaining, 1);
    }

    #[test]
    fn expired_intent_collector_removes_orphan_and_keeps_committed_overwrite() {
        let meta_fs = SimFs::new(6101);
        let data_fs = SimFs::new(6102);
        let meta = LocalMetaKv::open_single_node(&meta_fs, "meta", 1).unwrap();
        let s3 = ObjectStoreEngine::new(meta, 4096);
        let mut domains = Vec::new();
        for i in 0..3 {
            domains.push(VolumeStore::open(data_fs.clone(), format!("d{i}"), VolumeConfig::default()).unwrap());
        }
        let pool = Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
        s3.register_pool("pool", pool.clone());

        let live = s3.put_object(1, "store", "pool", "bucket", "key", "text/plain", b"live-value").unwrap();
        let orphan_id = BlobId::new(1, 0, 77, 1, S3_CHANNEL, 0);
        pool.put_blob(orphan_id, b"orphan-value").unwrap();
        let upload_id = format!("v2-{:014x}", 77);
        let intent_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let intent = UploadIntent {
            upload_id,
            manifest_key: "/s3/1/store/bucket/orphan".into(),
            generation: 77,
            pool: "pool".into(),
            chunks: vec![ChunkRef { blob_id: orphan_id, offset: 0, len: 12 }],
            state: UploadState::Prepared,
            created_revision: 0,
            lease_until_revision: 0,
            lease_until_unix_secs: 0,
            allocation_id: Some(77),
        };
        s3.meta.execute(KvCommand::Put { key: intent_key, value: serde_json::to_vec(&intent).unwrap() }).unwrap();
        assert_eq!(s3.collect_orphans(1).unwrap(), 1);
        assert!(matches!(pool.get_blob(orphan_id), Err(BlobError::NotFound(_))));
        assert_eq!(s3.get_object(1, "store", "bucket", "key").unwrap().1, b"live-value");
        assert_eq!(live.size, 10);
    }

    #[test]
    fn overwrite_and_delete_retire_old_fragments_without_deleting_new_manifest() {
        let fs = SimFs::new(6103);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let s3 = ObjectStoreEngine::new(meta, 4096);
        let mut domains = Vec::new();
        for i in 0..3 {
            domains.push(VolumeStore::open(fs.clone(), format!("d{i}"), VolumeConfig::default()).unwrap());
        }
        let pool = Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
        s3.register_pool("pool", pool.clone());
        let old = s3.put_object(1, "store", "pool", "bucket", "key", "text/plain", b"old").unwrap();
        let new = s3.put_object(1, "store", "pool", "bucket", "key", "text/plain", b"new").unwrap();
        assert_eq!(s3.collect_orphans(1).unwrap(), 0);
        assert_eq!(s3.read_manifest_range(&old, 0, 2).unwrap(), b"old");
        assert_eq!(s3.get_object(1, "store", "bucket", "key").unwrap().1, b"new");
        assert!(s3.delete_object(1, "store", "bucket", "key").unwrap());
        assert_eq!(s3.collect_orphans(1).unwrap(), 0);
        assert!(matches!(s3.head_object(1, "store", "bucket", "key"), Err(S3Error::ObjectNotFound { .. })));
        assert_eq!(s3.read_manifest_range(&new, 0, 2).unwrap(), b"new");
    }
}
