use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yadb_blob::{BlobError, BlobId, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, KvResult, KvValue, LocalMetaKv, MetaError, RevisionGuard, TxnOp};

// Keep new S3 ids separate from legacy S3/SQL (channel 2) and block (3).
/// Blob channel of S3 object chunks. The collector needs it to know that this
/// channel's generation space is monotonic, so a generation barrier is safe.
pub const S3_CHANNEL: u8 = 4;
const INTENT_GRACE_SECS: u64 = 300;
const INTENT_GRACE_REVISIONS: u64 = 32;
const MAX_ALLOCATION: u64 = (1u64 << 56) - 1;
/// Largest single multipart part the engine accepts. The daemon applies the
/// same bound to the request body before calling in, so a part is always
/// buffered once, never streamed unbounded.
pub const MULTIPART_PART_MAX: usize = 16 * 1024 * 1024;
/// Largest part number an S3 multipart upload may name (AWS: 1..=10000).
pub const MULTIPART_MAX_PARTS: u32 = 10_000;

/// RFC 1321 MD5, used for S3 part and multipart ETags. Kept here so the S3
/// engine owns the ETag it publishes instead of trusting a caller-supplied
/// digest.
pub fn md5(input: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];
    fn compress(state: &mut [u32; 4], chunk: &[u8], s: &[u32; 64], k: &[u32; 64]) {
        let mut m = [0u32; 16];
        for (i, b) in chunk.chunks_exact(4).enumerate() {
            m[i] = u32::from_le_bytes(b.try_into().unwrap());
        }
        let [a0, b0, c0, d0] = *state;
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = if i < 16 {
                ((b & c) | (!b & d), i)
            } else if i < 32 {
                ((d & b) | (!d & c), (5 * i + 1) % 16)
            } else if i < 48 {
                (b ^ c ^ d, (3 * i + 5) % 16)
            } else {
                (c ^ (b | !d), (7 * i) % 16)
            };
            let next = b.wrapping_add(
                a.wrapping_add(f)
                    .wrapping_add(k[i])
                    .wrapping_add(m[g])
                    .rotate_left(s[i]),
            );
            a = d;
            d = c;
            c = b;
            b = next;
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
    }

    let mut state = [0x67452301u32, 0xefcdab89u32, 0x98badcfeu32, 0x10325476u32];
    let mut blocks = input.chunks_exact(64);
    for chunk in &mut blocks {
        compress(&mut state, chunk, &S, &K);
    }
    // Only the tail is copied, so hashing a 16 MiB part does not duplicate it.
    let bits = (input.len() as u64) * 8;
    let mut tail = Vec::with_capacity(128);
    tail.extend_from_slice(blocks.remainder());
    tail.push(0x80);
    while tail.len() % 64 != 56 {
        tail.push(0);
    }
    tail.extend_from_slice(&bits.to_le_bytes());
    for chunk in tail.chunks_exact(64) {
        compress(&mut state, chunk, &S, &K);
    }
    let mut out = [0u8; 16];
    for (i, word) in state.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_le_bytes());
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Lowercase hex MD5, the form S3 uses for a part ETag.
pub fn md5_hex(input: &[u8]) -> String {
    hex(&md5(input))
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("namespace-wide orphan collection is unavailable without an atomic cross-group view")]
    NamespaceScanUnavailable,
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

    #[error("multipart upload not found: {0}")]
    MultipartUploadNotFound(String),

    #[error("invalid part {part}: {reason}")]
    InvalidPart { part: u32, reason: String },

    #[error("multipart parts are not in strictly ascending order")]
    InvalidPartOrder,

    #[error("part {part} is {size} bytes, over the {max}-byte part limit")]
    PartTooLarge { part: u32, size: u64, max: usize },

    #[error("malformed multipart completion request: {0}")]
    MalformedMultipart(String),
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

/// Extra guards and ops a caller commits in the same `RevisionTxn` as the
/// manifest publication or deletion.
///
/// This is the S3 counterpart of `yadb_block::MapPublication`: the engine owns
/// the manifest/intent guards and ops, the caller appends its own (an
/// idempotency record and its admission counter, in practice), and all of them
/// are checked and applied by one Raft apply under one cluster revision. A
/// failing caller guard therefore aborts the manifest write too — there is no
/// state in which the object advanced without the record, or the record exists
/// without the object.
///
/// Nothing here may be split into a second transaction: a second `RevisionTxn`
/// could commit alone.
#[derive(Debug, Default)]
pub struct ObjectPublication {
    pub guards: Vec<RevisionGuard>,
    pub ops: Vec<TxnOp>,
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
    /// Set on the one record that represents a multipart upload. Its `parts`
    /// name the per-part `v2-` intent records; the upload record itself owns no
    /// fragments, so it adds nothing to the mark set.
    #[serde(default)]
    multipart: bool,
    /// Content type captured at initiate time and applied on completion.
    #[serde(default)]
    content_type: String,
    /// Registered parts, strictly ascending by part number.
    #[serde(default)]
    parts: Vec<MultipartPartRef>,
}

/// One registered part of a multipart upload. The part bytes live in the
/// separate `intent_key` record, which is an ordinary single-allocation intent
/// so the existing collector can validate and reclaim it without changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MultipartPartRef {
    part_number: u32,
    intent_key: String,
    /// Lowercase hex MD5 of the part bytes, without quotes.
    etag: String,
    size: u64,
    #[serde(default)]
    last_modified: u64,
}

impl Default for UploadIntent {
    fn default() -> Self {
        Self {
            upload_id: String::new(),
            manifest_key: String::new(),
            generation: 0,
            pool: String::new(),
            chunks: Vec::new(),
            state: UploadState::Prepared,
            created_revision: 0,
            lease_until_revision: 0,
            lease_until_unix_secs: 0,
            allocation_id: None,
            multipart: false,
            content_type: String::new(),
            parts: Vec::new(),
        }
    }
}

/// A registered multipart part as seen by the wire layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartInfo {
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum UploadState {
    Prepared,
    Aborted,
}

/// Reads the chunk references out of one raw `/s3-intent/` record.
///
/// A prepared upload names fragments that no manifest references yet. A
/// collector that ignores them would delete fragments of an upload that is still
/// in flight, so the mark phase must be able to see them. Only the fields a
/// collector needs are interpreted, and an unreadable record is an error rather
/// than an empty set: "I could not read this" must never mean "nothing here".
pub fn intent_chunk_ids(value: &[u8]) -> Result<Vec<(String, BlobId)>, S3Error> {
    let intent: UploadIntent = serde_json::from_slice(value)?;
    Ok(intent
        .chunks
        .iter()
        .map(|chunk| (intent.pool.clone(), chunk.blob_id))
        .collect())
}

/// Multi-tenant S3 / ObjectStore engine built directly on top of YADB's `MetaKV` + `PoolStriper`.
///
/// - Data chunks are striped across failure domains via `PoolStriper` (e.g. `ReedSolomon 4+2`)
///   without passing payload bytes through Raft.
/// - Metadata (allocation, upload intent and manifest) is committed through Raft;
///   payload bytes bypass Raft. The default read methods use the local MetaKV view;
///   protocol handlers must provide any required quorum fence before serving that
///   view. The explicit `*_consistent` methods take one quorum-confirmed snapshot
///   themselves and never fall back to local state.
pub struct ObjectStoreEngine<FS: StorageFs> {
    meta: LocalMetaKv<FS>,
    pools: Arc<RwLock<BTreeMap<String, Arc<PoolStriper<FS>>>>>,
    chunk_size: usize,
    /// Allocation ids already reserved from the durable counter but not yet
    /// handed out, per project, as `[next, end)`.
    ///
    /// One Raft CAS per upload would make N simultaneous writers contend N
    /// times each on the single sequence key. A writer takes a whole block per
    /// successful CAS and serves the rest of that block locally. The unused
    /// tail is never returned to the counter: an id only has to be unique and
    /// increasing, so skipping is safe, while reuse would alias two uploads
    /// after a restart.
    allocation_cache: Arc<Mutex<BTreeMap<u32, (u64, u64)>>>,
}

impl<FS: StorageFs> Clone for ObjectStoreEngine<FS> {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            pools: self.pools.clone(),
            chunk_size: self.chunk_size,
            allocation_cache: self.allocation_cache.clone(),
        }
    }
}

impl<FS: StorageFs> ObjectStoreEngine<FS> {
    pub fn new(meta: LocalMetaKv<FS>, chunk_size: usize) -> Self {
        Self {
            meta,
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            chunk_size: chunk_size.max(4096),
            allocation_cache: Arc::new(Mutex::new(BTreeMap::new())),
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
    ///
    /// The counter is a single key, so handing out one id per CAS makes N
    /// simultaneous uploads contend N times each: 24 concurrent writers issued
    /// 596 sequence transactions for 48 uploads, and the queue behind that hot
    /// key pushed individual transactions past the metadata bridge's deadline.
    /// A writer therefore takes a whole block per successful CAS and serves the
    /// rest of it locally, so the storm costs one round trip per block per
    /// process instead of one per upload. The unused tail is deliberately not
    /// returned: an allocation id only has to be unique and increasing, so
    /// skipping ids is safe, while reusing one would alias two uploads.
    const ALLOCATION_BLOCK: u64 = 32;

    fn reserve_upload(&self, project_id: u32) -> Result<u64, S3Error> {
        if let Some(id) = self.take_cached_allocation(project_id) {
            return Ok(id);
        }
        let key = format!("/s3-upload-sequence/{project_id}");
        for _ in 0..128 {
            let current = self.meta.try_get(&key)?;
            let previous = match &current {
                Some(v) => serde_json::from_slice::<u64>(&v.value)?,
                None => 0,
            };
            let next = previous
                .checked_add(Self::ALLOCATION_BLOCK)
                .filter(|v| *v <= MAX_ALLOCATION)
                .ok_or(S3Error::GenerationExhausted)?;
            if matches!(
                self.meta.execute(KvCommand::RevisionTxn {
                    guards: vec![RevisionGuard {
                        key: key.clone(),
                        expected_mod_revision: current.as_ref().map_or(0, |v| v.mod_revision),
                    }],
                    ops: vec![TxnOp::Put {
                        key: key.clone(),
                        value: serde_json::to_vec(&next)?
                    }],
                })?,
                KvResult::TxnOk { .. }
            ) {
                // [previous+2, next] is the rest of the block; the durable
                // counter already sits at `next`, so a crash loses these ids
                // instead of handing one out twice.
                self.allocation_cache
                    .lock()
                    .unwrap()
                    .insert(project_id, (previous + 2, next + 1));
                return Ok(previous + 1);
            }
        }
        Err(S3Error::GenerationExhausted)
    }

    fn take_cached_allocation(&self, project_id: u32) -> Option<u64> {
        let mut cache = self.allocation_cache.lock().unwrap();
        let (next, end) = cache.get_mut(&project_id)?;
        if *next >= *end {
            cache.remove(&project_id);
            return None;
        }
        let id = *next;
        *next += 1;
        Some(id)
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
        self.put_object_with(
            project_id,
            store,
            pool_name,
            bucket,
            key,
            content_type,
            payload,
            |_| Ok(ObjectPublication::default()),
        )
    }

    /// [`Self::put_object`] with caller-supplied guards and ops appended to the
    /// manifest-publication transaction.
    ///
    /// `prepare` receives the manifest *as it will be published* — the same
    /// etag, size, chunk list and pool the success path returns — so a caller
    /// can record exactly the response it is about to send. It is invoked once,
    /// after the intent exists and the fragments are written, immediately before
    /// the single publication attempt. The engine does not retry a publication
    /// (`ConcurrentModification` is returned), so a caller must re-read its own
    /// record on that error to tell "a concurrent attempt committed this key"
    /// from "the object moved under us"; it must never retry the put blindly.
    ///
    /// Fragments and the intent are written before the publication, as in
    /// [`Self::put_object`]. A refusal, a conflict, or a crash before the
    /// publication leaves them unreferenced: the intent is collectable by the
    /// orphan scan and the fragments are reclaimed with it. Nothing rolls the
    /// fragments back.
    pub fn put_object_with<F>(
        &self,
        project_id: u32,
        store: &str,
        pool_name: &str,
        bucket: &str,
        key: &str,
        content_type: &str,
        payload: &[u8],
        mut prepare: F,
    ) -> Result<ObjectManifest, S3Error>
    where
        F: FnMut(&ObjectManifest) -> Result<ObjectPublication, S3Error>,
    {
        let pool = self.get_pool(pool_name)?;
        let mkey = Self::meta_key(project_id, store, bucket, key);
        let allocation_id = self.reserve_upload(project_id)?;
        let owner_id = (allocation_id >> 24) as u32;
        let next_gen = (allocation_id & BlobId::MAX_U24 as u64) as u32;
        let upload_id = format!("v2-{allocation_id:014x}");
        let intent_key = Self::intent_key(project_id, &upload_id);
        let intent_lease = self
            .meta
            .revision_for_key(&intent_key)?
            .saturating_add(INTENT_GRACE_REVISIONS);
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        for (index, slice) in payload.chunks(self.chunk_size).enumerate() {
            let step = u32::try_from(index + 1)
                .ok()
                .filter(|v| *v <= BlobId::MAX_U24)
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
            created_revision: self.meta.revision_for_key(&intent_key)?,
            lease_until_revision: intent_lease,
            lease_until_unix_secs: unix_seconds().saturating_add(INTENT_GRACE_SECS),
            allocation_id: Some(allocation_id),
            ..UploadIntent::default()
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

        let extra = prepare(&manifest)?;
        self.publish_upload_with(project_id, store, &manifest, &intent_key, extra)?;
        Ok(manifest)
    }

    /// Starts a multipart upload and durably records it before any part exists.
    ///
    /// The returned id is derived from a fresh allocation, so it cannot collide
    /// with a concurrent or restarted initiator. The record lives under
    /// `/s3-intent/` and names no fragments itself; each part is registered in
    /// its own single-allocation intent record.
    pub fn create_multipart_upload(
        &self,
        project_id: u32,
        store: &str,
        pool_name: &str,
        bucket: &str,
        key: &str,
        content_type: &str,
    ) -> Result<String, S3Error> {
        self.get_pool(pool_name)?;
        let allocation = self.reserve_upload(project_id)?;
        let upload_id = format!("mpu-{allocation:014x}");
        let intent_key = Self::intent_key(project_id, &upload_id);
        let revision = self.meta.revision_for_key(&intent_key)?;
        let intent = UploadIntent {
            upload_id: upload_id.clone(),
            manifest_key: Self::meta_key(project_id, store, bucket, key),
            pool: pool_name.to_string(),
            state: UploadState::Prepared,
            created_revision: revision,
            lease_until_revision: revision.saturating_add(INTENT_GRACE_REVISIONS),
            lease_until_unix_secs: unix_seconds().saturating_add(INTENT_GRACE_SECS),
            multipart: true,
            content_type: content_type.to_string(),
            ..UploadIntent::default()
        };
        let created = self.meta.execute(KvCommand::CompareAndSwap {
            key: intent_key,
            expected_version: 0,
            new_value: Some(serde_json::to_vec(&intent)?),
        })?;
        if !matches!(created, KvResult::CasOk { .. }) {
            return Err(S3Error::ConcurrentModification);
        }
        Ok(upload_id)
    }

    /// Reads and validates the multipart upload record for `upload_id`.
    fn multipart_upload(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(String, yadb_metakv::KvValue, UploadIntent), S3Error> {
        let intent_key = Self::intent_key(project_id, upload_id);
        let kv = self
            .meta
            .try_get(&intent_key)?
            .ok_or_else(|| S3Error::MultipartUploadNotFound(upload_id.to_string()))?;
        let intent: UploadIntent = serde_json::from_slice(&kv.value)?;
        if !intent.multipart
            || intent.state != UploadState::Prepared
            || intent.manifest_key != Self::meta_key(project_id, store, bucket, key)
        {
            return Err(S3Error::MultipartUploadNotFound(upload_id.to_string()));
        }
        Ok((intent_key, kv, intent))
    }

    /// Writes one part as its own durable allocation, then registers it in the
    /// upload record.
    ///
    /// Ordering is the safety property: the part intent (which names the
    /// fragments and the MD5 the client will complete with) is committed before
    /// a fragment is written, so a crash or a refused registration leaves only
    /// collectable state. The upload record is updated only after the fragments
    /// are durable, so it never points at a part that cannot be read.
    pub fn upload_part(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        payload: &[u8],
    ) -> Result<String, S3Error> {
        if part_number == 0 || part_number > MULTIPART_MAX_PARTS {
            return Err(S3Error::InvalidPart {
                part: part_number,
                reason: format!("part number must be 1..={MULTIPART_MAX_PARTS}"),
            });
        }
        if payload.len() > MULTIPART_PART_MAX {
            return Err(S3Error::PartTooLarge {
                part: part_number,
                size: payload.len() as u64,
                max: MULTIPART_PART_MAX,
            });
        }
        let (upload_key, _, upload) =
            self.multipart_upload(project_id, store, bucket, key, upload_id)?;
        let pool = self.get_pool(&upload.pool)?;
        let allocation = self.reserve_upload(project_id)?;
        let owner_id = (allocation >> 24) as u32;
        let generation = (allocation & BlobId::MAX_U24 as u64) as u32;
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        for (index, slice) in payload.chunks(self.chunk_size).enumerate() {
            let step = u32::try_from(index + 1)
                .ok()
                .filter(|v| *v <= BlobId::MAX_U24)
                .ok_or(S3Error::GenerationExhausted)?;
            chunks.push(ChunkRef {
                blob_id: BlobId::new(project_id, owner_id, generation, step, S3_CHANNEL, 0),
                offset,
                len: slice.len() as u32,
            });
            offset += slice.len() as u64;
        }
        let part_upload_id = format!("v2-{allocation:014x}");
        let part_intent_key = Self::intent_key(project_id, &part_upload_id);
        let part_revision = self.meta.revision_for_key(&part_intent_key)?;
        let part_intent = UploadIntent {
            upload_id: part_upload_id,
            manifest_key: upload.manifest_key.clone(),
            generation,
            pool: upload.pool.clone(),
            chunks: chunks.clone(),
            state: UploadState::Prepared,
            created_revision: part_revision,
            lease_until_revision: part_revision.saturating_add(INTENT_GRACE_REVISIONS),
            lease_until_unix_secs: unix_seconds().saturating_add(INTENT_GRACE_SECS),
            allocation_id: Some(allocation),
            ..UploadIntent::default()
        };
        let created = self.meta.execute(KvCommand::CompareAndSwap {
            key: part_intent_key.clone(),
            expected_version: 0,
            new_value: Some(serde_json::to_vec(&part_intent)?),
        })?;
        if !matches!(created, KvResult::CasOk { .. }) {
            return Err(S3Error::ConcurrentModification);
        }

        for (slice, chunk) in payload.chunks(self.chunk_size).zip(chunks.iter()) {
            pool.put_blob(chunk.blob_id, slice)?;
        }

        let etag = md5_hex(payload);
        let part = MultipartPartRef {
            part_number,
            intent_key: part_intent_key.clone(),
            etag: etag.clone(),
            size: payload.len() as u64,
            last_modified: unix_seconds(),
        };
        // The part intent must still be the `Prepared` record this call wrote.
        // Its own lease may lapse while this call is stalled, and collection is
        // then entitled to abort it. Registering without guarding it would
        // acknowledge a part whose fragments are already scheduled for
        // deletion, so the guard is part of the registration transaction and a
        // lost part guard ends the call instead of being retried.
        let part_kv = self
            .meta
            .try_get(&part_intent_key)?
            .filter(|kv| {
                serde_json::from_slice::<UploadIntent>(&kv.value)
                    .is_ok_and(|intent| intent.state == UploadState::Prepared)
            })
            .ok_or_else(|| S3Error::MultipartUploadNotFound(upload_id.to_string()))?;
        self.register_multipart_part(&upload_key, upload_id, &part, &part_kv)?;
        Ok(etag)
    }

    /// Registers one uploaded part against its parent upload.
    ///
    /// Registration is a small, bounded CAS retry: a parallel part changes the
    /// same record. The transaction guards both the upload record and the part's
    /// own `Prepared` intent, and refreshes both leases. A collector that
    /// aborted the part while the upload call was stalled therefore makes
    /// registration fail instead of acknowledging bytes that are already being
    /// reclaimed; a part whose own lease lapsed is refreshed here so the
    /// acknowledgement is durable.
    fn register_multipart_part(
        &self,
        upload_key: &str,
        upload_id: &str,
        part: &MultipartPartRef,
        part_kv: &KvValue,
    ) -> Result<(), S3Error> {
        let part_intent_key = part.intent_key.as_str();
        for _ in 0..64 {
            let current = self
                .meta
                .try_get(upload_key)?
                .ok_or_else(|| S3Error::MultipartUploadNotFound(upload_id.to_string()))?;
            let mut record: UploadIntent = serde_json::from_slice(&current.value)?;
            if !record.multipart || record.state != UploadState::Prepared {
                return Err(S3Error::MultipartUploadNotFound(upload_id.to_string()));
            }
            // A concurrent collector may have aborted the part between the
            // caller's read and this attempt; re-check before building the
            // guard.
            let live_part = self.meta.try_get(part_intent_key)?.filter(|kv| {
                kv.mod_revision == part_kv.mod_revision
                    && serde_json::from_slice::<UploadIntent>(&kv.value)
                        .is_ok_and(|intent| intent.state == UploadState::Prepared)
            });
            let Some(live_part) = live_part else {
                return Err(S3Error::MultipartUploadNotFound(upload_id.to_string()));
            };
            record.parts.retain(|p| p.part_number != part.part_number);
            record.parts.push(part.clone());
            record.parts.sort_by_key(|p| p.part_number);
            let revision = self.meta.revision_for_key(upload_key)?;
            record.lease_until_revision = revision.saturating_add(INTENT_GRACE_REVISIONS);
            record.lease_until_unix_secs = unix_seconds().saturating_add(INTENT_GRACE_SECS);
            // The part intent is refreshed in the same transaction. Its own
            // lease may have lapsed while this call was stalled; extending it
            // here is what makes the acknowledged part survive the collector
            // that is allowed to abort an expired, unregistered part.
            let mut live_intent: UploadIntent = serde_json::from_slice(&live_part.value)?;
            live_intent.lease_until_revision = revision.saturating_add(INTENT_GRACE_REVISIONS);
            live_intent.lease_until_unix_secs = unix_seconds().saturating_add(INTENT_GRACE_SECS);
            let result = self.meta.execute(KvCommand::RevisionTxn {
                guards: vec![
                    RevisionGuard {
                        key: upload_key.to_string(),
                        expected_mod_revision: current.mod_revision,
                    },
                    RevisionGuard {
                        key: part_intent_key.to_string(),
                        expected_mod_revision: part_kv.mod_revision,
                    },
                ],
                ops: vec![
                    TxnOp::Put {
                        key: upload_key.to_string(),
                        value: serde_json::to_vec(&record)?,
                    },
                    TxnOp::Put {
                        key: part_intent_key.to_string(),
                        value: serde_json::to_vec(&live_intent)?,
                    },
                ],
            })?;
            if matches!(result, KvResult::TxnOk { .. }) {
                return Ok(());
            }
        }
        Err(S3Error::ConcurrentModification)
    }

    /// Registers nothing and returns the parts the upload currently holds.
    pub fn list_multipart_parts(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<MultipartPartInfo>, S3Error> {
        let (_, _, upload) = self.multipart_upload(project_id, store, bucket, key, upload_id)?;
        Ok(upload
            .parts
            .iter()
            .map(|part| MultipartPartInfo {
                part_number: part.part_number,
                etag: part.etag.clone(),
                size: part.size,
                last_modified: part.last_modified,
            })
            .collect())
    }

    /// Completes a multipart upload in one revision-guarded transaction:
    /// manifest publication, retirement of the previous manifest, deletion of
    /// the upload record and deletion of every named part intent.
    ///
    /// The client's part list is checked against the recorded parts before the
    /// transaction. The transaction guards the upload record and every part
    /// intent, so a concurrent part upload, abort or collector transition makes
    /// it fail rather than publish a manifest that disagrees with what it read.
    /// The caller must not retry blindly on `ConcurrentModification`.
    pub fn complete_multipart_upload(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        requested: &[(u32, String)],
    ) -> Result<ObjectManifest, S3Error> {
        if requested.is_empty() {
            return Err(S3Error::MalformedMultipart(
                "at least one part is required".into(),
            ));
        }
        let (upload_key, upload_kv, upload) =
            self.multipart_upload(project_id, store, bucket, key, upload_id)?;
        // Fail before any guard is built if the pool is no longer mounted: the
        // transaction publishes ids that were written under this pool name.
        self.get_pool(&upload.pool)?;
        let mut previous_part = 0u32;
        for (part_number, _) in requested {
            if *part_number <= previous_part {
                return Err(S3Error::InvalidPartOrder);
            }
            previous_part = *part_number;
        }

        let mut chunks = Vec::new();
        let mut offset = 0u64;
        let mut part_digests = Vec::with_capacity(requested.len() * 16);
        let mut guards = vec![RevisionGuard {
            key: upload_key.clone(),
            expected_mod_revision: upload_kv.mod_revision,
        }];
        let mut ops = vec![TxnOp::Delete {
            key: upload_key.clone(),
        }];
        for (part_number, etag) in requested {
            let part = upload
                .parts
                .iter()
                .find(|p| p.part_number == *part_number)
                .ok_or_else(|| S3Error::InvalidPart {
                    part: *part_number,
                    reason: "part was not uploaded".into(),
                })?;
            let expected = part.etag.trim_matches('"');
            if !expected.eq_ignore_ascii_case(etag.trim_matches('"')) {
                return Err(S3Error::InvalidPart {
                    part: *part_number,
                    reason: "ETag mismatch".into(),
                });
            }
            let kv = self
                .meta
                .try_get(&part.intent_key)?
                .ok_or_else(|| S3Error::InvalidPart {
                    part: *part_number,
                    reason: "part data was collected".into(),
                })?;
            let intent: UploadIntent = serde_json::from_slice(&kv.value)?;
            if intent.state != UploadState::Prepared {
                return Err(S3Error::InvalidPart {
                    part: *part_number,
                    reason: "part data was collected".into(),
                });
            }
            let recorded: u64 = intent.chunks.iter().map(|c| c.len as u64).sum();
            if recorded != part.size {
                return Err(S3Error::InvalidPart {
                    part: *part_number,
                    reason: "part size changed".into(),
                });
            }
            let mut local = 0u64;
            for chunk in &intent.chunks {
                if chunk.offset != local || chunk.len as u64 > MULTIPART_PART_MAX as u64 {
                    return Err(S3Error::InvalidPart {
                        part: *part_number,
                        reason: "part chunk layout is invalid".into(),
                    });
                }
                chunks.push(ChunkRef {
                    blob_id: chunk.blob_id,
                    offset,
                    len: chunk.len,
                });
                local += chunk.len as u64;
                offset += chunk.len as u64;
            }
            let digest = unhex(expected).ok_or_else(|| S3Error::InvalidPart {
                part: *part_number,
                reason: "stored ETag is not an MD5 digest".into(),
            })?;
            if digest.len() != 16 {
                return Err(S3Error::InvalidPart {
                    part: *part_number,
                    reason: "stored ETag is not an MD5 digest".into(),
                });
            }
            part_digests.extend_from_slice(&digest);
            guards.push(RevisionGuard {
                key: part.intent_key.clone(),
                expected_mod_revision: kv.mod_revision,
            });
            ops.push(TxnOp::Delete {
                key: part.intent_key.clone(),
            });
        }

        let etag = format!("\"{}-{}\"", hex(&md5(&part_digests)), requested.len());
        let manifest = ObjectManifest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: offset,
            etag,
            content_type: upload.content_type.clone(),
            pool: upload.pool.clone(),
            chunks,
            last_modified: unix_seconds(),
        };
        let mkey = Self::meta_key(project_id, store, bucket, key);
        let previous = self.meta.try_get(&mkey)?;
        if let Some(value) = &previous {
            let old: ObjectManifest = serde_json::from_slice(&value.value)?;
            ops.push(TxnOp::Put {
                key: Self::retired_key(project_id, store, bucket, key, value.mod_revision),
                value: serde_json::to_vec(&old)?,
            });
        }
        guards.push(RevisionGuard {
            key: mkey.clone(),
            expected_mod_revision: previous.as_ref().map_or(0, |v| v.mod_revision),
        });
        ops.push(TxnOp::Put {
            key: mkey,
            value: serde_json::to_vec(&manifest)?,
        });
        match self.meta.execute(KvCommand::RevisionTxn { guards, ops })? {
            KvResult::TxnOk { .. } => Ok(manifest),
            _ => Err(S3Error::ConcurrentModification),
        }
    }

    /// Aborts an upload: deletes its record and marks every still-live part
    /// intent `Aborted` in the same transaction. The part fragments are then
    /// reclaimed by the existing collector, which re-confirms each transition
    /// through consensus before deleting anything.
    pub fn abort_multipart_upload(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), S3Error> {
        let (upload_key, upload_kv, upload) =
            self.multipart_upload(project_id, store, bucket, key, upload_id)?;
        let mut guards = vec![RevisionGuard {
            key: upload_key.clone(),
            expected_mod_revision: upload_kv.mod_revision,
        }];
        let mut ops = vec![TxnOp::Delete { key: upload_key }];
        for part in &upload.parts {
            let Some(kv) = self.meta.try_get(&part.intent_key)? else {
                continue;
            };
            let mut intent: UploadIntent = serde_json::from_slice(&kv.value)?;
            if intent.state != UploadState::Prepared {
                continue;
            }
            intent.state = UploadState::Aborted;
            guards.push(RevisionGuard {
                key: part.intent_key.clone(),
                expected_mod_revision: kv.mod_revision,
            });
            ops.push(TxnOp::Put {
                key: part.intent_key.clone(),
                value: serde_json::to_vec(&intent)?,
            });
        }
        match self.meta.execute(KvCommand::RevisionTxn { guards, ops })? {
            KvResult::TxnOk { .. } => Ok(()),
            _ => Err(S3Error::ConcurrentModification),
        }
    }

    // Used by the tests that publish a hand-built manifest; production paths go
    // through `publish_upload_with` so a caller can append guards and ops.
    #[cfg(test)]
    fn publish_upload(
        &self,
        project_id: u32,
        store: &str,
        manifest: &ObjectManifest,
        intent_key: &str,
    ) -> Result<(), S3Error> {
        self.publish_upload_with(
            project_id,
            store,
            manifest,
            intent_key,
            ObjectPublication::default(),
        )
    }

    fn publish_upload_with(
        &self,
        project_id: u32,
        store: &str,
        manifest: &ObjectManifest,
        intent_key: &str,
        extra: ObjectPublication,
    ) -> Result<(), S3Error> {
        let mkey = Self::meta_key(project_id, store, &manifest.bucket, &manifest.key);
        // The previous manifest and its guard MUST come from the same read.
        let previous = self.meta.try_get(&mkey)?;
        let encoded = serde_json::to_vec(&manifest)?;
        // Publish the manifest and remove the intent in one Raft transaction.
        // Publication and abort guard the same intent revision: only one may
        // win, even if both proposals are in flight after the grace expires.
        let mut ops = vec![
            yadb_metakv::TxnOp::Put {
                key: mkey.clone(),
                value: encoded,
            },
            yadb_metakv::TxnOp::Delete {
                key: intent_key.to_string(),
            },
        ];
        if let Some(value) = &previous {
            let old: ObjectManifest = serde_json::from_slice(&value.value)?;
            ops.push(yadb_metakv::TxnOp::Put {
                key: Self::retired_key(
                    project_id,
                    store,
                    &manifest.bucket,
                    &manifest.key,
                    value.mod_revision,
                ),
                value: serde_json::to_vec(&old)?,
            });
        }
        let current_intent = self
            .meta
            .try_get(&intent_key)?
            .ok_or(S3Error::ConcurrentModification)?;
        let current_intent_state: UploadIntent = serde_json::from_slice(&current_intent.value)?;
        if current_intent_state.state != UploadState::Prepared
            || current_intent_state.manifest_key != mkey
            || current_intent_state.pool != manifest.pool
            || current_intent_state.chunks != manifest.chunks
        {
            return Err(S3Error::ConcurrentModification);
        }
        let mut guards = vec![
            RevisionGuard {
                key: intent_key.to_string(),
                expected_mod_revision: current_intent.mod_revision,
            },
            RevisionGuard {
                key: mkey.clone(),
                expected_mod_revision: previous.as_ref().map_or(0, |v| v.mod_revision),
            },
        ];
        guards.extend(extra.guards);
        ops.extend(extra.ops);
        // The caller's guards are evaluated by the same apply, before any op
        // runs, so a failing caller guard also blocks the manifest write and
        // the intent deletion.
        let result = self.meta.execute(KvCommand::RevisionTxn { guards, ops })?;
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
            .try_get(&mkey)?
            .ok_or_else(|| S3Error::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
        Ok(serde_json::from_slice(&kv.value)?)
    }

    /// Quorum-confirmed manifest lookup. Legacy metadata stores return a routing
    /// error rather than the local applied view, so a caller that asked for a
    /// consistent read never silently receives stale metadata.
    pub fn head_object_consistent(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectManifest, S3Error> {
        let mkey = Self::meta_key(project_id, store, bucket, key);
        let snapshot = self.meta.try_linearizable_snapshot(&[mkey.clone()], &[])?;
        let kv = snapshot.get(&mkey).ok_or_else(|| S3Error::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })?;
        Ok(serde_json::from_slice(&kv.value)?)
    }

    /// Quorum-confirmed full object read: one linearizable manifest snapshot,
    /// then the chunks that manifest names. A manifest key absent from the
    /// snapshot is `ObjectNotFound`; a chunk that the snapshot references but
    /// that is gone surfaces as the underlying `BlobError`, never as zeros or a
    /// silent skip. No fallback to the locally applied manifest is attempted.
    pub fn get_object_consistent(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(ObjectManifest, Vec<u8>), S3Error> {
        let manifest = self.head_object_consistent(project_id, store, bucket, key)?;
        if manifest.size == 0 {
            return Ok((manifest, Vec::new()));
        }
        let end = manifest.size - 1;
        let bytes = self.read_manifest_range(&manifest, 0, end)?;
        Ok((manifest, bytes))
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
        self.delete_object_with(project_id, store, bucket, key, |_| {
            Ok(ObjectPublication::default())
        })
    }

    /// [`Self::delete_object`] with caller-supplied guards and ops appended to
    /// the deletion transaction.
    ///
    /// `prepare` receives whether the object existed in the same read whose
    /// revision guards the delete, so a caller can record exactly the
    /// `{"deleted": …}` answer the success path returns. It is invoked once,
    /// immediately before the single attempt; a `ConcurrentModification` is a
    /// definite non-commit and the caller must re-read its own record rather
    /// than retry blindly.
    pub fn delete_object_with<F>(
        &self,
        project_id: u32,
        store: &str,
        bucket: &str,
        key: &str,
        mut prepare: F,
    ) -> Result<bool, S3Error>
    where
        F: FnMut(bool) -> Result<ObjectPublication, S3Error>,
    {
        let mkey = Self::meta_key(project_id, store, bucket, key);
        let previous = self.meta.try_get(&mkey)?;
        let existed = previous.is_some();
        let mut ops = vec![yadb_metakv::TxnOp::Delete { key: mkey.clone() }];
        if let Some(value) = &previous {
            let manifest: ObjectManifest = serde_json::from_slice(&value.value)?;
            let retired = Self::retired_key(project_id, store, bucket, key, value.mod_revision);
            ops.push(yadb_metakv::TxnOp::Put {
                key: retired,
                value: serde_json::to_vec(&manifest)?,
            });
        }
        let extra = prepare(existed)?;
        let mut guards = vec![RevisionGuard {
            key: mkey,
            expected_mod_revision: previous.as_ref().map_or(0, |v| v.mod_revision),
        }];
        guards.extend(extra.guards);
        ops.extend(extra.ops);
        match self.meta.execute(KvCommand::RevisionTxn { guards, ops })? {
            KvResult::TxnOk { .. } => Ok(existed),
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
        let intents = self
            .meta
            .try_scan_prefix(&Self::intent_prefix(project_id))?;
        self.collect_orphans_from(project_id, now, intents)
    }

    /// One collection pass over a scan of the project's intents.
    ///
    /// Split from [`Self::collect_orphans_at`] so a test can hand it the stale
    /// scan a real pass may hold: the pass reads the record it is about to
    /// delete only through its guard, and must re-read after losing that guard.
    fn collect_orphans_from(
        &self,
        project_id: u32,
        now: u64,
        intents: Vec<(String, KvValue)>,
    ) -> Result<usize, S3Error> {
        let current_revision = self
            .meta
            .revision_for_key(&Self::intent_prefix(project_id))?;
        let mut removed = 0usize;

        // Multipart upload records name parts, never fragments, so they carry no
        // allocation id and the fragment checks below do not apply to them. A
        // live record pins its parts: a part's own lease may lapse while the
        // upload is still being assembled, and collecting it would break a
        // completion the client is entitled to make. An expired record is
        // deleted here (one guarded transaction, so a completion that commits in
        // between wins and this pass does nothing); its parts then become
        // ordinary unreferenced intents and are reclaimed by the loop below
        // once their own grace has elapsed.
        let mut pinned_parts: BTreeSet<String> = BTreeSet::new();
        for (key, value) in &intents {
            let Ok(record) = serde_json::from_slice::<UploadIntent>(&value.value) else {
                continue;
            };
            if !record.multipart
                || record.allocation_id.is_some()
                || key != &Self::intent_key(project_id, &record.upload_id)
            {
                continue;
            }
            if record.state == UploadState::Prepared
                && (record.lease_until_revision > current_revision
                    || record.lease_until_unix_secs > now)
            {
                pinned_parts.extend(record.parts.iter().map(|part| part.intent_key.clone()));
                continue;
            }
            let deleted = self.meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: key.clone(),
                    expected_mod_revision: value.mod_revision,
                }],
                ops: vec![TxnOp::Delete { key: key.clone() }],
            })?;
            if !matches!(deleted, KvResult::TxnOk { .. }) {
                // The record changed under this pass: a concurrent part
                // registration refreshed it, or a completion won. Never collect
                // from the stale scan. Re-read the record and retain the parts
                // it names now, so an older part that a registration just
                // re-confirmed is not aborted on the strength of the old pin
                // set. If the record is gone, its parts were deleted or
                // aborted by the transaction that won. An unreadable record
                // aborts the pass rather than leaving the pin set incomplete.
                if let Some(fresh) = self.meta.try_get(key)? {
                    if let Ok(fresh) = serde_json::from_slice::<UploadIntent>(&fresh.value) {
                        pinned_parts.extend(fresh.parts.iter().map(|part| part.intent_key.clone()));
                    }
                }
            }
        }

        for (key, value) in intents {
            let Ok(intent) = serde_json::from_slice::<UploadIntent>(&value.value) else {
                continue;
            };
            if pinned_parts.contains(&key) {
                continue;
            }
            let Some(allocation) = intent
                .allocation_id
                .filter(|id| *id > 0 && *id <= MAX_ALLOCATION)
            else {
                continue;
            };
            if key != Self::intent_key(project_id, &format!("v2-{allocation:014x}"))
                || intent.chunks.len() > BlobId::MAX_U24 as usize
                || intent.chunks.iter().enumerate().any(|(i, chunk)| {
                    chunk.blob_id
                        != BlobId::new(
                            project_id,
                            (allocation >> 24) as u32,
                            (allocation & BlobId::MAX_U24 as u64) as u32,
                            (i + 1) as u32,
                            S3_CHANNEL,
                            0,
                        )
                })
            {
                // Legacy/invalid intent: do not turn a hash collision into data loss.
                continue;
            }
            if intent.state == UploadState::Prepared
                && (intent.lease_until_revision > current_revision
                    || intent.lease_until_unix_secs > now)
            {
                continue;
            }
            // Even an already-Aborted value is re-confirmed through consensus.
            // Local reads may be stale; neither a follower nor an isolated old
            // leader is allowed to delete based only on its local view.
            let aborted = UploadIntent {
                state: UploadState::Aborted,
                ..intent.clone()
            };
            let abort = self.meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: key.clone(),
                    expected_mod_revision: value.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&aborted)?,
                }],
            })?;
            let KvResult::TxnOk {
                revision: aborted_revision,
                ..
            } = abort
            else {
                continue;
            };
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
                guards: vec![RevisionGuard {
                    key: key.clone(),
                    expected_mod_revision: aborted_revision,
                }],
                ops: vec![TxnOp::Delete { key }],
            })?;
        }
        Ok(removed)
    }

    fn is_reachable(&self, project_id: u32, pool: &str, blob_id: BlobId) -> Result<bool, S3Error> {
        for prefix in [
            format!("/s3/{project_id}/"),
            format!("/s3-retired/{project_id}/"),
        ] {
            for (_, value) in self.meta.try_scan_prefix(&prefix)? {
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
        // A Multi-Raft LocalMetaKv cannot provide one atomic, leader-fenced
        // snapshot across project groups.  Never scan/delete from a mixed
        // view: callers must disable the namespace-wide operation instead.
        if self.meta.is_openraft() {
            return Err(S3Error::NamespaceScanUnavailable);
        }
        let mut projects = std::collections::BTreeSet::new();
        for prefix in ["/s3/", "/s3-intent/", "/s3-retired/"] {
            for (key, _) in self.meta.try_scan_prefix(prefix)? {
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
        let rows = self.meta.try_scan_prefix(&full_prefix)?;
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
    use yadb_io::{FaultConfig, SimFs, SyncFault};

    #[test]
    fn publication_sync_error_recovers_as_published_or_collectible_not_rollback() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let meta_fs = SimFs::new(6310);
            let disks = [SimFs::new(6311), SimFs::new(6312), SimFs::new(6313)];
            let s3 = engine(&meta_fs, &disks);
            let (key, _, manifest) = prepared(&s3);
            let id = manifest.chunks[0].blob_id;
            s3.get_pool("pool")
                .unwrap()
                .put_blob(id, b"payload")
                .unwrap();
            meta_fs.set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            assert!(matches!(
                s3.publish_upload(1, "store", &manifest, &key),
                Err(S3Error::Meta(MetaError::Io(_)))
            ));
            meta_fs.set_faults(FaultConfig::default());
            assert!(matches!(
                s3.publish_upload(1, "store", &manifest, &key),
                Err(S3Error::Meta(MetaError::StorageUnavailable))
            ));
            drop(s3);
            meta_fs.simulate_power_cut();
            for disk in &disks {
                disk.simulate_power_cut();
            }
            let recovered = engine(&meta_fs, &disks);
            advance_revisions(&recovered);
            if fault == SyncFault::AfterPersist {
                assert!(recovered.meta.get_or_panic(&key).is_none());
                assert_eq!(recovered.collect_orphans_at(1, 1001).unwrap(), 0);
                assert_eq!(
                    recovered.get_object(1, "store", "bucket", "key").unwrap().1,
                    b"payload"
                );
            } else {
                assert!(recovered.head_object(1, "store", "bucket", "key").is_err());
                assert_eq!(recovered.collect_orphans_at(1, 1001).unwrap(), 1);
                assert!(recovered.meta.get_or_panic(&key).is_none());
                assert!(recovered
                    .get_pool("pool")
                    .unwrap()
                    .put_blob(id, b"payload")
                    .is_err());
            }
        }
    }

    #[test]
    fn tombstone_sync_error_keeps_aborted_cleanup_until_target_recovery() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let meta_fs = SimFs::new(6320);
            let disks = [SimFs::new(6321), SimFs::new(6322), SimFs::new(6323)];
            let s3 = engine(&meta_fs, &disks);
            let (key, _, manifest) = prepared(&s3);
            let id = manifest.chunks[0].blob_id;
            s3.get_pool("pool")
                .unwrap()
                .put_blob(id, b"payload")
                .unwrap();
            advance_revisions(&s3);
            disks[1].set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            assert!(s3.collect_orphans_at(1, 1001).is_err());
            let intent: UploadIntent =
                serde_json::from_slice(&s3.meta.get_or_panic(&key).unwrap().value).unwrap();
            assert_eq!(intent.state, UploadState::Aborted);
            disks[1].set_faults(FaultConfig::default());
            // Clearing injected EIO is not enough: target runtime state remains
            // suspect, and the collector must not forget an unconfirmed fence.
            assert!(s3.collect_orphans_at(1, 1001).is_err());
            assert!(s3.meta.get_or_panic(&key).is_some());
            assert!(s3.publish_upload(1, "store", &manifest, &key).is_err());
            drop(s3);
            meta_fs.simulate_power_cut();
            for disk in &disks {
                disk.simulate_power_cut();
            }
            let recovered = engine(&meta_fs, &disks);
            assert_eq!(
                recovered.collect_orphans_all().unwrap(),
                if fault == SyncFault::BeforePersist {
                    1
                } else {
                    0
                }
            );
            assert!(recovered.meta.get_or_panic(&key).is_none());
            assert!(recovered
                .get_pool("pool")
                .unwrap()
                .put_blob(id, b"payload")
                .is_err());
        }
    }

    fn engine(meta_fs: &SimFs, disks: &[SimFs]) -> ObjectStoreEngine<SimFs> {
        let meta = LocalMetaKv::open_single_node(meta_fs, "meta", 1).unwrap();
        let s3 = ObjectStoreEngine::new(meta, 4096);
        let domains = disks
            .iter()
            .enumerate()
            .map(|(i, fs)| {
                VolumeStore::open(fs.clone(), format!("d{i}"), VolumeConfig::default()).unwrap()
            })
            .collect();
        s3.register_pool(
            "pool",
            Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap()),
        );
        s3
    }

    // Stop an upload exactly after intent persistence, before metadata publication.
    fn prepared(s3: &ObjectStoreEngine<SimFs>) -> (String, UploadIntent, ObjectManifest) {
        let allocation = s3.reserve_upload(1).unwrap();
        let upload_id = format!("v2-{allocation:014x}");
        let key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let chunks = vec![ChunkRef {
            blob_id: BlobId::new(
                1,
                (allocation >> 24) as u32,
                (allocation & BlobId::MAX_U24 as u64) as u32,
                1,
                S3_CHANNEL,
                0,
            ),
            offset: 0,
            len: 7,
        }];
        let intent = UploadIntent {
            upload_id,
            manifest_key: "/s3/1/store/bucket/key".into(),
            generation: chunks[0].blob_id.generation(),
            pool: "pool".into(),
            chunks: chunks.clone(),
            state: UploadState::Prepared,
            created_revision: s3.meta.revision_or_panic(),
            lease_until_revision: s3.meta.revision_or_panic() + INTENT_GRACE_REVISIONS,
            lease_until_unix_secs: 1000,
            allocation_id: Some(allocation),
            ..UploadIntent::default()
        };
        s3.meta
            .execute(KvCommand::CompareAndSwap {
                key: key.clone(),
                expected_version: 0,
                new_value: Some(serde_json::to_vec(&intent).unwrap()),
            })
            .unwrap();
        let manifest = ObjectManifest {
            bucket: "bucket".into(),
            key: "key".into(),
            size: 7,
            etag: "test".into(),
            content_type: "text/plain".into(),
            pool: "pool".into(),
            chunks,
            last_modified: 0,
        };
        (key, intent, manifest)
    }

    fn advance_revisions(s3: &ObjectStoreEngine<SimFs>) {
        for _ in 0..INTENT_GRACE_REVISIONS {
            s3.meta
                .execute(KvCommand::Put {
                    key: "/test-tick".into(),
                    value: Vec::new(),
                })
                .unwrap();
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
        assert!(s3.meta.get_or_panic(&key).is_none());
        assert!(matches!(
            s3.publish_upload(1, "store", &manifest, &key),
            Err(S3Error::ConcurrentModification)
        ));
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
        s3.get_pool("pool")
            .unwrap()
            .put_blob(id, b"payload")
            .unwrap();
        advance_revisions(&s3);
        disks[1].set_faults(FaultConfig {
            write_eio_prob: 1.0,
            ..Default::default()
        });
        assert!(s3.collect_orphans_at(1, 1001).is_err());
        let aborted: UploadIntent =
            serde_json::from_slice(&s3.meta.get_or_panic(&key).unwrap().value).unwrap();
        assert_eq!(aborted.state, UploadState::Aborted);
        drop(s3);
        disks[1].set_faults(FaultConfig::default());
        meta_fs.simulate_power_cut();
        for disk in &disks {
            disk.simulate_power_cut();
        }
        let recovered = engine(&meta_fs, &disks);
        // No live object in this project: discovery must use the intent namespace.
        assert_eq!(recovered.collect_orphans_all().unwrap(), 1);
        assert!(recovered.meta.get_or_panic(&key).is_none());
        assert!(recovered
            .get_pool("pool")
            .unwrap()
            .put_blob(id, b"payload")
            .is_err());
    }

    #[test]
    fn published_upload_wins_over_late_collector_and_survives_power_cut() {
        let meta_fs = SimFs::new(6220);
        let disks = [SimFs::new(6221), SimFs::new(6222), SimFs::new(6223)];
        let s3 = engine(&meta_fs, &disks);
        let (key, _, manifest) = prepared(&s3);
        s3.get_pool("pool")
            .unwrap()
            .put_blob(manifest.chunks[0].blob_id, b"payload")
            .unwrap();
        s3.publish_upload(1, "store", &manifest, &key).unwrap();
        advance_revisions(&s3);
        assert_eq!(s3.collect_orphans_at(1, 1001).unwrap(), 0);
        drop(s3);
        meta_fs.simulate_power_cut();
        for disk in &disks {
            disk.simulate_power_cut();
        }
        let recovered = engine(&meta_fs, &disks);
        assert_eq!(
            recovered.get_object(1, "store", "bucket", "key").unwrap().1,
            b"payload"
        );
    }

    #[test]
    fn unpublishable_upload_cannot_commit_while_aborted_cleanup_is_incomplete() {
        let meta_fs = SimFs::new(6230);
        let disks = [SimFs::new(6231), SimFs::new(6232), SimFs::new(6233)];
        let s3 = engine(&meta_fs, &disks);
        let (key, _, manifest) = prepared(&s3);
        s3.get_pool("pool")
            .unwrap()
            .put_blob(manifest.chunks[0].blob_id, b"payload")
            .unwrap();
        advance_revisions(&s3);
        disks[0].set_faults(FaultConfig {
            write_eio_prob: 1.0,
            ..Default::default()
        });
        assert!(s3.collect_orphans_at(1, 1001).is_err());
        assert!(matches!(
            s3.publish_upload(1, "store", &manifest, &key),
            Err(S3Error::ConcurrentModification)
        ));
    }

    #[test]
    fn concurrent_independent_writers_use_disjoint_ids_and_restart_never_reuses_them() {
        let meta_fs = SimFs::new(6240);
        let disks = [SimFs::new(6241), SimFs::new(6242), SimFs::new(6243)];
        let s3 = engine(&meta_fs, &disks);
        let ids = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..12)
                .map(|i| {
                    let writer = ObjectStoreEngine::new(s3.meta.clone(), 4096);
                    writer.register_pool("pool", s3.get_pool("pool").unwrap());
                    scope.spawn(move || {
                        writer
                            .put_object(
                                1,
                                "store",
                                "pool",
                                "bucket",
                                &format!("key-{i}"),
                                "text/plain",
                                b"payload",
                            )
                            .unwrap()
                            .chunks[0]
                            .blob_id
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<std::collections::BTreeSet<_>>()
        });
        assert_eq!(ids.len(), 12);
        drop(s3);
        meta_fs.simulate_power_cut();
        for disk in &disks {
            disk.simulate_power_cut();
        }
        let recovered = engine(&meta_fs, &disks);
        let new = recovered
            .put_object(1, "store", "pool", "bucket", "key-0", "text/plain", b"new")
            .unwrap();
        assert!(!ids.contains(&new.chunks[0].blob_id));
    }

    #[test]
    fn allocator_reserves_a_block_so_concurrent_writers_do_not_alias_or_reuse_ids() {
        let meta_fs = SimFs::new(6260);
        let disks = [SimFs::new(6261), SimFs::new(6262), SimFs::new(6263)];
        let s3 = engine(&meta_fs, &disks);
        let ids = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16)
                .map(|_| {
                    let writer = ObjectStoreEngine::new(s3.meta.clone(), 4096);
                    scope.spawn(move || writer.reserve_upload(1).unwrap())
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut distinct = ids.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            ids.len(),
            "two writers shared an id: {ids:?}"
        );
        // Every success advanced the durable counter by a whole block, so the
        // first id after a restart lies beyond every id handed out *and* beyond
        // the unused tails: reusing a tail would alias an upload that may still
        // exist, which is why the tail is skipped rather than reclaimed.
        let stored = s3.meta.get_or_panic("/s3-upload-sequence/1").unwrap();
        let counter: u64 = serde_json::from_slice(&stored.value).unwrap();
        assert_eq!(counter, 16 * ObjectStoreEngine::<SimFs>::ALLOCATION_BLOCK);
        drop(s3);
        meta_fs.simulate_power_cut();
        for disk in &disks {
            disk.simulate_power_cut();
        }
        let recovered = engine(&meta_fs, &disks);
        let first = recovered.reserve_upload(1).unwrap();
        assert!(
            first > *ids.iter().max().unwrap(),
            "{first} <= {:?}",
            ids.iter().max()
        );
        assert!(first > counter);
        // One process serves a whole block locally: sixteen reservations cost a
        // single extra CAS, and every id is still distinct and beyond the
        // durable counter.
        let mut block = vec![first];
        block.extend((0..15).map(|_| recovered.reserve_upload(1).unwrap()));
        block.sort_unstable();
        block.dedup();
        assert_eq!(block.len(), 16, "{block:?}");
        let stored = recovered
            .meta
            .get_or_panic("/s3-upload-sequence/1")
            .unwrap();
        let advanced: u64 = serde_json::from_slice(&stored.value).unwrap();
        assert_eq!(
            advanced,
            counter + ObjectStoreEngine::<SimFs>::ALLOCATION_BLOCK
        );
    }

    #[test]
    fn allocator_crosses_generation_boundary_and_stops_before_id_wraparound() {
        let meta_fs = SimFs::new(6245);
        let disks = [SimFs::new(6246), SimFs::new(6247), SimFs::new(6248)];
        let s3 = engine(&meta_fs, &disks);
        s3.meta
            .execute(KvCommand::Put {
                key: "/s3-upload-sequence/1".into(),
                value: serde_json::to_vec(&(BlobId::MAX_U24 as u64)).unwrap(),
            })
            .unwrap();
        let manifest = s3
            .put_object(
                1,
                "store",
                "pool",
                "bucket",
                "key",
                "text/plain",
                b"payload",
            )
            .unwrap();
        let id = manifest.chunks[0].blob_id;
        assert_eq!(id.owner_id(), 1);
        assert_eq!(id.generation(), 0);
        assert_eq!(id.channel(), S3_CHANNEL);
        s3.meta
            .execute(KvCommand::Put {
                key: "/s3-upload-sequence/1".into(),
                value: serde_json::to_vec(&MAX_ALLOCATION).unwrap(),
            })
            .unwrap();
        // A process-local block would still be serving ids; a restart drops it,
        // so the exhaustion check uses a fresh engine over the same metadata.
        let restarted = ObjectStoreEngine::new(s3.meta.clone(), 4096);
        assert!(matches!(
            restarted.reserve_upload(1),
            Err(S3Error::GenerationExhausted)
        ));
    }

    #[test]
    fn malformed_manifest_stops_cleanup_without_forgetting_the_aborted_intent() {
        let meta_fs = SimFs::new(6249);
        let disks = [SimFs::new(6256), SimFs::new(6257), SimFs::new(6258)];
        let s3 = engine(&meta_fs, &disks);
        let (key, _, manifest) = prepared(&s3);
        let id = manifest.chunks[0].blob_id;
        s3.get_pool("pool")
            .unwrap()
            .put_blob(id, b"payload")
            .unwrap();
        advance_revisions(&s3);
        s3.meta
            .execute(KvCommand::Put {
                key: manifest_key(),
                value: b"invalid-json".to_vec(),
            })
            .unwrap();
        assert!(matches!(
            s3.collect_orphans_at(1, 1001),
            Err(S3Error::Json(_))
        ));
        assert!(s3.meta.get_or_panic(&key).is_some());
        assert_eq!(
            s3.get_pool("pool").unwrap().get_blob(id).unwrap(),
            b"payload"
        );
    }

    #[test]
    fn intent_collision_prevents_any_fragment_put() {
        let meta_fs = SimFs::new(6250);
        let disks = [SimFs::new(6251), SimFs::new(6252), SimFs::new(6253)];
        let s3 = engine(&meta_fs, &disks);
        s3.meta
            .execute(KvCommand::Put {
                key: ObjectStoreEngine::<SimFs>::intent_key(1, "v2-00000000000001"),
                value: b"occupied".to_vec(),
            })
            .unwrap();
        assert!(matches!(
            s3.put_object(
                1,
                "store",
                "pool",
                "bucket",
                "key",
                "text/plain",
                b"payload"
            ),
            Err(S3Error::ConcurrentModification)
        ));
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
            s3.get_pool("pool")
                .unwrap()
                .put_blob(id, b"payload")
                .unwrap();
            meta_fs.set_faults(FaultConfig {
                torn_write_prob: if torn { 1.0 } else { 0.0 },
                write_eio_prob: if torn { 0.0 } else { 1.0 },
                ..Default::default()
            });
            assert!(matches!(
                s3.publish_upload(1, "store", &manifest, &key),
                Err(S3Error::Meta(_))
            ));
            assert!(s3.meta.get_or_panic(&manifest_key()).is_none());
            assert!(s3.meta.get_or_panic(&key).is_some());
            drop(s3);
            meta_fs.set_faults(FaultConfig::default());
            meta_fs.simulate_power_cut();
            for disk in &disks {
                disk.simulate_power_cut();
            }
            let recovered = engine(&meta_fs, &disks);
            advance_revisions(&recovered);
            assert_eq!(recovered.collect_orphans_at(1, 1001).unwrap(), 1);
            assert!(recovered.meta.get_or_panic(&key).is_none());
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
            s3.get_pool("pool")
                .unwrap()
                .put_blob(id, b"payload")
                .unwrap();
            advance_revisions(&s3);
            let start = std::sync::Barrier::new(2);
            let (published, collected) = std::thread::scope(|scope| {
                let publisher = scope.spawn(|| {
                    start.wait();
                    s3.publish_upload(1, "store", &manifest, &key)
                });
                let collector = scope.spawn(|| {
                    start.wait();
                    s3.collect_orphans_at(1, 1001)
                });
                (
                    publisher.join().unwrap(),
                    collector.join().unwrap().unwrap(),
                )
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
        s3.meta
            .execute(KvCommand::Put {
                key,
                value: serde_json::to_vec(&intent).unwrap(),
            })
            .unwrap();
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
        s3.get_pool("pool")
            .unwrap()
            .put_blob(id, b"payload")
            .unwrap();
        intent.state = UploadState::Aborted;
        s3.meta
            .execute(KvCommand::Put {
                key,
                value: serde_json::to_vec(&intent).unwrap(),
            })
            .unwrap();
        let term = s3.meta.role_info().2;
        s3.meta
            .step_rpc(yadb_metakv::RaftRpc::RequestVote {
                term: term + 1,
                candidate_id: 2,
                last_log_index: u64::MAX,
                last_log_term: term + 1,
            })
            .unwrap();
        assert!(matches!(
            s3.collect_orphans_at(1, 1001),
            Err(S3Error::Meta(MetaError::NotLeader { .. }))
        ));
        assert_eq!(
            s3.get_pool("pool").unwrap().get_blob(id).unwrap(),
            b"payload"
        );
    }

    #[test]
    fn failed_fragment_quorum_is_reclaimed_but_committed_object_stays_durable() {
        for torn in [false, true] {
            let meta_fs = SimFs::new(6300);
            let disks = [SimFs::new(6301), SimFs::new(6302), SimFs::new(6303)];
            let s3 = engine(&meta_fs, &disks);
            s3.put_object(1, "store", "pool", "bucket", "live", "text/plain", b"live")
                .unwrap();
            for disk in &disks[1..] {
                disk.set_faults(FaultConfig {
                    torn_write_prob: if torn { 1.0 } else { 0.0 },
                    write_eio_prob: if torn { 0.0 } else { 1.0 },
                    ..Default::default()
                });
            }
            assert!(matches!(
                s3.put_object(
                    1,
                    "store",
                    "pool",
                    "bucket",
                    "failed",
                    "text/plain",
                    b"orphan"
                ),
                Err(S3Error::Blob(_))
            ));
            let pending = s3.meta.scan_prefix_or_panic("/s3-intent/1/");
            assert_eq!(pending.len(), 1);
            let intent: UploadIntent = serde_json::from_slice(&pending[0].1.value).unwrap();
            drop(s3);
            meta_fs.simulate_power_cut();
            for disk in &disks {
                disk.set_faults(FaultConfig::default());
                disk.simulate_power_cut();
            }
            let recovered = engine(&meta_fs, &disks);
            advance_revisions(&recovered);
            assert_eq!(
                recovered
                    .collect_orphans_at(1, intent.lease_until_unix_secs + 1)
                    .unwrap(),
                1
            );
            assert_eq!(
                recovered
                    .get_object(1, "store", "bucket", "live")
                    .unwrap()
                    .1,
                b"live"
            );
            assert!(recovered
                .head_object(1, "store", "bucket", "failed")
                .is_err());
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
        s3.put_object(
            1,
            "receipts",
            "data-ec",
            "invoices-2026",
            "nov/inv-002.bin",
            "text/plain",
            b"replacement",
        )
        .unwrap();
        assert_eq!(
            s3.read_manifest_range(&mf, 4090, 8200).unwrap(),
            payload[4090..=8200]
        );
        assert!(matches!(
            s3.read_manifest_range(&mf, 0, mf.size),
            Err(S3Error::InvalidRange { .. })
        ));

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
        let pool =
            Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
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
            ..Default::default()
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
            domains.push(
                VolumeStore::open(data_fs.clone(), format!("d{i}"), VolumeConfig::default())
                    .unwrap(),
            );
        }
        let pool =
            Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
        s3.register_pool("pool", pool.clone());

        let live = s3
            .put_object(
                1,
                "store",
                "pool",
                "bucket",
                "key",
                "text/plain",
                b"live-value",
            )
            .unwrap();
        let orphan_id = BlobId::new(1, 0, 77, 1, S3_CHANNEL, 0);
        pool.put_blob(orphan_id, b"orphan-value").unwrap();
        let upload_id = format!("v2-{:014x}", 77);
        let intent_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let intent = UploadIntent {
            upload_id,
            manifest_key: "/s3/1/store/bucket/orphan".into(),
            generation: 77,
            pool: "pool".into(),
            chunks: vec![ChunkRef {
                blob_id: orphan_id,
                offset: 0,
                len: 12,
            }],
            state: UploadState::Prepared,
            created_revision: 0,
            lease_until_revision: 0,
            lease_until_unix_secs: 0,
            allocation_id: Some(77),
            ..UploadIntent::default()
        };
        s3.meta
            .execute(KvCommand::Put {
                key: intent_key,
                value: serde_json::to_vec(&intent).unwrap(),
            })
            .unwrap();
        assert_eq!(s3.collect_orphans(1).unwrap(), 1);
        assert!(matches!(
            pool.get_blob(orphan_id),
            Err(BlobError::NotFound(_))
        ));
        assert_eq!(
            s3.get_object(1, "store", "bucket", "key").unwrap().1,
            b"live-value"
        );
        assert_eq!(live.size, 10);
    }

    #[test]
    fn overwrite_and_delete_retire_old_fragments_without_deleting_new_manifest() {
        let fs = SimFs::new(6103);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let s3 = ObjectStoreEngine::new(meta, 4096);
        let mut domains = Vec::new();
        for i in 0..3 {
            domains.push(
                VolumeStore::open(fs.clone(), format!("d{i}"), VolumeConfig::default()).unwrap(),
            );
        }
        let pool =
            Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap());
        s3.register_pool("pool", pool.clone());
        let old = s3
            .put_object(1, "store", "pool", "bucket", "key", "text/plain", b"old")
            .unwrap();
        let new = s3
            .put_object(1, "store", "pool", "bucket", "key", "text/plain", b"new")
            .unwrap();
        assert_eq!(s3.collect_orphans(1).unwrap(), 0);
        assert_eq!(s3.read_manifest_range(&old, 0, 2).unwrap(), b"old");
        assert_eq!(
            s3.get_object(1, "store", "bucket", "key").unwrap().1,
            b"new"
        );
        assert!(s3.delete_object(1, "store", "bucket", "key").unwrap());
        assert_eq!(s3.collect_orphans(1).unwrap(), 0);
        assert!(matches!(
            s3.head_object(1, "store", "bucket", "key"),
            Err(S3Error::ObjectNotFound { .. })
        ));
        assert_eq!(s3.read_manifest_range(&new, 0, 2).unwrap(), b"new");
    }

    // The opt-in path must never fall back to the local applied manifest. On a
    // legacy store it has no quorum-confirmed snapshot to offer, so it answers
    // with the documented routing error even though the object is locally visible.
    #[test]
    fn consistent_get_refuses_legacy_meta_instead_of_returning_local_object() {
        let meta_fs = SimFs::new(6300);
        let disks: Vec<SimFs> = (0..3).map(|i| SimFs::new(6301 + i)).collect();
        let s3 = engine(&meta_fs, &disks);
        s3.put_object(
            1,
            "store",
            "pool",
            "bucket",
            "key",
            "text/plain",
            b"payload",
        )
        .unwrap();

        assert_eq!(
            s3.get_object(1, "store", "bucket", "key").unwrap().1,
            b"payload",
            "the default path still serves the local applied state"
        );
        assert!(matches!(
            s3.get_object_consistent(1, "store", "bucket", "key"),
            Err(S3Error::Meta(MetaError::Routing(_)))
        ));
        assert!(matches!(
            s3.head_object_consistent(1, "store", "bucket", "key"),
            Err(S3Error::Meta(MetaError::Routing(_)))
        ));
    }

    // A manifest pinned by a consistent read is a point-in-time view, not a GC
    // fence: a chunk it names may be collected before the read fetches it. The
    // read must then fail rather than return zeros or a short body.
    // `read_manifest_range` is exactly the chunk fetch used by
    // `get_object_consistent`, and the daemon maps this `BlobError` shape to 503
    // on the consistent path.
    #[test]
    fn consistent_read_reports_a_missing_chunk_instead_of_partial_bytes() {
        let meta_fs = SimFs::new(6310);
        let disks: Vec<SimFs> = (0..3).map(|i| SimFs::new(6311 + i)).collect();
        let s3 = engine(&meta_fs, &disks);
        let payload: Vec<u8> = (0..9000u32).map(|i| (i % 251) as u8).collect();
        let mf = s3
            .put_object(
                1,
                "store",
                "pool",
                "bucket",
                "key",
                "application/octet-stream",
                &payload,
            )
            .unwrap();
        assert!(mf.chunks.len() >= 2);

        let pool = s3.get_pool(&mf.pool).unwrap();
        for chunk in &mf.chunks {
            assert!(pool.delete(chunk.blob_id).unwrap());
        }
        assert!(matches!(
            s3.read_manifest_range(&mf, 0, mf.size - 1),
            Err(S3Error::Blob(BlobError::NotFound(_)))
        ));
    }
    // The manifest and the caller record carry one cluster revision, which only
    // a single transaction can produce, and `prepare` saw the manifest that
    // committed (so a recorded response cannot describe a different object).
    #[test]
    fn keyed_put_commits_manifest_and_record_in_one_transaction() {
        let meta_fs = SimFs::new(6104);
        let disks = [SimFs::new(6105), SimFs::new(6106), SimFs::new(6107)];
        let s3 = engine(&meta_fs, &disks);
        let record = "/s3-put/1/store/bucket/key/op-1".to_string();
        let mut seen = None;
        let manifest = s3
            .put_object_with(
                1,
                "store",
                "pool",
                "bucket",
                "key",
                "text/plain",
                b"keyed-payload",
                |mf| {
                    seen = Some(mf.clone());
                    Ok(ObjectPublication {
                        guards: vec![RevisionGuard {
                            key: record.clone(),
                            expected_mod_revision: 0,
                        }],
                        ops: vec![TxnOp::Put {
                            key: record.clone(),
                            value: b"done".to_vec(),
                        }],
                    })
                },
            )
            .unwrap();
        let stored = s3.meta.get_or_panic(&record).unwrap();
        let mkey = ObjectStoreEngine::<SimFs>::meta_key(1, "store", "bucket", "key");
        let published = s3.meta.get_or_panic(&mkey).unwrap();
        assert_eq!(stored.value, b"done");
        assert_eq!(
            published.mod_revision, stored.mod_revision,
            "manifest and record were not written by one transaction"
        );
        let seen = seen.expect("prepare must be invoked");
        assert_eq!(seen.etag, manifest.etag);
        assert_eq!(seen.size, manifest.size);
        assert_eq!(seen.chunks, manifest.chunks);
        assert_eq!(seen.pool, manifest.pool);
        assert_eq!(
            s3.get_object(1, "store", "bucket", "key").unwrap().1,
            b"keyed-payload"
        );
    }

    // Atomicity: an unsatisfiable caller guard must abort the whole apply, so
    // neither the manifest nor the record appears. The fragments and intent were
    // already written; they stay unreferenced and the orphan scan reclaims them.
    #[test]
    fn failed_caller_guard_leaves_manifest_and_record_unpublished() {
        let meta_fs = SimFs::new(6108);
        let disks = [SimFs::new(6109), SimFs::new(6110), SimFs::new(6111)];
        let s3 = engine(&meta_fs, &disks);
        let record = "/s3-put/1/store/bucket/key/never".to_string();
        let result = s3.put_object_with(
            1,
            "store",
            "pool",
            "bucket",
            "key",
            "text/plain",
            b"refused",
            |_| {
                Ok(ObjectPublication {
                    guards: vec![RevisionGuard {
                        key: record.clone(),
                        expected_mod_revision: 1,
                    }],
                    ops: vec![TxnOp::Put {
                        key: record.clone(),
                        value: b"done".to_vec(),
                    }],
                })
            },
        );
        assert!(matches!(result, Err(S3Error::ConcurrentModification)));
        assert!(
            s3.meta.get_or_panic(&record).is_none(),
            "caller record leaked although the transaction never committed"
        );
        assert!(matches!(
            s3.head_object(1, "store", "bucket", "key"),
            Err(S3Error::ObjectNotFound { .. })
        ));
        // The intent survived the refusal, so the orphan scan can reclaim it
        // (and its fragments) once its grace elapses.
        advance_revisions(&s3);
        assert_eq!(s3.collect_orphans_at(1, u64::MAX).unwrap(), 1);
    }

    #[test]
    fn keyed_delete_commits_tombstone_and_record_in_one_transaction() {
        let meta_fs = SimFs::new(6112);
        let disks = [SimFs::new(6113), SimFs::new(6114), SimFs::new(6115)];
        let s3 = engine(&meta_fs, &disks);
        s3.put_object(1, "store", "pool", "bucket", "key", "text/plain", b"value")
            .unwrap();
        let record = "/s3-delete/1/store/bucket/key/op-1".to_string();
        let mkey = ObjectStoreEngine::<SimFs>::meta_key(1, "store", "bucket", "key");
        let before = s3.meta.get_or_panic(&mkey).unwrap();
        let mut seen = None;
        let existed = s3
            .delete_object_with(1, "store", "bucket", "key", |existed| {
                seen = Some(existed);
                Ok(ObjectPublication {
                    guards: vec![RevisionGuard {
                        key: record.clone(),
                        expected_mod_revision: 0,
                    }],
                    ops: vec![TxnOp::Put {
                        key: record.clone(),
                        value: b"done".to_vec(),
                    }],
                })
            })
            .unwrap();
        assert!(existed);
        assert_eq!(seen, Some(true), "prepare must see the guarded existence");
        let stored = s3.meta.get_or_panic(&record).unwrap();
        assert!(
            s3.meta.get_or_panic(&mkey).is_none(),
            "manifest survived a committed delete"
        );
        assert!(matches!(
            s3.head_object(1, "store", "bucket", "key"),
            Err(S3Error::ObjectNotFound { .. })
        ));
        assert_eq!(stored.value, b"done");
        // The retirement tombstone and the record are in that same revision.
        let retired = ObjectStoreEngine::<SimFs>::retired_key(
            1,
            "store",
            "bucket",
            "key",
            before.mod_revision,
        );
        assert_eq!(
            s3.meta.get_or_panic(&retired).unwrap().mod_revision,
            stored.mod_revision
        );
    }

    #[test]
    fn failed_caller_guard_leaves_object_and_delete_record_untouched() {
        let meta_fs = SimFs::new(6116);
        let disks = [SimFs::new(6117), SimFs::new(6118), SimFs::new(6119)];
        let s3 = engine(&meta_fs, &disks);
        s3.put_object(1, "store", "pool", "bucket", "key", "text/plain", b"value")
            .unwrap();
        let mkey = ObjectStoreEngine::<SimFs>::meta_key(1, "store", "bucket", "key");
        let before = s3.meta.get_or_panic(&mkey).unwrap();
        let record = "/s3-delete/1/store/bucket/key/never".to_string();
        let result = s3.delete_object_with(1, "store", "bucket", "key", |_| {
            Ok(ObjectPublication {
                guards: vec![RevisionGuard {
                    key: record.clone(),
                    expected_mod_revision: 1,
                }],
                ops: vec![TxnOp::Put {
                    key: record.clone(),
                    value: b"done".to_vec(),
                }],
            })
        });
        assert!(matches!(result, Err(S3Error::ConcurrentModification)));
        assert!(s3.meta.get_or_panic(&record).is_none());
        assert_eq!(
            s3.meta.get_or_panic(&mkey).unwrap().mod_revision,
            before.mod_revision
        );
        assert_eq!(
            s3.get_object(1, "store", "bucket", "key").unwrap().1,
            b"value"
        );
    }

    // A refusal from `prepare` is reported without a transaction and without
    // publishing; it is not retried.
    #[test]
    fn put_prepare_refusal_publishes_nothing() {
        let meta_fs = SimFs::new(6120);
        let disks = [SimFs::new(6121), SimFs::new(6122), SimFs::new(6123)];
        let s3 = engine(&meta_fs, &disks);
        let mut attempts = 0u32;
        let result = s3.put_object_with(
            1,
            "store",
            "pool",
            "bucket",
            "key",
            "text/plain",
            b"refused",
            |_| {
                attempts += 1;
                Err(S3Error::PoolNotMounted("refused".into()))
            },
        );
        assert!(matches!(result, Err(S3Error::PoolNotMounted(_))));
        assert_eq!(attempts, 1);
        assert!(matches!(
            s3.head_object(1, "store", "bucket", "key"),
            Err(S3Error::ObjectNotFound { .. })
        ));
    }

    // ---- multipart upload -------------------------------------------------

    #[test]
    fn md5_matches_rfc_1321_vectors() {
        // Padding boundary cases matter here: a part's ETag is the MD5 of its
        // bytes, and the multipart ETag is the MD5 of the concatenated digests.
        for (input, expected) in [
            ("", "d41d8cd98f00b204e9800998ecf8427e"),
            ("abc", "900150983cd24fb0d6963f7d28e17f72"),
            (
                "The quick brown fox jumps over the lazy dog",
                "9e107d9d372bb6826bd81d3542a419d6",
            ),
            (
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "ef1772b6dff9a122358552954ad0df65",
            ),
            (
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "3b0c8ac703f828b04c6c197006d17218",
            ),
            (
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "014842d480b571495a4a0363793f7367",
            ),
        ] {
            assert_eq!(hex(&md5(input.as_bytes())), expected, "len {}", input.len());
        }
    }

    fn multipart_engine(tag: u64) -> ObjectStoreEngine<SimFs> {
        let meta_fs = SimFs::new(tag);
        let disks = [
            SimFs::new(tag + 1),
            SimFs::new(tag + 2),
            SimFs::new(tag + 3),
        ];
        engine(&meta_fs, &disks)
    }

    fn expected_multipart_etag(parts: &[&[u8]]) -> String {
        let mut digests = Vec::new();
        for part in parts {
            digests.extend_from_slice(&md5(part));
        }
        format!("\"{}-{}\"", hex(&md5(&digests)), parts.len())
    }

    #[test]
    fn multipart_roundtrip_assembles_parts_in_order_and_retires_the_previous_object() {
        let s3 = multipart_engine(7100);
        s3.put_object(
            1,
            "store",
            "pool",
            "bucket",
            "key",
            "text/plain",
            b"previous",
        )
        .unwrap();
        let previous = s3
            .meta
            .get_or_panic(&ObjectStoreEngine::<SimFs>::meta_key(
                1, "store", "bucket", "key",
            ))
            .unwrap();

        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        assert!(upload_id.starts_with("mpu-"), "{upload_id}");

        let parts: [&[u8]; 3] = [&[7u8; 5000], &[8u8; 4096], &[9u8; 100]];
        let mut etags = Vec::new();
        for (index, part) in parts.iter().enumerate() {
            let etag = s3
                .upload_part(
                    1,
                    "store",
                    "bucket",
                    "key",
                    &upload_id,
                    index as u32 + 1,
                    part,
                )
                .unwrap();
            assert_eq!(etag, hex(&md5(part)));
            etags.push((index as u32 + 1, etag));
        }

        let listed = s3
            .list_multipart_parts(1, "store", "bucket", "key", &upload_id)
            .unwrap();
        assert_eq!(listed.len(), 3);
        assert_eq!(listed[0].part_number, 1);
        assert_eq!(listed[0].size, 5000);
        assert_eq!(listed[2].part_number, 3);

        let manifest = s3
            .complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &etags)
            .unwrap();
        assert_eq!(manifest.size, 5000 + 4096 + 100);
        assert_eq!(manifest.etag, expected_multipart_etag(&parts));
        assert_eq!(manifest.content_type, "text/plain");
        // Part boundaries are not chunk boundaries: the 5000-byte part spans two
        // 4096-byte chunks, so the assembled object is four chunks.
        assert_eq!(manifest.chunks.len(), 4);
        assert_eq!(manifest.chunks[1].offset, 4096);
        assert_eq!(manifest.chunks[2].offset, 5000);

        let (_, bytes) = s3.get_object(1, "store", "bucket", "key").unwrap();
        let expected: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();
        assert_eq!(bytes, expected);

        // The upload record and every part intent are gone in the same
        // transaction that published the manifest.
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        assert!(s3.meta.get_or_panic(&upload_key).is_none());
        for (_, value) in s3
            .meta
            .scan_prefix_or_panic(&ObjectStoreEngine::<SimFs>::intent_prefix(1))
        {
            panic!(
                "an intent survived completion: {}",
                String::from_utf8_lossy(&value.value)
            );
        }
        // The replaced object is retained as a retirement record, not deleted.
        assert!(s3
            .meta
            .get_or_panic(&ObjectStoreEngine::<SimFs>::retired_key(
                1,
                "store",
                "bucket",
                "key",
                previous.mod_revision
            ))
            .is_some());
        // A second completion of the same upload is NoSuchUpload, never a
        // second publication.
        assert!(matches!(
            s3.complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &etags),
            Err(S3Error::MultipartUploadNotFound(_))
        ));
    }

    #[test]
    fn multipart_rejects_a_part_that_was_never_uploaded() {
        let s3 = multipart_engine(7110);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        s3.upload_part(1, "store", "bucket", "key", &upload_id, 1, b"first")
            .unwrap();
        let err = s3
            .complete_multipart_upload(
                1,
                "store",
                "bucket",
                "key",
                &upload_id,
                &[(1, hex(&md5(b"first"))), (2, hex(&md5(b"second")))],
            )
            .unwrap_err();
        assert!(matches!(err, S3Error::InvalidPart { part: 2, .. }), "{err}");
        // The refused completion published nothing and left the upload intact.
        assert!(s3.head_object(1, "store", "bucket", "key").is_err());
        assert_eq!(
            s3.list_multipart_parts(1, "store", "bucket", "key", &upload_id)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn multipart_rejects_a_wrong_part_etag_and_a_changed_part_size() {
        let s3 = multipart_engine(7120);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        let etag = s3
            .upload_part(1, "store", "bucket", "key", &upload_id, 1, b"payload")
            .unwrap();
        // A digest the client did not upload is refused.
        let err = s3
            .complete_multipart_upload(
                1,
                "store",
                "bucket",
                "key",
                &upload_id,
                &[(1, hex(&md5(b"other")))],
            )
            .unwrap_err();
        assert!(matches!(err, S3Error::InvalidPart { part: 1, .. }), "{err}");
        // A record whose size disagrees with the stored intent is refused too:
        // the size is re-derived from the durable part, not trusted from the
        // upload record.
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let kv = s3.meta.get_or_panic(&upload_key).unwrap();
        let mut record: UploadIntent = serde_json::from_slice(&kv.value).unwrap();
        record.parts[0].size += 1;
        s3.meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: upload_key.clone(),
                    expected_mod_revision: kv.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: upload_key,
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            })
            .unwrap();
        let err = s3
            .complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &[(1, etag)])
            .unwrap_err();
        assert!(matches!(err, S3Error::InvalidPart { part: 1, .. }), "{err}");
        assert!(s3.head_object(1, "store", "bucket", "key").is_err());
    }

    #[test]
    fn multipart_rejects_duplicate_and_out_of_order_parts() {
        let s3 = multipart_engine(7130);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        let one = s3
            .upload_part(1, "store", "bucket", "key", &upload_id, 1, b"one")
            .unwrap();
        let two = s3
            .upload_part(1, "store", "bucket", "key", &upload_id, 2, b"two")
            .unwrap();
        for requested in [
            vec![(2, two.clone()), (1, one.clone())],
            vec![(1, one.clone()), (1, one.clone())],
        ] {
            let err = s3
                .complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &requested)
                .unwrap_err();
            assert!(matches!(err, S3Error::InvalidPartOrder), "{err}");
        }
        let err = s3
            .complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &[])
            .unwrap_err();
        assert!(matches!(err, S3Error::MalformedMultipart(_)), "{err}");
    }

    #[test]
    fn multipart_rejects_out_of_range_part_numbers_and_oversized_parts() {
        let s3 = multipart_engine(7140);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        for part in [0, MULTIPART_MAX_PARTS + 1] {
            let err = s3
                .upload_part(1, "store", "bucket", "key", &upload_id, part, b"x")
                .unwrap_err();
            assert!(matches!(err, S3Error::InvalidPart { .. }), "{err}");
        }
        let oversized = vec![0u8; MULTIPART_PART_MAX + 1];
        let err = s3
            .upload_part(1, "store", "bucket", "key", &upload_id, 1, &oversized)
            .unwrap_err();
        assert!(
            matches!(err, S3Error::PartTooLarge { part: 1, max, .. } if max == MULTIPART_PART_MAX),
            "{err}"
        );
        // Nothing was registered by any refusal.
        assert!(s3
            .list_multipart_parts(1, "store", "bucket", "key", &upload_id)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn multipart_reupload_of_one_part_replaces_the_earlier_bytes() {
        let s3 = multipart_engine(7150);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        let stale = s3
            .upload_part(1, "store", "bucket", "key", &upload_id, 1, b"stale")
            .unwrap();
        let fresh = s3
            .upload_part(1, "store", "bucket", "key", &upload_id, 1, b"fresh!")
            .unwrap();
        assert_ne!(stale, fresh);
        let listed = s3
            .list_multipart_parts(1, "store", "bucket", "key", &upload_id)
            .unwrap();
        assert_eq!(listed.len(), 1, "one part number is one part");
        assert_eq!(listed[0].etag, fresh);
        // The superseded ETag is no longer completable.
        assert!(matches!(
            s3.complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &[(1, stale)]),
            Err(S3Error::InvalidPart { .. })
        ));
        s3.complete_multipart_upload(
            1,
            "store",
            "bucket",
            "key",
            &upload_id,
            &[(1, fresh.clone())],
        )
        .unwrap();
        assert_eq!(
            s3.get_object(1, "store", "bucket", "key").unwrap().1,
            b"fresh!"
        );
    }

    #[test]
    fn multipart_completion_reads_every_part_from_its_own_durable_intent() {
        // The completion does not trust the upload record for sizes: it re-reads
        // each part intent and re-derives the byte count. A record whose size
        // disagrees is refused rather than published.
        let s3 = multipart_engine(7160);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        let etag = s3
            .upload_part(1, "store", "bucket", "key", &upload_id, 1, b"payload")
            .unwrap();
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let kv = s3.meta.get_or_panic(&upload_key).unwrap();
        let mut record: UploadIntent = serde_json::from_slice(&kv.value).unwrap();
        record.parts[0].size = 1;
        s3.meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: upload_key.clone(),
                    expected_mod_revision: kv.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: upload_key.clone(),
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            })
            .unwrap();
        let err = s3
            .complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &[(1, etag)])
            .unwrap_err();
        assert!(matches!(err, S3Error::InvalidPart { part: 1, .. }), "{err}");
        assert!(s3.head_object(1, "store", "bucket", "key").is_err());
        assert!(s3.meta.get_or_panic(&upload_key).is_some());
    }

    #[test]
    fn multipart_abort_retires_the_record_and_lets_the_collector_reclaim_parts() {
        let s3 = multipart_engine(7170);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        s3.upload_part(1, "store", "bucket", "key", &upload_id, 1, &[1u8; 5000])
            .unwrap();
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        assert!(s3.meta.get_or_panic(&upload_key).is_some());
        s3.abort_multipart_upload(1, "store", "bucket", "key", &upload_id)
            .unwrap();
        assert!(s3.meta.get_or_panic(&upload_key).is_none());
        // The part intent is still present but marked Aborted, so the collector
        // reclaims its fragments without waiting for the lease.
        assert!(matches!(
            s3.upload_part(1, "store", "bucket", "key", &upload_id, 2, b"late"),
            Err(S3Error::MultipartUploadNotFound(_))
        ));
        assert!(matches!(
            s3.abort_multipart_upload(1, "store", "bucket", "key", &upload_id),
            Err(S3Error::MultipartUploadNotFound(_))
        ));
        // Two chunks of 4096 + 904 bytes.
        assert_eq!(s3.collect_orphans(1).unwrap(), 2);
        for (key, _) in s3
            .meta
            .scan_prefix_or_panic(&ObjectStoreEngine::<SimFs>::intent_prefix(1))
        {
            panic!("aborted part intent `{key}` survived collection");
        }
        assert!(s3.head_object(1, "store", "bucket", "key").is_err());
    }

    #[test]
    fn incomplete_multipart_upload_is_collected_after_the_grace_and_not_before() {
        let s3 = multipart_engine(7180);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        s3.upload_part(1, "store", "bucket", "key", &upload_id, 1, &[2u8; 5000])
            .unwrap();
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        // Neither grace alone is enough.
        assert_eq!(s3.collect_orphans_at(1, 1).unwrap(), 0);
        assert!(s3.meta.get_or_panic(&upload_key).is_some());
        advance_revisions(&s3);
        assert_eq!(
            s3.collect_orphans_at(1, unix_seconds() + INTENT_GRACE_SECS + 1)
                .unwrap(),
            2
        );
        assert!(s3.meta.get_or_panic(&upload_key).is_none());
        for (key, _) in s3
            .meta
            .scan_prefix_or_panic(&ObjectStoreEngine::<SimFs>::intent_prefix(1))
        {
            panic!("incomplete upload `{key}` leaked");
        }
        // The client learns the upload is gone instead of completing a
        // half-collected upload.
        assert!(matches!(
            s3.upload_part(1, "store", "bucket", "key", &upload_id, 2, b"late"),
            Err(S3Error::MultipartUploadNotFound(_))
        ));
    }

    #[test]
    fn a_live_multipart_record_pins_its_parts_past_their_own_lease() {
        let s3 = multipart_engine(7190);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        let etag = s3
            .upload_part(1, "store", "bucket", "key", &upload_id, 1, b"pinned")
            .unwrap();
        // Expire only the part intent: an upload that takes longer than the
        // part lease must still be completable.
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let kv = s3.meta.get_or_panic(&upload_key).unwrap();
        let record: UploadIntent = serde_json::from_slice(&kv.value).unwrap();
        let part_key = record.parts[0].intent_key.clone();
        let part_kv = s3.meta.get_or_panic(&part_key).unwrap();
        let mut part: UploadIntent = serde_json::from_slice(&part_kv.value).unwrap();
        part.lease_until_unix_secs = 0;
        part.lease_until_revision = 0;
        s3.meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: part_key.clone(),
                    expected_mod_revision: part_kv.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: part_key.clone(),
                    value: serde_json::to_vec(&part).unwrap(),
                }],
            })
            .unwrap();
        assert_eq!(s3.collect_orphans_at(1, u64::MAX).unwrap(), 0);
        assert!(s3.meta.get_or_panic(&part_key).is_some());
        let manifest = s3
            .complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &[(1, etag)])
            .unwrap();
        assert_eq!(manifest.size, 6);
        assert_eq!(
            s3.get_object(1, "store", "bucket", "key").unwrap().1,
            b"pinned"
        );
    }

    #[test]
    fn multipart_registration_guards_the_part_intent_and_refreshes_its_lease() {
        let s3 = multipart_engine(7200);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        s3.upload_part(1, "store", "bucket", "key", &upload_id, 1, b"part")
            .unwrap();
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let record: UploadIntent =
            serde_json::from_slice(&s3.meta.get_or_panic(&upload_key).unwrap().value).unwrap();
        let part = record.parts[0].clone();
        let part_key = part.intent_key.clone();

        // Expire the part intent's own lease, then register the same part
        // again. Registration must refresh that lease in the same transaction:
        // an acknowledged part must not stay collectable because the upload
        // call outlived the lease it was created with.
        let kv = s3.meta.get_or_panic(&part_key).unwrap();
        let mut intent: UploadIntent = serde_json::from_slice(&kv.value).unwrap();
        intent.lease_until_unix_secs = 0;
        intent.lease_until_revision = 0;
        s3.meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: part_key.clone(),
                    expected_mod_revision: kv.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: part_key.clone(),
                    value: serde_json::to_vec(&intent).unwrap(),
                }],
            })
            .unwrap();
        let stale = s3.meta.get_or_panic(&part_key).unwrap();
        s3.register_multipart_part(&upload_key, &upload_id, &part, &stale)
            .unwrap();
        let refreshed: UploadIntent =
            serde_json::from_slice(&s3.meta.get_or_panic(&part_key).unwrap().value).unwrap();
        assert!(refreshed.lease_until_unix_secs > unix_seconds());
        assert!(refreshed.lease_until_revision > 0);

        // A collector aborting the part must make registration refuse rather
        // than return an ETag for bytes it is reclaiming. The value passed here
        // is the aborted record's own current revision, so the revision guard
        // matches and only the state check can refuse: otherwise the test would
        // pass on a stale revision alone and prove nothing about the state.
        let kv = s3.meta.get_or_panic(&part_key).unwrap();
        let mut aborted: UploadIntent = serde_json::from_slice(&kv.value).unwrap();
        aborted.state = UploadState::Aborted;
        s3.meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: part_key.clone(),
                    expected_mod_revision: kv.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: part_key.clone(),
                    value: serde_json::to_vec(&aborted).unwrap(),
                }],
            })
            .unwrap();
        let aborted_now = s3.meta.get_or_panic(&part_key).unwrap();
        let aborted_record: UploadIntent = serde_json::from_slice(&aborted_now.value).unwrap();
        assert_eq!(aborted_record.state, UploadState::Aborted);
        assert!(matches!(
            s3.register_multipart_part(&upload_key, &upload_id, &part, &aborted_now),
            Err(S3Error::MultipartUploadNotFound(_))
        ));
    }

    #[test]
    fn orphan_collection_re_reads_a_record_whose_guard_it_lost() {
        let s3 = multipart_engine(7200);
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        s3.upload_part(1, "store", "bucket", "key", &upload_id, 1, b"one")
            .unwrap();
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let part_one = {
            let record: UploadIntent =
                serde_json::from_slice(&s3.meta.get_or_panic(&upload_key).unwrap().value).unwrap();
            record.parts[0].intent_key.clone()
        };

        // Make the upload record look expired, take the scan a collector would
        // hold, and only then register a second part. Registration refreshes the
        // record's lease and bumps its revision, so the collector's delete guard
        // fails — the interleaving this branch exists for.
        //
        // Part 1's own leases are expired too. Otherwise its revision lease
        // (extended by 32 revisions at registration) would outlive the few
        // revisions a second registration advances, and the collector would
        // retain it on that lease alone even without re-pinning the record it
        // just failed to delete — the test would pass with the branch removed.
        let kv = s3.meta.get_or_panic(&upload_key).unwrap();
        let mut expired: UploadIntent = serde_json::from_slice(&kv.value).unwrap();
        expired.lease_until_unix_secs = 0;
        expired.lease_until_revision = 0;
        s3.meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: upload_key.clone(),
                    expected_mod_revision: kv.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: upload_key.clone(),
                    value: serde_json::to_vec(&expired).unwrap(),
                }],
            })
            .unwrap();
        let part_kv = s3.meta.get_or_panic(&part_one).unwrap();
        let mut expired_part: UploadIntent = serde_json::from_slice(&part_kv.value).unwrap();
        expired_part.lease_until_unix_secs = 0;
        expired_part.lease_until_revision = 0;
        s3.meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: part_one.clone(),
                    expected_mod_revision: part_kv.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: part_one.clone(),
                    value: serde_json::to_vec(&expired_part).unwrap(),
                }],
            })
            .unwrap();
        let stale = s3
            .meta
            .scan_prefix_or_panic(&ObjectStoreEngine::<SimFs>::intent_prefix(1));
        s3.upload_part(1, "store", "bucket", "key", &upload_id, 2, b"two")
            .unwrap();

        // The pass must not abort the parts it saw in the stale scan.
        assert_eq!(s3.collect_orphans_from(1, u64::MAX, stale).unwrap(), 0);
        for key in [&part_one, &upload_key] {
            let kv = s3.meta.get_or_panic(key).expect("record survives the pass");
            let record: UploadIntent = serde_json::from_slice(&kv.value).unwrap();
            assert_ne!(record.state, UploadState::Aborted, "{key}");
        }
        let record: UploadIntent =
            serde_json::from_slice(&s3.meta.get_or_panic(&upload_key).unwrap().value).unwrap();
        assert_eq!(record.parts.len(), 2);
    }

    /// Filesystems kept alive so a test can inject faults and power-cycle them
    /// after the engine has been dropped.
    fn multipart_fixture(tag: u64) -> (SimFs, [SimFs; 3], ObjectStoreEngine<SimFs>) {
        let meta_fs = SimFs::new(tag);
        let disks = [
            SimFs::new(tag + 1),
            SimFs::new(tag + 2),
            SimFs::new(tag + 3),
        ];
        let s3 = engine(&meta_fs, &disks);
        (meta_fs, disks, s3)
    }

    /// Starts a multipart upload, uploads `parts`, and returns the upload id,
    /// the completion list, and the body a correct completion must publish.
    fn multipart_parts(
        s3: &ObjectStoreEngine<SimFs>,
        parts: &[&[u8]],
    ) -> (String, Vec<(u32, String)>, Vec<u8>) {
        let upload_id = s3
            .create_multipart_upload(1, "store", "pool", "bucket", "key", "text/plain")
            .unwrap();
        let mut etags = Vec::with_capacity(parts.len());
        for (index, part) in parts.iter().enumerate() {
            let etag = s3
                .upload_part(
                    1,
                    "store",
                    "bucket",
                    "key",
                    &upload_id,
                    index as u32 + 1,
                    part,
                )
                .unwrap();
            etags.push((index as u32 + 1, etag));
        }
        let body = parts.iter().flat_map(|part| part.iter().copied()).collect();
        (upload_id, etags, body)
    }

    /// Part intent keys named by the upload record, in part order.
    fn part_intents(s3: &ObjectStoreEngine<SimFs>, upload_id: &str) -> Vec<String> {
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, upload_id);
        let record: UploadIntent =
            serde_json::from_slice(&s3.meta.get_or_panic(&upload_key).unwrap().value).unwrap();
        record
            .parts
            .iter()
            .map(|part| part.intent_key.clone())
            .collect()
    }

    /// Every fragment a part intent names, captured before a completion deletes
    /// those intents.
    fn part_fragments(s3: &ObjectStoreEngine<SimFs>, part_keys: &[String]) -> Vec<BlobId> {
        part_keys
            .iter()
            .flat_map(|key| {
                let intent: UploadIntent =
                    serde_json::from_slice(&s3.meta.get_or_panic(key).unwrap().value).unwrap();
                intent
                    .chunks
                    .iter()
                    .map(|chunk| chunk.blob_id)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Drops the engine, cuts power to every filesystem and reopens it, so the
    /// assertions only see what survived on durable storage.
    fn crash_and_reopen(
        s3: ObjectStoreEngine<SimFs>,
        meta_fs: &SimFs,
        disks: &[SimFs],
    ) -> ObjectStoreEngine<SimFs> {
        drop(s3);
        meta_fs.simulate_power_cut();
        for disk in disks {
            disk.simulate_power_cut();
        }
        engine(meta_fs, disks)
    }

    #[test]
    fn multipart_completion_before_persist_failure_leaves_parts_reclaimable() {
        let (meta_fs, disks, s3) = multipart_fixture(7300);
        let parts: [&[u8]; 3] = [&[7u8; 5000], &[8u8; 4096], &[9u8; 100]];
        let (upload_id, etags, _) = multipart_parts(&s3, &parts);
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let part_keys = part_intents(&s3, &upload_id);
        // 5000 bytes span two 4096-byte chunks, the other parts one each.
        let fragments = part_fragments(&s3, &part_keys);
        assert_eq!(fragments.len(), 4);

        meta_fs.set_faults(FaultConfig {
            sync_data_fault: SyncFault::BeforePersist,
            ..Default::default()
        });
        assert!(matches!(
            s3.complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &etags),
            Err(S3Error::Meta(MetaError::Io(_)))
        ));
        // The single transaction was not applied at all: no manifest, and the
        // record and part intents it would have deleted are still there.
        assert!(s3.head_object(1, "store", "bucket", "key").is_err());
        assert!(s3.meta.get_or_panic(&upload_key).is_some());
        for key in &part_keys {
            assert!(
                s3.meta.get_or_panic(key).is_some(),
                "part intent `{key}` vanished"
            );
        }
        meta_fs.set_faults(FaultConfig::default());

        let recovered = crash_and_reopen(s3, &meta_fs, &disks);
        // The unsynced completion is gone after the power cut: no object, so no
        // manifest can name fragments that were reclaimed.
        assert!(matches!(
            recovered.head_object(1, "store", "bucket", "key"),
            Err(S3Error::ObjectNotFound { .. })
        ));
        // The upload and every part survived intact and are still readable, so
        // nothing was leaked or silently lost.
        assert!(recovered.meta.get_or_panic(&upload_key).is_some());
        assert_eq!(
            recovered
                .list_multipart_parts(1, "store", "bucket", "key", &upload_id)
                .unwrap()
                .len(),
            3
        );
        for blob_id in &fragments {
            assert!(
                recovered
                    .get_pool("pool")
                    .unwrap()
                    .get_blob(*blob_id)
                    .is_ok(),
                "fragment {blob_id:?} was lost"
            );
        }
        // And they are reclaimable: abort retires the record and the collector
        // deletes every fragment and every part intent.
        recovered
            .abort_multipart_upload(1, "store", "bucket", "key", &upload_id)
            .unwrap();
        assert_eq!(
            recovered.collect_orphans(1).unwrap(),
            fragments.len(),
            "the collector must reclaim every part fragment"
        );
        assert!(recovered.meta.get_or_panic(&upload_key).is_none());
        for key in &part_keys {
            assert!(
                recovered.meta.get_or_panic(key).is_none(),
                "aborted part intent `{key}` leaked"
            );
        }
        for blob_id in &fragments {
            assert!(recovered
                .get_pool("pool")
                .unwrap()
                .get_blob(*blob_id)
                .is_err());
        }
        assert!(recovered.head_object(1, "store", "bucket", "key").is_err());
    }

    #[test]
    fn multipart_completion_after_persist_failure_is_durable_and_never_half_published() {
        let (meta_fs, disks, s3) = multipart_fixture(7310);
        let parts: [&[u8]; 3] = [&[3u8; 5000], &[4u8; 4096], &[5u8; 100]];
        let (upload_id, etags, body) = multipart_parts(&s3, &parts);
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let part_keys = part_intents(&s3, &upload_id);
        let fragments = part_fragments(&s3, &part_keys);
        assert_eq!(fragments.len(), 4);

        meta_fs.set_faults(FaultConfig {
            sync_data_fault: SyncFault::AfterPersist,
            ..Default::default()
        });
        // The caller is told the outcome is unknown: the sync failed after the
        // transaction reached the WAL.
        assert!(matches!(
            s3.complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &etags),
            Err(S3Error::Meta(MetaError::Io(_)))
        ));
        meta_fs.set_faults(FaultConfig::default());

        let recovered = crash_and_reopen(s3, &meta_fs, &disks);
        // Observed on this fixture: the whole transaction was durable and the
        // `Ok` arm below is taken. The `Err` arm is still asserted because the
        // invariant is "one of the two", not "published".
        match recovered.get_object(1, "store", "bucket", "key") {
            // The transaction was durable, so the object is complete and no part
            // intent survived it.
            Ok((manifest, bytes)) => {
                assert_eq!(bytes, body, "a durable completion must publish every byte");
                assert_eq!(manifest.size, body.len() as u64);
                assert_eq!(manifest.etag, expected_multipart_etag(&parts));
                assert!(recovered.meta.get_or_panic(&upload_key).is_none());
                for key in &part_keys {
                    assert!(
                        recovered.meta.get_or_panic(key).is_none(),
                        "part intent `{key}` survived"
                    );
                }
                // The published manifest still names readable fragments, and a
                // collector pass reclaims nothing it needs.
                for chunk in &manifest.chunks {
                    assert!(recovered
                        .get_pool("pool")
                        .unwrap()
                        .get_blob(chunk.blob_id)
                        .is_ok());
                }
                assert_eq!(recovered.collect_orphans_all().unwrap(), 0);
            }
            // The other legal outcome: the transaction did not survive. Then the
            // upload must still be whole and every part reclaimable.
            Err(S3Error::ObjectNotFound { .. }) => {
                assert!(recovered.meta.get_or_panic(&upload_key).is_some());
                for key in &part_keys {
                    assert!(
                        recovered.meta.get_or_panic(key).is_some(),
                        "part intent `{key}` vanished"
                    );
                }
                recovered
                    .abort_multipart_upload(1, "store", "bucket", "key", &upload_id)
                    .unwrap();
                assert_eq!(recovered.collect_orphans(1).unwrap(), fragments.len());
            }
            // A manifest naming reclaimed fragments, or a partial body, is the
            // in-between state this test exists to rule out.
            Err(other) => panic!("recovered read is neither published nor absent: {other}"),
        }
    }

    #[test]
    fn multipart_completion_torn_write_recovers_old_or_new_manifest_never_garbage() {
        let (meta_fs, disks, s3) = multipart_fixture(7320);
        let previous = s3
            .put_object(
                1,
                "store",
                "pool",
                "bucket",
                "key",
                "text/plain",
                b"previous",
            )
            .unwrap();
        let parts: [&[u8]; 2] = [&[1u8; 5000], &[2u8; 500]];
        let (upload_id, etags, body) = multipart_parts(&s3, &parts);
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let part_keys = part_intents(&s3, &upload_id);

        meta_fs.set_faults(FaultConfig {
            torn_write_prob: 1.0,
            ..Default::default()
        });
        assert!(matches!(
            s3.complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &etags),
            Err(S3Error::Meta(MetaError::Io(_)))
        ));
        meta_fs.set_faults(FaultConfig::default());

        let recovered = crash_and_reopen(s3, &meta_fs, &disks);
        let mkey = ObjectStoreEngine::<SimFs>::meta_key(1, "store", "bucket", "key");
        let stored = recovered
            .meta
            .get_or_panic(&mkey)
            .expect("a manifest is present after recovery");
        // A torn frame must never leave a half-written JSON value behind.
        let manifest: ObjectManifest =
            serde_json::from_slice(&stored.value).expect("the recovered manifest parses");
        let (_, bytes) = recovered
            .get_object(1, "store", "bucket", "key")
            .expect("the recovered object reads back");
        assert_eq!(manifest.size, bytes.len() as u64, "truncated body");
        // Observed on this fixture: the `if` arm is taken, because a torn frame
        // is never synced and the power cut discards it. The `else` arm is kept
        // because recovery must accept either state.
        if manifest.etag == previous.etag {
            // Old state: the torn frame was discarded as an unacknowledged tail.
            assert_eq!(bytes, b"previous");
            assert!(recovered.meta.get_or_panic(&upload_key).is_some());
            for key in &part_keys {
                assert!(
                    recovered.meta.get_or_panic(key).is_some(),
                    "part intent `{key}` vanished"
                );
            }
            // A retry after recovery publishes every part: the torn attempt
            // never leaves a prefix of the body behind.
            recovered
                .complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &etags)
                .unwrap();
            assert_eq!(
                recovered.get_object(1, "store", "bucket", "key").unwrap().1,
                body
            );
        } else {
            // New state: the frame survived whole, so the manifest is the new
            // one and names only fragments that are present.
            assert_eq!(manifest.etag, expected_multipart_etag(&parts));
            assert_eq!(bytes, body);
            assert!(recovered.meta.get_or_panic(&upload_key).is_none());
            for key in &part_keys {
                assert!(
                    recovered.meta.get_or_panic(key).is_none(),
                    "part intent `{key}` survived"
                );
            }
            assert_eq!(recovered.collect_orphans_all().unwrap(), 0);
        }
        // Every manifest in the namespace parses, so recovery did not keep a
        // garbage value next to the good one.
        assert_eq!(
            recovered
                .list_objects(1, "store", "bucket", "")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn failed_multipart_completion_keeps_the_upload_completable_and_the_retry_is_whole() {
        let (meta_fs, disks, s3) = multipart_fixture(7330);
        let parts: [&[u8]; 3] = [&[1u8; 5000], &[2u8; 4096], &[3u8; 7]];
        let (upload_id, etags, body) = multipart_parts(&s3, &parts);
        let upload_key = ObjectStoreEngine::<SimFs>::intent_key(1, &upload_id);
        let part_keys = part_intents(&s3, &upload_id);

        meta_fs.set_faults(FaultConfig {
            sync_data_fault: SyncFault::BeforePersist,
            ..Default::default()
        });
        assert!(matches!(
            s3.complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &etags),
            Err(S3Error::Meta(MetaError::Io(_)))
        ));
        meta_fs.set_faults(FaultConfig::default());

        let recovered = crash_and_reopen(s3, &meta_fs, &disks);
        // The failed completion deleted nothing: the manifest was not published,
        // so the part intents that would have been deleted in the same
        // transaction are all still there.
        assert!(recovered.head_object(1, "store", "bucket", "key").is_err());
        assert!(recovered.meta.get_or_panic(&upload_key).is_some());
        for key in &part_keys {
            assert!(
                recovered.meta.get_or_panic(key).is_some(),
                "completion deleted part intent `{key}` without publishing"
            );
        }
        // Observed outcome: the upload is still completable (test 1 shows the
        // abort branch), and the retry publishes the whole concatenation.
        let manifest = recovered
            .complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &etags)
            .unwrap();
        assert_eq!(manifest.size, body.len() as u64);
        assert_eq!(manifest.etag, expected_multipart_etag(&parts));
        assert_eq!(
            recovered.get_object(1, "store", "bucket", "key").unwrap().1,
            body
        );
        assert!(recovered.meta.get_or_panic(&upload_key).is_none());
        for key in &part_keys {
            assert!(
                recovered.meta.get_or_panic(key).is_none(),
                "part intent `{key}` survived"
            );
        }
        assert_eq!(recovered.collect_orphans_all().unwrap(), 0);
        // A second completion cannot republish or truncate the body.
        assert!(matches!(
            recovered.complete_multipart_upload(1, "store", "bucket", "key", &upload_id, &etags),
            Err(S3Error::MultipartUploadNotFound(_))
        ));
        assert_eq!(
            recovered.get_object(1, "store", "bucket", "key").unwrap().1,
            body
        );
    }
}
