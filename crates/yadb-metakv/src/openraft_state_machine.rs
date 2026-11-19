//! OpenRaft state machine and snapshot storage.
//!
//! It is opt-in and keeps the state-machine file in
//! its own directory so its advisory lock does not conflict with the log
//! store's lock.  The on-disk image contains application state, applied log
//! position and membership; watchers are runtime-only and are recreated by the
//! owner after restart.
//!
//! Not production-ready: snapshot metadata/ordering and crash recovery need
//! further work and fault tests, as do lifecycle ownership and suite coverage.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use yadb_io::OsDirectoryLock;

use crate::kv::{MetaSnapshot, MetaStateMachine};
use crate::multiraft::{GroupIdentity, ReplicaIdentity};
use crate::openraft_runtime::YadbRaftTypeConfig as C;
use crate::openraft_storage::{frame_payload, read_frame, Frame, LogCoverage, TailRepair};
use crate::{KvResult, MetaError, WatchEvent};

const MAGIC: &[u8; 8] = b"YORSM001";
const SNAPSHOT_MAGIC: &[u8; 8] = b"YORSNAP1";
const VERSION: u32 = 1;
const MAX_IMAGE: usize = 64 * 1024 * 1024;
const IMAGE_HEADER_LEN: usize = 24;

/// Applied entries appended after the last full-image checkpoint.
///
/// `state.json` is a checkpoint: publishing it writes the whole image, syncs
/// it, renames it over the previous one and syncs the directory. Measured on
/// this host's f2fs volume with eight concurrent writers, that sequence has a
/// 1.6 s tail (the rename alone reached 1.65 s) even though its median is 8 ms.
/// Paying it on every apply puts that tail between a committed entry and the
/// client's answer, because OpenRaft answers a `client_write` only after the
/// state machine has applied the entry.
///
/// The delta file holds the same entries as small appended frames, so the
/// steady state of `apply` is one append plus one file sync: the same load
/// measured a 25 ms maximum and no tail at all. The checkpoint is rewritten
/// only when the delta outgrows a bound, and whenever a snapshot is built.
const DELTA_FILE: &str = "state.delta";
/// Fold the delta into a fresh checkpoint once it holds this many entries.
const COMPACT_AFTER_ENTRIES: u64 = 4096;
/// ...or once it holds this many bytes. Both bounds are deliberately well above
/// one apply batch: folding costs one full-image publish, and that cost is only
/// amortised if it happens far less often than the applies it replaces.
const COMPACT_AFTER_BYTES: u64 = 8 * 1024 * 1024;

/// One delta frame: the entries of one `apply` call, in log order.
///
/// The frame is serialised by reference so appending never clones the batch.
#[derive(Serialize)]
#[serde(bound = "")]
struct DeltaFrameRef<'a> {
    version: u32,
    entries: &'a [Entry<C>],
}

#[derive(Deserialize)]
#[serde(bound = "")]
pub(crate) struct DeltaFrame {
    pub(crate) version: u32,
    pub(crate) entries: Vec<Entry<C>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
struct Image {
    version: u32,
    group: GroupIdentity,
    node_id: u32,
    applied: Option<LogId<u32>>,
    membership: StoredMembership<u32, BasicNode>,
    state: MetaSnapshot,
    current_snapshot: Option<SnapshotImage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
struct SnapshotImage {
    version: u32,
    group: GroupIdentity,
    applied: Option<LogId<u32>>,
    membership: StoredMembership<u32, BasicNode>,
    state: MetaSnapshot,
}

struct Inner {
    dir: PathBuf,
    _lock: OsDirectoryLock,
    group: GroupIdentity,
    node_id: u32,
    applied: Option<LogId<u32>>,
    membership: StoredMembership<u32, BasicNode>,
    state: MetaStateMachine,
    current_snapshot: Option<SnapshotImage>,
    rebuilt: Option<ImageRebuild>,
    /// Open handle for [`DELTA_FILE`], opened lazily on the first append and
    /// dropped whenever the delta is folded into a checkpoint.
    delta: Option<File>,
    /// Entries and bytes currently in [`DELTA_FILE`]. Both are advisory: they
    /// only decide when to fold, and the file itself stays authoritative.
    delta_entries: u64,
    delta_bytes: u64,
    /// A torn final delta frame discarded by the last open.
    repaired_delta: Option<TailRepair>,
    failed: bool,
    fault: Option<PersistFault>,
}

/// A corrupt `state.json` that [`DurableMetaStateMachine::open_or_rebuild`]
/// replaced with an empty image.
///
/// The original bytes are renamed aside, never truncated or overwritten, so an
/// operator can still read exactly what failed validation. The rebuild is
/// surfaced instead of logged because this module has no logging facility of
/// its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRebuild {
    /// Path the corrupt image was renamed to.
    pub preserved_as: PathBuf,
    /// Length of the corrupt image in bytes.
    pub corrupt_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(test), allow(dead_code))]
enum PersistFault {
    AfterWrite,
    AfterFileSync,
    AfterRename,
    AfterDirSync,
}

#[derive(Clone)]
pub struct DurableMetaStateMachine {
    inner: Arc<Mutex<Inner>>,
}

fn storage_error(
    subject: ErrorSubject<u32>,
    verb: ErrorVerb,
    error: io::Error,
) -> StorageError<u32> {
    StorageError::from_io_error(subject, verb, error)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn identity_header(identity: &ReplicaIdentity) -> (GroupIdentity, u32) {
    (identity.group().clone(), identity.node())
}

fn image_bytes(image: &Image) -> io::Result<Vec<u8>> {
    let payload = serde_json::to_vec(image).map_err(|e| invalid(e.to_string()))?;
    if payload.len() > MAX_IMAGE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "state-machine image is too large",
        ));
    }
    let len =
        u64::try_from(payload.len()).map_err(|_| invalid("state-machine image length overflow"))?;
    let mut bytes = Vec::with_capacity(MAGIC.len() + 8 + 4 + payload.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn snapshot_image_bytes(image: &SnapshotImage) -> io::Result<Vec<u8>> {
    let payload = serde_json::to_vec(image).map_err(|e| invalid(e.to_string()))?;
    if payload.len() > MAX_IMAGE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "snapshot image is too large",
        ));
    }
    let mut bytes = Vec::with_capacity(24 + payload.len());
    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn decode_image(bytes: &[u8]) -> io::Result<Image> {
    const HEADER: usize = 8 + 4 + 8 + 4;
    if bytes.len() < HEADER || &bytes[..8] != MAGIC {
        return Err(invalid("invalid state-machine image header"));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != VERSION {
        return Err(invalid("unsupported state-machine image version"));
    }
    let len = usize::try_from(u64::from_le_bytes(bytes[12..20].try_into().unwrap()))
        .map_err(|_| invalid("state-machine image length overflow"))?;
    if len > MAX_IMAGE
        || bytes.len()
            != HEADER
                .checked_add(len)
                .ok_or_else(|| invalid("image length overflow"))?
    {
        return Err(invalid("invalid state-machine image length"));
    }
    let payload = &bytes[HEADER..];
    let checksum = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    if crc32fast::hash(payload) != checksum {
        return Err(invalid("state-machine image checksum mismatch"));
    }
    let image: Image = serde_json::from_slice(payload).map_err(|e| invalid(e.to_string()))?;
    if image.version != VERSION {
        return Err(invalid("invalid state-machine envelope version"));
    }
    Ok(image)
}

fn decode_snapshot_image(bytes: &[u8]) -> io::Result<SnapshotImage> {
    const HEADER: usize = 8 + 4 + 8 + 4;
    if bytes.len() < HEADER || &bytes[..8] != SNAPSHOT_MAGIC {
        return Err(invalid("invalid snapshot image header"));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let len = usize::try_from(u64::from_le_bytes(bytes[12..20].try_into().unwrap()))
        .map_err(|_| invalid("snapshot image length overflow"))?;
    let end = HEADER
        .checked_add(len)
        .ok_or_else(|| invalid("snapshot image length overflow"))?;
    if version != VERSION || len > MAX_IMAGE || bytes.len() != end {
        return Err(invalid("invalid snapshot image length or version"));
    }
    let payload = &bytes[HEADER..];
    let checksum = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    if crc32fast::hash(payload) != checksum {
        return Err(invalid("snapshot image checksum mismatch"));
    }
    let image: SnapshotImage =
        serde_json::from_slice(payload).map_err(|e| invalid(e.to_string()))?;
    if image.version != VERSION {
        return Err(invalid("invalid snapshot envelope version"));
    }
    Ok(image)
}

fn read_image(file: &mut File) -> io::Result<Image> {
    let mut bytes = Vec::new();
    file.take((MAX_IMAGE + IMAGE_HEADER_LEN + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_IMAGE + IMAGE_HEADER_LEN {
        return Err(invalid("state-machine image exceeds limit"));
    }
    decode_image(&bytes)
}

fn image_from_inner(inner: &Inner, group: &GroupIdentity, node_id: u32) -> Image {
    Image {
        version: VERSION,
        group: group.clone(),
        node_id,
        applied: inner.applied,
        membership: inner.membership.clone(),
        state: inner.state.durable_snapshot(),
        current_snapshot: inner.current_snapshot.clone(),
    }
}

fn snapshot_image_from_inner(inner: &Inner) -> SnapshotImage {
    SnapshotImage {
        version: VERSION,
        group: inner.group.clone(),
        applied: inner.applied,
        membership: inner.membership.clone(),
        state: inner.state.durable_snapshot(),
    }
}

fn snapshot_id(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    let mut id = String::with_capacity(7 + 64);
    id.push_str("sha256-");
    for byte in digest.as_ref() {
        write!(&mut id, "{byte:02x}").expect("writing to a string cannot fail");
    }
    id
}

fn snapshot_from_image(image: SnapshotImage) -> io::Result<Snapshot<C>> {
    let bytes = snapshot_image_bytes(&image)?;
    let meta = SnapshotMeta {
        last_log_id: image.applied,
        last_membership: image.membership.clone(),
        snapshot_id: snapshot_id(&bytes),
    };
    Ok(Snapshot {
        meta,
        snapshot: Box::new(Cursor::new(bytes)),
    })
}

/// Apply one entry to a state machine, returning what a client would observe.
///
/// Both the live `apply` path and delta replay use this, so replay cannot drift
/// from the code that produced the durable record in the first place.
fn apply_entry(
    state: &mut MetaStateMachine,
    membership: &mut StoredMembership<u32, BasicNode>,
    entry: &Entry<C>,
) -> KvResult {
    match &entry.payload {
        EntryPayload::Normal(command) => state.apply(command.clone()),
        EntryPayload::Membership(updated) => {
            *membership = StoredMembership::new(Some(entry.log_id), updated.clone());
            KvResult::TxnOk {
                revision: state.revision(),
                versions: BTreeMap::new(),
            }
        }
        EntryPayload::Blank => KvResult::TxnOk {
            revision: state.revision(),
            versions: BTreeMap::new(),
        },
    }
}

/// Append one frame holding `entries` to [`DELTA_FILE`] and sync it.
///
/// The caller appends before it mutates its in-memory view, so a crash can
/// never leave memory ahead of the file. One frame per `apply` batch keeps
/// replay cheap and makes an interrupted append visible as a torn *final*
/// frame, the same way the Raft log store frames its records.
fn append_delta_locked(inner: &mut Inner, entries: &[Entry<C>]) -> io::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let payload = serde_json::to_vec(&DeltaFrameRef {
        version: VERSION,
        entries,
    })
    .map_err(|e| invalid(e.to_string()))?;
    let frame = frame_payload(payload)?;
    if inner.delta.is_none() {
        let path = inner.dir.join(DELTA_FILE);
        // The directory entry is synced once, when the file is first created
        // (after every fold). Paying it per apply is exactly the cost this
        // change removes; paying it once makes the delta self-contained rather
        // than relying on the Raft log to replay a file a crash never linked.
        let existed = path.exists();
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        if !existed {
            File::open(&inner.dir)?.sync_all()?;
        }
        inner.delta = Some(file);
    }
    // Only the two phases an append actually has are consumed here. The
    // rename and directory-sync faults belong to `persist_locked`, so they must
    // survive an apply for the checkpoint path to observe them.
    let fault = match inner.fault {
        Some(PersistFault::AfterWrite) | Some(PersistFault::AfterFileSync) => inner.fault.take(),
        _ => None,
    };
    let file = inner.delta.as_mut().expect("delta handle is open");
    let start_len = file.metadata()?.len();
    let result = (|| -> io::Result<()> {
        file.write_all(&frame)?;
        if matches!(fault, Some(PersistFault::AfterWrite)) {
            return Err(io::Error::other("simulated failure after write"));
        }
        file.sync_data()?;
        if matches!(fault, Some(PersistFault::AfterFileSync)) {
            return Err(io::Error::other("simulated failure after file sync"));
        }
        Ok(())
    })();
    if let Err(error) = result {
        // A failed append must leave no trace: a complete-but-unsynced frame
        // would otherwise be replayed after a restart and apply an entry this
        // call reported as failed. Truncating back to the previous end makes
        // the durable delta exactly what was acknowledged. If the truncation
        // itself fails the handle is poisoned anyway, and `replay_delta`
        // resolves a torn final frame on the next open.
        let _ = file.set_len(start_len);
        let _ = file.sync_data();
        return Err(error);
    }
    inner.delta_entries = inner.delta_entries.saturating_add(entries.len() as u64);
    inner.delta_bytes = inner.delta_bytes.saturating_add(frame.len() as u64);
    Ok(())
}

/// Replay the delta frames that follow the checkpoint's applied index.
///
/// Frames at or below `applied` are skipped rather than rejected: that is what
/// makes a failed fold harmless, because the checkpoint and the delta may both
/// cover the same entries for as long as the unlink has not landed.
///
/// Returns the replayed entry count, the valid length of the file and the torn
/// final frame that was truncated away, if any.
fn replay_delta(
    dir: &Path,
    state: &mut MetaStateMachine,
    applied: &mut Option<LogId<u32>>,
    membership: &mut StoredMembership<u32, BasicNode>,
) -> io::Result<(u64, u64, Option<TailRepair>)> {
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join(DELTA_FILE))
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((0, 0, None)),
        Err(error) => return Err(error),
    };
    let file_len = file.metadata()?.len();
    let mut replayed = 0u64;
    let mut valid_len = 0u64;
    loop {
        let payload = match read_frame(&mut file)? {
            Frame::End => break,
            Frame::Torn => {
                // `append_delta_locked` writes one whole frame per `write_all`
                // and syncs it before acknowledging, and the directory lock
                // admits a single writer, so a frame that runs past the end of
                // the file is that writer's final, unacknowledged append.
                // Truncating here cannot drop an acknowledged entry.
                file.set_len(valid_len)?;
                file.sync_data()?;
                return Ok((
                    replayed,
                    valid_len,
                    Some(TailRepair {
                        valid_len,
                        discarded: file_len.saturating_sub(valid_len),
                    }),
                ));
            }
            Frame::Complete { payload, .. } => payload,
        };
        let frame: DeltaFrame =
            serde_json::from_slice(&payload).map_err(|e| invalid(e.to_string()))?;
        if frame.version != VERSION {
            return Err(invalid("unsupported state-machine delta version"));
        }
        for entry in frame.entries {
            if applied.is_some_and(|id| entry.log_id.index <= id.index) {
                continue;
            }
            // The delta is a suffix of the log, so a missing frame is a gap in
            // the applied history. Applying past it would silently produce a
            // state the log never described; fail closed instead, exactly as
            // the live `apply` path does.
            if applied.is_some_and(|id| entry.log_id.index != id.index.saturating_add(1)) {
                return Err(invalid("state-machine delta has a gap"));
            }
            apply_entry(state, membership, &entry);
            *applied = Some(entry.log_id);
            replayed = replayed.saturating_add(1);
        }
        valid_len = file.stream_position()?;
    }
    Ok((replayed, valid_len, None))
}

/// Fold the delta into a fresh checkpoint and start an empty one.
///
/// The checkpoint is published before the delta is dropped, so a crash in
/// between leaves a delta whose frames the checkpoint already covers; replay
/// skips them. Dropping the file is therefore best-effort: failing to unlink it
/// only costs replay work on the next open, never correctness.
fn compact_locked(inner: &mut Inner) -> Result<(), StorageError<u32>> {
    persist_locked(inner)?;
    reset_delta_locked(inner);
    Ok(())
}

fn reset_delta_locked(inner: &mut Inner) {
    inner.delta = None;
    inner.delta_entries = 0;
    inner.delta_bytes = 0;
    match fs::remove_file(inner.dir.join(DELTA_FILE)) {
        Ok(()) => {
            let _ = File::open(&inner.dir).and_then(|dir| dir.sync_all());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {}
    }
}

fn persist_locked(inner: &mut Inner) -> Result<(), StorageError<u32>> {
    if inner.failed {
        return Err(storage_error(
            ErrorSubject::StateMachine,
            ErrorVerb::Write,
            io::Error::other("state machine requires recovery"),
        ));
    }
    let image = image_from_inner(inner, &inner.group, inner.node_id);
    // Serialization is the other way this function fails without touching the
    // disk, and it must poison too. The fold that called us cannot publish a
    // checkpoint, the next attempt fails the same way, and leaving `failed`
    // clear would repeat the fold's log line forever while applies kept
    // answering from an image that can never be written.
    let bytes = match image_bytes(&image) {
        Ok(bytes) => bytes,
        Err(error) => {
            inner.failed = true;
            return Err(storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                error,
            ));
        }
    };
    let temp = inner.dir.join("state.json.tmp");
    let path = inner.dir.join("state.json");
    let fault = inner.fault.take();
    let mut created = false;
    let result = (|| -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        created = true;
        file.write_all(&bytes)?;
        if matches!(fault, Some(PersistFault::AfterWrite)) {
            return Err(io::Error::other("simulated failure after write"));
        }
        file.sync_data()?;
        if matches!(fault, Some(PersistFault::AfterFileSync)) {
            return Err(io::Error::other("simulated failure after file sync"));
        }
        fs::rename(&temp, &path)?;
        if matches!(fault, Some(PersistFault::AfterRename)) {
            return Err(io::Error::other("simulated failure after rename"));
        }
        File::open(&inner.dir)?.sync_all()?;
        if matches!(fault, Some(PersistFault::AfterDirSync)) {
            return Err(io::Error::other("simulated failure after directory sync"));
        }
        Ok(())
    })();
    if let Err(error) = result {
        if created {
            let _ = fs::remove_file(&temp);
        }
        inner.failed = true;
        return Err(storage_error(
            ErrorSubject::StateMachine,
            ErrorVerb::Write,
            error,
        ));
    }
    Ok(())
}

fn validate_image(image: &Image, identity: &ReplicaIdentity) -> io::Result<()> {
    let (group, node_id) = identity_header(identity);
    if image.version != VERSION || image.group != group || image.node_id != node_id {
        return Err(invalid("state-machine identity mismatch"));
    }
    Ok(())
}

/// The image a fresh [`DurableMetaStateMachine::create`] writes and a rebuild
/// starts from: nothing applied, no membership, no state and no snapshot.
fn empty_image(identity: &ReplicaIdentity) -> Image {
    Image {
        version: VERSION,
        group: identity.group().clone(),
        node_id: identity.node(),
        applied: None,
        membership: StoredMembership::default(),
        state: MetaStateMachine::new().durable_snapshot(),
        current_snapshot: None,
    }
}

/// Rename an unreadable `state.json` aside, keeping every byte intact.
///
/// The image is never truncated or rewritten in place: its bytes are the only
/// evidence of which validation failed. `OsDirectoryLock` excludes concurrent
/// writers, so probing for a free name cannot race with another rebuild; an
/// earlier `state.json.corrupt` is never overwritten.
fn preserve_corrupt_image(dir: &Path, path: &Path) -> io::Result<ImageRebuild> {
    let corrupt_bytes = fs::metadata(path)?.len();
    let mut preserved_as = dir.join("state.json.corrupt");
    let mut sequence = 0u32;
    loop {
        match fs::symlink_metadata(&preserved_as) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Ok(_) => {
                sequence = sequence.saturating_add(1);
                if sequence > 1000 {
                    return Err(io::Error::other("too many preserved state-machine images"));
                }
                preserved_as = dir.join(format!("state.json.corrupt.{sequence}"));
            }
            Err(error) => return Err(error),
        }
    }
    fs::rename(path, &preserved_as)?;
    File::open(dir)?.sync_all()?;
    Ok(ImageRebuild {
        preserved_as,
        corrupt_bytes,
    })
}

/// Publish `image` as `state.json` through a synced temporary file, so a crash
/// leaves either the previous image or a complete new one under the final
/// name. A leftover `state.json.tmp` was never renamed into place, so it is not
/// evidence of a published image and is discarded first.
fn publish_image(dir: &Path, image: &Image) -> io::Result<()> {
    let bytes = image_bytes(image)?;
    let temp = dir.join("state.json.tmp");
    let path = dir.join("state.json");
    match fs::remove_file(&temp) {
        Ok(()) => File::open(dir)?.sync_all()?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let result = (|| -> io::Result<()> {
        file.write_all(&bytes)?;
        file.sync_data()?;
        fs::rename(&temp, &path)?;
        File::open(dir)?.sync_all()
    })();
    drop(file);
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Read `state.json`, or, when `rebuild_allowed`, replace an image that cannot
/// be decoded with a fresh empty one.
///
/// Only `InvalidData` from [`read_image`] counts as corruption. A missing file,
/// an I/O error, an identity mismatch and invalid nested snapshot metadata all
/// keep failing: rebuilding them would either create state for a directory that
/// never had any or adopt another replica's data. The returned file handle is
/// the published image, so the caller can re-sync it as `open` always has.
fn load_image(
    dir: &Path,
    identity: &ReplicaIdentity,
    rebuild_allowed: bool,
) -> io::Result<(File, Image, Option<ImageRebuild>)> {
    let path = dir.join("state.json");
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    let image = match read_image(&mut file) {
        Ok(image) => image,
        Err(error) if rebuild_allowed && error.kind() == io::ErrorKind::InvalidData => {
            drop(file);
            let rebuilt = preserve_corrupt_image(dir, &path)?;
            let image = empty_image(identity);
            publish_image(dir, &image)?;
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            return Ok((file, image, Some(rebuilt)));
        }
        Err(error) => return Err(error),
    };
    Ok((file, image, None))
}

impl DurableMetaStateMachine {
    /// Copy selected locally applied keys and prefix ranges at one revision.
    /// This does not perform a quorum or linearizable read barrier.
    pub fn try_snapshot(
        &self,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| MetaError::StorageUnavailable)?;
        if inner.failed {
            return Err(MetaError::StorageUnavailable);
        }
        let mut values = BTreeMap::new();
        for key in keys {
            if let Some(value) = inner.state.get(key) {
                values.insert(key.clone(), value);
            }
        }
        for prefix in prefixes {
            values.extend(inner.state.scan_prefix(prefix));
        }
        Ok(MetaSnapshot {
            revision: inner.state.revision(),
            values,
        })
    }

    /// Register a local watch, or fail if this state machine requires recovery.
    pub fn try_watch_prefix(
        &self,
        prefix: String,
    ) -> Result<std::sync::mpsc::Receiver<WatchEvent>, MetaError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| MetaError::StorageUnavailable)?;
        if inner.failed {
            return Err(MetaError::StorageUnavailable);
        }
        let (tx, rx) = std::sync::mpsc::channel();
        inner.state.watch_prefix(prefix, tx);
        Ok(rx)
    }

    pub fn create(dir: impl AsRef<Path>, identity: &ReplicaIdentity) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let lock = OsDirectoryLock::try_acquire(&dir)?;
        let path = dir.join("state.json");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        drop(file);
        let image = empty_image(identity);
        let bytes = image_bytes(&image)?;
        let mut file = OpenOptions::new().write(true).truncate(true).open(&path)?;
        file.write_all(&bytes)?;
        file.sync_data()?;
        File::open(&dir)?.sync_all()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                dir,
                _lock: lock,
                group: identity.group().clone(),
                node_id: identity.node(),
                applied: None,
                membership: image.membership,
                state: MetaStateMachine::new(),
                current_snapshot: None,
                rebuilt: None,
                delta: None,
                delta_entries: 0,
                delta_bytes: 0,
                repaired_delta: None,
                failed: false,
                fault: None,
            })),
        })
    }

    /// Open an existing state machine image, failing closed on any corruption.
    ///
    /// Only [`Self::open_or_rebuild`] may discard a corrupt image, and only when
    /// the caller knows the log still holds every entry.
    pub fn open(dir: impl AsRef<Path>, identity: &ReplicaIdentity) -> io::Result<Self> {
        Self::open_image(dir.as_ref(), identity, false)
    }

    /// Open an existing state machine, rebuilding it from an empty image when
    /// `state.json` is corrupt and the log can still replay from its start.
    ///
    /// `coverage` must describe the log that will replay into this state
    /// machine. Only when it [`LogCoverage::replays_from_start`] is the log a
    /// complete record of every entry, making an empty state machine plus
    /// replay equivalent to the lost image. A purge marker, a log that starts at
    /// a snapshot boundary, or an empty log all mean an empty state machine
    /// could never reproduce the applied state, so a corrupt image then fails
    /// closed exactly like [`Self::open`].
    ///
    /// The corrupt bytes are renamed to `state.json.corrupt` (or a numbered
    /// sibling) and reported through [`Self::rebuilt_image`]; they are never
    /// truncated or overwritten. The rebuilt image has `applied: None`, an empty
    /// membership and an empty state, never a partially decoded one.
    ///
    /// A corrupt image carries no readable identity, so the caller must already
    /// have established that this directory belongs to `identity`;
    /// `openraft_runtime` verifies `replica.json` before calling.
    pub fn open_or_rebuild(
        dir: impl AsRef<Path>,
        identity: &ReplicaIdentity,
        coverage: LogCoverage,
    ) -> io::Result<Self> {
        Self::open_image(dir.as_ref(), identity, coverage.replays_from_start())
    }

    /// Shared body of [`Self::open`] and [`Self::open_or_rebuild`]. Strict
    /// [`Self::open`] passes `rebuild_allowed = false`, so a corrupt image is
    /// only ever discarded by a caller that supplied log coverage proving a
    /// replay would be complete.
    fn open_image(
        dir: &Path,
        identity: &ReplicaIdentity,
        rebuild_allowed: bool,
    ) -> io::Result<Self> {
        let dir = dir.to_path_buf();
        let lock = OsDirectoryLock::try_acquire(&dir)?;
        let (file, image, rebuilt) = load_image(&dir, identity, rebuild_allowed)?;
        validate_image(&image, identity)?;
        if let Some(snapshot) = &image.current_snapshot {
            if snapshot.version != VERSION
                || snapshot.group != image.group
                || snapshot.applied.map_or(false, |id| {
                    image
                        .applied
                        .map_or(true, |applied| id.index > applied.index)
                })
            {
                return Err(invalid("invalid current snapshot metadata"));
            }
        }
        // A process-only restart can observe a renamed image whose previous
        // sync returned an error. Re-sync that exact file before treating the
        // recovered state as durable; a directory sync alone is insufficient.
        file.sync_data()?;
        // Inspect identity before changing anything in this directory.
        match fs::remove_file(dir.join("state.json.tmp")) {
            Ok(()) => File::open(&dir)?.sync_all()?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let mut state = MetaStateMachine::new();
        state.restore_durable_snapshot(image.state);
        if rebuilt.is_some() {
            // A rebuilt image is empty and the log replays from its start, so a
            // delta suffix must not be replayed on top: it holds only the last
            // few entries and would produce a state missing everything before
            // them.
            match fs::remove_file(dir.join(DELTA_FILE)) {
                Ok(()) => File::open(&dir)?.sync_all()?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        let mut applied = image.applied;
        let mut membership = image.membership;
        let (delta_entries, delta_bytes, repaired_delta) =
            replay_delta(&dir, &mut state, &mut applied, &mut membership)?;
        File::open(&dir)?.sync_all()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                dir,
                _lock: lock,
                group: identity.group().clone(),
                node_id: identity.node(),
                applied,
                membership,
                state,
                current_snapshot: image.current_snapshot,
                rebuilt,
                delta: None,
                delta_entries,
                delta_bytes,
                repaired_delta,
                failed: false,
                fault: None,
            })),
        })
    }

    /// The corrupt image discarded by the last [`Self::open_or_rebuild`], if
    /// any.
    ///
    /// The original bytes are preserved verbatim under
    /// [`ImageRebuild::preserved_as`]. A later open sees the rebuilt image and
    /// reports `None`, so a rebuild is reported once per discard.
    pub fn rebuilt_image(&self) -> Option<ImageRebuild> {
        self.inner.lock().unwrap().rebuilt.clone()
    }

    /// A torn final delta frame that the last open truncated away, if any.
    ///
    /// The frame was never acknowledged, so nothing was lost; the report exists
    /// because the truncation itself is silent and an operator must be able to
    /// tell a clean start from a recovered one.
    pub fn repaired_delta(&self) -> Option<TailRepair> {
        self.inner.lock().unwrap().repaired_delta
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> MetaSnapshot {
        self.inner.lock().unwrap().state.durable_snapshot()
    }

    /// Test-only: put this handle into the state a failed durable apply leaves
    /// it in. Read paths must fail closed in that state rather than panic or
    /// report an absent key.
    #[cfg(test)]
    pub(crate) fn poison_for_test(&self) {
        self.inner.lock().unwrap().failed = true;
    }
}

impl RaftSnapshotBuilder<C> for DurableMetaStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<u32>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                io::Error::other("state machine requires recovery"),
            ));
        }
        let snapshot = snapshot_image_from_inner(&inner);
        let result = snapshot_from_image(snapshot.clone())
            .map_err(|e| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        let previous = inner.current_snapshot.clone();
        inner.current_snapshot = Some(snapshot.clone());
        if let Err(error) = persist_locked(&mut inner) {
            inner.current_snapshot = previous;
            return Err(error);
        }
        // The checkpoint now covers every entry the delta holds.
        reset_delta_locked(&mut inner);
        Ok(result)
    }
}

impl RaftStateMachine<C> for DurableMetaStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u32>>, StoredMembership<u32, BasicNode>), StorageError<u32>> {
        let inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                io::Error::other("state machine requires recovery"),
            ));
        }
        Ok((inner.applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<KvResult>, StorageError<u32>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                io::Error::other("state machine requires recovery"),
            ));
        }
        let mut expected = inner.applied.map(|id| id.index.saturating_add(1));
        for entry in &entries {
            if expected.is_some_and(|index| entry.log_id.index != index) {
                return Err(storage_error(
                    ErrorSubject::Apply(entry.log_id),
                    ErrorVerb::Write,
                    invalid("state-machine apply is not consecutive"),
                ));
            }
            expected = Some(entry.log_id.index.saturating_add(1));
        }
        let mut candidate_state = inner.state.clone();
        candidate_state.defer_notifications();
        let mut candidate_membership = inner.membership.clone();
        let mut candidate_applied = inner.applied;
        let mut results = Vec::new();
        // Durability first: the frame must be on disk before any of it becomes
        // visible in memory or to a watcher.
        if let Err(error) = append_delta_locked(&mut inner, &entries) {
            inner.failed = true;
            return Err(storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                error,
            ));
        }
        for entry in &entries {
            candidate_applied = Some(entry.log_id);
            results.push(apply_entry(
                &mut candidate_state,
                &mut candidate_membership,
                entry,
            ));
        }
        let events = candidate_state.take_deferred_notifications();
        inner.applied = candidate_applied;
        inner.membership = candidate_membership;
        inner.state = candidate_state;
        inner.state.deliver_notifications(events);
        if inner.delta_entries >= COMPACT_AFTER_ENTRIES || inner.delta_bytes >= COMPACT_AFTER_BYTES
        {
            // The entries are already durable in the delta, so a failed fold is
            // not a failed apply: `persist_locked` poisons the handle and the
            // next operation fails closed, while this call still reports what
            // it durably applied. It must still be reported here, because the
            // handle is poisoned only for *later* calls: without this line a
            // node keeps answering from a state machine whose checkpoint could
            // not be published, and the only symptom is an error on some later,
            // unrelated request. The handle is poisoned, so this is logged at
            // most once per failed state machine.
            if let Err(error) = compact_locked(&mut inner) {
                eprintln!(
                    "openraft state machine group={} node={} checkpoint fold failed: {error}; \
                     the applied entries remain durable in the delta and this replica now requires recovery",
                    inner.group.group, inner.node_id
                );
            }
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u32>> {
        if self.inner.lock().unwrap().failed {
            return Err(storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                io::Error::other("state machine requires recovery"),
            ));
        }
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u32, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u32>> {
        let bytes = snapshot.into_inner();
        if bytes.len() > MAX_IMAGE + IMAGE_HEADER_LEN {
            return Err(storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                invalid("snapshot is too large"),
            ));
        }
        let image = decode_snapshot_image(&bytes)
            .map_err(|e| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        let mut inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                io::Error::other("state machine requires recovery"),
            ));
        }
        if image.group != inner.group
            || image.applied != meta.last_log_id
            || image.membership != meta.last_membership
            || snapshot_id(&bytes) != meta.snapshot_id
        {
            return Err(storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                invalid("snapshot metadata or identity mismatch"),
            ));
        }
        if let Some(applied) = inner.applied {
            match image.applied {
                Some(incoming) if incoming.index > applied.index || incoming == applied => {}
                _ => {
                    return Err(storage_error(
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Write,
                        invalid("snapshot would regress or conflict with applied state"),
                    ))
                }
            }
        }
        if image.applied == inner.applied {
            let current = inner.state.durable_snapshot();
            if image.membership != inner.membership
                || image.state.revision != current.revision
                || image.state.values != current.values
            {
                return Err(storage_error(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Write,
                    invalid("snapshot conflicts with state at the same applied log"),
                ));
            }
        }
        let old_applied = inner.applied;
        let old_membership = inner.membership.clone();
        let old_state = inner.state.clone();
        let old_snapshot = inner.current_snapshot.clone();
        // Snapshot installation cannot reproduce command history. Notify the
        // net key/value delta, using each installed value's own revision and
        // the snapshot revision for deletions, only after durable publication.
        let previous = inner.state.durable_snapshot();
        let mut events = Vec::new();
        for (key, value) in &image.state.values {
            if previous.values.get(key) != Some(value) {
                events.push(WatchEvent::Put {
                    key: key.clone(),
                    value: value.value.clone(),
                    version: value.version,
                    revision: value.mod_revision,
                });
            }
        }
        for key in previous.values.keys() {
            if !image.state.values.contains_key(key) {
                events.push(WatchEvent::Delete {
                    key: key.clone(),
                    revision: image.state.revision,
                });
            }
        }
        inner.applied = image.applied;
        inner.membership = image.membership.clone();
        inner.state.replace_durable_snapshot(image.state.clone());
        inner.current_snapshot = Some(image.clone());
        if let Err(error) = persist_locked(&mut inner) {
            inner.applied = old_applied;
            inner.membership = old_membership;
            inner.state = old_state;
            inner.current_snapshot = old_snapshot;
            return Err(error);
        }
        // The installed snapshot is authoritative up to its own applied index,
        // so every delta frame is either covered by it or obsolete.
        reset_delta_locked(&mut inner);
        inner.state.deliver_notifications(events);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<u32>> {
        let inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                io::Error::other("state machine requires recovery"),
            ));
        }
        match inner.current_snapshot.clone() {
            Some(image) => snapshot_from_image(image)
                .map(Some)
                .map_err(|e| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::KvCommand;
    use openraft::{CommittedLeaderId, EntryPayload};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            loop {
                let nonce = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let path = std::env::temp_dir().join(format!(
                    "yadb-openraft-sm-{nonce}-{}-{sequence}",
                    std::process::id()
                ));
                match fs::create_dir(&path) {
                    Ok(()) => return Self(path),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => panic!("create test directory {}: {error}", path.display()),
                }
            }
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn identity() -> ReplicaIdentity {
        ReplicaIdentity::new(
            GroupIdentity {
                cluster: [9; 16],
                group: 4,
                incarnation: 2,
            },
            1,
            vec![1],
        )
        .unwrap()
    }

    fn put(index: u64, key: &str, value: &[u8]) -> Entry<C> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(KvCommand::Put {
                key: key.into(),
                value: value.to_vec(),
            }),
        }
    }

    #[tokio::test]
    async fn applied_state_and_kv_survive_restart_without_watchers() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"one")]).await.unwrap();
        assert_eq!(sm.snapshot().get("a").unwrap().value, b"one");
        drop(sm);

        let mut reopened = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.applied_state().await.unwrap().0.unwrap().index, 1);
        assert_eq!(reopened.snapshot().get("a").unwrap().version, 1);
        assert_eq!(reopened.inner.lock().unwrap().state.watcher_count(), 0);
    }

    #[tokio::test]
    async fn apply_appends_to_the_delta_instead_of_republishing_the_checkpoint() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        let checkpoint = fs::read(dir.0.join("state.json")).unwrap();
        sm.apply(vec![put(1, "a", b"one")]).await.unwrap();
        sm.apply(vec![put(2, "b", b"two")]).await.unwrap();
        // The whole point of the delta: an apply must not rewrite the
        // checkpoint. Before this change every batch published a new full
        // image, and its rename plus directory sync was measured at up to
        // 1.6s under concurrent writers on this filesystem.
        assert_eq!(fs::read(dir.0.join("state.json")).unwrap(), checkpoint);
        assert!(!fs::read(dir.0.join(DELTA_FILE)).unwrap().is_empty());
        assert_eq!(sm.snapshot().get("a").unwrap().value, b"one");
        assert_eq!(sm.snapshot().get("b").unwrap().value, b"two");
        drop(sm);
        let mut reopened = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.snapshot().get("a").unwrap().value, b"one");
        assert_eq!(reopened.snapshot().get("b").unwrap().value, b"two");
        assert_eq!(reopened.applied_state().await.unwrap().0.unwrap().index, 2);
        assert!(reopened.repaired_delta().is_none());
    }

    #[tokio::test]
    async fn torn_delta_tail_is_truncated_and_reported() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"one")]).await.unwrap();
        sm.apply(vec![put(2, "b", b"two")]).await.unwrap();
        let valid = fs::metadata(dir.0.join(DELTA_FILE)).unwrap().len();
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(dir.0.join(DELTA_FILE))
                .unwrap();
            // A complete, checksum-valid header whose payload never reached
            // disk: exactly the final unacknowledged append of a crash.
            let mut torn = Vec::new();
            torn.extend_from_slice(b"YORF");
            torn.extend_from_slice(&64u32.to_le_bytes());
            torn.extend_from_slice(&0u32.to_le_bytes());
            let header_crc = crc32fast::hash(&torn);
            torn.extend_from_slice(&header_crc.to_le_bytes());
            file.write_all(&torn).unwrap();
            file.sync_data().unwrap();
        }
        drop(sm);
        let mut reopened = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.snapshot().get("a").unwrap().value, b"one");
        assert_eq!(reopened.snapshot().get("b").unwrap().value, b"two");
        assert_eq!(reopened.applied_state().await.unwrap().0.unwrap().index, 2);
        let repair = reopened.repaired_delta().expect("torn delta reported");
        assert_eq!(repair.valid_len, valid);
        assert_eq!(repair.discarded, 16);
        assert_eq!(fs::metadata(dir.0.join(DELTA_FILE)).unwrap().len(), valid);
    }

    #[tokio::test]
    async fn a_delta_over_the_bound_is_folded_into_the_checkpoint() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        let entries: Vec<_> = (1..=COMPACT_AFTER_ENTRIES)
            .map(|index| put(index, "key", b"value"))
            .collect();
        sm.apply(entries).await.unwrap();
        // The fold published a checkpoint covering every entry and removed the
        // delta, so the next apply starts a fresh one.
        assert!(!dir.0.join(DELTA_FILE).exists());
        drop(sm);
        let mut reopened = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            reopened.applied_state().await.unwrap().0.unwrap().index,
            COMPACT_AFTER_ENTRIES
        );
        assert_eq!(reopened.snapshot().get("key").unwrap().value, b"value");
    }

    #[tokio::test]
    async fn selected_snapshot_is_owned_and_rejects_failed_state_machine() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![
            put(1, "a", b"one"),
            put(2, "prefix/x", b"two"),
            put(3, "other", b"three"),
        ])
        .await
        .unwrap();
        let snapshot = sm
            .try_snapshot(&["a".into(), "missing".into()], &["prefix/".into()])
            .unwrap();
        assert_eq!(snapshot.revision(), 3);
        assert_eq!(snapshot.get("a").unwrap().value, b"one");
        assert_eq!(snapshot.get("prefix/x").unwrap().value, b"two");
        assert!(snapshot.get("other").is_none());
        assert!(snapshot.get("missing").is_none());
        sm.apply(vec![put(4, "a", b"new")]).await.unwrap();
        assert_eq!(snapshot.get("a").unwrap().value, b"one");
        sm.inner.lock().unwrap().fault = Some(PersistFault::AfterWrite);
        assert!(sm.apply(vec![put(5, "a", b"failed")]).await.is_err());
        assert!(matches!(
            sm.try_snapshot(&[], &[]),
            Err(MetaError::StorageUnavailable)
        ));
        drop(sm);
        let sm = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            sm.try_snapshot(&["a".into()], &[])
                .unwrap()
                .get("a")
                .unwrap()
                .value,
            b"new"
        );
    }

    #[tokio::test]
    async fn watch_registration_delivers_durable_apply_and_rejects_failed_handle() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        let rx = sm.try_watch_prefix("a/".into()).unwrap();
        sm.apply(vec![put(1, "a/key", b"one"), put(2, "other", b"two")])
            .await
            .unwrap();
        assert!(
            matches!(rx.try_recv().unwrap(), WatchEvent::Put { key, value, .. } if key == "a/key" && value == b"one")
        );
        assert!(rx.try_recv().is_err());
        sm.inner.lock().unwrap().fault = Some(PersistFault::AfterWrite);
        assert!(sm.apply(vec![put(3, "a/key", b"failed")]).await.is_err());
        assert!(rx.try_recv().is_err());
        assert!(matches!(
            sm.try_watch_prefix("a/".into()),
            Err(MetaError::StorageUnavailable)
        ));
    }

    #[tokio::test]
    async fn snapshot_build_and_install_are_atomic_and_metadata_bound() {
        let source_dir = TempDir::new();
        let mut source = DurableMetaStateMachine::create(&source_dir.0, &identity()).unwrap();
        source.apply(vec![put(1, "a", b"one")]).await.unwrap();
        let snapshot = source
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();

        let target_dir = TempDir::new();
        let mut target = DurableMetaStateMachine::create(&target_dir.0, &identity()).unwrap();
        target.begin_receiving_snapshot().await.unwrap();
        let mut received = target.begin_receiving_snapshot().await.unwrap();
        received
            .write_all(&snapshot.snapshot.clone().into_inner())
            .unwrap();
        let meta = snapshot.meta.clone();
        target.install_snapshot(&meta, received).await.unwrap();
        assert_eq!(target.snapshot().get("a").unwrap().value, b"one");

        let before = fs::read(target_dir.0.join("state.json")).unwrap();
        let wrong = SnapshotMeta {
            snapshot_id: meta.snapshot_id.clone(),
            last_log_id: None,
            last_membership: meta.last_membership.clone(),
        };
        let mut bytes = source
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap()
            .snapshot
            .into_inner();
        assert!(target
            .install_snapshot(&wrong, Box::new(Cursor::new(std::mem::take(&mut bytes))))
            .await
            .is_err());
        assert_eq!(fs::read(target_dir.0.join("state.json")).unwrap(), before);
    }

    #[tokio::test]
    async fn snapshot_can_install_on_another_replica_of_same_group() {
        let source_dir = TempDir::new();
        let source_identity = identity();
        let mut source = DurableMetaStateMachine::create(&source_dir.0, &source_identity).unwrap();
        source
            .apply(vec![put(1, "portable", b"yes")])
            .await
            .unwrap();
        let snapshot = source
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let bytes = snapshot.snapshot.into_inner();

        let target_identity =
            ReplicaIdentity::new(source_identity.group().clone(), 2, vec![1, 2]).unwrap();
        let target_dir = TempDir::new();
        let mut target = DurableMetaStateMachine::create(&target_dir.0, &target_identity).unwrap();
        target
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes)))
            .await
            .unwrap();
        assert_eq!(target.snapshot().get("portable").unwrap().value, b"yes");
        drop(target);
        let reopened = DurableMetaStateMachine::open(&target_dir.0, &target_identity).unwrap();
        assert_eq!(reopened.snapshot().get("portable").unwrap().value, b"yes");
    }

    #[tokio::test]
    async fn failed_apply_does_not_notify_watchers_and_poisoned_handle_fails_closed() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        sm.inner.lock().unwrap().state.watch_prefix("", tx);
        // The apply path now appends to the delta file instead of publishing
        // `state.json`, so a stale `state.json.tmp` no longer reaches it. Fail
        // the append at its write phase instead; this is still an in-process
        // simulation, not a power-loss or real filesystem fault.
        sm.inner.lock().unwrap().fault = Some(PersistFault::AfterWrite);
        assert!(sm.apply(vec![put(1, "key", b"value")]).await.is_err());
        assert!(rx.try_recv().is_err());
        assert!(sm.applied_state().await.is_err());
        drop(sm);
        let reopened = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert!(reopened.snapshot().get("key").is_none());
        assert!(reopened.inner.lock().unwrap().applied.is_none());
    }

    #[tokio::test]
    async fn snapshot_install_simulated_failures_require_reopen() {
        let source_dir = TempDir::new();
        let mut source = DurableMetaStateMachine::create(&source_dir.0, &identity()).unwrap();
        source
            .apply(vec![put(1, "key", b"old"), put(2, "key", b"new")])
            .await
            .unwrap();
        let snapshot = source
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let bytes = snapshot.snapshot.into_inner();

        for (fault, expected_value, expected_index) in [
            (PersistFault::AfterWrite, &b"old"[..], 1),
            (PersistFault::AfterFileSync, &b"old"[..], 1),
            (PersistFault::AfterRename, &b"new"[..], 2),
            (PersistFault::AfterDirSync, &b"new"[..], 2),
        ] {
            let dir = TempDir::new();
            let mut target = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
            target.apply(vec![put(1, "key", b"old")]).await.unwrap();
            target.inner.lock().unwrap().fault = Some(fault);
            assert!(target
                .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes.clone())))
                .await
                .is_err());
            assert!(target.applied_state().await.is_err());
            assert!(target.get_current_snapshot().await.is_err());
            drop(target);

            let mut reopened = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
            assert_eq!(
                reopened.snapshot().get("key").unwrap().value,
                expected_value
            );
            assert_eq!(
                reopened.applied_state().await.unwrap().0.unwrap().index,
                expected_index
            );
            assert_eq!(
                reopened.get_current_snapshot().await.unwrap().is_some(),
                expected_index == 2
            );
        }
    }

    #[tokio::test]
    async fn snapshot_install_notifies_delta_only_after_durable_success() {
        let source_dir = TempDir::new();
        let mut source = DurableMetaStateMachine::create(&source_dir.0, &identity()).unwrap();
        source
            .apply(vec![
                put(1, "control", b"old"),
                put(2, "revoked", b"yes"),
                put(3, "control", b"new"),
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                    payload: EntryPayload::Normal(KvCommand::Delete {
                        key: "revoked".into(),
                    }),
                },
            ])
            .await
            .unwrap();
        let snapshot = source
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let bytes = snapshot.snapshot.into_inner();

        for fail in [true, false] {
            let dir = TempDir::new();
            let mut target = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
            target
                .apply(vec![put(1, "control", b"old"), put(2, "revoked", b"yes")])
                .await
                .unwrap();
            let rx = target.try_watch_prefix("".into()).unwrap();
            if fail {
                target.inner.lock().unwrap().fault = Some(PersistFault::AfterWrite);
            }
            let result = target
                .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes.clone())))
                .await;
            if fail {
                assert!(result.is_err());
                assert!(rx.try_recv().is_err());
            } else {
                result.unwrap();
                assert_eq!(
                    rx.try_recv().unwrap(),
                    WatchEvent::Put {
                        key: "control".into(),
                        value: b"new".to_vec(),
                        version: 2,
                        revision: 3
                    }
                );
                assert_eq!(
                    rx.try_recv().unwrap(),
                    WatchEvent::Delete {
                        key: "revoked".into(),
                        revision: 4
                    }
                );
                assert!(rx.try_recv().is_err());
                target
                    .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes.clone())))
                    .await
                    .unwrap();
                assert!(rx.try_recv().is_err());
                target
                    .apply(vec![put(5, "control", b"later")])
                    .await
                    .unwrap();
                assert!(
                    matches!(rx.try_recv().unwrap(), WatchEvent::Put { key, value, .. } if key == "control" && value == b"later")
                );
            }
        }
    }

    /// A failed installation must deliver no delta at all, and the refusal must
    /// poison the handle rather than leave it able to notify a watcher from a
    /// state whose durability was never proved. `snapshot_install_notifies_
    /// delta_only_after_durable_success` already checks the two outcomes on
    /// separate targets and that a *successful* install keeps the subscription;
    /// this is the install-path counterpart of
    /// `failed_apply_does_not_notify_watchers_and_poisoned_handle_fails_closed`,
    /// which covers the apply path only.
    ///
    /// After the validated reopen the watcher set is empty by design (watchers
    /// are runtime-only), and a fresh subscription must still receive later
    /// applies: that is the "retain subscriptions for later applies" clause in
    /// the only form that survives a poisoning.
    #[tokio::test]
    async fn a_failed_install_delivers_nothing_and_poisons_before_any_later_notification() {
        let source_dir = TempDir::new();
        let mut source = DurableMetaStateMachine::create(&source_dir.0, &identity()).unwrap();
        source
            .apply(vec![
                put(1, "control", b"installed"),
                put(2, "extra", b"only-in-snapshot"),
            ])
            .await
            .unwrap();
        let snapshot = source
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let bytes = snapshot.snapshot.into_inner();

        let dir = TempDir::new();
        let mut target = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        target.apply(vec![put(1, "control", b"before")]).await.unwrap();
        let rx = target.try_watch_prefix("".into()).unwrap();

        target.inner.lock().unwrap().fault = Some(PersistFault::AfterWrite);
        assert!(
            target
                .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes.clone())))
                .await
                .is_err(),
            "the injected fault must refuse the installation"
        );
        assert!(
            rx.try_recv().is_err(),
            "a refused installation must not deliver a delta"
        );
        // The refusal is not "retry in place": the handle is poisoned, so no
        // later apply can feed the subscription from a state whose durability
        // was never established.
        assert!(
            target.applied_state().await.is_err(),
            "a failed installation must poison the handle"
        );
        assert!(
            target.apply(vec![put(3, "control", b"poisoned")]).await.is_err(),
            "a poisoned handle must refuse a later apply, not notify from it"
        );
        assert!(
            rx.try_recv().is_err(),
            "a poisoned handle must not deliver the refused apply either"
        );
        drop(target);

        let mut reopened = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            reopened.inner.lock().unwrap().state.watcher_count(),
            0,
            "watchers are runtime-only and must not survive a reopen"
        );
        assert_eq!(
            reopened.snapshot().get("control").unwrap().value,
            b"before",
            "the refused installation must not have become durable"
        );
        assert!(reopened.snapshot().get("extra").is_none());

        let rx = reopened.try_watch_prefix("".into()).unwrap();
        reopened
            .apply(vec![put(2, "control", b"after reopen")])
            .await
            .unwrap();
        assert!(
            matches!(rx.try_recv().unwrap(), WatchEvent::Put { key, value, .. } if key == "control" && value == b"after reopen"),
            "a subscription taken after the reopen must receive later applies"
        );
    }

    #[tokio::test]
    async fn failed_apply_does_not_publish_and_recovery_accepts_unknown_outcome() {
        // An apply has two durability phases: the frame write and the file
        // sync. A failure at either must not leave an entry behind for a
        // restart to replay. The checkpoint's rename and directory-sync phases
        // are not part of `apply` any more; `snapshot_install_simulated_
        // failures_require_reopen` still covers them through `persist_locked`.
        for fault in [PersistFault::AfterWrite, PersistFault::AfterFileSync] {
            let dir = TempDir::new();
            let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            sm.inner.lock().unwrap().state.watch_prefix("", tx);
            sm.inner.lock().unwrap().fault = Some(fault);
            assert!(sm.apply(vec![put(1, "key", b"value")]).await.is_err());
            assert!(rx.try_recv().is_err());
            assert!(sm.applied_state().await.is_err());
            assert!(sm.begin_receiving_snapshot().await.is_err());
            let handle = Arc::downgrade(&sm.inner);
            drop(sm);
            assert!(
                handle.upgrade().is_none(),
                "state-machine handle retained for {}",
                dir.0.display()
            );
            let mut reopened = DurableMetaStateMachine::open(&dir.0, &identity())
                .unwrap_or_else(|error| panic!("reopen after fault {fault:?}: {error:?}"));
            assert!(reopened.snapshot().get("key").is_none());
            assert!(reopened.applied_state().await.unwrap().0.is_none());
            assert_eq!(reopened.inner.lock().unwrap().state.watcher_count(), 0);
        }
    }

    #[tokio::test]
    async fn current_snapshot_remains_unchanged_after_later_apply_and_reopen() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        assert!(sm.get_current_snapshot().await.unwrap().is_none());
        sm.apply(vec![put(1, "a", b"first")]).await.unwrap();
        let built = sm
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        sm.apply(vec![put(2, "a", b"second")]).await.unwrap();
        let current = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta, built.meta);
        assert_eq!(current.snapshot.into_inner(), built.snapshot.into_inner());
        drop(sm);
        let mut reopened = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.snapshot().get("a").unwrap().value, b"second");
        assert_eq!(
            reopened
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .meta
                .last_log_id
                .unwrap()
                .index,
            1
        );
    }

    #[tokio::test]
    async fn stale_snapshot_install_cannot_roll_back_applied_state() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"old")]).await.unwrap();
        let old = sm
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        sm.apply(vec![put(2, "a", b"new")]).await.unwrap();
        let before = fs::read(dir.0.join("state.json")).unwrap();
        assert!(sm.install_snapshot(&old.meta, old.snapshot).await.is_err());
        assert_eq!(sm.snapshot().get("a").unwrap().value, b"new");
        assert_eq!(sm.applied_state().await.unwrap().0.unwrap().index, 2);
        assert_eq!(fs::read(dir.0.join("state.json")).unwrap(), before);
    }

    #[tokio::test]
    async fn snapshot_id_binds_bytes_not_just_last_applied_log() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"one")]).await.unwrap();
        let snapshot = sm
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let mut altered = decode_snapshot_image(&snapshot.snapshot.into_inner()).unwrap();
        altered.state = MetaStateMachine::new().durable_snapshot();
        let altered_bytes = snapshot_image_bytes(&altered).unwrap();
        assert_ne!(snapshot.meta.snapshot_id, snapshot_id(&altered_bytes));
        let before = fs::read(dir.0.join("state.json")).unwrap();
        assert!(sm
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(altered_bytes)))
            .await
            .is_err());
        assert_eq!(fs::read(dir.0.join("state.json")).unwrap(), before);
    }

    #[tokio::test]
    async fn same_applied_snapshot_requires_identical_state_and_membership() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"one")]).await.unwrap();
        let snap = sm
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let original = snap.snapshot.into_inner();
        let before = fs::read(dir.0.join("state.json")).unwrap();
        let mut altered = decode_snapshot_image(&original).unwrap();
        altered.state = MetaStateMachine::new().durable_snapshot();
        let bytes = snapshot_image_bytes(&altered).unwrap();
        let mut meta = snap.meta.clone();
        meta.snapshot_id = snapshot_id(&bytes);
        assert!(sm
            .install_snapshot(&meta, Box::new(Cursor::new(bytes)))
            .await
            .is_err());
        let mut altered = decode_snapshot_image(&original).unwrap();
        altered.membership = StoredMembership::new(
            altered.applied,
            openraft::Membership::new(vec![[1u32, 2].into_iter().collect()], None),
        );
        let bytes = snapshot_image_bytes(&altered).unwrap();
        let mut membership_meta = snap.meta.clone();
        membership_meta.last_membership = altered.membership;
        membership_meta.snapshot_id = snapshot_id(&bytes);
        assert!(sm
            .install_snapshot(&membership_meta, Box::new(Cursor::new(bytes)))
            .await
            .is_err());
        let mut altered = decode_snapshot_image(&original).unwrap();
        altered.applied = Some(LogId::new(CommittedLeaderId::new(2, 1), 1));
        let bytes = snapshot_image_bytes(&altered).unwrap();
        meta.last_log_id = altered.applied;
        meta.snapshot_id = snapshot_id(&bytes);
        assert!(sm
            .install_snapshot(&meta, Box::new(Cursor::new(bytes)))
            .await
            .is_err());
        assert_eq!(fs::read(dir.0.join("state.json")).unwrap(), before);
        assert_eq!(sm.snapshot().get("a").unwrap().value, b"one");
        sm.install_snapshot(&snap.meta, Box::new(Cursor::new(original)))
            .await
            .unwrap();
        drop(sm);
        let sm = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(sm.snapshot().get("a").unwrap().value, b"one");
    }

    #[test]
    fn corrupted_image_is_rejected_without_reinitialization() {
        let dir = TempDir::new();
        let sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        drop(sm);
        let path = dir.0.join("state.json");
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(DurableMetaStateMachine::open(&dir.0, &identity()).is_err());
    }

    /// Flip one bit in `state.json`'s payload, which the header CRC covers, so
    /// the image fails validation the way bit rot would. Returns the corrupted
    /// bytes as they were left on disk.
    fn corrupt_image(dir: &Path) -> Vec<u8> {
        let path = dir.join("state.json");
        let mut bytes = fs::read(&path).unwrap();
        let offset = IMAGE_HEADER_LEN + 1;
        assert!(offset < bytes.len(), "image has no payload byte to corrupt");
        bytes[offset] ^= 1;
        fs::write(&path, &bytes).unwrap();
        bytes
    }

    // The whole point of the finding: an unpurged log is a complete record, so
    // a corrupt image costs a replay, not the replica. The corrupt bytes stay
    // on disk because they are the only evidence of what failed validation.
    #[tokio::test]
    async fn corrupt_image_is_rebuilt_when_the_log_replays_from_the_start() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"one"), put(2, "b", b"two")])
            .await
            .unwrap();
        drop(sm);
        let corrupt = corrupt_image(&dir.0);

        let mut rebuilt = DurableMetaStateMachine::open_or_rebuild(
            &dir.0,
            &identity(),
            LogCoverage {
                last_purged: None,
                first_index: Some(0),
            },
        )
        .unwrap();
        let evidence = dir.0.join("state.json.corrupt");
        assert_eq!(
            rebuilt.rebuilt_image(),
            Some(ImageRebuild {
                preserved_as: evidence.clone(),
                corrupt_bytes: corrupt.len() as u64,
            })
        );
        assert_eq!(fs::read(&evidence).unwrap(), corrupt);
        // The rebuild is a genuinely empty state, not a partial decode.
        let (applied, membership) = rebuilt.applied_state().await.unwrap();
        assert_eq!(applied, None);
        assert_eq!(membership, StoredMembership::default());
        assert_eq!(rebuilt.snapshot().revision(), 0);
        assert!(rebuilt.snapshot().get("a").is_none());
        assert!(rebuilt.get_current_snapshot().await.unwrap().is_none());
        drop(rebuilt);

        // The rebuilt image is valid, so a second open reports no rebuild and
        // still starts empty: openraft replays the log from its first index.
        let mut reopened = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.rebuilt_image(), None);
        assert_eq!(reopened.applied_state().await.unwrap().0, None);
        reopened
            .apply(vec![put(1, "a", b"one"), put(2, "b", b"two")])
            .await
            .unwrap();
        assert_eq!(reopened.snapshot().get("a").unwrap().value, b"one");
        assert_eq!(reopened.snapshot().get("b").unwrap().value, b"two");
        drop(reopened);

        let mut final_open = DurableMetaStateMachine::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            final_open.applied_state().await.unwrap().0.unwrap().index,
            2
        );
        assert_eq!(final_open.snapshot().get("a").unwrap().value, b"one");
        // Nothing rewrote or truncated the evidence.
        assert_eq!(fs::read(&evidence).unwrap(), corrupt);
    }

    // A purge marker means the log is not a complete record: an empty state
    // machine plus replay would silently lose the purged prefix, so the
    // original fail-closed behaviour is the only safe answer.
    #[tokio::test]
    async fn corrupt_image_with_a_purged_log_still_fails_closed() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"one"), put(2, "b", b"two")])
            .await
            .unwrap();
        drop(sm);
        let corrupt = corrupt_image(&dir.0);
        let path = dir.0.join("state.json");

        let error = DurableMetaStateMachine::open_or_rebuild(
            &dir.0,
            &identity(),
            LogCoverage {
                last_purged: Some(LogId::new(CommittedLeaderId::new(1, 1), 2)),
                first_index: Some(3),
            },
        )
        .err()
        .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(&path).unwrap(), corrupt);
        assert!(!dir.0.join("state.json.corrupt").exists());
        // Strict open is unchanged, and still refuses the same bytes.
        assert!(DurableMetaStateMachine::open(&dir.0, &identity()).is_err());
        assert_eq!(fs::read(&path).unwrap(), corrupt);
    }

    // The audit's fix keys on the purge marker alone, but this log accepts a
    // fresh snapshot-installed replica whose entries begin at the snapshot
    // boundary with no purge marker (`prepare_append`). Replaying such a log
    // into an empty state machine would start mid-history and lose the prefix,
    // so coverage must include where the retained log begins.
    #[tokio::test]
    async fn unpurged_log_that_starts_past_the_first_index_is_not_rebuilt() {
        for first_index in [Some(1), Some(3), None] {
            let dir = TempDir::new();
            let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
            sm.apply(vec![put(1, "a", b"one")]).await.unwrap();
            drop(sm);
            let corrupt = corrupt_image(&dir.0);
            let path = dir.0.join("state.json");

            assert!(DurableMetaStateMachine::open_or_rebuild(
                &dir.0,
                &identity(),
                LogCoverage {
                    last_purged: None,
                    first_index,
                },
            )
            .is_err());
            assert_eq!(fs::read(&path).unwrap(), corrupt);
            assert!(!dir.0.join("state.json.corrupt").exists());
        }
    }

    #[test]
    fn missing_image_is_not_rebuilt() {
        let dir = TempDir::new();
        let sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        drop(sm);
        fs::remove_file(dir.0.join("state.json")).unwrap();
        let error = DurableMetaStateMachine::open_or_rebuild(
            &dir.0,
            &identity(),
            LogCoverage {
                last_purged: None,
                first_index: Some(0),
            },
        )
        .err()
        .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(!dir.0.join("state.json").exists());
        assert!(!dir.0.join("state.json.corrupt").exists());
    }

    #[tokio::test]
    async fn valid_image_is_never_rebuilt() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"one")]).await.unwrap();
        drop(sm);
        let before = fs::read(dir.0.join("state.json")).unwrap();

        // Even a coverage that would permit a rebuild leaves a valid image
        // alone: the file must not be rewritten or renamed.
        let mut reopened = DurableMetaStateMachine::open_or_rebuild(
            &dir.0,
            &identity(),
            LogCoverage {
                last_purged: None,
                first_index: Some(0),
            },
        )
        .unwrap();
        assert_eq!(reopened.rebuilt_image(), None);
        assert_eq!(reopened.applied_state().await.unwrap().0.unwrap().index, 1);
        assert_eq!(reopened.snapshot().get("a").unwrap().value, b"one");
        drop(reopened);
        assert_eq!(fs::read(dir.0.join("state.json")).unwrap(), before);
        assert!(!dir.0.join("state.json.corrupt").exists());
    }

    // Evidence must survive a second incident: the earlier bytes are never
    // overwritten by a later rebuild.
    #[tokio::test]
    async fn a_second_rebuild_preserves_the_first_evidence() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"one")]).await.unwrap();
        drop(sm);
        let first = corrupt_image(&dir.0);
        let coverage = LogCoverage {
            last_purged: None,
            first_index: Some(0),
        };
        let rebuilt =
            DurableMetaStateMachine::open_or_rebuild(&dir.0, &identity(), coverage).unwrap();
        let first_evidence = rebuilt.rebuilt_image().unwrap().preserved_as;
        assert_eq!(first_evidence, dir.0.join("state.json.corrupt"));
        drop(rebuilt);

        let second = corrupt_image(&dir.0);
        let rebuilt =
            DurableMetaStateMachine::open_or_rebuild(&dir.0, &identity(), coverage).unwrap();
        let report = rebuilt.rebuilt_image().unwrap();
        assert_eq!(report.preserved_as, dir.0.join("state.json.corrupt.1"));
        assert_eq!(fs::read(&report.preserved_as).unwrap(), second);
        assert_eq!(fs::read(&first_evidence).unwrap(), first);
    }

    #[tokio::test]
    async fn invalid_nested_snapshot_does_not_remove_recovery_files() {
        let dir = TempDir::new();
        let mut sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        sm.apply(vec![put(1, "a", b"one")]).await.unwrap();
        sm.get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        drop(sm);
        let path = dir.0.join("state.json");
        let mut image = read_image(&mut File::open(&path).unwrap()).unwrap();
        image.current_snapshot.as_mut().unwrap().version += 1;
        let bytes = image_bytes(&image).unwrap();
        fs::write(&path, &bytes).unwrap();
        let temp = dir.0.join("state.json.tmp");
        fs::write(&temp, b"recovery evidence").unwrap();
        assert!(DurableMetaStateMachine::open(&dir.0, &identity()).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(fs::read(&temp).unwrap(), b"recovery evidence");
    }

    #[test]
    fn oversized_state_image_is_rejected_without_modifying_files() {
        let dir = TempDir::new();
        let sm = DurableMetaStateMachine::create(&dir.0, &identity()).unwrap();
        drop(sm);
        let path = dir.0.join("state.json");
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len((MAX_IMAGE + IMAGE_HEADER_LEN + 1) as u64)
            .unwrap();
        drop(file);
        let temp = dir.0.join("state.json.tmp");
        fs::write(&temp, b"recovery evidence").unwrap();
        assert!(DurableMetaStateMachine::open(&dir.0, &identity()).is_err());
        assert_eq!(
            fs::metadata(path).unwrap().len(),
            (MAX_IMAGE + IMAGE_HEADER_LEN + 1) as u64
        );
        assert_eq!(fs::read(temp).unwrap(), b"recovery evidence");
    }
}
