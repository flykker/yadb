//! Opt-in file-backed OpenRaft log and vote store.
//!
//! Not suitable for production: group identity, recovery, failure handling and
//! the OpenRaft storage contracts need further work and testing.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use yadb_io::OsDirectoryLock;

use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{Entry, ErrorSubject, ErrorVerb, LogId, LogState, StorageError, Vote};
use serde::{Deserialize, Serialize};

use crate::multiraft::ReplicaIdentity;
use crate::openraft_runtime::YadbRaftTypeConfig as C;

// Version 02 used suffix-replacing append; version 03 lacks checkpoints.
// Neither format is migrated or reinterpreted by this store.
// Version 05 makes the frame header self-checking: 04 carried a payload CRC but
// an unprotected length, so a length damaged upward was byte-identical to a torn
// tail and got repaired by truncating away every acknowledged frame after it.
const MAGIC: &[u8; 8] = b"YORLOG05";
const IDENTITY_LEN: usize = 44;
const FRAME_MAGIC: &[u8; 4] = b"YORF";
/// `magic`, `len`, `payload_crc` and `header_crc` over the first three.
const FRAME_HEADER_LEN: usize = 16;
/// Bytes of the header covered by its own checksum.
const FRAME_HEADER_CHECKED_LEN: usize = 12;
const MAX_FRAME_PAYLOAD: usize = 16 * 1024 * 1024;
// A checkpoint must fit a single frame. Physical log and replay are bounded
// independently; temporary replacement can briefly double disk usage.
const MAX_CHECKPOINT_PAYLOAD: usize = 8 * 1024 * 1024;
const MAX_LIVE_ENTRIES: usize = 4096;
const MAX_LOG_BYTES: u64 = 32 * 1024 * 1024;
const MAX_LOG_FRAMES: usize = 1024;
const CHECKPOINT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
enum Record {
    Checkpoint {
        version: u32,
        vote: Option<Vote<u32>>,
        purged: Option<LogId<u32>>,
        entries: Vec<Entry<C>>,
    },
    Vote(Vote<u32>),
    /// Append entries; conflicting existing entries require an explicit truncate.
    Append(Vec<Entry<C>>),
    Truncate(LogId<u32>),
    Purge(LogId<u32>),
}

#[derive(Debug, Default)]
struct State {
    log: BTreeMap<u64, Entry<C>>,
    vote: Option<Vote<u32>>,
    purged: Option<LogId<u32>>,
}

#[derive(Clone)]
pub struct DurableOpenRaftLog {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    file: File,
    dir: PathBuf,
    header: Vec<u8>,
    bytes: u64,
    frames: usize,
    _lock: OsDirectoryLock,
    state: State,
    repaired_tail: Option<TailRepair>,
    failed: bool,
    fault: Option<Fault>,
    /// Test-only rendezvous at the publication boundary of [`append_durable`].
    #[cfg(test)]
    gate: Option<AppendGate>,
}

/// Test-only rendezvous inside [`DurableOpenRaftLog::append_durable`].
///
/// The writer signals `reached` after the frame has been written and synced but
/// before the new state is published to readers, then blocks on `release`
/// while still holding the inner lock. A test can therefore show that a
/// concurrent reader cannot observe the log between those two points, which is
/// the serialisation the no-torn guarantee rests on.
#[cfg(test)]
struct AppendGate {
    reached: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

/// A torn final frame that `open` discarded.
///
/// `append_frame` writes a whole frame and syncs it before OpenRaft is told the
/// append succeeded, so a frame cut short by a crash was never acknowledged and
/// can be dropped. The repair is surfaced instead of logged so callers can
/// report it: this module has no logging facility of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailRepair {
    /// Length the log was truncated to: the end of the last complete frame.
    pub valid_len: u64,
    /// Bytes discarded from the cut-short final frame.
    pub discarded: u64,
}

/// The first log index OpenRaft can ever assign.
///
/// `LogIdOptionExt::next_index()` of `None` is 0 in openraft 0.9, so a group's
/// very first entry is at index 0. This constant is what makes "the log starts
/// at the beginning" checkable; it would have to change if OpenRaft started
/// numbering a group elsewhere.
const FIRST_LOG_INDEX: u64 = 0;

/// What the log still retains, as needed to decide whether an empty state
/// machine can be rebuilt from it.
///
/// Replaying the log into an empty state machine reproduces the previous state
/// only if the log is a complete record of every entry. Two things can break
/// that: a purge marker drops a prefix on purpose, and a snapshot install lets
/// a fresh log begin at the snapshot boundary with no purge marker at all
/// (`prepare_append` documents that case). Both are reported here so the
/// decision does not depend on the purge marker alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogCoverage {
    /// `last_purged_log_id`, i.e. the highest entry the log has dropped.
    pub last_purged: Option<LogId<u32>>,
    /// Lowest retained entry index, or `None` when the log holds no entries.
    pub first_index: Option<u64>,
}

impl LogCoverage {
    /// Whether the retained log holds every entry from the first index, so
    /// replaying it into an empty state machine loses nothing.
    ///
    /// An empty log is not enough: the corrupt image may have covered a
    /// snapshot that the log no longer repeats.
    pub fn replays_from_start(&self) -> bool {
        self.last_purged.is_none() && self.first_index == Some(FIRST_LOG_INDEX)
    }
}

/// One in-process failure at a named syscall, for tests that assert the durable
/// contract rather than only that an operation returned an error.
///
/// Every variant names the operation and the syscall that fails, because what a
/// reopen must see depends on exactly how far the record got: nothing written,
/// a prefix written, a complete but unsynced frame, or a complete synced frame
/// whose publication (`rename`, directory sync) did not finish. A `Fault` is
/// consumed by the call it is set for, so a test cannot accidentally inject the
/// same failure twice.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// `write_all` of an appended frame fails before any byte reaches the file.
    WriteFails,
    /// `write_all` of an appended frame fails after half of it was written.
    WritePartial,
    /// The frame is complete in the file but its `sync_data` fails. The bytes
    /// may or may not reach the platter, but they form a whole frame.
    SyncBefore,
    /// The frame is written and synced, and then the operation reports failure.
    SyncAfter,

    /// Checkpoint: writing the checkpoint body to the temporary file fails
    /// before any of it is written (the copied header is already there).
    CheckpointWriteFails,
    /// Checkpoint: writing the checkpoint body fails after half of it.
    CheckpointWritePartial,
    /// Checkpoint: the temporary file is complete but its `sync_data` fails.
    CheckpointSync,
    /// Checkpoint: `rename` fails, so the active log is still the old file and
    /// the complete temporary file is left unpublished beside it.
    CheckpointRename,
    /// Checkpoint: `rename` succeeded but the directory sync fails, so the new
    /// name may not survive a crash.
    CheckpointDirSync,
    /// Checkpoint: `rename` and the directory sync succeeded, and then the
    /// operation reports failure before the append handle is rebound.
    CheckpointAfterDirSync,

    /// Bootstrap: writing the temporary header fails before any byte is written.
    BootstrapWriteFails,
    /// Bootstrap: the temporary header write fails after half of it.
    BootstrapWritePartial,
    /// Bootstrap: the temporary header is complete but its `sync_data` fails.
    BootstrapSync,
    /// Bootstrap: `rename` of the temporary file fails, so nothing is published.
    BootstrapRename,
    /// Bootstrap: `rename` succeeded but the directory sync fails.
    BootstrapDirSync,
    /// Bootstrap: syncing the replica directory's parent fails, after
    /// `create_dir_all` created the directory but before any log is published.
    BootstrapParentSync,
    /// Bootstrap: syncing the parent of a directory created between an existing
    /// ancestor and the replica directory fails. Only a nested `create_dir_all`
    /// reaches this sync; a single-level path never does.
    BootstrapAncestorSync,

    /// Reopen: the post-recovery `sync_data` fails.
    OpenSync,
    /// Reopen: the post-recovery directory sync fails.
    OpenDirSync,
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

/// Make the directory entry that names `dir` in its parent durable.
///
/// `create_dir_all` only changes the parent directory in memory; syncing the
/// parent is what makes the new entry survive a crash. Without this a log
/// created inside a fresh directory can be durable while the directory that
/// contains it is not.
fn sync_parent_dir(dir: &Path, fault: Option<Fault>) -> io::Result<()> {
    let Some(parent) = dir.parent() else {
        return Ok(());
    };
    // A relative single-component path has the empty path as its parent, which
    // is the current directory.
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    if matches!(fault, Some(Fault::BootstrapParentSync)) {
        return Err(io::Error::other(
            "simulated bootstrap parent-directory sync failure",
        ));
    }
    if matches!(fault, Some(Fault::BootstrapAncestorSync)) {
        return Err(io::Error::other(
            "simulated bootstrap ancestor-directory sync failure",
        ));
    }
    File::open(parent)?.sync_all()
}

/// Create `dir` and make every directory entry this call creates durable.
///
/// A nested path whose parents did not exist makes `create_dir_all` create more
/// than one level. Syncing only the leaf's immediate parent leaves each new
/// intermediate directory's own entry unsynced in *its* parent, so a crash can
/// make the whole chain unreachable even though the leaf's entry is durable.
/// Every directory created here therefore has its parent synced before a handle
/// is returned, and the immediate parent is synced even when `dir` already
/// existed so a replica directory is never handed out without that barrier.
fn create_dir_all_durable(dir: &Path, fault: Option<Fault>) -> io::Result<()> {
    // Record what `create_dir_all` is about to create before it exists; after
    // the call there is no way to tell a new directory from a pre-existing one.
    let mut created = Vec::new();
    for ancestor in dir.ancestors() {
        if ancestor.as_os_str().is_empty() || ancestor.exists() {
            break;
        }
        created.push(ancestor.to_path_buf());
    }
    fs::create_dir_all(dir)?;
    let parent_fault =
        matches!(fault, Some(Fault::BootstrapParentSync)).then_some(Fault::BootstrapParentSync);
    sync_parent_dir(dir, parent_fault)?;
    let ancestor_fault =
        matches!(fault, Some(Fault::BootstrapAncestorSync)).then_some(Fault::BootstrapAncestorSync);
    // `created` starts with `dir` itself, whose entry the sync above covered.
    // Each remaining entry is an intermediate directory that must itself become
    // reachable through its own parent.
    for path in created.iter().skip(1) {
        sync_parent_dir(path, ancestor_fault)?;
    }
    Ok(())
}

struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "serialized OpenRaft image exceeds limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialize_bounded(value: &impl Serialize, limit: usize) -> io::Result<Vec<u8>> {
    let mut buffer = LimitedBuffer {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut buffer, value).map_err(io::Error::other)?;
    Ok(buffer.bytes)
}

fn encode(record: &Record) -> io::Result<Vec<u8>> {
    let payload = serialize_bounded(record, MAX_FRAME_PAYLOAD)?;
    frame_payload(payload)
}

pub(crate) fn frame_payload(payload: Vec<u8>) -> io::Result<Vec<u8>> {
    if payload.len() > MAX_FRAME_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "OpenRaft record exceeds frame limit",
        ));
    }
    let len = u32::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "OpenRaft record length overflow",
        )
    })?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(FRAME_MAGIC);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    // Covers magic, len and payload_crc. Without it a damaged `len` is
    // indistinguishable from a frame that never finished reaching disk, and the
    // only safe reading of that ambiguity is the one that loses data.
    frame.extend_from_slice(&crc32fast::hash(&frame[..FRAME_HEADER_CHECKED_LEN]).to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn checkpoint_frame(state: &State) -> io::Result<Vec<u8>> {
    #[derive(Serialize)]
    enum Checkpoint<'a> {
        Checkpoint {
            version: u32,
            vote: Option<Vote<u32>>,
            purged: Option<LogId<u32>>,
            entries: Vec<&'a Entry<C>>,
        },
    }
    if state.log.len() > MAX_LIVE_ENTRIES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "OpenRaft live entry limit exceeded",
        ));
    }
    let checkpoint = Checkpoint::Checkpoint {
        version: CHECKPOINT_VERSION,
        vote: state.vote,
        purged: state.purged,
        entries: state.log.values().collect(),
    };
    let payload = serialize_bounded(&checkpoint, MAX_CHECKPOINT_PAYLOAD)?;
    frame_payload(payload)
}

fn compact(inner: &mut Inner, checkpoint: &[u8], fault: Option<Fault>) -> io::Result<()> {
    let temp = inner.dir.join("openraft.log.tmp");
    let active = inner.dir.join("openraft.log");
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(&temp)?;
    file.write_all(&inner.header)?;
    if matches!(fault, Some(Fault::CheckpointWriteFails)) {
        return Err(io::Error::other("simulated checkpoint write failure"));
    }
    if matches!(fault, Some(Fault::CheckpointWritePartial)) {
        file.write_all(&checkpoint[..checkpoint.len() / 2])?;
        return Err(io::Error::other("simulated partial checkpoint write"));
    }
    file.write_all(checkpoint)?;
    if matches!(fault, Some(Fault::CheckpointSync)) {
        return Err(io::Error::other("simulated checkpoint sync failure"));
    }
    file.sync_data()?;
    // Explicitly rebind the append handle to the active path after replacement.
    // POSIX rename does not invalidate the temporary file descriptor.
    drop(file);
    if matches!(fault, Some(Fault::CheckpointRename)) {
        return Err(io::Error::other("simulated checkpoint rename failure"));
    }
    fs::rename(&temp, &active)?;
    if matches!(fault, Some(Fault::CheckpointDirSync)) {
        return Err(io::Error::other(
            "simulated checkpoint directory sync failure",
        ));
    }
    File::open(&inner.dir)?.sync_all()?;
    if matches!(fault, Some(Fault::CheckpointAfterDirSync)) {
        return Err(io::Error::other(
            "simulated failure after checkpoint directory sync",
        ));
    }
    inner.file = OpenOptions::new().read(true).append(true).open(&active)?;
    inner.bytes = (inner.header.len() + checkpoint.len()) as u64;
    inner.frames = 1;
    Ok(())
}

fn append_frame(file: &mut File, record: &Record) -> io::Result<()> {
    let frame = encode(record)?;
    file.seek(SeekFrom::End(0))?;
    file.write_all(&frame)?;
    file.sync_data()
}

fn validate_entries(entries: &[Entry<C>]) -> io::Result<()> {
    for pair in entries.windows(2) {
        if pair[0].log_id.index.checked_add(1) != Some(pair[1].log_id.index) {
            return Err(invalid("OpenRaft append contains a log-index hole"));
        }
    }
    Ok(())
}

fn prepare_append(state: &State, entries: Vec<Entry<C>>) -> io::Result<Vec<Entry<C>>> {
    validate_entries(&entries)?;
    if entries.iter().any(|entry| {
        state
            .purged
            .is_some_and(|id| entry.log_id.index <= id.index)
    }) {
        return Err(invalid("cannot append purged logs"));
    }
    for entry in &entries {
        if let Some(existing) = state.log.get(&entry.log_id.index) {
            if serialize_bounded(existing, MAX_FRAME_PAYLOAD)?
                != serialize_bounded(entry, MAX_FRAME_PAYLOAD)?
            {
                return Err(invalid(
                    "conflicting log entry requires truncate before append",
                ));
            }
        }
    }
    let Some(first) = entries.first() else {
        return Ok(entries);
    };
    if let Some((&first_existing, _)) = state.log.first_key_value() {
        if first.log_id.index < first_existing
            && entries.last().unwrap().log_id.index < first_existing.saturating_sub(1)
        {
            return Err(invalid(
                "OpenRaft append would leave a hole before existing logs",
            ));
        }
    }
    // A fresh, unpurged log may start at any index: after installing a
    // snapshot, entries before the snapshot boundary live in the state
    // machine, not in the log. Once the log has entries or a purge marker,
    // a batch that skips indexes would leave a real hole.
    let expected = state
        .log
        .last_key_value()
        .map(|(_, entry)| entry.log_id.index.saturating_add(1))
        .or_else(|| state.purged.map(|id| id.index.saturating_add(1)));
    if expected.is_some_and(|expected| first.log_id.index > expected) {
        return Err(invalid("OpenRaft append would leave a log-index hole"));
    }
    Ok(entries)
}

fn apply_record(state: &mut State, record: Record) -> io::Result<()> {
    match record {
        Record::Checkpoint {
            version,
            vote,
            purged,
            entries,
        } => {
            if version != CHECKPOINT_VERSION
                || state.vote.is_some()
                || state.purged.is_some()
                || !state.log.is_empty()
            {
                return Err(invalid("invalid checkpoint position or version"));
            }
            if entries.len() > MAX_LIVE_ENTRIES {
                return Err(invalid("checkpoint contains too many entries"));
            }
            validate_entries(&entries)?;
            if let (Some(boundary), Some(first)) = (purged, entries.first()) {
                if boundary.index.checked_add(1) != Some(first.log_id.index) {
                    return Err(invalid("checkpoint has a hole after purge"));
                }
            }
            state.vote = vote;
            state.purged = purged;
            for entry in entries {
                state.log.insert(entry.log_id.index, entry);
            }
        }
        Record::Vote(vote) => {
            if state.vote.is_some_and(|old| {
                vote.partial_cmp(&old)
                    .map_or(true, |ordering| ordering.is_lt())
            }) {
                return Err(invalid("vote cannot move backwards"));
            }
            state.vote = Some(vote);
        }
        Record::Append(entries) => {
            let entries = prepare_append(state, entries)?;
            for entry in entries {
                state.log.insert(entry.log_id.index, entry);
            }
        }
        Record::Truncate(log_id) => {
            if state.purged.is_some_and(|id| log_id.index <= id.index) {
                return Err(invalid("cannot truncate purged logs"));
            }
            state.log.retain(|index, _| *index < log_id.index);
        }
        Record::Purge(log_id) => {
            if state.purged.is_some_and(|id| {
                log_id.index < id.index || (log_id.index == id.index && id != log_id)
            }) {
                return Err(invalid("purge cannot move backwards or change identity"));
            }
            if state
                .log
                .get(&log_id.index)
                .is_some_and(|entry| entry.log_id != log_id)
            {
                return Err(invalid("purge conflicts with stored log identity"));
            }
            // A fresh log may have an unknown prefix, but once a purge marker
            // is recorded it is the predecessor of every retained suffix.
            // Reject a marker that would expose a hole, as required by the
            // OpenRaft purge contract and by checkpoint replay.
            if let Some((&first, _)) = state.log.first_key_value() {
                if log_id.index < first && log_id.index.checked_add(1) != Some(first) {
                    return Err(invalid("purge would leave a hole before existing logs"));
                }
            }
            state.log.retain(|index, _| *index > log_id.index);
            state.purged = Some(log_id);
        }
    }
    Ok(())
}

fn header(identity: &ReplicaIdentity) -> Vec<u8> {
    let mut result = Vec::with_capacity(IDENTITY_LEN + 8);
    result.extend_from_slice(MAGIC);
    result.extend_from_slice(&identity.group().cluster);
    result.extend_from_slice(&identity.group().group.to_le_bytes());
    result.extend_from_slice(&identity.group().incarnation.to_le_bytes());
    result.extend_from_slice(&identity.node().to_le_bytes());
    result.extend_from_slice(&crc32fast::hash(&result).to_le_bytes());
    result
}

/// One step of a log walk.
pub(crate) enum Frame {
    /// The log ends exactly on a frame boundary.
    End,
    /// The file ends inside its final frame.
    Torn,
    Complete {
        header: [u8; FRAME_HEADER_LEN],
        payload: Vec<u8>,
    },
}

/// Fill `buf`, reporting a short read instead of failing.
///
/// Only a short read is meaningful here: the caller decides whether the missing
/// bytes are a crash artefact or corruption.
fn read_full_or_eof(file: &mut File, buf: &mut [u8]) -> io::Result<bool> {
    match file.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

/// Read the next frame without interpreting it.
///
/// A frame is reported [`Frame::Torn`] only when the file ends in the middle of
/// it. A complete header is validated before its length is used: bad magic or a
/// header checksum mismatch is an error, because those bytes were synced before
/// being acknowledged and discarding them would lose committed entries. The
/// header checksum is what makes a damaged `len` detectable, so the only way to
/// reach a length past EOF is a payload that genuinely never reached disk.
pub(crate) fn read_frame(file: &mut File) -> io::Result<Frame> {
    let mut frame_header = [0; FRAME_HEADER_LEN];
    loop {
        match file.read(&mut frame_header[..1]) {
            Ok(0) => return Ok(Frame::End),
            Ok(_) => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    // The file does not end on a frame boundary: at least one byte of the next
    // frame is present. `append_frame` writes one whole frame per `write_all`
    // and syncs it before acknowledging, and the log has a single writer behind
    // `OsDirectoryLock`, so a frame that runs past the end of the file can only
    // be that writer's final, unacknowledged append interrupted by a crash.
    // Nothing follows it, so truncating to the frame start loses nothing.
    if !read_full_or_eof(file, &mut frame_header[1..])? {
        return Ok(Frame::Torn);
    }
    if &frame_header[..4] != FRAME_MAGIC {
        return Err(invalid("invalid OpenRaft log frame"));
    }
    let stored_header_crc = u32::from_le_bytes(frame_header[12..16].try_into().unwrap());
    if crc32fast::hash(&frame_header[..FRAME_HEADER_CHECKED_LEN]) != stored_header_crc {
        // The framing bytes damaged themselves. A frame cut short by a torn
        // write leaves fewer than a full header and is handled above, so this
        // cannot be a tail: report it instead of discarding the frames after it.
        return Err(invalid("OpenRaft log frame header checksum mismatch"));
    }
    let len = u32::from_le_bytes(frame_header[4..8].try_into().unwrap()) as usize;
    if len > MAX_FRAME_PAYLOAD {
        return Err(invalid("OpenRaft log frame exceeds limit"));
    }
    let mut payload = vec![0; len];
    if !read_full_or_eof(file, &mut payload)? {
        // The header proved the length, so the payload simply never reached the
        // file: the final, unacknowledged append.
        return Ok(Frame::Torn);
    }
    Ok(Frame::Complete {
        header: frame_header,
        payload,
    })
}

struct Recovered {
    state: State,
    bytes: u64,
    frames: usize,
    repaired_tail: Option<TailRepair>,
}

fn recover(file: &mut File, dir: &Path, identity: &ReplicaIdentity) -> io::Result<Recovered> {
    let file_len = file.metadata()?.len();
    if file_len > MAX_LOG_BYTES {
        return Err(invalid("OpenRaft log exceeds disk limit"));
    }
    file.seek(SeekFrom::Start(0))?;
    let expected = header(identity);
    let mut actual = vec![0; expected.len()];
    file.read_exact(&mut actual).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            // `create` writes the header once and syncs it before the log is
            // usable, so a file that cannot hold it never was a valid log.
            // Repairing it would invent a replica identity.
            invalid("truncated OpenRaft log header")
        } else {
            e
        }
    })?;
    if actual != expected {
        return Err(invalid("invalid OpenRaft log header or replica identity"));
    }

    let mut state = State::default();
    let mut frames = 0;
    // Offset of the next frame, and therefore the length of the longest fully
    // valid prefix seen so far.
    let mut valid_len = expected.len() as u64;
    let mut repaired_tail = None;
    loop {
        let (frame_header, payload) = match read_frame(file)? {
            Frame::End => break,
            Frame::Torn => {
                // Drop the torn tail and make the shorter file durable before
                // serving reads, the same way `compact` and `create` publish a
                // new length. Truncation cannot remove an acknowledged frame:
                // the cut-short frame was never acknowledged, and every earlier
                // frame was fully written and synced before its acknowledgement.
                // Batching several frames per sync would not weaken this, since
                // a partial write still only leaves a partial *final* frame.
                // The argument would fail if a second writer bypassed
                // `OsDirectoryLock`, or if appends could overwrite or skip
                // bytes of the log instead of extending it; `compact` does
                // neither, because it publishes a synced temporary file with
                // `rename`. The frame header is self-checking, so a length
                // damaged upward is caught as corruption rather than repaired as
                // a tail: reaching this branch means the header was intact and
                // the declared payload genuinely does not exist.
                file.set_len(valid_len)?;
                file.sync_data()?;
                File::open(dir)?.sync_all()?;
                repaired_tail = Some(TailRepair {
                    valid_len,
                    discarded: file_len - valid_len,
                });
                break;
            }
            Frame::Complete {
                header: frame_header,
                payload,
            } => (frame_header, payload),
        };
        frames += 1;
        if frames > MAX_LOG_FRAMES {
            return Err(invalid("OpenRaft log exceeds frame count limit"));
        }
        let len = payload.len();
        let expected_crc = u32::from_le_bytes(frame_header[8..12].try_into().unwrap());
        if crc32fast::hash(&payload) != expected_crc {
            return Err(invalid("OpenRaft log checksum mismatch"));
        }
        let record: Record =
            serde_json::from_slice(&payload).map_err(|e| invalid(e.to_string()))?;
        if let Record::Checkpoint { .. } = &record {
            if frames != 1 || len > MAX_CHECKPOINT_PAYLOAD {
                return Err(invalid("invalid checkpoint frame"));
            }
        }
        apply_record(&mut state, record)?;
        if state.log.len() > MAX_LIVE_ENTRIES {
            return Err(invalid("OpenRaft log exceeds live entry limit"));
        }
        valid_len += (FRAME_HEADER_LEN + len) as u64;
    }
    file.seek(SeekFrom::End(0))?;
    Ok(Recovered {
        state,
        bytes: valid_len,
        frames,
        repaired_tail,
    })
}

impl DurableOpenRaftLog {
    /// Explicitly create a new test log. Existing files are never overwritten.
    pub fn create(dir: impl AsRef<Path>, identity: &ReplicaIdentity) -> io::Result<Self> {
        Self::create_with_fault(dir, identity, None)
    }

    /// Create with an optional bootstrap failure injection.
    ///
    /// The bootstrap is ordered so that a failure before the rename publishes
    /// nothing that `open` could accept: it leaves at most a temporary file,
    /// which `open` ignores and a retry truncates. A failure after the rename
    /// (the directory sync) leaves a complete log whose name is not yet proven
    /// durable; `open` accepts it and re-syncs the directory, so recovery from
    /// that outcome is a reopen, not a retry of `create`.
    ///
    /// Directory durability comes first: every directory this call creates has
    /// its parent synced, including intermediates of a nested path, before the
    /// header is written. The header is then written and synced, renamed into
    /// place, and the directory that holds it is synced before the append handle
    /// is opened.
    fn create_with_fault(
        dir: impl AsRef<Path>,
        identity: &ReplicaIdentity,
        fault: Option<Fault>,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        // The directory chain must itself be durable before a log inside it can
        // be acknowledged; otherwise a crash can leave the log while the
        // directory entries that reach it are gone.
        create_dir_all_durable(&dir, fault)?;
        let lock = OsDirectoryLock::try_acquire(&dir)?;
        let path = dir.join("openraft.log");
        if path.try_exists()? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "OpenRaft log already exists",
            ));
        }
        // Publish the header with a rename so a failure cannot leave a
        // headerless or partially written `openraft.log` behind. A temporary
        // left by an earlier failed bootstrap is truncated and rewritten.
        let temp = dir.join("openraft.log.tmp");
        let header = header(identity);
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&temp)?;
            if matches!(fault, Some(Fault::BootstrapWriteFails)) {
                return Err(io::Error::other("simulated bootstrap write failure"));
            }
            if matches!(fault, Some(Fault::BootstrapWritePartial)) {
                file.write_all(&header[..header.len() / 2])?;
                return Err(io::Error::other("simulated partial bootstrap write"));
            }
            file.write_all(&header)?;
            if matches!(fault, Some(Fault::BootstrapSync)) {
                return Err(io::Error::other("simulated bootstrap sync failure"));
            }
            file.sync_data()?;
        }
        if matches!(fault, Some(Fault::BootstrapRename)) {
            return Err(io::Error::other("simulated bootstrap rename failure"));
        }
        fs::rename(&temp, &path)?;
        if matches!(fault, Some(Fault::BootstrapDirSync)) {
            return Err(io::Error::other(
                "simulated bootstrap directory sync failure",
            ));
        }
        File::open(&dir)?.sync_all()?;
        // Rebind the append handle to the active path after the replacement.
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                file,
                dir,
                bytes: header.len() as u64,
                header,
                frames: 0,
                _lock: lock,
                state: State::default(),
                repaired_tail: None,
                failed: false,
                fault: None,
                #[cfg(test)]
                gate: None,
            })),
        })
    }

    /// Reopen only an existing log for the same group incarnation and node.
    ///
    /// Reopen creates no directory entries, so the parent-of-directory barrier
    /// belongs to `create`: the replica directory and the chain that reaches it
    /// must already be durable. The syncs here cover the log file's contents,
    /// the directory that holds it, and the removal of a leftover temporary.
    pub fn open(dir: impl AsRef<Path>, identity: &ReplicaIdentity) -> io::Result<Self> {
        Self::open_with_fault(dir, identity, None)
    }

    /// Reopen with an optional failure injection for the steps after recovery.
    ///
    /// Recovery repairs a torn tail before these steps, so a failure here never
    /// returns a handle: the repair is already durable and a retry reopens the
    /// repaired file.
    fn open_with_fault(
        dir: impl AsRef<Path>,
        identity: &ReplicaIdentity,
        fault: Option<Fault>,
    ) -> io::Result<Self> {
        let dir = dir.as_ref();
        let lock = OsDirectoryLock::try_acquire(dir)?;
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(dir.join("openraft.log"))?;
        let Recovered {
            state,
            bytes,
            frames,
            repaired_tail,
        } = recover(&mut file, dir, identity)?;
        // Validate checkpoint size too, so every recovered state can later be compacted.
        checkpoint_frame(&state)?;
        // A process-only restart may still see bytes whose previous sync failed.
        if matches!(fault, Some(Fault::OpenSync)) {
            return Err(io::Error::other("simulated reopen sync failure"));
        }
        file.sync_data()?;
        if matches!(fault, Some(Fault::OpenDirSync)) {
            return Err(io::Error::other("simulated reopen directory sync failure"));
        }
        File::open(dir)?.sync_all()?;
        match fs::remove_file(dir.join("openraft.log.tmp")) {
            Ok(()) => File::open(dir)?.sync_all()?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                file,
                dir: dir.to_path_buf(),
                header: header(identity),
                bytes,
                frames,
                _lock: lock,
                state,
                repaired_tail,
                failed: false,
                fault: None,
                #[cfg(test)]
                gate: None,
            })),
        })
    }

    /// The torn final frame discarded by the last [`Self::open`], if any.
    ///
    /// A torn frame was cut short by a crash before it was synced, so it was
    /// never acknowledged to OpenRaft and dropping it cannot lose an entry.
    /// Corruption is never reported here: it makes `open` fail instead.
    pub fn repaired_tail(&self) -> Option<TailRepair> {
        self.inner.lock().unwrap().repaired_tail
    }

    /// What the recovered log retains, for deciding whether a corrupt
    /// state-machine image can be rebuilt by replaying it.
    ///
    /// Reads the same recovered state as [`RaftLogStorage::get_log_state`],
    /// plus the lowest retained index that `LogState` does not expose.
    pub fn coverage(&self) -> LogCoverage {
        let inner = self.inner.lock().unwrap();
        LogCoverage {
            last_purged: inner.state.purged,
            first_index: inner.state.log.first_key_value().map(|(index, _)| *index),
        }
    }

    /// Append one record and sync it before the caller is told it succeeded.
    ///
    /// Any failure poisons the handle: reads, state queries and further writes
    /// are refused until a validated [`Self::open`].
    ///
    /// A failed `sync_data` is an *unknown outcome*, not an abort: the whole
    /// frame was written, but it may or may not have reached the platter. The
    /// adapter therefore refuses the handle and forces a reopen rather than
    /// guessing. On reopen the frame is either present and complete — accepted
    /// and re-synced, which can make the failed record durable after all — or
    /// absent/torn and dropped as never acknowledged. Callers must treat a
    /// failed append as "possibly applied" and rely on OpenRaft re-appending the
    /// same record idempotently after recovery; they must never infer that the
    /// record is gone from the error.
    fn append_durable(&self, record: &Record) -> Result<(), StorageError<u32>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(
                ErrorSubject::Store,
                ErrorVerb::Write,
                io::Error::other("storage requires recovery"),
            ));
        }
        // Validate before persisting so rejected transitions never poison replay.
        let mut next = State {
            log: inner.state.log.clone(),
            vote: inner.state.vote,
            purged: inner.state.purged,
        };
        apply_record(&mut next, record.clone())
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
        let frame = encode(record)
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
        // Check the resulting live image before any I/O. This permits later
        // checkpointing even when the append itself does not trigger it.
        checkpoint_frame(&next)
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
        if inner.frames + 1 > MAX_LOG_FRAMES || inner.bytes + frame.len() as u64 > MAX_LOG_BYTES {
            let checkpoint = checkpoint_frame(&inner.state)
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
            if (inner.header.len() + checkpoint.len() + frame.len()) as u64 > MAX_LOG_BYTES {
                return Err(storage_error(
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                    invalid("record cannot fit after checkpoint"),
                ));
            }
            let fault = match inner.fault {
                Some(
                    fault @ (Fault::CheckpointWriteFails
                    | Fault::CheckpointWritePartial
                    | Fault::CheckpointSync
                    | Fault::CheckpointRename
                    | Fault::CheckpointDirSync
                    | Fault::CheckpointAfterDirSync),
                ) => {
                    inner.fault = None;
                    Some(fault)
                }
                _ => None,
            };
            if let Err(error) = compact(&mut inner, &checkpoint, fault) {
                inner.failed = true;
                return Err(storage_error(ErrorSubject::Logs, ErrorVerb::Write, error));
            }
        }
        let result = match inner.fault.take() {
            Some(Fault::WriteFails) => {
                encode(record).and_then(|_| Err(io::Error::other("simulated write failure")))
            }
            Some(Fault::WritePartial) => encode(record).and_then(|frame| {
                inner.file.write_all(&frame[..frame.len() / 2])?;
                Err(io::Error::other("simulated partial write"))
            }),
            Some(Fault::SyncBefore) => encode(record).and_then(|frame| {
                inner.file.write_all(&frame)?;
                Err(io::Error::other(
                    "simulated sync failure before persistence",
                ))
            }),
            Some(Fault::SyncAfter) => append_frame(&mut inner.file, record)
                .and_then(|_| Err(io::Error::other("simulated sync failure after persistence"))),
            Some(
                fault @ (Fault::CheckpointWriteFails
                | Fault::CheckpointWritePartial
                | Fault::CheckpointSync
                | Fault::CheckpointRename
                | Fault::CheckpointDirSync
                | Fault::CheckpointAfterDirSync),
            ) => {
                // No checkpoint was needed for this record: keep the fault for
                // the compaction that will consume it and append normally.
                inner.fault = Some(fault);
                inner
                    .file
                    .write_all(&frame)
                    .and_then(|_| inner.file.sync_data())
            }
            // Faults that belong to `create`/`open` are consumed there. A stray
            // one on a live handle injects nothing, like `None`.
            Some(
                Fault::BootstrapWriteFails
                | Fault::BootstrapWritePartial
                | Fault::BootstrapSync
                | Fault::BootstrapRename
                | Fault::BootstrapDirSync
                | Fault::BootstrapParentSync
                | Fault::BootstrapAncestorSync
                | Fault::OpenSync
                | Fault::OpenDirSync,
            )
            | None => append_frame(&mut inner.file, record),
        };
        if let Err(error) = result {
            inner.failed = true;
            return Err(storage_error(ErrorSubject::Logs, ErrorVerb::Write, error));
        }
        inner.bytes += frame.len() as u64;
        inner.frames += 1;
        // The serialisation point for concurrent readers. The whole call runs
        // under one `Mutex<Inner>` guard: validation, framing, the durable write
        // and this publication. A `LogReader` therefore observes either the
        // state before the append or the state after it, never a batch that is
        // only partly inserted and never an entry whose frame is not yet synced.
        // The test-only gate parks the writer exactly here so a test can show
        // that a reader cannot pass it.
        #[cfg(test)]
        if let Some(gate) = inner.gate.take() {
            let _ = gate.reached.send(());
            let _ = gate.release.recv();
        }
        inner.state = next;
        Ok(())
    }

    #[cfg(test)]
    fn read_entries(&self, start: u64, end: u64) -> Vec<Entry<C>> {
        self.inner
            .lock()
            .unwrap()
            .state
            .log
            .range(start..end)
            .map(|(_, e)| e.clone())
            .collect()
    }

    /// Test-only: stop the next durable append after its frame is synced but
    /// before the new state is published to readers.
    ///
    /// Returns the signal that the boundary was reached and the sender that
    /// releases it. The gate is consumed by that append, so a later one runs
    /// normally. The append holds the inner lock while it waits, so this also
    /// proves that a concurrent reader is serialised against publication.
    #[cfg(test)]
    pub(crate) fn gate_next_append_for_test(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (reached, reached_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        self.inner.lock().unwrap().gate = Some(AppendGate {
            reached,
            release: release_rx,
        });
        (reached_rx, release)
    }
}

impl RaftLogReader<C> for DurableOpenRaftLog {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry<C>>, StorageError<u32>> {
        let inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(
                ErrorSubject::Store,
                ErrorVerb::Read,
                io::Error::other("storage requires recovery"),
            ));
        }
        Ok(inner
            .state
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<C> for DurableOpenRaftLog {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u32>> {
        let inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(
                ErrorSubject::Store,
                ErrorVerb::Read,
                io::Error::other("storage requires recovery"),
            ));
        }
        Ok(LogState {
            last_purged_log_id: inner.state.purged,
            last_log_id: inner
                .state
                .log
                .last_key_value()
                .map(|(_, e)| e.log_id)
                .or(inner.state.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u32>) -> Result<(), StorageError<u32>> {
        self.append_durable(&Record::Vote(*vote))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u32>>, StorageError<u32>> {
        let inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(
                ErrorSubject::Store,
                ErrorVerb::Read,
                io::Error::other("storage requires recovery"),
            ));
        }
        Ok(inner.state.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<u32>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().take(MAX_LIVE_ENTRIES + 1).collect();
        if entries.len() > MAX_LIVE_ENTRIES {
            return Err(storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Write,
                invalid("OpenRaft append exceeds entry limit"),
            ));
        }
        validate_entries(&entries)
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
        self.append_durable(&Record::Append(entries))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
        self.append_durable(&Record::Truncate(log_id))
    }

    async fn purge(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
        self.append_durable(&Record::Purge(log_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvCommand;
    use openraft::storage::RaftLogStorageExt;
    use openraft::{CommittedLeaderId, EntryPayload};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("yadb-openraft-{nonce}-{}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn identity() -> ReplicaIdentity {
        ReplicaIdentity::new(
            crate::multiraft::GroupIdentity {
                cluster: [1; 16],
                group: 1,
                incarnation: 1,
            },
            1,
            vec![1],
        )
        .unwrap()
    }

    fn entry(index: u64) -> Entry<C> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(KvCommand::Put {
                key: format!("k{index}"),
                value: vec![index as u8],
            }),
        }
    }

    #[tokio::test]
    async fn vote_and_log_survive_restart_and_matching_overlap() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Vote(Vote::new(4, 2)))
            .unwrap();
        store
            .append_durable(&Record::Append(vec![
                entry(0),
                entry(1),
                entry(2),
                entry(3),
            ]))
            .unwrap();
        store
            .append_durable(&Record::Append(vec![entry(2)]))
            .unwrap();
        drop(store);
        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        let mut reopened_reader = reopened.clone();
        assert_eq!(
            reopened_reader.read_vote().await.unwrap(),
            Some(Vote::new(4, 2))
        );
        let entries = reopened.read_entries(1, 4);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].log_id.index, 1);
        assert_eq!(entries[1].log_id.index, 2);
    }

    // A crash between `write_all` and `sync_data` in `append_frame` can leave
    // any prefix of the final frame. Nothing follows it and it was never
    // acknowledged, so `open` must repair it instead of refusing the replica.
    // This replaces `truncated_or_corrupt_tail_fails_closed`, which asserted
    // the old availability bug for exactly this input.
    #[tokio::test]
    async fn torn_final_frame_header_is_repaired_on_open() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Vote(Vote::new(1, 1)))
            .unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0), entry(1)]))
            .unwrap();
        drop(store);
        let path = dir.0.join("openraft.log");
        let prefix = fs::read(&path).unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        // Five bytes of a twelve-byte frame header: magic plus one length byte.
        file.write_all(b"YORF\x08").unwrap();
        drop(file);

        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            reopened.repaired_tail(),
            Some(TailRepair {
                valid_len: prefix.len() as u64,
                discarded: 5,
            })
        );
        assert_eq!(fs::read(&path).unwrap(), prefix);
        assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(1, 1)));
        assert_eq!(
            reopened
                .read_entries(0, 10)
                .iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        // The repaired file is appendable, and reopening it repairs nothing.
        reopened.save_vote(&Vote::new(2, 1)).await.unwrap();
        drop(reopened);
        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.repaired_tail(), None);
        assert_eq!(reopened.read_entries(0, 10).len(), 2);
    }

    // Same argument for a frame whose header arrived but whose payload did not.
    #[test]
    fn torn_final_frame_payload_is_repaired_on_open() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0)]))
            .unwrap();
        drop(store);
        let path = dir.0.join("openraft.log");
        let prefix = fs::read(&path).unwrap();
        let next = encode(&Record::Append(vec![entry(1)])).unwrap();
        let torn = FRAME_HEADER_LEN + 7;
        assert!(torn < next.len());
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&next[..torn]).unwrap();
        drop(file);

        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            reopened.repaired_tail(),
            Some(TailRepair {
                valid_len: prefix.len() as u64,
                discarded: torn as u64,
            })
        );
        assert_eq!(fs::read(&path).unwrap(), prefix);
        assert_eq!(reopened.read_entries(0, 10).len(), 1);
        assert_eq!(reopened.read_entries(0, 10)[0].log_id.index, 0);

        // Idempotent: the surviving prefix is a complete log.
        drop(reopened);
        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.repaired_tail(), None);
        assert_eq!(fs::read(&path).unwrap(), prefix);
    }

    // Frames 1..3 of a three-frame log, used by the corruption tests below.
    fn three_frame_log(dir: &Path) -> (PathBuf, usize) {
        let store = DurableOpenRaftLog::create(dir, &identity()).unwrap();
        store
            .append_durable(&Record::Vote(Vote::new(1, 1)))
            .unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0), entry(1)]))
            .unwrap();
        store
            .append_durable(&Record::Append(vec![entry(2)]))
            .unwrap();
        drop(store);
        let second =
            header(&identity()).len() + encode(&Record::Vote(Vote::new(1, 1))).unwrap().len();
        (dir.join("openraft.log"), second)
    }

    // A complete frame that fails validation was synced before being
    // acknowledged, so it is corruption: discarding it could drop an entry.
    #[test]
    fn mid_file_checksum_corruption_still_fails_closed() {
        let dir = TempDir::new();
        let (path, second) = three_frame_log(&dir.0);
        let mut bytes = fs::read(&path).unwrap();
        bytes[second + FRAME_HEADER_LEN + 5] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let error = DurableOpenRaftLog::open(&dir.0, &identity()).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "OpenRaft log checksum mismatch");
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn mid_file_bad_frame_magic_still_fails_closed() {
        let dir = TempDir::new();
        let (path, second) = three_frame_log(&dir.0);
        let mut bytes = fs::read(&path).unwrap();
        bytes[second..second + 4].copy_from_slice(b"XXXX");
        fs::write(&path, &bytes).unwrap();

        let error = DurableOpenRaftLog::open(&dir.0, &identity()).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "invalid OpenRaft log frame");
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    // The openraft-visible image, so a repair can be shown to drop only bytes
    // that never reached the state.
    type Image = (
        Option<LogId<u32>>,
        Option<LogId<u32>>,
        Option<Vote<u32>>,
        Vec<u8>,
    );

    async fn image(store: &mut DurableOpenRaftLog) -> Image {
        let state = store.get_log_state().await.unwrap();
        let vote = store.read_vote().await.unwrap();
        let entries = store.try_get_log_entries(0..u64::MAX).await.unwrap();
        (
            state.last_log_id,
            state.last_purged_log_id,
            vote,
            serde_json::to_vec(&entries).unwrap(),
        )
    }

    /// The indexes a `RaftLogReader` can currently return, in order.
    async fn readable_indices(store: &mut DurableOpenRaftLog) -> Vec<u64> {
        store
            .try_get_log_entries(0..u64::MAX)
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.log_id.index)
            .collect()
    }

    /// The invariants `LogState` documents, checked against what the log
    /// actually serves (`openraft-0.9.25/src/storage/mod.rs:138-149`):
    /// `last_purged_log_id <= last_applied <= last_log_id`, and the reported
    /// `last_log_id` is the last retained entry or the purge marker when
    /// nothing is retained.
    async fn assert_log_state_invariants(store: &mut DurableOpenRaftLog, what: &str) {
        let state = store.get_log_state().await.unwrap();
        let entries = store.try_get_log_entries(0..u64::MAX).await.unwrap();
        let coverage = store.coverage();

        if let (Some(purged), Some(last)) = (state.last_purged_log_id, state.last_log_id) {
            assert!(
                purged <= last,
                "{what}: last_purged {purged:?} is greater than last_log {last:?}"
            );
        }
        assert_eq!(
            coverage.last_purged, state.last_purged_log_id,
            "{what}: the log's own purge view disagrees with get_log_state"
        );
        assert_eq!(
            state.last_log_id,
            entries.last().map(|entry| entry.log_id).or(state.last_purged_log_id),
            "{what}: last_log_id is neither the last retained entry nor the purge marker"
        );
        if let Some(purged) = state.last_purged_log_id {
            assert!(
                entries
                    .iter()
                    .all(|entry| entry.log_id.index > purged.index),
                "{what}: an entry at or below the purge marker is still readable"
            );
        }
        for pair in entries.windows(2) {
            assert_eq!(
                pair[0].log_id.index + 1,
                pair[1].log_id.index,
                "{what}: the retained log has a hole"
            );
        }
    }

    #[tokio::test]
    async fn repairing_a_torn_tail_preserves_the_recovered_image() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store.save_vote(&Vote::new(3, 1)).await.unwrap();
        store
            .append_durable(&Record::Append((0..=4).map(entry).collect()))
            .unwrap();
        store.purge(entry(1).log_id).await.unwrap();
        store.truncate(entry(4).log_id).await.unwrap();
        let expected = image(&mut store).await;
        drop(store);

        let path = dir.0.join("openraft.log");
        let prefix = fs::read(&path).unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"YORF").unwrap();
        drop(file);

        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert!(reopened.repaired_tail().is_some());
        assert_eq!(fs::read(&path).unwrap(), prefix);
        assert_eq!(image(&mut reopened).await, expected);
    }

    #[test]
    fn oversized_frame_is_rejected_before_reading_its_payload() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        drop(store);
        let path = dir.0.join("openraft.log");
        let mut header = Vec::new();
        header.extend_from_slice(FRAME_MAGIC);
        header.extend_from_slice(&((MAX_FRAME_PAYLOAD as u32) + 1).to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        header.extend_from_slice(&crc32fast::hash(&header).to_le_bytes());
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&header).unwrap();
        drop(file);
        let before = fs::read(&path).unwrap();
        assert_eq!(
            DurableOpenRaftLog::open(&dir.0, &identity())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(fs::read(path).unwrap(), before);
    }

    // The defect that made S1 worth fixing in the legacy WAL, in the OpenRaft
    // log: a length damaged upward in a *non-final* frame used to look exactly
    // like a torn tail, so `open` truncated there and silently dropped every
    // acknowledged frame after it while reporting success. The header checksum
    // is what separates the two, and this asserts the file is left untouched.
    #[test]
    fn upward_corrupted_length_in_the_middle_is_refused_not_truncated() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        for index in 0..3 {
            store
                .append_durable(&Record::Append(vec![entry(index)]))
                .unwrap();
        }
        drop(store);
        let path = dir.0.join("openraft.log");
        let mut bytes = fs::read(&path).unwrap();
        // Byte 4..8 of the first frame is its `len`; the header checksum no
        // longer matches once it is grown.
        let frame_start = header(&identity()).len();
        let stored =
            u32::from_le_bytes(bytes[frame_start + 4..frame_start + 8].try_into().unwrap());
        bytes[frame_start + 4..frame_start + 8].copy_from_slice(&(stored + 4096).to_le_bytes());
        fs::write(&path, &bytes).unwrap();
        let damaged = fs::read(&path).unwrap();

        let error = DurableOpenRaftLog::open(&dir.0, &identity())
            .err()
            .expect("a damaged length must not be repaired as a tail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("header checksum"),
            "unexpected error: {error}"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            damaged,
            "refusing must not rewrite the log"
        );
    }

    // `coverage()` feeds the state-machine rebuild decision, so it must agree
    // with the openraft-visible log state and with what replay will actually
    // see: nothing retained means no replay, a purge moves the start, and a
    // fresh log with entries starts at the first index.
    #[tokio::test]
    async fn coverage_reports_purge_and_first_retained_index() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        assert_eq!(
            store.coverage(),
            LogCoverage {
                last_purged: None,
                first_index: None,
            }
        );
        assert!(!store.coverage().replays_from_start());

        store
            .append_durable(&Record::Append((0..=3).map(entry).collect()))
            .unwrap();
        assert_eq!(
            store.coverage(),
            LogCoverage {
                last_purged: None,
                first_index: Some(0),
            }
        );
        assert!(store.coverage().replays_from_start());

        store.purge(entry(1).log_id).await.unwrap();
        let coverage = store.coverage();
        assert_eq!(coverage.last_purged, Some(entry(1).log_id));
        assert_eq!(coverage.first_index, Some(2));
        assert_eq!(
            coverage.last_purged,
            store.get_log_state().await.unwrap().last_purged_log_id
        );
        assert!(!coverage.replays_from_start());

        drop(store);
        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.coverage(), coverage);
    }

    // Maps to OpenRaft suite: initial_logs, get_log_state, purge_logs_upto_0,
    // purge_logs_upto_5 and purge_logs_upto_20. This is partial coverage only.
    #[tokio::test]
    async fn contract_log_state_and_purge_boundaries_survive_reopen() {
        for boundary in [0, 5, 20] {
            let dir = TempDir::new();
            let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
            let initial = store.get_log_state().await.unwrap();
            assert_eq!(initial.last_log_id, None);
            assert_eq!(initial.last_purged_log_id, None);
            store
                .append_durable(&Record::Append((0..=10).map(entry).collect()))
                .unwrap();
            let purged = entry(boundary).log_id;
            store.purge(purged).await.unwrap();
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id, Some(purged));
            assert_eq!(
                state.last_log_id,
                Some(if boundary > 10 {
                    purged
                } else {
                    entry(10).log_id
                })
            );
            let entries = store.try_get_log_entries(0..100).await.unwrap();
            assert_eq!(entries.len(), 10usize.saturating_sub(boundary as usize));
            drop(store);
            let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
            let recovered = reopened.get_log_state().await.unwrap();
            assert_eq!(recovered.last_log_id, state.last_log_id);
            assert_eq!(recovered.last_purged_log_id, state.last_purged_log_id);
            assert_eq!(
                reopened.try_get_log_entries(0..100).await.unwrap().len(),
                entries.len()
            );
        }
    }

    // Partial mapping to suite delete_logs_since_0, delete_logs_since_11 and
    // append_to_log, with additional reopen checks on the actual file store.
    #[tokio::test]
    async fn contract_truncate_and_append_after_purge_survive_reopen() {
        for boundary in [0, 11] {
            let dir = TempDir::new();
            let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
            store
                .append_durable(&Record::Append((0..=10).map(entry).collect()))
                .unwrap();
            store.truncate(entry(boundary).log_id).await.unwrap();
            let expected = if boundary == 0 {
                None
            } else {
                Some(entry(10).log_id)
            };
            assert_eq!(store.get_log_state().await.unwrap().last_log_id, expected);
            drop(store);
            let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
            assert_eq!(
                reopened.get_log_state().await.unwrap().last_log_id,
                expected
            );
            assert_eq!(
                reopened.try_get_log_entries(0..100).await.unwrap().len(),
                boundary as usize
            );
        }

        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append((0..=10).map(entry).collect()))
            .unwrap();
        store.purge(entry(0).log_id).await.unwrap();
        store
            .append_durable(&Record::Append(vec![entry(11)]))
            .unwrap();
        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        let state = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(entry(0).log_id));
        assert_eq!(state.last_log_id, Some(entry(11).log_id));
        let entries = reopened.try_get_log_entries(0..100).await.unwrap();
        assert_eq!(entries.len(), 11);
        assert_eq!(entries.first().unwrap().log_id.index, 1);
        assert_eq!(entries.last().unwrap().log_id.index, 11);
    }

    #[tokio::test]
    async fn contract_vote_and_range_reads_survive_reopen() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), None);
        store.save_vote(&Vote::new(3, 1)).await.unwrap();
        store.save_vote(&Vote::new_committed(3, 1)).await.unwrap();
        store
            .append_durable(&Record::Append((0..=10).map(entry).collect()))
            .unwrap();
        let ids = |entries: Vec<Entry<C>>| {
            entries
                .into_iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(store.try_get_log_entries(3..7).await.unwrap()),
            vec![3, 4, 5, 6]
        );
        assert_eq!(
            ids(store.try_get_log_entries(8..=10).await.unwrap()),
            vec![8, 9, 10]
        );
        assert!(store.try_get_log_entries(11..20).await.unwrap().is_empty());
        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            reopened.read_vote().await.unwrap(),
            Some(Vote::new_committed(3, 1))
        );
        assert_eq!(
            ids(reopened.try_get_log_entries(3..7).await.unwrap()),
            vec![3, 4, 5, 6]
        );
    }

    #[test]
    fn append_with_a_hole_is_rejected_without_changing_memory() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0), entry(1)]))
            .unwrap();
        assert!(store
            .append_durable(&Record::Append(vec![entry(3)]))
            .is_err());
        assert_eq!(store.read_entries(1, 4).len(), 1);
        drop(store);
        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.read_entries(1, 4).len(), 1);
    }

    #[test]
    fn prepend_must_join_existing_suffix_and_survives_checkpoint() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append((5..=7).map(entry).collect()))
            .unwrap();
        let before = fs::read(dir.0.join("openraft.log")).unwrap();
        assert!(store
            .append_durable(&Record::Append(vec![entry(1), entry(2)]))
            .is_err());
        assert_eq!(fs::read(dir.0.join("openraft.log")).unwrap(), before);
        store
            .append_durable(&Record::Append(vec![entry(3), entry(4)]))
            .unwrap();
        store
            .append_durable(&Record::Append(vec![entry(1), entry(2)]))
            .unwrap();
        store.inner.lock().unwrap().frames = MAX_LOG_FRAMES;
        store
            .append_durable(&Record::Vote(Vote::new(1, 1)))
            .unwrap();
        drop(store);
        let store = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            store
                .read_entries(0, 10)
                .iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<_>>(),
            (1..=7).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn purge_missing_prefix_preserves_checkpoint_continuity() {
        for boundary in [2, 4, 6, 9] {
            let dir = TempDir::new();
            let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
            store
                .append_durable(&Record::Append((5..=7).map(entry).collect()))
                .unwrap();
            let mut wrong = entry(6).log_id;
            wrong.leader_id = CommittedLeaderId::new(2, 1);
            let before = fs::read(dir.0.join("openraft.log")).unwrap();
            assert!(store.purge(wrong).await.is_err());
            assert_eq!(fs::read(dir.0.join("openraft.log")).unwrap(), before);
            if boundary == 2 {
                assert!(store.purge(entry(boundary).log_id).await.is_err());
                assert_eq!(fs::read(dir.0.join("openraft.log")).unwrap(), before);
                // Failure does not poison the handle; a joining marker works.
                store.purge(entry(4).log_id).await.unwrap();
            } else {
                store.purge(entry(boundary).log_id).await.unwrap();
            }
            let effective = if boundary == 2 { 4 } else { boundary };
            store.inner.lock().unwrap().frames = MAX_LOG_FRAMES;
            store.save_vote(&Vote::new(1, 1)).await.unwrap();
            drop(store);
            let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
            let state = reopened.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id, Some(entry(effective).log_id));
            let expected: Vec<u64> = (5..=7).filter(|index| *index > effective).collect();
            assert_eq!(
                reopened
                    .read_entries(0, 10)
                    .iter()
                    .map(|e| e.log_id.index)
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn automatic_checkpoint_preserves_vote_purge_and_suffix() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store.save_vote(&Vote::new_committed(4, 1)).await.unwrap();
        store
            .append_durable(&Record::Append((0..=5).map(entry).collect()))
            .unwrap();
        store.purge(entry(2).log_id).await.unwrap();
        store.inner.lock().unwrap().frames = MAX_LOG_FRAMES;
        store
            .append_durable(&Record::Append(vec![entry(6)]))
            .unwrap();
        assert_eq!(store.inner.lock().unwrap().frames, 2);
        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            reopened.read_vote().await.unwrap(),
            Some(Vote::new_committed(4, 1))
        );
        assert_eq!(
            reopened.get_log_state().await.unwrap().last_purged_log_id,
            Some(entry(2).log_id)
        );
        assert_eq!(
            reopened
                .read_entries(0, 10)
                .iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<_>>(),
            vec![3, 4, 5, 6]
        );
    }

    #[tokio::test]
    async fn append_after_checkpoint_survives_reopen() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append((0..=5).map(entry).collect()))
            .unwrap();
        store.inner.lock().unwrap().frames = MAX_LOG_FRAMES;
        store
            .append_durable(&Record::Append(vec![entry(6)]))
            .unwrap();
        store
            .append_durable(&Record::Append(vec![entry(7)]))
            .unwrap();
        drop(store);
        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            reopened
                .read_entries(0, 10)
                .iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<_>>(),
            (0..=7).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn real_frame_limit_triggers_checkpoint() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        for term in 1..=(MAX_LOG_FRAMES as u64 + 3) {
            store.save_vote(&Vote::new(term, 1)).await.unwrap();
        }
        assert_eq!(store.inner.lock().unwrap().frames, 4);
        drop(store);
        let mut store = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            store.read_vote().await.unwrap(),
            Some(Vote::new(MAX_LOG_FRAMES as u64 + 3, 1))
        );
    }

    // Every checkpoint failure must leave a log that a reopen accepts at the
    // last durable state, and the failure must poison the live handle rather
    // than let the next append run against a half-published file. The
    // difference between the faults is what they publish: a failure before the
    // rename leaves the old log plus an unpublished temporary, and a failure
    // after it leaves the new checkpoint under the active name.
    #[tokio::test]
    async fn checkpoint_faults_poison_and_recover_valid_primary() {
        for fault in [
            Fault::CheckpointWriteFails,
            Fault::CheckpointWritePartial,
            Fault::CheckpointSync,
            Fault::CheckpointRename,
            Fault::CheckpointDirSync,
            Fault::CheckpointAfterDirSync,
        ] {
            let dir = TempDir::new();
            let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
            store.save_vote(&Vote::new(1, 1)).await.unwrap();
            store
                .append_durable(&Record::Append(vec![entry(0), entry(1)]))
                .unwrap();
            let path = dir.0.join("openraft.log");
            let temp = dir.0.join("openraft.log.tmp");
            let before_bytes = fs::read(&path).unwrap();
            let before = image(&mut store).await;
            {
                let mut inner = store.inner.lock().unwrap();
                inner.frames = MAX_LOG_FRAMES;
                inner.fault = Some(fault);
            }
            assert!(
                store.save_vote(&Vote::new(2, 1)).await.is_err(),
                "{fault:?} was not injected"
            );
            // Every read and write is refused until a validated reopen.
            assert!(store.read_vote().await.is_err(), "{fault:?}");
            assert!(store.get_log_state().await.is_err(), "{fault:?}");
            assert!(store.try_get_log_entries(0..10).await.is_err(), "{fault:?}");
            assert!(
                store.save_vote(&Vote::new(3, 1)).await.is_err(),
                "{fault:?}"
            );
            match fault {
                Fault::CheckpointWriteFails
                | Fault::CheckpointWritePartial
                | Fault::CheckpointSync
                | Fault::CheckpointRename => {
                    assert!(temp.exists(), "{fault:?} must leave the temporary behind");
                    assert_eq!(
                        fs::read(&path).unwrap(),
                        before_bytes,
                        "{fault:?} must not touch the active log"
                    );
                }
                _ => {
                    assert!(!temp.exists(), "{fault:?} published the temporary");
                    assert_ne!(
                        fs::read(&path).unwrap(),
                        before_bytes,
                        "{fault:?} must publish the checkpoint"
                    );
                }
            }
            drop(store);

            let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
            assert_eq!(
                image(&mut reopened).await,
                before,
                "{fault:?} must reopen to the last durable state"
            );
            assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(1, 1)));
            assert_eq!(reopened.read_entries(0, 2).len(), 2);
            assert!(!temp.exists());
            // The failed record is absent, and the recovered log is writable.
            reopened.save_vote(&Vote::new(2, 1)).await.unwrap();
        }
    }

    #[test]
    fn rejected_limits_do_not_write_or_poison() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        let before = fs::read(dir.0.join("openraft.log")).unwrap();
        assert!(store
            .append_durable(&Record::Append(
                (0..=MAX_LIVE_ENTRIES as u64).map(entry).collect()
            ))
            .is_err());
        assert_eq!(fs::read(dir.0.join("openraft.log")).unwrap(), before);
        store
            .append_durable(&Record::Vote(Vote::new(2, 1)))
            .unwrap();
        assert!(store
            .append_durable(&Record::Vote(Vote::new(1, 1)))
            .is_err());
        store
            .append_durable(&Record::Vote(Vote::new(3, 1)))
            .unwrap();
    }

    #[test]
    fn invalid_primary_preserves_checkpoint_temp() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        drop(store);
        let active = dir.0.join("openraft.log");
        let temp = dir.0.join("openraft.log.tmp");
        fs::write(&temp, b"recovery evidence").unwrap();
        fs::write(&active, b"corrupt").unwrap();
        let error = DurableOpenRaftLog::open(&dir.0, &identity())
            .err()
            .expect("a corrupt primary must not open");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        // Refusing must keep both the unreadable primary (the evidence of what
        // happened) and the temporary (the only complete copy of the state).
        assert_eq!(fs::read(&active).unwrap(), b"corrupt");
        assert_eq!(fs::read(temp).unwrap(), b"recovery evidence");
    }

    #[test]
    fn overlap_conflicts_and_purge_identity_are_rejected_across_reopen() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append((0..=4).map(entry).collect()))
            .unwrap();
        store
            .append_durable(&Record::Append(vec![entry(1), entry(2)]))
            .unwrap();
        let mut conflict = entry(2);
        conflict.log_id = LogId::new(CommittedLeaderId::new(2, 1), 2);
        let mut different_payload = entry(2);
        different_payload.payload = EntryPayload::Blank;
        let before = fs::read(dir.0.join("openraft.log")).unwrap();
        assert!(store
            .append_durable(&Record::Append(vec![conflict.clone()]))
            .is_err());
        assert!(store
            .append_durable(&Record::Append(vec![different_payload]))
            .is_err());
        assert!(store
            .append_durable(&Record::Purge(conflict.log_id))
            .is_err());
        assert_eq!(fs::read(dir.0.join("openraft.log")).unwrap(), before);
        store
            .append_durable(&Record::Purge(entry(2).log_id))
            .unwrap();
        assert!(store
            .append_durable(&Record::Append(vec![conflict, entry(3)]))
            .is_err());
        drop(store);
        let store = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(store.read_entries(3, 5).len(), 2);
        store
            .append_durable(&Record::Truncate(entry(3).log_id))
            .unwrap();
        let mut replacement = entry(3);
        replacement.log_id = LogId::new(CommittedLeaderId::new(2, 1), 3);
        store
            .append_durable(&Record::Append(vec![replacement.clone()]))
            .unwrap();
        drop(store);
        let store = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(store.read_entries(3, 5)[0].log_id, replacement.log_id);
        assert_eq!(store.read_entries(3, 5).len(), 1);
    }

    #[test]
    fn second_open_is_rejected_while_first_handle_is_alive() {
        let dir = TempDir::new();
        let first = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_err());
        drop(first);
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_ok());
    }

    // `Fault::WritePartial` writes the first half of a frame and fails, which is
    // exactly the artefact a crash between `write_all` and `sync_data` leaves:
    // a complete header and a partial payload, with nothing after it. The frame
    // was never acknowledged, so the reopen repairs it instead of failing.
    #[tokio::test]
    async fn partial_write_poisoned_until_reopen_which_repairs_the_torn_tail() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Vote(Vote::new(1, 1)))
            .unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0), entry(1)]))
            .unwrap();
        let path = dir.0.join("openraft.log");
        let prefix = fs::read(&path).unwrap();
        let torn = encode(&Record::Vote(Vote::new(2, 1))).unwrap();
        assert!(torn.len() / 2 > FRAME_HEADER_LEN);
        store.inner.lock().unwrap().fault = Some(Fault::WritePartial);
        assert!(store
            .append_durable(&Record::Vote(Vote::new(2, 1)))
            .is_err());
        // The live handle stays poisoned until it is reopened, even though the
        // failure left only a repairable tail on disk. Reads are refused too, so
        // a caller cannot observe a state the log could not confirm.
        assert!(store.read_vote().await.is_err());
        assert!(store.get_log_state().await.is_err());
        assert!(store.try_get_log_entries(0..10).await.is_err());
        assert!(store
            .append_durable(&Record::Vote(Vote::new(3, 1)))
            .is_err());
        drop(store);
        assert_eq!(
            fs::read(&path).unwrap().len(),
            prefix.len() + torn.len() / 2
        );

        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            reopened.repaired_tail(),
            Some(TailRepair {
                valid_len: prefix.len() as u64,
                discarded: (torn.len() / 2) as u64,
            })
        );
        assert_eq!(fs::read(&path).unwrap(), prefix);
        assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(1, 1)));
        assert_eq!(
            reopened
                .read_entries(0, 10)
                .iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        // The repaired log is writable again, and the repair is not sticky.
        reopened.save_vote(&Vote::new(2, 1)).await.unwrap();
        drop(reopened);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.repaired_tail(), None);
        assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
    }

    // A sync failure happens after the whole frame reached the file, so a
    // reopen must accept that complete frame instead of repairing it away. The
    // fault is still an error to the caller and still poisons the handle.
    #[tokio::test]
    async fn sync_errors_poison_writer_and_reopen_resyncs_visible_record() {
        for fault in [Fault::SyncBefore, Fault::SyncAfter] {
            let dir = TempDir::new();
            let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
            store
                .append_durable(&Record::Vote(Vote::new(1, 1)))
                .unwrap();
            let path = dir.0.join("openraft.log");
            let before = fs::read(&path).unwrap();
            let frame = encode(&Record::Vote(Vote::new(2, 1))).unwrap();
            store.inner.lock().unwrap().fault = Some(fault);
            assert!(store
                .append_durable(&Record::Vote(Vote::new(2, 1)))
                .is_err());
            assert!(store
                .append_durable(&Record::Vote(Vote::new(3, 1)))
                .is_err());
            assert!(store.read_vote().await.is_err());
            assert!(store.get_log_state().await.is_err());
            drop(store);
            let after = fs::read(&path).unwrap();
            assert_eq!(after.len(), before.len() + frame.len(), "{fault:?}");
            assert_eq!(&after[..before.len()], &before[..], "{fault:?}");
            let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
            assert_eq!(reopened.repaired_tail(), None, "{fault:?}");
            assert_eq!(fs::read(&path).unwrap(), after, "{fault:?}");
            assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
        }
    }

    /// The durable record appends the adapter performs, so a fault on the shared
    /// append path is checked for every record kind and not only for votes.
    #[derive(Clone, Copy, Debug)]
    enum AppendOp {
        Vote,
        Append,
        Truncate,
        Purge,
    }

    impl AppendOp {
        const ALL: [AppendOp; 4] = [
            AppendOp::Vote,
            AppendOp::Append,
            AppendOp::Truncate,
            AppendOp::Purge,
        ];

        fn record(self) -> Record {
            match self {
                AppendOp::Vote => Record::Vote(Vote::new(2, 1)),
                AppendOp::Append => Record::Append(vec![entry(2)]),
                AppendOp::Truncate => Record::Truncate(entry(1).log_id),
                AppendOp::Purge => Record::Purge(entry(0).log_id),
            }
        }
    }

    /// The state a store reaches after applying `op` to the common prefix. This
    /// is an oracle for the faulted store's recovered state, so the expectations
    /// are not hand-written copies of the adapter's own serialization.
    async fn image_after(op: AppendOp) -> Image {
        let dir = TempDir::new();
        let mut control = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        control.save_vote(&Vote::new(1, 1)).await.unwrap();
        control
            .append_durable(&Record::Append(vec![entry(0), entry(1)]))
            .unwrap();
        control.append_durable(&op.record()).unwrap();
        image(&mut control).await
    }

    /// Every record kind shares `append_durable`, so every fault on that path is
    /// exercised for all of them. The contract differs only in how far the
    /// record got: a failed write must leave the file untouched, a partial write
    /// must be repaired as a tail, and a failed sync must keep the whole frame,
    /// because a complete frame is never a torn record.
    #[tokio::test]
    async fn append_path_faults_poison_and_reopen_to_a_complete_state() {
        for op in AppendOp::ALL {
            let expected_after = image_after(op).await;
            for fault in [
                Fault::WriteFails,
                Fault::WritePartial,
                Fault::SyncBefore,
                Fault::SyncAfter,
            ] {
                let dir = TempDir::new();
                let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
                store.save_vote(&Vote::new(1, 1)).await.unwrap();
                store
                    .append_durable(&Record::Append(vec![entry(0), entry(1)]))
                    .unwrap();
                let path = dir.0.join("openraft.log");
                let before_bytes = fs::read(&path).unwrap();
                let before = image(&mut store).await;
                let frame = encode(&op.record()).unwrap();
                assert!(
                    frame.len() / 2 > FRAME_HEADER_LEN,
                    "{op:?} must exercise a torn payload, not a torn header"
                );

                store.inner.lock().unwrap().fault = Some(fault);
                assert!(
                    store.append_durable(&op.record()).is_err(),
                    "{op:?}/{fault:?} was not injected"
                );
                // Poisoned: reads, state and further writes are all refused, so
                // nothing can be appended after a partial frame.
                assert!(store.read_vote().await.is_err(), "{op:?}/{fault:?}");
                assert!(store.get_log_state().await.is_err(), "{op:?}/{fault:?}");
                assert!(
                    store.try_get_log_entries(0..10).await.is_err(),
                    "{op:?}/{fault:?}"
                );
                assert!(
                    store
                        .append_durable(&Record::Vote(Vote::new(9, 1)))
                        .is_err(),
                    "{op:?}/{fault:?}"
                );

                let on_disk = fs::read(&path).unwrap();
                match fault {
                    Fault::WriteFails => assert_eq!(
                        on_disk, before_bytes,
                        "{op:?}: a failed write must not change the file"
                    ),
                    Fault::WritePartial => {
                        assert_eq!(
                            on_disk.len(),
                            before_bytes.len() + frame.len() / 2,
                            "{op:?}"
                        );
                        assert_eq!(&on_disk[..before_bytes.len()], &before_bytes[..]);
                    }
                    _ => {
                        assert_eq!(on_disk.len(), before_bytes.len() + frame.len(), "{op:?}");
                        assert_eq!(&on_disk[..before_bytes.len()], &before_bytes[..]);
                    }
                }
                drop(store);

                let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
                match fault {
                    Fault::WriteFails => {
                        assert_eq!(reopened.repaired_tail(), None, "{op:?}");
                        assert_eq!(image(&mut reopened).await, before, "{op:?}");
                    }
                    Fault::WritePartial => {
                        assert_eq!(
                            reopened.repaired_tail(),
                            Some(TailRepair {
                                valid_len: before_bytes.len() as u64,
                                discarded: (frame.len() / 2) as u64,
                            }),
                            "{op:?}"
                        );
                        assert_eq!(fs::read(&path).unwrap(), before_bytes, "{op:?}");
                        assert_eq!(image(&mut reopened).await, before, "{op:?}");
                    }
                    _ => {
                        // The whole frame is on disk and complete: accepting it
                        // is correct, and repairing it would lose a record.
                        assert_eq!(reopened.repaired_tail(), None, "{op:?}");
                        assert_eq!(fs::read(&path).unwrap(), on_disk, "{op:?}");
                        assert_eq!(image(&mut reopened).await, expected_after, "{op:?}");
                    }
                }
                reopened.save_vote(&Vote::new(7, 1)).await.unwrap();
            }
        }
    }

    /// Every bootstrap failure is classified by what it published: a failure
    /// before the rename leaves nothing `open` accepts, and a failure after it
    /// leaves a complete log that `open` accepts and re-syncs. Neither may leave
    /// a log that opens to a torn or headerless image.
    #[test]
    fn bootstrap_faults_publish_nothing_or_a_reopenable_log() {
        for (fault, message, published) in [
            (
                Fault::BootstrapWriteFails,
                "simulated bootstrap write failure",
                false,
            ),
            (
                Fault::BootstrapWritePartial,
                "simulated partial bootstrap write",
                false,
            ),
            (
                Fault::BootstrapSync,
                "simulated bootstrap sync failure",
                false,
            ),
            (
                Fault::BootstrapRename,
                "simulated bootstrap rename failure",
                false,
            ),
            (
                Fault::BootstrapDirSync,
                "simulated bootstrap directory sync failure",
                true,
            ),
            (
                Fault::BootstrapParentSync,
                "simulated bootstrap parent-directory sync failure",
                false,
            ),
        ] {
            let dir = TempDir::new();
            let path = dir.0.join("openraft.log");
            let error =
                match DurableOpenRaftLog::create_with_fault(&dir.0, &identity(), Some(fault)) {
                    Ok(_) => panic!("{fault:?} did not fail"),
                    Err(error) => error,
                };
            assert_eq!(error.to_string(), message, "{fault:?}");
            assert_eq!(path.exists(), published, "{fault:?}");

            if published {
                // The rename landed but its directory entry is not proven
                // durable. A reopen accepts the complete log and syncs the
                // directory, so recovery is `open`, not a second `create`.
                assert!(
                    DurableOpenRaftLog::create(&dir.0, &identity()).is_err(),
                    "{fault:?} must not be re-bootstrapped over"
                );
                let store = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
                assert_eq!(store.coverage().first_index, None, "{fault:?}");
                drop(store);
            } else {
                assert!(
                    DurableOpenRaftLog::open(&dir.0, &identity()).is_err(),
                    "{fault:?} must not publish an openable log"
                );
                // Bootstrap is retryable over whatever temporary was left.
                let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
                drop(store);
            }
            let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
            let expected = header(&identity());
            assert_eq!(
                &fs::read(&path).unwrap()[..expected.len()],
                &expected[..],
                "{fault:?} must end with the requested identity"
            );
            assert_eq!(reopened.coverage().first_index, None, "{fault:?}");
        }
    }

    /// A failure after recovery must not return a handle, and because recovery
    /// truncates a torn tail before the injected sync, the retry sees a repaired
    /// file and reports nothing left to repair.
    #[test]
    fn reopen_faults_refuse_and_a_retry_recovers_the_repaired_log() {
        for (fault, message) in [
            (Fault::OpenSync, "simulated reopen sync failure"),
            (
                Fault::OpenDirSync,
                "simulated reopen directory sync failure",
            ),
        ] {
            let dir = TempDir::new();
            let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
            store
                .append_durable(&Record::Append(vec![entry(0), entry(1)]))
                .unwrap();
            drop(store);
            let path = dir.0.join("openraft.log");
            let prefix = fs::read(&path).unwrap();
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"YORF\x08").unwrap();
            drop(file);

            let error = match DurableOpenRaftLog::open_with_fault(&dir.0, &identity(), Some(fault))
            {
                Ok(_) => panic!("{fault:?} did not fail"),
                Err(error) => error,
            };
            assert_eq!(error.to_string(), message);
            assert_eq!(
                fs::read(&path).unwrap(),
                prefix,
                "{fault:?}: the repair must be complete before the failure"
            );
            let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
            assert_eq!(reopened.repaired_tail(), None, "{fault:?}");
            assert_eq!(
                reopened
                    .read_entries(0, 10)
                    .iter()
                    .map(|e| e.log_id.index)
                    .collect::<Vec<_>>(),
                vec![0, 1]
            );
        }
    }

    #[test]
    fn existing_empty_log_is_not_reinitialized() {
        let dir = TempDir::new();
        let path = dir.0.join("openraft.log");
        File::create(&path).unwrap();
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_err());
        assert_eq!(fs::metadata(path).unwrap().len(), 0);
    }

    #[test]
    fn previous_log_format_is_rejected_without_modification() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0), entry(1)]))
            .unwrap();
        drop(store);
        let path = dir.0.join("openraft.log");
        let original = fs::read(&path).unwrap();
        // A pre-fix binary would read a 05 frame as a torn tail and truncate it,
        // so the format is versioned rather than silently reinterpreted.
        for magic in [b"YORLOG02", b"YORLOG03", b"YORLOG04"] {
            let mut bytes = original.clone();
            bytes[..8].copy_from_slice(magic);
            let checksum_offset = header(&identity()).len() - 4;
            let checksum = crc32fast::hash(&bytes[..checksum_offset]);
            bytes[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum.to_le_bytes());
            fs::write(&path, &bytes).unwrap();
            assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn wrong_identity_and_missing_log_never_modify_data() {
        let dir = TempDir::new();
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_err());
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Vote(Vote::new(1, 1)))
            .unwrap();
        drop(store);
        let path = dir.0.join("openraft.log");
        let before = fs::read(&path).unwrap();
        let base = identity().group().clone();
        let variants = [
            ReplicaIdentity::new(
                crate::multiraft::GroupIdentity {
                    cluster: [2; 16],
                    ..base.clone()
                },
                1,
                vec![1],
            )
            .unwrap(),
            ReplicaIdentity::new(
                crate::multiraft::GroupIdentity {
                    group: 2,
                    ..base.clone()
                },
                1,
                vec![1],
            )
            .unwrap(),
            ReplicaIdentity::new(
                crate::multiraft::GroupIdentity {
                    incarnation: 2,
                    ..base.clone()
                },
                1,
                vec![1],
            )
            .unwrap(),
            ReplicaIdentity::new(base, 2, vec![2]).unwrap(),
        ];
        for other in variants {
            assert!(DurableOpenRaftLog::open(&dir.0, &other).is_err());
            assert_eq!(fs::read(&path).unwrap(), before);
        }
        assert!(DurableOpenRaftLog::create(&dir.0, &identity()).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_ok());
    }

    #[tokio::test]
    async fn purge_boundary_survives_reopen_and_rejects_invalid_mutations() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append((0..=3).map(entry).collect()))
            .unwrap();
        let boundary = entry(2).log_id;
        store.purge(boundary).await.unwrap();
        assert!(store.truncate(boundary).await.is_err());
        assert!(store.purge(entry(1).log_id).await.is_err());
        assert!(store
            .append_durable(&Record::Append(vec![entry(2)]))
            .is_err());
        assert!(store.try_get_log_entries(2..=2).await.unwrap().is_empty());
        drop(store);
        let mut store = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(boundary));
        assert_eq!(state.last_log_id, Some(entry(3).log_id));
        store.truncate(entry(3).log_id).await.unwrap();
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(boundary)
        );
        drop(store);
        let mut store = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(boundary)
        );
    }

    /// A failure after the header is written but before it is published must
    /// leave nothing that `open` accepts, and a retry must be able to bootstrap
    /// over the leftover temporary file.
    #[test]
    fn failed_bootstrap_leaves_nothing_openable_and_retry_succeeds() {
        let dir = TempDir::new();
        let error = match DurableOpenRaftLog::create_with_fault(
            &dir.0,
            &identity(),
            Some(Fault::BootstrapSync),
        ) {
            Ok(_) => panic!("create succeeded despite an injected bootstrap sync failure"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "simulated bootstrap sync failure");
        assert!(
            !dir.0.join("openraft.log").exists(),
            "a failed bootstrap must not publish a log file"
        );
        assert!(
            DurableOpenRaftLog::open(&dir.0, &identity()).is_err(),
            "open must refuse a directory whose bootstrap never completed"
        );

        // A retry must truncate and rewrite whatever temporary the failed
        // attempt left behind instead of treating it as an obstacle.
        fs::write(dir.0.join("openraft.log.tmp"), b"stale partial bootstrap").unwrap();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        drop(store);
        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        let expected = header(&identity());
        assert_eq!(
            &fs::read(dir.0.join("openraft.log")).unwrap()[..expected.len()],
            &expected[..],
            "the retried bootstrap must publish the requested identity"
        );
        assert_eq!(reopened.coverage().first_index, None);
    }

    /// `create` is a bootstrap operation, never an overwrite: an existing log
    /// and its acknowledged records must be left byte-for-byte unchanged.
    #[test]
    fn create_never_overwrites_an_existing_complete_log() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0)]))
            .unwrap();
        drop(store);
        let before = fs::read(dir.0.join("openraft.log")).unwrap();

        let error = match DurableOpenRaftLog::create(&dir.0, &identity()) {
            Ok(_) => panic!("create overwrote an existing complete log"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            fs::read(dir.0.join("openraft.log")).unwrap(),
            before,
            "create must not modify an existing log"
        );
    }

    /// The publish ordering is internally consistent: once `create` returns,
    /// `open` reads the same identity from the file that was just synced.
    #[test]
    fn created_log_opens_immediately_with_the_same_identity() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        drop(store);

        let expected = header(&identity());
        let bytes = fs::read(dir.0.join("openraft.log")).unwrap();
        assert_eq!(&bytes[..expected.len()], &expected[..]);

        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.coverage().first_index, None);
        assert_eq!(reopened.coverage().last_purged, None);

        let other = ReplicaIdentity::new(
            crate::multiraft::GroupIdentity {
                incarnation: 2,
                ..identity().group().clone()
            },
            1,
            vec![1],
        )
        .unwrap();
        assert!(
            DurableOpenRaftLog::open(&dir.0, &other).is_err(),
            "the published header must be bound to the identity passed to create"
        );
    }

    /// Creating a directory is only durable once its parent is synced. This
    /// test proves the bootstrap path performs that sync before publishing.
    #[test]
    fn create_propagates_a_parent_directory_sync_failure() {
        let dir = TempDir::new();
        let error = match DurableOpenRaftLog::create_with_fault(
            &dir.0,
            &identity(),
            Some(Fault::BootstrapParentSync),
        ) {
            Ok(_) => panic!("create succeeded despite an injected parent-directory sync failure"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "simulated bootstrap parent-directory sync failure"
        );
        assert!(dir.0.is_dir(), "create_dir_all ran before the sync failed");
        assert!(!dir.0.join("openraft.log").exists());
        assert!(
            !dir.0.join("openraft.log.tmp").exists(),
            "the parent sync is a barrier before any temporary header is written"
        );

        // Only the sync failed: the same call succeeds without the injection.
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        drop(store);
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_ok());
    }

    /// A nested replica path makes `create_dir_all` create more than one level.
    /// Syncing only the leaf's immediate parent would leave the new
    /// intermediate directories' own entries unsynced in *their* parents, so a
    /// crash could make the whole chain unreachable. Every created level must
    /// have its parent synced before a handle is returned.
    ///
    /// An in-process test cannot observe a real crash, so the closest
    /// observable evidence is the failure path: a fault at an ancestor sync —
    /// which the single-level `create_propagates_a_parent_directory_sync_failure`
    /// never reaches — must make `create` refuse even though the whole chain
    /// exists, and no log may be published.
    #[test]
    fn create_syncs_each_new_ancestor_before_publishing_the_log() {
        let root = TempDir::new();
        let dir = root.0.join("a/b/c");
        let error = match DurableOpenRaftLog::create_with_fault(
            &dir,
            &identity(),
            Some(Fault::BootstrapAncestorSync),
        ) {
            Ok(_) => panic!("create succeeded despite an injected ancestor-directory sync failure"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "simulated bootstrap ancestor-directory sync failure"
        );
        assert!(
            dir.is_dir() && root.0.join("a/b").is_dir() && root.0.join("a").is_dir(),
            "create_dir_all created the chain before the ancestor sync failed"
        );
        assert!(
            !dir.join("openraft.log").exists(),
            "a failed ancestor sync must not publish a log"
        );
        assert!(
            DurableOpenRaftLog::open(&dir, &identity()).is_err(),
            "a failed ancestor sync must not leave an openable log"
        );

        // Only the sync failed: the same nested path bootstraps and reopens
        // without the injection.
        let store = DurableOpenRaftLog::create(&dir, &identity()).unwrap();
        drop(store);
        assert!(DurableOpenRaftLog::open(&dir, &identity()).is_ok());
    }

    /// OpenRaft 0.9.25 contract rows: `save_vote` must persist before it
    /// returns (`storage/v2.rs:74-79`), and votes are compared with
    /// `Vote::partial_cmp` (`vote/vote.rs:20-44`), so a later vote must never
    /// be replaced by an earlier one.
    ///
    /// `rejected_limits_do_not_write_or_poison` already asserts that one
    /// decreasing vote errors and leaves the file unchanged. This test pins the
    /// ordering relation itself: equal, greater and committed transitions are
    /// accepted, decreasing term and decreasing node id are refused without
    /// changing the durable image, and the accepted vote survives a reopen.
    #[tokio::test]
    async fn contract_save_vote_is_monotonic_and_durable() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();

        store.save_vote(&Vote::new(5, 2)).await.unwrap();
        // Re-saving the same vote is not a regression and must be accepted.
        store.save_vote(&Vote::new(5, 2)).await.unwrap();
        // A greater term is accepted.
        store.save_vote(&Vote::new(6, 1)).await.unwrap();
        // For an equal term, a greater node id is greater.
        store.save_vote(&Vote::new(6, 2)).await.unwrap();
        // The committed bit orders above the uncommitted vote for the same
        // leader id (`vote/vote.rs:24`).
        store.save_vote(&Vote::new_committed(6, 2)).await.unwrap();

        let accepted = image(&mut store).await;
        assert_eq!(accepted.2, Some(Vote::new_committed(6, 2)));

        for older in [Vote::new(5, 1), Vote::new(6, 1), Vote::new(6, 2)] {
            let error = store.save_vote(&older).await.unwrap_err();
            assert!(
                format!("{error}").contains("vote cannot move backwards"),
                "saving {older:?} must be refused as a regression, got {error}"
            );
            assert_eq!(
                image(&mut store).await,
                accepted,
                "a refused vote {older:?} changed the durable image"
            );
        }

        // The refusal did not poison the handle: the next greater vote is
        // accepted and survives a reopen.
        store.save_vote(&Vote::new(7, 1)).await.unwrap();
        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            reopened.read_vote().await.unwrap(),
            Some(Vote::new(7, 1)),
            "save_vote must be durable before it returns"
        );
    }

    /// OpenRaft 0.9.25 contract rows: `truncate` removes `log_id` and
    /// everything after it, inclusive (`storage/v2.rs:131-136`), and must not
    /// leave a hole. Purged entries must never become readable again, so a
    /// truncate at or below the purge marker is refused.
    ///
    /// Existing coverage truncates at 0 and at 11 after appending 0..=10
    /// (`contract_truncate_and_append_after_purge_survive_reopen`). This test
    /// covers the four boundaries the contract leaves open: at the last index,
    /// above it, at the purge marker and below the marker.
    #[tokio::test]
    async fn contract_truncate_boundaries_are_inclusive_and_reject_the_purged_prefix() {
        // At the last index: exactly that entry is removed and the predecessor
        // becomes the last log id.
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append((0..=10).map(entry).collect()))
            .unwrap();
        store.truncate(entry(10).log_id).await.unwrap();
        assert_eq!(readable_indices(&mut store).await, (0..=9).collect::<Vec<_>>());
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(entry(9).log_id)
        );
        assert_log_state_invariants(&mut store, "truncate at the last index").await;

        // Above the last index: nothing is removed and no hole appears.
        store.truncate(entry(11).log_id).await.unwrap();
        assert_eq!(readable_indices(&mut store).await, (0..=9).collect::<Vec<_>>());
        assert_log_state_invariants(&mut store, "truncate above the last index").await;

        // The next contiguous append still fits, and a reopen preserves it.
        store
            .append_durable(&Record::Append(vec![entry(10)]))
            .unwrap();
        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            readable_indices(&mut reopened).await,
            (0..=10).collect::<Vec<_>>()
        );

        // At and below the purge marker: refused, with no durable change and no
        // poison, because either would make a purged entry readable again.
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append((0..=10).map(entry).collect()))
            .unwrap();
        store.purge(entry(5).log_id).await.unwrap();
        let durable = image(&mut store).await;
        for boundary in [5, 3] {
            let error = store.truncate(entry(boundary).log_id).await.unwrap_err();
            assert!(
                format!("{error}").contains("cannot truncate purged logs"),
                "truncate({boundary}) must be refused, got {error}"
            );
            assert_eq!(
                image(&mut store).await,
                durable,
                "truncate({boundary}) changed the log"
            );
            assert_log_state_invariants(&mut store, "rejected truncate").await;
        }

        // At the first retained index: allowed. The retained suffix is removed
        // and the purge marker becomes the last log id, which is exactly the
        // fallback `LogState` documents (`storage/mod.rs:146-148`).
        store.truncate(entry(6).log_id).await.unwrap();
        assert!(readable_indices(&mut store).await.is_empty());
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(entry(5).log_id));
        assert_eq!(state.last_log_id, Some(entry(5).log_id));
        assert_log_state_invariants(&mut store, "truncate at the first retained index").await;

        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert!(readable_indices(&mut reopened).await.is_empty());
        assert_eq!(
            reopened.get_log_state().await.unwrap().last_purged_log_id,
            Some(entry(5).log_id)
        );
    }

    /// OpenRaft 0.9.25 contract rows: `RaftLogReader::try_get_log_entries`
    /// returns `[start, stop)` and explicitly allows an entry that is not found
    /// (`storage/mod.rs:162-171`); `limited_get_log_entries` must not return an
    /// empty result for a non-empty range (`storage/mod.rs:173-188`).
    ///
    /// After a purge, a range that starts below the marker and ends above it is
    /// the interesting case: the purged prefix must be skipped, not turned into
    /// an error and not filled with placeholder entries.
    #[tokio::test]
    async fn contract_range_read_spanning_the_purge_boundary_returns_only_retained_entries() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append((0..=10).map(entry).collect()))
            .unwrap();
        store.purge(entry(5).log_id).await.unwrap();

        let ids = |entries: Vec<Entry<C>>| {
            entries
                .into_iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>()
        };

        // Starts below the marker, ends above it.
        assert_eq!(
            ids(store.try_get_log_entries(2..9).await.unwrap()),
            vec![6, 7, 8]
        );
        // A range entirely inside the purged prefix is empty, not an error.
        assert!(store.try_get_log_entries(0..5).await.unwrap().is_empty());
        assert!(store.try_get_log_entries(5..6).await.unwrap().is_empty());
        // The retained suffix, including the first retained entry.
        assert_eq!(
            ids(store.try_get_log_entries(6..11).await.unwrap()),
            vec![6, 7, 8, 9, 10]
        );
        // The default `limited_get_log_entries` path must not return empty for a
        // non-empty range that contains retained entries.
        assert_eq!(
            ids(store.limited_get_log_entries(6, 9).await.unwrap()),
            vec![6, 7, 8]
        );
        assert_log_state_invariants(&mut store, "spanning read").await;

        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            ids(reopened.try_get_log_entries(4..8).await.unwrap()),
            vec![6, 7]
        );
    }

    /// OpenRaft 0.9.25 contract rows: `append` must make the entries readable
    /// when it returns, the callback must fire only once they are persisted,
    /// and there must be no hole (`storage/v2.rs:108-129`). All storage access
    /// is concurrent only with the log reader (`storage/mod.rs:220-228`), and
    /// all write IO is serialised (`storage/v2.rs:45-48`).
    ///
    /// The adapter publishes `inner.state` only after `write_all` and
    /// `sync_data` succeed, all under one `Mutex<Inner>` guard. This test parks
    /// the writer at that publication boundary and shows a concurrent reader
    /// cannot pass it, then observes the whole batch once the writer is
    /// released. A torn or partly inserted batch is therefore not observable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reader_is_serialised_against_append_publication() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0), entry(1)]))
            .unwrap();

        let (reached, release) = store.gate_next_append_for_test();
        let writer = store.clone();
        let append = tokio::spawn(async move {
            writer
                .append_durable(&Record::Append(vec![entry(2), entry(3)]))
                .unwrap();
        });
        reached
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the append must reach its publication boundary");

        // The writer has written and synced the frame but has not published the
        // new state, and it still holds the inner lock.
        let mut reader = store.get_log_reader().await;
        let (started, started_rx) = std::sync::mpsc::channel();
        let (done, done_rx) = std::sync::mpsc::channel();
        let read = tokio::spawn(async move {
            started.send(()).unwrap();
            let indices: Vec<u64> = reader
                .try_get_log_entries(0..10)
                .await
                .unwrap()
                .iter()
                .map(|entry| entry.log_id.index)
                .collect();
            done.send(indices).unwrap();
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the reader task must start");
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(200))
                .is_err(),
            "a reader returned while the append was between its sync and its publication"
        );

        release.send(()).unwrap();
        append.await.unwrap();
        assert_eq!(
            done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("the reader must finish after the writer is released"),
            vec![0, 1, 2, 3],
            "the reader must observe the whole batch, never a partly inserted one"
        );
        read.await.unwrap();

        // The same complete batch is what a fresh handle recovers.
        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(
            readable_indices(&mut reopened).await,
            vec![0, 1, 2, 3]
        );
    }

    /// OpenRaft 0.9.25 contract rows: `get_log_state` returns the last present
    /// entry or `last_purged_log_id` when nothing is present
    /// (`storage/v2.rs:59-66`), and `LogState` satisfies
    /// `last_purged_log_id <= last_applied <= last_log_id`
    /// (`storage/mod.rs:138-149`).
    ///
    /// The state machine owns `last_applied`, so this store can only check the
    /// log side; the helper also checks that the reported view agrees with what
    /// the reader actually serves. The invariant is re-checked after every
    /// transition, including an automatic checkpoint and a reopen.
    #[tokio::test]
    async fn contract_log_state_invariants_hold_across_the_whole_lifecycle() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        assert_log_state_invariants(&mut store, "create").await;

        store.save_vote(&Vote::new(1, 1)).await.unwrap();
        assert_log_state_invariants(&mut store, "vote").await;

        store
            .append_durable(&Record::Append((0..=10).map(entry).collect()))
            .unwrap();
        assert_log_state_invariants(&mut store, "append").await;

        store.truncate(entry(7).log_id).await.unwrap();
        assert_log_state_invariants(&mut store, "truncate").await;

        store
            .append_durable(&Record::Append((7..=12).map(entry).collect()))
            .unwrap();
        assert_log_state_invariants(&mut store, "append after truncate").await;

        store.purge(entry(9).log_id).await.unwrap();
        assert_log_state_invariants(&mut store, "purge").await;

        // Saturate the frame budget so the next append rewrites the live image
        // as a checkpoint before appending. The invariant must survive the
        // compaction as well as the append.
        store.inner.lock().unwrap().frames = MAX_LOG_FRAMES;
        store
            .append_durable(&Record::Append(vec![entry(13)]))
            .unwrap();
        assert_log_state_invariants(&mut store, "checkpoint").await;

        let expected = image(&mut store).await;
        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_log_state_invariants(&mut reopened, "reopen").await;
        assert_eq!(image(&mut reopened).await, expected);
    }

    /// OpenRaft 0.9.25 contract row: `blocking_append` returns only after the
    /// `LogFlushed` callback fires (`storage/v2/raft_log_storage_ext.rs:21-40`),
    /// and the callback means the entries are persisted
    /// (`storage/callback.rs:32-46`, `storage/v2.rs:120`). The adapter fires it
    /// after `write_all` and `sync_data`, so a successful blocking append must
    /// be visible to a fresh handle with no recovery.
    #[tokio::test]
    async fn contract_append_callback_reports_only_persisted_entries() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .blocking_append(vec![entry(0), entry(1)])
            .await
            .unwrap();

        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(readable_indices(&mut reopened).await, vec![0, 1]);
    }

    /// The adapter's limits are stricter than anything OpenRaft specifies: it
    /// keeps at most `MAX_LIVE_ENTRIES` entries per append and a 16 MiB frame.
    /// A stricter bound is allowed, but it must refuse the whole request
    /// cleanly. Silently keeping the first `MAX_LIVE_ENTRIES` entries would
    /// acknowledge a batch OpenRaft never saw whole.
    #[tokio::test]
    async fn contract_entry_count_bound_is_refused_without_truncation_or_poison() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0)]))
            .unwrap();
        let durable = image(&mut store).await;
        let before = fs::read(dir.0.join("openraft.log")).unwrap();

        let too_many: Vec<_> = (1..=(MAX_LIVE_ENTRIES as u64 + 1)).map(entry).collect();
        let error = store.blocking_append(too_many).await.unwrap_err();
        assert!(
            format!("{error}").contains("exceeds entry limit"),
            "the entry-count bound must be reported, got {error}"
        );
        assert_eq!(
            fs::read(dir.0.join("openraft.log")).unwrap(),
            before,
            "a refused entry-count bound must not write a partial batch"
        );
        assert_eq!(image(&mut store).await, durable);
        assert_log_state_invariants(&mut store, "rejected entry count").await;

        // Not poisoned: the next valid append works and survives a reopen.
        store
            .append_durable(&Record::Append(vec![entry(1)]))
            .unwrap();
        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(readable_indices(&mut reopened).await, vec![0, 1]);
    }

    /// The frame-payload bound is checked by `encode` before any file IO. An
    /// entry that cannot fit must be refused whole rather than serialised in
    /// part, and the refusal must not poison the handle.
    #[tokio::test]
    async fn contract_frame_payload_bound_is_refused_before_any_io() {
        let dir = TempDir::new();
        let mut store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store
            .append_durable(&Record::Append(vec![entry(0)]))
            .unwrap();
        let durable = image(&mut store).await;
        let before = fs::read(dir.0.join("openraft.log")).unwrap();

        let oversized = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(KvCommand::Put {
                key: "k".repeat(MAX_FRAME_PAYLOAD),
                value: vec![1],
            }),
        };
        let error = store.blocking_append(vec![oversized]).await.unwrap_err();
        assert!(
            format!("{error}").contains("exceeds limit"),
            "the frame bound must be reported, got {error}"
        );
        assert_eq!(
            fs::read(dir.0.join("openraft.log")).unwrap(),
            before,
            "a refused frame must not leave a partial record"
        );
        assert_eq!(image(&mut store).await, durable);
        assert_log_state_invariants(&mut store, "rejected frame").await;

        // Not poisoned: the next valid append works and survives a reopen.
        store
            .append_durable(&Record::Append(vec![entry(1)]))
            .unwrap();
        drop(store);
        let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(readable_indices(&mut reopened).await, vec![0, 1]);
    }
}
