//! Experimental, test-only file-backed OpenRaft log and vote prototype.
//!
//! Not suitable for production: group identity, recovery, failure handling and
//! the OpenRaft storage contracts need further work and testing.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};
use yadb_io::OsDirectoryLock;

use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{Entry, ErrorSubject, ErrorVerb, LogId, LogState, StorageError, Vote};
use serde::{Deserialize, Serialize};

use crate::openraft_runtime::YadbRaftTypeConfig as C;
use crate::multiraft::ReplicaIdentity;

const MAGIC: &[u8; 8] = b"YORLOG02";
const IDENTITY_LEN: usize = 44;
const FRAME_MAGIC: &[u8; 4] = b"YORF";
const FRAME_HEADER_LEN: usize = 12;
const MAX_FRAME_PAYLOAD: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
enum Record {
    Vote(Vote<u32>),
    /// An append replaces any existing suffix from the first entry onward.
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
    _lock: OsDirectoryLock,
    state: State,
    failed: bool,
    #[cfg(test)]
    fault: Option<Fault>,
}

#[derive(Clone, Copy)]
enum Fault { WritePartial, SyncBefore, SyncAfter }

fn storage_error(subject: ErrorSubject<u32>, verb: ErrorVerb, error: io::Error) -> StorageError<u32> {
    StorageError::from_io_error(subject, verb, error)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn encode(record: &Record) -> io::Result<Vec<u8>> {
    let payload = serde_json::to_vec(record).map_err(|e| invalid(e.to_string()))?;
    if payload.len() > MAX_FRAME_PAYLOAD {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "OpenRaft record exceeds frame limit"));
    }
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "OpenRaft record length overflow"))?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(FRAME_MAGIC);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn append_frame(file: &mut File, record: &Record) -> io::Result<()> {
    let frame = encode(record)?;
    file.seek(SeekFrom::End(0))?;
    file.write_all(&frame)?;
    file.sync_data()
}

fn validate_entries(entries: &[Entry<C>]) -> io::Result<()> {
    for pair in entries.windows(2) {
        if pair[1].log_id.index != pair[0].log_id.index.saturating_add(1) {
            return Err(invalid("OpenRaft append contains a log-index hole"));
        }
    }
    Ok(())
}

fn validate_append_state(state: &State, entries: &[Entry<C>]) -> io::Result<()> {
    validate_entries(entries)?;
    let Some(first) = entries.first() else { return Ok(()); };
    let expected = state.log.last_key_value().map_or_else(
        || state.purged.map_or(0, |id| id.index.saturating_add(1)),
        |(_, entry)| entry.log_id.index.saturating_add(1),
    );
    if first.log_id.index > expected {
        return Err(invalid("OpenRaft append would leave a log-index hole"));
    }
    Ok(())
}

fn apply_record(state: &mut State, record: Record) -> io::Result<()> {
    match record {
        Record::Vote(vote) => state.vote = Some(vote),
        Record::Append(entries) => {
            validate_append_state(state, &entries)?;
            if let Some(first) = entries.first() {
                state.log.retain(|index, _| *index < first.log_id.index);
                for entry in entries { state.log.insert(entry.log_id.index, entry); }
            }
        }
        Record::Truncate(log_id) => {
            state.log.retain(|index, _| *index < log_id.index);
        }
        Record::Purge(log_id) => {
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

fn recover(file: &mut File, identity: &ReplicaIdentity) -> io::Result<State> {
    let mut bytes = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut bytes)?;
    let expected = header(identity);
    if bytes.len() < expected.len() || bytes[..expected.len()] != expected {
        return Err(invalid("invalid OpenRaft log header or replica identity"));
    }

    let mut state = State::default();
    let mut pos = expected.len();
    while pos < bytes.len() {
        if bytes.len() - pos < FRAME_HEADER_LEN || &bytes[pos..pos + 4] != FRAME_MAGIC {
            return Err(invalid("truncated or invalid OpenRaft log frame"));
        }
        let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
        if len > MAX_FRAME_PAYLOAD {
            return Err(invalid("OpenRaft log frame exceeds limit"));
        }
        let end = pos.checked_add(FRAME_HEADER_LEN).and_then(|n| n.checked_add(len))
            .ok_or_else(|| invalid("OpenRaft log frame length overflow"))?;
        if end > bytes.len() { return Err(invalid("truncated OpenRaft log frame")); }
        let payload = &bytes[pos + FRAME_HEADER_LEN..end];
        let expected = u32::from_le_bytes(bytes[pos + 8..pos + 12].try_into().unwrap());
        if crc32fast::hash(payload) != expected { return Err(invalid("OpenRaft log checksum mismatch")); }
        let record: Record = serde_json::from_slice(payload).map_err(|e| invalid(e.to_string()))?;
        apply_record(&mut state, record)?;
        pos = end;
    }
    file.seek(SeekFrom::End(0))?;
    Ok(state)
}

impl DurableOpenRaftLog {
    /// Explicitly create a new test log. Existing files are never overwritten.
    pub fn create(dir: impl AsRef<Path>, identity: &ReplicaIdentity) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let lock = OsDirectoryLock::try_acquire(&dir)?;
        let path = dir.join("openraft.log");
        let mut file = OpenOptions::new().create_new(true).read(true).append(true).open(&path)?;
        file.write_all(&header(identity))?;
        file.sync_data()?;
        File::open(&dir)?.sync_all()?;
        Ok(Self { inner: Arc::new(Mutex::new(Inner { file, _lock: lock, state: State::default(), failed: false, fault: None })) })
    }

    /// Reopen only an existing log for the same group incarnation and node.
    pub fn open(dir: impl AsRef<Path>, identity: &ReplicaIdentity) -> io::Result<Self> {
        let dir = dir.as_ref();
        let lock = OsDirectoryLock::try_acquire(dir)?;
        let mut file = OpenOptions::new().read(true).append(true).open(dir.join("openraft.log"))?;
        let state = recover(&mut file, identity)?;
        // A process-only restart may still see bytes whose previous sync failed.
        file.sync_data()?;
        File::open(&dir)?.sync_all()?;
        Ok(Self { inner: Arc::new(Mutex::new(Inner { file, _lock: lock, state, failed: false, fault: None })) })
    }

    fn append_durable(&self, record: &Record) -> Result<(), StorageError<u32>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.failed {
            return Err(storage_error(ErrorSubject::Store, ErrorVerb::Write, io::Error::other("storage requires recovery")));
        }
        if let Record::Append(entries) = record {
            validate_append_state(&inner.state, entries)
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
        }
        let result = match inner.fault.take() {
            Some(Fault::WritePartial) => encode(record).and_then(|frame| {
                inner.file.write_all(&frame[..frame.len() / 2])?;
                Err(io::Error::other("simulated partial write"))
            }),
            Some(Fault::SyncBefore) => encode(record).and_then(|frame| {
                inner.file.write_all(&frame)?;
                Err(io::Error::other("simulated sync failure before persistence"))
            }),
            Some(Fault::SyncAfter) => append_frame(&mut inner.file, record)
                .and_then(|_| Err(io::Error::other("simulated sync failure after persistence"))),
            None => append_frame(&mut inner.file, record),
        };
        if let Err(error) = result {
            inner.failed = true;
            return Err(storage_error(ErrorSubject::Logs, ErrorVerb::Write, error));
        }
        apply_record(&mut inner.state, record.clone())
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))
    }

    #[cfg(test)]
    fn read_entries(&self, start: u64, end: u64) -> Vec<Entry<C>> {
        self.inner.lock().unwrap().state.log.range(start..end).map(|(_, e)| e.clone()).collect()
    }
}

impl RaftLogReader<C> for DurableOpenRaftLog {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self, range: R,
    ) -> Result<Vec<Entry<C>>, StorageError<u32>> {
        let inner = self.inner.lock().unwrap();
        if inner.failed { return Err(storage_error(ErrorSubject::Store, ErrorVerb::Read, io::Error::other("storage requires recovery"))); }
        Ok(inner.state.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<C> for DurableOpenRaftLog {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u32>> {
        let inner = self.inner.lock().unwrap();
        if inner.failed { return Err(storage_error(ErrorSubject::Store, ErrorVerb::Read, io::Error::other("storage requires recovery"))); }
        Ok(LogState {
            last_purged_log_id: inner.state.purged,
            last_log_id: inner.state.log.last_key_value().map(|(_, e)| e.log_id).or(inner.state.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self { self.clone() }

    async fn save_vote(&mut self, vote: &Vote<u32>) -> Result<(), StorageError<u32>> {
        self.append_durable(&Record::Vote(*vote))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u32>>, StorageError<u32>> {
        let inner = self.inner.lock().unwrap();
        if inner.failed { return Err(storage_error(ErrorSubject::Store, ErrorVerb::Read, io::Error::other("storage requires recovery"))); }
        Ok(inner.state.vote)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<u32>>
    where I: IntoIterator<Item = Entry<C>> + Send, I::IntoIter: Send {
        let entries: Vec<_> = entries.into_iter().collect();
        validate_entries(&entries).map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
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
    use openraft::{CommittedLeaderId, EntryPayload};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let path = std::env::temp_dir().join(format!("yadb-openraft-{nonce}-{}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TempDir { fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); } }

    fn identity() -> ReplicaIdentity {
        ReplicaIdentity::new(crate::multiraft::GroupIdentity { cluster: [1; 16], group: 1, incarnation: 1 }, 1, vec![1]).unwrap()
    }

    fn entry(index: u64) -> Entry<C> {
        Entry { log_id: LogId::new(CommittedLeaderId::new(1, 1), index), payload: EntryPayload::Normal(KvCommand::Put {
            key: format!("k{index}"), value: vec![index as u8],
        }) }
    }

    #[tokio::test]
    async fn vote_and_log_survive_restart_and_suffix_replacement() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store.append_durable(&Record::Vote(Vote::new(4, 2))).unwrap();
        store.append_durable(&Record::Append(vec![entry(0), entry(1), entry(2), entry(3)])).unwrap();
        store.append_durable(&Record::Append(vec![entry(2)])).unwrap();
        drop(store);
        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        let mut reopened_reader = reopened.clone();
        assert_eq!(reopened_reader.read_vote().await.unwrap(), Some(Vote::new(4, 2)));
        let entries = reopened.read_entries(1, 4);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].log_id.index, 1);
        assert_eq!(entries[1].log_id.index, 2);
    }

    #[test]
    fn truncated_or_corrupt_tail_fails_closed() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store.append_durable(&Record::Vote(Vote::new(1, 1))).unwrap();
        drop(store);
        let path = dir.0.join("openraft.log");
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"YORF\x08\x00").unwrap();
        drop(file);
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_err());
    }

    #[test]
    fn append_with_a_hole_is_rejected_without_changing_memory() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store.append_durable(&Record::Append(vec![entry(0), entry(1)])).unwrap();
        assert!(store.append_durable(&Record::Append(vec![entry(3)] )).is_err());
        assert_eq!(store.read_entries(1, 4).len(), 1);
        drop(store);
        let reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
        assert_eq!(reopened.read_entries(1, 4).len(), 1);
    }

    #[test]
    fn second_open_is_rejected_while_first_handle_is_alive() {
        let dir = TempDir::new();
        let first = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_err());
        drop(first);
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_ok());
    }

    #[test]
    fn partial_write_poisoned_until_reopen_which_fails_closed() {
        let dir = TempDir::new();
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store.append_durable(&Record::Vote(Vote::new(1, 1))).unwrap();
        store.inner.lock().unwrap().fault = Some(Fault::WritePartial);
        assert!(store.append_durable(&Record::Vote(Vote::new(2, 1))).is_err());
        assert!(store.append_durable(&Record::Vote(Vote::new(3, 1))).is_err());
        drop(store);
        let path = dir.0.join("openraft.log");
        let before = fs::read(&path).unwrap();
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[tokio::test]
    async fn sync_errors_poison_writer_and_reopen_resyncs_visible_record() {
        for fault in [Fault::SyncBefore, Fault::SyncAfter] {
            let dir = TempDir::new();
            let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
            store.append_durable(&Record::Vote(Vote::new(1, 1))).unwrap();
            store.inner.lock().unwrap().fault = Some(fault);
            assert!(store.append_durable(&Record::Vote(Vote::new(2, 1))).is_err());
            assert!(store.append_durable(&Record::Vote(Vote::new(3, 1))).is_err());
            drop(store);
            let mut reopened = DurableOpenRaftLog::open(&dir.0, &identity()).unwrap();
            assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
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
    fn wrong_identity_and_missing_log_never_modify_data() {
        let dir = TempDir::new();
        assert!(DurableOpenRaftLog::open(&dir.0, &identity()).is_err());
        let store = DurableOpenRaftLog::create(&dir.0, &identity()).unwrap();
        store.append_durable(&Record::Vote(Vote::new(1, 1))).unwrap();
        drop(store);
        let path = dir.0.join("openraft.log");
        let before = fs::read(&path).unwrap();
        let base = identity().group().clone();
        let variants = [
            ReplicaIdentity::new(crate::multiraft::GroupIdentity { cluster: [2; 16], ..base.clone() }, 1, vec![1]).unwrap(),
            ReplicaIdentity::new(crate::multiraft::GroupIdentity { group: 2, ..base.clone() }, 1, vec![1]).unwrap(),
            ReplicaIdentity::new(crate::multiraft::GroupIdentity { incarnation: 2, ..base.clone() }, 1, vec![1]).unwrap(),
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
}
