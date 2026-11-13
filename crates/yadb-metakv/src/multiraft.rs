//! Experimental identity, descriptor and envelope validation; not a Raft runtime.
//!
//! An experimental framed WAL binds identity but does not interpret Raft records.
//! This module does not authenticate senders, validate Raft log structure,
//! or make `RaftCore` safe to expose to untrusted messages. It is deliberately
//! disconnected from the daemon and existing metadata traffic.

use serde::{Deserialize, Serialize};
use yadb_io::{OsDirectoryLock, OsFs, StorageFs, VirtualFile};

use crate::RaftRpc;

pub const PROTOCOL_VERSION: u16 = 1;

/// All fields are part of the identity; IDs must never be reused by the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupIdentity {
    pub cluster: [u8; 16],
    pub group: u64,
    pub incarnation: u64,
}

/// Fixed configuration for one replica. Construction rejects ambiguous input
/// rather than silently changing membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaIdentity {
    group: GroupIdentity,
    node: u32,
    voters: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    InvalidIdentity,
    InvalidVoters,
    WrongVersion,
    WrongGroup,
    WrongRecipient,
    InvalidSender,
    SenderMismatch,
}

impl ReplicaIdentity {
    pub fn new(group: GroupIdentity, node: u32, voters: Vec<u32>) -> Result<Self, IdentityError> {
        if group.cluster == [0; 16] || group.group == 0 || group.incarnation == 0 || node == 0 {
            return Err(IdentityError::InvalidIdentity);
        }
        if voters.is_empty() || voters.len() > 1024 || voters.iter().any(|&id| id == 0)
            || voters.windows(2).any(|pair| pair[0] >= pair[1])
            || voters.binary_search(&node).is_err()
        {
            return Err(IdentityError::InvalidVoters);
        }
        Ok(Self { group, node, voters })
    }

    pub fn voters(&self) -> &[u32] { &self.voters }
    pub fn group(&self) -> &GroupIdentity { &self.group }
    pub fn node(&self) -> u32 { self.node }

    /// Validate routing on a trusted transport before any Raft state mutation.
    /// This alone is insufficient to authorize calling `RaftCore::step`.
    /// A transport must also bind the peer connection to `envelope.sender`.
    pub fn validate(&self, envelope: &GroupEnvelope, peer: u32) -> Result<(), IdentityError> {
        if envelope.version != PROTOCOL_VERSION { return Err(IdentityError::WrongVersion); }
        if envelope.group != self.group { return Err(IdentityError::WrongGroup); }
        if envelope.recipient != self.node { return Err(IdentityError::WrongRecipient); }
        if peer != envelope.sender || peer == self.node || self.voters.binary_search(&peer).is_err() {
            return Err(IdentityError::InvalidSender);
        }
        let claimed = match &envelope.rpc {
            RaftRpc::RequestVote { candidate_id, .. } => *candidate_id,
            RaftRpc::RequestVoteReply { from, .. } => *from,
            RaftRpc::AppendEntries { leader_id, .. } => *leader_id,
            RaftRpc::AppendEntriesReply { from, .. } => *from,
        };
        if claimed != peer { return Err(IdentityError::SenderMismatch); }
        Ok(())
    }
}

const DESCRIPTOR_MAGIC: &[u8; 8] = b"YMRID001";
const DESCRIPTOR_FILE: &str = "replica.identity";

#[derive(Debug, thiserror::Error)]
pub enum DescriptorError {
    #[error("replica directory is not empty")]
    DirectoryNotEmpty,
    #[error("replica identity does not match or is corrupt")]
    Mismatch,
    #[error("replica identity I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// OS-only experimental descriptor owner. Holds an advisory directory lock for
/// its lifetime; all users must cooperate and must not replace the directory.
/// This does not provide WAL ownership, replay, or a runnable replica.
pub struct LockedDescriptor {
    _lock: OsDirectoryLock,
}

impl LockedDescriptor {
    pub fn bootstrap(fs: &OsFs, dir: &str, identity: &ReplicaIdentity) -> Result<Self, DescriptorError> {
        let lock = fs.try_lock_directory(dir)?;
        bootstrap_descriptor(fs, dir, identity)?;
        Ok(Self { _lock: lock })
    }

    pub fn open(fs: &OsFs, dir: &str, identity: &ReplicaIdentity) -> Result<Self, DescriptorError> {
        let lock = fs.try_lock_directory(dir)?;
        verify_descriptor(fs, dir, identity)?;
        Ok(Self { _lock: lock })
    }
}

fn descriptor_bytes(identity: &ReplicaIdentity) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(DESCRIPTOR_MAGIC);
    bytes.extend_from_slice(&identity.group.cluster);
    bytes.extend_from_slice(&identity.group.group.to_le_bytes());
    bytes.extend_from_slice(&identity.group.incarnation.to_le_bytes());
    bytes.extend_from_slice(&identity.node.to_le_bytes());
    bytes.extend_from_slice(&(identity.voters.len() as u32).to_le_bytes());
    for voter in &identity.voters { bytes.extend_from_slice(&voter.to_le_bytes()); }
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

const GROUP_WAL_MAGIC: &[u8; 8] = b"YMRWAL01";
const GROUP_WAL_FILE: &str = "group.wal";

fn group_wal_header(identity: &ReplicaIdentity) -> Vec<u8> {
    let mut bytes = descriptor_bytes(identity);
    bytes[..8].copy_from_slice(GROUP_WAL_MAGIC);
    let checksum_offset = bytes.len() - 4;
    let checksum = crc32fast::hash(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn group_wal_path(dir: &str) -> String {
    if dir.is_empty() { GROUP_WAL_FILE.to_owned() } else {
        format!("{}/{GROUP_WAL_FILE}", dir.trim_end_matches('/'))
    }
}

/// Experimental OS-only ownership of a provisioned, empty, identity-bound WAL.
/// It cannot append or replay records, and cannot be used by `RaftCore`. The
/// directory and its parents must already be durable, and all access must respect
/// the advisory lock and refrain from renaming/replacing the directory or file.
pub struct LockedEmptyGroupWal {
    _lock: OsDirectoryLock,
}

impl LockedEmptyGroupWal {
    /// Explicit first provisioning only. An error has an unknown durability
    /// outcome; never automatically retry bootstrap or delete existing files.
    pub fn bootstrap(fs: &OsFs, dir: &str, identity: &ReplicaIdentity) -> Result<Self, DescriptorError> {
        let lock = fs.try_lock_directory(dir)?;
        bootstrap_empty_wal(fs, dir, identity)?;
        Ok(Self { _lock: lock })
    }

    /// Never creates an absent WAL. Rejects any extra bytes, including records
    /// in a future format, before any write or replay.
    pub fn open(fs: &OsFs, dir: &str, identity: &ReplicaIdentity) -> Result<Self, DescriptorError> {
        let lock = fs.try_lock_directory(dir)?;
        verify_empty_wal(fs, dir, identity)?;
        Ok(Self { _lock: lock })
    }
}

fn bootstrap_empty_wal<FS: StorageFs>(fs: &FS, dir: &str, identity: &ReplicaIdentity) -> Result<(), DescriptorError> {
    if !fs.list_files(dir)?.is_empty() || fs.exists(&group_wal_path(dir)) {
        return Err(DescriptorError::DirectoryNotEmpty);
    }
    let file = fs.open_rw(&group_wal_path(dir), true)?;
    file.write_at(0, &group_wal_header(identity))?;
    file.sync_data()?;
    fs.sync_dir(dir)?;
    Ok(())
}

fn verify_empty_wal<FS: StorageFs>(fs: &FS, dir: &str, identity: &ReplicaIdentity) -> Result<(), DescriptorError> {
    let file = fs.open_rw(&group_wal_path(dir), false)?;
    let expected = group_wal_header(identity);
    if file.len()? != expected.len() as u64 { return Err(DescriptorError::Mismatch); }
    let mut actual = vec![0; expected.len()];
    if file.read_at(0, &mut actual)? != actual.len() || actual != expected {
        return Err(DescriptorError::Mismatch);
    }
    file.sync_data()?;
    fs.sync_dir(dir)?;
    Ok(())
}

const FRAME_MAGIC: &[u8; 4] = b"YMF1";
const FRAME_HEADER: usize = 12;
const MAX_RECORD_BYTES: usize = 1024 * 1024;
const MAX_WAL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RECORDS: usize = 65_536;

#[derive(Debug, thiserror::Error)]
pub enum GroupWalError {
    #[error("group WAL identity mismatch or malformed frame")]
    Corrupt,
    #[error("group WAL size limit exceeded")]
    Limit,
    #[error("group WAL unavailable after an I/O error; reopen required")]
    Unavailable,
    #[error("group WAL I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// A bounded experimental framed log, not a Raft WAL implementation. Records
/// are opaque: callers cannot assume Raft term, vote, index or commit validation.
/// All cooperating users must respect the advisory lock; directory replacement,
/// external modification and arbitrary storage corruption are outside its model.
pub struct LockedGroupWal {
    _lock: OsDirectoryLock,
    inner: FramedGroupWal<<OsFs as StorageFs>::File>,
}

struct FramedGroupWal<F: VirtualFile> {
    file: F,
    end: u64,
    records: usize,
    failed: bool,
}

impl LockedGroupWal {
    /// Opens an existing provisioned WAL only; never bootstraps on failure.
    /// The recovered records are untrusted bytes, not applied Raft state.
    pub fn open(fs: &OsFs, dir: &str, identity: &ReplicaIdentity) -> Result<(Self, Vec<Vec<u8>>), GroupWalError> {
        let lock = fs.try_lock_directory(dir)?;
        let file = fs.open_rw(&group_wal_path(dir), false)?;
        let (end, records) = replay_group_wal(&file, identity)?;
        // Re-establish durability after a process-only restart before reuse.
        file.sync_data()?;
        fs.sync_dir(dir)?;
        Ok((Self { _lock: lock, inner: FramedGroupWal { file, end, records: records.len(), failed: false } }, records))
    }

    /// An I/O error has an unknown outcome and poisons this handle. A complete
    /// recovered frame can survive; a partial frame makes reopen fail closed.
    pub fn append(&mut self, payload: &[u8]) -> Result<(), GroupWalError> {
        self.inner.append(payload)
    }
}

impl<F: VirtualFile> FramedGroupWal<F> {
    fn append(&mut self, payload: &[u8]) -> Result<(), GroupWalError> {
        if self.failed { return Err(GroupWalError::Unavailable); }
        if self.records >= MAX_RECORDS { return Err(GroupWalError::Limit); }
        let frame = encode_group_frame(payload, self.end)?;
        if let Err(e) = self.file.write_at(self.end, &frame).and_then(|_| self.file.sync_data()) {
            self.failed = true;
            return Err(e.into());
        }
        self.end += frame.len() as u64;
        self.records += 1;
        Ok(())
    }
}

fn encode_group_frame(payload: &[u8], end: u64) -> Result<Vec<u8>, GroupWalError> {
    if payload.len() > MAX_RECORD_BYTES || end.checked_add(FRAME_HEADER as u64 + payload.len() as u64)
        .filter(|&n| n <= MAX_WAL_BYTES).is_none() { return Err(GroupWalError::Limit); }
    let mut frame = Vec::with_capacity(FRAME_HEADER + payload.len());
    frame.extend_from_slice(FRAME_MAGIC);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn replay_group_wal<F: VirtualFile>(file: &F, identity: &ReplicaIdentity) -> Result<(u64, Vec<Vec<u8>>), GroupWalError> {
    let len = file.len()?;
    if len > MAX_WAL_BYTES { return Err(GroupWalError::Limit); }
    let expected = group_wal_header(identity);
    if len < expected.len() as u64 { return Err(GroupWalError::Corrupt); }
    let mut header = vec![0; expected.len()];
    if file.read_at(0, &mut header)? != header.len() || header != expected { return Err(GroupWalError::Corrupt); }
    let mut pos = expected.len() as u64;
    let mut records = Vec::new();
    while pos < len {
        if records.len() >= MAX_RECORDS { return Err(GroupWalError::Limit); }
        if len - pos < FRAME_HEADER as u64 { return Err(GroupWalError::Corrupt); }
        let mut hdr = [0u8; FRAME_HEADER];
        if file.read_at(pos, &mut hdr)? != hdr.len() || &hdr[..4] != FRAME_MAGIC { return Err(GroupWalError::Corrupt); }
        let size = u32::from_le_bytes(hdr[4..8].try_into().unwrap()) as usize;
        if size > MAX_RECORD_BYTES || size as u64 > len - pos - FRAME_HEADER as u64 { return Err(GroupWalError::Corrupt); }
        let mut payload = vec![0; size];
        if file.read_at(pos + FRAME_HEADER as u64, &mut payload)? != size
            || crc32fast::hash(&payload) != u32::from_le_bytes(hdr[8..12].try_into().unwrap()) {
            return Err(GroupWalError::Corrupt);
        }
        records.push(payload);
        pos += FRAME_HEADER as u64 + size as u64;
    }
    Ok((pos, records))
}

/// Versioned experimental records; no snapshots, membership changes or Raft
/// protocol are implemented. Commit is recorded explicitly before application.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum GroupRaftRecord {
    HardState { term: u64, voted_for: Option<u32> },
    ReplaceSuffix { from_index: u64, entries: Vec<crate::LogEntry> },
    Commit { index: u64 },
}

#[derive(Debug, Clone, Default)]
pub struct GroupRaftState {
    pub term: u64,
    pub voted_for: Option<u32>,
    pub log: Vec<crate::LogEntry>,
    pub commit_index: u64,
}

impl GroupRaftState {
    fn apply(&mut self, record: &GroupRaftRecord, identity: &ReplicaIdentity) -> Result<(), GroupWalError> {
        match record {
            GroupRaftRecord::HardState { term, voted_for } => {
                if *term < self.term || (*term == 0 && voted_for.is_some())
                    || voted_for.is_some_and(|v| identity.voters.binary_search(&v).is_err())
                    || (*term == self.term && self.voted_for.is_some() && *voted_for != self.voted_for) {
                    return Err(GroupWalError::Corrupt);
                }
                self.term = *term;
                self.voted_for = *voted_for;
            }
            GroupRaftRecord::ReplaceSuffix { from_index, entries } => {
                let keep = usize::try_from(from_index.checked_sub(1).ok_or(GroupWalError::Corrupt)?)
                    .map_err(|_| GroupWalError::Corrupt)?;
                if keep > self.log.len() || *from_index <= self.commit_index { return Err(GroupWalError::Corrupt); }
                let mut previous_term = if keep == 0 { 0 } else { self.log[keep - 1].term };
                for (offset, entry) in entries.iter().enumerate() {
                    let index = from_index.checked_add(offset as u64).ok_or(GroupWalError::Corrupt)?;
                    if entry.index != index || entry.term == 0 || entry.term < previous_term || entry.term > self.term {
                        return Err(GroupWalError::Corrupt);
                    }
                    if let Some(existing) = self.log.get(keep + offset) {
                        // Log matching: the same (term,index) must never refer
                        // to two different commands, even in an uncommitted suffix.
                        if existing.term == entry.term && existing != entry {
                            return Err(GroupWalError::Corrupt);
                        }
                    }
                    previous_term = entry.term;
                }
                self.log.truncate(keep);
                self.log.extend_from_slice(entries);
            }
            GroupRaftRecord::Commit { index } => {
                if *index < self.commit_index || *index > self.log.len() as u64 { return Err(GroupWalError::Corrupt); }
                self.commit_index = *index;
            }
        }
        Ok(())
    }
}

/// Typed validation layer over the isolated WAL. It does not establish quorum,
/// apply the state machine, or make a runnable Raft group.
pub struct TypedGroupWal {
    wal: LockedGroupWal,
    identity: ReplicaIdentity,
    state: GroupRaftState,
}

impl TypedGroupWal {
    pub fn open(fs: &OsFs, dir: &str, identity: &ReplicaIdentity) -> Result<Self, GroupWalError> {
        let (wal, records) = LockedGroupWal::open(fs, dir, identity)?;
        let mut state = GroupRaftState::default();
        for bytes in records {
            let record: GroupRaftRecord = serde_json::from_slice(&bytes).map_err(|_| GroupWalError::Corrupt)?;
            state.apply(&record, identity)?;
        }
        Ok(Self { wal, identity: identity.clone(), state })
    }

    pub fn state(&self) -> &GroupRaftState { &self.state }

    pub fn append(&mut self, record: &GroupRaftRecord) -> Result<(), GroupWalError> {
        let mut next = self.state.clone();
        next.apply(record, &self.identity)?;
        let bytes = serde_json::to_vec(record).map_err(|_| GroupWalError::Corrupt)?;
        self.wal.append(&bytes)?;
        self.state = next;
        Ok(())
    }
}

fn descriptor_path(dir: &str) -> String {
    if dir.is_empty() { DESCRIPTOR_FILE.to_owned() } else {
        format!("{}/{DESCRIPTOR_FILE}", dir.trim_end_matches('/'))
    }
}

/// Provision an explicitly new, empty directory; never call as an automatic
/// fallback after a failed open or an ambiguous bootstrap. The caller MUST hold
/// exclusive ownership across this call and all subsequent use, and must have
/// durably created the directory and its parent entries. `StorageFs` does not
/// provide atomic create-new or locking, so this API is not a runtime lock.
/// An error may leave a partial or durable descriptor and requires investigation.
pub fn bootstrap_descriptor<FS: StorageFs>(fs: &FS, dir: &str, identity: &ReplicaIdentity) -> Result<(), DescriptorError> {
    if !fs.list_files(dir)?.is_empty() || fs.exists(&descriptor_path(dir)) {
        return Err(DescriptorError::DirectoryNotEmpty);
    }
    let file = fs.open_rw(&descriptor_path(dir), true)?;
    file.write_at(0, &descriptor_bytes(identity))?;
    file.sync_data()?;
    fs.sync_dir(dir)?;
    Ok(())
}

/// Match the complete canonical descriptor without changing it. A successful
/// check also syncs its bytes and directory entry, including after a process-only
/// restart following an ambiguous bootstrap. This does not validate any WAL.
pub fn verify_descriptor<FS: StorageFs>(fs: &FS, dir: &str, identity: &ReplicaIdentity) -> Result<(), DescriptorError> {
    let file = fs.open_rw(&descriptor_path(dir), false)?;
    let expected = descriptor_bytes(identity);
    if file.len()? != expected.len() as u64 { return Err(DescriptorError::Mismatch); }
    let mut actual = vec![0; expected.len()];
    if file.read_at(0, &mut actual)? != actual.len() || actual != expected {
        return Err(DescriptorError::Mismatch);
    }
    file.sync_data()?;
    fs.sync_dir(dir)?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupEnvelope {
    pub version: u16,
    pub group: GroupIdentity,
    pub sender: u32,
    pub recipient: u32,
    pub rpc: RaftRpc,
}

#[cfg(test)]
mod tests {
    use super::*;
    use yadb_io::{FaultConfig, SimFs, SyncFault};

    fn group() -> GroupIdentity { GroupIdentity { cluster: [1; 16], group: 1, incarnation: 1 } }
    fn replica() -> ReplicaIdentity { ReplicaIdentity::new(group(), 1, vec![1, 2, 3]).unwrap() }
    fn envelope() -> GroupEnvelope {
        GroupEnvelope { version: PROTOCOL_VERSION, group: group(), sender: 2, recipient: 1,
            rpc: RaftRpc::RequestVoteReply { from: 2, term: 1, vote_granted: true } }
    }

    #[test]
    fn configuration_is_strict_and_canonical() {
        for voters in [vec![], vec![1, 1, 2], vec![2, 1], vec![2, 3], vec![0, 1, 2]] {
            assert_eq!(ReplicaIdentity::new(group(), 1, voters), Err(IdentityError::InvalidVoters));
        }
        assert_eq!(ReplicaIdentity::new(group(), 0, vec![1]), Err(IdentityError::InvalidIdentity));
        assert_eq!(ReplicaIdentity::new(GroupIdentity { incarnation: 0, ..group() }, 1, vec![1]), Err(IdentityError::InvalidIdentity));
        assert_eq!(replica().voters(), &[1, 2, 3]);
    }

    #[test]
    fn rejects_wrong_identity_version_and_routing() {
        let r = replica();
        assert_eq!(r.validate(&envelope(), 2), Ok(()));
        let mut e = envelope(); e.version += 1;
        assert_eq!(r.validate(&e, 2), Err(IdentityError::WrongVersion));
        for identity in [GroupIdentity { cluster: [2; 16], ..group() },
            GroupIdentity { group: 2, ..group() }, GroupIdentity { incarnation: 2, ..group() }] {
            let mut e = envelope(); e.group = identity;
            assert_eq!(r.validate(&e, 2), Err(IdentityError::WrongGroup));
        }
        let mut e = envelope(); e.recipient = 3;
        assert_eq!(r.validate(&e, 2), Err(IdentityError::WrongRecipient));
        let mut e = envelope(); e.sender = 999;
        assert_eq!(r.validate(&e, 999), Err(IdentityError::InvalidSender));
        assert_eq!(r.validate(&envelope(), 3), Err(IdentityError::InvalidSender));
    }

    #[test]
    fn all_rpc_claims_must_match_the_transport_peer() {
        let r = replica();
        for rpc in [
            RaftRpc::RequestVote { term: 1, candidate_id: 3, last_log_index: 0, last_log_term: 0 },
            RaftRpc::RequestVoteReply { term: 1, from: 3, vote_granted: true },
            RaftRpc::AppendEntries { term: 1, leader_id: 3, prev_log_index: 0, prev_log_term: 0, entries: vec![], leader_commit: 0 },
            RaftRpc::AppendEntriesReply { term: 1, from: 3, success: true, match_index: 0 },
        ] {
            let mut e = envelope(); e.rpc = rpc;
            assert_eq!(r.validate(&e, 2), Err(IdentityError::SenderMismatch));
        }
    }

    #[test]
    fn descriptor_survives_restart_and_mismatches_never_modify_it() {
        let fs = SimFs::new(31);
        fs.create_dir_all("group").unwrap();
        bootstrap_descriptor(&fs, "group", &replica()).unwrap();
        fs.simulate_power_cut();
        verify_descriptor(&fs, "group", &replica()).unwrap();
        let different = [
            ReplicaIdentity::new(GroupIdentity { group: 2, ..group() }, 1, vec![1, 2, 3]).unwrap(),
            ReplicaIdentity::new(GroupIdentity { cluster: [2; 16], ..group() }, 1, vec![1, 2, 3]).unwrap(),
            ReplicaIdentity::new(GroupIdentity { incarnation: 2, ..group() }, 1, vec![1, 2, 3]).unwrap(),
            ReplicaIdentity::new(group(), 2, vec![1, 2, 3]).unwrap(),
            ReplicaIdentity::new(group(), 1, vec![1]).unwrap(),
        ];
        let file = fs.open_rw("group/replica.identity", false).unwrap();
        let mut before = vec![0; file.len().unwrap() as usize];
        file.read_at(0, &mut before).unwrap();
        for wrong in different {
            assert!(matches!(verify_descriptor(&fs, "group", &wrong), Err(DescriptorError::Mismatch)));
            let mut after = vec![0; before.len()];
            file.read_at(0, &mut after).unwrap();
            assert_eq!(before, after);
        }
        assert!(matches!(bootstrap_descriptor(&fs, "group", &replica()), Err(DescriptorError::DirectoryNotEmpty)));
        fs.corrupt_byte("group/replica.identity", 12).unwrap();
        assert!(matches!(verify_descriptor(&fs, "group", &replica()), Err(DescriptorError::Mismatch)));
    }

    #[test]
    fn bootstrap_is_explicit_and_refuses_legacy_wal() {
        let fs = SimFs::new(32);
        fs.create_dir_all("group").unwrap();
        fs.open_rw("group/raft.wal", true).unwrap().write_at(0, b"YADBWAL1").unwrap();
        assert!(matches!(bootstrap_descriptor(&fs, "group", &replica()), Err(DescriptorError::DirectoryNotEmpty)));
        assert!(matches!(verify_descriptor(&fs, "group", &replica()), Err(DescriptorError::Io(_))));
    }

    #[test]
    fn os_descriptor_owner_excludes_second_owner() {
        let base = std::env::var_os("YADB_TEST_TMPDIR").map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = base.join(format!("yadb-descriptor-lock-{}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        let fs = OsFs::new(&dir).unwrap();
        let first = LockedDescriptor::bootstrap(&fs, "", &replica()).unwrap();
        assert!(LockedDescriptor::open(&fs, "", &replica()).is_err());
        drop(first);
        let second = LockedDescriptor::open(&fs, "", &replica()).unwrap();
        drop(second);
        std::fs::remove_file(dir.join(DESCRIPTOR_FILE)).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn wal_header_binds_full_identity_without_modifying_mismatches() {
        let fs = SimFs::new(901);
        fs.create_dir_all("g").unwrap();
        bootstrap_empty_wal(&fs, "g", &replica()).unwrap();
        fs.simulate_power_cut();
        verify_empty_wal(&fs, "g", &replica()).unwrap();
        let file = fs.open_rw("g/group.wal", false).unwrap();
        let mut original = vec![0; file.len().unwrap() as usize];
        file.read_at(0, &mut original).unwrap();
        for wrong in [
            ReplicaIdentity::new(GroupIdentity { cluster: [2; 16], ..group() }, 1, vec![1, 2, 3]).unwrap(),
            ReplicaIdentity::new(GroupIdentity { group: 2, ..group() }, 1, vec![1, 2, 3]).unwrap(),
            ReplicaIdentity::new(GroupIdentity { incarnation: 2, ..group() }, 1, vec![1, 2, 3]).unwrap(),
            ReplicaIdentity::new(group(), 2, vec![1, 2, 3]).unwrap(),
            ReplicaIdentity::new(group(), 1, vec![1]).unwrap(),
        ] {
            assert!(matches!(verify_empty_wal(&fs, "g", &wrong), Err(DescriptorError::Mismatch)));
            let mut after = vec![0; original.len()];
            file.read_at(0, &mut after).unwrap();
            assert_eq!(after, original);
        }
        file.write_at(original.len() as u64, b"record").unwrap();
        assert!(matches!(verify_empty_wal(&fs, "g", &replica()), Err(DescriptorError::Mismatch)));
        assert_eq!(file.len().unwrap(), original.len() as u64 + 6);
        fs.corrupt_byte("g/group.wal", 12).unwrap();
        assert!(verify_empty_wal(&fs, "g", &replica()).is_err());
    }

    #[test]
    fn framed_wal_fails_closed_on_truncation_and_corruption() {
        let fs = SimFs::new(904);
        fs.create_dir_all("g").unwrap();
        bootstrap_empty_wal(&fs, "g", &replica()).unwrap();
        let file = fs.open_rw("g/group.wal", false).unwrap();
        let offset = file.len().unwrap();
        let first = encode_group_frame(b"one", offset).unwrap();
        let second = encode_group_frame(b"two", offset + first.len() as u64).unwrap();
        file.write_at(offset, &first).unwrap();
        file.write_at(offset + first.len() as u64, &second).unwrap();
        file.sync_data().unwrap();
        let (_, records) = replay_group_wal(&file, &replica()).unwrap();
        assert_eq!(records, vec![b"one".to_vec(), b"two".to_vec()]);
        assert!(replay_group_wal(&file, &ReplicaIdentity::new(group(), 1, vec![1]).unwrap()).is_err());
        let len = file.len().unwrap();
        fs.corrupt_byte("g/group.wal", (offset + FRAME_HEADER as u64) as usize).unwrap();
        assert!(matches!(replay_group_wal(&file, &replica()), Err(GroupWalError::Corrupt)));
        assert_eq!(file.len().unwrap(), len);
        fs.corrupt_byte("g/group.wal", (offset + FRAME_HEADER as u64) as usize).unwrap();
        file.set_len(len - 1).unwrap();
        assert!(matches!(replay_group_wal(&file, &replica()), Err(GroupWalError::Corrupt)));
        assert_eq!(file.len().unwrap(), len - 1);
        assert!(matches!(encode_group_frame(&vec![0; MAX_RECORD_BYTES + 1], offset), Err(GroupWalError::Limit)));
    }

    #[test]
    fn framed_wal_faults_poison_writer_and_preserve_unknown_outcomes() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(906);
            fs.create_dir_all("g").unwrap();
            bootstrap_empty_wal(&fs, "g", &replica()).unwrap();
            let file = fs.open_rw("g/group.wal", false).unwrap();
            let mut wal = FramedGroupWal { end: file.len().unwrap(), file, records: 0, failed: false };
            wal.append(b"committed").unwrap();
            fs.set_faults(FaultConfig { sync_data_fault: fault, ..Default::default() });
            assert!(matches!(wal.append(b"unknown"), Err(GroupWalError::Io(_))));
            fs.set_faults(FaultConfig::default());
            assert!(matches!(wal.append(b"forbidden"), Err(GroupWalError::Unavailable)));
            drop(wal);
            fs.simulate_power_cut();
            let file = fs.open_rw("g/group.wal", false).unwrap();
            let (_, records) = replay_group_wal(&file, &replica()).unwrap();
            let expected = if fault == SyncFault::AfterPersist {
                vec![b"committed".to_vec(), b"unknown".to_vec()]
            } else { vec![b"committed".to_vec()] };
            assert_eq!(records, expected);
        }
        for torn in [false, true] {
            let fs = SimFs::new(907);
            fs.create_dir_all("g").unwrap();
            bootstrap_empty_wal(&fs, "g", &replica()).unwrap();
            let file = fs.open_rw("g/group.wal", false).unwrap();
            let mut wal = FramedGroupWal { end: file.len().unwrap(), file, records: 0, failed: false };
            fs.set_faults(if torn { FaultConfig { torn_write_prob: 1.0, ..Default::default() } }
                else { FaultConfig { write_eio_prob: 1.0, ..Default::default() } });
            assert!(matches!(wal.append(b"entry"), Err(GroupWalError::Io(_))));
            fs.set_faults(FaultConfig::default());
            assert!(matches!(wal.append(b"forbidden"), Err(GroupWalError::Unavailable)));
            let result = replay_group_wal(&wal.file, &replica());
            if torn { assert!(matches!(result, Err(GroupWalError::Corrupt))); }
            else { assert!(result.unwrap().1.is_empty()); }
        }
    }

    #[test]
    fn os_framed_wal_recovers_records_and_holds_lock() {
        let base = std::env::var_os("YADB_TEST_TMPDIR").map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = base.join(format!("yadb-group-wal-{}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        let fs = OsFs::new(&dir).unwrap();
        drop(LockedEmptyGroupWal::bootstrap(&fs, "", &replica()).unwrap());
        let (mut wal, records) = LockedGroupWal::open(&fs, "", &replica()).unwrap();
        assert!(records.is_empty());
        wal.append(b"first").unwrap();
        wal.append(b"second").unwrap();
        assert!(LockedGroupWal::open(&fs, "", &replica()).is_err());
        drop(wal);
        let (wal, records) = LockedGroupWal::open(&fs, "", &replica()).unwrap();
        assert_eq!(records, vec![b"first".to_vec(), b"second".to_vec()]);
        drop(wal);
        std::fs::remove_file(dir.join(GROUP_WAL_FILE)).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn typed_raft_records_enforce_vote_log_and_commit_invariants() {
        use GroupRaftRecord::*;
        let identity = replica();
        let mut state = GroupRaftState::default();
        let entry = |index, term| crate::LogEntry { index, term,
            cmd: crate::KvCommand::Put { key: "k".into(), value: vec![1] } };
        state.apply(&HardState { term: 1, voted_for: Some(1) }, &identity).unwrap();
        assert!(state.apply(&HardState { term: 1, voted_for: Some(2) }, &identity).is_err());
        assert!(state.apply(&HardState { term: 1, voted_for: None }, &identity).is_err());
        assert!(state.apply(&HardState { term: 2, voted_for: Some(999) }, &identity).is_err());
        assert!(state.apply(&ReplaceSuffix { from_index: 0, entries: vec![entry(0, 1)] }, &identity).is_err());
        assert!(state.apply(&ReplaceSuffix { from_index: 2, entries: vec![entry(2, 1)] }, &identity).is_err());
        assert!(state.apply(&ReplaceSuffix { from_index: 1, entries: vec![entry(2, 1)] }, &identity).is_err());
        assert!(state.apply(&ReplaceSuffix { from_index: 1, entries: vec![entry(1, 2)] }, &identity).is_err());
        state.apply(&ReplaceSuffix { from_index: 1, entries: vec![entry(1, 1), entry(2, 1)] }, &identity).unwrap();
        let different = crate::LogEntry { index: 2, term: 1,
            cmd: crate::KvCommand::Put { key: "k".into(), value: vec![2] } };
        assert!(state.apply(&ReplaceSuffix { from_index: 2, entries: vec![different] }, &identity).is_err());
        assert_eq!(state.log[1], entry(2, 1));
        state.apply(&ReplaceSuffix { from_index: 2, entries: vec![entry(2, 1)] }, &identity).unwrap();
        assert!(state.apply(&Commit { index: 3 }, &identity).is_err());
        state.apply(&Commit { index: 1 }, &identity).unwrap();
        assert!(state.apply(&ReplaceSuffix { from_index: 1, entries: vec![] }, &identity).is_err());
        assert!(state.apply(&Commit { index: 0 }, &identity).is_err());
        state.apply(&HardState { term: 2, voted_for: None }, &identity).unwrap();
        state.apply(&ReplaceSuffix { from_index: 2, entries: vec![entry(2, 2)] }, &identity).unwrap();
        assert_eq!(state.log[1].term, 2);
        assert_eq!(state.commit_index, 1);
    }

    #[test]
    fn typed_wal_recovers_and_rejects_invalid_records_without_modification() {
        use GroupRaftRecord::*;
        let base = std::env::var_os("YADB_TEST_TMPDIR").map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = base.join(format!("yadb-typed-wal-{}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        let fs = OsFs::new(&dir).unwrap();
        drop(LockedEmptyGroupWal::bootstrap(&fs, "", &replica()).unwrap());
        let mut wal = TypedGroupWal::open(&fs, "", &replica()).unwrap();
        wal.append(&HardState { term: 1, voted_for: Some(1) }).unwrap();
        let before = std::fs::metadata(dir.join(GROUP_WAL_FILE)).unwrap().len();
        assert!(wal.append(&HardState { term: 1, voted_for: Some(2) }).is_err());
        assert_eq!(std::fs::metadata(dir.join(GROUP_WAL_FILE)).unwrap().len(), before);
        drop(wal);
        let wal = TypedGroupWal::open(&fs, "", &replica()).unwrap();
        assert_eq!(wal.state().voted_for, Some(1));
        drop(wal);
        let (mut raw, _) = LockedGroupWal::open(&fs, "", &replica()).unwrap();
        raw.append(&serde_json::to_vec(&HardState { term: 1, voted_for: Some(2) }).unwrap()).unwrap();
        drop(raw);
        let before = std::fs::read(dir.join(GROUP_WAL_FILE)).unwrap();
        assert!(matches!(TypedGroupWal::open(&fs, "", &replica()), Err(GroupWalError::Corrupt)));
        assert_eq!(std::fs::read(dir.join(GROUP_WAL_FILE)).unwrap(), before);
        std::fs::remove_file(dir.join(GROUP_WAL_FILE)).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn typed_replay_rejects_same_term_index_with_different_command() {
        use GroupRaftRecord::*;
        let fs = SimFs::new(905);
        fs.create_dir_all("g").unwrap();
        bootstrap_empty_wal(&fs, "g", &replica()).unwrap();
        let file = fs.open_rw("g/group.wal", false).unwrap();
        let mut end = file.len().unwrap();
        let entry = |value| crate::LogEntry { index: 1, term: 1,
            cmd: crate::KvCommand::Put { key: "k".into(), value: vec![value] } };
        for record in [HardState { term: 1, voted_for: Some(1) },
            ReplaceSuffix { from_index: 1, entries: vec![entry(1)] },
            ReplaceSuffix { from_index: 1, entries: vec![entry(2)] }] {
            let frame = encode_group_frame(&serde_json::to_vec(&record).unwrap(), end).unwrap();
            file.write_at(end, &frame).unwrap();
            end += frame.len() as u64;
        }
        let (_, records) = replay_group_wal(&file, &replica()).unwrap();
        let mut state = GroupRaftState::default();
        for bytes in &records[..2] {
            state.apply(&serde_json::from_slice(bytes).unwrap(), &replica()).unwrap();
        }
        assert!(state.apply(&serde_json::from_slice(&records[2]).unwrap(), &replica()).is_err());
        assert_eq!(file.len().unwrap(), end);
        assert_eq!(state.log[0], entry(1));
    }

    #[test]
    fn wal_bootstrap_faults_never_trigger_implicit_creation() {
        for (data_fault, dir_fault) in [
            (SyncFault::BeforePersist, SyncFault::None),
            (SyncFault::AfterPersist, SyncFault::None),
            (SyncFault::None, SyncFault::BeforePersist),
            (SyncFault::None, SyncFault::AfterPersist),
        ] {
            let fs = SimFs::new(902);
            fs.create_dir_all("g").unwrap();
            fs.set_faults(FaultConfig { sync_data_fault: data_fault, sync_dir_fault: dir_fault, ..Default::default() });
            assert!(bootstrap_empty_wal(&fs, "g", &replica()).is_err());
            fs.set_faults(FaultConfig::default());
            assert!(matches!(bootstrap_empty_wal(&fs, "g", &replica()), Err(DescriptorError::DirectoryNotEmpty)));
            fs.simulate_power_cut();
            let opened = verify_empty_wal(&fs, "g", &replica());
            if dir_fault == SyncFault::AfterPersist { opened.unwrap(); } else { assert!(opened.is_err()); }
        }
        let fs = SimFs::new(903);
        fs.create_dir_all("g").unwrap();
        let file = fs.open_rw("g/group.wal", true).unwrap();
        file.write_at(0, b"YADBWAL1").unwrap();
        assert!(matches!(verify_empty_wal(&fs, "g", &replica()), Err(DescriptorError::Mismatch)));
        assert!(matches!(bootstrap_empty_wal(&fs, "g", &replica()), Err(DescriptorError::DirectoryNotEmpty)));
    }

    #[test]
    fn ambiguous_bootstrap_is_not_automatically_recreated() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(33);
            fs.create_dir_all("group").unwrap();
            fs.set_faults(FaultConfig { sync_data_fault: fault, ..Default::default() });
            assert!(bootstrap_descriptor(&fs, "group", &replica()).is_err());
            fs.set_faults(FaultConfig::default());
            assert!(matches!(bootstrap_descriptor(&fs, "group", &replica()), Err(DescriptorError::DirectoryNotEmpty)));
            verify_descriptor(&fs, "group", &replica()).unwrap();
            fs.simulate_power_cut();
            verify_descriptor(&fs, "group", &replica()).unwrap();
        }
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(34);
            fs.create_dir_all("group").unwrap();
            fs.set_faults(FaultConfig { sync_dir_fault: fault, ..Default::default() });
            assert!(bootstrap_descriptor(&fs, "group", &replica()).is_err());
            fs.set_faults(FaultConfig::default());
            fs.simulate_power_cut();
            // Absence is an error, never an invitation to bootstrap automatically.
            let result = verify_descriptor(&fs, "group", &replica());
            if fault == SyncFault::BeforePersist { assert!(result.is_err()); }
            else { result.unwrap(); }
        }
    }
}
