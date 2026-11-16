use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use yadb_io::{StorageFs, VirtualFile};

use crate::kv::{KvCommand, KvResult, MetaStateMachine, WatchEvent};
use crate::wal::{RaftWal, WalRecord};
use crate::{KvValue, MetaError, MetaSnapshot};

const WAL_COMPACTION_BYTES: u64 = 8 * 1024 * 1024;
const WAL_COMPACTION_ENTRIES: u64 = 4096;

/// Version of `checkpoint.json`. Version 2 adds the membership the image was
/// taken under. A version-1 image is still read (it predates membership), but a
/// reader that only knows version 1 refuses version 2 explicitly instead of
/// ignoring the field it cannot see.
const CHECKPOINT_FORMAT_VERSION: u32 = 2;

/// Upper bound on one snapshot image, matching the OpenRaft state machine's
/// image bound. It is also the largest `total_len` a receiver will accept.
const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;

/// Bounded chunk of a snapshot image carried by one `InstallSnapshot` RPC.
const SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;

/// In-progress snapshot received from a leader. It was never acknowledged as a
/// whole, so it is not authoritative and is discarded when the node opens.
const SNAPSHOT_PARTIAL_FILE: &str = "snapshot.partial";

/// Leader-side copy of the image streamed to peers. It is a cache of the
/// checkpoint image plus membership, rebuilt whenever the base moves.
const SNAPSHOT_EXPORT_FILE: &str = "snapshot.export";

/// How long a Leader waits for its own proposal to be committed and applied
/// before answering with an explicit unknown outcome. It must stay below the
/// forwarding follower's 5 s reply timeout so the Leader can answer first with a
/// definite refusal instead of the follower reporting a lost response.
const COMMIT_WAIT: Duration = Duration::from_millis(3000);

/// If this many consecutive replication rounds reach no peer at all, the Leader
/// has no quorum and there is nothing to wait for: refusing at once beats
/// polling refused connections for the whole [`COMMIT_WAIT`] budget.
const COMMIT_WAIT_EMPTY_ROUNDS: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableCheckpoint {
    format_version: u32,
    applied_index: u64,
    applied_term: u64,
    commit_index: u64,
    hard_state: DurableHardState,
    compaction_floor: u64,
    /// Voters this image was taken under. Version-1 images predate the field
    /// and deserialize to an empty list.
    #[serde(default)]
    membership: Vec<u32>,
    snapshot: MetaSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableHardState {
    term: u64,
    voted_for: Option<u32>,
}

fn checkpoint_path(dir: &str) -> String {
    if dir.is_empty() {
        "checkpoint.json".to_string()
    } else {
        format!("{}/checkpoint.json", dir.trim_end_matches('/'))
    }
}

fn snapshot_partial_path(dir: &str) -> String {
    if dir.is_empty() {
        SNAPSHOT_PARTIAL_FILE.to_string()
    } else {
        format!("{}/{}", dir.trim_end_matches('/'), SNAPSHOT_PARTIAL_FILE)
    }
}

fn snapshot_export_path(dir: &str) -> String {
    if dir.is_empty() {
        SNAPSHOT_EXPORT_FILE.to_string()
    } else {
        format!("{}/{}", dir.trim_end_matches('/'), SNAPSHOT_EXPORT_FILE)
    }
}

/// Identity of a legacy Raft group. Legacy membership is fixed configuration,
/// so the sorted voter set is the whole group identity: a snapshot built by
/// another set of voters is a different group and must never be installed.
fn group_id(membership: &[u32]) -> u64 {
    let mut ids = membership.to_vec();
    ids.sort_unstable();
    ids.dedup();
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(b"yadb-raft-group/1");
    for id in ids {
        hasher.update(&id.to_le_bytes());
    }
    u64::from(hasher.finalize())
}

/// Content identity of one snapshot image: its CRC32 and its exact length. A
/// chunk repeats it so a receiver can never mix bytes from two images, and the
/// full checksum is re-derived from the assembled file before anything is
/// published.
fn snapshot_id(crc: u32, len: u64) -> u64 {
    (u64::from(crc) << 32) | (len & 0xffff_ffff)
}

/// Streams `Write` calls into a `VirtualFile` at increasing offsets, hashing
/// the same bytes so an image can be exported without holding it in memory.
struct FsSink<'a, F: VirtualFile> {
    file: &'a F,
    pos: u64,
    crc: crc32fast::Hasher,
}

impl<'a, F: VirtualFile> FsSink<'a, F> {
    fn new(file: &'a F) -> Self {
        Self {
            file,
            pos: 0,
            crc: crc32fast::Hasher::new(),
        }
    }
}

impl<F: VirtualFile> std::io::Write for FsSink<'_, F> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write_at(self.pos, buf)?;
        self.pos += buf.len() as u64;
        self.crc.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Reads a `VirtualFile` as a bounded `Read`, so a receiver can checksum and
/// decode an assembled image without first copying it into one buffer.
struct FsSource<'a, F: VirtualFile> {
    file: &'a F,
    pos: u64,
    len: u64,
}

impl<'a, F: VirtualFile> FsSource<'a, F> {
    fn new(file: &'a F, len: u64) -> Self {
        Self { file, pos: 0, len }
    }
}

impl<F: VirtualFile> std::io::Read for FsSource<'_, F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.len || buf.is_empty() {
            return Ok(0);
        }
        let want = (self.len - self.pos).min(buf.len() as u64) as usize;
        let read = self.file.read_at(self.pos, &mut buf[..want])?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "snapshot image ended before its declared length",
            ));
        }
        self.pos += read as u64;
        Ok(read)
    }
}

fn load_checkpoint<FS: StorageFs>(
    fs: &FS,
    dir: &str,
) -> Result<Option<DurableCheckpoint>, MetaError> {
    let path = checkpoint_path(dir);
    if !fs.exists(&path) {
        return Ok(None);
    }
    let file = fs.open_rw(&path, false)?;
    let mut bytes = vec![0; file.len()? as usize];
    file.read_at(0, &mut bytes)?;
    let checkpoint: DurableCheckpoint =
        serde_json::from_slice(&bytes).map_err(|e| MetaError::Checkpoint(format!("{e}")))?;
    if checkpoint.format_version != 1 && checkpoint.format_version != CHECKPOINT_FORMAT_VERSION {
        return Err(MetaError::Checkpoint(format!(
            "unsupported format version {}",
            checkpoint.format_version
        )));
    }
    if checkpoint.applied_index > checkpoint.commit_index {
        return Err(MetaError::Checkpoint(
            "applied index exceeds checkpoint commit index".into(),
        ));
    }
    Ok(Some(checkpoint))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoleState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub cmd: KvCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRpc {
    RequestVote {
        term: u64,
        candidate_id: u32,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteReply {
        from: u32,
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: u32,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        #[serde(default)]
        compaction_floor: u64,
    },
    AppendEntriesReply {
        from: u32,
        term: u64,
        success: bool,
        /// On success, the highest index this follower now matches. On failure,
        /// the follower's last log index, used only as a backoff hint so the
        /// leader does not walk back one entry per round.
        match_index: u64,
    },
    /// One bounded chunk of a snapshot image. Every chunk repeats the full
    /// identity (group, membership, last included log id, length and content
    /// id) so the receiver validates it before touching its data.
    InstallSnapshot {
        term: u64,
        leader_id: u32,
        snapshot_id: u64,
        group_id: u64,
        membership: Vec<u32>,
        last_index: u64,
        last_term: u64,
        total_len: u64,
        offset: u64,
        data: Vec<u8>,
    },
    InstallSnapshotReply {
        from: u32,
        term: u64,
        snapshot_id: u64,
        accepted: bool,
        /// Bytes held for this snapshot; the offset the leader resumes from. A
        /// refusal carries the offset the receiver will accept next.
        next_offset: u64,
        error: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct OutboundMsg {
    pub to: u32,
    pub rpc: RaftRpc,
}

/// The durable checkpoint image as it was last exported for peers, plus the
/// pointers a chunk header must carry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotExport {
    base_index: u64,
    base_term: u64,
    membership: Vec<u32>,
    group_id: u64,
    id: u64,
    len: u64,
}

/// How far one peer has acknowledged an in-flight snapshot transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotSend {
    id: u64,
    next_offset: u64,
}

/// A partially received image. Bytes live in `snapshot.partial`; this is the
/// identity that makes a resume safe and a different snapshot restart clean.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotReceive {
    snapshot_id: u64,
    group_id: u64,
    membership: Vec<u32>,
    last_index: u64,
    last_term: u64,
    total_len: u64,
    received: u64,
}

/// Core Raft + Ordered KV state machine node (works identically in single-node mode,
/// deterministic in-memory multi-node simulation, or networked HTTP/TCP transport).
pub struct RaftCore<FS: StorageFs> {
    fs: FS,
    dir: String,
    node_id: u32,
    peers: Vec<u32>,
    role: RoleState,
    current_term: u64,
    voted_for: Option<u32>,
    leader_id: Option<u32>,
    log: Vec<LogEntry>,
    base_index: u64,
    base_term: u64,
    compaction_floor: u64,
    checkpoint_snapshot: MetaSnapshot,
    commit_index: u64,
    last_applied: u64,
    votes_received: BTreeSet<u32>,
    next_index: BTreeMap<u32, u64>,
    match_index: BTreeMap<u32, u64>,
    applied_results: BTreeMap<u64, KvResult>,
    wal: RaftWal<FS>,
    sm: MetaStateMachine,
    compaction_bytes: u64,
    compaction_entries: u64,
    /// Set when a checkpoint or WAL rewrite failed. The applied state and the
    /// WAL are still consistent at that point, but the log can no longer be
    /// bounded, so the node refuses further work instead of reporting success
    /// for a mutation whose durable maintenance is broken. Only a restart (and
    /// an operator) clears it.
    compaction_error: Option<String>,
    /// Image cached for peers that are below the compacted base.
    snapshot_export: Option<SnapshotExport>,
    /// Per-peer resume offsets for an in-flight snapshot transfer.
    snapshot_sends: BTreeMap<u32, SnapshotSend>,
    /// Partially received image, if any.
    snapshot_receive: Option<SnapshotReceive>,
    /// Bytes per `InstallSnapshot` chunk. A field so a test can shrink it.
    snapshot_chunk_bytes: usize,
}

impl<FS: StorageFs> RaftCore<FS> {
    pub fn open(
        fs: &FS,
        dir: &str,
        node_id: u32,
        mut peers: Vec<u32>,
        sync_writes: bool,
    ) -> Result<Self, MetaError> {
        if !peers.contains(&node_id) {
            peers.push(node_id);
        }
        peers.sort_unstable();
        peers.dedup();

        let (mut wal, mut recovered) = RaftWal::open(fs, dir, sync_writes)?;
        let checkpoint = load_checkpoint(fs, dir)?;
        // A partial or exported image is never authoritative: a received image
        // is only installed by publishing it as `checkpoint.json`, and an
        // export is only a cache. Drop both so a restart starts from the same
        // place the leader will be told to resume from.
        for path in [snapshot_partial_path(dir), snapshot_export_path(dir)] {
            if fs.exists(&path) {
                fs.remove_file(&path)?;
                fs.sync_dir(dir)?;
            }
        }
        let (
            sm,
            applied_index,
            checkpoint_floor,
            checkpoint_snapshot,
            checkpoint_term,
            checkpoint_vote,
        ) = if let Some(cp) = checkpoint {
            // Version-1 images predate membership; a version-2 image records
            // the voters it was written under, and installing it into a
            // different group would be silently wrong.
            if !cp.membership.is_empty() && cp.membership != peers {
                return Err(MetaError::Checkpoint(format!(
                    "checkpoint was written for voters {:?}, this replica is configured for {peers:?}",
                    cp.membership
                )));
            }
            if cp.applied_index < recovered.base_index {
                return Err(MetaError::Checkpoint(
                    "checkpoint applied index is below the WAL base".into(),
                ));
            }
            if cp.applied_index == recovered.base_index && cp.applied_term != recovered.base_term {
                return Err(MetaError::Checkpoint(
                    "checkpoint applied term disagrees with the WAL base term".into(),
                ));
            }
            let snapshot = cp.snapshot.clone();
            let retained: Vec<_> = recovered
                .log
                .iter()
                .filter(|entry| entry.index > cp.applied_index)
                .cloned()
                .collect();
            let needs_rewrite = recovered.base_index != cp.applied_index;
            recovered.base_index = cp.applied_index;
            recovered.base_term = cp.applied_term;
            recovered.log = retained;
            if needs_rewrite {
                let mut records = vec![
                    WalRecord::HardState {
                        term: cp.hard_state.term,
                        voted_for: cp.hard_state.voted_for,
                    },
                    WalRecord::Base {
                        index: cp.applied_index,
                        term: cp.applied_term,
                    },
                ];
                if !recovered.log.is_empty() {
                    records.push(WalRecord::AppendEntries {
                        from_index: recovered.log[0].index,
                        entries: recovered.log.clone(),
                    });
                }
                wal.rewrite(&records)?;
            }
            let mut sm = MetaStateMachine::new();
            sm.restore_durable_snapshot(snapshot.clone());
            (
                sm,
                cp.applied_index,
                cp.compaction_floor,
                snapshot,
                cp.hard_state.term,
                cp.hard_state.voted_for,
            )
        } else {
            if recovered.base_index != 0 {
                return Err(MetaError::Checkpoint(
                    "WAL has a compacted base but checkpoint is missing".into(),
                ));
            }
            let snapshot = MetaSnapshot {
                revision: 0,
                values: BTreeMap::new(),
            };
            (
                MetaStateMachine::new(),
                0,
                0,
                snapshot,
                recovered.term,
                recovered.voted_for,
            )
        };
        let mut core = Self {
            fs: fs.clone(),
            dir: dir.to_string(),
            node_id,
            peers,
            role: RoleState::Follower,
            current_term: checkpoint_term.max(recovered.term),
            voted_for: if recovered.term >= checkpoint_term {
                recovered.voted_for
            } else {
                checkpoint_vote
            },
            leader_id: None,
            log: recovered.log,
            base_index: recovered.base_index,
            base_term: recovered.base_term,
            compaction_floor: checkpoint_floor,
            checkpoint_snapshot,
            commit_index: applied_index,
            last_applied: applied_index,
            votes_received: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            applied_results: BTreeMap::new(),
            wal,
            sm,
            compaction_bytes: WAL_COMPACTION_BYTES,
            compaction_entries: WAL_COMPACTION_ENTRIES,
            compaction_error: None,
            snapshot_export: None,
            snapshot_sends: BTreeMap::new(),
            snapshot_receive: None,
            snapshot_chunk_bytes: SNAPSHOT_CHUNK_BYTES,
        };

        // Single-node cluster automatically becomes leader and replays its own durable WAL!
        if core.peers.len() == 1 {
            if core.current_term == 0 {
                core.current_term = 1;
                core.voted_for = Some(node_id);
                core.wal.append(&WalRecord::HardState {
                    term: 1,
                    voted_for: Some(node_id),
                })?;
            }
            core.role = RoleState::Leader;
            core.leader_id = Some(node_id);
            let last_idx = core.last_log_index();
            core.commit_index = core.commit_index.max(last_idx);
            // A single node is its own quorum, so everything it holds is
            // committed and nothing else can need it: the floor may follow.
            core.compaction_floor = core.compaction_floor.max(core.commit_index);
            core.apply_committed()?;
        }

        Ok(core)
    }

    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    pub fn role(&self) -> RoleState {
        self.role
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn leader_id(&self) -> Option<u32> {
        self.leader_id
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn quorum_size(&self) -> usize {
        (self.peers.len() / 2) + 1
    }

    pub fn last_log_index(&self) -> u64 {
        self.base_index + self.log.len() as u64
    }

    pub fn base_index(&self) -> u64 {
        self.base_index
    }

    pub fn compaction_floor(&self) -> u64 {
        self.compaction_floor
    }

    pub fn wal_bytes(&self) -> Result<u64, MetaError> {
        self.wal.byte_len()
    }

    /// The explicit reason this node stopped accepting work after a failed
    /// checkpoint or WAL rewrite, if any. Nothing clears it except a restart.
    pub fn compaction_error(&self) -> Option<String> {
        self.compaction_error.clone()
    }

    fn ensure_operable(&self) -> Result<(), MetaError> {
        if let Some(err) = &self.compaction_error {
            return Err(MetaError::CompactionFailed(err.clone()));
        }
        self.wal.ensure_healthy()
    }

    #[cfg(test)]
    pub(crate) fn set_compaction_thresholds(&mut self, bytes: u64, entries: u64) {
        self.compaction_bytes = bytes;
        self.compaction_entries = entries;
    }

    #[cfg(test)]
    pub(crate) fn voted_for(&self) -> Option<u32> {
        self.voted_for
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map_or(self.base_term, |e| e.term)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.base_index {
            // `base_index == 0` is genesis, whose term is 0.
            return Some(self.base_term);
        }
        if index < self.base_index {
            return None;
        }
        self.log
            .get((index - self.base_index - 1) as usize)
            .map(|e| e.term)
    }

    fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.base_index {
            None
        } else {
            self.log.get((index - self.base_index - 1) as usize)
        }
    }

    /// Triggers an election timeout on this node: transitions to `Candidate` (or directly `Leader` if N=1).
    pub fn start_election(&mut self) -> Result<Vec<OutboundMsg>, MetaError> {
        self.ensure_operable()?;
        self.current_term += 1;
        self.role = RoleState::Candidate;
        self.voted_for = Some(self.node_id);
        self.leader_id = None;
        self.votes_received.clear();
        self.votes_received.insert(self.node_id);

        self.wal.append(&WalRecord::HardState {
            term: self.current_term,
            voted_for: self.voted_for,
        })?;

        if self.votes_received.len() >= self.quorum_size() {
            return self.become_leader();
        }

        let last_log_index = self.last_log_index();
        let last_log_term = self.last_log_term();
        let mut out = Vec::new();
        for &peer in &self.peers {
            if peer != self.node_id {
                out.push(OutboundMsg {
                    to: peer,
                    rpc: RaftRpc::RequestVote {
                        term: self.current_term,
                        candidate_id: self.node_id,
                        last_log_index,
                        last_log_term,
                    },
                });
            }
        }
        Ok(out)
    }

    fn become_leader(&mut self) -> Result<Vec<OutboundMsg>, MetaError> {
        self.role = RoleState::Leader;
        self.leader_id = Some(self.node_id);
        let next = self.last_log_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        self.snapshot_sends.clear();
        for &peer in &self.peers {
            self.next_index.insert(peer, next);
            self.match_index.insert(
                peer,
                if peer == self.node_id {
                    self.last_log_index()
                } else {
                    0
                },
            );
        }
        // A new leader must commit an entry in its own term before older
        // committed entries can be safely applied after a full restart. An empty
        // transaction is a no-op on keys and uses the existing WAL format.
        let (_, messages) = self.propose(KvCommand::Txn {
            guards: vec![],
            ops: vec![],
        })?;
        Ok(messages)
    }

    /// Proposes a KV mutation on the Leader, appends to local WAL, and returns replication RPCs
    /// (for N=1 single-node mode, commits and applies immediately).
    pub fn propose(&mut self, cmd: KvCommand) -> Result<(u64, Vec<OutboundMsg>), MetaError> {
        self.ensure_operable()?;
        if self.role != RoleState::Leader {
            return Err(MetaError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.last_log_index() + 1;
        let entry = LogEntry {
            index: idx,
            term: self.current_term,
            cmd,
        };
        self.wal.append(&WalRecord::AppendEntries {
            from_index: idx,
            entries: vec![entry.clone()],
        })?;
        self.log.push(entry);
        self.match_index.insert(self.node_id, idx);

        if self.quorum_size() == 1 {
            self.commit_index = idx;
            self.compaction_floor = self.compaction_floor.max(idx);
            self.apply_committed()?;
            return Ok((idx, Vec::new()));
        }

        Ok((idx, self.broadcast_append_entries_checked()?))
    }

    pub fn broadcast_append_entries(&mut self) -> Vec<OutboundMsg> {
        self.broadcast_append_entries_checked().unwrap_or_default()
    }

    fn broadcast_append_entries_checked(&mut self) -> Result<Vec<OutboundMsg>, MetaError> {
        if self.ensure_operable().is_err() || self.role != RoleState::Leader {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for peer in self.peers.clone() {
            if peer == self.node_id {
                continue;
            }
            let next_idx = self
                .next_index
                .get(&peer)
                .copied()
                .unwrap_or(self.last_log_index() + 1)
                .max(1);
            if next_idx <= self.base_index {
                // The entries this peer needs are gone. Install the durable
                // image instead of refusing it forever.
                out.push(self.snapshot_chunk_for(peer)?);
                continue;
            }
            let prev_log_index = next_idx - 1;
            let prev_log_term = self.term_at(prev_log_index).unwrap_or(0);
            let entries = self.log[(next_idx - self.base_index - 1) as usize..].to_vec();
            out.push(OutboundMsg {
                to: peer,
                rpc: RaftRpc::AppendEntries {
                    term: self.current_term,
                    leader_id: self.node_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                    compaction_floor: self.compaction_floor,
                },
            });
        }
        Ok(out)
    }

    pub fn step(&mut self, rpc: RaftRpc) -> Result<Vec<OutboundMsg>, MetaError> {
        self.ensure_operable()?;
        match rpc {
            RaftRpc::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                if term > self.current_term {
                    self.step_down(term)?;
                }
                let up_to_date = (last_log_term, last_log_index)
                    >= (self.last_log_term(), self.last_log_index());
                let can_vote = term == self.current_term
                    && (self.voted_for.is_none() || self.voted_for == Some(candidate_id))
                    && up_to_date;

                if can_vote {
                    self.voted_for = Some(candidate_id);
                    self.wal.append(&WalRecord::HardState {
                        term: self.current_term,
                        voted_for: self.voted_for,
                    })?;
                }

                Ok(vec![OutboundMsg {
                    to: candidate_id,
                    rpc: RaftRpc::RequestVoteReply {
                        from: self.node_id,
                        term: self.current_term,
                        vote_granted: can_vote,
                    },
                }])
            }

            RaftRpc::RequestVoteReply {
                from,
                term,
                vote_granted,
            } => {
                if term > self.current_term {
                    self.step_down(term)?;
                    return Ok(Vec::new());
                }
                if self.role == RoleState::Candidate && term == self.current_term && vote_granted {
                    self.votes_received.insert(from);
                    if self.votes_received.len() >= self.quorum_size() {
                        return self.become_leader();
                    }
                }
                Ok(Vec::new())
            }

            RaftRpc::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                compaction_floor,
            } => {
                if term < self.current_term {
                    return Ok(vec![OutboundMsg {
                        to: leader_id,
                        rpc: RaftRpc::AppendEntriesReply {
                            from: self.node_id,
                            term: self.current_term,
                            success: false,
                            match_index: self.last_log_index(),
                        },
                    }]);
                }
                if term > self.current_term || self.role != RoleState::Follower {
                    let prev_voted = if term > self.current_term {
                        None
                    } else {
                        self.voted_for
                    };
                    if term > self.current_term {
                        self.current_term = term;
                        self.voted_for = prev_voted;
                        self.wal.append(&WalRecord::HardState {
                            term: self.current_term,
                            voted_for: self.voted_for,
                        })?;
                    }
                    self.role = RoleState::Follower;
                }
                self.leader_id = Some(leader_id);

                // Raft §5.3 log matching check
                if self.term_at(prev_log_index) != Some(prev_log_term) {
                    return Ok(vec![OutboundMsg {
                        to: leader_id,
                        rpc: RaftRpc::AppendEntriesReply {
                            from: self.node_id,
                            term: self.current_term,
                            success: false,
                            match_index: self.last_log_index(),
                        },
                    }]);
                }

                // Only the prefix covered by this RPC is known to match the
                // leader. A follower may still have an unrelated local suffix.
                let matched_index = prev_log_index + entries.len() as u64;

                // Truncate any conflicting uncommitted suffix and append new entries
                let mut write_start: Option<u64> = None;
                let mut to_persist: Vec<LogEntry> = Vec::new();
                for entry in entries {
                    if entry.index <= self.base_index {
                        return Err(MetaError::PeerBehindCompactedBase(leader_id));
                    }
                    let pos = (entry.index - self.base_index - 1) as usize;
                    if pos < self.log.len() {
                        if self.log[pos].term != entry.term {
                            self.log.truncate(pos);
                            if write_start.is_none() {
                                write_start = Some(entry.index);
                            }
                            self.log.push(entry.clone());
                            to_persist.push(entry);
                        }
                    } else {
                        if write_start.is_none() {
                            write_start = Some(entry.index);
                        }
                        self.log.push(entry.clone());
                        to_persist.push(entry);
                    }
                }

                if let Some(from_index) = write_start {
                    self.wal.append(&WalRecord::AppendEntries {
                        from_index,
                        entries: to_persist,
                    })?;
                }

                if leader_commit > self.commit_index {
                    self.commit_index = self.commit_index.max(leader_commit.min(matched_index));
                    self.apply_committed()?;
                }
                self.compaction_floor = self.compaction_floor.max(compaction_floor);
                self.try_compact();

                Ok(vec![OutboundMsg {
                    to: leader_id,
                    rpc: RaftRpc::AppendEntriesReply {
                        from: self.node_id,
                        term: self.current_term,
                        success: true,
                        match_index: matched_index,
                    },
                }])
            }

            RaftRpc::AppendEntriesReply {
                from,
                term,
                success,
                match_index,
            } => {
                if term > self.current_term {
                    self.step_down(term)?;
                    return Ok(Vec::new());
                }
                if self.role != RoleState::Leader || term != self.current_term {
                    return Ok(Vec::new());
                }

                if success {
                    let cur_match = self.match_index.get(&from).copied().unwrap_or(0);
                    let updated_match = cur_match.max(match_index);
                    self.match_index.insert(from, updated_match);
                    self.next_index.insert(from, updated_match + 1);
                    let candidate = self
                        .commit_index
                        .min(self.match_index.values().copied().min().unwrap_or(0));
                    self.compaction_floor = self.compaction_floor.max(candidate);

                    // Raft §5.4.2: advance `commit_index` only for entries from `current_term`
                    let old_commit = self.commit_index;
                    for n in ((self.commit_index + 1)..=self.last_log_index()).rev() {
                        if self.term_at(n) == Some(self.current_term) {
                            let replicated = self
                                .peers
                                .iter()
                                .filter(|&&p| self.match_index.get(&p).copied().unwrap_or(0) >= n)
                                .count();
                            if replicated >= self.quorum_size() {
                                self.commit_index = n;
                                self.apply_committed()?;
                                break;
                            }
                        }
                    }
                    // Broadcast updated `leader_commit` so followers apply immediately
                    if self.commit_index > old_commit {
                        return self.broadcast_append_entries_checked();
                    }
                } else {
                    let cur_next = self.next_index.get(&from).copied().unwrap_or(2);
                    let backed_off = cur_next.saturating_sub(1).max(1);
                    // A failed reply carries the follower's last log index in
                    // `match_index`. Jump to it when that skips more entries,
                    // but never stall: an unchanged hint falls back to the
                    // one-step backoff so a divergent entry still converges.
                    let hint = if match_index > 0 {
                        match_index + 1
                    } else {
                        backed_off
                    };
                    let next = if hint < cur_next {
                        hint.max(1)
                    } else {
                        backed_off
                    };
                    self.next_index.insert(from, next);
                    // If the entries are gone, this sends the durable image
                    // rather than refusing the peer forever.
                    return self.broadcast_append_entries_checked();
                }
                Ok(Vec::new())
            }

            RaftRpc::InstallSnapshot {
                term,
                leader_id,
                snapshot_id,
                group_id,
                membership,
                last_index,
                last_term,
                total_len,
                offset,
                data,
            } => {
                if term < self.current_term {
                    return Ok(vec![OutboundMsg {
                        to: leader_id,
                        rpc: RaftRpc::InstallSnapshotReply {
                            from: self.node_id,
                            term: self.current_term,
                            snapshot_id,
                            accepted: false,
                            next_offset: 0,
                            error: Some(format!(
                                "snapshot term {term} is below the current term {}",
                                self.current_term
                            )),
                        },
                    }]);
                }
                if term > self.current_term || self.role != RoleState::Follower {
                    if term > self.current_term {
                        self.current_term = term;
                        self.voted_for = None;
                        self.wal.append(&WalRecord::HardState {
                            term: self.current_term,
                            voted_for: None,
                        })?;
                    }
                    self.role = RoleState::Follower;
                }
                self.leader_id = Some(leader_id);
                let result = self.receive_snapshot_chunk(
                    snapshot_id,
                    group_id,
                    &membership,
                    last_index,
                    last_term,
                    total_len,
                    offset,
                    data,
                );
                let (accepted, next_offset, error) = match result {
                    Ok(next_offset) => (true, next_offset, None),
                    Err((next_offset, reason)) => (false, next_offset, Some(reason)),
                };
                Ok(vec![OutboundMsg {
                    to: leader_id,
                    rpc: RaftRpc::InstallSnapshotReply {
                        from: self.node_id,
                        term: self.current_term,
                        snapshot_id,
                        accepted,
                        next_offset,
                        error,
                    },
                }])
            }

            RaftRpc::InstallSnapshotReply {
                from,
                term,
                snapshot_id,
                accepted,
                next_offset,
                error: _,
            } => {
                if term > self.current_term {
                    self.step_down(term)?;
                    return Ok(Vec::new());
                }
                if self.role != RoleState::Leader || term != self.current_term {
                    return Ok(Vec::new());
                }
                if !accepted {
                    // The receiver refused this image or cannot resume here.
                    // Drop the progress so the next round restarts at 0; the
                    // refusal is already explicit in the reply.
                    self.snapshot_sends.remove(&from);
                    return Ok(Vec::new());
                }
                let export = self.ensure_snapshot_export()?;
                if snapshot_id != export.id || next_offset > export.len {
                    // A superseded image or an impossible offset: never keep
                    // streaming it.
                    self.snapshot_sends.remove(&from);
                    return Ok(Vec::new());
                }
                if next_offset < export.len {
                    self.snapshot_sends.insert(
                        from,
                        SnapshotSend {
                            id: export.id,
                            next_offset,
                        },
                    );
                    return Ok(vec![self.snapshot_chunk_for(from)?]);
                }
                // The image is durable on the follower. From here it is a
                // normal log follower: start appending above the new base.
                self.snapshot_sends.remove(&from);
                self.match_index.insert(from, export.base_index);
                self.next_index.insert(from, export.base_index + 1);
                self.broadcast_append_entries_checked()
            }
        }
    }

    fn step_down(&mut self, new_term: u64) -> Result<(), MetaError> {
        self.current_term = new_term;
        self.role = RoleState::Follower;
        self.voted_for = None;
        self.leader_id = None;
        self.votes_received.clear();
        self.snapshot_sends.clear();
        self.wal.append(&WalRecord::HardState {
            term: self.current_term,
            voted_for: None,
        })
    }

    fn apply_committed(&mut self) -> Result<(), MetaError> {
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            // A committed index the log cannot serve is a gap, not something to
            // skip: skipping it would apply later entries over a state the
            // earlier ones never produced.
            let entry = self.entry_at(next).cloned().ok_or(MetaError::CorruptWal(
                "a committed entry is missing from the log",
            ))?;
            self.last_applied = next;
            let res = self.sm.apply(entry.cmd);
            self.applied_results.insert(next, res);
        }
        self.try_compact();
        Ok(())
    }

    /// Runs compaction and records, rather than propagates, its failure.
    ///
    /// The entries applied so far are already durable in the WAL, so failing the
    /// operation that happened to trigger compaction would report a committed
    /// mutation as failed. The node instead refuses every later operation with
    /// [`MetaError::CompactionFailed`] until it is restarted.
    fn try_compact(&mut self) {
        if self.compaction_error.is_some() {
            return;
        }
        if let Err(err) = self.maybe_compact() {
            self.compaction_error = Some(err.to_string());
        }
    }

    fn snapshot_at(&self, index: u64) -> MetaSnapshot {
        let mut sm = MetaStateMachine::new();
        sm.restore_durable_snapshot(self.checkpoint_snapshot.clone());
        for entry in self
            .log
            .iter()
            .filter(|e| e.index > self.base_index && e.index <= index)
        {
            sm.apply(entry.cmd.clone());
        }
        sm.durable_snapshot()
    }

    fn maybe_compact(&mut self) -> Result<(), MetaError> {
        let target = self.last_applied.min(self.compaction_floor);
        if target <= self.base_index {
            return Ok(());
        }
        let over_threshold = self.wal.byte_len()? >= self.compaction_bytes
            || self.last_applied.saturating_sub(self.base_index) >= self.compaction_entries;
        if !over_threshold {
            return Ok(());
        }
        let applied_term = self
            .term_at(target)
            .ok_or(MetaError::Checkpoint("missing compaction term".into()))?;
        // Everything above `target` is kept, so the suffix must start exactly at
        // `target + 1`. A gap here would be written into the WAL as a silent
        // hole, so refuse before touching either file.
        let suffix: Vec<_> = self
            .log
            .iter()
            .filter(|e| e.index > target)
            .cloned()
            .collect();
        if suffix.first().is_some_and(|e| e.index != target + 1) {
            return Err(MetaError::Checkpoint(
                "compaction would leave a gap above the new base".into(),
            ));
        }
        let snapshot = self.snapshot_at(target);
        let checkpoint = DurableCheckpoint {
            format_version: CHECKPOINT_FORMAT_VERSION,
            applied_index: target,
            applied_term,
            commit_index: self.commit_index,
            hard_state: DurableHardState {
                term: self.current_term,
                voted_for: self.voted_for,
            },
            compaction_floor: self.compaction_floor,
            membership: self.peers.clone(),
            snapshot: snapshot.clone(),
        };
        let path = checkpoint_path(&self.dir);
        let tmp = format!("{path}.tmp");
        let file = self.fs.open_rw(&tmp, true)?;
        let bytes = serde_json::to_vec(&checkpoint)?;
        file.set_len(0)?;
        file.write_at(0, &bytes)?;
        file.sync_data()?;
        self.fs.rename_atomic(&tmp, &path)?;
        self.fs.sync_dir(&self.dir)?;
        let mut records = vec![
            WalRecord::HardState {
                term: self.current_term,
                voted_for: self.voted_for,
            },
            WalRecord::Base {
                index: target,
                term: applied_term,
            },
        ];
        if !suffix.is_empty() {
            records.push(WalRecord::AppendEntries {
                from_index: suffix[0].index,
                entries: suffix.clone(),
            });
        }
        self.wal.rewrite(&records)?;
        self.log = suffix;
        self.base_index = target;
        self.base_term = applied_term;
        self.checkpoint_snapshot = snapshot;
        // The image for peers changed with the base: rebuild it on demand and
        // make any in-flight transfer restart from the new snapshot.
        self.snapshot_export = None;
        self.snapshot_sends.clear();
        Ok(())
    }

    /// Build (or reuse) the image streamed to peers below the compacted base.
    ///
    /// The image is the durable checkpoint format itself, so a receiver that
    /// installs it publishes exactly the bytes `maybe_compact` writes. It is
    /// serialised straight into a file and checksummed as it is written, so a
    /// large image is never held in memory.
    fn ensure_snapshot_export(&mut self) -> Result<SnapshotExport, MetaError> {
        if let Some(export) = &self.snapshot_export {
            if export.base_index == self.base_index
                && export.base_term == self.base_term
                && export.membership == self.peers
            {
                return Ok(export.clone());
            }
        }
        let checkpoint = DurableCheckpoint {
            format_version: CHECKPOINT_FORMAT_VERSION,
            applied_index: self.base_index,
            applied_term: self.base_term,
            commit_index: self.commit_index,
            // A snapshot carries no vote of its own: the receiver persists its
            // own hard state in the rewritten WAL.
            hard_state: DurableHardState {
                term: self.current_term,
                voted_for: None,
            },
            compaction_floor: self.compaction_floor,
            membership: self.peers.clone(),
            snapshot: self.checkpoint_snapshot.clone(),
        };
        let path = snapshot_export_path(&self.dir);
        let file = self.fs.open_rw(&path, true)?;
        file.set_len(0)?;
        let mut sink = FsSink::new(&file);
        serde_json::to_writer(&mut sink, &checkpoint)?;
        let len = sink.pos;
        let crc = sink.crc.finalize();
        if len > MAX_SNAPSHOT_BYTES {
            file.set_len(0)?;
            return Err(MetaError::Checkpoint(format!(
                "snapshot image is {len} bytes, above the {MAX_SNAPSHOT_BYTES} byte limit"
            )));
        }
        let export = SnapshotExport {
            base_index: self.base_index,
            base_term: self.base_term,
            membership: self.peers.clone(),
            group_id: group_id(&self.peers),
            id: snapshot_id(crc, len),
            len,
        };
        self.snapshot_export = Some(export.clone());
        self.snapshot_sends.clear();
        Ok(export)
    }

    /// One bounded chunk of the current image for `peer`.
    fn snapshot_chunk_for(&mut self, peer: u32) -> Result<OutboundMsg, MetaError> {
        if self.base_index == 0 {
            // Nothing has been compacted, so there is no image to install.
            return Err(MetaError::PeerBehindCompactedBase(peer));
        }
        let export = self.ensure_snapshot_export()?;
        let mut offset = match self.snapshot_sends.get(&peer) {
            Some(send) if send.id == export.id => send.next_offset,
            _ => 0,
        };
        if offset >= export.len {
            // The previous transfer finished but its final reply was lost:
            // restart deterministically instead of sending an empty chunk.
            offset = 0;
        }
        let path = snapshot_export_path(&self.dir);
        let file = self.fs.open_rw(&path, false)?;
        let want = (export.len - offset).min(self.snapshot_chunk_bytes as u64) as usize;
        let mut data = vec![0u8; want];
        let read = file.read_at(offset, &mut data)?;
        if read == 0 {
            return Err(MetaError::Checkpoint(
                "snapshot export ended before its declared length".into(),
            ));
        }
        data.truncate(read);
        self.snapshot_sends.insert(
            peer,
            SnapshotSend {
                id: export.id,
                next_offset: offset,
            },
        );
        Ok(OutboundMsg {
            to: peer,
            rpc: RaftRpc::InstallSnapshot {
                term: self.current_term,
                leader_id: self.node_id,
                snapshot_id: export.id,
                group_id: export.group_id,
                membership: export.membership,
                last_index: export.base_index,
                last_term: export.base_term,
                total_len: export.len,
                offset,
                data,
            },
        })
    }

    /// Validate and durably hold one chunk. Returns the next offset the leader
    /// should send, or the offset the receiver will accept on a refusal.
    #[allow(clippy::too_many_arguments)]
    fn receive_snapshot_chunk(
        &mut self,
        snapshot_id: u64,
        image_group: u64,
        membership: &[u32],
        last_index: u64,
        last_term: u64,
        total_len: u64,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<u64, (u64, String)> {
        let local_group = group_id(&self.peers);
        if image_group != local_group {
            return Err((
                0,
                format!("snapshot is for group {image_group}, this replica is {local_group}"),
            ));
        }
        if membership != self.peers.as_slice() {
            return Err((0, "snapshot membership does not match this replica".into()));
        }
        if total_len == 0 || total_len > MAX_SNAPSHOT_BYTES {
            return Err((0, format!("snapshot length {total_len} is out of range")));
        }
        if last_index == 0 {
            return Err((0, "snapshot index 0 is impossible".into()));
        }
        if last_index < self.base_index {
            return Err((
                0,
                format!(
                    "snapshot at {last_index} is older than the installed base {}",
                    self.base_index
                ),
            ));
        }
        if last_index == self.base_index && last_term != self.base_term {
            return Err((
                0,
                format!(
                    "snapshot term {last_term} conflicts with base term {}",
                    self.base_term
                ),
            ));
        }
        if last_index < self.last_applied {
            return Err((
                0,
                format!(
                    "snapshot at {last_index} would roll back applied state {}",
                    self.last_applied
                ),
            ));
        }
        if let Some(term) = self.term_at(last_index) {
            if term != last_term {
                return Err((
                    0,
                    format!("snapshot at {last_index} conflicts with this log"),
                ));
            }
        }
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or((0, "snapshot offset overflow".to_string()))?;
        if data.is_empty() || end > total_len {
            return Err((
                0,
                format!(
                    "snapshot chunk at {offset} with {} bytes is outside {total_len}",
                    data.len()
                ),
            ));
        }
        let path = snapshot_partial_path(&self.dir);
        let file = self
            .fs
            .open_rw(&path, true)
            .map_err(|e| (0, format!("cannot open the partial snapshot: {e}")))?;
        let same = self.snapshot_receive.as_ref().is_some_and(|rx| {
            rx.snapshot_id == snapshot_id
                && rx.group_id == image_group
                && rx.membership == membership
                && rx.last_index == last_index
                && rx.last_term == last_term
                && rx.total_len == total_len
        });
        if !same {
            if offset != 0 {
                return Err((
                    0,
                    "a different snapshot is in progress; restart at offset 0".into(),
                ));
            }
            file.set_len(0)
                .map_err(|e| (0, format!("cannot reset the partial snapshot: {e}")))?;
            self.snapshot_receive = Some(SnapshotReceive {
                snapshot_id,
                group_id: image_group,
                membership: membership.to_vec(),
                last_index,
                last_term,
                total_len,
                received: 0,
            });
        }
        let done = {
            let rx = self
                .snapshot_receive
                .as_mut()
                .expect("snapshot receive state was just installed");
            if offset > rx.received {
                return Err((
                    rx.received,
                    format!("snapshot chunk at {offset} leaves a gap at {}", rx.received),
                ));
            }
            if offset < rx.received {
                // A retransmitted chunk: drop everything after it so bytes of
                // one snapshot can never be appended twice.
                file.set_len(offset).map_err(|e| {
                    (
                        rx.received,
                        format!("cannot truncate the partial snapshot: {e}"),
                    )
                })?;
                rx.received = offset;
            }
            if let Err(e) = file.write_at(offset, &data) {
                // A torn write leaves the file at an unknown point: the next
                // attempt restarts at this offset.
                rx.received = offset;
                return Err((offset, format!("snapshot chunk write failed: {e}")));
            }
            rx.received = end;
            rx.received == total_len
        };
        if done {
            if let Err((offset, reason)) = self.finish_snapshot_install() {
                // The image was refused as a whole: drop the scratch copy so
                // the receiver is back to the state it was serving and the
                // leader restarts from offset 0.
                self.snapshot_receive = None;
                let _ = self.fs.remove_file(&snapshot_partial_path(&self.dir));
                return Err((offset, reason));
            }
            return Ok(total_len);
        }
        Ok(self.snapshot_receive.as_ref().map_or(0, |rx| rx.received))
    }

    /// Validate the assembled image and make it the base, durably.
    ///
    /// Order is: sync the image, verify its whole-image checksum, decode and
    /// cross-check every pointer, publish it as `checkpoint.json` with one
    /// atomic rename plus a directory sync, rewrite the WAL to the new base,
    /// and only then move the in-memory applied/base pointer. A crash before
    /// the rename leaves the old image; a crash after it leaves the new image
    /// and `open` reconciles the older WAL to it. Nothing is acknowledged
    /// before both files are durable.
    fn finish_snapshot_install(&mut self) -> Result<(), (u64, String)> {
        let rx = self
            .snapshot_receive
            .clone()
            .ok_or((0, "no snapshot is being received".to_string()))?;
        let path = snapshot_partial_path(&self.dir);
        let file = self
            .fs
            .open_rw(&path, false)
            .map_err(|e| (0, format!("cannot open the partial snapshot: {e}")))?;
        let len = file
            .len()
            .map_err(|e| (0, format!("cannot size the partial snapshot: {e}")))?;
        if len != rx.total_len {
            return Err((
                0,
                format!("snapshot has {len} bytes, expected {}", rx.total_len),
            ));
        }
        file.sync_data()
            .map_err(|e| (0, format!("cannot sync the snapshot image: {e}")))?;
        let mut source = FsSource::new(&file, len);
        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = vec![0u8; SNAPSHOT_CHUNK_BYTES];
        loop {
            let read = source
                .read(&mut buffer)
                .map_err(|e| (0, format!("cannot read the snapshot image: {e}")))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        if snapshot_id(hasher.finalize(), len) != rx.snapshot_id {
            return Err((0, "snapshot checksum mismatch".into()));
        }
        let mut source = FsSource::new(&file, len);
        let image: DurableCheckpoint = serde_json::from_reader(&mut source)
            .map_err(|e| (0, format!("snapshot image is not a checkpoint: {e}")))?;
        if image.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err((
                0,
                format!(
                    "unsupported snapshot format version {}",
                    image.format_version
                ),
            ));
        }
        if image.applied_index != rx.last_index || image.applied_term != rx.last_term {
            return Err((0, "snapshot log pointer disagrees with its header".into()));
        }
        if image.membership != self.peers {
            return Err((0, "snapshot membership does not match this replica".into()));
        }
        if image.commit_index < image.applied_index {
            return Err((0, "snapshot commit index is below its applied index".into()));
        }
        if image.applied_index < self.base_index {
            return Err((
                0,
                format!(
                    "snapshot at {} is older than the installed base {}",
                    image.applied_index, self.base_index
                ),
            ));
        }
        if image.applied_index == self.base_index {
            if image.applied_term != self.base_term {
                return Err((0, "snapshot conflicts with the installed base".into()));
            }
            let current = self.sm.durable_snapshot();
            if image.snapshot.revision != current.revision
                || image.snapshot.values != current.values
            {
                return Err((
                    0,
                    "snapshot conflicts with the state at the same index".into(),
                ));
            }
        }
        // The checkpoint is authoritative on open, so publish it before the
        // WAL: a crash in between is reconciled to the new base.
        self.fs
            .rename_atomic(&path, &checkpoint_path(&self.dir))
            .map_err(|e| (0, format!("cannot publish the snapshot image: {e}")))?;
        self.fs
            .sync_dir(&self.dir)
            .map_err(|e| (0, format!("cannot sync the published snapshot: {e}")))?;
        let records = [
            WalRecord::HardState {
                term: self.current_term,
                voted_for: self.voted_for,
            },
            WalRecord::Base {
                index: image.applied_index,
                term: image.applied_term,
            },
        ];
        if let Err(error) = self.wal.rewrite(&records) {
            // The image is already published, so the next open adopts it. Do
            // not advance the in-memory base: refuse work until that restart.
            self.compaction_error = Some(error.to_string());
            return Err((
                0,
                format!("cannot rewrite the log for the snapshot: {error}"),
            ));
        }
        self.sm.install_durable_snapshot(image.snapshot.clone());
        self.checkpoint_snapshot = image.snapshot;
        self.base_index = image.applied_index;
        self.base_term = image.applied_term;
        self.log.clear();
        self.last_applied = image.applied_index;
        self.commit_index = self.commit_index.max(image.applied_index);
        self.compaction_floor = self.compaction_floor.max(image.compaction_floor);
        self.applied_results.clear();
        self.snapshot_receive = None;
        self.snapshot_export = None;
        self.snapshot_sends.clear();
        Ok(())
    }

    pub fn take_applied_result(&mut self, index: u64) -> Option<KvResult> {
        self.applied_results.remove(&index)
    }

    pub fn get(&self, key: &str) -> Option<KvValue> {
        self.sm.get(key)
    }

    pub fn scan_prefix(&self, prefix: &str) -> Vec<(String, KvValue)> {
        self.sm.scan_prefix(prefix)
    }

    pub fn revision(&self) -> u64 {
        self.sm.revision()
    }

    pub fn watch_prefix(&mut self, prefix: impl Into<String>) -> Receiver<WatchEvent> {
        let (tx, rx) = mpsc::channel();
        self.sm.watch_prefix(prefix, tx);
        rx
    }
}

/// Thread-safe handle to `MetaKv` supporting both zero-hop single-node mode and
/// multi-node Raft cluster replication over HTTP (`/raft/rpc`).
#[derive(Clone)]
pub struct LocalMetaKv<FS: StorageFs> {
    inner: Option<Arc<Mutex<RaftCore<FS>>>>,
    #[cfg(any(test, feature = "openraft-storage"))]
    openraft: Option<crate::openraft_client::GroupRouter>,
    peer_addrs: Arc<BTreeMap<u32, String>>,
    last_leader_contact: Arc<Mutex<Instant>>,
    /// Shared secret presented to peers on the internal Raft endpoint.
    raft_token: Option<Arc<str>>,
}

impl<FS: StorageFs> LocalMetaKv<FS> {
    fn legacy(&self) -> &Mutex<RaftCore<FS>> {
        self.inner
            .as_deref()
            .expect("legacy Raft unavailable for OpenRaft backend")
    }

    /// Whether this handle routes metadata writes through OpenRaft.
    pub fn is_openraft(&self) -> bool {
        #[cfg(any(test, feature = "openraft-storage"))]
        {
            return self.openraft.is_some();
        }
        #[cfg(not(any(test, feature = "openraft-storage")))]
        {
            false
        }
    }

    /// Wraps an existing OpenRaft and its shared durable state. `peer_addrs`
    /// contains public HTTP addresses, not the Raft transport addresses.
    /// Reads are local-applied and may be stale, not linearizable. Writes on
    /// current-thread Tokio runtimes (including their blocking threads) are
    /// rejected before submission. Multi-thread runtimes use block_in_place.
    #[cfg(any(test, feature = "openraft-storage"))]
    pub fn from_openraft(
        raft: crate::openraft_runtime::YadbRaft,
        state: crate::openraft_state_machine::DurableMetaStateMachine,
        peer_addrs: BTreeMap<u32, String>,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            inner: None,
            openraft: Some(crate::openraft_client::GroupRouter::singleton(
                crate::openraft_client::OpenRaftClient::new(raft, state, handle),
            )),
            peer_addrs: Arc::new(peer_addrs),
            last_leader_contact: Arc::new(Mutex::new(Instant::now())),
            raft_token: None,
        }
    }
    /// Explicit immutable project routing; groups must contain a separate
    /// control group and at least two distinct mapped data groups.
    /// The owner must persist and verify the mapping and bind each supplied
    /// Raft handle to its matching state machine and group identity. Infallible
    /// read methods panic on invalid scope or unavailable state; use
    /// `try_snapshot` where errors must be handled. Namespace-wide scans merge
    /// local views without an atomic cross-group revision.
    #[cfg(any(test, feature = "openraft-storage"))]
    pub fn from_openraft_groups(
        control: u64,
        groups: BTreeMap<
            u64,
            (
                crate::openraft_runtime::YadbRaft,
                crate::openraft_state_machine::DurableMetaStateMachine,
            ),
        >,
        projects: BTreeMap<u32, u64>,
        peer_addrs: BTreeMap<u32, String>,
        handle: tokio::runtime::Handle,
    ) -> Result<Self, MetaError> {
        let router = crate::openraft_client::GroupRouter::new(control, groups, projects, handle)?;
        Ok(Self {
            inner: None,
            openraft: Some(router),
            peer_addrs: Arc::new(peer_addrs),
            last_leader_contact: Arc::new(Mutex::new(Instant::now())),
            raft_token: None,
        })
    }

    pub fn revision_for_key(&self, key: &str) -> Result<u64, MetaError> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(router) = &self.openraft {
            return router.revision_for_key(key);
        }
        let _ = key;
        Ok(self.revision())
    }

    /// Return the locally observed role of the group owning this key; it is
    /// not proof that the reported leader currently has quorum.
    pub fn role_info_for_key(
        &self,
        key: &str,
    ) -> Result<(u32, RoleState, u64, Option<u32>, u64), MetaError> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(router) = &self.openraft {
            return router.role_for_key(key);
        }
        let _ = key;
        Ok(self.role_info())
    }
    pub fn open_single_node(fs: &FS, dir: &str, node_id: u32) -> Result<Self, MetaError> {
        let core = RaftCore::open(fs, dir, node_id, vec![node_id], true)?;
        Ok(Self {
            inner: Some(Arc::new(Mutex::new(core))),
            #[cfg(any(test, feature = "openraft-storage"))]
            openraft: None,
            peer_addrs: Arc::new(BTreeMap::new()),
            last_leader_contact: Arc::new(Mutex::new(Instant::now())),
            raft_token: None,
        })
    }

    /// Clustered mode without an internal Raft token.
    ///
    /// Deprecated for deployments: the internal Raft endpoint is then
    /// unauthenticated and any client can inject Raft messages. Use
    /// [`Self::open_clustered_with_token`] instead.
    #[deprecated(note = "use open_clustered_with_token so the Raft endpoint is authenticated")]
    pub fn open_clustered(
        fs: &FS,
        dir: &str,
        node_id: u32,
        peer_addrs: BTreeMap<u32, String>,
    ) -> Result<Self, MetaError> {
        let mut peer_ids: Vec<u32> = peer_addrs.keys().copied().collect();
        if !peer_ids.contains(&node_id) {
            peer_ids.push(node_id);
        }
        let core = RaftCore::open(fs, dir, node_id, peer_ids, true)?;
        Ok(Self {
            inner: Some(Arc::new(Mutex::new(core))),
            #[cfg(any(test, feature = "openraft-storage"))]
            openraft: None,
            peer_addrs: Arc::new(peer_addrs),
            last_leader_contact: Arc::new(Mutex::new(Instant::now())),
            raft_token: None,
        })
    }

    /// Clustered mode with the shared secret required by the internal Raft
    /// endpoint. An empty token is rejected: it would leave the Raft RPC
    /// endpoint unauthenticated.
    pub fn open_clustered_with_token(
        fs: &FS,
        dir: &str,
        node_id: u32,
        peer_addrs: BTreeMap<u32, String>,
        token: String,
    ) -> Result<Self, MetaError> {
        if token.is_empty() {
            return Err(MetaError::StorageUnavailable);
        }
        let mut peer_ids: Vec<u32> = peer_addrs.keys().copied().collect();
        if !peer_ids.contains(&node_id) {
            peer_ids.push(node_id);
        }
        let core = RaftCore::open(fs, dir, node_id, peer_ids, true)?;
        Ok(Self {
            inner: Some(Arc::new(Mutex::new(core))),
            #[cfg(any(test, feature = "openraft-storage"))]
            openraft: None,
            peer_addrs: Arc::new(peer_addrs),
            last_leader_contact: Arc::new(Mutex::new(Instant::now())),
            raft_token: Some(Arc::from(token.as_str())),
        })
    }

    /// The shared secret used for internal Raft RPCs.
    pub fn raft_token(&self) -> Option<&str> {
        self.raft_token.as_deref()
    }

    /// The last value is the commit index for legacy Raft and the locally
    /// applied log index for OpenRaft; it is not a quorum-read guarantee.
    pub fn role_info(&self) -> (u32, RoleState, u64, Option<u32>, u64) {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client.role_info();
        }
        let g = self.legacy().lock().unwrap();
        (
            g.node_id(),
            g.role(),
            g.current_term(),
            g.leader_id(),
            g.commit_index(),
        )
    }

    pub fn compaction_info(&self) -> Result<(u64, u64, u64), MetaError> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if self.openraft.is_some() {
            return Ok((0, 0, 0));
        }
        let g = self.legacy().lock().unwrap();
        Ok((g.base_index(), g.compaction_floor(), g.wal_bytes()?))
    }

    /// The reason this node stopped accepting metadata work after a failed
    /// checkpoint or WAL rewrite. `None` on a healthy node and on OpenRaft.
    pub fn compaction_error(&self) -> Option<String> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if self.openraft.is_some() {
            return None;
        }
        self.legacy().lock().unwrap().compaction_error()
    }

    pub fn node_id(&self) -> u32 {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client.role_info().0;
        }
        self.legacy().lock().unwrap().node_id()
    }

    pub fn peer_addr(&self, node_id: u32) -> Option<String> {
        self.peer_addrs.get(&node_id).cloned()
    }

    pub fn peer_addrs(&self) -> &BTreeMap<u32, String> {
        &self.peer_addrs
    }

    /// Handles an inbound Raft RPC (`RequestVote` or `AppendEntries`) and returns any immediate reply RPC.
    pub fn step_rpc(&self, rpc: RaftRpc) -> Result<Option<RaftRpc>, MetaError> {
        if self.is_openraft() {
            return Err(MetaError::StorageUnavailable);
        }
        let is_heartbeat = matches!(&rpc, RaftRpc::AppendEntries { .. });
        let reply = {
            let mut guard = self.legacy().lock().unwrap();
            let cur_term = guard.current_term();
            let valid_ae = match &rpc {
                RaftRpc::AppendEntries { term, .. } => *term >= cur_term,
                _ => false,
            };
            let mut out = guard.step(rpc)?;
            let r = out.pop().map(|m| m.rpc);
            let granted_vote = matches!(
                &r,
                Some(RaftRpc::RequestVoteReply {
                    vote_granted: true,
                    ..
                })
            );
            if (is_heartbeat && valid_ae) || granted_vote {
                *self.last_leader_contact.lock().unwrap() = Instant::now();
            }
            r
        };
        Ok(reply)
    }

    /// Triggers a Raft election round across configured peers and returns the resulting role.
    pub fn trigger_election(&self) -> Result<RoleState, MetaError> {
        if self.is_openraft() {
            return Err(MetaError::StorageUnavailable);
        }
        *self.last_leader_contact.lock().unwrap() = Instant::now();
        let initial_msgs = {
            let mut guard = self.legacy().lock().unwrap();
            guard.start_election()?
        };
        self.drive_outbound(initial_msgs)?;
        let role = self.legacy().lock().unwrap().role();
        if role == RoleState::Leader {
            *self.last_leader_contact.lock().unwrap() = Instant::now();
        }
        Ok(role)
    }

    /// Checks whether the follower election timeout has elapsed without valid Leader heartbeats;
    /// if so, automatically triggers a new Raft election round!
    pub fn check_election_timeout(&self, timeout: Duration) -> Result<bool, MetaError> {
        if self.is_openraft() {
            return Ok(false);
        }
        if self.peer_addrs.len() <= 1 {
            return Ok(false);
        }
        let is_leader = self.legacy().lock().unwrap().role() == RoleState::Leader;
        if is_leader {
            *self.last_leader_contact.lock().unwrap() = Instant::now();
            return Ok(false);
        }
        let elapsed = self.last_leader_contact.lock().unwrap().elapsed();
        if elapsed >= timeout {
            let _ = self.trigger_election()?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Broadcasts a heartbeat / catch-up `AppendEntries` round if this node is Leader.
    pub fn broadcast_heartbeat(&self) -> Result<(), MetaError> {
        if self.is_openraft() {
            return Ok(());
        }
        let msgs = {
            let mut guard = self.legacy().lock().unwrap();
            if guard.role() == RoleState::Leader {
                *self.last_leader_contact.lock().unwrap() = Instant::now();
            }
            guard.broadcast_append_entries_checked()?
        };
        self.drive_outbound(msgs)
    }

    fn drive_outbound(&self, pending: Vec<OutboundMsg>) -> Result<(), MetaError> {
        let token = self.raft_token.clone();
        let mut send =
            move |addr: &str, rpc: &RaftRpc| send_raft_http_rpc(addr, rpc, token.as_deref());
        self.drive_outbound_with(pending, &mut send)
    }

    fn drive_outbound_with(
        &self,
        pending: Vec<OutboundMsg>,
        send: &mut impl FnMut(&str, &RaftRpc) -> Result<Option<RaftRpc>, std::io::Error>,
    ) -> Result<(), MetaError> {
        self.drive_round(pending, send).map(|_| ())
    }

    /// Runs one cascade of replication rounds and returns how many peer replies
    /// it applied. Callers that are waiting for a specific entry use the count
    /// to distinguish "no peer answered at all" from "a peer answered but the
    /// entry is still not committed".
    fn drive_round(
        &self,
        mut pending: Vec<OutboundMsg>,
        send: &mut impl FnMut(&str, &RaftRpc) -> Result<Option<RaftRpc>, std::io::Error>,
    ) -> Result<usize, MetaError> {
        let mut replies_applied = 0;
        // Bound synchronous cascade rounds (e.g. RequestVote -> become_leader -> AppendEntries -> commit broadcast)
        for _round in 0..8 {
            if pending.is_empty() {
                break;
            }
            let batch = std::mem::take(&mut pending);
            let mut replies = Vec::new();
            for msg in batch {
                if let Some(addr) = self.peer_addrs.get(&msg.to) {
                    if let Ok(Some(reply_rpc)) = send(addr, &msg.rpc) {
                        replies.push(reply_rpc);
                    }
                }
            }
            if replies.is_empty() {
                break;
            }
            let mut guard = self.legacy().lock().unwrap();
            for rep in replies {
                let next_msgs = guard.step(rep)?;
                pending.extend(next_msgs);
                replies_applied += 1;
            }
        }
        Ok(replies_applied)
    }

    /// Returns the result only for this proposal's (index, term). An error after
    /// proposing is not a rollback guarantee: replication may still commit it.
    pub fn execute(&self, cmd: KvCommand) -> Result<KvResult, MetaError> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client.execute(cmd);
        }
        let token = self.raft_token.clone();
        let mut send =
            move |addr: &str, rpc: &RaftRpc| send_raft_http_rpc(addr, rpc, token.as_deref());
        self.execute_with(cmd, &mut send)
    }

    fn execute_with(
        &self,
        cmd: KvCommand,
        send: &mut impl FnMut(&str, &RaftRpc) -> Result<Option<RaftRpc>, std::io::Error>,
    ) -> Result<KvResult, MetaError> {
        let (idx, term, msgs) = {
            let mut guard = self.legacy().lock().unwrap();
            let (idx, msgs) = guard.propose(cmd)?;
            (idx, guard.current_term(), msgs)
        };
        if !msgs.is_empty() {
            self.drive_round(msgs, send)?;
        }
        // A proposal that is already in the log can commit at most once, so
        // waiting for it is not a retry: it only decides whether the client sees
        // the committed result or an explicit unknown outcome. A concurrent
        // proposer may have advanced `next_index` past our entry before we sent
        // it, in which case `propose` returns no messages for us and there is no
        // acknowledgement of our own to collect. Without this wait, 24 concurrent
        // PUTs on one Leader reported four committed writes as
        // `UncommittedIndex` merely because another thread had already shipped
        // the entry.
        let deadline = Instant::now() + COMMIT_WAIT;
        let mut heard_from_peer = false;
        let mut empty_rounds = 0u32;
        loop {
            let nudge = {
                let mut guard = self.legacy().lock().unwrap();
                // A new leader may have replaced the uncommitted proposal while
                // the mutex was released for RPCs. An index alone does not
                // identify an entry across terms; never return (or consume) the
                // replacement's result.
                if guard.term_at(idx) != Some(term) {
                    return Err(MetaError::UncommittedIndex(idx));
                }
                if let Some(res) = guard.take_applied_result(idx) {
                    return Ok(res);
                }
                // A peer may already hold the entry, so a heartbeat round
                // collects its acknowledgement instead of waiting for the next
                // timer tick. A replication failure is surfaced, not flattened
                // into "no messages": that would turn a concrete error such as
                // a compacted base into an ambiguous timeout.
                guard.broadcast_append_entries_checked()
            }?;
            if Instant::now() >= deadline {
                return Err(MetaError::UncommittedIndex(idx));
            }
            let replies = self.drive_round(nudge, send)?;
            if replies > 0 {
                heard_from_peer = true;
                empty_rounds = 0;
            } else {
                empty_rounds += 1;
                // Several rounds without a single peer reply mean there is no
                // quorum to wait for, so refuse instead of polling refused
                // connections for the whole budget.
                if empty_rounds >= COMMIT_WAIT_EMPTY_ROUNDS {
                    return Err(MetaError::UncommittedIndex(idx));
                }
            }
            // A round that reached a peer is worth retrying immediately; one
            // that reached nobody should back off rather than spin.
            std::thread::sleep(Duration::from_millis(if heard_from_peer { 2 } else { 20 }));
        }
    }

    pub fn get(&self, key: &str) -> Option<KvValue> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client
                .try_snapshot(&[key.to_owned()], &[])
                .expect("invalid or unavailable metadata read")
                .get(key);
        }
        self.legacy().lock().unwrap().get(key)
    }

    /// Copies selected keys/ranges while holding the same mutex as Raft apply.
    /// Release it before reading blobs/network I/O; later writes cannot change
    /// the owned values. Unselected keys are not available in this snapshot.
    pub fn snapshot(&self, keys: &[String], prefixes: &[String]) -> MetaSnapshot {
        self.try_snapshot(keys, prefixes)
            .expect("invalid or unavailable metadata snapshot")
    }

    pub fn try_snapshot(
        &self,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client.try_snapshot(keys, prefixes);
        }
        let guard = self.legacy().lock().unwrap();
        let mut values = BTreeMap::new();
        for key in keys {
            if let Some(value) = guard.get(key) {
                values.insert(key.clone(), value);
            }
        }
        for prefix in prefixes {
            values.extend(guard.scan_prefix(prefix));
        }
        Ok(MetaSnapshot {
            revision: guard.revision(),
            values,
        })
    }

    /// Quorum-confirmed snapshot of one selected group, including its revision.
    /// Followers, unavailable quorums, cross-group requests and legacy metadata
    /// return errors; this does not provide cross-group atomicity.
    pub fn try_linearizable_snapshot(
        &self,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client.linearizable_snapshot(keys, prefixes);
        }
        Err(MetaError::Routing(
            "linearizable snapshot requires OpenRaft".into(),
        ))
    }

    /// Quorum-confirmed local snapshot of the control group. This only works on
    /// the current control-group leader; other replicas must ask that leader.
    /// Legacy metadata has no separate control group and is rejected here.
    pub fn try_linearizable_control_snapshot(
        &self,
        keys: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client.linearizable_control_snapshot(keys);
        }
        Err(MetaError::Routing(
            "linearizable control snapshot requires OpenRaft".into(),
        ))
    }

    /// Record namespace and admission-counter key for a project's idempotency
    /// records.
    ///
    /// The record is project-scoped (`/idempotency/<project-id>/…`) so two
    /// projects that share a Raft group keep separate keyspaces; the counter is
    /// group-scoped (`/idempotency/g<group>/_usage`) because the limit it
    /// enforces is the group's state image, not the project's. Both keys route to
    /// the same group, which is what lets one `RevisionTxn` guard them together
    /// with the mutation they record.
    ///
    /// `project` is the canonical decimal project id, not its name: the router
    /// resolves it against its immutable mapping and fails closed for an unmapped
    /// or non-canonical value. A fallback to control would put the record in a
    /// group that never resolves it back to the project.
    pub fn project_idempotency_scope(&self, project: &str) -> Result<(String, String), MetaError> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            let records = client.idempotency_namespace(Some(project))?;
            let group = client.group_id_for_project(Some(project))?;
            return Ok((records, client.group_idempotency_usage_key(group)?));
        }
        let _ = project;
        Err(MetaError::Routing(
            "project idempotency requires OpenRaft".into(),
        ))
    }

    pub fn scan_prefix(&self, prefix: &str) -> Vec<(String, KvValue)> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client.scan(prefix);
        }
        self.legacy().lock().unwrap().scan_prefix(prefix)
    }

    pub fn revision(&self) -> u64 {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client.revision();
        }
        self.legacy().lock().unwrap().revision()
    }

    /// Revision to stamp a *legacy-only* GC lease with, or `0` on OpenRaft.
    ///
    /// Multi-Raft metadata has no global revision (`revision()` asserts that),
    /// and collection is disabled there, so no lease is ever published and the
    /// value is unused. Callers that pass a revision to
    /// `gc::acquire_writer_if_legacy` must use this: evaluating `revision()`
    /// eagerly as an argument panics before the legacy check can run.
    pub fn legacy_revision(&self) -> u64 {
        if self.is_openraft() {
            return 0;
        }
        self.legacy().lock().unwrap().revision()
    }

    pub fn watch_prefix(&self, prefix: impl Into<String>) -> Receiver<WatchEvent> {
        #[cfg(any(test, feature = "openraft-storage"))]
        if let Some(client) = &self.openraft {
            return client.watch_prefix(prefix.into());
        }
        self.legacy().lock().unwrap().watch_prefix(prefix)
    }
}

fn send_raft_http_rpc(
    peer_addr: &str,
    rpc: &RaftRpc,
    token: Option<&str>,
) -> Result<Option<RaftRpc>, std::io::Error> {
    let clean = peer_addr
        .trim()
        .strip_prefix("http://")
        .unwrap_or(peer_addr.trim())
        .trim_end_matches('/');
    let sock: SocketAddr = clean
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let body = serde_json::to_vec(rpc)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut stream = TcpStream::connect_timeout(&sock, Duration::from_millis(350))?;
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    stream.set_write_timeout(Some(Duration::from_millis(350)))?;

    let req = match token {
        Some(token) => format!(
            "POST /raft/rpc HTTP/1.1\r\nHost: {clean}\r\nx-yadb-raft-token: {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        ),
        None => format!(
            "POST /raft/rpc HTTP/1.1\r\nHost: {clean}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        ),
    };
    stream.write_all(req.as_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let Some(split) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Ok(None);
    };
    let payload = &raw[split + 4..];
    if payload.is_empty() {
        return Ok(None);
    }
    let parsed: Option<RaftRpc> = serde_json::from_slice(payload).ok().flatten();
    Ok(parsed)
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use yadb_io::SimFs;

    fn cluster() -> BTreeMap<u32, LocalMetaKv<SimFs>> {
        let peers: BTreeMap<_, _> = (1..=3).map(|id| (id, id.to_string())).collect();
        let nodes: BTreeMap<_, _> = (1..=3)
            .map(|id| {
                let fs = SimFs::new(2000 + id as u64);
                let meta = LocalMetaKv::open_clustered(&fs, "meta", id, peers.clone()).unwrap();
                (id, meta)
            })
            .collect();
        elect(&nodes, 1);
        nodes
    }

    fn deliver(
        nodes: &BTreeMap<u32, LocalMetaKv<SimFs>>,
        addr: &str,
        rpc: &RaftRpc,
    ) -> Result<Option<RaftRpc>, std::io::Error> {
        Ok(nodes[&addr.parse::<u32>().unwrap()]
            .step_rpc(rpc.clone())
            .unwrap())
    }

    fn elect(nodes: &BTreeMap<u32, LocalMetaKv<SimFs>>, id: u32) {
        let msgs = nodes[&id]
            .legacy()
            .lock()
            .unwrap()
            .start_election()
            .unwrap();
        nodes[&id]
            .drive_outbound_with(msgs, &mut |addr, rpc| deliver(nodes, addr, rpc))
            .unwrap();
        assert_eq!(nodes[&id].role_info().1, RoleState::Leader);
    }

    #[test]
    fn execute_error_can_be_followed_by_commit_or_discard_after_election() {
        // The caller sees the same error in both cases, but the new leader either
        // retains the proposal (delivery followed by reply loss) or replaces it
        // (failure before delivery). There is no automatic retry of the command.
        for delivered in [false, true] {
            let nodes = cluster();
            let result = nodes[&1].execute_with(
                KvCommand::Put {
                    key: "ambiguous".into(),
                    value: b"original".to_vec(),
                },
                &mut |addr, rpc| {
                    if delivered && addr == "2" {
                        let reply = deliver(&nodes, addr, rpc)?;
                        assert!(matches!(
                            reply,
                            Some(RaftRpc::AppendEntriesReply { success: true, .. })
                        ));
                    }
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "injected reply loss",
                    ))
                },
            );
            assert!(
                matches!(result, Err(MetaError::UncommittedIndex(_))),
                "{result:?}"
            );
            assert!(nodes[&1].get("ambiguous").is_none());
            elect(&nodes, 2);
            for node in nodes.values() {
                assert_eq!(node.get("ambiguous").is_some(), delivered);
            }
        }
    }

    // A concurrent burst can time out both of a proposer's replication RPCs
    // even though its entry is in the log and another proposer will commit it.
    // Reporting `UncommittedIndex` there refused committed writes: in a 24-way
    // PUT burst four writes were refused this way. A later round that does reach
    // a peer must still return the committed result.
    #[test]
    fn execute_waits_for_commit_after_a_round_that_reached_no_peer() {
        let nodes = cluster();
        let mut sends = 0;
        let result = nodes[&1].execute_with(
            KvCommand::Put {
                key: "burst".into(),
                value: b"value".to_vec(),
            },
            &mut |addr, rpc| {
                sends += 1;
                // Both peers of the first round are reported as timed out, as a
                // real 500 ms Raft RPC timeout under load would be.
                if sends <= 2 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "injected round timeout",
                    ));
                }
                deliver(&nodes, addr, rpc)
            },
        );
        assert!(matches!(result, Ok(KvResult::PutOk { .. })), "{result:?}");
        assert!(nodes[&1].get("burst").is_some());
        assert!(
            sends > 2,
            "the wait must retry replication, not the command"
        );
    }

    #[test]
    fn execute_must_not_return_result_of_replacement_at_same_index() {
        let nodes = cluster();
        let mut changed_leader = false;
        let result = nodes[&1].execute_with(
            KvCommand::Put {
                key: "discarded".into(),
                value: b"original".to_vec(),
            },
            &mut |addr, rpc| {
                if !changed_leader {
                    changed_leader = true;
                    // Before the proposal reaches any follower, elect node 2. Its
                    // no-op replaces and commits the pending proposal's log index
                    // on node 1 while that node's execute() is outside its mutex.
                    elect(&nodes, 2);
                }
                deliver(&nodes, addr, rpc)
            },
        );
        assert!(
            matches!(result, Err(MetaError::UncommittedIndex(_))),
            "{result:?}"
        );
        assert!(nodes.values().all(|node| node.get("discarded").is_none()));
        // Rejecting the stale caller must not consume the replacement's result.
        assert!(matches!(
            nodes[&1].legacy().lock().unwrap().take_applied_result(2),
            Some(KvResult::TxnOk { .. })
        ));
    }

    #[test]
    fn execute_must_not_return_success_for_replacement_put() {
        let nodes = cluster();
        // Leave an earlier unreplicated entry at index 2, so the original Put
        // below uses index 3. The new leader's no-op will occupy index 2 and its
        // own Put will occupy index 3.
        let pending = nodes[&1].execute_with(
            KvCommand::Put {
                key: "earlier".into(),
                value: b"pending".to_vec(),
            },
            &mut |_, _| Ok(None),
        );
        assert!(matches!(pending, Err(MetaError::UncommittedIndex(2))));
        let mut changed_leader = false;
        let result = nodes[&1].execute_with(
            KvCommand::Put {
                key: "discarded".into(),
                value: b"original".to_vec(),
            },
            &mut |addr, rpc| {
                if !changed_leader {
                    changed_leader = true;
                    elect(&nodes, 2);
                    let replacement = nodes[&2]
                        .execute_with(
                            KvCommand::Put {
                                key: "replacement".into(),
                                value: b"new leader".to_vec(),
                            },
                            &mut |addr, rpc| deliver(&nodes, addr, rpc),
                        )
                        .unwrap();
                    assert!(matches!(replacement, KvResult::PutOk { .. }));
                }
                deliver(&nodes, addr, rpc)
            },
        );
        assert!(
            matches!(result, Err(MetaError::UncommittedIndex(_))),
            "{result:?}"
        );
        assert!(nodes.values().all(|node| node.get("discarded").is_none()));
        for node in nodes.values() {
            assert_eq!(node.get("replacement").unwrap().value, b"new leader");
        }
    }

    #[test]
    fn execute_returns_original_committed_result_after_leader_change() {
        let nodes = cluster();
        let mut changed_leader = false;
        let result = nodes[&1].execute_with(
            KvCommand::Put {
                key: "retained".into(),
                value: b"original".to_vec(),
            },
            &mut |addr, rpc| {
                // The first replication round commits the Put on node 1. Change
                // leaders during the subsequent commit broadcast, before execute
                // reads its result. Rejecting every term change would lose success.
                if !changed_leader
                    && matches!(rpc, RaftRpc::AppendEntries { leader_commit, .. }
                if *leader_commit >= 2)
                {
                    changed_leader = true;
                    elect(&nodes, 2);
                }
                deliver(&nodes, addr, rpc)
            },
        );
        assert!(changed_leader);
        assert!(
            matches!(result, Ok(KvResult::PutOk { version: 1, .. })),
            "{result:?}"
        );
        for node in nodes.values() {
            assert_eq!(node.get("retained").unwrap().value, b"original");
        }
    }

    #[test]
    fn execute_succeeds_when_commit_notification_is_lost() {
        let nodes = cluster();
        let mut lost_notifications = 0;
        let result = nodes[&1].execute_with(KvCommand::Put {
            key: "committed".into(), value: b"original".to_vec(),
        }, &mut |addr, rpc| {
            if matches!(rpc, RaftRpc::AppendEntries { leader_commit, .. } if *leader_commit >= 2) {
                lost_notifications += 1;
                return Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "lost commit broadcast"));
            }
            deliver(&nodes, addr, rpc)
        });
        assert!(lost_notifications > 0);
        assert!(
            matches!(result, Ok(KvResult::PutOk { version: 1, .. })),
            "{result:?}"
        );
        assert!(nodes[&2].get("committed").is_none());
        let msgs = nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .broadcast_append_entries();
        nodes[&1]
            .drive_outbound_with(msgs, &mut |addr, rpc| deliver(&nodes, addr, rpc))
            .unwrap();
        for node in nodes.values() {
            assert_eq!(node.get("committed").unwrap().value, b"original");
        }
    }
}

#[cfg(test)]
mod compaction_tests {
    use super::*;
    use yadb_io::{FaultConfig, SimFs, SyncFault};

    const DIR: &str = "meta";

    fn key(i: u32) -> String {
        format!("/k/{i:03}")
    }

    fn value(i: u32) -> Vec<u8> {
        vec![i as u8]
    }

    fn put_cmd(i: u32) -> KvCommand {
        KvCommand::Put {
            key: key(i),
            value: value(i),
        }
    }

    fn try_single(fs: &SimFs) -> Result<RaftCore<SimFs>, MetaError> {
        RaftCore::open(fs, DIR, 1, vec![1], true)
    }

    fn single(fs: &SimFs) -> RaftCore<SimFs> {
        try_single(fs).unwrap()
    }

    fn put(core: &mut RaftCore<SimFs>, i: u32) {
        core.propose(put_cmd(i)).unwrap();
    }

    /// The checkpoint `maybe_compact` would write for `index`.
    fn checkpoint_at(core: &RaftCore<SimFs>, index: u64) -> DurableCheckpoint {
        DurableCheckpoint {
            format_version: 1,
            applied_index: index,
            applied_term: core.term_at(index).expect("index must be in the log"),
            commit_index: core.commit_index(),
            hard_state: DurableHardState {
                term: core.current_term(),
                voted_for: core.voted_for(),
            },
            compaction_floor: core.compaction_floor(),
            // Written as a version-1 image on purpose: the reader must keep
            // accepting pre-membership checkpoints.
            membership: Vec::new(),
            snapshot: core.snapshot_at(index),
        }
    }

    fn write_checkpoint(fs: &SimFs, checkpoint: &DurableCheckpoint) {
        fs.create_dir_all(DIR).unwrap();
        let file = fs.open_rw(&checkpoint_path(DIR), true).unwrap();
        let mut value = serde_json::to_value(checkpoint).unwrap();
        if checkpoint.format_version == 1 {
            // A genuine version-1 image predates the membership field.
            value.as_object_mut().unwrap().remove("membership");
        }
        let bytes = serde_json::to_vec(&value).unwrap();
        file.set_len(0).unwrap();
        file.write_at(0, &bytes).unwrap();
        file.sync_data().unwrap();
    }

    fn cluster_fs(ids: &[u32]) -> BTreeMap<u32, SimFs> {
        ids.iter()
            .map(|&id| (id, SimFs::new(5000 + id as u64)))
            .collect()
    }

    fn open_cluster(fss: &BTreeMap<u32, SimFs>) -> BTreeMap<u32, LocalMetaKv<SimFs>> {
        let peers: BTreeMap<u32, String> = fss.keys().map(|&id| (id, id.to_string())).collect();
        fss.iter()
            .map(|(&id, fs)| {
                let meta = LocalMetaKv::open_clustered_with_token(
                    fs,
                    DIR,
                    id,
                    peers.clone(),
                    "test-token".to_string(),
                )
                .unwrap();
                (id, meta)
            })
            .collect()
    }

    fn deliver(
        nodes: &BTreeMap<u32, LocalMetaKv<SimFs>>,
        addr: &str,
        rpc: &RaftRpc,
    ) -> Result<Option<RaftRpc>, std::io::Error> {
        Ok(nodes[&addr.parse::<u32>().unwrap()]
            .step_rpc(rpc.clone())
            .unwrap())
    }

    fn deliver_but(
        nodes: &BTreeMap<u32, LocalMetaKv<SimFs>>,
        isolated: &BTreeSet<u32>,
        addr: &str,
        rpc: &RaftRpc,
    ) -> Result<Option<RaftRpc>, std::io::Error> {
        if isolated.contains(&addr.parse::<u32>().unwrap()) {
            return Ok(None);
        }
        deliver(nodes, addr, rpc)
    }

    fn elect(nodes: &BTreeMap<u32, LocalMetaKv<SimFs>>, id: u32) {
        let msgs = nodes[&id]
            .legacy()
            .lock()
            .unwrap()
            .start_election()
            .unwrap();
        nodes[&id]
            .drive_outbound_with(msgs, &mut |addr, rpc| deliver(nodes, addr, rpc))
            .unwrap();
        assert_eq!(nodes[&id].role_info().1, RoleState::Leader);
    }

    fn propose_on(
        nodes: &BTreeMap<u32, LocalMetaKv<SimFs>>,
        leader: u32,
        i: u32,
        isolated: &BTreeSet<u32>,
    ) -> u64 {
        let (idx, msgs) = nodes[&leader]
            .legacy()
            .lock()
            .unwrap()
            .propose(put_cmd(i))
            .unwrap();
        nodes[&leader]
            .drive_outbound_with(msgs, &mut |addr, rpc| {
                deliver_but(nodes, isolated, addr, rpc)
            })
            .unwrap();
        idx
    }

    #[test]
    fn compaction_bounds_the_wal_and_preserves_state_across_reopen() {
        let fs = SimFs::new(4101);
        let mut core = single(&fs);
        for i in 0..50u32 {
            put(&mut core, i);
        }
        let wal_before = core.wal_bytes().unwrap();
        let revision_before = core.revision();
        assert_eq!(core.base_index(), 0);

        core.set_compaction_thresholds(1, 1);
        put(&mut core, 50);

        let base = core.base_index();
        let floor = core.compaction_floor();
        let wal_after = core.wal_bytes().unwrap();
        assert!(base > 0, "compaction must advance the base");
        assert_eq!(floor, base);
        assert!(wal_after < wal_before, "{wal_after} !< {wal_before}");
        assert_eq!(core.revision(), revision_before + 1);
        assert_eq!(core.compaction_error(), None);
        drop(core);

        let core = single(&fs);
        assert_eq!(core.base_index(), base);
        assert_eq!(core.compaction_floor(), floor);
        assert_eq!(core.revision(), revision_before + 1);
        for i in 0..=50u32 {
            assert_eq!(
                core.get(&key(i)).unwrap().value,
                value(i),
                "key {i} must survive compaction"
            );
        }
        assert_eq!(
            core.wal_bytes().unwrap(),
            wal_after,
            "reopening a compacted WAL must not append to it"
        );
    }

    #[test]
    fn checkpoint_renamed_before_the_wal_rewrite_is_reconciled_on_open() {
        let fs = SimFs::new(4102);
        let mut core = single(&fs);
        for i in 0..20u32 {
            put(&mut core, i);
        }
        core.set_compaction_thresholds(1, 1);
        put(&mut core, 20);
        let base_after_compaction = core.base_index();
        assert!(base_after_compaction > 0);

        // Entries above the base stay in the WAL, so the on-disk checkpoint
        // still covers only `base_after_compaction`.
        core.set_compaction_thresholds(u64::MAX, u64::MAX);
        for i in 21..30u32 {
            put(&mut core, i);
        }
        let applied = core.last_applied;
        assert!(applied > base_after_compaction);

        // The crash window: the checkpoint for `applied` reached the disk, the
        // WAL rewrite did not. Write that checkpoint by hand and reopen.
        let checkpoint = checkpoint_at(&core, applied);
        let revision = core.revision();
        let wal_before = core.wal_bytes().unwrap();
        write_checkpoint(&fs, &checkpoint);
        drop(core);

        let core = single(&fs);
        assert_eq!(
            core.base_index(),
            applied,
            "opening must rewrite the stale WAL to the checkpoint"
        );
        assert_eq!(core.revision(), revision);
        assert!(core.wal_bytes().unwrap() < wal_before);
        for i in 0..30u32 {
            assert_eq!(core.get(&key(i)).unwrap().value, value(i));
        }
    }

    #[test]
    fn a_compacted_base_without_a_checkpoint_refuses_to_start() {
        let fs = SimFs::new(4103);
        {
            let (mut wal, _) = RaftWal::open(&fs, DIR, true).unwrap();
            wal.append(&WalRecord::HardState {
                term: 4,
                voted_for: Some(2),
            })
            .unwrap();
            wal.append(&WalRecord::Base { index: 10, term: 2 }).unwrap();
        }
        match try_single(&fs) {
            Err(MetaError::Checkpoint(_)) => {}
            Err(other) => panic!("expected an explicit checkpoint error, got {other:?}"),
            Ok(_) => panic!("a compacted WAL without its state image must not open"),
        }
    }

    #[test]
    fn a_checkpoint_from_another_membership_refuses_to_start() {
        let fs = SimFs::new(4106);
        let mut core = single(&fs);
        for i in 0..3u32 {
            put(&mut core, i);
        }
        core.set_compaction_thresholds(1, 1);
        put(&mut core, 3);
        let base = core.base_index();
        assert!(base > 0);

        // A version-2 image records its voters; adopting it into a replica
        // configured for another set would silently install another group.
        let mut checkpoint = checkpoint_at(&core, base);
        checkpoint.format_version = CHECKPOINT_FORMAT_VERSION;
        checkpoint.membership = vec![1, 2, 3];
        write_checkpoint(&fs, &checkpoint);
        drop(core);

        match try_single(&fs) {
            Err(MetaError::Checkpoint(message)) => {
                assert!(message.contains("voters"), "{message}");
            }
            Err(other) => panic!("expected a membership refusal, got {other:?}"),
            Ok(_) => panic!("a checkpoint from another group must not open"),
        }
    }

    #[test]
    fn an_unparsable_checkpoint_refuses_to_start() {
        let fs = SimFs::new(4104);
        fs.create_dir_all(DIR).unwrap();
        let file = fs.open_rw(&checkpoint_path(DIR), true).unwrap();
        file.write_at(0, b"{not json").unwrap();
        match try_single(&fs) {
            Err(MetaError::Checkpoint(_)) => {}
            Err(other) => panic!("expected an explicit checkpoint error, got {other:?}"),
            Ok(_) => panic!("a damaged checkpoint must not silently re-initialise"),
        }
    }

    #[test]
    fn term_and_vote_survive_compaction() {
        let fs = SimFs::new(4105);
        let mut core = single(&fs);
        let term = core.current_term();
        let vote = core.voted_for();
        assert_eq!(vote, Some(1));
        for i in 0..5u32 {
            put(&mut core, i);
        }
        core.set_compaction_thresholds(1, 1);
        put(&mut core, 5);
        let base = core.base_index();
        assert!(base > 0);
        drop(core);

        // The rewritten WAL must carry the hard state itself, not only the
        // checkpoint: replaying it is what prevents double voting in a term.
        let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, term);
        assert_eq!(recovered.voted_for, vote);
        assert_eq!(recovered.base_index, base);

        let core = single(&fs);
        assert_eq!(core.current_term(), term);
        assert_eq!(core.voted_for(), vote);
    }

    #[test]
    fn compaction_never_drops_an_entry_above_a_stopped_peer_match() {
        let fss = cluster_fs(&[1, 2, 3]);
        let nodes = open_cluster(&fss);
        elect(&nodes, 1);
        let none = BTreeSet::new();
        for i in 0..10u32 {
            propose_on(&nodes, 1, i, &none);
        }
        let common = nodes[&1].legacy().lock().unwrap().commit_index();
        for i in 0..10u32 {
            assert!(nodes[&3].get(&key(i)).is_some());
        }

        let isolated: BTreeSet<u32> = [3].into_iter().collect();
        for i in 10..40u32 {
            propose_on(&nodes, 1, i, &isolated);
        }
        assert!(nodes[&3].get(&key(39)).is_none());

        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(1, 1);
        propose_on(&nodes, 1, 99, &isolated);

        let (base, floor) = {
            let g = nodes[&1].legacy().lock().unwrap();
            (g.base_index(), g.compaction_floor())
        };
        assert_eq!(
            base, common,
            "the leader must not compact past the stopped peer's match"
        );
        assert_eq!(floor, common);

        // Healing the link must let the stopped peer catch up from the base,
        // which only works because nothing above the floor was dropped.
        let msgs = nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .broadcast_append_entries_checked()
            .unwrap();
        nodes[&1]
            .drive_outbound_with(msgs, &mut |addr, rpc| deliver(&nodes, addr, rpc))
            .unwrap();
        for i in 0..40u32 {
            assert_eq!(nodes[&3].get(&key(i)).unwrap().value, value(i));
        }
        assert!(nodes[&3].get(&key(99)).is_some());
    }

    /// The first chunk the leader would stream for its current base.
    fn snapshot_chunk(
        nodes: &BTreeMap<u32, LocalMetaKv<SimFs>>,
        leader: u32,
        follower: u32,
    ) -> OutboundMsg {
        let base = nodes[&leader].legacy().lock().unwrap().base_index();
        assert!(base > 0, "the leader must have compacted first");
        let msgs = {
            let mut guard = nodes[&leader].legacy().lock().unwrap();
            guard.next_index.insert(follower, base);
            guard.broadcast_append_entries_checked().unwrap()
        };
        let msg = msgs
            .into_iter()
            .find(|m| m.to == follower)
            .expect("a chunk for the follower");
        assert!(
            matches!(msg.rpc, RaftRpc::InstallSnapshot { .. }),
            "{msg:?}"
        );
        msg
    }

    fn edited_chunk(msg: &OutboundMsg, edit: impl FnOnce(&mut RaftRpc)) -> RaftRpc {
        let mut rpc = msg.rpc.clone();
        edit(&mut rpc);
        rpc
    }

    fn expect_refusal(reply: Option<RaftRpc>, needle: &str) {
        match reply {
            Some(RaftRpc::InstallSnapshotReply {
                accepted: false,
                error: Some(error),
                next_offset,
                ..
            }) => {
                assert!(
                    error.contains(needle),
                    "{error:?} does not mention {needle:?}"
                );
                assert_eq!(next_offset, 0, "a refusal restarts the transfer");
            }
            other => panic!("expected a refusal mentioning {needle:?}, got {other:?}"),
        }
    }

    /// A replica that lost its whole directory comes back empty; the leader must
    /// discover that, stream the compacted image and then use the ordinary log.
    fn replace_with_empty_replica(
        fss: &mut BTreeMap<u32, SimFs>,
        nodes: &mut BTreeMap<u32, LocalMetaKv<SimFs>>,
        id: u32,
        seed: u64,
    ) {
        let fs = SimFs::new(seed);
        let peers: BTreeMap<u32, String> = fss.keys().map(|&i| (i, i.to_string())).collect();
        let meta =
            LocalMetaKv::open_clustered_with_token(&fs, DIR, id, peers, "test-token".to_string())
                .unwrap();
        fss.insert(id, fs);
        nodes.insert(id, meta);
    }

    /// Leader id and follower state agree and `last_key` is present.
    fn converged(nodes: &BTreeMap<u32, LocalMetaKv<SimFs>>, last_key: &str) -> bool {
        let (leader_base, leader_revision) = {
            let g = nodes[&1].legacy().lock().unwrap();
            (g.base_index(), g.revision())
        };
        let (follower_base, follower_revision) = {
            let g = nodes[&3].legacy().lock().unwrap();
            (g.base_index(), g.revision())
        };
        follower_base == leader_base
            && follower_revision == leader_revision
            && nodes[&3].get(last_key).is_some()
    }

    fn drive_until(
        nodes: &BTreeMap<u32, LocalMetaKv<SimFs>>,
        limit: usize,
        done: impl Fn(&BTreeMap<u32, LocalMetaKv<SimFs>>) -> bool,
    ) {
        for _ in 0..limit {
            if done(nodes) {
                return;
            }
            let msgs = nodes[&1]
                .legacy()
                .lock()
                .unwrap()
                .broadcast_append_entries_checked()
                .unwrap();
            nodes[&1]
                .drive_outbound_with(msgs, &mut |addr, rpc| deliver(nodes, addr, rpc))
                .unwrap();
        }
        assert!(
            done(nodes),
            "the cluster did not converge in {limit} rounds"
        );
    }

    #[test]
    fn a_replica_that_lost_its_log_rejoins_through_the_durable_image() {
        let mut fss = cluster_fs(&[1, 2, 3]);
        let mut nodes = open_cluster(&fss);
        elect(&nodes, 1);
        let none = BTreeSet::new();
        for i in 0..5u32 {
            propose_on(&nodes, 1, i, &none);
        }
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(1, 1);
        propose_on(&nodes, 1, 5, &none);
        let (base, revision) = {
            let g = nodes[&1].legacy().lock().unwrap();
            (g.base_index(), g.revision())
        };
        assert!(base > 0, "the leader must have compacted");

        // The replica comes back with an empty directory: no log, no state.
        replace_with_empty_replica(&mut fss, &mut nodes, 3, 7103);
        assert_eq!(nodes[&3].legacy().lock().unwrap().base_index(), 0);
        assert!(nodes[&3].get(&key(0)).is_none());

        drive_until(&nodes, 64, |nodes| converged(nodes, &key(5)));

        assert_eq!(
            nodes[&3].legacy().lock().unwrap().base_index(),
            base,
            "the follower must install the leader's image, not replay a log"
        );
        for i in 0..=5u32 {
            assert_eq!(
                nodes[&3].get(&key(i)).unwrap().value,
                value(i),
                "key {i} must arrive through the image"
            );
        }
        assert_eq!(
            nodes[&3]
                .legacy()
                .lock()
                .unwrap()
                .sm
                .durable_snapshot()
                .revision,
            revision
        );
        assert!(
            fss[&3].exists(&checkpoint_path(DIR)),
            "the installed image must be published as the durable checkpoint"
        );
        assert!(
            !fss[&3].exists(&snapshot_partial_path(DIR)),
            "a finished install must not leave a partial image"
        );
        assert!(
            nodes[&3].legacy().lock().unwrap().wal_bytes().unwrap() < 4096,
            "the log must be rewritten to the new base, not replayed"
        );

        // Catch-up after the image is the ordinary log path: no second image.
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(u64::MAX, u64::MAX);
        for i in 6..9u32 {
            propose_on(&nodes, 1, i, &none);
        }
        drive_until(&nodes, 8, |nodes| converged(nodes, &key(8)));
        assert_eq!(
            nodes[&3].legacy().lock().unwrap().base_index(),
            base,
            "later entries must be appended, not installed again"
        );
        assert!(nodes[&1].legacy().lock().unwrap().snapshot_sends.is_empty());
        for i in 6..9u32 {
            assert_eq!(nodes[&3].get(&key(i)).unwrap().value, value(i));
        }
    }

    #[test]
    fn a_peer_below_the_compacted_base_is_sent_the_durable_image() {
        let fss = cluster_fs(&[1, 2, 3]);
        let nodes = open_cluster(&fss);
        elect(&nodes, 1);
        let none = BTreeSet::new();
        for i in 0..5u32 {
            propose_on(&nodes, 1, i, &none);
        }
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(1, 1);
        propose_on(&nodes, 1, 5, &none);

        let base = nodes[&1].legacy().lock().unwrap().base_index();
        assert!(base > 0);

        // An ordinary round teaches the follower the floor, so it compacts to
        // the same base the leader did.
        nodes[&3]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(1, 1);
        let msgs = nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .broadcast_append_entries_checked()
            .unwrap();
        nodes[&1]
            .drive_outbound_with(msgs, &mut |addr, rpc| deliver(&nodes, addr, rpc))
            .unwrap();

        let chunk = snapshot_chunk(&nodes, 1, 3);
        match chunk.rpc {
            RaftRpc::InstallSnapshot {
                offset,
                last_index,
                last_term,
                group_id: image_group,
                ref membership,
                total_len,
                ref data,
                ..
            } => {
                assert_eq!(offset, 0);
                assert_eq!(last_index, base);
                assert_eq!(last_term, nodes[&1].legacy().lock().unwrap().base_term);
                assert_eq!(image_group, group_id(&[1, 2, 3]));
                assert_eq!(membership, &vec![1, 2, 3]);
                assert!(total_len > 0 && total_len as usize == data.len());
            }
            other => panic!("expected an image chunk, got {other:?}"),
        }

        // The follower side of the same boundary still refuses an append whose
        // prefix lies below its own base: it can never verify it.
        let (term, follower_base, floor) = {
            let leader = nodes[&1].legacy().lock().unwrap();
            let follower = nodes[&3].legacy().lock().unwrap();
            (
                leader.current_term(),
                follower.base_index(),
                leader.compaction_floor(),
            )
        };
        assert!(follower_base > 0, "the follower must have compacted too");
        let replies = nodes[&3]
            .legacy()
            .lock()
            .unwrap()
            .step(RaftRpc::AppendEntries {
                term,
                leader_id: 1,
                prev_log_index: follower_base - 1,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: follower_base,
                compaction_floor: floor,
            })
            .unwrap();
        assert!(
            matches!(
                replies[0].rpc,
                RaftRpc::AppendEntriesReply { success: false, .. }
            ),
            "{replies:?}"
        );
    }

    #[test]
    fn a_snapshot_from_another_group_or_membership_is_refused() {
        let fss = cluster_fs(&[1, 2, 3]);
        let nodes = open_cluster(&fss);
        elect(&nodes, 1);
        let none = BTreeSet::new();
        for i in 0..5u32 {
            propose_on(&nodes, 1, i, &none);
        }
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(1, 1);
        propose_on(&nodes, 1, 5, &none);

        let before = nodes[&2].legacy().lock().unwrap().sm.durable_snapshot();
        let chunk = snapshot_chunk(&nodes, 1, 2);

        let wrong_group = edited_chunk(&chunk, |rpc| {
            if let RaftRpc::InstallSnapshot { group_id, .. } = rpc {
                *group_id ^= 0xffff;
            }
        });
        expect_refusal(nodes[&2].step_rpc(wrong_group).unwrap(), "group");

        // Legacy membership is the whole group identity: a replica configured
        // with different voters is a different group, not a new incarnation.
        let wrong_membership = edited_chunk(&chunk, |rpc| {
            if let RaftRpc::InstallSnapshot { membership, .. } = rpc {
                *membership = vec![1, 2, 4];
            }
        });
        expect_refusal(nodes[&2].step_rpc(wrong_membership).unwrap(), "membership");

        let after = nodes[&2].legacy().lock().unwrap().sm.durable_snapshot();
        assert_eq!(after.revision, before.revision);
        assert_eq!(after.values, before.values);
        assert_eq!(nodes[&2].legacy().lock().unwrap().base_index(), 0);
        assert!(!fss[&2].exists(&checkpoint_path(DIR)));
        assert!(!fss[&2].exists(&snapshot_partial_path(DIR)));
    }

    #[test]
    fn a_corrupted_or_stale_snapshot_is_refused_without_publishing() {
        let mut fss = cluster_fs(&[1, 2, 3]);
        let mut nodes = open_cluster(&fss);
        elect(&nodes, 1);
        let none = BTreeSet::new();
        for i in 0..5u32 {
            propose_on(&nodes, 1, i, &none);
        }
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(1, 1);
        propose_on(&nodes, 1, 5, &none);

        // A replica that must actually take the image, so the chunk reaches
        // the whole-image validation instead of being refused as stale.
        replace_with_empty_replica(&mut fss, &mut nodes, 3, 7403);
        let before = nodes[&3].legacy().lock().unwrap().sm.durable_snapshot();
        let chunk = snapshot_chunk(&nodes, 1, 3);

        let corrupted = edited_chunk(&chunk, |rpc| {
            if let RaftRpc::InstallSnapshot { data, .. } = rpc {
                data[0] ^= 0xff;
            }
        });
        expect_refusal(nodes[&3].step_rpc(corrupted).unwrap(), "checksum");
        let after = nodes[&3].legacy().lock().unwrap().sm.durable_snapshot();
        assert_eq!(after.revision, before.revision);
        assert_eq!(after.values, before.values);
        assert_eq!(nodes[&3].legacy().lock().unwrap().base_index(), 0);
        assert!(!fss[&3].exists(&checkpoint_path(DIR)));
        assert!(!fss[&3].exists(&snapshot_partial_path(DIR)));

        // The refusal must not wedge the receiver: the same image, intact this
        // time, still installs.
        let reply = nodes[&3].step_rpc(chunk.rpc.clone()).unwrap();
        assert!(
            matches!(
                reply,
                Some(RaftRpc::InstallSnapshotReply { accepted: true, .. })
            ),
            "{reply:?}"
        );
        let installed_base = nodes[&3].legacy().lock().unwrap().base_index();
        assert!(installed_base > 0);

        // A stale image (below the installed base) is refused, as is one that
        // reuses the base index with another term.
        let stale = edited_chunk(&chunk, |rpc| {
            if let RaftRpc::InstallSnapshot { last_index, .. } = rpc {
                *last_index = installed_base - 1;
            }
        });
        expect_refusal(nodes[&3].step_rpc(stale).unwrap(), "older than");
        let conflicting = edited_chunk(&chunk, |rpc| {
            if let RaftRpc::InstallSnapshot { last_term, .. } = rpc {
                *last_term += 1;
            }
        });
        expect_refusal(
            nodes[&3].step_rpc(conflicting).unwrap(),
            "conflicts with base term",
        );
        assert_eq!(
            nodes[&3].legacy().lock().unwrap().base_index(),
            installed_base
        );
        // The image covers the state at its index; later entries still come
        // from the log.
        for i in 0..5u32 {
            assert_eq!(nodes[&3].get(&key(i)).unwrap().value, value(i));
        }
    }

    fn next_offset(reply: &RaftRpc) -> u64 {
        match reply {
            RaftRpc::InstallSnapshotReply { next_offset, .. } => *next_offset,
            other => panic!("expected a snapshot reply, got {other:?}"),
        }
    }

    #[test]
    fn a_snapshot_transfer_resumes_without_duplicating_bytes() {
        let mut fss = cluster_fs(&[1, 2, 3]);
        let mut nodes = open_cluster(&fss);
        elect(&nodes, 1);
        let none = BTreeSet::new();
        for i in 0..5u32 {
            propose_on(&nodes, 1, i, &none);
        }
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(1, 1);
        propose_on(&nodes, 1, 5, &none);

        replace_with_empty_replica(&mut fss, &mut nodes, 3, 7503);
        nodes[&1].legacy().lock().unwrap().snapshot_chunk_bytes = 32;

        let first = snapshot_chunk(&nodes, 1, 3).rpc;
        let reply = nodes[&3].step_rpc(first.clone()).unwrap().unwrap();
        assert_eq!(next_offset(&reply), 32, "one bounded chunk");

        // A retransmitted chunk must not append the same bytes twice.
        let again = nodes[&3].step_rpc(first).unwrap().unwrap();
        assert_eq!(next_offset(&again), 32);

        // The leader asks for the next chunk, which then arrives too far ahead.
        let second = nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .step(reply)
            .unwrap()
            .pop()
            .unwrap()
            .rpc;
        let gapped = edited_chunk(
            &OutboundMsg {
                to: 3,
                rpc: second.clone(),
            },
            |rpc| {
                if let RaftRpc::InstallSnapshot { offset, .. } = rpc {
                    *offset += 32;
                }
            },
        );
        let refusal = nodes[&3].step_rpc(gapped).unwrap().unwrap();
        assert!(matches!(
            refusal,
            RaftRpc::InstallSnapshotReply {
                accepted: false,
                ..
            }
        ));
        assert_eq!(next_offset(&refusal), 32, "the receiver asks for its gap");

        // The correct chunk resumes the transfer and the rest completes from
        // the leader's recorded offset.
        let accepted = nodes[&3].step_rpc(second).unwrap().unwrap();
        assert!(matches!(
            accepted,
            RaftRpc::InstallSnapshotReply { accepted: true, .. }
        ));
        drive_until(&nodes, 64, |nodes| converged(nodes, &key(5)));
        assert_eq!(
            nodes[&3].legacy().lock().unwrap().base_index(),
            nodes[&1].legacy().lock().unwrap().base_index()
        );
        for i in 0..=5u32 {
            assert_eq!(nodes[&3].get(&key(i)).unwrap().value, value(i));
        }
    }

    #[test]
    fn an_install_interrupted_by_a_power_cut_leaves_the_old_state() {
        let mut fss = cluster_fs(&[1, 2, 3]);
        let mut nodes = open_cluster(&fss);
        elect(&nodes, 1);
        let none = BTreeSet::new();
        for i in 0..5u32 {
            propose_on(&nodes, 1, i, &none);
        }
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(1, 1);
        propose_on(&nodes, 1, 5, &none);

        replace_with_empty_replica(&mut fss, &mut nodes, 3, 7203);
        // Small chunks so the transfer can be stopped in the middle.
        nodes[&1].legacy().lock().unwrap().snapshot_chunk_bytes = 32;

        let mut rpc = snapshot_chunk(&nodes, 1, 3).rpc;
        for _ in 0..2 {
            let reply = nodes[&3].step_rpc(rpc).unwrap().unwrap();
            rpc = nodes[&1]
                .legacy()
                .lock()
                .unwrap()
                .step(reply)
                .unwrap()
                .pop()
                .unwrap()
                .rpc;
        }
        assert!(fss[&3].exists(&snapshot_partial_path(DIR)));
        assert_eq!(nodes[&3].legacy().lock().unwrap().base_index(), 0);
        assert!(nodes[&3].get(&key(0)).is_none());

        fss[&3].simulate_power_cut();
        nodes.remove(&3);
        nodes.insert(
            3,
            LocalMetaKv::open_clustered_with_token(
                &fss[&3],
                DIR,
                3,
                fss.keys().map(|&i| (i, i.to_string())).collect(),
                "test-token".to_string(),
            )
            .unwrap(),
        );

        assert_eq!(
            nodes[&3].legacy().lock().unwrap().base_index(),
            0,
            "a half-received image must never become the base"
        );
        assert!(nodes[&3].get(&key(0)).is_none());
        assert!(
            !fss[&3].exists(&snapshot_partial_path(DIR)),
            "opening must discard a partial image"
        );
        assert!(!fss[&3].exists(&checkpoint_path(DIR)));
    }

    #[test]
    fn a_crash_after_publishing_the_image_keeps_the_new_base() {
        let mut fss = cluster_fs(&[1, 2, 3]);
        let mut nodes = open_cluster(&fss);
        elect(&nodes, 1);
        let none = BTreeSet::new();
        for i in 0..5u32 {
            propose_on(&nodes, 1, i, &none);
        }
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .set_compaction_thresholds(1, 1);
        propose_on(&nodes, 1, 5, &none);
        let (base, base_term) = {
            let g = nodes[&1].legacy().lock().unwrap();
            (g.base_index(), g.base_term)
        };

        replace_with_empty_replica(&mut fss, &mut nodes, 3, 7303);
        // The rename of the image reaches the disk, the following sync fails:
        // this is exactly the window between publishing the checkpoint and
        // rewriting the log.
        fss[&3].set_faults(FaultConfig {
            sync_dir_fault: SyncFault::AfterPersist,
            ..Default::default()
        });
        let reply = nodes[&3]
            .step_rpc(snapshot_chunk(&nodes, 1, 3).rpc)
            .unwrap();
        expect_refusal(reply, "cannot sync the published snapshot");
        assert_eq!(
            nodes[&3].legacy().lock().unwrap().base_index(),
            0,
            "the in-memory base must not advance before the log is rewritten"
        );
        let published = load_checkpoint(&fss[&3], DIR).unwrap().unwrap();
        assert_eq!(published.applied_index, base);
        assert_eq!(published.applied_term, base_term);

        fss[&3].simulate_power_cut();
        fss[&3].set_faults(FaultConfig::default());
        nodes.remove(&3);
        nodes.insert(
            3,
            LocalMetaKv::open_clustered_with_token(
                &fss[&3],
                DIR,
                3,
                fss.keys().map(|&i| (i, i.to_string())).collect(),
                "test-token".to_string(),
            )
            .unwrap(),
        );

        assert_eq!(
            nodes[&3].legacy().lock().unwrap().base_index(),
            base,
            "a published image is adopted and the stale log reconciled to it"
        );
        for i in 0..5u32 {
            assert_eq!(nodes[&3].get(&key(i)).unwrap().value, value(i));
        }
        // The entry above the image is still only in the leader's log: the
        // retried install is idempotent and the normal append carries it.
        assert!(nodes[&3].get(&key(5)).is_none());
        drive_until(&nodes, 16, |nodes| nodes[&3].get(&key(5)).is_some());
        assert_eq!(nodes[&3].get(&key(5)).unwrap().value, value(5));
        assert_eq!(nodes[&3].legacy().lock().unwrap().base_index(), base);
        assert!(
            nodes[&3].legacy().lock().unwrap().wal_bytes().unwrap() < 4096,
            "opening must rewrite the log to the published base"
        );
        assert!(!fss[&3].exists(&snapshot_partial_path(DIR)));
    }
}
