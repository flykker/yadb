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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableCheckpoint {
    format_version: u32,
    applied_index: u64,
    applied_term: u64,
    commit_index: u64,
    hard_state: DurableHardState,
    compaction_floor: u64,
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
    if checkpoint.format_version != 1 {
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
        match_index: u64,
    },
}

#[derive(Debug, Clone)]
pub struct OutboundMsg {
    pub to: u32,
    pub rpc: RaftRpc,
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
        let (
            sm,
            applied_index,
            checkpoint_floor,
            checkpoint_snapshot,
            checkpoint_term,
            checkpoint_vote,
        ) = if let Some(cp) = checkpoint {
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

    pub fn broadcast_append_entries(&self) -> Vec<OutboundMsg> {
        self.broadcast_append_entries_checked().unwrap_or_default()
    }

    fn broadcast_append_entries_checked(&self) -> Result<Vec<OutboundMsg>, MetaError> {
        if self.ensure_operable().is_err() || self.role != RoleState::Leader {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for &peer in &self.peers {
            if peer == self.node_id {
                continue;
            }
            let next_idx = self
                .next_index
                .get(&peer)
                .copied()
                .unwrap_or(self.last_log_index() + 1)
                .max(1);
            let prev_log_index = next_idx - 1;
            let prev_log_term = self.term_at(prev_log_index).unwrap_or(0);
            if next_idx <= self.base_index {
                return Err(MetaError::PeerBehindCompactedBase(peer));
            }
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
                            match_index: 0,
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
                            match_index: 0,
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
                    if backed_off <= self.base_index {
                        return Err(MetaError::PeerBehindCompactedBase(from));
                    }
                    self.next_index.insert(from, backed_off);
                    return self.broadcast_append_entries_checked();
                }
                Ok(Vec::new())
            }
        }
    }

    fn step_down(&mut self, new_term: u64) -> Result<(), MetaError> {
        self.current_term = new_term;
        self.role = RoleState::Follower;
        self.voted_for = None;
        self.leader_id = None;
        self.votes_received.clear();
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
            format_version: 1,
            applied_index: target,
            applied_term,
            commit_index: self.commit_index,
            hard_state: DurableHardState {
                term: self.current_term,
                voted_for: self.voted_for,
            },
            compaction_floor: self.compaction_floor,
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
            let guard = self.legacy().lock().unwrap();
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
        mut pending: Vec<OutboundMsg>,
        send: &mut impl FnMut(&str, &RaftRpc) -> Result<Option<RaftRpc>, std::io::Error>,
    ) -> Result<(), MetaError> {
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
            }
        }
        Ok(())
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
            self.drive_outbound_with(msgs, send)?;
        }
        let mut guard = self.legacy().lock().unwrap();
        // A new leader may have replaced the uncommitted proposal while the
        // mutex was released for RPCs. An index alone does not identify an
        // entry across terms; never return (or consume) the replacement's result.
        if guard.term_at(idx) != Some(term) {
            return Err(MetaError::UncommittedIndex(idx));
        }
        guard
            .take_applied_result(idx)
            .ok_or(MetaError::UncommittedIndex(idx))
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
    use yadb_io::SimFs;

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
            snapshot: core.snapshot_at(index),
        }
    }

    fn write_checkpoint(fs: &SimFs, checkpoint: &DurableCheckpoint) {
        fs.create_dir_all(DIR).unwrap();
        let file = fs.open_rw(&checkpoint_path(DIR), true).unwrap();
        let bytes = serde_json::to_vec(checkpoint).unwrap();
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

    #[test]
    fn a_peer_below_the_compacted_base_is_refused_explicitly() {
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

        let (base, term) = {
            let g = nodes[&1].legacy().lock().unwrap();
            (g.base_index(), g.current_term())
        };
        assert!(base > 0);

        // A peer that lost its log needs entries from below the base.
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .next_index
            .insert(3, base);
        let refused = nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .broadcast_append_entries_checked();
        assert!(
            matches!(refused, Err(MetaError::PeerBehindCompactedBase(3))),
            "{refused:?}"
        );

        // Backing off to the base is refused too, never turned into a silent skip.
        nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .next_index
            .insert(3, base + 1);
        let refused = nodes[&1]
            .legacy()
            .lock()
            .unwrap()
            .step(RaftRpc::AppendEntriesReply {
                from: 3,
                term,
                success: false,
                match_index: 0,
            });
        assert!(
            matches!(refused, Err(MetaError::PeerBehindCompactedBase(3))),
            "{refused:?}"
        );

        // The follower side of the same boundary: with its own base at `base`,
        // an append whose prefix lies below it cannot be verified, so it is
        // rejected instead of being treated as a match.
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
        let floor = nodes[&1].legacy().lock().unwrap().compaction_floor();
        let follower_base = nodes[&3].legacy().lock().unwrap().base_index();
        assert_eq!(
            follower_base, floor,
            "the follower must compact to the floor the leader published"
        );

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
}
