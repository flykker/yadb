use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use yadb_io::StorageFs;

use crate::kv::{KvCommand, KvResult, MetaStateMachine, WatchEvent};
use crate::wal::{RaftWal, WalRecord};
use crate::{KvValue, MetaError};

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
    node_id: u32,
    peers: Vec<u32>,
    role: RoleState,
    current_term: u64,
    voted_for: Option<u32>,
    leader_id: Option<u32>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    votes_received: BTreeSet<u32>,
    next_index: BTreeMap<u32, u64>,
    match_index: BTreeMap<u32, u64>,
    applied_results: BTreeMap<u64, KvResult>,
    wal: RaftWal<FS>,
    sm: MetaStateMachine,
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

        let (wal, recovered) = RaftWal::open(fs, dir, sync_writes)?;
        let mut core = Self {
            node_id,
            peers,
            role: RoleState::Follower,
            current_term: recovered.term,
            voted_for: recovered.voted_for,
            leader_id: None,
            log: recovered.log,
            commit_index: 0,
            last_applied: 0,
            votes_received: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            applied_results: BTreeMap::new(),
            wal,
            sm: MetaStateMachine::new(),
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
            core.commit_index = last_idx;
            core.apply_committed();
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
        self.log.len() as u64
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log.get((index - 1) as usize).map(|e| e.term)
    }

    /// Triggers an election timeout on this node: transitions to `Candidate` (or directly `Leader` if N=1).
    pub fn start_election(&mut self) -> Result<Vec<OutboundMsg>, MetaError> {
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
            return Ok(self.become_leader());
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

    fn become_leader(&mut self) -> Vec<OutboundMsg> {
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
        self.broadcast_append_entries()
    }

    /// Proposes a KV mutation on the Leader, appends to local WAL, and returns replication RPCs
    /// (for N=1 single-node mode, commits and applies immediately).
    pub fn propose(&mut self, cmd: KvCommand) -> Result<(u64, Vec<OutboundMsg>), MetaError> {
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
            self.apply_committed();
            return Ok((idx, Vec::new()));
        }

        Ok((idx, self.broadcast_append_entries()))
    }

    pub fn broadcast_append_entries(&self) -> Vec<OutboundMsg> {
        if self.role != RoleState::Leader {
            return Vec::new();
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
            let entries = self.log[(next_idx as usize - 1)..].to_vec();
            out.push(OutboundMsg {
                to: peer,
                rpc: RaftRpc::AppendEntries {
                    term: self.current_term,
                    leader_id: self.node_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                },
            });
        }
        out
    }

    pub fn step(&mut self, rpc: RaftRpc) -> Result<Vec<OutboundMsg>, MetaError> {
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
                        return Ok(self.become_leader());
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

                // Truncate any conflicting uncommitted suffix and append new entries
                let mut write_start: Option<u64> = None;
                let mut to_persist: Vec<LogEntry> = Vec::new();
                for entry in entries {
                    let pos = (entry.index - 1) as usize;
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
                    self.commit_index = leader_commit.min(self.last_log_index());
                    self.apply_committed();
                }

                Ok(vec![OutboundMsg {
                    to: leader_id,
                    rpc: RaftRpc::AppendEntriesReply {
                        from: self.node_id,
                        term: self.current_term,
                        success: true,
                        match_index: self.last_log_index(),
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

                    // Raft §5.4.2: advance `commit_index` only for entries from `current_term`
                    let old_commit = self.commit_index;
                    for n in ((self.commit_index + 1)..=self.last_log_index()).rev() {
                        if self.term_at(n) == Some(self.current_term) {
                            let replicated = self
                                .peers
                                .iter()
                                .filter(|&&p| {
                                    self.match_index.get(&p).copied().unwrap_or(0) >= n
                                })
                                .count();
                            if replicated >= self.quorum_size() {
                                self.commit_index = n;
                                self.apply_committed();
                                break;
                            }
                        }
                    }
                    // Broadcast updated `leader_commit` so followers apply immediately
                    if self.commit_index > old_commit {
                        return Ok(self.broadcast_append_entries());
                    }
                } else {
                    let cur_next = self.next_index.get(&from).copied().unwrap_or(2);
                    let backed_off = cur_next.saturating_sub(1).max(1);
                    self.next_index.insert(from, backed_off);
                    return Ok(self.broadcast_append_entries());
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

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = self.log[(self.last_applied - 1) as usize].clone();
            let res = self.sm.apply(entry.cmd);
            self.applied_results.insert(self.last_applied, res);
        }
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
    inner: Arc<Mutex<RaftCore<FS>>>,
    peer_addrs: Arc<BTreeMap<u32, String>>,
}

impl<FS: StorageFs> LocalMetaKv<FS> {
    pub fn open_single_node(fs: &FS, dir: &str, node_id: u32) -> Result<Self, MetaError> {
        let core = RaftCore::open(fs, dir, node_id, vec![node_id], true)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(core)),
            peer_addrs: Arc::new(BTreeMap::new()),
        })
    }

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
            inner: Arc::new(Mutex::new(core)),
            peer_addrs: Arc::new(peer_addrs),
        })
    }

    pub fn role_info(&self) -> (u32, RoleState, u64, Option<u32>, u64) {
        let g = self.inner.lock().unwrap();
        (
            g.node_id(),
            g.role(),
            g.current_term(),
            g.leader_id(),
            g.commit_index(),
        )
    }

    pub fn peer_addr(&self, node_id: u32) -> Option<String> {
        self.peer_addrs.get(&node_id).cloned()
    }

    /// Handles an inbound Raft RPC (`RequestVote` or `AppendEntries`) and returns any immediate reply RPC.
    pub fn step_rpc(&self, rpc: RaftRpc) -> Result<Option<RaftRpc>, MetaError> {
        let mut guard = self.inner.lock().unwrap();
        let mut out = guard.step(rpc)?;
        Ok(out.pop().map(|m| m.rpc))
    }

    /// Triggers a Raft election round across configured peers and returns the resulting role.
    pub fn trigger_election(&self) -> Result<RoleState, MetaError> {
        let initial_msgs = {
            let mut guard = self.inner.lock().unwrap();
            guard.start_election()?
        };
        self.drive_outbound(initial_msgs)?;
        Ok(self.inner.lock().unwrap().role())
    }

    /// Broadcasts a heartbeat / catch-up `AppendEntries` round if this node is Leader.
    pub fn broadcast_heartbeat(&self) -> Result<(), MetaError> {
        let msgs = {
            let guard = self.inner.lock().unwrap();
            guard.broadcast_append_entries()
        };
        self.drive_outbound(msgs)
    }

    fn drive_outbound(&self, mut pending: Vec<OutboundMsg>) -> Result<(), MetaError> {
        // Bound synchronous cascade rounds (e.g. RequestVote -> become_leader -> AppendEntries -> commit broadcast)
        for _round in 0..8 {
            if pending.is_empty() {
                break;
            }
            let batch = std::mem::take(&mut pending);
            let mut replies = Vec::new();
            for msg in batch {
                if let Some(addr) = self.peer_addrs.get(&msg.to) {
                    if let Ok(Some(reply_rpc)) = send_raft_http_rpc(addr, &msg.rpc) {
                        replies.push(reply_rpc);
                    }
                }
            }
            if replies.is_empty() {
                break;
            }
            let mut guard = self.inner.lock().unwrap();
            for rep in replies {
                let next_msgs = guard.step(rep)?;
                pending.extend(next_msgs);
            }
        }
        Ok(())
    }

    pub fn execute(&self, cmd: KvCommand) -> Result<KvResult, MetaError> {
        let (idx, msgs) = {
            let mut guard = self.inner.lock().unwrap();
            guard.propose(cmd)?
        };
        if !msgs.is_empty() {
            self.drive_outbound(msgs)?;
        }
        let mut guard = self.inner.lock().unwrap();
        guard
            .take_applied_result(idx)
            .ok_or(MetaError::UncommittedIndex(idx))
    }

    pub fn get(&self, key: &str) -> Option<KvValue> {
        self.inner.lock().unwrap().get(key)
    }

    pub fn scan_prefix(&self, prefix: &str) -> Vec<(String, KvValue)> {
        self.inner.lock().unwrap().scan_prefix(prefix)
    }

    pub fn revision(&self) -> u64 {
        self.inner.lock().unwrap().revision()
    }

    pub fn watch_prefix(&self, prefix: impl Into<String>) -> Receiver<WatchEvent> {
        self.inner.lock().unwrap().watch_prefix(prefix)
    }
}

fn send_raft_http_rpc(peer_addr: &str, rpc: &RaftRpc) -> Result<Option<RaftRpc>, std::io::Error> {
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

    let req = format!(
        "POST /raft/rpc HTTP/1.1\r\nHost: {clean}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
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
