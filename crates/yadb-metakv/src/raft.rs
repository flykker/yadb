use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use yadb_io::StorageFs;

use crate::kv::{KvCommand, KvResult, MetaStateMachine, WatchEvent};
use crate::wal::{RaftWal, WalRecord};
use crate::{KvValue, MetaError, MetaSnapshot};

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
        self.wal.ensure_healthy()?;
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
        self.wal.ensure_healthy()?;
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
        if self.wal.ensure_healthy().is_err() || self.role != RoleState::Leader {
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
        self.wal.ensure_healthy()?;
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
                    self.commit_index = self.commit_index.max(leader_commit.min(matched_index));
                    self.apply_committed();
                }

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
            guard.broadcast_append_entries()
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
