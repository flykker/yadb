//! Single-node durable metadata store, plus the message vocabulary the group
//! protocol prototype shares with it.
//!
//! OpenRaft is the only clustered metadata runtime. What used to live here — a
//! hand-written multi-node Raft core with terms, votes, elections, log
//! replication, an outbound HTTP transport and snapshot transfer — is gone. The
//! only thing left that writes metadata by itself is [`LocalKv`]: a
//! non-replicated store for one node, used where no cluster exists at all
//! (unit tests on [`yadb_io::SimFs`], which the OpenRaft storage adapters cannot
//! serve because they require `std::fs`).
//!
//! [`LocalKv`] keeps exactly the durability contract those callers rely on:
//!
//! * every committed command is appended to the durable WAL frame format
//!   ([`crate::wal`]) and only then applied through [`MetaStateMachine`];
//! * opening replays that WAL in order;
//! * a torn final frame is discarded because it was never acknowledged, while
//!   any other damage refuses to open. Nothing is repaired silently.
//!
//! It has no terms, votes, peers, elections, RPC, replication, snapshots or
//! transport, and it is always the leader of its own single node.

use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use yadb_io::StorageFs;

use crate::kv::{KvCommand, KvResult, MetaStateMachine, WatchEvent};
use crate::openraft_client::WriteAck;
use crate::wal::{RaftWal, WalRecord};
use crate::{KvValue, MetaError, MetaSnapshot};

/// Locally observed role of a metadata replica.
///
/// A single-node [`LocalKv`] is always [`RoleState::Leader`]. An OpenRaft
/// replica reports the state of its own group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoleState {
    Follower,
    Candidate,
    Leader,
}

/// One ordered entry of the metadata log, as it is framed in the WAL.
///
/// `term` is part of the on-disk frame format and is written as `0` by
/// [`LocalKv`]: a non-replicated store has no terms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub cmd: KvCommand,
}

/// The message vocabulary of the group-protocol prototype in
/// [`crate::multiraft`].
///
/// The daemon no longer serves a Raft RPC endpoint and nothing in this crate
/// sends or steps these messages; the prototype keeps the types because its
/// envelope validation is typed over them. They are *not* a runtime.
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
        next_offset: u64,
        error: Option<String>,
    },
}

/// Single-node, non-replicated durable key/value store.
///
/// Writes are durable before they are applied and before they are acknowledged.
/// There is no quorum to wait for and no peer to replicate to, so the node is
/// its own leader and commits every entry it accepts.
pub struct LocalKv<FS: StorageFs> {
    node_id: u32,
    /// Index the next accepted entry will take. Entries are numbered from 1.
    next_index: u64,
    /// Set when a peer message proved another node believes this store is one
    /// replica of a cluster. A single node can never satisfy that, so writes
    /// fail closed until the store is reopened.
    fenced: bool,
    wal: RaftWal<FS>,
    sm: MetaStateMachine,
}

impl<FS: StorageFs> LocalKv<FS> {
    /// Opens (or creates) the store in `dir` and replays its WAL.
    ///
    /// A WAL with a compacted base is refused: that base was written by the
    /// removed Raft runtime together with a `checkpoint.json` image, and this
    /// store has no way to restore the state below it. Refusing is the only
    /// alternative to silently starting from an empty state under a non-zero
    /// base.
    pub fn open(fs: &FS, dir: &str, node_id: u32) -> Result<Self, MetaError> {
        let (wal, recovered) = RaftWal::open(fs, dir, true)?;
        if recovered.base_index != 0 {
            return Err(MetaError::Checkpoint(
                "metadata WAL has a compacted base and this runtime keeps no state image to \
                 restore it from"
                    .into(),
            ));
        }
        let mut sm = MetaStateMachine::new();
        for entry in &recovered.log {
            sm.apply(entry.cmd.clone());
        }
        Ok(Self {
            node_id,
            next_index: recovered.base_index + recovered.log.len() as u64 + 1,
            fenced: false,
            wal,
            sm,
        })
    }

    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    /// Durably appends `cmd`, then applies it. A failed append is not applied
    /// and is not acknowledged; the WAL refuses every later write until the
    /// store is reopened.
    pub fn propose(&mut self, cmd: KvCommand) -> Result<KvResult, MetaError> {
        self.wal.ensure_healthy()?;
        if self.fenced {
            return Err(MetaError::NotLeader { leader: None });
        }
        let index = self.next_index;
        let entry = LogEntry {
            index,
            term: 0,
            cmd: cmd.clone(),
        };
        self.wal.append(&WalRecord::AppendEntries {
            from_index: index,
            entries: vec![entry],
        })?;
        self.next_index = index + 1;
        Ok(self.sm.apply(cmd))
    }

    /// `(node_id, role, term, leader_id, commit_index)`.
    ///
    /// A single node has no term, so `term` is `0`; its own last applied entry
    /// is its commit index. A fenced store reports itself as a follower with no
    /// leader because it refuses every write.
    pub fn role_info(&self) -> (u32, RoleState, u64, Option<u32>, u64) {
        if self.fenced {
            (
                self.node_id,
                RoleState::Follower,
                0,
                None,
                self.commit_index(),
            )
        } else {
            (
                self.node_id,
                RoleState::Leader,
                0,
                Some(self.node_id),
                self.commit_index(),
            )
        }
    }

    /// Index of the last applied entry; `0` before the first write.
    pub fn commit_index(&self) -> u64 {
        self.next_index - 1
    }

    /// Refuses every later write until the store is reopened. Nothing is
    /// voted, appended or sent: this is a local fail-closed flag.
    fn fence(&mut self) {
        self.fenced = true;
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

/// Handle to whichever metadata runtime this process uses.
///
/// Either a single-node [`LocalKv`] or an OpenRaft [`crate::openraft_client::GroupRouter`].
/// Reads are local-applied; they are not a linearizable guarantee.
#[derive(Clone)]
pub struct LocalMetaKv<FS: StorageFs> {
    inner: Option<Arc<Mutex<LocalKv<FS>>>>,
    openraft: Option<crate::openraft_client::GroupRouter>,
    peer_addrs: Arc<BTreeMap<u32, String>>,
}

impl<FS: StorageFs> LocalMetaKv<FS> {
    fn legacy(&self) -> &Mutex<LocalKv<FS>> {
        self.inner
            .as_deref()
            .expect("single-node store unavailable for OpenRaft backend")
    }

    /// Whether this handle routes metadata writes through OpenRaft.
    pub fn is_openraft(&self) -> bool {
        self.openraft.is_some()
    }

    /// Wraps an existing OpenRaft and its shared durable state. `peer_addrs`
    /// contains public HTTP addresses, not the Raft transport addresses.
    /// Reads are local-applied and may be stale, not linearizable. Writes on
    /// current-thread Tokio runtimes (including their blocking threads) are
    /// rejected before submission. Multi-thread runtimes use block_in_place.
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
        }
    }
    /// Explicit immutable project routing; groups must contain a separate
    /// control group and at least two distinct mapped data groups.
    /// The owner must persist and verify the mapping and bind each supplied
    /// Raft handle to its matching state machine and group identity. Read
    /// methods named `*_or_panic` panic on invalid scope or unavailable state
    /// and exist only for tests; production callers use `try_get`,
    /// `try_scan_prefix`, `try_revision` and `try_snapshot`, which return the
    /// error instead of a default. Namespace-wide scans merge local views
    /// without an atomic cross-group revision.
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
        })
    }

    /// One scope per data group for a per-group collection pass.
    ///
    /// A single-group deployment has no explicit project routing, so its one
    /// scope covers every project. Single-node metadata has no groups;
    /// collection there is a single-namespace pass and must not call this.
    pub fn gc_group_scopes(&self) -> Result<Vec<crate::gc::GcGroupScope>, MetaError> {
        if let Some(router) = &self.openraft {
            return Ok(router.gc_scopes());
        }
        Err(MetaError::Routing(
            "per-group garbage collection requires OpenRaft".into(),
        ))
    }

    /// The data group that owns `project_id`, or `None` on single-node metadata.
    ///
    /// This is what routes a writer lease into the group whose manifests it
    /// fences, and what lets a per-group pass decide which fragments it owns.
    pub fn gc_group_for_project(&self, project_id: u32) -> Result<Option<u64>, MetaError> {
        if let Some(router) = &self.openraft {
            return router.group_for_project_id(project_id).map(Some);
        }
        let _ = project_id;
        Ok(None)
    }

    /// Locally observed role of one named metadata group. Like
    /// [`Self::role_info_for_key`] this is not proof that the leader has quorum;
    /// the linearizable snapshot is.
    pub fn role_info_for_group(
        &self,
        group: u64,
    ) -> Result<(u32, RoleState, u64, Option<u32>, u64), MetaError> {
        if let Some(router) = &self.openraft {
            return router.role_for_group(group);
        }
        let _ = group;
        Err(MetaError::Routing(
            "per-group role information requires OpenRaft".into(),
        ))
    }

    /// Locally observed state of every configured metadata group.
    ///
    /// Read-only and in-process: each row comes from this node's own Raft
    /// handles, so no peer is contacted and no metadata key is read. Legacy
    /// single-node metadata configures no groups and returns an empty list
    /// rather than a fabricated row.
    pub async fn group_observations(
        &self,
    ) -> Result<Vec<crate::openraft_client::GroupObservation>, MetaError> {
        // Clone the router out of the handle so the returned future borrows
        // nothing from `self`; the daemon awaits it from a request handler.
        let Some(router) = self.openraft.clone() else {
            return Ok(Vec::new());
        };
        Ok(router.observations().await)
    }

    /// Quorum-confirmed snapshot of exactly one metadata group.
    ///
    /// The group is named directly instead of being routed from a key, because
    /// a collection pass marks every manifest prefix of the group and no single
    /// project key names that scope. Followers and unavailable quorums return
    /// an error; there is no fallback to local applied state.
    pub fn try_linearizable_group_snapshot(
        &self,
        group: u64,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        if let Some(router) = &self.openraft {
            return router.linearizable_group_snapshot(group, keys, prefixes);
        }
        let _ = (group, keys, prefixes);
        Err(MetaError::Routing(
            "per-group linearizable snapshot requires OpenRaft".into(),
        ))
    }

    pub fn revision_for_key(&self, key: &str) -> Result<u64, MetaError> {
        if let Some(router) = &self.openraft {
            return router.revision_for_key(key);
        }
        let _ = key;
        self.try_revision()
    }

    /// Return the locally observed role of the group owning this key; it is
    /// not proof that the reported leader currently has quorum.
    pub fn role_info_for_key(
        &self,
        key: &str,
    ) -> Result<(u32, RoleState, u64, Option<u32>, u64), MetaError> {
        if let Some(router) = &self.openraft {
            return router.role_for_key(key);
        }
        let _ = key;
        Ok(self.role_info())
    }

    /// Opens a single-node, non-replicated store. Used where no cluster exists
    /// at all: unit tests on [`yadb_io::SimFs`], which the OpenRaft storage
    /// adapters cannot serve because they require `std::fs`.
    ///
    /// **Not a deployment mode.** A write through this handle is acknowledged
    /// after it reaches one local WAL; there is no replica, no quorum and no
    /// failover, so losing this node loses the data. The daemon never opens it —
    /// `--metadata-runtime` accepts only `openraft`. It also never compacts, so
    /// its WAL grows for as long as the store is open; that is acceptable for a
    /// test fixture and unacceptable for anything long-lived.
    pub fn open_single_node(fs: &FS, dir: &str, node_id: u32) -> Result<Self, MetaError> {
        let core = LocalKv::open(fs, dir, node_id)?;
        Ok(Self {
            inner: Some(Arc::new(Mutex::new(core))),
            openraft: None,
            peer_addrs: Arc::new(BTreeMap::new()),
        })
    }

    /// The last value is the commit index for a single-node store and the
    /// locally applied log index for OpenRaft; it is not a quorum-read
    /// guarantee.
    pub fn role_info(&self) -> (u32, RoleState, u64, Option<u32>, u64) {
        if let Some(client) = &self.openraft {
            return client.role_info();
        }
        self.legacy().lock().unwrap().role_info()
    }

    pub fn node_id(&self) -> u32 {
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

    /// A single-node store cannot answer a Raft message, and OpenRaft has its
    /// own transport, so the daemon no longer serves a Raft RPC endpoint.
    ///
    /// A message can therefore only come from a peer that believes this store
    /// is one replica of a cluster it can never be part of. The store fences
    /// itself — every later write fails with `NotLeader` — and answers nothing.
    /// Nothing is voted, appended or sent.
    pub fn step_rpc(&self, _rpc: RaftRpc) -> Result<Option<RaftRpc>, MetaError> {
        if self.is_openraft() {
            return Err(MetaError::StorageUnavailable);
        }
        self.legacy().lock().unwrap().fence();
        Ok(None)
    }

    pub fn execute(&self, cmd: KvCommand) -> Result<KvResult, MetaError> {
        if let Some(client) = &self.openraft {
            return client.execute(cmd);
        }
        self.legacy().lock().unwrap().propose(cmd)
    }

    /// The sibling of [`Self::execute`] that also reports the committed log
    /// entry the write occupies.
    ///
    /// This is what makes "the write is durable in the log at index N" an
    /// observation rather than an inference from a later status sample. A
    /// legacy (non-OpenRaft) store has no log, so it reports
    /// `StorageUnavailable` instead of inventing an index.
    pub fn execute_with_ack(&self, cmd: KvCommand) -> Result<WriteAck, MetaError> {
        if let Some(client) = &self.openraft {
            return client.execute_with_ack(cmd);
        }
        Err(MetaError::StorageUnavailable)
    }

    /// Read one key. `Ok(None)` means the key is absent; `Err` means it could
    /// not be read and says nothing about presence.
    ///
    /// This is the production read path. A group that cannot serve its local
    /// applied snapshot — a reopened group before it has applied its log, or a
    /// state machine that requires recovery — returns the error so a request
    /// handler can answer "not ready" instead of aborting the task.
    pub fn try_get(&self, key: &str) -> Result<Option<KvValue>, MetaError> {
        if let Some(client) = &self.openraft {
            return Ok(client.try_snapshot(&[key.to_owned()], &[])?.get(key));
        }
        Ok(self.legacy().lock().unwrap().get(key))
    }

    /// Panicking read for tests and fixtures. Production callers must use
    /// [`Self::try_get`]: this cannot distinguish "absent" from "unavailable"
    /// and aborts the calling task when the snapshot cannot be served.
    pub fn get_or_panic(&self, key: &str) -> Option<KvValue> {
        self.try_get(key)
            .expect("invalid or unavailable metadata read")
    }

    /// Copies selected keys/ranges while holding the same lock as apply.
    /// Release it before reading blobs/network I/O; later writes cannot change
    /// the owned values. Unselected keys are not available in this snapshot.
    ///
    /// Panicking form, for tests only. Use [`Self::try_snapshot`] in production.
    pub fn snapshot_or_panic(&self, keys: &[String], prefixes: &[String]) -> MetaSnapshot {
        self.try_snapshot(keys, prefixes)
            .expect("invalid or unavailable metadata snapshot")
    }

    pub fn try_snapshot(
        &self,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
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
    /// Followers, unavailable quorums, cross-group requests and single-node
    /// metadata return errors; this does not provide cross-group atomicity.
    pub fn try_linearizable_snapshot(
        &self,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        if let Some(client) = &self.openraft {
            return client.linearizable_snapshot(keys, prefixes);
        }
        Err(MetaError::Routing(
            "linearizable snapshot requires OpenRaft".into(),
        ))
    }

    /// Quorum-confirmed local snapshot of the control group. This only works on
    /// the current control-group leader; other replicas must ask that leader.
    /// Single-node metadata has no separate control group and is rejected here.
    pub fn try_linearizable_control_snapshot(
        &self,
        keys: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
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

    /// Scan a prefix in the local applied state. `Ok` with an empty vector means
    /// no key matched; `Err` means the range could not be read.
    pub fn try_scan_prefix(&self, prefix: &str) -> Result<Vec<(String, KvValue)>, MetaError> {
        if let Some(client) = &self.openraft {
            return client.try_scan(prefix);
        }
        Ok(self.legacy().lock().unwrap().scan_prefix(prefix))
    }

    /// Panicking scan, for tests only. Use [`Self::try_scan_prefix`] in
    /// production: an unreadable range is not an empty one.
    pub fn scan_prefix_or_panic(&self, prefix: &str) -> Vec<(String, KvValue)> {
        self.try_scan_prefix(prefix)
            .expect("invalid or unavailable metadata scan")
    }

    /// Locally applied revision, or an error when it cannot be read.
    pub fn try_revision(&self) -> Result<u64, MetaError> {
        if let Some(client) = &self.openraft {
            return client.try_revision();
        }
        Ok(self.legacy().lock().unwrap().revision())
    }

    /// Panicking revision read, for tests only. Use [`Self::try_revision`] in
    /// production.
    pub fn revision_or_panic(&self) -> u64 {
        self.try_revision()
            .expect("invalid or unavailable metadata revision")
    }

    /// Revision to stamp a single-node GC lease with, or `0` on OpenRaft.
    ///
    /// Multi-Raft metadata has no global revision (`try_revision` refuses), and
    /// a group-scoped lease does not use the value for anything: the collector
    /// refuses on any live lease instead of reasoning about revisions. Callers
    /// that pass a revision to `gc::acquire_writer_if_legacy` must use this:
    /// evaluating a fallible read eagerly as an argument refuses before the
    /// per-group path can run.
    pub fn legacy_revision(&self) -> u64 {
        if self.is_openraft() {
            return 0;
        }
        self.legacy().lock().unwrap().revision()
    }

    pub fn watch_prefix(&self, prefix: impl Into<String>) -> Receiver<WatchEvent> {
        if let Some(client) = &self.openraft {
            return client.watch_prefix(prefix.into());
        }
        self.legacy().lock().unwrap().watch_prefix(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yadb_io::SimFs;

    const DIR: &str = "meta";

    fn put(i: u32) -> KvCommand {
        KvCommand::Put {
            key: format!("/k/{i:03}"),
            value: vec![i as u8],
        }
    }

    fn single(fs: &SimFs) -> LocalKv<SimFs> {
        LocalKv::open(fs, DIR, 1).unwrap()
    }

    #[test]
    fn acknowledged_writes_replay_in_order_after_a_power_cut() {
        let fs = SimFs::new(4301);
        let mut core = single(&fs);
        for i in 0..8u32 {
            let KvResult::PutOk { revision, version } = core.propose(put(i)).unwrap() else {
                panic!("a put must answer with its own revision");
            };
            assert_eq!(version, 1);
            assert_eq!(revision, u64::from(i) + 1);
        }
        assert_eq!(core.commit_index(), 8);
        drop(core);
        fs.simulate_power_cut();

        let core = single(&fs);
        assert_eq!(
            core.commit_index(),
            8,
            "every acknowledged entry must replay"
        );
        assert_eq!(core.revision(), 8);
        for i in 0..8u32 {
            assert_eq!(
                core.get(&format!("/k/{i:03}")).unwrap().value,
                vec![i as u8]
            );
        }
        // Reopening must not append anything to the log.
        let before = core.commit_index();
        drop(core);
        let core = single(&fs);
        assert_eq!(core.commit_index(), before);
    }

    #[test]
    fn a_store_with_a_compacted_base_it_cannot_restore_refuses_to_open() {
        let fs = SimFs::new(4302);
        {
            let (mut wal, _) = RaftWal::open(&fs, DIR, true).unwrap();
            wal.append(&WalRecord::Base { index: 10, term: 1 }).unwrap();
        }
        match LocalKv::open(&fs, DIR, 1) {
            Err(MetaError::Checkpoint(message)) => {
                assert!(message.contains("compacted base"), "{message}");
            }
            Err(other) => panic!("expected an explicit checkpoint error, got {other:?}"),
            Ok(_) => panic!("a base with no state image must not open"),
        }
    }

    #[test]
    fn a_fenced_store_refuses_writes_without_voting_or_logging() {
        let fs = SimFs::new(4303);
        let meta = LocalMetaKv::open_single_node(&fs, DIR, 1).unwrap();
        assert_eq!(meta.role_info().1, RoleState::Leader);
        meta.execute(put(0)).unwrap();
        assert!(meta
            .step_rpc(RaftRpc::RequestVote {
                term: 2,
                candidate_id: 2,
                last_log_index: 1,
                last_log_term: 1,
            })
            .unwrap()
            .is_none());
        let (node_id, role, term, leader, commit) = meta.role_info();
        assert_eq!(
            (node_id, role, term, leader),
            (1, RoleState::Follower, 0, None)
        );
        assert_eq!(commit, 1, "a refusal must not change the applied state");
        assert!(matches!(
            meta.execute(put(1)),
            Err(MetaError::NotLeader { leader: None })
        ));
        assert!(meta.get_or_panic("/k/001").is_none());
        assert_eq!(meta.get_or_panic("/k/000").unwrap().value, vec![0]);
    }

    #[test]
    fn a_failed_append_is_neither_applied_nor_acknowledged() {
        let fs = SimFs::new(4304);
        let mut core = single(&fs);
        core.propose(put(0)).unwrap();
        fs.set_faults(yadb_io::FaultConfig {
            sync_data_fault: yadb_io::SyncFault::BeforePersist,
            ..Default::default()
        });
        assert!(matches!(core.propose(put(1)), Err(MetaError::Io(_))));
        assert!(core.get("/k/001").is_none());
        assert_eq!(core.commit_index(), 1);
        // The log is no longer trusted: matching in-memory state is not evidence
        // of durability, so every later write refuses until a reopen.
        fs.set_faults(yadb_io::FaultConfig::default());
        assert!(matches!(
            core.propose(put(2)),
            Err(MetaError::StorageUnavailable)
        ));
        assert!(core.get("/k/002").is_none());
    }
}
