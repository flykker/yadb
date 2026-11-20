//! Synchronous metadata facade for an OpenRaft replica.
//! Reads reflect locally applied state, not a linearizable quorum read.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use crate::openraft_runtime::YadbRaft;
use crate::openraft_state_machine::DurableMetaStateMachine;
use crate::{KvCommand, KvResult, MetaError, MetaSnapshot, RoleState, WatchEvent};
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};

/// How long one group's health observation waits for the Raft core to answer a
/// local `committed` query before reporting the commit index as unavailable.
///
/// `RaftMetrics` carries no commit index, and a wedged core must not turn a
/// read-only health request into a hang: the row then names the reason instead
/// of a number.
const COMMIT_OBSERVATION_BUDGET: Duration = Duration::from_millis(500);

/// One metadata group's locally observed state.
///
/// Every field is this node's own view: the role, term and leader come from the
/// metrics channel, `applied_index` is the last applied log index and
/// `commit_index` is `RaftState::committed`. Nothing here is fetched from a
/// peer, and reading it takes no lease and changes no metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupObservation {
    pub group: u64,
    pub node_id: u32,
    pub role: RoleState,
    pub term: u64,
    pub leader_id: Option<u32>,
    pub applied_index: u64,
    /// `None` only when the Raft core did not answer within
    /// [`COMMIT_OBSERVATION_BUDGET`]; `commit_error` then carries the reason.
    pub commit_index: Option<u64>,
    pub commit_error: Option<String>,
}

/// The committed log entry a client write occupies: its index in the group's
/// log and the leader term that proposed it.
///
/// This pair is what makes an acknowledgement checkable against a log. A
/// replica that holds `index` at `term` holds the entry the acknowledgement
/// named; a replica that does not hold it has not caught up with that write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommittedLogId {
    pub index: u64,
    pub term: u64,
}

/// A client write this leader acknowledged, together with the committed log
/// entry the write occupies.
///
/// The log id is the one OpenRaft returned for this entry — never an index
/// sampled from the local metrics after the fact, which another writer's
/// append may already have moved past. An outcome that is not known to have
/// committed (forward-to-leader, lost reply, timeout) is a [`MetaError`]
/// instead, so it carries no index at all.
///
/// A caller that must answer an idempotent replay with the *original* index
/// stores this value with the replay record: re-serving the stored
/// acknowledgement reports the entry that first committed, not a later one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAck {
    pub result: KvResult,
    pub committed: CommittedLogId,
}

impl WriteAck {
    /// Index of the log entry that holds this write.
    pub fn index(&self) -> u64 {
        self.committed.index
    }

    /// Term of the leader that proposed the entry at [`Self::index`].
    pub fn term(&self) -> u64 {
        self.committed.term
    }
}

/// Control-scope idempotency records live here. It is byte-identical to the
/// prefix `record_key` in `yadbd` builds, so existing records stay readable.
///
/// Control keeps this spelling and only this spelling. It is deliberately *not*
/// `/idempotency/g<control>/`: the same group would then have two namespaces, and
/// a writer that picked the other one would split the control records, and the
/// counter guarding them, across two key spaces in one group.
///
/// `dead_code`: no production caller exists yet. The only consumer today is the
/// multigroup routing test. The routing table entry in `group_id` and the
/// `scan`/`snapshot_group` entries ARE live; these helpers and constants are the
/// prepared contract for the unwritten project-scope record path
/// (`docs/multiraft-idempotency-design.md`). Marking them allowed keeps the
/// warning list meaningful instead of hiding a future real one.
#[allow(dead_code)]
const CONTROL_IDEMPOTENCY_NAMESPACE: &str = "/sys/idempotency/";
/// Project-scope records cannot live under `/sys/`: `group_id` routes every
/// `/sys/` key to the control group, so a project namespace needs a prefix that
/// carries the project id in its first segment. The trailing segments are the
/// subject and key hashes appended by the record writer.
#[allow(dead_code)]
const PROJECT_IDEMPOTENCY_NAMESPACE: &str = "/idempotency/";
/// Group-scope records name their owning group directly:
/// `/idempotency/g<group>/...`. A record and the admission counter that bounds
/// it must be guarded in one transaction, and a transaction may not span groups,
/// so the counter has to be addressable through the same group id as the record.
///
/// The `g` marker keeps the segment distinguishable from a decimal project id in
/// the `/idempotency/` allowlist entry below, which also makes the two families
/// impossible to confuse in `group_id`'s first-segment parse.
const GROUP_IDEMPOTENCY_NAMESPACE: &str = "/idempotency/g";

/// A project id is a canonical decimal `u32` in every project namespace; a
/// noncanonical spelling would address the same group through a second key.
fn project_id(part: &str, key: &str) -> Result<u32, MetaError> {
    let id: u32 = part
        .parse()
        .map_err(|_| MetaError::Routing(format!("invalid project key: {key}")))?;
    if part != id.to_string() {
        return Err(MetaError::Routing(format!(
            "noncanonical project key: {key}"
        )));
    }
    Ok(id)
}

/// A group id carried by a namespace is canonical decimal `u64`, for the same
/// reason a project id is: a second spelling (`g01`, `g+1`) would address the
/// same group through a second key, and `scan` would return both. The `g` marker
/// lives in the namespace prefix, so only the digits are compared here.
fn canonical_group(part: &str, key: &str) -> Result<u64, MetaError> {
    let id: u64 = part
        .parse()
        .map_err(|_| MetaError::Routing(format!("invalid group key: {key}")))?;
    if part != id.to_string() {
        return Err(MetaError::Routing(format!("noncanonical group key: {key}")));
    }
    Ok(id)
}

#[derive(Clone)]
pub(crate) struct GroupRouter {
    control: u64,
    groups: BTreeMap<u64, OpenRaftClient>,
    projects: BTreeMap<u32, u64>,
    singleton: bool,
}

impl GroupRouter {
    pub(crate) fn singleton(client: OpenRaftClient) -> Self {
        Self {
            control: 0,
            groups: BTreeMap::from([(0, client)]),
            projects: BTreeMap::new(),
            singleton: true,
        }
    }
    pub(crate) fn new(
        control: u64,
        groups: BTreeMap<u64, (YadbRaft, DurableMetaStateMachine)>,
        projects: BTreeMap<u32, u64>,
        handle: tokio::runtime::Handle,
    ) -> Result<Self, MetaError> {
        let data: std::collections::BTreeSet<_> = projects.values().copied().collect();
        if control == 0
            || groups.contains_key(&0)
            || !groups.contains_key(&control)
            || data.len() < 2
            || data.contains(&control)
            || data.iter().any(|id| !groups.contains_key(id))
        {
            return Err(MetaError::Routing(
                "control and at least two mapped, distinct data groups required".into(),
            ));
        }
        let nodes: std::collections::BTreeSet<_> = groups
            .values()
            .map(|(raft, _)| raft.metrics().borrow().id)
            .collect();
        if nodes.len() != 1 {
            return Err(MetaError::Routing(
                "all local groups must belong to the same node".into(),
            ));
        }
        Ok(Self {
            control,
            groups: groups
                .into_iter()
                .map(|(id, (raft, state))| (id, OpenRaftClient::new(raft, state, handle.clone())))
                .collect(),
            projects,
            singleton: false,
        })
    }
    /// Route one key to its owning group. This is the single source of truth for
    /// the mapping between key namespaces and groups.
    pub(crate) fn group_id(&self, key: &str) -> Result<u64, MetaError> {
        if self.singleton || key.starts_with("/sys/") {
            return Ok(self.control);
        }
        let rest = [
            "/db/",
            "/sql-blob-sequence/",
            "/s3/",
            "/s3-intent/",
            "/s3-retired/",
            "/s3-upload-sequence/",
            "/block/",
            "/block-write-sequence/",
            // Per-group garbage-collection records: `/gc/g<group>/...`.
            crate::gc::GROUP_GC_NAMESPACE,
            // Must precede `/idempotency/`, or a group namespace would match the
            // project entry and its `g` segment would be parsed as a project id,
            // failing as noncanonical instead of routing to the named group.
            GROUP_IDEMPOTENCY_NAMESPACE,
            "/idempotency/",
        ]
        .iter()
        .find_map(|p| key.strip_prefix(p))
        .ok_or_else(|| MetaError::Routing(format!("unknown namespace: {key}")))?;
        let part = rest.split('/').next().unwrap_or("");
        let sequence = [
            "/sql-blob-sequence/",
            "/s3-upload-sequence/",
            "/block-write-sequence/",
        ]
        .iter()
        .any(|p| key.starts_with(p));
        if key.starts_with(GROUP_IDEMPOTENCY_NAMESPACE) {
            // Same separator rule as a project prefix: a bare `/idempotency/g2`
            // must not be able to match `/idempotency/g20/...` in another group.
            if !rest.contains('/') {
                return Err(MetaError::Routing(format!("invalid group key: {key}")));
            }
            return self.configured_group(canonical_group(part, key)?);
        }
        if key.starts_with(crate::gc::GROUP_GC_NAMESPACE) {
            // `/gc/g<group>/...` names one data group. The same separator rule
            // keeps `/gc/g2` from matching `/gc/g20/...` in another group.
            if !rest.contains('/') {
                return Err(MetaError::Routing(format!("invalid group key: {key}")));
            }
            return self.configured_group(canonical_group(part, key)?);
        }
        if (sequence && rest.contains('/')) || (!sequence && !rest.contains('/')) {
            return Err(MetaError::Routing(format!("invalid project key: {key}")));
        }
        let id = project_id(part, key)?;
        self.projects
            .get(&id)
            .copied()
            .ok_or_else(|| MetaError::Routing(format!("unmapped project: {id}")))
    }
    /// Resolve a group id carried by a namespace against the groups this router
    /// actually owns. Failing closed matters twice: a caller must never mint a
    /// key that `group_id` would reject, and a project-scope writer must never
    /// compute a counter key that lands in a different group than the record it
    /// guards.
    ///
    /// Control is addressed only as `/sys/idempotency/`. `g<control>` names a
    /// group this router does own, so the id check alone would accept it; it is
    /// rejected explicitly because two spellings of one group are a routing bug
    /// waiting for a writer to pick the wrong one. Group 0 is never in `groups`
    /// for a multi-group router, so `g0` fails the same check. A singleton router
    /// owns only control, so it has no group-scoped namespace at all.
    fn configured_group(&self, group: u64) -> Result<u64, MetaError> {
        if group == self.control || !self.groups.contains_key(&group) {
            return Err(MetaError::Routing(format!("unconfigured group: {group}")));
        }
        Ok(group)
    }
    /// Group that owns `project`'s namespace, or the control group when `project`
    /// is `None`. Fails closed when the project has no explicit group mapping:
    /// silently falling back to control would write a project record into a group
    /// that never resolves it.
    ///
    /// `dead_code`: the routing it describes is live, but no production caller
    /// selects a project-scope record yet; only the multigroup test calls this.
    #[allow(dead_code)]
    pub(crate) fn group_id_for_project(&self, project: Option<&str>) -> Result<u64, MetaError> {
        let Some(project) = project else {
            return Ok(self.control);
        };
        // `group_id` short-circuits singleton mode to control before validating
        // the namespace, so project names are not validated there either.
        if self.singleton {
            return Ok(self.control);
        }
        let id = project_id(project, project)?;
        self.projects
            .get(&id)
            .copied()
            .ok_or_else(|| MetaError::Routing(format!("unmapped project: {id}")))
    }
    /// Key namespace for idempotency records owned by `project`, or the control
    /// namespace when `project` is `None`. The result always routes to
    /// `group_id_for_project(project)`; an unmapped project is an error rather
    /// than a namespace whose records could never be read back.
    ///
    /// `dead_code`: prepared contract with no production consumer yet, see
    /// `group_id_for_project`.
    #[allow(dead_code)]
    pub(crate) fn idempotency_namespace(&self, project: Option<&str>) -> Result<String, MetaError> {
        let Some(project) = project else {
            return Ok(CONTROL_IDEMPOTENCY_NAMESPACE.to_owned());
        };
        // Resolve first so this can never emit a key the router would reject or
        // send to a different group.
        self.group_id_for_project(Some(project))?;
        Ok(format!("{PROJECT_IDEMPOTENCY_NAMESPACE}{project}/"))
    }
    /// Key namespace for idempotency records owned by `group`, i.e.
    /// `/idempotency/g<group>/`. The group id is resolved against this router
    /// first, so a caller can never mint a namespace that `group_id` would reject
    /// or send to a group other than the one it names.
    ///
    /// `dead_code`: prepared contract with no production consumer yet, see
    /// `group_id_for_project`.
    #[allow(dead_code)]
    pub(crate) fn group_idempotency_namespace(&self, group: u64) -> Result<String, MetaError> {
        self.configured_group(group)?;
        Ok(format!("{GROUP_IDEMPOTENCY_NAMESPACE}{group}/"))
    }
    /// Admission counter key for `group`'s idempotency records:
    /// `/idempotency/g<group>/_usage`.
    ///
    /// It must live in the same group as the records it bounds: the writer guards
    /// the counter and the new record in one transaction, and `group_for` rejects
    /// a transaction whose keys span groups. Deriving it from
    /// `group_idempotency_namespace` keeps the two in one group by construction.
    ///
    /// `dead_code`: prepared contract with no production consumer yet, see
    /// `group_id_for_project`.
    #[allow(dead_code)]
    pub(crate) fn group_idempotency_usage_key(&self, group: u64) -> Result<String, MetaError> {
        Ok(format!(
            "{}_usage",
            self.group_idempotency_namespace(group)?
        ))
    }
    /// Namespace of the group that owns `project`'s idempotency records. This is
    /// the constructor a project-scope writer uses: it resolves the project with
    /// `group_id_for_project` instead of requiring the caller to know the group
    /// id, so two projects that share a group also share one record namespace and
    /// one counter. An unmapped project is an error rather than a control-group
    /// fallback, whose records would never be resolved back to the project.
    ///
    /// `dead_code`: prepared contract with no production consumer yet, see
    /// `group_id_for_project`.
    #[allow(dead_code)]
    pub(crate) fn group_idempotency_namespace_for_project(
        &self,
        project: &str,
    ) -> Result<String, MetaError> {
        let group = self.group_id_for_project(Some(project))?;
        self.group_idempotency_namespace(group)
    }
    /// The single group that owns every key in `keys`.
    ///
    /// Every key is routed before any group is returned, so a scope that spans
    /// groups fails here with [`MetaError::Routing`] and the command is never
    /// handed to a Raft core. That is the "before submission" half of the
    /// cross-group rule: no group appends a log entry for a rejected command.
    fn group_for<'a, 'k>(
        &'a self,
        keys: impl IntoIterator<Item = &'k str>,
    ) -> Result<&'a OpenRaftClient, MetaError> {
        let mut group = None;
        for key in keys {
            let id = self.group_id(key)?;
            if group.is_some_and(|old| old != id) {
                return Err(MetaError::Routing("cross-group metadata operation".into()));
            }
            group = Some(id);
        }
        let id = group.ok_or_else(|| MetaError::Routing("operation has no routing key".into()))?;
        Ok(&self.groups[&id])
    }
    pub(crate) fn execute(&self, cmd: KvCommand) -> Result<KvResult, MetaError> {
        self.group_for(routing_keys(&cmd))?.execute(cmd)
    }
    /// Submit one write and report the committed log entry it occupies.
    ///
    /// The keys are routed before anything is submitted, on the same rule as
    /// [`Self::execute`]: a scope spanning groups fails here and no group
    /// appends an entry.
    ///
    /// `dead_code`: the library-level route the daemon's write path will call
    /// once the acknowledgement is surfaced; the in-crate tests drive
    /// `OpenRaftClient::execute_with_ack` directly. Marking it allowed keeps the
    /// warning list meaningful until that caller exists.
    #[allow(dead_code)]
    pub(crate) fn execute_with_ack(&self, cmd: KvCommand) -> Result<WriteAck, MetaError> {
        self.group_for(routing_keys(&cmd))?.execute_with_ack(cmd)
    }
    pub(crate) fn try_snapshot(
        &self,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        let group = self.snapshot_group(keys, prefixes)?;
        group.state.try_snapshot(keys, prefixes)
    }
    fn snapshot_group(
        &self,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<&OpenRaftClient, MetaError> {
        if !self.singleton {
            for prefix in prefixes {
                for namespace in [
                    "/db/",
                    "/s3/",
                    "/s3-intent/",
                    "/s3-retired/",
                    "/block/",
                    // Same reason as the data namespaces: `/idempotency/1` must not
                    // be able to match `/idempotency/10/...` in another group, so a
                    // project prefix has to carry its separator.
                    "/idempotency/",
                    // Group prefixes need the identical rule: `/idempotency/g2`
                    // could otherwise match `/idempotency/g20/...` in another
                    // group. Both entries match a group prefix, and both reject
                    // it for the same missing-separator reason.
                    GROUP_IDEMPOTENCY_NAMESPACE,
                ] {
                    if let Some(rest) = prefix.strip_prefix(namespace) {
                        if !rest.contains('/') {
                            return Err(MetaError::Routing(
                                "namespace prefix must include trailing separator".into(),
                            ));
                        }
                    }
                }
                if [
                    "/sql-blob-sequence/",
                    "/s3-upload-sequence/",
                    "/block-write-sequence/",
                ]
                .iter()
                .any(|namespace| prefix.starts_with(namespace))
                {
                    return Err(MetaError::Routing("sequence prefix is ambiguous".into()));
                }
            }
        }
        self.group_for(keys.iter().chain(prefixes).map(String::as_str))
    }
    /// Quorum-confirmed snapshot of one selected group; followers and replicas
    /// without quorum return an error rather than locally applied state.
    pub(crate) fn linearizable_snapshot(
        &self,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        self.snapshot_group(keys, prefixes)?
            .linearizable_snapshot(keys, prefixes)
    }
    pub(crate) fn role_info(&self) -> (u32, RoleState, u64, Option<u32>, u64) {
        self.groups[&self.control].role_info()
    }
    /// A quorum-confirmed snapshot of the control group. This is only valid on
    /// the current control-group leader; followers must ask that leader.
    pub(crate) fn linearizable_control_snapshot(
        &self,
        keys: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        self.groups[&self.control].linearizable_snapshot(keys, &[])
    }
    pub(crate) fn role_for_key(
        &self,
        key: &str,
    ) -> Result<(u32, RoleState, u64, Option<u32>, u64), MetaError> {
        Ok(self.group_for([key])?.role_info())
    }
    pub(crate) fn revision_for_key(&self, key: &str) -> Result<u64, MetaError> {
        Ok(self
            .group_for([key])?
            .state
            .try_snapshot(&[], &[])?
            .revision())
    }
    /// One scope per data group a per-group collection pass may run over.
    ///
    /// A single-group deployment has no explicit project routing, so its one
    /// scope covers every project: every fragment belongs to that group.
    pub(crate) fn gc_scopes(&self) -> Vec<crate::gc::GcGroupScope> {
        if self.singleton {
            return vec![crate::gc::GcGroupScope {
                group_id: self.control,
                project_ids: None,
            }];
        }
        let mut by_group: BTreeMap<u64, std::collections::BTreeSet<u32>> = BTreeMap::new();
        for (project, group) in &self.projects {
            by_group.entry(*group).or_default().insert(*project);
        }
        by_group
            .into_iter()
            .map(|(group_id, project_ids)| crate::gc::GcGroupScope {
                group_id,
                project_ids: Some(project_ids),
            })
            .collect()
    }
    /// The data group that owns `project`, failing closed for an unmapped one.
    pub(crate) fn group_for_project_id(&self, project: u32) -> Result<u64, MetaError> {
        if self.singleton {
            return Ok(self.control);
        }
        self.projects
            .get(&project)
            .copied()
            .ok_or_else(|| MetaError::Routing(format!("unmapped project: {project}")))
    }
    pub(crate) fn role_for_group(
        &self,
        group: u64,
    ) -> Result<(u32, RoleState, u64, Option<u32>, u64), MetaError> {
        self.groups
            .get(&group)
            .map(|client| client.role_info())
            .ok_or_else(|| MetaError::Routing(format!("unconfigured group: {group}")))
    }
    /// Every configured local group, observed from this node's own handles.
    ///
    /// Used by the daemon's read-only health surface, so it must stay a local
    /// observation: no peer is contacted and no metadata key is read.
    pub(crate) async fn observations(&self) -> Vec<GroupObservation> {
        let mut rows = Vec::with_capacity(self.groups.len());
        for (group, client) in &self.groups {
            rows.push(client.observation(*group).await);
        }
        rows
    }
    /// Quorum-confirmed snapshot of exactly one group, named directly.
    pub(crate) fn linearizable_group_snapshot(
        &self,
        group: u64,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        self.groups
            .get(&group)
            .ok_or_else(|| MetaError::Routing(format!("unconfigured group: {group}")))?
            .linearizable_snapshot(keys, prefixes)
    }
    pub(crate) fn watch_prefix(&self, prefix: String) -> mpsc::Receiver<WatchEvent> {
        self.try_snapshot(&[], &[prefix.clone()])
            .expect("invalid watch scope");
        self.group_for([prefix.as_str()])
            .expect("invalid watch scope")
            .watch_prefix(prefix)
    }
    /// The deployment-wide revision, available only in singleton mode.
    ///
    /// Multi-group metadata has one revision per group and no global one, so
    /// this refuses instead of combining or synthesizing the group revisions.
    /// Use `revision_for_key` for a key's owning group and
    /// `role_info_for_group(..).4` for a group's applied index.
    pub(crate) fn try_revision(&self) -> Result<u64, MetaError> {
        if !self.singleton {
            return Err(MetaError::Routing(
                "multi-group metadata has no global revision".into(),
            ));
        }
        Ok(self.groups[&self.control]
            .state
            .try_snapshot(&[], &[])?
            .revision())
    }
    /// Merged scan across every group that can hold keys under `prefix`.
    ///
    /// This is an administrative **local observation, not a snapshot**. Each
    /// group's rows are copied from this node's applied state at a different
    /// instant and then concatenated, so a write that commits in one group
    /// between another group's read can appear for one and not the other. The
    /// result carries no revision and is not a point-in-time view. A caller
    /// that needs one instant must read a single group with `try_snapshot` /
    /// `linearizable_snapshot`, which reject a scope spanning groups.
    pub(crate) fn try_scan(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, crate::KvValue)>, MetaError> {
        if !self.singleton
            && [
                "/db/",
                "/sql-blob-sequence/",
                "/s3/",
                "/s3-intent/",
                "/s3-retired/",
                "/s3-upload-sequence/",
                "/block/",
                "/block-write-sequence/",
                // A project-scoped prefix spans several groups, so it needs the
                // same fan-out as the data namespaces. Without this entry
                // `scan("/idempotency/")` falls through to `try_snapshot`, whose
                // single-group routing rejects a bare project prefix, and the
                // `?` below would turn that into an error rather than a fan-out.
                "/idempotency/",
                // A bare group prefix spans every data group for the same reason.
                // Without this entry `scan("/idempotency/g")` reaches
                // `try_snapshot`, whose separator rule rejects the bare prefix,
                // and the read fails instead of covering every group.
                GROUP_IDEMPOTENCY_NAMESPACE,
            ]
            .contains(&prefix)
        {
            // Project namespaces exist only in the groups their project maps to,
            // so the `/idempotency/` fan-out uses `projects`. A group-scoped
            // record can be written into any configured data group, including one
            // that no project currently maps to, so its fan-out must cover every
            // non-control group instead. Control is excluded: it is addressed only
            // as `/sys/idempotency/`.
            let ids: std::collections::BTreeSet<u64> = if prefix == GROUP_IDEMPOTENCY_NAMESPACE {
                self.groups
                    .keys()
                    .copied()
                    .filter(|id| *id != self.control)
                    .collect()
            } else {
                self.projects.values().copied().collect()
            };
            let mut values = BTreeMap::new();
            for id in ids {
                values.extend(
                    self.groups[&id]
                        .state
                        .try_snapshot(&[], &[prefix.to_owned()])?
                        .scan_prefix(prefix),
                );
            }
            return Ok(values.into_iter().collect());
        }
        Ok(self
            .try_snapshot(&[], &[prefix.to_owned()])?
            .scan_prefix(prefix))
    }
}

/// Every key `cmd` touches, in the order the routing rules see them.
///
/// Shared by the two write entry points so `execute` and `execute_with_ack`
/// can never disagree about which group owns a command.
fn routing_keys(cmd: &KvCommand) -> Vec<&str> {
    use crate::TxnOp;
    let mut keys: Vec<&str> = Vec::new();
    match cmd {
        KvCommand::Put { key, .. }
        | KvCommand::Delete { key }
        | KvCommand::CompareAndSwap { key, .. } => keys.push(key),
        KvCommand::Txn { guards, ops } => {
            keys.extend(guards.iter().map(|g| g.key.as_str()));
            keys.extend(ops.iter().map(|op| match op {
                TxnOp::Put { key, .. } | TxnOp::Delete { key } => key.as_str(),
            }));
        }
        KvCommand::RevisionTxn { guards, ops } => {
            keys.extend(guards.iter().map(|g| g.key.as_str()));
            keys.extend(ops.iter().map(|op| match op {
                TxnOp::Put { key, .. } | TxnOp::Delete { key } => key.as_str(),
            }));
        }
    }
    keys
}

type Request = (KvCommand, mpsc::SyncSender<Result<WriteAck, MetaError>>);

/// Enqueue a command for the bridge worker.
///
/// `try_send` fails in exactly two ways — `Full` and `Disconnected` — and
/// neither can have enqueued the command, so the write provably never reached
/// Raft. Reporting that as `not-sent` lets the caller retry; `unknown` would
/// forbid a retry for a mutation that definitively did not happen.
fn enqueue(sender: &mpsc::SyncSender<Request>, request: Request) -> Result<(), MetaError> {
    sender.try_send(request).map_err(|_| {
        MetaError::OpenRaftWriteNotSent("metadata bridge unavailable or full".into())
    })
}

/// Wait for the bridge worker's reply to an already-enqueued command.
///
/// A dropped or silent worker is genuinely ambiguous: the command was enqueued,
/// so it may have been submitted to Raft before the reply path disappeared.
/// The result is then an error, which carries no log id: reporting an index
/// here would claim a commit the leader never confirmed.
fn await_reply(
    receiver: mpsc::Receiver<Result<WriteAck, MetaError>>,
) -> Result<WriteAck, MetaError> {
    receiver
        .recv_timeout(Duration::from_secs(6))
        .map_err(|_| {
            MetaError::OpenRaftWriteUnknown(
                "metadata bridge response timed out or stopped".into(),
            )
        })
        .and_then(|inner| inner)
}

#[derive(Clone)]
pub(crate) struct OpenRaftClient {
    raft: YadbRaft,
    state: DurableMetaStateMachine,
    handle: tokio::runtime::Handle,
    bridge: Arc<Bridge>,
}

struct Bridge {
    sender: Option<mpsc::SyncSender<Request>>,
    worker: Option<std::thread::JoinHandle<()>>,
    stopping: Arc<AtomicBool>,
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let join = || {
                let _ = worker.join();
            };
            if tokio::runtime::Handle::try_current()
                .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
            {
                tokio::task::block_in_place(join);
            } else {
                join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multiraft::{GroupIdentity, ReplicaIdentity};
    use crate::openraft_runtime::YadbRaftTypeConfig as C;
    use crate::openraft_storage::DurableOpenRaftLog;
    use crate::{CasGuard, LocalMetaKv, RevisionGuard, TxnOp};
    use openraft::error::{
        Fatal, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError, Unreachable,
    };
    use openraft::network::RPCOption;
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::{
        BasicNode, EntryPayload, RaftLogReader, RaftNetwork, RaftNetworkFactory, Snapshot, Vote,
    };
    use std::{collections::BTreeMap, io, sync::Arc};
    use yadb_io::SimFs;

    fn delete_command() -> KvCommand {
        KvCommand::Delete { key: "k".into() }
    }

    #[test]
    fn a_full_bridge_reports_not_sent_not_unknown() {
        // Capacity zero makes every `try_send` find the queue full. The command
        // is provably not enqueued, so the outcome must be retryable.
        let (sender, _receiver) = mpsc::sync_channel(0);
        let (reply, _reply_rx) = mpsc::sync_channel(1);
        let error = enqueue(&sender, (delete_command(), reply)).unwrap_err();
        assert!(
            matches!(error, MetaError::OpenRaftWriteNotSent(_)),
            "a full bridge must be not-sent, got {error:?}"
        );
        assert_eq!(
            error.to_string(),
            "OpenRaft write was not sent: metadata bridge unavailable or full"
        );
    }

    #[test]
    fn a_disconnected_bridge_reports_not_sent_not_unknown() {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(receiver);
        let (reply, _reply_rx) = mpsc::sync_channel(1);
        let error = enqueue(&sender, (delete_command(), reply)).unwrap_err();
        assert!(
            matches!(error, MetaError::OpenRaftWriteNotSent(_)),
            "a closed bridge must be not-sent, got {error:?}"
        );
    }

    #[test]
    fn a_lost_reply_after_a_successful_send_stays_unknown() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (reply, reply_rx) = mpsc::sync_channel(1);
        enqueue(&sender, (delete_command(), reply)).unwrap();
        // Take the accepted request out and drop it: the reply sender dies, as
        // it would if the worker exited after accepting the command. The write
        // was enqueued, so this outcome is ambiguous and must not be retryable.
        let accepted = receiver.recv().unwrap();
        drop(accepted);
        let error = await_reply(reply_rx).unwrap_err();
        assert!(
            matches!(error, MetaError::OpenRaftWriteUnknown(_)),
            "a lost reply must stay unknown, got {error:?}"
        );
    }

    struct NoNetwork;
    impl RaftNetworkFactory<C> for NoNetwork {
        type Network = NoNetwork;
        async fn new_client(&mut self, _: u32, _: &BasicNode) -> NoNetwork {
            NoNetwork
        }
    }
    impl RaftNetwork<C> for NoNetwork {
        async fn append_entries(
            &mut self,
            _: AppendEntriesRequest<C>,
            _: RPCOption,
        ) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
            Err(Unreachable::new(&io::Error::other("singleton")).into())
        }
        async fn vote(
            &mut self,
            _: VoteRequest<u32>,
            _: RPCOption,
        ) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
            Err(Unreachable::new(&io::Error::other("singleton")).into())
        }
        async fn full_snapshot(
            &mut self,
            _: Vote<u32>,
            _: Snapshot<C>,
            _: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
            _: RPCOption,
        ) -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
            Err(Unreachable::new(&io::Error::other("singleton")).into())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn singleton_client_writes_watch_aba_and_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "yadb-adapter-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let id = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [44; 16],
                group: 1,
                incarnation: 1,
            },
            1,
            vec![1],
        )
        .unwrap();
        let log = DurableOpenRaftLog::create(dir.join("log"), &id).unwrap();
        let state = DurableMetaStateMachine::create(dir.join("state"), &id).unwrap();
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let raft = YadbRaft::new(1, config.clone(), NoNetwork, log, state.clone())
            .await
            .unwrap();
        raft.initialize(BTreeMap::from([(1, BasicNode::new("one"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        let meta: LocalMetaKv<SimFs> = LocalMetaKv::from_openraft(
            raft.clone(),
            state,
            BTreeMap::new(),
            tokio::runtime::Handle::current(),
        );
        assert!(matches!(
            meta.execute(KvCommand::Put {
                key: "runtime-worker".into(),
                value: vec![1]
            }),
            Ok(KvResult::PutOk { .. })
        ));
        let current_thread_meta = meta.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                assert!(matches!(
                    current_thread_meta.execute(KvCommand::Put {
                        key: "must-not-submit".into(),
                        value: vec![1]
                    }),
                    Err(MetaError::OpenRaftAsyncContext)
                ));
            });
        })
        .join()
        .unwrap();
        assert!(meta.get_or_panic("must-not-submit").is_none());
        let worker = meta.clone();
        tokio::task::spawn_blocking(move || {
            let watch = worker.watch_prefix("k");
            assert!(matches!(
                worker
                    .execute(KvCommand::CompareAndSwap {
                        key: "k".into(),
                        expected_version: 0,
                        new_value: Some(b"one".to_vec())
                    })
                    .unwrap(),
                KvResult::CasOk { .. }
            ));
            assert!(watch.recv_timeout(Duration::from_secs(2)).is_ok());
            let old = worker.get_or_panic("k").unwrap();
            worker
                .execute(KvCommand::Delete { key: "k".into() })
                .unwrap();
            worker
                .execute(KvCommand::Put {
                    key: "k".into(),
                    value: b"two".to_vec(),
                })
                .unwrap();
            assert!(matches!(
                worker
                    .execute(KvCommand::RevisionTxn {
                        guards: vec![RevisionGuard {
                            key: "k".into(),
                            expected_mod_revision: old.mod_revision
                        }],
                        ops: vec![TxnOp::Delete { key: "k".into() }]
                    })
                    .unwrap(),
                KvResult::CasConflict { .. }
            ));
            worker
                .execute(KvCommand::RevisionTxn {
                    guards: vec![],
                    ops: vec![
                        TxnOp::Put {
                            key: "p/a".into(),
                            value: vec![1],
                        },
                        TxnOp::Put {
                            key: "p/b".into(),
                            value: vec![2],
                        },
                    ],
                })
                .unwrap();
            let snapshot = worker.snapshot_or_panic(&["k".into()], &["p/".into()]);
            assert_eq!(snapshot.scan_prefix("p/").len(), 2);
            assert_eq!(snapshot.revision(), worker.revision_or_panic());
        })
        .await
        .unwrap();
        drop(meta);
        raft.shutdown().await.unwrap();
        drop(raft);
        let log = DurableOpenRaftLog::open(dir.join("log"), &id).unwrap();
        let state = DurableMetaStateMachine::open(dir.join("state"), &id).unwrap();
        let raft = YadbRaft::new(1, config, NoNetwork, log, state.clone())
            .await
            .unwrap();
        let meta: LocalMetaKv<SimFs> = LocalMetaKv::from_openraft(
            raft.clone(),
            state,
            BTreeMap::new(),
            tokio::runtime::Handle::current(),
        );
        assert_eq!(meta.get_or_panic("k").unwrap().value, b"two");
        drop(meta);
        raft.shutdown().await.unwrap();
        drop(raft);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A snapshot the local state machine cannot serve is an error, not a panic
    /// and not "the key is absent".
    ///
    /// This is the inversion for `try_get`. Implemented as the old panicking
    /// read, it aborts this test task. Implemented as `try_snapshot(..).ok()`,
    /// it reports `Ok(None)` both for a key that is present and for one that is
    /// absent, so both assertions below fail: "could not read" would become
    /// "not found" and the caller would answer 404 for a key it never saw.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreadable_state_machine_errors_instead_of_reporting_absence() {
        let dir = std::env::temp_dir().join(format!(
            "yadb-unreadable-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let id = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [45; 16],
                group: 1,
                incarnation: 1,
            },
            1,
            vec![1],
        )
        .unwrap();
        let log = DurableOpenRaftLog::create(dir.join("log"), &id).unwrap();
        let state = DurableMetaStateMachine::create(dir.join("state"), &id).unwrap();
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let raft = YadbRaft::new(1, config, NoNetwork, log, state.clone())
            .await
            .unwrap();
        raft.initialize(BTreeMap::from([(1, BasicNode::new("one"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        let meta: LocalMetaKv<SimFs> = LocalMetaKv::from_openraft(
            raft.clone(),
            state.clone(),
            BTreeMap::new(),
            tokio::runtime::Handle::current(),
        );
        meta.execute(KvCommand::Put {
            key: "present".into(),
            value: b"value".to_vec(),
        })
        .unwrap();
        // Before the fault, presence and absence are distinguishable.
        assert_eq!(
            meta.try_get("present").unwrap().unwrap().value,
            b"value".to_vec()
        );
        assert!(meta.try_get("absent").unwrap().is_none());

        // The state a failed durable apply leaves behind: every read of the
        // applied snapshot is refused until this node is recovered.
        state.poison_for_test();

        for key in ["present", "absent"] {
            assert!(
                matches!(meta.try_get(key), Err(MetaError::StorageUnavailable)),
                "try_get({key}) must report the unreadable snapshot, not a value or absence"
            );
        }
        assert!(matches!(
            meta.try_scan_prefix(""),
            Err(MetaError::StorageUnavailable)
        ));
        assert!(matches!(
            meta.try_revision(),
            Err(MetaError::StorageUnavailable)
        ));
        assert!(matches!(
            meta.try_snapshot(&["present".into()], &[]),
            Err(MetaError::StorageUnavailable)
        ));
        // The GC fence readers must make the same distinction. `live_collector`
        // used to flatten its error into `None`, which reads as "no pass holds
        // the record" - the direction that admits a second collector on a
        // namespace whose state is unknown.
        assert!(
            matches!(crate::gc::live_collector(&meta), Err(MetaError::StorageUnavailable)),
            "an unreadable collector record must refuse, not report an idle pass"
        );
        assert!(
            matches!(crate::gc::live_leases(&meta), Err(MetaError::StorageUnavailable)),
            "an unreadable lease namespace must refuse, not report no live leases"
        );

        drop(meta);
        raft.shutdown().await.unwrap();
        drop(raft);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Open a state directory that a just-shut-down replica still holds for one
    /// task turn.
    ///
    /// `openraft::Raft::new` spawns a state-machine worker that owns the `SM`
    /// (`openraft-0.9.25` `src/core/sm/worker.rs`), and `Raft::shutdown` joins
    /// only the core and tick tasks. The worker exits when its command channel
    /// closes, on a later scheduling turn, so its clone of this state machine —
    /// and the directory lock that clone holds — outlives `shutdown()` by that
    /// turn. An immediate in-process reopen therefore races it and can see
    /// `WouldBlock`. This waits for the handle to be released instead of
    /// treating one upstream task turn as corruption.
    async fn open_state_after_worker_exit(
        dir: &std::path::Path,
        identity: &ReplicaIdentity,
    ) -> DurableMetaStateMachine {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match DurableMetaStateMachine::open(dir, identity) {
                Ok(state) => return state,
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(error) => panic!(
                    "state directory {} did not become free: {error}",
                    dir.display()
                ),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multiple_real_groups_route_and_reject_cross_group() {
        let dir = std::env::temp_dir().join(format!(
            "yadb-routing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let mut groups = BTreeMap::new();
        let mut rafts = Vec::new();
        for group in 1..=3 {
            let id = ReplicaIdentity::new(
                GroupIdentity {
                    cluster: [45; 16],
                    group,
                    incarnation: 1,
                },
                1,
                vec![1],
            )
            .unwrap();
            let log = DurableOpenRaftLog::create(dir.join(format!("log-{group}")), &id).unwrap();
            let state =
                DurableMetaStateMachine::create(dir.join(format!("state-{group}")), &id).unwrap();
            let raft = YadbRaft::new(1, config.clone(), NoNetwork, log, state.clone())
                .await
                .unwrap();
            raft.initialize(BTreeMap::from([(1, BasicNode::new("one"))]))
                .await
                .unwrap();
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "leader")
                .await
                .unwrap();
            groups.insert(group, (raft.clone(), state));
            rafts.push(raft);
        }
        let meta: LocalMetaKv<SimFs> = LocalMetaKv::from_openraft_groups(
            1,
            groups,
            BTreeMap::from([(1, 2), (2, 3)]),
            BTreeMap::new(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        for key in ["/sys/config", "/db/1/a", "/db/2/a"] {
            meta.execute(KvCommand::Put {
                key: key.into(),
                value: key.as_bytes().to_vec(),
            })
            .unwrap();
        }
        // A rejected cross-group command must not reach any group's Raft core:
        // an accepted write appends a log entry even if it never applies, so the
        // leaders' log and applied indices are the observable proof. All three
        // groups are single-node leaders here, so a submission would apply
        // immediately and this snapshot would move.
        let progress = |rafts: &[YadbRaft]| -> Vec<(u64, u64)> {
            rafts
                .iter()
                .map(|raft| {
                    let metrics = raft.metrics().borrow().clone();
                    (
                        metrics.last_log_index.unwrap_or(0),
                        metrics.last_applied.map_or(0, |id| id.index),
                    )
                })
                .collect()
        };
        let before = progress(&rafts);
        let rev1 = meta.revision_for_key("/db/1/a").unwrap();
        let rev2 = meta.revision_for_key("/db/2/a").unwrap();
        assert!(matches!(
            meta.execute(KvCommand::RevisionTxn {
                guards: vec![],
                ops: vec![
                    TxnOp::Delete {
                        key: "/db/1/a".into()
                    },
                    TxnOp::Delete {
                        key: "/db/2/a".into()
                    }
                ]
            }),
            Err(MetaError::Routing(_))
        ));
        assert!(matches!(
            meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: "/db/1/a".into(),
                    expected_mod_revision: 1
                }],
                ops: vec![TxnOp::Delete {
                    key: "/db/2/a".into()
                }]
            }),
            Err(MetaError::Routing(_))
        ));
        // A plain `Txn` is rejected on the same rule: the guard names project 1's
        // group and the op names project 2's. So is a `Txn` whose ops alone span
        // both groups.
        assert!(matches!(
            meta.execute(KvCommand::Txn {
                guards: vec![CasGuard {
                    key: "/db/1/a".into(),
                    expected_version: 1
                }],
                ops: vec![TxnOp::Put {
                    key: "/db/2/b".into(),
                    value: vec![1]
                }]
            }),
            Err(MetaError::Routing(_))
        ));
        assert!(matches!(
            meta.execute(KvCommand::Txn {
                guards: vec![],
                ops: vec![
                    TxnOp::Put {
                        key: "/db/1/x".into(),
                        value: vec![1]
                    },
                    TxnOp::Put {
                        key: "/db/2/y".into(),
                        value: vec![1]
                    }
                ]
            }),
            Err(MetaError::Routing(_))
        ));
        assert!(matches!(
            meta.try_snapshot(&["/db/1/a".into(), "/db/2/a".into()], &[]),
            Err(MetaError::Routing(_))
        ));
        // Two prefixes that route to different groups have no single revision
        // to carry, so no snapshot is produced for them either.
        assert!(matches!(
            meta.try_snapshot(&[], &["/db/1/".into(), "/db/2/".into()]),
            Err(MetaError::Routing(_))
        ));
        // Multi-group metadata has no global revision to synthesize.
        assert!(matches!(meta.try_revision(), Err(MetaError::Routing(_))));
        // The GC lease path stamps its revision through `legacy_revision`,
        // which is `0` on OpenRaft rather than a computed global value.
        assert_eq!(meta.legacy_revision(), 0);
        // Nothing above may have been submitted anywhere. A write that was
        // accepted would append to one group's log before the call returned.
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            progress(&rafts),
            before,
            "a rejected cross-group command must not be submitted to any group: \
             an accepted write appends a log entry even if it never applies"
        );
        for key in ["/db/2/b", "/db/1/x", "/db/2/y"] {
            assert!(
                meta.get_or_panic(key).is_none(),
                "{key} must not exist after a rejected cross-group transaction"
            );
        }
        assert!(matches!(
            meta.try_snapshot(&[], &["/db/1".into()]),
            Err(MetaError::Routing(_))
        ));
        assert!(meta.try_snapshot(&[], &["/sys/".into()]).is_ok());
        assert_eq!(meta.revision_for_key("/db/1/a").unwrap(), rev1);
        assert_eq!(meta.revision_for_key("/db/2/a").unwrap(), rev2);
        assert_eq!(meta.scan_prefix_or_panic("/db/").len(), 2);
        assert!(matches!(
            meta.execute(KvCommand::Delete {
                key: "/db/3/a".into()
            }),
            Err(MetaError::Routing(_))
        ));
        drop(meta);
        for raft in &rafts {
            raft.shutdown().await.unwrap();
        }
        drop(rafts);
        let mut reopened = BTreeMap::new();
        let mut rafts = Vec::new();
        for group in 1..=3 {
            let id = ReplicaIdentity::new(
                GroupIdentity {
                    cluster: [45; 16],
                    group,
                    incarnation: 1,
                },
                1,
                vec![1],
            )
            .unwrap();
            let log = DurableOpenRaftLog::open(dir.join(format!("log-{group}")), &id).unwrap();
            let state =
                open_state_after_worker_exit(&dir.join(format!("state-{group}")), &id).await;
            let raft = YadbRaft::new(1, config.clone(), NoNetwork, log, state.clone())
                .await
                .unwrap();
            reopened.insert(group, (raft.clone(), state));
            rafts.push(raft);
        }
        let meta: LocalMetaKv<SimFs> = LocalMetaKv::from_openraft_groups(
            1,
            reopened,
            BTreeMap::from([(1, 2), (2, 3)]),
            BTreeMap::new(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        assert_eq!(meta.get_or_panic("/db/1/a").unwrap().value, b"/db/1/a");
        assert_eq!(meta.get_or_panic("/db/2/a").unwrap().value, b"/db/2/a");
        drop(meta);
        for raft in &rafts {
            raft.shutdown().await.unwrap();
        }
        drop(rafts);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A linearizable read must not leave a clone of the durable state machine
    /// in a task it spawned.
    ///
    /// The clone owns the group's directory lock, so a task that outlives the
    /// call keeps the lock and an in-process reopen fails with `WouldBlock`.
    /// That is the flake this test removes: the barrier task used to own the
    /// clone and the caller returned as soon as the reply was sent, before the
    /// executor had dropped the finished task.
    ///
    /// The runtime has one worker and a parked task occupies it, so the barrier
    /// task is never polled. The read times out and aborts it, and the clone
    /// count observed afterwards is therefore deterministic: a clone parked in
    /// the aborted task cannot have been dropped yet.
    #[test]
    fn a_timed_out_linearizable_read_does_not_retain_the_durable_lock() {
        let dir = std::env::temp_dir().join(format!(
            "yadb-linearizable-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let id = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [46; 16],
                group: 1,
                incarnation: 1,
            },
            1,
            vec![1],
        )
        .unwrap();
        let (state, meta, raft) = rt.block_on(async {
            let log = DurableOpenRaftLog::create(dir.join("log"), &id).unwrap();
            let state = DurableMetaStateMachine::create(dir.join("state"), &id).unwrap();
            let config = Arc::new(openraft::Config::default().validate().unwrap());
            let raft = YadbRaft::new(1, config, NoNetwork, log, state.clone())
                .await
                .unwrap();
            raft.initialize(BTreeMap::from([(1, BasicNode::new("one"))]))
                .await
                .unwrap();
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "leader")
                .await
                .unwrap();
            let meta: LocalMetaKv<SimFs> = LocalMetaKv::from_openraft(
                raft.clone(),
                state.clone(),
                BTreeMap::new(),
                rt.handle().clone(),
            );
            (state, meta, raft)
        });

        // Occupy the only worker so the barrier task cannot be polled at all.
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let blocker = rt.spawn(async move {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the worker must be parked before the read starts");

        let before = state.clone_count();
        let keys = vec!["/sys/probe".to_string()];
        let error = meta
            .try_linearizable_snapshot(&keys, &[])
            .expect_err("a barrier that cannot be polled must time out");
        assert!(
            matches!(error, MetaError::ControlFreshnessUnavailable(_)),
            "{error:?}"
        );
        assert_eq!(
            state.clone_count(),
            before,
            "the read must not leave a state-machine clone in the aborted task: \
             that clone keeps the group's directory lock"
        );

        release_tx.send(()).unwrap();
        rt.block_on(blocker).unwrap();
        drop(meta);
        rt.block_on(raft.shutdown()).unwrap();
        drop(raft);
        drop(state);

        // With no clone left behind, the same directory can be reopened.
        drop(DurableMetaStateMachine::open(dir.join("state"), &id).unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A durable single-node group whose log handle is kept aside, so a test can
    /// read back the entry an acknowledgement names.
    struct SingleNodeGroup {
        dir: std::path::PathBuf,
        raft: YadbRaft,
        state: DurableMetaStateMachine,
        log: DurableOpenRaftLog,
    }

    async fn single_node_group(cluster: u8) -> SingleNodeGroup {
        let dir = std::env::temp_dir().join(format!(
            "yadb-write-ack-{cluster}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let id = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [cluster; 16],
                group: 1,
                incarnation: 1,
            },
            1,
            vec![1],
        )
        .unwrap();
        let log = DurableOpenRaftLog::create(dir.join("log"), &id).unwrap();
        let state = DurableMetaStateMachine::create(dir.join("state"), &id).unwrap();
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let raft = YadbRaft::new(1, config, NoNetwork, log.clone(), state.clone())
            .await
            .unwrap();
        raft.initialize(BTreeMap::from([(1, BasicNode::new("one"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        SingleNodeGroup {
            dir,
            raft,
            state,
            log,
        }
    }

    /// The log entry at `index`, or a failure naming the index when the log does
    /// not hold it.
    async fn log_entry(log: &DurableOpenRaftLog, index: u64) -> openraft::Entry<C> {
        let mut log = log.clone();
        let mut entries = log
            .try_get_log_entries(index..index + 1)
            .await
            .unwrap_or_else(|error| panic!("reading log entry {index} failed: {error}"));
        assert_eq!(entries.len(), 1, "log index {index} must hold an entry");
        entries.remove(0)
    }

    fn put(key: &str, value: &[u8]) -> KvCommand {
        KvCommand::Put {
            key: key.into(),
            value: value.to_vec(),
        }
    }

    /// A committed write reports the log entry it occupies, and that entry is
    /// the one the node then applies.
    ///
    /// Inversion: report a constant or zero index and the payload assertions
    /// below fail; the second write's strictly greater index makes a constant
    /// impossible to miss.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_committed_write_reports_the_index_it_occupies() {
        let node = single_node_group(51).await;
        let client = OpenRaftClient::new(
            node.raft.clone(),
            node.state.clone(),
            tokio::runtime::Handle::current(),
        );

        let first = put("ack/one", b"v1");
        let ack = client.execute_with_ack(first.clone()).unwrap();
        assert!(matches!(ack.result, KvResult::PutOk { .. }));
        assert!(ack.index() > 0, "a committed entry has a real index");

        let entry = log_entry(&node.log, ack.index()).await;
        assert_eq!(entry.log_id.index, ack.index());
        assert_eq!(
            entry.log_id.leader_id.term,
            ack.term(),
            "the reported term must be the term of the entry at the reported index"
        );
        match &entry.payload {
            EntryPayload::Normal(command) => assert_eq!(command, &first),
            other => panic!("the acknowledged index holds a non-command payload: {other:?}"),
        }

        // The entry appears applied on the node that acknowledged it.
        // `RaftMetrics` is published asynchronously, so the acknowledgement can
        // land before the metrics watcher reports the apply; poll briefly rather
        // than reading one lagging sample.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let applied = loop {
            let applied = client.role_info().4;
            if applied >= ack.index() {
                break applied;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "applied index {applied} never reached the acknowledged index {}",
                ack.index()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert!(applied >= ack.index());
        let observation = loop {
            let observation = client.observation(1).await;
            if observation.commit_index.is_some_and(|i| i >= ack.index()) {
                break observation;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "commit index {:?} never reached the acknowledged index {} ({:?})",
                observation.commit_index,
                ack.index(),
                observation.commit_error
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert!(observation.commit_index.is_some_and(|i| i >= ack.index()));
        assert_eq!(
            node.state
                .try_snapshot(&["ack/one".into()], &[])
                .unwrap()
                .get("ack/one")
                .unwrap()
                .value,
            b"v1".to_vec()
        );

        // A later write occupies a later entry, so the report tracks the entry
        // rather than being a fixed value.
        let second = put("ack/two", b"v2");
        let next = client.execute_with_ack(second.clone()).unwrap();
        assert!(
            next.index() > ack.index(),
            "the second write must occupy a later entry: {} then {}",
            ack.index(),
            next.index()
        );
        let entry = log_entry(&node.log, next.index()).await;
        match &entry.payload {
            EntryPayload::Normal(command) => assert_eq!(command, &second),
            other => panic!("the second index holds a non-command payload: {other:?}"),
        }

        drop(client);
        node.raft.shutdown().await.unwrap();
        drop(node.raft);
        std::fs::remove_dir_all(node.dir).unwrap();
    }

    /// Each acknowledgement names the entry that holds *its* command, even when
    /// several writers are in flight.
    ///
    /// This is the inversion guard against reporting a freshly sampled index:
    /// an implementation that answers with `metrics.last_log_index` (or any
    /// other "current" index) names another writer's entry as soon as writes
    /// overlap, and the payload comparison fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlapping_writes_each_report_their_own_entry() {
        let node = single_node_group(52).await;
        let client = OpenRaftClient::new(
            node.raft.clone(),
            node.state.clone(),
            tokio::runtime::Handle::current(),
        );
        let mut writers = Vec::new();
        for writer in 0..2u8 {
            let client = client.clone();
            writers.push(std::thread::spawn(move || {
                (0..8u8)
                    .map(|n| {
                        let command = put(&format!("ack/{writer}/{n}"), &[writer, n]);
                        let ack = client.execute_with_ack(command.clone()).unwrap();
                        (command, ack)
                    })
                    .collect::<Vec<_>>()
            }));
        }
        let mut acks: Vec<(KvCommand, WriteAck)> = writers
            .into_iter()
            .flat_map(|writer| writer.join().unwrap())
            .collect();
        acks.sort_by_key(|(_, ack)| ack.index());

        let mut indices: Vec<u64> = acks.iter().map(|(_, ack)| ack.index()).collect();
        indices.dedup();
        assert_eq!(indices.len(), acks.len(), "every write owns a distinct entry");
        for (command, ack) in &acks {
            let entry = log_entry(&node.log, ack.index()).await;
            assert_eq!(entry.log_id.index, ack.index());
            assert_eq!(entry.log_id.leader_id.term, ack.term());
            match &entry.payload {
                EntryPayload::Normal(committed) => assert_eq!(
                    committed, command,
                    "index {} must hold the command its acknowledgement reported",
                    ack.index()
                ),
                other => panic!("index {} holds a non-command payload: {other:?}", ack.index()),
            }
        }
        let last = acks.last().unwrap().1.index();
        assert!(
            client.role_info().4 >= last,
            "every acknowledged entry must be applied"
        );

        drop(client);
        node.raft.shutdown().await.unwrap();
        drop(node.raft);
        std::fs::remove_dir_all(node.dir).unwrap();
    }

    /// A stored acknowledgement replays the original committed index.
    ///
    /// This is what an idempotent replay needs from this layer: the index is
    /// carried *with* the result, so a caller that stores the pair re-serves the
    /// entry that first committed, even after the log has moved past it. The
    /// stored value does not drift as later writes apply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stored_ack_replays_the_original_committed_index() {
        let node = single_node_group(53).await;
        let client = OpenRaftClient::new(
            node.raft.clone(),
            node.state.clone(),
            tokio::runtime::Handle::current(),
        );
        let command = put("ack/replayed", b"once");
        let original = client.execute_with_ack(command.clone()).unwrap();
        // The replay record a caller keeps holds both halves of the answer.
        let stored = original.clone();

        for n in 0..3u8 {
            client
                .execute_with_ack(put(&format!("ack/later/{n}"), &[n]))
                .unwrap();
        }
        assert!(
            client.role_info().4 > stored.index(),
            "later writes must move the applied index past the stored entry"
        );
        assert_eq!(stored.index(), original.index());
        assert_eq!(stored.result, original.result);

        let entry = log_entry(&node.log, stored.index()).await;
        assert_eq!(entry.log_id.leader_id.term, stored.term());
        match &entry.payload {
            EntryPayload::Normal(committed) => assert_eq!(committed, &command),
            other => panic!("the stored index holds a non-command payload: {other:?}"),
        }

        // Re-submitting through this layer is a new entry: OpenRaft appends one
        // entry per `client_write` and has no request-id dedup. A caller that
        // must answer with the original index replays the stored ack, which the
        // assertions above pin to the original entry.
        let resent = client.execute_with_ack(command).unwrap();
        assert!(
            resent.index() > original.index(),
            "an unkeyed re-submission occupies its own entry: {} then {}",
            original.index(),
            resent.index()
        );

        drop(client);
        node.raft.shutdown().await.unwrap();
        drop(node.raft);
        std::fs::remove_dir_all(node.dir).unwrap();
    }

    /// In-process peer routing: every RPC is delivered straight to the target
    /// `Raft` handle, so a three-node group needs no sockets.
    #[derive(Clone, Default)]
    struct InProcessNetwork {
        peers: Arc<std::sync::Mutex<BTreeMap<u32, YadbRaft>>>,
    }

    struct InProcessLink {
        target: u32,
        peers: Arc<std::sync::Mutex<BTreeMap<u32, YadbRaft>>>,
    }

    impl InProcessLink {
        fn peer(&self) -> Result<YadbRaft, Unreachable> {
            self.peers
                .lock()
                .unwrap()
                .get(&self.target)
                .cloned()
                .ok_or_else(|| Unreachable::new(&io::Error::other("peer is not registered")))
        }
    }

    impl RaftNetworkFactory<C> for InProcessNetwork {
        type Network = InProcessLink;

        async fn new_client(&mut self, target: u32, _: &BasicNode) -> InProcessLink {
            InProcessLink {
                target,
                peers: self.peers.clone(),
            }
        }
    }

    impl RaftNetwork<C> for InProcessLink {
        async fn append_entries(
            &mut self,
            rpc: AppendEntriesRequest<C>,
            _: RPCOption,
        ) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
            let peer = self.peer().map_err(RPCError::Unreachable)?;
            peer.append_entries(rpc)
                .await
                .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
        }

        async fn vote(
            &mut self,
            rpc: VoteRequest<u32>,
            _: RPCOption,
        ) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
            let peer = self.peer().map_err(RPCError::Unreachable)?;
            peer.vote(rpc)
                .await
                .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
        }

        async fn full_snapshot(
            &mut self,
            vote: Vote<u32>,
            snapshot: Snapshot<C>,
            _: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
            _: RPCOption,
        ) -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
            let peer = self.peer()?;
            peer.install_full_snapshot(vote, snapshot)
                .await
                .map_err(|fatal| StreamingError::from(Unreachable::new(&fatal)))
        }
    }

    /// A reported index is present on a follower after it converges.
    ///
    /// This is the library-level half of "acknowledged only once a follower
    /// holds the index": the acknowledgement names an entry, and a replica that
    /// did not serve the write ends up holding that entry at that index and
    /// term. It does not show the follower held it at acknowledgement time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_reported_index_reaches_a_follower() {
        let dir = std::env::temp_dir().join(format!(
            "yadb-write-ack-followers-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let peers: Arc<std::sync::Mutex<BTreeMap<u32, YadbRaft>>> = Arc::default();
        let mut rafts = BTreeMap::new();
        let mut states = BTreeMap::new();
        let mut logs = BTreeMap::new();
        for node in 1..=3u32 {
            let id = ReplicaIdentity::new(
                GroupIdentity {
                    cluster: [54; 16],
                    group: 1,
                    incarnation: 1,
                },
                node,
                vec![1, 2, 3],
            )
            .unwrap();
            let log = DurableOpenRaftLog::create(dir.join(format!("log-{node}")), &id).unwrap();
            let state =
                DurableMetaStateMachine::create(dir.join(format!("state-{node}")), &id).unwrap();
            let raft = YadbRaft::new(
                node,
                config.clone(),
                InProcessNetwork {
                    peers: peers.clone(),
                },
                log.clone(),
                state.clone(),
            )
            .await
            .unwrap();
            peers.lock().unwrap().insert(node, raft.clone());
            rafts.insert(node, raft);
            states.insert(node, state);
            logs.insert(node, log);
        }
        rafts[&1]
            .initialize(BTreeMap::from([
                (1, BasicNode::new("one")),
                (2, BasicNode::new("two")),
                (3, BasicNode::new("three")),
            ]))
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let leader = loop {
            let seen: Vec<Option<u32>> = rafts
                .values()
                .map(|raft| raft.metrics().borrow().current_leader)
                .collect();
            if let Some(id) = seen[0].filter(|id| {
                seen.iter().all(|leader| *leader == Some(*id))
                    && rafts[id].metrics().borrow().state == openraft::ServerState::Leader
            }) {
                break id;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the group did not settle on one leader: {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };

        let client = OpenRaftClient::new(
            rafts[&leader].clone(),
            states[&leader].clone(),
            tokio::runtime::Handle::current(),
        );
        let command = put("ack/follower", b"replicated");
        let ack = client.execute_with_ack(command.clone()).unwrap();
        assert!(ack.index() > 0);

        let follower = (1..=3).find(|node| *node != leader).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let applied = rafts[&follower]
                .metrics()
                .borrow()
                .last_applied
                .map_or(0, |id| id.index);
            if applied >= ack.index() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "follower {follower} did not apply index {} (applied {applied})",
                ack.index()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let entry = log_entry(&logs[&follower], ack.index()).await;
        assert_eq!(entry.log_id.index, ack.index());
        assert_eq!(
            entry.log_id.leader_id.term,
            ack.term(),
            "the follower's entry at the reported index must be the reported term"
        );
        match &entry.payload {
            EntryPayload::Normal(committed) => assert_eq!(committed, &command),
            other => panic!("the follower's index holds a non-command payload: {other:?}"),
        }
        assert_eq!(
            states[&follower]
                .try_snapshot(&["ack/follower".into()], &[])
                .unwrap()
                .get("ack/follower")
                .unwrap()
                .value,
            b"replicated".to_vec()
        );

        drop(client);
        for raft in rafts.values() {
            raft.shutdown().await.unwrap();
        }
        drop(rafts);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A bridge whose worker submits the command and then loses the reply, the
    /// ambiguity `await_reply` exists for. Unlike a bridge that refuses the
    /// command, this one really commits it, so the test can show that the write
    /// happened while no index was reported for it.
    fn reply_dropping_client(
        raft: YadbRaft,
        state: DurableMetaStateMachine,
        handle: tokio::runtime::Handle,
    ) -> OpenRaftClient {
        let (sender, receiver) = mpsc::sync_channel::<Request>(64);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = stopping.clone();
        let worker_handle = handle.clone();
        let worker_raft = raft.clone();
        let worker = std::thread::Builder::new()
            .name("yadb-openraft-lost-reply".into())
            .spawn(move || {
                while let Ok((cmd, reply)) = receiver.recv() {
                    if worker_stopping.load(Ordering::Acquire) {
                        break;
                    }
                    let raft = worker_raft.clone();
                    worker_handle.spawn(async move {
                        let _ = raft.client_write(cmd).await;
                        // The outcome is no longer reported: the reply path is
                        // gone after the write was already submitted.
                        drop(reply);
                    });
                }
            })
            .expect("could not start the lost-reply bridge");
        OpenRaftClient {
            raft,
            state,
            handle,
            bridge: Arc::new(Bridge {
                sender: Some(sender),
                worker: Some(worker),
                stopping,
            }),
        }
    }

    /// An ambiguous write reports no index and keeps the unknown outcome.
    ///
    /// Inversion: report an index for the lost reply and this test fails, because
    /// the call would return `Ok` where the assertion demands
    /// `OpenRaftWriteUnknown`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_ambiguous_write_reports_no_index() {
        let node = single_node_group(55).await;
        let client = reply_dropping_client(
            node.raft.clone(),
            node.state.clone(),
            tokio::runtime::Handle::current(),
        );
        let error = client
            .execute_with_ack(put("ack/ambiguous", b"maybe"))
            .expect_err("a lost reply is not a confirmed commit");
        assert!(
            matches!(error, MetaError::OpenRaftWriteUnknown(_)),
            "a lost reply must stay unknown, got {error:?}"
        );
        // The write itself did commit: the ambiguity is in the report, and no
        // index is available for it.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let present = node
                .state
                .try_snapshot(&["ack/ambiguous".into()], &[])
                .unwrap()
                .get("ack/ambiguous")
                .is_some();
            if present {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the ambiguous write must still have committed for this test to be meaningful"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        drop(client);
        node.raft.shutdown().await.unwrap();
        drop(node.raft);
        std::fs::remove_dir_all(node.dir).unwrap();
    }
}

impl OpenRaftClient {
    pub(crate) fn new(
        raft: YadbRaft,
        state: DurableMetaStateMachine,
        handle: tokio::runtime::Handle,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<Request>(64);
        let worker_raft = raft.clone();
        let worker_handle = handle.clone();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = stopping.clone();
        let worker = std::thread::Builder::new()
            .name("yadb-openraft-client".into())
            .spawn(move || {
                while let Ok((cmd, reply)) = receiver.recv() {
                    if worker_stopping.load(Ordering::Acquire) {
                        break;
                    }
                    // Dispatch without waiting. `client_write` is safe to call
                    // concurrently and that is the only way the runtime can
                    // batch entries: a bridge that waits for one write before
                    // dequeuing the next turns N concurrent writers into N
                    // serial round trips, and the state machine then persists
                    // one entry at a time instead of one committed batch.
                    let raft = worker_raft.clone();
                    worker_handle.spawn(async move {
                        let result = match raft.client_write(cmd).await {
                            // `ClientWriteResponse::log_id` is the entry OpenRaft
                            // applied for this request. It is the only value that
                            // names where the write actually lives: sampling the
                            // metrics afterwards would name whatever was last
                            // appended, not this write.
                            Ok(response) => Ok(WriteAck {
                                result: response.data,
                                committed: CommittedLogId {
                                    index: response.log_id.index,
                                    term: response.log_id.leader_id.term,
                                },
                            }),
                            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                                Err(MetaError::NotLeader {
                                    leader: f.leader_id,
                                })
                            }
                            // No confirmed commit, so no log id is reported.
                            Err(error) => Err(MetaError::OpenRaftWriteUnknown(error.to_string())),
                        };
                        let _ = reply.send(result);
                    });
                }
            })
            .expect("could not start OpenRaft metadata bridge");
        Self {
            raft,
            state,
            handle,
            bridge: Arc::new(Bridge {
                sender: Some(sender),
                worker: Some(worker),
                stopping,
            }),
        }
    }

    pub(crate) fn execute(&self, cmd: KvCommand) -> Result<KvResult, MetaError> {
        self.execute_with_ack(cmd).map(|ack| ack.result)
    }

    /// Submit one write and report the committed log entry it occupies.
    ///
    /// This is the sibling of [`Self::execute`] for callers that need the
    /// evidence, not only the result: a committed write returns the index and
    /// term of the entry that holds it, while every outcome that is not known
    /// to have committed returns an error and therefore no index.
    pub(crate) fn execute_with_ack(&self, cmd: KvCommand) -> Result<WriteAck, MetaError> {
        let runtime = tokio::runtime::Handle::try_current().ok();
        if runtime
            .as_ref()
            .is_some_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread)
        {
            return Err(MetaError::OpenRaftAsyncContext);
        }
        let (tx, rx) = mpsc::sync_channel(1);
        enqueue(
            self.bridge.sender.as_ref().expect("bridge sender exists"),
            (cmd, tx),
        )?;
        // A caller timeout never causes a retry. The queued request may still
        // execute; response loss is an unknown outcome.
        let wait = || await_reply(rx);
        if runtime.is_some() {
            tokio::task::block_in_place(wait)
        } else {
            wait()
        }
    }

    /// Perform a quorum-confirmed read barrier on this group and then copy the
    /// requested keys. Returns `NotLeader` when this replica cannot serve the
    /// barrier; callers must not fall back to an unbarriered local snapshot.
    pub(crate) fn linearizable_snapshot(
        &self,
        keys: &[String],
        prefixes: &[String],
    ) -> Result<MetaSnapshot, MetaError> {
        let runtime = tokio::runtime::Handle::try_current().ok();
        if runtime
            .as_ref()
            .is_some_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread)
        {
            return Err(MetaError::OpenRaftAsyncContext);
        }
        // The durable state machine owns the group's directory lock, and a clone
        // of it keeps that lock for as long as the clone lives. The clone
        // therefore stays on this stack: if the barrier task owned one, it would
        // outlive this call by however long the executor takes to drop a finished
        // (or aborted) task, and an in-process reopen of the group in that window
        // would fail with `WouldBlock`. Only the `Raft` handle, which does not
        // own the state machine, enters the task.
        let state = self.state.clone();
        let raft = self.raft.clone();
        let keys = keys.to_vec();
        let prefixes = prefixes.to_vec();
        let (tx, rx) = mpsc::sync_channel(1);
        let task = self.handle.spawn(async move {
            let barrier = match raft.ensure_linearizable().await {
                Ok(_) => Ok(()),
                Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
                    Err(MetaError::NotLeader {
                        leader: f.leader_id,
                    })
                }
                Err(error) => Err(MetaError::ControlFreshnessUnavailable(format!(
                    "linearizable control read failed: {error}"
                ))),
            };
            let _ = tx.send(barrier);
        });
        let wait = || {
            rx.recv_timeout(Duration::from_millis(1000))
                .unwrap_or_else(|_| {
                    task.abort();
                    Err(MetaError::ControlFreshnessUnavailable(
                        "linearizable control read timed out".into(),
                    ))
                })
        };
        let barrier = if runtime.is_some() {
            tokio::task::block_in_place(wait)
        } else {
            wait()
        };
        barrier?;
        state.try_snapshot(&keys, &prefixes)
    }

    pub(crate) fn watch_prefix(&self, prefix: String) -> mpsc::Receiver<WatchEvent> {
        self.state
            .try_watch_prefix(prefix)
            .expect("OpenRaft state unavailable; refusing to watch metadata")
    }

    pub(crate) fn role_info(&self) -> (u32, RoleState, u64, Option<u32>, u64) {
        let metrics = self.raft.metrics().borrow().clone();
        let role = match metrics.state {
            openraft::ServerState::Leader => RoleState::Leader,
            openraft::ServerState::Candidate => RoleState::Candidate,
            _ => RoleState::Follower,
        };
        (
            metrics.id,
            role,
            metrics.current_term,
            metrics.current_leader,
            metrics.last_applied.map_or(0, |id| id.index),
        )
    }

    /// One group's locally observed state, including the commit index.
    ///
    /// `RaftMetrics` has no commit index: `last_applied` says how far this node
    /// applied and `last_log_index` how much it appended, which is why a stalled
    /// commit was previously indistinguishable from a stalled append. The commit
    /// index lives in `RaftState`, reachable only through an in-process core
    /// query. That query is bounded here so a wedged core cannot hang a health
    /// response; a failed query leaves `commit_index` absent and the reason in
    /// `commit_error` rather than inventing a number.
    pub(crate) async fn observation(&self, group: u64) -> GroupObservation {
        let metrics = self.raft.metrics().borrow().clone();
        let role = match metrics.state {
            openraft::ServerState::Leader => RoleState::Leader,
            openraft::ServerState::Candidate => RoleState::Candidate,
            _ => RoleState::Follower,
        };
        let (commit_index, commit_error) = match tokio::time::timeout(
            COMMIT_OBSERVATION_BUDGET,
            self.raft
                .with_raft_state(|state| state.committed.map_or(0, |id| id.index)),
        )
        .await
        {
            Ok(Ok(index)) => (Some(index), None),
            Ok(Err(fatal)) => (None, Some(format!("commit index unavailable: {fatal}"))),
            Err(_) => (
                None,
                Some(format!(
                    "commit index unavailable: the Raft core did not answer within {} ms",
                    COMMIT_OBSERVATION_BUDGET.as_millis()
                )),
            ),
        };
        GroupObservation {
            group,
            node_id: metrics.id,
            role,
            term: metrics.current_term,
            leader_id: metrics.current_leader,
            applied_index: metrics.last_applied.map_or(0, |id| id.index),
            commit_index,
            commit_error,
        }
    }
}
