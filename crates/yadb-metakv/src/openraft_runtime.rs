//! OpenRaft integration boundary for the future Multi-Raft runtime.
//!
//! OpenRaft owns the consensus algorithm, elections, replication, quorum
//! commit, membership and snapshot protocol. YADB owns the group identity,
//! storage implementation, state machine and transport routing around it.
//!
//! The removed legacy `RaftCore` and the
//! `DeterministicGroupCore` prototype are not silently adapted to OpenRaft: their WAL and
//! RPC formats are incompatible with OpenRaft's storage contracts. OpenRaft is
//! the only clustered metadata runtime; its durable owners, registry and
//! authenticated transport are always compiled in.

use std::io::Cursor;

use crate::multiraft::{GroupIdentity, ReplicaIdentity};
use crate::{KvCommand, KvResult};

openraft::declare_raft_types!(
    /// YADB's application types used by one OpenRaft group.
    pub YadbRaftTypeConfig:
        D = KvCommand,
        R = KvResult,
        NodeId = u32,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Compile-time description of one OpenRaft group. Every Multi-Raft group will
/// own one independent `openraft::Raft<YadbRaftTypeConfig>` instance.
pub type YadbRaft = openraft::Raft<YadbRaftTypeConfig>;

/// Owner for one persistent replica. Network configuration and
/// migration remain separate responsibilities; daemon integration is opt-in.
pub mod persistent {
    use super::{ReplicaIdentity, YadbRaft, YadbRaftTypeConfig};
    use crate::{
        openraft_state_machine::{DurableMetaStateMachine, ImageRebuild},
        openraft_storage::{DurableOpenRaftLog, TailRepair},
    };
    use openraft::{error::Fatal, RaftNetworkFactory};
    use std::{
        io::{self, Read, Write},
        path::Path,
        sync::Arc,
    };
    use yadb_io::OsDirectoryLock;

    #[derive(Debug, thiserror::Error)]
    pub enum StartError {
        #[error("replica storage: {0}")]
        Storage(#[from] io::Error),
        #[error("raft startup: {0}")]
        Raft(#[from] Fatal<u32>),
    }

    /// Storage damage that opening a replica repaired or discarded.
    ///
    /// Every repair is silent by construction: a torn log tail or delta frame
    /// is truncated and a corrupt `state.json` is replaced with an empty image
    /// plus a full replay. The bytes are preserved, but an operator who is
    /// never told sees a healthy cluster and cannot distinguish a clean start
    /// from a recovered one. This report is what a caller surfaces; it is empty
    /// for a clean open.
    #[derive(Debug, Clone, Default)]
    pub struct RecoveryReport {
        /// A torn final log frame that `open` truncated away.
        pub repaired_tail: Option<TailRepair>,
        /// A torn final state-machine delta frame that `open` truncated away.
        pub repaired_delta: Option<TailRepair>,
        /// A corrupt `state.json` that was renamed aside and rebuilt empty.
        pub rebuilt_image: Option<ImageRebuild>,
    }

    impl RecoveryReport {
        /// Whether the replica opened without repairing or discarding anything.
        pub fn is_clean(&self) -> bool {
            self.repaired_tail.is_none()
                && self.repaired_delta.is_none()
                && self.rebuilt_image.is_none()
        }
    }

    pub struct PersistentGroup {
        raft: YadbRaft,
        state_machine: DurableMetaStateMachine,
        identity: ReplicaIdentity,
        recovered: RecoveryReport,
        _owner: OsDirectoryLock,
    }

    fn descriptor(identity: &ReplicaIdentity) -> io::Result<Vec<u8>> {
        serde_json::to_vec(&(1u32, identity.group(), identity.node(), identity.voters()))
            .map_err(io::Error::other)
    }

    fn verify_descriptor(dir: &Path, identity: &ReplicaIdentity) -> io::Result<()> {
        let expected = descriptor(identity)?;
        let mut actual = Vec::new();
        std::fs::File::open(dir.join("replica.json"))?
            .take(16 * 1024)
            .read_to_end(&mut actual)?;
        if actual != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "replica identity or bootstrap voters mismatch",
            ));
        }
        Ok(())
    }

    impl PersistentGroup {
        /// Creates a fresh replica. A failed or partial creation is never
        /// automatically retried or treated as an existing valid replica.
        pub async fn create<N: RaftNetworkFactory<YadbRaftTypeConfig>>(
            dir: impl AsRef<Path>,
            identity: &ReplicaIdentity,
            config: Arc<openraft::Config>,
            network: N,
        ) -> Result<Self, StartError> {
            let dir = dir.as_ref();
            // Bootstrap reserves a new replica directory exclusively. An
            // existing (possibly partially created) directory must never be
            // treated as an invitation to resume or reinitialize creation.
            let parent = dir
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            std::fs::create_dir(dir)?;
            std::fs::File::open(parent)?.sync_all()?;
            let owner = OsDirectoryLock::try_acquire(dir)?;
            let mut identity_file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(dir.join("replica.json"))?;
            identity_file.write_all(&descriptor(identity)?)?;
            identity_file.sync_all()?;
            std::fs::File::open(dir)?.sync_all()?;
            let log = DurableOpenRaftLog::create(dir.join("log"), identity)?;
            std::fs::File::open(dir)?.sync_all()?;
            let state = DurableMetaStateMachine::create(dir.join("state"), identity)?;
            std::fs::File::open(dir)?.sync_all()?;
            let raft = YadbRaft::new(identity.node(), config, network, log, state.clone()).await?;
            Ok(Self {
                raft,
                state_machine: state,
                identity: identity.clone(),
                recovered: RecoveryReport::default(),
                _owner: owner,
            })
        }

        /// Opens existing storage only; missing or invalid files fail closed.
        ///
        /// A corrupt state-machine image is rebuilt from an empty image only
        /// when the log still holds every entry. The log is opened first so its
        /// coverage can decide that; if anything was purged or the log starts
        /// past the first index, the state machine cannot be reconstructed and
        /// `open` fails as before.
        pub async fn open<N: RaftNetworkFactory<YadbRaftTypeConfig>>(
            dir: impl AsRef<Path>,
            identity: &ReplicaIdentity,
            config: Arc<openraft::Config>,
            network: N,
        ) -> Result<Self, StartError> {
            let dir = dir.as_ref();
            let owner = OsDirectoryLock::try_acquire(dir)?;
            verify_descriptor(dir, identity)?;
            let log = DurableOpenRaftLog::open(dir.join("log"), identity)?;
            let repaired_tail = log.repaired_tail();
            let state = DurableMetaStateMachine::open_or_rebuild(
                dir.join("state"),
                identity,
                log.coverage(),
            )?;
            let recovered = RecoveryReport {
                repaired_tail,
                repaired_delta: state.repaired_delta(),
                rebuilt_image: state.rebuilt_image(),
            };
            let raft = YadbRaft::new(identity.node(), config, network, log, state.clone()).await?;
            Ok(Self {
                raft,
                state_machine: state,
                identity: identity.clone(),
                recovered,
                _owner: owner,
            })
        }

        pub fn raft(&self) -> &YadbRaft {
            &self.raft
        }
        /// The same locally applied state machine used by this Raft instance.
        pub fn state_machine(&self) -> &DurableMetaStateMachine {
            &self.state_machine
        }
        /// Storage damage repaired while this replica was opened.
        ///
        /// Empty for a clean open. A caller must report a non-empty result:
        /// the repairs themselves are silent and the evidence files are only
        /// discoverable if the operator is told they exist.
        pub fn recovery(&self) -> &RecoveryReport {
            &self.recovered
        }
        pub fn identity(&self) -> &ReplicaIdentity {
            &self.identity
        }

        /// Stop the core without releasing directory ownership. Cancellation
        /// retains ownership as long as the caller retains this group. After a
        /// successful shutdown, drop the group to release its directory lock.
        pub async fn shutdown(&self) -> io::Result<()> {
            self.raft.shutdown().await.map_err(io::Error::other)
        }
    }
}

/// In-process group ownership. This does not route network
/// traffic, enforce per-group memory/disk limits, or provide daemon lifecycle.
pub mod registry {
    use super::{
        persistent::{PersistentGroup, StartError},
        ReplicaIdentity, YadbRaft, YadbRaftTypeConfig,
    };
    use openraft::RaftNetworkFactory;
    use std::{
        collections::BTreeMap,
        io,
        path::{Path, PathBuf},
        sync::Arc,
    };

    type Key = ([u8; 16], u64, u64, u32);

    #[derive(Debug, thiserror::Error)]
    pub enum RegistryError {
        #[error("group is already registered")]
        Duplicate,
        #[error("group registry is full")]
        Full,
        #[error("group is not registered")]
        NotFound,
        #[error(transparent)]
        Start(#[from] StartError),
    }

    pub struct GroupRegistry {
        root: PathBuf,
        max_groups: usize,
        groups: BTreeMap<Key, PersistentGroup>,
    }

    impl GroupRegistry {
        /// The root must already exist and be durably provisioned by the operator.
        pub fn new(root: impl AsRef<Path>, max_groups: usize) -> io::Result<Self> {
            if max_groups == 0 || !root.as_ref().is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing root and nonzero group limit required",
                ));
            }
            Ok(Self {
                root: root.as_ref().to_path_buf(),
                max_groups,
                groups: BTreeMap::new(),
            })
        }

        fn key(identity: &ReplicaIdentity) -> Key {
            let group = identity.group();
            (
                group.cluster,
                group.group,
                group.incarnation,
                identity.node(),
            )
        }

        fn path(&self, identity: &ReplicaIdentity) -> PathBuf {
            let cluster = identity
                .group()
                .cluster
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            self.root.join(format!(
                "{cluster}-{}-{}-{}",
                identity.group().group,
                identity.group().incarnation,
                identity.node()
            ))
        }

        async fn start<N: RaftNetworkFactory<YadbRaftTypeConfig>>(
            &mut self,
            identity: &ReplicaIdentity,
            config: Arc<openraft::Config>,
            network: N,
            create: bool,
        ) -> Result<(), RegistryError> {
            let key = Self::key(identity);
            if self.groups.contains_key(&key) {
                return Err(RegistryError::Duplicate);
            }
            if self.groups.len() >= self.max_groups {
                return Err(RegistryError::Full);
            }
            let path = self.path(identity);
            let group = if create {
                PersistentGroup::create(path, identity, config, network).await?
            } else {
                PersistentGroup::open(path, identity, config, network).await?
            };
            self.groups.insert(key, group);
            Ok(())
        }

        pub async fn create<N: RaftNetworkFactory<YadbRaftTypeConfig>>(
            &mut self,
            identity: &ReplicaIdentity,
            config: Arc<openraft::Config>,
            network: N,
        ) -> Result<(), RegistryError> {
            self.start(identity, config, network, true).await
        }

        pub async fn open<N: RaftNetworkFactory<YadbRaftTypeConfig>>(
            &mut self,
            identity: &ReplicaIdentity,
            config: Arc<openraft::Config>,
            network: N,
        ) -> Result<(), RegistryError> {
            self.start(identity, config, network, false).await
        }

        pub fn get(&self, identity: &ReplicaIdentity) -> Option<&YadbRaft> {
            self.groups
                .get(&Self::key(identity))
                .filter(|group| group.identity() == identity)
                .map(PersistentGroup::raft)
        }

        pub async fn shutdown(&mut self, identity: &ReplicaIdentity) -> Result<(), io::Error> {
            // Retain ownership across await: cancelling this future must not
            // free a registry slot while the Raft core is still shutting down.
            let key = Self::key(identity);
            let group = self
                .groups
                .get(&key)
                .filter(|group| group.identity() == identity)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "group is not registered")
                })?;
            group.shutdown().await?;
            self.groups.remove(&key);
            Ok(())
        }
    }
}

/// Configuration boundary for one future OpenRaft group.
///
/// This validates YADB's stable group identity and fixed voters before an
/// OpenRaft instance is constructed. It does not create a node or persist
/// anything; construction belongs to the durable runtime step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenRaftGroupSpec {
    pub group: GroupIdentity,
    pub node_id: u32,
    pub voters: Vec<u32>,
}

impl OpenRaftGroupSpec {
    pub fn new(identity: &ReplicaIdentity) -> Self {
        Self {
            group: identity.group().clone(),
            node_id: identity.node(),
            voters: identity.voters().to_vec(),
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        ReplicaIdentity::new(self.group.clone(), self.node_id, self.voters.clone())
            .map(|_| ())
            .map_err(|_| "invalid OpenRaft group membership")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::RaftTypeConfig;

    #[test]
    fn openraft_type_config_uses_yadb_commands_and_node_ids() {
        fn assert_types<C: RaftTypeConfig<D = KvCommand, R = KvResult, NodeId = u32>>() {}
        assert_types::<YadbRaftTypeConfig>();
        let _: Option<YadbRaft> = None;
    }

    #[test]
    fn openraft_group_spec_preserves_fixed_yadb_identity() {
        let identity = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [7; 16],
                group: 11,
                incarnation: 3,
            },
            2,
            vec![1, 2, 3],
        )
        .unwrap();
        let spec = OpenRaftGroupSpec::new(&identity);
        assert_eq!(spec.group, *identity.group());
        assert_eq!(spec.node_id, 2);
        assert_eq!(spec.voters, vec![1, 2, 3]);
        spec.validate().unwrap();
    }
}

#[cfg(test)]
#[path = "openraft_test_gate.rs"]
mod test_gate;

#[cfg(test)]
#[path = "openraft_test.rs"]
mod runtime_tests;

#[cfg(test)]
#[path = "openraft_net_test.rs"]
mod net_tests;

#[cfg(test)]
#[path = "openraft_process_test.rs"]
mod process_tests;

#[cfg(test)]
#[path = "openraft_multigroup_test.rs"]
mod multigroup_tests;
