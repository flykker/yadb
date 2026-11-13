//! OpenRaft integration boundary for the future Multi-Raft runtime.
//!
//! OpenRaft owns the consensus algorithm, elections, replication, quorum
//! commit, membership and snapshot protocol. YADB owns the group identity,
//! storage implementation, state machine and transport routing around it.
//!
//! This module intentionally exposes only the type configuration in the first
//! increment. The existing legacy `RaftCore` and the experimental
//! `DeterministicGroupCore` are not silently adapted to OpenRaft: their WAL and
//! RPC formats are incompatible with OpenRaft's storage contracts. A durable
//! OpenRaft store and versioned group transport are separate follow-up steps.

use std::io::Cursor;

use crate::{KvCommand, KvResult};
use crate::multiraft::{GroupIdentity, ReplicaIdentity};

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
            GroupIdentity { cluster: [7; 16], group: 11, incarnation: 3 },
            2,
            vec![1, 2, 3],
        ).unwrap();
        let spec = OpenRaftGroupSpec::new(&identity);
        assert_eq!(spec.group, *identity.group());
        assert_eq!(spec.node_id, 2);
        assert_eq!(spec.voters, vec![1, 2, 3]);
        spec.validate().unwrap();
    }
}
