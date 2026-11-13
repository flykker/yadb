//! Experimental identity and envelope validation only; not a Raft runtime.
//!
//! This module does not persist identities, authenticate senders, validate Raft
//! log structure, or make `RaftCore` safe to expose to untrusted messages. It is
//! deliberately disconnected from the daemon and existing metadata traffic.

use serde::{Deserialize, Serialize};

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
/// rather than silently changing membership. Persistence is not implemented yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaIdentity {
    pub group: GroupIdentity,
    pub node: u32,
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
        if voters.is_empty() || voters.iter().any(|&id| id == 0)
            || voters.windows(2).any(|pair| pair[0] >= pair[1])
            || voters.binary_search(&node).is_err()
        {
            return Err(IdentityError::InvalidVoters);
        }
        Ok(Self { group, node, voters })
    }

    pub fn voters(&self) -> &[u32] { &self.voters }

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
}
