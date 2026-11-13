//! Experimental identity, descriptor and envelope validation; not a Raft runtime.
//!
//! The descriptor is not yet bound to a WAL and its caller must provide exclusive
//! ownership. This module does not authenticate senders, validate Raft log structure,
//! or make `RaftCore` safe to expose to untrusted messages. It is deliberately
//! disconnected from the daemon and existing metadata traffic.

use serde::{Deserialize, Serialize};
use yadb_io::{StorageFs, VirtualFile};

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
