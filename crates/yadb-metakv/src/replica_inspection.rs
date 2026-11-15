//! Read-only, local checks of one replica; not a consensus or restore proof.
use crate::{
    multiraft::{GroupIdentity, ReplicaIdentity},
    openraft_state_machine::inspect_image,
    openraft_storage::inspect_log,
};
use openraft::LogId;
use serde::Serialize;
use std::{
    io::{self, Read},
    path::Path,
};

#[derive(Debug, Serialize)]
pub struct MembershipDifference {
    pub descriptor_voters: Vec<u32>,
    pub stored_voters: Vec<u32>,
}

#[derive(Debug, Serialize)]
pub struct ReplicaVerdict {
    pub group: Option<GroupIdentity>,
    pub node: Option<u32>,
    pub first_index: Option<u64>,
    pub last_index: Option<u64>,
    pub last_purged: Option<LogId<u32>>,
    pub applied: Option<LogId<u32>>,
    pub snapshot: Option<LogId<u32>>,
    pub entry_count: u64,
    pub torn_tail: bool,
    pub restorable: bool,
    pub reason: String,
    pub membership_differs_from_descriptor: Option<MembershipDifference>,
}

/// Read the same canonical descriptor accepted by the persistent opener.
pub fn read_replica_identity(dir: impl AsRef<Path>) -> io::Result<ReplicaIdentity> {
    let mut bytes = Vec::new();
    std::fs::File::open(dir.as_ref().join("replica.json"))?
        .take(16385)
        .read_to_end(&mut bytes)?;
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "invalid replica descriptor");
    if bytes.len() > 16384 {
        return Err(bad());
    }
    let (version, group, node, voters): (u32, GroupIdentity, u32, Vec<u32>) =
        serde_json::from_slice(&bytes).map_err(|_| bad())?;
    if version != 1 {
        return Err(bad());
    }
    let identity = ReplicaIdentity::new(group, node, voters).map_err(|_| bad())?;
    let canonical =
        serde_json::to_vec(&(1u32, identity.group(), identity.node(), identity.voters()))
            .map_err(io::Error::other)?;
    if bytes != canonical {
        return Err(bad());
    }
    Ok(identity)
}

/// Verify a stopped, stable replica. The caller must ensure the files do not change.
pub fn verify_replica(dir: impl AsRef<Path>, identity: &ReplicaIdentity) -> ReplicaVerdict {
    let dir = dir.as_ref();
    let mut v = ReplicaVerdict {
        group: None,
        node: None,
        first_index: None,
        last_index: None,
        last_purged: None,
        applied: None,
        snapshot: None,
        entry_count: 0,
        torn_tail: false,
        restorable: false,
        reason: String::new(),
        membership_differs_from_descriptor: None,
    };
    let result = (|| -> io::Result<()> {
        let descriptor = read_replica_identity(dir)?;
        v.group = Some(descriptor.group().clone());
        v.node = Some(descriptor.node());
        if &descriptor != identity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "replica descriptor identity or bootstrap voters mismatch",
            ));
        }
        let log = inspect_log(dir.join("log"), identity)
            .map_err(|e| io::Error::new(e.kind(), format!("log inspection: {e}")))?;
        v.first_index = log.first_index;
        v.last_index = log.last_index;
        v.last_purged = log.last_purged;
        v.entry_count = log.entry_count;
        v.torn_tail = log.torn_tail;
        let image = inspect_image(dir.join("state"), identity)
            .map_err(|e| io::Error::new(e.kind(), format!("image inspection: {e}")))?;
        v.applied = image.applied;
        v.snapshot = image.snapshot;
        if image.voters != descriptor.voters() {
            v.membership_differs_from_descriptor = Some(MembershipDifference {
                descriptor_voters: descriptor.voters().to_vec(),
                stored_voters: image.voters,
            });
        }
        if let Some(applied) = v.applied {
            if v.entry_count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "applied log id with empty log: nothing to replay",
                ));
            }
            let covered = |boundary: Option<LogId<u32>>| {
                boundary.is_some_and(|b| applied.index < b.index || applied == b)
            };
            if log.entries.get(&applied.index) != Some(&applied)
                && !covered(v.snapshot)
                && !covered(v.last_purged)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "applied log id absent from log and not covered by snapshot or last_purged",
                ));
            }
        }
        Ok(())
    })();
    match result {
        Ok(()) => v.restorable = true,
        Err(e) => v.reason = e.to_string(),
    }
    v
}
