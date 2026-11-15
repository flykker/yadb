//! Volatile in-process tests only. Never use this store or network for real data.
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::error::{
    Fatal, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader,
    RaftNetwork, RaftNetworkFactory, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};

use super::{YadbRaft, YadbRaftTypeConfig as C};
use crate::openraft_runtime::persistent::PersistentGroup;
use crate::openraft_state_machine::DurableMetaStateMachine;
use crate::openraft_storage::DurableOpenRaftLog;
use crate::{KvCommand, KvResult, MetaStateMachine};

#[derive(Default)]
struct Data {
    log: BTreeMap<u64, Entry<C>>,
    vote: Option<Vote<u32>>,
    purged: Option<LogId<u32>>,
    applied: Option<LogId<u32>>,
    membership: StoredMembership<u32, BasicNode>,
    state: MetaStateMachine,
}

#[derive(Clone, Default)]
struct Memory(Arc<Mutex<Data>>);

fn unsupported() -> StorageError<u32> {
    StorageError::from_io_error(
        ErrorSubject::Store,
        ErrorVerb::Write,
        io::Error::new(
            io::ErrorKind::Unsupported,
            "snapshots disabled in volatile smoke test",
        ),
    )
}

impl RaftLogReader<C> for Memory {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry<C>>, StorageError<u32>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<C> for Memory {
    type LogReader = Memory;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u32>> {
        let data = self.0.lock().unwrap();
        Ok(LogState {
            last_purged_log_id: data.purged,
            last_log_id: data
                .log
                .last_key_value()
                .map(|(_, e)| e.log_id)
                .or(data.purged),
        })
    }
    async fn get_log_reader(&mut self) -> Memory {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &Vote<u32>) -> Result<(), StorageError<u32>> {
        self.0.lock().unwrap().vote = Some(*vote);
        Ok(())
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u32>>, StorageError<u32>> {
        Ok(self.0.lock().unwrap().vote)
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<u32>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let mut data = self.0.lock().unwrap();
        for entry in entries {
            data.log.insert(entry.log_id.index, entry);
        }
        drop(data);
        // Deliberately not durable: this implementation exists only under cfg(test).
        callback.log_io_completed(Ok(()));
        Ok(())
    }
    async fn truncate(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
        self.0.lock().unwrap().log.split_off(&log_id.index);
        Ok(())
    }
    async fn purge(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
        let mut data = self.0.lock().unwrap();
        data.log.retain(|index, _| *index > log_id.index);
        data.purged = Some(log_id);
        Ok(())
    }
}

impl RaftSnapshotBuilder<C> for Memory {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<u32>> {
        Err(unsupported())
    }
}

impl RaftStateMachine<C> for Memory {
    type SnapshotBuilder = Memory;
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u32>>, StoredMembership<u32, BasicNode>), StorageError<u32>> {
        let data = self.0.lock().unwrap();
        Ok((data.applied, data.membership.clone()))
    }
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<KvResult>, StorageError<u32>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let mut data = self.0.lock().unwrap();
        let mut results = Vec::new();
        for entry in entries {
            data.applied = Some(entry.log_id);
            results.push(match entry.payload {
                EntryPayload::Normal(cmd) => data.state.apply(cmd),
                EntryPayload::Membership(m) => {
                    data.membership = StoredMembership::new(Some(entry.log_id), m);
                    KvResult::TxnOk {
                        revision: data.state.revision(),
                        versions: BTreeMap::new(),
                    }
                }
                EntryPayload::Blank => KvResult::TxnOk {
                    revision: data.state.revision(),
                    versions: BTreeMap::new(),
                },
            });
        }
        Ok(results)
    }
    async fn get_snapshot_builder(&mut self) -> Memory {
        self.clone()
    }
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u32>> {
        Err(unsupported())
    }
    async fn install_snapshot(
        &mut self,
        _: &SnapshotMeta<u32, BasicNode>,
        _: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u32>> {
        Err(unsupported())
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<u32>> {
        Ok(None)
    }
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
        Err(Unreachable::new(&io::Error::new(io::ErrorKind::NotConnected, "singleton")).into())
    }
    async fn vote(
        &mut self,
        _: VoteRequest<u32>,
        _: RPCOption,
    ) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
        Err(Unreachable::new(&io::Error::new(io::ErrorKind::NotConnected, "singleton")).into())
    }
    async fn full_snapshot(
        &mut self,
        _: Vote<u32>,
        _: Snapshot<C>,
        _: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _: RPCOption,
    ) -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
        Err(Unreachable::new(&io::Error::new(io::ErrorKind::NotConnected, "singleton")).into())
    }
}

#[tokio::test]
async fn openraft_singleton_elects_and_applies_yadb_command_in_memory() {
    let memory = Memory::default();
    let config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = YadbRaft::new(1, config, NoNetwork, memory.clone(), memory.clone())
        .await
        .unwrap();
    raft.initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "singleton leader")
        .await
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        raft.client_write(KvCommand::Put {
            key: "key".into(),
            value: b"value".to_vec(),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(response.data, KvResult::PutOk { .. }));
    assert_eq!(
        memory.0.lock().unwrap().state.get("key").unwrap().value,
        b"value"
    );
    tokio::time::timeout(Duration::from_secs(5), raft.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn openraft_singleton_uses_durable_file_log_across_reopen() {
    let dir = std::env::temp_dir().join(format!(
        "yadb-openraft-smoke-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [1; 16],
            group: 1,
            incarnation: 1,
        },
        1,
        vec![1],
    )
    .unwrap();
    let log = DurableOpenRaftLog::create(&dir, &identity).unwrap();
    let state = Memory::default();
    let config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = YadbRaft::new(1, config, NoNetwork, log.clone(), state.clone())
        .await
        .unwrap();
    raft.initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "durable singleton leader")
        .await
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        raft.client_write(KvCommand::Put {
            key: "durable".into(),
            value: b"value".to_vec(),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(response.data, KvResult::PutOk { .. }));
    tokio::time::timeout(Duration::from_secs(5), raft.shutdown())
        .await
        .unwrap()
        .unwrap();
    drop(raft);
    drop(log);

    let reopened = DurableOpenRaftLog::open(&dir, &identity).unwrap();
    let mut reader = reopened.clone();
    let entries = reader.try_get_log_entries(0..u64::MAX).await.unwrap();
    assert!(entries.iter().any(|entry| matches!(
        &entry.payload,
        EntryPayload::Normal(KvCommand::Put { key, value }) if key == "durable" && value == b"value"
    )));
    drop(reopened);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn openraft_singleton_restarts_with_durable_log_and_state_machine() {
    let root = std::env::temp_dir().join(format!(
        "yadb-openraft-durable-sm-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let log_dir = root.join("log");
    let state_dir = root.join("state");
    let identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [3; 16],
            group: 7,
            incarnation: 1,
        },
        1,
        vec![1],
    )
    .unwrap();
    let log = DurableOpenRaftLog::create(&log_dir, &identity).unwrap();
    let state = DurableMetaStateMachine::create(&state_dir, &identity).unwrap();
    let config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = YadbRaft::new(1, config, NoNetwork, log.clone(), state.clone())
        .await
        .unwrap();
    raft.initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "durable state-machine leader")
        .await
        .unwrap();
    let response = raft
        .client_write(KvCommand::Put {
            key: "restart".into(),
            value: b"yes".to_vec(),
        })
        .await
        .unwrap();
    assert!(matches!(response.data, KvResult::PutOk { .. }));
    raft.shutdown().await.unwrap();
    drop(raft);
    drop(log);
    drop(state);

    let reopened_log = DurableOpenRaftLog::open(&log_dir, &identity).unwrap();
    let reopened_state = DurableMetaStateMachine::open(&state_dir, &identity).unwrap();
    assert_eq!(
        reopened_state.snapshot().get("restart").unwrap().value,
        b"yes"
    );
    let mut reader = reopened_log.clone();
    assert!(reader.try_get_log_entries(0..u64::MAX).await.unwrap().iter().any(|entry| {
        matches!(&entry.payload, EntryPayload::Normal(KvCommand::Put { key, .. }) if key == "restart")
    }));
    drop(reopened_log);
    drop(reopened_state);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn persistent_group_lifecycle_initializes_shuts_down_and_reopens() {
    let root = std::env::temp_dir().join(format!(
        "yadb-openraft-owner-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [8; 16],
            group: 12,
            incarnation: 1,
        },
        1,
        vec![1],
    )
    .unwrap();
    let config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let group = PersistentGroup::create(&root, &identity, config.clone(), NoNetwork)
        .await
        .unwrap();
    assert!(
        PersistentGroup::open(&root, &identity, config.clone(), NoNetwork)
            .await
            .is_err()
    );
    assert!(
        PersistentGroup::create(&root, &identity, config.clone(), NoNetwork)
            .await
            .is_err()
    );
    group
        .raft()
        .initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    group
        .raft()
        .client_write(KvCommand::Put {
            key: "owner".into(),
            value: b"recovered".to_vec(),
        })
        .await
        .unwrap();
    group.shutdown().await.unwrap();
    drop(group);
    let wrong_identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [8; 16],
            group: 12,
            incarnation: 2,
        },
        1,
        vec![1],
    )
    .unwrap();
    assert!(
        PersistentGroup::open(&root, &wrong_identity, config.clone(), NoNetwork)
            .await
            .is_err()
    );
    let wrong_voters =
        crate::multiraft::ReplicaIdentity::new(identity.group().clone(), 1, vec![1, 2, 3]).unwrap();
    assert!(
        matches!(PersistentGroup::open(&root, &wrong_voters, config.clone(), NoNetwork).await,
        Err(super::persistent::StartError::Storage(e)) if e.kind() == io::ErrorKind::InvalidData)
    );
    let group = PersistentGroup::open(&root, &identity, config.clone(), NoNetwork)
        .await
        .unwrap();
    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "reopened leader")
        .await
        .unwrap();
    group
        .raft()
        .client_write(KvCommand::Put {
            key: "after".into(),
            value: b"restart".to_vec(),
        })
        .await
        .unwrap();
    group.shutdown().await.unwrap();
    drop(group);
    let state = DurableMetaStateMachine::open(root.join("state"), &identity).unwrap();
    assert_eq!(state.snapshot().get("owner").unwrap().value, b"recovered");
    assert_eq!(state.snapshot().get("after").unwrap().value, b"restart");
    drop(state);
    let _ = std::fs::remove_dir_all(root);
}

// End to end: the unit tests prove the state-machine half in isolation, but
// only a real OpenRaft restart proves that `applied: None` plus an unpurged
// log actually replays the whole history into the rebuilt machine. The
// singleton writes one key, loses its image to bit rot, and must serve both
// that key and a new one afterwards.
#[tokio::test]
async fn corrupt_state_image_is_rebuilt_and_replayed_by_openraft() {
    let root = std::env::temp_dir().join(format!(
        "yadb-openraft-rebuild-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [6; 16],
            group: 21,
            incarnation: 1,
        },
        1,
        vec![1],
    )
    .unwrap();
    let config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let group = PersistentGroup::create(&root, &identity, config.clone(), NoNetwork)
        .await
        .unwrap();
    group
        .raft()
        .initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    group
        .raft()
        .client_write(KvCommand::Put {
            key: "before".into(),
            value: b"durable".to_vec(),
        })
        .await
        .unwrap();
    group.shutdown().await.unwrap();
    drop(group);

    let image = root.join("state").join("state.json");
    let mut bytes = std::fs::read(&image).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&image, &bytes).unwrap();
    assert!(DurableMetaStateMachine::open(root.join("state"), &identity).is_err());

    let group = PersistentGroup::open(&root, &identity, config.clone(), NoNetwork)
        .await
        .unwrap();
    assert!(group.state_machine().rebuilt_image().is_some());
    assert_eq!(
        std::fs::read(root.join("state").join("state.json.corrupt")).unwrap(),
        bytes
    );
    wait_durable_value(group.state_machine(), "before", b"durable").await;
    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "rebuilt leader")
        .await
        .unwrap();
    group
        .raft()
        .client_write(KvCommand::Put {
            key: "after".into(),
            value: b"rebuild".to_vec(),
        })
        .await
        .unwrap();
    group.shutdown().await.unwrap();
    drop(group);

    // The rebuilt image is an ordinary valid image again, and a later open
    // reports no rebuild.
    let state = DurableMetaStateMachine::open(root.join("state"), &identity).unwrap();
    assert_eq!(state.rebuilt_image(), None);
    assert_eq!(state.snapshot().get("before").unwrap().value, b"durable");
    assert_eq!(state.snapshot().get("after").unwrap().value, b"rebuild");
    drop(state);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn registry_isolates_groups_and_enforces_capacity() {
    use super::registry::{GroupRegistry, RegistryError};
    let root = std::env::temp_dir().join(format!(
        "yadb-openraft-registry-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let identity = |group| {
        crate::multiraft::ReplicaIdentity::new(
            crate::multiraft::GroupIdentity {
                cluster: [9; 16],
                group,
                incarnation: 1,
            },
            1,
            vec![1],
        )
        .unwrap()
    };
    let a = identity(1);
    let b = identity(2);
    let c = identity(3);
    let config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let mut registry = GroupRegistry::new(&root, 2).unwrap();
    registry
        .create(&a, config.clone(), NoNetwork)
        .await
        .unwrap();
    let wrong_voters =
        crate::multiraft::ReplicaIdentity::new(a.group().clone(), 1, vec![1, 2, 3]).unwrap();
    assert!(registry.get(&wrong_voters).is_none());
    assert_eq!(
        registry.shutdown(&wrong_voters).await.unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert!(matches!(
        registry
            .create(&wrong_voters, config.clone(), NoNetwork)
            .await,
        Err(RegistryError::Duplicate)
    ));
    assert!(registry.get(&a).is_some());
    assert!(matches!(
        registry.create(&a, config.clone(), NoNetwork).await,
        Err(RegistryError::Duplicate)
    ));
    registry
        .create(&b, config.clone(), NoNetwork)
        .await
        .unwrap();
    assert!(matches!(
        registry.create(&c, config.clone(), NoNetwork).await,
        Err(RegistryError::Full)
    ));
    for id in [&a, &b] {
        let raft = registry.get(id).unwrap();
        raft.initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "registry leader")
            .await
            .unwrap();
    }
    registry
        .get(&a)
        .unwrap()
        .client_write(KvCommand::Put {
            key: "a".into(),
            value: b"one".to_vec(),
        })
        .await
        .unwrap();
    registry
        .get(&b)
        .unwrap()
        .client_write(KvCommand::Put {
            key: "b".into(),
            value: b"two".to_vec(),
        })
        .await
        .unwrap();
    registry.shutdown(&a).await.unwrap();
    assert!(registry.get(&a).is_none());
    registry
        .get(&b)
        .unwrap()
        .client_write(KvCommand::Put {
            key: "still-running".into(),
            value: vec![],
        })
        .await
        .unwrap();
    registry.open(&a, config.clone(), NoNetwork).await.unwrap();
    registry
        .get(&a)
        .unwrap()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "reopened registry leader")
        .await
        .unwrap();
    registry.shutdown(&a).await.unwrap();
    registry.shutdown(&b).await.unwrap();
    drop(registry);
    let dirs: Vec<_> = std::fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert_eq!(dirs.len(), 2);
    for dir in dirs {
        let state_a = DurableMetaStateMachine::open(dir.join("state"), &a);
        let state_b = DurableMetaStateMachine::open(dir.join("state"), &b);
        match (state_a, state_b) {
            (Ok(state), Err(_)) => {
                assert!(state.snapshot().get("a").is_some());
                assert!(state.snapshot().get("b").is_none());
            }
            (Err(_), Ok(state)) => {
                assert!(state.snapshot().get("b").is_some());
                assert!(state.snapshot().get("a").is_none());
            }
            _ => panic!("each state directory must belong to exactly one group"),
        }
    }
    let _ = std::fs::remove_dir_all(root);
}

// Poll once on the current-thread runtime without yielding to the core. This
// starts shutdown but prevents the core task from completing before cancellation.
fn poll_shutdown_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(
        future.poll(&mut cx).is_pending(),
        "shutdown unexpectedly completed before cancellation"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn persistent_shutdown_cancellation_retains_owner_and_stale_handle() {
    let root = std::env::var_os("YADB_TEST_TMPDIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!(
            "yadb-owner-cancel-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [44; 16],
            group: 1,
            incarnation: 1,
        },
        1,
        vec![1],
    )
    .unwrap();
    let config = Arc::new(openraft::Config::default().validate().unwrap());
    let group = PersistentGroup::create(&root, &identity, config.clone(), NoNetwork)
        .await
        .unwrap();
    let stale = group.raft().clone();
    assert!(
        PersistentGroup::open(&root, &identity, config.clone(), NoNetwork)
            .await
            .is_err()
    );
    {
        let mut future = Box::pin(group.shutdown());
        poll_shutdown_once(future.as_mut());
    }
    assert!(
        PersistentGroup::open(&root, &identity, config.clone(), NoNetwork)
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(5), group.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(
        PersistentGroup::open(&root, &identity, config.clone(), NoNetwork)
            .await
            .is_err()
    );
    drop(group);
    // A cloned Raft/status handle does not retain the completed core's stores.
    let reopened = PersistentGroup::open(&root, &identity, config.clone(), NoNetwork)
        .await
        .unwrap();
    assert!(stale
        .client_write(KvCommand::Put {
            key: "stale".into(),
            value: vec![]
        })
        .await
        .is_err());
    tokio::time::timeout(Duration::from_secs(5), reopened.shutdown())
        .await
        .unwrap()
        .unwrap();
    drop(reopened);
    drop(stale);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn registry_shutdown_cancellation_retains_slot_and_owner() {
    use super::registry::{GroupRegistry, RegistryError};
    let root = std::env::var_os("YADB_TEST_TMPDIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!(
            "yadb-registry-cancel-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir(&root).unwrap();
    let identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [45; 16],
            group: 1,
            incarnation: 1,
        },
        1,
        vec![1],
    )
    .unwrap();
    let config = Arc::new(openraft::Config::default().validate().unwrap());
    let mut registry = GroupRegistry::new(&root, 1).unwrap();
    registry
        .create(&identity, config.clone(), NoNetwork)
        .await
        .unwrap();
    let stale = registry.get(&identity).unwrap().clone();
    let path = std::fs::read_dir(&root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    {
        let mut future = Box::pin(registry.shutdown(&identity));
        poll_shutdown_once(future.as_mut());
    }
    assert!(registry.get(&identity).is_some());
    assert!(matches!(
        registry.open(&identity, config.clone(), NoNetwork).await,
        Err(RegistryError::Duplicate)
    ));
    assert!(
        PersistentGroup::open(&path, &identity, config.clone(), NoNetwork)
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(5), registry.shutdown(&identity))
        .await
        .unwrap()
        .unwrap();
    assert!(registry.get(&identity).is_none());
    registry.open(&identity, config, NoNetwork).await.unwrap();
    assert!(stale
        .client_write(KvCommand::Put {
            key: "stale".into(),
            value: vec![]
        })
        .await
        .is_err());
    tokio::time::timeout(Duration::from_secs(5), registry.shutdown(&identity))
        .await
        .unwrap()
        .unwrap();
    drop(registry);
    drop(stale);
    std::fs::remove_dir_all(root).unwrap();
}

// The child announces only after successful client_write responses, then stays
// alive until killed. This tests process death, not an OS or power failure.
#[tokio::test]
async fn persistent_singleton_process_kill_reopen() {
    const ENV: &str = "YADB_OPENRAFT_KILL_TEST_DIR";
    // This test kills a real child process and waits for it to come back with
    // committed data. The child re-runs this same function with ENV set, so only
    // the parent takes the heavy-test gate.
    let _gate = if std::env::var_os(ENV).is_some() {
        None
    } else {
        Some(super::test_gate::exclusive().await)
    };
    let identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [42; 16],
            group: 42,
            incarnation: 1,
        },
        1,
        vec![1],
    )
    .unwrap();
    let config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    if let Some(root) = std::env::var_os(ENV) {
        let root = std::path::PathBuf::from(root);
        tokio::time::timeout(Duration::from_secs(15), async {
            let group = PersistentGroup::create(root.join("replica"), &identity, config, NoNetwork)
                .await
                .unwrap();
            group
                .raft()
                .initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            group
                .raft()
                .wait(Some(Duration::from_secs(5)))
                .current_leader(1, "child leader")
                .await
                .unwrap();
            for i in 0..3 {
                let response = group
                    .raft()
                    .client_write(KvCommand::Put {
                        key: format!("ack-{i}"),
                        value: vec![i],
                    })
                    .await
                    .unwrap();
                assert!(matches!(response.data, KvResult::PutOk { .. }));
            }
            std::fs::write(root.join("ack"), b"ready").unwrap();
            std::future::pending::<()>().await;
        })
        .await
        .expect("child timed out");
        return;
    }
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let root = std::env::var_os("YADB_TEST_TMPDIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!(
            "yadb-openraft-kill-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir(&root).unwrap();
    let mut child = ChildGuard(
        std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("openraft_runtime::runtime_tests::persistent_singleton_process_kill_reopen")
            .arg("--nocapture")
            .env(ENV, &root)
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if root.join("ack").exists() {
                break;
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before acknowledgement"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("child acknowledgement timed out");
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    drop(child);
    let state = DurableMetaStateMachine::open(root.join("replica/state"), &identity).unwrap();
    for i in 0..3 {
        assert_eq!(
            state.snapshot().get(&format!("ack-{i}")).unwrap().value,
            vec![i]
        );
    }
    drop(state);
    let group = tokio::time::timeout(
        Duration::from_secs(10),
        PersistentGroup::open(root.join("replica"), &identity, config, NoNetwork),
    )
    .await
    .unwrap()
    .unwrap();
    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "reopened leader")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), group.shutdown())
        .await
        .unwrap()
        .unwrap();
    drop(group);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn persistent_group_partial_bootstrap_is_not_reinitialized() {
    let root = std::env::temp_dir().join(format!(
        "yadb-openraft-partial-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [8; 16],
            group: 12,
            incarnation: 1,
        },
        1,
        vec![1],
    )
    .unwrap();
    let config = Arc::new(openraft::Config::default().validate().unwrap());
    let log = DurableOpenRaftLog::create(root.join("log"), &identity).unwrap();
    drop(log);
    let before = std::fs::read(root.join("log/openraft.log")).unwrap();
    assert!(
        PersistentGroup::create(&root, &identity, config.clone(), NoNetwork)
            .await
            .is_err()
    );
    assert!(PersistentGroup::open(&root, &identity, config, NoNetwork)
        .await
        .is_err());
    assert_eq!(
        std::fs::read(root.join("log/openraft.log")).unwrap(),
        before
    );
    assert!(!root.join("state").exists());
    let _ = std::fs::remove_dir_all(root);
}

#[derive(Default)]
struct NetworkData {
    nodes: BTreeMap<u32, YadbRaft>,
    isolated: BTreeSet<u32>,
    duplicate_append: bool,
    duplicated: usize,
    captured_append: Option<(u32, AppendEntriesRequest<C>)>,
    snapshots_delivered: usize,
}

#[derive(Clone)]
struct TestNetwork {
    shared: Arc<Mutex<NetworkData>>,
    source: u32,
    target: u32,
}

impl TestNetwork {
    fn peer(&self) -> Result<YadbRaft, Unreachable> {
        let network = self.shared.lock().unwrap();
        if network.isolated.contains(&self.source) || network.isolated.contains(&self.target) {
            return Err(Unreachable::new(&io::Error::new(
                io::ErrorKind::NotConnected,
                "partition",
            )));
        }
        network.nodes.get(&self.target).cloned().ok_or_else(|| {
            Unreachable::new(&io::Error::new(io::ErrorKind::NotFound, "unknown node"))
        })
    }
}

impl RaftNetworkFactory<C> for TestNetwork {
    type Network = Self;
    async fn new_client(&mut self, target: u32, _: &BasicNode) -> Self {
        Self {
            target,
            ..self.clone()
        }
    }
}

impl RaftNetwork<C> for TestNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
        let peer = self.peer()?;
        let duplicate = {
            let mut network = self.shared.lock().unwrap();
            let duplicate = network.duplicate_append && !rpc.entries.is_empty();
            if duplicate {
                network.duplicated += 1;
                if network.captured_append.is_none() {
                    network.captured_append = Some((self.target, rpc.clone()));
                }
            }
            duplicate
        };
        if duplicate {
            peer.append_entries(rpc.clone())
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))?;
        }
        peer.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<u32>,
        _: RPCOption,
    ) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
        let peer = self.peer()?;
        peer.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
    async fn full_snapshot(
        &mut self,
        vote: Vote<u32>,
        snapshot: Snapshot<C>,
        cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _: RPCOption,
    ) -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
        let peer = self.peer().map_err(StreamingError::from)?;
        let result = tokio::select! {
            result = peer.install_full_snapshot(vote, snapshot) => result.map_err(|e| StreamingError::RemoteError(RemoteError::new(self.target, e))),
            closed = cancel => Err(StreamingError::Closed(closed)),
        };
        if result.is_ok() {
            self.shared.lock().unwrap().snapshots_delivered += 1;
        }
        result
    }
}

async fn wait_value(memory: &Memory, key: &str, value: &[u8]) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if memory
                .0
                .lock()
                .unwrap()
                .state
                .get(key)
                .is_some_and(|v| v.value == value)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replica did not apply value");
}

async fn wait_durable_value(state: &DurableMetaStateMachine, key: &str, value: &[u8]) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if state.snapshot().get(key).is_some_and(|v| v.value == value) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("durable replica did not apply value");
}

/// Waits until this replica's own durable state machine holds a snapshot.
///
/// `NetworkData::snapshots_delivered` cannot prove this: it counts a transport
/// success for any target, and openraft answers successfully even when it skips
/// an install because the snapshot is not newer than the follower's committed
/// log. Poll the state machine that must actually contain the image.
async fn wait_durable_snapshot(state: &mut DurableMetaStateMachine) -> bool {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if state.get_current_snapshot().await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok()
}

async fn persistent_raft(
    id: u32,
    root: &std::path::Path,
    shared: Arc<Mutex<NetworkData>>,
    config: Arc<openraft::Config>,
) -> (YadbRaft, DurableOpenRaftLog, DurableMetaStateMachine) {
    let identity = crate::multiraft::ReplicaIdentity::new(
        crate::multiraft::GroupIdentity {
            cluster: [6; 16],
            group: 9,
            incarnation: 1,
        },
        id,
        vec![1, 2, 3],
    )
    .unwrap();
    let log = DurableOpenRaftLog::create(root.join(format!("node-{id}/log")), &identity).unwrap();
    let state =
        DurableMetaStateMachine::create(root.join(format!("node-{id}/state")), &identity).unwrap();
    let network = TestNetwork {
        shared,
        source: id,
        target: 0,
    };
    let raft = YadbRaft::new(id, config, network, log.clone(), state.clone())
        .await
        .unwrap();
    (raft, log, state)
}

#[tokio::test]
async fn openraft_three_nodes_replicate_and_recover_a_partitioned_follower() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let shared = Arc::new(Mutex::new(NetworkData::default()));
        let config = Arc::new(
            openraft::Config {
                snapshot_policy: openraft::SnapshotPolicy::Never,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let mut nodes = BTreeMap::new();
        let mut stores = BTreeMap::new();
        for id in 1..=3 {
            let memory = Memory::default();
            let network = TestNetwork {
                shared: shared.clone(),
                source: id,
                target: 0,
            };
            let raft = YadbRaft::new(id, config.clone(), network, memory.clone(), memory.clone())
                .await
                .unwrap();
            shared.lock().unwrap().nodes.insert(id, raft.clone());
            stores.insert(id, memory);
            nodes.insert(id, raft);
        }
        nodes[&1]
            .initialize(
                (1..=3)
                    .map(|id| (id, BasicNode::new(format!("node-{id}"))))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .unwrap();
        let leader = nodes[&1]
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .unwrap()
            .current_leader
            .unwrap();
        let follower = (1..=3).find(|id| *id != leader).unwrap();
        let write = |key: &str| KvCommand::Put {
            key: key.into(),
            value: b"value".to_vec(),
        };
        nodes[&leader].client_write(write("initial")).await.unwrap();
        for store in stores.values() {
            wait_value(store, "initial", b"value").await;
        }

        shared.lock().unwrap().isolated.insert(follower);
        nodes[&leader]
            .client_write(write("during-partition"))
            .await
            .unwrap();
        assert!(stores[&follower]
            .0
            .lock()
            .unwrap()
            .state
            .get("during-partition")
            .is_none());
        shared.lock().unwrap().isolated.remove(&follower);
        wait_value(&stores[&follower], "during-partition", b"value").await;

        // Isolate both followers. A timed-out write has an unknown outcome: it
        // may commit after connectivity returns, so never assert its rollback.
        {
            let mut network = shared.lock().unwrap();
            network.isolated.extend((1..=3).filter(|id| *id != leader));
        }
        let result = tokio::time::timeout(
            Duration::from_millis(400),
            nodes[&leader].client_write(write("no-quorum")),
        )
        .await;
        assert!(
            !matches!(result, Ok(Ok(_))),
            "write committed without quorum"
        );
        for store in stores.values() {
            assert!(store.0.lock().unwrap().state.get("no-quorum").is_none());
        }
        shared.lock().unwrap().isolated.clear();

        // Elections may have advanced during the partition. Do not assume the
        // old leader remains leader, and retry only an idempotent test command:
        // a timed-out client request may still be committed later.
        let recovered_leader = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (id, raft) in &nodes {
                    if matches!(
                        tokio::time::timeout(
                            Duration::from_millis(500),
                            raft.client_write(write("after-recovery"))
                        )
                        .await,
                        Ok(Ok(_))
                    ) {
                        return *id;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("cluster did not regain quorum");
        for store in stores.values() {
            wait_value(store, "after-recovery", b"value").await;
        }

        // Deliver the same non-empty AppendEntries twice and verify that the
        // state machine sees the command exactly once on every replica.
        let revisions: BTreeMap<_, _> = stores
            .iter()
            .map(|(id, store)| (*id, store.0.lock().unwrap().state.revision()))
            .collect();
        shared.lock().unwrap().duplicate_append = true;
        nodes[&recovered_leader]
            .client_write(write("duplicate-rpc"))
            .await
            .unwrap();
        for (id, store) in &stores {
            wait_value(store, "duplicate-rpc", b"value").await;
            assert_eq!(store.0.lock().unwrap().state.revision(), revisions[id] + 1);
        }
        assert!(
            shared.lock().unwrap().duplicated > 0,
            "no AppendEntries was duplicated"
        );

        // Replay an actual earlier request after a newer value has been applied.
        // This models a delayed duplicate, not arbitrary transport reordering.
        let (target, old_rpc) = {
            let mut network = shared.lock().unwrap();
            network.duplicate_append = false;
            network
                .captured_append
                .take()
                .expect("no non-empty AppendEntries captured")
        };
        nodes[&recovered_leader]
            .client_write(KvCommand::Put {
                key: "duplicate-rpc".into(),
                value: b"newer".to_vec(),
            })
            .await
            .unwrap();
        for store in stores.values() {
            wait_value(store, "duplicate-rpc", b"newer").await;
        }
        let before = {
            let data = stores[&target].0.lock().unwrap();
            (
                data.state.revision(),
                data.applied,
                data.log.last_key_value().map(|(_, e)| e.log_id),
            )
        };
        nodes[&target].append_entries(old_rpc).await.unwrap();
        let data = stores[&target].0.lock().unwrap();
        assert_eq!(data.state.get("duplicate-rpc").unwrap().value, b"newer");
        assert_eq!(
            (
                data.state.revision(),
                data.applied,
                data.log.last_key_value().map(|(_, e)| e.log_id)
            ),
            before
        );
        drop(data);

        // The majority must elect another leader and make progress while the
        // former leader is unreachable, then bring that node up to date.
        shared.lock().unwrap().isolated.insert(recovered_leader);
        let new_leader = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (id, raft) in &nodes {
                    if *id == recovered_leader {
                        continue;
                    }
                    if matches!(
                        tokio::time::timeout(
                            Duration::from_millis(500),
                            raft.client_write(write("new-leader"))
                        )
                        .await,
                        Ok(Ok(_))
                    ) {
                        return *id;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("majority did not elect a leader and commit");
        assert_ne!(new_leader, recovered_leader);
        for (id, store) in &stores {
            if *id != recovered_leader {
                wait_value(store, "new-leader", b"value").await;
            }
        }
        assert!(stores[&recovered_leader]
            .0
            .lock()
            .unwrap()
            .state
            .get("new-leader")
            .is_none());
        shared.lock().unwrap().isolated.remove(&recovered_leader);
        wait_value(&stores[&recovered_leader], "new-leader", b"value").await;

        for raft in nodes.values() {
            raft.shutdown().await.unwrap();
        }
        shared.lock().unwrap().nodes.clear();
    })
    .await
    .expect("three-node test timed out");
}

#[tokio::test]
async fn openraft_persistent_three_nodes_restart_and_catch_up_follower() {
    // Three durable replicas with real elections: never run beside another
    // heavy cluster test.
    let _gate = super::test_gate::exclusive().await;
    tokio::time::timeout(Duration::from_secs(45), async {
        let root = std::env::temp_dir().join(format!(
            "yadb-openraft-persistent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let shared = Arc::new(Mutex::new(NetworkData::default()));
        let config = Arc::new(
            openraft::Config {
                snapshot_policy: openraft::SnapshotPolicy::Never,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let mut nodes = BTreeMap::new();
        let mut logs = BTreeMap::new();
        let mut states = BTreeMap::new();
        for id in 1..=3 {
            let (raft, log, state) =
                persistent_raft(id, &root, shared.clone(), config.clone()).await;
            shared.lock().unwrap().nodes.insert(id, raft.clone());
            nodes.insert(id, raft);
            logs.insert(id, log);
            states.insert(id, state);
        }
        nodes[&1]
            .initialize(
                (1..=3)
                    .map(|id| (id, BasicNode::new(format!("node-{id}"))))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .unwrap();
        let leader = nodes[&1]
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader.is_some(), "persistent leader")
            .await
            .unwrap()
            .current_leader
            .unwrap();
        let follower = (1..=3).find(|id| *id != leader).unwrap();

        shared.lock().unwrap().isolated.insert(follower);
        nodes[&leader]
            .client_write(KvCommand::Put {
                key: "before-restart".into(),
                value: b"majority".to_vec(),
            })
            .await
            .unwrap();
        for id in 1..=3 {
            if id != follower {
                wait_durable_value(&states[&id], "before-restart", b"majority").await;
            }
        }

        nodes[&follower].shutdown().await.unwrap();
        shared.lock().unwrap().nodes.remove(&follower);
        drop(nodes.remove(&follower).unwrap());
        drop(logs.remove(&follower).unwrap());
        drop(states.remove(&follower).unwrap());
        let (restarted, log, state) = {
            let identity = crate::multiraft::ReplicaIdentity::new(
                crate::multiraft::GroupIdentity {
                    cluster: [6; 16],
                    group: 9,
                    incarnation: 1,
                },
                follower,
                vec![1, 2, 3],
            )
            .unwrap();
            let log =
                DurableOpenRaftLog::open(root.join(format!("node-{follower}/log")), &identity)
                    .unwrap();
            let state = DurableMetaStateMachine::open(
                root.join(format!("node-{follower}/state")),
                &identity,
            )
            .unwrap();
            let network = TestNetwork {
                shared: shared.clone(),
                source: follower,
                target: 0,
            };
            let raft = YadbRaft::new(
                follower,
                config.clone(),
                network,
                log.clone(),
                state.clone(),
            )
            .await
            .unwrap();
            (raft, log, state)
        };
        shared
            .lock()
            .unwrap()
            .nodes
            .insert(follower, restarted.clone());
        nodes.insert(follower, restarted);
        logs.insert(follower, log);
        states.insert(follower, state);
        shared.lock().unwrap().isolated.remove(&follower);

        wait_durable_value(&states[&follower], "before-restart", b"majority").await;
        nodes[&leader]
            .client_write(KvCommand::Put {
                key: "after-restart".into(),
                value: b"caught-up".to_vec(),
            })
            .await
            .unwrap();
        for state in states.values() {
            wait_durable_value(state, "after-restart", b"caught-up").await;
        }
        for raft in nodes.values() {
            raft.shutdown().await.unwrap();
        }
        drop(logs);
        drop(states);
        shared.lock().unwrap().nodes.clear();
        let _ = std::fs::remove_dir_all(root);
    })
    .await
    .expect("persistent three-node test timed out");
}

#[tokio::test]
async fn openraft_persistent_cluster_recovers_lagging_follower_via_snapshot() {
    // Durable three-node cluster plus a purge/snapshot install: serialized with
    // the other heavy cluster tests so the timing assumptions hold.
    let _gate = super::test_gate::exclusive().await;
    tokio::time::timeout(Duration::from_secs(45), async {
        let root = std::env::temp_dir().join(format!(
            "yadb-openraft-snapshot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let shared = Arc::new(Mutex::new(NetworkData::default()));
        // openraft 0.9.25 has no pre-vote. With the default 150-300 ms election
        // timeout the isolated follower campaigns repeatedly, inflates its term
        // and deposes the leader when it rejoins; the new leader still has an
        // unpurged log, so the follower catches up by append entries and no
        // snapshot is installed. That is correct Raft behavior, not a bug, but
        // it makes the snapshot assertion racy. Long election timeouts keep the
        // leader stable for this short test so the purged log forces a snapshot.
        let config = Arc::new(
            openraft::Config {
                snapshot_policy: openraft::SnapshotPolicy::Never,
                max_in_snapshot_log_to_keep: 0,
                purge_batch_size: 1,
                election_timeout_min: 20_000,
                election_timeout_max: 25_000,
                heartbeat_interval: 200,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let mut nodes = BTreeMap::new();
        let mut logs = BTreeMap::new();
        let mut states = BTreeMap::new();
        for id in 1..=3 {
            let (raft, log, state) =
                persistent_raft(id, &root, shared.clone(), config.clone()).await;
            shared.lock().unwrap().nodes.insert(id, raft.clone());
            nodes.insert(id, raft);
            logs.insert(id, log);
            states.insert(id, state);
        }
        nodes[&1]
            .initialize(
                (1..=3)
                    .map(|id| (id, BasicNode::new(format!("node-{id}"))))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .unwrap();
        let leader = nodes[&1]
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .unwrap()
            .current_leader
            .unwrap();
        let follower = (1..=3).find(|id| *id != leader).unwrap();
        shared.lock().unwrap().isolated.insert(follower);
        for i in 0..5 {
            nodes[&leader]
                .client_write(KvCommand::Put {
                    key: format!("snapshot-{i}"),
                    value: b"persisted".to_vec(),
                })
                .await
                .unwrap();
        }
        nodes[&leader].trigger().snapshot().await.unwrap();
        nodes[&leader]
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |m| m.purged.is_some_and(|id| id.index >= 5),
                "snapshot log purge",
            )
            .await
            .unwrap();
        shared.lock().unwrap().isolated.remove(&follower);
        wait_durable_value(&states[&follower], "snapshot-4", b"persisted").await;
        if !wait_durable_snapshot(states.get_mut(&follower).unwrap()).await {
            let leader_metrics = {
                let m = nodes[&leader].metrics();
                let m = m.borrow();
                format!(
                    "snapshot={:?} purged={:?} last_log_index={:?} last_applied={:?}",
                    m.snapshot, m.purged, m.last_log_index, m.last_applied
                )
            };
            let follower_metrics = {
                let m = nodes[&follower].metrics();
                let m = m.borrow();
                format!(
                    "snapshot={:?} purged={:?} last_log_index={:?} last_applied={:?}",
                    m.snapshot, m.purged, m.last_log_index, m.last_applied
                )
            };
            let snapshot = states[&follower].snapshot();
            panic!(
                "durable replica did not install a snapshot; delivered={} leader[{}] follower[{}] follower_revision={} follower_has_snapshot_4={}",
                shared.lock().unwrap().snapshots_delivered,
                leader_metrics,
                follower_metrics,
                snapshot.revision,
                snapshot.get("snapshot-4").is_some(),
            );
        }
        // Transport-level sanity check only: this counter is global and also
        // counts installs that openraft skipped, so it is not evidence for this
        // follower. The poll above is.
        assert!(
            shared.lock().unwrap().snapshots_delivered > 0,
            "no snapshot transport succeeded"
        );
        nodes[&follower].shutdown().await.unwrap();
        shared.lock().unwrap().nodes.remove(&follower);
        drop(nodes.remove(&follower));
        drop(logs.remove(&follower));
        drop(states.remove(&follower));
        let identity = crate::multiraft::ReplicaIdentity::new(
            crate::multiraft::GroupIdentity {
                cluster: [6; 16],
                group: 9,
                incarnation: 1,
            },
            follower,
            vec![1, 2, 3],
        )
        .unwrap();
        let mut reopened =
            DurableMetaStateMachine::open(root.join(format!("node-{follower}/state")), &identity)
                .unwrap();
        assert_eq!(
            reopened.snapshot().get("snapshot-4").unwrap().value,
            b"persisted"
        );
        assert!(reopened.get_current_snapshot().await.unwrap().is_some());
        drop(reopened);
        for raft in nodes.values() {
            raft.shutdown().await.unwrap();
        }
        shared.lock().unwrap().nodes.clear();
        drop(nodes);
        drop(logs);
        drop(states);
        let _ = std::fs::remove_dir_all(root);
    })
    .await
    .expect("persistent snapshot cluster test timed out");
}
