//! Volatile in-process tests only. Never use this store or network for real data.
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::error::{Fatal, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader,
    RaftNetwork, RaftNetworkFactory, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote};

use super::{YadbRaft, YadbRaftTypeConfig as C};
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
    StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Write,
        io::Error::new(io::ErrorKind::Unsupported, "snapshots disabled in volatile smoke test"))
}

impl RaftLogReader<C> for Memory {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self, range: R,
    ) -> Result<Vec<Entry<C>>, StorageError<u32>> {
        Ok(self.0.lock().unwrap().log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<C> for Memory {
    type LogReader = Memory;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u32>> {
        let data = self.0.lock().unwrap();
        Ok(LogState { last_purged_log_id: data.purged,
            last_log_id: data.log.last_key_value().map(|(_, e)| e.log_id).or(data.purged) })
    }
    async fn get_log_reader(&mut self) -> Memory { self.clone() }
    async fn save_vote(&mut self, vote: &Vote<u32>) -> Result<(), StorageError<u32>> {
        self.0.lock().unwrap().vote = Some(*vote); Ok(())
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u32>>, StorageError<u32>> {
        Ok(self.0.lock().unwrap().vote)
    }
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<u32>>
    where I: IntoIterator<Item = Entry<C>> + Send, I::IntoIter: Send {
        let mut data = self.0.lock().unwrap();
        for entry in entries { data.log.insert(entry.log_id.index, entry); }
        drop(data);
        // Deliberately not durable: this implementation exists only under cfg(test).
        callback.log_io_completed(Ok(()));
        Ok(())
    }
    async fn truncate(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
        self.0.lock().unwrap().log.split_off(&log_id.index); Ok(())
    }
    async fn purge(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
        let mut data = self.0.lock().unwrap();
        data.log.retain(|index, _| *index > log_id.index);
        data.purged = Some(log_id); Ok(())
    }
}

impl RaftSnapshotBuilder<C> for Memory {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<u32>> { Err(unsupported()) }
}

impl RaftStateMachine<C> for Memory {
    type SnapshotBuilder = Memory;
    async fn applied_state(&mut self) -> Result<(Option<LogId<u32>>, StoredMembership<u32, BasicNode>), StorageError<u32>> {
        let data = self.0.lock().unwrap(); Ok((data.applied, data.membership.clone()))
    }
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<KvResult>, StorageError<u32>>
    where I: IntoIterator<Item = Entry<C>> + Send, I::IntoIter: Send {
        let mut data = self.0.lock().unwrap();
        let mut results = Vec::new();
        for entry in entries {
            data.applied = Some(entry.log_id);
            results.push(match entry.payload {
                EntryPayload::Normal(cmd) => data.state.apply(cmd),
                EntryPayload::Membership(m) => {
                    data.membership = StoredMembership::new(Some(entry.log_id), m);
                    KvResult::TxnOk { revision: data.state.revision(), versions: BTreeMap::new() }
                }
                EntryPayload::Blank => KvResult::TxnOk { revision: data.state.revision(), versions: BTreeMap::new() },
            });
        }
        Ok(results)
    }
    async fn get_snapshot_builder(&mut self) -> Memory { self.clone() }
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u32>> { Err(unsupported()) }
    async fn install_snapshot(&mut self, _: &SnapshotMeta<u32, BasicNode>, _: Box<Cursor<Vec<u8>>>) -> Result<(), StorageError<u32>> { Err(unsupported()) }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<u32>> { Ok(None) }
}

struct NoNetwork;
impl RaftNetworkFactory<C> for NoNetwork {
    type Network = NoNetwork;
    async fn new_client(&mut self, _: u32, _: &BasicNode) -> NoNetwork { NoNetwork }
}
impl RaftNetwork<C> for NoNetwork {
    async fn append_entries(&mut self, _: AppendEntriesRequest<C>, _: RPCOption) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
        Err(Unreachable::new(&io::Error::new(io::ErrorKind::NotConnected, "singleton" )).into())
    }
    async fn vote(&mut self, _: VoteRequest<u32>, _: RPCOption) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
        Err(Unreachable::new(&io::Error::new(io::ErrorKind::NotConnected, "singleton" )).into())
    }
    async fn full_snapshot(&mut self, _: Vote<u32>, _: Snapshot<C>, _: impl std::future::Future<Output = ReplicationClosed> + Send + 'static, _: RPCOption)
        -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
        Err(Unreachable::new(&io::Error::new(io::ErrorKind::NotConnected, "singleton" )).into())
    }
}

#[tokio::test]
async fn openraft_singleton_elects_and_applies_yadb_command_in_memory() {
    let memory = Memory::default();
    let config = Arc::new(openraft::Config { snapshot_policy: openraft::SnapshotPolicy::Never, ..Default::default() }.validate().unwrap());
    let raft = YadbRaft::new(1, config, NoNetwork, memory.clone(), memory.clone()).await.unwrap();
    raft.initialize(BTreeMap::from([(1, BasicNode::new("node-1"))])).await.unwrap();
    raft.wait(Some(Duration::from_secs(5))).current_leader(1, "singleton leader").await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), raft.client_write(KvCommand::Put { key: "key".into(), value: b"value".to_vec() })).await.unwrap().unwrap();
    assert!(matches!(response.data, KvResult::PutOk { .. }));
    assert_eq!(memory.0.lock().unwrap().state.get("key").unwrap().value, b"value");
    tokio::time::timeout(Duration::from_secs(5), raft.shutdown()).await.unwrap().unwrap();
}

#[derive(Default)]
struct NetworkData {
    nodes: BTreeMap<u32, YadbRaft>,
    isolated: BTreeSet<u32>,
    duplicate_append: bool,
    duplicated: usize,
    captured_append: Option<(u32, AppendEntriesRequest<C>)>,
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
            return Err(Unreachable::new(&io::Error::new(io::ErrorKind::NotConnected, "partition")));
        }
        network.nodes.get(&self.target).cloned().ok_or_else(||
            Unreachable::new(&io::Error::new(io::ErrorKind::NotFound, "unknown node")))
    }
}

impl RaftNetworkFactory<C> for TestNetwork {
    type Network = Self;
    async fn new_client(&mut self, target: u32, _: &BasicNode) -> Self {
        Self { target, ..self.clone() }
    }
}

impl RaftNetwork<C> for TestNetwork {
    async fn append_entries(&mut self, rpc: AppendEntriesRequest<C>, _: RPCOption) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
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
            peer.append_entries(rpc.clone()).await.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))?;
        }
        peer.append_entries(rpc).await.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
    async fn vote(&mut self, rpc: VoteRequest<u32>, _: RPCOption) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
        let peer = self.peer()?;
        peer.vote(rpc).await.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
    async fn full_snapshot(&mut self, _: Vote<u32>, _: Snapshot<C>, _: impl std::future::Future<Output = ReplicationClosed> + Send + 'static, _: RPCOption)
        -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
        Err(Unreachable::new(&io::Error::new(io::ErrorKind::Unsupported, "snapshots disabled")).into())
    }
}

async fn wait_value(memory: &Memory, key: &str, value: &[u8]) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if memory.0.lock().unwrap().state.get(key).is_some_and(|v| v.value == value) { break; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("replica did not apply value");
}

#[tokio::test]
async fn openraft_three_nodes_replicate_and_recover_a_partitioned_follower() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let shared = Arc::new(Mutex::new(NetworkData::default()));
        let config = Arc::new(openraft::Config { snapshot_policy: openraft::SnapshotPolicy::Never, ..Default::default() }.validate().unwrap());
        let mut nodes = BTreeMap::new();
        let mut stores = BTreeMap::new();
        for id in 1..=3 {
            let memory = Memory::default();
            let network = TestNetwork { shared: shared.clone(), source: id, target: 0 };
            let raft = YadbRaft::new(id, config.clone(), network, memory.clone(), memory.clone()).await.unwrap();
            shared.lock().unwrap().nodes.insert(id, raft.clone());
            stores.insert(id, memory);
            nodes.insert(id, raft);
        }
        nodes[&1].initialize((1..=3).map(|id| (id, BasicNode::new(format!("node-{id}")))).collect::<BTreeMap<_, _>>()).await.unwrap();
        let leader = nodes[&1].wait(Some(Duration::from_secs(5))).metrics(|m| m.current_leader.is_some(), "leader").await.unwrap().current_leader.unwrap();
        let follower = (1..=3).find(|id| *id != leader).unwrap();
        let write = |key: &str| KvCommand::Put { key: key.into(), value: b"value".to_vec() };
        nodes[&leader].client_write(write("initial")).await.unwrap();
        for store in stores.values() { wait_value(store, "initial", b"value").await; }

        shared.lock().unwrap().isolated.insert(follower);
        nodes[&leader].client_write(write("during-partition")).await.unwrap();
        assert!(stores[&follower].0.lock().unwrap().state.get("during-partition").is_none());
        shared.lock().unwrap().isolated.remove(&follower);
        wait_value(&stores[&follower], "during-partition", b"value").await;

        // Isolate both followers. A timed-out write has an unknown outcome: it
        // may commit after connectivity returns, so never assert its rollback.
        {
            let mut network = shared.lock().unwrap();
            network.isolated.extend((1..=3).filter(|id| *id != leader));
        }
        let result = tokio::time::timeout(Duration::from_millis(400), nodes[&leader].client_write(write("no-quorum"))).await;
        assert!(!matches!(result, Ok(Ok(_))), "write committed without quorum");
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
                    if matches!(tokio::time::timeout(Duration::from_millis(500),
                        raft.client_write(write("after-recovery"))).await, Ok(Ok(_))) {
                        return *id;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.expect("cluster did not regain quorum");
        for store in stores.values() { wait_value(store, "after-recovery", b"value").await; }

        // Deliver the same non-empty AppendEntries twice and verify that the
        // state machine sees the command exactly once on every replica.
        let revisions: BTreeMap<_, _> = stores.iter().map(|(id, store)|
            (*id, store.0.lock().unwrap().state.revision())).collect();
        shared.lock().unwrap().duplicate_append = true;
        nodes[&recovered_leader].client_write(write("duplicate-rpc")).await.unwrap();
        for (id, store) in &stores {
            wait_value(store, "duplicate-rpc", b"value").await;
            assert_eq!(store.0.lock().unwrap().state.revision(), revisions[id] + 1);
        }
        assert!(shared.lock().unwrap().duplicated > 0, "no AppendEntries was duplicated");

        // Replay an actual earlier request after a newer value has been applied.
        // This models a delayed duplicate, not arbitrary transport reordering.
        let (target, old_rpc) = {
            let mut network = shared.lock().unwrap();
            network.duplicate_append = false;
            network.captured_append.take().expect("no non-empty AppendEntries captured")
        };
        nodes[&recovered_leader].client_write(KvCommand::Put {
            key: "duplicate-rpc".into(), value: b"newer".to_vec(),
        }).await.unwrap();
        for store in stores.values() { wait_value(store, "duplicate-rpc", b"newer").await; }
        let before = {
            let data = stores[&target].0.lock().unwrap();
            (data.state.revision(), data.applied, data.log.last_key_value().map(|(_, e)| e.log_id))
        };
        nodes[&target].append_entries(old_rpc).await.unwrap();
        let data = stores[&target].0.lock().unwrap();
        assert_eq!(data.state.get("duplicate-rpc").unwrap().value, b"newer");
        assert_eq!((data.state.revision(), data.applied, data.log.last_key_value().map(|(_, e)| e.log_id)), before);
        drop(data);

        for raft in nodes.values() { raft.shutdown().await.unwrap(); }
        shared.lock().unwrap().nodes.clear();
    }).await.expect("three-node test timed out");
}
