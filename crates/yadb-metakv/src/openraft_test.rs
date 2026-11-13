//! Volatile singleton smoke test only. Never use this store for real data.
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::error::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
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
    let response = raft.client_write(KvCommand::Put { key: "key".into(), value: b"value".to_vec() }).await.unwrap();
    assert!(matches!(response.data, KvResult::PutOk { .. }));
    assert_eq!(memory.0.lock().unwrap().state.get("key").unwrap().value, b"value");
    raft.shutdown().await.unwrap();
}
