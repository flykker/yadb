#![cfg(feature = "openraft-storage")]
//! Engine-level integration only: no daemon protocol or multi-node claims.
use openraft::{
    error::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode, RaftNetwork, RaftNetworkFactory, Snapshot, Vote,
};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use yadb_blob::{ErasureScheme, PoolStriper, VolumeConfig, VolumeStore};
use yadb_block::BlockVolumeEngine;
use yadb_db::{DatabaseEngine, SqlValue};
use yadb_io::OsFs;
use yadb_metakv::{
    multiraft::{GroupIdentity, ReplicaIdentity},
    openraft_runtime::{persistent::PersistentGroup, YadbRaftTypeConfig as C},
    KvCommand, LocalMetaKv, MetaError, TxnOp,
};
use yadb_s3::ObjectStoreEngine;

struct NoNetwork;
impl RaftNetworkFactory<C> for NoNetwork {
    type Network = Self;
    async fn new_client(&mut self, _: u32, _: &BasicNode) -> Self {
        Self
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

struct TestDir(PathBuf);
impl Drop for TestDir {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("openraft client artifacts: {}", self.0.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
fn pool(root: &Path) -> Arc<PoolStriper<OsFs>> {
    let fs = OsFs::new(root).unwrap();
    let domains = (0..3)
        .map(|n| {
            VolumeStore::open(fs.clone(), format!("disk-{n}"), VolumeConfig::default()).unwrap()
        })
        .collect();
    Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, domains).unwrap())
}

fn exercise(meta: LocalMetaKv<OsFs>, root: &Path, create: bool, pid: u32) {
    assert!(meta.is_openraft());
    let pool = pool(root);
    let db = DatabaseEngine::new(meta.clone(), 16);
    db.register_pool("wal", pool.clone());
    db.register_pool("data", pool.clone());
    let s3 = ObjectStoreEngine::new(meta.clone(), 4096);
    assert!(matches!(
        s3.collect_orphans_all(),
        Err(yadb_s3::S3Error::NamespaceScanUnavailable)
    ));
    s3.register_pool("data", pool.clone());
    let block = BlockVolumeEngine::new(meta, 4096);
    block.register_pool("data", pool);
    let sql = |statement| {
        db.execute_sql(pid, "ledger", "wal", "data", 4, statement)
            .unwrap()
    };
    if create {
        sql("CREATE TABLE accounts (id INT PRIMARY KEY, tier TEXT, balance INT)");
        sql("INSERT INTO accounts VALUES (1,'gold',5000),(2,'silver',2000)");
        sql("CREATE INDEX idx_tier ON accounts (tier)");
        sql("BEGIN; UPDATE accounts SET balance = balance - 1500 WHERE id = 1; UPDATE accounts SET balance = balance + 1500, tier = 'gold' WHERE id = 2; COMMIT;");
        s3.put_object(
            pid,
            "store",
            "data",
            "bucket",
            "docs/a",
            "text/plain",
            format!("object-a-{pid}").as_bytes(),
        )
        .unwrap();
        s3.put_object(
            pid,
            "store",
            "data",
            "bucket",
            "docs/b",
            "text/plain",
            format!("object-b-{pid}").as_bytes(),
        )
        .unwrap();
        block
            .write_at(pid, "volume", "data", 8192, 0, &vec![pid as u8; 8192])
            .unwrap();
        block.unmap_at(pid, "volume", 8192, 0, 4096).unwrap();
    }
    assert_eq!(
        sql("SELECT id, balance FROM accounts WHERE tier = 'gold'").rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(3500)],
            vec![SqlValue::Int(2), SqlValue::Int(3500)]
        ]
    );
    assert_eq!(
        s3.get_object(pid, "store", "bucket", "docs/a").unwrap().1,
        format!("object-a-{pid}").as_bytes()
    );
    assert_eq!(
        s3.get_object(pid, "store", "bucket", "docs/b").unwrap().1,
        format!("object-b-{pid}").as_bytes()
    );
    let keys: Vec<_> = s3
        .list_objects(pid, "store", "bucket", "docs/")
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .collect();
    assert_eq!(keys, vec!["docs/a", "docs/b"]);
    assert_eq!(
        block.read_at(pid, "volume", "data", 8192, 0, 4096).unwrap(),
        vec![0; 4096]
    );
    assert_eq!(
        block
            .read_at(pid, "volume", "data", 8192, 4096, 4096)
            .unwrap(),
        vec![pid as u8; 4096]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn singleton_openraft_backed_sql_s3_block_survive_reopen() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!(
            "yadb-openraft-clients-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let dir = TestDir(root);
        let blobs = dir.0.join("blobs");
        std::fs::create_dir(&blobs).unwrap();
        let legacy = dir.0.join("legacy-untouched");
        std::fs::create_dir(&legacy).unwrap();
        std::fs::write(legacy.join("marker"), b"legacy data").unwrap();
        let id = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [72; 16],
                group: 1,
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
        let path = dir.0.join("replica");
        let group = PersistentGroup::create(&path, &id, config.clone(), NoNetwork)
            .await
            .unwrap();
        group
            .raft()
            .initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        group
            .raft()
            .wait(Some(Duration::from_secs(10)))
            .current_leader(1, "singleton leader")
            .await
            .unwrap();
        let meta = LocalMetaKv::<OsFs>::from_openraft(
            group.raft().clone(),
            group.state_machine().clone(),
            BTreeMap::from([(1, "127.0.0.1:1".to_string())]),
            tokio::runtime::Handle::current(),
        );
        let blob_path = blobs.clone();
        tokio::task::spawn_blocking(move || exercise(meta, &blob_path, true, 1))
            .await
            .unwrap();
        group.shutdown().await.unwrap();
        drop(group);
        let group = PersistentGroup::open(&path, &id, config, NoNetwork)
            .await
            .unwrap();
        group
            .raft()
            .wait(Some(Duration::from_secs(10)))
            .current_leader(1, "reopened leader")
            .await
            .unwrap();
        let meta = LocalMetaKv::<OsFs>::from_openraft(
            group.raft().clone(),
            group.state_machine().clone(),
            BTreeMap::from([(1, "127.0.0.1:1".to_string())]),
            tokio::runtime::Handle::current(),
        );
        tokio::task::spawn_blocking(move || exercise(meta, &blobs, false, 1))
            .await
            .unwrap();
        group.shutdown().await.unwrap();
        drop(group);
        assert_eq!(
            std::fs::read(legacy.join("marker")).unwrap(),
            b"legacy data"
        );
        assert_eq!(std::fs::read_dir(legacy).unwrap().count(), 1);
    })
    .await
    .expect("engine integration timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_group_openraft_routes_clients_and_reopens() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!(
            "yadb-openraft-multigroup-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let dir = TestDir(root);
        let blobs = dir.0.join("blobs");
        std::fs::create_dir(&blobs).unwrap();
        let legacy = dir.0.join("legacy-untouched");
        std::fs::create_dir(&legacy).unwrap();
        std::fs::write(legacy.join("marker"), b"legacy data").unwrap();
        let config = Arc::new(
            openraft::Config {
                snapshot_policy: openraft::SnapshotPolicy::Never,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let mut groups = BTreeMap::new();
        for group in 1..=3u64 {
            let id = ReplicaIdentity::new(
                GroupIdentity {
                    cluster: [73; 16],
                    group,
                    incarnation: 1,
                },
                1,
                vec![1],
            )
            .unwrap();
            let owner = PersistentGroup::create(
                dir.0.join(format!("group-{group}")),
                &id,
                config.clone(),
                NoNetwork,
            )
            .await
            .unwrap();
            owner
                .raft()
                .initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            owner
                .raft()
                .wait(Some(Duration::from_secs(10)))
                .current_leader(1, "group leader")
                .await
                .unwrap();
            groups.insert(group, owner);
        }
        for reopen in [false, true] {
            let members = groups
                .iter()
                .map(|(id, group)| (*id, (group.raft().clone(), group.state_machine().clone())))
                .collect();
            let meta = LocalMetaKv::<OsFs>::from_openraft_groups(
                1,
                members,
                BTreeMap::from([(1, 2), (2, 3)]),
                BTreeMap::from([(1, "127.0.0.1:1".to_string())]),
                tokio::runtime::Handle::current(),
            )
            .unwrap();
            let path = blobs.clone();
            tokio::task::spawn_blocking(move || {
                if !reopen {
                    meta.execute(KvCommand::Put {
                        key: "/sys/integration".into(),
                        value: b"control".to_vec(),
                    })
                    .unwrap();
                }
                for pid in 1..=2 {
                    exercise(meta.clone(), &path, !reopen, pid);
                }
                let before1 = meta.revision_for_key("/db/1/cross").unwrap();
                let before2 = meta.revision_for_key("/db/2/cross").unwrap();
                let keys = ["/db/1/cross".to_string(), "/db/2/cross".to_string()];
                let result = meta.execute(KvCommand::RevisionTxn {
                    guards: vec![],
                    ops: keys
                        .iter()
                        .map(|key| TxnOp::Put {
                            key: key.clone(),
                            value: vec![1],
                        })
                        .collect(),
                });
                assert!(
                    matches!(result, Err(MetaError::Routing(_))),
                    "cross-group transaction must be rejected: {result:?}"
                );
                assert!(
                    matches!(meta.try_snapshot(&keys, &[]), Err(MetaError::Routing(_))),
                    "cross-group snapshot must be rejected"
                );
                assert_eq!(meta.revision_for_key(&keys[0]).unwrap(), before1);
                assert_eq!(meta.revision_for_key(&keys[1]).unwrap(), before2);
                assert!(meta.get(&keys[0]).is_none());
                assert!(meta.get(&keys[1]).is_none());
            })
            .await
            .unwrap();
            let control = groups[&1]
                .state_machine()
                .try_snapshot(&[], &["/".into()])
                .unwrap();
            assert_eq!(control.get("/sys/integration").unwrap().value, b"control");
            assert!(control
                .scan_prefix("/")
                .iter()
                .all(|(key, _)| key.starts_with("/sys/")));
            for (group, pid, other) in [(2, 1, 2), (3, 2, 1)] {
                let state = groups[&group]
                    .state_machine()
                    .try_snapshot(&[], &["/".into()])
                    .unwrap();
                for namespace in ["db", "s3", "block"] {
                    assert!(
                        !state
                            .scan_prefix(&format!("/{namespace}/{pid}/"))
                            .is_empty(),
                        "group {group} missing {namespace} project {pid}"
                    );
                    assert!(state
                        .scan_prefix(&format!("/{namespace}/{other}/"))
                        .is_empty());
                }
                assert!(state.scan_prefix("/sys/").is_empty());
            }
            for group in groups.values() {
                group.shutdown().await.unwrap();
            }
            drop(groups);
            if reopen {
                break;
            }
            groups = BTreeMap::new();
            for group in 1..=3u64 {
                let id = ReplicaIdentity::new(
                    GroupIdentity {
                        cluster: [73; 16],
                        group,
                        incarnation: 1,
                    },
                    1,
                    vec![1],
                )
                .unwrap();
                let owner = PersistentGroup::open(
                    dir.0.join(format!("group-{group}")),
                    &id,
                    config.clone(),
                    NoNetwork,
                )
                .await
                .unwrap();
                owner
                    .raft()
                    .wait(Some(Duration::from_secs(10)))
                    .current_leader(1, "reopened group leader")
                    .await
                    .unwrap();
                groups.insert(group, owner);
            }
        }
        assert_eq!(
            std::fs::read(legacy.join("marker")).unwrap(),
            b"legacy data"
        );
        assert_eq!(std::fs::read_dir(legacy).unwrap().count(), 1);
    })
    .await
    .expect("multi-group engine integration timed out");
}
