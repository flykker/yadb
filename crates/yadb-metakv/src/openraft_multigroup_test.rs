//! Independent real Raft groups over loopback mTLS, not a multi-host test.
use crate::multiraft::{GroupIdentity, ReplicaIdentity};
use crate::openraft_client::{GroupRouter, OpenRaftClient};
use crate::openraft_net::{
    AuthenticatedTcpNetworkFactory, BoxFuture, RaftPeerServer, TransportConfig, WireRequest,
    WireResponse,
};
use crate::openraft_runtime::YadbRaft;
use crate::openraft_state_machine::DurableMetaStateMachine;
use crate::openraft_storage::DurableOpenRaftLog;
use crate::openraft_tls::test_support;
use crate::openraft_transport::{NodeSigningKey, PeerKeyRegistry};
use crate::{KvCommand, LocalMetaKv, MetaError, RoleState};
use openraft::{BasicNode, Snapshot};
use std::{collections::BTreeMap, io::Cursor, sync::Arc, time::Duration};

struct TestDir(std::path::PathBuf);
impl Drop for TestDir {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("multigroup test artifacts: {}", self.0.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

struct Servers(BTreeMap<(u64, u32), tokio::task::JoinHandle<()>>);
impl Drop for Servers {
    fn drop(&mut self) {
        for task in self.0.values() {
            task.abort();
        }
    }
}

fn identity(group: u64, node: u32) -> ReplicaIdentity {
    ReplicaIdentity::new(
        GroupIdentity {
            cluster: [73; 16],
            group,
            incarnation: 1,
        },
        node,
        vec![1, 2, 3],
    )
    .unwrap()
}

fn handler(raft: YadbRaft) -> Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> {
    Arc::new(move |req| {
        let raft = raft.clone();
        Box::pin(async move {
            match req {
                WireRequest::Vote(rpc) => {
                    WireResponse::Vote(Box::new(raft.vote(*rpc).await.unwrap()))
                }
                WireRequest::AppendEntries(rpc) => {
                    WireResponse::AppendEntries(Box::new(raft.append_entries(*rpc).await.unwrap()))
                }
                WireRequest::InstallSnapshot { vote, meta, data } => {
                    WireResponse::InstallSnapshot(Box::new(
                        raft.install_full_snapshot(
                            vote,
                            Snapshot {
                                meta,
                                snapshot: Box::new(Cursor::new(data)),
                            },
                        )
                        .await
                        .unwrap(),
                    ))
                }
            }
        })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_three_groups_route_independent_leaders_over_mtls() {
    // A hung Raft group would otherwise hold the heavy-test gate forever and, with
    // it, every other cluster test in the suite. Bound the whole body so a hang
    // fails one test instead of deadlocking the run.
    tokio::time::timeout(Duration::from_secs(120), multigroup_routing_body())
        .await
        .expect("multigroup routing test exceeded its 120 s budget");
}

async fn multigroup_routing_body() {
    // Nine live Raft groups on nine loopback mTLS listeners. Their quorum
    // confirmations are measured against real deadlines, so this test must not
    // share the machine with the other heavy cluster tests.
    let _gate = super::test_gate::exclusive().await;
    let path = std::env::temp_dir().join(format!(
        "yadb-multigroup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    let _dir = TestDir(path.clone());
    let keys: BTreeMap<_, _> = (1..=3u32)
        .map(|n| {
            (
                n,
                Arc::new(NodeSigningKey::from_seed(&[n as u8; 32]).unwrap()),
            )
        })
        .collect();
    let registry = Arc::new(
        PeerKeyRegistry::new(
            keys.iter()
                .map(|(id, key)| (*id, key.public_key()))
                .collect(),
        )
        .unwrap(),
    );
    let tls: BTreeMap<_, _> = (1..=3)
        .map(|n| (n, Arc::new(test_support::config(n))))
        .collect();
    let mut listeners = BTreeMap::new();
    let mut addresses: BTreeMap<u64, BTreeMap<u32, _>> = BTreeMap::new();
    for group in 1..=3u64 {
        for node in 1..=3u32 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses
                .entry(group)
                .or_default()
                .insert(node, listener.local_addr().unwrap());
            listeners.insert((group, node), listener);
        }
    }
    let config = Arc::new(
        openraft::Config {
            enable_elect: false,
            snapshot_policy: openraft::SnapshotPolicy::Never,
            // A linearizable read confirms leadership by sending empty
            // AppendEntries to every voter and requiring a quorum inside a window
            // of exactly `heartbeat_interval`
            // (openraft 0.9.25 `RaftCore::handle_check_is_leader_request`:
            // `let ttl = Duration::from_millis(self.config.heartbeat_interval)`).
            // The 50 ms default is a sub-round-trip assumption that does not hold
            // while the whole workspace suite competes for 8 cores, and this test
            // runs nine mTLS Raft groups. A miss surfaces as
            // `QuorumNotEnough { got: {leader} }`, not as stale data. Use a window
            // that matches a real deployment instead of assuming an idle machine.
            // Long election timeouts are safe here because elections are manual
            // (`enable_elect: false`) and are only re-armed on the designate.
            heartbeat_interval: 500,
            election_timeout_min: 5_000,
            election_timeout_max: 10_000,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let mut rafts = BTreeMap::new();
    let mut states = BTreeMap::new();
    let mut servers = Servers(BTreeMap::new());
    for group in 1..=3u64 {
        for node in 1..=3u32 {
            let id = identity(group, node);
            let dir = path.join(format!("{group}-{node}"));
            let log = DurableOpenRaftLog::create(dir.join("log"), &id).unwrap();
            let state = DurableMetaStateMachine::create(dir.join("state"), &id).unwrap();
            let transport = TransportConfig::new(
                id,
                keys[&node].clone(),
                addresses[&group].clone(),
                registry.clone(),
            )
            .with_tls(tls[&node].clone());
            let raft = YadbRaft::new(
                node,
                config.clone(),
                AuthenticatedTcpNetworkFactory::new(transport.clone()),
                log,
                state.clone(),
            )
            .await
            .unwrap();
            let server = RaftPeerServer::from_listener(
                listeners.remove(&(group, node)).unwrap(),
                transport,
                handler(raft.clone()),
            );
            servers
                .0
                .insert((group, node), tokio::spawn(server.serve()));
            rafts.insert((group, node), raft);
            states.insert((group, node), state);
        }
    }
    let members: BTreeMap<_, _> = (1..=3)
        .map(|n| (n, BasicNode::new(format!("node-{n}"))))
        .collect();
    for (group, leader) in [(1, 1), (2, 1), (3, 2)] {
        // initialize() starts the first election itself. Only the selected
        // node's normal election timer is enabled, so a lost vote RPC can be
        // retried by OpenRaft without allowing a different test leader.
        rafts[&(group, leader)].runtime_config().elect(true);
        rafts[&(group, leader)]
            .initialize(members.clone())
            .await
            .unwrap();
        for node in 1..=3 {
            if let Err(error) = rafts[&(group, node)]
                .wait(Some(Duration::from_secs(15)))
                .current_leader(leader, "independent leader")
                .await
            {
                for peer in 1..=3 {
                    eprintln!(
                        "group={group} node={peer} metrics={:?}",
                        rafts[&(group, peer)].metrics().borrow().clone()
                    );
                }
                panic!("group={group} node={node} expected_leader={leader}: {error}");
            }
        }
    }
    let routers: BTreeMap<_, _> = (1..=3)
        .map(|node| {
            let groups = (1..=3)
                .map(|group| {
                    (
                        group,
                        (
                            rafts[&(group, node)].clone(),
                            states[&(group, node)].clone(),
                        ),
                    )
                })
                .collect();
            (
                node,
                LocalMetaKv::<yadb_io::SimFs>::from_openraft_groups(
                    1,
                    groups,
                    BTreeMap::from([(1, 2), (2, 3), (4, 2)]),
                    BTreeMap::new(),
                    tokio::runtime::Handle::current(),
                )
                .unwrap(),
            )
        })
        .collect();
    // Project namespace ownership is decided by the router, not by the caller: a
    // project-scoped idempotency record must land in the group that owns the
    // project, and the control namespace must stay in the control group.
    let router = GroupRouter::new(
        1,
        (1..=3)
            .map(|group| {
                (
                    group,
                    (rafts[&(group, 1)].clone(), states[&(group, 1)].clone()),
                )
            })
            .collect(),
        BTreeMap::from([(1, 2), (2, 3), (4, 2)]),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    assert_eq!(router.group_id_for_project(None).unwrap(), 1);
    let project_1 = router.group_id_for_project(Some("1")).unwrap();
    let project_2 = router.group_id_for_project(Some("2")).unwrap();
    assert_eq!((project_1, project_2), (2, 3));
    assert_ne!(project_1, project_2);
    // An unmapped or noncanonical project must not silently fall back to control.
    assert!(matches!(
        router.group_id_for_project(Some("3")),
        Err(MetaError::Routing(_))
    ));
    assert!(matches!(
        router.group_id_for_project(Some("01")),
        Err(MetaError::Routing(_))
    ));
    // The control namespace is byte-identical to `record_key` in `yadbd`.
    assert_eq!(
        router.idempotency_namespace(None).unwrap(),
        "/sys/idempotency/"
    );
    for project in ["1", "2"] {
        let namespace = router.idempotency_namespace(Some(project)).unwrap();
        let expected = router.group_id_for_project(Some(project)).unwrap();
        // The routing function, not this test, decides the group; a namespace
        // that disagrees with `group_id_for_project` must fail here.
        assert_eq!(router.group_id(&namespace).unwrap(), expected);
        assert_eq!(
            router
                .group_id(&format!("{namespace}subject-hash/key-hash"))
                .unwrap(),
            expected
        );
    }
    assert!(matches!(
        router.idempotency_namespace(Some("3")),
        Err(MetaError::Routing(_))
    ));
    // Routing is only half the contract: a namespace that routes correctly but is
    // missing from `scan`'s fan-out list would still be unreadable across
    // projects, because `scan` would fall through to single-group routing and
    // panic on the rejected bare prefix. Commit one record per project and read
    // both back through `scan` so a future namespace addition that forgets the
    // fan-out list fails here instead of at runtime. Each write goes through the
    // router of the node that leads that project's group (group 2 leads on node 1,
    // group 3 on node 2).
    for (project, node) in [("1", 1u32), ("2", 2u32)] {
        let namespace = router.idempotency_namespace(Some(project)).unwrap();
        let key = format!("{namespace}subject-hash/key-hash");
        routers[&node]
            .execute(KvCommand::Put {
                key: key.clone(),
                value: key.as_bytes().to_vec(),
            })
            .unwrap();
        // Read back through the writer's own replica: it leads that group, so its
        // own committed write is already applied. A specific project namespace
        // routes to that one group, not through the cross-project fan-out.
        assert_eq!(
            routers[&node]
                .scan_prefix_or_panic(&namespace)
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>(),
            vec![key.clone()],
            "project {project} record is not readable from its own group"
        );
    }
    // Node 1 is not the leader of group 3, so its replica applies the project-2
    // record asynchronously. Wait for that replication instead of assuming it:
    // a cross-project scan must see every project's record on any node.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = router.try_scan("/idempotency/").unwrap().len();
        if seen == 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cross-project scan saw {seen} of 2 records after 10 s; the fan-out \
             list is probably missing the project idempotency namespace"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The same fan-out must be reachable through `LocalMetaKv::scan_prefix`, which
    // is the surface the daemon uses.
    assert_eq!(routers[&1].scan_prefix_or_panic("/idempotency/").len(), 2);
    // A bare project prefix must still fail closed rather than silently scan one
    // group: `/idempotency/1` has no trailing separator, so it could otherwise
    // match `/idempotency/10/...` in a different group.
    assert!(matches!(
        router.try_snapshot(&[], &["/idempotency/1".into()]),
        Err(MetaError::Routing(_))
    ));
    // Group-scoped idempotency records name their group in the namespace, so the
    // record and the counter that admits it are guarded in one transaction inside
    // one group. Control is not `g<control>`: `/sys/idempotency/` is its only
    // spelling, and accepting both would split the control records across two key
    // spaces in the same group.
    assert_eq!(
        router.group_idempotency_namespace(2).unwrap(),
        "/idempotency/g2/"
    );
    assert_eq!(
        router.group_idempotency_usage_key(2).unwrap(),
        "/idempotency/g2/_usage"
    );
    for group in [2u64, 3] {
        let namespace = router.group_idempotency_namespace(group).unwrap();
        let usage = router.group_idempotency_usage_key(group).unwrap();
        // The routing function, not this test, decides the group: a namespace
        // that disagrees with the group it names must fail here.
        assert_eq!(router.group_id(&namespace).unwrap(), group);
        assert_eq!(
            router
                .group_id(&format!("{namespace}subject-hash/key-hash"))
                .unwrap(),
            group
        );
        // The counter must resolve to the same group as the record it bounds, or
        // the guarded transaction would be rejected as cross-group.
        assert_eq!(router.group_id(&usage).unwrap(), group);
        assert_eq!(
            router.group_id(&format!("{namespace}_usage")).unwrap(),
            group
        );
    }
    // Control keeps its one spelling, and `group_id` still short-circuits every
    // `/sys/` key to it before the allowlist runs.
    assert_eq!(
        router.idempotency_namespace(None).unwrap(),
        "/sys/idempotency/"
    );
    assert_eq!(router.group_id("/sys/idempotency/").unwrap(), 1);
    assert_eq!(router.group_id("/sys/idempotency/_usage").unwrap(), 1);
    // Noncanonical, control, and unconfigured group ids are all rejected: `g01`
    // and `g+1` would be a second spelling, `g1` is control, `g0` is never a
    // configured data group, and a bare `/idempotency/g2` could prefix-match
    // `/idempotency/g20/...` in another group.
    for key in [
        "/idempotency/g01/record",
        "/idempotency/g+1/record",
        "/idempotency/g1/record",
        "/idempotency/g0/record",
        "/idempotency/g999/record",
        "/idempotency/g/record",
        "/idempotency/g2",
        "/idempotency/g",
    ] {
        assert!(
            matches!(router.group_id(key), Err(MetaError::Routing(_))),
            "{key} must be rejected"
        );
    }
    for group in [0u64, 1, 999] {
        assert!(matches!(
            router.group_idempotency_namespace(group),
            Err(MetaError::Routing(_))
        ));
        assert!(matches!(
            router.group_idempotency_usage_key(group),
            Err(MetaError::Routing(_))
        ));
    }
    // Two projects that share a group must share the record namespace and the
    // counter, otherwise two writers would each pass admission against a
    // different counter and together exceed the bound. Projects 1 and 4 both map
    // to group 2 in this fixture.
    assert_eq!(router.group_id_for_project(Some("1")).unwrap(), 2);
    assert_eq!(router.group_id_for_project(Some("4")).unwrap(), 2);
    assert_eq!(
        router.group_idempotency_namespace_for_project("1").unwrap(),
        router.group_idempotency_namespace_for_project("4").unwrap()
    );
    assert_eq!(
        router
            .group_idempotency_usage_key(router.group_id_for_project(Some("1")).unwrap())
            .unwrap(),
        router
            .group_idempotency_usage_key(router.group_id_for_project(Some("4")).unwrap())
            .unwrap()
    );
    // An unmapped or noncanonical project must not fall back to control.
    assert!(matches!(
        router.group_idempotency_namespace_for_project("3"),
        Err(MetaError::Routing(_))
    ));
    assert!(matches!(
        router.group_idempotency_namespace_for_project("01"),
        Err(MetaError::Routing(_))
    ));
    // The group fan-out is only half the contract: commit one record into each
    // group namespace through the node that leads it (group 2 leads on node 1,
    // group 3 on node 2) and read it back. A group namespace missing from `scan`'s
    // fan-out list would panic on the bare prefix instead of returning records.
    for (group, node) in [(2u64, 1u32), (3, 2)] {
        let namespace = router.group_idempotency_namespace(group).unwrap();
        let key = format!("{namespace}subject-hash/key-hash");
        routers[&node]
            .execute(KvCommand::Put {
                key: key.clone(),
                value: key.as_bytes().to_vec(),
            })
            .unwrap();
        assert_eq!(
            routers[&node]
                .scan_prefix_or_panic(&namespace)
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>(),
            vec![key.clone()],
            "group {group} record is not readable from its own group"
        );
    }
    // `router` is built on node 1: group 2 is local and group 3 replicates in, so
    // wait instead of assuming. The fan-out must cover every data group, not only
    // the groups a project maps to.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = router.try_scan("/idempotency/g").unwrap().len();
        if seen == 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "group scan saw {seen} of 2 records after 10 s; the fan-out list is \
             probably missing the group idempotency namespace"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // A specific group namespace routes to exactly one group, not the fan-out.
    assert_eq!(router.try_scan("/idempotency/g2/").unwrap().len(), 1);
    // A bare group prefix must fail closed rather than scan one arbitrary group.
    for prefix in ["/idempotency/g", "/idempotency/g2", "/idempotency/g1/"] {
        assert!(
            matches!(
                router.try_snapshot(&[], &[prefix.into()]),
                Err(MetaError::Routing(_))
            ),
            "{prefix} must be rejected"
        );
    }
    // Singleton mode has one group, and `group_id` sends every key to it, so
    // project routing must do the same instead of inventing a second group.
    let singleton_router = GroupRouter::singleton(OpenRaftClient::new(
        rafts[&(1, 1)].clone(),
        states[&(1, 1)].clone(),
        tokio::runtime::Handle::current(),
    ));
    assert_eq!(singleton_router.group_id_for_project(None).unwrap(), 0);
    assert_eq!(singleton_router.group_id_for_project(Some("2")).unwrap(), 0);
    assert_eq!(
        singleton_router.idempotency_namespace(Some("2")).unwrap(),
        "/idempotency/2/"
    );
    assert_eq!(
        singleton_router
            .group_id("/idempotency/2/subject-hash/key-hash")
            .unwrap(),
        0
    );
    // A singleton router owns only control, so it has no group-scoped namespace:
    // the constructors fail closed instead of minting a `g0` spelling of control.
    assert!(matches!(
        singleton_router.group_idempotency_namespace(0),
        Err(MetaError::Routing(_))
    ));
    assert!(matches!(
        singleton_router.group_idempotency_usage_key(2),
        Err(MetaError::Routing(_))
    ));
    assert_eq!(
        routers[&1].role_info_for_key("/db/1/key").unwrap().1,
        RoleState::Leader
    );
    assert_eq!(
        routers[&1].role_info_for_key("/db/2/key").unwrap().3,
        Some(2)
    );
    assert_eq!(
        routers[&2].role_info_for_key("/db/1/key").unwrap().3,
        Some(1)
    );
    assert_eq!(
        routers[&2].role_info_for_key("/db/2/key").unwrap().1,
        RoleState::Leader
    );
    assert_eq!(
        routers[&1].role_info_for_key("/sys/key").unwrap().3,
        Some(1)
    );
    let put = |key: &str| KvCommand::Put {
        key: key.into(),
        value: key.as_bytes().to_vec(),
    };
    routers[&1].execute(put("/sys/key")).unwrap();
    routers[&1].execute(put("/db/1/key")).unwrap();
    routers[&2].execute(put("/db/2/key")).unwrap();
    let singleton = LocalMetaKv::<yadb_io::SimFs>::from_openraft(
        rafts[&(1, 1)].clone(),
        states[&(1, 1)].clone(),
        BTreeMap::new(),
        tokio::runtime::Handle::current(),
    );
    assert_eq!(
        singleton
            .try_linearizable_snapshot(&["/sys/key".into()], &[])
            .unwrap()
            .get("/sys/key")
            .unwrap()
            .value,
        b"/sys/key"
    );
    drop(singleton);
    let project_key = "/db/1/key".to_owned();
    let snapshot = routers[&1]
        .try_linearizable_snapshot(&[project_key.clone()], &["/db/1/".into()])
        .unwrap();
    assert_eq!(
        snapshot.get(&project_key).unwrap().value,
        project_key.as_bytes()
    );
    assert_eq!(snapshot.scan_prefix("/db/1/").len(), 1);
    assert_eq!(
        snapshot.revision(),
        routers[&1].revision_for_key(&project_key).unwrap()
    );
    assert!(matches!(
        routers[&2].try_linearizable_snapshot(&[project_key.clone()], &[]),
        Err(MetaError::NotLeader { .. })
    ));
    assert!(matches!(
        routers[&1].try_linearizable_snapshot(&[project_key.clone()], &["/db/2/".into()]),
        Err(MetaError::Routing(_))
    ));
    assert!(matches!(
        routers[&1].try_linearizable_snapshot(&[], &["/db/1".into()]),
        Err(MetaError::Routing(_))
    ));
    let control = routers[&1]
        .try_linearizable_control_snapshot(&["/sys/key".into()])
        .unwrap();
    assert_eq!(control.get("/sys/key").unwrap().value, b"/sys/key");
    assert!(matches!(
        routers[&2].try_linearizable_control_snapshot(&["/sys/key".into()]),
        Err(MetaError::NotLeader { .. })
    ));
    assert_eq!(
        routers[&1]
            .try_linearizable_snapshot(&["/sys/key".into()], &[])
            .unwrap()
            .get("/sys/key")
            .unwrap()
            .value,
        b"/sys/key"
    );
    assert!(matches!(
        routers[&1].execute(put("/db/2/wrong")),
        Err(MetaError::NotLeader { leader: Some(2) })
    ));
    assert!(matches!(
        routers[&2].execute(put("/db/1/wrong")),
        Err(MetaError::NotLeader { leader: Some(1) })
    ));
    // Remove both peers' listeners for group 2, leaving its leader without quorum.
    for node in [2, 3] {
        let server = servers.0.remove(&(2, node)).unwrap();
        server.abort();
        let _ = server.await;
    }
    let started = std::time::Instant::now();
    assert!(routers[&1]
        .try_linearizable_snapshot(&[project_key], &[])
        .is_err());
    assert!(started.elapsed() < Duration::from_secs(3));
    // Stop just group 2's leader; group 3 must remain writable on node 2.
    let server = servers.0.remove(&(2, 1)).unwrap();
    server.abort();
    let _ = server.await;
    rafts[&(2, 1)].shutdown().await.unwrap();
    routers[&2].execute(put("/db/2/still-live")).unwrap();
    for group in [1, 2, 3] {
        let (node, key) = match group {
            1 => (1, "/sys/key"),
            2 => (1, "/db/1/key"),
            _ => (2, "/db/2/still-live"),
        };
        assert_eq!(
            states[&(group, node)].snapshot().get(key).unwrap().value,
            key.as_bytes()
        );
    }
    // These routers hold `DurableMetaStateMachine` clones, which own the durable
    // file lock. Release them before reopening the same state directories below,
    // or the reopen fails with `WouldBlock`.
    drop(routers);
    drop(router);
    drop(singleton_router);
    for (_, server) in std::mem::take(&mut servers.0) {
        server.abort();
        let _ = server.await;
    }
    for ((group, node), raft) in &rafts {
        if (*group, *node) != (2, 1) {
            raft.shutdown().await.unwrap();
        }
    }
    drop(rafts);
    drop(states);
    for (group, node, key) in [
        (1, 1, "/sys/key"),
        (2, 1, "/db/1/key"),
        (3, 2, "/db/2/still-live"),
    ] {
        let state = DurableMetaStateMachine::open(
            path.join(format!("{group}-{node}/state")),
            &identity(group, node),
        )
        .unwrap();
        assert_eq!(state.snapshot().get(key).unwrap().value, key.as_bytes());
    }
}
