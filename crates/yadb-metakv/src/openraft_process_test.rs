//! Independent processes on loopback. Process kill is not a power-loss test.
use crate::{
    multiraft::{GroupIdentity, ReplicaIdentity},
    openraft_net::{
        AuthenticatedTcpNetworkFactory, BoxFuture, RaftPeerServer, TransportConfig, WireRequest,
        WireResponse,
    },
    openraft_runtime::{persistent::PersistentGroup, YadbRaft},
    openraft_state_machine::DurableMetaStateMachine,
    openraft_storage::DurableOpenRaftLog,
    openraft_transport::{NodeSigningKey, PeerKeyRegistry},
    KvCommand, KvResult,
};
use openraft::{storage::RaftLogStorage, BasicNode, Snapshot};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const CHILD_ENV: &str = "YADB_PROCESS_RAFT_CHILD";
const TEST_NAME: &str =
    "openraft_runtime::process_tests::three_process_mtls_failover_and_quorum_loss";

fn identity(node: u32) -> ReplicaIdentity {
    ReplicaIdentity::new(
        GroupIdentity {
            cluster: [61; 16],
            group: 61,
            incarnation: 1,
        },
        node,
        vec![1, 2, 3],
    )
    .unwrap()
}
fn key(node: u32) -> NodeSigningKey {
    NodeSigningKey::from_seed(&[node as u8; 32]).unwrap()
}
fn handler(
    raft: YadbRaft,
    snapshots: Arc<AtomicUsize>,
) -> Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> {
    Arc::new(move |req| {
        let raft = raft.clone();
        let snapshots = snapshots.clone();
        Box::pin(async move {
            match req {
                WireRequest::AppendEntries(rpc) => {
                    WireResponse::AppendEntries(Box::new(raft.append_entries(*rpc).await.unwrap()))
                }
                WireRequest::Vote(rpc) => {
                    WireResponse::Vote(Box::new(raft.vote(*rpc).await.unwrap()))
                }
                WireRequest::InstallSnapshot { vote, meta, data } => {
                    let response = raft
                        .install_full_snapshot(
                            vote,
                            Snapshot {
                                meta,
                                snapshot: Box::new(Cursor::new(data)),
                            },
                        )
                        .await
                        .unwrap_or_else(|error| panic!("snapshot install RPC failed: {error:?}"));
                    snapshots.fetch_add(1, Ordering::SeqCst);
                    WireResponse::InstallSnapshot(Box::new(response))
                }
            }
        })
    })
}
fn atomic_json(path: &Path, value: &Value) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::rename(tmp, path).unwrap();
}

// The state image is atomically replaced by the adapter. This read-only test
// observer checks its bounded header and checksum before inspecting its value.
fn image_value(path: &Path, key: &str) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .ok()?
        .take(64 * 1024 * 1024 + 25)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() < 24
        || bytes.len() > 64 * 1024 * 1024 + 24
        || &bytes[..8] != b"YORSM001"
        || u32::from_le_bytes(bytes[8..12].try_into().ok()?) != 1
    {
        return None;
    }
    let len = u64::from_le_bytes(bytes[12..20].try_into().ok()?) as usize;
    if len != bytes.len() - 24
        || crc32fast::hash(&bytes[24..]) != u32::from_le_bytes(bytes[20..24].try_into().ok()?)
    {
        return None;
    }
    let image: Value = serde_json::from_slice(&bytes[24..]).ok()?;
    serde_json::from_value(image["state"]["values"][key]["value"].clone()).ok()
}

async fn child(config: Value) {
    let node = config["node"].as_u64().unwrap() as u32;
    let root = PathBuf::from(config["root"].as_str().unwrap());
    let control = PathBuf::from(config["control"].as_str().unwrap());
    let peers: BTreeMap<u32, SocketAddr> = (1..=3)
        .map(|n| {
            (
                n,
                config["peers"][n.to_string()]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
            )
        })
        .collect();
    let registry = Arc::new(
        PeerKeyRegistry::new((1..=3).map(|n| (n, key(n).public_key())).collect()).unwrap(),
    );
    let transport =
        TransportConfig::new(identity(node), Arc::new(key(node)), peers.clone(), registry)
            .with_tls(Arc::new(crate::openraft_tls::test_support::config(node)));
    let listen: SocketAddr = config["listen"].as_str().unwrap().parse().unwrap();
    let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
    let config_raft = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let network = AuthenticatedTcpNetworkFactory::new(transport.clone());
    let dir = root.join(format!("node-{node}"));
    let group = if config["open"].as_bool().unwrap() {
        PersistentGroup::open(&dir, &identity(node), config_raft, network)
            .await
            .unwrap()
    } else {
        PersistentGroup::create(&dir, &identity(node), config_raft, network)
            .await
            .unwrap()
    };
    let snapshots = Arc::new(AtomicUsize::new(0));
    // The child owns this task for its entire lifetime; the parent kills and
    // reaps the process rather than requesting graceful shutdown.
    let _server = tokio::spawn(
        RaftPeerServer::from_listener(
            listener,
            transport,
            handler(group.raft().clone(), snapshots.clone()),
        )
        .serve(),
    );
    atomic_json(&control.join("ready"), &json!({"pid": std::process::id()}));
    let mut seq = 1u64;
    loop {
        let path = control.join(format!("request-{seq}"));
        if !path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let request: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let result = match request["op"].as_str().unwrap() {
            "initialize" => {
                json!({"ok": group.raft().initialize((1..=3).map(|n| (n, BasicNode::new(format!("node-{n}.test")))).collect::<BTreeMap<_, _>>()).await.is_ok()})
            }
            "status" => {
                let m = group.raft().metrics().borrow().clone();
                json!({"leader": m.current_leader, "last_log_index": m.last_log_index, "applied_index": m.last_applied.map(|id| id.index), "snapshot_index": m.snapshot.map(|id| id.index), "purged": m.purged.map(|id| id.index), "snapshots": snapshots.load(Ordering::SeqCst), "metrics": format!("{m:?}")})
            }
            "value" => {
                json!({"value": image_value(&dir.join("state/state.json"), request["key"].as_str().unwrap())})
            }
            "write" => {
                let key = request["key"].as_str().unwrap().to_owned();
                let value = request["value"].as_str().unwrap().as_bytes().to_vec();
                let result = tokio::time::timeout(
                    Duration::from_millis(1200),
                    group.raft().client_write(KvCommand::Put { key, value }),
                )
                .await;
                match result {
                    Ok(Ok(response)) => {
                        json!({"ack": matches!(response.data, KvResult::PutOk { .. }), "result": format!("{:?}", response.data)})
                    }
                    Ok(Err(error)) => json!({"ack": false, "error": format!("{error:?}")}),
                    Err(error) => {
                        json!({"ack": false, "error": format!("client_write timeout: {error}")})
                    }
                }
            }
            "snapshot" => json!({"ok": group.raft().trigger().snapshot().await.is_ok()}),
            other => panic!("unknown test control operation {other}"),
        };
        atomic_json(&control.join(format!("response-{seq}")), &result);
        seq += 1;
    }
}

struct TestDir(PathBuf);
impl Drop for TestDir {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("process test artifacts: {}", self.0.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
struct Process {
    child: Child,
    control: PathBuf,
    seq: u64,
    node: u32,
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Process {
    async fn request(&mut self, req: Value) -> Value {
        let seq = self.seq;
        self.seq += 1;
        atomic_json(&self.control.join(format!("request-{seq}")), &req);
        let path = self.control.join(format!("response-{seq}"));
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if path.exists() {
                    return serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                }
                assert!(
                    self.child.try_wait().unwrap().is_none(),
                    "node {} exited while handling {req}",
                    self.node
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("node {} control timeout: {req}", self.node))
    }
}
fn start(root: &Path, peers: &BTreeMap<u32, SocketAddr>, node: u32, generation: u32) -> Process {
    start_with_listener(root, peers, peers[&node], node, generation)
}
fn start_with_listener(
    root: &Path,
    peers: &BTreeMap<u32, SocketAddr>,
    listen: SocketAddr,
    node: u32,
    generation: u32,
) -> Process {
    let control = root.join(format!("control-{node}-{generation}"));
    std::fs::create_dir(&control).unwrap();
    let config = json!({"node": node, "root": root, "control": control, "listen": listen.to_string(), "peers": peers, "open": generation > 0});
    let child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .env_remove("YADB_OPENRAFT_KILL_TEST_DIR")
        .env(CHILD_ENV, config.to_string())
        .spawn()
        .unwrap();
    Process {
        child,
        control,
        seq: 1,
        node,
    }
}
async fn ready(process: &mut Process) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !process.control.join("ready").exists() {
            assert!(
                process.child.try_wait().unwrap().is_none(),
                "node {} exited during startup",
                process.node
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("child startup timeout");
}
async fn leader(nodes: &mut BTreeMap<u32, Process>) -> u32 {
    // This is only a cached leader hint, not a quorum/read barrier. Progress
    // is established separately by successful client_write acknowledgements.
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            for (id, p) in nodes.iter_mut() {
                let status = p.request(json!({"op":"status"})).await;
                if status["leader"].as_u64() == Some(*id as u64) {
                    return *id;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("no leader elected")
}
async fn write(nodes: &mut BTreeMap<u32, Process>, key: &str) -> u32 {
    let mut last_attempt = None;
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let id = leader(nodes).await;
            let response = nodes
                .get_mut(&id)
                .unwrap()
                .request(json!({"op":"write", "key":key, "value":key}))
                .await;
            if response["ack"] == true {
                return id;
            }
            last_attempt = Some((id, response));
        }
    })
    .await;
    match result {
        Ok(id) => id,
        Err(_) => {
            eprintln!("no acknowledged write for key {key:?}; last attempt: {last_attempt:?}");
            for (id, process) in nodes.iter_mut() {
                let status = tokio::time::timeout(
                    Duration::from_secs(2),
                    process.request(json!({"op":"status"})),
                )
                .await;
                eprintln!("node {id} status: {status:?}");
            }
            panic!("no acknowledged write for key {key:?}");
        }
    }
}
async fn check_values(nodes: &mut BTreeMap<u32, Process>, keys: &[&str]) {
    let mut waiting = String::new();
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        for (id, process) in nodes.iter_mut() {
            for key in keys {
                waiting = format!("node {id}, key {key}");
                loop {
                    let result = process.request(json!({"op":"value", "key":key})).await;
                    if result["value"] == json!(key.as_bytes()) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                eprintln!("node {id} applied {key}");
            }
        }
    })
    .await;
    if result.is_err() {
        eprintln!("replica catch-up timed out waiting for {waiting}");
        for (id, process) in nodes.iter_mut() {
            let status = tokio::time::timeout(
                Duration::from_secs(2),
                process.request(json!({"op":"status"})),
            )
            .await;
            eprintln!("node {id} status: {status:?}");
        }
        panic!("replica catch-up timed out waiting for {waiting}");
    }
}

// One listener per directed link means the parent knows both source and
// destination without inspecting or terminating the encrypted TLS stream.
// A block command closes all existing tunnels and disables new accepts before
// acknowledging to the test. TCP clients may connect into the listen backlog,
// but cannot exchange bytes until the link is healed.
struct Proxy {
    address: SocketAddr,
    control: tokio::sync::mpsc::Sender<(bool, tokio::sync::oneshot::Sender<()>)>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Proxy {
    async fn new(target: SocketAddr) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (control, mut commands) =
            tokio::sync::mpsc::channel::<(bool, tokio::sync::oneshot::Sender<()>)>(1);
        let task = tokio::spawn(async move {
            let mut blocked = false;
            let mut tunnels = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    command = commands.recv() => match command {
                        Some((block, ack)) => {
                            blocked = block;
                            if blocked {
                                tunnels.abort_all();
                                while tunnels.join_next().await.is_some() {}
                            }
                            let _ = ack.send(());
                        }
                        None => break,
                    },
                    accepted = listener.accept(), if !blocked => {
                        let (mut incoming, _) = match accepted { Ok(v) => v, Err(_) => break };
                        if tunnels.len() >= 32 { continue; }
                        tunnels.spawn(async move {
                            let _ = tokio::time::timeout(Duration::from_secs(10), async {
                                let mut outgoing = tokio::net::TcpStream::connect(target).await?;
                                tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await?;
                                Ok::<(), std::io::Error>(())
                            }).await;
                        });
                    },
                    Some(_) = tunnels.join_next(), if !tunnels.is_empty() => {},
                }
            }
            tunnels.abort_all();
            while tunnels.join_next().await.is_some() {}
        });
        Self {
            address,
            control,
            task,
        }
    }
    async fn block(&self, blocked: bool) {
        let (ack, received) = tokio::sync::oneshot::channel();
        tokio::time::timeout(Duration::from_secs(5), async {
            self.control
                .send((blocked, ack))
                .await
                .expect("proxy stopped");
            received
                .await
                .expect("proxy did not acknowledge link state");
        })
        .await
        .expect("proxy control timed out");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_process_mtls_network_partition() {
    // Avoid inheriting the child selector into unrelated invocations.
    if std::env::var_os(CHILD_ENV).is_some() {
        panic!("partition parent cannot be a child");
    }
    // Three real processes with mTLS and default 150–300 ms election timeouts:
    // only one such test may run at a time.
    let _gate = super::test_gate::exclusive().await;
    tokio::time::timeout(Duration::from_secs(90), async {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!(
            "yadb-partition-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let dir = TestDir(root);
        let reservations: Vec<_> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addresses: BTreeMap<_, _> = reservations
            .iter()
            .enumerate()
            .map(|(i, l)| ((i + 1) as u32, l.local_addr().unwrap()))
            .collect();
        let mut proxies = BTreeMap::new();
        for source in 1..=3 {
            for target in 1..=3 {
                if source != target {
                    proxies.insert((source, target), Proxy::new(addresses[&target]).await);
                }
            }
        }
        drop(reservations);
        let mut nodes = BTreeMap::new();
        for source in 1..=3 {
            let peers = (1..=3)
                .map(|target| {
                    (
                        target,
                        if target == source {
                            addresses[&target]
                        } else {
                            proxies[&(source, target)].address
                        },
                    )
                })
                .collect();
            nodes.insert(
                source,
                start_with_listener(&dir.0, &peers, addresses[&source], source, 0),
            );
        }
        for node in nodes.values_mut() {
            ready(node).await;
        }
        assert_eq!(
            nodes
                .get_mut(&1)
                .unwrap()
                .request(json!({"op":"initialize"}))
                .await["ok"],
            true
        );
        let isolated = write(&mut nodes, "partition-before").await;
        check_values(&mut nodes, &["partition-before"]).await;
        for ((source, target), proxy) in &proxies {
            if *source == isolated || *target == isolated {
                proxy.block(true).await;
            }
        }
        // A timed-out isolated write has unknown outcome, never assert rollback.
        let result = nodes
            .get_mut(&isolated)
            .unwrap()
            .request(json!({"op":"write", "key":"partition-unknown", "value":"partition-unknown"}))
            .await;
        assert_eq!(result["ack"], false, "isolated leader acknowledged a write");
        // Restrict leader hints and writes to the majority, leaving the isolated
        // process running and cut off in both directions.
        let isolated_process = nodes.remove(&isolated).unwrap();
        let majority_leader = write(&mut nodes, "partition-majority").await;
        assert_ne!(majority_leader, isolated);
        check_values(&mut nodes, &["partition-majority"]).await;
        nodes.insert(isolated, isolated_process);
        for ((source, target), proxy) in &proxies {
            if *source == isolated || *target == isolated {
                proxy.block(false).await;
            }
        }
        write(&mut nodes, "partition-healed").await;
        check_values(
            &mut nodes,
            &["partition-before", "partition-majority", "partition-healed"],
        )
        .await;
        drop(nodes);
        // Explicitly abort and drain the proxy tasks on the success path; their
        // JoinSets abort and release all owned tunnels on cancellation.
        for (_, proxy) in proxies {
            proxy.task.abort();
            // Proxy implements Drop, so await through a mutable reference.
            let mut proxy = proxy;
            let _ = (&mut proxy.task).await;
        }
        for n in 1..=3 {
            let state =
                DurableMetaStateMachine::open(dir.0.join(format!("node-{n}/state")), &identity(n))
                    .unwrap();
            for key in ["partition-before", "partition-majority", "partition-healed"] {
                assert_eq!(state.snapshot().get(key).unwrap().value, key.as_bytes());
            }
        }
    })
    .await
    .expect("partition test exceeded total timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_process_mtls_failover_and_quorum_loss() {
    if let Some(config) = std::env::var_os(CHILD_ENV) {
        let config: Value = serde_json::from_str(config.to_str().unwrap()).unwrap();
        tokio::time::timeout(Duration::from_secs(120), child(config))
            .await
            .expect("child lifetime exceeded");
        return;
    }
    // Acquired by the parent only. The gate is a process-local mutex, so a child
    // acquiring it would lock a private copy and change nothing; skipping it keeps
    // that explicit. Three processes plus repeated leader kills need a quiet
    // machine, so this test never overlaps another heavy cluster test.
    let _gate = super::test_gate::exclusive().await;
    tokio::time::timeout(Duration::from_secs(100), async {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!(
            "yadb-process-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let dir = TestDir(root);
        // Reserve distinct ports until all addresses are selected. Bind races with
        // unrelated local processes remain possible between release and child bind.
        let reservations: Vec<_> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let peers: BTreeMap<_, _> = reservations
            .iter()
            .enumerate()
            .map(|(i, l)| ((i + 1) as u32, l.local_addr().unwrap()))
            .collect();
        drop(reservations);
        let mut nodes: BTreeMap<_, _> = (1..=3).map(|n| (n, start(&dir.0, &peers, n, 0))).collect();
        for node in nodes.values_mut() {
            ready(node).await;
        }
        assert_eq!(
            nodes
                .get_mut(&1)
                .unwrap()
                .request(json!({"op":"initialize"}))
                .await["ok"],
            true
        );
        let old_leader = write(&mut nodes, "before-kill").await;
        check_values(&mut nodes, &["before-kill"]).await;
        drop(nodes.remove(&old_leader));
        let new_leader = write(&mut nodes, "after-kill").await;
        assert_ne!(old_leader, new_leader);
        nodes.insert(old_leader, start(&dir.0, &peers, old_leader, 1));
        ready(nodes.get_mut(&old_leader).unwrap()).await;
        check_values(&mut nodes, &["before-kill", "after-kill"]).await;
        let survivor = leader(&mut nodes).await;
        for n in (1..=3).filter(|n| *n != survivor) {
            drop(nodes.remove(&n));
        }
        let no_quorum = nodes
            .get_mut(&survivor)
            .unwrap()
            .request(json!({"op":"write", "key":"unknown-outcome", "value":"unknown-outcome"}))
            .await;
        assert_eq!(
            no_quorum["ack"], false,
            "singleton acknowledged without quorum"
        );
        for n in (1..=3).filter(|n| *n != survivor) {
            let generation = if n == old_leader { 2 } else { 1 };
            nodes.insert(n, start(&dir.0, &peers, n, generation));
            ready(nodes.get_mut(&n).unwrap()).await;
        }
        write(&mut nodes, "restored-quorum").await;
        check_values(
            &mut nodes,
            &["before-kill", "after-kill", "restored-quorum"],
        )
        .await;
        let active_leader = write(&mut nodes, "pre-snapshot").await;
        let offline = (1..=3).find(|n| *n != active_leader).unwrap();
        drop(nodes.remove(&offline));
        let offline_last_log = {
            let mut log = DurableOpenRaftLog::open(
                dir.0.join(format!("node-{offline}/log")),
                &identity(offline),
            )
            .unwrap();
            log.get_log_state()
                .await
                .unwrap()
                .last_log_id
                .unwrap_or_else(|| panic!("offline node {offline} has no last log id"))
                .index
        };
        let snapshot_leader = write(&mut nodes, "snapshot-offline-1").await;
        let snapshot_leader2 = write(&mut nodes, "snapshot-offline-2").await;
        assert_eq!(
            snapshot_leader, snapshot_leader2,
            "leadership changed during snapshot setup"
        );
        let p = nodes.get_mut(&snapshot_leader).unwrap();
        assert_eq!(p.request(json!({"op":"snapshot"})).await["ok"], true);
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let status = p.request(json!({"op":"status"})).await;
                assert_eq!(
                    status["leader"].as_u64(),
                    Some(snapshot_leader as u64),
                    "leadership changed during snapshot purge: {status}"
                );
                if status["purged"]
                    .as_u64()
                    .is_some_and(|index| index > offline_last_log)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("snapshot purge timed out");
        nodes.insert(offline, start(&dir.0, &peers, offline, 3));
        ready(nodes.get_mut(&offline).unwrap()).await;
        check_values(
            &mut nodes,
            &[
                "before-kill",
                "after-kill",
                "restored-quorum",
                "pre-snapshot",
                "snapshot-offline-1",
                "snapshot-offline-2",
            ],
        )
        .await;
        let leader_status = nodes
            .get_mut(&snapshot_leader)
            .unwrap()
            .request(json!({"op":"status"}))
            .await;
        assert_eq!(
            leader_status["leader"].as_u64(),
            Some(snapshot_leader as u64),
            "leadership changed during snapshot catch-up: {leader_status}"
        );
        let status = nodes
            .get_mut(&offline)
            .unwrap()
            .request(json!({"op":"status"}))
            .await;
        assert!(
            status["snapshots"].as_u64().unwrap() > 0,
            "offline follower did not install a snapshot: {status}"
        );
        drop(nodes);
        // Verify persisted values with the actual adapter after every process is dead.
        for n in 1..=3 {
            let state =
                DurableMetaStateMachine::open(dir.0.join(format!("node-{n}/state")), &identity(n))
                    .unwrap();
            for key in [
                "before-kill",
                "after-kill",
                "restored-quorum",
                "pre-snapshot",
                "snapshot-offline-1",
                "snapshot-offline-2",
            ] {
                assert_eq!(state.snapshot().get(key).unwrap().value, key.as_bytes());
            }
        }
    })
    .await
    .expect("multi-process test exceeded total timeout");
}
