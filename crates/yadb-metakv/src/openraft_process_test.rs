//! Independent processes on loopback over mTLS.
//!
//! Process kill is not a power-loss test: `SIGKILL` leaves the page cache
//! intact, so the power-loss test here proves crash consistency of what the
//! child had already written, not device-level power-loss safety. Delayed and
//! duplicated RPCs are injected by a userspace TLS-terminating relay in the
//! parent, not by a real network.
use crate::{
    multiraft::{GroupIdentity, ReplicaIdentity},
    openraft_net::{
        AuthenticatedTcpNetworkFactory, BoxFuture, RaftPeerServer, TransportConfig, WireRequest,
        WireResponse,
    },
    openraft_runtime::{persistent::PersistentGroup, YadbRaft},
    openraft_state_machine::{DeltaFrame, DurableMetaStateMachine},
    openraft_storage::{read_frame, DurableOpenRaftLog, Frame},
    openraft_transport::{NodeSigningKey, PeerKeyRegistry, SignedRaftEnvelope},
    KvCommand, KvResult,
};
use openraft::{
    network::RaftNetworkFactory,
    storage::{RaftLogStorage, RaftStateMachine},
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Snapshot,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Cursor, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
/// What the child saw at the Raft RPC boundary.
///
/// The transport has already decrypted and authenticated the frame before the
/// handler runs, so these counters say nothing about TLS. `duplicate_appends`
/// is the observable a duplicating network must move: a second delivery of the
/// same leader/term, previous log id and entry batch. The Raft handler is then
/// expected to stay idempotent (the durable state machine rejects a repeated
/// log id), which is what the delayed/duplicated process test asserts.
#[derive(Default)]
struct RpcObservations {
    appends: AtomicUsize,
    duplicate_appends: AtomicUsize,
    votes: AtomicUsize,
    seen_appends: Mutex<BTreeSet<String>>,
}

fn note_append(
    rpc: &openraft::raft::AppendEntriesRequest<crate::openraft_runtime::YadbRaftTypeConfig>,
    observations: &RpcObservations,
) {
    observations.appends.fetch_add(1, Ordering::SeqCst);
    // Empty appends are heartbeats: they repeat byte-for-byte by design, so
    // counting them as duplicates would say nothing about delivery.
    if rpc.entries.is_empty() {
        return;
    }
    let signature = format!(
        "{:?}|{:?}|{:?}|{}",
        rpc.vote,
        rpc.prev_log_id,
        rpc.entries.last().map(|entry| entry.log_id),
        rpc.entries.len()
    );
    if !observations.seen_appends.lock().unwrap().insert(signature) {
        observations
            .duplicate_appends
            .fetch_add(1, Ordering::SeqCst);
    }
}

fn handler(
    raft: YadbRaft,
    snapshots: Arc<AtomicUsize>,
    observations: Arc<RpcObservations>,
) -> Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> {
    Arc::new(move |req| {
        let raft = raft.clone();
        let snapshots = snapshots.clone();
        let observations = observations.clone();
        Box::pin(async move {
            match req {
                WireRequest::AppendEntries(rpc) => {
                    note_append(&rpc, &observations);
                    WireResponse::AppendEntries(Box::new(raft.append_entries(*rpc).await.unwrap()))
                }
                WireRequest::Vote(rpc) => {
                    observations.votes.fetch_add(1, Ordering::SeqCst);
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
//
// Entries applied since the last checkpoint live in the append-only delta file
// next to the image, so the observer replays those frames too: that is the same
// durable state the state machine reconstructs on reopen.
fn delta_value(dir: &Path, key: &str, checkpoint_index: Option<u64>) -> Option<Option<Vec<u8>>> {
    let mut file = std::fs::File::open(dir.join("state.delta")).ok()?;
    let mut found: Option<Option<Vec<u8>>> = None;
    loop {
        match read_frame(&mut file).ok()? {
            Frame::End | Frame::Torn => break,
            Frame::Complete { payload, .. } => {
                let frame: DeltaFrame = serde_json::from_slice(&payload).ok()?;
                for entry in frame.entries {
                    if checkpoint_index.is_some_and(|index| entry.log_id.index <= index) {
                        continue;
                    }
                    match entry.payload {
                        EntryPayload::Normal(KvCommand::Put { key: k, value }) if k == key => {
                            found = Some(Some(value));
                        }
                        EntryPayload::Normal(KvCommand::Delete { key: k }) if k == key => {
                            found = Some(None);
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    found
}

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
    let checkpoint_index = image["applied"]["index"].as_u64();
    let mut value: Option<Vec<u8>> =
        serde_json::from_value(image["state"]["values"][key]["value"].clone()).ok();
    if let Some(delta) = path
        .parent()
        .and_then(|dir| delta_value(dir, key, checkpoint_index))
    {
        value = delta;
    }
    value
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
    // A probe child opens an existing replica directory under a different
    // incarnation to prove a mismatched-identity restart is refused. Every
    // normal child uses `identity(node)`.
    let replica_identity = match config["incarnation"].as_u64() {
        Some(incarnation) => ReplicaIdentity::new(
            GroupIdentity {
                cluster: [61; 16],
                group: 61,
                incarnation,
            },
            node,
            vec![1, 2, 3],
        )
        .unwrap(),
        None => identity(node),
    };
    let registry = Arc::new(
        PeerKeyRegistry::new((1..=3).map(|n| (n, key(n).public_key())).collect()).unwrap(),
    );
    let transport = TransportConfig::new(
        replica_identity.clone(),
        Arc::new(key(node)),
        peers.clone(),
        registry,
    )
    .with_tls(Arc::new(crate::openraft_tls::test_support::config(node)));
    let listen: SocketAddr = config["listen"].as_str().unwrap().parse().unwrap();
    let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
    let config_raft = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            // These were left at OpenRaft's defaults (50 ms heartbeat, 150-300 ms
            // elections) and that made the fixture flaky in two distinct ways:
            // the first client write after `initialize` sometimes never committed
            // (term 42, zero acks, `applied_index: None`), and a leader
            // installing a snapshot to the restarted follower was voted out
            // mid-install. Both are the same cause the daemon already refuses:
            // `C::timeout(heartbeat_interval, ...)` wraps every append, the
            // follower may have applied it, and OpenRaft does not retry a miss.
            // The daemon rejects a heartbeat below 500 ms for exactly this reason
            // and was measured clean at 750 ms (`docs/multiraft-verification.md`).
            // This fixture bypassed that validation, so it exercised a
            // configuration the product never accepts. Use the same budget as
            // `crates/yadbd/tests/openraft_daemon.rs`, keeping the 3:1 ratio
            // OpenRaft requires between election timeout and heartbeat.
            heartbeat_interval: 750,
            election_timeout_min: 2250,
            election_timeout_max: 3000,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let network = AuthenticatedTcpNetworkFactory::new(transport.clone());
    let dir = root.join(format!("node-{node}"));
    let group = match if config["open"].as_bool().unwrap() {
        PersistentGroup::open(&dir, &replica_identity, config_raft, network).await
    } else {
        PersistentGroup::create(&dir, &replica_identity, config_raft, network).await
    } {
        Ok(group) => group,
        // A probe reports the refusal instead of panicking so the parent can
        // assert the exact reason. Normal children still fail loudly.
        Err(error) if config["report_startup_error"].as_bool().unwrap_or(false) => {
            atomic_json(
                &control.join("ready"),
                &json!({"error": format!("{error}")}),
            );
            return;
        }
        Err(error) => panic!("replica startup failed: {error}"),
    };
    let snapshots = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(RpcObservations::default());
    // The child owns this task for its entire lifetime; the parent kills and
    // reaps the process rather than requesting graceful shutdown.
    let _server = tokio::spawn(
        RaftPeerServer::from_listener(
            listener,
            transport,
            handler(
                group.raft().clone(),
                snapshots.clone(),
                observations.clone(),
            ),
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
                json!({"leader": m.current_leader, "last_log_index": m.last_log_index, "applied_index": m.last_applied.map(|id| id.index), "snapshot_index": m.snapshot.map(|id| id.index), "purged": m.purged.map(|id| id.index), "snapshots": snapshots.load(Ordering::SeqCst), "metrics": format!("{m:?}"), "rpc": {"appends": observations.appends.load(Ordering::SeqCst), "duplicate_appends": observations.duplicate_appends.load(Ordering::SeqCst), "votes": observations.votes.load(Ordering::SeqCst), "transport": crate::openraft_net::transport_counters()}})
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
                        // The applied index at acknowledgement time is what the
                        // parent compares against every replica afterwards: an
                        // ack is only meaningful if the entry is in a log that a
                        // majority holds, and the replicas then have to reach it.
                        let applied = group
                            .raft()
                            .metrics()
                            .borrow()
                            .last_applied
                            .map(|id| id.index);
                        json!({"ack": matches!(response.data, KvResult::PutOk { .. }), "result": format!("{:?}", response.data), "applied_index": applied})
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
    /// SIGKILL, reaped and checked. `Child::kill` sends SIGKILL on Unix; the
    /// assertion keeps this distinct from a graceful shutdown or a proxy cut.
    fn sigkill(&mut self) {
        self.child.kill().unwrap();
        let status = self.child.wait().unwrap();
        #[cfg(unix)]
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(9),
            "node {} was not killed by SIGKILL: {status:?}",
            self.node
        );
        #[cfg(not(unix))]
        assert!(
            !status.success(),
            "node {} exited successfully instead of being killed: {status:?}",
            self.node
        );
    }

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
    start_with_options(root, peers, listen, node, generation, json!({}))
}
/// Spawns a child with extra config keys merged over the defaults. The child
/// entry point is `TEST_NAME`, the failover test, whose `CHILD_ENV` branch runs
/// `child` for every parent test in this file.
fn start_with_options(
    root: &Path,
    peers: &BTreeMap<u32, SocketAddr>,
    listen: SocketAddr,
    node: u32,
    generation: u32,
    extra: Value,
) -> Process {
    let control = root.join(format!("control-{node}-{generation}"));
    std::fs::create_dir(&control).unwrap();
    let mut config = json!({"node": node, "root": root, "control": control, "listen": listen.to_string(), "peers": peers, "open": generation > 0});
    config
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().cloned().unwrap_or_default());
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
/// Like [`write`], but also returns the leader's applied index at the moment the
/// client write was acknowledged. That index is the observable used to show the
/// acknowledgement reflects a committed (quorum) entry rather than a local
/// append: every replica is then required to reach it.
async fn write_and_index(nodes: &mut BTreeMap<u32, Process>, key: &str) -> (u32, u64) {
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
                let applied = response["applied_index"].as_u64().unwrap_or_else(|| {
                    panic!("node {id} acknowledged without an applied index: {response}")
                });
                return (id, applied);
            }
            last_attempt = Some((id, response));
        }
    })
    .await;
    match result {
        Ok(value) => value,
        Err(_) => {
            eprintln!("no acknowledged write for key {key:?}; last attempt: {last_attempt:?}");
            panic!("no acknowledged write for key {key:?}");
        }
    }
}

/// Waits until every replica's `last_applied` reaches `index`.
///
/// The leader reported this index when it acknowledged the client write. A
/// follower that has not applied it yet is still catching up, so this is a
/// poll, not a snapshot: it proves the entry reached every replica, not that it
/// was there at the instant of the ack.
async fn wait_applied_at_least(nodes: &mut BTreeMap<u32, Process>, index: u64) {
    tokio::time::timeout(Duration::from_secs(20), async {
        for process in nodes.values_mut() {
            loop {
                let status = process.request(json!({"op":"status"})).await;
                if status["applied_index"]
                    .as_u64()
                    .is_some_and(|applied| applied >= index)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    })
    .await
    .expect("a replica never applied the acknowledged index");
}

/// Waits until each replica has applied its whole log (`applied == last_log`).
///
/// This is the shape a duplicated delivery must not break. If a receiver
/// applied a repeated entry instead of ignoring it, its applied index would run
/// past the log or the durable adapter would reject the non-consecutive apply,
/// so equality is the observable the inversion attacks.
async fn wait_applied_caught_up(nodes: &mut BTreeMap<u32, Process>) {
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        for process in nodes.values_mut() {
            loop {
                let status = process.request(json!({"op":"status"})).await;
                let applied = status["applied_index"].as_u64();
                let last = status["last_log_index"].as_u64();
                if applied.is_some() && applied == last {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    })
    .await;
    if result.is_err() {
        for (id, process) in nodes.iter_mut() {
            let status = tokio::time::timeout(
                Duration::from_secs(2),
                process.request(json!({"op":"status"})),
            )
            .await;
            eprintln!("node {id} status while waiting for applied == last_log: {status:?}");
        }
        panic!("a replica never applied its whole log");
    }
}

/// Sum of `envelopes_rejected` across the child processes.
///
/// The counter lives in each child's transport, so this reads it through the
/// control channel rather than from the parent's own process.
async fn rejected_envelopes(nodes: &mut BTreeMap<u32, Process>) -> u64 {
    let mut total = 0;
    for process in nodes.values_mut() {
        let status = process.request(json!({"op":"status"})).await;
        total += status["rpc"]["transport"]["envelopes_rejected"]
            .as_u64()
            .unwrap_or_else(|| panic!("status has no transport counter: {status}"));
    }
    total
}

/// Sum of non-heartbeat append deliveries that repeated an earlier batch.
async fn duplicate_appends(nodes: &mut BTreeMap<u32, Process>) -> u64 {
    let mut total = 0;
    for process in nodes.values_mut() {
        let status = process.request(json!({"op":"status"})).await;
        total += status["rpc"]["duplicate_appends"]
            .as_u64()
            .unwrap_or_else(|| panic!("status has no duplicate counter: {status}"));
    }
    total
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

// A relay that terminates mTLS in the parent and re-originates Raft RPCs.
//
// The existing partition `Proxy` only copies ciphertext, so it can cut a link
// but cannot duplicate a frame: each Raft RPC is one TLS session, and a second
// copy of the ciphertext would be rejected by TLS before Raft saw it. To
// duplicate a *Raft* request the relay has to decrypt it, and to deliver the
// duplicate to the real child it has to authenticate as the original sender.
// Test TLS material is available to the parent, so both are possible without
// root and without a kernel network shim.
//
// The relay is per directed link `(source, target)`: it listens on the address
// the source's peer map points at, presents the target's certificate to the
// source, and opens a client connection to the real target as the source. It
// then forwards the challenge and the request frame, optionally after a delay.
// In `RELAY_DUPLICATE` it also re-originates the same `WireRequest` as a
// separate authenticated RPC on its own connection: that is a duplicated RPC
// arriving at the real receiving process, not a duplicated local channel.
// In `RELAY_REPLAY` it instead writes the *same* signed frame on a second
// connection, which the target must reject because the frame is bound to the
// first connection's server challenge.
const RELAY_DUPLICATE: u64 = 1;
const RELAY_REPLAY: u64 = 2;

#[derive(Default)]
struct RelayStats {
    forwarded: AtomicUsize,
    reoriginated: AtomicUsize,
    replayed: AtomicUsize,
}

struct FaultRelay {
    address: SocketAddr,
    mode: Arc<AtomicU64>,
    delay_ms: Arc<AtomicU64>,
    stats: Arc<RelayStats>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FaultRelay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FaultRelay {
    async fn new(
        source: u32,
        target: u32,
        target_address: SocketAddr,
        real_peers: BTreeMap<u32, SocketAddr>,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mode = Arc::new(AtomicU64::new(RELAY_DUPLICATE));
        let delay_ms = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(RelayStats::default());
        let task_mode = mode.clone();
        let task_delay = delay_ms.clone();
        let task_stats = stats.clone();
        let task = tokio::spawn(async move {
            let mut tunnels = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        if tunnels.len() >= 64 { continue; }
                        let mode = task_mode.clone();
                        let delay = task_delay.clone();
                        let stats = task_stats.clone();
                        let real_peers = real_peers.clone();
                        tunnels.spawn(async move {
                            let _ = tokio::time::timeout(
                                Duration::from_secs(20),
                                relay_connection(
                                    source,
                                    target,
                                    target_address,
                                    real_peers,
                                    stream,
                                    mode,
                                    delay,
                                    stats,
                                ),
                            )
                            .await;
                        });
                    }
                    Some(_) = tunnels.join_next(), if !tunnels.is_empty() => {}
                }
            }
            tunnels.abort_all();
            while tunnels.join_next().await.is_some() {}
        });
        Self {
            address,
            mode,
            delay_ms,
            stats,
            task,
        }
    }

    fn set_mode(&self, mode: u64) {
        self.mode.store(mode, Ordering::SeqCst);
    }

    fn set_delay(&self, delay: Duration) {
        self.delay_ms
            .store(delay.as_millis() as u64, Ordering::SeqCst);
    }

    fn reoriginated(&self) -> usize {
        self.stats.reoriginated.load(Ordering::SeqCst)
    }
}

fn relay_client_config(source: u32, peers: BTreeMap<u32, SocketAddr>) -> TransportConfig {
    let registry = Arc::new(
        PeerKeyRegistry::new((1..=3).map(|n| (n, key(n).public_key())).collect()).unwrap(),
    );
    TransportConfig::new(identity(source), Arc::new(key(source)), peers, registry)
        .with_tls(Arc::new(crate::openraft_tls::test_support::config(source)))
}

fn decode_request(body: &[u8]) -> Option<WireRequest> {
    let envelope = SignedRaftEnvelope::decode_json(body).ok()?;
    let call: Value = serde_json::from_slice(&envelope.payload).ok()?;
    serde_json::from_value(call["request"].clone()).ok()
}

async fn relay_read_frame<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
) -> std::io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let length = u32::from_le_bytes(header[1..].try_into().unwrap()) as usize;
    if length > crate::openraft_net::MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "relay frame exceeds limit",
        ));
    }
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await?;
    Ok((header[0], body))
}

async fn relay_write_frame<S: tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    kind: u8,
    body: &[u8],
) -> std::io::Result<()> {
    let mut header = [0u8; 5];
    header[0] = kind;
    header[1..].copy_from_slice(&(body.len() as u32).to_le_bytes());
    stream.write_all(&header).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

/// Opens a second authenticated connection and writes the same signed request
/// frame verbatim. The frame carries the challenge from the first connection,
/// so the target must reject it before the Raft handler.
async fn replay_frame(
    source: u32,
    target: u32,
    target_address: SocketAddr,
    body: &[u8],
) -> std::io::Result<()> {
    let client_tls = crate::openraft_tls::test_support::config(source);
    let stream = tokio::net::TcpStream::connect(target_address).await?;
    let mut stream = client_tls
        .client
        .connect(client_tls.name(target)?, stream)
        .await?;
    let peer = client_tls.peer(
        stream.get_ref().1.peer_certificates(),
        stream.get_ref().1.alpn_protocol(),
    )?;
    if peer != target {
        return Err(std::io::Error::other("replay reached the wrong peer"));
    }
    let (kind, _) = relay_read_frame(&mut stream).await?;
    if kind != 3 {
        return Err(std::io::Error::other("replay expected a challenge"));
    }
    relay_write_frame(&mut stream, 1, body).await?;
    // The target closes without a response; a read that ends here is the
    // rejection. Anything else is unexpected but is still reported by the
    // caller through the child's rejected-envelope counter.
    let mut byte = [0u8; 1];
    let _ = stream.read(&mut byte).await;
    Ok(())
}

async fn relay_connection(
    source: u32,
    target: u32,
    target_address: SocketAddr,
    real_peers: BTreeMap<u32, SocketAddr>,
    incoming: tokio::net::TcpStream,
    mode: Arc<AtomicU64>,
    delay_ms: Arc<AtomicU64>,
    stats: Arc<RelayStats>,
) -> std::io::Result<()> {
    let server_tls = crate::openraft_tls::test_support::config(target);
    let mut incoming = server_tls.server.accept(incoming).await?;
    let peer = server_tls.peer(
        incoming.get_ref().1.peer_certificates(),
        incoming.get_ref().1.alpn_protocol(),
    )?;
    if peer != source {
        return Err(std::io::Error::other("relay: source certificate mismatch"));
    }
    let client_tls = crate::openraft_tls::test_support::config(source);
    let outgoing = tokio::net::TcpStream::connect(target_address).await?;
    let mut outgoing = client_tls
        .client
        .connect(client_tls.name(target)?, outgoing)
        .await?;
    let peer = client_tls.peer(
        outgoing.get_ref().1.peer_certificates(),
        outgoing.get_ref().1.alpn_protocol(),
    )?;
    if peer != target {
        return Err(std::io::Error::other("relay: target certificate mismatch"));
    }
    // The source reads the challenge the real target generated, so the signed
    // request it produces is valid for the target's connection.
    let (kind, body) = relay_read_frame(&mut outgoing).await?;
    if kind != 3 {
        return Err(std::io::Error::other(
            "relay: target did not send a challenge",
        ));
    }
    relay_write_frame(&mut incoming, kind, &body).await?;

    let (kind, body) = relay_read_frame(&mut incoming).await?;
    let wait = delay_ms.load(Ordering::SeqCst);
    if wait > 0 {
        tokio::time::sleep(Duration::from_millis(wait)).await;
    }
    relay_write_frame(&mut outgoing, kind, &body).await?;
    let mode = mode.load(Ordering::SeqCst);

    if kind == 1 {
        if mode & RELAY_DUPLICATE != 0 {
            if let Some(request) = decode_request(&body) {
                let stats = stats.clone();
                let config = relay_client_config(source, real_peers);
                tokio::spawn(async move {
                    let mut factory = AuthenticatedTcpNetworkFactory::new(config);
                    let mut network = factory
                        .new_client(target, &BasicNode::new(format!("relay-{source}")))
                        .await;
                    if network
                        .call_for_test(request, Duration::from_secs(5))
                        .await
                        .is_ok()
                    {
                        stats.reoriginated.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        }
        if mode & RELAY_REPLAY != 0 {
            let stats = stats.clone();
            let body = body.clone();
            tokio::spawn(async move {
                if replay_frame(source, target, target_address, &body)
                    .await
                    .is_ok()
                {
                    stats.replayed.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
        let (kind, body) = relay_read_frame(&mut outgoing).await?;
        relay_write_frame(&mut incoming, kind, &body).await?;
        stats.forwarded.fetch_add(1, Ordering::SeqCst);
        let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
    } else {
        // Snapshot manifests stream chunks in both directions; the relay does
        // not duplicate them, it only carries them after the first frame.
        let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_process_mtls_network_partition() {
    // Avoid inheriting the child selector into unrelated invocations.
    if std::env::var_os(CHILD_ENV).is_some() {
        panic!("partition parent cannot be a child");
    }
    // Three real processes with mTLS and 2250-3000 ms election timeouts (see the
    // child config for why they are not OpenRaft's 150-300 ms defaults): only one
    // such test may run at a time.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_process_mtls_leader_kill_elects_a_replacement_and_the_dead_leader_catches_up() {
    // This parent kills a leader process with SIGKILL and restarts it; the
    // partition and snapshot-install shapes live in the two tests above. The
    // child entry point is still `TEST_NAME`, whose CHILD_ENV branch runs
    // `child`, so this function is never a child.
    if std::env::var_os(CHILD_ENV).is_some() {
        panic!("leader-kill parent cannot be a child");
    }
    let _gate = super::test_gate::exclusive().await;
    tokio::time::timeout(Duration::from_secs(120), async {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!(
            "yadb-leader-kill-{}-{}",
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
        const BEFORE: [&str; 3] = [
            "leader-kill-before-1",
            "leader-kill-before-2",
            "leader-kill-before-3",
        ];
        for key in BEFORE {
            write(&mut nodes, key).await;
        }
        // Every pre-kill key is acknowledged and applied on all three replicas
        // before the leader dies, so a later loss is a real loss.
        check_values(&mut nodes, &BEFORE).await;

        let old_leader = leader(&mut nodes).await;
        let mut killed = nodes.remove(&old_leader).unwrap();
        killed.sigkill();
        drop(killed);
        eprintln!("SIGKILLed leader {old_leader}; the two survivors must elect a replacement");

        // `leader` polls within the same 20 s budget the other tests use.
        let new_leader = leader(&mut nodes).await;
        assert_ne!(
            new_leader, old_leader,
            "the survivors still report the killed leader {old_leader}"
        );
        assert_eq!(nodes.len(), 2);

        let writer = write(&mut nodes, "leader-kill-after").await;
        assert_ne!(writer, old_leader, "a killed process acknowledged a write");
        let all_keys = [BEFORE.as_slice(), &["leader-kill-after"]].concat();
        check_values(&mut nodes, &all_keys).await;

        // A restart with the wrong identity must be refused before storage is
        // opened. Probe the killed node's own directory under incarnation 2.
        let mut probe = start_with_options(
            &dir.0,
            &peers,
            "127.0.0.1:0".parse().unwrap(),
            old_leader,
            99,
            json!({"incarnation": 2, "report_startup_error": true}),
        );
        let report = tokio::time::timeout(Duration::from_secs(10), async {
            while !probe.control.join("ready").exists() {
                assert!(
                    probe.child.try_wait().unwrap().is_none(),
                    "identity probe exited without reporting a refusal"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let report: Value =
                serde_json::from_slice(&std::fs::read(probe.control.join("ready")).unwrap())
                    .unwrap();
            report
        })
        .await
        .expect("identity probe timeout");
        let refusal = report["error"]
            .as_str()
            .unwrap_or_else(|| panic!("wrong-identity restart was not refused: {report}"));
        assert!(
            refusal.contains("replica identity or bootstrap voters mismatch"),
            "wrong-identity restart refused for another reason: {refusal}"
        );
        drop(probe);

        // Restart the killed node with the same identity and directory. The
        // poll-until-caught-up check is what proves the replacement leader
        // replicated the post-kill write to it.
        nodes.insert(old_leader, start(&dir.0, &peers, old_leader, 1));
        ready(nodes.get_mut(&old_leader).unwrap()).await;
        check_values(&mut nodes, &all_keys).await;
        let status = nodes
            .get_mut(&old_leader)
            .unwrap()
            .request(json!({"op":"status"}))
            .await;
        assert_ne!(
            status["leader"].as_u64(),
            Some(old_leader as u64),
            "the restarted old leader is leader again: {status}"
        );
        assert_eq!(
            status["leader"].as_u64(),
            Some(new_leader as u64),
            "the restarted old leader follows a different leader than the elected replacement: {status}"
        );

        drop(nodes);
        // Read the persisted state with the adapter after every process is dead.
        // No acknowledged key is absent from any replica.
        for n in 1..=3 {
            let state =
                DurableMetaStateMachine::open(dir.0.join(format!("node-{n}/state")), &identity(n))
                    .unwrap();
            for &key in &all_keys {
                assert_eq!(state.snapshot().get(key).unwrap().value, key.as_bytes());
            }
        }
    })
    .await
    .expect("leader-kill test exceeded total timeout");
}

fn copy_files(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_process_mtls_delayed_and_duplicated_rpcs_do_not_break_agreement() {
    // Delayed and duplicated Raft RPCs between three real child processes.
    //
    // What this proves: the three children exchange Raft RPCs through a
    // TLS-terminating relay in the parent that (a) holds every request for a
    // fixed delay before forwarding it and (b) re-originates every request as a
    // second authenticated RPC on its own connection, so the receiving child
    // really processes each AppendEntries/Vote twice. A client write still
    // returns a PutOk, at least one follower already held the entry at the
    // instant of the ack (so the ack waited for a majority), every replica
    // applies the entry, and every replica ends with
    // applied_index == last_log_index. It also proves the authentication
    // boundary rejects a *verbatim* duplicate frame: the replay phase writes the
    // same signed frame on a second connection, the target's server challenge
    // no longer matches, and the child's rejected-envelope counter rises while
    // its Raft handler is never entered a second time.
    //
    // What this does not prove: this is a userspace relay on loopback, not a
    // network. It delays and duplicates whole frames in order; it does not
    // reproduce a real network's loss, reordering, jitter or congestion, and
    // both endpoints are on one host with no MTU, queue or routing behaviour.
    // A multi-host deployment remains unproven.
    if std::env::var_os(CHILD_ENV).is_some() {
        panic!("delayed/duplicated parent cannot be a child");
    }
    let _gate = super::test_gate::exclusive().await;
    tokio::time::timeout(Duration::from_secs(150), async {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!(
            "yadb-delayed-{}-{}",
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
        let real: BTreeMap<u32, SocketAddr> = reservations
            .iter()
            .enumerate()
            .map(|(i, l)| ((i + 1) as u32, l.local_addr().unwrap()))
            .collect();
        drop(reservations);
        // One relay per directed link. Each child's peer map points at the
        // relay for that link, while the child itself binds the real address.
        let mut relays = BTreeMap::new();
        for source in 1..=3 {
            for target in 1..=3 {
                if source != target {
                    let relay = FaultRelay::new(source, target, real[&target], real.clone()).await;
                    relay.set_delay(Duration::from_millis(60));
                    relays.insert((source, target), relay);
                }
            }
        }
        let mut nodes = BTreeMap::new();
        for source in 1..=3 {
            let peers = (1..=3)
                .map(|target| {
                    (
                        target,
                        if target == source {
                            real[&target]
                        } else {
                            relays[&(source, target)].address
                        },
                    )
                })
                .collect();
            nodes.insert(
                source,
                start_with_listener(&dir.0, &peers, real[&source], source, 0),
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

        // Phase 1: 60 ms delay plus a duplicated RPC for every request.
        let (acked_leader, acked_index) = write_and_index(&mut nodes, "delayed-dup-1").await;
        // A log index never regresses, so a follower that already held the entry
        // when the leader acknowledged still holds it now. At least one
        // follower must, because OpenRaft only commits (and therefore only
        // acknowledges) after a majority matched the entry.
        let mut holders = Vec::new();
        for (id, process) in nodes.iter_mut() {
            if *id == acked_leader {
                continue;
            }
            let status = process.request(json!({"op":"status"})).await;
            if status["last_log_index"]
                .as_u64()
                .is_some_and(|last| last >= acked_index)
            {
                holders.push(*id);
            }
        }
        assert!(
            !holders.is_empty(),
            "leader {acked_leader} acknowledged index {acked_index} without a follower holding it"
        );
        eprintln!(
            "write acknowledged at index {acked_index} by node {acked_leader}; \
             follower(s) {holders:?} already held it"
        );
        wait_applied_at_least(&mut nodes, acked_index).await;
        check_values(&mut nodes, &["delayed-dup-1"]).await;
        write(&mut nodes, "delayed-dup-2").await;
        check_values(&mut nodes, &["delayed-dup-1", "delayed-dup-2"]).await;
        wait_applied_caught_up(&mut nodes).await;

        // The relay really re-originated RPCs, and the receiving children saw
        // the repeated entry batches at the Raft handler.
        let duplicates = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let duplicates = duplicate_appends(&mut nodes).await;
                if duplicates > 0 {
                    return duplicates;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no duplicated append reached any child");
        let reoriginated: usize = relays.values().map(FaultRelay::reoriginated).sum();
        eprintln!(
            "delay+duplicate: {duplicates} repeated append batches observed, \
             {reoriginated} RPCs re-originated by the relays"
        );
        assert!(duplicates > 0);

        // Phase 2 (inversion): the relay writes the *same signed frame* on a
        // second connection. The frame is bound to the first connection's
        // server challenge, so the target must reject it before the handler.
        // The rejected-envelope counter is the observable that catches a
        // protocol that accepted a replayed frame; the Raft handler counter is
        // the observable that catches a double apply.
        // Let a duplicate spawned just before the mode switch land before the
        // baseline is recorded; otherwise an in-flight duplicate could be
        // attributed to the replay phase.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let duplicates_before_replay = duplicate_appends(&mut nodes).await;
        let rejected_before = rejected_envelopes(&mut nodes).await;
        for relay in relays.values() {
            relay.set_mode(RELAY_REPLAY);
        }
        write(&mut nodes, "delayed-replay").await;
        let rejected_after = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let rejected = rejected_envelopes(&mut nodes).await;
                if rejected > rejected_before {
                    return rejected;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no verbatim replay was rejected by any child");
        check_values(
            &mut nodes,
            &["delayed-dup-1", "delayed-dup-2", "delayed-replay"],
        )
        .await;
        wait_applied_caught_up(&mut nodes).await;
        // A verbatim replay never reaches the Raft handler, so the duplicate
        // counter must not move during this phase.
        assert_eq!(
            duplicate_appends(&mut nodes).await,
            duplicates_before_replay,
            "a replayed frame reached the Raft handler instead of being rejected"
        );
        eprintln!(
            "verbatim replays rejected before the Raft handler: {} -> {}",
            rejected_before, rejected_after
        );

        // Phase 3: remove the delay and duplication. The cluster must still
        // agree; this is the pass-through control, not the inversion.
        for relay in relays.values() {
            relay.set_mode(0);
            relay.set_delay(Duration::ZERO);
        }
        write(&mut nodes, "delayed-clean").await;
        check_values(
            &mut nodes,
            &[
                "delayed-dup-1",
                "delayed-dup-2",
                "delayed-replay",
                "delayed-clean",
            ],
        )
        .await;
        wait_applied_caught_up(&mut nodes).await;

        drop(nodes);
        for (_, relay) in relays {
            relay.task.abort();
            let mut relay = relay;
            let _ = (&mut relay.task).await;
        }

        // Read the durable state with the adapter after every process is dead.
        for n in 1..=3 {
            let state =
                DurableMetaStateMachine::open(dir.0.join(format!("node-{n}/state")), &identity(n))
                    .unwrap();
            for key in [
                "delayed-dup-1",
                "delayed-dup-2",
                "delayed-replay",
                "delayed-clean",
            ] {
                assert_eq!(state.snapshot().get(key).unwrap().value, key.as_bytes());
            }
        }

        // Inversion, part two: a receiver that applied a duplicated entry
        // instead of ignoring it. The durable state machine requires
        // consecutive log ids, so applying the same entry twice fails on the
        // second apply. That is the observable the healthy run asserts by
        // requiring applied_index == last_log_index: a double apply cannot be
        // silent here.
        let copy = dir.0.join("inversion-state");
        copy_files(&dir.0.join("node-1/state"), &copy);
        let mut state = DurableMetaStateMachine::open(&copy, &identity(1)).unwrap();
        let (applied, _) = state.applied_state().await.unwrap();
        let next = applied.map(|id| id.index + 1).unwrap_or(0);
        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), next),
            payload: EntryPayload::Normal(KvCommand::Put {
                key: "duplicate-apply".into(),
                value: b"once".to_vec(),
            }),
        };
        state.apply(vec![entry.clone()]).await.unwrap();
        match state.apply(vec![entry]).await {
            Ok(_) => panic!("the durable state machine silently applied a duplicated entry twice"),
            Err(error) => assert!(
                format!("{error:?}").contains("not consecutive"),
                "duplicated apply failed for an unexpected reason: {error:?}"
            ),
        }
    })
    .await
    .expect("delayed/duplicated test exceeded total timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_process_mtls_power_loss_after_ack_keeps_acknowledged_writes() {
    // Acknowledged writes survive SIGKILL, read back from the children's own
    // files after every process is dead.
    //
    // What this proves: a client write is acknowledged only after the leader
    // committed it (OpenRaft's client_write contract) and the child's apply
    // path wrote and `sync_data`'d the delta frame before returning; after
    // SIGKILLing all three children (asserted to be signal 9, no graceful
    // shutdown path), the parent reopens each replica's log and state machine
    // from disk and every acknowledged key is there. The recovered log's last
    // index still covers the state machine's applied index, so no applied entry
    // is missing behind a discarded tail. Restarting all three on the same
    // directories then commits a new write, which proves the recovered log is
    // usable rather than merely readable.
    //
    // What this does not prove: SIGKILL is not a power cut. The page cache
    // survives the process, so this cannot distinguish "the child fsynced" from
    // "the kernel still held dirty pages". It does not cover OS panic, device
    // write-cache loss or a filesystem that loses a synced rename. Those need a
    // root-level crash harness (`dm-flakey`/`dm-log-writes`, a forced cache
    // flush, or a libeatmydata-style barrier) and stay with the separate
    // durability gates; the TODO item therefore remains partially open.
    if std::env::var_os(CHILD_ENV).is_some() {
        panic!("power-loss parent cannot be a child");
    }
    let _gate = super::test_gate::exclusive().await;
    tokio::time::timeout(Duration::from_secs(150), async {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!(
            "yadb-power-loss-{}-{}",
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
        const KEYS: [&str; 3] = ["power-ack-1", "power-ack-2", "power-ack-3"];
        let mut last_leader = None;
        for key in KEYS {
            last_leader = Some(write(&mut nodes, key).await);
        }
        // `check_values` reads the state image and delta file directly, so by
        // the time it returns each replica has written the applied value to its
        // own files. That is the child's write, not the parent's.
        check_values(&mut nodes, &KEYS).await;
        wait_applied_caught_up(&mut nodes).await;
        let acking_leader = last_leader.expect("no write was acknowledged");

        for (id, process) in nodes.iter_mut() {
            process.sigkill();
            eprintln!("SIGKILLed node {id}; no shutdown path ran");
        }
        drop(nodes);

        // Every process is dead. Whatever is in these files now was written and
        // synced by the child before it died; nothing in this test flushes,
        // syncs or repairs them from the parent.
        for n in 1..=3 {
            let log_dir = dir.0.join(format!("node-{n}/log"));
            let mut log = DurableOpenRaftLog::open(&log_dir, &identity(n)).unwrap();
            if let Some(repair) = log.repaired_tail() {
                // An unacknowledged append cut short by the kill is allowed to
                // be discarded, but it must never be one the client saw acked.
                eprintln!("node {n} discarded a torn unacknowledged tail: {repair:?}");
                assert_ne!(
                    n, acking_leader,
                    "the node that acknowledged the last write had a torn log tail"
                );
            }
            let log_state = log.get_log_state().await.unwrap();
            let last_log = log_state.last_log_id.unwrap_or_else(|| {
                panic!("node {n} recovered an empty log after an acknowledged write")
            });
            let mut state =
                DurableMetaStateMachine::open(dir.0.join(format!("node-{n}/state")), &identity(n))
                    .unwrap();
            let (applied, _) = state.applied_state().await.unwrap();
            let applied = applied.unwrap_or_else(|| panic!("node {n} recovered no applied state"));
            assert!(
                applied.index <= last_log.index,
                "node {n} recovered applied index {} past its log end {}",
                applied.index,
                last_log.index
            );
            for key in KEYS {
                assert_eq!(
                    state.snapshot().get(key).unwrap().value,
                    key.as_bytes(),
                    "node {n} lost acknowledged key {key} after SIGKILL"
                );
            }
        }

        // The recovered log is not merely readable: the same three replicas
        // restart on their own directories, elect, and commit a new entry.
        let mut nodes: BTreeMap<_, _> = (1..=3).map(|n| (n, start(&dir.0, &peers, n, 1))).collect();
        for node in nodes.values_mut() {
            ready(node).await;
        }
        write(&mut nodes, "power-after-restart").await;
        check_values(
            &mut nodes,
            &[
                "power-ack-1",
                "power-ack-2",
                "power-ack-3",
                "power-after-restart",
            ],
        )
        .await;
        wait_applied_caught_up(&mut nodes).await;
        drop(nodes);
        for n in 1..=3 {
            let state =
                DurableMetaStateMachine::open(dir.0.join(format!("node-{n}/state")), &identity(n))
                    .unwrap();
            for key in KEYS.iter().chain(std::iter::once(&"power-after-restart")) {
                assert_eq!(state.snapshot().get(*key).unwrap().value, key.as_bytes());
            }
        }
    })
    .await
    .expect("power-loss test exceeded total timeout");
}
