//! Consistent reads against real daemons across a group boundary.
//!
//! Three nodes run three singleton Raft groups. Project `alpha` maps to group 2
//! whose preferred (and actual) leader is node 2, so a request that arrives at
//! node 3 is a request that arrives at a *follower* of the group that owns the
//! data. Four contracts are exercised there:
//!
//! 1. `consistent=true` on a follower never answers from the serving node's
//!    stale applied state.
//! 2. The same read without the flag may answer from that local state; the test
//!    records which answer it got.
//! 3. A consistent read whose quorum barrier cannot be taken fails instead of
//!    falling back to local state.
//! 4. One consistent multi-key read never mixes snapshots.
//!
//! Staleness is made observable instead of timed. The test owns a TCP relay in
//! front of node 3's group-2 Raft transport: closing it severs replication to
//! node 3 while leaving every client listener up. The proof that node 3 really
//! is stale is its own `last_applied` index for group 2, asserted unchanged
//! across a write the leader applies, plus the version markers below.
//!
//! * an S3 object whose ETag changes with its body. A plain `HEAD` takes no
//!   lease and does not forward, so node 3 answers it from its own applied
//!   manifest; the consistent `HEAD` forwards to the group leader.
//! * a SQL script whose two statements must observe one snapshot. A write is
//!   committed while the first (slow) statement runs, so a read that took a
//!   second snapshot for the second statement would return a mixture.
//!
//! The relay is per node: `new_client` dials the address in the *calling*
//! node's own manifest, so the leader's manifest points node 1 and node 3 at
//! their relays while each node's own entry still matches its listener (which
//! the manifest validator requires). A relay is a transparent byte pump, so
//! mTLS still runs end to end between the real nodes.
//!
//! Every daemon is killed by its `Drop` guard, including on panic. Ports are
//! reserved ephemerally (`127.0.0.1:0`), so this file cannot collide with the
//! fixed bases used by other suites.
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, Instant},
};
use yadb_metakv::openraft_tls::MutualTls;

const SUBJECT: &str = "tester";
const SUBJECT_SECRET: &str = "integration-test-subject-secret";
const RAFT_TOKEN: &str = "integration-test-token";

const OBJECT: &str = "item";
/// The two object versions differ in length as well as content, so a stale
/// answer is visible in `Content-Length` even without trusting the ETag.
const STALE_BODY: &[u8] = b"stale-v1";
const COMMITTED_BODY: &[u8] = b"committed-v2-longer";

/// Serialises the daemons. Ports are reserved, dropped and only then bound by a
/// child, so a second test in this binary could take one in between.
static DAEMON_TESTS: Mutex<()> = Mutex::new(());

fn serialize_daemon_tests() -> MutexGuard<'static, ()> {
    DAEMON_TESTS.lock().unwrap_or_else(|error| error.into_inner())
}

fn subject_signature(subject: &str) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, SUBJECT_SECRET.as_bytes());
    ring::hmac::sign(&key, subject.as_bytes())
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("daemon artifacts: {}", self.0.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn config(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

fn test_root(tag: &str) -> Dir {
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    Dir(root.canonicalize().unwrap())
}

fn cert_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../yadb-metakv/testdata/tls")
        .canonicalize()
        .unwrap()
}

/// Reserves `n` ephemeral ports, returning the listeners still bound so the
/// caller can drop them immediately before spawning the children.
fn reserve(n: usize) -> (Vec<TcpListener>, Vec<SocketAddr>) {
    let listeners: Vec<_> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addrs = listeners
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    (listeners, addrs)
}

/// A transparent TCP relay in front of one Raft transport address.
///
/// It exists so a single node can be cut off from its group's replication while
/// every HTTP listener on that node stays up. A closed relay accepts and drops
/// (so the peer sees a reset rather than hanging in `connect`) and shuts down
/// the connections it already carries.
struct Relay {
    addr: SocketAddr,
    open: Arc<AtomicBool>,
    streams: Arc<Mutex<Vec<TcpStream>>>,
}

impl Relay {
    fn new(target: SocketAddr) -> Relay {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let open = Arc::new(AtomicBool::new(true));
        let streams: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        let (flag, held) = (open.clone(), streams.clone());
        std::thread::spawn(move || {
            for incoming in listener.incoming() {
                let Ok(client) = incoming else { continue };
                if !flag.load(Ordering::SeqCst) {
                    drop(client);
                    continue;
                }
                let Ok(upstream) = TcpStream::connect_timeout(&target, Duration::from_secs(2))
                else {
                    continue;
                };
                let _ = client.set_nodelay(true);
                let _ = upstream.set_nodelay(true);
                let (Ok(keep_client), Ok(keep_upstream), Ok(mut down), Ok(mut up)) = (
                    client.try_clone(),
                    upstream.try_clone(),
                    client.try_clone(),
                    upstream.try_clone(),
                ) else {
                    continue;
                };
                {
                    let mut registry = held.lock().unwrap();
                    registry.push(keep_client);
                    registry.push(keep_upstream);
                }
                // Re-check after registering: `close` may have run between the
                // accept and this point, and a connection that slipped in must
                // not survive the cut.
                if !flag.load(Ordering::SeqCst) {
                    for stream in held.lock().unwrap().iter() {
                        let _ = stream.shutdown(Shutdown::Both);
                    }
                    continue;
                }
                std::thread::spawn(move || {
                    let mut sink = upstream;
                    let _ = std::io::copy(&mut down, &mut sink);
                    let _ = sink.shutdown(Shutdown::Both);
                });
                std::thread::spawn(move || {
                    let mut sink = client;
                    let _ = std::io::copy(&mut up, &mut sink);
                    let _ = sink.shutdown(Shutdown::Both);
                });
            }
        });
        Relay {
            addr,
            open,
            streams,
        }
    }

    fn close(&self) {
        self.open.store(false, Ordering::SeqCst);
        for stream in self.streams.lock().unwrap().iter() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn reopen(&self) {
        self.open.store(true, Ordering::SeqCst);
    }
}

fn spawn(
    data: &Path,
    manifest: &Path,
    node: u32,
    peers: &str,
    public: SocketAddr,
    log: &Path,
) -> Daemon {
    let stderr = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_yadbd"));
    cmd.args([
        "--listen",
        &public.to_string(),
        "--data-dir",
        data.to_str().unwrap(),
        "--node-id",
        &node.to_string(),
        "--metadata-runtime",
        "openraft",
        "--openraft-config",
        manifest.to_str().unwrap(),
        "--peers",
        peers,
        "--raft-token",
        RAFT_TOKEN,
        "--subject-secret",
        SUBJECT_SECRET,
        "--admin-subject",
        SUBJECT,
    ])
    .env_remove("YADB_SUBJECT_SECRET")
    .env_remove("YADB_RAFT_TOKEN");
    Daemon(
        cmd.stdout(Stdio::from(stderr.try_clone().unwrap()))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap(),
    )
}

/// Sends one request with the suite's signed subject. Returns status, the raw
/// header block and the body.
fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
) -> (u16, String, Vec<u8>) {
    let identity = format!(
        "x-yadb-subject: {SUBJECT}\r\nx-yadb-subject-signature: {}\r\n",
        subject_signature(SUBJECT)
    );
    let mut socket = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(socket, "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{identity}\r\n", body.len()).unwrap();
    socket.write_all(body).unwrap();
    let mut response = Vec::new();
    socket
        .take(64 * 1024 * 1024)
        .read_to_end(&mut response)
        .unwrap();
    let split = response
        .windows(4)
        .position(|b| b == b"\r\n\r\n")
        .expect("HTTP headers missing");
    let header = std::str::from_utf8(&response[..split]).unwrap().to_string();
    let status = header
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        !header
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked"),
        "unexpected chunked response: {header}"
    );
    (status, header, response[split + 4..].to_vec())
}

fn ok(addr: SocketAddr, method: &str, path: &str, body: &[u8], ty: &str) -> Vec<u8> {
    let (status, _, response) = request(addr, method, path, body, ty);
    assert_eq!(
        status,
        200,
        "{method} {path}: {}",
        String::from_utf8_lossy(&response)
    );
    response
}

/// Polls until `method path` answers `200`, so the test never races the control
/// watch thread that materializes a project's group or its pool slots.
fn await_ok(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    ty: &str,
    what: &str,
) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let (status, _, response) = request(addr, method, path, body, ty);
        if status == 200 {
            return response;
        }
        assert!(
            Instant::now() < deadline,
            "{what} never became available: {status} {}",
            String::from_utf8_lossy(&response)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn header_value<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim().eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// The node's locally applied index for one group, parsed out of
/// `/openraft/status`. `last_applied` is rendered with `Debug`, so the index is
/// the number after `index: `; an unapplied replica renders `None`.
fn applied_index(addr: SocketAddr, group: u64) -> u64 {
    let body = ok(addr, "GET", "/openraft/status", b"", "application/json");
    let value: Value = serde_json::from_slice(&body).unwrap();
    let observation = value["local_observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["group"].as_u64() == Some(group))
        .unwrap_or_else(|| panic!("group {group} not observed on {addr}"));
    let text = observation["last_applied"]
        .as_str()
        .unwrap_or_else(|| panic!("last_applied is not a string: {observation}"));
    if text == "None" {
        return 0;
    }
    let start = text
        .find("index: ")
        .unwrap_or_else(|| panic!("no index in last_applied: {text}"))
        + "index: ".len();
    text[start..]
        .trim_end_matches(|c: char| !c.is_ascii_digit())
        .parse()
        .unwrap_or_else(|_| panic!("unparseable last_applied: {text}"))
}

fn sql(addr: SocketAddr, statement: &str, consistent: bool) -> (u16, String, Vec<u8>) {
    let mut payload = json!({"sql": statement});
    if consistent {
        payload["consistent"] = json!(true);
    }
    request(
        addr,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&payload).unwrap(),
        "application/json",
    )
}

fn head(addr: SocketAddr, consistent: bool) -> (u16, String, Vec<u8>) {
    let path = if consistent {
        format!("/s3/alpha/store/bucket/{OBJECT}?consistent=true")
    } else {
        format!("/s3/alpha/store/bucket/{OBJECT}")
    };
    request(addr, "HEAD", &path, b"", "application/octet-stream")
}

fn put_object(addr: SocketAddr, body: &[u8]) -> String {
    let response = ok(
        addr,
        "PUT",
        &format!("/s3/alpha/store/bucket/{OBJECT}"),
        body,
        "application/octet-stream",
    );
    let value: Value = serde_json::from_slice(&response).unwrap();
    value["etag"]
        .as_str()
        .unwrap_or_else(|| panic!("PUT response has no etag: {value}"))
        .to_string()
}

/// Reads `(id, v)` pairs out of a `SELECT id, v FROM t ORDER BY id` answer.
fn rows_of(body: &[u8]) -> Vec<(i64, i64)> {
    let value: Value = serde_json::from_slice(body)
        .unwrap_or_else(|_| panic!("not JSON: {}", String::from_utf8_lossy(body)));
    value["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows: {value}"))
        .iter()
        .map(|row| {
            (
                row[0].as_i64().expect("id is an integer"),
                row[1].as_i64().expect("v is an integer"),
            )
        })
        .collect()
}

fn ready_cluster(daemons: &mut [Daemon], public: &[SocketAddr]) {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        for daemon in daemons.iter_mut() {
            assert!(
                daemon.0.try_wait().unwrap().is_none(),
                "a node exited during startup"
            );
        }
        let mut all_ready = true;
        for &address in public {
            if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err() {
                all_ready = false;
                continue;
            }
            let (status, _, body) =
                request(address, "GET", "/openraft/status", b"", "application/json");
            if status != 200 {
                all_ready = false;
                continue;
            }
            let status: Value = serde_json::from_slice(&body).unwrap();
            let groups = match status["local_observations"].as_array() {
                Some(groups) => groups,
                None => {
                    all_ready = false;
                    continue;
                }
            };
            // `preference_met` is true only once the group has a leader and it
            // is the preferred one, which pins node N as group N's leader.
            if groups.len() != 3 || groups.iter().any(|g| g["preference_met"] != json!(true)) {
                all_ready = false;
            }
        }
        if all_ready {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cluster did not reach its preferred leaders"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

const CONTROL_YAML: &str = r#"apiVersion: yadb.io/v1alpha1
kind: StoragePool
metadata: {name: data}
spec: {erasure: {scheme: mirror, replicas: 3}, failureDomains: 3}
---
apiVersion: yadb.io/v1alpha1
kind: Project
metadata: {name: alpha}
spec: {quotas: {maxStorageBytes: 100000000, maxIops: 1000}, members: [{subject: tester, role: readWrite}]}
---
apiVersion: yadb.io/v1alpha1
kind: Database
metadata: {name: db, project: alpha}
spec: {walPool: data, dataPool: data, isolation: shared, shards: 2}
---
apiVersion: yadb.io/v1alpha1
kind: ObjectStore
metadata: {name: store, project: alpha}
spec: {pool: data, isolation: shared, buckets: [bucket]}
"#;

/// One test, because the phases share a cluster and a relay:
///
/// * phase A cuts node 3 off from group 2 and reads through it (contracts 1, 2
///   and 4),
/// * phase B cuts the group-2 leader off from its quorum and reads through the
///   leader (contract 3).
#[test]
fn consistent_reads_never_serve_local_state_across_a_group_boundary() {
    let _serial = serialize_daemon_tests();
    let dir = test_root("consistent-reads");
    let certs = cert_dir();
    // Three public, nine Raft transport (three groups x three nodes).
    let (reservations, addrs) = reserve(12);
    let public: Vec<SocketAddr> = addrs[..3].to_vec();
    let transport = &addrs[3..12];

    let mut signing = Vec::new();
    for node in 1..=3usize {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let path = dir.0.join(format!("signing-{node}.der"));
        std::fs::write(&path, pkcs8.as_ref()).unwrap();
        let cert = std::fs::read(certs.join(format!("node-{node}.test.cert.der"))).unwrap();
        signing.push((
            path,
            pair.public_key().as_ref().to_vec(),
            MutualTls::fingerprint(&cert),
        ));
    }

    // Group 2 is project `alpha`'s group. Nodes 1 and 3 advertise a relay
    // address to their peers so the test can cut group-2 replication to either
    // of them without touching its client listener. Node 2 (the leader) is
    // advertised directly. `new_client` dials the address in the *calling*
    // node's own manifest, so a relay only has to appear in the other nodes'
    // manifests; each node's own entry must still equal its listener, which is
    // what the manifest validator checks.
    let relay1 = Relay::new(transport[3]);
    let relay3 = Relay::new(transport[5]);
    let group2_peer = |node: usize| -> SocketAddr {
        match node {
            1 => relay1.addr,
            2 => transport[4],
            _ => relay3.addr,
        }
    };
    let group2_listen = |node: usize| -> SocketAddr { transport[3 + node - 1] };
    let transport_addr = |group: usize, node: usize| -> SocketAddr {
        if group == 2 {
            group2_peer(node)
        } else {
            transport[(group - 1) * 3 + node - 1]
        }
    };

    let mut manifests = Vec::new();
    for node in 1..=3usize {
        let metadata = dir.0.join(format!("metadata-{node}"));
        std::fs::create_dir(&metadata).unwrap();
        let mut paths = Vec::new();
        for group in 1..=3usize {
            let peers: Vec<_> = (1..=3usize)
                .map(|peer| {
                    let address = if group == 2 && peer == node {
                        // The local entry must match this node's listener.
                        group2_listen(node)
                    } else {
                        transport_addr(group, peer)
                    };
                    json!({"id":peer,"address":address,
                    "public_key":signing[peer-1].1,"server_name":format!("node-{peer}.test"),
                    "cert_sha256":signing[peer-1].2})
                })
                .collect();
            let path = dir.0.join(format!("node-{node}-group-{group}.json"));
            config(
                &path,
                &json!({"version":1,"root":metadata,"mode":"create","initialize":node==group,
                "cluster":vec![93;16],"group":group,"incarnation":1,"node":node,
                "listen":transport[(group - 1) * 3 + node - 1],"signing_key_der":signing[node-1].0,
                "preferred_leader":group,
                // Non-preferred replicas wait 30 s before campaigning, so a
                // relay that cuts a follower off for a few seconds cannot
                // trigger an election that would change the leader mid-test.
                "raft_timing":{"heartbeat_ms":750,"election_min_ms":2250,
                "nonpreferred_election_min_ms":30000,"election_max_ms":35000},
                "tls":{"roots_der":[certs.join("ca.der")],
                "chain_der":[certs.join(format!("node-{node}.test.cert.der"))],
                "key_der":certs.join(format!("node-{node}.test.key.der"))},"peers":peers}),
            );
            paths.push(path);
        }
        let manifest = dir.0.join(format!("node-{node}-multi.json"));
        config(
            &manifest,
            &json!({"version":2,"control_group":1,"project_groups":{"1":2,"2":3},"groups":paths}),
        );
        manifests.push(manifest);
    }

    let public_map = (1..=3usize)
        .map(|node| format!("{node}={}", public[node - 1]))
        .collect::<Vec<_>>()
        .join(",");
    drop(reservations);
    let mut daemons: Vec<_> = (1..=3usize)
        .map(|node| {
            spawn(
                &dir.0.join(format!("client-data-{node}")),
                &manifests[node - 1],
                node as u32,
                &public_map,
                public[node - 1],
                &dir.0.join(format!("node-{node}.log")),
            )
        })
        .collect();
    ready_cluster(&mut daemons, &public);
    let (node1, node2, node3) = (public[0], public[1], public[2]);

    // --- setup -----------------------------------------------------------------
    ok(
        node1,
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml": CONTROL_YAML})).unwrap(),
        "application/json",
    );
    await_ok(
        node2,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&json!({"sql":"CREATE TABLE t (id INT PRIMARY KEY, v INT)"})).unwrap(),
        "application/json",
        "CREATE TABLE through the group-2 leader",
    );
    let seed = format!(
        "INSERT INTO t VALUES {}",
        (1..=8i64)
            .map(|id| format!("({id},1)"))
            .collect::<Vec<_>>()
            .join(",")
    );
    ok(
        node2,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&json!({"sql": seed})).unwrap(),
        "application/json",
    );
    let etag_stale = put_object(node2, STALE_BODY);

    // Wait until node 3 holds the stale object locally, otherwise the plain
    // HEAD below would fail for a reason other than staleness.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, headers, _) = head(node3, false);
        if status == 200 && header_value(&headers, "etag") == Some(etag_stale.as_str()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node 3 never applied the seeded object: {status} {headers}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // --- phase A: a stale follower must not answer consistent reads ------------
    relay3.close();
    std::thread::sleep(Duration::from_millis(500));
    let stale_applied = applied_index(node3, 2);

    let etag_committed = put_object(node2, COMMITTED_BODY);
    ok(
        node2,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&json!({"sql":"UPDATE t SET v = 2"})).unwrap(),
        "application/json",
    );
    let leader_applied = applied_index(node2, 2);
    let observed = applied_index(node3, 2);
    assert!(
        observed < leader_applied,
        "node 3 ({observed}) did not stay behind the leader ({leader_applied}); \
         its group-2 replication was not actually cut"
    );
    assert_eq!(
        observed, stale_applied,
        "node 3 applied group-2 entries after its relay closed \
         ({stale_applied} -> {observed}); the staleness below would not be provable"
    );

    // Contract 1: the consistent read on the stale follower answers with the
    // committed state, not the state this node has applied.
    let (status, headers, body) = head(node3, true);
    assert_eq!(
        status,
        200,
        "consistent HEAD through node 3: {}",
        String::from_utf8_lossy(&body)
    );
    let consistent_etag = header_value(&headers, "etag").map(str::to_string);
    assert_eq!(
        consistent_etag.as_deref(),
        Some(etag_committed.as_str()),
        "consistent HEAD through a stale follower returned the local (stale) \
         manifest: {headers}"
    );
    let committed_length = COMMITTED_BODY.len().to_string();
    assert_eq!(
        header_value(&headers, "content-length"),
        Some(committed_length.as_str()),
        "consistent HEAD through a stale follower returned the stale length: {headers}"
    );

    // Contract 1, SQL path: the same committed values are visible through a
    // consistent SELECT that arrives at the stale follower.
    let (status, _, body) = sql(node3, "SELECT id, v FROM t ORDER BY id", true);
    assert_eq!(
        status,
        200,
        "consistent SELECT through node 3: {}",
        String::from_utf8_lossy(&body)
    );
    let versions: Vec<i64> = rows_of(&body).iter().map(|(_, v)| *v).collect();
    assert!(
        versions.iter().all(|v| *v == 2),
        "consistent SELECT through a stale follower returned the follower's \
         applied version: {versions:?}"
    );

    // Contract 2: the same read without the flag. This one is answered from the
    // serving node's own applied state (a plain HEAD reads no fragments and
    // takes no lease), so it must report the *stale* object; the test records
    // exactly which answer it got.
    let (status, headers, body) = head(node3, false);
    assert_eq!(
        status,
        200,
        "plain HEAD through node 3: {}",
        String::from_utf8_lossy(&body)
    );
    let plain_etag = header_value(&headers, "etag").map(str::to_string);
    eprintln!(
        "plain HEAD through stale node 3 returned etag {plain_etag:?} length {:?}; \
         consistent HEAD returned etag {consistent_etag:?}",
        header_value(&headers, "content-length")
    );
    assert_eq!(
        plain_etag.as_deref(),
        Some(etag_stale.as_str()),
        "the default read is documented to answer from local applied state; it \
         returned {plain_etag:?} instead of the stale {etag_stale:?}"
    );

    // --- phase A2: one consistent multi-statement read, one snapshot ----------
    //
    // The first statement scans a large table so the second statement runs well
    // after the snapshot was taken. A concurrent write commits inside that
    // window. The whole script must still observe the snapshot taken before the
    // write; a read that took a fresh snapshot (or read local state) for the
    // second statement would return the new value.
    await_ok(
        node2,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&json!({"sql":"CREATE TABLE big (id INT PRIMARY KEY, v TEXT)"}))
            .unwrap(),
        "application/json",
        "CREATE TABLE big through the group-2 leader",
    );
    for batch in 0..40i64 {
        let values = (0..500i64)
            .map(|i| {
                let id = batch * 500 + i;
                format!("({id},'{}')", "x".repeat(80))
            })
            .collect::<Vec<_>>()
            .join(",");
        ok(
            node2,
            "POST",
            "/db/alpha/db/query",
            &serde_json::to_vec(&json!({"sql": format!("INSERT INTO big VALUES {values}")}))
                .unwrap(),
            "application/json",
        );
    }
    // A second, small table holds the value the concurrent write changes.
    await_ok(
        node2,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&json!({"sql":"CREATE TABLE mark (id INT PRIMARY KEY, v INT); INSERT INTO mark VALUES (1,1)"}))
            .unwrap(),
        "application/json",
        "CREATE TABLE mark through the group-2 leader",
    );

    let writer = std::thread::spawn(move || {
        // Let the read take its snapshot first: the snapshot is taken within a
        // millisecond of the request, while the first statement runs for far
        // longer than this delay.
        std::thread::sleep(Duration::from_millis(50));
        let (status, _, body) = sql(node2, "UPDATE mark SET v = 2 WHERE id = 1", false);
        assert_eq!(
            status,
            200,
            "concurrent UPDATE: {}",
            String::from_utf8_lossy(&body)
        );
    });
    // Three slow statements before the observed one widen the window the
    // concurrent write has to land in, without any extra setup cost.
    let (status, _, body) = sql(
        node2,
        "SELECT * FROM big; SELECT * FROM big; SELECT * FROM big; SELECT v FROM mark WHERE id = 1",
        true,
    );
    assert_eq!(
        status,
        200,
        "consistent multi-statement read: {}",
        String::from_utf8_lossy(&body)
    );
    writer.join().unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    let observed_mark = value["rows"][0][0].as_i64().unwrap();
    assert_eq!(
        observed_mark, 1,
        "the multi-statement read observed the concurrent write ({observed_mark}); \
         its second statement did not come from the snapshot taken before it"
    );
    // Prove the write did commit: a fresh consistent read now sees it.
    let (status, _, body) = sql(node2, "SELECT v FROM mark WHERE id = 1", true);
    assert_eq!(status, 200);
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        value["rows"][0][0].as_i64(),
        Some(2),
        "the concurrent write did not commit: {value}"
    );

    // Restore replication and wait for node 3 to catch up, so phase B starts
    // from a cluster whose followers are current.
    relay3.reopen();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, headers, _) = head(node3, false);
        if status == 200 && header_value(&headers, "etag") == Some(etag_committed.as_str()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node 3 never caught up after the relay reopened: {status} {headers}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // --- phase B: no quorum means no consistent read, never local data --------
    relay1.close();
    relay3.close();
    std::thread::sleep(Duration::from_millis(200));

    // Contract 3: the group-2 leader can still read its own applied state but
    // cannot confirm it is current, so the consistent read must fail.
    let (status, headers, body) = head(node2, true);
    assert!(
        status >= 400,
        "consistent HEAD without a quorum answered {status}: {headers} {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        header_value(&headers, "etag"),
        None,
        "a failed consistent read still returned a manifest: {headers}"
    );
    eprintln!(
        "consistent HEAD on the quorum-less leader: {status} {}",
        String::from_utf8_lossy(&body)
    );

    // The same bytes *were* available locally: the plain HEAD takes no lease
    // and no barrier, so it answers from the leader's applied manifest. That is
    // the local answer the consistent read refused to serve.
    let (status, headers, body) = head(node2, false);
    assert_eq!(
        status,
        200,
        "plain HEAD on the quorum-less leader: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        header_value(&headers, "etag"),
        Some(etag_committed.as_str()),
        "the plain HEAD should still answer from local applied state: {headers}"
    );
}
