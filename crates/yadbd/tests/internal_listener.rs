//! The public client listener and the optional dedicated internal listener.
//!
//! `--internal-listen` moves the token-authenticated `/internal/...` routes off
//! the client port, so the peer network — not "anything that can reach the
//! client API" — is the bound on who can attempt a remote VDisk RPC. The flag
//! is opt-in: unset keeps the historical single-listener shape, which is the
//! half that a split router gets wrong most easily (the routes must still be
//! reachable there).
//!
//! Both tests run a real daemon. The flag-on test also checks that the routes
//! are *absent* from the public port rather than merely unauthorized: the
//! public 404 must have an empty body, because several handlers answer 404 with
//! a JSON body when a slot is not hosted here, and that would otherwise read as
//! a successful split.
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};
use yadb_metakv::openraft_tls::MutualTls;

const SUBJECT: &str = "tester";
const SUBJECT_SECRET: &str = "integration-test-subject-secret";
const RAFT_TOKEN: &str = "integration-test-token";

/// Serialises the two daemons. Each test reserves ephemeral ports, drops the
/// reservations and only then lets the child bind them, so a concurrent test in
/// this binary could take a port between the two steps.
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

/// Three singleton groups plus the version 2 manifest, and a client data root
/// outside the metadata roots. Returns the manifest path.
fn single_node_manifest(
    dir: &Path,
    cert_dir: &Path,
    addrs: &[SocketAddr],
    tag: &str,
) -> (PathBuf, PathBuf) {
    let metadata = dir.join(format!("metadata-{tag}"));
    std::fs::create_dir(&metadata).unwrap();
    let cert = std::fs::read(cert_dir.join("node-1.test.cert.der")).unwrap();
    let mut paths = Vec::new();
    for group in 1..=3usize {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let key = dir.join(format!("signing-{tag}-{group}.der"));
        std::fs::write(&key, pkcs8.as_ref()).unwrap();
        let path = dir.join(format!("group-{tag}-{group}.json"));
        config(
            &path,
            &json!({"version":1,"root":metadata,"mode":"create","initialize":true,"cluster":vec![91;16],"group":group,"incarnation":1,"node":1,"listen":addrs[group],"signing_key_der":key,
            "tls":{"roots_der":[cert_dir.join("ca.der")],"chain_der":[cert_dir.join("node-1.test.cert.der")],"key_der":cert_dir.join("node-1.test.key.der")},
            "peers":[{"id":1,"address":addrs[group],"public_key":pair.public_key().as_ref(),"server_name":"node-1.test","cert_sha256":MutualTls::fingerprint(&cert)}]}),
        );
        paths.push(path);
    }
    let manifest = dir.join(format!("multi-{tag}.json"));
    config(
        &manifest,
        &json!({"version":2,"control_group":1,"project_groups":{"1":2,"2":3},"groups":paths}),
    );
    (manifest, metadata)
}

fn spawn(
    data: &Path,
    manifest: &Path,
    node: u32,
    peers: &str,
    public: SocketAddr,
    log: &Path,
    extra: &[String],
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
    .args(extra)
    .env_remove("YADB_SUBJECT_SECRET")
    .env_remove("YADB_RAFT_TOKEN");
    Daemon(
        cmd.stdout(Stdio::from(stderr.try_clone().unwrap()))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap(),
    )
}

/// Sends one request with the suite's signed subject and caller-supplied extra
/// headers (each ending in CRLF). Returns status, headers and body.
fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    extra: &str,
) -> (u16, String, Vec<u8>) {
    let identity = format!(
        "x-yadb-subject: {SUBJECT}\r\nx-yadb-subject-signature: {}\r\n",
        subject_signature(SUBJECT)
    );
    let mut socket = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(socket, "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{identity}{extra}\r\n", body.len()).unwrap();
    socket.write_all(body).unwrap();
    let mut response = Vec::new();
    socket
        .take(8 * 1024 * 1024)
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
    ok_with(addr, method, path, body, ty, "")
}

fn ok_with(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    ty: &str,
    extra: &str,
) -> Vec<u8> {
    let (status, _, response) = request(addr, method, path, body, ty, extra);
    assert_eq!(
        status,
        200,
        "{method} {path}: {}",
        String::from_utf8_lossy(&response)
    );
    response
}

fn ready(child: &mut Daemon, addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "daemon exited during startup"
        );
        if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
            let (status, _, body) =
                request(addr, "GET", "/openraft/status", b"", "application/json", "");
            if status == 200 {
                let result: Value = serde_json::from_slice(&body).unwrap();
                if result["local_observations"]
                    .as_array()
                    .is_some_and(|groups| {
                        groups.len() == 3 && groups.iter().all(|g| g["leader_id"] == 1)
                    })
                {
                    return;
                }
            }
        }
        assert!(Instant::now() < deadline, "three group leaders not ready");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Reserves `n` ephemeral ports and returns them with the listeners still bound
/// so the caller can drop them immediately before spawning the child.
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

const POOL_YAML: &str = r#"apiVersion: yadb.io/v1alpha1
kind: StoragePool
metadata: {name: data}
spec: {erasure: {scheme: mirror, replicas: 3}, failureDomains: 3}
"#;

/// The public client surface must be identical in both modes. `/v1/apply` is
/// checked with a real commit, the read-only status routes with a `200`, and the
/// remaining client routes by their method mismatch: a `405` proves the path is
/// still routed, and would become `404` if the split had dropped it.
fn assert_public_surface_intact(public: SocketAddr, pool: &str) {
    for path in ["/healthz", "/raft/status", "/v1/status"] {
        let body = ok(public, "GET", path, b"", "application/json");
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert!(value.is_object(), "{path}: {}", String::from_utf8_lossy(&body));
    }
    let status: Value = serde_json::from_slice(&ok(
        public,
        "GET",
        "/openraft/status",
        b"",
        "application/json",
    ))
    .unwrap();
    assert!(status["local_observations"]
        .as_array()
        .is_some_and(|groups| groups.len() == 3));
    // A real control-plane mutation through the public port, signed as the
    // allowlisted admin subject. A split that accidentally moved `/v1/apply` to
    // the internal listener would answer 404 here.
    let yaml = format!(
        "apiVersion: yadb.io/v1alpha1\nkind: StoragePool\nmetadata: {{name: {pool}}}\nspec: {{erasure: {{scheme: mirror, replicas: 3}}, failureDomains: 3}}\n"
    );
    ok(
        public,
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml": yaml})).unwrap(),
        "application/json",
    );
    for (method, path) in [
        ("GET", "/db/alpha/db/query"),
        ("DELETE", "/block/alpha/disk"),
        ("POST", "/s3/alpha/store/bucket"),
    ] {
        let (status, _, body) = request(public, method, path, b"", "application/json", "");
        assert_eq!(
            status,
            405,
            "{method} {path} must stay routed on the public listener: {}",
            String::from_utf8_lossy(&body)
        );
    }
}

/// Polls `path` on `addr` until it answers `want`, so the test does not race the
/// control-plane watch thread that materializes the pool's local slots.
fn await_status(addr: SocketAddr, path: &str, extra: &str, want: u16) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (status, _, body) = request(addr, "GET", path, b"", "application/json", extra);
        if status == want {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "{path} on {addr}: expected {want}, last {status}: {}",
            String::from_utf8_lossy(&body)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn internal_listener_serves_internal_routes_only() {
    let _serial = serialize_daemon_tests();
    let dir = test_root("internal-listener");
    let certs = cert_dir();
    // public, three Raft transport listeners, internal, S3 wire listener.
    let (reservations, addrs) = reserve(6);
    let public = addrs[0];
    let internal = addrs[4];
    let s3 = addrs[5];
    let (manifest, _) = single_node_manifest(&dir.0, &certs, &addrs, "on");
    let credentials = dir.0.join("protocol-credentials.json");
    std::fs::write(
        &credentials,
        serde_json::to_vec(&json!({"users":{"s3user":{"subject":SUBJECT,"secret":"0123456789abcdef"}}})).unwrap(),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let data = dir.0.join("client-data");
    let log = dir.0.join("daemon.log");
    drop(reservations);
    let mut daemon = spawn(
        &data,
        &manifest,
        1,
        &format!("1={public}"),
        public,
        &log,
        &[
            "--internal-listen".into(),
            internal.to_string(),
            "--s3-listen".into(),
            s3.to_string(),
            "--s3-target".into(),
            "alpha/store".into(),
            "--protocol-credentials".into(),
            credentials.to_str().unwrap().into(),
        ],
    );
    ready(&mut daemon, public);

    // A hosted local slot is what makes `/internal/vdisk/.../stats` answer 200
    // rather than the handler's "slot not hosted here" 404, so create the pool
    // through the public port first.
    ok(
        public,
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml": POOL_YAML})).unwrap(),
        "application/json",
    );

    let token = format!("x-yadb-raft-token: {RAFT_TOKEN}\r\n");
    let stats = "/internal/vdisk/data/0/stats";
    let snapshot = "/internal/control/snapshot";

    // The internal routes do not exist on the client port: `404`, and an empty
    // body so this cannot be confused with a handler that answers 404 for an
    // unhosted slot. The token is irrelevant there — no token, the right token
    // and a wrong token all get the same not-found.
    for (path, extra) in [
        (stats, ""),
        (stats, token.as_str()),
        (stats, "x-yadb-raft-token: wrong\r\n"),
        (snapshot, token.as_str()),
    ] {
        let (status, _, body) = request(public, "GET", path, b"", "application/json", extra);
        assert_eq!(
            status,
            404,
            "{path} must not exist on the public listener: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            body.is_empty(),
            "{path} on the public listener answered a handler, not the router: {}",
            String::from_utf8_lossy(&body)
        );
    }
    // A mutation on the internal port is not a client surface either: the
    // listener carries peer transport only, so it cannot become an
    // unauthenticated path to the control plane.
    let (status, _, _) = request(
        internal,
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml": POOL_YAML})).unwrap(),
        "application/json",
        "",
    );
    assert_eq!(status, 404);
    let (status, _, _) = request(internal, "GET", "/healthz", b"", "application/json", "");
    assert_eq!(status, 404);

    // The same requests work on the dedicated listener, and the token check is
    // still the authentication: the split narrows who can attempt the call, it
    // does not replace the credential.
    for extra in ["", "x-yadb-raft-token: wrong\r\n"] {
        let (status, _, body) = request(internal, "GET", stats, b"", "application/json", extra);
        assert_eq!(
            status,
            401,
            "unauthenticated {stats} on the internal listener: {}",
            String::from_utf8_lossy(&body)
        );
    }
    let body = await_status(internal, stats, &token, 200);
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert!(value["live_blobs"].is_number(), "{value}");
    let body = ok_with(internal, "GET", snapshot, b"", "application/json", &token);
    assert!(serde_json::from_slice::<Value>(&body).unwrap()["revision"].is_number());

    // The client and protocol surfaces are unchanged by the flag.
    assert_public_surface_intact(public, "surface-on");
    // The S3 wire listener is a separate socket with its own serve loop; it must
    // still be up and still refuse an unauthenticated request.
    let (status, _, _) = request(s3, "GET", "/bucket", b"", "application/octet-stream", "");
    assert_eq!(status, 403, "S3 listener must still answer");
}

#[test]
fn without_internal_listener_internal_routes_stay_public() {
    let _serial = serialize_daemon_tests();
    let dir = test_root("no-internal-listener");
    let certs = cert_dir();
    let (reservations, addrs) = reserve(4);
    let public = addrs[0];
    let (manifest, _) = single_node_manifest(&dir.0, &certs, &addrs, "off");
    let data = dir.0.join("client-data");
    let log = dir.0.join("daemon.log");
    drop(reservations);
    let mut daemon = spawn(&data, &manifest, 1, &format!("1={public}"), public, &log, &[]);
    ready(&mut daemon, public);

    ok(
        public,
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml": POOL_YAML})).unwrap(),
        "application/json",
    );

    let token = format!("x-yadb-raft-token: {RAFT_TOKEN}\r\n");
    let stats = "/internal/vdisk/data/0/stats";

    // Unset means today's behaviour: the routes are on the public listener and
    // the token still authenticates them.
    for extra in ["", "x-yadb-raft-token: wrong\r\n"] {
        let (status, _, body) = request(public, "GET", stats, b"", "application/json", extra);
        assert_eq!(
            status,
            401,
            "unauthenticated {stats} without the flag: {}",
            String::from_utf8_lossy(&body)
        );
    }
    let body = await_status(public, stats, &token, 200);
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert!(value["live_blobs"].is_number(), "{value}");
    let body = ok_with(
        public,
        "GET",
        "/internal/control/snapshot",
        b"",
        "application/json",
        &token,
    );
    assert!(serde_json::from_slice::<Value>(&body).unwrap()["revision"].is_number());

    assert_public_surface_intact(public, "surface-off");
}

/// Three real nodes with `--internal-listen` and `--internal-peers`: a remote
/// VDisk RPC must reach the peer's *internal* listener, so a PUT through one
/// node and a GET through another prove replication still works after the split.
/// Without the peer map the same PUT would 404 on the peer's public port, which
/// is why the map is required in clustered mode rather than optional.
#[test]
fn cluster_replicates_over_the_internal_listener() {
    let _serial = serialize_daemon_tests();
    let dir = test_root("internal-listener-cluster");
    let certs = cert_dir();
    // Three public, nine Raft transport (three groups x three nodes), three
    // internal.
    let (reservations, addrs) = reserve(15);
    let public: Vec<SocketAddr> = addrs[..3].to_vec();
    let transport = &addrs[3..12];
    let internal: Vec<SocketAddr> = addrs[12..].to_vec();
    let mut signing = Vec::new();
    for node in 1..=3usize {
        let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(key.as_ref()).unwrap();
        let path = dir.0.join(format!("signing-{node}.der"));
        std::fs::write(&path, key.as_ref()).unwrap();
        let cert = std::fs::read(certs.join(format!("node-{node}.test.cert.der"))).unwrap();
        signing.push((path, pair.public_key().as_ref().to_vec(), MutualTls::fingerprint(&cert)));
    }
    let mut manifests = Vec::new();
    for node in 1..=3usize {
        let metadata = dir.0.join(format!("metadata-{node}"));
        std::fs::create_dir(&metadata).unwrap();
        let mut paths = Vec::new();
        for group in 1..=3usize {
            let address = transport[(group - 1) * 3 + node - 1];
            let peers: Vec<_> = (1..=3usize)
                .map(|peer| {
                    json!({"id":peer,"address":transport[(group - 1) * 3 + peer - 1],
                    "public_key":signing[peer-1].1,"server_name":format!("node-{peer}.test"),
                    "cert_sha256":signing[peer-1].2})
                })
                .collect();
            let path = dir.0.join(format!("node-{node}-group-{group}.json"));
            config(
                &path,
                &json!({"version":1,"root":metadata,"mode":"create","initialize":node==group,"cluster":vec![92;16],"group":group,"incarnation":1,"node":node,"listen":address,"signing_key_der":signing[node-1].0,
                "preferred_leader":group,
                "raft_timing":{"heartbeat_ms":750,"election_min_ms":2250,"nonpreferred_election_min_ms":3000,"election_max_ms":3500},
                "tls":{"roots_der":[certs.join("ca.der")],"chain_der":[certs.join(format!("node-{node}.test.cert.der"))],"key_der":certs.join(format!("node-{node}.test.key.der"))},"peers":peers}),
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
    let internal_map = (1..=3usize)
        .map(|node| format!("{node}={}", internal[node - 1]))
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
                &[
                    "--internal-listen".into(),
                    internal[node - 1].to_string(),
                    "--internal-peers".into(),
                    internal_map.clone(),
                ],
            )
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        for daemon in daemons.iter_mut() {
            assert!(
                daemon.0.try_wait().unwrap().is_none(),
                "node exited during startup"
            );
        }
        let mut all_ready = true;
        for &address in &public {
            if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err() {
                all_ready = false;
                continue;
            }
            let (status, _, body) =
                request(address, "GET", "/openraft/status", b"", "application/json", "");
            if status != 200 {
                all_ready = false;
                continue;
            }
            let status: Value = serde_json::from_slice(&body).unwrap();
            let groups = status["local_observations"].as_array().unwrap();
            if groups.len() != 3 || groups.iter().any(|g| g["leader_id"].is_null()) {
                all_ready = false;
            }
        }
        if all_ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cluster leader discovery timed out"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    let yaml = format!(
        "{POOL_YAML}---
apiVersion: yadb.io/v1alpha1
kind: Project
metadata: {{name: alpha}}
spec: {{quotas: {{maxStorageBytes: 100000000, maxIops: 1000}}, members: [{{subject: tester, role: readWrite}}]}}
---
apiVersion: yadb.io/v1alpha1
kind: ObjectStore
metadata: {{name: store, project: alpha}}
spec: {{pool: data, isolation: shared, buckets: [bucket]}}
"
    );
    ok(
        public[0],
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml": yaml})).unwrap(),
        "application/json",
    );
    // Wait until every node has materialized the pool: each node hosts exactly
    // one of the three slots, so each must answer exactly one slot's stats.
    let token = format!("x-yadb-raft-token: {RAFT_TOKEN}\r\n");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut ready = true;
        for addr in &internal {
            let hosted = (0..3usize).any(|slot| {
                let path = format!("/internal/vdisk/data/{slot}/stats");
                request(*addr, "GET", &path, b"", "application/json", &token).0 == 200
            });
            if !hosted {
                ready = false;
            }
        }
        if ready {
            break;
        }
        assert!(Instant::now() < deadline, "pool slots did not materialize");
        std::thread::sleep(Duration::from_millis(50));
    }
    for (node, addr) in internal.iter().enumerate() {
        let hosted: Vec<usize> = (0..3usize)
            .filter(|slot| {
                let path = format!("/internal/vdisk/data/{slot}/stats");
                request(*addr, "GET", &path, b"", "application/json", &token).0 == 200
            })
            .collect();
        assert_eq!(
            hosted.len(),
            1,
            "node {} internal listener hosts {hosted:?}",
            node + 1
        );
        // The same slot on the public port is not a handler refusal: the route
        // does not exist there, so the body is empty.
        for slot in 0..3usize {
            let path = format!("/internal/vdisk/data/{slot}/stats");
            let (status, _, body) =
                request(public[node], "GET", &path, b"", "application/json", &token);
            assert_eq!(status, 404, "{path} on node {} public", node + 1);
            assert!(
                body.is_empty(),
                "{path} reached a handler on node {} public: {}",
                node + 1,
                String::from_utf8_lossy(&body)
            );
        }
        // No token on the internal listener is still a 401, not a free pass.
        let path = format!("/internal/vdisk/data/{}/stats", hosted[0]);
        let (status, _, _) = request(*addr, "GET", &path, b"", "application/json", "");
        assert_eq!(status, 401, "unauthenticated {path} on node {}", node + 1);
    }

    // A write through one node and a read through another exercise the remote
    // slot RPCs across the internal listeners. With a 3-way mirror over three
    // nodes, two of the three fragment writes are remote.
    let object = b"cross-node object payload";
    ok(
        public[1],
        "PUT",
        "/s3/alpha/store/bucket/item",
        object,
        "application/octet-stream",
    );
    let read = ok(
        public[2],
        "GET",
        "/s3/alpha/store/bucket/item",
        b"",
        "application/octet-stream",
    );
    assert_eq!(read, object);

    assert_public_surface_intact(public[0], "surface-cluster");
}
