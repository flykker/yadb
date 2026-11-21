//! Real daemon, new nonproduction root and three local singleton groups.
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
use yadb_metakv::{
    multiraft::{GroupIdentity, ReplicaIdentity},
    openraft_state_machine::DurableMetaStateMachine,
    openraft_tls::MutualTls,
};

/// Subject every daemon in this suite is started to verify, and the shared
/// secret used to prove it. The daemons run with `--subject-secret`, so an
/// unsigned subject would be treated as anonymous.
const SUBJECT: &str = "tester";
const SUBJECT_SECRET: &str = "integration-test-subject-secret";

/// Held for the duration of each test that starts a daemon.
///
/// Each test reserves ephemeral ports, drops the reservations, and only then
/// lets the child bind them. Between the drop and the bind another test in this
/// binary can take a port, in which case `ready` polls a foreign daemon and the
/// test's own daemon fails to bind — the run then fails on a closed listener
/// rather than on the behaviour under test. The four daemon tests are already
/// slow enough that running them one at a time costs nothing measurable.
static DAEMON_TESTS: Mutex<()> = Mutex::new(());

fn serialize_daemon_tests() -> MutexGuard<'static, ()> {
    // A panicking test poisons the lock. The remaining tests still have to run
    // and report their own result, so the poison is ignored deliberately.
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
impl Daemon {
    fn wait(&mut self, duration: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + duration;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "daemon did not exit in time");
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    fn interrupt(&mut self) {
        assert!(Command::new("kill")
            .args(["-INT", &self.0.id().to_string()])
            .status()
            .unwrap()
            .success());
        assert!(
            self.wait(Duration::from_secs(15)).success(),
            "daemon did not shut down cleanly"
        );
    }
}
fn spawn(data: &Path, manifest: Option<&Path>, public: SocketAddr, log: &Path) -> Daemon {
    spawn_with_env(data, manifest, public, log, &[])
}

/// [`spawn`] plus environment variables for the child. Used by tests that need
/// a test-only hook the daemon reads from its environment.
fn spawn_with_env(
    data: &Path,
    manifest: Option<&Path>,
    public: SocketAddr,
    log: &Path,
    env: &[(&str, &str)],
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
    ])
    .env_remove("YADB_SUBJECT_SECRET")
    .env_remove("YADB_RAFT_TOKEN");
    for (name, value) in env {
        cmd.env(name, value);
    }
    if let Some(manifest) = manifest {
        cmd.args([
            "--metadata-runtime",
            "openraft",
            "--openraft-config",
            manifest.to_str().unwrap(),
            "--peers",
            &format!("1={public}"),
            "--raft-token",
            "integration-test-token",
            "--subject-secret",
            SUBJECT_SECRET,
            "--admin-subject",
            SUBJECT,
        ]);
    }
    Daemon(
        cmd.stdout(Stdio::from(stderr.try_clone().unwrap()))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap(),
    )
}
fn spawn_cluster(
    data: &Path,
    manifest: &Path,
    node: u32,
    public: &[SocketAddr],
    log: &Path,
) -> Daemon {
    let stderr = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    let peers = public
        .iter()
        .enumerate()
        .map(|(i, addr)| format!("{}={addr}", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    let child = Command::new(env!("CARGO_BIN_EXE_yadbd"))
        .args([
            "--listen",
            &public[node as usize - 1].to_string(),
            "--node-id",
            &node.to_string(),
            "--data-dir",
            data.to_str().unwrap(),
            "--metadata-runtime",
            "openraft",
            "--openraft-config",
            manifest.to_str().unwrap(),
            "--peers",
            &peers,
            "--raft-token",
            "integration-test-token",
            "--subject-secret",
            SUBJECT_SECRET,
            "--admin-subject",
            SUBJECT,
        ])
        .env_remove("YADB_SUBJECT_SECRET")
        .env_remove("YADB_RAFT_TOKEN")
        .stdout(Stdio::from(stderr.try_clone().unwrap()))
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap();
    Daemon(child)
}
fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
) -> (u16, Vec<u8>) {
    let (status, _, body) = request_headers(addr, method, path, body, content_type);
    (status, body)
}
fn request_headers(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
) -> (u16, String, Vec<u8>) {
    request_extra(addr, method, path, body, content_type, "")
}
fn request_extra(
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
    request_with_identity(addr, method, path, body, content_type, &identity, extra)
}

/// Sends a request with caller-supplied identity headers. `identity` is
/// written verbatim (each header must end with CRLF) so tests can omit the
/// subject, forge a signature, or prove a different subject.
fn request_with_identity(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    identity: &str,
    extra: &str,
) -> (u16, String, Vec<u8>) {
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
    let header = std::str::from_utf8(&response[..split]).unwrap();
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
    (status, header.to_string(), response[split + 4..].to_vec())
}
fn ok(addr: SocketAddr, method: &str, path: &str, body: &[u8], ty: &str) -> Vec<u8> {
    let (status, response) = request(addr, method, path, body, ty);
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
            let result: Value = serde_json::from_slice(&ok(
                addr,
                "GET",
                "/openraft/status",
                b"",
                "application/json",
            ))
            .unwrap();
            if result["local_observations"]
                .as_array()
                .is_some_and(|groups| {
                    groups.len() == 3 && groups.iter().all(|g| g["leader_id"] == 1)
                })
            {
                return;
            }
        }
        assert!(Instant::now() < deadline, "three group leaders not ready");
        std::thread::sleep(Duration::from_millis(50));
    }
}
fn config(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

/// Every path under `root` with its type, permission bits, modification time
/// and bytes. Comparing this before and after a refused startup proves the
/// directory was not rewritten (mtime catches a same-length in-place write),
/// imported, locked with a file, or added to.
#[derive(Debug, PartialEq, Eq)]
struct TreeEntry {
    kind: &'static str,
    mode: u32,
    modified: Option<std::time::SystemTime>,
    bytes: Vec<u8>,
}

fn tree_snapshot(root: &Path) -> std::collections::BTreeMap<PathBuf, TreeEntry> {
    use std::os::unix::fs::PermissionsExt;
    fn walk(root: &Path, path: &Path, out: &mut std::collections::BTreeMap<PathBuf, TreeEntry>) {
        let metadata = std::fs::symlink_metadata(path).unwrap();
        let kind = if metadata.is_dir() {
            "dir"
        } else if metadata.is_file() {
            "file"
        } else {
            "other"
        };
        out.insert(
            path.strip_prefix(root).unwrap().to_path_buf(),
            TreeEntry {
                kind,
                mode: metadata.permissions().mode(),
                modified: metadata.modified().ok(),
                bytes: if metadata.is_file() {
                    std::fs::read(path).unwrap()
                } else {
                    Vec::new()
                },
            },
        );
        if metadata.is_dir() {
            let mut children = std::fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>();
            children.sort();
            for child in children {
                walk(root, &child, out);
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    walk(root, root, &mut out);
    out
}

/// A pre-OpenRaft metadata directory: a WAL, a descriptor, a state image, an
/// empty subdirectory and a dotfile. None of it is an OpenRaft marker.
fn write_legacy_tree(root: &Path) {
    std::fs::create_dir_all(root.join("metakv")).unwrap();
    std::fs::write(
        root.join("metakv").join("state.json"),
        br#"{"revision":7,"projects":{"alpha":1}}"#,
    )
    .unwrap();
    std::fs::write(root.join("metakv").join("leases"), vec![0x7f; 64]).unwrap();
    std::fs::create_dir(root.join("group")).unwrap();
    std::fs::write(
        root.join("group").join("raft.wal"),
        b"YADBWAL1\x00\x01legacy",
    )
    .unwrap();
    std::fs::write(
        root.join("group").join("replica.identity"),
        br#"{"node":1,"group":1,"incarnation":1}"#,
    )
    .unwrap();
    std::fs::create_dir(root.join("empty-subdir")).unwrap();
    std::fs::write(root.join(".legacy-marker"), b"legacy-runtime-v1").unwrap();
}

/// Three valid singleton group configs (each with its own listener and signing
/// key) plus a version-2 manifest, which is what `main` requires before it looks
/// at `--data-dir`. Returns the public HTTP listener separately from the three
/// Raft listeners so the two never collide.
fn write_manifest(
    dir: &Path,
    cert_dir: &Path,
    metadata: &Path,
) -> (PathBuf, SocketAddr, Vec<SocketAddr>) {
    let reservations: Vec<_> = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addrs: Vec<SocketAddr> = reservations
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    let mut paths = Vec::new();
    for (index, addr) in addrs[1..].iter().enumerate() {
        let group = index + 1;
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let key = dir.join(format!("signing-{group}.der"));
        std::fs::write(&key, pkcs8.as_ref()).unwrap();
        let cert = std::fs::read(cert_dir.join("node-1.test.cert.der")).unwrap();
        let path = dir.join(format!("group-{group}.json"));
        config(
            &path,
            &json!({"version":1,"root":metadata,"mode":"create","initialize":true,"cluster":vec![91;16],"group":group,"incarnation":1,"node":1,"listen":addr,"signing_key_der":key,
            "tls":{"roots_der":[cert_dir.join("ca.der")],"chain_der":[cert_dir.join("node-1.test.cert.der")],"key_der":cert_dir.join("node-1.test.key.der")},
            "peers":[{"id":1,"address":addr,"public_key":pair.public_key().as_ref(),"server_name":"node-1.test","cert_sha256":MutualTls::fingerprint(&cert)}]}),
        );
        paths.push(path);
    }
    let manifest = dir.join("multi.json");
    config(
        &manifest,
        &json!({"version":2,"control_group":1,"project_groups":{"1":2,"2":3},"groups":paths}),
    );
    drop(reservations);
    (manifest, addrs[0], addrs[1..].to_vec())
}

#[test]
fn startup_refuses_a_legacy_data_root_and_leaves_it_byte_identical() {
    let _serial = serialize_daemon_tests();
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-legacy-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let dir = Dir(root.canonicalize().unwrap());
    let cert_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../yadb-metakv/testdata/tls")
        .canonicalize()
        .unwrap();
    // The metadata root is empty and separate; only the client data root is a
    // pre-OpenRaft directory.
    let metadata = dir.0.join("metadata");
    std::fs::create_dir(&metadata).unwrap();
    let (manifest, public, _raft_addrs) = write_manifest(&dir.0, &cert_dir, &metadata);
    let data = dir.0.join("data");
    write_legacy_tree(&data);
    let before = tree_snapshot(&data);
    let log = dir.0.join("daemon.log");

    let mut daemon = spawn(&data, Some(&manifest), public, &log);
    assert!(
        !daemon.wait(Duration::from_secs(20)).success(),
        "a pre-OpenRaft data root must be refused"
    );
    drop(daemon);

    let text = std::fs::read_to_string(&log).unwrap();
    // The refusal must be the root guard, not a port or config failure: that is
    // what makes this evidence for admission rather than for something else.
    assert!(
        text.contains("new or empty data root"),
        "unexpected refusal: {text}"
    );
    // No fallback: the daemon never reached the serving path, so it cannot have
    // opened the legacy runtime on this directory.
    assert!(
        !text.contains("listening on http"),
        "the daemon served despite refusing the data root: {text}"
    );
    assert_eq!(
        tree_snapshot(&data),
        before,
        "the refused startup modified the legacy directory"
    );
    assert!(!data.join(".yadb-openraft-runtime.json").exists());
    assert!(!data.join(".yadb-openraft-runtime.pending").exists());
}

#[test]
fn daemon_multigroup_http_shutdown_reopen_and_root_guards() {
    let _serial = serialize_daemon_tests();
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-http-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let dir = Dir(root.canonicalize().unwrap());
    let legacy = dir.0.join("legacy");
    std::fs::create_dir(&legacy).unwrap();
    std::fs::write(legacy.join("sentinel"), b"untouched").unwrap();
    let metadata = dir.0.join("metadata");
    std::fs::create_dir(&metadata).unwrap();
    let data = dir.0.join("client-data");
    let cert_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../yadb-metakv/testdata/tls")
        .canonicalize()
        .unwrap();
    let cert = std::fs::read(cert_dir.join("node-1.test.cert.der")).unwrap();
    let reservations: Vec<_> = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addrs: Vec<_> = reservations
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    let mut paths = Vec::new();
    for group in 1..=3usize {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let key = dir.0.join(format!("signing-{group}.der"));
        std::fs::write(&key, pkcs8.as_ref()).unwrap();
        let path = dir.0.join(format!("group-{group}.json"));
        config(
            &path,
            &json!({"version":1,"root":metadata,"mode":"create","initialize":true,"cluster":vec![81;16],"group":group,"incarnation":1,"node":1,"listen":addrs[group],"signing_key_der":key,
            "tls":{"roots_der":[cert_dir.join("ca.der")],"chain_der":[cert_dir.join("node-1.test.cert.der")],"key_der":cert_dir.join("node-1.test.key.der")},
            "peers":[{"id":1,"address":addrs[group],"public_key":pair.public_key().as_ref(),"server_name":"node-1.test","cert_sha256":MutualTls::fingerprint(&cert)}]}),
        );
        paths.push(path);
    }
    let manifest = dir.0.join("multi.json");
    let mut multi =
        json!({"version":2,"control_group":1,"project_groups":{"1":2,"2":3},"groups":paths});
    config(&manifest, &multi);
    drop(reservations);
    let public = addrs[0];
    let log = dir.0.join("daemon.log");
    let mut daemon = spawn(&data, Some(&manifest), public, &log);
    ready(&mut daemon, public);
    for path in ["/healthz", "/raft/status"] {
        let v: Value =
            serde_json::from_slice(&ok(public, "GET", path, b"", "application/json")).unwrap();
        assert_eq!(v["metadata_runtime"], "openraft");
        assert_eq!(v["readiness"]["ready"], true);
    }
    // The removed runtime's internal endpoints are not merely unauthorized in
    // OpenRaft mode; they are absent, so a request falls through to the S3 wire
    // fallback and `404`s. `401`/`403` would mean the old token-protected route
    // still exists and is refusing this caller, and `405` would mean the path is
    // still routed with a different verb; both are what this rejects. Every old
    // verb is checked so a re-added handler cannot pass on a method mismatch.
    for path in ["/raft/rpc", "/raft/elect"] {
        for method in ["GET", "POST"] {
            let (status, _, body) =
                request_extra(public, method, path, b"{}", "application/json", "");
            assert_eq!(
                status,
                404,
                "legacy endpoint {method} {path} is still routed: {}",
                String::from_utf8_lossy(&body)
            );
        }
    }
    // The internal snapshot endpoint is token-protected and must expose the
    // control key, not an empty barrier-only snapshot.
    for extra in ["", "x-yadb-raft-token: wrong\r\n"] {
        let (status, _, _) = request_extra(
            public,
            "GET",
            "/internal/control/snapshot",
            b"",
            "application/json",
            extra,
        );
        assert_eq!(status, 401);
    }
    let (status, _, body) = request_extra(
        public,
        "GET",
        "/internal/control/snapshot",
        b"",
        "application/json",
        "x-yadb-raft-token: integration-test-token\r\n",
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let fresh: Value = serde_json::from_slice(&body).unwrap();
    assert!(fresh["revision"].as_u64().is_some());
    assert!(fresh["control"]["projects"].as_object().unwrap().is_empty());
    let yaml = r#"apiVersion: yadb.io/v1alpha1
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
kind: Project
metadata: {name: beta}
spec: {quotas: {maxStorageBytes: 100000000, maxIops: 1000}, members: [{subject: tester, role: readWrite}]}
---
apiVersion: yadb.io/v1alpha1
kind: Database
metadata: {name: db, project: alpha}
spec: {walPool: data, dataPool: data, isolation: shared, shards: 2}
---
apiVersion: yadb.io/v1alpha1
kind: Database
metadata: {name: db, project: beta}
spec: {walPool: data, dataPool: data, isolation: shared, shards: 2}
---
apiVersion: yadb.io/v1alpha1
kind: ObjectStore
metadata: {name: store, project: alpha}
spec: {pool: data, isolation: shared, buckets: [bucket]}
---
apiVersion: yadb.io/v1alpha1
kind: ObjectStore
metadata: {name: store, project: beta}
spec: {pool: data, isolation: shared, buckets: [bucket]}
---
apiVersion: yadb.io/v1alpha1
kind: BlockVolume
metadata: {name: disk, project: alpha}
spec: {pool: data, sizeBytes: 8192}
---
apiVersion: yadb.io/v1alpha1
kind: BlockVolume
metadata: {name: disk, project: beta}
spec: {pool: data, sizeBytes: 8192}
"#;
    ok(
        public,
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml":yaml})).unwrap(),
        "application/json",
    );
    let keyed_body = serde_json::to_vec(&json!({"yaml":"apiVersion: yadb.io/v1alpha1\nkind: StoragePool\nmetadata: {name: idempotency-pool}\nspec: {erasure: {scheme: mirror, replicas: 3}, failureDomains: 3}\n"})).unwrap();
    let generation = || -> u64 {
        let status: Value =
            serde_json::from_slice(&ok(public, "GET", "/v1/status", b"", "application/json"))
                .unwrap();
        status["generation"].as_u64().unwrap()
    };
    let before = generation();
    for key in [String::new(), "a".repeat(129), "a b".into(), "a+b".into()] {
        let (status, _, _) = request_extra(
            public,
            "POST",
            "/v1/apply",
            &keyed_body,
            "application/json",
            &format!("Idempotency-Key: {key}\r\n"),
        );
        assert_eq!(status, 400);
    }
    let (status, _, _) = request_extra(
        public,
        "POST",
        "/v1/apply",
        &keyed_body,
        "application/json",
        "Idempotency-Key: a\r\nIdempotency-Key: b\r\n",
    );
    assert_eq!(status, 400);
    for (identity, expected) in [
        (String::new(), 401),
        (
            format!(
                "x-yadb-subject: other\r\nx-yadb-subject-signature: {}\r\n",
                subject_signature("other")
            ),
            403,
        ),
    ] {
        let (status, _, _) = request_with_identity(
            public,
            "POST",
            "/v1/apply",
            &keyed_body,
            "application/json",
            &identity,
            "Idempotency-Key: auth\r\n",
        );
        assert_eq!(status, expected);
    }
    // A forged signature must not be accepted even for an allowlisted subject:
    // the signature, not the claimed name, is what proves the subject.
    let forged = format!(
        "x-yadb-subject: admin\r\nx-yadb-subject-signature: {}\r\n",
        "00".repeat(32)
    );
    let (status, _, body) = request_with_identity(
        public,
        "POST",
        "/v1/apply",
        &keyed_body,
        "application/json",
        &forged,
        "Idempotency-Key: auth\r\n",
    );
    assert_eq!(
        status,
        401,
        "forged signature: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(generation(), before);
    let (status, _, recorded_body) = request_extra(
        public,
        "POST",
        "/v1/apply",
        &keyed_body,
        "application/json",
        "Idempotency-Key: durable\r\n",
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&recorded_body));
    assert_eq!(generation(), before + 1);
    let (status, _, replay) = request_extra(
        public,
        "POST",
        "/v1/apply",
        &keyed_body,
        "application/json",
        "Idempotency-Key: durable\r\n",
    );
    assert_eq!((status, replay), (200, recorded_body.clone()));
    let different = serde_json::to_vec(&json!({"yaml":yaml})).unwrap();
    let (status, _, body) = request_extra(
        public,
        "POST",
        "/v1/apply",
        &different,
        "application/json",
        "Idempotency-Key: durable\r\n",
    );
    assert_eq!(status, 409);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "idempotency_key_conflict"
    );
    assert_eq!(generation(), before + 1);
    let resolve = |key: &str| -> Value {
        let body = serde_json::to_vec(&json!({"scope":"control","key":key})).unwrap();
        serde_json::from_slice(&ok(
            public,
            "POST",
            "/v1/idempotency/resolve",
            &body,
            "application/json",
        ))
        .unwrap()
    };
    let resolved = resolve("durable");
    assert_eq!(resolved["outcome"], "committed");
    use base64::Engine as _;
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(resolved["response"]["body_base64"].as_str().unwrap())
            .unwrap(),
        recorded_body
    );
    assert_eq!(resolve("fence")["outcome"], "not_committed");
    let (status, _, body) = request_extra(
        public,
        "POST",
        "/v1/apply",
        &different,
        "application/json",
        "Idempotency-Key: fence\r\n",
    );
    assert_eq!(status, 409);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "idempotency_key_fenced"
    );
    assert_eq!(generation(), before + 1);
    // Concurrent distinct keys must each commit, while identical keys replay
    // exactly one committed response. The `test-hooks` park is compiled out of
    // this default build, so nothing here can pause a request mid-flight.
    let concurrent = |keys: &[&str]| {
        let barrier = std::sync::Barrier::new(keys.len());
        std::thread::scope(|scope| {
            let barrier = &barrier;
            keys.iter()
                .map(|key| {
                    scope.spawn(move || {
                        let body = serde_json::to_vec(&json!({"yaml": format!("apiVersion: yadb.io/v1alpha1\nkind: StoragePool\nmetadata: {{name: {key}}}\nspec: {{erasure: {{scheme: mirror, replicas: 3}}, failureDomains: 3}}\n")})).unwrap();
                        barrier.wait();
                        request_extra(
                            public,
                            "POST",
                            "/v1/apply",
                            &body,
                            "application/json",
                            &format!("Idempotency-Key: {key}\r\n"),
                        )
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        })
    };
    let results = concurrent(&["parallel-one", "parallel-two"]);
    let mut generations = Vec::new();
    for (status, _, body) in results {
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        generations.push(
            serde_json::from_slice::<Value>(&body).unwrap()["generation"]
                .as_u64()
                .unwrap(),
        );
    }
    generations.sort_unstable();
    assert_eq!(generations, [before + 2, before + 3]);
    assert_eq!(generation(), before + 3);
    let results = concurrent(&["parallel-same", "parallel-same", "parallel-same"]);
    let first = &results[0].2;
    for (status, _, body) in &results {
        assert_eq!(*status, 200, "{}", String::from_utf8_lossy(body));
        assert_eq!(body, first);
    }
    assert_eq!(
        serde_json::from_slice::<Value>(first).unwrap()["generation"],
        before + 4
    );
    assert_eq!(generation(), before + 4);
    // Keyed block writes are a project-scope idempotency operation, not an
    // unsupported endpoint: the first write commits and records its outcome,
    // and the same key with the same body replays that record instead of
    // writing again. (This assertion used to expect 501, from before keyed
    // block writes were implemented; on a node with OpenRaft metadata the
    // request reached the keyed path and failed with 500 instead.)
    let (status, _, keyed_write_body) = request_extra(
        public,
        "PUT",
        "/block/alpha/disk?offset=0",
        b"x",
        "application/octet-stream",
        "Idempotency-Key: unsupported\r\n",
    );
    assert_eq!(
        status,
        200,
        "keyed block write: {}",
        String::from_utf8_lossy(&keyed_write_body)
    );
    let (replay_status, _, replay_body) = request_extra(
        public,
        "PUT",
        "/block/alpha/disk?offset=0",
        b"x",
        "application/octet-stream",
        "Idempotency-Key: unsupported\r\n",
    );
    assert_eq!(
        replay_status,
        200,
        "keyed block write replay: {}",
        String::from_utf8_lossy(&replay_body)
    );
    assert_eq!(
        replay_body, keyed_write_body,
        "a replayed keyed write must return the recorded response"
    );
    // Give both volumes real content first: "reads back as zeros" is also true
    // for a volume that was never written, so without this the UNMAP assertion
    // would not prove the discard happened.
    let pattern: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8 + 1).collect();
    for project in ["alpha", "beta"] {
        ok(
            public,
            "PUT",
            &format!("/block/{project}/disk?offset=0"),
            &pattern,
            "application/octet-stream",
        );
        assert_eq!(
            ok(
                public,
                "GET",
                &format!("/block/{project}/disk?offset=0&len=4096&consistent=true"),
                b"",
                "application/json",
            ),
            pattern,
            "{project}: the seeded block is not readable before UNMAP"
        );
    }
    // Keyed UNMAP is the first project-scope keyed mutation: it has no staging,
    // so the extent-map publication and the record commit in one guarded
    // transaction. The map below is a single node, so every group leader is
    // local and this exercises the commit path, not forwarding.
    let unmap_body =
        |offset: u64, len: u64| serde_json::to_vec(&json!({"offset":offset,"len":len})).unwrap();
    let keyed_unmap = |project: &str, body: &[u8], key: &str| {
        request_extra(
            public,
            "POST",
            &format!("/block/{project}/disk/unmap"),
            body,
            "application/json",
            &format!("Idempotency-Key: {key}\r\n"),
        )
    };
    let resolve_scope = |project: Option<&str>, key: &str| -> (u16, Value) {
        let payload = match project {
            Some(project) => json!({"scope":"project","project":project,"key":key}),
            None => json!({"scope":"control","key":key}),
        };
        let (status, _, body) = request_extra(
            public,
            "POST",
            "/v1/idempotency/resolve",
            &serde_json::to_vec(&payload).unwrap(),
            "application/json",
            "",
        );
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    };
    let block_zeros = |project: &str, len: usize| -> Vec<u8> {
        ok(
            public,
            "GET",
            &format!("/block/{project}/disk?offset=0&len={len}&consistent=true"),
            b"",
            "application/json",
        )
    };
    // An unproven identity has no idempotency scope: the record is keyed by
    // subject, so "anonymous" must not be a shared keyspace.
    let (status, _, _) = request_with_identity(
        public,
        "POST",
        "/block/alpha/disk/unmap",
        &unmap_body(0, 4096),
        "application/json",
        "",
        "Idempotency-Key: no-identity\r\n",
    );
    assert_eq!(status, 401);
    // A verified subject that is not a project member is refused, and nothing is
    // recorded for it.
    let (status, _, _) = request_with_identity(
        public,
        "POST",
        "/block/alpha/disk/unmap",
        &unmap_body(0, 4096),
        "application/json",
        &format!(
            "x-yadb-subject: outsider\r\nx-yadb-subject-signature: {}\r\n",
            subject_signature("outsider")
        ),
        "Idempotency-Key: outsider\r\n",
    );
    assert_eq!(status, 403);
    // Resolve is authorized for the project too, so an outsider cannot use it to
    // probe or fence another project's keys.
    let (status, _, body) = request_with_identity(
        public,
        "POST",
        "/v1/idempotency/resolve",
        &serde_json::to_vec(&json!({"scope":"project","project":"alpha","key":"outsider"}))
            .unwrap(),
        "application/json",
        &format!(
            "x-yadb-subject: outsider\r\nx-yadb-subject-signature: {}\r\n",
            subject_signature("outsider")
        ),
        "",
    );
    assert_eq!(status, 403, "{}", String::from_utf8_lossy(&body));
    // And the refused mutation left no record behind: a member resolving the same
    // key finds nothing and fences it, rather than observing a record the 403
    // attempt could not have committed.
    let (status, resolved) = resolve_scope(Some("alpha"), "outsider");
    assert_eq!(
        (status, resolved["outcome"].as_str()),
        (200, Some("not_committed"))
    );

    // The same key in two projects must be two independent records: project
    // scope, not cluster scope.
    for project in ["alpha", "beta"] {
        let (status, _, body) = keyed_unmap(project, &unmap_body(0, 4096), "unmap-one");
        assert_eq!(status, 200, "{project}: {}", String::from_utf8_lossy(&body));
        // The discard is visible through the quorum-confirmed read path.
        assert_eq!(block_zeros(project, 4096), vec![0u8; 4096]);
    }
    // Replay returns the recorded response and changes nothing: a second UNMAP of
    // the same range is a no-op anyway, so the observable proof is that the same
    // key never turns into a conflict or an error.
    let (status, _, body) = keyed_unmap("alpha", &unmap_body(0, 4096), "unmap-one");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(block_zeros("alpha", 4096), vec![0u8; 4096]);
    // Same key, different request: a different range must not replay the first
    // response.
    let (status, _, body) = keyed_unmap("alpha", &unmap_body(0, 8192), "unmap-one");
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "idempotency_key_conflict"
    );
    // A byte-identical body that differs only in JSON formatting must fingerprint
    // differently, because the fingerprint is over the received bytes.
    let reformatted = br#"{"offset": 0, "len": 4096}"#;
    let (status, _, body) = keyed_unmap("alpha", reformatted, "unmap-one");
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "idempotency_key_conflict"
    );
    // The record is project-scoped: resolving the same key for beta must not
    // observe alpha's record.
    let (status, resolved) = resolve_scope(Some("alpha"), "unmap-one");
    assert_eq!(
        (status, resolved["outcome"].as_str()),
        (200, Some("committed"))
    );
    assert_eq!(resolved["response"]["status"], 200);
    let (status, resolved) = resolve_scope(Some("beta"), "unmap-one");
    assert_eq!(
        (status, resolved["outcome"].as_str()),
        (200, Some("committed"))
    );
    // A fence on a fresh key is a permanent not_committed, and a later keyed
    // mutation of that key must lose to it rather than run.
    let (status, resolved) = resolve_scope(Some("alpha"), "unmap-fenced");
    assert_eq!(
        (status, resolved["outcome"].as_str()),
        (200, Some("not_committed"))
    );
    assert_eq!(resolved["reason"], "fenced");
    let (status, _, body) = keyed_unmap("alpha", &unmap_body(0, 4096), "unmap-fenced");
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "idempotency_key_fenced"
    );
    // Control resolve still rejects a project payload and vice versa instead of
    // resolving against a guessed keyspace.
    for payload in [
        json!({"scope":"control","key":"unmap-one","project":"alpha"}),
        json!({"scope":"project","key":"unmap-one"}),
        json!({"scope":"project","project":"","key":"unmap-one"}),
        json!({"scope":"cluster","key":"unmap-one"}),
    ] {
        let (status, _, _) = request_extra(
            public,
            "POST",
            "/v1/idempotency/resolve",
            &serde_json::to_vec(&payload).unwrap(),
            "application/json",
            "",
        );
        assert_eq!(status, 400, "{payload}");
    }
    let (status, _, body) = request_extra(
        public,
        "GET",
        "/internal/control/snapshot",
        b"",
        "application/json",
        "x-yadb-raft-token: integration-test-token\r\n",
    );
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let fresh: Value = serde_json::from_slice(&body).unwrap();
    assert!(fresh["control"]["projects"]["alpha"].is_array());
    assert!(fresh["control"]["projects"]["beta"].is_array());
    for (project, value) in [("alpha", "alpha-value"), ("beta", "beta-value")] {
        for sql in [
            "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)".to_string(),
            format!("INSERT INTO t VALUES (1,'{value}')"),
        ] {
            ok(
                public,
                "POST",
                &format!("/db/{project}/db/query"),
                &serde_json::to_vec(&json!({"sql":sql})).unwrap(),
                "application/json",
            );
        }
        ok(
            public,
            "PUT",
            &format!("/s3/{project}/store/bucket/item"),
            value.as_bytes(),
            "application/octet-stream",
        );
        ok(
            public,
            "PUT",
            &format!("/block/{project}/disk?offset=0"),
            value.as_bytes(),
            "application/octet-stream",
        );
    }
    daemon.interrupt();
    drop(daemon);
    for (index, path) in paths.iter().enumerate() {
        let id = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [81; 16],
                group: index as u64 + 1,
                incarnation: 1,
            },
            1,
            vec![1],
        )
        .unwrap();
        let replica = metadata.join(format!("{}-{}-1-1", "51".repeat(16), index + 1));
        let state = DurableMetaStateMachine::open(replica.join("state"), &id).unwrap();
        let snapshot = state.try_snapshot(&[], &["/".into()]).unwrap();
        if index == 0 {
            assert!(!snapshot.scan_prefix("/sys/").is_empty());
            assert!(snapshot.scan_prefix("/db/").is_empty());
        } else {
            let pid = index;
            for ns in ["db", "s3", "block"] {
                assert!(!snapshot.scan_prefix(&format!("/{ns}/{pid}/")).is_empty());
            }
        }
        drop(state);
        let mut v: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        v["mode"] = json!("open");
        v["initialize"] = json!(false);
        config(path, &v);
    }
    assert!(!data.join("metakv").exists());
    let mut daemon = spawn(&data, Some(&manifest), public, &log);
    ready(&mut daemon, public);
    let (status, _, replay) = request_extra(
        public,
        "POST",
        "/v1/apply",
        &keyed_body,
        "application/json",
        "Idempotency-Key: durable\r\n",
    );
    assert_eq!((status, replay), (200, recorded_body));
    for (project, value) in [("alpha", "alpha-value"), ("beta", "beta-value")] {
        let sql = ok(
            public,
            "POST",
            &format!("/db/{project}/db/query"),
            &serde_json::to_vec(&json!({"sql":"SELECT v FROM t WHERE id = 1"})).unwrap(),
            "application/json",
        );
        assert!(String::from_utf8_lossy(&sql).contains(value));
        assert_eq!(
            ok(
                public,
                "GET",
                &format!("/s3/{project}/store/bucket/item"),
                b"",
                "application/json"
            ),
            value.as_bytes()
        );
        assert_eq!(
            ok(
                public,
                "GET",
                &format!("/block/{project}/disk?offset=0&len={}", value.len()),
                b"",
                "application/json"
            ),
            value.as_bytes()
        );
    }
    daemon.interrupt();
    drop(daemon);
    let mut rejected = spawn(&data, None, public, &log);
    assert!(!rejected.wait(Duration::from_secs(10)).success());
    drop(rejected);
    multi["project_groups"] = json!({"1":3,"2":2});
    config(&manifest, &multi);
    let marker = std::fs::read(data.join(".yadb-openraft-runtime.json")).unwrap();
    let mut rejected = spawn(&data, Some(&manifest), public, &log);
    assert!(!rejected.wait(Duration::from_secs(10)).success());
    drop(rejected);
    assert_eq!(
        std::fs::read(data.join(".yadb-openraft-runtime.json")).unwrap(),
        marker
    );
    assert_eq!(
        std::fs::read(legacy.join("sentinel")).unwrap(),
        b"untouched"
    );
    assert_eq!(std::fs::read_dir(legacy).unwrap().count(), 1);
}

/// Keyed SQL writes: one autocommit statement, its idempotency record and its
/// admission counter commit in one metadata transaction.
///
/// The counter itself is only observable inside the group, so the engine-level
/// test `keyed_single_statement_commits_data_record_and_counter_once` asserts
/// that it advances exactly once. What this test proves over HTTP is the
/// observable half: a replay returns the recorded response without applying the
/// statement again, a reused key with a different statement is a conflict, and a
/// multi-statement script stays unsupported and applies nothing.
#[test]
fn keyed_sql_single_statement_commits_with_its_record() {
    let _serial = serialize_daemon_tests();
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-sqlkey-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let dir = Dir(root.canonicalize().unwrap());
    let cert_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../yadb-metakv/testdata/tls")
        .canonicalize()
        .unwrap();
    let metadata = dir.0.join("metadata");
    std::fs::create_dir(&metadata).unwrap();
    let (manifest, public, _raft_addrs) = write_manifest(&dir.0, &cert_dir, &metadata);
    let data = dir.0.join("data");
    let log = dir.0.join("daemon.log");
    let mut daemon = spawn(&data, Some(&manifest), public, &log);
    ready(&mut daemon, public);

    let yaml = r#"apiVersion: yadb.io/v1alpha1
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
"#;
    ok(
        public,
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml":yaml})).unwrap(),
        "application/json",
    );

    let query = |sql: &str| -> Vec<u8> {
        ok(
            public,
            "POST",
            "/db/alpha/db/query",
            &serde_json::to_vec(&json!({"sql":sql})).unwrap(),
            "application/json",
        )
    };
    let keyed = |sql: &str, key: &str| -> (u16, Vec<u8>) {
        let (status, _, body) = request_extra(
            public,
            "POST",
            "/db/alpha/db/query",
            &serde_json::to_vec(&json!({"sql":sql})).unwrap(),
            "application/json",
            &format!("Idempotency-Key: {key}\r\n"),
        );
        (status, body)
    };
    let ids = || -> Vec<i64> {
        let body = query("SELECT id FROM t ORDER BY id");
        let value: Value = serde_json::from_slice(&body).unwrap();
        value["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row[0].as_i64().unwrap())
            .collect()
    };

    query("CREATE TABLE t (id INT PRIMARY KEY, v INT)");

    // A keyed single INSERT commits the row and its record together.
    let (status, first) = keyed("INSERT INTO t VALUES (1,10)", "sql-one");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&first));
    assert_eq!(ids(), vec![1]);

    // The same key and statement replays the recorded outcome byte for byte and
    // does not apply the row a second time.
    let (status, replay) = keyed("INSERT INTO t VALUES (1,10)", "sql-one");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&replay));
    assert_eq!(
        replay, first,
        "a replayed keyed SQL write must return the recorded response"
    );
    assert_eq!(ids(), vec![1], "a replay must not apply the row again");

    // The same key with a different statement is a conflict, not a replay.
    let (status, body) = keyed("INSERT INTO t VALUES (2,20)", "sql-one");
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"],
        "idempotency_key_conflict"
    );
    assert_eq!(
        ids(),
        vec![1],
        "a key conflict must not apply the statement"
    );

    // A multi-statement script has no cross-statement transaction to make its
    // record atomic with every statement, so it is refused before anything runs.
    let (status, body) = keyed(
        "INSERT INTO t VALUES (3,30); INSERT INTO t VALUES (4,40)",
        "sql-script",
    );
    assert_eq!(status, 501, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        ids(),
        vec![1],
        "a refused multi-statement script must apply nothing"
    );

    // A different key still commits: the refusals above are about this key and
    // this script shape, not a blanket block.
    let (status, body) = keyed("INSERT INTO t VALUES (5,50)", "sql-two");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(ids(), vec![1, 5]);

    // The committed record is visible through resolve and carries its response.
    let (status, _, resolved) = request_extra(
        public,
        "POST",
        "/v1/idempotency/resolve",
        &serde_json::to_vec(&json!({"scope":"project","project":"alpha","key":"sql-one"})).unwrap(),
        "application/json",
        "",
    );
    assert_eq!(status, 200);
    let resolved: Value = serde_json::from_slice(&resolved).unwrap();
    assert_eq!(resolved["outcome"], "committed");
    assert_eq!(resolved["response"]["status"], 200);

    // UPDATE and DELETE take the same one-transaction path; a replay of either
    // must not change the data again.
    let (status, body) = keyed("UPDATE t SET v=11 WHERE id=1", "sql-update");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let (status, replay) = keyed("UPDATE t SET v=11 WHERE id=1", "sql-update");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&replay));
    assert_eq!(replay, body);
    let (status, body) = keyed("DELETE FROM t WHERE id=5", "sql-delete");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let (status, replay) = keyed("DELETE FROM t WHERE id=5", "sql-delete");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&replay));
    assert_eq!(replay, body);
    assert_eq!(ids(), vec![1], "a replayed DELETE must not delete again");

    daemon.interrupt();
    drop(daemon);
}

/// Deterministic version of the concurrent-keyed-write race, built with
/// `--features test-hooks`.
///
/// The timing test below cannot prove the interleaving it describes: its
/// barrier releases the clients before they send, so a run where the requests
/// serialize still passes. This test forces the exact order instead. The
/// daemon's test hook parks the *first* keyed request that finds no record for
/// its key (after the fast-path read, before planning). The test waits for that
/// park, commits the same statement under the same key with a second request,
/// then releases the parked one. The parked request therefore plans against a
/// table that already holds its row and fails with `DuplicatePrimaryKey` before
/// the record guard or the extras builder runs — the precise failure the final
/// re-read exists for. With the re-read disabled this test fails with `400`;
/// with it, both callers see `200` and the same recorded body, and the table
/// holds one row.
#[cfg(feature = "test-hooks")]
#[test]
fn a_parked_keyed_writer_replays_the_record_committed_underneath_it() {
    let _serial = serialize_daemon_tests();
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-sqlpark-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let dir = Dir(root.canonicalize().unwrap());
    let cert_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../yadb-metakv/testdata/tls")
        .canonicalize()
        .unwrap();
    let metadata = dir.0.join("metadata");
    std::fs::create_dir(&metadata).unwrap();
    let race = dir.0.join("race");
    std::fs::create_dir(&race).unwrap();
    let (manifest, public, _raft_addrs) = write_manifest(&dir.0, &cert_dir, &metadata);
    let data = dir.0.join("data");
    let log = dir.0.join("daemon.log");
    let mut daemon = spawn_with_env(
        &data,
        Some(&manifest),
        public,
        &log,
        &[("YADB_TEST_KEYED_RACE_DIR", race.to_str().unwrap())],
    );
    ready(&mut daemon, public);

    let yaml = r#"apiVersion: yadb.io/v1alpha1
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
"#;
    ok(
        public,
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml":yaml})).unwrap(),
        "application/json",
    );
    ok(
        public,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&json!({"sql":"CREATE TABLE t (id INT PRIMARY KEY, v INT)"})).unwrap(),
        "application/json",
    );

    const SQL: &str = "INSERT INTO t VALUES (1,10)";
    let keyed = |key: &'static str| {
        let body = serde_json::to_vec(&json!({ "sql": SQL })).unwrap();
        let extra = format!("Idempotency-Key: {key}\r\n");
        std::thread::spawn(move || {
            let (status, _, body) =
                request_extra(public, "POST", "/db/alpha/db/query", &body, "application/json", &extra);
            (status, body)
        })
    };

    // The parked request reads "no record" and stops before planning.
    let parked = keyed("sql-park");
    let claimed = race.join("claimed");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !claimed.exists() {
        assert!(
            Instant::now() < deadline,
            "the keyed request never reached the test hook; daemon log:\n{}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    // The same key commits underneath it while it is parked.
    let (winner_status, winner_body) = keyed("sql-park").join().unwrap();
    assert_eq!(
        winner_status,
        200,
        "the winning request must commit: {}",
        String::from_utf8_lossy(&winner_body)
    );

    // Release the parked request: it now plans against the committed row.
    std::fs::write(race.join("go"), b"go").unwrap();
    let (parked_status, parked_body) = parked.join().unwrap();
    assert_eq!(
        parked_status,
        200,
        "a parked writer whose key committed underneath it must replay, not fail: {}",
        String::from_utf8_lossy(&parked_body)
    );
    assert_eq!(
        parked_body, winner_body,
        "the replay must be the recorded response, byte for byte"
    );

    let body = ok(
        public,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&json!({"sql":"SELECT id FROM t ORDER BY id"})).unwrap(),
        "application/json",
    );
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        value["rows"],
        json!([[1]]),
        "the statement must be applied exactly once: {value}"
    );

    daemon.interrupt();
    drop(daemon);
}

/// Concurrent requests with the same key: every caller sees the committed
/// response, and the statement is applied once.
///
/// The handler's fast path reads the record before planning, so two requests
/// that arrive together can both observe "no record". The first commits the row
/// and the record; the second then fails its INSERT on the now-existing primary
/// key *during planning*, before the record guard or the extras builder runs.
/// Reporting that as `400 duplicate primary key` would tell the client its
/// write failed when it had in fact committed under this key — the exact
/// ambiguity the key exists to remove. The keyed path therefore re-reads the
/// record on any execution error and replays it when it exists.
///
/// This is the throughput-shaped check: eight callers, one key, all answered.
/// It does not *force* the interleaving (a serialized run passes too), so the
/// ordering is pinned deterministically by
/// `a_parked_keyed_writer_replays_the_record_committed_underneath_it`, which
/// needs `--features test-hooks`.
#[test]
fn concurrent_keyed_writes_replay_instead_of_reporting_a_duplicate() {
    let _serial = serialize_daemon_tests();
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-sqlrace-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let dir = Dir(root.canonicalize().unwrap());
    let cert_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../yadb-metakv/testdata/tls")
        .canonicalize()
        .unwrap();
    let metadata = dir.0.join("metadata");
    std::fs::create_dir(&metadata).unwrap();
    let (manifest, public, _raft_addrs) = write_manifest(&dir.0, &cert_dir, &metadata);
    let data = dir.0.join("data");
    let log = dir.0.join("daemon.log");
    let mut daemon = spawn(&data, Some(&manifest), public, &log);
    ready(&mut daemon, public);

    let yaml = r#"apiVersion: yadb.io/v1alpha1
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
"#;
    ok(
        public,
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml":yaml})).unwrap(),
        "application/json",
    );
    ok(
        public,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&json!({"sql":"CREATE TABLE t (id INT PRIMARY KEY, v INT)"})).unwrap(),
        "application/json",
    );

    // Every caller sends the same statement under the same key, released
    // together. This maximises the chance of the race but does not force it: a
    // run where the requests serialize also passes. The forced ordering is in
    // `a_parked_keyed_writer_replays_the_record_committed_underneath_it`.
    const CALLERS: usize = 8;
    let start = std::sync::Barrier::new(CALLERS);
    let mut answers: Vec<(u16, Vec<u8>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..CALLERS)
            .map(|_| {
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    let (status, _, body) = request_extra(
                        public,
                        "POST",
                        "/db/alpha/db/query",
                        &serde_json::to_vec(&json!({"sql":"INSERT INTO t VALUES (1,10)"})).unwrap(),
                        "application/json",
                        "Idempotency-Key: sql-race\r\n",
                    );
                    (status, body)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect()
    });
    answers.sort();

    let first = answers
        .first()
        .cloned()
        .expect("every caller returns an answer");
    for (status, body) in &answers {
        assert_eq!(
            *status,
            200,
            "a caller whose key had already committed must replay, not fail: {}",
            String::from_utf8_lossy(body)
        );
        assert_eq!(
            body, &first.1,
            "every caller must see the same recorded response"
        );
    }

    let body = ok(
        public,
        "POST",
        "/db/alpha/db/query",
        &serde_json::to_vec(&json!({"sql":"SELECT id FROM t ORDER BY id"})).unwrap(),
        "application/json",
    );
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        value["rows"],
        json!([[1]]),
        "the statement must be applied exactly once: {value}"
    );

    daemon.interrupt();
    drop(daemon);
}

/// `GET /metrics` is routed on the daemon's public listener and answers with
/// the Prometheus text format.
///
/// The rendering itself is unit-tested in `crates/yadbd/src/metrics.rs` and the
/// handler against a multi-group fixture in the binary's tests. What only a
/// real process can prove is that the route is *registered* on the public
/// router: the router is built inside `main`, so a missing `.route("/metrics",
/// …)` would leave every other test green while a scraper received `404`.
/// This also pins the "a scrape is not a collection pass" rule at the process
/// level: a fresh daemon has run no pass, and two scrapes still report none.
#[test]
fn metrics_endpoint_is_served_by_the_daemon() {
    let _serial = serialize_daemon_tests();
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-metrics-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let dir = Dir(root.canonicalize().unwrap());
    let cert_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../yadb-metakv/testdata/tls")
        .canonicalize()
        .unwrap();
    let metadata = dir.0.join("metadata");
    std::fs::create_dir(&metadata).unwrap();
    let (manifest, public, _raft_addrs) = write_manifest(&dir.0, &cert_dir, &metadata);
    let data = dir.0.join("data");
    let log = dir.0.join("daemon.log");
    let mut daemon = spawn(&data, Some(&manifest), public, &log);
    ready(&mut daemon, public);

    let scrape = || -> (String, String) {
        let (status, header, body) =
            request_extra(public, "GET", "/metrics", b"", "application/json", "");
        assert_eq!(
            status,
            200,
            "/metrics is not routed on the public listener: {}",
            String::from_utf8_lossy(&body)
        );
        (header, String::from_utf8(body).expect("the exposition is UTF-8"))
    };

    let (header, first) = scrape();
    assert!(
        header.contains("text/plain; version=0.0.4"),
        "the scrape must be the Prometheus text format, got: {header}"
    );
    // The route is the health surface, not a fresh collection: a daemon that
    // has just started has no recorded pass and reports none.
    assert!(
        first.contains("yadb_gc_last_pass_present 0"),
        "a fresh daemon must report that no pass has run:\n{first}"
    );
    assert!(
        !first.contains("\nyadb_gc_refused "),
        "no pass means no refusal sample either:\n{first}"
    );
    // Every configured metadata group has a row, and the control group is
    // always group 1.
    assert!(
        first.contains("yadb_group_info{group=\"1\""),
        "the control group is missing from the exposition:\n{first}"
    );
    assert!(
        first.contains("yadb_node_info{"),
        "the node row is missing:\n{first}"
    );
    // A second scrape reads the same recorded state: scraping does not run a
    // pass, so the "present" gauge cannot flip to 1.
    let (_, second) = scrape();
    assert!(
        second.contains("yadb_gc_last_pass_present 0"),
        "a scrape must not run a collection pass:\n{second}"
    );

    daemon.interrupt();
    drop(daemon);
}

#[test]
fn three_daemons_forward_http_to_different_group_leaders() {
    let _serial = serialize_daemon_tests();
    let started = Instant::now();
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-forward-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let dir = Dir(root.canonicalize().unwrap());
    let cert_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../yadb-metakv/testdata/tls")
        .canonicalize()
        .unwrap();
    let reservations: Vec<_> = (0..12)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addrs: Vec<_> = reservations
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    let public = &addrs[..3];
    let mut signing = Vec::new();
    for node in 1..=3 {
        let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let public = Ed25519KeyPair::from_pkcs8(key.as_ref())
            .unwrap()
            .public_key()
            .as_ref()
            .to_vec();
        let path = dir.0.join(format!("signing-{node}.der"));
        std::fs::write(&path, key.as_ref()).unwrap();
        let cert = std::fs::read(cert_dir.join(format!("node-{node}.test.cert.der"))).unwrap();
        signing.push((path, public, MutualTls::fingerprint(&cert)));
    }
    let mut manifests = Vec::new();
    for node in 1..=3usize {
        let metadata = dir.0.join(format!("metadata-{node}"));
        std::fs::create_dir(&metadata).unwrap();
        let mut paths = Vec::new();
        for group in 1..=3usize {
            let address = addrs[3 + (group - 1) * 3 + node - 1];
            let peers: Vec<_> = (1..=3usize).map(|peer| json!({"id":peer,"address":addrs[3 + (group - 1) * 3 + peer - 1],"public_key":signing[peer-1].1,"server_name":format!("node-{peer}.test"),"cert_sha256":signing[peer-1].2})).collect();
            let path = dir.0.join(format!("node-{node}-group-{group}.json"));
            config(
                &path,
                &json!({"version":1,"root":metadata,"mode":"create","initialize":node==group,"cluster":vec![82;16],"group":group,"incarnation":1,"node":node,"listen":address,"signing_key_der":signing[node-1].0,
                "preferred_leader":group,"raft_timing":{"heartbeat_ms":750,"election_min_ms":2250,"nonpreferred_election_min_ms":3000,"election_max_ms":3500},
                "tls":{"roots_der":[cert_dir.join("ca.der")],"chain_der":[cert_dir.join(format!("node-{node}.test.cert.der"))],"key_der":cert_dir.join(format!("node-{node}.test.key.der"))},"peers":peers}),
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
    drop(reservations);
    let mut daemons: Vec<_> = (1..=3u32)
        .map(|node| {
            spawn_cluster(
                &dir.0.join(format!("data-{node}")),
                &manifests[node as usize - 1],
                node,
                public,
                &dir.0.join(format!("node-{node}.log")),
            )
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(35);
    loop {
        for (i, daemon) in daemons.iter_mut().enumerate() {
            assert!(
                daemon.0.try_wait().unwrap().is_none(),
                "node {} exited during startup",
                i + 1
            );
        }
        let mut all_ready = true;
        for &address in public {
            if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err() {
                all_ready = false;
                continue;
            }
            let status: Value = serde_json::from_slice(&ok(
                address,
                "GET",
                "/openraft/status",
                b"",
                "application/json",
            ))
            .unwrap();
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
    // Preferences bias the vote; the actual elected leader is authoritative.
    let status: Value = serde_json::from_slice(&ok(
        public[0],
        "GET",
        "/openraft/status",
        b"",
        "application/json",
    ))
    .unwrap();
    let groups = status["local_observations"].as_array().unwrap();
    let leaders: Vec<usize> = (1..=3)
        .map(|id| {
            let group = groups.iter().find(|g| g["group"] == id).unwrap();
            assert_eq!(group["preferred_leader"], id);
            let leader = group["leader_id"].as_u64().unwrap() as usize;
            assert_eq!(group["preference_met"], leader == id);
            leader - 1
        })
        .collect();
    let data_follower = (leaders[1] + 1) % 3;
    let control_follower = (leaders[0] + 1) % 3;
    let yaml = r#"apiVersion: yadb.io/v1alpha1
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
kind: Project
metadata: {name: beta}
spec: {quotas: {maxStorageBytes: 100000000, maxIops: 1000}, members: [{subject: tester, role: readWrite}]}
---
apiVersion: yadb.io/v1alpha1
kind: Database
metadata: {name: db, project: alpha}
spec: {walPool: data, dataPool: data, isolation: shared, shards: 2}
---
apiVersion: yadb.io/v1alpha1
kind: Database
metadata: {name: db, project: beta}
spec: {walPool: data, dataPool: data, isolation: shared, shards: 2}
---
apiVersion: yadb.io/v1alpha1
kind: ObjectStore
metadata: {name: store, project: alpha}
spec: {pool: data, isolation: shared, buckets: [bucket]}
---
apiVersion: yadb.io/v1alpha1
kind: ObjectStore
metadata: {name: store, project: beta}
spec: {pool: data, isolation: shared, buckets: [bucket]}
---
apiVersion: yadb.io/v1alpha1
kind: BlockVolume
metadata: {name: disk, project: alpha}
spec: {pool: data, sizeBytes: 8192}
"#;
    ok(
        public[0],
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml":yaml})).unwrap(),
        "application/json",
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut caught_up = true;
        for &address in public {
            let state: Value =
                serde_json::from_slice(&ok(address, "GET", "/v1/status", b"", "application/json"))
                    .unwrap();
            if state["projects"]["alpha"].is_null() || state["projects"]["beta"].is_null() {
                caught_up = false;
            }
        }
        if caught_up {
            break;
        }
        assert!(Instant::now() < deadline, "control cache did not catch up");
        std::thread::sleep(Duration::from_millis(50));
    }
    for (project, leader) in [("alpha", leaders[1]), ("beta", leaders[2])] {
        let follower = (leader + 1) % 3;
        for sql in [
            "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)".to_string(),
            format!("INSERT INTO t VALUES (1,'{project}')"),
        ] {
            let (status, headers, body) = request_headers(
                public[follower],
                "POST",
                &format!("/db/{project}/db/query"),
                &serde_json::to_vec(&json!({"sql":sql})).unwrap(),
                "application/json",
            );
            assert_eq!(status, 200, "SQL: {}", String::from_utf8_lossy(&body));
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains(&format!("x-yadb-leader-id: {}", leader + 1)),
                "wrong forwarding response: {headers}"
            );
        }
        let (status, headers, body) = request_headers(
            public[follower],
            "PUT",
            &format!("/s3/{project}/store/bucket/item"),
            project.as_bytes(),
            "application/octet-stream",
        );
        assert_eq!(status, 200, "S3: {}", String::from_utf8_lossy(&body));
        assert!(
            headers
                .to_ascii_lowercase()
                .contains(&format!("x-yadb-leader-id: {}", leader + 1)),
            "wrong forwarding response: {headers}"
        );
    }
    let forwarded = "x-yadb-forwarded: 1\r\n";
    let sql = serde_json::to_vec(&json!({"sql":"INSERT INTO t VALUES (99,'malicious')"})).unwrap();
    let control = serde_json::to_vec(&json!({"yaml":"apiVersion: yadb.io/v1alpha1\nkind: StoragePool\nmetadata: {name: injected}\nspec: {erasure: {scheme: mirror, replicas: 3}, failureDomains: 3}\n"})).unwrap();
    for (node, method, path, body, ty) in [
        (
            data_follower,
            "POST",
            "/db/alpha/db/query",
            sql.as_slice(),
            "application/json",
        ),
        (
            data_follower,
            "PUT",
            "/s3/alpha/store/bucket/evil",
            b"evil".as_slice(),
            "application/octet-stream",
        ),
        (
            data_follower,
            "PUT",
            "/block/alpha/disk?offset=0",
            b"evil".as_slice(),
            "application/octet-stream",
        ),
        (
            control_follower,
            "POST",
            "/v1/apply",
            control.as_slice(),
            "application/json",
        ),
        (
            control_follower,
            "PUT",
            "/s3/unknown/store/bucket/evil",
            b"evil".as_slice(),
            "application/octet-stream",
        ),
    ] {
        let (status, headers, response) =
            request_extra(public[node], method, path, body, ty, forwarded);
        // The contract this asserts: `maybe_forward_write` treats a request that
        // already carries `x-yadb-forwarded` as addressed to this node, and
        // refuses it when this node is not that group's leader. It never falls
        // through to a local mutation, so the only admissible outcomes are a
        // refusal (`not-sent`) or a refusal with no outcome claimed at all.
        // "unknown" would mean the request left this node, which cannot happen
        // on this path.
        assert_eq!(
            status,
            503,
            "forwarded {method} {path}: {status} {}",
            String::from_utf8_lossy(&response)
        );
        let headers = headers.to_ascii_lowercase();
        assert!(
            !headers.contains("x-yadb-write-outcome: unknown")
                && !headers.contains("x-yadb-write-outcome: committed"),
            "a refused forwarded write claimed an outcome: {headers}"
        );
        assert!(
            !headers.contains("x-yadb-retry: do-not-retry"),
            "a never-sent refusal asked the client not to retry: {headers}"
        );
    }
    let query = serde_json::to_vec(&json!({"sql":"SELECT v FROM t WHERE id = 99"})).unwrap();
    assert!(!String::from_utf8_lossy(&ok(
        public[1],
        "POST",
        "/db/alpha/db/query",
        &query,
        "application/json"
    ))
    .contains("malicious"));
    assert!(
        request(
            public[1],
            "GET",
            "/s3/alpha/store/bucket/evil",
            b"",
            "application/json"
        )
        .0 >= 400
    );
    assert_eq!(
        ok(
            public[1],
            "GET",
            "/block/alpha/disk?offset=0&len=4",
            b"",
            "application/json"
        ),
        vec![0; 4]
    );
    // The refused `/v1/apply` carried a StoragePool. `/v1/status` reports the
    // control-plane state this node has *applied locally*, so checking every
    // node proves the refusal did not commit on the follower that received it
    // (nor anywhere else).
    for &node in &[0usize, 1, 2] {
        let state: Value = serde_json::from_slice(&ok(
            public[node],
            "GET",
            "/v1/status",
            b"",
            "application/json",
        ))
        .unwrap();
        assert!(
            state["pools"]["injected"].is_null(),
            "node {} applied the refused pool: {}",
            node + 1,
            state["pools"]
        );
    }
    for &node in &[0usize, 1] {
        // A consistent read is served from a quorum-confirmed extent map on the
        // project-group leader. It must return the same bytes as the plain read
        // above, not 501 and not stale data.
        let (status, body) = request(
            public[node],
            "GET",
            "/block/alpha/disk?offset=0&len=4&consistent=true",
            b"",
            "application/json",
        );
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        assert_eq!(body, vec![0; 4]);
    }
    let unmap = serde_json::to_vec(&json!({"offset": 8192, "len": 4096})).unwrap();
    for &node in &[0usize, 1] {
        let (status, body) = request(
            public[node],
            "POST",
            "/block/alpha/disk/unmap",
            &unmap,
            "application/json",
        );
        assert_eq!(status, 416, "{}", String::from_utf8_lossy(&body));
    }
    for (node, method, path, body, ty) in [
        (
            leaders[1],
            "POST",
            "/db/alpha/db/query",
            sql.as_slice(),
            "application/json",
        ),
        (
            leaders[1],
            "PUT",
            "/s3/alpha/store/bucket/good",
            b"good".as_slice(),
            "application/octet-stream",
        ),
        (
            leaders[1],
            "PUT",
            "/block/alpha/disk?offset=0",
            b"good".as_slice(),
            "application/octet-stream",
        ),
        (
            leaders[0],
            "POST",
            "/v1/apply",
            control.as_slice(),
            "application/json",
        ),
    ] {
        let (status, _, response) = request_extra(public[node], method, path, body, ty, forwarded);
        assert_eq!(
            status,
            200,
            "forwarded {method} {path}: {}",
            String::from_utf8_lossy(&response)
        );
    }
    // Stop the actual data-2 leader, whether preferred or not.
    let stopped = leaders[1];
    let observer = (stopped + 1) % 3;
    daemons[stopped].interrupt();
    let deadline = Instant::now() + Duration::from_secs(20);
    let failover_leader = loop {
        let status: Value = serde_json::from_slice(&ok(
            public[observer],
            "GET",
            "/openraft/status",
            b"",
            "application/json",
        ))
        .unwrap();
        let group = status["local_observations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["group"] == 2)
            .unwrap();
        if let Some(leader) = group["leader_id"].as_u64() {
            if leader != (stopped + 1) as u64 {
                break leader;
            }
        }
        assert!(Instant::now() < deadline, "failover timed out: {status}");
        std::thread::sleep(Duration::from_millis(200));
    };
    // `data` is a three-replica mirror and one replica is down, so no write can
    // reach full redundancy. The strict contract refuses such a write instead of
    // acknowledging one that a single further loss would destroy. The refusal is
    // definitive: nothing is published, so the row must not be readable either.
    let post_failover =
        serde_json::to_vec(&json!({"sql":"INSERT INTO t VALUES (101,'after-failover')"})).unwrap();
    let (status, body) = request(
        public[failover_leader as usize - 1],
        "POST",
        "/db/alpha/db/query",
        &post_failover,
        "application/json",
    );
    assert_eq!(
        status,
        503,
        "a write with a missing replica must be refused: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        String::from_utf8_lossy(&body).contains("full redundancy"),
        "unexpected refusal body: {}",
        String::from_utf8_lossy(&body)
    );
    let refused_readback =
        serde_json::to_vec(&json!({"sql":"SELECT v FROM t WHERE id = 101"})).unwrap();
    let refused_rows = ok(
        public[failover_leader as usize - 1],
        "POST",
        "/db/alpha/db/query",
        &refused_readback,
        "application/json",
    );
    assert!(
        !String::from_utf8_lossy(&refused_rows).contains("after-failover"),
        "a refused write must not become visible: {}",
        String::from_utf8_lossy(&refused_rows)
    );
    for group in 1..=3 {
        let path = dir
            .0
            .join(format!("node-{}-group-{group}.json", stopped + 1));
        let mut value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["mode"] = json!("open");
        value["initialize"] = json!(false);
        config(&path, &value);
    }
    daemons[stopped] = spawn_cluster(
        &dir.0.join(format!("data-{}", stopped + 1)),
        &manifests[stopped],
        (stopped + 1) as u32,
        public,
        &dir.0.join(format!("node-{}.log", stopped + 1)),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if TcpStream::connect_timeout(&public[stopped], Duration::from_millis(100)).is_ok() {
            let status: Value = serde_json::from_slice(&ok(
                public[stopped],
                "GET",
                "/openraft/status",
                b"",
                "application/json",
            ))
            .unwrap();
            let group = status["local_observations"]
                .as_array()
                .unwrap()
                .iter()
                .find(|g| g["group"] == 2)
                .unwrap();
            if group["leader_id"] == failover_leader {
                assert_eq!(group["preferred_leader"], 2);
                assert_eq!(group["preference_met"], failover_leader == 2);
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "returning node did not observe failover leader"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    // With the replica back the same statement is fully redundant again. A 503
    // refusal is definitive — nothing was published — so retrying it cannot
    // double-apply: the row becomes visible only once a write really lands.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (status, _, body) = request_headers(
            public[failover_leader as usize - 1],
            "POST",
            "/db/alpha/db/query",
            &post_failover,
            "application/json",
        );
        if status == 200 {
            break;
        }
        assert_eq!(
            status,
            503,
            "unexpected status after the replica returned: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            Instant::now() < deadline,
            "pool never regained full redundancy: {}",
            String::from_utf8_lossy(&body)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // The returned replica answers reads from its own applied state, so it can
    // trail the commit briefly; wait for it instead of reading the leader and
    // calling that a replica check.
    let readback = serde_json::to_vec(&json!({"sql":"SELECT v FROM t WHERE id = 101"})).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let response = ok(
            public[stopped],
            "POST",
            "/db/alpha/db/query",
            &readback,
            "application/json",
        );
        if String::from_utf8_lossy(&response).contains("after-failover") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "returned replica missed committed row: {}",
            String::from_utf8_lossy(&response)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // Revocation must reach authorization on every node. Once each local
    // control cache shows the new membership, the old subject is denied.
    let revoke = r#"apiVersion: yadb.io/v1alpha1
kind: Project
metadata: {name: alpha}
spec: {quotas: {maxStorageBytes: 100000000, maxIops: 1000}, members: []}
"#;
    ok(
        public[0],
        "POST",
        "/v1/apply",
        &serde_json::to_vec(&json!({"yaml":revoke})).unwrap(),
        "application/json",
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut revoked_everywhere = true;
        for &address in public {
            let state: Value =
                serde_json::from_slice(&ok(address, "GET", "/v1/status", b"", "application/json"))
                    .unwrap();
            let members = state["projects"]["alpha"][1]["members"].as_array();
            if !members.is_some_and(|members| members.is_empty()) {
                revoked_everywhere = false;
            }
        }
        if revoked_everywhere {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "revocation did not reach all nodes"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let denied = serde_json::to_vec(&json!({"sql":"SELECT v FROM t WHERE id = 101"})).unwrap();
    let (status, body) = request(
        public[0],
        "POST",
        "/db/alpha/db/query",
        &denied,
        "application/json",
    );
    assert_eq!(status, 403, "{}", String::from_utf8_lossy(&body));
    for daemon in &mut daemons {
        daemon.interrupt();
    }
    drop(daemons);
    for (group, pid) in [(2u64, 1), (3, 2)] {
        let identity = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [82; 16],
                group,
                incarnation: 1,
            },
            group as u32,
            vec![1, 2, 3],
        )
        .unwrap();
        let path = dir.0.join(format!(
            "metadata-{group}/{}-{group}-1-{group}/state",
            "52".repeat(16)
        ));
        let state = DurableMetaStateMachine::open(path, &identity).unwrap();
        let snapshot = state.try_snapshot(&[], &["/".into()]).unwrap();
        assert!(!snapshot.scan_prefix(&format!("/db/{pid}/")).is_empty());
        assert!(!snapshot.scan_prefix(&format!("/s3/{pid}/")).is_empty());
        assert!(snapshot
            .scan_prefix(&format!("/db/{}/", 3 - pid))
            .is_empty());
    }
    assert!(
        started.elapsed() < Duration::from_secs(120),
        "test exceeded overall budget"
    );
}

/// A deployment whose admin allowlist can never be satisfied must refuse to
/// start, and each refusal has to name the specific missing value instead of
/// failing later for a less informative reason. `/v1/apply` authorization
/// against a live cluster is covered by
/// `daemon_multigroup_http_shutdown_reopen_and_root_guards`; this test owns only
/// the startup contract, so it needs no cluster and no manifest.
#[test]
fn startup_refuses_an_admin_allowlist_it_cannot_verify() {
    let _serial = serialize_daemon_tests();
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-admin-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let dir = Dir(root.canonicalize().unwrap());
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let public = reservation.local_addr().unwrap();
    drop(reservation);
    let data = dir.0.join("data");

    // The manifest path is a placeholder: every check below runs before the
    // manifest is read, so reaching one of them proves the omitted value is what
    // the daemon refuses on.
    let manifest = dir.0.join("missing-manifest.json");
    let refused = |tag: &str, omit: &str| -> String {
        let log = dir.0.join(format!("{tag}.log"));
        let stderr = std::fs::File::create(&log).unwrap();
        let mut args: Vec<String> = vec![
            "--listen".into(),
            public.to_string(),
            "--data-dir".into(),
            data.to_str().unwrap().into(),
            "--openraft-config".into(),
            manifest.to_str().unwrap().into(),
            "--peers".into(),
            format!("1={public}"),
            "--raft-token".into(),
            "integration-test-token".into(),
            "--subject-secret".into(),
            SUBJECT_SECRET.into(),
            "--admin-subject".into(),
            "admin".into(),
        ];
        let index = args
            .iter()
            .position(|a| a == omit)
            .unwrap_or_else(|| panic!("{tag}: {omit} is not in the argument list"));
        args.drain(index..=index + 1);
        let child = Command::new(env!("CARGO_BIN_EXE_yadbd"))
            .args(&args)
            .env_remove("YADB_SUBJECT_SECRET")
            .env_remove("YADB_RAFT_TOKEN")
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap();
        let mut daemon = Daemon(child);
        assert!(
            !daemon.wait(Duration::from_secs(10)).success(),
            "{tag}: a daemon missing {omit} must refuse to start"
        );
        drop(daemon);
        std::fs::read_to_string(&log).unwrap()
    };

    // An allowlist without a secret can never verify a subject.
    let text = refused("no-secret", "--subject-secret");
    assert!(
        text.contains("--subject-secret"),
        "unexpected startup error: {text}"
    );
    // An empty allowlist would refuse every control-plane mutation.
    let text = refused("no-admin", "--admin-subject");
    assert!(
        text.contains("--admin-subject"),
        "unexpected startup error: {text}"
    );
    // The internal endpoints are unauthenticated without a shared secret.
    let text = refused("no-token", "--raft-token");
    assert!(
        text.contains("--raft-token"),
        "unexpected startup error: {text}"
    );
    // OpenRaft needs an explicit new root, never an implicit one.
    let text = refused("no-data-dir", "--data-dir");
    assert!(
        text.contains("--data-dir"),
        "unexpected startup error: {text}"
    );
    // And an explicit multi-group manifest, never a default configuration.
    let text = refused("no-config", "--openraft-config");
    assert!(
        text.contains("--openraft-config"),
        "unexpected startup error: {text}"
    );

    // Every value present but the manifest absent: the daemon must refuse rather
    // than invent a configuration or bootstrap itself.
    let log = dir.0.join("missing-manifest.log");
    let stderr = std::fs::File::create(&log).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_yadbd"))
        .args([
            "--listen",
            &public.to_string(),
            "--data-dir",
            data.to_str().unwrap(),
            "--openraft-config",
            manifest.to_str().unwrap(),
            "--peers",
            &format!("1={public}"),
            "--raft-token",
            "integration-test-token",
            "--subject-secret",
            SUBJECT_SECRET,
            "--admin-subject",
            "admin",
        ])
        .env_remove("YADB_SUBJECT_SECRET")
        .env_remove("YADB_RAFT_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap();
    let mut daemon = Daemon(child);
    assert!(
        !daemon.wait(Duration::from_secs(10)).success(),
        "a missing manifest must refuse to start, not fall back to a default"
    );
    drop(daemon);
    assert!(
        !std::fs::read_to_string(&log).unwrap().is_empty(),
        "a refused start must say why"
    );
}

/// `/openraft/status` names every local group, so a deployment that configured
/// `--subject-secret` must not answer an unverified caller.
#[test]
fn openraft_status_requires_verified_subject() {
    let _serial = serialize_daemon_tests();
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!(
        "yadb-daemon-status-auth-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let dir = Dir(root.canonicalize().unwrap());
    let metadata = dir.0.join("metadata");
    std::fs::create_dir(&metadata).unwrap();
    let data = dir.0.join("client-data");
    let cert_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../yadb-metakv/testdata/tls")
        .canonicalize()
        .unwrap();
    let cert = std::fs::read(cert_dir.join("node-1.test.cert.der")).unwrap();
    let reservations: Vec<_> = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addrs: Vec<_> = reservations
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    let mut paths = Vec::new();
    for group in 1..=3usize {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let key = dir.0.join(format!("signing-{group}.der"));
        std::fs::write(&key, pkcs8.as_ref()).unwrap();
        let path = dir.0.join(format!("group-{group}.json"));
        config(
            &path,
            &json!({"version":1,"root":metadata,"mode":"create","initialize":true,"cluster":vec![81;16],"group":group,"incarnation":1,"node":1,"listen":addrs[group],"signing_key_der":key,
            "tls":{"roots_der":[cert_dir.join("ca.der")],"chain_der":[cert_dir.join("node-1.test.cert.der")],"key_der":cert_dir.join("node-1.test.key.der")},
            "peers":[{"id":1,"address":addrs[group],"public_key":pair.public_key().as_ref(),"server_name":"node-1.test","cert_sha256":MutualTls::fingerprint(&cert)}]}),
        );
        paths.push(path);
    }
    let manifest = dir.0.join("multi.json");
    config(
        &manifest,
        &json!({"version":2,"control_group":1,"project_groups":{"1":2,"2":3},"groups":paths}),
    );
    drop(reservations);
    let public = addrs[0];
    let log = dir.0.join("daemon.log");
    let mut daemon = spawn(&data, Some(&manifest), public, &log);
    ready(&mut daemon, public);

    // No identity at all is unverified.
    let (status, _, response) = request_with_identity(
        public,
        "GET",
        "/openraft/status",
        b"",
        "application/json",
        "",
        "",
    );
    assert_eq!(
        status,
        401,
        "unsigned status: {}",
        String::from_utf8_lossy(&response)
    );
    // A forged signature must not be accepted.
    let forged = format!(
        "x-yadb-subject: {SUBJECT}\r\nx-yadb-subject-signature: {}\r\n",
        "00".repeat(32)
    );
    let (status, _, response) = request_with_identity(
        public,
        "GET",
        "/openraft/status",
        b"",
        "application/json",
        &forged,
        "",
    );
    assert_eq!(
        status,
        401,
        "forged status signature: {}",
        String::from_utf8_lossy(&response)
    );
    // The signed subject the suite uses is accepted and the body is unchanged.
    let signed = format!(
        "x-yadb-subject: {SUBJECT}\r\nx-yadb-subject-signature: {}\r\n",
        subject_signature(SUBJECT)
    );
    let (status, _, body) = request_with_identity(
        public,
        "GET",
        "/openraft/status",
        b"",
        "application/json",
        &signed,
        "",
    );
    assert_eq!(
        status,
        200,
        "signed status: {}",
        String::from_utf8_lossy(&body)
    );
    let status: Value = serde_json::from_slice(&body).unwrap();
    assert!(status["local_observations"]
        .as_array()
        .is_some_and(|groups| groups.len() == 3));

    daemon.interrupt();
}
