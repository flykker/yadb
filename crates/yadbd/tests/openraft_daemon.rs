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
        assert_eq!(v["production_ready"], false);
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
    // exactly one committed response. No HTTP hook can pause after snapshot.
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
        let (status, _, response) = request_extra(public[node], method, path, body, ty, forwarded);
        assert!(
            status >= 400,
            "forwarded {method} {path}: {status} {}",
            String::from_utf8_lossy(&response)
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
    let state: Value =
        serde_json::from_slice(&ok(public[0], "GET", "/v1/status", b"", "application/json"))
            .unwrap();
    assert!(state["pools"]["injected"].is_null());
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
