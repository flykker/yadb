//! A `PoolStriper` over several independent loopback HTTP servers, each backed
//! by its own `VolumeStore` in its own temporary directory.
//!
//! The existing in-crate tests drive remote slots against a `BTreeMap` mock
//! (`MockVdisk` in `src/lib.rs`), and `crates/yadbd/tests/openraft_daemon.rs`
//! drives the daemon end to end. Neither observes what each *server* actually
//! wrote for a pool write. This fixture does: every assertion about the
//! fragments is made against the node's own `VolumeStore`, read back through
//! `get` (which verifies the stored CRC), never against the striper's
//! bookkeeping.
//!
//! The fixture speaks the exact wire contract `PoolSlot` uses (see `pool.rs`):
//!
//! * `PUT|GET|DELETE /internal/vdisk/{pool}/{slot}/blob/{hex32}` with the
//!   fragment as the raw body; `PUT` answers `200` (or `201`) only after
//!   `VolumeStore::put` synced the record, `GET` answers the raw bytes or
//!   `404`/`422`, and `DELETE` answers the literal bodies `deleted`/`absent`;
//! * `GET /epoch` -> `{"epoch":N}`, `POST /pin` -> `{"pin_id","epoch",
//!   "expires_at_ms"}`, `POST /unpin` -> `{"released":bool}`,
//!   `GET /pins` -> `{"pins":[...]}`, `POST /advance` -> `{"epoch":N}`;
//! * `GET /stats` -> the JSON `VolumeStats`, `GET /blobs` ->
//!   `{"ids":["<hex32>",...]}`, `POST /barrier` and `POST /compact`;
//! * every route requires `x-yadb-raft-token` when the slot has a token, and
//!   an unknown `(pool, slot)` is `404`.
//!
//! What this fixture does **not** prove:
//!
//! * separate hosts or a network partition. Each node is a thread on
//!   `127.0.0.1` with its own listener, its own `VolumeStore` and its own
//!   start/stop lifecycle, which is process-*equivalent* but not a separate
//!   process, host or failure domain, and every packet crosses the loopback
//!   stack rather than a real network;
//! * OS or device power loss. `VolumeStore` fsyncs to the local filesystem and
//!   nothing here cuts power, so durability is "the server answered 200 after
//!   its own `put` returned", not "the bytes survive a crash";
//! * the daemon's placement, epoch, barrier or collector logic. Slots are
//!   constructed directly with `PoolSlot::remote`, so `materialize_pools`,
//!   ClusterSpec/`--peers` placement, the configured raft token and the GC
//!   collector are not exercised;
//! * a real stopped process. A stopped node here keeps its listening socket
//!   bound (so the slot address stays stable) and accepts nothing, so the
//!   client sees a read timeout where a dead process would give
//!   `ECONNREFUSED`. Both are transport failures, and `PoolSlot` maps both to
//!   `BlobError::Io`, the only variant `is_unavailable()` accepts, which is
//!   exactly the class this test needs.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use yadb_blob::{
    BarrierRequest, BlobError, BlobId, ErasureCodec, ErasureScheme, PinId, PoolHealthState,
    PoolSlot, PoolStriper, VolumeConfig, VolumeStore,
};
use yadb_io::OsFs;

/// The token every node requires and every slot presents.
const RAFT_TOKEN: &str = "remote-pool-fixture-token";

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

/// A node's own directory, removed when the node is dropped.
struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(tag: &str) -> Self {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let path = base.join(format!(
            "yadb-blob-remote-pool-{}-{tag}-{}",
            std::process::id(),
            NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("create the node's temp root");
        Self { path }
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// One independent storage node: its own listener, its own `VolumeStore` in its
/// own directory, and a serving lifecycle that can be stopped and started.
struct Node {
    pool: String,
    slot: usize,
    addr: SocketAddr,
    store: Arc<VolumeStore<OsFs>>,
    listener: Arc<TcpListener>,
    serving: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
    seen: Arc<Mutex<Vec<String>>>,
    // Declared last so the directory outlives the store that writes into it.
    _root: TempRoot,
}

impl Node {
    fn new(tag: &str, pool: &str, slot: usize) -> Self {
        let root = TempRoot::new(tag);
        let fs = OsFs::new(root.path.join("fs")).expect("open the node's filesystem root");
        let store = Arc::new(
            VolumeStore::open(fs, "volume", VolumeConfig::default())
                .expect("open the node's volume"),
        );
        let listener =
            Arc::new(TcpListener::bind("127.0.0.1:0").expect("bind the node's listener"));
        let addr = listener.local_addr().expect("node address");
        let mut node = Self {
            pool: pool.to_string(),
            slot,
            addr,
            store,
            listener,
            serving: Arc::new(AtomicBool::new(false)),
            handle: None,
            seen: Arc::new(Mutex::new(Vec::new())),
            _root: root,
        };
        node.start();
        node
    }

    /// Starts serving. The listener is kept for the node's whole life, so a
    /// stop/start cycle reuses the same address without a rebind.
    fn start(&mut self) {
        assert!(self.handle.is_none(), "node is already serving");
        // Connections queued while the node was stopped carry requests the
        // client already gave up on. Discard them before answering anything:
        // replaying a PUT that the pool reported as failed would make the
        // refusal's on-disk state depend on when the node came back.
        self.drain_pending();
        self.serving.store(true, Ordering::SeqCst);
        let listener = self.listener.clone();
        let serving = self.serving.clone();
        let store = self.store.clone();
        let seen = self.seen.clone();
        let pool = self.pool.clone();
        let slot = self.slot;
        self.handle = Some(std::thread::spawn(move || loop {
            let Ok((stream, _)) = listener.accept() else {
                break;
            };
            if !serving.load(Ordering::SeqCst) {
                drop(stream);
                break;
            }
            let store = store.clone();
            let seen = seen.clone();
            let pool = pool.clone();
            std::thread::spawn(move || handle_connection(stream, &store, &pool, slot, &seen));
        }));
    }

    /// Stops the accept loop. The listening socket stays bound and nothing is
    /// accepted, so a client's request is never answered: `PoolSlot` sees a
    /// transport failure, exactly the class a dead process produces.
    fn stop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        self.serving.store(false, Ordering::SeqCst);
        // Wake the blocking `accept`; the loop drops the connection and exits.
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(500));
        handle.join().expect("node accept loop");
        self.drain_pending();
    }

    fn drain_pending(&self) {
        self.listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        while self.listener.accept().is_ok() {}
        self.listener
            .set_nonblocking(false)
            .expect("blocking listener");
    }

    /// Request lines this node answered or was asked, in order.
    fn requests(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

/// A pool whose every slot is a separate node, so a write really crosses TCP.
struct Fixture {
    nodes: Vec<Node>,
    striper: PoolStriper<OsFs>,
}

impl Fixture {
    fn new(tag: &str, scheme: ErasureScheme, quorum: Option<usize>) -> Self {
        let total = scheme.total_parts();
        let mut nodes = Vec::with_capacity(total);
        let mut slots = Vec::with_capacity(total);
        for index in 0..total {
            let node = Node::new(&format!("{tag}-node{index}"), "pool", index);
            slots.push(PoolSlot::remote(
                "pool",
                index,
                format!("domain-{index}"),
                index as u32 + 1,
                node.addr.to_string(),
                Some(RAFT_TOKEN.to_string()),
            ));
            nodes.push(node);
        }
        let striper = PoolStriper::from_slots("pool", scheme, slots)
            .expect("a fixture with one slot per part");
        let striper = match quorum {
            Some(quorum) => striper
                .with_min_write_quorum(quorum)
                .expect("quorum inside the read threshold and total parts"),
            None => striper,
        };
        Self { nodes, striper }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node.stop();
        }
    }
}

/// What a node's own store holds, keyed by fragment part number. Read back with
/// `get`, so a fragment that does not verify its CRC is not counted as held.
fn held_fragments(node: &Node) -> BTreeMap<u8, Vec<u8>> {
    let mut held = BTreeMap::new();
    for id in node.store.live_ids() {
        let bytes = node
            .store
            .get(id)
            .expect("read a fragment back from the node's own store");
        held.insert(id.part(), bytes);
    }
    held
}

/// Every node holds exactly one fragment, those fragments are the exact parts
/// of `payload`, and together they cover every part exactly once. This is the
/// assertion that a premature ack cannot satisfy: it asks each server, not the
/// striper, what it has.
fn assert_every_node_holds_its_own_fragment(
    fixture: &Fixture,
    id: BlobId,
    payload: &[u8],
    scheme: ErasureScheme,
) {
    let expected = ErasureCodec::new(scheme).expect("codec").encode(payload);
    let mut parts = Vec::with_capacity(fixture.nodes.len());
    for (index, node) in fixture.nodes.iter().enumerate() {
        let held = held_fragments(node);
        assert_eq!(
            held.len(),
            1,
            "node {index} must hold exactly one fragment of {id}, holds parts {:?}",
            held.keys().collect::<Vec<_>>()
        );
        let (&part, bytes) = held.iter().next().expect("exactly one fragment");
        assert_eq!(
            bytes,
            &expected[(part - 1) as usize],
            "node {index} holds bytes that are not part {part} of the payload"
        );
        assert_eq!(
            node.store.live_ids(),
            vec![id.with_part(part)],
            "node {index} must hold only {id} as part {part}"
        );
        parts.push(part);
    }
    parts.sort_unstable();
    let all_parts: Vec<u8> = (1..=scheme.total_parts() as u8).collect();
    assert_eq!(
        parts, all_parts,
        "every part must be on exactly one node, so the ack covers the whole payload"
    );
}

/// A payload with no repetition, so a reassembly bug cannot pass by accident.
fn payload() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4096);
    for index in 0..4096u32 {
        bytes.push((index.wrapping_mul(37).wrapping_add(index >> 8) & 0xFF) as u8);
    }
    bytes
}

#[test]
fn a_remote_pool_acks_only_when_every_server_holds_its_fragment() {
    let scheme = ErasureScheme::ReedSolomon { k: 3, m: 2 };
    let fixture = Fixture::new("ack", scheme, None);
    let id = BlobId::new(61, 1, 1, 1, 2, 0);
    let payload = payload();

    fixture
        .striper
        .put_blob(id, &payload)
        .expect("a healthy remote pool must ack");
    assert_eq!(fixture.striper.refused_put_count(), 0);
    assert_eq!(fixture.striper.degraded_put_count(), 0);
    assert_eq!(
        fixture.striper.min_write_quorum(),
        scheme.total_parts(),
        "the shipped default is every fragment"
    );

    // Each node really is a separate server on its own port and was asked for
    // its own part at the path the slot owns.
    for (slot, node) in fixture.nodes.iter().enumerate() {
        let requests = node.requests();
        assert_eq!(requests.len(), 1, "slot {slot} requests: {requests:?}");
        let held = held_fragments(node);
        assert_eq!(
            held.len(),
            1,
            "node {slot} must hold its fragment after an acked write, holds parts {:?}",
            held.keys().collect::<Vec<_>>()
        );
        let (&part, _) = held.iter().next().expect("exactly one fragment");
        assert_eq!(
            requests[0],
            format!(
                "PUT /internal/vdisk/pool/{slot}/blob/{}",
                id.with_part(part).to_hex()
            ),
            "slot {slot} must be asked for exactly the fragment it stores"
        );
    }

    assert_every_node_holds_its_own_fragment(&fixture, id, &payload, scheme);
    assert_eq!(
        fixture.striper.get_blob(id).expect("read the blob back"),
        payload,
        "the reassembled bytes must be the exact payload"
    );
}

#[test]
fn a_stopped_server_refuses_the_write_and_leaves_no_ack() {
    let scheme = ErasureScheme::ReedSolomon { k: 3, m: 2 };
    let mut fixture = Fixture::new("refused", scheme, None);
    let id = BlobId::new(61, 2, 1, 1, 2, 0);
    let payload = payload();
    let stopped = 2;

    fixture.nodes[stopped].stop();
    match fixture.striper.put_blob(id, &payload) {
        Err(BlobError::RedundancyNotMet {
            id: refused,
            written,
            required,
        }) => {
            assert_eq!(refused, id);
            assert_eq!(written, 4, "the four reachable nodes took their fragments");
            assert_eq!(required, 5, "the default quorum is every fragment");
        }
        other => panic!("expected RedundancyNotMet with one server stopped, got {other:?}"),
    }
    assert_eq!(
        fixture.striper.refused_put_count(),
        1,
        "a refused write must be counted"
    );
    assert_eq!(fixture.striper.degraded_put_count(), 0);
    let health = fixture.striper.health_snapshot(false);
    assert_eq!(health.refused_puts, 1);
    assert_eq!(health.min_write_slots, 5);
    assert_eq!(health.online_slots, 4);
    assert_eq!(health.status, PoolHealthState::Degraded);

    // There is no manifest in this crate: the ack *is* the publication step, and
    // it did not happen. What landed is therefore unreferenced. Assert exactly
    // what is on disk rather than assuming: the stopped node holds nothing, and
    // each of the four nodes that answered holds its own verified fragment.
    let expected = ErasureCodec::new(scheme).expect("codec").encode(&payload);
    let mut landed = 0;
    for (index, node) in fixture.nodes.iter().enumerate() {
        let held = held_fragments(node);
        if index == stopped {
            assert!(
                held.is_empty(),
                "the stopped node must hold no fragment, holds parts {:?}",
                held.keys().collect::<Vec<_>>()
            );
            continue;
        }
        assert_eq!(held.len(), 1, "node {index} holds parts {:?}", held.keys());
        let (&part, bytes) = held.iter().next().expect("exactly one fragment");
        assert_eq!(
            bytes,
            &expected[(part - 1) as usize],
            "node {index} holds bytes that are not part {part}"
        );
        landed += 1;
    }
    assert_eq!(landed, 4, "four of the five fragments are on disk");

    // The residual this test documents, not a property to preserve: the refusal
    // is not an erasure, and RS(3,2) decodes from the four fragments that did
    // land, so the refused id is still readable until a collector pass removes
    // them. See `docs/multiraft-production-todo.md` ("a refused write is still
    // readable by id until the next collection pass").
    assert_eq!(
        fixture
            .striper
            .get_blob(id)
            .expect("the four landed fragments still decode at k=3"),
        payload,
        "documented residual: refusal leaves the id readable while its fragments remain"
    );
}

#[test]
fn the_same_write_succeeds_after_the_stopped_server_returns() {
    let scheme = ErasureScheme::ReedSolomon { k: 3, m: 2 };
    let mut fixture = Fixture::new("recovered", scheme, None);
    let id = BlobId::new(61, 3, 1, 1, 2, 0);
    let payload = payload();
    let stopped = 3;

    fixture.nodes[stopped].stop();
    assert!(
        matches!(
            fixture.striper.put_blob(id, &payload),
            Err(BlobError::RedundancyNotMet { .. })
        ),
        "the write must be refused while one server is stopped"
    );
    assert_eq!(fixture.striper.refused_put_count(), 1);

    fixture.nodes[stopped].start();
    fixture
        .striper
        .put_blob(id, &payload)
        .expect("after the server returns the same write is fully redundant");
    assert_eq!(
        fixture.striper.refused_put_count(),
        1,
        "a successful retry does not erase the earlier refusal"
    );
    assert_eq!(fixture.striper.degraded_put_count(), 0);

    // The retry re-put the four fragments that had landed (same id, same bytes,
    // so `VolumeStore::put` is idempotent) and placed the missing fifth.
    assert_every_node_holds_its_own_fragment(&fixture, id, &payload, scheme);
    assert_eq!(
        fixture.striper.get_blob(id).expect("read the blob back"),
        payload,
        "the reassembled bytes must be the exact payload"
    );
}

#[test]
fn a_degraded_pool_acks_with_one_server_down_and_reads_from_the_survivors() {
    let scheme = ErasureScheme::ReedSolomon { k: 3, m: 2 };
    // 4 = total_parts() - 1, above the k=3 read threshold, so the pool may ack
    // one fragment short but must count it.
    let mut fixture = Fixture::new("degraded", scheme, Some(4));
    let id = BlobId::new(61, 4, 1, 1, 2, 0);
    let payload = payload();
    let stopped = 1;

    assert_eq!(fixture.striper.min_write_quorum(), 4);
    assert_eq!(fixture.striper.min_read_quorum(), 3);

    fixture.nodes[stopped].stop();
    fixture
        .striper
        .put_blob(id, &payload)
        .expect("a pool configured below full redundancy acks at its configured quorum");
    assert_eq!(
        fixture.striper.degraded_put_count(),
        1,
        "the ack promised four of five fragments and must be counted as degraded"
    );
    assert_eq!(fixture.striper.refused_put_count(), 0);
    let health = fixture.striper.health_snapshot(false);
    assert_eq!(health.degraded_puts, 1);
    assert_eq!(health.refused_puts, 0);
    assert_eq!(health.min_write_slots, 4);
    assert_eq!(health.online_slots, 4);

    // On disk: four survivors hold one verified fragment each, the stopped node
    // holds none. This is the deliberate contrast to the default-quorum test.
    let expected = ErasureCodec::new(scheme).expect("codec").encode(&payload);
    let mut landed = 0;
    for (index, node) in fixture.nodes.iter().enumerate() {
        let held = held_fragments(node);
        if index == stopped {
            assert!(held.is_empty(), "the stopped node holds no fragment");
            continue;
        }
        assert_eq!(held.len(), 1, "node {index} holds parts {:?}", held.keys());
        let (&part, bytes) = held.iter().next().expect("exactly one fragment");
        assert_eq!(bytes, &expected[(part - 1) as usize]);
        landed += 1;
    }
    assert_eq!(landed, 4);

    assert_eq!(
        fixture
            .striper
            .get_blob(id)
            .expect("k=3 of the four surviving fragments reconstruct the payload"),
        payload,
        "the degraded ack is still readable from the survivors"
    );

    // The counter is the repair backlog: once the node returns, a repair pass
    // restores the missing fragment and the blob is fully redundant again.
    fixture.nodes[stopped].start();
    let report = fixture
        .striper
        .repair_blob(id)
        .expect("repair reads the four survivors and rewrites the missing part");
    assert_eq!(report.intact_parts, 5);
    assert_eq!(report.repaired_parts, 1);
    assert_every_node_holds_its_own_fragment(&fixture, id, &payload, scheme);
    assert_eq!(fixture.striper.get_blob(id).expect("read back"), payload);
}

// ---------------------------------------------------------------------------
// The node: a minimal HTTP/1.1 server over one `VolumeStore`, matching the
// contract `PoolSlot` speaks.
// ---------------------------------------------------------------------------

struct Request {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

type Reply = (u16, &'static str, Vec<u8>);

fn handle_connection(
    mut stream: TcpStream,
    store: &VolumeStore<OsFs>,
    pool: &str,
    slot: usize,
    seen: &Mutex<Vec<String>>,
) {
    let _ = stream.set_read_timeout(Some(Duration::from_millis(2000)));
    let Some(request) = read_request(&mut stream) else {
        return;
    };
    seen.lock()
        .unwrap()
        .push(format!("{} {}", request.method, request.target));
    let (status, content_type, body) = route(store, pool, slot, &request);
    let _ = write_response(&mut stream, status, content_type, &body);
}

fn read_request(stream: &mut TcpStream) -> Option<Request> {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 4096];
    let header_end = loop {
        if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        raw.extend_from_slice(&buffer[..read]);
    };

    let head = String::from_utf8_lossy(&raw[..header_end]).into_owned();
    let mut body = raw[header_end + 4..].to_vec();
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let mut headers = BTreeMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let wanted: usize = headers
        .get("content-length")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    while body.len() < wanted {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&buffer[..read]);
    }
    body.truncate(wanted);
    Some(Request {
        method,
        target,
        headers,
        body,
    })
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reason(status),
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        412 => "Precondition Failed",
        422 => "Unprocessable Entity",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Error",
    }
}

fn json_reply(status: u16, code: &str, message: &str) -> Reply {
    (
        status,
        "application/json",
        json!({ "error": code, "message": message })
            .to_string()
            .into_bytes(),
    )
}

fn route(store: &VolumeStore<OsFs>, pool: &str, slot: usize, request: &Request) -> Reply {
    // The daemon requires the peer token on every vdisk route. A slot configured
    // with a token sends `x-yadb-raft-token`; anything else is refused here.
    if request.headers.get("x-yadb-raft-token").map(String::as_str) != Some(RAFT_TOKEN) {
        return json_reply(
            401,
            "unauthorized",
            "invalid or missing internal Raft token",
        );
    }

    let (path, query) = match request.target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (request.target.as_str(), ""),
    };
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if segments.len() < 5 || segments[0] != "internal" || segments[1] != "vdisk" {
        return json_reply(404, "not_found", "unknown internal route");
    }
    if segments[2] != pool || segments[3] != slot.to_string() {
        return json_reply(
            404,
            "slot_not_found",
            "local VDisk slot not hosted on this node",
        );
    }

    match (request.method.as_str(), &segments[4..]) {
        ("PUT", ["blob", hex]) => put_blob(store, hex, &request.body),
        ("GET", ["blob", hex]) => get_blob(store, hex),
        ("DELETE", ["blob", hex]) => delete_blob(store, hex, query),
        ("GET", ["stats"]) => match store.stats() {
            Ok(stats) => (
                200,
                "application/json",
                serde_json::to_vec(&stats).expect("serialize VolumeStats"),
            ),
            Err(error) => json_reply(500, "internal", &error.to_string()),
        },
        ("GET", ["blobs"]) => {
            let ids: Vec<String> = store.live_ids().into_iter().map(BlobId::to_hex).collect();
            (
                200,
                "application/json",
                json!({ "ids": ids }).to_string().into_bytes(),
            )
        }
        ("GET", ["epoch"]) => (
            200,
            "application/json",
            json!({ "epoch": store.epoch() }).to_string().into_bytes(),
        ),
        ("GET", ["pins"]) => match store.live_pins() {
            Ok(pins) => {
                let pins: Vec<Value> = pins
                    .into_iter()
                    .map(|pin| {
                        json!({
                            "pin_id": pin.pin_id.get(),
                            "epoch": pin.epoch,
                            "expires_at_ms": pin.expires_at_ms,
                        })
                    })
                    .collect();
                (
                    200,
                    "application/json",
                    json!({ "pins": pins }).to_string().into_bytes(),
                )
            }
            Err(error) => fence_error(error),
        },
        ("POST", ["pin"]) => match parse_pin(&request.body) {
            Some((epoch, ttl_ms)) => match store.pin(epoch, ttl_ms) {
                Ok(grant) => (
                    200,
                    "application/json",
                    json!({
                        "pin_id": grant.pin_id.get(),
                        "epoch": grant.epoch,
                        "expires_at_ms": grant.expires_at_ms,
                    })
                    .to_string()
                    .into_bytes(),
                ),
                Err(error) => fence_error(error),
            },
            None => json_reply(400, "bad_request", "pin body must be {epoch,ttl_ms}"),
        },
        ("POST", ["unpin"]) => match parse_unpin(&request.body) {
            Some(pin_id) => match store.unpin(PinId::from_wire(pin_id)) {
                Ok(released) => (
                    200,
                    "application/json",
                    json!({ "released": released }).to_string().into_bytes(),
                ),
                Err(error) => fence_error(error),
            },
            None => json_reply(400, "bad_request", "unpin body must be {pin_id}"),
        },
        ("POST", ["advance"]) => match parse_advance(&request.body) {
            Some(expected_epoch) => match store.advance_epoch(expected_epoch) {
                Ok(epoch) => (
                    200,
                    "application/json",
                    json!({ "epoch": epoch }).to_string().into_bytes(),
                ),
                Err(error) => fence_error(error),
            },
            None => json_reply(400, "bad_request", "advance body must be {expected_epoch}"),
        },
        ("POST", ["barrier"]) => match serde_json::from_slice::<BarrierRequest>(&request.body) {
            Ok(body) => match store.collect_garbage_barrier(
                body.project_id,
                body.owner_id,
                body.channel,
                body.up_to_generation,
                body.lease_expires_at_ms,
            ) {
                Ok(evicted) => {
                    let mut response = json!({ "evicted": evicted, "reclaimed_bytes": 0 });
                    if body.compact {
                        match store.seal_and_compact() {
                            Ok(bytes) => response["reclaimed_bytes"] = json!(bytes),
                            Err(error) => response["compaction_error"] = json!(error.to_string()),
                        }
                    }
                    (200, "application/json", response.to_string().into_bytes())
                }
                Err(error) => fence_error(error),
            },
            Err(error) => json_reply(400, "bad_request", &error.to_string()),
        },
        ("POST", ["compact"]) => match store.seal_and_compact() {
            Ok(reclaimed_bytes) => (
                200,
                "application/json",
                json!({ "reclaimed_bytes": reclaimed_bytes })
                    .to_string()
                    .into_bytes(),
            ),
            Err(error) => json_reply(500, "internal", &error.to_string()),
        },
        _ => json_reply(404, "not_found", "unknown internal route"),
    }
}

fn put_blob(store: &VolumeStore<OsFs>, hex: &str, body: &[u8]) -> Reply {
    let Some(id) = BlobId::from_hex(hex) else {
        return json_reply(400, "bad_blob_id", "invalid 32-char hex BlobId");
    };
    // The ack is sent only after `VolumeStore::put` returned, which is after the
    // record was appended and, with the default config, synced.
    match store.put(id, body) {
        Ok(()) => (200, "application/octet-stream", Vec::new()),
        Err(BlobError::ImmutableConflict(_)) => {
            json_reply(409, "immutable_conflict", "immutable blob conflict")
        }
        Err(BlobError::FencedByBarrier { .. }) => {
            json_reply(412, "fenced_by_barrier", "fenced by GC barrier")
        }
        Err(BlobError::Tombstoned(_)) => json_reply(412, "tombstoned", "immutable blob tombstoned"),
        Err(error) => json_reply(500, "internal", &error.to_string()),
    }
}

fn get_blob(store: &VolumeStore<OsFs>, hex: &str) -> Reply {
    let Some(id) = BlobId::from_hex(hex) else {
        return json_reply(400, "bad_blob_id", "invalid 32-char hex BlobId");
    };
    match store.get(id) {
        Ok(bytes) => (200, "application/octet-stream", bytes),
        Err(BlobError::NotFound(_)) => json_reply(404, "not_found", "fragment not found"),
        Err(BlobError::CorruptData { reason, .. }) => {
            json_reply(422, "corrupt_data", &reason.to_string())
        }
        Err(error) => json_reply(500, "internal", &error.to_string()),
    }
}

fn delete_blob(store: &VolumeStore<OsFs>, hex: &str, query: &str) -> Reply {
    let Some(id) = BlobId::from_hex(hex) else {
        return json_reply(400, "bad_blob_id", "invalid 32-char hex BlobId");
    };
    let mut fence_epoch = None;
    let mut collector_epoch = None;
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        match name {
            "fence_epoch" => fence_epoch = value.parse().ok(),
            "collector_epoch" => collector_epoch = value.parse().ok(),
            _ => {}
        }
    }
    let result = match (fence_epoch, collector_epoch) {
        (Some(_), Some(_)) => {
            return json_reply(
                400,
                "bad_fence_params",
                "fence_epoch and collector_epoch are mutually exclusive",
            )
        }
        (Some(expected), None) => store.delete_fenced(id, expected),
        (None, Some(collector)) => store.delete_collectable(id, collector),
        (None, None) => store.delete(id),
    };
    match result {
        Ok(true) => (200, "text/plain", b"deleted".to_vec()),
        Ok(false) => (200, "text/plain", b"absent".to_vec()),
        Err(error) => fence_error(error),
    }
}

fn parse_pin(body: &[u8]) -> Option<(u64, u64)> {
    let value: Value = serde_json::from_slice(body).ok()?;
    Some((
        value.get("epoch")?.as_u64()?,
        value.get("ttl_ms")?.as_u64()?,
    ))
}

fn parse_unpin(body: &[u8]) -> Option<u64> {
    serde_json::from_slice::<Value>(body)
        .ok()?
        .get("pin_id")?
        .as_u64()
}

fn parse_advance(body: &[u8]) -> Option<u64> {
    serde_json::from_slice::<Value>(body)
        .ok()?
        .get("expected_epoch")?
        .as_u64()
}

/// The fence endpoints' stable refusal envelope: a machine code plus a message.
/// The client only special-cases `storage_unavailable`; every other refusal is
/// still an answer, not a transport failure.
fn fence_error(error: BlobError) -> Reply {
    let (status, code) = match &error {
        BlobError::PinTtlZero => (400, "pin_ttl_zero"),
        BlobError::PinTtlTooLong { .. } => (400, "pin_ttl_too_long"),
        BlobError::EpochMismatch { .. } => (409, "epoch_mismatch"),
        BlobError::EpochPinned { .. } => (409, "epoch_pinned"),
        BlobError::EpochExhausted => (409, "epoch_exhausted"),
        BlobError::PinEpochNotCurrent { .. } => (409, "pin_epoch_not_current"),
        BlobError::PinIdsExhausted => (409, "pin_ids_exhausted"),
        BlobError::FencedByEpoch { .. } => (409, "fenced_by_epoch"),
        BlobError::FragmentNotOlder { .. } => (409, "fragment_not_older"),
        BlobError::FragmentPinned { .. } => (409, "fragment_pinned"),
        BlobError::PinClockUnreadable { .. } => (503, "pin_clock_unreadable"),
        BlobError::StorageUnavailable => (503, "storage_unavailable"),
        _ => (500, "internal"),
    };
    json_reply(status, code, &error.to_string())
}
