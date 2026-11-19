//! Authenticated OpenRaft network adapter over TCP.
//!
//! Every Raft RPC is carried in an Ed25519-signed envelope (see
//! [`crate::openraft_transport`]). The peer identity is bound to the
//! connection only when strict address checking is enabled; otherwise a
//! sender is verified by its trusted public key. Each connection uses a fresh
//! server challenge in the signed request and a client nonce in the response.
//!
//! `TransportConfig::with_tls` enables pinned TLS 1.3 mutual authentication.
//! Plaintext mode exists for isolated experiments only; there is no automatic
//! fallback from TLS to plaintext.

use std::collections::BTreeMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::error::{
    Fatal, NetworkError, RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use openraft::{SnapshotMeta, Vote};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinSet;

use crate::multiraft::ReplicaIdentity;
use crate::openraft_runtime::YadbRaftTypeConfig as C;
use crate::openraft_tls::MutualTls;
use crate::openraft_transport::{
    NodeSigningKey, PeerKeyRegistry, SignedRaftEnvelope, TransportError,
};

/// Maximum size of one wire frame, including the envelope overhead.
pub const MAX_FRAME: usize = 32 * 1024 * 1024;

/// Maximum number of concurrently served connections.
pub const MAX_CONNECTIONS: usize = 64;

/// Largest number of handler slots one source address may hold at once.
///
/// The global cap is shared, so without a sub-cap a single reachable address
/// can hold every slot and deny service to every other peer before either
/// connection has authenticated. Half of [`MAX_CONNECTIONS`] is the largest
/// sub-cap that still leaves an equal share for a second address, and it holds
/// even for a two-voter group where there is only one configured peer.
pub const MAX_CONNECTIONS_PER_PEER: usize = MAX_CONNECTIONS / 2;

/// Aggregate encoded request bytes reserved across active server handlers.
/// JSON decoding and response construction require additional memory.
///
/// Two maximum-size frames must fit at once. With a budget of exactly one
/// [`MAX_FRAME`], a single unauthenticated peer could declare the maximum
/// frame and hold every byte permit while it trickles or stalls the body,
/// blocking every other handler until `request_timeout`. Two frames is the
/// smallest multiple that lets one peer's maximum frame coexist with another
/// peer's request; a third maximum frame waits. The reserved-byte ceiling
/// therefore rises from 32 MiB to 64 MiB (64 MiB of live frame buffers, plus
/// the decoded JSON the comment below already excludes), rather than being
/// lowered to a frame size that a valid payload might exceed.
pub const MAX_IN_FLIGHT_REQUEST_BYTES: usize = 2 * MAX_FRAME;

/// Largest share of [`MAX_IN_FLIGHT_REQUEST_BYTES`] one source address may
/// hold at once.
///
/// The global budget alone still lets one reachable address take every byte:
/// with a budget of two frames, two stalled maximum frames from one address
/// reserve all 64 MiB. Half the budget is the same "never everything" rule as
/// [`MAX_CONNECTIONS_PER_PEER`]: one address can hold at most one maximum
/// frame, so at least one maximum frame always remains for another address.
/// The share equals [`MAX_FRAME`], so the ordinary case — one maximum-size
/// frame from one peer — is admitted without waiting; only that peer's second
/// simultaneous frame waits. Exceeding the share is *not* refused: the frame
/// is valid and simply waits for its own address's share, unlike a frame above
/// [`MAX_FRAME`], which is refused before any reservation.
pub const MAX_IN_FLIGHT_REQUEST_BYTES_PER_PEER: usize = MAX_IN_FLIGHT_REQUEST_BYTES / 2;

// Every admissible frame must fit one address's share, or a valid frame would
// wait forever instead of being admitted or refused.
const _: () = assert!(
    MAX_IN_FLIGHT_REQUEST_BYTES_PER_PEER >= MAX_FRAME,
    "one address's byte share must admit a maximum-size frame"
);

const REQUEST: u8 = 1;
const RESPONSE: u8 = 2;
const CHALLENGE: u8 = 3;
const SNAPSHOT_MANIFEST: u8 = 4;
const SNAPSHOT_CHUNK: u8 = 5;
const SNAPSHOT_CONTEXT: &str = "yadb-openraft-snapshot-stream/1";
pub const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024 + 24;
const SNAPSHOT_CHUNK_BYTES: usize = 256 * 1024;
const SNAPSHOT_MANIFEST_PAYLOAD: usize = 64 * 1024;
// JSON encodes bytes twice (payload and envelope), at most 16 bytes per byte.
const SNAPSHOT_MANIFEST_FRAME: usize = SNAPSHOT_MANIFEST_PAYLOAD * 4 + 4096;
const SNAPSHOT_CHUNK_FRAME: usize = SNAPSHOT_CHUNK_BYTES * 16 + 16384;

// Bound repeated diagnostic volume while retaining evidence of persistent faults.
static SNAPSHOT_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Process-wide transport counters, reported through `/openraft/status`.
///
/// Every Raft RPC opens a fresh TCP connection and performs a fresh mTLS
/// handshake, so the events worth counting are connection-level: a peer that
/// closed mid-handshake, an envelope that failed verification, and a listener
/// that dropped a connection because every handler slot was busy. None of them
/// produce a frame the peer can attribute, and OpenRaft turns all of them into a
/// progress error, so without a counter they look exactly like a healthy peer
/// with nothing to say. That silence is what made one investigation read 16 019
/// client-side handshake failures with no server-side counterpart.
struct TransportCounters {
    /// Accepted connections dropped because every handler slot was busy.
    cap_dropped: AtomicU64,
    /// Accepted connections dropped because their source address already held
    /// its share of the handler slots.
    peer_cap_dropped: AtomicU64,
    /// Frame reservations that had to wait for their source address's share of
    /// the in-flight byte budget.
    peer_bytes_waited: AtomicU64,
    /// Dispatched connections that ended in an I/O, TLS or frame-read error.
    failed: AtomicU64,
    /// Envelopes rejected by decoding, signature or challenge verification.
    rejected: AtomicU64,
    /// Outgoing RPCs that ended without a verified reply, including the ones
    /// cancelled by a caller's own deadline.
    rpc_failed: AtomicU64,
}

static TRANSPORT: TransportCounters = TransportCounters {
    cap_dropped: AtomicU64::new(0),
    peer_cap_dropped: AtomicU64::new(0),
    peer_bytes_waited: AtomicU64::new(0),
    failed: AtomicU64::new(0),
    rejected: AtomicU64::new(0),
    rpc_failed: AtomicU64::new(0),
};

/// A snapshot of the process-wide transport counters.
pub fn transport_counters() -> serde_json::Value {
    serde_json::json!({
        "connections_cap_dropped": TRANSPORT.cap_dropped.load(Ordering::Relaxed),
        "connections_peer_cap_dropped": TRANSPORT.peer_cap_dropped.load(Ordering::Relaxed),
        "request_bytes_peer_waited": TRANSPORT.peer_bytes_waited.load(Ordering::Relaxed),
        "connections_failed": TRANSPORT.failed.load(Ordering::Relaxed),
        "envelopes_rejected": TRANSPORT.rejected.load(Ordering::Relaxed),
        "outgoing_rpc_failures": TRANSPORT.rpc_failed.load(Ordering::Relaxed),
        "snapshot_failures": SNAPSHOT_FAILURES.load(Ordering::Relaxed),
    })
}

/// Counts an outgoing RPC that ends without a verified reply.
///
/// The increment cannot live after the `await`. OpenRaft wraps
/// `append_entries` in its own `C::timeout(heartbeat_interval, ...)`, and when
/// that outer deadline fires it drops this future at the await point, so no
/// statement after the `await` ever runs. Counting only the errors that return
/// therefore missed the append timeouts this instrumentation exists to expose —
/// exactly the events that were invisible before. A drop guard observes both
/// endings: an error return, and cancellation while still in flight.
struct RpcOutcome {
    armed: bool,
}

impl RpcOutcome {
    fn new() -> Self {
        Self { armed: true }
    }

    /// The caller got a reply it verified, so this is not a failure.
    fn verified(&mut self) {
        self.armed = false;
    }
}

impl Drop for RpcOutcome {
    fn drop(&mut self) {
        if self.armed {
            TRANSPORT.rpc_failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Count one connection dropped by the global handler cap, logging the first
/// few, then powers of two.
///
/// The global cap itself is unchanged. The two backpressure variants recorded
/// in `docs/multiraft-verification.md` moved *when* the global slot was
/// acquired and neither was shown to help; the per-address sub-cap below is a
/// different mechanism (it partitions the same slots) rather than a third
/// variant of that timing change.
fn cap_dropped(node: u32, group: u64, peer: Option<u32>) {
    let n = TRANSPORT.cap_dropped.fetch_add(1, Ordering::Relaxed) + 1;
    if n <= 8 || n.is_power_of_two() {
        eprintln!(
            "openraft transport node={node} group={group} peer={peer:?} connection dropped: \
             all {MAX_CONNECTIONS} handler slots busy occurrence={n}"
        );
    }
}

/// Slots per source address for a group with `voters` voters.
///
/// Each of the `voters - 1` other voters gets an equal share of the global
/// cap, rounded up, so a larger group does not let one peer hold a share it
/// could not legitimately need. The share is capped at
/// [`MAX_CONNECTIONS_PER_PEER`] and floored at one, so the divisor is never
/// zero and a group larger than the cap still admits one connection per
/// address.
///
/// Trade-off: an address that legitimately needs more than its share is
/// throttled. That includes several replicas behind one NAT, and test
/// clusters that put every node on loopback. Every RPC opens a fresh
/// connection, so the pressure this bounds is real; per-peer connection reuse
/// would remove the pressure rather than divide it.
fn per_peer_connection_cap(voters: usize) -> usize {
    let peers = voters.saturating_sub(1).max(1);
    MAX_CONNECTIONS
        .div_ceil(peers)
        .clamp(1, MAX_CONNECTIONS_PER_PEER)
}

/// Count one connection dropped by the per-address sub-cap.
///
/// Deliberately distinct from [`cap_dropped`]: the global counter means the
/// node is saturated, while this one means a single address is holding its
/// share and another address may still be admitted. Log volume is bounded the
/// same way as the other counters (first eight, then powers of two).
fn peer_cap_dropped(node: u32, group: u64, peer: Option<u32>, address: IpAddr, cap: usize) {
    let n = TRANSPORT.peer_cap_dropped.fetch_add(1, Ordering::Relaxed) + 1;
    if n <= 8 || n.is_power_of_two() {
        eprintln!(
            "openraft transport node={node} group={group} peer={peer:?} address={address} \
             connection dropped: source address holds all {cap} of its handler slots \
             occurrence={n}"
        );
    }
}

/// Per-source-address handler-slot accounting.
///
/// The map is bounded by the number of addresses that currently hold a slot,
/// which the global semaphore bounds at [`MAX_CONNECTIONS`]; an entry is
/// removed when its count reaches zero, so it cannot grow without bound from
/// addresses that connect once and leave. It is keyed by IP rather than
/// `SocketAddr` because every RPC opens a new TCP connection from a fresh
/// ephemeral port, and a port-keyed cap would never bind. IPv4-mapped IPv6
/// addresses are canonicalised so one host cannot count itself twice by
/// connecting through both families.
struct PeerSlots {
    counts: Mutex<BTreeMap<IpAddr, usize>>,
}

impl PeerSlots {
    fn new() -> Self {
        Self {
            counts: Mutex::new(BTreeMap::new()),
        }
    }

    /// Reserves one handler slot for `address`, or returns `None` when that
    /// address already holds `cap` of them.
    fn try_acquire(self: &Arc<Self>, address: IpAddr, cap: usize) -> Option<PeerSlot> {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let held = counts.entry(address).or_insert(0);
        if *held >= cap {
            return None;
        }
        *held += 1;
        Some(PeerSlot {
            slots: self.clone(),
            address,
        })
    }
}

/// One reserved per-address handler slot, released when the handler ends.
struct PeerSlot {
    slots: Arc<PeerSlots>,
    address: IpAddr,
}

impl Drop for PeerSlot {
    fn drop(&mut self) {
        let mut counts = self
            .slots
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(held) = counts.get_mut(&self.address) {
            *held -= 1;
            if *held == 0 {
                counts.remove(&self.address);
            }
        }
    }
}

/// Count one frame reservation that waited for its address's byte share.
///
/// This is backpressure, not a fault: the frame is valid and is admitted once
/// its own address releases bytes. It is counted apart from the global budget
/// so an operator can tell "this address is over its share" from "the node's
/// whole in-flight budget is in use". Log volume is bounded like the other
/// counters (first eight, then powers of two).
fn peer_bytes_waited(address: IpAddr) {
    let n = TRANSPORT.peer_bytes_waited.fetch_add(1, Ordering::Relaxed) + 1;
    if n <= 8 || n.is_power_of_two() {
        eprintln!(
            "openraft transport address={address} frame reservation waited for its \
             per-address byte share occurrence={n}"
        );
    }
}

/// Per-source-address in-flight frame-byte accounting.
///
/// Mirrors [`PeerSlots`]: keyed by canonical IP for the same reason (every RPC
/// opens a fresh ephemeral port), an entry is created on demand and removed
/// when its reservation returns to zero, so the map is bounded by the number
/// of addresses currently holding bytes rather than by addresses ever seen.
///
/// A reservation that does not fit is *waited for*, never refused: the frame
/// has already passed the [`MAX_FRAME`] and global-budget checks, so it is
/// valid and only needs its address's share to drain. `Notify` wakes every
/// waiter on release because the freed bytes may not fit the first waiter.
struct PeerBytes {
    cap: usize,
    reserved: Mutex<BTreeMap<IpAddr, usize>>,
    released: Notify,
}

impl PeerBytes {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            reserved: Mutex::new(BTreeMap::new()),
            released: Notify::new(),
        }
    }

    fn try_reserve(&self, address: IpAddr, bytes: usize) -> bool {
        let mut reserved = self
            .reserved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let used = reserved.entry(address).or_insert(0);
        if *used + bytes > self.cap {
            return false;
        }
        *used += bytes;
        true
    }

    /// Reserves `bytes` for `address`, waiting until its share has room.
    ///
    /// The compile-time assertion on [`MAX_IN_FLIGHT_REQUEST_BYTES_PER_PEER`]
    /// proves every admissible frame fits an empty share, so this always
    /// terminates once the address's own reservations drain.
    async fn reserve(self: &Arc<Self>, address: IpAddr, bytes: usize) -> PeerBytesGuard {
        let mut waited = false;
        loop {
            // Register interest before checking, so a release between the
            // check and the await cannot be lost.
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if self.try_reserve(address, bytes) {
                if waited {
                    peer_bytes_waited(address);
                }
                return PeerBytesGuard {
                    budget: self.clone(),
                    address,
                    bytes,
                };
            }
            waited = true;
            released.await;
        }
    }
}

/// Bytes reserved against one source address's share, released on drop.
struct PeerBytesGuard {
    budget: Arc<PeerBytes>,
    address: IpAddr,
    bytes: usize,
}

impl Drop for PeerBytesGuard {
    fn drop(&mut self) {
        {
            let mut reserved = self
                .budget
                .reserved
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(used) = reserved.get_mut(&self.address) {
                *used -= self.bytes;
                if *used == 0 {
                    reserved.remove(&self.address);
                }
            }
        }
        self.budget.released.notify_waiters();
    }
}

/// One source address's view of the frame-byte budget.
#[derive(Clone)]
struct PeerFrameBudget {
    shares: Arc<PeerBytes>,
    address: IpAddr,
}

impl PeerFrameBudget {
    async fn reserve(&self, bytes: usize) -> PeerBytesGuard {
        self.shares.reserve(self.address, bytes).await
    }
}

/// The bytes one frame reserved: its global budget permit and its address's
/// share, released together once the body is no longer needed.
struct FrameBytes {
    _global: tokio::sync::OwnedSemaphorePermit,
    _peer: Option<PeerBytesGuard>,
}

impl std::fmt::Debug for FrameBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameBytes")
            .field("peer_bytes", &self._peer.as_ref().map(|peer| peer.bytes))
            .finish_non_exhaustive()
    }
}

/// Count one rejected envelope and log the first few, then powers of two.
///
/// A rejected envelope is a protocol violation, not a slow peer: the frame
/// decoded, but its signature, challenge or body did not match. Volume is
/// bounded the same way [`snapshot_failure`] bounds it, so a peer that keeps
/// talking wrong leaves evidence without flooding the log.
fn envelope_rejected(node: u32, group: u64, reason: &'static str) {
    let n = TRANSPORT.rejected.fetch_add(1, Ordering::Relaxed) + 1;
    if n <= 8 || n.is_power_of_two() {
        eprintln!(
            "openraft transport node={node} group={group} rejected envelope: {reason} occurrence={n}"
        );
    }
}

fn snapshot_failure(node: u32, group: u64, peer: Option<u32>, phase: &str, reason: &str) {
    let n = SNAPSHOT_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
    if n <= 8 || n.is_power_of_two() {
        eprintln!("openraft snapshot transport node={node} group={group} peer={peer:?} phase={phase} reason={reason} occurrence={n}");
    }
}

/// Count one connection whose request frame could not be read.
///
/// This is the frame read *before* any envelope is decoded, so the peer never
/// said who it was: a truncated length prefix, a frame that overran the
/// reservation, or a socket closed after the challenge. The handler used to
/// return `Ok(())` here, which is right for control flow — there is nothing to
/// answer — but left the event completely uncounted, so it looked identical to
/// a peer that connected and said nothing. It is counted as a failed connection
/// because that is what it is: a dispatched connection that ended in an I/O
/// error before dispatch.
fn request_frame_unreadable(node: u32, group: u64, peer: Option<u32>, error: &io::Error) {
    let n = TRANSPORT.failed.fetch_add(1, Ordering::Relaxed) + 1;
    if n <= 8 || n.is_power_of_two() {
        eprintln!(
            "openraft transport node={node} group={group} peer={peer:?} request frame unreadable: \
             {error} occurrence={n}"
        );
    }
}

struct SnapshotPanicDiagnostic {
    node: u32,
    group: u64,
    peer: Arc<AtomicU64>,
    phase: Arc<AtomicU64>,
}

impl Drop for SnapshotPanicDiagnostic {
    fn drop(&mut self) {
        if std::thread::panicking() && self.phase.load(Ordering::Relaxed) != 0 {
            let peer = self.peer.load(Ordering::Relaxed);
            let phase = match self.phase.load(Ordering::Relaxed) {
                1 => "manifest",
                2 => "chunk",
                3 => "handler",
                _ => "reply",
            };
            snapshot_failure(
                self.node,
                self.group,
                (peer != 0).then_some(peer as u32),
                phase,
                "handler task panicked",
            );
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum WireRequest {
    AppendEntries(Box<AppendEntriesRequest<C>>),
    Vote(Box<VoteRequest<u32>>),
    InstallSnapshot {
        vote: Vote<u32>,
        meta: SnapshotMeta<u32, BasicNode>,
        data: Vec<u8>,
    },
}

impl WireRequest {
    fn sender_matches(&self, sender: u32) -> bool {
        let vote = match self {
            Self::AppendEntries(rpc) => &rpc.vote,
            Self::Vote(rpc) => &rpc.vote,
            Self::InstallSnapshot { vote, .. } => vote,
        };
        vote.leader_id.voted_for() == Some(sender)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum WireResponse {
    AppendEntries(Box<AppendEntriesResponse<u32>>),
    Vote(Box<VoteResponse<u32>>),
    InstallSnapshot(Box<SnapshotResponse<u32>>),
}

#[derive(Serialize, Deserialize)]
struct WireCall {
    nonce: [u8; 32],
    challenge: [u8; 32],
    request: WireRequest,
}

#[derive(Serialize, Deserialize)]
struct WireReply {
    nonce: [u8; 32],
    response: WireResponse,
}

#[derive(Serialize, Deserialize)]
struct SnapshotManifest {
    context: String,
    nonce: [u8; 32],
    challenge: [u8; 32],
    vote: Vote<u32>,
    meta: SnapshotMeta<u32, BasicNode>,
    length: usize,
    digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct SnapshotChunk {
    context: String,
    nonce: [u8; 32],
    challenge: [u8; 32],
    ordinal: usize,
    length: usize,
    digest: [u8; 32],
    data: Vec<u8>,
}

/// Minimal boxed future alias to keep the handler object safe.
pub type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// Shared configuration for one node's transport.
#[derive(Clone)]
pub struct TransportConfig {
    pub identity: ReplicaIdentity,
    pub signing_key: Arc<NodeSigningKey>,
    /// Peer listen addresses used for routing outgoing RPCs.
    pub peers: Arc<BTreeMap<u32, SocketAddr>>,
    /// Trusted peer public keys used for identity verification.
    pub registry: Arc<PeerKeyRegistry>,
    /// Rejects connections whose source address is not a known peer address.
    /// A TCP client normally uses an ephemeral source port, so this is off by
    /// default: the peer identity is then established by the signature alone.
    pub require_known_address: bool,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    /// Optional floor for this transport's own RPC deadline.
    ///
    /// This raises the deadline *this* transport enforces. It does not raise
    /// OpenRaft's: 0.9.25 wraps `append_entries` in
    /// `C::timeout(heartbeat_interval, ...)` and turns a miss into
    /// `RPCError::Timeout`, which is not retried. An append is therefore bounded
    /// by the configured heartbeat interval, and a heartbeat shorter than one
    /// handshake abandons appends the follower already applied. Use this floor
    /// for the RPC types OpenRaft does not wrap, and size `heartbeat_ms` above
    /// the transport's worst-case round trip for appends. `None` keeps
    /// OpenRaft's own deadline, which is what the in-process test transports
    /// rely on.
    pub rpc_ttl_floor: Option<Duration>,
    pub tls: Option<Arc<MutualTls>>,
}

impl TransportConfig {
    pub fn new(
        identity: ReplicaIdentity,
        signing_key: Arc<NodeSigningKey>,
        peers: BTreeMap<u32, SocketAddr>,
        registry: Arc<PeerKeyRegistry>,
    ) -> Self {
        Self {
            identity,
            signing_key,
            peers: Arc::new(peers),
            registry,
            require_known_address: false,
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            rpc_ttl_floor: None,
            tls: None,
        }
    }

    pub fn with_required_address(mut self, required: bool) -> Self {
        self.require_known_address = required;
        self
    }

    pub fn with_tls(mut self, tls: Arc<MutualTls>) -> Self {
        self.tls = Some(tls);
        self
    }
}

trait IoStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> IoStream for T {}
type PeerStream = Box<dyn IoStream>;

/// Creates a network client for one target node.
#[derive(Clone)]
pub struct AuthenticatedTcpNetworkFactory {
    config: TransportConfig,
    sequence: Arc<AtomicU64>,
}

impl AuthenticatedTcpNetworkFactory {
    pub fn new(config: TransportConfig) -> Self {
        Self {
            config,
            sequence: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl RaftNetworkFactory<C> for AuthenticatedTcpNetworkFactory {
    type Network = AuthenticatedTcpNetwork;

    async fn new_client(&mut self, target: u32, _node: &BasicNode) -> Self::Network {
        AuthenticatedTcpNetwork {
            config: self.config.clone(),
            target,
            sequence: self.sequence.clone(),
        }
    }
}

/// One authenticated client connection to a peer.
pub struct AuthenticatedTcpNetwork {
    config: TransportConfig,
    target: u32,
    sequence: Arc<AtomicU64>,
}

impl AuthenticatedTcpNetwork {
    /// Test helper that sends one request without going through OpenRaft.
    #[cfg(test)]
    pub async fn call_for_test(
        &mut self,
        request: WireRequest,
        ttl: Duration,
    ) -> Result<WireResponse, TransportError> {
        self.call(request, ttl).await
    }

    async fn call(
        &mut self,
        request: WireRequest,
        ttl: Duration,
    ) -> Result<WireResponse, TransportError> {
        let is_snapshot = matches!(&request, WireRequest::InstallSnapshot { .. });
        let address = *self
            .config
            .peers
            .get(&self.target)
            .ok_or(TransportError::InvalidInput("unknown peer address"))?;
        let mut nonce = [0u8; 32];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| TransportError::KeyGeneration)?;
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        let exchange = async {
            let stream =
                tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(address))
                    .await
                    .map_err(|_| TransportError::InvalidInput("connect timeout"))?
                    .map_err(|_| TransportError::InvalidInput("connect failed"))?;
            stream.set_nodelay(true).ok();
            let mut stream: PeerStream = if let Some(tls) = &self.config.tls {
                let name = tls.name(self.target).map_err(transport_io)?;
                let stream = tls
                    .client
                    .connect(name, stream)
                    .await
                    .map_err(transport_io)?;
                let session = stream.get_ref().1;
                let peer = tls
                    .peer(session.peer_certificates(), session.alpn_protocol())
                    .map_err(transport_io)?;
                if peer != self.target {
                    return Err(TransportError::AuthenticationFailed);
                }
                Box::new(stream)
            } else {
                Box::new(stream)
            };
            let (kind, challenge) = read_frame_limited(&mut stream, 32)
                .await
                .map_err(transport_io)?;
            if kind != CHALLENGE || challenge.len() != 32 {
                return Err(TransportError::AuthenticationFailed);
            }
            let challenge: [u8; 32] = challenge
                .try_into()
                .map_err(|_| TransportError::AuthenticationFailed)?;
            match request {
                WireRequest::InstallSnapshot { vote, meta, data } => {
                    if self.config.tls.is_none() {
                        return Err(TransportError::InvalidInput(
                            "snapshot streaming requires mutual TLS",
                        ));
                    }
                    if data.len() > MAX_SNAPSHOT_BYTES {
                        return Err(TransportError::PayloadTooLarge);
                    }
                    let digest: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, &data)
                        .as_ref()
                        .try_into()
                        .unwrap();
                    let manifest = SnapshotManifest {
                        context: SNAPSHOT_CONTEXT.into(),
                        nonce,
                        challenge,
                        vote,
                        meta,
                        length: data.len(),
                        digest,
                    };
                    let payload =
                        serde_json::to_vec(&manifest).map_err(|_| TransportError::Encoding)?;
                    if payload.len() > SNAPSHOT_MANIFEST_PAYLOAD {
                        return Err(TransportError::PayloadTooLarge);
                    }
                    let frame = SignedRaftEnvelope::sign(
                        &self.config.identity,
                        self.target,
                        sequence,
                        payload,
                        &self.config.signing_key,
                    )?
                    .encode_json()?;
                    write_frame(&mut stream, SNAPSHOT_MANIFEST, &frame)
                        .await
                        .map_err(transport_io)?;
                    for (ordinal, bytes) in data.chunks(SNAPSHOT_CHUNK_BYTES).enumerate() {
                        let chunk = SnapshotChunk {
                            context: SNAPSHOT_CONTEXT.into(),
                            nonce,
                            challenge,
                            ordinal,
                            length: data.len(),
                            digest,
                            data: bytes.to_vec(),
                        };
                        let payload =
                            serde_json::to_vec(&chunk).map_err(|_| TransportError::Encoding)?;
                        let frame = SignedRaftEnvelope::sign(
                            &self.config.identity,
                            self.target,
                            sequence,
                            payload,
                            &self.config.signing_key,
                        )?
                        .encode_json()?;
                        if frame.len() > SNAPSHOT_CHUNK_FRAME {
                            return Err(TransportError::PayloadTooLarge);
                        }
                        write_frame(&mut stream, SNAPSHOT_CHUNK, &frame)
                            .await
                            .map_err(transport_io)?;
                    }
                }
                request => {
                    let payload = serde_json::to_vec(&WireCall {
                        nonce,
                        challenge,
                        request,
                    })
                    .map_err(|_| TransportError::Encoding)?;
                    let frame = SignedRaftEnvelope::sign(
                        &self.config.identity,
                        self.target,
                        sequence,
                        payload,
                        &self.config.signing_key,
                    )?
                    .encode_json()?;
                    write_frame(&mut stream, REQUEST, &frame)
                        .await
                        .map_err(transport_io)?;
                }
            }
            let (kind, body) = read_frame(&mut stream).await.map_err(transport_io)?;
            if kind != RESPONSE {
                return Err(TransportError::AuthenticationFailed);
            }
            verify_reply(&body, &self.config, self.target, sequence, nonce)
        };

        // Armed before the exchange and disarmed only on a verified reply. If
        // the caller cancels us (OpenRaft's own heartbeat deadline does), the
        // guard is dropped while armed and the failure is still counted.
        let mut outcome = RpcOutcome::new();
        let result = match tokio::time::timeout(ttl, exchange).await {
            Ok(result) => result,
            Err(_) => Err(TransportError::InvalidInput("request timeout")),
        };
        if result.is_ok() {
            outcome.verified();
        }
        if is_snapshot {
            if let Err(error) = &result {
                snapshot_failure(
                    self.config.identity.node(),
                    self.config.identity.group().group,
                    Some(self.target),
                    "client",
                    &error.to_string(),
                );
            }
        }
        result
    }
}

fn verify_reply(
    body: &[u8],
    config: &TransportConfig,
    target: u32,
    sequence: u64,
    nonce: [u8; 32],
) -> Result<WireResponse, TransportError> {
    let response = SignedRaftEnvelope::decode_json(body)?;
    let payload = response.verify(&config.identity, Some(target), &config.registry)?;
    if response.sequence != sequence {
        return Err(TransportError::AuthenticationFailed);
    }
    let reply: WireReply = serde_json::from_slice(payload).map_err(|_| TransportError::Encoding)?;
    if reply.nonce != nonce {
        return Err(TransportError::AuthenticationFailed);
    }
    Ok(reply.response)
}

impl RaftNetwork<C> for AuthenticatedTcpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, openraft::error::RaftError<u32>>>
    {
        let ttl = rpc_deadline(self.config.rpc_ttl_floor, option.hard_ttl());
        let response = self
            .call(WireRequest::AppendEntries(Box::new(rpc)), ttl)
            .await
            .map_err(|error| rpc_error(self.target, error, ttl))?;
        match response {
            WireResponse::AppendEntries(response) => Ok(*response),
            _ => Err(rpc_error(
                self.target,
                TransportError::AuthenticationFailed,
                ttl,
            )),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u32>,
        option: RPCOption,
    ) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, openraft::error::RaftError<u32>>> {
        let ttl = rpc_deadline(self.config.rpc_ttl_floor, option.hard_ttl());
        let response = self
            .call(WireRequest::Vote(Box::new(rpc)), ttl)
            .await
            .map_err(|error| rpc_error(self.target, error, ttl))?;
        match response {
            WireResponse::Vote(response) => Ok(*response),
            _ => Err(rpc_error(
                self.target,
                TransportError::AuthenticationFailed,
                ttl,
            )),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u32>,
        snapshot: openraft::Snapshot<C>,
        cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
        let data = snapshot.snapshot.into_inner();
        let request = WireRequest::InstallSnapshot {
            vote,
            meta: snapshot.meta,
            data,
        };
        let ttl = rpc_deadline(self.config.rpc_ttl_floor, option.hard_ttl());
        let result = tokio::select! {
            result = self.call(request, ttl) => result,
            _ = cancel => return Err(StreamingError::Closed(ReplicationClosed::new("snapshot cancelled"))),
        };
        match result {
            Ok(WireResponse::InstallSnapshot(response)) => Ok(*response),
            Ok(_) => Err(StreamingError::Unreachable(Unreachable::new(
                &io::Error::other("unexpected Raft response from peer"),
            ))),
            Err(error) => Err(StreamingError::Unreachable(Unreachable::new(
                &io::Error::other(error.to_string()),
            ))),
        }
    }
}

/// Maps an I/O failure into a transport error, naming the failure class.
///
/// The previous version collapsed every non-timeout failure into the string
/// `"transport failure"`, so a handshake EOF, a reset mid-frame and a TLS
/// verification error were indistinguishable in both the returned error and the
/// OpenRaft log line that reports it. That cost an investigation: 10 584
/// identical `RPCError::Network(InvalidInput("transport failure"))` lines said
/// nothing about which of the three had happened. The classification is
/// unchanged — a non-timeout I/O error is still not retried — only the text now
/// names the class, using the same mapping the snapshot path already applies.
fn transport_io(error: io::Error) -> TransportError {
    TransportError::InvalidInput(match error.kind() {
        io::ErrorKind::TimedOut => "request timeout",
        io::ErrorKind::UnexpectedEof => "transport failure: peer closed the connection",
        io::ErrorKind::ConnectionReset => "transport failure: connection reset",
        io::ErrorKind::ConnectionRefused => "transport failure: connection refused",
        io::ErrorKind::BrokenPipe => "transport failure: broken pipe",
        io::ErrorKind::InvalidData => "transport failure: invalid frame or TLS record",
        _ => "transport failure",
    })
}

/// The deadline for one RPC: OpenRaft's own hard TTL, raised to the configured
/// floor when there is one. See [`TransportConfig::rpc_ttl_floor`].
fn rpc_deadline(floor: Option<Duration>, hard_ttl: Duration) -> Duration {
    match floor {
        Some(floor) => hard_ttl.max(floor),
        None => hard_ttl,
    }
}

fn rpc_error(
    target: u32,
    error: TransportError,
    timeout: Duration,
) -> RPCError<u32, BasicNode, openraft::error::RaftError<u32>> {
    match error {
        TransportError::InvalidInput("request timeout") => RPCError::Timeout(Timeout {
            action: openraft::network::RPCTypes::AppendEntries,
            id: 0,
            target,
            timeout,
        }),
        other => RPCError::Network(NetworkError::new(&io::Error::other(other.to_string()))),
    }
}

/// Serves authenticated Raft RPCs for one node.
pub struct RaftPeerServer {
    listener: TcpListener,
    config: TransportConfig,
    handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync>,
}

impl RaftPeerServer {
    pub async fn bind(
        address: SocketAddr,
        config: TransportConfig,
        handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        Ok(Self {
            listener,
            config,
            handler,
        })
    }

    /// Uses an already bound listener. The peer addresses must be known
    /// before the listener is created, so the address map cannot be derived
    /// from the bound port afterwards.
    pub fn from_listener(
        listener: TcpListener,
        config: TransportConfig,
        handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync>,
    ) -> Self {
        Self {
            listener,
            config,
            handler,
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections until dropped or the listener fails. Dropping aborts
    /// handlers, but does not synchronously wait for their cancellation.
    pub async fn serve(self) {
        let _ = self.serve_until(std::future::pending()).await;
    }

    /// Stop accepting on shutdown or listener failure, then abort and drain all
    /// handlers before returning. Dropping this future only requests abort and
    /// does not wait for handlers to finish cancelling.
    pub async fn serve_until(
        self,
        shutdown: impl std::future::Future<Output = ()> + Send,
    ) -> io::Result<()> {
        let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let request_bytes = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let snapshot_bytes = Arc::new(Semaphore::new(MAX_SNAPSHOT_BYTES));
        let peer_slots = Arc::new(PeerSlots::new());
        let peer_cap = per_peer_connection_cap(self.config.identity.voters().len());
        let peer_bytes = Arc::new(PeerBytes::new(MAX_IN_FLIGHT_REQUEST_BYTES_PER_PEER));
        let mut tasks = JoinSet::new();
        tokio::pin!(shutdown);
        let result = loop {
            let accepted = tokio::select! {
                biased;
                _ = &mut shutdown => break Ok(()),
                result = self.listener.accept() => result,
                Some(_) = tasks.join_next(), if !tasks.is_empty() => continue,
            };
            let (stream, peer) = match accepted {
                Ok(value) => value,
                Err(error) => break Err(error),
            };
            let node_id = self
                .config
                .peers
                .iter()
                .find(|(_, addr)| **addr == peer)
                .map(|(id, _)| *id);
            if self.config.require_known_address && node_id.is_none() {
                // Strict mode: only configured peer addresses may connect.
                drop(stream);
                continue;
            }
            let Ok(permit) = connections.clone().try_acquire_owned() else {
                // Every handler slot is busy, so the accepted socket is
                // dropped without being served. That is the one cap-related
                // event a peer cannot distinguish from a dead peer: it sees a
                // connection closed mid-handshake, which `rpc_error` turns into
                // `RPCError::Network`, and OpenRaft treats that as a progress
                // error rather than a retry. Counting it is what lets an
                // operator tell "cap reached" from "peer down".
                cap_dropped(
                    self.config.identity.node(),
                    self.config.identity.group().group,
                    node_id,
                );
                continue;
            };
            // Reserve this address's share after the global slot, so a refused
            // share cannot hold a slot another address could use. The guard is
            // moved into the handler and released when the handler ends, which
            // also covers abort and panic.
            let address = peer.ip().to_canonical();
            let Some(peer_slot) = peer_slots.try_acquire(address, peer_cap) else {
                drop(permit);
                peer_cap_dropped(
                    self.config.identity.node(),
                    self.config.identity.group().group,
                    node_id,
                    address,
                    peer_cap,
                );
                continue;
            };
            let config = self.config.clone();
            let handler = self.handler.clone();
            let request_bytes = request_bytes.clone();
            let snapshot_bytes = snapshot_bytes.clone();
            let peer_frame_budget = PeerFrameBudget {
                shares: peer_bytes.clone(),
                address,
            };
            tasks.spawn(async move {
                let _permit = permit;
                let _peer_slot = peer_slot;
                let timeout = config.request_timeout;
                let phase = Arc::new(AtomicU64::new(0));
                let snapshot_peer = Arc::new(AtomicU64::new(0));
                let node = config.identity.node();
                let group = config.identity.group().group;
                let _panic_diagnostic = SnapshotPanicDiagnostic {
                    node,
                    group,
                    peer: snapshot_peer.clone(),
                    phase: phase.clone(),
                };
                let result = tokio::time::timeout(
                    timeout,
                    serve_peer(
                        stream,
                        node_id,
                        config,
                        handler,
                        request_bytes,
                        snapshot_bytes,
                        Some(peer_frame_budget),
                        phase.clone(),
                        snapshot_peer.clone(),
                    ),
                )
                .await;
                let stage = phase.load(Ordering::Relaxed);
                if stage > 0 {
                    let peer = match snapshot_peer.load(Ordering::Relaxed) {
                        0 => node_id,
                        id => Some(id as u32),
                    };
                    let label = match stage {
                        1 => "manifest",
                        2 => "chunk",
                        3 => "handler",
                        _ => "reply",
                    };
                    match result {
                        Err(_) => {
                            snapshot_failure(node, group, peer, label, "server deadline exceeded")
                        }
                        Ok(Err(error)) => snapshot_failure(
                            node,
                            group,
                            peer,
                            label,
                            match error.kind() {
                                io::ErrorKind::UnexpectedEof
                                | io::ErrorKind::ConnectionReset
                                | io::ErrorKind::BrokenPipe => "connection closed",
                                io::ErrorKind::InvalidData => "invalid frame",
                                _ => "transport or validation error",
                            },
                        ),
                        Ok(Ok(())) => {}
                    }
                } else {
                    // An ordinary Raft connection. Count and report it here:
                    // `serve_connection` cannot answer a peer whose request it
                    // refused, so a handshake EOF, a deadline and a peer that
                    // simply had nothing to say are otherwise indistinguishable.
                    match result {
                        Err(_) => {
                            TRANSPORT.failed.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(Err(_)) => {
                            let n = TRANSPORT.failed.fetch_add(1, Ordering::Relaxed) + 1;
                            if n <= 8 || n.is_power_of_two() {
                                eprintln!(
                                    "openraft transport node={node} group={group} peer={node_id:?} connection closed before dispatch occurrence={n}"
                                );
                            }
                        }
                        Ok(Ok(())) => {}
                    }
                }
            });
        };
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    }
}

async fn serve_peer(
    stream: TcpStream,
    node_id: Option<u32>,
    config: TransportConfig,
    handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync>,
    request_bytes: Arc<Semaphore>,
    snapshot_bytes: Arc<Semaphore>,
    peer_frame_budget: Option<PeerFrameBudget>,
    phase: Arc<AtomicU64>,
    snapshot_peer: Arc<AtomicU64>,
) -> io::Result<()> {
    stream.set_nodelay(true).ok();
    let (stream, node_id): (PeerStream, Option<u32>) = if let Some(tls) = &config.tls {
        let stream = tls.server.accept(stream).await?;
        let session = stream.get_ref().1;
        let peer = tls.peer(session.peer_certificates(), session.alpn_protocol())?;
        if node_id.is_some_and(|node| node != peer) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "TLS node/address mismatch",
            ));
        }
        (Box::new(stream), Some(peer))
    } else {
        (Box::new(stream), node_id)
    };
    serve_connection(
        stream,
        node_id,
        config,
        handler,
        request_bytes,
        snapshot_bytes,
        peer_frame_budget,
        phase,
        snapshot_peer,
    )
    .await
}

async fn serve_connection(
    mut stream: PeerStream,
    node_id: Option<u32>,
    config: TransportConfig,
    handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync>,
    request_bytes: Arc<Semaphore>,
    snapshot_bytes: Arc<Semaphore>,
    peer_frame_budget: Option<PeerFrameBudget>,
    phase: Arc<AtomicU64>,
    snapshot_peer: Arc<AtomicU64>,
) -> io::Result<()> {
    let mut challenge = [0u8; 32];
    SystemRandom::new()
        .fill(&mut challenge)
        .map_err(|_| io::Error::other("challenge generation failed"))?;
    write_frame(&mut stream, CHALLENGE, &challenge).await?;
    let (kind, body, bytes_permit) = match read_request_frame(
        &mut stream,
        request_bytes.clone(),
        peer_frame_budget.clone(),
    )
    .await
    {
        Ok(value) => value,
        Err(error) => {
            // Counted before the return: the peer is gone and nothing can
            // answer it, but a frame that never decoded is still an event
            // this process observed and previously did not report.
            request_frame_unreadable(
                config.identity.node(),
                config.identity.group().group,
                node_id,
                &error,
            );
            return Ok(());
        }
    };
    if kind == SNAPSHOT_MANIFEST {
        phase.store(1, Ordering::Relaxed);
    }
    let envelope = match SignedRaftEnvelope::decode_json(&body) {
        Ok(envelope) => envelope,
        Err(_) if kind == SNAPSHOT_MANIFEST => {
            return Err(io::Error::other("invalid snapshot manifest envelope"))
        }
        Err(_) => {
            envelope_rejected(
                config.identity.node(),
                config.identity.group().group,
                "request envelope does not decode",
            );
            // Return `Ok` so this is counted once, as a rejection, not again
            // as a failed connection. The peer sees a closed socket either way.
            return Ok(());
        }
    };
    let payload = match envelope.verify(&config.identity, node_id, &config.registry) {
        Ok(payload) => payload,
        Err(_) if kind == SNAPSHOT_MANIFEST => {
            return Err(io::Error::other("invalid snapshot manifest signature"))
        }
        Err(_) => {
            envelope_rejected(
                config.identity.node(),
                config.identity.group().group,
                "request signature, sender or recipient does not verify",
            );
            return Ok(());
        }
    };
    let sender = envelope.sender;
    if kind == SNAPSHOT_MANIFEST {
        snapshot_peer.store(sender as u64, Ordering::Relaxed);
    }
    let sequence = envelope.sequence;
    let (nonce, request, _assembly_permit) = if kind == REQUEST {
        let call: WireCall = match serde_json::from_slice(payload) {
            Ok(call) => call,
            Err(_) => {
                envelope_rejected(
                    config.identity.node(),
                    config.identity.group().group,
                    "request body does not decode",
                );
                return Ok(());
            }
        };
        if call.challenge != challenge
            || !call.request.sender_matches(sender)
            || matches!(call.request, WireRequest::InstallSnapshot { .. })
        {
            // A wrong challenge is the one case a replayed or re-targeted
            // frame produces, so it is counted rather than ignored.
            envelope_rejected(
                config.identity.node(),
                config.identity.group().group,
                "request challenge, sender or request kind does not match",
            );
            return Ok(());
        }
        (call.nonce, call.request, None)
    } else {
        if config.tls.is_none() {
            return Err(io::Error::other("snapshot requires TLS"));
        }
        if payload.len() > SNAPSHOT_MANIFEST_PAYLOAD {
            return Err(io::Error::other("snapshot manifest too large"));
        }
        let manifest: SnapshotManifest = serde_json::from_slice(payload)
            .map_err(|_| io::Error::other("invalid snapshot manifest"))?;
        if manifest.context != SNAPSHOT_CONTEXT
            || manifest.challenge != challenge
            || manifest.length > MAX_SNAPSHOT_BYTES
            || manifest.vote.leader_id.voted_for() != Some(sender)
        {
            return Err(io::Error::other("snapshot manifest validation failed"));
        }
        // Do not hold the encoded-frame reservation while waiting for assembly
        // or subsequent chunk reservations.
        drop(envelope);
        drop(body);
        drop(bytes_permit);
        let permit = snapshot_bytes
            .acquire_many_owned(manifest.length as u32)
            .await
            .map_err(|_| io::Error::other("snapshot budget closed"))?;
        phase.store(2, Ordering::Relaxed);
        let mut data = Vec::with_capacity(manifest.length);
        let mut ordinal = 0;
        while data.len() < manifest.length {
            let (_, frame, _frame_permit) = read_budgeted_frame(
                &mut stream,
                request_bytes.clone(),
                peer_frame_budget.clone(),
                false,
            )
            .await?;
            let chunk_envelope = SignedRaftEnvelope::decode_json(&frame)
                .map_err(|_| io::Error::other("invalid chunk envelope"))?;
            if chunk_envelope.sender != sender || chunk_envelope.sequence != sequence {
                return Err(io::Error::other("snapshot chunk identity mismatch"));
            }
            let payload = chunk_envelope
                .verify(&config.identity, node_id, &config.registry)
                .map_err(|_| io::Error::other("invalid chunk signature"))?;
            let chunk: SnapshotChunk =
                serde_json::from_slice(payload).map_err(|_| io::Error::other("invalid chunk"))?;
            let expected = (manifest.length - data.len()).min(SNAPSHOT_CHUNK_BYTES);
            if chunk.context != SNAPSHOT_CONTEXT
                || chunk.nonce != manifest.nonce
                || chunk.challenge != challenge
                || chunk.ordinal != ordinal
                || chunk.length != manifest.length
                || chunk.digest != manifest.digest
                || chunk.data.len() != expected
            {
                return Err(io::Error::other("snapshot chunk validation failed"));
            }
            data.extend_from_slice(&chunk.data);
            ordinal += 1;
        }
        if ring::digest::digest(&ring::digest::SHA256, &data).as_ref() != manifest.digest {
            return Err(io::Error::other("snapshot digest mismatch"));
        }
        (
            manifest.nonce,
            WireRequest::InstallSnapshot {
                vote: manifest.vote,
                meta: manifest.meta,
                data,
            },
            Some(permit),
        )
    };
    if kind == SNAPSHOT_MANIFEST {
        phase.store(3, Ordering::Relaxed);
    }
    let response = handler(request).await;
    if kind == SNAPSHOT_MANIFEST {
        phase.store(4, Ordering::Relaxed);
    }
    let payload = match serde_json::to_vec(&WireReply { nonce, response }) {
        Ok(encoded) => encoded,
        Err(_) => return Ok(()),
    };
    let encoded = match SignedRaftEnvelope::sign(
        &config.identity,
        sender,
        sequence,
        payload,
        &config.signing_key,
    )
    .and_then(|response| response.encode_json())
    {
        Ok(encoded) => encoded,
        Err(_) => return Ok(()),
    };
    if write_frame(&mut stream, RESPONSE, &encoded).await.is_err() {
        if kind == SNAPSHOT_MANIFEST {
            return Err(io::Error::other("snapshot reply write failed"));
        }
        return Ok(());
    }
    Ok(())
}

async fn read_request_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    budget: Arc<Semaphore>,
    peer: Option<PeerFrameBudget>,
) -> io::Result<(u8, Vec<u8>, FrameBytes)> {
    read_budgeted_frame(stream, budget, peer, true).await
}

async fn read_budgeted_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    budget: Arc<Semaphore>,
    peer: Option<PeerFrameBudget>,
    initial: bool,
) -> io::Result<(u8, Vec<u8>, FrameBytes)> {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let length = u32::from_le_bytes(header[1..].try_into().unwrap()) as usize;
    let limit = match (initial, header[0]) {
        (true, REQUEST) => MAX_FRAME,
        (true, SNAPSHOT_MANIFEST) => SNAPSHOT_MANIFEST_FRAME,
        (false, SNAPSHOT_CHUNK) => SNAPSHOT_CHUNK_FRAME,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid frame kind",
            ))
        }
    };
    if length > limit || length > MAX_IN_FLIGHT_REQUEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid request frame",
        ));
    }
    // Take the address's share before the global budget. Taking the global
    // permits first would let one address park the global budget on frames
    // that cannot proceed, which is the starvation this sub-cap exists to
    // stop. Every handler takes them in this order, so the two resources
    // cannot deadlock: a task waiting for the global budget already holds its
    // own address's share and waits for nothing else.
    let peer_permit = match &peer {
        Some(peer) => Some(peer.reserve(length).await),
        None => None,
    };
    let permit = budget
        .acquire_many_owned(length as u32)
        .await
        .map_err(|_| io::Error::other("request budget closed"))?;
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await?;
    Ok((
        header[0],
        body,
        FrameBytes {
            _global: permit,
            _peer: peer_permit,
        },
    ))
}

async fn write_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    kind: u8,
    body: &[u8],
) -> io::Result<()> {
    if body.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds limit",
        ));
    }
    let mut header = [0u8; 5];
    header[0] = kind;
    header[1..].copy_from_slice(&(body.len() as u32).to_le_bytes());
    stream.write_all(&header).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

async fn read_frame<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<(u8, Vec<u8>)> {
    read_frame_limited(stream, MAX_FRAME).await
}

async fn read_frame_limited<S: AsyncRead + Unpin>(
    stream: &mut S,
    limit: usize,
) -> io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let length = u32::from_le_bytes(header[1..].try_into().unwrap()) as usize;
    if length > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds limit",
        ));
    }
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await?;
    Ok((header[0], body))
}

#[cfg(test)]
mod reply_tests {
    use super::*;
    use crate::multiraft::GroupIdentity;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn challenge_length_is_checked_before_body_read() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(&[CHALLENGE, 33, 0, 0, 0]).await.unwrap();
        assert_eq!(
            read_frame_limited(&mut reader, 32)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[tokio::test]
    async fn request_budget_is_acquired_before_body_and_released_on_cancel() {
        let budget = Arc::new(Semaphore::new(4));
        let held = budget.clone().acquire_many_owned(4).await.unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(&[REQUEST, 4, 0, 0, 0]).await.unwrap();
        let task = tokio::spawn(async move { read_request_frame(&mut reader, budget, None).await });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        drop(held);
        writer.write_all(&[1, 2, 3, 4]).await.unwrap();
        let (_, body, permit) = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(body, [1, 2, 3, 4]);
        drop(permit);
    }

    #[tokio::test]
    async fn permits_recover_after_panic_and_cancellation() {
        let connections = Arc::new(Semaphore::new(1));
        let bytes = Arc::new(Semaphore::new(4));
        let connection = connections.clone().acquire_owned().await.unwrap();
        let byte_permit = bytes.clone().acquire_many_owned(4).await.unwrap();
        let task = tokio::spawn(async move {
            let _connection = connection;
            let _bytes = byte_permit;
            panic!("test handler panic");
        });
        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(connections.available_permits(), 1);
        assert_eq!(bytes.available_permits(), 4);
        let connection = connections.clone().acquire_owned().await.unwrap();
        let byte_permit = bytes.clone().acquire_many_owned(4).await.unwrap();
        let (started, wait) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _connection = connection;
            let _bytes = byte_permit;
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        wait.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(connections.available_permits(), 1);
        assert_eq!(bytes.available_permits(), 4);
    }

    #[tokio::test]
    async fn dropping_server_cancels_active_handlers() {
        let group = GroupIdentity {
            cluster: [3; 16],
            group: 1,
            incarnation: 1,
        };
        let server_id = ReplicaIdentity::new(group.clone(), 2, vec![1, 2]).unwrap();
        let client_id = ReplicaIdentity::new(group, 1, vec![1, 2]).unwrap();
        let key1 = Arc::new(NodeSigningKey::from_seed(&[1; 32]).unwrap());
        let key2 = Arc::new(NodeSigningKey::from_seed(&[2; 32]).unwrap());
        let registry = Arc::new(
            PeerKeyRegistry::new(BTreeMap::from([
                (1, key1.public_key()),
                (2, key2.public_key()),
            ]))
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let started_tx = std::sync::Mutex::new(Some(started_tx));
        let handler = Arc::new(move |_: WireRequest| -> BoxFuture<WireResponse> {
            started_tx.lock().unwrap().take().unwrap().send(()).unwrap();
            Box::pin(std::future::pending())
        });
        let config = TransportConfig::new(server_id, key2, BTreeMap::new(), registry);
        let task = tokio::spawn(RaftPeerServer::from_listener(listener, config, handler).serve());
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let (_, challenge) = read_frame(&mut stream).await.unwrap();
        let call = WireCall {
            nonce: [0; 32],
            challenge: challenge.try_into().unwrap(),
            request: WireRequest::Vote(Box::new(VoteRequest {
                vote: Vote::new(1, 1),
                last_log_id: None,
            })),
        };
        let body =
            SignedRaftEnvelope::sign(&client_id, 2, 1, serde_json::to_vec(&call).unwrap(), &key1)
                .unwrap()
                .encode_json()
                .unwrap();
        write_frame(&mut stream, REQUEST, &body).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        let _ = task.await;
        assert!(
            tokio::time::timeout(Duration::from_secs(2), read_frame(&mut stream))
                .await
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn explicit_shutdown_drains_active_handler() {
        struct NotifyDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for NotifyDrop {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }
        let group = GroupIdentity {
            cluster: [4; 16],
            group: 1,
            incarnation: 1,
        };
        let server_id = ReplicaIdentity::new(group.clone(), 2, vec![1, 2]).unwrap();
        let client_id = ReplicaIdentity::new(group, 1, vec![1, 2]).unwrap();
        let key1 = Arc::new(NodeSigningKey::from_seed(&[1; 32]).unwrap());
        let key2 = Arc::new(NodeSigningKey::from_seed(&[2; 32]).unwrap());
        let registry = Arc::new(
            PeerKeyRegistry::new(BTreeMap::from([
                (1, key1.public_key()),
                (2, key2.public_key()),
            ]))
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, mut dropped_rx) = tokio::sync::oneshot::channel();
        let state = std::sync::Mutex::new(Some((started_tx, dropped_tx)));
        let handler = Arc::new(move |_: WireRequest| -> BoxFuture<WireResponse> {
            let (started, dropped) = state.lock().unwrap().take().unwrap();
            Box::pin(async move {
                let _guard = NotifyDrop(Some(dropped));
                let _ = started.send(());
                std::future::pending().await
            })
        });
        let config = TransportConfig::new(server_id, key2, BTreeMap::new(), registry);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(
            RaftPeerServer::from_listener(listener, config, handler).serve_until(async {
                let _ = shutdown_rx.await;
            }),
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let (_, challenge) = read_frame(&mut stream).await.unwrap();
        let call = WireCall {
            nonce: [0; 32],
            challenge: challenge.try_into().unwrap(),
            request: WireRequest::Vote(Box::new(VoteRequest {
                vote: Vote::new(1, 1),
                last_log_id: None,
            })),
        };
        let body =
            SignedRaftEnvelope::sign(&client_id, 2, 1, serde_json::to_vec(&call).unwrap(), &key1)
                .unwrap()
                .encode_json()
                .unwrap();
        write_frame(&mut stream, REQUEST, &body).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            dropped_rx.try_recv().is_ok(),
            "handler must be dropped before shutdown returns"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(2), read_frame(&mut stream))
                .await
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn large_snapshot_stream_round_trip() {
        use openraft::network::RaftNetworkFactory;
        let group = GroupIdentity {
            cluster: [5; 16],
            group: 1,
            incarnation: 1,
        };
        let client_id = ReplicaIdentity::new(group.clone(), 1, vec![1, 2]).unwrap();
        let server_id = ReplicaIdentity::new(group, 2, vec![1, 2]).unwrap();
        let key1 = Arc::new(NodeSigningKey::from_seed(&[1; 32]).unwrap());
        let key2 = Arc::new(NodeSigningKey::from_seed(&[2; 32]).unwrap());
        let registry = Arc::new(
            PeerKeyRegistry::new(BTreeMap::from([
                (1, key1.public_key()),
                (2, key2.public_key()),
            ]))
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = std::sync::Mutex::new(Some(tx));
        let handler = Arc::new(move |request: WireRequest| -> BoxFuture<WireResponse> {
            let WireRequest::InstallSnapshot { data, .. } = request else {
                panic!("unexpected request")
            };
            tx.lock()
                .unwrap()
                .take()
                .unwrap()
                .send((data.len(), data.iter().all(|x| *x == 42)))
                .unwrap();
            Box::pin(async {
                WireResponse::InstallSnapshot(Box::new(SnapshotResponse {
                    vote: Vote::new(1, 2),
                }))
            })
        });
        let mut server_config =
            TransportConfig::new(server_id, key2, BTreeMap::new(), registry.clone());
        server_config.request_timeout = Duration::from_secs(180);
        server_config =
            server_config.with_tls(Arc::new(crate::openraft_tls::test_support::config(2)));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            RaftPeerServer::from_listener(listener, server_config, handler).serve_until(async {
                let _ = stopped.await;
            }),
        );
        let config = TransportConfig::new(client_id, key1, BTreeMap::from([(2, addr)]), registry)
            .with_tls(Arc::new(crate::openraft_tls::test_support::config(1)));
        let mut client = AuthenticatedTcpNetworkFactory::new(config)
            .new_client(2, &BasicNode::new("node-2.test"))
            .await;
        let data = vec![42; 17 * 1024 * 1024];
        let result = client
            .call_for_test(
                WireRequest::InstallSnapshot {
                    vote: Vote::new(1, 1),
                    meta: SnapshotMeta::default(),
                    data,
                },
                Duration::from_secs(180),
            )
            .await;
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(rx.await.unwrap(), (17 * 1024 * 1024, true));
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn snapshot_stream_rejects_bad_data_and_releases_budget_on_disconnect() {
        let group = GroupIdentity {
            cluster: [6; 16],
            group: 1,
            incarnation: 1,
        };
        let client_id = ReplicaIdentity::new(group.clone(), 1, vec![1, 2]).unwrap();
        let server_id = ReplicaIdentity::new(group, 2, vec![1, 2]).unwrap();
        let key1 = Arc::new(NodeSigningKey::from_seed(&[1; 32]).unwrap());
        let key2 = Arc::new(NodeSigningKey::from_seed(&[2; 32]).unwrap());
        let registry = Arc::new(
            PeerKeyRegistry::new(BTreeMap::from([
                (1, key1.public_key()),
                (2, key2.public_key()),
            ]))
            .unwrap(),
        );
        // Exercise the framing layer after the TLS handshake (covered separately).
        let config = TransportConfig::new(server_id, key2, BTreeMap::new(), registry)
            .with_tls(Arc::new(crate::openraft_tls::test_support::config(2)));
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let assembly = Arc::new(Semaphore::new(MAX_SNAPSHOT_BYTES));
        let count = Arc::new(AtomicUsize::new(0));
        for case in 0..5 {
            let (mut client, server) = tokio::io::duplex(65536);
            let count = count.clone();
            let handler = Arc::new(move |_: WireRequest| -> BoxFuture<WireResponse> {
                count.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { panic!("invalid snapshot dispatched") })
            });
            let task = tokio::spawn(tokio::time::timeout(
                if case == 4 {
                    Duration::from_millis(500)
                } else {
                    Duration::from_secs(2)
                },
                serve_connection(
                    Box::new(server),
                    None,
                    config.clone(),
                    handler,
                    budget.clone(),
                    assembly.clone(),
                    None,
                    Arc::new(AtomicU64::new(0)),
                    Arc::new(AtomicU64::new(0)),
                ),
            ));
            let (_, challenge) = read_frame(&mut client).await.unwrap();
            let challenge: [u8; 32] = challenge.try_into().unwrap();
            let data = vec![1, 2, 3];
            let digest: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, &data)
                .as_ref()
                .try_into()
                .unwrap();
            let manifest = SnapshotManifest {
                context: SNAPSHOT_CONTEXT.into(),
                nonce: [9; 32],
                challenge,
                vote: Vote::new(1, 1),
                meta: SnapshotMeta::default(),
                length: if case == 2 { MAX_SNAPSHOT_BYTES + 1 } else { 3 },
                digest: if case == 1 { [0; 32] } else { digest },
            };
            let sign = |payload: Vec<u8>| {
                SignedRaftEnvelope::sign(&client_id, 2, 7, payload, &key1)
                    .unwrap()
                    .encode_json()
                    .unwrap()
            };
            write_frame(
                &mut client,
                SNAPSHOT_MANIFEST,
                &sign(serde_json::to_vec(&manifest).unwrap()),
            )
            .await
            .unwrap();
            if case < 2 {
                let chunk = SnapshotChunk {
                    context: SNAPSHOT_CONTEXT.into(),
                    nonce: manifest.nonce,
                    challenge,
                    ordinal: if case == 0 { 1 } else { 0 },
                    length: 3,
                    digest: manifest.digest,
                    data,
                };
                write_frame(
                    &mut client,
                    SNAPSHOT_CHUNK,
                    &sign(serde_json::to_vec(&chunk).unwrap()),
                )
                .await
                .unwrap();
            } else if case >= 3 {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while assembly.available_permits() == MAX_SNAPSHOT_BYTES {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            if case != 4 {
                drop(client);
            }
            let result = tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap();
            if case == 4 {
                assert!(result.is_err(), "stalled transfer must time out");
            }
            assert_eq!(assembly.available_permits(), MAX_SNAPSHOT_BYTES);
            assert_eq!(budget.available_permits(), MAX_IN_FLIGHT_REQUEST_BYTES);
        }
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    // ---- Snapshot reception bounds ----
    //
    // A receiver admits a snapshot only after the declared length has been
    // checked and the assembly budget reserved, and it dispatches the handler
    // only after the whole stream has arrived and its digest matches. The
    // tests below pin each step against the semaphore, the `phase` marker and
    // the dispatch record rather than against elapsed time.

    struct SnapshotPeer {
        client: ReplicaIdentity,
        server: ReplicaIdentity,
        client_key: Arc<NodeSigningKey>,
        server_key: Arc<NodeSigningKey>,
        registry: Arc<PeerKeyRegistry>,
        config: TransportConfig,
    }

    fn snapshot_peer(tag: u8) -> SnapshotPeer {
        let group = GroupIdentity {
            cluster: [tag; 16],
            group: 1,
            incarnation: 1,
        };
        let client = ReplicaIdentity::new(group.clone(), 1, vec![1, 2]).unwrap();
        let server = ReplicaIdentity::new(group, 2, vec![1, 2]).unwrap();
        let mut seed = [0u8; 32];
        seed[0] = 1;
        let client_key = Arc::new(NodeSigningKey::from_seed(&seed).unwrap());
        seed[0] = 2;
        let server_key = Arc::new(NodeSigningKey::from_seed(&seed).unwrap());
        let registry = Arc::new(
            PeerKeyRegistry::new(BTreeMap::from([
                (1, client_key.public_key()),
                (2, server_key.public_key()),
            ]))
            .unwrap(),
        );
        let config = TransportConfig::new(
            server.clone(),
            server_key.clone(),
            BTreeMap::new(),
            registry.clone(),
        )
        .with_tls(Arc::new(crate::openraft_tls::test_support::config(2)));
        SnapshotPeer {
            client,
            server,
            client_key,
            server_key,
            registry,
            config,
        }
    }

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        ring::digest::digest(&ring::digest::SHA256, bytes)
            .as_ref()
            .try_into()
            .unwrap()
    }

    fn signed_snapshot_frame(peer: &SnapshotPeer, sequence: u64, payload: Vec<u8>) -> Vec<u8> {
        SignedRaftEnvelope::sign(
            &peer.client,
            peer.server.node(),
            sequence,
            payload,
            &peer.client_key,
        )
        .unwrap()
        .encode_json()
        .unwrap()
    }

    fn manifest(
        peer: &SnapshotPeer,
        challenge: [u8; 32],
        length: usize,
        digest: [u8; 32],
    ) -> SnapshotManifest {
        SnapshotManifest {
            context: SNAPSHOT_CONTEXT.into(),
            nonce: [3; 32],
            challenge,
            vote: Vote::new(1, peer.client.node()),
            meta: SnapshotMeta::default(),
            length,
            digest,
        }
    }

    fn chunk(
        challenge: [u8; 32],
        ordinal: usize,
        length: usize,
        digest: [u8; 32],
        data: Vec<u8>,
    ) -> SnapshotChunk {
        SnapshotChunk {
            context: SNAPSHOT_CONTEXT.into(),
            nonce: [3; 32],
            challenge,
            ordinal,
            length,
            digest,
            data,
        }
    }

    fn recording_handler(
        received: Arc<std::sync::Mutex<Vec<usize>>>,
    ) -> Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> {
        Arc::new(move |request| {
            let WireRequest::InstallSnapshot { data, .. } = request else {
                panic!("unexpected request kind")
            };
            received.lock().unwrap().push(data.len());
            Box::pin(async {
                WireResponse::InstallSnapshot(Box::new(SnapshotResponse {
                    vote: Vote::new(1, 2),
                }))
            })
        })
    }

    struct SnapshotWire {
        client: tokio::io::DuplexStream,
        task: tokio::task::JoinHandle<io::Result<()>>,
    }

    /// Serves one snapshot connection over an in-memory duplex stream.
    fn snapshot_wire(
        peer: &SnapshotPeer,
        received: Arc<std::sync::Mutex<Vec<usize>>>,
        budget: Arc<Semaphore>,
        assembly: Arc<Semaphore>,
        phase: Arc<AtomicU64>,
        capacity: usize,
    ) -> SnapshotWire {
        let (client, server) = tokio::io::duplex(capacity);
        let config = peer.config.clone();
        let task = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_secs(10),
                serve_connection(
                    Box::new(server),
                    None,
                    config,
                    recording_handler(received),
                    budget,
                    assembly,
                    None,
                    phase,
                    Arc::new(AtomicU64::new(0)),
                ),
            )
            .await
            .expect("serve_connection exceeded its deadline")
        });
        SnapshotWire { client, task }
    }

    #[tokio::test]
    async fn an_over_declared_snapshot_length_is_refused_before_the_assembly_reservation() {
        // The assembly semaphore is the reservation taken immediately before
        // `Vec::with_capacity(manifest.length)`. Holding every permit makes a
        // legal manifest block there, so a refusal that still closes the
        // connection proves the declared-length check ran before the
        // reservation and therefore before the allocation. The second length
        // truncates to 1 in `manifest.length as u32`, so it also pins the
        // check that keeps a truncated request from reserving one permit and
        // then allocating 4 GiB.
        let peer = snapshot_peer(31);
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let assembly = Arc::new(Semaphore::new(MAX_SNAPSHOT_BYTES));
        let held = assembly
            .clone()
            .acquire_many_owned(MAX_SNAPSHOT_BYTES as u32)
            .await
            .unwrap();
        for declared in [MAX_SNAPSHOT_BYTES + 1, u32::MAX as usize + 2] {
            let received = Arc::new(std::sync::Mutex::new(Vec::new()));
            let wire = snapshot_wire(
                &peer,
                received.clone(),
                budget.clone(),
                assembly.clone(),
                Arc::new(AtomicU64::new(0)),
                4096,
            );
            let SnapshotWire { mut client, task } = wire;
            let (_, challenge) = read_frame(&mut client).await.unwrap();
            let challenge: [u8; 32] = challenge.try_into().unwrap();
            let manifest = manifest(&peer, challenge, declared, [0; 32]);
            write_frame(
                &mut client,
                SNAPSHOT_MANIFEST,
                &signed_snapshot_frame(&peer, 1, serde_json::to_vec(&manifest).unwrap()),
            )
            .await
            .unwrap();
            let refused = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut client))
                .await
                .expect("the declared length was not refused before the reservation");
            assert!(
                refused.is_err(),
                "a manifest declaring {declared} bytes was answered"
            );
            assert!(
                received.lock().unwrap().is_empty(),
                "a manifest declaring {declared} bytes was dispatched"
            );
            let result = task.await.unwrap();
            assert!(
                result.is_err(),
                "a manifest declaring {declared} bytes did not fail the connection"
            );
        }
        drop(held);
        assert_eq!(assembly.available_permits(), MAX_SNAPSHOT_BYTES);
        assert_eq!(budget.available_permits(), MAX_IN_FLIGHT_REQUEST_BYTES);
    }

    #[tokio::test]
    async fn an_exhausted_assembly_budget_blocks_the_manifest_before_allocation_or_dispatch() {
        // `phase` becomes 1 once the manifest frame has been read and 2 only
        // after the assembly reservation succeeds. With every permit held, a
        // valid manifest must stop at 1: reaching 2 would mean the receiver
        // allocated and dispatched without the reservation.
        let peer = snapshot_peer(32);
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let assembly = Arc::new(Semaphore::new(MAX_SNAPSHOT_BYTES));
        let held = assembly
            .clone()
            .acquire_many_owned(MAX_SNAPSHOT_BYTES as u32)
            .await
            .unwrap();
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let phase = Arc::new(AtomicU64::new(0));
        let wire = snapshot_wire(
            &peer,
            received.clone(),
            budget.clone(),
            assembly.clone(),
            phase.clone(),
            4096,
        );
        let SnapshotWire { mut client, task } = wire;
        let (_, challenge) = read_frame(&mut client).await.unwrap();
        let challenge: [u8; 32] = challenge.try_into().unwrap();
        let data = vec![1u8, 2, 3];
        let digest = sha256(&data);
        write_frame(
            &mut client,
            SNAPSHOT_MANIFEST,
            &signed_snapshot_frame(
                &peer,
                1,
                serde_json::to_vec(&manifest(&peer, challenge, data.len(), digest)).unwrap(),
            ),
        )
        .await
        .unwrap();
        write_frame(
            &mut client,
            SNAPSHOT_CHUNK,
            &signed_snapshot_frame(
                &peer,
                1,
                serde_json::to_vec(&chunk(challenge, 0, data.len(), digest, data.clone())).unwrap(),
            ),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while phase.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the manifest frame was never read");
        // The chunk is already buffered, so only the reservation can hold the
        // transfer back from here.
        let advanced = tokio::time::timeout(Duration::from_millis(300), async {
            while phase.load(Ordering::Relaxed) == 1 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            advanced.is_err(),
            "a manifest passed the assembly reservation while the budget was exhausted (phase {})",
            phase.load(Ordering::Relaxed)
        );
        assert!(
            received.lock().unwrap().is_empty(),
            "a manifest was dispatched while the assembly budget was exhausted"
        );
        drop(held);
        let (kind, _) = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut client))
            .await
            .expect("the transfer did not resume after the budget was released")
            .unwrap();
        assert_eq!(kind, RESPONSE);
        assert_eq!(*received.lock().unwrap(), vec![data.len()]);
        assert_eq!(phase.load(Ordering::Relaxed), 4);
        assert!(task.await.unwrap().is_ok());
        assert_eq!(assembly.available_permits(), MAX_SNAPSHOT_BYTES);
        assert_eq!(budget.available_permits(), MAX_IN_FLIGHT_REQUEST_BYTES);
    }

    #[tokio::test]
    async fn a_chunk_carrying_more_bytes_than_the_manifest_declares_is_refused() {
        // The sender signs both the declared length and the digest, so it can
        // sign a digest over four bytes while declaring three. Only the
        // per-chunk length check refuses the extra byte: without it the
        // receiver would append four bytes, the digest would match, and a
        // snapshot larger than its own declaration would be installed.
        let peer = snapshot_peer(33);
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let assembly = Arc::new(Semaphore::new(MAX_SNAPSHOT_BYTES));
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let wire = snapshot_wire(
            &peer,
            received.clone(),
            budget.clone(),
            assembly.clone(),
            Arc::new(AtomicU64::new(0)),
            4096,
        );
        let SnapshotWire { mut client, task } = wire;
        let (_, challenge) = read_frame(&mut client).await.unwrap();
        let challenge: [u8; 32] = challenge.try_into().unwrap();
        let data = vec![1u8, 2, 3, 4];
        let digest = sha256(&data);
        write_frame(
            &mut client,
            SNAPSHOT_MANIFEST,
            &signed_snapshot_frame(
                &peer,
                1,
                serde_json::to_vec(&manifest(&peer, challenge, 3, digest)).unwrap(),
            ),
        )
        .await
        .unwrap();
        write_frame(
            &mut client,
            SNAPSHOT_CHUNK,
            &signed_snapshot_frame(
                &peer,
                1,
                serde_json::to_vec(&chunk(challenge, 0, 3, digest, data)).unwrap(),
            ),
        )
        .await
        .unwrap();
        let refused = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut client))
            .await
            .expect("an over-long chunk hung the connection");
        assert!(refused.is_err(), "an over-long chunk was answered");
        drop(client);
        let result = task.await.unwrap();
        assert!(
            result.is_err(),
            "an over-long chunk did not fail the connection"
        );
        assert!(
            received.lock().unwrap().is_empty(),
            "an over-long chunk was dispatched"
        );
        assert_eq!(assembly.available_permits(), MAX_SNAPSHOT_BYTES);
        assert_eq!(budget.available_permits(), MAX_IN_FLIGHT_REQUEST_BYTES);
    }

    #[tokio::test]
    async fn a_truncated_snapshot_stream_never_dispatches_its_valid_prefix() {
        // The sender declares one byte more than it sends and signs a digest
        // over exactly the prefix it did send. A receiver that treated the
        // read error as the end of the stream would verify that digest and
        // install a snapshot one byte shorter than its own manifest.
        let peer = snapshot_peer(34);
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let assembly = Arc::new(Semaphore::new(MAX_SNAPSHOT_BYTES));
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let wire = snapshot_wire(
            &peer,
            received.clone(),
            budget.clone(),
            assembly.clone(),
            Arc::new(AtomicU64::new(0)),
            SNAPSHOT_CHUNK_FRAME,
        );
        let SnapshotWire { mut client, task } = wire;
        let (_, challenge) = read_frame(&mut client).await.unwrap();
        let challenge: [u8; 32] = challenge.try_into().unwrap();
        let declared = SNAPSHOT_CHUNK_BYTES + 1;
        let data = vec![7u8; SNAPSHOT_CHUNK_BYTES];
        let digest = sha256(&data);
        write_frame(
            &mut client,
            SNAPSHOT_MANIFEST,
            &signed_snapshot_frame(
                &peer,
                1,
                serde_json::to_vec(&manifest(&peer, challenge, declared, digest)).unwrap(),
            ),
        )
        .await
        .unwrap();
        write_frame(
            &mut client,
            SNAPSHOT_CHUNK,
            &signed_snapshot_frame(
                &peer,
                1,
                serde_json::to_vec(&chunk(challenge, 0, declared, digest, data)).unwrap(),
            ),
        )
        .await
        .unwrap();
        // Signal the end of the stream: the sender declared one byte more than
        // it sent, so the receiver must fail rather than install the prefix.
        client.shutdown().await.unwrap();
        let refused = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut client))
            .await
            .expect("a truncated snapshot hung the connection");
        assert!(refused.is_err(), "a truncated snapshot was answered");
        drop(client);
        let result = task.await.unwrap();
        assert!(
            result.is_err(),
            "a truncated snapshot did not fail the connection"
        );
        assert!(
            received.lock().unwrap().is_empty(),
            "a truncated snapshot dispatched its valid prefix"
        );
        assert_eq!(assembly.available_permits(), MAX_SNAPSHOT_BYTES);
        assert_eq!(budget.available_permits(), MAX_IN_FLIGHT_REQUEST_BYTES);
    }

    #[tokio::test]
    async fn concurrent_frames_share_one_in_flight_byte_budget() {
        // The budget is one semaphore shared by every handler, not one per
        // connection. One frame declaring the whole six-permit budget must
        // stop a second frame from reserving anything.
        let budget = Arc::new(Semaphore::new(6));
        let (mut first_writer, mut first_reader) = tokio::io::duplex(64);
        let (mut second_writer, mut second_reader) = tokio::io::duplex(64);
        first_writer
            .write_all(&[REQUEST, 6, 0, 0, 0])
            .await
            .unwrap();
        second_writer
            .write_all(&[REQUEST, 6, 0, 0, 0])
            .await
            .unwrap();
        let first = tokio::spawn({
            let budget = budget.clone();
            async move {
                let (_, body, permit) = read_request_frame(&mut first_reader, budget, None).await?;
                drop(permit);
                Ok::<Vec<u8>, io::Error>(body)
            }
        });
        let second = tokio::spawn({
            let budget = budget.clone();
            async move {
                let (_, body, permit) =
                    read_request_frame(&mut second_reader, budget, None).await?;
                drop(permit);
                Ok::<Vec<u8>, io::Error>(body)
            }
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while budget.available_permits() != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "one 6-byte frame did not consume the shared budget: {} permits remain",
                budget.available_permits()
            );
            tokio::task::yield_now().await;
        }
        // The first frame now holds the whole budget. The second frame may
        // take nothing from it: a per-connection budget, or a one-permit
        // reservation, would leave permits here. The first frame cannot
        // complete before its body arrives, so a correct receiver stays at
        // zero for the whole window.
        let stolen = tokio::time::timeout(Duration::from_millis(200), async {
            while budget.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            stolen.is_err(),
            "a second frame reserved {} permits while the first held the whole budget",
            6 - budget.available_permits()
        );
        first_writer.write_all(&[1; 6]).await.unwrap();
        second_writer.write_all(&[2; 6]).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first, [1; 6]);
        assert_eq!(second, [2; 6]);
        assert_eq!(budget.available_permits(), 6);
    }

    #[tokio::test]
    async fn a_refused_snapshot_chunk_fails_closed_and_the_next_snapshot_succeeds() {
        // A refused chunk must not answer, must not hang, and must not cost
        // the listener its budget: the same client sends a valid snapshot over
        // a fresh connection immediately afterwards.
        use openraft::network::RaftNetworkFactory;
        let peer = snapshot_peer(35);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut config = TransportConfig::new(
            peer.server.clone(),
            peer.server_key.clone(),
            BTreeMap::new(),
            peer.registry.clone(),
        )
        .with_tls(Arc::new(crate::openraft_tls::test_support::config(2)));
        config.request_timeout = Duration::from_secs(30);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            RaftPeerServer::from_listener(listener, config, recording_handler(received.clone()))
                .serve_until(async {
                    let _ = stopped.await;
                }),
        );

        // Speak the wire protocol directly so the test can send a chunk the
        // client encoder would never produce.
        let tls = crate::openraft_tls::test_support::config(1);
        let tcp = TcpStream::connect(addr).await.unwrap();
        let mut raw: PeerStream =
            Box::new(tls.client.connect(tls.name(2).unwrap(), tcp).await.unwrap());
        let (kind, challenge) = read_frame(&mut raw).await.unwrap();
        assert_eq!(kind, CHALLENGE);
        let challenge: [u8; 32] = challenge.try_into().unwrap();
        let data = vec![9u8; 3];
        let digest = sha256(&data);
        write_frame(
            &mut raw,
            SNAPSHOT_MANIFEST,
            &signed_snapshot_frame(
                &peer,
                1,
                serde_json::to_vec(&manifest(&peer, challenge, 3, digest)).unwrap(),
            ),
        )
        .await
        .unwrap();
        // One byte short of the three the manifest declares.
        write_frame(
            &mut raw,
            SNAPSHOT_CHUNK,
            &signed_snapshot_frame(
                &peer,
                1,
                serde_json::to_vec(&chunk(challenge, 0, 3, digest, data[..2].to_vec())).unwrap(),
            ),
        )
        .await
        .unwrap();
        let refused = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut raw))
            .await
            .expect("a refused chunk left the connection hanging");
        assert!(refused.is_err(), "a refused chunk was answered");
        assert!(
            received.lock().unwrap().is_empty(),
            "a refused chunk was dispatched"
        );
        drop(raw);

        // The sender is still usable: a fresh connection and a valid snapshot.
        let mut client = AuthenticatedTcpNetworkFactory::new(
            TransportConfig::new(
                peer.client.clone(),
                peer.client_key.clone(),
                BTreeMap::from([(2, addr)]),
                peer.registry.clone(),
            )
            .with_tls(Arc::new(crate::openraft_tls::test_support::config(1))),
        )
        .new_client(2, &BasicNode::new("node-2.test"))
        .await;
        let result = client
            .call_for_test(
                WireRequest::InstallSnapshot {
                    vote: Vote::new(1, 1),
                    meta: SnapshotMeta::default(),
                    data: vec![5u8; 3],
                },
                Duration::from_secs(10),
            )
            .await;
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(*received.lock().unwrap(), vec![3]);
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[test]
    fn all_rpc_vote_owners_must_match_sender() {
        let vote = Vote::new(7, 1);
        let requests = [
            WireRequest::Vote(Box::new(VoteRequest {
                vote,
                last_log_id: None,
            })),
            WireRequest::AppendEntries(Box::new(AppendEntriesRequest {
                vote,
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            })),
            WireRequest::InstallSnapshot {
                vote,
                meta: SnapshotMeta::default(),
                data: vec![],
            },
        ];
        for request in requests {
            assert!(request.sender_matches(1));
            assert!(!request.sender_matches(2));
        }
    }

    #[test]
    fn signed_replies_bind_peer_sequence_and_fresh_nonce() {
        let group = GroupIdentity {
            cluster: [1; 16],
            group: 1,
            incarnation: 1,
        };
        let local = ReplicaIdentity::new(group.clone(), 1, vec![1, 2]).unwrap();
        let remote = ReplicaIdentity::new(group, 2, vec![1, 2]).unwrap();
        let key1 = Arc::new(NodeSigningKey::from_seed(&[1; 32]).unwrap());
        let key2 = NodeSigningKey::from_seed(&[2; 32]).unwrap();
        let registry = Arc::new(
            PeerKeyRegistry::new(BTreeMap::from([
                (1, key1.public_key()),
                (2, key2.public_key()),
            ]))
            .unwrap(),
        );
        let config = TransportConfig::new(local, key1, BTreeMap::new(), registry);
        let response = WireResponse::Vote(Box::new(VoteResponse {
            vote: Vote::new(1, 2),
            vote_granted: true,
            last_log_id: None,
        }));
        let payload = serde_json::to_vec(&WireReply {
            nonce: [7; 32],
            response,
        })
        .unwrap();
        let mut signed = SignedRaftEnvelope::sign(&remote, 1, 4, payload, &key2).unwrap();
        let encoded = signed.encode_json().unwrap();
        assert!(verify_reply(&encoded, &config, 2, 4, [7; 32]).is_ok());
        assert!(verify_reply(&encoded, &config, 2, 4, [8; 32]).is_err());
        assert!(verify_reply(&encoded, &config, 2, 5, [7; 32]).is_err());
        signed.payload[0] ^= 1;
        assert!(verify_reply(&signed.encode_json().unwrap(), &config, 2, 4, [7; 32]).is_err());
        assert!(verify_reply(b"{}", &config, 2, 4, [7; 32]).is_err());
    }

    #[tokio::test]
    async fn captured_request_is_rejected_on_new_connection_after_restart() {
        let group = GroupIdentity {
            cluster: [2; 16],
            group: 1,
            incarnation: 1,
        };
        let client = ReplicaIdentity::new(group.clone(), 1, vec![1, 2]).unwrap();
        let server_id = ReplicaIdentity::new(group, 2, vec![1, 2]).unwrap();
        let key1 = Arc::new(NodeSigningKey::from_seed(&[1; 32]).unwrap());
        let key2 = Arc::new(NodeSigningKey::from_seed(&[2; 32]).unwrap());
        let registry = Arc::new(
            PeerKeyRegistry::new(BTreeMap::from([
                (1, key1.public_key()),
                (2, key2.public_key()),
            ]))
            .unwrap(),
        );
        let config = TransportConfig::new(server_id, key2, BTreeMap::new(), registry);
        let count = Arc::new(AtomicUsize::new(0));
        let handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> = {
            let count = count.clone();
            Arc::new(move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    WireResponse::Vote(Box::new(VoteResponse {
                        vote: Vote::new(1, 2),
                        vote_granted: true,
                        last_log_id: None,
                    }))
                })
            })
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(
            RaftPeerServer::from_listener(listener, config.clone(), handler.clone()).serve(),
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let (kind, challenge) = read_frame(&mut stream).await.unwrap();
        assert_eq!(kind, CHALLENGE);
        let call = WireCall {
            nonce: [3; 32],
            challenge: challenge.try_into().unwrap(),
            request: WireRequest::Vote(Box::new(VoteRequest {
                vote: Vote::new(1, 1),
                last_log_id: None,
            })),
        };
        let captured =
            SignedRaftEnvelope::sign(&client, 2, 1, serde_json::to_vec(&call).unwrap(), &key1)
                .unwrap()
                .encode_json()
                .unwrap();
        write_frame(&mut stream, REQUEST, &captured).await.unwrap();
        assert_eq!(read_frame(&mut stream).await.unwrap().0, RESPONSE);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        task.abort();
        let _ = task.await;
        let listener = TcpListener::bind(addr).await.unwrap();
        let task = tokio::spawn(RaftPeerServer::from_listener(listener, config, handler).serve());
        let mut stream = TcpStream::connect(addr).await.unwrap();
        assert_eq!(read_frame(&mut stream).await.unwrap().0, CHALLENGE);
        write_frame(&mut stream, REQUEST, &captured).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), read_frame(&mut stream))
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        task.abort();
    }
}

/// Wire-level tests for the five load-bearing protocol properties: bounded
/// length prefixes, the payload limit, request deadlines, the connection cap
/// and the challenge/nonce/reply bindings that the adversarial cases below
/// exercise.
#[cfg(test)]
mod protocol_tests {
    use super::*;
    use crate::multiraft::GroupIdentity;
    use std::sync::atomic::AtomicUsize;

    struct Fixture {
        client: ReplicaIdentity,
        server: ReplicaIdentity,
        client_key: Arc<NodeSigningKey>,
        server_key: Arc<NodeSigningKey>,
        registry: Arc<PeerKeyRegistry>,
    }

    fn fixture(tag: u8) -> Fixture {
        let group = GroupIdentity {
            cluster: [tag; 16],
            group: 1,
            incarnation: 1,
        };
        let client = ReplicaIdentity::new(group.clone(), 1, vec![1, 2]).unwrap();
        let server = ReplicaIdentity::new(group, 2, vec![1, 2]).unwrap();
        let mut seed = [0u8; 32];
        seed[0] = 1;
        let client_key = Arc::new(NodeSigningKey::from_seed(&seed).unwrap());
        seed[0] = 2;
        let server_key = Arc::new(NodeSigningKey::from_seed(&seed).unwrap());
        let registry = Arc::new(
            PeerKeyRegistry::new(BTreeMap::from([
                (1, client_key.public_key()),
                (2, server_key.public_key()),
            ]))
            .unwrap(),
        );
        Fixture {
            client,
            server,
            client_key,
            server_key,
            registry,
        }
    }

    fn vote(term: u64, node: u32) -> WireRequest {
        WireRequest::Vote(Box::new(VoteRequest {
            vote: Vote::new(term, node),
            last_log_id: None,
        }))
    }

    fn signed_call(
        f: &Fixture,
        sequence: u64,
        challenge: &[u8; 32],
        nonce: [u8; 32],
        request: WireRequest,
    ) -> Vec<u8> {
        let payload = serde_json::to_vec(&WireCall {
            nonce,
            challenge: *challenge,
            request,
        })
        .unwrap();
        SignedRaftEnvelope::sign(&f.client, f.server.node(), sequence, payload, &f.client_key)
            .unwrap()
            .encode_json()
            .unwrap()
    }

    fn counting_handler(
        count: Arc<AtomicUsize>,
    ) -> Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> {
        Arc::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                WireResponse::Vote(Box::new(VoteResponse {
                    vote: Vote::new(1, 2),
                    vote_granted: true,
                    last_log_id: None,
                }))
            })
        })
    }

    async fn spawn_server(
        f: &Fixture,
        handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync>,
        request_timeout: Duration,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut config = TransportConfig::new(
            f.server.clone(),
            f.server_key.clone(),
            BTreeMap::new(),
            f.registry.clone(),
        );
        config.request_timeout = request_timeout;
        let task = tokio::spawn(RaftPeerServer::from_listener(listener, config, handler).serve());
        (addr, task)
    }

    fn client_config(f: &Fixture, addr: SocketAddr) -> TransportConfig {
        TransportConfig::new(
            f.client.clone(),
            f.client_key.clone(),
            BTreeMap::from([(f.server.node(), addr)]),
            f.registry.clone(),
        )
    }

    async fn read_challenge(stream: &mut TcpStream) -> [u8; 32] {
        let (kind, body) = read_frame(stream).await.unwrap();
        assert_eq!(kind, CHALLENGE, "server must open with a challenge");
        body.try_into().unwrap()
    }

    /// Connects to `addr` from a specific loopback source address, so a test
    /// can present two distinct source addresses to one listener. Every RPC
    /// opens a fresh connection from a fresh ephemeral port, which is why the
    /// server keys fairness by address and not by `SocketAddr`.
    async fn connect_from(source: &str, addr: SocketAddr) -> io::Result<TcpStream> {
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.bind(SocketAddr::new(source.parse().unwrap(), 0))?;
        socket.connect(addr).await
    }

    // ---- 1. Frames: the declared length is bounded before reservation. ----

    #[tokio::test]
    async fn a_declared_length_above_the_limit_is_refused_before_any_reservation() {
        // The semaphore permits are the memory reservation taken immediately
        // before the body buffer is allocated. A refusal that leaves the
        // permits untouched therefore happened before the reservation and, by
        // code order, before `vec![0u8; length]`. This test cannot measure heap
        // bytes; what it pins is the ordering that makes the allocation
        // unreachable, and it uses a length the budget would otherwise grant.
        let chunk_over = SNAPSHOT_CHUNK_FRAME + 1;
        assert!(chunk_over < MAX_IN_FLIGHT_REQUEST_BYTES);
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let mut header = [0u8; 5];
        header[0] = SNAPSHOT_CHUNK;
        header[1..].copy_from_slice(&(chunk_over as u32).to_le_bytes());
        writer.write_all(&header).await.unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            read_budgeted_frame(&mut reader, budget.clone(), None, false),
        )
        .await
        .expect("the limit check must not wait for a body")
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            budget.available_permits(),
            MAX_IN_FLIGHT_REQUEST_BYTES,
            "an over-limit length must not reserve any of the byte budget"
        );

        // A length that cannot be represented as permits at all must still be
        // refused by the frame check, not by a budget error.
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let mut header = [0u8; 5];
        header[0] = REQUEST;
        header[1..].copy_from_slice(&u32::MAX.to_le_bytes());
        writer.write_all(&header).await.unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            read_budgeted_frame(&mut reader, budget.clone(), None, true),
        )
        .await
        .expect("a huge declared length must be refused immediately")
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(budget.available_permits(), MAX_IN_FLIGHT_REQUEST_BYTES);
    }

    #[tokio::test]
    async fn a_single_frame_cannot_reserve_the_whole_in_flight_budget() {
        // `read_budgeted_frame` reserves the declared length before it
        // allocates or reads the body, so a peer that declares the maximum
        // frame and stalls holds the reservation for `request_timeout`. If
        // that reservation can equal the whole budget, one unauthenticated
        // peer blocks every other handler on the shared byte semaphore.
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));

        let (mut writer, mut reader) = tokio::io::duplex(64);
        let mut header = [0u8; 5];
        header[0] = REQUEST;
        header[1..].copy_from_slice(&(MAX_FRAME as u32).to_le_bytes());
        writer.write_all(&header).await.unwrap();
        let first = tokio::spawn({
            let budget = budget.clone();
            async move { read_budgeted_frame(&mut reader, budget, None, true).await }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while budget.available_permits() == MAX_IN_FLIGHT_REQUEST_BYTES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first frame never reserved its bytes");
        assert_eq!(
            budget.available_permits(),
            MAX_IN_FLIGHT_REQUEST_BYTES - MAX_FRAME,
            "one frame must leave the rest of the budget available"
        );

        // A second, small frame from another handler must still be admitted
        // while the first is stalled mid-body.
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer.write_all(&[REQUEST, 4, 0, 0, 0]).await.unwrap();
        writer.write_all(&[1, 2, 3, 4]).await.unwrap();
        let (_, body, permit) = tokio::time::timeout(
            Duration::from_secs(2),
            read_budgeted_frame(&mut reader, budget.clone(), None, true),
        )
        .await
        .expect("a second frame was starved by the first frame's reservation")
        .unwrap();
        assert_eq!(body, [1, 2, 3, 4]);
        drop(permit);

        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(budget.available_permits(), MAX_IN_FLIGHT_REQUEST_BYTES);
    }

    fn peer_frame_budget(shares: &Arc<PeerBytes>, address: &str) -> PeerFrameBudget {
        PeerFrameBudget {
            shares: shares.clone(),
            address: address.parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn one_source_address_cannot_hold_the_whole_in_flight_byte_budget() {
        // Two maximum frames from one address: the first takes that address's
        // whole share, the second must wait rather than take the rest of the
        // global budget. If it could, one reachable peer would again reserve
        // 100% of the budget and starve every other peer.
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let shares = Arc::new(PeerBytes::new(MAX_IN_FLIGHT_REQUEST_BYTES_PER_PEER));
        let first_address = "127.0.0.2";

        let mut stalled = Vec::new();
        let mut writers = Vec::new();
        for _ in 0..2 {
            let (mut writer, mut reader) = tokio::io::duplex(64);
            let mut header = [0u8; 5];
            header[0] = REQUEST;
            header[1..].copy_from_slice(&(MAX_FRAME as u32).to_le_bytes());
            writer.write_all(&header).await.unwrap();
            // Keep the writer open: dropping it would end the stalled body
            // read and release the reservation this test measures.
            writers.push(writer);
            let budget = budget.clone();
            let peer = peer_frame_budget(&shares, first_address);
            stalled.push(tokio::spawn(async move {
                read_budgeted_frame(&mut reader, budget, Some(peer), true).await
            }));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while budget.available_permits() == MAX_IN_FLIGHT_REQUEST_BYTES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first frame never reserved its bytes");
        // Give the second frame ample time to reserve if the sub-cap is gone.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            MAX_IN_FLIGHT_REQUEST_BYTES_PER_PEER,
            MAX_IN_FLIGHT_REQUEST_BYTES / 2,
            "one address must hold at most half the global byte budget"
        );
        assert_eq!(
            budget.available_permits(),
            MAX_IN_FLIGHT_REQUEST_BYTES / 2,
            "one address reserved more than half of the global byte budget"
        );

        // Another address is served while the first is stalled at its share.
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer.write_all(&[REQUEST, 4, 0, 0, 0]).await.unwrap();
        writer.write_all(&[1, 2, 3, 4]).await.unwrap();
        let (_, body, permit) = tokio::time::timeout(
            Duration::from_secs(2),
            read_budgeted_frame(
                &mut reader,
                budget.clone(),
                Some(peer_frame_budget(&shares, "127.0.0.3")),
                true,
            ),
        )
        .await
        .expect("a second address was starved by one address's stalled frames")
        .unwrap();
        assert_eq!(body, [1, 2, 3, 4]);
        drop(permit);

        for task in stalled {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        assert_eq!(budget.available_permits(), MAX_IN_FLIGHT_REQUEST_BYTES);
        assert!(
            shares
                .reserved
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "per-address reservations must be released"
        );
    }

    #[tokio::test]
    async fn one_maximum_frame_from_one_address_is_admitted_without_waiting() {
        // The sub-cap must not throttle the ordinary case: a single maximum
        // frame from one address is exactly that address's share, so it is
        // admitted immediately with nothing else holding bytes.
        let budget = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUEST_BYTES));
        let shares = Arc::new(PeerBytes::new(MAX_IN_FLIGHT_REQUEST_BYTES_PER_PEER));
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let mut header = [0u8; 5];
        header[0] = REQUEST;
        header[1..].copy_from_slice(&(MAX_FRAME as u32).to_le_bytes());
        writer.write_all(&header).await.unwrap();
        let task = tokio::spawn({
            let budget = budget.clone();
            let peer = peer_frame_budget(&shares, "127.0.0.2");
            async move { read_budgeted_frame(&mut reader, budget, Some(peer), true).await }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while budget.available_permits() != MAX_IN_FLIGHT_REQUEST_BYTES - MAX_FRAME {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a maximum frame from one address waited for its own share");
        assert_eq!(
            *shares
                .reserved
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&"127.0.0.2".parse::<IpAddr>().unwrap())
                .expect("the frame must hold its address's share"),
            MAX_FRAME
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(budget.available_permits(), MAX_IN_FLIGHT_REQUEST_BYTES);
    }

    // ---- 2. Payload limit: over-limit fails closed, later connections work. ----

    #[tokio::test]
    async fn an_over_limit_request_frame_fails_closed_and_leaves_later_connections_usable() {
        let f = fixture(11);
        let count = Arc::new(AtomicUsize::new(0));
        let (addr, task) =
            spawn_server(&f, counting_handler(count.clone()), Duration::from_secs(5)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        read_challenge(&mut stream).await;
        let mut header = [0u8; 5];
        header[0] = REQUEST;
        header[1..].copy_from_slice(&((MAX_FRAME + 1) as u32).to_le_bytes());
        stream.write_all(&header).await.unwrap();
        // The protocol carries exactly one request per connection, so there is
        // no later request on this connection to desynchronise. Fail-closed
        // means the server closes instead of reading the declared body.
        let refused = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut stream))
            .await
            .expect("an over-limit frame must not leave the connection hanging");
        assert!(refused.is_err(), "server answered an over-limit frame");
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "over-limit frame was dispatched"
        );

        // A later connection is unaffected: the refusal did not poison the
        // listener or a shared budget.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let challenge = read_challenge(&mut stream).await;
        let body = signed_call(&f, 1, &challenge, [7; 32], vote(1, 1));
        write_frame(&mut stream, REQUEST, &body).await.unwrap();
        assert_eq!(read_frame(&mut stream).await.unwrap().0, RESPONSE);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn one_address_stalling_maximum_frames_does_not_starve_another_peer() {
        // End-to-end proof that the per-address byte sub-cap is wired into the
        // served path, not only into the read helper: one address declares two
        // maximum frames and never sends either body, then a different address
        // must still complete a signed RPC.
        let f = fixture(20);
        let count = Arc::new(AtomicUsize::new(0));
        let (addr, task) =
            spawn_server(&f, counting_handler(count.clone()), Duration::from_secs(10)).await;

        let mut stalled = Vec::new();
        for _ in 0..2 {
            let mut stream = connect_from("127.0.0.2", addr).await.unwrap();
            read_challenge(&mut stream).await;
            let mut header = [0u8; 5];
            header[0] = REQUEST;
            header[1..].copy_from_slice(&(MAX_FRAME as u32).to_le_bytes());
            stream.write_all(&header).await.unwrap();
            stalled.push(stream);
        }
        // Let the server read both headers and reserve: one frame takes the
        // address's share, the second waits for it.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut other = connect_from("127.0.0.3", addr).await.unwrap();
        let (kind, body) = tokio::time::timeout(Duration::from_secs(3), read_frame(&mut other))
            .await
            .expect("a second address was starved by one address's stalled frames")
            .expect("a second address was refused while another address stalled");
        assert_eq!(kind, CHALLENGE);
        let challenge: [u8; 32] = body.try_into().unwrap();
        let body = signed_call(&f, 1, &challenge, [5; 32], vote(1, 1));
        write_frame(&mut other, REQUEST, &body).await.unwrap();
        let (kind, _) = tokio::time::timeout(Duration::from_secs(3), read_frame(&mut other))
            .await
            .expect("a second address's request was starved by one address's stalled frames")
            .expect("a second address's connection failed while another address stalled");
        assert_eq!(kind, RESPONSE);
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "a legitimate peer was not served while another address stalled its frames"
        );

        drop(stalled);
        task.abort();
        let _ = task.await;
    }

    // ---- 3. Request timeouts on both sides. ----

    #[tokio::test]
    async fn the_server_deadline_ends_the_connection_instead_of_reporting_success() {
        let f = fixture(12);
        let handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> =
            Arc::new(|_| Box::pin(std::future::pending()));
        let (addr, task) = spawn_server(&f, handler, Duration::from_millis(300)).await;
        let mut factory = AuthenticatedTcpNetworkFactory::new(client_config(&f, addr));
        let mut client = factory.new_client(2, &BasicNode::new("node-2")).await;

        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.call_for_test(vote(1, 1), Duration::from_secs(5)),
        )
        .await
        .expect("the client must not outlive the server deadline");
        let elapsed = started.elapsed();
        let error = result.expect_err("a timed-out dispatch must never look like a reply");
        assert!(
            elapsed < Duration::from_secs(4),
            "the server deadline, not the client TTL, must have ended the exchange; took {elapsed:?}"
        );
        assert!(
            matches!(error, TransportError::InvalidInput(_)),
            "unexpected error classification: {error:?}"
        );
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn the_client_deadline_is_reported_as_a_timeout_not_a_reply() {
        // The listener accepts and then says nothing, so the client is in the
        // exchange when its own TTL fires.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let silent = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let f = fixture(13);
        let mut factory = AuthenticatedTcpNetworkFactory::new(client_config(&f, addr));
        let mut client = factory.new_client(2, &BasicNode::new("node-2")).await;
        let result = client
            .call_for_test(vote(1, 1), Duration::from_millis(200))
            .await;
        assert_eq!(
            result.unwrap_err(),
            TransportError::InvalidInput("request timeout"),
            "an expired client deadline must be classified as a timeout"
        );
        silent.abort();
        let _ = silent.await;
    }

    // ---- 4. Connection cap: refused above it, no task or fd left behind. ----

    #[tokio::test]
    async fn a_connection_above_the_cap_is_refused_and_the_cap_recovers() {
        let f = fixture(14);
        let count = Arc::new(AtomicUsize::new(0));
        let (addr, task) =
            spawn_server(&f, counting_handler(count.clone()), Duration::from_secs(5)).await;

        // Hold exactly the global cap. Reading each challenge proves the
        // handler was spawned and its connection permit is held (the handler
        // is blocked waiting for a request frame). The per-address sub-cap
        // deliberately stops one address from taking every slot, so fill the
        // cap from as many source addresses as its share requires. That is
        // the situation this test is about: the *global* cap is reached.
        let per_peer = per_peer_connection_cap(f.server.voters().len());
        assert!(per_peer < MAX_CONNECTIONS);
        let mut held = Vec::new();
        let mut source = 2u8;
        while held.len() < MAX_CONNECTIONS {
            let ip = format!("127.0.0.{source}");
            for _ in 0..per_peer.min(MAX_CONNECTIONS - held.len()) {
                let mut stream = connect_from(&ip, addr).await.unwrap();
                read_challenge(&mut stream).await;
                held.push(stream);
            }
            source += 1;
        }
        assert_eq!(held.len(), MAX_CONNECTIONS);

        // The next connection is accepted at the TCP layer and then dropped
        // without a handler. It comes from an address that holds no slot, so
        // only the global cap can be the reason. Reading EOF proves the server
        // closed its end: a leaked descriptor would keep the socket open and
        // this read would time out instead of returning EOF.
        let refused_source = format!("127.0.0.{source}");
        let mut refused = connect_from(&refused_source, addr).await.unwrap();
        match tokio::time::timeout(Duration::from_secs(3), read_frame(&mut refused)).await {
            Ok(Err(error)) => assert_eq!(
                error.kind(),
                io::ErrorKind::UnexpectedEof,
                "a refused connection must be closed, not reset or answered"
            ),
            Ok(Ok(_)) => panic!("the server served a connection above the cap"),
            Err(_) => panic!("the refused connection was not closed: its socket leaked"),
        }
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "a refused connection was dispatched"
        );

        // Releasing the held connections releases their permits. A new
        // connection being served proves no handler task outlived its permit.
        drop(held);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let mut stream = connect_from(&refused_source, addr).await.unwrap();
            match tokio::time::timeout(Duration::from_millis(500), read_frame(&mut stream)).await {
                Ok(Ok((kind, _))) => {
                    assert_eq!(kind, CHALLENGE);
                    break;
                }
                _ if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                other => panic!("the cap did not recover after its holders closed: {other:?}"),
            }
        }
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn one_source_address_cannot_hold_every_handler_slot() {
        let f = fixture(19);
        let count = Arc::new(AtomicUsize::new(0));
        let (addr, task) =
            spawn_server(&f, counting_handler(count.clone()), Duration::from_secs(10)).await;
        let cap = per_peer_connection_cap(f.server.voters().len());

        // One address asks for every slot. Only its share may be served; the
        // rest must be closed rather than held, so another address still has
        // slots to use.
        let mut held = Vec::new();
        for _ in 0..MAX_CONNECTIONS {
            let Ok(mut stream) = connect_from("127.0.0.2", addr).await else {
                continue;
            };
            match tokio::time::timeout(Duration::from_secs(2), read_frame(&mut stream)).await {
                Ok(Ok((kind, _))) => {
                    assert_eq!(kind, CHALLENGE);
                    held.push(stream);
                }
                Ok(Err(_)) => {}
                Err(_) => panic!("a connection was neither served nor closed"),
            }
        }
        assert_eq!(
            held.len(),
            cap,
            "one address held more than its share of the handler slots"
        );
        assert!(
            held.len() < MAX_CONNECTIONS,
            "one source address held {} of {} handler slots",
            held.len(),
            MAX_CONNECTIONS
        );

        // A second address is admitted while the first holds its whole share,
        // and a signed request on it is answered.
        let mut other = connect_from("127.0.0.3", addr).await.unwrap();
        let (kind, body) = tokio::time::timeout(Duration::from_secs(3), read_frame(&mut other))
            .await
            .expect("the second address was starved by the first")
            .expect("the second address was refused while the first held its share");
        assert_eq!(kind, CHALLENGE);
        let challenge: [u8; 32] = body.try_into().unwrap();
        let body = signed_call(&f, 1, &challenge, [4; 32], vote(1, 1));
        write_frame(&mut other, REQUEST, &body).await.unwrap();
        assert_eq!(read_frame(&mut other).await.unwrap().0, RESPONSE);
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "a legitimate peer was not served while another address held its share"
        );

        drop(held);
        task.abort();
        let _ = task.await;
    }

    // ---- 5. Adversarial review of the signature/challenge protocol. ----

    #[tokio::test]
    async fn a_request_bound_to_another_connections_challenge_is_refused() {
        let f = fixture(15);
        let count = Arc::new(AtomicUsize::new(0));
        let (addr, task) =
            spawn_server(&f, counting_handler(count.clone()), Duration::from_secs(5)).await;

        let mut first = TcpStream::connect(addr).await.unwrap();
        let challenge_a = read_challenge(&mut first).await;
        let mut second = TcpStream::connect(addr).await.unwrap();
        let challenge_b = read_challenge(&mut second).await;
        assert_ne!(
            challenge_a, challenge_b,
            "the per-connection challenge must be fresh"
        );

        // A valid signature by the real sender over a request bound to another
        // connection's challenge must not dispatch.
        let body = signed_call(&f, 1, &challenge_a, [3; 32], vote(1, 1));
        write_frame(&mut second, REQUEST, &body).await.unwrap();
        let refused = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut second))
            .await
            .expect("a foreign-challenge request must not hang the connection");
        assert!(refused.is_err(), "a foreign-challenge request was answered");
        assert_eq!(count.load(Ordering::SeqCst), 0);

        // Control: the same request bound to this connection's own challenge
        // is dispatched, so the refusal above was the challenge binding.
        let mut third = TcpStream::connect(addr).await.unwrap();
        let challenge_c = read_challenge(&mut third).await;
        let body = signed_call(&f, 2, &challenge_c, [4; 32], vote(1, 1));
        write_frame(&mut third, REQUEST, &body).await.unwrap();
        assert_eq!(read_frame(&mut third).await.unwrap().0, RESPONSE);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn a_captured_request_is_refused_on_another_live_connection() {
        let f = fixture(16);
        let count = Arc::new(AtomicUsize::new(0));
        let (addr, task) =
            spawn_server(&f, counting_handler(count.clone()), Duration::from_secs(5)).await;

        let mut first = TcpStream::connect(addr).await.unwrap();
        let challenge = read_challenge(&mut first).await;
        let captured = signed_call(&f, 1, &challenge, [5; 32], vote(1, 1));
        write_frame(&mut first, REQUEST, &captured).await.unwrap();
        assert_eq!(read_frame(&mut first).await.unwrap().0, RESPONSE);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Same process, same listener, a fresh connection: the captured bytes
        // are structurally valid but bound to the first connection's challenge.
        let mut second = TcpStream::connect(addr).await.unwrap();
        read_challenge(&mut second).await;
        write_frame(&mut second, REQUEST, &captured).await.unwrap();
        let refused = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut second))
            .await
            .expect("a replayed request must not hang the connection");
        assert!(
            refused.is_err(),
            "a captured request was replayed on a new connection"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1, "the replay was dispatched");
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn a_replayed_frame_after_the_response_is_not_dispatched() {
        // Within one live connection the protocol serves exactly one request.
        // A second copy of the same frame must not be dispatched: the server
        // answers once and closes.
        let f = fixture(17);
        let count = Arc::new(AtomicUsize::new(0));
        let (addr, task) =
            spawn_server(&f, counting_handler(count.clone()), Duration::from_secs(5)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let challenge = read_challenge(&mut stream).await;
        let frame = signed_call(&f, 1, &challenge, [6; 32], vote(1, 1));
        write_frame(&mut stream, REQUEST, &frame).await.unwrap();
        assert_eq!(read_frame(&mut stream).await.unwrap().0, RESPONSE);
        // The server closes after one request, so the replay either fails to
        // write (already closed) or is never answered. Neither may dispatch.
        if write_frame(&mut stream, REQUEST, &frame).await.is_ok() {
            let second = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut stream))
                .await
                .expect("the connection must be closed after one request");
            assert!(
                second.is_err(),
                "a second frame on a live connection was answered"
            );
        }
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "a replayed frame was dispatched"
        );
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn a_tampered_body_with_a_valid_frame_header_is_refused() {
        let f = fixture(18);
        let count = Arc::new(AtomicUsize::new(0));
        let (addr, task) =
            spawn_server(&f, counting_handler(count.clone()), Duration::from_secs(5)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let challenge = read_challenge(&mut stream).await;
        let mut envelope =
            SignedRaftEnvelope::decode_json(&signed_call(&f, 1, &challenge, [8; 32], vote(1, 1)))
                .unwrap();
        // Keep the frame header and the signature; change only the body.
        envelope.payload[0] ^= 1;
        write_frame(&mut stream, REQUEST, &envelope.encode_json().unwrap())
            .await
            .unwrap();
        let refused = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut stream))
            .await
            .expect("a tampered body must not hang the connection");
        assert!(refused.is_err(), "a tampered body was accepted");
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "a tampered body was dispatched"
        );
        task.abort();
        let _ = task.await;
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ReplyMode {
        Correct,
        WrongNonce,
        WrongSequence,
        UntrustedKey,
        TamperedPayload,
    }

    /// Plays the server role far enough to substitute a reply: it sends a
    /// challenge, reads the request, then answers with the requested variant.
    async fn substituted_reply_server(
        listener: TcpListener,
        signer: Arc<NodeSigningKey>,
        server: ReplicaIdentity,
        mode: ReplyMode,
    ) {
        let (mut stream, _) = listener.accept().await.unwrap();
        write_frame(&mut stream, CHALLENGE, &[9u8; 32])
            .await
            .unwrap();
        let (kind, body) = read_frame(&mut stream).await.unwrap();
        assert_eq!(kind, REQUEST);
        let request = SignedRaftEnvelope::decode_json(&body).unwrap();
        let call: WireCall = serde_json::from_slice(&request.payload).unwrap();
        let nonce = match mode {
            ReplyMode::WrongNonce => [0u8; 32],
            _ => call.nonce,
        };
        let sequence = match mode {
            ReplyMode::WrongSequence => request.sequence + 1,
            _ => request.sequence,
        };
        let payload = serde_json::to_vec(&WireReply {
            nonce,
            response: WireResponse::Vote(Box::new(VoteResponse {
                vote: Vote::new(1, 2),
                vote_granted: true,
                last_log_id: None,
            })),
        })
        .unwrap();
        let mut reply =
            SignedRaftEnvelope::sign(&server, request.sender, sequence, payload, &signer).unwrap();
        if mode == ReplyMode::TamperedPayload {
            reply.payload[0] ^= 1;
        }
        write_frame(&mut stream, RESPONSE, &reply.encode_json().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_substituted_response_is_rejected() {
        let f = fixture(19);
        let mut attacker_seed = [0u8; 32];
        attacker_seed[0] = 99;
        let attacker = Arc::new(NodeSigningKey::from_seed(&attacker_seed).unwrap());

        for (mode, expected_ok) in [
            (ReplyMode::Correct, true),
            (ReplyMode::WrongNonce, false),
            (ReplyMode::WrongSequence, false),
            (ReplyMode::UntrustedKey, false),
            (ReplyMode::TamperedPayload, false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let signer = if mode == ReplyMode::UntrustedKey {
                attacker.clone()
            } else {
                f.server_key.clone()
            };
            let server_id = f.server.clone();
            let fake = tokio::spawn(substituted_reply_server(listener, signer, server_id, mode));
            let mut factory = AuthenticatedTcpNetworkFactory::new(client_config(&f, addr));
            let mut client = factory.new_client(2, &BasicNode::new("node-2")).await;
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                client.call_for_test(vote(1, 1), Duration::from_secs(5)),
            )
            .await
            .expect("the client must not hang on a substituted reply");
            assert_eq!(
                result.is_ok(),
                expected_ok,
                "reply mode {mode:?} was not judged as expected: {result:?}"
            );
            fake.await.unwrap();
        }
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;

    /// OpenRaft asks for a heartbeat-derived deadline; the transport may need
    /// longer than that just to complete its handshake. The floor must win
    /// there, and must stay out of the way when no floor is configured.
    #[test]
    fn rpc_deadline_raises_the_heartbeat_derived_ttl_to_the_configured_floor() {
        let heartbeat = Duration::from_millis(250);
        let election = Duration::from_millis(1_500);
        assert_eq!(rpc_deadline(None, heartbeat), heartbeat);
        assert_eq!(rpc_deadline(Some(election), heartbeat), election);
        // A deadline OpenRaft already considers generous is never shortened.
        let snapshot = Duration::from_secs(30);
        assert_eq!(rpc_deadline(Some(election), snapshot), snapshot);
    }
}
