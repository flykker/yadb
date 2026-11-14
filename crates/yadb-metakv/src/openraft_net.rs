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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
use tokio::sync::Semaphore;
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

/// Aggregate encoded request bytes reserved across active server handlers.
/// JSON decoding and response construction require additional memory.
pub const MAX_IN_FLIGHT_REQUEST_BYTES: usize = 32 * 1024 * 1024;

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

fn snapshot_failure(node: u32, group: u64, peer: Option<u32>, phase: &str, reason: &str) {
    let n = SNAPSHOT_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
    if n <= 8 || n.is_power_of_two() {
        eprintln!("openraft snapshot transport node={node} group={group} peer={peer:?} phase={phase} reason={reason} occurrence={n}");
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

        let result = match tokio::time::timeout(ttl, exchange).await {
            Ok(result) => result,
            Err(_) => Err(TransportError::InvalidInput("request timeout")),
        };
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
        let response = self
            .call(WireRequest::AppendEntries(Box::new(rpc)), option.hard_ttl())
            .await
            .map_err(|error| rpc_error(self.target, error, option.hard_ttl()))?;
        match response {
            WireResponse::AppendEntries(response) => Ok(*response),
            _ => Err(rpc_error(
                self.target,
                TransportError::AuthenticationFailed,
                option.hard_ttl(),
            )),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u32>,
        option: RPCOption,
    ) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, openraft::error::RaftError<u32>>> {
        let response = self
            .call(WireRequest::Vote(Box::new(rpc)), option.hard_ttl())
            .await
            .map_err(|error| rpc_error(self.target, error, option.hard_ttl()))?;
        match response {
            WireResponse::Vote(response) => Ok(*response),
            _ => Err(rpc_error(
                self.target,
                TransportError::AuthenticationFailed,
                option.hard_ttl(),
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
        let result = tokio::select! {
            result = self.call(request, option.hard_ttl()) => result,
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

fn transport_io(error: io::Error) -> TransportError {
    TransportError::InvalidInput(match error.kind() {
        io::ErrorKind::TimedOut => "request timeout",
        _ => "transport failure",
    })
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
                continue;
            };
            let config = self.config.clone();
            let handler = self.handler.clone();
            let request_bytes = request_bytes.clone();
            let snapshot_bytes = snapshot_bytes.clone();
            tasks.spawn(async move {
                let _permit = permit;
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
    phase: Arc<AtomicU64>,
    snapshot_peer: Arc<AtomicU64>,
) -> io::Result<()> {
    let mut challenge = [0u8; 32];
    SystemRandom::new()
        .fill(&mut challenge)
        .map_err(|_| io::Error::other("challenge generation failed"))?;
    write_frame(&mut stream, CHALLENGE, &challenge).await?;
    let (kind, body, bytes_permit) =
        match read_request_frame(&mut stream, request_bytes.clone()).await {
            Ok(value) => value,
            Err(_) => return Ok(()),
        };
    if kind == SNAPSHOT_MANIFEST {
        phase.store(1, Ordering::Relaxed);
    }
    let envelope = match SignedRaftEnvelope::decode_json(&body) {
        Ok(envelope) => envelope,
        Err(_) if kind == SNAPSHOT_MANIFEST => {
            return Err(io::Error::other("invalid snapshot manifest envelope"))
        }
        Err(_) => return Ok(()),
    };
    let payload = match envelope.verify(&config.identity, node_id, &config.registry) {
        Ok(payload) => payload,
        Err(_) if kind == SNAPSHOT_MANIFEST => {
            return Err(io::Error::other("invalid snapshot manifest signature"))
        }
        Err(_) => return Ok(()),
    };
    let sender = envelope.sender;
    if kind == SNAPSHOT_MANIFEST {
        snapshot_peer.store(sender as u64, Ordering::Relaxed);
    }
    let sequence = envelope.sequence;
    let (nonce, request, _assembly_permit) = if kind == REQUEST {
        let call: WireCall = match serde_json::from_slice(payload) {
            Ok(call) => call,
            Err(_) => return Ok(()),
        };
        if call.challenge != challenge
            || !call.request.sender_matches(sender)
            || matches!(call.request, WireRequest::InstallSnapshot { .. })
        {
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
            let (_, frame, _frame_permit) =
                read_budgeted_frame(&mut stream, request_bytes.clone(), false).await?;
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
) -> io::Result<(u8, Vec<u8>, tokio::sync::OwnedSemaphorePermit)> {
    read_budgeted_frame(stream, budget, true).await
}

async fn read_budgeted_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    budget: Arc<Semaphore>,
    initial: bool,
) -> io::Result<(u8, Vec<u8>, tokio::sync::OwnedSemaphorePermit)> {
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
    let permit = budget
        .acquire_many_owned(length as u32)
        .await
        .map_err(|_| io::Error::other("request budget closed"))?;
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await?;
    Ok((header[0], body, permit))
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
        let task = tokio::spawn(async move { read_request_frame(&mut reader, budget).await });
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
