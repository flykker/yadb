//! One isolated OpenRaft group per daemon, with a dedicated mTLS listener.
//! Optional nonproduction client operations use this group; no migration exists.
//!
//! Configuration version 1 is JSON: {version, root, mode: "create"|"open",
//! initialize: bool, cluster: [16 bytes], group, incarnation, node, listen,
//! signing_key_der, tls: {roots_der: [paths], chain_der: [paths], key_der},
//! peers: [{id, address, public_key: [32 bytes], server_name, cert_sha256: [32 bytes]}]}.
//! Peers must be sorted by id and include the local node. Initialize is only
//! allowed on create and on exactly one operator-selected replica.

use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Cursor, Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, client::danger::ServerCertVerifier};
use yadb_metakv::{
    multiraft::{GroupIdentity, ReplicaIdentity},
    openraft_net::{
        AuthenticatedTcpNetworkFactory, BoxFuture, RaftPeerServer, TransportConfig, WireRequest,
        WireResponse,
    },
    openraft_runtime::{persistent::PersistentGroup, YadbRaft},
    openraft_tls::MutualTls,
    openraft_transport::{NodeSigningKey, PeerKeyRegistry, PinChange, TransportError},
};

/// Smallest append budget accepted for a new replica root.
///
/// `heartbeat_ms` is not a tuning knob: OpenRaft wraps `append_entries` in
/// `C::timeout(heartbeat_interval, ...)` and does not retry a miss, so a budget
/// shorter than one loopback mTLS handshake abandons appends the follower has
/// already applied.
///
/// The value is anchored on the two measurements this project actually has, not
/// on a derived worst case: at 250 ms a suite run recorded 668 abandoned
/// `AppendEntries` and a leader stuck at `last_log=290 applied=283` while the
/// follower held 290; at 750 ms three consecutive runs recorded none. The floor
/// sits between them — twice the failing value, below the provisioned one — so
/// a new root cannot be created with the configuration that was measured to
/// fail, and the configuration measured to work is still accepted.
///
/// It is deliberately not derived from the 960 ms suite-load tail: that number
/// was measured through the test proxy under load and does not bound a
/// production handshake, and a floor above 750 ms would reject the value known
/// to work. This rejects gross misconfiguration; it is not proof of adequacy.
const MIN_HEARTBEAT_MS: u64 = 500;

/// The message for an append budget below [`MIN_HEARTBEAT_MS`].
///
/// Split out of `prepare` so the wording is asserted by a test rather than
/// captured from stderr.
fn short_append_budget(heartbeat_ms: u64) -> Option<String> {
    (heartbeat_ms < MIN_HEARTBEAT_MS).then(|| {
        format!(
            "heartbeat_ms {heartbeat_ms} is below the {MIN_HEARTBEAT_MS} ms minimum append \
             budget; OpenRaft does not retry a timed-out append, so appends may be abandoned \
             after the follower applied them"
        )
    })
}

const MAX_CONFIG: u64 = 256 * 1024;
const MAX_DER: u64 = 1024 * 1024;
const MAX_PEERS: usize = 32;
const MAX_MARKER: u64 = 256 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u32,
    root: PathBuf,
    mode: Mode,
    #[serde(default)]
    initialize: bool,
    cluster: [u8; 16],
    group: u64,
    incarnation: u64,
    node: u32,
    listen: SocketAddr,
    signing_key_der: PathBuf,
    tls: TlsConfig,
    peers: Vec<Peer>,
    #[serde(default)]
    preferred_leader: Option<u32>,
    #[serde(default)]
    raft_timing: Option<RaftTiming>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RaftTiming {
    heartbeat_ms: u64,
    election_min_ms: u64,
    election_max_ms: u64,
    nonpreferred_election_min_ms: u64,
    #[serde(default)]
    snapshot_after_logs: Option<u64>,
    /// How many already-snapshotted log entries the leader keeps. Absent means
    /// OpenRaft's default of 1000.
    ///
    /// A threshold alone does not make the InstallSnapshot path reachable in a
    /// test: the leader builds a snapshot but still retains 1000 logs below it,
    /// so a follower that is only a few dozen entries behind is caught up by
    /// replay. Keeping 1 entry lets a test trigger a real purge, which is what
    /// forces the install. Test/tuning only; it changes no fence.
    #[serde(default)]
    max_in_snapshot_log_to_keep: Option<u64>,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Create,
    Open,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TlsConfig {
    roots_der: Vec<PathBuf>,
    chain_der: Vec<PathBuf>,
    key_der: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Peer {
    id: u32,
    address: SocketAddr,
    public_key: [u8; 32],
    server_name: String,
    cert_sha256: [u8; 32],
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn read_bounded(path: &Path, max: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(max + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() as u64 > max {
        return Err(invalid("empty or oversized configuration material"));
    }
    Ok(bytes)
}

/// Reads the runtime marker of an existing root.
///
/// The marker is the only thing that makes a directory an OpenRaft root. A
/// pre-OpenRaft directory reaches this branch, and a bare `NotFound` would tell
/// the operator nothing about why the root was refused, so the failure names
/// the guard. This reads; it never creates the marker.
fn read_marker(root: &Path, max: u64) -> io::Result<Vec<u8>> {
    read_bounded(&root.join(".yadb-openraft-runtime.json"), max).map_err(|error| {
        invalid(format!(
            "OpenRaft runtime marker is missing or unreadable in {}: {error}",
            root.display()
        ))
    })
}

pub struct Prepared {
    identity: ReplicaIdentity,
    path: PathBuf,
    mode: Mode,
    initialize: bool,
    listen: SocketAddr,
    transport: TransportConfig,
    members: BTreeMap<u32, openraft::BasicNode>,
    raft_config: Arc<openraft::Config>,
    preferred_leader: Option<u32>,
    #[cfg(test)]
    startup_pause: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}

// A successful result can sit in the channel when its receiver is dropped.
// Guard that handoff too: merely detecting a failed send misses this race.
struct StartupGuard<T: StartupShutdown + Send + 'static>(Option<T>);

trait StartupShutdown {
    fn startup_shutdown(&mut self) -> impl std::future::Future<Output = ()> + Send;
}

impl<T: StartupShutdown + Send + 'static> Drop for StartupGuard<T> {
    fn drop(&mut self) {
        if let Some(mut owner) = self.0.take() {
            tokio::spawn(async move { owner.startup_shutdown().await });
        }
    }
}

async fn supervised_start<T, F>(future: F) -> io::Result<T>
where
    T: StartupShutdown + Send + 'static,
    F: std::future::Future<Output = io::Result<T>> + Send + 'static,
{
    let (send, receive) = oneshot::channel();
    // Do not abort acquisition when the caller disappears: create/open may
    // already have spawned Raft but not yet returned its owner to us.
    tokio::spawn(async move {
        let result = future.await.map(|owner| StartupGuard(Some(owner)));
        let _ = send.send(result);
    });
    let mut guarded = receive.await.map_err(io::Error::other)??;
    Ok(guarded.0.take().unwrap())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MultiConfig {
    version: u32,
    control_group: u64,
    project_groups: BTreeMap<u32, u64>,
    groups: Vec<PathBuf>,
}

pub struct PreparedMulti {
    groups: BTreeMap<u64, Prepared>,
    control: u64,
    projects: BTreeMap<u32, u64>,
}

pub fn prepare_multi(path: &Path) -> io::Result<PreparedMulti> {
    let config: MultiConfig = serde_json::from_slice(&read_bounded(path, MAX_CONFIG)?)
        .map_err(|e| invalid(format!("invalid multi-group config: {e}")))?;
    if config.version != 2
        || !(3..=16).contains(&config.groups.len())
        || config.groups.iter().any(|p| !p.is_absolute())
        || config.project_groups.is_empty()
        || config.project_groups.len() > 4096
        || config.project_groups.contains_key(&0)
    {
        return Err(invalid(
            "version 2 requires 3-16 absolute group config paths and bounded project mapping",
        ));
    }
    let mut groups = BTreeMap::new();
    for path in config.groups {
        let group = prepare(&path)?;
        if groups.insert(group.identity.group().group, group).is_some() {
            return Err(invalid("duplicate group ID"));
        }
    }
    let first = groups.values().next().unwrap();
    let mut addresses = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut transport_addresses = BTreeSet::new();
    for group in groups.values() {
        if group.identity.group().cluster != first.identity.group().cluster
            || group.identity.node() != first.identity.node()
            || group.identity.voters() != first.identity.voters()
            || group.mode != first.mode
            || !addresses.insert(group.listen)
            || !paths.insert(group.path.clone())
        {
            return Err(invalid(
                "groups must share cluster, node, voters and mode, with unique listeners and paths",
            ));
        }
        for address in group.transport.peers.values() {
            if !transport_addresses.insert(*address) {
                return Err(invalid(
                    "transport peer addresses must be unique across groups",
                ));
            }
        }
    }
    let data = config
        .project_groups
        .values()
        .copied()
        .collect::<BTreeSet<_>>();
    if !groups.contains_key(&config.control_group)
        || data.len() < 2
        || data.contains(&config.control_group)
        || data.iter().any(|id| !groups.contains_key(id))
        || groups.len() != data.len() + 1
    {
        return Err(invalid(
            "control and at least two distinct mapped data groups required",
        ));
    }
    Ok(PreparedMulti {
        groups,
        control: config.control_group,
        projects: config.project_groups,
    })
}

impl PreparedMulti {
    fn marker(&self, root: &Path, peers: &BTreeMap<u32, String>) -> io::Result<Vec<u8>> {
        let groups = self
            .groups
            .iter()
            .map(|(id, g)| {
                (
                    *id,
                    g.identity.group(),
                    g.identity.node(),
                    g.identity.voters(),
                    &g.path,
                )
            })
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&(
            2u32,
            "openraft-multiraft",
            root,
            self.control,
            &self.projects,
            groups,
            peers,
        ))
        .map_err(io::Error::other)?;
        if bytes.len() as u64 > MAX_MARKER {
            return Err(invalid("multi-group runtime marker exceeds limit"));
        }
        Ok(bytes)
    }
    pub fn validate_client_root(
        &self,
        root: &Path,
        node: u32,
        peers: &BTreeMap<u32, String>,
        listen: &str,
    ) -> io::Result<()> {
        let first = self.groups.values().next().unwrap();
        if first.identity.node() != node
            || peers.keys().copied().collect::<Vec<_>>() != first.identity.voters()
            || peers.get(&node).and_then(|p| p.parse::<SocketAddr>().ok())
                != listen.parse::<SocketAddr>().ok()
            || peers.values().any(|p| {
                p.parse::<SocketAddr>()
                    .map_or(true, |a| a.port() == 0 || a.ip().is_unspecified())
            })
        {
            return Err(invalid(
                "public peers must exactly match voters and local public listener",
            ));
        }
        let absolute = if root.exists() {
            root.canonicalize()?
        } else {
            root.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .canonicalize()?
                .join(root.file_name().ok_or_else(|| invalid("invalid root"))?)
        };
        for g in self.groups.values() {
            if g.path.starts_with(&absolute)
                || absolute.starts_with(&g.path)
                || g.path.parent() == Some(absolute.as_path())
            {
                return Err(invalid(
                    "metadata roots must be separate from client data root",
                ));
            }
        }
        let marker = self.marker(&absolute, peers)?;
        if first.mode == Mode::Create {
            if root.exists() && (!root.is_dir() || std::fs::read_dir(root)?.next().is_some()) {
                return Err(invalid("OpenRaft create requires new or empty data root"));
            }
        } else if root.join(".yadb-openraft-runtime.pending").exists()
            || read_marker(&absolute, MAX_MARKER)? != marker
        {
            return Err(invalid("multi-group runtime marker mismatch"));
        }
        Ok(())
    }
    pub fn provision_client_root(
        &self,
        root: &Path,
        peers: &BTreeMap<u32, String>,
    ) -> io::Result<yadb_io::OsDirectoryLock> {
        let create = self.groups.values().next().unwrap().mode == Mode::Create;
        if create && !root.exists() {
            std::fs::create_dir(root)?;
            std::fs::File::open(
                root.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new(".")),
            )?
            .sync_all()?;
        }
        let lock = yadb_io::OsDirectoryLock::try_acquire(root)?;
        let expected = self.marker(&root.canonicalize()?, peers)?;
        if create {
            if std::fs::read_dir(root)?.next().is_some() {
                return Err(invalid("data root not empty"));
            }
            let pending = root.join(".yadb-openraft-runtime.pending");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&pending)?;
            file.write_all(&expected)?;
            file.sync_all()?;
            std::fs::hard_link(&pending, root.join(".yadb-openraft-runtime.json"))?;
            std::fs::remove_file(pending)?;
            std::fs::File::open(root)?.sync_all()?;
        } else if root.join(".yadb-openraft-runtime.pending").exists()
            || read_marker(root, MAX_MARKER)? != expected
        {
            return Err(invalid("multi-group runtime marker mismatch"));
        }
        Ok(lock)
    }
    pub async fn start(self) -> io::Result<MultiRunning> {
        supervised_start(self.start_inner()).await
    }

    async fn start_inner(self) -> io::Result<MultiRunning> {
        let mut groups = BTreeMap::new();
        let mut pins = BTreeMap::new();
        for (id, prepared) in self.groups {
            // Taken before `start` consumes the transport. Every group keeps
            // its own registry, so a pin change must be applied to all of them
            // or one group silently keeps trusting a credential the others
            // refuse.
            let registry = prepared.pin_registry();
            match prepared.start().await {
                Ok(group) => {
                    groups.insert(id, group);
                    pins.insert(id, registry);
                }
                Err(error) => {
                    for group in groups.values_mut() {
                        let _ = group.shutdown().await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(MultiRunning {
            groups,
            control: self.control,
            projects: self.projects,
            pins,
        })
    }
}

pub struct MultiRunning {
    groups: BTreeMap<u64, Running>,
    control: u64,
    projects: BTreeMap<u32, u64>,
    pins: BTreeMap<u64, Arc<PeerKeyRegistry>>,
}
impl StartupShutdown for MultiRunning {
    async fn startup_shutdown(&mut self) {
        let _ = self.shutdown().await;
    }
}
pub enum Owner {
    /// Single-group owner. OpenRaft is always started from a multi-group
    /// configuration now, so the daemon never constructs this variant.
    #[allow(dead_code)]
    Single(Running),
    Multi(MultiRunning),
}
impl Owner {
    pub fn status(&self) -> Status {
        match self {
            Self::Single(g) => g.status(false),
            Self::Multi(g) => g.status(),
        }
    }
    pub fn metadata(
        &self,
        peers: BTreeMap<u32, String>,
    ) -> Result<yadb_metakv::LocalMetaKv<yadb_io::OsFs>, yadb_metakv::MetaError> {
        match self {
            Self::Multi(g) => g.metadata(peers),
            Self::Single(_) => Err(yadb_metakv::MetaError::StorageUnavailable),
        }
    }
    pub async fn listener_finished(&mut self) -> String {
        match self {
            Self::Single(g) => g.listener_finished().await,
            Self::Multi(g) => g.listener_finished().await,
        }
    }
    pub async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Self::Single(g) => g.shutdown().await,
            Self::Multi(g) => g.shutdown().await,
        }
    }
    /// Applies a pin change across the whole daemon.
    ///
    /// A single-group daemon has no registry handle retained here (the
    /// transport is already owned by the peer server), so it reports that
    /// runtime rotation is unavailable rather than pretending to have applied
    /// something. The daemon runs Multi-Raft in every configuration this
    /// operator path exists for.
    pub fn pin_change(&self, change: &PinChange) -> Result<(), PinChangeRefused> {
        match self {
            Self::Multi(g) => g.pin_change(change),
            Self::Single(_) => Err(PinChangeRefused {
                group: 0,
                error: TransportError::InvalidInput(
                    "runtime pin changes need a multi-group daemon",
                ),
            }),
        }
    }
    pub fn group_count(&self) -> usize {
        match self {
            Self::Multi(g) => g.group_count(),
            Self::Single(_) => 1,
        }
    }
}

/// A pin change refused by one group, which is why nothing was applied.
#[derive(Debug)]
pub struct PinChangeRefused {
    pub group: u64,
    pub error: TransportError,
}

impl std::fmt::Display for PinChangeRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "group {} refused the certificate pin change: {}",
            self.group, self.error
        )
    }
}

impl std::error::Error for PinChangeRefused {}

impl MultiRunning {
    pub fn metadata(
        &self,
        peers: BTreeMap<u32, String>,
    ) -> Result<yadb_metakv::LocalMetaKv<yadb_io::OsFs>, yadb_metakv::MetaError> {
        let groups = self
            .groups
            .iter()
            .map(|(id, group)| {
                (
                    *id,
                    (
                        group.group.raft().clone(),
                        group.group.state_machine().clone(),
                    ),
                )
            })
            .collect();
        yadb_metakv::LocalMetaKv::from_openraft_groups(
            self.control,
            groups,
            self.projects.clone(),
            peers,
            tokio::runtime::Handle::current(),
        )
    }
    pub fn status(&self) -> Status {
        Status {
            groups: self
                .groups
                .iter()
                .map(|(id, g)| (*id, (g.group.raft().clone(), g.preferred_leader)))
                .collect(),
            client_mode: true,
        }
    }
    /// Applies one certificate-pin change to every group, or to none.
    ///
    /// Each group owns a separate registry, so a change applied group by group
    /// would leave the daemon in a split state when a later group refuses:
    /// some groups would trust the credential and others would not. Every
    /// group is therefore checked first, and the change is applied only when
    /// all of them accept it.
    ///
    /// The check-then-apply window is only sound while pin changes are
    /// serialised, so the caller must hold one lock across the whole call.
    /// Nothing else in this process mutates the registries, so the daemon's
    /// own mutex is sufficient.
    pub fn pin_change(&self, change: &PinChange) -> Result<(), PinChangeRefused> {
        for (id, registry) in &self.pins {
            registry
                .check_pin_change(change)
                .map_err(|error| PinChangeRefused { group: *id, error })?;
        }
        for (id, registry) in &self.pins {
            registry
                .pin_change(change)
                .map_err(|error| PinChangeRefused { group: *id, error })?;
        }
        Ok(())
    }

    /// The number of groups a pin change has to reach. Used by the daemon to
    /// state the blast radius before it applies one.
    pub fn group_count(&self) -> usize {
        self.pins.len()
    }

    pub async fn listener_finished(&mut self) -> String {
        loop {
            for (id, group) in &mut self.groups {
                if group.listener.as_ref().is_some_and(|t| t.is_finished()) {
                    return format!("group {id}: {}", group.listener_finished().await);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    /// Cancelling this future retains all group owners in `self`; retry
    /// shutdown before dropping `self`. Dropping `MultiRunning` itself is not
    /// safe while shutdown remains incomplete.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        let mut error = None;
        for group in self.groups.values_mut() {
            if let Err(e) = group.shutdown().await {
                error = Some(e);
            }
        }
        error.map_or(Ok(()), Err)
    }
}

pub fn prepare(path: &Path) -> io::Result<Prepared> {
    let config: Config = serde_json::from_slice(&read_bounded(path, MAX_CONFIG)?)
        .map_err(|e| invalid(format!("invalid OpenRaft config: {e}")))?;
    if config.version != 1
        || config.peers.is_empty()
        || config.peers.len() > MAX_PEERS
        || config.tls.roots_der.is_empty()
        || config.tls.roots_der.len() > 16
        || config.tls.chain_der.is_empty()
        || config.tls.chain_der.len() > 16
    {
        return Err(invalid(
            "unsupported version or config count outside bounds",
        ));
    }
    if config.initialize && config.mode != Mode::Create {
        return Err(invalid("initialize is only permitted with create"));
    }
    if !config.root.is_dir() {
        return Err(invalid("openraft root must already exist"));
    }
    if config.listen.port() == 0
        || config.peers.iter().any(|p| {
            p.address.port() == 0
                || p.address.ip().is_unspecified()
                || p.server_name.is_empty()
                || p.server_name.len() > 253
        })
        || config.peers.windows(2).any(|p| p[0].id >= p[1].id)
        || config
            .peers
            .iter()
            .map(|p| p.address)
            .collect::<BTreeSet<_>>()
            .len()
            != config.peers.len()
        || config
            .peers
            .iter()
            .map(|p| p.public_key)
            .collect::<BTreeSet<_>>()
            .len()
            != config.peers.len()
    {
        return Err(invalid("invalid or duplicate peer configuration"));
    }
    let identity = ReplicaIdentity::new(
        GroupIdentity {
            cluster: config.cluster,
            group: config.group,
            incarnation: config.incarnation,
        },
        config.node,
        config.peers.iter().map(|p| p.id).collect(),
    )
    .map_err(|e| invalid(format!("invalid replica identity: {e:?}")))?;
    let local = config
        .peers
        .iter()
        .find(|p| p.id == config.node)
        .ok_or_else(|| invalid("local peer missing"))?;
    if local.address.port() != config.listen.port()
        || (!config.listen.ip().is_unspecified() && local.address != config.listen)
    {
        return Err(invalid("local peer address must match listener"));
    }
    let key = Arc::new(
        NodeSigningKey::from_pkcs8(&read_bounded(&config.signing_key_der, MAX_DER)?)
            .map_err(|_| invalid("invalid signing key"))?,
    );
    if key.public_key() != local.public_key {
        return Err(invalid("local signing public key mismatch"));
    }
    let roots = config
        .tls
        .roots_der
        .iter()
        .map(|p| read_bounded(p, MAX_DER).map(CertificateDer::from))
        .collect::<io::Result<Vec<_>>>()?;
    let chain = config
        .tls
        .chain_der
        .iter()
        .map(|p| read_bounded(p, MAX_DER).map(CertificateDer::from))
        .collect::<io::Result<Vec<_>>>()?;
    if MutualTls::fingerprint(chain[0].as_ref()) != local.cert_sha256 {
        return Err(invalid("local TLS certificate pin mismatch"));
    }
    // The TLS constructors validate key syntax and key/cert correspondence,
    // but do not verify our own certificate's trust, name, validity or EKUs.
    let mut trust = rustls::RootCertStore::empty();
    for root in &roots {
        trust
            .add(root.clone())
            .map_err(|e| invalid(format!("invalid TLS root: {e}")))?;
    }
    let trust = Arc::new(trust);
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server_verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        trust.clone(),
        provider.clone(),
    )
    .build()
    .map_err(|e| invalid(e.to_string()))?;
    let client_verifier =
        rustls::server::WebPkiClientVerifier::builder_with_provider(trust, provider)
            .build()
            .map_err(|e| invalid(e.to_string()))?;
    let name = rustls::pki_types::ServerName::try_from(local.server_name.clone())
        .map_err(|e| invalid(e.to_string()))?;
    let now = rustls::pki_types::UnixTime::now();
    server_verifier
        .verify_server_cert(&chain[0], &chain[1..], &name, &[], now)
        .map_err(|e| invalid(format!("local server certificate invalid: {e}")))?;
    client_verifier
        .verify_client_cert(&chain[0], &chain[1..], now)
        .map_err(|e| invalid(format!("local client certificate invalid: {e}")))?;
    let tls_key = PrivateKeyDer::try_from(read_bounded(&config.tls.key_der, MAX_DER)?)
        .map_err(|_| invalid("invalid TLS private key DER"))?;
    let initial_pins: BTreeMap<u32, [u8; 32]> = config
        .peers
        .iter()
        .map(|p| (p.id, p.cert_sha256))
        .collect();
    let tls = MutualTls::new(
        roots,
        chain,
        tls_key,
        config
            .peers
            .iter()
            .map(|p| (p.id, p.server_name.clone()))
            .collect(),
        initial_pins.clone(),
    )?;
    let cluster = config
        .cluster
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let path = config.root.canonicalize()?.join(format!(
        "{cluster}-{}-{}-{}",
        config.group, config.incarnation, config.node
    ));
    if config.mode == Mode::Create && path.exists() {
        return Err(invalid(format!(
            "replica {} already exists; create cannot safely retry: a replica may have started. Preserve the data; use open mode only if all replicas are complete, or restore the entire cluster from a consistent backup",
            path.display()
        )));
    }
    if config.mode == Mode::Open && !path.is_dir() {
        return Err(invalid(
            "replica path conflicts with explicit create/open mode",
        ));
    }
    // The config's pins are the initial set; the replica's durable pin state is
    // merged over them here, before the transport can bind any pin. A state
    // file that cannot be read or merged refuses startup rather than letting
    // the config resurrect a credential the operator retired or revoked.
    let registry = PeerKeyRegistry::open_for_replica(
        config
            .peers
            .iter()
            .map(|p| (p.id, p.public_key.to_vec()))
            .collect(),
        &initial_pins,
        &path,
    )
    .map_err(|error| invalid(error.to_string()))?;
    let members = config
        .peers
        .iter()
        .map(|p| (p.id, openraft::BasicNode::new(p.address.to_string())))
        .collect();
    let transport = TransportConfig::new(
        identity.clone(),
        key,
        config.peers.iter().map(|p| (p.id, p.address)).collect(),
        Arc::new(registry),
    )
    .with_tls(Arc::new(tls));
    if config
        .preferred_leader
        .is_some_and(|id| !identity.voters().contains(&id))
    {
        return Err(invalid("preferred leader must be a voter"));
    }
    if config.preferred_leader.is_some() != config.raft_timing.is_some() {
        return Err(invalid(
            "preferred_leader and raft_timing must be configured together",
        ));
    }
    let mut raft_config = openraft::Config::default();
    if let Some(timing) = &config.raft_timing {
        if !(10..=10_000).contains(&timing.heartbeat_ms)
            || timing.election_min_ms < timing.heartbeat_ms.saturating_mul(3)
            || timing.election_min_ms >= timing.nonpreferred_election_min_ms
            || timing.nonpreferred_election_min_ms >= timing.election_max_ms
            || timing.election_max_ms > 120_000
        {
            return Err(invalid("invalid raft timing bounds"));
        }
        // The bounds above constrain the heartbeat from *above* only
        // (`election_min_ms >= 3 x heartbeat_ms`), so a config can still be too
        // short to survive one handshake. A new root is refused; an existing
        // root is opened with a warning, because a documented live deployment
        // may already carry a short value and refusing to start it would turn a
        // tuning mistake into an outage.
        if let Some(message) = short_append_budget(timing.heartbeat_ms) {
            match config.mode {
                Mode::Create => return Err(invalid(message)),
                Mode::Open => eprintln!("openraft: {message}"),
            }
        }
        raft_config.heartbeat_interval = timing.heartbeat_ms;
        raft_config.election_timeout_min = if config.preferred_leader == Some(config.node) {
            timing.election_min_ms
        } else {
            timing.nonpreferred_election_min_ms
        };
        // OpenRaft derives the leader lease from election_timeout_max. This
        // maximum MUST match on all replicas, including the preferred one.
        raft_config.election_timeout_max = timing.election_max_ms;
        // Test/tuning knob only: the default (5000 logs since the last
        // snapshot) is unchanged when the field is absent, so an existing root
        // behaves exactly as before. Lowering the threshold does not weaken any
        // fence; snapshots are a log-compaction optimisation, and the
        // InstallSnapshot path is the same one the default policy eventually
        // exercises. It exists so a test deployment can trigger that path
        // without writing thousands of entries.
        if let Some(threshold) = timing.snapshot_after_logs {
            if threshold == 0 || threshold > 1_000_000 {
                return Err(invalid("invalid snapshot_after_logs bound"));
            }
            raft_config.snapshot_policy = openraft::SnapshotPolicy::LogsSinceLast(threshold);
        }
        if let Some(keep) = timing.max_in_snapshot_log_to_keep {
            if keep == 0 || keep > 1_000_000 {
                return Err(invalid("invalid max_in_snapshot_log_to_keep bound"));
            }
            raft_config.max_in_snapshot_log_to_keep = keep;
        }
    }
    let raft_config = Arc::new(raft_config.validate().map_err(|e| invalid(e.to_string()))?);
    // Every Raft RPC opens a fresh connection and pays a pinned mTLS handshake.
    // `rpc_ttl_floor` raises this transport's own deadline, but it cannot raise
    // OpenRaft's: 0.9.25 wraps `append_entries` in
    // `C::timeout(heartbeat_interval, ...)` (replication/mod.rs) and reports a
    // miss as `RPCError::Timeout`, which it does **not** retry — a timed-out
    // heartbeat resets no inflight state, so nothing is retried until the next
    // event. The floor is therefore only effective for the RPC types OpenRaft
    // does not wrap; an append is bounded by `heartbeat_ms`. That budget is
    // guarded by `MIN_HEARTBEAT_MS` for a new root and warned about for an
    // existing one; the relation `election_min_ms >= 3 x heartbeat_ms` bounds
    // it from above, not from below.
    let mut transport = transport;
    transport.rpc_ttl_floor = Some(Duration::from_millis(raft_config.election_timeout_min));
    Ok(Prepared {
        identity,
        path,
        mode: config.mode,
        initialize: config.initialize,
        listen: config.listen,
        transport,
        members,
        raft_config,
        preferred_leader: config.preferred_leader,
        #[cfg(test)]
        startup_pause: None,
    })
}

#[derive(Clone)]
pub struct Status {
    groups: BTreeMap<u64, (YadbRaft, Option<u32>)>,
    client_mode: bool,
}
impl Status {
    pub fn snapshot(&self) -> serde_json::Value {
        let groups = self.groups.iter().map(|(id, (raft, preferred))| { let m = raft.metrics().borrow().clone();
            // `last_applied` alone cannot say where a stalled write went. If
            // `last_log_index` is ahead of it, this node holds entries it has
            // not applied; if they match, nothing is pending here and the stall
            // is upstream. That is a direction, not an attribution: the endpoint
            // does not know which index a given client write received, so it
            // cannot prove that write is the one waiting. `replication` (leader
            // only) names the log id each peer has acknowledged, which is the
            // shape a lost or timed-out append leaves behind. Reporting all
            // three turns "write outcome unknown" into a narrower question.
            let replication = m.replication.as_ref().map(|r| r.iter().map(|(peer, matched)| (*peer, format!("{matched:?}"))).collect::<BTreeMap<u32, String>>());
            serde_json::json!({"group": id, "node_id": m.id, "state": format!("{:?}", m.state), "term": m.current_term, "leader_id": m.current_leader, "preferred_leader": preferred, "preference_met": preferred.zip(m.current_leader).map(|(want, actual)| want == actual), "last_log_index": m.last_log_index, "last_applied": format!("{:?}", m.last_applied), "replication": replication, "millis_since_quorum_ack": m.millis_since_quorum_ack, "running_state": format!("{:?}", m.running_state)}) }).collect::<Vec<_>>();
        serde_json::json!({"enabled": true, "client_traffic": if self.client_mode { "multiraft" } else { "single-group" }, "readiness": crate::readiness(), "limitation": "dedicated listener per group; reads are local-applied", "local_observations": groups})
    }
}

pub struct Running {
    group: PersistentGroup,
    listener: Option<JoinHandle<io::Result<()>>>,
    stop: Option<oneshot::Sender<()>>,
    preferred_leader: Option<u32>,
}

impl StartupShutdown for Running {
    async fn startup_shutdown(&mut self) {
        let _ = self.shutdown().await;
    }
}

impl Prepared {
    /// The certificate-pin registry this group's transport will enforce.
    ///
    /// The daemon needs this handle to install, retire and revoke pins at
    /// runtime. It must be taken before `start`, because starting moves the
    /// transport into the peer server.
    pub fn pin_registry(&self) -> Arc<PeerKeyRegistry> {
        self.transport.registry.clone()
    }

    /// Checks the separate client-data root without writing anything. The
    /// versioned marker is a mode guard, not an authorization to migrate.
    #[cfg(test)]
    pub fn validate_client_root(
        &self,
        root: &Path,
        node: u32,
        peers: &BTreeMap<u32, String>,
        listen: &str,
    ) -> io::Result<()> {
        if self.identity.node() != node
            || peers.keys().copied().collect::<Vec<_>>() != self.identity.voters()
        {
            return Err(invalid(
                "public peer map and node must match the exact OpenRaft voters",
            ));
        }
        let public: SocketAddr = listen
            .parse()
            .map_err(|_| invalid("public listener must be a socket address"))?;
        if peers.get(&node).and_then(|p| p.parse::<SocketAddr>().ok()) != Some(public)
            || peers.values().any(|p| {
                p.parse::<SocketAddr>()
                    .map_or(true, |a| a.port() == 0 || a.ip().is_unspecified())
            })
        {
            return Err(invalid(
                "public peer addresses must be concrete and local address must match --listen",
            ));
        }
        let canonical_root = if root.exists() {
            root.canonicalize()?
        } else {
            let parent = root
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            parent.canonicalize()?.join(
                root.file_name()
                    .ok_or_else(|| invalid("invalid data root"))?,
            )
        };
        if self.path.starts_with(&canonical_root)
            || canonical_root.starts_with(&self.path)
            || self.path.parent() == Some(canonical_root.as_path())
        {
            return Err(invalid(
                "replica root must be separate from client data root",
            ));
        }
        match self.mode {
            Mode::Create => {
                if root.exists() && (!root.is_dir() || std::fs::read_dir(root)?.next().is_some()) {
                    return Err(invalid("OpenRaft create requires a new or empty data root"));
                }
            }
            Mode::Open => {
                if root.join(".yadb-openraft-runtime.pending").exists() {
                    return Err(invalid("incomplete OpenRaft root provisioning"));
                }
                let actual = read_marker(&canonical_root, 16 * 1024)?;
                if actual != self.marker_bytes(&canonical_root)? {
                    return Err(invalid("OpenRaft runtime marker mismatch"));
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn marker_bytes(&self, root: &Path) -> io::Result<Vec<u8>> {
        serde_json::to_vec(&(
            1u32,
            "openraft",
            self.identity.group(),
            self.identity.node(),
            self.identity.voters(),
            &self.path,
            root,
        ))
        .map_err(io::Error::other)
    }

    #[cfg(test)]
    pub fn provision_client_root(&self, root: &Path) -> io::Result<yadb_io::OsDirectoryLock> {
        if self.mode == Mode::Open {
            let lock = yadb_io::OsDirectoryLock::try_acquire(root)?;
            if root.join(".yadb-openraft-runtime.pending").exists()
                || read_marker(&root.canonicalize()?, 16 * 1024)?
                    != self.marker_bytes(&root.canonicalize()?)?
            {
                return Err(invalid("OpenRaft runtime marker mismatch"));
            }
            return Ok(lock);
        }
        if !root.exists() {
            std::fs::create_dir(root)?;
            let parent = root
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            std::fs::File::open(parent)?.sync_all()?;
        }
        let lock = yadb_io::OsDirectoryLock::try_acquire(root)?;
        if std::fs::read_dir(root)?.next().is_some() {
            return Err(invalid("OpenRaft data root is not empty"));
        }
        // A leftover temporary marker fails closed in either mode.
        let temporary = root.join(".yadb-openraft-runtime.pending");
        let marker = root.join(".yadb-openraft-runtime.json");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&self.marker_bytes(&root.canonicalize()?)?)?;
        file.sync_all()?;
        std::fs::hard_link(&temporary, &marker)?;
        std::fs::remove_file(&temporary)?;
        std::fs::File::open(root)?.sync_all()?;
        Ok(lock)
    }

    pub async fn start(self) -> io::Result<Running> {
        supervised_start(self.start_inner()).await
    }

    async fn start_inner(self) -> io::Result<Running> {
        // Bind first, so an address conflict cannot create a partial replica.
        let listener = TcpListener::bind(self.listen).await?;
        let network = AuthenticatedTcpNetworkFactory::new(self.transport.clone());
        let group = match self.mode {
            Mode::Create => {
                PersistentGroup::create(&self.path, &self.identity, self.raft_config, network).await
            }
            Mode::Open => {
                PersistentGroup::open(&self.path, &self.identity, self.raft_config, network).await
            }
        }
        .map_err(|e| io::Error::other(e.to_string()))?;
        // A torn log tail or a rebuilt state image is silent by design, so the
        // daemon must be the one that says it happened. Without this line an
        // operator cannot tell a clean start from a recovered one.
        let recovery = group.recovery();
        if !recovery.is_clean() {
            if let Some(tail) = recovery.repaired_tail {
                eprintln!(
                    "group {} node {}: truncated a torn log tail at byte {}, discarded {} byte(s)",
                    self.identity.group().group,
                    self.identity.node(),
                    tail.valid_len,
                    tail.discarded,
                );
            }
            if let Some(tail) = recovery.repaired_delta {
                eprintln!(
                    "group {} node {}: truncated a torn state-machine delta at byte {}, discarded {} byte(s)",
                    self.identity.group().group,
                    self.identity.node(),
                    tail.valid_len,
                    tail.discarded,
                );
            }
            if let Some(image) = &recovery.rebuilt_image {
                eprintln!(
                    "group {} node {}: rebuilt a corrupt state image ({} byte(s), preserved as {})",
                    self.identity.group().group,
                    self.identity.node(),
                    image.corrupt_bytes,
                    image.preserved_as.display(),
                );
                if image.dropped_images > 0 {
                    eprintln!(
                        "group {} node {}: dropped {} older corrupt image(s) ({} byte(s)) to keep the evidence spool bounded",
                        self.identity.group().group,
                        self.identity.node(),
                        image.dropped_images,
                        image.dropped_bytes,
                    );
                }
            }
        }
        #[cfg(test)]
        if let Some((ready, resume)) = self.startup_pause {
            let _ = ready.send(());
            let _ = resume.await;
        }
        if self.initialize {
            if let Err(e) = group.raft().initialize(self.members).await {
                let error = io::Error::other(format!("bootstrap failed: {e}"));
                group.raft().shutdown().await.map_err(io::Error::other)?;
                return Err(error);
            }
        }
        let raft = group.raft().clone();
        let handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> =
            Arc::new(move |request| {
                let raft = raft.clone();
                Box::pin(async move {
                    // On fatal Raft errors panic only this connection task: the wire
                    // protocol has no error variant and must not forge a success.
                    match request {
                        WireRequest::AppendEntries(rpc) => WireResponse::AppendEntries(Box::new(
                            raft.append_entries(*rpc)
                                .await
                                .expect("OpenRaft append failed"),
                        )),
                        WireRequest::Vote(rpc) => WireResponse::Vote(Box::new(
                            raft.vote(*rpc).await.expect("OpenRaft vote failed"),
                        )),
                        WireRequest::InstallSnapshot { vote, meta, data } => {
                            WireResponse::InstallSnapshot(Box::new(
                                raft.install_full_snapshot(
                                    vote,
                                    openraft::Snapshot {
                                        meta,
                                        snapshot: Box::new(Cursor::new(data)),
                                    },
                                )
                                .await
                                .expect("OpenRaft snapshot failed"),
                            ))
                        }
                    }
                })
            });
        let server = RaftPeerServer::from_listener(listener, self.transport, handler);
        let (stop, stopped) = oneshot::channel();
        let listener = tokio::spawn(server.serve_until(async {
            let _ = stopped.await;
        }));
        Ok(Running {
            group,
            listener: Some(listener),
            stop: Some(stop),
            preferred_leader: self.preferred_leader,
        })
    }
}

impl Running {
    pub fn status(&self, client_mode: bool) -> Status {
        Status {
            groups: BTreeMap::from([(
                self.group.identity().group().group,
                (self.group.raft().clone(), self.preferred_leader),
            )]),
            client_mode,
        }
    }
    pub async fn listener_finished(&mut self) -> String {
        let result = self
            .listener
            .as_mut()
            .expect("listener already joined")
            .await;
        self.listener.take();
        format!("{result:?}")
    }
    /// Cancelling this future retains the owner and listener in `self`; retry
    /// shutdown before dropping `self`. Dropping `Running` itself is not safe
    /// while shutdown remains incomplete.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let listener_result = match self.listener.as_mut() {
            Some(listener) => listener.await.map_err(io::Error::other).and_then(|r| r),
            None => Ok(()),
        };
        self.listener.take();
        self.group
            .raft()
            .shutdown()
            .await
            .map_err(io::Error::other)?;
        listener_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };
    use serde_json::{json, Value};
    use std::future::Future;

    fn pin_registry(nodes: &[u32]) -> Arc<PeerKeyRegistry> {
        Arc::new(
            PeerKeyRegistry::new(
                nodes
                    .iter()
                    .map(|id| (*id, vec![*id as u8; 32]))
                    .collect(),
            )
            .unwrap(),
        )
    }

    /// A daemon serves one registry per group. A rotation that one group
    /// refuses must not reach the others, or the daemon would end up trusting
    /// a credential on some groups and refusing it on others.
    #[test]
    fn a_pin_change_refused_by_one_group_is_applied_to_none() {
        let first = pin_registry(&[1, 2]);
        let second = pin_registry(&[1, 2]);
        let old = [0xaa; 32];
        let new = [0xbb; 32];
        first.install_pin(1, old).unwrap();
        second.install_pin(1, old).unwrap();
        first.install_pin(1, new).unwrap();
        // `second` never received the replacement, so retiring `old` there
        // would leave node 1 with no accepted pin.

        let running = MultiRunning {
            groups: BTreeMap::new(),
            control: 1,
            projects: BTreeMap::new(),
            pins: BTreeMap::from([(1u64, first.clone()), (2u64, second.clone())]),
        };
        assert_eq!(running.group_count(), 2);
        let refused = running
            .pin_change(&PinChange::Retire { node: 1, pin: old })
            .unwrap_err();
        assert_eq!(refused.group, 2, "{refused}");
        assert!(
            refused.to_string().contains("last active pin"),
            "{refused}"
        );

        // Nothing was applied anywhere: `old` is still active in the first
        // group, so retiring it there now succeeds and leaves `new`.
        first
            .retire_pin(1, old)
            .expect("a refused change must not have retired the pin anywhere");
        assert!(
            first.retire_pin(1, new).is_err(),
            "the second group's refusal must not have changed the first"
        );
    }

    #[test]
    fn a_pin_change_accepted_by_every_group_reaches_every_group() {
        let first = pin_registry(&[1, 2]);
        let second = pin_registry(&[1, 2]);
        let running = MultiRunning {
            groups: BTreeMap::new(),
            control: 1,
            projects: BTreeMap::new(),
            pins: BTreeMap::from([(1u64, first.clone()), (2u64, second.clone())]),
        };
        running
            .pin_change(&PinChange::Install {
                node: 1,
                pin: [0xcc; 32],
            })
            .unwrap();
        // Both groups now accept the pin: retiring it is refused in both only
        // because it is the last one, which proves the install landed.
        for registry in [&first, &second] {
            assert!(registry.retire_pin(1, [0xcc; 32]).is_err());
        }
    }

    #[test]
    fn a_single_group_daemon_refuses_runtime_pin_changes() {
        // `Running` owns a started group and cannot be built without starting
        // one, so the branch is asserted through the error text instead.
        let refused = PinChangeRefused {
            group: 0,
            error: TransportError::InvalidInput(
                "runtime pin changes need a multi-group daemon",
            ),
        };
        assert!(refused.to_string().contains("multi-group daemon"));
    }

    /// The replica directory `prepare` computes for a config, so a test can
    /// place the durable pin state where startup will look for it.
    fn replica_dir(config: &Value) -> PathBuf {
        let root = Path::new(config["root"].as_str().unwrap())
            .canonicalize()
            .unwrap();
        let cluster = config["cluster"]
            .as_array()
            .unwrap()
            .iter()
            .map(|byte| format!("{:02x}", byte.as_u64().unwrap()))
            .collect::<String>();
        root.join(format!(
            "{cluster}-{}-{}-{}",
            config["group"].as_u64().unwrap(),
            config["incarnation"].as_u64().unwrap(),
            config["node"].as_u64().unwrap()
        ))
    }

    /// A `[u8; 32]` as the config serialises it.
    fn fixed_bytes(value: &Value) -> [u8; 32] {
        let mut out = [0u8; 32];
        for (slot, byte) in out.iter_mut().zip(value.as_array().unwrap()) {
            *slot = byte.as_u64().unwrap() as u8;
        }
        out
    }

    /// Startup merges the replica's stored pin state over the config, so a pin
    /// installed before the restart is still accepted after it.
    #[test]
    fn an_installed_pin_survives_a_daemon_restart() {
        let (dir, path, mut config) = fixture();
        let replica = replica_dir(&config);
        std::fs::create_dir(&replica).unwrap();
        let peer = &config["peers"][0];
        let keys = BTreeMap::from([(1u32, fixed_bytes(&peer["public_key"]).to_vec())]);
        let initial = BTreeMap::from([(1u32, fixed_bytes(&peer["cert_sha256"]))]);
        let registry = PeerKeyRegistry::open_for_replica(keys, &initial, &replica).unwrap();
        let replacement = [0x5a; 32];
        registry.install_pin(1, replacement).unwrap();
        drop(registry);

        config["mode"] = json!("open");
        config["initialize"] = json!(false);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let prepared = prepare(&path).unwrap();
        // Retiring the installed pin is accepted here; a registry rebuilt from
        // the config alone would refuse it as absent.
        prepared
            .pin_registry()
            .check_pin_change(&PinChange::Retire {
                node: 1,
                pin: replacement,
            })
            .expect("the installed pin must still be active after the restart");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A damaged state file must refuse startup, not silently revert to the
    /// config's pins.
    #[test]
    fn a_corrupt_certificate_pin_state_refuses_startup() {
        let (dir, path, mut config) = fixture();
        let replica = replica_dir(&config);
        std::fs::create_dir(&replica).unwrap();
        std::fs::write(replica.join("certificate-pins.json"), b"{\"version\":1,").unwrap();

        config["mode"] = json!("open");
        config["initialize"] = json!(false);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let error = prepare(&path)
            .err()
            .expect("a corrupt certificate pin state must refuse startup");
        let text = error.to_string();
        assert!(text.contains("certificate pin state"), "{text}");
        assert!(text.contains("is corrupt"), "{text}");
        assert!(text.contains("certificate-pins.json"), "{text}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn fixture() -> (PathBuf, PathBuf, Value) {
        let mut random = [0u8; 16];
        ring::rand::SecureRandom::fill(&SystemRandom::new(), &mut random).unwrap();
        let suffix = random
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>();
        let dir = std::env::temp_dir().join(format!("yadb-openraft-daemon-{suffix}"));
        std::fs::create_dir(&dir).unwrap();
        let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(key.as_ref()).unwrap();
        let signing_key = dir.join("signing.der");
        std::fs::write(&signing_key, key.as_ref()).unwrap();
        let tls_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../yadb-metakv/testdata/tls");
        let cert = std::fs::read(tls_dir.join("node-1.test.cert.der")).unwrap();
        let address = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let config = json!({
            "version": 1, "root": dir, "mode": "create", "initialize": true,
            "cluster": [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1], "group": 1, "incarnation": 1,
            "node": 1, "listen": address, "signing_key_der": signing_key,
            "tls": {"roots_der": [tls_dir.join("ca.der")], "chain_der": [tls_dir.join("node-1.test.cert.der")], "key_der": tls_dir.join("node-1.test.key.der")},
            "peers": [{"id": 1, "address": address, "public_key": pair.public_key().as_ref(), "server_name": "node-1.test", "cert_sha256": MutualTls::fingerprint(&cert)}]
        });
        let path = dir.join("config.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        (dir, path, config)
    }

    /// Everything a refused open must not change: the set of paths, each
    /// entry's type, permissions and modification time, and every file's
    /// bytes. Comparing mtimes catches an in-place rewrite that keeps the same
    /// length; comparing the path set catches a created or removed lock,
    /// marker or pending file.
    #[derive(Debug, PartialEq, Eq)]
    struct TreeEntry {
        kind: &'static str,
        mode: u32,
        modified: Option<std::time::SystemTime>,
        bytes: Vec<u8>,
    }

    fn tree_snapshot(root: &Path) -> BTreeMap<PathBuf, TreeEntry> {
        fn walk(root: &Path, path: &Path, out: &mut BTreeMap<PathBuf, TreeEntry>) {
            use std::os::unix::fs::PermissionsExt;
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
        let mut out = BTreeMap::new();
        walk(root, root, &mut out);
        out
    }

    /// A synthetic pre-OpenRaft directory: a legacy metadata tree with a WAL,
    /// descriptors and an empty subdirectory. Nothing here is an OpenRaft
    /// marker; a create must not adopt it and a refusal must not touch it.
    fn legacy_tree(root: &Path) {
        std::fs::create_dir_all(root.join("metakv")).unwrap();
        std::fs::write(
            root.join("metakv").join("state.json"),
            br#"{"revision":7,"projects":{"alpha":1}}"#,
        )
        .unwrap();
        std::fs::write(root.join("metakv").join("leases"), vec![0x7f; 64]).unwrap();
        std::fs::create_dir_all(root.join("group")).unwrap();
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
        std::fs::write(root.join("raft.wal"), b"YADBWAL1").unwrap();
        std::fs::create_dir(root.join("empty-subdir")).unwrap();
        std::fs::write(root.join("zero-byte"), b"").unwrap();
        std::fs::write(root.join(".legacy-marker"), b"legacy-runtime-v1").unwrap();
    }

    /// Three singleton group configs plus a version-2 manifest, the shape the
    /// daemon requires. No listener is ever bound here: the guards under test
    /// run before `start`.
    fn multi_fixture() -> (PathBuf, PathBuf, Vec<PathBuf>, String) {
        let (dir, _, template) = fixture();
        let mut paths = Vec::new();
        let mut sockets = Vec::new();
        for id in 1..=3u64 {
            let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = socket.local_addr().unwrap();
            sockets.push(socket);
            let mut cfg = template.clone();
            cfg["group"] = json!(id);
            cfg["listen"] = json!(addr);
            cfg["peers"][0]["address"] = json!(addr);
            let path = dir.join(format!("group-{id}.json"));
            std::fs::write(&path, serde_json::to_vec(&cfg).unwrap()).unwrap();
            paths.push(path);
        }
        drop(sockets);
        let manifest = dir.join("multi.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(
                &json!({"version":2,"control_group":1,"project_groups":{"1":2,"2":3},"groups":paths}),
            )
            .unwrap(),
        )
        .unwrap();
        (dir, manifest, paths, "127.0.0.1:8420".to_string())
    }

    async fn reopen_after_cancel(path: &Path) {
        let mut config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        config["mode"] = json!("open");
        config["initialize"] = json!(false);
        std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(mut running) = prepare(path).unwrap().start().await {
                    running.shutdown().await.unwrap();
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("cancelled startup leaked a listener or replica directory lock");
    }

    #[tokio::test]
    async fn cancelled_start_releases_acquired_replica() {
        let (dir, path, _) = fixture();
        let mut prepared = prepare(&path).unwrap();
        let (ready, reached) = oneshot::channel();
        let (resume, wait) = oneshot::channel();
        prepared.startup_pause = Some((ready, wait));
        let mut start = Box::pin(prepared.start());
        tokio::select! {
            _ = &mut start => panic!("startup did not pause"),
            result = reached => result.unwrap(),
        }
        drop(start);
        resume.send(()).unwrap();
        reopen_after_cancel(&path).await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn cancelled_multi_start_releases_first_and_second_replicas() {
        let (dir, _, template) = fixture();
        let mut paths = Vec::new();
        let mut sockets = Vec::new();
        for id in 1..=3 {
            let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = socket.local_addr().unwrap();
            sockets.push(socket);
            let mut config = template.clone();
            config["group"] = json!(id);
            config["listen"] = json!(addr);
            config["peers"][0]["address"] = json!(addr);
            let path = dir.join(format!("group-{id}.json"));
            std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
            paths.push(path);
        }
        drop(sockets);
        let manifest = dir.join("multi.json");
        std::fs::write(&manifest, serde_json::to_vec(&json!({"version":2,"control_group":1,"project_groups":{"1":2,"2":3},"groups":paths})).unwrap()).unwrap();
        let mut prepared = prepare_multi(&manifest).unwrap();
        let (ready, reached) = oneshot::channel();
        let (resume, wait) = oneshot::channel();
        prepared.groups.get_mut(&2).unwrap().startup_pause = Some((ready, wait));
        let mut start = Box::pin(prepared.start());
        tokio::select! {
            _ = &mut start => panic!("multi startup did not pause at group 2"),
            result = reached => result.unwrap(),
        }
        drop(start);
        resume.send(()).unwrap();
        for path in &paths {
            reopen_after_cancel(path).await;
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn create_status_shutdown_and_reopen() {
        let (dir, path, mut config) = fixture();
        let mut running = prepare(&path).unwrap().start().await.unwrap();
        let status = running.status(false).snapshot();
        assert_eq!(status["readiness"]["ready"], true);
        assert_eq!(status["readiness"]["limits"].as_array().unwrap().len(), 3);
        assert_eq!(status["client_traffic"], "single-group");
        running.shutdown().await.unwrap();
        drop(running);
        config["mode"] = json!("open");
        config["initialize"] = json!(false);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let mut reopened = prepare(&path).unwrap().start().await.unwrap();
        reopened.shutdown().await.unwrap();
        drop(reopened);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn cancelled_shutdown_retains_owner_and_listener_until_retry() {
        let (dir, path, mut config) = fixture();
        let mut running = prepare(&path).unwrap().start().await.unwrap();
        // Replace the listener task with a deterministically blocked one after
        // first draining the real server. This exercises cancellation at the
        // exact join boundary without relying on scheduler timing.
        running.stop.take().unwrap().send(()).unwrap();
        running.listener.as_mut().unwrap().await.unwrap().unwrap();
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        running.listener = Some(tokio::spawn(async move {
            let _ = wait.await;
            Ok(())
        }));
        let mut shutdown = Box::pin(running.shutdown());
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                shutdown.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        drop(shutdown);
        assert!(running.listener.is_some());
        config["mode"] = json!("open");
        config["initialize"] = json!(false);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(prepare(&path).unwrap().start().await.is_err());
        release.send(()).unwrap();
        running.shutdown().await.unwrap();
        drop(running);
        let mut reopened = prepare(&path).unwrap().start().await.unwrap();
        reopened.shutdown().await.unwrap();
        drop(reopened);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn multigroup_cancelled_shutdown_retains_all_owners_until_retry() {
        let (dir, _, template) = fixture();
        let mut paths = Vec::new();
        let mut sockets = Vec::new();
        for id in 1..=3 {
            let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = socket.local_addr().unwrap();
            sockets.push(socket);
            let mut config = template.clone();
            config["group"] = json!(id);
            config["listen"] = json!(addr);
            config["peers"][0]["address"] = json!(addr);
            let path = dir.join(format!("group-{id}.json"));
            std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
            paths.push(path);
        }
        drop(sockets);
        let manifest = dir.join("multi.json");
        std::fs::write(&manifest, serde_json::to_vec(&json!({"version":2,"control_group":1,"project_groups":{"1":2,"2":3},"groups":paths})).unwrap()).unwrap();
        let mut running = prepare_multi(&manifest).unwrap().start().await.unwrap();
        // Drain every real listener so a failed reopen demonstrates the root
        // owner, not a port conflict. Block the middle group after group 1.
        for group in running.groups.values_mut() {
            group.stop.take().unwrap().send(()).unwrap();
            group.listener.as_mut().unwrap().await.unwrap().unwrap();
            group.listener.take();
        }
        let (release, wait) = oneshot::channel::<()>();
        running.groups.get_mut(&2).unwrap().listener = Some(tokio::spawn(async move {
            let _ = wait.await;
            Ok(())
        }));
        let mut shutdown = Box::pin(running.shutdown());
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                shutdown.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        drop(shutdown);
        assert!(running.groups[&1].listener.is_none());
        assert!(running.groups[&2].listener.is_some());
        for path in &paths {
            let mut config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            config["mode"] = json!("open");
            config["initialize"] = json!(false);
            std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
            assert!(
                prepare(path).unwrap().start().await.is_err(),
                "root reopened while multi shutdown was incomplete: {path:?}"
            );
        }
        release.send(()).unwrap();
        running.shutdown().await.unwrap();
        drop(running);
        let mut reopened = prepare_multi(&manifest).unwrap().start().await.unwrap();
        reopened.shutdown().await.unwrap();
        drop(reopened);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn invalid_config_is_rejected_before_replica_creation() {
        let (dir, path, config) = fixture();
        for change in 0..10 {
            let mut bad = config.clone();
            match change {
                0 => bad["unknown"] = json!(true),
                1 => bad["version"] = json!(2),
                2 => bad["peers"][0]["public_key"] = json!(vec![0; 32]),
                3 => bad["peers"][0]["cert_sha256"] = json!(vec![0; 32]),
                4 => bad["tls"]["roots_der"] = json!([]),
                5 => bad["mode"] = json!("open"),
                6 => bad["node"] = json!(0),
                7 => bad["peers"][0]["server_name"] = json!("wrong.example"),
                8 | 9 => {
                    let tls_dir =
                        Path::new(env!("CARGO_MANIFEST_DIR")).join("../yadb-metakv/testdata/tls");
                    let cert_path = tls_dir.join(if change == 8 {
                        "untrusted.test.cert.der"
                    } else {
                        "expired.test.cert.der"
                    });
                    let cert = std::fs::read(&cert_path).unwrap();
                    bad["tls"]["chain_der"] = json!([cert_path]);
                    bad["peers"][0]["cert_sha256"] = json!(MutualTls::fingerprint(&cert));
                }
                _ => unreachable!(),
            }
            std::fs::write(&path, serde_json::to_vec(&bad).unwrap()).unwrap();
            let error = match prepare(&path) {
                Ok(_) => panic!("invalid case {change}"),
                Err(error) => error.to_string(),
            };
            if change >= 7 {
                assert!(
                    error.contains("local server certificate invalid"),
                    "case {change}: {error}"
                );
            }
            if change == 9 {
                assert!(error.to_lowercase().contains("expired"), "{error}");
            }
            assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 2);
        }
        std::fs::write(&path, vec![b' '; MAX_CONFIG as usize + 1]).unwrap();
        assert!(prepare(&path).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn create_refusal_explains_existing_replica_recovery_without_changing_it() {
        let (dir, config, _) = fixture();
        let replica = dir.join(format!("{}-1-1-1", "01".repeat(16)));
        std::fs::create_dir(&replica).unwrap();
        std::fs::write(replica.join("replica.json"), b"existing replica").unwrap();
        let before = tree_snapshot(&replica);
        let error = match prepare(&config) {
            Ok(_) => panic!("create accepted an existing replica"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("Preserve the data"), "{error}");
        assert!(error.contains("open mode"), "{error}");
        assert!(error.contains("consistent backup"), "{error}");
        assert_eq!(tree_snapshot(&replica), before);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn preference_and_timing_are_opt_in_and_validated() {
        let (dir, path, mut config) = fixture();
        let original = prepare(&path).unwrap();
        assert_eq!(
            original.raft_config.heartbeat_interval,
            openraft::Config::default().heartbeat_interval
        );
        config["preferred_leader"] = json!(1);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(prepare(&path).is_err());
        config["raft_timing"] = json!({"heartbeat_ms":750,"election_min_ms":2250,"nonpreferred_election_min_ms":3000,"election_max_ms":3500});
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let preferred = prepare(&path).unwrap();
        assert_eq!(preferred.raft_config.election_timeout_min, 2250);
        assert_eq!(preferred.raft_config.election_timeout_max, 3500);
        config["preferred_leader"] = json!(2);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(prepare(&path).is_err());
        config["preferred_leader"] = json!(1);
        config["raft_timing"]["election_max_ms"] = json!(2600);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(prepare(&path).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn snapshot_policy_defaults_to_five_thousand_logs() {
        let (dir, path, mut config) = fixture();
        config["preferred_leader"] = json!(1);
        config["raft_timing"] = json!({"heartbeat_ms":750,"election_min_ms":2250,"nonpreferred_election_min_ms":3000,"election_max_ms":3500});
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let prepared = prepare(&path).unwrap();
        assert_eq!(
            prepared.raft_config.snapshot_policy,
            openraft::SnapshotPolicy::LogsSinceLast(5000)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_configured_snapshot_threshold_replaces_the_default() {
        let (dir, path, mut config) = fixture();
        config["preferred_leader"] = json!(1);
        config["raft_timing"] = json!({"heartbeat_ms":750,"election_min_ms":2250,"nonpreferred_election_min_ms":3000,"election_max_ms":3500,"snapshot_after_logs":32});
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let prepared = prepare(&path).unwrap();
        assert_eq!(
            prepared.raft_config.snapshot_policy,
            openraft::SnapshotPolicy::LogsSinceLast(32)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A threshold alone does not make an install reachable: the leader still
    /// retains its default 1000 snapshotted logs, so a follower a few dozen
    /// entries behind is caught up by replay. The retention knob is what lets a
    /// test force a purge, and it must be separately configurable and bounded.
    #[test]
    fn the_snapshot_log_retention_is_configurable_and_bounded() {
        let (dir, path, mut config) = fixture();
        config["preferred_leader"] = json!(1);
        config["raft_timing"] = json!({"heartbeat_ms":750,"election_min_ms":2250,"nonpreferred_election_min_ms":3000,"election_max_ms":3500});
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        assert_eq!(prepare(&path).unwrap().raft_config.max_in_snapshot_log_to_keep, 1000);

        config["raft_timing"] = json!({"heartbeat_ms":750,"election_min_ms":2250,"nonpreferred_election_min_ms":3000,"election_max_ms":3500,"max_in_snapshot_log_to_keep":1});
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        assert_eq!(prepare(&path).unwrap().raft_config.max_in_snapshot_log_to_keep, 1);

        for keep in [0, 1_000_001] {
            config["raft_timing"] = json!({"heartbeat_ms":750,"election_min_ms":2250,"nonpreferred_election_min_ms":3000,"election_max_ms":3500,"max_in_snapshot_log_to_keep":keep});
            std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
            assert!(
                prepare(&path).is_err(),
                "max_in_snapshot_log_to_keep {keep} must be rejected"
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_zero_or_oversized_snapshot_threshold_is_rejected() {
        let (dir, path, mut config) = fixture();
        config["preferred_leader"] = json!(1);
        for threshold in [0, 1_000_001] {
            config["raft_timing"] = json!({"heartbeat_ms":750,"election_min_ms":2250,"nonpreferred_election_min_ms":3000,"election_max_ms":3500,"snapshot_after_logs":threshold});
            std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
            assert!(
                prepare(&path).is_err(),
                "snapshot_after_logs {threshold} must be rejected"
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_short_append_budget_is_refused_for_a_new_root_and_allowed_for_an_existing_one() {
        let (dir, path, mut config) = fixture();
        // `election_min_ms >= 3 x heartbeat_ms` holds, so the bounds check
        // passes and only the append-budget floor can reject this.
        assert_eq!(short_append_budget(750), None);
        assert_eq!(short_append_budget(MIN_HEARTBEAT_MS), None);
        let message = short_append_budget(250).expect("250 ms is below the floor");
        assert!(
            message.contains("500 ms minimum append budget"),
            "the refusal must name the floor, got: {message}"
        );
        config["preferred_leader"] = json!(1);
        config["raft_timing"] = json!({"heartbeat_ms":250,"election_min_ms":1500,"nonpreferred_election_min_ms":2750,"election_max_ms":3000});
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let error = match prepare(&path) {
            Ok(_) => panic!("a create with a 250 ms append budget must be refused"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("minimum append budget"),
            "the create refusal must explain the budget, got: {error}"
        );

        // The same config on an existing root opens: a live deployment that
        // already carries the short value is not turned into an outage. The
        // warning goes to stderr and is covered by `short_append_budget`.
        // `root` is the fixture's directory itself, and the replica path is
        // `<root>/<cluster hex>-<group>-<incarnation>-<node>`.
        std::fs::create_dir(dir.join(format!("{}-1-1-1", "01".repeat(16)))).unwrap();
        config["mode"] = json!("open");
        config["initialize"] = json!(false);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let opened = prepare(&path).unwrap();
        assert_eq!(opened.raft_config.heartbeat_interval, 250);

        // At the floor the config is accepted in create mode.
        std::fs::remove_dir(dir.join(format!("{}-1-1-1", "01".repeat(16)))).unwrap();
        config["mode"] = json!("create");
        config["initialize"] = json!(true);
        config["raft_timing"] = json!({"heartbeat_ms":500,"election_min_ms":1500,"nonpreferred_election_min_ms":2000,"election_max_ms":2500});
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        assert_eq!(prepare(&path).unwrap().raft_config.heartbeat_interval, 500);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn client_root_rejects_existing_data_and_marker_mismatch() {
        let (dir, path, _) = fixture();
        let prepared = prepare(&path).unwrap();
        let root = dir.join("data");
        std::fs::create_dir(&root).unwrap();
        let listen = prepared.members[&1].addr.clone();
        let peers = BTreeMap::from([(1, listen.clone())]);
        std::fs::write(root.join("old-data"), b"untouched").unwrap();
        assert!(prepared
            .validate_client_root(&root, 1, &peers, &listen)
            .is_err());
        assert_eq!(std::fs::read(root.join("old-data")).unwrap(), b"untouched");
        std::fs::remove_file(root.join("old-data")).unwrap();
        prepared
            .validate_client_root(&root, 1, &peers, &listen)
            .unwrap();
        prepared.provision_client_root(&root).unwrap();
        assert!(prepared
            .validate_client_root(&root, 1, &peers, &listen)
            .is_err());
        assert!(prepared
            .validate_client_root(&root, 2, &peers, &listen)
            .is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Admission for creation is "the root is missing or empty", and every
    /// refusal must leave the root exactly as it was. A root containing only an
    /// *empty subdirectory* is the case a naive
    /// `read_dir(root)?.next().is_none()` gets wrong, so it is checked
    /// explicitly alongside a stray file and a half-written marker.
    #[test]
    fn create_admits_only_a_missing_or_empty_root() {
        let (dir, path, mut config) = fixture();
        let prepared = prepare(&path).unwrap();
        let listen = prepared.members[&1].addr.clone();
        let peers = BTreeMap::from([(1, listen.clone())]);
        let root = dir.join("data");
        let cluster = "01".repeat(16);

        // Missing: validation admits it without writing; provisioning creates
        // the directory and binds exactly one marker.
        assert!(!root.exists());
        prepared
            .validate_client_root(&root, 1, &peers, &listen)
            .unwrap();
        assert!(!root.exists(), "validation must not create the root");
        drop(prepared.provision_client_root(&root).unwrap());
        let created = tree_snapshot(&root);
        assert_eq!(created.len(), 2, "root and marker only: {created:?}");
        assert!(created.contains_key(Path::new(".yadb-openraft-runtime.json")));
        assert!(created.contains_key(Path::new("")));

        // An existing OpenRaft root is reopened, not recreated, and reopening
        // changes nothing. An open requires the replica path to exist, which
        // creation itself would have made.
        std::fs::create_dir(dir.join(format!("{cluster}-1-1-1"))).unwrap();
        config["mode"] = json!("open");
        config["initialize"] = json!(false);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let reopened = prepare(&path).unwrap();
        reopened
            .validate_client_root(&root, 1, &peers, &listen)
            .unwrap();
        drop(reopened.provision_client_root(&root).unwrap());
        assert_eq!(tree_snapshot(&root), created);

        // Creating again against the marked root is refused, not re-initialized.
        // A new incarnation keeps the replica path absent, so the refusal below
        // is the root guard and not the create/open path check.
        config["mode"] = json!("create");
        config["initialize"] = json!(true);
        config["incarnation"] = json!(2);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let recreate = prepare(&path).unwrap();
        let error = recreate
            .validate_client_root(&root, 1, &peers, &listen)
            .expect_err("a marked OpenRaft root must not be recreated");
        assert!(error.to_string().contains("new or empty"), "{error}");
        assert_eq!(tree_snapshot(&root), created);

        // Anything else is refused, and the refusal is non-destructive.
        for (case, build, message) in [
            (
                "empty-subdir",
                (|root: &Path| std::fs::create_dir(root.join("sub")).unwrap()) as fn(&Path),
                "empty",
            ),
            (
                "stray-file",
                (|root: &Path| std::fs::write(root.join("stray"), b"keep").unwrap()) as fn(&Path),
                "empty",
            ),
            (
                "partial-descriptor",
                (|root: &Path| {
                    std::fs::write(
                        root.join(".yadb-openraft-runtime.pending"),
                        b"{\"version\":",
                    )
                    .unwrap()
                }) as fn(&Path),
                "empty",
            ),
        ] {
            let case_root = dir.join(case);
            std::fs::create_dir(&case_root).unwrap();
            build(&case_root);
            let before = tree_snapshot(&case_root);
            let error = prepared
                .validate_client_root(&case_root, 1, &peers, &listen)
                .expect_err(case);
            assert!(error.to_string().contains(message), "{case}: {error}");
            let error = match prepared.provision_client_root(&case_root) {
                Ok(_) => panic!("{case}: provisioning accepted a non-empty root"),
                Err(error) => error,
            };
            assert!(error.to_string().contains(message), "{case}: {error}");
            assert_eq!(tree_snapshot(&case_root), before, "{case} was modified");
            assert!(
                !case_root.join(".yadb-openraft-runtime.json").exists(),
                "{case}: a refused create wrote the marker"
            );
        }

        // A root that is a file rather than a directory is refused too.
        let file_root = dir.join("not-a-directory");
        std::fs::write(&file_root, b"x").unwrap();
        let error = prepared
            .validate_client_root(&file_root, 1, &peers, &listen)
            .expect_err("a file root");
        assert!(error.to_string().contains("new or empty"), "{error}");

        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The marker binds the replica identity, so a reopen that changes the
    /// group, incarnation, node or voter set must be refused even when the
    /// replica path exists and every other config check passes.
    #[test]
    fn a_changed_group_incarnation_node_or_voter_set_is_refused_on_reopen() {
        let (dir, path, config) = fixture();
        let prepared = prepare(&path).unwrap();
        let root = dir.join("data");
        std::fs::create_dir(&root).unwrap();
        let listen = prepared.members[&1].addr.clone();
        let peers = BTreeMap::from([(1, listen.clone())]);
        drop(prepared.provision_client_root(&root).unwrap());
        let marker = std::fs::read(root.join(".yadb-openraft-runtime.json")).unwrap();
        let cluster = "01".repeat(16);
        let tls_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../yadb-metakv/testdata/tls");

        // Node 2's material, so a voters or node mutation is a config `prepare`
        // accepts rather than one it refuses for an unrelated reason.
        let node2_key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let node2_pair = Ed25519KeyPair::from_pkcs8(node2_key.as_ref()).unwrap();
        let node2_signing = dir.join("signing-2.der");
        std::fs::write(&node2_signing, node2_key.as_ref()).unwrap();
        let node2_cert = std::fs::read(tls_dir.join("node-2.test.cert.der")).unwrap();
        let node2_socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let node2_addr = node2_socket.local_addr().unwrap();
        drop(node2_socket);
        let node2 = json!({"id":2,"address":node2_addr,"public_key":node2_pair.public_key().as_ref(),
            "server_name":"node-2.test","cert_sha256":MutualTls::fingerprint(&node2_cert)});
        let both = BTreeMap::from([(1, listen.clone()), (2, node2_addr.to_string())]);

        let opened = || {
            let mut cfg = config.clone();
            cfg["mode"] = json!("open");
            cfg["initialize"] = json!(false);
            cfg
        };
        // `prepare` refuses a changed replica path before the marker is read,
        // so each case creates the path it would compute and then proves the
        // refusal came from the marker instead.
        let refuse = |cfg: Value, peers: &BTreeMap<u32, String>, listen: &str, case: &str| {
            std::fs::write(&path, serde_json::to_vec(&cfg).unwrap()).unwrap();
            let prepared = prepare(&path).unwrap_or_else(|error| {
                panic!("{case}: prepare refused before the marker check: {error}")
            });
            let node = cfg["node"].as_u64().unwrap() as u32;
            let error = prepared
                .validate_client_root(&root, node, peers, listen)
                .expect_err(case);
            assert!(
                error.to_string().contains("marker mismatch"),
                "{case}: {error}"
            );
        };

        std::fs::create_dir(dir.join(format!("{cluster}-2-1-1"))).unwrap();
        let mut group = opened();
        group["group"] = json!(2);
        refuse(group, &peers, &listen, "group");

        std::fs::create_dir(dir.join(format!("{cluster}-1-2-1"))).unwrap();
        let mut incarnation = opened();
        incarnation["incarnation"] = json!(2);
        refuse(incarnation, &peers, &listen, "incarnation");

        std::fs::create_dir(dir.join(format!("{}-1-1-1", "02".repeat(16)))).unwrap();
        let mut changed_cluster = opened();
        changed_cluster["cluster"] = json!(vec![2u8; 16]);
        refuse(changed_cluster, &peers, &listen, "cluster");

        let mut voters = opened();
        voters["peers"].as_array_mut().unwrap().push(node2.clone());
        // The unchanged replica path must exist for an open to reach the marker.
        std::fs::create_dir(dir.join(format!("{cluster}-1-1-1"))).unwrap();
        refuse(voters, &both, &listen, "voters");

        std::fs::create_dir(dir.join(format!("{cluster}-1-1-2"))).unwrap();
        let mut node = opened();
        node["node"] = json!(2);
        node["listen"] = json!(node2_addr);
        node["signing_key_der"] = json!(node2_signing);
        node["tls"]["chain_der"] = json!([tls_dir.join("node-2.test.cert.der")]);
        node["tls"]["key_der"] = json!(tls_dir.join("node-2.test.key.der"));
        node["peers"].as_array_mut().unwrap().push(node2);
        refuse(node, &both, &node2_addr.to_string(), "node");

        assert_eq!(
            std::fs::read(root.join(".yadb-openraft-runtime.json")).unwrap(),
            marker,
            "a refused reopen must not rewrite the marker"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A pre-OpenRaft directory passed as the client-data root must be refused
    /// by creation (it is not empty) and by reopen (it carries no marker), and
    /// the refusal must leave every path, byte, mode and mtime untouched: no
    /// import, no rewrite, no lock file, no marker.
    #[test]
    fn a_legacy_directory_is_refused_and_left_byte_identical() {
        let (dir, path, mut config) = fixture();
        let prepared = prepare(&path).unwrap();
        let root = dir.join("legacy-data");
        legacy_tree(&root);
        let before = tree_snapshot(&root);
        let listen = prepared.members[&1].addr.clone();
        let peers = BTreeMap::from([(1, listen.clone())]);

        let error = prepared
            .validate_client_root(&root, 1, &peers, &listen)
            .expect_err("a legacy directory must be refused for creation");
        assert!(error.to_string().contains("new or empty"), "{error}");
        let error = match prepared.provision_client_root(&root) {
            Ok(_) => panic!("provisioning adopted a legacy directory"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("not empty"), "{error}");
        assert_eq!(
            tree_snapshot(&root),
            before,
            "the create refusal was destructive"
        );

        // An open needs the replica path, which creation itself would have made.
        std::fs::create_dir(dir.join(format!("{}-1-1-1", "01".repeat(16)))).unwrap();
        config["mode"] = json!("open");
        config["initialize"] = json!(false);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let reopened = prepare(&path).unwrap();
        let error = reopened
            .validate_client_root(&root, 1, &peers, &listen)
            .expect_err("a legacy directory must be refused on reopen");
        assert!(error.to_string().contains("marker"), "{error}");
        let error = match reopened.provision_client_root(&root) {
            Ok(_) => panic!("provisioning opened a legacy directory"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("marker"), "{error}");
        assert_eq!(
            tree_snapshot(&root),
            before,
            "the open refusal was destructive"
        );

        for name in [
            ".yadb-openraft-runtime.json",
            ".yadb-openraft-runtime.pending",
        ] {
            assert!(!root.join(name).exists(), "{name} was created");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The same guard on the multi-group path the daemon actually runs.
    #[test]
    fn a_legacy_directory_is_refused_by_the_multi_group_root_guard() {
        let (dir, manifest, paths, public) = multi_fixture();
        let prepared = prepare_multi(&manifest).unwrap();
        let peers = BTreeMap::from([(1, public.clone())]);
        let root = dir.join("client-data");
        legacy_tree(&root);
        let before = tree_snapshot(&root);

        let error = prepared
            .validate_client_root(&root, 1, &peers, &public)
            .expect_err("a legacy directory must be refused for creation");
        assert!(error.to_string().contains("new or empty"), "{error}");
        let error = match prepared.provision_client_root(&root, &peers) {
            Ok(_) => panic!("multi-group provisioning adopted a legacy directory"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("not empty"), "{error}");
        assert_eq!(tree_snapshot(&root), before);

        // An open needs each replica path, which creation itself would have made.
        let cluster = "01".repeat(16);
        for group in 1..=3 {
            std::fs::create_dir(dir.join(format!("{cluster}-{group}-1-1"))).unwrap();
        }
        for path in &paths {
            let mut cfg: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            cfg["mode"] = json!("open");
            cfg["initialize"] = json!(false);
            std::fs::write(path, serde_json::to_vec(&cfg).unwrap()).unwrap();
        }
        let reopened = prepare_multi(&manifest).unwrap();
        let error = reopened
            .validate_client_root(&root, 1, &peers, &public)
            .expect_err("a legacy directory must be refused on reopen");
        assert!(error.to_string().contains("marker"), "{error}");
        let error = match reopened.provision_client_root(&root, &peers) {
            Ok(_) => panic!("multi-group provisioning opened a legacy directory"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("marker"), "{error}");
        assert_eq!(tree_snapshot(&root), before);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multigroup_create_write_reopen_and_marker_binding() {
        let (dir, _, template) = fixture();
        let mut paths = Vec::new();
        let mut used = Vec::new();
        for id in 1..=3 {
            let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = socket.local_addr().unwrap();
            used.push(socket);
            let mut cfg = template.clone();
            cfg["group"] = json!(id);
            cfg["listen"] = json!(addr);
            cfg["peers"][0]["address"] = json!(addr);
            let path = dir.join(format!("group-{id}.json"));
            std::fs::write(&path, serde_json::to_vec(&cfg).unwrap()).unwrap();
            paths.push(path);
        }
        drop(used);
        let manifest = dir.join("multi.json");
        let mut config =
            json!({"version":2,"control_group":1,"project_groups":{"1":2,"2":3},"groups":paths});
        std::fs::write(&manifest, serde_json::to_vec(&config).unwrap()).unwrap();
        let root = dir.join("client-data");
        let public = "127.0.0.1:8420";
        let peers = BTreeMap::from([(1, public.to_string())]);
        let prepared = prepare_multi(&manifest).unwrap();
        prepared
            .validate_client_root(&root, 1, &peers, public)
            .unwrap();
        let owner = prepared.provision_client_root(&root, &peers).unwrap();
        let mut running = prepared.start().await.unwrap();
        let meta = running.metadata(peers.clone()).unwrap();
        // Waiting for leadership must tolerate the states a starting group
        // actually passes through: `role_info_for_key` can fail outright while the
        // group is still coming up, and that used to panic *inside* the wait loop,
        // turning "not ready yet" into a confusing unwrap failure. Record the last
        // observation and assert it, so a slow start fails here with the state that
        // was seen rather than three statements later.
        let mut last = String::from("no observation");
        let mut both_leading = false;
        for _ in 0..100 {
            last = match (
                meta.role_info_for_key("/db/1/"),
                meta.role_info_for_key("/db/2/"),
            ) {
                (Ok((_, one, ..)), Ok((_, two, ..))) => {
                    both_leading = one == yadb_metakv::RoleState::Leader
                        && two == yadb_metakv::RoleState::Leader;
                    format!("/db/1/={one:?} /db/2/={two:?}")
                }
                (one, two) => format!("role info unavailable: /db/1/={one:?} /db/2/={two:?}"),
            };
            if both_leading {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            both_leading,
            "both data groups must elect a leader before the write; last observation: {last}"
        );
        for key in ["/db/1/value", "/db/2/value"] {
            let m = meta.clone();
            let key = key.to_string();
            tokio::task::spawn_blocking(move || {
                m.execute(yadb_metakv::KvCommand::Put {
                    key,
                    value: b"ok".to_vec(),
                })
            })
            .await
            .unwrap()
            .unwrap();
        }
        assert!(!root.join("metakv").exists());
        assert_eq!(running.status().snapshot()["readiness"]["ready"], true);
        drop(meta);
        running.shutdown().await.unwrap();
        drop(running);
        drop(owner);
        for path in config["groups"].as_array().unwrap() {
            let path = PathBuf::from(path.as_str().unwrap());
            let mut group: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            group["mode"] = json!("open");
            group["initialize"] = json!(false);
            std::fs::write(path, serde_json::to_vec(&group).unwrap()).unwrap();
        }
        let prepared = prepare_multi(&manifest).unwrap();
        prepared
            .validate_client_root(&root, 1, &peers, public)
            .unwrap();
        let owner = prepared.provision_client_root(&root, &peers).unwrap();
        let mut running = prepared.start().await.unwrap();
        let meta = running.metadata(peers.clone()).unwrap();
        // A reopened group is readable only once it has applied its log and
        // answered a quorum read. `LocalMetaKv::get` *panics* when that snapshot is
        // unavailable, which is what turned a slow reopen into an unwrap failure
        // with no cause; use `try_snapshot` so the reason is available and the
        // wait can be bounded. The assertion still fails on a lost value. One key
        // at a time: a snapshot spanning two groups is refused by design.
        let keys = ["/db/1/value", "/db/2/value"];
        let mut last = String::from("no read attempted");
        let mut values: Vec<Vec<u8>> = Vec::new();
        for _ in 0..100 {
            let mut read = Vec::new();
            let mut failure = None;
            for key in keys {
                match meta.try_snapshot(&[key.to_string()], &[]) {
                    Ok(snapshot) => match snapshot.get(key) {
                        Some(kv) => read.push(kv.value),
                        None => failure = Some(format!("{key} is absent")),
                    },
                    Err(error) => failure = Some(format!("{key}: {error}")),
                }
            }
            match failure {
                Some(reason) => last = reason,
                None => {
                    values = read;
                    last.clear();
                    if values == vec![b"ok".to_vec(), b"ok".to_vec()] {
                        break;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(
            values,
            vec![b"ok".to_vec(), b"ok".to_vec()],
            "both groups must serve the value they committed before the restart; {last}"
        );
        drop(meta);
        running.shutdown().await.unwrap();
        drop(running);
        drop(owner);
        config["project_groups"]["1"] = json!(3);
        config["project_groups"]["2"] = json!(2);
        std::fs::write(&manifest, serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(prepare_multi(&manifest)
            .unwrap()
            .validate_client_root(&root, 1, &peers, public)
            .is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
