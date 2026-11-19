use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use clap::Parser;
use serde::{Deserialize, Serialize};
use yadb_blob::{
    BarrierRequest, BarrierResponse, BlobError, BlobId, PinId, PoolHealthSnapshot, PoolSlot,
    PoolStriper, VolumeConfig, VolumeStore,
};
use yadb_block::{BlockError, BlockVolumeEngine, VolumeExtentMap, DEFAULT_BLOCK_SIZE};
use yadb_control::{parse_manifests, ControlError, ControlPlaneState, DiffOp, Role};
use yadb_db::{parse_sql_script, script_has_writes, DatabaseEngine, DbError, TabletManifest};
use yadb_io::OsFs;
use yadb_metakv::{KvCommand, LocalMetaKv, MetaError, RoleState, WatchEvent};
use yadb_s3::{ObjectManifest, ObjectStoreEngine, S3Error};

mod gc;
mod openraft;
mod protocols;

const CONTROL_STATE_KEY: &str = "/sys/control_plane_state";

/// How often the leader's periodic repair scan also runs a collection pass.
/// Collection takes the exclusive collector lease for its whole duration, so a
/// per-scan pass would keep an explicit `POST /v1/pools/gc` refused.
const BACKGROUND_SWEEP_INTERVAL: Duration = Duration::from_secs(300);
/// How often a node tries to confirm control-group freshness.
const CONTROL_REFRESH_INTERVAL: Duration = Duration::from_millis(200);
/// Maximum time an authorization decision may rely on the last confirmed
/// control snapshot. After this, access fails closed until quorum is proven.
const CONTROL_FRESHNESS_TTL: Duration = Duration::from_secs(2);
const EMBEDDED_UI_HTML: &str = include_str!("ui/index.html");
const EMBEDDED_SAMPLE_YAML: &str = include_str!("../../../examples/platform.yaml");

#[derive(Parser)]
#[command(
    name = "yadbd",
    version,
    about = "YADB Unified Storage, SQL, S3 & Block Node Daemon"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8420")]
    listen: String,

    /// Metadata runtime. OpenRaft is the only clustered metadata runtime and
    /// is always used; the flag is kept so an unsupported value fails loudly.
    #[arg(long, default_value = "openraft", value_parser = ["openraft"])]
    metadata_runtime: String,

    #[arg(long, default_value_t = 1)]
    node_id: u32,

    /// Optional comma-separated public peer map: `1=127.0.0.1:8421,2=127.0.0.1:8422,3=127.0.0.1:8423`
    #[arg(long)]
    peers: Option<String>,

    /// Shared secret required by the token-authenticated internal endpoints
    /// (`/internal/control/snapshot`, `/internal/vdisk/...`). Required and must
    /// be non-empty: an unauthenticated internal endpoint lets any client
    /// mutate storage behind the metadata runtime's back.
    #[arg(long, env = "YADB_RAFT_TOKEN", hide_env_values = true)]
    raft_token: Option<String>,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// OpenRaft group configuration path; required.
    #[arg(long)]
    openraft_config: Option<PathBuf>,

    /// S3 chunk size in bytes (default 1 MiB)
    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size: usize,

    /// Test-only override for how long an abandoned S3 upload intent is
    /// protected from orphan collection. Unset in production, where the engine's
    /// shipped default of 300 s applies; the protocol harness sets 1 so an
    /// abandoned intent becomes collectable inside one run. Shortening it is a
    /// test knob, not a deployment option: a shorter grace lets the collector
    /// race a client that is still uploading.
    #[arg(long)]
    s3_intent_grace_secs: Option<u64>,

    /// MemTable auto-flush threshold per SQL shard (default 1024 rows)
    #[arg(long, default_value_t = 1024)]
    db_flush_rows: usize,

    /// Opt-in iSCSI listener (CHAP required).
    #[arg(long)]
    iscsi_listen: Option<String>,
    /// Advertised iSCSI portals, independent of HTTP peer ports.
    #[arg(long, value_delimiter = ',')]
    iscsi_portals: Vec<String>,
    /// Opt-in PostgreSQL v3 Simple Query listener.
    #[arg(long)]
    pg_listen: Option<String>,
    /// PEM or DER certificate chain for TLS on the pgwire listener.
    #[arg(long, requires_all = ["pg_tls_key"])]
    pg_tls_cert: Option<PathBuf>,
    /// PEM or DER private key matching `--pg-tls-cert`.
    #[arg(long, requires_all = ["pg_tls_cert"])]
    pg_tls_key: Option<PathBuf>,
    /// Optional client CA bundle: when set, pgwire TLS requires a client
    /// certificate signed by one of these roots (mTLS).
    #[arg(long, requires_all = ["pg_tls_cert"])]
    pg_tls_client_ca: Option<PathBuf>,
    /// Refuse cleartext startup packets on the pgwire listener.
    #[arg(long, requires_all = ["pg_tls_cert"])]
    pg_require_tls: bool,
    /// Opt-in AWS S3 path-style listener.
    #[arg(long, requires = "s3_target")]
    s3_listen: Option<String>,
    /// Declarative project/ObjectStore exposed on the S3 listener.
    #[arg(long)]
    s3_target: Option<String>,
    /// JSON users -> {subject, secret}; file must be mode 0600.
    #[arg(long)]
    protocol_credentials: Option<PathBuf>,

    /// Optional shared secret for signed client subjects. When set, a client
    /// `x-yadb-subject` header is only honoured together with a valid
    /// `x-yadb-subject-signature`; unsigned subjects are treated as
    /// anonymous. Without this secret the subject header stays unverified.
    #[arg(long, env = "YADB_SUBJECT_SECRET", hide_env_values = true)]
    subject_secret: Option<String>,

    /// Signed subject(s) allowed to mutate the control plane through
    /// `/v1/apply`. Requires `--subject-secret`; a plain, unsigned
    /// `x-yadb-subject` header is never accepted. In OpenRaft mode the list
    /// must be non-empty, otherwise the daemon refuses to start.
    #[arg(long, value_delimiter = ',')]
    admin_subject: Vec<String>,
}

/// Bounded lease for the locally cached control snapshot. A fresh lease means
/// this node recently obtained a quorum-confirmed snapshot from the control
/// group leader; it does not make local data-group reads linearizable.
#[derive(Debug, Default)]
struct ControlFreshness {
    valid_until: Option<Instant>,
    revision: u64,
}

impl ControlFreshness {
    fn is_fresh_at(&self, now: Instant) -> bool {
        self.valid_until.is_some_and(|deadline| now < deadline)
    }

    /// Accept a snapshot unless it is older than the last accepted revision.
    /// Rejected snapshots do not extend the lease.
    fn admits(&self, revision: u64) -> bool {
        revision >= self.revision
    }

    fn accept_at(&mut self, revision: u64, now: Instant, ttl: Duration) -> bool {
        if !self.admits(revision) {
            return false;
        }
        self.revision = revision;
        self.valid_until = Some(now + ttl);
        true
    }

    fn invalidate(&mut self) {
        self.valid_until = None;
    }
}

/// Legacy metadata keeps its existing authorization behavior. OpenRaft
/// authorization is allowed only while a quorum-confirmed lease is valid.
fn control_authorization_fresh(openraft: bool, freshness: &ControlFreshness, now: Instant) -> bool {
    !openraft || freshness.is_fresh_at(now)
}

/// Why an authorization snapshot was refused.
#[derive(Debug)]
enum ControlAuthError {
    /// The locally applied snapshot is too old, or quorum cannot confirm it.
    Stale,
    /// No control image has been applied on this node yet. Authorization would
    /// treat every subject as a stranger and answer `403`, which clients do not
    /// retry — even though the same request can succeed moments later.
    NotReady,
}

/// A node that has bound its listeners but not installed a control image cannot
/// decide any request. Answer "not ready" so retrying clients retry and
/// operators see catch-up, rather than a denial that looks final.
const CONTROL_NOT_READY: &str = "control plane has not applied an image yet; the node is not ready";

impl std::fmt::Display for ControlAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stale => write!(
                f,
                "control-plane authorization state is stale or control quorum is unavailable; refusing access"
            ),
            Self::NotReady => f.write_str(CONTROL_NOT_READY),
        }
    }
}

impl IntoResponse for ControlAuthError {
    fn into_response(self) -> Response {
        api_err(StatusCode::SERVICE_UNAVAILABLE, self.to_string())
    }
}

/// Why a control-plane mutation was refused.
#[derive(Debug)]
enum ControlAdminError {
    /// No admin allowlist, or no usable subject secret to verify with.
    NotConfigured,
    /// The caller did not present a verifiably signed subject.
    Unverified,
    /// The caller proved a subject that is not on the admin allowlist.
    NotAdmin,
}

impl std::fmt::Display for ControlAdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => write!(
                f,
                "control-plane mutation requires a configured admin subject allowlist and subject secret"
            ),
            Self::Unverified => write!(
                f,
                "control-plane mutation requires a valid signed subject; unsigned or forged subjects are refused"
            ),
            Self::NotAdmin => write!(
                f,
                "subject is not authorized to mutate the control plane"
            ),
        }
    }
}

impl IntoResponse for ControlAdminError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::NotConfigured => StatusCode::SERVICE_UNAVAILABLE,
            Self::Unverified => StatusCode::UNAUTHORIZED,
            Self::NotAdmin => StatusCode::FORBIDDEN,
        };
        api_err(status, self.to_string())
    }
}

/// Compares two byte strings without an early exit on content mismatch.
/// Length is not secret here, so a length mismatch returns immediately.
fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Checks an already-verified subject against the allowlist. Every entry is
/// compared so the loop cost does not depend on the matching position.
fn subject_is_admin(subject: &str, admins: &[String]) -> bool {
    let mut matched = false;
    for admin in admins {
        matched |= constant_time_eq(subject, admin);
    }
    matched
}

/// Authorizes a control-plane mutation.
///
/// The subject must be proven with the shared secret; a plain
/// `x-yadb-subject` header is never enough. With an allowlist configured a
/// missing secret fails closed rather than allowing everyone.
///
/// `openraft` deployments must always configure an allowlist (the daemon
/// refuses to start otherwise), so an empty list there is still a failure. The
/// legacy runtime keeps its historical behavior when no allowlist is
/// configured: it is the default runtime and existing deployments have no
/// admin identity. Configuring `--admin-subject` opts legacy into the same
/// signed-subject check.
fn require_admin(
    headers: &HeaderMap,
    secret: Option<&str>,
    admins: &[String],
    openraft: bool,
) -> Result<Option<String>, ControlAdminError> {
    if admins.is_empty() {
        return if openraft {
            Err(ControlAdminError::NotConfigured)
        } else {
            Ok(None)
        };
    }
    let Some(secret) = secret.filter(|value| !value.is_empty()) else {
        return Err(ControlAdminError::NotConfigured);
    };
    let subject = verified_subject(headers, Some(secret)).ok_or(ControlAdminError::Unverified)?;
    if subject_is_admin(&subject, admins) {
        Ok(Some(subject))
    } else {
        Err(ControlAdminError::NotAdmin)
    }
}

#[derive(Clone)]
struct AppState {
    openraft: Option<openraft::Status>,
    listen_addr: String,
    fs: OsFs,
    meta: LocalMetaKv<OsFs>,
    /// Shared secret required by the token-authenticated internal endpoints.
    raft_token: Option<String>,
    /// Optional secret for signed client subjects.
    subject_secret: Option<String>,
    /// Signed subjects allowed to mutate the control plane.
    admin_subjects: Vec<String>,
    control: Arc<RwLock<ControlPlaneState>>,
    control_refresh: Arc<std::sync::Mutex<()>>,
    control_freshness: Arc<std::sync::Mutex<ControlFreshness>>,
    pools: Arc<RwLock<BTreeMap<String, Arc<PoolStriper<OsFs>>>>>,
    #[allow(clippy::type_complexity)]
    local_vdisks: Arc<RwLock<BTreeMap<(String, usize), Arc<VolumeStore<OsFs>>>>>,
    last_scrub: Arc<RwLock<Option<ScrubSummary>>>,
    /// The reader leases this node currently holds for garbage-collection
    /// fencing, keyed by metadata group (`None` is the legacy namespace).
    /// Reused across statements: publishing a fresh record per read would put
    /// two Raft entries in the log for every `SELECT`. A record in another
    /// group is not a fence for this read, so one slot for the whole node
    /// cannot describe Multi-Raft.
    reader_lease: Arc<std::sync::Mutex<BTreeMap<Option<u64>, yadb_metakv::GcLease>>>,
    s3: ObjectStoreEngine<OsFs>,
    db: DatabaseEngine<OsFs>,
    block: BlockVolumeEngine<OsFs>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ScrubSummary {
    pub pools_checked: usize,
    /// Live blobs the repair scan examined, over every group it was allowed to
    /// scan. Populated on Multi-Raft as well as single-node metadata: each group
    /// contributes the blobs its own manifests name.
    pub blobs_checked: usize,
    pub blobs_intact: usize,
    pub blobs_repaired: usize,
    pub fragments_repaired: usize,
    /// Blobs the pass could not prove intact. Two shapes produce one: the
    /// payload could not be reconstructed from the surviving fragments, or a
    /// fragment sat on a target that could not be reached, so its absence was
    /// never proved. Counted, never repaired and never reported intact: a pass
    /// that cannot read a blob must not claim it healed one.
    pub blobs_unrecoverable: usize,
    /// Writes refused because the pool could not promise the redundancy it
    /// advertises, summed over pools since this process started. A refused write
    /// is not acknowledged, so this is a loss of write availability, not a
    /// backlog of repairs to finish.
    pub refused_puts: usize,
    /// Writes acknowledged below full redundancy, summed over pools since this
    /// process started. Only a pool with an explicit `minWriteQuorum` below its
    /// fragment count can produce one; the fragments are repaired by a later
    /// pass, so this is the repair backlog rather than a failure.
    pub degraded_puts: usize,
    pub orphan_blobs_collected: usize,
    pub orphan_collection_error: Option<String>,
    /// Why the repair scan did not run for at least one metadata group, naming
    /// the group. `None` means every group's live set was proved and scanned on
    /// Multi-Raft, or that the pass ran on single-node metadata.
    pub repair_error: Option<String>,
    /// Distinct live logical blobs the sweep's final mark found.
    pub blobs_live: usize,
    /// Physical fragments removed by the sweep (barrier evictions and tombstones).
    pub fragments_deleted: usize,
    /// Bytes returned to the filesystem by the compaction that followed.
    pub bytes_reclaimed: u64,
    /// Live reader/writer leases observed. Non-zero always means the sweep refused.
    pub leases_live: usize,
    /// Set when the sweep deleted nothing. Always an explicit reason, never a
    /// silent no-op: a refused collection is a reported failure, not success.
    pub gc_refusal: Option<String>,
    /// Mounted pools the sweep declined to touch, with the reason.
    pub gc_pools_refused: Vec<String>,
    /// Multi-Raft metadata groups the sweep refused, with the reason. Empty on
    /// legacy metadata, which is a single namespace.
    #[serde(default)]
    pub gc_groups_refused: Vec<String>,
    /// Failures observed after the refusal point: a barrier or compaction that
    /// did not complete, and a live blob the repair pass could not check
    /// because its pool was not mounted. Reported, never swallowed: a failed
    /// compaction means space was not actually reclaimed, and an unchecked blob
    /// means its redundancy was not verified.
    pub gc_errors: Vec<String>,
    pub pools: Vec<PoolHealthSnapshot>,
}

#[derive(Serialize, Deserialize)]
struct ManifestPayload {
    yaml: String,
}

#[derive(Serialize)]
struct PlanResponse {
    current_generation: u64,
    next_generation: u64,
    ops: Vec<DiffOp>,
}

#[derive(Serialize)]
struct ApplyResponse {
    generation: u64,
    ops: Vec<DiffOp>,
}

#[derive(Deserialize, Default)]
struct ListQuery {
    #[serde(default)]
    prefix: String,
}

#[derive(Serialize, Deserialize)]
struct SqlQueryPayload {
    sql: String,
    #[serde(default)]
    session_id: Option<String>,
    /// Routing preference only; this does not provide a linearizable read fence.
    #[serde(default)]
    force_leader: bool,
    #[serde(default)]
    require_transaction: bool,
    /// Opt-in quorum-confirmed read of a `SELECT`/`SHOW` script. Absent or
    /// `false` keeps the default local applied-state behaviour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    consistent: Option<bool>,
}

#[derive(Deserialize)]
struct BlockWriteQuery {
    offset: u64,
}

#[derive(Deserialize)]
struct BlockReadQuery {
    offset: u64,
    len: usize,
    #[serde(default)]
    consistent: bool,
}

#[derive(Serialize, Deserialize)]
struct UnmapPayload {
    offset: u64,
    len: u64,
}

impl AppState {
    /// Authorization must not use a locally applied control snapshot after the
    /// control group can no longer demonstrate quorum-confirmed freshness.
    /// Legacy mode preserves its existing behavior.
    fn control_for_authorization(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, ControlPlaneState>, ControlAuthError> {
        if self.meta.is_openraft() {
            self.ensure_control_fresh();
        }
        self.authorization_snapshot()
    }

    /// Tokio-handler variant: any required quorum refresh runs on a blocking
    /// thread. A stalled control leader must not occupy a runtime worker or hold
    /// the refresh mutex from an async context, which could otherwise stall all
    /// listeners on this node and prevent this node from serving the leader's
    /// own internal snapshot request.
    async fn control_for_authorization_async(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, ControlPlaneState>, ControlAuthError> {
        if self.meta.is_openraft() && !self.control_lease_fresh() {
            self.ensure_control_fresh_blocking().await;
        }
        self.authorization_snapshot()
    }

    fn authorization_snapshot(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, ControlPlaneState>, ControlAuthError> {
        if !control_authorization_fresh(
            self.meta.is_openraft(),
            &self.control_freshness.lock().unwrap(),
            Instant::now(),
        ) {
            return Err(ControlAuthError::Stale);
        }
        let cp = self.control.read().unwrap();
        if cp.generation == 0 {
            return Err(ControlAuthError::NotReady);
        }
        Ok(cp)
    }

    /// Run the blocking refresh path off the async runtime. A panic or runtime
    /// shutdown is not treated as freshness: the caller re-checks the lease and
    /// fails closed if it is still expired.
    async fn ensure_control_fresh_blocking(&self) {
        let state = self.clone();
        if tokio::task::spawn_blocking(move || state.ensure_control_fresh())
            .await
            .is_err()
        {
            eprintln!("control freshness refresh task failed");
        }
    }

    fn control_lease_fresh(&self) -> bool {
        self.control_freshness
            .lock()
            .unwrap()
            .is_fresh_at(Instant::now())
    }

    /// Publishes or reuses this node's reader lease before a handler reads
    /// fragments.
    ///
    /// The record lives in the metadata group that owns the project's
    /// manifests, resolved exactly as a writer lease is. A record in another
    /// group would be invisible to that group's collector, so the group is
    /// resolved before a cached record is considered: only a record of the
    /// same group is a fence for this read.
    ///
    /// A reader never refuses on a live collector lease: its applied view is at
    /// least as new as the collector's mark, and the collector re-checks reader
    /// leases after publishing its own, so the collector is the side that backs
    /// off.
    ///
    /// The record is kept for the next statement instead of being deleted: one
    /// published record fences every read on this node and group, and a fresh
    /// record per statement would put two Raft entries in the log for every
    /// `SELECT`.
    ///
    /// A cached record is *renewed*, not merely checked: a read must start with a
    /// full TTL in front of it, otherwise a slow read would run past the fence it
    /// published. Renewal also refuses when the record was reaped or replaced, so
    /// a stale cached lease is never used as a fence.
    ///
    /// Blocking; call from `spawn_blocking`.
    fn begin_read_lease(&self, project_id: u32) -> Result<yadb_metakv::GcLease, MetaError> {
        let group = self.meta.gc_group_for_project(project_id)?;
        let mut cached = self.reader_lease.lock().unwrap();
        if let Some(lease) = cached.remove(&group) {
            match yadb_metakv::gc::renew(&self.meta, &lease, yadb_metakv::gc::DEFAULT_TTL_MS) {
                Ok(renewed) => return Ok(renewed),
                Err(error) => {
                    // The record expired — reaped or not — or was taken over:
                    // publish a fresh one below instead of reading under a lost
                    // fence. `renew` refuses an expired record precisely so this
                    // branch is taken rather than extending a dead lease.
                    eprintln!("reader lease renewal failed, publishing a fresh one: {error}");
                }
            }
        }
        // A group-scoped lease cannot carry the deployment-wide revision: there
        // is none. It carries the owning group's applied log index instead, a
        // value the group really supplies. The collector re-reads that index
        // after closing admission and refuses a record that names another group
        // or claims an index this group has not applied, so the field is a
        // checkable provenance token rather than a decorative number.
        let revision = match group {
            Some(group) => self.meta.role_info_for_group(group)?.4,
            None => self.meta.try_revision()?,
        };
        let lease = yadb_metakv::gc::acquire(
            &self.meta,
            yadb_metakv::gc::LeaseKind::Reader,
            project_id,
            revision,
            yadb_metakv::gc::DEFAULT_TTL_MS,
        )?;
        // The durable watermark is a single-namespace fence. On Multi-Raft the
        // lease publication above is itself a committed write in the owning
        // group, so this node's applied view includes every entry committed
        // before it; the group collector's admission CAS is the other half of
        // the fence, and it refuses a live record after taking its mark.
        if group.is_none() {
            // The lease makes a concurrent pass refuse, but it cannot make this
            // node's *applied* view current: a lagging follower can still hold an
            // old manifest whose fragments a completed pass already reclaimed.
            // Publishing the lease first and checking the sweep watermark second
            // closes that window — the pass writes the watermark before it
            // re-checks leases.
            if let Err(error) = yadb_metakv::gc::reader_is_safe(&self.meta) {
                let _ = yadb_metakv::gc::release(&self.meta, &lease);
                return Err(error);
            }
        }
        Ok(lease)
    }

    /// Ends a read: proves the fence still held, then keeps the record for the
    /// next read instead of deleting it. The record expires on its own, so a
    /// crashed node still stops blocking collection; an idle node stops blocking
    /// it one TTL after its last read.
    ///
    /// The proof is not cosmetic. A read that outlived its lease can no longer
    /// claim that no pass ran while it was fetching fragments: a pass that reaped
    /// the expired record could have reclaimed exactly those fragments, so the
    /// answer it produced may be incomplete. The caller must refuse rather than
    /// return it. A dead record is not cached either — it is already gone.
    fn end_read_lease(&self, lease: &yadb_metakv::GcLease) -> Result<(), MetaError> {
        yadb_metakv::gc::ensure_live(&self.meta, lease)?;
        self.reader_lease
            .lock()
            .unwrap()
            .insert(lease.group, lease.clone());
        Ok(())
    }

    /// Establish a fresh lease on demand, e.g. immediately after restart
    /// before the periodic refresher has run. Concurrent requests serialize on
    /// the refresh mutex and re-check the lease, so a burst performs at most
    /// one quorum-confirmed read while the control group is available.
    fn ensure_control_fresh(&self) {
        if !self.meta.is_openraft() || self.control_lease_fresh() {
            return;
        }
        let _guard = self.control_refresh.lock().unwrap();
        if self.control_lease_fresh() {
            return;
        }
        self.refresh_openraft_control_locked();
    }

    fn refresh_openraft_control(&self) {
        if !self.meta.is_openraft() {
            return;
        }
        let _guard = self.control_refresh.lock().unwrap();
        self.refresh_openraft_control_locked();
    }

    /// Async variant for HTTP handlers; the quorum-confirmed fetch is blocking.
    async fn refresh_openraft_control_async(&self) {
        if !self.meta.is_openraft() {
            return;
        }
        let state = self.clone();
        if tokio::task::spawn_blocking(move || state.refresh_openraft_control())
            .await
            .is_err()
        {
            eprintln!("control freshness refresh task failed");
        }
    }

    /// Caller must hold `control_refresh`.
    fn refresh_openraft_control_locked(&self) {
        // Keep the last snapshot but let its lease expire. Authorization fails
        // closed while the control group cannot prove freshness.
        let Some((revision, cp)) = self.fetch_fresh_control().ok() else {
            return;
        };
        let changed =
            serde_json::to_vec(&*self.control.read().unwrap()).ok() != serde_json::to_vec(&cp).ok();
        {
            let mut freshness = self.control_freshness.lock().unwrap();
            // A regressed revision must not extend the lease.
            if !freshness.admits(revision) {
                return;
            }
            // Install the snapshot before extending the lease: a request that
            // observes a fresh lease must also observe the snapshot that
            // confirmed it, otherwise a revocation could be missed.
            *self.control.write().unwrap() = cp.clone();
            freshness.accept_at(revision, Instant::now(), CONTROL_FRESHNESS_TTL);
        }
        // Replace authorization state before potentially slow pool I/O.
        if changed {
            if cp.pools.is_empty() {
                self.pools.write().unwrap().clear();
            }
            let _ = self.materialize_pools(&cp);
        }
    }

    /// Obtain a control snapshot only after a linearizable barrier on the
    /// current control-group leader. A follower asks that leader over the
    /// token-authenticated internal endpoint instead of trusting its own
    /// possibly stale applied state.
    fn fetch_fresh_control(&self) -> Result<(u64, ControlPlaneState), String> {
        let (node_id, role, _, leader, _) = self.meta.role_info();
        if role == RoleState::Leader {
            let snapshot = self
                .meta
                .try_linearizable_control_snapshot(&[CONTROL_STATE_KEY.to_string()])
                .map_err(|e| e.to_string())?;
            return decode_fresh_control_snapshot(snapshot);
        }
        let leader = leader
            .filter(|id| *id != node_id)
            .ok_or_else(|| "control-group leader unknown".to_string())?;
        let addr = self
            .meta
            .peer_addr(leader)
            .ok_or_else(|| format!("control-group leader {leader} public address missing"))?;
        let token = self
            .raft_token
            .as_deref()
            .ok_or_else(|| "internal control snapshot token missing".to_string())?;
        fetch_control_snapshot_http(&addr, token)
    }

    fn open_or_get_local_vdisk(
        &self,
        pool_name: &str,
        slot: usize,
    ) -> Result<Arc<VolumeStore<OsFs>>, String> {
        let key = (pool_name.to_string(), slot);
        // The check, the open and the insert happen under one write lock. Two
        // requests that both miss the map would otherwise open the same
        // directory twice, and each instance keeps its own `write_pos`,
        // tombstones and barriers. Both then append to the same segment from the
        // same offset, so the second writer overwrites records the first one
        // acknowledged — including tombstones and GC barriers, which is how a
        // deleted blob could be resurrected after a restart. Serialising here is
        // what makes "one store per slot directory" true.
        let mut map = self.local_vdisks.write().unwrap();
        if let Some(existing) = map.get(&key) {
            return Ok(existing.clone());
        }
        let dir = format!("pools/{pool_name}/slot_{slot:02}");
        let store = Arc::new(
            VolumeStore::open(self.fs.clone(), dir, VolumeConfig::default())
                .map_err(|e| e.to_string())?,
        );
        map.insert(key, store.clone());
        Ok(store)
    }

    /// Computes `(domain_name, node_id, peer_addr)` targets across failure domains.
    /// In clustered `--peers` mode, slots are striped across real cluster nodes in domain round-robin!
    fn placement_targets(&self, cp: &ControlPlaneState) -> Vec<(String, u32, Option<String>)> {
        let my_id = self.meta.node_id();
        let peer_addrs = self.meta.peer_addrs();

        if peer_addrs.len() > 1 {
            let mut targets: Vec<(String, u32, Option<String>)> = Vec::new();
            let mut seen_nodes = BTreeSet::new();

            // Interleave across declared Cluster failure domains first (round-robin across racks/AZs)
            if let Some((_, cluster_spec)) = &cp.cluster {
                let max_len = cluster_spec
                    .domains
                    .iter()
                    .map(|d| d.nodes.len())
                    .max()
                    .unwrap_or(0);
                for depth in 0..max_len {
                    for dom in &cluster_spec.domains {
                        if let Some(node) = dom.nodes.get(depth) {
                            if let Some(addr) = peer_addrs.get(&node.id) {
                                if seen_nodes.insert(node.id) {
                                    targets.push((dom.name.clone(), node.id, Some(addr.clone())));
                                }
                            }
                        }
                    }
                }
            }

            // Include any remaining `--peers` nodes not listed in ClusterSpec
            for (&nid, addr) in peer_addrs {
                if seen_nodes.insert(nid) {
                    targets.push((format!("zone-{nid}"), nid, Some(addr.clone())));
                }
            }

            if !targets.is_empty() {
                return targets;
            }
        }

        // Single-node mode: all slots reside locally on `my_id`, tagged with Cluster domain names
        if let Some((_, cluster_spec)) = &cp.cluster {
            if !cluster_spec.domains.is_empty() {
                return cluster_spec
                    .domains
                    .iter()
                    .map(|d| (d.name.clone(), my_id, Some(self.listen_addr.clone())))
                    .collect();
            }
        }

        vec![("local".to_string(), my_id, Some(self.listen_addr.clone()))]
    }

    /// Reconciles distributed `PoolSlot` (`Local` + `Remote` VDisks) + `PoolStriper` instances
    /// across S3, SQL Database, and BlockVolume engines!
    fn materialize_pools(&self, cp: &ControlPlaneState) -> Result<(), String> {
        let my_id = self.meta.node_id();
        let targets = self.placement_targets(cp);

        for (pool_name, spec) in &cp.pools {
            let scheme = spec.erasure.to_scheme();
            let parts = scheme.total_parts();
            // Handoff slots come after the nominal ones, so `select_slot` (which
            // reduces modulo `parts`) keeps every existing part exactly where it
            // was. The daemon only has to create the extra slots.
            let handoff = spec.handoff_slots.unwrap_or(0);
            let mut slots = Vec::with_capacity(parts + handoff);

            for slot in 0..(parts + handoff) {
                let (dom_name, target_node, target_addr) = &targets[slot % targets.len()];
                if *target_node == my_id {
                    let store = self.open_or_get_local_vdisk(pool_name, slot)?;
                    slots.push(PoolSlot::local(
                        slot,
                        dom_name.clone(),
                        *target_node,
                        target_addr.clone(),
                        store,
                    ));
                } else {
                    let addr = target_addr
                        .clone()
                        .ok_or_else(|| format!("missing address for remote node {target_node}"))?;
                    slots.push(PoolSlot::remote(
                        pool_name.clone(),
                        slot,
                        dom_name.clone(),
                        *target_node,
                        addr,
                        self.raft_token.clone(),
                    ));
                }
            }

            let striper = Arc::new(
                PoolStriper::from_slots(pool_name.clone(), scheme, slots)
                    .and_then(|striper| match spec.min_write_quorum {
                        // Absent keeps the default: an ack promises full redundancy.
                        None => Ok(striper),
                        Some(quorum) => striper.with_min_write_quorum(quorum),
                    })
                    .map_err(|e| e.to_string())?,
            );
            self.pools
                .write()
                .unwrap()
                .insert(pool_name.clone(), striper.clone());
            self.s3.register_pool(pool_name.clone(), striper.clone());
            self.db.register_pool(pool_name.clone(), striper.clone());
            self.block.register_pool(pool_name.clone(), striper);
        }
        Ok(())
    }

    fn collect_pool_health(&self, probe_remote: bool) -> Vec<PoolHealthSnapshot> {
        let pools: Vec<Arc<PoolStriper<OsFs>>> =
            self.pools.read().unwrap().values().cloned().collect();
        pools
            .into_iter()
            .map(|p| p.health_snapshot(probe_remote))
            .collect()
    }

    /// Runs one mark-and-sweep pass without the repair scan. Same snapshot and
    /// fence rules as [`Self::scrub_all_pools`]; a refusal is returned to the
    /// caller as an error response, not hidden in a 200 body.
    ///
    /// Collection runs on both metadata runtimes. On Multi-Raft the pass is
    /// per metadata group and refuses a group whose leadership, leases or
    /// storage fence cannot be proved; a refusal is reported, never swallowed.
    fn gc_all_pools(&self) -> gc::SweepOutcome {
        let control = self.control.read().unwrap().clone();
        let pools = self.pools.read().unwrap().clone();
        gc::sweep(&self.meta, &pools, &control)
    }

    /// Collects orphan S3 blobs one metadata group at a time.
    ///
    /// A project's manifests live in exactly one group, so a per-group pass can
    /// answer "is this chunk still referenced" from a quorum-confirmed view of
    /// that group. The namespace-wide pass cannot: it would need every group's
    /// view at once. A group this node does not lead is reported, not skipped -
    /// an unproven group must not look collected.
    fn collect_orphans_per_group(&self) -> (usize, Option<String>) {
        let scopes = match self.meta.gc_group_scopes() {
            Ok(scopes) => scopes,
            Err(error) => return (0, Some(error.to_string())),
        };
        let mut collected = 0usize;
        let mut errors = Vec::new();
        for scope in scopes {
            match self.s3.collect_orphans_in_group(scope.group_id) {
                Ok(count) => collected += count,
                Err(error) => errors.push(format!("group {}: {error}", scope.group_id)),
            }
        }
        (collected, (!errors.is_empty()).then(|| errors.join("; ")))
    }

    /// Scans all live SQL Tablet manifests, S3 Object manifests, and BlockVolume extent maps in `MetaKV`,
    /// verifies every fragment across cluster VDisks, and heals missing or bit-rotted fragments!
    ///
    /// `sweep` controls the garbage-collection phase. The periodic repair scan
    /// passes `false` most of the time: a pass holds the exclusive collector
    /// lease, and running one every few seconds means an explicit
    /// `POST /v1/pools/gc` almost always finds the lease held and refuses.
    fn scrub_all_pools(&self, sweep: bool) -> ScrubSummary {
        // A short write is refused at `put_blob`, so there is no in-memory backlog
        // to drain here: every acknowledged blob was fully redundant when it was
        // written. This scan heals damage that happened *after* the ack (a lost or
        // bit-rotted fragment), which is a different failure and needs the manifest
        // scan to know which blobs should exist.
        if self.meta.is_openraft() {
            // Repair runs per group, because the question it asks of metadata -
            // "which blobs should exist" - is answered completely by one
            // group's snapshot: every data namespace routes a project id to
            // exactly one group, and a blob id embeds its project id, so the
            // blobs one group's manifests name are exactly that group's blobs.
            // A group this node does not lead, or one whose quorum does not
            // answer, is refused and named; it does not stop another group's
            // pass. There is no fallback to locally applied state.
            let (orphan_blobs_collected, orphan_collection_error) =
                self.collect_orphans_per_group();
            let control = self.control.read().unwrap().clone();
            let pools = self.pools.read().unwrap().clone();
            let repair = gc::repair_scan_groups(&self.meta, &pools, &control);
            let sweep_outcome = if sweep {
                gc::sweep(&self.meta, &pools, &control)
            } else {
                gc::SweepOutcome::refuse(
                    "collection skipped in this repair scan: it runs every five minutes and on POST /v1/pools/gc",
                )
            };
            // A blob the repair pass could not check (an unmounted pool) is
            // reported next to the sweep's failures, never folded into a count.
            let mut gc_errors = sweep_outcome.errors;
            gc_errors.extend(repair.errors);
            let summary = ScrubSummary {
                refused_puts: self
                    .pools
                    .read()
                    .unwrap()
                    .values()
                    .map(|striper| striper.refused_put_count() as usize)
                    .sum(),
                degraded_puts: self
                    .pools
                    .read()
                    .unwrap()
                    .values()
                    .map(|striper| striper.degraded_put_count() as usize)
                    .sum(),
                blobs_checked: repair.blobs_checked,
                blobs_intact: repair.blobs_intact,
                blobs_repaired: repair.blobs_repaired,
                fragments_repaired: repair.fragments_repaired,
                blobs_unrecoverable: repair.blobs_unrecoverable,
                // Orphan blob collection also runs per group, for the same
                // reason: a project's manifests live in one group.
                orphan_blobs_collected,
                orphan_collection_error,
                // `None` only when every group's live set was proved and
                // scanned; otherwise the refused groups are named.
                repair_error: repair.refusal,
                blobs_live: sweep_outcome.blobs_live,
                fragments_deleted: sweep_outcome.fragments_deleted,
                bytes_reclaimed: sweep_outcome.bytes_reclaimed,
                leases_live: sweep_outcome.leases_live,
                gc_refusal: sweep_outcome.refusal,
                gc_pools_refused: sweep_outcome.pools_refused,
                gc_groups_refused: sweep_outcome.groups_refused,
                gc_errors,
                ..ScrubSummary::default()
            };
            *self.last_scrub.write().unwrap() = Some(summary.clone());
            return summary;
        }
        let mut live_blobs: BTreeSet<(String, BlobId)> = BTreeSet::new();

        // A failed read is not an empty range: the live set would be
        // incomplete, so the pass refuses rather than repairing (or sweeping)
        // against a view it could not read.
        let read_prefix = |prefix: &str| match self.meta.try_scan_prefix(prefix) {
            Ok(rows) => Ok(rows),
            Err(error) => Err(ScrubSummary {
                gc_refusal: Some(format!(
                    "repair scan refused: live manifests under `{prefix}` cannot be read: {error}"
                )),
                gc_errors: vec![error.to_string()],
                ..ScrubSummary::default()
            }),
        };

        // Record the refusal so `/status` shows why the last pass did nothing,
        // exactly like the Multi-Raft branch above.
        let refuse_scrub = |summary: ScrubSummary| -> ScrubSummary {
            *self.last_scrub.write().unwrap() = Some(summary.clone());
            summary
        };

        // 1. SQL DataShard Tablets (`/db/{pid}/{db}/shards/{table}/{idx}`)
        let db_rows = match read_prefix("/db/") {
            Ok(rows) => rows,
            Err(summary) => return refuse_scrub(summary),
        };
        for (k, kv) in db_rows {
            if k.contains("/shards/") {
                if let Ok(mf) = serde_json::from_slice::<TabletManifest>(&kv.value) {
                    for b in mf.wal_blobs {
                        live_blobs.insert((mf.wal_pool.clone(), b));
                    }
                    for sst in mf.sst_blobs {
                        live_blobs.insert((mf.data_pool.clone(), sst.blob_id));
                    }
                }
            }
        }

        // 2. S3 ObjectStore manifests (`/s3/{pid}/{store}/{bucket}/{key}`)
        let s3_rows = match read_prefix("/s3/") {
            Ok(rows) => rows,
            Err(summary) => return refuse_scrub(summary),
        };
        for (_k, kv) in s3_rows {
            if let Ok(mf) = serde_json::from_slice::<ObjectManifest>(&kv.value) {
                for ch in mf.chunks {
                    live_blobs.insert((mf.pool.clone(), ch.blob_id));
                }
            }
        }

        // 3. Thin-Provisioned BlockVolumes (`/block/{pid}/{volume}`)
        let block_pool_lookup: BTreeMap<(u32, String), String> = {
            let cp = self.control.read().unwrap();
            let mut map = BTreeMap::new();
            for (full_name, spec) in &cp.block_volumes {
                if let Some((proj, vol)) = full_name.split_once('/') {
                    if let Some((pid, _)) = cp.projects.get(proj) {
                        map.insert((*pid, vol.to_string()), spec.pool.clone());
                    }
                }
            }
            map
        };
        let block_rows = match read_prefix("/block/") {
            Ok(rows) => rows,
            Err(summary) => return refuse_scrub(summary),
        };
        for (k, kv) in block_rows {
            let rest = k.strip_prefix("/block/").unwrap_or("");
            if let Some((pid_s, vol_name)) = rest.split_once('/') {
                if let Ok(pid) = pid_s.parse::<u32>() {
                    if let Some(pool_name) = block_pool_lookup.get(&(pid, vol_name.to_string())) {
                        if let Ok(ext) = serde_json::from_slice::<VolumeExtentMap>(&kv.value) {
                            for &bid in ext.blocks.values() {
                                live_blobs.insert((pool_name.clone(), bid));
                            }
                        }
                    }
                }
            }
        }

        let pools_map = self.pools.read().unwrap().clone();
        let mut blobs_checked = 0usize;
        let mut blobs_intact = 0usize;
        let mut blobs_repaired = 0usize;
        let mut fragments_repaired = 0usize;
        let mut blobs_unrecoverable = 0usize;
        let (orphan_blobs_collected, orphan_collection_error) = match self.s3.collect_orphans_all()
        {
            Ok(count) => (count, None),
            Err(error) => (0, Some(error.to_string())),
        };

        for (pool_name, blob_id) in live_blobs {
            let Some(striper) = pools_map.get(&pool_name) else {
                continue;
            };
            blobs_checked += 1;
            match striper.repair_blob(blob_id) {
                Ok(rep) => {
                    if rep.repaired_parts == 0 {
                        blobs_intact += 1;
                    } else {
                        blobs_repaired += 1;
                        fragments_repaired += rep.repaired_parts;
                    }
                }
                Err(_) => {
                    blobs_unrecoverable += 1;
                }
            }
        }

        let pools_health = self.collect_pool_health(true);
        // Mark-and-sweep runs after the repair pass so fragments it just healed are
        // part of the live set. It takes its own snapshots and refuses on its own
        // terms; a refusal deletes nothing and is reported, never hidden.
        let sweep = if sweep {
            let control = self.control.read().unwrap().clone();
            let pools = self.pools.read().unwrap().clone();
            gc::sweep(&self.meta, &pools, &control)
        } else {
            // A repair scan that skipped collection must say so, rather than
            // report zero deletions that could be read as "nothing to collect".
            gc::SweepOutcome::refuse(
                "collection skipped in this repair scan: it runs every five minutes and on POST /v1/pools/gc",
            )
        };
        let summary = ScrubSummary {
            pools_checked: pools_health.len(),
            blobs_checked,
            blobs_intact,
            blobs_repaired,
            fragments_repaired,
            blobs_unrecoverable,
            refused_puts: pools_health.iter().map(|p| p.refused_puts as usize).sum(),
            degraded_puts: pools_health.iter().map(|p| p.degraded_puts as usize).sum(),
            orphan_blobs_collected,
            orphan_collection_error,
            repair_error: None,
            blobs_live: sweep.blobs_live,
            fragments_deleted: sweep.fragments_deleted,
            bytes_reclaimed: sweep.bytes_reclaimed,
            leases_live: sweep.leases_live,
            gc_refusal: sweep.refusal,
            gc_pools_refused: sweep.pools_refused,
            gc_groups_refused: sweep.groups_refused,
            gc_errors: sweep.errors,
            pools: pools_health,
        };
        *self.last_scrub.write().unwrap() = Some(summary.clone());
        summary
    }
}

#[cfg(test)]
fn decode_control_snapshot(
    snapshot: Result<yadb_metakv::MetaSnapshot, MetaError>,
) -> Option<ControlPlaneState> {
    snapshot
        .ok()
        .and_then(|snapshot| snapshot.get(CONTROL_STATE_KEY))
        .and_then(|value| serde_json::from_slice(&value.value).ok())
}

fn decode_fresh_control_snapshot(
    snapshot: yadb_metakv::MetaSnapshot,
) -> Result<(u64, ControlPlaneState), String> {
    let revision = snapshot.revision();
    match snapshot.get(CONTROL_STATE_KEY) {
        Some(value) => serde_json::from_slice::<ControlPlaneState>(&value.value)
            .map(|control| (revision, control))
            .map_err(|e| format!("invalid control snapshot: {e}")),
        // A new cluster may not have written the control key yet. An absent key
        // at a quorum-confirmed revision is a valid empty control plane.
        None => Ok((revision, ControlPlaneState::new())),
    }
}

#[derive(Debug, Deserialize)]
struct FreshControlSnapshot {
    revision: u64,
    control: ControlPlaneState,
}

/// Fetch a leader-confirmed control snapshot over the internal token-protected
/// HTTP endpoint. This function is deliberately bounded and synchronous so it
/// can run on the control-refresh thread without a Tokio runtime dependency.
fn fetch_control_snapshot_http(
    peer_addr: &str,
    token: &str,
) -> Result<(u64, ControlPlaneState), String> {
    let clean = peer_addr
        .trim()
        .strip_prefix("http://")
        .unwrap_or(peer_addr.trim())
        .trim_end_matches('/');
    let sock: SocketAddr = clean
        .parse()
        .map_err(|e| format!("invalid control leader address `{peer_addr}`: {e}"))?;
    let mut stream = TcpStream::connect_timeout(&sock, Duration::from_millis(400))
        .map_err(|e| format!("connect to control leader {clean}: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_millis(1500)))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_millis(500)))
        .map_err(|e| e.to_string())?;
    let request = format!(
        "GET /internal/control/snapshot HTTP/1.1\r\nHost: {clean}\r\nx-yadb-raft-token: {token}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write control snapshot request: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("flush control snapshot request: {e}"))?;
    let mut raw = Vec::new();
    stream
        .take((MAX_PROXY_WIRE + 1) as u64)
        .read_to_end(&mut raw)
        .map_err(|e| format!("read control snapshot response: {e}"))?;
    if raw.len() > MAX_PROXY_WIRE {
        return Err("control snapshot response too large".into());
    }
    let response = parse_proxy_response(&raw)
        .map_err(|e| format!("malformed control snapshot response: {e}"))?;
    if response.code != 200 {
        return Err(format!(
            "control leader returned HTTP {}: {}",
            response.code,
            String::from_utf8_lossy(&response.body)
        ));
    }
    let parsed: FreshControlSnapshot = serde_json::from_slice(&response.body)
        .map_err(|e| format!("invalid control snapshot JSON: {e}"))?;
    Ok((parsed.revision, parsed.control))
}

fn parse_peer_map(raw: &str) -> Result<BTreeMap<u32, String>, String> {
    let mut map = BTreeMap::new();
    for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id_s, addr) = entry
            .split_once('=')
            .ok_or_else(|| format!("invalid peer entry `{entry}`, expected `<id>=<host:port>`"))?;
        let id: u32 = id_s
            .trim()
            .parse()
            .map_err(|_| format!("invalid peer node id `{id_s}`"))?;
        map.insert(id, addr.trim().to_string());
    }
    Ok(map)
}

fn api_err(status: StatusCode, msg: impl Into<String>) -> Response {
    let body = serde_json::json!({ "error": msg.into() });
    (status, Json(body)).into_response()
}

fn meta_err_resp(st: &AppState, err: MetaError) -> Response {
    meta_err_response(err, |id| st.meta.peer_addr(id))
}

fn meta_err_response(err: MetaError, peer_addr: impl FnOnce(u32) -> Option<String>) -> Response {
    // A write whose commit outcome the node cannot determine must not be reported
    // as a definite failure. `UncommittedIndex` means exactly that on the legacy
    // path: the proposal may still commit, or a later leader may have replaced it.
    // Answering 500 made "unknown" indistinguishable from "rejected", which is how
    // a client ends up retrying a mutation that already happened.
    if matches!(
        err,
        MetaError::OpenRaftWriteUnknown(_) | MetaError::UncommittedIndex(_)
    ) {
        return forward_error_response(err.to_string(), Some("unknown"));
    }
    match err {
        // The node has stopped accepting metadata work until it is restarted;
        // another replica may still be able to serve the request. A read of the
        // local applied state that cannot be served is the same situation: the
        // answer is "not ready", not "this key is absent" and not an internal
        // error the client should interpret as a rejected request.
        MetaError::CompactionFailed(_) | MetaError::StorageUnavailable => {
            api_err(StatusCode::SERVICE_UNAVAILABLE, err.to_string())
        }
        MetaError::NotLeader { leader } => {
            let leader_addr = leader.and_then(peer_addr);
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": err.to_string(),
                    "leader_id": leader,
                    "leader_addr": leader_addr,
                })),
            )
                .into_response()
        }
        other => api_err(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

/// Computes the HMAC-SHA256 signature of a subject.
fn subject_signature(secret: &str, subject: &str) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    ring::hmac::sign(&key, subject.as_bytes())
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Verifies a subject signature in constant time.
fn subject_signature_matches(secret: &str, subject: &str, signature: &str) -> bool {
    let expected = subject_signature(secret, subject);
    if expected.len() != signature.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in expected.bytes().zip(signature.bytes()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Returns the verified subject, or `None` when the caller did not prove it.
///
/// With `--subject-secret` configured an unsigned subject is treated as
/// anonymous. Without the secret no subject can be verified and the legacy
/// unverified header is used.
fn verified_subject(headers: &HeaderMap, secret: Option<&str>) -> Option<String> {
    let Some(secret) = secret else {
        return None;
    };
    let subject = headers
        .get("x-yadb-subject")
        .and_then(|h| h.to_str().ok())?
        .trim()
        .to_string();
    if subject.is_empty() {
        return None;
    }
    let signature = headers
        .get("x-yadb-subject-signature")
        .and_then(|h| h.to_str().ok())
        .unwrap_or_default();
    if subject_signature_matches(secret, &subject, signature) {
        Some(subject)
    } else {
        None
    }
}

/// Extracts the caller's subject.
///
/// A client-supplied `x-yadb-subject` header is not an authenticated
/// identity: anybody can send it. When `--subject-secret` is configured the
/// header is only honoured together with a valid HMAC signature, so a
/// forwarded request keeps the same verified subject at the leader. Without
/// the secret the legacy behaviour is kept, and the subject stays an
/// unverified label.
fn extract_subject(headers: &HeaderMap, secret: Option<&str>) -> String {
    if let Some(secret) = secret {
        // A signed subject is the only trusted form when a secret is set.
        return verified_subject(headers, Some(secret)).unwrap_or_else(|| "anonymous".to_string());
    }
    if let Some(v) = headers.get("x-yadb-subject").and_then(|h| h.to_str().ok()) {
        return v.trim().to_string();
    }
    if let Some(auth) = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    {
        if let Some(tok) = auth.strip_prefix("Bearer ") {
            return tok.trim().to_string();
        }
    }
    "anonymous".to_string()
}

const MAX_PROXY_HEADERS: usize = 64 * 1024;
const MAX_PROXY_BODY: usize = 16 * 1024 * 1024;
// Includes bounded chunk framing and trailers in addition to the decoded body.
const MAX_PROXY_WIRE: usize = MAX_PROXY_BODY + MAX_PROXY_HEADERS * 2;

#[derive(Debug)]
struct ProxyResponse {
    code: u16,
    content_type: String,
    etag: Option<String>,
    write_outcome: Option<String>,
    retry: Option<String>,
    headers: Vec<(String, String)>,
    /// Declared body length. A `HEAD` answer reports the size of the
    /// representation while carrying no bytes, so the length must survive the
    /// hop even though `body` is empty.
    content_length: Option<usize>,
    body: Vec<u8>,
}

/// All field lines of a list-valued header combined into one value, or `None`
/// when the header is absent.
///
/// RFC 9110 lets a recipient combine repeated field lines of a list field into
/// one comma-separated value. The local evaluator and the forwarded request both
/// read this combined value, so the leader cannot interpret the header
/// differently from the follower that forwarded it.
///
/// The bytes are preserved verbatim: an `entity-tag` may contain `obs-text`,
/// which is not valid UTF-8, and a lossy conversion could turn one opaque tag
/// into another.
fn joined_field_value(headers: &HeaderMap, name: &HeaderName) -> Option<Vec<u8>> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?;
    let mut joined = first.as_bytes().to_vec();
    for value in values {
        joined.extend_from_slice(b", ");
        joined.extend_from_slice(value.as_bytes());
    }
    Some(joined)
}

/// The read headers a proxied request must carry unchanged.
///
/// Every value comes from an already-parsed `HeaderValue`, which cannot contain
/// CR or LF, so a forwarded value cannot add a header line. There is no separate
/// size bound here: the front-end HTTP parser bounds inbound header values.
#[derive(Clone, Copy, Default)]
struct ForwardedRead<'a> {
    range: Option<&'a str>,
    if_match: Option<&'a [u8]>,
    if_none_match: Option<&'a [u8]>,
}

/// Appends one `name: value` field line without copying `value` through a
/// `str`, so an `obs-text` entity-tag keeps its bytes.
fn push_header(request: &mut Vec<u8>, name: &str, value: &[u8]) {
    request.extend_from_slice(name.as_bytes());
    request.extend_from_slice(b": ");
    request.extend_from_slice(value);
    request.extend_from_slice(b"\r\n");
}

/// The internal HTTP/1.1 request `proxy_http_to_peer` writes. It is a separate
/// function so the forwarded-header contract can be asserted without a peer.
fn forwarded_request(
    method: &str,
    path_and_query: &str,
    host: &str,
    subject: &str,
    subject_signature: Option<&str>,
    content_type: &str,
    body_len: usize,
    idempotency_key: Option<&str>,
    read: ForwardedRead<'_>,
) -> Vec<u8> {
    let mut request = Vec::new();
    request.extend_from_slice(
        format!(
            "{method} {path_and_query} HTTP/1.1\r\nHost: {host}\r\nx-yadb-subject: {subject}\r\n"
        )
        .as_bytes(),
    );
    if let Some(signature) = subject_signature {
        push_header(
            &mut request,
            "x-yadb-subject-signature",
            signature.as_bytes(),
        );
    }
    if let Some(key) = idempotency_key {
        push_header(&mut request, "Idempotency-Key", key.as_bytes());
    }
    // A proxied read must ask the leader for the same byte range the caller
    // asked for: without this header the leader would answer 200 with the whole
    // object (or block) instead of the requested 206 slice.
    if let Some(range) = read.range {
        push_header(&mut request, "Range", range.as_bytes());
    }
    // Conditional reads are decided by the node that reads the manifest, so a
    // forwarded read carries the caller's conditions instead of evaluating them
    // against the follower's possibly lagging metadata.
    if let Some(value) = read.if_match {
        push_header(&mut request, "If-Match", value);
    }
    if let Some(value) = read.if_none_match {
        push_header(&mut request, "If-None-Match", value);
    }
    request.extend_from_slice(
        format!(
            "x-yadb-forwarded: 1\r\nContent-Type: {content_type}\r\nContent-Length: {body_len}\r\nConnection: close\r\n\r\n"
        )
        .as_bytes(),
    );
    request
}

/// A forwarding attempt that failed.
///
/// `sent` records whether any byte of the request reached the peer. A connect
/// failure provably did not run the mutation, so a client may retry it; a
/// failure after the first write leaves the outcome unknown, so a mutation must
/// never be retried automatically. Collapsing the two would either lose writes
/// (refusing a request that never left) or duplicate them (retrying one that
/// may have committed).
#[derive(Debug)]
struct ForwardFailure {
    message: String,
    sent: bool,
}

impl ForwardFailure {
    fn not_sent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            sent: false,
        }
    }
    fn unknown(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            sent: true,
        }
    }
    /// The outcome label a client sees; only `unknown` forbids a retry.
    fn outcome(&self) -> &'static str {
        if self.sent {
            "unknown"
        } else {
            "not-sent"
        }
    }
}

/// How long a Follower waits for the Leader's answer to a forwarded request.
///
/// This is a client-visible budget, not a retry policy: when it expires the
/// request has already been delivered, so the outcome is reported as `unknown`
/// and is never resent. It therefore has to exceed the Leader's legitimate
/// service time under load, or a slow-but-successful mutation is reported as an
/// ambiguous failure.
///
/// Measured on a three-node local cluster: one SQL statement takes ~130 ms
/// (the engine serializes statements on a single lock and each metadata
/// mutation costs a synchronous two-peer Raft round), and one S3 PUT ~160 ms. A
/// 24-way burst through a Follower therefore needs ~4-5 s, which is why the
/// previous 5 s budget intermittently refused writes that were committing
/// normally. Reads keep a short budget: a read that is merely slow can be
/// failed over and retried, while a mutation cannot.
const FORWARD_WRITE_REPLY_BUDGET: Duration = Duration::from_secs(20);
const FORWARD_READ_REPLY_BUDGET: Duration = Duration::from_secs(5);

/// Raw HTTP/1.1 forwarding helper used by Followers to transparently proxy client writes to the Raft Leader.
fn proxy_http_to_peer(
    peer_addr: &str,
    method: &str,
    path_and_query: &str,
    subject: &str,
    subject_signature: Option<&str>,
    content_type: &str,
    body: &[u8],
    idempotency_key: Option<&str>,
    read: ForwardedRead<'_>,
) -> Result<ProxyResponse, ForwardFailure> {
    let clean = peer_addr
        .trim()
        .strip_prefix("http://")
        .unwrap_or(peer_addr.trim())
        .trim_end_matches('/');
    let sock: SocketAddr = clean
        .parse()
        .map_err(|e| ForwardFailure::not_sent(format!("invalid peer address: {e}")))?;

    // Nothing has been written yet, so every failure up to the first `write_all`
    // is `not_sent`: the leader cannot have executed anything.
    let mut stream = TcpStream::connect_timeout(&sock, Duration::from_millis(400))
        .map_err(|e| ForwardFailure::not_sent(format!("connect to leader failed: {e}")))?;
    let reply_budget = if method == "GET" || method == "HEAD" {
        FORWARD_READ_REPLY_BUDGET
    } else {
        FORWARD_WRITE_REPLY_BUDGET
    };
    stream
        .set_read_timeout(Some(reply_budget))
        .map_err(|e| ForwardFailure::not_sent(format!("socket setup failed: {e}")))?;
    stream
        .set_write_timeout(Some(Duration::from_millis(3000)))
        .map_err(|e| ForwardFailure::not_sent(format!("socket setup failed: {e}")))?;

    let req = forwarded_request(
        method,
        path_and_query,
        clean,
        subject,
        subject_signature,
        content_type,
        body.len(),
        idempotency_key,
        read,
    );
    // From here on a partial write may have delivered a prefix of the request,
    // and a delivered request may commit even when the reply never arrives.
    stream
        .write_all(&req)
        .map_err(|e| ForwardFailure::unknown(format!("sending request to leader failed: {e}")))?;
    if !body.is_empty() {
        stream
            .write_all(body)
            .map_err(|e| ForwardFailure::unknown(format!("sending body to leader failed: {e}")))?;
    }
    stream
        .flush()
        .map_err(|e| ForwardFailure::unknown(format!("flushing request to leader failed: {e}")))?;

    let mut raw = Vec::new();
    stream
        .take((MAX_PROXY_WIRE + 1) as u64)
        .read_to_end(&mut raw)
        .map_err(|e| ForwardFailure::unknown(format!("reading leader response failed: {e}")))?;
    if raw.len() > MAX_PROXY_WIRE {
        return Err(ForwardFailure::unknown("leader response too large"));
    }

    parse_proxy_response_for(&raw, method == "HEAD")
        .map_err(|e| ForwardFailure::unknown(format!("malformed response from leader: {e}")))
}

fn parse_proxy_response(raw: &[u8]) -> Result<ProxyResponse, std::io::Error> {
    parse_proxy_response_for(raw, false)
}

/// Parses a leader response. `head` marks a response to a `HEAD` request: it
/// reports the representation's length but never carries it, so an empty
/// payload is complete rather than a truncated body.
fn parse_proxy_response_for(raw: &[u8], head: bool) -> Result<ProxyResponse, std::io::Error> {
    let invalid = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed HTTP response from Raft Leader",
        )
    };
    let Some(hdr_end) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed HTTP response from Raft Leader",
        ));
    };
    if hdr_end + 4 > MAX_PROXY_HEADERS {
        return Err(invalid());
    }
    let hdr_str = std::str::from_utf8(&raw[..hdr_end]).map_err(|_| invalid())?;
    let status_line = hdr_str.split("\r\n").next().ok_or_else(invalid)?;
    let bytes = status_line.as_bytes();
    if bytes.len() < 13
        || !matches!(&bytes[..8], b"HTTP/1.0" | b"HTTP/1.1")
        || bytes[8] != b' '
        || !bytes[9..12].iter().all(|b| b.is_ascii_digit())
        || bytes[12] != b' '
        || bytes[13..]
            .iter()
            .any(|b| *b < 32 && *b != b'\t' || *b == 127)
    {
        return Err(invalid());
    }
    let status_code: u16 = status_line[9..12].parse().map_err(|_| invalid())?;

    let mut resp_ct = "application/json".to_string();
    let mut resp_etag = None;
    if !(200..=599).contains(&status_code) {
        return Err(invalid());
    }
    let mut is_chunked = false;
    let mut content_length = None;
    let mut write_outcome = None;
    let mut retry = None;
    let mut headers = Vec::new();
    for line in hdr_str.split("\r\n").skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            axum::http::HeaderName::from_bytes(k.as_bytes()).map_err(|_| invalid())?;
            HeaderValue::from_str(v.trim()).map_err(|_| invalid())?;
            let kl = k.trim().to_ascii_lowercase();
            let vt = v.trim();
            if kl == "content-type" {
                resp_ct = vt.to_string();
            } else if kl == "etag" {
                resp_etag = Some(vt.to_string());
            } else if kl == "transfer-encoding" {
                if is_chunked || !vt.eq_ignore_ascii_case("chunked") {
                    return Err(invalid());
                }
                is_chunked = true;
            } else if kl == "content-length" {
                if vt.is_empty() || !vt.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(invalid());
                }
                let len: usize = vt.parse().map_err(|_| invalid())?;
                if content_length.replace(len).is_some_and(|old| old != len) {
                    return Err(invalid());
                }
            } else if kl == "x-yadb-write-outcome" {
                if write_outcome.replace(vt.to_string()).is_some() {
                    return Err(invalid());
                }
            } else if kl == "x-yadb-retry" {
                if retry.replace(vt.to_string()).is_some() {
                    return Err(invalid());
                }
            } else if matches!(
                kl.as_str(),
                "content-range"
                    | "last-modified"
                    | "accept-ranges"
                    | "cache-control"
                    | "content-disposition"
            ) {
                if headers.iter().any(|(name, _)| name == &kl) {
                    return Err(invalid());
                }
                // HeaderName and HeaderValue validation above also applies to
                // the allowlisted headers before they are retained.
                headers.push((kl, vt.to_string()));
            }
        } else {
            return Err(invalid());
        }
    }

    let payload_slice = &raw[hdr_end + 4..];
    // A 304 carries no content and a peer may omit `Content-Length` for it. It
    // is not a close-delimited body: any payload at all is malformed.
    if status_code == StatusCode::NOT_MODIFIED.as_u16() {
        if !payload_slice.is_empty() {
            return Err(invalid());
        }
        return Ok(ProxyResponse {
            code: status_code,
            content_type: resp_ct,
            etag: resp_etag,
            write_outcome,
            retry,
            headers,
            content_length: None,
            body: Vec::new(),
        });
    }
    if head {
        // A HEAD answer must not carry bytes. Its `Content-Length` is the
        // length of the representation the GET would return, so it is kept and
        // checked against nothing.
        if !payload_slice.is_empty() || is_chunked {
            return Err(invalid());
        }
        return Ok(ProxyResponse {
            code: status_code,
            content_type: resp_ct,
            etag: resp_etag,
            write_outcome,
            retry,
            headers,
            content_length,
            body: Vec::new(),
        });
    }
    if is_chunked && content_length.is_some() {
        return Err(invalid());
    }
    // Do not accept close-delimited responses: a dropped connection must not
    // be mistaken for a complete write response.
    if !is_chunked && content_length.is_none() {
        return Err(invalid());
    }
    let decoded = if is_chunked {
        decode_http_chunked(payload_slice)?
    } else {
        if content_length.is_some_and(|len| len != payload_slice.len()) {
            return Err(invalid());
        }
        if payload_slice.len() > MAX_PROXY_BODY {
            return Err(invalid());
        }
        payload_slice.to_vec()
    };
    Ok(ProxyResponse {
        code: status_code,
        content_type: resp_ct,
        etag: resp_etag,
        write_outcome,
        retry,
        headers,
        content_length,
        body: decoded,
    })
}

fn decode_http_chunked(mut input: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = input.windows(2).position(|w| w == b"\r\n") else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing chunk size or terminator",
            ));
        };
        if line_end > MAX_PROXY_HEADERS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk header too large",
            ));
        }
        let size_str = std::str::from_utf8(&input[..line_end])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if size_str.is_empty() || !size_str.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid chunk size",
            ));
        }
        let chunk_len = usize::from_str_radix(size_str, 16)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        input = &input[line_end + 2..];
        if chunk_len == 0 {
            let mut trailer_bytes = 0;
            loop {
                let end = input.windows(2).position(|w| w == b"\r\n").ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "truncated chunk trailers",
                    )
                })?;
                trailer_bytes += end + 2;
                if trailer_bytes > MAX_PROXY_HEADERS {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "chunk trailers too large",
                    ));
                }
                if end == 0 {
                    if input.len() != 2 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "extra bytes after chunk trailers",
                        ));
                    }
                    return Ok(out);
                }
                let line = std::str::from_utf8(&input[..end])
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let (name, value) = line.split_once(':').ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid trailer")
                })?;
                axum::http::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                HeaderValue::from_str(value.trim())
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                input = &input[end + 2..];
            }
        }
        if chunk_len > MAX_PROXY_BODY.saturating_sub(out.len()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked body too large",
            ));
        }
        if input.len() < chunk_len + 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated chunked response",
            ));
        }
        if &input[chunk_len..chunk_len + 2] != b"\r\n" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid chunk delimiter",
            ));
        }
        out.extend_from_slice(&input[..chunk_len]);
        input = &input[chunk_len + 2..];
    }
}

/// Forward by the decoded project supplied by the handler. Only legacy mode
/// retries; an OpenRaft forwarding failure has an unknown outcome.
fn forward_error_response(message: String, outcome: Option<&'static str>) -> Response {
    let mut response = api_err(StatusCode::SERVICE_UNAVAILABLE, message);
    if let Some(outcome) = outcome {
        response
            .headers_mut()
            .insert("x-yadb-write-outcome", HeaderValue::from_static(outcome));
        if outcome == "unknown" {
            response
                .headers_mut()
                .insert("x-yadb-retry", HeaderValue::from_static("do-not-retry"));
        }
    }
    response
}

async fn maybe_forward_write(
    st: &AppState,
    headers: &HeaderMap,
    project: Option<&str>,
    method: &'static str,
    path_and_query: String,
    content_type: String,
    body: Bytes,
) -> Option<Response> {
    if headers.contains_key("x-yadb-forwarded") {
        // A forwarded request must never fall through to a local mutation on
        // a follower (or on the leader of another project group).  Resolve
        // the addressed group from the locally applied control view and fail
        // closed if it is unavailable or the local role does not match. This
        // does not establish that the view is fresh.
        let addressed = if st.meta.is_openraft() {
            match project {
                Some(name) => {
                    let cp = match st.control_for_authorization_async().await {
                        Ok(cp) => cp,
                        Err(e) => {
                            return Some(forward_error_response(e.to_string(), Some("not-sent")))
                        }
                    };
                    let Some((id, _)) = cp.projects.get(name) else {
                        return Some(api_err(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "forwarded request has no known project-group leader",
                        ));
                    };
                    match st.meta.role_info_for_key(&format!("/db/{id}/")) {
                        Ok(info) => info,
                        Err(e) => {
                            return Some(api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))
                        }
                    }
                }
                None => st.meta.role_info(),
            }
        } else {
            st.meta.role_info()
        };
        if addressed.1 == RoleState::Leader {
            return None;
        }
        let mut response = api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "forwarded request reached a node that is not the addressed project-group leader",
        );
        if let Some(leader) = addressed.3 {
            if let Ok(value) = HeaderValue::from_str(&leader.to_string()) {
                response.headers_mut().insert("x-yadb-leader-id", value);
            }
        }
        return Some(response);
    }
    let route = if st.meta.is_openraft() && project.is_some() {
        let project = project.unwrap();
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return Some(forward_error_response(e.to_string(), Some("not-sent"))),
        };
        match cp.projects.get(project) {
            Some((id, _)) => Some(format!("/db/{id}/")),
            None => {
                return Some(api_err(
                    StatusCode::NOT_FOUND,
                    "unknown project or unsupported OpenRaft route",
                ))
            }
        }
    } else {
        None
    };
    let role_info = match route.as_deref() {
        Some(key) => st.meta.role_info_for_key(key),
        None => Ok(st.meta.role_info()),
    };
    let (_, role, _, _, _) = match role_info {
        Ok(info) => info,
        Err(e) => {
            return Some(forward_error_response(
                e.to_string(),
                st.meta.is_openraft().then_some("not-sent"),
            ))
        }
    };
    if role == RoleState::Leader {
        return None;
    }

    let subject = extract_subject(headers, st.subject_secret.as_deref());
    // Forward the caller's subject signature so the leader verifies the same
    // identity instead of trusting a forwarded plain header.
    let subject_signature = st.subject_secret.as_deref().and_then(|secret| {
        headers
            .get("x-yadb-subject-signature")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .or_else(|| {
                verified_subject(headers, Some(secret))
                    .map(|verified| subject_signature(secret, &verified))
            })
    });
    let meta = st.meta.clone();
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    // The local evaluator reads the combined value of every field line, so the
    // forwarded request carries exactly that value (byte for byte) and the
    // leader reaches the same decision.
    let if_match = joined_field_value(headers, &header::IF_MATCH);
    let if_none_match = joined_field_value(headers, &header::IF_NONE_MATCH);

    let openraft = meta.is_openraft();
    let outcome = tokio::task::spawn_blocking(move || {
        if meta.is_openraft() {
            let (id, role, _, leader, _) = match route.as_deref() {
                Some(key) => meta
                    .role_info_for_key(key)
                    .map_err(|e| (e.to_string(), Some("not-sent")))?,
                None => meta.role_info(),
            };
            if role == RoleState::Leader {
                return Ok(None);
            }
            let target = leader.filter(|leader| *leader != id).ok_or((
                "OpenRaft leader unknown; request not sent".to_string(),
                Some("not-sent"),
            ))?;
            let addr = meta.peer_addr(target).ok_or((
                "OpenRaft leader public address missing; request not sent".to_string(),
                Some("not-sent"),
            ))?;
            return proxy_http_to_peer(
                &addr,
                method,
                &path_and_query,
                &subject,
                subject_signature.as_deref(),
                &content_type,
                &body,
                idempotency_key.as_deref(),
                ForwardedRead {
                    range: range.as_deref(),
                    if_match: if_match.as_deref(),
                    if_none_match: if_none_match.as_deref(),
                },
            )
            .map(|response| Some((target, response)))
            .map_err(|e| {
                // A connect failure never reached the leader, so the mutation
                // provably did not run and the client may retry it.
                if e.sent {
                    (
                        format!("OpenRaft forwarding failed; write outcome unknown, do not automatically retry: {}", e.message),
                        Some("unknown"),
                    )
                } else {
                    (
                        format!("OpenRaft leader unreachable; request not sent: {}", e.message),
                        Some("not-sent"),
                    )
                }
            });
        }
        // The last attempt that never left this node, so a final refusal can
        // say "not sent" (safe to retry) instead of leaving the outcome blank.
        let mut unsent: Option<ForwardFailure> = None;
        for attempt in 0..3 {
            let (my_id, cur_role, _, leader_opt, _) = meta.role_info();
            if cur_role == RoleState::Leader {
                return Ok(None);
            }
            let target_id = match leader_opt {
                Some(id) if id != my_id => id,
                _ => {
                    // No known leader yet. OpenRaft runs its own election, so
                    // wait briefly and re-read the control group's leader.
                    let (_, new_role, _, new_leader, _) = meta.role_info();
                    if new_role == RoleState::Leader {
                        return Ok(None);
                    }
                    if let Some(lid) = new_leader {
                        lid
                    } else {
                        std::thread::sleep(Duration::from_millis(120 * (attempt + 1)));
                        continue;
                    }
                }
            };

            let Some(peer_addr) = meta.peer_addr(target_id) else {
                break;
            };

            match proxy_http_to_peer(
                &peer_addr,
                method,
                &path_and_query,
                &subject,
                subject_signature.as_deref(),
                &content_type,
                &body,
                idempotency_key.as_deref(),
                ForwardedRead {
                    range: range.as_deref(),
                    if_match: if_match.as_deref(),
                    if_none_match: if_none_match.as_deref(),
                },
            ) {
                Ok(ref response)
                    if response.code == 409
                        && String::from_utf8_lossy(&response.body).contains("not Raft leader") =>
                {
                    // The control group's leader moved. OpenRaft elects on its
                    // own; retry against the leader reported by the next read.
                    continue;
                }
                Ok(ok_tuple) => return Ok(Some((target_id, ok_tuple))),
                Err(e) if !e.sent => {
                    // The request provably never reached the leader, so nothing
                    // committed: retrying inside this loop is safe for both
                    // reads and mutations, and a mutation that still cannot be
                    // forwarded is reported as not sent rather than unknown.
                    unsent = Some(e);
                    let (_, new_role, _, _, _) = meta.role_info();
                    if new_role == RoleState::Leader {
                        return Ok(None);
                    }
                }
                Err(e) => {
                    // A lost response to a mutation is not evidence that it did
                    // not commit, and an unkeyed write cannot be replayed.
                    if method != "GET" && method != "HEAD" {
                        return Err((
                            format!(
                                "forwarded request was sent but the write outcome is unknown; do not automatically retry: {}",
                                e.message
                            ),
                            Some("unknown"),
                        ));
                    }
                    // Read-only requests can safely retry against the leader
                    // the control group reports next.
                    let (_, new_role, _, _, _) = meta.role_info();
                    if new_role == RoleState::Leader {
                        return Ok(None);
                    }
                }
            }
        }
        match unsent {
            Some(e) => Err((
                format!(
                    "cluster has no reachable Raft leader (quorum unavailable); request not sent: {}",
                    e.message
                ),
                Some("not-sent"),
            )),
            None => Err((
                "cluster has no reachable Raft leader (quorum unavailable)".to_string(),
                None,
            )),
        }
    })
    .await;

    match outcome {
        Ok(Ok(None)) => None, // This node just became Leader; execute locally!
        Ok(Ok(Some((leader_id, response)))) => {
            let status =
                StatusCode::from_u16(response.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let mut builder = Response::builder()
                .status(status)
                .header(header::CONTENT_TYPE, response.content_type)
                .header("x-yadb-leader-id", leader_id.to_string());
            if let Some(etag) = response.etag {
                builder = builder.header(header::ETAG, etag);
            }
            if let Some(value) = response.write_outcome {
                builder = builder.header("x-yadb-write-outcome", value);
            }
            if let Some(value) = response.retry {
                builder = builder.header("x-yadb-retry", value);
            }
            // A HEAD answer keeps the leader's declared representation length;
            // axum would otherwise report the empty body's length (zero).
            if method == "HEAD" {
                if let Some(length) = response.content_length {
                    builder = builder.header(header::CONTENT_LENGTH, length);
                }
            }
            for (name, value) in response.headers {
                builder = builder.header(name, value);
            }
            Some(
                builder
                    .body(axum::body::Body::from(response.body))
                    .unwrap_or_else(|e| api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
            )
        }
        Ok(Err((msg, certainty))) => Some(forward_error_response(msg, certainty)),
        Err(e) if openraft => Some(forward_error_response(
            format!("forwarding task failed; write outcome unknown: {e}"),
            Some("unknown"),
        )),
        Err(e) => Some(api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// Parses a single `bytes=start-end` range against a known size.
///
/// An end past the end of the resource is clamped: RFC 9110 defines such a
/// range as satisfiable and denoting the remainder of the representation, and
/// the native S3 listener has always answered it with `206` and a clamped
/// `Content-Range`. Only a range that starts at or after the size, or is
/// reversed, is unsatisfiable.
fn parse_range_header(val: &str, total_size: u64) -> Option<(u64, u64)> {
    let spec = val.trim().strip_prefix("bytes=")?;
    let (start_s, end_s) = spec.split_once('-')?;
    let start: u64 = start_s.parse().ok()?;
    let end: u64 = if end_s.is_empty() {
        total_size.checked_sub(1)?
    } else {
        end_s.parse().ok()?
    };
    if total_size == 0 || start >= total_size || start > end {
        return None;
    }
    Some((start, end.min(total_size - 1)))
}

/// Formats a manifest timestamp as an HTTP date, the same way the native S3
/// listener does, so a proxied read keeps `Last-Modified`.
pub(crate) fn http_date(timestamp: u64) -> String {
    chrono::DateTime::from_timestamp(timestamp.min(i64::MAX as u64) as i64, 0)
        .unwrap_or_default()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

/// Largest slice the S3 read paths serve in one response. The native listener
/// has always refused a larger read (`400 InvalidRequest`) and the forwarding
/// wire has the same bound, so both the local and the proxied read enforce it
/// before any fragment is fetched.
pub(crate) const S3_READ_CAP: u64 = 16 * 1024 * 1024;

/// What a read of `size` bytes may fetch for the given `Range` header.
pub(crate) enum RangeDecision {
    /// Serve this slice, or the whole object when `None`.
    Serve(Option<(u64, u64)>),
    /// The range starts at or past the size, or is reversed: `416`.
    Unsatisfiable,
    /// The slice exceeds `S3_READ_CAP`: `400 InvalidRequest`.
    TooLarge,
}

/// The range rule shared by the native listener and the gateway. It is decided
/// against one manifest, so the slice description and the bytes always agree.
pub(crate) fn range_decision(range: Option<&str>, size: u64) -> RangeDecision {
    let parsed = match range {
        Some(value) => match parse_range_header(value, size) {
            Some(range) => Some(range),
            None => return RangeDecision::Unsatisfiable,
        },
        None => None,
    };
    if size > S3_READ_CAP && parsed.is_none() {
        return RangeDecision::TooLarge;
    }
    if let Some((start, end)) = parsed {
        if end - start + 1 > S3_READ_CAP {
            return RangeDecision::TooLarge;
        }
    }
    RangeDecision::Serve(parsed)
}

/// Outcome of `If-Match`/`If-None-Match` for a read that produced `etag`.
pub(crate) enum Conditional {
    Passed,
    /// `If-Match` did not name the object version: `412`.
    Failed,
    /// `If-None-Match` named the object version: `304`.
    NotModified,
}

/// One RFC 9110 entity-tag: its opaque value and whether it carried the `W/`
/// weak marker.
#[derive(Clone, Copy)]
struct EntityTag<'a> {
    opaque: &'a [u8],
    weak: bool,
}

/// Trims optional whitespace (SP / HTAB), the only whitespace RFC 9110 allows
/// around list elements.
fn trim_ows(mut value: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = value {
        if *first == b' ' || *first == b'\t' {
            value = rest;
        } else {
            break;
        }
    }
    while let [rest @ .., last] = value {
        if *last == b' ' || *last == b'\t' {
            value = rest;
        } else {
            break;
        }
    }
    value
}

/// Splits one `entity-tag` off the front of `value`, returning it and the
/// remainder.
///
/// `entity-tag = [ "W/" ] DQUOTE *etagc DQUOTE`, and `etagc` excludes `DQUOTE`,
/// space and control bytes. The first closing quote therefore ends the tag, so a
/// comma inside the quotes is part of the opaque value and not a separator. The
/// value is handled as bytes, not `str`: `etagc` includes `obs-text`, which is
/// not valid UTF-8, and such a tag is still a tag that simply does not match a
/// stored ASCII one.
fn split_entity_tag(value: &[u8]) -> Option<(EntityTag<'_>, &[u8])> {
    let value = trim_ows(value);
    let (weak, value) = match value.strip_prefix(b"W/") {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    let value = value.strip_prefix(b"\"")?;
    let end = value.iter().position(|b| *b == b'"')?;
    let opaque = &value[..end];
    if !opaque
        .iter()
        .all(|b| matches!(*b, 0x21 | 0x23..=0x7e | 0x80..=0xff))
    {
        return None;
    }
    Some((EntityTag { opaque, weak }, &value[end + 1..]))
}

/// A parsed `If-Match`/`If-None-Match` value: `*`, or a non-empty list of
/// entity-tags.
enum ETagCondition<'a> {
    /// `*`: the condition is about the representation existing, not about a
    /// particular version of it.
    Any,
    Tags(Vec<EntityTag<'a>>),
}

/// Parses a conditional value as RFC 9110 `*` / `#entity-tag`.
///
/// Empty list elements are skipped, as RFC 9110 requires of a recipient: a
/// trailing comma or `"a", , "b"` is a valid list, not a reason to drop the
/// whole condition. `None` means the field is not a condition at all (not `*`,
/// no parsable entity-tag). What that means is decided by the caller, because it
/// differs per header: `If-Match` is true only on a match, `If-None-Match` is
/// false only on a match.
fn parse_etag_condition(value: &[u8]) -> Option<ETagCondition<'_>> {
    if trim_ows(value) == b"*" {
        return Some(ETagCondition::Any);
    }
    let mut tags = Vec::new();
    let mut rest = value;
    loop {
        rest = trim_ows(rest);
        if rest.is_empty() {
            break;
        }
        if let Some(tail) = rest.strip_prefix(b",") {
            rest = tail;
            continue;
        }
        let (tag, tail) = split_entity_tag(rest)?;
        tags.push(tag);
        rest = trim_ows(tail);
        if rest.is_empty() {
            break;
        }
        rest = rest.strip_prefix(b",")?;
    }
    (!tags.is_empty()).then_some(ETagCondition::Tags(tags))
}

/// Whether the combined value of `name` matches `current`.
///
/// `strong` selects RFC 9110 strong comparison (`If-Match`) or weak comparison
/// (`If-None-Match`). A value that is not a readable condition matches nothing:
/// what that means is decided by the caller.
fn condition_matches(
    headers: &HeaderMap,
    name: &HeaderName,
    current: Option<EntityTag<'_>>,
    strong: bool,
) -> bool {
    let Some(value) = joined_field_value(headers, name) else {
        return false;
    };
    match parse_etag_condition(&value) {
        Some(ETagCondition::Any) => true,
        Some(ETagCondition::Tags(tags)) => tags.iter().any(|candidate| {
            current.is_some_and(|tag| {
                if strong {
                    strong_match(candidate, &tag)
                } else {
                    weak_match(candidate, &tag)
                }
            })
        }),
        None => false,
    }
}

/// The manifest ETag as an entity-tag, or `None` when the stored value is not a
/// well-formed entity-tag.
///
/// A malformed stored validator is not repaired into a match: it never
/// satisfies a condition, so `If-Match` refuses and `If-None-Match` serves the
/// bytes. Nothing this engine writes produces such a value, and refusing to
/// invent a match keeps a corrupted manifest from answering a conditional read
/// as if it were the version the caller named.
fn manifest_tag(etag: &str) -> Option<EntityTag<'_>> {
    let (tag, rest) = split_entity_tag(etag.as_bytes())?;
    trim_ows(rest).is_empty().then_some(tag)
}

/// RFC 9110 strong comparison: both tags must be strong and equal.
fn strong_match(candidate: &EntityTag<'_>, current: &EntityTag<'_>) -> bool {
    !candidate.weak && !current.weak && candidate.opaque == current.opaque
}

/// RFC 9110 weak comparison: the opaque values are equal, whatever either side's
/// weak marker says.
fn weak_match(candidate: &EntityTag<'_>, current: &EntityTag<'_>) -> bool {
    candidate.opaque == current.opaque
}

/// Evaluates the conditional headers against the ETag a read actually
/// produced. The native listener renders this as an S3 XML error and the
/// gateway as an API error, but both must decide it from the same manifest the
/// bytes came from.
///
/// `If-Match` uses RFC 9110's strong comparison and `If-None-Match` its weak
/// comparison, and both support `*` and a list of tags. `*` matches because the
/// representation exists by construction (a manifest was read). A field that is
/// present but cannot be read fails `If-Match` — it never turns into an
/// unconditional read — while for `If-None-Match` it names no version that can
/// be confirmed, so it does not suppress the bytes.
pub(crate) fn evaluate_conditions(headers: &HeaderMap, etag: &str) -> Conditional {
    let current = manifest_tag(etag);
    if headers.contains_key(header::IF_MATCH)
        && !condition_matches(headers, &header::IF_MATCH, current, true)
    {
        return Conditional::Failed;
    }
    if headers.contains_key(header::IF_NONE_MATCH)
        && condition_matches(headers, &header::IF_NONE_MATCH, current, false)
    {
        return Conditional::NotModified;
    }
    Conditional::Passed
}

async fn handle_ui() -> Html<&'static str> {
    Html(EMBEDDED_UI_HTML)
}

async fn handle_sample_yaml() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/yaml; charset=utf-8")],
        EMBEDDED_SAMPLE_YAML,
    )
}

async fn handle_health(State(st): State<AppState>) -> impl IntoResponse {
    let gen = st.control.read().unwrap().generation;
    let rev = if st.meta.is_openraft() {
        None
    } else {
        match st.meta.try_revision() {
            Ok(revision) => Some(revision),
            Err(error) => {
                return api_err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("metadata revision cannot be read: {error}"),
                )
                .into_response()
            }
        }
    };
    let (node_id, role, term, leader_id, commit_index) = st.meta.role_info();
    Json(serde_json::json!({
        "status": "ok",
        "metadata_runtime": if st.meta.is_openraft() { "openraft" } else { "single-node" },
        "role_scope": if st.meta.is_openraft() { "control_group; see /openraft/status for all groups" } else { "single" },
        "production_ready": false,
        "node_id": node_id,
        "role": format!("{role:?}"),
        "term": term,
        "leader_id": leader_id,
        "commit_index": commit_index,
        "generation": gen,
        "metakv_revision": rev
    }))
    .into_response()
}

async fn handle_raft_status(State(st): State<AppState>) -> impl IntoResponse {
    // `term` and `commit_index` are the control group's: every deployment has
    // one, and a data group's term says nothing about the control plane. A
    // single-node store has no term at all, so it reports `null` rather than a
    // made-up number.
    let (node_id, role, term, leader_id, commit_index) = st.meta.role_info();
    Json(serde_json::json!({
        "metadata_runtime": if st.meta.is_openraft() { "openraft" } else { "single-node" },
        "production_ready": false,
        "node_id": node_id,
        "role": format!("{role:?}"),
        "term": if st.meta.is_openraft() { Some(term) } else { None },
        "leader_id": leader_id,
        "commit_index": commit_index,
    }))
}

/// Cross-group OpenRaft observations.
///
/// The body names every local group, so a deployment that configured
/// `--subject-secret` requires a verified signed subject. Without the secret no
/// subject can be verified and the legacy open route is kept.
async fn handle_openraft_status(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(secret) = st.subject_secret.as_deref() {
        if verified_subject(&headers, Some(secret)).is_none() {
            return api_err(
                StatusCode::UNAUTHORIZED,
                "OpenRaft status requires a verified signed subject",
            );
        }
    }
    // Transport counters are process-wide, not per group: a node has one peer
    // listener per group but one set of sockets. They are reported next to the
    // group observations because a connection failure has no Raft error that
    // names it, so this is the only place an operator can see one.
    let transport = yadb_metakv::openraft_net::transport_counters();
    Json(match &st.openraft {
        Some(status) => {
            let mut snapshot = status.snapshot();
            if let Some(object) = snapshot.as_object_mut() {
                object.insert("transport".into(), transport);
            }
            snapshot
        }
        None => serde_json::json!({
            "enabled": false,
            "client_traffic": "none",
            "production_ready": false,
            "transport": transport,
        }),
    })
    .into_response()
}

#[derive(Serialize)]
struct FreshControlResponse {
    revision: u64,
    applied_index: u64,
    control: ControlPlaneState,
}

/// Internal, token-authenticated control snapshot. Only the control-group
/// leader can answer because the read performs a quorum-confirmed barrier.
async fn handle_internal_control_snapshot(
    State(st): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let Some(expected) = st.raft_token.as_deref().filter(|token| !token.is_empty()) else {
        return api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "internal control snapshot requires a configured Raft token",
        );
    };
    if !raft_token_matches(&headers, Some(expected)) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    if !st.meta.is_openraft() {
        return api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "internal control snapshot requires OpenRaft metadata",
        );
    }
    let meta = st.meta.clone();
    let joined = tokio::task::spawn_blocking(move || {
        let snapshot = meta.try_linearizable_control_snapshot(&[CONTROL_STATE_KEY.to_string()])?;
        let revision = snapshot.revision();
        let control = match snapshot.get(CONTROL_STATE_KEY) {
            Some(value) => serde_json::from_slice::<ControlPlaneState>(&value.value)?,
            None => ControlPlaneState::new(),
        };
        Ok::<_, MetaError>((revision, control))
    })
    .await;
    match joined {
        Ok(Ok((revision, control))) => {
            let applied_index = st.meta.role_info().4;
            Json(FreshControlResponse {
                revision,
                applied_index,
                control,
            })
            .into_response()
        }
        Ok(Err(MetaError::NotLeader { leader })) => {
            let mut response = api_err(
                StatusCode::CONFLICT,
                "control group is not led by this node",
            );
            if let Some(leader) = leader {
                if let Ok(value) = HeaderValue::from_str(&leader.to_string()) {
                    response.headers_mut().insert("x-yadb-leader-id", value);
                }
            }
            response
        }
        Ok(Err(e)) => api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Constant-time comparison of the internal Raft token.
fn raft_token_matches(headers: &axum::http::HeaderMap, expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        // No token configured: single-node mode without peers.
        return true;
    };
    // An explicitly empty token is a misconfiguration, not a wildcard: the
    // absent header also decodes to the empty string, so accepting it would
    // authenticate every caller.
    if expected.is_empty() {
        return false;
    }
    let provided = headers
        .get("x-yadb-raft-token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided.len() != expected.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in provided.bytes().zip(expected.bytes()) {
        diff |= a ^ b;
    }
    diff == 0
}

async fn handle_status(State(st): State<AppState>) -> impl IntoResponse {
    let cp = st.control.read().unwrap().clone();
    Json(cp)
}

async fn handle_plan(State(st): State<AppState>, Json(req): Json<ManifestPayload>) -> Response {
    let manifests = match parse_manifests(&req.yaml) {
        Ok(m) => m,
        Err(e) => return api_err(StatusCode::BAD_REQUEST, e.to_string()),
    };
    let cur = st.control.read().unwrap().clone();
    match cur.plan(&manifests) {
        Ok((ops, next)) => Json(PlanResponse {
            current_generation: cur.generation,
            next_generation: next.generation,
            ops,
        })
        .into_response(),
        Err(e) => api_err(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct RecordedResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body_base64: String,
}

#[derive(Serialize, Deserialize)]
struct IdempotencyRecord {
    version: u8,
    fingerprint: String,
    outcome: String,
    response: Option<RecordedResponse>,
}

/// Replicated admission counter for idempotency records.
///
/// Every keyed `/v1/apply` and every `resolve` of a fresh key adds one permanent
/// record to the control group. Without a bound the state image eventually
/// exceeds `MAX_IMAGE` (64 MiB) in the metadata state machine, and an entry that
/// cannot be serialized does not get rejected cleanly: `apply` rolls back and
/// returns a storage error, so the control group stops advancing through the
/// committed entry. That would take down authorization refresh along with it.
///
/// The budget is deliberately far below 64 MiB so the counter trips while the
/// image is still serializable. `MAX_IDEMPOTENCY_BYTES` is the binding limit;
/// the record count is a second, cheaper guard for tiny records.
const IDEMPOTENCY_USAGE_KEY: &str = "/sys/idempotency/_usage";
const MAX_IDEMPOTENCY_RECORDS: u64 = 10_000;
const MAX_IDEMPOTENCY_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Default, Serialize, Deserialize)]
struct IdempotencyUsage {
    #[serde(default)]
    version: u32,
    #[serde(default)]
    records: u64,
    #[serde(default)]
    bytes: u64,
}

impl IdempotencyUsage {
    fn admits(&self, record_len: usize) -> bool {
        self.records < MAX_IDEMPOTENCY_RECORDS
            && self.bytes.saturating_add(record_len as u64) <= MAX_IDEMPOTENCY_BYTES
    }
}

/// Reads the admission counter and the revision it must be guarded at.
///
/// Records written before this counter existed are counted once, on the first
/// keyed mutation that finds no counter: the scan is O(records) but happens at
/// most once, and the guarded put in the same transaction means only one
/// concurrent first writer can install it. Under-counting here would be worse
/// than the scan, because the ceiling is what keeps the control group alive.
///
/// The quorum read and the fallback scan are both blocking, so they run on the
/// blocking pool: a stalled control leader must not occupy a runtime worker.
async fn idempotency_usage(st: &AppState) -> Result<(IdempotencyUsage, u64), String> {
    let meta = st.meta.clone();
    tokio::task::spawn_blocking(move || {
        let snapshot = meta
            .try_linearizable_control_snapshot(&[IDEMPOTENCY_USAGE_KEY.to_string()])
            .map_err(|e| e.to_string())?;
        match snapshot.get(IDEMPOTENCY_USAGE_KEY) {
            Some(kv) => Ok((decode_usage(&kv.value)?, kv.mod_revision)),
            None => Ok((
                derive_idempotency_usage(&meta).map_err(|e| e.to_string())?,
                0,
            )),
        }
    })
    .await
    .map_err(|e| e.to_string())?
}

/// Control state plus the admission counter, read in one quorum snapshot.
///
/// They must come from the same moment: planning against one control revision and
/// admitting against a counter read at a different revision would let two writers
/// both pass admission and then both commit.
struct ApplySnapshot {
    control: ControlPlaneState,
    revision: u64,
    usage: IdempotencyUsage,
    usage_revision: u64,
}

async fn read_apply_snapshot(st: &AppState) -> Result<ApplySnapshot, Response> {
    let meta = st.meta.clone();
    let read = tokio::task::spawn_blocking(move || -> Result<ApplySnapshot, String> {
        let snapshot = meta
            .try_snapshot(
                &[
                    CONTROL_STATE_KEY.to_string(),
                    IDEMPOTENCY_USAGE_KEY.to_string(),
                ],
                &[],
            )
            .map_err(|e| e.to_string())?;
        let (control, revision) = match snapshot.get(CONTROL_STATE_KEY) {
            Some(kv) => (
                serde_json::from_slice::<ControlPlaneState>(&kv.value)
                    .map_err(|_| "invalid control state".to_string())?,
                kv.mod_revision,
            ),
            None => (ControlPlaneState::new(), 0),
        };
        let (usage, usage_revision) = match snapshot.get(IDEMPOTENCY_USAGE_KEY) {
            Some(kv) => (decode_usage(&kv.value)?, kv.mod_revision),
            None => (
                derive_idempotency_usage(&meta).map_err(|e| e.to_string())?,
                0,
            ),
        };
        Ok(ApplySnapshot {
            control,
            revision,
            usage,
            usage_revision,
        })
    })
    .await
    .map_err(|e| api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    read.map_err(|e| api_err(StatusCode::SERVICE_UNAVAILABLE, e))
}

fn decode_usage(value: &[u8]) -> Result<IdempotencyUsage, String> {
    serde_json::from_slice::<IdempotencyUsage>(value).map_err(|e| e.to_string())
}

/// Counts the records that already exist when no counter is stored yet.
fn derive_idempotency_usage(
    meta: &yadb_metakv::LocalMetaKv<OsFs>,
) -> Result<IdempotencyUsage, MetaError> {
    let mut usage = IdempotencyUsage {
        version: 1,
        ..IdempotencyUsage::default()
    };
    for (key, kv) in meta.try_scan_prefix("/sys/idempotency/")? {
        if key == IDEMPOTENCY_USAGE_KEY {
            continue;
        }
        usage.records = usage.records.saturating_add(1);
        usage.bytes = usage.bytes.saturating_add(kv.value.len() as u64);
    }
    Ok(usage)
}

fn idempotency_quota_response() -> Response {
    api_err(
        StatusCode::TOO_MANY_REQUESTS,
        "idempotency record quota exhausted; replay or resolve existing keys instead",
    )
}

/// Refusal for `resolve` when the fence record itself cannot be admitted.
///
/// `resolve` promises exactly three answer shapes and one of them is
/// `unknown` with a reason; `capacity_exhausted` is a reason, not a status.
/// A `429` here would be unclassifiable for a client that parses `outcome`, and
/// worse, it could be read as "the write did not happen", which the fence
/// failed to establish either way.
fn resolve_capacity_exhausted() -> Response {
    resolve_unknown("capacity_exhausted")
}

#[derive(Deserialize)]
struct ResolvePayload {
    scope: String,
    key: String,
    /// Project name, required for `scope = "project"` and rejected for control so
    /// one request cannot name two different record keyspaces.
    #[serde(default)]
    project: Option<String>,
}

fn valid_idempotency_key(key: &[u8]) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && key
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"._~-".contains(b))
}

fn idempotency_header(headers: &HeaderMap) -> Result<Option<String>, Response> {
    let mut values = headers.get_all("idempotency-key").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() || !valid_idempotency_key(value.as_bytes()) {
        return Err(api_err(StatusCode::BAD_REQUEST, "invalid idempotency key"));
    }
    Ok(Some(value.to_str().unwrap().to_string()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn record_key(subject: &str, key: &str) -> String {
    format!(
        "/sys/idempotency/{}/{}",
        sha256_hex(subject.as_bytes()),
        sha256_hex(key.as_bytes())
    )
}

fn request_fingerprint(body: &[u8]) -> String {
    let mut encoded = Vec::new();
    for field in [
        b"yadb-control-idempotency-v1".as_slice(),
        b"POST",
        b"/v1/apply",
        body,
    ] {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field);
    }
    sha256_hex(&encoded)
}

async fn read_record(st: &AppState, key: &str) -> Result<Option<IdempotencyRecord>, String> {
    let meta = st.meta.clone();
    let key = key.to_owned();
    tokio::task::spawn_blocking(move || {
        let snapshot = meta
            .try_linearizable_control_snapshot(&[key.clone()])
            .map_err(|e| e.to_string())?;
        snapshot
            .get(&key)
            .map(|kv| {
                serde_json::from_slice::<IdempotencyRecord>(&kv.value).map_err(|e| e.to_string())
            })
            .transpose()
    })
    .await
    .map_err(|e| e.to_string())?
}

fn replay_record(record: IdempotencyRecord, fingerprint: &str) -> Response {
    if record.version != 1 {
        return api_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid idempotency record",
        );
    }
    if record.outcome == "fenced" {
        return api_err(StatusCode::CONFLICT, "idempotency_key_fenced");
    }
    if record.fingerprint != fingerprint {
        return api_err(StatusCode::CONFLICT, "idempotency_key_conflict");
    }
    let Some(response) = record.response.filter(|_| record.outcome == "committed") else {
        return api_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid idempotency record",
        );
    };
    let Ok(body) = BASE64.decode(&response.body_base64) else {
        return api_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid idempotency record",
        );
    };
    let mut builder = Response::builder().status(response.status);
    for (name, value) in &response.headers {
        builder = builder.header(name, value);
    }
    builder
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| {
            api_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid idempotency record",
            )
        })
}

fn unsupported_idempotency() -> Response {
    api_err(
        StatusCode::NOT_IMPLEMENTED,
        "idempotency is not supported on this endpoint",
    )
}

/// Keys that scope one keyed project mutation.
///
/// The record is project-scoped so two projects sharing a Raft group keep
/// separate keyspaces, and the admission counter is group-scoped because the
/// limit it enforces is the group's state image. Both route to the same group,
/// which is the precondition for guarding them in one transaction.
struct ProjectIdempotencyScope {
    /// Record namespace, `/idempotency/<project-id>/`, trailing slash included.
    records: String,
    /// Admission counter, `/idempotency/g<group-id>/_usage`.
    usage_key: String,
}

async fn project_idempotency_scope(
    st: &AppState,
    project_id: u32,
) -> Result<ProjectIdempotencyScope, Response> {
    let meta = st.meta.clone();
    let id = project_id.to_string();
    let resolved = tokio::task::spawn_blocking(move || meta.project_idempotency_scope(&id))
        .await
        .map_err(|e| api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    resolved
        .map(|(records, usage_key)| ProjectIdempotencyScope { records, usage_key })
        .map_err(|e| meta_err_resp(st, e))
}

/// Record key for a project-scope idempotency key.
///
/// Both halves are hashed: a group image is readable by anyone who can inspect
/// storage or a snapshot, and a record must not disclose which keys or subjects
/// a project used. Hashing also keeps one subject's keys from colliding with
/// another's inside the shared project namespace.
fn project_record_key(records: &str, subject: &str, key: &str) -> String {
    format!(
        "{records}{}/{}",
        sha256_hex(subject.as_bytes()),
        sha256_hex(key.as_bytes())
    )
}

/// Fingerprint of a keyed block UNMAP.
///
/// The target is named by immutable project id plus volume name, and the body is
/// hashed exactly as received. Re-serializing `offset`/`len` would let two
/// different requests that happen to parse to the same struct share a
/// fingerprint, which is precisely the collision the fingerprint exists to
/// prevent.
fn block_unmap_fingerprint(project_id: u32, volume: &str, body: &[u8]) -> String {
    let project = project_id.to_string();
    let mut encoded = Vec::new();
    for field in [
        b"yadb-project-idempotency-v1".as_slice(),
        b"POST".as_slice(),
        b"block-unmap".as_slice(),
        project.as_bytes(),
        volume.as_bytes(),
        body,
    ] {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field);
    }
    sha256_hex(&encoded)
}

/// Fingerprint of a keyed block write.
///
/// The target is named by immutable project id plus volume name, the offset is
/// included explicitly because it is a query parameter rather than part of the
/// body, and the body is hashed exactly as received. Every field is
/// length-prefixed, so a decimal offset cannot run into the body bytes and two
/// requests that merely parse to the same values cannot share a digest. The
/// operation tag keeps this domain separate from `block_unmap_fingerprint`: the
/// same key reused for a write and an unmap is a conflict, not a replay.
fn block_write_fingerprint(project_id: u32, volume: &str, offset: u64, body: &[u8]) -> String {
    let project = project_id.to_string();
    let offset = offset.to_string();
    let mut encoded = Vec::new();
    for field in [
        b"yadb-project-idempotency-v1".as_slice(),
        b"PUT".as_slice(),
        b"block-write".as_slice(),
        project.as_bytes(),
        volume.as_bytes(),
        offset.as_bytes(),
        body,
    ] {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field);
    }
    sha256_hex(&encoded)
}

/// Fingerprint of a keyed S3 object mutation.
///
/// `method`/`operation` keep PUT and DELETE in separate domains (and separate
/// from the block domains), and the body is hashed as received; DELETE hashes an
/// empty body. Length-prefixing every field makes store, bucket and key
/// unambiguous even though S3 keys may contain any separator.
fn s3_object_fingerprint(
    method: &str,
    operation: &str,
    project_id: u32,
    store: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
) -> String {
    let project = project_id.to_string();
    let mut encoded = Vec::new();
    for field in [
        b"yadb-project-idempotency-v1".as_slice(),
        method.as_bytes(),
        operation.as_bytes(),
        project.as_bytes(),
        store.as_bytes(),
        bucket.as_bytes(),
        key.as_bytes(),
        body,
    ] {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field);
    }
    sha256_hex(&encoded)
}

/// The record and admission counter for one project group, read in a single
/// quorum-confirmed snapshot.
///
/// They must come from the same moment for the same reason the control pair
/// does: admitting against a counter read at another revision would let two
/// writers both pass admission and then both commit.
struct ProjectApplySnapshot {
    record: Option<IdempotencyRecord>,
    usage: IdempotencyUsage,
    usage_revision: u64,
}

fn read_project_apply_snapshot(
    meta: &yadb_metakv::LocalMetaKv<OsFs>,
    record_key: &str,
    usage_key: &str,
) -> Result<ProjectApplySnapshot, String> {
    let snapshot = meta
        .try_linearizable_snapshot(&[record_key.to_owned(), usage_key.to_owned()], &[])
        .map_err(|e| e.to_string())?;
    let record = match snapshot.get(record_key) {
        Some(kv) => Some(
            serde_json::from_slice::<IdempotencyRecord>(&kv.value).map_err(|e| e.to_string())?,
        ),
        None => None,
    };
    // No scan fallback here, unlike control: project-scope records did not exist
    // before this counter did, so an absent counter is genuinely zero. The
    // control path needs its scan because records there predate the counter.
    let (usage, usage_revision) = match snapshot.get(usage_key) {
        Some(kv) => (decode_usage(&kv.value)?, kv.mod_revision),
        None => (
            IdempotencyUsage {
                version: 1,
                ..IdempotencyUsage::default()
            },
            0,
        ),
    };
    Ok(ProjectApplySnapshot {
        record,
        usage,
        usage_revision,
    })
}

/// The two writes that admit one record, or the refusal that replaces them.
///
/// Admission and the record are built together so the caller cannot submit one
/// without the other: a record that committed without charging the counter would
/// let the group image grow past the limit the counter exists to enforce.
fn admission_writes(
    usage: &IdempotencyUsage,
    usage_key: &str,
    record_key: &str,
    encoded: &[u8],
) -> Result<(yadb_metakv::TxnOp, yadb_metakv::TxnOp), Response> {
    if !usage.admits(encoded.len()) {
        return Err(idempotency_quota_response());
    }
    let next_usage = IdempotencyUsage {
        version: 1,
        records: usage.records.saturating_add(1),
        bytes: usage.bytes.saturating_add(encoded.len() as u64),
    };
    let usage_value = serde_json::to_vec(&next_usage)
        .map_err(|e| api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((
        yadb_metakv::TxnOp::Put {
            key: record_key.to_owned(),
            value: encoded.to_vec(),
        },
        yadb_metakv::TxnOp::Put {
            key: usage_key.to_owned(),
            value: usage_value,
        },
    ))
}

async fn s3_wire_dispatch(
    State(st): State<protocols::s3::S3State>,
    req: axum::extract::Request,
) -> Response {
    // Keyed PUT/DELETE are not refused here: the wire protocol forwards the
    // header to the same gateway handlers the JSON routes use, so a replay
    // returns the recorded response through the normal success wrapper (200 +
    // ETag for PUT, 204 for DELETE).
    protocols::s3::handle(State(st), req).await
}

async fn apply_dispatch(State(st): State<AppState>, req: axum::extract::Request) -> Response {
    use axum::extract::FromRequest;
    if !req.headers().contains_key("idempotency-key") {
        let headers = req.headers().clone();
        return match Json::<ManifestPayload>::from_request(req, &st).await {
            Ok(json) => handle_apply(State(st), headers, json).await,
            Err(e) => e.into_response(),
        };
    }
    let headers = req.headers().clone();
    let subject = match require_admin(
        &headers,
        st.subject_secret.as_deref(),
        &st.admin_subjects,
        st.meta.is_openraft(),
    ) {
        Ok(subject) => subject,
        Err(e) => return e.into_response(),
    };
    let key = match idempotency_header(&headers) {
        Ok(Some(key)) => key,
        Ok(None) => unreachable!(),
        Err(e) => return e,
    };
    if !st.meta.is_openraft() {
        return unsupported_idempotency();
    }
    let Some(subject) = subject else {
        return ControlAdminError::Unverified.into_response();
    };
    let (parts, body) = req.into_parts();
    let body = match axum::body::to_bytes(body, 2 * 1024 * 1024).await {
        Ok(body) => body,
        Err(e) => return api_err(StatusCode::BAD_REQUEST, e.to_string()),
    };
    let rebuilt = axum::http::Request::from_parts(parts, axum::body::Body::from(body.clone()));
    let Json(payload) = match Json::<ManifestPayload>::from_request(rebuilt, &st).await {
        Ok(json) => json,
        Err(e) => return e.into_response(),
    };
    if let Some(response) = maybe_forward_write(
        &st,
        &headers,
        None,
        "POST",
        "/v1/apply".into(),
        "application/json".into(),
        body.clone(),
    )
    .await
    {
        return response;
    }
    let key = record_key(&subject, &key);
    let fingerprint = request_fingerprint(&body);
    match read_record(&st, &key).await {
        Ok(Some(record)) => return replay_record(record, &fingerprint),
        Ok(None) => {}
        Err(e) => return api_err(StatusCode::SERVICE_UNAVAILABLE, e),
    }
    let manifests = match parse_manifests(&payload.yaml) {
        Ok(m) => m,
        Err(e) => return api_err(StatusCode::BAD_REQUEST, e.to_string()),
    };
    commit_keyed_apply(&st, &key, &fingerprint, &manifests).await
}

async fn commit_keyed_apply(
    st: &AppState,
    key: &str,
    fingerprint: &str,
    manifests: &[yadb_control::Manifest],
) -> Response {
    // Each CAS conflict is a definite non-commit; bound retries to avoid
    // livelock when other writers continuously change the control state.
    const MAX_ATTEMPTS: usize = 3;
    for attempt in 1..=MAX_ATTEMPTS {
        let ApplySnapshot {
            control,
            revision,
            usage,
            usage_revision,
        } = match read_apply_snapshot(st).await {
            Ok(s) => s,
            Err(response) => return response,
        };
        // Reject before planning, so a full quota cannot be mistaken for a
        // planning failure and no side effect is staged for a record that will
        // not be admitted.
        if usage.records >= MAX_IDEMPOTENCY_RECORDS {
            return idempotency_quota_response();
        }
        let (ops, next) = match control.plan(&manifests) {
            Ok(result) => result,
            Err(e) => return api_err(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        };
        for (project_id, _) in next.projects.values() {
            if st
                .meta
                .role_info_for_key(&format!("/db/{project_id}/"))
                .is_err()
            {
                return api_err(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!("project {project_id} has no immutable OpenRaft group mapping"),
                );
            }
        }
        let response_body = match serde_json::to_vec(&ApplyResponse {
            generation: next.generation,
            ops,
        }) {
            Ok(b) => b,
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        if response_body.len() > 8192 {
            return api_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "idempotency response body exceeds 8 KiB",
            );
        }
        let record = IdempotencyRecord {
            version: 1,
            fingerprint: fingerprint.to_owned(),
            outcome: "committed".into(),
            response: Some(RecordedResponse {
                status: 200,
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                body_base64: BASE64.encode(response_body),
            }),
        };
        let encoded = serde_json::to_vec(&record).unwrap();
        if encoded.len() > 16384 {
            return api_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "idempotency record exceeds 16 KiB",
            );
        }
        // The byte budget is checked after the record is serialized because only
        // then is its exact size known, and before the transaction because an
        // oversized record must never be submitted.
        if !usage.admits(encoded.len()) {
            return idempotency_quota_response();
        }
        let next_usage = IdempotencyUsage {
            version: 1,
            records: usage.records.saturating_add(1),
            bytes: usage.bytes.saturating_add(encoded.len() as u64),
        };
        let usage_value = match serde_json::to_vec(&next_usage) {
            Ok(v) => v,
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let serialized = match serde_json::to_vec(&next) {
            Ok(s) => s,
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let meta = st.meta.clone();
        let record_key = key.to_owned();
        let result = tokio::task::spawn_blocking(move || {
            meta.execute(KvCommand::RevisionTxn {
                guards: vec![
                    yadb_metakv::RevisionGuard {
                        key: CONTROL_STATE_KEY.into(),
                        expected_mod_revision: revision,
                    },
                    yadb_metakv::RevisionGuard {
                        key: record_key.clone(),
                        expected_mod_revision: 0,
                    },
                    // The counter is guarded at the revision it was read at, so a
                    // concurrent keyed apply cannot both pass admission against the
                    // same counter and both commit.
                    yadb_metakv::RevisionGuard {
                        key: IDEMPOTENCY_USAGE_KEY.into(),
                        expected_mod_revision: usage_revision,
                    },
                ],
                ops: vec![
                    yadb_metakv::TxnOp::Put {
                        key: CONTROL_STATE_KEY.into(),
                        value: serialized,
                    },
                    yadb_metakv::TxnOp::Put {
                        key: record_key,
                        value: encoded,
                    },
                    yadb_metakv::TxnOp::Put {
                        key: IDEMPOTENCY_USAGE_KEY.into(),
                        value: usage_value,
                    },
                ],
            })
        })
        .await;
        match result {
            Ok(Ok(yadb_metakv::KvResult::TxnOk { .. })) => {
                st.refresh_openraft_control_async().await;
                return replay_record(record, fingerprint);
            }
            Ok(Ok(yadb_metakv::KvResult::CasConflict { .. })) => match read_record(st, key).await {
                Ok(Some(record)) => return replay_record(record, fingerprint),
                Ok(None) if attempt < MAX_ATTEMPTS => continue,
                Ok(None) => {
                    return api_err(
                        StatusCode::CONFLICT,
                        "control state changed; replan and retry",
                    )
                }
                Err(e) => return api_err(StatusCode::SERVICE_UNAVAILABLE, e),
            },
            Ok(Ok(_)) => {
                return api_err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected control write result",
                )
            }
            Ok(Err(e)) => return meta_err_resp(st, e),
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    }
    unreachable!("at least one attempt is made")
}

fn resolve_unknown(reason: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"outcome":"unknown","reason":reason,"response":null})),
    )
        .into_response()
}

fn resolve_result(record: IdempotencyRecord) -> Response {
    if record.version != 1 {
        return resolve_unknown("storage_unavailable");
    }
    match (record.outcome.as_str(), record.response) {
        ("committed", Some(response)) => {
            Json(serde_json::json!({"outcome":"committed","reason":null,"response":response}))
                .into_response()
        }
        ("fenced", _) => {
            Json(serde_json::json!({"outcome":"not_committed","reason":"fenced","response":null}))
                .into_response()
        }
        _ => resolve_unknown("storage_unavailable"),
    }
}

async fn handle_resolve(State(st): State<AppState>, req: axum::extract::Request) -> Response {
    use axum::extract::FromRequest;
    let headers = req.headers().clone();
    let (parts, body) = req.into_parts();
    let body = match axum::body::to_bytes(body, 2 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return api_err(StatusCode::BAD_REQUEST, e.to_string()),
    };
    let rebuilt = axum::http::Request::from_parts(parts, axum::body::Body::from(body.clone()));
    let Json(payload) = match Json::<ResolvePayload>::from_request(rebuilt, &st).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    // The body is parsed before authorization because the scope in the body
    // selects the credential: a control resolve is an admin operation, a project
    // resolve is a `readWrite` project operation. The consequence is that a
    // malformed body on this route is 400 before an unauthenticated caller is
    // told 401. That is deliberate and bounded: the parse reveals nothing about
    // stored records, and no record is read or fenced until the scope-specific
    // check has passed.
    if !valid_idempotency_key(payload.key.as_bytes()) {
        return api_err(StatusCode::BAD_REQUEST, "invalid idempotency scope or key");
    }
    // The scope decides both the keyspace and the credential: control is an
    // admin operation, while a project record belongs to whoever may write that
    // project. A request that names both scopes, or a project scope with no
    // project, is rejected rather than resolved against a guess.
    match payload.scope.as_str() {
        "control" if payload.project.is_none() => {
            resolve_control(&st, &headers, &payload.key, body).await
        }
        "project" => match payload.project.as_deref() {
            Some(project) if !project.is_empty() => {
                resolve_project(&st, &headers, project, &payload.key, body).await
            }
            _ => api_err(StatusCode::BAD_REQUEST, "invalid idempotency scope or key"),
        },
        _ => api_err(StatusCode::BAD_REQUEST, "invalid idempotency scope or key"),
    }
}

/// Control-scope resolve.
///
/// The caller must be a proven admin, exactly as `/v1/apply` requires: fencing a
/// key is a permanent, non-reversible state change, so the identity that may
/// commit an apply is also the identity that may fence one.
async fn resolve_control(
    st: &AppState,
    headers: &HeaderMap,
    requested_key: &str,
    body: Bytes,
) -> Response {
    let subject = match require_admin(
        headers,
        st.subject_secret.as_deref(),
        &st.admin_subjects,
        st.meta.is_openraft(),
    ) {
        Ok(subject) => subject,
        Err(e) => return e.into_response(),
    };
    if !st.meta.is_openraft() {
        return unsupported_idempotency();
    }
    let Some(subject) = subject else {
        return ControlAdminError::Unverified.into_response();
    };
    if let Some(response) = maybe_forward_write(
        st,
        headers,
        None,
        "POST",
        "/v1/idempotency/resolve".into(),
        "application/json".into(),
        body,
    )
    .await
    {
        if response.status() == StatusCode::SERVICE_UNAVAILABLE {
            return resolve_unknown("leader_unavailable");
        }
        return response;
    }
    let key = record_key(&subject, requested_key);
    match read_record(st, &key).await {
        Ok(Some(record)) => return resolve_result(record),
        Ok(None) => {}
        Err(_) => return resolve_unknown("quorum_unavailable"),
    }
    let record = IdempotencyRecord {
        version: 1,
        fingerprint: String::new(),
        outcome: "fenced".into(),
        response: None,
    };
    let encoded = serde_json::to_vec(&record).unwrap();
    // Fencing a fresh key is a new permanent record, so it draws from the same
    // budget as a committed apply. Exhaustion here must not be reported as a
    // refusal with an outcome the client cannot classify: `resolve` promises
    // exactly three shapes, and "the fence could not be committed" is `unknown`,
    // never `not_committed` and never a bare 429. `429` stays on `/v1/apply`,
    // where it is the documented refusal for a new key.
    for attempt in 1..=3 {
        let (usage, usage_revision) = match idempotency_usage(st).await {
            Ok(v) => v,
            Err(_) => return resolve_unknown("quorum_unavailable"),
        };
        if !usage.admits(encoded.len()) {
            return resolve_capacity_exhausted();
        }
        let next_usage = IdempotencyUsage {
            version: 1,
            records: usage.records.saturating_add(1),
            bytes: usage.bytes.saturating_add(encoded.len() as u64),
        };
        let usage_value = match serde_json::to_vec(&next_usage) {
            Ok(v) => v,
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let meta = st.meta.clone();
        let key2 = key.clone();
        let record_value = encoded.clone();
        let result = tokio::task::spawn_blocking(move || {
            meta.execute(KvCommand::RevisionTxn {
                guards: vec![
                    yadb_metakv::RevisionGuard {
                        key: key2.clone(),
                        expected_mod_revision: 0,
                    },
                    yadb_metakv::RevisionGuard {
                        key: IDEMPOTENCY_USAGE_KEY.into(),
                        expected_mod_revision: usage_revision,
                    },
                ],
                ops: vec![
                    yadb_metakv::TxnOp::Put {
                        key: key2,
                        value: record_value,
                    },
                    yadb_metakv::TxnOp::Put {
                        key: IDEMPOTENCY_USAGE_KEY.into(),
                        value: usage_value,
                    },
                ],
            })
        })
        .await;
        match result {
            Ok(Ok(yadb_metakv::KvResult::TxnOk { .. })) => return resolve_result(record),
            // A definite non-commit: either another keyed apply moved the counter
            // or another resolve fenced this key. Re-read rather than guess which.
            Ok(Ok(yadb_metakv::KvResult::CasConflict { .. })) if attempt < 3 => {
                match read_record(&st, &key).await {
                    Ok(Some(record)) => return resolve_result(record),
                    Ok(None) => continue,
                    Err(_) => return resolve_unknown("quorum_unavailable"),
                }
            }
            Ok(Ok(yadb_metakv::KvResult::CasConflict { .. })) => {
                return match read_record(st, &key).await {
                    Ok(Some(record)) => resolve_result(record),
                    _ => resolve_unknown("quorum_unavailable"),
                }
            }
            _ => return resolve_unknown("fence_outcome_unknown"),
        }
    }
    unreachable!("at least one attempt is made")
}

/// Project-scope resolve.
///
/// The caller needs write access to the project, not admin: a record exists only
/// because that identity was allowed to mutate the project, and revealing it
/// needs no more authority than creating it did. The fence is committed in the
/// project's own group, guarding the absent record and the group's admission
/// counter in one transaction — the same two keys a keyed mutation guards, which
/// is what makes "fence wins" and "mutation wins" mutually exclusive.
async fn resolve_project(
    st: &AppState,
    headers: &HeaderMap,
    project: &str,
    requested_key: &str,
    body: Bytes,
) -> Response {
    if !st.meta.is_openraft() {
        return unsupported_idempotency();
    }
    let Some(subject) = verified_subject(headers, st.subject_secret.as_deref()) else {
        return ControlAdminError::Unverified.into_response();
    };
    if let Some(response) = maybe_forward_write(
        st,
        headers,
        Some(project),
        "POST",
        "/v1/idempotency/resolve".into(),
        "application/json".into(),
        body,
    )
    .await
    {
        if response.status() == StatusCode::SERVICE_UNAVAILABLE {
            return resolve_unknown("leader_unavailable");
        }
        return response;
    }
    let pid = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match cp.authorize(project, &subject, Role::ReadWrite) {
            Ok(pid) => pid,
            Err(ControlError::Validation(_)) => {
                return api_err(
                    StatusCode::NOT_FOUND,
                    format!("unknown project `{project}`"),
                )
            }
            Err(e) => return api_err(StatusCode::FORBIDDEN, e.to_string()),
        }
    };
    let scope = match project_idempotency_scope(st, pid).await {
        Ok(scope) => scope,
        Err(resp) => return resp,
    };
    let record_key = project_record_key(&scope.records, &subject, requested_key);
    let record = IdempotencyRecord {
        version: 1,
        fingerprint: String::new(),
        outcome: "fenced".into(),
        response: None,
    };
    let encoded = serde_json::to_vec(&record).unwrap();
    for attempt in 1..=3 {
        let pre = {
            let meta = st.meta.clone();
            let record_key = record_key.clone();
            let usage_key = scope.usage_key.clone();
            match tokio::task::spawn_blocking(move || {
                read_project_apply_snapshot(&meta, &record_key, &usage_key)
            })
            .await
            {
                Ok(Ok(snapshot)) => snapshot,
                _ => return resolve_unknown("quorum_unavailable"),
            }
        };
        if let Some(record) = pre.record {
            return resolve_result(record);
        }
        // A fence that cannot be admitted is `unknown`, never `not_committed`:
        // the client cannot tell "no mutation won" from "the fence never ran".
        let (record_op, usage_op) =
            match admission_writes(&pre.usage, &scope.usage_key, &record_key, &encoded) {
                Ok(ops) => ops,
                Err(_) => return resolve_capacity_exhausted(),
            };
        let usage_revision = pre.usage_revision;
        let meta = st.meta.clone();
        let record_key_in = record_key.clone();
        let usage_key_in = scope.usage_key.clone();
        let result = tokio::task::spawn_blocking(move || {
            meta.execute(KvCommand::RevisionTxn {
                guards: vec![
                    yadb_metakv::RevisionGuard {
                        key: record_key_in,
                        expected_mod_revision: 0,
                    },
                    yadb_metakv::RevisionGuard {
                        key: usage_key_in,
                        expected_mod_revision: usage_revision,
                    },
                ],
                ops: vec![record_op, usage_op],
            })
        })
        .await;
        match result {
            Ok(Ok(yadb_metakv::KvResult::TxnOk { .. })) => return resolve_result(record),
            // A definite non-commit: another writer moved the counter, or another
            // resolve fenced this key. Re-read rather than guess which, and never
            // retry a submission whose outcome is unknown.
            Ok(Ok(yadb_metakv::KvResult::CasConflict { .. })) if attempt < 3 => continue,
            _ => return resolve_unknown("fence_outcome_unknown"),
        }
    }
    unreachable!("at least one attempt is made")
}

async fn handle_apply(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ManifestPayload>,
) -> Response {
    // The control plane is mutated only by a proven, allowlisted subject. This
    // check runs before forwarding so a follower cannot be used as an open
    // proxy; the leader repeats it on the forwarded request.
    if let Err(e) = require_admin(
        &headers,
        st.subject_secret.as_deref(),
        &st.admin_subjects,
        st.meta.is_openraft(),
    ) {
        return e.into_response();
    }

    if let Ok(raw_body) = serde_json::to_vec(&req) {
        if let Some(proxied) = maybe_forward_write(
            &st,
            &headers,
            None,
            "POST",
            "/v1/apply".to_string(),
            "application/json".to_string(),
            Bytes::from(raw_body),
        )
        .await
        {
            // Ensure local zero-hop cache & VDisk pools are immediately synced before returning!
            if proxied.status().is_success() {
                if st.meta.is_openraft() {
                    st.refresh_openraft_control_async().await;
                } else {
                    match st.meta.try_get(CONTROL_STATE_KEY) {
                        Ok(Some(kv)) => {
                            if let Ok(cp) = serde_json::from_slice::<ControlPlaneState>(&kv.value) {
                                let _ = st.materialize_pools(&cp);
                                *st.control.write().unwrap() = cp;
                            }
                        }
                        Ok(None) => {}
                        // The proxy already answered; this only refreshes the
                        // local cache. The legacy store cannot fail this read,
                        // so a failure here is reported rather than mistaken
                        // for an empty control plane.
                        Err(error) => {
                            eprintln!("local control state refresh after proxy failed: {error}")
                        }
                    }
                }
            }
            return proxied;
        }
    }

    let manifests = match parse_manifests(&req.yaml) {
        Ok(m) => m,
        Err(e) => return api_err(StatusCode::BAD_REQUEST, e.to_string()),
    };

    let (ops, next_state, expected_revision) = {
        let source = if st.meta.is_openraft() {
            match st.meta.try_snapshot(&[CONTROL_STATE_KEY.to_string()], &[]) {
                Ok(snapshot) => match snapshot.get(CONTROL_STATE_KEY) {
                    Some(kv) => match serde_json::from_slice::<ControlPlaneState>(&kv.value) {
                        Ok(cp) => (cp, kv.mod_revision),
                        Err(_) => {
                            return api_err(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "invalid control state",
                            )
                        }
                    },
                    None => (ControlPlaneState::new(), 0),
                },
                Err(e) => return api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
            }
        } else {
            (st.control.read().unwrap().clone(), 0)
        };
        match source.0.plan(&manifests) {
            Ok((ops, state)) => (ops, state, source.1),
            Err(e) => return api_err(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        }
    };

    if st.meta.is_openraft() {
        for (project_id, _) in next_state.projects.values() {
            if st
                .meta
                .role_info_for_key(&format!("/db/{project_id}/"))
                .is_err()
            {
                return api_err(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!("project {project_id} has no immutable OpenRaft group mapping"),
                );
            }
        }
    }

    if !st.meta.is_openraft() {
        if let Err(e) = st.materialize_pools(&next_state) {
            return api_err(StatusCode::INTERNAL_SERVER_ERROR, e);
        }
    }

    let serialized = match serde_json::to_vec(&next_state) {
        Ok(b) => b,
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let meta = st.meta.clone();
    let put_res = tokio::task::spawn_blocking(move || {
        let command = if meta.is_openraft() {
            KvCommand::RevisionTxn {
                guards: vec![yadb_metakv::RevisionGuard {
                    key: CONTROL_STATE_KEY.to_string(),
                    expected_mod_revision: expected_revision,
                }],
                ops: vec![yadb_metakv::TxnOp::Put {
                    key: CONTROL_STATE_KEY.to_string(),
                    value: serialized,
                }],
            }
        } else {
            KvCommand::Put {
                key: CONTROL_STATE_KEY.to_string(),
                value: serialized,
            }
        };
        meta.execute(command)
    })
    .await;

    match put_res {
        Ok(Ok(yadb_metakv::KvResult::TxnOk { .. })) if st.meta.is_openraft() => {}
        Ok(Ok(yadb_metakv::KvResult::PutOk { .. })) if !st.meta.is_openraft() => {}
        Ok(Ok(yadb_metakv::KvResult::CasConflict { .. })) => {
            return api_err(
                StatusCode::CONFLICT,
                "control state changed; replan and retry",
            )
        }
        Ok(Ok(_)) => {
            return api_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected control write result",
            )
        }
        Ok(Err(e)) => return meta_err_resp(&st, e),
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }

    let generation = next_state.generation;
    if st.meta.is_openraft() {
        st.refresh_openraft_control_async().await;
    } else {
        *st.control.write().unwrap() = next_state;
    }

    Json(ApplyResponse { generation, ops }).into_response()
}

// ============================================================================
// Sharded SQL Database Gateway (`/db/{project}/{database}/query`)
// ============================================================================

fn db_query_path(project: &str, database: &str) -> String {
    format!(
        "/db/{}/{}/query",
        protocols::url(project),
        protocols::url(database)
    )
}

fn block_path(project: &str, volume: &str) -> String {
    format!(
        "/block/{}/{}",
        protocols::url(project),
        protocols::url(volume)
    )
}

async fn handle_db_query(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, database)): Path<(String, String)>,
    Json(req): Json<SqlQueryPayload>,
) -> Response {
    let stmts = match parse_sql_script(&req.sql) {
        Ok(s) => s,
        Err(e) => return api_err(StatusCode::BAD_REQUEST, e.to_string()),
    };

    let has_writes = script_has_writes(&stmts);
    let consistent = req.consistent.unwrap_or(false);
    if consistent {
        // A consistent read is one quorum-confirmed snapshot of read-only state.
        // Anything that could mutate, open a transaction or depend on a
        // session buffer is refused before a snapshot is taken, so a mutating
        // statement can never reach the consistent path.
        if req.session_id.is_some() || req.require_transaction {
            return api_err(
                StatusCode::BAD_REQUEST,
                "consistent reads cannot use session_id or require_transaction",
            );
        }
        if has_writes {
            return api_err(
                StatusCode::BAD_REQUEST,
                "consistent reads support only SELECT and SHOW statements",
            );
        }
        if !st.meta.is_openraft() {
            // Legacy metadata has no quorum-confirmed snapshot to take. Serving
            // the locally applied state instead would be exactly the silent
            // degradation a consistent read exists to refuse, so answer "not
            // available here" instead of stale data.
            return api_err(
                StatusCode::NOT_IMPLEMENTED,
                "consistent reads require OpenRaft metadata; this node cannot take a quorum-confirmed snapshot",
            );
        }
    }
    if headers.contains_key("idempotency-key") {
        // SQL writes are deliberately still unkeyed. A script can mutate any
        // number of tables, shards and secondary indexes through
        // `DatabaseEngine::commit_tx_buffer_at_boundaries`, which builds its own
        // metadata transactions; keying it means threading the record and its
        // counter into *that* transaction (or proving the script's visible
        // effects are recoverable from one), not appending ops here. Until then
        // the honest answer is 501 rather than a record that commits separately
        // from the data. Read-only scripts are refused separately because a key
        // on them would be meaningless rather than unsupported.
        return if has_writes {
            unsupported_idempotency()
        } else {
            api_err(StatusCode::BAD_REQUEST, "idempotency key on read-only SQL")
        };
    }
    let min_role = if has_writes {
        Role::ReadWrite
    } else {
        Role::ReadOnly
    };
    let subject = extract_subject(&headers, st.subject_secret.as_deref());

    // Every request runs on the project-group leader. A quorum-confirmed read
    // needs the leader's applied view; every other read needs it too, because
    // the reader lease that fences it is a metadata write and only the leader
    // can publish one. A follower that served the read locally would have to
    // run unfenced, and one that published the lease would fail on every
    // statement. A legacy node cannot serve a consistent read at all: it is
    // still forwarded, and the engine answers with the documented routing
    // error instead of a stale local read. On the leader this returns `None`
    // and the statement runs locally.
    if let Ok(raw_body) = serde_json::to_vec(&req) {
        if let Some(proxied) = maybe_forward_write(
            &st,
            &headers,
            Some(&project),
            "POST",
            db_query_path(&project, &database),
            "application/json".to_string(),
            Bytes::from(raw_body),
        )
        .await
        {
            return proxied;
        }
    }

    let (project_id, wal_pool, data_pool, shards) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        let pid = match cp.authorize(&project, &subject, min_role) {
            Ok(id) => id,
            Err(e) => return api_err(StatusCode::FORBIDDEN, e.to_string()),
        };
        let db_key = format!("{project}/{database}");
        let Some(db_spec) = cp.databases.get(&db_key) else {
            return api_err(
                StatusCode::NOT_FOUND,
                format!("Database `{db_key}` not found"),
            );
        };
        (
            pid,
            db_spec.wal_pool.clone(),
            db_spec.data_pool.clone(),
            db_spec.shards,
        )
    };

    let db_engine = st.db.clone();
    // Refuse a lost transaction before taking the reader lease: this check reads
    // no fragment, and an early return after `begin_read_lease` would keep the
    // lease live until it expires, making collection refuse for no reason.
    if req.require_transaction {
        let session = req.session_id.as_ref().map(|id| format!("{subject}#{id}"));
        if !session
            .as_ref()
            .is_some_and(|s| db_engine.has_session(project_id, &database, s))
        {
            return api_err(
                StatusCode::CONFLICT,
                "transaction lost on leader change; ROLLBACK and restart",
            );
        }
    }

    // Fence the statement against collection: a scan reads SST/WAL fragments by
    // the ids its tablet manifest names, and a concurrent compaction makes those
    // fragments unreferenced while the scan is still reading them. A write takes
    // its own writer lease as well; this reader lease covers the read half.
    //
    // The lease is published in the metadata group that owns the project, in
    // both modes. On Multi-Raft that is a committed group write, so the request
    // has already been forwarded to that group's leader above; a failure here
    // is fail-closed with the retryable 503 rather than an unfenced read.
    let mut reader_lease: Option<yadb_metakv::GcLease>;
    let st_fence = st.clone();
    match tokio::task::spawn_blocking(move || st_fence.begin_read_lease(project_id)).await {
        Ok(Ok(lease)) => reader_lease = Some(lease),
        Ok(Err(error)) => return api_err(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
        Err(error) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
    let sql_raw = req.sql;
    let subj_session = req
        .session_id
        .map(|id| format!("{subject}#{id}"))
        .unwrap_or(subject);
    let exec = tokio::task::spawn_blocking(move || {
        if consistent {
            db_engine.execute_read_consistent(project_id, &database, &sql_raw)
        } else {
            db_engine.execute_sql_with_session(
                project_id,
                &database,
                &wal_pool,
                &data_pool,
                shards,
                Some(&subj_session),
                &sql_raw,
            )
        }
    })
    .await;

    // A read that outlived its lease may have had fragments reclaimed, so its
    // answer must be refused. A committed mutation is not retroactively undone
    // by a lost read fence; reporting failure would make the client retry a
    // mutation that already landed. Verification caches only a live lease.
    let verified = if let Some(lease) = reader_lease.take() {
        let st_fence = st.clone();
        Some(tokio::task::spawn_blocking(move || st_fence.end_read_lease(&lease)).await)
    } else {
        None
    };
    if matches!(&exec, Ok(Ok(_))) {
        match (has_writes, verified) {
            (_, None) | (_, Some(Ok(Ok(())))) => {}
            (false, Some(Ok(Err(error)))) => {
                return api_err(StatusCode::SERVICE_UNAVAILABLE, error.to_string())
            }
            (false, Some(Err(error))) => {
                return api_err(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
            }
            (true, Some(Ok(Err(error)))) => eprintln!(
                "reader lease lost after committed SQL mutation; returning the successful result: {error}"
            ),
            (true, Some(Err(error))) => eprintln!(
                "reader lease verification failed after committed SQL mutation; returning the successful result: {error}"
            ),
        }
    }

    match exec {
        Ok(Ok(res)) => Json(res).into_response(),
        Ok(Err(e @ (DbError::TableNotFound(_) | DbError::IndexNotFound(_, _)))) => {
            api_err(StatusCode::NOT_FOUND, e.to_string())
        }
        Ok(Err(
            e @ (DbError::InvalidQuery(_)
            | DbError::UnrecognizedSetting(_)
            | DbError::TableAlreadyExists(_)
            | DbError::IndexAlreadyExists(_, _)
            | DbError::DuplicatePrimaryKey(_)),
        )) => api_err(StatusCode::BAD_REQUEST, e.to_string()),
        Ok(Err(DbError::Meta(me))) => meta_err_resp(&st, me),
        // A refused pool write means the statement did not commit: report it as
        // unavailable rather than as an internal error, so a client retries instead
        // of treating the data as lost.
        Ok(Err(DbError::Blob(BlobError::RedundancyNotMet { .. }))) => api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage pool cannot reach full redundancy; the write was refused",
        ),
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

// ============================================================================
// Thin-Provisioned BlockVolume Gateway (`/block/{project}/{volume}`)
// ============================================================================

fn resolve_block_target(
    cp: &ControlPlaneState,
    project: &str,
    volume: &str,
    subject: &str,
    min_role: Role,
) -> Result<(u32, String, u64), Response> {
    let project_id = cp
        .authorize(project, subject, min_role)
        .map_err(|e| api_err(StatusCode::FORBIDDEN, e.to_string()))?;
    let vol_key = format!("{project}/{volume}");
    let spec = cp.block_volumes.get(&vol_key).ok_or_else(|| {
        api_err(
            StatusCode::NOT_FOUND,
            format!("BlockVolume `{vol_key}` not found"),
        )
    })?;
    Ok((project_id, spec.pool.clone(), spec.size_bytes))
}

async fn handle_block_write(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, volume)): Path<(String, String)>,
    Query(q): Query<BlockWriteQuery>,
    body: Bytes,
) -> Response {
    if headers.contains_key("idempotency-key") {
        // A legacy runtime has no record store, so the key is refused before
        // authorization, parsing or any mutation, exactly like keyed UNMAP.
        if !st.meta.is_openraft() {
            return unsupported_idempotency();
        }
        return keyed_block_write(&st, &headers, &project, &volume, q.offset, &body).await;
    }
    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        Some(&project),
        "PUT",
        format!("{}?offset={}", block_path(&project, &volume), q.offset),
        "application/octet-stream".to_string(),
        body.clone(),
    )
    .await
    {
        return proxied;
    }

    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, pool_name, size_bytes) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_block_target(&cp, &project, &volume, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };

    let blk = st.block.clone();
    let res = tokio::task::spawn_blocking(move || {
        blk.write_at(project_id, &volume, &pool_name, size_bytes, q.offset, &body)
    })
    .await;

    match res {
        Ok(Ok(written)) => Json(serde_json::json!({
            "offset": q.offset,
            "bytes_written": written,
        }))
        .into_response(),
        Ok(Err(e @ BlockError::OutOfBounds { .. })) => {
            api_err(StatusCode::RANGE_NOT_SATISFIABLE, e.to_string())
        }
        Ok(Err(BlockError::Meta(me))) => meta_err_resp(&st, me),
        // The pool refused the write because it could not reach full redundancy.
        // Nothing was published, so this is availability, not a failed volume.
        Ok(Err(BlockError::Blob(BlobError::RedundancyNotMet { .. }))) => api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage pool cannot reach full redundancy; the write was refused",
        ),
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_block_read(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, volume)): Path<(String, String)>,
    Query(q): Query<BlockReadQuery>,
) -> Response {
    if q.len > 16 * 1024 * 1024 {
        return api_err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "maximum block read is 16 MiB",
        );
    }
    // Every block read runs on the project-group leader. A consistent read
    // needs the leader's quorum-confirmed map; every other read needs the
    // leader because the reader lease that fences it is a committed metadata
    // write only the leader can publish. A follower that served the read
    // locally would run unfenced. On the leader this returns `None` and the
    // read runs locally under the lease taken below.
    let mut forward_path = format!(
        "{}?offset={}&len={}",
        block_path(&project, &volume),
        q.offset,
        q.len
    );
    if q.consistent {
        forward_path.push_str("&consistent=true");
    }
    if let Some(resp) = maybe_forward_write(
        &st,
        &headers,
        Some(&project),
        "GET",
        forward_path,
        "application/octet-stream".into(),
        Bytes::new(),
    )
    .await
    {
        return resp;
    }
    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, pool_name, size_bytes) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_block_target(&cp, &project, &volume, &subject, Role::ReadOnly) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };

    // Publish a reader lease *before* the metadata read and keep it until the
    // fragments have been fetched. The collector refuses while any lease is
    // live, so a fragment this read pins cannot be swept underneath it. The
    // lease is also the reason a read that starts during a collection pass is
    // refused: the pass already proved the fence clear, so it would delete what
    // this read is about to pin.
    //
    // On legacy metadata a consistent read additionally takes one empty quorum
    // transaction after the lease, so the map it reads is confirmed by a quorum
    // and not just by this node's applied state. On Multi-Raft the lease
    // publication is itself a committed group write applied on this node, and
    // a consistent read below reads a quorum-confirmed snapshot.
    let mut reader_lease: Option<yadb_metakv::GcLease>;
    let consistent = q.consistent;
    let st_fence = st.clone();
    let fence = tokio::task::spawn_blocking(move || {
        let lease = st_fence.begin_read_lease(project_id)?;
        if !consistent || st_fence.meta.is_openraft() {
            return Ok(lease);
        }
        match st_fence.meta.execute(KvCommand::Txn {
            guards: vec![],
            ops: vec![],
        }) {
            Ok(yadb_metakv::KvResult::TxnOk { .. }) => Ok(lease),
            Ok(_) => {
                let _ = st_fence.end_read_lease(&lease);
                Err(MetaError::Routing(
                    "read quorum unavailable or metadata changed".into(),
                ))
            }
            Err(error) => {
                let _ = st_fence.end_read_lease(&lease);
                Err(error)
            }
        }
    })
    .await;
    match fence {
        Ok(Ok(lease)) => reader_lease = Some(lease),
        Ok(Err(error)) => return api_err(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
        Err(error) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }

    let blk = st.block.clone();
    let read_consistent = q.consistent;
    let res = tokio::task::spawn_blocking(move || {
        if read_consistent {
            blk.read_at_consistent(project_id, &volume, &pool_name, size_bytes, q.offset, q.len)
        } else {
            blk.read_at(project_id, &volume, &pool_name, size_bytes, q.offset, q.len)
        }
    })
    .await;

    // The fragments named by the pinned map have been fetched (or the read
    // failed). Proving the fence held is part of the read: a lease that expired
    // mid-read let a pass reclaim what was just read, so the bytes are not
    // returned.
    if let Some(lease) = reader_lease.take() {
        let st_fence = st.clone();
        match tokio::task::spawn_blocking(move || st_fence.end_read_lease(&lease)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return api_err(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
            Err(error) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
        }
    }

    match res {
        Ok(Ok(bytes)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response(),
        Ok(Err(e @ BlockError::OutOfBounds { .. })) => {
            api_err(StatusCode::RANGE_NOT_SATISFIABLE, e.to_string())
        }
        Ok(Err(BlockError::Meta(me))) => meta_err_resp(&st, me),
        // The linearizable map is a point-in-time view, not a GC fence: a
        // newer write may have replaced a block and a barrier may have
        // collected the old blob between the map read and the fetch. Failing
        // is the only honest answer; zeros or a silent skip would return data
        // that never existed at any single revision.
        Ok(Err(e @ BlockError::Inconsistent { .. })) => {
            api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string())
        }
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_block_unmap(
    State(st): State<AppState>,
    Path((project, volume)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Response {
    use axum::extract::FromRequest;
    let headers = req.headers().clone();
    let keyed = headers.contains_key("idempotency-key");
    // A legacy runtime has no record store, so the key is refused before the body
    // is even parsed: whether the request happens to be well-formed must not
    // change a 501 into a 400.
    if keyed && !st.meta.is_openraft() {
        return unsupported_idempotency();
    }
    let (parts, body) = req.into_parts();
    let body = match axum::body::to_bytes(body, 2 * 1024 * 1024).await {
        Ok(body) => body,
        Err(e) => return api_err(StatusCode::PAYLOAD_TOO_LARGE, e.to_string()),
    };
    // One parse path for both modes, but the bytes are kept: a keyed request must
    // fingerprint exactly what the client sent, and forwarding must not re-encode
    // a body whose meaning depends on its original bytes.
    let rebuilt = axum::http::Request::from_parts(parts, axum::body::Body::from(body.clone()));
    let Json(payload) = match Json::<UnmapPayload>::from_request(rebuilt, &st).await {
        Ok(json) => json,
        Err(e) => return e.into_response(),
    };
    if !keyed {
        return block_unmap_legacy(&st, &headers, &project, &volume, &payload, body).await;
    }
    let key = match idempotency_header(&headers) {
        Ok(Some(key)) => key,
        Ok(None) => unreachable!("keyed is derived from the same header"),
        Err(e) => return e,
    };
    // A project record is keyed by subject, so an identity that cannot be proven
    // has no scope to be idempotent in. `extract_subject` would fall back to
    // "anonymous", which would silently share one keyspace across unauthenticated
    // callers.
    let Some(subject) = verified_subject(&headers, st.subject_secret.as_deref()) else {
        return ControlAdminError::Unverified.into_response();
    };
    if let Some(resp) = maybe_forward_write(
        &st,
        &headers,
        Some(&project),
        "POST",
        format!("{}/unmap", block_path(&project, &volume)),
        "application/json".into(),
        body.clone(),
    )
    .await
    {
        return resp;
    }
    keyed_block_unmap(&st, &project, &volume, &subject, &key, &payload, &body).await
}

async fn block_unmap_legacy(
    st: &AppState,
    headers: &HeaderMap,
    project: &str,
    volume: &str,
    req: &UnmapPayload,
    body: Bytes,
) -> Response {
    if let Some(resp) = maybe_forward_write(
        st,
        headers,
        Some(project),
        "POST",
        format!("{}/unmap", block_path(project, volume)),
        "application/json".into(),
        body,
    )
    .await
    {
        return resp;
    }
    let subject = extract_subject(headers, st.subject_secret.as_deref());
    let (pid, _, size) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_block_target(&cp, project, volume, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(r) => return r,
        }
    };
    if req.offset % DEFAULT_BLOCK_SIZE as u64 != 0 || req.len % DEFAULT_BLOCK_SIZE as u64 != 0 {
        return api_err(StatusCode::BAD_REQUEST, "UNMAP must be 4 KiB aligned");
    }
    let block = st.block.clone();
    let volume = volume.to_owned();
    let offset = req.offset;
    let len = req.len;
    match tokio::task::spawn_blocking(move || block.unmap_at(pid, &volume, size, offset, len)).await
    {
        Ok(Ok(())) => StatusCode::OK.into_response(),
        Ok(Err(e @ BlockError::OutOfBounds { .. })) => {
            api_err(StatusCode::RANGE_NOT_SATISFIABLE, e.to_string())
        }
        Ok(Err(BlockError::Meta(e))) => meta_err_resp(st, e),
        Ok(Err(BlockError::Blob(BlobError::RedundancyNotMet { .. }))) => api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage pool cannot reach full redundancy; the write was refused",
        ),
        _ => api_err(StatusCode::SERVICE_UNAVAILABLE, "UNMAP failed"),
    }
}

/// Keyed UNMAP: the extent-map publication and the idempotency record commit in
/// one guarded `RevisionTxn`.
///
/// UNMAP is the first project-scope keyed mutation because it needs no staging:
/// it changes only the map, so the mutation and its record can share the single
/// transaction that `unmap_at_with` builds. Nothing else here may be split into a
/// second transaction — a record that commits without the map, or the map without
/// the record, is exactly the ambiguity the key exists to remove.
async fn keyed_block_unmap(
    st: &AppState,
    project: &str,
    volume: &str,
    subject: &str,
    key: &str,
    payload: &UnmapPayload,
    body: &[u8],
) -> Response {
    let (pid, _, size) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_block_target(&cp, project, volume, subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(r) => return r,
        }
    };
    if payload.offset % DEFAULT_BLOCK_SIZE as u64 != 0
        || payload.len % DEFAULT_BLOCK_SIZE as u64 != 0
    {
        return api_err(StatusCode::BAD_REQUEST, "UNMAP must be 4 KiB aligned");
    }
    let scope = match project_idempotency_scope(st, pid).await {
        Ok(scope) => scope,
        Err(resp) => return resp,
    };
    let record_key = project_record_key(&scope.records, subject, key);
    let fingerprint = block_unmap_fingerprint(pid, volume, body);
    // Fast path: an existing record replays without reading or touching the map.
    let pre = {
        let meta = st.meta.clone();
        let record_key = record_key.clone();
        let usage_key = scope.usage_key.clone();
        match tokio::task::spawn_blocking(move || {
            read_project_apply_snapshot(&meta, &record_key, &usage_key)
        })
        .await
        {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(e)) => return api_err(StatusCode::SERVICE_UNAVAILABLE, e),
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    };
    if let Some(record) = pre.record {
        return replay_record(record, &fingerprint);
    }
    if pre.usage.records >= MAX_IDEMPOTENCY_RECORDS {
        // The cheap refusal: the exact byte budget is only checkable once the
        // record is serialized, which happens inside the map transaction. This
        // one keeps a full group from even reading the extent map.
        return idempotency_quota_response();
    }
    let record = IdempotencyRecord {
        version: 1,
        fingerprint: fingerprint.clone(),
        outcome: "committed".into(),
        response: Some(RecordedResponse {
            status: StatusCode::OK.as_u16(),
            headers: BTreeMap::new(),
            body_base64: String::new(),
        }),
    };
    let encoded = match serde_json::to_vec(&record) {
        Ok(encoded) => encoded,
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    // The engine retries a guard conflict up to its own bound. It cannot tell
    // which guard failed, so the closure re-reads the record and the counter on
    // every attempt: retrying a stale counter revision would only spin, and a
    // record that appeared means a concurrent attempt already won this key.
    //
    // Every refusal here returns `Err` from `prepare`, which propagates out of
    // `unmap_at_with` immediately without another attempt, and the captured
    // response is what the handler returns. That matters for a quota rejection:
    // expressing it as an unsatisfiable guard would burn all 32 attempts before
    // reporting a mere conflict. `Conflict` is used only because the engine
    // needs *an* error to propagate; the outer match takes the captured response
    // before it ever treats it as a map conflict.
    let abort: std::sync::Arc<std::sync::Mutex<Option<Response>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let abort_in = abort.clone();
    let block = st.block.clone();
    let meta = st.meta.clone();
    let volume_owned = volume.to_owned();
    let record_key_in = record_key.clone();
    let usage_key_in = scope.usage_key.clone();
    let fingerprint_in = fingerprint.clone();
    let offset = payload.offset;
    let len = payload.len;
    let res = tokio::task::spawn_blocking(move || {
        block.unmap_at_with(
            pid,
            &volume_owned,
            size,
            offset,
            len,
            |_map: &yadb_block::VolumeExtentMap| {
                let snapshot = match meta
                    .try_linearizable_snapshot(&[record_key_in.clone(), usage_key_in.clone()], &[])
                {
                    Ok(snapshot) => snapshot,
                    Err(e) => {
                        *abort_in.lock().unwrap() =
                            Some(api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()));
                        return Err(BlockError::Conflict);
                    }
                };
                if let Some(kv) = snapshot.get(&record_key_in) {
                    let response = match serde_json::from_slice::<IdempotencyRecord>(&kv.value) {
                        Ok(record) => replay_record(record, &fingerprint_in),
                        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                    };
                    *abort_in.lock().unwrap() = Some(response);
                    return Err(BlockError::Conflict);
                }
                let (usage, usage_revision) = match snapshot.get(&usage_key_in) {
                    Some(kv) => match decode_usage(&kv.value) {
                        Ok(usage) => (usage, kv.mod_revision),
                        Err(e) => {
                            *abort_in.lock().unwrap() =
                                Some(api_err(StatusCode::INTERNAL_SERVER_ERROR, e));
                            return Err(BlockError::Conflict);
                        }
                    },
                    None => (
                        IdempotencyUsage {
                            version: 1,
                            ..IdempotencyUsage::default()
                        },
                        0,
                    ),
                };
                let (record_op, usage_op) =
                    match admission_writes(&usage, &usage_key_in, &record_key_in, &encoded) {
                        Ok(ops) => ops,
                        Err(response) => {
                            *abort_in.lock().unwrap() = Some(response);
                            return Err(BlockError::Conflict);
                        }
                    };
                Ok(yadb_block::MapPublication {
                    guards: vec![
                        yadb_metakv::RevisionGuard {
                            key: record_key_in.clone(),
                            expected_mod_revision: 0,
                        },
                        yadb_metakv::RevisionGuard {
                            key: usage_key_in.clone(),
                            expected_mod_revision: usage_revision,
                        },
                    ],
                    ops: vec![record_op, usage_op],
                })
            },
        )
    })
    .await;
    match res {
        Ok(Ok(())) => StatusCode::OK.into_response(),
        Ok(Err(BlockError::Conflict)) => {
            if let Some(response) = abort.lock().unwrap().take() {
                return response;
            }
            // The loop exhausted its attempts without the caller aborting: a
            // definite non-commit, so re-read the record to tell "a concurrent
            // attempt committed this key" from "the map moved under us". Never
            // retry blindly.
            let meta = st.meta.clone();
            let record_key_in = record_key.clone();
            let record_key_read = record_key.clone();
            match tokio::task::spawn_blocking(move || {
                meta.try_linearizable_snapshot(&[record_key_in], &[])
                    .map_err(|e| e.to_string())
            })
            .await
            {
                Ok(Ok(snapshot)) => match snapshot.get(&record_key_read) {
                    Some(kv) => match serde_json::from_slice::<IdempotencyRecord>(&kv.value) {
                        Ok(record) => replay_record(record, &fingerprint),
                        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                    },
                    None => api_err(
                        StatusCode::CONFLICT,
                        "block map changed; re-read the volume and retry",
                    ),
                },
                _ => api_err(StatusCode::SERVICE_UNAVAILABLE, "quorum unavailable"),
            }
        }
        Ok(Err(e @ BlockError::OutOfBounds { .. })) => {
            api_err(StatusCode::RANGE_NOT_SATISFIABLE, e.to_string())
        }
        Ok(Err(BlockError::Meta(e))) => meta_err_resp(st, e),
        Ok(Err(e)) => api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Keyed block write: the extent-map publication and the idempotency record
/// commit in one guarded `RevisionTxn` (`BlockVolumeEngine::write_at_with`).
///
/// The new fragments are written before the publication, so a refusal, an
/// exhausted retry bound or a crash before the publication leaves them
/// unreferenced. They are immutable garbage that the orphan scan reclaims; no
/// rollback is attempted. The response body depends only on the request
/// (`bytes_written` is the body length the engine reports), so it is built and
/// recorded before the transaction and replayed verbatim afterwards.
async fn keyed_block_write(
    st: &AppState,
    headers: &HeaderMap,
    project: &str,
    volume: &str,
    offset: u64,
    body: &Bytes,
) -> Response {
    let key = match idempotency_header(headers) {
        Ok(Some(key)) => key,
        Ok(None) => unreachable!("the keyed path is chosen from this header"),
        Err(e) => return e,
    };
    // A project record is keyed by subject, so an unproven identity has no
    // scope to be idempotent in (same rule as keyed UNMAP).
    let Some(subject) = verified_subject(headers, st.subject_secret.as_deref()) else {
        return ControlAdminError::Unverified.into_response();
    };
    if let Some(resp) = maybe_forward_write(
        st,
        headers,
        Some(project),
        "PUT",
        format!("{}?offset={}", block_path(project, volume), offset),
        "application/octet-stream".into(),
        body.clone(),
    )
    .await
    {
        return resp;
    }
    let (pid, pool_name, size) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_block_target(&cp, project, volume, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(r) => return r,
        }
    };
    let scope = match project_idempotency_scope(st, pid).await {
        Ok(scope) => scope,
        Err(resp) => return resp,
    };
    let record_key = project_record_key(&scope.records, &subject, &key);
    let fingerprint = block_write_fingerprint(pid, volume, offset, body);
    // Fast path: an existing record replays without touching the volume.
    let pre = {
        let meta = st.meta.clone();
        let record_key = record_key.clone();
        let usage_key = scope.usage_key.clone();
        match tokio::task::spawn_blocking(move || {
            read_project_apply_snapshot(&meta, &record_key, &usage_key)
        })
        .await
        {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(e)) => return api_err(StatusCode::SERVICE_UNAVAILABLE, e),
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    };
    if let Some(record) = pre.record {
        return replay_record(record, &fingerprint);
    }
    if pre.usage.records >= MAX_IDEMPOTENCY_RECORDS {
        // Cheap refusal before any fragment is written; the exact byte budget is
        // re-checked once the record is serialized, inside the map transaction.
        return idempotency_quota_response();
    }
    let response_body = match serde_json::to_vec(&serde_json::json!({
        "offset": offset,
        "bytes_written": body.len(),
    })) {
        Ok(body) => body,
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let record = IdempotencyRecord {
        version: 1,
        fingerprint: fingerprint.clone(),
        outcome: "committed".into(),
        response: Some(RecordedResponse {
            status: StatusCode::OK.as_u16(),
            headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
            body_base64: BASE64.encode(response_body),
        }),
    };
    let encoded = match serde_json::to_vec(&record) {
        Ok(encoded) => encoded,
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    // The engine retries a guard conflict up to its own bound and cannot tell
    // which guard failed, so the closure re-reads the record and the counter on
    // every attempt. A record that appeared means a concurrent attempt already
    // won this key; a stale counter revision would only spin.
    //
    // Every refusal returns `Err` from `prepare`, which propagates out of
    // `write_at_with` immediately without another attempt, and the captured
    // response is what the handler returns. `Conflict` is used only because the
    // engine needs an error to propagate; the outer match takes the captured
    // response before it treats the failure as a map conflict.
    let abort: std::sync::Arc<std::sync::Mutex<Option<Response>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let abort_in = abort.clone();
    let block = st.block.clone();
    let meta = st.meta.clone();
    let volume_owned = volume.to_owned();
    let pool_in = pool_name.clone();
    let record_key_in = record_key.clone();
    let usage_key_in = scope.usage_key.clone();
    let fingerprint_in = fingerprint.clone();
    let body_in = body.clone();
    let res = tokio::task::spawn_blocking(move || {
        block.write_at_with(
            pid,
            &volume_owned,
            &pool_in,
            size,
            offset,
            &body_in,
            |_map: &yadb_block::VolumeExtentMap| {
                let snapshot = match meta
                    .try_linearizable_snapshot(&[record_key_in.clone(), usage_key_in.clone()], &[])
                {
                    Ok(snapshot) => snapshot,
                    Err(e) => {
                        *abort_in.lock().unwrap() =
                            Some(api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()));
                        return Err(BlockError::Conflict);
                    }
                };
                if let Some(kv) = snapshot.get(&record_key_in) {
                    let response = match serde_json::from_slice::<IdempotencyRecord>(&kv.value) {
                        Ok(record) => replay_record(record, &fingerprint_in),
                        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                    };
                    *abort_in.lock().unwrap() = Some(response);
                    return Err(BlockError::Conflict);
                }
                let (usage, usage_revision) = match snapshot.get(&usage_key_in) {
                    Some(kv) => match decode_usage(&kv.value) {
                        Ok(usage) => (usage, kv.mod_revision),
                        Err(e) => {
                            *abort_in.lock().unwrap() =
                                Some(api_err(StatusCode::INTERNAL_SERVER_ERROR, e));
                            return Err(BlockError::Conflict);
                        }
                    },
                    None => (
                        IdempotencyUsage {
                            version: 1,
                            ..IdempotencyUsage::default()
                        },
                        0,
                    ),
                };
                let (record_op, usage_op) =
                    match admission_writes(&usage, &usage_key_in, &record_key_in, &encoded) {
                        Ok(ops) => ops,
                        Err(response) => {
                            *abort_in.lock().unwrap() = Some(response);
                            return Err(BlockError::Conflict);
                        }
                    };
                Ok(yadb_block::MapPublication {
                    guards: vec![
                        yadb_metakv::RevisionGuard {
                            key: record_key_in.clone(),
                            expected_mod_revision: 0,
                        },
                        yadb_metakv::RevisionGuard {
                            key: usage_key_in.clone(),
                            expected_mod_revision: usage_revision,
                        },
                    ],
                    ops: vec![record_op, usage_op],
                })
            },
        )
    })
    .await;
    match res {
        Ok(Ok(_written)) => replay_record(record, &fingerprint),
        Ok(Err(BlockError::Conflict)) => {
            if let Some(response) = abort.lock().unwrap().take() {
                return response;
            }
            // A definite non-commit: re-read the record to tell "a concurrent
            // attempt committed this key" from "the map moved under us". Never
            // retry the write blindly.
            let meta = st.meta.clone();
            let record_key_in = record_key.clone();
            let record_key_read = record_key.clone();
            match tokio::task::spawn_blocking(move || {
                meta.try_linearizable_snapshot(&[record_key_in], &[])
                    .map_err(|e| e.to_string())
            })
            .await
            {
                Ok(Ok(snapshot)) => match snapshot.get(&record_key_read) {
                    Some(kv) => match serde_json::from_slice::<IdempotencyRecord>(&kv.value) {
                        Ok(record) => replay_record(record, &fingerprint),
                        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                    },
                    None => api_err(
                        StatusCode::CONFLICT,
                        "block map changed; re-read the volume and retry",
                    ),
                },
                _ => api_err(StatusCode::SERVICE_UNAVAILABLE, "quorum unavailable"),
            }
        }
        Ok(Err(e @ BlockError::OutOfBounds { .. })) => {
            api_err(StatusCode::RANGE_NOT_SATISFIABLE, e.to_string())
        }
        Ok(Err(BlockError::Meta(me))) => meta_err_resp(st, me),
        Ok(Err(BlockError::Blob(BlobError::RedundancyNotMet { .. }))) => api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage pool cannot reach full redundancy; the write was refused",
        ),
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

// ============================================================================
// S3 ObjectStore Gateway (`/s3/{project}/{store}/{bucket}[/{*key}]`)
// ============================================================================

fn resolve_s3_target(
    cp: &ControlPlaneState,
    project: &str,
    store: &str,
    bucket: &str,
    subject: &str,
    min_role: Role,
) -> Result<(u32, String), Response> {
    let project_id = cp
        .authorize(project, subject, min_role)
        .map_err(|e| api_err(StatusCode::FORBIDDEN, e.to_string()))?;

    let store_key = format!("{project}/{store}");
    let store_spec = cp.object_stores.get(&store_key).ok_or_else(|| {
        api_err(
            StatusCode::NOT_FOUND,
            format!("ObjectStore `{store_key}` not found"),
        )
    })?;

    if !store_spec.buckets.is_empty() && !store_spec.buckets.iter().any(|b| b == bucket) {
        return Err(api_err(
            StatusCode::NOT_FOUND,
            format!("bucket `{bucket}` is not declared in ObjectStore `{store_key}`"),
        ));
    }

    Ok((project_id, store_spec.pool.clone()))
}

async fn handle_s3_list(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, store, bucket)): Path<(String, String, String)>,
    Query(q): Query<ListQuery>,
) -> Response {
    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, _) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadOnly) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };

    match st.s3.list_objects(project_id, &store, &bucket, &q.prefix) {
        Ok(objects) => Json(serde_json::json!({
            "project": project,
            "store": store,
            "bucket": bucket,
            "prefix": q.prefix,
            "objects": objects,
        }))
        .into_response(),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Query parameters of a keyed S3 write. Multipart part upload, completion and
/// abort are the only operations that read them; a single `PUT`/`DELETE` leaves
/// every field empty and keeps its previous path byte-for-byte.
#[derive(Deserialize, Default)]
struct S3WriteQuery {
    /// Present (usually with an empty value) on `POST ...?uploads`.
    #[serde(default)]
    uploads: Option<String>,
    #[serde(default, rename = "uploadId", alias = "upload_id")]
    upload_id: Option<String>,
    #[serde(default, rename = "partNumber", alias = "part_number")]
    part_number: Option<u32>,
}

/// Maps an engine multipart error onto the gateway's JSON error convention.
///
/// `ConcurrentModification` means the single completion transaction lost its
/// revision guard: the manifest may or may not have been published by a racing
/// attempt, so the caller must not replay it. It is reported as an ambiguous
/// failure with the explicit no-retry marker the forwarding path uses.
fn s3_multipart_err(st: &AppState, err: S3Error) -> Response {
    let ambiguous = matches!(err, S3Error::ConcurrentModification);
    let (status, code, message) = match err {
        S3Error::MultipartUploadNotFound(id) => (
            StatusCode::NOT_FOUND,
            "NoSuchUpload",
            format!("upload `{id}` does not exist"),
        ),
        S3Error::InvalidPart { part, reason } => {
            (StatusCode::BAD_REQUEST, "InvalidPart", format!("part {part}: {reason}"))
        }
        S3Error::InvalidPartOrder => (
            StatusCode::BAD_REQUEST,
            "InvalidPartOrder",
            "parts must be listed in strictly ascending order".into(),
        ),
        S3Error::PartTooLarge { part, size, max } => (
            StatusCode::BAD_REQUEST,
            "EntityTooLarge",
            format!("part {part} is {size} bytes, over the {max}-byte part limit"),
        ),
        S3Error::MalformedMultipart(reason) => (
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            format!("malformed completion request: {reason}"),
        ),
        S3Error::Meta(me) => return meta_err_resp(st, me),
        S3Error::Blob(BlobError::RedundancyNotMet { .. }) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "storage pool cannot reach full redundancy; the write was refused".into(),
        ),
        S3Error::ConcurrentModification => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "multipart state changed under the request; the outcome is ambiguous, do not retry automatically".into(),
        ),
        e => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            e.to_string(),
        ),
    };
    let mut response = (
        status,
        Json(serde_json::json!({ "error": message, "code": code })),
    )
        .into_response();
    if matches!(status, StatusCode::SERVICE_UNAVAILABLE) {
        response
            .headers_mut()
            .insert("x-yadb-retry", HeaderValue::from_static("do-not-retry"));
    }
    if ambiguous {
        // The guard was lost: the completion may or may not have published. Say
        // so explicitly instead of leaving the client to infer it from prose.
        response
            .headers_mut()
            .insert("x-yadb-write-outcome", HeaderValue::from_static("unknown"));
    }
    response
}

/// Uploads one part of a multipart upload.
///
/// The body is bounded to 16 MiB by the daemon's body limit before this runs and
/// re-checked by the engine. The part ETag is computed by the engine from the
/// bytes it stored, so a client-supplied digest is never trusted.
async fn handle_s3_upload_part(
    st: AppState,
    headers: HeaderMap,
    project: String,
    store: String,
    bucket: String,
    key: String,
    upload_id: String,
    part_number: u32,
    body: Bytes,
) -> Response {
    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        Some(&project),
        "PUT",
        format!(
            "/s3/{}/{}/{}/{}?uploadId={}&partNumber={part_number}",
            protocols::url(&project),
            protocols::url(&store),
            protocols::url(&bucket),
            key.split('/')
                .map(protocols::url)
                .collect::<Vec<_>>()
                .join("/"),
            protocols::url(&upload_id),
        ),
        "application/octet-stream".into(),
        body.clone(),
    )
    .await
    {
        return proxied;
    }
    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, _) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
    let s3 = st.s3.clone();
    let res = tokio::task::spawn_blocking(move || {
        s3.upload_part(
            project_id,
            &store,
            &bucket,
            &key,
            &upload_id,
            part_number,
            &body,
        )
    })
    .await;
    match res {
        Ok(Ok(etag)) => (
            StatusCode::OK,
            [(header::ETAG, format!("\"{etag}\""))],
            Json(serde_json::json!({ "partNumber": part_number, "etag": etag })),
        )
            .into_response(),
        Ok(Err(e)) => s3_multipart_err(&st, e),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Rejects object-creation options this build cannot honour.
///
/// A single `PUT` refuses the same options on the native listener, but a
/// multipart upload takes a different dispatch path and used to skip them
/// entirely: a client could ask for encryption or an ACL, receive a successful
/// initiation, and complete an object without the requested semantics. The
/// check runs before any durable upload state exists.
fn unsupported_object_options(headers: &HeaderMap) -> Option<&'static str> {
    if headers
        .keys()
        .any(|k| k.as_str().starts_with("x-amz-meta-"))
    {
        return Some("custom metadata is not supported");
    }
    for name in [
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-aws-kms-key-id",
        "x-amz-acl",
    ] {
        if headers.contains_key(name) {
            return Some("requested encryption or ACL is unsupported");
        }
    }
    None
}

/// Starts a multipart upload. The record is durable before this returns, so the
/// id it hands out always names a collectable upload.
async fn handle_s3_create_multipart(
    st: AppState,
    headers: HeaderMap,
    project: String,
    store: String,
    bucket: String,
    key: String,
) -> Response {
    if let Some(reason) = unsupported_object_options(&headers) {
        return api_err(StatusCode::NOT_IMPLEMENTED, reason);
    }
    // The forwarded initiation must carry the client's own content type, not a
    // placeholder, or a follower would store a different type than the leader
    // was asked for.
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        Some(&project),
        "POST",
        format!(
            "/s3/{}/{}/{}/{}?uploads",
            protocols::url(&project),
            protocols::url(&store),
            protocols::url(&bucket),
            key.split('/')
                .map(protocols::url)
                .collect::<Vec<_>>()
                .join("/"),
        ),
        content_type.clone(),
        Bytes::new(),
    )
    .await
    {
        return proxied;
    }
    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, pool_name) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
    let s3 = st.s3.clone();
    let res = tokio::task::spawn_blocking(move || {
        let upload_id = s3.create_multipart_upload(
            project_id,
            &store,
            &pool_name,
            &bucket,
            &key,
            &content_type,
        )?;
        Ok::<_, S3Error>((upload_id, bucket, key))
    })
    .await;
    match res {
        Ok(Ok((upload_id, bucket, key))) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "bucket": bucket,
                "key": key,
                "uploadId": upload_id,
            })),
        )
            .into_response(),
        Ok(Err(e)) => s3_multipart_err(&st, e),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Publishes a completed multipart upload in the engine's one guarded
/// transaction, then reports the object exactly as a single PUT would.
async fn handle_s3_complete_multipart(
    st: AppState,
    headers: HeaderMap,
    project: String,
    store: String,
    bucket: String,
    key: String,
    upload_id: String,
    body: Bytes,
) -> Response {
    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        Some(&project),
        "POST",
        format!(
            "/s3/{}/{}/{}/{}?uploadId={}",
            protocols::url(&project),
            protocols::url(&store),
            protocols::url(&bucket),
            key.split('/')
                .map(protocols::url)
                .collect::<Vec<_>>()
                .join("/"),
            protocols::url(&upload_id),
        ),
        "application/xml".into(),
        body.clone(),
    )
    .await
    {
        return proxied;
    }
    let parts = match parse_complete_multipart(&body) {
        Ok(parts) => parts,
        Err(e) => return s3_multipart_err(&st, e),
    };
    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, _) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
    let s3 = st.s3.clone();
    let res = tokio::task::spawn_blocking(move || {
        s3.complete_multipart_upload(project_id, &store, &bucket, &key, &upload_id, &parts)
    })
    .await;
    match res {
        Ok(Ok(mf)) => (
            StatusCode::OK,
            [(header::ETAG, mf.etag.clone())],
            Json(serde_json::json!({
                "etag": mf.etag,
                "size": mf.size,
                "chunks": mf.chunks.len(),
                "pool": mf.pool,
            })),
        )
            .into_response(),
        Ok(Err(e)) => s3_multipart_err(&st, e),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Abandons a multipart upload. Its record and every still-live part intent are
/// retired in one transaction; the existing collector reclaims the fragments
/// after the documented grace.
async fn handle_s3_abort_multipart(
    st: AppState,
    headers: HeaderMap,
    project: String,
    store: String,
    bucket: String,
    key: String,
    upload_id: String,
) -> Response {
    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        Some(&project),
        "DELETE",
        format!(
            "/s3/{}/{}/{}/{}?uploadId={}",
            protocols::url(&project),
            protocols::url(&store),
            protocols::url(&bucket),
            key.split('/')
                .map(protocols::url)
                .collect::<Vec<_>>()
                .join("/"),
            protocols::url(&upload_id),
        ),
        "application/octet-stream".into(),
        Bytes::new(),
    )
    .await
    {
        return proxied;
    }
    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, _) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
    let s3 = st.s3.clone();
    let res = tokio::task::spawn_blocking(move || {
        s3.abort_multipart_upload(project_id, &store, &bucket, &key, &upload_id)
    })
    .await;
    match res {
        Ok(Ok(())) => Json(serde_json::json!({ "aborted": true })).into_response(),
        Ok(Err(e)) => s3_multipart_err(&st, e),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// `GET ...?uploadId=...` — the parts registered so far.
async fn handle_s3_list_parts(
    st: AppState,
    headers: HeaderMap,
    project: String,
    store: String,
    bucket: String,
    key: String,
    upload_id: String,
    part_number_marker: u32,
    max_parts: u32,
) -> Response {
    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, _) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadOnly) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
    let s3 = st.s3.clone();
    let res = tokio::task::spawn_blocking(move || {
        let parts = s3.list_multipart_parts(project_id, &store, &bucket, &key, &upload_id)?;
        Ok::<_, S3Error>((parts, bucket, key, upload_id))
    })
    .await;
    match res {
        Ok(Ok((parts, bucket, key, upload_id))) => {
            // Paginate before serializing so one response stays bounded even for
            // an upload with many parts. `parts` is ascending by part number.
            let after = parts
                .into_iter()
                .filter(|part| part.part_number > part_number_marker)
                .collect::<Vec<_>>();
            let is_truncated = after.len() > max_parts as usize;
            let shown = after
                .into_iter()
                .take(max_parts as usize)
                .collect::<Vec<_>>();
            let next_part_number_marker = if is_truncated {
                shown
                    .last()
                    .map(|part| part.part_number)
                    .unwrap_or(part_number_marker)
            } else {
                0
            };
            Json(serde_json::json!({
                "bucket": bucket,
                "key": key,
                "uploadId": upload_id,
                "partNumberMarker": part_number_marker,
                "maxParts": max_parts,
                "isTruncated": is_truncated,
                "nextPartNumberMarker": next_part_number_marker,
                "parts": shown.iter().map(|part| serde_json::json!({
                    "partNumber": part.part_number,
                    "etag": part.etag,
                    "size": part.size,
                    "lastModified": part.last_modified,
                })).collect::<Vec<_>>(),
            }))
            .into_response()
        }
        Ok(Err(e)) => s3_multipart_err(&st, e),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Reads the `<Part><PartNumber>`/`<ETag>` list out of a completion document.
///
/// The parser is deliberately narrow: it accepts the document S3 clients send
/// and rejects anything it cannot read as a whole, rather than guessing. The
/// client's list is the authority on what the object must contain, so a
/// partial extraction would publish a different object than the one asked for:
/// every token is consumed, only the expected elements are accepted, and any
/// leftover content after the root is refused. An empty list is refused here as
/// well as in the engine.
fn parse_complete_multipart(body: &[u8]) -> Result<Vec<(u32, String)>, S3Error> {
    let text = std::str::from_utf8(body)
        .map_err(|_| S3Error::MalformedMultipart("body is not UTF-8".into()))?;
    // A leading byte-order mark is an encoding signature, not content: XML
    // parsers consume it, and it may precede even the declaration.
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut input = text;
    // Only XML whitespace, comments, processing instructions and one XML
    // declaration may precede the root. The declaration is legal only as the
    // very first token of the document: a comment or any whitespace before it
    // is a malformed document, and one inside the root is not a token to skip.
    let mut first = true;
    loop {
        match xml_token(&mut input)? {
            XmlToken::Declaration if first => {
                first = false;
                continue;
            }
            XmlToken::Declaration => {
                return Err(S3Error::MalformedMultipart(
                    "an XML declaration is only allowed at the start of the document".into(),
                ))
            }
            XmlToken::Comment | XmlToken::ProcessingInstruction => {
                first = false;
                continue;
            }
            XmlToken::Text(text) if xml_is_ws(text) => {
                first = false;
                continue;
            }
            XmlToken::Open {
                name: "CompleteMultipartUpload",
                self_closing: false,
            } => break,
            _ => {
                return Err(S3Error::MalformedMultipart(
                    "expected a CompleteMultipartUpload document".into(),
                ))
            }
        }
    }
    let mut parts = Vec::new();
    loop {
        match xml_token(&mut input)? {
            XmlToken::Comment | XmlToken::ProcessingInstruction => continue,
            XmlToken::Text(text) if xml_is_ws(text) => continue,
            XmlToken::Open {
                name: "Part",
                self_closing: false,
            } => parts.push(read_complete_part(&mut input)?),
            XmlToken::Close {
                name: "CompleteMultipartUpload",
            } => break,
            _ => {
                return Err(S3Error::MalformedMultipart(
                    "unexpected content in CompleteMultipartUpload".into(),
                ))
            }
        }
    }
    loop {
        match xml_token(&mut input)? {
            XmlToken::Comment | XmlToken::ProcessingInstruction => continue,
            XmlToken::Text(text) if xml_is_ws(text) => continue,
            XmlToken::Eof => break,
            _ => {
                return Err(S3Error::MalformedMultipart(
                    "unexpected content after CompleteMultipartUpload".into(),
                ))
            }
        }
    }
    if parts.is_empty() {
        return Err(S3Error::MalformedMultipart(
            "at least one part is required".into(),
        ));
    }
    Ok(parts)
}

/// One `<Part>` element: exactly one `<PartNumber>` and one `<ETag>`, nothing
/// else. Duplicates are refused instead of letting the last one win.
fn read_complete_part(input: &mut &str) -> Result<(u32, String), S3Error> {
    let mut number = None;
    let mut etag = None;
    loop {
        match xml_token(input)? {
            XmlToken::Comment | XmlToken::ProcessingInstruction => continue,
            XmlToken::Text(text) if xml_is_ws(text) => continue,
            XmlToken::Open {
                name: "PartNumber",
                self_closing: false,
            } => {
                let value = read_element_text(input, "PartNumber")?;
                if number.replace(value).is_some() {
                    return Err(S3Error::MalformedMultipart(
                        "a <Part> has more than one <PartNumber>".into(),
                    ));
                }
            }
            XmlToken::Open {
                name: "ETag",
                self_closing: false,
            } => {
                let value = read_element_text(input, "ETag")?;
                if etag.replace(value).is_some() {
                    return Err(S3Error::MalformedMultipart(
                        "a <Part> has more than one <ETag>".into(),
                    ));
                }
            }
            XmlToken::Close { name: "Part" } => break,
            _ => {
                return Err(S3Error::MalformedMultipart(
                    "unexpected content in <Part>".into(),
                ))
            }
        }
    }
    let number =
        number.ok_or_else(|| S3Error::MalformedMultipart("a <Part> has no <PartNumber>".into()))?;
    let number = xml_trim_end(xml_trim_start(&number));
    let number = number
        .parse::<u32>()
        .map_err(|_| S3Error::MalformedMultipart(format!("invalid part number `{number}`")))?;
    let etag = etag.ok_or_else(|| S3Error::MalformedMultipart("a <Part> has no <ETag>".into()))?;
    Ok((number, xml_trim_end(xml_trim_start(&etag)).to_string()))
}

/// Reads the character content of one element and its closing tag.
///
/// Comments and processing instructions inside the element are not content in
/// XML, so they are skipped rather than treated as the value; their text is not
/// concatenated. Nested elements are refused: the expected values are plain
/// text, and `<PartNumber><a>1</a></PartNumber>` is not a number.
fn read_element_text(input: &mut &str, name: &str) -> Result<String, S3Error> {
    let mut text = String::new();
    loop {
        match xml_token(input)? {
            XmlToken::Comment | XmlToken::ProcessingInstruction => continue,
            XmlToken::Text(chunk) => text.push_str(&xml_decode(chunk, false)?),
            XmlToken::Close { name: closing } if closing == name => return Ok(text),
            _ => {
                return Err(S3Error::MalformedMultipart(format!(
                    "<{name}> does not contain plain text"
                )))
            }
        }
    }
}

/// One XML token.
///
/// Comments and processing instructions are reported to the caller instead of
/// being skipped here: whether one is legal depends on where it sits, and
/// silently dropping framing is what let a malformed document complete an
/// upload. `<!` constructs the tokenizer does not implement (CDATA, a DOCTYPE)
/// are refused rather than treated as text.
enum XmlToken<'a> {
    Open {
        name: &'a str,
        self_closing: bool,
    },
    Close {
        name: &'a str,
    },
    /// `<?xml ...?>` with an already-validated body. Legal only as the very
    /// first token of the document.
    Declaration,
    Comment,
    ProcessingInstruction,
    Text(&'a str),
    Eof,
}

/// XML 1.0 `S`: space, tab, CR or LF.
///
/// Rust's `trim` also eats NBSP, U+2028 and the rest of Unicode whitespace,
/// which XML does not permit inside markup. Using it would accept
/// `<Part a="1"\u{a0}b="2">` as two attributes.
fn is_xml_ws(ch: char) -> bool {
    matches!(ch, ' ' | '\t' | '\r' | '\n')
}

/// XML 1.0 `Char`: the characters a document may contain at all. Control
/// characters other than tab/CR/LF, and the surrogate range, are excluded.
fn is_xml_char(ch: char) -> bool {
    matches!(ch, '\u{9}' | '\u{A}' | '\u{D}')
        || ('\u{20}'..='\u{D7FF}').contains(&ch)
        || ('\u{E000}'..='\u{FFFD}').contains(&ch)
        || ch >= '\u{10000}'
}

fn xml_trim_start(value: &str) -> &str {
    value.trim_start_matches(is_xml_ws)
}

fn xml_trim_end(value: &str) -> &str {
    value.trim_end_matches(is_xml_ws)
}

fn xml_is_ws(value: &str) -> bool {
    value.chars().all(is_xml_ws)
}

fn is_xml_name_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || ch == ':'
}

fn is_xml_name_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':')
}

/// Byte length of the `Name` at the start of `value`, or `None` when it does
/// not start with one. The completion document only uses ASCII names, so the
/// ASCII subset of the XML production is both sufficient and strict.
fn xml_name_len(value: &str) -> Option<usize> {
    let mut chars = value.char_indices();
    let (_, first) = chars.next()?;
    if !is_xml_name_start(first) {
        return None;
    }
    let mut end = first.len_utf8();
    for (index, ch) in chars {
        if !is_xml_name_char(ch) {
            break;
        }
        end = index + ch.len_utf8();
    }
    Some(end)
}

fn xml_token<'a>(input: &mut &'a str) -> Result<XmlToken<'a>, S3Error> {
    if input.is_empty() {
        return Ok(XmlToken::Eof);
    }
    if let Some(rest) = input.strip_prefix("<?") {
        let end = rest.find("?>").ok_or_else(|| {
            S3Error::MalformedMultipart("unterminated processing instruction".into())
        })?;
        let body = &rest[..end];
        *input = &rest[end + 2..];
        let name_len = xml_name_len(body).ok_or_else(|| {
            S3Error::MalformedMultipart("processing instruction without a valid target".into())
        })?;
        let target = &body[..name_len];
        let data = &body[name_len..];
        // The target is a Name, followed by whitespace or by the end of the
        // instruction. `<?1?>` and `<?xml?>`-style targets are not Names.
        if !data.is_empty() && !data.starts_with(is_xml_ws) {
            return Err(S3Error::MalformedMultipart(
                "processing instruction target must be followed by whitespace".into(),
            ));
        }
        if !data.chars().all(is_xml_char) {
            return Err(S3Error::MalformedMultipart(
                "invalid character in processing instruction".into(),
            ));
        }
        if target == "xml" {
            // `xml` is reserved for the declaration; its body is a real grammar,
            // not free text, so it is validated here.
            xml_validate_declaration(data)?;
            return Ok(XmlToken::Declaration);
        }
        if target.eq_ignore_ascii_case("xml") {
            return Err(S3Error::MalformedMultipart(
                "reserved processing-instruction target".into(),
            ));
        }
        return Ok(XmlToken::ProcessingInstruction);
    }
    if let Some(rest) = input.strip_prefix("<!--") {
        let end = rest
            .find("-->")
            .ok_or_else(|| S3Error::MalformedMultipart("unterminated XML comment".into()))?;
        let body = &rest[..end];
        // XML forbids `--` inside a comment and a comment ending in `-`.
        if body.contains("--") || body.ends_with('-') {
            return Err(S3Error::MalformedMultipart("malformed XML comment".into()));
        }
        if !body.chars().all(is_xml_char) {
            return Err(S3Error::MalformedMultipart(
                "invalid character in XML comment".into(),
            ));
        }
        *input = &rest[end + 3..];
        return Ok(XmlToken::Comment);
    }
    if input.starts_with("<!") {
        return Err(S3Error::MalformedMultipart(
            "unsupported XML declaration or CDATA section".into(),
        ));
    }
    if let Some(rest) = input.strip_prefix("</") {
        let end = rest
            .find('>')
            .ok_or_else(|| S3Error::MalformedMultipart("unterminated closing tag".into()))?;
        let inner = &rest[..end];
        // `</Name S? >`. No whitespace may follow `</`: `</ Part>` names no
        // element in XML and must not be read as `</Part>`.
        let name_len = xml_name_len(inner)
            .ok_or_else(|| S3Error::MalformedMultipart("invalid closing tag".into()))?;
        let name = &inner[..name_len];
        if !xml_is_ws(&inner[name_len..]) {
            return Err(S3Error::MalformedMultipart("invalid closing tag".into()));
        }
        *input = &rest[end + 1..];
        return Ok(XmlToken::Close { name });
    }
    if let Some(rest) = input.strip_prefix('<') {
        let end = xml_tag_end(rest)?;
        let inner = &rest[..end];
        let name_len = xml_name_len(inner)
            .ok_or_else(|| S3Error::MalformedMultipart("invalid opening tag".into()))?;
        let name = &inner[..name_len];
        // `EmptyElemTag` ends with `/>`: the slash must be adjacent to `>`, so
        // `<Part a="1" / >` is malformed rather than a self-closing tag with a
        // stray slash. The raw tail is used, before trimming.
        let after_name = &inner[name_len..];
        let (tail, self_closing) = match after_name.strip_suffix('/') {
            Some(before) => (xml_trim_end(before), true),
            None => (xml_trim_end(after_name), false),
        };
        // Everything after the element name is an attribute list. It is
        // validated instead of discarded: an unvalidated attribute can hide the
        // real tag end, so `<Part ignored="><PartNumber>1<...` used to be read as
        // a normal `<Part>` and could publish an object.
        xml_validate_attributes(tail)?;
        *input = &rest[end + 1..];
        return Ok(XmlToken::Open { name, self_closing });
    }
    let end = input.find('<').unwrap_or(input.len());
    let text = &input[..end];
    *input = &input[end..];
    // `]]>` may not appear in character data, and no literal character may be
    // outside XML's `Char` production.
    if text.contains("]]>") {
        return Err(S3Error::MalformedMultipart(
            "`]]>` is not allowed in character data".into(),
        ));
    }
    if !text.chars().all(is_xml_char) {
        return Err(S3Error::MalformedMultipart(
            "invalid character in character data".into(),
        ));
    }
    Ok(XmlToken::Text(text))
}

/// Index of the `>` that closes an opening tag, ignoring a `>` inside an
/// attribute value. A tag whose quote never closes is unterminated, not a tag
/// that ends at the first `>`: treating the latter as the end is how a
/// malformed attribute could smuggle a real element past the parser.
fn xml_tag_end(rest: &str) -> Result<usize, S3Error> {
    let mut quote: Option<char> = None;
    for (index, ch) in rest.char_indices() {
        match quote {
            Some(q) if ch == q => quote = None,
            Some(_) => {}
            None if ch == '"' || ch == '\'' => quote = Some(ch),
            None if ch == '>' => return Ok(index),
            None if ch == '<' => {
                return Err(S3Error::MalformedMultipart(
                    "unterminated opening tag".into(),
                ))
            }
            None => {}
        }
    }
    Err(S3Error::MalformedMultipart(if quote.is_some() {
        "unterminated attribute value".into()
    } else {
        "unterminated opening tag".into()
    }))
}

/// Validates the attribute list of an opening tag: `Name S? = S? "value"`,
/// separated by XML whitespace, nothing else. The completion document needs no
/// attribute semantics, but its syntax must still be well-formed — an unquoted
/// or unterminated value is a malformed document, not something to ignore.
fn xml_validate_attributes(mut rest: &str) -> Result<(), S3Error> {
    let malformed = || S3Error::MalformedMultipart("malformed attribute list".into());
    let mut names: BTreeSet<&str> = BTreeSet::new();
    while !rest.is_empty() {
        // Every attribute must be preceded by XML whitespace, so `<Part a="1"b="2">`
        // and `<Part\u{a0}a="1">` are not two attributes.
        if !rest.starts_with(is_xml_ws) {
            return Err(malformed());
        }
        rest = xml_trim_start(rest);
        if rest.is_empty() {
            break;
        }
        let name_len = xml_name_len(rest).ok_or_else(malformed)?;
        let name = &rest[..name_len];
        // XML forbids repeating an attribute name on one element.
        if !names.insert(name) {
            return Err(malformed());
        }
        rest = xml_trim_start(&rest[name_len..]);
        rest = rest.strip_prefix('=').ok_or_else(malformed)?;
        rest = xml_trim_start(rest);
        let quote = match rest.chars().next() {
            Some(q @ ('"' | '\'')) => q,
            _ => return Err(malformed()),
        };
        let value = &rest[quote.len_utf8()..];
        let end = value
            .find(quote)
            .ok_or_else(|| S3Error::MalformedMultipart("unterminated attribute value".into()))?;
        // Decodes nothing here, but validates the value: no raw `<`, only
        // permitted characters, and only declared entities or character
        // references.
        xml_decode(&value[..end], true)?;
        rest = &value[end + quote.len_utf8()..];
    }
    Ok(())
}

/// One `Name S? = S? "value"` pseudo-attribute of the XML declaration, or
/// `None` when the text does not start with XML whitespace (nothing left).
fn xml_pseudo_attribute<'a>(rest: &mut &'a str) -> Result<Option<(&'a str, String)>, S3Error> {
    let malformed = || S3Error::MalformedMultipart("malformed XML declaration".into());
    let base: &'a str = *rest;
    if !base.starts_with(is_xml_ws) {
        return Ok(None);
    }
    let mut cursor: &'a str = xml_trim_start(base);
    if cursor.is_empty() {
        *rest = cursor;
        return Ok(None);
    }
    let name_len = xml_name_len(cursor).ok_or_else(malformed)?;
    let name = &cursor[..name_len];
    cursor = xml_trim_start(&cursor[name_len..]);
    cursor = cursor.strip_prefix('=').ok_or_else(malformed)?;
    cursor = xml_trim_start(cursor);
    let quote = match cursor.chars().next() {
        Some(q @ ('"' | '\'')) => q,
        _ => return Err(malformed()),
    };
    let value = &cursor[quote.len_utf8()..];
    let end = value.find(quote).ok_or_else(malformed)?;
    let decoded = xml_decode(&value[..end], true)?;
    *rest = &value[end + quote.len_utf8()..];
    Ok(Some((name, decoded)))
}

/// Validates an XML declaration's body (the text between `<?xml` and `?>`): a
/// mandatory `version`, then optional `encoding` and `standalone`, in that
/// order, and nothing else. XML 1.0 only.
fn xml_validate_declaration(data: &str) -> Result<(), S3Error> {
    let malformed = || S3Error::MalformedMultipart("malformed XML declaration".into());
    let mut rest = data;
    let (name, version) = xml_pseudo_attribute(&mut rest)?.ok_or_else(malformed)?;
    if name != "version" || !is_version_num(&version) {
        return Err(malformed());
    }
    let mut encoding_seen = false;
    let mut standalone_seen = false;
    while let Some((name, value)) = xml_pseudo_attribute(&mut rest)? {
        match name {
            "encoding" if !encoding_seen && !standalone_seen => {
                if !is_enc_name(&value) {
                    return Err(malformed());
                }
                encoding_seen = true;
            }
            "standalone" if !standalone_seen => {
                if value != "yes" && value != "no" {
                    return Err(malformed());
                }
                standalone_seen = true;
            }
            _ => return Err(malformed()),
        }
    }
    if !xml_is_ws(rest) {
        return Err(malformed());
    }
    Ok(())
}

fn is_version_num(value: &str) -> bool {
    value
        .strip_prefix("1.")
        .is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
}

fn is_enc_name(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(ch) if ch.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
}

/// Validates XML character data and decodes its references.
///
/// `reject_lt` is set for attribute values, where a raw `<` is forbidden; a
/// text token can never contain one because the tokenizer splits on `<`.
/// Every `&` must start a declared entity or a character reference, and every
/// literal character must be a permitted XML character.
fn xml_decode(value: &str, reject_lt: bool) -> Result<String, S3Error> {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    loop {
        let Some(amp) = rest.find('&') else {
            push_literal(&mut out, rest, reject_lt)?;
            return Ok(out);
        };
        push_literal(&mut out, &rest[..amp], reject_lt)?;
        let after = &rest[amp + 1..];
        let end = after
            .find(';')
            .ok_or_else(|| S3Error::MalformedMultipart("unterminated entity reference".into()))?;
        let entity = &after[..end];
        match entity {
            "amp" => out.push('&'),
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ => out.push(xml_char_reference(entity).ok_or_else(|| {
                S3Error::MalformedMultipart(format!("unknown entity reference `&{entity};`"))
            })?),
        }
        rest = &after[end + 1..];
    }
}

fn push_literal(out: &mut String, literal: &str, reject_lt: bool) -> Result<(), S3Error> {
    if reject_lt && literal.contains('<') {
        return Err(S3Error::MalformedMultipart(
            "malformed attribute value".into(),
        ));
    }
    if !literal.chars().all(is_xml_char) {
        return Err(S3Error::MalformedMultipart(
            "invalid character in character data".into(),
        ));
    }
    out.push_str(literal);
    Ok(())
}

/// `#123` or `#x1F`: a character reference naming a permitted XML character.
/// The `x` must be lowercase, the digits must not overflow, and the code point
/// must be a legal XML character (`&#0;` and `&#xD800;` are not).
fn xml_char_reference(entity: &str) -> Option<char> {
    let digits = entity.strip_prefix('#')?;
    let (radix, digits) = match digits.strip_prefix('x') {
        Some(hex) => (16, hex),
        None => (10, digits),
    };
    if digits.is_empty()
        || !digits.bytes().all(|b| match radix {
            16 => b.is_ascii_hexdigit(),
            _ => b.is_ascii_digit(),
        })
    {
        return None;
    }
    let value = u32::from_str_radix(digits, radix).ok()?;
    char::from_u32(value).filter(|ch| is_xml_char(*ch))
}

/// `POST /s3/{project}/{store}/{bucket}/{key}`: create or complete a multipart
/// upload, selected by the subresource the client sent.
async fn handle_s3_multipart(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, store, bucket, key)): Path<(String, String, String, String)>,
    Query(q): Query<S3WriteQuery>,
    body: Bytes,
) -> Response {
    if q.uploads.is_some() {
        return handle_s3_create_multipart(st, headers, project, store, bucket, key).await;
    }
    match q.upload_id {
        Some(upload_id) => {
            handle_s3_complete_multipart(st, headers, project, store, bucket, key, upload_id, body)
                .await
        }
        None => api_err(
            StatusCode::NOT_IMPLEMENTED,
            "unsupported S3 POST subresource",
        ),
    }
}

async fn handle_s3_put(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, store, bucket, key)): Path<(String, String, String, String)>,
    Query(q): Query<S3WriteQuery>,
    body: Bytes,
) -> Response {
    if let Some(upload_id) = q.upload_id {
        let Some(part_number) = q.part_number else {
            return api_err(
                StatusCode::BAD_REQUEST,
                "partNumber is required when uploadId is present",
            );
        };
        return handle_s3_upload_part(
            st,
            headers,
            project,
            store,
            bucket,
            key,
            upload_id,
            part_number,
            body,
        )
        .await;
    }
    if q.part_number.is_some() || q.uploads.is_some() {
        return api_err(
            StatusCode::BAD_REQUEST,
            "partNumber requires uploadId, and uploads is only valid on POST",
        );
    }
    if headers.contains_key("idempotency-key") {
        // A legacy runtime has no record store, so the key is refused before
        // authorization or any fragment write.
        if !st.meta.is_openraft() {
            return unsupported_idempotency();
        }
        return keyed_s3_put(&st, &headers, &project, &store, &bucket, &key, &body).await;
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        Some(&project),
        "PUT",
        format!(
            "/s3/{}/{}/{}/{}",
            protocols::url(&project),
            protocols::url(&store),
            protocols::url(&bucket),
            key.split('/')
                .map(protocols::url)
                .collect::<Vec<_>>()
                .join("/")
        ),
        content_type.clone(),
        body.clone(),
    )
    .await
    {
        return proxied;
    }

    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, pool_name) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };

    let s3 = st.s3.clone();
    let res = tokio::task::spawn_blocking(move || {
        s3.put_object(
            project_id,
            &store,
            &pool_name,
            &bucket,
            &key,
            &content_type,
            &body,
        )
    })
    .await;

    match res {
        Ok(Ok(mf)) => (
            StatusCode::OK,
            [(header::ETAG, mf.etag.clone())],
            Json(serde_json::json!({
                "etag": mf.etag,
                "size": mf.size,
                "chunks": mf.chunks.len(),
                "pool": mf.pool,
            })),
        )
            .into_response(),
        Ok(Err(S3Error::Meta(me))) => meta_err_resp(&st, me),
        // The pool refused the write because it could not reach full redundancy.
        // Nothing was published, so this is availability, not a corrupt object.
        Ok(Err(S3Error::Blob(BlobError::RedundancyNotMet { .. }))) => api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage pool cannot reach full redundancy; the write was refused",
        ),
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Deserialize)]
struct S3GetQuery {
    /// Opt-in quorum-confirmed read of the object manifest. Absent or `false`
    /// keeps the default local applied-state behaviour.
    #[serde(default)]
    consistent: bool,
}

/// A manifest obtained from a quorum-confirmed snapshot is a point-in-time
/// view, not a GC fence: a newer overwrite may have retired a chunk before the
/// read fetches it. For a consistent read that is an availability failure
/// (503), never zeros or a silent skip. The default local path keeps its
/// existing mapping.
fn is_stale_chunk_error(err: &S3Error) -> bool {
    matches!(
        err,
        S3Error::Blob(
            BlobError::NotFound(_)
                | BlobError::Tombstoned(_)
                | BlobError::InsufficientFragments { .. }
        )
    )
}

fn s3_read_err(st: &AppState, err: S3Error) -> Response {
    match err {
        S3Error::ObjectNotFound { .. } => api_err(StatusCode::NOT_FOUND, "object not found"),
        S3Error::Meta(me) => meta_err_resp(st, me),
        S3Error::InvalidRange { .. } => {
            api_err(StatusCode::RANGE_NOT_SATISFIABLE, "range out of bounds")
        }
        err if is_stale_chunk_error(&err) => {
            api_err(StatusCode::SERVICE_UNAVAILABLE, err.to_string())
        }
        err => api_err(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn handle_s3_get(
    method: Method,
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, store, bucket, key)): Path<(String, String, String, String)>,
    Query(q): Query<S3GetQuery>,
) -> Response {
    let head = method == Method::HEAD;
    if q.consistent && !st.meta.is_openraft() {
        // Legacy metadata has no quorum-confirmed snapshot. Serving the locally
        // applied manifest would be a silent stale read, which is what the
        // option exists to refuse.
        return api_err(
            StatusCode::NOT_IMPLEMENTED,
            "consistent reads require OpenRaft metadata; this node cannot take a quorum-confirmed snapshot",
        );
    }
    // Every GET runs on the project-group leader: its reader lease is a
    // committed metadata write only the leader can publish. A follower that
    // served the read locally would run unfenced. A consistent HEAD also needs
    // the leader's quorum-confirmed snapshot; a plain HEAD reads no fragments
    // and stays local.
    if !head || q.consistent {
        let mut path = format!(
            "/s3/{}/{}/{}/{}",
            protocols::url(&project),
            protocols::url(&store),
            protocols::url(&bucket),
            key.split('/')
                .map(protocols::url)
                .collect::<Vec<_>>()
                .join("/")
        );
        if q.consistent {
            path.push_str("?consistent=true");
        }
        if let Some(resp) = maybe_forward_write(
            &st,
            &headers,
            Some(&project),
            if head { "HEAD" } else { "GET" },
            path,
            "application/octet-stream".into(),
            Bytes::new(),
        )
        .await
        {
            return resp;
        }
    }

    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, _) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadOnly) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };

    let range_hdr = headers
        .get(header::RANGE)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string);

    if q.consistent {
        return s3_get_consistent(
            st, project_id, store, bucket, key, &headers, range_hdr, head,
        )
        .await;
    }

    // A HEAD is answered from the manifest alone: it reads no fragments, so
    // there is nothing for a reader lease to pin and nothing to forward.
    if head {
        return s3_head_local(&st, project_id, &store, &bucket, &key, &headers).await;
    }

    // Fence the chunk read against collection. The manifest read and the chunk
    // fetch must both happen under one lease: an overwrite or delete retires the
    // previous manifest, so the chunks this read is about to fetch can become
    // unreferenced between the two steps.
    //
    // The record lives in the group that owns the project, and on Multi-Raft
    // its publication is a committed group write on the leader the request was
    // forwarded to above. A read that starts during a collector's admission
    // window loses that write's race with the collector's closed admission and
    // is refused instead of reading fragments the pass is deleting.
    let st_fence = st.clone();
    let lease =
        match tokio::task::spawn_blocking(move || st_fence.begin_read_lease(project_id)).await {
            Ok(Ok(lease)) => lease,
            Ok(Err(error)) => return api_err(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
            Err(error) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
        };

    let response = s3_get_fenced(
        &st,
        project_id,
        &store,
        &bucket,
        &key,
        &headers,
        range_hdr.as_deref(),
    )
    .await;

    let st_fence = st.clone();
    match tokio::task::spawn_blocking(move || st_fence.end_read_lease(&lease)).await {
        Ok(Ok(())) => response,
        // The manifest read and the chunk fetch both happened under this lease;
        // if it did not survive them, a pass may have reclaimed the chunks in
        // between and the answer cannot be vouched for.
        Ok(Err(error)) => api_err(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
        Err(error) => api_err(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

/// One unfenced HEAD: the manifest is read once and answered directly. No
/// fragment is fetched, so no reader lease is needed.
async fn s3_head_local(
    st: &AppState,
    project_id: u32,
    store: &str,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Response {
    let s3 = st.s3.clone();
    let (store, bucket, key) = (store.to_string(), bucket.to_string(), key.to_string());
    let head =
        tokio::task::spawn_blocking(move || s3.head_object(project_id, &store, &bucket, &key))
            .await;
    let mf = match head {
        Ok(Ok(mf)) => mf,
        Ok(Err(S3Error::ObjectNotFound { .. })) => {
            return api_err(StatusCode::NOT_FOUND, "object not found")
        }
        Ok(Err(e)) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    match evaluate_conditions(headers, &mf.etag) {
        Conditional::Passed => object_head_response(mf),
        Conditional::Failed => api_err(StatusCode::PRECONDITION_FAILED, "ETag mismatch"),
        Conditional::NotModified => StatusCode::NOT_MODIFIED.into_response(),
    }
}

/// One fenced legacy read. The manifest is read once, and that same manifest
/// decides the conditional result, the range, the size rule and the chunk ids
/// that are fetched: a second metadata read would let an overwrite make the
/// slice description disagree with the bytes.
async fn s3_get_fenced(
    st: &AppState,
    project_id: u32,
    store: &str,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    range_hdr: Option<&str>,
) -> Response {
    let s3 = st.s3.clone();
    let (store, bucket, key) = (store.to_string(), bucket.to_string(), key.to_string());
    let head =
        tokio::task::spawn_blocking(move || s3.head_object(project_id, &store, &bucket, &key))
            .await;
    let mf = match head {
        Ok(Ok(mf)) => mf,
        Ok(Err(S3Error::ObjectNotFound { .. })) => {
            return api_err(StatusCode::NOT_FOUND, "object not found")
        }
        Ok(Err(e)) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    match evaluate_conditions(headers, &mf.etag) {
        Conditional::Passed => {}
        Conditional::Failed => {
            return api_err(StatusCode::PRECONDITION_FAILED, "ETag mismatch");
        }
        Conditional::NotModified => return StatusCode::NOT_MODIFIED.into_response(),
    }
    let range = match range_decision(range_hdr, mf.size) {
        RangeDecision::Serve(range) => range,
        RangeDecision::Unsatisfiable => {
            // Keep the size hint a client uses to retry.
            let mut response = api_err(StatusCode::RANGE_NOT_SATISFIABLE, "range not satisfiable");
            if let Ok(value) = format!("bytes */{}", mf.size).parse() {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
            return response;
        }
        RangeDecision::TooLarge => {
            return api_err(
                StatusCode::BAD_REQUEST,
                if range_hdr.is_some() {
                    "range exceeds 16 MiB"
                } else {
                    "use Range for objects over 16 MiB"
                },
            );
        }
    };
    let (start, end) = range.unwrap_or((0, mf.size.saturating_sub(1)));
    let s3 = st.s3.clone();
    let snapshot = mf.clone();
    let res = tokio::task::spawn_blocking(move || {
        if snapshot.size == 0 {
            Ok(Vec::new())
        } else {
            s3.read_manifest_range(&snapshot, start, end)
        }
    })
    .await;
    let bytes = match res {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    object_read_response(mf, range, bytes)
}

/// Quorum-confirmed GET. The manifest comes from exactly one linearizable
/// snapshot; the chunks are then fetched by the ids that manifest names. There
/// is no fallback to the locally applied manifest and no retry.
///
/// The quorum-confirmed snapshot is not a GC fence: an overwrite or delete
/// retires the manifest it read, and a pass can reclaim the chunks it names
/// before they are fetched. A reader lease is therefore taken before the
/// snapshot and held across the chunk fetch, in the group that owns the
/// project, exactly as on the non-consistent path. A read that starts during a
/// collector's admission window loses that lease write and is refused. A HEAD
/// reads no fragments and takes no lease.
async fn s3_get_consistent(
    st: AppState,
    project_id: u32,
    store: String,
    bucket: String,
    key: String,
    headers: &HeaderMap,
    range_hdr: Option<String>,
    head_request: bool,
) -> Response {
    if head_request {
        return s3_get_consistent_fenced(
            &st, project_id, store, bucket, key, headers, range_hdr, true,
        )
        .await;
    }
    let st_fence = st.clone();
    let lease =
        match tokio::task::spawn_blocking(move || st_fence.begin_read_lease(project_id)).await {
            Ok(Ok(lease)) => lease,
            Ok(Err(error)) => return api_err(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
            Err(error) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
        };
    let response = s3_get_consistent_fenced(
        &st, project_id, store, bucket, key, headers, range_hdr, false,
    )
    .await;
    let st_fence = st.clone();
    match tokio::task::spawn_blocking(move || st_fence.end_read_lease(&lease)).await {
        Ok(Ok(())) => response,
        // The snapshot and the chunk fetch both happened under this lease; if
        // it did not survive them, a pass may have reclaimed the chunks in
        // between and the answer cannot be vouched for.
        Ok(Err(error)) => api_err(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
        Err(error) => api_err(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn s3_get_consistent_fenced(
    st: &AppState,
    project_id: u32,
    store: String,
    bucket: String,
    key: String,
    headers: &HeaderMap,
    range_hdr: Option<String>,
    head_request: bool,
) -> Response {
    let s3 = st.s3.clone();
    let (s, b, k) = (store.clone(), bucket.clone(), key.clone());
    let lookup =
        tokio::task::spawn_blocking(move || s3.head_object_consistent(project_id, &s, &b, &k))
            .await;
    let mf = match lookup {
        Ok(Ok(mf)) => mf,
        Ok(Err(e)) => return s3_read_err(st, e),
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    match evaluate_conditions(headers, &mf.etag) {
        Conditional::Passed => {}
        Conditional::Failed => {
            return api_err(StatusCode::PRECONDITION_FAILED, "ETag mismatch");
        }
        Conditional::NotModified => return StatusCode::NOT_MODIFIED.into_response(),
    }
    // A HEAD is answered from the manifest alone: it needs no fragments and no
    // range, so the GET body cap must not turn a metadata read into an error.
    if head_request {
        return object_head_response(mf);
    }
    let range = match range_decision(range_hdr.as_deref(), mf.size) {
        RangeDecision::Serve(range) => range,
        RangeDecision::Unsatisfiable => {
            let mut response = api_err(StatusCode::RANGE_NOT_SATISFIABLE, "range not satisfiable");
            if let Ok(value) = format!("bytes */{}", mf.size).parse() {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
            return response;
        }
        RangeDecision::TooLarge => {
            return api_err(
                StatusCode::BAD_REQUEST,
                if range_hdr.is_some() {
                    "range exceeds 16 MiB"
                } else {
                    "use Range for objects over 16 MiB"
                },
            );
        }
    };
    let (start, end) = range.unwrap_or((0, mf.size.saturating_sub(1)));
    let s3 = st.s3.clone();
    let snapshot = mf.clone();
    let res = tokio::task::spawn_blocking(move || {
        if snapshot.size == 0 {
            Ok(Vec::new())
        } else {
            s3.read_manifest_range(&snapshot, start, end)
        }
    })
    .await;
    match res {
        Ok(Ok(bytes)) => object_read_response(mf, range, bytes),
        Ok(Err(e)) => s3_read_err(st, e),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// A HEAD answer built from the manifest: the same metadata a GET reports, no
/// body and no fragment read.
fn object_head_response(mf: ObjectManifest) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mf.content_type)
        .header(header::ETAG, mf.etag)
        .header(header::LAST_MODIFIED, http_date(mf.last_modified))
        .header(header::CONTENT_LENGTH, mf.size)
        .header(header::ACCEPT_RANGES, "bytes")
        .body(axum::body::Body::empty())
        .unwrap_or_else(|e| api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// Renders one object read: the slice description, the version and the
/// timestamps all come from the same manifest the bytes were read with.
fn object_read_response(mf: ObjectManifest, range: Option<(u64, u64)>, bytes: Vec<u8>) -> Response {
    let mut response = Response::builder()
        .status(if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header(header::CONTENT_TYPE, mf.content_type)
        .header(header::ETAG, mf.etag)
        .header(header::LAST_MODIFIED, http_date(mf.last_modified))
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some((start, end)) = range {
        response = response.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{}", mf.size),
        );
    }
    response
        .body(axum::body::Body::from(bytes))
        .unwrap_or_else(|e| api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn handle_s3_delete(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, store, bucket, key)): Path<(String, String, String, String)>,
    Query(q): Query<S3WriteQuery>,
) -> Response {
    if let Some(upload_id) = q.upload_id {
        return handle_s3_abort_multipart(st, headers, project, store, bucket, key, upload_id)
            .await;
    }
    if headers.contains_key("idempotency-key") {
        if !st.meta.is_openraft() {
            return unsupported_idempotency();
        }
        return keyed_s3_delete(&st, &headers, &project, &store, &bucket, &key).await;
    }
    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        Some(&project),
        "DELETE",
        format!(
            "/s3/{}/{}/{}/{}",
            protocols::url(&project),
            protocols::url(&store),
            protocols::url(&bucket),
            key.split('/')
                .map(protocols::url)
                .collect::<Vec<_>>()
                .join("/")
        ),
        "application/octet-stream".to_string(),
        Bytes::new(),
    )
    .await
    {
        return proxied;
    }

    let subject = extract_subject(&headers, st.subject_secret.as_deref());
    let (project_id, _) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };

    let s3 = st.s3.clone();
    let res =
        tokio::task::spawn_blocking(move || s3.delete_object(project_id, &store, &bucket, &key))
            .await;

    match res {
        Ok(Ok(existed)) => Json(serde_json::json!({ "deleted": existed })).into_response(),
        Ok(Err(S3Error::Meta(me))) => meta_err_resp(&st, me),
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Keyed S3 PUT: the manifest publication, its retirement of the previous
/// manifest, the intent deletion, the idempotency record and the admission
/// counter all commit in one guarded `RevisionTxn`
/// (`ObjectStoreEngine::put_object_with`).
///
/// The response is derived from the manifest that is about to be published, so
/// the replayed body is exactly the `etag`/`size`/`chunks`/`pool` JSON and the
/// `ETag` header the success path returns — never an invented S3 XML shape.
/// Fragments and the upload intent are written before the publication; a
/// refusal or a conflict leaves them unreferenced for the orphan scan, and
/// nothing rolls them back.
async fn keyed_s3_put(
    st: &AppState,
    headers: &HeaderMap,
    project: &str,
    store: &str,
    bucket: &str,
    key: &str,
    body: &Bytes,
) -> Response {
    let key_id = match idempotency_header(headers) {
        Ok(Some(key)) => key,
        Ok(None) => unreachable!("the keyed path is chosen from this header"),
        Err(e) => return e,
    };
    let Some(subject) = verified_subject(headers, st.subject_secret.as_deref()) else {
        return ControlAdminError::Unverified.into_response();
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    if let Some(resp) = maybe_forward_write(
        st,
        headers,
        Some(project),
        "PUT",
        format!(
            "/s3/{}/{}/{}/{}",
            protocols::url(project),
            protocols::url(store),
            protocols::url(bucket),
            key.split('/')
                .map(protocols::url)
                .collect::<Vec<_>>()
                .join("/")
        ),
        content_type.clone(),
        body.clone(),
    )
    .await
    {
        return resp;
    }
    let (pid, pool_name) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, project, store, bucket, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
    let scope = match project_idempotency_scope(st, pid).await {
        Ok(scope) => scope,
        Err(resp) => return resp,
    };
    let record_key = project_record_key(&scope.records, &subject, &key_id);
    let fingerprint = s3_object_fingerprint("PUT", "s3-put", pid, store, bucket, key, body);
    // Fast path: replay without writing a fragment or an intent.
    let pre = {
        let meta = st.meta.clone();
        let record_key = record_key.clone();
        let usage_key = scope.usage_key.clone();
        match tokio::task::spawn_blocking(move || {
            read_project_apply_snapshot(&meta, &record_key, &usage_key)
        })
        .await
        {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(e)) => return api_err(StatusCode::SERVICE_UNAVAILABLE, e),
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    };
    if let Some(record) = pre.record {
        return replay_record(record, &fingerprint);
    }
    if pre.usage.records >= MAX_IDEMPOTENCY_RECORDS {
        return idempotency_quota_response();
    }
    // `prepare` runs immediately before the single publication attempt, so the
    // record and counter are read at the moment of the transaction. A record
    // that appeared means a concurrent attempt already won this key. The
    // refusal is reported through the captured response; `ConcurrentModification`
    // is only the error the engine needs to propagate.
    let abort: std::sync::Arc<std::sync::Mutex<Option<Response>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let record_out: std::sync::Arc<std::sync::Mutex<Option<IdempotencyRecord>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let abort_in = abort.clone();
    let record_out_in = record_out.clone();
    let meta = st.meta.clone();
    let record_key_in = record_key.clone();
    let usage_key_in = scope.usage_key.clone();
    let fingerprint_in = fingerprint.clone();
    let s3 = st.s3.clone();
    let store_in = store.to_owned();
    let pool_in = pool_name.clone();
    let bucket_in = bucket.to_owned();
    let key_in = key.to_owned();
    let content_type_in = content_type.clone();
    let body_in = body.clone();
    let res = tokio::task::spawn_blocking(move || {
        s3.put_object_with(
            pid,
            &store_in,
            &pool_in,
            &bucket_in,
            &key_in,
            &content_type_in,
            &body_in,
            |manifest: &yadb_s3::ObjectManifest| {
                let snapshot = match meta
                    .try_linearizable_snapshot(&[record_key_in.clone(), usage_key_in.clone()], &[])
                {
                    Ok(snapshot) => snapshot,
                    Err(e) => {
                        *abort_in.lock().unwrap() =
                            Some(api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()));
                        return Err(S3Error::ConcurrentModification);
                    }
                };
                if let Some(kv) = snapshot.get(&record_key_in) {
                    let response = match serde_json::from_slice::<IdempotencyRecord>(&kv.value) {
                        Ok(record) => replay_record(record, &fingerprint_in),
                        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                    };
                    *abort_in.lock().unwrap() = Some(response);
                    return Err(S3Error::ConcurrentModification);
                }
                let (usage, usage_revision) = match snapshot.get(&usage_key_in) {
                    Some(kv) => match decode_usage(&kv.value) {
                        Ok(usage) => (usage, kv.mod_revision),
                        Err(e) => {
                            *abort_in.lock().unwrap() =
                                Some(api_err(StatusCode::INTERNAL_SERVER_ERROR, e));
                            return Err(S3Error::ConcurrentModification);
                        }
                    },
                    None => (
                        IdempotencyUsage {
                            version: 1,
                            ..IdempotencyUsage::default()
                        },
                        0,
                    ),
                };
                // Exactly what `handle_s3_put` returns on success, including
                // the ETag header, so a replay is indistinguishable.
                let response_body = match serde_json::to_vec(&serde_json::json!({
                    "etag": manifest.etag,
                    "size": manifest.size,
                    "chunks": manifest.chunks.len(),
                    "pool": manifest.pool,
                })) {
                    Ok(body) => body,
                    Err(e) => {
                        *abort_in.lock().unwrap() =
                            Some(api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
                        return Err(S3Error::ConcurrentModification);
                    }
                };
                let record = IdempotencyRecord {
                    version: 1,
                    fingerprint: fingerprint_in.clone(),
                    outcome: "committed".into(),
                    response: Some(RecordedResponse {
                        status: StatusCode::OK.as_u16(),
                        headers: BTreeMap::from([
                            ("content-type".into(), "application/json".into()),
                            ("etag".into(), manifest.etag.clone()),
                        ]),
                        body_base64: BASE64.encode(response_body),
                    }),
                };
                let encoded = match serde_json::to_vec(&record) {
                    Ok(encoded) => encoded,
                    Err(e) => {
                        *abort_in.lock().unwrap() =
                            Some(api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
                        return Err(S3Error::ConcurrentModification);
                    }
                };
                let (record_op, usage_op) =
                    match admission_writes(&usage, &usage_key_in, &record_key_in, &encoded) {
                        Ok(ops) => ops,
                        Err(response) => {
                            *abort_in.lock().unwrap() = Some(response);
                            return Err(S3Error::ConcurrentModification);
                        }
                    };
                *record_out_in.lock().unwrap() = Some(record);
                Ok(yadb_s3::ObjectPublication {
                    guards: vec![
                        yadb_metakv::RevisionGuard {
                            key: record_key_in.clone(),
                            expected_mod_revision: 0,
                        },
                        yadb_metakv::RevisionGuard {
                            key: usage_key_in.clone(),
                            expected_mod_revision: usage_revision,
                        },
                    ],
                    ops: vec![record_op, usage_op],
                })
            },
        )
    })
    .await;
    match res {
        Ok(Ok(_manifest)) => match record_out.lock().unwrap().take() {
            Some(record) => replay_record(record, &fingerprint),
            None => api_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "idempotency record was not prepared",
            ),
        },
        Ok(Err(S3Error::ConcurrentModification)) => {
            if let Some(response) = abort.lock().unwrap().take() {
                return response;
            }
            // A definite non-commit: re-read the record to tell "a concurrent
            // attempt committed this key" from "the object moved under us".
            let meta = st.meta.clone();
            let record_key_in = record_key.clone();
            let record_key_read = record_key.clone();
            match tokio::task::spawn_blocking(move || {
                meta.try_linearizable_snapshot(&[record_key_in], &[])
                    .map_err(|e| e.to_string())
            })
            .await
            {
                Ok(Ok(snapshot)) => match snapshot.get(&record_key_read) {
                    Some(kv) => match serde_json::from_slice::<IdempotencyRecord>(&kv.value) {
                        Ok(record) => replay_record(record, &fingerprint),
                        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                    },
                    None => api_err(
                        StatusCode::CONFLICT,
                        "object changed; re-read the object and retry",
                    ),
                },
                _ => api_err(StatusCode::SERVICE_UNAVAILABLE, "quorum unavailable"),
            }
        }
        Ok(Err(S3Error::Meta(me))) => meta_err_resp(st, me),
        Ok(Err(S3Error::Blob(BlobError::RedundancyNotMet { .. }))) => api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage pool cannot reach full redundancy; the write was refused",
        ),
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Keyed S3 DELETE: the manifest deletion, its retirement tombstone and the
/// idempotency record commit in one guarded `RevisionTxn`
/// (`ObjectStoreEngine::delete_object_with`).
///
/// The recorded body is the same `{"deleted": …}` JSON the success path
/// returns, derived from the existence read that also guards the deletion.
async fn keyed_s3_delete(
    st: &AppState,
    headers: &HeaderMap,
    project: &str,
    store: &str,
    bucket: &str,
    key: &str,
) -> Response {
    let key_id = match idempotency_header(headers) {
        Ok(Some(key)) => key,
        Ok(None) => unreachable!("the keyed path is chosen from this header"),
        Err(e) => return e,
    };
    let Some(subject) = verified_subject(headers, st.subject_secret.as_deref()) else {
        return ControlAdminError::Unverified.into_response();
    };
    if let Some(resp) = maybe_forward_write(
        st,
        headers,
        Some(project),
        "DELETE",
        format!(
            "/s3/{}/{}/{}/{}",
            protocols::url(project),
            protocols::url(store),
            protocols::url(bucket),
            key.split('/')
                .map(protocols::url)
                .collect::<Vec<_>>()
                .join("/")
        ),
        "application/octet-stream".to_string(),
        Bytes::new(),
    )
    .await
    {
        return resp;
    }
    let (pid, _) = {
        let cp = match st.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(e) => return e.into_response(),
        };
        match resolve_s3_target(&cp, project, store, bucket, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
    let scope = match project_idempotency_scope(st, pid).await {
        Ok(scope) => scope,
        Err(resp) => return resp,
    };
    let record_key = project_record_key(&scope.records, &subject, &key_id);
    let fingerprint = s3_object_fingerprint("DELETE", "s3-delete", pid, store, bucket, key, &[]);
    let pre = {
        let meta = st.meta.clone();
        let record_key = record_key.clone();
        let usage_key = scope.usage_key.clone();
        match tokio::task::spawn_blocking(move || {
            read_project_apply_snapshot(&meta, &record_key, &usage_key)
        })
        .await
        {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(e)) => return api_err(StatusCode::SERVICE_UNAVAILABLE, e),
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    };
    if let Some(record) = pre.record {
        return replay_record(record, &fingerprint);
    }
    if pre.usage.records >= MAX_IDEMPOTENCY_RECORDS {
        return idempotency_quota_response();
    }
    let abort: std::sync::Arc<std::sync::Mutex<Option<Response>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let record_out: std::sync::Arc<std::sync::Mutex<Option<IdempotencyRecord>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let abort_in = abort.clone();
    let record_out_in = record_out.clone();
    let meta = st.meta.clone();
    let record_key_in = record_key.clone();
    let usage_key_in = scope.usage_key.clone();
    let fingerprint_in = fingerprint.clone();
    let s3 = st.s3.clone();
    let store_in = store.to_owned();
    let bucket_in = bucket.to_owned();
    let key_in = key.to_owned();
    let res = tokio::task::spawn_blocking(move || {
        s3.delete_object_with(pid, &store_in, &bucket_in, &key_in, |existed: bool| {
            let snapshot = match meta
                .try_linearizable_snapshot(&[record_key_in.clone(), usage_key_in.clone()], &[])
            {
                Ok(snapshot) => snapshot,
                Err(e) => {
                    *abort_in.lock().unwrap() =
                        Some(api_err(StatusCode::SERVICE_UNAVAILABLE, e.to_string()));
                    return Err(S3Error::ConcurrentModification);
                }
            };
            if let Some(kv) = snapshot.get(&record_key_in) {
                let response = match serde_json::from_slice::<IdempotencyRecord>(&kv.value) {
                    Ok(record) => replay_record(record, &fingerprint_in),
                    Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                };
                *abort_in.lock().unwrap() = Some(response);
                return Err(S3Error::ConcurrentModification);
            }
            let (usage, usage_revision) = match snapshot.get(&usage_key_in) {
                Some(kv) => match decode_usage(&kv.value) {
                    Ok(usage) => (usage, kv.mod_revision),
                    Err(e) => {
                        *abort_in.lock().unwrap() =
                            Some(api_err(StatusCode::INTERNAL_SERVER_ERROR, e));
                        return Err(S3Error::ConcurrentModification);
                    }
                },
                None => (
                    IdempotencyUsage {
                        version: 1,
                        ..IdempotencyUsage::default()
                    },
                    0,
                ),
            };
            let response_body = match serde_json::to_vec(&serde_json::json!({
                "deleted": existed,
            })) {
                Ok(body) => body,
                Err(e) => {
                    *abort_in.lock().unwrap() =
                        Some(api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
                    return Err(S3Error::ConcurrentModification);
                }
            };
            let record = IdempotencyRecord {
                version: 1,
                fingerprint: fingerprint_in.clone(),
                outcome: "committed".into(),
                response: Some(RecordedResponse {
                    status: StatusCode::OK.as_u16(),
                    headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                    body_base64: BASE64.encode(response_body),
                }),
            };
            let encoded = match serde_json::to_vec(&record) {
                Ok(encoded) => encoded,
                Err(e) => {
                    *abort_in.lock().unwrap() =
                        Some(api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
                    return Err(S3Error::ConcurrentModification);
                }
            };
            let (record_op, usage_op) =
                match admission_writes(&usage, &usage_key_in, &record_key_in, &encoded) {
                    Ok(ops) => ops,
                    Err(response) => {
                        *abort_in.lock().unwrap() = Some(response);
                        return Err(S3Error::ConcurrentModification);
                    }
                };
            *record_out_in.lock().unwrap() = Some(record);
            Ok(yadb_s3::ObjectPublication {
                guards: vec![
                    yadb_metakv::RevisionGuard {
                        key: record_key_in.clone(),
                        expected_mod_revision: 0,
                    },
                    yadb_metakv::RevisionGuard {
                        key: usage_key_in.clone(),
                        expected_mod_revision: usage_revision,
                    },
                ],
                ops: vec![record_op, usage_op],
            })
        })
    })
    .await;
    match res {
        Ok(Ok(_existed)) => match record_out.lock().unwrap().take() {
            Some(record) => replay_record(record, &fingerprint),
            None => api_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "idempotency record was not prepared",
            ),
        },
        Ok(Err(S3Error::ConcurrentModification)) => {
            if let Some(response) = abort.lock().unwrap().take() {
                return response;
            }
            let meta = st.meta.clone();
            let record_key_in = record_key.clone();
            let record_key_read = record_key.clone();
            match tokio::task::spawn_blocking(move || {
                meta.try_linearizable_snapshot(&[record_key_in], &[])
                    .map_err(|e| e.to_string())
            })
            .await
            {
                Ok(Ok(snapshot)) => match snapshot.get(&record_key_read) {
                    Some(kv) => match serde_json::from_slice::<IdempotencyRecord>(&kv.value) {
                        Ok(record) => replay_record(record, &fingerprint),
                        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                    },
                    None => api_err(
                        StatusCode::CONFLICT,
                        "object changed; re-read the object and retry",
                    ),
                },
                _ => api_err(StatusCode::SERVICE_UNAVAILABLE, "quorum unavailable"),
            }
        }
        Ok(Err(S3Error::Meta(me))) => meta_err_resp(st, me),
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

// ============================================================================
// Distributed VDisk RPC & Self-Healing Pool Scrubber
// ============================================================================

// VDisk operations are daemon-to-daemon traffic, not admin-subject requests;
// checking the same peer credential as Raft prevents remote tombstones and fences.
async fn handle_vdisk_put(
    State(st): State<AppState>,
    Path((pool, slot, blob_hex)): Path<(String, usize, String)>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(blob_id) = BlobId::from_hex(&blob_hex) else {
        return api_err(StatusCode::BAD_REQUEST, "invalid 32-char hex BlobId");
    };
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(
            StatusCode::NOT_FOUND,
            "local VDisk slot not hosted on this node",
        );
    };

    let res = tokio::task::spawn_blocking(move || store.put(blob_id, &body)).await;
    match res {
        Ok(Ok(())) => StatusCode::OK.into_response(),
        Ok(Err(BlobError::ImmutableConflict(_))) => {
            api_err(StatusCode::CONFLICT, "immutable blob conflict")
        }
        Ok(Err(BlobError::FencedByBarrier { .. })) => {
            api_err(StatusCode::PRECONDITION_FAILED, "fenced by GC barrier")
        }
        Ok(Err(BlobError::Tombstoned(_))) => {
            api_err(StatusCode::PRECONDITION_FAILED, "immutable blob tombstoned")
        }
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_vdisk_get(
    State(st): State<AppState>,
    Path((pool, slot, blob_hex)): Path<(String, usize, String)>,
    headers: axum::http::HeaderMap,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(blob_id) = BlobId::from_hex(&blob_hex) else {
        return api_err(StatusCode::BAD_REQUEST, "invalid 32-char hex BlobId");
    };
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(
            StatusCode::NOT_FOUND,
            "local VDisk slot not hosted on this node",
        );
    };

    let res = tokio::task::spawn_blocking(move || store.get(blob_id)).await;
    match res {
        Ok(Ok(bytes)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            Bytes::from(bytes),
        )
            .into_response(),
        Ok(Err(BlobError::NotFound(_))) => api_err(StatusCode::NOT_FOUND, "fragment not found"),
        Ok(Err(BlobError::CorruptData { reason, .. })) => {
            api_err(StatusCode::UNPROCESSABLE_ENTITY, reason)
        }
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_vdisk_stats(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    headers: axum::http::HeaderMap,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(
            StatusCode::NOT_FOUND,
            "local VDisk slot not hosted on this node",
        );
    };
    match store.stats() {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

// Storage-fence RPC. These endpoints expose the durable epoch and pin table of
// one VDisk slot. They are transport only: the collector that drives them is a
// later change, so nothing here takes a pin or advances an epoch on its own.
#[derive(Deserialize)]
struct VdiskPinBody {
    epoch: u64,
    ttl_ms: u64,
}

#[derive(Deserialize)]
struct VdiskUnpinBody {
    pin_id: u64,
}

#[derive(Deserialize)]
struct VdiskAdvanceBody {
    expected_epoch: u64,
}

#[derive(Deserialize, Default)]
struct VdiskDeleteQuery {
    #[serde(default)]
    fence_epoch: Option<u64>,
    #[serde(default)]
    collector_epoch: Option<u64>,
}

#[derive(Clone, Copy)]
enum VdiskDeleteFence {
    Exact(u64),
    Collectable(u64),
}

/// Error envelope of the fence endpoints: a stable machine code plus a human
/// message. Kept separate from `api_err`, whose body is a single `error` string
/// shared with the legacy endpoints.
fn fence_api_err(status: StatusCode, code: &'static str, message: impl Into<String>) -> Response {
    (
        status,
        Json(serde_json::json!({
            "error": code,
            "message": message.into(),
        })),
    )
        .into_response()
}

fn fence_slot_not_found() -> Response {
    fence_api_err(
        StatusCode::NOT_FOUND,
        "slot_not_found",
        "local VDisk slot not hosted on this node",
    )
}

/// The status/code each [`BlobError`] means on a fence endpoint. A refusal is a
/// durable-state answer the caller stops on, so every pin/fence variant has a
/// stable `409` (or `503` for an unreadable clock/storage) rather than becoming
/// a generic server error.
fn fence_blob_error(error: BlobError) -> Response {
    let (status, code) = match &error {
        BlobError::PinTtlZero => (StatusCode::BAD_REQUEST, "pin_ttl_zero"),
        BlobError::PinTtlTooLong { .. } => (StatusCode::BAD_REQUEST, "pin_ttl_too_long"),
        BlobError::EpochMismatch { .. } => (StatusCode::CONFLICT, "epoch_mismatch"),
        BlobError::EpochPinned { .. } => (StatusCode::CONFLICT, "epoch_pinned"),
        BlobError::EpochExhausted => (StatusCode::CONFLICT, "epoch_exhausted"),
        BlobError::PinEpochNotCurrent { .. } => (StatusCode::CONFLICT, "pin_epoch_not_current"),
        BlobError::PinIdsExhausted => (StatusCode::CONFLICT, "pin_ids_exhausted"),
        BlobError::FencedByEpoch { .. } => (StatusCode::CONFLICT, "fenced_by_epoch"),
        BlobError::FragmentNotOlder { .. } => (StatusCode::CONFLICT, "fragment_not_older"),
        BlobError::FragmentPinned { .. } => (StatusCode::CONFLICT, "fragment_pinned"),
        BlobError::PinClockUnreadable { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "pin_clock_unreadable")
        }
        BlobError::StorageUnavailable => (StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };
    fence_api_err(status, code, error.to_string())
}

async fn handle_vdisk_epoch(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    headers: axum::http::HeaderMap,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return fence_slot_not_found();
    };
    match tokio::task::spawn_blocking(move || store.epoch()).await {
        Ok(epoch) => Json(serde_json::json!({ "epoch": epoch })).into_response(),
        Err(e) => fence_api_err(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
    }
}

async fn handle_vdisk_pins(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    headers: axum::http::HeaderMap,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return fence_slot_not_found();
    };
    match tokio::task::spawn_blocking(move || store.live_pins()).await {
        Ok(Ok(pins)) => {
            let pins: Vec<_> = pins
                .into_iter()
                .map(|pin| {
                    serde_json::json!({
                        "pin_id": pin.pin_id.get(),
                        "epoch": pin.epoch,
                        "expires_at_ms": pin.expires_at_ms,
                    })
                })
                .collect();
            Json(serde_json::json!({ "pins": pins })).into_response()
        }
        Ok(Err(error)) => fence_blob_error(error),
        Err(e) => fence_api_err(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
    }
}

async fn handle_vdisk_pin(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    headers: axum::http::HeaderMap,
    Json(body): Json<VdiskPinBody>,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return fence_slot_not_found();
    };
    match tokio::task::spawn_blocking(move || store.pin(body.epoch, body.ttl_ms)).await {
        Ok(Ok(grant)) => Json(serde_json::json!({
            "pin_id": grant.pin_id.get(),
            "epoch": grant.epoch,
            "expires_at_ms": grant.expires_at_ms,
        }))
        .into_response(),
        Ok(Err(error)) => fence_blob_error(error),
        Err(e) => fence_api_err(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
    }
}

async fn handle_vdisk_unpin(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    headers: axum::http::HeaderMap,
    Json(body): Json<VdiskUnpinBody>,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return fence_slot_not_found();
    };
    let pin_id = PinId::from_wire(body.pin_id);
    match tokio::task::spawn_blocking(move || store.unpin(pin_id)).await {
        Ok(Ok(released)) => Json(serde_json::json!({ "released": released })).into_response(),
        Ok(Err(error)) => fence_blob_error(error),
        Err(e) => fence_api_err(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
    }
}

async fn handle_vdisk_advance(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    headers: axum::http::HeaderMap,
    Json(body): Json<VdiskAdvanceBody>,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return fence_slot_not_found();
    };
    match tokio::task::spawn_blocking(move || store.advance_epoch(body.expected_epoch)).await {
        Ok(Ok(epoch)) => Json(serde_json::json!({ "epoch": epoch })).into_response(),
        Ok(Err(error)) => fence_blob_error(error),
        Err(e) => fence_api_err(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
    }
}

async fn handle_vdisk_delete(
    State(st): State<AppState>,
    Path((pool, slot, blob_hex)): Path<(String, usize, String)>,
    Query(query): Query<VdiskDeleteQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let fence = match (query.fence_epoch, query.collector_epoch) {
        (Some(_), Some(_)) => {
            return fence_api_err(
                StatusCode::BAD_REQUEST,
                "bad_fence_params",
                "fence_epoch and collector_epoch are mutually exclusive",
            );
        }
        (Some(expected), None) => Some(VdiskDeleteFence::Exact(expected)),
        (None, Some(collector)) => Some(VdiskDeleteFence::Collectable(collector)),
        (None, None) => None,
    };
    let Some(blob_id) = BlobId::from_hex(&blob_hex) else {
        return match fence {
            Some(_) => fence_api_err(
                StatusCode::BAD_REQUEST,
                "bad_blob_id",
                "invalid 32-char hex BlobId",
            ),
            None => api_err(StatusCode::BAD_REQUEST, "invalid 32-char hex BlobId"),
        };
    };
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return match fence {
            Some(_) => fence_slot_not_found(),
            None => api_err(
                StatusCode::NOT_FOUND,
                "local VDisk slot not hosted on this node",
            ),
        };
    };
    let fenced = fence.is_some();
    let res = match fence {
        None => tokio::task::spawn_blocking(move || store.delete(blob_id)).await,
        Some(VdiskDeleteFence::Exact(expected_epoch)) => {
            tokio::task::spawn_blocking(move || store.delete_fenced(blob_id, expected_epoch)).await
        }
        Some(VdiskDeleteFence::Collectable(collector_epoch)) => {
            tokio::task::spawn_blocking(move || store.delete_collectable(blob_id, collector_epoch))
                .await
        }
    };
    match res {
        Ok(Ok(true)) => (StatusCode::OK, "deleted").into_response(),
        Ok(Ok(false)) => (StatusCode::OK, "absent").into_response(),
        Ok(Err(error)) if fenced => fence_blob_error(error),
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) if fenced => {
            fence_api_err(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string())
        }
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_vdisk_barrier(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    headers: axum::http::HeaderMap,
    Json(req): Json<BarrierRequest>,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(
            StatusCode::NOT_FOUND,
            "local VDisk slot not hosted on this node",
        );
    };
    let res = tokio::task::spawn_blocking(move || {
        let evicted = store.collect_garbage_barrier(
            req.project_id,
            req.owner_id,
            req.channel,
            req.up_to_generation,
            req.lease_expires_at_ms,
        )?;
        let mut response = BarrierResponse {
            evicted,
            ..BarrierResponse::default()
        };
        if req.compact {
            match store.seal_and_compact() {
                Ok(bytes) => response.reclaimed_bytes = bytes,
                Err(error) => response.compaction_error = Some(error.to_string()),
            }
        }
        Ok::<BarrierResponse, BlobError>(response)
    })
    .await;
    match res {
        Ok(Ok(response)) => Json(response).into_response(),
        // The collector's lease lapsed before this target applied the barrier, so
        // the range was left intact. `409` rather than `500`: the request was
        // well formed and refused on purpose, and the caller must stop deleting
        // instead of retrying. An unreadable target clock is the same class of
        // refusal for the same reason: the target cannot prove the lease is live,
        // so it declines the eviction and the collector must stop.
        Ok(Err(error @ BlobError::BarrierLeaseExpired { .. }))
        | Ok(Err(error @ BlobError::BarrierClockUnreadable { .. })) => {
            api_err(StatusCode::CONFLICT, error.to_string())
        }
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_vdisk_blobs(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    headers: axum::http::HeaderMap,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(
            StatusCode::NOT_FOUND,
            "local VDisk slot not hosted on this node",
        );
    };
    let ids = match tokio::task::spawn_blocking(move || {
        store
            .live_ids()
            .into_iter()
            .map(|id| id.to_hex())
            .collect::<Vec<_>>()
    })
    .await
    {
        Ok(ids) => ids,
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    Json(yadb_blob::BlobIdList { ids }).into_response()
}

async fn handle_vdisk_compact(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    headers: axum::http::HeaderMap,
) -> Response {
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(
            StatusCode::NOT_FOUND,
            "local VDisk slot not hosted on this node",
        );
    };
    let res = tokio::task::spawn_blocking(move || store.seal_and_compact()).await;
    match res {
        Ok(Ok(reclaimed_bytes)) => {
            Json(yadb_blob::CompactResponse { reclaimed_bytes }).into_response()
        }
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_pools_health(State(st): State<AppState>) -> Response {
    let st_clone = st.clone();
    let res = tokio::task::spawn_blocking(move || st_clone.collect_pool_health(true)).await;
    match res {
        Ok(pools) => {
            let last_scrub = st.last_scrub.read().unwrap().clone();
            Json(serde_json::json!({
                "node_id": st.meta.node_id(),
                "pools": pools,
                "last_scrub": last_scrub,
            }))
            .into_response()
        }
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_pools_scrub(State(st): State<AppState>, headers: HeaderMap) -> Response {
    // Scrub is not a read: on single-node metadata it collects orphan blobs,
    // rewrites fragments and records the summary. Any client that can reach the
    // port must not be able to start it, so the check mirrors `/v1/apply`
    // exactly: a signed subject on the `--admin-subject` allowlist. Startup
    // refuses an empty allowlist (`main`), so every daemon that reaches this
    // handler has one configured; `require_admin`'s empty-allowlist branch is
    // kept for library callers and tests, not as a reachable daemon mode.
    if let Err(e) = require_admin(
        &headers,
        st.subject_secret.as_deref(),
        &st.admin_subjects,
        st.meta.is_openraft(),
    ) {
        return e.into_response();
    }
    let st_clone = st.clone();
    let res = tokio::task::spawn_blocking(move || st_clone.scrub_all_pools(true)).await;
    match res {
        Ok(summary) => {
            // Same rule as `/v1/pools/gc`: a refusal is an error, never a 200
            // with zero counts. On Multi-Raft that means "every metadata group's
            // live set was proved and scanned, and collection did not refuse".
            // A node that leads only some groups answers 409 and names the
            // groups it does not lead, while the groups it does lead are really
            // repaired - the counters in the body say which.
            let status = if summary.repair_error.is_some() || summary.gc_refusal.is_some() {
                StatusCode::CONFLICT
            } else {
                StatusCode::OK
            };
            (status, Json(summary)).into_response()
        }
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_pools_gc(State(st): State<AppState>, headers: HeaderMap) -> Response {
    // Same mutation class as scrub: it deletes fragments and rewrites durable
    // barrier records, so it needs the same admin gate.
    if let Err(e) = require_admin(
        &headers,
        st.subject_secret.as_deref(),
        &st.admin_subjects,
        st.meta.is_openraft(),
    ) {
        return e.into_response();
    }
    let st_clone = st.clone();
    let res = tokio::task::spawn_blocking(move || st_clone.gc_all_pools()).await;
    match res {
        Ok(outcome) => {
            // A refusal is an error, never a 200 with zero counts: the caller
            // must not read "nothing to collect" into "could not prove safety".
            let status = if outcome.refusal.is_some() {
                StatusCode::CONFLICT
            } else {
                StatusCode::OK
            };
            (status, Json(outcome)).into_response()
        }
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    // OpenRaft reports replication failures (`RPCError`, `StorageError`,
    // election and commit events) through `tracing`. With no subscriber those
    // events are dropped, which is why a stalled commit previously left no
    // daemon-side trace: the only visible symptom was a client-side 503. The
    // subscriber is installed only when `RUST_LOG` is set, so a normal start is
    // byte-for-byte the same and pays nothing for a filter that would reject
    // every event.
    if std::env::var_os("RUST_LOG").is_some() {
        use tracing_subscriber::EnvFilter;
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .try_init();
    }
    // `--metadata-runtime` is parsed and pinned to `openraft`; read it so the
    // value is not silently ignored.
    debug_assert_eq!(args.metadata_runtime, "openraft");
    // Every requirement below is fail-closed: there is no default config, no
    // fallback runtime, no auto-bootstrap and no retry.
    if args.openraft_config.is_none() {
        return Err("--openraft-config is required: OpenRaft is the metadata runtime".into());
    }
    if args.data_dir.is_none() {
        return Err("--data-dir is required: OpenRaft needs an explicit new root".into());
    }
    if args.raft_token.as_deref().is_none_or(str::is_empty) {
        return Err(
            "--raft-token (or YADB_RAFT_TOKEN) is required and must be non-empty; the internal \
             endpoints are unauthenticated without it"
                .into(),
        );
    }
    // A control-plane mutation needs a proven subject and an allowlist. Fail
    // closed at startup instead of refusing every `/v1/apply` at runtime.
    if args.admin_subject.is_empty() {
        return Err(
            "--admin-subject is required: an empty allowlist would refuse every control-plane \
             mutation"
                .into(),
        );
    }
    if args.admin_subject.iter().any(|s| s.trim().is_empty()) {
        return Err("--admin-subject must not contain empty entries".into());
    }
    if args.subject_secret.as_deref().is_none_or(str::is_empty) {
        return Err(
            "--admin-subject requires a non-empty --subject-secret (or YADB_SUBJECT_SECRET) to \
             verify signed subjects"
                .into(),
        );
    }
    let data_dir = args.data_dir.clone().unwrap_or_else(|| {
        PathBuf::from("/data/data/com.foxdebug.acodefree/cache/opencode/yadbd-data")
    });
    let prepared_multi = openraft::prepare_multi(args.openraft_config.as_deref().unwrap())?;
    let enabled =
        args.pg_listen.is_some() || args.iscsi_listen.is_some() || args.s3_listen.is_some();
    let credentials = if enabled {
        protocols::Credentials::load(
            args.protocol_credentials
                .as_deref()
                .ok_or("--protocol-credentials is required for protocol listeners")?,
        )?
    } else {
        protocols::Credentials::default()
    };
    let public_peers = parse_peer_map(
        args.peers
            .as_deref()
            .ok_or("--peers public HTTP map required for OpenRaft")?,
    )
    .map_err(std::io::Error::other)?;
    prepared_multi.validate_client_root(&data_dir, args.node_id, &public_peers, &args.listen)?;
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    let _client_root_owner = prepared_multi.provision_client_root(&data_dir, &public_peers)?;
    let mut raft_owner = openraft::Owner::Multi(prepared_multi.start().await?);
    let result: Result<(), Box<dyn std::error::Error>> = async {
    let fs = OsFs::new(&data_dir)?;

    let meta = raft_owner.metadata(public_peers.clone())?;

    // Subscribe first so an apply concurrent with the initial read is queued.
    let watch_rx = meta.watch_prefix(CONTROL_STATE_KEY);
    // Recover persisted declarative ControlPlaneState from the selected backend.
    let recovered_cp: ControlPlaneState = match meta.try_get(CONTROL_STATE_KEY)? {
        Some(kv) => serde_json::from_slice(&kv.value)?,
        None => ControlPlaneState::new(),
    };

    let s3 = ObjectStoreEngine::new(meta.clone(), args.chunk_size);
    // Unset in production: the engine keeps its shipped 300 s default. The
    // protocol harness shortens it to observe orphan collection inside a run.
    if let Some(grace) = args.s3_intent_grace_secs {
        s3.set_intent_grace_secs(grace);
    }
    let db = DatabaseEngine::new(meta.clone(), args.db_flush_rows);
    let block = BlockVolumeEngine::new(meta.clone(), DEFAULT_BLOCK_SIZE);

    let state = AppState {
        openraft: Some(raft_owner.status()),
        listen_addr: args.listen.clone(),
        fs,
        meta: meta.clone(),
        raft_token: args.raft_token.clone(),
        subject_secret: args.subject_secret.clone(),
        admin_subjects: args.admin_subject.clone(),
        control: Arc::new(RwLock::new(recovered_cp.clone())),
        control_refresh: Arc::new(std::sync::Mutex::new(())),
        control_freshness: Arc::new(std::sync::Mutex::new(ControlFreshness::default())),
        pools: Arc::new(RwLock::new(BTreeMap::new())),
        local_vdisks: Arc::new(RwLock::new(BTreeMap::new())),
        last_scrub: Arc::new(RwLock::new(None)),
        reader_lease: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        s3,
        db,
        block,
    };
    state
        .materialize_pools(&recovered_cp)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // Watch `/sys/control_plane_state` so Raft followers automatically materialize pools
    // and update their zero-hop RBAC cache as soon as the leader commits an `apply`!
    let watch_state = state.clone();
    let (watch_stop, stop_rx) = std::sync::mpsc::channel::<()>();
    let watch_thread = std::thread::spawn(move || {
        while matches!(stop_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)) {
            let ev = match watch_rx.recv_timeout(CONTROL_REFRESH_INTERVAL) {
                Ok(ev) => ev,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => { if watch_state.meta.is_openraft() { watch_state.refresh_openraft_control(); } continue; },
                Err(_) => { if watch_state.meta.is_openraft() { let _guard = watch_state.control_refresh.lock().unwrap(); watch_state.control_freshness.lock().unwrap().invalidate(); *watch_state.control.write().unwrap() = ControlPlaneState::new(); watch_state.pools.write().unwrap().clear(); } break; },
            };
            // Re-read the latest applied value, rather than replaying older
            // queued values over the initial snapshot.
            if watch_state.meta.is_openraft() {
                watch_state.refresh_openraft_control();
            } else if let WatchEvent::Put { value, .. } = ev {
                if let Ok(cp) = serde_json::from_slice::<ControlPlaneState>(&value) {
                    let _ = watch_state.materialize_pools(&cp);
                    *watch_state.control.write().unwrap() = cp;
                }
            }
        }
    });

    // Self-Healing VDisk scrubber. OpenRaft owns elections and heartbeats; a
    // node only sweeps while its control group believes it is the leader.
    if args.peers.is_some() {
        let scrub_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(4));
            // The repair scan is cheap and idempotent, so it runs every few
            // seconds. Collection holds the exclusive collector lease for the
            // whole pass, so it runs on a much slower cadence: otherwise an
            // explicit `POST /v1/pools/gc` finds the lease held almost always.
            let mut last_sweep = std::time::Instant::now();
            loop {
                interval.tick().await;
                let (_, role, _, _, _) = scrub_state.meta.role_info();
                if role == RoleState::Leader {
                    let due = last_sweep.elapsed() >= BACKGROUND_SWEEP_INTERVAL;
                    if due {
                        last_sweep = std::time::Instant::now();
                    }
                    let st = scrub_state.clone();
                    match tokio::task::spawn_blocking(move || st.scrub_all_pools(due)).await {
                        Ok(summary) if due => {
                            if let Some(reason) = summary.gc_refusal {
                                eprintln!("background garbage collection: {reason}");
                            }
                        }
                        Err(error) => eprintln!("background scrub failed: {error}"),
                        _ => {}
                    }
                }
            }
        });
    }

    if let Some(addr) = &args.pg_listen {
        let listener = std::net::TcpListener::bind(addr)?;
        let tls = match (&args.pg_tls_cert, &args.pg_tls_key) {
            (Some(cert), Some(key)) => Some(std::sync::Arc::new(protocols::pg::Tls::load(
                cert,
                key,
                args.pg_tls_client_ca.as_deref(),
                args.pg_require_tls,
            )?)),
            (None, None) => None,
            _ => return Err("--pg-tls-cert and --pg-tls-key must be given together".into()),
        };
        let st = state.clone(); let creds = credentials.clone();
        let handler_tls = tls.clone();
        protocols::serve(listener, move |c| {
            protocols::pg::connection(c, st.clone(), creds.clone(), handler_tls.clone())
        });
        match tls {
            Some(_) => println!(
                "pgwire listening on {addr} ({}; COPY text, extended query, cleartext{})",
                if args.pg_tls_client_ca.is_some() { "TLS with client certificates" } else { "TLS" },
                if args.pg_require_tls { " refused" } else { " allowed" }
            ),
            None => println!("pgwire listening on {addr} (no TLS; trusted networks only)"),
        }
    }
    if let Some(addr) = &args.iscsi_listen {
        let listener = std::net::TcpListener::bind(addr)?;
        let portals = if args.iscsi_portals.is_empty() {
            let a: SocketAddr = addr.parse()?;
            if a.ip().is_unspecified() { return Err("set --iscsi-portals when listening on a wildcard address".into()); }
            vec![addr.clone()]
        } else { args.iscsi_portals.clone() };
        for p in &portals { let _: SocketAddr = p.parse()?; }
        let st = state.clone(); let creds = credentials.clone();
        protocols::serve(listener, move |c| protocols::iscsi::connection(c,st.clone(),creds.clone(),portals.clone()));
        println!("iSCSI listening on {addr} (CHAP, ERL=0; multipath managed by initiator)");
    }
    if let Some(addr) = &args.s3_listen {
        let (project,store) = args.s3_target.as_deref().and_then(|s|s.split_once('/')).ok_or("--s3-target must be project/store")?;
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let s3state = protocols::s3::S3State { app: state.clone(), credentials: credentials.clone(), project:project.into(),store:store.into() };
        let router = Router::new().fallback(s3_wire_dispatch).with_state(s3state);
        tokio::spawn(async move { if let Err(e)=axum::serve(listener,router).await {eprintln!("S3 listener stopped: {e}");} });
        println!("AWS S3 path-style listening on {addr}");
    }

    let app = Router::new()
        .route("/", get(handle_ui))
        .route("/ui", get(handle_ui))
        .route("/healthz", get(handle_health))
        .route("/raft/status", get(handle_raft_status))
        .route("/v1/sample-yaml", get(handle_sample_yaml))
        .route("/v1/status", get(handle_status))
        .route("/v1/plan", post(handle_plan))
        .route("/v1/apply", post(apply_dispatch))
        .route("/v1/idempotency/resolve", post(handle_resolve))
        .route("/v1/pools/health", get(handle_pools_health))
        .route("/v1/pools/scrub", post(handle_pools_scrub))
        .route("/v1/pools/gc", post(handle_pools_gc))
        .route(
            "/internal/vdisk/{pool}/{slot}/blob/{blob_hex}",
            put(handle_vdisk_put).get(handle_vdisk_get).delete(handle_vdisk_delete),
        )
        .route(
            "/internal/vdisk/{pool}/{slot}/stats",
            get(handle_vdisk_stats),
        )
        .route(
            "/internal/vdisk/{pool}/{slot}/barrier",
            post(handle_vdisk_barrier),
        )
        .route(
            "/internal/vdisk/{pool}/{slot}/blobs",
            get(handle_vdisk_blobs),
        )
        .route(
            "/internal/vdisk/{pool}/{slot}/compact",
            post(handle_vdisk_compact),
        )
        .route(
            "/internal/vdisk/{pool}/{slot}/epoch",
            get(handle_vdisk_epoch),
        )
        .route(
            "/internal/vdisk/{pool}/{slot}/pins",
            get(handle_vdisk_pins),
        )
        .route(
            "/internal/vdisk/{pool}/{slot}/pin",
            post(handle_vdisk_pin),
        )
        .route(
            "/internal/vdisk/{pool}/{slot}/unpin",
            post(handle_vdisk_unpin),
        )
        .route(
            "/internal/vdisk/{pool}/{slot}/advance",
            post(handle_vdisk_advance),
        )
        .route("/db/{project}/{database}/query", post(handle_db_query))
        .route("/block/{project}/{volume}/unmap", post(handle_block_unmap))
        .route(
            "/block/{project}/{volume}",
            put(handle_block_write).get(handle_block_read),
        )
        .route("/s3/{project}/{store}/{bucket}", get(handle_s3_list))
        .route(
            "/s3/{project}/{store}/{bucket}/{*key}",
            put(handle_s3_put)
                .get(handle_s3_get)
                .delete(handle_s3_delete)
                .post(handle_s3_multipart),
        )
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024 * 1024))
        ;
    let app = app
        .route(
            "/openraft/status",
            get(handle_openraft_status),
        )
        .route(
            "/internal/control/snapshot",
            get(handle_internal_control_snapshot),
        );
    let app = app.with_state(state);

    println!(
        "yadbd node {} listening on http://{} (data={:?}, generation={})",
        args.node_id, args.listen, data_dir, recovered_cp.generation
    );
    {
        let result = tokio::select! {
            result = axum::serve(listener, app) => result,
            result = tokio::signal::ctrl_c() => result,
            result = raft_owner.listener_finished() => Err(std::io::Error::other(format!("OpenRaft listener stopped: {result}"))),
        };
        let _ = watch_stop.send(());
        let _ = watch_thread.join();
        result?;
    }
    Ok(())
    }.await;
    raft_owner.shutdown().await?;
    result
}

#[cfg(test)]
mod security_tests {
    use super::*;

    #[test]
    fn idempotency_validation_and_fingerprint() {
        for key in ["", "a b", "a+b", &"a".repeat(129)] {
            assert!(!valid_idempotency_key(key.as_bytes()));
        }
        assert!(valid_idempotency_key(b"aZ09._~-"));
        let mut headers = HeaderMap::new();
        headers.append("idempotency-key", HeaderValue::from_static("a"));
        headers.append("idempotency-key", HeaderValue::from_static("a"));
        assert!(idempotency_header(&headers).is_err());
        assert_ne!(
            request_fingerprint(b"{\"x\":1}"),
            request_fingerprint(b"{ \"x\":1}")
        );
        assert_ne!(record_key("a", "b"), record_key("b", "a"));
    }

    #[test]
    fn forwarded_paths_encode_each_decoded_segment() {
        let project = "project ?#雪";
        let target = "name ?#é/%";
        assert_eq!(
            db_query_path(project, target),
            "/db/project%20%3F%23%E9%9B%AA/name%20%3F%23%C3%A9%2F%25/query"
        );
        let block = block_path(project, target);
        assert_eq!(
            block,
            "/block/project%20%3F%23%E9%9B%AA/name%20%3F%23%C3%A9%2F%25"
        );
        assert_eq!(
            format!("{block}?offset=0"),
            "/block/project%20%3F%23%E9%9B%AA/name%20%3F%23%C3%A9%2F%25?offset=0"
        );
        assert_eq!(
            format!("{block}/unmap"),
            "/block/project%20%3F%23%E9%9B%AA/name%20%3F%23%C3%A9%2F%25/unmap"
        );
        assert_eq!(format!("{block}?offset=0&len=4&consistent=true"), "/block/project%20%3F%23%E9%9B%AA/name%20%3F%23%C3%A9%2F%25?offset=0&len=4&consistent=true");
    }

    #[test]
    fn consistent_flags_are_opt_in_and_default_off() {
        let plain: SqlQueryPayload = serde_json::from_str(r#"{"sql":"SELECT * FROM t"}"#).unwrap();
        assert_eq!(plain.consistent, None);
        let opt_in: SqlQueryPayload =
            serde_json::from_str(r#"{"sql":"SELECT * FROM t","consistent":true}"#).unwrap();
        assert_eq!(opt_in.consistent, Some(true));

        let plain: S3GetQuery = serde_json::from_str("{}").unwrap();
        assert!(!plain.consistent);
        let opt_in: S3GetQuery = serde_json::from_str(r#"{"consistent":true}"#).unwrap();
        assert!(opt_in.consistent);
    }

    // A consistent S3 read reports a chunk that the pinned manifest names but
    // that is gone as an availability failure (503), never as zeros or a short
    // body. The default path keeps its existing mapping.
    #[test]
    fn consistent_s3_read_maps_a_collected_chunk_to_availability() {
        let id = BlobId::new(1, 1, 1, 1, 4, 0);
        for err in [
            S3Error::Blob(BlobError::NotFound(id)),
            S3Error::Blob(BlobError::Tombstoned(id)),
            S3Error::Blob(BlobError::InsufficientFragments {
                available: 1,
                required: 4,
            }),
        ] {
            assert!(is_stale_chunk_error(&err), "{err:?} must map to 503");
        }
        assert!(!is_stale_chunk_error(&S3Error::ObjectNotFound {
            bucket: "b".into(),
            key: "k".into(),
        }));
        assert!(!is_stale_chunk_error(&S3Error::Blob(
            BlobError::CorruptData {
                id,
                reason: "corrupt",
            }
        )));
    }

    #[test]
    fn forwarding_error_outcomes_are_machine_readable() {
        let unknown = forward_error_response("failed".into(), Some("unknown"));
        assert_eq!(unknown.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unknown.headers()["x-yadb-write-outcome"], "unknown");
        assert_eq!(unknown.headers()["x-yadb-retry"], "do-not-retry");
        let not_sent = forward_error_response("not sent".into(), Some("not-sent"));
        assert_eq!(not_sent.headers()["x-yadb-write-outcome"], "not-sent");
        assert!(!not_sent.headers().contains_key("x-yadb-retry"));
    }

    // A refused connection never delivered the request, so the mutation
    // provably did not run; only a failure after the request left this node is
    // ambiguous. Reporting both as "unknown" refused writes that were safe to
    // retry (observed as 4/24 spurious 503s under a concurrent PUT burst).
    #[test]
    fn forwarding_distinguishes_not_sent_from_unknown_outcome() {
        // Reserve a port, then close it, so nothing is listening there.
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = closed.local_addr().unwrap();
        drop(closed);
        let failure = proxy_http_to_peer(
            &addr.to_string(),
            "PUT",
            "/v1/apply",
            "sa:test",
            None,
            "application/json",
            b"{}",
            None,
            ForwardedRead::default(),
        )
        .unwrap_err();
        assert!(!failure.sent, "a refused connect must not be ambiguous");
        assert_eq!(failure.outcome(), "not-sent");

        // A peer that accepts the request and closes without answering leaves
        // the outcome unknown: the leader may have committed before it died.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.peek(&mut buf);
            }
            // Dropping the stream closes it without a response.
        });
        let failure = proxy_http_to_peer(
            &addr.to_string(),
            "PUT",
            "/v1/apply",
            "sa:test",
            None,
            "application/json",
            b"{}",
            None,
            ForwardedRead::default(),
        )
        .unwrap_err();
        let _ = peer.join();
        assert!(failure.sent, "a sent request must stay ambiguous");
        assert_eq!(failure.outcome(), "unknown");
    }

    #[test]
    fn openraft_unknown_meta_error_is_non_retryable_503() {
        let mapped = meta_err_response(MetaError::OpenRaftWriteUnknown("timeout".into()), |_| None);
        assert_eq!(mapped.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.headers()["x-yadb-write-outcome"], "unknown");
        assert_eq!(mapped.headers()["x-yadb-retry"], "do-not-retry");
    }

    // S7: the legacy path has the same ambiguity as the OpenRaft bridge, but its
    // error was mapped to a plain 500, so a client could not tell "unknown" from
    // "rejected" and would retry a mutation that may already have committed.
    #[test]
    fn legacy_uncommitted_proposal_is_reported_as_unknown_not_failed() {
        let mapped = meta_err_response(MetaError::UncommittedIndex(42), |_| None);
        assert_eq!(mapped.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.headers()["x-yadb-write-outcome"], "unknown");
        assert_eq!(mapped.headers()["x-yadb-retry"], "do-not-retry");
    }

    // The distinction must survive for errors that really are definite: a client
    // that treats every 5xx as "unknown" would stop retrying a plain rejection.
    #[test]
    fn definite_meta_errors_stay_500_without_an_outcome_header() {
        for err in [
            MetaError::CorruptWal("bad record"),
            MetaError::Io(std::io::Error::other("disk")),
        ] {
            let mapped = meta_err_response(err, |_| None);
            assert_eq!(mapped.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert!(
                !mapped.headers().contains_key("x-yadb-write-outcome"),
                "a definite failure must not claim an unknown outcome"
            );
        }
    }

    // An unavailable metadata read is "not ready", not "not found" and not an
    // internal error: the client should ask another replica, not conclude the
    // key is absent or that its request was rejected.
    #[test]
    fn unavailable_metadata_read_is_not_ready_503() {
        let mapped = meta_err_response(MetaError::StorageUnavailable, |_| None);
        assert_eq!(mapped.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            !mapped.headers().contains_key("x-yadb-write-outcome"),
            "a read failure is not an ambiguous write"
        );
    }

    #[test]
    fn stale_control_authorization_fails_closed_even_if_snapshot_grants() {
        use yadb_control::{MemberBinding, ProjectSpec};

        let now = Instant::now();
        let mut cp = ControlPlaneState::new();
        cp.projects.insert(
            "alpha".into(),
            (
                1,
                ProjectSpec {
                    quotas: Default::default(),
                    members: vec![MemberBinding {
                        subject: "tester".into(),
                        role: Role::ReadWrite,
                    }],
                },
            ),
        );
        // The locally applied snapshot would grant access...
        assert!(cp.authorize("alpha", "tester", Role::ReadWrite).is_ok());
        // ...but without a quorum-confirmed lease it must not be used.
        let mut freshness = ControlFreshness::default();
        assert!(!control_authorization_fresh(true, &freshness, now));
        // Legacy mode keeps its existing behavior.
        assert!(control_authorization_fresh(false, &freshness, now));

        assert!(freshness.accept_at(7, now, Duration::from_secs(2)));
        assert!(control_authorization_fresh(
            true,
            &freshness,
            now + Duration::from_millis(1999)
        ));
        // An expired lease denies even though the stale snapshot still grants.
        assert!(!control_authorization_fresh(
            true,
            &freshness,
            now + Duration::from_secs(3)
        ));
        assert!(cp.authorize("alpha", "tester", Role::ReadWrite).is_ok());

        // A revoked snapshot is denied once a fresh lease confirms it.
        cp.projects.get_mut("alpha").unwrap().1.members.clear();
        assert!(cp.authorize("alpha", "tester", Role::ReadWrite).is_err());
        assert!(freshness.accept_at(8, now, Duration::from_secs(2)));
        assert!(control_authorization_fresh(
            true,
            &freshness,
            now + Duration::from_secs(1)
        ));
    }

    #[test]
    fn control_freshness_rejects_revision_regression_and_never_extends_lease() {
        let now = Instant::now();
        let mut freshness = ControlFreshness::default();
        assert!(freshness.accept_at(9, now, Duration::from_secs(1)));
        assert!(!freshness.accept_at(8, now + Duration::from_millis(1500), Duration::from_secs(5)));
        assert!(!freshness.is_fresh_at(now + Duration::from_millis(1500)));
        assert!(freshness.accept_at(9, now + Duration::from_millis(1500), Duration::from_secs(1)));
        assert!(freshness.is_fresh_at(now + Duration::from_millis(2000)));
        freshness.invalidate();
        assert!(!freshness.is_fresh_at(now + Duration::from_millis(2000)));
    }

    // A proxied read must arrive at the leader with the caller's byte range and
    // conditions, because the leader (not the follower) decides them from the
    // manifest it serves.
    #[test]
    fn a_forwarded_read_carries_its_range_and_conditions() {
        let request = forwarded_request(
            "GET",
            "/s3/proj/store/bucket/key",
            "127.0.0.1:1",
            "sa:billing",
            None,
            "application/octet-stream",
            0,
            None,
            ForwardedRead {
                range: Some("bytes=7-999"),
                if_match: Some(b"\"a\", \"b\""),
                if_none_match: Some(b"W/\"c\""),
            },
        );
        assert!(
            request.starts_with(b"GET /s3/proj/store/bucket/key HTTP/1.1\r\n"),
            "{request:?}"
        );
        for line in [
            &b"Range: bytes=7-999\r\n"[..],
            &b"If-Match: \"a\", \"b\"\r\n"[..],
            &b"If-None-Match: W/\"c\"\r\n"[..],
            &b"x-yadb-forwarded: 1\r\n"[..],
        ] {
            assert!(
                request.windows(line.len()).any(|window| window == line),
                "missing {line:?} in {request:?}"
            );
        }
        assert!(request.ends_with(b"\r\n\r\n"), "{request:?}");

        // An `obs-text` tag is forwarded with its exact bytes; a lossy text
        // conversion could turn it into a tag that names another version.
        let obs_text = forwarded_request(
            "GET",
            "/s3/x",
            "127.0.0.1:1",
            "sa:billing",
            None,
            "text/plain",
            0,
            None,
            ForwardedRead {
                range: None,
                if_match: Some(b"\"\x80\""),
                if_none_match: None,
            },
        );
        assert!(
            obs_text
                .windows(b"If-Match: \"\x80\"\r\n".len())
                .any(|window| window == b"If-Match: \"\x80\"\r\n"),
            "{obs_text:?}"
        );

        // A read with no conditions carries none of these headers, so a plain
        // read is unchanged by this path.
        let plain = forwarded_request(
            "GET",
            "/s3/x",
            "127.0.0.1:1",
            "sa:billing",
            None,
            "text/plain",
            0,
            None,
            ForwardedRead::default(),
        );
        for line in [&b"Range:"[..], &b"If-Match:"[..], &b"If-None-Match:"[..]] {
            assert!(
                !plain.windows(line.len()).any(|window| window == line),
                "{line:?} must be absent from {plain:?}"
            );
        }

        // These values reach the request only as `HeaderValue`s, and one that
        // would add a header line cannot be built at all.
        assert!(HeaderValue::from_str("bytes=0-1\r\nX-Injected: 1").is_err());
    }

    /// The header map a follower forwards: the combined value, byte-exact.
    fn forwarded_condition_map(headers: &HeaderMap, name: &HeaderName) -> HeaderMap {
        let mut forwarded = HeaderMap::new();
        if let Some(value) = joined_field_value(headers, name) {
            forwarded.insert(name.clone(), HeaderValue::from_bytes(&value).unwrap());
        }
        forwarded
    }

    // A conditional header may arrive as several field lines. The forwarded
    // request carries the combined value the local evaluator reads, so the
    // leader cannot interpret the header differently from this node.
    #[test]
    fn a_forwarded_condition_keeps_every_field_line() {
        let mut headers = headers_with(&[("if-none-match", "\"a\"")]);
        headers.append(header::IF_NONE_MATCH, HeaderValue::from_static("\"b\""));
        assert_eq!(
            joined_field_value(&headers, &header::IF_NONE_MATCH).as_deref(),
            Some(&b"\"a\", \"b\""[..])
        );
        // The other header is absent, so it is not forwarded at all.
        assert!(joined_field_value(&headers, &header::IF_MATCH).is_none());

        // Combining is lossless: an `obs-text` tag keeps its bytes, so a
        // forwarded tag cannot start naming a different validator.
        let mut obs_text = headers_with(&[("if-match", "\"other\"")]);
        obs_text.append(
            header::IF_MATCH,
            HeaderValue::from_bytes(b"\"\x80\"").unwrap(),
        );
        assert_eq!(
            joined_field_value(&obs_text, &header::IF_MATCH).as_deref(),
            Some(&b"\"other\", \"\x80\""[..])
        );
    }

    // The local evaluator and the forwarded request must read the same combined
    // value, including when one field line is unreadable. Otherwise a follower
    // and the leader would answer the same request differently.
    #[test]
    fn a_forwarded_condition_is_evaluated_like_the_local_one() {
        let mut headers = headers_with(&[("if-match", "\"abc-11\"")]);
        headers.append(header::IF_MATCH, HeaderValue::from_static("junk"));
        assert!(matches!(
            evaluate_conditions(&headers, "\"abc-11\""),
            Conditional::Failed
        ));
        let forwarded = forwarded_condition_map(&headers, &header::IF_MATCH);
        assert!(matches!(
            evaluate_conditions(&forwarded, "\"abc-11\""),
            Conditional::Failed
        ));

        // For `If-None-Match` an unreadable value names no confirmable version,
        // so it does not suppress the bytes at either hop.
        let mut headers = headers_with(&[("if-none-match", "\"abc-11\"")]);
        headers.append(header::IF_NONE_MATCH, HeaderValue::from_static("junk"));
        assert!(matches!(
            evaluate_conditions(&headers, "\"abc-11\""),
            Conditional::Passed
        ));
        let forwarded = forwarded_condition_map(&headers, &header::IF_NONE_MATCH);
        assert!(matches!(
            evaluate_conditions(&forwarded, "\"abc-11\""),
            Conditional::Passed
        ));

        // A wildcard plus a malformed line is one unreadable value too.
        let mut headers = headers_with(&[("if-match", "*")]);
        headers.append(header::IF_MATCH, HeaderValue::from_static("junk"));
        assert!(matches!(
            evaluate_conditions(&headers, "\"abc-11\""),
            Conditional::Failed
        ));

        // Well-formed lines still combine into a list that matches either way.
        let mut headers = headers_with(&[("if-match", "\"other\"")]);
        headers.append(header::IF_MATCH, HeaderValue::from_static("\"abc-11\""));
        assert!(matches!(
            evaluate_conditions(&headers, "\"abc-11\""),
            Conditional::Passed
        ));
        let forwarded = forwarded_condition_map(&headers, &header::IF_MATCH);
        assert!(matches!(
            evaluate_conditions(&forwarded, "\"abc-11\""),
            Conditional::Passed
        ));
    }

    #[test]
    fn proxy_response_accepts_a_bodyless_304_without_content_length() {
        // A peer may omit Content-Length on a 304. It is not a close-delimited
        // body: anything but an empty payload is malformed.
        let response = parse_proxy_response(b"HTTP/1.1 304 Not Modified\r\nETag: \"a\"\r\n\r\n")
            .expect("a 304 needs no content-length");
        assert_eq!(response.code, 304);
        assert_eq!(response.etag.as_deref(), Some("\"a\""));
        assert!(response.body.is_empty());
        assert!(parse_proxy_response(b"HTTP/1.1 304 Not Modified\r\n\r\nx").is_err());
    }

    #[test]
    fn proxy_response_accepts_a_bodyless_head_with_its_declared_length() {
        // A forwarded HEAD reports the object's size but carries no bytes. The
        // GET framing rule (payload must equal Content-Length) would reject it.
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 4096\r\nAccept-Ranges: bytes\r\n\r\n";
        let response = parse_proxy_response_for(raw, true).unwrap();
        assert_eq!(response.code, 200);
        assert_eq!(response.content_length, Some(4096));
        assert!(response.body.is_empty());
        // A HEAD answer with bytes is malformed, not a body to forward.
        let with_body = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody";
        assert!(parse_proxy_response_for(with_body, true).is_err());
        // The GET rule still requires the declared length to be present.
        assert!(parse_proxy_response(raw).is_err());
    }

    #[test]
    fn proxy_response_requires_complete_framing_and_preserves_outcome() {
        let raw = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 2\r\nx-yadb-write-outcome: unknown\r\nx-yadb-retry: do-not-retry\r\n\r\nno";
        let response = parse_proxy_response(raw).unwrap();
        assert_eq!(response.body, b"no");
        assert_eq!(response.write_outcome.as_deref(), Some("unknown"));
        assert_eq!(response.retry.as_deref(), Some("do-not-retry"));
        assert!(parse_proxy_response(&raw[..raw.len() - 1]).is_err());
        assert!(parse_proxy_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\n"
        )
        .is_err());
        assert!(parse_proxy_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\naX\r\n0\r\n\r\n"
        )
        .is_err());
    }

    #[test]
    fn proxy_response_preserves_allowlisted_entity_headers_and_rejects_duplicates() {
        let raw = b"HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\nContent-Range: bytes 0-4/11\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nAccept-Ranges: bytes\r\nCache-Control: public\r\nContent-Disposition: inline\r\n\r\nhello";
        let response = parse_proxy_response(raw).unwrap();
        assert!(response
            .headers
            .iter()
            .any(|(name, value)| name == "content-range" && value == "bytes 0-4/11"));
        assert!(parse_proxy_response(
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\nContent-Range: bytes 0-4/11\r\ncontent-range: bytes 0-4/11\r\n\r\nhello"
        )
        .is_err());
    }

    #[test]
    fn range_end_past_the_size_is_clamped_but_a_start_past_it_is_refused() {
        // RFC 9110: an end past the representation is satisfiable and denotes
        // the remainder, so it is clamped rather than refused. This keeps the
        // gateway identical to the native listener a follower forwards to.
        assert_eq!(parse_range_header("bytes=7-999", 9), Some((7, 8)));
        assert_eq!(parse_range_header("bytes=0-", 9), Some((0, 8)));
        assert_eq!(parse_range_header("bytes=4-4", 9), Some((4, 4)));
        // A start at or past the size, a reversed range, an empty object and a
        // malformed unit are all unsatisfiable.
        assert_eq!(parse_range_header("bytes=900-999", 9), None);
        assert_eq!(parse_range_header("bytes=9-9", 9), None);
        assert_eq!(parse_range_header("bytes=5-3", 9), None);
        assert_eq!(parse_range_header("bytes=0-0", 0), None);
        assert_eq!(parse_range_header("items=0-1", 9), None);
    }

    #[test]
    fn proxy_response_rejects_invalid_grammar_and_limits() {
        for raw in [
            &b"HTTP/2.0 200 OK\r\nContent-Length: 0\r\n\r\n"[..],
            b"HTTP/1.1 0200 OK\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: +0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1;x=y\r\na\r\n0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n 1\r\na\r\n0\r\n\r\n",
        ] {
            assert!(parse_proxy_response(raw).is_err());
        }
        let oversized_header = format!(
            "HTTP/1.1 200 OK\r\nX-Test: {}\r\nContent-Length: 0\r\n\r\n",
            "a".repeat(MAX_PROXY_HEADERS)
        );
        assert!(parse_proxy_response(oversized_header.as_bytes()).is_err());
        let oversized_body = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            MAX_PROXY_BODY + 1
        ) + &"x".repeat(MAX_PROXY_BODY + 1);
        assert!(parse_proxy_response(oversized_body.as_bytes()).is_err());
        assert!(decode_http_chunked(b"1000001\r\n").is_err());
    }

    #[test]
    fn unavailable_or_missing_control_fails_closed() {
        assert!(decode_control_snapshot(Err(MetaError::StorageUnavailable)).is_none());
        let fs = yadb_io::SimFs::new(1);
        let meta = LocalMetaKv::open_single_node(&fs, "test-control", 1).unwrap();
        assert!(
            decode_control_snapshot(meta.try_snapshot(&[CONTROL_STATE_KEY.into()], &[])).is_none()
        );
        meta.execute(KvCommand::Put {
            key: CONTROL_STATE_KEY.into(),
            value: b"malformed".to_vec(),
        })
        .unwrap();
        assert!(
            decode_control_snapshot(meta.try_snapshot(&[CONTROL_STATE_KEY.into()], &[])).is_none()
        );
        let state = ControlPlaneState::new();
        meta.execute(KvCommand::Put {
            key: CONTROL_STATE_KEY.into(),
            value: serde_json::to_vec(&state).unwrap(),
        })
        .unwrap();
        assert!(
            decode_control_snapshot(meta.try_snapshot(&[CONTROL_STATE_KEY.into()], &[])).is_some()
        );
        meta.execute(KvCommand::Delete {
            key: CONTROL_STATE_KEY.into(),
        })
        .unwrap();
        assert!(
            decode_control_snapshot(meta.try_snapshot(&[CONTROL_STATE_KEY.into()], &[])).is_none()
        );
    }

    #[test]
    fn resolve_refuses_capacity_as_unknown_not_as_a_quota_status() {
        // `resolve` must never answer with something a client cannot classify:
        // the three documented shapes are committed / not_committed / unknown,
        // and a fence that cannot be admitted did not establish either outcome.
        let response = resolve_capacity_exhausted();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn idempotency_usage_admits_until_both_limits() {
        let empty = IdempotencyUsage {
            version: 1,
            records: 0,
            bytes: 0,
        };
        assert!(empty.admits(1));
        assert!(empty.admits(MAX_IDEMPOTENCY_BYTES as usize));

        let full_records = IdempotencyUsage {
            version: 1,
            records: MAX_IDEMPOTENCY_RECORDS,
            bytes: 0,
        };
        assert!(!full_records.admits(0), "the record ceiling must bind");

        let full_bytes = IdempotencyUsage {
            version: 1,
            records: 1,
            bytes: MAX_IDEMPOTENCY_BYTES,
        };
        assert!(!full_bytes.admits(1), "the byte ceiling must bind");
        assert!(full_bytes.admits(0), "an empty record still fits");

        let one_byte_left = IdempotencyUsage {
            version: 1,
            records: 1,
            bytes: MAX_IDEMPOTENCY_BYTES - 1,
        };
        assert!(one_byte_left.admits(1));
        assert!(!one_byte_left.admits(2));
    }

    #[test]
    fn idempotency_usage_round_trips_and_defaults_missing_fields() {
        let usage = IdempotencyUsage {
            version: 1,
            records: 7,
            bytes: 4096,
        };
        let encoded = serde_json::to_vec(&usage).unwrap();
        let decoded = decode_usage(&encoded).unwrap();
        assert_eq!((decoded.records, decoded.bytes), (7, 4096));

        // The fields are `#[serde(default)]` so a hand-written or partially
        // migrated counter cannot fail the whole keyed path on deserialization.
        // The real legacy case is a *missing key*, which `derive_idempotency_usage`
        // handles by scanning the existing records; a present-but-empty object
        // therefore only ever means "zero", never "unknown".
        let empty = decode_usage(b"{}").unwrap();
        assert_eq!((empty.records, empty.bytes), (0, 0));
    }

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    #[test]
    fn raft_token_is_required_when_configured() {
        let headers = headers_with(&[("x-yadb-raft-token", "secret")]);
        assert!(raft_token_matches(&headers, Some("secret")));
        assert!(!raft_token_matches(&headers, Some("other")));
        assert!(!raft_token_matches(&headers, Some("secret-longer")));
        assert!(!raft_token_matches(&headers_with(&[]), Some("secret")));
        assert!(!raft_token_matches(
            &headers_with(&[("x-yadb-raft-token", "wrong")]),
            Some("secret")
        ));
    }

    #[tokio::test]
    async fn vdisk_handlers_require_peer_token_and_preserve_legacy_mode() {
        let dir = std::env::temp_dir().join(format!(
            "yadb-vdisk-auth-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let fs = OsFs::new(&dir).unwrap();
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let mut st = AppState {
            openraft: None,
            listen_addr: "127.0.0.1:0".into(),
            fs,
            meta: meta.clone(),
            raft_token: Some("secret".into()),
            subject_secret: None,
            admin_subjects: vec![],
            control: Arc::new(RwLock::new(ControlPlaneState::new())),
            control_refresh: Arc::new(std::sync::Mutex::new(())),
            control_freshness: Arc::new(std::sync::Mutex::new(ControlFreshness::default())),
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            local_vdisks: Arc::new(RwLock::new(BTreeMap::new())),
            last_scrub: Arc::new(RwLock::new(None)),
            reader_lease: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            s3: ObjectStoreEngine::new(meta.clone(), 4096),
            db: DatabaseEngine::new(meta.clone(), 100),
            block: BlockVolumeEngine::new(meta, DEFAULT_BLOCK_SIZE),
        };
        st.open_or_get_local_vdisk("pool", 0).unwrap();
        for (configured, supplied, expected) in [
            (true, None, StatusCode::UNAUTHORIZED),
            (true, Some("wrong"), StatusCode::UNAUTHORIZED),
            (true, Some("secret"), StatusCode::OK),
            (false, None, StatusCode::OK),
        ] {
            st.raft_token = configured.then(|| "secret".into());
            let headers = supplied.map_or_else(HeaderMap::new, |token| {
                headers_with(&[("x-yadb-raft-token", token)])
            });
            let blob = BlobId::new(1, 2, 3, 4, 5, 6).to_hex();
            let blob_path = || Path(("pool".to_string(), 0, blob.clone()));
            let slot_path = || Path(("pool".to_string(), 0));
            let put = handle_vdisk_put(
                State(st.clone()),
                blob_path(),
                headers.clone(),
                Bytes::from_static(b"data"),
            )
            .await;
            assert_eq!(put.status(), expected);
            let get = handle_vdisk_get(State(st.clone()), blob_path(), headers.clone()).await;
            assert_eq!(get.status(), expected);
            let stats = handle_vdisk_stats(State(st.clone()), slot_path(), headers.clone()).await;
            assert_eq!(stats.status(), expected);
            let delete = handle_vdisk_delete(
                State(st.clone()),
                blob_path(),
                Query(VdiskDeleteQuery::default()),
                headers.clone(),
            )
            .await;
            assert_eq!(delete.status(), expected);
            let barrier = handle_vdisk_barrier(
                State(st.clone()),
                slot_path(),
                headers,
                Json(BarrierRequest {
                    project_id: 9,
                    owner_id: 9,
                    channel: 9,
                    up_to_generation: 0x00ff_ffff,
                    compact: false,
                    // This test drives the handler directly and holds no
                    // collector lease; the deadline is the far future so the
                    // refusal path under test is the auth/route one, not expiry.
                    lease_expires_at_ms: u64::MAX,
                }),
            )
            .await;
            assert_eq!(barrier.status(), expected);
            // Use a fresh ID in each successful iteration: DELETE installs a permanent tombstone.
            if expected == StatusCode::OK {
                break;
            }
        }
        // The legacy path is exercised with a fresh store after the authenticated mutation.
        st.raft_token = None;
        let headers = HeaderMap::new();
        let blob = BlobId::new(10, 2, 3, 4, 5, 6).to_hex();
        let path = || Path(("pool".to_string(), 0, blob.clone()));
        assert_eq!(
            handle_vdisk_put(
                State(st.clone()),
                path(),
                headers.clone(),
                Bytes::from_static(b"data")
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            handle_vdisk_get(State(st.clone()), path(), headers.clone())
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            handle_vdisk_stats(State(st.clone()), Path(("pool".into(), 0)), headers.clone())
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            handle_vdisk_delete(
                State(st.clone()),
                path(),
                Query(VdiskDeleteQuery::default()),
                headers.clone()
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            handle_vdisk_barrier(
                State(st.clone()),
                Path(("pool".into(), 0)),
                headers,
                Json(BarrierRequest {
                    project_id: 9,
                    owner_id: 9,
                    channel: 9,
                    up_to_generation: 0x00ff_ffff,
                    compact: false,
                    // No collector lease in this test: see the note above.
                    lease_expires_at_ms: u64::MAX,
                })
            )
            .await
            .status(),
            StatusCode::OK
        );
        drop(st);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn an_expired_barrier_answers_409_and_evicts_nothing() {
        // The target-side deadline check has to surface as a refusal the collector
        // stops on, not as a 500 it might retry. `409` also keeps it distinct from
        // a malformed request (422) and a missing slot (404).
        let dir = std::env::temp_dir().join(format!(
            "yadb-vdisk-barrier-expired-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let fs = OsFs::new(&dir).unwrap();
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let st = AppState {
            openraft: None,
            listen_addr: "127.0.0.1:0".into(),
            fs,
            meta: meta.clone(),
            raft_token: None,
            subject_secret: None,
            admin_subjects: vec![],
            control: Arc::new(RwLock::new(ControlPlaneState::new())),
            control_refresh: Arc::new(std::sync::Mutex::new(())),
            control_freshness: Arc::new(std::sync::Mutex::new(ControlFreshness::default())),
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            local_vdisks: Arc::new(RwLock::new(BTreeMap::new())),
            last_scrub: Arc::new(RwLock::new(None)),
            reader_lease: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            s3: ObjectStoreEngine::new(meta.clone(), 4096),
            db: DatabaseEngine::new(meta.clone(), 100),
            block: BlockVolumeEngine::new(meta, DEFAULT_BLOCK_SIZE),
        };
        st.open_or_get_local_vdisk("pool", 0).unwrap();
        let id = BlobId::new(1, 2, 1, 0, yadb_s3::S3_CHANNEL, 0);
        let path = || Path(("pool".to_string(), 0, id.to_hex()));
        let headers = HeaderMap::new();
        assert_eq!(
            handle_vdisk_put(
                State(st.clone()),
                path(),
                headers.clone(),
                Bytes::from_static(b"pinned fragment"),
            )
            .await
            .status(),
            StatusCode::OK
        );

        let response = handle_vdisk_barrier(
            State(st.clone()),
            Path(("pool".to_string(), 0)),
            headers.clone(),
            Json(BarrierRequest {
                project_id: id.project_id(),
                owner_id: id.owner_id(),
                channel: id.channel(),
                up_to_generation: id.generation(),
                compact: false,
                lease_expires_at_ms: 1,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);

        // The fragment the barrier would have evicted is still readable, and no
        // durable barrier was left behind: a later write in the same range is
        // still accepted. A different step, because the same id is immutable.
        assert_eq!(
            handle_vdisk_get(State(st.clone()), path(), headers.clone())
                .await
                .status(),
            StatusCode::OK
        );
        let later = BlobId::new(1, 2, 1, 1, yadb_s3::S3_CHANNEL, 0);
        assert_eq!(
            handle_vdisk_put(
                State(st.clone()),
                Path(("pool".to_string(), 0, later.to_hex())),
                headers,
                Bytes::from_static(b"still writable"),
            )
            .await
            .status(),
            StatusCode::OK
        );
        drop(st);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn project_records_are_scoped_by_project_subject_and_key() {
        // A group image and a Raft snapshot are readable by anyone who can
        // inspect storage, so a record must not disclose which keys or subjects a
        // project used: both halves are hashed.
        let records = "/idempotency/7/";
        let a = project_record_key(records, "tester", "k1");
        assert!(a.starts_with(records));
        assert!(!a.contains("tester"));
        assert!(!a.contains("k1"));
        assert_eq!(a, project_record_key(records, "tester", "k1"));
        // Same key, different subject or project: a different record.
        assert_ne!(a, project_record_key(records, "other", "k1"));
        assert_ne!(a, project_record_key("/idempotency/8/", "tester", "k1"));
        assert_ne!(a, project_record_key(records, "tester", "k2"));
    }

    #[test]
    fn block_unmap_fingerprint_covers_the_received_bytes() {
        let one = br#"{"offset":0,"len":4096}"#;
        let reformatted = br#"{"offset": 0, "len": 4096}"#;
        // The fingerprint is over the bytes the client sent. Two bodies that
        // parse to the same struct are different requests, and replaying one for
        // the other would return a response for a request that was never made.
        assert_ne!(
            block_unmap_fingerprint(7, "disk", one),
            block_unmap_fingerprint(7, "disk", reformatted)
        );
        // The target is part of the fingerprint: the same key and body against a
        // different volume or project is a conflict, not a replay.
        assert_ne!(
            block_unmap_fingerprint(7, "disk", one),
            block_unmap_fingerprint(7, "other", one)
        );
        assert_ne!(
            block_unmap_fingerprint(7, "disk", one),
            block_unmap_fingerprint(8, "disk", one)
        );
        assert_eq!(
            block_unmap_fingerprint(7, "disk", one),
            block_unmap_fingerprint(7, "disk", one)
        );
    }

    #[test]
    fn block_write_fingerprint_covers_offset_and_received_bytes() {
        let one = b"first-payload";
        // Same request, same digest.
        assert_eq!(
            block_write_fingerprint(7, "disk", 4096, one),
            block_write_fingerprint(7, "disk", 4096, one)
        );
        // The body is hashed as received, so two byte-different bodies conflict
        // even if they carry the same bytes once parsed.
        assert_ne!(
            block_write_fingerprint(7, "disk", 4096, b"one"),
            block_write_fingerprint(7, "disk", 4096, b"two")
        );
        // The offset is a query parameter, so it must be in the fingerprint
        // explicitly: the same body at another offset is another request.
        assert_ne!(
            block_write_fingerprint(7, "disk", 4096, one),
            block_write_fingerprint(7, "disk", 8192, one)
        );
        // The target is part of the fingerprint.
        assert_ne!(
            block_write_fingerprint(7, "disk", 4096, one),
            block_write_fingerprint(7, "other", 4096, one)
        );
        assert_ne!(
            block_write_fingerprint(7, "disk", 4096, one),
            block_write_fingerprint(8, "disk", 4096, one)
        );
        // Domain separation: a key reused across a write and an unmap of the
        // same volume must conflict rather than replay the wrong response.
        assert_ne!(
            block_write_fingerprint(7, "disk", 0, br#"{"offset":0,"len":4096}"#),
            block_unmap_fingerprint(7, "disk", br#"{"offset":0,"len":4096}"#)
        );
        // Length prefixes keep the offset from running into the body: shifting
        // a digit into the payload must not collide.
        assert_ne!(
            block_write_fingerprint(7, "disk", 1, b"2"),
            block_write_fingerprint(7, "disk", 12, b"")
        );
    }

    #[test]
    fn s3_fingerprint_covers_operation_target_and_body() {
        let body = b"payload";
        // Same request, same digest.
        assert_eq!(
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/b", body),
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/b", body)
        );
        // PUT and DELETE are different operations even for the same target.
        assert_ne!(
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/b", body),
            s3_object_fingerprint("DELETE", "s3-delete", 7, "store", "bucket", "a/b", b"")
        );
        // The body is hashed as received (DELETE hashes an empty body).
        assert_ne!(
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/b", b"one"),
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/b", b"two")
        );
        assert_ne!(
            s3_object_fingerprint("DELETE", "s3-delete", 7, "store", "bucket", "a/b", b""),
            s3_object_fingerprint("DELETE", "s3-delete", 7, "store", "bucket", "a/b", b"x")
        );
        // Every path component is part of the target; a key may contain "/",
        // so the fields are length-prefixed rather than joined.
        assert_ne!(
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/b", body),
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/bc", body)
        );
        assert_ne!(
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/b", body),
            s3_object_fingerprint("PUT", "s3-put", 7, "other", "bucket", "a/b", body)
        );
        assert_ne!(
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/b", body),
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "other", "a/b", body)
        );
        assert_ne!(
            s3_object_fingerprint("PUT", "s3-put", 7, "store", "bucket", "a/b", body),
            s3_object_fingerprint("PUT", "s3-put", 8, "store", "bucket", "a/b", body)
        );
        // Domain separation from the block fingerprints.
        assert_ne!(
            s3_object_fingerprint("PUT", "s3-put", 7, "disk", "bucket", "a/b", body),
            block_write_fingerprint(7, "disk", 0, body)
        );
    }

    #[test]
    fn admission_writes_refuse_at_the_record_ceiling() {
        let usage = IdempotencyUsage {
            version: 1,
            records: MAX_IDEMPOTENCY_RECORDS,
            bytes: 0,
        };
        let refusal = admission_writes(&usage, "/idempotency/g2/_usage", "/idempotency/1/r", b"x")
            .expect_err("a full counter must not admit another record");
        assert_eq!(refusal.status(), StatusCode::TOO_MANY_REQUESTS);
        // At capacity the record and the counter are returned together, never
        // one without the other: a record that committed without charging the
        // counter would let the group image grow past the limit it enforces.
        let usage = IdempotencyUsage {
            version: 1,
            records: 1,
            bytes: 10,
        };
        let (record, counter) =
            admission_writes(&usage, "/idempotency/g2/_usage", "/idempotency/1/r", b"abc").unwrap();
        match (record, counter) {
            (
                yadb_metakv::TxnOp::Put { key, .. },
                yadb_metakv::TxnOp::Put {
                    key: counter_key,
                    value,
                },
            ) => {
                assert_eq!(key, "/idempotency/1/r");
                assert_eq!(counter_key, "/idempotency/g2/_usage");
                let next = decode_usage(&value).unwrap();
                assert_eq!((next.records, next.bytes), (2, 13));
            }
            _ => panic!("admission must produce two puts"),
        }
        // The byte ceiling is the binding one.
        let usage = IdempotencyUsage {
            version: 1,
            records: 0,
            bytes: MAX_IDEMPOTENCY_BYTES,
        };
        assert!(
            admission_writes(&usage, "/idempotency/g2/_usage", "/idempotency/1/r", b"x").is_err(),
            "a full byte budget must refuse another record"
        );
    }

    #[test]
    fn scrub_requires_a_signed_admin_when_an_allowlist_is_configured() {
        // Scrub mutates: it collects orphan blobs, rewrites fragments and records
        // a summary. The route must not be reachable without the same credential
        // `/v1/apply` requires. An empty allowlist is the historical legacy mode
        // and stays open, so this asserts both halves.
        let secret = "subject-secret";
        let admins = vec!["operator".to_string()];
        let signed = headers_with(&[
            ("x-yadb-subject", "operator"),
            (
                "x-yadb-subject-signature",
                &subject_signature(secret, "operator"),
            ),
        ]);
        assert_eq!(
            require_admin(&signed, Some(secret), &admins, true).unwrap(),
            Some("operator".to_string())
        );

        // No credential at all: unverified, so the handler refuses before scrubbing.
        assert!(matches!(
            require_admin(&HeaderMap::new(), Some(secret), &admins, true),
            Err(ControlAdminError::Unverified)
        ));
        // A forged signature is not an identity.
        let forged = headers_with(&[
            ("x-yadb-subject", "operator"),
            ("x-yadb-subject-signature", "deadbeef"),
        ]);
        assert!(matches!(
            require_admin(&forged, Some(secret), &admins, true),
            Err(ControlAdminError::Unverified)
        ));
        // A verified but non-admin subject is a different refusal.
        let other = headers_with(&[
            ("x-yadb-subject", "intruder"),
            (
                "x-yadb-subject-signature",
                &subject_signature(secret, "intruder"),
            ),
        ]);
        assert!(matches!(
            require_admin(&other, Some(secret), &admins, true),
            Err(ControlAdminError::NotAdmin)
        ));
        // Legacy: no allowlist means the historical no-auth mode is preserved.
        assert_eq!(
            require_admin(&HeaderMap::new(), None, &[], false).unwrap(),
            None
        );
        // But an OpenRaft daemon never reaches that branch: it cannot start
        // without an allowlist, and an empty one is a hard refusal here too.
        assert!(matches!(
            require_admin(&HeaderMap::new(), Some(secret), &[], true),
            Err(ControlAdminError::NotConfigured)
        ));
    }

    #[test]
    fn raft_token_is_optional_without_configuration() {
        assert!(raft_token_matches(&headers_with(&[]), None));
    }

    #[test]
    fn empty_raft_token_is_rejected_not_treated_as_wildcard() {
        // An absent header decodes to "", so an empty configured token must
        // never authenticate a caller.
        assert!(!raft_token_matches(&headers_with(&[]), Some("")));
        assert!(!raft_token_matches(
            &headers_with(&[("x-yadb-raft-token", "")]),
            Some("")
        ));
    }

    #[test]
    fn unsigned_subject_is_not_trusted_when_secret_is_configured() {
        let headers = headers_with(&[("x-yadb-subject", "alice")]);
        assert_eq!(extract_subject(&headers, None), "alice");
        assert_eq!(extract_subject(&headers, Some("secret")), "anonymous");
    }

    #[test]
    fn signed_subject_is_accepted_and_forgeries_are_rejected() {
        let secret = "shared-secret";
        let subject = "alice";
        let signature = subject_signature(secret, subject);
        let headers = headers_with(&[
            ("x-yadb-subject", subject),
            ("x-yadb-subject-signature", &signature),
        ]);
        assert_eq!(extract_subject(&headers, Some(secret)), "alice");
        assert_eq!(
            verified_subject(&headers, Some(secret)).as_deref(),
            Some("alice")
        );

        let tampered = headers_with(&[
            ("x-yadb-subject", "mallory"),
            ("x-yadb-subject-signature", &signature),
        ]);
        assert_eq!(extract_subject(&tampered, Some(secret)), "anonymous");
        assert!(verified_subject(&tampered, Some(secret)).is_none());

        let wrong_secret = headers_with(&[
            ("x-yadb-subject", subject),
            (
                "x-yadb-subject-signature",
                &subject_signature("other-secret", subject),
            ),
        ]);
        assert!(verified_subject(&wrong_secret, Some(secret)).is_none());
    }

    /// A legacy daemon state with one mirror pool, one project and one block
    /// volume: enough to exercise the request paths that read fragments.
    fn legacy_fragment_state(tag: &str) -> AppState {
        legacy_fragment_state_with_dir(tag).0
    }

    /// Opening one slot directory concurrently must hand out one store.
    ///
    /// Two instances over one directory each keep their own `write_pos`,
    /// tombstones and barriers and append to the same segment from the same
    /// offset. The later writer then overwrites records the first one
    /// acknowledged: a tombstone or GC barrier disappears from disk, and after
    /// a restart the deleted blob is writable again. This is the shape of a
    /// live flake where an internal `DELETE` tombstone vanished while the
    /// engine kept writing to the slot.
    #[test]
    fn concurrent_local_vdisk_opens_return_one_store() {
        let (state, dir) = legacy_fragment_state_with_dir("vdisk-open-race");
        let _dir = TempDirGuard(dir);
        let state = std::sync::Arc::new(state);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let state = state.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                state.open_or_get_local_vdisk("pool", 0).unwrap()
            }));
        }
        let stores: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        for store in &stores[1..] {
            assert!(
                std::sync::Arc::ptr_eq(&stores[0], store),
                "two VolumeStore instances were opened for one slot directory"
            );
        }
    }

    /// Removes a fixture directory when the test ends, including when an
    /// assertion panics mid-test.
    struct TempDirGuard(std::path::PathBuf);

    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The same fixture, with its root directory so a test that writes real
    /// payload can remove it afterwards.
    fn legacy_fragment_state_with_dir(tag: &str) -> (AppState, std::path::PathBuf) {
        use yadb_control::{BlockVolumeSpec, MemberBinding, ProjectQuotas, ProjectSpec};
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "yadb-gc-state-{tag}-{}-{nanos}",
            std::process::id()
        ));
        let fs = OsFs::new(&dir).unwrap();
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let stores = (0..3)
            .map(|i| {
                yadb_blob::VolumeStore::open(
                    fs.clone(),
                    format!("pool/data-{i}"),
                    yadb_blob::VolumeConfig::default(),
                )
                .unwrap()
            })
            .collect();
        let pool = std::sync::Arc::new(
            yadb_blob::PoolStriper::new(yadb_blob::ErasureScheme::Mirror { replicas: 3 }, stores)
                .unwrap(),
        );
        let mut control = ControlPlaneState::new();
        control.projects.insert(
            "proj".into(),
            (
                1,
                ProjectSpec {
                    quotas: ProjectQuotas::default(),
                    members: vec![MemberBinding {
                        subject: "anonymous".into(),
                        role: Role::ReadWrite,
                    }],
                },
            ),
        );
        control.block_volumes.insert(
            "proj/disk".into(),
            BlockVolumeSpec {
                pool: "data".into(),
                size_bytes: 1 << 20,
            },
        );
        // Fixtures describe a configured cluster; a zero generation is reserved
        // for a node whose control image has not been installed yet.
        control.generation = 1;
        let block = BlockVolumeEngine::new(meta.clone(), DEFAULT_BLOCK_SIZE);
        block.register_pool("data", pool.clone());
        let state = AppState {
            openraft: None,
            listen_addr: "127.0.0.1:0".into(),
            fs,
            meta: meta.clone(),
            raft_token: None,
            subject_secret: None,
            admin_subjects: vec![],
            control: Arc::new(RwLock::new(control)),
            control_refresh: Arc::new(std::sync::Mutex::new(())),
            control_freshness: Arc::new(std::sync::Mutex::new(ControlFreshness::default())),
            pools: Arc::new(RwLock::new(BTreeMap::from([("data".to_string(), pool)]))),
            local_vdisks: Arc::new(RwLock::new(BTreeMap::new())),
            last_scrub: Arc::new(RwLock::new(None)),
            reader_lease: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            s3: ObjectStoreEngine::new(meta.clone(), 4096),
            db: DatabaseEngine::new(meta.clone(), 100),
            block,
        };
        (state, dir)
    }

    async fn body_bytes(response: Response) -> Vec<u8> {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    // A consistent block read publishes a lease for the duration of the read, so
    // a collection pass that overlaps it refuses. The read itself never backs off
    // on a collector lease: its own applied view is at least as new as the
    // collector's mark, and it must either return the exact bytes of the revision
    // it pinned or fail with 503, never a short or zero-filled body.
    #[tokio::test]
    async fn consistent_block_read_fences_collection_and_is_never_partial() {
        let st = legacy_fragment_state("block-read-fence");
        st.block
            .write_at(1, "disk", "data", 1 << 20, 0, b"hello")
            .unwrap();
        let read = || {
            handle_block_read(
                State(st.clone()),
                HeaderMap::new(),
                Path(("proj".to_string(), "disk".to_string())),
                Query(BlockReadQuery {
                    offset: 0,
                    len: 5,
                    consistent: true,
                }),
            )
        };

        let ok = read().await;
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(body_bytes(ok).await, b"hello");

        // A live collector lease does not make the read refuse, and does not make
        // it return anything but the pinned bytes.
        let held = yadb_metakv::gc::acquire_collector(&st.meta, 0, 60_000).unwrap();
        let served = read().await;
        assert_eq!(served.status(), StatusCode::OK);
        assert_eq!(body_bytes(served).await, b"hello");
        held.release().unwrap();

        // The reverse direction is the safety property: while this node holds its
        // read lease, a collection pass refuses and deletes nothing.
        let pools = st.pools.read().unwrap().clone();
        let control = st.control.read().unwrap().clone();
        let outcome = crate::gc::sweep(&st.meta, &pools, &control);
        assert_eq!(outcome.leases_live, 1, "{outcome:?}");
        assert_eq!(outcome.fragments_deleted, 0, "{outcome:?}");
        assert!(outcome.refusal.unwrap().contains("live lease"));
        // Dropping the cached record, as an idle node does when it expires, lets
        // the pass run; the bytes stay because the extent map still references them.
        let cached = st.reader_lease.lock().unwrap().remove(&None).unwrap();
        yadb_metakv::gc::release(&st.meta, &cached).unwrap();
        assert!(
            crate::gc::sweep(&st.meta, &pools, &control)
                .refusal
                .is_none(),
            "an idle node must not block collection"
        );

        // Once the pass is gone the read is served again, from the same bytes,
        // and the node keeps exactly one record for reuse rather than one per read.
        let again = read().await;
        assert_eq!(again.status(), StatusCode::OK);
        assert_eq!(body_bytes(again).await, b"hello");
        assert_eq!(
            yadb_metakv::gc::live_leases(&st.meta).unwrap().len(),
            1,
            "reads must reuse a single lease, not leak one per statement"
        );
    }

    // The gateway read path applies the same 16 MiB rule as the native
    // listener. Serving any size here used to be possible, so this is a
    // deliberate change: it is what keeps a follower's proxied read inside the
    // forwarding wire cap, and it is documented in the README.
    #[tokio::test]
    async fn gateway_read_enforces_the_16_mib_cap() {
        let (state, dir) = legacy_fragment_state_with_dir("s3-read-cap");
        // Declared before `st` so the directory is removed after the state (and
        // its open handles) is dropped.
        let _dir = TempDirGuard(dir);
        let mut st = state;
        // Few large chunks keep this test to a handful of fragments; the blob
        // layer caps a single fragment at 10 MiB.
        st.s3 = ObjectStoreEngine::new(st.meta.clone(), 8 * 1024 * 1024);
        st.s3
            .register_pool("data", st.pools.read().unwrap()["data"].clone());
        st.control.write().unwrap().object_stores.insert(
            "proj/store".into(),
            yadb_control::ObjectStoreSpec {
                pool: "data".into(),
                isolation: Default::default(),
                buckets: vec![],
            },
        );
        let big = vec![7u8; S3_READ_CAP as usize + 1];
        st.s3
            .put_object(1, "store", "data", "bucket", "big", "text/plain", &big)
            .unwrap();
        st.s3
            .put_object(
                1,
                "store",
                "data",
                "bucket",
                "small",
                "text/plain",
                b"hello",
            )
            .unwrap();
        st.s3
            .put_object(1, "store", "data", "bucket", "empty", "text/plain", b"")
            .unwrap();

        let get = |key: &'static str, headers: HeaderMap| {
            let st = st.clone();
            async move {
                handle_s3_get(
                    Method::GET,
                    State(st),
                    headers,
                    Path((
                        "proj".to_string(),
                        "store".to_string(),
                        "bucket".to_string(),
                        key.to_string(),
                    )),
                    Query(S3GetQuery { consistent: false }),
                )
                .await
            }
        };

        // A full read over the cap is the documented 400 on this path too, not
        // a 200 that a follower could not carry across the wire.
        assert_eq!(
            get("big", HeaderMap::new()).await.status(),
            StatusCode::BAD_REQUEST
        );
        // A range within the cap is served from the same object.
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "bytes=0-9".parse().unwrap());
        let response = get("big", headers).await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(body_bytes(response).await, vec![7u8; 10]);
        // A range larger than the cap is refused the same way.
        let mut headers = HeaderMap::new();
        headers.insert(
            header::RANGE,
            format!("bytes=0-{S3_READ_CAP}").parse().unwrap(),
        );
        assert_eq!(get("big", headers).await.status(), StatusCode::BAD_REQUEST);
        // An object at or under the cap is unaffected.
        let response = get("small", HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, b"hello");
        // An empty object reads as an empty `200`, and any range on it is the
        // local `416` with `Content-Range: bytes */0`.
        let response = get("empty", HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, b"");
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "bytes=0-0".parse().unwrap());
        let response = get("empty", headers).await;
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes */0");
    }

    /// A gateway state whose one ObjectStore is usable end to end: one mirror
    /// pool, one project and one declared `proj/store` ObjectStore.
    fn s3_multipart_state(tag: &str) -> (AppState, std::path::PathBuf) {
        let (state, dir) = legacy_fragment_state_with_dir(tag);
        let mut st = state;
        st.s3 = ObjectStoreEngine::new(st.meta.clone(), 4096);
        st.s3
            .register_pool("data", st.pools.read().unwrap()["data"].clone());
        st.control.write().unwrap().object_stores.insert(
            "proj/store".into(),
            yadb_control::ObjectStoreSpec {
                pool: "data".into(),
                isolation: Default::default(),
                buckets: vec![],
            },
        );
        (st, dir)
    }

    // The multipart gateway handlers publish through the same engine as a single
    // PUT, so the assembled object must be indistinguishable from one written in
    // one piece: same bytes, same chunk offsets, and an ETag derived from the
    // part digests.
    #[tokio::test]
    async fn gateway_multipart_upload_assembles_an_object_identical_to_a_single_put() {
        let (state, dir) = s3_multipart_state("s3-multipart-roundtrip");
        let _dir = TempDirGuard(dir);
        let st = state;
        let key = "big/object.bin";
        let parts: [Vec<u8>; 3] = [vec![1u8; 5000], vec![2u8; 4096], vec![3u8; 17]];
        let expected: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();

        let response = handle_s3_create_multipart(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            key.into(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let created: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).unwrap();
        let upload_id = created["uploadId"].as_str().unwrap().to_string();
        assert!(upload_id.starts_with("mpu-"), "{upload_id}");

        let mut etags = Vec::new();
        for (index, part) in parts.iter().enumerate() {
            let response = handle_s3_upload_part(
                st.clone(),
                HeaderMap::new(),
                "proj".into(),
                "store".into(),
                "bucket".into(),
                key.into(),
                upload_id.clone(),
                index as u32 + 1,
                Bytes::copy_from_slice(part),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let etag = response.headers()[header::ETAG]
                .to_str()
                .unwrap()
                .to_string();
            assert_eq!(etag, format!("\"{}\"", yadb_s3::md5_hex(part)));
            etags.push((index as u32 + 1, etag.trim_matches('"').to_string()));
        }

        // An ETag the client did not upload is refused with InvalidPart, and the
        // upload survives the refusal so the client can retry correctly.
        let mut wrong = etags.clone();
        wrong[0].1 = "0".repeat(32);
        let response = handle_s3_complete_multipart(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            key.into(),
            upload_id.clone(),
            Bytes::from(completion_xml(&wrong)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let refusal = String::from_utf8(body_bytes(response).await).unwrap();
        assert!(refusal.contains("\"code\":\"InvalidPart\""), "{refusal}");
        assert!(st.s3.head_object(1, "store", "bucket", key).is_err());

        let response = handle_s3_complete_multipart(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            key.into(),
            upload_id.clone(),
            Bytes::from(completion_xml(&etags)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let completed: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).unwrap();
        let etag = completed["etag"].as_str().unwrap().to_string();
        assert_eq!(
            etag,
            format!(
                "\"{}-3\"",
                yadb_s3::md5_hex(
                    &parts
                        .iter()
                        .flat_map(|p| yadb_s3::md5(p).to_vec())
                        .collect::<Vec<_>>()
                )
            )
        );

        // The published manifest is a normal object: HEAD, full GET and a range
        // read all agree with the concatenated parts.
        let head = st.s3.head_object(1, "store", "bucket", key).unwrap();
        assert_eq!(head.size, expected.len() as u64);
        assert_eq!(head.etag, etag);
        let response = handle_s3_get(
            Method::GET,
            State(st.clone()),
            HeaderMap::new(),
            Path(("proj".into(), "store".into(), "bucket".into(), key.into())),
            Query(S3GetQuery { consistent: false }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, expected);
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "bytes=4096-4999".parse().unwrap());
        let response = handle_s3_get(
            Method::GET,
            State(st.clone()),
            headers,
            Path(("proj".into(), "store".into(), "bucket".into(), key.into())),
            Query(S3GetQuery { consistent: false }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(body_bytes(response).await, vec![1u8; 904]);

        // A second completion of the consumed upload is NoSuchUpload, not a
        // second publication.
        let response = handle_s3_complete_multipart(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            key.into(),
            upload_id,
            Bytes::from(completion_xml(&etags)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn gateway_multipart_abort_removes_the_upload_and_leaves_no_object() {
        let (state, dir) = s3_multipart_state("s3-multipart-abort");
        let _dir = TempDirGuard(dir);
        let st = state;
        let response = handle_s3_create_multipart(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            "aborted".into(),
        )
        .await;
        let created: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).unwrap();
        let upload_id = created["uploadId"].as_str().unwrap().to_string();
        handle_s3_upload_part(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            "aborted".into(),
            upload_id.clone(),
            1,
            Bytes::from_static(b"payload"),
        )
        .await;
        let response = handle_s3_abort_multipart(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            "aborted".into(),
            upload_id.clone(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(st.s3.head_object(1, "store", "bucket", "aborted").is_err());
        // The upload cannot be continued or aborted twice.
        let response = handle_s3_upload_part(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            "aborted".into(),
            upload_id.clone(),
            2,
            Bytes::from_static(b"late"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = handle_s3_abort_multipart(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            "aborted".into(),
            upload_id,
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn gateway_multipart_refuses_a_bad_completion_document() {
        let (state, dir) = s3_multipart_state("s3-multipart-malformed");
        let _dir = TempDirGuard(dir);
        let st = state;
        let response = handle_s3_create_multipart(
            st.clone(),
            HeaderMap::new(),
            "proj".into(),
            "store".into(),
            "bucket".into(),
            "key".into(),
        )
        .await;
        let created: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).unwrap();
        let upload_id = created["uploadId"].as_str().unwrap().to_string();
        for (document, code) in [
            ("<not-a-completion/>".to_string(), "MalformedXML"),
            (
                "<CompleteMultipartUpload></CompleteMultipartUpload>".to_string(),
                "MalformedXML",
            ),
            (
                "<CompleteMultipartUpload><Part><PartNumber>x</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>".to_string(),
                "MalformedXML",
            ),
        ] {
            let response = handle_s3_complete_multipart(
                st.clone(),
                HeaderMap::new(),
                "proj".into(),
                "store".into(),
                "bucket".into(),
                "key".into(),
                upload_id.clone(),
                Bytes::from(document.clone()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{document}");
            let body = String::from_utf8(body_bytes(response).await).unwrap();
            assert!(body.contains(&format!("\"code\":\"{code}\"")), "{body}");
        }
    }

    fn completion_xml(parts: &[(u32, String)]) -> String {
        let mut out = String::from("<CompleteMultipartUpload>");
        for (number, etag) in parts {
            out.push_str(&format!(
                "<Part><PartNumber>{number}</PartNumber><ETag>&quot;{etag}&quot;</ETag></Part>"
            ));
        }
        out.push_str("</CompleteMultipartUpload>");
        out
    }

    #[test]
    fn completion_parser_reads_the_part_list_and_rejects_what_it_cannot_read() {
        let parsed = parse_complete_multipart(
            br#"<?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part>
              <Part><PartNumber>2</PartNumber><ETag>&quot;def&quot;</ETag></Part>
            </CompleteMultipartUpload>"#,
        )
        .unwrap();
        assert_eq!(
            parsed,
            vec![(1, "\"abc\"".to_string()), (2, "\"def\"".to_string())]
        );
        // A body that is not a completion document, a part without an ETag, an
        // empty list and a non-numeric number are all refused, never guessed.
        for body in [
            &b"<Delete/>"[..],
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part>",
        ] {
            assert!(
                matches!(
                    parse_complete_multipart(body),
                    Err(S3Error::MalformedMultipart(_))
                ),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
        assert!(matches!(
            parse_complete_multipart(b"\xff\xfe"),
            Err(S3Error::MalformedMultipart(_))
        ));
    }

    #[test]
    fn element_text_skips_markup_that_is_not_content() {
        // A comment or processing instruction is not character data in XML, so
        // it neither splits nor contributes to the value: `1<!--x-->2` is `12`.
        let parsed = parse_complete_multipart(
            b"<CompleteMultipartUpload><Part><PartNumber>1<!--x-->2</PartNumber><ETag>\"ab<?pi?>cd\"</ETag></Part></CompleteMultipartUpload>",
        )
        .unwrap();
        assert_eq!(parsed, vec![(12, "\"abcd\"".to_string())]);
        // A byte-order mark is an encoding signature, not content.
        let with_bom = parse_complete_multipart(
            "\u{feff}<?xml version=\"1.0\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>".as_bytes(),
        )
        .unwrap();
        assert_eq!(with_bom, vec![(1, "\"a\"".to_string())]);
        // A nested element is not text, and an empty number is not a number.
        for body in [
            &b"<CompleteMultipartUpload><Part><PartNumber><a>1</a></PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>"[..],
            b"<CompleteMultipartUpload><Part><PartNumber></PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
        ] {
            assert!(
                matches!(
                    parse_complete_multipart(body),
                    Err(S3Error::MalformedMultipart(_))
                ),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn completion_parser_consumes_the_whole_document() {
        // Valid XML with whitespace before `>` is the same element; the old
        // substring scan silently skipped it and completed a shorter object.
        let parsed = parse_complete_multipart(
            b"<CompleteMultipartUpload><Part ><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
        )
        .unwrap();
        assert_eq!(parsed, vec![(1, "\"a\"".to_string())]);

        // A comment is one token, not text to scan for `<Part>`.
        let parsed = parse_complete_multipart(
            b"<CompleteMultipartUpload><!-- <Part><PartNumber>9</PartNumber><ETag>\"x\"</ETag></Part> --><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
        )
        .unwrap();
        assert_eq!(parsed, vec![(1, "\"a\"".to_string())]);

        for body in [
            // Trailing content after the root must not be ignored.
            &b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload><Part/>"[..],
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload><CompleteMultipartUpload/>",
            // An unknown element inside the root is not silently dropped.
            b"<CompleteMultipartUpload><Extra/><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // A duplicate element would otherwise let the last one win.
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><PartNumber>2</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // An attribute whose quote never closes must not end the tag at the
            // `>` inside it: that read the rest as a real `<Part>` and could
            // publish an object from a malformed document.
            b"<CompleteMultipartUpload><Part ignored=\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // Unterminated or unquoted attribute values are malformed too.
            b"<CompleteMultipartUpload><Part x=1><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part x=\"a><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part =\"a\"><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>",
            // Attributes must be separated by whitespace, may not repeat, and
            // their values may not carry an undeclared reference. Each body
            // still names a real part, so accepting it would publish.
            b"<CompleteMultipartUpload><Part a=\"1\"b=\"2\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part a=\"1\" a=\"2\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part a=\"&undeclared;\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part a=\"&amp\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // A closing tag may not carry attributes, and `</ Part>` is not a
            // closing tag: XML does not allow whitespace after `</`.
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part x=\"1\"></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></ Part></CompleteMultipartUpload>",
            // `--` inside a comment is forbidden, and a malformed comment must
            // not be skipped as if it were valid.
            b"<CompleteMultipartUpload><!-- bad -- comment --><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><!-- ends with dash ---><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // A declaration belongs before the root, never inside it.
            b"<CompleteMultipartUpload><?xml version=\"1.0\"?><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // `xml` is reserved: a processing instruction may not use it.
            b"<CompleteMultipartUpload><?XML version=\"1.0\"?><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // Only one declaration, and only before the root.
            b"<?xml version=\"1.0\"?><?xml version=\"1.0\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<?xml version=\"1.0\"?><CompleteMultipartUpload><?xml version=\"1.0\"?><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // The declaration must be the very first thing in the document: a
            // comment or whitespace before it is malformed, not something to
            // skip past.
            b"<!--c--><?xml version=\"1.0\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b" <?xml version=\"1.0\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // A declaration has a grammar: a version is required and XML 1.0 is
            // the only version this server speaks.
            b"<?xml?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<?xml version=\"2.0\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<?xml encoding=\"UTF-8\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<?xml version=\"1.0\" standalone=\"maybe\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<?xml version=\"1.0\" junk?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // A processing-instruction target is a Name: `<?1?>` has none.
            b"<?1?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // Character references name a permitted XML character. `&#0;` is not
            // one, a surrogate and an out-of-range code point are not characters,
            // and the hexadecimal marker is lowercase `x` only.
            b"<CompleteMultipartUpload><Part a=\"&#0;\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part a=\"&#xD800;\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part a=\"&#x110000;\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part a=\"&#X41;\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part a=\"&#99999999999999999999;\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // A raw control character is not an XML `Char` anywhere.
            b"<CompleteMultipartUpload><Part a=\"\x01\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // `]]>` may not appear in character data.
            b"<CompleteMultipartUpload>]]><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // XML whitespace is space/tab/CR/LF: a no-break space is not a
            // separator, neither between attributes nor around `=`.
            "<CompleteMultipartUpload><Part a=\"1\"\u{a0}b=\"2\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>".as_bytes(),
            "<CompleteMultipartUpload><Part a=\u{a0}\"1\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>".as_bytes(),
            "<CompleteMultipartUpload><Part\u{a0}a=\"1\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>".as_bytes(),
            // The empty-element slash is adjacent to `>`; a space between them
            // is malformed, not a self-closing tag with a stray slash.
            b"<CompleteMultipartUpload><Part a=\"1\" / ><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part a=\"1\"/ ><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part/ ><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
        ] {
            assert!(
                matches!(
                    parse_complete_multipart(body),
                    Err(S3Error::MalformedMultipart(_))
                ),
                "{}",
                String::from_utf8_lossy(body)
            );
        }

        // Well-formed documents are still accepted: the namespace S3 clients
        // send, a legal declaration at the start, a valid entity reference and a
        // character reference in an attribute value, and a comment between the
        // root and its parts.
        for body in [
            &b"<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\" note='ok'><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>"[..],
            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            // A non-reserved processing instruction is legal before the root.
            b"<?xml-stylesheet type=\"text/xsl\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><!-- a comment --><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part a=\"&amp;&#65;&#x41;\"><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>",
            b"<CompleteMultipartUpload><Part\n a=\"1\"\t b='2' ><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part ></CompleteMultipartUpload>",
        ] {
            let parsed = parse_complete_multipart(body).unwrap_or_else(|e| {
                panic!("{}: {e:?}", String::from_utf8_lossy(body))
            });
            assert_eq!(parsed, vec![(1, "\"a\"".to_string())]);
        }
    }

    #[test]
    fn multipart_initiation_refuses_options_a_single_put_refuses() {
        let mut headers = HeaderMap::new();
        assert_eq!(unsupported_object_options(&headers), None);
        for (name, value) in [
            ("x-amz-server-side-encryption", "AES256"),
            ("x-amz-acl", "public-read"),
            ("x-amz-meta-owner", "billing"),
        ] {
            let mut probe = HeaderMap::new();
            probe.insert(name, value.parse().unwrap());
            assert!(
                unsupported_object_options(&probe).is_some(),
                "{name} must not be silently accepted at initiation"
            );
        }
    }

    // A restarted node accepts connections before the control watcher has
    // installed the persisted image. Authorization must classify that window as
    // "not ready" (retryable) rather than "unknown project" (403, which S3
    // clients treat as final), and must keep the old denial/not-found answers
    // once an image is installed.
    #[tokio::test]
    async fn an_unapplied_control_image_is_reported_as_not_ready() {
        let (state, dir) = legacy_fragment_state_with_dir("startup-window");
        let _dir = TempDirGuard(dir);
        let mut st = state;
        st.control.write().unwrap().object_stores.insert(
            "proj/store".into(),
            yadb_control::ObjectStoreSpec {
                pool: "data".into(),
                isolation: Default::default(),
                buckets: vec![],
            },
        );
        let get = |st: AppState, subject: &'static str| async move {
            handle_s3_get(
                Method::GET,
                State(st),
                headers_with(&[("x-yadb-subject", subject)]),
                Path((
                    "proj".to_string(),
                    "store".to_string(),
                    "bucket".to_string(),
                    "missing".to_string(),
                )),
                Query(S3GetQuery { consistent: false }),
            )
            .await
        };

        // A configured cluster keeps its historical answers: a member reaches
        // object lookup, a stranger is denied.
        assert_eq!(
            get(st.clone(), "anonymous").await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            get(st.clone(), "stranger").await.status(),
            StatusCode::FORBIDDEN
        );

        // A node without an installed image has not learned the project yet. It
        // must not turn that into a denial the client will not retry.
        st.control.write().unwrap().generation = 0;
        let response = get(st, "anonymous").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = String::from_utf8(body_bytes(response).await).unwrap();
        assert!(body.contains(CONTROL_NOT_READY), "{body}");
    }

    // The dedicated endpoint is the explicit-error surface: a refusal is a 409
    // with the reason in the body, never a 200 that looks like "nothing to do".
    #[tokio::test]
    async fn gc_endpoint_reports_a_refusal_as_conflict() {
        let st = legacy_fragment_state("gc-endpoint");
        st.block
            .write_at(1, "disk", "data", 1 << 20, 0, b"payload")
            .unwrap();

        let ok = handle_pools_gc(State(st.clone()), HeaderMap::new()).await;
        assert_eq!(ok.status(), StatusCode::OK);
        let outcome: crate::gc::SweepOutcome =
            serde_json::from_slice(&body_bytes(ok).await).unwrap();
        assert!(outcome.refusal.is_none(), "{outcome:?}");

        let held = yadb_metakv::gc::acquire_collector(&st.meta, 0, 60_000).unwrap();
        let refused = handle_pools_gc(State(st.clone()), HeaderMap::new()).await;
        assert_eq!(refused.status(), StatusCode::CONFLICT);
        let outcome: crate::gc::SweepOutcome =
            serde_json::from_slice(&body_bytes(refused).await).unwrap();
        let reason = outcome.refusal.expect("a 409 must carry its reason");
        assert!(reason.contains("collector lease"), "{reason}");
        assert_eq!(outcome.fragments_deleted, 0);
        held.release().unwrap();
    }

    // A conditional read must follow RFC 9110, not a byte comparison of the
    // whole header value: `*`, lists and weak tags are all valid inputs, and
    // serving bytes for one of them would violate the condition the caller sent.
    #[test]
    fn conditional_reads_follow_rfc_9110_matching() {
        let etag = "\"abc-11\"";
        let check = |pairs: &[(&str, &str)]| evaluate_conditions(&headers_with(pairs), etag);
        let failed = |pairs: &[(&str, &str)]| matches!(check(pairs), Conditional::Failed);
        let not_modified =
            |pairs: &[(&str, &str)]| matches!(check(pairs), Conditional::NotModified);
        let passed = |pairs: &[(&str, &str)]| matches!(check(pairs), Conditional::Passed);

        // If-Match: exact version, `*`, and a list containing the version.
        assert!(passed(&[("if-match", "\"abc-11\"")]));
        assert!(passed(&[("if-match", "*")]));
        assert!(passed(&[("if-match", "\"other\", \"abc-11\"")]));
        assert!(failed(&[("if-match", "\"other\"")]));
        assert!(failed(&[("if-match", "\"other\", \"third\"")]));
        // Strong comparison: a weak tag never satisfies If-Match.
        assert!(failed(&[("if-match", "W/\"abc-11\"")]));
        // A comma inside the opaque tag is part of the tag, not a separator.
        assert!(matches!(
            evaluate_conditions(&headers_with(&[("if-match", "\"a,b\"")]), "\"a,b\""),
            Conditional::Passed
        ));
        assert!(matches!(
            evaluate_conditions(&headers_with(&[("if-match", "\"a,b\"")]), "\"a\""),
            Conditional::Failed
        ));

        // If-None-Match: exact version, `*`, a list, and weak comparison.
        assert!(not_modified(&[("if-none-match", "\"abc-11\"")]));
        assert!(not_modified(&[("if-none-match", "*")]));
        assert!(not_modified(&[("if-none-match", "\"other\", \"abc-11\"")]));
        assert!(not_modified(&[("if-none-match", "W/\"abc-11\"")]));
        assert!(passed(&[("if-none-match", "\"other\"")]));
        assert!(passed(&[("if-none-match", "\"other\", \"third\"")]));

        // Empty list elements are ignored, as RFC 9110 requires, so a trailing
        // or doubled comma keeps the rest of the condition.
        assert!(passed(&[("if-match", "\"abc-11\",")]));
        assert!(failed(&[("if-match", "\"other\",")]));
        assert!(failed(&[("if-match", ", \"other\"")]));
        assert!(not_modified(&[("if-none-match", "\"abc-11\",")]));
        assert!(not_modified(&[(
            "if-none-match",
            "\"other\", , \"abc-11\""
        )]));

        // A field value that cannot be read fails `If-Match` rather than
        // turning into an unconditional read, because that condition is true
        // only on a match. `If-None-Match` is false only on a match, so a value
        // that names no readable version does not suppress the bytes.
        assert!(failed(&[("if-match", "not a tag")]));
        assert!(failed(&[("if-match", "\"unterminated")]));
        assert!(failed(&[("if-match", ",")]));
        assert!(passed(&[("if-none-match", "not a tag")]));
        assert!(passed(&[("if-none-match", "")]));

        // A tag with `obs-text` is a valid entity-tag that simply does not
        // match a stored ASCII one; it must not be dropped as unreadable.
        let mut obs_text = HeaderMap::new();
        obs_text.insert(
            header::IF_MATCH,
            HeaderValue::from_bytes(b"\"other\x80\"").unwrap(),
        );
        assert!(matches!(
            evaluate_conditions(&obs_text, etag),
            Conditional::Failed
        ));

        // A stored ETag that is not a well-formed entity-tag is never repaired
        // into a match.
        assert!(matches!(
            evaluate_conditions(&headers_with(&[("if-match", "\"abc-11\"")]), "abc-11"),
            Conditional::Failed
        ));
        assert!(matches!(
            evaluate_conditions(&headers_with(&[("if-none-match", "\"abc-11\"")]), "abc-11"),
            Conditional::Passed
        ));

        // A list header may arrive as several field lines; a match on any of
        // them satisfies the condition.
        let mut repeated = headers_with(&[("if-match", "\"other\"")]);
        repeated.append(header::IF_MATCH, HeaderValue::from_static("\"abc-11\""));
        assert!(matches!(
            evaluate_conditions(&repeated, etag),
            Conditional::Passed
        ));

        // No conditional headers at all.
        assert!(passed(&[]));
    }

    fn vdisk_fence_test_state(label: &str) -> (std::path::PathBuf, AppState) {
        let dir = std::env::temp_dir().join(format!(
            "yadb-vdisk-fence-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let fs = OsFs::new(&dir).unwrap();
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let st = AppState {
            openraft: None,
            listen_addr: "127.0.0.1:0".into(),
            fs,
            meta: meta.clone(),
            raft_token: None,
            subject_secret: None,
            admin_subjects: vec![],
            control: Arc::new(RwLock::new(ControlPlaneState::new())),
            control_refresh: Arc::new(std::sync::Mutex::new(())),
            control_freshness: Arc::new(std::sync::Mutex::new(ControlFreshness::default())),
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            local_vdisks: Arc::new(RwLock::new(BTreeMap::new())),
            last_scrub: Arc::new(RwLock::new(None)),
            reader_lease: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            s3: ObjectStoreEngine::new(meta.clone(), 4096),
            db: DatabaseEngine::new(meta.clone(), 100),
            block: BlockVolumeEngine::new(meta, DEFAULT_BLOCK_SIZE),
        };
        st.open_or_get_local_vdisk("pool", 0).unwrap();
        (dir, st)
    }

    async fn response_json(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    async fn response_text(response: Response) -> (StatusCode, String) {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn vdisk_fence_rpc_drives_the_durable_epoch_and_pin_table() {
        let (dir, st) = vdisk_fence_test_state("rpc");
        let slot_path = || Path(("pool".to_string(), 0));
        let headers = || HeaderMap::new();
        let blob_path = |id: BlobId| Path(("pool".to_string(), 0, id.to_hex()));
        let delete_query = |fence_epoch, collector_epoch| {
            Query(VdiskDeleteQuery {
                fence_epoch,
                collector_epoch,
            })
        };

        // A fresh slot starts at epoch 0 and reports it over the wire.
        let (status, body) =
            response_json(handle_vdisk_epoch(State(st.clone()), slot_path(), headers()).await)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["epoch"], 0);

        // A pin must name the target's current epoch.
        let (status, body) = response_json(
            handle_vdisk_pin(
                State(st.clone()),
                slot_path(),
                headers(),
                Json(VdiskPinBody {
                    epoch: 1,
                    ttl_ms: 1_000,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "pin_epoch_not_current");

        let (status, body) = response_json(
            handle_vdisk_pin(
                State(st.clone()),
                slot_path(),
                headers(),
                Json(VdiskPinBody {
                    epoch: 0,
                    ttl_ms: 0,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "pin_ttl_zero");

        let (status, body) = response_json(
            handle_vdisk_pin(
                State(st.clone()),
                slot_path(),
                headers(),
                Json(VdiskPinBody {
                    epoch: 0,
                    ttl_ms: yadb_blob::MAX_PIN_TTL_MS + 1,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "pin_ttl_too_long");

        // A valid pin is durable enough to refuse an epoch advance, and
        // `GET /pins` reports exactly that pin.
        let (status, body) = response_json(
            handle_vdisk_pin(
                State(st.clone()),
                slot_path(),
                headers(),
                Json(VdiskPinBody {
                    epoch: 0,
                    ttl_ms: 60_000,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["epoch"], 0);
        let pin_id = body["pin_id"].as_u64().expect("pin id");
        let expires_at_ms = body["expires_at_ms"].as_u64().expect("pin deadline");

        // Written while the slot is still at epoch 0, so it is the fragment the
        // collectable path may reclaim after the advance.
        let old_id = BlobId::new(11, 12, 13, 0, 14, 0);
        let store = st
            .local_vdisks
            .read()
            .unwrap()
            .get(&("pool".to_string(), 0))
            .unwrap()
            .clone();
        store.put(old_id, b"epoch-zero fragment").unwrap();

        let (status, body) = response_json(
            handle_vdisk_advance(
                State(st.clone()),
                slot_path(),
                headers(),
                Json(VdiskAdvanceBody { expected_epoch: 0 }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "epoch_pinned");

        let (status, body) =
            response_json(handle_vdisk_pins(State(st.clone()), slot_path(), headers()).await).await;
        assert_eq!(status, StatusCode::OK);
        let pins = body["pins"].as_array().expect("pins array");
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[0]["pin_id"], pin_id);
        assert_eq!(pins[0]["epoch"], 0);
        assert_eq!(pins[0]["expires_at_ms"], expires_at_ms);

        // Releasing the pin unblocks the advance.
        let (status, body) = response_json(
            handle_vdisk_unpin(
                State(st.clone()),
                slot_path(),
                headers(),
                Json(VdiskUnpinBody { pin_id }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["released"], true);

        let (status, body) = response_json(
            handle_vdisk_advance(
                State(st.clone()),
                slot_path(),
                headers(),
                Json(VdiskAdvanceBody { expected_epoch: 0 }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["epoch"], 1);

        // The epoch-0 fragment is older than the collector's epoch 1.
        let (status, text) = response_text(
            handle_vdisk_delete(
                State(st.clone()),
                blob_path(old_id),
                delete_query(None, Some(1)),
                headers(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(text, "deleted");

        // A fragment written at epoch 1 is not deletable under the old fence,
        // but its own epoch deletes it.
        let new_id = BlobId::new(11, 12, 13, 1, 14, 0);
        store.put(new_id, b"epoch-one fragment").unwrap();

        let (status, body) = response_json(
            handle_vdisk_delete(
                State(st.clone()),
                blob_path(new_id),
                delete_query(Some(0), None),
                headers(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "fenced_by_epoch");

        let (status, text) = response_text(
            handle_vdisk_delete(
                State(st.clone()),
                blob_path(new_id),
                delete_query(Some(1), None),
                headers(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(text, "deleted");

        // The two fences are different rules and may not be combined.
        let (status, body) = response_json(
            handle_vdisk_delete(
                State(st.clone()),
                blob_path(new_id),
                delete_query(Some(1), Some(1)),
                headers(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "bad_fence_params");

        // Fenced endpoints answer 404 with the JSON envelope.
        let (status, body) = response_json(
            handle_vdisk_epoch(State(st.clone()), Path(("pool".to_string(), 1)), headers()).await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "slot_not_found");

        // A fenced delete names a fragment, so bad hex is a request error.
        let (status, body) = response_json(
            handle_vdisk_delete(
                State(st.clone()),
                Path(("pool".to_string(), 0, "not-hex".to_string())),
                delete_query(Some(1), None),
                headers(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "bad_blob_id");

        drop(st);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn vdisk_fence_rpc_requires_peer_token() {
        let (dir, mut st) = vdisk_fence_test_state("auth");
        st.raft_token = Some("secret".into());
        let slot_path = || Path(("pool".to_string(), 0));
        let blob_path = || {
            Path((
                "pool".to_string(),
                0,
                BlobId::new(21, 22, 23, 1, 24, 0).to_hex(),
            ))
        };

        for headers in [
            HeaderMap::new(),
            headers_with(&[("x-yadb-raft-token", "wrong")]),
        ] {
            let responses = vec![
                handle_vdisk_epoch(State(st.clone()), slot_path(), headers.clone()).await,
                handle_vdisk_pins(State(st.clone()), slot_path(), headers.clone()).await,
                handle_vdisk_pin(
                    State(st.clone()),
                    slot_path(),
                    headers.clone(),
                    Json(VdiskPinBody {
                        epoch: 0,
                        ttl_ms: 1_000,
                    }),
                )
                .await,
                handle_vdisk_unpin(
                    State(st.clone()),
                    slot_path(),
                    headers.clone(),
                    Json(VdiskUnpinBody { pin_id: 1 }),
                )
                .await,
                handle_vdisk_advance(
                    State(st.clone()),
                    slot_path(),
                    headers.clone(),
                    Json(VdiskAdvanceBody { expected_epoch: 0 }),
                )
                .await,
                handle_vdisk_delete(
                    State(st.clone()),
                    blob_path(),
                    Query(VdiskDeleteQuery {
                        fence_epoch: Some(1),
                        collector_epoch: None,
                    }),
                    headers.clone(),
                )
                .await,
                // The legacy unfenced delete keeps the same peer check.
                handle_vdisk_delete(
                    State(st.clone()),
                    blob_path(),
                    Query(VdiskDeleteQuery::default()),
                    headers.clone(),
                )
                .await,
            ];
            for response in responses {
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            }
        }

        // The configured token still reaches the store.
        let response = handle_vdisk_epoch(
            State(st.clone()),
            slot_path(),
            headers_with(&[("x-yadb-raft-token", "secret")]),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        drop(st);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

/// `/raft/status` must answer from the control group in OpenRaft mode. This
/// runs a real single-node OpenRaft group so the response is the same one the
/// daemon serves, not a hand-built fixture.
#[cfg(test)]
mod raft_status_tests {
    use super::*;
    use ::openraft::error::{
        Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable,
    };
    use ::openraft::network::RPCOption;
    use ::openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    };
    use ::openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, Snapshot, Vote};
    use std::sync::Arc;
    use std::time::Duration;
    use yadb_metakv::multiraft::{GroupIdentity, ReplicaIdentity};
    use yadb_metakv::openraft_runtime::{YadbRaft, YadbRaftTypeConfig as C};
    use yadb_metakv::openraft_state_machine::DurableMetaStateMachine;
    use yadb_metakv::openraft_storage::DurableOpenRaftLog;

    /// A single-node group never calls its network.
    struct NoNetwork;
    impl RaftNetworkFactory<C> for NoNetwork {
        type Network = NoNetwork;
        async fn new_client(&mut self, _: u32, _: &BasicNode) -> NoNetwork {
            NoNetwork
        }
    }
    impl RaftNetwork<C> for NoNetwork {
        async fn append_entries(
            &mut self,
            _: AppendEntriesRequest<C>,
            _: RPCOption,
        ) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
            Err(Unreachable::new(&std::io::Error::other("singleton")).into())
        }
        async fn vote(
            &mut self,
            _: VoteRequest<u32>,
            _: RPCOption,
        ) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
            Err(Unreachable::new(&std::io::Error::other("singleton")).into())
        }
        async fn full_snapshot(
            &mut self,
            _: Vote<u32>,
            _: Snapshot<C>,
            _: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
            _: RPCOption,
        ) -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
            Err(Unreachable::new(&std::io::Error::other("singleton")).into())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raft_status_reports_the_control_group_in_openraft_mode() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("yadb-raft-status-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let identity = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [91; 16],
                group: 1,
                incarnation: 1,
            },
            1,
            vec![1],
        )
        .unwrap();
        let log = DurableOpenRaftLog::create(dir.join("log"), &identity).unwrap();
        let state = DurableMetaStateMachine::create(dir.join("state"), &identity).unwrap();
        let config = Arc::new(::openraft::Config::default().validate().unwrap());
        let raft = YadbRaft::new(1, config, NoNetwork, log, state.clone())
            .await
            .unwrap();
        raft.initialize(BTreeMap::from([(1, BasicNode::new("one"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        let meta = LocalMetaKv::from_openraft(
            raft.clone(),
            state,
            BTreeMap::from([(1, "127.0.0.1:1".to_string())]),
            tokio::runtime::Handle::current(),
        );
        let fs = OsFs::new(&dir).unwrap();
        let st = AppState {
            openraft: None,
            listen_addr: "127.0.0.1:0".into(),
            fs,
            meta: meta.clone(),
            raft_token: Some("secret".into()),
            subject_secret: None,
            admin_subjects: vec![],
            control: Arc::new(RwLock::new(ControlPlaneState::new())),
            control_refresh: Arc::new(std::sync::Mutex::new(())),
            control_freshness: Arc::new(std::sync::Mutex::new(ControlFreshness::default())),
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            local_vdisks: Arc::new(RwLock::new(BTreeMap::new())),
            last_scrub: Arc::new(RwLock::new(None)),
            reader_lease: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            s3: ObjectStoreEngine::new(meta.clone(), 4096),
            db: DatabaseEngine::new(meta.clone(), 100),
            block: BlockVolumeEngine::new(meta.clone(), DEFAULT_BLOCK_SIZE),
        };

        let response = handle_raft_status(State(st)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        println!("{}", String::from_utf8_lossy(&body));

        assert_eq!(json["metadata_runtime"], "openraft");
        assert_eq!(json["production_ready"], false);
        assert_eq!(json["node_id"], 1);
        assert!(json["role"].is_string());
        assert!(json["term"].is_u64());
        assert!(json["leader_id"].is_u64());
        assert!(json["commit_index"].is_u64());
        // Exactly the documented keys: no compaction or WAL bookkeeping that
        // this runtime cannot answer honestly.
        assert_eq!(
            json.as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                "commit_index",
                "leader_id",
                "metadata_runtime",
                "node_id",
                "production_ready",
                "role",
                "term"
            ]
        );

        raft.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// One single-node metadata group on node 1, with its own durable log and
    /// state machine.
    async fn singleton_group(
        dir: &std::path::Path,
        group: u64,
    ) -> (YadbRaft, DurableMetaStateMachine) {
        let identity = ReplicaIdentity::new(
            GroupIdentity {
                cluster: [92; 16],
                group,
                incarnation: 1,
            },
            1,
            vec![1],
        )
        .unwrap();
        let log = DurableOpenRaftLog::create(dir.join(format!("g{group}-log")), &identity).unwrap();
        let state = DurableMetaStateMachine::create(dir.join(format!("g{group}-state")), &identity)
            .unwrap();
        let config = Arc::new(::openraft::Config::default().validate().unwrap());
        let raft = YadbRaft::new(1, config, NoNetwork, log, state.clone())
            .await
            .unwrap();
        raft.initialize(BTreeMap::from([(1, BasicNode::new("one"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        (raft, state)
    }

    /// Control group 1 with data groups 2 and 3, and projects 1 and 2 routed to
    /// them. Every group is a single-node cluster on node 1, so this node leads
    /// both data groups and can publish a reader lease in either.
    async fn multigroup_state(tag: &str) -> (AppState, std::path::PathBuf, Vec<YadbRaft>) {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("yadb-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (control, control_state) = singleton_group(&dir, 1).await;
        let (group2, group2_state) = singleton_group(&dir, 2).await;
        let (group3, group3_state) = singleton_group(&dir, 3).await;
        let meta: LocalMetaKv<OsFs> = LocalMetaKv::from_openraft_groups(
            1,
            BTreeMap::from([
                (1, (control.clone(), control_state)),
                (2, (group2.clone(), group2_state)),
                (3, (group3.clone(), group3_state)),
            ]),
            BTreeMap::from([(1, 2), (2, 3)]),
            BTreeMap::new(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let fs = OsFs::new(&dir).unwrap();
        let state = AppState {
            openraft: None,
            listen_addr: "127.0.0.1:0".into(),
            fs,
            meta: meta.clone(),
            raft_token: None,
            subject_secret: None,
            admin_subjects: vec![],
            control: Arc::new(RwLock::new(ControlPlaneState::new())),
            control_refresh: Arc::new(std::sync::Mutex::new(())),
            control_freshness: Arc::new(std::sync::Mutex::new(ControlFreshness::default())),
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            local_vdisks: Arc::new(RwLock::new(BTreeMap::new())),
            last_scrub: Arc::new(RwLock::new(None)),
            reader_lease: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            s3: ObjectStoreEngine::new(meta.clone(), 4096),
            db: DatabaseEngine::new(meta.clone(), 100),
            block: BlockVolumeEngine::new(meta.clone(), DEFAULT_BLOCK_SIZE),
        };
        (state, dir, vec![control, group2, group3])
    }

    /// A reader lease is published in the group that owns the project, carries
    /// that group's applied index, and is cached per group.
    ///
    /// The cache is the point: a node serving projects in two groups must not
    /// reuse one record for the other, because the collector of a group only
    /// consults its own records. One slot for the whole node would let a read
    /// of project 1 be fenced by a record group 2 never sees.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_leases_are_published_and_cached_per_metadata_group() {
        let (st, dir, rafts) = multigroup_state("reader-lease-groups").await;

        // Project 1 is routed to group 2, project 2 to group 3.
        let applied_before = st.meta.role_info_for_group(2).unwrap().4;
        let group2_lease = st.begin_read_lease(1).unwrap();
        assert_eq!(
            group2_lease.group,
            Some(2),
            "a lease must name the group that owns the project's manifests"
        );
        assert_eq!(
            group2_lease.revision, applied_before,
            "a group-scoped lease carries the group's applied index, not a \
             deployment-wide revision that does not exist"
        );
        st.end_read_lease(&group2_lease).unwrap();

        let group3_lease = st.begin_read_lease(2).unwrap();
        assert_eq!(group3_lease.group, Some(3));
        st.end_read_lease(&group3_lease).unwrap();

        assert_eq!(
            st.reader_lease.lock().unwrap().len(),
            2,
            "one cached record per group, not one for the whole node"
        );

        // A second read of project 1 renews the group-2 record instead of
        // publishing another one, and leaves the group-3 record alone.
        let renewed = st.begin_read_lease(1).unwrap();
        assert_eq!(
            renewed.id, group2_lease.id,
            "a cached record must be renewed, not replaced"
        );
        assert_eq!(renewed.group, Some(2));
        st.end_read_lease(&renewed).unwrap();
        assert_eq!(
            yadb_metakv::gc::live_leases_in(&st.meta, 2).unwrap().len(),
            1
        );
        assert_eq!(
            yadb_metakv::gc::live_leases_in(&st.meta, 3).unwrap().len(),
            1
        );

        // The collector of each group consults those records: both groups
        // refuse while their lease is live, and delete nothing.
        let pools: BTreeMap<String, Arc<PoolStriper<OsFs>>> = BTreeMap::new();
        let control = st.control.read().unwrap().clone();
        let outcome = crate::gc::sweep(&st.meta, &pools, &control);
        assert_eq!(outcome.leases_live, 2, "{outcome:?}");
        assert_eq!(outcome.fragments_deleted, 0, "{outcome:?}");
        let refusal = outcome.refusal.expect("a live lease must refuse the pass");
        assert!(refusal.contains("group 2 refused"), "{refusal}");
        assert!(refusal.contains("group 3 refused"), "{refusal}");
        assert!(refusal.contains("live lease(s)"), "{refusal}");

        // Dropping one group's record frees that group only. This is the
        // inversion: the refusal follows the record, per group, and is not a
        // node-wide "some read happened" flag.
        let released = st.reader_lease.lock().unwrap().remove(&Some(2)).unwrap();
        yadb_metakv::gc::release(&st.meta, &released).unwrap();
        let outcome = crate::gc::sweep(&st.meta, &pools, &control);
        assert_eq!(outcome.leases_live, 1, "{outcome:?}");
        let refusal = outcome.refusal.expect("group 3 still holds a lease");
        assert!(refusal.contains("group 3 refused"), "{refusal}");
        assert!(
            !refusal.contains("group 2 refused"),
            "group 2 has no live record, so its pass must not refuse: {refusal}"
        );

        drop(st);
        for raft in rafts {
            let _ = raft.shutdown().await;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
