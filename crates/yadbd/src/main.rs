use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use clap::Parser;
use serde::{Deserialize, Serialize};
use yadb_blob::{
    BarrierRequest, BarrierResponse, BlobError, BlobId, PoolHealthSnapshot, PoolSlot, PoolStriper,
    VolumeConfig, VolumeStore,
};
use yadb_block::{BlockError, BlockVolumeEngine, VolumeExtentMap, DEFAULT_BLOCK_SIZE};
use yadb_control::{parse_manifests, ControlError, ControlPlaneState, DiffOp, Role};
use yadb_db::{parse_sql_script, script_has_writes, DatabaseEngine, DbError, TabletManifest};
use yadb_io::OsFs;
use yadb_metakv::{KvCommand, LocalMetaKv, MetaError, RaftRpc, RoleState, WatchEvent};
use yadb_s3::{ObjectManifest, ObjectStoreEngine, S3Error};

#[cfg(feature = "experimental-openraft-storage")]
mod experimental_openraft;
mod protocols;

const CONTROL_STATE_KEY: &str = "/sys/control_plane_state";
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

    /// Metadata backend; openraft is only for new experimental clusters.
    #[arg(long, default_value = "legacy", value_parser = ["legacy", "openraft"])]
    metadata_runtime: String,

    #[arg(long, default_value_t = 1)]
    node_id: u32,

    /// Optional comma-separated Raft peer map: `1=127.0.0.1:8421,2=127.0.0.1:8422,3=127.0.0.1:8423`
    #[arg(long)]
    peers: Option<String>,

    /// Shared secret required by the internal Raft endpoints (`/raft/rpc`,
    /// `/raft/elect`). Required when `--peers` is set: an unauthenticated
    /// Raft endpoint lets any client inject Raft messages.
    #[arg(long, env = "YADB_RAFT_TOKEN", hide_env_values = true)]
    raft_token: Option<String>,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Experimental single OpenRaft group configuration; no client traffic or migration.
    #[arg(long)]
    experimental_openraft_config: Option<PathBuf>,

    /// S3 chunk size in bytes (default 1 MiB)
    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size: usize,

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

fn check_experimental_openraft_config(path: Option<&std::path::Path>) -> Result<(), &'static str> {
    if path.is_none() {
        return Ok(());
    }
    if !cfg!(feature = "experimental-openraft-storage") {
        return Err(
            "--experimental-openraft-config requires the experimental-openraft-storage feature",
        );
    }
    Ok(())
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

#[derive(Debug)]
struct ControlAuthError;

impl std::fmt::Display for ControlAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "control-plane authorization state is stale or control quorum is unavailable; refusing access"
        )
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
    #[cfg(feature = "experimental-openraft-storage")]
    experimental_openraft: Option<experimental_openraft::Status>,
    listen_addr: String,
    fs: OsFs,
    meta: LocalMetaKv<OsFs>,
    /// Shared secret required by the internal Raft endpoints.
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
    s3: ObjectStoreEngine<OsFs>,
    db: DatabaseEngine<OsFs>,
    block: BlockVolumeEngine<OsFs>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ScrubSummary {
    pub pools_checked: usize,
    pub blobs_checked: usize,
    pub blobs_intact: usize,
    pub blobs_repaired: usize,
    pub fragments_repaired: usize,
    pub blobs_unrecoverable: usize,
    pub orphan_blobs_collected: usize,
    pub orphan_collection_error: Option<String>,
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
            return Err(ControlAuthError);
        }
        Ok(self.control.read().unwrap())
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
        if let Some(existing) = self.local_vdisks.read().unwrap().get(&key) {
            return Ok(existing.clone());
        }
        let dir = format!("pools/{pool_name}/slot_{slot:02}");
        let store = Arc::new(
            VolumeStore::open(self.fs.clone(), dir, VolumeConfig::default())
                .map_err(|e| e.to_string())?,
        );
        self.local_vdisks
            .write()
            .unwrap()
            .insert(key, store.clone());
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
            let mut slots = Vec::with_capacity(parts);

            for slot in 0..parts {
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

    /// Scans all live SQL Tablet manifests, S3 Object manifests, and BlockVolume extent maps in `MetaKV`,
    /// verifies every fragment across cluster VDisks, and heals missing or bit-rotted fragments!
    fn scrub_all_pools(&self) -> ScrubSummary {
        if self.meta.is_openraft() {
            // Global orphan scans are unsafe without per-group leader fencing.
            return ScrubSummary {
                orphan_collection_error: Some("scrub disabled for experimental Multi-Raft".into()),
                ..ScrubSummary::default()
            };
        }
        let mut live_blobs: BTreeSet<(String, BlobId)> = BTreeSet::new();

        // 1. SQL DataShard Tablets (`/db/{pid}/{db}/shards/{table}/{idx}`)
        for (k, kv) in self.meta.scan_prefix("/db/") {
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
        for (_k, kv) in self.meta.scan_prefix("/s3/") {
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
        for (k, kv) in self.meta.scan_prefix("/block/") {
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
        let summary = ScrubSummary {
            pools_checked: pools_health.len(),
            blobs_checked,
            blobs_intact,
            blobs_repaired,
            fragments_repaired,
            blobs_unrecoverable,
            orphan_blobs_collected,
            orphan_collection_error,
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
    if matches!(err, MetaError::OpenRaftWriteUnknown(_)) {
        return forward_error_response(err.to_string(), Some("unknown"));
    }
    match err {
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
    body: Vec<u8>,
}

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
) -> Result<ProxyResponse, std::io::Error> {
    let clean = peer_addr
        .trim()
        .strip_prefix("http://")
        .unwrap_or(peer_addr.trim())
        .trim_end_matches('/');
    let sock: SocketAddr = clean
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let mut stream = TcpStream::connect_timeout(&sock, Duration::from_millis(400))?;
    stream.set_read_timeout(Some(Duration::from_millis(5000)))?;
    stream.set_write_timeout(Some(Duration::from_millis(3000)))?;

    let signature_header = match subject_signature {
        Some(signature) => format!("x-yadb-subject-signature: {signature}\r\n"),
        None => String::new(),
    };
    let key_header = idempotency_key
        .map(|key| format!("Idempotency-Key: {key}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path_and_query} HTTP/1.1\r\nHost: {clean}\r\nx-yadb-subject: {subject}\r\n{signature_header}{key_header}x-yadb-forwarded: 1\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(req.as_bytes())?;
    if !body.is_empty() {
        stream.write_all(body)?;
    }
    stream.flush()?;

    let mut raw = Vec::new();
    stream
        .take((MAX_PROXY_WIRE + 1) as u64)
        .read_to_end(&mut raw)?;
    if raw.len() > MAX_PROXY_WIRE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "HTTP response too large",
        ));
    }

    parse_proxy_response(&raw)
}

fn parse_proxy_response(raw: &[u8]) -> Result<ProxyResponse, std::io::Error> {
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
            }
        } else {
            return Err(invalid());
        }
    }

    let payload_slice = &raw[hdr_end + 4..];
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
            )
            .map(|response| Some((target, response)))
            .map_err(|_| {
                (
                    "OpenRaft forwarding failed; write outcome unknown, do not automatically retry"
                        .to_string(),
                    Some("unknown"),
                )
            });
        }
        for attempt in 0..3 {
            let (my_id, cur_role, _, leader_opt, _) = meta.role_info();
            if cur_role == RoleState::Leader {
                return Ok(None);
            }
            let target_id = match leader_opt {
                Some(id) if id != my_id => id,
                _ => {
                    // No known leader yet: trigger election or wait briefly for peer election
                    let _ = meta.trigger_election();
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
            ) {
                Ok(ref response)
                    if response.code == 409
                        && String::from_utf8_lossy(&response.body).contains("not Raft leader") =>
                {
                    let _ = meta.trigger_election();
                    continue;
                }
                Ok(ok_tuple) => return Ok(Some((target_id, ok_tuple))),
                Err(_) => {
                    // Old leader just died! Immediately run failover election and retry!
                    let _ = meta.trigger_election();
                    let (_, new_role, _, _, _) = meta.role_info();
                    if new_role == RoleState::Leader {
                        return Ok(None);
                    }
                }
            }
        }
        Err((
            "cluster has no reachable Raft leader (quorum unavailable)".to_string(),
            None,
        ))
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

fn parse_range_header(val: &str, total_size: u64) -> Option<(u64, u64)> {
    let spec = val.trim().strip_prefix("bytes=")?;
    let (start_s, end_s) = spec.split_once('-')?;
    let start: u64 = start_s.parse().ok()?;
    let end: u64 = if end_s.is_empty() {
        total_size.checked_sub(1)?
    } else {
        end_s.parse().ok()?
    };
    Some((start, end))
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
        Some(st.meta.revision())
    };
    let (node_id, role, term, leader_id, commit_index) = st.meta.role_info();
    Json(serde_json::json!({
        "status": "ok",
        "metadata_runtime": if st.meta.is_openraft() { "openraft" } else { "legacy" },
        "role_scope": if st.meta.is_openraft() { "control_group; see /experimental/openraft/status for all groups" } else { "legacy" },
        "production_ready": false,
        "node_id": node_id,
        "role": format!("{role:?}"),
        "term": term,
        "leader_id": leader_id,
        "commit_index": commit_index,
        "generation": gen,
        "metakv_revision": rev
    }))
}

async fn handle_raft_status(State(st): State<AppState>) -> impl IntoResponse {
    let (node_id, role, term, leader_id, commit_index) = st.meta.role_info();
    Json(serde_json::json!({
        "metadata_runtime": if st.meta.is_openraft() { "openraft" } else { "legacy" },
        "role_scope": if st.meta.is_openraft() { "control_group; see /experimental/openraft/status for all groups" } else { "legacy" },
        "production_ready": false,
        "node_id": node_id,
        "role": format!("{role:?}"),
        "term": term,
        "leader_id": leader_id,
        "commit_index": commit_index,
        "revision": if st.meta.is_openraft() { None } else { Some(st.meta.revision()) },
    }))
}

#[cfg(feature = "experimental-openraft-storage")]
async fn handle_experimental_openraft_status(State(st): State<AppState>) -> impl IntoResponse {
    Json(match &st.experimental_openraft {
        Some(status) => status.snapshot(),
        None => {
            serde_json::json!({"experimental": true, "enabled": false, "client_traffic": "legacy", "production_ready": false})
        }
    })
}

#[cfg(feature = "experimental-openraft-storage")]
#[derive(Serialize)]
struct FreshControlResponse {
    revision: u64,
    applied_index: u64,
    control: ControlPlaneState,
}

/// Internal, token-authenticated control snapshot. Only the control-group
/// leader can answer because the read performs a quorum-confirmed barrier.
#[cfg(feature = "experimental-openraft-storage")]
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

async fn handle_raft_elect(State(st): State<AppState>, headers: axum::http::HeaderMap) -> Response {
    if st.meta.is_openraft() {
        return api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "legacy Raft endpoint disabled in OpenRaft mode",
        );
    }
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    let meta = st.meta.clone();
    let res = tokio::task::spawn_blocking(move || meta.trigger_election()).await;
    match res {
        Ok(Ok(role)) => {
            let (node_id, _, term, leader_id, commit_index) = st.meta.role_info();
            Json(serde_json::json!({
                "node_id": node_id,
                "role": format!("{role:?}"),
                "term": term,
                "leader_id": leader_id,
                "commit_index": commit_index,
            }))
            .into_response()
        }
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_raft_rpc(
    State(st): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(rpc): Json<RaftRpc>,
) -> Response {
    if st.meta.is_openraft() {
        return api_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "legacy Raft endpoint disabled in OpenRaft mode",
        );
    }
    if !raft_token_matches(&headers, st.raft_token.as_deref()) {
        return api_err(
            StatusCode::UNAUTHORIZED,
            "invalid or missing internal Raft token",
        );
    }
    match st.meta.step_rpc(rpc) {
        Ok(reply) => Json(reply).into_response(),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
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
            None => Ok((derive_idempotency_usage(&meta), 0)),
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
            None => (derive_idempotency_usage(&meta), 0),
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
fn derive_idempotency_usage(meta: &yadb_metakv::LocalMetaKv<OsFs>) -> IdempotencyUsage {
    let mut usage = IdempotencyUsage {
        version: 1,
        ..IdempotencyUsage::default()
    };
    for (key, kv) in meta.scan_prefix("/sys/idempotency/") {
        if key == IDEMPOTENCY_USAGE_KEY {
            continue;
        }
        usage.records = usage.records.saturating_add(1);
        usage.bytes = usage.bytes.saturating_add(kv.value.len() as u64);
    }
    usage
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
    if (req.method() == axum::http::Method::PUT || req.method() == axum::http::Method::DELETE)
        && req.headers().contains_key("idempotency-key")
    {
        return unsupported_idempotency();
    }
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
                } else if let Some(kv) = st.meta.get(CONTROL_STATE_KEY) {
                    if let Ok(cp) = serde_json::from_slice::<ControlPlaneState>(&kv.value) {
                        let _ = st.materialize_pools(&cp);
                        *st.control.write().unwrap() = cp;
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
    if headers.contains_key("idempotency-key") {
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

    if has_writes || req.force_leader {
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
    let sql_raw = req.sql;
    let subj_session = req
        .session_id
        .map(|id| format!("{subject}#{id}"))
        .unwrap_or(subject);
    let exec = tokio::task::spawn_blocking(move || {
        db_engine.execute_sql_with_session(
            project_id,
            &database,
            &wal_pool,
            &data_pool,
            shards,
            Some(&subj_session),
            &sql_raw,
        )
    })
    .await;

    match exec {
        Ok(Ok(res)) => Json(res).into_response(),
        Ok(Err(e @ (DbError::TableNotFound(_) | DbError::IndexNotFound(_, _)))) => {
            api_err(StatusCode::NOT_FOUND, e.to_string())
        }
        Ok(Err(
            e @ (DbError::InvalidQuery(_)
            | DbError::TableAlreadyExists(_)
            | DbError::IndexAlreadyExists(_, _)
            | DbError::DuplicatePrimaryKey(_)),
        )) => api_err(StatusCode::BAD_REQUEST, e.to_string()),
        Ok(Err(DbError::Meta(me))) => meta_err_resp(&st, me),
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
        return unsupported_idempotency();
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
    if q.consistent && st.meta.is_openraft() {
        // The extent map is read through a quorum-confirmed snapshot on the
        // project-group leader, so this request must run on that leader. The
        // forward is the same one the legacy path uses; only the read primitive
        // below differs.
        if let Some(resp) = maybe_forward_write(
            &st,
            &headers,
            Some(&project),
            "GET",
            format!(
                "{}?offset={}&len={}&consistent=true",
                block_path(&project, &volume),
                q.offset,
                q.len
            ),
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
        let blk = st.block.clone();
        let res = tokio::task::spawn_blocking(move || {
            blk.read_at_consistent(project_id, &volume, &pool_name, size_bytes, q.offset, q.len)
        })
        .await;
        return match res {
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
        };
    }
    if q.consistent {
        if let Some(resp) = maybe_forward_write(
            &st,
            &headers,
            Some(&project),
            "GET",
            format!(
                "{}?offset={}&len={}&consistent=true",
                block_path(&project, &volume),
                q.offset,
                q.len
            ),
            "application/octet-stream".into(),
            Bytes::new(),
        )
        .await
        {
            return resp;
        }
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

    if q.consistent {
        let meta = st.meta.clone();
        let fence = tokio::task::spawn_blocking(move || {
            meta.execute(KvCommand::Txn {
                guards: vec![],
                ops: vec![],
            })
        })
        .await;
        if !matches!(fence, Ok(Ok(yadb_metakv::KvResult::TxnOk { .. }))) {
            return api_err(
                StatusCode::SERVICE_UNAVAILABLE,
                "read quorum unavailable or metadata changed",
            );
        }
    }

    let blk = st.block.clone();
    let res = tokio::task::spawn_blocking(move || {
        blk.read_at(project_id, &volume, &pool_name, size_bytes, q.offset, q.len)
    })
    .await;

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

async fn handle_s3_put(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, store, bucket, key)): Path<(String, String, String, String)>,
    body: Bytes,
) -> Response {
    if headers.contains_key("idempotency-key") {
        return unsupported_idempotency();
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
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_s3_get(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, store, bucket, key)): Path<(String, String, String, String)>,
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

    if let Some(range_hdr) = headers
        .get(header::RANGE)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string)
    {
        let head = match st.s3.head_object(project_id, &store, &bucket, &key) {
            Ok(h) => h,
            Err(S3Error::ObjectNotFound { .. }) => {
                return api_err(StatusCode::NOT_FOUND, "object not found")
            }
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let Some((start, end)) = parse_range_header(&range_hdr, head.size) else {
            return api_err(StatusCode::RANGE_NOT_SATISFIABLE, "invalid Range header");
        };
        let s3 = st.s3.clone();
        let res = tokio::task::spawn_blocking(move || {
            s3.get_object_range(project_id, &store, &bucket, &key, start, end)
        })
        .await;
        return match res {
            Ok(Ok((mf, bytes))) => (
                StatusCode::PARTIAL_CONTENT,
                [
                    (header::CONTENT_TYPE, mf.content_type),
                    (header::ETAG, mf.etag),
                    (
                        header::CONTENT_RANGE,
                        format!("bytes {start}-{end}/{}", mf.size),
                    ),
                ],
                bytes,
            )
                .into_response(),
            Ok(Err(S3Error::InvalidRange { .. })) => {
                api_err(StatusCode::RANGE_NOT_SATISFIABLE, "range out of bounds")
            }
            Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
    }

    let s3 = st.s3.clone();
    let res =
        tokio::task::spawn_blocking(move || s3.get_object(project_id, &store, &bucket, &key)).await;

    match res {
        Ok(Ok((mf, bytes))) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, mf.content_type),
                (header::ETAG, mf.etag),
            ],
            bytes,
        )
            .into_response(),
        Ok(Err(S3Error::ObjectNotFound { .. })) => {
            api_err(StatusCode::NOT_FOUND, "object not found")
        }
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_s3_delete(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, store, bucket, key)): Path<(String, String, String, String)>,
) -> Response {
    if headers.contains_key("idempotency-key") {
        return unsupported_idempotency();
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

async fn handle_vdisk_delete(
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
    match tokio::task::spawn_blocking(move || store.delete(blob_id)).await {
        Ok(Ok(true)) => (StatusCode::OK, "deleted").into_response(),
        Ok(Ok(false)) => (StatusCode::OK, "absent").into_response(),
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
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
        )?;
        let _ = store.compact_sealed_segments();
        Ok::<usize, BlobError>(evicted)
    })
    .await;
    match res {
        Ok(Ok(evicted)) => Json(BarrierResponse { evicted }).into_response(),
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
    // Scrub is not a read: in legacy mode it collects orphan blobs, rewrites
    // fragments and records the summary. Any client that can reach the port must
    // not be able to start it. The check mirrors `/v1/apply` exactly so a legacy
    // deployment with no `--admin-subject` keeps working unchanged: an empty
    // allowlist is the historical no-auth mode, a configured allowlist requires a
    // signed admin subject.
    if let Err(e) = require_admin(
        &headers,
        st.subject_secret.as_deref(),
        &st.admin_subjects,
        st.meta.is_openraft(),
    ) {
        return e.into_response();
    }
    let st_clone = st.clone();
    let res = tokio::task::spawn_blocking(move || st_clone.scrub_all_pools()).await;
    match res {
        Ok(summary) => Json(summary).into_response(),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    check_experimental_openraft_config(args.experimental_openraft_config.as_deref())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let client_openraft = args.metadata_runtime == "openraft";
    if client_openraft && args.experimental_openraft_config.is_none() {
        return Err("--metadata-runtime openraft requires --experimental-openraft-config".into());
    }
    if client_openraft && !cfg!(feature = "experimental-openraft-storage") {
        return Err(
            "--metadata-runtime openraft requires experimental-openraft-storage feature".into(),
        );
    }
    if client_openraft && args.data_dir.is_none() {
        return Err("--metadata-runtime openraft requires an explicit new --data-dir".into());
    }
    if client_openraft && args.raft_token.as_deref().is_none_or(str::is_empty) {
        return Err(
            "--metadata-runtime openraft requires a non-empty --raft-token (or YADB_RAFT_TOKEN)"
                .into(),
        );
    }
    // A control-plane mutation needs a proven subject and an allowlist. Fail
    // closed at startup instead of refusing every `/v1/apply` at runtime.
    if client_openraft && args.admin_subject.is_empty() {
        return Err(
            "--metadata-runtime openraft requires at least one --admin-subject; an empty \
             allowlist would refuse every control-plane mutation"
                .into(),
        );
    }
    if args.admin_subject.iter().any(|s| s.trim().is_empty()) {
        return Err("--admin-subject must not contain empty entries".into());
    }
    if !args.admin_subject.is_empty() && args.subject_secret.as_deref().is_none_or(str::is_empty) {
        return Err(
            "--admin-subject requires a non-empty --subject-secret (or YADB_SUBJECT_SECRET) to \
             verify signed subjects"
                .into(),
        );
    }
    let data_dir = args.data_dir.clone().unwrap_or_else(|| {
        PathBuf::from("/data/data/com.foxdebug.acodefree/cache/opencode/yadbd-data")
    });
    if !client_openraft
        && (data_dir.join(".yadb-openraft-runtime.json").exists()
            || data_dir.join(".yadb-openraft-runtime.pending").exists())
    {
        return Err("legacy metadata runtime refuses an OpenRaft-marked data directory".into());
    }
    #[cfg(feature = "experimental-openraft-storage")]
    let prepared = if client_openraft {
        None
    } else {
        args.experimental_openraft_config
            .as_deref()
            .map(experimental_openraft::prepare)
            .transpose()?
    };
    #[cfg(feature = "experimental-openraft-storage")]
    let prepared_multi = if client_openraft {
        Some(experimental_openraft::prepare_multi(
            args.experimental_openraft_config.as_deref().unwrap(),
        )?)
    } else {
        None
    };
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
    #[cfg(feature = "experimental-openraft-storage")]
    let public_peers = if client_openraft {
        Some(
            parse_peer_map(
                args.peers
                    .as_deref()
                    .ok_or("--peers public HTTP map required for OpenRaft")?,
            )
            .map_err(std::io::Error::other)?,
        )
    } else {
        None
    };
    #[cfg(feature = "experimental-openraft-storage")]
    if let Some(peers) = &public_peers {
        prepared_multi.as_ref().unwrap().validate_client_root(
            &data_dir,
            args.node_id,
            peers,
            &args.listen,
        )?;
    }
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    #[cfg(feature = "experimental-openraft-storage")]
    let _client_root_owner = if client_openraft {
        Some(
            prepared_multi
                .as_ref()
                .unwrap()
                .provision_client_root(&data_dir, public_peers.as_ref().unwrap())?,
        )
    } else {
        None
    };
    #[cfg(feature = "experimental-openraft-storage")]
    let mut experimental = if let Some(prepared) = prepared_multi {
        Some(experimental_openraft::Owner::Multi(prepared.start().await?))
    } else if let Some(prepared) = prepared {
        Some(experimental_openraft::Owner::Single(
            prepared.start().await?,
        ))
    } else {
        None
    };
    let result: Result<(), Box<dyn std::error::Error>> = async {
    let fs = OsFs::new(&data_dir)?;

    #[cfg(feature = "experimental-openraft-storage")]
    let meta = if client_openraft {
        experimental.as_ref().unwrap().metadata(public_peers.clone().unwrap())?
    } else if let Some(peers_raw) = &args.peers {
        let peer_map = parse_peer_map(peers_raw).map_err(std::io::Error::other)?;
        let token = args.raft_token.clone().filter(|t| !t.is_empty()).ok_or(
            "--raft-token (or YADB_RAFT_TOKEN) is required and must be non-empty with --peers",
        )?;
        LocalMetaKv::open_clustered_with_token(&fs, "metakv", args.node_id, peer_map, token)?
    } else { LocalMetaKv::open_single_node(&fs, "metakv", args.node_id)? };
    #[cfg(not(feature = "experimental-openraft-storage"))]
    let meta = if let Some(peers_raw) = &args.peers {
        let peer_map = parse_peer_map(peers_raw)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        // Fail closed: a clustered node without the internal token would
        // expose an unauthenticated Raft endpoint to every client.
        let token = args.raft_token.clone().filter(|t| !t.is_empty()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--raft-token (or YADB_RAFT_TOKEN) is required and must be non-empty with --peers",
            )
        })?;
        LocalMetaKv::open_clustered_with_token(&fs, "metakv", args.node_id, peer_map, token)?
    } else {
        LocalMetaKv::open_single_node(&fs, "metakv", args.node_id)?
    };

    // Subscribe first so an apply concurrent with the initial read is queued.
    let watch_rx = meta.watch_prefix(CONTROL_STATE_KEY);
    // Recover persisted declarative ControlPlaneState from the selected backend.
    let recovered_cp: ControlPlaneState = match meta.get(CONTROL_STATE_KEY) {
        Some(kv) => serde_json::from_slice(&kv.value)?,
        None => ControlPlaneState::new(),
    };

    let s3 = ObjectStoreEngine::new(meta.clone(), args.chunk_size);
    let db = DatabaseEngine::new(meta.clone(), args.db_flush_rows);
    let block = BlockVolumeEngine::new(meta.clone(), DEFAULT_BLOCK_SIZE);

    let state = AppState {
        #[cfg(feature = "experimental-openraft-storage")]
        experimental_openraft: experimental.as_ref().map(|group| group.status()),
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

    // Periodic Raft leader heartbeat, automatic follower election timeout & Self-Healing VDisk Scrubber
    if args.peers.is_some() && !client_openraft {
        let hb_meta = meta.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(200));
            loop {
                interval.tick().await;
                let m = hb_meta.clone();
                let _ = tokio::task::spawn_blocking(move || m.broadcast_heartbeat()).await;
            }
        });

        let elect_meta = meta.clone();
        let node_id = args.node_id;
        tokio::spawn(async move {
            let mut tick_no: u64 = 0;
            let mut interval = tokio::time::interval(Duration::from_millis(150));
            loop {
                interval.tick().await;
                tick_no = tick_no.wrapping_add(1);
                let jitter_ms = ((node_id as u64).wrapping_mul(193).wrapping_add(tick_no * 37)) % 450;
                let timeout = Duration::from_millis(850 + jitter_ms);
                let m = elect_meta.clone();
                let _ = tokio::task::spawn_blocking(move || m.check_election_timeout(timeout)).await;
            }
        });

    }
    if args.peers.is_some() {
        let scrub_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(4));
            loop {
                interval.tick().await;
                let (_, role, _, _, _) = scrub_state.meta.role_info();
                if role == RoleState::Leader {
                    let st = scrub_state.clone();
                    let _ = tokio::task::spawn_blocking(move || st.scrub_all_pools()).await;
                }
            }
        });
    }

    if let Some(addr) = &args.pg_listen {
        let listener = std::net::TcpListener::bind(addr)?;
        let st = state.clone(); let creds = credentials.clone();
        protocols::serve(listener, move |c| protocols::pg::connection(c,st.clone(),creds.clone()));
        println!("pgwire Simple Query listening on {addr} (no TLS; trusted networks only)");
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
        .route("/raft/elect", post(handle_raft_elect))
        .route("/raft/rpc", post(handle_raft_rpc))
        .route("/v1/sample-yaml", get(handle_sample_yaml))
        .route("/v1/status", get(handle_status))
        .route("/v1/plan", post(handle_plan))
        .route("/v1/apply", post(apply_dispatch))
        .route("/v1/idempotency/resolve", post(handle_resolve))
        .route("/v1/pools/health", get(handle_pools_health))
        .route("/v1/pools/scrub", post(handle_pools_scrub))
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
        .route("/db/{project}/{database}/query", post(handle_db_query))
        .route("/block/{project}/{volume}/unmap", post(handle_block_unmap))
        .route(
            "/block/{project}/{volume}",
            put(handle_block_write).get(handle_block_read),
        )
        .route("/s3/{project}/{store}/{bucket}", get(handle_s3_list))
        .route(
            "/s3/{project}/{store}/{bucket}/{*key}",
            put(handle_s3_put).get(handle_s3_get).delete(handle_s3_delete),
        )
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024 * 1024))
        ;
    #[cfg(feature = "experimental-openraft-storage")]
    let app = app
        .route(
            "/experimental/openraft/status",
            get(handle_experimental_openraft_status),
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
    #[cfg(feature = "experimental-openraft-storage")]
    {
        let result = if let Some(group) = experimental.as_mut() {
            tokio::select! {
                result = axum::serve(listener, app) => result,
                result = tokio::signal::ctrl_c() => result,
                result = group.listener_finished() => Err(std::io::Error::other(format!("experimental OpenRaft listener stopped: {result}"))),
            }
        } else {
            tokio::select! {
                result = axum::serve(listener, app) => result,
                result = tokio::signal::ctrl_c() => result,
            }
        };
        let _ = watch_stop.send(());
        let _ = watch_thread.join();
        result?;
    }
    #[cfg(not(feature = "experimental-openraft-storage"))]
    {
        let result = axum::serve(listener, app).await;
        let _ = watch_stop.send(());
        let _ = watch_thread.join();
        result?;
    }
    Ok(())
    }.await;
    #[cfg(feature = "experimental-openraft-storage")]
    if let Some(group) = experimental.as_mut() {
        group.shutdown().await?;
    }
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
    fn forwarding_error_outcomes_are_machine_readable() {
        let unknown = forward_error_response("failed".into(), Some("unknown"));
        assert_eq!(unknown.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unknown.headers()["x-yadb-write-outcome"], "unknown");
        assert_eq!(unknown.headers()["x-yadb-retry"], "do-not-retry");
        let not_sent = forward_error_response("not sent".into(), Some("not-sent"));
        assert_eq!(not_sent.headers()["x-yadb-write-outcome"], "not-sent");
        assert!(!not_sent.headers().contains_key("x-yadb-retry"));
    }

    #[test]
    fn openraft_unknown_meta_error_is_non_retryable_503() {
        let mapped = meta_err_response(MetaError::OpenRaftWriteUnknown("timeout".into()), |_| None);
        assert_eq!(mapped.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.headers()["x-yadb-write-outcome"], "unknown");
        assert_eq!(mapped.headers()["x-yadb-retry"], "do-not-retry");
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
    fn experimental_openraft_config_fails_closed() {
        assert!(check_experimental_openraft_config(None).is_ok());
        if cfg!(feature = "experimental-openraft-storage") {
            assert!(
                check_experimental_openraft_config(Some(std::path::Path::new("config.json")))
                    .is_ok()
            );
        } else {
            let error =
                check_experimental_openraft_config(Some(std::path::Path::new("config.json")))
                    .unwrap_err();
            assert!(error.contains("requires the experimental-openraft-storage feature"));
        }
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
            #[cfg(feature = "experimental-openraft-storage")]
            experimental_openraft: None,
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
            let delete = handle_vdisk_delete(State(st.clone()), blob_path(), headers.clone()).await;
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
            handle_vdisk_delete(State(st.clone()), path(), headers.clone())
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
                    up_to_generation: 0x00ff_ffff
                })
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
}
