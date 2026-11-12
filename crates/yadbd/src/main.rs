use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use clap::Parser;
use serde::{Deserialize, Serialize};
use yadb_blob::{
    BarrierRequest, BarrierResponse, BlobError, BlobId, PoolHealthSnapshot, PoolSlot, PoolStriper,
    VolumeConfig, VolumeStore,
};
use yadb_block::{BlockError, BlockVolumeEngine, VolumeExtentMap, DEFAULT_BLOCK_SIZE};
use yadb_control::{parse_manifests, ControlPlaneState, DiffOp, Role};
use yadb_db::{parse_sql_script, script_has_writes, DatabaseEngine, DbError, TabletManifest};
use yadb_io::OsFs;
use yadb_metakv::{KvCommand, LocalMetaKv, MetaError, RaftRpc, RoleState, WatchEvent};
use yadb_s3::{ObjectManifest, ObjectStoreEngine, S3Error};

const CONTROL_STATE_KEY: &str = "/sys/control_plane_state";
const EMBEDDED_UI_HTML: &str = include_str!("ui/index.html");
const EMBEDDED_SAMPLE_YAML: &str = include_str!("../../../examples/platform.yaml");

#[derive(Parser)]
#[command(name = "yadbd", version, about = "YADB Unified Storage, SQL, S3 & Block Node Daemon")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8420")]
    listen: String,

    #[arg(long, default_value_t = 1)]
    node_id: u32,

    /// Optional comma-separated Raft peer map: `1=127.0.0.1:8421,2=127.0.0.1:8422,3=127.0.0.1:8423`
    #[arg(long)]
    peers: Option<String>,

    #[arg(long, default_value = "/data/data/com.foxdebug.acodefree/cache/opencode/yadbd-data")]
    data_dir: PathBuf,

    /// S3 chunk size in bytes (default 1 MiB)
    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size: usize,

    /// MemTable auto-flush threshold per SQL shard (default 1024 rows)
    #[arg(long, default_value_t = 1024)]
    db_flush_rows: usize,
}

#[derive(Clone)]
struct AppState {
    listen_addr: String,
    fs: OsFs,
    meta: LocalMetaKv<OsFs>,
    control: Arc<RwLock<ControlPlaneState>>,
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
}

#[derive(Deserialize)]
struct BlockWriteQuery {
    offset: u64,
}

#[derive(Deserialize)]
struct BlockReadQuery {
    offset: u64,
    len: usize,
}

impl AppState {
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
                                    targets.push((
                                        dom.name.clone(),
                                        node.id,
                                        Some(addr.clone()),
                                    ));
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
            pools: pools_health,
        };
        *self.last_scrub.write().unwrap() = Some(summary.clone());
        summary
    }
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
    match err {
        MetaError::NotLeader { leader } => {
            let leader_addr = leader.and_then(|id| st.meta.peer_addr(id));
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

fn extract_subject(headers: &HeaderMap) -> String {
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

/// Raw HTTP/1.1 forwarding helper used by Followers to transparently proxy client writes to the Raft Leader.
fn proxy_http_to_peer(
    peer_addr: &str,
    method: &str,
    path_and_query: &str,
    subject: &str,
    content_type: &str,
    body: &[u8],
) -> Result<(u16, String, Option<String>, Vec<u8>), std::io::Error> {
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

    let req = format!(
        "{method} {path_and_query} HTTP/1.1\r\nHost: {clean}\r\nx-yadb-subject: {subject}\r\nx-yadb-forwarded: 1\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(req.as_bytes())?;
    if !body.is_empty() {
        stream.write_all(body)?;
    }
    stream.flush()?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;

    let Some(hdr_end) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed HTTP response from Raft Leader",
        ));
    };
    let hdr_str = String::from_utf8_lossy(&raw[..hdr_end]);
    let status_code: u16 = hdr_str
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid status line from Raft Leader",
            )
        })?;

    let mut resp_ct = "application/json".to_string();
    let mut resp_etag = None;
    let mut is_chunked = false;
    for line in hdr_str.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            let kl = k.trim().to_ascii_lowercase();
            let vt = v.trim();
            if kl == "content-type" {
                resp_ct = vt.to_string();
            } else if kl == "etag" {
                resp_etag = Some(vt.to_string());
            } else if kl == "transfer-encoding" && vt.to_ascii_lowercase().contains("chunked") {
                is_chunked = true;
            }
        }
    }

    let payload_slice = &raw[hdr_end + 4..];
    let decoded = if is_chunked {
        decode_http_chunked(payload_slice)?
    } else {
        payload_slice.to_vec()
    };
    Ok((status_code, resp_ct, resp_etag, decoded))
}

fn decode_http_chunked(mut input: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = input.windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size_str = std::str::from_utf8(&input[..line_end])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        let chunk_len = usize::from_str_radix(size_str, 16)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        input = &input[line_end + 2..];
        if chunk_len == 0 {
            break;
        }
        if input.len() < chunk_len + 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated chunked response",
            ));
        }
        out.extend_from_slice(&input[..chunk_len]);
        input = &input[chunk_len + 2..];
    }
    Ok(out)
}

/// Transparently forwards mutating requests from any Raft Follower to the active Raft Leader!
/// If the known leader just crashed (connection refused) or no leader is elected yet, automatically
/// triggers a Raft election and either executes locally (if promoted to Leader) or routes to the new Leader.
async fn maybe_forward_write(
    st: &AppState,
    headers: &HeaderMap,
    method: &'static str,
    path_and_query: String,
    content_type: String,
    body: Bytes,
) -> Option<Response> {
    if headers.contains_key("x-yadb-forwarded") {
        return None;
    }
    let (_, role, _, _, _) = st.meta.role_info();
    if role == RoleState::Leader {
        return None;
    }

    let subject = extract_subject(headers);
    let meta = st.meta.clone();

    let outcome = tokio::task::spawn_blocking(move || {
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
                &content_type,
                &body,
            ) {
                Ok((409, _, _, resp_body))
                    if String::from_utf8_lossy(&resp_body).contains("not Raft leader") =>
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
        Err("cluster has no reachable Raft leader (quorum unavailable)".to_string())
    })
    .await;

    match outcome {
        Ok(Ok(None)) => None, // This node just became Leader; execute locally!
        Ok(Ok(Some((leader_id, (code, ct, etag_opt, resp_bytes))))) => {
            let status = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let mut builder = Response::builder()
                .status(status)
                .header(header::CONTENT_TYPE, ct)
                .header("x-yadb-leader-id", leader_id.to_string());
            if let Some(etag) = etag_opt {
                builder = builder.header(header::ETAG, etag);
            }
            Some(
                builder
                    .body(axum::body::Body::from(resp_bytes))
                    .unwrap_or_else(|e| api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
            )
        }
        Ok(Err(msg)) => Some(api_err(StatusCode::SERVICE_UNAVAILABLE, msg)),
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
    let rev = st.meta.revision();
    let (node_id, role, term, leader_id, commit_index) = st.meta.role_info();
    Json(serde_json::json!({
        "status": "ok",
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
        "node_id": node_id,
        "role": format!("{role:?}"),
        "term": term,
        "leader_id": leader_id,
        "commit_index": commit_index,
        "revision": st.meta.revision(),
    }))
}

async fn handle_raft_elect(State(st): State<AppState>) -> Response {
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

async fn handle_raft_rpc(State(st): State<AppState>, Json(rpc): Json<RaftRpc>) -> Response {
    match st.meta.step_rpc(rpc) {
        Ok(reply) => Json(reply).into_response(),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_status(State(st): State<AppState>) -> impl IntoResponse {
    let cp = st.control.read().unwrap().clone();
    Json(cp)
}

async fn handle_plan(
    State(st): State<AppState>,
    Json(req): Json<ManifestPayload>,
) -> Response {
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

async fn handle_apply(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ManifestPayload>,
) -> Response {
    if let Ok(raw_body) = serde_json::to_vec(&req) {
        if let Some(proxied) = maybe_forward_write(
            &st,
            &headers,
            "POST",
            "/v1/apply".to_string(),
            "application/json".to_string(),
            Bytes::from(raw_body),
        )
        .await
        {
            // Ensure local zero-hop cache & VDisk pools are immediately synced before returning!
            if proxied.status().is_success() {
                if let Some(kv) = st.meta.get(CONTROL_STATE_KEY) {
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

    let (ops, next_state) = {
        let cur = st.control.read().unwrap();
        match cur.plan(&manifests) {
            Ok(r) => r,
            Err(e) => return api_err(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        }
    };

    if let Err(e) = st.materialize_pools(&next_state) {
        return api_err(StatusCode::INTERNAL_SERVER_ERROR, e);
    }

    let serialized = match serde_json::to_vec(&next_state) {
        Ok(b) => b,
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let meta = st.meta.clone();
    let put_res = tokio::task::spawn_blocking(move || {
        meta.execute(KvCommand::Put {
            key: CONTROL_STATE_KEY.to_string(),
            value: serialized,
        })
    })
    .await;

    match put_res {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return meta_err_resp(&st, e),
        Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }

    let generation = next_state.generation;
    *st.control.write().unwrap() = next_state;

    Json(ApplyResponse { generation, ops }).into_response()
}

// ============================================================================
// Sharded SQL Database Gateway (`/db/{project}/{database}/query`)
// ============================================================================

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
    let min_role = if has_writes {
        Role::ReadWrite
    } else {
        Role::ReadOnly
    };
    let subject = extract_subject(&headers);

    if has_writes {
        if let Ok(raw_body) = serde_json::to_vec(&req) {
            if let Some(proxied) = maybe_forward_write(
                &st,
                &headers,
                "POST",
                format!("/db/{project}/{database}/query"),
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
        let cp = st.control.read().unwrap();
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
    let sql_raw = req.sql;
    let subj_session = subject;
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
    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        "PUT",
        format!("/block/{project}/{volume}?offset={}", q.offset),
        "application/octet-stream".to_string(),
        body.clone(),
    )
    .await
    {
        return proxied;
    }

    let subject = extract_subject(&headers);
    let (project_id, pool_name, size_bytes) = {
        let cp = st.control.read().unwrap();
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
    let subject = extract_subject(&headers);
    let (project_id, pool_name, size_bytes) = {
        let cp = st.control.read().unwrap();
        match resolve_block_target(&cp, &project, &volume, &subject, Role::ReadOnly) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };

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
    let subject = extract_subject(&headers);
    let (project_id, _) = {
        let cp = st.control.read().unwrap();
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
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        "PUT",
        format!("/s3/{project}/{store}/{bucket}/{key}"),
        content_type.clone(),
        body.clone(),
    )
    .await
    {
        return proxied;
    }

    let subject = extract_subject(&headers);
    let (project_id, pool_name) = {
        let cp = st.control.read().unwrap();
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
    let subject = extract_subject(&headers);
    let (project_id, _) = {
        let cp = st.control.read().unwrap();
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
    let res = tokio::task::spawn_blocking(move || s3.get_object(project_id, &store, &bucket, &key))
        .await;

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
    if let Some(proxied) = maybe_forward_write(
        &st,
        &headers,
        "DELETE",
        format!("/s3/{project}/{store}/{bucket}/{key}"),
        "application/octet-stream".to_string(),
        Bytes::new(),
    )
    .await
    {
        return proxied;
    }

    let subject = extract_subject(&headers);
    let (project_id, _) = {
        let cp = st.control.read().unwrap();
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

async fn handle_vdisk_put(
    State(st): State<AppState>,
    Path((pool, slot, blob_hex)): Path<(String, usize, String)>,
    body: Bytes,
) -> Response {
    let Some(blob_id) = BlobId::from_hex(&blob_hex) else {
        return api_err(StatusCode::BAD_REQUEST, "invalid 32-char hex BlobId");
    };
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(StatusCode::NOT_FOUND, "local VDisk slot not hosted on this node");
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
        Ok(Err(e)) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_vdisk_get(
    State(st): State<AppState>,
    Path((pool, slot, blob_hex)): Path<(String, usize, String)>,
) -> Response {
    let Some(blob_id) = BlobId::from_hex(&blob_hex) else {
        return api_err(StatusCode::BAD_REQUEST, "invalid 32-char hex BlobId");
    };
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(StatusCode::NOT_FOUND, "local VDisk slot not hosted on this node");
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
) -> Response {
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(StatusCode::NOT_FOUND, "local VDisk slot not hosted on this node");
    };
    match store.stats() {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_vdisk_barrier(
    State(st): State<AppState>,
    Path((pool, slot)): Path<(String, usize)>,
    Json(req): Json<BarrierRequest>,
) -> Response {
    let Some(store) = st.local_vdisks.read().unwrap().get(&(pool, slot)).cloned() else {
        return api_err(StatusCode::NOT_FOUND, "local VDisk slot not hosted on this node");
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

async fn handle_pools_scrub(State(st): State<AppState>) -> Response {
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
    let fs = OsFs::new(&args.data_dir)?;

    let meta = if let Some(peers_raw) = &args.peers {
        let peer_map = parse_peer_map(peers_raw)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        LocalMetaKv::open_clustered(&fs, "metakv", args.node_id, peer_map)?
    } else {
        LocalMetaKv::open_single_node(&fs, "metakv", args.node_id)?
    };

    // Recover persisted declarative ControlPlaneState from MetaKV WAL on startup
    let recovered_cp: ControlPlaneState = match meta.get(CONTROL_STATE_KEY) {
        Some(kv) => serde_json::from_slice(&kv.value)?,
        None => ControlPlaneState::new(),
    };

    let s3 = ObjectStoreEngine::new(meta.clone(), args.chunk_size);
    let db = DatabaseEngine::new(meta.clone(), args.db_flush_rows);
    let block = BlockVolumeEngine::new(meta.clone(), DEFAULT_BLOCK_SIZE);

    let state = AppState {
        listen_addr: args.listen.clone(),
        fs,
        meta: meta.clone(),
        control: Arc::new(RwLock::new(recovered_cp.clone())),
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
    let watch_rx = meta.watch_prefix(CONTROL_STATE_KEY);
    let watch_state = state.clone();
    std::thread::spawn(move || {
        while let Ok(ev) = watch_rx.recv() {
            if let WatchEvent::Put { value, .. } = ev {
                if let Ok(cp) = serde_json::from_slice::<ControlPlaneState>(&value) {
                    let _ = watch_state.materialize_pools(&cp);
                    *watch_state.control.write().unwrap() = cp;
                }
            }
        }
    });

    // Periodic Raft leader heartbeat, automatic follower election timeout & Self-Healing VDisk Scrubber
    if args.peers.is_some() {
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
        .route("/v1/apply", post(handle_apply))
        .route("/v1/pools/health", get(handle_pools_health))
        .route("/v1/pools/scrub", post(handle_pools_scrub))
        .route(
            "/internal/vdisk/{pool}/{slot}/blob/{blob_hex}",
            put(handle_vdisk_put).get(handle_vdisk_get),
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
        .route(
            "/block/{project}/{volume}",
            put(handle_block_write).get(handle_block_read),
        )
        .route("/s3/{project}/{store}/{bucket}", get(handle_s3_list))
        .route(
            "/s3/{project}/{store}/{bucket}/{*key}",
            put(handle_s3_put).get(handle_s3_get).delete(handle_s3_delete),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    println!(
        "yadbd node {} listening on http://{} (data={:?}, generation={})",
        args.node_id, args.listen, args.data_dir, recovered_cp.generation
    );
    axum::serve(listener, app).await?;
    Ok(())
}
