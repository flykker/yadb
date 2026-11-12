use std::collections::BTreeMap;
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
use yadb_blob::{PoolStriper, VolumeConfig, VolumeStore};
use yadb_block::{BlockError, BlockVolumeEngine, DEFAULT_BLOCK_SIZE};
use yadb_control::{parse_manifests, ControlPlaneState, DiffOp, Role};
use yadb_db::{parse_sql, DatabaseEngine, DbError};
use yadb_io::OsFs;
use yadb_metakv::{KvCommand, LocalMetaKv, MetaError, RaftRpc, WatchEvent};
use yadb_s3::{ObjectStoreEngine, S3Error};

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
    fs: OsFs,
    meta: LocalMetaKv<OsFs>,
    control: Arc<RwLock<ControlPlaneState>>,
    s3: ObjectStoreEngine<OsFs>,
    db: DatabaseEngine<OsFs>,
    block: BlockVolumeEngine<OsFs>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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
    /// Reconciles local `VolumeStore` + `PoolStriper` instances across S3, DB, and Block engines!
    fn materialize_pools(&self, cp: &ControlPlaneState) -> Result<(), String> {
        for (pool_name, spec) in &cp.pools {
            if self.s3.has_pool(pool_name) && self.db.has_pool(pool_name) {
                continue;
            }
            let scheme = spec.erasure.to_scheme();
            let parts = scheme.total_parts();
            let mut domains = Vec::with_capacity(parts);
            for slot in 0..parts {
                let dir = format!("pools/{pool_name}/slot_{slot:02}");
                let store = VolumeStore::open(self.fs.clone(), dir, VolumeConfig::default())
                    .map_err(|e| e.to_string())?;
                domains.push(store);
            }
            let striper = Arc::new(PoolStriper::new(scheme, domains).map_err(|e| e.to_string())?);
            self.s3.register_pool(pool_name.clone(), striper.clone());
            self.db.register_pool(pool_name.clone(), striper.clone());
            self.block.register_pool(pool_name.clone(), striper);
        }
        Ok(())
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
    Json(req): Json<ManifestPayload>,
) -> Response {
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
    let stmt = match parse_sql(&req.sql) {
        Ok(s) => s,
        Err(e) => return api_err(StatusCode::BAD_REQUEST, e.to_string()),
    };

    let min_role = if stmt.is_write() {
        Role::ReadWrite
    } else {
        Role::ReadOnly
    };
    let subject = extract_subject(&headers);

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
    let exec = tokio::task::spawn_blocking(move || {
        db_engine.execute_stmt(project_id, &database, &wal_pool, &data_pool, shards, stmt)
    })
    .await;

    match exec {
        Ok(Ok(res)) => Json(res).into_response(),
        Ok(Err(DbError::TableNotFound(t))) => {
            api_err(StatusCode::NOT_FOUND, format!("table `{t}` does not exist"))
        }
        Ok(Err(
            e @ (DbError::InvalidQuery(_)
            | DbError::TableAlreadyExists(_)
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

    match st
        .block
        .read_at(project_id, &volume, &pool_name, size_bytes, q.offset, q.len)
    {
        Ok(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response(),
        Err(e @ BlockError::OutOfBounds { .. }) => {
            api_err(StatusCode::RANGE_NOT_SATISFIABLE, e.to_string())
        }
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
    let subject = extract_subject(&headers);
    let (project_id, pool_name) = {
        let cp = st.control.read().unwrap();
        match resolve_s3_target(&cp, &project, &store, &bucket, &subject, Role::ReadWrite) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

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

    if let Some(range_hdr) = headers.get(header::RANGE).and_then(|h| h.to_str().ok()) {
        let head = match st.s3.head_object(project_id, &store, &bucket, &key) {
            Ok(h) => h,
            Err(S3Error::ObjectNotFound { .. }) => {
                return api_err(StatusCode::NOT_FOUND, "object not found")
            }
            Err(e) => return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let Some((start, end)) = parse_range_header(range_hdr, head.size) else {
            return api_err(StatusCode::RANGE_NOT_SATISFIABLE, "invalid Range header");
        };
        return match st
            .s3
            .get_object_range(project_id, &store, &bucket, &key, start, end)
        {
            Ok((mf, bytes)) => (
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
            Err(S3Error::InvalidRange { .. }) => {
                api_err(StatusCode::RANGE_NOT_SATISFIABLE, "range out of bounds")
            }
            Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
    }

    match st.s3.get_object(project_id, &store, &bucket, &key) {
        Ok((mf, bytes)) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, mf.content_type),
                (header::ETAG, mf.etag),
            ],
            bytes,
        )
            .into_response(),
        Err(S3Error::ObjectNotFound { .. }) => {
            api_err(StatusCode::NOT_FOUND, "object not found")
        }
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn handle_s3_delete(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path((project, store, bucket, key)): Path<(String, String, String, String)>,
) -> Response {
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
        fs,
        meta: meta.clone(),
        control: Arc::new(RwLock::new(recovered_cp.clone())),
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

    // Periodic Raft leader heartbeat in multi-node mode
    if args.peers.is_some() {
        let hb_meta = meta.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                interval.tick().await;
                let m = hb_meta.clone();
                let _ = tokio::task::spawn_blocking(move || m.broadcast_heartbeat()).await;
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
