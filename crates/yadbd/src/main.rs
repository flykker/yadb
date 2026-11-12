use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use clap::Parser;
use serde::{Deserialize, Serialize};
use yadb_blob::{PoolStriper, VolumeConfig, VolumeStore};
use yadb_control::{parse_manifests, ControlPlaneState, DiffOp, Role};
use yadb_io::OsFs;
use yadb_metakv::{KvCommand, LocalMetaKv};
use yadb_s3::{ObjectStoreEngine, S3Error};

const CONTROL_STATE_KEY: &str = "/sys/control_plane_state";

#[derive(Parser)]
#[command(name = "yadbd", version, about = "YADB Node & Gateway Daemon")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8420")]
    listen: String,

    #[arg(long, default_value_t = 1)]
    node_id: u32,

    #[arg(long, default_value = "/data/data/com.foxdebug.acodefree/cache/opencode/yadbd-data")]
    data_dir: PathBuf,

    /// S3 chunk size in bytes (default 1 MiB)
    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size: usize,
}

#[derive(Clone)]
struct AppState {
    fs: OsFs,
    meta: LocalMetaKv<OsFs>,
    control: Arc<RwLock<ControlPlaneState>>,
    s3: ObjectStoreEngine<OsFs>,
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

impl AppState {
    /// Reconciles local `VolumeStore` + `PoolStriper` instances for newly declared `StoragePool`s.
    fn materialize_pools(&self, cp: &ControlPlaneState) -> Result<(), String> {
        for (pool_name, spec) in &cp.pools {
            if self.s3.has_pool(pool_name) {
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
            let striper = PoolStriper::new(scheme, domains).map_err(|e| e.to_string())?;
            self.s3.register_pool(pool_name.clone(), Arc::new(striper));
        }
        Ok(())
    }
}

fn api_err(status: StatusCode, msg: impl Into<String>) -> Response {
    let body = serde_json::json!({ "error": msg.into() });
    (status, Json(body)).into_response()
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

async fn handle_health(State(st): State<AppState>) -> impl IntoResponse {
    let gen = st.control.read().unwrap().generation;
    let rev = st.meta.revision();
    Json(serde_json::json!({
        "status": "ok",
        "generation": gen,
        "metakv_revision": rev
    }))
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
    if let Err(e) = st.meta.execute(KvCommand::Put {
        key: CONTROL_STATE_KEY.to_string(),
        value: serialized,
    }) {
        return api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
    }

    let generation = next_state.generation;
    *st.control.write().unwrap() = next_state;

    Json(ApplyResponse { generation, ops }).into_response()
}

/// Validates project RBAC + `ObjectStore` + bucket declaration from local zero-hop memory cache!
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

    match st.s3.put_object(
        project_id,
        &store,
        &pool_name,
        &bucket,
        &key,
        &content_type,
        &body,
    ) {
        Ok(mf) => (
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

    match st.s3.delete_object(project_id, &store, &bucket, &key) {
        Ok(existed) => Json(serde_json::json!({ "deleted": existed })).into_response(),
        Err(e) => api_err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let fs = OsFs::new(&args.data_dir)?;
    let meta = LocalMetaKv::open_single_node(&fs, "metakv", args.node_id)?;

    // Recover persisted declarative ControlPlaneState from MetaKV WAL on startup!
    let recovered_cp: ControlPlaneState = match meta.get(CONTROL_STATE_KEY) {
        Some(kv) => serde_json::from_slice(&kv.value)?,
        None => ControlPlaneState::new(),
    };

    let s3 = ObjectStoreEngine::new(meta.clone(), args.chunk_size);
    let state = AppState {
        fs,
        meta,
        control: Arc::new(RwLock::new(recovered_cp.clone())),
        s3,
    };
    state
        .materialize_pools(&recovered_cp)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let mut _unused_check: BTreeMap<String, ()> = BTreeMap::new();
    _unused_check.clear();

    let app = Router::new()
        .route("/healthz", get(handle_health))
        .route("/v1/status", get(handle_status))
        .route("/v1/plan", post(handle_plan))
        .route("/v1/apply", post(handle_apply))
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
