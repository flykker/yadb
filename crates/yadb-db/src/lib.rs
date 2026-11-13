pub mod sql;
pub mod tablet;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yadb_blob::{BlobError, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, KvResult, LocalMetaKv, MetaError, RevisionGuard, TxnOp};

pub use sql::{
    parse_sql, parse_sql_script, script_has_writes, AggFunc, ArithOp, AssignExpr, CmpOp, ColumnDef,
    ColumnType, Predicate, Projection, SelectItem, SqlStmt, SqlValue,
};
pub use tablet::{CompactionReport, DataShardTablet, PreparedShardCommit, TabletManifest};

#[derive(Debug, Error)]
pub enum DbError {
    #[error("SQL blob allocation, generation or row sequence space exhausted")]
    CounterExhausted,

    #[error("blob engine error: {0}")]
    Blob(#[from] BlobError),

    #[error("metakv error: {0}")]
    Meta(#[from] MetaError),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("storage pool `{0}` is not mounted on this node")]
    PoolNotMounted(String),

    #[error("SQL error: {0}")]
    InvalidQuery(String),

    #[error("table `{0}` already exists")]
    TableAlreadyExists(String),

    #[error("table `{0}` does not exist")]
    TableNotFound(String),

    #[error("index `{0}` already exists on table `{1}`")]
    IndexAlreadyExists(String, String),

    #[error("index `{0}` does not exist on table `{1}`")]
    IndexNotFound(String, String),

    #[error("duplicate primary key in table `{0}` (use UPSERT INTO to overwrite)")]
    DuplicatePrimaryKey(String),

    #[error("stale tablet generation or concurrent transaction conflict (owner_id={owner_id})")]
    StaleTabletGeneration { owner_id: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub column: String,
    pub col_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub pk_index: usize,
    pub shards: u32,
    pub wal_pool: String,
    pub data_pool: String,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub rows_affected: u64,
}

/// Uncommitted overlay buffer for multi-shard `BEGIN ... COMMIT / ROLLBACK` transactions.
#[derive(Debug, Clone, Default)]
struct TxBuffer {
    /// `table` -> `pk_bytes` -> `(initial_committed_row, latest_tx_row)`
    #[allow(clippy::type_complexity)]
    writes: BTreeMap<String, BTreeMap<Vec<u8>, (Option<Vec<SqlValue>>, Option<Vec<SqlValue>>)>>,
}

impl TxBuffer {
    fn is_empty(&self) -> bool {
        self.writes.values().all(BTreeMap::is_empty)
    }

    fn record_mutation(
        &mut self,
        table: &str,
        pk: Vec<u8>,
        base_row: Option<Vec<SqlValue>>,
        new_row: Option<Vec<SqlValue>>,
    ) {
        let tbl = self.writes.entry(table.to_string()).or_default();
        if let Some((orig, cur)) = tbl.get_mut(&pk) {
            let _ = orig;
            *cur = new_row;
        } else {
            tbl.insert(pk, (base_row, new_row));
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0F) as usize] as char);
    }
    out
}

fn encode_nullable_index_key(val: &SqlValue) -> Result<Vec<u8>, DbError> {
    match val {
        SqlValue::Null => Ok(vec![0x00]),
        other => other.encode_order_preserving_key(),
    }
}

/// Multi-tenant Sharded SQL & Distributed Transaction Engine built on `MetaKV` + dual `PoolStriper`
/// channels (`walPool` for low-latency mirrored WALs, `dataPool` for erasure-coded SSTables).
pub struct DatabaseEngine<FS: StorageFs> {
    meta: LocalMetaKv<FS>,
    pools: Arc<RwLock<BTreeMap<String, Arc<PoolStriper<FS>>>>>,
    #[allow(clippy::type_complexity)]
    shards: Arc<Mutex<BTreeMap<String, Arc<Mutex<DataShardTablet<FS>>>>>>,
    sessions: Arc<Mutex<BTreeMap<String, TxBuffer>>>,
    query_lock: Arc<Mutex<()>>,
    flush_threshold_rows: usize,
}

impl<FS: StorageFs> Clone for DatabaseEngine<FS> {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            pools: self.pools.clone(),
            shards: self.shards.clone(),
            sessions: self.sessions.clone(),
            query_lock: self.query_lock.clone(),
            flush_threshold_rows: self.flush_threshold_rows,
        }
    }
}

impl<FS: StorageFs> DatabaseEngine<FS> {
    pub fn has_session(&self, pid: u32, db: &str, subject: &str) -> bool {
        self.sessions.lock().unwrap().contains_key(&format!("{pid}:{db}:{subject}"))
    }
    pub fn new(meta: LocalMetaKv<FS>, flush_threshold_rows: usize) -> Self {
        Self {
            meta,
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            shards: Arc::new(Mutex::new(BTreeMap::new())),
            sessions: Arc::new(Mutex::new(BTreeMap::new())),
            query_lock: Arc::new(Mutex::new(())),
            flush_threshold_rows: flush_threshold_rows.max(16),
        }
    }

    pub fn register_pool(&self, name: impl Into<String>, striper: Arc<PoolStriper<FS>>) {
        self.pools.write().unwrap().insert(name.into(), striper);
    }

    pub fn has_pool(&self, name: &str) -> bool {
        self.pools.read().unwrap().contains_key(name)
    }

    fn get_pool(&self, name: &str) -> Result<Arc<PoolStriper<FS>>, DbError> {
        self.pools
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| DbError::PoolNotMounted(name.to_string()))
    }

    fn schema_key(project_id: u32, db: &str, table: &str) -> String {
        format!("/db/{project_id}/{db}/tables/{table}")
    }

    fn schema_prefix(project_id: u32, db: &str) -> String {
        format!("/db/{project_id}/{db}/tables/")
    }

    fn shard_meta_key(project_id: u32, db: &str, table: &str, shard_idx: u32) -> String {
        format!("/db/{project_id}/{db}/shards/{table}/{shard_idx:04}")
    }

    fn index_prefix_all(project_id: u32, db: &str, table: &str, idx_name: &str) -> String {
        format!("/db/{project_id}/{db}/idx/{table}/{idx_name}/")
    }

    fn index_prefix_value(
        project_id: u32,
        db: &str,
        table: &str,
        idx_name: &str,
        col_val: &SqlValue,
    ) -> Result<String, DbError> {
        let val_bytes = encode_nullable_index_key(col_val)?;
        Ok(format!(
            "/db/{project_id}/{db}/idx/{table}/{idx_name}/{}/",
            hex_encode(&val_bytes)
        ))
    }

    fn index_entry_key(
        project_id: u32,
        db: &str,
        table: &str,
        idx_name: &str,
        col_val: &SqlValue,
        pk_bytes: &[u8],
    ) -> Result<String, DbError> {
        let val_bytes = encode_nullable_index_key(col_val)?;
        Ok(format!(
            "/db/{project_id}/{db}/idx/{table}/{idx_name}/{}/{}",
            hex_encode(&val_bytes),
            hex_encode(pk_bytes)
        ))
    }

    fn shard_owner_id(db: &str, table: &str, shard_idx: u32) -> u32 {
        crc32fast::hash(format!("db:{db}/{table}/{shard_idx}").as_bytes()).max(1)
    }

    fn route_shard(pk_bytes: &[u8], shard_count: u32) -> u32 {
        crc32fast::hash(pk_bytes) % shard_count.max(1)
    }

    fn load_schema_with_revision(
        &self,
        project_id: u32,
        db: &str,
        table: &str,
    ) -> Result<(TableSchema, u64), DbError> {
        let key = Self::schema_key(project_id, db, table);
        let kv = self
            .meta
            .get(&key)
            .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;
        Ok((serde_json::from_slice(&kv.value)?, kv.mod_revision))
    }

    fn load_schema(&self, project_id: u32, db: &str, table: &str) -> Result<TableSchema, DbError> {
        Ok(self.load_schema_with_revision(project_id, db, table)?.0)
    }

    fn get_or_open_shard(
        &self,
        project_id: u32,
        db: &str,
        schema: &TableSchema,
        shard_idx: u32,
    ) -> Result<Arc<Mutex<DataShardTablet<FS>>>, DbError> {
        let mkey = Self::shard_meta_key(project_id, db, &schema.table, shard_idx);
        let current_revision = self.meta.get(&mkey).map(|kv| kv.mod_revision).unwrap_or(0);
        {
            let map = self.shards.lock().unwrap();
            if let Some(existing) = map.get(&mkey) {
                if existing.lock().unwrap().manifest_revision() == current_revision {
                    return Ok(existing.clone());
                }
            }
        }

        let wal_striper = self.get_pool(&schema.wal_pool)?;
        let data_striper = self.get_pool(&schema.data_pool)?;
        let owner_id = Self::shard_owner_id(db, &schema.table, shard_idx);

        let tablet = DataShardTablet::open_or_recover(
            self.meta.clone(),
            mkey.clone(),
            project_id,
            owner_id,
            schema.wal_pool.clone(),
            schema.data_pool.clone(),
            wal_striper,
            data_striper,
            self.flush_threshold_rows,
        )?;

        let handle = Arc::new(Mutex::new(tablet));
        let mut map = self.shards.lock().unwrap();
        map.insert(mkey, handle.clone());
        Ok(handle)
    }

    fn read_committed_row(
        &self,
        project_id: u32,
        db: &str,
        schema: &TableSchema,
        pk_bytes: &[u8],
    ) -> Result<Option<Vec<SqlValue>>, DbError> {
        let shard_idx = Self::route_shard(pk_bytes, schema.shards);
        let mkey = Self::shard_meta_key(project_id, db, &schema.table, shard_idx);
        let revision = self.meta.get(&mkey).map(|kv| kv.mod_revision).unwrap_or(0);
        if revision == 0 {
            return Ok(None);
        }
        if let Some(existing) = self.shards.lock().unwrap().get(&mkey).cloned() {
            let guard = existing.lock().unwrap();
            if guard.manifest_revision() == revision {
                return guard.get_row(pk_bytes);
            }
        }
        let wal_striper = self.get_pool(&schema.wal_pool)?;
        let data_striper = self.get_pool(&schema.data_pool)?;
        if let Some(ro) = DataShardTablet::open_readonly_snapshot(
            self.meta.clone(),
            mkey,
            wal_striper,
            data_striper,
        )? {
            ro.get_row(pk_bytes)
        } else {
            Ok(None)
        }
    }

    fn scan_committed_shard_rows(
        &self,
        project_id: u32,
        db: &str,
        schema: &TableSchema,
        shard_idx: u32,
    ) -> Result<Vec<Vec<SqlValue>>, DbError> {
        let mkey = Self::shard_meta_key(project_id, db, &schema.table, shard_idx);
        let revision = self.meta.get(&mkey).map(|kv| kv.mod_revision).unwrap_or(0);
        if revision == 0 {
            return Ok(Vec::new());
        }
        if let Some(existing) = self.shards.lock().unwrap().get(&mkey).cloned() {
            let guard = existing.lock().unwrap();
            if guard.manifest_revision() == revision {
                return guard.scan_all_live_rows();
            }
        }
        let wal_striper = self.get_pool(&schema.wal_pool)?;
        let data_striper = self.get_pool(&schema.data_pool)?;
        if let Some(ro) = DataShardTablet::open_readonly_snapshot(
            self.meta.clone(),
            mkey,
            wal_striper,
            data_striper,
        )? {
            ro.scan_all_live_rows()
        } else {
            Ok(Vec::new())
        }
    }

    fn read_row_in_tx(
        &self,
        project_id: u32,
        db: &str,
        schema: &TableSchema,
        pk_bytes: &[u8],
        tx: Option<&TxBuffer>,
    ) -> Result<Option<Vec<SqlValue>>, DbError> {
        if let Some(buf) = tx {
            if let Some(tbl_map) = buf.writes.get(&schema.table) {
                if let Some((_, cur)) = tbl_map.get(pk_bytes) {
                    return Ok(cur.clone());
                }
            }
        }
        self.read_committed_row(project_id, db, schema, pk_bytes)
    }

    /// Collects candidate rows using:
    /// 1) O(1) single-shard PK lookup when `WHERE <pk> = <val>` is present;
    /// 2) O(log N) Secondary Index lookup when `WHERE <indexed_col> = <val>` is present;
    /// 3) Full parallelizable shard scan + transaction overlay merge otherwise.
    fn collect_table_rows(
        &self,
        project_id: u32,
        db: &str,
        schema: &TableSchema,
        resolved_preds: &[(usize, Predicate)],
        tx: Option<&TxBuffer>,
    ) -> Result<Vec<Vec<SqlValue>>, DbError> {
        // 1. Exact PK equality fast path
        for (col_i, pred) in resolved_preds {
            if *col_i == schema.pk_index
                && pred.op == CmpOp::Eq
                && pred.value.check_type(schema.columns[*col_i].col_type, false)
            {
                let pk_bytes = pred.value.encode_order_preserving_key()?;
                let mut out = Vec::new();
                if let Some(row) = self.read_row_in_tx(project_id, db, schema, &pk_bytes, tx)? {
                    if resolved_preds.iter().all(|(ci, p)| p.eval(&row[*ci])) {
                        out.push(row);
                    }
                }
                return Ok(out);
            }
        }

        // 2. Secondary index equality fast path!
        let has_tx_overlay = tx
            .and_then(|b| b.writes.get(&schema.table))
            .is_some_and(|m| !m.is_empty());
        if !has_tx_overlay {
            for (col_i, pred) in resolved_preds {
                if pred.op == CmpOp::Eq {
                    if let Some(idx_def) = schema.indexes.iter().find(|ix| ix.col_index == *col_i) {
                        let prefix = Self::index_prefix_value(
                            project_id,
                            db,
                            &schema.table,
                            &idx_def.name,
                            &pred.value,
                        )?;
                        let idx_hits = self.meta.scan_prefix(&prefix);
                        let mut out = Vec::with_capacity(idx_hits.len());
                        for (_, kv) in idx_hits {
                            if let Some(row) =
                                self.read_committed_row(project_id, db, schema, &kv.value)?
                            {
                                if resolved_preds.iter().all(|(ci, p)| p.eval(&row[*ci])) {
                                    out.push(row);
                                }
                            }
                        }
                        let pk_i = schema.pk_index;
                        out.sort_by(|a, b| a[pk_i].sql_cmp(&b[pk_i]).unwrap_or(Ordering::Equal));
                        return Ok(out);
                    }
                }
            }
        }

        // 3. Full shard scan + uncommitted transaction overlay
        let mut by_pk: BTreeMap<Vec<u8>, Vec<SqlValue>> = BTreeMap::new();
        for shard_idx in 0..schema.shards {
            for row in self.scan_committed_shard_rows(project_id, db, schema, shard_idx)? {
                let pk = row[schema.pk_index].encode_order_preserving_key()?;
                by_pk.insert(pk, row);
            }
        }
        if let Some(buf) = tx {
            if let Some(tbl_map) = buf.writes.get(&schema.table) {
                for (pk, (_, cur_opt)) in tbl_map {
                    match cur_opt {
                        Some(r) => {
                            by_pk.insert(pk.clone(), r.clone());
                        }
                        None => {
                            by_pk.remove(pk);
                        }
                    }
                }
            }
        }

        let mut rows: Vec<Vec<SqlValue>> = by_pk
            .into_values()
            .filter(|row| resolved_preds.iter().all(|(ci, p)| p.eval(&row[*ci])))
            .collect();
        let pk_i = schema.pk_index;
        rows.sort_by(|a, b| a[pk_i].sql_cmp(&b[pk_i]).unwrap_or(Ordering::Equal));
        Ok(rows)
    }

    /// Atomically commits a `TxBuffer` across ANY number of tables, shards, and secondary indexes
    /// using immutable preparation followed by one atomic metadata transaction:
    /// - Phase 1: write immutable WAL batches to `walPool` for every touched shard;
    /// - Phase 2: `RevisionTxn` sealing all shard manifests + secondary index keys.
    fn commit_tx_buffer(
        &self,
        project_id: u32,
        db: &str,
        tx: TxBuffer,
    ) -> Result<(), DbError> {
        if tx.is_empty() {
            return Ok(());
        }

        let mut schemas: BTreeMap<String, (TableSchema, u64)> = BTreeMap::new();
        #[allow(clippy::type_complexity)]
        let mut shard_batches: BTreeMap<(String, u32), Vec<(Vec<u8>, Option<Vec<SqlValue>>)>> =
            BTreeMap::new();
        let mut index_ops: Vec<TxnOp> = Vec::new();

        for (table, pk_map) in tx.writes {
            if pk_map.is_empty() {
                continue;
            }
            let (schema, schema_revision) = self.load_schema_with_revision(project_id, db, &table)?;
            for (pk_bytes, (old_row, new_row)) in pk_map {
                if self.read_committed_row(project_id, db, &schema, &pk_bytes)? != old_row {
                    return Err(DbError::InvalidQuery("transaction write conflict; restart transaction".into()));
                }
                if old_row == new_row {
                    continue;
                }
                let shard_idx = Self::route_shard(&pk_bytes, schema.shards);
                shard_batches
                    .entry((table.clone(), shard_idx))
                    .or_default()
                    .push((pk_bytes.clone(), new_row.clone()));

                // Maintain all secondary indexes atomically!
                for idx in &schema.indexes {
                    if let Some(prev) = &old_row {
                        let old_ikey = Self::index_entry_key(
                            project_id,
                            db,
                            &table,
                            &idx.name,
                            &prev[idx.col_index],
                            &pk_bytes,
                        )?;
                        index_ops.push(TxnOp::Delete { key: old_ikey });
                    }
                    if let Some(next) = &new_row {
                        let new_ikey = Self::index_entry_key(
                            project_id,
                            db,
                            &table,
                            &idx.name,
                            &next[idx.col_index],
                            &pk_bytes,
                        )?;
                        index_ops.push(TxnOp::Put {
                            key: new_ikey,
                            value: pk_bytes.clone(),
                        });
                    }
                }
            }
            schemas.insert(table, (schema, schema_revision));
        }

        if shard_batches.is_empty() {
            return Ok(());
        }

        // Open all participating shards in deterministic `(table, shard_idx)` order to prevent deadlocks
        let mut shard_handles: Vec<((String, u32), Arc<Mutex<DataShardTablet<FS>>>)> =
            Vec::with_capacity(shard_batches.len());
        for (tbl, shard_idx) in shard_batches.keys() {
            let (schema, _) = &schemas[tbl];
            let handle = self.get_or_open_shard(project_id, db, schema, *shard_idx)?;
            shard_handles.push(((tbl.clone(), *shard_idx), handle));
        }

        // Phase 1: Prepare immutable WAL blobs on every touched shard
        let mut guards: Vec<RevisionGuard> = Vec::new();
        let mut ops: Vec<TxnOp> = Vec::new();
        let mut prepared_list: Vec<(Arc<Mutex<DataShardTablet<FS>>>, PreparedShardCommit)> =
            Vec::with_capacity(shard_handles.len());

        // Guard schema revisions so delete/recreate cannot satisfy a stale guard.
        for (tbl, (_, revision)) in &schemas {
            guards.push(RevisionGuard {
                key: Self::schema_key(project_id, db, tbl),
                expected_mod_revision: *revision,
            });
        }

        for (key_pair, handle) in shard_handles {
            let batch = shard_batches.remove(&key_pair).unwrap();
            let prep = {
                let guard = handle.lock().unwrap();
                guard.prepare_batch(batch)?
            };
            guards.push(RevisionGuard {
                key: prep.meta_key.clone(),
                expected_mod_revision: prep.expected_mod_revision,
            });
            ops.push(TxnOp::Put {
                key: prep.meta_key.clone(),
                value: prep.encoded_manifest.clone(),
            });
            prepared_list.push((handle, prep));
        }

        ops.extend(index_ops);

        // Phase 2: Single atomic Raft transaction across all shard manifests & secondary indexes!
        let revision = match self.meta.execute(KvCommand::RevisionTxn { guards, ops })? {
            KvResult::TxnOk { revision, .. } => revision,
            KvResult::CasConflict { .. } => {
                let first_owner = prepared_list
                    .first()
                    .map(|(h, _)| h.lock().unwrap().owner_id())
                    .unwrap_or(0);
                return Err(DbError::StaleTabletGeneration {
                    owner_id: first_owner,
                });
            }
            _ => unreachable!(),
        };

        // Finalize in-memory MemTables & manifest revisions.
        for (handle, prep) in prepared_list {
            let mut guard = handle.lock().unwrap();
            guard.apply_prepared(prep, revision)?;
        }

        Ok(())
    }

    /// Executes a SQL script (single statement or semicolon-separated `BEGIN ... COMMIT` script)
    /// under an optional interactive session key (e.g. `subject`).
    pub fn execute_sql(
        &self,
        project_id: u32,
        db: &str,
        wal_pool: &str,
        data_pool: &str,
        default_shards: u32,
        sql: &str,
    ) -> Result<QueryResult, DbError> {
        self.execute_sql_with_session(project_id, db, wal_pool, data_pool, default_shards, None, sql)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn execute_sql_with_session(
        &self,
        project_id: u32,
        db: &str,
        wal_pool: &str,
        data_pool: &str,
        default_shards: u32,
        session_subject: Option<&str>,
        sql: &str,
    ) -> Result<QueryResult, DbError> {
        let stmts = parse_sql_script(sql)?;
        let _serial = self.query_lock.lock().unwrap();
        let session_key = session_subject.map(|subj| format!("{project_id}:{db}:{subj}"));

        let mut active_tx: Option<TxBuffer> = if let Some(sk) = &session_key {
            self.sessions.lock().unwrap().remove(sk)
        } else {
            None
        };

        let mut last_result = QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
        };
        let mut cumulative_affected = 0u64;

        for stmt in stmts {
            match self.execute_one_in_ctx(
                project_id,
                db,
                wal_pool,
                data_pool,
                default_shards,
                &mut active_tx,
                stmt,
            ) {
                Ok(res) => {
                    cumulative_affected += res.rows_affected;
                    last_result = res;
                }
                Err(err) => {
                    // Any error aborts an active transaction cleanly without partial writes!
                    return Err(err);
                }
            }
        }

        // Persist unfinished interactive `BEGIN` transaction in session table if a session subject was given;
        // if no session subject was given (stateless call) and caller forgot `COMMIT`, do NOT leak uncommitted state.
        if let (Some(sk), Some(tx_buf)) = (session_key, active_tx) {
            self.sessions.lock().unwrap().insert(sk, tx_buf);
        }

        if last_result.columns.is_empty() && cumulative_affected > last_result.rows_affected {
            last_result.rows_affected = cumulative_affected;
        }
        Ok(last_result)
    }

    pub fn execute_stmt(
        &self,
        project_id: u32,
        db: &str,
        wal_pool: &str,
        data_pool: &str,
        default_shards: u32,
        stmt: SqlStmt,
    ) -> Result<QueryResult, DbError> {
        let _serial = self.query_lock.lock().unwrap();
        let mut active_tx = None;
        self.execute_one_in_ctx(
            project_id,
            db,
            wal_pool,
            data_pool,
            default_shards,
            &mut active_tx,
            stmt,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_one_in_ctx(
        &self,
        project_id: u32,
        db: &str,
        wal_pool: &str,
        data_pool: &str,
        default_shards: u32,
        active_tx: &mut Option<TxBuffer>,
        stmt: SqlStmt,
    ) -> Result<QueryResult, DbError> {
        match stmt {
            SqlStmt::Begin => {
                if active_tx.is_some() {
                    return Err(DbError::InvalidQuery(
                        "transaction is already active (COMMIT or ROLLBACK first)".into(),
                    ));
                }
                *active_tx = Some(TxBuffer::default());
                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: 0,
                })
            }

            SqlStmt::Commit => {
                let Some(tx_buf) = active_tx.take() else {
                    return Err(DbError::InvalidQuery(
                        "COMMIT without an active BEGIN transaction".into(),
                    ));
                };
                self.commit_tx_buffer(project_id, db, tx_buf)?;
                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: 0,
                })
            }

            SqlStmt::Rollback => {
                let _ = active_tx.take();
                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: 0,
                })
            }

            SqlStmt::ShowTables => {
                let prefix = Self::schema_prefix(project_id, db);
                let items = self.meta.scan_prefix(&prefix);
                let mut rows = Vec::with_capacity(items.len());
                for (_, kv) in items {
                    let sc: TableSchema = serde_json::from_slice(&kv.value)?;
                    rows.push(vec![
                        SqlValue::Text(sc.table),
                        SqlValue::Int(sc.shards as i64),
                        SqlValue::Text(sc.wal_pool),
                        SqlValue::Text(sc.data_pool),
                        SqlValue::Int(sc.indexes.len() as i64),
                    ]);
                }
                Ok(QueryResult {
                    columns: vec![
                        "table".into(),
                        "shards".into(),
                        "wal_pool".into(),
                        "data_pool".into(),
                        "indexes".into(),
                    ],
                    rows,
                    rows_affected: 0,
                })
            }

            SqlStmt::ShowIndexes { table } => {
                let schema = self.load_schema(project_id, db, &table)?;
                let rows = schema
                    .indexes
                    .into_iter()
                    .map(|ix| {
                        vec![
                            SqlValue::Text(ix.name),
                            SqlValue::Text(schema.table.clone()),
                            SqlValue::Text(ix.column),
                        ]
                    })
                    .collect();
                Ok(QueryResult {
                    columns: vec!["index_name".into(), "table".into(), "column".into()],
                    rows,
                    rows_affected: 0,
                })
            }

            SqlStmt::CreateTable {
                table,
                if_not_exists,
                columns,
            } => {
                if columns.is_empty() {
                    return Err(DbError::InvalidQuery(
                        "CREATE TABLE requires at least one column".into(),
                    ));
                }
                let pk_positions: Vec<usize> = columns
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, c)| c.primary_key.then_some(idx))
                    .collect();
                if pk_positions.len() != 1 {
                    return Err(DbError::InvalidQuery(
                        "CREATE TABLE must declare exactly one PRIMARY KEY column".into(),
                    ));
                }

                let _ = self.get_pool(wal_pool)?;
                let _ = self.get_pool(data_pool)?;

                let schema = TableSchema {
                    table: table.clone(),
                    columns,
                    pk_index: pk_positions[0],
                    shards: default_shards.max(1),
                    wal_pool: wal_pool.to_string(),
                    data_pool: data_pool.to_string(),
                    indexes: Vec::new(),
                };
                let key = Self::schema_key(project_id, db, &table);
                let encoded = serde_json::to_vec(&schema)?;

                match self.meta.execute(KvCommand::CompareAndSwap {
                    key,
                    expected_version: 0,
                    new_value: Some(encoded),
                })? {
                    KvResult::CasOk { .. } => Ok(QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        rows_affected: 0,
                    }),
                    KvResult::CasConflict { .. } if if_not_exists => Ok(QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        rows_affected: 0,
                    }),
                    KvResult::CasConflict { .. } => Err(DbError::TableAlreadyExists(table)),
                    _ => unreachable!(),
                }
            }

            SqlStmt::CreateIndex {
                index_name,
                if_not_exists,
                table,
                column,
            } => {
                if active_tx.is_some() {
                    return Err(DbError::InvalidQuery(
                        "CREATE INDEX cannot run inside an open transaction".into(),
                    ));
                }
                let (mut schema, schema_revision) = self.load_schema_with_revision(project_id, db, &table)?;
                if schema
                    .indexes
                    .iter()
                    .any(|ix| ix.name.eq_ignore_ascii_case(&index_name))
                {
                    if if_not_exists {
                        return Ok(QueryResult {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            rows_affected: 0,
                        });
                    }
                    return Err(DbError::IndexAlreadyExists(index_name, table));
                }
                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&column))
                    .ok_or_else(|| {
                        DbError::InvalidQuery(format!(
                            "unknown column `{column}` in table `{table}`"
                        ))
                    })?;

                // Backfill secondary index from all live rows across shards!
                let live_rows = self.collect_table_rows(project_id, db, &schema, &[], None)?;
                let mut ops = Vec::with_capacity(live_rows.len() + 1);
                let indexed_count = live_rows.len() as u64;

                for row in &live_rows {
                    let pk_bytes = row[schema.pk_index].encode_order_preserving_key()?;
                    let ikey = Self::index_entry_key(
                        project_id,
                        db,
                        &table,
                        &index_name,
                        &row[col_idx],
                        &pk_bytes,
                    )?;
                    ops.push(TxnOp::Put {
                        key: ikey,
                        value: pk_bytes,
                    });
                }

                schema.indexes.push(IndexDef {
                    name: index_name,
                    column: schema.columns[col_idx].name.clone(),
                    col_index: col_idx,
                });
                let schema_key = Self::schema_key(project_id, db, &table);
                ops.push(TxnOp::Put {
                    key: schema_key.clone(),
                    value: serde_json::to_vec(&schema)?,
                });

                match self.meta.execute(KvCommand::RevisionTxn {
                    guards: vec![RevisionGuard {
                        key: schema_key,
                        expected_mod_revision: schema_revision,
                    }],
                    ops,
                })? {
                    KvResult::TxnOk { .. } => Ok(QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        rows_affected: indexed_count,
                    }),
                    KvResult::CasConflict { .. } => Err(DbError::InvalidQuery(
                        "concurrent schema modification during CREATE INDEX".into(),
                    )),
                    _ => unreachable!(),
                }
            }

            SqlStmt::DropIndex {
                index_name,
                if_exists,
                table,
            } => {
                if active_tx.is_some() {
                    return Err(DbError::InvalidQuery(
                        "DROP INDEX cannot run inside an open transaction".into(),
                    ));
                }
                let (mut schema, schema_revision) = self.load_schema_with_revision(project_id, db, &table)?;
                let Some(pos) = schema
                    .indexes
                    .iter()
                    .position(|ix| ix.name.eq_ignore_ascii_case(&index_name))
                else {
                    if if_exists {
                        return Ok(QueryResult {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            rows_affected: 0,
                        });
                    }
                    return Err(DbError::IndexNotFound(index_name, table));
                };
                let removed = schema.indexes.remove(pos);
                let prefix = Self::index_prefix_all(project_id, db, &table, &removed.name);
                let existing_keys = self.meta.scan_prefix(&prefix);
                let mut ops: Vec<TxnOp> = existing_keys
                    .into_iter()
                    .map(|(k, _)| TxnOp::Delete { key: k })
                    .collect();

                let schema_key = Self::schema_key(project_id, db, &table);
                ops.push(TxnOp::Put {
                    key: schema_key.clone(),
                    value: serde_json::to_vec(&schema)?,
                });

                match self.meta.execute(KvCommand::RevisionTxn {
                    guards: vec![RevisionGuard {
                        key: schema_key,
                        expected_mod_revision: schema_revision,
                    }],
                    ops,
                })? {
                    KvResult::TxnOk { .. } => (),
                    KvResult::CasConflict { .. } => return Err(DbError::InvalidQuery(
                        "concurrent schema modification during DROP INDEX".into())),
                    _ => unreachable!(),
                }

                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: 0,
                })
            }

            SqlStmt::Insert {
                table,
                upsert,
                columns: col_list,
                rows,
            } => {
                let schema = self.load_schema(project_id, db, &table)?;
                let col_mapping: Vec<usize> = if let Some(names) = col_list {
                    let mut idxs = Vec::with_capacity(names.len());
                    for name in &names {
                        let pos = schema
                            .columns
                            .iter()
                            .position(|c| c.name.eq_ignore_ascii_case(name))
                            .ok_or_else(|| {
                                DbError::InvalidQuery(format!(
                                    "unknown column `{name}` in table `{table}`"
                                ))
                            })?;
                        idxs.push(pos);
                    }
                    idxs
                } else {
                    (0..schema.columns.len()).collect()
                };

                let is_autocommit = active_tx.is_none();
                let mut working_tx = active_tx.take().unwrap_or_default();
                let mut total_written = 0u64;

                for raw_row in rows {
                    if raw_row.len() != col_mapping.len() {
                        return Err(DbError::InvalidQuery(format!(
                            "expected {} values, got {}",
                            col_mapping.len(),
                            raw_row.len()
                        )));
                    }
                    let mut full_row = vec![SqlValue::Null; schema.columns.len()];
                    for (val, &dst_idx) in raw_row.into_iter().zip(col_mapping.iter()) {
                        let col_def = &schema.columns[dst_idx];
                        if !val.check_type(col_def.col_type, !col_def.primary_key) {
                            return Err(DbError::InvalidQuery(format!(
                                "type mismatch for column `{}` (expected {:?})",
                                col_def.name, col_def.col_type
                            )));
                        }
                        full_row[dst_idx] = val;
                    }

                    let pk_bytes = full_row[schema.pk_index].encode_order_preserving_key()?;
                    let existing = self.read_row_in_tx(
                        project_id,
                        db,
                        &schema,
                        &pk_bytes,
                        Some(&working_tx),
                    )?;
                    if !upsert && existing.is_some() {
                        return Err(DbError::DuplicatePrimaryKey(table));
                    }
                    let base_committed = if working_tx
                        .writes
                        .get(&table)
                        .is_some_and(|m| m.contains_key(&pk_bytes))
                    {
                        None
                    } else {
                        existing
                    };
                    working_tx.record_mutation(&table, pk_bytes, base_committed, Some(full_row));
                    total_written += 1;
                }

                if is_autocommit {
                    self.commit_tx_buffer(project_id, db, working_tx)?;
                } else {
                    *active_tx = Some(working_tx);
                }

                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: total_written,
                })
            }

            SqlStmt::Update {
                table,
                assignments,
                predicates,
            } => {
                let schema = self.load_schema(project_id, db, &table)?;
                let resolved_preds = Self::resolve_predicates(&schema, predicates, "UPDATE WHERE")?;

                // Validate assignments up front
                let mut resolved_assigns: Vec<(usize, AssignExpr)> =
                    Vec::with_capacity(assignments.len());
                for (col_name, expr) in assignments {
                    let dst_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&col_name))
                        .ok_or_else(|| {
                            DbError::InvalidQuery(format!(
                                "unknown column `{col_name}` in UPDATE SET"
                            ))
                        })?;
                    resolved_assigns.push((dst_idx, expr));
                }

                let is_autocommit = active_tx.is_none();
                let mut working_tx = active_tx.take().unwrap_or_default();

                let matched_rows = self.collect_table_rows(
                    project_id,
                    db,
                    &schema,
                    &resolved_preds,
                    Some(&working_tx),
                )?;
                let mut updated_count = 0u64;

                for old_row in matched_rows {
                    let old_pk = old_row[schema.pk_index].encode_order_preserving_key()?;
                    let mut new_row = old_row.clone();

                    for (dst_idx, expr) in &resolved_assigns {
                        let new_val = Self::eval_assign_expr(&schema, &old_row, expr)?;
                        let col_def = &schema.columns[*dst_idx];
                        if !new_val.check_type(col_def.col_type, !col_def.primary_key) {
                            return Err(DbError::InvalidQuery(format!(
                                "type mismatch assigning to column `{}` (expected {:?})",
                                col_def.name, col_def.col_type
                            )));
                        }
                        new_row[*dst_idx] = new_val;
                    }

                    let new_pk = new_row[schema.pk_index].encode_order_preserving_key()?;
                    if new_pk == old_pk {
                        let base = self.read_committed_row(project_id, db, &schema, &old_pk)?;
                        working_tx.record_mutation(&table, old_pk, base, Some(new_row));
                    } else {
                        // Primary key changed: ensure target PK does not collide, delete old PK & insert new PK
                        if self
                            .read_row_in_tx(project_id, db, &schema, &new_pk, Some(&working_tx))?
                            .is_some()
                        {
                            return Err(DbError::DuplicatePrimaryKey(table));
                        }
                        let old_base =
                            self.read_committed_row(project_id, db, &schema, &old_pk)?;
                        let new_base =
                            self.read_committed_row(project_id, db, &schema, &new_pk)?;
                        working_tx.record_mutation(&table, old_pk, old_base, None);
                        working_tx.record_mutation(&table, new_pk, new_base, Some(new_row));
                    }
                    updated_count += 1;
                }

                if is_autocommit {
                    self.commit_tx_buffer(project_id, db, working_tx)?;
                } else {
                    *active_tx = Some(working_tx);
                }

                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: updated_count,
                })
            }

            SqlStmt::Select {
                table,
                projection,
                predicates,
                group_by,
                order_by,
                limit,
            } => {
                let schema = self.load_schema(project_id, db, &table)?;
                let resolved_preds = Self::resolve_predicates(&schema, predicates, "WHERE")?;
                let candidate_rows = self.collect_table_rows(
                    project_id,
                    db,
                    &schema,
                    &resolved_preds,
                    active_tx.as_ref(),
                )?;

                let items: Vec<SelectItem> = match projection {
                    Projection::All => vec![SelectItem::Wildcard],
                    Projection::Columns(cols) => cols
                        .into_iter()
                        .map(|name| SelectItem::Column { name, alias: None })
                        .collect(),
                    Projection::Items(it) => it,
                };

                let is_agg = !group_by.is_empty()
                    || items
                        .iter()
                        .any(|it| matches!(it, SelectItem::Aggregate { .. }));

                if is_agg {
                    return Self::eval_aggregate_select(
                        &schema,
                        candidate_rows,
                        &items,
                        &group_by,
                        order_by,
                        limit,
                    );
                }

                let mut rows = candidate_rows;

                // Resolve non-aggregate projection columns & optional aliases
                let mut out_cols = Vec::new();
                let mut proj_idxs = Vec::new();
                for item in items {
                    match item {
                        SelectItem::Wildcard => {
                            for (i, c) in schema.columns.iter().enumerate() {
                                out_cols.push(c.name.clone());
                                proj_idxs.push(i);
                            }
                        }
                        SelectItem::Column { name, alias } => {
                            let pos = schema
                                .columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(&name))
                                .ok_or_else(|| {
                                    DbError::InvalidQuery(format!(
                                        "unknown column `{name}` in SELECT list"
                                    ))
                                })?;
                            out_cols.push(alias.unwrap_or_else(|| schema.columns[pos].name.clone()));
                            proj_idxs.push(pos);
                        }
                        SelectItem::Aggregate { .. } => unreachable!(),
                    }
                }

                if let Some((ord_col, desc)) = order_by {
                    if let Some(schema_pos) = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ord_col))
                    {
                        rows.sort_by(|a, b| {
                            let cmp = a[schema_pos]
                                .sql_cmp(&b[schema_pos])
                                .unwrap_or(Ordering::Equal);
                            if desc { cmp.reverse() } else { cmp }
                        });
                    } else if let Some(alias_pos) = out_cols
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(&ord_col))
                    {
                        let src_idx = proj_idxs[alias_pos];
                        rows.sort_by(|a, b| {
                            let cmp = a[src_idx]
                                .sql_cmp(&b[src_idx])
                                .unwrap_or(Ordering::Equal);
                            if desc { cmp.reverse() } else { cmp }
                        });
                    } else {
                        return Err(DbError::InvalidQuery(format!(
                            "unknown column `{ord_col}` in ORDER BY"
                        )));
                    }
                }

                if let Some(max_rows) = limit {
                    rows.truncate(max_rows);
                }

                let projected_rows = rows
                    .into_iter()
                    .map(|row| proj_idxs.iter().map(|&i| row[i].clone()).collect())
                    .collect();

                Ok(QueryResult {
                    columns: out_cols,
                    rows: projected_rows,
                    rows_affected: 0,
                })
            }

            SqlStmt::Delete { table, predicates } => {
                let schema = self.load_schema(project_id, db, &table)?;
                let resolved_preds = Self::resolve_predicates(&schema, predicates, "DELETE WHERE")?;

                let is_autocommit = active_tx.is_none();
                let mut working_tx = active_tx.take().unwrap_or_default();

                let matched = self.collect_table_rows(
                    project_id,
                    db,
                    &schema,
                    &resolved_preds,
                    Some(&working_tx),
                )?;
                let mut deleted = 0u64;

                for row in matched {
                    let pk_bytes = row[schema.pk_index].encode_order_preserving_key()?;
                    let base = self.read_committed_row(project_id, db, &schema, &pk_bytes)?;
                    working_tx.record_mutation(&table, pk_bytes, base, None);
                    deleted += 1;
                }

                if is_autocommit {
                    self.commit_tx_buffer(project_id, db, working_tx)?;
                } else {
                    *active_tx = Some(working_tx);
                }

                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: deleted,
                })
            }

            SqlStmt::FlushTable { table } => {
                let schema = self.load_schema(project_id, db, &table)?;
                let mut flushed_shards = 0u64;
                for shard_idx in 0..schema.shards {
                    let shard = self.get_or_open_shard(project_id, db, &schema, shard_idx)?;
                    let mut guard = shard.lock().unwrap();
                    if guard.flush_to_sstable()? {
                        flushed_shards += 1;
                    }
                }
                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: flushed_shards,
                })
            }

            SqlStmt::CompactTable { table } => {
                if active_tx.is_some() {
                    return Err(DbError::InvalidQuery(
                        "COMPACT TABLE cannot run inside an open transaction".into(),
                    ));
                }
                let schema = self.load_schema(project_id, db, &table)?;
                let mut compacted_shards = 0u64;
                let mut total_merged_ssts = 0u64;
                let mut total_live_rows = 0u64;
                let mut total_evicted_frags = 0u64;

                for shard_idx in 0..schema.shards {
                    let mkey = Self::shard_meta_key(project_id, db, &table, shard_idx);
                    if self.meta.get(&mkey).is_none()
                        && !self.shards.lock().unwrap().contains_key(&mkey)
                    {
                        continue;
                    }
                    let shard = self.get_or_open_shard(project_id, db, &schema, shard_idx)?;
                    let mut guard = shard.lock().unwrap();
                    let rep = guard.compact()?;
                    if rep.merged_ssts > 0 || rep.evicted_fragments > 0 || rep.live_rows > 0 {
                        compacted_shards += 1;
                        total_merged_ssts += rep.merged_ssts as u64;
                        total_live_rows += rep.live_rows as u64;
                        total_evicted_frags += rep.evicted_fragments as u64;
                    }
                }

                Ok(QueryResult {
                    columns: vec![
                        "compacted_shards".into(),
                        "merged_sstables".into(),
                        "live_rows".into(),
                        "evicted_fragments".into(),
                    ],
                    rows: vec![vec![
                        SqlValue::Int(compacted_shards as i64),
                        SqlValue::Int(total_merged_ssts as i64),
                        SqlValue::Int(total_live_rows as i64),
                        SqlValue::Int(total_evicted_frags as i64),
                    ]],
                    rows_affected: compacted_shards,
                })
            }
        }
    }

    fn resolve_predicates(
        schema: &TableSchema,
        predicates: Vec<Predicate>,
        clause: &str,
    ) -> Result<Vec<(usize, Predicate)>, DbError> {
        let mut out = Vec::with_capacity(predicates.len());
        for pred in predicates {
            let idx = schema
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&pred.column))
                .ok_or_else(|| {
                    DbError::InvalidQuery(format!(
                        "unknown column `{}` in {clause}",
                        pred.column
                    ))
                })?;
            out.push((idx, pred));
        }
        Ok(out)
    }

    fn eval_assign_expr(
        schema: &TableSchema,
        row: &[SqlValue],
        expr: &AssignExpr,
    ) -> Result<SqlValue, DbError> {
        match expr {
            AssignExpr::Literal(v) => Ok(v.clone()),
            AssignExpr::Column(col_name) => {
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| {
                        DbError::InvalidQuery(format!(
                            "unknown source column `{col_name}` in UPDATE SET"
                        ))
                    })?;
                Ok(row[idx].clone())
            }
            AssignExpr::BinOp { column, op, rhs } => {
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(column))
                    .ok_or_else(|| {
                        DbError::InvalidQuery(format!(
                            "unknown column `{column}` in UPDATE arithmetic expression"
                        ))
                    })?;
                match &row[idx] {
                    SqlValue::Int(cur) => {
                        let next = match op {
                            ArithOp::Add => cur.checked_add(*rhs),
                            ArithOp::Sub => cur.checked_sub(*rhs),
                            ArithOp::Mul => cur.checked_mul(*rhs),
                        }
                        .ok_or_else(|| {
                            DbError::InvalidQuery("integer overflow in UPDATE SET".into())
                        })?;
                        Ok(SqlValue::Int(next))
                    }
                    SqlValue::Null => Ok(SqlValue::Null),
                    _ => Err(DbError::InvalidQuery(format!(
                        "column `{column}` is not INT for arithmetic expression"
                    ))),
                }
            }
        }
    }

    fn eval_aggregate_select(
        schema: &TableSchema,
        candidate_rows: Vec<Vec<SqlValue>>,
        items: &[SelectItem],
        group_by: &[String],
        order_by: Option<(String, bool)>,
        limit: Option<usize>,
    ) -> Result<QueryResult, DbError> {
        let mut group_idxs = Vec::with_capacity(group_by.len());
        let mut group_idx_set = BTreeSet::new();
        for gcol in group_by {
            let pos = schema
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(gcol))
                .ok_or_else(|| {
                    DbError::InvalidQuery(format!("unknown column `{gcol}` in GROUP BY"))
                })?;
            group_idxs.push(pos);
            group_idx_set.insert(pos);
        }

        // Validate projection items against GROUP BY rules
        let mut out_cols = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Wildcard => {
                    return Err(DbError::InvalidQuery(
                        "cannot use `*` with aggregate functions or GROUP BY".into(),
                    ));
                }
                SelectItem::Column { name, alias } => {
                    let pos = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(name))
                        .ok_or_else(|| {
                            DbError::InvalidQuery(format!("unknown column `{name}` in SELECT"))
                        })?;
                    if !group_idx_set.contains(&pos) {
                        return Err(DbError::InvalidQuery(format!(
                            "column `{name}` must appear in GROUP BY or be wrapped in an aggregate function"
                        )));
                    }
                    out_cols.push(alias.clone().unwrap_or_else(|| schema.columns[pos].name.clone()));
                }
                SelectItem::Aggregate {
                    func,
                    column,
                    alias,
                } => {
                    if let Some(cname) = column {
                        let pos = schema
                            .columns
                            .iter()
                            .position(|c| c.name.eq_ignore_ascii_case(cname))
                            .ok_or_else(|| {
                                DbError::InvalidQuery(format!(
                                    "unknown column `{cname}` in {}()",
                                    func.name()
                                ))
                            })?;
                        if matches!(func, AggFunc::Sum | AggFunc::Avg)
                            && schema.columns[pos].col_type != ColumnType::Int
                        {
                            return Err(DbError::InvalidQuery(format!(
                                "{}() requires an INT column, `{cname}` is {:?}",
                                func.name(),
                                schema.columns[pos].col_type
                            )));
                        }
                        out_cols.push(
                            alias
                                .clone()
                                .unwrap_or_else(|| format!("{}({})", func.name(), cname)),
                        );
                    } else {
                        out_cols.push(alias.clone().unwrap_or_else(|| "COUNT(*)".into()));
                    }
                }
            }
        }

        // Partition rows into deterministic ordered groups
        #[allow(clippy::type_complexity)]
        let mut groups: BTreeMap<Vec<u8>, (Vec<SqlValue>, Vec<Vec<SqlValue>>)> = BTreeMap::new();
        if group_idxs.is_empty() {
            groups.insert(Vec::new(), (Vec::new(), candidate_rows));
        } else {
            for row in candidate_rows {
                let mut gkey_bytes = Vec::new();
                let mut gvals = Vec::with_capacity(group_idxs.len());
                for &gi in &group_idxs {
                    let enc = encode_nullable_index_key(&row[gi])?;
                    gkey_bytes.extend_from_slice(&(enc.len() as u32).to_be_bytes());
                    gkey_bytes.extend_from_slice(&enc);
                    gvals.push(row[gi].clone());
                }
                groups
                    .entry(gkey_bytes)
                    .or_insert_with(|| (gvals, Vec::new()))
                    .1
                    .push(row);
            }
        }

        let mut out_rows: Vec<Vec<SqlValue>> = Vec::with_capacity(groups.len());
        for (_, (_gvals, grows)) in groups {
            let mut out_row = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    SelectItem::Column { name, .. } => {
                        let pos = schema
                            .columns
                            .iter()
                            .position(|c| c.name.eq_ignore_ascii_case(name))
                            .unwrap();
                        let val = grows
                            .first()
                            .map(|r| r[pos].clone())
                            .unwrap_or(SqlValue::Null);
                        out_row.push(val);
                    }
                    SelectItem::Aggregate { func, column, .. } => {
                        let col_pos = column.as_ref().map(|cname| {
                            schema
                                .columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(cname))
                                .unwrap()
                        });
                        let val = match func {
                            AggFunc::Count => {
                                let cnt = match col_pos {
                                    None => grows.len() as i64,
                                    Some(ci) => grows
                                        .iter()
                                        .filter(|r| !matches!(r[ci], SqlValue::Null))
                                        .count() as i64,
                                };
                                SqlValue::Int(cnt)
                            }
                            AggFunc::Sum => {
                                let ci = col_pos.unwrap();
                                let mut sum = 0i64;
                                let mut any = false;
                                for r in &grows {
                                    if let SqlValue::Int(v) = r[ci] {
                                        sum = sum.saturating_add(v);
                                        any = true;
                                    }
                                }
                                if any { SqlValue::Int(sum) } else { SqlValue::Null }
                            }
                            AggFunc::Avg => {
                                let ci = col_pos.unwrap();
                                let mut sum: i128 = 0;
                                let mut cnt: i128 = 0;
                                for r in &grows {
                                    if let SqlValue::Int(v) = r[ci] {
                                        sum += v as i128;
                                        cnt += 1;
                                    }
                                }
                                if cnt > 0 {
                                    SqlValue::Int((sum / cnt) as i64)
                                } else {
                                    SqlValue::Null
                                }
                            }
                            AggFunc::Min => {
                                let ci = col_pos.unwrap();
                                let mut best: Option<SqlValue> = None;
                                for r in &grows {
                                    if matches!(r[ci], SqlValue::Null) {
                                        continue;
                                    }
                                    best = Some(match best {
                                        None => r[ci].clone(),
                                        Some(cur) => {
                                            if r[ci].sql_cmp(&cur) == Some(Ordering::Less) {
                                                r[ci].clone()
                                            } else {
                                                cur
                                            }
                                        }
                                    });
                                }
                                best.unwrap_or(SqlValue::Null)
                            }
                            AggFunc::Max => {
                                let ci = col_pos.unwrap();
                                let mut best: Option<SqlValue> = None;
                                for r in &grows {
                                    if matches!(r[ci], SqlValue::Null) {
                                        continue;
                                    }
                                    best = Some(match best {
                                        None => r[ci].clone(),
                                        Some(cur) => {
                                            if r[ci].sql_cmp(&cur) == Some(Ordering::Greater) {
                                                r[ci].clone()
                                            } else {
                                                cur
                                            }
                                        }
                                    });
                                }
                                best.unwrap_or(SqlValue::Null)
                            }
                        };
                        out_row.push(val);
                    }
                    SelectItem::Wildcard => unreachable!(),
                }
            }
            out_rows.push(out_row);
        }

        if let Some((ord_col, desc)) = order_by {
            let ord_pos = out_cols
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&ord_col))
                .ok_or_else(|| {
                    DbError::InvalidQuery(format!(
                        "unknown output column or alias `{ord_col}` in ORDER BY"
                    ))
                })?;
            out_rows.sort_by(|a, b| {
                let cmp = a[ord_pos].sql_cmp(&b[ord_pos]).unwrap_or(Ordering::Equal);
                if desc { cmp.reverse() } else { cmp }
            });
        }

        if let Some(max_rows) = limit {
            out_rows.truncate(max_rows);
        }

        Ok(QueryResult {
            columns: out_cols,
            rows: out_rows,
            rows_affected: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yadb_blob::{ErasureScheme, VolumeConfig, VolumeStore};
    use yadb_io::{SimFs, StorageFs};

    fn make_pools(fs: &SimFs) -> (Arc<PoolStriper<SimFs>>, Arc<PoolStriper<SimFs>>) {
        let mut wal_doms = Vec::new();
        for i in 0..3 {
            wal_doms.push(
                VolumeStore::open(fs.clone(), format!("wal_{i}"), VolumeConfig::default()).unwrap(),
            );
        }
        let wal = Arc::new(
            PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, wal_doms).unwrap(),
        );

        let mut data_doms = Vec::new();
        for i in 0..6 {
            data_doms.push(
                VolumeStore::open(fs.clone(), format!("data_{i}"), VolumeConfig::default())
                    .unwrap(),
            );
        }
        let data = Arc::new(
            PoolStriper::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }, data_doms).unwrap(),
        );

        (wal, data)
    }

    #[test]
    fn shard_cache_rejects_manifest_delete_recreate_aba() {
        let fs = SimFs::new(9200);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let (wal, data) = make_pools(&fs);
        let db = DatabaseEngine::new(meta.clone(), 1024);
        db.register_pool("wal", wal);
        db.register_pool("data", data);
        db.execute_sql(1, "db", "wal", "data", 1,
            "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1,10);").unwrap();
        let key = DatabaseEngine::<SimFs>::shard_meta_key(1, "db", "t", 0);
        let old = meta.get(&key).unwrap();
        let mut replacement: TabletManifest = serde_json::from_slice(&old.value).unwrap();
        replacement.wal_blobs.clear();
        replacement.sst_blobs.clear();
        meta.execute(KvCommand::Delete { key: key.clone() }).unwrap();
        for _ in 0..old.version {
            meta.execute(KvCommand::Put { key: key.clone(), value: serde_json::to_vec(&replacement).unwrap() }).unwrap();
        }
        assert_eq!(meta.get(&key).unwrap().version, old.version);
        assert_ne!(meta.get(&key).unwrap().mod_revision, old.mod_revision);
        let schema = db.load_schema(1, "db", "t").unwrap();
        let pk = SqlValue::Int(1).encode_order_preserving_key().unwrap();
        assert!(db.read_committed_row(1, "db", &schema, &pk).unwrap().is_none());
        assert!(db.scan_committed_shard_rows(1, "db", &schema, 0).unwrap().is_empty());
        let handle = db.get_or_open_shard(1, "db", &schema, 0).unwrap();
        assert!(handle.lock().unwrap().get_row(&pk).unwrap().is_none());
    }

    #[test]
    fn sharded_sql_wal_sstable_flush_and_crash_recovery_with_disk_loss() {
        let fs = SimFs::new(20261112);

        {
            let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
            let (wal, data) = make_pools(&fs);
            let db = DatabaseEngine::new(meta, 16);
            db.register_pool("wal-fast", wal);
            db.register_pool("data-ec", data);

            db.execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT, balance INT, active BOOL)",
            )
            .unwrap();

            db.execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "INSERT INTO accounts VALUES (101, 'alice', 5000, true), (102, 'bob', 1200, true), (103, 'carol', 9300, false)",
            )
            .unwrap();

            let fl = db
                .execute_sql(1, "ledger", "wal-fast", "data-ec", 4, "FLUSH TABLE accounts")
                .unwrap();
            assert!(fl.rows_affected >= 1);

            db.execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "UPSERT INTO accounts VALUES (102, 'bob', 2750, true), (104, 'dave', 400, true)",
            )
            .unwrap();
        }

        // Simulate sudden power cut AND wipe 2 of 6 Reed-Solomon domains (`data_1` & `data_4`)!
        fs.simulate_power_cut();
        for f in fs.list_files("data_1").unwrap() {
            fs.remove_file(&format!("data_1/{f}")).unwrap();
        }
        for f in fs.list_files("data_4").unwrap() {
            fs.remove_file(&format!("data_4/{f}")).unwrap();
        }

        let meta2 = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let (wal2, data2) = make_pools(&fs);
        let db2 = DatabaseEngine::new(meta2, 16);
        db2.register_pool("wal-fast", wal2);
        db2.register_pool("data-ec", data2);

        let bob = db2
            .execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "SELECT owner, balance FROM accounts WHERE id = 102",
            )
            .unwrap();
        assert_eq!(
            bob.rows,
            vec![vec![SqlValue::Text("bob".into()), SqlValue::Int(2750)]]
        );

        let active = db2
            .execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "SELECT id, owner, balance FROM accounts WHERE active = true ORDER BY balance DESC LIMIT 2",
            )
            .unwrap();
        assert_eq!(active.rows.len(), 2);
        assert_eq!(active.rows[0][1], SqlValue::Text("alice".into()));
        assert_eq!(active.rows[1][1], SqlValue::Text("bob".into()));
    }

    #[test]
    fn zombie_tablet_generation_is_fenced_out_by_metakv_cas() {
        let fs = SimFs::new(77);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let (wal, data) = make_pools(&fs);

        let mut old_leader = DataShardTablet::open_or_recover(
            meta.clone(),
            "/db/1/ledger/shards/accounts/0000".into(),
            1,
            42,
            "wal-fast".into(),
            "data-ec".into(),
            wal.clone(),
            data.clone(),
            16,
        )
        .unwrap();
        assert_eq!(old_leader.generation(), 1);

        let mut new_leader = DataShardTablet::open_or_recover(
            meta.clone(),
            "/db/1/ledger/shards/accounts/0000".into(),
            1,
            42,
            "wal-fast".into(),
            "data-ec".into(),
            wal,
            data,
            16,
        )
        .unwrap();
        assert_eq!(new_leader.generation(), 2);

        let zombie_res = old_leader.apply_batch(vec![(
            SqlValue::Int(1).encode_order_preserving_key().unwrap(),
            Some(vec![SqlValue::Int(1), SqlValue::Text("zombie".into())]),
        )]);
        assert!(matches!(
            zombie_res,
            Err(DbError::StaleTabletGeneration { owner_id: 42 })
        ));

        new_leader
            .apply_batch(vec![(
                SqlValue::Int(1).encode_order_preserving_key().unwrap(),
                Some(vec![SqlValue::Int(1), SqlValue::Text("legit".into())]),
            )])
            .unwrap();
    }

    #[test]
    fn cross_shard_transactions_update_aggregations_indexes_and_compaction() {
        let fs = SimFs::new(20261113);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let (wal, data) = make_pools(&fs);
        let db = DatabaseEngine::new(meta, 16);
        db.register_pool("wal-fast", wal);
        db.register_pool("data-ec", data.clone());

        db.execute_sql(
            1,
            "ledger",
            "wal-fast",
            "data-ec",
            4,
            "CREATE TABLE accounts (id INT PRIMARY KEY, tier TEXT, balance INT, active BOOL);",
        )
        .unwrap();

        db.execute_sql(
            1,
            "ledger",
            "wal-fast",
            "data-ec",
            4,
            "INSERT INTO accounts VALUES
                (1, 'gold', 5000, true),
                (2, 'silver', 2000, true),
                (3, 'gold', 7000, true),
                (4, 'silver', 1000, false);",
        )
        .unwrap();

        // 1. Secondary index backfill & O(log N) lookup
        let idx_res = db
            .execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "CREATE INDEX idx_tier ON accounts (tier);",
            )
            .unwrap();
        assert_eq!(idx_res.rows_affected, 4);

        let gold = db
            .execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "SELECT id, balance FROM accounts WHERE tier = 'gold';",
            )
            .unwrap();
        assert_eq!(
            gold.rows,
            vec![
                vec![SqlValue::Int(1), SqlValue::Int(5000)],
                vec![SqlValue::Int(3), SqlValue::Int(7000)],
            ]
        );

        // 2. Atomic cross-shard bank transfer via `BEGIN ... UPDATE ... COMMIT`!
        db.execute_sql(
            1,
            "ledger",
            "wal-fast",
            "data-ec",
            4,
            "BEGIN;
             UPDATE accounts SET balance = balance - 1500 WHERE id = 1;
             UPDATE accounts SET balance = balance + 1500, tier = 'gold' WHERE id = 2;
             COMMIT;",
        )
        .unwrap();

        // Verify secondary index automatically updated when row 2 changed tier 'silver' -> 'gold'!
        let gold_after = db
            .execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "SELECT id, balance FROM accounts WHERE tier = 'gold';",
            )
            .unwrap();
        assert_eq!(
            gold_after.rows,
            vec![
                vec![SqlValue::Int(1), SqlValue::Int(3500)],
                vec![SqlValue::Int(2), SqlValue::Int(3500)],
                vec![SqlValue::Int(3), SqlValue::Int(7000)],
            ]
        );

        // 3. Transaction ROLLBACK leaves committed balances completely untouched!
        db.execute_sql_with_session(
            1,
            "ledger",
            "wal-fast",
            "data-ec",
            4,
            Some("sa:billing"),
            "BEGIN; UPDATE accounts SET balance = 0 WHERE id = 3;",
        )
        .unwrap();
        // Inside same session, uncommitted write is visible:
        let in_tx = db
            .execute_sql_with_session(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                Some("sa:billing"),
                "SELECT balance FROM accounts WHERE id = 3;",
            )
            .unwrap();
        assert_eq!(in_tx.rows, vec![vec![SqlValue::Int(0)]]);
        // Outside session, committed balance is still 7000!
        let outside = db
            .execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "SELECT balance FROM accounts WHERE id = 3;",
            )
            .unwrap();
        assert_eq!(outside.rows, vec![vec![SqlValue::Int(7000)]]);
        // Rollback discards uncommitted changes:
        db.execute_sql_with_session(
            1,
            "ledger",
            "wal-fast",
            "data-ec",
            4,
            Some("sa:billing"),
            "ROLLBACK;",
        )
        .unwrap();
        let after_rb = db
            .execute_sql_with_session(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                Some("sa:billing"),
                "SELECT balance FROM accounts WHERE id = 3;",
            )
            .unwrap();
        assert_eq!(after_rb.rows, vec![vec![SqlValue::Int(7000)]]);

        // 4. GROUP BY + COUNT, SUM, MIN, MAX, AVG + ORDER BY alias
        let agg = db
            .execute_sql(
                1,
                "ledger",
                "wal-fast",
                "data-ec",
                4,
                "SELECT tier, COUNT(*) AS cnt, SUM(balance) AS total_bal, MIN(balance), MAX(balance), AVG(balance)
                 FROM accounts
                 GROUP BY tier
                 ORDER BY total_bal DESC;",
            )
            .unwrap();
        assert_eq!(
            agg.rows,
            vec![
                vec![
                    SqlValue::Text("gold".into()),
                    SqlValue::Int(3),
                    SqlValue::Int(14000),
                    SqlValue::Int(3500),
                    SqlValue::Int(7000),
                    SqlValue::Int(4666),
                ],
                vec![
                    SqlValue::Text("silver".into()),
                    SqlValue::Int(1),
                    SqlValue::Int(1000),
                    SqlValue::Int(1000),
                    SqlValue::Int(1000),
                    SqlValue::Int(1000),
                ],
            ]
        );

        // 5. Multiple SSTable flushes + DELETE + logical COMPACT TABLE.
        db.execute_sql(1, "ledger", "wal-fast", "data-ec", 4, "FLUSH TABLE accounts;")
            .unwrap();
        db.execute_sql(
            1,
            "ledger",
            "wal-fast",
            "data-ec",
            4,
            "DELETE FROM accounts WHERE active = false;",
        )
        .unwrap();
        db.execute_sql(1, "ledger", "wal-fast", "data-ec", 4, "FLUSH TABLE accounts;")
            .unwrap();

        let comp = db
            .execute_sql(1, "ledger", "wal-fast", "data-ec", 4, "COMPACT TABLE accounts;")
            .unwrap();
        assert_eq!(comp.rows[0][2], SqlValue::Int(3)); // 3 live rows remain
        assert_eq!(comp.rows[0][3], SqlValue::Int(0)); // Retain fragments for old readers.
    }
}
