pub mod sql;
pub mod tablet;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yadb_blob::{BlobError, PoolStriper};
use yadb_io::StorageFs;
use yadb_metakv::{KvCommand, KvResult, LocalMetaKv, MetaError};

pub use sql::{parse_sql, CmpOp, ColumnDef, ColumnType, Predicate, Projection, SqlStmt, SqlValue};
pub use tablet::{DataShardTablet, TabletManifest};

#[derive(Debug, Error)]
pub enum DbError {
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

    #[error("duplicate primary key in table `{0}` (use UPSERT INTO to overwrite)")]
    DuplicatePrimaryKey(String),

    #[error("stale tablet generation fenced out for owner_id={owner_id}")]
    StaleTabletGeneration { owner_id: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub pk_index: usize,
    pub shards: u32,
    pub wal_pool: String,
    pub data_pool: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub rows_affected: u64,
}

/// Multi-tenant Sharded SQL & Tablet Engine built on `MetaKV` + dual `PoolStriper` channels
/// (`walPool` for low-latency mirrored WALs, `dataPool` for erasure-coded SSTables).
pub struct DatabaseEngine<FS: StorageFs> {
    meta: LocalMetaKv<FS>,
    pools: Arc<RwLock<BTreeMap<String, Arc<PoolStriper<FS>>>>>,
    #[allow(clippy::type_complexity)]
    shards: Arc<Mutex<BTreeMap<String, Arc<Mutex<DataShardTablet<FS>>>>>>,
    flush_threshold_rows: usize,
}

impl<FS: StorageFs> Clone for DatabaseEngine<FS> {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            pools: self.pools.clone(),
            shards: self.shards.clone(),
            flush_threshold_rows: self.flush_threshold_rows,
        }
    }
}

impl<FS: StorageFs> DatabaseEngine<FS> {
    pub fn new(meta: LocalMetaKv<FS>, flush_threshold_rows: usize) -> Self {
        Self {
            meta,
            pools: Arc::new(RwLock::new(BTreeMap::new())),
            shards: Arc::new(Mutex::new(BTreeMap::new())),
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

    fn shard_owner_id(db: &str, table: &str, shard_idx: u32) -> u32 {
        crc32fast::hash(format!("db:{db}/{table}/{shard_idx}").as_bytes()).max(1)
    }

    fn route_shard(pk_bytes: &[u8], shard_count: u32) -> u32 {
        crc32fast::hash(pk_bytes) % shard_count.max(1)
    }

    fn load_schema(&self, project_id: u32, db: &str, table: &str) -> Result<TableSchema, DbError> {
        let key = Self::schema_key(project_id, db, table);
        let kv = self
            .meta
            .get(&key)
            .ok_or_else(|| DbError::TableNotFound(table.to_string()))?;
        Ok(serde_json::from_slice(&kv.value)?)
    }

    fn get_or_open_shard(
        &self,
        project_id: u32,
        db: &str,
        schema: &TableSchema,
        shard_idx: u32,
    ) -> Result<Arc<Mutex<DataShardTablet<FS>>>, DbError> {
        let mkey = Self::shard_meta_key(project_id, db, &schema.table, shard_idx);
        {
            let map = self.shards.lock().unwrap();
            if let Some(existing) = map.get(&mkey) {
                return Ok(existing.clone());
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
        Ok(map
            .entry(mkey)
            .or_insert_with(|| handle.clone())
            .clone())
    }

    fn read_shard_row(
        &self,
        project_id: u32,
        db: &str,
        schema: &TableSchema,
        shard_idx: u32,
        pk_bytes: &[u8],
    ) -> Result<Option<Vec<SqlValue>>, DbError> {
        let mkey = Self::shard_meta_key(project_id, db, &schema.table, shard_idx);
        let meta_ver = self.meta.get(&mkey).map(|kv| kv.version).unwrap_or(0);
        if meta_ver == 0 {
            return Ok(None);
        }
        if let Some(existing) = self.shards.lock().unwrap().get(&mkey).cloned() {
            let guard = existing.lock().unwrap();
            if guard.manifest_version() == meta_ver {
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

    fn scan_shard_rows(
        &self,
        project_id: u32,
        db: &str,
        schema: &TableSchema,
        shard_idx: u32,
    ) -> Result<Vec<Vec<SqlValue>>, DbError> {
        let mkey = Self::shard_meta_key(project_id, db, &schema.table, shard_idx);
        let meta_ver = self.meta.get(&mkey).map(|kv| kv.version).unwrap_or(0);
        if meta_ver == 0 {
            return Ok(Vec::new());
        }
        if let Some(existing) = self.shards.lock().unwrap().get(&mkey).cloned() {
            let guard = existing.lock().unwrap();
            if guard.manifest_version() == meta_ver {
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
    pub fn execute_sql(
        &self,
        project_id: u32,
        db: &str,
        wal_pool: &str,
        data_pool: &str,
        default_shards: u32,
        sql: &str,
    ) -> Result<QueryResult, DbError> {
        let stmt = parse_sql(sql)?;
        self.execute_stmt(project_id, db, wal_pool, data_pool, default_shards, stmt)
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
        match stmt {
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
                    ]);
                }
                Ok(QueryResult {
                    columns: vec![
                        "table".into(),
                        "shards".into(),
                        "wal_pool".into(),
                        "data_pool".into(),
                    ],
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

                // Ensure backing storage pools exist on this node
                let _ = self.get_pool(wal_pool)?;
                let _ = self.get_pool(data_pool)?;

                let schema = TableSchema {
                    table: table.clone(),
                    columns,
                    pk_index: pk_positions[0],
                    shards: default_shards.max(1),
                    wal_pool: wal_pool.to_string(),
                    data_pool: data_pool.to_string(),
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

                let mut per_shard: BTreeMap<u32, Vec<(Vec<u8>, Option<Vec<SqlValue>>)>> =
                    BTreeMap::new();
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
                    let shard_idx = Self::route_shard(&pk_bytes, schema.shards);
                    per_shard
                        .entry(shard_idx)
                        .or_default()
                        .push((pk_bytes, Some(full_row)));
                    total_written += 1;
                }

                for (shard_idx, batch) in per_shard {
                    let shard = self.get_or_open_shard(project_id, db, &schema, shard_idx)?;
                    let mut guard = shard.lock().unwrap();
                    if !upsert {
                        for (pk, _) in &batch {
                            if guard.get_row(pk)?.is_some() {
                                return Err(DbError::DuplicatePrimaryKey(table));
                            }
                        }
                    }
                    guard.apply_batch(batch)?;
                }

                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: total_written,
                })
            }

            SqlStmt::Select {
                table,
                projection,
                predicates,
                order_by,
                limit,
            } => {
                let schema = self.load_schema(project_id, db, &table)?;
                let pk_col_name = &schema.columns[schema.pk_index].name;

                // Validate predicate columns up front
                let mut resolved_preds: Vec<(usize, Predicate)> =
                    Vec::with_capacity(predicates.len());
                let mut exact_pk_lookup: Option<Vec<u8>> = None;
                for pred in predicates {
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&pred.column))
                        .ok_or_else(|| {
                            DbError::InvalidQuery(format!(
                                "unknown column `{}` in WHERE clause",
                                pred.column
                            ))
                        })?;
                    if idx == schema.pk_index
                        && pred.op == CmpOp::Eq
                        && pred
                            .value
                            .check_type(schema.columns[idx].col_type, false)
                    {
                        exact_pk_lookup = Some(pred.value.encode_order_preserving_key()?);
                    }
                    resolved_preds.push((idx, pred));
                }

                // Single-shard O(1) routing when `WHERE <pk> = <literal>` is present!
                let mut candidate_rows = Vec::new();
                if let Some(pk_bytes) = exact_pk_lookup {
                    let shard_idx = Self::route_shard(&pk_bytes, schema.shards);
                    if let Some(row) =
                        self.read_shard_row(project_id, db, &schema, shard_idx, &pk_bytes)?
                    {
                        candidate_rows.push(row);
                    }
                } else {
                    for shard_idx in 0..schema.shards {
                        candidate_rows
                            .extend(self.scan_shard_rows(project_id, db, &schema, shard_idx)?);
                    }
                    // Deterministic default sort by PRIMARY KEY across shards
                    let pk_i = schema.pk_index;
                    candidate_rows.sort_by(|a, b| {
                        a[pk_i].sql_cmp(&b[pk_i]).unwrap_or(Ordering::Equal)
                    });
                }

                // Filter by all WHERE predicates
                candidate_rows.retain(|row| {
                    resolved_preds
                        .iter()
                        .all(|(col_i, pred)| pred.eval(&row[*col_i]))
                });

                // Optional ORDER BY
                if let Some((ord_col, desc)) = order_by {
                    let ord_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ord_col))
                        .ok_or_else(|| {
                            DbError::InvalidQuery(format!(
                                "unknown column `{ord_col}` in ORDER BY"
                            ))
                        })?;
                    candidate_rows.sort_by(|a, b| {
                        let cmp = a[ord_idx].sql_cmp(&b[ord_idx]).unwrap_or(Ordering::Equal);
                        if desc {
                            cmp.reverse()
                        } else {
                            cmp
                        }
                    });
                }

                if let Some(max_rows) = limit {
                    candidate_rows.truncate(max_rows);
                }

                let (out_cols, proj_idxs): (Vec<String>, Vec<usize>) = match projection {
                    Projection::All => (
                        schema.columns.iter().map(|c| c.name.clone()).collect(),
                        (0..schema.columns.len()).collect(),
                    ),
                    Projection::Columns(names) => {
                        let mut cols = Vec::with_capacity(names.len());
                        let mut idxs = Vec::with_capacity(names.len());
                        for name in names {
                            let pos = schema
                                .columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(&name))
                                .ok_or_else(|| {
                                    DbError::InvalidQuery(format!(
                                        "unknown column `{name}` in SELECT list (pk is `{pk_col_name}`)"
                                    ))
                                })?;
                            cols.push(schema.columns[pos].name.clone());
                            idxs.push(pos);
                        }
                        (cols, idxs)
                    }
                };

                let projected_rows = candidate_rows
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
                let mut resolved_preds: Vec<(usize, Predicate)> =
                    Vec::with_capacity(predicates.len());
                for pred in predicates {
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&pred.column))
                        .ok_or_else(|| {
                            DbError::InvalidQuery(format!(
                                "unknown column `{}` in DELETE WHERE",
                                pred.column
                            ))
                        })?;
                    resolved_preds.push((idx, pred));
                }

                let mut deleted = 0u64;
                for shard_idx in 0..schema.shards {
                    let shard = self.get_or_open_shard(project_id, db, &schema, shard_idx)?;
                    let mut guard = shard.lock().unwrap();
                    let live = guard.scan_all_live_rows()?;
                    let mut tombstones = Vec::new();
                    for row in live {
                        if resolved_preds
                            .iter()
                            .all(|(col_i, pred)| pred.eval(&row[*col_i]))
                        {
                            let pk_bytes = row[schema.pk_index].encode_order_preserving_key()?;
                            tombstones.push((pk_bytes, None));
                            deleted += 1;
                        }
                    }
                    if !tombstones.is_empty() {
                        guard.apply_batch(tombstones)?;
                    }
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
        }
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

            // Flush shards to Reed-Solomon 4+2 SSTables, then write one more row to Mirror-3 WAL!
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

        // Reopen cluster from disk: SSTables reconstruct via RS(4+2) and WAL tail replays!
        let meta2 = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let (wal2, data2) = make_pools(&fs);
        let db2 = DatabaseEngine::new(meta2, 16);
        db2.register_pool("wal-fast", wal2);
        db2.register_pool("data-ec", data2);

        // Single-shard PK lookup for updated row 102
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

        // Cross-shard filtered & ordered query
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

        let mut old_leader_tablet = DataShardTablet::open_or_recover(
            meta.clone(),
            "/db/1/ledger/shards/t/0000".into(),
            1,
            500,
            "wal-fast".into(),
            "data-ec".into(),
            wal.clone(),
            data.clone(),
            64,
        )
        .unwrap();
        assert_eq!(old_leader_tablet.generation(), 1);

        // A new tablet instance takes over ownership -> bumps generation to 2 in MetaKV!
        let mut new_leader_tablet = DataShardTablet::open_or_recover(
            meta.clone(),
            "/db/1/ledger/shards/t/0000".into(),
            1,
            500,
            "wal-fast".into(),
            "data-ec".into(),
            wal,
            data,
            64,
        )
        .unwrap();
        assert_eq!(new_leader_tablet.generation(), 2);

        // Stale generation-1 tablet attempts to commit a write -> MUST BE FENCED OUT!
        let stale_err = old_leader_tablet
            .apply_batch(vec![(vec![1, 2, 3], Some(vec![SqlValue::Int(999)]))])
            .unwrap_err();
        assert!(matches!(stale_err, DbError::StaleTabletGeneration { .. }));

        // Generation-2 tablet commits cleanly!
        new_leader_tablet
            .apply_batch(vec![(vec![1, 2, 3], Some(vec![SqlValue::Int(42)]))])
            .unwrap();
        assert_eq!(
            new_leader_tablet.get_row(&[1, 2, 3]).unwrap(),
            Some(vec![SqlValue::Int(42)])
        );
    }
}
