pub mod yaml;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yadb_blob::ErasureScheme;

pub const API_VERSION_V1ALPHA1: &str = "yadb.io/v1alpha1";

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("YAML syntax error at line {line}: {msg}")]
    YamlSyntax { line: usize, msg: String },

    #[error("manifest schema error: {0}")]
    Schema(String),

    #[error("validation failed: {0}")]
    Validation(String),

    #[error("access denied for `{subject}` on project `{project}` (requires {required:?})")]
    AccessDenied {
        project: String,
        subject: String,
        required: Role,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    pub name: String,
    #[serde(default)]
    pub project: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Manifest {
    Cluster {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: ObjectMeta,
        spec: ClusterSpec,
    },
    StoragePool {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: ObjectMeta,
        spec: StoragePoolSpec,
    },
    Project {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: ObjectMeta,
        spec: ProjectSpec,
    },
    Database {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: ObjectMeta,
        spec: DatabaseSpec,
    },
    ObjectStore {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: ObjectMeta,
        spec: ObjectStoreSpec,
    },
    BlockVolume {
        #[serde(rename = "apiVersion")]
        api_version: String,
        metadata: ObjectMeta,
        spec: BlockVolumeSpec,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterSpec {
    pub domains: Vec<FailureDomainSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureDomainSpec {
    pub name: String,
    pub nodes: Vec<NodeSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpec {
    pub id: u32,
    pub addr: String,
    pub mounts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoragePoolSpec {
    pub erasure: ErasureSpec,
    pub failure_domains: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scheme", rename_all = "kebab-case")]
pub enum ErasureSpec {
    Mirror { replicas: u8 },
    ReedSolomon { k: u8, m: u8 },
}

impl ErasureSpec {
    pub fn to_scheme(&self) -> ErasureScheme {
        match *self {
            ErasureSpec::Mirror { replicas } => ErasureScheme::Mirror { replicas },
            ErasureSpec::ReedSolomon { k, m } => ErasureScheme::ReedSolomon { k, m },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectSpec {
    #[serde(default)]
    pub quotas: ProjectQuotas,
    #[serde(default)]
    pub members: Vec<MemberBinding>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectQuotas {
    pub max_storage_bytes: Option<u64>,
    pub max_iops: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Role {
    ReadOnly,
    ReadWrite,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberBinding {
    pub subject: String,
    pub role: Role,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IsolationMode {
    /// Zero-process-overhead multi-tenant execution isolated by `project_id` prefix + token bucket.
    #[default]
    Shared,
    /// Dedicated worker OS process isolated via Linux cgroups.
    Dedicated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseSpec {
    pub wal_pool: String,
    pub data_pool: String,
    #[serde(default)]
    pub isolation: IsolationMode,
    #[serde(default = "default_shards")]
    pub shards: u32,
}

fn default_shards() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectStoreSpec {
    pub pool: String,
    #[serde(default)]
    pub isolation: IsolationMode,
    #[serde(default)]
    pub buckets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockVolumeSpec {
    pub pool: String,
    pub size_bytes: u64,
}

pub fn parse_manifests(yaml_text: &str) -> Result<Vec<Manifest>, ControlError> {
    let values = yaml::parse_yaml_documents(yaml_text)?;
    let mut out = Vec::with_capacity(values.len());
    for val in values {
        let m: Manifest =
            serde_json::from_value(val).map_err(|e| ControlError::Schema(e.to_string()))?;
        out.push(m);
    }
    Ok(out)
}

// ============================================================================
// Declarative Control-Plane State & Reconciler (`yadbctl plan` / `apply`)
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffOp {
    Create { key: String },
    Update { key: String },
    Unchanged { key: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ControlPlaneState {
    pub generation: u64,
    pub next_project_id: u32,
    pub cluster: Option<(ObjectMeta, ClusterSpec)>,
    pub pools: BTreeMap<String, StoragePoolSpec>,
    pub projects: BTreeMap<String, (u32, ProjectSpec)>,
    pub databases: BTreeMap<String, DatabaseSpec>,
    pub object_stores: BTreeMap<String, ObjectStoreSpec>,
    pub block_volumes: BTreeMap<String, BlockVolumeSpec>,
}

impl ControlPlaneState {
    pub fn new() -> Self {
        Self {
            generation: 0,
            next_project_id: 1,
            ..Default::default()
        }
    }

    /// Computes dry-run diff AND validates all cross-resource references before mutating state.
    pub fn plan(&self, manifests: &[Manifest]) -> Result<(Vec<DiffOp>, ControlPlaneState), ControlError> {
        let mut staged = self.clone();
        let mut ops = Vec::new();

        for m in manifests {
            let api = match m {
                Manifest::Cluster { api_version, .. }
                | Manifest::StoragePool { api_version, .. }
                | Manifest::Project { api_version, .. }
                | Manifest::Database { api_version, .. }
                | Manifest::ObjectStore { api_version, .. }
                | Manifest::BlockVolume { api_version, .. } => api_version,
            };
            if api != API_VERSION_V1ALPHA1 {
                return Err(ControlError::Validation(format!(
                    "unsupported apiVersion `{api}`, expected `{API_VERSION_V1ALPHA1}`"
                )));
            }

            match m.clone() {
                Manifest::Cluster { metadata, spec, .. } => {
                    let key = format!("Cluster/{}", metadata.name);
                    match &staged.cluster {
                        None => ops.push(DiffOp::Create { key }),
                        Some(prev) if prev == &(metadata.clone(), spec.clone()) => {
                            ops.push(DiffOp::Unchanged { key })
                        }
                        Some(_) => ops.push(DiffOp::Update { key }),
                    }
                    staged.cluster = Some((metadata, spec));
                }
                Manifest::StoragePool { metadata, spec, .. } => {
                    spec.erasure
                        .to_scheme()
                        .validate()
                        .map_err(|e| ControlError::Validation(e.to_string()))?;
                    let key = format!("StoragePool/{}", metadata.name);
                    match staged.pools.get(&metadata.name) {
                        None => ops.push(DiffOp::Create { key }),
                        Some(prev) if prev == &spec => ops.push(DiffOp::Unchanged { key }),
                        Some(_) => ops.push(DiffOp::Update { key }),
                    }
                    staged.pools.insert(metadata.name, spec);
                }
                Manifest::Project { metadata, spec, .. } => {
                    let key = format!("Project/{}", metadata.name);
                    if let Some((pid, prev_spec)) = staged.projects.get(&metadata.name).cloned() {
                        if prev_spec == spec {
                            ops.push(DiffOp::Unchanged { key });
                        } else {
                            ops.push(DiffOp::Update { key });
                            staged.projects.insert(metadata.name, (pid, spec));
                        }
                    } else {
                        let pid = staged.next_project_id;
                        staged.next_project_id += 1;
                        ops.push(DiffOp::Create { key });
                        staged.projects.insert(metadata.name, (pid, spec));
                    }
                }
                Manifest::Database { metadata, spec, .. } => {
                    let proj = metadata.project.ok_or_else(|| {
                        ControlError::Validation(format!(
                            "Database `{}` must specify `metadata.project`",
                            metadata.name
                        ))
                    })?;
                    let map_key = format!("{proj}/{}", metadata.name);
                    let key = format!("Database/{map_key}");
                    match staged.databases.get(&map_key) {
                        None => ops.push(DiffOp::Create { key }),
                        Some(prev) if prev == &spec => ops.push(DiffOp::Unchanged { key }),
                        Some(_) => ops.push(DiffOp::Update { key }),
                    }
                    staged.databases.insert(map_key, spec);
                }
                Manifest::ObjectStore { metadata, spec, .. } => {
                    let proj = metadata.project.ok_or_else(|| {
                        ControlError::Validation(format!(
                            "ObjectStore `{}` must specify `metadata.project`",
                            metadata.name
                        ))
                    })?;
                    let map_key = format!("{proj}/{}", metadata.name);
                    let key = format!("ObjectStore/{map_key}");
                    match staged.object_stores.get(&map_key) {
                        None => ops.push(DiffOp::Create { key }),
                        Some(prev) if prev == &spec => ops.push(DiffOp::Unchanged { key }),
                        Some(_) => ops.push(DiffOp::Update { key }),
                    }
                    staged.object_stores.insert(map_key, spec);
                }
                Manifest::BlockVolume { metadata, spec, .. } => {
                    let proj = metadata.project.ok_or_else(|| {
                        ControlError::Validation(format!(
                            "BlockVolume `{}` must specify `metadata.project`",
                            metadata.name
                        ))
                    })?;
                    let map_key = format!("{proj}/{}", metadata.name);
                    let key = format!("BlockVolume/{map_key}");
                    match staged.block_volumes.get(&map_key) {
                        None => ops.push(DiffOp::Create { key }),
                        Some(prev) if prev == &spec => ops.push(DiffOp::Unchanged { key }),
                        Some(_) => ops.push(DiffOp::Update { key }),
                    }
                    staged.block_volumes.insert(map_key, spec);
                }
            }
        }

        staged.validate_referential_integrity()?;
        if ops.iter().any(|op| !matches!(op, DiffOp::Unchanged { .. })) {
            staged.generation += 1;
        }
        Ok((ops, staged))
    }

    pub fn apply(&mut self, manifests: &[Manifest]) -> Result<Vec<DiffOp>, ControlError> {
        let (ops, next_state) = self.plan(manifests)?;
        *self = next_state;
        Ok(ops)
    }

    fn validate_referential_integrity(&self) -> Result<(), ControlError> {
        if let Some((_, cluster)) = &self.cluster {
            let total_mounts: usize = cluster
                .domains
                .iter()
                .map(|d| d.nodes.iter().map(|n| n.mounts.len()).sum::<usize>())
                .sum();
            for (pool_name, pool) in &self.pools {
                if pool.failure_domains > cluster.domains.len() {
                    return Err(ControlError::Validation(format!(
                        "StoragePool `{pool_name}` requests {} failureDomains, but Cluster only has {}",
                        pool.failure_domains,
                        cluster.domains.len()
                    )));
                }
                let needed_parts = pool.erasure.to_scheme().total_parts();
                if needed_parts > total_mounts {
                    return Err(ControlError::Validation(format!(
                        "StoragePool `{pool_name}` requires {needed_parts} mounts for {:?}, cluster has {total_mounts}",
                        pool.erasure
                    )));
                }
            }
        }

        for (full_key, db) in &self.databases {
            let proj = full_key.split('/').next().unwrap_or("");
            if !self.projects.contains_key(proj) {
                return Err(ControlError::Validation(format!(
                    "Database `{full_key}` references missing Project `{proj}`"
                )));
            }
            if !self.pools.contains_key(&db.wal_pool) {
                return Err(ControlError::Validation(format!(
                    "Database `{full_key}` references missing walPool `{}`",
                    db.wal_pool
                )));
            }
            if !self.pools.contains_key(&db.data_pool) {
                return Err(ControlError::Validation(format!(
                    "Database `{full_key}` references missing dataPool `{}`",
                    db.data_pool
                )));
            }
        }

        for (full_key, os) in &self.object_stores {
            let proj = full_key.split('/').next().unwrap_or("");
            if !self.projects.contains_key(proj) {
                return Err(ControlError::Validation(format!(
                    "ObjectStore `{full_key}` references missing Project `{proj}`"
                )));
            }
            if !self.pools.contains_key(&os.pool) {
                return Err(ControlError::Validation(format!(
                    "ObjectStore `{full_key}` references missing pool `{}`",
                    os.pool
                )));
            }
        }

        for (full_key, vol) in &self.block_volumes {
            let proj = full_key.split('/').next().unwrap_or("");
            if !self.projects.contains_key(proj) {
                return Err(ControlError::Validation(format!(
                    "BlockVolume `{full_key}` references missing Project `{proj}`"
                )));
            }
            if !self.pools.contains_key(&vol.pool) {
                return Err(ControlError::Validation(format!(
                    "BlockVolume `{full_key}` references missing pool `{}`",
                    vol.pool
                )));
            }
        }

        Ok(())
    }

    /// In-memory RBAC check (~nanoseconds, zero network hops on hot path) returning internal `project_id`.
    pub fn authorize(
        &self,
        project_name: &str,
        subject: &str,
        min_role: Role,
    ) -> Result<u32, ControlError> {
        let (project_id, spec) = self
            .projects
            .get(project_name)
            .ok_or_else(|| ControlError::Validation(format!("unknown project `{project_name}`")))?;

        let granted = spec
            .members
            .iter()
            .filter(|m| m.subject == subject)
            .map(|m| m.role)
            .max();

        match granted {
            Some(role) if role >= min_role => Ok(*project_id),
            _ => Err(ControlError::AccessDenied {
                project: project_name.to_string(),
                subject: subject.to_string(),
                required: min_role,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_reconciles_and_enforces_project_rbac() {
        let yaml = include_str!("../../../examples/platform.yaml");
        let manifests = parse_manifests(yaml).unwrap();
        assert_eq!(manifests.len(), 7);

        let mut state = ControlPlaneState::new();
        let ops1 = state.apply(&manifests).unwrap();
        assert_eq!(state.generation, 1);
        assert!(ops1.iter().all(|o| matches!(o, DiffOp::Create { .. })));

        // Re-applying identical YAML is a zero-generation-bump No-Op!
        let ops2 = state.apply(&manifests).unwrap();
        assert_eq!(state.generation, 1);
        assert!(ops2.iter().all(|o| matches!(o, DiffOp::Unchanged { .. })));

        // RBAC checks
        let pid = state
            .authorize("billing", "sa:billing-backend", Role::ReadWrite)
            .unwrap();
        assert_eq!(pid, 1);
        assert!(state
            .authorize("billing", "user:auditor", Role::ReadOnly)
            .is_ok());
        assert!(state
            .authorize("billing", "user:auditor", Role::ReadWrite)
            .is_err());
    }
}
