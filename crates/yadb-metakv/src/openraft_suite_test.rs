//! Runs the adapted upstream OpenRaft storage suite against the durable
//! file-backed adapters.
//!
//! The suite adapts `openraft::testing::Suite` with helper, assertion and input
//! substitutions (see [`crate::openraft_suite`]); it does not close the
//! unmodified upstream suite gate. Its case-by-case mapping, including the
//! upstream assertions that are observed through a different public API, is
//! held and machine-checked by `scripts/test_openraft_suite_mapping.py`. Its
//! cases run against [`DurableOpenRaftLog`] and [`DurableMetaStateMachine`],
//! each in a fresh temporary directory.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::testing::StoreBuilder;
use openraft::{AnyError, StorageError, StorageIOError};

use crate::multiraft::{GroupIdentity, ReplicaIdentity};
use crate::openraft_runtime::YadbRaftTypeConfig;
use crate::openraft_state_machine::DurableMetaStateMachine;
use crate::openraft_storage::DurableOpenRaftLog;
use crate::openraft_suite::Suite;

/// Removes the case directory once the stores released their directory locks.
struct CaseGuard(PathBuf);

impl Drop for CaseGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Builds a fresh durable store for every suite case.
struct FileStoreBuilder {
    root: PathBuf,
    counter: AtomicU64,
}

impl FileStoreBuilder {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "yadb-openraft-suite-{nonce}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self {
            root,
            counter: AtomicU64::new(0),
        }
    }

    fn identity() -> ReplicaIdentity {
        ReplicaIdentity::new(
            GroupIdentity {
                cluster: [3; 16],
                group: 1,
                incarnation: 1,
            },
            1,
            vec![1, 2, 3],
        )
        .unwrap()
    }
}

fn storage_error(error: std::io::Error) -> StorageError<u32> {
    StorageError::from(StorageIOError::write_logs(AnyError::error(error)))
}

impl StoreBuilder<YadbRaftTypeConfig, DurableOpenRaftLog, DurableMetaStateMachine, CaseGuard>
    for FileStoreBuilder
{
    async fn build(
        &self,
    ) -> Result<(CaseGuard, DurableOpenRaftLog, DurableMetaStateMachine), StorageError<u32>> {
        let index = self.counter.fetch_add(1, Ordering::SeqCst);
        let dir = self.root.join(format!("case-{index}"));
        std::fs::create_dir_all(&dir).map_err(storage_error)?;
        let identity = Self::identity();
        let log = DurableOpenRaftLog::create(dir.join("log"), &identity).map_err(storage_error)?;
        let state =
            DurableMetaStateMachine::create(dir.join("state"), &identity).map_err(storage_error)?;
        Ok((CaseGuard(dir), log, state))
    }
}

#[test]
fn adapted_storage_suite_on_durable_adapters() {
    let builder = FileStoreBuilder::new();
    if let Err(error) = Suite::test_all(builder) {
        panic!("adapted OpenRaft storage suite failed: {error}");
    }
}
