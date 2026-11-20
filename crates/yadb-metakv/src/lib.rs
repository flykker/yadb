pub mod fence_token;
pub mod gc;
pub mod kv;
pub mod multiraft;
pub mod openraft_client;
pub mod openraft_net;
pub mod openraft_runtime;
pub mod openraft_state_machine;
pub mod openraft_storage;
#[cfg(test)]
mod openraft_suite;
#[cfg(test)]
mod openraft_suite_test;
pub mod openraft_tls;
pub mod openraft_transport;
pub mod raft;
pub mod wal;

pub use fence_token::{FenceOperation, FenceToken, FenceTokenError, FENCE_TOKEN_VERSION};
pub use gc::{CollectorGuard, GcGroupScope, GcLease, GcLeaseGuard, LeaseKind};
pub use kv::{
    CasGuard, KvCommand, KvResult, KvValue, MetaSnapshot, MetaStateMachine, RevisionGuard, TxnOp,
    WatchEvent,
};
pub use openraft_client::{CommittedLogId, GroupObservation, WriteAck};
pub use raft::{LocalKv, LocalMetaKv, LogEntry, RaftRpc, RoleState};
pub use wal::RaftWal;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("metadata storage failed; restart and recover before participating in Raft")]
    StorageUnavailable,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("corrupt Raft WAL: {0}")]
    CorruptWal(&'static str),

    #[error("corrupt metadata checkpoint: {0}")]
    Checkpoint(String),

    #[error("peer {0} cannot be served from the compacted log base")]
    PeerBehindCompactedBase(u32),

    #[error("metadata log compaction failed ({0}); this node refuses further work until it is restarted")]
    CompactionFailed(String),

    #[error("node is not Raft leader (known leader: {leader:?})")]
    NotLeader { leader: Option<u32> },

    /// No confirmed result for the original proposal. It may still commit, or a
    /// later leader may replace it; callers must not interpret this as rollback.
    #[error("no confirmed result for proposal at log index {0}; write outcome is unknown")]
    UncommittedIndex(u64),

    /// A submitted OpenRaft write may or may not have committed.
    #[error("OpenRaft write outcome unknown: {0}")]
    OpenRaftWriteUnknown(String),

    /// The write provably never reached Raft: the bridge queue was full or
    /// already closed, so `try_send` refused it. Unlike an unknown outcome this
    /// is safe to retry, and telling a client otherwise would suppress a
    /// mutation that definitively did not happen.
    #[error("OpenRaft write was not sent: {0}")]
    OpenRaftWriteNotSent(String),

    /// A control-group linearizable read could not be established. Callers must
    /// fail closed rather than use the locally applied snapshot.
    #[error("control-plane freshness unavailable: {0}")]
    ControlFreshnessUnavailable(String),

    #[error("OpenRaft synchronous writes are unsupported in a current-thread Tokio runtime")]
    OpenRaftAsyncContext,

    #[error("metadata routing error: {0}")]
    Routing(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use yadb_io::{FaultConfig, SimFs, StorageFs, SyncFault, VirtualFile};

    #[test]
    fn corrupt_wal_refuses_to_start_instead_of_dropping_acked_writes() {
        // End-to-end through the production entry point: five acknowledged
        // writes, then one bit flip inside the third data frame. Before the
        // fix this reopened `Ok` with a two-entry log and the single-node
        // recovery path committed that shorter prefix as truth.
        let fs = SimFs::new(9021);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        for n in 0..5u64 {
            meta.execute(KvCommand::Put {
                key: format!("k{n}"),
                value: n.to_le_bytes().to_vec(),
            })
            .unwrap();
        }
        drop(meta);
        fs.simulate_power_cut();

        let path = "meta/raft.wal";
        let file = fs.open_rw(path, false).unwrap();
        let mut bytes = vec![0u8; file.len().unwrap() as usize];
        file.read_at(0, &mut bytes).unwrap();
        drop(file);

        // Walk the frame headers to find the third data frame's payload.
        let mut pos = 8usize;
        let mut starts = Vec::new();
        while pos + 16 <= bytes.len() {
            starts.push(pos);
            let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
            pos += 16 + len;
        }
        // Frame 0 is the first data write, so frame 3 is the fourth write.
        assert!(starts.len() >= 4, "expected five data frames");
        fs.corrupt_byte(path, starts[3] + 16).unwrap();
        let damaged = {
            let file = fs.open_rw(path, false).unwrap();
            let mut after = vec![0u8; file.len().unwrap() as usize];
            file.read_at(0, &mut after).unwrap();
            after
        };

        match LocalMetaKv::open_single_node(&fs, "meta", 1) {
            Err(MetaError::CorruptWal(_)) => {}
            Err(error) => panic!("expected CorruptWal, got {error:?}"),
            Ok(_) => panic!("reopened with a truncated log after mid-file corruption"),
        }

        let file = fs.open_rw(path, false).unwrap();
        let mut after = vec![0u8; file.len().unwrap() as usize];
        file.read_at(0, &mut after).unwrap();
        assert_eq!(after, damaged, "refusing to start must not rewrite the WAL");
    }

    #[test]
    fn selected_snapshot_is_owned_atomic_and_does_not_write_metadata() {
        let fs = SimFs::new(9020);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let publish = |n: u64| KvCommand::RevisionTxn {
            guards: vec![],
            ops: ["schema", "shards/a", "idx/a"]
                .into_iter()
                .map(|key| TxnOp::Put {
                    key: key.into(),
                    value: n.to_le_bytes().to_vec(),
                })
                .collect(),
        };
        meta.execute(publish(0)).unwrap();
        let keys = vec!["schema".into(), "absent".into()];
        let prefixes = vec!["shards/".into(), "idx/".into(), "idx/".into()];
        let initial_revision = meta.revision_or_panic();
        let pinned = meta.snapshot_or_panic(&keys, &prefixes);
        assert_eq!(meta.revision_or_panic(), initial_revision);
        assert_eq!(pinned.revision(), initial_revision);
        let writer = meta.clone();
        let thread = std::thread::spawn(move || {
            for n in 1..65 {
                writer.execute(publish(n)).unwrap();
            }
            writer
                .execute(KvCommand::Put {
                    key: "absent".into(),
                    value: vec![1],
                })
                .unwrap();
        });
        for _ in 0..128 {
            let snapshot = meta.snapshot_or_panic(&keys, &prefixes);
            let schema = snapshot.get("schema").unwrap();
            for key in ["shards/a", "idx/a"] {
                let row = snapshot.get(key).unwrap();
                assert_eq!(row.value, schema.value);
                assert_eq!(row.mod_revision, schema.mod_revision);
                assert!(row.mod_revision <= snapshot.revision());
            }
            assert_eq!(snapshot.scan_prefix("idx/").len(), 1);
        }
        thread.join().unwrap();
        assert_eq!(pinned.get("schema").unwrap().value, 0u64.to_le_bytes());
        assert!(pinned.get("absent").is_none());
        meta.execute(KvCommand::Delete {
            key: "idx/a".into(),
        })
        .unwrap();
        assert_eq!(pinned.scan_prefix("idx/").len(), 1);
        drop(meta);
        fs.simulate_power_cut();
        let recovered = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        assert_eq!(
            recovered
                .snapshot_or_panic(&keys, &prefixes)
                .get("schema")
                .unwrap()
                .value,
            64u64.to_le_bytes()
        );
        // A snapshot is an owned in-memory value, not a persistent reader lease.
        assert_eq!(pinned.get("schema").unwrap().value, 0u64.to_le_bytes());
    }

    #[test]
    fn leader_sync_failure_is_unknown_outcome_and_blocks_further_proposals() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(9012);
            let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
            fs.set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            let cmd = KvCommand::Put {
                key: "key".into(),
                value: b"unknown".to_vec(),
            };
            assert!(matches!(meta.execute(cmd.clone()), Err(MetaError::Io(_))));
            fs.set_faults(FaultConfig::default());
            assert!(matches!(
                meta.execute(cmd),
                Err(MetaError::StorageUnavailable)
            ));
            assert!(meta.get_or_panic("key").is_none());
            drop(meta);
            fs.simulate_power_cut();
            let recovered = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
            assert_eq!(
                recovered.get_or_panic("key").is_some(),
                fault == SyncFault::AfterPersist
            );
            recovered
                .execute(KvCommand::Put {
                    key: "next".into(),
                    value: b"durable".to_vec(),
                })
                .unwrap();
        }
    }

    #[test]
    fn revision_guards_reject_delete_recreate_aba_after_wal_recovery() {
        let fs = SimFs::new(9001);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        meta.execute(KvCommand::Put {
            key: "key".into(),
            value: b"old".to_vec(),
        })
        .unwrap();
        let old = meta.get_or_panic("key").unwrap();
        meta.execute(KvCommand::Delete { key: "key".into() })
            .unwrap();
        meta.execute(KvCommand::Put {
            key: "key".into(),
            value: b"new".to_vec(),
        })
        .unwrap();
        let new = meta.get_or_panic("key").unwrap();
        assert_eq!(old.version, new.version); // ordinary version guards have ABA
        assert_ne!(old.mod_revision, new.mod_revision);
        drop(meta);
        fs.simulate_power_cut();
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let result = meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: "key".into(),
                    expected_mod_revision: old.mod_revision,
                }],
                ops: vec![
                    TxnOp::Delete { key: "key".into() },
                    TxnOp::Put {
                        key: "retired".into(),
                        value: b"old".to_vec(),
                    },
                ],
            })
            .unwrap();
        assert!(matches!(result, KvResult::CasConflict { .. }));
        assert_eq!(meta.get_or_panic("key").unwrap().value, b"new");
        assert!(meta.get_or_panic("retired").is_none());
        assert!(matches!(
            meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: "key".into(),
                    expected_mod_revision: new.mod_revision
                }],
                ops: vec![TxnOp::Delete { key: "key".into() }],
            })
            .unwrap(),
            KvResult::TxnOk { .. }
        ));
    }

    #[test]
    fn watch_and_ordered_prefix_scan_work_after_wal_recovery() {
        let fs = SimFs::new(888);
        {
            let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
            let rx = meta.watch_prefix("/s3/1/receipts/");
            meta.execute(KvCommand::Put {
                key: "/s3/1/receipts/b.pdf".into(),
                value: b"m2".to_vec(),
            })
            .unwrap();
            meta.execute(KvCommand::Put {
                key: "/s3/1/receipts/a.pdf".into(),
                value: b"m1".to_vec(),
            })
            .unwrap();

            let ev = rx.try_recv().unwrap();
            assert!(matches!(ev, WatchEvent::Put { ref key, .. } if key == "/s3/1/receipts/b.pdf"));
        }

        // Simulate power cut & reopen WAL -> lexicographical order preserved!
        fs.simulate_power_cut();
        let reopened = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let items = reopened.scan_prefix_or_panic("/s3/1/receipts/");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, "/s3/1/receipts/a.pdf");
        assert_eq!(items[1].0, "/s3/1/receipts/b.pdf");
    }
}
