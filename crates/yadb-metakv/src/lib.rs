pub mod kv;
pub mod multiraft;
pub mod openraft_runtime;
#[cfg(test)]
mod openraft_storage;
pub mod raft;
pub mod wal;

pub use kv::{CasGuard, KvCommand, KvResult, KvValue, MetaSnapshot, MetaStateMachine, RevisionGuard, TxnOp, WatchEvent};
pub use raft::{LocalMetaKv, LogEntry, OutboundMsg, RaftCore, RaftRpc, RoleState};
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

    #[error("node is not Raft leader (known leader: {leader:?})")]
    NotLeader { leader: Option<u32> },

    /// No confirmed result for the original proposal. It may still commit, or a
    /// later leader may replace it; callers must not interpret this as rollback.
    #[error("no confirmed result for proposal at log index {0}; write outcome is unknown")]
    UncommittedIndex(u64),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use yadb_io::{FaultConfig, SimFs, SyncFault};

    #[test]
    fn selected_snapshot_is_owned_atomic_and_does_not_write_metadata() {
        let fs = SimFs::new(9020);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let publish = |n: u64| KvCommand::RevisionTxn {
            guards: vec![],
            ops: ["schema", "shards/a", "idx/a"].into_iter().map(|key| TxnOp::Put {
                key: key.into(), value: n.to_le_bytes().to_vec(),
            }).collect(),
        };
        meta.execute(publish(0)).unwrap();
        let keys = vec!["schema".into(), "absent".into()];
        let prefixes = vec!["shards/".into(), "idx/".into(), "idx/".into()];
        let initial_revision = meta.revision();
        let pinned = meta.snapshot(&keys, &prefixes);
        assert_eq!(meta.revision(), initial_revision);
        assert_eq!(pinned.revision(), initial_revision);
        let writer = meta.clone();
        let thread = std::thread::spawn(move || {
            for n in 1..65 { writer.execute(publish(n)).unwrap(); }
            writer.execute(KvCommand::Put { key: "absent".into(), value: vec![1] }).unwrap();
        });
        for _ in 0..128 {
            let snapshot = meta.snapshot(&keys, &prefixes);
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
        meta.execute(KvCommand::Delete { key: "idx/a".into() }).unwrap();
        assert_eq!(pinned.scan_prefix("idx/").len(), 1);
        drop(meta);
        fs.simulate_power_cut();
        let recovered = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        assert_eq!(recovered.snapshot(&keys, &prefixes).get("schema").unwrap().value, 64u64.to_le_bytes());
        // A snapshot is an owned in-memory value, not a persistent reader lease.
        assert_eq!(pinned.get("schema").unwrap().value, 0u64.to_le_bytes());
    }

    #[test]
    fn process_only_reopen_syncs_recovered_wal_before_matching_log_ack() {
        let fs = SimFs::new(9013);
        let mut follower = RaftCore::open(&fs, "meta", 2, vec![1, 2, 3], true).unwrap();
        follower.step(RaftRpc::AppendEntries {
            term: 1, leader_id: 1, prev_log_index: 0, prev_log_term: 0,
            entries: vec![], leader_commit: 0,
        }).unwrap();
        let rpc = RaftRpc::AppendEntries {
            term: 1, leader_id: 1, prev_log_index: 0, prev_log_term: 0,
            entries: vec![LogEntry { index: 1, term: 1,
                cmd: KvCommand::Put { key: "key".into(), value: b"value".to_vec() } }],
            leader_commit: 1,
        };
        fs.set_faults(FaultConfig { sync_data_fault: SyncFault::BeforePersist, ..Default::default() });
        assert!(follower.step(rpc.clone()).is_err());
        drop(follower);
        assert!(RaftCore::open(&fs, "meta", 2, vec![1, 2, 3], true).is_err());
        fs.set_faults(FaultConfig::default());
        let mut reopened = RaftCore::open(&fs, "meta", 2, vec![1, 2, 3], true).unwrap();
        let replies = reopened.step(rpc).unwrap();
        assert!(replies.iter().any(|reply| matches!(reply.rpc,
            RaftRpc::AppendEntriesReply { success: true, match_index: 1, .. })));
        drop(reopened);
        fs.simulate_power_cut();
        let durable = RaftCore::open(&fs, "meta", 2, vec![1, 2, 3], true).unwrap();
        assert_eq!(durable.last_log_index(), 1);
    }

    #[test]
    fn follower_never_acknowledges_memory_only_log_after_wal_failure() {
        for faults in [
            FaultConfig { write_eio_prob: 1.0, ..Default::default() },
            FaultConfig { torn_write_prob: 1.0, ..Default::default() },
            FaultConfig { sync_data_fault: SyncFault::BeforePersist, ..Default::default() },
            FaultConfig { sync_data_fault: SyncFault::AfterPersist, ..Default::default() },
        ] {
            let fs = SimFs::new(9010);
            let mut follower = RaftCore::open(&fs, "meta", 2, vec![1, 2, 3], true).unwrap();
            let heartbeat = RaftRpc::AppendEntries {
                term: 1, leader_id: 1, prev_log_index: 0, prev_log_term: 0,
                entries: vec![], leader_commit: 0,
            };
            follower.step(heartbeat).unwrap(); // persist term before faulting log
            let rpc = RaftRpc::AppendEntries {
                term: 1, leader_id: 1, prev_log_index: 0, prev_log_term: 0,
                entries: vec![LogEntry { index: 1, term: 1,
                    cmd: KvCommand::Put { key: "key".into(), value: b"value".to_vec() } }],
                leader_commit: 1,
            };
            fs.set_faults(faults);
            assert!(matches!(follower.step(rpc.clone()), Err(MetaError::Io(_))));
            assert_eq!(follower.commit_index(), 0);
            fs.set_faults(FaultConfig::default());
            // Matching in-memory entries are NOT evidence of durability.
            assert!(matches!(follower.step(rpc.clone()), Err(MetaError::StorageUnavailable)));
            assert!(matches!(follower.start_election(), Err(MetaError::StorageUnavailable)));
            assert!(follower.broadcast_append_entries().is_empty());
            assert!(follower.get("key").is_none());
            drop(follower);
            fs.simulate_power_cut();
            let mut recovered = RaftCore::open(&fs, "meta", 2, vec![1, 2, 3], true).unwrap();
            let replies = recovered.step(rpc).unwrap();
            assert!(replies.iter().any(|reply| matches!(reply.rpc,
                RaftRpc::AppendEntriesReply { success: true, match_index: 1, .. })));
            assert_eq!(recovered.get("key").unwrap().value, b"value");
        }
    }

    #[test]
    fn failed_hard_state_sync_blocks_votes_and_heartbeats_until_recovery() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(9011);
            let mut follower = RaftCore::open(&fs, "meta", 2, vec![1, 2, 3], true).unwrap();
            let vote = RaftRpc::RequestVote {
                term: 1, candidate_id: 1, last_log_index: 0, last_log_term: 0,
            };
            fs.set_faults(FaultConfig { sync_data_fault: fault, ..Default::default() });
            assert!(matches!(follower.step(vote.clone()), Err(MetaError::Io(_))));
            fs.set_faults(FaultConfig::default());
            assert!(matches!(follower.step(vote.clone()), Err(MetaError::StorageUnavailable)));
            assert!(matches!(follower.step(RaftRpc::AppendEntries {
                term: 1, leader_id: 1, prev_log_index: 0, prev_log_term: 0,
                entries: vec![], leader_commit: 0,
            }), Err(MetaError::StorageUnavailable)));
            drop(follower);
            fs.simulate_power_cut();
            let mut recovered = RaftCore::open(&fs, "meta", 2, vec![1, 2, 3], true).unwrap();
            let replies = recovered.step(vote).unwrap();
            assert!(replies.iter().any(|reply| matches!(reply.rpc,
                RaftRpc::RequestVoteReply { vote_granted: true, .. })));
        }
    }

    #[test]
    fn leader_sync_failure_is_unknown_outcome_and_blocks_further_proposals() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(9012);
            let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
            fs.set_faults(FaultConfig { sync_data_fault: fault, ..Default::default() });
            let cmd = KvCommand::Put { key: "key".into(), value: b"unknown".to_vec() };
            assert!(matches!(meta.execute(cmd.clone()), Err(MetaError::Io(_))));
            fs.set_faults(FaultConfig::default());
            assert!(matches!(meta.execute(cmd), Err(MetaError::StorageUnavailable)));
            assert!(meta.get("key").is_none());
            drop(meta);
            fs.simulate_power_cut();
            let recovered = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
            assert_eq!(recovered.get("key").is_some(), fault == SyncFault::AfterPersist);
            recovered.execute(KvCommand::Put { key: "next".into(), value: b"durable".to_vec() }).unwrap();
        }
    }

    #[test]
    fn revision_guards_reject_delete_recreate_aba_after_wal_recovery() {
        let fs = SimFs::new(9001);
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        meta.execute(KvCommand::Put { key: "key".into(), value: b"old".to_vec() }).unwrap();
        let old = meta.get("key").unwrap();
        meta.execute(KvCommand::Delete { key: "key".into() }).unwrap();
        meta.execute(KvCommand::Put { key: "key".into(), value: b"new".to_vec() }).unwrap();
        let new = meta.get("key").unwrap();
        assert_eq!(old.version, new.version); // ordinary version guards have ABA
        assert_ne!(old.mod_revision, new.mod_revision);
        drop(meta);
        fs.simulate_power_cut();
        let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
        let result = meta.execute(KvCommand::RevisionTxn {
            guards: vec![RevisionGuard { key: "key".into(), expected_mod_revision: old.mod_revision }],
            ops: vec![TxnOp::Delete { key: "key".into() }, TxnOp::Put { key: "retired".into(), value: b"old".to_vec() }],
        }).unwrap();
        assert!(matches!(result, KvResult::CasConflict { .. }));
        assert_eq!(meta.get("key").unwrap().value, b"new");
        assert!(meta.get("retired").is_none());
        assert!(matches!(meta.execute(KvCommand::RevisionTxn {
            guards: vec![RevisionGuard { key: "key".into(), expected_mod_revision: new.mod_revision }],
            ops: vec![TxnOp::Delete { key: "key".into() }],
        }).unwrap(), KvResult::TxnOk { .. }));
    }

    /// Deterministic in-memory Raft network simulator supporting partitions & message delivery.
    struct SimCluster {
        nodes: BTreeMap<u32, RaftCore<SimFs>>,
        partitioned_links: BTreeSet<(u32, u32)>,
        queue: VecDeque<(u32, OutboundMsg)>,
    }

    impl SimCluster {
        fn new(ids: &[u32]) -> Self {
            let mut nodes = BTreeMap::new();
            for &id in ids {
                let fs = SimFs::new(1000 + id as u64);
                let core = RaftCore::open(&fs, "raft", id, ids.to_vec(), true).unwrap();
                nodes.insert(id, core);
            }
            Self {
                nodes,
                partitioned_links: BTreeSet::new(),
                queue: VecDeque::new(),
            }
        }

        fn isolate_subset(&mut self, side_a: &[u32], side_b: &[u32]) {
            for &a in side_a {
                for &b in side_b {
                    self.partitioned_links.insert((a, b));
                    self.partitioned_links.insert((b, a));
                }
            }
        }

        fn heal_all(&mut self) {
            self.partitioned_links.clear();
        }

        fn enqueue_from(&mut self, from: u32, msgs: Vec<OutboundMsg>) {
            for m in msgs {
                self.queue.push_back((from, m));
            }
        }

        fn drain_network(&mut self) {
            while let Some((from, msg)) = self.queue.pop_front() {
                if self.partitioned_links.contains(&(from, msg.to)) {
                    continue; // Dropped by simulated network partition!
                }
                if let Some(dst) = self.nodes.get_mut(&msg.to) {
                    let replies = dst.step(msg.rpc).unwrap();
                    for r in replies {
                        self.queue.push_back((msg.to, r));
                    }
                }
            }
        }
    }

    #[test]
    fn heartbeat_does_not_confirm_or_commit_divergent_suffix() {
        let mut cluster = SimCluster::new(&[1, 2, 3]);
        let msgs = cluster.nodes.get_mut(&1).unwrap().start_election().unwrap();
        cluster.enqueue_from(1, msgs);
        cluster.drain_network();
        let common = cluster.nodes[&1].commit_index();
        cluster.isolate_subset(&[1], &[2, 3]);
        for value in [b"stale-1".as_slice(), b"stale-2".as_slice()] {
            let (_, msgs) = cluster.nodes.get_mut(&1).unwrap().propose(KvCommand::Put {
                key: "key".into(), value: value.to_vec(),
            }).unwrap();
            cluster.enqueue_from(1, msgs);
            cluster.drain_network();
        }
        assert_eq!(cluster.nodes[&1].commit_index(), common);
        let term = cluster.nodes[&1].current_term();
        let replies = cluster.nodes.get_mut(&1).unwrap().step(RaftRpc::AppendEntries {
            term: term + 1,
            leader_id: 2,
            prev_log_index: common,
            prev_log_term: term,
            entries: vec![],
            leader_commit: common + 2,
        }).unwrap();
        assert!(matches!(replies[0].rpc, RaftRpc::AppendEntriesReply {
            success: true, match_index, ..
        } if match_index == common), "{replies:?}");
        assert_eq!(cluster.nodes[&1].commit_index(), common);
        assert!(cluster.nodes[&1].get("key").is_none());
    }

    #[test]
    fn three_node_partition_discards_stale_leader_suffix() {
        let mut cluster = SimCluster::new(&[1, 2, 3]);
        let msgs = cluster.nodes.get_mut(&1).unwrap().start_election().unwrap();
        cluster.enqueue_from(1, msgs);
        cluster.drain_network();
        let (_, msgs) = cluster.nodes.get_mut(&1).unwrap().propose(KvCommand::Put {
            key: "committed".into(), value: b"before".to_vec(),
        }).unwrap();
        cluster.enqueue_from(1, msgs);
        cluster.drain_network();

        cluster.isolate_subset(&[1], &[2, 3]);
        let (stale_index, msgs) = cluster.nodes.get_mut(&1).unwrap().propose(KvCommand::Put {
            key: "conflict".into(), value: b"minority".to_vec(),
        }).unwrap();
        cluster.enqueue_from(1, msgs);
        cluster.drain_network();
        assert!(cluster.nodes[&1].commit_index() < stale_index);
        assert!(cluster.nodes[&1].get("conflict").is_none());

        let msgs = cluster.nodes.get_mut(&2).unwrap().start_election().unwrap();
        cluster.enqueue_from(2, msgs);
        cluster.drain_network();
        assert_eq!(cluster.nodes[&2].role(), RoleState::Leader);
        let (majority_index, msgs) = cluster.nodes.get_mut(&2).unwrap().propose(KvCommand::Put {
            key: "conflict".into(), value: b"majority".to_vec(),
        }).unwrap();
        cluster.enqueue_from(2, msgs);
        cluster.drain_network();
        assert!(cluster.nodes[&2].commit_index() >= majority_index);
        cluster.heal_all();
        let msgs = cluster.nodes[&2].broadcast_append_entries();
        cluster.enqueue_from(2, msgs);
        cluster.drain_network();
        assert_eq!(cluster.nodes[&1].role(), RoleState::Follower);
        for id in 1..=3 {
            assert_eq!(cluster.nodes[&id].get("committed").unwrap().value, b"before");
            assert_eq!(cluster.nodes[&id].get("conflict").unwrap().value, b"majority");
        }
    }

    #[test]
    fn replicated_write_can_commit_after_response_loss_and_leader_change() {
        let mut cluster = SimCluster::new(&[1, 2, 3]);

        // Elect node 1 and commit the leader's current-term no-op.
        let msgs = cluster.nodes.get_mut(&1).unwrap().start_election().unwrap();
        cluster.enqueue_from(1, msgs);
        cluster.drain_network();
        assert_eq!(cluster.nodes[&1].role(), RoleState::Leader);

        // Keep node 1's client-facing process alive. Deliver the proposal to node
        // 2 and deliberately drop its successful AppendEntries reply. At this
        // boundary execute() can return UncommittedIndex even though the entry
        // may later become committed.
        let (ambiguous_index, msgs) = cluster.nodes.get_mut(&1).unwrap().propose(
            KvCommand::Put {
                key: "ambiguous".into(),
                value: b"may-commit".to_vec(),
            },
        ).unwrap();
        let to_node_2 = msgs
            .into_iter()
            .find(|msg| msg.to == 2)
            .expect("leader must send the proposal to node 2");
        let replies = cluster.nodes.get_mut(&2).unwrap().step(to_node_2.rpc).unwrap();
        assert!(matches!(
            replies.first().map(|msg| &msg.rpc),
            Some(RaftRpc::AppendEntriesReply { success: true, match_index, .. })
                if *match_index >= ambiguous_index
        ));
        // The reply is lost. Node 1 has not learned that a majority stored the
        // entry, and neither node has applied the value yet.
        assert!(cluster.nodes[&1].commit_index() < ambiguous_index);
        assert!(cluster.nodes[&1].get("ambiguous").is_none());
        assert!(cluster.nodes[&2].get("ambiguous").is_none());

        // Node 2 wins the majority election. Its current-term no-op carries the
        // old proposal to node 3, which makes the old proposal commit even though
        // the original client never received the replication response.
        let msgs = cluster.nodes.get_mut(&2).unwrap().start_election().unwrap();
        cluster.enqueue_from(2, msgs);
        cluster.drain_network();
        assert_eq!(cluster.nodes[&2].role(), RoleState::Leader);
        assert_eq!(
            cluster.nodes[&2].get("ambiguous").unwrap().value,
            b"may-commit"
        );
        assert_eq!(
            cluster.nodes[&3].get("ambiguous").unwrap().value,
            b"may-commit"
        );

        // The old leader converges; the value is not silently rolled back.
        let msgs = cluster.nodes.get(&2).unwrap().broadcast_append_entries();
        cluster.enqueue_from(2, msgs);
        cluster.drain_network();
        for id in 1..=3 {
            assert_eq!(
                cluster.nodes[&id].get("ambiguous").unwrap().value,
                b"may-commit"
            );
        }
    }

    #[test]
    fn five_node_raft_survives_leader_kill_and_prevents_split_brain() {
        let mut cluster = SimCluster::new(&[1, 2, 3, 4, 5]);

        // Node 1 wins initial election in 5-node cluster
        let msgs = cluster.nodes.get_mut(&1).unwrap().start_election().unwrap();
        cluster.enqueue_from(1, msgs);
        cluster.drain_network();
        assert_eq!(cluster.nodes[&1].role(), RoleState::Leader);

        // Propose CAS create on `/projects/billing` -> committed across all 5 nodes!
        let (_, repl_msgs) = cluster
            .nodes
            .get_mut(&1)
            .unwrap()
            .propose(KvCommand::CompareAndSwap {
                key: "/projects/billing".into(),
                expected_version: 0,
                new_value: Some(b"gen1".to_vec()),
            })
            .unwrap();
        cluster.enqueue_from(1, repl_msgs);
        cluster.drain_network();

        for id in 1..=5 {
            assert_eq!(
                cluster.nodes[&id].get("/projects/billing").unwrap().value,
                b"gen1"
            );
        }

        // Split-brain test: isolate old leader (1) + follower (2) into a 2-node minority,
        // leaving [3, 4, 5] as a 3-node majority!
        cluster.isolate_subset(&[1, 2], &[3, 4, 5]);

        // Minority old leader 1 tries to overwrite `/projects/billing` -> MUST NOT COMMIT!
        let (minority_idx, min_msgs) = cluster
            .nodes
            .get_mut(&1)
            .unwrap()
            .propose(KvCommand::Put {
                key: "/projects/billing".into(),
                value: b"stale-split-brain-write".to_vec(),
            })
            .unwrap();
        cluster.enqueue_from(1, min_msgs);
        cluster.drain_network();
        assert!(
            cluster.nodes[&1].commit_index() < minority_idx,
            "minority partition must NEVER commit!"
        );
        assert_eq!(
            cluster.nodes[&1].get("/projects/billing").unwrap().value,
            b"gen1"
        );

        // Majority side [3, 4, 5] elects node 3 in higher term and commits `/projects/billing = gen2`!
        let elect_msgs = cluster.nodes.get_mut(&3).unwrap().start_election().unwrap();
        cluster.enqueue_from(3, elect_msgs);
        cluster.drain_network();
        assert_eq!(cluster.nodes[&3].role(), RoleState::Leader);

        let (maj_idx, maj_msgs) = cluster
            .nodes
            .get_mut(&3)
            .unwrap()
            .propose(KvCommand::CompareAndSwap {
                key: "/projects/billing".into(),
                expected_version: 1,
                new_value: Some(b"gen2-majority".to_vec()),
            })
            .unwrap();
        cluster.enqueue_from(3, maj_msgs);
        cluster.drain_network();
        assert_eq!(cluster.nodes[&3].commit_index(), maj_idx);

        // Heal partition: old leader 1 steps down, truncates uncommitted minority entry, and converges to `gen2-majority`!
        cluster.heal_all();
        let heal_hb = cluster.nodes[&3].broadcast_append_entries();
        cluster.enqueue_from(3, heal_hb);
        cluster.drain_network();

        assert_eq!(cluster.nodes[&1].role(), RoleState::Follower);
        for id in 1..=5 {
            let v = cluster.nodes[&id].get("/projects/billing").unwrap();
            assert_eq!(v.value, b"gen2-majority");
            assert_eq!(v.version, 2);
        }
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
        let items = reopened.scan_prefix("/s3/1/receipts/");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, "/s3/1/receipts/a.pdf");
        assert_eq!(items[1].0, "/s3/1/receipts/b.pdf");
    }
}
