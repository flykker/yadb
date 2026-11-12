pub mod kv;
pub mod raft;
pub mod wal;

pub use kv::{KvCommand, KvResult, KvValue, MetaStateMachine, WatchEvent};
pub use raft::{LocalMetaKv, LogEntry, OutboundMsg, RaftCore, RaftRpc, RoleState};
pub use wal::RaftWal;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("corrupt Raft WAL: {0}")]
    CorruptWal(&'static str),

    #[error("node is not Raft leader (known leader: {leader:?})")]
    NotLeader { leader: Option<u32> },

    #[error("log index {0} is not yet committed by quorum")]
    UncommittedIndex(u64),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use yadb_io::SimFs;

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
