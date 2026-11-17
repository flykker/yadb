//! Integration tests for the authenticated TCP transport.
//!
//! These tests use real TCP sockets on loopback and the durable file-backed
//! stores. They still run inside one process and do not cover power loss,
//! TLS confidentiality or a real multi-host deployment.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use openraft::network::RaftNetworkFactory;
use openraft::raft::VoteRequest;
use openraft::{BasicNode, Snapshot};

use crate::multiraft::{GroupIdentity, ReplicaIdentity};
use crate::openraft_net::{
    transport_counters, AuthenticatedTcpNetworkFactory, BoxFuture, RaftPeerServer, TransportConfig,
    WireRequest, WireResponse,
};
use crate::openraft_runtime::YadbRaft;
use crate::openraft_state_machine::DurableMetaStateMachine;
use crate::openraft_storage::DurableOpenRaftLog;
use crate::openraft_transport::{NodeSigningKey, PeerKeyRegistry};
use crate::KvCommand;

struct TestDir(std::path::PathBuf);

impl TestDir {
    fn new(tag: &str) -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "yadb-openraft-net-{tag}-{nonce}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn group() -> GroupIdentity {
    GroupIdentity {
        cluster: [21; 16],
        group: 5,
        incarnation: 1,
    }
}

fn identity(node: u32) -> ReplicaIdentity {
    ReplicaIdentity::new(group(), node, vec![1, 2, 3]).unwrap()
}

fn seed(node: u32) -> NodeSigningKey {
    let mut bytes = [0u8; 32];
    bytes[0] = node as u8;
    NodeSigningKey::from_seed(&bytes).unwrap()
}

fn handler(raft: YadbRaft) -> Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> {
    Arc::new(move |request| {
        let raft = raft.clone();
        Box::pin(async move {
            match request {
                WireRequest::AppendEntries(rpc) => {
                    let response = raft.append_entries(*rpc).await.expect("append entries");
                    WireResponse::AppendEntries(Box::new(response))
                }
                WireRequest::Vote(rpc) => {
                    let response = raft.vote(*rpc).await.expect("vote");
                    WireResponse::Vote(Box::new(response))
                }
                WireRequest::InstallSnapshot { vote, meta, data } => {
                    let snapshot = Snapshot {
                        meta,
                        snapshot: Box::new(Cursor::new(data)),
                    };
                    let response = raft
                        .install_full_snapshot(vote, snapshot)
                        .await
                        .expect("install snapshot");
                    WireResponse::InstallSnapshot(Box::new(response))
                }
            }
        })
    })
}

async fn wait_value(state: &DurableMetaStateMachine, key: &str, value: &[u8]) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if state.snapshot().get(key).is_some_and(|v| v.value == value) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replica did not apply the value");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_replicate_over_authenticated_tcp() {
    replicate_over_tcp(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_replicate_over_mutual_tls() {
    replicate_over_tcp(true).await;
}

async fn replicate_over_tcp(tls: bool) {
    tokio::time::timeout(Duration::from_secs(60), async {
        let dir = TestDir::new("cluster");
        // Bind the listeners first: the peer address map must contain the
        // real ports before any node starts talking to its peers.
        let mut listeners = Vec::new();
        let mut addrs = BTreeMap::new();
        for node in 1..=3u32 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            addrs.insert(node, addr);
            listeners.push(listener);
        }

        let keys: BTreeMap<u32, Arc<NodeSigningKey>> =
            (1..=3u32).map(|n| (n, Arc::new(seed(n)))).collect();
        let registry = Arc::new(
            PeerKeyRegistry::new(
                keys.iter()
                    .map(|(id, key)| (*id, key.public_key()))
                    .collect(),
            )
            .unwrap(),
        );

        let mut rafts = BTreeMap::new();
        let mut states = BTreeMap::new();
        for node in 1..=3u32 {
            let config = TransportConfig::new(
                identity(node),
                keys[&node].clone(),
                addrs.clone(),
                registry.clone(),
            );
            let config = if tls {
                config.with_tls(Arc::new(crate::openraft_tls::test_support::config(node)))
            } else {
                config
            };
            let network = AuthenticatedTcpNetworkFactory::new(config);
            let log =
                DurableOpenRaftLog::create(dir.0.join(format!("node-{node}/log")), &identity(node))
                    .unwrap();
            let state = DurableMetaStateMachine::create(
                dir.0.join(format!("node-{node}/state")),
                &identity(node),
            )
            .unwrap();
            let raft = YadbRaft::new(
                node,
                Arc::new(
                    openraft::Config {
                        snapshot_policy: openraft::SnapshotPolicy::Never,
                        ..Default::default()
                    }
                    .validate()
                    .unwrap(),
                ),
                network,
                log.clone(),
                state.clone(),
            )
            .await
            .unwrap();
            rafts.insert(node, raft);
            states.insert(node, state);
        }

        for node in 1..=3u32 {
            let config = TransportConfig::new(
                identity(node),
                keys[&node].clone(),
                addrs.clone(),
                registry.clone(),
            );
            let config = if tls {
                config.with_tls(Arc::new(crate::openraft_tls::test_support::config(node)))
            } else {
                config
            };
            let server = RaftPeerServer::from_listener(
                listeners.remove(0),
                config,
                handler(rafts[&node].clone()),
            );
            tokio::spawn(server.serve());
        }

        rafts[&1]
            .initialize(
                (1..=3)
                    .map(|id| (id, BasicNode::new(format!("node-{id}"))))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .unwrap();
        let leader = rafts[&1]
            .wait(Some(Duration::from_secs(15)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .unwrap()
            .current_leader
            .unwrap();
        rafts[&leader]
            .client_write(KvCommand::Put {
                key: "tcp".into(),
                value: b"replicated".to_vec(),
            })
            .await
            .unwrap();
        for state in states.values() {
            wait_value(state, "tcp", b"replicated").await;
        }
        for raft in rafts.values() {
            raft.shutdown().await.unwrap();
        }
    })
    .await
    .expect("TCP cluster test timed out");
}

/// Reads the process-wide rejected-envelope counter.
fn rejected_envelopes() -> u64 {
    transport_counters()["envelopes_rejected"]
        .as_u64()
        .expect("envelopes_rejected is a number")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rejects_unknown_address_and_forged_sender() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let node = 1u32;
        let key = Arc::new(seed(node));
        let registry = Arc::new(
            PeerKeyRegistry::new(BTreeMap::from([
                (node, key.public_key()),
                (2, seed(2).public_key()),
            ]))
            .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        // The server trusts only its own address, so a client connecting from
        // another ephemeral port is not a known peer.
        let mut trusted = BTreeMap::new();
        trusted.insert(node, server_addr);
        let server = RaftPeerServer::from_listener(
            listener,
            TransportConfig::new(identity(node), key.clone(), trusted, registry.clone())
                .with_required_address(true),
            Arc::new(|_request| Box::pin(async { unreachable!("no request is expected") })),
        );
        tokio::spawn(server.serve());

        let request = WireRequest::Vote(Box::new(VoteRequest {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        }));

        // A client whose address is not registered must not get an answer.
        let mut client_addrs = BTreeMap::new();
        client_addrs.insert(node, server_addr);
        let mut factory = AuthenticatedTcpNetworkFactory::new(TransportConfig::new(
            identity(node),
            key.clone(),
            client_addrs.clone(),
            registry.clone(),
        ));
        let mut network = factory.new_client(node, &BasicNode::new("node-1")).await;
        let result = network
            .call_for_test(request.clone(), Duration::from_secs(2))
            .await;
        assert!(
            result.is_err(),
            "server answered a connection from an unknown address"
        );

        // Use a separate server without strict address checks so the forged
        // signature reaches verification (TCP clients use ephemeral ports).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let forged_addr = listener.local_addr().unwrap();
        let server = RaftPeerServer::from_listener(
            listener,
            TransportConfig::new(identity(node), key, BTreeMap::new(), registry.clone()),
            Arc::new(|_request| Box::pin(async { panic!("forgery reached handler") })),
        );
        tokio::spawn(server.serve());
        client_addrs.insert(node, forged_addr);
        let mut factory = AuthenticatedTcpNetworkFactory::new(TransportConfig::new(
            identity(2),
            Arc::new(seed(99)),
            client_addrs,
            registry.clone(),
        ));
        let mut network = factory.new_client(node, &BasicNode::new("node-1")).await;
        let before = rejected_envelopes();
        let result = network.call_for_test(request, Duration::from_secs(2)).await;
        assert!(
            result.is_err(),
            "server accepted a message signed by a different key"
        );
        // The counter is process-wide and other tests run concurrently, so
        // assert only that this rejection was counted. Without it the counter
        // could stay at zero and report a clean transport while refusing
        // frames, which is exactly the silence it exists to remove.
        let after = rejected_envelopes();
        assert!(
            after > before,
            "a forged envelope was refused but not counted ({before} -> {after})"
        );
    })
    .await
    .expect("rejection test timed out");
}

#[tokio::test]
async fn mutual_tls_rejects_untrusted_expired_wrong_node_and_plaintext_clients() {
    use crate::openraft_tls::test_support;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let count = Arc::new(AtomicUsize::new(0));
    let handler: Arc<dyn Fn(WireRequest) -> BoxFuture<WireResponse> + Send + Sync> = {
        let count = count.clone();
        Arc::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                WireResponse::Vote(Box::new(openraft::raft::VoteResponse {
                    vote: openraft::Vote::new(1, 3),
                    vote_granted: true,
                    last_log_id: None,
                }))
            })
        })
    };
    let keys: BTreeMap<_, _> = (1..=3).map(|node| (node, Arc::new(seed(node)))).collect();
    let registry = Arc::new(
        PeerKeyRegistry::new(
            keys.iter()
                .map(|(id, key)| (*id, key.public_key()))
                .collect(),
        )
        .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peers = BTreeMap::from([(3, addr)]);
    let server = RaftPeerServer::from_listener(
        listener,
        TransportConfig::new(
            identity(3),
            keys[&3].clone(),
            peers.clone(),
            registry.clone(),
        )
        .with_tls(Arc::new(test_support::config(3))),
        handler,
    );
    let task = tokio::spawn(server.serve());
    let request = WireRequest::Vote(Box::new(VoteRequest {
        vote: openraft::Vote::new(1, 1),
        last_log_id: None,
    }));
    // The first case proves the listener and certificates work. Each remaining
    // case must fail before dispatch, not merely fail during client setup.
    for (node, tls_node, expected) in [
        (1, Some(1), true),
        (1, Some(4), false),
        (1, Some(5), false),
        (2, Some(1), false),
        (1, None, false),
    ] {
        let config = TransportConfig::new(
            identity(node),
            keys[&node].clone(),
            peers.clone(),
            registry.clone(),
        );
        let config = match tls_node {
            Some(cert) => config.with_tls(Arc::new(test_support::config(cert))),
            None => config,
        };
        let mut factory = AuthenticatedTcpNetworkFactory::new(config);
        let mut client = factory.new_client(3, &BasicNode::new("node-3.test")).await;
        let result = client
            .call_for_test(request.clone(), Duration::from_millis(500))
            .await;
        assert_eq!(
            result.is_ok(),
            expected,
            "node={node}, TLS fixture={tls_node:?}: {result:?}"
        );
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    task.abort();
    let _ = task.await;
}
