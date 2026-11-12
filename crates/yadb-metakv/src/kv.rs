use std::collections::BTreeMap;
use std::sync::mpsc::Sender;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvValue {
    pub value: Vec<u8>,
    /// Per-key version (starts at 1 on creation, increments on every update; reset on delete).
    pub version: u64,
    /// Global cluster revision at which this key was last modified.
    pub mod_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasGuard {
    pub key: String,
    pub expected_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnOp {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    Put {
        key: String,
        value: Vec<u8>,
    },
    Delete {
        key: String,
    },
    /// Atomic Compare-And-Swap:
    /// - `expected_version == 0`: succeeds iff the key does NOT currently exist (create-only).
    /// - `expected_version > 0` : succeeds iff the key exists with `version == expected_version`.
    CompareAndSwap {
        key: String,
        expected_version: u64,
        new_value: Option<Vec<u8>>,
    },
    /// Atomic multi-key Raft transaction: verifies all `guards` first; if all match,
    /// applies every `Put`/`Delete` in `ops` atomically under a single cluster revision!
    Txn {
        guards: Vec<CasGuard>,
        ops: Vec<TxnOp>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvResult {
    PutOk { revision: u64, version: u64 },
    Deleted { existed: bool, revision: u64 },
    CasOk { revision: u64, version: u64 },
    CasConflict { current_version: u64 },
    TxnOk {
        revision: u64,
        versions: BTreeMap<String, u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchEvent {
    Put {
        key: String,
        value: Vec<u8>,
        version: u64,
        revision: u64,
    },
    Delete {
        key: String,
        revision: u64,
    },
}

struct Watcher {
    prefix: String,
    tx: Sender<WatchEvent>,
}

/// Ordered deterministic state machine applied identically on all Raft replicas.
#[derive(Default)]
pub struct MetaStateMachine {
    revision: u64,
    kv: BTreeMap<String, KvValue>,
    watchers: Vec<Watcher>,
}

impl MetaStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn get(&self, key: &str) -> Option<KvValue> {
        self.kv.get(key).cloned()
    }

    /// Lexicographically ordered prefix range scan (for S3 `ListObjectsV2`, shard maps, RBAC sync).
    pub fn scan_prefix(&self, prefix: &str) -> Vec<(String, KvValue)> {
        self.kv
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn watch_prefix(&mut self, prefix: impl Into<String>, tx: Sender<WatchEvent>) {
        self.watchers.push(Watcher {
            prefix: prefix.into(),
            tx,
        });
    }

    pub fn apply(&mut self, cmd: KvCommand) -> KvResult {
        match cmd {
            KvCommand::Put { key, value } => {
                self.revision += 1;
                let rev = self.revision;
                let next_ver = self.kv.get(&key).map_or(1, |cur| cur.version + 1);
                self.kv.insert(
                    key.clone(),
                    KvValue {
                        value: value.clone(),
                        version: next_ver,
                        mod_revision: rev,
                    },
                );
                self.notify(WatchEvent::Put {
                    key,
                    value,
                    version: next_ver,
                    revision: rev,
                });
                KvResult::PutOk {
                    revision: rev,
                    version: next_ver,
                }
            }
            KvCommand::Delete { key } => {
                if self.kv.remove(&key).is_some() {
                    self.revision += 1;
                    let rev = self.revision;
                    self.notify(WatchEvent::Delete { key, revision: rev });
                    KvResult::Deleted {
                        existed: true,
                        revision: rev,
                    }
                } else {
                    KvResult::Deleted {
                        existed: false,
                        revision: self.revision,
                    }
                }
            }
            KvCommand::CompareAndSwap {
                key,
                expected_version,
                new_value,
            } => {
                let cur_ver = self.kv.get(&key).map_or(0, |v| v.version);
                if cur_ver != expected_version {
                    return KvResult::CasConflict {
                        current_version: cur_ver,
                    };
                }
                self.revision += 1;
                let rev = self.revision;
                match new_value {
                    Some(value) => {
                        let next_ver = cur_ver + 1;
                        self.kv.insert(
                            key.clone(),
                            KvValue {
                                value: value.clone(),
                                version: next_ver,
                                mod_revision: rev,
                            },
                        );
                        self.notify(WatchEvent::Put {
                            key,
                            value,
                            version: next_ver,
                            revision: rev,
                        });
                        KvResult::CasOk {
                            revision: rev,
                            version: next_ver,
                        }
                    }
                    None => {
                        self.kv.remove(&key);
                        self.notify(WatchEvent::Delete { key, revision: rev });
                        KvResult::CasOk {
                            revision: rev,
                            version: 0,
                        }
                    }
                }
            }
            KvCommand::Txn { guards, ops } => {
                for g in &guards {
                    let cur_ver = self.kv.get(&g.key).map_or(0, |v| v.version);
                    if cur_ver != g.expected_version {
                        return KvResult::CasConflict {
                            current_version: cur_ver,
                        };
                    }
                }
                self.revision += 1;
                let rev = self.revision;
                let mut versions = BTreeMap::new();
                for op in ops {
                    match op {
                        TxnOp::Put { key, value } => {
                            let next_ver = self.kv.get(&key).map_or(1, |cur| cur.version + 1);
                            self.kv.insert(
                                key.clone(),
                                KvValue {
                                    value: value.clone(),
                                    version: next_ver,
                                    mod_revision: rev,
                                },
                            );
                            versions.insert(key.clone(), next_ver);
                            self.notify(WatchEvent::Put {
                                key,
                                value,
                                version: next_ver,
                                revision: rev,
                            });
                        }
                        TxnOp::Delete { key } => {
                            if self.kv.remove(&key).is_some() {
                                self.notify(WatchEvent::Delete {
                                    key: key.clone(),
                                    revision: rev,
                                });
                            }
                            versions.insert(key, 0);
                        }
                    }
                }
                KvResult::TxnOk {
                    revision: rev,
                    versions,
                }
            }
        }
    }

    fn notify(&mut self, ev: WatchEvent) {
        let key = match &ev {
            WatchEvent::Put { key, .. } | WatchEvent::Delete { key, .. } => key.as_str(),
        };
        self.watchers.retain(|w| {
            if key.starts_with(&w.prefix) {
                w.tx.send(ev.clone()).is_ok()
            } else {
                true
            }
        });
    }
}
