//! Durable leases used to fence garbage collection against live readers and
//! in-flight writers.
//!
//! A collector may only delete a fragment that no committed revision references.
//! Two independent facts are needed to prove that, and neither can be read from
//! the fragment store itself:
//!
//! * a consistent reader pins a metadata revision and then fetches fragments by
//!   the ids that revision names. A fragment that is unreferenced *now* may be
//!   referenced by that pinned revision, so the collector has to know that no
//!   such read is in flight;
//! * a writer appends fragments before it publishes the metadata that references
//!   them. A fragment that is unreferenced *now* may be about to become
//!   referenced, so the collector has to know that no such write is in flight.
//!
//! Both are recorded as short-lived leases in the same metadata group as the
//! manifests, so one read of that group sees the whole picture. A lease is
//! published before the reader/writer touches fragments and removed afterwards;
//! the expiry is only the backstop for a process that died in between, never a
//! substitute for the explicit release. Collectors refuse the whole pass while
//! any lease of either kind is live, and record their own collector lease so
//! two passes cannot interleave their mark and sweep phases.
//!
//! This is deliberately a fence, not a grace period: there is no window in which
//! the collector deletes something "recent enough" to be safe.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::{KvCommand, KvResult, LocalMetaKv, MetaError, RevisionGuard, TxnOp};
use yadb_io::StorageFs;

/// Prefix of every lease record. Nothing else in the metadata namespace may use it.
pub const LEASE_PREFIX: &str = "/gc-lease/";
pub const READER_PREFIX: &str = "/gc-lease/reader/";
pub const WRITER_PREFIX: &str = "/gc-lease/writer/";
pub const COLLECTOR_KEY: &str = "/gc-lease/collector";

/// Default lease lifetime. Long enough to cover a metadata round trip plus a
/// fragment read or write on a slow link, short enough that a crashed holder
/// blocks collection for a bounded time.
pub const DEFAULT_TTL_MS: u64 = 30_000;

/// Writer leases cover an append *and* the publication that names the appended
/// fragments. The collector may delete a fragment that is unreferenced at its
/// mark, so a writer whose lease expired before it published could lose that
/// fragment. The lifetime is therefore generous: a crash blocks collection for
/// up to this long, which is the safe direction.
pub const WRITER_TTL_MS: u64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseKind {
    Reader,
    Writer,
    Collector,
}

impl LeaseKind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Reader => READER_PREFIX,
            Self::Writer => WRITER_PREFIX,
            Self::Collector => LEASE_PREFIX,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcLease {
    pub id: String,
    pub kind: LeaseKind,
    pub project_id: u32,
    /// The metadata revision the holder is reading (readers) or the revision it
    /// observed before writing (writers). Informational: the collector refuses on
    /// any live lease and does not try to reason about this value.
    pub revision: u64,
    pub expires_at_ms: u64,
}

impl GcLease {
    pub fn is_live_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms > now_ms
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

/// Unique lease id. Uniqueness only has to hold between live leases in one
/// namespace, so a monotonic counter mixed with the wall clock and the process
/// id is enough; no randomness source is required.
pub fn new_lease_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in nanos.to_le_bytes().iter().chain(&pid.to_le_bytes()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    format!("{hash:016x}{n:08x}")
}

/// A lease held for the lifetime of this value. Dropping it releases the record
/// on a best-effort basis; [`Self::release`] reports the outcome instead of
/// discarding it, which is what write paths that must not leak a fence use.
pub struct GcLeaseGuard<'a, FS: StorageFs> {
    meta: &'a LocalMetaKv<FS>,
    key: String,
    lease: GcLease,
}

impl<'a, FS: StorageFs> GcLeaseGuard<'a, FS> {
    pub fn lease(&self) -> &GcLease {
        &self.lease
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Releases the record and reports the outcome. On error the lease stays in
    /// metadata until it expires, which blocks collection but never unblocks a
    /// deletion that should not happen.
    pub fn release(mut self) -> Result<(), MetaError> {
        let result = self.meta.execute(KvCommand::Delete {
            key: self.key.clone(),
        });
        if result.is_ok() {
            self.key.clear();
        }
        result.map(|_| ())
    }
}

impl<FS: StorageFs> Drop for GcLeaseGuard<'_, FS> {
    fn drop(&mut self) {
        if self.key.is_empty() {
            return;
        }
        let _ = self.meta.execute(KvCommand::Delete {
            key: std::mem::take(&mut self.key),
        });
    }
}

fn put_lease<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    kind: LeaseKind,
    project_id: u32,
    revision: u64,
    ttl_ms: u64,
) -> Result<(String, GcLease), MetaError> {
    if kind == LeaseKind::Collector {
        // The collector lease is a single fixed key with CAS semantics; it must
        // not be created through the reader/writer id-keyed path.
        return Err(MetaError::Routing(
            "the collector lease must be acquired with acquire_collector".into(),
        ));
    }
    let id = new_lease_id();
    let key = format!("{}{}", kind.prefix(), id);
    let lease = GcLease {
        id: id.clone(),
        kind,
        project_id,
        revision,
        expires_at_ms: now_ms().saturating_add(ttl_ms.max(1)),
    };
    let value = serde_json::to_vec(&lease)?;
    // A lease record must be durable before the holder reads or writes any
    // fragment: an acknowledged lease that is only in memory would let the
    // collector delete underneath it after a leader change.
    match meta.execute(KvCommand::RevisionTxn {
        guards: vec![RevisionGuard {
            key: key.clone(),
            expected_mod_revision: 0,
        }],
        ops: vec![TxnOp::Put {
            key: key.clone(),
            value,
        }],
    })? {
        KvResult::TxnOk { .. } => Ok((key, lease)),
        KvResult::CasConflict { .. } => Err(MetaError::Routing(
            "gc lease id collided with a live record".into(),
        )),
        other => Err(MetaError::Routing(format!(
            "gc lease publication returned an unexpected result: {other:?}"
        ))),
    }
}

/// Key of a lease record. Collector leases are addressed by their fixed key, so
/// this maps them there rather than to an id-keyed slot.
pub fn lease_key(lease: &GcLease) -> String {
    if lease.kind == LeaseKind::Collector {
        return COLLECTOR_KEY.to_string();
    }
    format!("{}{}", lease.kind.prefix(), lease.id)
}

/// Publishes a lease and returns the owned record. Use this when the holder
/// cannot keep a borrow of the metadata handle (for example across a
/// `spawn_blocking` boundary); [`release`] removes it by key.
pub fn acquire<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    kind: LeaseKind,
    project_id: u32,
    revision: u64,
    ttl_ms: u64,
) -> Result<GcLease, MetaError> {
    let (_, lease) = put_lease(meta, kind, project_id, revision, ttl_ms)?;
    Ok(lease)
}

/// Removes a lease published by [`acquire`]. An error leaves the record until it
/// expires, which blocks collection but never unblocks a deletion.
pub fn release<FS: StorageFs>(meta: &LocalMetaKv<FS>, lease: &GcLease) -> Result<(), MetaError> {
    meta.execute(KvCommand::Delete {
        key: lease_key(lease),
    })
    .map(|_| ())
}

/// Publishes a reader lease. Callers must hold it across the metadata read *and*
/// the fragment fetch, then release it.
pub fn acquire_reader<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    project_id: u32,
    revision: u64,
    ttl_ms: u64,
) -> Result<GcLeaseGuard<'_, FS>, MetaError> {
    let (key, lease) = put_lease(meta, LeaseKind::Reader, project_id, revision, ttl_ms)?;
    Ok(GcLeaseGuard { meta, key, lease })
}

/// Publishes a writer lease. Callers must publish it before appending fragments
/// and release it only after the publication transaction returns.
pub fn acquire_writer<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    project_id: u32,
    revision: u64,
    ttl_ms: u64,
) -> Result<GcLeaseGuard<'_, FS>, MetaError> {
    let (key, lease) = put_lease(meta, LeaseKind::Writer, project_id, revision, ttl_ms)?;
    Ok(GcLeaseGuard { meta, key, lease })
}

/// The collector lease. Only one pass may hold it at a time; a pass that cannot
/// acquire it must refuse rather than run concurrently with another.
pub struct CollectorGuard<'a, FS: StorageFs> {
    meta: &'a LocalMetaKv<FS>,
    key: String,
    lease: GcLease,
}

impl<'a, FS: StorageFs> CollectorGuard<'a, FS> {
    pub fn lease(&self) -> &GcLease {
        &self.lease
    }

    pub fn release(mut self) -> Result<(), MetaError> {
        let result = self.meta.execute(KvCommand::Delete {
            key: self.key.clone(),
        });
        if result.is_ok() {
            self.key.clear();
        }
        result.map(|_| ())
    }
}

impl<FS: StorageFs> Drop for CollectorGuard<'_, FS> {
    fn drop(&mut self) {
        if self.key.is_empty() {
            return;
        }
        let _ = self.meta.execute(KvCommand::Delete {
            key: std::mem::take(&mut self.key),
        });
    }
}

fn read_lease(key: &str, value: &[u8]) -> GcLease {
    match serde_json::from_slice::<GcLease>(value) {
        Ok(lease) => lease,
        Err(_) => {
            // An unreadable lease record is not evidence that nobody is reading.
            // Report it as a live, non-expiring lease so the caller refuses.
            GcLease {
                id: key.trim_start_matches(LEASE_PREFIX).to_string(),
                kind: LeaseKind::Collector,
                project_id: 0,
                revision: 0,
                expires_at_ms: u64::MAX,
            }
        }
    }
}

/// Removes expired lease records. Best effort: a record that cannot be removed
/// stays live (it is still counted) and the next pass tries again.
fn reap_expired<FS: StorageFs>(meta: &LocalMetaKv<FS>, now: u64) -> usize {
    let mut reaped = 0usize;
    for (key, value) in meta.scan_prefix(LEASE_PREFIX) {
        let lease = read_lease(&key, &value.value);
        if lease.is_live_at(now) {
            continue;
        }
        if matches!(
            meta.execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: key.clone(),
                    expected_mod_revision: value.mod_revision,
                }],
                ops: vec![TxnOp::Delete { key }],
            }),
            Ok(KvResult::TxnOk { .. })
        ) {
            reaped += 1;
        }
    }
    reaped
}

/// Every live reader or writer lease in the namespace, after reaping expired
/// records. The collector lease is reported separately by [`live_collector`].
///
/// A malformed record is reported as live: refusing to collect is always safer
/// than collecting on a namespace whose fence cannot be read.
pub fn live_leases<FS: StorageFs>(meta: &LocalMetaKv<FS>) -> Result<Vec<GcLease>, MetaError> {
    let now = now_ms();
    reap_expired(meta, now);
    let mut live = Vec::new();
    for prefix in [READER_PREFIX, WRITER_PREFIX] {
        for (key, value) in meta.scan_prefix(prefix) {
            let lease = read_lease(&key, &value.value);
            if lease.is_live_at(now) {
                live.push(lease);
            }
        }
    }
    // Any record under the lease prefix that is neither a reader, a writer nor
    // the collector is unparseable or foreign; refuse rather than ignore it.
    for (key, value) in meta.scan_prefix(LEASE_PREFIX) {
        if key == COLLECTOR_KEY || key.starts_with(READER_PREFIX) || key.starts_with(WRITER_PREFIX)
        {
            continue;
        }
        let lease = read_lease(&key, &value.value);
        if lease.is_live_at(now) {
            live.push(lease);
        }
    }
    Ok(live)
}

/// The current collector lease, if one is live. An expired record is reaped and
/// reported as absent so a crashed pass cannot block collection forever.
pub fn live_collector<FS: StorageFs>(meta: &LocalMetaKv<FS>) -> Option<GcLease> {
    let value = meta.get(COLLECTOR_KEY)?;
    let lease = read_lease(COLLECTOR_KEY, &value.value);
    lease.is_live_at(now_ms()).then_some(lease)
}

/// Acquires the collector lease, refusing when another pass already holds it.
/// `expected_mod_revision` is the value observed by [`live_collector`] (`0` when
/// absent), so a concurrent acquisition loses the race instead of interleaving.
pub fn acquire_collector<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    expected_mod_revision: u64,
    ttl_ms: u64,
) -> Result<CollectorGuard<'_, FS>, MetaError> {
    let id = new_lease_id();
    let lease = GcLease {
        id,
        kind: LeaseKind::Collector,
        project_id: 0,
        revision: meta.revision(),
        expires_at_ms: now_ms().saturating_add(ttl_ms.max(1)),
    };
    let value = serde_json::to_vec(&lease)?;
    match meta.execute(KvCommand::RevisionTxn {
        guards: vec![RevisionGuard {
            key: COLLECTOR_KEY.into(),
            expected_mod_revision,
        }],
        ops: vec![TxnOp::Put {
            key: COLLECTOR_KEY.into(),
            value,
        }],
    })? {
        KvResult::TxnOk { .. } => Ok(CollectorGuard {
            meta,
            key: COLLECTOR_KEY.into(),
            lease,
        }),
        KvResult::CasConflict { .. } => Err(MetaError::Routing(
            "another garbage collection pass holds the collector lease".into(),
        )),
        other => Err(MetaError::Routing(format!(
            "collector lease publication returned an unexpected result: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yadb_io::SimFs;

    fn meta() -> LocalMetaKv<SimFs> {
        LocalMetaKv::open_single_node(&SimFs::new(4242), "meta", 1).unwrap()
    }

    #[test]
    fn a_lease_is_visible_until_it_is_released() {
        let meta = meta();
        let reader = acquire_reader(&meta, 7, 11, DEFAULT_TTL_MS).unwrap();
        let live = live_leases(&meta).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].kind, LeaseKind::Reader);
        assert_eq!(live[0].project_id, 7);
        reader.release().unwrap();
        assert!(live_leases(&meta).unwrap().is_empty());
    }

    #[test]
    fn an_expired_lease_is_reaped_and_a_malformed_one_is_not() {
        let meta = meta();
        let expired = acquire_writer(&meta, 1, 1, 1).unwrap();
        // Force the record into the past without waiting for the clock.
        let key = expired.key().to_string();
        let mut lease = expired.lease().clone();
        lease.expires_at_ms = now_ms() - 1;
        meta.execute(KvCommand::Put {
            key: key.clone(),
            value: serde_json::to_vec(&lease).unwrap(),
        })
        .unwrap();
        drop(expired);
        meta.execute(KvCommand::Put {
            key: key.clone(),
            value: serde_json::to_vec(&lease).unwrap(),
        })
        .unwrap();
        assert!(live_leases(&meta).unwrap().is_empty());
        assert!(meta.get(&key).is_none(), "expired record must be reaped");

        meta.execute(KvCommand::Put {
            key: format!("{READER_PREFIX}broken"),
            value: b"not json".to_vec(),
        })
        .unwrap();
        let live = live_leases(&meta).unwrap();
        assert_eq!(live.len(), 1, "an unreadable lease must be treated as live");
    }

    #[test]
    fn collector_lease_is_exclusive_and_reaped_when_expired() {
        let meta = meta();
        assert!(live_collector(&meta).is_none());
        let first = acquire_collector(&meta, 0, DEFAULT_TTL_MS).unwrap();
        assert!(live_collector(&meta).is_some());
        assert!(acquire_collector(&meta, 0, DEFAULT_TTL_MS).is_err());
        first.release().unwrap();
        assert!(live_collector(&meta).is_none());

        let second = acquire_collector(&meta, 0, 1).unwrap();
        let mut lease = second.lease().clone();
        lease.expires_at_ms = now_ms() - 1;
        drop(second);
        meta.execute(KvCommand::Put {
            key: COLLECTOR_KEY.into(),
            value: serde_json::to_vec(&lease).unwrap(),
        })
        .unwrap();
        assert!(live_collector(&meta).is_none());
        // A fresh pass can take over an expired record.
        let value = meta.get(COLLECTOR_KEY).unwrap();
        let third = acquire_collector(&meta, value.mod_revision, DEFAULT_TTL_MS).unwrap();
        third.release().unwrap();
    }
}
