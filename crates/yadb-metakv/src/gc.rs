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
//! On Multi-Raft there is one metadata group per project, so every record is
//! scoped to the group that owns the project: `/gc/g<group>/collector`,
//! `/gc/g<group>/reader/<id>` and `/gc/g<group>/writer/<id>`. A pass over one
//! group then fences exactly that group, and a pass over another group neither
//! blocks it nor is blocked by it. The single legacy namespace keeps its
//! original keys (`/gc-lease/...`), unchanged.
//!
//! This is deliberately a fence, not a grace period: there is no window in which
//! the collector deletes something "recent enough" to be safe.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::{KvCommand, KvResult, LocalMetaKv, MetaError, RevisionGuard, TxnOp};
use yadb_io::StorageFs;

/// Prefix of every legacy lease record. Nothing else in the metadata namespace
/// may use it. Multi-Raft uses [`GROUP_GC_NAMESPACE`] instead, because a lease
/// has to live in the group whose manifests it fences.
pub const LEASE_PREFIX: &str = "/gc-lease/";
pub const READER_PREFIX: &str = "/gc-lease/reader/";
pub const WRITER_PREFIX: &str = "/gc-lease/writer/";
pub const COLLECTOR_KEY: &str = "/gc-lease/collector";
/// Namespace of the per-group GC records on Multi-Raft: `/gc/g<group>/`.
///
/// The router treats the segment after `g` as a canonical group id, exactly
/// like `/idempotency/g<group>/`, so one key family names one group and cannot
/// be confused with a project namespace.
pub const GROUP_GC_NAMESPACE: &str = "/gc/g";

/// Collector record of one metadata group. The key is what makes the fence
/// per-group: admission for a project guards on its own group's record only.
pub fn group_collector_key(group: u64) -> String {
    format!("{GROUP_GC_NAMESPACE}{group}/collector")
}

/// Prefix of one group's reader or writer lease records.
pub fn group_lease_prefix(group: u64) -> String {
    format!("{GROUP_GC_NAMESPACE}{group}/")
}
/// Highest revision whose dead fragments a completed pass may already have
/// deleted. A reader whose applied view is older than this cannot prove that the
/// manifests it sees are still complete, so it must refuse instead of reading.
pub const WATERMARK_KEY: &str = "/gc/watermark";

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

/// The metadata namespace a lease lives in.
///
/// Legacy metadata has exactly one namespace, addressed by the original
/// `/gc-lease/` keys. Multi-Raft has one namespace per data group, and a lease
/// must live in the group whose manifests it protects: a writer lease published
/// in another group would not be visible to that group's collector, and the
/// collector's admission guard would not exclude it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseScope {
    Legacy,
    Group(u64),
}

impl LeaseScope {
    fn collector_key(self) -> String {
        match self {
            Self::Legacy => COLLECTOR_KEY.to_string(),
            Self::Group(group) => group_collector_key(group),
        }
    }

    fn lease_prefix(self) -> String {
        match self {
            Self::Legacy => LEASE_PREFIX.to_string(),
            Self::Group(group) => group_lease_prefix(group),
        }
    }

    fn reader_prefix(self) -> String {
        match self {
            Self::Legacy => READER_PREFIX.to_string(),
            Self::Group(group) => format!("{}{}", group_lease_prefix(group), "reader/"),
        }
    }

    fn writer_prefix(self) -> String {
        match self {
            Self::Legacy => WRITER_PREFIX.to_string(),
            Self::Group(group) => format!("{}{}", group_lease_prefix(group), "writer/"),
        }
    }

    fn of(lease: &GcLease) -> Self {
        lease.group.map_or(Self::Legacy, Self::Group)
    }
}

/// One metadata group a collection pass can run over, with the projects whose
/// fragments its manifests may name.
///
/// A fragment id carries its project id and a manifest only names fragments of
/// its own project, so a pass that marks one group cannot orphan another
/// group's fragments. That is what makes a per-group sweep safe without a
/// cross-group snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcGroupScope {
    pub group_id: u64,
    /// `None` when the deployment has a single group and no explicit project
    /// routing, so every project's fragments belong to it.
    pub project_ids: Option<std::collections::BTreeSet<u32>>,
}

impl GcGroupScope {
    pub fn covers(&self, project_id: u32) -> bool {
        self.project_ids
            .as_ref()
            .is_none_or(|ids| ids.contains(&project_id))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseKind {
    Reader,
    Writer,
    Collector,
}

impl LeaseKind {
    /// Legacy key prefix. Group-scoped records are addressed through
    /// [`LeaseScope`] instead.
    #[allow(dead_code)]
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
    /// Metadata group whose namespace holds this record. `None` is the single
    /// legacy namespace; a legacy record never carries this field.
    ///
    /// `skip_serializing_if` is part of the legacy contract, not cosmetics: a
    /// record written before per-group GC must be byte-identical to the one
    /// written after it, otherwise a mixed-version deployment produces a
    /// different `revision` for the same logical event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<u64>,
    /// The metadata revision the holder is reading (readers) or the revision it
    /// observed before writing (writers). Informational: the collector refuses on
    /// any live lease and does not try to reason about this value.
    pub revision: u64,
    pub expires_at_ms: u64,
    /// The metadata `mod_revision` of this record at the moment it was published,
    /// filled in by the acquirer (never read back from the record itself). It is
    /// the ownership token for every later operation on the lease: a renewal,
    /// release or publication guard that carries a stale value loses the CAS
    /// instead of touching a record that now belongs to somebody else.
    #[serde(default)]
    pub mod_revision: u64,
}

impl GcLease {
    pub fn is_live_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms > now_ms
    }

    /// A guard that only matches this exact publication of the record.
    pub fn guard(&self) -> RevisionGuard {
        RevisionGuard {
            key: lease_key(self),
            expected_mod_revision: self.mod_revision,
        }
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

    /// A transaction guard that only commits while this exact lease record is
    /// still the one this holder published.
    ///
    /// Publication paths join this guard to the transaction that names the
    /// fragments they appended. A writer whose lease expired (or was reaped) can
    /// then no longer publish a manifest over fragments a pass has already
    /// reclaimed: the transaction loses its CAS and the write is refused.
    pub fn guard(&self) -> RevisionGuard {
        RevisionGuard {
            key: self.key.clone(),
            expected_mod_revision: self.lease.mod_revision,
        }
    }

    /// Extends the lease to a full `ttl_ms` from now, refusing if this holder no
    /// longer owns the record, if the record's lifetime already ran out, or if the
    /// write is acknowledged only after the deadline it was extending.
    ///
    /// Long writes and reads call this before the step that must stay fenced
    /// (the publication transaction, the fragment fetch), so a slow operation
    /// does not silently run past the lifetime its fence promised.
    pub fn renew(&mut self, ttl_ms: u64) -> Result<(), MetaError> {
        self.lease = extend(
            self.meta,
            &self.key,
            &self.lease,
            ttl_ms,
            lost_writer_or_reader(&self.lease),
            &now_ms,
        )?;
        Ok(())
    }

    /// Verifies the record is still the one this holder published.
    pub fn ensure_live(&self) -> Result<(), MetaError> {
        ensure_live(self.meta, &self.lease)
    }

    /// Releases the record and reports the outcome. On error the lease stays in
    /// metadata until it expires, which blocks collection but never unblocks a
    /// deletion that should not happen.
    pub fn release(mut self) -> Result<(), MetaError> {
        let result = release(self.meta, &self.lease);
        if result.is_ok() {
            self.key.clear();
        }
        result
    }
}

impl<FS: StorageFs> Drop for GcLeaseGuard<'_, FS> {
    fn drop(&mut self) {
        if self.key.is_empty() {
            return;
        }
        // Guarded like an explicit release: a lease that expired and was reaped
        // must not have its key deleted after a later holder republished it.
        let _ = release(self.meta, &self.lease);
    }
}

fn kind_name(kind: LeaseKind) -> &'static str {
    match kind {
        LeaseKind::Reader => "reader",
        LeaseKind::Writer => "writer",
        LeaseKind::Collector => "collector",
    }
}

/// True while this exact publication of the lease record is still in metadata
/// *and* has not run out.
///
/// A reader calls this after fetching fragments: if its lease expired mid-read,
/// a pass may have deleted what it just read, so the read is refused instead of
/// returning a possibly incomplete answer.
///
/// Ownership and liveness are separate checks because the record outlives the
/// lease: the collector only reaps an expired record on its next pass, so the
/// record can still be present, with the holder's `mod_revision`, after the
/// lease it encodes has stopped fencing anything. Reporting the two cases
/// separately tells an operator whether the record was reaped or replaced, or
/// merely outlived its TTL while nobody collected yet.
pub fn ensure_live<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    lease: &GcLease,
) -> Result<(), MetaError> {
    let current = meta.try_get(&lease_key(lease))?;
    let owned = current
        .as_ref()
        .is_some_and(|kv| kv.mod_revision == lease.mod_revision && lease.mod_revision != 0);
    if !owned {
        return Err(MetaError::Routing(format!(
            "gc {} lease {} was lost (reaped or replaced) while it was in use; the operation is refused rather than completed under a fence that no longer holds",
            kind_name(lease.kind),
            lease.id
        )));
    }
    if !lease.is_live_at(now_ms()) {
        return Err(MetaError::Routing(format!(
            "gc {} lease {} expired at {} ms but its record is still present and unreaped; the operation is refused rather than completed under a fence that no longer holds",
            kind_name(lease.kind),
            lease.id,
            lease.expires_at_ms
        )));
    }
    Ok(())
}

/// The revision at which a pass may already have deleted fragments. Absent means
/// no pass has ever run, so every applied revision is safe.
pub fn watermark<FS: StorageFs>(meta: &LocalMetaKv<FS>) -> Result<u64, MetaError> {
    match meta.try_get(WATERMARK_KEY)? {
        None => Ok(0),
        Some(kv) => serde_json::from_slice::<u64>(&kv.value)
            .map_err(|e| MetaError::Routing(format!("gc watermark is unreadable: {e}"))),
    }
}

/// Raises the watermark to `revision`, never lowers it.
///
/// The collector calls this *before* it re-checks the reader leases and takes its
/// mark, so the two sides close the window between them: a reader that publishes
/// its lease before that re-check is seen and the pass refuses; a reader that
/// publishes after it has already applied a revision at least as new as the
/// watermark, so [`reader_is_safe`] passes. A reader that is still behind refuses
/// instead of reading a manifest whose fragments may be gone.
pub fn publish_watermark<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    revision: u64,
) -> Result<u64, MetaError> {
    let (current, mod_revision) = match meta.try_get(WATERMARK_KEY)? {
        None => (0, 0),
        Some(kv) => (
            serde_json::from_slice::<u64>(&kv.value)
                .map_err(|e| MetaError::Routing(format!("gc watermark is unreadable: {e}")))?,
            kv.mod_revision,
        ),
    };
    let next = current.max(revision);
    if next == current && mod_revision != 0 {
        return Ok(next);
    }
    let value = serde_json::to_vec(&next)?;
    match meta.execute(KvCommand::RevisionTxn {
        guards: vec![RevisionGuard {
            key: WATERMARK_KEY.into(),
            expected_mod_revision: mod_revision,
        }],
        ops: vec![TxnOp::Put {
            key: WATERMARK_KEY.into(),
            value,
        }],
    })? {
        KvResult::TxnOk { .. } => Ok(next),
        KvResult::CasConflict { .. } => Err(MetaError::Routing(
            "gc watermark changed during the pass; another collection ran".into(),
        )),
        other => Err(MetaError::Routing(format!(
            "gc watermark publication returned an unexpected result: {other:?}"
        ))),
    }
}

/// Refuses when this node's applied view is older than the last sweep's mark.
///
/// Call it *after* publishing the reader lease: a lease published before the
/// collector's re-check makes the pass refuse, and one published after it has
/// necessarily applied a revision at least as new as the watermark.
pub fn reader_is_safe<FS: StorageFs>(meta: &LocalMetaKv<FS>) -> Result<(), MetaError> {
    let mark = watermark(meta)?;
    let applied = meta.try_revision()?;
    if applied < mark {
        return Err(MetaError::Routing(format!(
            "this node applied revision {applied} but garbage collection has swept up to {mark}; refusing to read a manifest whose fragments may have been reclaimed"
        )));
    }
    Ok(())
}

/// Publishes one reader or writer lease record.
///
/// The transaction is guarded on two keys at once: the lease's own (never
/// reused) id slot, and the collector record being **absent**. That second guard
/// is what makes admission mutually exclusive with a collection pass:
///
/// * a reader/writer that publishes before the collector does is visible to the
///   collector's post-acquisition fence check, so the pass refuses;
/// * one that publishes after it can only succeed if the collector record is
///   gone, and if the collector published in between the guard loses the CAS.
///
/// A plain "read the collector record, then publish" sequence would not be
/// enough: the collector could publish between the read and the write, take its
/// mark, and then delete fragments the lease was supposed to protect.
fn put_lease<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    kind: LeaseKind,
    project_id: u32,
    revision: u64,
    ttl_ms: u64,
) -> Result<(String, GcLease), MetaError> {
    // The lease must land in the group that owns the project's manifests. On
    // legacy metadata that is the single namespace, so the keys are unchanged.
    let scope = match meta.gc_group_for_project(project_id)? {
        Some(group) => LeaseScope::Group(group),
        None => LeaseScope::Legacy,
    };
    put_lease_in(meta, scope, kind, project_id, revision, ttl_ms)
}

fn put_lease_in<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    scope: LeaseScope,
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
    let collector_key = scope.collector_key();
    let prefix = match kind {
        LeaseKind::Reader => scope.reader_prefix(),
        LeaseKind::Writer => scope.writer_prefix(),
        LeaseKind::Collector => unreachable!(),
    };
    for _ in 0..2 {
        let id = new_lease_id();
        let key = format!("{prefix}{id}");
        let mut lease = GcLease {
            id: id.clone(),
            kind,
            project_id,
            group: match scope {
                LeaseScope::Legacy => None,
                LeaseScope::Group(group) => Some(group),
            },
            revision,
            expires_at_ms: now_ms().saturating_add(ttl_ms.max(1)),
            mod_revision: 0,
        };
        let value = serde_json::to_vec(&lease)?;
        // A lease record must be durable before the holder reads or writes any
        // fragment: an acknowledged lease that is only in memory would let the
        // collector delete underneath it after a leader change.
        match meta.execute(KvCommand::RevisionTxn {
            guards: vec![
                RevisionGuard {
                    key: key.clone(),
                    expected_mod_revision: 0,
                },
                RevisionGuard {
                    key: collector_key.clone(),
                    expected_mod_revision: 0,
                },
            ],
            ops: vec![TxnOp::Put {
                key: key.clone(),
                value,
            }],
        })? {
            KvResult::TxnOk { revision, .. } => {
                // Every key written by one transaction shares its revision, so
                // this is the record's own `mod_revision` without a read-back.
                lease.mod_revision = revision;
                return Ok((key, lease));
            }
            KvResult::CasConflict { .. } => {
                // Either the id collided (astronomically unlikely) or a
                // collection pass holds the collector record. Reap an expired
                // collector record and retry once; otherwise refuse, because
                // publishing now would race the pass.
                if live_collector_in_scope(meta, scope)?.is_some() {
                    return Err(MetaError::Routing(
                        "garbage collection is running; the read or write was refused rather than \
                         admitted while fragments are being reclaimed"
                            .into(),
                    ));
                }
                reap_expired(meta, &scope.lease_prefix(), now_ms())?;
            }
            other => {
                return Err(MetaError::Routing(format!(
                    "gc lease publication returned an unexpected result: {other:?}"
                )))
            }
        }
    }
    Err(MetaError::Routing(
        "gc lease publication kept losing its guard; refusing to read or write".into(),
    ))
}

/// Key of a lease record. Collector leases are addressed by their fixed key, so
/// this maps them there rather than to an id-keyed slot.
pub fn lease_key(lease: &GcLease) -> String {
    let scope = LeaseScope::of(lease);
    if lease.kind == LeaseKind::Collector {
        return scope.collector_key();
    }
    let prefix = match lease.kind {
        LeaseKind::Reader => scope.reader_prefix(),
        LeaseKind::Writer => scope.writer_prefix(),
        LeaseKind::Collector => unreachable!(),
    };
    format!("{prefix}{}", lease.id)
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

/// Removes a lease published by [`acquire`], but only while it is still the
/// record that holder published.
///
/// A record that is already gone, or whose `mod_revision` no longer matches (it
/// expired, was reaped, and possibly republished by somebody else), is not ours
/// to delete: reporting success is correct, deleting would remove a successor's
/// fence.
pub fn release<FS: StorageFs>(meta: &LocalMetaKv<FS>, lease: &GcLease) -> Result<(), MetaError> {
    if lease.mod_revision == 0 {
        return Err(MetaError::Routing(
            "refusing to release a lease without the revision it was published at".into(),
        ));
    }
    let key = lease_key(lease);
    match meta.try_get(&key)? {
        None => return Ok(()),
        Some(kv) if kv.mod_revision != lease.mod_revision => return Ok(()),
        Some(_) => {}
    }
    match meta.execute(KvCommand::RevisionTxn {
        guards: vec![lease.guard()],
        ops: vec![TxnOp::Delete { key }],
    })? {
        KvResult::TxnOk { .. } | KvResult::CasConflict { .. } => Ok(()),
        other => Err(MetaError::Routing(format!(
            "gc lease release returned an unexpected result: {other:?}"
        ))),
    }
}

/// Refuses to renew a lease whose record is still present but whose lifetime has
/// run out.
///
/// `renew` used to extend any record this holder still owned, including one that
/// had already expired but had not been reaped yet. That resurrected a fence
/// that had stopped fencing: a collector was entitled to treat the record as
/// dead, and `begin_read_lease` took its "renewed" branch instead of publishing a
/// fresh record, so a read could start under a lease that no longer excluded
/// anything. Expiry is checked before the transaction, so the stale record is
/// left untouched rather than rewritten with a new deadline.
fn renew_refusal(lease: &GcLease, now: u64) -> Option<MetaError> {
    (!lease.is_live_at(now)).then(|| {
        MetaError::Routing(format!(
            "gc {} lease {} expired at {} ms and cannot be renewed; publish a fresh lease instead of extending a fence that no longer holds",
            kind_name(lease.kind),
            lease.id,
            lease.expires_at_ms
        ))
    })
}

/// The single renewal implementation behind [`renew`], [`GcLeaseGuard::renew`]
/// and [`CollectorGuard::renew`].
///
/// They were three copies of the same transaction, and one of them silently kept
/// the old behaviour when the others learned to refuse an expired record. Funnelling
/// them through one function is what stops that from happening again.
///
/// Two refusals matter, and the second is why this is more than a guarded write:
///
/// * before the transaction, the record must still be live;
/// * after it, the write must have been acknowledged before the deadline it was
///   extending. A compare-and-swap proves the holder still *owns* the record; it
///   does not prove the record was still *live* when the write was applied. The
///   deadline can pass during the round trip, and the write then replaces an
///   expired record with a live one — resurrecting a fence that another pass is
///   entitled to treat as dead, and letting this holder delete or publish under a
///   fence that stopped holding before its own check returned. The acknowledgement
///   time bounds the apply time from above, so refusing whenever the
///   acknowledgement is at or past the deadline is the conservative direction: a
///   holder that cannot prove its renewal landed in time stops instead.
///
/// The write has already been applied when the second refusal fires, so the
/// record stays behind with its extended deadline and expires on its own. The
/// caller's release loses its CAS and leaves it, which is the safe direction:
/// admission stays refused until the record lapses rather than being admitted
/// under a fence this holder is abandoning.
///
/// `clock` is a parameter so a test can place the two readings on either side of
/// the deadline without sleeping; production passes [`now_ms`].
fn extend<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    key: &str,
    lease: &GcLease,
    ttl_ms: u64,
    lost: String,
    clock: &dyn Fn() -> u64,
) -> Result<GcLease, MetaError> {
    if lease.mod_revision == 0 {
        return Err(MetaError::Routing(
            "refusing to renew a lease without the revision it was published at".into(),
        ));
    }
    let checked = clock();
    if let Some(error) = renew_refusal(lease, checked) {
        return Err(error);
    }
    let mut next = lease.clone();
    next.expires_at_ms = checked.saturating_add(ttl_ms.max(1));
    let value = serde_json::to_vec(&next)?;
    let revision = match meta.execute(KvCommand::RevisionTxn {
        guards: vec![RevisionGuard {
            key: key.into(),
            expected_mod_revision: lease.mod_revision,
        }],
        ops: vec![TxnOp::Put {
            key: key.into(),
            value,
        }],
    })? {
        KvResult::TxnOk { revision, .. } => revision,
        KvResult::CasConflict { .. } => return Err(MetaError::Routing(lost)),
        other => {
            return Err(MetaError::Routing(format!(
                "gc lease renewal returned an unexpected result: {other:?}"
            )))
        }
    };
    // `next.expires_at_ms` can be earlier than the old deadline when the caller
    // renews with a shorter TTL, so the binding deadline is the earlier of the
    // two: the record must be live at the acknowledgement either way.
    let deadline = lease.expires_at_ms.min(next.expires_at_ms);
    let acknowledged = clock();
    if acknowledged >= deadline {
        return Err(MetaError::Routing(format!(
            "gc {} lease {} renewal was acknowledged at {acknowledged} ms, at or after the deadline {deadline} ms it was meant to extend; the write may have been applied after the fence lapsed, so the holder stops instead of continuing under a renewed record it cannot prove was still live",
            kind_name(lease.kind),
            lease.id
        )));
    }
    next.mod_revision = revision;
    Ok(next)
}

fn lost_writer_or_reader(lease: &GcLease) -> String {
    format!(
        "gc {} lease {} is no longer held by this operation; refusing to continue under a fence that was lost",
        kind_name(lease.kind),
        lease.id
    )
}

/// Extends a lease published by [`acquire`] to a full `ttl_ms` from now.
///
/// Returns the updated record (with its new `mod_revision`). Refuses when this
/// holder no longer owns the record, so a caller can distinguish "renewed" from
/// "my fence is gone" and abort instead of continuing under an expired lease.
pub fn renew<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    lease: &GcLease,
    ttl_ms: u64,
) -> Result<GcLease, MetaError> {
    extend(
        meta,
        &lease_key(lease),
        lease,
        ttl_ms,
        lost_writer_or_reader(lease),
        &now_ms,
    )
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

/// A writer lease for a project, in whichever metadata runtime is active.
///
/// On legacy metadata the lease lives in `/gc-lease/`. On Multi-Raft it is
/// published in the group that owns the project (`/gc/g<group>/writer/`), so
/// that group's collector sees it and refuses, and a collector of another group
/// is unaffected. SQL tablet writes and block writes have no intent record of
/// their own, so this lease is their only fence against a collector reclaiming
/// fragments they appended but have not published yet.
///
/// The name is kept for its call sites; it no longer means "legacy only".
pub fn acquire_writer_if_legacy<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    project_id: u32,
    revision: u64,
    ttl_ms: u64,
) -> Result<Option<GcLeaseGuard<'_, FS>>, MetaError> {
    acquire_writer(meta, project_id, revision, ttl_ms).map(Some)
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

    /// Extends the collector record, refusing if this pass no longer owns it, if
    /// the record's lifetime already ran out, or if the write is acknowledged only
    /// after the deadline it was extending.
    ///
    /// A pass is allowed to take arbitrarily long between its fence check and its
    /// last deletion only while it keeps proving it is still the pass that owns
    /// the record. Renewal is that proof: if the record expired and was taken
    /// over, this fails and the caller must stop deleting. Expiry is refused for
    /// the same reason — an expired record no longer keeps another pass out, so
    /// extending it would resume deleting under a fence that was already gone.
    /// The acknowledgement is checked as well because the CAS alone cannot tell
    /// whether the record was still live when the write was applied.
    pub fn renew(&mut self, ttl_ms: u64) -> Result<(), MetaError> {
        self.lease = extend(
            self.meta,
            &self.key,
            &self.lease,
            ttl_ms,
            "this collection pass lost the collector lease; no further fragment is deleted".into(),
            &now_ms,
        )?;
        Ok(())
    }

    /// Verifies this pass still owns the record without extending it.
    pub fn ensure_owned(&self) -> Result<(), MetaError> {
        ensure_live(self.meta, &self.lease)
    }

    pub fn release(mut self) -> Result<(), MetaError> {
        let result = self.guarded_release();
        if result.is_ok() {
            self.key.clear();
        }
        result
    }

    fn guarded_release(&self) -> Result<(), MetaError> {
        match self.meta.try_get(&self.key)? {
            None => Ok(()),
            Some(kv) if kv.mod_revision != self.lease.mod_revision => Err(MetaError::Routing(
                "the collector lease is no longer held by this pass; leaving the record in place"
                    .into(),
            )),
            Some(_) => match self.meta.execute(KvCommand::RevisionTxn {
                guards: vec![self.lease.guard()],
                ops: vec![TxnOp::Delete {
                    key: self.key.clone(),
                }],
            })? {
                KvResult::TxnOk { .. } => Ok(()),
                KvResult::CasConflict { .. } => Err(MetaError::Routing(
                    "the collector lease changed while it was being released; leaving the record in place"
                        .into(),
                )),
                other => Err(MetaError::Routing(format!(
                    "collector lease release returned an unexpected result: {other:?}"
                ))),
            },
        }
    }
}

impl<FS: StorageFs> Drop for CollectorGuard<'_, FS> {
    fn drop(&mut self) {
        if self.key.is_empty() {
            return;
        }
        // Ownership-guarded: a pass that lost the record must not delete the
        // record of the pass that took over.
        let _ = self.guarded_release();
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
                group: None,
                revision: 0,
                expires_at_ms: u64::MAX,
                mod_revision: 0,
            }
        }
    }
}

/// Removes expired lease records. Best effort: a record that cannot be removed
/// stays live (it is still counted) and the next pass tries again.
fn reap_expired<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    prefix: &str,
    now: u64,
) -> Result<usize, MetaError> {
    let mut reaped = 0usize;
    for (key, value) in meta.try_scan_prefix(prefix)? {
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
    Ok(reaped)
}

fn live_leases_in_scope<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    scope: LeaseScope,
) -> Result<Vec<GcLease>, MetaError> {
    let now = now_ms();
    reap_expired(meta, &scope.lease_prefix(), now)?;
    let mut live = Vec::new();
    let reader_prefix = scope.reader_prefix();
    let writer_prefix = scope.writer_prefix();
    for prefix in [&reader_prefix, &writer_prefix] {
        for (key, value) in meta.try_scan_prefix(prefix)? {
            let lease = read_lease(&key, &value.value);
            if lease.is_live_at(now) {
                live.push(lease);
            }
        }
    }
    // Any record under the lease prefix that is neither a reader, a writer nor
    // the collector is unparseable or foreign; refuse rather than ignore it.
    let collector_key = scope.collector_key();
    for (key, value) in meta.try_scan_prefix(&scope.lease_prefix())? {
        if key == collector_key
            || key.starts_with(&reader_prefix)
            || key.starts_with(&writer_prefix)
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

/// Every live reader or writer lease in the namespace, after reaping expired
/// records. The collector lease is reported separately by [`live_collector`].
///
/// A malformed record is reported as live: refusing to collect is always safer
/// than collecting on a namespace whose fence cannot be read.
pub fn live_leases<FS: StorageFs>(meta: &LocalMetaKv<FS>) -> Result<Vec<GcLease>, MetaError> {
    live_leases_in_scope(meta, LeaseScope::Legacy)
}

/// Every live reader or writer lease of one metadata group, after reaping
/// expired records in that group.
///
/// This is the per-group fence: a pass over group `group` refuses on a live
/// lease here, and ignores leases held in other groups, because a write to
/// another group cannot reference this group's fragments.
pub fn live_leases_in<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    group: u64,
) -> Result<Vec<GcLease>, MetaError> {
    live_leases_in_scope(meta, LeaseScope::Group(group))
}

/// The current collector lease, if one is live. An expired record is reaped and
/// reported as absent so a crashed pass cannot block collection forever.
///
/// Reaping matters for admission as well as for collection: reader and writer
/// leases are only published while this key is absent, so an expired record left
/// in place would refuse every read and write until somebody removed it.
pub fn live_collector<FS: StorageFs>(meta: &LocalMetaKv<FS>) -> Option<GcLease> {
    live_collector_in_scope(meta, LeaseScope::Legacy)
        .ok()
        .flatten()
}

/// The collector lease of one metadata group, if one is live.
///
/// Returns `Err` when the record cannot be read: an unreadable fence must
/// refuse the group, not be treated as "no pass is running".
pub fn live_collector_in<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    group: u64,
) -> Result<Option<GcLease>, MetaError> {
    live_collector_in_scope(meta, LeaseScope::Group(group))
}

fn live_collector_in_scope<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    scope: LeaseScope,
) -> Result<Option<GcLease>, MetaError> {
    let key = scope.collector_key();
    let Some(value) = read_key(meta, &key)? else {
        return Ok(None);
    };
    let lease = read_lease(&key, &value.value);
    if lease.is_live_at(now_ms()) {
        return Ok(Some(lease));
    }
    let _ = meta.execute(KvCommand::RevisionTxn {
        guards: vec![RevisionGuard {
            key: key.clone(),
            expected_mod_revision: value.mod_revision,
        }],
        ops: vec![TxnOp::Delete { key }],
    });
    Ok(None)
}

/// Reads one key without panicking on an unavailable or unroutable namespace.
fn read_key<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    key: &str,
) -> Result<Option<crate::KvValue>, MetaError> {
    Ok(meta.try_snapshot(&[key.to_owned()], &[])?.get(key))
}

/// Acquires the collector lease, refusing when another pass already holds it.
/// `expected_mod_revision` is the value observed by [`live_collector`] (`0` when
/// absent), so a concurrent acquisition loses the race instead of interleaving.
pub fn acquire_collector<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    expected_mod_revision: u64,
    ttl_ms: u64,
) -> Result<CollectorGuard<'_, FS>, MetaError> {
    acquire_collector_in_scope(meta, LeaseScope::Legacy, expected_mod_revision, ttl_ms)
}

/// Acquires one group's collector lease, refusing when another pass already
/// holds that group's record.
///
/// The record is written into the group itself, so two passes over the same
/// group cannot interleave their mark and sweep, while a pass over another
/// group neither blocks this one nor is blocked by it.
pub fn acquire_collector_in<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    group: u64,
    expected_mod_revision: u64,
    ttl_ms: u64,
) -> Result<CollectorGuard<'_, FS>, MetaError> {
    acquire_collector_in_scope(
        meta,
        LeaseScope::Group(group),
        expected_mod_revision,
        ttl_ms,
    )
}

fn acquire_collector_in_scope<FS: StorageFs>(
    meta: &LocalMetaKv<FS>,
    scope: LeaseScope,
    expected_mod_revision: u64,
    ttl_ms: u64,
) -> Result<CollectorGuard<'_, FS>, MetaError> {
    let key = scope.collector_key();
    let id = new_lease_id();
    let mut lease = GcLease {
        id,
        kind: LeaseKind::Collector,
        project_id: 0,
        group: match scope {
            LeaseScope::Legacy => None,
            LeaseScope::Group(group) => Some(group),
        },
        revision: meta.legacy_revision(),
        expires_at_ms: now_ms().saturating_add(ttl_ms.max(1)),
        mod_revision: 0,
    };
    let value = serde_json::to_vec(&lease)?;
    match meta.execute(KvCommand::RevisionTxn {
        guards: vec![RevisionGuard {
            key: key.clone(),
            expected_mod_revision,
        }],
        ops: vec![TxnOp::Put {
            key: key.clone(),
            value,
        }],
    })? {
        KvResult::TxnOk { revision, .. } => {
            lease.mod_revision = revision;
            Ok(CollectorGuard { meta, key, lease })
        }
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
    fn admission_is_closed_while_a_pass_holds_the_collector_record() {
        let meta = meta();
        let collector = acquire_collector(&meta, 0, DEFAULT_TTL_MS).unwrap();

        // A reader or writer published now would be invisible to the mark the
        // pass is about to take, so both are refused instead.
        let Err(refused) = acquire_reader(&meta, 1, 1, DEFAULT_TTL_MS) else {
            panic!("admission must be closed while a pass holds the collector record");
        };
        assert!(
            refused
                .to_string()
                .contains("garbage collection is running"),
            "{refused}"
        );
        let Err(refused) = acquire_writer(&meta, 1, 1, DEFAULT_TTL_MS) else {
            panic!("admission must be closed while a pass holds the collector record");
        };
        assert!(
            refused
                .to_string()
                .contains("garbage collection is running"),
            "{refused}"
        );
        assert!(
            meta.scan_prefix_or_panic(READER_PREFIX).is_empty()
                && meta.scan_prefix_or_panic(WRITER_PREFIX).is_empty(),
            "a refused admission must not leave a record behind"
        );

        // Once the pass is gone, admission reopens.
        collector.release().unwrap();
        let reader = acquire_reader(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        reader.release().unwrap();
        let writer = acquire_writer(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        writer.release().unwrap();
    }

    #[test]
    fn a_lost_lease_cannot_release_or_guard_its_successors_record() {
        let meta = meta();
        let mut lease = acquire_writer(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        let key = lease.key().to_string();
        let stale = lease.lease().clone();

        // Simulate the record expiring, being reaped, and the key being taken
        // over by a later holder (same key, later revision).
        let mut successor = stale.clone();
        successor.id = "successor".into();
        meta.execute(KvCommand::Put {
            key: key.clone(),
            value: serde_json::to_vec(&successor).unwrap(),
        })
        .unwrap();

        // The stale holder can neither prove its fence nor extend it.
        assert!(lease.ensure_live().is_err());
        assert!(lease.renew(DEFAULT_TTL_MS).is_err());
        // A publication guarded by the stale lease loses its CAS.
        assert!(matches!(
            meta.execute(KvCommand::RevisionTxn {
                guards: vec![stale.guard()],
                ops: vec![TxnOp::Put {
                    key: "/db/1/proof".into(),
                    value: b"published".to_vec(),
                }],
            })
            .unwrap(),
            KvResult::CasConflict { .. }
        ));
        assert!(meta.get_or_panic("/db/1/proof").is_none());

        // Releasing through the stale value is a no-op, not a deletion.
        assert!(release(&meta, &stale).is_ok());
        assert!(meta.get_or_panic(&key).is_some());
        // Dropping the guard must not delete the successor's record either.
        drop(lease);
        assert!(
            meta.get_or_panic(&key).is_some(),
            "a lost lease must never delete the record that replaced it"
        );
    }

    #[test]
    fn a_renewed_lease_keeps_its_ownership_and_extends_its_ttl() {
        let meta = meta();
        // A live lease: renewing an expired one is refused now, and that case has
        // its own test. The TTL is doubled on renewal so the deadline must move
        // forward even if both calls land in the same millisecond.
        let guard = acquire_writer(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        let lease = guard.lease().clone();
        let first = lease.mod_revision;
        assert!(first > 0, "the publication revision is the ownership token");
        let renewed = renew(&meta, &lease, DEFAULT_TTL_MS * 2).unwrap();
        assert!(
            renewed.mod_revision > first,
            "renewal republishes the record"
        );
        assert!(renewed.expires_at_ms > lease.expires_at_ms);
        assert!(ensure_live(&meta, &renewed).is_ok());
        // The superseded token no longer owns the record.
        assert!(ensure_live(&meta, &lease).is_err());
        assert!(renew(&meta, &lease, DEFAULT_TTL_MS).is_err());
        // Renewing a lease that was never published is refused rather than
        // silently treated as ownership.
        let mut unpublished = renewed.clone();
        unpublished.mod_revision = 0;
        assert!(renew(&meta, &unpublished, DEFAULT_TTL_MS).is_err());
    }

    #[test]
    fn an_expired_but_unreaped_lease_is_refused_by_ensure_live() {
        let meta = meta();
        let guard = acquire_writer(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        let key = guard.key().to_string();
        let mut lease = guard.lease().clone();

        // Rewrite the record in place with a past expiry through the same kv
        // path a renewal uses. The transaction returns the record's new
        // `mod_revision`, so the holder still owns it: only the clock moved.
        lease.expires_at_ms = now_ms() - 1;
        let revision = match meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: key.clone(),
                    expected_mod_revision: lease.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&lease).unwrap(),
                }],
            })
            .unwrap()
        {
            KvResult::TxnOk { revision, .. } => revision,
            other => panic!("the expired rewrite must commit, got {other:?}"),
        };
        lease.mod_revision = revision;

        // The record is still present and still this holder's, but the lease it
        // encodes no longer fences anything: a pass may delete the fragments
        // this holder is reading before it reaps the record.
        let stored = meta
            .get_or_panic(&key)
            .expect("the expired record is not reaped yet");
        assert_eq!(stored.mod_revision, lease.mod_revision);
        assert!(!lease.is_live_at(now_ms()));

        let error = ensure_live(&meta, &lease).unwrap_err().to_string();
        assert!(error.contains("expired at"), "{error}");
        assert!(
            !error.contains("reaped or replaced"),
            "an unreaped record must not be reported as lost: {error}"
        );
    }

    #[test]
    fn a_live_record_with_a_stale_revision_is_still_refused() {
        let meta = meta();
        let guard = acquire_reader(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        let stale = guard.lease().clone();
        let renewed = renew(&meta, &stale, DEFAULT_TTL_MS).unwrap();
        assert!(renewed.is_live_at(now_ms()));

        // The record is live, but the old token no longer owns it.
        let error = ensure_live(&meta, &stale).unwrap_err().to_string();
        assert!(error.contains("reaped or replaced"), "{error}");
        assert!(
            !error.contains("expired at"),
            "a live record must not be reported as expired: {error}"
        );
        assert!(ensure_live(&meta, &renewed).is_ok());
    }

    #[test]
    fn a_live_owned_lease_is_accepted() {
        let meta = meta();
        let guard = acquire_reader(&meta, 3, 1, DEFAULT_TTL_MS).unwrap();
        assert!(guard.ensure_live().is_ok());
        let lease = guard.lease().clone();
        assert!(lease.is_live_at(now_ms()));
        assert!(ensure_live(&meta, &lease).is_ok());
    }

    /// Rewrites a lease record in place with a past expiry, keeping ownership:
    /// the transaction returns the record's new `mod_revision` and the holder
    /// adopts it, so only the clock moved.
    fn expire_in_place<FS: StorageFs>(meta: &LocalMetaKv<FS>, lease: &mut GcLease) {
        lease.expires_at_ms = now_ms() - 1;
        let key = lease_key(lease);
        let revision = match meta
            .execute(KvCommand::RevisionTxn {
                guards: vec![RevisionGuard {
                    key: key.clone(),
                    expected_mod_revision: lease.mod_revision,
                }],
                ops: vec![TxnOp::Put {
                    key,
                    value: serde_json::to_vec(&*lease).unwrap(),
                }],
            })
            .unwrap()
        {
            KvResult::TxnOk { revision, .. } => revision,
            other => panic!("the expired rewrite must commit, got {other:?}"),
        };
        lease.mod_revision = revision;
    }

    #[test]
    fn renew_refuses_an_expired_but_unreaped_record_and_leaves_it_untouched() {
        let meta = meta();
        let guard = acquire_reader(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        let mut lease = guard.lease().clone();
        expire_in_place(&meta, &mut lease);
        let before = meta.get_or_panic(&lease_key(&lease)).unwrap();

        let error = renew(&meta, &lease, DEFAULT_TTL_MS)
            .unwrap_err()
            .to_string();
        assert!(error.contains("expired at"), "{error}");
        assert!(
            !error.contains("no longer held"),
            "an expired record must not be reported as lost: {error}"
        );
        // A refusal must not rewrite the record: the deadline it carries is the
        // evidence that this lease had already stopped fencing.
        let after = meta.get_or_panic(&lease_key(&lease)).unwrap();
        assert_eq!(after.mod_revision, before.mod_revision);
        assert_eq!(after.value, before.value);
        assert_eq!(after.mod_revision, lease.mod_revision);
    }

    #[test]
    fn renew_extends_a_live_owned_record_and_still_refuses_a_replaced_one() {
        let meta = meta();
        let guard = acquire_reader(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        let stale = guard.lease().clone();
        let renewed = renew(&meta, &stale, DEFAULT_TTL_MS).unwrap();
        assert!(renewed.is_live_at(now_ms()));
        assert!(renewed.expires_at_ms >= stale.expires_at_ms);
        assert_ne!(renewed.mod_revision, stale.mod_revision);

        // The superseded token is refused as replaced, not as expired, so the
        // two failures stay distinguishable for the operator.
        let error = renew(&meta, &stale, DEFAULT_TTL_MS)
            .unwrap_err()
            .to_string();
        assert!(error.contains("no longer held"), "{error}");
        assert!(!error.contains("expired at"), "{error}");
        assert!(renew(&meta, &renewed, DEFAULT_TTL_MS).is_ok());
    }

    #[test]
    fn a_renewal_acknowledged_after_its_deadline_is_refused_even_though_the_write_landed() {
        // The gap this closes: `renew` checked liveness and then did its CAS, so
        // a record that expired *during* the round trip was still extended. The
        // holder then deleted under a record it had just resurrected, which is
        // the exact failure the expiry refusal exists to prevent - only narrowed
        // from a whole TTL to one round trip.
        let meta = meta();
        let guard = acquire_writer(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        let lease = guard.lease().clone();
        let key = lease_key(&lease);
        // Live when checked, acknowledged one millisecond after the deadline.
        let checked = lease.expires_at_ms - 1;
        let calls = std::cell::Cell::new(0u32);
        let clock = || {
            calls.set(calls.get() + 1);
            if calls.get() == 1 {
                checked
            } else {
                lease.expires_at_ms + 1
            }
        };
        let error = extend(&meta, &key, &lease, DEFAULT_TTL_MS, "lost".into(), &clock)
            .unwrap_err()
            .to_string();
        assert!(error.contains("acknowledged at"), "{error}");
        assert!(error.contains("deadline"), "{error}");

        // The write did land. Asserting that keeps the refusal from being
        // satisfied by a second pre-check, which would leave the original window
        // open; the record really does carry the extended deadline that the
        // holder is refusing to trust.
        let stored = meta.get_or_panic(&key).expect("the renewal was written");
        assert_ne!(stored.mod_revision, lease.mod_revision);
        let stored: GcLease = serde_json::from_slice(&stored.value).unwrap();
        assert!(stored.is_live_at(lease.expires_at_ms));
    }

    #[test]
    fn a_renewal_acknowledged_before_its_deadline_is_still_accepted() {
        // The positive control for the same seam: the post-write check must not
        // refuse a renewal that provably landed while the fence was live.
        let meta = meta();
        let guard = acquire_reader(&meta, 1, 1, DEFAULT_TTL_MS).unwrap();
        let lease = guard.lease().clone();
        let checked = lease.expires_at_ms - 1000;
        let calls = std::cell::Cell::new(0u32);
        let clock = || {
            calls.set(calls.get() + 1);
            if calls.get() == 1 {
                checked
            } else {
                checked + 10
            }
        };
        let renewed = extend(
            &meta,
            &lease_key(&lease),
            &lease,
            DEFAULT_TTL_MS,
            "lost".into(),
            &clock,
        )
        .unwrap();
        assert!(renewed.is_live_at(checked));
        assert!(renewed.expires_at_ms >= lease.expires_at_ms);
        assert_ne!(renewed.mod_revision, lease.mod_revision);
    }

    #[test]
    fn a_guard_cannot_renew_its_own_expired_record() {
        // The guard shares the free function's refusal. The collector mid-pass is
        // the caller that depends on it: continuing to delete under an expired
        // record is the failure this prevents.
        let meta = meta();
        let mut guard = acquire_collector(&meta, 0, 1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(!guard.lease().is_live_at(now_ms()));
        let error = guard.renew(DEFAULT_TTL_MS).unwrap_err().to_string();
        assert!(error.contains("expired at"), "{error}");
    }

    #[test]
    fn a_superseded_collector_cannot_renew_delete_or_release_the_record() {
        let meta = meta();
        let collector = acquire_collector(&meta, 0, DEFAULT_TTL_MS).unwrap();
        let stale = collector.lease().clone();

        // Another pass took the record over (the previous one expired and was
        // reaped). Every operation of the superseded pass must now refuse: it may
        // not extend its fence, may not delete, and may not remove the record.
        let mut successor = stale.clone();
        successor.id = "successor".into();
        meta.execute(KvCommand::Put {
            key: COLLECTOR_KEY.into(),
            value: serde_json::to_vec(&successor).unwrap(),
        })
        .unwrap();

        let mut collector = collector;
        assert!(collector.ensure_owned().is_err());
        let renewed = collector.renew(DEFAULT_TTL_MS);
        assert!(
            renewed
                .unwrap_err()
                .to_string()
                .contains("lost the collector lease"),
            "renewal must report the lost fence"
        );
        assert!(collector.release().is_err());
        assert!(
            meta.get_or_panic(COLLECTOR_KEY).is_some(),
            "the successor's record must survive the superseded pass"
        );
        // `release` consumed the guard only on success, so Drop runs too.
        assert!(live_collector(&meta).is_some());
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
        assert!(
            meta.get_or_panic(&key).is_none(),
            "expired record must be reaped"
        );

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
        // The expired record is reaped, not merely ignored: leaving it in place
        // would refuse every reader and writer, which are only admitted while
        // this key is absent. A fresh pass therefore takes it from the absent
        // state.
        assert!(meta.get_or_panic(COLLECTOR_KEY).is_none());
        let third = acquire_collector(&meta, 0, DEFAULT_TTL_MS).unwrap();
        third.release().unwrap();
    }

    #[test]
    fn group_leases_are_scoped_to_their_group() {
        let meta = meta();
        // A group-scoped writer record lands in that group's namespace, not in
        // the legacy one and not in another group's.
        let (key, writer) = put_lease_in(
            &meta,
            LeaseScope::Group(2),
            LeaseKind::Writer,
            1,
            0,
            DEFAULT_TTL_MS,
        )
        .unwrap();
        assert!(key.starts_with("/gc/g2/writer/"), "{key}");
        assert_eq!(writer.group, Some(2));
        assert_eq!(lease_key(&writer), key);

        assert_eq!(live_leases_in(&meta, 2).unwrap().len(), 1);
        assert!(
            live_leases_in(&meta, 3).unwrap().is_empty(),
            "a lease of group 2 must not fence group 3"
        );
        assert!(
            live_leases(&meta).unwrap().is_empty(),
            "a group-scoped lease must not appear in the legacy namespace"
        );

        release(&meta, &writer).unwrap();
        assert!(live_leases_in(&meta, 2).unwrap().is_empty());
        assert!(meta.get_or_panic(&key).is_none());
    }

    #[test]
    fn a_legacy_lease_record_is_byte_identical_to_the_pre_group_format() {
        // Per-group GC added a `group` field. A legacy record must still encode
        // exactly as it did before that field existed: same keys, same order,
        // no `"group":null`. Otherwise the same logical event produces a
        // different stored revision in a mixed-version deployment.
        let lease = GcLease {
            id: "abc".into(),
            kind: LeaseKind::Writer,
            project_id: 7,
            group: None,
            revision: 5,
            expires_at_ms: 1_700_000_000_000,
            mod_revision: 0,
        };
        let encoded = serde_json::to_string(&lease).unwrap();
        assert_eq!(
            encoded,
            r#"{"id":"abc","kind":"writer","project_id":7,"revision":5,"expires_at_ms":1700000000000,"mod_revision":0}"#
        );
        // The pre-group format still decodes, so an older record stays readable.
        let decoded: GcLease = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, lease);
        assert_eq!(decoded.group, None);

        // A group record does carry the field, and it is the only difference.
        let grouped = GcLease {
            group: Some(2),
            ..lease
        };
        let grouped = serde_json::to_string(&grouped).unwrap();
        assert_eq!(
            grouped,
            r#"{"id":"abc","kind":"writer","project_id":7,"group":2,"revision":5,"expires_at_ms":1700000000000,"mod_revision":0}"#
        );
    }

    #[test]
    fn group_collectors_are_independent_and_close_admission_per_group() {
        let meta = meta();
        let group2 = acquire_collector_in(&meta, 2, 0, DEFAULT_TTL_MS).unwrap();
        let group3 = acquire_collector_in(&meta, 3, 0, DEFAULT_TTL_MS).unwrap();
        assert!(live_collector_in(&meta, 2).unwrap().is_some());
        assert!(live_collector_in(&meta, 3).unwrap().is_some());
        assert!(live_collector(&meta).is_none());

        // Admission in group 2 is closed while its own collector record is held.
        let refused = put_lease_in(
            &meta,
            LeaseScope::Group(2),
            LeaseKind::Reader,
            1,
            0,
            DEFAULT_TTL_MS,
        )
        .unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("garbage collection is running"),
            "{refused}"
        );

        // Releasing group 2's collector neither removes group 3's record nor
        // reopens admission for group 3.
        group2.release().unwrap();
        assert!(live_collector_in(&meta, 2).unwrap().is_none());
        assert!(live_collector_in(&meta, 3).unwrap().is_some());
        let refused = put_lease_in(
            &meta,
            LeaseScope::Group(3),
            LeaseKind::Writer,
            2,
            0,
            DEFAULT_TTL_MS,
        )
        .unwrap_err();
        assert!(refused
            .to_string()
            .contains("garbage collection is running"));

        // Once group 3's pass is gone its admission reopens, independently.
        group3.release().unwrap();
        let (key, writer) = put_lease_in(
            &meta,
            LeaseScope::Group(3),
            LeaseKind::Writer,
            2,
            0,
            DEFAULT_TTL_MS,
        )
        .unwrap();
        assert!(key.starts_with("/gc/g3/writer/"), "{key}");
        release(&meta, &writer).unwrap();
    }

    #[test]
    fn watermark_only_rises_and_fences_lagging_readers() {
        let meta = meta();
        // No pass has run: every applied revision is safe.
        assert_eq!(watermark(&meta).unwrap(), 0);
        assert!(reader_is_safe(&meta).is_ok());

        let applied = meta.revision_or_panic();
        assert_eq!(publish_watermark(&meta, applied).unwrap(), applied);
        assert!(
            reader_is_safe(&meta).is_ok(),
            "a reader at the mark is safe"
        );

        // A later pass cannot lower it, even when asked to.
        assert_eq!(publish_watermark(&meta, 0).unwrap(), applied);
        assert_eq!(watermark(&meta).unwrap(), applied);

        // A reader whose applied view is behind the mark must refuse: the
        // manifests it can see may name fragments a pass already reclaimed.
        // Simulate a leader that is ahead of this node by publishing a watermark
        // past the local applied revision.
        assert_eq!(
            publish_watermark(&meta, applied + 100).unwrap(),
            applied + 100,
            "the collector may raise the watermark past a lagging node's view"
        );
        let error = reader_is_safe(&meta).unwrap_err().to_string();
        assert!(
            error.contains("garbage collection has swept up to"),
            "{error}"
        );
        assert!(error.contains("refusing to read"), "{error}");
    }
}
