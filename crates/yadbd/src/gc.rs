//! Mark-and-sweep garbage collection for the legacy metadata runtime.
//!
//! The collector is deliberately narrow. It runs on one metadata snapshot, it
//! refuses rather than guesses, and every deletion is ordered after the durable
//! fence that makes it safe:
//!
//! 1. **Admission fence.** Reader and writer leases can only be published while
//!    the collector record is *absent* (their publication transaction guards on
//!    it), and the collector record can only be published while no other pass
//!    holds it. Publishing the collector record therefore closes admission
//!    atomically: a lease published before it is visible to the check in step 2,
//!    and one attempted after it loses its CAS. That is what makes the fence hold
//!    for the whole pass, not just at one instant.
//! 2. **Fence.** On legacy metadata, read every live reader/writer lease and reap
//!    expired ones; any live lease means a read or write is in flight, so the
//!    pass refuses and deletes nothing.
//! 3. **Collector lease.** Publish a durable collector record so a second pass
//!    cannot interleave its mark and sweep phases with this one, and so no new
//!    reader or writer can be admitted until the pass is done. The record is
//!    renewed before the first deletion, before each barrier group and every
//!    [`FENCE_CHECK_INTERVAL`] exact tombstones: a pass that stalls past its
//!    lease stops deleting instead of continuing without a fence, and its
//!    release is ownership-guarded so it can never delete a successor's record.
//! 4. **Enumerate.** Ask every slot of every mounted pool which physical fragment
//!    ids it holds. A slot that cannot answer makes that pool unprovable: refuse
//!    that pool, sweep no part of it.
//! 5. **Re-check the fence, then mark.** A writer that published during step 4
//!    would be invisible to a mark taken earlier, so the fence is checked again
//!    and one snapshot of `/db/`, `/s3/`, `/s3-intent/` and `/block/` is taken
//!    *after* it. A manifest that cannot be parsed, or a block extent map whose
//!    pool cannot be resolved, makes the live set unprovable: refuse. Nothing is
//!    deleted that this snapshot references.
//! 6. **Delete.** Durable generation barriers on channels whose `(owner,
//!    generation)` space is monotonic and where the dead generation range is
//!    provably disjoint from every live generation in that `(project, owner,
//!    channel)`; exact per-fragment tombstones everywhere else.
//! 7. **Compact.** Seal a wasteful active segment and compact sealed ones, so the
//!    bytes the barrier/tombstone freed are actually returned to the filesystem.
//!
//! **Multi-Raft.** Collection is disabled: a writer can renew an expired lease
//! during a pass, and a collector RPC can continue after the collector lease
//! expires. Per-group snapshots and lease checks do not close those races.
//!
//! Crash safety comes from the order, not from atomicity: the fence and the
//! barrier/tombstone records are durable before any byte is freed, and every step
//! is idempotent, so a process that dies mid-pass leaves a volume that a rerun
//! finishes. A rerun after a crash cannot delete a referenced fragment, because
//! the mark phase is recomputed from committed metadata.

use std::collections::{BTreeMap, BTreeSet};

use yadb_blob::{BlobId, PoolStriper};
use yadb_control::ControlPlaneState;
use yadb_db::TabletManifest;
use yadb_io::OsFs;
use yadb_metakv::{gc, LocalMetaKv, MetaError};
use yadb_s3::ObjectManifest;

/// Metadata prefixes that can name a fragment. Any prefix that can reference a
/// blob must be listed here: an unlisted reference would be swept as garbage.
///
/// `/s3-retired/` is deliberately *not* live. Those records are the historical
/// manifest of a superseded or deleted object; no read may serve them (a reader
/// holds a lease, and a read that starts during a pass is refused), so the chunks
/// they name are exactly the ones an overwrite or delete made unreachable. The
/// records themselves are kept: they are small, and pruning them would need its
/// own retention rule.
const LIVE_PREFIXES: [&str; 4] = ["/db/", "/s3/", "/s3-intent/", "/block/"];

/// Lease lifetime for a collection pass. The pass re-checks the fence mid-way, so
/// this only has to cover one pass, not an arbitrary stall.
const COLLECTOR_LEASE_MS: u64 = 120_000;

/// How many exact tombstones may be issued between collector-lease checks.
///
/// A check is a metadata CAS, i.e. one Raft round trip on a real cluster, so
/// checking before every fragment would add a transaction per delete. Checking
/// only once per pool is the window this constant exists to close: a pass that
/// stalled after the per-pool renew could tombstone an unbounded number of
/// fragments with no fence. Thirty-two bounds the overshoot to a few dozen
/// tombstones (at most 96 physical parts for a three-replica pool) while
/// amortizing the check over enough work that it does not dominate a sweep.
const FENCE_CHECK_INTERVAL: usize = 32;

/// Channels whose `(project, owner, generation)` space comes from a monotonic
/// per-project allocation counter, so a pair is never handed out twice.
///
/// A generation barrier is permanent and fences *future* writes at or below the
/// barrier, so it is only safe on such a channel. The SQL tablet channels are
/// excluded: a tablet's owner id is derived from its name and its generation
/// restarts at zero, so recreating a table after a barrier would make every
/// write to it fail with `FencedByBarrier`. Those channels use exact tombstones.
const BARRIER_SAFE_CHANNELS: [u8; 2] = [yadb_s3::S3_CHANNEL, yadb_block::BLOCK_CHANNEL];

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SweepOutcome {
    /// Distinct live logical blobs across every mounted pool at the final mark.
    pub blobs_live: usize,
    /// Physical fragments removed (barrier evictions plus exact tombstones).
    pub fragments_deleted: usize,
    /// Bytes returned to the filesystem by compaction.
    pub bytes_reclaimed: u64,
    /// Live leases observed. Non-zero always means the pass refused.
    pub leases_live: usize,
    /// Durable barriers written.
    pub barriers_applied: usize,
    /// Set when at least one group or pool deleted nothing. Always an explicit
    /// reason.
    #[serde(rename = "gc_refusal")]
    pub refusal: Option<String>,
    /// Pools that were mounted but not swept, with the reason.
    #[serde(rename = "gc_pools_refused")]
    pub pools_refused: Vec<String>,
    /// Multi-Raft metadata groups that were refused, with the reason. Empty on
    /// legacy metadata, which has one namespace rather than per-group passes.
    #[serde(rename = "gc_groups_refused", default)]
    pub groups_refused: Vec<String>,
    /// Per-operation failures that happened after the refusal point. A barrier or
    /// compaction failure is reported here, never swallowed.
    #[serde(rename = "gc_errors")]
    pub errors: Vec<String>,
}

impl SweepOutcome {
    pub fn refuse(reason: impl Into<String>) -> Self {
        Self {
            refusal: Some(reason.into()),
            ..Self::default()
        }
    }
}

/// A mark set plus the pool each reference names.
type MarkSet = BTreeSet<(String, BlobId)>;

pub const MULTIRAFT_GC_REFUSAL: &str = "garbage collection refused: mark-and-sweep disabled for Multi-Raft; writer leases can renew after expiry and collector RPCs can outlive their lease";

pub fn sweep(
    meta: &LocalMetaKv<OsFs>,
    pools: &BTreeMap<String, std::sync::Arc<PoolStriper<OsFs>>>,
    control: &ControlPlaneState,
) -> SweepOutcome {
    if meta.is_openraft() {
        return SweepOutcome::refuse(MULTIRAFT_GC_REFUSAL);
    }
    sweep_legacy(meta, pools, control)
}

/// The single-namespace pass used by legacy metadata. Its behaviour, keys and
/// ordering are unchanged; Multi-Raft collection is temporarily disabled.
fn sweep_legacy(
    meta: &LocalMetaKv<OsFs>,
    pools: &BTreeMap<String, std::sync::Arc<PoolStriper<OsFs>>>,
    control: &ControlPlaneState,
) -> SweepOutcome {
    // Step 1: fence. Only the metadata leader may collect. A follower's applied
    // state can lag the committed state, and a fragment that looks unreferenced
    // in that lagging view can be referenced by a manifest the leader has already
    // committed: deleting it would corrupt a committed revision. The leader's own
    // view is current, so a fragment unreferenced there is unreferenced for good.
    let (node_id, role, _, leader, _) = meta.role_info();
    if role != yadb_metakv::RoleState::Leader {
        return SweepOutcome::refuse(format!(
            "garbage collection refused: node {node_id} is not the metadata leader (leader is {leader:?}); a lagging view could delete a fragment the committed state still references"
        ));
    }
    let live = match gc::live_leases(meta) {
        Ok(live) => live,
        Err(error) => {
            return SweepOutcome::refuse(format!(
                "garbage collection refused: lease fence unreadable: {error}"
            ))
        }
    };
    if !live.is_empty() {
        let mut outcome = SweepOutcome::refuse(format!(
            "garbage collection refused: {} live lease(s) ({} reader, {} writer); no fragment was deleted",
            live.len(),
            live.iter()
                .filter(|lease| lease.kind == gc::LeaseKind::Reader)
                .count(),
            live.iter()
                .filter(|lease| lease.kind == gc::LeaseKind::Writer)
                .count()
        ));
        outcome.leases_live = live.len();
        return outcome;
    }
    let collector_revision = match gc::live_collector(meta) {
        // A live collector lease means another pass is between its fence check
        // and its deletions. Its CAS below would still succeed (the record only
        // changes when it is released), so this must be refused explicitly.
        Ok(Some(lease)) => {
            return SweepOutcome::refuse(format!(
                "garbage collection refused: another pass holds the collector lease until {}",
                lease.expires_at_ms
            ))
        }
        // "Cannot be read" is not "no pass is running": the pass must refuse
        // rather than acquire a record whose current holder it could not see.
        Err(error) => {
            return SweepOutcome::refuse(format!(
                "garbage collection refused: collector lease record cannot be read: {error}"
            ))
        }
        Ok(None) => match meta.try_get(gc::COLLECTOR_KEY) {
            Ok(current) => current.map_or(0, |kv| kv.mod_revision),
            Err(error) => {
                return SweepOutcome::refuse(format!(
                    "garbage collection refused: collector lease record cannot be read: {error}"
                ))
            }
        },
    };
    let mut collector = match gc::acquire_collector(meta, collector_revision, COLLECTOR_LEASE_MS) {
        Ok(guard) => guard,
        Err(error) => {
            return SweepOutcome::refuse(format!(
                "garbage collection refused: collector lease unavailable: {error}"
            ))
        }
    };

    let mut outcome = SweepOutcome::default();

    // Step 3: enumerate what every slot actually holds. A pool that cannot
    // answer is unprovable: it is recorded as refused and never touched.
    let present = enumerate_present(pools, &mut outcome);

    // Step 4: raise the durable watermark before re-checking the fence. A reader
    // that is still behind this revision can no longer prove its manifests are
    // complete, so it refuses; a reader that publishes after this point has
    // already applied at least this revision.
    //
    // Renew first: enumeration can be slow, and everything below deletes bytes.
    // A pass that has lost its record must stop before the watermark, because
    // from that moment readers can be admitted again.
    if let Err(error) = collector.renew(COLLECTOR_LEASE_MS) {
        return SweepOutcome::refuse(format!(
            "garbage collection refused: {error}; no fragment was deleted"
        ));
    }
    let swept_up_to = match meta.try_revision() {
        Ok(revision) => revision,
        Err(error) => {
            return SweepOutcome::refuse(format!(
                "garbage collection refused: applied revision cannot be read: {error}; no fragment was deleted"
            ))
        }
    };
    if let Err(error) = gc::publish_watermark(meta, swept_up_to) {
        let _ = collector.release();
        return SweepOutcome::refuse(format!(
            "garbage collection refused: sweep watermark unavailable, so readers cannot be fenced: {error}"
        ));
    }

    // The fence must still be clear, and the mark must be taken after that check.
    // A writer that published its lease after step 1 is then either still holding
    // it (refuse) or already referenced by this snapshot (kept). Admission is
    // already closed by the collector record, so this is the check for leases
    // that were live before it was acquired.
    if let Err(error) = collector.ensure_owned() {
        return SweepOutcome::refuse(format!(
            "garbage collection refused: {error}; no fragment was deleted"
        ));
    }
    let live = match gc::live_leases(meta) {
        Ok(live) => live,
        Err(error) => {
            let _ = collector.release();
            return SweepOutcome::refuse(format!(
                "garbage collection refused: lease fence unreadable during sweep: {error}"
            ));
        }
    };
    if !live.is_empty() {
        let _ = collector.release();
        let mut refused = SweepOutcome::refuse(format!(
            "garbage collection refused: {} lease(s) appeared during the pass; no fragment was deleted",
            live.len()
        ));
        refused.leases_live = live.len();
        return refused;
    }
    let second = match mark(meta, control) {
        Ok(mark) => mark,
        Err(reason) => {
            let _ = collector.release();
            return SweepOutcome::refuse(format!(
                "garbage collection refused: live set is not provable at sweep time: {reason}"
            ));
        }
    };
    outcome.blobs_live = second.len();

    // Step 5: delete, pool by pool, only ids that the mark does not reference and
    // that the pool actually holds.
    //
    // Renew once more immediately before the first deletion: the mark snapshot
    // above is only meaningful while admission is still closed, so a pass that
    // cannot prove it still owns the collector record deletes nothing.
    if let Err(error) = collector.renew(COLLECTOR_LEASE_MS) {
        return SweepOutcome::refuse(format!(
            "garbage collection refused: {error}; no fragment was deleted"
        ));
    }
    let live_by_pool = group_by_pool(&second);
    for (pool_name, pool_present) in &present {
        let Some(striper) = pools.get(pool_name) else {
            outcome.pools_refused.push(format!(
                "pool `{pool_name}` refused: no mounted striper to sweep"
            ));
            continue;
        };
        // Every pool starts from a fresh lease, so a slow sweep of an earlier
        // pool cannot leave this one deleting under an expired fence.
        if let Err(error) = collector.renew(COLLECTOR_LEASE_MS) {
            outcome
                .errors
                .push(format!("sweep stopped before pool `{pool_name}`: {error}"));
            break;
        }
        let empty = BTreeSet::new();
        let live = live_by_pool.get(pool_name).unwrap_or(&empty);
        let dead: BTreeSet<BlobId> = pool_present
            .iter()
            .filter(|id| !live.contains(*id))
            .copied()
            .collect();
        if dead.is_empty() {
            continue;
        }
        let mut fence = || {
            // The deadline travels with each barrier: a successful renewal proves
            // the lease is live now, and the value it returns is what a target
            // compares against its own clock when the barrier arrives.
            collector
                .renew(COLLECTOR_LEASE_MS)
                .map(|_| collector.lease().expires_at_ms)
                .map_err(|error| error.to_string())
        };
        if sweep_pool(striper, pool_name, &dead, live, &mut fence, &mut outcome).is_err() {
            // The collector record no longer proves the fence, so admission can be
            // open again. `outcome.errors` already names the stop; nothing else in
            // this pass may delete.
            break;
        }
    }

    // Step 7: compact, so the bytes the barriers and tombstones freed are actually
    // returned to the filesystem. Refused pools are left alone: their state is
    // unknown, and a pass that refuses a pool must not touch it at all.
    //
    // Compaction only reclaims space the barriers already fenced, so it is safe
    // even if the lease is lost here; it is renewed anyway so a long compaction
    // does not hand the next pass a record that expires mid-way.
    let _ = collector.renew(COLLECTOR_LEASE_MS);
    compact_pools(pools, &present, &mut outcome);

    if let Err(error) = collector.release() {
        // The pass already deleted what it proved dead; the stale collector lease
        // only blocks the next pass until it expires. Report it.
        outcome
            .errors
            .push(format!("collector lease release failed: {error}"));
    }
    outcome
}

/// The Multi-Raft pass: one leader-only, quorum-confirmed sweep per data group.
///
/// Enumeration is physical and therefore shared: every slot is asked once which
/// fragments it holds, and a pool that cannot answer is refused for every group.
/// The mark and the deletion are per group, and each group's fragments are the
/// ones whose project id that group owns.
#[allow(dead_code)] // Retained temporarily, unreachable while Multi-Raft GC is disabled.
fn sweep_groups(
    meta: &LocalMetaKv<OsFs>,
    pools: &BTreeMap<String, std::sync::Arc<PoolStriper<OsFs>>>,
    control: &ControlPlaneState,
) -> SweepOutcome {
    let scopes = match meta.gc_group_scopes() {
        Ok(scopes) => scopes,
        Err(error) => {
            return SweepOutcome::refuse(format!(
                "garbage collection refused: metadata groups cannot be enumerated: {error}"
            ))
        }
    };
    if scopes.is_empty() {
        return SweepOutcome::refuse(
            "garbage collection refused: no data group is configured, so no manifest can be marked",
        );
    }

    let mut outcome = SweepOutcome::default();
    let present = enumerate_present(pools, &mut outcome);

    let mut refusals = Vec::new();
    let mut swept = 0usize;
    for scope in &scopes {
        match sweep_group(meta, pools, control, scope, &present, &mut outcome) {
            Ok(()) => swept += 1,
            Err(reason) => refusals.push(format!("group {} refused: {reason}", scope.group_id)),
        }
    }

    if !refusals.is_empty() {
        outcome.refusal = Some(format!(
            "garbage collection refused for {} of {} metadata group(s): {}",
            refusals.len(),
            scopes.len(),
            refusals.join("; ")
        ));
    }
    outcome.groups_refused = refusals;

    // Compaction is physical and only reclaims space a durable barrier or
    // tombstone already fenced, so it is safe across groups. A pass in which
    // every group refused must not touch the volumes at all.
    if swept > 0 {
        compact_pools(pools, &present, &mut outcome);
    }
    outcome
}

/// One group's pass. Every step either succeeds under a provable fence or
/// returns the reason the group is refused; it never deletes on a guess.
fn sweep_group(
    meta: &LocalMetaKv<OsFs>,
    pools: &BTreeMap<String, std::sync::Arc<PoolStriper<OsFs>>>,
    control: &ControlPlaneState,
    scope: &yadb_metakv::GcGroupScope,
    present: &BTreeMap<String, BTreeSet<BlobId>>,
    outcome: &mut SweepOutcome,
) -> Result<(), String> {
    let group = scope.group_id;

    // 1. Leader-only. A follower's applied state can lag a committed manifest,
    // and deleting from that view loses a fragment the manifest references.
    let (node_id, role, _, leader, _) = meta
        .role_info_for_group(group)
        .map_err(|error| format!("role is unreadable: {error}"))?;
    if role != yadb_metakv::RoleState::Leader {
        return Err(format!(
            "node {node_id} is not the leader of metadata group {group} (leader is {leader:?}); a lagging view could delete a fragment the committed state still references"
        ));
    }

    // 2. Writer fence. SQL tablet writes and block writes have no intent record,
    // so an in-flight writer is only visible through its per-group writer lease.
    // This is checked before admission closes, so a lease published earlier is
    // seen and one attempted later loses its CAS on the collector record.
    let live = yadb_metakv::gc::live_leases_in(meta, group)
        .map_err(|error| format!("writer fence is unreadable: {error}"))?;
    if !live.is_empty() {
        outcome.leases_live += live.len();
        return Err(format!(
            "{} live lease(s) ({} reader, {} writer) in metadata group {group}; no fragment of that group was deleted",
            live.len(),
            live.iter()
                .filter(|lease| lease.kind == gc::LeaseKind::Reader)
                .count(),
            live.iter()
                .filter(|lease| lease.kind == gc::LeaseKind::Writer)
                .count()
        ));
    }

    // 3. Collector lease of this group. It closes admission for the group, so
    // the mark taken below cannot be invalidated by a writer that starts later.
    let expected = match yadb_metakv::gc::live_collector_in(meta, group) {
        Ok(Some(lease)) => {
            return Err(format!(
                "another pass holds the collector lease of group {group} until {}",
                lease.expires_at_ms
            ))
        }
        Ok(None) => mod_revision(meta, &gc::group_collector_key(group))?,
        Err(error) => return Err(format!("collector lease is unreadable: {error}")),
    };
    let mut collector =
        yadb_metakv::gc::acquire_collector_in(meta, group, expected, COLLECTOR_LEASE_MS)
            .map_err(|error| format!("collector lease unavailable: {error}"))?;

    // 3b. Re-check the fence now that admission is closed. A writer that
    // published its lease between the check in step 2 and this acquisition is
    // visible here; one that tries after it loses its CAS, because publication
    // guards on this group's collector record being absent. Without this check
    // a writer could publish in that window, be absent from the mark, and have
    // its fragments deleted.
    let live = match yadb_metakv::gc::live_leases_in(meta, group) {
        Ok(live) => live,
        Err(error) => {
            let _ = collector.release();
            return Err(format!(
                "writer fence is unreadable during the pass: {error}; no fragment of group {group} was deleted"
            ));
        }
    };
    if !live.is_empty() {
        outcome.leases_live += live.len();
        let _ = collector.release();
        return Err(format!(
            "{} lease(s) appeared during the pass ({} reader, {} writer) in metadata group {group}; no fragment of that group was deleted",
            live.len(),
            live.iter()
                .filter(|lease| lease.kind == gc::LeaseKind::Reader)
                .count(),
            live.iter()
                .filter(|lease| lease.kind == gc::LeaseKind::Writer)
                .count()
        ));
    }

    // 4. Quorum-confirmed snapshot of this group, taken after admission closed.
    // A local applied view is not enough: it may lag a committed manifest.
    let prefixes: Vec<String> = LIVE_PREFIXES.iter().map(|p| (*p).to_string()).collect();
    let snapshot = match meta.try_linearizable_group_snapshot(group, &[], &prefixes) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let _ = collector.release();
            return Err(format!("linearizable snapshot failed: {error}"));
        }
    };

    // 5. Mark from that snapshot. An unreadable manifest, intent or extent map
    // makes the live set unprovable, so the group is refused.
    let mark = match mark_snapshot(&snapshot, control) {
        Ok(mark) => mark,
        Err(reason) => {
            let _ = collector.release();
            return Err(format!("live set is not provable: {reason}"));
        }
    };
    outcome.blobs_live += mark.len();

    // 6. Delete only this group's fragments. A fragment whose project this group
    // does not own belongs to another group's pass and is never considered here.
    if let Err(error) = collector.renew(COLLECTOR_LEASE_MS) {
        // Release before refusing: a pass that cannot prove it still holds the
        // record must not leave admission closed for this group until its TTL.
        let _ = collector.release();
        return Err(format!("{error}; no fragment of group {group} was deleted"));
    }
    let live_by_pool = group_by_pool(&mark);
    for (pool_name, pool_present) in present {
        let Some(striper) = pools.get(pool_name) else {
            outcome.pools_refused.push(format!(
                "pool `{pool_name}` refused: no mounted striper to sweep"
            ));
            continue;
        };
        if let Err(error) = collector.renew(COLLECTOR_LEASE_MS) {
            outcome.errors.push(format!(
                "sweep of group {group} stopped before pool `{pool_name}`: {error}"
            ));
            break;
        }
        let empty = BTreeSet::new();
        let live = live_by_pool.get(pool_name).unwrap_or(&empty);
        let dead: BTreeSet<BlobId> = pool_present
            .iter()
            .filter(|id| scope.covers(id.project_id()) && !live.contains(*id))
            .copied()
            .collect();
        if dead.is_empty() {
            continue;
        }
        let mut fence = || {
            // The deadline travels with each barrier: a successful renewal proves
            // the lease is live now, and the value it returns is what a target
            // compares against its own clock when the barrier arrives.
            collector
                .renew(COLLECTOR_LEASE_MS)
                .map(|_| collector.lease().expires_at_ms)
                .map_err(|error| error.to_string())
        };
        if sweep_pool(striper, pool_name, &dead, live, &mut fence, outcome).is_err() {
            // Same contract as a failed renew before a pool: the reason is in
            // `outcome.errors` and no later pool of this group deletes.
            break;
        }
    }

    if let Err(error) = collector.release() {
        outcome.errors.push(format!(
            "collector lease release failed for group {group}: {error}"
        ));
    }
    Ok(())
}

/// `mod_revision` of a key, or `0` when it is absent. Errors are reported, never
/// flattened into "absent": a fence that cannot be read must refuse the group.
fn mod_revision(meta: &LocalMetaKv<OsFs>, key: &str) -> Result<u64, String> {
    meta.try_snapshot(&[key.to_string()], &[])
        .map(|snapshot| snapshot.get(key).map_or(0, |kv| kv.mod_revision))
        .map_err(|error| format!("`{key}` cannot be read: {error}"))
}

/// Asks every slot of every mounted pool which physical fragment ids it holds.
/// A pool that cannot answer is recorded as refused and left out entirely.
fn enumerate_present(
    pools: &BTreeMap<String, std::sync::Arc<PoolStriper<OsFs>>>,
    outcome: &mut SweepOutcome,
) -> BTreeMap<String, BTreeSet<BlobId>> {
    let mut present: BTreeMap<String, BTreeSet<BlobId>> = BTreeMap::new();
    for (pool_name, striper) in pools {
        let mut pool_ids = BTreeSet::new();
        let mut failed = None;
        for slot in striper.slots() {
            match slot.scan_live() {
                Ok(ids) => {
                    for id in &ids {
                        pool_ids.insert(id.with_part(0));
                    }
                }
                Err(error) => {
                    failed = Some(format!(
                        "pool `{pool_name}` refused: slot {} cannot be enumerated: {error}",
                        slot.slot_index()
                    ));
                    break;
                }
            }
        }
        match failed {
            Some(reason) => outcome.pools_refused.push(reason),
            None => {
                present.insert(pool_name.clone(), pool_ids);
            }
        }
    }
    present
}

/// Seals and compacts every enumerated pool. A pool that was refused during
/// enumeration is not in `present` and is never touched.
fn compact_pools(
    pools: &BTreeMap<String, std::sync::Arc<PoolStriper<OsFs>>>,
    present: &BTreeMap<String, BTreeSet<BlobId>>,
    outcome: &mut SweepOutcome,
) {
    for (pool_name, striper) in pools {
        if !present.contains_key(pool_name) {
            continue;
        }
        for slot in striper.slots() {
            match slot.seal_and_compact() {
                Ok(bytes) => {
                    outcome.bytes_reclaimed = outcome.bytes_reclaimed.saturating_add(bytes)
                }
                Err(error) => outcome.errors.push(format!(
                    "pool `{pool_name}` slot {}: compaction failed: {error}",
                    slot.slot_index()
                )),
            }
        }
    }
}

fn group_by_pool(mark: &MarkSet) -> BTreeMap<String, BTreeSet<BlobId>> {
    let mut out: BTreeMap<String, BTreeSet<BlobId>> = BTreeMap::new();
    for (pool, id) in mark {
        out.entry(pool.clone()).or_default().insert(*id);
    }
    out
}

/// Deletes the dead ids of one pool.
///
/// A generation barrier is preferred because one durable record fences a whole
/// `(project, owner, channel)` range and lets compaction drop it in one pass. It
/// is only used when the channel allocates `(owner, generation)` from a monotonic
/// counter (so no future write can land at or below the barrier), *and* every dead
/// generation in the group is strictly below every live generation in that group:
/// then the barrier evicts exactly the dead set and nothing a committed revision
/// references. Otherwise each dead fragment gets an exact tombstone, which is
/// narrower and always safe.
///
/// `fence` proves the collector record still holds. It is called before the
/// first deletion, before every barrier group (one barrier can evict a whole
/// generation range) and every [`FENCE_CHECK_INTERVAL`] exact tombstones. A
/// failed check stops the pool immediately: no fragment after it is deleted,
/// because from that point admission is open again.
///
/// An allocation that is still in flight cannot be fenced by a barrier here. A
/// reservation writes no fragment, so it is never part of the dead set (and a
/// fresh generation is the newest in its group, above every `up_to` this pass can
/// compute); the fragments it later appends exist only while it holds a writer
/// lease, and a live lease makes the pass refuse before this point. If that lease
/// expires while the writer is stalled, its publication transaction carries the
/// lease's own `RevisionGuard` and therefore fails instead of publishing a map
/// that names fragments this pass reclaimed.
fn sweep_pool(
    striper: &PoolStriper<OsFs>,
    pool_name: &str,
    dead: &BTreeSet<BlobId>,
    live: &BTreeSet<BlobId>,
    fence: &mut dyn FnMut() -> Result<u64, String>,
    outcome: &mut SweepOutcome,
) -> Result<(), String> {
    // Nothing below may delete under an expired fence. The caller renewed before
    // entering the pool, but the mark and the grouping above take time.
    check_fence(fence, pool_name, "before the first deletion", outcome)?;

    let mut dead_groups: BTreeMap<(u32, u32, u8), u32> = BTreeMap::new();
    for id in dead {
        let entry = dead_groups
            .entry((id.project_id(), id.owner_id(), id.channel()))
            .or_insert(0);
        *entry = (*entry).max(id.generation());
    }
    let mut live_groups: BTreeMap<(u32, u32, u8), u32> = BTreeMap::new();
    for id in live {
        let entry = live_groups
            .entry((id.project_id(), id.owner_id(), id.channel()))
            .or_insert(u32::MAX);
        *entry = (*entry).min(id.generation());
    }

    let mut exact: Vec<BlobId> = Vec::new();
    for ((project, owner, channel), up_to) in dead_groups {
        let safe = BARRIER_SAFE_CHANNELS.contains(&channel)
            && live_groups
                .get(&(project, owner, channel))
                .is_none_or(|min_live| up_to < *min_live);
        if !safe {
            exact.extend(
                dead.iter()
                    .filter(|id| {
                        id.project_id() == project
                            && id.owner_id() == owner
                            && id.channel() == channel
                    })
                    .copied(),
            );
            continue;
        }
        // A barrier evicts every fragment in the range in one durable record, so
        // it is checked like a deletion batch even though it is not a tombstone.
        //
        // The check also returns the deadline it just proved, and that deadline
        // travels with the barrier. This is the one deletion the interval below
        // cannot bound: one record evicts a whole generation range, so a request
        // that reaches a target after the lease lapsed must be refused there
        // instead of deleting on behalf of a pass that lost its fence.
        let lease_expires_at_ms = check_fence(
            fence,
            pool_name,
            &format!("before barrier {project}/{owner}/ch{channel}<={up_to}"),
            outcome,
        )?;
        let mut barrier_ok = true;
        for slot in striper.slots() {
            match slot.apply_barrier(project, owner, channel, up_to, false, lease_expires_at_ms) {
                Ok(response) => outcome.fragments_deleted += response.evicted,
                Err(error) => {
                    barrier_ok = false;
                    outcome.errors.push(format!(
                        "pool `{pool_name}` slot {}: barrier {project}/{owner}/ch{channel}<={up_to} failed: {error}",
                        slot.slot_index()
                    ));
                }
            }
        }
        if barrier_ok {
            outcome.barriers_applied += 1;
        }
    }

    let mut deleted_since_check = 0usize;
    for id in exact {
        if deleted_since_check >= FENCE_CHECK_INTERVAL {
            check_fence(
                fence,
                pool_name,
                &format!("after {deleted_since_check} exact deletion(s)"),
                outcome,
            )?;
            deleted_since_check = 0;
        }
        // Deleting every part is what makes the tombstone meaningful: a fragment
        // that survives on one slot would be readable again after reconstruction.
        match striper.delete(id) {
            Ok(true) => outcome.fragments_deleted += striper.scheme().total_parts(),
            Ok(false) => {}
            Err(error) => outcome.errors.push(format!(
                "pool `{pool_name}`: tombstone for {id} failed: {error}"
            )),
        }
        deleted_since_check += 1;
    }
    Ok(())
}

/// Runs the caller's fence check and records a failure in `outcome.errors`.
///
/// The returned reason is what makes the caller stop: a pass that can no longer
/// prove it owns the collector record must not delete another fragment, and the
/// pool loop must not continue as if this pool finished.
///
/// A successful check also returns the lease deadline it proved, so the caller can
/// carry it to a target that must refuse the deletion if it applies it late.
fn check_fence(
    fence: &mut dyn FnMut() -> Result<u64, String>,
    pool_name: &str,
    when: &str,
    outcome: &mut SweepOutcome,
) -> Result<u64, String> {
    fence().map_err(|error| {
        let reason = format!(
            "pool `{pool_name}`: collector fence lost {when}: {error}; no further fragment was deleted"
        );
        outcome.errors.push(reason.clone());
        reason
    })
}

/// One snapshot of every manifest that can name a fragment.
fn mark(meta: &LocalMetaKv<OsFs>, control: &ControlPlaneState) -> Result<MarkSet, String> {
    let prefixes: Vec<String> = LIVE_PREFIXES.iter().map(|p| (*p).to_string()).collect();
    let snapshot = meta
        .try_snapshot(&[], &prefixes)
        .map_err(|error: MetaError| error.to_string())?;
    mark_snapshot(&snapshot, control)
}

/// The live set named by one metadata snapshot. Used by the legacy pass (one
/// whole-namespace snapshot) and by each Multi-Raft group pass (one
/// quorum-confirmed group snapshot).
fn mark_snapshot(
    snapshot: &yadb_metakv::MetaSnapshot,
    control: &ControlPlaneState,
) -> Result<MarkSet, String> {
    let mut mark: MarkSet = BTreeSet::new();

    for (key, value) in snapshot.scan_prefix("/db/") {
        if !key.contains("/shards/") {
            continue;
        }
        let manifest: TabletManifest = serde_json::from_slice(&value.value)
            .map_err(|error| format!("tablet manifest `{key}` is unreadable: {error}"))?;
        for blob in manifest.wal_blobs {
            mark.insert((manifest.wal_pool.clone(), blob));
        }
        for sst in manifest.sst_blobs {
            mark.insert((manifest.data_pool.clone(), sst.blob_id));
        }
    }

    for (key, value) in snapshot.scan_prefix("/s3/") {
        let manifest: ObjectManifest = serde_json::from_slice(&value.value)
            .map_err(|error| format!("object manifest `{key}` is unreadable: {error}"))?;
        for chunk in manifest.chunks {
            mark.insert((manifest.pool.clone(), chunk.blob_id));
        }
    }

    for (key, value) in snapshot.scan_prefix("/s3-intent/") {
        // An intent names the chunks of an upload that has not published yet. They
        // are live for collection purposes: the upload may still complete, and the
        // intent collector is the only thing allowed to abort it.
        for (pool, id) in yadb_s3::intent_chunk_ids(&value.value)
            .map_err(|error| format!("upload intent `{key}` is unreadable: {error}"))?
        {
            mark.insert((pool, id));
        }
    }

    let mut block_pools: BTreeMap<(u32, String), String> = BTreeMap::new();
    for (full_name, spec) in &control.block_volumes {
        let Some((project, volume)) = full_name.split_once('/') else {
            return Err(format!(
                "block volume `{full_name}` is not in `project/volume` form"
            ));
        };
        let Some((project_id, _)) = control.projects.get(project) else {
            return Err(format!(
                "block volume `{full_name}` names unknown project `{project}`"
            ));
        };
        block_pools.insert((*project_id, volume.to_string()), spec.pool.clone());
    }
    for (key, value) in snapshot.scan_prefix("/block/") {
        let rest = key.strip_prefix("/block/").unwrap_or_default();
        let Some((project_id, volume)) = rest.split_once('/') else {
            return Err(format!(
                "extent map key `{key}` is not in `pid/volume` form"
            ));
        };
        let project_id: u32 = project_id
            .parse()
            .map_err(|_| format!("extent map key `{key}` has a non-numeric project"))?;
        let Some(pool) = block_pools.get(&(project_id, volume.to_string())) else {
            return Err(format!(
                "extent map `{key}` has no pool: the control plane does not map it"
            ));
        };
        let map: yadb_block::VolumeExtentMap = serde_json::from_slice(&value.value)
            .map_err(|error| format!("extent map `{key}` is unreadable: {error}"))?;
        for blob in map.blocks.values() {
            mark.insert((pool.clone(), *blob));
        }
    }

    Ok(mark)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use yadb_blob::{ErasureScheme, VolumeConfig, VolumeStore};
    use yadb_block::{BlockError, BlockVolumeEngine};
    use yadb_control::{BlockVolumeSpec, ProjectQuotas, ProjectSpec};
    use yadb_db::DatabaseEngine;
    use yadb_metakv::KvCommand;
    use yadb_s3::ObjectStoreEngine;

    const POOL: &str = "data";
    const BLOCK_SIZE: u32 = 4096;

    /// One pool of three local mirror slots, one single-node metadata group, and
    /// the control-plane mapping the block channel needs.
    struct Fixture {
        dir: std::path::PathBuf,
        meta: LocalMetaKv<OsFs>,
        pool: Arc<PoolStriper<OsFs>>,
        pools: BTreeMap<String, Arc<PoolStriper<OsFs>>>,
        control: ControlPlaneState,
    }

    impl Fixture {
        fn new(tag: &str) -> Self {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dir =
                std::env::temp_dir().join(format!("yadb-gc-{tag}-{}-{nanos}", std::process::id()));
            Self::open(&dir)
        }

        fn open(dir: &std::path::Path) -> Self {
            let fs = OsFs::new(dir).unwrap();
            let meta = LocalMetaKv::open_single_node(&fs, "meta", 1).unwrap();
            let stores: Vec<VolumeStore<OsFs>> = (0..3)
                .map(|i| {
                    VolumeStore::open(
                        fs.clone(),
                        format!("pool/data-{i}"),
                        VolumeConfig::default(),
                    )
                    .unwrap()
                })
                .collect();
            let pool =
                Arc::new(PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, stores).unwrap());
            let mut control = ControlPlaneState::new();
            control.projects.insert(
                "proj".into(),
                (
                    1,
                    ProjectSpec {
                        quotas: ProjectQuotas::default(),
                        members: Vec::new(),
                    },
                ),
            );
            control.block_volumes.insert(
                "proj/disk".into(),
                BlockVolumeSpec {
                    pool: POOL.into(),
                    size_bytes: 1 << 20,
                },
            );
            let pools = BTreeMap::from([(POOL.to_string(), pool.clone())]);
            Self {
                dir: dir.to_path_buf(),
                meta,
                pool,
                pools,
                control,
            }
        }

        fn sweep(&self) -> SweepOutcome {
            sweep(&self.meta, &self.pools, &self.control)
        }

        /// Real bytes on disk across every slot.
        fn disk_bytes(&self) -> u64 {
            self.pool
                .slots()
                .iter()
                .map(|slot| {
                    slot.local_store()
                        .unwrap()
                        .stats()
                        .unwrap()
                        .total_disk_bytes
                })
                .sum()
        }

        /// Bytes the volume still considers live (records, not slack).
        fn live_bytes(&self) -> u64 {
            self.pool
                .slots()
                .iter()
                .map(|slot| slot.local_store().unwrap().stats().unwrap().live_bytes)
                .sum()
        }

        fn live_ids(&self) -> Vec<BlobId> {
            self.pool
                .slots()
                .iter()
                .flat_map(|slot| slot.local_store().unwrap().live_ids())
                .collect()
        }
    }

    fn block_engine(f: &Fixture) -> BlockVolumeEngine<OsFs> {
        let engine = BlockVolumeEngine::new(f.meta.clone(), BLOCK_SIZE);
        engine.register_pool(POOL, f.pool.clone());
        engine
    }

    fn write_generation(engine: &BlockVolumeEngine<OsFs>, value: u8) {
        let payload = vec![value; 16 * BLOCK_SIZE as usize];
        engine
            .write_at(1, "disk", POOL, 1 << 20, 0, &payload)
            .unwrap();
    }

    #[test]
    fn repeated_block_overwrite_returns_disk_to_the_live_set() {
        let f = Fixture::new("block-overwrite");
        let engine = block_engine(&f);
        for i in 0..48u8 {
            write_generation(&engine, i);
        }
        let before = f.disk_bytes();
        let outcome = f.sweep();
        assert!(outcome.refusal.is_none(), "{outcome:?}");
        assert!(outcome.barriers_applied > 0, "{outcome:?}");
        assert!(outcome.fragments_deleted > 0, "{outcome:?}");
        assert!(outcome.bytes_reclaimed > 0, "{outcome:?}");
        assert!(outcome.errors.is_empty(), "{outcome:?}");

        let after = f.disk_bytes();
        let live = f.live_bytes();
        eprintln!(
            "block overwrite: before={before} after={after} live={live} fragments={} reclaimed={}",
            outcome.fragments_deleted, outcome.bytes_reclaimed
        );
        assert!(after < before, "before={before} after={after}");
        // Only the newest generation may remain: 16 fragments per slot plus the
        // per-segment magic. The bound is generous but far below `before`.
        assert!(
            after <= live + 3 * 64 * 1024,
            "before={before} after={after} live={live}"
        );

        // The surviving bytes are the newest write, intact.
        let read = engine
            .read_at(1, "disk", POOL, 1 << 20, 0, 16 * BLOCK_SIZE as usize)
            .unwrap();
        assert_eq!(read, vec![47u8; 16 * BLOCK_SIZE as usize]);

        // A rerun with nothing to reclaim deletes nothing and frees nothing.
        let again = f.sweep();
        assert!(again.refusal.is_none(), "{again:?}");
        assert_eq!(again.fragments_deleted, 0, "{again:?}");
        assert_eq!(again.bytes_reclaimed, 0, "{again:?}");
    }

    #[test]
    fn table_flush_and_compaction_garbage_is_reclaimed() {
        let f = Fixture::new("table-compaction");
        let db = DatabaseEngine::new(f.meta.clone(), 16);
        db.register_pool("wal", f.pool.clone());
        db.register_pool("data", f.pool.clone());
        db.execute_sql(
            1,
            "db",
            "wal",
            "data",
            1,
            "CREATE TABLE t (id INT PRIMARY KEY, v INT);",
        )
        .unwrap();
        for i in 0..200 {
            db.execute_sql(
                1,
                "db",
                "wal",
                "data",
                1,
                &format!("INSERT INTO t VALUES ({i}, {i});"),
            )
            .unwrap();
        }
        // Half the rows go away, so the merged image is smaller than the history.
        db.execute_sql(1, "db", "wal", "data", 1, "DELETE FROM t WHERE id < 100;")
            .unwrap();

        let before = f.disk_bytes();
        let outcome = f.sweep();
        assert!(outcome.refusal.is_none(), "{outcome:?}");
        assert!(outcome.fragments_deleted > 0, "{outcome:?}");
        assert!(outcome.bytes_reclaimed > 0, "{outcome:?}");
        assert!(outcome.errors.is_empty(), "{outcome:?}");
        let after = f.disk_bytes();
        eprintln!(
            "table compaction: before={before} after={after} live={} fragments={} reclaimed={}",
            f.live_bytes(),
            outcome.fragments_deleted,
            outcome.bytes_reclaimed
        );
        assert!(after < before, "before={before} after={after}");

        // The table is still correct: the surviving rows are readable and the
        // deleted ones are gone.
        let rows = db
            .execute_sql(1, "db", "wal", "data", 1, "SELECT id, v FROM t;")
            .unwrap();
        assert_eq!(rows.rows.len(), 100, "{rows:?}");
        let one = db
            .execute_sql(1, "db", "wal", "data", 1, "SELECT v FROM t WHERE id = 199;")
            .unwrap();
        assert_eq!(one.rows.len(), 1, "{one:?}");
    }

    #[test]
    fn s3_overwrite_and_delete_chunks_are_reclaimed() {
        let f = Fixture::new("s3-overwrite");
        let s3 = ObjectStoreEngine::new(f.meta.clone(), 8 * 1024);
        s3.register_pool(POOL, f.pool.clone());
        for i in 0..8u8 {
            s3.put_object(
                1,
                "store",
                POOL,
                "bucket",
                "key",
                "text/plain",
                &vec![i; 32 * 1024],
            )
            .unwrap();
        }
        s3.put_object(1, "store", POOL, "bucket", "keep", "text/plain", b"keep-me")
            .unwrap();
        s3.delete_object(1, "store", "bucket", "key").unwrap();

        let before = f.disk_bytes();
        let outcome = f.sweep();
        assert!(outcome.refusal.is_none(), "{outcome:?}");
        assert!(outcome.fragments_deleted > 0, "{outcome:?}");
        assert!(outcome.bytes_reclaimed > 0, "{outcome:?}");
        assert!(outcome.errors.is_empty(), "{outcome:?}");
        let after = f.disk_bytes();
        let live = f.live_bytes();
        eprintln!(
            "s3 overwrite+delete: before={before} after={after} live={live} fragments={} reclaimed={}",
            outcome.fragments_deleted, outcome.bytes_reclaimed
        );
        assert!(after < before, "before={before} after={after}");
        assert!(
            after <= live + 3 * 64 * 1024,
            "before={before} after={after} live={live}"
        );

        let (_, bytes) = s3.get_object(1, "store", "bucket", "keep").unwrap();
        assert_eq!(bytes, b"keep-me");
        assert!(s3.get_object(1, "store", "bucket", "key").is_err());
    }

    #[test]
    fn fragments_of_a_refused_write_are_reclaimed_and_the_old_data_survives() {
        let f = Fixture::new("refused-write");
        let engine = block_engine(&f);
        engine
            .write_at(1, "disk", POOL, 1 << 20, 0, b"committed-before")
            .unwrap();
        let before = f.disk_bytes();

        // The fragments are appended, then the publication is refused: exactly the
        // state a client-visible "write did not commit" leaves behind.
        let refused = engine.write_at_with(1, "disk", POOL, 1 << 20, 0, b"never-published", |_| {
            Err(BlockError::Conflict)
        });
        assert!(matches!(refused, Err(BlockError::Conflict)), "{refused:?}");
        let after_failure = f.disk_bytes();
        assert!(
            after_failure > before,
            "the refused write must have appended"
        );

        let outcome = f.sweep();
        eprintln!(
            "refused write: committed={before} after-failure={after_failure} after-sweep={} fragments={} reclaimed={}",
            f.disk_bytes(),
            outcome.fragments_deleted,
            outcome.bytes_reclaimed
        );
        assert!(outcome.refusal.is_none(), "{outcome:?}");
        assert!(outcome.fragments_deleted > 0, "{outcome:?}");
        assert!(outcome.errors.is_empty(), "{outcome:?}");
        assert!(
            f.disk_bytes() <= before + 3 * 64 * 1024,
            "before={before} after={}",
            f.disk_bytes()
        );
        let read = engine
            .read_at(1, "disk", POOL, 1 << 20, 0, b"committed-before".len())
            .unwrap();
        assert_eq!(read, b"committed-before");
    }

    #[test]
    fn a_crash_between_barrier_and_compaction_loses_no_referenced_fragment() {
        let f = Fixture::new("crash-barrier");
        let engine = block_engine(&f);
        for i in 0..6u8 {
            write_generation(&engine, i);
        }
        let newest = *f
            .live_ids()
            .iter()
            .max_by_key(|id| (id.owner_id(), id.generation()))
            .expect("six generations were written");
        let owner = newest.owner_id();
        let channel = newest.channel();

        // Durably fence the five superseded generations, then stop: no compaction
        // and no released collector lease. This is the state a pass leaves behind
        // if the process dies after its barrier records are durable.
        for slot in f.pool.slots() {
            slot.apply_barrier(1, owner, channel, newest.generation() - 1, false, DEADLINE)
                .unwrap();
        }
        let collector = yadb_metakv::gc::acquire_collector(&f.meta, 0, 120_000).unwrap();
        let mut expired = collector.lease().clone();
        collector.release().unwrap();
        expired.expires_at_ms = 1;
        f.meta
            .execute(KvCommand::Put {
                key: yadb_metakv::gc::COLLECTOR_KEY.into(),
                value: serde_json::to_vec(&expired).unwrap(),
            })
            .unwrap();
        let fenced_disk = f.disk_bytes();
        let dir = f.dir.clone();
        drop(f);

        // Reopen exactly as a restarted daemon would: the expired collector lease
        // is reaped, the durable barriers survive, and the rerun finishes.
        let reopened = Fixture::open(&dir);
        let outcome = reopened.sweep();
        assert!(outcome.refusal.is_none(), "{outcome:?}");
        assert!(outcome.bytes_reclaimed > 0, "{outcome:?}");
        assert!(outcome.errors.is_empty(), "{outcome:?}");
        assert!(
            reopened.disk_bytes() < fenced_disk,
            "the rerun must finish the compaction it inherited"
        );

        let engine = block_engine(&reopened);
        let read = engine
            .read_at(1, "disk", POOL, 1 << 20, 0, 16 * BLOCK_SIZE as usize)
            .unwrap();
        assert_eq!(read, vec![5u8; 16 * BLOCK_SIZE as usize]);
    }

    #[test]
    fn a_live_reader_lease_refuses_the_pass_and_deletes_nothing() {
        let f = Fixture::new("refuse-reader");
        let engine = block_engine(&f);
        for i in 0..4u8 {
            engine
                .write_at(1, "disk", POOL, 1 << 20, 0, &vec![i; BLOCK_SIZE as usize])
                .unwrap();
        }
        let before = f.disk_bytes();
        let lease = yadb_metakv::gc::acquire_reader(&f.meta, 1, f.meta.revision_or_panic(), 30_000)
            .unwrap();
        let outcome = f.sweep();
        let reason = outcome.refusal.clone().expect("a live lease must refuse");
        assert!(reason.contains("live lease"), "{reason}");
        assert_eq!(outcome.leases_live, 1, "{outcome:?}");
        assert_eq!(outcome.fragments_deleted, 0, "{outcome:?}");
        assert_eq!(f.disk_bytes(), before, "a refused pass must not free bytes");
        lease.release().unwrap();

        // With the lease gone the same pass is allowed to run.
        let outcome = f.sweep();
        assert!(outcome.refusal.is_none(), "{outcome:?}");
        assert!(outcome.fragments_deleted > 0, "{outcome:?}");
    }

    #[test]
    fn an_unreadable_lease_record_refuses_the_pass() {
        let f = Fixture::new("refuse-broken-lease");
        let engine = block_engine(&f);
        for i in 0..3u8 {
            engine
                .write_at(1, "disk", POOL, 1 << 20, 0, &vec![i; BLOCK_SIZE as usize])
                .unwrap();
        }
        f.meta
            .execute(KvCommand::Put {
                key: "/gc-lease/reader/broken".into(),
                value: b"not-json".to_vec(),
            })
            .unwrap();
        let before = f.disk_bytes();
        let outcome = f.sweep();
        assert!(outcome.refusal.is_some(), "{outcome:?}");
        assert_eq!(outcome.fragments_deleted, 0, "{outcome:?}");
        assert_eq!(f.disk_bytes(), before);
    }

    #[test]
    fn another_collector_lease_refuses_the_pass() {
        let f = Fixture::new("refuse-collector");
        let engine = block_engine(&f);
        for i in 0..3u8 {
            engine
                .write_at(1, "disk", POOL, 1 << 20, 0, &vec![i; BLOCK_SIZE as usize])
                .unwrap();
        }
        let held = yadb_metakv::gc::acquire_collector(&f.meta, 0, 60_000).unwrap();
        let outcome = f.sweep();
        let reason = outcome
            .refusal
            .clone()
            .expect("a held collector must refuse");
        assert!(reason.contains("collector lease"), "{reason}");
        assert_eq!(outcome.fragments_deleted, 0, "{outcome:?}");
        held.release().unwrap();
        assert!(f.sweep().refusal.is_none());
    }

    #[test]
    fn a_pass_publishes_a_watermark_that_fences_a_lagging_reader() {
        let f = Fixture::new("watermark");
        let engine = block_engine(&f);
        for i in 0..3u8 {
            engine
                .write_at(1, "disk", POOL, 1 << 20, 0, &vec![i; BLOCK_SIZE as usize])
                .unwrap();
        }
        assert_eq!(yadb_metakv::gc::watermark(&f.meta).unwrap(), 0);
        let before_sweep = f.meta.revision_or_panic();
        let outcome = f.sweep();
        assert!(outcome.refusal.is_none(), "{outcome:?}");
        let mark = yadb_metakv::gc::watermark(&f.meta).unwrap();
        assert!(
            mark >= before_sweep,
            "the watermark must cover the revision the mark was taken at ({mark} < {before_sweep})"
        );
        assert!(
            yadb_metakv::gc::reader_is_safe(&f.meta).is_ok(),
            "a reader at the current applied revision is safe"
        );

        // A second pass only raises it.
        f.sweep();
        assert!(yadb_metakv::gc::watermark(&f.meta).unwrap() >= mark);
    }

    #[test]
    fn a_referenced_fragment_is_never_deleted() {
        let f = Fixture::new("keep-live");
        let engine = block_engine(&f);
        // Two blocks in one volume: only the rewritten block is superseded.
        engine
            .write_at(1, "disk", POOL, 1 << 20, 0, b"first-block")
            .unwrap();
        engine
            .write_at(1, "disk", POOL, 1 << 20, BLOCK_SIZE as u64, b"second-block")
            .unwrap();
        let outcome = f.sweep();
        assert!(outcome.refusal.is_none(), "{outcome:?}");
        assert_eq!(
            outcome.fragments_deleted, 0,
            "nothing is dead yet: {outcome:?}"
        );
        let read = engine
            .read_at(1, "disk", POOL, 1 << 20, 0, b"first-block".len())
            .unwrap();
        assert_eq!(read, b"first-block");
    }

    /// A channel outside `BARRIER_SAFE_CHANNELS`, so `sweep_pool` takes the
    /// exact-tombstone path and the in-pool check interval is exercised one
    /// deletion at a time.
    const EXACT_ONLY_CHANNEL: u8 = 6;

    /// The deadline a test fence reports as proved: far enough away that the
    /// barrier path is not exercising expiry, only the plumbing.
    const DEADLINE: u64 = u64::MAX;

    /// Writes `count` unreferenced fragments and returns their logical ids.
    fn seed_exact_dead(f: &Fixture, count: u32) -> BTreeSet<BlobId> {
        let mut dead = BTreeSet::new();
        for generation in 0..count {
            let id = BlobId::new(1, 1, generation, 0, EXACT_ONLY_CHANNEL, 0);
            f.pool.put_blob(id, b"garbage").unwrap();
            dead.insert(id);
        }
        dead
    }

    /// Whether any slot still holds a part of `id`.
    fn fragment_present(f: &Fixture, id: BlobId) -> bool {
        f.pool.slots().iter().any(|slot| {
            slot.local_store()
                .unwrap()
                .live_ids()
                .iter()
                .any(|present| present.with_part(0) == id)
        })
    }

    #[test]
    fn the_fence_is_checked_before_the_first_deletion_and_every_interval() {
        let f = Fixture::new("fence-interval");
        let dead = seed_exact_dead(&f, 2 * FENCE_CHECK_INTERVAL as u32 + 1);
        let calls = std::cell::Cell::new(0usize);
        let first_call_saw_everything = std::cell::Cell::new(false);
        let mut fence = || {
            if calls.get() == 0 {
                // The closure can see the pool, so this proves the check really
                // runs before the first tombstone rather than after a batch.
                first_call_saw_everything.set(dead.iter().all(|id| fragment_present(&f, *id)));
            }
            calls.set(calls.get() + 1);
            Ok(DEADLINE)
        };

        let mut outcome = SweepOutcome::default();
        sweep_pool(
            &f.pool,
            POOL,
            &dead,
            &BTreeSet::new(),
            &mut fence,
            &mut outcome,
        )
        .unwrap();

        // One check before the first tombstone, then one before deletion
        // `FENCE_CHECK_INTERVAL + 1` and one before `2 * FENCE_CHECK_INTERVAL + 1`.
        assert_eq!(calls.get(), 3, "{outcome:?}");
        assert!(
            first_call_saw_everything.get(),
            "the first check must run before any fragment is deleted"
        );
        assert_eq!(
            outcome.fragments_deleted,
            dead.len() * f.pool.scheme().total_parts(),
            "{outcome:?}"
        );
        assert!(outcome.errors.is_empty(), "{outcome:?}");
        assert!(
            dead.iter().all(|id| !fragment_present(&f, *id)),
            "a clean check must not stop the sweep: {outcome:?}"
        );
    }

    #[test]
    fn a_failed_check_mid_sweep_stops_further_deletions() {
        let f = Fixture::new("fence-fail-mid");
        let dead = seed_exact_dead(&f, 2 * FENCE_CHECK_INTERVAL as u32 + 1);
        let calls = std::cell::Cell::new(0usize);
        let mut fence = || {
            calls.set(calls.get() + 1);
            if calls.get() == 2 {
                Err("injected fence loss".to_string())
            } else {
                Ok(DEADLINE)
            }
        };

        let mut outcome = SweepOutcome::default();
        let error = sweep_pool(
            &f.pool,
            POOL,
            &dead,
            &BTreeSet::new(),
            &mut fence,
            &mut outcome,
        )
        .expect_err("a failed check must stop the pool");

        assert!(error.contains("collector fence lost"), "{error}");
        assert!(error.contains("injected fence loss"), "{error}");
        assert!(error.contains(POOL), "{error}");
        assert_eq!(calls.get(), 2, "no check may run after the failure");
        assert_eq!(
            outcome.fragments_deleted,
            FENCE_CHECK_INTERVAL * f.pool.scheme().total_parts(),
            "only deletions before the failed check may run: {outcome:?}"
        );
        assert_eq!(outcome.errors.len(), 1, "{outcome:?}");
        assert!(
            outcome.errors[0].contains("injected fence loss"),
            "{outcome:?}"
        );

        let (deleted, survivors): (Vec<_>, Vec<_>) = dead
            .iter()
            .copied()
            .enumerate()
            .partition(|(index, _)| *index < FENCE_CHECK_INTERVAL);
        assert!(
            deleted.iter().all(|(_, id)| !fragment_present(&f, *id)),
            "fragments before the failure were deleted: {outcome:?}"
        );
        assert!(
            survivors.iter().all(|(_, id)| fragment_present(&f, *id)),
            "fragments after the failure must still be present: {outcome:?}"
        );
    }

    #[test]
    fn a_pass_without_a_failed_check_still_deletes_every_dead_fragment() {
        let f = Fixture::new("fence-no-failure");
        let dead = seed_exact_dead(&f, 2 * FENCE_CHECK_INTERVAL as u32 + 1);

        // The real legacy pass, so the closure is wired to the live collector
        // record rather than a test stub.
        let outcome = f.sweep();
        assert!(outcome.refusal.is_none(), "{outcome:?}");
        assert!(outcome.errors.is_empty(), "{outcome:?}");
        assert_eq!(
            outcome.fragments_deleted,
            dead.len() * f.pool.scheme().total_parts(),
            "{outcome:?}"
        );
        assert!(
            dead.iter().all(|id| !fragment_present(&f, *id)),
            "a pass whose checks all succeed must delete every dead fragment: {outcome:?}"
        );
    }

    /// A barrier is the one deletion the fence interval cannot bound, so the
    /// deadline a successful check proved has to reach the target. A pass that
    /// dropped it — passing `u64::MAX`, or checking the lease again only after the
    /// barrier — would evict a whole generation range under a lapsed lease, and
    /// nothing else in this file would notice.
    #[test]
    fn a_barrier_carries_the_deadline_the_fence_proved_and_a_lapsed_one_evicts_nothing() {
        let f = Fixture::new("barrier-deadline");
        // S3's channel is barrier-safe, so `sweep_pool` takes the barrier path
        // rather than the exact-tombstone one.
        let mut dead = BTreeSet::new();
        for generation in 0..3 {
            let id = BlobId::new(1, 1, generation, 0, yadb_s3::S3_CHANNEL, 0);
            f.pool.put_blob(id, b"garbage").unwrap();
            dead.insert(id);
        }

        // The check succeeds and reports a deadline that has already passed. The
        // real fence cannot produce that by itself — `extend` refuses a late
        // acknowledgement — but the deadline it proves can lapse during the leg
        // that follows it: the grouping above, then one RPC per remote slot. The
        // target is the only party that sees the barrier at the moment it would
        // evict, so this stub puts the deadline in the past deterministically
        // instead of racing that window.
        let mut expired = || Ok(1u64);
        let mut outcome = SweepOutcome::default();
        sweep_pool(
            &f.pool,
            POOL,
            &dead,
            &BTreeSet::new(),
            &mut expired,
            &mut outcome,
        )
        .expect("a refused barrier is reported in the outcome, not returned");

        assert_eq!(outcome.fragments_deleted, 0, "{outcome:?}");
        assert_eq!(outcome.barriers_applied, 0, "{outcome:?}");
        assert_eq!(
            outcome.errors.len(),
            f.pool.slots().len(),
            "every slot must report its refusal: {outcome:?}"
        );
        assert!(
            outcome
                .errors
                .iter()
                .all(|e| e.contains("expired at 1 ms") && e.contains("barrier")),
            "{outcome:?}"
        );
        assert!(
            dead.iter().all(|id| fragment_present(&f, *id)),
            "an expired barrier must evict nothing: {outcome:?}"
        );

        // Positive control on the same pool: the only change is the deadline, so
        // the refusal above is the deadline and not a broken barrier path.
        let mut live = || Ok(DEADLINE);
        let mut outcome = SweepOutcome::default();
        sweep_pool(
            &f.pool,
            POOL,
            &dead,
            &BTreeSet::new(),
            &mut live,
            &mut outcome,
        )
        .unwrap();
        assert_eq!(outcome.errors, Vec::<String>::new(), "{outcome:?}");
        assert_eq!(outcome.barriers_applied, 1, "{outcome:?}");
        assert_eq!(
            outcome.fragments_deleted,
            dead.len() * f.pool.scheme().total_parts(),
            "{outcome:?}"
        );
        assert!(
            dead.iter().all(|id| !fragment_present(&f, *id)),
            "{outcome:?}"
        );
    }

    /// Multi-Raft passes: one leader-only, quorum-confirmed sweep per data
    /// group. These tests use the OpenRaft adapters, which are always compiled
    /// in now.
    mod openraft_tests {
        use super::*;
        use std::collections::{BTreeMap, BTreeSet};
        use std::path::{Path, PathBuf};
        use std::sync::Mutex;
        use std::time::Duration;

        use openraft::error::{
            Fatal, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError, Unreachable,
        };
        use openraft::network::RPCOption;
        use openraft::raft::{
            AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest,
            VoteResponse,
        };
        use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, Snapshot, Vote};
        use yadb_metakv::multiraft::{GroupIdentity, ReplicaIdentity};
        use yadb_metakv::openraft_runtime::{YadbRaft, YadbRaftTypeConfig as C};
        use yadb_metakv::openraft_state_machine::DurableMetaStateMachine;
        use yadb_metakv::openraft_storage::DurableOpenRaftLog;

        use yadb_s3::S3Error;

        const CLUSTER: [u8; 16] = [71; 16];

        /// A single-node group never calls its network. Refusing every call is
        /// enough and keeps the fixture honest about having no peers.
        struct NoNetwork;
        impl RaftNetworkFactory<C> for NoNetwork {
            type Network = NoNetwork;
            async fn new_client(&mut self, _: u32, _: &BasicNode) -> NoNetwork {
                NoNetwork
            }
        }
        impl RaftNetwork<C> for NoNetwork {
            async fn append_entries(
                &mut self,
                _: AppendEntriesRequest<C>,
                _: RPCOption,
            ) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>>
            {
                Err(Unreachable::new(&std::io::Error::other("singleton")).into())
            }
            async fn vote(
                &mut self,
                _: VoteRequest<u32>,
                _: RPCOption,
            ) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
                Err(Unreachable::new(&std::io::Error::other("singleton")).into())
            }
            async fn full_snapshot(
                &mut self,
                _: Vote<u32>,
                _: Snapshot<C>,
                _: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
                _: RPCOption,
            ) -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
                Err(Unreachable::new(&std::io::Error::other("singleton")).into())
            }
        }

        /// In-process transport between the two nodes of the follower fixture.
        #[derive(Clone)]
        struct TestNetwork {
            nodes: Arc<Mutex<BTreeMap<u32, YadbRaft>>>,
            target: u32,
        }

        impl TestNetwork {
            fn peer(&self) -> Result<YadbRaft, Unreachable> {
                self.nodes
                    .lock()
                    .unwrap()
                    .get(&self.target)
                    .cloned()
                    .ok_or_else(|| {
                        Unreachable::new(&std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "unknown node",
                        ))
                    })
            }
        }

        impl RaftNetworkFactory<C> for TestNetwork {
            type Network = Self;
            async fn new_client(&mut self, target: u32, _: &BasicNode) -> Self {
                Self {
                    target,
                    ..self.clone()
                }
            }
        }

        impl RaftNetwork<C> for TestNetwork {
            async fn append_entries(
                &mut self,
                rpc: AppendEntriesRequest<C>,
                _: RPCOption,
            ) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>>
            {
                let peer = self.peer()?;
                peer.append_entries(rpc)
                    .await
                    .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            async fn vote(
                &mut self,
                rpc: VoteRequest<u32>,
                _: RPCOption,
            ) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
                let peer = self.peer()?;
                peer.vote(rpc)
                    .await
                    .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            async fn full_snapshot(
                &mut self,
                vote: Vote<u32>,
                snapshot: Snapshot<C>,
                cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
                _: RPCOption,
            ) -> Result<SnapshotResponse<u32>, StreamingError<C, Fatal<u32>>> {
                let peer = self.peer().map_err(StreamingError::from)?;
                tokio::select! {
                    result = peer.install_full_snapshot(vote, snapshot) => result
                        .map_err(|e| StreamingError::RemoteError(RemoteError::new(self.target, e))),
                    closed = cancel => Err(StreamingError::Closed(closed)),
                }
            }
        }

        struct GroupFixture {
            meta: LocalMetaKv<OsFs>,
            pool: Arc<PoolStriper<OsFs>>,
            pools: BTreeMap<String, Arc<PoolStriper<OsFs>>>,
            control: ControlPlaneState,
            rafts: Vec<YadbRaft>,
        }

        impl GroupFixture {
            fn new(dir: PathBuf, meta: LocalMetaKv<OsFs>, rafts: Vec<YadbRaft>) -> Self {
                let fs = OsFs::new(&dir).unwrap();
                let stores: Vec<VolumeStore<OsFs>> = (0..3)
                    .map(|i| {
                        VolumeStore::open(
                            fs.clone(),
                            format!("pool/data-{i}"),
                            VolumeConfig::default(),
                        )
                        .unwrap()
                    })
                    .collect();
                let pool = Arc::new(
                    PoolStriper::new(ErasureScheme::Mirror { replicas: 3 }, stores).unwrap(),
                );
                let mut control = ControlPlaneState::new();
                for (name, id) in [("p1", 1u32), ("p2", 2u32)] {
                    control.projects.insert(
                        name.into(),
                        (
                            id,
                            ProjectSpec {
                                quotas: ProjectQuotas::default(),
                                members: Vec::new(),
                            },
                        ),
                    );
                    control.block_volumes.insert(
                        format!("{name}/disk"),
                        BlockVolumeSpec {
                            pool: POOL.into(),
                            size_bytes: 1 << 20,
                        },
                    );
                }
                let pools = BTreeMap::from([(POOL.to_string(), pool.clone())]);
                Self {
                    meta,
                    pool,
                    pools,
                    control,
                    rafts,
                }
            }

            fn engine(&self) -> BlockVolumeEngine<OsFs> {
                let engine = BlockVolumeEngine::new(self.meta.clone(), BLOCK_SIZE);
                engine.register_pool(POOL, self.pool.clone());
                engine
            }

            fn s3(&self) -> ObjectStoreEngine<OsFs> {
                let s3 = ObjectStoreEngine::new(self.meta.clone(), 8 * 1024);
                s3.register_pool(POOL, self.pool.clone());
                s3
            }

            fn sweep(&self) -> SweepOutcome {
                sweep(&self.meta, &self.pools, &self.control)
            }

            fn live_ids(&self) -> Vec<BlobId> {
                self.pool
                    .slots()
                    .iter()
                    .flat_map(|slot| slot.local_store().unwrap().live_ids())
                    .collect()
            }

            fn distinct_ids(&self) -> BTreeSet<BlobId> {
                self.live_ids()
                    .into_iter()
                    .map(|id| id.with_part(0))
                    .collect()
            }
        }

        async fn shutdown(f: GroupFixture) {
            let GroupFixture { meta, rafts, .. } = f;
            drop(meta);
            for raft in rafts {
                let _ = raft.shutdown().await;
            }
        }

        fn temp_dir(tag: &str) -> PathBuf {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dir = std::env::temp_dir()
                .join(format!("yadb-gc-raft-{tag}-{}-{nanos}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        }

        async fn single_group(dir: &Path, group: u64) -> (YadbRaft, DurableMetaStateMachine) {
            let identity = ReplicaIdentity::new(
                GroupIdentity {
                    cluster: CLUSTER,
                    group,
                    incarnation: 1,
                },
                1,
                vec![1],
            )
            .unwrap();
            let log =
                DurableOpenRaftLog::create(dir.join(format!("g{group}-log")), &identity).unwrap();
            let state =
                DurableMetaStateMachine::create(dir.join(format!("g{group}-state")), &identity)
                    .unwrap();
            let config = Arc::new(openraft::Config::default().validate().unwrap());
            let raft = YadbRaft::new(1, config, NoNetwork, log, state.clone())
                .await
                .unwrap();
            raft.initialize(BTreeMap::from([(1, BasicNode::new("one"))]))
                .await
                .unwrap();
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "leader")
                .await
                .unwrap();
            (raft, state)
        }

        /// Two groups (2 and 3) over projects 1 and 2, with group 1 as the
        /// control group. Every group is a single-node cluster on node 1, which
        /// is what a one-node deployment looks like.
        async fn multigroup(tag: &str) -> GroupFixture {
            let dir = temp_dir(tag);
            let (control, control_state) = single_group(&dir, 1).await;
            let (group2, group2_state) = single_group(&dir, 2).await;
            let (group3, group3_state) = single_group(&dir, 3).await;
            let meta = LocalMetaKv::from_openraft_groups(
                1,
                BTreeMap::from([
                    (1, (control.clone(), control_state)),
                    (2, (group2.clone(), group2_state)),
                    (3, (group3.clone(), group3_state)),
                ]),
                BTreeMap::from([(1, 2), (2, 3)]),
                BTreeMap::new(),
                tokio::runtime::Handle::current(),
            )
            .unwrap();
            GroupFixture::new(dir, meta, vec![control, group2, group3])
        }

        /// Group 2 is a two-node cluster whose local node (1) is kept a follower
        /// by a long election timeout; node 2 is the leader. Groups 1 and 3 are
        /// single-node leaders on node 1.
        async fn follower_multigroup(tag: &str) -> (GroupFixture, LocalMetaKv<OsFs>) {
            let dir = temp_dir(tag);
            let (control, control_state) = single_group(&dir, 1).await;
            let (group3, group3_state) = single_group(&dir, 3).await;

            let shared = Arc::new(Mutex::new(BTreeMap::new()));
            let follower_config = Arc::new(
                openraft::Config {
                    election_timeout_min: 2_000,
                    election_timeout_max: 3_000,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            );
            let leader_config = Arc::new(openraft::Config::default().validate().unwrap());
            let node1 = ReplicaIdentity::new(
                GroupIdentity {
                    cluster: CLUSTER,
                    group: 2,
                    incarnation: 1,
                },
                1,
                vec![1, 2],
            )
            .unwrap();
            let node2 = ReplicaIdentity::new(
                GroupIdentity {
                    cluster: CLUSTER,
                    group: 2,
                    incarnation: 1,
                },
                2,
                vec![1, 2],
            )
            .unwrap();
            let log1 = DurableOpenRaftLog::create(dir.join("g2-n1-log"), &node1).unwrap();
            let state1 = DurableMetaStateMachine::create(dir.join("g2-n1-state"), &node1).unwrap();
            let raft1 = YadbRaft::new(
                1,
                follower_config,
                TestNetwork {
                    nodes: shared.clone(),
                    target: 0,
                },
                log1,
                state1.clone(),
            )
            .await
            .unwrap();
            let log2 = DurableOpenRaftLog::create(dir.join("g2-n2-log"), &node2).unwrap();
            let state2 = DurableMetaStateMachine::create(dir.join("g2-n2-state"), &node2).unwrap();
            let raft2 = YadbRaft::new(
                2,
                leader_config,
                TestNetwork {
                    nodes: shared.clone(),
                    target: 0,
                },
                log2,
                state2.clone(),
            )
            .await
            .unwrap();
            shared.lock().unwrap().insert(1, raft1.clone());
            shared.lock().unwrap().insert(2, raft2.clone());
            raft2
                .initialize(BTreeMap::from([
                    (1, BasicNode::new("one")),
                    (2, BasicNode::new("two")),
                ]))
                .await
                .unwrap();
            raft2
                .wait(Some(Duration::from_secs(5)))
                .current_leader(2, "leader")
                .await
                .unwrap();

            let meta = LocalMetaKv::from_openraft_groups(
                1,
                BTreeMap::from([
                    (1, (control.clone(), control_state)),
                    (2, (raft1.clone(), state1)),
                    (3, (group3.clone(), group3_state)),
                ]),
                BTreeMap::from([(1, 2), (2, 3)]),
                BTreeMap::new(),
                tokio::runtime::Handle::current(),
            )
            .unwrap();
            let leader_meta = LocalMetaKv::from_openraft(
                raft2.clone(),
                state2,
                BTreeMap::new(),
                tokio::runtime::Handle::current(),
            );
            (
                GroupFixture::new(dir, meta, vec![control, raft1, raft2, group3]),
                leader_meta,
            )
        }

        fn write_generations(engine: &BlockVolumeEngine<OsFs>, project: u32, base: u8) {
            for i in 0..6u8 {
                engine
                    .write_at(
                        project,
                        "disk",
                        POOL,
                        1 << 20,
                        0,
                        &vec![base.wrapping_add(i); 16 * BLOCK_SIZE as usize],
                    )
                    .unwrap();
            }
        }

        fn assert_refused_without_mutation(f: &GroupFixture) {
            let before = f.distinct_ids();
            let outcome = f.sweep();
            assert_eq!(outcome.refusal.as_deref(), Some(MULTIRAFT_GC_REFUSAL));
            assert_eq!(outcome.fragments_deleted, 0);
            assert_eq!(outcome.barriers_applied, 0);
            assert_eq!(outcome.bytes_reclaimed, 0);
            assert_eq!(f.distinct_ids(), before);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn multigroup_leader_refuses_before_collection() {
            let f = multigroup("refuse-leader").await;
            let engine = f.engine();
            write_generations(&engine, 1, 10);
            write_generations(&engine, 2, 20);
            assert!(f.distinct_ids().len() > 32);
            assert_refused_without_mutation(&f);
            let writer = yadb_metakv::gc::acquire_writer(&f.meta, 1, 0, 60_000).unwrap();
            assert_refused_without_mutation(&f);
            writer.release().unwrap();
            drop(engine);
            shutdown(f).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn multigroup_follower_and_leader_refuse_before_collection() {
            let (f, leader_meta) = follower_multigroup("refuse-follower").await;
            let engine = BlockVolumeEngine::new(leader_meta.clone(), BLOCK_SIZE);
            engine.register_pool(POOL, f.pool.clone());
            write_generations(&engine, 1, 0);
            assert_refused_without_mutation(&f);
            let before = f.distinct_ids();
            let outcome = sweep(&leader_meta, &f.pools, &f.control);
            assert_eq!(outcome.refusal.as_deref(), Some(MULTIRAFT_GC_REFUSAL));
            assert_eq!(outcome.fragments_deleted, 0);
            assert_eq!(outcome.barriers_applied, 0);
            assert_eq!(outcome.bytes_reclaimed, 0);
            assert_eq!(f.distinct_ids(), before);
            drop(engine);
            drop(leader_meta);
            shutdown(f).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn manual_routes_and_periodic_scrub_refuse_without_collection() {
            let f = multigroup("refuse-routes").await;
            let engine = f.engine();
            write_generations(&engine, 1, 0);
            let before = f.live_ids();
            let meta = f.meta.clone();
            let st = crate::AppState {
                openraft: None,
                listen_addr: "127.0.0.1:0".into(),
                fs: OsFs::new(temp_dir("route-app")).unwrap(),
                meta: meta.clone(),
                raft_token: None,
                subject_secret: Some("secret".into()),
                admin_subjects: vec!["operator".into()],
                control: Arc::new(std::sync::RwLock::new(f.control.clone())),
                control_refresh: Arc::new(Mutex::new(())),
                control_freshness: Arc::new(Mutex::new(crate::ControlFreshness::default())),
                pools: Arc::new(std::sync::RwLock::new(f.pools.clone())),
                local_vdisks: Arc::new(std::sync::RwLock::new(BTreeMap::new())),
                last_scrub: Arc::new(std::sync::RwLock::new(None)),
                reader_lease: Arc::new(Mutex::new(None)),
                s3: ObjectStoreEngine::new(meta.clone(), 4096),
                db: yadb_db::DatabaseEngine::new(meta.clone(), 100),
                block: BlockVolumeEngine::new(meta, BLOCK_SIZE),
            };
            let mut headers = axum::http::HeaderMap::new();
            headers.insert("x-yadb-subject", "operator".parse().unwrap());
            headers.insert(
                "x-yadb-subject-signature",
                crate::subject_signature("secret", "operator")
                    .parse()
                    .unwrap(),
            );
            for response in [
                crate::handle_pools_gc(axum::extract::State(st.clone()), headers.clone()).await,
                crate::handle_pools_scrub(axum::extract::State(st.clone()), headers).await,
            ] {
                assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
                let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                assert!(String::from_utf8_lossy(&body).contains(MULTIRAFT_GC_REFUSAL));
            }
            let summary = st.scrub_all_pools(true);
            assert_eq!(summary.gc_refusal.as_deref(), Some(MULTIRAFT_GC_REFUSAL));
            assert_eq!(summary.fragments_deleted, 0);
            assert_eq!(f.live_ids(), before);
            drop(st);
            drop(engine);
            shutdown(f).await;
        }
        #[ignore = "Multi-Raft GC is temporarily disabled"]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_multigroup_pass_reclaims_each_groups_dead_fragments() {
            tokio::time::timeout(Duration::from_secs(30), async {
                let f = multigroup("reclaim").await;
                let project1 = f.engine();
                let project2 = f.engine();
                write_generations(&project1, 1, 10);
                write_generations(&project2, 2, 20);

                let before = f.distinct_ids();
                assert!(
                    before.iter().filter(|id| id.project_id() == 1).count() > 16,
                    "the fixture must leave superseded fragments: {before:?}"
                );
                assert!(before.iter().filter(|id| id.project_id() == 2).count() > 16);

                let outcome = f.sweep();
                assert!(outcome.refusal.is_none(), "{outcome:?}");
                assert!(outcome.groups_refused.is_empty(), "{outcome:?}");
                assert!(outcome.fragments_deleted > 0, "{outcome:?}");
                assert!(outcome.errors.is_empty(), "{outcome:?}");

                // Exactly the newest generation of each project survives.
                let after = f.distinct_ids();
                assert_eq!(
                    after.iter().filter(|id| id.project_id() == 1).count(),
                    16,
                    "{outcome:?}"
                );
                assert_eq!(
                    after.iter().filter(|id| id.project_id() == 2).count(),
                    16,
                    "{outcome:?}"
                );

                // Both groups still serve their newest bytes.
                let read1 = project1
                    .read_at(1, "disk", POOL, 1 << 20, 0, 16 * BLOCK_SIZE as usize)
                    .unwrap();
                assert_eq!(read1, vec![15u8; 16 * BLOCK_SIZE as usize]);
                let read2 = project2
                    .read_at(2, "disk", POOL, 1 << 20, 0, 16 * BLOCK_SIZE as usize)
                    .unwrap();
                assert_eq!(read2, vec![25u8; 16 * BLOCK_SIZE as usize]);

                drop(project1);
                drop(project2);
                shutdown(f).await;
            })
            .await
            .expect("multigroup sweep timed out");
        }

        #[ignore = "Multi-Raft GC is temporarily disabled"]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_follower_group_refuses_and_deletes_nothing() {
            tokio::time::timeout(Duration::from_secs(30), async {
                let (f, leader_meta) = follower_multigroup("follower").await;
                let leader = BlockVolumeEngine::new(leader_meta.clone(), BLOCK_SIZE);
                leader.register_pool(POOL, f.pool.clone());
                write_generations(&leader, 1, 0);

                let before = f.distinct_ids();
                assert!(before.iter().filter(|id| id.project_id() == 1).count() > 16);

                let outcome = f.sweep();
                assert_eq!(outcome.fragments_deleted, 0, "{outcome:?}");
                assert_eq!(outcome.groups_refused.len(), 1, "{outcome:?}");
                let refusal = &outcome.groups_refused[0];
                assert!(refusal.starts_with("group 2 refused:"), "{refusal}");
                assert!(refusal.contains("is not the leader"), "{refusal}");
                assert_eq!(
                    f.distinct_ids(),
                    before,
                    "a follower's pass must delete nothing"
                );

                // The same fragments are provably dead: group 2's leader
                // reclaims them, so the refusal (not the data) kept them alive.
                let leader_outcome = sweep(&leader_meta, &f.pools, &f.control);
                assert!(leader_outcome.refusal.is_none(), "{leader_outcome:?}");
                assert!(leader_outcome.fragments_deleted > 0, "{leader_outcome:?}");
                let read = leader
                    .read_at(1, "disk", POOL, 1 << 20, 0, 16 * BLOCK_SIZE as usize)
                    .unwrap();
                assert_eq!(read, vec![5u8; 16 * BLOCK_SIZE as usize]);

                drop(leader);
                drop(leader_meta);
                shutdown(f).await;
            })
            .await
            .expect("follower refusal test timed out");
        }

        #[ignore = "Multi-Raft GC is temporarily disabled"]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_group_writer_lease_protects_an_unpublished_fragment() {
            tokio::time::timeout(Duration::from_secs(30), async {
                let f = multigroup("writer-lease").await;
                let project1 = f.engine();
                let project2 = f.engine();
                write_generations(&project1, 1, 0);
                write_generations(&project2, 2, 10);

                // A fragment appended by a writer that has not published the
                // manifest naming it. Nothing in the mark set refers to it.
                let orphan = BlobId::new(1, 0x00FF_FFFF, 1, 0, yadb_block::BLOCK_CHANNEL, 0);
                f.pool
                    .put_blob(orphan, &vec![0xABu8; BLOCK_SIZE as usize])
                    .unwrap();
                let before = f.distinct_ids();
                assert!(before.contains(&orphan));

                // The lease is published in group 2, so only group 2 refuses;
                // group 3 still reclaims its own dead fragments.
                let writer = yadb_metakv::gc::acquire_writer(&f.meta, 1, 0, 60_000).unwrap();
                let outcome = f.sweep();
                assert_eq!(outcome.groups_refused.len(), 1, "{outcome:?}");
                let refusal = &outcome.groups_refused[0];
                assert!(refusal.starts_with("group 2 refused:"), "{refusal}");
                assert!(refusal.contains("live lease"), "{refusal}");

                let held = f.distinct_ids();
                assert!(
                    held.contains(&orphan),
                    "an unexpired writer lease must protect its fragment: {outcome:?}"
                );
                assert_eq!(
                    held.iter().filter(|id| id.project_id() == 1).count(),
                    before.iter().filter(|id| id.project_id() == 1).count(),
                    "group 2 must delete nothing while its writer lease is live"
                );
                assert_eq!(
                    held.iter().filter(|id| id.project_id() == 2).count(),
                    16,
                    "group 3 is not blocked by group 2's lease: {outcome:?}"
                );

                writer.release().unwrap();
                let outcome = f.sweep();
                assert!(outcome.refusal.is_none(), "{outcome:?}");
                assert!(outcome.fragments_deleted > 0, "{outcome:?}");
                let after = f.distinct_ids();
                assert!(
                    !after.contains(&orphan),
                    "without the lease the orphan is provably dead"
                );
                assert_eq!(after.iter().filter(|id| id.project_id() == 1).count(), 16);

                drop(project1);
                drop(project2);
                shutdown(f).await;
            })
            .await
            .expect("writer-lease test timed out");
        }

        #[ignore = "Multi-Raft GC is temporarily disabled"]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_prepared_s3_intent_protects_its_chunks() {
            tokio::time::timeout(Duration::from_secs(30), async {
                let f = multigroup("s3-intent").await;
                let s3 = f.s3();
                let payload = vec![0x5Au8; 32 * 1024];
                let failed = s3.put_object_with(
                    1,
                    "store",
                    POOL,
                    "bucket",
                    "inflight",
                    "text/plain",
                    &payload,
                    |_| Err(S3Error::ConcurrentModification),
                );
                assert!(
                    matches!(failed, Err(S3Error::ConcurrentModification)),
                    "{failed:?}"
                );

                let intents = f.meta.scan_prefix_or_panic("/s3-intent/1/");
                assert_eq!(intents.len(), 1, "{intents:?}");
                let (intent_key, intent_value) = &intents[0];
                let chunks: BTreeSet<BlobId> = yadb_s3::intent_chunk_ids(&intent_value.value)
                    .unwrap()
                    .into_iter()
                    .map(|(_, id)| id.with_part(0))
                    .collect();
                assert!(!chunks.is_empty(), "{intents:?}");
                let present = f.distinct_ids();
                assert!(
                    chunks.iter().all(|id| present.contains(id)),
                    "the chunks must be on disk: {chunks:?}"
                );

                // The prepared intent is part of the mark, so the pass succeeds
                // and keeps the chunks it names.
                let outcome = f.sweep();
                assert!(outcome.refusal.is_none(), "{outcome:?}");
                assert_eq!(outcome.fragments_deleted, 0, "{outcome:?}");
                let after = f.distinct_ids();
                assert!(
                    chunks.iter().all(|id| after.contains(id)),
                    "a prepared intent must keep its chunks: {outcome:?}"
                );

                // With the intent gone the same chunks are provably dead.
                f.meta
                    .execute(KvCommand::Delete {
                        key: intent_key.clone(),
                    })
                    .unwrap();
                let outcome = f.sweep();
                assert!(outcome.refusal.is_none(), "{outcome:?}");
                assert!(outcome.fragments_deleted > 0, "{outcome:?}");
                let after = f.distinct_ids();
                assert!(
                    chunks.iter().all(|id| !after.contains(id)),
                    "without the intent the chunks are dead: {outcome:?}"
                );

                drop(s3);
                shutdown(f).await;
            })
            .await
            .expect("s3-intent test timed out");
        }
    }
}
