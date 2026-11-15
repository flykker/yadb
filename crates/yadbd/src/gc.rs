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
//! 2. **Fence.** Refuse on OpenRaft (no single atomic cross-group view). Read
//!    every live reader/writer lease and reap expired ones; any live lease means
//!    a read or write is in flight, so the pass refuses and deletes nothing.
//! 3. **Collector lease.** Publish a durable collector record so a second pass
//!    cannot interleave its mark and sweep phases with this one, and so no new
//!    reader or writer can be admitted until the pass is done. The record is
//!    renewed before each deletion batch: a pass that stalls past its lease stops
//!    deleting instead of continuing without a fence, and its release is
//!    ownership-guarded so it can never delete a successor's record.
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
    /// Set when the pass deleted nothing. Always an explicit reason.
    #[serde(rename = "gc_refusal")]
    pub refusal: Option<String>,
    /// Pools that were mounted but not swept, with the reason.
    #[serde(rename = "gc_pools_refused")]
    pub pools_refused: Vec<String>,
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

pub fn sweep(
    meta: &LocalMetaKv<OsFs>,
    pools: &BTreeMap<String, std::sync::Arc<PoolStriper<OsFs>>>,
    control: &ControlPlaneState,
) -> SweepOutcome {
    if meta.is_openraft() {
        // A Multi-Raft deployment has one metadata group per project. There is no
        // single atomic snapshot that covers every group's manifests, so a
        // fragment that is unreferenced in the groups this node can read may be
        // referenced by one it cannot. Refuse instead of guessing.
        return SweepOutcome::refuse(
            "garbage collection disabled for Multi-Raft: no single atomic cross-group metadata snapshot",
        );
    }

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
        Some(lease) => {
            return SweepOutcome::refuse(format!(
                "garbage collection refused: another pass holds the collector lease until {}",
                lease.expires_at_ms
            ))
        }
        None => meta.get(gc::COLLECTOR_KEY).map_or(0, |kv| kv.mod_revision),
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
    let swept_up_to = meta.revision();
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
        sweep_pool(striper, pool_name, &dead, live, &mut outcome);
    }

    // Step 7: compact, so the bytes the barriers and tombstones freed are actually
    // returned to the filesystem. Refused pools are left alone: their state is
    // unknown, and a pass that refuses a pool must not touch it at all.
    //
    // Compaction only reclaims space the barriers already fenced, so it is safe
    // even if the lease is lost here; it is renewed anyway so a long compaction
    // does not hand the next pass a record that expires mid-way.
    let _ = collector.renew(COLLECTOR_LEASE_MS);
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

    if let Err(error) = collector.release() {
        // The pass already deleted what it proved dead; the stale collector lease
        // only blocks the next pass until it expires. Report it.
        outcome
            .errors
            .push(format!("collector lease release failed: {error}"));
    }
    outcome
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
    outcome: &mut SweepOutcome,
) {
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
        let mut barrier_ok = true;
        for slot in striper.slots() {
            match slot.apply_barrier(project, owner, channel, up_to, false) {
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

    for id in exact {
        // Deleting every part is what makes the tombstone meaningful: a fragment
        // that survives on one slot would be readable again after reconstruction.
        match striper.delete(id) {
            Ok(true) => outcome.fragments_deleted += striper.scheme().total_parts(),
            Ok(false) => {}
            Err(error) => outcome.errors.push(format!(
                "pool `{pool_name}`: tombstone for {id} failed: {error}"
            )),
        }
    }
}

/// One snapshot of every manifest that can name a fragment.
fn mark(meta: &LocalMetaKv<OsFs>, control: &ControlPlaneState) -> Result<MarkSet, String> {
    let prefixes: Vec<String> = LIVE_PREFIXES.iter().map(|p| (*p).to_string()).collect();
    let snapshot = meta
        .try_snapshot(&[], &prefixes)
        .map_err(|error: MetaError| error.to_string())?;

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
            slot.apply_barrier(1, owner, channel, newest.generation() - 1, false)
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
        let lease = yadb_metakv::gc::acquire_reader(&f.meta, 1, f.meta.revision(), 30_000).unwrap();
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
        let before_sweep = f.meta.revision();
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
}
