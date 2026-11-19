use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use serde::{Deserialize, Serialize};
use yadb_io::{StorageFs, VirtualFile};

use crate::{BlobError, BlobId};

const VOLUME_MAGIC: &[u8; 8] = b"YADBVOL1";
/// The original record header: 33 bytes, no epoch. Still read, so a volume
/// written before target epochs existed keeps opening; every such record belongs
/// to epoch 0, which is the epoch a fresh volume starts at.
const RECORD_MAGIC_V1: u32 = 0x5942_5243; // "YBRC"
/// The current record header: 41 bytes, carrying the epoch the record was
/// written under. The first 29 bytes mean exactly what they meant in V1, so the
/// two layouts differ only in what follows the data CRC.
const RECORD_MAGIC_V2: u32 = 0x5942_5244; // "YBRD"
const REC_KIND_PUT: u8 = 1;
const REC_KIND_BARRIER: u8 = 2;
const REC_KIND_TOMBSTONE: u8 = 3;
/// Records the durable target epoch. Appended, never overwritten in place: the
/// value recovery derives is the maximum over every record in the volume, so a
/// torn append can only fail to raise the epoch, never lower it.
const REC_KIND_EPOCH: u8 = 4;
/// Records a reader pin: its id and its deadline in the payload, the epoch it
/// names in the header. Appended and synced like every other record, never
/// rewritten in place, so replay order - not a mutable field - decides whether
/// a pin is live.
const REC_KIND_PIN: u8 = 5;
/// Releases the pin named in its payload. A release is a record of its own so a
/// pin is never rewritten in place: recovery sees PIN then UNPIN and the later
/// record wins.
const REC_KIND_UNPIN: u8 = 6;
/// `pin_id: u64` + `expires_at_ms: u64`, little-endian, CRC32 in the header.
const PIN_PAYLOAD_LEN: usize = 16;
/// `pin_id: u64`, little-endian, CRC32 in the header.
const UNPIN_PAYLOAD_LEN: usize = 8;
/// The longest lifetime a pin may name.
///
/// A pin is held across one fragment fetch, so its bound is minutes rather than
/// hours. Without a bound one buggy reader could hold collection off
/// indefinitely; a TTL above this is refused rather than clamped, because
/// silently shortening a lease the reader asked for would expire a pin under a
/// read the reader believes it fenced.
pub const MAX_PIN_TTL_MS: u64 = 300_000;
const RECORD_HEADER_LEN_V1: usize = 33;
pub(crate) const RECORD_HEADER_LEN: usize = 41;
/// `BlobId` field of an epoch record. An epoch record names no fragment, so the
/// id is unused; it is written as the all-zero id so the record is obviously not
/// a fragment.
fn epoch_record_id() -> BlobId {
    BlobId::new(0, 0, 0, 0, 0, 0)
}

/// Identifies one reader pin.
///
/// Ids are drawn from the volume's own durable log (`max(id) + 1` over every
/// PIN and UNPIN record), never from a counter that restarts with the process.
/// An id is therefore not handed out again while any record naming it survives
/// on disk, including across reopen, so a late `unpin` of an old id cannot
/// release a newer pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PinId(u64);

impl PinId {
    /// The numeric value, for the wire format the RPC will carry.
    pub fn get(self) -> u64 {
        self.0
    }

    /// Rebuilds an id from its wire value. An id this volume never issued is
    /// simply unknown to [`VolumeStore::unpin`], which answers `Ok(false)`.
    pub fn from_wire(value: u64) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for PinId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pin#{}", self.0)
    }
}

/// What the volume remembers about one pin. Liveness is `now < expires_at_ms`
/// and is decided at the point of use, never by a field that is flipped.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PinEntry {
    /// The epoch the pin names. A pin is only ever taken on the epoch that was
    /// current when it was taken.
    pub(crate) epoch: u64,
    pub(crate) expires_at_ms: u64,
}

/// One PIN or UNPIN record, in log order. Replayed by [`rebuild_pins`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum PinRecord {
    Pin {
        pin_id: PinId,
        epoch: u64,
        expires_at_ms: u64,
    },
    Unpin {
        pin_id: PinId,
    },
}

/// How [`VolumeStore::delete_inner`] is fenced. Three distinct rules, kept in one
/// place so the difference between them is visible rather than implied:
/// unfenced, exact-epoch, and older-and-unpinned.
#[derive(Debug, Clone, Copy)]
enum DeleteFence {
    Unfenced,
    Exact(u64),
    Collectable(u64),
}

/// The target's answer to [`VolumeStore::pin`]: the id the reader must present
/// to [`VolumeStore::unpin`], and the deadline the target recorded for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinGrant {
    pub pin_id: PinId,
    pub epoch: u64,
    pub expires_at_ms: u64,
}

/// One unexpired pin, as [`VolumeStore::live_pins`] reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinView {
    pub pin_id: PinId,
    pub epoch: u64,
    pub expires_at_ms: u64,
}

fn encode_pin_payload(pin_id: PinId, expires_at_ms: u64) -> [u8; PIN_PAYLOAD_LEN] {
    let mut payload = [0u8; PIN_PAYLOAD_LEN];
    payload[0..8].copy_from_slice(&pin_id.0.to_le_bytes());
    payload[8..16].copy_from_slice(&expires_at_ms.to_le_bytes());
    payload
}

fn encode_unpin_payload(pin_id: PinId) -> [u8; UNPIN_PAYLOAD_LEN] {
    pin_id.0.to_le_bytes()
}

/// A mostly-garbage active segment is sealed even when small, because rolling it
/// is the only way `compact_sealed_segments` can reclaim its dead records. A
/// mostly-live one is only sealed once it is large enough that a roll would
/// happen soon anyway, so collection does not scatter the volume into many
/// tiny segments.
const MIN_SEAL_GARBAGE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct VolumeConfig {
    /// Roll over to a new append-only segment file once active segment exceeds this size.
    pub max_segment_bytes: u64,
    /// Maximum allowed single blob size (default 10 MiB, matching YDB LogoBlob limit).
    pub max_blob_bytes: usize,
    /// `fdatasync` on every put (can be disabled only for bulk load).
    pub sync_on_put: bool,
}

impl Default for VolumeConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 64 * 1024 * 1024, // 64 MiB default segment size
            max_blob_bytes: 10 * 1024 * 1024,    // 10 MiB max blob
            sync_on_put: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    seg_id: u32,
    data_offset: u64,
    data_len: u32,
    data_crc: u32,
    /// Target epoch this fragment was written under, recovered from its PUT
    /// record. A fenced delete compares it with the epoch the delete names.
    epoch: u64,
}

struct ActiveSegment<F: VirtualFile> {
    seg_id: u32,
    file: F,
    write_pos: u64,
    failed: bool,
}

impl<F: VirtualFile> ActiveSegment<F> {
    fn ensure_healthy(&self) -> Result<(), BlobError> {
        if self.failed {
            return Err(BlobError::StorageUnavailable);
        }
        Ok(())
    }

    fn sync_data(&mut self) -> Result<(), BlobError> {
        if let Err(error) = self.file.sync_data() {
            // The record may already be durable. Never overwrite this offset
            // or acknowledge an idempotent retry from the old in-memory view.
            self.failed = true;
            return Err(error.into());
        }
        Ok(())
    }
}

/// Appends one header-plus-payload record and syncs it, rolling back a torn tail
/// so the next append stays aligned.
///
/// The sync is unconditional: a pin is fencing state, and deletion fencing has
/// to survive restart even on a bulk-load volume. A failed sync marks the
/// segment failed instead of retrying an append whose outcome is unknown.
fn append_record<F: VirtualFile>(
    writer: &mut ActiveSegment<F>,
    hdr: &[u8],
    payload: &[u8],
) -> Result<(), BlobError> {
    let pos = writer.write_pos;
    let mut frame = Vec::with_capacity(hdr.len() + payload.len());
    frame.extend_from_slice(hdr);
    frame.extend_from_slice(payload);
    if let Err(error) = writer.file.write_at(pos, &frame) {
        if writer.file.set_len(pos).is_err() {
            writer.failed = true;
        }
        return Err(error.into());
    }
    writer.sync_data()?;
    writer.write_pos = pos + frame.len() as u64;
    Ok(())
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct VolumeStats {
    pub live_blobs: usize,
    pub live_bytes: u64,
    pub segment_count: usize,
    pub total_disk_bytes: u64,
}

/// Append-only segment storage engine running on standard filesystems (`OsFs` or `SimFs`).
///
/// Packs immutable blobs into segment files (`vol_00000001.dat`), verifies CRC32 checksums on read,
/// supports sub-range reads (`get_slice`), truncates torn crash tails on recovery, and executes
/// YDB-style generation barrier garbage collection + background segment compaction.
///
/// It also keeps a durable, monotonic **target epoch** (see [`VolumeStore::advance_epoch`]) and
/// stamps every fragment with the epoch it was written under, so a delete can be refused when the
/// fragment belongs to a different epoch than the one the collector names
/// ([`VolumeStore::delete_fenced`]).
///
/// On top of the epoch it keeps a durable **pin table** (see [`VolumeStore::pin`]): readers pin the
/// epoch they read, an epoch advance is refused while a pin on it is unexpired, and
/// [`VolumeStore::delete_collectable`] refuses every fragment older than the collector while *any*
/// pin is live. The last rule is deliberately broader than "the fragment's own epoch", because a
/// reader can only pin the current epoch and may fetch a fragment written under an older one.
pub struct VolumeStore<FS: StorageFs> {
    fs: FS,
    dir: String,
    cfg: VolumeConfig,
    index: RwLock<BTreeMap<BlobId, IndexEntry>>,
    barriers: RwLock<BTreeMap<(u32, u32, u8), u32>>,
    tombstones: RwLock<BTreeSet<BlobId>>,
    segments: RwLock<BTreeMap<u32, FS::File>>,
    writer: Mutex<ActiveSegment<FS::File>>,
    /// Current durable target epoch. Only [`VolumeStore::advance_epoch`] raises
    /// it, and only after the record that carries the new value is synced.
    epoch: AtomicU64,
    /// The live pin set, rebuilt from the log on open and extended by
    /// [`VolumeStore::pin`]. Holds expired pins too: liveness is decided by
    /// comparing `expires_at_ms` with the clock, never by the entry's presence,
    /// so a stale entry can only ever be forgotten, not believed.
    pins: RwLock<BTreeMap<PinId, PinEntry>>,
    /// The next pin id. Seeded from the log, so it survives reopen.
    next_pin_id: AtomicU64,
}

impl<FS: StorageFs> VolumeStore<FS> {
    pub fn open(fs: FS, dir: impl Into<String>, cfg: VolumeConfig) -> Result<Self, BlobError> {
        let dir = dir.into();
        fs.create_dir_all(&dir)?;

        let mut index = BTreeMap::new();
        let mut barriers: BTreeMap<(u32, u32, u8), u32> = BTreeMap::new();
        let mut tombstones: BTreeSet<BlobId> = BTreeSet::new();
        let mut segments = BTreeMap::new();
        let mut max_seg_id = 0u32;
        let mut segment_ids: Vec<u32> = Vec::new();
        // The epoch is derived from what is on disk, never invented. Recovery
        // only ever raises this value: it is the maximum over every record, and
        // opening a volume is not an epoch advance.
        let mut epoch = 0u64;

        for name in fs.list_files(&dir)? {
            // Clean up leftover temporary compaction files after crash
            if name.ends_with(".compact") {
                let _ = fs.remove_file(&join_path(&dir, &name));
                continue;
            }
            if let Some(seg_id) = parse_segment_name(&name) {
                segment_ids.push(seg_id);
            }
        }
        // Replay order is part of recovery: a pin and the unpin that releases it
        // can live in different segments and the later record must win, so
        // segments are replayed by ascending id. `list_files` order is not
        // guaranteed by the operating system.
        segment_ids.sort_unstable();
        segment_ids.dedup();

        let mut pin_records: Vec<PinRecord> = Vec::new();
        for seg_id in segment_ids {
            max_seg_id = max_seg_id.max(seg_id);
            let path = segment_path(&dir, seg_id);
            let file = fs.open_rw(&path, false)?;
            recover_segment(
                &file,
                seg_id,
                &mut index,
                &mut barriers,
                &mut tombstones,
                &mut epoch,
                &mut pin_records,
            )?;
            // Recovery can see unsynced page-cache records after a process
            // restart. Sync before serving idempotent PUT/DELETE acks or
            // relying on recovered fences, even on bulk-load volumes.
            file.sync_data()?;
            segments.insert(seg_id, file);
        }

        // Rebuilt from the records, never invented: a live pin is one whose PIN
        // record was not followed by its UNPIN. Dropping an expired pin here is
        // the only way a pin may disappear without a record saying so, and it is
        // decided by the clock, not by a default.
        let (pins, next_pin_id) = rebuild_pins(pin_records, &target_clock())?;

        let active_id = if max_seg_id == 0 { 1 } else { max_seg_id };
        let active_file = if segments.contains_key(&active_id) {
            let path = segment_path(&dir, active_id);
            fs.open_rw(&path, false)?
        } else {
            let f = init_new_segment(&fs, &dir, active_id)?;
            let read_handle = fs.open_rw(&segment_path(&dir, active_id), false)?;
            segments.insert(active_id, read_handle);
            f
        };
        let write_pos = active_file.len()?;
        fs.sync_dir(&dir)?;

        Ok(Self {
            fs,
            dir,
            cfg,
            index: RwLock::new(index),
            barriers: RwLock::new(barriers),
            tombstones: RwLock::new(tombstones),
            segments: RwLock::new(segments),
            writer: Mutex::new(ActiveSegment {
                seg_id: active_id,
                file: active_file,
                write_pos,
                failed: false,
            }),
            epoch: AtomicU64::new(epoch),
            pins: RwLock::new(pins),
            next_pin_id: AtomicU64::new(next_pin_id),
        })
    }

    /// The durable target epoch this volume is currently at.
    ///
    /// A fragment written now is stamped with this value; a delete that names a
    /// different one is refused (see [`Self::delete_fenced`]).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// The epoch the fragment `id` was written under, or `None` if the volume
    /// does not hold it. The value is recovered from the fragment's own record,
    /// so it is stable across reopen and compaction.
    pub fn fragment_epoch(&self, id: BlobId) -> Option<u64> {
        self.index.read().unwrap().get(&id).map(|entry| entry.epoch)
    }

    /// Takes a reader pin on `epoch` for `ttl_ms`, and returns the id the reader
    /// must present to [`Self::unpin`] together with the deadline the target
    /// recorded.
    ///
    /// A pin is durable, not in-memory: the record is appended and synced before
    /// this returns. A target that forgot a pin across a restart would let a
    /// collector advance past a reader that is still holding bytes - the one
    /// direction that must not happen - while a pin that outlives its reader
    /// only blocks collection until its TTL runs out.
    ///
    /// `epoch` must be the target's current epoch. An older one names an epoch
    /// the collector may already have advanced past, so the pin would prove
    /// nothing; a newer one does not exist yet. Both are refused with
    /// [`BlobError::PinEpochNotCurrent`], which names both epochs.
    ///
    /// `ttl_ms` of zero is refused, and a lifetime above [`MAX_PIN_TTL_MS`] is
    /// refused rather than clamped: a pin that outlives the read it protects
    /// blocks collection for nothing, and one that was silently shortened would
    /// expire underneath a read the reader believes is fenced.
    pub fn pin(&self, epoch: u64, ttl_ms: u64) -> Result<PinGrant, BlobError> {
        if ttl_ms == 0 {
            return Err(BlobError::PinTtlZero);
        }
        if ttl_ms > MAX_PIN_TTL_MS {
            return Err(BlobError::PinTtlTooLong {
                requested_ms: ttl_ms,
                max_ms: MAX_PIN_TTL_MS,
            });
        }
        let mut writer = self.writer.lock().unwrap();
        writer.ensure_healthy()?;
        // Checked under the append lock: an advance takes the same lock, so the
        // epoch this pin names cannot move between the check and the record.
        let current = self.epoch.load(Ordering::SeqCst);
        if epoch != current {
            return Err(BlobError::PinEpochNotCurrent {
                requested_epoch: epoch,
                actual_epoch: current,
            });
        }
        // The deadline is the target's own clock plus the TTL. An unreadable
        // clock refuses here too: a pin with no provable deadline is not a pin.
        let now_ms = target_clock().map_err(|reason| BlobError::PinClockUnreadable { reason })?;
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        let pin_id = self.allocate_pin_id()?;
        let payload = encode_pin_payload(pin_id, expires_at_ms);
        let hdr = encode_record_header(
            REC_KIND_PIN,
            epoch_record_id(),
            PIN_PAYLOAD_LEN as u32,
            crc32fast::hash(&payload),
            epoch,
        );
        append_record(&mut writer, &hdr, &payload)?;
        // Published only after the record is durable: an id the caller was told
        // about is always one recovery will find.
        self.pins.write().unwrap().insert(
            pin_id,
            PinEntry {
                epoch,
                expires_at_ms,
            },
        );
        Ok(PinGrant {
            pin_id,
            epoch,
            expires_at_ms,
        })
    }

    /// Releases `pin_id`.
    ///
    /// Idempotent and one-directional: `Ok(true)` means a live pin was removed,
    /// `Ok(false)` means the id was unknown or had already expired. It never
    /// extends a deadline, never re-creates a pin and never touches the epoch, so
    /// releasing twice - or after the TTL ran out - cannot make a fragment
    /// collectable that was not already.
    pub fn unpin(&self, pin_id: PinId) -> Result<bool, BlobError> {
        let mut writer = self.writer.lock().unwrap();
        writer.ensure_healthy()?;
        let Some(entry) = self.pins.read().unwrap().get(&pin_id).copied() else {
            return Ok(false);
        };
        // An expired pin blocks nothing, so releasing it is a no-op rather than
        // a record - and reporting `true` would claim a live pin was removed.
        if !pin_is_live(entry.expires_at_ms, &target_clock())? {
            return Ok(false);
        }
        let payload = encode_unpin_payload(pin_id);
        let hdr = encode_record_header(
            REC_KIND_UNPIN,
            epoch_record_id(),
            UNPIN_PAYLOAD_LEN as u32,
            crc32fast::hash(&payload),
            0,
        );
        append_record(&mut writer, &hdr, &payload)?;
        // Removed only after the release is durable. A failed append leaves the
        // pin in place, which blocks collection rather than admitting it.
        self.pins.write().unwrap().remove(&pin_id);
        Ok(true)
    }

    /// Every pin that is unexpired right now: the set the collector's
    /// "re-read the pin set" step has to consult before advancing an epoch.
    ///
    /// An unreadable clock refuses with [`BlobError::PinClockUnreadable`] instead
    /// of being read as zero, which would make every pin look expired.
    pub fn live_pins(&self) -> Result<Vec<PinView>, BlobError> {
        self.live_pins_on(None)
    }

    /// Unexpired pins, optionally restricted to one epoch.
    fn live_pins_on(&self, epoch: Option<u64>) -> Result<Vec<PinView>, BlobError> {
        let pins = self.pins.read().unwrap();
        if pins.is_empty() {
            // No pins: nothing to compare, so no clock is needed and a broken
            // one cannot block an otherwise empty table.
            return Ok(Vec::new());
        }
        let now = target_clock();
        let mut live = Vec::new();
        for (&pin_id, entry) in pins.iter() {
            if epoch.is_some_and(|wanted| wanted != entry.epoch) {
                continue;
            }
            if pin_is_live(entry.expires_at_ms, &now)? {
                live.push(PinView {
                    pin_id,
                    epoch: entry.epoch,
                    expires_at_ms: entry.expires_at_ms,
                });
            }
        }
        Ok(live)
    }

    fn allocate_pin_id(&self) -> Result<PinId, BlobError> {
        // A compare-exchange loop rather than `fetch_add`: an id that wrapped
        // would be one a surviving record may already name, and reissuing an id
        // would let a stale unpin release a newer pin.
        let mut next = self.next_pin_id.load(Ordering::SeqCst);
        loop {
            let following = next.checked_add(1).ok_or(BlobError::PinIdsExhausted)?;
            match self.next_pin_id.compare_exchange(
                next,
                following,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(PinId(next)),
                Err(observed) => next = observed,
            }
        }
    }

    /// The collector's epoch step: raises the durable epoch by one, from exactly
    /// `expected_epoch`, and returns the new value.
    ///
    /// This is the *only* way the epoch moves. It is never raised by `open`,
    /// by recovery, by a put, a delete, a barrier or a compaction, because an
    /// epoch advance is the one event that makes every older collector's fence
    /// stale: doing it implicitly would revoke a pass that is still entitled to
    /// delete.
    ///
    /// `expected_epoch` must be the value the caller read and fenced its pass
    /// with. A target that is no longer at that epoch refuses with
    /// [`BlobError::EpochMismatch`] instead of advancing: a superseded collector
    /// must not be able to move the epoch past a newer one.
    ///
    /// An unexpired pin refuses the advance as well, with
    /// [`BlobError::EpochPinned`], which names the epoch and how many pins
    /// blocked it. A reader pins the epoch it is about to read, so advancing past
    /// it is exactly the event that would let a pass delete bytes the reader still
    /// holds. Both refusals are "stop this pass": neither is retryable, and
    /// neither appends a record.
    ///
    /// Durability: the new epoch is appended as a record and synced before the
    /// call returns, and before the in-memory value is published. A failed sync
    /// leaves the store refusing further mutations ([`BlobError::StorageUnavailable`])
    /// rather than retrying an append whose outcome is unknown; the epoch the
    /// caller was told is therefore always one that survived. If the sync failed
    /// *after* persisting, recovery may find the higher epoch even though this
    /// call reported an error - the epoch can only over-advance, never go
    /// backwards, and a collector that was refused must stop rather than retry.
    pub fn advance_epoch(&self, expected_epoch: u64) -> Result<u64, BlobError> {
        let mut writer = self.writer.lock().unwrap();
        writer.ensure_healthy()?;
        let current = self.epoch.load(Ordering::SeqCst);
        if current != expected_epoch {
            return Err(BlobError::EpochMismatch {
                expected_epoch,
                actual_epoch: current,
            });
        }
        // Checked before anything is appended, and under the same append lock a
        // pin takes, so "no pins" and "epoch advanced" cannot interleave with a
        // pin landing between them.
        let live_pins = self.live_pins_on(None)?.len();
        if live_pins > 0 {
            return Err(BlobError::EpochPinned {
                epoch: current,
                live_pins,
            });
        }
        let next = current.checked_add(1).ok_or(BlobError::EpochExhausted)?;
        let hdr = encode_record_header(REC_KIND_EPOCH, epoch_record_id(), 0, 0, next);
        let pos = writer.write_pos;
        if let Err(error) = writer.file.write_at(pos, &hdr) {
            // Same rule as a PUT: roll back a torn tail so the next append stays
            // aligned. A rollback that fails marks the volume unhealthy.
            if writer.file.set_len(pos).is_err() {
                writer.failed = true;
            }
            return Err(error.into());
        }
        // Deletion fencing must survive restart even for bulk-load volumes, and
        // the epoch is fencing state, so this sync is unconditional.
        writer.sync_data()?;
        writer.write_pos += RECORD_HEADER_LEN as u64;
        // Published only now: a value that is not proven durable must not be
        // visible to a fragment write or a fenced delete.
        self.epoch.store(next, Ordering::SeqCst);
        Ok(next)
    }

    /// Writes an immutable blob. Idempotent if the same id and bytes already exist;
    /// rejects conflicting writes to an existing `BlobId` or writes older than a GC barrier.
    ///
    /// The record carries the current target epoch ([`Self::epoch`]) durably, so a
    /// later fenced delete can tell which epoch this fragment belongs to. A
    /// rewrite after bit-rot is a fresh write and takes the epoch current at that
    /// moment, which can only make an older delete fence refuse, never pass.
    pub fn put(&self, id: BlobId, data: &[u8]) -> Result<(), BlobError> {
        if data.len() > self.cfg.max_blob_bytes {
            return Err(BlobError::BlobTooLarge {
                size: data.len(),
                max: self.cfg.max_blob_bytes,
            });
        }

        // Serialize validation, append and index publication with DELETE,
        // barriers and compaction, not just the physical append.
        let mut writer = self.writer.lock().unwrap();
        writer.ensure_healthy()?;
        if self.tombstones.read().unwrap().contains(&id) {
            return Err(BlobError::Tombstoned(id));
        }

        // Enforce generation fencing barrier
        if let Some(&barrier_gen) =
            self.barriers
                .read()
                .unwrap()
                .get(&(id.project_id(), id.owner_id(), id.channel()))
        {
            if id.generation() <= barrier_gen {
                return Err(BlobError::FencedByBarrier { id, barrier_gen });
            }
        }

        let data_crc = crc32fast::hash(data);

        // Idempotency check for immutable blobs (also verifies on-disk CRC32 so Scrubber
        // can heal silent bit-rot by appending a fresh verified copy when on-disk bytes corrupted!)
        let existing = self.index.read().unwrap().get(&id).copied();
        if let Some(existing) = existing {
            if existing.data_len == data.len() as u32 && existing.data_crc == data_crc {
                if let Ok(bytes) = self.get(id) {
                    return if bytes == data {
                        Ok(())
                    } else {
                        Err(BlobError::ImmutableConflict(id))
                    };
                }
            } else {
                return Err(BlobError::ImmutableConflict(id));
            }
        }

        let rec_total = (RECORD_HEADER_LEN + data.len()) as u64;
        if writer.write_pos > VOLUME_MAGIC.len() as u64
            && writer.write_pos + rec_total > self.cfg.max_segment_bytes
        {
            let next_id = writer.seg_id + 1;
            let new_file = match init_new_segment(&self.fs, &self.dir, next_id) {
                Ok(file) => file,
                Err(error) => {
                    writer.failed = true;
                    return Err(error);
                }
            };
            let read_handle = match self.fs.open_rw(&segment_path(&self.dir, next_id), false) {
                Ok(file) => file,
                Err(error) => {
                    writer.failed = true;
                    return Err(error.into());
                }
            };
            self.segments.write().unwrap().insert(next_id, read_handle);
            *writer = ActiveSegment {
                seg_id: next_id,
                file: new_file,
                write_pos: VOLUME_MAGIC.len() as u64,
                failed: false,
            };
        }

        // Read once, under the append lock: an epoch advance also takes this
        // lock, so the value stamped in the record and in the index entry below
        // are the same one.
        let epoch = self.epoch.load(Ordering::SeqCst);
        let hdr = encode_record_header(REC_KIND_PUT, id, data.len() as u32, data_crc, epoch);
        let mut frame = Vec::with_capacity(RECORD_HEADER_LEN + data.len());
        frame.extend_from_slice(&hdr);
        frame.extend_from_slice(data);

        let start_pos = writer.write_pos;
        if let Err(e) = writer.file.write_at(start_pos, &frame) {
            // Roll back partial/torn tail immediately so next write stays aligned
            if writer.file.set_len(start_pos).is_err() {
                writer.failed = true;
            }
            return Err(BlobError::Io(e));
        }
        if self.cfg.sync_on_put {
            writer.sync_data()?;
        }

        let data_offset = start_pos + RECORD_HEADER_LEN as u64;
        let seg_id = writer.seg_id;
        writer.write_pos = start_pos + frame.len() as u64;
        // Publish the in-memory index while still holding the append lock.
        // Exact tombstone deletion uses the same lock; releasing it first
        // would allow DELETE to be followed by a stale PUT index insertion.
        self.index.write().unwrap().insert(
            id,
            IndexEntry {
                seg_id,
                data_offset,
                data_len: data.len() as u32,
                data_crc,
                epoch,
            },
        );
        drop(writer);
        Ok(())
    }

    /// Durably removes one immutable blob and fences the exact id forever.
    /// This is intentionally narrower than a generation barrier and is used
    /// by the S3 orphan collector after metadata reachability has been proven.
    ///
    /// **Unfenced.** This does not consult the target epoch: it unlinks a
    /// fragment regardless of the epoch it was written under. It is kept for
    /// callers whose safety argument does not involve a collector epoch. A
    /// collector deleting under a fence epoch must use [`Self::delete_fenced`]
    /// (exactly this epoch) or [`Self::delete_collectable`] (older and unpinned).
    pub fn delete(&self, id: BlobId) -> Result<bool, BlobError> {
        self.delete_inner(id, DeleteFence::Unfenced)
    }

    /// Durably removes one immutable blob, but only if the volume holds it
    /// under `expected_epoch`.
    ///
    /// The recorded epoch is the one the fragment's own PUT record carries
    /// ([`Self::fragment_epoch`]), so the comparison is against durable state,
    /// not against a value the caller supplied earlier. A fragment from any
    /// other epoch is left untouched and reported with
    /// [`BlobError::FencedByEpoch`], which names both epochs. That refusal is a
    /// "stop, do not retry" answer: retrying with the recorded epoch would be a
    /// delete the caller did not prove it was entitled to.
    ///
    /// The check is **equality**, the strict form, and it does not consult the
    /// pin table: it answers "is this the epoch I named", not "is this epoch
    /// still readable". A collector that has advanced an epoch therefore deletes
    /// nothing through this method, which is why
    /// [`Self::delete_collectable`] exists next to it rather than replacing it.
    ///
    /// A fragment that is already absent (or already tombstoned) answers
    /// `Ok(false)`, like [`Self::delete`], and the id is tombstoned so a late
    /// PUT cannot resurrect it. There is no epoch to compare in that case, and
    /// nothing to unlink, so no fence is bypassed.
    pub fn delete_fenced(&self, id: BlobId, expected_epoch: u64) -> Result<bool, BlobError> {
        self.delete_inner(id, DeleteFence::Exact(expected_epoch))
    }

    /// Durably removes one immutable blob that a collector at `collector_epoch`
    /// is entitled to collect: its recorded epoch must be **older** than the
    /// collector's, and no unexpired pin may name that epoch.
    ///
    /// This is the relaxed form the epoch exists for. It differs from
    /// [`Self::delete_fenced`] in both directions:
    ///
    /// * the comparison is `fragment_epoch < collector_epoch`, not equality, so a
    ///   collector that advanced past a fragment can still reclaim it;
    /// * it refuses while any reader pin is live on the slot, which equality
    ///   alone could not express.
    ///
    /// The pin rule is "any live pin", not "a pin that names this fragment's
    /// epoch". A reader can only pin the target's *current* epoch
    /// ([`Self::pin`] refuses anything else), while the fragment it fetches may
    /// have been written under an older one. Consulting only pins that name the
    /// fragment's epoch would therefore admit a delete of a fragment a reader is
    /// holding - the exact loss the pin exists to prevent. The cost is
    /// deliberate: one live pin pauses every collectable delete on the slot
    /// until its TTL, which is the direction that deletes less.
    ///
    /// A fragment that is not older is refused with
    /// [`BlobError::FragmentNotOlder`]; one protected by a live pin is refused
    /// with [`BlobError::FragmentPinned`]. Both name the fragment's epoch and the
    /// collector's, both happen before any record is appended, and neither is
    /// retryable: a pass that hits one stops instead of re-reading the pin table
    /// until it can proceed.
    ///
    /// A fragment that is already absent (or already tombstoned) answers
    /// `Ok(false)` and is tombstoned, exactly like [`Self::delete_fenced`]:
    /// there is no epoch to compare and nothing to unlink.
    pub fn delete_collectable(&self, id: BlobId, collector_epoch: u64) -> Result<bool, BlobError> {
        self.delete_inner(id, DeleteFence::Collectable(collector_epoch))
    }

    fn delete_inner(&self, id: BlobId, fence: DeleteFence) -> Result<bool, BlobError> {
        let mut writer = self.writer.lock().unwrap();
        writer.ensure_healthy()?;
        if self.tombstones.read().unwrap().contains(&id) {
            return Ok(false);
        }
        let recorded_epoch = self.index.read().unwrap().get(&id).map(|entry| entry.epoch);
        match fence {
            DeleteFence::Unfenced => {}
            DeleteFence::Exact(expected_epoch) => {
                if let Some(recorded_epoch) = recorded_epoch {
                    if recorded_epoch != expected_epoch {
                        // Checked before anything is appended: a refused delete
                        // must not leave a tombstone behind, because that is a
                        // permanent fence on an id this caller was not entitled
                        // to delete.
                        return Err(BlobError::FencedByEpoch {
                            id,
                            recorded_epoch,
                            expected_epoch,
                        });
                    }
                }
            }
            DeleteFence::Collectable(collector_epoch) => {
                if let Some(fragment_epoch) = recorded_epoch {
                    if fragment_epoch >= collector_epoch {
                        // Equality is not collectable either: the collector has
                        // not advanced past this fragment, so a reader may still
                        // be reading it under an unexpired pin.
                        return Err(BlobError::FragmentNotOlder {
                            id,
                            fragment_epoch,
                            collector_epoch,
                        });
                    }
                    // Any live pin, not only one that names `fragment_epoch`.
                    // A reader cannot pin an older epoch - `pin` accepts only
                    // the current one - so the pin that protects this fragment
                    // is whichever pin is live now. Consulting the fragment's
                    // own epoch would let a collector delete this fragment
                    // under a reader's live pin on the current epoch.
                    //
                    // Read under the append lock a pin also takes, so a pin
                    // that lands after this check is ordered after the
                    // tombstone, not between the check and the unlink.
                    let live_pins = self.live_pins_on(None)?.len();
                    if live_pins > 0 {
                        return Err(BlobError::FragmentPinned {
                            id,
                            fragment_epoch,
                            collector_epoch,
                            live_pins,
                        });
                    }
                }
            }
        }
        let hdr = encode_record_header(REC_KIND_TOMBSTONE, id, 0, 0, 0);
        let pos = writer.write_pos;
        if let Err(error) = writer.file.write_at(pos, &hdr) {
            if writer.file.set_len(pos).is_err() {
                writer.failed = true;
            }
            return Err(error.into());
        }
        // Deletion fencing must survive restart even for bulk-load volumes.
        writer.sync_data()?;
        writer.write_pos += RECORD_HEADER_LEN as u64;
        self.tombstones.write().unwrap().insert(id);
        Ok(self.index.write().unwrap().remove(&id).is_some())
    }

    /// Reads and verifies the full CRC32 of a blob.
    pub fn get(&self, id: BlobId) -> Result<Vec<u8>, BlobError> {
        let index = self.index.read().unwrap();
        let entry = index.get(&id).copied().ok_or(BlobError::NotFound(id))?;

        let segs = self.segments.read().unwrap();
        // Pin the matching segment handle before allowing compaction to swap
        // its file and offsets. Lock order is index -> segments everywhere.
        drop(index);
        let file = segs.get(&entry.seg_id).ok_or(BlobError::NotFound(id))?;

        let mut buf = vec![0u8; entry.data_len as usize];
        let n = file.read_at(entry.data_offset, &mut buf)?;
        if n != buf.len() {
            return Err(BlobError::CorruptData {
                id,
                reason: "short read from segment",
            });
        }
        let actual_crc = crc32fast::hash(&buf);
        if actual_crc != entry.data_crc {
            return Err(BlobError::CorruptData {
                id,
                reason: "CRC32 mismatch",
            });
        }
        Ok(buf)
    }

    /// Sub-blob range read (`offset..offset+len`) for DB SSTable pages or virtual block volumes.
    pub fn get_slice(&self, id: BlobId, offset: usize, len: usize) -> Result<Vec<u8>, BlobError> {
        let index = self.index.read().unwrap();
        let entry = index.get(&id).copied().ok_or(BlobError::NotFound(id))?;

        if offset + len > entry.data_len as usize {
            return Err(BlobError::SliceOutOfBounds {
                id,
                offset,
                len,
                blob_len: entry.data_len as usize,
            });
        }

        let segs = self.segments.read().unwrap();
        drop(index);
        let file = segs.get(&entry.seg_id).ok_or(BlobError::NotFound(id))?;
        let mut buf = vec![0u8; len];
        let n = file.read_at(entry.data_offset + offset as u64, &mut buf)?;
        if n != len {
            return Err(BlobError::CorruptData {
                id,
                reason: "short slice read",
            });
        }
        Ok(buf)
    }

    pub fn contains(&self, id: BlobId) -> bool {
        self.index.read().unwrap().contains_key(&id)
    }

    /// YDB-style generation barrier GC: durably records a barrier and evicts all blobs belonging
    /// to `(project_id, owner_id, channel)` with `generation <= up_to_generation`.
    ///
    /// `lease_expires_at_ms` is the deadline of the collector lease the caller
    /// issued this barrier under. It is checked here, immediately before the
    /// durable barrier record and the eviction, because a barrier is the one
    /// deletion a per-tombstone fence interval cannot bound: one record evicts a
    /// whole generation range. A barrier that arrives after its lease lapsed
    /// therefore evicts nothing and reports [`BlobError::BarrierLeaseExpired`].
    /// The residual window is this call's own barrier write and fsync; the
    /// caller's clock and this one are compared directly, so a target clock that
    /// is behind extends that window by the skew.
    ///
    /// A target that cannot read its own clock refuses rather than proceeds: this
    /// is a guard on deletion, and "the clock is broken, so assume the lease is
    /// fine" is the one direction that must not be taken.
    pub fn collect_garbage_barrier(
        &self,
        project_id: u32,
        owner_id: u32,
        channel: u8,
        up_to_generation: u32,
        lease_expires_at_ms: u64,
    ) -> Result<usize, BlobError> {
        check_barrier_deadline(
            (project_id, owner_id, channel, up_to_generation),
            lease_expires_at_ms,
            target_clock(),
        )?;
        let marker_id = BlobId::new(project_id, owner_id, up_to_generation, 0, channel, 0);
        let mut writer = self.writer.lock().unwrap();
        writer.ensure_healthy()?;
        // A barrier names a generation range, not an epoch, so its record carries
        // no epoch. It neither raises nor consumes the target epoch.
        let hdr = encode_record_header(REC_KIND_BARRIER, marker_id, 0, 0, 0);
        let pos = writer.write_pos;
        if let Err(error) = writer.file.write_at(pos, &hdr) {
            if writer.file.set_len(pos).is_err() {
                writer.failed = true;
            }
            return Err(error.into());
        }
        writer.sync_data()?;
        writer.write_pos += RECORD_HEADER_LEN as u64;

        {
            let mut bmap = self.barriers.write().unwrap();
            let cur = bmap.entry((project_id, owner_id, channel)).or_insert(0);
            *cur = (*cur).max(up_to_generation);
        }

        // Fast BTreeMap range scan across matching (project_id, owner_id)
        let start = BlobId::new(project_id, owner_id, 0, 0, 0, 0);
        let end = BlobId::new(
            project_id,
            owner_id,
            up_to_generation,
            BlobId::MAX_U24,
            u8::MAX,
            u8::MAX,
        );

        let mut idx = self.index.write().unwrap();
        let doomed: Vec<BlobId> = idx
            .range(start..=end)
            .filter_map(|(&k, _)| {
                (k.channel() == channel && k.generation() <= up_to_generation).then_some(k)
            })
            .collect();
        let removed = doomed.len();
        for k in doomed {
            idx.remove(&k);
        }
        Ok(removed)
    }

    /// Every physical blob id currently readable from this volume, in ascending
    /// order. This is the only complete view of what the volume holds: a
    /// collector cannot infer it from manifests, because the fragments it must
    /// reclaim are exactly the ones no manifest names.
    pub fn live_ids(&self) -> Vec<BlobId> {
        self.index.read().unwrap().keys().copied().collect()
    }

    /// Rolls the active segment over so its dead records become eligible for
    /// compaction. `compact_sealed_segments` never touches the active segment, so
    /// without this a volume that keeps overwriting the same blob reclaims nothing.
    ///
    /// Sealing is bounded rather than unconditional: an active segment is only
    /// rolled when it already carries garbage and is either mostly garbage or
    /// large enough that a roll would happen soon anyway. Returns `true` when a
    /// new segment was opened.
    pub fn seal_active_segment(&self, min_garbage_bytes: u64) -> Result<bool, BlobError> {
        let mut writer = self.writer.lock().unwrap();
        writer.ensure_healthy()?;
        if writer.write_pos <= VOLUME_MAGIC.len() as u64 {
            // Nothing was appended since the last roll: a new segment would only
            // add an empty file.
            return Ok(false);
        }
        let active_id = writer.seg_id;
        let active_len = writer.write_pos;
        let live_in_active: u64 = self
            .index
            .read()
            .unwrap()
            .values()
            .filter(|entry| entry.seg_id == active_id)
            .map(|entry| (RECORD_HEADER_LEN + entry.data_len as usize) as u64)
            .sum();
        let garbage = active_len
            .saturating_sub(VOLUME_MAGIC.len() as u64)
            .saturating_sub(live_in_active)
            .saturating_sub(
                // Barriers, tombstones and pins are live markers. So is exactly
                // one epoch record: every older one in the segment is dead,
                // because recovery only reads the maximum. Counting one as live
                // keeps an idle volume that has advanced its epoch a few times
                // from sealing a segment holding nothing but superseded epoch
                // records.
                (self.barriers.read().unwrap().len()
                    + self.tombstones.read().unwrap().len()
                    + self.pins.read().unwrap().len()
                    + 1) as u64
                    * RECORD_HEADER_LEN as u64,
            );
        if garbage == 0 {
            return Ok(false);
        }
        if garbage < min_garbage_bytes && garbage.saturating_mul(4) < active_len {
            return Ok(false);
        }

        let next_id = active_id + 1;
        let new_file = init_new_segment(&self.fs, &self.dir, next_id)?;
        let read_handle = self.fs.open_rw(&segment_path(&self.dir, next_id), false)?;
        self.segments.write().unwrap().insert(next_id, read_handle);
        *writer = ActiveSegment {
            seg_id: next_id,
            file: new_file,
            write_pos: VOLUME_MAGIC.len() as u64,
            failed: false,
        };
        Ok(true)
    }

    /// Rolls the active segment over when it is worth compacting, then compacts
    /// every sealed segment. Returns bytes freed. Idempotent: a second call with
    /// nothing to reclaim returns 0.
    pub fn seal_and_compact(&self) -> Result<u64, BlobError> {
        self.seal_active_segment(MIN_SEAL_GARBAGE_BYTES)?;
        self.compact_sealed_segments()
    }

    /// Compacts a sealed (non-active) segment by copying only live blobs into a replacement file
    /// and atomically renaming it into place. Reclaims disk space from barrier-deleted blobs.
    pub fn compact_sealed_segments(&self) -> Result<u64, BlobError> {
        let active_seg = {
            let writer = self.writer.lock().unwrap();
            writer.ensure_healthy()?;
            writer.seg_id
        };
        let seg_ids: Vec<u32> = self
            .segments
            .read()
            .unwrap()
            .keys()
            .copied()
            .filter(|&id| id < active_seg)
            .collect();

        let mut reclaimed_bytes = 0u64;
        for seg_id in seg_ids {
            reclaimed_bytes += self.compact_single_segment(seg_id)?;
        }
        Ok(reclaimed_bytes)
    }

    fn compact_single_segment(&self, seg_id: u32) -> Result<u64, BlobError> {
        // Conservative serialization: prevent PUT/repair, barriers, tombstones
        // and another compactor from invalidating the replacement snapshot.
        let writer = self.writer.lock().unwrap();
        writer.ensure_healthy()?;
        let old_len = {
            let segs = self.segments.read().unwrap();
            match segs.get(&seg_id) {
                Some(f) => f.len()?,
                None => return Ok(0),
            }
        };

        let live_items: Vec<(BlobId, IndexEntry)> = self
            .index
            .read()
            .unwrap()
            .iter()
            .filter(|(_, e)| e.seg_id == seg_id)
            .map(|(&id, &e)| (id, e))
            .collect();

        // Barriers are durable state too. They are not part of the live-blob
        // index, so copying only `live_items` would silently drop a barrier
        // when its old segment is compacted. Preserve the current barriers in
        // the replacement segment; duplicate markers are harmless during
        // recovery and keep fencing intact even if every old segment is
        // compacted away.
        let barrier_markers: Vec<BlobId> = self
            .barriers
            .read()
            .unwrap()
            .iter()
            .map(|(&(project_id, owner_id, channel), &generation)| {
                BlobId::new(project_id, owner_id, generation, 0, channel, 0)
            })
            .collect();
        let tombstone_markers: Vec<BlobId> =
            self.tombstones.read().unwrap().iter().copied().collect();
        // Pins are durable state too, and dropping a live one while rewriting a
        // segment would be the same loss as forgetting it on restart: the
        // collector would be free to advance past a reader that still holds
        // bytes. Unexpired pins are copied into every replacement segment, so a
        // pin whose own segment is compacted away survives; the duplicates
        // replay idempotently, exactly like barrier markers. Expired pins may be
        // dropped, because they block nothing. An unreadable clock keeps every
        // pin rather than guessing that it lapsed.
        let pins_to_keep = select_pins_to_keep(&self.pins.read().unwrap(), &target_clock());
        // The epoch is durable state of its own. A replacement segment must carry
        // it, or compacting away the segment that held the last epoch record
        // would lower the epoch on the next open.
        let epoch = self.epoch.load(Ordering::SeqCst);

        let tmp_path = format!("{}.compact", segment_path(&self.dir, seg_id));
        let final_path = segment_path(&self.dir, seg_id);
        let tmp_file = self.fs.open_rw(&tmp_path, true)?;
        tmp_file.set_len(0)?;
        tmp_file.write_at(0, VOLUME_MAGIC)?;
        let mut pos = VOLUME_MAGIC.len() as u64;

        let mut new_offsets: Vec<(BlobId, IndexEntry)> = Vec::with_capacity(live_items.len());
        for (id, entry) in live_items {
            let payload = self.get(id)?;
            // Keep the fragment's own epoch: compaction rewrites bytes, it does
            // not create a new fragment, so a fenced delete must still see the
            // epoch the fragment was originally written under.
            let hdr = encode_record_header(
                REC_KIND_PUT,
                id,
                payload.len() as u32,
                entry.data_crc,
                entry.epoch,
            );
            tmp_file.write_at(pos, &hdr)?;
            let data_offset = pos + RECORD_HEADER_LEN as u64;
            tmp_file.write_at(data_offset, &payload)?;
            pos = data_offset + payload.len() as u64;
            new_offsets.push((
                id,
                IndexEntry {
                    seg_id,
                    data_offset,
                    data_len: entry.data_len,
                    data_crc: entry.data_crc,
                    epoch: entry.epoch,
                },
            ));
        }

        for marker_id in barrier_markers {
            let hdr = encode_record_header(REC_KIND_BARRIER, marker_id, 0, 0, 0);
            tmp_file.write_at(pos, &hdr)?;
            pos += RECORD_HEADER_LEN as u64;
        }

        for marker_id in tombstone_markers {
            let hdr = encode_record_header(REC_KIND_TOMBSTONE, marker_id, 0, 0, 0);
            tmp_file.write_at(pos, &hdr)?;
            pos += RECORD_HEADER_LEN as u64;
        }

        for (pin_id, entry) in pins_to_keep {
            let payload = encode_pin_payload(pin_id, entry.expires_at_ms);
            let hdr = encode_record_header(
                REC_KIND_PIN,
                epoch_record_id(),
                PIN_PAYLOAD_LEN as u32,
                crc32fast::hash(&payload),
                entry.epoch,
            );
            tmp_file.write_at(pos, &hdr)?;
            tmp_file.write_at(pos + RECORD_HEADER_LEN as u64, &payload)?;
            pos += (RECORD_HEADER_LEN + PIN_PAYLOAD_LEN) as u64;
        }

        let hdr = encode_record_header(REC_KIND_EPOCH, epoch_record_id(), 0, 0, epoch);
        tmp_file.write_at(pos, &hdr)?;
        pos += RECORD_HEADER_LEN as u64;

        tmp_file.sync_data()?;
        drop(tmp_file);

        // Open the replacement before rename: once rename succeeds, even a
        // subsequent directory-sync error must not leave runtime reads using
        // the old file while new opens use the replacement.
        let reopened = self.fs.open_rw(&tmp_path, false)?;
        let new_len = reopened.len()?;
        self.fs.rename_atomic(&tmp_path, &final_path)?;
        let mut idx = self.index.write().unwrap();
        let mut segs = self.segments.write().unwrap();
        segs.insert(seg_id, reopened);
        for (id, updated) in new_offsets {
            if let Some(cur) = idx.get_mut(&id) {
                if cur.seg_id == seg_id {
                    *cur = updated;
                }
            }
        }

        self.fs.sync_dir(&self.dir)?;

        Ok(old_len.saturating_sub(new_len))
    }

    pub fn stats(&self) -> Result<VolumeStats, BlobError> {
        let idx = self.index.read().unwrap();
        let live_blobs = idx.len();
        let live_bytes: u64 = idx.values().map(|e| e.data_len as u64).sum();
        let segs = self.segments.read().unwrap();
        let mut total_disk_bytes = 0u64;
        for f in segs.values() {
            total_disk_bytes += f.len()?;
        }
        Ok(VolumeStats {
            live_blobs,
            live_bytes,
            segment_count: segs.len(),
            total_disk_bytes,
        })
    }
}

fn join_path(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", dir.trim_end_matches('/'), name)
    }
}

/// The target's own wall clock, in milliseconds since the Unix epoch.
///
/// Returns the failure rather than a default: a barrier is a deletion guard, and
/// substituting `0` for an unreadable clock would make every deadline look live.
fn target_clock() -> Result<u64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().min(u64::MAX as u128) as u64)
        .map_err(|error| error.to_string())
}

/// Refuses a barrier whose collector lease has lapsed at the target.
///
/// Split out of [`VolumeStore::collect_garbage_barrier`] so the refusal can be
/// tested with an injected clock and an injected clock *failure*: the second is
/// the case that must not silently pass, and it cannot be produced by calling
/// `SystemTime::now()` in a test.
pub(crate) fn check_barrier_deadline(
    ids: (u32, u32, u8, u32),
    lease_expires_at_ms: u64,
    now_ms: Result<u64, String>,
) -> Result<(), BlobError> {
    let (project_id, owner_id, channel, up_to_generation) = ids;
    let now_ms = now_ms.map_err(|reason| BlobError::BarrierClockUnreadable {
        project_id,
        owner_id,
        channel,
        up_to_generation,
        reason,
    })?;
    if now_ms >= lease_expires_at_ms {
        return Err(BlobError::BarrierLeaseExpired {
            project_id,
            owner_id,
            channel,
            up_to_generation,
            expires_at_ms: lease_expires_at_ms,
            now_ms,
        });
    }
    Ok(())
}

/// Selects the pins a replacement segment must carry.
///
/// Split out of [`VolumeStore::compact_single_segment`] so the clock branch can
/// be tested with an injected clock and an injected clock *failure*. The two
/// directions are deliberate and opposite:
///
/// * a readable clock keeps every pin for which `now < expires_at_ms`, because
///   an expired pin blocks nothing;
/// * an unreadable clock keeps **every** pin. Dropping a live pin while
///   rewriting a segment would let a collector advance past a reader that still
///   holds bytes; keeping a dead one only delays collection until the next
///   compaction, so the unreadable case takes the direction that cannot delete.
///
/// Keeping this a pure function is what makes the second branch a verified
/// property rather than a claim about code no test can reach.
pub(crate) fn select_pins_to_keep(
    pins: &BTreeMap<PinId, PinEntry>,
    now_ms: &Result<u64, String>,
) -> Vec<(PinId, PinEntry)> {
    match now_ms {
        Ok(now) => pins
            .iter()
            .filter(|(_, entry)| *now < entry.expires_at_ms)
            .map(|(&pin_id, &entry)| (pin_id, entry))
            .collect(),
        Err(_) => pins
            .iter()
            .map(|(&pin_id, &entry)| (pin_id, entry))
            .collect(),
    }
}

/// Liveness of a pin, as a pure decision so it can be tested with an injected
/// clock and an injected clock *failure*.
///
/// `now < expires_at_ms`, and an unreadable clock refuses. The tempting default -
/// substitute `0` for an unreadable clock - makes every pin look expired, which
/// lets a collector advance past a reader that is still holding bytes. That is
/// the same failure direction as the barrier clock, so the same rule applies
/// here.
pub(crate) fn pin_is_live(
    expires_at_ms: u64,
    now_ms: &Result<u64, String>,
) -> Result<bool, BlobError> {
    let now = now_ms
        .as_ref()
        .map_err(|reason| BlobError::PinClockUnreadable {
            reason: reason.clone(),
        })?;
    Ok(*now < expires_at_ms)
}

/// Rebuilds the pin set from the PIN/UNPIN records recovery collected, in log
/// order, and returns it with the next pin id to issue.
///
/// A pin is live when its PIN record was not followed by its UNPIN and its
/// deadline has not passed; the second half is decided by [`pin_is_live`], so a
/// broken clock refuses here instead of dropping every pin. Expired pins are
/// dropped - the one place a pin may disappear without a record saying so - and
/// no pin is ever invented: every entry comes from a record that is on disk.
///
/// The next id is `max(id) + 1` over every PIN and UNPIN record, including ones
/// whose pin was released or had expired. An id is therefore never reissued while
/// a record naming it survives, so a late `unpin` of an old id cannot release a
/// newer pin.
pub(crate) fn rebuild_pins(
    records: Vec<PinRecord>,
    now_ms: &Result<u64, String>,
) -> Result<(BTreeMap<PinId, PinEntry>, u64), BlobError> {
    let mut max_pin_id = 0u64;
    let mut pins: BTreeMap<PinId, PinEntry> = BTreeMap::new();
    for record in records {
        match record {
            PinRecord::Pin {
                pin_id,
                epoch,
                expires_at_ms,
            } => {
                max_pin_id = max_pin_id.max(pin_id.0);
                pins.insert(
                    pin_id,
                    PinEntry {
                        epoch,
                        expires_at_ms,
                    },
                );
            }
            PinRecord::Unpin { pin_id } => {
                max_pin_id = max_pin_id.max(pin_id.0);
                pins.remove(&pin_id);
            }
        }
    }
    if !pins.is_empty() {
        let mut expired = Vec::new();
        for (&pin_id, entry) in pins.iter() {
            if !pin_is_live(entry.expires_at_ms, now_ms)? {
                expired.push(pin_id);
            }
        }
        for pin_id in expired {
            pins.remove(&pin_id);
        }
    }
    Ok((pins, max_pin_id.saturating_add(1)))
}

fn segment_path(dir: &str, seg_id: u32) -> String {
    join_path(dir, &format!("vol_{seg_id:08}.dat"))
}

fn parse_segment_name(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("vol_")?.strip_suffix(".dat")?;
    rest.parse::<u32>().ok()
}

fn init_new_segment<FS: StorageFs>(fs: &FS, dir: &str, seg_id: u32) -> Result<FS::File, BlobError> {
    let path = segment_path(dir, seg_id);
    let file = fs.open_rw(&path, true)?;
    // Creating a segment must never discard one that already exists. `open`
    // only calls this for an id its directory scan did not see, so a non-empty
    // file here means two `VolumeStore` instances are serving one directory (a
    // lost race) or the scan missed a file. Truncating would drop every record
    // in it — including tombstones and barriers, which resurrect deleted blobs —
    // so this refuses instead of reinitialising.
    if file.len()? > 0 {
        return Err(BlobError::InvalidSegment(
            "refusing to reinitialize a non-empty segment",
        ));
    }
    file.set_len(0)?;
    file.write_at(0, VOLUME_MAGIC)?;
    file.sync_data()?;
    fs.sync_dir(dir)?;
    Ok(file)
}

/// Encodes the current (V2) record header.
///
/// `epoch` is the target epoch the record was written under. It is meaningful for
/// `REC_KIND_PUT` and `REC_KIND_EPOCH`; a barrier and a tombstone name no
/// fragment and pass `0`.
fn encode_record_header(
    kind: u8,
    id: BlobId,
    data_len: u32,
    data_crc: u32,
    epoch: u64,
) -> [u8; RECORD_HEADER_LEN] {
    let mut hdr = [0u8; RECORD_HEADER_LEN];
    hdr[0..4].copy_from_slice(&RECORD_MAGIC_V2.to_le_bytes());
    hdr[4] = kind;
    hdr[5..21].copy_from_slice(&id.to_be_bytes());
    hdr[21..25].copy_from_slice(&data_len.to_le_bytes());
    hdr[25..29].copy_from_slice(&data_crc.to_le_bytes());
    hdr[29..37].copy_from_slice(&epoch.to_le_bytes());
    let hdr_crc = crc32fast::hash(&hdr[0..37]);
    hdr[37..41].copy_from_slice(&hdr_crc.to_le_bytes());
    hdr
}

fn recover_segment<F: VirtualFile>(
    file: &F,
    seg_id: u32,
    index: &mut BTreeMap<BlobId, IndexEntry>,
    barriers: &mut BTreeMap<(u32, u32, u8), u32>,
    tombstones: &mut BTreeSet<BlobId>,
    epoch: &mut u64,
    pins: &mut Vec<PinRecord>,
) -> Result<(), BlobError> {
    let file_len = file.len()?;
    if file_len < VOLUME_MAGIC.len() as u64 {
        return Err(BlobError::InvalidSegment(
            "segment shorter than magic header",
        ));
    }
    let mut magic = [0u8; 8];
    file.read_at(0, &mut magic)?;
    if &magic != VOLUME_MAGIC {
        return Err(BlobError::InvalidSegment("bad volume magic"));
    }

    let mut pos = VOLUME_MAGIC.len() as u64;
    let mut hdr = [0u8; RECORD_HEADER_LEN];

    while pos < file_len {
        let remaining = file_len - pos;
        if remaining < RECORD_HEADER_LEN_V1 as u64 {
            // Torn header at tail after power cut -> truncate cleanly!
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }
        // Read as much of a current header as the file still holds, so a torn
        // tail of a V2 header is distinguishable from a damaged one.
        let want = remaining.min(RECORD_HEADER_LEN as u64) as usize;
        let n = file.read_at(pos, &mut hdr[..want])?;
        if n < want {
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }

        let rec_magic = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        // The first 29 bytes mean the same thing in both layouts; the epoch and
        // the header CRC follow. A V1 record has no epoch and belongs to epoch 0,
        // the epoch every volume starts at.
        let hdr_len = match rec_magic {
            RECORD_MAGIC_V1 => RECORD_HEADER_LEN_V1,
            RECORD_MAGIC_V2 => {
                if want < RECORD_HEADER_LEN {
                    // A complete V2 magic but an incomplete header: the tail of a
                    // torn append, which was never acknowledged.
                    file.set_len(pos)?;
                    file.sync_data()?;
                    break;
                }
                RECORD_HEADER_LEN
            }
            _ => {
                return Err(BlobError::InvalidSegment("corrupt record header"));
            }
        };
        let crc_at = hdr_len - 4;
        let expected_hdr_crc = u32::from_le_bytes([
            hdr[crc_at],
            hdr[crc_at + 1],
            hdr[crc_at + 2],
            hdr[crc_at + 3],
        ]);
        let actual_hdr_crc = crc32fast::hash(&hdr[0..crc_at]);
        if expected_hdr_crc != actual_hdr_crc {
            // A complete header is present, so this is damage to a record that
            // may be followed by durable TOMBSTONE/BARRIER records. Truncating
            // here would drop those markers and could resurrect deleted blobs,
            // so refuse to open instead. A torn header write leaves fewer than
            // the header's bytes and is handled above.
            return Err(BlobError::InvalidSegment("corrupt record header"));
        }

        let kind = hdr[4];
        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&hdr[5..21]);
        let id = BlobId::from_be_bytes(id_bytes);
        let data_len = u32::from_le_bytes([hdr[21], hdr[22], hdr[23], hdr[24]]);
        let data_crc = u32::from_le_bytes([hdr[25], hdr[26], hdr[27], hdr[28]]);
        let record_epoch = if hdr_len == RECORD_HEADER_LEN {
            u64::from_le_bytes([
                hdr[29], hdr[30], hdr[31], hdr[32], hdr[33], hdr[34], hdr[35], hdr[36],
            ])
        } else {
            0
        };
        // Any record that names an epoch is proof the volume reached it: a
        // fragment cannot have been written under an epoch the target had not
        // reached. Recovery therefore takes the maximum, which is what makes a
        // torn epoch append unable to lower the value.
        *epoch = (*epoch).max(record_epoch);

        let data_offset = pos + hdr_len as u64;
        let next_pos = data_offset + data_len as u64;
        if next_pos > file_len {
            // Torn payload write -> truncate back to `pos`!
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }

        match kind {
            REC_KIND_PUT => {
                // A complete record with a valid header but bad payload is
                // bit-rot, not proof that the rest of the log is a torn tail.
                // Keep its location so GET detects CRC failure and repair can
                // rewrite it. Never truncate later durable data/fencing markers
                // merely because this payload was corrupted.
                let fenced = barriers
                    .get(&(id.project_id(), id.owner_id(), id.channel()))
                    .is_some_and(|generation| id.generation() <= *generation);
                if !tombstones.contains(&id) && !fenced {
                    index.insert(
                        id,
                        IndexEntry {
                            seg_id,
                            data_offset,
                            data_len,
                            data_crc,
                            epoch: record_epoch,
                        },
                    );
                }
            }
            REC_KIND_EPOCH => {
                // No index effect: the maximum above is the whole of its meaning.
            }
            REC_KIND_BARRIER => {
                let b_gen = id.generation();
                let key = (id.project_id(), id.owner_id(), id.channel());
                let cur = barriers.entry(key).or_insert(0);
                *cur = (*cur).max(b_gen);
                index.retain(|k, _| {
                    !(k.project_id() == id.project_id()
                        && k.owner_id() == id.owner_id()
                        && k.channel() == id.channel()
                        && k.generation() <= b_gen)
                });
            }
            REC_KIND_TOMBSTONE => {
                tombstones.insert(id);
                index.remove(&id);
            }
            REC_KIND_PIN => {
                // A pin's payload is read at recovery time because recovery has
                // to decide whether it is still live. Its CRC is therefore
                // checked here, unlike a PUT payload which is verified on read.
                let mut payload = [0u8; PIN_PAYLOAD_LEN];
                if data_len as usize != PIN_PAYLOAD_LEN
                    || file.read_at(data_offset, &mut payload)? != PIN_PAYLOAD_LEN
                    || crc32fast::hash(&payload) != data_crc
                {
                    // A complete record with a valid header but a bad payload is
                    // bit-rot, not a torn tail: the append was acknowledged, so
                    // truncating here could discard a live pin (and every later
                    // record). Refuse to open instead.
                    return Err(BlobError::InvalidSegment("corrupt pin record"));
                }
                pins.push(PinRecord::Pin {
                    pin_id: PinId(u64::from_le_bytes([
                        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                        payload[6], payload[7],
                    ])),
                    epoch: record_epoch,
                    expires_at_ms: u64::from_le_bytes([
                        payload[8],
                        payload[9],
                        payload[10],
                        payload[11],
                        payload[12],
                        payload[13],
                        payload[14],
                        payload[15],
                    ]),
                });
            }
            REC_KIND_UNPIN => {
                let mut payload = [0u8; UNPIN_PAYLOAD_LEN];
                if data_len as usize != UNPIN_PAYLOAD_LEN
                    || file.read_at(data_offset, &mut payload)? != UNPIN_PAYLOAD_LEN
                    || crc32fast::hash(&payload) != data_crc
                {
                    return Err(BlobError::InvalidSegment("corrupt unpin record"));
                }
                pins.push(PinRecord::Unpin {
                    pin_id: PinId(u64::from_le_bytes([
                        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                        payload[6], payload[7],
                    ])),
                });
            }
            _ => {
                // The kind is covered by the header checksum, so this is a
                // format this reader cannot interpret, not a torn tail.
                // Truncating would silently discard every later record.
                return Err(BlobError::InvalidSegment("unknown record kind"));
            }
        }

        pos = next_pos;
    }

    Ok(())
}

#[cfg(test)]
mod clock_injection_tests {
    use super::*;

    fn pin(id: u64, epoch: u64, expires_at_ms: u64) -> (PinId, PinEntry) {
        (
            PinId::from_wire(id),
            PinEntry {
                epoch,
                expires_at_ms,
            },
        )
    }

    fn pins() -> BTreeMap<PinId, PinEntry> {
        [
            pin(1, 7, 199),
            pin(2, 7, 200),
            // Exactly the injected "now": `now < expires_at_ms` is strict, so
            // this one is still live.
            pin(3, 8, 201),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn a_segment_replacement_copies_exactly_the_pins_live_on_its_clock() {
        let kept = select_pins_to_keep(&pins(), &Ok(200));
        // 199 and 200 are expired at now=200; 201 is live. Keeping a dead pin
        // only delays collection, dropping the live one would let a collector
        // advance past a reader that still holds bytes.
        assert_eq!(kept.len(), 1, "kept: {kept:?}");
        assert_eq!(kept[0].0.get(), 3);
        assert_eq!(kept[0].1.epoch, 8);
        assert_eq!(kept[0].1.expires_at_ms, 201);
    }

    #[test]
    fn an_unreadable_clock_keeps_every_pin_including_expired_ones() {
        let kept = select_pins_to_keep(&pins(), &Err("clock source unavailable".into()));
        // This is the branch that cannot be reached through a real
        // `target_clock()` in a test. Emptying the replacement segment here
        // would be the one direction that loses a live pin.
        assert_eq!(kept.len(), 3, "kept: {kept:?}");
        let ids: Vec<u64> = kept.iter().map(|(pin_id, _)| pin_id.get()).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(kept[2].1.epoch, 8);
    }
}

#[cfg(test)]
mod fence_race_tests {
    use super::*;
    use std::sync::{Arc, Barrier, Mutex};
    use yadb_io::SimFs;

    /// The pin/advance race has one invariant: a live pin is never left on an
    /// epoch the collector moved past.
    ///
    /// Both operations take the same append lock for the epoch compare and the
    /// durable record, so each interleaving must resolve one way: either the
    /// advance wins and a later pin is refused as "not the current epoch", or
    /// the pin wins and every advance from that epoch is refused as
    /// `EpochPinned`. A torn outcome - a pin granted on `E` while the epoch
    /// moves to `E+1` - would let a collector delete bytes that pin protects.
    /// Hammering the two calls from separate threads is what exercises the
    /// interleaving a single-threaded test cannot.
    #[test]
    fn no_live_pin_survives_on_an_epoch_the_collector_moved_past() {
        const ROUNDS: usize = 100;
        let fs = SimFs::new(0x5eed_c0de);
        let store = Arc::new(
            VolumeStore::open(fs.clone(), "race", VolumeConfig::default()).unwrap(),
        );
        let start = Arc::new(Barrier::new(4));
        let granted = Arc::new(Mutex::new(Vec::new()));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = store.clone();
            let start = start.clone();
            let granted = granted.clone();
            handles.push(std::thread::spawn(move || {
                start.wait();
                for _ in 0..ROUNDS {
                    let requested = store.epoch();
                    match store.pin(requested, 60_000) {
                        Ok(grant) => {
                            // The grant names the epoch that was current under
                            // the same lock, and no advance can move it while
                            // this pin is live.
                            assert_eq!(grant.epoch, store.epoch());
                            granted.lock().unwrap().push(grant);
                        }
                        Err(BlobError::PinEpochNotCurrent {
                            requested_epoch,
                            actual_epoch,
                        }) => {
                            // The collector advanced between the read and the
                            // pin. The epoch only moves forward, and refusing a
                            // pin for a superseded epoch is the safe answer.
                            assert!(requested_epoch < actual_epoch);
                        }
                        Err(error) => panic!("unexpected pin failure: {error:?}"),
                    }
                }
            }));
        }
        for _ in 0..2 {
            let store = store.clone();
            let start = start.clone();
            handles.push(std::thread::spawn(move || {
                start.wait();
                for _ in 0..ROUNDS {
                    let requested = store.epoch();
                    match store.advance_epoch(requested) {
                        Ok(next) => assert_eq!(next, requested + 1),
                        Err(BlobError::EpochPinned {
                            epoch,
                            live_pins,
                        }) => {
                            assert_eq!(epoch, requested);
                            assert!(live_pins > 0);
                        }
                        Err(BlobError::EpochMismatch {
                            expected_epoch,
                            actual_epoch,
                        }) => {
                            // Another observer advanced first; the epoch only
                            // moves forward, and the stale request stops.
                            assert!(expected_epoch < actual_epoch);
                        }
                        Err(error) => panic!("unexpected advance failure: {error:?}"),
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // TTL is 60 s, far above this test's run time, so every pin is still
        // live. Once any pin landed, no advance could move past its epoch; if
        // no pin landed at all, the assertion is vacuous and reported.
        let final_epoch = store.epoch();
        let granted = granted.lock().unwrap();
        assert!(!granted.is_empty(), "no pin won a round");
        for grant in granted.iter() {
            assert_eq!(
                grant.epoch, final_epoch,
                "a live pin was left on epoch {} while the target moved to {final_epoch}",
                grant.epoch
            );
        }
        let live = store.live_pins().unwrap();
        assert_eq!(live.len(), granted.len());
        assert!(live.iter().all(|pin| pin.epoch == final_epoch));

        // Release every pin, then advance once. Recovery must find the higher
        // epoch and no resurrected pin: the race left no torn record behind.
        for grant in granted.iter() {
            assert!(store.unpin(grant.pin_id).unwrap());
        }
        drop(granted);
        assert_eq!(store.advance_epoch(final_epoch).unwrap(), final_epoch + 1);
        drop(store);

        let reopened =
            VolumeStore::open(fs, "race", VolumeConfig::default()).unwrap();
        assert_eq!(reopened.epoch(), final_epoch + 1);
        assert!(reopened.live_pins().unwrap().is_empty());
    }
}
