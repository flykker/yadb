use std::collections::BTreeMap;
use std::sync::{Mutex, RwLock};

use yadb_io::{StorageFs, VirtualFile};

use crate::{BlobError, BlobId};

const VOLUME_MAGIC: &[u8; 8] = b"YADBVOL1";
const RECORD_MAGIC: u32 = 0x5942_5243; // "YBRC"
const REC_KIND_PUT: u8 = 1;
const REC_KIND_BARRIER: u8 = 2;
const RECORD_HEADER_LEN: usize = 33;

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
}

struct ActiveSegment<F: VirtualFile> {
    seg_id: u32,
    file: F,
    write_pos: u64,
}

#[derive(Debug, Clone, Copy, Default)]
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
pub struct VolumeStore<FS: StorageFs> {
    fs: FS,
    dir: String,
    cfg: VolumeConfig,
    index: RwLock<BTreeMap<BlobId, IndexEntry>>,
    barriers: RwLock<BTreeMap<(u32, u32, u8), u32>>,
    segments: RwLock<BTreeMap<u32, FS::File>>,
    writer: Mutex<ActiveSegment<FS::File>>,
}

impl<FS: StorageFs> VolumeStore<FS> {
    pub fn open(fs: FS, dir: impl Into<String>, cfg: VolumeConfig) -> Result<Self, BlobError> {
        let dir = dir.into();
        fs.create_dir_all(&dir)?;

        let mut index = BTreeMap::new();
        let mut barriers: BTreeMap<(u32, u32, u8), u32> = BTreeMap::new();
        let mut segments = BTreeMap::new();
        let mut max_seg_id = 0u32;

        for name in fs.list_files(&dir)? {
            // Clean up leftover temporary compaction files after crash
            if name.ends_with(".compact") {
                let _ = fs.remove_file(&join_path(&dir, &name));
                continue;
            }
            if let Some(seg_id) = parse_segment_name(&name) {
                max_seg_id = max_seg_id.max(seg_id);
                let path = segment_path(&dir, seg_id);
                let file = fs.open_rw(&path, false)?;
                recover_segment(&file, seg_id, &mut index, &mut barriers)?;
                segments.insert(seg_id, file);
            }
        }

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

        Ok(Self {
            fs,
            dir,
            cfg,
            index: RwLock::new(index),
            barriers: RwLock::new(barriers),
            segments: RwLock::new(segments),
            writer: Mutex::new(ActiveSegment {
                seg_id: active_id,
                file: active_file,
                write_pos,
            }),
        })
    }

    /// Writes an immutable blob. Idempotent if the exact same `BlobId` and byte length already exist;
    /// rejects conflicting writes to an existing `BlobId` or writes older than a GC barrier.
    pub fn put(&self, id: BlobId, data: &[u8]) -> Result<(), BlobError> {
        if data.len() > self.cfg.max_blob_bytes {
            return Err(BlobError::BlobTooLarge {
                size: data.len(),
                max: self.cfg.max_blob_bytes,
            });
        }

        // Enforce generation fencing barrier
        if let Some(&barrier_gen) = self
            .barriers
            .read()
            .unwrap()
            .get(&(id.project_id(), id.owner_id(), id.channel()))
        {
            if id.generation() <= barrier_gen {
                return Err(BlobError::FencedByBarrier {
                    id,
                    barrier_gen,
                });
            }
        }

        let data_crc = crc32fast::hash(data);

        // Idempotency check for immutable blobs
        if let Some(existing) = self.index.read().unwrap().get(&id).copied() {
            if existing.data_len == data.len() as u32 && existing.data_crc == data_crc {
                return Ok(());
            }
            return Err(BlobError::ImmutableConflict(id));
        }

        let mut writer = self.writer.lock().unwrap();
        let rec_total = (RECORD_HEADER_LEN + data.len()) as u64;
        if writer.write_pos > VOLUME_MAGIC.len() as u64
            && writer.write_pos + rec_total > self.cfg.max_segment_bytes
        {
            let next_id = writer.seg_id + 1;
            let new_file = init_new_segment(&self.fs, &self.dir, next_id)?;
            let read_handle = self.fs.open_rw(&segment_path(&self.dir, next_id), false)?;
            self.segments.write().unwrap().insert(next_id, read_handle);
            *writer = ActiveSegment {
                seg_id: next_id,
                file: new_file,
                write_pos: VOLUME_MAGIC.len() as u64,
            };
        }

        let hdr = encode_record_header(REC_KIND_PUT, id, data.len() as u32, data_crc);
        let mut frame = Vec::with_capacity(RECORD_HEADER_LEN + data.len());
        frame.extend_from_slice(&hdr);
        frame.extend_from_slice(data);

        let start_pos = writer.write_pos;
        if let Err(e) = writer.file.write_at(start_pos, &frame) {
            // Roll back partial/torn tail immediately so next write stays aligned
            let _ = writer.file.set_len(start_pos);
            return Err(BlobError::Io(e));
        }
        if self.cfg.sync_on_put {
            writer.file.sync_data()?;
        }

        let data_offset = start_pos + RECORD_HEADER_LEN as u64;
        let seg_id = writer.seg_id;
        writer.write_pos = start_pos + frame.len() as u64;
        drop(writer);

        self.index.write().unwrap().insert(
            id,
            IndexEntry {
                seg_id,
                data_offset,
                data_len: data.len() as u32,
                data_crc,
            },
        );
        Ok(())
    }

    /// Reads and verifies the full CRC32 of a blob.
    pub fn get(&self, id: BlobId) -> Result<Vec<u8>, BlobError> {
        let entry = self
            .index
            .read()
            .unwrap()
            .get(&id)
            .copied()
            .ok_or(BlobError::NotFound(id))?;

        let segs = self.segments.read().unwrap();
        let file = segs
            .get(&entry.seg_id)
            .ok_or(BlobError::NotFound(id))?;

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
        let entry = self
            .index
            .read()
            .unwrap()
            .get(&id)
            .copied()
            .ok_or(BlobError::NotFound(id))?;

        if offset + len > entry.data_len as usize {
            return Err(BlobError::SliceOutOfBounds {
                id,
                offset,
                len,
                blob_len: entry.data_len as usize,
            });
        }

        let segs = self.segments.read().unwrap();
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
    pub fn collect_garbage_barrier(
        &self,
        project_id: u32,
        owner_id: u32,
        channel: u8,
        up_to_generation: u32,
    ) -> Result<usize, BlobError> {
        let marker_id = BlobId::new(project_id, owner_id, up_to_generation, 0, channel, 0);
        {
            let mut writer = self.writer.lock().unwrap();
            let hdr = encode_record_header(REC_KIND_BARRIER, marker_id, 0, 0);
            let pos = writer.write_pos;
            writer.file.write_at(pos, &hdr)?;
            if self.cfg.sync_on_put {
                writer.file.sync_data()?;
            }
            writer.write_pos += RECORD_HEADER_LEN as u64;
        }

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
            .filter_map(|(&k, _)| (k.channel() == channel && k.generation() <= up_to_generation).then_some(k))
            .collect();
        let removed = doomed.len();
        for k in doomed {
            idx.remove(&k);
        }
        Ok(removed)
    }

    /// Compacts a sealed (non-active) segment by copying only live blobs into a replacement file
    /// and atomically renaming it into place. Reclaims disk space from barrier-deleted blobs.
    pub fn compact_sealed_segments(&self) -> Result<u64, BlobError> {
        let active_seg = self.writer.lock().unwrap().seg_id;
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

        let tmp_path = format!("{}.compact", segment_path(&self.dir, seg_id));
        let final_path = segment_path(&self.dir, seg_id);
        let tmp_file = self.fs.open_rw(&tmp_path, true)?;
        tmp_file.set_len(0)?;
        tmp_file.write_at(0, VOLUME_MAGIC)?;
        let mut pos = VOLUME_MAGIC.len() as u64;

        let mut new_offsets: Vec<(BlobId, IndexEntry)> = Vec::with_capacity(live_items.len());
        for (id, entry) in live_items {
            let payload = self.get(id)?;
            let hdr = encode_record_header(REC_KIND_PUT, id, payload.len() as u32, entry.data_crc);
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
                },
            ));
        }

        tmp_file.sync_data()?;
        drop(tmp_file);

        self.fs.rename_atomic(&tmp_path, &final_path)?;
        self.fs.sync_dir(&self.dir)?;

        let reopened = self.fs.open_rw(&final_path, false)?;
        let new_len = reopened.len()?;
        self.segments.write().unwrap().insert(seg_id, reopened);

        let mut idx = self.index.write().unwrap();
        for (id, updated) in new_offsets {
            if let Some(cur) = idx.get_mut(&id) {
                if cur.seg_id == seg_id {
                    *cur = updated;
                }
            }
        }

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
    file.set_len(0)?;
    file.write_at(0, VOLUME_MAGIC)?;
    file.sync_data()?;
    fs.sync_dir(dir)?;
    Ok(file)
}

fn encode_record_header(kind: u8, id: BlobId, data_len: u32, data_crc: u32) -> [u8; RECORD_HEADER_LEN] {
    let mut hdr = [0u8; RECORD_HEADER_LEN];
    hdr[0..4].copy_from_slice(&RECORD_MAGIC.to_le_bytes());
    hdr[4] = kind;
    hdr[5..21].copy_from_slice(&id.to_be_bytes());
    hdr[21..25].copy_from_slice(&data_len.to_le_bytes());
    hdr[25..29].copy_from_slice(&data_crc.to_le_bytes());
    let hdr_crc = crc32fast::hash(&hdr[0..29]);
    hdr[29..33].copy_from_slice(&hdr_crc.to_le_bytes());
    hdr
}

fn recover_segment<F: VirtualFile>(
    file: &F,
    seg_id: u32,
    index: &mut BTreeMap<BlobId, IndexEntry>,
    barriers: &mut BTreeMap<(u32, u32, u8), u32>,
) -> Result<(), BlobError> {
    let file_len = file.len()?;
    if file_len < VOLUME_MAGIC.len() as u64 {
        return Err(BlobError::InvalidSegment("segment shorter than magic header"));
    }
    let mut magic = [0u8; 8];
    file.read_at(0, &mut magic)?;
    if &magic != VOLUME_MAGIC {
        return Err(BlobError::InvalidSegment("bad volume magic"));
    }

    let mut pos = VOLUME_MAGIC.len() as u64;
    let mut hdr = [0u8; RECORD_HEADER_LEN];

    while pos < file_len {
        if pos + RECORD_HEADER_LEN as u64 > file_len {
            // Torn header at tail after power cut -> truncate cleanly!
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }
        let n = file.read_at(pos, &mut hdr)?;
        if n < RECORD_HEADER_LEN {
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }

        let rec_magic = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        let expected_hdr_crc = u32::from_le_bytes([hdr[29], hdr[30], hdr[31], hdr[32]]);
        let actual_hdr_crc = crc32fast::hash(&hdr[0..29]);
        if rec_magic != RECORD_MAGIC || expected_hdr_crc != actual_hdr_crc {
            // Corrupted tail record -> truncate cleanly at last valid boundary
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }

        let kind = hdr[4];
        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&hdr[5..21]);
        let id = BlobId::from_be_bytes(id_bytes);
        let data_len = u32::from_le_bytes([hdr[21], hdr[22], hdr[23], hdr[24]]);
        let data_crc = u32::from_le_bytes([hdr[25], hdr[26], hdr[27], hdr[28]]);

        let data_offset = pos + RECORD_HEADER_LEN as u64;
        let next_pos = data_offset + data_len as u64;
        if next_pos > file_len {
            // Torn payload write -> truncate back to `pos`!
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }

        match kind {
            REC_KIND_PUT => {
                let mut buf = vec![0u8; data_len as usize];
                file.read_at(data_offset, &mut buf)?;
                if crc32fast::hash(&buf) != data_crc {
                    file.set_len(pos)?;
                    file.sync_data()?;
                    break;
                }
                index.insert(
                    id,
                    IndexEntry {
                        seg_id,
                        data_offset,
                        data_len,
                        data_crc,
                    },
                );
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
            _ => {
                file.set_len(pos)?;
                file.sync_data()?;
                break;
            }
        }

        pos = next_pos;
    }

    Ok(())
}
