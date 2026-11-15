use serde::{Deserialize, Serialize};
use yadb_io::{StorageFs, VirtualFile};

use crate::{LogEntry, MetaError};

/// File magic written by this version. New WALs are `YADBWAL2`, which promises
/// that every frame carries a self-checking header (see [`FrameFormat::V2`]).
///
/// The magic is bumped rather than silently reusing `YADBWAL1` so that a binary
/// that predates this format refuses to open the file (`invalid wal magic`)
/// instead of replaying the legacy reader, which treats an unknown frame magic
/// as a torn tail and would truncate the whole log.
const WAL_MAGIC: &[u8; 8] = b"YADBWAL2";
/// Legacy file magic. Still replayed so existing WALs keep opening.
const WAL_MAGIC_V1: &[u8; 8] = b"YADBWAL1";

const FRAME_MAGIC_V1: u32 = 0x5957_414C; // "YWAL"
const FRAME_HDR_LEN_V1: usize = 12;

const FRAME_MAGIC_V2: u32 = u32::from_le_bytes(*b"YW2L");
const FRAME_HDR_LEN_V2: usize = 16;

/// On-disk frame layout, fixed by the file magic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrameFormat {
    /// `magic | len | crc32(payload) | payload`.
    ///
    /// The checksum covers only the payload, so a damaged `len` that grows past
    /// EOF is indistinguishable from a torn final frame: the bytes after the
    /// header are then read as a short payload. This format is replayed for
    /// files written before the header checksum existed.
    V1,
    /// `magic | len | crc32(payload) | crc32(magic || len || payload_crc) | payload`.
    ///
    /// The header checksums itself, so a damaged `len` is reported as
    /// corruption instead of being mistaken for a torn tail. That distinction
    /// is the whole point: truncating at a damaged `len` silently discards
    /// every acknowledged frame after it, including later `HardState` frames.
    V2,
}

impl FrameFormat {
    fn from_wal_magic(magic: &[u8; 8]) -> Option<Self> {
        if magic == WAL_MAGIC_V1 {
            Some(Self::V1)
        } else if magic == WAL_MAGIC {
            Some(Self::V2)
        } else {
            None
        }
    }

    fn hdr_len(self) -> usize {
        match self {
            Self::V1 => FRAME_HDR_LEN_V1,
            Self::V2 => FRAME_HDR_LEN_V2,
        }
    }

    fn frame_magic(self) -> u32 {
        match self {
            Self::V1 => FRAME_MAGIC_V1,
            Self::V2 => FRAME_MAGIC_V2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    HardState {
        term: u64,
        voted_for: Option<u32>,
    },
    AppendEntries {
        from_index: u64,
        entries: Vec<LogEntry>,
    },
}

#[derive(Debug, Default)]
pub struct RecoveredWal {
    pub term: u64,
    pub voted_for: Option<u32>,
    pub log: Vec<LogEntry>,
}

/// Durable Raft Write-Ahead Log backed by `StorageFs` (`OsFs` or `SimFs`).
pub struct RaftWal<FS: StorageFs> {
    file: FS::File,
    format: FrameFormat,
    write_pos: u64,
    sync_writes: bool,
    failed: bool,
}

impl<FS: StorageFs> RaftWal<FS> {
    pub fn open(fs: &FS, dir: &str, sync_writes: bool) -> Result<(Self, RecoveredWal), MetaError> {
        fs.create_dir_all(dir)?;
        let path = if dir.is_empty() {
            "raft.wal".to_string()
        } else {
            format!("{}/raft.wal", dir.trim_end_matches('/'))
        };

        let exists = fs.exists(&path);
        let file = fs.open_rw(&path, true)?;
        if !exists || file.len()? == 0 {
            file.set_len(0)?;
            file.write_at(0, WAL_MAGIC)?;
            if sync_writes {
                file.sync_data()?;
                fs.sync_dir(dir)?;
            }
            return Ok((
                Self {
                    file,
                    format: FrameFormat::V2,
                    write_pos: WAL_MAGIC.len() as u64,
                    sync_writes,
                    failed: false,
                },
                RecoveredWal::default(),
            ));
        }

        let format = read_wal_magic(&file)?;
        let (recovered, valid_end) = replay_wal(&file, format)?;
        if sync_writes {
            // A process-only restart retains page cache, including records
            // whose previous sync failed. Reading valid bytes is not evidence
            // of durability. Establish it before Raft can acknowledge matches.
            file.sync_data()?;
            fs.sync_dir(dir)?;
        }
        Ok((
            Self {
                file,
                format,
                write_pos: valid_end,
                sync_writes,
                failed: false,
            },
            recovered,
        ))
    }

    pub fn ensure_healthy(&self) -> Result<(), MetaError> {
        if self.failed {
            return Err(MetaError::StorageUnavailable);
        }
        Ok(())
    }

    pub fn append(&mut self, record: &WalRecord) -> Result<(), MetaError> {
        self.ensure_healthy()?;
        let payload = serde_json::to_vec(record)?;
        let frame = encode_frame(self.format, &payload);

        let pos = self.write_pos;
        if let Err(e) = self.file.write_at(pos, &frame) {
            // Raft may already have changed its in-memory term/vote/log. Do
            // not let a retry acknowledge that state without durable evidence.
            self.failed = true;
            let _ = self.file.set_len(pos);
            return Err(MetaError::Io(e));
        }
        if self.sync_writes {
            if let Err(e) = self.file.sync_data() {
                self.failed = true;
                return Err(e.into());
            }
        }
        self.write_pos = pos + frame.len() as u64;
        Ok(())
    }
}

fn read_wal_magic<F: VirtualFile>(file: &F) -> Result<FrameFormat, MetaError> {
    if file.len()? < WAL_MAGIC_V1.len() as u64 {
        return Err(MetaError::CorruptWal("wal shorter than magic header"));
    }
    let mut magic = [0u8; 8];
    if file.read_at(0, &mut magic)? < magic.len() {
        return Err(MetaError::CorruptWal("wal shorter than magic header"));
    }
    FrameFormat::from_wal_magic(&magic).ok_or(MetaError::CorruptWal("invalid wal magic"))
}

fn encode_frame(format: FrameFormat, payload: &[u8]) -> Vec<u8> {
    let payload_len = payload.len() as u32;
    let payload_crc = crc32fast::hash(payload);
    let mut frame = Vec::with_capacity(format.hdr_len() + payload.len());
    frame.extend_from_slice(&format.frame_magic().to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&payload_crc.to_le_bytes());
    if format == FrameFormat::V2 {
        // Covers magic, len and payload_crc: the framing fields validate
        // themselves, so a damaged len can never masquerade as a torn tail.
        let hdr_crc = crc32fast::hash(&frame[..FRAME_HDR_LEN_V1]);
        frame.extend_from_slice(&hdr_crc.to_le_bytes());
    }
    frame.extend_from_slice(payload);
    frame
}

enum FrameOutcome {
    /// A fully validated record whose frame ends at `next_pos`.
    Record { next_pos: u64, record: WalRecord },
    /// The final bytes are an incomplete frame that was never acknowledged.
    TornTail,
}

/// Replays the WAL prefix, distinguishing a torn tail from real corruption.
///
/// `append` writes one whole frame at the current end of the file and only
/// advances `write_pos` after the write (and, when configured, its
/// `sync_data`) succeeds. Appends are strictly sequential, so an incomplete
/// frame can only be the final bytes of the file, and it was never
/// acknowledged: discarding it loses nothing.
///
/// A frame that is present in full but fails validation is *not* a tail.
/// Truncating there would silently drop acknowledged entries after it and any
/// later `HardState`, rewinding `current_term`/`voted_for` (which permits
/// double-voting in a term). Such damage is reported as `CorruptWal` and the
/// caller refuses to start.
///
/// Truncation is therefore limited to the cases where the frame provably runs
/// past the end of the file:
///
/// * fewer bytes remain than a header needs, or a short read at EOF;
/// * a `V2` frame whose self-checking header is intact but whose payload never
///   reached the file;
/// * a `V1` frame whose declared length runs past EOF. A `V1` header is not
///   checksummed, so this case is ambiguous between a torn tail and a damaged
///   `len`; it keeps the legacy truncate behaviour for existing files, and
///   `V2` removes the ambiguity for every frame written from now on.
fn replay_wal<F: VirtualFile>(
    file: &F,
    format: FrameFormat,
) -> Result<(RecoveredWal, u64), MetaError> {
    let total_len = file.len()?;
    let mut out = RecoveredWal::default();
    let mut pos = WAL_MAGIC.len() as u64;

    while pos < total_len {
        match read_frame(file, pos, total_len, format)? {
            FrameOutcome::Record { next_pos, record } => {
                match record {
                    WalRecord::HardState { term, voted_for } => {
                        out.term = term;
                        out.voted_for = voted_for;
                    }
                    WalRecord::AppendEntries {
                        from_index,
                        entries,
                    } => {
                        // `from_index` is 1-based Raft log index
                        let keep = (from_index.saturating_sub(1)) as usize;
                        if keep < out.log.len() {
                            out.log.truncate(keep);
                        }
                        out.log.extend(entries);
                    }
                }
                pos = next_pos;
            }
            FrameOutcome::TornTail => {
                truncate_torn_tail(file, pos)?;
                break;
            }
        }
    }

    Ok((out, pos))
}

fn read_frame<F: VirtualFile>(
    file: &F,
    pos: u64,
    total_len: u64,
    format: FrameFormat,
) -> Result<FrameOutcome, MetaError> {
    let hdr_len = format.hdr_len();
    // Fewer bytes remain than a header needs. Only the final frame can be
    // short, so this is a provable torn tail.
    if total_len - pos < hdr_len as u64 {
        return Ok(FrameOutcome::TornTail);
    }

    let mut hdr = [0u8; FRAME_HDR_LEN_V2];
    if file.read_at(pos, &mut hdr[..hdr_len])? < hdr_len {
        // Short read at EOF: same provable torn tail.
        return Ok(FrameOutcome::TornTail);
    }

    let fmagic = u32::from_le_bytes(hdr[0..4].try_into().expect("4-byte magic"));
    if fmagic != format.frame_magic() {
        // A complete header is present and a torn write can only leave a
        // *prefix* of a frame (so a short magic, not a wrong one). Damage to a
        // frame that may be followed by acknowledged records is corruption.
        return Err(MetaError::CorruptWal("invalid frame magic"));
    }

    if format == FrameFormat::V2 {
        let stored_hdr_crc = u32::from_le_bytes(hdr[12..16].try_into().expect("4-byte hdr crc"));
        if crc32fast::hash(&hdr[..FRAME_HDR_LEN_V1]) != stored_hdr_crc {
            // The framing bytes damaged themselves. A frame cut short by a torn
            // write is caught by the length checks above, so this cannot be a
            // tail: report it instead of discarding later acknowledged frames.
            return Err(MetaError::CorruptWal("frame header checksum mismatch"));
        }
    }

    let len = u32::from_le_bytes(hdr[4..8].try_into().expect("4-byte len")) as usize;
    let expected_crc = u32::from_le_bytes(hdr[8..12].try_into().expect("4-byte crc"));
    let next_pos = pos + hdr_len as u64 + len as u64;
    if next_pos > total_len {
        // The frame runs past EOF, so only its leading bytes exist and no
        // acknowledged record can be hiding in them. For `V2` the header
        // checksum proved `len` is intact, so the payload simply never reached
        // disk. For `V1` this is the legacy ambiguous case documented above.
        return Ok(FrameOutcome::TornTail);
    }

    let mut payload = vec![0u8; len];
    if file.read_at(pos + hdr_len as u64, &mut payload)? < len {
        // The declared payload lies inside the file, so the bytes exist but the
        // read returned fewer. The file changed underneath us; that is not a
        // torn tail and must not be truncated.
        return Err(MetaError::CorruptWal("frame payload shorter than declared"));
    }
    if crc32fast::hash(&payload) != expected_crc {
        return Err(MetaError::CorruptWal("frame payload checksum mismatch"));
    }
    let record: WalRecord = serde_json::from_slice(&payload)
        .map_err(|_| MetaError::CorruptWal("frame payload is not a WalRecord"))?;
    Ok(FrameOutcome::Record { next_pos, record })
}

/// Removes a provably incomplete final frame. Callers must have established
/// that `pos` is the start of the file's last, never-acknowledged bytes.
fn truncate_torn_tail<F: VirtualFile>(file: &F, pos: u64) -> Result<(), MetaError> {
    file.set_len(pos)?;
    file.sync_data()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvCommand;
    use yadb_io::SimFs;

    const DIR: &str = "meta";
    const PATH: &str = "meta/raft.wal";

    fn hard_state(term: u64, voted_for: Option<u32>) -> WalRecord {
        WalRecord::HardState { term, voted_for }
    }

    fn entry(index: u64) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            cmd: KvCommand::Put {
                key: format!("k{index}"),
                value: vec![index as u8],
            },
        }
    }

    fn append(from_index: u64, entries: Vec<LogEntry>) -> WalRecord {
        WalRecord::AppendEntries {
            from_index,
            entries,
        }
    }

    fn encode(record: &WalRecord) -> Vec<u8> {
        encode_frame(
            FrameFormat::V2,
            &serde_json::to_vec(record).expect("record serializes"),
        )
    }

    /// Writes a WAL file directly, bypassing `RaftWal::append`, so tests control
    /// frame layout and damage precisely.
    fn write_wal(fs: &SimFs, magic: &[u8; 8], frames: &[Vec<u8>]) -> u64 {
        fs.create_dir_all(DIR).unwrap();
        let file = fs.open_rw(PATH, true).unwrap();
        let mut bytes = magic.to_vec();
        for frame in frames {
            bytes.extend_from_slice(frame);
        }
        file.write_at(0, &bytes).unwrap();
        file.sync_data().unwrap();
        bytes.len() as u64
    }

    fn write_frames(fs: &SimFs, frames: &[Vec<u8>]) -> u64 {
        write_wal(fs, WAL_MAGIC, frames)
    }

    fn file_len(fs: &SimFs) -> u64 {
        fs.open_rw(PATH, false).unwrap().len().unwrap()
    }

    fn file_bytes(fs: &SimFs) -> Vec<u8> {
        let file = fs.open_rw(PATH, false).unwrap();
        let mut buf = vec![0u8; file.len().unwrap() as usize];
        file.read_at(0, &mut buf).unwrap();
        buf
    }

    fn assert_corrupt(result: Result<(RaftWal<SimFs>, RecoveredWal), MetaError>) {
        match result {
            Err(MetaError::CorruptWal(_)) => {}
            Err(other) => panic!("expected CorruptWal, got {other:?}"),
            Ok(_) => panic!("expected CorruptWal, but open succeeded"),
        }
    }

    #[test]
    fn acked_appends_round_trip_in_order() {
        let fs = SimFs::new(3200);
        let (mut wal, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 0);
        assert!(recovered.log.is_empty());

        wal.append(&hard_state(1, Some(1))).unwrap();
        for index in 1..=3 {
            wal.append(&append(index, vec![entry(index)])).unwrap();
        }
        wal.append(&hard_state(2, Some(3))).unwrap();
        drop(wal);

        let (wal, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 2);
        assert_eq!(recovered.voted_for, Some(3));
        assert_eq!(
            recovered.log,
            vec![entry(1), entry(2), entry(3)],
            "a clean WAL must recover every acknowledged append"
        );
        drop(wal);

        // A second reopen must observe the same durable state.
        let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 2);
        assert_eq!(recovered.log, vec![entry(1), entry(2), entry(3)]);
    }

    /// S1 loss test: five acknowledged appends, one payload bit flip in frame 3.
    /// The old reader truncated at the bad frame and returned `Ok` with two
    /// entries; the only safe answer is to refuse to start.
    #[test]
    fn mid_file_payload_corruption_refuses_to_start() {
        let fs = SimFs::new(3201);
        let (mut wal, _) = RaftWal::open(&fs, DIR, true).unwrap();
        let mut starts = Vec::new();
        for index in 1..=5 {
            starts.push(file_len(&fs));
            wal.append(&append(index, vec![entry(index)])).unwrap();
        }
        drop(wal);
        let len = file_len(&fs);

        // Flip one byte of frame 3's payload. Frames 4 and 5 follow it, so this
        // cannot be a torn tail.
        fs.corrupt_byte(PATH, (starts[2] + FRAME_HDR_LEN_V2 as u64) as usize)
            .unwrap();
        let damaged = file_bytes(&fs);

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(
            file_bytes(&fs),
            damaged,
            "corruption must never be repaired by truncating acknowledged records"
        );
        assert_eq!(file_len(&fs), len);
    }

    /// S1 term/vote regression: the `HardState` after the damaged frame is
    /// acknowledged. A reader that truncates at frame 3 recovers the older
    /// term/vote and can vote twice in term 5.
    #[test]
    fn corrupt_frame_before_hard_state_does_not_rewind_term() {
        let fs = SimFs::new(3202);
        let (mut wal, _) = RaftWal::open(&fs, DIR, true).unwrap();
        let mut starts = Vec::new();
        for record in [
            hard_state(1, Some(1)),
            append(1, vec![entry(1)]),
            append(2, vec![entry(2)]),
            hard_state(5, Some(2)),
        ] {
            starts.push(file_len(&fs));
            wal.append(&record).unwrap();
        }
        drop(wal);
        fs.corrupt_byte(PATH, (starts[2] + FRAME_HDR_LEN_V2 as u64) as usize)
            .unwrap();
        let damaged = file_bytes(&fs);

        match RaftWal::open(&fs, DIR, true) {
            Ok((_, recovered)) => panic!(
                "reopened with term {} vote {:?} after mid-file damage; the durable \
                 state was term 5 vote Some(2)",
                recovered.term, recovered.voted_for
            ),
            Err(MetaError::CorruptWal(_)) => {}
            Err(other) => panic!("expected CorruptWal, got {other:?}"),
        }
        assert_eq!(
            file_bytes(&fs),
            damaged,
            "a rewound term must not be produced by truncating the WAL"
        );
    }

    /// A genuine torn tail is still truncated: fewer bytes than a header, a
    /// complete header with no payload, and a header with a short payload.
    #[test]
    fn torn_tail_is_truncated_and_the_wal_stays_appendable() {
        for cut in [5usize, FRAME_HDR_LEN_V2, FRAME_HDR_LEN_V2 + 4] {
            let fs = SimFs::new(3203);
            let (mut wal, _) = RaftWal::open(&fs, DIR, true).unwrap();
            wal.append(&hard_state(3, Some(1))).unwrap();
            wal.append(&append(1, vec![entry(1), entry(2)])).unwrap();
            drop(wal);
            let valid_len = file_len(&fs);

            // Simulate a torn final write: only `cut` bytes of the frame ever
            // reached the file, and the frame was never acknowledged.
            let torn = encode(&hard_state(9, None));
            let file = fs.open_rw(PATH, false).unwrap();
            file.write_at(valid_len, &torn[..cut]).unwrap();
            file.sync_data().unwrap();
            assert_eq!(file_len(&fs), valid_len + cut as u64);

            let (mut wal, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
            assert_eq!(recovered.term, 3, "the torn frame must not be applied");
            assert_eq!(recovered.log, vec![entry(1), entry(2)]);
            assert_eq!(
                file_len(&fs),
                valid_len,
                "the unacknowledged torn bytes must be removed"
            );

            // The recovered WAL must accept new appends at the truncation point.
            wal.append(&hard_state(4, Some(2))).unwrap();
            wal.append(&append(3, vec![entry(3)])).unwrap();
            drop(wal);

            let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
            assert_eq!(recovered.term, 4);
            assert_eq!(recovered.voted_for, Some(2));
            assert_eq!(recovered.log, vec![entry(1), entry(2), entry(3)]);
        }
    }

    /// Bad magic in the middle is corruption, not a tail: the frame is followed
    /// by acknowledged records, and the file must be left byte-identical.
    #[test]
    fn mid_file_bad_magic_is_not_repaired() {
        let fs = SimFs::new(3204);
        let frames = [
            hard_state(1, Some(1)),
            append(1, vec![entry(1)]),
            hard_state(2, Some(2)),
        ]
        .map(|record| encode(&record));
        write_frames(&fs, &frames);

        let second_frame = WAL_MAGIC.len() as u64 + frames[0].len() as u64;
        fs.corrupt_byte(PATH, second_frame as usize).unwrap();
        let damaged = file_bytes(&fs);

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(
            file_bytes(&fs),
            damaged,
            "no set_len may run for mid-file damage"
        );
    }

    /// A CRC flip in the middle is corruption, not a tail.
    #[test]
    fn mid_file_crc_flip_is_not_repaired() {
        let fs = SimFs::new(3205);
        let frames = [
            hard_state(1, Some(1)),
            append(1, vec![entry(1)]),
            append(2, vec![entry(2)]),
            hard_state(3, Some(3)),
        ]
        .map(|record| encode(&record));
        write_frames(&fs, &frames);

        let third_frame = WAL_MAGIC.len() as u64 + frames[0].len() as u64 + frames[1].len() as u64;
        fs.corrupt_byte(PATH, (third_frame + FRAME_HDR_LEN_V2 as u64) as usize)
            .unwrap();
        let damaged = file_bytes(&fs);

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(file_bytes(&fs), damaged);
    }

    /// A complete, checksum-valid frame whose payload is not a `WalRecord` (an
    /// older or newer variant) is corruption, not a torn tail.
    #[test]
    fn mid_file_undecodable_record_is_corruption() {
        let fs = SimFs::new(3206);
        let frames = vec![
            encode(&hard_state(1, Some(1))),
            encode_frame(FrameFormat::V2, br#"{"LegacyVariant":{"term":7}}"#),
            encode(&hard_state(2, Some(2))),
        ];
        write_frames(&fs, &frames);
        let len = file_len(&fs);

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(file_len(&fs), len);
    }

    /// A complete, checksum-valid but undecodable frame at the tail is still a
    /// complete frame: it was written and acknowledged as a whole.
    #[test]
    fn undecodable_record_at_tail_is_corruption() {
        let fs = SimFs::new(3207);
        let frames = vec![
            encode(&hard_state(1, Some(1))),
            encode_frame(FrameFormat::V2, br#"{"LegacyVariant":{"term":7}}"#),
        ];
        write_frames(&fs, &frames);
        let len = file_len(&fs);

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(file_len(&fs), len);
    }

    /// The v2 header checksum exists for this case: a damaged `len` that grows
    /// past EOF has the exact shape of a torn tail. Without the header checksum
    /// the reader would truncate and drop every acknowledged frame after it.
    #[test]
    fn corrupted_length_is_corruption_not_a_torn_tail() {
        let fs = SimFs::new(3208);
        let (mut wal, _) = RaftWal::open(&fs, DIR, true).unwrap();
        let mut starts = Vec::new();
        for record in [
            hard_state(1, Some(1)),
            append(1, vec![entry(1)]),
            append(2, vec![entry(2)]),
            hard_state(4, Some(2)),
        ] {
            starts.push(file_len(&fs));
            wal.append(&record).unwrap();
        }
        drop(wal);
        let len = file_len(&fs);

        // Flip the top byte of frame 2's `len` so the frame appears to run far
        // past EOF.
        fs.corrupt_byte(PATH, (starts[1] + 7) as usize).unwrap();
        let mut hdr = [0u8; FRAME_HDR_LEN_V2];
        fs.open_rw(PATH, false)
            .unwrap()
            .read_at(starts[1], &mut hdr)
            .unwrap();
        let declared = u32::from_le_bytes(hdr[4..8].try_into().unwrap()) as u64;
        assert!(
            starts[1] + FRAME_HDR_LEN_V2 as u64 + declared > len,
            "the damaged length must look like a frame running past EOF; only the \
             v2 header checksum distinguishes it from a torn tail"
        );
        let damaged = file_bytes(&fs);

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(
            file_bytes(&fs),
            damaged,
            "a damaged length is not a torn tail"
        );
    }

    /// New WALs use the self-checking format, and the file magic says so, which
    /// is what makes an older binary fail loudly instead of truncating.
    #[test]
    fn new_wal_uses_the_self_checking_format() {
        let fs = SimFs::new(3209);
        let (mut wal, _) = RaftWal::open(&fs, DIR, true).unwrap();
        wal.append(&hard_state(1, Some(1))).unwrap();
        drop(wal);

        let bytes = file_bytes(&fs);
        assert_eq!(&bytes[..8], WAL_MAGIC);
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            FRAME_MAGIC_V2
        );

        let mut unknown = *WAL_MAGIC;
        unknown[7] = b'9';
        write_wal(&fs, &unknown, &[]);
        assert_corrupt(RaftWal::open(&fs, DIR, true));
    }

    /// Legacy files keep their legacy behaviour: payload corruption still fails
    /// closed, a torn tail is still truncated, and appends stay in the file's
    /// original format.
    #[test]
    fn legacy_v1_wal_replays_with_the_same_tail_rule() {
        let v1 = |record: &WalRecord| {
            encode_frame(
                FrameFormat::V1,
                &serde_json::to_vec(record).expect("record serializes"),
            )
        };

        // Clean replay and in-format append.
        let fs = SimFs::new(3210);
        write_wal(
            &fs,
            WAL_MAGIC_V1,
            &[v1(&hard_state(3, Some(1))), v1(&append(1, vec![entry(1)]))],
        );
        let (mut wal, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 3);
        assert_eq!(recovered.log, vec![entry(1)]);
        wal.append(&append(2, vec![entry(2)])).unwrap();
        drop(wal);
        let bytes = file_bytes(&fs);
        assert_eq!(&bytes[..8], WAL_MAGIC_V1, "the file format must not change");
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            FRAME_MAGIC_V1
        );
        let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.log, vec![entry(1), entry(2)]);

        // A torn v1 tail (complete header, short payload) is still a tail.
        let fs = SimFs::new(3211);
        let torn = v1(&hard_state(9, None));
        let valid_len = write_wal(&fs, WAL_MAGIC_V1, &[v1(&hard_state(3, Some(1)))]);
        let file = fs.open_rw(PATH, false).unwrap();
        file.write_at(valid_len, &torn[..FRAME_HDR_LEN_V1 + 4])
            .unwrap();
        file.sync_data().unwrap();
        drop(file);
        let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 3);
        assert_eq!(file_len(&fs), valid_len);

        // Mid-file payload corruption in a v1 file is corruption.
        let fs = SimFs::new(3212);
        let frames = [
            v1(&hard_state(1, Some(1))),
            v1(&append(1, vec![entry(1)])),
            v1(&hard_state(2, Some(2))),
        ];
        write_wal(&fs, WAL_MAGIC_V1, &frames);
        let second = WAL_MAGIC_V1.len() as u64 + frames[0].len() as u64;
        fs.corrupt_byte(PATH, (second + FRAME_HDR_LEN_V1 as u64) as usize)
            .unwrap();
        let damaged = file_bytes(&fs);
        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(file_bytes(&fs), damaged);
    }
}
