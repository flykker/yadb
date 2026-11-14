use serde::{Deserialize, Serialize};
use yadb_io::{StorageFs, VirtualFile};

use crate::{LogEntry, MetaError};

const WAL_MAGIC: &[u8; 8] = b"YADBWAL1";

/// Legacy frame: `magic | len | crc32(payload) | payload`. The checksum does not
/// cover the framing fields, so a corrupted `len` is indistinguishable from a
/// torn tail. Still replayed so existing files keep opening.
const FRAME_MAGIC_V1: u32 = 0x5957_414C; // "YWAL"
const FRAME_HDR_LEN_V1: usize = 12;

/// Current frame: `magic | len | payload_crc | header_crc | payload`, where
/// `header_crc = crc32(magic || len || payload_crc)`. A distinct magic means a
/// reader never has to guess which checksum layout a frame uses. Because the
/// framing fields validate themselves, a corrupted `len` is reported as
/// corruption instead of being mistaken for an incomplete final frame.
const FRAME_MAGIC_V2: u32 = u32::from_le_bytes(*b"YW2L");
const FRAME_HDR_LEN_V2: usize = 16;

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
                    write_pos: WAL_MAGIC.len() as u64,
                    sync_writes,
                    failed: false,
                },
                RecoveredWal::default(),
            ));
        }

        let (recovered, valid_end) = replay_wal(&file)?;
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
        let crc = crc32fast::hash(&payload);
        let hdr = v2_header(payload.len(), crc);
        let mut frame = Vec::with_capacity(FRAME_HDR_LEN_V2 + payload.len());
        frame.extend_from_slice(&hdr);
        frame.extend_from_slice(&payload);

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

/// Encodes a v2 frame header. The header checksum covers every other header
/// field, so `len` cannot be silently damaged into a value that merely looks
/// like an incomplete payload at the end of the file.
fn v2_header(payload_len: usize, payload_crc: u32) -> [u8; FRAME_HDR_LEN_V2] {
    let mut hdr = [0u8; FRAME_HDR_LEN_V2];
    hdr[0..4].copy_from_slice(&FRAME_MAGIC_V2.to_le_bytes());
    hdr[4..8].copy_from_slice(&(payload_len as u32).to_le_bytes());
    hdr[8..12].copy_from_slice(&payload_crc.to_le_bytes());
    let header_crc = crc32fast::hash(&hdr[..12]);
    hdr[12..16].copy_from_slice(&header_crc.to_le_bytes());
    hdr
}

enum FrameOutcome {
    /// A fully validated record whose frame ends at `next_pos`.
    Record { next_pos: u64, record: WalRecord },
    /// The final bytes are an incomplete frame that was never acknowledged.
    TornTail,
}

fn replay_wal<F: VirtualFile>(file: &F) -> Result<(RecoveredWal, u64), MetaError> {
    let total_len = file.len()?;
    if total_len < WAL_MAGIC.len() as u64 {
        return Err(MetaError::CorruptWal("wal shorter than magic header"));
    }
    let mut magic = [0u8; 8];
    file.read_at(0, &mut magic)?;
    if &magic != WAL_MAGIC {
        return Err(MetaError::CorruptWal("invalid wal magic"));
    }

    let mut out = RecoveredWal::default();
    let mut pos = WAL_MAGIC.len() as u64;

    // `append` writes one whole frame at the current end of the file and only
    // advances `write_pos` after the write (and, when configured, its
    // `sync_data`) succeeds. Appends are strictly sequential, so an incomplete
    // frame can only be the final bytes of the file, and it was never
    // acknowledged. Anything with a complete, frame-shaped header that fails
    // validation and is followed by more bytes is corruption: silently
    // dropping it would discard acknowledged records and could rewind the
    // `HardState` term/vote, which violates Raft safety.
    while pos < total_len {
        match read_frame(file, pos, total_len)? {
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
                truncate_tail(file, pos)?;
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
) -> Result<FrameOutcome, MetaError> {
    let remaining = total_len - pos;
    // Neither frame layout fits in the remaining bytes, and only the final
    // frame can be short. Provable torn tail.
    if remaining < FRAME_HDR_LEN_V1 as u64 {
        return Ok(FrameOutcome::TornTail);
    }

    let mut hdr = [0u8; FRAME_HDR_LEN_V2];
    let n = file.read_at(pos, &mut hdr[..FRAME_HDR_LEN_V1])?;
    if n < FRAME_HDR_LEN_V1 {
        return Ok(FrameOutcome::TornTail);
    }
    let fmagic = u32::from_le_bytes(hdr[0..4].try_into().expect("4-byte magic"));

    let (hdr_len, payload_len, expected_crc, next_pos) = match fmagic {
        FRAME_MAGIC_V1 => {
            let len = u32::from_le_bytes(hdr[4..8].try_into().expect("4-byte len")) as usize;
            let crc = u32::from_le_bytes(hdr[8..12].try_into().expect("4-byte crc"));
            let next = pos + FRAME_HDR_LEN_V1 as u64 + len as u64;
            if next > total_len {
                // A v1 header carries no checksum over `len`, so a length
                // damaged by bit rot looks exactly like a torn final frame
                // here. Truncating would silently discard acknowledged entries
                // and could rewind `HardState`; losing a whole replica is the
                // lesser harm, so fail closed. The one overrun that cannot be
                // corruption of acknowledged data is a write that reached
                // fresh, never-written space without its bytes: the region
                // after the header is then all zeroes, and an acknowledged
                // payload is JSON, never all zeroes.
                if tail_is_zero(file, pos + FRAME_HDR_LEN_V1 as u64, total_len)? {
                    return Ok(FrameOutcome::TornTail);
                }
                return Err(MetaError::CorruptWal(
                    "v1 frame length runs past EOF and cannot be validated",
                ));
            }
            (FRAME_HDR_LEN_V1, len, crc, next)
        }
        FRAME_MAGIC_V2 => {
            if remaining < FRAME_HDR_LEN_V2 as u64 {
                // A v2 header was cut short by a torn write. Only the final
                // frame can be short, and it was never acknowledged.
                return Ok(FrameOutcome::TornTail);
            }
            let n = file.read_at(pos, &mut hdr)?;
            if n < FRAME_HDR_LEN_V2 {
                return Ok(FrameOutcome::TornTail);
            }
            let stored_crc = u32::from_le_bytes(hdr[12..16].try_into().expect("4-byte hdr crc"));
            if crc32fast::hash(&hdr[..12]) != stored_crc {
                // The header damaged its own checksum, and a frame cut short by
                // a torn write is caught above by the length check. Damage to
                // framing bytes of a frame that is followed by acknowledged
                // records must never be read as a tail.
                return Err(MetaError::CorruptWal("v2 frame header checksum mismatch"));
            }
            let len = u32::from_le_bytes(hdr[4..8].try_into().expect("4-byte len")) as usize;
            let crc = u32::from_le_bytes(hdr[8..12].try_into().expect("4-byte crc"));
            let next = pos + FRAME_HDR_LEN_V2 as u64 + len as u64;
            if next > total_len {
                // The header checksum verified, so `len` is intact and the
                // payload simply never reached disk. Torn, unacknowledged tail.
                return Ok(FrameOutcome::TornTail);
            }
            (FRAME_HDR_LEN_V2, len, crc, next)
        }
        _ => {
            // An unknown magic at a position that held at least a full legacy
            // header. Only a zero-filled remainder is a torn write into fresh
            // space; anything else is corruption of an acknowledged frame.
            if tail_is_zero(file, pos, total_len)? {
                return Ok(FrameOutcome::TornTail);
            }
            return Err(MetaError::CorruptWal("invalid frame magic"));
        }
    };

    let mut payload = vec![0u8; payload_len];
    let n = file.read_at(pos + hdr_len as u64, &mut payload)?;
    if n < payload_len {
        // The declared payload lies inside the file, so a short read means the
        // file changed underneath us. Never treat that as a torn tail: a torn
        // frame is only ever the final bytes and never has a validated header.
        return Err(MetaError::CorruptWal("frame payload shorter than declared"));
    }
    if crc32fast::hash(&payload) != expected_crc {
        return Err(MetaError::CorruptWal("frame payload checksum mismatch"));
    }
    let record: WalRecord = serde_json::from_slice(&payload)
        .map_err(|_| MetaError::CorruptWal("frame payload is not a WalRecord"))?;
    Ok(FrameOutcome::Record { next_pos, record })
}

fn truncate_tail<F: VirtualFile>(file: &F, pos: u64) -> Result<(), MetaError> {
    file.set_len(pos)?;
    file.sync_data()?;
    Ok(())
}

/// True when `[from, total_len)` is entirely zero bytes. A torn write can leave
/// the file length extended over blocks that were never filled; those bytes are
/// zeros and are safe to discard. Damage to acknowledged data is not zero-filled
/// in practice, so this distinguishes the two without guessing.
fn tail_is_zero<F: VirtualFile>(file: &F, from: u64, total_len: u64) -> Result<bool, MetaError> {
    let mut buf = [0u8; 4096];
    let mut pos = from;
    while pos < total_len {
        let want = ((total_len - pos) as usize).min(buf.len());
        let n = file.read_at(pos, &mut buf[..want])?;
        if n == 0 {
            break;
        }
        if buf[..n].iter().any(|&b| b != 0) {
            return Ok(false);
        }
        pos += n as u64;
    }
    Ok(true)
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

    fn append(index: u64, entries: Vec<LogEntry>) -> WalRecord {
        WalRecord::AppendEntries {
            from_index: index,
            entries,
        }
    }

    fn encode_v2(record: &WalRecord) -> Vec<u8> {
        let payload = serde_json::to_vec(record).unwrap();
        let mut frame = v2_header(payload.len(), crc32fast::hash(&payload)).to_vec();
        frame.extend_from_slice(&payload);
        frame
    }

    fn encode_v1(record: &WalRecord) -> Vec<u8> {
        let payload = serde_json::to_vec(record).unwrap();
        let mut frame = Vec::new();
        frame.extend_from_slice(&FRAME_MAGIC_V1.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// Writes a WAL file directly, bypassing `RaftWal::append`, so tests control
    /// frame layout and damage precisely.
    fn write_frames(fs: &SimFs, frames: &[Vec<u8>]) -> u64 {
        fs.create_dir_all(DIR).unwrap();
        let file = fs.open_rw(PATH, true).unwrap();
        let mut bytes = WAL_MAGIC.to_vec();
        for frame in frames {
            bytes.extend_from_slice(frame);
        }
        file.write_at(0, &bytes).unwrap();
        file.sync_data().unwrap();
        bytes.len() as u64
    }

    fn file_len(fs: &SimFs) -> u64 {
        fs.open_rw(PATH, false).unwrap().len().unwrap()
    }

    fn assert_corrupt(result: Result<(RaftWal<SimFs>, RecoveredWal), MetaError>) {
        match result {
            Err(MetaError::CorruptWal(_)) => {}
            Err(other) => panic!("expected CorruptWal, got {other:?}"),
            Ok(_) => panic!("expected CorruptWal, but open succeeded"),
        }
    }

    #[test]
    fn v2_round_trip_recovers_records_in_order() {
        let fs = SimFs::new(3100);
        let (mut wal, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 0);
        assert!(recovered.log.is_empty());

        for record in [
            hard_state(1, Some(1)),
            append(1, vec![entry(1), entry(2)]),
            hard_state(2, Some(3)),
            append(3, vec![entry(3)]),
        ] {
            wal.append(&record).unwrap();
        }
        drop(wal);

        let (wal, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 2);
        assert_eq!(recovered.voted_for, Some(3));
        assert_eq!(recovered.log, vec![entry(1), entry(2), entry(3)]);
        drop(wal);

        // A second reopen must observe the same durable state.
        let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 2);
        assert_eq!(recovered.log, vec![entry(1), entry(2), entry(3)]);
    }

    #[test]
    fn mid_file_payload_corruption_is_reported_and_not_truncated() {
        let fs = SimFs::new(3101);
        let frames: Vec<Vec<u8>> = [
            hard_state(1, Some(1)),
            append(1, vec![entry(1)]),
            append(2, vec![entry(2)]),
            hard_state(4, Some(2)),
        ]
        .iter()
        .map(encode_v2)
        .collect();
        let len = write_frames(&fs, &frames);

        // Flip one payload byte inside frame 2 of 4. The frame is followed by
        // more bytes, so this cannot be a torn tail.
        let second_payload =
            WAL_MAGIC.len() as u64 + frames[0].len() as u64 + FRAME_HDR_LEN_V2 as u64;
        fs.corrupt_byte(PATH, second_payload as usize).unwrap();

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(
            file_len(&fs),
            len,
            "corruption must never truncate the file; that would drop acknowledged records"
        );
    }

    #[test]
    fn corrupted_v2_length_is_corruption_not_a_torn_tail() {
        let fs = SimFs::new(3102);
        let frames: Vec<Vec<u8>> = [
            hard_state(1, Some(1)),
            append(1, vec![entry(1)]),
            append(2, vec![entry(2)]),
            append(3, vec![entry(3)]),
        ]
        .iter()
        .map(encode_v2)
        .collect();
        let len = write_frames(&fs, &frames);

        // Flip the top byte of frame 2's `len`. Without the header checksum the
        // decoded length would run past EOF, which is exactly the shape of a
        // torn tail -- and the old reader would have truncated the log there.
        let second_frame = WAL_MAGIC.len() as u64 + frames[0].len() as u64;
        fs.corrupt_byte(PATH, (second_frame + 7) as usize).unwrap();

        let mut hdr = [0u8; FRAME_HDR_LEN_V2];
        fs.open_rw(PATH, false)
            .unwrap()
            .read_at(second_frame, &mut hdr)
            .unwrap();
        let decoded_len = u32::from_le_bytes(hdr[4..8].try_into().unwrap()) as u64;
        assert!(
            second_frame + FRAME_HDR_LEN_V2 as u64 + decoded_len > len,
            "the corrupted length must look like a frame running past EOF; \
             only the v2 header checksum distinguishes it from a torn tail"
        );

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(file_len(&fs), len, "a corrupted length is not a torn tail");
    }

    #[test]
    fn torn_tail_is_truncated_and_the_wal_stays_appendable() {
        for keep_payload_bytes in [0usize, 5] {
            let fs = SimFs::new(3103);
            let (mut wal, _) = RaftWal::open(&fs, DIR, true).unwrap();
            wal.append(&hard_state(3, Some(1))).unwrap();
            wal.append(&append(1, vec![entry(1), entry(2)])).unwrap();
            drop(wal);
            let valid_len = file_len(&fs);

            // Simulate a torn final write: a complete header plus at most part
            // of its payload, never acknowledged.
            let torn = encode_v2(&hard_state(9, None));
            let cut = FRAME_HDR_LEN_V2 + keep_payload_bytes;
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

    #[test]
    fn hard_state_after_mid_file_corruption_is_not_silently_rewound() {
        let fs = SimFs::new(3104);
        let frames: Vec<Vec<u8>> = [
            hard_state(5, Some(1)),
            append(1, vec![entry(1)]),
            hard_state(9, Some(2)),
        ]
        .iter()
        .map(encode_v2)
        .collect();
        let len = write_frames(&fs, &frames);

        // Damage the final HardState payload: term 9/vote 2 is acknowledged and
        // must never be replaced by the earlier term 5/vote 1.
        let last_payload = WAL_MAGIC.len() as u64
            + frames[0].len() as u64
            + frames[1].len() as u64
            + FRAME_HDR_LEN_V2 as u64;
        fs.corrupt_byte(PATH, last_payload as usize).unwrap();

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(file_len(&fs), len);
    }

    #[test]
    fn v1_frames_still_replay_and_accept_v2_appends() {
        let fs = SimFs::new(3105);
        write_frames(
            &fs,
            &[
                encode_v1(&hard_state(3, Some(1))),
                encode_v1(&append(1, vec![entry(1)])),
                encode_v1(&hard_state(4, Some(2))),
            ],
        );

        let (mut wal, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 4);
        assert_eq!(recovered.voted_for, Some(2));
        assert_eq!(recovered.log, vec![entry(1)]);

        // Upgrade in place: new records are v2, appended after the v1 prefix.
        wal.append(&append(2, vec![entry(2)])).unwrap();
        wal.append(&hard_state(5, Some(3))).unwrap();
        drop(wal);

        let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 5);
        assert_eq!(recovered.voted_for, Some(3));
        assert_eq!(recovered.log, vec![entry(1), entry(2)]);
    }

    #[test]
    fn v1_length_overrun_is_corruption_and_the_ambiguity_is_documented() {
        // A v1 header checksums only the payload, so an overrun is genuinely
        // ambiguous: a damaged `len` looks exactly like a torn final frame.
        // Truncation could drop acknowledged entries and rewind HardState, so
        // the reader fails closed unless the bytes after the header are all
        // zero (see below).
        let fs = SimFs::new(3106);
        let first = encode_v1(&hard_state(7, Some(1)));
        let second = encode_v1(&hard_state(8, Some(2)));
        let len = write_frames(&fs, &[first, second]);
        // Top byte of the first frame's `len`: the declared payload now runs
        // far past EOF, the shape a torn tail would have.
        fs.corrupt_byte(PATH, (WAL_MAGIC.len() + 7) as usize)
            .unwrap();

        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(file_len(&fs), len, "a v1 overrun is not provably torn");

        // Header plus a *nonzero* partial payload is the truly ambiguous v1
        // shape: it may be a torn write, or a corrupted `len` over a payload
        // that is really complete. v1 cannot tell, so it reports corruption
        // rather than risk dropping acknowledged bytes.
        let fs = SimFs::new(3107);
        let valid = encode_v1(&hard_state(7, Some(1)));
        let torn = encode_v1(&hard_state(8, Some(2)));
        let cut = FRAME_HDR_LEN_V1 + 4;
        let len = write_frames(&fs, &[valid, torn[..cut].to_vec()]);
        assert_corrupt(RaftWal::open(&fs, DIR, true));
        assert_eq!(file_len(&fs), len);

        // A header with nothing at all after it is still provable: the declared
        // payload bytes are absent, so no acknowledged record can be hiding
        // there. Discarding the header loses nothing.
        let fs = SimFs::new(3110);
        let valid = encode_v1(&hard_state(7, Some(1)));
        let torn = encode_v1(&hard_state(8, Some(2)));
        let len = write_frames(&fs, &[valid, torn[..FRAME_HDR_LEN_V1].to_vec()]);
        let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 7);
        assert_eq!(
            file_len(&fs),
            len - FRAME_HDR_LEN_V1 as u64,
            "the payload-less v1 header is discarded"
        );

        // A zero-filled remainder is the other overrun v1 can rule out as data
        // loss: a torn write that extended the file over never-written space.
        let fs = SimFs::new(3108);
        let len = write_frames(&fs, &[encode_v1(&hard_state(7, Some(1)))]);
        let file = fs.open_rw(PATH, false).unwrap();
        file.write_at(len, &[0u8; 64]).unwrap();
        file.sync_data().unwrap();
        let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 7);
        assert_eq!(file_len(&fs), len, "the zero-filled tail is discarded");
    }

    #[test]
    fn zeroed_tail_after_a_v2_frame_is_treated_as_a_torn_write() {
        let fs = SimFs::new(3109);
        let frames = vec![encode_v2(&hard_state(2, Some(1)))];
        let len = write_frames(&fs, &frames);
        let file = fs.open_rw(PATH, false).unwrap();
        file.write_at(len, &[0u8; 32]).unwrap();
        file.sync_data().unwrap();

        let (_, recovered) = RaftWal::open(&fs, DIR, true).unwrap();
        assert_eq!(recovered.term, 2);
        assert_eq!(file_len(&fs), len);
    }
}
