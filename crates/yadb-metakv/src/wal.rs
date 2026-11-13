use serde::{Deserialize, Serialize};
use yadb_io::{StorageFs, VirtualFile};

use crate::{LogEntry, MetaError};

const WAL_MAGIC: &[u8; 8] = b"YADBWAL1";
const FRAME_MAGIC: u32 = 0x5957_414C; // "YWAL"
const FRAME_HDR_LEN: usize = 12;

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
        let mut frame = Vec::with_capacity(FRAME_HDR_LEN + payload.len());
        frame.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
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
    let mut hdr = [0u8; FRAME_HDR_LEN];

    while pos < total_len {
        if pos + FRAME_HDR_LEN as u64 > total_len {
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }
        let n = file.read_at(pos, &mut hdr)?;
        if n < FRAME_HDR_LEN {
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }
        let fmagic = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        let len = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]) as usize;
        let expected_crc = u32::from_le_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]);

        let next_pos = pos + FRAME_HDR_LEN as u64 + len as u64;
        if fmagic != FRAME_MAGIC || next_pos > total_len {
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }

        let mut payload = vec![0u8; len];
        file.read_at(pos + FRAME_HDR_LEN as u64, &mut payload)?;
        if crc32fast::hash(&payload) != expected_crc {
            file.set_len(pos)?;
            file.sync_data()?;
            break;
        }

        let rec: WalRecord = match serde_json::from_slice(&payload) {
            Ok(r) => r,
            Err(_) => {
                file.set_len(pos)?;
                file.sync_data()?;
                break;
            }
        };

        match rec {
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

    Ok((out, pos))
}
