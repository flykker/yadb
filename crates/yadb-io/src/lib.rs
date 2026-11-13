use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub type IoResult<T> = Result<T, io::Error>;

/// Positional file handle suitable for concurrent reads and append-only volume writes.
pub trait VirtualFile: Send + Sync {
    fn write_at(&self, offset: u64, buf: &[u8]) -> IoResult<()>;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize>;
    fn len(&self) -> IoResult<u64>;
    fn is_empty(&self) -> IoResult<bool> {
        Ok(self.len()? == 0)
    }
    fn set_len(&self, size: u64) -> IoResult<()>;
    fn sync_data(&self) -> IoResult<()>;
}

/// Pluggable filesystem interface: backed by Linux ext4/xfs in production or `SimFs` in tests.
pub trait StorageFs: Send + Sync + Clone + 'static {
    type File: VirtualFile;

    fn create_dir_all(&self, dir: &str) -> IoResult<()>;
    fn open_rw(&self, path: &str, create: bool) -> IoResult<Self::File>;
    fn list_files(&self, dir: &str) -> IoResult<Vec<String>>;
    fn remove_file(&self, path: &str) -> IoResult<()>;
    fn rename_atomic(&self, from: &str, to: &str) -> IoResult<()>;
    fn sync_dir(&self, dir: &str) -> IoResult<()>;
    fn exists(&self, path: &str) -> bool;
}

// ============================================================================
// Production Linux OS Filesystem (`OsFs`)
// ============================================================================

#[derive(Clone)]
pub struct OsFs {
    root: PathBuf,
}

impl OsFs {
    pub fn new(root: impl AsRef<Path>) -> IoResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn resolve(&self, rel: &str) -> PathBuf {
        let trimmed = rel.trim_start_matches('/');
        if trimmed.is_empty() {
            self.root.clone()
        } else {
            self.root.join(trimmed)
        }
    }
}

pub struct OsFile {
    file: File,
}

impl VirtualFile for OsFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> IoResult<()> {
        self.file.write_all_at(buf, offset)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        self.file.read_at(buf, offset)
    }

    fn len(&self) -> IoResult<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&self, size: u64) -> IoResult<()> {
        self.file.set_len(size)
    }

    fn sync_data(&self) -> IoResult<()> {
        self.file.sync_data()
    }
}

impl StorageFs for OsFs {
    type File = OsFile;

    fn create_dir_all(&self, dir: &str) -> IoResult<()> {
        fs::create_dir_all(self.resolve(dir))
    }

    fn open_rw(&self, path: &str, create: bool) -> IoResult<Self::File> {
        let full = self.resolve(path);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(full)?;
        Ok(OsFile { file })
    }

    fn list_files(&self, dir: &str) -> IoResult<Vec<String>> {
        let full = self.resolve(dir);
        if !full.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in fs::read_dir(full)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    out.push(name.to_string());
                }
            }
        }
        out.sort();
        Ok(out)
    }

    fn remove_file(&self, path: &str) -> IoResult<()> {
        fs::remove_file(self.resolve(path))
    }

    fn rename_atomic(&self, from: &str, to: &str) -> IoResult<()> {
        let dst = self.resolve(to);
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::rename(self.resolve(from), dst)
    }

    fn sync_dir(&self, dir: &str) -> IoResult<()> {
        let f = File::open(self.resolve(dir))?;
        f.sync_all()
    }

    fn exists(&self, path: &str) -> bool {
        self.resolve(path).exists()
    }
}

// ============================================================================
// Deterministic Fault-Injecting Simulator (`SimFs`)
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct FaultConfig {
    /// Deterministic PRNG state (SplitMix64).
    pub seed: u64,
    /// Probability (0.0..=1.0) that a `write_at` becomes a torn write (writes prefix only + EIO).
    pub torn_write_prob: f64,
    /// Probability (0.0..=1.0) that a `read_at` returns a transient I/O error.
    pub read_eio_prob: f64,
    /// Probability (0.0..=1.0) that a `write_at` fails outright, simulating a
    /// full disk / rejected write. The file content is left untouched.
    pub write_eio_prob: f64,
}

#[derive(Clone, Default)]
struct SimInode {
    /// Volatile OS page cache view.
    page_cache: Vec<u8>,
    /// Durable platter/NVMe state persisted via `sync_data`.
    durable: Vec<u8>,
}

struct SimState {
    files: BTreeMap<String, Arc<Mutex<SimInode>>>,
    durable_dir: BTreeMap<String, Arc<Mutex<SimInode>>>,
    dirs: BTreeSet<String>,
    faults: FaultConfig,
    rng_state: u64,
}

impl SimState {
    fn next_f64(&mut self) -> f64 {
        // SplitMix64 deterministic generator
        self.rng_state = self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z = z ^ (z >> 31);
        (z >> 11) as f64 / ((1u64 << 53) as f64)
    }
}

#[derive(Clone)]
pub struct SimFs {
    inner: Arc<Mutex<SimState>>,
}

impl SimFs {
    pub fn new(seed: u64) -> Self {
        let mut dirs = BTreeSet::new();
        dirs.insert(String::new());
        Self {
            inner: Arc::new(Mutex::new(SimState {
                files: BTreeMap::new(),
                durable_dir: BTreeMap::new(),
                dirs,
                faults: FaultConfig {
                    seed,
                    ..Default::default()
                },
                rng_state: seed,
            })),
        }
    }

    pub fn set_faults(&self, cfg: FaultConfig) {
        let mut guard = self.inner.lock().unwrap();
        guard.rng_state = cfg.seed;
        guard.faults = cfg;
    }

    /// Simulates sudden power loss: discards all unsynced `page_cache` bytes and unsynced renames/creates!
    pub fn simulate_power_cut(&self) {
        let mut guard = self.inner.lock().unwrap();
        let restored: BTreeMap<String, Arc<Mutex<SimInode>>> = guard
            .durable_dir
            .iter()
            .map(|(path, inode_arc)| {
                let inode = inode_arc.lock().unwrap();
                let clean = SimInode {
                    page_cache: inode.durable.clone(),
                    durable: inode.durable.clone(),
                };
                (path.clone(), Arc::new(Mutex::new(clean)))
            })
            .collect();
        guard.files = restored.clone();
        guard.durable_dir = restored;
    }

    /// Corrupts bytes directly on durable storage to test CRC32 & Reed-Solomon self-healing.
    pub fn corrupt_byte(&self, path: &str, offset: usize) -> IoResult<()> {
        let norm = normalize_path(path);
        let guard = self.inner.lock().unwrap();
        let inode = guard
            .files
            .get(&norm)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, norm))?;
        let mut lock = inode.lock().unwrap();
        if offset < lock.page_cache.len() {
            lock.page_cache[offset] ^= 0xFF;
        }
        if offset < lock.durable.len() {
            lock.durable[offset] ^= 0xFF;
        }
        Ok(())
    }
}

pub struct SimFile {
    fs: Arc<Mutex<SimState>>,
    inode: Arc<Mutex<SimInode>>,
}

impl VirtualFile for SimFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> IoResult<()> {
        let torn_len = {
            let mut st = self.fs.lock().unwrap();
            if st.faults.write_eio_prob > 0.0 && st.next_f64() < st.faults.write_eio_prob {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "simulated write failure (disk full)",
                ));
            }
            if st.faults.torn_write_prob > 0.0
                && buf.len() > 1
                && st.next_f64() < st.faults.torn_write_prob
            {
                let frac = st.next_f64();
                let cut = ((buf.len() as f64 * frac) as usize).clamp(1, buf.len() - 1);
                Some(cut)
            } else {
                None
            }
        };

        let mut inode = self.inode.lock().unwrap();
        let slice = match torn_len {
            Some(cut) => &buf[..cut],
            None => buf,
        };
        let end = offset as usize + slice.len();
        if inode.page_cache.len() < end {
            inode.page_cache.resize(end, 0);
        }
        inode.page_cache[offset as usize..end].copy_from_slice(slice);

        if torn_len.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "simulated torn write I/O fault",
            ));
        }
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        {
            let mut st = self.fs.lock().unwrap();
            if st.faults.read_eio_prob > 0.0 && st.next_f64() < st.faults.read_eio_prob {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "simulated disk read EIO",
                ));
            }
        }
        let inode = self.inode.lock().unwrap();
        let off = offset as usize;
        if off >= inode.page_cache.len() {
            return Ok(0);
        }
        let avail = (inode.page_cache.len() - off).min(buf.len());
        buf[..avail].copy_from_slice(&inode.page_cache[off..off + avail]);
        Ok(avail)
    }

    fn len(&self) -> IoResult<u64> {
        Ok(self.inode.lock().unwrap().page_cache.len() as u64)
    }

    fn set_len(&self, size: u64) -> IoResult<()> {
        self.inode.lock().unwrap().page_cache.resize(size as usize, 0);
        Ok(())
    }

    fn sync_data(&self) -> IoResult<()> {
        let mut inode = self.inode.lock().unwrap();
        inode.durable = inode.page_cache.clone();
        Ok(())
    }
}

fn normalize_path(p: &str) -> String {
    p.trim_matches('/').to_string()
}

fn parent_dir(norm: &str) -> &str {
    match norm.rfind('/') {
        Some(idx) => &norm[..idx],
        None => "",
    }
}

impl StorageFs for SimFs {
    type File = SimFile;

    fn create_dir_all(&self, dir: &str) -> IoResult<()> {
        let norm = normalize_path(dir);
        let mut st = self.inner.lock().unwrap();
        let mut cur = String::new();
        st.dirs.insert(String::new());
        for seg in norm.split('/').filter(|s| !s.is_empty()) {
            if !cur.is_empty() {
                cur.push('/');
            }
            cur.push_str(seg);
            st.dirs.insert(cur.clone());
        }
        Ok(())
    }

    fn open_rw(&self, path: &str, create: bool) -> IoResult<Self::File> {
        let norm = normalize_path(path);
        let mut st = self.inner.lock().unwrap();
        let inode = if let Some(existing) = st.files.get(&norm) {
            existing.clone()
        } else if create {
            let created = Arc::new(Mutex::new(SimInode::default()));
            st.files.insert(norm, created.clone());
            created
        } else {
            return Err(io::Error::new(io::ErrorKind::NotFound, norm));
        };
        Ok(SimFile {
            fs: self.inner.clone(),
            inode,
        })
    }

    fn list_files(&self, dir: &str) -> IoResult<Vec<String>> {
        let norm_dir = normalize_path(dir);
        let st = self.inner.lock().unwrap();
        let mut res = Vec::new();
        for key in st.files.keys() {
            if parent_dir(key) == norm_dir {
                let file_name = match key.rfind('/') {
                    Some(idx) => &key[idx + 1..],
                    None => key.as_str(),
                };
                res.push(file_name.to_string());
            }
        }
        res.sort();
        Ok(res)
    }

    fn remove_file(&self, path: &str) -> IoResult<()> {
        let norm = normalize_path(path);
        let mut st = self.inner.lock().unwrap();
        st.files
            .remove(&norm)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, norm))?;
        Ok(())
    }

    fn rename_atomic(&self, from: &str, to: &str) -> IoResult<()> {
        let src = normalize_path(from);
        let dst = normalize_path(to);
        let mut st = self.inner.lock().unwrap();
        let inode = st
            .files
            .remove(&src)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, src))?;
        st.files.insert(dst, inode);
        Ok(())
    }

    fn sync_dir(&self, dir: &str) -> IoResult<()> {
        let norm_dir = normalize_path(dir);
        let mut st = self.inner.lock().unwrap();
        // Persist directory entries belonging to `norm_dir`
        let durable_keys: Vec<String> = st
            .durable_dir
            .keys()
            .filter(|k| parent_dir(k) == norm_dir)
            .cloned()
            .collect();
        for k in durable_keys {
            st.durable_dir.remove(&k);
        }
        let current_entries: Vec<(String, Arc<Mutex<SimInode>>)> = st
            .files
            .iter()
            .filter(|(k, _)| parent_dir(k) == norm_dir)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (k, v) in current_entries {
            st.durable_dir.insert(k, v);
        }
        Ok(())
    }

    fn exists(&self, path: &str) -> bool {
        let norm = normalize_path(path);
        self.inner.lock().unwrap().files.contains_key(&norm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simfs_discards_unsynced_writes_on_power_cut() {
        let fs = SimFs::new(42);
        fs.create_dir_all("vol").unwrap();

        let f = fs.open_rw("vol/seg1.dat", true).unwrap();
        f.write_at(0, b"committed-block").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir("vol").unwrap();

        // Write second block WITHOUT sync_data, then cut power
        f.write_at(15, b"-unflushed-tail").unwrap();
        fs.simulate_power_cut();

        let reopened = fs.open_rw("vol/seg1.dat", false).unwrap();
        assert_eq!(reopened.len().unwrap(), 15);
        let mut buf = vec![0u8; 15];
        reopened.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"committed-block");
    }

    #[test]
    fn rejected_write_changes_neither_volatile_nor_durable_bytes() {
        let fs = SimFs::new(43);
        let file = fs.open_rw("file", true).unwrap();
        file.write_at(0, b"original").unwrap();
        file.sync_data().unwrap();
        fs.sync_dir("").unwrap();
        fs.set_faults(FaultConfig { write_eio_prob: 1.0, ..Default::default() });
        assert!(file.write_at(0, b"replacement-longer-than-original").is_err());
        let mut buf = [0; 8];
        file.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"original");
        fs.simulate_power_cut();
        let recovered = fs.open_rw("file", false).unwrap();
        assert_eq!(recovered.len().unwrap(), 8);
        recovered.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"original");
    }
}
