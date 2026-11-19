use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

mod sys;

pub type IoResult<T> = Result<T, io::Error>;

/// Positional file handle suitable for concurrent reads and append-only volume writes.
pub trait VirtualFile: Send + Sync {
    fn write_at(&self, offset: u64, buf: &[u8]) -> IoResult<()>;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize>;
    /// Read exactly `buf.len()` bytes at `offset`, refusing a short file.
    ///
    /// A positional read may legally return fewer bytes than requested, so a
    /// partial read is retried from where it stopped. A read of zero bytes
    /// before `buf` is full means the file ends early: this returns
    /// [`io::ErrorKind::UnexpectedEof`] rather than a shorter, silently valid
    /// record. This is the validated read an open-time recovery path should
    /// use; [`read_at`](Self::read_at) alone cannot tell a truncated record
    /// from a complete shorter one.
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<()> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.read_at(offset + filled as u64, &mut buf[filled..])? {
                0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "file ended before the requested bytes were read",
                    ))
                }
                read => filled += read,
            }
        }
        Ok(())
    }

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

/// Advisory exclusive lock on an existing directory inode, held until dropped.
/// All owners must cooperate and lock the same directory. The directory must not
/// be renamed, removed or replaced while in use; network filesystems and hostile
/// filesystem mutation are outside this primitive's contract. This does not
/// create or sync directories and does not lock legacy users of `OsFs`.
/// Forked children must not use an inherited descriptor as independent lock ownership;
/// dropping this wrapper explicitly releases the lock, including for such descriptors.
pub struct OsDirectoryLock {
    _directory: File,
}

impl OsDirectoryLock {
    pub fn try_acquire(path: impl AsRef<Path>) -> IoResult<Self> {
        let directory = sys::open(path.as_ref())?;
        if !directory.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "lock target is not a directory",
            ));
        }
        sys::try_lock(&directory)?;
        Ok(Self {
            _directory: directory,
        })
    }
}

impl Drop for OsDirectoryLock {
    fn drop(&mut self) {
        // On Linux the lock belongs to the open file description, which may also
        // be referenced by a descriptor inherited across fork. Closing this File
        // alone would retain the lock until every such descriptor is closed.
        let _ = sys::unlock(&self._directory);
    }
}

impl OsFs {
    pub fn new(root: impl AsRef<Path>) -> IoResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Lock an existing directory under this filesystem root. Callers must use a
    /// trusted, normalized relative path; do not accept `..` from remote input.
    pub fn try_lock_directory(&self, dir: &str) -> IoResult<OsDirectoryLock> {
        if Path::new(dir)
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
            && !dir.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory path must be relative and normalized",
            ));
        }
        OsDirectoryLock::try_acquire(self.resolve(dir))
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
        sys::read_at(&self.file, offset, buf)
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

/// A failed sync has an unknown persistence outcome, not rollback semantics.
/// These endpoints exercise both no persistence and full persistence before EIO;
/// they do not model arbitrary partial writeback or real device behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncFault {
    #[default]
    None,
    BeforePersist,
    AfterPersist,
}

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
    /// Persistent fault mode for every file-data sync until configuration changes.
    pub sync_data_fault: SyncFault,
    /// Persistent fault mode for directory-entry sync (including rename/create).
    pub sync_dir_fault: SyncFault,
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
        self.inode
            .lock()
            .unwrap()
            .page_cache
            .resize(size as usize, 0);
        Ok(())
    }

    fn sync_data(&self) -> IoResult<()> {
        let fault = self.fs.lock().unwrap().faults.sync_data_fault;
        if fault == SyncFault::BeforePersist {
            return Err(io::Error::other(
                "simulated sync_data failure before persistence",
            ));
        }
        let mut inode = self.inode.lock().unwrap();
        inode.durable = inode.page_cache.clone();
        if fault == SyncFault::AfterPersist {
            return Err(io::Error::other(
                "simulated sync_data failure after persistence",
            ));
        }
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
        let fault = st.faults.sync_dir_fault;
        if fault == SyncFault::BeforePersist {
            return Err(io::Error::other(
                "simulated sync_dir failure before persistence",
            ));
        }
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
        if fault == SyncFault::AfterPersist {
            return Err(io::Error::other(
                "simulated sync_dir failure after persistence",
            ));
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

    fn tmp_dir(tag: &str) -> PathBuf {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = base.join(format!("yadb-io-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn write_record(fs: &OsFs, path: &str, record: &[u8]) {
        let file = fs.open_rw(path, true).unwrap();
        file.write_at(0, record).unwrap();
        file.sync_data().unwrap();
    }

    #[test]
    fn lock_acquisition_failure_is_distinguishable_and_leaves_nothing_behind() {
        let dir = tmp_dir("lock-fail");
        fs::create_dir(&dir).unwrap();

        // The next try_lock fails as if another holder won the race.
        let armed = sys::fault::lock(&[sys::fault::LockFault::TryLock(io::ErrorKind::WouldBlock)]);
        let error = OsDirectoryLock::try_acquire(&dir)
            .err()
            .expect("an injected try_lock failure must be reported");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        // The fault is one-shot: the same thread can acquire immediately.
        drop(OsDirectoryLock::try_acquire(&dir).unwrap());
        drop(armed);

        // A failed acquisition creates no lock file, marker or partial state.
        assert!(fs::read_dir(&dir).unwrap().next().is_none());

        // A failed open is a different, distinguishable failure and also leaves
        // the directory usable.
        let armed =
            sys::fault::lock(&[sys::fault::LockFault::Open(io::ErrorKind::PermissionDenied)]);
        let error = OsDirectoryLock::try_acquire(&dir)
            .err()
            .expect("an injected open failure must be reported");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        drop(armed);
        assert!(fs::read_dir(&dir).unwrap().next().is_none());
        drop(OsDirectoryLock::try_acquire(&dir).unwrap());

        // A missing target and a non-directory target are refused with kinds
        // that cannot be confused with a held lock.
        let missing = dir.join("missing");
        assert_eq!(
            OsDirectoryLock::try_acquire(&missing)
                .err()
                .expect("missing target")
                .kind(),
            io::ErrorKind::NotFound
        );
        let file = dir.join("file");
        fs::write(&file, b"not a directory").unwrap();
        assert_eq!(
            OsDirectoryLock::try_acquire(&file)
                .err()
                .expect("file target")
                .kind(),
            io::ErrorKind::InvalidInput
        );

        // `OsFs` refuses a path that is not relative and normalized before it
        // reaches the filesystem.
        let fs_root = tmp_dir("lock-normalize");
        let os_fs = OsFs::new(&fs_root).unwrap();
        assert_eq!(
            os_fs
                .try_lock_directory("../escape")
                .err()
                .expect("unnormalized path")
                .kind(),
            io::ErrorKind::InvalidInput
        );

        fs::remove_file(&file).unwrap();
        fs::remove_dir(&dir).unwrap();
        fs::remove_dir_all(&fs_root).unwrap();
    }

    #[test]
    fn held_lock_refuses_a_second_acquirer_without_disturbing_the_holder() {
        let dir = tmp_dir("lock-held");
        fs::create_dir(&dir).unwrap();
        let holder = OsDirectoryLock::try_acquire(&dir).unwrap();
        for _ in 0..2 {
            let error = OsDirectoryLock::try_acquire(&dir)
                .err()
                .expect("a held lock must refuse a second acquirer");
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            // The refusal neither released nor invalidated the holder.
            assert!(holder._directory.metadata().unwrap().is_dir());
            assert!(dir.is_dir());
        }
        drop(holder);
        let reacquired = OsDirectoryLock::try_acquire(&dir).unwrap();
        drop(reacquired);
        fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn truncated_recovery_read_is_refused_not_accepted_as_a_shorter_file() {
        let root = tmp_dir("truncated-read");
        let os_fs = OsFs::new(&root).unwrap();
        let record = b"0123456789abcdef";
        write_record(&os_fs, "state", record);

        // Control: the complete record reads back through the validated read.
        let file = os_fs.open_rw("state", false).unwrap();
        let mut buf = [0u8; 16];
        file.read_exact_at(0, &mut buf).unwrap();
        assert_eq!(&buf, record);
        drop(file);

        // A read that stops after 8 bytes is what a truncated file looks like.
        // The raw read exposes the hazard: it happily returns 8 bytes.
        let file = os_fs.open_rw("state", false).unwrap();
        let armed = sys::fault::reads(&[sys::fault::ReadFault::Truncate(8)]);
        let mut raw = [0u8; 16];
        assert_eq!(file.read_at(0, &mut raw).unwrap(), 8);
        // The validated read refuses it instead of accepting 8 valid bytes as
        // the whole record.
        let mut buf = [0u8; 16];
        let error = file
            .read_exact_at(0, &mut buf)
            .err()
            .expect("a truncated read must be refused");
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        drop(armed);
        drop(file);

        // The injection did not touch the file: a later open sees every byte.
        let file = os_fs.open_rw("state", false).unwrap();
        assert_eq!(file.len().unwrap(), record.len() as u64);
        let mut buf = [0u8; 16];
        file.read_exact_at(0, &mut buf).unwrap();
        assert_eq!(&buf, record);
        drop(file);

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn simfs_short_file_is_refused_by_the_validated_read() {
        let sim = SimFs::new(7);
        let file = sim.open_rw("state", true).unwrap();
        file.write_at(0, b"0123456789abcdef").unwrap();
        file.set_len(8).unwrap();
        let mut buf = [0u8; 16];
        let error = file
            .read_exact_at(0, &mut buf)
            .err()
            .expect("a short file must be refused");
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn mid_read_failure_is_refused_and_a_later_open_is_deterministic() {
        let root = tmp_dir("mid-read");
        let os_fs = OsFs::new(&root).unwrap();
        let record: Vec<u8> = (0..64).collect();
        write_record(&os_fs, "state", &record);

        // A partial read followed by an error is a failure in the middle of a
        // record, not a short record.
        let file = os_fs.open_rw("state", false).unwrap();
        let armed = sys::fault::reads(&[
            sys::fault::ReadFault::Partial(16),
            sys::fault::ReadFault::Error(io::ErrorKind::Other),
        ]);
        let mut buf = vec![0u8; record.len()];
        let error = file
            .read_exact_at(0, &mut buf)
            .err()
            .expect("a failed read must be refused");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        drop(armed);
        drop(file);

        // The failure changed nothing, and repeated opens agree byte for byte.
        for _ in 0..2 {
            let file = os_fs.open_rw("state", false).unwrap();
            assert_eq!(file.len().unwrap(), record.len() as u64);
            let mut buf = vec![0u8; record.len()];
            file.read_exact_at(0, &mut buf).unwrap();
            assert_eq!(buf, record);
        }

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn directory_lock_child() {
        if let Some(path) = std::env::var_os("YADB_DIRECTORY_LOCK_TEST") {
            assert!(OsDirectoryLock::try_acquire(path).is_err());
        }
    }

    #[test]
    fn directory_lock_excludes_other_handles_and_processes() {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = base.join(format!("yadb-directory-lock-{}", std::process::id()));
        fs::create_dir(&dir).unwrap();
        let lock = OsDirectoryLock::try_acquire(&dir).unwrap();
        assert!(OsDirectoryLock::try_acquire(&dir).is_err());
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::directory_lock_child"])
            .env("YADB_DIRECTORY_LOCK_TEST", &dir)
            .status()
            .unwrap();
        assert!(status.success());
        drop(lock);
        let reacquired = OsDirectoryLock::try_acquire(&dir).unwrap();
        drop(reacquired);
        fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn directory_lock_drop_unlocks_even_with_duplicated_descriptor() {
        let base = std::env::var_os("YADB_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = base.join(format!("yadb-directory-lock-dup-{}", std::process::id()));
        fs::create_dir(&dir).unwrap();
        let lock = OsDirectoryLock::try_acquire(&dir).unwrap();
        // A clone shares the open file description, as a fork-inherited fd does.
        let duplicate = lock._directory.try_clone().unwrap();
        assert!(
            matches!(OsDirectoryLock::try_acquire(&dir), Err(e) if e.kind() == io::ErrorKind::WouldBlock)
        );
        drop(lock);
        let reacquired = OsDirectoryLock::try_acquire(&dir).unwrap();
        assert!(
            matches!(OsDirectoryLock::try_acquire(&dir), Err(e) if e.kind() == io::ErrorKind::WouldBlock)
        );
        drop(duplicate);
        drop(reacquired);
        fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn sync_data_error_does_not_imply_persistence_or_rollback() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(44);
            let file = fs.open_rw("file", true).unwrap();
            file.write_at(0, b"old").unwrap();
            file.sync_data().unwrap();
            fs.sync_dir("").unwrap();
            file.write_at(0, b"new").unwrap();
            fs.set_faults(FaultConfig {
                sync_data_fault: fault,
                ..Default::default()
            });
            assert!(file.sync_data().is_err());
            fs.simulate_power_cut();
            let recovered = fs.open_rw("file", false).unwrap();
            let mut bytes = [0; 3];
            recovered.read_at(0, &mut bytes).unwrap();
            assert_eq!(
                &bytes,
                if fault == SyncFault::AfterPersist {
                    b"new"
                } else {
                    b"old"
                }
            );
        }
    }

    #[test]
    fn sync_dir_error_can_leave_either_side_of_rename_durable() {
        for fault in [SyncFault::BeforePersist, SyncFault::AfterPersist] {
            let fs = SimFs::new(45);
            let file = fs.open_rw("old", true).unwrap();
            file.write_at(0, b"data").unwrap();
            file.sync_data().unwrap();
            fs.sync_dir("").unwrap();
            fs.rename_atomic("old", "new").unwrap();
            fs.set_faults(FaultConfig {
                sync_dir_fault: fault,
                ..Default::default()
            });
            assert!(fs.sync_dir("").is_err());
            fs.simulate_power_cut();
            assert_eq!(fs.exists("new"), fault == SyncFault::AfterPersist);
            assert_eq!(fs.exists("old"), fault == SyncFault::BeforePersist);
        }
    }

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
        fs.set_faults(FaultConfig {
            write_eio_prob: 1.0,
            ..Default::default()
        });
        assert!(file
            .write_at(0, b"replacement-longer-than-original")
            .is_err());
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
