//! The syscall boundary used by [`crate::OsDirectoryLock`], [`crate::OsFile`]
//! and [`crate::OsFs`].
//!
//! Each function here is the `std` call it names; in production this module
//! adds a call and nothing else. It exists so unit tests can reach syscalls
//! the public API only issues internally. Under `cfg(test)` a test arms a
//! fault for the next matching call on its own thread (see [`fault::lock`],
//! [`fault::reads`], [`fault::writes`], [`fault::file_syncs`],
//! [`fault::dir_syncs`] and [`fault::renames`]); nothing is armed by default,
//! the fault tables are thread-local, and the guard clears them on drop. The
//! same seam is compiled into dependent crates' test builds through the
//! `test-faults` feature, which re-exports it as [`crate::fault`].
//!
//! Faults are consumed in order per operation, and every operation has a
//! `Pass` slot, so a sequence can put the failure on the second call rather
//! than the first: `writes(&[Pass, Error(Other)])` lets one write land and
//! fails the next. That is what makes a cross-operation sequence (a second
//! append, a file sync followed by a read) expressible at all. The seam does
//! not model a device, a page cache or power loss; an injected error proves the
//! code path that sees it, not what a disk would have stored.

use std::fs::File;
use std::io;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub fn open(path: &Path) -> io::Result<File> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(fault::LockFault::Open(kind)) = fault::take_lock(fault::LockOp::Open) {
        return Err(io::Error::new(kind, "injected lock open fault"));
    }
    File::open(path)
}

pub fn try_lock(file: &File) -> io::Result<()> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(fault::LockFault::TryLock(kind)) = fault::take_lock(fault::LockOp::TryLock) {
        return Err(io::Error::new(kind, "injected try_lock fault"));
    }
    file.try_lock().map_err(io::Error::from)
}

pub fn unlock(file: &File) -> io::Result<()> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(fault::LockFault::Unlock(kind)) = fault::take_lock(fault::LockOp::Unlock) {
        return Err(io::Error::new(kind, "injected unlock fault"));
    }
    file.unlock()
}

pub fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(result) = fault::inject_read(file, offset, buf) {
        return result;
    }
    file.read_at(buf, offset)
}

pub fn write_all(file: &File, buf: &[u8]) -> io::Result<()> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(fault::WriteFault::Error(kind)) = fault::take_write() {
        return Err(io::Error::new(kind, "injected write fault"));
    }
    // `Write` is implemented for `&File`, which is what an `OsFile` holds: the
    // append-mode delta handle is shared by `&self` callers.
    let mut handle = file;
    handle.write_all(buf)
}

pub fn write_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<()> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(fault::WriteFault::Error(kind)) = fault::take_write() {
        return Err(io::Error::new(kind, "injected write fault"));
    }
    file.write_all_at(buf, offset)
}

pub fn sync_data(file: &File) -> io::Result<()> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(fault::FileSyncFault::Error(kind)) = fault::take_file_sync() {
        return Err(io::Error::new(kind, "injected file sync fault"));
    }
    file.sync_data()
}

pub fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(fault::DirSyncFault::Error(kind)) = fault::take_dir_sync() {
        return Err(io::Error::new(kind, "injected directory sync fault"));
    }
    File::open(path)?.sync_all()
}

pub fn rename(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(fault::RenameFault::Error(kind)) = fault::take_rename() {
        return Err(io::Error::new(kind, "injected rename fault"));
    }
    std::fs::rename(from, to)
}

#[cfg(any(test, feature = "test-faults"))]
pub mod fault {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io;
    use std::marker::PhantomData;
    use std::os::fd::{AsRawFd, RawFd};
    use std::os::unix::fs::FileExt;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum LockOp {
        Open,
        TryLock,
        Unlock,
    }

    /// A fault for the next lock syscall of the matching operation.
    ///
    /// `Pass` consumes the slot without changing the call, so a test can place
    /// a fault on the second of two operations instead of the first.
    #[derive(Debug, Clone, Copy)]
    pub enum LockFault {
        Open(io::ErrorKind),
        TryLock(io::ErrorKind),
        Unlock(io::ErrorKind),
        Pass(LockOp),
    }

    impl LockFault {
        fn op(self) -> LockOp {
            match self {
                Self::Open(_) => LockOp::Open,
                Self::TryLock(_) => LockOp::TryLock,
                Self::Unlock(_) => LockOp::Unlock,
                Self::Pass(op) => op,
            }
        }
    }

    /// A fault for the next positional read.
    #[derive(Debug, Clone, Copy)]
    pub enum ReadFault {
        /// The read fails with this error kind and leaves the file untouched.
        Error(io::ErrorKind),
        /// The read returns at most this many bytes. This is a legal short
        /// read: the next read resumes where it stopped, and the file is not
        /// truncated.
        Partial(usize),
        /// The file appears to end this many bytes into the faulted read: this
        /// call returns at most that many bytes and later reads on the same
        /// descriptor stop there. Models a truncated file.
        Truncate(usize),
        /// The read proceeds normally. Consumes the slot so a later fault in
        /// the same sequence lands on the call the test means.
        Pass,
    }

    /// A fault for the next write. `Pass` is the positioning slot described on
    /// [`LockFault`].
    #[derive(Debug, Clone, Copy)]
    pub enum WriteFault {
        Error(io::ErrorKind),
        Pass,
    }

    /// A fault for the next file-data sync.
    #[derive(Debug, Clone, Copy)]
    pub enum FileSyncFault {
        Error(io::ErrorKind),
        Pass,
    }

    /// A fault for the next directory sync.
    #[derive(Debug, Clone, Copy)]
    pub enum DirSyncFault {
        Error(io::ErrorKind),
        Pass,
    }

    /// A fault for the next rename.
    #[derive(Debug, Clone, Copy)]
    pub enum RenameFault {
        Error(io::ErrorKind),
        Pass,
    }

    thread_local! {
        static LOCKS: RefCell<Vec<LockFault>> = const { RefCell::new(Vec::new()) };
        static READS: RefCell<Vec<ReadFault>> = const { RefCell::new(Vec::new()) };
        static WRITES: RefCell<Vec<WriteFault>> = const { RefCell::new(Vec::new()) };
        static FILE_SYNCS: RefCell<Vec<FileSyncFault>> = const { RefCell::new(Vec::new()) };
        static DIR_SYNCS: RefCell<Vec<DirSyncFault>> = const { RefCell::new(Vec::new()) };
        static RENAMES: RefCell<Vec<RenameFault>> = const { RefCell::new(Vec::new()) };
        /// Per-descriptor virtual end of file installed by [`ReadFault::Truncate`].
        static TRUNCATED: RefCell<HashMap<RawFd, u64>> = RefCell::new(HashMap::new());
    }

    /// Clears every armed fault on drop, so one test cannot leak into another.
    ///
    /// Not `Send`: the fault tables are per-thread and the guard must stay on
    /// the thread that armed it.
    pub struct Armed(PhantomData<*const ()>);

    impl Drop for Armed {
        fn drop(&mut self) {
            LOCKS.with(|faults| faults.borrow_mut().clear());
            READS.with(|faults| faults.borrow_mut().clear());
            WRITES.with(|faults| faults.borrow_mut().clear());
            FILE_SYNCS.with(|faults| faults.borrow_mut().clear());
            DIR_SYNCS.with(|faults| faults.borrow_mut().clear());
            RENAMES.with(|faults| faults.borrow_mut().clear());
            TRUNCATED.with(|map| map.borrow_mut().clear());
        }
    }

    /// Arm lock faults for this thread, consumed in order by matching calls.
    pub fn lock(faults: &[LockFault]) -> Armed {
        LOCKS.with(|armed| armed.borrow_mut().extend_from_slice(faults));
        Armed(PhantomData)
    }

    /// Arm read faults for this thread, consumed in order by successive reads.
    pub fn reads(faults: &[ReadFault]) -> Armed {
        READS.with(|armed| armed.borrow_mut().extend_from_slice(faults));
        Armed(PhantomData)
    }

    /// Arm write faults for this thread, consumed in order by successive writes.
    pub fn writes(faults: &[WriteFault]) -> Armed {
        WRITES.with(|armed| armed.borrow_mut().extend_from_slice(faults));
        Armed(PhantomData)
    }

    /// Arm file-data sync faults, consumed in order by successive syncs.
    pub fn file_syncs(faults: &[FileSyncFault]) -> Armed {
        FILE_SYNCS.with(|armed| armed.borrow_mut().extend_from_slice(faults));
        Armed(PhantomData)
    }

    /// Arm directory sync faults, consumed in order by successive syncs.
    pub fn dir_syncs(faults: &[DirSyncFault]) -> Armed {
        DIR_SYNCS.with(|armed| armed.borrow_mut().extend_from_slice(faults));
        Armed(PhantomData)
    }

    /// Arm rename faults, consumed in order by successive renames.
    pub fn renames(faults: &[RenameFault]) -> Armed {
        RENAMES.with(|armed| armed.borrow_mut().extend_from_slice(faults));
        Armed(PhantomData)
    }

    pub(crate) fn take_lock(op: LockOp) -> Option<LockFault> {
        LOCKS.with(|faults| {
            let mut faults = faults.borrow_mut();
            let index = faults.iter().position(|fault| fault.op() == op)?;
            Some(faults.remove(index))
        })
    }

    fn take_from<T: Copy>(faults: &RefCell<Vec<T>>) -> Option<T> {
        let mut faults = faults.borrow_mut();
        if faults.is_empty() {
            None
        } else {
            Some(faults.remove(0))
        }
    }

    pub(crate) fn take_write() -> Option<WriteFault> {
        WRITES.with(take_from)
    }

    pub(crate) fn take_file_sync() -> Option<FileSyncFault> {
        FILE_SYNCS.with(take_from)
    }

    pub(crate) fn take_dir_sync() -> Option<DirSyncFault> {
        DIR_SYNCS.with(take_from)
    }

    pub(crate) fn take_rename() -> Option<RenameFault> {
        RENAMES.with(take_from)
    }

    /// Returns `Some(result)` when a fault (or an installed truncation) applies.
    pub(crate) fn inject_read(
        file: &File,
        offset: u64,
        buf: &mut [u8],
    ) -> Option<io::Result<usize>> {
        let fd = file.as_raw_fd();
        if let Some(end) = TRUNCATED.with(|map| map.borrow().get(&fd).copied()) {
            let available = end.saturating_sub(offset).min(buf.len() as u64) as usize;
            return Some(Ok(available));
        }
        let fault = READS.with(take_from)?;
        Some(match fault {
            ReadFault::Pass => return None,
            ReadFault::Error(kind) => Err(io::Error::new(kind, "injected read fault")),
            ReadFault::Partial(limit) => {
                let limit = limit.min(buf.len());
                file.read_at(&mut buf[..limit], offset)
            }
            ReadFault::Truncate(limit) => {
                let limit = limit.min(buf.len());
                match file.read_at(&mut buf[..limit], offset) {
                    Ok(read) => {
                        TRUNCATED.with(|map| map.borrow_mut().insert(fd, offset + read as u64));
                        Ok(read)
                    }
                    Err(error) => Err(error),
                }
            }
        })
    }
}
