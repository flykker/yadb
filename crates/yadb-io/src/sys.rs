//! The syscall boundary used by [`crate::OsDirectoryLock`] and
//! [`crate::OsFile`].
//!
//! Each function here is the `std` call it names; in production this module
//! adds a call and nothing else. It exists so unit tests can reach syscalls
//! the public API only issues internally. Under `cfg(test)` a test arms a
//! one-shot fault for the next matching call on its own thread (see
//! [`fault::lock`] and [`fault::reads`]); nothing is armed by default, the
//! fault tables are thread-local, and the guard clears them on drop. The same
//! seam is compiled into dependent crates' test builds through the
//! `test-faults` feature, which re-exports it as [`crate::fault`].
//!
//! The seam deliberately covers only what the open-time path uses: opening and
//! locking the directory, and positional reads. It does not model a device, a
//! page cache or power loss; an injected error proves the code path that sees
//! it, not what a disk would have stored.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub(crate) fn open(path: &Path) -> io::Result<File> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(kind) = fault::take_lock(fault::LockOp::Open) {
        return Err(io::Error::new(kind, "injected lock open fault"));
    }
    File::open(path)
}

pub(crate) fn try_lock(file: &File) -> io::Result<()> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(kind) = fault::take_lock(fault::LockOp::TryLock) {
        return Err(io::Error::new(kind, "injected try_lock fault"));
    }
    file.try_lock().map_err(io::Error::from)
}

pub(crate) fn unlock(file: &File) -> io::Result<()> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(kind) = fault::take_lock(fault::LockOp::Unlock) {
        return Err(io::Error::new(kind, "injected unlock fault"));
    }
    file.unlock()
}

pub(crate) fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    #[cfg(any(test, feature = "test-faults"))]
    if let Some(result) = fault::inject_read(file, offset, buf) {
        return result;
    }
    file.read_at(buf, offset)
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
    #[derive(Debug, Clone, Copy)]
    pub enum LockFault {
        Open(io::ErrorKind),
        TryLock(io::ErrorKind),
        Unlock(io::ErrorKind),
    }

    impl LockFault {
        fn op(self) -> LockOp {
            match self {
                Self::Open(_) => LockOp::Open,
                Self::TryLock(_) => LockOp::TryLock,
                Self::Unlock(_) => LockOp::Unlock,
            }
        }

        fn kind(self) -> io::ErrorKind {
            match self {
                Self::Open(kind) | Self::TryLock(kind) | Self::Unlock(kind) => kind,
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
    }

    thread_local! {
        static LOCKS: RefCell<Vec<LockFault>> = const { RefCell::new(Vec::new()) };
        static READS: RefCell<Vec<ReadFault>> = const { RefCell::new(Vec::new()) };
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

    pub(crate) fn take_lock(op: LockOp) -> Option<io::ErrorKind> {
        LOCKS.with(|faults| {
            let mut faults = faults.borrow_mut();
            let index = faults.iter().position(|fault| fault.op() == op)?;
            Some(faults.remove(index).kind())
        })
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
        let fault = READS.with(|faults| {
            let mut faults = faults.borrow_mut();
            if faults.is_empty() {
                None
            } else {
                Some(faults.remove(0))
            }
        })?;
        Some(match fault {
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
