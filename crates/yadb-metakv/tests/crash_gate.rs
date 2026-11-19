//! A separate-process crash gate for the durable state machine.
//!
//! Every other fault test in this crate injects a *returned error*: the process
//! survives, sees the error, and the code decides what to do. That cannot show
//! what a process death leaves behind, because a dead process rolls nothing
//! back, runs no destructor and flushes nothing. This gate does the real thing
//! with two processes:
//!
//! 1. the parent creates a directory, spawns this same test binary with an
//!    environment variable naming the point to die at, and waits;
//! 2. the child creates a state machine, applies one entry and waits for the
//!    append to be acknowledged, then arms a fault that terminates the process
//!    from inside the next syscall (`fault::WriteFault::Kill` before the write,
//!    `fault::FileSyncFault::Kill` after it) and applies a second entry;
//! 3. the parent reopens the directory with a fresh `DurableMetaStateMachine`
//!    and asserts what survived.
//!
//! The child terminates with `std::process::exit(97)` from inside the seam: no
//! unwinding, no destructors, no buffered-data flush. For the durable file
//! state that is what `SIGKILL` produces — the writes and syncs that already
//! returned are on disk, everything after them is not. It is not a power-loss
//! test: the page cache survives process death, which is exactly why the
//! "after the write, before the sync" case below can assert that the complete
//! frame is replayed.
//!
//! The two cases are deterministic on a running OS, which is what makes them
//! useful: a death before the write leaves the acknowledged prefix, and a death
//! after the write but before the sync leaves a complete, checksum-valid frame
//! that replays. The second entry was never acknowledged, so replaying it is
//! not a lost or duplicated promise — the Raft log holds the same entry, and
//! the state machine is only ever asked to replay what the log already has.

use std::path::{Path, PathBuf};

use openraft::storage::RaftStateMachine;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use yadb_io::fault::{file_syncs, writes, FileSyncFault, WriteFault};
use yadb_metakv::kv::KvCommand;
use yadb_metakv::multiraft::{GroupIdentity, ReplicaIdentity};
use yadb_metakv::openraft_runtime::YadbRaftTypeConfig as C;
use yadb_metakv::openraft_state_machine::DurableMetaStateMachine;

const CHILD_ENV: &str = "YADB_CRASH_GATE_CHILD";
const BEFORE_WRITE: &str = "before-write";
const AFTER_WRITE: &str = "after-write";

fn identity() -> ReplicaIdentity {
    ReplicaIdentity::new(
        GroupIdentity {
            cluster: [77; 16],
            group: 1,
            incarnation: 1,
        },
        1,
        vec![1],
    )
    .unwrap()
}

fn put(index: u64, key: &str, value: &[u8]) -> Entry<C> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(KvCommand::Put {
            key: key.into(),
            value: value.to_vec(),
        }),
    }
}

/// A directory no other test run shares: the child must start from nothing.
fn fresh_dir(label: &str) -> PathBuf {
    let base = std::env::var_os("YADB_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!(
        "yadb-crash-gate-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Spawn this test binary as the child, running only `test_name`, and require
/// that it died at the seam rather than finishing or failing.
fn run_child(mode: &str, dir: &Path, test_name: &str) {
    let exe = std::env::current_exe().expect("test binary path");
    let status = std::process::Command::new(exe)
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_ENV, format!("{mode}|{}", dir.display()))
        .status()
        .expect("spawn the child test process");
    assert_eq!(
        status.code(),
        Some(97),
        "the child must terminate inside the seam, not finish or fail: {status:?}"
    );
}

/// The child half. Never returns: either the fault terminates the process or the
/// test fails loudly because the fault did not fire.
async fn child(mode: String, dir: PathBuf) -> ! {
    let id = identity();
    let mut sm = DurableMetaStateMachine::create(&dir, &id).expect("child create");
    sm.apply(vec![put(1, "a", b"one")])
        .await
        .expect("the acknowledged entry must apply");
    let _armed = match mode.as_str() {
        BEFORE_WRITE => writes(&[WriteFault::Kill]),
        AFTER_WRITE => file_syncs(&[FileSyncFault::Kill]),
        other => panic!("unknown crash-gate mode {other:?}"),
    };
    // Dies inside this apply: before its write, or at its file sync.
    let _ = sm.apply(vec![put(2, "b", b"two")]).await;
    panic!("the armed fault did not fire");
}

/// `None` in the parent, `Some(mode, dir)` in the child.
fn child_spec() -> Option<(String, PathBuf)> {
    let spec = std::env::var(CHILD_ENV).ok()?;
    let (mode, dir) = spec.split_once('|').expect("mode|dir");
    Some((mode.to_string(), PathBuf::from(dir)))
}

fn reopen(dir: &Path) -> DurableMetaStateMachine {
    DurableMetaStateMachine::open(dir, &identity()).expect("the directory must reopen")
}

#[tokio::test]
async fn a_process_death_before_the_write_leaves_only_the_acknowledged_entry() {
    if let Some((mode, dir)) = child_spec() {
        child(mode, dir).await;
    }
    let dir = fresh_dir(BEFORE_WRITE);
    run_child(
        BEFORE_WRITE,
        &dir,
        "a_process_death_before_the_write_leaves_only_the_acknowledged_entry",
    );

    let mut sm = reopen(&dir);
    let snapshot = sm.try_snapshot(&["a".into(), "b".into()], &[]).unwrap();
    assert_eq!(
        snapshot.get("a").expect("the acknowledged entry survives").value,
        b"one"
    );
    assert!(
        snapshot.get("b").is_none(),
        "the entry whose write never happened must not appear"
    );
    let (applied, _) = sm.applied_state().await.unwrap();
    assert_eq!(applied.unwrap().index, 1);
    assert!(
        sm.repaired_delta().is_none(),
        "nothing was written, so there is no torn tail to report"
    );
    assert!(!dir.join("state.json.corrupt").exists());

    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_process_death_after_the_write_before_the_sync_replays_the_complete_frame() {
    if let Some((mode, dir)) = child_spec() {
        child(mode, dir).await;
    }
    let dir = fresh_dir(AFTER_WRITE);
    run_child(
        AFTER_WRITE,
        &dir,
        "a_process_death_after_the_write_before_the_sync_replays_the_complete_frame",
    );

    let mut sm = reopen(&dir);
    let snapshot = sm.try_snapshot(&["a".into(), "b".into()], &[]).unwrap();
    assert_eq!(
        snapshot.get("a").expect("the acknowledged entry survives").value,
        b"one"
    );
    // The frame reached the page cache before the process died, and a process
    // death does not lose the page cache. The frame is complete and its
    // checksum validates, so replay applies it: the entry was never
    // acknowledged, and the Raft log holds the same entry, so this is replay of
    // an unacknowledged but complete record, not a promise that was broken.
    assert_eq!(
        snapshot.get("b").expect("a complete frame replays").value,
        b"two"
    );
    let (applied, _) = sm.applied_state().await.unwrap();
    assert_eq!(applied.unwrap().index, 2);
    assert!(
        sm.repaired_delta().is_none(),
        "a complete frame is not a torn tail: the crash lost nothing it had written"
    );
    assert!(!dir.join("state.json.corrupt").exists());

    std::fs::remove_dir_all(&dir).unwrap();
}
