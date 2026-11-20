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
//!
//! # Modelled power cuts
//!
//! The second half of this file is a separate section that uses the *volatile
//! write-cache model* in `yadb-io`'s fault seam (`yadb_io::fault`). Unlike the
//! process-death cases above, the model does not let unsynced bytes survive:
//! `sys::write_at`/`sys::write_all` stage bytes in a thread-local cache, and
//! only `sys::sync_data`/`sys::sync_dir` publish them to the real file. A cut
//! armed with `power_cuts` discards the staged bytes and staged renames at a
//! chosen seam call and returns `Interrupted` instead of performing it.
//!
//! This proves only what the model states: the tested code distinguishes
//! staged state from synced state at the seam, and a cut at a chosen seam point
//! leaves exactly the state the model says it leaves. It is **not** evidence
//! about a real device, filesystem write ordering, host crashes or power-loss
//! safety on hardware. The child is still a real process: it enables the model,
//! reaches the cut, and exits immediately so nothing after the cut runs.

use std::io::Cursor;
use std::path::{Path, PathBuf};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use yadb_io::fault::{
    file_syncs, power_cuts, volatile_cache, writes, FileSyncFault, PowerCutFault, WriteFault,
};
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

// ===========================================================================
// Modelled power cuts
// ===========================================================================
//
// Everything below runs the same parent/child shape as the process-death cases
// above, with one difference: the child enables `yadb_io::fault::volatile_cache`
// before it touches the store, so `yadb_io::sys` write/sync/rename calls go
// through the volatile write-cache model. Nothing the store writes is visible
// to a real reader until its `sync_data` (or `sync_dir` for a rename) runs, and
// a cut armed with `power_cuts` discards the staged bytes and staged renames at
// a chosen seam call instead of performing it.
//
// The child asserts the seam outcome itself (so a cut that silently did not
// fire cannot pass as a scenario) and then exits immediately, so no store
// operation beyond the one the cut interrupted ever runs.
//
// Two honest caveats about the model, stated once and relied on nowhere else:
//
// 1. The cut returns `Interrupted` to the caller, so the store's own error path
//    runs after it (`append_delta_locked` truncates the delta back to its
//    acknowledged length, `persist_locked` unlinks the temporary file). Those
//    are the same two cleanups a reopen performs anyway, and every assertion
//    below is about what a reopen observes, so the outcome does not depend on
//    them. A real cut would leave `state.json.tmp` behind; `open_image` deletes
//    it either way.
// 2. Because of that truncation, the delta-append case below cannot by itself
//    tell "the cut discarded the staged frame" from "the cut published it and
//    the store's failed-append rollback removed it again": both end with a
//    zero-length delta. It asserts the end-to-end outcome, which two
//    independent mechanisms guarantee. The publication cases are the ones that
//    isolate the model's discard, because a staged rename has no rollback.

/// The child's exit code for a modelled cut. Distinct from the process-death
/// gate's 97 so a run cannot be mistaken for the other section.
const CUT_EXIT: i32 = 98;

/// A cut after the acknowledged frame's sync: the write must survive.
const POWER_ACKED: &str = "power-acked-after-sync";
/// A cut at the frame's sync, before any sync published it: the frame is lost.
const POWER_UNSYNCED: &str = "power-unsynced-before-sync";
/// A cut between the image's temporary-file sync and its rename.
const POWER_IMAGE_BEFORE_RENAME: &str = "power-image-before-rename";
/// A cut between the image's rename and the directory sync that publishes it.
const POWER_IMAGE_BEFORE_DIR_SYNC: &str = "power-image-before-dir-sync";
/// Control: the image publication runs to completion.
const POWER_IMAGE_CONTROL: &str = "power-image-control";
/// A cut between the installed snapshot's temporary-file sync and its rename.
const POWER_SNAPSHOT_BEFORE_RENAME: &str = "power-snapshot-before-rename";
/// A cut between the installed snapshot's rename and its directory sync.
const POWER_SNAPSHOT_BEFORE_DIR_SYNC: &str = "power-snapshot-before-dir-sync";
/// Control: the snapshot publication runs to completion.
const POWER_SNAPSHOT_CONTROL: &str = "power-snapshot-control";

/// Where the child keeps the store it is cutting. The store directory is one
/// level down so the parent can keep captured artefacts beside it without ever
/// adding a file the store's own open path might look at.
fn store_dir(base: &Path) -> PathBuf {
    base.join("state")
}

/// The child half of a modelled-cut case. Never returns normally: it asserts the
/// cut fired, then exits so nothing after the cut runs.
async fn power_child(mode: String, base: PathBuf) -> ! {
    let id = identity();
    let dir = store_dir(&base);
    match mode.as_str() {
        POWER_ACKED => {
            let _cache = volatile_cache();
            let mut sm = DurableMetaStateMachine::create(&dir, &id).expect("child create");
            sm.apply(vec![put(1, "a", b"one")])
                .await
                .expect("the acknowledged entry must apply");
            // An acknowledgement is only meaningful if the bytes really are on
            // disk. The model flushes staged bytes at `sync_data`, so a reader
            // outside the model must already see the frame in the delta file.
            let delta = std::fs::read(dir.join("state.delta")).expect("the delta file");
            assert!(
                !delta.is_empty(),
                "the acknowledged frame must be on disk, not still staged"
            );
            // Cut at the next seam call, which is the next apply's frame write.
            power_cuts(&[PowerCutFault::Cut]);
            let error = sm
                .apply(vec![put(2, "b", b"two")])
                .await
                .expect_err("the armed cut must interrupt the second apply");
            assert!(
                format!("{error}").contains("injected power cut"),
                "the failure must be the modelled cut, not something else: {error}"
            );
            std::process::exit(CUT_EXIT)
        }
        POWER_UNSYNCED => {
            let _cache = volatile_cache();
            let mut sm = DurableMetaStateMachine::create(&dir, &id).expect("child create");
            // The first apply's seam calls, in order: the new delta file's
            // directory sync, the frame write, the frame sync. Pass the first
            // two so the frame is staged, then cut at the sync: the staged
            // bytes are discarded before any of them was published.
            power_cuts(&[
                PowerCutFault::Pass,
                PowerCutFault::Pass,
                PowerCutFault::Cut,
            ]);
            let error = sm
                .apply(vec![put(1, "a", b"one")])
                .await
                .expect_err("the armed cut must interrupt the first apply");
            assert!(
                format!("{error}").contains("injected power cut"),
                "the failure must be the modelled cut, not something else: {error}"
            );
            // The store *refuses* rather than claiming the write: a failed
            // append poisons the handle, and every read on it fails closed.
            assert!(
                sm.try_snapshot(&["a".into()], &[]).is_err(),
                "a handle whose append was cut must refuse a read, not answer as if the key were absent"
            );
            // And the discarded bytes never reached the real file.
            let delta = std::fs::read(dir.join("state.delta")).expect("the delta file exists");
            assert!(
                delta.is_empty(),
                "a cut before the sync must leave no bytes on disk, found {}",
                delta.len()
            );
            std::process::exit(CUT_EXIT)
        }
        POWER_IMAGE_BEFORE_RENAME | POWER_IMAGE_BEFORE_DIR_SYNC | POWER_IMAGE_CONTROL => {
            let _cache = volatile_cache();
            let mut sm = DurableMetaStateMachine::create(&dir, &id).expect("child create");
            sm.apply(vec![put(1, "a", b"one")])
                .await
                .expect("apply before the publication");
            // Capture the image `create` published, so "the old image survived"
            // is a byte-level claim rather than an inference from behaviour.
            let old = std::fs::read(dir.join("state.json")).expect("the published image");
            std::fs::write(base.join("old-image"), &old).expect("capture the old image");
            // `persist_locked` makes exactly four seam calls: write the
            // temporary image, sync it, rename it, sync the directory.
            let steps: &[PowerCutFault] = match mode.as_str() {
                POWER_IMAGE_BEFORE_RENAME => &[
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Cut,
                ],
                POWER_IMAGE_BEFORE_DIR_SYNC => &[
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Cut,
                ],
                _ => &[
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                ],
            };
            power_cuts(steps);
            let outcome = sm.build_snapshot().await;
            if mode == POWER_IMAGE_CONTROL {
                outcome.expect("the control publication must complete");
            } else {
                let error = outcome.expect_err("the armed cut must interrupt the publication");
                assert!(
                    format!("{error}").contains("injected power cut"),
                    "the failure must be the modelled cut, not something else: {error}"
                );
            }
            std::process::exit(CUT_EXIT)
        }
        POWER_SNAPSHOT_BEFORE_RENAME
        | POWER_SNAPSHOT_BEFORE_DIR_SYNC
        | POWER_SNAPSHOT_CONTROL => {
            // Build a real snapshot on a second, uncut store, then install it
            // into the store that gets cut. `install_snapshot` publishes it
            // through the same synced-temporary-file sequence.
            let source_dir = base.join("source");
            let mut source =
                DurableMetaStateMachine::create(&source_dir, &id).expect("source create");
            source
                .apply(vec![put(1, "a", b"one"), put(2, "b", b"two")])
                .await
                .expect("source apply");
            let built = source.build_snapshot().await.expect("source snapshot");
            let meta = built.meta;
            let bytes = (*built.snapshot).into_inner();

            let _cache = volatile_cache();
            let mut target = DurableMetaStateMachine::create(&dir, &id).expect("target create");
            let steps: &[PowerCutFault] = match mode.as_str() {
                POWER_SNAPSHOT_BEFORE_RENAME => &[
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Cut,
                ],
                POWER_SNAPSHOT_BEFORE_DIR_SYNC => &[
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Cut,
                ],
                _ => &[
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                    PowerCutFault::Pass,
                ],
            };
            power_cuts(steps);
            let outcome = target
                .install_snapshot(&meta, Box::new(Cursor::new(bytes)))
                .await;
            if mode == POWER_SNAPSHOT_CONTROL {
                outcome.expect("the control installation must complete");
            } else {
                let error = outcome.expect_err("the armed cut must interrupt the installation");
                assert!(
                    format!("{error}").contains("injected power cut"),
                    "the failure must be the modelled cut, not something else: {error}"
                );
            }
            std::process::exit(CUT_EXIT)
        }
        other => panic!("unknown modelled-cut mode {other:?}"),
    }
}

/// Spawn this test binary as a modelled-cut child and require that it reached
/// the cut and exited with [`CUT_EXIT`].
fn run_power_child(mode: &str, base: &Path, test_name: &str) {
    let exe = std::env::current_exe().expect("test binary path");
    let output = std::process::Command::new(exe)
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_ENV, format!("{mode}|{}", base.display()))
        .output()
        .expect("spawn the child test process");
    assert_eq!(
        output.status.code(),
        Some(CUT_EXIT),
        "the child must reach the modelled cut and stop there, not finish or fail: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// The captured pre-cut image the child left beside the store.
fn captured_old_image(base: &Path) -> Vec<u8> {
    std::fs::read(base.join("old-image")).expect("the child captured the old image")
}

#[tokio::test]
async fn a_modelled_cut_after_the_acknowledged_sync_keeps_the_write() {
    if let Some((mode, dir)) = child_spec() {
        power_child(mode, dir).await;
    }
    let base = fresh_dir(POWER_ACKED);
    run_power_child(
        POWER_ACKED,
        &base,
        "a_modelled_cut_after_the_acknowledged_sync_keeps_the_write",
    );

    // The frame was synced before the cut, so it is in the delta file and the
    // reopen replays it. This is the one case where the modelled cut and a
    // process death agree: bytes that reached the device are still there.
    let mut sm = reopen(&store_dir(&base));
    let snapshot = sm.try_snapshot(&["a".into(), "b".into()], &[]).unwrap();
    assert_eq!(
        snapshot
            .get("a")
            .expect("the acknowledged entry survives a cut after its sync")
            .value,
        b"one"
    );
    assert!(
        snapshot.get("b").is_none(),
        "the entry whose frame write was cut must not appear"
    );
    let (applied, _) = sm.applied_state().await.unwrap();
    assert_eq!(applied.expect("an applied position survives").index, 1);
    assert!(
        sm.repaired_delta().is_none(),
        "a synced frame is not a torn tail"
    );
    assert!(!store_dir(&base).join("state.json.corrupt").exists());

    std::fs::remove_dir_all(&base).unwrap();
}

#[tokio::test]
async fn a_modelled_cut_before_the_sync_discards_the_staged_frame() {
    if let Some((mode, dir)) = child_spec() {
        power_child(mode, dir).await;
    }
    let base = fresh_dir(POWER_UNSYNCED);
    run_power_child(
        POWER_UNSYNCED,
        &base,
        "a_modelled_cut_before_the_sync_discards_the_staged_frame",
    );

    // Two distinct facts, and the code does both: at the cut the store
    // *refused* (the child asserts the poisoned handle fails closed), and after
    // reopening it *reports absence* — it does not refuse to open, and it does
    // not claim a key it never made durable.
    let mut sm = reopen(&store_dir(&base));
    let snapshot = sm.try_snapshot(&["a".into()], &[]).unwrap();
    assert!(
        snapshot.get("a").is_none(),
        "the discarded frame must not reappear after reopening"
    );
    assert_eq!(snapshot.revision(), 0, "nothing was ever applied");
    let (applied, _) = sm.applied_state().await.unwrap();
    assert!(
        applied.is_none(),
        "no applied position may be claimed for a frame that never reached the device"
    );
    assert!(
        sm.repaired_delta().is_none(),
        "the cut left an empty delta, not a torn tail: nothing was truncated"
    );
    assert!(!store_dir(&base).join("state.json.corrupt").exists());

    std::fs::remove_dir_all(&base).unwrap();
}

/// Both image-publication boundaries are asserted by one shape, because the
/// claim is the same: the published image is byte-for-byte one of the two
/// complete images, never a torn or missing one.
async fn assert_old_image_survived(mode: &str, test_name: &str) {
    let base = fresh_dir(mode);
    run_power_child(mode, &base, test_name);

    let dir = store_dir(&base);
    let old = captured_old_image(&base);
    let mut sm = reopen(&dir);
    assert_eq!(
        std::fs::read(dir.join("state.json")).unwrap(),
        old,
        "a cut before the publication completes must leave the previous image byte for byte"
    );
    // The image is the old one, but the acknowledged entry is in the delta, so
    // the store serves the complete applied state rather than the old one: a
    // failed checkpoint is replay work, not data loss.
    let snapshot = sm.try_snapshot(&["a".into()], &[]).unwrap();
    assert_eq!(
        snapshot
            .get("a")
            .expect("the delta still replays the acknowledged entry")
            .value,
        b"one"
    );
    let (applied, _) = sm.applied_state().await.unwrap();
    assert_eq!(applied.expect("the applied position survives").index, 1);
    assert!(
        sm.get_current_snapshot().await.unwrap().is_none(),
        "the old image carries no snapshot, and the cut published no new one"
    );
    assert!(
        sm.repaired_delta().is_none(),
        "the delta was never torn, only unflushed work was lost"
    );
    assert!(!dir.join("state.json.corrupt").exists());

    std::fs::remove_dir_all(&base).unwrap();
}

#[tokio::test]
async fn a_cut_between_the_image_sync_and_its_rename_keeps_the_old_image() {
    if let Some((mode, dir)) = child_spec() {
        power_child(mode, dir).await;
    }
    assert_old_image_survived(
        POWER_IMAGE_BEFORE_RENAME,
        "a_cut_between_the_image_sync_and_its_rename_keeps_the_old_image",
    )
    .await;
}

#[tokio::test]
async fn a_cut_between_the_rename_and_the_directory_sync_keeps_the_old_image() {
    if let Some((mode, dir)) = child_spec() {
        power_child(mode, dir).await;
    }
    assert_old_image_survived(
        POWER_IMAGE_BEFORE_DIR_SYNC,
        "a_cut_between_the_rename_and_the_directory_sync_keeps_the_old_image",
    )
    .await;
}

#[tokio::test]
async fn a_completed_image_publication_publishes_the_new_image() {
    if let Some((mode, dir)) = child_spec() {
        power_child(mode, dir).await;
    }
    let base = fresh_dir(POWER_IMAGE_CONTROL);
    run_power_child(
        POWER_IMAGE_CONTROL,
        &base,
        "a_completed_image_publication_publishes_the_new_image",
    );

    let dir = store_dir(&base);
    let old = captured_old_image(&base);
    let mut sm = reopen(&dir);
    // This is the other half of the pair the two cut cases are measured
    // against: once the directory sync returns, the *new* image is the one on
    // disk, and it is complete (the reopen validated its checksum).
    assert_ne!(
        std::fs::read(dir.join("state.json")).unwrap(),
        old,
        "the control must publish a different, new image"
    );
    let snapshot = sm
        .get_current_snapshot()
        .await
        .unwrap()
        .expect("the completed publication publishes its snapshot metadata");
    assert_eq!(
        snapshot.meta.last_log_id.as_ref().map(|id| id.index),
        Some(1)
    );
    let kv = sm.try_snapshot(&["a".into()], &[]).unwrap();
    assert_eq!(kv.get("a").expect("the applied entry survives").value, b"one");
    let (applied, _) = sm.applied_state().await.unwrap();
    assert_eq!(applied.expect("an applied position is published").index, 1);

    std::fs::remove_dir_all(&base).unwrap();
}

/// The snapshot publication is the same `state.json` publication, with the
/// snapshot bytes embedded in the image. There is no separate snapshot or
/// replica directory in this crate: `build_snapshot` and `install_snapshot`
/// both publish through `persist_locked`'s synced temporary file plus rename,
/// and the only other publication sequences (`replica.json` in
/// `openraft_runtime`, the log checkpoint and bootstrap in `openraft_storage`)
/// do not route their writes through the `yadb-io` seam, so this model cannot
/// speak about them at all. These cases therefore cut the snapshot *metadata*
/// publication and assert it is old-or-new, never torn.
async fn assert_snapshot_publication_is_old_or_new(mode: &str, test_name: &str) {
    let base = fresh_dir(mode);
    run_power_child(mode, &base, test_name);

    let mut sm = reopen(&store_dir(&base));
    let (applied, _) = sm.applied_state().await.unwrap();
    assert!(
        applied.is_none(),
        "a cut before the rename must leave the target with nothing installed"
    );
    let kv = sm.try_snapshot(&["a".into(), "b".into()], &[]).unwrap();
    assert!(
        kv.get("a").is_none() && kv.get("b").is_none(),
        "the target must not serve any key from a snapshot it never published"
    );
    assert_eq!(kv.revision(), 0);
    assert!(
        sm.get_current_snapshot().await.unwrap().is_none(),
        "the snapshot metadata must be absent, not torn: no snapshot was published"
    );
    assert!(!store_dir(&base).join("state.json.corrupt").exists());

    std::fs::remove_dir_all(&base).unwrap();
}

#[tokio::test]
async fn a_cut_between_the_snapshot_sync_and_its_rename_publishes_no_snapshot() {
    if let Some((mode, dir)) = child_spec() {
        power_child(mode, dir).await;
    }
    assert_snapshot_publication_is_old_or_new(
        POWER_SNAPSHOT_BEFORE_RENAME,
        "a_cut_between_the_snapshot_sync_and_its_rename_publishes_no_snapshot",
    )
    .await;
}

#[tokio::test]
async fn a_cut_between_the_snapshot_rename_and_the_directory_sync_publishes_no_snapshot() {
    if let Some((mode, dir)) = child_spec() {
        power_child(mode, dir).await;
    }
    assert_snapshot_publication_is_old_or_new(
        POWER_SNAPSHOT_BEFORE_DIR_SYNC,
        "a_cut_between_the_snapshot_rename_and_the_directory_sync_publishes_no_snapshot",
    )
    .await;
}

#[tokio::test]
async fn a_completed_snapshot_publication_installs_the_snapshot() {
    if let Some((mode, dir)) = child_spec() {
        power_child(mode, dir).await;
    }
    let base = fresh_dir(POWER_SNAPSHOT_CONTROL);
    run_power_child(
        POWER_SNAPSHOT_CONTROL,
        &base,
        "a_completed_snapshot_publication_installs_the_snapshot",
    );

    let mut sm = reopen(&store_dir(&base));
    let (applied, _) = sm.applied_state().await.unwrap();
    assert_eq!(
        applied
            .expect("the installed snapshot publishes an applied position")
            .index,
        2
    );
    let kv = sm.try_snapshot(&["a".into(), "b".into()], &[]).unwrap();
    assert_eq!(kv.get("a").expect("the installed value is served").value, b"one");
    assert_eq!(kv.get("b").expect("the installed value is served").value, b"two");
    let snapshot = sm
        .get_current_snapshot()
        .await
        .unwrap()
        .expect("the completed installation publishes its snapshot metadata");
    assert_eq!(
        snapshot.meta.last_log_id.as_ref().map(|id| id.index),
        Some(2)
    );

    std::fs::remove_dir_all(&base).unwrap();
}
