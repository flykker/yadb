//! Serializes the OpenRaft tests that need real wall-clock quorum progress.
//!
//! These tests drive live Raft groups (and, for the process tests, real child
//! processes) over loopback mTLS. They assert progress inside time windows that
//! are measured on the wall clock: a quorum confirmation inside
//! `heartbeat_interval`, a write inside the client bridge deadline, a snapshot
//! installed before a purge. Every one of those windows assumes the machine is
//! not oversubscribed.
//!
//! `cargo test` runs the whole lib target in parallel, so on an 8-core machine
//! these tests used to compete with ~120 other tests and with each other. That
//! produced three distinct failures. Two are pure scheduler artefacts: a
//! linearizable read reporting `not enough for a quorum, got: {leader}` (the
//! quorum window is `heartbeat_interval`, so a late loopback mTLS round trip
//! misses it) and a client write hitting the bridge's 5 s deadline. The third, an
//! election that deposed a healthy leader because its heartbeats were late, is
//! load-aggravated but also exposes a real product gap: openraft 0.9.25 has no
//! pre-vote, so a stalled replica inflates its term and deposes a leader that had
//! committed state. The gate removes the starvation trigger; it does not fix that
//! amplifier, and nothing here should be read as saying the underlying
//! availability limits are gone.
//!
//! The gate makes the assumption explicit instead of loosening an assertion: one
//! heavy cluster test runs at a time, with the same assertions as before.
//! Concurrency *inside* a test is untouched — the multigroup test still runs nine
//! Raft groups on a four-worker runtime, and the process tests still run three
//! processes. The scope is deliberately narrow: this mutex serializes only these
//! six callers within one process. Other test binaries, other Cargo invocations
//! and the live cluster still compete for CPU, so it reduces contention rather
//! than guaranteeing a dedicated core.
//!
//! Child processes re-entering a test function skip the gate. That is not what
//! prevents a cross-process deadlock — `OnceLock` is process-local, so a child
//! re-executing the binary gets a different mutex and could never block on its
//! parent's guard. They skip it because acquiring a private no-op mutex would
//! only obscure that fact.

use std::sync::OnceLock;
use tokio::sync::{Mutex, MutexGuard};

fn gate() -> &'static Mutex<()> {
    static GATE: OnceLock<Mutex<()>> = OnceLock::new();
    GATE.get_or_init(|| Mutex::new(()))
}

/// Waits until no other heavy cluster test is running. A panicking test releases
/// the gate, because `tokio::sync::Mutex` has no poisoning.
pub(crate) async fn exclusive() -> MutexGuard<'static, ()> {
    gate().lock().await
}
