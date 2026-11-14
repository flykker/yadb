//! Adapted copy of the upstream OpenRaft 0.9.25 storage test suite.
//!
//! Upstream source: `openraft-0.9.25/src/testing/suite.rs`
//! (dual licensed MIT / Apache-2.0, Copyright the openraft authors).
//!
//! The upstream harness is `pub` but it can only be executed from inside the
//! openraft crate: it builds a `LogFlushed` callback with a crate-private
//! constructor and it requires `NodeId: From<u64>`, which YADB's `u32` node
//! ids do not implement. This copy keeps every upstream test case and
//! replaces those crate-internal helpers and also substitutes public-view
//! assertions for inaccessible internal state and fills an applied-log gap:
//!
//! * `LogFlushed` is replaced by the public `RaftLogStorageExt::blocking_append`.
//! * `NodeId: From<u64>` is replaced by the local [`SuiteNodeId`] trait, whose
//!   only purpose is to provide the node id `0` used by the upstream cases.
//!
//! These substitutions mean this is not unmodified upstream suite coverage;
//! the full upstream compatibility gate remains open.
//! The initial-state membership case is also split across fresh stores: the
//! upstream sequence appends below the purge boundary produced by its first
//! recovery call, conflicting with append's readable-on-return requirement.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::time::Duration;

use openraft::entry::RaftEntry;
use openraft::log_id::RaftLogId;
use openraft::storage::RaftLogReaderExt;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftLogStorageExt;
use openraft::storage::RaftStateMachine;
use openraft::testing::StoreBuilder;
use openraft::CommittedLeaderId;
use openraft::EffectiveMembership;
use openraft::LogId;
use openraft::LogState;
use openraft::Membership;
use openraft::MembershipState;
use openraft::NodeId;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageHelper;
use openraft::StoredMembership;
use openraft::Vote;

/// Helper to construct a `BTreeSet` of `C::NodeId` from numbers.
///
/// Upstream this used `$key.into()`, which required `NodeId: From<u64>`.
/// The adapted suite converts the literal through [`SuiteNodeId::from_u64`]
/// instead, so YADB's `u32` node ids work unchanged.
macro_rules! btreeset {
    ($($key:expr,)+) => (btreeset!($($key),+));
    ( $($key:expr),* ) => {{
        let mut _set = ::std::collections::BTreeSet::new();
        $( _set.insert(<C::NodeId as SuiteNodeId>::from_u64($key as u64)); )*
        _set
    }};
}

/// Node ids used by the upstream suite.
///
/// The upstream harness requires `NodeId: From<u64>` only to build the
/// node id `0`. YADB uses `u32` node ids, so the requirement is expressed as
/// this small trait instead of a lossy conversion.
pub trait SuiteNodeId: NodeId {
    /// The node id used by the upstream test cases.
    fn zero() -> Self;

    /// Builds a node id from the numeric literals used by the upstream cases.
    fn from_u64(value: u64) -> Self;
}

impl SuiteNodeId for u32 {
    fn zero() -> Self {
        0
    }

    fn from_u64(value: u64) -> Self {
        u32::try_from(value).expect("suite node id fits into u32")
    }
}

/// Test suite to ensure a `RaftStore` impl works as expected.
///
/// Additional traits are required to be implemented by the store builder for testing:
/// - `C::D` and `C::R` requires `Debug` for debugging.
/// - `C::NodeId` requires `From<u64>` to build a node id.
pub struct Suite<C, LS, SM, B, G>
where
    C: RaftTypeConfig,
    C::D: Debug,
    C::R: Debug,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
    B: StoreBuilder<C, LS, SM, G>,
    G: Send + Sync,
{
    _p: PhantomData<(C, LS, SM, B, G)>,
}

#[allow(unused)]
impl<C, LS, SM, B, G> Suite<C, LS, SM, B, G>
where
    C: RaftTypeConfig,
    C::D: Debug,
    C::R: Debug,
    C::NodeId: SuiteNodeId,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
    B: StoreBuilder<C, LS, SM, G>,
    G: Send + Sync,
{
    pub fn test_all(builder: B) -> Result<(), StorageError<C::NodeId>> {
        Suite::test_store(&builder)?;
        Ok(())
    }

    /// Runs one upstream case and reports its name on failure.
    fn run_case<TestFn, Fu>(
        builder: &B,
        name: &'static str,
        test_fn: TestFn,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        Fu: Future<Output = Result<(), StorageError<C::NodeId>>> + OptionalSend,
        TestFn: Fn(LS, SM) -> Fu + Sync + Send,
    {
        match run_fut(run_test(builder, test_fn)) {
            Ok(()) => Ok(()),
            Err(error) => {
                eprintln!("storage suite case `{name}` failed: {error}");
                Err(error)
            }
        }
    }

    pub fn test_store(builder: &B) -> Result<(), StorageError<C::NodeId>> {
        Self::run_case(
            builder,
            "last_membership_in_log_initial",
            Self::last_membership_in_log_initial,
        )?;
        Self::run_case(
            builder,
            "last_membership_in_log",
            Self::last_membership_in_log,
        )?;
        Self::run_case(
            builder,
            "last_membership_in_log_multi_step",
            Self::last_membership_in_log_multi_step,
        )?;
        Self::run_case(
            builder,
            "get_membership_initial",
            Self::get_membership_initial,
        )?;
        Self::run_case(
            builder,
            "get_membership_from_log_and_empty_sm",
            Self::get_membership_from_log_and_empty_sm,
        )?;
        Self::run_case(
            builder,
            "get_membership_from_empty_log_and_sm",
            Self::get_membership_from_empty_log_and_sm,
        )?;
        Self::run_case(
            builder,
            "get_membership_from_log_le_sm_last_applied",
            Self::get_membership_from_log_le_sm_last_applied,
        )?;
        Self::run_case(
            builder,
            "get_membership_from_log_gt_sm_last_applied_1",
            Self::get_membership_from_log_gt_sm_last_applied_1,
        )?;
        Self::run_case(
            builder,
            "get_membership_from_log_gt_sm_last_applied_2",
            Self::get_membership_from_log_gt_sm_last_applied_2,
        )?;
        Self::run_case(
            builder,
            "get_initial_state_without_init",
            Self::get_initial_state_without_init,
        )?;
        Self::run_case(
            builder,
            "get_initial_state_membership_empty_log",
            Self::get_initial_state_membership_empty_log,
        )?;
        Self::run_case(
            builder,
            "get_initial_state_membership_from_log_and_sm",
            Self::get_initial_state_membership_from_log_and_sm,
        )?;
        Self::run_case(
            builder,
            "get_initial_state_with_state",
            Self::get_initial_state_with_state,
        )?;
        Self::run_case(
            builder,
            "get_initial_state_last_log_gt_sm",
            Self::get_initial_state_last_log_gt_sm,
        )?;
        Self::run_case(
            builder,
            "get_initial_state_last_log_lt_sm",
            Self::get_initial_state_last_log_lt_sm,
        )?;
        Self::run_case(
            builder,
            "get_initial_state_log_ids",
            Self::get_initial_state_log_ids,
        )?;
        Self::run_case(
            builder,
            "get_initial_state_re_apply_committed",
            Self::get_initial_state_re_apply_committed,
        )?;
        Self::run_case(builder, "save_vote", Self::save_vote)?;
        Self::run_case(builder, "get_log_entries", Self::get_log_entries)?;
        Self::run_case(
            builder,
            "limited_get_log_entries",
            Self::limited_get_log_entries,
        )?;
        Self::run_case(builder, "try_get_log_entry", Self::try_get_log_entry)?;
        Self::run_case(builder, "initial_logs", Self::initial_logs)?;
        Self::run_case(builder, "get_log_state", Self::get_log_state)?;
        Self::run_case(builder, "get_log_id", Self::get_log_id)?;
        Self::run_case(builder, "last_id_in_log", Self::last_id_in_log)?;
        Self::run_case(builder, "last_applied_state", Self::last_applied_state)?;
        Self::run_case(builder, "purge_logs_upto_0", Self::purge_logs_upto_0)?;
        Self::run_case(builder, "purge_logs_upto_5", Self::purge_logs_upto_5)?;
        Self::run_case(builder, "purge_logs_upto_20", Self::purge_logs_upto_20)?;
        Self::run_case(builder, "delete_logs_since_11", Self::delete_logs_since_11)?;
        Self::run_case(builder, "delete_logs_since_0", Self::delete_logs_since_0)?;
        Self::run_case(builder, "append_to_log", Self::append_to_log)?;
        Self::run_case(builder, "snapshot_meta", Self::snapshot_meta)?;

        Self::run_case(builder, "apply_single", Self::apply_single)?;
        Self::run_case(builder, "apply_multiple", Self::apply_multiple)?;

        if let Err(error) = run_fut(Self::transfer_snapshot(builder)) {
            eprintln!("storage suite case `transfer_snapshot` failed: {error}");
            return Err(error);
        }

        // TODO(xp): test: do_log_compaction

        Ok(())
    }

    pub async fn last_membership_in_log_initial(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        let membership = StorageHelper::new(&mut store, &mut sm)
            .last_membership_in_log(0)
            .await?;

        assert!(membership.is_empty());

        Ok(())
    }

    pub async fn last_membership_in_log(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- no log, do not read membership from state machine");
        {
            apply(
                &mut sm,
                [
                    blank_ent_0::<C>(1, 1),
                    membership_ent_0::<C>(1, 2, btreeset! {3,4,5}),
                ],
            )
            .await?;

            let mem = StorageHelper::new(&mut store, &mut sm)
                .last_membership_in_log(0)
                .await?;

            assert!(mem.is_empty());
        }

        // Ensure last_membership_in_log() won't be affected by state machine.
        tracing::info!("--- membership presents in log, smaller than last_applied, read from log");
        {
            append(&mut store, [membership_ent_0::<C>(1, 1, btreeset! {1,2,3})]).await?;

            let mem = StorageHelper::new(&mut store, &mut sm)
                .last_membership_in_log(0)
                .await?;
            assert_eq!(1, mem.len());
            let mem = mem[0].clone();
            assert_eq!(
                &Membership::new(vec![btreeset! {1, 2, 3}], None),
                mem.membership(),
            );

            let mem = StorageHelper::new(&mut store, &mut sm)
                .last_membership_in_log(1)
                .await?;
            assert_eq!(1, mem.len());
            let mem = mem[0].clone();
            assert_eq!(
                &Membership::new(vec![btreeset! {1, 2, 3}], None),
                mem.membership(),
            );

            let mem = StorageHelper::new(&mut store, &mut sm)
                .last_membership_in_log(2)
                .await?;
            assert!(mem.is_empty());
        }

        tracing::info!("--- membership presents in log and > sm.last_applied, read 2 membership entries from log");
        {
            append(
                &mut store,
                [
                    blank_ent_0::<C>(1, 2),
                    membership_ent_0::<C>(1, 3, btreeset! {7,8,9}),
                    blank_ent_0::<C>(1, 4),
                ],
            )
            .await?;

            let mems = StorageHelper::new(&mut store, &mut sm)
                .last_membership_in_log(0)
                .await?;
            assert_eq!(2, mems.len());

            let mem = mems[0].clone();
            assert_eq!(
                &Membership::new(vec![btreeset! {1,2,3}], None),
                mem.membership(),
            );

            let mem = mems[1].clone();
            assert_eq!(
                &Membership::new(vec![btreeset! {7,8,9}], None),
                mem.membership(),
            );
        }

        tracing::info!("--- membership presents in log and > sm.last_applied, read from log but since_index is greater than the last");
        {
            let mem = StorageHelper::new(&mut store, &mut sm)
                .last_membership_in_log(4)
                .await?;
            assert!(mem.is_empty());
        }

        tracing::info!("--- 3 memberships in log, only return the last 2 of them");
        {
            append(&mut store, [membership_ent_0::<C>(1, 5, btreeset! {10,11})]).await?;

            let mems = StorageHelper::new(&mut store, &mut sm)
                .last_membership_in_log(0)
                .await?;
            assert_eq!(2, mems.len());

            let mem = mems[0].clone();
            assert_eq!(
                &Membership::new(vec![btreeset! {7,8,9}], None),
                mem.membership(),
            );

            let mem = mems[1].clone();
            assert_eq!(
                &Membership::new(vec![btreeset! {10,11}], None),
                mem.membership(),
            );
        }

        Ok(())
    }

    pub async fn last_membership_in_log_multi_step(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- find membership log entry backwards, multiple steps");
        {
            append(
                &mut store,
                [
                    //
                    membership_ent_0::<C>(1, 1, btreeset! {1,2,3}),
                    membership_ent_0::<C>(1, 2, btreeset! {3,4,5}),
                ],
            )
            .await?;

            for i in 3..100 {
                append(&mut store, [blank_ent_0::<C>(1, i)]).await?;
            }

            append(
                &mut store,
                [membership_ent_0::<C>(1, 100, btreeset! {5,6,7})],
            )
            .await?;

            let mems = StorageHelper::new(&mut store, &mut sm)
                .last_membership_in_log(0)
                .await?;
            assert_eq!(2, mems.len());
            let mem = mems[0].clone();
            assert_eq!(
                &Membership::new(vec![btreeset! {3,4,5}], None),
                mem.membership(),
            );

            let mem = mems[1].clone();
            assert_eq!(
                &Membership::new(vec![btreeset! {5,6,7}], None),
                mem.membership(),
            );
        }

        Ok(())
    }

    pub async fn get_membership_initial(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        let mem_state = StorageHelper::new(&mut store, &mut sm)
            .get_membership()
            .await?;

        assert_eq!(
            &EffectiveMembership::default(),
            mem_state.committed().as_ref()
        );
        assert_eq!(
            &EffectiveMembership::default(),
            mem_state.effective().as_ref()
        );

        Ok(())
    }

    pub async fn get_membership_from_log_and_empty_sm(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- no log, read membership from state machine");
        {
            // There is an empty membership config in an empty state machine.

            append(&mut store, [membership_ent_0::<C>(1, 1, btreeset! {1,2,3})]).await?;

            let mem_state = StorageHelper::new(&mut store, &mut sm)
                .get_membership()
                .await?;

            assert_eq!(
                &EffectiveMembership::default(),
                mem_state.committed().as_ref()
            );
            assert_eq!(
                &Membership::new(vec![btreeset! {1,2,3}], None),
                mem_state.effective().membership(),
            );
        }

        Ok(())
    }

    pub async fn get_membership_from_empty_log_and_sm(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- no log, read membership from state machine");
        {
            apply(
                &mut sm,
                [
                    blank_ent_0::<C>(1, 1),
                    membership_ent_0::<C>(1, 2, btreeset! {3,4,5}),
                ],
            )
            .await?;

            let mem_state = StorageHelper::new(&mut store, &mut sm)
                .get_membership()
                .await?;

            assert_eq!(
                &Membership::new(vec![btreeset! {3,4,5}], None),
                mem_state.committed().membership(),
            );
            assert_eq!(
                &Membership::new(vec![btreeset! {3,4,5}], None),
                mem_state.effective().membership(),
            );
        }
        Ok(())
    }

    pub async fn get_membership_from_log_le_sm_last_applied(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- membership presents in log, but smaller than last_applied, read from state machine");
        {
            apply(
                &mut sm,
                [
                    blank_ent_0::<C>(1, 1),
                    membership_ent_0::<C>(1, 2, btreeset! {3,4,5}),
                    blank_ent_0::<C>(1, 3),
                    blank_ent_0::<C>(1, 4),
                ],
            )
            .await?;

            // Intentionally append a membership entry that does not match the state machine,
            // in order to see which membership is loaded.
            append(
                &mut store,
                [
                    blank_ent_0::<C>(1, 1),
                    blank_ent_0::<C>(1, 2),
                    membership_ent_0::<C>(1, 3, btreeset! {1,2,3}),
                ],
            )
            .await?;

            let mem_state = StorageHelper::new(&mut store, &mut sm)
                .get_membership()
                .await?;

            assert_eq!(
                &Membership::new(vec![btreeset! {3,4,5}], None),
                mem_state.committed().membership(),
            );
            assert_eq!(
                &Membership::new(vec![btreeset! {3,4,5}], None),
                mem_state.effective().membership(),
            );
        }
        Ok(())
    }

    pub async fn get_membership_from_log_gt_sm_last_applied_1(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- membership presents in log and > sm.last_applied, read from log");
        {
            apply(
                &mut sm,
                [
                    blank_ent_0::<C>(1, 1),
                    membership_ent_0::<C>(1, 2, btreeset! {3,4,5}),
                ],
            )
            .await?;

            append(
                &mut store,
                [
                    membership_ent_0::<C>(1, 1, btreeset! {1,2,3}),
                    blank_ent_0::<C>(1, 2),
                    membership_ent_0::<C>(1, 3, btreeset! {7,8,9}),
                ],
            )
            .await?;

            let mem_state = StorageHelper::new(&mut store, &mut sm)
                .get_membership()
                .await?;

            assert_eq!(
                &Membership::new(vec![btreeset! {3,4,5}], None),
                mem_state.committed().membership(),
            );
            assert_eq!(
                &Membership::new(vec![btreeset! {7,8,9}], None),
                mem_state.effective().membership(),
            );
        }
        Ok(())
    }

    pub async fn get_membership_from_log_gt_sm_last_applied_2(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- two membership present in log and > sm.last_applied, read 2 from log");
        {
            apply(
                &mut sm,
                [
                    blank_ent_0::<C>(1, 1),
                    membership_ent_0::<C>(1, 2, btreeset! {3,4,5}),
                ],
            )
            .await?;

            append(
                &mut store,
                [
                    membership_ent_0::<C>(1, 1, btreeset! {1,2,3}),
                    blank_ent_0::<C>(1, 2),
                    membership_ent_0::<C>(1, 3, btreeset! {7,8,9}),
                    blank_ent_0::<C>(1, 4),
                    membership_ent_0::<C>(1, 5, btreeset! {10,11}),
                ],
            )
            .await?;

            let mem_state = StorageHelper::new(&mut store, &mut sm)
                .get_membership()
                .await?;

            assert_eq!(
                &Membership::new(vec![btreeset! {7,8,9}], None),
                mem_state.committed().membership(),
            );
            assert_eq!(
                &Membership::new(vec![btreeset! {10,11}], None),
                mem_state.effective().membership(),
            );
        }

        Ok(())
    }

    pub async fn get_initial_state_without_init(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        let initial = StorageHelper::new(&mut store, &mut sm)
            .get_initial_state()
            .await?;

        // Adapted: upstream compares the whole `RaftState` after patching a
        // crate-private vote timestamp. The public parts carry the same
        // meaning: an uninitialized store exposes the default vote, default
        // membership and no snapshot.
        assert_eq!(&Vote::default(), initial.vote_ref(), "uninitialized vote");
        assert_eq!(
            &MembershipState::<C::NodeId, C::Node>::default(),
            &initial.membership_state,
            "uninitialized membership"
        );
        assert_eq!(
            &SnapshotMeta::<C::NodeId, C::Node>::default(),
            &initial.snapshot_meta,
            "uninitialized snapshot"
        );
        let log_state = store.get_log_state().await?;
        assert_eq!(None, log_state.last_log_id, "uninitialized log");
        assert_eq!(
            None, log_state.last_purged_log_id,
            "uninitialized purge marker"
        );
        Ok(())
    }

    pub async fn get_initial_state_with_state(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        Self::default_vote(&mut store).await?;

        append(
            &mut store,
            [
                blank_ent_0::<C>(0, 0),
                blank_ent_0::<C>(1, 1),
                blank_ent_0::<C>(3, 2),
            ],
        )
        .await?;

        apply(&mut sm, [blank_ent_0::<C>(3, 1)]).await?;

        let initial = StorageHelper::new(&mut store, &mut sm)
            .get_initial_state()
            .await?;

        // Adapted: `last_log_id()`/`committed()` on `RaftState` are
        // crate-private. The same values are observable through the log store
        // and the state machine.
        let log_state = store.get_log_state().await?;
        assert_eq!(
            Some(log_id_0(3, 2)),
            log_state.last_log_id,
            "state machine has higher log"
        );
        assert_eq!(
            Some(log_id_0(3, 1)),
            sm.applied_state().await?.0,
            "unexpected value for last applied log"
        );
        assert_eq!(
            Vote::new(1, C::NodeId::zero()),
            *initial.vote_ref(),
            "unexpected value for default hard state"
        );
        Ok(())
    }

    pub async fn get_initial_state_membership_empty_log(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        Self::default_vote(&mut store).await?;
        apply(
            &mut sm,
            [
                blank_ent_0::<C>(1, 1),
                membership_ent_0::<C>(1, 2, btreeset! {3,4,5}),
            ],
        )
        .await?;
        let initial = StorageHelper::new(&mut store, &mut sm)
            .get_initial_state()
            .await?;
        assert_eq!(
            &Membership::new(vec![btreeset! {3,4,5}], None),
            initial.membership_state.effective().membership()
        );
        Ok(())
    }

    pub async fn get_initial_state_membership_from_log_and_sm(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        // It should never return membership from logs that are included in state machine present.

        Self::default_vote(&mut store).await?;

        // copy the test from get_membership_config

        tracing::info!("--- no log, read membership from state machine");
        {
            apply(
                &mut sm,
                [
                    blank_ent_0::<C>(1, 1),
                    membership_ent_0::<C>(1, 2, btreeset! {3,4,5}),
                ],
            )
            .await?;

            // The empty-log recovery assertion runs in its own fresh case.
            // Recovery here would purge through index 2 before the next
            // section appends index 1, an invalid append for this adapter.
        }

        tracing::info!("--- membership presents in log, but smaller than last_applied, read from state machine");
        {
            append(&mut store, [membership_ent_0::<C>(1, 1, btreeset! {1,2,3})]).await?;

            let initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;

            assert_eq!(
                &Membership::new(vec![btreeset! {3,4,5}], None),
                initial.membership_state.effective().membership(),
            );
        }

        tracing::info!("--- membership presents in log and > sm.last_applied, read from log");
        {
            append(&mut store, [membership_ent_0::<C>(1, 3, btreeset! {1,2,3})]).await?;

            let initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;

            assert_eq!(
                &Membership::new(vec![btreeset! {1,2,3}], None),
                initial.membership_state.effective().membership(),
            );
        }

        Ok(())
    }

    pub async fn get_initial_state_last_log_gt_sm(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        Self::default_vote(&mut store).await?;

        append(&mut store, [blank_ent_0::<C>(0, 0), blank_ent_0::<C>(2, 1)]).await?;

        apply(&mut sm, [blank_ent_0::<C>(1, 1), blank_ent_0::<C>(1, 2)]).await?;

        let _initial = StorageHelper::new(&mut store, &mut sm)
            .get_initial_state()
            .await?;

        // Adapted: `RaftState::last_log_id()` is crate-private; the log store
        // exposes the same value.
        let log_state = store.get_log_state().await?;
        assert_eq!(
            Some(log_id_0(2, 1)),
            log_state.last_log_id,
            "state machine has higher log"
        );
        Ok(())
    }

    pub async fn get_initial_state_last_log_lt_sm(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        Self::default_vote(&mut store).await?;

        append(&mut store, [blank_ent_0::<C>(1, 2)]).await?;

        apply(&mut sm, [blank_ent_0::<C>(3, 1)]).await?;

        let _initial = StorageHelper::new(&mut store, &mut sm)
            .get_initial_state()
            .await?;

        // The internal RaftState log-id cache is private; verify the public
        // purge side effect and retained suffix separately instead.
        let log_state = store.get_log_state().await?;
        assert_eq!(
            Some(log_id_0(1, 2)),
            log_state.last_log_id,
            "suffix remains in storage"
        );
        assert_eq!(
            Some(log_id_0(3, 1)),
            log_state.last_purged_log_id,
            "state machine has higher log"
        );
        Ok(())
    }

    /// Equivalent of the upstream `RaftState::log_ids.key_log_ids()` view,
    /// rebuilt only from the public log API.
    ///
    /// Upstream asserts on an internal `LogIdList` cache. The same observable
    /// facts are: the purge boundary followed by the first log id of every
    /// leader term still present in the log.
    async fn key_log_ids(store: &mut LS) -> Result<Vec<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        let state = store.get_log_state().await?;
        let mut ids = Vec::new();
        let purged = state.last_purged_log_id.clone();
        let start = purged.as_ref().map(|id| id.index + 1).unwrap_or(0);
        if let Some(purged) = purged {
            ids.push(purged);
        }
        let mut last_entry = None;
        for entry in store.try_get_log_entries(start..).await? {
            let log_id = entry.get_log_id().clone();
            if ids
                .last()
                .map_or(true, |last| last.leader_id != log_id.leader_id)
            {
                ids.push(log_id.clone());
            }
            last_entry = Some(log_id);
        }
        if let Some(last) = last_entry {
            if ids.last() != Some(&last) {
                ids.push(last);
            }
        }
        Ok(ids)
    }

    pub async fn get_initial_state_log_ids(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        let log_id = |t, n: u64, i| LogId::<C::NodeId> {
            leader_id: CommittedLeaderId::new(t, SuiteNodeId::from_u64(n)),
            index: i,
        };

        tracing::info!("--- empty store, expect []");
        {
            let _initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;
            assert_eq!(
                Vec::<LogId<C::NodeId>>::new(),
                Self::key_log_ids(&mut store).await?
            );
        }

        tracing::info!("--- log terms: [0], last_purged_log_id is None, expect [(0,0)]");
        {
            append(&mut store, [blank_ent_0::<C>(0, 0)]).await?;

            let _initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;
            assert_eq!(vec![log_id(0, 0, 0)], Self::key_log_ids(&mut store).await?);
        }

        tracing::info!(
            "--- log terms: [0,1,1,2], last_purged_log_id is None, expect [(0,0),(1,1),(2,3)]"
        );
        {
            append(
                &mut store,
                [
                    blank_ent_0::<C>(1, 1),
                    blank_ent_0::<C>(1, 2),
                    blank_ent_0::<C>(2, 3),
                ],
            )
            .await?;

            let _initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;
            assert_eq!(
                vec![log_id(0, 0, 0), log_id(1, 0, 1), log_id(2, 0, 3)],
                Self::key_log_ids(&mut store).await?
            );
        }

        tracing::info!(
            "--- log terms: [0,1,1,2,2,3,3], last_purged_log_id is None, expect [(0,0),(1,1),(2,3),(3,5),(3,6)]"
        );
        {
            append(
                &mut store,
                [
                    blank_ent_0::<C>(2, 4),
                    blank_ent_0::<C>(3, 5),
                    blank_ent_0::<C>(3, 6),
                ],
            )
            .await?;

            let _initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;
            assert_eq!(
                vec![
                    log_id(0, 0, 0),
                    log_id(1, 0, 1),
                    log_id(2, 0, 3),
                    log_id(3, 0, 5),
                    log_id(3, 0, 6)
                ],
                Self::key_log_ids(&mut store).await?
            );
        }

        tracing::info!(
            "--- log terms: [x,1,1,2,2,3,3], last_purged_log_id: (0,0), expect [(0,0),(1,1),(2,3),(3,5),(3,6)]"
        );
        {
            store.purge(log_id(0, 0, 0)).await?;

            let _initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;
            assert_eq!(
                vec![
                    log_id(0, 0, 0),
                    log_id(1, 0, 1),
                    log_id(2, 0, 3),
                    log_id(3, 0, 5),
                    log_id(3, 0, 6)
                ],
                Self::key_log_ids(&mut store).await?
            );
        }

        tracing::info!("--- log terms: [x,x,1,2,2,3,3], last_purged_log_id: (1,1), expect [(1,1),(2,3),(3,5),(3,6)]");
        {
            store.purge(log_id(1, 0, 1)).await?;

            let _initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;
            assert_eq!(
                vec![
                    log_id(1, 0, 1),
                    log_id(2, 0, 3),
                    log_id(3, 0, 5),
                    log_id(3, 0, 6)
                ],
                Self::key_log_ids(&mut store).await?
            );
        }

        tracing::info!("--- log terms: [x,x,x,2,2,3,3], last_purged_log_id: (1,2), expect [(1,2),(2,3),(3,5),(3,6)]");
        {
            store.purge(log_id(1, 0, 2)).await?;

            let _initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;
            assert_eq!(
                vec![
                    log_id(1, 0, 2),
                    log_id(2, 0, 3),
                    log_id(3, 0, 5),
                    log_id(3, 0, 6)
                ],
                Self::key_log_ids(&mut store).await?
            );
        }

        tracing::info!(
            "--- log terms: [x,x,x,x,2,3,3], last_purged_log_id: (2,3), expect [(2,3),(3,5),(3,6)]"
        );
        {
            store.purge(log_id(2, 0, 3)).await?;

            let _initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;
            assert_eq!(
                vec![log_id(2, 0, 3), log_id(3, 0, 5), log_id(3, 0, 6)],
                Self::key_log_ids(&mut store).await?
            );
        }

        tracing::info!("--- log terms: [x,x,x,x,x,x,x], last_purged_log_id: (3,6), e.g., all purged expect [(3,6)]");
        {
            store.purge(log_id(3, 0, 6)).await?;

            let _initial = StorageHelper::new(&mut store, &mut sm)
                .get_initial_state()
                .await?;
            assert_eq!(vec![log_id(3, 0, 6)], Self::key_log_ids(&mut store).await?);
        }

        Ok(())
    }

    /// Test if committed logs are re-applied.
    pub async fn get_initial_state_re_apply_committed(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        Self::default_vote(&mut store).await?;

        append(
            &mut store,
            [
                blank_ent_0::<C>(1, 2),
                blank_ent_0::<C>(1, 3),
                blank_ent_0::<C>(1, 4),
                blank_ent_0::<C>(1, 5),
            ],
        )
        .await?;
        store.purge(log_id_0(1, 1)).await?;

        apply(&mut sm, [blank_ent_0::<C>(1, 2)]).await?;

        store.save_committed(Some(log_id_0(1, 4))).await?;
        let got = store.read_committed().await?;
        if got.is_none() {
            tracing::info!("This implementation does not store committed log id, skip test re-applying committed logs");
            return Ok(());
        }

        let _initial = StorageHelper::new(&mut store, &mut sm)
            .get_initial_state()
            .await?;

        // Adapted: `RaftState::io_applied()` is crate-private. The state
        // machine exposes the same last-applied log id, which is asserted
        // exactly as upstream does on the next line.
        assert_eq!(
            Some(log_id_0(1, 4)),
            sm.applied_state().await?.0,
            "last_applied is updated"
        );

        Ok(())
    }

    pub async fn save_vote(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        store.save_vote(&Vote::new(100, C::NodeId::zero())).await?;

        let got = store.read_vote().await?;

        assert_eq!(Some(Vote::new(100, C::NodeId::zero())), got,);
        Ok(())
    }

    pub async fn get_log_entries(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        Self::feed_10_logs_vote_self(&mut store).await?;

        tracing::info!("--- get start == stop");
        {
            let logs = store.get_log_entries(3..3).await?;
            assert_eq!(logs.len(), 0, "expected no logs to be returned");
        }

        tracing::info!("--- get start < stop");
        {
            let logs = store.get_log_entries(5..7).await?;

            assert_eq!(logs.len(), 2);
            assert_eq!(*logs[0].get_log_id(), log_id_0(1, 5));
            assert_eq!(*logs[1].get_log_id(), log_id_0(1, 6));
        }

        Ok(())
    }

    pub async fn limited_get_log_entries(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        Self::feed_10_logs_vote_self(&mut store).await?;

        tracing::info!("--- get start == stop");
        {
            let logs = store.limited_get_log_entries(3, 3).await?;
            assert_eq!(logs.len(), 0, "expected no logs to be returned");
        }

        tracing::info!("--- get start < stop");
        {
            let logs = store.limited_get_log_entries(5, 7).await?;

            assert!(!logs.is_empty());
            assert!(logs.len() <= 2);
            assert_eq!(*logs[0].get_log_id(), log_id_0(1, 5));
        }

        Ok(())
    }

    pub async fn try_get_log_entry(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        Self::feed_10_logs_vote_self(&mut store).await?;

        store
            .purge(LogId::new(
                CommittedLeaderId::new(0, C::NodeId::default()),
                0,
            ))
            .await?;

        // `purge()` does not have to do the purge at once.
        // The implementation may choose to do it in the background.
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        let ent = store.try_get_log_entry(3).await?;
        assert_eq!(Some(log_id_0(1, 3)), ent.map(|x| x.get_log_id().clone()));

        let ent = store.try_get_log_entry(0).await?;
        assert_eq!(None, ent.map(|x| x.get_log_id().clone()));

        let ent = store.try_get_log_entry(11).await?;
        assert_eq!(None, ent.map(|x| x.get_log_id().clone()));

        Ok(())
    }

    pub async fn initial_logs(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        let ent = store.try_get_log_entry(0).await?;
        assert!(ent.is_none(), "store initialized");

        Ok(())
    }

    pub async fn get_log_state(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        let st = store.get_log_state().await?;

        assert_eq!(None, st.last_purged_log_id);
        assert_eq!(None, st.last_log_id);

        tracing::info!("--- only logs");
        {
            append(
                &mut store,
                [
                    blank_ent_0::<C>(0, 0),
                    blank_ent_0::<C>(1, 1),
                    blank_ent_0::<C>(1, 2),
                ],
            )
            .await?;

            let st = store.get_log_state().await?;
            assert_eq!(None, st.last_purged_log_id);
            assert_eq!(Some(log_id_0(1, 2)), st.last_log_id);
        }

        tracing::info!("--- delete log 0-0");
        {
            store.purge(log_id_0(0, 0)).await?;

            let st = store.get_log_state().await?;
            assert_eq!(
                Some(LogId::new(
                    CommittedLeaderId::new(0, C::NodeId::default()),
                    0
                )),
                st.last_purged_log_id
            );
            assert_eq!(Some(log_id_0(1, 2)), st.last_log_id);
        }

        tracing::info!("--- delete all log");
        {
            store.purge(log_id_0(1, 2)).await?;

            let st = store.get_log_state().await?;
            assert_eq!(Some(log_id_0(1, 2)), st.last_purged_log_id);
            assert_eq!(Some(log_id_0(1, 2)), st.last_log_id);
        }

        tracing::info!("--- delete advance last present logs");
        {
            store.purge(log_id_0(2, 3)).await?;

            // `purge()` does not have to do the purge at once.
            // The implementation may choose to do it in the background.
            tokio::time::sleep(Duration::from_millis(1_000)).await;

            let st = store.get_log_state().await?;
            assert_eq!(Some(log_id_0(2, 3)), st.last_purged_log_id);
            assert_eq!(Some(log_id_0(2, 3)), st.last_log_id);
        }

        Ok(())
    }

    pub async fn get_log_id(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        Self::feed_10_logs_vote_self(&mut store).await?;

        store.purge(log_id_0(1, 3)).await?;

        // `purge()` does not have to do the purge at once.
        // The implementation may choose to do it in the background.
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        let res = store.get_log_id(0).await;
        assert!(res.is_err());

        let res = store.get_log_id(3).await;
        assert!(res.is_err());

        let res = store.get_log_id(4).await?;
        assert_eq!(log_id_0(1, 4), res);

        let res = store.get_log_id(10).await?;
        assert_eq!(log_id_0(1, 10), res);

        let res = store.get_log_id(11).await;
        assert!(res.is_err());

        Ok(())
    }

    pub async fn last_id_in_log(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        let last_log_id = store.get_log_state().await?.last_log_id;
        assert_eq!(None, last_log_id);

        tracing::info!("--- only logs");
        {
            append(
                &mut store,
                [
                    blank_ent_0::<C>(0, 0),
                    blank_ent_0::<C>(1, 1),
                    blank_ent_0::<C>(1, 2),
                ],
            )
            .await?;

            let last_log_id = store.get_log_state().await?.last_log_id;
            assert_eq!(Some(log_id_0(1, 2)), last_log_id);
        }

        tracing::info!("--- last id in logs < last applied id in sm, only return the id in logs");
        {
            apply(&mut sm, [blank_ent_0::<C>(1, 3)]).await?;
            let last_log_id = store.get_log_state().await?.last_log_id;
            assert_eq!(Some(log_id_0(1, 2)), last_log_id);
        }

        tracing::info!("--- no logs, return default");
        {
            store.purge(log_id_0(1, 2)).await?;

            // `purge()` does not have to do the purge at once.
            // The implementation may choose to do it in the background.
            tokio::time::sleep(Duration::from_millis(1_000)).await;

            let last_log_id = store.get_log_state().await?.last_log_id;
            assert_eq!(Some(log_id_0(1, 2)), last_log_id);
        }

        Ok(())
    }

    pub async fn last_applied_state(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        let (applied, mem) = sm.applied_state().await?;
        assert_eq!(None, applied);
        assert_eq!(StoredMembership::default(), mem);

        tracing::info!("--- with last_applied and last_membership");
        {
            apply(&mut sm, [membership_ent_0::<C>(1, 3, btreeset! {1,2})]).await?;

            let (applied, mem) = sm.applied_state().await?;
            assert_eq!(Some(log_id_0(1, 3)), applied);
            assert_eq!(
                StoredMembership::new(
                    Some(log_id_0(1, 3)),
                    Membership::new(vec![btreeset! {1,2}], None)
                ),
                mem
            );
        }

        tracing::info!("--- no logs, return default");
        {
            // Upstream skips index 4; this adapter rejects gaps in applied
            // entries. Include a blank entry without changing the tested
            // last-applied or last-membership assertions.
            apply(&mut sm, [blank_ent_0::<C>(1, 4), blank_ent_0::<C>(1, 5)]).await?;

            let (applied, mem) = sm.applied_state().await?;
            assert_eq!(Some(log_id_0(1, 5)), applied);
            assert_eq!(
                StoredMembership::new(
                    Some(log_id_0(1, 3)),
                    Membership::new(vec![btreeset! {1,2}], None)
                ),
                mem
            );
        }

        Ok(())
    }

    pub async fn purge_logs_upto_0(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- delete (-oo, 0]");

        Self::feed_10_logs_vote_self(&mut store).await?;

        store.purge(log_id_0(0, 0)).await?;

        // `purge()` does not have to do the purge at once.
        // The implementation may choose to do it in the background.
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        let logs = store.try_get_log_entries(0..100).await?;
        assert_eq!(logs.len(), 10);
        assert_eq!(logs[0].get_log_id().index, 1);

        assert_eq!(
            LogState {
                last_purged_log_id: Some(log_id_0(0, 0)),
                last_log_id: Some(log_id_0(1, 10)),
            },
            store.get_log_state().await?
        );
        Ok(())
    }

    pub async fn purge_logs_upto_5(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- delete (-oo, 5]");

        Self::feed_10_logs_vote_self(&mut store).await?;

        store.purge(log_id_0(1, 5)).await?;

        // `purge()` does not have to do the purge at once.
        // The implementation may choose to do it in the background.
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        let logs = store.try_get_log_entries(0..100).await?;
        assert_eq!(logs.len(), 5);
        assert_eq!(logs[0].get_log_id().index, 6);

        assert_eq!(
            LogState {
                last_purged_log_id: Some(log_id_0(1, 5)),
                last_log_id: Some(log_id_0(1, 10)),
            },
            store.get_log_state().await?
        );
        Ok(())
    }

    pub async fn purge_logs_upto_20(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- delete (-oo, 20]");

        Self::feed_10_logs_vote_self(&mut store).await?;

        store.purge(log_id_0(1, 20)).await?;

        // `purge()` does not have to do the purge at once.
        // The implementation may choose to do it in the background.
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        let logs = store.try_get_log_entries(0..100).await?;
        assert_eq!(logs.len(), 0);

        assert_eq!(
            LogState {
                last_purged_log_id: Some(log_id_0(1, 20)),
                last_log_id: Some(log_id_0(1, 20)),
            },
            store.get_log_state().await?
        );
        Ok(())
    }

    pub async fn delete_logs_since_11(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- delete [11, +oo)");

        Self::feed_10_logs_vote_self(&mut store).await?;

        store.truncate(log_id_0(1, 11)).await?;

        let logs = store.try_get_log_entries(0..100).await?;
        assert_eq!(logs.len(), 11);

        assert_eq!(
            LogState {
                last_purged_log_id: None,
                last_log_id: Some(log_id_0(1, 10)),
            },
            store.get_log_state().await?
        );
        Ok(())
    }

    pub async fn delete_logs_since_0(
        mut store: LS,
        mut sm: SM,
    ) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- delete [0, +oo)");

        Self::feed_10_logs_vote_self(&mut store).await?;

        store.truncate(log_id_0(0, 0)).await?;

        let logs = store.try_get_log_entries(0..100).await?;
        assert_eq!(logs.len(), 0);

        assert_eq!(
            LogState {
                last_purged_log_id: None,
                last_log_id: None,
            },
            store.get_log_state().await?
        );

        Ok(())
    }

    pub async fn append_to_log(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        Self::feed_10_logs_vote_self(&mut store).await?;

        store.purge(log_id_0(0, 0)).await?;

        // `purge()` does not have to do the purge at once.
        // The implementation may choose to do it in the background.
        tokio::time::sleep(Duration::from_millis(1_000)).await;

        append(&mut store, [blank_ent_0::<C>(2, 11)]).await?;

        let l = store.try_get_log_entries(0..).await?.len();
        let last = store
            .try_get_log_entries(0..)
            .await?
            .into_iter()
            .last()
            .unwrap();

        assert_eq!(l, 11, "expected 11 entries to exist in the log");
        assert_eq!(*last.get_log_id(), log_id_0(2, 11), "unexpected log id");
        Ok(())
    }

    pub async fn snapshot_meta(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        tracing::info!("--- just initialized");
        {
            apply(&mut sm, [membership_ent_0::<C>(0, 0, btreeset! {1,2})]).await?;

            let mut b = sm.get_snapshot_builder().await;
            let snap = b.build_snapshot().await?;
            let meta = snap.meta;
            assert_eq!(Some(log_id_0(0, 0)), meta.last_log_id);
            assert_eq!(&Some(log_id_0(0, 0)), meta.last_membership.log_id());
            assert_eq!(
                &Membership::new(vec![btreeset! {1,2}], None),
                meta.last_membership.membership()
            );
        }

        tracing::info!("--- one app log, one membership log");
        {
            apply(
                &mut sm,
                [
                    blank_ent_0::<C>(1, 1),
                    membership_ent_0::<C>(2, 2, btreeset! {3,4}),
                ],
            )
            .await?;

            let mut b = sm.get_snapshot_builder().await;
            let snap = b.build_snapshot().await?;
            let meta = snap.meta;
            assert_eq!(Some(log_id_0(2, 2)), meta.last_log_id);
            assert_eq!(&Some(log_id_0(2, 2)), meta.last_membership.log_id());
            assert_eq!(
                &Membership::new(vec![btreeset! {3,4}], None),
                meta.last_membership.membership()
            );
        }

        Ok(())
    }

    pub async fn apply_single(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        let (last_applied, _) = sm.applied_state().await?;
        assert_eq!(last_applied, None,);

        tracing::info!("--- apply blank entry");
        {
            let entry = blank_ent_0::<C>(0, 0);

            let replies = apply(&mut sm, [entry]).await?;
            assert_eq!(replies.len(), 1, "expected 1 response");
            let (last_applied, _) = sm.applied_state().await?;

            assert_eq!(last_applied, Some(log_id_0(0, 0)),);
        }

        tracing::info!("--- apply membership entry");
        {
            let entry = membership_ent_0::<C>(1, 1, btreeset! {1,2});

            let replies = apply(&mut sm, [entry]).await?;
            assert_eq!(replies.len(), 1, "expected 1 response");
            let (last_applied, mem) = sm.applied_state().await?;

            assert_eq!(last_applied, Some(log_id_0(1, 1)),);
            assert_eq!(
                mem.membership(),
                &Membership::new(vec![btreeset! {1,2}], None)
            );
        }

        // TODO: figure out how to test applying normal entry. `C::D` can not be built by Openraft
        // tracing::info!("--- apply normal entry");
        // {
        //     let entry = {
        //         let mut e = C::Entry::from_app_data(C::D::from(1));
        //         e.set_log_id(&log_id_0(2, 2));
        //         e
        //     };
        //
        //     let replies = apply(&mut sm, [entry]).await?;
        //     assert_eq!(replies.len(), 1, "expected 1 response");
        //     let (last_applied, _) = sm.applied_state().await?;
        //
        //     assert_eq!(last_applied, Some(log_id_0(2, 2)),);
        // }

        Ok(())
    }

    pub async fn apply_multiple(mut store: LS, mut sm: SM) -> Result<(), StorageError<C::NodeId>> {
        let (last_applied, _) = sm.applied_state().await?;
        assert_eq!(last_applied, None,);

        let entries = [
            blank_ent_0::<C>(0, 0),
            membership_ent_0::<C>(1, 1, btreeset! {1,2}),
        ];

        let replies = apply(&mut sm, entries).await?;
        assert_eq!(replies.len(), 2);

        let (last_applied, mem) = sm.applied_state().await?;
        assert_eq!(last_applied, Some(log_id_0(1, 1)),);
        assert_eq!(
            mem.membership(),
            &Membership::new(vec![btreeset! {1,2}], None)
        );

        Ok(())
    }

    /// Rudimentary test for snapshotting that builds a snapshot on one node and installs it on
    /// another
    pub async fn transfer_snapshot(builder: &B) -> Result<(), StorageError<C::NodeId>> {
        // Create a snapshot on sm_l, and install it on sm_f
        let (_g_l, _store_l, mut sm_l) = builder.build().await?;
        let (_g_f, _store_f, mut sm_f) = builder.build().await?;

        tracing::info!("--- make sure that initial snapshot is empty");
        // Start with empty snapshot
        assert!(
            sm_l.get_current_snapshot().await?.is_none(),
            "initialized snapshot"
        );
        assert!(
            sm_f.get_current_snapshot().await?.is_none(),
            "initialized snapshot"
        );

        // Add a few entries so we have state to snapshot
        let snapshot_entries = vec![
            membership_ent_0::<C>(1, 2, btreeset! {1, 2, 3}),
            blank_ent_0::<C>(3, 3),
        ];
        sm_l.apply(snapshot_entries).await?;
        let snapshot_last_log_id = Some(log_id_0(3, 3));
        let snapshot_last_membership = StoredMembership::new(
            Some(log_id_0(1, 2)),
            Membership::new(vec![btreeset![1, 2, 3]], None),
        );
        let snapshot_applied_state = (
            snapshot_last_log_id.clone(),
            snapshot_last_membership.clone(),
        );

        tracing::info!("--- build and get snapshot on leader state machine");
        let ss1 = sm_l.get_snapshot_builder().await.build_snapshot().await?;
        assert_eq!(
            ss1.meta.last_log_id, snapshot_last_log_id,
            "built snapshot has wrong last log id"
        );
        assert_eq!(
            ss1.meta.last_membership, snapshot_last_membership,
            "built snapshot has wrong last membership"
        );
        let ss1_cur = sm_l
            .get_current_snapshot()
            .await?
            .expect("uninitialized snapshot");
        assert_eq!(
            ss1_cur.meta, ss1.meta,
            "current snapshot metadata not updated correctly on leader sm"
        );

        tracing::info!("--- install snapshot on follower state machine");
        let mut ss2_box = sm_f.begin_receiving_snapshot().await?;
        *ss2_box = *ss1_cur.snapshot;

        sm_f.install_snapshot(&ss1_cur.meta, ss2_box).await?;

        tracing::info!("--- check correctness of installed snapshot");
        // ... by requesting whole snapshot
        let ss2 = sm_f
            .get_current_snapshot()
            .await?
            .expect("uninitialized snapshot");
        assert_eq!(
            ss2.meta, ss1.meta,
            "snapshot metadata not updated correctly on follower sm"
        );
        // ... by checking smstore state
        assert_eq!(sm_f.applied_state().await?, snapshot_applied_state);
        Ok(())
    }

    pub async fn feed_10_logs_vote_self(sto: &mut LS) -> Result<(), StorageError<C::NodeId>> {
        append(sto, [blank_ent_0::<C>(0, 0)]).await?;

        for i in 1..=10 {
            append(sto, [blank_ent_0::<C>(1, i)]).await?;
        }

        Self::default_vote(sto).await?;

        Ok(())
    }

    pub async fn default_vote(sto: &mut LS) -> Result<(), StorageError<C::NodeId>> {
        sto.save_vote(&Vote::new(1, C::NodeId::zero())).await?;

        Ok(())
    }
}

/// Create a log id with node id 0 for testing.
fn log_id_0<NID>(term: u64, index: u64) -> LogId<NID>
where
    NID: NodeId + SuiteNodeId,
{
    LogId {
        leader_id: CommittedLeaderId::new(term, NID::zero()),
        index,
    }
}

/// Create a blank log entry with node_id 0 for test.
fn blank_ent_0<C: RaftTypeConfig>(term: u64, index: u64) -> C::Entry
where
    C::NodeId: SuiteNodeId,
{
    C::Entry::new_blank(log_id_0(term, index))
}

/// Create a membership entry with node_id 0 for test.
fn membership_ent_0<C: RaftTypeConfig>(term: u64, index: u64, bs: BTreeSet<C::NodeId>) -> C::Entry
where
    C::NodeId: SuiteNodeId,
{
    C::Entry::new_membership(log_id_0(term, index), Membership::new(vec![bs], ()))
}

/// Block until a future is finished.
/// The future will be running in a clean tokio runtime, to prevent an unfinished task affecting the
/// test.
pub fn run_fut<NID, F>(f: F) -> Result<(), StorageError<NID>>
where
    NID: NodeId,
    F: Future<Output = Result<(), StorageError<NID>>>,
{
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(f)?;
    Ok(())
}

/// Build a `RaftLogStorage` and `RaftStateMachine` implementation and run a test on it.
async fn run_test<C, LS, SM, G, B, TestFn, Ret, Fu>(
    builder: &B,
    test_fn: TestFn,
) -> Result<Ret, StorageError<C::NodeId>>
where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
    B: StoreBuilder<C, LS, SM, G>,
    Fu: Future<Output = Result<Ret, StorageError<C::NodeId>>> + OptionalSend,
    TestFn: Fn(LS, SM) -> Fu + Sync + Send,
{
    let (_g, store, sm) = builder.build().await?;
    test_fn(store, sm).await
}

/// A wrapper for calling nonblocking `RaftStateMachine::apply()`
async fn apply<C, SM, I>(sm: &mut SM, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
    I: IntoIterator<Item = C::Entry> + OptionalSend,
    I::IntoIter: OptionalSend,
{
    let resp = sm.apply(entries).await?;
    Ok(resp)
}

/// A wrapper for calling nonblocking `RaftLogStorage::append()`
async fn append<C, LS, I>(store: &mut LS, entries: I) -> Result<(), StorageError<C::NodeId>>
where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
    I: IntoIterator<Item = C::Entry> + OptionalSend,
    I::IntoIter: OptionalSend,
{
    store.blocking_append(entries).await
}
