//! Phase 1 (durable account/leader state, section 6) through Phase 6
//! (Squadron, CAG, and Control Tower, section 11) of
//! `docs/COPY_ENGINE_BLUEPRINT.md`.
//!
//! Schema covers every table blueprint section 6 lists.
//! Live venue writes live behind `CopyExecution` / `execute_one_intent` and
//! the `copy_run` binary, gated by `POLYCOPY_ENGINE_EXECUTE=yes`. Tests use
//! fakes and never contact the venue.

pub mod control_tower;
pub mod db;
pub mod plan;
pub mod setup;

#[cfg(feature = "execute")]
pub mod execute;

#[cfg(all(feature = "execute", feature = "intl_clob"))]
pub mod prepare;

#[cfg(feature = "execute")]
pub mod orchestrate;

#[cfg(feature = "execute")]
pub mod persistent;

#[cfg(feature = "execute")]
pub mod reconcile;

#[cfg(feature = "ingest")]
pub mod ingest;

pub use control_tower::{
    leader_intents, leader_lots, leader_reconciliation_cases, leader_status, trace_attempt,
    AccountSummary, AttemptTrace, ControlTowerError, CopyStrategyStatusShim, EventSummary,
    IntentSummary, LeaderStatus, LotSummary, ReconciliationCaseSummary, SignalStatus,
};
pub use db::{open, open_and_migrate, open_read_only, DbError};
pub use plan::{
    plan_next_batch, plan_next_batch_with_limit, verify_schedule_compatible_with_pending_work,
    PlanError, PlanSummary, PolicySnapshot,
};
pub use setup::{
    configure_fresh_high_frequency_policy, initialize_fresh_test_copy_setup, InitialCopySetup,
    InitialCopySetupResult, SetupError,
};

#[cfg(feature = "execute")]
pub use execute::{
    execute_intent, finalize_receipt, ExecuteError, ExecutionOutcome, OrderSubmitter, Side,
    SizedDecision,
};

#[cfg(feature = "execute")]
pub use reconcile::{
    attempts_in_window, load_or_prepare_attempt, mark_attempt_rejected, mark_attempt_submitting,
    mark_attempt_uncertain_after_submission_error, open_reconciliation_case,
    permitted_recovery_action, recover_fak_taker_order_from_trades,
    recover_lost_submission_response, CopyExecution, LostSubmissionRecoveryOutcome, OrderId,
    PreparedOrderEnvelope, ReconcileError, RecoveryAction, SubmitError, TradeHistoryLookup,
    TradeHistoryRecoveryError, TradeHistoryWindow, VenueOrderState, MAX_ATTEMPTS_PER_WINDOW,
    RETRY_WINDOW_SECONDS,
};

#[cfg(feature = "execute")]
pub use orchestrate::{
    execute_one_intent, execute_one_intent_with_marker, list_runnable_intents,
    live_execute_enabled, EnvelopeFactory, OrchestrateError, OrchestrateOutcome,
    StandardSubmitAttemptMarker, SubmitAttemptMarker,
};

#[cfg(feature = "execute")]
pub use persistent::{
    assert_startup_clear as assert_persistent_startup_clear, ensure_fuse_clear,
    fuse_status as persistent_fuse_status, init_config as init_persistent_config,
    pause_fuse as pause_persistent_fuse, release_pre_boundary_failure,
    reserve_budget_and_mark_submitting, resolve_pre_submit_balance_case,
    resume_fuse as resume_persistent_fuse, rolling_reserved_total, PersistentError,
    PersistentRuntimeConfig, PersistentSubmitMarker, EXIT_BUDGET_STATE, EXIT_CONFIG,
    EXIT_FUSE_OPEN, EXIT_LOCK_COLLISION, EXIT_UNRESOLVED_RECOVERY,
};
