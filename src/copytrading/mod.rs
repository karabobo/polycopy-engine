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
#[cfg(feature = "execute")]
pub mod book_observation;
#[cfg(feature = "execute")]
pub mod book_sampler;
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

#[cfg(feature = "dashboard")]
pub mod dashboard;

#[cfg(feature = "ops_panel")]
pub mod ops;

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
    apply_trading_config, AccountConfigInput, ChangeKind, ConfigApplyOptions, ConfigApplySummary,
    ConfigError, LeaderApplySummary, LeaderConfigInput, LeaderPolicyInput, TradingConfig,
    CONFIG_APPLIED_PREFIX,
};

#[cfg(feature = "dashboard")]
pub use dashboard::{
    classify_log_line, collect_dashboard, draw_ui, AppState, LeaderDashboardSummary, LogLineKind,
    LogTailer,
};

#[cfg(feature = "execute")]
pub use execute::{
    cancel_overdue_pre_submit_intent, execute_intent, finalize_receipt, ExecuteError,
    ExecutionOutcome, OrderSubmitter, Side, SizedDecision,
};

#[cfg(feature = "execute")]
pub use reconcile::{
    attempts_in_window, inspect_uncertain_attempt_for_operator, load_or_prepare_attempt,
    mark_attempt_gtd_lookup_failed, mark_attempt_rejected, mark_attempt_submitting,
    mark_attempt_uncertain_after_submission_error,
    open_reconciliation_case, permitted_recovery_action, recover_fak_taker_order_from_trades,
    recover_gtd_maker_order_from_trades, recover_lost_submission_response, CopyExecution,
    LostSubmissionRecoveryOutcome, OperatorUncertainLookup, OrderId, PreparedOrderEnvelope,
    ReconcileError, RecoveryAction, SubmitError, TradeHistoryLookup, TradeHistoryRecoveryError,
    TradeHistoryWindow, VenueOrderState, MAX_ATTEMPTS_PER_WINDOW, RETRY_WINDOW_SECONDS,
};

#[cfg(feature = "execute")]
pub use orchestrate::{
    execute_one_intent, execute_one_intent_with_marker, gtd_poll_requires_fuse,
    list_runnable_intents, list_runnable_intents_by_phase, live_execute_enabled,
    poll_accepted_gtd_intent, EnvelopeFactory, OrchestrateError, OrchestrateOutcome,
    StandardSubmitAttemptMarker, SubmitAttemptMarker,
};

#[cfg(feature = "execute")]
pub use persistent::{
    assert_startup_clear as assert_persistent_startup_clear, ensure_fuse_clear,
    fuse_status as persistent_fuse_status, init_config as init_persistent_config,
    pause_fuse as pause_persistent_fuse, reconfigure_config as reconfigure_persistent_config,
    release_definitive_rejection, release_pre_boundary_failure, reserve_budget_and_mark_submitting,
    resolve_no_virtual_lot_sell_case, resolve_operator_confirmed_no_fill,
    resolve_chain_proven_gtd_no_fill,
    resolve_pre_submit_balance_case,
    resolve_exhausted_fak_no_match,
    resolve_exhausted_maker_only_crossing,
    restore_reservation_and_finalize_recovered_fill,
    resume_fuse as resume_persistent_fuse, rolling_reserved_total,
    PersistentError, PersistentRuntimeConfig, PersistentSubmitMarker, EXIT_BUDGET_STATE,
    EXIT_CONFIG, EXIT_FUSE_OPEN, EXIT_LOCK_COLLISION, EXIT_UNRESOLVED_RECOVERY,
};
