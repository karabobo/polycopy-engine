//! Phase 1 (durable account/leader state, section 6) through Phase 5
//! (prepared submission and reconciliation, section 10) of
//! `docs/COPY_ENGINE_BLUEPRINT.md`.
//!
//! Schema covers every table blueprint section 6 lists.
//! `reconcile::CopyExecution` (Phase 5) and `execute::OrderSubmitter`
//! (Phase 4) both have no implementation anywhere in this crate outside
//! test code: neither this project nor its assistant ever submits a live
//! order.

pub mod db;
pub mod plan;

#[cfg(feature = "execute")]
pub mod execute;

#[cfg(feature = "execute")]
pub mod reconcile;

#[cfg(feature = "ingest")]
pub mod ingest;

pub use db::{open, open_and_migrate, DbError};
pub use plan::{plan_next_batch, plan_next_batch_with_limit, PlanError, PlanSummary, PolicySnapshot};

#[cfg(feature = "execute")]
pub use execute::{execute_intent, finalize_receipt, ExecuteError, ExecutionOutcome, OrderSubmitter, Side, SizedDecision};

#[cfg(feature = "execute")]
pub use reconcile::{
    attempts_in_window, load_or_prepare_attempt, open_reconciliation_case, permitted_recovery_action,
    CopyExecution, OrderId, PreparedOrderEnvelope, ReconcileError, RecoveryAction, VenueOrderState,
    MAX_ATTEMPTS_PER_WINDOW, RETRY_WINDOW_SECONDS,
};
