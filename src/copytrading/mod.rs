//! Phase 1 (durable account/leader state, section 6), Phase 2 (activity
//! ingestion, section 7), Phase 3 (transactional intent planning, section
//! 8), and Phase 4 (fixed-lane executor, section 9) of
//! `docs/COPY_ENGINE_BLUEPRINT.md`.
//!
//! Schema covers every table blueprint section 6 lists. The
//! prepared-submission/reconciliation layer (Phase 5) is not built yet --
//! `execute::OrderSubmitter` is Phase 4's seam for it.

pub mod db;
pub mod plan;

#[cfg(feature = "execute")]
pub mod execute;

#[cfg(feature = "ingest")]
pub mod ingest;

pub use db::{open, open_and_migrate, DbError};
pub use plan::{plan_next_batch, plan_next_batch_with_limit, PlanError, PlanSummary, PolicySnapshot};

#[cfg(feature = "execute")]
pub use execute::{execute_intent, finalize_receipt, ExecuteError, ExecutionOutcome, OrderSubmitter, Side, SizedDecision};
