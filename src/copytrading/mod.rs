//! Phase 1 (durable account/leader state, section 6), Phase 2 (activity
//! ingestion, section 7), and Phase 3 (transactional intent planning,
//! section 8) of `docs/COPY_ENGINE_BLUEPRINT.md`.
//!
//! Schema covers every table blueprint section 6 lists. The fixed-lane
//! executor and prepared-submission/reconciliation layer (Phases 4-5) are
//! not built yet.

pub mod db;
pub mod plan;

#[cfg(feature = "ingest")]
pub mod ingest;

pub use db::{open, open_and_migrate, DbError};
pub use plan::{plan_next_batch, plan_next_batch_with_limit, PlanError, PlanSummary};
