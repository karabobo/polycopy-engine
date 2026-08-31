//! Phase 1 (durable account/leader state, section 6) and Phase 2 (activity
//! ingestion, section 7) of `docs/COPY_ENGINE_BLUEPRINT.md`.
//!
//! Schema covers every table blueprint section 6 lists. The intent planner
//! and executor (Phases 3-5) are not built yet.

pub mod db;

#[cfg(feature = "ingest")]
pub mod ingest;

pub use db::{open, open_and_migrate, DbError};
