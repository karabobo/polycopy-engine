//! Phase 1: durable account/leader state. See
//! `docs/COPY_ENGINE_BLUEPRINT.md` section 6.
//!
//! This module holds only connection setup and schema so far (`accounts`,
//! `leader_config`, `leader_wallet_aliases`); the event ledger, intent
//! planner, and executor described later in section 6 are not built yet.

pub mod db;

pub use db::{open, open_and_migrate, DbError};
