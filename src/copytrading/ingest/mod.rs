//! Phase 2: Activity-led ingestion. See `docs/COPY_ENGINE_BLUEPRINT.md`
//! section 7.
//!
//! `address_resolver` and `normalize` are pure and fully unit-tested.
//! `activity_ws` is the network-facing connection manager built on top of
//! them; its live-connection behavior is not unit-testable the way the pure
//! modules are, matching this project's existing precedent for
//! network-touching code (see `ghost_run.rs`, `canary_run.rs`).

pub mod activity_ws;
pub mod address_resolver;
pub mod apply;
pub mod backfill;
pub mod normalize;

pub use activity_ws::{process_message, run, ActivityWsError, RTDS_URL};
pub use address_resolver::AddressResolver;
pub use apply::{apply_trade, ProcessOutcome};
pub use backfill::{backfill_leader, BackfillError, BackfillSummary};
pub use normalize::{NormalizedTrade, ParseResult, TradeSide};
