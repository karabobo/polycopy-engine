//! Core local-safety primitives for the copy-execution engine.
//!
//! This crate intentionally has no venue client or order-submission surface
//! until the Phase 0.5 and Phase 7 financial-correctness gates are satisfied.

pub mod engine_lock;
pub mod venue;

pub use engine_lock::{EngineLock, EngineLockError};
pub use venue::{OrderReceipt, ReceiptError};

#[cfg(feature = "intl_clob")]
pub use venue::intl_clob::{
    IntlClobReadAdapter, OutcomeTokenId, OutcomeTokenIdError, StrictPositionError,
    StrictTokenBalanceReader,
};
