//! Core local-safety primitives for the copy-execution engine.
//!
//! `venue` has no order-submission surface (see that module's own doc
//! comment). The only order-writing code in this crate is the narrowly
//! scoped Phase 0.5 canary in `canary_run`, gated behind an explicit
//! operator-set environment variable — see `src/bin/canary_probe.rs`.

pub mod canary;
pub mod engine_lock;
pub mod venue;

#[cfg(feature = "intl_clob")]
pub mod canary_run;

#[cfg(feature = "intl_clob")]
pub mod ghost;

#[cfg(feature = "intl_clob")]
pub mod ghost_run;

pub use engine_lock::{EngineLock, EngineLockError};
pub use venue::{OrderReceipt, ReceiptError};

#[cfg(feature = "intl_clob")]
pub use ghost::{
    BalanceVerification, ExpectedTokenBalance, GhostSnapshot, GhostSnapshotError,
    GhostVerification, GhostVerifier, TokenBalanceVerification,
};

#[cfg(feature = "intl_clob")]
pub use venue::intl_clob::{
    IntlClobReadAdapter, OutcomeTokenId, OutcomeTokenIdError, StrictAccountBalanceReader,
    StrictCollateralError, StrictPositionError, StrictTokenBalanceReader,
};
