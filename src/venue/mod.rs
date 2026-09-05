//! Venue-neutral types and the gated Intl CLOB adapters.

mod receipt;

// P0-1: venue-neutral shared types (OrderId, VenueOrderState). Defined
// here — not in copytrading — so venue implementations never depend
// upward on the copytrading layer (AGENTS.md architecture boundary).
pub mod types;

// P0-1: the venue-side execution contract (Side, SizedDecision,
// PreparedOrderEnvelope, SubmitError, CopyExecution). Canonical home of the
// seam both copytrading (policy) and intl_clob_exec (live adapter) share;
// copytrading re-exports for backward compatibility. Ungated: these types
// are venue-neutral and import nothing feature-gated (NEW-1).
pub mod execution_contract;

// NEW-1: the Intl-CLOB-specific trade-history recovery half (matches a
// prepared envelope against AccountTrade streams). Gated: it depends on
// venue::intl_clob primitives, so declaring it ungated broke every
// feature combination without intl_clob.
#[cfg(feature = "intl_clob")]
pub mod trade_history_recovery;

#[cfg(feature = "intl_clob")]
pub mod intl_clob;

#[cfg(feature = "intl_clob")]
pub mod order_hash;

#[cfg(feature = "intl_clob")]
pub mod signed_order;

#[cfg(feature = "execute")]
#[cfg(feature = "intl_clob")]
pub mod intl_clob_exec;

pub use receipt::{OrderReceipt, ReceiptError};
