//! Venue-neutral types that preserve financial quantities at the adapter edge.
//!
//! This module contains no SDK client, signing, HTTP, or order-submission code.
//! Those capabilities remain gated by the Phase 0.5 canary.

mod receipt;

#[cfg(feature = "intl_clob")]
pub mod intl_clob;

pub use receipt::{OrderReceipt, ReceiptError};
