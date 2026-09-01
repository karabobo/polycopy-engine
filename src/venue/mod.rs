//! Venue-neutral types and the gated Intl CLOB adapters.

mod receipt;

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
