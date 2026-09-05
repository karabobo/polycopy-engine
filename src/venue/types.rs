//! Venue-facing types that must not live inside the copytrading layer.
//!
//! AGENTS.md architecture boundary: venue implementations must not depend
//! on `copytrading::*`. `OrderId` and `VenueOrderState` are the minimal
//! surface that both `reconcile` (the recovery matrix) and `orchestrate`
//! (the execution matrix) need to share. They are therefore defined in
//! the neutral `venue` module and re-exported by `reconcile` for
//! backward compatibility.

use rust_decimal::Decimal;

/// A venue order identifier. The concrete format is venue-specific
/// (e.g. a 0x-prefixed hex string on the Polymarket CLOB). Stored as a
/// `String` so the type remains serializable without forcing a particular
/// encoding at the type level.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OrderId(pub String);

/// The subset of a venue order's state that the orchestrator needs to
/// decide recovery actions. The `size_matched` field is authoritative for
/// lot accounting — only a confirmed non-zero `size_matched` on a
/// terminal status may credit or decrement a virtual lot.
#[derive(Clone, Debug)]
pub struct VenueOrderState {
    pub order_id: OrderId,
    pub status: String,
    pub size_matched: Decimal,
}

