//! The venue-side execution contract shared by `copytrading::reconcile`
//! (policy/recovery) and `venue::intl_clob_exec` (the live adapter).
//!
//! P0-1 architecture inversion (AGENTS.md: "venue 不向上依赖
//! copytrading"): the venue layer must never import from
//! `crate::copytrading::*`. Before this module existed,
//! `venue::intl_clob_exec` imported `OrderId`, `VenueOrderState`,
//! `SubmitError`, `PreparedOrderEnvelope`, the `CopyExecution` trait, and
//! the trade-history recovery helpers from
//! `copytrading::reconcile` — an upward dependency that made the venue
//! layer unable to compile or be tested without the entire copytrading
//! policy stack.
//!
//! Everything in this module is venue-neutral and compiles with **no**
//! features enabled: [`Side`] / [`SizedDecision`] (the order specification
//! a venue must be able to execute — `intent_id` is provenance metadata
//! only), [`PreparedOrderEnvelope`] (the persisted signed-order
//! representation, blueprint invariant #5), [`SubmitError`] (the
//! Local/Transport/Rejected classification the venue adapter *produces*
//! and the orchestrator *consumes*), and the [`CopyExecution`] trait (the
//! seam the venue adapter implements and the orchestrator consumes).
//!
//! The Intl-CLOB-specific trade-history recovery half of the original
//! module (matching a prepared envelope against `AccountTrade` streams)
//! depends on `venue::intl_clob` primitives and now lives in
//! [`crate::venue::trade_history_recovery`] behind the `intl_clob` gate —
//! NEW-1 (post-P0-1 review): an ungated module must not import a
//! feature-gated one.
//!
//! `copytrading::reconcile` and `copytrading::execute` re-export all of
//! these names, so existing call sites continue to compile unchanged.
//! The canonical definitions live here.

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::venue::{
    types::{OrderId, VenueOrderState},
    OrderReceipt,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

/// The request-side amount handed to the CLOB builder. This deliberately
/// models the venue contract rather than a policy decision: a marketable BUY
/// is maker-side USDC, while a SELL is maker-side outcome-token shares.
/// Canary and production keep independent SDK construction code, but must
/// agree on this value before either signs an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClobOrderAmount {
    BuyMakerUsdc(Decimal),
    SellMakerShares(Decimal),
}

impl Side {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }

    pub(crate) fn from_str(raw: &str) -> Option<Self> {
        match raw {
            "BUY" => Some(Self::Buy),
            "SELL" => Some(Self::Sell),
            _ => None,
        }
    }
}

/// One already-sized, already-priced decision ready to submit. Everything
/// on this struct is already durably persisted on `copy_intents` by the
/// time a caller has one -- Phase 5's real submitter reads it back from
/// there, not from an in-memory value that a crash could lose.
#[derive(Debug, Clone)]
pub struct SizedDecision {
    pub intent_id: i64,
    pub token_id: String,
    pub side: Side,
    pub qty: Decimal,
    pub limit_price: Decimal,
    /// BUYs are submitted as a USDC-denominated marketable FAK. This is the
    /// exact maximum maker amount, rounded down to cents before signing. It
    /// is absent for SELLs, whose request unit remains outcome-token shares.
    pub buy_budget: Option<Decimal>,
}

/// The exact, plainly-serializable fields of one signed order attempt.
/// Persisted once per `(intent_id, attempt_number)` and never rebuilt
/// (blueprint invariant #5): a fresh salt would produce a different signed
/// order hash, defeating the entire point of "one immutable envelope per
/// attempt".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedOrderEnvelope {
    pub token_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    /// Exact BUY maker budget in USDC. Kept separately from `size`, which is
    /// the expected taker/share quantity in the signed order. `None` is
    /// retained only to deserialize historical share-denominated envelopes;
    /// new BUY envelopes must populate it.
    #[serde(default)]
    pub buy_budget_usdc: Option<String>,
    pub salt: u64,
    /// Always "FAK" in v1 (blueprint section 8's stated v1-wide policy).
    pub order_type: String,
    /// The deterministic order identifier calculated from the exact signed
    /// wire envelope *before* it crosses the HTTP boundary. A response may be
    /// lost, but this value must not depend on that response. Phase 0.5 still
    /// has to prove that it equals the CLOB history endpoint's
    /// `taker_order_id` for the real FAK path.
    pub expected_taker_order_id: String,
    /// Exact serialized signed-order wire payload, retained only in the
    /// local order-attempt database for forensic replay/reconciliation. It
    /// contains all version-specific maker/signer/amount/expiry/signature
    /// fields and must never be committed or logged.
    pub signed_order_json: String,
}

/// What Phase 5 needs to actually talk to the venue. Only
/// `position_for_token_strict` and the two lookup methods are read-only;
/// `submit_exact_envelope` is the one order-writing call in this trait, and
/// **no implementation of it exists anywhere in this crate's non-test
/// code**. A real implementation would sign and POST a live order -- see
/// `copytrading::reconcile`'s module doc comment.
pub trait CopyExecution {
    fn position_for_token_strict(
        &self,
        token_id: &str,
    ) -> impl std::future::Future<Output = Result<Decimal, String>> + Send;

    fn order_for_receipt(
        &self,
        order_id: &OrderId,
    ) -> impl std::future::Future<Output = Result<VenueOrderState, String>> + Send;

    fn query_prepared_envelope(
        &self,
        envelope: &PreparedOrderEnvelope,
    ) -> impl std::future::Future<Output = Result<Option<OrderReceipt>, String>> + Send;

    fn submit_exact_envelope(
        &self,
        envelope: &PreparedOrderEnvelope,
    ) -> impl std::future::Future<Output = Result<OrderReceipt, SubmitError>> + Send;
}

/// Distinguishes a local failure (the request never left this process) from
/// a transport failure (the request may have crossed the venue boundary)
/// from a definitive venue rejection (no `order_id` was created).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitError {
    /// Reconstruction, validation, or other local work failed. The attempt
    /// must not be marked `uncertain` because nothing was submitted.
    Local(String),
    /// A network/timeout/5xx error after the request may have been sent.
    /// The attempt becomes `uncertain` and is never retried automatically.
    Transport(String),
    /// The venue processed the request and refused it before creating an
    /// order (HTTP 4xx, including the live `invalid. Duplicated.` case).
    Rejected(String),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(detail) => write!(formatter, "local submission error: {detail}"),
            Self::Transport(detail) => write!(formatter, "transport submission error: {detail}"),
            Self::Rejected(detail) => write!(formatter, "venue rejected order: {detail}"),
        }
    }
}

impl std::error::Error for SubmitError {}
