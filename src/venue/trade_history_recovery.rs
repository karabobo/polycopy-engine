//! Intl-CLOB-specific trade-history recovery: matching one prepared order
//! envelope against authenticated account trade history to recover a
//! submission whose outcome could not be established live.
//!
//! Two independent matchers live here, covering the two ways this project
//! submits orders (`docs/COPY_ENGINE_BLUEPRINT.md` section 10):
//! [`recover_fak_taker_order_from_trades`] recovers a FAK whose POST
//! response was lost before its venue order ID could be durably stored, by
//! matching the offline-precomputed `expected_taker_order_id` against
//! taker-role trades. [`recover_gtd_maker_order_from_trades`] recovers a
//! GTD/post-only maker order whose live status lookup failed
//! (`docs/poll-resting-gtd-404-permanently-blocks-startup.md`), by matching
//! the attempt's persisted `venue_order_id` (the signed order's precomputed
//! hash, recorded before submission) against maker-role trades'
//! per-order fill breakdown. Neither matcher accepts the other's envelope
//! shape or order-id source; see the "never falsely resolve" regression
//! tests in `copytrading::reconcile`.
//!
//! NEW-1 (post-P0-1 architecture review): this logic depends on
//! `venue::intl_clob` primitives (`AccountTrade`, `OutcomeTokenId`,
//! `StrictTradeHistoryError`, `StrictTradeHistoryReader`), so it must live
//! behind the `intl_clob` feature gate. The venue-neutral contract types it
//! operates on live in [`crate::venue::execution_contract`]; this module is
//! the Intl-CLOB realization of that contract's recovery surface.
//!
//! Re-exported by `copytrading::reconcile` for backward compatibility.

use std::{collections::HashMap, str::FromStr as _};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::venue::{
    execution_contract::PreparedOrderEnvelope,
    intl_clob::{
        AccountTrade, AccountTradeRole, AccountTradeSide, AccountTradeStatus, OutcomeTokenId,
        StrictTradeHistoryError, StrictTradeHistoryReader,
    },
    types::OrderId,
};

/// The bounded server-time range in which one attempt may have crossed the
/// venue boundary. It is persisted/constructed by the caller from the moment
/// the attempt was marked `submitting`; a later trade is never matched merely
/// because it happens to share a token and price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeHistoryWindow {
    after: DateTime<Utc>,
    before: DateTime<Utc>,
}

impl TradeHistoryWindow {
    pub fn new(
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> Result<Self, TradeHistoryRecoveryError> {
        if after > before {
            return Err(TradeHistoryRecoveryError::InvalidWindow);
        }

        Ok(Self { after, before })
    }

    pub fn after(&self) -> DateTime<Utc> {
        self.after
    }

    pub fn before(&self) -> DateTime<Utc> {
        self.before
    }

    fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        self.after <= timestamp && timestamp <= self.before
    }
}

/// Result of read-only lookup of a prepared FAK through authenticated trade
/// history. Only [`Self::Recovered`] may supply a venue order ID to a later,
/// still-strict order lookup. Every other outcome keeps the attempt uncertain
/// and must be reconciled rather than resubmitted.
#[derive(Debug, Clone, PartialEq)]
pub enum TradeHistoryLookup {
    Recovered {
        order_id: OrderId,
        filled_qty: Decimal,
        /// Sum of the matched outcome shares times their execution prices.
        /// This is the maker-side USDC principal that the rolling budget must
        /// retain after an operator recovers a real BUY fill.
        maker_notional_usdc: Decimal,
    },
    NotFound,
}

/// Strict failures while deciding whether account trade history identifies one
/// prepared envelope. None of these errors mean an order was absent.
#[derive(Debug, PartialEq)]
pub enum TradeHistoryRecoveryError {
    InvalidWindow,
    InvalidTokenId,
    InvalidSide,
    InvalidLimitPrice,
    UnsupportedOrderType,
    MissingOrderFingerprint,
    MissingMakerOrderId,
    InvalidSignedOrderJson,
    ConflictingDuplicateTrade { trade_id: String },
    Query(StrictTradeHistoryError),
}

impl std::fmt::Display for TradeHistoryRecoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidWindow => write!(formatter, "trade-history window ends before it starts"),
            Self::InvalidTokenId => write!(
                formatter,
                "prepared envelope has an invalid outcome token ID"
            ),
            Self::InvalidSide => write!(formatter, "prepared envelope has an invalid order side"),
            Self::InvalidLimitPrice => {
                write!(formatter, "prepared envelope has an invalid limit price")
            }
            Self::UnsupportedOrderType => write!(
                formatter,
                "trade-history recovery only supports FAK envelopes"
            ),
            Self::MissingOrderFingerprint => write!(
                formatter,
                "prepared envelope has no precomputed taker-order identifier"
            ),
            Self::MissingMakerOrderId => write!(
                formatter,
                "no persisted maker order ID was supplied to match against trade history"
            ),
            Self::InvalidSignedOrderJson => write!(
                formatter,
                "prepared envelope has no valid serialized signed-order payload"
            ),
            Self::ConflictingDuplicateTrade { trade_id } => write!(
                formatter,
                "trade history returned conflicting observations for trade {trade_id}"
            ),
            Self::Query(source) => write!(formatter, "strict trade-history query failed: {source}"),
        }
    }
}

impl std::error::Error for TradeHistoryRecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Query(source) => Some(source),
            _ => None,
        }
    }
}

/// Queries the authenticated account's complete trade history page stream for
/// this envelope's token and applies [`recover_fak_taker_order_from_trades`].
/// This method makes GET requests only; it has no signing, submission,
/// cancellation, allowance, or retry behavior.
pub async fn lookup_prepared_fak_in_trade_history<R>(
    reader: &R,
    envelope: &PreparedOrderEnvelope,
    window: TradeHistoryWindow,
) -> Result<TradeHistoryLookup, TradeHistoryRecoveryError>
where
    R: StrictTradeHistoryReader + ?Sized,
{
    let token_id = OutcomeTokenId::from_str(&envelope.token_id)
        .map_err(|_| TradeHistoryRecoveryError::InvalidTokenId)?;
    let trades = reader
        .trades_for_token_between(&token_id, window.after(), window.before())
        .await
        .map_err(TradeHistoryRecoveryError::Query)?;

    recover_fak_taker_order_from_trades(envelope, window, &trades)
}

/// Matches a FAK envelope against authenticated trade history without making a
/// network request. A filled FAK is expected to be the taker: every accepted
/// fill must have the precomputed `expected_taker_order_id` from the prepared
/// envelope. The matcher deliberately does **not** compare `trade.size` with
/// the envelope's `size` for BUY: the Phase 0.5 canary proved a BUY's
/// requested size is a budget cap, while a trade's size is actual matched
/// shares. Any missing fingerprint, unknown status/side/role, out-of-window
/// trade, limit-incompatible price, duplicate conflict, or zero result is
/// fail-closed.
pub fn recover_fak_taker_order_from_trades(
    envelope: &PreparedOrderEnvelope,
    window: TradeHistoryWindow,
    trades: &[AccountTrade],
) -> Result<TradeHistoryLookup, TradeHistoryRecoveryError> {
    if envelope.order_type != "FAK" {
        return Err(TradeHistoryRecoveryError::UnsupportedOrderType);
    }
    if envelope.expected_taker_order_id.trim().is_empty() {
        return Err(TradeHistoryRecoveryError::MissingOrderFingerprint);
    }
    if !matches!(
        serde_json::from_str::<serde_json::Value>(&envelope.signed_order_json),
        Ok(serde_json::Value::Object(_))
    ) {
        return Err(TradeHistoryRecoveryError::InvalidSignedOrderJson);
    }
    if envelope.token_id.is_empty() || !envelope.token_id.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(TradeHistoryRecoveryError::InvalidTokenId);
    }
    let side = match envelope.side.as_str() {
        "BUY" => AccountTradeSide::Buy,
        "SELL" => AccountTradeSide::Sell,
        _ => return Err(TradeHistoryRecoveryError::InvalidSide),
    };
    let limit_price = Decimal::from_str(&envelope.price)
        .ok()
        .filter(|price| *price > Decimal::ZERO && *price < Decimal::ONE)
        .ok_or(TradeHistoryRecoveryError::InvalidLimitPrice)?;

    let mut seen_by_trade_id: HashMap<&str, &AccountTrade> = HashMap::new();
    let mut filled_qty = Decimal::ZERO;
    let mut maker_notional_usdc = Decimal::ZERO;

    for trade in trades {
        if let Some(previous) = seen_by_trade_id.insert(&trade.trade_id, trade) {
            if previous != trade {
                return Err(TradeHistoryRecoveryError::ConflictingDuplicateTrade {
                    trade_id: trade.trade_id.clone(),
                });
            }
            continue;
        }

        let is_limit_compatible = match side {
            AccountTradeSide::Buy => trade.price <= limit_price,
            AccountTradeSide::Sell => trade.price >= limit_price,
            AccountTradeSide::Unknown => false,
        };
        if trade.taker_order_id != envelope.expected_taker_order_id
            || trade.token_id.to_string() != envelope.token_id
            || trade.side != side
            || trade.role != AccountTradeRole::Taker
            || !matches!(
                trade.status,
                AccountTradeStatus::Matched
                    | AccountTradeStatus::Mined
                    | AccountTradeStatus::Confirmed
            )
            || !window.contains(trade.match_time)
            || !is_limit_compatible
            || trade.size <= Decimal::ZERO
        {
            continue;
        }

        filled_qty += trade.size;
        maker_notional_usdc += trade.size * trade.price;
    }

    if filled_qty == Decimal::ZERO {
        Ok(TradeHistoryLookup::NotFound)
    } else {
        Ok(TradeHistoryLookup::Recovered {
            order_id: OrderId(envelope.expected_taker_order_id.clone()),
            filled_qty,
            maker_notional_usdc,
        })
    }
}

/// Queries the authenticated account's complete unfiltered trade history
/// page stream and applies [`recover_gtd_maker_order_from_trades`].
/// This method makes GET requests only; it has no signing, submission,
/// cancellation, allowance, or retry behavior.
pub async fn lookup_gtd_maker_fill_in_trade_history<R>(
    reader: &R,
    envelope: &PreparedOrderEnvelope,
    maker_order_id: &str,
    window: TradeHistoryWindow,
) -> Result<TradeHistoryLookup, TradeHistoryRecoveryError>
where
    R: StrictTradeHistoryReader + ?Sized,
{
    let token_id = OutcomeTokenId::from_str(&envelope.token_id)
        .map_err(|_| TradeHistoryRecoveryError::InvalidTokenId)?;
    // Real maker fills for two GTDs appeared only under the complementary
    // taker asset; an asset_id filter suppressed both exact-order matches.
    let trades = reader
        .trades_between_unfiltered(&token_id, window.after(), window.before())
        .await
        .map_err(TradeHistoryRecoveryError::Query)?;

    recover_gtd_maker_order_from_trades(envelope, maker_order_id, window, &trades)
}

/// Matches a GTD/post-only envelope against authenticated trade history
/// without making a network request. Unlike a FAK taker fill, a GTD maker
/// order's match key is read from `order_attempts.venue_order_id`, populated
/// with the signed order's precomputed hash by
/// `orchestrate::persist_expected_venue_order_id` before submission. The
/// submit response does not populate this column; the caller must independently
/// require an accepted attempt. The key is passed explicitly rather than
/// derived from `envelope.expected_taker_order_id` inside the matcher.
///
/// A single trade can sweep several resting maker orders (possibly
/// belonging to other accounts) in one match, so this sums only the
/// `AccountTrade::maker_orders` entries whose own `order_id` equals
/// `maker_order_id` -- it never uses the trade's top-level `size`/`price`,
/// which describe the taker's aggregate fill across every maker order in
/// that sweep, not this one order's. Any missing maker order ID, unknown
/// status/side/role, out-of-window trade, limit-incompatible fill,
/// duplicate conflict, or zero result is fail-closed. The trade's top-level
/// asset and side can refer to the complementary taker asset: match the maker
/// leg's asset and side, never the top-level fields.
pub fn recover_gtd_maker_order_from_trades(
    envelope: &PreparedOrderEnvelope,
    maker_order_id: &str,
    window: TradeHistoryWindow,
    trades: &[AccountTrade],
) -> Result<TradeHistoryLookup, TradeHistoryRecoveryError> {
    if envelope.order_type != "GTD" {
        return Err(TradeHistoryRecoveryError::UnsupportedOrderType);
    }
    if maker_order_id.trim().is_empty() {
        return Err(TradeHistoryRecoveryError::MissingMakerOrderId);
    }
    if !matches!(
        serde_json::from_str::<serde_json::Value>(&envelope.signed_order_json),
        Ok(serde_json::Value::Object(_))
    ) {
        return Err(TradeHistoryRecoveryError::InvalidSignedOrderJson);
    }
    if envelope.token_id.is_empty() || !envelope.token_id.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(TradeHistoryRecoveryError::InvalidTokenId);
    }
    let side = match envelope.side.as_str() {
        "BUY" => AccountTradeSide::Buy,
        "SELL" => AccountTradeSide::Sell,
        _ => return Err(TradeHistoryRecoveryError::InvalidSide),
    };
    let limit_price = Decimal::from_str(&envelope.price)
        .ok()
        .filter(|price| *price > Decimal::ZERO && *price < Decimal::ONE)
        .ok_or(TradeHistoryRecoveryError::InvalidLimitPrice)?;

    let mut seen_by_trade_id: HashMap<&str, &AccountTrade> = HashMap::new();
    let mut filled_qty = Decimal::ZERO;
    let mut maker_notional_usdc = Decimal::ZERO;

    for trade in trades {
        if let Some(previous) = seen_by_trade_id.insert(&trade.trade_id, trade) {
            if previous != trade {
                return Err(TradeHistoryRecoveryError::ConflictingDuplicateTrade {
                    trade_id: trade.trade_id.clone(),
                });
            }
            continue;
        }

        if trade.role != AccountTradeRole::Maker
            || !matches!(
                trade.status,
                AccountTradeStatus::Matched
                    | AccountTradeStatus::Mined
                    | AccountTradeStatus::Confirmed
            )
            || !window.contains(trade.match_time)
        {
            continue;
        }

        for fill in &trade.maker_orders {
            if fill.order_id != maker_order_id
                || fill.asset_id.to_string() != envelope.token_id
                || fill.side != side
                || fill.matched_amount <= Decimal::ZERO
            {
                continue;
            }
            // The venue reports executed USDC (6 dp) divided by shares
            // (6 dp) as a high-precision effective price. A 1-atomic-unit
            // rounding artifact must not hide an exact-hash maker fill.
            let executed_usdc = (fill.price * fill.matched_amount)
                .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::MidpointAwayFromZero);
            let limit_usdc = fill.matched_amount * limit_price;
            let is_limit_compatible = match side {
                AccountTradeSide::Buy => executed_usdc <= limit_usdc + Decimal::new(1, 6),
                AccountTradeSide::Sell => executed_usdc + Decimal::new(1, 6) >= limit_usdc,
                AccountTradeSide::Unknown => false,
            };
            if !is_limit_compatible {
                continue;
            }

            filled_qty += fill.matched_amount;
            maker_notional_usdc += executed_usdc;
        }
    }

    if filled_qty == Decimal::ZERO {
        Ok(TradeHistoryLookup::NotFound)
    } else {
        Ok(TradeHistoryLookup::Recovered {
            order_id: OrderId(maker_order_id.to_owned()),
            filled_qty,
            maker_notional_usdc,
        })
    }
}
