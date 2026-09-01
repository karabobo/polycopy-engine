//! Strict, read-only Polymarket Intl CLOB boundary.
//!
//! This adapter exposes strict per-token balances and authenticated trade
//! history reads. It has no order construction, signing, submission,
//! cancellation, retry, or automatic envelope resolution method. Those
//! capabilities remain prohibited until the Phase 0.5 canary proves the
//! necessary submission and reconciliation behavior.

use std::{error::Error, fmt, str::FromStr};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt as _;
use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, Normal},
    clob::{
        types::{
            request::{BalanceAllowanceRequest, TradesRequest},
            response::TradeResponse,
            AssetType, Side as SdkSide, TradeStatusType, TraderSide,
        },
        Client,
    },
    error::Error as SdkError,
    types::{Decimal, U256},
};

/// CLOB balance-allowance responses use the collateral/token's six-decimal
/// atomic units even though the SDK deserializes them into `Decimal`. Every
/// ledger, policy, and GHOST value in this project is expressed in human
/// units, so this conversion belongs at the venue boundary.
const CLOB_BALANCE_ATOMIC_SCALE: i64 = 1_000_000;

fn from_clob_atomic_units(raw: Decimal) -> Decimal {
    raw / Decimal::from(CLOB_BALANCE_ATOMIC_SCALE)
}

/// An outcome-token ID, not a market-wide condition ID.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OutcomeTokenId(U256);

impl OutcomeTokenId {
    fn as_sdk_token_id(&self) -> U256 {
        self.0
    }
}

impl fmt::Display for OutcomeTokenId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for OutcomeTokenId {
    type Err = OutcomeTokenIdError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(OutcomeTokenIdError::Invalid(raw.to_owned()));
        }

        raw.parse::<U256>()
            .map(Self)
            .map_err(|_| OutcomeTokenIdError::Invalid(raw.to_owned()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutcomeTokenIdError {
    Invalid(String),
}

impl fmt::Display for OutcomeTokenIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(raw) => write!(formatter, "invalid Polymarket outcome token ID: {raw}"),
        }
    }
}

impl Error for OutcomeTokenIdError {}

/// A strict position query: any venue error remains an error, never zero.
#[async_trait]
pub trait StrictTokenBalanceReader: Send + Sync {
    async fn position_for_token_strict(
        &self,
        token_id: &OutcomeTokenId,
    ) -> Result<Decimal, StrictPositionError>;
}

/// Strict account-level reads required before an account can leave GHOST mode.
///
/// The collateral query is deliberately separate from outcome-token queries:
/// neither a missing token nor a venue error may be inferred from the other.
#[async_trait]
pub trait StrictAccountBalanceReader: StrictTokenBalanceReader {
    async fn collateral_balance_strict(&self) -> Result<Decimal, StrictCollateralError>;

    /// Conservative usable collateral allowance.  The venue returns one or
    /// more exchange allowances; the minimum reported value is used so an
    /// order is never sent on the assumption that a different exchange
    /// contract is approved.  Missing or malformed allowance data is a hard
    /// error, not an implicit unlimited allowance.
    async fn collateral_allowance_strict(&self) -> Result<Decimal, StrictCollateralError>;
}

/// Strict authenticated account-trade history. A failure is never an empty
/// history and must be handled as an uncertain submission by the caller.
#[async_trait]
pub trait StrictTradeHistoryReader: Send + Sync {
    async fn trades_for_token_between(
        &self,
        token_id: &OutcomeTokenId,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> Result<Vec<AccountTrade>, StrictTradeHistoryError>;
}

/// Read-only adapter for an already authenticated official CLOB SDK client.
///
/// Authentication is intentionally performed outside this type. This avoids
/// accepting private keys, persisting credentials, or constructing any signed
/// order in the copy engine's Phase 0 surface.
#[derive(Clone, Debug)]
pub struct IntlClobReadAdapter {
    client: Client<Authenticated<Normal>>,
}

impl IntlClobReadAdapter {
    pub fn new(client: Client<Authenticated<Normal>>) -> Self {
        Self { client }
    }

    /// Returns every authenticated-account trade for one token in the supplied
    /// server-side time window. This is a strict read: a request or pagination
    /// failure remains an error and must never be interpreted as no trade.
    ///
    /// The caller still has to prove that a returned trade belongs to one
    /// particular prepared envelope. The copy-execution layer does that with
    /// a fail-closed exact-order-ID rule; this adapter deliberately exposes
    /// raw, read-only venue observations rather than guessing.
    pub async fn trades_for_token_between(
        &self,
        token_id: &OutcomeTokenId,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> Result<Vec<AccountTrade>, StrictTradeHistoryError> {
        if after > before {
            return Err(StrictTradeHistoryError::InvalidWindow);
        }

        let request = TradesRequest::builder()
            .asset_id(token_id.as_sdk_token_id())
            .after(after.timestamp())
            .before(before.timestamp())
            .build();
        let trades: Vec<TradeResponse> = self
            .client
            .stream_data(|client, cursor| client.trades(&request, cursor))
            .try_collect()
            .await
            .map_err(|source| StrictTradeHistoryError::Query {
                token_id: token_id.clone(),
                source,
            })?;

        Ok(trades.into_iter().map(AccountTrade::from).collect())
    }
}

#[async_trait]
impl StrictTradeHistoryReader for IntlClobReadAdapter {
    async fn trades_for_token_between(
        &self,
        token_id: &OutcomeTokenId,
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> Result<Vec<AccountTrade>, StrictTradeHistoryError> {
        Self::trades_for_token_between(self, token_id, after, before).await
    }
}

/// One authenticated-account trade returned by the CLOB trade-history
/// endpoint. It intentionally retains the venue's taker order ID: a filled
/// FAK is only recoverable when a later, read-only query can identify exactly
/// one such ID for the prepared envelope.
#[derive(Clone, Debug, PartialEq)]
pub struct AccountTrade {
    pub trade_id: String,
    pub taker_order_id: String,
    pub token_id: OutcomeTokenId,
    pub side: AccountTradeSide,
    pub price: Decimal,
    pub size: Decimal,
    pub match_time: DateTime<Utc>,
    pub role: AccountTradeRole,
    pub status: AccountTradeStatus,
}

impl From<TradeResponse> for AccountTrade {
    fn from(trade: TradeResponse) -> Self {
        let side = match trade.side {
            SdkSide::Buy => AccountTradeSide::Buy,
            SdkSide::Sell => AccountTradeSide::Sell,
            SdkSide::Unknown | _ => AccountTradeSide::Unknown,
        };
        let role = match trade.trader_side {
            TraderSide::Taker => AccountTradeRole::Taker,
            TraderSide::Maker => AccountTradeRole::Maker,
            TraderSide::Unknown(_) | _ => AccountTradeRole::Unknown,
        };
        let status = match trade.status {
            TradeStatusType::Matched => AccountTradeStatus::Matched,
            TradeStatusType::Mined => AccountTradeStatus::Mined,
            TradeStatusType::Confirmed => AccountTradeStatus::Confirmed,
            TradeStatusType::Retrying => AccountTradeStatus::Retrying,
            TradeStatusType::Failed => AccountTradeStatus::Failed,
            TradeStatusType::Unknown(_) | _ => AccountTradeStatus::Unknown,
        };

        Self {
            trade_id: trade.id,
            taker_order_id: trade.taker_order_id,
            token_id: OutcomeTokenId(trade.asset_id),
            side,
            price: trade.price,
            size: trade.size,
            match_time: trade.match_time,
            role,
            status,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountTradeSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountTradeRole {
    Taker,
    Maker,
    Unknown,
}

/// Trade lifecycle state copied from the CLOB response. Unknown and failed
/// states cannot prove a fill and are excluded from automatic recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountTradeStatus {
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
    Unknown,
}

/// A strict account-trade query failure. It is intentionally distinct from an
/// empty trade list, because a failed query cannot prove that no submitted
/// order exists.
#[derive(Debug)]
pub enum StrictTradeHistoryError {
    InvalidWindow,
    Query {
        token_id: OutcomeTokenId,
        source: SdkError,
    },
}

impl fmt::Display for StrictTradeHistoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWindow => write!(formatter, "trade-history window ends before it starts"),
            Self::Query { token_id, source } => write!(
                formatter,
                "strict authenticated trade-history query failed for {token_id}: {source}"
            ),
        }
    }
}

impl Error for StrictTradeHistoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidWindow => None,
            Self::Query { source, .. } => Some(source),
        }
    }
}

#[async_trait]
impl StrictTokenBalanceReader for IntlClobReadAdapter {
    async fn position_for_token_strict(
        &self,
        token_id: &OutcomeTokenId,
    ) -> Result<Decimal, StrictPositionError> {
        let request = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Conditional)
            .token_id(token_id.as_sdk_token_id())
            .build();

        self.client
            .balance_allowance(request)
            .await
            .map(|response| from_clob_atomic_units(response.balance))
            .map_err(|source| StrictPositionError::Query {
                token_id: token_id.clone(),
                source,
            })
    }
}

#[async_trait]
impl StrictAccountBalanceReader for IntlClobReadAdapter {
    async fn collateral_balance_strict(&self) -> Result<Decimal, StrictCollateralError> {
        let request = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Collateral)
            .build();

        self.client
            .balance_allowance(request)
            .await
            .map(|response| from_clob_atomic_units(response.balance))
            .map_err(|source| StrictCollateralError::Query { source })
    }

    async fn collateral_allowance_strict(&self) -> Result<Decimal, StrictCollateralError> {
        let request = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Collateral)
            .build();

        let response = self
            .client
            .balance_allowance(request)
            .await
            .map_err(|source| StrictCollateralError::Query { source })?;

        response
            .allowances
            .into_values()
            .map(|raw| {
                raw.parse::<Decimal>()
                    .map(from_clob_atomic_units)
                    .map_err(|_| StrictCollateralError::InvalidAllowance { raw })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .min()
            .ok_or(StrictCollateralError::MissingAllowance)
    }
}

#[derive(Debug)]
pub enum StrictPositionError {
    Query {
        token_id: OutcomeTokenId,
        source: SdkError,
    },
}

impl fmt::Display for StrictPositionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query { token_id, source } => write!(
                formatter,
                "strict conditional-token balance query failed for {token_id}: {source}"
            ),
        }
    }
}

impl Error for StrictPositionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Query { source, .. } => Some(source),
        }
    }
}

/// A strict collateral query: any venue error remains an error, never zero.
#[derive(Debug)]
pub enum StrictCollateralError {
    Query { source: SdkError },
    MissingAllowance,
    InvalidAllowance { raw: String },
}

impl fmt::Display for StrictCollateralError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query { source } => {
                write!(
                    formatter,
                    "strict collateral balance query failed: {source}"
                )
            }
            Self::MissingAllowance => write!(
                formatter,
                "strict collateral allowance query returned no exchange allowances"
            ),
            Self::InvalidAllowance { raw } => write!(
                formatter,
                "strict collateral allowance query returned an invalid allowance: {raw}"
            ),
        }
    }
}

impl Error for StrictCollateralError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Query { source } => Some(source),
            Self::MissingAllowance | Self::InvalidAllowance { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balance_allowance_atomic_units_are_normalized_before_entering_the_ledger() {
        assert_eq!(
            from_clob_atomic_units(Decimal::from(4_125_747_i64)),
            Decimal::new(4_125_747, 6)
        );
        assert_eq!(
            from_clob_atomic_units(Decimal::from(10_000_000_i64)),
            Decimal::from(10_i64)
        );
    }
}
