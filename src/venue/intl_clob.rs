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
    use std::{
        io::{Read as _, Write as _},
        net::TcpListener,
        thread,
    };

    use chrono::TimeZone as _;
    use polymarket_client_sdk_v2::{
        auth::{Credentials, LocalSigner, Signer as _},
        clob::{types::SignatureType, Client, Config},
        POLYGON,
    };

    use super::*;

    fn trade_history_server(
        responses: [(&'static str, &'static str); 3],
    ) -> (String, thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener must bind");
        let host = format!(
            "http://{}",
            listener.local_addr().expect("listener address")
        );
        let worker = thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().expect("test client must connect");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut buffer).expect("request must be readable");
                    assert!(read > 0, "request must contain complete headers");
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                requests.push(String::from_utf8(request).expect("request must be UTF-8"));
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                )
                .expect("test response must be writable");
            }
            requests
        });
        (host, worker)
    }

    async fn authenticated_test_client(host: &str) -> Client<Authenticated<Normal>> {
        let signer = LocalSigner::from_str(
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8417e4edc86cb4f5d5",
        )
        .expect("test signing key must parse")
        .with_chain_id(Some(POLYGON));
        let client = Client::new(host, Config::default()).expect("test client must initialize");
        client
            .authentication_builder(&signer)
            .credentials(Credentials::new(
                "ffffffff-ffff-ffff-ffff-ffffffffffff"
                    .parse()
                    .expect("test API key must parse"),
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ))
            .signature_type(SignatureType::Eoa)
            .authenticate()
            .await
            .expect("test client must authenticate locally")
    }

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

    #[tokio::test]
    async fn delayed_trade_history_page_with_empty_fee_metadata_is_strictly_readable() {
        const NOT_FOUND: &str = r#"{"error":"order not found"}"#;
        const EMPTY_PAGE: &str = r#"{"data":[],"limit":100,"count":0,"next_cursor":"LTE="}"#;
        const MATCHED_PAGE: &str = r#"{"data":[{"id":"trade-a","taker_order_id":"order-a","market":"0x000000000000000000000000000000000000000000000000000000006d61726b","asset_id":"123456","side":"BUY","size":"5.288460","fee_rate_bps":"","price":"0.20","status":"MATCHED","match_time":"1788264001","last_update":"1788264002","outcome":"YES","bucket_index":0,"owner":"ffffffff-ffff-ffff-ffff-ffffffffffff","maker_address":"0x2222222222222222222222222222222222222222","maker_orders":[{"order_id":"maker-a","owner":"ffffffff-ffff-ffff-ffff-ffffffffffff","maker_address":"0x4444444444444444444444444444444444444444","matched_amount":"5.288460","price":"0.20","fee_rate_bps":"","asset_id":"123456","outcome":"YES","side":"SELL"}],"transaction_hash":"","trader_side":"TAKER"}],"limit":100,"count":1,"next_cursor":"LTE="}"#;

        let (host, server) = trade_history_server([
            ("404 Not Found", NOT_FOUND),
            ("200 OK", EMPTY_PAGE),
            ("200 OK", MATCHED_PAGE),
        ]);
        let client = authenticated_test_client(&host).await;
        assert!(
            client.order("order-a").await.is_err(),
            "an absent immediate by-ID record must remain a strict failure, not an empty receipt"
        );
        let reader = IntlClobReadAdapter::new(client);
        let token = OutcomeTokenId::from_str("123456").expect("valid fixture token");
        let after = Utc
            .with_ymd_and_hms(2026, 9, 1, 12, 0, 0)
            .single()
            .expect("valid start time");
        let before = Utc
            .with_ymd_and_hms(2026, 9, 1, 12, 1, 0)
            .single()
            .expect("valid end time");

        assert!(reader
            .trades_for_token_between(&token, after, before)
            .await
            .expect("initial delayed page must be a successful empty read")
            .is_empty());
        let recovered = reader
            .trades_for_token_between(&token, after, before)
            .await
            .expect("later matched page must deserialize despite empty fee metadata");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].taker_order_id, "order-a");
        assert_eq!(recovered[0].size, Decimal::new(5_288_460, 6));

        let requests = server.join().expect("test server must complete");
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("GET /data/order/order-a "));
        assert!(requests[1..]
            .iter()
            .all(|request| request.starts_with("GET /data/trades?")));
        assert!(requests[1..]
            .iter()
            .all(|request| request.contains("asset_id=123456")));
    }
}
