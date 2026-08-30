//! Strict, read-only Polymarket Intl CLOB boundary.
//!
//! This adapter deliberately exposes only a per-token balance query. It has no
//! order construction, signing, submission, cancellation, retry, or envelope
//! lookup method. Those methods remain prohibited until the Phase 0.5 canary
//! proves the necessary submission and reconciliation behavior.

use std::{error::Error, fmt, str::FromStr};

use async_trait::async_trait;
use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, Normal},
    clob::{types::request::BalanceAllowanceRequest, types::AssetType, Client},
    error::Error as SdkError,
    types::{Decimal, U256},
};

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
            .map(|response| response.balance)
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
            .map(|response| response.balance)
            .map_err(|source| StrictCollateralError::Query { source })
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
        }
    }
}

impl Error for StrictCollateralError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Query { source } => Some(source),
        }
    }
}
