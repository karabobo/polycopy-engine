//! Read-only GHOST verification for Phase 0.
//!
//! This module compares strict CLOB reads with a timestamped, manually
//! collected wallet snapshot. It has no credential, persistence, order, or
//! retry surface. Comparisons are exact: a tolerance must be explicitly
//! designed and approved before it can be introduced.

use std::{collections::HashSet, error::Error, fmt};

use polymarket_client_sdk_v2::types::Decimal;

use crate::{
    OutcomeTokenId, StrictAccountBalanceReader, StrictCollateralError, StrictPositionError,
};

/// A manually captured account snapshot to compare with CLOB balances.
#[derive(Clone, Debug, PartialEq)]
pub struct GhostSnapshot {
    collateral: Decimal,
    token_balances: Vec<ExpectedTokenBalance>,
}

impl GhostSnapshot {
    /// Creates an exact, non-negative snapshot with no repeated outcome token.
    pub fn new(
        collateral: Decimal,
        token_balances: Vec<ExpectedTokenBalance>,
    ) -> Result<Self, GhostSnapshotError> {
        if collateral.is_sign_negative() {
            return Err(GhostSnapshotError::NegativeCollateral { collateral });
        }

        let mut seen_tokens = HashSet::with_capacity(token_balances.len());
        for token_balance in &token_balances {
            if token_balance.balance.is_sign_negative() {
                return Err(GhostSnapshotError::NegativeTokenBalance {
                    token_id: token_balance.token_id.clone(),
                    balance: token_balance.balance,
                });
            }

            if !seen_tokens.insert(token_balance.token_id.clone()) {
                return Err(GhostSnapshotError::DuplicateToken {
                    token_id: token_balance.token_id.clone(),
                });
            }
        }

        Ok(Self {
            collateral,
            token_balances,
        })
    }

    pub fn collateral(&self) -> Decimal {
        self.collateral
    }

    pub fn token_balances(&self) -> &[ExpectedTokenBalance] {
        &self.token_balances
    }
}

/// One expected outcome-token balance from a GHOST snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedTokenBalance {
    token_id: OutcomeTokenId,
    balance: Decimal,
}

impl ExpectedTokenBalance {
    pub fn new(token_id: OutcomeTokenId, balance: Decimal) -> Self {
        Self { token_id, balance }
    }

    pub fn token_id(&self) -> &OutcomeTokenId {
        &self.token_id
    }

    pub fn balance(&self) -> Decimal {
        self.balance
    }
}

/// Validates a manual snapshot by issuing strict, read-only balance queries.
#[derive(Debug)]
pub struct GhostVerifier<R> {
    reader: R,
}

impl<R> GhostVerifier<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R> GhostVerifier<R>
where
    R: StrictAccountBalanceReader,
{
    /// Returns every comparison result for auditability.
    ///
    /// A query failure does not become a zero balance and makes the resulting
    /// verification unclean. Reads continue only to collect the full GHOST
    /// report; this method never sends an order or changes account state.
    pub async fn verify(&self, snapshot: &GhostSnapshot) -> GhostVerification {
        let collateral = match self.reader.collateral_balance_strict().await {
            Ok(observed) => BalanceVerification::compare(snapshot.collateral(), observed),
            Err(source) => BalanceVerification::QueryFailed {
                expected: snapshot.collateral(),
                source,
            },
        };

        let mut token_balances = Vec::with_capacity(snapshot.token_balances().len());
        for expected in snapshot.token_balances() {
            let result = match self
                .reader
                .position_for_token_strict(expected.token_id())
                .await
            {
                Ok(observed) => BalanceVerification::compare(expected.balance(), observed),
                Err(source) => BalanceVerification::QueryFailed {
                    expected: expected.balance(),
                    source,
                },
            };

            token_balances.push(TokenBalanceVerification::from_expected(expected, result));
        }

        GhostVerification {
            collateral,
            token_balances,
        }
    }
}

/// Complete output from one GHOST-only verification attempt.
#[derive(Debug)]
pub struct GhostVerification {
    collateral: BalanceVerification<StrictCollateralError>,
    token_balances: Vec<TokenBalanceVerification>,
}

impl GhostVerification {
    /// `true` only when every strict query succeeded and exactly matched.
    pub fn is_clean(&self) -> bool {
        self.collateral.is_match()
            && self
                .token_balances
                .iter()
                .all(TokenBalanceVerification::is_match)
    }

    pub fn collateral(&self) -> &BalanceVerification<StrictCollateralError> {
        &self.collateral
    }

    pub fn token_balances(&self) -> &[TokenBalanceVerification] {
        &self.token_balances
    }
}

/// Exact collateral comparison, or the query error that prevented one.
#[derive(Debug)]
pub enum BalanceVerification<E> {
    Match {
        expected: Decimal,
        observed: Decimal,
    },
    Mismatch {
        expected: Decimal,
        observed: Decimal,
    },
    QueryFailed {
        expected: Decimal,
        source: E,
    },
}

impl<E> BalanceVerification<E> {
    fn compare(expected: Decimal, observed: Decimal) -> Self {
        if expected == observed {
            Self::Match { expected, observed }
        } else {
            Self::Mismatch { expected, observed }
        }
    }

    pub fn is_match(&self) -> bool {
        matches!(self, Self::Match { .. })
    }
}

/// Exact result for one expected outcome token.
#[derive(Debug)]
pub struct TokenBalanceVerification {
    token_id: OutcomeTokenId,
    result: BalanceVerification<StrictPositionError>,
}

impl TokenBalanceVerification {
    fn from_expected(
        expected: &ExpectedTokenBalance,
        result: BalanceVerification<StrictPositionError>,
    ) -> Self {
        Self {
            token_id: expected.token_id.clone(),
            result,
        }
    }

    pub fn token_id(&self) -> &OutcomeTokenId {
        &self.token_id
    }

    pub fn result(&self) -> &BalanceVerification<StrictPositionError> {
        &self.result
    }

    fn is_match(&self) -> bool {
        self.result.is_match()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GhostSnapshotError {
    NegativeCollateral {
        collateral: Decimal,
    },
    NegativeTokenBalance {
        token_id: OutcomeTokenId,
        balance: Decimal,
    },
    DuplicateToken {
        token_id: OutcomeTokenId,
    },
}

impl fmt::Display for GhostSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeCollateral { collateral } => {
                write!(
                    formatter,
                    "GHOST collateral balance cannot be negative: {collateral}"
                )
            }
            Self::NegativeTokenBalance { token_id, balance } => write!(
                formatter,
                "GHOST token balance cannot be negative for {token_id}: {balance}"
            ),
            Self::DuplicateToken { token_id } => {
                write!(
                    formatter,
                    "GHOST snapshot repeats outcome token: {token_id}"
                )
            }
        }
    }
}

impl Error for GhostSnapshotError {}
