#![cfg(feature = "intl_clob")]

use std::{collections::HashMap, str::FromStr as _};

use async_trait::async_trait;
use polycopy_engine::{
    ghost_to_record, BalanceRecordStatus, BalanceVerification, ExpectedTokenBalance,
    GhostRunRecord, GhostSnapshot, GhostVerifier, OutcomeTokenId, StrictAccountBalanceReader,
    StrictCollateralError, StrictPositionError, StrictTokenBalanceReader,
};
use polymarket_client_sdk_v2::{error::Error as SdkError, types::Decimal};

#[tokio::test]
async fn matching_read_only_balances_produce_a_clean_ghost_report() {
    let token_id = token("123456789");
    let snapshot = GhostSnapshot::new(
        Decimal::from(50),
        vec![ExpectedTokenBalance::new(
            token_id.clone(),
            Decimal::from(7),
        )],
    )
    .expect("valid snapshot");
    let reader = StubReader::matching(Decimal::from(50), [(token_id, Decimal::from(7))]);

    let report = GhostVerifier::new(reader).verify(&snapshot).await;

    assert!(report.is_clean());
    assert!(matches!(
        report.collateral(),
        BalanceVerification::Match { .. }
    ));
    assert!(matches!(
        report.token_balances()[0].result(),
        BalanceVerification::Match { .. }
    ));
}

#[tokio::test]
async fn a_balance_mismatch_keeps_ghost_verification_unclean() {
    let token_id = token("123456789");
    let snapshot = GhostSnapshot::new(
        Decimal::from(50),
        vec![ExpectedTokenBalance::new(
            token_id.clone(),
            Decimal::from(7),
        )],
    )
    .expect("valid snapshot");
    let reader = StubReader::matching(Decimal::from(49), [(token_id, Decimal::from(7))]);

    let report = GhostVerifier::new(reader).verify(&snapshot).await;

    assert!(!report.is_clean());
    assert!(matches!(
        report.collateral(),
        BalanceVerification::Mismatch {
            expected,
            observed
        } if *expected == Decimal::from(50) && *observed == Decimal::from(49)
    ));
}

#[tokio::test]
async fn a_token_query_failure_never_passes_or_becomes_zero() {
    let token_id = token("123456789");
    let snapshot = GhostSnapshot::new(
        Decimal::ZERO,
        vec![ExpectedTokenBalance::new(
            token_id.clone(),
            Decimal::from(7),
        )],
    )
    .expect("valid snapshot");
    let reader = StubReader::failing_token(Decimal::ZERO, token_id.clone());

    let report = GhostVerifier::new(reader).verify(&snapshot).await;

    assert!(!report.is_clean());
    assert!(matches!(
        report.token_balances()[0].result(),
        BalanceVerification::QueryFailed {
            expected,
            source: StrictPositionError::Query { token_id: failed, .. }
        } if *expected == Decimal::from(7) && failed == &token_id
    ));
}

#[tokio::test]
async fn a_collateral_query_failure_never_passes_or_becomes_zero() {
    let snapshot = GhostSnapshot::new(Decimal::from(50), Vec::new()).expect("valid snapshot");
    let reader = StubReader::failing_collateral();

    let report = GhostVerifier::new(reader).verify(&snapshot).await;

    assert!(!report.is_clean());
    assert!(matches!(
        report.collateral(),
        BalanceVerification::QueryFailed {
            expected,
            source: StrictCollateralError::Query { .. }
        } if *expected == Decimal::from(50)
    ));
}

#[test]
fn a_ghost_snapshot_rejects_ambiguous_or_invalid_balances() {
    let token_id = token("123456789");
    let duplicate = GhostSnapshot::new(
        Decimal::ZERO,
        vec![
            ExpectedTokenBalance::new(token_id.clone(), Decimal::ZERO),
            ExpectedTokenBalance::new(token_id, Decimal::ZERO),
        ],
    )
    .expect_err("a token may appear only once");

    assert!(duplicate
        .to_string()
        .starts_with("GHOST snapshot repeats outcome token:"));
    assert!(GhostSnapshot::new(Decimal::NEGATIVE_ONE, Vec::new()).is_err());
}

fn token(raw: &str) -> OutcomeTokenId {
    OutcomeTokenId::from_str(raw).expect("test token ID is valid")
}

#[derive(Debug)]
struct StubReader {
    collateral: Decimal,
    collateral_error: bool,
    token_balances: HashMap<OutcomeTokenId, Decimal>,
    failed_token: Option<OutcomeTokenId>,
}

impl StubReader {
    fn matching(
        collateral: Decimal,
        token_balances: impl IntoIterator<Item = (OutcomeTokenId, Decimal)>,
    ) -> Self {
        Self {
            collateral,
            collateral_error: false,
            token_balances: token_balances.into_iter().collect(),
            failed_token: None,
        }
    }

    fn failing_token(collateral: Decimal, token_id: OutcomeTokenId) -> Self {
        Self {
            collateral,
            collateral_error: false,
            token_balances: HashMap::new(),
            failed_token: Some(token_id),
        }
    }

    fn failing_collateral() -> Self {
        Self {
            collateral: Decimal::ZERO,
            collateral_error: true,
            token_balances: HashMap::new(),
            failed_token: None,
        }
    }
}

#[async_trait]
impl StrictTokenBalanceReader for StubReader {
    async fn position_for_token_strict(
        &self,
        token_id: &OutcomeTokenId,
    ) -> Result<Decimal, StrictPositionError> {
        if self.failed_token.as_ref() == Some(token_id) {
            return Err(StrictPositionError::Query {
                token_id: token_id.clone(),
                source: SdkError::validation("simulated GHOST token-query failure"),
            });
        }

        self.token_balances
            .get(token_id)
            .copied()
            .ok_or_else(|| StrictPositionError::Query {
                token_id: token_id.clone(),
                source: SdkError::validation("unexpected GHOST token query"),
            })
    }
}

#[async_trait]
impl StrictAccountBalanceReader for StubReader {
    async fn collateral_balance_strict(&self) -> Result<Decimal, StrictCollateralError> {
        if self.collateral_error {
            return Err(StrictCollateralError::Query {
                source: SdkError::validation("simulated GHOST collateral-query failure"),
            });
        }

        Ok(self.collateral)
    }

    async fn collateral_allowance_strict(&self) -> Result<Decimal, StrictCollateralError> {
        if self.collateral_error {
            return Err(StrictCollateralError::Query {
                source: SdkError::validation("simulated GHOST allowance-query failure"),
            });
        }

        Ok(self.collateral)
    }
}

// Phase 7's 72-hour GHOST run needs one persistable record per run so a
// later pass can reconcile the whole window without holding a venue
// connection open the entire time -- these tests lock in that the record
// faithfully reflects a real GhostVerification produced by the exact same
// GhostVerifier::verify path the other tests in this file already exercise.

#[tokio::test]
async fn a_clean_report_produces_a_clean_record_with_no_error_fields() {
    let token_id = token("123456789");
    let snapshot = GhostSnapshot::new(
        Decimal::from(50),
        vec![ExpectedTokenBalance::new(
            token_id.clone(),
            Decimal::from(7),
        )],
    )
    .expect("valid snapshot");
    let reader = StubReader::matching(Decimal::from(50), [(token_id, Decimal::from(7))]);
    let report = GhostVerifier::new(reader).verify(&snapshot).await;

    let record = ghost_to_record(&report, "2026-09-01T00:00:00Z", "2026-09-01T00:00:01Z");

    assert!(record.is_clean);
    assert_eq!(record.snapshot_at_utc, "2026-09-01T00:00:00Z");
    assert_eq!(record.checked_at_utc, "2026-09-01T00:00:01Z");
    assert_eq!(record.collateral.status, BalanceRecordStatus::Match);
    assert_eq!(record.collateral.error, None);
    assert_eq!(record.token_balances.len(), 1);
    assert_eq!(record.token_balances[0].token_id, "123456789");
    assert_eq!(
        record.token_balances[0].balance.status,
        BalanceRecordStatus::Match
    );
}

#[tokio::test]
async fn a_query_failure_is_recorded_with_its_error_and_no_observed_value() {
    let snapshot = GhostSnapshot::new(Decimal::from(50), vec![]).expect("valid snapshot");
    let reader = StubReader::failing_collateral();
    let report = GhostVerifier::new(reader).verify(&snapshot).await;

    let record = ghost_to_record(&report, "2026-09-01T00:00:00Z", "2026-09-01T00:00:01Z");

    assert!(!record.is_clean);
    assert_eq!(record.collateral.status, BalanceRecordStatus::QueryFailed);
    assert_eq!(record.collateral.observed, None);
    assert!(record.collateral.error.is_some());
}

#[tokio::test]
async fn a_mismatch_is_recorded_as_unclean_with_both_magnitudes() {
    let snapshot = GhostSnapshot::new(Decimal::from(50), vec![]).expect("valid snapshot");
    let reader = StubReader::matching(Decimal::from(49), []);
    let report = GhostVerifier::new(reader).verify(&snapshot).await;

    let record = ghost_to_record(&report, "2026-09-01T00:00:00Z", "2026-09-01T00:00:01Z");

    assert!(!record.is_clean);
    assert_eq!(record.collateral.status, BalanceRecordStatus::Mismatch);
    assert_eq!(record.collateral.expected, "50");
    assert_eq!(record.collateral.observed.as_deref(), Some("49"));
}

#[tokio::test]
async fn a_record_round_trips_through_json() {
    let snapshot = GhostSnapshot::new(Decimal::from(50), vec![]).expect("valid snapshot");
    let reader = StubReader::matching(Decimal::from(50), []);
    let report = GhostVerifier::new(reader).verify(&snapshot).await;
    let record = ghost_to_record(&report, "2026-09-01T00:00:00Z", "2026-09-01T00:00:01Z");

    let json = serde_json::to_string(&record).expect("record must serialize");
    let restored: GhostRunRecord = serde_json::from_str(&json).expect("record must deserialize");

    assert_eq!(restored, record);
}
