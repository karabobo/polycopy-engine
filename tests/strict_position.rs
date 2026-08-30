#![cfg(feature = "intl_clob")]

use std::str::FromStr as _;

use async_trait::async_trait;
use polycopy_engine::{OutcomeTokenId, StrictPositionError, StrictTokenBalanceReader};
use polymarket_client_sdk_v2::{error::Error as SdkError, types::Decimal};

#[test]
fn outcome_token_id_rejects_non_numeric_values() {
    let error = OutcomeTokenId::from_str("condition-id-is-not-an-outcome-token")
        .expect_err("a token ID must be a decimal U256 value");

    assert_eq!(
        error.to_string(),
        "invalid Polymarket outcome token ID: condition-id-is-not-an-outcome-token"
    );
}

#[test]
fn outcome_token_id_rejects_a_hex_condition_id() {
    let error = OutcomeTokenId::from_str(
        "0x747dc809fb79e1b05be09c42d6179459a58de2ef3e40f02484a4e1260f741f75",
    )
    .expect_err("a condition ID must never be accepted as an outcome token ID");

    assert!(error
        .to_string()
        .starts_with("invalid Polymarket outcome token ID:"));
}

#[tokio::test]
async fn a_token_query_failure_never_degrades_to_a_zero_position() {
    let token_id = OutcomeTokenId::from_str("123456789").expect("valid token ID");
    let result = FailingReader.position_for_token_strict(&token_id).await;

    assert!(matches!(result, Err(StrictPositionError::Query { .. })));
}

struct FailingReader;

#[async_trait]
impl StrictTokenBalanceReader for FailingReader {
    async fn position_for_token_strict(
        &self,
        token_id: &OutcomeTokenId,
    ) -> Result<Decimal, StrictPositionError> {
        Err(StrictPositionError::Query {
            token_id: token_id.clone(),
            source: SdkError::validation("simulated token-query failure"),
        })
    }
}
