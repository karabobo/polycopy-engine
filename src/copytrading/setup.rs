//! One-time, transactionally safe initialization of a bounded test-copy setup.
//!
//! This module writes configuration rows only. It has no venue client and no
//! order-submission surface. A non-empty database is refused rather than
//! overwritten, so rerunning setup cannot silently change a live strategy.

use std::fmt;

use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::SqlitePool;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialCopySetup {
    pub account_label: String,
    pub signing_address: String,
    pub signature_type: String,
    pub funder_address: String,
    pub leader_label: String,
    pub leader_address: String,
    pub activation_at: String,
    pub max_order_notional: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialCopySetupResult {
    pub account_id: i64,
    pub leader_id: i64,
    pub activation_at: String,
}

/// Creates exactly one test account, one enabled leader, its policy, and a
/// one-lane execution schedule in one transaction.
pub async fn initialize_fresh_test_copy_setup(
    pool: &SqlitePool,
    setup: &InitialCopySetup,
) -> Result<InitialCopySetupResult, SetupError> {
    let account_label = non_empty(&setup.account_label, "account label")?;
    let leader_label = non_empty(&setup.leader_label, "leader label")?;
    let signing_address = normalize_address(&setup.signing_address, "signing address")?;
    let funder_address = normalize_address(&setup.funder_address, "funder address")?;
    let leader_address = normalize_address(&setup.leader_address, "leader address")?;
    if setup.signature_type != "gnosis_safe" {
        return Err(SetupError::UnsupportedSignatureType);
    }
    let activation_at = DateTime::parse_from_rfc3339(&setup.activation_at)
        .map_err(|_| SetupError::InvalidActivationTimestamp)?
        .with_timezone(&Utc)
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let max_order_notional: rust_decimal::Decimal = setup
        .max_order_notional
        .parse()
        .map_err(|_| SetupError::InvalidMaxOrderNotional)?;
    if !(max_order_notional > rust_decimal::Decimal::ZERO
        && max_order_notional <= rust_decimal::Decimal::ONE)
    {
        return Err(SetupError::InvalidMaxOrderNotional);
    }

    let mut tx = pool.begin().await.map_err(SetupError::Database)?;
    let configured_rows: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM accounts) + (SELECT COUNT(*) FROM leader_config) \
         + (SELECT COUNT(*) FROM execution_schedule) + (SELECT COUNT(*) FROM copy_intents)",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(SetupError::Database)?;
    if configured_rows != 0 {
        return Err(SetupError::AlreadyConfigured);
    }

    sqlx::query(
        "INSERT INTO accounts (id, label, signing_address, funder_address, signature_type) \
         VALUES (1, ?, ?, ?, 'gnosis_safe')",
    )
    .bind(account_label)
    .bind(signing_address)
    .bind(funder_address)
    .execute(&mut *tx)
    .await
    .map_err(SetupError::Database)?;
    sqlx::query(
        "INSERT INTO leader_config (id, label, enabled, activation_at) VALUES (1, ?, 1, ?)",
    )
    .bind(leader_label)
    .bind(&activation_at)
    .execute(&mut *tx)
    .await
    .map_err(SetupError::Database)?;
    sqlx::query("INSERT INTO leader_wallet_aliases (leader_id, address, enabled) VALUES (1, ?, 1)")
        .bind(leader_address)
        .execute(&mut *tx)
        .await
        .map_err(SetupError::Database)?;
    sqlx::query(
        "INSERT INTO leader_policy \
         (leader_id, max_signal_age_seconds, decision_window_seconds, price_tolerance_bps, \
          tick_size, min_price, max_price, max_order_notional, min_leader_trade_size) \
         VALUES (1, 300, 120, 100, '0.01', '0.01', '0.99', ?, '0')",
    )
    .bind(max_order_notional.to_string())
    .execute(&mut *tx)
    .await
    .map_err(SetupError::Database)?;
    sqlx::query(
        "INSERT INTO execution_schedule (id, shard_scheme_version, shard_algorithm, lane_count) \
         VALUES (1, 1, 'hash_mod_lane_count', 1)",
    )
    .execute(&mut *tx)
    .await
    .map_err(SetupError::Database)?;
    tx.commit().await.map_err(SetupError::Database)?;

    Ok(InitialCopySetupResult {
        account_id: 1,
        leader_id: 1,
        activation_at,
    })
}

fn non_empty(value: &str, field: &'static str) -> Result<String, SetupError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(SetupError::EmptyField(field))
    } else {
        Ok(trimmed.to_owned())
    }
}

fn normalize_address(value: &str, field: &'static str) -> Result<String, SetupError> {
    let trimmed = value.trim();
    if trimmed.len() != 42
        || !trimmed.starts_with("0x")
        || !trimmed[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(SetupError::InvalidAddress(field));
    }
    Ok(trimmed.to_ascii_lowercase())
}

#[derive(Debug)]
pub enum SetupError {
    Database(sqlx::Error),
    AlreadyConfigured,
    EmptyField(&'static str),
    InvalidAddress(&'static str),
    InvalidActivationTimestamp,
    InvalidMaxOrderNotional,
    UnsupportedSignatureType,
}

impl fmt::Display for SetupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "setup database error: {error}"),
            Self::AlreadyConfigured => write!(
                formatter,
                "refusing to overwrite a configured copy database"
            ),
            Self::EmptyField(field) => write!(formatter, "{field} must not be empty"),
            Self::InvalidAddress(field) => write!(formatter, "invalid {field}"),
            Self::InvalidActivationTimestamp => {
                write!(formatter, "activation timestamp must be RFC 3339")
            }
            Self::InvalidMaxOrderNotional => write!(
                formatter,
                "test maximum must be greater than zero and no more than 1 USDC"
            ),
            Self::UnsupportedSignatureType => {
                write!(formatter, "test setup currently requires gnosis_safe")
            }
        }
    }
}

impl std::error::Error for SetupError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::copytrading::db::open_and_migrate;

    #[tokio::test]
    async fn setup_is_atomic_and_refuses_to_overwrite_existing_configuration() {
        let path = std::env::temp_dir().join(format!(
            "polycopy-engine-setup-test-{}.sqlite",
            std::process::id()
        ));
        let pool = open_and_migrate(&path)
            .await
            .expect("migrations must apply");
        let setup = InitialCopySetup {
            account_label: "test-safe".to_owned(),
            signing_address: "0x1111111111111111111111111111111111111111".to_owned(),
            signature_type: "gnosis_safe".to_owned(),
            funder_address: "0x2222222222222222222222222222222222222222".to_owned(),
            leader_label: "test-leader".to_owned(),
            leader_address: "0xac4f1554bd58f0b4f1d07e2726aa4e192510d862".to_owned(),
            activation_at: "2026-09-03T00:00:00Z".to_owned(),
            max_order_notional: "1".to_owned(),
        };

        let result = initialize_fresh_test_copy_setup(&pool, &setup)
            .await
            .expect("fresh setup must succeed");
        assert_eq!(result.account_id, 1);
        assert_eq!(result.leader_id, 1);
        let policy: String =
            sqlx::query_scalar("SELECT max_order_notional FROM leader_policy WHERE leader_id = 1")
                .fetch_one(&pool)
                .await
                .expect("policy must exist");
        assert_eq!(policy, "1");
        assert!(matches!(
            initialize_fresh_test_copy_setup(&pool, &setup).await,
            Err(SetupError::AlreadyConfigured)
        ));

        drop(pool);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
