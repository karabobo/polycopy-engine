//! No-order-capable detection of redeemable positions.
//!
//! Polymarket's own `/positions` endpoint returns `redeemable: bool`
//! directly on each position once its market has resolved -- confirmed
//! against this project's predecessor, PolyHermes, whose
//! `AccountPositionDto.redeemable` reads the same upstream field. There is
//! no need to independently track UMA resolution state here.
//!
//! This module only reads: the account's own row, Polymarket's positions
//! API (a GET request), and the local `position_lots` ledger. It has no
//! signer and no venue write call anywhere in its dependency graph, and
//! cannot execute the on-chain/relay redemption transaction itself -- that
//! moves real funds and is a human's job, the same boundary this project
//! already draws around `CopyExecution::submit_exact_envelope`.

use std::{fmt, str::FromStr as _};

use chrono::{SecondsFormat, Utc};
use polymarket_client_sdk_v2::{
    data::{types::request::PositionsRequest, Client},
    types::Address,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

/// The API's own page-size ceiling (0-500, see `PositionsRequest::limit`'s
/// doc). Requesting exactly the ceiling makes a full page ambiguous with a
/// truncated one, so a full page is treated as "possibly more exist" --
/// see the `TooManyPositions` guard in `detect_redeemable_positions`.
const PAGE_LIMIT: i32 = 500;

pub const REDEMPTION_EVENT_PREFIX: &str = "REDEMPTION_EVENT: ";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RedeemablePosition {
    pub detected_at_utc: String,
    pub account_id: i64,
    pub condition_id: String,
    pub token_id: String,
    pub outcome_index: i32,
    pub outcome: String,
    pub size: String,
    pub current_value: String,
    pub title: String,
    /// Whether this project's own `position_lots` ledger still shows a
    /// nonzero quantity for this token. `false` is worth a second look but
    /// not necessarily a bug: the position may have been opened outside
    /// this engine's copy-trading flow (e.g. manually, before this engine
    /// tracked the account).
    pub tracked_in_local_lots: bool,
}

/// Queries Polymarket for every position of `account_id`'s holding address
/// that the venue itself reports as `redeemable`, cross-referenced against
/// the local `position_lots` ledger. Read-only in both directions: no
/// write to the database, no signed request to the venue.
pub async fn detect_redeemable_positions(
    pool: &SqlitePool,
    client: &Client,
    account_id: i64,
) -> Result<Vec<RedeemablePosition>, RedemptionError> {
    let account: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT signing_address, funder_address FROM accounts WHERE id = ?")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| RedemptionError::Database(error.to_string()))?;
    let (signing_address, funder_address) =
        account.ok_or(RedemptionError::AccountNotFound(account_id))?;
    let holding_address = resolve_holding_address(&signing_address, funder_address.as_deref());

    let user = Address::from_str(&holding_address)
        .map_err(|_| RedemptionError::InvalidAddress(holding_address.clone()))?;

    let request = PositionsRequest::builder()
        .user(user)
        .redeemable(true)
        .limit(PAGE_LIMIT)
        .map_err(|_| RedemptionError::InvalidLimit)?
        .build();
    let positions = client
        .positions(&request)
        .await
        .map_err(|error| RedemptionError::Fetch(error.to_string()))?;

    if positions.len() as i32 == PAGE_LIMIT {
        // The API silently truncates past `limit`; there may be more
        // redeemable positions than fit on one page. Detection must never
        // under-report, so this is an error rather than a partial result.
        return Err(RedemptionError::TooManyPositions(positions.len()));
    }

    let detected_at_utc = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut out = Vec::with_capacity(positions.len());
    for position in positions {
        let token_id = position.asset.to_string();
        let raw_qtys: Vec<String> = sqlx::query_scalar(
            "SELECT qty FROM position_lots WHERE account_id = ? AND token_id = ?",
        )
        .bind(account_id)
        .bind(&token_id)
        .fetch_all(pool)
        .await
        .map_err(|error| RedemptionError::Database(error.to_string()))?;
        let tracked_in_local_lots = has_positive_qty(&raw_qtys)?;

        out.push(RedeemablePosition {
            detected_at_utc: detected_at_utc.clone(),
            account_id,
            condition_id: position.condition_id.to_string(),
            token_id,
            outcome_index: position.outcome_index,
            outcome: position.outcome,
            size: position.size.to_string(),
            current_value: position.current_value.to_string(),
            title: position.title,
            tracked_in_local_lots,
        });
    }
    Ok(out)
}

/// A proxy/Safe/poly1271 account trades and holds collateral through its
/// funder address, not the signing EOA (see `accounts` table's own CHECK
/// constraint comment); an `eoa` account has no funder row at all and
/// funds itself. Positions belong to whichever address actually holds
/// them, so that is the one to query.
fn resolve_holding_address(signing_address: &str, funder_address: Option<&str>) -> String {
    funder_address.unwrap_or(signing_address).to_owned()
}

/// Sums `position_lots.qty` across every matching row (there can be more
/// than one if the same token was copied from more than one leader) using
/// `Decimal`, never SQL-side numeric casting -- this project stores every
/// amount as exact decimal text specifically to avoid float comparison
/// bugs, and a local existence check is not an exemption from that.
fn has_positive_qty(raw_qtys: &[String]) -> Result<bool, RedemptionError> {
    let mut total = Decimal::ZERO;
    for qty in raw_qtys {
        total += qty
            .parse::<Decimal>()
            .map_err(|_| RedemptionError::InvalidDecimal("position_lots.qty"))?;
    }
    Ok(total > Decimal::ZERO)
}

#[derive(Debug)]
pub enum RedemptionError {
    Database(String),
    AccountNotFound(i64),
    InvalidAddress(String),
    InvalidLimit,
    Fetch(String),
    InvalidDecimal(&'static str),
    TooManyPositions(usize),
}

impl fmt::Display for RedemptionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::AccountNotFound(id) => write!(formatter, "no account with id {id} exists"),
            Self::InvalidAddress(address) => {
                write!(formatter, "not a valid on-chain address: {address}")
            }
            Self::InvalidLimit => write!(formatter, "invalid positions page limit"),
            Self::Fetch(error) => write!(formatter, "unable to fetch positions: {error}"),
            Self::InvalidDecimal(field) => write!(formatter, "{field} is not a valid decimal"),
            Self::TooManyPositions(count) => write!(
                formatter,
                "received {count} redeemable positions, at the API page limit -- there may be \
                 more than fit on one page; refusing to under-report rather than silently \
                 truncating"
            ),
        }
    }
}

impl std::error::Error for RedemptionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::copytrading::db::open_and_migrate;

    struct TestDb {
        pool: SqlitePool,
        path: std::path::PathBuf,
    }

    impl TestDb {
        async fn new() -> Self {
            use std::{
                env, process,
                sync::atomic::{AtomicU64, Ordering},
                time::{SystemTime, UNIX_EPOCH},
            };
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time must be after the Unix epoch")
                .as_nanos();
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "polycopy-engine-redemption-test-{}-{nonce}-{counter}.sqlite",
                process::id()
            ));
            let pool = open_and_migrate(&path)
                .await
                .expect("migrations must apply to a fresh database");
            Self { pool, path }
        }
    }

    impl std::ops::Deref for TestDb {
        type Target = SqlitePool;

        fn deref(&self) -> &SqlitePool {
            &self.pool
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
            let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
        }
    }

    #[test]
    fn funder_address_is_preferred_when_present() {
        assert_eq!(
            resolve_holding_address("0xsigner", Some("0xfunder")),
            "0xfunder"
        );
    }

    #[test]
    fn signing_address_is_used_when_there_is_no_funder() {
        assert_eq!(resolve_holding_address("0xsigner", None), "0xsigner");
    }

    #[test]
    fn no_lot_rows_is_not_tracked() {
        assert!(!has_positive_qty(&[]).unwrap());
    }

    #[test]
    fn a_zero_qty_lot_is_not_tracked() {
        assert!(!has_positive_qty(&["0".to_owned()]).unwrap());
    }

    #[test]
    fn a_positive_qty_lot_is_tracked() {
        assert!(has_positive_qty(&["12.5".to_owned()]).unwrap());
    }

    #[test]
    fn quantities_from_more_than_one_leader_are_summed() {
        // The same token copied from two leaders produces two
        // position_lots rows keyed by different leader_id; a stale/rounding
        // zero on one leader must not hide real qty held via another.
        assert!(has_positive_qty(&["0".to_owned(), "3".to_owned()]).unwrap());
    }

    #[test]
    fn an_unparseable_qty_is_rejected_not_treated_as_zero() {
        let result = has_positive_qty(&["not-a-number".to_owned()]);
        assert!(matches!(
            result,
            Err(RedemptionError::InvalidDecimal("position_lots.qty"))
        ));
    }

    #[tokio::test]
    async fn detection_fails_closed_on_an_unknown_account_without_any_network_call() {
        let db = TestDb::new().await;
        let client = Client::new("https://data-api.polymarket.com").unwrap();

        let result = detect_redeemable_positions(&db, &client, 999).await;

        assert!(matches!(result, Err(RedemptionError::AccountNotFound(999))));
    }

    #[tokio::test]
    async fn detection_fails_closed_on_an_unparseable_stored_address_without_any_network_call() {
        let db = TestDb::new().await;
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'test', 'not-an-address', 'eoa')",
        )
        .execute(&*db)
        .await
        .unwrap();
        let client = Client::new("https://data-api.polymarket.com").unwrap();

        let result = detect_redeemable_positions(&db, &client, 1).await;

        assert!(
            matches!(result, Err(RedemptionError::InvalidAddress(address)) if address == "not-an-address")
        );
    }
}
