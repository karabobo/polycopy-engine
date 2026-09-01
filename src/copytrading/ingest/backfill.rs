//! Phase 2 REST backfill: catches up on trades a watched leader made while
//! the activity WebSocket connection was down, or before it first
//! connected. Blueprint section 7: "Backfill uses a high-water mark
//! (occurred_at, activity_trade_id) plus a small overlap. Canonical event
//! uniqueness absorbs overlap." There is no `activity_trade_id` field on
//! this REST response either (see `normalize.rs`'s module doc for the same
//! gap on the WS side); this uses `occurred_at` alone as the cursor, with a
//! small time overlap, and relies on `apply_trade`'s canonical-event
//! uniqueness to absorb any duplicate re-fetched at the boundary.
//!
//! Uses `polymarket_client_sdk_v2::data::Client` (the public, unauthenticated
//! Data API at `data-api.polymarket.com`) rather than the activity
//! WebSocket's undocumented firehose -- this REST endpoint *is* officially
//! documented and typed by the SDK.

use std::{fmt, str::FromStr as _};

use chrono::SecondsFormat;
use polymarket_client_sdk_v2::{
    data::{
        types::{
            request::ActivityRequest, response::Activity, ActivitySortBy, ActivityType, Side,
            SortDirection,
        },
        Client,
    },
    types::Address,
};
use sqlx::SqlitePool;

use super::{
    address_resolver::AddressResolver,
    apply::{apply_trade, ProcessOutcome},
    normalize::{NormalizedTrade, TradeSide},
};

const PAGE_LIMIT: i32 = 500;
/// A small overlap before the high-water mark, so a trade landing right at
/// the boundary of the previous run is never missed by an exclusive cursor.
const OVERLAP_SECONDS: i64 = 30;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackfillSummary {
    pub fetched: usize,
    pub ingested: usize,
    pub skipped_not_watched: usize,
    pub skipped_before_activation: usize,
    pub rejected: usize,
}

/// Fetches and applies every trade `leader_address` (the leader identified
/// by `leader_id`) has made since this leader's high-water mark (the latest
/// `occurred_at` already recorded for it, or the beginning of time if none
/// yet), minus a small overlap.
pub async fn backfill_leader(
    pool: &SqlitePool,
    resolver: &AddressResolver,
    client: &Client,
    leader_id: i64,
    leader_address: &str,
) -> Result<BackfillSummary, BackfillError> {
    let high_water_mark: Option<String> =
        sqlx::query_scalar("SELECT MAX(occurred_at) FROM leader_events WHERE leader_id = ?")
            .bind(leader_id)
            .fetch_one(pool)
            .await
            .map_err(|error| BackfillError::Database(error.to_string()))?;

    let start_unix: u64 = match high_water_mark {
        Some(watermark) => {
            let parsed = chrono::DateTime::parse_from_rfc3339(&watermark)
                .map_err(|_| BackfillError::InvalidWatermark(watermark.clone()))?;
            (parsed.timestamp() - OVERLAP_SECONDS).max(0) as u64
        }
        None => 0,
    };

    let user = Address::from_str(leader_address)
        .map_err(|_| BackfillError::InvalidAddress(leader_address.to_owned()))?;

    let request = ActivityRequest::builder()
        .user(user)
        .activity_types(vec![ActivityType::Trade])
        .start(start_unix)
        .sort_by(ActivitySortBy::Timestamp)
        .sort_direction(SortDirection::Asc)
        .limit(PAGE_LIMIT)
        .map_err(|_| BackfillError::InvalidLimit)?
        .build();

    let activities = client
        .activity(&request)
        .await
        .map_err(|error| BackfillError::Fetch(error.to_string()))?;

    let mut summary = BackfillSummary {
        fetched: activities.len(),
        ..BackfillSummary::default()
    };

    for activity in &activities {
        let Some(trade) = normalize_rest_activity(activity) else {
            summary.rejected += 1;
            continue;
        };
        // Activity only implements Deserialize (it's a response type), not
        // Serialize, so this is a debug dump, not verbatim JSON like the WS
        // path's observations. Still a faithful record for audit purposes.
        let raw_payload = format!("{activity:?}");
        let transaction_hash = trade.transaction_hash.clone();
        let outcome = apply_trade(
            pool,
            resolver,
            &trade,
            "activity_backfill",
            &transaction_hash,
            &raw_payload,
        )
        .await;
        match outcome {
            ProcessOutcome::Ingested { .. } => summary.ingested += 1,
            ProcessOutcome::NotWatched => summary.skipped_not_watched += 1,
            ProcessOutcome::LeaderNotActivated | ProcessOutcome::BeforeActivation => {
                summary.skipped_before_activation += 1;
            }
            ProcessOutcome::Rejected(_) | ProcessOutcome::Skip => summary.rejected += 1,
            ProcessOutcome::DatabaseError(error) => return Err(BackfillError::Database(error)),
        }
    }

    Ok(summary)
}

/// Pure conversion from the Data API's strongly-typed `Activity` row into
/// this project's `NormalizedTrade`. Returns `None` for a row missing a
/// field required to act on it, mirroring `normalize::parse`'s `Rejected`
/// case for the WS payload -- most commonly a non-trade activity type that
/// slipped past the `activity_types` filter, or a row with no condition ID
/// (redemptions, conversions, and similar row shapes seen live on the WS
/// firehose omit it).
fn normalize_rest_activity(activity: &Activity) -> Option<NormalizedTrade> {
    let condition_id = activity.condition_id?;
    let asset = activity.asset?;
    let side = match activity.side.clone()? {
        Side::Buy => TradeSide::Buy,
        Side::Sell => TradeSide::Sell,
        // Side is #[non_exhaustive]: an SDK-added variant we don't
        // recognize yet is a reason to reject, not to guess.
        _ => return None,
    };
    let outcome_index = i64::from(activity.outcome_index?);
    let price = activity.price?;
    let occurred_at_utc = chrono::DateTime::from_timestamp(activity.timestamp, 0)?
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    Some(NormalizedTrade {
        // Address's Display is EIP-55 checksummed (mixed case); normalized
        // to lowercase to match address_resolver's lookup key.
        trader_address: activity.proxy_wallet.to_string().to_ascii_lowercase(),
        token_id: asset.to_string(),
        condition_id: condition_id.to_string(),
        outcome_index,
        side,
        size: activity.size.to_string(),
        price: price.to_string(),
        occurred_at_utc,
        transaction_hash: activity.transaction_hash.to_string(),
    })
}

#[derive(Debug)]
pub enum BackfillError {
    Database(String),
    InvalidWatermark(String),
    InvalidAddress(String),
    InvalidLimit,
    Fetch(String),
}

impl fmt::Display for BackfillError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::InvalidWatermark(value) => {
                write!(
                    formatter,
                    "stored high-water mark is not valid RFC 3339: {value}"
                )
            }
            Self::InvalidAddress(value) => write!(formatter, "invalid leader address: {value}"),
            Self::InvalidLimit => write!(formatter, "invalid page limit"),
            Self::Fetch(error) => write!(formatter, "unable to fetch activity: {error}"),
        }
    }
}

impl std::error::Error for BackfillError {}

#[cfg(test)]
mod tests {
    use polymarket_client_sdk_v2::types::{Address, Decimal, B256, U256};

    use super::*;

    fn sample_activity() -> Activity {
        Activity::builder()
            .proxy_wallet(Address::from([0xaau8; 20]))
            .timestamp(1_735_689_600)
            .condition_id(B256::from([0x11u8; 32]))
            .activity_type(ActivityType::Trade)
            .size(Decimal::from_str("5").unwrap())
            .usdc_size(Decimal::from_str("2.5").unwrap())
            .transaction_hash(B256::from([0x22u8; 32]))
            .price(Decimal::from_str("0.5").unwrap())
            .asset(U256::from(123u64))
            .side(Side::Buy)
            .outcome_index(0)
            .build()
    }

    #[test]
    fn normalizes_a_well_formed_trade_activity_row() {
        let trade = normalize_rest_activity(&sample_activity()).expect("must normalize");
        assert_eq!(trade.side, TradeSide::Buy);
        assert_eq!(trade.outcome_index, 0);
        assert_eq!(trade.price, "0.5");
        assert_eq!(trade.size, "5");
        assert_eq!(
            trade.trader_address,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn rejects_a_row_missing_a_condition_id() {
        let mut activity = sample_activity();
        activity.condition_id = None;
        assert!(normalize_rest_activity(&activity).is_none());
    }

    #[test]
    fn rejects_a_row_missing_a_side() {
        let mut activity = sample_activity();
        activity.side = None;
        assert!(normalize_rest_activity(&activity).is_none());
    }

    #[test]
    fn rejects_a_row_missing_a_price() {
        let mut activity = sample_activity();
        activity.price = None;
        assert!(normalize_rest_activity(&activity).is_none());
    }
}
