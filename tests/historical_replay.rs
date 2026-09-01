//! Phase 7 historical replay (`docs/COPY_ENGINE_BLUEPRINT.md` section 12).
//!
//! `tests/fixtures/leader_activity_slim.json` is 305 real rows extracted
//! read-only from a PolyHermes production database backup
//! (`/srv/polyhermes/backups/mysql-final-cutover.sql` on the account
//! owner's own execution server, `leader_activity_event` table, 2026-07-21
//! through 2026-07-23, 9 distinct leader wallets, 105 distinct outcome
//! tokens) -- downloaded to this repo with the account owner's explicit
//! permission. Only trade-shape fields (wallet, token, condition, side,
//! outcome index, price, size, event time, source event ID) were kept; no
//! credential, internal ID, or PolyHermes-internal processing-status field
//! was carried over. `replays_a_real_production_batch_without_loss_or_a_panic`
//! below replays every row through the real `apply_trade` -> `plan_next_batch`
//! pipeline.
//!
//! This harness also replays a small, hand-built batch of `NormalizedTrade`
//! rows -- the exact same venue-agnostic type `activity_ws` and `backfill`
//! both produce -- through the same pipeline, to pin the two regression
//! patterns the blueprint explicitly calls out (the real dataset above
//! happens not to contain a naturally-occurring duplicate-transaction-hash
//! case, so that specific pattern is still exercised synthetically):
//!
//! - "Aggregated events that lost outcome direction are positive
//!   regressions: the new Activity event parser must preserve a real
//!   outcome token." This is a real, already-fixed defect from this
//!   project's own Phase 2 development (see the `canonical_event_key`
//!   history in `apply.rs`): one `transaction_hash` can carry two distinct
//!   fills (different token/side/price/size). This replay proves the full
//!   pipeline, not just `apply.rs`'s own unit test, still keeps both.
//! - "Sell retries that expired after hundreds of attempts are
//!   regressions: they must converge to a receipt-backed result or visible
//!   `needs_reconcile` case." Covered structurally by the retry budget
//!   (`reconcile::MAX_ATTEMPTS_PER_WINDOW`/`RETRY_WINDOW_SECONDS`, see
//!   `reconcile.rs`'s own tests) rather than repeated here.
//!
//! "Correct rejections, such as price or daily-limit policy, remain
//! negative tests" is exercised below via a too-small trade. "Combination
//! bets remain an explicit v1 exclusion" is implemented in
//! `normalize.rs::parse` (not exercised here, since this harness replays
//! already-`NormalizedTrade` rows -- see `normalize.rs`'s own
//! `rejects_a_trade_marked_is_combo_true` test) and grounded in PolyHermes's
//! own source, not a guess: `CopyOrderTrackingService.kt` skips any trade
//! where `isCombo == true`, fed from the `isCombo` field PolyHermes's
//! `PolymarketClobApi.kt` reads off the real activity/trade response. This
//! project's Rust SDK does not expose that field on its typed structs, so
//! it is read directly from the raw WS payload instead.

#![cfg(all(feature = "ingest", feature = "execute"))]

use std::sync::Arc;

use polycopy_engine::copytrading::{
    ingest::{apply_trade, AddressResolver, NormalizedTrade, TradeSide},
    open_and_migrate, plan_next_batch,
};
use sqlx::Row as _;

async fn open_temp_db() -> sqlx::SqlitePool {
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
        "polycopy-engine-historical-replay-{}-{nonce}-{counter}.sqlite",
        process::id()
    ));
    open_and_migrate(&path)
        .await
        .expect("migrations must apply to a fresh database")
}

fn row(
    trader: &str,
    token_id: &str,
    side: TradeSide,
    size: &str,
    price: &str,
    occurred_at_utc: &str,
    transaction_hash: &str,
) -> NormalizedTrade {
    NormalizedTrade {
        trader_address: trader.to_owned(),
        token_id: token_id.to_owned(),
        condition_id: "0xcond".to_owned(),
        outcome_index: 0,
        side,
        size: size.to_owned(),
        price: price.to_owned(),
        occurred_at_utc: occurred_at_utc.to_owned(),
        transaction_hash: transaction_hash.to_owned(),
    }
}

#[tokio::test]
async fn a_replayed_historical_batch_preserves_aggregated_outcome_direction_and_rejects_correctly()
{
    let pool = open_temp_db().await;

    sqlx::query(
        "INSERT INTO accounts (id, label, signing_address, signature_type) \
         VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO leader_config (id, label, enabled, activation_at) VALUES (1, 'leader-one', 1, '2020-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO leader_wallet_aliases (leader_id, address, enabled) VALUES (1, '0xleader', 1)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO leader_policy \
         (leader_id, max_signal_age_seconds, decision_window_seconds, price_tolerance_bps, \
          tick_size, max_order_notional, min_leader_trade_size) \
         VALUES (1, 315360000, 300, 100, '0.01', '10000', '2')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO execution_schedule (id, shard_scheme_version, shard_algorithm, lane_count) VALUES (1, 1, 'hash_mod_lane_count', 1)")
        .execute(&pool)
        .await
        .unwrap();

    let resolver = Arc::new(AddressResolver::new());
    resolver
        .reload_from_db(&pool)
        .await
        .expect("resolver must reload");

    let now = chrono::Utc::now().to_rfc3339();

    // Row 1 (positive control): an ordinary, qualifying trade -- must
    // ingest and plan as pending.
    let ordinary = row(
        "0xleader",
        "111",
        TradeSide::Buy,
        "5",
        "0.40",
        &now,
        "0xtx1",
    );

    // Rows 2 and 3 (the aggregated-event regression): one transaction hash,
    // two genuinely different fills -- different token, side, and price.
    // A parser/dedup defect that keyed only on transaction_hash would
    // silently drop one of these, losing which outcome was actually
    // traded.
    let aggregated_leg_yes = row(
        "0xleader",
        "222",
        TradeSide::Buy,
        "3",
        "0.60",
        &now,
        "0xtx2",
    );
    let aggregated_leg_no = row(
        "0xleader",
        "333",
        TradeSide::Sell,
        "4",
        "0.35",
        &now,
        "0xtx2",
    );

    // Row 4 (negative test: correct rejection): below the leader's policy
    // minimum (2) -- must ingest as an event but plan as a durable
    // rejection, not silently vanish and not become a live order.
    let too_small = row(
        "0xleader",
        "444",
        TradeSide::Buy,
        "1",
        "0.50",
        &now,
        "0xtx3",
    );

    for trade in [
        &ordinary,
        &aggregated_leg_yes,
        &aggregated_leg_no,
        &too_small,
    ] {
        let outcome = apply_trade(
            &pool,
            &resolver,
            trade,
            "activity_backfill",
            &trade.transaction_hash,
            "{}",
        )
        .await;
        assert!(
            matches!(outcome, polycopy_engine::copytrading::ingest::ProcessOutcome::Ingested { .. }),
            "every row in this batch is watched, activated, and well-formed -- all four must ingest: {outcome:?}"
        );
    }

    let event_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_events")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        event_count, 4,
        "the two same-transaction-hash fills must both be recorded as distinct events, not collapsed into one"
    );

    let recorded_tokens: Vec<String> = sqlx::query_scalar(
        "SELECT token_id FROM leader_events WHERE canonical_event_key LIKE 'activity:0xtx2:%' ORDER BY token_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        recorded_tokens,
        vec!["222".to_owned(), "333".to_owned()],
        "each aggregated fill's own outcome token must survive, not just one of the two"
    );

    let summary = plan_next_batch(&pool, 1)
        .await
        .expect("planning must succeed");
    assert_eq!(summary.processed, 4);
    assert_eq!(
        summary.pending, 3,
        "ordinary + both aggregated legs qualify"
    );
    assert_eq!(
        summary.rejected, 1,
        "the too-small trade is a durable rejection, not a dropped event"
    );

    let rejected_token: String =
        sqlx::query_scalar("SELECT token_id FROM copy_intents WHERE status = 'rejected'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(rejected_token, "444");

    let pending_tokens: Vec<String> = sqlx::query_scalar(
        "SELECT token_id FROM copy_intents WHERE status = 'pending' ORDER BY token_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        pending_tokens,
        vec!["111".to_owned(), "222".to_owned(), "333".to_owned()]
    );
}

#[derive(serde::Deserialize)]
struct FixtureRow {
    trader_address: String,
    token_id: String,
    condition_id: String,
    outcome_index: i64,
    side: String,
    size: String,
    price: String,
    event_time_ms: i64,
    source_event_id: String,
}

#[tokio::test]
async fn replays_a_real_production_batch_without_loss_or_a_panic() {
    let fixture = include_str!("fixtures/leader_activity_slim.json");
    let rows: Vec<FixtureRow> = serde_json::from_str(fixture).expect("fixture must be valid JSON");
    assert_eq!(
        rows.len(),
        305,
        "the fixture's known real row count -- catches a corrupted or truncated file"
    );

    let pool = open_temp_db().await;
    sqlx::query(
        "INSERT INTO accounts (id, label, signing_address, signature_type) \
         VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO execution_schedule (id, shard_scheme_version, shard_algorithm, lane_count) VALUES (1, 1, 'hash_mod_lane_count', 1)")
        .execute(&pool)
        .await
        .unwrap();

    // One leader per distinct real wallet in the fixture, all watched and
    // activated well before any row's real event time, with a policy loose
    // enough that this replay is testing pipeline integrity against
    // real-world-shaped data (varied precision, many distinct tokens, a mix
    // of BUY/SELL across nine independent leaders) -- not re-testing policy
    // rejection logic, which the synthetic test above already covers.
    let mut leader_id_by_wallet = std::collections::HashMap::new();
    for (index, wallet) in rows
        .iter()
        .map(|row| &row.trader_address)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .enumerate()
    {
        let leader_id = (index as i64) + 1;
        sqlx::query("INSERT INTO leader_config (id, label, enabled, activation_at) VALUES (?, ?, 1, '2020-01-01T00:00:00Z')")
            .bind(leader_id)
            .bind(format!("leader-{leader_id}"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO leader_wallet_aliases (leader_id, address, enabled) VALUES (?, ?, 1)",
        )
        .bind(leader_id)
        .bind(wallet)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO leader_policy \
             (leader_id, max_signal_age_seconds, decision_window_seconds, price_tolerance_bps, \
              tick_size, max_order_notional, min_leader_trade_size) \
             VALUES (?, 999999999, 300, 100, '0.01', '1000000', '0')",
        )
        .bind(leader_id)
        .execute(&pool)
        .await
        .unwrap();
        leader_id_by_wallet.insert(wallet.clone(), leader_id);
    }

    let resolver = Arc::new(AddressResolver::new());
    resolver
        .reload_from_db(&pool)
        .await
        .expect("resolver must reload");

    let mut ingested = 0usize;
    for fixture_row in &rows {
        let side = match fixture_row.side.as_str() {
            "BUY" => TradeSide::Buy,
            "SELL" => TradeSide::Sell,
            other => panic!("fixture has an unrecognized side: {other}"),
        };
        let occurred_at_utc = chrono::DateTime::from_timestamp_millis(fixture_row.event_time_ms)
            .expect("fixture event_time_ms must be a valid millisecond timestamp")
            .to_rfc3339();
        let trade = row(
            &fixture_row.trader_address,
            &fixture_row.token_id,
            side,
            &fixture_row.size,
            &fixture_row.price,
            &occurred_at_utc,
            &fixture_row.source_event_id,
        );
        let mut trade = trade;
        trade.condition_id = fixture_row.condition_id.clone();
        trade.outcome_index = fixture_row.outcome_index;

        let outcome = apply_trade(
            &pool,
            &resolver,
            &trade,
            "activity_backfill",
            &fixture_row.source_event_id,
            "{}",
        )
        .await;
        match outcome {
            polycopy_engine::copytrading::ingest::ProcessOutcome::Ingested { .. } => ingested += 1,
            other => panic!(
                "every fixture row is for a watched, activated leader with a well-formed side -- \
                 none should fail to ingest: {other:?}"
            ),
        }
    }
    assert_eq!(
        ingested, 305,
        "no real row was silently lost during ingestion"
    );

    let event_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_events")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        event_count, 305,
        "each real row must produce its own event -- no accidental collapsing"
    );

    let _ = leader_id_by_wallet;
    let summary =
        polycopy_engine::copytrading::plan_next_batch_with_limit(&pool, 1, rows.len() as i64)
            .await
            .unwrap_or_else(|error| {
                panic!("planning must never error on real production-shaped data: {error}")
            });
    assert_eq!(
        summary.processed, 305,
        "one batch large enough to cover every real event"
    );

    let total_intents: i64 = sqlx::query("SELECT COUNT(*) FROM copy_intents")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        total_intents, 305,
        "every ingested event must become exactly one intent (pending or a durable rejection), never zero and never more than one"
    );
    let unaccounted: i64 = sqlx::query(
        "SELECT COUNT(*) FROM copy_intents WHERE status NOT IN ('pending', 'rejected')",
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(
        unaccounted, 0,
        "a fresh plan over never-executed intents must only ever produce pending or rejected"
    );
}
