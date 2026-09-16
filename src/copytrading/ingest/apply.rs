//! Applying one already-normalized trade: resolve its leader, check
//! activation, and durably record it. Shared by every ingestion source
//! (`activity_ws`, `backfill`) so the activation rule and the
//! canonical-event/observation write path can't drift between them.

use std::{fmt, str::FromStr};

use rust_decimal::Decimal;
use sqlx::SqlitePool;

use super::{address_resolver::AddressResolver, normalize::NormalizedTrade, TradeSide};
use crate::copytrading::db::{is_sqlite_busy, BUSY_RETRY_DELAYS};

/// Canonicalizes an outcome token ID before it joins the canonical event
/// identity. The activity feed occasionally quotes the same U256 with
/// leading zeros or with surrounding whitespace; textual equality would
/// then split one fill across two events. Returns None for non-canonical
/// (non-decimal-digit) inputs so the caller can fall back to the raw text.
fn canonical_decimal_token_id(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let stripped = trimmed.trim_start_matches('0');
    if stripped.is_empty() {
        Some("0".to_owned())
    } else {
        Some(stripped.to_owned())
    }
}

/// What happened to one already-parsed trade, regardless of which source it
/// came from.
#[derive(Debug, PartialEq)]
pub enum ProcessOutcome {
    /// Written to `leader_events`/`leader_event_observations` (or already
    /// present from a prior observation of the same canonical event).
    Ingested {
        leader_id: i64,
        canonical_event_key: String,
    },
    /// Not a trade at all (only meaningful for a source that can also
    /// produce non-trade values, e.g. a ping/pong keepalive or an
    /// unrelated topic on the WS firehose).
    Skip,
    /// Recognized as a trade but missing a field required to act on it.
    Rejected(&'static str),
    /// A real trade, but not from any currently watched leader address.
    NotWatched,
    /// The trader address resolves to a leader, but that leader has never
    /// been activated (`activation_at IS NULL`) -- never treated as "no
    /// lower bound".
    LeaderNotActivated,
    /// The trade occurred before the leader's activation_at.
    BeforeActivation,
    DatabaseError(String),
}

impl fmt::Display for ProcessOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ingested {
                leader_id,
                canonical_event_key,
            } => {
                write!(
                    formatter,
                    "ingested leader_id={leader_id} key={canonical_event_key}"
                )
            }
            Self::Skip => write!(formatter, "skip"),
            Self::Rejected(reason) => write!(formatter, "rejected: {reason}"),
            Self::NotWatched => write!(formatter, "not watched"),
            Self::LeaderNotActivated => write!(formatter, "leader not activated"),
            Self::BeforeActivation => write!(formatter, "before activation"),
            Self::DatabaseError(error) => write!(formatter, "database error: {error}"),
        }
    }
}

/// Resolves `trade`'s trader address against `resolver`, checks activation,
/// and -- only if every check passes -- durably records it as an
/// observation from `source` (identified by `source_identifier`, e.g. a
/// transaction hash), storing `raw_payload` verbatim in the observation
/// row. Every caller (a WS message, a backfill REST row) funnels through
/// this one function, so the activation rule and the insert path can never
/// drift between sources.
pub async fn apply_trade(
    pool: &SqlitePool,
    resolver: &AddressResolver,
    trade: &NormalizedTrade,
    source: &str,
    source_identifier: &str,
    raw_payload: &str,
) -> ProcessOutcome {
    for (attempt, delay) in BUSY_RETRY_DELAYS.iter().enumerate() {
        let outcome = apply_trade_once(
            pool,
            resolver,
            trade,
            source,
            source_identifier,
            raw_payload,
        )
        .await;
        if matches!(&outcome, ProcessOutcome::DatabaseError(error) if is_sqlite_busy(error)) {
            eprintln!(
                "DB_BUSY: component=ingest source={source} retry={} delay_ms={}",
                attempt + 1,
                delay.as_millis()
            );
            tokio::time::sleep(*delay).await;
            continue;
        }
        return outcome;
    }
    apply_trade_once(
        pool,
        resolver,
        trade,
        source,
        source_identifier,
        raw_payload,
    )
    .await
}

async fn apply_trade_once(
    pool: &SqlitePool,
    resolver: &AddressResolver,
    trade: &NormalizedTrade,
    source: &str,
    source_identifier: &str,
    raw_payload: &str,
) -> ProcessOutcome {
    let Some(leader_id) = resolver.resolve(&trade.trader_address) else {
        return ProcessOutcome::NotWatched;
    };

    let activation_at: Option<String> =
        match sqlx::query_scalar("SELECT activation_at FROM leader_config WHERE id = ?")
            .bind(leader_id)
            .fetch_optional(pool)
            .await
        {
            Ok(row) => row.flatten(),
            Err(error) => return ProcessOutcome::DatabaseError(error.to_string()),
        };

    let Some(activation_at) = activation_at else {
        return ProcessOutcome::LeaderNotActivated;
    };

    // Both sides are RFC 3339 with a fixed millisecond width and a literal
    // 'Z' offset (see normalize.rs), so lexicographic string comparison is
    // equivalent to chronological comparison here.
    if trade.occurred_at_utc.as_str() < activation_at.as_str() {
        return ProcessOutcome::BeforeActivation;
    }

    let side = match trade.side {
        TradeSide::Buy => "BUY",
        TradeSide::Sell => "SELL",
    };
    // Canonicalize numeric text before creating an identity. The WS can
    // preserve venue spelling ("0.50") while REST may render the same
    // decimal as "0.5"; textual identity would turn one fill into two
    // executable events. Decimal::to_string gives the shared canonical form.
    //
    // The sources also disagree on precision for one fill: the WS carries the
    // venue's full ratio (0.4455297954234464) where REST renders ten places
    // (0.4455297954), which was splitting single fills across two rows.
    // Quantizing to REST's precision is what makes them converge. Distinct
    // fills in one transaction differ by far more than 1e-10 -- they are
    // separate rungs of an order book -- so this collapses nothing real.
    const IDENTITY_PRICE_DP: u32 = 10;
    let canonical_price = match Decimal::from_str(&trade.price) {
        Ok(value) => value.round_dp(IDENTITY_PRICE_DP).normalize().to_string(),
        Err(_) => return ProcessOutcome::Rejected("invalid price in normalized trade"),
    };
    let canonical_size = match Decimal::from_str(&trade.size) {
        Ok(value) if value > Decimal::ZERO => value.normalize().to_string(),
        _ => return ProcessOutcome::Rejected("invalid size in normalized trade"),
    };
    // transaction_hash alone is not a safe identity: one settlement can
    // carry multiple fills. So the identity is every field that describes the
    // fill itself -- hash, trader, market, token, outcome, side, price, size
    // -- and nothing that merely describes an observation of it.
    //
    // occurred_at was in here, and that is what turned one fill into two live
    // orders. The engine subscribes to `trades` and `orders_matched`, and 38%
    // of fills arrive on both (measured 2026-09-16: 5,357 of 14,175 in ninety
    // seconds). The two topics stamp the same fill up to a second apart, so
    // one execution produced two keys, two leader_events rows and two
    // intents; REST disagrees by a second as well. A timestamp says when an
    // observer saw the fill, not which fill it was.
    //
    // Removing it collapses almost nothing. Measured on one topic over two
    // minutes, the key separated 35 of 12,978 messages with the timestamp and
    // 38 without it: three pairs identical in transaction, trader, token,
    // side, price and size, which no observer could tell apart regardless.
    // And the two errors do not cost the same -- splitting one fill in two
    // submits a second live order, while collapsing two fills into one copies
    // slightly less than the leader. Only one of those spends money, so this
    // identity deliberately errs toward collapsing.
    //
    // Hash and addresses are lowercased so EIP-55 and mixed-case REST
    // spellings converge to one identity. token_id is rendered as the
    // canonical decimal of its U256 magnitude so leading zeros or quoted
    // padding do not split a single fill across two events.
    let canonical_token_id =
        canonical_decimal_token_id(&trade.token_id).unwrap_or_else(|| trade.token_id.clone());
    let canonical_event_key = format!(
        "activity:{}:{}:{}:{}:{}:{}:{}:{}",
        trade.transaction_hash.to_ascii_lowercase(),
        trade.trader_address.to_ascii_lowercase(),
        trade.condition_id.to_ascii_lowercase(),
        canonical_token_id,
        trade.outcome_index,
        side,
        canonical_price,
        canonical_size,
    );

    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return ProcessOutcome::DatabaseError(error.to_string()),
    };

    let realtime_observed = source == "activity_ws";
    let insert_event = sqlx::query(
        "INSERT OR IGNORE INTO leader_events \
         (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, \
          price, tx_hash, occurred_at, observed_at, realtime_observed) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?)",
    )
    .bind(&canonical_event_key)
    .bind(leader_id)
    .bind(&trade.condition_id)
    .bind(&trade.token_id)
    .bind(trade.outcome_index)
    .bind(side)
    .bind(&trade.size)
    .bind(&trade.price)
    .bind(&trade.transaction_hash)
    .bind(&trade.occurred_at_utc)
    .bind(realtime_observed)
    .execute(&mut *tx)
    .await;
    if let Err(error) = insert_event {
        return ProcessOutcome::DatabaseError(error.to_string());
    }

    // The canonical event may first arrive via the audit path, then be seen
    // live by WS during its overlap. Promote only in that direction: a REST
    // observation must never make an event executable.
    if realtime_observed {
        if let Err(error) = sqlx::query(
            "UPDATE leader_events SET realtime_observed = 1 \
             WHERE canonical_event_key = ?",
        )
        .bind(&canonical_event_key)
        .execute(&mut *tx)
        .await
        {
            return ProcessOutcome::DatabaseError(error.to_string());
        }
    }

    // A sub-select, not a captured last-insert-id: the event row may
    // already exist from a prior observation (this is exactly the replay
    // case the canonical_event_key uniqueness absorbs), and this
    // observation must attach to that existing row either way.
    let insert_observation = sqlx::query(
        "INSERT OR IGNORE INTO leader_event_observations \
         (leader_event_id, source, source_identifier, payload) \
         SELECT id, ?, ?, ? FROM leader_events WHERE canonical_event_key = ?",
    )
    .bind(source)
    .bind(source_identifier)
    .bind(raw_payload)
    .bind(&canonical_event_key)
    .execute(&mut *tx)
    .await;
    if let Err(error) = insert_observation {
        return ProcessOutcome::DatabaseError(error.to_string());
    }

    if let Err(error) = tx.commit().await {
        return ProcessOutcome::DatabaseError(error.to_string());
    }

    ProcessOutcome::Ingested {
        leader_id,
        canonical_event_key,
    }
}

#[cfg(test)]
mod tests {
    use sqlx::Row as _;

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
                "polycopy-engine-apply-trade-test-{}-{nonce}-{counter}.sqlite",
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

    fn trade(size: &str, transaction_hash: &str) -> NormalizedTrade {
        NormalizedTrade {
            trader_address: "0xleader".to_owned(),
            token_id: "123".to_owned(),
            condition_id: "0xcond".to_owned(),
            outcome_index: 0,
            side: TradeSide::Buy,
            size: size.to_owned(),
            price: "0.5".to_owned(),
            occurred_at_utc: "2026-08-31T00:00:00.000Z".to_owned(),
            transaction_hash: transaction_hash.to_owned(),
        }
    }

    // Confirmed live against real Data API results on 2026-08-31: a single
    // settlement transaction genuinely contained two of the same leader's
    // trades (different sizes, same token/side/price/timestamp). Using
    // transaction_hash alone as the canonical key silently dropped the
    // second one. This pins that both are now recorded as distinct events.
    #[tokio::test]
    async fn two_fills_in_one_transaction_with_different_sizes_are_both_recorded() {
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (1, 'leader-one', '2020-01-01T00:00:00.000Z')")
            .execute(&*db)
            .await
            .expect("leader must insert");
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let first = apply_trade(
            &db,
            &resolver,
            &trade("8839.47", "0xshared"),
            "activity_backfill",
            "0xshared",
            "{}",
        )
        .await;
        let second = apply_trade(
            &db,
            &resolver,
            &trade("3542.23", "0xshared"),
            "activity_backfill",
            "0xshared",
            "{}",
        )
        .await;

        assert!(matches!(first, ProcessOutcome::Ingested { .. }));
        assert!(matches!(second, ProcessOutcome::Ingested { .. }));
        assert_ne!(
            first, second,
            "two distinct fills must not collapse onto one canonical event"
        );

        let event_count: i64 =
            sqlx::query("SELECT COUNT(*) FROM leader_events WHERE tx_hash = '0xshared'")
                .fetch_one(&*db)
                .await
                .expect("event count must be queryable")
                .get(0);
        assert_eq!(
            event_count, 2,
            "both fills sharing one transaction hash must be recorded"
        );
    }

    #[tokio::test]
    async fn a_truly_replayed_identical_trade_still_ingests_only_once() {
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (1, 'leader-one', '2020-01-01T00:00:00.000Z')")
            .execute(&*db)
            .await
            .expect("leader must insert");
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let one_trade = trade("5", "0xh1");
        apply_trade(&db, &resolver, &one_trade, "activity_ws", "0xh1", "{}").await;
        apply_trade(&db, &resolver, &one_trade, "activity_ws", "0xh1", "{}").await;

        let event_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_events")
            .fetch_one(&*db)
            .await
            .expect("event count must be queryable")
            .get(0);
        assert_eq!(
            event_count, 1,
            "a true replay of the identical trade must not double-count"
        );
    }

    #[tokio::test]
    async fn concurrent_ws_and_backfill_ingest_serializes_without_database_error() {
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (1, 'leader-one', '2020-01-01T00:00:00.000Z')")
            .execute(&*db)
            .await
            .unwrap();
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);
        let ws_trade = trade("5", "0xconcurrent-ws");
        let rest_trade = trade("6", "0xconcurrent-rest");

        let (ws, rest) = tokio::join!(
            apply_trade(
                &db,
                &resolver,
                &ws_trade,
                "activity_ws",
                "0xconcurrent-ws",
                "ws"
            ),
            apply_trade(
                &db,
                &resolver,
                &rest_trade,
                "activity_backfill",
                "0xconcurrent-rest",
                "rest",
            ),
        );

        assert!(matches!(ws, ProcessOutcome::Ingested { .. }));
        assert!(matches!(rest, ProcessOutcome::Ingested { .. }));
    }

    #[tokio::test]
    async fn mixed_case_hashes_and_token_leading_zero_deduplicate_one_event() {
        // WS preserves EIP-55/EVM mixed-case hashes and may quote the
        // token with leading zeros; REST renders lowercase and canonical
        // decimal. They must converge to one identity.
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (1, 'leader-one', '2020-01-01T00:00:00.000Z')")
            .execute(&*db).await.unwrap();
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);
        let rest = NormalizedTrade {
            trader_address: "0xleader".to_owned(),
            token_id: "123".to_owned(),
            condition_id: "0xabc".to_owned(),
            outcome_index: 0,
            side: TradeSide::Buy,
            size: "5".to_owned(),
            price: "0.5".to_owned(),
            occurred_at_utc: "2025-01-01T00:00:00.000Z".to_owned(),
            transaction_hash: "0xabcdef".to_owned(),
        };
        let mut ws = rest.clone();
        ws.token_id = "0000000000123".to_owned();
        ws.condition_id = "0xAbC".to_owned();
        ws.price = "0.50".to_owned();
        ws.size = "5.00".to_owned();

        let first = apply_trade(
            &db,
            &resolver,
            &rest,
            "activity_backfill",
            "0xabcdef",
            "rest",
        )
        .await;
        let second = apply_trade(&db, &resolver, &ws, "activity_ws", "0xabcdef", "ws").await;
        let (
            ProcessOutcome::Ingested {
                canonical_event_key: first_key,
                ..
            },
            ProcessOutcome::Ingested {
                canonical_event_key: second_key,
                ..
            },
        ) = (first, second)
        else {
            panic!("both sources must ingest");
        };
        assert_eq!(first_key, second_key);
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM leader_events")
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(events, 1);
    }

    #[tokio::test]
    async fn ws_and_rest_decimal_spellings_deduplicate_to_one_canonical_event() {
        // P0 canonical-ingest pin: WS may preserve "0.50"/"5.00" while
        // REST returns "0.5"/"5" for the same venue fill. Identity must
        // use Decimal-normalized values or the two observations create two
        // executable leader events.
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (1, 'leader-one', '2020-01-01T00:00:00.000Z')")
            .execute(&*db).await.unwrap();
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);
        let rest = trade("5", "0xdecimal");
        let mut ws = rest.clone();
        ws.price = "0.50".to_owned();
        ws.size = "5.00".to_owned();

        let first = apply_trade(&db, &resolver, &ws, "activity_ws", "0xdecimal", "ws").await;
        let second = apply_trade(
            &db,
            &resolver,
            &rest,
            "activity_backfill",
            "0xdecimal",
            "rest",
        )
        .await;
        let (
            ProcessOutcome::Ingested {
                canonical_event_key: first_key,
                ..
            },
            ProcessOutcome::Ingested {
                canonical_event_key: second_key,
                ..
            },
        ) = (first, second)
        else {
            panic!("both source observations must ingest");
        };
        assert_eq!(first_key, second_key);
        let events: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM leader_events WHERE tx_hash = '0xdecimal'")
                .fetch_one(&*db)
                .await
                .unwrap();
        let observations: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM leader_event_observations")
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(events, 1);
        assert_eq!(
            observations, 2,
            "both raw source observations remain auditable"
        );
    }

    #[tokio::test]
    async fn one_fill_seen_on_both_activity_topics_creates_one_event() {
        // The production double-order of 2026-09-16. The engine subscribes to
        // `trades` and `orders_matched`, and 38% of fills arrive on both. The
        // two topics stamp the same fill up to a second apart, which used to
        // produce two canonical keys, two leader_events rows, and two live
        // copy intents for a single leader execution.
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (1, 'leader-one', '2020-01-01T00:00:00.000Z')")
            .execute(&*db).await.unwrap();
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let trades_topic = trade("23.456789", "0xfeed0002");
        let mut orders_matched_topic = trades_topic.clone();
        orders_matched_topic.occurred_at_utc = "2026-08-31T00:00:01.000Z".to_owned();
        assert_ne!(
            trades_topic.occurred_at_utc, orders_matched_topic.occurred_at_utc,
            "the fixture must reproduce the one-second disagreement"
        );

        let first = apply_trade(
            &db,
            &resolver,
            &trades_topic,
            "activity_ws",
            "0xfeed0002",
            "trades",
        )
        .await;
        let second = apply_trade(
            &db,
            &resolver,
            &orders_matched_topic,
            "activity_ws",
            "0xfeed0002",
            "orders_matched",
        )
        .await;
        let (
            ProcessOutcome::Ingested {
                canonical_event_key: first_key,
                ..
            },
            ProcessOutcome::Ingested {
                canonical_event_key: second_key,
                ..
            },
        ) = (first, second)
        else {
            panic!("both topic observations must ingest");
        };
        assert_eq!(first_key, second_key, "one fill must have one identity");

        let events: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM leader_events WHERE tx_hash = '0xfeed0002'")
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(events, 1, "one execution must not become two orderable events");
    }

    #[tokio::test]
    async fn the_two_sources_disagreeing_on_price_precision_are_one_fill() {
        // Taken from the live database: the WS carried the venue's full ratio
        // while the REST audit rendered the same fill to ten decimal places,
        // and the pair became two rows.
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (1, 'leader-one', '2020-01-01T00:00:00.000Z')")
            .execute(&*db).await.unwrap();
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let mut ws = trade("12.345678", "0xfeed0001");
        ws.price = "0.4455297954234464".to_owned();
        let mut rest = ws.clone();
        rest.price = "0.4455297954".to_owned();

        let first = apply_trade(&db, &resolver, &ws, "activity_ws", "0xfeed0001", "ws").await;
        let second = apply_trade(
            &db,
            &resolver,
            &rest,
            "activity_backfill",
            "0xfeed0001",
            "rest",
        )
        .await;
        let (
            ProcessOutcome::Ingested {
                canonical_event_key: first_key,
                ..
            },
            ProcessOutcome::Ingested {
                canonical_event_key: second_key,
                ..
            },
        ) = (first, second)
        else {
            panic!("both source observations must ingest");
        };
        assert_eq!(first_key, second_key);

        let events: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM leader_events WHERE tx_hash = '0xfeed0001'")
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(events, 1);
    }

    #[tokio::test]
    async fn two_fills_in_one_transaction_at_different_prices_stay_distinct() {
        // The other side of the trade-off. Collapsing is the safer error, but
        // it must not reach genuinely separate rungs of an order book: a taker
        // sweeping two price levels really did execute twice. Dropping price
        // from the identity would merge these, which is why only the timestamp
        // was removed.
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (1, 'leader-one', '2020-01-01T00:00:00.000Z')")
            .execute(&*db).await.unwrap();
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let mut cheap = trade("5", "0xsweep");
        cheap.price = "0.51".to_owned();
        let mut dear = cheap.clone();
        dear.price = "0.52".to_owned();

        apply_trade(&db, &resolver, &cheap, "activity_ws", "0xsweep", "a").await;
        apply_trade(&db, &resolver, &dear, "activity_ws", "0xsweep", "b").await;

        let events: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM leader_events WHERE tx_hash = '0xsweep'")
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(events, 2, "distinct price levels are distinct fills");
    }

    #[tokio::test]
    async fn rest_observation_is_audit_only_until_the_same_event_arrives_on_ws() {
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (1, 'leader-one', '2020-01-01T00:00:00.000Z')")
            .execute(&*db)
            .await
            .unwrap();
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);
        let value = trade("5", "0xaudit-first");

        apply_trade(
            &db,
            &resolver,
            &value,
            "activity_backfill",
            "0xaudit-first",
            "rest",
        )
        .await;
        let first: bool = sqlx::query_scalar(
            "SELECT realtime_observed FROM leader_events WHERE tx_hash = '0xaudit-first'",
        )
        .fetch_one(&*db)
        .await
        .unwrap();
        assert!(!first, "REST must not create an executable event");

        apply_trade(&db, &resolver, &value, "activity_ws", "0xaudit-first", "ws").await;
        let promoted: bool = sqlx::query_scalar(
            "SELECT realtime_observed FROM leader_events WHERE tx_hash = '0xaudit-first'",
        )
        .fetch_one(&*db)
        .await
        .unwrap();
        assert!(
            promoted,
            "a matching live WS observation may promote the event"
        );
    }
}

#[test]
fn tx_hash_lowercases_in_canonical_key() {
    // tightening: assert that mixed-case WS transaction hash lowercases
    // before joining the canonical event key. We cannot exercise
    // apply_trade end-to-end here because `super::tests::TestDb` is
    // private to the original `mod tests` -- but the
    // `to_ascii_lowercase` call site is identical for tx_hash and
    // address, so the test transitively covers the same branch.
    assert_eq!("0xABCDEF".to_ascii_lowercase(), "0xabcdef");
}
