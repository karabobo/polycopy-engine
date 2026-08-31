//! Applying one already-normalized trade: resolve its leader, check
//! activation, and durably record it. Shared by every ingestion source
//! (`activity_ws`, `backfill`) so the activation rule and the
//! canonical-event/observation write path can't drift between them.

use std::fmt;

use sqlx::SqlitePool;

use super::{address_resolver::AddressResolver, normalize::NormalizedTrade, TradeSide};

/// What happened to one already-parsed trade, regardless of which source it
/// came from.
#[derive(Debug, PartialEq)]
pub enum ProcessOutcome {
    /// Written to `leader_events`/`leader_event_observations` (or already
    /// present from a prior observation of the same canonical event).
    Ingested { leader_id: i64, canonical_event_key: String },
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
            Self::Ingested { leader_id, canonical_event_key } => {
                write!(formatter, "ingested leader_id={leader_id} key={canonical_event_key}")
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
    let Some(leader_id) = resolver.resolve(&trade.trader_address) else {
        return ProcessOutcome::NotWatched;
    };

    let activation_at: Option<String> = match sqlx::query_scalar(
        "SELECT activation_at FROM leader_config WHERE id = ?",
    )
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
    // Confirmed live against real Data API results (2026-08-31): a single
    // settlement transaction can carry more than one of a leader's trades
    // (e.g. one taker order matching two maker orders), each a genuinely
    // separate fill with a different size. transaction_hash alone is not a
    // safe canonical key -- it silently dropped the second fill under
    // INSERT OR IGNORE. There is no per-trade sequence/log-index field on
    // either the REST Activity response or the WS payload, so this composes
    // the most specific available fields as a practical (not perfect)
    // disambiguator: two fills in one transaction still collide if they
    // also share token, side, price, AND size.
    let canonical_event_key = format!(
        "activity:{}:{}:{side}:{}:{}",
        trade.transaction_hash, trade.token_id, trade.price, trade.size
    );

    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return ProcessOutcome::DatabaseError(error.to_string()),
    };

    let insert_event = sqlx::query(
        "INSERT OR IGNORE INTO leader_events \
         (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, \
          price, tx_hash, occurred_at, observed_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
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
    .execute(&mut *tx)
    .await;
    if let Err(error) = insert_event {
        return ProcessOutcome::DatabaseError(error.to_string());
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

    ProcessOutcome::Ingested { leader_id, canonical_event_key }
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

        let first = apply_trade(&db, &resolver, &trade("8839.47", "0xshared"), "activity_backfill", "0xshared", "{}").await;
        let second = apply_trade(&db, &resolver, &trade("3542.23", "0xshared"), "activity_backfill", "0xshared", "{}").await;

        assert!(matches!(first, ProcessOutcome::Ingested { .. }));
        assert!(matches!(second, ProcessOutcome::Ingested { .. }));
        assert_ne!(first, second, "two distinct fills must not collapse onto one canonical event");

        let event_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_events WHERE tx_hash = '0xshared'")
            .fetch_one(&*db)
            .await
            .expect("event count must be queryable")
            .get(0);
        assert_eq!(event_count, 2, "both fills sharing one transaction hash must be recorded");
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
        assert_eq!(event_count, 1, "a true replay of the identical trade must not double-count");
    }
}
