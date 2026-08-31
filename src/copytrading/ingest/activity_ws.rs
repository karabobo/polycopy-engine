//! Activity WebSocket connection manager and message processing.
//!
//! Protocol details (endpoint, subscribe payload, ping/pong keepalive,
//! reconnect behavior) are confirmed against `PolymarketActivityWsService.kt`
//! in this project's predecessor, PolyHermes -- see `normalize.rs`'s module
//! doc for why this isn't sourced from official Polymarket documentation.

use std::{error::Error as StdError, fmt, time::Duration};

use futures_util::{SinkExt as _, StreamExt as _};
use sqlx::SqlitePool;
use tokio_tungstenite::tungstenite::Message;

use super::{address_resolver::AddressResolver, apply::apply_trade, normalize, normalize::ParseResult};
pub use super::apply::ProcessOutcome;

pub const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";
const SUBSCRIBE_MESSAGE: &str = r#"{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"},{"topic":"activity","type":"orders_matched"}]}"#;
const PING_INTERVAL: Duration = Duration::from_secs(10);
const STALE_AFTER: Duration = Duration::from_secs(30);
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(3);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

/// `rustls` 0.23+ does not pick a default crypto backend on its own; without
/// installing one, every TLS connect attempt (including
/// `tokio_tungstenite::connect_async`) panics or hangs. Safe to call before
/// every reconnect: `install_default` only has an effect the first time.
fn ensure_crypto_provider_installed() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Parses `raw`, then delegates to [`apply_trade`] (shared with
/// `backfill`) for resolution, activation, and the durable write. This is
/// the entire per-message decision the connection loop makes; it does no
/// networking itself, so it is testable against a real (temp-file)
/// database without a live WebSocket.
pub async fn process_message(
    pool: &SqlitePool,
    resolver: &AddressResolver,
    raw: &str,
) -> ProcessOutcome {
    let trade = match normalize::parse(raw) {
        ParseResult::Trade(trade) => trade,
        ParseResult::Skip => return ProcessOutcome::Skip,
        ParseResult::Rejected(reason) => return ProcessOutcome::Rejected(reason),
    };
    let transaction_hash = trade.transaction_hash.clone();
    apply_trade(pool, resolver, &trade, "activity_ws", &transaction_hash, raw).await
}

/// Runs the activity WebSocket connection forever, reconnecting with
/// exponential backoff (3s, 6s, 12s, 24s, capped at 60s, matching the
/// reference implementation) on any disconnect, error, or detected staleness.
/// Never returns under normal operation; only returns if `pool` itself
/// becomes unusable in a way a reconnect cannot fix.
pub async fn run(pool: SqlitePool, resolver: &AddressResolver) -> ! {
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;
    loop {
        match run_once(&pool, resolver).await {
            Err(error) => eprintln!("activity ws: {error}, reconnecting in {reconnect_delay:?}"),
            Ok(never) => match never {},
        }
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
    }
}

/// One connection attempt. Returns `Ok` only in the unreachable case (there
/// is currently no clean-shutdown signal); every real exit path is an
/// `Err`, including a graceful server-initiated close, so the caller always
/// treats leaving this function as "reconnect".
async fn run_once(pool: &SqlitePool, resolver: &AddressResolver) -> Result<std::convert::Infallible, ActivityWsError> {
    ensure_crypto_provider_installed();

    let (ws_stream, _response) = tokio_tungstenite::connect_async(RTDS_URL)
        .await
        .map_err(|error| ActivityWsError::Connect(Box::new(error)))?;
    let (mut writer, mut reader) = ws_stream.split();

    writer
        .send(Message::Text(SUBSCRIBE_MESSAGE.into()))
        .await
        .map_err(|error| ActivityWsError::Send(Box::new(error)))?;

    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    ping_interval.tick().await; // the first tick fires immediately; skip it.
    let mut last_activity = tokio::time::Instant::now();

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if last_activity.elapsed() > STALE_AFTER {
                    return Err(ActivityWsError::Stale);
                }
                writer
                    .send(Message::Text("ping".into()))
                    .await
                    .map_err(|error| ActivityWsError::Send(Box::new(error)))?;
            }
            message = reader.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let outcome = process_message(pool, resolver, &text).await;
                        if outcome != ProcessOutcome::Skip {
                            // A real activity-topic message, whether or not
                            // it belonged to a watched leader: proves the
                            // subscription is actually delivering data, not
                            // just that the socket is open (a plain pong
                            // would not prove that).
                            last_activity = tokio::time::Instant::now();
                        }
                        if let ProcessOutcome::DatabaseError(_) = &outcome {
                            eprintln!("activity ws: {outcome}");
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => return Err(ActivityWsError::ConnectionClosed),
                    // Binary/native ping/pong frames: this protocol uses
                    // application-level text "ping"/"PONG", not WS control
                    // frames, so there is nothing to act on here.
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(ActivityWsError::Stream(Box::new(error))),
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum ActivityWsError {
    // Boxed: tungstenite::Error is 136+ bytes, which would otherwise make
    // every ActivityWsError (including the cheap variants) that large too.
    Connect(Box<tokio_tungstenite::tungstenite::Error>),
    Send(Box<tokio_tungstenite::tungstenite::Error>),
    Stream(Box<tokio_tungstenite::tungstenite::Error>),
    ConnectionClosed,
    Stale,
}

impl fmt::Display for ActivityWsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(source) => write!(formatter, "unable to connect: {source}"),
            Self::Send(source) => write!(formatter, "unable to send: {source}"),
            Self::Stream(source) => write!(formatter, "stream error: {source}"),
            Self::ConnectionClosed => write!(formatter, "connection closed"),
            Self::Stale => write!(
                formatter,
                "no activity-topic message received within {STALE_AFTER:?}"
            ),
        }
    }
}

impl StdError for ActivityWsError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Connect(source) | Self::Send(source) | Self::Stream(source) => Some(source),
            Self::ConnectionClosed | Self::Stale => None,
        }
    }
}

#[cfg(all(test, feature = "ingest"))]
mod tests {
    use sqlx::Row as _;

    use super::*;
    use crate::copytrading::db::open_and_migrate;

    // Mirrors src/copytrading/db.rs's TestDb: a migrated pool at a unique
    // temp path, cleaned up on drop. Not shared with db.rs because that
    // module's test helper is private to its own `#[cfg(test)]` block.
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
                "polycopy-engine-activity-ws-test-{}-{nonce}-{counter}.sqlite",
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

    async fn seed_activated_leader(db: &TestDb, leader_id: i64, activation_at: &str) {
        sqlx::query("INSERT INTO leader_config (id, label, activation_at) VALUES (?, ?, ?)")
            .bind(leader_id)
            .bind(format!("leader-{leader_id}"))
            .bind(activation_at)
            .execute(&**db)
            .await
            .expect("leader must insert");
    }

    fn trade_message(tx_hash: &str, trader_address: &str, occurred_at_unix: i64) -> String {
        format!(
            r#"{{"topic":"activity","type":"trades","payload":{{"asset":"123","conditionId":"0xcond","outcomeIndex":0,"side":"BUY","price":"0.5","size":"5","timestamp":{occurred_at_unix},"transactionHash":"{tx_hash}","trader":{{"address":"{trader_address}"}}}}}}"#
        )
    }

    #[tokio::test]
    async fn a_message_from_an_unwatched_address_is_not_ingested() {
        let db = TestDb::new().await;
        let resolver = AddressResolver::new();

        let outcome = process_message(&db, &resolver, &trade_message("0xh1", "0xdeadbeef", 1735689600)).await;
        assert_eq!(outcome, ProcessOutcome::NotWatched);
    }

    #[tokio::test]
    async fn a_never_activated_leader_rejects_every_event() {
        let db = TestDb::new().await;
        sqlx::query("INSERT INTO leader_config (id, label) VALUES (1, 'leader-one')")
            .execute(&*db)
            .await
            .expect("leader must insert");
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let outcome = process_message(&db, &resolver, &trade_message("0xh1", "0xleader", 1735689600)).await;
        assert_eq!(outcome, ProcessOutcome::LeaderNotActivated);
    }

    #[tokio::test]
    async fn a_trade_before_activation_is_rejected_even_though_the_leader_is_activated() {
        let db = TestDb::new().await;
        seed_activated_leader(&db, 1, "2026-01-01T00:00:00.000Z").await;
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        // 1735689600 is 2025-01-01T00:00:00Z -- before the 2026 activation.
        let outcome = process_message(&db, &resolver, &trade_message("0xh1", "0xleader", 1735689600)).await;
        assert_eq!(outcome, ProcessOutcome::BeforeActivation);
    }

    #[tokio::test]
    async fn a_watched_activated_leader_trade_is_ingested_into_both_tables() {
        let db = TestDb::new().await;
        seed_activated_leader(&db, 1, "2020-01-01T00:00:00.000Z").await;
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let outcome = process_message(&db, &resolver, &trade_message("0xh1", "0xleader", 1735689600)).await;
        assert_eq!(
            outcome,
            ProcessOutcome::Ingested {
                leader_id: 1,
                canonical_event_key: "activity:0xh1:123:BUY:0.5:5".to_owned()
            }
        );

        let event_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_events WHERE canonical_event_key = 'activity:0xh1:123:BUY:0.5:5'")
            .fetch_one(&*db)
            .await
            .expect("event count must be queryable")
            .get(0);
        assert_eq!(event_count, 1);

        let observation_count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM leader_event_observations WHERE source = 'activity_ws' AND source_identifier = '0xh1'",
        )
        .fetch_one(&*db)
        .await
        .expect("observation count must be queryable")
        .get(0);
        assert_eq!(observation_count, 1);
    }

    #[tokio::test]
    async fn replaying_the_same_message_twice_ingests_once_and_the_second_call_still_reports_ingested() {
        let db = TestDb::new().await;
        seed_activated_leader(&db, 1, "2020-01-01T00:00:00.000Z").await;
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let message = trade_message("0xh1", "0xleader", 1735689600);
        let first = process_message(&db, &resolver, &message).await;
        let second = process_message(&db, &resolver, &message).await;

        assert_eq!(first, second, "replay must be idempotent, not an error");

        let event_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_events")
            .fetch_one(&*db)
            .await
            .expect("event count must be queryable")
            .get(0);
        assert_eq!(event_count, 1, "replay must not create a second event");
    }

    #[tokio::test]
    async fn orders_matched_observing_the_same_trade_as_trades_attaches_to_one_event() {
        let db = TestDb::new().await;
        seed_activated_leader(&db, 1, "2020-01-01T00:00:00.000Z").await;
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let trades_message = trade_message("0xh1", "0xleader", 1735689600);
        let orders_matched_message = trades_message.replace("\"type\":\"trades\"", "\"type\":\"orders_matched\"");

        process_message(&db, &resolver, &trades_message).await;
        process_message(&db, &resolver, &orders_matched_message).await;

        let event_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_events")
            .fetch_one(&*db)
            .await
            .expect("event count must be queryable")
            .get(0);
        assert_eq!(event_count, 1);

        // Both pushes share one source ("activity_ws") and one
        // source_identifier (the tx hash): the second is a duplicate under
        // migration 0002's unique index, not a second observation.
        let observation_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_event_observations")
            .fetch_one(&*db)
            .await
            .expect("observation count must be queryable")
            .get(0);
        assert_eq!(observation_count, 1);
    }
}
