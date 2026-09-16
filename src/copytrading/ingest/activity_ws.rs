//! Activity WebSocket connection manager and message processing.
//!
//! Protocol details (endpoint, subscribe payload, ping/pong keepalive,
//! reconnect behavior) are confirmed against `PolymarketActivityWsService.kt`
//! in this project's predecessor, PolyHermes -- see `normalize.rs`'s module
//! doc for why this isn't sourced from official Polymarket documentation.

use std::{
    error::Error as StdError,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use futures_util::{SinkExt as _, StreamExt as _};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio_tungstenite::tungstenite::Message;

pub use super::apply::ProcessOutcome;
use super::{
    address_resolver::AddressResolver, apply::apply_trade, normalize, normalize::ParseResult,
};

pub const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";
const SUBSCRIBE_MESSAGE: &str = r#"{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"},{"topic":"activity","type":"orders_matched"}]}"#;
// The Activity topic can legitimately be quiet for much longer than a
// heartbeat interval. Its silence is useful telemetry, but cannot establish
// that the transport is dead. The venue's application-level PING/PONG is the
// liveness contract (the vendored SDK uses the same convention). A standard
// WebSocket control ping is sent alongside it so either supported heartbeat
// form proves transport health.
const PING_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);
// How long the *subscription* may deliver nothing before the connection is
// treated as dead, independent of transport health. See `StreamWatchdog`.
const STREAM_SILENCE_TIMEOUT: Duration = Duration::from_secs(60);
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(3);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

/// Shared execution gate for the live Activity WS. It is deliberately
/// transport-only: a disconnected feed must suspend planning/submission, but
/// it must not turn a temporary reconnect into an account-wide fuse. REST
/// backfill has its own audit health and never controls this gate.
#[derive(Clone, Debug, Default)]
pub struct WsExecutionGate {
    connected: Arc<AtomicBool>,
}

impl WsExecutionGate {
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn mark_connected(&self) {
        self.connected.store(true, Ordering::Release);
    }

    fn mark_disconnected(&self) {
        self.connected.store(false, Ordering::Release);
    }
}

/// Reset the backoff delay to its initial value. Exposed for testing so the
/// reset contract (a successful connection clears prior backoff) can be
/// asserted directly without running the full connection loop.
pub fn reset_backoff(delay: &mut Duration) {
    *delay = INITIAL_RECONNECT_DELAY;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeartbeatTick {
    SendPing,
    AwaitingPong,
    TimedOut,
}

/// Transport health is deliberately independent from activity-topic traffic:
/// a quiet leader must not make a healthy socket look stale.
#[derive(Debug, Default)]
struct ApplicationHeartbeat {
    awaiting_pong_since: Option<tokio::time::Instant>,
}

impl ApplicationHeartbeat {
    fn tick(&mut self, now: tokio::time::Instant) -> HeartbeatTick {
        match self.awaiting_pong_since {
            Some(sent_at) if now.duration_since(sent_at) >= HEARTBEAT_TIMEOUT => {
                HeartbeatTick::TimedOut
            }
            Some(_) => HeartbeatTick::AwaitingPong,
            None => {
                self.awaiting_pong_since = Some(now);
                HeartbeatTick::SendPing
            }
        }
    }

    fn observe_pong(&mut self) {
        self.awaiting_pong_since = None;
    }
}

/// Watches the *subscription*, where `ApplicationHeartbeat` watches the
/// *transport*. Both are needed because they fail independently.
///
/// They came apart in production on 2026-09-16: PING/PONG kept answering
/// normally on a connection whose activity subscription had silently stopped
/// delivering roughly fourteen minutes in. No reconnect condition existed for
/// that state, so the engine held the dead subscription for the rest of the
/// run. Every leader trade after it arrived only through REST backfill, and
/// backfill deliberately creates no intents -- so nothing was copied, and no
/// error was logged anywhere. Measured at the time: the socket took 288 bytes
/// in sixty seconds while a second client on the same host, same endpoint and
/// same subscribe frame took thousands of messages a minute.
///
/// The distinction an earlier revision got wrong by deleting this check: the
/// subscription is the venue's *global* activity firehose, not one leader's
/// tape. Measured on the engine host it carries on the order of 3,200
/// messages a minute across all of Polymarket. A watched leader being quiet
/// for hours is ordinary and must never trigger a reconnect; the firehose
/// itself being quiet for a minute is not ordinary, and means this connection
/// has stopped being useful. So the timer below is fed by every activity
/// message from any wallet, and never by watched-leader events -- which are
/// far too rare to serve as a liveness signal.
///
/// A false reconnect costs one connection setup (`INITIAL_RECONNECT_DELAY`);
/// a missed one costs every fill until someone notices by hand. The threshold
/// is set accordingly.
#[derive(Debug)]
struct StreamWatchdog {
    last_message_at: tokio::time::Instant,
}

impl StreamWatchdog {
    /// A fresh connection starts the clock, so a subscription that never
    /// delivers anything at all is caught by the same timer that catches one
    /// which stops delivering later.
    fn started_at(now: tokio::time::Instant) -> Self {
        Self {
            last_message_at: now,
        }
    }

    fn observe_message(&mut self, now: tokio::time::Instant) {
        self.last_message_at = now;
    }

    fn is_silent(&self, now: tokio::time::Instant) -> bool {
        now.duration_since(self.last_message_at) >= STREAM_SILENCE_TIMEOUT
    }
}

fn is_application_pong(text: &str) -> bool {
    text.trim().eq_ignore_ascii_case("pong")
}

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
    apply_trade(
        pool,
        resolver,
        &trade,
        "activity_ws",
        &transaction_hash,
        raw,
    )
    .await
}

/// One entry in the WS connection's lifecycle, for a caller to log in a
/// greppable, parseable form -- the same `PREFIX: {json}` convention
/// `ghost_verify`'s `GHOST_RECORD:` already uses. This project keeps no
/// durable table for these: they are operational telemetry about the
/// transport, not a business record like `leader_events`, so a caller that
/// wants them retained redirects stdout to a log file and parses
/// `WS_EVENT:` lines back out later (see `ingest_report`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WsConnectionEvent {
    pub at_utc: String,
    pub kind: WsConnectionEventKind,
    pub detail: String,
    /// Only set on a `Disconnected` event: how long `run` will wait before
    /// the next connection attempt.
    pub next_reconnect_delay_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WsConnectionEventKind {
    Connected,
    Disconnected,
}

pub const WS_EVENT_PREFIX: &str = "WS_EVENT: ";

fn log_ws_event(event: &WsConnectionEvent) {
    println!(
        "{WS_EVENT_PREFIX}{}",
        serde_json::to_string(event).unwrap_or_default()
    );
}

fn now_utc() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Runs the activity WebSocket connection forever, reconnecting with
/// exponential backoff (3s, 6s, 12s, 24s, capped at 60s, matching the
/// reference implementation) on any disconnect, error, or missed heartbeat.
/// The backoff resets to INITIAL_RECONNECT_DELAY on every successful
/// connection establishment (Connected event emitted). Never returns under
/// normal operation; only returns if `pool` itself becomes unusable in a way
/// a reconnect cannot fix.
pub async fn run(pool: SqlitePool, resolver: &AddressResolver) -> ! {
    run_with_execution_gate(pool, resolver, WsExecutionGate::default()).await
}

/// Like [`run`], but exposes whether the socket is subscribed and receiving
/// a live session to the execution loop. A false gate means "do not plan or
/// submit"; the connection manager continues its own bounded reconnect loop.
pub async fn run_with_execution_gate(
    pool: SqlitePool,
    resolver: &AddressResolver,
    execution_gate: WsExecutionGate,
) -> ! {
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;
    execution_gate.mark_disconnected();
    loop {
        match run_once(&pool, resolver, &mut reconnect_delay, &execution_gate).await {
            Err(error) => {
                execution_gate.mark_disconnected();
                eprintln!("activity ws: {error}, reconnecting in {reconnect_delay:?}");
                log_ws_event(&WsConnectionEvent {
                    at_utc: now_utc(),
                    kind: WsConnectionEventKind::Disconnected,
                    detail: error.to_string(),
                    next_reconnect_delay_ms: Some(reconnect_delay.as_millis() as u64),
                });
            }
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
///
/// On successful connection establishment (after the Connected event is
/// logged), `reconnect_delay` is reset to INITIAL_RECONNECT_DELAY so the
/// next failure starts the backoff from the beginning.
async fn run_once(
    pool: &SqlitePool,
    resolver: &AddressResolver,
    reconnect_delay: &mut Duration,
    execution_gate: &WsExecutionGate,
) -> Result<std::convert::Infallible, ActivityWsError> {
    ensure_crypto_provider_installed();

    let (ws_stream, _response) = tokio_tungstenite::connect_async(RTDS_URL)
        .await
        .map_err(|error| ActivityWsError::Connect(Box::new(error)))?;
    let (mut writer, mut reader) = ws_stream.split();

    writer
        .send(Message::Text(SUBSCRIBE_MESSAGE.into()))
        .await
        .map_err(|error| ActivityWsError::Send(Box::new(error)))?;
    log_ws_event(&WsConnectionEvent {
        at_utc: now_utc(),
        kind: WsConnectionEventKind::Connected,
        detail: String::new(),
        next_reconnect_delay_ms: None,
    });
    execution_gate.mark_connected();
    // Reset backoff on every successful connection establishment.
    // A future failure will start the exponential sequence from the beginning.
    *reconnect_delay = INITIAL_RECONNECT_DELAY;

    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    ping_interval.tick().await; // the first tick fires immediately; skip it.
    let mut heartbeat = ApplicationHeartbeat::default();
    let mut stream = StreamWatchdog::started_at(tokio::time::Instant::now());

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                // Checked before the heartbeat: a silent subscription is the
                // failure PING/PONG cannot see, so it must not be masked by a
                // transport that is answering perfectly.
                if stream.is_silent(tokio::time::Instant::now()) {
                    return Err(ActivityWsError::StreamSilent);
                }
                match heartbeat.tick(tokio::time::Instant::now()) {
                    HeartbeatTick::SendPing => {
                        writer
                            .send(Message::Text("PING".into()))
                            .await
                            .map_err(|error| ActivityWsError::Send(Box::new(error)))?;
                        writer
                            .send(Message::Ping(Vec::new()))
                            .await
                            .map_err(|error| ActivityWsError::Send(Box::new(error)))?;
                    }
                    HeartbeatTick::AwaitingPong => {}
                    HeartbeatTick::TimedOut => return Err(ActivityWsError::HeartbeatTimedOut),
                }
            }
            message = reader.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        if is_application_pong(&text) {
                            heartbeat.observe_pong();
                        } else {
                            // Any activity message, watched or not, proves the
                            // subscription is still delivering.
                            stream.observe_message(tokio::time::Instant::now());
                            let outcome = process_message(pool, resolver, &text).await;
                            if let ProcessOutcome::DatabaseError(_) = &outcome {
                                eprintln!("activity ws: {outcome}");
                            }
                        }
                    }
                    Some(Ok(Message::Pong(_))) => heartbeat.observe_pong(),
                    Some(Ok(Message::Ping(payload))) => writer
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|error| ActivityWsError::Send(Box::new(error)))?,
                    Some(Ok(Message::Close(_))) | None => return Err(ActivityWsError::ConnectionClosed),
                    // Binary frames carry no activity or heartbeat semantics.
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
    HeartbeatTimedOut,
    StreamSilent,
}

impl fmt::Display for ActivityWsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(source) => write!(formatter, "unable to connect: {source}"),
            Self::Send(source) => write!(formatter, "unable to send: {source}"),
            Self::Stream(source) => write!(formatter, "stream error: {source}"),
            Self::ConnectionClosed => write!(formatter, "connection closed"),
            Self::HeartbeatTimedOut => write!(
                formatter,
                "no application PONG received within {HEARTBEAT_TIMEOUT:?}"
            ),
            Self::StreamSilent => write!(
                formatter,
                "subscription delivered no activity message within \
                 {STREAM_SILENCE_TIMEOUT:?} while the transport stayed alive"
            ),
        }
    }
}

impl StdError for ActivityWsError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Connect(source) | Self::Send(source) | Self::Stream(source) => Some(source),
            Self::ConnectionClosed | Self::HeartbeatTimedOut | Self::StreamSilent => None,
        }
    }
}

#[cfg(all(test, feature = "ingest"))]
mod tests {
    use sqlx::Row as _;

    use super::*;
    use crate::copytrading::db::open_and_migrate;

    #[test]
    fn a_ws_connection_event_round_trips_through_the_ws_event_prefixed_json_line() {
        let event = WsConnectionEvent {
            at_utc: "2026-09-04T00:00:00.000Z".to_owned(),
            kind: WsConnectionEventKind::Disconnected,
            detail: "connection closed".to_owned(),
            next_reconnect_delay_ms: Some(3_000),
        };

        let json = serde_json::to_string(&event).expect("event must serialize");
        let restored: WsConnectionEvent =
            serde_json::from_str(&json).expect("event must deserialize");
        assert_eq!(restored, event);

        // The exact prefix a log-parsing tool must match on.
        let line = format!("{WS_EVENT_PREFIX}{json}");
        assert!(line.starts_with("WS_EVENT: {"));
    }

    // P2-3: pin the reconnect backoff reset contract. A successful
    // connection must clear the accumulated delay so the next failure
    // starts the exponential sequence from the beginning (3s), not from
    // a previously-doubled value.

    #[test]
    fn reconnect_backoff_resets_to_initial_on_success() {
        let mut delay = Duration::from_secs(24); // arbitrary doubled state
        reset_backoff(&mut delay);
        assert_eq!(delay, INITIAL_RECONNECT_DELAY);

        // Idempotent: resetting an already-initial delay keeps it initial.
        reset_backoff(&mut delay);
        assert_eq!(delay, INITIAL_RECONNECT_DELAY);
    }

    #[test]
    fn execution_gate_is_closed_until_a_subscription_is_live() {
        let gate = WsExecutionGate::default();
        assert!(!gate.is_connected());
        gate.mark_connected();
        assert!(gate.is_connected());
        gate.mark_disconnected();
        assert!(!gate.is_connected());
    }

    #[test]
    fn quiet_activity_with_timely_pongs_never_times_out() {
        let start = tokio::time::Instant::now();
        let mut heartbeat = ApplicationHeartbeat::default();

        // This models 45 seconds without a single activity-topic message.
        // Each matching PONG is sufficient transport evidence to keep the
        // realtime gate open; leader silence is not a disconnect.
        //
        // Transport evidence only. Whether the *subscription* is still
        // delivering is `StreamWatchdog`'s question, not this one -- see
        // `a_silent_subscription_reconnects_while_pongs_keep_arriving`.
        for elapsed in [0_u64, 10, 20, 30, 40] {
            assert_eq!(
                heartbeat.tick(start + Duration::from_secs(elapsed)),
                HeartbeatTick::SendPing
            );
            heartbeat.observe_pong();
        }
        assert_eq!(
            heartbeat.tick(start + Duration::from_secs(45)),
            HeartbeatTick::SendPing
        );
    }

    #[test]
    fn missing_pong_is_the_only_heartbeat_timeout() {
        let start = tokio::time::Instant::now();
        let mut heartbeat = ApplicationHeartbeat::default();

        assert_eq!(heartbeat.tick(start), HeartbeatTick::SendPing);
        assert_eq!(
            heartbeat.tick(start + HEARTBEAT_TIMEOUT - Duration::from_millis(1)),
            HeartbeatTick::AwaitingPong
        );
        assert_eq!(
            heartbeat.tick(start + HEARTBEAT_TIMEOUT),
            HeartbeatTick::TimedOut
        );
    }

    #[test]
    fn application_pong_is_case_insensitive_and_not_an_activity_message() {
        assert!(is_application_pong(" PONG "));
        assert!(is_application_pong("pong"));
        assert!(!is_application_pong("{\"topic\":\"activity\"}"));
    }

    #[test]
    fn a_silent_subscription_reconnects_while_pongs_keep_arriving() {
        // The exact production state of 2026-09-16: the transport answered
        // every heartbeat while the subscription delivered nothing. Before
        // the watchdog existed this combination had no reconnect condition
        // at all, so the engine sat on a dead subscription indefinitely and
        // copied nothing while looking healthy from every angle.
        let start = tokio::time::Instant::now();
        let mut heartbeat = ApplicationHeartbeat::default();
        let stream = StreamWatchdog::started_at(start);

        let mut elapsed = Duration::ZERO;
        while elapsed < STREAM_SILENCE_TIMEOUT {
            assert_eq!(heartbeat.tick(start + elapsed), HeartbeatTick::SendPing);
            heartbeat.observe_pong();
            assert!(
                !stream.is_silent(start + elapsed),
                "must not reconnect before the threshold, at {elapsed:?}"
            );
            elapsed += PING_INTERVAL;
        }

        // Transport still perfect at the threshold; the subscription is not.
        assert_eq!(
            heartbeat.tick(start + STREAM_SILENCE_TIMEOUT),
            HeartbeatTick::SendPing
        );
        assert!(stream.is_silent(start + STREAM_SILENCE_TIMEOUT));
    }

    #[test]
    fn an_activity_message_from_any_wallet_keeps_the_subscription_alive() {
        // The watchdog is fed by the global firehose, not by watched-leader
        // events. A leader can be quiet for hours; that must never reconnect.
        let start = tokio::time::Instant::now();
        let mut stream = StreamWatchdog::started_at(start);

        // Four hours of unwatched-wallet traffic at one message every thirty
        // seconds -- orders of magnitude below the real firehose rate, and
        // still never silent.
        const STEP: u64 = 30;
        let ticks = 4 * 60 * 60 / STEP;
        for tick in 1..=ticks {
            let now = start + Duration::from_secs(tick * STEP);
            assert!(!stream.is_silent(now), "silent at tick {tick}");
            stream.observe_message(now);
        }

        // The moment that traffic stops, the threshold applies again.
        let last = start + Duration::from_secs(ticks * STEP);
        assert!(!stream.is_silent(last + STREAM_SILENCE_TIMEOUT - Duration::from_millis(1)));
        assert!(stream.is_silent(last + STREAM_SILENCE_TIMEOUT));
    }

    #[test]
    fn a_subscription_that_never_delivers_anything_is_caught_too() {
        // A connection that subscribes and is answered with silence from the
        // first second is the same failure, and the clock starts at connect
        // so the same timer covers it.
        let start = tokio::time::Instant::now();
        let stream = StreamWatchdog::started_at(start);

        assert!(!stream.is_silent(start));
        assert!(stream.is_silent(start + STREAM_SILENCE_TIMEOUT));
    }

    #[test]
    fn stream_silence_reports_itself_distinctly_from_a_heartbeat_timeout() {
        // These two reach the same reconnect path, so the log line is the
        // only way an operator can tell which failure actually happened.
        let silent = ActivityWsError::StreamSilent.to_string();
        let heartbeat = ActivityWsError::HeartbeatTimedOut.to_string();

        assert_ne!(silent, heartbeat);
        assert!(silent.contains("no activity message"), "{silent}");
        assert!(silent.contains("transport stayed alive"), "{silent}");
    }

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

        let outcome = process_message(
            &db,
            &resolver,
            &trade_message("0xh1", "0xdeadbeef", 1735689600),
        )
        .await;
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

        let outcome = process_message(
            &db,
            &resolver,
            &trade_message("0xh1", "0xleader", 1735689600),
        )
        .await;
        assert_eq!(outcome, ProcessOutcome::LeaderNotActivated);
    }

    #[tokio::test]
    async fn a_trade_before_activation_is_rejected_even_though_the_leader_is_activated() {
        let db = TestDb::new().await;
        seed_activated_leader(&db, 1, "2026-01-01T00:00:00.000Z").await;
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        // 1735689600 is 2025-01-01T00:00:00Z -- before the 2026 activation.
        let outcome = process_message(
            &db,
            &resolver,
            &trade_message("0xh1", "0xleader", 1735689600),
        )
        .await;
        assert_eq!(outcome, ProcessOutcome::BeforeActivation);
    }

    #[tokio::test]
    async fn a_watched_activated_leader_trade_is_ingested_into_both_tables() {
        let db = TestDb::new().await;
        seed_activated_leader(&db, 1, "2020-01-01T00:00:00.000Z").await;
        let resolver = AddressResolver::new();
        resolver.reload([("0xleader".to_owned(), 1)]);

        let outcome = process_message(
            &db,
            &resolver,
            &trade_message("0xh1", "0xleader", 1735689600),
        )
        .await;
        assert_eq!(
            outcome,
            ProcessOutcome::Ingested {
                leader_id: 1,
                // No timestamp: the key identifies the fill, not the
                // observation of it. See apply.rs's canonical_event_key.
                canonical_event_key: "activity:0xh1:0xleader:0xcond:123:0:BUY:0.5:5".to_owned()
            }
        );

        let event_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_events WHERE canonical_event_key = 'activity:0xh1:0xleader:0xcond:123:0:BUY:0.5:5'")
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
    async fn replaying_the_same_message_twice_ingests_once_and_the_second_call_still_reports_ingested(
    ) {
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
        let orders_matched_message =
            trades_message.replace("\"type\":\"trades\"", "\"type\":\"orders_matched\"");

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
