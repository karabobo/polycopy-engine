//! Bounded, no-order-capable observation of the activity ingestion
//! pipeline (WS firehose + REST backfill) for a fixed duration. Written to
//! answer one question safely: for a leader's real trades, how does
//! WS-observed latency (`observed_at - occurred_at`) compare to REST, and
//! does the WS connection stay healthy.
//!
//! **This binary cannot place an order.** It imports only
//! `copytrading::{open_and_migrate}` and `copytrading::ingest::*` --
//! nothing from `plan`, `execute`, `reconcile`, or `orchestrate`, and
//! nothing from `venue::intl_clob_exec` or `venue::signed_order`. There is
//! no `submit`-shaped function anywhere in this file's dependency graph;
//! this is an architectural guarantee, not a runtime flag someone could
//! misconfigure.
//!
//! It is **not** a read-only tool, and does not claim to be one: exactly
//! like `copy_run`'s own ingestion path, it durably writes to
//! `leader_events` and `leader_event_observations` -- that write is the
//! whole point, since those rows are what `ingest_latency_report` later
//! reads. "No order" and "read-only" are different properties; this binary
//! only promises the first. Reuses whatever leader(s) are already
//! configured `enabled = 1` in the target database (e.g. via `copy_setup`)
//! -- it does not create or change any configuration itself.
//!
//! ```text
//! Usage:
//!   POLYCOPY_DB_PATH=...       (required) existing, already-configured database
//!   POLYCOPY_OBSERVE_SECONDS=... (required, must be > 0) how long to observe;
//!                                no default, the operator must consciously
//!                                choose a window long enough to see real activity
//!   POLYCOPY_BACKFILL_EVERY_SECONDS=60  (optional, default 60, must be > 0)
//! ```
//!
//! Emits `OBSERVE_EVENT:`/`WS_EVENT:` lines to stdout throughout the run;
//! redirect them to a file and pass that file to `ingest_latency_report`
//! afterward -- it needs this run's exact start/stop window to scope its
//! query and cannot produce a trustworthy report without it.

#[cfg(feature = "ingest")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::{process, sync::Arc, time::Duration};

    use chrono::{SecondsFormat, Utc};
    use polycopy_engine::copytrading::{
        ingest::{
            activity_ws, backfill_leader, AddressResolver, ObserveEvent, ObserveEventKind,
            OBSERVE_EVENT_PREFIX,
        },
        open_and_migrate,
    };
    use sqlx::SqlitePool;

    fn now_utc() -> String {
        Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
    }

    fn log_observe_event(kind: ObserveEventKind, detail: impl Into<String>) {
        let event = ObserveEvent { at_utc: now_utc(), kind, detail: detail.into() };
        println!("{OBSERVE_EVENT_PREFIX}{}", serde_json::to_string(&event).unwrap_or_default());
    }

    let result: Result<(), String> = async {
        let db_path = required("POLYCOPY_DB_PATH")?;
        let observe_seconds: u64 = required("POLYCOPY_OBSERVE_SECONDS")?
            .parse()
            .map_err(|_| "POLYCOPY_OBSERVE_SECONDS must be a positive integer".to_owned())?;
        if observe_seconds == 0 {
            return Err("POLYCOPY_OBSERVE_SECONDS must be greater than 0".to_owned());
        }
        let backfill_every_seconds: u64 = match std::env::var("POLYCOPY_BACKFILL_EVERY_SECONDS") {
            Ok(raw) if !raw.trim().is_empty() => raw
                .parse()
                .map_err(|_| "POLYCOPY_BACKFILL_EVERY_SECONDS must be a positive integer".to_owned())?,
            _ => 60,
        };
        if backfill_every_seconds == 0 {
            // A zero delay would spin the backfill loop with no pause
            // between REST calls -- a busy loop, not a poll.
            return Err("POLYCOPY_BACKFILL_EVERY_SECONDS must be greater than 0".to_owned());
        }

        let pool = open_and_migrate(&db_path).await.map_err(|error| error.to_string())?;
        let resolver = Arc::new(AddressResolver::new());
        resolver.reload_from_db(&pool).await.map_err(|error| error.to_string())?;

        let enabled: Vec<(i64, String)> = sqlx::query_as(
            "SELECT leader_id, address FROM leader_wallet_aliases WHERE enabled = 1",
        )
        .fetch_all(&pool)
        .await
        .map_err(|error| error.to_string())?;
        if enabled.is_empty() {
            return Err(
                "no enabled leader_wallet_aliases in this database -- run copy_setup first"
                    .to_owned(),
            );
        }
        println!(
            "observing {} enabled leader(s) for {observe_seconds}s (backfill every {backfill_every_seconds}s); no order can be placed by this binary",
            enabled.len()
        );
        log_observe_event(ObserveEventKind::Started, format!("{} leader(s)", enabled.len()));

        let ws_pool = pool.clone();
        let ws_resolver = resolver.clone();
        let websocket = activity_ws::run(ws_pool, ws_resolver.as_ref());

        let backfill_pool = pool.clone();
        let backfill_resolver = resolver.clone();
        let backfill_client = polymarket_client_sdk_v2::data::Client::new(
            "https://data-api.polymarket.com",
        )
        .map_err(|error| error.to_string())?;
        // Spawned, not a select! arm: backfill_loop never returns (a
        // failure is now a logged ObserveEvent, not an early exit), so it
        // must run concurrently in the background rather than being polled
        // as a branch that could otherwise never be reached.
        let backfill_task = tokio::spawn(backfill_loop(
            backfill_pool,
            backfill_resolver,
            backfill_client,
            enabled,
            Duration::from_secs(backfill_every_seconds),
        ));

        let stop_reason = tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(observe_seconds)) => {
                "observation window elapsed"
            }
            _ = websocket => {
                // activity_ws::run's return type is `!`: this branch can
                // never actually complete, but tokio::select! still needs a
                // syntactically valid arm for it.
                unreachable!("activity_ws::run never returns")
            }
        };
        println!("{stop_reason}; stopping (no order was placed)");
        backfill_task.abort();

        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM leader_events")
            .fetch_one(&pool)
            .await
            .map_err(|error| error.to_string())?;
        println!("total leader_events in database at end of window: {event_count}");
        log_observe_event(ObserveEventKind::Stopped, stop_reason);
        println!(
            "next: run ingest_latency_report against this database and this run's captured log \
             to see WS-vs-REST latency, dedup results, and connection health"
        );

        Ok(())
    }
    .await;

    async fn backfill_loop(
        pool: SqlitePool,
        resolver: Arc<AddressResolver>,
        client: polymarket_client_sdk_v2::data::Client,
        enabled: Vec<(i64, String)>,
        every: Duration,
    ) {
        use polycopy_engine::copytrading::ingest::{ObserveEvent, ObserveEventKind, OBSERVE_EVENT_PREFIX};

        loop {
            for (leader_id, address) in &enabled {
                match backfill_leader(&pool, resolver.as_ref(), &client, *leader_id, address).await {
                    Ok(summary) => println!(
                        "backfill leader {leader_id}: fetched={} ingested={} rejected={}",
                        summary.fetched, summary.ingested, summary.rejected
                    ),
                    Err(error) => {
                        eprintln!("backfill leader {leader_id} failed: {error}");
                        let event = ObserveEvent {
                            at_utc: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            kind: ObserveEventKind::BackfillFailure,
                            detail: format!("leader {leader_id}: {error}"),
                        };
                        println!("{OBSERVE_EVENT_PREFIX}{}", serde_json::to_string(&event).unwrap_or_default());
                    }
                }
            }
            tokio::time::sleep(every).await;
        }
    }

    if let Err(error) = result {
        eprintln!("ingest_observe failed: {error}");
        process::exit(3);
    }
}

#[cfg(feature = "ingest")]
fn required(name: &'static str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

#[cfg(not(feature = "ingest"))]
fn main() {
    eprintln!("ingest_observe requires the ingest feature");
    std::process::exit(2);
}
