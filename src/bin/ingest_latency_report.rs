//! Summarizes one `ingest_observe` window: WS-vs-REST observation latency,
//! dedup/race outcome, and WS connection health. Strictly read-only --
//! opens the database with `copytrading::open_read_only` (never
//! `open_and_migrate`, whose own migration bookkeeping can itself write)
//! and never contacts the venue.
//!
//! ```text
//! Usage: ingest_latency_report <db-path> <log-file> [leader-id]
//! ```
//!
//! `log-file` is the operator's own captured stdout/journal from the
//! `ingest_observe` run: required, not optional. It supplies two things a
//! trustworthy report cannot do without: the `OBSERVE_EVENT:` start/stop
//! window this report scopes its database query to (without it, the query
//! would summarize the database's *entire* history, silently mixing in any
//! earlier run's leftover events), and the `WS_EVENT:` lines used for
//! connection-health stats. `leader-id` is optional: omit it to report
//! across every leader active in the window.
//!
//! Exit code 0: a complete, in-window report with no backfill failures and
//! no unparseable log lines. Exit code 3: otherwise -- including simply
//! running without a usable window, which this binary still does (for
//! debugging) but always flags as untrustworthy.

#[cfg(feature = "ingest")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::{fs, process};

    use polycopy_engine::copytrading::{
        ingest::{
            build_connection_health_report, build_report, parse_observe_window,
            query_observation_rows,
        },
        open_read_only,
    };

    let mut args = std::env::args().skip(1);
    let (Some(db_path), Some(log_path)) = (args.next(), args.next()) else {
        eprintln!("Usage: ingest_latency_report <db-path> <log-file> [leader-id]");
        process::exit(2);
    };
    let leader_id: Option<i64> = match args.next() {
        Some(raw) => match raw.parse() {
            Ok(value) => Some(value),
            Err(_) => {
                eprintln!("leader-id must be an integer, got: {raw}");
                process::exit(2);
            }
        },
        None => None,
    };

    let result: Result<bool, String> = async {
        let log = fs::read_to_string(&log_path)
            .map_err(|error| format!("unable to read log file {log_path}: {error}"))?;
        let window = parse_observe_window(&log);

        println!("=== observation window ===");
        println!(
            "started: {}   stopped: {}",
            window.started_at.as_deref().unwrap_or("(not found in log)"),
            window.stopped_at.as_deref().unwrap_or("(not found in log)")
        );
        if window.unparseable_lines > 0 {
            println!(
                "WARNING: {} OBSERVE_EVENT line(s) failed to parse",
                window.unparseable_lines
            );
        }
        let query_window = window.as_query_window();
        if window.ambiguous_multiple_runs {
            println!(
                "WARNING: this log contains more than one Started or Stopped OBSERVE_EVENT -- it \
                 looks like two or more ingest_observe runs got concatenated into one file. \
                 Refusing to guess a combined window (which would silently span both runs and \
                 the gap between them); pass a log from exactly one run instead."
            );
        } else if query_window.is_none() {
            println!(
                "WARNING: no complete start/stop window found in this log -- refusing to query \
                 the database, since that would summarize its ENTIRE history for the leader \
                 filter rather than just this run, possibly contaminated by earlier runs' \
                 leftover events. Re-run ingest_observe (it always emits OBSERVE_EVENT \
                 start/stop) before trusting a report."
            );
        }

        // Fail closed on an untrustworthy window: do not open the database,
        // do not query, do not print any historical stats. A caller that
        // ignores the exit code must not be able to read numbers computed
        // over the wrong (or entire) history.
        let window_usable = window.unparseable_lines == 0 && query_window.is_some();
        if !window_usable {
            println!(
                "refusing to open the database or print any statistics -- the observation \
                 window above is not trustworthy (see WARNINGs)"
            );
            return Ok(false);
        }

        let mut complete = true;
        if window.backfill_failure_count > 0 {
            println!(
                "WARNING: {} REST backfill failure(s) during this window -- REST-only/dedup \
                 conclusions below may be incomplete, not just slow",
                window.backfill_failure_count
            );
            complete = false;
        }

        let pool = open_read_only(&db_path)
            .await
            .map_err(|error| error.to_string())?;
        let rows = query_observation_rows(&pool, leader_id, query_window)
            .await
            .map_err(|error| error.to_string())?;
        let report = build_report(&rows);

        println!("=== ingestion latency report ===");
        println!(
            "leader filter: {}",
            leader_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "(all)".to_owned())
        );
        println!("total canonical events: {}", report.total_events);
        println!(
            "observed by both WS and REST (dedup working): {}",
            report.both_observed_count
        );
        println!(
            "  WS arrived first: {}   REST arrived first: {}",
            report.ws_won_race_count, report.rest_won_race_count
        );
        println!(
            "WS-only: {}   REST-only: {}   other source only: {}",
            report.ws_only_count, report.rest_only_count, report.other_source_only_count
        );
        println!(
            "WS latency (observed_at - occurred_at), ms: count={} min={:?} max={:?} avg={:?}",
            report.ws_stats.count,
            report.ws_stats.min_latency_ms,
            report.ws_stats.max_latency_ms,
            report.ws_stats.avg_latency_ms
        );
        println!(
            "REST latency (observed_at - occurred_at), ms: count={} min={:?} max={:?} avg={:?}",
            report.rest_stats.count,
            report.rest_stats.min_latency_ms,
            report.rest_stats.max_latency_ms,
            report.rest_stats.avg_latency_ms
        );

        let health = build_connection_health_report(&log, window.stopped_at.as_deref());
        println!("=== WS connection health ===");
        println!(
            "connected events: {}   disconnected events: {}",
            health.connected_events, health.disconnected_events
        );
        println!(
            "total connected time: {}ms (still connected when the window ended: {})",
            health.total_connected_ms, health.still_connected_at_end
        );
        println!("longest downtime: {:?}ms", health.longest_downtime_ms);
        if health.unparseable_lines > 0 {
            println!(
                "WARNING: {} WS_EVENT line(s) failed to parse",
                health.unparseable_lines
            );
            complete = false;
        }

        Ok(complete)
    }
    .await;

    match result {
        Ok(true) => {
            println!("report is complete: one window, no backfill failures, no parse errors")
        }
        Ok(false) => {
            eprintln!("report is NOT complete -- see WARNINGs above before trusting it");
            process::exit(3);
        }
        Err(error) => {
            eprintln!("ingest_latency_report failed: {error}");
            process::exit(3);
        }
    }
}

#[cfg(not(feature = "ingest"))]
fn main() {
    eprintln!("ingest_latency_report requires the ingest feature");
    std::process::exit(2);
}
