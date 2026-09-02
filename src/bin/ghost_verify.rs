//! Authenticated, read-only Phase 0 GHOST verification command.

#[cfg(feature = "intl_clob")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use polycopy_engine::ghost_run::{run_ghost_verification, GhostRunConfig};

    let result: Result<(), String> = async {
        let config = GhostRunConfig::from_env().map_err(|error| error.to_string())?;
        let report = run_ghost_verification(&config)
            .await
            .map_err(|error| error.to_string())?;

        println!("GHOST snapshot timestamp: {}", config.snapshot_at_utc());
        println!("collateral: {}", status_name(report.collateral()));
        for (index, token_balance) in report.token_balances().iter().enumerate() {
            println!(
                "outcome token #{}: {}",
                index + 1,
                status_name(token_balance.result())
            );
        }

        // One structured, greppable line per run. A Phase 7 12-hour GHOST
        // run is many separate invocations of this binary (e.g. one every
        // few minutes via cron/systemd timer, the operator's own choice of
        // scheduler -- this binary itself still runs exactly once and
        // exits, same as always); the operator's wrapper appends this line
        // to a log, and `ghost_drift_report` reads that log afterward to
        // check the whole window for any mismatch, query failure, or gap
        // wide enough to represent unexplained event loss. Contains only
        // comparison results, never a credential or raw response body.
        let record =
            polycopy_engine::ghost_to_record(&report, config.snapshot_at_utc(), &now_utc());
        println!(
            "GHOST_RECORD: {}",
            serde_json::to_string(&record).unwrap_or_default()
        );

        if report.is_clean() {
            println!("GHOST verification is clean; this does not authorize automated trading.");
            Ok(())
        } else {
            Err("GHOST verification is unclean; reconciliation is required".to_owned())
        }
    }
    .await;

    if let Err(error) = result {
        eprintln!("GHOST verification failed: {error}");
        std::process::exit(3);
    }
}

#[cfg(feature = "intl_clob")]
fn now_utc() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::SecondsFormat;

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after the Unix epoch")
        .as_secs();
    polymarket_client_sdk_v2::types::DateTime::from_timestamp(secs as i64, 0)
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_default()
}

#[cfg(feature = "intl_clob")]
fn status_name<E>(result: &polycopy_engine::BalanceVerification<E>) -> &'static str {
    match result {
        polycopy_engine::BalanceVerification::Match { .. } => "match",
        polycopy_engine::BalanceVerification::Mismatch { .. } => "mismatch",
        polycopy_engine::BalanceVerification::QueryFailed { .. } => "query_failed",
    }
}

#[cfg(not(feature = "intl_clob"))]
fn main() {
    eprintln!("ghost_verify requires the intl_clob feature");
    std::process::exit(2);
}
