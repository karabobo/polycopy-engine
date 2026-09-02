//! Summarizes a log of `ghost_verify`'s `GHOST_RECORD:` lines for Phase 7's
//! 12-hour GHOST run. Reads only a local log file; never contacts the venue,
//! never opens a database, never reads a credential.
//!
//! ```text
//! Usage: ghost_drift_report <log-file> [max-gap-seconds]
//! ```
//!
//! `max-gap-seconds` (default 1200 = 20 minutes) is the longest acceptable
//! time between two consecutive runs' `checked_at_utc` before it counts as
//! a gap; pick this to comfortably exceed the operator's own chosen run
//! cadence (e.g. 4x a 5-minute cron interval), not the blueprint's 12-hour
//! window itself.
//!
//! Exit code 0: every run in the log was clean, with no gap and no
//! malformed record line. Exit code 3: otherwise -- matches
//! `ghost_verify`/`canary_probe`'s existing "3 means reconciliation
//! required" convention.

#[cfg(feature = "intl_clob")]
fn main() {
    use std::{fs, process};

    use polycopy_engine::{build_drift_report, MIN_GHOST_WINDOW_SECONDS};

    let mut args = std::env::args().skip(1);
    let Some(log_path) = args.next() else {
        eprintln!("Usage: ghost_drift_report <log-file> [max-gap-seconds]");
        process::exit(2);
    };
    let max_gap_seconds: i64 = match args.next() {
        Some(raw) => match raw.parse() {
            Ok(value) => value,
            Err(_) => {
                eprintln!("max-gap-seconds must be a positive integer, got: {raw}");
                process::exit(2);
            }
        },
        None => 1200,
    };

    let log = match fs::read_to_string(&log_path) {
        Ok(contents) => contents,
        Err(error) => {
            eprintln!("unable to read {log_path}: {error}");
            process::exit(2);
        }
    };

    let report = build_drift_report(&log, max_gap_seconds);

    println!("total runs: {}", report.total_runs);
    println!("clean runs: {}", report.clean_runs);
    println!(
        "window: {} .. {}",
        report.first_checked_at_utc.as_deref().unwrap_or("(none)"),
        report.last_checked_at_utc.as_deref().unwrap_or("(none)")
    );
    match report.observed_window_seconds() {
        Some(seconds) => {
            println!("window duration: {seconds}s (minimum required: {MIN_GHOST_WINDOW_SECONDS}s)")
        }
        None => {
            println!("window duration: unavailable (minimum required: {MIN_GHOST_WINDOW_SECONDS}s)")
        }
    }
    if report.unparseable_record_lines > 0 {
        println!(
            "WARNING: {} GHOST_RECORD line(s) failed to parse as JSON -- the log itself may be truncated or corrupted",
            report.unparseable_record_lines
        );
    }
    for record in &report.unclean_runs {
        println!(
            "UNCLEAN at {}: collateral={:?} token_balances={:?}",
            record.checked_at_utc, record.collateral.status, record.token_balances
        );
    }
    for gap in &report.gaps {
        println!(
            "GAP: {} .. {} ({} seconds, exceeds the {max_gap_seconds}s tolerance)",
            gap.after_checked_at_utc, gap.before_checked_at_utc, gap.gap_seconds
        );
    }

    if report.is_clean() && report.meets_minimum_window() {
        println!(
            "clean: {} run(s) covering the window above, no gap, no malformed record",
            report.total_runs
        );
    } else {
        if report.is_clean() {
            eprintln!(
                "GHOST drift report is clean but has not yet covered the required 12-hour window"
            );
        }
        eprintln!("GHOST drift report is not clean; see above");
        process::exit(3);
    }
}

#[cfg(not(feature = "intl_clob"))]
fn main() {
    eprintln!("ghost_drift_report requires the intl_clob feature");
    std::process::exit(2);
}
