//! Aggregates a log of `ghost_verify`'s `GHOST_RECORD:` lines into one
//! summary for Phase 7's multi-day GHOST run
//! (`docs/COPY_ENGINE_BLUEPRINT.md` section 12, required test 7: "a 12-hour
//! GHOST run that reconciles ledger, intent, and strict venue reads without
//! unexplained event loss").
//!
//! This module never contacts the venue, never opens a database, and never
//! reads a credential. It only parses text a wrapper script already
//! produced by running the existing `ghost_verify` binary repeatedly (the
//! operator's own choice of scheduler -- cron, a systemd timer, or a plain
//! loop -- is out of scope here) and redirecting its output to a log file.

use chrono::{DateTime, Utc};

use crate::ghost::GhostRunRecord;

pub const GHOST_RECORD_PREFIX: &str = "GHOST_RECORD: ";
/// The Phase 7 observation window must cover at least twelve hours. This is
/// independent of the per-run gap tolerance.
pub const MIN_GHOST_WINDOW_SECONDS: i64 = 12 * 60 * 60;

/// One gap between two consecutive parseable runs wider than the caller's
/// tolerance -- a candidate for "unexplained event loss": either the
/// scheduler missed a run, or the process producing the log was down.
#[derive(Debug, Clone, PartialEq)]
pub struct DriftGap {
    pub after_checked_at_utc: String,
    pub before_checked_at_utc: String,
    pub gap_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DriftReport {
    pub total_runs: usize,
    pub clean_runs: usize,
    pub unclean_runs: Vec<GhostRunRecord>,
    pub gaps: Vec<DriftGap>,
    /// A line contained the `GHOST_RECORD:` prefix but did not parse as
    /// valid JSON -- a real problem with the log, never silently dropped.
    pub unparseable_record_lines: usize,
    pub first_checked_at_utc: Option<String>,
    pub last_checked_at_utc: Option<String>,
}

impl DriftReport {
    /// `true` only when every run was clean, no gap exceeded the caller's
    /// tolerance, and no record line failed to parse. Mirrors
    /// `GhostVerification::is_clean`'s "only a genuinely clean result counts"
    /// discipline at the level of a whole multi-day run.
    pub fn is_clean(&self) -> bool {
        self.total_runs > 0
            && self.unclean_runs.is_empty()
            && self.gaps.is_empty()
            && self.unparseable_record_lines == 0
    }

    /// Elapsed time between the first and last parseable record, if both are
    /// available. A clean but shorter window is evidence, not a Phase 7 pass.
    pub fn observed_window_seconds(&self) -> Option<i64> {
        let first = DateTime::parse_from_rfc3339(self.first_checked_at_utc.as_deref()?)
            .ok()?
            .with_timezone(&Utc);
        let last = DateTime::parse_from_rfc3339(self.last_checked_at_utc.as_deref()?)
            .ok()?
            .with_timezone(&Utc);
        Some((last - first).num_seconds())
    }

    pub fn meets_minimum_window(&self) -> bool {
        self.observed_window_seconds()
            .is_some_and(|seconds| seconds >= MIN_GHOST_WINDOW_SECONDS)
    }
}

/// Extracts every `GHOST_RECORD:` line from `log`, in file order, without
/// attempting to parse or sort them yet.
fn extract_record_lines(log: &str) -> impl Iterator<Item = &str> {
    log.lines()
        .filter_map(|line| line.trim().strip_prefix(GHOST_RECORD_PREFIX))
}

/// Builds a drift report from a raw log (the concatenated stdout of many
/// `ghost_verify` runs). `max_gap_seconds` is the longest acceptable time
/// between two consecutive runs' `checked_at_utc` before it counts as a
/// gap; the blueprint does not fix this number, since it depends on the
/// operator's own chosen run cadence.
pub fn build_drift_report(log: &str, max_gap_seconds: i64) -> DriftReport {
    let mut records = Vec::new();
    let mut unparseable_record_lines = 0usize;

    for raw in extract_record_lines(log) {
        match serde_json::from_str::<GhostRunRecord>(raw.trim()) {
            Ok(record) => records.push(record),
            Err(_) => unparseable_record_lines += 1,
        }
    }

    let mut timestamped: Vec<(DateTime<Utc>, GhostRunRecord)> = records
        .into_iter()
        .filter_map(|record| {
            DateTime::parse_from_rfc3339(&record.checked_at_utc)
                .ok()
                .map(|timestamp| (timestamp.with_timezone(&Utc), record))
        })
        .collect();
    timestamped.sort_by_key(|(timestamp, _)| *timestamp);

    let total_runs = timestamped.len();
    let clean_runs = timestamped
        .iter()
        .filter(|(_, record)| record.is_clean)
        .count();
    let unclean_runs: Vec<GhostRunRecord> = timestamped
        .iter()
        .filter(|(_, record)| !record.is_clean)
        .map(|(_, record)| record.clone())
        .collect();

    let mut gaps = Vec::new();
    for window in timestamped.windows(2) {
        let [(before_ts, before_record), (after_ts, after_record)] = window else {
            continue;
        };
        let gap_seconds = (*after_ts - *before_ts).num_seconds();
        if gap_seconds > max_gap_seconds {
            gaps.push(DriftGap {
                after_checked_at_utc: before_record.checked_at_utc.clone(),
                before_checked_at_utc: after_record.checked_at_utc.clone(),
                gap_seconds,
            });
        }
    }

    DriftReport {
        total_runs,
        clean_runs,
        unclean_runs,
        gaps,
        unparseable_record_lines,
        first_checked_at_utc: timestamped
            .first()
            .map(|(_, record)| record.checked_at_utc.clone()),
        last_checked_at_utc: timestamped
            .last()
            .map(|(_, record)| record.checked_at_utc.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_line(checked_at_utc: &str, is_clean: bool) -> String {
        format!(
            r#"GHOST_RECORD: {{"snapshot_at_utc":"2026-09-01T00:00:00Z","checked_at_utc":"{checked_at_utc}","is_clean":{is_clean},"collateral":{{"status":"match","expected":"1","observed":"1","error":null}},"token_balances":[]}}"#
        )
    }

    #[test]
    fn ignores_the_human_readable_lines_around_each_record() {
        let log = format!(
            "== ghost run ==\ncollateral: match\n{}\nGHOST verification is clean; this does not authorize automated trading.\n",
            record_line("2026-09-01T00:00:00Z", true)
        );
        let report = build_drift_report(&log, 3600);
        assert_eq!(report.total_runs, 1);
        assert_eq!(report.unparseable_record_lines, 0);
    }

    #[test]
    fn a_clean_run_of_regularly_spaced_records_is_clean() {
        let log = [
            record_line("2026-09-01T00:00:00Z", true),
            record_line("2026-09-01T00:10:00Z", true),
            record_line("2026-09-01T00:20:00Z", true),
        ]
        .join("\n");

        let report = build_drift_report(&log, 900);
        assert!(report.is_clean());
        assert_eq!(report.total_runs, 3);
        assert_eq!(report.clean_runs, 3);
        assert!(report.gaps.is_empty());
        assert_eq!(
            report.first_checked_at_utc.as_deref(),
            Some("2026-09-01T00:00:00Z")
        );
        assert_eq!(
            report.last_checked_at_utc.as_deref(),
            Some("2026-09-01T00:20:00Z")
        );
    }

    #[test]
    fn an_unclean_run_is_never_hidden_by_surrounding_clean_ones() {
        let log = [
            record_line("2026-09-01T00:00:00Z", true),
            record_line("2026-09-01T00:10:00Z", false),
            record_line("2026-09-01T00:20:00Z", true),
        ]
        .join("\n");

        let report = build_drift_report(&log, 900);
        assert!(!report.is_clean());
        assert_eq!(report.unclean_runs.len(), 1);
        assert_eq!(
            report.unclean_runs[0].checked_at_utc,
            "2026-09-01T00:10:00Z"
        );
    }

    #[test]
    fn a_wide_gap_between_two_clean_runs_is_still_flagged() {
        // Every individual run reported clean, but a 3-hour silent gap in
        // the middle is exactly the "unexplained event loss" this report
        // exists to catch -- clean runs on either side must not hide it.
        let log = [
            record_line("2026-09-01T00:00:00Z", true),
            record_line("2026-09-01T03:00:00Z", true),
        ]
        .join("\n");

        let report = build_drift_report(&log, 900);
        assert!(!report.is_clean());
        assert_eq!(report.gaps.len(), 1);
        assert_eq!(report.gaps[0].gap_seconds, 3 * 3600);
    }

    #[test]
    fn a_malformed_record_line_is_counted_never_silently_dropped() {
        let log = format!(
            "{}\nGHOST_RECORD: {{not valid json\n",
            record_line("2026-09-01T00:00:00Z", true)
        );
        let report = build_drift_report(&log, 900);
        assert_eq!(report.total_runs, 1);
        assert_eq!(report.unparseable_record_lines, 1);
        assert!(
            !report.is_clean(),
            "a malformed line must keep the whole report unclean"
        );
    }

    #[test]
    fn records_out_of_file_order_are_still_gap_checked_in_time_order() {
        let log = [
            record_line("2026-09-01T00:20:00Z", true),
            record_line("2026-09-01T00:00:00Z", true),
            record_line("2026-09-01T00:10:00Z", true),
        ]
        .join("\n");

        let report = build_drift_report(&log, 900);
        assert!(
            report.is_clean(),
            "sorted by time, the three runs are evenly spaced"
        );
    }

    #[test]
    fn an_empty_log_is_not_reported_clean() {
        // Phase 7 needs positive evidence of 12 hours of runs, not the
        // absence of bad news -- an empty log must never read as "clean."
        let report = build_drift_report("", 900);
        assert!(!report.is_clean());
        assert_eq!(report.total_runs, 0);
    }

    #[test]
    fn the_phase_seven_window_requires_a_full_twelve_hours() {
        let short = [
            record_line("2026-09-01T00:00:00Z", true),
            record_line("2026-09-01T11:59:59Z", true),
        ]
        .join("\n");
        let full = [
            record_line("2026-09-01T00:00:00Z", true),
            record_line("2026-09-01T12:00:00Z", true),
        ]
        .join("\n");

        assert!(!build_drift_report(&short, 13 * 60 * 60).meets_minimum_window());
        assert!(build_drift_report(&full, 13 * 60 * 60).meets_minimum_window());
    }
}
