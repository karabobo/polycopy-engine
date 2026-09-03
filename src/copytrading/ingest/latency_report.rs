//! Read-only analysis of ingestion latency and WS/REST dedup, built to
//! answer exactly the question a controlled `ingest_observe` run exists
//! for: for a leader's real trades, how does WS-observed latency
//! (`observed_at - occurred_at`) compare to REST's, did both sources
//! observe the same trade (dedup working), and how healthy was the WS
//! connection during the window. This module only issues `SELECT`
//! queries and only reads log text; the report binary that calls it must
//! still open the database with `copytrading::open_read_only`, never
//! `open_and_migrate`, so the connection itself cannot write (see
//! `db::open_read_only`'s doc for why `open_and_migrate`'s own migration
//! bookkeeping is a write this analysis must never risk).
//!
//! A report is only trustworthy for one observation window, not a
//! database's entire history: an earlier run's events are still sitting in
//! `leader_events`/`leader_event_observations`, and a report with no window
//! would silently mix them in. `ObserveEvent`/`OBSERVE_EVENT_PREFIX` (the
//! `ingest_observe` binary emits these) exist to mark exactly one run's
//! start and stop so `query_observation_rows` can be scoped to it.

use std::collections::BTreeMap;

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use super::activity_ws::{WsConnectionEvent, WsConnectionEventKind, WS_EVENT_PREFIX};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceLatencyStats {
    pub count: usize,
    pub min_latency_ms: Option<i64>,
    pub max_latency_ms: Option<i64>,
    pub avg_latency_ms: Option<f64>,
}

impl SourceLatencyStats {
    fn from_samples(samples: &[i64]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        let sum: i64 = samples.iter().sum();
        Self {
            count: samples.len(),
            min_latency_ms: samples.iter().copied().min(),
            max_latency_ms: samples.iter().copied().max(),
            avg_latency_ms: Some(sum as f64 / samples.len() as f64),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct LatencyReport {
    pub total_events: usize,
    pub ws_stats: SourceLatencyStats,
    pub rest_stats: SourceLatencyStats,
    /// Both `activity_ws` and `activity_backfill` independently observed
    /// this canonical event -- dedup absorbed the overlap into one
    /// `leader_events` row, exactly as designed.
    pub both_observed_count: usize,
    pub ws_only_count: usize,
    pub rest_only_count: usize,
    /// Recognized neither `activity_ws` nor `activity_backfill` as a
    /// source (e.g. `onchain_ws`, or a source added later this report
    /// doesn't yet know about) -- counted, never silently dropped.
    pub other_source_only_count: usize,
    /// Of the events both sources observed, how many did WS observe first
    /// (at or before REST's own observation).
    pub ws_won_race_count: usize,
    pub rest_won_race_count: usize,
}

const WS_SOURCE: &str = "activity_ws";
const REST_SOURCE: &str = "activity_backfill";

/// One row per (event, source) pair: the event's canonical `occurred_at`
/// and one source's `observed_at`. `query_observation_rows` produces this
/// shape from the database; `build_report` consumes it without needing a
/// database at all, so the aggregation math is directly testable.
pub type ObservationRow = (i64, String, String, String);

/// Reads observation rows, optionally scoped to one leader and/or one
/// `(start_utc, end_utc)` window (inclusive, matched against
/// `leader_event_observations.observed_at` -- i.e. *when this process
/// witnessed the event*, not when the trade itself occurred, since an old
/// trade REST only just backfilled during this window is legitimately part
/// of this window's REST performance). Passing no window returns the
/// database's entire history for the leader filter, which a caller should
/// only do after making the contamination risk explicit to the operator.
pub async fn query_observation_rows(
    pool: &SqlitePool,
    leader_id: Option<i64>,
    window: Option<(&str, &str)>,
) -> Result<Vec<ObservationRow>, sqlx::Error> {
    let (start, end) = match window {
        Some((start, end)) => (Some(start), Some(end)),
        None => (None, None),
    };
    sqlx::query_as(
        "SELECT le.id, le.occurred_at, leo.source, leo.observed_at \
         FROM leader_events le \
         JOIN leader_event_observations leo ON leo.leader_event_id = le.id \
         WHERE (? IS NULL OR le.leader_id = ?) \
           AND (? IS NULL OR leo.observed_at >= ?) \
           AND (? IS NULL OR leo.observed_at <= ?) \
         ORDER BY le.id",
    )
    .bind(leader_id)
    .bind(leader_id)
    .bind(start)
    .bind(start)
    .bind(end)
    .bind(end)
    .fetch_all(pool)
    .await
}

/// Aggregates one event's *earliest* observation per source (a reconnect
/// replaying the same message would otherwise double-count a slower,
/// duplicate observation) into per-source latency stats and a WS-vs-REST
/// race outcome per event.
pub fn build_report(rows: &[ObservationRow]) -> LatencyReport {
    let mut by_event: BTreeMap<i64, (&str, BTreeMap<&str, &str>)> = BTreeMap::new();
    for (event_id, occurred_at, source, observed_at) in rows {
        let entry = by_event.entry(*event_id).or_insert_with(|| (occurred_at.as_str(), BTreeMap::new()));
        entry
            .1
            .entry(source.as_str())
            .and_modify(|earliest| {
                if observed_at.as_str() < *earliest {
                    *earliest = observed_at.as_str();
                }
            })
            .or_insert(observed_at.as_str());
    }

    let mut report = LatencyReport {
        total_events: by_event.len(),
        ..LatencyReport::default()
    };
    let mut ws_samples = Vec::new();
    let mut rest_samples = Vec::new();

    for (occurred_at, sources) in by_event.values() {
        let Ok(occurred) = DateTime::parse_from_rfc3339(occurred_at) else {
            continue;
        };
        let latency_ms = |observed_at: &str| {
            DateTime::parse_from_rfc3339(observed_at)
                .ok()
                .map(|observed| (observed - occurred).num_milliseconds())
        };
        let ws_latency = sources.get(WS_SOURCE).and_then(|observed_at| latency_ms(observed_at));
        let rest_latency = sources.get(REST_SOURCE).and_then(|observed_at| latency_ms(observed_at));
        let recognized_other = sources.keys().any(|source| *source != WS_SOURCE && *source != REST_SOURCE);

        match (ws_latency, rest_latency) {
            (Some(ws), Some(rest)) => {
                report.both_observed_count += 1;
                if ws <= rest {
                    report.ws_won_race_count += 1;
                } else {
                    report.rest_won_race_count += 1;
                }
                ws_samples.push(ws);
                rest_samples.push(rest);
            }
            (Some(ws), None) => {
                report.ws_only_count += 1;
                ws_samples.push(ws);
            }
            (None, Some(rest)) => {
                report.rest_only_count += 1;
                rest_samples.push(rest);
            }
            (None, None) if recognized_other => report.other_source_only_count += 1,
            (None, None) => {}
        }
    }

    report.ws_stats = SourceLatencyStats::from_samples(&ws_samples);
    report.rest_stats = SourceLatencyStats::from_samples(&rest_samples);
    report
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConnectionHealthReport {
    pub connected_events: usize,
    pub disconnected_events: usize,
    /// Sum of time between a `Connected` event and the next `Disconnected`
    /// event. If the connection was still open at `as_of` (the window's
    /// end, not just wherever the log happens to stop), that final open
    /// span is included too -- see `still_connected_at_end`.
    pub total_connected_ms: i64,
    /// The longest gap between a `Disconnected` event and the next
    /// `Connected` event. `None` if the connection never dropped.
    pub longest_downtime_ms: Option<i64>,
    /// `true` if the log ends with an open `Connected` span that `as_of`
    /// closed out. `false` either because the connection was down at
    /// `as_of`, or because no `as_of` was given -- in the latter case the
    /// final open span is *not* counted in `total_connected_ms`, since
    /// there is nothing to bound it with.
    pub still_connected_at_end: bool,
    pub unparseable_lines: usize,
}

/// Parses `WS_EVENT:` lines (see `activity_ws::WsConnectionEvent`) from a
/// captured log -- the operator's own stdout/journal redirect from an
/// `ingest_observe` (or `copy_run`) run -- into connection-health stats.
/// `as_of` should be the observation window's end (`ObserveWindow`'s
/// `stopped_at`, parsed), so a connection still open when the window ended
/// is credited with its final open span instead of silently losing it.
/// Never touches the network or the database.
pub fn build_connection_health_report(log: &str, as_of: Option<&str>) -> ConnectionHealthReport {
    let as_of = as_of.and_then(|value| DateTime::parse_from_rfc3339(value).ok());
    let mut report = ConnectionHealthReport::default();
    let mut connected_at: Option<DateTime<FixedOffset>> = None;
    let mut disconnected_at: Option<DateTime<FixedOffset>> = None;

    for line in log.lines() {
        let Some(raw) = line.trim().strip_prefix(WS_EVENT_PREFIX) else {
            continue;
        };
        let Ok(event) = serde_json::from_str::<WsConnectionEvent>(raw.trim()) else {
            report.unparseable_lines += 1;
            continue;
        };
        let Ok(at) = DateTime::parse_from_rfc3339(&event.at_utc) else {
            report.unparseable_lines += 1;
            continue;
        };

        match event.kind {
            WsConnectionEventKind::Connected => {
                report.connected_events += 1;
                if let Some(disconnected) = disconnected_at.take() {
                    let downtime = (at - disconnected).num_milliseconds();
                    report.longest_downtime_ms =
                        Some(report.longest_downtime_ms.map_or(downtime, |current| current.max(downtime)));
                }
                connected_at = Some(at);
            }
            WsConnectionEventKind::Disconnected => {
                report.disconnected_events += 1;
                if let Some(connected) = connected_at.take() {
                    report.total_connected_ms += (at - connected).num_milliseconds();
                }
                disconnected_at = Some(at);
            }
        }
    }

    if let (Some(connected), Some(as_of)) = (connected_at, as_of) {
        report.total_connected_ms += (as_of - connected).num_milliseconds();
        report.still_connected_at_end = true;
    }

    report
}

/// One entry in `ingest_observe`'s own lifecycle -- printed as
/// `OBSERVE_EVENT: {json}`, the same convention `WS_EVENT:`/`GHOST_RECORD:`
/// already use. `Started`/`Stopped` bound the one window a report should
/// ever be scoped to; `BackfillFailure` marks that REST backfill errored
/// during the window (logged, not silently swallowed), so a report can
/// refuse to call a run with any failures "complete."
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObserveEvent {
    pub at_utc: String,
    pub kind: ObserveEventKind,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserveEventKind {
    Started,
    Stopped,
    BackfillFailure,
}

pub const OBSERVE_EVENT_PREFIX: &str = "OBSERVE_EVENT: ";

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ObserveWindow {
    /// The first `Started` event's timestamp.
    pub started_at: Option<String>,
    /// The last `Stopped` event's timestamp -- also the `as_of` a caller
    /// should pass to `build_connection_health_report`.
    pub stopped_at: Option<String>,
    pub backfill_failure_count: usize,
    /// A JSON parse failure, or a `kind`-recognized event whose `at_utc`
    /// was not valid RFC 3339 -- either way, an event this parser refused
    /// to use rather than guess a timestamp for.
    pub unparseable_lines: usize,
    /// More than one `Started` or more than one `Stopped` event was found
    /// -- e.g. a log accidentally concatenating two separate
    /// `ingest_observe` runs. Taking "first started, last stopped" in that
    /// case would silently span both runs (and the gap between them) as if
    /// they were one continuous window, reintroducing exactly the
    /// cross-run contamination this whole windowing mechanism exists to
    /// prevent. `as_query_window` refuses to guess and returns `None`.
    pub ambiguous_multiple_runs: bool,
}

impl ObserveWindow {
    /// `(started_at, stopped_at)` only when both are present and
    /// unambiguous -- the shape `query_observation_rows`'s `window`
    /// parameter expects. `None` if the log's start/stop markers were
    /// ambiguous (more than one of either), even if `started_at`/
    /// `stopped_at` are individually set to *some* value.
    pub fn as_query_window(&self) -> Option<(&str, &str)> {
        if self.ambiguous_multiple_runs {
            return None;
        }
        Some((self.started_at.as_deref()?, self.stopped_at.as_deref()?))
    }
}

/// Parses `OBSERVE_EVENT:` lines from a captured log into the window one
/// `ingest_observe` run covered. Never touches the network or the database.
/// Fail-closed on two shapes of bad input: a malformed `at_utc` (rejected
/// the same way a JSON parse failure is, not stored as a boundary), and a
/// log containing more than one `Started`/`Stopped` pair (flagged as
/// `ambiguous_multiple_runs`, never guessed at).
pub fn parse_observe_window(log: &str) -> ObserveWindow {
    let mut window = ObserveWindow::default();
    let mut started_count = 0usize;
    let mut stopped_count = 0usize;

    for line in log.lines() {
        let Some(raw) = line.trim().strip_prefix(OBSERVE_EVENT_PREFIX) else {
            continue;
        };
        let Ok(event) = serde_json::from_str::<ObserveEvent>(raw.trim()) else {
            window.unparseable_lines += 1;
            continue;
        };
        if DateTime::parse_from_rfc3339(&event.at_utc).is_err() {
            window.unparseable_lines += 1;
            continue;
        }

        match event.kind {
            ObserveEventKind::Started => {
                started_count += 1;
                if window.started_at.is_none() {
                    window.started_at = Some(event.at_utc);
                }
            }
            ObserveEventKind::Stopped => {
                stopped_count += 1;
                window.stopped_at = Some(event.at_utc);
            }
            ObserveEventKind::BackfillFailure => window.backfill_failure_count += 1,
        }
    }

    window.ambiguous_multiple_runs = started_count > 1 || stopped_count > 1;
    window
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(event_id: i64, occurred_at: &str, source: &str, observed_at: &str) -> ObservationRow {
        (event_id, occurred_at.to_owned(), source.to_owned(), observed_at.to_owned())
    }

    #[test]
    fn a_ws_only_event_is_counted_and_latency_measured() {
        let rows = vec![row(1, "2026-09-04T00:00:00.000Z", WS_SOURCE, "2026-09-04T00:00:01.500Z")];
        let report = build_report(&rows);

        assert_eq!(report.total_events, 1);
        assert_eq!(report.ws_only_count, 1);
        assert_eq!(report.rest_only_count, 0);
        assert_eq!(report.ws_stats.count, 1);
        assert_eq!(report.ws_stats.min_latency_ms, Some(1_500));
    }

    #[test]
    fn an_event_observed_by_both_sources_counts_the_race_winner() {
        let rows = vec![
            row(1, "2026-09-04T00:00:00.000Z", WS_SOURCE, "2026-09-04T00:00:01.000Z"),
            row(1, "2026-09-04T00:00:00.000Z", REST_SOURCE, "2026-09-04T00:01:00.000Z"),
        ];
        let report = build_report(&rows);

        assert_eq!(report.total_events, 1);
        assert_eq!(report.both_observed_count, 1);
        assert_eq!(report.ws_won_race_count, 1);
        assert_eq!(report.rest_won_race_count, 0);
        assert_eq!(report.ws_stats.min_latency_ms, Some(1_000));
        assert_eq!(report.rest_stats.min_latency_ms, Some(60_000));
    }

    #[test]
    fn a_reconnect_replaying_the_same_source_twice_keeps_only_the_earliest_observation() {
        let rows = vec![
            row(1, "2026-09-04T00:00:00.000Z", WS_SOURCE, "2026-09-04T00:00:05.000Z"),
            row(1, "2026-09-04T00:00:00.000Z", WS_SOURCE, "2026-09-04T00:00:01.000Z"),
        ];
        let report = build_report(&rows);

        assert_eq!(report.ws_stats.count, 1, "one event, one source -- not two samples");
        assert_eq!(report.ws_stats.min_latency_ms, Some(1_000), "the earliest observation wins");
    }

    #[test]
    fn an_unrecognized_source_is_counted_not_silently_dropped() {
        let rows = vec![row(1, "2026-09-04T00:00:00.000Z", "onchain_ws", "2026-09-04T00:00:01.000Z")];
        let report = build_report(&rows);

        assert_eq!(report.total_events, 1);
        assert_eq!(report.other_source_only_count, 1);
        assert_eq!(report.ws_only_count, 0);
        assert_eq!(report.rest_only_count, 0);
    }

    fn ws_event_line(at_utc: &str, kind: WsConnectionEventKind) -> String {
        let event = WsConnectionEvent {
            at_utc: at_utc.to_owned(),
            kind,
            detail: String::new(),
            next_reconnect_delay_ms: None,
        };
        format!("{WS_EVENT_PREFIX}{}", serde_json::to_string(&event).unwrap())
    }

    #[test]
    fn connection_health_measures_uptime_and_the_longest_downtime() {
        let log = [
            ws_event_line("2026-09-04T00:00:00.000Z", WsConnectionEventKind::Connected),
            ws_event_line("2026-09-04T00:01:00.000Z", WsConnectionEventKind::Disconnected),
            ws_event_line("2026-09-04T00:01:10.000Z", WsConnectionEventKind::Connected),
            ws_event_line("2026-09-04T00:05:10.000Z", WsConnectionEventKind::Disconnected),
        ]
        .join("\n");

        let report = build_connection_health_report(&log, None);

        assert_eq!(report.connected_events, 2);
        assert_eq!(report.disconnected_events, 2);
        assert_eq!(report.total_connected_ms, 60_000 + 240_000);
        assert_eq!(report.longest_downtime_ms, Some(10_000));
        assert!(!report.still_connected_at_end);
        assert_eq!(report.unparseable_lines, 0);
    }

    #[test]
    fn a_connection_that_never_dropped_has_no_downtime() {
        let log = ws_event_line("2026-09-04T00:00:00.000Z", WsConnectionEventKind::Connected);
        let report = build_connection_health_report(&log, None);
        assert_eq!(report.longest_downtime_ms, None);
    }

    #[test]
    fn a_connection_still_open_when_the_log_ends_is_lost_without_an_as_of() {
        // The exact gap this fixes: no as_of means the final open span
        // cannot be bounded, so it must not be silently invented -- but
        // this also means, without an as_of, that time is genuinely
        // uncounted, which is why ingest_observe always emits a `stopped`
        // OBSERVE_EVENT for the report to use as as_of.
        let log = ws_event_line("2026-09-04T00:00:00.000Z", WsConnectionEventKind::Connected);
        let report = build_connection_health_report(&log, None);
        assert_eq!(report.total_connected_ms, 0);
        assert!(!report.still_connected_at_end);
    }

    #[test]
    fn a_connection_still_open_at_as_of_credits_the_final_open_span() {
        let log = ws_event_line("2026-09-04T00:00:00.000Z", WsConnectionEventKind::Connected);
        let report = build_connection_health_report(&log, Some("2026-09-04T00:05:00.000Z"));
        assert_eq!(report.total_connected_ms, 300_000);
        assert!(report.still_connected_at_end);
    }

    fn observe_event_line(at_utc: &str, kind: ObserveEventKind) -> String {
        let event = ObserveEvent { at_utc: at_utc.to_owned(), kind, detail: String::new() };
        format!("{OBSERVE_EVENT_PREFIX}{}", serde_json::to_string(&event).unwrap())
    }

    #[test]
    fn parse_observe_window_finds_the_start_and_stop_and_counts_backfill_failures() {
        let log = [
            observe_event_line("2026-09-04T00:00:00.000Z", ObserveEventKind::Started),
            observe_event_line("2026-09-04T00:02:00.000Z", ObserveEventKind::BackfillFailure),
            observe_event_line("2026-09-04T01:00:00.000Z", ObserveEventKind::Stopped),
        ]
        .join("\n");

        let window = parse_observe_window(&log);
        assert_eq!(window.started_at.as_deref(), Some("2026-09-04T00:00:00.000Z"));
        assert_eq!(window.stopped_at.as_deref(), Some("2026-09-04T01:00:00.000Z"));
        assert_eq!(window.backfill_failure_count, 1);
        assert_eq!(
            window.as_query_window(),
            Some(("2026-09-04T00:00:00.000Z", "2026-09-04T01:00:00.000Z"))
        );
    }

    #[test]
    fn a_missing_stopped_event_has_no_query_window() {
        let log = observe_event_line("2026-09-04T00:00:00.000Z", ObserveEventKind::Started);
        let window = parse_observe_window(&log);
        assert_eq!(window.as_query_window(), None);
    }

    #[test]
    fn two_concatenated_runs_are_flagged_ambiguous_not_silently_spanned() {
        // The exact hazard this fixes: naively taking "first started, last
        // stopped" here would produce 00:00 .. 02:00, silently spanning
        // both runs and the gap between them as if it were one continuous
        // window.
        let log = [
            observe_event_line("2026-09-04T00:00:00.000Z", ObserveEventKind::Started),
            observe_event_line("2026-09-04T00:10:00.000Z", ObserveEventKind::Stopped),
            observe_event_line("2026-09-04T01:00:00.000Z", ObserveEventKind::Started),
            observe_event_line("2026-09-04T02:00:00.000Z", ObserveEventKind::Stopped),
        ]
        .join("\n");

        let window = parse_observe_window(&log);
        assert!(window.ambiguous_multiple_runs);
        assert_eq!(
            window.as_query_window(),
            None,
            "an ambiguous log must never produce a guessed query window"
        );
    }

    #[test]
    fn two_started_events_with_only_one_stopped_are_still_flagged_ambiguous() {
        let log = [
            observe_event_line("2026-09-04T00:00:00.000Z", ObserveEventKind::Started),
            observe_event_line("2026-09-04T00:05:00.000Z", ObserveEventKind::Started),
            observe_event_line("2026-09-04T00:10:00.000Z", ObserveEventKind::Stopped),
        ]
        .join("\n");

        let window = parse_observe_window(&log);
        assert!(window.ambiguous_multiple_runs);
        assert_eq!(window.as_query_window(), None);
    }

    #[test]
    fn an_event_with_a_non_rfc3339_at_utc_is_rejected_not_used_as_a_boundary() {
        let log = [
            format!(
                "{OBSERVE_EVENT_PREFIX}{}",
                serde_json::to_string(&ObserveEvent {
                    at_utc: "not-a-timestamp".to_owned(),
                    kind: ObserveEventKind::Started,
                    detail: String::new(),
                })
                .unwrap()
            ),
            observe_event_line("2026-09-04T00:10:00.000Z", ObserveEventKind::Stopped),
        ]
        .join("\n");

        let window = parse_observe_window(&log);
        assert_eq!(window.started_at, None, "the malformed timestamp must never become the window start");
        assert_eq!(window.unparseable_lines, 1);
        assert_eq!(window.as_query_window(), None, "half a window is not a usable window");
    }

    #[test]
    fn events_outside_the_window_are_excluded_from_the_latency_report() {
        // The exact contamination this fixes: an earlier run's leftover
        // REST-only event must not appear in a later, differently-scoped
        // report.
        let all_rows = vec![
            row(1, "2026-09-01T00:00:00.000Z", REST_SOURCE, "2026-09-01T00:00:05.000Z"), // earlier run
            row(2, "2026-09-04T00:00:00.000Z", WS_SOURCE, "2026-09-04T00:00:01.000Z"),   // this run
        ];
        let window_start = "2026-09-04T00:00:00.000Z";
        let window_end = "2026-09-04T01:00:00.000Z";
        let in_window: Vec<ObservationRow> = all_rows
            .into_iter()
            .filter(|(_, _, _, observed_at)| {
                observed_at.as_str() >= window_start && observed_at.as_str() <= window_end
            })
            .collect();

        let report = build_report(&in_window);
        assert_eq!(report.total_events, 1);
        assert_eq!(report.rest_only_count, 0);
        assert_eq!(report.ws_only_count, 1);
    }
}
