//! Read-only aggregation for the terminal dashboard (`copy_dashboard`):
//! summarizes every leader's status, lots, and intent/reconciliation
//! counts from `control_tower`'s existing read functions, plus a
//! restart-safe log-file tailer, a classifier for this project's
//! structured event log lines, and the one-shot "given the current state,
//! produce a frame" render function ([`draw_ui`]). No venue write, no
//! order adapter.
//!
//! The interactive event loop -- polling keyboard input, sleeping between
//! ticks, repeatedly calling `terminal.draw` -- lives in the binary, not
//! here, matching this project's existing precedent for IO-facing code
//! that cannot be meaningfully unit-tested without a live terminal (see
//! `ingest`'s own module doc). [`draw_ui`] itself takes no terminal or
//! event source at all, only an [`AppState`] and a `Frame` to render
//! into, so it can be (and is) tested against `ratatui`'s headless
//! `TestBackend` -- no real terminal, no live credential, no network.

use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, Read as _, Seek as _, SeekFrom},
    path::PathBuf,
};

use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, Borders, List, ListItem, Paragraph, Row, Table},
    Frame,
};
use rust_decimal::Decimal;
use sqlx::SqlitePool;

use super::control_tower::{
    self, ControlTowerError, IntentSummary, LeaderStatus, LotSummary, ReconciliationCaseSummary,
};

#[derive(Debug, Clone, PartialEq)]
pub struct LeaderDashboardSummary {
    pub leader_id: i64,
    pub label: String,
    pub enabled: bool,
    pub activation_at: Option<String>,
    pub tokens_held: usize,
    pub intents_by_status: BTreeMap<String, usize>,
    pub unresolved_reconciliation_cases: usize,
}

/// One row per configured leader, in `leader_config.id` order. A leader
/// with zero of everything still gets a row -- an empty dashboard entry is
/// meaningful ("nothing has happened for this leader yet"), not something
/// to hide.
pub async fn collect_dashboard(
    pool: &SqlitePool,
) -> Result<Vec<LeaderDashboardSummary>, ControlTowerError> {
    let leader_ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM leader_config ORDER BY id")
        .fetch_all(pool)
        .await
        .map_err(|error| ControlTowerError::Database(error.to_string()))?;

    let mut summaries = Vec::with_capacity(leader_ids.len());
    for leader_id in leader_ids {
        let status = control_tower::leader_status(pool, leader_id).await?;
        let lots = control_tower::leader_lots(pool, leader_id).await?;
        let intents = control_tower::leader_intents(pool, leader_id).await?;
        let cases = control_tower::leader_reconciliation_cases(pool, leader_id).await?;
        summaries.push(summarize_leader(&status, &lots, &intents, &cases));
    }
    Ok(summaries)
}

fn summarize_leader(
    status: &LeaderStatus,
    lots: &[LotSummary],
    intents: &[IntentSummary],
    cases: &[ReconciliationCaseSummary],
) -> LeaderDashboardSummary {
    let tokens_held = lots
        .iter()
        .filter(|lot| {
            lot.qty
                .parse::<Decimal>()
                .is_ok_and(|qty| qty > Decimal::ZERO)
        })
        .count();

    let mut intents_by_status = BTreeMap::new();
    for intent in intents {
        *intents_by_status.entry(intent.status.clone()).or_insert(0) += 1;
    }

    let unresolved_reconciliation_cases = cases
        .iter()
        .filter(|case| case.resolved_at.is_none())
        .count();

    LeaderDashboardSummary {
        leader_id: status.leader_id,
        label: status.label.clone(),
        enabled: status.enabled,
        activation_at: status.activation_at.clone(),
        tokens_held,
        intents_by_status,
        unresolved_reconciliation_cases,
    }
}

/// Restart-safe, append-only tail of one log file: reads only whatever has
/// been written since the previous [`Self::poll`], and buffers a trailing
/// line the writer has not finished (no newline yet) until it completes,
/// rather than displaying a line that will be split across two polls.
pub struct LogTailer {
    path: PathBuf,
    offset: u64,
    pending: String,
}

impl LogTailer {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            offset: 0,
            pending: String::new(),
        }
    }

    /// Returns every complete line appended since the last poll, oldest
    /// first. A missing file is not an error (the operator may not have
    /// started redirecting output yet) -- it simply yields no lines. If
    /// the file is now shorter than the last known offset (rotated or
    /// truncated), tailing restarts from its current beginning rather
    /// than erroring: a dashboard must keep working across log rotation,
    /// not go blank or crash.
    pub fn poll(&mut self) -> io::Result<Vec<String>> {
        let mut file = match std::fs::File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let len = file.metadata()?.len();
        if len < self.offset {
            self.offset = 0;
            self.pending.clear();
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        self.offset += buf.len() as u64;
        self.pending.push_str(&String::from_utf8_lossy(&buf));

        let mut complete_lines = Vec::new();
        while let Some(newline_at) = self.pending.find('\n') {
            let line = self.pending[..newline_at].trim_end_matches('\r').to_owned();
            complete_lines.push(line);
            self.pending.drain(..=newline_at);
        }
        Ok(complete_lines)
    }
}

/// These must match the exact prefix constants each event's own emitting
/// module defines (`WS_EVENT_PREFIX` in `ingest::activity_ws`,
/// `OBSERVE_EVENT_PREFIX` in `ingest::latency_report`,
/// `REDEMPTION_EVENT_PREFIX` in `redemption`, `CONFIG_APPLIED_PREFIX` in
/// `setup`). Duplicated as plain strings, rather than importing those
/// constants, on purpose: the dashboard's whole point is to stay a
/// lightweight passive viewer, and importing them would drag in `ingest`
/// (the WebSocket/backfill stack) and `redeem_detect` as compile-time
/// dependencies of a binary that only ever reads a text file.
/// `GHOST_RECORD_PREFIX` is the one exception: `dashboard` already
/// requires `execute`, which already implies `intl_clob`, which already
/// carries `crate::ghost_drift` -- importing it there costs nothing new.
const WS_EVENT_PREFIX: &str = "WS_EVENT: ";
const OBSERVE_EVENT_PREFIX: &str = "OBSERVE_EVENT: ";
const REDEMPTION_EVENT_PREFIX: &str = "REDEMPTION_EVENT: ";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLineKind {
    Ghost,
    Ws,
    Observe,
    Redemption,
    ConfigApplied,
    Plain,
}

pub fn classify_log_line(line: &str) -> LogLineKind {
    if line.starts_with(crate::ghost_drift::GHOST_RECORD_PREFIX) {
        LogLineKind::Ghost
    } else if line.starts_with(WS_EVENT_PREFIX) {
        LogLineKind::Ws
    } else if line.starts_with(OBSERVE_EVENT_PREFIX) {
        LogLineKind::Observe
    } else if line.starts_with(REDEMPTION_EVENT_PREFIX) {
        LogLineKind::Redemption
    } else if line.starts_with(super::setup::CONFIG_APPLIED_PREFIX) {
        LogLineKind::ConfigApplied
    } else {
        LogLineKind::Plain
    }
}

/// Everything [`draw_ui`] renders. Four independent error slots, not one
/// shared "last error" field: a fresh log-tail failure every ~200ms must
/// never silently erase a dashboard or balance query failure that only
/// refreshes every few seconds (and vice versa) just because they would
/// otherwise share one mutable slot with last-write-wins semantics.
pub struct AppState {
    pub log_path: String,
    pub dashboard: Vec<LeaderDashboardSummary>,
    pub collateral: Option<Decimal>,
    pub allowance: Option<Decimal>,
    pub log_lines: VecDeque<(LogLineKind, String)>,
    pub dashboard_error: Option<String>,
    pub balance_error: Option<String>,
    pub allowance_error: Option<String>,
    pub log_error: Option<String>,
}

impl AppState {
    pub fn new(log_path: String) -> Self {
        Self {
            log_path,
            dashboard: Vec::new(),
            collateral: None,
            allowance: None,
            log_lines: VecDeque::new(),
            dashboard_error: None,
            balance_error: None,
            allowance_error: None,
            log_error: None,
        }
    }

    /// Appends one classified log line, dropping the oldest once `cap` is
    /// exceeded -- a long-running session must not grow this without
    /// bound.
    pub fn push_log_line(&mut self, kind: LogLineKind, line: String, cap: usize) {
        self.log_lines.push_back((kind, line));
        while self.log_lines.len() > cap {
            self.log_lines.pop_front();
        }
    }

    fn errors(&self) -> Vec<&str> {
        [
            &self.dashboard_error,
            &self.balance_error,
            &self.allowance_error,
            &self.log_error,
        ]
        .into_iter()
        .filter_map(|error| error.as_deref())
        .collect()
    }
}

/// Renders one frame from `state`. Pure with respect to the terminal: it
/// only calls `frame.render_widget`, never reads state from `frame`
/// itself or performs any IO -- see the module doc for why this makes it
/// testable against `ratatui::backend::TestBackend`.
pub fn draw_ui(frame: &mut Frame, state: &AppState) {
    let area = frame.area();
    // 2 rows for the block's own top/bottom border, +1 for the header
    // row, +1 per leader. Forgetting the border rows here previously
    // meant a one-leader table was allocated a 2-row block -- entirely
    // consumed by its own border, with zero rows left for the header or
    // any data -- caught by draw_ui's own TestBackend test, not by eye on
    // a real terminal.
    let leaders_panel_height = 3 + state.dashboard.len() as u16;
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(leaders_panel_height),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .split(area);

    let balance_text = match (state.collateral, state.allowance) {
        (Some(balance), Some(allow)) => {
            format!("collateral: {balance} USDC   allowance: {allow} USDC")
        }
        _ => "collateral: (loading...)".to_owned(),
    };
    frame.render_widget(
        Paragraph::new(balance_text).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Account Balance"),
        ),
        rows[0],
    );

    let table_rows: Vec<Row> = state
        .dashboard
        .iter()
        .map(|leader| {
            let statuses: Vec<String> = leader
                .intents_by_status
                .iter()
                .map(|(status, count)| format!("{status}={count}"))
                .collect();
            Row::new(vec![
                leader.leader_id.to_string(),
                leader.label.clone(),
                if leader.enabled {
                    "yes".to_owned()
                } else {
                    "no".to_owned()
                },
                leader.tokens_held.to_string(),
                if statuses.is_empty() {
                    "-".to_owned()
                } else {
                    statuses.join(", ")
                },
                leader.unresolved_reconciliation_cases.to_string(),
            ])
        })
        .collect();
    let table = Table::new(
        table_rows,
        [
            Constraint::Length(4),
            Constraint::Length(16),
            Constraint::Length(7),
            Constraint::Length(12),
            Constraint::Min(20),
            Constraint::Length(11),
        ],
    )
    .header(Row::new(vec![
        "id",
        "label",
        "enabled",
        "tokens held",
        "intents by status",
        "open cases",
    ]))
    .block(Block::default().borders(Borders::ALL).title("Leaders"));
    frame.render_widget(table, rows[1]);

    let log_items: Vec<ListItem> = state
        .log_lines
        .iter()
        .rev()
        .take(rows[2].height.saturating_sub(2) as usize)
        .rev()
        .map(|(kind, line)| {
            let style = match kind {
                LogLineKind::Ghost => Style::default().fg(Color::Cyan),
                LogLineKind::Ws => Style::default().fg(Color::Blue),
                LogLineKind::Observe => Style::default().fg(Color::Magenta),
                LogLineKind::Redemption => Style::default().fg(Color::Green),
                LogLineKind::ConfigApplied => Style::default().fg(Color::Yellow),
                LogLineKind::Plain => Style::default(),
            };
            ListItem::new(line.as_str()).style(style)
        })
        .collect();
    frame.render_widget(
        List::new(log_items).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Log: {}", state.log_path)),
        ),
        rows[2],
    );

    let errors = state.errors();
    let status_text = if errors.is_empty() {
        "q/Esc to quit".to_owned()
    } else {
        format!("q/Esc to quit -- {}", errors.join(" | "))
    };
    frame.render_widget(Paragraph::new(status_text), rows[3]);
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn leader(enabled: bool) -> LeaderStatus {
        LeaderStatus {
            leader_id: 1,
            label: "leader-a".to_owned(),
            enabled,
            activation_at: Some("2026-09-04T00:00:00Z".to_owned()),
        }
    }

    fn lot(token_id: &str, qty: &str) -> LotSummary {
        LotSummary {
            account_id: 1,
            leader_id: 1,
            token_id: token_id.to_owned(),
            qty: qty.to_owned(),
            updated_at: "2026-09-04T00:00:00Z".to_owned(),
        }
    }

    fn intent(status: &str) -> IntentSummary {
        IntentSummary {
            intent_id: 1,
            event_id: 1,
            leader_id: 1,
            account_id: 1,
            token_id: "token-1".to_owned(),
            side: "BUY".to_owned(),
            status: status.to_owned(),
            reserved_qty: "0".to_owned(),
            config_snapshot_json: "{}".to_owned(),
            config_snapshot_hash: "hash".to_owned(),
            created_at: "2026-09-04T00:00:00Z".to_owned(),
        }
    }

    fn case(resolved: bool) -> ReconciliationCaseSummary {
        ReconciliationCaseSummary {
            case_id: 1,
            account_id: 1,
            token_id: "token-1".to_owned(),
            intent_id: Some(1),
            order_attempt_id: None,
            case_type: "other".to_owned(),
            detail: None,
            opened_at: "2026-09-04T00:00:00Z".to_owned(),
            resolved_at: resolved.then(|| "2026-09-04T01:00:00Z".to_owned()),
        }
    }

    #[test]
    fn only_positive_qty_lots_are_counted_as_tokens_held() {
        let lots = [lot("a", "5"), lot("b", "0"), lot("c", "-1")];
        let summary = summarize_leader(&leader(true), &lots, &[], &[]);
        assert_eq!(summary.tokens_held, 1);
    }

    #[test]
    fn intents_are_grouped_by_status() {
        let intents = [intent("pending"), intent("pending"), intent("completed")];
        let summary = summarize_leader(&leader(true), &[], &intents, &[]);
        assert_eq!(summary.intents_by_status.get("pending"), Some(&2));
        assert_eq!(summary.intents_by_status.get("completed"), Some(&1));
    }

    #[test]
    fn only_unresolved_reconciliation_cases_are_counted() {
        let cases = [case(false), case(false), case(true)];
        let summary = summarize_leader(&leader(true), &[], &[], &cases);
        assert_eq!(summary.unresolved_reconciliation_cases, 2);
    }

    #[test]
    fn classifies_every_known_prefix_and_falls_back_to_plain() {
        assert_eq!(classify_log_line("GHOST_RECORD: {}"), LogLineKind::Ghost);
        assert_eq!(classify_log_line("WS_EVENT: {}"), LogLineKind::Ws);
        assert_eq!(classify_log_line("OBSERVE_EVENT: {}"), LogLineKind::Observe);
        assert_eq!(
            classify_log_line("REDEMPTION_EVENT: {}"),
            LogLineKind::Redemption
        );
        assert_eq!(
            classify_log_line("CONFIG_APPLIED: {}"),
            LogLineKind::ConfigApplied
        );
        assert_eq!(
            classify_log_line("just some other line"),
            LogLineKind::Plain
        );
    }

    struct TempPath(PathBuf);
    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn temp_log_path(name: &str) -> TempPath {
        use std::{
            process,
            time::{SystemTime, UNIX_EPOCH},
        };
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        TempPath(std::env::temp_dir().join(format!(
            "polycopy-engine-dashboard-test-{name}-{}-{nonce}.log",
            process::id()
        )))
    }

    #[test]
    fn a_missing_log_file_yields_no_lines_not_an_error() {
        let path = temp_log_path("missing");
        let mut tailer = LogTailer::new(&path.0);
        assert_eq!(tailer.poll().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn only_complete_lines_are_returned_a_trailing_partial_line_waits() {
        let path = temp_log_path("partial");
        std::fs::write(&path.0, "line one\nline two\nline th").unwrap();
        let mut tailer = LogTailer::new(&path.0);

        let first_poll = tailer.poll().unwrap();
        assert_eq!(first_poll, vec!["line one", "line two"]);

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path.0)
            .unwrap();
        writeln!(file, "ree").unwrap();
        drop(file);

        let second_poll = tailer.poll().unwrap();
        assert_eq!(second_poll, vec!["line three"]);
    }

    #[test]
    fn a_truncated_or_rotated_file_is_tailed_from_the_start_again() {
        let path = temp_log_path("rotated");
        std::fs::write(&path.0, "old line one\nold line two\n").unwrap();
        let mut tailer = LogTailer::new(&path.0);
        assert_eq!(tailer.poll().unwrap().len(), 2);

        std::fs::write(&path.0, "new short file\n").unwrap();
        assert_eq!(tailer.poll().unwrap(), vec!["new short file"]);
    }

    #[test]
    fn push_log_line_drops_the_oldest_once_the_cap_is_exceeded() {
        let mut state = AppState::new("test.log".to_owned());
        for i in 0..5 {
            state.push_log_line(LogLineKind::Plain, format!("line {i}"), 3);
        }
        assert_eq!(state.log_lines.len(), 3);
        assert_eq!(state.log_lines.front().unwrap().1, "line 2");
        assert_eq!(state.log_lines.back().unwrap().1, "line 4");
    }

    #[test]
    fn errors_only_reports_slots_that_are_actually_set() {
        let mut state = AppState::new("test.log".to_owned());
        assert!(state.errors().is_empty());
        state.balance_error = Some("balance broke".to_owned());
        assert_eq!(state.errors(), vec!["balance broke"]);
    }

    /// `draw_ui` takes no terminal or event source, only state and a
    /// `Frame` -- this exercises the exact render function `copy_dashboard`
    /// calls every tick, against `ratatui`'s headless `TestBackend`, with
    /// no real terminal, no live credential, and no network involved.
    #[test]
    fn draw_ui_renders_leader_rows_balance_and_errors_without_a_real_terminal() {
        use ratatui::{backend::TestBackend, Terminal};

        let mut state = AppState::new("/var/log/copy.log".to_owned());
        state.collateral = Some(Decimal::new(12345, 2));
        state.allowance = Some(Decimal::new(100, 0));
        state.dashboard.push(LeaderDashboardSummary {
            leader_id: 1,
            label: "leader-a".to_owned(),
            enabled: true,
            activation_at: Some("2026-09-04T00:00:00Z".to_owned()),
            tokens_held: 2,
            intents_by_status: BTreeMap::from([("pending".to_owned(), 3)]),
            unresolved_reconciliation_cases: 1,
        });
        state.balance_error = Some("stale balance".to_owned());
        state.push_log_line(LogLineKind::Ghost, "GHOST_RECORD: {}".to_owned(), 500);

        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        terminal.draw(|frame| draw_ui(frame, &state)).unwrap();

        let rendered: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();

        assert!(
            rendered.contains("123.45"),
            "balance not rendered: {rendered}"
        );
        assert!(
            rendered.contains("leader-a"),
            "label not rendered: {rendered}"
        );
        assert!(
            rendered.contains("pending=3"),
            "intent count not rendered: {rendered}"
        );
        assert!(
            rendered.contains("stale balance"),
            "error not rendered: {rendered}"
        );
        assert!(
            rendered.contains("GHOST_RECORD"),
            "log line not rendered: {rendered}"
        );
        assert!(
            rendered.contains("/var/log/copy.log"),
            "log path not in title: {rendered}"
        );
    }
}
