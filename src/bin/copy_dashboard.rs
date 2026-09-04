//! Read-only terminal dashboard: leader/lot/intent/reconciliation status,
//! account collateral balance, and a live tail of the operator's own log
//! file. Meant to run over SSH on the same host as the database and log
//! files. Its database/log panels are local-only; its optional account
//! balance panel makes authenticated read-only CLOB requests.
//!
//! It authenticates once, at startup, only to derive the existing CLOB L2
//! credential for the same strict, read-only collateral query
//! `persistent_control`/`ghost_verify` already perform; it never creates
//! an API key and never prepares, signs, submits, cancels, or changes a
//! venue order. It never starts, stops, or monitors `copy_run` or any
//! other process -- the log panel only reads a file the operator already
//! redirected output into, the same way `ingest_latency_report` and
//! `ghost_drift_report` do.
//!
//! Unlike `redeem_check`/`ingest_observe`, this binary cannot be kept
//! architecturally independent of the `execute` feature (the balance read
//! lives under `execute`+`intl_clob`, same as every other operator tool
//! that reads a balance), so "no order capability" here is a source-level
//! guarantee -- this file imports nothing from `execute`, `orchestrate`,
//! `reconcile`, or `venue::signed_order` -- not a feature-gate one.
//!
//! The rendering logic itself (`copytrading::draw_ui`/`AppState`) is
//! tested headlessly in `copytrading::dashboard`; this file is only the
//! thin, untestable-without-a-real-terminal event loop around it.
//!
//! ```text
//! Usage:
//!   POLYCOPY_DB_PATH=...                   (required)
//!   POLYCOPY_DASHBOARD_LOG_PATH=...        (optional) log file to tail;
//!                                           omit when the service logs only
//!                                           to journald
//!   POLYCOPY_DASHBOARD_REFRESH_SECONDS=5   (optional, default 5, must be > 0)
//!                                           how often to re-query the database
//!                                           and the venue for balance
//! ```
//!
//! Press 'q' or Esc to quit. A failed refresh (database or venue) is shown
//! in the status line and retried on the next cycle -- it never ends the
//! session; only quitting or a startup failure does.

#[cfg(feature = "dashboard")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::{process, time::Duration};

    use polycopy_engine::{
        copytrading::{
            classify_log_line, collect_dashboard, draw_ui, open_read_only, AppState, LogTailer,
        },
        venue::{intl_clob::StrictAccountBalanceReader, intl_clob_exec::IntlClobCopyAdapter},
    };
    use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind};
    use tokio::time::Instant;

    const MAX_LOG_LINES: usize = 500;
    const TICK: Duration = Duration::from_millis(200);

    let startup: Result<(String, String, Duration), String> = (|| {
        let db_path = required("POLYCOPY_DB_PATH")?;
        let log_path = std::env::var("POLYCOPY_DASHBOARD_LOG_PATH")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "(log tail disabled; use journalctl for service logs)".to_owned());
        let refresh_seconds: u64 = match std::env::var("POLYCOPY_DASHBOARD_REFRESH_SECONDS") {
            Ok(raw) if !raw.trim().is_empty() => raw.parse().map_err(|_| {
                "POLYCOPY_DASHBOARD_REFRESH_SECONDS must be a positive integer".to_owned()
            })?,
            _ => 5,
        };
        if refresh_seconds == 0 {
            return Err("POLYCOPY_DASHBOARD_REFRESH_SECONDS must be greater than 0".to_owned());
        }
        Ok((db_path, log_path, Duration::from_secs(refresh_seconds)))
    })();
    let (db_path, log_path, refresh_every) = match startup {
        Ok(values) => values,
        Err(error) => {
            eprintln!("copy_dashboard failed: {error}");
            process::exit(3);
        }
    };

    let pool = match open_read_only(&db_path).await {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!("copy_dashboard failed: unable to open database: {error}");
            process::exit(3);
        }
    };

    // Derive-only: proves the configured signer can represent the
    // account, never creates or writes anything to the venue. Done once,
    // up front -- the resulting adapter is reused for every balance
    // refresh rather than re-authenticating every cycle.
    let adapter = match IntlClobCopyAdapter::from_env().await {
        Ok(adapter) => adapter,
        Err(error) => {
            eprintln!("copy_dashboard failed: derive-only authentication failed: {error}");
            process::exit(3);
        }
    };

    let mut log_tailer = LogTailer::new(&log_path);
    let mut state = AppState::new(log_path);

    let mut terminal = match ratatui::try_init() {
        Ok(terminal) => terminal,
        Err(error) => {
            eprintln!("copy_dashboard failed: unable to initialize the terminal: {error}");
            process::exit(3);
        }
    };

    let mut last_refresh = Instant::now() - refresh_every; // refresh immediately on first loop
    let quit_reason = 'main_loop: loop {
        if last_refresh.elapsed() >= refresh_every {
            last_refresh = Instant::now();
            match collect_dashboard(&pool).await {
                Ok(rows) => {
                    state.dashboard = rows;
                    state.dashboard_error = None;
                }
                Err(error) => {
                    state.dashboard_error = Some(format!("dashboard query failed: {error}"))
                }
            }
            match adapter.read_adapter().collateral_balance_strict().await {
                Ok(value) => {
                    state.collateral = Some(value);
                    state.balance_error = None;
                }
                Err(error) => state.balance_error = Some(format!("balance query failed: {error}")),
            }
            match adapter.read_adapter().collateral_allowance_strict().await {
                Ok(value) => {
                    state.allowance = Some(value);
                    state.allowance_error = None;
                }
                Err(error) => {
                    state.allowance_error = Some(format!("allowance query failed: {error}"))
                }
            }
        }

        match log_tailer.poll() {
            Ok(new_lines) => {
                for line in new_lines {
                    let kind = classify_log_line(&line);
                    state.push_log_line(kind, line, MAX_LOG_LINES);
                }
                state.log_error = None;
            }
            Err(error) => state.log_error = Some(format!("log tail failed: {error}")),
        }

        if let Err(error) = terminal.draw(|frame| draw_ui(frame, &state)) {
            break 'main_loop format!("render error: {error}");
        }

        match event::poll(TICK) {
            Ok(true) => match event::read() {
                Ok(Event::Key(key))
                    if key.kind == KeyEventKind::Press
                        && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc) =>
                {
                    break 'main_loop "quit requested".to_owned();
                }
                Ok(_) => {}
                Err(error) => break 'main_loop format!("input error: {error}"),
            },
            Ok(false) => {}
            Err(error) => break 'main_loop format!("input error: {error}"),
        }
    };

    ratatui::restore();
    eprintln!("copy_dashboard stopped: {quit_reason}");
}

#[cfg(feature = "dashboard")]
fn required(name: &'static str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

#[cfg(not(feature = "dashboard"))]
fn main() {
    eprintln!("copy_dashboard requires the dashboard feature");
    std::process::exit(2);
}
