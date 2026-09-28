//! 跟单操作面板:the owner's Chinese operations panel, run on the execution
//! server as root:
//!
//! ```text
//! ssh -t <host> /opt/polycopy-engine/current/target/release/ops_panel
//! ```
//!
//! Shows service state (with start/stop/restart after an explicit
//! confirmation), live journald logs, and every live configuration value
//! with cross-checks between the database, the public env file and the
//! persistent runtime row. It reads the database read-only and never
//! prepares, signs, submits or cancels an order. The secret credential file
//! is only checked for existence, never read.
//!
//! ```text
//! Optional overrides:
//!   POLYCOPY_OPS_PUBLIC_ENV      default /etc/polycopy-engine/persistent-public.env
//!   POLYCOPY_OPS_SECRETS_FILE    default /etc/polycopy-engine/credentials/copy-secrets.env
//!   POLYCOPY_OPS_TRADING_CONFIG  default /etc/polycopy-engine/trading-config.json
//!                                (only read for leader display names)
//! ```
//!
//! The rendering and every decision about what to show live in
//! `copytrading::ops` and are unit-tested there; this file is the thin,
//! terminal- and systemd-bound loop around them.

#[cfg(feature = "ops_panel")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::{
        io::{BufRead, BufReader},
        process::{self, Child, Command, Stdio},
        sync::mpsc::{self, Receiver},
        time::Duration,
    };

    use chrono::{Local, Utc};
    use polycopy_engine::copytrading::{
        open_read_only,
        ops::{
            checks::{consistency_checks, Level},
            env_file::EnvFile,
            live_config::{display_names_from_json, load_live_config},
            services::{disk_usage, systemctl_action, systemctl_show, Action, UNITS},
            stats::{outcomes_last_24h, safety_state},
            ui::{draw, AppState, Page, Pending},
        },
    };
    use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
    use tokio::time::Instant;

    const MAX_LOG_LINES: usize = 3_000;
    const TICK: Duration = Duration::from_millis(200);
    const SERVICE_REFRESH: Duration = Duration::from_secs(2);
    const DATA_REFRESH: Duration = Duration::from_secs(5);

    fn path_from_env(key: &str, default: &str) -> String {
        std::env::var(key)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| default.to_owned())
    }

    struct Journal {
        child: Child,
        lines: Receiver<String>,
    }

    impl Journal {
        fn follow(unit: &str) -> Result<Self, String> {
            let mut child = Command::new("journalctl")
                .args(["-u", unit, "-f", "-n", "300", "-o", "short-iso", "--no-pager"])
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .map_err(|error| format!("无法运行 journalctl:{error}"))?;
            let stdout = child.stdout.take().ok_or("journalctl 没有输出")?;
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    if tx.send(line).is_err() {
                        break;
                    }
                }
            });
            Ok(Self { child, lines: rx })
        }
    }

    impl Drop for Journal {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    let public_env_path = path_from_env(
        "POLYCOPY_OPS_PUBLIC_ENV",
        "/etc/polycopy-engine/persistent-public.env",
    );
    let secrets_path = path_from_env(
        "POLYCOPY_OPS_SECRETS_FILE",
        "/etc/polycopy-engine/credentials/copy-secrets.env",
    );
    let trading_config_path = path_from_env(
        "POLYCOPY_OPS_TRADING_CONFIG",
        "/etc/polycopy-engine/trading-config.json",
    );

    let read_env = || -> Option<EnvFile> {
        std::fs::read_to_string(&public_env_path)
            .ok()
            .map(|text| EnvFile::parse(&text))
    };
    let Some(initial_env) = read_env() else {
        eprintln!("ops_panel:读不到 {public_env_path}(需要 root 权限,或用 POLYCOPY_OPS_PUBLIC_ENV 指定路径)");
        process::exit(3);
    };
    let Some(db_path) = initial_env.get("POLYCOPY_DB_PATH").map(str::to_owned) else {
        eprintln!("ops_panel:{public_env_path} 里没有 POLYCOPY_DB_PATH");
        process::exit(3);
    };
    let pool = match open_read_only(&db_path).await {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!("ops_panel:无法只读打开数据库 {db_path}:{error}");
            process::exit(3);
        }
    };

    let is_root = Command::new("id")
        .arg("-u")
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim() == "0")
        .unwrap_or(false);
    let mut state = AppState::new(is_root);
    if !is_root {
        state.message = Some((
            Level::Warn,
            "当前不是 root:可以查看,但启动/停止会失败,私钥和部分日志也看不到".to_owned(),
        ));
    }

    let mut terminal = match ratatui::try_init() {
        Ok(terminal) => terminal,
        Err(error) => {
            eprintln!("ops_panel:无法初始化终端:{error}");
            process::exit(3);
        }
    };

    let mut journal = Journal::follow(UNITS[0].unit).ok();
    let mut last_services = Instant::now() - SERVICE_REFRESH;
    let mut last_data = Instant::now() - DATA_REFRESH;

    let quit_reason: String = 'main: loop {
        if last_services.elapsed() >= SERVICE_REFRESH {
            last_services = Instant::now();
            for service in &mut state.services {
                match systemctl_show(service.unit.unit) {
                    Ok(status) => {
                        service.status = Some(status);
                        service.error = None;
                    }
                    Err(error) => service.error = Some(error),
                }
            }
            state.disk = disk_usage("/");
            state.clock = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        }

        if last_data.elapsed() >= DATA_REFRESH {
            last_data = Instant::now();
            let mut errors = Vec::new();
            match safety_state(&pool).await {
                Ok(safety) => state.safety = Some(safety),
                Err(error) => errors.push(format!("安全状态:{error}")),
            }
            match outcomes_last_24h(&pool, Utc::now()).await {
                Ok(outcomes) => state.outcomes = Some(outcomes),
                Err(error) => errors.push(format!("跟单统计:{error}")),
            }
            state.display_names = std::fs::read_to_string(&trading_config_path)
                .map(|text| display_names_from_json(&text))
                .unwrap_or_default();
            match load_live_config(&pool).await {
                Ok(live) => {
                    let secrets_present = match std::fs::metadata(&secrets_path) {
                        Ok(_) => Some(true),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Some(false),
                        Err(_) => None,
                    };
                    let env = read_env();
                    state.checks = consistency_checks(
                        &live,
                        env.as_ref(),
                        secrets_present,
                        &state.display_names,
                    );
                    if state.selected_leader >= live.leaders.len() {
                        state.selected_leader = live.leaders.len().saturating_sub(1);
                    }
                    state.live = Some(live);
                }
                Err(error) => errors.push(format!("配置:{error}")),
            }
            state.errors = errors;
        }

        if !state.log_paused {
            if let Some(journal) = &journal {
                while let Ok(line) = journal.lines.try_recv() {
                    state.push_log_line(line, MAX_LOG_LINES);
                }
            }
        }

        if let Err(error) = terminal.draw(|frame| draw(frame, &state)) {
            break 'main format!("绘制失败:{error}");
        }

        let key = match event::poll(TICK) {
            Ok(true) => match event::read() {
                Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => key,
                Ok(_) => continue,
                Err(error) => break 'main format!("读取按键失败:{error}"),
            },
            Ok(false) => continue,
            Err(error) => break 'main format!("读取按键失败:{error}"),
        };

        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            break 'main "已退出".to_owned();
        }

        if let Some(pending) = state.pending.clone() {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    state.pending = None;
                    let unit = state.services[pending.unit_index].unit;
                    state.message = Some(match systemctl_action(pending.action, unit.unit) {
                        Ok(()) => (
                            Level::Ok,
                            format!(
                                "已{}「{}」,几秒后状态会刷新",
                                pending.action.name(),
                                unit.name
                            ),
                        ),
                        Err(error) => (
                            Level::Error,
                            format!("{}「{}」失败:{error}", pending.action.name(), unit.name),
                        ),
                    });
                    last_services = Instant::now() - SERVICE_REFRESH;
                    last_data = Instant::now() - DATA_REFRESH;
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    state.pending = None;
                    state.message = Some((Level::Warn, "已取消".to_owned()));
                }
                _ => {}
            }
            continue;
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => break 'main "已退出".to_owned(),
            KeyCode::Char('1') => state.page = Page::Overview,
            KeyCode::Char('2') => state.page = Page::Logs,
            KeyCode::Char('3') => state.page = Page::Config,
            KeyCode::Tab => {
                state.page = match state.page {
                    Page::Overview => Page::Logs,
                    Page::Logs => Page::Config,
                    Page::Config => Page::Overview,
                }
            }
            code => match state.page {
                Page::Overview => {
                    let action = match code {
                        KeyCode::Up => {
                            state.selected_service = state.selected_service.saturating_sub(1);
                            None
                        }
                        KeyCode::Down => {
                            state.selected_service =
                                (state.selected_service + 1).min(state.services.len() - 1);
                            None
                        }
                        KeyCode::Char('s') => Some(Action::Start),
                        KeyCode::Char('t') => Some(Action::Stop),
                        KeyCode::Char('r') => Some(Action::Restart),
                        _ => None,
                    };
                    if let Some(action) = action {
                        let unit_index = state.selected_service;
                        let warnings = state.action_warnings(action, unit_index);
                        state.pending = Some(Pending {
                            action,
                            unit_index,
                            warnings,
                        });
                    }
                }
                Page::Logs => match code {
                    KeyCode::Left | KeyCode::Right => {
                        let count = state.services.len();
                        state.log_unit = if code == KeyCode::Left {
                            (state.log_unit + count - 1) % count
                        } else {
                            (state.log_unit + 1) % count
                        };
                        state.log_lines.clear();
                        state.log_scroll = 0;
                        drop(journal.take());
                        journal = match Journal::follow(state.services[state.log_unit].unit.unit) {
                            Ok(follower) => Some(follower),
                            Err(error) => {
                                state.message = Some((Level::Error, error));
                                None
                            }
                        };
                    }
                    KeyCode::Char('i') => state.important_only = !state.important_only,
                    KeyCode::Char(' ') => state.log_paused = !state.log_paused,
                    KeyCode::Up => state.log_scroll += 1,
                    KeyCode::Down => state.log_scroll = state.log_scroll.saturating_sub(1),
                    KeyCode::PageUp => state.log_scroll += 20,
                    KeyCode::PageDown => state.log_scroll = state.log_scroll.saturating_sub(20),
                    KeyCode::End => state.log_scroll = 0,
                    _ => {}
                },
                Page::Config => {
                    let count = state.live.as_ref().map_or(0, |live| live.leaders.len());
                    match code {
                        KeyCode::Up => state.selected_leader = state.selected_leader.saturating_sub(1),
                        KeyCode::Down if count > 0 => {
                            state.selected_leader = (state.selected_leader + 1).min(count - 1)
                        }
                        _ => {}
                    }
                }
            },
        }
        let max_scroll = state.log_lines.len();
        state.log_scroll = state.log_scroll.min(max_scroll);
    };

    drop(journal);
    ratatui::restore();
    eprintln!("ops_panel:{quit_reason}");
}

#[cfg(not(feature = "ops_panel"))]
fn main() {
    eprintln!("ops_panel requires the ops_panel feature");
    std::process::exit(2);
}
