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
//! persistent runtime row. On the config page, `e` changes configuration
//! through the single trading-config.json: the file is regenerated from the
//! database, edited in `$EDITOR` (nano by default), dry-run against a copy of
//! the database, previewed in Chinese, and only then applied (backup, stop,
//! `copy_config_apply`, regenerated env, `persistent_control reconfigure`,
//! verify, confirm start). The panel itself never prepares, signs, submits or
//! cancels an order, opens the database read-only, and never reads the
//! secret credential file (it is sourced only inside the `copy_config_apply`
//! child process, the same way an operator runs it by hand).
//!
//! ```text
//! Optional overrides:
//!   POLYCOPY_OPS_PUBLIC_ENV      default /etc/polycopy-engine/persistent-public.env
//!   POLYCOPY_OPS_SECRETS_FILE    default /etc/polycopy-engine/credentials/copy-secrets.env
//!   POLYCOPY_OPS_TRADING_CONFIG  default /etc/polycopy-engine/trading-config.json
//!   VISUAL / EDITOR              editor for the config file (default nano)
//! ```
//!
//! Every decision about what to show or whether an edit is valid lives in
//! `copytrading::ops` and is unit-tested there; this file is the thin loop
//! bound to a terminal, systemd and the filesystem.

#[cfg(feature = "ops_panel")]
mod panel {
    use std::{
        collections::BTreeMap,
        io::{BufRead, BufReader},
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        sync::mpsc::{self, Receiver},
        time::Duration,
    };

    use chrono::{Local, Utc};
    use polycopy_engine::copytrading::{
        open_read_only,
        ops::{
            checks::{consistency_checks, Level},
            edit::{
                dry_run, env_values_differ, generate_env, parse_unified, snapshot_database,
                write_atomically, DryRun,
            },
            env_file::EnvFile,
            live_config::{display_names_from_json, export_unified_json, load_live_config},
            services::{
                compact_journal_line, describe, disk_usage, run_with_env_files, systemctl_action,
                systemctl_show, Action, UNITS,
            },
            stats::{account_budget, outcomes_last_24h, safety_state},
            ui::{draw, AppState, Flow, FlowStage, Page, Pending},
        },
    };
    use ratatui::{
        crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
        DefaultTerminal,
    };
    use sqlx::SqlitePool;
    use tokio::time::Instant;

    const MAX_LOG_LINES: usize = 3_000;
    const TICK: Duration = Duration::from_millis(200);
    const SERVICE_REFRESH: Duration = Duration::from_secs(2);
    const DATA_REFRESH: Duration = Duration::from_secs(5);
    const TRADING_UNIT: &str = UNITS[0].unit;

    struct Paths {
        public_env: PathBuf,
        secrets: PathBuf,
        trading_config: PathBuf,
        edit_copy: PathBuf,
        archive_root: PathBuf,
        backup_root: PathBuf,
        bin_dir: PathBuf,
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

    /// What an in-progress edit needs across key presses.
    struct EditContext {
        /// Export of the database state before the edit: the true previous
        /// configuration, used for rollback (the file on disk may be stale).
        before_export: String,
        old_names: BTreeMap<String, String>,
        notices: Vec<(Level, String)>,
        session: Option<EditSession>,
        /// Set once the trading service was stopped by this edit.
        archive_dir: Option<PathBuf>,
    }

    struct EditSession {
        edited_text: String,
        dry: DryRun,
        generated_env: String,
        env_changed: bool,
        db_config_changed: bool,
    }

    fn path_from_env(key: &str, default: &str) -> PathBuf {
        PathBuf::from(
            std::env::var(key)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| default.to_owned()),
        )
    }

    fn read_env(path: &Path) -> Option<EnvFile> {
        std::fs::read_to_string(path)
            .ok()
            .map(|text| EnvFile::parse(&text))
    }

    fn redraw(terminal: &mut DefaultTerminal, state: &AppState) {
        let _ = terminal.draw(|frame| draw(frame, state));
    }

    fn flow_push(terminal: &mut DefaultTerminal, state: &mut AppState, level: Level, text: impl Into<String>) {
        if let Some(flow) = &mut state.flow {
            flow.push(level, text);
        }
        redraw(terminal, state);
    }

    async fn wait_for_state(unit: &str, wanted: &[&str], seconds: u64) -> bool {
        for _ in 0..seconds {
            if let Ok(status) = systemctl_show(unit) {
                if wanted.contains(&status.active_state.as_str()) {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        false
    }

    fn run_editor(terminal: &mut DefaultTerminal, path: &Path) -> Result<(), String> {
        ratatui::restore();
        let editor = std::env::var("VISUAL")
            .or_else(|_| std::env::var("EDITOR"))
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "nano".to_owned());
        let status = Command::new(&editor).arg(path).status();
        *terminal = ratatui::try_init().map_err(|error| format!("终端恢复失败:{error}"))?;
        match status {
            Ok(status) if status.success() => Ok(()),
            Ok(status) => Err(format!("编辑器 {editor} 异常退出({status})")),
            Err(error) => Err(format!("无法启动编辑器 {editor}:{error}")),
        }
    }

    /// `e` on the config page: regenerate the file from the database and
    /// open it in the editor.
    async fn begin_edit(
        terminal: &mut DefaultTerminal,
        state: &mut AppState,
        pool: &SqlitePool,
        paths: &Paths,
    ) -> Option<EditContext> {
        if !state.is_root {
            state.message = Some((Level::Error, "修改配置需要 root 权限".to_owned()));
            return None;
        }
        let live = match load_live_config(pool).await {
            Ok(live) => live,
            Err(error) => {
                state.message = Some((Level::Error, format!("读取数据库配置失败:{error}")));
                return None;
            }
        };
        let current_file = std::fs::read_to_string(&paths.trading_config).ok();
        let old_names = current_file
            .as_deref()
            .map(display_names_from_json)
            .unwrap_or_default();
        let before_export = match export_unified_json(&live, &old_names) {
            Ok(text) => text,
            Err(error) => {
                state.message = Some((Level::Error, error));
                return None;
            }
        };
        let mut notices = Vec::new();
        let stale = match current_file.as_deref().map(parse_unified) {
            None => Some("服务器上还没有 trading-config.json,已按数据库当前配置生成。"),
            Some(Err(_)) => Some("服务器上的 trading-config.json 无法解析,已按数据库当前配置重新生成。"),
            Some(Ok(on_disk)) => match parse_unified(&before_export) {
                Ok(fresh) if fresh.trading_config() != on_disk.trading_config() => Some(
                    "服务器上的 trading-config.json 已过时(和数据库不一致),本次以数据库当前配置为起点。",
                ),
                _ => None,
            },
        };
        if let Some(text) = stale {
            notices.push((Level::Warn, text.to_owned()));
        }
        if let Err(error) = write_atomically(&paths.edit_copy, &before_export, 0o600) {
            state.message = Some((Level::Error, error));
            return None;
        }
        let mut context = EditContext {
            before_export,
            old_names,
            notices,
            session: None,
            archive_dir: None,
        };
        edit_and_preview(terminal, state, pool, paths, &mut context).await;
        Some(context)
    }

    async fn edit_and_preview(
        terminal: &mut DefaultTerminal,
        state: &mut AppState,
        pool: &SqlitePool,
        paths: &Paths,
        context: &mut EditContext,
    ) {
        context.session = None;
        let mut flow = Flow::new(FlowStage::Invalid, "修改配置 · 预览");
        for (level, text) in &context.notices {
            flow.push(*level, text.clone());
        }
        if let Err(error) = run_editor(terminal, &paths.edit_copy) {
            flow.push(Level::Error, error);
            state.flow = Some(flow);
            return;
        }
        let edited_text = match std::fs::read_to_string(&paths.edit_copy) {
            Ok(text) => text,
            Err(error) => {
                flow.push(Level::Error, format!("读取编辑后的文件失败:{error}"));
                state.flow = Some(flow);
                return;
            }
        };
        let unified = match parse_unified(&edited_text) {
            Ok(unified) => unified,
            Err(error) => {
                flow.push(Level::Error, format!("文件有问题,没有做任何改动:{error}"));
                state.flow = Some(flow);
                return;
            }
        };
        flow.push(Level::Ok, "正在数据库副本上试运行这些改动…");
        state.flow = Some(flow.clone());
        redraw(terminal, state);
        let dry = match dry_run(pool, &unified, &context.old_names, &std::env::temp_dir()).await {
            Ok(dry) => dry,
            Err(error) => {
                flow.push(Level::Error, format!("试运行失败,没有做任何改动:{error}"));
                state.flow = Some(flow);
                return;
            }
        };
        flow.lines.pop();

        let current_env = read_env(&paths.public_env).unwrap_or_default();
        let stamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let Some(account) = dry.after.account().cloned() else {
            flow.push(Level::Error, "无法确定账户,没有做任何改动");
            state.flow = Some(flow);
            return;
        };
        let generated_env = generate_env(&current_env, &dry.runtime, &account, &stamp);
        let env_changed = env_values_differ(&current_env, &generated_env);
        let db_config_changed = dry.summary.account_change
            != polycopy_engine::copytrading::ChangeKind::Unchanged
            || dry
                .summary
                .leaders
                .iter()
                .any(|leader| leader.change != polycopy_engine::copytrading::ChangeKind::Unchanged);

        flow.push(Level::Ok, "试运行通过。改动如下:");
        for change in &dry.changes {
            flow.push(change.level, format!("  • {}", change.text));
        }
        if env_changed {
            flow.push(Level::Warn, "  • 公开配置文件 persistent-public.env 会同步更新");
        }
        flow.push(Level::Ok, "");
        if dry.needs_service_stop || env_changed {
            flow.push(
                Level::Warn,
                "应用时会先备份,再停止交易服务(大约 10–20 秒,期间的信号会错过),改完后等你确认再启动。",
            );
        } else {
            flow.push(Level::Ok, "只改了显示名,不需要停止交易服务。");
        }
        if let Some(safety) = &state.safety {
            if safety.fuse.is_some() || !safety.open_cases.is_empty() {
                flow.push(
                    Level::Error,
                    "注意:保险丝已打开或有未处理的异常单,改完配置后交易服务仍然无法启动,需要先处理异常。",
                );
            }
        }
        flow.stage = FlowStage::Preview;
        state.flow = Some(flow);
        context.session = Some(EditSession {
            edited_text,
            dry,
            generated_env,
            env_changed,
            db_config_changed,
        });
    }

    fn archive(paths: &Paths, context: &EditContext, session: &EditSession) -> Result<PathBuf, String> {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = paths
            .archive_root
            .join(Local::now().format("%Y%m%d-%H%M%S").to_string());
        std::fs::create_dir_all(&dir).map_err(|error| format!("无法创建备份目录:{error}"))?;
        let _ = std::fs::set_permissions(&paths.archive_root, std::fs::Permissions::from_mode(0o700));
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        write_atomically(&dir.join("trading-config.before.json"), &context.before_export, 0o600)?;
        if let Ok(text) = std::fs::read_to_string(&paths.trading_config) {
            write_atomically(&dir.join("trading-config.previous-file.json"), &text, 0o600)?;
        }
        if let Ok(text) = std::fs::read_to_string(&paths.public_env) {
            write_atomically(&dir.join("persistent-public.env"), &text, 0o600)?;
        }
        let changes = session
            .dry
            .changes
            .iter()
            .map(|change| format!("- {}", change.text))
            .collect::<Vec<_>>()
            .join("\n");
        write_atomically(&dir.join("CHANGES.txt"), &format!("{changes}\n"), 0o600)?;
        Ok(dir)
    }

    /// `y` on the preview: back up, stop, apply, sync, verify.
    async fn apply_edit(
        terminal: &mut DefaultTerminal,
        state: &mut AppState,
        pool: &SqlitePool,
        paths: &Paths,
        context: &mut EditContext,
    ) {
        let Some(session) = context.session.take() else {
            return;
        };
        let mut flow = Flow::new(FlowStage::Applying, "修改配置 · 正在应用");
        flow.push(Level::Ok, "开始应用改动:");
        state.flow = Some(flow);
        redraw(terminal, state);

        let archive_dir = match archive(paths, context, &session) {
            Ok(dir) => dir,
            Err(error) => return finish_without_changes(terminal, state, error),
        };
        flow_push(terminal, state, Level::Ok, format!("✓ 修改前的配置已备份到 {}", archive_dir.display()));
        context.archive_dir = Some(archive_dir.clone());

        if let Err(error) = std::fs::create_dir_all(&paths.backup_root) {
            return finish_without_changes(terminal, state, format!("无法创建数据库备份目录:{error}"));
        }
        let backup = paths.backup_root.join(format!(
            "polycopy-{}.sqlite",
            Local::now().format("%Y%m%d-%H%M%S")
        ));
        if let Err(error) = snapshot_database(pool, &backup).await {
            return finish_without_changes(terminal, state, error);
        }
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = std::fs::set_permissions(&backup, std::fs::Permissions::from_mode(0o600));
        }
        flow_push(terminal, state, Level::Ok, format!("✓ 数据库已备份到 {}", backup.display()));

        let stops = session.dry.needs_service_stop || session.env_changed;
        if stops {
            flow_push(terminal, state, Level::Warn, "… 正在停止交易服务");
            if let Err(error) = systemctl_action(Action::Stop, TRADING_UNIT) {
                return finish_without_changes(terminal, state, format!("停止交易服务失败:{error}"));
            }
            if !wait_for_state(TRADING_UNIT, &["inactive", "failed"], 30).await {
                return fail_after_stop(terminal, state, "交易服务 30 秒内没有停下来");
            }
            flow_push(terminal, state, Level::Ok, "✓ 交易服务已停止");
        }

        if let Err(error) = write_atomically(&paths.trading_config, &session.edited_text, 0o640) {
            return fail_after_stop(terminal, state, error);
        }
        flow_push(terminal, state, Level::Ok, format!("✓ 已写入 {}", paths.trading_config.display()));

        if session.db_config_changed {
            let ceiling = parse_unified(&session.edited_text)
                .and_then(|unified| unified.account_cap())
                .map(|cap| cap.normalize().to_string());
            let ceiling = match ceiling {
                Ok(ceiling) => ceiling,
                Err(error) => return fail_after_stop(terminal, state, error),
            };
            flow_push(terminal, state, Level::Warn, "… 正在把配置写入数据库(copy_config_apply)");
            match run_with_env_files(
                &paths.public_env,
                Some(&paths.secrets),
                &[
                    ("POLYCOPY_SETUP_CONFIG", paths.trading_config.to_string_lossy().to_string()),
                    ("POLYCOPY_CONFIG_MAX_NOTIONAL_CEILING", ceiling),
                ],
                &paths.bin_dir.join("copy_config_apply"),
                &[],
            ) {
                Ok(output) => {
                    flow_push(terminal, state, Level::Ok, "✓ 数据库配置已更新");
                    for line in output.lines().filter(|l| !l.starts_with("CONFIG_APPLIED")).take(8) {
                        flow_push(terminal, state, Level::Ok, format!("    {line}"));
                    }
                }
                Err(error) => return fail_after_stop(terminal, state, format!("copy_config_apply 失败:{error}")),
            }
        }

        if session.env_changed {
            if let Err(error) = write_atomically(&paths.public_env, &session.generated_env, 0o644) {
                return fail_after_stop(terminal, state, error);
            }
            flow_push(terminal, state, Level::Ok, "✓ 公开配置文件已同步");
        }

        if session.env_changed || session.dry.before.runtime != session.dry.after.runtime {
            flow_push(terminal, state, Level::Warn, "… 正在同步运行参数(persistent_control reconfigure)");
            if let Err(error) = run_with_env_files(
                &paths.public_env,
                None,
                &[],
                &paths.bin_dir.join("persistent_control"),
                &["reconfigure"],
            ) {
                return fail_after_stop(terminal, state, format!("reconfigure 失败:{error}"));
            }
            flow_push(terminal, state, Level::Ok, "✓ 运行参数已同步");
        }

        match verify(pool, paths, state).await {
            Ok(()) => flow_push(terminal, state, Level::Ok, "✓ 复查通过:数据库、公开配置文件、运行参数三处一致"),
            Err(problems) => {
                for problem in problems {
                    flow_push(terminal, state, Level::Error, format!("✗ {problem}"));
                }
                return fail_after_stop(terminal, state, "复查发现不一致");
            }
        }
        let _ = std::fs::remove_file(&paths.edit_copy);

        if let Some(flow) = &mut state.flow {
            flow.title = "修改配置 · 完成".to_owned();
            if stops {
                flow.push(Level::Warn, "配置已生效。交易服务目前是停止状态。");
                flow.stage = FlowStage::AskStart;
            } else {
                flow.push(Level::Ok, "配置已生效,交易服务没有被打断。");
                flow.stage = FlowStage::Finished;
            }
        }
    }

    async fn verify(
        pool: &SqlitePool,
        paths: &Paths,
        state: &mut AppState,
    ) -> Result<(), Vec<String>> {
        let live = load_live_config(pool).await.map_err(|error| vec![error.to_string()])?;
        let env = read_env(&paths.public_env);
        let names = std::fs::read_to_string(&paths.trading_config)
            .map(|text| display_names_from_json(&text))
            .unwrap_or_default();
        let secrets = std::fs::metadata(&paths.secrets).map(|_| true).ok();
        let checks = consistency_checks(&live, env.as_ref(), secrets, &names);
        let problems: Vec<String> = checks
            .iter()
            .filter(|check| check.level == Level::Error)
            .map(|check| format!("{}:{}", check.title, check.detail))
            .collect();
        state.display_names = names;
        state.checks = checks;
        state.live = Some(live);
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems)
        }
    }

    fn finish_without_changes(terminal: &mut DefaultTerminal, state: &mut AppState, error: String) {
        if let Some(flow) = &mut state.flow {
            flow.push(Level::Error, format!("✗ {error}"));
            flow.push(Level::Ok, "没有做任何改动,交易服务没有被打断。");
            flow.stage = FlowStage::Finished;
        }
        redraw(terminal, state);
    }

    fn fail_after_stop(terminal: &mut DefaultTerminal, state: &mut AppState, error: impl Into<String>) {
        if let Some(flow) = &mut state.flow {
            flow.title = "修改配置 · 出错".to_owned();
            flow.push(Level::Error, format!("✗ {}", error.into()));
            flow.push(
                Level::Error,
                "交易服务目前是停止状态。可以按 r 回滚到修改前的配置(按数据库修改前的状态恢复),再决定是否启动。",
            );
            flow.stage = FlowStage::Failed;
        }
        redraw(terminal, state);
    }

    /// `r` after a failure: re-apply the pre-edit configuration and env.
    async fn rollback(
        terminal: &mut DefaultTerminal,
        state: &mut AppState,
        pool: &SqlitePool,
        paths: &Paths,
        context: &EditContext,
    ) {
        if let Some(flow) = &mut state.flow {
            flow.title = "修改配置 · 回滚".to_owned();
            flow.stage = FlowStage::Applying;
        }
        let Some(dir) = &context.archive_dir else {
            flow_push(terminal, state, Level::Error, "✗ 找不到备份目录,无法回滚");
            if let Some(flow) = &mut state.flow {
                flow.stage = FlowStage::Finished;
            }
            return;
        };
        let mut ok = true;
        if let Ok(text) = std::fs::read_to_string(dir.join("persistent-public.env")) {
            if let Err(error) = write_atomically(&paths.public_env, &text, 0o644) {
                flow_push(terminal, state, Level::Error, format!("✗ {error}"));
                ok = false;
            } else {
                flow_push(terminal, state, Level::Ok, "✓ 公开配置文件已恢复");
            }
        }
        if let Err(error) = write_atomically(&paths.trading_config, &context.before_export, 0o640) {
            flow_push(terminal, state, Level::Error, format!("✗ {error}"));
            ok = false;
        }
        let ceiling = parse_unified(&context.before_export)
            .and_then(|unified| unified.account_cap())
            .map(|cap| cap.normalize().to_string())
            .unwrap_or_else(|_| "10".to_owned());
        match run_with_env_files(
            &paths.public_env,
            Some(&paths.secrets),
            &[
                ("POLYCOPY_SETUP_CONFIG", paths.trading_config.to_string_lossy().to_string()),
                ("POLYCOPY_CONFIG_MAX_NOTIONAL_CEILING", ceiling),
            ],
            &paths.bin_dir.join("copy_config_apply"),
            &[],
        ) {
            Ok(_) => flow_push(terminal, state, Level::Ok, "✓ 数据库配置已恢复"),
            Err(error) => {
                flow_push(terminal, state, Level::Error, format!("✗ 恢复数据库配置失败:{error}"));
                ok = false;
            }
        }
        match run_with_env_files(
            &paths.public_env,
            None,
            &[],
            &paths.bin_dir.join("persistent_control"),
            &["reconfigure"],
        ) {
            Ok(_) => flow_push(terminal, state, Level::Ok, "✓ 运行参数已恢复"),
            Err(error) => {
                flow_push(terminal, state, Level::Error, format!("✗ 恢复运行参数失败:{error}"));
                ok = false;
            }
        }
        match verify(pool, paths, state).await {
            Ok(()) => flow_push(terminal, state, Level::Ok, "✓ 复查通过"),
            Err(problems) => {
                ok = false;
                for problem in problems {
                    flow_push(terminal, state, Level::Error, format!("✗ {problem}"));
                }
            }
        }
        if let Some(flow) = &mut state.flow {
            if ok {
                flow.push(Level::Warn, "已回滚到修改前的配置。交易服务目前是停止状态。");
                flow.stage = FlowStage::AskStart;
            } else {
                flow.push(
                    Level::Error,
                    format!("回滚没有完全成功,请不要启动交易服务,把以上信息发给维护人员。备份在 {}", dir.display()),
                );
                flow.stage = FlowStage::Finished;
            }
        }
    }

    async fn start_trading(terminal: &mut DefaultTerminal, state: &mut AppState) {
        if let Some(flow) = &mut state.flow {
            flow.stage = FlowStage::Applying;
        }
        flow_push(terminal, state, Level::Warn, "… 正在启动交易服务");
        let result = systemctl_action(Action::Start, TRADING_UNIT);
        tokio::time::sleep(Duration::from_secs(6)).await;
        let status = systemctl_show(TRADING_UNIT).ok();
        let (level, text) = match (&result, &status) {
            (Err(error), _) => (Level::Error, format!("✗ 启动命令失败:{error}")),
            (Ok(()), Some(status)) => {
                let (level, text) = describe(status);
                (level, format!("交易服务:{text}"))
            }
            (Ok(()), None) => (Level::Warn, "已发送启动命令,但读不到状态".to_owned()),
        };
        flow_push(terminal, state, level, text);
        if let Some(flow) = &mut state.flow {
            flow.stage = FlowStage::Finished;
        }
    }

    pub async fn run() -> Result<String, String> {
        let public_env = path_from_env(
            "POLYCOPY_OPS_PUBLIC_ENV",
            "/etc/polycopy-engine/persistent-public.env",
        );
        let initial_env = read_env(&public_env).ok_or_else(|| {
            format!(
                "读不到 {}(需要 root 权限,或用 POLYCOPY_OPS_PUBLIC_ENV 指定路径)",
                public_env.display()
            )
        })?;
        let db_path = initial_env
            .get("POLYCOPY_DB_PATH")
            .map(PathBuf::from)
            .ok_or_else(|| format!("{} 里没有 POLYCOPY_DB_PATH", public_env.display()))?;
        let pool = open_read_only(&db_path)
            .await
            .map_err(|error| format!("无法只读打开数据库 {}:{error}", db_path.display()))?;
        let trading_config = path_from_env(
            "POLYCOPY_OPS_TRADING_CONFIG",
            "/etc/polycopy-engine/trading-config.json",
        );
        let config_dir = trading_config
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("/etc/polycopy-engine"));
        let paths = Paths {
            secrets: path_from_env(
                "POLYCOPY_OPS_SECRETS_FILE",
                "/etc/polycopy-engine/credentials/copy-secrets.env",
            ),
            edit_copy: config_dir.join("trading-config.edit.json"),
            archive_root: config_dir.join("archive"),
            backup_root: db_path
                .parent()
                .map(|dir| dir.join("backups"))
                .unwrap_or_else(|| PathBuf::from("/var/lib/polycopy-engine/backups")),
            bin_dir: std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(Path::to_path_buf))
                .unwrap_or_else(|| PathBuf::from("/opt/polycopy-engine/current/target/release")),
            trading_config,
            public_env,
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
                "当前不是 root:可以查看,但启动/停止/修改配置会失败,部分日志也看不到".to_owned(),
            ));
        }

        let mut terminal =
            ratatui::try_init().map_err(|error| format!("无法初始化终端:{error}"))?;
        let mut journal = Journal::follow(UNITS[0].unit).ok();
        let mut edit: Option<EditContext> = None;
        let mut last_services = Instant::now() - SERVICE_REFRESH;
        let mut last_data = Instant::now() - DATA_REFRESH;

        let reason = 'main: loop {
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

            if last_data.elapsed() >= DATA_REFRESH && state.flow.is_none() {
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
                state.display_names = std::fs::read_to_string(&paths.trading_config)
                    .map(|text| display_names_from_json(&text))
                    .unwrap_or_default();
                match load_live_config(&pool).await {
                    Ok(live) => {
                        let secrets_present = match std::fs::metadata(&paths.secrets) {
                            Ok(_) => Some(true),
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Some(false),
                            Err(_) => None,
                        };
                        let env = read_env(&paths.public_env);
                        state.checks =
                            consistency_checks(&live, env.as_ref(), secrets_present, &state.display_names);
                        match &live.runtime {
                            Some(runtime) => match account_budget(&pool, runtime, Utc::now()).await {
                                Ok(budget) => state.account_budget = Some(budget),
                                Err(error) => errors.push(format!("账户额度:{error}")),
                            },
                            None => state.account_budget = None,
                        }
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
                    let today = Local::now().format("%Y-%m-%d").to_string();
                    while let Ok(line) = journal.lines.try_recv() {
                        state.push_log_line(compact_journal_line(&line, &today), MAX_LOG_LINES);
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
            let ctrl_c = key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c');

            // The config-change flow owns the keyboard while it is open.
            if let Some(stage) = state.flow.as_ref().map(|flow| flow.stage) {
                match (stage, key.code) {
                    (FlowStage::Applying, _) => {}
                    (FlowStage::Preview, KeyCode::Char('y')) => {
                        if let Some(context) = &mut edit {
                            apply_edit(&mut terminal, &mut state, &pool, &paths, context).await;
                        }
                    }
                    (FlowStage::Preview | FlowStage::Invalid, KeyCode::Char('e')) => {
                        if let Some(context) = &mut edit {
                            edit_and_preview(&mut terminal, &mut state, &pool, &paths, context).await;
                        }
                    }
                    (FlowStage::Preview | FlowStage::Invalid, KeyCode::Char('n') | KeyCode::Esc) => {
                        let _ = std::fs::remove_file(&paths.edit_copy);
                        state.flow = None;
                        edit = None;
                        state.message = Some((Level::Warn, "已放弃修改,没有做任何改动".to_owned()));
                    }
                    (FlowStage::AskStart, KeyCode::Char('y')) => {
                        start_trading(&mut terminal, &mut state).await;
                    }
                    (FlowStage::AskStart, KeyCode::Char('n')) => {
                        if let Some(flow) = &mut state.flow {
                            flow.push(Level::Warn, "交易服务保持停止。可以之后在总览页按 s 启动。");
                            flow.stage = FlowStage::Finished;
                        }
                    }
                    (FlowStage::Failed, KeyCode::Char('r')) => {
                        if let Some(context) = &edit {
                            rollback(&mut terminal, &mut state, &pool, &paths, context).await;
                        }
                    }
                    (FlowStage::Failed, KeyCode::Char('n')) => {
                        if let Some(flow) = &mut state.flow {
                            flow.push(Level::Error, "交易服务保持停止,配置可能处于修改到一半的状态。");
                            flow.stage = FlowStage::Finished;
                        }
                    }
                    (FlowStage::Finished, _) => {
                        state.flow = None;
                        edit = None;
                        last_services = Instant::now() - SERVICE_REFRESH;
                        last_data = Instant::now() - DATA_REFRESH;
                    }
                    _ if ctrl_c && stage != FlowStage::Applying => {
                        break 'main "已退出".to_owned();
                    }
                    _ => {}
                }
                continue;
            }

            if ctrl_c {
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
                                format!("已{}「{}」,几秒后状态会刷新", pending.action.name(), unit.name),
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
                            KeyCode::Char('e') => {
                                edit = begin_edit(&mut terminal, &mut state, &pool, &paths).await;
                            }
                            _ => {}
                        }
                    }
                },
            }
            state.log_scroll = state.log_scroll.min(state.log_lines.len());
        };

        drop(journal);
        ratatui::restore();
        Ok(reason)
    }
}

#[cfg(feature = "ops_panel")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    match panel::run().await {
        Ok(reason) => eprintln!("ops_panel:{reason}"),
        Err(error) => {
            ratatui::restore();
            eprintln!("ops_panel:{error}");
            std::process::exit(3);
        }
    }
}

#[cfg(not(feature = "ops_panel"))]
fn main() {
    eprintln!("ops_panel requires the ops_panel feature");
    std::process::exit(2);
}
