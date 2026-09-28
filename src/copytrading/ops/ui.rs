//! Panel state and pure render functions (three pages plus a confirmation
//! dialog). No IO: the binary fills [`AppState`] and calls [`draw`].

use std::collections::{BTreeMap, VecDeque};

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Row, Table, Wrap},
    Frame,
};

use super::{
    checks::{worst_level, Check, Level},
    labels::{duration_zh, leader_field_rows, short_address, strategy_summary, Effect},
    live_config::LiveConfig,
    services::{describe, human_bytes, is_important_log, Action, ServiceStatus, Unit, UNITS},
    stats::{case_type_zh, OutcomeCounts, SafetyState},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    Overview,
    Logs,
    Config,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceView {
    pub unit: Unit,
    pub status: Option<ServiceStatus>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pending {
    pub action: Action,
    pub unit_index: usize,
    pub warnings: Vec<String>,
}

/// Where the config-change flow is; decides which keys are live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowStage {
    /// Edited file parsed and dry-run done: y apply / e edit again / n abandon.
    Preview,
    /// The edit could not be validated: e edit again / n abandon.
    Invalid,
    /// Steps are running; no keys.
    Applying,
    /// Applied; the trading service is stopped: y start it / n leave stopped.
    AskStart,
    /// A step failed after the service was stopped: r roll back / n leave it.
    Failed,
    /// Finished; any key closes.
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flow {
    pub stage: FlowStage,
    pub title: String,
    pub lines: Vec<(Level, String)>,
}

impl Flow {
    pub fn new(stage: FlowStage, title: impl Into<String>) -> Self {
        Self {
            stage,
            title: title.into(),
            lines: Vec::new(),
        }
    }

    pub fn push(&mut self, level: Level, text: impl Into<String>) {
        self.lines.push((level, text.into()));
    }

    pub fn prompt(&self) -> &'static str {
        match self.stage {
            FlowStage::Preview => "y 应用这些改动    e 继续编辑    n 放弃(不做任何改动)",
            FlowStage::Invalid => "e 重新编辑    n 放弃",
            FlowStage::Applying => "正在执行,请稍候…",
            FlowStage::AskStart => "y 现在启动交易服务    n 暂不启动",
            FlowStage::Failed => "r 回滚到修改前的配置    n 先不处理(交易服务保持停止)",
            FlowStage::Finished => "按任意键关闭",
        }
    }
}

pub struct AppState {
    pub page: Page,
    pub services: Vec<ServiceView>,
    pub selected_service: usize,
    pub disk: Option<(u8, u64)>,
    pub safety: Option<SafetyState>,
    pub outcomes: Option<OutcomeCounts>,
    pub live: Option<LiveConfig>,
    pub display_names: BTreeMap<String, String>,
    pub checks: Vec<Check>,
    pub selected_leader: usize,
    pub log_unit: usize,
    pub log_lines: VecDeque<String>,
    pub important_only: bool,
    pub log_paused: bool,
    /// How many lines above the newest the view is scrolled.
    pub log_scroll: usize,
    pub pending: Option<Pending>,
    pub flow: Option<Flow>,
    pub message: Option<(Level, String)>,
    pub errors: Vec<String>,
    pub clock: String,
    pub is_root: bool,
}

impl AppState {
    pub fn new(is_root: bool) -> Self {
        Self {
            page: Page::Overview,
            services: UNITS
                .iter()
                .map(|unit| ServiceView {
                    unit: *unit,
                    status: None,
                    error: None,
                })
                .collect(),
            selected_service: 0,
            disk: None,
            safety: None,
            outcomes: None,
            live: None,
            display_names: BTreeMap::new(),
            checks: Vec::new(),
            selected_leader: 0,
            log_unit: 0,
            log_lines: VecDeque::new(),
            important_only: false,
            log_paused: false,
            log_scroll: 0,
            pending: None,
            flow: None,
            message: None,
            errors: Vec::new(),
            clock: String::new(),
            is_root,
        }
    }

    pub fn display_name<'a>(&'a self, label: &'a str) -> &'a str {
        self.display_names
            .get(label)
            .map(String::as_str)
            .unwrap_or(label)
    }

    pub fn push_log_line(&mut self, line: String, cap: usize) {
        self.log_lines.push_back(line);
        while self.log_lines.len() > cap {
            self.log_lines.pop_front();
        }
    }

    /// The lines the log view should show, newest last, honouring the
    /// "important only" filter and the scroll offset.
    pub fn visible_log_lines(&self, height: usize) -> Vec<&str> {
        let filtered: Vec<&str> = self
            .log_lines
            .iter()
            .map(String::as_str)
            .filter(|line| !self.important_only || is_important_log(line))
            .collect();
        let end = filtered.len().saturating_sub(self.log_scroll);
        let start = end.saturating_sub(height);
        filtered[start..end].to_vec()
    }

    /// Consequences the owner should read before confirming `action`.
    pub fn action_warnings(&self, action: Action, unit_index: usize) -> Vec<String> {
        let mut warnings = Vec::new();
        if !self.is_root {
            warnings.push("当前不是 root 用户,这个操作会失败。请用 root 运行面板。".to_owned());
        }
        let is_trading = unit_index == 0;
        if is_trading && matches!(action, Action::Start | Action::Restart) {
            if let Some(safety) = &self.safety {
                if let Some((reason, _)) = &safety.fuse {
                    warnings.push(format!(
                        "保险丝已打开({reason}),启动后会立即退出。需要先处理异常并恢复保险丝。"
                    ));
                }
                if !safety.open_cases.is_empty() {
                    warnings.push(format!(
                        "还有 {} 个待处理的异常单,启动后会立即退出。",
                        safety.open_cases.len()
                    ));
                }
            }
            let errors = self.checks.iter().filter(|c| c.level == Level::Error).count();
            if errors > 0 {
                warnings.push(format!(
                    "配置检查有 {errors} 项不一致,启动会失败(详见配置页)。"
                ));
            }
        }
        if is_trading && matches!(action, Action::Stop | Action::Restart) {
            warnings.push("停止期间不会跟单,这段时间的 leader 信号会错过。".to_owned());
            warnings.push("已经挂出的 maker 单不会被撤销;重新启动后会继续跟踪它们。".to_owned());
        }
        if unit_index == 2 && action == Action::Stop {
            warnings.push("停止后不会再收到飞书通知。".to_owned());
        }
        warnings
    }
}

fn level_style(level: Level) -> Style {
    match level {
        Level::Ok => Style::default().fg(Color::Green),
        Level::Warn => Style::default().fg(Color::Yellow),
        Level::Error => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
    }
}

fn level_mark(level: Level) -> &'static str {
    match level {
        Level::Ok => "✓",
        Level::Warn => "!",
        Level::Error => "✗",
    }
}

fn boxed(title: &str) -> Block<'_> {
    Block::default().borders(Borders::ALL).title(format!(" {title} "))
}

pub fn draw(frame: &mut Frame, state: &AppState) {
    let area = frame.area();
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);

    draw_header(frame, state, rows[0]);
    match (&state.flow, state.page) {
        (Some(flow), _) => draw_flow(frame, flow, rows[1]),
        (None, Page::Overview) => draw_overview(frame, state, rows[1]),
        (None, Page::Logs) => draw_logs(frame, state, rows[1]),
        (None, Page::Config) => draw_config(frame, state, rows[1]),
    }
    draw_help(frame, state, rows[2]);
    draw_message(frame, state, rows[3]);
    if let Some(pending) = &state.pending {
        draw_confirm(frame, state, pending, area);
    }
}

fn draw_header(frame: &mut Frame, state: &AppState, area: Rect) {
    let tab = |page: Page, text: &'static str| {
        if state.page == page {
            Span::styled(
                text,
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            Span::raw(text)
        }
    };
    let line = Line::from(vec![
        Span::styled(" 跟单操作面板 ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("  "),
        tab(Page::Overview, " 1 总览 "),
        Span::raw(" "),
        tab(Page::Logs, " 2 日志 "),
        Span::raw(" "),
        tab(Page::Config, " 3 配置 "),
        Span::raw("    "),
        Span::raw(state.clock.clone()),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn draw_help(frame: &mut Frame, state: &AppState, area: Rect) {
    let text = if let Some(flow) = &state.flow {
        flow.prompt()
    } else if state.pending.is_some() {
        "y 确认执行    n / Esc 取消"
    } else {
        match state.page {
            Page::Overview => "↑↓ 选择服务   s 启动   t 停止   r 重启   1/2/3 切换页面   q 退出",
            Page::Logs => "←→ 切换服务   i 只看重要   空格 暂停/继续   ↑↓ PgUp PgDn 翻看   End 最新   q 退出",
            Page::Config => "↑↓ 选择 leader   e 修改配置   1/2/3 切换页面   q 退出",
        }
    };
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

fn draw_message(frame: &mut Frame, state: &AppState, area: Rect) {
    let line = if let Some((level, text)) = &state.message {
        Line::from(Span::styled(text.clone(), level_style(*level)))
    } else if !state.errors.is_empty() {
        Line::from(Span::styled(
            format!("刷新出错:{}", state.errors.join(" | ")),
            level_style(Level::Warn),
        ))
    } else {
        Line::from("")
    };
    frame.render_widget(Paragraph::new(line), area);
}

fn draw_overview(frame: &mut Frame, state: &AppState, area: Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(state.services.len() as u16 + 3),
            Constraint::Min(8),
        ])
        .split(area);

    let service_rows: Vec<Row> = state
        .services
        .iter()
        .enumerate()
        .map(|(index, service)| {
            let marker = if index == state.selected_service { "›" } else { " " };
            let (level, text) = match (&service.status, &service.error) {
                (_, Some(error)) => (Level::Warn, format!("读取失败:{error}")),
                (Some(status), None) => describe(status),
                (None, None) => (Level::Warn, "读取中…".to_owned()),
            };
            let status = service.status.as_ref();
            Row::new(vec![
                Line::from(format!("{marker} {}", service.unit.name)),
                Line::from(Span::styled(text, level_style(level))),
                Line::from(
                    status
                        .and_then(|s| s.release.clone())
                        .unwrap_or_else(|| "-".to_owned()),
                ),
                Line::from(
                    status
                        .and_then(|s| s.since.clone())
                        .unwrap_or_else(|| "-".to_owned()),
                ),
                Line::from(service.unit.description),
            ])
        })
        .collect();
    let table = Table::new(
        service_rows,
        [
            Constraint::Length(12),
            Constraint::Length(34),
            Constraint::Length(10),
            Constraint::Length(30),
            Constraint::Min(10),
        ],
    )
    .header(
        Row::new(vec!["  服务", "状态", "运行版本", "启动时间", "说明"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(boxed("服务"));
    frame.render_widget(table, rows[0]);

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(rows[1]);

    let mut safety_lines: Vec<Line> = Vec::new();
    match &state.safety {
        None => safety_lines.push(Line::from("读取中…")),
        Some(safety) => {
            safety_lines.push(match &safety.fuse {
                None => Line::from(vec![
                    Span::raw("保险丝:"),
                    Span::styled("正常", level_style(Level::Ok)),
                ]),
                Some((reason, at)) => Line::from(vec![
                    Span::raw("保险丝:"),
                    Span::styled(format!("已打开 — {reason}({at})"), level_style(Level::Error)),
                ]),
            });
            if safety.open_cases.is_empty() {
                safety_lines.push(Line::from(vec![
                    Span::raw("待处理异常单:"),
                    Span::styled("无", level_style(Level::Ok)),
                ]));
            } else {
                safety_lines.push(Line::from(Span::styled(
                    format!("待处理异常单:{} 个", safety.open_cases.len()),
                    level_style(Level::Error),
                )));
                for case in safety.open_cases.iter().take(4) {
                    safety_lines.push(Line::from(format!(
                        "  #{} {} 指令 {} 开于 {}",
                        case.id,
                        case_type_zh(&case.case_type),
                        case.intent_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "-".to_owned()),
                        case.opened_at
                    )));
                }
            }
            safety_lines.push(Line::from(format!(
                "最新 leader 信号:{}",
                safety.last_signal_at.as_deref().unwrap_or("-")
            )));
        }
    }
    let config_level = worst_level(&state.checks);
    let problems = state.checks.iter().filter(|c| c.level != Level::Ok).count();
    safety_lines.push(Line::from(vec![
        Span::raw("配置检查:"),
        if state.checks.is_empty() {
            Span::raw("读取中…")
        } else if problems == 0 {
            Span::styled("全部一致", level_style(Level::Ok))
        } else {
            Span::styled(
                format!("{problems} 项需要注意(见配置页)"),
                level_style(config_level),
            )
        },
    ]));
    safety_lines.push(match state.disk {
        Some((percent, free)) => {
            let level = if percent >= 90 {
                Level::Error
            } else if percent >= 80 {
                Level::Warn
            } else {
                Level::Ok
            };
            Line::from(vec![
                Span::raw("磁盘:"),
                Span::styled(
                    format!("已用 {percent}%,剩余 {}", human_bytes(free)),
                    level_style(level),
                ),
            ])
        }
        None => Line::from("磁盘:-"),
    });
    frame.render_widget(
        Paragraph::new(safety_lines)
            .wrap(Wrap { trim: false })
            .block(boxed("安全状态")),
        bottom[0],
    );

    let outcome_lines: Vec<Line> = match &state.outcomes {
        None => vec![Line::from("读取中…")],
        Some(o) => vec![
            Line::from(format!("收到信号  {}", o.signals)),
            Line::from(Span::styled(format!("已成交    {}", o.filled), level_style(Level::Ok))),
            Line::from(format!("挂单没成交 {}", o.unfilled)),
            Line::from(format!("超时取消  {}", o.deadline_expired)),
            Line::from(format!("被拒      {}", o.rejected)),
            Line::from(format!("处理中    {}", o.in_progress)),
            Line::from(format!("其他      {}", o.other)),
        ],
    };
    frame.render_widget(
        Paragraph::new(outcome_lines).block(boxed("最近 24 小时跟单")),
        bottom[1],
    );
}

fn draw_logs(frame: &mut Frame, state: &AppState, area: Rect) {
    let unit = state.services.get(state.log_unit).map(|s| s.unit.name).unwrap_or("-");
    let mut flags = Vec::new();
    if state.important_only {
        flags.push("只看重要");
    }
    if state.log_paused {
        flags.push("已暂停");
    }
    if state.log_scroll > 0 {
        flags.push("翻看中(End 回到最新)");
    }
    let title = if flags.is_empty() {
        format!("日志 · {unit}")
    } else {
        format!("日志 · {unit} · {}", flags.join(" · "))
    };
    let height = area.height.saturating_sub(2) as usize;
    let lines: Vec<Line> = state
        .visible_log_lines(height)
        .into_iter()
        .map(|line| {
            let style = if is_important_log(line) {
                let lower = line.to_ascii_lowercase();
                if lower.contains("fuse") || lower.contains("error") || lower.contains("failed") || lower.contains("panicked") {
                    level_style(Level::Error)
                } else if lower.contains("filled_qty") {
                    level_style(Level::Ok)
                } else {
                    level_style(Level::Warn)
                }
            } else {
                Style::default()
            };
            Line::from(Span::styled(line.to_owned(), style))
        })
        .collect();
    frame.render_widget(Paragraph::new(lines).block(boxed(&title)), area);
}

fn draw_config(frame: &mut Frame, state: &AppState, area: Rect) {
    let Some(live) = &state.live else {
        frame.render_widget(
            Paragraph::new("正在读取数据库配置…").block(boxed("配置")),
            area,
        );
        return;
    };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(9)])
        .split(area);
    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(36), Constraint::Min(40)])
        .split(rows[0]);

    let leader_lines: Vec<Line> = live
        .leaders
        .iter()
        .enumerate()
        .map(|(index, leader)| {
            let marker = if index == state.selected_leader { "›" } else { " " };
            let status = if leader.enabled {
                Span::styled("启用", level_style(Level::Ok))
            } else {
                Span::styled("停用", Style::default().fg(Color::DarkGray))
            };
            Line::from(vec![
                Span::raw(format!("{marker} {} #{} ", state.display_name(&leader.label), leader.id)),
                status,
            ])
        })
        .collect();
    frame.render_widget(
        Paragraph::new(leader_lines).block(boxed("Leader")),
        top[0],
    );

    match live.leaders.get(state.selected_leader) {
        None => frame.render_widget(Paragraph::new("没有 leader").block(boxed("详细参数")), top[1]),
        Some(leader) => {
            let detail = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(6), Constraint::Min(5)])
                .split(top[1]);
            let addresses = if leader.addresses.is_empty() {
                "无".to_owned()
            } else {
                leader
                    .addresses
                    .iter()
                    .map(|a| short_address(a))
                    .collect::<Vec<_>>()
                    .join("、")
            };
            let mut head = vec![
                Line::from(format!(
                    "显示名:{}    数据库名称:{}    编号:#{}",
                    state.display_name(&leader.label),
                    leader.label,
                    leader.id
                )),
                Line::from(vec![
                    Span::raw("状态:"),
                    if leader.enabled {
                        Span::styled("启用(正在跟单)", level_style(Level::Ok))
                    } else {
                        Span::styled("停用", Style::default().fg(Color::DarkGray))
                    },
                ]),
                Line::from(format!(
                    "跟踪钱包:{addresses}{}",
                    if leader.disabled_address_count > 0 {
                        format!("(另有 {} 个已停用)", leader.disabled_address_count)
                    } else {
                        String::new()
                    }
                )),
            ];
            if let Some(policy) = &leader.policy {
                head.push(Line::from(Span::styled(
                    format!("策略:{}", strategy_summary(policy)),
                    Style::default().add_modifier(Modifier::BOLD),
                )));
            }
            frame.render_widget(
                Paragraph::new(head).wrap(Wrap { trim: false }).block(boxed(state.display_name(&leader.label))),
                detail[0],
            );
            let field_rows: Vec<Row> = match &leader.policy {
                None => vec![Row::new(vec!["(没有交易参数)", "", ""])],
                Some(policy) => leader_field_rows(policy)
                    .into_iter()
                    .map(|field| {
                        let style = match field.effect {
                            Effect::Active => Style::default(),
                            Effect::Inactive => Style::default().fg(Color::DarkGray),
                        };
                        Row::new(vec![
                            Line::from(field.name),
                            Line::from(field.value),
                            Line::from(field.note),
                        ])
                        .style(style)
                    })
                    .collect(),
            };
            frame.render_widget(
                Table::new(
                    field_rows,
                    [Constraint::Length(20), Constraint::Length(20), Constraint::Min(20)],
                )
                .header(
                    Row::new(vec!["参数", "当前值", "说明(灰色 = 当前模式下不生效)"])
                        .style(Style::default().add_modifier(Modifier::BOLD)),
                )
                .block(boxed("详细参数")),
                detail[1],
            );
        }
    }

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(rows[1]);

    let mut account_lines = Vec::new();
    match live.account() {
        Some(account) => {
            account_lines.push(Line::from(format!("账户:{} (#{})", account.label, account.id)));
            account_lines.push(Line::from(format!("签名方式:{}", account.signature_type)));
            account_lines.push(Line::from(format!(
                "资金地址:{}",
                account
                    .funder_address
                    .as_deref()
                    .map(short_address)
                    .unwrap_or_else(|| "不需要".to_owned())
            )));
            account_lines.push(Line::from(format!(
                "签名地址:{}",
                short_address(&account.signing_address)
            )));
        }
        None => account_lines.push(Line::from("账户:无法确定")),
    }
    if let Some(runtime) = &live.runtime {
        account_lines.push(Line::from(format!(
            "账户单笔上限:{} USDC",
            runtime.max_order_notional_usdc
        )));
        account_lines.push(Line::from(format!(
            "账户滚动预算:{} USDC / {}",
            runtime.rolling_budget_usdc,
            duration_zh(runtime.budget_window_seconds)
        )));
        account_lines.push(Line::from(format!(
            "轮询间隔:{} 秒    补抓间隔:{}",
            runtime.tick_seconds,
            duration_zh(runtime.backfill_every_seconds)
        )));
    }
    frame.render_widget(
        Paragraph::new(account_lines).wrap(Wrap { trim: false }).block(boxed("账户与运行参数")),
        bottom[0],
    );

    let mut check_lines: Vec<Line> = state
        .checks
        .iter()
        .filter(|c| c.level != Level::Ok)
        .map(|c| {
            Line::from(Span::styled(
                format!("{} {}:{}", level_mark(c.level), c.title, c.detail),
                level_style(c.level),
            ))
        })
        .collect();
    let ok = state.checks.iter().filter(|c| c.level == Level::Ok).count();
    check_lines.push(Line::from(Span::styled(
        format!("✓ 其余 {ok} 项一致"),
        level_style(Level::Ok),
    )));
    frame.render_widget(
        Paragraph::new(check_lines)
            .wrap(Wrap { trim: false })
            .block(boxed("配置一致性检查(数据库 / 公开配置文件 / 运行参数)")),
        bottom[1],
    );
}

fn draw_flow(frame: &mut Frame, flow: &Flow, area: Rect) {
    let mut lines: Vec<Line> = flow
        .lines
        .iter()
        .map(|(level, text)| {
            let style = match level {
                Level::Ok => Style::default(),
                other => level_style(*other),
            };
            Line::from(Span::styled(text.clone(), style))
        })
        .collect();
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        flow.prompt(),
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
    )));
    // Keep the newest lines (and the prompt) visible on a short terminal.
    let height = area.height.saturating_sub(2) as usize;
    let skip = lines.len().saturating_sub(height);
    frame.render_widget(
        Paragraph::new(lines.into_iter().skip(skip).collect::<Vec<_>>())
            .wrap(Wrap { trim: false })
            .block(boxed(&flow.title)),
        area,
    );
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    }
}

fn draw_confirm(frame: &mut Frame, state: &AppState, pending: &Pending, area: Rect) {
    let name = state
        .services
        .get(pending.unit_index)
        .map(|s| s.unit.name)
        .unwrap_or("-");
    let mut lines = vec![
        Line::from(Span::styled(
            format!("确定要{}「{}」吗?", pending.action.name(), name),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];
    if pending.warnings.is_empty() {
        lines.push(Line::from("没有需要特别注意的事项。"));
    } else {
        for warning in &pending.warnings {
            lines.push(Line::from(Span::styled(
                format!("• {warning}"),
                level_style(Level::Warn),
            )));
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::from("按 y 确认执行,按 n 或 Esc 取消"));
    let height = lines.len() as u16 + 4;
    let rect = centered(area, 80, height);
    frame.render_widget(Clear, rect);
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(boxed("请确认").style(Style::default())),
        rect,
    );
}

#[cfg(test)]
mod tests {
    use ratatui::{backend::TestBackend, Terminal};

    use super::*;
    use crate::copytrading::ops::{
        live_config::{LiveAccount, LiveLeader, LivePolicy, LiveRuntime},
        services::parse_systemctl_show,
        stats::OpenCase,
    };

    fn render(state: &AppState) -> String {
        let mut terminal = Terminal::new(TestBackend::new(140, 42)).unwrap();
        terminal.draw(|frame| draw(frame, state)).unwrap();
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect()
    }

    fn has(rendered: &str, text: &str) -> bool {
        let needle: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        rendered.contains(&needle)
    }

    fn live() -> LiveConfig {
        LiveConfig {
            accounts: vec![LiveAccount {
                id: 1,
                label: "acct".into(),
                signing_address: "0x00000000000000000000000000000000000000aa".into(),
                funder_address: Some("0x00000000000000000000000000000000000000bb".into()),
                signature_type: "proxy".into(),
            }],
            leaders: vec![LiveLeader {
                id: 2,
                label: "leader2-fixed-5-shares".into(),
                enabled: true,
                addresses: vec!["0x00000000000000000000000000000000000000c2".into()],
                disabled_address_count: 0,
                policy: Some(LivePolicy {
                    max_signal_age_seconds: 30,
                    decision_window_seconds: 20,
                    price_tolerance_bps: 0,
                    tick_size: "0.01".into(),
                    min_price: "0.01".into(),
                    max_price: "0.99".into(),
                    max_order_notional: "10".into(),
                    min_leader_trade_size: "0".into(),
                    rolling_budget_usdc: Some("30".into()),
                    budget_window_seconds: Some(600),
                    max_order_shares: Some("10".into()),
                    balance_within_market: false,
                    price_tolerance_abs: "0.02".into(),
                    size_ratio: Some("0.2".into()),
                    allow_repeated_market_direction: true,
                    maker_only: true,
                }),
            }],
            runtime: Some(LiveRuntime {
                account_id: 1,
                enabled: true,
                allowed_leader_ids: "2".into(),
                max_order_notional_usdc: "10".into(),
                rolling_budget_usdc: "600".into(),
                budget_window_seconds: 86_400,
                tick_seconds: 1,
                backfill_every_seconds: 600,
            }),
        }
    }

    fn state() -> AppState {
        let mut state = AppState::new(true);
        state.services[0].status = Some(parse_systemctl_show(
            "ActiveState=active\nSubState=running\nActiveEnterTimestamp=Sat 2026-09-26 22:38:48 CST\nMainPID=1\nLoadState=loaded\n",
        ));
        state.services[0].status.as_mut().unwrap().release = Some("c0cd23d".into());
        state.services[1].status = Some(parse_systemctl_show(
            "ActiveState=failed\nSubState=failed\nExecMainStatus=21\nLoadState=loaded\n",
        ));
        state.disk = Some((43, 22 * 1024 * 1024 * 1024));
        state.safety = Some(SafetyState::default());
        state.outcomes = Some(OutcomeCounts {
            signals: 12,
            filled: 9,
            ..OutcomeCounts::default()
        });
        state.live = Some(live());
        state.display_names =
            BTreeMap::from([("leader2-fixed-5-shares".into(), "leader2-ratio-maker".into())]);
        state
    }

    #[test]
    fn overview_shows_services_safety_and_outcomes_in_chinese() {
        let rendered = render(&state());
        for text in ["跟单操作面板", "交易服务", "运行中", "c0cd23d", "盘口采样", "保险丝已打开", "保险丝:正常", "已用43%,剩余22.0GB", "已成交9"] {
            assert!(has(&rendered, text), "missing {text}: {rendered}");
        }
    }

    #[test]
    fn config_page_lists_every_parameter_with_effect_notes() {
        let mut state = state();
        state.page = Page::Config;
        let rendered = render(&state);
        for text in [
            "leader2-ratio-maker",
            "数据库名称:leader2-fixed-5-shares",
            "跟单比例",
            "20%",
            "固定下单份数",
            "已被跟单比例覆盖",
            "leader滚动预算",
            "30USDC/10分钟",
            "账户单笔上限:10USDC",
            "其余0项一致",
        ] {
            assert!(has(&rendered, text), "missing {text}: {rendered}");
        }
    }

    #[test]
    fn logs_page_filters_scrolls_and_labels_the_view() {
        let mut state = state();
        state.page = Page::Logs;
        for line in [
            "intent 1: post-only GTD remains on book",
            "intent 1: filled_qty=5",
            "activity ws: no activity within 60s",
        ] {
            state.push_log_line(line.to_owned(), 100);
        }
        assert_eq!(state.visible_log_lines(10).len(), 3);
        state.important_only = true;
        assert_eq!(state.visible_log_lines(10), vec!["intent 1: filled_qty=5"]);
        state.important_only = false;
        state.log_scroll = 1;
        assert_eq!(
            state.visible_log_lines(10),
            vec!["intent 1: post-only GTD remains on book", "intent 1: filled_qty=5"]
        );
        let rendered = render(&state);
        assert!(has(&rendered, "日志·交易服务·翻看中"), "{rendered}");
    }

    #[test]
    fn change_flow_takes_over_the_body_and_shows_its_prompt() {
        let mut state = state();
        state.page = Page::Config;
        let mut flow = Flow::new(FlowStage::Preview, "修改配置 · 预览");
        flow.push(Level::Ok, "试运行通过。改动如下:");
        flow.push(Level::Ok, "  • leader2-ratio-maker:跟单比例 20% → 30%");
        flow.push(Level::Error, "  • old:启用 → 停用");
        state.flow = Some(flow);
        let rendered = render(&state);
        for text in ["修改配置·预览", "跟单比例20%→30%", "启用→停用", "y应用这些改动", "e继续编辑"] {
            assert!(has(&rendered, text), "missing {text}: {rendered}");
        }
        assert!(!has(&rendered, "详细参数"), "config page should be hidden behind the flow");
        for stage in [FlowStage::Invalid, FlowStage::Applying, FlowStage::AskStart, FlowStage::Failed, FlowStage::Finished] {
            assert!(!Flow::new(stage, "x").prompt().is_empty());
        }
    }

    #[test]
    fn starting_with_an_open_fuse_or_case_warns_before_confirming() {
        let mut state = state();
        state.safety = Some(SafetyState {
            fuse: Some(("uncertain submission".into(), "2026-09-24T10:09:34Z".into())),
            open_cases: vec![OpenCase {
                id: 9,
                case_type: "unknown_submission".into(),
                intent_id: Some(734),
                opened_at: "2026-09-25".into(),
            }],
            last_signal_at: None,
        });
        state.checks = vec![Check {
            level: Level::Error,
            title: "x".into(),
            detail: "y".into(),
        }];
        let warnings = state.action_warnings(Action::Start, 0);
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        let stop = state.action_warnings(Action::Stop, 0);
        assert!(stop.iter().any(|w| w.contains("错过")));
        assert!(AppState::new(false)
            .action_warnings(Action::Restart, 1)
            .iter()
            .any(|w| w.contains("root")));

        state.pending = Some(Pending {
            action: Action::Start,
            unit_index: 0,
            warnings,
        });
        let rendered = render(&state);
        assert!(has(&rendered, "确定要启动「交易服务」吗?"), "{rendered}");
        assert!(has(&rendered, "按y确认执行"), "{rendered}");
    }
}
