//! systemd service state, start/stop/restart, disk space, and log-line
//! triage for the panel.
//!
//! Parsing is pure and tested; the thin `Command` wrappers are only used by
//! the binary. Actions are never taken here on their own: the binary calls
//! [`systemctl_action`] only after the owner confirms in the UI.

use std::process::Command;

use super::checks::Level;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unit {
    pub unit: &'static str,
    pub name: &'static str,
    pub description: &'static str,
}

/// The services the owner operates. The trading service is first because it
/// is the one that matters; the others are auxiliary.
pub const UNITS: [Unit; 3] = [
    Unit {
        unit: "polycopy-engine-persistent.service",
        name: "交易服务",
        description: "跟单下单的主程序",
    },
    Unit {
        unit: "polycopy-engine-book-sampler.service",
        name: "盘口采样",
        description: "记录 leader 成交后的盘口变化,只读,不下单",
    },
    Unit {
        unit: "polycopy-engine-notify.service",
        name: "飞书通知",
        description: "把成交、未跟上等消息推送到飞书",
    },
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceStatus {
    pub active_state: String,
    pub sub_state: String,
    pub since: Option<String>,
    pub main_pid: Option<u32>,
    pub exit_status: Option<i32>,
    pub load_state: String,
    /// Short commit of the release the running binary was started from.
    pub release: Option<String>,
}

pub const SHOW_PROPERTIES: &str =
    "ActiveState,SubState,ActiveEnterTimestamp,MainPID,ExecMainStatus,LoadState";

/// Parses `systemctl show -p <SHOW_PROPERTIES> <unit>` output.
pub fn parse_systemctl_show(text: &str) -> ServiceStatus {
    let mut status = ServiceStatus::default();
    for line in text.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim();
        match key.trim() {
            "ActiveState" => status.active_state = value.to_owned(),
            "SubState" => status.sub_state = value.to_owned(),
            "LoadState" => status.load_state = value.to_owned(),
            "ActiveEnterTimestamp" if !value.is_empty() => status.since = Some(value.to_owned()),
            "MainPID" => status.main_pid = value.parse().ok().filter(|pid| *pid > 0),
            "ExecMainStatus" => status.exit_status = value.parse().ok(),
            _ => {}
        }
    }
    status
}

/// Meaning of `copy_persistent`'s own exit codes (`persistent::EXIT_*`), in
/// the owner's terms. systemd does not restart the service after these.
pub fn exit_code_meaning(code: i32) -> Option<&'static str> {
    match code {
        20 => Some("已有另一个交易进程在用数据库"),
        21 => Some("保险丝已打开,需要先处理异常再恢复"),
        22 => Some("配置不一致或无效,见配置页的检查结果"),
        23 => Some("有未处理完的异常单,需要先对账"),
        24 => Some("触发了预算限制"),
        _ => None,
    }
}

/// Human state and a severity for the overview.
pub fn describe(status: &ServiceStatus) -> (Level, String) {
    if status.load_state == "not-found" {
        return (Level::Warn, "未安装".to_owned());
    }
    match status.active_state.as_str() {
        "active" => (Level::Ok, "运行中".to_owned()),
        "activating" if status.sub_state == "auto-restart" => (
            Level::Error,
            "反复重启中(启动后很快退出)".to_owned(),
        ),
        "activating" => (Level::Warn, "正在启动".to_owned()),
        "deactivating" => (Level::Warn, "正在停止".to_owned()),
        "failed" => {
            let reason = status
                .exit_status
                .map(|code| match exit_code_meaning(code) {
                    Some(meaning) => format!("已停止(退出码 {code}:{meaning})"),
                    None => format!("已停止(退出码 {code})"),
                })
                .unwrap_or_else(|| "已停止(异常)".to_owned());
            (Level::Error, reason)
        }
        "inactive" => (Level::Warn, "已停止".to_owned()),
        "" => (Level::Warn, "状态未知".to_owned()),
        other => (Level::Warn, other.to_owned()),
    }
}

/// `/opt/polycopy-engine/releases/<commit>/target/release/x` -> `<commit>`
/// shortened to 7 characters (other release directory names are kept whole).
pub fn release_from_exe(path: &str) -> Option<String> {
    let rest = path.split("/releases/").nth(1)?;
    let name = rest.split('/').next()?;
    if name.is_empty() {
        return None;
    }
    if name.len() == 40 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(name[..7].to_owned())
    } else {
        Some(name.to_owned())
    }
}

/// Lines worth the owner's attention in "只看重要" mode: fills, rejections,
/// cancellations, fuse/exit/errors, and lookup retries. The websocket
/// keep-alive churn and routine backfill counters are hidden.
pub fn is_important_log(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    const NOISE: [&str; 5] = [
        "no activity within",
        "ws_event",
        "reconnecting in",
        "backfill leader",
        "execution paused until reconnect",
    ];
    if NOISE.iter().any(|noise| lower.contains(noise)) {
        return false;
    }
    const SIGNAL: [&str; 14] = [
        "filled_qty",
        "rejected",
        "non-submitted",
        "expired",
        "fuse",
        "error",
        "failed",
        "exited",
        "panicked",
        "capped",
        "lookup retry",
        "lookup failed",
        "started ",
        "stopped ",
    ];
    SIGNAL.iter().any(|signal| lower.contains(signal))
}

/// Parses the data line of `df -Pk <path>` into (used percent, free bytes).
pub fn parse_df(text: &str) -> Option<(u8, u64)> {
    let line = text.lines().nth(1)?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() < 5 {
        return None;
    }
    let free_kib: u64 = fields[3].parse().ok()?;
    let percent: u8 = fields[4].trim_end_matches('%').parse().ok()?;
    Some((percent, free_kib * 1024))
}

pub fn human_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    let value = bytes as f64;
    if value >= GIB {
        format!("{:.1} GB", value / GIB)
    } else {
        format!("{:.0} MB", value / MIB)
    }
}

// ---- thin IO wrappers used by the binary -------------------------------

pub fn systemctl_show(unit: &str) -> Result<ServiceStatus, String> {
    let output = Command::new("systemctl")
        .args(["show", "-p", SHOW_PROPERTIES, unit])
        .output()
        .map_err(|error| format!("无法运行 systemctl:{error}"))?;
    let mut status = parse_systemctl_show(&String::from_utf8_lossy(&output.stdout));
    if let Some(pid) = status.main_pid {
        if let Ok(exe) = std::fs::read_link(format!("/proc/{pid}/exe")) {
            status.release = release_from_exe(&exe.to_string_lossy());
        }
    }
    Ok(status)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Start,
    Stop,
    Restart,
}

impl Action {
    pub fn verb(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Start => "启动",
            Self::Stop => "停止",
            Self::Restart => "重启",
        }
    }
}

pub fn systemctl_action(action: Action, unit: &str) -> Result<(), String> {
    let output = Command::new("systemctl")
        .args([action.verb(), unit])
        .output()
        .map_err(|error| format!("无法运行 systemctl:{error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

pub fn disk_usage(path: &str) -> Option<(u8, u64)> {
    let output = Command::new("df").args(["-Pk", path]).output().ok()?;
    parse_df(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_show_output_and_describes_states() {
        let running = parse_systemctl_show(
            "ActiveState=active\nSubState=running\nActiveEnterTimestamp=Sat 2026-09-26 22:38:48 CST\n\
             MainPID=3616653\nExecMainStatus=0\nLoadState=loaded\n",
        );
        assert_eq!(running.main_pid, Some(3_616_653));
        assert_eq!(describe(&running), (Level::Ok, "运行中".to_owned()));

        let fused = parse_systemctl_show(
            "ActiveState=failed\nSubState=failed\nMainPID=0\nExecMainStatus=21\nLoadState=loaded\n",
        );
        assert_eq!(fused.main_pid, None);
        let (level, text) = describe(&fused);
        assert_eq!(level, Level::Error);
        assert!(text.contains("保险丝"));

        let looping = parse_systemctl_show("ActiveState=activating\nSubState=auto-restart\nLoadState=loaded\n");
        assert_eq!(describe(&looping).0, Level::Error);

        let missing = parse_systemctl_show("ActiveState=inactive\nLoadState=not-found\n");
        assert_eq!(describe(&missing).1, "未安装");
    }

    #[test]
    fn release_is_read_from_the_binary_path() {
        assert_eq!(
            release_from_exe("/opt/polycopy-engine/releases/c0cd23df4d3c340da3872677b361b98cc1d0cf20/target/release/copy_persistent"),
            Some("c0cd23d".to_owned())
        );
        assert_eq!(
            release_from_exe("/opt/polycopy-engine/releases/overfill-fix-linux/target/release/x"),
            Some("overfill-fix-linux".to_owned())
        );
        assert_eq!(release_from_exe("/usr/bin/python3.12"), None);
    }

    #[test]
    fn important_filter_keeps_trades_and_drops_keepalive_noise() {
        assert!(is_important_log("intent 861: filled_qty=17.2"));
        assert!(is_important_log("intent 787: non-submitted outcome"));
        assert!(is_important_log("persistent execution fuse is open"));
        assert!(is_important_log("intent 5: size_ratio capped from 60 shares to 20 shares"));
        assert!(!is_important_log("activity ws: subscription delivered no activity message within 60s"));
        assert!(!is_important_log("WS_EVENT: {\"kind\":\"connected\"}"));
        assert!(!is_important_log("backfill leader 2: fetched=1 ingested=1 rejected=0"));
        assert!(!is_important_log("intent 861: post-only GTD remains on book"));
    }

    #[test]
    fn parses_df_and_formats_sizes() {
        let text = "Filesystem 1024-blocks Used Available Capacity Mounted on\n\
                    /dev/vda3 41152736 16777216 23068672 43% /\n";
        let (percent, free) = parse_df(text).unwrap();
        assert_eq!(percent, 43);
        assert_eq!(human_bytes(free), "22.0 GB");
        assert_eq!(parse_df("garbage"), None);
    }
}
