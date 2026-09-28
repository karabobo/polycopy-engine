//! Cross-checks between the three places configuration lives today: the
//! database (`leader_config`/`leader_policy`/`accounts`), the non-secret
//! runtime env file, and the `persistent_execution_config` row.
//!
//! The runtime comparison reuses `PersistentRuntimeConfig::from_values`, the
//! same parser `copy_persistent` applies to its env and to the database row
//! before comparing them (`persistent::verify_config`), so a red line here
//! means the service really would refuse to start -- not a stylistic nit.

use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use super::{
    env_file::EnvFile,
    labels::short_address,
    live_config::LiveConfig,
};
use crate::copytrading::persistent::{
    PersistentRuntimeConfig, ACCOUNT_ID_ENV, ALLOWED_LEADERS_ENV, BACKFILL_SECONDS_ENV,
    BUDGET_WINDOW_ENV, ENGINE_EXECUTE_ENV, EXECUTE_ENV, MAX_ORDER_NOTIONAL_ENV,
    ROLLING_BUDGET_ENV, TICK_SECONDS_ENV,
};

pub const SIGNATURE_TYPE_ENV: &str = "POLYCOPY_CLOB_SIGNATURE_TYPE";
pub const FUNDER_ENV: &str = "POLYCOPY_CLOB_FUNDER";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Ok,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    pub level: Level,
    pub title: String,
    pub detail: String,
}

fn check(level: Level, title: &str, detail: impl Into<String>) -> Check {
    Check {
        level,
        title: title.to_owned(),
        detail: detail.into(),
    }
}

const WILL_NOT_START: &str = "交易服务启动时会因为配置不一致而拒绝运行";

/// `secrets_present`: `None` when the panel could not look (not root).
pub fn consistency_checks(
    live: &LiveConfig,
    env: Option<&EnvFile>,
    secrets_present: Option<bool>,
    display_names: &BTreeMap<String, String>,
) -> Vec<Check> {
    let mut checks = Vec::new();
    let name_of = |id: i64| -> String {
        match live.leader_label(id) {
            Some(label) => {
                let shown = display_names.get(label).map(String::as_str).unwrap_or(label);
                format!("{shown}(#{id})")
            }
            None => format!("#{id}(数据库里不存在)"),
        }
    };
    let names = |ids: &BTreeSet<i64>| -> String {
        if ids.is_empty() {
            "无".to_owned()
        } else {
            ids.iter().map(|id| name_of(*id)).collect::<Vec<_>>().join("、")
        }
    };

    let env_runtime = match env {
        None => {
            checks.push(check(
                Level::Error,
                "公开配置文件",
                "读不到 persistent-public.env,无法核对运行参数",
            ));
            None
        }
        Some(env) => {
            let switches_on = env.get(ENGINE_EXECUTE_ENV) == Some("yes")
                && env.get(EXECUTE_ENV) == Some("yes");
            checks.push(if switches_on {
                check(Level::Ok, "交易总开关", "已打开")
            } else {
                check(
                    Level::Error,
                    "交易总开关",
                    format!("{ENGINE_EXECUTE_ENV} 和 {EXECUTE_ENV} 都必须是 yes,否则交易服务会立即退出"),
                )
            });
            match runtime_from_env(env) {
                Ok(runtime) => Some(runtime),
                Err(error) => {
                    checks.push(check(
                        Level::Error,
                        "公开配置文件里的运行参数",
                        format!("无效:{error}"),
                    ));
                    None
                }
            }
        }
    };

    let db_runtime = match &live.runtime {
        None => {
            checks.push(check(
                Level::Error,
                "数据库运行参数",
                "还没有初始化(需要 persistent_control init-config)",
            ));
            None
        }
        Some(row) => match PersistentRuntimeConfig::from_values(
            row.account_id,
            row.enabled,
            &row.allowed_leader_ids,
            &row.max_order_notional_usdc,
            &row.rolling_budget_usdc,
            row.budget_window_seconds.max(0) as u64,
            row.tick_seconds.max(0) as u64,
            row.backfill_every_seconds.max(0) as u64,
        ) {
            Ok(runtime) => {
                if !runtime.enabled {
                    checks.push(check(Level::Error, "数据库运行参数", "被停用,交易服务不会运行"));
                }
                Some(runtime)
            }
            Err(error) => {
                checks.push(check(Level::Error, "数据库运行参数", format!("无效:{error}")));
                None
            }
        },
    };

    if let (Some(env_rt), Some(db_rt)) = (&env_runtime, &db_runtime) {
        let mut compare = |title: &str, env_value: String, db_value: String| {
            checks.push(if env_value == db_value {
                check(Level::Ok, title, env_value)
            } else {
                check(
                    Level::Error,
                    title,
                    format!("配置文件是 {env_value},数据库是 {db_value}:{WILL_NOT_START}"),
                )
            });
        };
        compare(
            "账户编号",
            env_rt.account_id.to_string(),
            db_rt.account_id.to_string(),
        );
        compare(
            "账户单笔上限(USDC)",
            env_rt.max_order_notional.normalize().to_string(),
            db_rt.max_order_notional.normalize().to_string(),
        );
        compare(
            "账户滚动预算(USDC)",
            env_rt.rolling_budget.normalize().to_string(),
            db_rt.rolling_budget.normalize().to_string(),
        );
        compare(
            "账户预算窗口(秒)",
            env_rt.budget_window.as_secs().to_string(),
            db_rt.budget_window.as_secs().to_string(),
        );
        compare(
            "轮询间隔(秒)",
            env_rt.tick.as_secs().to_string(),
            db_rt.tick.as_secs().to_string(),
        );
        compare(
            "补抓间隔(秒)",
            env_rt.backfill_every.as_secs().to_string(),
            db_rt.backfill_every.as_secs().to_string(),
        );
        if env_rt.allowed_leader_ids != db_rt.allowed_leader_ids {
            checks.push(check(
                Level::Error,
                "允许运行的 leader",
                format!(
                    "配置文件是 {},数据库是 {}:{WILL_NOT_START}",
                    names(&env_rt.allowed_leader_ids),
                    names(&db_rt.allowed_leader_ids)
                ),
            ));
        }
    }

    let enabled: BTreeSet<i64> = live.enabled_leader_ids().into_iter().collect();
    if let Some(runtime) = env_runtime.as_ref().or(db_runtime.as_ref()) {
        checks.push(if runtime.allowed_leader_ids == enabled {
            check(Level::Ok, "正在跟的 leader", names(&enabled))
        } else {
            check(
                Level::Error,
                "正在跟的 leader",
                format!(
                    "运行参数允许 {},但启用的是 {}:{WILL_NOT_START}",
                    names(&runtime.allowed_leader_ids),
                    names(&enabled)
                ),
            )
        });

        if !live.accounts.iter().any(|a| a.id == runtime.account_id) {
            checks.push(check(
                Level::Error,
                "账户",
                format!("运行参数指向账户 #{},但数据库里没有这个账户", runtime.account_id),
            ));
        }

        for leader in live.leaders.iter().filter(|l| l.enabled) {
            let Some(policy) = &leader.policy else {
                checks.push(check(
                    Level::Error,
                    "leader 交易参数",
                    format!("{} 没有交易参数", name_of(leader.id)),
                ));
                continue;
            };
            match policy.max_order_notional.parse::<Decimal>() {
                Ok(cap) if cap > runtime.max_order_notional => checks.push(check(
                    Level::Error,
                    "leader 单笔上限",
                    format!(
                        "{} 的单笔上限 {} 超过账户单笔上限 {}:它的信号会让交易服务报配置不一致而停下",
                        name_of(leader.id),
                        cap.normalize(),
                        runtime.max_order_notional.normalize()
                    ),
                )),
                Ok(_) => {}
                Err(_) => checks.push(check(
                    Level::Error,
                    "leader 单笔上限",
                    format!("{} 的单笔上限不是有效数字", name_of(leader.id)),
                )),
            }
        }
    }

    if let (Some(env), Some(account)) = (env, live.account()) {
        match env.get(SIGNATURE_TYPE_ENV) {
            Some(value) if value.trim().eq_ignore_ascii_case(&account.signature_type) => {
                checks.push(check(Level::Ok, "签名方式", account.signature_type.clone()))
            }
            Some(value) => checks.push(check(
                Level::Error,
                "签名方式",
                format!("配置文件是 {value},数据库账户是 {}", account.signature_type),
            )),
            None => checks.push(check(
                Level::Error,
                "签名方式",
                format!("公开配置文件缺少 {SIGNATURE_TYPE_ENV}"),
            )),
        }
        let env_funder = env.get(FUNDER_ENV).map(|v| v.trim().to_ascii_lowercase());
        match (env_funder.as_deref(), account.funder_address.as_deref()) {
            (Some(env_value), Some(db_value)) if env_value == db_value => checks.push(check(
                Level::Ok,
                "资金地址",
                short_address(db_value),
            )),
            (None, None) => checks.push(check(Level::Ok, "资金地址", "不需要(eoa 签名)")),
            (env_value, db_value) => checks.push(check(
                Level::Error,
                "资金地址",
                format!(
                    "配置文件是 {},数据库账户是 {}",
                    env_value.map(short_address).unwrap_or_else(|| "未设置".to_owned()),
                    db_value.map(short_address).unwrap_or_else(|| "未设置".to_owned())
                ),
            )),
        }
    }

    checks.push(match secrets_present {
        Some(true) => check(Level::Ok, "私钥", "已配置(内容不显示)"),
        Some(false) => check(Level::Error, "私钥", "私钥文件不存在,交易服务无法启动"),
        None => check(Level::Warn, "私钥", "没有权限检查私钥文件(请用 root 运行面板)"),
    });

    checks
}

fn runtime_from_env(env: &EnvFile) -> Result<PersistentRuntimeConfig, String> {
    let required = |key: &str| -> Result<&str, String> {
        env.get(key)
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| format!("缺少 {key}"))
    };
    let number = |key: &str| -> Result<u64, String> {
        required(key)?
            .trim()
            .parse()
            .map_err(|_| format!("{key} 不是有效的正整数"))
    };
    let account_id: i64 = required(ACCOUNT_ID_ENV)?
        .trim()
        .parse()
        .map_err(|_| format!("{ACCOUNT_ID_ENV} 不是有效的整数"))?;
    PersistentRuntimeConfig::from_values(
        account_id,
        true,
        required(ALLOWED_LEADERS_ENV)?,
        required(MAX_ORDER_NOTIONAL_ENV)?,
        required(ROLLING_BUDGET_ENV)?,
        number(BUDGET_WINDOW_ENV)?,
        number(TICK_SECONDS_ENV)?,
        number(BACKFILL_SECONDS_ENV)?,
    )
    .map_err(|error| error.to_string())
}

pub fn worst_level(checks: &[Check]) -> Level {
    checks.iter().map(|c| c.level).max().unwrap_or(Level::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::copytrading::ops::live_config::{
        LiveAccount, LiveConfig, LiveLeader, LivePolicy, LiveRuntime,
    };

    const FUNDER: &str = "0x00000000000000000000000000000000000000bb";

    fn policy(max: &str) -> LivePolicy {
        LivePolicy {
            max_signal_age_seconds: 30,
            decision_window_seconds: 20,
            price_tolerance_bps: 0,
            tick_size: "0.01".into(),
            min_price: "0.01".into(),
            max_price: "0.99".into(),
            max_order_notional: max.into(),
            min_leader_trade_size: "0".into(),
            rolling_budget_usdc: None,
            budget_window_seconds: None,
            max_order_shares: None,
            balance_within_market: false,
            price_tolerance_abs: "0.02".into(),
            size_ratio: Some("0.2".into()),
            allow_repeated_market_direction: true,
            maker_only: true,
        }
    }

    fn live() -> LiveConfig {
        LiveConfig {
            accounts: vec![LiveAccount {
                id: 1,
                label: "acct".into(),
                signing_address: "0x00000000000000000000000000000000000000aa".into(),
                funder_address: Some(FUNDER.into()),
                signature_type: "proxy".into(),
            }],
            leaders: vec![
                LiveLeader {
                    id: 1,
                    label: "old".into(),
                    enabled: false,
                    addresses: vec!["0x1".into()],
                    disabled_address_count: 0,
                    policy: Some(policy("10")),
                },
                LiveLeader {
                    id: 2,
                    label: "leader2-fixed-5-shares".into(),
                    enabled: true,
                    addresses: vec!["0x2".into()],
                    disabled_address_count: 0,
                    policy: Some(policy("10")),
                },
            ],
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

    fn env(allowed: &str, max: &str, funder: &str) -> EnvFile {
        EnvFile::parse(&format!(
            "POLYCOPY_ENGINE_EXECUTE=yes\nPOLYCOPY_PERSISTENT_EXECUTE=yes\n\
             POLYCOPY_PERSISTENT_ACCOUNT_ID=1\nPOLYCOPY_PERSISTENT_ALLOWED_LEADER_IDS={allowed}\n\
             POLYCOPY_PERSISTENT_MAX_ORDER_NOTIONAL={max}\nPOLYCOPY_PERSISTENT_ROLLING_BUDGET_USDC=600\n\
             POLYCOPY_PERSISTENT_BUDGET_WINDOW_SECONDS=86400\nPOLYCOPY_PERSISTENT_TICK_SECONDS=1\n\
             POLYCOPY_PERSISTENT_BACKFILL_EVERY_SECONDS=600\nPOLYCOPY_CLOB_SIGNATURE_TYPE=proxy\n\
             POLYCOPY_CLOB_FUNDER={funder}\n"
        ))
    }

    fn names() -> BTreeMap<String, String> {
        BTreeMap::from([("leader2-fixed-5-shares".into(), "leader2-ratio-maker".into())])
    }

    #[test]
    fn matching_production_shape_is_all_green() {
        let checks = consistency_checks(&live(), Some(&env("2", "10", FUNDER)), Some(true), &names());
        let red: Vec<_> = checks.iter().filter(|c| c.level != Level::Ok).collect();
        assert!(red.is_empty(), "unexpected problems: {red:?}");
        assert!(checks
            .iter()
            .any(|c| c.title == "正在跟的 leader" && c.detail.contains("leader2-ratio-maker(#2)")));
    }

    #[test]
    fn env_allowing_a_disabled_leader_is_reported_as_a_start_failure() {
        let checks = consistency_checks(&live(), Some(&env("1,2", "10", FUNDER)), Some(true), &names());
        assert_eq!(worst_level(&checks), Level::Error);
        assert!(checks.iter().any(|c| c.title == "允许运行的 leader" && c.level == Level::Error));
        assert!(checks.iter().any(|c| c.title == "正在跟的 leader" && c.level == Level::Error));
    }

    #[test]
    fn cap_mismatch_funder_mismatch_and_missing_secrets_are_errors() {
        let checks = consistency_checks(
            &live(),
            Some(&env("2", "5", "0x00000000000000000000000000000000000000cc")),
            Some(false),
            &names(),
        );
        for title in ["账户单笔上限(USDC)", "资金地址", "私钥", "leader 单笔上限"] {
            assert!(
                checks.iter().any(|c| c.title == title && c.level == Level::Error),
                "{title} not flagged: {checks:?}"
            );
        }
    }

    #[test]
    fn missing_env_and_missing_runtime_row_are_reported_not_panicked() {
        let mut config = live();
        config.runtime = None;
        let checks = consistency_checks(&config, None, None, &names());
        assert!(checks.iter().any(|c| c.title == "公开配置文件" && c.level == Level::Error));
        assert!(checks.iter().any(|c| c.title == "数据库运行参数" && c.level == Level::Error));
        assert!(checks.iter().any(|c| c.title == "私钥" && c.level == Level::Warn));
    }

    #[test]
    fn switches_off_are_an_error() {
        let text = "POLYCOPY_ENGINE_EXECUTE=no\n".to_owned();
        let checks = consistency_checks(&live(), Some(&EnvFile::parse(&text)), Some(true), &names());
        assert!(checks.iter().any(|c| c.title == "交易总开关" && c.level == Level::Error));
    }
}
