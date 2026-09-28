//! Stage 2 of the panel: changing configuration through one owner-edited
//! file, with everything that could fail checked *before* the trading
//! service is stopped.
//!
//! The edited unified JSON (account, `runtime`, leaders with `display_name`)
//! is validated by running the two real mutation functions --
//! `apply_trading_config` and `persistent::reconfigure_config` -- against a
//! throwaway copy of the live database. Their errors, the resulting
//! configuration and a Chinese change list come back as a [`DryRun`]; only if
//! that succeeds does the binary stop the service and run the same steps for
//! real (`copy_config_apply`, a regenerated env file, `persistent_control
//! reconfigure`). A typo in a field name is an error here, never silently
//! dropped: serde's default is to ignore unknown fields, which would turn a
//! misspelt setting into "no change" without a word.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use sqlx::SqlitePool;

use super::{
    checks::{Level, FUNDER_ENV, SIGNATURE_TYPE_ENV},
    env_file::EnvFile,
    labels::{duration_zh, leader_field_rows, short_address},
    live_config::{load_live_config, LiveAccount, LiveConfig},
};
use crate::copytrading::{
    apply_trading_config,
    persistent::{
        reconfigure_config, PersistentRuntimeConfig, ACCOUNT_ID_ENV, ALLOWED_LEADERS_ENV,
        BACKFILL_SECONDS_ENV, BUDGET_WINDOW_ENV, MAX_ORDER_NOTIONAL_ENV, ROLLING_BUDGET_ENV,
        TICK_SECONDS_ENV,
    },
    AccountConfigInput, ChangeKind, ConfigApplyOptions, ConfigApplySummary, LeaderConfigInput,
    LeaderPolicyInput, TradingConfig,
};

const ACCOUNT_KEYS: [&str; 3] = ["label", "signature_type", "funder_address"];
const RUNTIME_KEYS: [&str; 5] = [
    "max_order_notional_usdc",
    "rolling_budget_usdc",
    "budget_window_seconds",
    "tick_seconds",
    "backfill_every_seconds",
];
const LEADER_KEYS: [&str; 5] = ["label", "display_name", "enabled", "addresses", "policy"];
const POLICY_KEYS: [&str; 16] = [
    "max_signal_age_seconds",
    "decision_window_seconds",
    "price_tolerance_bps",
    "tick_size",
    "min_price",
    "max_price",
    "max_order_notional",
    "min_leader_trade_size",
    "rolling_budget_usdc",
    "budget_window_seconds",
    "max_order_shares",
    "balance_within_market",
    "price_tolerance_abs",
    "size_ratio",
    "allow_repeated_market_direction",
    "maker_only",
];

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeInput {
    pub max_order_notional_usdc: String,
    pub rolling_budget_usdc: String,
    pub budget_window_seconds: u64,
    pub tick_seconds: u64,
    pub backfill_every_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UnifiedLeader {
    pub label: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub addresses: Vec<String>,
    pub policy: LeaderPolicyInput,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct UnifiedConfig {
    pub account: AccountConfigInput,
    pub runtime: Option<RuntimeInput>,
    pub leaders: Vec<UnifiedLeader>,
}

fn unknown_keys(value: &Value, allowed: &[&str], place: &str) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Err(format!("{place} 必须是一个 {{ … }} 对象"));
    };
    let unknown: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|key| !allowed.contains(key))
        .collect();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{place} 里有不认识的字段:{}(可能拼错了)",
            unknown.join("、")
        ))
    }
}

/// Parses the owner-edited file, refusing any field name the engine would
/// not read.
pub fn parse_unified(text: &str) -> Result<UnifiedConfig, String> {
    let value: Value =
        serde_json::from_str(text).map_err(|error| format!("JSON 格式错误:{error}"))?;
    unknown_keys(&value, &["account", "runtime", "leaders"], "文件最外层")?;
    if let Some(account) = value.get("account") {
        unknown_keys(account, &ACCOUNT_KEYS, "account")?;
    }
    if let Some(runtime) = value.get("runtime") {
        unknown_keys(runtime, &RUNTIME_KEYS, "runtime")?;
    }
    if let Some(leaders) = value.get("leaders").and_then(Value::as_array) {
        for (index, leader) in leaders.iter().enumerate() {
            let name = leader
                .get("label")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("第 {} 个 leader", index + 1));
            unknown_keys(leader, &LEADER_KEYS, &format!("leader {name}"))?;
            if let Some(policy) = leader.get("policy") {
                unknown_keys(policy, &POLICY_KEYS, &format!("leader {name} 的 policy"))?;
            }
        }
    }
    serde_json::from_value(value).map_err(|error| format!("配置内容不完整或类型不对:{error}"))
}

impl UnifiedConfig {
    pub fn trading_config(&self) -> TradingConfig {
        TradingConfig {
            account: self.account.clone(),
            leaders: self
                .leaders
                .iter()
                .map(|leader| LeaderConfigInput {
                    label: leader.label.clone(),
                    enabled: leader.enabled,
                    addresses: leader.addresses.clone(),
                    policy: leader.policy.clone(),
                })
                .collect(),
        }
    }

    pub fn display_names(&self) -> BTreeMap<String, String> {
        self.leaders
            .iter()
            .filter_map(|leader| {
                leader
                    .display_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .map(|name| (leader.label.trim().to_owned(), name.to_owned()))
            })
            .collect()
    }

    /// The account-level per-order cap, which is also the ceiling
    /// `copy_config_apply` must be given for the leader caps.
    pub fn account_cap(&self) -> Result<Decimal, String> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or("缺少 runtime 段(账户级运行参数)")?;
        runtime
            .max_order_notional_usdc
            .trim()
            .parse()
            .map_err(|_| "runtime.max_order_notional_usdc 不是有效数字".to_owned())
    }

    pub fn runtime_for(
        &self,
        account_id: i64,
        enabled: &BTreeSet<i64>,
    ) -> Result<PersistentRuntimeConfig, String> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or("缺少 runtime 段(账户级运行参数)")?;
        if enabled.is_empty() {
            return Err("至少要启用一个 leader".to_owned());
        }
        let allowed = enabled
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        PersistentRuntimeConfig::from_values(
            account_id,
            true,
            &allowed,
            &runtime.max_order_notional_usdc,
            &runtime.rolling_budget_usdc,
            runtime.budget_window_seconds,
            runtime.tick_seconds,
            runtime.backfill_every_seconds,
        )
        .map_err(|error| format!("runtime 参数无效:{error}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub level: Level,
    pub text: String,
}

fn change(level: Level, text: impl Into<String>) -> Change {
    Change {
        level,
        text: text.into(),
    }
}

#[derive(Debug)]
pub struct DryRun {
    pub before: LiveConfig,
    pub after: LiveConfig,
    pub summary: ConfigApplySummary,
    pub runtime: PersistentRuntimeConfig,
    pub changes: Vec<Change>,
    /// Anything the trading service reads changed (database rows or runtime
    /// parameters): applying requires stopping it.
    pub needs_service_stop: bool,
}

/// Consistent copy of the live database via `VACUUM INTO`.
pub async fn snapshot_database(pool: &SqlitePool, dest: &Path) -> Result<(), String> {
    if dest.exists() {
        return Err(format!("{} 已存在,不覆盖", dest.display()));
    }
    sqlx::query("VACUUM INTO ?")
        .bind(dest.to_string_lossy().to_string())
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|error| format!("数据库备份失败:{error}"))
}

fn remove_database_files(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

async fn open_lot_counts(pool: &SqlitePool) -> Result<BTreeMap<i64, i64>, String> {
    let rows: Vec<(i64, String)> =
        sqlx::query_as("SELECT leader_id, qty FROM position_lots")
            .fetch_all(pool)
            .await
            .map_err(|error| error.to_string())?;
    let mut counts = BTreeMap::new();
    for (leader_id, qty) in rows {
        if qty.parse::<Decimal>().is_ok_and(|qty| qty > Decimal::ZERO) {
            *counts.entry(leader_id).or_insert(0) += 1;
        }
    }
    Ok(counts)
}

/// Runs the real apply + reconfigure against a copy of `pool` in
/// `scratch_dir` and describes the result. The live database is only read.
pub async fn dry_run(
    pool: &SqlitePool,
    unified: &UnifiedConfig,
    old_names: &BTreeMap<String, String>,
    scratch_dir: &Path,
) -> Result<DryRun, String> {
    let before = load_live_config(pool)
        .await
        .map_err(|error| format!("读取当前配置失败:{error}"))?;
    let account = before.account().ok_or("无法确定当前账户")?.clone();
    if unified.account.label.trim() != account.label {
        return Err(format!(
            "账户名称从 {} 改成了 {}:这会新建一个账户而不是修改现有账户,不允许。请改回原名称。",
            account.label, unified.account.label
        ));
    }
    let ceiling = unified.account_cap()?;
    let lots = open_lot_counts(pool).await?;

    let copy: PathBuf = scratch_dir.join(format!(
        "polycopy-ops-dryrun-{}-{}.sqlite",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    snapshot_database(pool, &copy).await?;
    let result = async {
        let copy_pool = crate::copytrading::db::open(&copy)
            .await
            .map_err(|error| format!("打开数据库副本失败:{error}"))?;
        let outcome = async {
            let summary = apply_trading_config(
                &copy_pool,
                &unified.trading_config(),
                &account.signing_address,
                &ConfigApplyOptions {
                    max_notional_ceiling: ceiling,
                },
            )
            .await
            .map_err(|error| format!("配置校验失败:{error}"))?;
            let applied = load_live_config(&copy_pool)
                .await
                .map_err(|error| error.to_string())?;
            let enabled: BTreeSet<i64> = applied.enabled_leader_ids().into_iter().collect();
            let runtime = unified.runtime_for(account.id, &enabled)?;
            if applied.runtime.is_none() {
                return Err("数据库还没有运行参数(需要先 persistent_control init-config)".to_owned());
            }
            reconfigure_config(&copy_pool, &runtime)
                .await
                .map_err(|error| format!("运行参数无法生效:{error}"))?;
            let after = load_live_config(&copy_pool)
                .await
                .map_err(|error| error.to_string())?;
            Ok((summary, runtime, after))
        }
        .await;
        copy_pool.close().await;
        outcome
    }
    .await;
    remove_database_files(&copy);
    let (summary, runtime, after) = result?;

    let new_names = unified.display_names();
    let changes = describe_changes(&before, &after, old_names, &new_names, &lots);
    let leaders_changed = summary.account_change != ChangeKind::Unchanged
        || summary
            .leaders
            .iter()
            .any(|leader| leader.change != ChangeKind::Unchanged);
    let needs_service_stop = leaders_changed || before.runtime != after.runtime;
    Ok(DryRun {
        before,
        after,
        summary,
        runtime,
        changes,
        needs_service_stop,
    })
}

fn shown<'a>(names: &'a BTreeMap<String, String>, label: &'a str) -> &'a str {
    names.get(label).map(String::as_str).unwrap_or(label)
}

/// Chinese, one line per change, dangerous ones flagged.
pub fn describe_changes(
    before: &LiveConfig,
    after: &LiveConfig,
    old_names: &BTreeMap<String, String>,
    new_names: &BTreeMap<String, String>,
    open_lots: &BTreeMap<i64, i64>,
) -> Vec<Change> {
    let mut changes = Vec::new();
    for leader in &after.leaders {
        let name = shown(new_names, &leader.label);
        let Some(old) = before.leaders.iter().find(|l| l.label == leader.label) else {
            changes.push(change(
                Level::Warn,
                format!(
                    "新增 leader {name}({})",
                    if leader.enabled { "启用,会开始跟单" } else { "停用" }
                ),
            ));
            continue;
        };
        let old_name = shown(old_names, &old.label);
        if old_name != name {
            changes.push(change(Level::Ok, format!("{old_name}:显示名改为 {name}")));
        }
        if old.enabled && !leader.enabled {
            let lots = open_lots.get(&leader.id).copied().unwrap_or(0);
            let warning = if lots > 0 {
                format!("。它还有 {lots} 个持仓,停用后 leader 卖出时我们不会跟卖")
            } else {
                String::new()
            };
            changes.push(change(Level::Error, format!("{name}:启用 → 停用{warning}")));
        } else if !old.enabled && leader.enabled {
            changes.push(change(Level::Warn, format!("{name}:停用 → 启用(会开始跟单)")));
        }
        let added: Vec<String> = leader
            .addresses
            .iter()
            .filter(|a| !old.addresses.contains(a))
            .map(|a| short_address(a))
            .collect();
        let removed: Vec<String> = old
            .addresses
            .iter()
            .filter(|a| !leader.addresses.contains(a))
            .map(|a| short_address(a))
            .collect();
        if !added.is_empty() {
            changes.push(change(Level::Warn, format!("{name}:新增跟踪钱包 {}", added.join("、"))));
        }
        if !removed.is_empty() {
            changes.push(change(Level::Warn, format!("{name}:不再跟踪钱包 {}", removed.join("、"))));
        }
        if let (Some(old_policy), Some(new_policy)) = (&old.policy, &leader.policy) {
            let old_rows = leader_field_rows(old_policy);
            for row in leader_field_rows(new_policy) {
                if let Some(previous) = old_rows.iter().find(|r| r.name == row.name) {
                    if previous.value != row.value {
                        changes.push(change(
                            Level::Ok,
                            format!("{name}:{} {} → {}", row.name, previous.value, row.value),
                        ));
                    }
                }
            }
        }
    }
    for old in &before.leaders {
        if !after.leaders.iter().any(|l| l.label == old.label) {
            // apply never deletes; unreachable in practice, kept for safety.
            changes.push(change(Level::Error, format!("{}:从数据库消失", shown(old_names, &old.label))));
        }
    }

    if let (Some(old), Some(new)) = (&before.runtime, &after.runtime) {
        let mut runtime_change = |title: &str, old_value: String, new_value: String| {
            if old_value != new_value {
                changes.push(change(Level::Warn, format!("{title}:{old_value} → {new_value}")));
            }
        };
        runtime_change(
            "账户单笔上限",
            format!("{} USDC", normalize(&old.max_order_notional_usdc)),
            format!("{} USDC", normalize(&new.max_order_notional_usdc)),
        );
        runtime_change(
            "账户滚动预算",
            format!("{} USDC / {}", normalize(&old.rolling_budget_usdc), duration_zh(old.budget_window_seconds)),
            format!("{} USDC / {}", normalize(&new.rolling_budget_usdc), duration_zh(new.budget_window_seconds)),
        );
        runtime_change(
            "轮询间隔",
            format!("{} 秒", old.tick_seconds),
            format!("{} 秒", new.tick_seconds),
        );
        runtime_change(
            "补抓间隔",
            duration_zh(old.backfill_every_seconds),
            duration_zh(new.backfill_every_seconds),
        );
        runtime_change(
            "允许运行的 leader",
            old.allowed_leader_ids.clone(),
            new.allowed_leader_ids.clone(),
        );
    }

    if changes.is_empty() {
        changes.push(change(Level::Ok, "没有任何改动"));
    }
    changes
}

fn normalize(raw: &str) -> String {
    raw.trim()
        .parse::<Decimal>()
        .map(|value| value.normalize().to_string())
        .unwrap_or_else(|_| raw.to_owned())
}

/// Regenerates the public env file from the unified config. Keys the panel
/// owns are rewritten; every other key (execute switches, database path)
/// keeps its value and order.
pub fn generate_env(
    existing: &EnvFile,
    runtime: &PersistentRuntimeConfig,
    account: &LiveAccount,
    stamp: &str,
) -> String {
    let managed: Vec<(&str, Option<String>)> = vec![
        (ACCOUNT_ID_ENV, Some(runtime.account_id.to_string())),
        (ALLOWED_LEADERS_ENV, Some(runtime.allowed_leaders_text())),
        (
            MAX_ORDER_NOTIONAL_ENV,
            Some(runtime.max_order_notional.normalize().to_string()),
        ),
        (
            ROLLING_BUDGET_ENV,
            Some(runtime.rolling_budget.normalize().to_string()),
        ),
        (BUDGET_WINDOW_ENV, Some(runtime.budget_window.as_secs().to_string())),
        (TICK_SECONDS_ENV, Some(runtime.tick.as_secs().to_string())),
        (
            BACKFILL_SECONDS_ENV,
            Some(runtime.backfill_every.as_secs().to_string()),
        ),
        (SIGNATURE_TYPE_ENV, Some(account.signature_type.clone())),
        (FUNDER_ENV, account.funder_address.clone()),
    ];
    let mut lines = vec![
        "# 由跟单操作面板(ops_panel)根据 trading-config.json 生成,请在面板里修改,不要手改。".to_owned(),
        format!("# 生成时间:{stamp}"),
    ];
    let mut written: BTreeSet<&str> = BTreeSet::new();
    for (key, value) in existing.entries() {
        match managed.iter().find(|(managed_key, _)| managed_key == key) {
            Some((managed_key, Some(new_value))) => {
                lines.push(format!("{managed_key}={new_value}"));
                written.insert(managed_key);
            }
            Some((managed_key, None)) => {
                written.insert(managed_key);
            }
            None => lines.push(format!("{key}={value}")),
        }
    }
    for (key, value) in &managed {
        if let (false, Some(value)) = (written.contains(key), value) {
            lines.push(format!("{key}={value}"));
        }
    }
    lines.push(String::new());
    lines.join("\n")
}

/// Writes `text` to `path` via a temporary sibling and a rename, so a crash
/// or full disk never leaves a half-written config or env file behind.
pub fn write_atomically(path: &Path, text: &str, mode: u32) -> Result<(), String> {
    use std::{io::Write as _, os::unix::fs::PermissionsExt as _};
    let parent = path.parent().ok_or("路径没有上级目录")?;
    let file_name = path
        .file_name()
        .ok_or("路径没有文件名")?
        .to_string_lossy()
        .to_string();
    let temp = parent.join(format!(".{file_name}.ops-tmp-{}", std::process::id()));
    let result = (|| -> std::io::Result<()> {
        let mut file = std::fs::File::create(&temp)?;
        file.set_permissions(std::fs::Permissions::from_mode(mode))?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result.map_err(|error| format!("写入 {} 失败:{error}", path.display()))
}

/// True when the regenerated env assigns different values than the current
/// file (comments and ordering do not count).
pub fn env_values_differ(current: &EnvFile, generated: &str) -> bool {
    // Case-insensitive: a checksummed funder address and its lowercase form
    // are the same setting and must not force a service stop.
    let normalized = |env: &EnvFile| {
        let mut entries: Vec<(String, String)> = env
            .entries()
            .iter()
            .map(|(key, value)| (key.clone(), value.trim().to_ascii_lowercase()))
            .collect();
        entries.sort();
        entries
    };
    normalized(current) != normalized(&EnvFile::parse(generated))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::copytrading::{
        ops::{
            checks::consistency_checks,
            live_config::export_unified_json,
            test_db::TestDb,
        },
        persistent::init_config,
    };

    const SIGNER: &str = "0x00000000000000000000000000000000000000aa";
    const FUNDER: &str = "0x00000000000000000000000000000000000000bb";

    fn seed_json() -> String {
        format!(
            r#"{{
  "account": {{ "label": "acct", "signature_type": "proxy", "funder_address": "{FUNDER}" }},
  "runtime": {{ "max_order_notional_usdc": "10", "rolling_budget_usdc": "600", "budget_window_seconds": 86400, "tick_seconds": 1, "backfill_every_seconds": 600 }},
  "leaders": [
    {{ "label": "old-leader", "enabled": false, "addresses": ["0x00000000000000000000000000000000000000c1"],
       "policy": {{ "max_signal_age_seconds": 30, "decision_window_seconds": 20, "price_tolerance_bps": 0,
                   "price_tolerance_abs": "0.02", "tick_size": "0.01", "min_price": "0.01", "max_price": "0.99",
                   "max_order_notional": "10", "min_leader_trade_size": "0", "max_order_shares": "10" }} }},
    {{ "label": "leader2-fixed-5-shares", "enabled": true, "addresses": ["0x00000000000000000000000000000000000000c2"],
       "policy": {{ "max_signal_age_seconds": 30, "decision_window_seconds": 20, "price_tolerance_bps": 0,
                   "price_tolerance_abs": "0.02", "tick_size": "0.01", "min_price": "0.01", "max_price": "0.99",
                   "max_order_notional": "10", "min_leader_trade_size": "0", "rolling_budget_usdc": "30",
                   "budget_window_seconds": 600, "max_order_shares": "10", "size_ratio": "0.2",
                   "allow_repeated_market_direction": true, "maker_only": true }} }}
  ]
}}"#
        )
    }

    async fn seeded() -> (TestDb, UnifiedConfig) {
        let db = TestDb::new().await;
        let unified = parse_unified(&seed_json()).unwrap();
        apply_trading_config(
            &db.pool,
            &unified.trading_config(),
            SIGNER,
            &ConfigApplyOptions {
                max_notional_ceiling: Decimal::new(10, 0),
            },
        )
        .await
        .unwrap();
        let live = load_live_config(&db.pool).await.unwrap();
        let enabled: BTreeSet<i64> = live.enabled_leader_ids().into_iter().collect();
        let account_id = live.account().unwrap().id;
        init_config(&db.pool, &unified.runtime_for(account_id, &enabled).unwrap())
            .await
            .unwrap();
        (db, unified)
    }

    fn edited(unified_json: &str, edit: impl FnOnce(&mut Value)) -> UnifiedConfig {
        let mut value: Value = serde_json::from_str(unified_json).unwrap();
        edit(&mut value);
        parse_unified(&serde_json::to_string(&value).unwrap()).unwrap()
    }

    #[test]
    fn misspelt_or_unknown_fields_are_rejected_not_ignored() {
        let text = seed_json().replace("\"size_ratio\"", "\"size_ratoi\"");
        let error = parse_unified(&text).unwrap_err();
        assert!(error.contains("size_ratoi"), "{error}");
        let error = parse_unified(r#"{"account":{"label":"a","signature_type":"proxy"},"leaders":[],"extra":1}"#)
            .unwrap_err();
        assert!(error.contains("extra"), "{error}");
        assert!(parse_unified("{ not json").unwrap_err().contains("JSON 格式错误"));
    }

    #[tokio::test]
    async fn exported_file_dry_runs_as_no_change_and_leaves_the_live_db_untouched() {
        let (db, _) = seeded().await;
        let live = load_live_config(&db.pool).await.unwrap();
        let exported = export_unified_json(&live, &BTreeMap::new()).unwrap();
        let unified = parse_unified(&exported).unwrap();
        let dry = dry_run(&db.pool, &unified, &BTreeMap::new(), &std::env::temp_dir())
            .await
            .unwrap();
        assert_eq!(dry.changes, vec![change(Level::Ok, "没有任何改动")]);
        assert!(!dry.needs_service_stop);
        assert_eq!(load_live_config(&db.pool).await.unwrap(), live);
    }

    #[tokio::test]
    async fn owner_decisions_show_as_chinese_changes_without_touching_the_live_db() {
        let (db, _) = seeded().await;
        let live = load_live_config(&db.pool).await.unwrap();
        let exported = export_unified_json(&live, &BTreeMap::new()).unwrap();
        let unified = edited(&exported, |value| {
            let leader = &mut value["leaders"][1];
            leader["display_name"] = Value::from("leader2-ratio-maker");
            leader["policy"]
                .as_object_mut()
                .unwrap()
                .remove("max_order_shares");
            leader["policy"]["size_ratio"] = Value::from("0.3");
        });
        let dry = dry_run(&db.pool, &unified, &BTreeMap::new(), &std::env::temp_dir())
            .await
            .unwrap();
        let texts: Vec<&str> = dry.changes.iter().map(|c| c.text.as_str()).collect();
        assert!(texts.contains(&"leader2-fixed-5-shares:显示名改为 leader2-ratio-maker"), "{texts:?}");
        assert!(texts.contains(&"leader2-ratio-maker:跟单比例 20% → 30%"), "{texts:?}");
        assert!(texts.contains(&"leader2-ratio-maker:固定下单份数 10 份 → 未设置"), "{texts:?}");
        assert!(dry.needs_service_stop);
        assert_eq!(load_live_config(&db.pool).await.unwrap(), live, "dry run wrote to the live db");
    }

    #[tokio::test]
    async fn display_name_only_edit_does_not_need_a_service_stop() {
        let (db, _) = seeded().await;
        let live = load_live_config(&db.pool).await.unwrap();
        let exported = export_unified_json(&live, &BTreeMap::new()).unwrap();
        let unified = edited(&exported, |value| {
            value["leaders"][1]["display_name"] = Value::from("leader2-ratio-maker");
        });
        let dry = dry_run(&db.pool, &unified, &BTreeMap::new(), &std::env::temp_dir())
            .await
            .unwrap();
        assert!(!dry.needs_service_stop);
        assert_eq!(dry.changes.len(), 1);
    }

    #[tokio::test]
    async fn dropping_a_leader_disables_it_and_is_flagged_red() {
        let (db, _) = seeded().await;
        let live = load_live_config(&db.pool).await.unwrap();
        let exported = export_unified_json(&live, &BTreeMap::new()).unwrap();
        let unified = edited(&exported, |value| {
            value["leaders"][0]["enabled"] = Value::from(true);
            value["leaders"][1]["enabled"] = Value::from(false);
        });
        let dry = dry_run(&db.pool, &unified, &BTreeMap::new(), &std::env::temp_dir())
            .await
            .unwrap();
        assert!(dry
            .changes
            .iter()
            .any(|c| c.level == Level::Error && c.text.contains("启用 → 停用")));
        assert!(dry
            .changes
            .iter()
            .any(|c| c.text.contains("允许运行的 leader")));
    }

    #[tokio::test]
    async fn invalid_edits_fail_in_the_dry_run_before_anything_is_stopped() {
        let (db, _) = seeded().await;
        let live = load_live_config(&db.pool).await.unwrap();
        let exported = export_unified_json(&live, &BTreeMap::new()).unwrap();
        let names = BTreeMap::new();
        let temp = std::env::temp_dir();

        let renamed = edited(&exported, |value| value["account"]["label"] = Value::from("other"));
        assert!(dry_run(&db.pool, &renamed, &names, &temp).await.unwrap_err().contains("新建一个账户"));

        let over_cap = edited(&exported, |value| {
            value["leaders"][1]["policy"]["max_order_notional"] = Value::from("20")
        });
        assert!(dry_run(&db.pool, &over_cap, &names, &temp).await.unwrap_err().contains("配置校验失败"));

        let runtime_over_hard_cap = edited(&exported, |value| {
            value["runtime"]["max_order_notional_usdc"] = Value::from("20");
            value["leaders"][1]["policy"]["max_order_notional"] = Value::from("20");
        });
        assert!(dry_run(&db.pool, &runtime_over_hard_cap, &names, &temp)
            .await
            .unwrap_err()
            .contains("runtime 参数无效"));

        let nobody = edited(&exported, |value| value["leaders"][1]["enabled"] = Value::from(false));
        assert!(dry_run(&db.pool, &nobody, &names, &temp).await.unwrap_err().contains("至少要启用"));

        assert_eq!(load_live_config(&db.pool).await.unwrap(), live);
    }

    #[tokio::test]
    async fn generated_env_keeps_foreign_keys_and_passes_the_startup_checks() {
        let (db, unified) = seeded().await;
        let live = load_live_config(&db.pool).await.unwrap();
        let account = live.account().unwrap().clone();
        let enabled: BTreeSet<i64> = live.enabled_leader_ids().into_iter().collect();
        let runtime = unified.runtime_for(account.id, &enabled).unwrap();
        let current = EnvFile::parse(
            "POLYCOPY_ENGINE_EXECUTE=yes\nPOLYCOPY_PERSISTENT_EXECUTE=yes\nPOLYCOPY_DB_PATH=/x.sqlite\n\
             POLYCOPY_PERSISTENT_ALLOWED_LEADER_IDS=1,2\nPOLYCOPY_CLOB_SIGNATURE_TYPE=proxy\n",
        );
        let text = generate_env(&current, &runtime, &account, "2026-09-29 16:00:00");
        let generated = EnvFile::parse(&text);
        assert_eq!(generated.get("POLYCOPY_DB_PATH"), Some("/x.sqlite"));
        assert_eq!(generated.get("POLYCOPY_ENGINE_EXECUTE"), Some("yes"));
        assert_eq!(
            generated.get("POLYCOPY_PERSISTENT_ALLOWED_LEADER_IDS"),
            Some(runtime.allowed_leaders_text().as_str())
        );
        assert_eq!(generated.get("POLYCOPY_CLOB_FUNDER"), Some(FUNDER));
        assert!(text.starts_with("# 由跟单操作面板"));
        assert!(env_values_differ(&current, &text));
        assert!(!env_values_differ(&generated, &text));

        let checks = consistency_checks(&live, Some(&generated), Some(true), &BTreeMap::new());
        let problems: Vec<_> = checks.iter().filter(|c| c.level != Level::Ok).collect();
        assert!(problems.is_empty(), "{problems:?}");
    }

    #[test]
    fn atomic_write_replaces_the_file_with_the_requested_mode_and_no_leftovers() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = std::env::temp_dir().join(format!(
            "polycopy-ops-write-test-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trading-config.json");
        std::fs::write(&path, "old").unwrap();
        write_atomically(&path, "new", 0o640).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o640);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1, "temp file left behind");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn snapshot_works_from_a_read_only_connection() {
        let (db, _) = seeded().await;
        let path: String = sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        let read_only = crate::copytrading::db::open_read_only(&path).await.unwrap();
        let dest = std::env::temp_dir().join(format!(
            "polycopy-ops-snapshot-test-{}-{}.sqlite",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        snapshot_database(&read_only, &dest).await.unwrap();
        assert!(dest.exists());
        assert!(snapshot_database(&read_only, &dest).await.unwrap_err().contains("不覆盖"));
        remove_database_files(&dest);
    }
}
