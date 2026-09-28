//! The configuration the engine is *actually* running with, read straight
//! from the database, plus an exporter that turns it back into the single
//! owner-edited trading-config JSON.
//!
//! The exporter's contract is exact round-tripping: feeding its output to
//! [`crate::copytrading::apply_trading_config`] must report every account and
//! leader `Unchanged`. That is what makes it safe to regenerate the JSON from
//! the database before an edit, instead of editing a stale copy on disk (a
//! stale copy on the server would have silently re-enabled a disabled leader
//! and reverted another leader's strategy). The unified file adds a `runtime`
//! section and per-leader `display_name`; `apply_trading_config`'s input types
//! ignore unknown fields, so the same file still feeds `copy_config_apply`.

use std::collections::BTreeMap;

use serde::Serialize;
use sqlx::SqlitePool;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveAccount {
    pub id: i64,
    pub label: String,
    pub signing_address: String,
    pub funder_address: Option<String>,
    pub signature_type: String,
}

/// Every `leader_policy` column that `apply_trading_config` owns, exactly as
/// stored (same shape as `setup`'s private `StoredPolicy`).
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct LivePolicy {
    pub max_signal_age_seconds: i64,
    pub decision_window_seconds: i64,
    pub price_tolerance_bps: i64,
    pub tick_size: String,
    pub min_price: String,
    pub max_price: String,
    pub max_order_notional: String,
    pub min_leader_trade_size: String,
    pub rolling_budget_usdc: Option<String>,
    pub budget_window_seconds: Option<i64>,
    pub max_order_shares: Option<String>,
    pub balance_within_market: bool,
    pub price_tolerance_abs: String,
    pub size_ratio: Option<String>,
    pub allow_repeated_market_direction: bool,
    pub maker_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveLeader {
    pub id: i64,
    pub label: String,
    pub enabled: bool,
    /// Addresses currently used as copying sources (enabled aliases), in
    /// insertion order.
    pub addresses: Vec<String>,
    pub disabled_address_count: usize,
    pub policy: Option<LivePolicy>,
}

/// The single `persistent_execution_config` row (`persistent_control
/// init-config` / `reconfigure`), which `copy_persistent` compares against
/// its env file at every start.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct LiveRuntime {
    pub account_id: i64,
    pub enabled: bool,
    pub allowed_leader_ids: String,
    pub max_order_notional_usdc: String,
    pub rolling_budget_usdc: String,
    pub budget_window_seconds: i64,
    pub tick_seconds: i64,
    pub backfill_every_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LiveConfig {
    pub accounts: Vec<LiveAccount>,
    pub leaders: Vec<LiveLeader>,
    pub runtime: Option<LiveRuntime>,
}

impl LiveConfig {
    /// The account the persistent runner is configured for, or the only
    /// account when no runtime row exists yet.
    pub fn account(&self) -> Option<&LiveAccount> {
        match &self.runtime {
            Some(runtime) => self.accounts.iter().find(|a| a.id == runtime.account_id),
            None if self.accounts.len() == 1 => self.accounts.first(),
            None => None,
        }
    }

    pub fn enabled_leader_ids(&self) -> Vec<i64> {
        self.leaders
            .iter()
            .filter(|l| l.enabled)
            .map(|l| l.id)
            .collect()
    }

    pub fn leader_label(&self, id: i64) -> Option<&str> {
        self.leaders
            .iter()
            .find(|l| l.id == id)
            .map(|l| l.label.as_str())
    }
}

pub async fn load_live_config(pool: &SqlitePool) -> Result<LiveConfig, sqlx::Error> {
    let accounts: Vec<(i64, String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT id, label, signing_address, funder_address, signature_type \
         FROM accounts ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let accounts = accounts
        .into_iter()
        .map(
            |(id, label, signing_address, funder_address, signature_type)| LiveAccount {
                id,
                label,
                signing_address,
                funder_address,
                signature_type,
            },
        )
        .collect();

    let leader_rows: Vec<(i64, String, bool)> =
        sqlx::query_as("SELECT id, label, enabled FROM leader_config ORDER BY id")
            .fetch_all(pool)
            .await?;
    let mut leaders = Vec::with_capacity(leader_rows.len());
    for (id, label, enabled) in leader_rows {
        let aliases: Vec<(String, bool)> = sqlx::query_as(
            "SELECT address, enabled FROM leader_wallet_aliases WHERE leader_id = ? ORDER BY id",
        )
        .bind(id)
        .fetch_all(pool)
        .await?;
        let addresses = aliases
            .iter()
            .filter(|(_, enabled)| *enabled)
            .map(|(address, _)| address.clone())
            .collect();
        let disabled_address_count = aliases.iter().filter(|(_, enabled)| !enabled).count();
        let policy: Option<LivePolicy> = sqlx::query_as(
            "SELECT max_signal_age_seconds, decision_window_seconds, price_tolerance_bps, \
             tick_size, min_price, max_price, max_order_notional, min_leader_trade_size, \
             rolling_budget_usdc, budget_window_seconds, max_order_shares, balance_within_market, \
             price_tolerance_abs, size_ratio, allow_repeated_market_direction, maker_only \
             FROM leader_policy WHERE leader_id = ?",
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;
        leaders.push(LiveLeader {
            id,
            label,
            enabled,
            addresses,
            disabled_address_count,
            policy,
        });
    }

    let runtime: Option<LiveRuntime> = sqlx::query_as(
        "SELECT account_id, enabled, allowed_leader_ids, max_order_notional_usdc, \
         rolling_budget_usdc, budget_window_seconds, tick_seconds, backfill_every_seconds \
         FROM persistent_execution_config WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    Ok(LiveConfig {
        accounts,
        leaders,
        runtime,
    })
}

#[derive(Debug, Serialize)]
struct UnifiedConfig<'a> {
    account: AccountOut<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<RuntimeOut<'a>>,
    leaders: Vec<LeaderOut<'a>>,
}

#[derive(Debug, Serialize)]
struct AccountOut<'a> {
    label: &'a str,
    signature_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    funder_address: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct RuntimeOut<'a> {
    max_order_notional_usdc: &'a str,
    rolling_budget_usdc: &'a str,
    budget_window_seconds: i64,
    tick_seconds: i64,
    backfill_every_seconds: i64,
}

#[derive(Debug, Serialize)]
struct LeaderOut<'a> {
    label: &'a str,
    display_name: &'a str,
    enabled: bool,
    addresses: &'a [String],
    policy: PolicyOut<'a>,
}

#[derive(Debug, Serialize)]
struct PolicyOut<'a> {
    size_ratio: Option<&'a str>,
    maker_only: bool,
    allow_repeated_market_direction: bool,
    max_order_notional: &'a str,
    max_order_shares: Option<&'a str>,
    min_leader_trade_size: &'a str,
    price_tolerance_abs: &'a str,
    price_tolerance_bps: i64,
    min_price: &'a str,
    max_price: &'a str,
    tick_size: &'a str,
    max_signal_age_seconds: i64,
    decision_window_seconds: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    rolling_budget_usdc: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    budget_window_seconds: Option<i64>,
    balance_within_market: bool,
}

/// Renders the live configuration as the unified trading-config JSON.
/// `display_names` maps leader label to the owner's display name (kept only
/// in this file, never in the database); a leader without one shows its
/// label. Fails rather than emitting a file that could not round-trip.
pub fn export_unified_json(
    live: &LiveConfig,
    display_names: &BTreeMap<String, String>,
) -> Result<String, String> {
    let account = live
        .account()
        .ok_or_else(|| "无法确定要导出的账户(数据库里没有账户,或有多个账户且缺少运行参数)".to_owned())?;
    let mut leaders = Vec::with_capacity(live.leaders.len());
    for leader in &live.leaders {
        let policy = leader
            .policy
            .as_ref()
            .ok_or_else(|| format!("leader {} 没有交易参数(leader_policy 缺行)", leader.label))?;
        if leader.addresses.is_empty() {
            return Err(format!(
                "leader {} 没有启用中的钱包地址,导出后无法原样应用",
                leader.label
            ));
        }
        leaders.push(LeaderOut {
            label: &leader.label,
            display_name: display_names
                .get(&leader.label)
                .map(String::as_str)
                .unwrap_or(&leader.label),
            enabled: leader.enabled,
            addresses: &leader.addresses,
            policy: PolicyOut {
                size_ratio: policy.size_ratio.as_deref(),
                maker_only: policy.maker_only,
                allow_repeated_market_direction: policy.allow_repeated_market_direction,
                max_order_notional: &policy.max_order_notional,
                max_order_shares: policy.max_order_shares.as_deref(),
                min_leader_trade_size: &policy.min_leader_trade_size,
                price_tolerance_abs: &policy.price_tolerance_abs,
                price_tolerance_bps: policy.price_tolerance_bps,
                min_price: &policy.min_price,
                max_price: &policy.max_price,
                tick_size: &policy.tick_size,
                max_signal_age_seconds: policy.max_signal_age_seconds,
                decision_window_seconds: policy.decision_window_seconds,
                rolling_budget_usdc: policy.rolling_budget_usdc.as_deref(),
                budget_window_seconds: policy.budget_window_seconds,
                balance_within_market: policy.balance_within_market,
            },
        });
    }
    let unified = UnifiedConfig {
        account: AccountOut {
            label: &account.label,
            signature_type: &account.signature_type,
            funder_address: account.funder_address.as_deref(),
        },
        runtime: live.runtime.as_ref().map(|r| RuntimeOut {
            max_order_notional_usdc: &r.max_order_notional_usdc,
            rolling_budget_usdc: &r.rolling_budget_usdc,
            budget_window_seconds: r.budget_window_seconds,
            tick_seconds: r.tick_seconds,
            backfill_every_seconds: r.backfill_every_seconds,
        }),
        leaders,
    };
    serde_json::to_string_pretty(&unified)
        .map(|mut text| {
            text.push('\n');
            text
        })
        .map_err(|error| format!("配置序列化失败: {error}"))
}

/// Reads `display_name` per leader label from an existing unified JSON, so a
/// regeneration keeps the owner's names. Anything unparsable yields no names
/// rather than an error: display names are cosmetic.
pub fn display_names_from_json(text: &str) -> BTreeMap<String, String> {
    let mut names = BTreeMap::new();
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return names;
    };
    let Some(leaders) = value.get("leaders").and_then(|l| l.as_array()) else {
        return names;
    };
    for leader in leaders {
        if let (Some(label), Some(name)) = (
            leader.get("label").and_then(|v| v.as_str()),
            leader.get("display_name").and_then(|v| v.as_str()),
        ) {
            if !name.trim().is_empty() {
                names.insert(label.to_owned(), name.trim().to_owned());
            }
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::copytrading::{
        apply_trading_config, ops::test_db::TestDb, AccountConfigInput, ChangeKind,
        ConfigApplyOptions, LeaderConfigInput, LeaderPolicyInput, TradingConfig,
    };

    const SIGNER: &str = "0x00000000000000000000000000000000000000aa";

    fn base_policy() -> LeaderPolicyInput {
        LeaderPolicyInput {
            max_signal_age_seconds: 30,
            decision_window_seconds: 20,
            price_tolerance_bps: 0,
            price_tolerance_abs: Some("0.02".to_owned()),
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            max_order_notional: "10".to_owned(),
            min_leader_trade_size: "0".to_owned(),
            rolling_budget_usdc: Some("30".to_owned()),
            budget_window_seconds: Some(600),
            max_order_shares: Some("10".to_owned()),
            balance_within_market: false,
            size_ratio: Some("0.2".to_owned()),
            allow_repeated_market_direction: true,
            maker_only: true,
        }
    }

    fn config() -> TradingConfig {
        let mut plain = base_policy();
        plain.price_tolerance_abs = None;
        plain.rolling_budget_usdc = None;
        plain.budget_window_seconds = None;
        plain.max_order_shares = None;
        plain.size_ratio = None;
        plain.allow_repeated_market_direction = false;
        plain.maker_only = false;
        plain.price_tolerance_bps = 300;
        TradingConfig {
            account: AccountConfigInput {
                label: "test-account".to_owned(),
                signature_type: "proxy".to_owned(),
                funder_address: Some("0x00000000000000000000000000000000000000bb".to_owned()),
            },
            leaders: vec![
                LeaderConfigInput {
                    label: "leader-plain".to_owned(),
                    enabled: false,
                    addresses: vec!["0x00000000000000000000000000000000000000c1".to_owned()],
                    policy: plain,
                },
                LeaderConfigInput {
                    label: "leader-ratio".to_owned(),
                    enabled: true,
                    addresses: vec![
                        "0x00000000000000000000000000000000000000d1".to_owned(),
                        "0x00000000000000000000000000000000000000d2".to_owned(),
                    ],
                    policy: base_policy(),
                },
            ],
        }
    }

    fn options() -> ConfigApplyOptions {
        ConfigApplyOptions {
            max_notional_ceiling: Decimal::new(10, 0),
        }
    }

    #[tokio::test]
    async fn exported_json_applies_back_as_unchanged() {
        let db = TestDb::new().await;
        apply_trading_config(&db.pool, &config(), SIGNER, &options())
            .await
            .unwrap();

        let live = load_live_config(&db.pool).await.unwrap();
        let names = BTreeMap::from([("leader-ratio".to_owned(), "leader2-ratio-maker".to_owned())]);
        let text = export_unified_json(&live, &names).unwrap();

        let reparsed: TradingConfig = serde_json::from_str(&text).unwrap();
        let summary = apply_trading_config(&db.pool, &reparsed, SIGNER, &options())
            .await
            .unwrap();
        assert_eq!(summary.account_change, ChangeKind::Unchanged);
        for leader in &summary.leaders {
            assert_eq!(leader.change, ChangeKind::Unchanged, "{} changed: {text}", leader.label);
        }
        assert_eq!(load_live_config(&db.pool).await.unwrap(), live);
        // A leader without a chosen name is written out under its label, so
        // the owner sees (and can edit) a display_name for every leader.
        assert_eq!(
            display_names_from_json(&text),
            BTreeMap::from([
                ("leader-plain".to_owned(), "leader-plain".to_owned()),
                ("leader-ratio".to_owned(), "leader2-ratio-maker".to_owned()),
            ])
        );
    }

    #[tokio::test]
    async fn loads_policy_aliases_and_derived_views() {
        let db = TestDb::new().await;
        apply_trading_config(&db.pool, &config(), SIGNER, &options())
            .await
            .unwrap();
        let live = load_live_config(&db.pool).await.unwrap();

        assert_eq!(live.account().unwrap().label, "test-account");
        assert_eq!(live.leaders.len(), 2);
        let ratio = &live.leaders[1];
        assert_eq!(ratio.addresses.len(), 2);
        let policy = ratio.policy.as_ref().unwrap();
        assert_eq!(policy.size_ratio.as_deref(), Some("0.2"));
        assert!(policy.maker_only);
        assert_eq!(policy.price_tolerance_abs, "0.02");
        assert_eq!(live.enabled_leader_ids(), vec![ratio.id]);
        assert_eq!(live.leader_label(ratio.id), Some("leader-ratio"));
        assert!(live.runtime.is_none());
    }

    #[test]
    fn display_names_ignore_garbage_and_blank_names() {
        assert!(display_names_from_json("not json").is_empty());
        let names = display_names_from_json(
            r#"{"leaders":[{"label":"a","display_name":"  "},{"label":"b","display_name":"B"},{"label":"c"}]}"#,
        );
        assert_eq!(names, BTreeMap::from([("b".to_owned(), "B".to_owned())]));
    }
}
