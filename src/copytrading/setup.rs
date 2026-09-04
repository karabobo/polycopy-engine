//! Reconciles a database's account/leader/policy configuration against one
//! JSON description of the intended trading configuration.
//!
//! Two operations, not one: creating configuration that does not exist yet
//! (what this module used to call one-shot setup), and adjusting
//! configuration that already exists. Both go through the same function --
//! [`apply_trading_config`] -- so there is one code path, not two that can
//! quietly drift apart. The intended workflow is stop the engine, edit the
//! JSON, reapply, restart; this module does not need to handle a
//! concurrently running engine because [`crate::engine_lock::EngineLock`]
//! (acquired by the caller, e.g. `copy_config_apply`, before opening the
//! database) already refuses to run alongside one.
//!
//! The Trading Config a caller supplies is declarative, not a patch (see
//! `docs/adr/0001-trading-config-apply-is-declarative.md`): applying it
//! makes the database's set of leaders match it exactly. A leader
//! currently in the database but missing from this config is disabled --
//! never deleted, so its `position_lots`/history are never orphaned --
//! exactly like an address missing from a leader's own
//! `leader_wallet_aliases` list is disabled rather than left alone. This
//! applies with no extra warning even when the disabled leader still
//! holds open lots or an unresolved reconciliation case; that trade-off
//! is deliberate, not an oversight -- see the ADR.
//!
//! `leader_config.enabled`, a leader's address book, and every
//! `leader_policy` field may all change freely, at any time, regardless
//! of how much has already happened. A leader's `label` is the immutable
//! key this module matches leaders by (renaming means adding a new
//! leader, not editing one), so it has no lock of its own to worry about.
//!
//! What is locked once the account has any real activity
//! (`copy_intents`, `reconciliation_cases`, or `position_lots`): the
//! account's own wallet identity (`funder_address`, `signature_type`).
//! Changing it after that would silently re-describe already-recorded
//! history as belonging to a different wallet. Before any of that exists,
//! the account is still just a draft and its identity may be corrected
//! freely. There is deliberately no equivalent guard against a *label*
//! mismatch creating an unintended second account: this project runs
//! exactly one account, so a mismatched label can only be an operator
//! typo, and the operator owns catching it.
//!
//! `max_order_notional` additionally has a caller-supplied ceiling
//! ([`ConfigApplyOptions::max_notional_ceiling`], default 1 USDC): raising
//! it is an explicit choice the operator makes when invoking this tool, not
//! a number that can silently grow just by editing the JSON. This governs
//! only what may be *configured* -- `copy_run`'s own independent,
//! hardcoded runtime ceiling on what may actually be *submitted* is a
//! separate gate this module has no part in and does not affect.
//!
//! This module writes configuration rows only. It has no venue client and
//! no order-submission surface.

use std::{collections::BTreeSet, fmt};

use chrono::{SecondsFormat, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqlitePool, Transaction};

pub const CONFIG_APPLIED_PREFIX: &str = "CONFIG_APPLIED: ";

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TradingConfig {
    pub account: AccountConfigInput,
    pub leaders: Vec<LeaderConfigInput>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AccountConfigInput {
    pub label: String,
    pub signature_type: String,
    #[serde(default)]
    pub funder_address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LeaderConfigInput {
    pub label: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub addresses: Vec<String>,
    pub policy: LeaderPolicyInput,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LeaderPolicyInput {
    pub max_signal_age_seconds: i64,
    pub decision_window_seconds: i64,
    pub price_tolerance_bps: i64,
    pub tick_size: String,
    pub min_price: String,
    pub max_price: String,
    pub max_order_notional: String,
    pub min_leader_trade_size: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConfigApplyOptions {
    pub max_notional_ceiling: Decimal,
}

impl Default for ConfigApplyOptions {
    fn default() -> Self {
        Self {
            max_notional_ceiling: Decimal::ONE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Created,
    Updated,
    Unchanged,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConfigApplySummary {
    pub account_id: i64,
    pub account_change: ChangeKind,
    pub leaders: Vec<LeaderApplySummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LeaderApplySummary {
    pub leader_id: i64,
    pub label: String,
    pub change: ChangeKind,
    /// Counts a brand-new alias row and a previously-disabled alias being
    /// re-enabled the same way: both mean this address is now considered
    /// an active copying source when it was not a moment ago.
    pub aliases_added: usize,
    pub aliases_disabled: usize,
    pub policy_changed: bool,
}

/// Creates or reconciles `config` against `pool`. `signing_address` must
/// already be freshly derived from the live credential (the caller's job,
/// e.g. via a derive-only authentication) -- this function only ever
/// reads it, never derives it itself, so it has no venue client.
///
/// Every leader is validated up front; if any one of them fails
/// validation (including the notional ceiling), nothing is written at
/// all, not even the leaders that would have been fine on their own.
pub async fn apply_trading_config(
    pool: &SqlitePool,
    config: &TradingConfig,
    signing_address: &str,
    options: &ConfigApplyOptions,
) -> Result<ConfigApplySummary, ConfigError> {
    let account_label = non_empty(&config.account.label, "account label")?;
    let signing_address = normalize_address(signing_address, "signing address")?;
    if !matches!(
        config.account.signature_type.as_str(),
        "eoa" | "proxy" | "gnosis_safe" | "poly1271"
    ) {
        return Err(ConfigError::UnsupportedSignatureType);
    }
    let funder_address = normalize_funder_address(
        config.account.funder_address.as_deref(),
        &config.account.signature_type,
    )?;
    if config.leaders.is_empty() {
        return Err(ConfigError::NoLeaders);
    }
    let mut seen_labels = BTreeSet::new();
    for leader in &config.leaders {
        if !seen_labels.insert(leader.label.trim()) {
            return Err(ConfigError::DuplicateLeaderLabel(leader.label.clone()));
        }
    }
    let mut normalized_leaders = Vec::with_capacity(config.leaders.len());
    for leader in &config.leaders {
        let label = non_empty(&leader.label, "leader label")?;
        if leader.addresses.is_empty() {
            return Err(ConfigError::EmptyField("leader addresses"));
        }
        let mut addresses = Vec::with_capacity(leader.addresses.len());
        for address in &leader.addresses {
            addresses.push(normalize_address(address, "leader address")?);
        }
        let policy = normalize_policy(&leader.policy, &label, options)?;
        normalized_leaders.push((label, leader.enabled, addresses, policy));
    }

    let mut tx = pool.begin().await.map_err(ConfigError::Database)?;

    let existing_account: Option<(i64, String, Option<String>, String)> = sqlx::query_as(
        "SELECT id, signing_address, funder_address, signature_type FROM accounts WHERE label = ?",
    )
    .bind(&account_label)
    .fetch_optional(&mut *tx)
    .await
    .map_err(ConfigError::Database)?;

    let (account_id, account_change) = match existing_account {
        None => {
            let inserted = sqlx::query(
                "INSERT INTO accounts (label, signing_address, funder_address, signature_type) \
                 VALUES (?, ?, ?, ?)",
            )
            .bind(&account_label)
            .bind(&signing_address)
            .bind(&funder_address)
            .bind(&config.account.signature_type)
            .execute(&mut *tx)
            .await
            .map_err(ConfigError::Database)?;
            (inserted.last_insert_rowid(), ChangeKind::Created)
        }
        Some((id, stored_signing, stored_funder, stored_signature_type)) => {
            let identity_matches = stored_signing == signing_address
                && stored_funder == funder_address
                && stored_signature_type == config.account.signature_type;
            if identity_matches {
                (id, ChangeKind::Unchanged)
            } else {
                let work_count: i64 = sqlx::query_scalar(
                    "SELECT (SELECT COUNT(*) FROM copy_intents WHERE account_id = ?) \
                     + (SELECT COUNT(*) FROM reconciliation_cases WHERE account_id = ?) \
                     + (SELECT COUNT(*) FROM position_lots WHERE account_id = ?)",
                )
                .bind(id)
                .bind(id)
                .bind(id)
                .fetch_one(&mut *tx)
                .await
                .map_err(ConfigError::Database)?;
                if work_count > 0 {
                    return Err(ConfigError::AccountIdentityLocked);
                }
                sqlx::query(
                    "UPDATE accounts SET signing_address = ?, funder_address = ?, \
                     signature_type = ? WHERE id = ?",
                )
                .bind(&signing_address)
                .bind(&funder_address)
                .bind(&config.account.signature_type)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(ConfigError::Database)?;
                (id, ChangeKind::Updated)
            }
        }
    };

    sqlx::query(
        "INSERT OR IGNORE INTO execution_schedule \
         (id, shard_scheme_version, shard_algorithm, lane_count) \
         VALUES (1, 1, 'hash_mod_lane_count', 1)",
    )
    .execute(&mut *tx)
    .await
    .map_err(ConfigError::Database)?;

    let mut leader_summaries = Vec::with_capacity(normalized_leaders.len());
    for (label, enabled, addresses, policy) in normalized_leaders {
        leader_summaries
            .push(apply_one_leader(&mut tx, &label, enabled, &addresses, &policy).await?);
    }

    // A Trading Config is declarative (docs/adr/0001-trading-config-apply-is-declarative.md):
    // any leader already in the database but not named in this config is
    // disabled, not left alone -- matching how its own wallet-alias list
    // already behaved. Never deleted, and its position_lots/history are
    // untouched either way; disabling carries no warning even if it
    // still holds open lots, by deliberate choice.
    let existing_leaders: Vec<(i64, String, bool)> =
        sqlx::query_as("SELECT id, label, enabled FROM leader_config")
            .fetch_all(&mut *tx)
            .await
            .map_err(ConfigError::Database)?;
    for (leader_id, label, enabled) in existing_leaders {
        if seen_labels.contains(label.as_str()) {
            continue;
        }
        if enabled {
            sqlx::query(
                "UPDATE leader_config SET enabled = 0, \
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
            )
            .bind(leader_id)
            .execute(&mut *tx)
            .await
            .map_err(ConfigError::Database)?;
        }
        leader_summaries.push(LeaderApplySummary {
            leader_id,
            label,
            change: if enabled {
                ChangeKind::Updated
            } else {
                ChangeKind::Unchanged
            },
            aliases_added: 0,
            aliases_disabled: 0,
            policy_changed: false,
        });
    }

    tx.commit().await.map_err(ConfigError::Database)?;

    Ok(ConfigApplySummary {
        account_id,
        account_change,
        leaders: leader_summaries,
    })
}

async fn apply_one_leader(
    tx: &mut Transaction<'_, Sqlite>,
    label: &str,
    enabled: bool,
    addresses: &[String],
    policy: &NormalizedPolicy,
) -> Result<LeaderApplySummary, ConfigError> {
    let existing: Option<(i64, bool)> =
        sqlx::query_as("SELECT id, enabled FROM leader_config WHERE label = ?")
            .bind(label)
            .fetch_optional(&mut **tx)
            .await
            .map_err(ConfigError::Database)?;

    let (leader_id, created, enabled_changed) = match existing {
        None => {
            let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            let inserted = sqlx::query(
                "INSERT INTO leader_config (label, enabled, activation_at) VALUES (?, ?, ?)",
            )
            .bind(label)
            .bind(enabled)
            .bind(&now)
            .execute(&mut **tx)
            .await
            .map_err(ConfigError::Database)?;
            let leader_id = inserted.last_insert_rowid();
            for address in addresses {
                sqlx::query(
                    "INSERT INTO leader_wallet_aliases (leader_id, address, enabled) \
                     VALUES (?, ?, 1)",
                )
                .bind(leader_id)
                .bind(address)
                .execute(&mut **tx)
                .await
                .map_err(ConfigError::Database)?;
            }
            insert_policy(tx, leader_id, policy).await?;
            (leader_id, true, false)
        }
        Some((leader_id, stored_enabled)) => {
            let enabled_changed = stored_enabled != enabled;
            if enabled_changed {
                sqlx::query(
                    "UPDATE leader_config SET enabled = ?, \
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
                )
                .bind(enabled)
                .bind(leader_id)
                .execute(&mut **tx)
                .await
                .map_err(ConfigError::Database)?;
            }
            (leader_id, false, enabled_changed)
        }
    };

    let (aliases_added, aliases_disabled) = if created {
        (addresses.len(), 0)
    } else {
        reconcile_aliases(tx, leader_id, addresses).await?
    };

    let policy_changed = if created {
        true
    } else {
        update_policy_if_changed(tx, leader_id, policy).await?
    };

    let change = if created {
        ChangeKind::Created
    } else if enabled_changed || aliases_added > 0 || aliases_disabled > 0 || policy_changed {
        ChangeKind::Updated
    } else {
        ChangeKind::Unchanged
    };

    Ok(LeaderApplySummary {
        leader_id,
        label: label.to_owned(),
        change,
        aliases_added,
        aliases_disabled,
        policy_changed,
    })
}

async fn reconcile_aliases(
    tx: &mut Transaction<'_, Sqlite>,
    leader_id: i64,
    addresses: &[String],
) -> Result<(usize, usize), ConfigError> {
    let existing: Vec<(String, bool)> =
        sqlx::query_as("SELECT address, enabled FROM leader_wallet_aliases WHERE leader_id = ?")
            .bind(leader_id)
            .fetch_all(&mut **tx)
            .await
            .map_err(ConfigError::Database)?;

    let mut added = 0usize;
    for address in addresses {
        match existing
            .iter()
            .find(|(existing_address, _)| existing_address == address)
        {
            None => {
                sqlx::query(
                    "INSERT INTO leader_wallet_aliases (leader_id, address, enabled) \
                     VALUES (?, ?, 1)",
                )
                .bind(leader_id)
                .bind(address)
                .execute(&mut **tx)
                .await
                .map_err(ConfigError::Database)?;
                added += 1;
            }
            Some((_, true)) => {}
            Some((_, false)) => {
                sqlx::query(
                    "UPDATE leader_wallet_aliases SET enabled = 1 \
                     WHERE leader_id = ? AND address = ?",
                )
                .bind(leader_id)
                .bind(address)
                .execute(&mut **tx)
                .await
                .map_err(ConfigError::Database)?;
                added += 1;
            }
        }
    }

    let mut disabled = 0usize;
    for (existing_address, existing_enabled) in &existing {
        if *existing_enabled && !addresses.contains(existing_address) {
            sqlx::query(
                "UPDATE leader_wallet_aliases SET enabled = 0 \
                 WHERE leader_id = ? AND address = ?",
            )
            .bind(leader_id)
            .bind(existing_address)
            .execute(&mut **tx)
            .await
            .map_err(ConfigError::Database)?;
            disabled += 1;
        }
    }

    Ok((added, disabled))
}

async fn insert_policy(
    tx: &mut Transaction<'_, Sqlite>,
    leader_id: i64,
    policy: &NormalizedPolicy,
) -> Result<(), ConfigError> {
    sqlx::query(
        "INSERT INTO leader_policy \
         (leader_id, max_signal_age_seconds, decision_window_seconds, price_tolerance_bps, \
          tick_size, min_price, max_price, max_order_notional, min_leader_trade_size) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(leader_id)
    .bind(policy.max_signal_age_seconds)
    .bind(policy.decision_window_seconds)
    .bind(policy.price_tolerance_bps)
    .bind(&policy.tick_size)
    .bind(&policy.min_price)
    .bind(&policy.max_price)
    .bind(&policy.max_order_notional)
    .bind(&policy.min_leader_trade_size)
    .execute(&mut **tx)
    .await
    .map_err(ConfigError::Database)?;
    Ok(())
}

#[allow(clippy::type_complexity)]
async fn update_policy_if_changed(
    tx: &mut Transaction<'_, Sqlite>,
    leader_id: i64,
    policy: &NormalizedPolicy,
) -> Result<bool, ConfigError> {
    let current: Option<(i64, i64, i64, String, String, String, String, String)> = sqlx::query_as(
        "SELECT max_signal_age_seconds, decision_window_seconds, price_tolerance_bps, \
         tick_size, min_price, max_price, max_order_notional, min_leader_trade_size \
         FROM leader_policy WHERE leader_id = ?",
    )
    .bind(leader_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(ConfigError::Database)?;

    let desired = (
        policy.max_signal_age_seconds,
        policy.decision_window_seconds,
        policy.price_tolerance_bps,
        policy.tick_size.clone(),
        policy.min_price.clone(),
        policy.max_price.clone(),
        policy.max_order_notional.clone(),
        policy.min_leader_trade_size.clone(),
    );
    if current.as_ref() == Some(&desired) {
        return Ok(false);
    }

    if current.is_some() {
        sqlx::query(
            "UPDATE leader_policy SET max_signal_age_seconds = ?, decision_window_seconds = ?, \
             price_tolerance_bps = ?, tick_size = ?, min_price = ?, max_price = ?, \
             max_order_notional = ?, min_leader_trade_size = ?, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE leader_id = ?",
        )
        .bind(policy.max_signal_age_seconds)
        .bind(policy.decision_window_seconds)
        .bind(policy.price_tolerance_bps)
        .bind(&policy.tick_size)
        .bind(&policy.min_price)
        .bind(&policy.max_price)
        .bind(&policy.max_order_notional)
        .bind(&policy.min_leader_trade_size)
        .bind(leader_id)
        .execute(&mut **tx)
        .await
        .map_err(ConfigError::Database)?;
    } else {
        insert_policy(tx, leader_id, policy).await?;
    }
    Ok(true)
}

struct NormalizedPolicy {
    max_signal_age_seconds: i64,
    decision_window_seconds: i64,
    price_tolerance_bps: i64,
    tick_size: String,
    min_price: String,
    max_price: String,
    max_order_notional: String,
    min_leader_trade_size: String,
}

fn normalize_policy(
    policy: &LeaderPolicyInput,
    leader_label: &str,
    options: &ConfigApplyOptions,
) -> Result<NormalizedPolicy, ConfigError> {
    if policy.max_signal_age_seconds <= 0 {
        return Err(ConfigError::InvalidPolicyField("max_signal_age_seconds"));
    }
    if policy.decision_window_seconds <= 0 {
        return Err(ConfigError::InvalidPolicyField("decision_window_seconds"));
    }
    if policy.price_tolerance_bps < 0 {
        return Err(ConfigError::InvalidPolicyField("price_tolerance_bps"));
    }
    let tick_size = parse_positive_decimal(&policy.tick_size, "tick_size")?;
    let min_price = parse_positive_decimal(&policy.min_price, "min_price")?;
    let max_price = parse_positive_decimal(&policy.max_price, "max_price")?;
    if min_price >= max_price {
        return Err(ConfigError::InvalidPolicyField("min_price/max_price"));
    }
    let max_order_notional =
        parse_positive_decimal(&policy.max_order_notional, "max_order_notional")?;
    if max_order_notional > options.max_notional_ceiling {
        return Err(ConfigError::NotionalAboveCeiling {
            leader_label: leader_label.to_owned(),
            value: max_order_notional,
            ceiling: options.max_notional_ceiling,
        });
    }
    let min_leader_trade_size: Decimal = policy
        .min_leader_trade_size
        .parse()
        .map_err(|_| ConfigError::InvalidPolicyField("min_leader_trade_size"))?;
    if min_leader_trade_size < Decimal::ZERO {
        return Err(ConfigError::InvalidPolicyField("min_leader_trade_size"));
    }

    Ok(NormalizedPolicy {
        max_signal_age_seconds: policy.max_signal_age_seconds,
        decision_window_seconds: policy.decision_window_seconds,
        price_tolerance_bps: policy.price_tolerance_bps,
        tick_size: tick_size.to_string(),
        min_price: min_price.to_string(),
        max_price: max_price.to_string(),
        max_order_notional: max_order_notional.to_string(),
        min_leader_trade_size: min_leader_trade_size.to_string(),
    })
}

fn parse_positive_decimal(value: &str, field: &'static str) -> Result<Decimal, ConfigError> {
    let parsed: Decimal = value
        .parse()
        .map_err(|_| ConfigError::InvalidPolicyField(field))?;
    if parsed <= Decimal::ZERO {
        return Err(ConfigError::InvalidPolicyField(field));
    }
    Ok(parsed)
}

fn non_empty(value: &str, field: &'static str) -> Result<String, ConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(ConfigError::EmptyField(field))
    } else {
        Ok(trimmed.to_owned())
    }
}

fn normalize_address(value: &str, field: &'static str) -> Result<String, ConfigError> {
    let trimmed = value.trim();
    if trimmed.len() != 42
        || !trimmed.starts_with("0x")
        || !trimmed[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ConfigError::InvalidAddress(field));
    }
    Ok(trimmed.to_ascii_lowercase())
}

/// A funder address is required for every signature type except `eoa`
/// (where the signer funds itself, see `accounts` table's own CHECK
/// constraint comment) and must be absent for `eoa` -- supplying one
/// there would silently be ignored by the schema (the column would just
/// go unused), which is worse than rejecting the ambiguity outright.
fn normalize_funder_address(
    raw: Option<&str>,
    signature_type: &str,
) -> Result<Option<String>, ConfigError> {
    match (signature_type, raw) {
        ("eoa", None) => Ok(None),
        ("eoa", Some(_)) => Err(ConfigError::UnexpectedFunderForEoa),
        (_, Some(address)) => Ok(Some(normalize_address(address, "funder address")?)),
        (_, None) => Err(ConfigError::EmptyField("funder_address")),
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Database(sqlx::Error),
    EmptyField(&'static str),
    InvalidAddress(&'static str),
    UnsupportedSignatureType,
    UnexpectedFunderForEoa,
    InvalidPolicyField(&'static str),
    NotionalAboveCeiling {
        leader_label: String,
        value: Decimal,
        ceiling: Decimal,
    },
    AccountIdentityLocked,
    NoLeaders,
    DuplicateLeaderLabel(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::EmptyField(field) => write!(formatter, "{field} must not be empty"),
            Self::InvalidAddress(field) => write!(formatter, "invalid {field}"),
            Self::UnsupportedSignatureType => write!(
                formatter,
                "signature_type must be one of eoa, proxy, gnosis_safe, poly1271"
            ),
            Self::UnexpectedFunderForEoa => write!(
                formatter,
                "funder_address must not be set when signature_type is eoa (the signer funds itself)"
            ),
            Self::InvalidPolicyField(field) => write!(formatter, "invalid policy field: {field}"),
            Self::NotionalAboveCeiling { leader_label, value, ceiling } => write!(
                formatter,
                "leader \"{leader_label}\": max_order_notional {value} exceeds the configured \
                 ceiling {ceiling} -- raise POLYCOPY_CONFIG_MAX_NOTIONAL_CEILING explicitly if \
                 this is intended"
            ),
            Self::AccountIdentityLocked => write!(
                formatter,
                "refusing to change this account's funder_address/signature_type: it already has \
                 recorded activity (copy_intents, reconciliation_cases, or position_lots). Add a \
                 new account instead of editing this one's wallet identity"
            ),
            Self::NoLeaders => write!(formatter, "config must list at least one leader"),
            Self::DuplicateLeaderLabel(label) => {
                write!(formatter, "leader label \"{label}\" appears more than once")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDb {
        pool: SqlitePool,
        path: std::path::PathBuf,
    }

    impl TestDb {
        async fn new() -> Self {
            use std::{
                env, process,
                sync::atomic::{AtomicU64, Ordering},
                time::{SystemTime, UNIX_EPOCH},
            };
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time must be after the Unix epoch")
                .as_nanos();
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "polycopy-engine-config-test-{}-{nonce}-{counter}.sqlite",
                process::id()
            ));
            let pool = crate::copytrading::db::open_and_migrate(&path)
                .await
                .expect("migrations must apply to a fresh database");
            Self { pool, path }
        }
    }

    impl std::ops::Deref for TestDb {
        type Target = SqlitePool;

        fn deref(&self) -> &SqlitePool {
            &self.pool
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
            let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
        }
    }

    fn policy(max_order_notional: &str) -> LeaderPolicyInput {
        LeaderPolicyInput {
            max_signal_age_seconds: 3,
            decision_window_seconds: 3,
            price_tolerance_bps: 100,
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            max_order_notional: max_order_notional.to_owned(),
            min_leader_trade_size: "0".to_owned(),
        }
    }

    fn config_with_one_leader(max_order_notional: &str) -> TradingConfig {
        TradingConfig {
            account: AccountConfigInput {
                label: "test-account".to_owned(),
                signature_type: "gnosis_safe".to_owned(),
                funder_address: Some("0x2222222222222222222222222222222222222222".to_owned()),
            },
            leaders: vec![LeaderConfigInput {
                label: "leader-a".to_owned(),
                enabled: true,
                addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
                policy: policy(max_order_notional),
            }],
        }
    }

    const SIGNING_ADDRESS: &str = "0x1111111111111111111111111111111111111111";

    #[tokio::test]
    async fn a_fresh_database_creates_the_account_and_leader() {
        let db = TestDb::new().await;
        let config = config_with_one_leader("1");

        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .expect("fresh config must apply");

        assert_eq!(summary.account_change, ChangeKind::Created);
        assert_eq!(summary.leaders.len(), 1);
        assert_eq!(summary.leaders[0].change, ChangeKind::Created);
        assert_eq!(summary.leaders[0].aliases_added, 1);

        let stored_notional: String =
            sqlx::query_scalar("SELECT max_order_notional FROM leader_policy WHERE leader_id = ?")
                .bind(summary.leaders[0].leader_id)
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(stored_notional, "1");
    }

    #[tokio::test]
    async fn reapplying_the_identical_config_reports_no_changes() {
        let db = TestDb::new().await;
        let config = config_with_one_leader("1");
        apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();

        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .expect("reapplying an identical config must succeed");

        assert_eq!(summary.account_change, ChangeKind::Unchanged);
        assert_eq!(summary.leaders[0].change, ChangeKind::Unchanged);
        assert_eq!(summary.leaders[0].aliases_added, 0);
        assert_eq!(summary.leaders[0].aliases_disabled, 0);
        assert!(!summary.leaders[0].policy_changed);
    }

    #[tokio::test]
    async fn a_second_leader_can_be_added_to_an_existing_account_without_touching_the_first() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();

        config.leaders.push(LeaderConfigInput {
            label: "leader-b".to_owned(),
            enabled: true,
            addresses: vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()],
            policy: policy("1"),
        });
        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .expect("adding a second leader must succeed");

        assert_eq!(summary.leaders[0].change, ChangeKind::Unchanged);
        assert_eq!(summary.leaders[1].change, ChangeKind::Created);
        let leader_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM leader_config")
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(leader_count, 2);
    }

    #[tokio::test]
    async fn a_leader_omitted_from_a_later_config_is_disabled_not_deleted() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        config.leaders.push(LeaderConfigInput {
            label: "leader-b".to_owned(),
            enabled: true,
            addresses: vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()],
            policy: policy("1"),
        });
        let first = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();
        let leader_b_id = first.leaders[1].leader_id;

        // leader-b still has a real open lot -- Q1's answer was that
        // omission disables it anyway, with no extra warning.
        sqlx::query(
            "INSERT INTO position_lots (account_id, leader_id, token_id, qty) \
             VALUES (?, ?, 'token-1', '5')",
        )
        .bind(first.account_id)
        .bind(leader_b_id)
        .execute(&*db)
        .await
        .unwrap();

        config.leaders.pop(); // drop leader-b from the config
        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .expect("omitting a leader must still succeed, not error");

        let disabled = summary
            .leaders
            .iter()
            .find(|leader| leader.leader_id == leader_b_id)
            .expect("the omitted leader must still appear in the summary");
        assert_eq!(disabled.change, ChangeKind::Updated);

        let (enabled, leader_count): (bool, i64) = (
            sqlx::query_scalar("SELECT enabled FROM leader_config WHERE id = ?")
                .bind(leader_b_id)
                .fetch_one(&*db)
                .await
                .unwrap(),
            sqlx::query_scalar("SELECT COUNT(*) FROM leader_config")
                .fetch_one(&*db)
                .await
                .unwrap(),
        );
        assert!(!enabled, "leader-b must be disabled, not left alone");
        assert_eq!(leader_count, 2, "leader-b must still exist, not be deleted");

        let lot_qty: String =
            sqlx::query_scalar("SELECT qty FROM position_lots WHERE leader_id = ?")
                .bind(leader_b_id)
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(lot_qty, "5", "disabling must never touch existing lots");
    }

    #[tokio::test]
    async fn an_already_disabled_omitted_leader_is_reported_unchanged() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        config.leaders.push(LeaderConfigInput {
            label: "leader-b".to_owned(),
            enabled: false,
            addresses: vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()],
            policy: policy("1"),
        });
        apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();

        config.leaders.pop();
        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();

        let leader_b = summary
            .leaders
            .iter()
            .find(|leader| leader.label == "leader-b")
            .unwrap();
        assert_eq!(leader_b.change, ChangeKind::Unchanged);
    }

    #[tokio::test]
    async fn a_new_address_for_an_existing_leader_is_added_as_an_alias() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();

        config.leaders[0]
            .addresses
            .push("0xcccccccccccccccccccccccccccccccccccccccc".to_owned());
        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .expect("adding an alias must succeed");

        assert_eq!(summary.leaders[0].aliases_added, 1);
        assert_eq!(summary.leaders[0].aliases_disabled, 0);
        let alias_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM leader_wallet_aliases WHERE leader_id = ?")
                .bind(summary.leaders[0].leader_id)
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(alias_count, 2);
    }

    #[tokio::test]
    async fn removing_an_address_disables_the_alias_instead_of_deleting_it() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        config.leaders[0]
            .addresses
            .push("0xcccccccccccccccccccccccccccccccccccccccc".to_owned());
        apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();

        config.leaders[0].addresses.remove(1);
        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .expect("removing an address from the config must succeed");

        assert_eq!(summary.leaders[0].aliases_disabled, 1);
        let rows: Vec<(String, bool)> = sqlx::query_as(
            "SELECT address, enabled FROM leader_wallet_aliases WHERE leader_id = ?",
        )
        .bind(summary.leaders[0].leader_id)
        .fetch_all(&*db)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2, "the disabled alias row must still exist");
        let disabled_row = rows
            .iter()
            .find(|(address, _)| address == "0xcccccccccccccccccccccccccccccccccccccccc")
            .unwrap();
        assert!(!disabled_row.1);
    }

    #[tokio::test]
    async fn a_policy_field_change_updates_leader_policy() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();

        config.leaders[0].policy.max_signal_age_seconds = 30;
        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .expect("policy change must succeed");

        assert!(summary.leaders[0].policy_changed);
        let stored: i64 = sqlx::query_scalar(
            "SELECT max_signal_age_seconds FROM leader_policy WHERE leader_id = ?",
        )
        .bind(summary.leaders[0].leader_id)
        .fetch_one(&*db)
        .await
        .unwrap();
        assert_eq!(stored, 30);
    }

    #[tokio::test]
    async fn a_notional_above_the_default_ceiling_is_rejected_and_writes_nothing() {
        let db = TestDb::new().await;
        let config = config_with_one_leader("5");

        let result = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await;

        assert!(matches!(
            result,
            Err(ConfigError::NotionalAboveCeiling { .. })
        ));
        let account_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(account_count, 0);
    }

    #[tokio::test]
    async fn a_notional_within_a_raised_ceiling_is_accepted() {
        let db = TestDb::new().await;
        let config = config_with_one_leader("5");

        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions {
                max_notional_ceiling: Decimal::new(10, 0),
            },
        )
        .await
        .expect("a notional within the raised ceiling must be accepted");

        assert_eq!(summary.account_change, ChangeKind::Created);
    }

    #[tokio::test]
    async fn account_identity_is_locked_once_the_account_has_recorded_activity() {
        let db = TestDb::new().await;
        let config = config_with_one_leader("1");
        let first = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO position_lots (account_id, leader_id, token_id, qty) \
             VALUES (?, ?, 'token-1', '5')",
        )
        .bind(first.account_id)
        .bind(first.leaders[0].leader_id)
        .execute(&*db)
        .await
        .unwrap();

        let mut changed = config.clone();
        changed.account.funder_address =
            Some("0x3333333333333333333333333333333333333333".to_owned());

        let result = apply_trading_config(
            &db,
            &changed,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await;

        assert!(matches!(result, Err(ConfigError::AccountIdentityLocked)));
        let stored_funder: Option<String> =
            sqlx::query_scalar("SELECT funder_address FROM accounts WHERE id = ?")
                .bind(first.account_id)
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(
            stored_funder.as_deref(),
            Some("0x2222222222222222222222222222222222222222")
        );
    }

    #[tokio::test]
    async fn account_identity_may_be_corrected_before_any_activity_exists() {
        let db = TestDb::new().await;
        let config = config_with_one_leader("1");
        apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .unwrap();

        let mut corrected = config.clone();
        corrected.account.funder_address =
            Some("0x3333333333333333333333333333333333333333".to_owned());

        let summary = apply_trading_config(
            &db,
            &corrected,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .expect("a typo fix before any activity must be allowed");

        assert_eq!(summary.account_change, ChangeKind::Updated);
    }

    #[tokio::test]
    async fn an_empty_leader_list_is_rejected() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        config.leaders.clear();

        let result = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await;

        assert!(matches!(result, Err(ConfigError::NoLeaders)));
    }

    #[tokio::test]
    async fn duplicate_leader_labels_in_one_config_are_rejected() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        let mut second = config.leaders[0].clone();
        second.addresses = vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()];
        config.leaders.push(second);

        let result = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await;

        assert!(matches!(result, Err(ConfigError::DuplicateLeaderLabel(_))));
    }

    #[tokio::test]
    async fn one_invalid_leader_blocks_the_whole_apply_including_otherwise_valid_leaders() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        config.leaders.push(LeaderConfigInput {
            label: "leader-b".to_owned(),
            enabled: true,
            addresses: vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()],
            policy: policy("5"), // exceeds the default ceiling
        });

        let result = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await;

        assert!(matches!(
            result,
            Err(ConfigError::NotionalAboveCeiling { .. })
        ));
        let leader_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM leader_config")
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(
            leader_count, 0,
            "leader-a must not have been written either"
        );
    }

    #[tokio::test]
    async fn an_unsupported_signature_type_with_no_funder_address_reports_the_right_error() {
        // Regression: signature_type validity must be checked before
        // funder_address is normalized, or a bogus signature_type with no
        // funder_address gets misreported as a missing funder_address
        // instead of the actually-wrong field.
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        config.account.signature_type = "bogus".to_owned();
        config.account.funder_address = None;

        let result = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await;

        assert!(matches!(result, Err(ConfigError::UnsupportedSignatureType)));
    }

    #[tokio::test]
    async fn eoa_with_a_funder_address_is_rejected() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        config.account.signature_type = "eoa".to_owned();

        let result = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await;

        assert!(matches!(result, Err(ConfigError::UnexpectedFunderForEoa)));
    }

    #[tokio::test]
    async fn eoa_without_a_funder_address_is_accepted() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        config.account.signature_type = "eoa".to_owned();
        config.account.funder_address = None;

        let summary = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await
        .expect("eoa without a funder address must be accepted");

        let stored_funder: Option<String> =
            sqlx::query_scalar("SELECT funder_address FROM accounts WHERE id = ?")
                .bind(summary.account_id)
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(stored_funder, None);
    }

    #[tokio::test]
    async fn an_invalid_min_max_price_range_is_rejected() {
        let db = TestDb::new().await;
        let mut config = config_with_one_leader("1");
        config.leaders[0].policy.min_price = "0.99".to_owned();
        config.leaders[0].policy.max_price = "0.01".to_owned();

        let result = apply_trading_config(
            &db,
            &config,
            SIGNING_ADDRESS,
            &ConfigApplyOptions::default(),
        )
        .await;

        assert!(matches!(result, Err(ConfigError::InvalidPolicyField(_))));
    }
}
