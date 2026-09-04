//! Persistent live-copy safety state.
//!
//! This module is database-only. It owns the persistent-mode config row,
//! account fuse, and the rolling budget reservation that must be written in
//! the same transaction that moves an attempt to `submitting`.

use std::{collections::BTreeSet, env, fmt, time::Duration};

use chrono::{DateTime, SecondsFormat, Utc};
use rust_decimal::Decimal;
use sqlx::SqlitePool;

use crate::copytrading::orchestrate::{OrchestrateError, SubmitAttemptMarker};

pub const EXIT_LOCK_COLLISION: i32 = 20;
pub const EXIT_FUSE_OPEN: i32 = 21;
pub const EXIT_CONFIG: i32 = 22;
pub const EXIT_UNRESOLVED_RECOVERY: i32 = 23;
pub const EXIT_BUDGET_STATE: i32 = 24;

pub const EXECUTE_ENV: &str = "POLYCOPY_PERSISTENT_EXECUTE";
pub const ENGINE_EXECUTE_ENV: &str = "POLYCOPY_ENGINE_EXECUTE";
pub const ACCOUNT_ID_ENV: &str = "POLYCOPY_PERSISTENT_ACCOUNT_ID";
pub const ALLOWED_LEADERS_ENV: &str = "POLYCOPY_PERSISTENT_ALLOWED_LEADER_IDS";
pub const MAX_ORDER_NOTIONAL_ENV: &str = "POLYCOPY_PERSISTENT_MAX_ORDER_NOTIONAL";
pub const ROLLING_BUDGET_ENV: &str = "POLYCOPY_PERSISTENT_ROLLING_BUDGET_USDC";
pub const BUDGET_WINDOW_ENV: &str = "POLYCOPY_PERSISTENT_BUDGET_WINDOW_SECONDS";
pub const TICK_SECONDS_ENV: &str = "POLYCOPY_PERSISTENT_TICK_SECONDS";
pub const BACKFILL_SECONDS_ENV: &str = "POLYCOPY_PERSISTENT_BACKFILL_EVERY_SECONDS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentRuntimeConfig {
    pub account_id: i64,
    pub enabled: bool,
    pub allowed_leader_ids: BTreeSet<i64>,
    pub max_order_notional: Decimal,
    pub rolling_budget: Decimal,
    pub budget_window: Duration,
    pub tick: Duration,
    pub backfill_every: Duration,
}

impl PersistentRuntimeConfig {
    pub fn from_env() -> Result<Self, PersistentError> {
        if env::var(ENGINE_EXECUTE_ENV).ok().as_deref() != Some("yes") {
            return Err(PersistentError::Config(format!(
                "{ENGINE_EXECUTE_ENV} must be set to the exact value yes"
            )));
        }
        if env::var(EXECUTE_ENV).ok().as_deref() != Some("yes") {
            return Err(PersistentError::Config(format!(
                "{EXECUTE_ENV} must be set to the exact value yes"
            )));
        }
        Self::from_values(
            parse_i64_env(ACCOUNT_ID_ENV)?,
            true,
            &required_env(ALLOWED_LEADERS_ENV)?,
            &required_env(MAX_ORDER_NOTIONAL_ENV)?,
            &required_env(ROLLING_BUDGET_ENV)?,
            parse_u64_env(BUDGET_WINDOW_ENV)?,
            parse_u64_env(TICK_SECONDS_ENV)?,
            parse_u64_env(BACKFILL_SECONDS_ENV)?,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_values(
        account_id: i64,
        enabled: bool,
        allowed_leaders: &str,
        max_order_notional: &str,
        rolling_budget: &str,
        budget_window_seconds: u64,
        tick_seconds: u64,
        backfill_every_seconds: u64,
    ) -> Result<Self, PersistentError> {
        if account_id <= 0 {
            return Err(PersistentError::Config(
                "persistent account id must be positive".to_owned(),
            ));
        }
        let allowed_leader_ids = parse_allowed_leaders(allowed_leaders)?;
        let max_order_notional = parse_decimal(max_order_notional, MAX_ORDER_NOTIONAL_ENV)?;
        if !(max_order_notional > Decimal::ZERO && max_order_notional <= Decimal::ONE) {
            return Err(PersistentError::Config(
                "persistent max order notional must be > 0 and <= 1 USDC".to_owned(),
            ));
        }
        let rolling_budget = parse_decimal(rolling_budget, ROLLING_BUDGET_ENV)?;
        if rolling_budget <= Decimal::ZERO {
            return Err(PersistentError::Config(
                "persistent rolling budget must be greater than 0 USDC".to_owned(),
            ));
        }
        if budget_window_seconds != 86_400 {
            return Err(PersistentError::Config(
                "persistent budget window must be exactly 86400 seconds".to_owned(),
            ));
        }
        if tick_seconds == 0 || backfill_every_seconds == 0 {
            return Err(PersistentError::Config(
                "persistent tick and backfill intervals must be positive".to_owned(),
            ));
        }
        Ok(Self {
            account_id,
            enabled,
            allowed_leader_ids,
            max_order_notional,
            rolling_budget,
            budget_window: Duration::from_secs(budget_window_seconds),
            tick: Duration::from_secs(tick_seconds),
            backfill_every: Duration::from_secs(backfill_every_seconds),
        })
    }

    pub fn allowed_leaders_text(&self) -> String {
        self.allowed_leader_ids
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

pub struct PersistentSubmitMarker<'a> {
    pub config: &'a PersistentRuntimeConfig,
}

impl SubmitAttemptMarker for PersistentSubmitMarker<'_> {
    fn mark_submitting<'a>(
        &'a self,
        pool: &'a SqlitePool,
        intent_id: i64,
        attempt_id: i64,
        now: DateTime<Utc>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), OrchestrateError>> + Send + 'a>,
    > {
        Box::pin(async move {
            reserve_budget_and_mark_submitting(pool, self.config, intent_id, attempt_id, now)
                .await
                .map_err(OrchestrateError::Persistent)
        })
    }
}

pub async fn init_config(
    pool: &SqlitePool,
    config: &PersistentRuntimeConfig,
) -> Result<(), PersistentError> {
    let mut tx = pool.begin().await.map_err(db_err)?;
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_execution_config")
        .fetch_one(&mut *tx)
        .await
        .map_err(db_err)?;
    if exists != 0 {
        return Err(PersistentError::AlreadyConfigured);
    }
    validate_account_and_leaders_tx(&mut tx, config).await?;
    sqlx::query(
        "INSERT INTO persistent_execution_config \
         (id, account_id, enabled, allowed_leader_ids, max_order_notional_usdc, rolling_budget_usdc, \
          budget_window_seconds, tick_seconds, backfill_every_seconds) \
         VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(config.account_id)
    .bind(if config.enabled { 1 } else { 0 })
    .bind(config.allowed_leaders_text())
    .bind(config.max_order_notional.to_string())
    .bind(config.rolling_budget.to_string())
    .bind(config.budget_window.as_secs() as i64)
    .bind(config.tick.as_secs() as i64)
    .bind(config.backfill_every.as_secs() as i64)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)
}

/// Replaces the database-owned persistent runtime configuration while no
/// execution process owns the database.  This is deliberately a code path,
/// not a manual SQLite edit: the incoming leader set must exactly equal the
/// enabled leaders, recovery must be clean, and an operator cannot lower the
/// rolling ceiling below reservations that are still inside its own window.
pub async fn reconfigure_config(
    pool: &SqlitePool,
    config: &PersistentRuntimeConfig,
) -> Result<(), PersistentError> {
    assert_startup_clear(pool, config.account_id).await?;
    validate_account_and_leaders(pool, config).await?;

    let reserved =
        rolling_reserved_total(pool, config.account_id, config.budget_window, Utc::now()).await?;
    if reserved > config.rolling_budget {
        return Err(PersistentError::BudgetExceeded {
            used: reserved,
            requested: Decimal::ZERO,
            cap: config.rolling_budget,
        });
    }

    let changed = sqlx::query(
        "UPDATE persistent_execution_config SET account_id = ?, enabled = ?, \
         allowed_leader_ids = ?, max_order_notional_usdc = ?, rolling_budget_usdc = ?, \
         budget_window_seconds = ?, tick_seconds = ?, backfill_every_seconds = ? WHERE id = 1",
    )
    .bind(config.account_id)
    .bind(if config.enabled { 1 } else { 0 })
    .bind(config.allowed_leaders_text())
    .bind(config.max_order_notional.to_string())
    .bind(config.rolling_budget.to_string())
    .bind(config.budget_window.as_secs() as i64)
    .bind(config.tick.as_secs() as i64)
    .bind(config.backfill_every.as_secs() as i64)
    .execute(pool)
    .await
    .map_err(db_err)?;
    if changed.rows_affected() != 1 {
        return Err(PersistentError::MissingConfig);
    }
    Ok(())
}

pub async fn verify_config(
    pool: &SqlitePool,
    expected: &PersistentRuntimeConfig,
) -> Result<(), PersistentError> {
    type ConfigRow = (i64, i64, String, String, String, i64, i64, i64);
    let row: Option<ConfigRow> = sqlx::query_as(
        "SELECT account_id, enabled, allowed_leader_ids, max_order_notional_usdc, \
         rolling_budget_usdc, budget_window_seconds, tick_seconds, backfill_every_seconds \
         FROM persistent_execution_config WHERE id = 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(db_err)?;
    let Some(row) = row else {
        return Err(PersistentError::MissingConfig);
    };
    let actual = PersistentRuntimeConfig::from_values(
        row.0,
        row.1 == 1,
        &row.2,
        &row.3,
        &row.4,
        row.5 as u64,
        row.6 as u64,
        row.7 as u64,
    )?;
    if actual != *expected {
        return Err(PersistentError::ConfigMismatch);
    }
    if !actual.enabled {
        return Err(PersistentError::Config(
            "persistent database config is disabled".to_owned(),
        ));
    }
    validate_account_and_leaders(pool, expected).await
}

pub async fn assert_startup_clear(
    pool: &SqlitePool,
    account_id: i64,
) -> Result<(), PersistentError> {
    let open_cases: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases WHERE account_id = ? AND resolved_at IS NULL",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .map_err(db_err)?;
    let nonterminal: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM order_attempts oa \
         JOIN copy_intents ci ON ci.id = oa.intent_id \
         WHERE ci.account_id = ? AND oa.status IN ('submitting', 'uncertain')",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .map_err(db_err)?;
    if open_cases != 0 || nonterminal != 0 {
        return Err(PersistentError::UnresolvedRecovery);
    }
    Ok(())
}

pub async fn pause_fuse(
    pool: &SqlitePool,
    account_id: i64,
    reason: &str,
    actor_source: &str,
) -> Result<(), PersistentError> {
    let reason = non_empty(reason, "reason")?;
    let actor_source = non_empty(actor_source, "actor_source")?;
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    sqlx::query(
        "INSERT INTO persistent_execution_fuse (account_id, paused_at, reason, actor_source, updated_at) \
         VALUES (?, ?, ?, ?, ?) \
         ON CONFLICT(account_id) DO UPDATE SET \
         paused_at = excluded.paused_at, reason = excluded.reason, \
         actor_source = excluded.actor_source, updated_at = excluded.updated_at",
    )
    .bind(account_id)
    .bind(&now)
    .bind(reason)
    .bind(actor_source)
    .bind(&now)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(db_err)
}

pub async fn resume_fuse(
    pool: &SqlitePool,
    account_id: i64,
    reason: &str,
) -> Result<(), PersistentError> {
    let _ = non_empty(reason, "reason")?;
    assert_startup_clear(pool, account_id).await?;
    let result = sqlx::query("DELETE FROM persistent_execution_fuse WHERE account_id = ?")
        .bind(account_id)
        .execute(pool)
        .await
        .map_err(db_err)?;
    if result.rows_affected() == 0 {
        return Err(PersistentError::FuseNotOpen);
    }
    Ok(())
}

/// Resolves exactly one account-level reconciliation case that was opened
/// before an order attempt existed. This is deliberately narrower than a
/// general reconciliation control: it cannot resolve a case with any attempt,
/// so it can never bless an order that might have crossed the venue boundary.
/// The caller must first complete a fresh strict collateral/allowance read.
pub async fn resolve_pre_submit_balance_case(
    pool: &SqlitePool,
    account_id: i64,
) -> Result<i64, PersistentError> {
    let mut tx = pool.begin().await.map_err(db_err)?;
    let rows: Vec<(i64, i64, String)> = sqlx::query_as(
        "SELECT rc.id, rc.intent_id, ci.status \
         FROM reconciliation_cases rc \
         JOIN copy_intents ci ON ci.id = rc.intent_id \
         WHERE rc.account_id = ? AND rc.resolved_at IS NULL \
           AND rc.case_type = 'balance_drift' \
           AND rc.detail = 'strict collateral allowance query failed'",
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(db_err)?;
    let [(case_id, intent_id, status)] = rows.as_slice() else {
        return Err(PersistentError::UnresolvedRecovery);
    };
    if status != "needs_reconcile" {
        return Err(PersistentError::UnresolvedRecovery);
    }
    let attempts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(db_err)?;
    if attempts != 0 {
        return Err(PersistentError::UnresolvedRecovery);
    }

    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let intent_update = sqlx::query(
        "UPDATE copy_intents SET status = 'rejected', \
         rejection_reason = 'pre-submission allowance failure reconciled; signal is not replayed', \
         updated_at = ? WHERE id = ? AND status = 'needs_reconcile'",
    )
    .bind(&now)
    .bind(intent_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    if intent_update.rows_affected() != 1 {
        return Err(PersistentError::UnresolvedRecovery);
    }
    let case_update = sqlx::query(
        "UPDATE reconciliation_cases SET resolved_at = ?, \
         resolution = 'fresh strict collateral and allowance read passed; no order attempt existed' \
         WHERE id = ? AND resolved_at IS NULL",
    )
    .bind(&now)
    .bind(case_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    if case_update.rows_affected() != 1 {
        return Err(PersistentError::UnresolvedRecovery);
    }
    tx.commit().await.map_err(db_err)?;
    Ok(*case_id)
}

pub async fn fuse_status(
    pool: &SqlitePool,
    account_id: i64,
) -> Result<Option<(String, String, String)>, PersistentError> {
    sqlx::query_as(
        "SELECT paused_at, reason, actor_source FROM persistent_execution_fuse WHERE account_id = ?",
    )
    .bind(account_id)
    .fetch_optional(pool)
    .await
    .map_err(db_err)
}

pub async fn ensure_fuse_clear(pool: &SqlitePool, account_id: i64) -> Result<(), PersistentError> {
    if fuse_status(pool, account_id).await?.is_some() {
        Err(PersistentError::FuseOpen)
    } else {
        Ok(())
    }
}

pub async fn rolling_reserved_total(
    pool: &SqlitePool,
    account_id: i64,
    window: Duration,
    now: DateTime<Utc>,
) -> Result<Decimal, PersistentError> {
    let cutoff = now - chrono::Duration::seconds(window.as_secs() as i64);
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT amount_usdc FROM persistent_budget_reservations \
         WHERE account_id = ? AND state = 'reserved' AND reserved_at >= ?",
    )
    .bind(account_id)
    .bind(cutoff.to_rfc3339_opts(SecondsFormat::Millis, true))
    .fetch_all(pool)
    .await
    .map_err(db_err)?;
    let mut total = Decimal::ZERO;
    for row in rows {
        let amount = row
            .parse::<Decimal>()
            .map_err(|_| PersistentError::MalformedBudgetState)?;
        if amount <= Decimal::ZERO {
            return Err(PersistentError::MalformedBudgetState);
        }
        total += amount;
    }
    Ok(total)
}

pub async fn reserve_budget_and_mark_submitting(
    pool: &SqlitePool,
    config: &PersistentRuntimeConfig,
    intent_id: i64,
    attempt_id: i64,
    now: DateTime<Utc>,
) -> Result<(), PersistentError> {
    let mut conn = pool.acquire().await.map_err(db_err)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

    let result: Result<(), PersistentError> = async {
        let row: (i64, Option<String>, Option<String>, String) = sqlx::query_as(
            "SELECT account_id, decision_deadline_at, planned_notional_usdc, status \
             FROM copy_intents WHERE id = ?",
        )
        .bind(intent_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
        if row.0 != config.account_id {
            return Err(PersistentError::ConfigMismatch);
        }
        let fuse_open: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM persistent_execution_fuse WHERE account_id = ?)",
        )
        .bind(config.account_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
        if fuse_open != 0 {
            return Err(PersistentError::FuseOpen);
        }
        if let Some(deadline) = row.1 {
            let deadline = DateTime::parse_from_rfc3339(&deadline)
                .map_err(|_| PersistentError::MalformedBudgetState)?
                .with_timezone(&Utc);
            if now > deadline {
                sqlx::query(
                    "UPDATE copy_intents SET status = 'cancelled', \
                     rejection_reason = 'decision deadline expired', \
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
                )
                .bind(intent_id)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
                return Err(PersistentError::DecisionExpired);
            }
        }
        if row.3 != "in_progress" {
            return Err(PersistentError::InvalidAttemptTransition);
        }
        let amount = row
            .2
            .ok_or(PersistentError::MalformedBudgetState)?
            .parse::<Decimal>()
            .map_err(|_| PersistentError::MalformedBudgetState)?;
        if amount <= Decimal::ZERO || amount > config.max_order_notional {
            return Err(PersistentError::BudgetExceeded {
                used: Decimal::ZERO,
                requested: amount,
                cap: config.max_order_notional,
            });
        }

        let cutoff = now - chrono::Duration::seconds(config.budget_window.as_secs() as i64);
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT amount_usdc FROM persistent_budget_reservations \
             WHERE account_id = ? AND state = 'reserved' AND reserved_at >= ?",
        )
        .bind(config.account_id)
        .bind(cutoff.to_rfc3339_opts(SecondsFormat::Millis, true))
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
        let mut used = Decimal::ZERO;
        for row in rows {
            let existing = row
                .parse::<Decimal>()
                .map_err(|_| PersistentError::MalformedBudgetState)?;
            if existing <= Decimal::ZERO {
                return Err(PersistentError::MalformedBudgetState);
            }
            used += existing;
        }
        if used + amount > config.rolling_budget {
            return Err(PersistentError::BudgetExceeded {
                used,
                requested: amount,
                cap: config.rolling_budget,
            });
        }

        let update = sqlx::query(
            "UPDATE order_attempts SET status = 'submitting', submission_started_at = ?, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
             WHERE id = ? AND intent_id = ? AND status = 'prepared'",
        )
        .bind(now.to_rfc3339_opts(SecondsFormat::Millis, true))
        .bind(attempt_id)
        .bind(intent_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        if update.rows_affected() != 1 {
            return Err(PersistentError::InvalidAttemptTransition);
        }
        sqlx::query(
            "INSERT INTO persistent_budget_reservations \
             (order_attempt_id, account_id, amount_usdc, reserved_at, state) \
             VALUES (?, ?, ?, ?, 'reserved')",
        )
        .bind(attempt_id)
        .bind(config.account_id)
        .bind(amount.to_string())
        .bind(now.to_rfc3339_opts(SecondsFormat::Millis, true))
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }
    .await;

    match &result {
        Ok(()) => {
            sqlx::query("COMMIT")
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
        }
        Err(PersistentError::DecisionExpired) => {
            sqlx::query("COMMIT")
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
        }
        Err(_) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
        }
    }
    result
}

pub async fn release_pre_boundary_failure(
    pool: &SqlitePool,
    attempt_id: i64,
    reason: &str,
) -> Result<(), PersistentError> {
    let reason = non_empty(reason, "reason")?;
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    sqlx::query(
        "UPDATE persistent_budget_reservations \
         SET state = 'released_pre_boundary', release_reason = ?, released_at = ? \
         WHERE order_attempt_id = ? AND state = 'reserved'",
    )
    .bind(reason)
    .bind(now)
    .bind(attempt_id)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(db_err)
}

/// Releases a rolling-budget reservation only for an attempt with a durable,
/// definitive venue rejection. It is intentionally unavailable for uncertain
/// or merely prepared attempts, which could still require reconciliation.
pub async fn release_definitive_rejection(
    pool: &SqlitePool,
    account_id: i64,
    attempt_id: i64,
) -> Result<(), PersistentError> {
    let definitive: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM order_attempts oa \
         JOIN copy_intents ci ON ci.id = oa.intent_id \
         WHERE oa.id = ? AND ci.account_id = ? AND oa.status = 'rejected' \
           AND oa.submission_started_at IS NOT NULL \
           AND oa.failure_detail IS NOT NULL AND trim(oa.failure_detail) <> '')",
    )
    .bind(attempt_id)
    .bind(account_id)
    .fetch_one(pool)
    .await
    .map_err(db_err)?;
    if definitive == 0 {
        return Err(PersistentError::UnresolvedRecovery);
    }
    release_pre_boundary_failure(
        pool,
        attempt_id,
        "operator verified definitive venue rejection",
    )
    .await
}

async fn validate_account_and_leaders(
    pool: &SqlitePool,
    config: &PersistentRuntimeConfig,
) -> Result<(), PersistentError> {
    let mut tx = pool.begin().await.map_err(db_err)?;
    let result = validate_account_and_leaders_tx(&mut tx, config).await;
    tx.commit().await.map_err(db_err)?;
    result
}

async fn validate_account_and_leaders_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    config: &PersistentRuntimeConfig,
) -> Result<(), PersistentError> {
    let account_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts WHERE id = ?")
        .bind(config.account_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(db_err)?;
    if account_count != 1 {
        return Err(PersistentError::Config(
            "persistent account does not exist".to_owned(),
        ));
    }
    let enabled: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM leader_config WHERE enabled = 1 ORDER BY id")
            .fetch_all(&mut **tx)
            .await
            .map_err(db_err)?;
    let enabled: BTreeSet<i64> = enabled.into_iter().collect();
    if enabled != config.allowed_leader_ids {
        return Err(PersistentError::ConfigMismatch);
    }
    Ok(())
}

fn required_env(name: &'static str) -> Result<String, PersistentError> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| PersistentError::Config(format!("missing {name}")))
}

fn parse_i64_env(name: &'static str) -> Result<i64, PersistentError> {
    required_env(name)?
        .parse()
        .map_err(|_| PersistentError::Config(format!("invalid {name}")))
}

fn parse_u64_env(name: &'static str) -> Result<u64, PersistentError> {
    let value: u64 = required_env(name)?
        .parse()
        .map_err(|_| PersistentError::Config(format!("invalid {name}")))?;
    if value == 0 {
        return Err(PersistentError::Config(format!("{name} must be positive")));
    }
    Ok(value)
}

fn parse_allowed_leaders(raw: &str) -> Result<BTreeSet<i64>, PersistentError> {
    let mut ids = BTreeSet::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let id: i64 = trimmed
            .parse()
            .map_err(|_| PersistentError::Config(format!("invalid leader id: {trimmed}")))?;
        if id <= 0 {
            return Err(PersistentError::Config(
                "leader ids must be positive".to_owned(),
            ));
        }
        ids.insert(id);
    }
    if ids.is_empty() {
        return Err(PersistentError::Config(
            "at least one allowed leader is required".to_owned(),
        ));
    }
    Ok(ids)
}

fn parse_decimal(raw: &str, name: &'static str) -> Result<Decimal, PersistentError> {
    raw.parse()
        .map_err(|_| PersistentError::Config(format!("invalid {name}")))
}

fn non_empty<'a>(value: &'a str, field: &'static str) -> Result<&'a str, PersistentError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(PersistentError::Config(format!(
            "{field} must not be empty"
        )))
    } else {
        Ok(trimmed)
    }
}

fn db_err(error: sqlx::Error) -> PersistentError {
    PersistentError::Database(error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistentError {
    Database(String),
    Config(String),
    MissingConfig,
    ConfigMismatch,
    AlreadyConfigured,
    FuseOpen,
    FuseNotOpen,
    UnresolvedRecovery,
    MalformedBudgetState,
    InvalidAttemptTransition,
    DecisionExpired,
    BudgetExceeded {
        used: Decimal,
        requested: Decimal,
        cap: Decimal,
    },
}

impl PersistentError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::FuseOpen => EXIT_FUSE_OPEN,
            Self::Config(_)
            | Self::MissingConfig
            | Self::ConfigMismatch
            | Self::AlreadyConfigured => EXIT_CONFIG,
            Self::UnresolvedRecovery => EXIT_UNRESOLVED_RECOVERY,
            Self::MalformedBudgetState | Self::BudgetExceeded { .. } => EXIT_BUDGET_STATE,
            Self::Database(_)
            | Self::FuseNotOpen
            | Self::InvalidAttemptTransition
            | Self::DecisionExpired => EXIT_FUSE_OPEN,
        }
    }
}

impl fmt::Display for PersistentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "persistent database error: {error}"),
            Self::Config(error) => write!(formatter, "persistent config error: {error}"),
            Self::MissingConfig => write!(formatter, "persistent config is missing"),
            Self::ConfigMismatch => write!(
                formatter,
                "persistent runtime config does not match the database"
            ),
            Self::AlreadyConfigured => write!(formatter, "persistent config already exists"),
            Self::FuseOpen => write!(formatter, "persistent execution fuse is open"),
            Self::FuseNotOpen => write!(formatter, "persistent execution fuse is not open"),
            Self::UnresolvedRecovery => write!(
                formatter,
                "persistent startup found unresolved reconciliation or uncertain submission state"
            ),
            Self::MalformedBudgetState => write!(formatter, "persistent budget state is malformed"),
            Self::InvalidAttemptTransition => {
                write!(formatter, "persistent attempt transition is invalid")
            }
            Self::DecisionExpired => write!(
                formatter,
                "persistent decision deadline expired before submit"
            ),
            Self::BudgetExceeded {
                used,
                requested,
                cap,
            } => write!(
                formatter,
                "persistent rolling budget exceeded: used={used} requested={requested} cap={cap}"
            ),
        }
    }
}

impl std::error::Error for PersistentError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::copytrading::{
        db::open_and_migrate,
        reconcile::{load_or_prepare_attempt, PreparedOrderEnvelope},
    };

    struct TestDb {
        pool: SqlitePool,
        path: std::path::PathBuf,
    }

    impl TestDb {
        async fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);

            let path = std::env::temp_dir().join(format!(
                "polycopy-engine-persistent-test-{}-{}.sqlite",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            let pool = open_and_migrate(&path).await.expect("migrate");
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
            let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
            let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
        }
    }

    fn cfg() -> PersistentRuntimeConfig {
        PersistentRuntimeConfig::from_values(1, true, "1", "1", "5", 86_400, 1, 60)
            .expect("valid config")
    }

    async fn seed_base(db: &SqlitePool) {
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, funder_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', \
             '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'gnosis_safe')",
        )
        .execute(db)
        .await
        .expect("account");
        sqlx::query("INSERT INTO leader_config (id, label, enabled) VALUES (1, 'leader', 1)")
            .execute(db)
            .await
            .expect("leader");
    }

    async fn seed_attempt(
        db: &SqlitePool,
        event_key: &str,
        notional: &str,
        deadline: DateTime<Utc>,
    ) -> (i64, i64) {
        let event_id: i64 = sqlx::query_scalar(
            "INSERT INTO leader_events \
             (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, price, occurred_at, observed_at) \
             VALUES (?, 1, '0xcond', '123456', 0, 'BUY', '1', '1', ?, ?) RETURNING id",
        )
        .bind(event_key)
        .bind(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
        .bind(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
        .fetch_one(db)
        .await
        .expect("event");
        let intent_id: i64 = sqlx::query_scalar(
            "INSERT INTO copy_intents \
             (event_id, account_id, leader_id, token_id, side, config_snapshot_json, config_snapshot_hash, \
              planned_qty, planned_price, planned_notional_usdc, reserved_qty, shard_scheme_version, lane_count, shard_id, status, decision_deadline_at) \
             VALUES (?, 1, 1, '123456', 'BUY', '{}', 'hash', ?, '1', ?, ?, 1, 1, 0, 'in_progress', ?) RETURNING id",
        )
        .bind(event_id)
        .bind(notional)
        .bind(notional)
        .bind(notional)
        .bind(deadline.to_rfc3339_opts(SecondsFormat::Millis, true))
        .fetch_one(db)
        .await
        .expect("intent");
        let envelope = PreparedOrderEnvelope {
            token_id: "123456".to_owned(),
            side: "BUY".to_owned(),
            price: "1".to_owned(),
            size: notional.to_owned(),
            salt: intent_id as u64,
            order_type: "FAK".to_owned(),
            expected_taker_order_id: format!("0x{intent_id:x}"),
            signed_order_json: "{}".to_owned(),
        };
        load_or_prepare_attempt(db, intent_id, 1, &envelope)
            .await
            .expect("attempt");
        let attempt_id: i64 =
            sqlx::query_scalar("SELECT id FROM order_attempts WHERE intent_id = ?")
                .bind(intent_id)
                .fetch_one(db)
                .await
                .expect("attempt id");
        (intent_id, attempt_id)
    }

    #[tokio::test]
    async fn init_refuses_to_overwrite_and_verify_requires_exact_config() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        let config = cfg();
        init_config(&db, &config).await.expect("init");
        assert!(matches!(
            init_config(&db, &config).await,
            Err(PersistentError::AlreadyConfigured)
        ));
        verify_config(&db, &config).await.expect("verify");
        let mismatched =
            PersistentRuntimeConfig::from_values(1, true, "1", "1", "4", 86_400, 1, 60)
                .expect("valid but different");
        assert_eq!(
            verify_config(&db, &mismatched).await,
            Err(PersistentError::ConfigMismatch)
        );
    }

    #[tokio::test]
    async fn persistent_config_accepts_multiple_leaders_and_a_configured_budget() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        sqlx::query("INSERT INTO leader_config (id, label, enabled) VALUES (2, 'leader-two', 1)")
            .execute(&*db)
            .await
            .expect("second leader");

        let config = PersistentRuntimeConfig::from_values(1, true, "1,2", "1", "50", 86_400, 1, 60)
            .expect(
                "a non-empty multi-leader configuration and explicit positive budget are valid",
            );
        init_config(&db, &config)
            .await
            .expect("init multi-leader config");
        verify_config(&db, &config)
            .await
            .expect("verify multi-leader config");
    }

    #[tokio::test]
    async fn reconfigure_updates_the_leader_scope_and_budget_only_when_recovery_is_clear() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        init_config(&db, &cfg()).await.expect("initial config");
        sqlx::query("INSERT INTO leader_config (id, label, enabled) VALUES (2, 'leader-two', 1)")
            .execute(&*db)
            .await
            .expect("second leader");
        let updated =
            PersistentRuntimeConfig::from_values(1, true, "1,2", "1", "50", 86_400, 1, 60)
                .expect("updated config");

        reconfigure_config(&db, &updated)
            .await
            .expect("clear recovery state permits a configuration update");
        verify_config(&db, &updated)
            .await
            .expect("updated config persisted");
    }

    #[tokio::test]
    async fn reconfigure_refuses_to_lower_the_budget_below_active_reservations() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        let initial = PersistentRuntimeConfig::from_values(1, true, "1", "1", "50", 86_400, 1, 60)
            .expect("initial config");
        init_config(&db, &initial)
            .await
            .expect("initial config persisted");
        let now = Utc::now();
        let (intent_id, attempt_id) =
            seed_attempt(&db, "reserved", "1", now + chrono::Duration::seconds(60)).await;
        reserve_budget_and_mark_submitting(&db, &initial, intent_id, attempt_id, now)
            .await
            .expect("reserve budget");
        sqlx::query("UPDATE order_attempts SET status = 'rejected' WHERE id = ?")
            .bind(attempt_id)
            .execute(&*db)
            .await
            .expect("mark terminal");
        let lower = PersistentRuntimeConfig::from_values(1, true, "1", "1", "0.5", 86_400, 1, 60)
            .expect("lower config parses");

        assert!(matches!(
            reconfigure_config(&db, &lower).await,
            Err(PersistentError::BudgetExceeded { .. })
        ));
    }

    #[tokio::test]
    async fn definitive_rejections_release_their_budget_reservation() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        let config = cfg();
        let now = Utc::now();
        for index in 0..5 {
            let (intent_id, attempt_id) = seed_attempt(
                &db,
                &format!("event-{index}"),
                "1",
                now + chrono::Duration::seconds(30),
            )
            .await;
            reserve_budget_and_mark_submitting(&db, &config, intent_id, attempt_id, now)
                .await
                .expect("reserve");
            sqlx::query("UPDATE order_attempts SET status = 'rejected' WHERE id = ?")
                .bind(attempt_id)
                .execute(&db.pool)
                .await
                .expect("mark rejected");
            release_pre_boundary_failure(&db, attempt_id, "definitive venue rejection")
                .await
                .expect("release rejected attempt");
        }
        let (sixth_intent, sixth_attempt) = seed_attempt(
            &db,
            "event-six",
            "1",
            now + chrono::Duration::seconds(86_500),
        )
        .await;
        reserve_budget_and_mark_submitting(&db, &config, sixth_intent, sixth_attempt, now)
            .await
            .expect("released rejections must free budget immediately");
    }

    #[tokio::test]
    async fn operator_release_requires_a_definitive_submitted_rejection() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        let config = cfg();
        let now = Utc::now();
        let (intent_id, attempt_id) = seed_attempt(
            &db,
            "operator-release",
            "1",
            now + chrono::Duration::seconds(60),
        )
        .await;
        reserve_budget_and_mark_submitting(&db, &config, intent_id, attempt_id, now)
            .await
            .expect("reserve");
        assert_eq!(
            release_definitive_rejection(&db, 1, attempt_id).await,
            Err(PersistentError::UnresolvedRecovery),
            "a merely submitting attempt must never be released by the operator control"
        );
        sqlx::query(
            "UPDATE order_attempts SET status = 'rejected', failure_detail = 'HTTP 400 definitive rejection' WHERE id = ?",
        )
        .bind(attempt_id)
        .execute(&db.pool)
        .await
        .expect("reject");
        release_definitive_rejection(&db, 1, attempt_id)
            .await
            .expect("definitive rejection releases");
        let state: String = sqlx::query_scalar(
            "SELECT state FROM persistent_budget_reservations WHERE order_attempt_id = ?",
        )
        .bind(attempt_id)
        .fetch_one(&db.pool)
        .await
        .expect("reservation");
        assert_eq!(state, "released_pre_boundary");
    }

    #[tokio::test]
    async fn expired_decision_is_cancelled_without_budget_reservation() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        let config = cfg();
        let now = Utc::now();
        let (intent_id, attempt_id) =
            seed_attempt(&db, "expired", "1", now - chrono::Duration::seconds(1)).await;
        assert_eq!(
            reserve_budget_and_mark_submitting(&db, &config, intent_id, attempt_id, now).await,
            Err(PersistentError::DecisionExpired)
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_budget_reservations")
            .fetch_one(&db.pool)
            .await
            .expect("count");
        let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .expect("status");
        assert_eq!(count, 0);
        assert_eq!(status, "cancelled");
    }

    #[tokio::test]
    async fn fuse_opened_before_the_marker_prevents_budget_reservation_and_submit_transition() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        let config = cfg();
        let now = Utc::now();
        let (intent_id, attempt_id) = seed_attempt(
            &db,
            "paused-before-marker",
            "1",
            now + chrono::Duration::seconds(60),
        )
        .await;
        pause_fuse(&db, 1, "operator pause", "test")
            .await
            .expect("pause");

        assert_eq!(
            reserve_budget_and_mark_submitting(&db, &config, intent_id, attempt_id, now).await,
            Err(PersistentError::FuseOpen)
        );
        let reservation_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM persistent_budget_reservations")
                .fetch_one(&db.pool)
                .await
                .expect("reservation count");
        let attempt_status: String =
            sqlx::query_scalar("SELECT status FROM order_attempts WHERE id = ?")
                .bind(attempt_id)
                .fetch_one(&db.pool)
                .await
                .expect("attempt status");
        assert_eq!(reservation_count, 0);
        assert_eq!(attempt_status, "prepared");
    }

    #[tokio::test]
    async fn fuse_blocks_and_manual_resume_requires_clean_recovery_state() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        pause_fuse(&db, 1, "operator pause", "test")
            .await
            .expect("pause");
        assert_eq!(
            ensure_fuse_clear(&db, 1).await,
            Err(PersistentError::FuseOpen)
        );
        let (_intent_id, attempt_id) = seed_attempt(
            &db,
            "uncertain",
            "1",
            Utc::now() + chrono::Duration::seconds(30),
        )
        .await;
        sqlx::query("UPDATE order_attempts SET status = 'uncertain' WHERE id = ?")
            .bind(attempt_id)
            .execute(&db.pool)
            .await
            .expect("uncertain");
        assert_eq!(
            resume_fuse(&db, 1, "resume").await,
            Err(PersistentError::UnresolvedRecovery)
        );
    }

    #[tokio::test]
    async fn pre_submit_balance_case_can_close_only_when_no_order_attempt_exists() {
        let db = TestDb::new().await;
        seed_base(&db).await;
        let event_id: i64 = sqlx::query_scalar(
            "INSERT INTO leader_events \
             (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, price, occurred_at, observed_at) \
             VALUES ('pre-submit-case', 1, '0xcond', '123456', 0, 'BUY', '1', '1', ?, ?) RETURNING id",
        )
        .bind(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
        .bind(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
        .fetch_one(&db.pool)
        .await
        .expect("event");
        let intent_id: i64 = sqlx::query_scalar(
            "INSERT INTO copy_intents \
             (event_id, account_id, leader_id, token_id, side, config_snapshot_json, config_snapshot_hash, \
              shard_scheme_version, lane_count, shard_id, status) \
             VALUES (?, 1, 1, '123456', 'BUY', '{}', 'hash', 1, 1, 0, 'needs_reconcile') RETURNING id",
        )
        .bind(event_id)
        .fetch_one(&db.pool)
        .await
        .expect("intent");
        sqlx::query(
            "INSERT INTO reconciliation_cases (account_id, token_id, intent_id, case_type, detail) \
             VALUES (1, '123456', ?, 'balance_drift', 'strict collateral allowance query failed')",
        )
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .expect("case");

        resolve_pre_submit_balance_case(&db, 1)
            .await
            .expect("a no-attempt pre-submit case can close");
        let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .expect("status");
        let open_cases: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ? AND resolved_at IS NULL",
        )
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("case status");
        assert_eq!(status, "rejected");
        assert_eq!(open_cases, 0);
    }

    #[test]
    fn fixed_exit_code_mapping_is_stable() {
        assert_eq!(PersistentError::FuseOpen.exit_code(), EXIT_FUSE_OPEN);
        assert_eq!(PersistentError::MissingConfig.exit_code(), EXIT_CONFIG);
        assert_eq!(
            PersistentError::UnresolvedRecovery.exit_code(),
            EXIT_UNRESOLVED_RECOVERY
        );
        assert_eq!(
            PersistentError::MalformedBudgetState.exit_code(),
            EXIT_BUDGET_STATE
        );
    }
}
