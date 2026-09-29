//! Last-24-hour copy outcomes and the safety state (fuse, open cases) for the
//! overview page. Read-only queries against the ledger.

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rust_decimal::{prelude::ToPrimitive as _, Decimal, RoundingStrategy};
use sqlx::SqlitePool;

use super::live_config::LiveRuntime;
use crate::copytrading::persistent::rolling_reserved_total;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutcomeCounts {
    pub signals: i64,
    pub filled: i64,
    pub unfilled: i64,
    pub deadline_expired: i64,
    pub rejected: i64,
    pub in_progress: i64,
    pub other: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCase {
    pub id: i64,
    pub case_type: String,
    pub intent_id: Option<i64>,
    pub opened_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SafetyState {
    /// (reason, paused_at) when the account fuse is open.
    pub fuse: Option<(String, String)>,
    pub open_cases: Vec<OpenCase>,
    pub last_signal_at: Option<String>,
}

pub fn case_type_zh(case_type: &str) -> &str {
    match case_type {
        "strict_query_failure" => "订单查询失败",
        "unknown_submission" => "下单结果不确定",
        "blocked_recovery" => "恢复被阻塞",
        "local_submission_failure" => "本地提交失败",
        other => other,
    }
}

fn classify(status: &str, reason: &str, counts: &mut OutcomeCounts) {
    counts.signals += 1;
    match status {
        "completed" | "partially_filled" => counts.filled += 1,
        "pending" | "in_progress" => counts.in_progress += 1,
        "cancelled" if reason.contains("deadline") => counts.deadline_expired += 1,
        "cancelled" if reason.contains("GTD expired") => counts.unfilled += 1,
        "rejected" => counts.rejected += 1,
        _ => counts.other += 1,
    }
}

pub async fn outcomes_since(
    pool: &SqlitePool,
    since: DateTime<Utc>,
) -> Result<OutcomeCounts, sqlx::Error> {
    let cutoff = since.to_rfc3339_opts(SecondsFormat::Millis, true);
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT status, rejection_reason FROM copy_intents WHERE created_at >= ?",
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await?;
    let mut counts = OutcomeCounts::default();
    for (status, reason) in rows {
        classify(&status, reason.as_deref().unwrap_or_default(), &mut counts);
    }
    Ok(counts)
}

pub async fn outcomes_last_24h(
    pool: &SqlitePool,
    now: DateTime<Utc>,
) -> Result<OutcomeCounts, sqlx::Error> {
    outcomes_since(pool, now - Duration::hours(24)).await
}

pub async fn safety_state(pool: &SqlitePool) -> Result<SafetyState, sqlx::Error> {
    let fuse: Option<(String, String)> =
        sqlx::query_as("SELECT reason, paused_at FROM persistent_execution_fuse LIMIT 1")
            .fetch_optional(pool)
            .await?;
    let cases: Vec<(i64, String, Option<i64>, String)> = sqlx::query_as(
        "SELECT id, case_type, intent_id, opened_at FROM reconciliation_cases \
         WHERE resolved_at IS NULL ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let last_signal_at: Option<String> =
        sqlx::query_scalar("SELECT MAX(observed_at) FROM leader_events")
            .fetch_one(pool)
            .await?;
    Ok(SafetyState {
        fuse,
        open_cases: cases
            .into_iter()
            .map(|(id, case_type, intent_id, opened_at)| OpenCase {
                id,
                case_type,
                intent_id,
                opened_at,
            })
            .collect(),
        last_signal_at,
    })
}

/// The account's rolling budget as the engine counts it before every order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountBudget {
    pub used: Decimal,
    pub cap: Decimal,
    pub window_seconds: i64,
    /// When the oldest reservation still inside the window drops out of it
    /// (UTC, RFC 3339): the earliest moment room starts to come back.
    pub first_release_at: Option<String>,
}

impl AccountBudget {
    pub fn percent(&self) -> u32 {
        if self.cap <= Decimal::ZERO {
            return 0;
        }
        // Half up (92.5% shows as 93%), matching the notifier; the
        // library's default is banker's rounding.
        (self.used * Decimal::from(100) / self.cap)
            .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
            .to_u32()
            .unwrap_or(u32::MAX)
    }
}

/// Uses the engine's own `rolling_reserved_total`, so the panel shows the
/// number the next order is checked against: every submitted order's
/// notional for the whole window, filled or not.
pub async fn account_budget(
    pool: &SqlitePool,
    runtime: &LiveRuntime,
    now: DateTime<Utc>,
) -> Result<AccountBudget, String> {
    let window = u64::try_from(runtime.budget_window_seconds)
        .map_err(|_| format!("预算窗口无效:{}", runtime.budget_window_seconds))?;
    let cap: Decimal = runtime
        .rolling_budget_usdc
        .parse()
        .map_err(|_| format!("滚动额度无效:{}", runtime.rolling_budget_usdc))?;
    let used = rolling_reserved_total(pool, runtime.account_id, std::time::Duration::from_secs(window), now)
        .await
        .map_err(|error| error.to_string())?;
    let cutoff = (now - Duration::seconds(runtime.budget_window_seconds))
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let oldest: Option<String> = sqlx::query_scalar(
        "SELECT MIN(reserved_at) FROM persistent_budget_reservations \
         WHERE account_id = ? AND state = 'reserved' AND reserved_at >= ?",
    )
    .bind(runtime.account_id)
    .bind(cutoff)
    .fetch_one(pool)
    .await
    .map_err(|error| error.to_string())?;
    let first_release_at = oldest
        .and_then(|stored| DateTime::parse_from_rfc3339(&stored).ok())
        .map(|at| {
            (at.with_timezone(&Utc) + Duration::seconds(runtime.budget_window_seconds))
                .to_rfc3339_opts(SecondsFormat::Millis, true)
        });
    Ok(AccountBudget {
        used,
        cap,
        window_seconds: runtime.budget_window_seconds,
        first_release_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcomes_are_bucketed_like_the_owner_reads_them() {
        let mut counts = OutcomeCounts::default();
        for (status, reason) in [
            ("completed", ""),
            ("partially_filled", ""),
            ("cancelled", "post-only GTD expired or cancelled without a fill"),
            ("cancelled", "decision deadline expired"),
            ("rejected", "computed buy notional is below the CLOB minimum of 1 USDC"),
            ("in_progress", ""),
            ("needs_reconcile", ""),
        ] {
            classify(status, reason, &mut counts);
        }
        assert_eq!(
            counts,
            OutcomeCounts {
                signals: 7,
                filled: 2,
                unfilled: 1,
                deadline_expired: 1,
                rejected: 1,
                in_progress: 1,
                other: 1,
            }
        );
    }

    #[test]
    fn case_types_have_chinese_names() {
        assert_eq!(case_type_zh("unknown_submission"), "下单结果不确定");
        assert_eq!(case_type_zh("something_new"), "something_new");
    }

    #[tokio::test]
    async fn safety_state_reads_an_empty_ledger() {
        let db = crate::copytrading::ops::test_db::TestDb::new().await;
        let state = safety_state(&db.pool).await.unwrap();
        assert_eq!(state, SafetyState::default());
        let counts = outcomes_last_24h(&db.pool, Utc::now()).await.unwrap();
        assert_eq!(counts, OutcomeCounts::default());
    }

    #[tokio::test]
    async fn account_budget_counts_like_the_engine_and_says_when_room_returns() {
        let db = crate::copytrading::ops::test_db::TestDb::new().await;
        let now = Utc::now();
        let at = |seconds_ago: i64| {
            (now - Duration::seconds(seconds_ago)).to_rfc3339_opts(SecondsFormat::Millis, true)
        };
        // Reservations reference attempts; the arithmetic under test does
        // not, so this one connection skips the foreign keys.
        let mut conn = db.pool.acquire().await.unwrap();
        sqlx::query("PRAGMA foreign_keys = OFF").execute(&mut *conn).await.unwrap();
        for (attempt, account, amount, reserved_at, state) in [
            (1, 1, "100", at(3_600), "reserved"),
            (2, 1, "50.5", at(60), "reserved"),
            (3, 1, "400", at(90_000), "reserved"),
            (4, 1, "70", at(60), "released_pre_boundary"),
            (5, 2, "80", at(60), "reserved"),
        ] {
            sqlx::query(
                "INSERT INTO persistent_budget_reservations \
                 (order_attempt_id, account_id, amount_usdc, reserved_at, state) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(attempt)
            .bind(account)
            .bind(amount)
            .bind(reserved_at)
            .bind(state)
            .execute(&mut *conn)
            .await
            .unwrap();
        }
        drop(conn);
        let runtime = LiveRuntime {
            account_id: 1,
            enabled: true,
            allowed_leader_ids: "2".into(),
            max_order_notional_usdc: "10".into(),
            rolling_budget_usdc: "600".into(),
            budget_window_seconds: 86_400,
            tick_seconds: 1,
            backfill_every_seconds: 600,
        };
        let budget = account_budget(&db.pool, &runtime, now).await.unwrap();
        assert_eq!(budget.used, "150.5".parse::<Decimal>().unwrap());
        assert_eq!(budget.percent(), 25);
        let release = DateTime::parse_from_rfc3339(budget.first_release_at.as_deref().unwrap()).unwrap();
        let expected = now - Duration::seconds(3_600) + Duration::seconds(86_400);
        assert!((release.with_timezone(&Utc) - expected).num_milliseconds().abs() < 2);
    }
}
