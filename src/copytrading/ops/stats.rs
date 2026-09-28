//! Last-24-hour copy outcomes and the safety state (fuse, open cases) for the
//! overview page. Read-only queries against the ledger.

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use sqlx::SqlitePool;

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
}
