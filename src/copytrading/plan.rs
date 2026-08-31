//! Phase 3: Transactional intent planning. See
//! `docs/COPY_ENGINE_BLUEPRINT.md` section 8.
//!
//! `plan_next_batch` reads the event ledger by cursor. It never mutates
//! `position_lots` or sends orders (Phase 4/5). For each event past the
//! cursor it durably records either an explainable rejection or a pending
//! intent -- it never silently skips an event. It does **not** compute
//! `planned_qty`/`planned_price`/tick-rounded limit price/TIF: the
//! blueprint assigns that to the lane (Phase 4), because those depend on
//! live account state and market data, not on the event alone.

use std::{
    collections::hash_map::DefaultHasher,
    fmt,
    hash::{Hash as _, Hasher as _},
};

use sqlx::{FromRow, SqlitePool};

const DEFAULT_BATCH_SIZE: i64 = 200;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PlanSummary {
    pub processed: usize,
    pub pending: usize,
    pub rejected: usize,
}

/// Plans up to [`DEFAULT_BATCH_SIZE`] events past `account_id`'s cursor.
pub async fn plan_next_batch(pool: &SqlitePool, account_id: i64) -> Result<PlanSummary, PlanError> {
    plan_next_batch_with_limit(pool, account_id, DEFAULT_BATCH_SIZE).await
}

pub async fn plan_next_batch_with_limit(
    pool: &SqlitePool,
    account_id: i64,
    batch_size: i64,
) -> Result<PlanSummary, PlanError> {
    let schedule = load_execution_schedule(pool).await?;

    let cursor: i64 =
        sqlx::query_scalar("SELECT last_event_id FROM planner_cursor WHERE account_id = ?")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| PlanError::Database(error.to_string()))?
            .unwrap_or(0);

    let events: Vec<LeaderEventRow> = sqlx::query_as(
        "SELECT id, leader_id, token_id, side, size, occurred_at, observed_at \
         FROM leader_events WHERE id > ? ORDER BY id LIMIT ?",
    )
    .bind(cursor)
    .bind(batch_size)
    .fetch_all(pool)
    .await
    .map_err(|error| PlanError::Database(error.to_string()))?;

    let mut summary = PlanSummary::default();

    for event in &events {
        let decision = evaluate_event(pool, account_id, event, &schedule).await?;

        let mut tx = pool.begin().await.map_err(|error| PlanError::Database(error.to_string()))?;

        sqlx::query(
            "INSERT OR IGNORE INTO copy_intents \
             (event_id, account_id, leader_id, token_id, side, config_snapshot_json, \
              config_snapshot_hash, shard_scheme_version, lane_count, shard_id, status, \
              rejection_reason, decision_deadline_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.id)
        .bind(account_id)
        .bind(event.leader_id)
        .bind(&event.token_id)
        .bind(&event.side)
        .bind(&decision.config_snapshot_json)
        .bind(&decision.config_snapshot_hash)
        .bind(schedule.shard_scheme_version)
        .bind(schedule.lane_count)
        .bind(decision.shard_id)
        .bind(decision.status())
        .bind(decision.rejection_reason)
        .bind(&decision.decision_deadline_at)
        .execute(&mut *tx)
        .await
        .map_err(|error| PlanError::Database(error.to_string()))?;

        sqlx::query(
            "INSERT INTO planner_cursor (account_id, last_event_id) VALUES (?, ?) \
             ON CONFLICT(account_id) DO UPDATE SET \
             last_event_id = excluded.last_event_id, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        )
        .bind(account_id)
        .bind(event.id)
        .execute(&mut *tx)
        .await
        .map_err(|error| PlanError::Database(error.to_string()))?;

        tx.commit().await.map_err(|error| PlanError::Database(error.to_string()))?;

        summary.processed += 1;
        if decision.rejection_reason.is_some() {
            summary.rejected += 1;
        } else {
            summary.pending += 1;
        }
    }

    Ok(summary)
}

#[derive(Debug, FromRow)]
struct LeaderEventRow {
    id: i64,
    leader_id: i64,
    token_id: String,
    side: String,
    size: String,
    occurred_at: String,
    observed_at: String,
}

struct ExecutionSchedule {
    shard_scheme_version: i64,
    lane_count: i64,
}

async fn load_execution_schedule(pool: &SqlitePool) -> Result<ExecutionSchedule, PlanError> {
    sqlx::query_as("SELECT shard_scheme_version, lane_count FROM execution_schedule WHERE id = 1")
        .fetch_optional(pool)
        .await
        .map_err(|error| PlanError::Database(error.to_string()))?
        .map(|(shard_scheme_version, lane_count)| ExecutionSchedule { shard_scheme_version, lane_count })
        .ok_or(PlanError::NoExecutionSchedule)
}

struct Decision {
    config_snapshot_json: String,
    config_snapshot_hash: String,
    shard_id: i64,
    rejection_reason: Option<&'static str>,
    decision_deadline_at: Option<String>,
}

impl Decision {
    fn status(&self) -> &'static str {
        if self.rejection_reason.is_some() {
            "rejected"
        } else {
            "pending"
        }
    }
}

/// Validates one event against the leader's policy and the account/leader
/// enable state, and computes the deterministic shard for it. Never
/// silently skips: every path returns a `Decision`, accepted or rejected
/// with a reason.
async fn evaluate_event(
    pool: &SqlitePool,
    account_id: i64,
    event: &LeaderEventRow,
    schedule: &ExecutionSchedule,
) -> Result<Decision, PlanError> {
    let shard_id = shard_for(account_id, &event.token_id, schedule.lane_count);

    let leader_enabled: Option<bool> =
        sqlx::query_scalar("SELECT enabled FROM leader_config WHERE id = ?")
            .bind(event.leader_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| PlanError::Database(error.to_string()))?;

    let Some(true) = leader_enabled else {
        return Ok(reject(shard_id, "leader is disabled"));
    };

    let policy = sqlx::query_as::<_, PolicySnapshot>(
        "SELECT max_signal_age_seconds, decision_window_seconds, price_tolerance_bps, \
                tick_size, min_price, max_price, max_order_notional, min_leader_trade_size \
         FROM leader_policy WHERE leader_id = ?",
    )
    .bind(event.leader_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| PlanError::Database(error.to_string()))?;

    let Some(policy) = policy else {
        return Ok(reject(shard_id, "no policy configured for this leader"));
    };

    let occurred_at = parse_rfc3339(&event.occurred_at).ok_or(PlanError::InvalidTimestamp)?;
    let observed_at = parse_rfc3339(&event.observed_at).ok_or(PlanError::InvalidTimestamp)?;
    let now = chrono::Utc::now();

    let age_seconds = (now - occurred_at).num_seconds();
    if age_seconds > policy.max_signal_age_seconds {
        return Ok(reject(shard_id, "signal age exceeds policy"));
    }

    let size: rust_decimal::Decimal = event
        .size
        .parse()
        .map_err(|_| PlanError::InvalidDecimal("leader_events.size"))?;
    let min_size: rust_decimal::Decimal = policy
        .min_leader_trade_size
        .parse()
        .map_err(|_| PlanError::InvalidDecimal("leader_policy.min_leader_trade_size"))?;
    if size < min_size {
        return Ok(reject(shard_id, "leader trade size below policy minimum"));
    }

    // The complete policy, not a subset: Phase 4 must size and price using
    // exactly what this decision was made under, immune to a later policy
    // edit (blueprint section 3's "immutable configuration snapshot").
    let config_snapshot_json = serde_json::to_string(&policy)
        .map_err(|_| PlanError::InvalidDecimal("leader_policy snapshot"))?;
    let config_snapshot_hash = format!("{:016x}", hash_str(&config_snapshot_json));

    let decision_deadline_at = observed_at + chrono::Duration::seconds(policy.decision_window_seconds);

    Ok(Decision {
        config_snapshot_json,
        config_snapshot_hash,
        shard_id,
        rejection_reason: None,
        decision_deadline_at: Some(decision_deadline_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
    })
}

fn reject(shard_id: i64, reason: &'static str) -> Decision {
    Decision {
        config_snapshot_json: "{}".to_owned(),
        config_snapshot_hash: format!("{:016x}", hash_str("{}")),
        shard_id,
        rejection_reason: Some(reason),
        decision_deadline_at: None,
    }
}

/// The complete policy a planning decision (and later, Phase 4's sizing) is
/// made under. Serialized verbatim into `copy_intents.config_snapshot_json`
/// so a later edit to `leader_policy` never retroactively changes an
/// already-planned intent's behavior; deserialized back out by whatever
/// reads that snapshot rather than re-querying live policy.
#[derive(Debug, Clone, FromRow, serde::Serialize, serde::Deserialize)]
pub struct PolicySnapshot {
    pub max_signal_age_seconds: i64,
    pub decision_window_seconds: i64,
    pub price_tolerance_bps: i64,
    pub tick_size: String,
    pub min_price: String,
    pub max_price: String,
    pub max_order_notional: String,
    pub min_leader_trade_size: String,
}

fn shard_for(account_id: i64, token_id: &str, lane_count: i64) -> i64 {
    if lane_count <= 1 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    account_id.hash(&mut hasher);
    token_id.hash(&mut hasher);
    (hasher.finish() % lane_count.unsigned_abs()) as i64
}

fn hash_str(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn parse_rfc3339(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

#[derive(Debug)]
pub enum PlanError {
    Database(String),
    NoExecutionSchedule,
    InvalidTimestamp,
    InvalidDecimal(&'static str),
}

impl fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::NoExecutionSchedule => write!(
                formatter,
                "no execution_schedule row exists yet; the planner refuses to guess a lane count"
            ),
            Self::InvalidTimestamp => write!(formatter, "a stored event timestamp is not valid RFC 3339"),
            Self::InvalidDecimal(field) => write!(formatter, "invalid decimal value in {field}"),
        }
    }
}

impl std::error::Error for PlanError {}

#[cfg(test)]
mod tests {
    use sqlx::Row as _;

    use super::*;
    use crate::copytrading::db::open_and_migrate;

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
                "polycopy-engine-plan-test-{}-{nonce}-{counter}.sqlite",
                process::id()
            ));
            let pool = open_and_migrate(&path)
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

    async fn seed_account_and_leader(db: &TestDb) {
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&**db)
        .await
        .expect("account must insert");
        sqlx::query("INSERT INTO leader_config (id, label, enabled) VALUES (1, 'leader-one', 1)")
            .execute(&**db)
            .await
            .expect("leader must insert");
        sqlx::query("INSERT INTO execution_schedule (id, shard_scheme_version, shard_algorithm, lane_count) VALUES (1, 1, 'hash_mod_lane_count', 1)")
            .execute(&**db)
            .await
            .expect("execution_schedule must insert");
    }

    async fn seed_policy(db: &TestDb, max_signal_age_seconds: i64, min_leader_trade_size: &str) {
        sqlx::query(
            "INSERT INTO leader_policy \
             (leader_id, max_signal_age_seconds, decision_window_seconds, price_tolerance_bps, \
              tick_size, max_order_notional, min_leader_trade_size) \
             VALUES (1, ?, 300, 100, '0.01', '1000', ?)",
        )
        .bind(max_signal_age_seconds)
        .bind(min_leader_trade_size)
        .execute(&**db)
        .await
        .expect("policy must insert");
    }

    async fn insert_event(db: &TestDb, size: &str, occurred_at: &str) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO leader_events \
             (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, \
              price, occurred_at, observed_at) \
             VALUES (?, 1, '0xcond', '123', 0, 'BUY', ?, '0.5', ?, ?) RETURNING id",
        )
        .bind(format!("activity:{}", uuid_like()))
        .bind(size)
        .bind(occurred_at)
        .bind(occurred_at)
        .fetch_one(&**db)
        .await
        .expect("event must insert")
    }

    fn uuid_like() -> u64 {
        use std::{
            sync::atomic::{AtomicU64, Ordering},
            time::{SystemTime, UNIX_EPOCH},
        };
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        nanos.wrapping_add(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    #[tokio::test]
    async fn refuses_to_plan_without_an_execution_schedule() {
        let db = TestDb::new().await;
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&*db)
        .await
        .expect("account must insert");

        let result = plan_next_batch(&db, 1).await;
        assert!(matches!(result, Err(PlanError::NoExecutionSchedule)));
    }

    #[tokio::test]
    async fn an_event_from_a_leader_with_no_policy_is_a_durable_rejection() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        insert_event(&db, "5", "2026-08-31T00:00:00.000Z").await;

        let summary = plan_next_batch(&db, 1).await.expect("planning must succeed");
        assert_eq!(summary, PlanSummary { processed: 1, pending: 0, rejected: 1 });

        let reason: String = sqlx::query_scalar("SELECT rejection_reason FROM copy_intents LIMIT 1")
            .fetch_one(&*db)
            .await
            .expect("intent must exist");
        assert_eq!(reason, "no policy configured for this leader");
    }

    #[tokio::test]
    async fn a_disabled_leaders_event_is_a_durable_rejection() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        sqlx::query("UPDATE leader_config SET enabled = 0 WHERE id = 1")
            .execute(&*db)
            .await
            .expect("leader must update");
        insert_event(&db, "5", "2026-08-31T00:00:00.000Z").await;

        let summary = plan_next_batch(&db, 1).await.expect("planning must succeed");
        assert_eq!(summary.rejected, 1);
    }

    #[tokio::test]
    async fn an_event_smaller_than_the_policy_minimum_is_a_durable_rejection() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "10").await;
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;

        let summary = plan_next_batch(&db, 1).await.expect("planning must succeed");
        assert_eq!(summary.rejected, 1);
    }

    #[tokio::test]
    async fn a_stale_event_beyond_max_signal_age_is_a_durable_rejection_not_a_current_order() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 60, "1").await; // 60-second max signal age
        let ancient = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        insert_event(&db, "5", &ancient).await;

        let summary = plan_next_batch(&db, 1).await.expect("planning must succeed");
        assert_eq!(summary.rejected, 1);

        let reason: String = sqlx::query_scalar("SELECT rejection_reason FROM copy_intents LIMIT 1")
            .fetch_one(&*db)
            .await
            .expect("intent must exist");
        assert_eq!(reason, "signal age exceeds policy");
    }

    #[tokio::test]
    async fn a_fresh_qualifying_event_becomes_a_pending_intent_with_a_deadline() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;

        let summary = plan_next_batch(&db, 1).await.expect("planning must succeed");
        assert_eq!(summary, PlanSummary { processed: 1, pending: 1, rejected: 0 });

        let (status, deadline): (String, Option<String>) =
            sqlx::query_as("SELECT status, decision_deadline_at FROM copy_intents LIMIT 1")
                .fetch_one(&*db)
                .await
                .expect("intent must exist");
        assert_eq!(status, "pending");
        assert!(deadline.is_some(), "a pending intent must have a decision deadline");
    }

    #[tokio::test]
    async fn the_config_snapshot_captures_the_complete_policy_not_a_subset() {
        // A later leader_policy edit must never retroactively change an
        // already-planned intent (blueprint section 3): that only holds if
        // every field Phase 4 needs (tick_size, price_tolerance_bps,
        // max_order_notional, ...) is actually in the snapshot, not just
        // the subset the planner itself happens to check.
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;
        plan_next_batch(&db, 1).await.expect("planning must succeed");

        let snapshot_json: String =
            sqlx::query_scalar("SELECT config_snapshot_json FROM copy_intents LIMIT 1")
                .fetch_one(&*db)
                .await
                .expect("intent must exist");
        let snapshot: PolicySnapshot =
            serde_json::from_str(&snapshot_json).expect("snapshot must deserialize");
        assert_eq!(snapshot.tick_size, "0.01");
        assert_eq!(snapshot.price_tolerance_bps, 100);
        assert_eq!(snapshot.max_order_notional, "1000");
    }

    #[tokio::test]
    async fn replaying_the_same_batch_creates_no_second_intent_and_the_cursor_does_not_regress() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;

        let first = plan_next_batch(&db, 1).await.expect("first planning run");
        let second = plan_next_batch(&db, 1).await.expect("second planning run over the same events");

        assert_eq!(first.processed, 1);
        assert_eq!(second.processed, 0, "no new events past the cursor to replan");

        let intent_count: i64 = sqlx::query("SELECT COUNT(*) FROM copy_intents")
            .fetch_one(&*db)
            .await
            .expect("intent count must be queryable")
            .get(0);
        assert_eq!(intent_count, 1);
    }

    #[tokio::test]
    async fn the_cursor_advances_past_every_processed_event_in_a_batch() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        for _ in 0..3 {
            insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;
        }

        let summary = plan_next_batch(&db, 1).await.expect("planning must succeed");
        assert_eq!(summary.processed, 3);

        let max_event_id: i64 = sqlx::query("SELECT MAX(id) FROM leader_events")
            .fetch_one(&*db)
            .await
            .expect("max id must be queryable")
            .get(0);
        let cursor: i64 = sqlx::query("SELECT last_event_id FROM planner_cursor WHERE account_id = 1")
            .fetch_one(&*db)
            .await
            .expect("cursor must be queryable")
            .get(0);
        assert_eq!(cursor, max_event_id, "the cursor must advance to the last processed event");
    }
}
