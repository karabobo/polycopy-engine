//! Phase 3: Transactional intent planning. See
//! `docs/COPY_ENGINE_BLUEPRINT.md` section 8.
//!
//! `plan_next_batch` reads the event ledger by cursor. It never mutates
//! `position_lots` or sends orders (Phase 4/5). For each event past the
//! cursor it durably records an explainable rejection or pending intent for
//! a realtime event, while REST-only audit events advance the cursor without
//! creating an intent. It does **not** compute
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
        "SELECT id, leader_id, condition_id, token_id, side, size, occurred_at, observed_at, realtime_observed \
         FROM leader_events WHERE id > ? ORDER BY id LIMIT ?",
    )
    .bind(cursor)
    .bind(batch_size)
    .fetch_all(pool)
    .await
    .map_err(|error| PlanError::Database(error.to_string()))?;

    let mut summary = PlanSummary::default();

    for event in &events {
        // Activity REST backfill is audit/recovery input only.  It must be
        // consumed by the planner cursor so it cannot block later WS events,
        // but it must never become a copy intent or a synthetic rejection.
        if !event.realtime_observed {
            advance_cursor(pool, account_id, event.id).await?;
            summary.processed += 1;
            continue;
        }
        let decision = evaluate_event(pool, account_id, event, &schedule).await?;

        let mut tx = pool
            .begin()
            .await
            .map_err(|error| PlanError::Database(error.to_string()))?;

        let inserted = sqlx::query(
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

        // Persist an immutable account/market-direction gate before this event
        // can be made runnable. A later signal for the same outcome, including
        // one from another Leader, becomes an auditable rejection; the
        // opposite outcome token remains independently eligible.
        //
        // Per-leader opt-out: when `decision.allow_repeated_market_direction`
        // is true, this leader's events skip both the INSERT and the
        // rejection-on-second-signal logic. The opt-out is a real safety
        // backstop removal (see migrations/0018) and is leader-scoped so
        // other leaders keep their first-wins gate intact. The combined
        // condition `is_none() && inserted == 1 && !allow_repeated` matches
        // the original `is_none() && inserted == 1` for every leader that
        // does not opt out -- nothing about the default behaviour changes.
        let market_limit_rejected = if decision.rejection_reason.is_none()
            && inserted.rows_affected() == 1
            && !decision.allow_repeated_market_direction
        {
            let intent_id: i64 = sqlx::query_scalar(
                "SELECT id FROM copy_intents WHERE event_id = ? AND account_id = ?",
            )
            .bind(event.id)
            .bind(account_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| PlanError::Database(error.to_string()))?;
            let lock = sqlx::query(
                "INSERT OR IGNORE INTO market_direction_order_locks \
                 (account_id, condition_id, token_id, intent_id) VALUES (?, ?, ?, ?)",
            )
            .bind(account_id)
            .bind(&event.condition_id)
            .bind(&event.token_id)
            .bind(intent_id)
            .execute(&mut *tx)
            .await
            .map_err(|error| PlanError::Database(error.to_string()))?;
            if lock.rows_affected() == 1 {
                false
            } else {
                let rejected = sqlx::query(
                    "UPDATE copy_intents SET status = 'rejected', \
                     rejection_reason = 'market direction already has a copy order for this account', \
                     decision_deadline_at = NULL, \
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE id = ? AND status = 'pending'",
                )
                .bind(intent_id)
                .execute(&mut *tx)
                .await
                .map_err(|error| PlanError::Database(error.to_string()))?;
                if rejected.rows_affected() != 1 {
                    return Err(PlanError::Database(
                        "new market-direction-limited intent was not pending".to_owned(),
                    ));
                }
                true
            }
        } else {
            false
        };

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

        tx.commit()
            .await
            .map_err(|error| PlanError::Database(error.to_string()))?;

        summary.processed += 1;
        if decision.rejection_reason.is_some() || market_limit_rejected {
            summary.rejected += 1;
        } else {
            summary.pending += 1;
        }
    }

    Ok(summary)
}

async fn advance_cursor(
    pool: &SqlitePool,
    account_id: i64,
    event_id: i64,
) -> Result<(), PlanError> {
    sqlx::query(
        "INSERT INTO planner_cursor (account_id, last_event_id) VALUES (?, ?) \
         ON CONFLICT(account_id) DO UPDATE SET \
         last_event_id = excluded.last_event_id, \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
    )
    .bind(account_id)
    .bind(event_id)
    .execute(pool)
    .await
    .map_err(|error| PlanError::Database(error.to_string()))?;
    Ok(())
}

#[derive(Debug, FromRow)]
struct LeaderEventRow {
    id: i64,
    leader_id: i64,
    condition_id: String,
    token_id: String,
    side: String,
    size: String,
    occurred_at: String,
    observed_at: String,
    realtime_observed: bool,
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
        .map(|(shard_scheme_version, lane_count)| ExecutionSchedule {
            shard_scheme_version,
            lane_count,
        })
        .ok_or(PlanError::NoExecutionSchedule)
}

/// Refuses to proceed when the configured `execution_schedule` no longer
/// matches what a non-terminal intent was originally sharded under.
///
/// Blueprint (section 12, "startup with a different lane count, shard
/// algorithm, or scheme version must refuse to run while any older
/// non-terminal intent exists"): an intent's `shard_id` is computed once,
/// at planning time, from the `lane_count` active then
/// (`plan_next_batch_with_limit` above). If the operator later changes
/// `lane_count` or `shard_scheme_version` while an old intent is still
/// `pending`/`in_progress`/`partially_filled`/`needs_reconcile`, that
/// intent's `shard_id` no longer means what a lane worker under the new
/// scheme would assume -- two lanes could both believe they own it, or none
/// could. `completed`/`rejected`/`cancelled`/`dead_letter` intents are
/// exempt: a terminal intent's shard assignment can no longer be acted on
/// by anything, so a stale value there is inert, not a hazard.
///
/// This is a read-only check with no side effects; a caller (the process
/// startup sequence, not this function) decides what "refuse to run" means.
pub async fn verify_schedule_compatible_with_pending_work(
    pool: &SqlitePool,
) -> Result<(), PlanError> {
    let schedule = load_execution_schedule(pool).await?;

    let mismatched: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM copy_intents \
         WHERE status IN ('pending', 'in_progress', 'partially_filled', 'needs_reconcile') \
           AND (shard_scheme_version != ? OR lane_count != ?)",
    )
    .bind(schedule.shard_scheme_version)
    .bind(schedule.lane_count)
    .fetch_one(pool)
    .await
    .map_err(|error| PlanError::Database(error.to_string()))?;

    if mismatched > 0 {
        return Err(PlanError::ScheduleChangedWithPendingWork {
            stale_intent_count: mismatched,
        });
    }

    Ok(())
}

struct Decision {
    config_snapshot_json: String,
    config_snapshot_hash: String,
    shard_id: i64,
    rejection_reason: Option<&'static str>,
    decision_deadline_at: Option<String>,
    /// Whether the leader's policy opts out of the market_direction_order_locks
    /// safety backstop for this event. When true, plan_next_batch_with_limit
    /// skips the INSERT (and the rejection-on-second-signal logic). Mirrors
    /// `PolicySnapshot::allow_repeated_market_direction` -- the snapshot is
    /// read once in evaluate_event, so we thread the decision-relevant subset
    /// here to keep plan_next_batch_with_limit a single round-trip per event.
    allow_repeated_market_direction: bool,
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
                tick_size, min_price, max_price, max_order_notional, max_order_shares, balance_within_market, min_leader_trade_size, \
                price_tolerance_abs, allow_repeated_market_direction, size_ratio, maker_only \
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

    let decision_deadline_at =
        observed_at + chrono::Duration::seconds(policy.decision_window_seconds);

    Ok(Decision {
        config_snapshot_json,
        config_snapshot_hash,
        shard_id,
        rejection_reason: None,
        decision_deadline_at: Some(
            decision_deadline_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        ),
        allow_repeated_market_direction: policy.allow_repeated_market_direction,
    })
}

fn reject(shard_id: i64, reason: &'static str) -> Decision {
    Decision {
        config_snapshot_json: "{}".to_owned(),
        config_snapshot_hash: format!("{:016x}", hash_str("{}")),
        shard_id,
        rejection_reason: Some(reason),
        decision_deadline_at: None,
        // A rejection never reaches the market_direction_order_locks INSERT
        // (the `if decision.rejection_reason.is_none() && inserted.rows_affected() == 1`
        // guard short-circuits before the INSERT), so the flag value here
        // is inert. Mirror the accept path's default (off) so a future
        // refactor that drops the guard fails loud instead of inheriting a
        // surprising default.
        allow_repeated_market_direction: false,
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
    /// Absolute price tolerance in the market's own units, taken alongside
    /// `price_tolerance_bps`; execution uses whichever is larger. Optional so
    /// that intents snapshotted before this field existed still deserialize,
    /// where absent means zero and behaviour is unchanged.
    #[serde(default)]
    pub price_tolerance_abs: Option<String>,
    /// Optional per-leader fixed BUY share target. Defaulting keeps snapshots
    /// written before this field was introduced executable.
    #[serde(default)]
    pub max_order_shares: Option<String>,
    /// Opt-in same-market hedge sizing. When a BUY has a confirmed virtual
    /// lot in this leader's other outcome of the same condition, execution
    /// targets only the remaining quantity needed for parity. Defaulting
    /// keeps snapshots written before this field executable.
    #[serde(default)]
    pub balance_within_market: bool,
    pub min_leader_trade_size: String,
    /// Opt-out for the account-wide market_direction_order_locks safety
    /// backstop, scoped per leader. Default false leaves the backstop on;
    /// true skips the INSERT in plan_next_batch_with_limit so this leader's
    /// repeated same-direction signals each become a new copy order. See
    /// migrations/0018 for the safety-net removal note.
    #[serde(default)]
    pub allow_repeated_market_direction: bool,
    /// Optional proportional-sizing mode for a leader's BUY. When present
    /// (and leader 2's case, "0.2" = 1/5), execute.rs's BUY branch sizes
    /// target_qty = leader_event_size * ratio. When both this and
    /// max_order_shares are set, `size_ratio` wins. Defaulting keeps
    /// snapshots written before this field executable.
    #[serde(default)]
    pub size_ratio: Option<String>,
    /// When true, orchestrate/mod.rs branches the persisted decision onto
    /// the maker-only GTD path. Read from `leader_policy.maker_only` once at
    /// planning time and snapshotted here so size_and_reserve (which reads
    /// the persisted snapshot, not live policy) can populate SizedDecision
    /// without a second DB round-trip. See migrations/0019 + the leader-2
    /// redesign handoff Part 3.
    #[serde(default)]
    pub maker_only: bool,
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
    ScheduleChangedWithPendingWork { stale_intent_count: i64 },
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
            Self::ScheduleChangedWithPendingWork { stale_intent_count } => write!(
                formatter,
                "execution_schedule changed while {stale_intent_count} non-terminal intent(s) were \
                 planned under the old lane count/shard scheme; refusing to start until they reach \
                 a terminal status"
            ),
            Self::InvalidTimestamp => {
                write!(formatter, "a stored event timestamp is not valid RFC 3339")
            }
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

    /// Seeds two leaders (1, 2) and their policies so a single
    /// plan_next_batch_with_limit call can exercise both leaders'
    /// `allow_repeated_market_direction` settings in isolation. Leader 1
    /// keeps the backstop on; leader 2 opts out. Returns nothing; the
    /// caller arranges events and leader_config rows before this.
    async fn seed_two_leaders_with_repeat_policy(
        db: &TestDb,
        leader1_repeat: bool,
        leader2_repeat: bool,
    ) {
        // leader_config rows: leader 2 mirrors leader 1 (enabled, label
        // chosen so config-driven tests elsewhere still parse).
        sqlx::query(
            "INSERT INTO leader_config (id, label, enabled) VALUES (2, 'leader-2-test', 1)",
        )
        .execute(&**db)
        .await
        .expect("leader_config 2 must insert");
        // Two distinct policies, one per leader. Each row sets
        // max_signal_age wide enough (3600s) that a freshly-inserted
        // event is never stale; the backstop under test is the
        // market_direction_order_locks INSERT, which depends only on
        // allow_repeated_market_direction.
        for (leader_id, repeat) in [(1, leader1_repeat), (2, leader2_repeat)] {
            sqlx::query(
                "INSERT INTO leader_policy \
                 (leader_id, max_signal_age_seconds, decision_window_seconds, \
                  price_tolerance_bps, tick_size, max_order_notional, \
                  min_leader_trade_size, allow_repeated_market_direction) \
                 VALUES (?, 3600, 300, 100, '0.01', '1000', '0', ?)",
            )
            .bind(leader_id)
            .bind(if repeat { 1i64 } else { 0i64 })
            .execute(&**db)
            .await
            .expect("policy must insert");
        }
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
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
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

        let summary = plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");
        assert_eq!(
            summary,
            PlanSummary {
                processed: 1,
                pending: 0,
                rejected: 1
            }
        );

        let reason: String =
            sqlx::query_scalar("SELECT rejection_reason FROM copy_intents LIMIT 1")
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

        let summary = plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");
        assert_eq!(summary.rejected, 1);
    }

    #[tokio::test]
    async fn an_event_smaller_than_the_policy_minimum_is_a_durable_rejection() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "10").await;
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;

        let summary = plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");
        assert_eq!(summary.rejected, 1);
    }

    #[tokio::test]
    async fn a_stale_event_beyond_max_signal_age_is_a_durable_rejection_not_a_current_order() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 60, "1").await; // 60-second max signal age
        let ancient = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        insert_event(&db, "5", &ancient).await;

        let summary = plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");
        assert_eq!(summary.rejected, 1);

        let reason: String =
            sqlx::query_scalar("SELECT rejection_reason FROM copy_intents LIMIT 1")
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

        let summary = plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");
        assert_eq!(
            summary,
            PlanSummary {
                processed: 1,
                pending: 1,
                rejected: 0
            }
        );

        let (status, deadline): (String, Option<String>) =
            sqlx::query_as("SELECT status, decision_deadline_at FROM copy_intents LIMIT 1")
                .fetch_one(&*db)
                .await
                .expect("intent must exist");
        assert_eq!(status, "pending");
        assert!(
            deadline.is_some(),
            "a pending intent must have a decision deadline"
        );
    }

    #[tokio::test]
    async fn a_second_signal_in_the_same_market_is_a_durable_rejection() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        let now = chrono::Utc::now().to_rfc3339();
        insert_event(&db, "5", &now).await;
        insert_event(&db, "5", &now).await;

        let summary = plan_next_batch(&db, 1).await.expect("planning must succeed");
        assert_eq!(summary, PlanSummary { processed: 2, pending: 1, rejected: 1 });
        let intents: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT status, rejection_reason FROM copy_intents ORDER BY id",
        )
        .fetch_all(&*db)
        .await
        .expect("intents must be queryable");
        assert_eq!(intents[0].0, "pending");
        assert_eq!(intents[1].0, "rejected");
        assert_eq!(
            intents[1].1.as_deref(),
            Some("market direction already has a copy order for this account"),
        );
        let locks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_direction_order_locks")
            .fetch_one(&*db)
            .await
            .expect("market-direction lock must be queryable");
        assert_eq!(locks, 1);
    }

    #[tokio::test]
    async fn the_opposite_outcome_in_one_market_remains_eligible() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        let now = chrono::Utc::now().to_rfc3339();
        insert_event(&db, "5", &now).await;
        let opposite_event_id = insert_event(&db, "5", &now).await;
        sqlx::query("UPDATE leader_events SET token_id = '456', outcome_index = 1 WHERE id = ?")
            .bind(opposite_event_id)
            .execute(&*db)
            .await
            .expect("opposite outcome must update");

        let summary = plan_next_batch(&db, 1).await.expect("planning must succeed");
        assert_eq!(summary, PlanSummary { processed: 2, pending: 2, rejected: 0 });
        let locks: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM market_direction_order_locks")
                .fetch_one(&*db)
                .await
                .expect("market-direction locks must be queryable");
        assert_eq!(locks, 2);
    }

    #[tokio::test]
    async fn allow_repeated_market_direction_is_a_leader_scoped_opt_out() {
        // The market_direction_order_locks safety backstop is account-wide,
        // but the opt-out is leader-scoped: leader 1 keeps the backstop on
        // (second same-direction signal becomes a durable rejection), while
        // leader 2 opts out (every repeated same-direction signal becomes
        // a new pending intent). One batch exercises both leaders' policies
        // in isolation, so the test cannot be fooled by a global flag or
        // a leaky read.
        //
        // Why the assertion is shape-specific: leader 2's two same-direction
        // events both become pending intents AND each event persists a
        // copy_intents row, so the leader-2 count of pending == 2; leader
        // 1's two same-direction events produce one pending + one
        // market-direction-limited rejection, so leader 1's pending == 1
        // and rejected == 1. The market_direction_order_locks table holds
        // exactly one row per (account, condition, token, direction): for
        // leader 1 that's 1 (the lock), for leader 2 that's 0 (no lock
        // taken because of the opt-out). That last assertion is the one
        // that catches the silent regression of the leader-scoping.
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_two_leaders_with_repeat_policy(&db, false, true).await;

        // Two real-time events from each leader, all targeting the same
        // (condition_id, token_id) -> same market direction. Insert the
        // leader-1 pair first so the lock (or absence of it) is observed
        // before leader 2's batch lands.
        let now = chrono::Utc::now().to_rfc3339();
        let l1_e1 = insert_event(&db, "5", &now).await;
        let l1_e2 = insert_event(&db, "5", &now).await;
        let l2_e1 = insert_event(&db, "5", &now).await;
        let l2_e2 = insert_event(&db, "5", &now).await;
        sqlx::query("UPDATE leader_events SET leader_id = 2 WHERE id IN (?, ?)")
            .bind(l2_e1)
            .bind(l2_e2)
            .execute(&*db)
            .await
            .expect("leader 2 event re-tagging must succeed");

        // cursor starts at 0; planning 4 events.
        let summary = plan_next_batch(&db, 1).await.expect("planning must succeed");
        assert_eq!(
            summary,
            PlanSummary { processed: 4, pending: 3, rejected: 1 },
            "leader 1: 1 pending + 1 rejected; leader 2: 2 pending (no backstop)",
        );

        // The pending rows must come from both leaders, not all from one.
        let leader_1_pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM copy_intents WHERE leader_id = 1 AND status = 'pending'",
        )
        .fetch_one(&*db)
        .await
        .unwrap();
        let leader_2_pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM copy_intents WHERE leader_id = 2 AND status = 'pending'",
        )
        .fetch_one(&*db)
        .await
        .unwrap();
        let leader_1_rejected: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM copy_intents WHERE leader_id = 1 \
             AND status = 'rejected' AND rejection_reason LIKE 'market direction already%'",
        )
        .fetch_one(&*db)
        .await
        .unwrap();
        assert_eq!(leader_1_pending, 1, "leader 1: first event wins the lock");
        assert_eq!(
            leader_1_rejected, 1,
            "leader 1: second same-direction signal must be rejected by the backstop",
        );
        assert_eq!(
            leader_2_pending, 2,
            "leader 2 opted out: both same-direction signals become pending intents",
        );

        // The decisive assertion: market_direction_order_locks holds exactly
        // one row (leader 1's winning signal) and zero rows for leader 2's
        // repeated same-direction signals. This is the only assertion that
        // catches a silent regression of the leader-scoping, because the
        // pending/rejected counts above can pass if both leaders' rows are
        // accidentally stored under one leader_id by a buggy test seed.
        let lock_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM market_direction_order_locks")
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(
            lock_count, 1,
            "exactly one lock exists -- leader 1's first event; leader 2's opt-out took none",
        );
        let _ = (l1_e1, l1_e2); // silence unused-bind warnings if any
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
        sqlx::query("UPDATE leader_policy SET max_order_shares = '10' WHERE leader_id = 1")
            .execute(&*db)
            .await
            .expect("fixed-share policy must update");
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;
        plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");

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
        assert_eq!(snapshot.max_order_shares.as_deref(), Some("10"));
    }

    #[tokio::test]
    async fn replaying_the_same_batch_creates_no_second_intent_and_the_cursor_does_not_regress() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;

        let first = plan_next_batch(&db, 1).await.expect("first planning run");
        let second = plan_next_batch(&db, 1)
            .await
            .expect("second planning run over the same events");

        assert_eq!(first.processed, 1);
        assert_eq!(
            second.processed, 0,
            "no new events past the cursor to replan"
        );

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

        let summary = plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");
        assert_eq!(summary.processed, 3);

        let max_event_id: i64 = sqlx::query("SELECT MAX(id) FROM leader_events")
            .fetch_one(&*db)
            .await
            .expect("max id must be queryable")
            .get(0);
        let cursor: i64 =
            sqlx::query("SELECT last_event_id FROM planner_cursor WHERE account_id = 1")
                .fetch_one(&*db)
                .await
                .expect("cursor must be queryable")
                .get(0);
        assert_eq!(
            cursor, max_event_id,
            "the cursor must advance to the last processed event"
        );
    }

    #[tokio::test]
    async fn a_crash_between_intent_insert_and_cursor_advance_leaves_neither_persisted() {
        // Phase 7 required test: "cursor crash injection." `plan_next_batch`
        // wraps the intent insert and the cursor advance for one event in a
        // single transaction (see the loop body above) so a real process
        // crash between the two writes cannot happen -- SQLite rolls back
        // everything an uncommitted transaction did the moment the
        // connection that opened it goes away, which is exactly what
        // dropping a `Transaction` without calling `.commit()` simulates
        // here.
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        let event_id = insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;

        {
            let mut tx = db.begin().await.expect("transaction must open");
            sqlx::query(
                "INSERT INTO copy_intents \
                 (event_id, account_id, leader_id, token_id, side, config_snapshot_json, \
                  config_snapshot_hash, shard_scheme_version, lane_count, shard_id, status) \
                 VALUES (?, 1, 1, '123', 'BUY', '{}', 'hash', 1, 1, 0, 'pending')",
            )
            .bind(event_id)
            .execute(&mut *tx)
            .await
            .expect("intent insert must succeed inside the open transaction");
            sqlx::query(
                "INSERT INTO planner_cursor (account_id, last_event_id) VALUES (1, ?) \
                 ON CONFLICT(account_id) DO UPDATE SET last_event_id = excluded.last_event_id",
            )
            .bind(event_id)
            .execute(&mut *tx)
            .await
            .expect("cursor update must succeed inside the open transaction");
            // No `tx.commit()` -- dropping `tx` here is the crash.
        }

        let intent_count: i64 = sqlx::query("SELECT COUNT(*) FROM copy_intents")
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            intent_count, 0,
            "the uncommitted intent must not survive the crash"
        );

        let cursor: Option<i64> =
            sqlx::query_scalar("SELECT last_event_id FROM planner_cursor WHERE account_id = 1")
                .fetch_optional(&*db)
                .await
                .unwrap();
        assert_eq!(
            cursor, None,
            "the uncommitted cursor advance must not survive the crash"
        );

        // Recovery: a fresh plan_next_batch call sees the event as still
        // unprocessed (cursor never advanced past it) and processes it
        // exactly once, from scratch -- no manual repair needed.
        seed_policy(&db, 3600, "1").await;
        let summary = plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed after the crash");
        assert_eq!(summary.processed, 1);
        let intent_count: i64 = sqlx::query("SELECT COUNT(*) FROM copy_intents")
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            intent_count, 1,
            "recovery must produce exactly one intent, not zero or two"
        );
    }

    #[tokio::test]
    async fn refuses_to_start_when_a_non_terminal_intent_was_planned_under_a_different_lane_count()
    {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;
        plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");

        // The operator changes lane_count while the intent above is still
        // pending -- its stamped shard_id was computed under lane_count=1
        // and no longer means what a lane worker under the new scheme
        // would assume.
        sqlx::query("UPDATE execution_schedule SET lane_count = 4 WHERE id = 1")
            .execute(&*db)
            .await
            .unwrap();

        let result = verify_schedule_compatible_with_pending_work(&db).await;
        assert!(matches!(
            result,
            Err(PlanError::ScheduleChangedWithPendingWork {
                stale_intent_count: 1
            })
        ));
    }

    #[tokio::test]
    async fn allows_starting_once_the_stale_intent_reaches_a_terminal_status() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;
        plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");

        sqlx::query("UPDATE copy_intents SET status = 'completed'")
            .execute(&*db)
            .await
            .unwrap();
        sqlx::query("UPDATE execution_schedule SET lane_count = 4 WHERE id = 1")
            .execute(&*db)
            .await
            .unwrap();

        verify_schedule_compatible_with_pending_work(&db)
            .await
            .expect("a terminal intent's stale shard stamp is inert, not a hazard");
    }

    #[tokio::test]
    async fn allows_starting_when_the_schedule_never_changed() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;
        plan_next_batch(&db, 1)
            .await
            .expect("planning must succeed");

        verify_schedule_compatible_with_pending_work(&db)
            .await
            .expect("an unchanged schedule must never block startup");
    }

    #[tokio::test]
    async fn rest_audit_event_advances_cursor_without_creating_an_intent() {
        let db = TestDb::new().await;
        seed_account_and_leader(&db).await;
        seed_policy(&db, 3600, "1").await;
        let audit_event_id = insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;
        sqlx::query("UPDATE leader_events SET realtime_observed = 0 WHERE id = ?")
            .bind(audit_event_id)
            .execute(&*db)
            .await
            .unwrap();
        let realtime_event_id = insert_event(&db, "5", &chrono::Utc::now().to_rfc3339()).await;

        let summary = plan_next_batch(&db, 1).await.unwrap();
        assert_eq!(summary.processed, 2);
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.rejected, 0);
        let audit_intents: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM copy_intents WHERE event_id = ?")
                .bind(audit_event_id)
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(audit_intents, 0);
        let realtime_intents: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM copy_intents WHERE event_id = ?")
                .bind(realtime_event_id)
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(realtime_intents, 1);
        let cursor: i64 =
            sqlx::query_scalar("SELECT last_event_id FROM planner_cursor WHERE account_id = 1")
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(cursor, realtime_event_id);
    }
}
