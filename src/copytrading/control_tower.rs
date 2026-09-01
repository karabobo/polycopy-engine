//! Phase 6: Squadron, CAG, and Control Tower. See
//! `docs/COPY_ENGINE_BLUEPRINT.md` section 11.
//!
//! DRADIS's own Squadron/CAG objects are configuration and read-only status
//! consumers -- this crate does not link against, vendor, or depend on
//! DRADIS in any way (see `docs/DRADIS_REFERENCE_BASELINE.md`).
//! [`CopyStrategyStatusShim`] is this project's own, self-contained
//! implementation of that read-only contract: it always reports
//! [`SignalStatus::NoSignal`] because the copy pipeline owns execution
//! end-to-end (ingest, plan, execute, reconcile) and never asks an external
//! caller to generate or confirm a trading signal. Everything else in this
//! module is read-only status and traceability data: leader status, that
//! leader's intents/lots/reconciliation cases, and a full trace from one
//! order attempt back to its event, leader, account, configuration
//! snapshot, and reconciliation history.
//!
//! Nothing here writes to `leader_config`, `copy_intents`, or any other
//! table -- enabling/disabling a leader and updating its policy remain
//! plain `UPDATE` statements a caller runs directly; this module only reads
//! back what already happened.

use sqlx::SqlitePool;

/// A DRADIS Squadron/CAG-compatible strategy status. See the module doc for
/// why this is always [`Self::NoSignal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalStatus {
    NoSignal,
}

/// Always reports [`SignalStatus::NoSignal`] -- see the module doc.
#[derive(Debug, Clone, Copy, Default)]
pub struct CopyStrategyStatusShim;

impl CopyStrategyStatusShim {
    pub fn status(&self) -> SignalStatus {
        SignalStatus::NoSignal
    }
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct LeaderStatus {
    pub leader_id: i64,
    pub label: String,
    pub enabled: bool,
    pub activation_at: Option<String>,
}

/// One leader's current configuration status. Reading this after an
/// `UPDATE leader_config` or `UPDATE leader_policy` always reflects that
/// leader's own row only -- no other leader's already-planned intents are
/// touched by it (blueprint acceptance: "updating one leader does not
/// affect another leader's snapshot").
pub async fn leader_status(pool: &SqlitePool, leader_id: i64) -> Result<LeaderStatus, ControlTowerError> {
    sqlx::query_as::<_, LeaderStatus>(
        "SELECT id AS leader_id, label, enabled, activation_at FROM leader_config WHERE id = ?",
    )
    .bind(leader_id)
    .fetch_optional(pool)
    .await
    .map_err(db_err)?
    .ok_or(ControlTowerError::LeaderNotFound)
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct IntentSummary {
    pub intent_id: i64,
    pub event_id: i64,
    pub leader_id: i64,
    pub account_id: i64,
    pub token_id: String,
    pub side: String,
    pub status: String,
    pub reserved_qty: String,
    pub config_snapshot_json: String,
    pub config_snapshot_hash: String,
    pub created_at: String,
}

/// Every intent this leader has ever produced, durably-rejected or not.
/// Disabling a leader stops new intents from being *planned* for it (Phase
/// 3's `plan_next_batch`); it never deletes or hides an intent already
/// here.
pub async fn leader_intents(pool: &SqlitePool, leader_id: i64) -> Result<Vec<IntentSummary>, ControlTowerError> {
    sqlx::query_as::<_, IntentSummary>(
        "SELECT id AS intent_id, event_id, leader_id, account_id, token_id, side, status, \
         reserved_qty, config_snapshot_json, config_snapshot_hash, created_at \
         FROM copy_intents WHERE leader_id = ? ORDER BY created_at, id",
    )
    .bind(leader_id)
    .fetch_all(pool)
    .await
    .map_err(db_err)
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct LotSummary {
    pub account_id: i64,
    pub leader_id: i64,
    pub token_id: String,
    pub qty: String,
    pub updated_at: String,
}

/// Every virtual lot this leader's copied trades hold, regardless of
/// whether the leader is currently enabled. Disabling a leader must never
/// erase or silently close a position (blueprint: "it does not erase lots
/// or silently close positions") -- this reads the same `position_lots`
/// rows `execute::finalize_receipt` writes, with no `enabled` filter at
/// all.
pub async fn leader_lots(pool: &SqlitePool, leader_id: i64) -> Result<Vec<LotSummary>, ControlTowerError> {
    sqlx::query_as::<_, LotSummary>(
        "SELECT account_id, leader_id, token_id, qty, updated_at \
         FROM position_lots WHERE leader_id = ? ORDER BY token_id",
    )
    .bind(leader_id)
    .fetch_all(pool)
    .await
    .map_err(db_err)
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ReconciliationCaseSummary {
    pub case_id: i64,
    pub account_id: i64,
    pub token_id: String,
    pub intent_id: Option<i64>,
    pub order_attempt_id: Option<i64>,
    pub case_type: String,
    pub detail: Option<String>,
    pub opened_at: String,
    pub resolved_at: Option<String>,
}

/// Every reconciliation case attributable to this leader (via its
/// intents). A case whose `intent_id` is `NULL` has no leader attribution
/// by construction and is correctly excluded here, not silently dropped.
pub async fn leader_reconciliation_cases(
    pool: &SqlitePool,
    leader_id: i64,
) -> Result<Vec<ReconciliationCaseSummary>, ControlTowerError> {
    sqlx::query_as::<_, ReconciliationCaseSummary>(
        "SELECT rc.id AS case_id, rc.account_id, rc.token_id, rc.intent_id, rc.order_attempt_id, \
         rc.case_type, rc.detail, rc.opened_at, rc.resolved_at \
         FROM reconciliation_cases rc \
         JOIN copy_intents ci ON ci.id = rc.intent_id \
         WHERE ci.leader_id = ? ORDER BY rc.opened_at, rc.id",
    )
    .bind(leader_id)
    .fetch_all(pool)
    .await
    .map_err(db_err)
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct EventSummary {
    pub event_id: i64,
    pub canonical_event_key: String,
    pub condition_id: String,
    pub token_id: String,
    pub side: String,
    pub size: String,
    pub price: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct AccountSummary {
    pub account_id: i64,
    pub label: String,
    pub signing_address: String,
    pub signature_type: String,
}

/// The full trace from one order attempt back to everything that produced
/// and now surrounds it: the leader event that triggered it, the intent
/// (with its immutable policy snapshot), the leader and account, and every
/// reconciliation case attributable to that intent -- the blueprint's
/// Control Tower acceptance criterion in one read.
#[derive(Debug, Clone, PartialEq)]
pub struct AttemptTrace {
    pub attempt_id: i64,
    pub attempt_number: i64,
    pub attempt_status: String,
    pub venue_order_id: Option<String>,
    pub receipt_json: Option<String>,
    pub intent: IntentSummary,
    pub event: EventSummary,
    pub leader: LeaderStatus,
    pub account: AccountSummary,
    pub reconciliation_cases: Vec<ReconciliationCaseSummary>,
}

#[derive(sqlx::FromRow)]
struct AttemptTraceRow {
    attempt_id: i64,
    attempt_number: i64,
    attempt_status: String,
    venue_order_id: Option<String>,
    receipt_json: Option<String>,
    intent_id: i64,
    event_id: i64,
    account_id: i64,
    leader_id: i64,
    token_id: String,
    side: String,
    intent_status: String,
    reserved_qty: String,
    config_snapshot_json: String,
    config_snapshot_hash: String,
    intent_created_at: String,
    canonical_event_key: String,
    condition_id: String,
    event_token_id: String,
    event_side: String,
    event_size: String,
    event_price: String,
    occurred_at: String,
    leader_label: String,
    leader_enabled: bool,
    leader_activation_at: Option<String>,
    account_label: String,
    signing_address: String,
    signature_type: String,
}

pub async fn trace_attempt(pool: &SqlitePool, attempt_id: i64) -> Result<AttemptTrace, ControlTowerError> {
    let row: Option<AttemptTraceRow> = sqlx::query_as(
        "SELECT \
            oa.id AS attempt_id, oa.attempt_number, oa.status AS attempt_status, \
            oa.venue_order_id, oa.receipt_json, \
            ci.id AS intent_id, ci.event_id, ci.account_id, ci.leader_id, ci.token_id, ci.side, \
            ci.status AS intent_status, ci.reserved_qty, ci.config_snapshot_json, \
            ci.config_snapshot_hash, ci.created_at AS intent_created_at, \
            le.canonical_event_key, le.condition_id, le.token_id AS event_token_id, \
            le.side AS event_side, le.size AS event_size, le.price AS event_price, \
            le.occurred_at, \
            lc.label AS leader_label, lc.enabled AS leader_enabled, \
            lc.activation_at AS leader_activation_at, \
            a.label AS account_label, a.signing_address, a.signature_type \
         FROM order_attempts oa \
         JOIN copy_intents ci ON ci.id = oa.intent_id \
         JOIN leader_events le ON le.id = ci.event_id \
         JOIN leader_config lc ON lc.id = ci.leader_id \
         JOIN accounts a ON a.id = ci.account_id \
         WHERE oa.id = ?",
    )
    .bind(attempt_id)
    .fetch_optional(pool)
    .await
    .map_err(db_err)?;

    let row = row.ok_or(ControlTowerError::AttemptNotFound)?;
    let reconciliation_cases = sqlx::query_as::<_, ReconciliationCaseSummary>(
        "SELECT id AS case_id, account_id, token_id, intent_id, order_attempt_id, case_type, \
         detail, opened_at, resolved_at \
         FROM reconciliation_cases WHERE intent_id = ? ORDER BY opened_at, id",
    )
    .bind(row.intent_id)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    Ok(AttemptTrace {
        attempt_id: row.attempt_id,
        attempt_number: row.attempt_number,
        attempt_status: row.attempt_status,
        venue_order_id: row.venue_order_id,
        receipt_json: row.receipt_json,
        intent: IntentSummary {
            intent_id: row.intent_id,
            event_id: row.event_id,
            leader_id: row.leader_id,
            account_id: row.account_id,
            token_id: row.token_id,
            side: row.side,
            status: row.intent_status,
            reserved_qty: row.reserved_qty,
            config_snapshot_json: row.config_snapshot_json,
            config_snapshot_hash: row.config_snapshot_hash,
            created_at: row.intent_created_at,
        },
        event: EventSummary {
            event_id: row.event_id,
            canonical_event_key: row.canonical_event_key,
            condition_id: row.condition_id,
            token_id: row.event_token_id,
            side: row.event_side,
            size: row.event_size,
            price: row.event_price,
            occurred_at: row.occurred_at,
        },
        leader: LeaderStatus {
            leader_id: row.leader_id,
            label: row.leader_label,
            enabled: row.leader_enabled,
            activation_at: row.leader_activation_at,
        },
        account: AccountSummary {
            account_id: row.account_id,
            label: row.account_label,
            signing_address: row.signing_address,
            signature_type: row.signature_type,
        },
        reconciliation_cases,
    })
}

fn db_err(error: sqlx::Error) -> ControlTowerError {
    ControlTowerError::Database(error.to_string())
}

#[derive(Debug)]
pub enum ControlTowerError {
    Database(String),
    LeaderNotFound,
    AttemptNotFound,
}

impl std::fmt::Display for ControlTowerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::LeaderNotFound => write!(formatter, "no leader with that ID exists"),
            Self::AttemptNotFound => write!(formatter, "no order attempt with that ID exists"),
        }
    }
}

impl std::error::Error for ControlTowerError {}

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
                "polycopy-engine-control-tower-test-{}-{nonce}-{counter}.sqlite",
                process::id()
            ));
            let pool = open_and_migrate(&path).await.expect("migrations must apply to a fresh database");
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

    async fn seed_leader(db: &TestDb, leader_id: i64, label: &str) {
        sqlx::query("INSERT INTO leader_config (id, label) VALUES (?, ?)")
            .bind(leader_id)
            .bind(label)
            .execute(&**db)
            .await
            .expect("leader must insert");
    }

    async fn seed_event(db: &TestDb, event_id: i64, leader_id: i64, canonical_key: &str) {
        sqlx::query(
            "INSERT INTO leader_events \
             (id, canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, \
              size, price, occurred_at, observed_at) \
             VALUES (?, ?, ?, '0xcond', '123456', 0, 'BUY', '5', '0.5', \
                     strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
        )
        .bind(event_id)
        .bind(canonical_key)
        .bind(leader_id)
        .execute(&**db)
        .await
        .expect("event must insert");
    }

    /// Returns the new intent's id.
    async fn seed_intent(db: &TestDb, event_id: i64, account_id: i64, leader_id: i64) -> i64 {
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (?, ?, ?, 'eoa') ON CONFLICT(id) DO NOTHING",
        )
        .bind(account_id)
        .bind(format!("account-{account_id}"))
        .bind(format!("0x{account_id:040x}"))
        .execute(&**db)
        .await
        .expect("account must insert");

        sqlx::query_scalar(
            "INSERT INTO copy_intents \
             (event_id, account_id, leader_id, token_id, side, config_snapshot_json, \
              config_snapshot_hash, shard_scheme_version, lane_count, shard_id) \
             VALUES (?, ?, ?, '123456', 'BUY', ?, 'hash', 1, 1, 0) RETURNING id",
        )
        .bind(event_id)
        .bind(account_id)
        .bind(leader_id)
        .bind(format!(r#"{{"leader_id":{leader_id}}}"#))
        .fetch_one(&**db)
        .await
        .expect("intent must insert")
    }

    #[tokio::test]
    async fn the_shim_always_reports_no_signal() {
        assert_eq!(CopyStrategyStatusShim.status(), SignalStatus::NoSignal);
    }

    #[tokio::test]
    async fn updating_one_leader_does_not_affect_another_leaders_snapshot() {
        let db = TestDb::new().await;
        seed_leader(&db, 1, "leader-one").await;
        seed_leader(&db, 2, "leader-two").await;
        seed_event(&db, 1, 1, "activity:1").await;
        seed_event(&db, 2, 2, "activity:2").await;
        let intent_one = seed_intent(&db, 1, 1, 1).await;
        let intent_two = seed_intent(&db, 2, 1, 2).await;

        let before = leader_intents(&db, 2).await.unwrap();
        assert_eq!(before.len(), 1);
        let snapshot_before = before[0].config_snapshot_json.clone();

        // Update leader one's config and status -- leader two's already
        // planned intent, and leader two's own status row, must be
        // unaffected.
        sqlx::query("UPDATE leader_config SET label = 'leader-one-renamed', enabled = 0 WHERE id = 1")
            .execute(&*db)
            .await
            .unwrap();
        let _ = intent_one;

        let after = leader_intents(&db, 2).await.unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].config_snapshot_json, snapshot_before);
        assert_eq!(after[0].intent_id, intent_two);

        let leader_two_status = leader_status(&db, 2).await.unwrap();
        assert!(leader_two_status.enabled, "leader two must still be enabled");
        assert_eq!(leader_two_status.label, "leader-two");
    }

    #[tokio::test]
    async fn disabling_a_leader_retains_its_existing_lot_visibility() {
        let db = TestDb::new().await;
        seed_leader(&db, 1, "leader-one").await;
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&*db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO position_lots (account_id, leader_id, token_id, qty) \
             VALUES (1, 1, '123456', '20')",
        )
        .execute(&*db)
        .await
        .unwrap();

        sqlx::query("UPDATE leader_config SET enabled = 0 WHERE id = 1")
            .execute(&*db)
            .await
            .unwrap();

        let status = leader_status(&db, 1).await.unwrap();
        assert!(!status.enabled);

        let lots = leader_lots(&db, 1).await.unwrap();
        assert_eq!(lots.len(), 1, "disabling a leader must never erase its lots");
        assert_eq!(lots[0].qty, "20");
    }

    #[tokio::test]
    async fn trace_attempt_links_event_leader_account_snapshot_and_reconciliation_case() {
        let db = TestDb::new().await;
        seed_leader(&db, 1, "leader-one").await;
        seed_event(&db, 1, 1, "activity:1").await;
        let intent_id = seed_intent(&db, 1, 1, 1).await;

        let attempt_id: i64 = sqlx::query_scalar(
            "INSERT INTO order_attempts (intent_id, attempt_number, envelope_json, status, requested_qty) \
             VALUES (?, 1, '{}', 'accepted', '5') RETURNING id",
        )
        .bind(intent_id)
        .fetch_one(&*db)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO reconciliation_cases (account_id, token_id, intent_id, order_attempt_id, case_type, detail) \
             VALUES (1, '123456', ?, ?, 'strict_query_failure', 'test case')",
        )
        .bind(intent_id)
        .bind(attempt_id)
        .execute(&*db)
        .await
        .unwrap();

        let trace = trace_attempt(&db, attempt_id).await.unwrap();

        assert_eq!(trace.attempt_id, attempt_id);
        assert_eq!(trace.intent.intent_id, intent_id);
        assert_eq!(trace.intent.leader_id, 1);
        assert_eq!(trace.event.canonical_event_key, "activity:1");
        assert_eq!(trace.leader.label, "leader-one");
        assert_eq!(trace.account.account_id, 1);
        assert_eq!(trace.reconciliation_cases.len(), 1);
        assert_eq!(trace.reconciliation_cases[0].case_type, "strict_query_failure");
    }

    #[tokio::test]
    async fn tracing_an_unknown_attempt_is_a_named_error_not_a_panic() {
        let db = TestDb::new().await;
        let result = trace_attempt(&db, 999).await;
        assert!(matches!(result, Err(ControlTowerError::AttemptNotFound)));
    }

    #[tokio::test]
    async fn a_case_with_no_intent_id_is_never_attributed_to_any_leader() {
        let db = TestDb::new().await;
        seed_leader(&db, 1, "leader-one").await;
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&*db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO reconciliation_cases (account_id, token_id, intent_id, case_type, detail) \
             VALUES (1, '123456', NULL, 'other', 'unattributed case')",
        )
        .execute(&*db)
        .await
        .unwrap();

        let cases = leader_reconciliation_cases(&db, 1).await.unwrap();
        assert!(cases.is_empty());

        let row_count: i64 = sqlx::query("SELECT COUNT(*) FROM reconciliation_cases")
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(row_count, 1, "the unattributed case must still exist in the table");
    }
}
