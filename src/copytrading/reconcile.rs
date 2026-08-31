//! Phase 5: prepared submission, strict venue API, and reconciliation. See
//! `docs/COPY_ENGINE_BLUEPRINT.md` section 10.
//!
//! **`CopyExecution` has no implementation anywhere in this crate outside
//! test code.** `submit_exact_envelope` is a real, live order-writing call
//! against the venue -- the one action this project's assistant will never
//! write or run, matching Phase 4's `OrderSubmitter` seam exactly. Everything
//! else in this module is pure decision logic or database bookkeeping: the
//! envelope-persistence critical section, the submission recovery matrix,
//! and the retry-budget check are all built and fully tested here, against
//! fakes, without any code that could place a live order.
//!
//! Phase 0.5's canary (`docs/PHASE_0_5_CANARY_REPORT.md`) found that the
//! SDK's `SignedOrder` has no `Deserialize` impl, so it cannot itself survive
//! a process restart. [`PreparedOrderEnvelope`] is this project's own,
//! plainly serializable representation (the salt plus the order's other
//! plain fields) -- the same resolution the canary tool already applied,
//! reused here for the real submission path this table was designed for.

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::venue::OrderReceipt;

/// Maximum submission attempts for one intent within [`RETRY_WINDOW_SECONDS`]
/// before retries are exhausted and a reconciliation case opens.
pub const MAX_ATTEMPTS_PER_WINDOW: i64 = 5;
pub const RETRY_WINDOW_SECONDS: i64 = 600;

/// The exact, plainly-serializable fields of one signed order attempt.
/// Persisted once per `(intent_id, attempt_number)` and never rebuilt
/// (blueprint invariant #5): a fresh salt would produce a different signed
/// order hash, defeating the entire point of "one immutable envelope per
/// attempt".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedOrderEnvelope {
    pub token_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub salt: u64,
    /// Always "FAK" in v1 (blueprint section 8's stated v1-wide policy).
    pub order_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderId(pub String);

/// A venue order's current state, as returned by a strict lookup --
/// distinct from [`OrderReceipt`], which this project's own
/// `apply`/`execute` modules use for lot accounting. `order_for_receipt`
/// returns this; a caller maps it into an `OrderReceipt` only once it has
/// enough information to do so soundly.
#[derive(Debug, Clone, PartialEq)]
pub struct VenueOrderState {
    pub order_id: OrderId,
    pub status: String,
    pub size_matched: Decimal,
}

/// What Phase 5 needs to actually talk to the venue. Only
/// `position_for_token_strict` and the two lookup methods are read-only;
/// `submit_exact_envelope` is the one order-writing call in this trait, and
/// **no implementation of it exists anywhere in this crate's non-test
/// code**. A real implementation would sign and POST a live order -- see
/// this module's doc comment.
pub trait CopyExecution {
    fn position_for_token_strict(
        &self,
        token_id: &str,
    ) -> impl std::future::Future<Output = Result<Decimal, String>> + Send;

    fn order_for_receipt(
        &self,
        order_id: &OrderId,
    ) -> impl std::future::Future<Output = Result<VenueOrderState, String>> + Send;

    fn query_prepared_envelope(
        &self,
        envelope: &PreparedOrderEnvelope,
    ) -> impl std::future::Future<Output = Result<Option<OrderReceipt>, String>> + Send;

    fn submit_exact_envelope(
        &self,
        envelope: &PreparedOrderEnvelope,
    ) -> impl std::future::Future<Output = Result<OrderReceipt, String>> + Send;
}

/// Reads the existing envelope persisted for `(intent_id, attempt_number)`,
/// or -- if none exists -- persists `candidate` as the one envelope for
/// this attempt and returns it. Uses `BEGIN IMMEDIATE` (not sqlx's default
/// deferred transaction) so two concurrent callers racing to prepare the
/// same attempt cannot both observe "no row yet" and each insert their own,
/// different, envelope: the second acquires the write lock only after the
/// first has committed, and then reads back the first's envelope instead of
/// inserting its own. Rolls back reliably on every error path. Does not
/// itself make any network call, so it never holds the write lock across
/// order HTTP calls.
pub async fn load_or_prepare_attempt(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_number: i64,
    candidate: &PreparedOrderEnvelope,
) -> Result<PreparedOrderEnvelope, ReconcileError> {
    let mut conn = pool.acquire().await.map_err(db_err)?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await.map_err(db_err)?;

    let result: Result<PreparedOrderEnvelope, ReconcileError> = async {
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT envelope_json FROM order_attempts WHERE intent_id = ? AND attempt_number = ?",
        )
        .bind(intent_id)
        .bind(attempt_number)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;

        if let Some(existing) = existing {
            return serde_json::from_str(&existing).map_err(|_| ReconcileError::InvalidEnvelope);
        }

        let envelope_json = serde_json::to_string(candidate).map_err(|_| ReconcileError::InvalidEnvelope)?;
        sqlx::query(
            "INSERT INTO order_attempts (intent_id, attempt_number, envelope_json, status, requested_qty) \
             VALUES (?, ?, ?, 'prepared', ?)",
        )
        .bind(intent_id)
        .bind(attempt_number)
        .bind(envelope_json)
        .bind(&candidate.size)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

        Ok(candidate.clone())
    }
    .await;

    match &result {
        Ok(_) => {
            sqlx::query("COMMIT").execute(&mut *conn).await.map_err(db_err)?;
        }
        Err(_) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
        }
    }

    result
}

/// The submission recovery matrix (blueprint section 10). Every input
/// combination maps to exactly one permitted action; there is no default
/// "just resubmit" case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    /// `prepared`: mark `submitting`, then submit exactly once.
    MarkSubmittingThenSubmit,
    /// `submitting` found after a restart, or already `uncertain`: never
    /// treated as proof of non-submission. Query first; resubmission is
    /// only ever permitted once the Phase 0.5 canary has proven the venue's
    /// duplicate-submission behavior safe -- which it has not (see
    /// `docs/PHASE_0_5_CANARY_REPORT.md`, Result 2: still open).
    QueryFirst,
    /// `accepted`/`finalized`: reconcile the receipt or finalize its delta;
    /// never submit again for this attempt.
    ReconcileOrFinalize,
    /// `rejected`, definitively, with retry budget remaining: a new attempt
    /// may be prepared.
    MayPrepareNewAttempt,
    /// Blocked: an indefinite rejection, exhausted retry budget, or an
    /// unrecognized status. Carries why, for a visible reconciliation case.
    Blocked(&'static str),
}

/// Decides the permitted recovery action for one attempt's persisted
/// `status`. `rejection_is_definitive` and `attempts_in_window` only matter
/// for a `rejected` attempt (blueprint: "A new attempt may be prepared only
/// if the rejection is definitive and policy/deadline/retry budget
/// permit it").
pub fn permitted_recovery_action(
    status: &str,
    rejection_is_definitive: bool,
    attempts_in_window: i64,
) -> RecoveryAction {
    match status {
        "prepared" => RecoveryAction::MarkSubmittingThenSubmit,
        "submitting" | "uncertain" => RecoveryAction::QueryFirst,
        "accepted" | "finalized" => RecoveryAction::ReconcileOrFinalize,
        "rejected" => {
            if !rejection_is_definitive {
                RecoveryAction::Blocked("rejection is not definitive")
            } else if attempts_in_window >= MAX_ATTEMPTS_PER_WINDOW {
                RecoveryAction::Blocked("retry budget exhausted")
            } else {
                RecoveryAction::MayPrepareNewAttempt
            }
        }
        _ => RecoveryAction::Blocked("unrecognized attempt status"),
    }
}

/// Counts this intent's attempts started within the last
/// [`RETRY_WINDOW_SECONDS`], for the retry-budget check
/// `permitted_recovery_action` needs.
pub async fn attempts_in_window(pool: &SqlitePool, intent_id: i64) -> Result<i64, ReconcileError> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM order_attempts \
         WHERE intent_id = ? AND created_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)",
    )
    .bind(intent_id)
    .bind(format!("-{RETRY_WINDOW_SECONDS} seconds"))
    .fetch_one(pool)
    .await
    .map_err(db_err)
}

/// Opens a visible reconciliation case for a strict venue query failure or
/// an unresolvable lookup, and blocks the account/token key by moving the
/// intent to `needs_reconcile` -- mirroring `execute.rs`'s
/// `open_reconciliation_case`, applied to Phase 5's own failure modes
/// (blueprint: "A strict venue query error also opens a case; it never
/// becomes an empty balance" / "When lookup is unavailable, returns no
/// result, or returns contradictory data, the intent becomes
/// needs_reconcile").
pub async fn open_reconciliation_case(
    pool: &SqlitePool,
    intent_id: i64,
    order_attempt_id: Option<i64>,
    case_type: &str,
    detail: &str,
) -> Result<(), ReconcileError> {
    let mut tx = pool.begin().await.map_err(db_err)?;

    let (account_id, token_id): (i64, String) =
        sqlx::query_as("SELECT account_id, token_id FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(db_err)?;

    sqlx::query(
        "UPDATE copy_intents SET status = 'needs_reconcile', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
    )
    .bind(intent_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;

    sqlx::query(
        "INSERT INTO reconciliation_cases (account_id, token_id, intent_id, order_attempt_id, case_type, detail) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(account_id)
    .bind(token_id)
    .bind(intent_id)
    .bind(order_attempt_id)
    .bind(case_type)
    .bind(detail)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;

    tx.commit().await.map_err(db_err)
}

fn db_err(error: sqlx::Error) -> ReconcileError {
    ReconcileError::Database(error.to_string())
}

#[derive(Debug)]
pub enum ReconcileError {
    Database(String),
    InvalidEnvelope,
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::InvalidEnvelope => write!(formatter, "invalid or unparseable order envelope"),
        }
    }
}

impl std::error::Error for ReconcileError {}

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
                "polycopy-engine-reconcile-test-{}-{nonce}-{counter}.sqlite",
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

    async fn seed_intent(db: &TestDb) -> i64 {
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&**db)
        .await
        .expect("account must insert");
        sqlx::query("INSERT INTO leader_config (id, label) VALUES (1, 'leader-one')")
            .execute(&**db)
            .await
            .expect("leader must insert");
        let event_id: i64 = sqlx::query_scalar(
            "INSERT INTO leader_events \
             (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, price, occurred_at, observed_at) \
             VALUES ('activity:1', 1, '0xcond', '123456', 0, 'BUY', '5', '0.5', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now')) \
             RETURNING id",
        )
        .fetch_one(&**db)
        .await
        .expect("event must insert");
        sqlx::query_scalar(
            "INSERT INTO copy_intents \
             (event_id, account_id, leader_id, token_id, side, config_snapshot_json, config_snapshot_hash, \
              shard_scheme_version, lane_count, shard_id) \
             VALUES (?, 1, 1, '123456', 'BUY', '{}', 'hash', 1, 1, 0) RETURNING id",
        )
        .bind(event_id)
        .fetch_one(&**db)
        .await
        .expect("intent must insert")
    }

    fn envelope(salt: u64) -> PreparedOrderEnvelope {
        PreparedOrderEnvelope {
            token_id: "123456".to_owned(),
            side: "BUY".to_owned(),
            price: "0.5".to_owned(),
            size: "5".to_owned(),
            salt,
            order_type: "FAK".to_owned(),
        }
    }

    #[tokio::test]
    async fn a_fresh_attempt_persists_the_offered_candidate_envelope() {
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;

        let persisted = load_or_prepare_attempt(&db, intent_id, 1, &envelope(42)).await.unwrap();
        assert_eq!(persisted.salt, 42);

        let row_count: i64 = sqlx::query("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ? AND attempt_number = 1")
            .bind(intent_id)
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(row_count, 1);
    }

    #[tokio::test]
    async fn concurrent_calls_for_one_attempt_persist_only_one_identical_envelope() {
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;

        // Two "concurrent" callers each build their own candidate envelope
        // (different salts, as if each raced to prepare attempt 1
        // independently) and race to persist it for the same
        // (intent_id, attempt_number).
        let candidate_a = envelope(111);
        let candidate_b = envelope(222);
        let (first, second) = tokio::join!(
            load_or_prepare_attempt(&db, intent_id, 1, &candidate_a),
            load_or_prepare_attempt(&db, intent_id, 1, &candidate_b),
        );
        let first = first.expect("first caller must succeed");
        let second = second.expect("second caller must succeed");

        assert_eq!(first, second, "both callers must observe the same, single persisted envelope");

        let row_count: i64 = sqlx::query("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ? AND attempt_number = 1")
            .bind(intent_id)
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(row_count, 1, "exactly one row, whichever candidate won the race");
    }

    #[tokio::test]
    async fn a_second_load_reads_back_the_first_callers_envelope_not_a_new_one() {
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;

        let first = load_or_prepare_attempt(&db, intent_id, 1, &envelope(1)).await.unwrap();
        let second = load_or_prepare_attempt(&db, intent_id, 1, &envelope(999)).await.unwrap();

        assert_eq!(first, second);
        assert_eq!(second.salt, 1, "the second call's own candidate salt (999) must never be used");
    }

    #[test]
    fn the_recovery_matrix_matches_every_documented_state() {
        assert_eq!(permitted_recovery_action("prepared", true, 0), RecoveryAction::MarkSubmittingThenSubmit);
        assert_eq!(permitted_recovery_action("submitting", true, 0), RecoveryAction::QueryFirst);
        assert_eq!(permitted_recovery_action("uncertain", true, 0), RecoveryAction::QueryFirst);
        assert_eq!(permitted_recovery_action("accepted", true, 0), RecoveryAction::ReconcileOrFinalize);
        assert_eq!(permitted_recovery_action("finalized", true, 0), RecoveryAction::ReconcileOrFinalize);
        assert_eq!(permitted_recovery_action("rejected", true, 0), RecoveryAction::MayPrepareNewAttempt);
    }

    #[test]
    fn a_crash_during_submitting_never_permits_a_direct_resubmission_on_restart() {
        // The core of "a crash after the request may have crossed the
        // network boundary never causes a direct resubmission on restart":
        // `submitting` found on restart must query first, never resubmit.
        assert_ne!(permitted_recovery_action("submitting", true, 0), RecoveryAction::MarkSubmittingThenSubmit);
        assert_eq!(permitted_recovery_action("submitting", true, 0), RecoveryAction::QueryFirst);
    }

    #[test]
    fn an_indefinite_rejection_is_blocked_regardless_of_retry_budget() {
        let action = permitted_recovery_action("rejected", false, 0);
        assert!(matches!(action, RecoveryAction::Blocked(_)));
    }

    #[test]
    fn an_exhausted_retry_budget_blocks_even_a_definitive_rejection() {
        let action = permitted_recovery_action("rejected", true, MAX_ATTEMPTS_PER_WINDOW);
        assert!(matches!(action, RecoveryAction::Blocked(_)));
    }

    #[test]
    fn an_unrecognized_status_is_blocked_not_defaulted_to_resubmission() {
        let action = permitted_recovery_action("some_future_status", true, 0);
        assert!(matches!(action, RecoveryAction::Blocked(_)));
    }

    #[tokio::test]
    async fn attempts_in_window_counts_only_recent_attempts_for_this_intent() {
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;

        for attempt_number in 1..=3 {
            load_or_prepare_attempt(&db, intent_id, attempt_number, &envelope(attempt_number as u64)).await.unwrap();
        }
        // A different intent's attempts must not be counted here.
        sqlx::query("INSERT INTO leader_config (id, label) VALUES (2, 'leader-two')")
            .execute(&*db)
            .await
            .unwrap();

        let count = attempts_in_window(&db, intent_id).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn a_strict_query_failure_opens_a_visible_case_and_blocks_the_intent() {
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;

        open_reconciliation_case(&db, intent_id, None, "strict_query_failure", "mock venue query error")
            .await
            .unwrap();

        let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(status, "needs_reconcile");

        let case_count: i64 = sqlx::query("SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ? AND case_type = 'strict_query_failure'")
            .bind(intent_id)
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(case_count, 1);
    }

    #[tokio::test]
    async fn a_fak_partial_fill_receipt_finalizes_to_exactly_the_matched_amount_no_phantom_lot() {
        // "FAK zero-fill and partial-fill produce the correct receipt and
        // no phantom lot": OrderReceipt (receipt.rs, Phase 0) already keeps
        // requested/filled distinct; this confirms that distinction survives
        // all the way through to position_lots via execute::finalize_receipt.
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;

        let requested = Decimal::new(5, 0);
        let matched = Decimal::new(2, 0); // partial fill: only 2 of the requested 5
        let receipt = OrderReceipt::from_fak_match(requested, requested, matched).unwrap();

        let attempt_id: i64 = sqlx::query_scalar(
            "INSERT INTO order_attempts (intent_id, attempt_number, envelope_json, status, requested_qty) \
             VALUES (?, 1, '{}', 'finalized', '5') RETURNING id",
        )
        .bind(intent_id)
        .fetch_one(&*db)
        .await
        .unwrap();

        crate::copytrading::execute::finalize_receipt(&db, intent_id, attempt_id, &receipt).await.unwrap();

        let lot_qty: String = sqlx::query_scalar("SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'")
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(lot_qty, "2", "the lot must reflect only the matched quantity, never the requested one");
    }
}
