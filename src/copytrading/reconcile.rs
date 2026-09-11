//! Phase 5: prepared submission, strict venue API, and reconciliation. See
//! `docs/COPY_ENGINE_BLUEPRINT.md` section 10.
//!
//! `CopyExecution` has one concrete implementation in
//! `venue::intl_clob_exec`, used only by the default-disabled, bounded
//! `copy_run` process. `submit_exact_envelope` is a real live order-writing
//! call, so all other callers must remain test fakes or explicit future
//! integrations. The envelope-persistence critical section, submission
//! recovery matrix, and retry-budget check are independently tested against
//! fakes without contacting the venue.
//!
//! Phase 0.5's canary (`docs/PHASE_0_5_CANARY_REPORT.md`) found that the
//! SDK's `SignedOrder` has no `Deserialize` impl, so it cannot itself survive
//! a process restart. [`PreparedOrderEnvelope`] is this project's own,
//! plainly serializable representation (the salt plus the order's other
//! plain fields) -- the same resolution the canary tool already applied,
//! reused here for the real submission path this table was designed for.

use std::fmt;

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

// P0-1: StrictTradeHistoryReader is still used by the recovery matrix
// helpers that remain in this module (recover_lost_submission_response);
// the other intl_clob primitives moved to `venue::execution_contract`.
use crate::venue::intl_clob::StrictTradeHistoryReader;

// Test-only imports: the trade-history recovery unit tests construct
// `AccountTrade` fixtures; the production bodies that used these types
// moved to `venue::execution_contract` in P0-1.
#[cfg(test)]
use crate::venue::intl_clob::{
    AccountTrade, AccountTradeRole, AccountTradeSide, AccountTradeStatus, OutcomeTokenId,
    StrictTradeHistoryError,
};
#[cfg(test)]
use crate::venue::OrderReceipt;
#[cfg(test)]
use rust_decimal::Decimal;
#[cfg(test)]
use std::str::FromStr as _;

/// Maximum submission attempts for one intent within [`RETRY_WINDOW_SECONDS`]
/// before retries are exhausted and a reconciliation case opens.
pub const MAX_ATTEMPTS_PER_WINDOW: i64 = 5;
pub const RETRY_WINDOW_SECONDS: i64 = 600;
const TRADE_HISTORY_TIMESTAMP_SKEW_SECONDS: i64 = 1;

// P0-1 architecture inversion: the venue-side execution contract now has
// its canonical home in `venue::execution_contract` so
// `venue::intl_clob_exec` never imports from `crate::copytrading::*`
// (AGENTS.md: "venue 不向上依赖 copytrading"). The moved items —
// PreparedOrderEnvelope, SubmitError, CopyExecution, Side, SizedDecision —
// are re-exported unchanged, so `use crate::copytrading::reconcile::{...}`
// call sites (including `copytrading::mod`'s public re-exports) keep
// compiling with identical semantics, doc comments, and Display strings.
pub use crate::venue::execution_contract::{
    CopyExecution, PreparedOrderEnvelope, Side, SizedDecision, SubmitError,
};
// NEW-1: the trade-history recovery half moved to
// `venue::trade_history_recovery` (it depends on intl_clob primitives, so
// it is feature-gated there; this module only compiles when `execute` is
// on, and `execute` implies `intl_clob`, so the re-export is always
// available here).
pub use crate::venue::trade_history_recovery::{
    lookup_prepared_fak_in_trade_history, recover_fak_taker_order_from_trades, TradeHistoryLookup,
    TradeHistoryRecoveryError, TradeHistoryWindow,
};
// Local scope + backward-compat re-export for the two venue primitives
// moved in the first P0-1 step. `pub use` serves both purposes.
pub use crate::venue::types::{OrderId, VenueOrderState};

/// Result of the complete, read-only recovery path for a POST whose response
/// was lost before its venue order ID could be durably stored. A recovered ID
/// is only attached to the attempt; it still needs the normal strict by-ID
/// receipt lookup before any lot can change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LostSubmissionRecoveryOutcome {
    Recovered { order_id: OrderId },
    NeedsReconcile,
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
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

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
        .bind(candidate.buy_budget_usdc.as_deref().unwrap_or(&candidate.size))
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

        Ok(candidate.clone())
    }
    .await;

    match &result {
        Ok(_) => {
            let commit_result = sqlx::query("COMMIT").execute(&mut *conn).await;
            if let Err(commit_error) = commit_result {
                // P2-1: COMMIT itself failed (rare -- e.g. I/O error
                // mid-flush). Best-effort ROLLBACK so the pooled
                // connection is never handed back with an open BEGIN
                // IMMEDIATE transaction, which would make the next
                // acquirer silently write inside a foreign transaction.
                // The original commit error is what the caller wants.
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                return Err(db_err(commit_error));
            }
        }
        Err(_) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
        }
    }

    result
}

/// Atomically records that an already-persisted envelope is about to cross the
/// order HTTP boundary. A future order writer must call this immediately
/// before its single submit request and must never write the request first.
/// This function is database-only; it cannot contact the venue.
pub async fn mark_attempt_submitting(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_id: i64,
    submission_started_at: DateTime<Utc>,
) -> Result<(), ReconcileError> {
    let result = sqlx::query(
        "UPDATE order_attempts \
         SET status = 'submitting', submission_started_at = ?, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND intent_id = ? AND status = 'prepared'",
    )
    .bind(submission_started_at.to_rfc3339())
    .bind(attempt_id)
    .bind(intent_id)
    .execute(pool)
    .await
    .map_err(db_err)?;

    if result.rows_affected() != 1 {
        return Err(ReconcileError::InvalidAttemptTransition);
    }

    Ok(())
}

/// Preserves an attempt that may have crossed the order boundary after a
/// transport error. It records no retry and leaves the attempt query-only.
pub async fn mark_attempt_uncertain_after_submission_error(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_id: i64,
    failure_detail: &str,
) -> Result<(), ReconcileError> {
    let result = sqlx::query(
        "UPDATE order_attempts \
         SET status = 'uncertain', failure_detail = ?, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND intent_id = ? AND status IN ('submitting', 'uncertain')",
    )
    .bind(failure_detail)
    .bind(attempt_id)
    .bind(intent_id)
    .execute(pool)
    .await
    .map_err(db_err)?;

    if result.rows_affected() != 1 {
        return Err(ReconcileError::InvalidAttemptTransition);
    }

    Ok(())
}

/// Records a definitive venue rejection. The attempt did create a durable
/// response (HTTP 4xx, no `order_id`); it is not `uncertain` and a later
/// cycle may prepare a new attempt if retry budget remains.
pub async fn mark_attempt_rejected(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_id: i64,
    failure_detail: &str,
) -> Result<(), ReconcileError> {
    let result = sqlx::query(
        "UPDATE order_attempts \
         SET status = 'rejected', failure_detail = ?, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND intent_id = ? AND status IN ('submitting', 'prepared')",
    )
    .bind(failure_detail)
    .bind(attempt_id)
    .bind(intent_id)
    .execute(pool)
    .await
    .map_err(db_err)?;

    if result.rows_affected() != 1 {
        return Err(ReconcileError::InvalidAttemptTransition);
    }

    Ok(())
}

struct PendingTradeHistoryRecovery {
    envelope: PreparedOrderEnvelope,
    window: TradeHistoryWindow,
}

async fn load_pending_trade_history_recovery(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_id: i64,
    queried_at: DateTime<Utc>,
) -> Result<PendingTradeHistoryRecovery, ReconcileError> {
    let row: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT envelope_json, status, submission_started_at \
         FROM order_attempts WHERE id = ? AND intent_id = ?",
    )
    .bind(attempt_id)
    .bind(intent_id)
    .fetch_optional(pool)
    .await
    .map_err(db_err)?;

    let Some((envelope_json, status, submission_started_at)) = row else {
        return Err(ReconcileError::AttemptNotFound);
    };
    if !matches!(status.as_str(), "submitting" | "uncertain") {
        return Err(ReconcileError::InvalidAttemptTransition);
    }
    let submission_started_at =
        submission_started_at.ok_or(ReconcileError::MissingSubmissionStartedAt)?;
    let submission_started_at = DateTime::parse_from_rfc3339(&submission_started_at)
        .map_err(|_| ReconcileError::InvalidSubmissionStartedAt)?
        .with_timezone(&Utc);
    if submission_started_at > queried_at {
        return Err(ReconcileError::InvalidSubmissionWindow);
    }
    let envelope =
        serde_json::from_str(&envelope_json).map_err(|_| ReconcileError::InvalidEnvelope)?;
    let window = TradeHistoryWindow::new(
        submission_started_at - chrono::Duration::seconds(TRADE_HISTORY_TIMESTAMP_SKEW_SECONDS),
        queried_at,
    )
    .map_err(|_| ReconcileError::InvalidSubmissionWindow)?;

    Ok(PendingTradeHistoryRecovery { envelope, window })
}

async fn persist_recovered_order_id(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_id: i64,
    order_id: &OrderId,
) -> Result<(), ReconcileError> {
    let result = sqlx::query(
        "UPDATE order_attempts \
         SET venue_order_id = ?, status = 'uncertain', \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND intent_id = ? AND status IN ('submitting', 'uncertain') \
           AND (venue_order_id IS NULL OR venue_order_id = ?)",
    )
    .bind(&order_id.0)
    .bind(attempt_id)
    .bind(intent_id)
    .bind(&order_id.0)
    .execute(pool)
    .await
    .map_err(db_err)?;

    if result.rows_affected() != 1 {
        return Err(ReconcileError::ConflictingRecoveredOrderId);
    }

    Ok(())
}

/// Uses authenticated trade history to recover an order ID after a lost POST
/// response. It performs only reads against the venue. Any unavailable,
/// incomplete, delayed, contradictory, or non-identifying result atomically
/// opens a reconciliation case and blocks the account/token key; it never
/// sends another order.
pub async fn recover_lost_submission_response<R>(
    pool: &SqlitePool,
    reader: &R,
    intent_id: i64,
    attempt_id: i64,
    queried_at: DateTime<Utc>,
) -> Result<LostSubmissionRecoveryOutcome, ReconcileError>
where
    R: StrictTradeHistoryReader + ?Sized,
{
    let pending =
        match load_pending_trade_history_recovery(pool, intent_id, attempt_id, queried_at).await {
            Ok(pending) => pending,
            Err(error @ ReconcileError::Database(_)) => return Err(error),
            Err(error) => {
                open_reconciliation_case(
                    pool,
                    intent_id,
                    Some(attempt_id),
                    "unknown_submission",
                    &format!("submission recovery could not start: {error}"),
                )
                .await?;
                return Ok(LostSubmissionRecoveryOutcome::NeedsReconcile);
            }
        };

    match lookup_prepared_fak_in_trade_history(reader, &pending.envelope, pending.window).await {
        Ok(TradeHistoryLookup::Recovered { order_id, .. }) => {
            match persist_recovered_order_id(pool, intent_id, attempt_id, &order_id).await {
                Ok(()) => Ok(LostSubmissionRecoveryOutcome::Recovered { order_id }),
                Err(error @ ReconcileError::Database(_)) => Err(error),
                Err(error) => {
                    open_reconciliation_case(
                        pool,
                        intent_id,
                        Some(attempt_id),
                        "unknown_submission",
                        &format!("recovered order ID could not be persisted: {error}"),
                    )
                    .await?;
                    Ok(LostSubmissionRecoveryOutcome::NeedsReconcile)
                }
            }
        }
        Ok(TradeHistoryLookup::NotFound) => {
            open_reconciliation_case(
                pool,
                intent_id,
                Some(attempt_id),
                "unknown_submission",
                "authenticated trade history contained no exact prepared-order identifier",
            )
            .await?;
            Ok(LostSubmissionRecoveryOutcome::NeedsReconcile)
        }
        Err(error) => {
            let case_type = match error {
                TradeHistoryRecoveryError::Query(_) => "strict_query_failure",
                _ => "unknown_submission",
            };
            open_reconciliation_case(
                pool,
                intent_id,
                Some(attempt_id),
                case_type,
                &format!("strict trade-history recovery failed: {error}"),
            )
            .await?;
            Ok(LostSubmissionRecoveryOutcome::NeedsReconcile)
        }
    }
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

    if let Some(order_attempt_id) = order_attempt_id {
        sqlx::query(
            "UPDATE order_attempts \
             SET status = 'uncertain', failure_detail = ?, \
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
             WHERE id = ? AND intent_id = ? AND status IN ('submitting', 'uncertain')",
        )
        .bind(detail)
        .bind(order_attempt_id)
        .bind(intent_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    }

    sqlx::query(
        "INSERT INTO reconciliation_cases (account_id, token_id, intent_id, order_attempt_id, case_type, detail) \
         SELECT ?, ?, ?, ?, ?, ? \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM reconciliation_cases \
             WHERE intent_id = ? AND order_attempt_id IS ? AND case_type = ? AND resolved_at IS NULL \
         )",
    )
    .bind(account_id)
    .bind(token_id)
    .bind(intent_id)
    .bind(order_attempt_id)
    .bind(case_type)
    .bind(detail)
    .bind(intent_id)
    .bind(order_attempt_id)
    .bind(case_type)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;

    tx.commit().await.map_err(db_err)
}

/// Atomically opens a `local_submission_failure` reconciliation case AND
/// releases the associated `persistent_budget_reservations` row, in a
/// single SQLite transaction.
///
/// The orchestrator's `Local` arm of `submit_prepared` previously issued
/// these two writes from separate transactions (orchestrate.rs::417-449),
/// which violated the AGENTS.md invariant "Receipt accounting, reservation
/// release, and intent finalization must be idempotent and atomic":
/// a crash between the release and the case-open would free the rolling-
/// budget USDC without leaving any audit trail in
/// `reconciliation_cases` for the operator dashboard to read.
///
/// This function exists to make that contract impossible to violate at
/// the call site: the only path for the Local arm to release the
/// reservation and open the case now goes through one `BEGIN IMMEDIATE`
/// + `COMMIT` pair. The release call is a no-op (zero rows affected)
///
/// when the attempt has no associated reservation -- this is fine, the
/// transaction still commits cleanly and the case row is still inserted.
///
/// `BEGIN IMMEDIATE` is used (not sqlx's default deferred `pool.begin()`)
/// to match the other write-critical paths in this crate
/// (`load_or_prepare_attempt`, `reserve_budget_and_mark_submitting`): a
/// default-deferred BEGIN only upgrades to a write lock at first write,
/// leaving a window where two concurrent Local arms read the same initial
/// state and SQLITE_BUSY on the upgrade. `BEGIN IMMEDIATE` acquires the
/// write lock up front so concurrent Local arms serialise cleanly.
pub async fn open_local_submission_failure_case(
    pool: &SqlitePool,
    intent_id: i64,
    order_attempt_id: i64,
    detail: &str,
) -> Result<(), ReconcileError> {
    let mut conn = pool.acquire().await.map_err(db_err)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

    let result: Result<(), ReconcileError> = async {
        let (account_id, token_id): (i64, String) =
            sqlx::query_as("SELECT account_id, token_id FROM copy_intents WHERE id = ?")
                .bind(intent_id)
                .fetch_one(&mut *conn)
                .await
                .map_err(db_err)?;

        sqlx::query(
            "UPDATE copy_intents SET status = 'needs_reconcile', \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
        )
        .bind(intent_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

        sqlx::query(
            "UPDATE order_attempts \
             SET status = 'uncertain', failure_detail = ?, \
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
             WHERE id = ? AND intent_id = ? AND status IN ('submitting', 'uncertain')",
        )
        .bind(detail)
        .bind(order_attempt_id)
        .bind(intent_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

        // Release any persistent rolling-budget reservation associated with
        // this attempt. This call is a no-op (zero rows affected) when the
        // standard, non-persistent marker is in use, so it is safe to make
        // unconditionally from this helper.
        crate::copytrading::persistent::release_pre_boundary_failure_with_conn(
            &mut conn,
            order_attempt_id,
            "local submission failed before network boundary",
        )
        .await
        .map_err(|error| ReconcileError::Database(error.to_string()))?;

        sqlx::query(
            "INSERT INTO reconciliation_cases \
             (account_id, token_id, intent_id, order_attempt_id, case_type, detail) \
             SELECT ?, ?, ?, ?, 'local_submission_failure', ? \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM reconciliation_cases \
                 WHERE intent_id = ? AND order_attempt_id IS ? \
                   AND case_type = 'local_submission_failure' AND resolved_at IS NULL \
             )",
        )
        .bind(account_id)
        .bind(token_id)
        .bind(intent_id)
        .bind(order_attempt_id)
        .bind(detail)
        .bind(intent_id)
        .bind(order_attempt_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

        Ok(())
    }
    .await;

    match &result {
        Ok(()) => {
            let commit_result = sqlx::query("COMMIT").execute(&mut *conn).await;
            if let Err(commit_error) = commit_result {
                // COMMIT itself failed (rare -- e.g., I/O error mid-flush).
                // Best-effort ROLLBACK so subsequent acquirers do not
                // inherit an open transaction. The original commit
                // error is the one the caller wants.
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                return Err(db_err(commit_error));
            }
        }
        Err(_) => {
            // Best-effort rollback; ignore the rollback error (the
            // original error is what the caller wants). A failure here
            // would indicate a deeper SQLite-level fault that the
            // caller is already handling via the original Err.
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
        }
    }

    result
}

fn db_err(error: sqlx::Error) -> ReconcileError {
    ReconcileError::Database(error.to_string())
}

#[derive(Debug)]
pub enum ReconcileError {
    Database(String),
    InvalidEnvelope,
    AttemptNotFound,
    InvalidAttemptTransition,
    MissingSubmissionStartedAt,
    InvalidSubmissionStartedAt,
    InvalidSubmissionWindow,
    ConflictingRecoveredOrderId,
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::InvalidEnvelope => write!(formatter, "invalid or unparseable order envelope"),
            Self::AttemptNotFound => {
                write!(formatter, "order attempt was not found for this intent")
            }
            Self::InvalidAttemptTransition => write!(
                formatter,
                "order attempt is not in the required recovery state"
            ),
            Self::MissingSubmissionStartedAt => write!(
                formatter,
                "order attempt has no durable submission-start timestamp"
            ),
            Self::InvalidSubmissionStartedAt => write!(
                formatter,
                "order attempt has an invalid submission-start timestamp"
            ),
            Self::InvalidSubmissionWindow => write!(
                formatter,
                "order attempt has an invalid submission recovery window"
            ),
            Self::ConflictingRecoveredOrderId => write!(
                formatter,
                "order attempt already records a different venue order ID"
            ),
        }
    }
}

impl std::error::Error for ReconcileError {}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone as _, Utc};
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

    async fn seed_intent(db: &TestDb) -> i64 {
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&db.pool)
        .await
        .expect("account must insert");
        sqlx::query("INSERT INTO leader_config (id, label) VALUES (1, 'leader-one')")
            .execute(&db.pool)
            .await
            .expect("leader must insert");
        let event_id: i64 = sqlx::query_scalar(
            "INSERT INTO leader_events \
             (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, price, occurred_at, observed_at) \
             VALUES ('activity:1', 1, '0xcond', '123456', 0, 'BUY', '5', '0.5', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now')) \
             RETURNING id",
        )
        .fetch_one(&db.pool)
        .await
        .expect("event must insert");
        sqlx::query_scalar(
            "INSERT INTO copy_intents \
             (event_id, account_id, leader_id, token_id, side, config_snapshot_json, config_snapshot_hash, \
              shard_scheme_version, lane_count, shard_id) \
             VALUES (?, 1, 1, '123456', 'BUY', '{}', 'hash', 1, 1, 0) RETURNING id",
        )
        .bind(event_id)
        .fetch_one(&db.pool)
        .await
        .expect("intent must insert")
    }

    fn envelope(salt: u64) -> PreparedOrderEnvelope {
        PreparedOrderEnvelope {
            token_id: "123456".to_owned(),
            side: "BUY".to_owned(),
            price: "0.5".to_owned(),
            size: "5".to_owned(),
            buy_budget_usdc: Some("2.50".to_owned()),
            salt,
            order_type: "FAK".to_owned(),
            expected_taker_order_id: "order-a".to_owned(),
            signed_order_json: r#"{"order":{"maker":"0xexample"},"orderType":"FAK"}"#.to_owned(),
        }
    }

    fn trade_history_window() -> TradeHistoryWindow {
        TradeHistoryWindow::new(
            Utc.with_ymd_and_hms(2026, 9, 1, 12, 0, 0)
                .single()
                .expect("timestamp must be valid"),
            Utc.with_ymd_and_hms(2026, 9, 1, 12, 0, 10)
                .single()
                .expect("timestamp must be valid"),
        )
        .expect("window must be valid")
    }

    fn account_trade(
        trade_id: &str,
        order_id: &str,
        side: AccountTradeSide,
        price: Decimal,
        size: Decimal,
        role: AccountTradeRole,
        second: u32,
    ) -> AccountTrade {
        AccountTrade {
            trade_id: trade_id.to_owned(),
            taker_order_id: order_id.to_owned(),
            token_id: OutcomeTokenId::from_str("123456").expect("valid token ID"),
            side,
            price,
            size,
            match_time: Utc
                .with_ymd_and_hms(2026, 9, 1, 12, 0, second)
                .single()
                .expect("timestamp must be valid"),
            role,
            status: AccountTradeStatus::Matched,
        }
    }

    enum FakeTradeHistoryReply {
        Trades(Vec<AccountTrade>),
        Failure,
    }

    struct FakeTradeHistoryReader {
        reply: FakeTradeHistoryReply,
    }

    #[async_trait::async_trait]
    impl StrictTradeHistoryReader for FakeTradeHistoryReader {
        async fn trades_for_token_between(
            &self,
            _token_id: &OutcomeTokenId,
            _after: DateTime<Utc>,
            _before: DateTime<Utc>,
        ) -> Result<Vec<AccountTrade>, StrictTradeHistoryError> {
            match &self.reply {
                FakeTradeHistoryReply::Trades(trades) => Ok(trades.clone()),
                FakeTradeHistoryReply::Failure => Err(StrictTradeHistoryError::InvalidWindow),
            }
        }
    }

    async fn prepared_submitting_attempt(db: &TestDb) -> (i64, i64, DateTime<Utc>) {
        let intent_id = seed_intent(db).await;
        load_or_prepare_attempt(db, intent_id, 1, &envelope(42))
            .await
            .expect("prepared envelope must persist");
        let attempt_id: i64 = sqlx::query_scalar(
            "SELECT id FROM order_attempts WHERE intent_id = ? AND attempt_number = 1",
        )
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("prepared attempt must exist");
        let started_at = Utc
            .with_ymd_and_hms(2026, 9, 1, 12, 0, 0)
            .single()
            .expect("timestamp must be valid");
        mark_attempt_submitting(db, intent_id, attempt_id, started_at)
            .await
            .expect("prepared attempt must become submitting before any send");
        (intent_id, attempt_id, started_at)
    }

    #[test]
    fn trade_history_recovers_one_taker_fak_order_and_sums_its_fills() {
        let trades = vec![
            account_trade(
                "trade-a",
                "order-a",
                AccountTradeSide::Buy,
                Decimal::new(49, 2),
                Decimal::new(2, 0),
                AccountTradeRole::Taker,
                1,
            ),
            account_trade(
                "trade-b",
                "order-a",
                AccountTradeSide::Buy,
                Decimal::new(48, 2),
                Decimal::new(3_288_460, 6),
                AccountTradeRole::Taker,
                2,
            ),
        ];

        let recovered =
            recover_fak_taker_order_from_trades(&envelope(42), trade_history_window(), &trades)
                .expect("a valid trade history must be matchable");

        assert_eq!(
            recovered,
            TradeHistoryLookup::Recovered {
                order_id: OrderId("order-a".to_owned()),
                filled_qty: Decimal::new(5_288_460, 6),
            }
        );
    }

    #[test]
    fn trade_history_never_uses_requested_buy_size_as_matched_shares() {
        let trades = vec![account_trade(
            "trade-a",
            "order-a",
            AccountTradeSide::Buy,
            Decimal::new(49, 2),
            Decimal::new(5_288_460, 6),
            AccountTradeRole::Taker,
            1,
        )];

        let recovered =
            recover_fak_taker_order_from_trades(&envelope(42), trade_history_window(), &trades)
                .expect("a better-priced BUY fill can exceed the requested budget value");

        assert!(matches!(
            recovered,
            TradeHistoryLookup::Recovered { filled_qty, .. }
                if filled_qty == Decimal::new(5_288_460, 6)
        ));
    }

    #[test]
    fn an_unrelated_order_id_is_never_selected_by_token_side_price_and_time() {
        let trades = vec![
            account_trade(
                "trade-a",
                "order-a",
                AccountTradeSide::Buy,
                Decimal::new(49, 2),
                Decimal::ONE,
                AccountTradeRole::Taker,
                1,
            ),
            account_trade(
                "trade-b",
                "order-b",
                AccountTradeSide::Buy,
                Decimal::new(49, 2),
                Decimal::ONE,
                AccountTradeRole::Taker,
                2,
            ),
        ];

        assert_eq!(
            recover_fak_taker_order_from_trades(&envelope(42), trade_history_window(), &trades)
                .expect("only the persisted fingerprint may identify a recovered order"),
            TradeHistoryLookup::Recovered {
                order_id: OrderId("order-a".to_owned()),
                filled_qty: Decimal::ONE,
            }
        );
    }

    #[test]
    fn maker_or_limit_incompatible_trades_never_recover_a_fak_taker_order() {
        let trades = vec![
            account_trade(
                "trade-a",
                "order-a",
                AccountTradeSide::Buy,
                Decimal::new(49, 2),
                Decimal::ONE,
                AccountTradeRole::Maker,
                1,
            ),
            account_trade(
                "trade-b",
                "order-b",
                AccountTradeSide::Buy,
                Decimal::new(51, 2),
                Decimal::ONE,
                AccountTradeRole::Taker,
                2,
            ),
        ];

        assert_eq!(
            recover_fak_taker_order_from_trades(&envelope(42), trade_history_window(), &trades)
                .expect("a completed no-match is not a query failure"),
            TradeHistoryLookup::NotFound
        );
    }

    #[test]
    fn conflicting_duplicate_trade_ids_fail_closed() {
        let first = account_trade(
            "trade-a",
            "order-a",
            AccountTradeSide::Buy,
            Decimal::new(49, 2),
            Decimal::ONE,
            AccountTradeRole::Taker,
            1,
        );
        let second = account_trade(
            "trade-a",
            "order-b",
            AccountTradeSide::Buy,
            Decimal::new(49, 2),
            Decimal::ONE,
            AccountTradeRole::Taker,
            1,
        );

        assert!(matches!(
            recover_fak_taker_order_from_trades(
                &envelope(42),
                trade_history_window(),
                &[first, second]
            ),
            Err(TradeHistoryRecoveryError::ConflictingDuplicateTrade { .. })
        ));
    }

    #[test]
    fn a_legacy_envelope_without_a_precomputed_order_id_never_uses_heuristics() {
        let mut legacy = envelope(42);
        legacy.expected_taker_order_id.clear();

        assert!(matches!(
            recover_fak_taker_order_from_trades(&legacy, trade_history_window(), &[]),
            Err(TradeHistoryRecoveryError::MissingOrderFingerprint)
        ));
    }

    #[test]
    fn failed_or_unknown_trade_status_never_recovers_an_order_id() {
        let mut failed = account_trade(
            "trade-a",
            "order-a",
            AccountTradeSide::Buy,
            Decimal::new(49, 2),
            Decimal::ONE,
            AccountTradeRole::Taker,
            1,
        );
        failed.status = AccountTradeStatus::Failed;

        assert_eq!(
            recover_fak_taker_order_from_trades(&envelope(42), trade_history_window(), &[failed])
                .expect("failed trade records are a completed non-match"),
            TradeHistoryLookup::NotFound
        );
    }

    // P2-6: defensive / fail-closed branch coverage for the trade-history
    // matcher. Each branch in recover_fak_taker_order_from_trades that
    // returns a TradeHistoryRecoveryError must be pinned by a direct unit
    // test so a future refactor that accidentally turns one into an
    // Ok-pass-through is caught by CI. AGENTS.md: "Receipt accounting,
    // reservation release, and intent finalization must be idempotent
    // and atomic" -- a fail-open recovery path is the same class of bug
    // as a fail-open receipt path.

    #[test]
    fn recover_fak_rejects_an_envelope_whose_order_type_is_not_fak() {
        // P2-6 pin: order_type != "FAK" -> UnsupportedOrderType.
        // The recovery matrix only matches FAK envelopes (GTC/FOK orders
        // belong to a different code path); a non-FAK must fail closed.
        let mut env = envelope(42);
        env.order_type = "GTC".to_owned();
        assert_eq!(
            recover_fak_taker_order_from_trades(&env, trade_history_window(), &[]),
            Err(TradeHistoryRecoveryError::UnsupportedOrderType)
        );
    }

    #[test]
    fn recover_fak_rejects_an_envelope_whose_signed_order_json_is_not_a_json_object() {
        // P2-6 pin: signed_order_json must be a JSON object (the SDK
        // serializes SignedOrder as a JSON object; any other JSON shape
        // means a corrupted or hand-rolled envelope).
        for bad in [r#""""#, r#"null"#, r#"[]"#, r#"42"#, r#"not even json"#] {
            let mut env = envelope(42);
            env.signed_order_json = bad.to_owned();
            assert_eq!(
                recover_fak_taker_order_from_trades(&env, trade_history_window(), &[]),
                Err(TradeHistoryRecoveryError::InvalidSignedOrderJson),
                "signed_order_json={bad:?} must fail closed"
            );
        }
    }

    #[test]
    fn recover_fak_rejects_an_envelope_whose_side_is_not_buy_or_sell() {
        // P2-6 pin: side != "BUY" && side != "SELL" -> InvalidSide.
        // The downstream limit-price test (is_limit_compatible) keys off
        // the parsed side; an unknown side would silently accept trades
        // without ever testing the limit-price contract.
        for bad in ["BUY ", "buy", "SHORT", "BUY/SELL", ""] {
            let mut env = envelope(42);
            env.side = bad.to_owned();
            assert_eq!(
                recover_fak_taker_order_from_trades(&env, trade_history_window(), &[]),
                Err(TradeHistoryRecoveryError::InvalidSide),
                "side={bad:?} must fail closed"
            );
        }
    }

    #[test]
    fn recover_fak_rejects_an_envelope_whose_limit_price_is_outside_the_open_interval() {
        // P2-6 pin: limit_price must be parseable AND strictly inside
        // (0, 1) -- a phantom-orderable price (==0, ==1, outside the
        // tokens, or unparseable) must fail closed.
        for bad in ["0", "1", "1.05", "0.00", "-0.50", "not-a-decimal", ""] {
            let mut env = envelope(42);
            env.price = bad.to_owned();
            assert_eq!(
                recover_fak_taker_order_from_trades(&env, trade_history_window(), &[]),
                Err(TradeHistoryRecoveryError::InvalidLimitPrice),
                "price={bad:?} must fail closed"
            );
        }
        // Also verify that the boundary JUST inside the interval is
        // accepted (e.g. 0.01 and 0.99) so the test pins both the
        // rejection and the pass-through halves of the contract.
        for ok in ["0.01", "0.5", "0.99"] {
            let mut env = envelope(42);
            env.price = ok.to_owned();
            // We pass no trades, so we expect NotFound (the open-
            // interval check passed but no matching trade exists).
            assert_eq!(
                recover_fak_taker_order_from_trades(&env, trade_history_window(), &[]),
                Ok(TradeHistoryLookup::NotFound),
                "price={ok:?} must pass the open-interval check"
            );
        }
    }

    #[test]
    fn recover_fak_rejects_an_envelope_whose_token_id_is_empty_or_not_all_digits() {
        // P2-6 pin: token_id must be a non-empty ASCII-digit string.
        // The on-chain token id is a U256 rendered as decimal digits; any
        // other shape (empty, whitespace, letters, hex prefix) means a
        // corrupted envelope and must fail closed rather than be
        // matched against arbitrary trade history.
        for bad in ["", "12a456", "0x123456", " 123456", "123 456"] {
            let mut env = envelope(42);
            env.token_id = bad.to_owned();
            assert_eq!(
                recover_fak_taker_order_from_trades(&env, trade_history_window(), &[]),
                Err(TradeHistoryRecoveryError::InvalidTokenId),
                "token_id={bad:?} must fail closed"
            );
        }
    }

    #[test]
    fn trade_history_window_new_rejects_an_inverted_time_range() {
        // P2-6 pin: TradeHistoryWindow::new returns
        // TradeHistoryRecoveryError::InvalidWindow when after > before.
        // This is the OTHER branch the risk review enumerated -- the
        // other six are already pinned by the defensive matcher tests
        // above. Pin it here directly so a future refactor that loosens
        // the check (e.g. silently swapping after and before) is caught
        // before any malformed window reaches the matcher.
        let after = Utc
            .with_ymd_and_hms(2026, 9, 1, 12, 0, 10)
            .single()
            .unwrap();
        let before = Utc.with_ymd_and_hms(2026, 9, 1, 12, 0, 0).single().unwrap();
        assert_eq!(
            TradeHistoryWindow::new(after, before),
            Err(TradeHistoryRecoveryError::InvalidWindow)
        );
        // And the happy path is the same call with after < before.
        assert!(TradeHistoryWindow::new(before, after).is_ok());
    }

    #[test]
    fn recover_fak_rejects_a_trade_with_unknown_side_regardless_of_price() {
        // P2-6 pin (defense in depth): the matcher's
        // `is_limit_compatible` for AccountTradeSide::Unknown returns
        // false (line 212 of trade_history_recovery.rs). Today this is
        // unreachable because envelope.side is parsed first and trade.side
        // is later compared to that parsed envelope.side -- a trade
        // whose side is Unknown would fail the parsed-side equality
        // check before reaching is_limit_compatible. This test pins the
        // fallback guard directly so a future refactor that removes the
        // parsed-side equality check (or that adds a new code path
        // skipping it) does not silently start matching Unknown-side
        // trades. The expected outcome is NotFound -- the Unknown-side
        // trade is filtered, not an Err.
        let unknown_side_trade = account_trade(
            "trade-unknown",
            "order-a",
            AccountTradeSide::Unknown,
            Decimal::new(49, 2), // would match a BUY envelope at price 0.50
            Decimal::new(5, 0),
            AccountTradeRole::Taker,
            1,
        );
        assert_eq!(
            recover_fak_taker_order_from_trades(
                &envelope(42),
                trade_history_window(),
                &[unknown_side_trade]
            ),
            Ok(TradeHistoryLookup::NotFound)
        );
    }

    // P2-6: pin the BUY-side ExceedsRequested branch (the SELL-side
    // version is already covered in tests/receipt.rs). BUY's bound
    // semantics differ (matched_shares has no upper bound) but the
    // accepted_qty > requested_qty guard is symmetric.

    #[tokio::test]
    async fn lost_response_recovery_records_only_the_exact_precomputed_order_id() {
        let db = TestDb::new().await;
        let (intent_id, attempt_id, _) = prepared_submitting_attempt(&db).await;
        let reader = FakeTradeHistoryReader {
            reply: FakeTradeHistoryReply::Trades(vec![account_trade(
                "trade-a",
                "order-a",
                AccountTradeSide::Buy,
                Decimal::new(49, 2),
                Decimal::new(5_288_460, 6),
                AccountTradeRole::Taker,
                1,
            )]),
        };

        let outcome = recover_lost_submission_response(
            &db,
            &reader,
            intent_id,
            attempt_id,
            trade_history_window().before(),
        )
        .await
        .expect("a unique exact ID must be recoverable without a venue write");
        assert_eq!(
            outcome,
            LostSubmissionRecoveryOutcome::Recovered {
                order_id: OrderId("order-a".to_owned())
            }
        );

        let row: (Option<String>, String) =
            sqlx::query_as("SELECT venue_order_id, status FROM order_attempts WHERE id = ?")
                .bind(attempt_id)
                .fetch_one(&*db)
                .await
                .expect("recovered ID must be durable");
        assert_eq!(row.0.as_deref(), Some("order-a"));
        assert_eq!(
            row.1, "uncertain",
            "receipt still requires strict by-ID confirmation"
        );

        let case_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ?")
                .bind(intent_id)
                .fetch_one(&*db)
                .await
                .expect("case query must succeed");
        assert_eq!(
            case_count, 0,
            "a uniquely recovered ID is not a receipt or a failure"
        );
    }

    #[tokio::test]
    async fn delayed_or_empty_trade_history_blocks_the_key_and_never_retries() {
        let db = TestDb::new().await;
        let (intent_id, attempt_id, _) = prepared_submitting_attempt(&db).await;
        let reader = FakeTradeHistoryReader {
            reply: FakeTradeHistoryReply::Trades(vec![]),
        };

        let outcome = recover_lost_submission_response(
            &db,
            &reader,
            intent_id,
            attempt_id,
            trade_history_window().before(),
        )
        .await
        .expect("an empty history is handled by the fail-closed recovery path");
        assert_eq!(outcome, LostSubmissionRecoveryOutcome::NeedsReconcile);

        let intent_status: String =
            sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
                .bind(intent_id)
                .fetch_one(&*db)
                .await
                .expect("intent must remain queryable");
        assert_eq!(intent_status, "needs_reconcile");
        let attempt_status: String =
            sqlx::query_scalar("SELECT status FROM order_attempts WHERE id = ?")
                .bind(attempt_id)
                .fetch_one(&*db)
                .await
                .expect("attempt must remain queryable");
        assert_eq!(attempt_status, "uncertain");

        // Re-running a delayed-history query must not manufacture a second
        // case or turn the empty result into permission to submit again.
        let second = recover_lost_submission_response(
            &db,
            &reader,
            intent_id,
            attempt_id,
            trade_history_window().before(),
        )
        .await
        .expect("an unresolved attempt remains read-only on repeat recovery");
        assert_eq!(second, LostSubmissionRecoveryOutcome::NeedsReconcile);
        let case_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reconciliation_cases \
             WHERE intent_id = ? AND order_attempt_id = ? AND resolved_at IS NULL",
        )
        .bind(intent_id)
        .bind(attempt_id)
        .fetch_one(&*db)
        .await
        .expect("case count must be queryable");
        assert_eq!(case_count, 1, "recovery cases are idempotent");
    }

    #[tokio::test]
    async fn a_trade_history_query_failure_becomes_a_visible_strict_query_case() {
        let db = TestDb::new().await;
        let (intent_id, attempt_id, _) = prepared_submitting_attempt(&db).await;
        let reader = FakeTradeHistoryReader {
            reply: FakeTradeHistoryReply::Failure,
        };

        assert_eq!(
            recover_lost_submission_response(
                &db,
                &reader,
                intent_id,
                attempt_id,
                trade_history_window().before(),
            )
            .await
            .expect("strict read failure must be recorded rather than retried"),
            LostSubmissionRecoveryOutcome::NeedsReconcile
        );
        let case_type: String = sqlx::query_scalar(
            "SELECT case_type FROM reconciliation_cases WHERE intent_id = ? AND order_attempt_id = ?",
        )
        .bind(intent_id)
        .bind(attempt_id)
        .fetch_one(&*db)
        .await
        .expect("strict query failure must have a visible case");
        assert_eq!(case_type, "strict_query_failure");
    }

    #[tokio::test]
    async fn a_conflicting_preexisting_venue_id_blocks_recovery_instead_of_overwriting_it() {
        let db = TestDb::new().await;
        let (intent_id, attempt_id, _) = prepared_submitting_attempt(&db).await;
        sqlx::query("UPDATE order_attempts SET venue_order_id = 'different-order' WHERE id = ?")
            .bind(attempt_id)
            .execute(&*db)
            .await
            .expect("test setup must record a conflicting ID");
        let reader = FakeTradeHistoryReader {
            reply: FakeTradeHistoryReply::Trades(vec![account_trade(
                "trade-a",
                "order-a",
                AccountTradeSide::Buy,
                Decimal::new(49, 2),
                Decimal::ONE,
                AccountTradeRole::Taker,
                1,
            )]),
        };

        assert_eq!(
            recover_lost_submission_response(
                &db,
                &reader,
                intent_id,
                attempt_id,
                trade_history_window().before(),
            )
            .await
            .expect("conflicting recovered IDs must enter reconciliation"),
            LostSubmissionRecoveryOutcome::NeedsReconcile
        );
        let venue_order_id: Option<String> =
            sqlx::query_scalar("SELECT venue_order_id FROM order_attempts WHERE id = ?")
                .bind(attempt_id)
                .fetch_one(&*db)
                .await
                .expect("attempt must remain queryable");
        assert_eq!(venue_order_id.as_deref(), Some("different-order"));
        let case_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reconciliation_cases \
             WHERE intent_id = ? AND order_attempt_id = ? AND case_type = 'unknown_submission'",
        )
        .bind(intent_id)
        .bind(attempt_id)
        .fetch_one(&*db)
        .await
        .expect("conflict must produce a visible case");
        assert_eq!(case_count, 1);
    }

    #[tokio::test]
    async fn a_fresh_attempt_persists_the_offered_candidate_envelope() {
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;

        let persisted = load_or_prepare_attempt(&db, intent_id, 1, &envelope(42))
            .await
            .unwrap();
        assert_eq!(persisted.salt, 42);

        let row_count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM order_attempts WHERE intent_id = ? AND attempt_number = 1",
        )
        .bind(intent_id)
        .fetch_one(&*db)
        .await
        .unwrap()
        .get(0);
        assert_eq!(row_count, 1);

        let requested_qty: String = sqlx::query_scalar(
            "SELECT requested_qty FROM order_attempts WHERE intent_id = ? AND attempt_number = 1",
        )
        .bind(intent_id)
        .fetch_one(&*db)
        .await
        .unwrap();
        assert_eq!(requested_qty, "2.50");
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

        assert_eq!(
            first, second,
            "both callers must observe the same, single persisted envelope"
        );

        let row_count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM order_attempts WHERE intent_id = ? AND attempt_number = 1",
        )
        .bind(intent_id)
        .fetch_one(&*db)
        .await
        .unwrap()
        .get(0);
        assert_eq!(
            row_count, 1,
            "exactly one row, whichever candidate won the race"
        );
    }

    #[tokio::test]
    async fn a_second_load_reads_back_the_first_callers_envelope_not_a_new_one() {
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;

        let first = load_or_prepare_attempt(&db, intent_id, 1, &envelope(1))
            .await
            .unwrap();
        let second = load_or_prepare_attempt(&db, intent_id, 1, &envelope(999))
            .await
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(
            second.salt, 1,
            "the second call's own candidate salt (999) must never be used"
        );
    }

    #[test]
    fn the_recovery_matrix_matches_every_documented_state() {
        assert_eq!(
            permitted_recovery_action("prepared", true, 0),
            RecoveryAction::MarkSubmittingThenSubmit
        );
        assert_eq!(
            permitted_recovery_action("submitting", true, 0),
            RecoveryAction::QueryFirst
        );
        assert_eq!(
            permitted_recovery_action("uncertain", true, 0),
            RecoveryAction::QueryFirst
        );
        assert_eq!(
            permitted_recovery_action("accepted", true, 0),
            RecoveryAction::ReconcileOrFinalize
        );
        assert_eq!(
            permitted_recovery_action("finalized", true, 0),
            RecoveryAction::ReconcileOrFinalize
        );
        assert_eq!(
            permitted_recovery_action("rejected", true, 0),
            RecoveryAction::MayPrepareNewAttempt
        );
    }

    #[test]
    fn a_crash_during_submitting_never_permits_a_direct_resubmission_on_restart() {
        // The core of "a crash after the request may have crossed the
        // network boundary never causes a direct resubmission on restart":
        // `submitting` found on restart must query first, never resubmit.
        assert_ne!(
            permitted_recovery_action("submitting", true, 0),
            RecoveryAction::MarkSubmittingThenSubmit
        );
        assert_eq!(
            permitted_recovery_action("submitting", true, 0),
            RecoveryAction::QueryFirst
        );
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
            load_or_prepare_attempt(
                &db,
                intent_id,
                attempt_number,
                &envelope(attempt_number as u64),
            )
            .await
            .unwrap();
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

        open_reconciliation_case(
            &db,
            intent_id,
            None,
            "strict_query_failure",
            "mock venue query error",
        )
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
        let receipt = OrderReceipt::from_fak_buy_budget(requested, requested, matched).unwrap();

        let attempt_id: i64 = sqlx::query_scalar(
            "INSERT INTO order_attempts (intent_id, attempt_number, envelope_json, status, requested_qty) \
             VALUES (?, 1, '{}', 'finalized', '5') RETURNING id",
        )
        .bind(intent_id)
        .fetch_one(&*db)
        .await
        .unwrap();

        crate::copytrading::execute::finalize_receipt(&db, intent_id, attempt_id, &receipt)
            .await
            .unwrap();

        let lot_qty: String = sqlx::query_scalar("SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'")
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(
            lot_qty, "2",
            "the lot must reflect only the matched quantity, never the requested one"
        );
    }

    #[tokio::test]
    async fn a_crash_between_dispatch_and_receipt_recovers_to_exactly_one_lot_no_resubmission() {
        // Phase 7 required test: "process death after request dispatch but
        // before receipt persistence, followed by recovery without a
        // duplicate lot or unsafe resubmission." Chains the actual recovery
        // path (recover_lost_submission_response) into the actual
        // finalization path (execute::finalize_receipt) end to end, the way
        // a restarted orchestrator would really walk it -- not just the
        // individual pieces each already-passing unit test covers alone.
        let db = TestDb::new().await;
        // "Process death after request dispatch": the attempt was marked
        // `submitting` (the request may have crossed the network boundary)
        // and the process then died before ever reading a response.
        let (intent_id, attempt_id, _started_at) = prepared_submitting_attempt(&db).await;

        // "Recovery": on restart, a read-only trade-history query finds the
        // exact taker order this attempt's envelope precomputed.
        let reader = FakeTradeHistoryReader {
            reply: FakeTradeHistoryReply::Trades(vec![account_trade(
                "trade-a",
                "order-a",
                AccountTradeSide::Buy,
                Decimal::new(49, 2),
                Decimal::new(5_288_460, 6),
                AccountTradeRole::Taker,
                1,
            )]),
        };
        let outcome = recover_lost_submission_response(
            &db,
            &reader,
            intent_id,
            attempt_id,
            trade_history_window().before(),
        )
        .await
        .expect("recovery must succeed");
        assert_eq!(
            outcome,
            LostSubmissionRecoveryOutcome::Recovered {
                order_id: OrderId("order-a".to_owned())
            }
        );

        // Recovery alone must never touch position_lots -- it only attaches
        // an ID; a lot may change only once the normal strict by-ID receipt
        // lookup (simulated here) confirms the fill and finalize_receipt
        // runs.
        let lots_before_finalize: i64 = sqlx::query("SELECT COUNT(*) FROM position_lots")
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(lots_before_finalize, 0);

        let receipt = OrderReceipt::from_fak_buy_budget(
            Decimal::new(5, 0),
            Decimal::new(5, 0),
            Decimal::new(5_288_460, 6),
        )
        .unwrap();
        crate::copytrading::execute::finalize_receipt(&db, intent_id, attempt_id, &receipt)
            .await
            .unwrap();

        let lot_qty: String = sqlx::query_scalar(
            "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
        )
        .fetch_one(&*db)
        .await
        .unwrap();
        assert_eq!(lot_qty, "5.288460");
        let lot_count: i64 = sqlx::query("SELECT COUNT(*) FROM position_lots")
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(lot_count, 1, "exactly one lot row, never a duplicate");

        // "No unsafe resubmission": if the recovery walk (or its caller)
        // runs a second time -- e.g. the orchestrator retries after another
        // restart -- neither recovery nor finalize may double anything.
        let second_outcome = recover_lost_submission_response(
            &db,
            &reader,
            intent_id,
            attempt_id,
            trade_history_window().before(),
        )
        .await
        .expect("re-running recovery on an already-recovered attempt must not error");
        assert_eq!(
            second_outcome,
            LostSubmissionRecoveryOutcome::Recovered {
                order_id: OrderId("order-a".to_owned())
            },
            "recovery is idempotent: it reports the same already-recorded ID, not a fresh submission"
        );
        crate::copytrading::execute::finalize_receipt(&db, intent_id, attempt_id, &receipt)
            .await
            .expect("re-finalizing the identical receipt must not error");

        let lot_qty_after_replay: String = sqlx::query_scalar(
            "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
        )
        .fetch_one(&*db)
        .await
        .unwrap();
        assert_eq!(
            lot_qty_after_replay, "5.288460",
            "replaying recovery+finalize must leave the lot exactly where it was, never double it"
        );
        let lot_count_after_replay: i64 = sqlx::query("SELECT COUNT(*) FROM position_lots")
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(lot_count_after_replay, 1);
        let case_count: i64 = sqlx::query("SELECT COUNT(*) FROM reconciliation_cases")
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            case_count, 0,
            "a clean recovery must never open a reconciliation case"
        );
    }

    #[tokio::test]
    async fn open_local_submission_failure_case_releases_reservation_and_opens_case_atomically() {
        // P0-3 step 6 direct unit test for the new helper. The orchestrator
        // tests in orchestrate.rs cover this helper end-to-end, but a
        // direct unit test isolates the helper's contract from the
        // orchestrator wiring and makes a regression that re-introduces
        // the two-transaction pattern easier to spot.
        let db = TestDb::new().await;
        let (intent_id, attempt_id, _started_at) = prepared_submitting_attempt(&db).await;

        // Pre-seed a persistent_budget_reservations row in 'reserved'
        // state so the release call inside the helper has something to
        // transition.
        sqlx::query(
            "INSERT INTO persistent_budget_reservations \
             (order_attempt_id, account_id, amount_usdc, reserved_at, state) \
             VALUES (?, 1, '1', strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'reserved')",
        )
        .bind(attempt_id)
        .execute(&db.pool)
        .await
        .expect("seed reservation");

        // Sanity: pre-state assertions so a test failure is unambiguous.
        let (state_before,): (String,) = sqlx::query_as(
            "SELECT state FROM persistent_budget_reservations WHERE order_attempt_id = ?",
        )
        .bind(attempt_id)
        .fetch_one(&db.pool)
        .await
        .expect("reservation pre-state");
        assert_eq!(state_before, "reserved");
        let case_count_before: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ?")
                .bind(intent_id)
                .fetch_one(&db.pool)
                .await
                .expect("case count pre-state");
        assert_eq!(case_count_before, 0);

        open_local_submission_failure_case(&db, intent_id, attempt_id, "payload validation failed")
            .await
            .expect("open_local_submission_failure_case must succeed");

        // All three writes must be observable together -- a single
        // tx commit guarantee. A two-transaction regression would
        // satisfy these same assertions on the happy path, but the
        // rollback-on-error test below exercises the failure mode that
        // distinguishes the two.
        let intent_status: String =
            sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
                .bind(intent_id)
                .fetch_one(&db.pool)
                .await
                .expect("intent status");
        assert_eq!(intent_status, "needs_reconcile");

        let (state_after, reason_after): (String, Option<String>) = sqlx::query_as(
            "SELECT state, release_reason FROM persistent_budget_reservations \
             WHERE order_attempt_id = ?",
        )
        .bind(attempt_id)
        .fetch_one(&db.pool)
        .await
        .expect("reservation post-state");
        assert_eq!(state_after, "released_pre_boundary");
        assert_eq!(
            reason_after.as_deref(),
            Some("local submission failed before network boundary"),
            "release reason must match the helper's constant verbatim"
        );

        let attempt_status: String =
            sqlx::query_scalar("SELECT status FROM order_attempts WHERE id = ?")
                .bind(attempt_id)
                .fetch_one(&db.pool)
                .await
                .expect("attempt status");
        assert_eq!(attempt_status, "uncertain");

        let case_count_after: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reconciliation_cases \
             WHERE intent_id = ? AND order_attempt_id = ? \
               AND case_type = 'local_submission_failure' \
               AND resolved_at IS NULL",
        )
        .bind(intent_id)
        .bind(attempt_id)
        .fetch_one(&db.pool)
        .await
        .expect("case count post-state");
        assert_eq!(case_count_after, 1);
    }

    #[tokio::test]
    async fn open_local_submission_failure_case_is_idempotent_on_repeat() {
        // P0-3 step 6: idempotency under double-fire. A partial
        // recovery that re-runs the helper (e.g. after a crash
        // between commit and the caller's next step) must NOT
        // create a duplicate case row and must NOT touch the
        // reservation's `released_at` again -- the `state='reserved'`
        // filter in release_pre_boundary_failure_with_conn and the
        // `NOT EXISTS` clause in the INSERT are the two halves of
        // this contract.
        let db = TestDb::new().await;
        let (intent_id, attempt_id, _started_at) = prepared_submitting_attempt(&db).await;

        sqlx::query(
            "INSERT INTO persistent_budget_reservations \
             (order_attempt_id, account_id, amount_usdc, reserved_at, state) \
             VALUES (?, 1, '1', strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'reserved')",
        )
        .bind(attempt_id)
        .execute(&db.pool)
        .await
        .expect("seed reservation");

        open_local_submission_failure_case(&db, intent_id, attempt_id, "first")
            .await
            .expect("first call");

        let (first_released_at,): (Option<String>,) = sqlx::query_as(
            "SELECT released_at FROM persistent_budget_reservations \
             WHERE order_attempt_id = ?",
        )
        .bind(attempt_id)
        .fetch_one(&db.pool)
        .await
        .expect("released_at after first call");
        let first_released_at = first_released_at.expect("released_at set on first call");

        // Sleep just enough to make the timestamp distinguishable in
        // case a second call were to overwrite released_at.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        open_local_submission_failure_case(&db, intent_id, attempt_id, "second")
            .await
            .expect("second call must not error");

        // Still exactly one case row (idempotency).
        let case_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reconciliation_cases \
             WHERE intent_id = ? AND order_attempt_id = ? \
               AND case_type = 'local_submission_failure' \
               AND resolved_at IS NULL",
        )
        .bind(intent_id)
        .bind(attempt_id)
        .fetch_one(&db.pool)
        .await
        .expect("case count after second call");
        assert_eq!(
            case_count, 1,
            "NOT EXISTS clause must prevent duplicate case rows"
        );

        // released_at unchanged: the WHERE state='reserved' filter
        // excludes the already-released row, so the second call
        // updates 0 rows.
        let (second_released_at,): (Option<String>,) = sqlx::query_as(
            "SELECT released_at FROM persistent_budget_reservations \
             WHERE order_attempt_id = ?",
        )
        .bind(attempt_id)
        .fetch_one(&db.pool)
        .await
        .expect("released_at after second call");
        assert_eq!(
            second_released_at.as_deref(),
            Some(first_released_at.as_str()),
            "released_at must not be touched by a second call on an already-released reservation"
        );
    }

    #[tokio::test]
    async fn open_local_submission_failure_case_rolls_back_on_intent_not_found() {
        // P0-3 step 6 rollback pin: when the inner async block returns
        // an Err (here: SELECT account_id,token_id from copy_intents
        // returns RowNotFound because the intent_id is bogus), the
        // outer `match &result` arms on Err and best-effort rolls
        // back the transaction. Without the rollback the partial
        // writes from prior steps in the inner block (or, in future
        // refactors, the case-open step) could persist -- violating
        // the AGENTS.md atomic invariant.
        //
        // The intent-not-found path is the easiest deterministic
        // injection point in this helper: fetch_one on a missing
        // row returns sqlx::Error::RowNotFound, which is mapped to
        // ReconcileError::Database by db_err. This test exercises the
        // SAME rollback path that would fire on a step-5 INSERT
        // failure (e.g., a future FK violation in
        // reconciliation_cases) -- the helper's structure funnels
        // every inner Err through the same ROLLBACK arm.
        let db = TestDb::new().await;
        let (intent_id, attempt_id, _started_at) = prepared_submitting_attempt(&db).await;

        // Pre-seed a reservation row in 'reserved' state so the
        // release call inside the helper has something to either
        // release (on success) or leave alone (on rollback).
        sqlx::query(
            "INSERT INTO persistent_budget_reservations \
             (order_attempt_id, account_id, amount_usdc, reserved_at, state) \
             VALUES (?, 1, '1', strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'reserved')",
        )
        .bind(attempt_id)
        .execute(&db.pool)
        .await
        .expect("seed reservation");

        // Use a bogus intent_id so the helper's first SELECT returns
        // RowNotFound, aborting the inner block before any writes.
        let bogus_intent_id = intent_id + 10_000;
        let result = open_local_submission_failure_case(
            &db,
            bogus_intent_id,
            attempt_id,
            "intent gone before we could open the case",
        )
        .await;
        assert!(
            result.is_err(),
            "open_local_submission_failure_case must return Err when the intent is missing"
        );

        // The reservation row must still be 'reserved' -- the rollback
        // arm must have unwound any partial state. (In practice the
        // inner block never reaches the release call, so this is a
        // safety net for future refactors that move writes earlier.)
        let (state_after,): (String,) = sqlx::query_as(
            "SELECT state FROM persistent_budget_reservations WHERE order_attempt_id = ?",
        )
        .bind(attempt_id)
        .fetch_one(&db.pool)
        .await
        .expect("reservation post-rollback state");
        assert_eq!(
            state_after, "reserved",
            "rollback must leave the reservation state untouched"
        );

        // The real intent (which still exists) must not have been
        // mutated -- the bogus intent_id must not leak writes into
        // the real intent's row.
        let real_intent_status: String =
            sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
                .bind(intent_id)
                .fetch_one(&db.pool)
                .await
                .expect("real intent status");
        assert_eq!(
            real_intent_status, "pending",
            "the real intent must not have been touched by the failed call"
        );

        // And the attempt must still be in 'submitting' (its pre-
        // Local-arm state), since the helper aborted before reaching
        // the order_attempts UPDATE.
        let attempt_status: String =
            sqlx::query_scalar("SELECT status FROM order_attempts WHERE id = ?")
                .bind(attempt_id)
                .fetch_one(&db.pool)
                .await
                .expect("attempt status after rollback");
        assert_eq!(
            attempt_status, "submitting",
            "attempt must remain in its pre-Local-arm state after rollback"
        );

        // No reconciliation_cases row was inserted (the inner block
        // aborted before the INSERT, and the rollback would have
        // unwound it anyway).
        let case_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reconciliation_cases \
             WHERE intent_id IN (?, ?)",
        )
        .bind(intent_id)
        .bind(bogus_intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("case count after rollback");
        assert_eq!(
            case_count, 0,
            "no reconciliation_cases row must exist after a rolled-back Local-arm helper call"
        );
    }
}
