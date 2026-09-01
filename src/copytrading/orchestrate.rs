//! Walks one intent through the Phase 5 recovery matrix using already-tested
//! claim/size/prepare/submit primitives. Tests use fakes; they never contact
//! the venue.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::SqlitePool;

use crate::{
    copytrading::{
        execute::{
            cancel_expired_intent, claim_or_resume_intent, finalize_receipt, next_attempt_number,
            open_reconciliation_case as open_execute_case, size_and_reserve, ClaimedIntent,
            ExecuteError, SizedDecision, SizingOutcome,
        },
        reconcile::{
            attempts_in_window, load_or_prepare_attempt, mark_attempt_rejected,
            mark_attempt_submitting, mark_attempt_uncertain_after_submission_error,
            open_reconciliation_case, permitted_recovery_action, recover_lost_submission_response,
            CopyExecution, LostSubmissionRecoveryOutcome, PreparedOrderEnvelope, ReconcileError,
            RecoveryAction, SubmitError,
        },
    },
    venue::{
        intl_clob::{StrictAccountBalanceReader, StrictTradeHistoryReader},
        intl_clob_exec::receipt_from_submitted_envelope,
        OrderReceipt,
    },
};

/// Operator-facing live-run guard. Only the exact value `yes` enables venue
/// writes, matching the canary probe.
pub fn live_execute_enabled(value: Option<&str>) -> bool {
    value == Some("yes")
}

/// Builds a `PreparedOrderEnvelope` from a persisted decision. A live
/// implementation signs once; tests inject a deterministic fake.
pub trait EnvelopeFactory {
    fn prepare(
        &self,
        decision: &SizedDecision,
    ) -> impl std::future::Future<Output = Result<PreparedOrderEnvelope, String>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrchestrateOutcome {
    Filled { filled_qty: Decimal },
    Uncertain,
    NeedsReconcile(&'static str),
    Expired,
    Rejected,
    Blocked(&'static str),
    NotClaimed,
}

#[derive(Debug)]
pub enum OrchestrateError {
    Execute(ExecuteError),
    Reconcile(ReconcileError),
    Prepare(String),
    Submit(SubmitError),
    Receipt(String),
}

impl std::fmt::Display for OrchestrateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Execute(error) => write!(formatter, "{error}"),
            Self::Reconcile(error) => write!(formatter, "{error}"),
            Self::Prepare(error) => write!(formatter, "envelope prepare failed: {error}"),
            Self::Submit(error) => write!(formatter, "{error}"),
            Self::Receipt(error) => write!(formatter, "receipt mapping failed: {error}"),
        }
    }
}

impl std::error::Error for OrchestrateError {}

impl From<ExecuteError> for OrchestrateError {
    fn from(error: ExecuteError) -> Self {
        Self::Execute(error)
    }
}

impl From<ReconcileError> for OrchestrateError {
    fn from(error: ReconcileError) -> Self {
        Self::Reconcile(error)
    }
}

pub async fn list_runnable_intents(
    pool: &SqlitePool,
    account_id: i64,
) -> Result<Vec<i64>, OrchestrateError> {
    sqlx::query_scalar(
        "SELECT ci.id FROM copy_intents ci \
         WHERE ci.account_id = ? AND ci.status IN ('pending', 'in_progress') \
           AND NOT EXISTS ( \
               SELECT 1 FROM copy_intents blocked \
               WHERE blocked.account_id = ci.account_id \
                 AND blocked.token_id = ci.token_id \
                 AND blocked.status = 'needs_reconcile' \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM reconciliation_cases rc \
               WHERE rc.account_id = ci.account_id \
                 AND rc.token_id = ci.token_id \
                 AND rc.resolved_at IS NULL \
           ) \
         ORDER BY token_id, id",
    )
    .bind(account_id)
    .fetch_all(pool)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))
}

struct AttemptRow {
    id: i64,
    status: String,
    envelope: PreparedOrderEnvelope,
}

/// Claims, sizes, prepares (at most once per attempt), and walks the
/// submission recovery matrix for one intent.
pub async fn execute_one_intent<B, E, F, H>(
    pool: &SqlitePool,
    balance_reader: &B,
    execution: &E,
    envelopes: &F,
    trade_history: &H,
    intent_id: i64,
    now: DateTime<Utc>,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    B: StrictAccountBalanceReader,
    E: CopyExecution,
    F: EnvelopeFactory,
    H: StrictTradeHistoryReader,
{
    if token_has_open_reconciliation_lock(pool, intent_id).await? {
        return Ok(OrchestrateOutcome::Blocked(
            "account/token needs reconciliation",
        ));
    }

    let Some(claimed) = claim_or_resume_intent(pool, intent_id).await? else {
        return Ok(OrchestrateOutcome::NotClaimed);
    };

    if let Some(attempt) = load_latest_attempt(pool, intent_id).await? {
        return walk_existing_attempt(
            pool,
            balance_reader,
            execution,
            envelopes,
            trade_history,
            &claimed,
            attempt,
            now,
        )
        .await;
    }

    let decision = match size_and_reserve(pool, balance_reader, &claimed).await? {
        SizingOutcome::Decision(decision) => decision,
        SizingOutcome::NeedsReconcile(reason) => {
            open_execute_case(pool, &claimed, reason).await?;
            return Ok(OrchestrateOutcome::NeedsReconcile(reason));
        }
        SizingOutcome::Expired => {
            cancel_expired_intent(pool, claimed.intent_id).await?;
            return Ok(OrchestrateOutcome::Expired);
        }
    };

    let envelope = envelopes
        .prepare(&decision)
        .await
        .map_err(OrchestrateError::Prepare)?;
    let attempt_number = next_attempt_number(pool, intent_id).await?;
    load_or_prepare_attempt(pool, intent_id, attempt_number, &envelope).await?;
    let attempt = load_latest_attempt(pool, intent_id).await?.ok_or_else(|| {
        OrchestrateError::Execute(ExecuteError::Database(
            "missing attempt after prepare".into(),
        ))
    })?;
    persist_expected_venue_order_id(pool, intent_id, attempt.id, &attempt.envelope).await?;
    submit_prepared(pool, execution, intent_id, attempt, now).await
}

#[allow(clippy::too_many_arguments)]
async fn walk_existing_attempt<B, E, F, H>(
    pool: &SqlitePool,
    balance_reader: &B,
    execution: &E,
    envelopes: &F,
    trade_history: &H,
    claimed: &ClaimedIntent,
    attempt: AttemptRow,
    now: DateTime<Utc>,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    B: StrictAccountBalanceReader,
    E: CopyExecution,
    F: EnvelopeFactory,
    H: StrictTradeHistoryReader,
{
    let window_count = attempts_in_window(pool, claimed.intent_id).await?;
    match permitted_recovery_action(&attempt.status, true, window_count) {
        RecoveryAction::MarkSubmittingThenSubmit => {
            submit_prepared(pool, execution, claimed.intent_id, attempt, now).await
        }
        RecoveryAction::QueryFirst => {
            query_first(
                pool,
                execution,
                trade_history,
                claimed.intent_id,
                attempt,
                now,
            )
            .await
        }
        RecoveryAction::ReconcileOrFinalize => {
            reconcile_or_finalize(pool, execution, claimed.intent_id, attempt).await
        }
        RecoveryAction::MayPrepareNewAttempt => {
            prepare_new_attempt(pool, balance_reader, envelopes, claimed).await?;
            let attempt = load_latest_attempt(pool, claimed.intent_id)
                .await?
                .ok_or_else(|| {
                    OrchestrateError::Execute(ExecuteError::Database(
                        "missing attempt after retry prepare".into(),
                    ))
                })?;
            persist_expected_venue_order_id(pool, claimed.intent_id, attempt.id, &attempt.envelope)
                .await?;
            submit_prepared(pool, execution, claimed.intent_id, attempt, now).await
        }
        RecoveryAction::Blocked(reason) => {
            open_reconciliation_case(
                pool,
                claimed.intent_id,
                Some(attempt.id),
                "blocked_recovery",
                reason,
            )
            .await?;
            Ok(OrchestrateOutcome::Blocked(reason))
        }
    }
}

async fn prepare_new_attempt<B, F>(
    pool: &SqlitePool,
    balance_reader: &B,
    envelopes: &F,
    claimed: &ClaimedIntent,
) -> Result<(), OrchestrateError>
where
    B: StrictAccountBalanceReader,
    F: EnvelopeFactory,
{
    let decision = match size_and_reserve(pool, balance_reader, claimed).await? {
        SizingOutcome::Decision(decision) => decision,
        SizingOutcome::NeedsReconcile(reason) => {
            open_execute_case(pool, claimed, reason).await?;
            return Err(OrchestrateError::Execute(ExecuteError::Database(
                reason.to_owned(),
            )));
        }
        SizingOutcome::Expired => {
            cancel_expired_intent(pool, claimed.intent_id).await?;
            return Err(OrchestrateError::Execute(ExecuteError::Database(
                "intent expired before retry prepare".into(),
            )));
        }
    };
    let envelope = envelopes
        .prepare(&decision)
        .await
        .map_err(OrchestrateError::Prepare)?;
    let attempt_number = next_attempt_number(pool, claimed.intent_id).await?;
    load_or_prepare_attempt(pool, claimed.intent_id, attempt_number, &envelope).await?;
    Ok(())
}

async fn submit_prepared<E>(
    pool: &SqlitePool,
    execution: &E,
    intent_id: i64,
    attempt: AttemptRow,
    now: DateTime<Utc>,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    E: CopyExecution,
{
    mark_attempt_submitting(pool, intent_id, attempt.id, now).await?;
    match execution.submit_exact_envelope(&attempt.envelope).await {
        Ok(receipt) => {
            mark_attempt_accepted(pool, intent_id, attempt.id).await?;
            finalize_receipt(pool, intent_id, attempt.id, &receipt).await?;
            mark_attempt_finalized(pool, intent_id, attempt.id).await?;
            Ok(OrchestrateOutcome::Filled {
                filled_qty: receipt.filled_qty(),
            })
        }
        Err(SubmitError::Transport(detail)) => {
            mark_attempt_uncertain_after_submission_error(pool, intent_id, attempt.id, &detail)
                .await?;
            Ok(OrchestrateOutcome::Uncertain)
        }
        Err(SubmitError::Rejected(detail)) => {
            mark_attempt_rejected(pool, intent_id, attempt.id, &detail).await?;
            Ok(OrchestrateOutcome::Rejected)
        }
        Err(SubmitError::Local(detail)) => {
            open_reconciliation_case(
                pool,
                intent_id,
                Some(attempt.id),
                "local_submission_failure",
                &detail,
            )
            .await?;
            Ok(OrchestrateOutcome::NeedsReconcile(
                "local submission failure",
            ))
        }
    }
}

async fn query_first<E, H>(
    pool: &SqlitePool,
    execution: &E,
    trade_history: &H,
    intent_id: i64,
    attempt: AttemptRow,
    now: DateTime<Utc>,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    E: CopyExecution,
    H: StrictTradeHistoryReader,
{
    match recover_lost_submission_response(pool, trade_history, intent_id, attempt.id, now).await? {
        LostSubmissionRecoveryOutcome::NeedsReconcile => {
            Ok(OrchestrateOutcome::NeedsReconcile("lost submission"))
        }
        LostSubmissionRecoveryOutcome::Recovered { order_id } => {
            let state = match execution.order_for_receipt(&order_id).await {
                Ok(state) => state,
                Err(detail) => {
                    open_reconciliation_case(
                        pool,
                        intent_id,
                        Some(attempt.id),
                        "strict_query_failure",
                        &detail,
                    )
                    .await?;
                    return Ok(OrchestrateOutcome::NeedsReconcile(
                        "strict order lookup failed",
                    ));
                }
            };
            let receipt = match receipt_from_terminal_order_state(&attempt.envelope, &state) {
                Ok(receipt) => receipt,
                Err(detail) => {
                    open_reconciliation_case(
                        pool,
                        intent_id,
                        Some(attempt.id),
                        "unknown_submission",
                        &detail,
                    )
                    .await?;
                    return Ok(OrchestrateOutcome::NeedsReconcile(
                        "strict order state not terminal",
                    ));
                }
            };
            mark_attempt_accepted(pool, intent_id, attempt.id).await?;
            finalize_receipt(pool, intent_id, attempt.id, &receipt).await?;
            mark_attempt_finalized(pool, intent_id, attempt.id).await?;
            Ok(OrchestrateOutcome::Filled {
                filled_qty: receipt.filled_qty(),
            })
        }
    }
}

async fn reconcile_or_finalize<E>(
    pool: &SqlitePool,
    execution: &E,
    intent_id: i64,
    attempt: AttemptRow,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    E: CopyExecution,
{
    if let Some(receipt) = execution
        .query_prepared_envelope(&attempt.envelope)
        .await
        .map_err(|detail| OrchestrateError::Submit(SubmitError::Local(detail)))?
    {
        finalize_receipt(pool, intent_id, attempt.id, &receipt).await?;
        mark_attempt_finalized(pool, intent_id, attempt.id).await?;
        return Ok(OrchestrateOutcome::Filled {
            filled_qty: receipt.filled_qty(),
        });
    }
    open_reconciliation_case(
        pool,
        intent_id,
        Some(attempt.id),
        "unknown_submission",
        "accepted attempt has no recoverable receipt",
    )
    .await?;
    Ok(OrchestrateOutcome::NeedsReconcile(
        "accepted without receipt",
    ))
}

fn receipt_from_terminal_order_state(
    envelope: &PreparedOrderEnvelope,
    state: &crate::copytrading::reconcile::VenueOrderState,
) -> Result<OrderReceipt, String> {
    if !is_terminal_filled_order_status(&state.status) {
        return Err(format!(
            "strict order lookup returned non-terminal status {} for {}",
            state.status, state.order_id.0
        ));
    }
    if state.size_matched <= Decimal::ZERO {
        return Err(format!(
            "strict order lookup returned terminal status {} but zero matched size for {}",
            state.status, state.order_id.0
        ));
    }
    receipt_from_submitted_envelope(envelope, state.size_matched, state.size_matched)
}

fn is_terminal_filled_order_status(status: &str) -> bool {
    let normalized = status.trim().to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "matched" | "filled" | "confirmed" | "mined"
    )
}

async fn token_has_open_reconciliation_lock(
    pool: &SqlitePool,
    intent_id: i64,
) -> Result<bool, OrchestrateError> {
    let locked: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM copy_intents ci \
             WHERE ci.id = ? \
               AND ( \
                   EXISTS ( \
                       SELECT 1 FROM copy_intents blocked \
                       WHERE blocked.account_id = ci.account_id \
                         AND blocked.token_id = ci.token_id \
                         AND blocked.status = 'needs_reconcile' \
                         AND blocked.id <> ci.id \
                   ) \
                   OR EXISTS ( \
                       SELECT 1 FROM reconciliation_cases rc \
                       WHERE rc.account_id = ci.account_id \
                         AND rc.token_id = ci.token_id \
                         AND rc.resolved_at IS NULL \
                         AND (rc.intent_id IS NULL OR rc.intent_id <> ci.id) \
                   ) \
               ) \
         )",
    )
    .bind(intent_id)
    .fetch_one(pool)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    Ok(locked != 0)
}

async fn load_latest_attempt(
    pool: &SqlitePool,
    intent_id: i64,
) -> Result<Option<AttemptRow>, OrchestrateError> {
    let row: Option<(i64, String, String)> = sqlx::query_as(
        "SELECT id, status, envelope_json \
         FROM order_attempts WHERE intent_id = ? ORDER BY attempt_number DESC LIMIT 1",
    )
    .bind(intent_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    let Some((id, status, envelope_json)) = row else {
        return Ok(None);
    };
    let envelope = serde_json::from_str(&envelope_json)
        .map_err(|_| OrchestrateError::Reconcile(ReconcileError::InvalidEnvelope))?;
    Ok(Some(AttemptRow {
        id,
        status,
        envelope,
    }))
}

async fn persist_expected_venue_order_id(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_id: i64,
    envelope: &PreparedOrderEnvelope,
) -> Result<(), OrchestrateError> {
    let result = sqlx::query(
        "UPDATE order_attempts SET venue_order_id = ? \
         WHERE id = ? AND intent_id = ? AND (venue_order_id IS NULL OR venue_order_id = ?)",
    )
    .bind(&envelope.expected_taker_order_id)
    .bind(attempt_id)
    .bind(intent_id)
    .bind(&envelope.expected_taker_order_id)
    .execute(pool)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    if result.rows_affected() != 1 {
        return Err(OrchestrateError::Reconcile(
            ReconcileError::ConflictingRecoveredOrderId,
        ));
    }
    Ok(())
}

async fn mark_attempt_accepted(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_id: i64,
) -> Result<(), OrchestrateError> {
    let result = sqlx::query(
        "UPDATE order_attempts SET status = 'accepted', \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND intent_id = ? AND status IN ('submitting', 'accepted')",
    )
    .bind(attempt_id)
    .bind(intent_id)
    .execute(pool)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    if result.rows_affected() != 1 {
        return Err(OrchestrateError::Reconcile(
            ReconcileError::InvalidAttemptTransition,
        ));
    }
    Ok(())
}

async fn mark_attempt_finalized(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_id: i64,
) -> Result<(), OrchestrateError> {
    sqlx::query(
        "UPDATE order_attempts SET status = 'finalized', \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND intent_id = ? AND status IN ('accepted', 'finalized', 'submitting')",
    )
    .bind(attempt_id)
    .bind(intent_id)
    .execute(pool)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    Ok(())
}

#[allow(clippy::manual_async_fn)]
impl EnvelopeFactory for crate::venue::intl_clob_exec::IntlClobCopyAdapter {
    fn prepare(
        &self,
        decision: &SizedDecision,
    ) -> impl std::future::Future<Output = Result<PreparedOrderEnvelope, String>> + Send {
        let client = self.client().clone();
        let signer = self.signer().clone();
        let decision = decision.clone();
        async move {
            let preparer = crate::copytrading::prepare::EnvelopePreparer {
                client: &client,
                signer: &signer,
            };
            preparer
                .prepare(&decision)
                .await
                .map(|prepared| prepared.envelope)
                .map_err(|error| error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    };

    use async_trait::async_trait;
    use chrono::Utc;
    use rust_decimal::Decimal;

    use super::*;
    use crate::venue::intl_clob::StrictTokenBalanceReader;
    use crate::{
        copytrading::{db::open_and_migrate, plan::PolicySnapshot, reconcile::OrderId},
        venue::{
            intl_clob::{
                AccountTrade, OutcomeTokenId, StrictCollateralError, StrictPositionError,
                StrictTradeHistoryError,
            },
            OrderReceipt,
        },
    };

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
                "polycopy-engine-orchestrate-test-{}-{nonce}-{counter}.sqlite",
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

    struct FixedBalance(Decimal);

    #[async_trait]
    impl StrictTokenBalanceReader for FixedBalance {
        async fn position_for_token_strict(
            &self,
            _token_id: &OutcomeTokenId,
        ) -> Result<Decimal, StrictPositionError> {
            Ok(self.0)
        }
    }

    #[async_trait]
    impl StrictAccountBalanceReader for FixedBalance {
        async fn collateral_balance_strict(&self) -> Result<Decimal, StrictCollateralError> {
            Ok(Decimal::new(100, 0))
        }

        async fn collateral_allowance_strict(&self) -> Result<Decimal, StrictCollateralError> {
            Ok(Decimal::new(100, 0))
        }
    }

    struct EmptyHistory;

    #[async_trait]
    impl StrictTradeHistoryReader for EmptyHistory {
        async fn trades_for_token_between(
            &self,
            _token_id: &OutcomeTokenId,
            _after: DateTime<Utc>,
            _before: DateTime<Utc>,
        ) -> Result<Vec<AccountTrade>, StrictTradeHistoryError> {
            Ok(Vec::new())
        }
    }

    struct RecoveredHistory;

    #[async_trait]
    impl StrictTradeHistoryReader for RecoveredHistory {
        async fn trades_for_token_between(
            &self,
            token_id: &OutcomeTokenId,
            _after: DateTime<Utc>,
            _before: DateTime<Utc>,
        ) -> Result<Vec<AccountTrade>, StrictTradeHistoryError> {
            Ok(vec![AccountTrade {
                trade_id: "trade-1".to_owned(),
                taker_order_id: "0xdead0".to_owned(),
                token_id: token_id.clone(),
                side: crate::venue::intl_clob::AccountTradeSide::Buy,
                price: Decimal::new(55, 2),
                size: Decimal::new(5, 0),
                // The recovery query's upper bound is captured immediately
                // before this fake is called. Keep the fixture inside that
                // closed window instead of racing a freshly-created timestamp
                // against it.
                match_time: Utc::now() - chrono::Duration::seconds(1),
                role: crate::venue::intl_clob::AccountTradeRole::Taker,
                status: crate::venue::intl_clob::AccountTradeStatus::Matched,
            }])
        }
    }

    struct FakeVenue {
        prepare_count: AtomicU64,
        submit_count: AtomicU64,
        submit_result: Mutex<Result<OrderReceipt, SubmitError>>,
        order_status: Mutex<String>,
        last_salt: Mutex<Option<u64>>,
    }

    impl FakeVenue {
        fn succeeding(filled: Decimal) -> Self {
            Self {
                prepare_count: AtomicU64::new(0),
                submit_count: AtomicU64::new(0),
                submit_result: Mutex::new(Ok(OrderReceipt::from_fak_buy_budget(
                    Decimal::new(5, 0),
                    Decimal::new(5, 0),
                    filled,
                )
                .expect("receipt"))),
                order_status: Mutex::new("MATCHED".to_owned()),
                last_salt: Mutex::new(None),
            }
        }

        fn transport_error() -> Self {
            Self {
                prepare_count: AtomicU64::new(0),
                submit_count: AtomicU64::new(0),
                submit_result: Mutex::new(Err(SubmitError::Transport("connection reset".into()))),
                order_status: Mutex::new("MATCHED".to_owned()),
                last_salt: Mutex::new(None),
            }
        }

        fn set_order_status(&self, status: &str) {
            *self.order_status.lock().expect("order status lock") = status.to_owned();
        }
    }

    impl EnvelopeFactory for FakeVenue {
        #[allow(clippy::manual_async_fn)]
        fn prepare(
            &self,
            decision: &SizedDecision,
        ) -> impl std::future::Future<Output = Result<PreparedOrderEnvelope, String>> + Send
        {
            let count = self.prepare_count.fetch_add(1, Ordering::SeqCst);
            let envelope = PreparedOrderEnvelope {
                token_id: decision.token_id.clone(),
                side: decision.side.as_str().to_owned(),
                price: decision.limit_price.to_string(),
                size: decision.qty.to_string(),
                salt: 1000 + count,
                order_type: "FAK".to_owned(),
                expected_taker_order_id: format!("0xdead{count}"),
                signed_order_json: r#"{"order":{}}"#.to_owned(),
            };
            async move { Ok(envelope) }
        }
    }

    impl CopyExecution for FakeVenue {
        #[allow(clippy::manual_async_fn)]
        fn position_for_token_strict(
            &self,
            _token_id: &str,
        ) -> impl std::future::Future<Output = Result<Decimal, String>> + Send {
            async { Ok(Decimal::ZERO) }
        }

        #[allow(clippy::manual_async_fn)]
        fn order_for_receipt(
            &self,
            order_id: &OrderId,
        ) -> impl std::future::Future<
            Output = Result<crate::copytrading::reconcile::VenueOrderState, String>,
        > + Send {
            let order_id = order_id.clone();
            let status = self.order_status.lock().expect("order status lock").clone();
            async move {
                Ok(crate::copytrading::reconcile::VenueOrderState {
                    order_id,
                    status,
                    size_matched: Decimal::new(5, 0),
                })
            }
        }

        #[allow(clippy::manual_async_fn)]
        fn query_prepared_envelope(
            &self,
            _envelope: &PreparedOrderEnvelope,
        ) -> impl std::future::Future<Output = Result<Option<OrderReceipt>, String>> + Send
        {
            async { Ok(None) }
        }

        #[allow(clippy::manual_async_fn)]
        fn submit_exact_envelope(
            &self,
            envelope: &PreparedOrderEnvelope,
        ) -> impl std::future::Future<Output = Result<OrderReceipt, SubmitError>> + Send {
            self.submit_count.fetch_add(1, Ordering::SeqCst);
            *self.last_salt.lock().expect("salt lock") = Some(envelope.salt);
            let result = self
                .submit_result
                .lock()
                .expect("submit result lock")
                .clone();
            async move { result }
        }
    }

    async fn seed_account_and_schedule(db: &TestDb) {
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&db.pool)
        .await
        .expect("account must insert");
        sqlx::query(
            "INSERT INTO execution_schedule (id, shard_scheme_version, shard_algorithm, lane_count) \
             VALUES (1, 1, 'hash_mod_lane_count', 1)",
        )
        .execute(&db.pool)
        .await
        .expect("execution_schedule must insert");
    }

    async fn seed_leader(db: &TestDb, leader_id: i64) {
        sqlx::query("INSERT INTO leader_config (id, label, enabled) VALUES (?, ?, 1)")
            .bind(leader_id)
            .bind(format!("leader-{leader_id}"))
            .execute(&db.pool)
            .await
            .expect("leader must insert");
    }

    async fn seed_pending_buy(db: &TestDb) -> i64 {
        seed_pending_buy_with_event_key(db, "activity:1:tok:BUY:5:1").await
    }

    async fn seed_pending_buy_with_event_key(db: &TestDb, event_key: &str) -> i64 {
        let event_id: i64 = sqlx::query_scalar(
            "INSERT INTO leader_events \
             (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, price, occurred_at, observed_at) \
             VALUES (?, 1, '0xcond', '123456', 0, 'BUY', '5', '0.55', \
              strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now')) RETURNING id",
        )
        .bind(event_key)
        .fetch_one(&db.pool)
        .await
        .expect("event");
        let snapshot = PolicySnapshot {
            max_signal_age_seconds: 3600,
            decision_window_seconds: 300,
            price_tolerance_bps: 0,
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            max_order_notional: "100000".to_owned(),
            min_leader_trade_size: "0".to_owned(),
        };
        sqlx::query_scalar(
            "INSERT INTO copy_intents \
             (event_id, account_id, leader_id, token_id, side, config_snapshot_json, config_snapshot_hash, \
              shard_scheme_version, lane_count, shard_id, status, decision_deadline_at) \
             VALUES (?, 1, 1, '123456', 'BUY', ?, 'hash', 1, 1, 0, 'pending', ?) RETURNING id",
        )
        .bind(event_id)
        .bind(serde_json::to_string(&snapshot).unwrap())
        .bind((Utc::now() + chrono::Duration::seconds(300)).to_rfc3339())
        .fetch_one(&db.pool)
        .await
        .expect("intent")
    }

    async fn attempt_status(db: &TestDb, intent_id: i64) -> String {
        sqlx::query_scalar("SELECT status FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .expect("attempt status")
    }

    #[test]
    fn live_execute_guard_requires_exact_yes() {
        assert!(!live_execute_enabled(None));
        assert!(!live_execute_enabled(Some("YES")));
        assert!(!live_execute_enabled(Some("true")));
        assert!(live_execute_enabled(Some("yes")));
    }

    #[tokio::test]
    async fn a_crash_after_prepare_does_not_rebuild_the_envelope() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent_id = seed_pending_buy(&db).await;
        let venue = FakeVenue::succeeding(Decimal::new(528846, 5));
        let first = execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("first pass");
        assert!(matches!(first, OrchestrateOutcome::Filled { .. }));
        assert_eq!(venue.prepare_count.load(Ordering::SeqCst), 1);
        assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);

        let db2 = TestDb::new().await;
        seed_account_and_schedule(&db2).await;
        seed_leader(&db2, 1).await;
        let intent_id = seed_pending_buy(&db2).await;
        let venue = FakeVenue::succeeding(Decimal::new(528846, 5));
        let claimed = claim_or_resume_intent(&db2, intent_id)
            .await
            .unwrap()
            .unwrap();
        let decision = match size_and_reserve(&db2, &FixedBalance(Decimal::new(100, 0)), &claimed)
            .await
            .unwrap()
        {
            SizingOutcome::Decision(decision) => decision,
            _ => panic!("expected a persisted sizing decision"),
        };
        let envelope = venue.prepare(&decision).await.unwrap();
        load_or_prepare_attempt(&db2, intent_id, 1, &envelope)
            .await
            .unwrap();
        assert_eq!(venue.prepare_count.load(Ordering::SeqCst), 1);

        let outcome = execute_one_intent(
            &db2,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("resume");
        assert!(matches!(outcome, OrchestrateOutcome::Filled { .. }));
        assert_eq!(
            venue.prepare_count.load(Ordering::SeqCst),
            1,
            "resume must not sign a second envelope"
        );
        assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
        assert_eq!(*venue.last_salt.lock().unwrap(), Some(1000));
    }

    #[tokio::test]
    async fn a_transport_error_marks_uncertain_and_never_resubmits() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent_id = seed_pending_buy(&db).await;
        let venue = FakeVenue::transport_error();
        let first = execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("transport pass");
        assert_eq!(first, OrchestrateOutcome::Uncertain);
        assert_eq!(attempt_status(&db, intent_id).await, "uncertain");
        assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);

        let second = execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("query-first pass");
        assert_eq!(
            second,
            OrchestrateOutcome::NeedsReconcile("lost submission")
        );
        assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
        assert_eq!(venue.prepare_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn open_reconciliation_for_account_token_excludes_and_blocks_later_intents() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let blocked_intent =
            seed_pending_buy_with_event_key(&db, "activity:1:tok:BUY:5:blocked").await;
        crate::copytrading::reconcile::open_reconciliation_case(
            &db,
            blocked_intent,
            None,
            "unknown_submission",
            "manual unresolved canary",
        )
        .await
        .expect("reconciliation case");
        let later_intent = seed_pending_buy_with_event_key(&db, "activity:1:tok:BUY:5:later").await;

        let runnable = list_runnable_intents(&db, 1).await.expect("runnable");
        assert!(
            !runnable.contains(&later_intent),
            "a later same-token intent must not be runnable while an open case exists"
        );

        let venue = FakeVenue::succeeding(Decimal::new(5, 0));
        let outcome = execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            later_intent,
            Utc::now(),
        )
        .await
        .expect("direct execute must fail closed");
        assert_eq!(
            outcome,
            OrchestrateOutcome::Blocked("account/token needs reconciliation")
        );
        assert_eq!(venue.submit_count.load(Ordering::SeqCst), 0);
        let later_status: String =
            sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
                .bind(later_intent)
                .fetch_one(&db.pool)
                .await
                .expect("later status");
        assert_eq!(later_status, "pending");
    }

    #[tokio::test]
    async fn recovered_order_id_with_non_terminal_order_state_opens_reconciliation() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent_id = seed_pending_buy(&db).await;
        let venue = FakeVenue::transport_error();

        let first = execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("transport pass");
        assert_eq!(first, OrchestrateOutcome::Uncertain);
        venue.set_order_status("LIVE");

        let recovered = execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &RecoveredHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("recovery pass");
        assert_eq!(
            recovered,
            OrchestrateOutcome::NeedsReconcile("strict order state not terminal")
        );
        assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);

        let intent_status: String =
            sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
                .bind(intent_id)
                .fetch_one(&db.pool)
                .await
                .expect("intent status");
        assert_eq!(intent_status, "needs_reconcile");
        let case_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reconciliation_cases \
             WHERE intent_id = ? AND case_type = 'unknown_submission' AND resolved_at IS NULL",
        )
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("case count");
        assert_eq!(case_count, 1);
        let lot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots")
            .fetch_one(&db.pool)
            .await
            .expect("lot count");
        assert_eq!(lot_count, 0);
    }

    #[tokio::test]
    async fn a_buy_fill_uses_the_receipt_filled_qty_not_the_requested_size() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent_id = seed_pending_buy(&db).await;
        let filled = Decimal::new(528846, 5);
        let venue = FakeVenue::succeeding(filled);
        let outcome = execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("fill");
        assert_eq!(outcome, OrchestrateOutcome::Filled { filled_qty: filled });
        let lot: String = sqlx::query_scalar(
            "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
        )
        .fetch_one(&db.pool)
        .await
        .expect("lot");
        assert_eq!(lot.parse::<Decimal>().unwrap(), filled);
        assert_ne!(lot.parse::<Decimal>().unwrap(), Decimal::new(5, 0));
    }
}
