//! Walks one intent through the Phase 5 recovery matrix using already-tested
//! claim/size/prepare/submit primitives. Tests use fakes; they never contact
//! the venue.

use chrono::{DateTime, Utc};
use std::{future::Future, pin::Pin};

use rust_decimal::Decimal;
use sqlx::SqlitePool;

use crate::{
    copytrading::{
        execute::{
            cancel_expired_intent, claim_or_resume_intent, finalize_receipt, next_attempt_number,
            open_reconciliation_case as open_execute_case, reject_pre_submit_intent,
            size_and_reserve, ClaimedIntent, ExecuteError, SizedDecision, SizingOutcome,
        },
        persistent::release_pre_boundary_failure,
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

pub trait SubmitAttemptMarker {
    fn mark_submitting<'a>(
        &'a self,
        pool: &'a SqlitePool,
        intent_id: i64,
        attempt_id: i64,
        now: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<(), OrchestrateError>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy)]
pub struct StandardSubmitAttemptMarker;

impl SubmitAttemptMarker for StandardSubmitAttemptMarker {
    fn mark_submitting<'a>(
        &'a self,
        pool: &'a SqlitePool,
        intent_id: i64,
        attempt_id: i64,
        now: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<(), OrchestrateError>> + Send + 'a>> {
        Box::pin(async move {
            mark_attempt_submitting(pool, intent_id, attempt_id, now)
                .await
                .map_err(OrchestrateError::Reconcile)
        })
    }
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
    Persistent(crate::copytrading::persistent::PersistentError),
    Prepare(String),
    Submit(SubmitError),
    Receipt(String),
}

impl std::fmt::Display for OrchestrateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Execute(error) => write!(formatter, "{error}"),
            Self::Reconcile(error) => write!(formatter, "{error}"),
            Self::Persistent(error) => write!(formatter, "{error}"),
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

impl From<crate::copytrading::persistent::PersistentError> for OrchestrateError {
    fn from(error: crate::copytrading::persistent::PersistentError) -> Self {
        Self::Persistent(error)
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
    execute_one_intent_with_marker(
        pool,
        balance_reader,
        execution,
        envelopes,
        trade_history,
        &StandardSubmitAttemptMarker,
        intent_id,
        now,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_one_intent_with_marker<B, E, F, H, M>(
    pool: &SqlitePool,
    balance_reader: &B,
    execution: &E,
    envelopes: &F,
    trade_history: &H,
    marker: &M,
    intent_id: i64,
    now: DateTime<Utc>,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    B: StrictAccountBalanceReader,
    E: CopyExecution,
    F: EnvelopeFactory,
    H: StrictTradeHistoryReader,
    M: SubmitAttemptMarker,
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
            marker,
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
        SizingOutcome::Rejected(reason) => {
            reject_pre_submit_intent(pool, claimed.intent_id, reason).await?;
            return Ok(OrchestrateOutcome::Rejected);
        }
    };

    let envelope = match envelopes.prepare(&decision).await {
        Ok(envelope) => envelope,
        Err(detail) => {
            // No request has crossed the venue boundary: the decision is
            // reserved but signing/preparation failed locally. Release the
            // reservation atomically with a durable pre-submit rejection;
            // never strand an in_progress intent or propagate an error that
            // a runner might blindly retry.
            reject_pre_submit_intent(
                pool,
                intent_id,
                &format!("envelope preparation failed: {detail}"),
            )
            .await?;
            return Ok(OrchestrateOutcome::Rejected);
        }
    };
    let attempt_number = next_attempt_number(pool, intent_id).await?;
    load_or_prepare_attempt(pool, intent_id, attempt_number, &envelope).await?;
    let attempt = load_latest_attempt(pool, intent_id).await?.ok_or_else(|| {
        OrchestrateError::Execute(ExecuteError::Database(
            "missing attempt after prepare".into(),
        ))
    })?;
    persist_expected_venue_order_id(pool, intent_id, attempt.id, &attempt.envelope).await?;
    submit_prepared(pool, execution, marker, intent_id, attempt, now).await
}

#[allow(clippy::too_many_arguments)]
async fn walk_existing_attempt<B, E, F, H, M>(
    pool: &SqlitePool,
    balance_reader: &B,
    execution: &E,
    envelopes: &F,
    trade_history: &H,
    marker: &M,
    claimed: &ClaimedIntent,
    attempt: AttemptRow,
    now: DateTime<Utc>,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    B: StrictAccountBalanceReader,
    E: CopyExecution,
    F: EnvelopeFactory,
    H: StrictTradeHistoryReader,
    M: SubmitAttemptMarker,
{
    let window_count = attempts_in_window(pool, claimed.intent_id).await?;
    match permitted_recovery_action(&attempt.status, true, window_count) {
        RecoveryAction::MarkSubmittingThenSubmit => {
            submit_prepared(pool, execution, marker, claimed.intent_id, attempt, now).await
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
            match prepare_new_attempt(pool, balance_reader, envelopes, claimed).await? {
                RetryPreparation::Prepared => {}
                RetryPreparation::Expired => return Ok(OrchestrateOutcome::Expired),
                RetryPreparation::Rejected => return Ok(OrchestrateOutcome::Rejected),
            }
            let attempt = load_latest_attempt(pool, claimed.intent_id)
                .await?
                .ok_or_else(|| {
                    OrchestrateError::Execute(ExecuteError::Database(
                        "missing attempt after retry prepare".into(),
                    ))
                })?;
            persist_expected_venue_order_id(pool, claimed.intent_id, attempt.id, &attempt.envelope)
                .await?;
            submit_prepared(pool, execution, marker, claimed.intent_id, attempt, now).await
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

enum RetryPreparation {
    Prepared,
    Expired,
    Rejected,
}

async fn prepare_new_attempt<B, F>(
    pool: &SqlitePool,
    balance_reader: &B,
    envelopes: &F,
    claimed: &ClaimedIntent,
) -> Result<RetryPreparation, OrchestrateError>
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
            return Ok(RetryPreparation::Expired);
        }
        SizingOutcome::Rejected(reason) => {
            reject_pre_submit_intent(pool, claimed.intent_id, reason).await?;
            return Ok(RetryPreparation::Rejected);
        }
    };
    let envelope = match envelopes.prepare(&decision).await {
        Ok(envelope) => envelope,
        Err(detail) => {
            reject_pre_submit_intent(
                pool,
                claimed.intent_id,
                &format!("envelope preparation failed: {detail}"),
            )
            .await?;
            return Ok(RetryPreparation::Rejected);
        }
    };
    let attempt_number = next_attempt_number(pool, claimed.intent_id).await?;
    load_or_prepare_attempt(pool, claimed.intent_id, attempt_number, &envelope).await?;
    Ok(RetryPreparation::Prepared)
}

async fn submit_prepared<E>(
    pool: &SqlitePool,
    execution: &E,
    marker: &impl SubmitAttemptMarker,
    intent_id: i64,
    attempt: AttemptRow,
    now: DateTime<Utc>,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    E: CopyExecution,
{
    if let Err(error) = marker
        .mark_submitting(pool, intent_id, attempt.id, now)
        .await
    {
        // One Leader spending its own window budget is that limit working,
        // not a fault. Every other error here is a reason to stop; this one
        // is a reason to skip a signal. Without this arm it reaches the
        // runner as EXIT_BUDGET_STATE, which systemd is told not to restart,
        // so the busiest Leader silently halts copying for all the others --
        // precisely the coupling per-Leader budgets exist to remove. It did:
        // "leader 2 rolling budget exhausted: used=9.36 requested=9.3590
        // cap=10" stopped the engine for eight hours. Nothing crossed the
        // venue boundary, and no reservation was taken, so the attempt and
        // its intent close out exactly like any other pre-submit rejection.
        if matches!(
            error,
            OrchestrateError::Persistent(
                crate::copytrading::persistent::PersistentError::LeaderBudgetExhausted { .. }
            )
        ) {
            let reason = error.to_string();
            mark_attempt_rejected(pool, intent_id, attempt.id, &reason).await?;
            reject_pre_submit_intent(pool, intent_id, &reason).await?;
            return Ok(OrchestrateOutcome::Rejected);
        }
        return Err(error);
    }
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
            release_pre_boundary_failure(pool, attempt.id, "venue definitively rejected order")
                .await?;
            Ok(OrchestrateOutcome::Rejected)
        }
        Err(SubmitError::Local(detail)) => {
            // AGENTS.md distinguishes three classes of submit error.
            // `Local` is the *most* definitively pre-boundary: the
            // request never left the process (reconstruction / signing /
            // serialization failed before the HTTP boundary). Unlike
            // `Rejected` (venue definitively refused before creating
            // an order -- it *was* sent), `Local` cannot have crossed
            // the boundary by definition, so the rolling-budget
            // reservation is unconditionally releaseable here, and the
            // reconciliation case must be opened so the operator
            // dashboard can read the audit trail.
            //
            // The release + case-open MUST happen atomically. P0-3 step 5
            // (regression-audit high finding) collapsed the two
            // transactions into a single helper so a crash between the
            // release and the case-open can no longer free the rolling-
            // budget USDC without leaving an audit case behind.
            crate::copytrading::reconcile::open_local_submission_failure_case(
                pool, intent_id, attempt.id, &detail,
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
                    let detail_string = detail.to_string();
                    open_reconciliation_case(
                        pool,
                        intent_id,
                        Some(attempt.id),
                        "strict_query_failure",
                        &detail_string,
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
    let receipt = match execution.query_prepared_envelope(&attempt.envelope).await {
        Ok(receipt) => receipt,
        Err(detail) => {
            // P3-4: this query occurs after the submission boundary. A
            // failure means venue state is unknown, never a local
            // pre-submit error. Record a visible case and fail closed;
            // recovery may query again later but must never resubmit.
            open_reconciliation_case(
                pool,
                intent_id,
                Some(attempt.id),
                "strict_query_failure",
                &detail,
            )
            .await?;
            return Ok(OrchestrateOutcome::NeedsReconcile(
                "prepared envelope lookup failed",
            ));
        }
    };
    if let Some(receipt) = receipt {
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
    state: &crate::venue::types::VenueOrderState,
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
            let resolver = crate::copytrading::prepare::SdkNegRiskResolver { client: &client };
            let preparer = crate::copytrading::prepare::EnvelopePreparer {
                client: &client,
                signer: &signer,
                neg_risk_resolver: &resolver,
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
mod tests;
