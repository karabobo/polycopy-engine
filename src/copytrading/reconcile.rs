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

use std::{collections::HashMap, fmt, str::FromStr as _};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::venue::{
    intl_clob::{
        AccountTrade, AccountTradeRole, AccountTradeSide, AccountTradeStatus, OutcomeTokenId,
        StrictTradeHistoryError, StrictTradeHistoryReader,
    },
    OrderReceipt,
};

/// Maximum submission attempts for one intent within [`RETRY_WINDOW_SECONDS`]
/// before retries are exhausted and a reconciliation case opens.
pub const MAX_ATTEMPTS_PER_WINDOW: i64 = 5;
pub const RETRY_WINDOW_SECONDS: i64 = 600;
const TRADE_HISTORY_TIMESTAMP_SKEW_SECONDS: i64 = 1;

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
    /// The deterministic order identifier calculated from the exact signed
    /// wire envelope *before* it crosses the HTTP boundary. A response may be
    /// lost, but this value must not depend on that response. Phase 0.5 still
    /// has to prove that it equals the CLOB history endpoint's
    /// `taker_order_id` for the real FAK path.
    pub expected_taker_order_id: String,
    /// Exact serialized signed-order wire payload, retained only in the
    /// local order-attempt database for forensic replay/reconciliation. It
    /// contains all version-specific maker/signer/amount/expiry/signature
    /// fields and must never be committed or logged.
    pub signed_order_json: String,
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

/// The bounded server-time range in which one attempt may have crossed the
/// venue boundary. It is persisted/constructed by the caller from the moment
/// the attempt was marked `submitting`; a later trade is never matched merely
/// because it happens to share a token and price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeHistoryWindow {
    after: DateTime<Utc>,
    before: DateTime<Utc>,
}

impl TradeHistoryWindow {
    pub fn new(
        after: DateTime<Utc>,
        before: DateTime<Utc>,
    ) -> Result<Self, TradeHistoryRecoveryError> {
        if after > before {
            return Err(TradeHistoryRecoveryError::InvalidWindow);
        }

        Ok(Self { after, before })
    }

    pub fn after(&self) -> DateTime<Utc> {
        self.after
    }

    pub fn before(&self) -> DateTime<Utc> {
        self.before
    }

    fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        self.after <= timestamp && timestamp <= self.before
    }
}

/// Result of read-only lookup of a prepared FAK through authenticated trade
/// history. Only [`Self::Recovered`] may supply a venue order ID to a later,
/// still-strict order lookup. Every other outcome keeps the attempt uncertain
/// and must be reconciled rather than resubmitted.
#[derive(Debug, Clone, PartialEq)]
pub enum TradeHistoryLookup {
    Recovered {
        order_id: OrderId,
        filled_qty: Decimal,
    },
    NotFound,
}

/// Result of the complete, read-only recovery path for a POST whose response
/// was lost before its venue order ID could be durably stored. A recovered ID
/// is only attached to the attempt; it still needs the normal strict by-ID
/// receipt lookup before any lot can change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LostSubmissionRecoveryOutcome {
    Recovered { order_id: OrderId },
    NeedsReconcile,
}

/// Strict failures while deciding whether account trade history identifies one
/// prepared envelope. None of these errors mean an order was absent.
#[derive(Debug)]
pub enum TradeHistoryRecoveryError {
    InvalidWindow,
    InvalidTokenId,
    InvalidSide,
    InvalidLimitPrice,
    UnsupportedOrderType,
    MissingOrderFingerprint,
    InvalidSignedOrderJson,
    ConflictingDuplicateTrade { trade_id: String },
    Query(StrictTradeHistoryError),
}

impl fmt::Display for TradeHistoryRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWindow => write!(formatter, "trade-history window ends before it starts"),
            Self::InvalidTokenId => write!(
                formatter,
                "prepared envelope has an invalid outcome token ID"
            ),
            Self::InvalidSide => write!(formatter, "prepared envelope has an invalid order side"),
            Self::InvalidLimitPrice => {
                write!(formatter, "prepared envelope has an invalid limit price")
            }
            Self::UnsupportedOrderType => write!(
                formatter,
                "trade-history recovery only supports FAK envelopes"
            ),
            Self::MissingOrderFingerprint => write!(
                formatter,
                "prepared envelope has no precomputed taker-order identifier"
            ),
            Self::InvalidSignedOrderJson => write!(
                formatter,
                "prepared envelope has no valid serialized signed-order payload"
            ),
            Self::ConflictingDuplicateTrade { trade_id } => write!(
                formatter,
                "trade history returned conflicting observations for trade {trade_id}"
            ),
            Self::Query(source) => write!(formatter, "strict trade-history query failed: {source}"),
        }
    }
}

impl std::error::Error for TradeHistoryRecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Query(source) => Some(source),
            _ => None,
        }
    }
}

/// Queries the authenticated account's complete trade history page stream for
/// this envelope's token and applies [`recover_fak_taker_order_from_trades`].
/// This method makes GET requests only; it has no signing, submission,
/// cancellation, allowance, or retry behavior.
pub async fn lookup_prepared_fak_in_trade_history<R>(
    reader: &R,
    envelope: &PreparedOrderEnvelope,
    window: TradeHistoryWindow,
) -> Result<TradeHistoryLookup, TradeHistoryRecoveryError>
where
    R: StrictTradeHistoryReader + ?Sized,
{
    let token_id = OutcomeTokenId::from_str(&envelope.token_id)
        .map_err(|_| TradeHistoryRecoveryError::InvalidTokenId)?;
    let trades = reader
        .trades_for_token_between(&token_id, window.after(), window.before())
        .await
        .map_err(TradeHistoryRecoveryError::Query)?;

    recover_fak_taker_order_from_trades(envelope, window, &trades)
}

/// Matches a FAK envelope against authenticated trade history without making a
/// network request. A filled FAK is expected to be the taker: every accepted
/// fill must have the precomputed `expected_taker_order_id` from the prepared
/// envelope. The matcher deliberately does **not** compare `trade.size` with
/// the envelope's `size` for BUY: the Phase 0.5 canary proved a BUY's
/// requested size is a budget cap, while a trade's size is actual matched
/// shares. Any missing fingerprint, unknown status/side/role, out-of-window
/// trade, limit-incompatible price, duplicate conflict, or zero result is
/// fail-closed.
pub fn recover_fak_taker_order_from_trades(
    envelope: &PreparedOrderEnvelope,
    window: TradeHistoryWindow,
    trades: &[AccountTrade],
) -> Result<TradeHistoryLookup, TradeHistoryRecoveryError> {
    if envelope.order_type != "FAK" {
        return Err(TradeHistoryRecoveryError::UnsupportedOrderType);
    }
    if envelope.expected_taker_order_id.trim().is_empty() {
        return Err(TradeHistoryRecoveryError::MissingOrderFingerprint);
    }
    if !matches!(
        serde_json::from_str::<serde_json::Value>(&envelope.signed_order_json),
        Ok(serde_json::Value::Object(_))
    ) {
        return Err(TradeHistoryRecoveryError::InvalidSignedOrderJson);
    }
    if envelope.token_id.is_empty() || !envelope.token_id.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(TradeHistoryRecoveryError::InvalidTokenId);
    }
    let side = match envelope.side.as_str() {
        "BUY" => AccountTradeSide::Buy,
        "SELL" => AccountTradeSide::Sell,
        _ => return Err(TradeHistoryRecoveryError::InvalidSide),
    };
    let limit_price = Decimal::from_str(&envelope.price)
        .ok()
        .filter(|price| *price > Decimal::ZERO && *price < Decimal::ONE)
        .ok_or(TradeHistoryRecoveryError::InvalidLimitPrice)?;

    let mut seen_by_trade_id: HashMap<&str, &AccountTrade> = HashMap::new();
    let mut filled_qty = Decimal::ZERO;

    for trade in trades {
        if let Some(previous) = seen_by_trade_id.insert(&trade.trade_id, trade) {
            if previous != trade {
                return Err(TradeHistoryRecoveryError::ConflictingDuplicateTrade {
                    trade_id: trade.trade_id.clone(),
                });
            }
            continue;
        }

        let is_limit_compatible = match side {
            AccountTradeSide::Buy => trade.price <= limit_price,
            AccountTradeSide::Sell => trade.price >= limit_price,
            AccountTradeSide::Unknown => false,
        };
        if trade.taker_order_id != envelope.expected_taker_order_id
            || trade.token_id.to_string() != envelope.token_id
            || trade.side != side
            || trade.role != AccountTradeRole::Taker
            || !matches!(
                trade.status,
                AccountTradeStatus::Matched
                    | AccountTradeStatus::Mined
                    | AccountTradeStatus::Confirmed
            )
            || !window.contains(trade.match_time)
            || !is_limit_compatible
            || trade.size <= Decimal::ZERO
        {
            continue;
        }

        filled_qty += trade.size;
    }

    if filled_qty == Decimal::ZERO {
        Ok(TradeHistoryLookup::NotFound)
    } else {
        Ok(TradeHistoryLookup::Recovered {
            order_id: OrderId(envelope.expected_taker_order_id.clone()),
            filled_qty,
        })
    }
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
    ) -> impl std::future::Future<Output = Result<OrderReceipt, SubmitError>> + Send;
}

/// Distinguishes a local failure (the request never left this process) from
/// a transport failure (the request may have crossed the venue boundary)
/// from a definitive venue rejection (no `order_id` was created).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitError {
    /// Reconstruction, validation, or other local work failed. The attempt
    /// must not be marked `uncertain` because nothing was submitted.
    Local(String),
    /// A network/timeout/5xx error after the request may have been sent.
    /// The attempt becomes `uncertain` and is never retried automatically.
    Transport(String),
    /// The venue processed the request and refused it before creating an
    /// order (HTTP 4xx, including the live `invalid. Duplicated.` case).
    Rejected(String),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(detail) => write!(formatter, "local submission error: {detail}"),
            Self::Transport(detail) => write!(formatter, "transport submission error: {detail}"),
            Self::Rejected(detail) => write!(formatter, "venue rejected order: {detail}"),
        }
    }
}

impl std::error::Error for SubmitError {}

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
        .bind(&candidate.size)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

        Ok(candidate.clone())
    }
    .await;

    match &result {
        Ok(_) => {
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
        .fetch_one(&**db)
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
}
