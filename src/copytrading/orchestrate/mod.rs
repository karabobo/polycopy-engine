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
            reprice_fixed_share_buy_after_no_fak, size_and_reserve, ClaimedIntent, ExecuteError,
            SizedDecision, SizingOutcome,
        },
        persistent::release_pre_boundary_failure,
        reconcile::{
            attempts_in_window, load_or_prepare_attempt, mark_attempt_gtd_lookup_failed,
            mark_attempt_rejected, mark_attempt_submitting,
            mark_attempt_uncertain_after_submission_error, open_reconciliation_case,
            permitted_recovery_action, recover_lost_submission_response, CopyExecution,
            LostSubmissionRecoveryOutcome, PreparedOrderEnvelope, ReconcileError, RecoveryAction,
            SubmitError,
        },
    },
    venue::{
        intl_clob::{StrictAccountBalanceReader, StrictTradeHistoryReader},
        intl_clob_exec::receipt_from_submitted_envelope,
        OrderReceipt,
    },
};

/// A fresh-book best ask for the single FAK retry after a deterministic
/// no-match.  This is deliberately one displayed level, never a depth sweep:
/// the retry may take only immediately available liquidity at the best ask.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NoFakSweepQuote {
    pub limit_price: Decimal,
    pub visible_shares: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GtdMarketSpec {
    pub expires_at: DateTime<Utc>,
    pub tick_size: Decimal,
}

/// Bounded resting lifetime for the post-only GTD maker retry. Deliberately
/// unrelated to `MarketResponse.end_date_iso`: that field is a day-level
/// placeholder for auto-generated recurring crypto slots, not the real
/// per-window resolution instant, so anchoring a maker order's expiry to it
/// leaves resting orders open far longer than intended (and, in some
/// genuinely-still-open markets, rejects the order outright). The value is
/// pinned above the venue's hard floor of 180 seconds (Polymarket rejects
/// GTD expirations closer than that with `"expiration is less than 180
/// seconds in the future"`), with a small margin to absorb clock skew and
/// the time between computing `expires_at` here and the venue receiving the
/// signed request. It remains short relative to a market's actual lifetime
/// (these are 5/15-minute crypto slots), so the original design goal --
/// bounded resting exposure, decoupled from the unreliable `end_date_iso`
/// field -- is unaffected.
pub const GTD_MAKER_EXPIRY: chrono::Duration = chrono::Duration::seconds(200);

/// Validates a freshly-fetched `MarketResponse` and derives the bounded GTD
/// maker spec used by the post-only retry. Pure: takes `now` explicitly so
/// callers (and tests) can pin the expected `expires_at`. The "is this
/// market still open?" question is answered by `closed`/`accepting_orders`,
/// not by `end_date_iso`, because the latter is not a reliable per-window
/// timestamp on Polymarket's recurring crypto slots.
pub fn derive_gtd_market_spec(
    market: &polymarket_client_sdk_v2::clob::types::response::MarketResponse,
    now: DateTime<Utc>,
) -> Result<GtdMarketSpec, String> {
    if market.closed || !market.accepting_orders {
        return Err(
            "market is closed or not accepting orders; refusing GTD maker order".to_owned(),
        );
    }
    if market.minimum_tick_size <= Decimal::ZERO {
        return Err("market end lookup returned invalid minimum_tick_size".to_owned());
    }
    Ok(GtdMarketSpec {
        expires_at: now + GTD_MAKER_EXPIRY,
        tick_size: market.minimum_tick_size,
    })
}

/// Returns the lowest-displayed ask price from a fresh book, ignoring the
/// advertised size at that level. The maker-only execution path caps a
/// post-only GTD BUY at this value (when it is more favorable than the
/// leader-derived ceiling) so the resting order never crosses an existing
/// ask on arrival. Unlike `no_fak_sweep_quote`, this helper does NOT
/// validate against a target share count: the maker-only path is shares-
/// pinned by `SizeRatio` *or* `max_order_shares`, and the price is the
/// only thing the live book informs. Empty book returns `Ok(None)` so the
/// caller (orchestrate) can fail closed with a distinct message rather
/// than parsing a "no asks" error string.
pub fn best_ask_price<I>(asks: I) -> Result<Option<Decimal>, String>
where
    I: IntoIterator<Item = (Decimal, Decimal)>,
{
    let mut levels: Vec<_> = asks.into_iter().collect();
    levels.sort_by_key(|(price, _)| *price);
    let Some((price, size)) = levels.into_iter().next() else {
        return Ok(None);
    };
    if price <= Decimal::ZERO || price >= Decimal::ONE || size <= Decimal::ZERO {
        return Err("fresh order book contains an invalid best ask level".to_owned());
    }
    Ok(Some(price))
}

/// Returns the best valid displayed ask from a fresh book. `target_shares` is
/// retained as a mandatory positive guard for the fixed-share retry contract;
/// it must not influence the price ceiling, otherwise a thin book turns a
/// retry into an unbounded multi-level sweep.
pub fn no_fak_sweep_quote<I>(
    asks: I,
    target_shares: Decimal,
) -> Result<NoFakSweepQuote, String>
where
    I: IntoIterator<Item = (Decimal, Decimal)>,
{
    if target_shares <= Decimal::ZERO {
        return Err("fixed-share no-FAK retry has a non-positive target".to_owned());
    }

    let mut levels: Vec<_> = asks.into_iter().collect();
    levels.sort_by_key(|(price, _)| *price);
    let Some((price, size)) = levels.into_iter().next() else {
        return Err("fresh order book has no asks".to_owned());
    };
    if price <= Decimal::ZERO || price >= Decimal::ONE || size <= Decimal::ZERO {
        return Err("fresh order book contains an invalid ask level".to_owned());
    }
    Ok(NoFakSweepQuote {
        limit_price: price,
        visible_shares: size,
    })
}

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

    /// Reads a fresh ask sweep only for the single explicit no-FAK retry.
    /// The default is fail-closed so test or alternate factories cannot
    /// accidentally gain a price-chasing path.
    fn sweep_quote_for_no_fak_retry<'a>(
        &'a self,
        _token_id: &'a str,
        _target_shares: Decimal,
    ) -> Pin<Box<dyn Future<Output = Result<NoFakSweepQuote, String>> + Send + 'a>> {
        Box::pin(async { Err("no-FAK fresh-book sweep retry is unavailable".to_owned()) })
    }

    /// Validates that the fetched market is still open and returns the
    /// bounded GTD maker spec used by the post-only retry. The default is
    /// fail-closed so alternate factories cannot accidentally relax the
    /// "no maker order without a proven spec" invariant.
    fn market_spec_for_gtd<'a>(
        &'a self,
        _condition_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<GtdMarketSpec, String>> + Send + 'a>> {
        Box::pin(async { Err("market end lookup for GTD maker retry is unavailable".to_owned()) })
    }

    /// Fetches the real-time best ask for `token_id` and returns just the
    /// price. Used only by the maker-only execution path (decision.maker_only
    /// == true), which then caps the post-only GTD BUY price at this value
    /// when it is more favorable than the leader-derived ceiling. Distinct
    /// from `sweep_quote_for_no_fak_retry`: that helper answers a VWAP
    /// question for a dead FAK retry and is intentionally a different
    /// shape; this one answers a single-price question for the maker-only
    /// path and shares its pure-function implementation `best_ask_price`
    /// with whatever tests need it. Default is fail-closed (an order-book
    /// read failure becomes a pre-submit rejection, never a stale-price
    /// fallback -- see docs/leader2-strategy-redesign-handoff.md Part 3
    /// Change 4 for the design choice).
    fn fetch_best_ask_for_maker_only<'a>(
        &'a self,
        _token_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Decimal, String>> + Send + 'a>> {
        Box::pin(async {
            Err("best-ask lookup for maker-only execution is unavailable".to_owned())
        })
    }

    /// Signs the third, post-only GTD maker attempt. It receives the exact
    /// persisted sizing decision plus the independently fetched end time;
    /// implementations must retain both in the signed envelope.
    fn prepare_post_only_gtd_buy<'a>(
        &'a self,
        _decision: &'a SizedDecision,
        _expires_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<PreparedOrderEnvelope, String>> + Send + 'a>> {
        Box::pin(async { Err("post-only GTD maker retry is unavailable".to_owned()) })
    }
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
    /// A post-only GTD order is accepted by the venue and remains open. Its
    /// reservation stays active; the next runner tick polls the exact order.
    Resting,
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
         -- A short decision window is a freshness bound, not a queueing
         -- hint. Token ordering can put an almost-expired intent behind an
         -- unrelated, later-deadline token and make expiry deterministic
         -- during a burst. Null is last for legacy/corrupt rows; normal
         -- planned intents always carry a durable deadline.
         ORDER BY CASE WHEN ci.decision_deadline_at IS NULL THEN 1 ELSE 0 END, \
                  ci.decision_deadline_at ASC, ci.id ASC",
    )
    .bind(account_id)
    .fetch_all(pool)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))
}

#[derive(Clone)]
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

/// Builds a post-only GTD envelope for a maker-only `decision`. Reads the
/// market end time, fetches the real-time best ask, and chooses
/// `maker_price = min(decision.limit_price, best_ask)` (the leader-derived
/// ceiling is the upper bound; the live ask replaces it only when more
/// favorable). All three steps are fail-closed at the caller: this helper
/// returns an error and lets orchestrate turn that into a durable
/// `reject_pre_submit_intent` rather than falling back to a stale price
/// (see docs/leader2-strategy-redesign-handoff.md Part 3 Change 4 for the
/// design choice).
///
/// Same shape in both call sites (fresh intent path and `prepare_new_attempt`'s
/// resume path): they pass the intent's `condition_id` and the relevant
/// `decision`, then get back the prepared envelope (and the persisted attempt
/// row). Reusing one helper rather than
/// duplicating keeps the fail-closed guarantees in one place -- a future
/// edit can't accidentally drop the best-ask fetch from one path and not
/// the other. The caller owns the post-failure `reject_pre_submit_intent`
/// call so the same intent-state guarantee applies on both code paths.
async fn prepare_maker_only_envelope<F>(
    envelopes: &F,
    condition_id: &str,
    decision: &SizedDecision,
    now: DateTime<Utc>,
) -> Result<MakerOnlyEnvelope, MakerOnlyError>
where
    F: EnvelopeFactory,
{
    let market = envelopes
        .market_spec_for_gtd(condition_id)
        .await
        .map_err(MakerOnlyError::MarketSpec)?;
    if market.expires_at <= now {
        return Err(MakerOnlyError::Expired);
    }
    let best_ask = envelopes
        .fetch_best_ask_for_maker_only(&decision.token_id)
        .await
        .map_err(MakerOnlyError::BestAsk)?;
    // Per docs/maker-only-post-only-crosses-book-bug.md Fix section:
    // venue evidence (5/5 attempts for intent 721) confirms a post-only BUY
    // priced exactly at best_ask is rejected as "crosses book" unconditionally.
    // Use std::cmp::min to guarantee the price is strictly below best_ask
    // by at least one tick, in both the leader-price-better and market-better
    // cases. Deviation from the 2026-09-23 literal instruction ("用 best ask
    // 价格") is justified by direct venue evidence.
    let maker_price = std::cmp::min(decision.limit_price, best_ask - market.tick_size);
    let mut priced_decision = decision.clone();
    priced_decision.limit_price = maker_price;
    let envelope = envelopes
        .prepare_post_only_gtd_buy(&priced_decision, market.expires_at)
        .await
        .map_err(MakerOnlyError::Prepare)?;
    Ok(MakerOnlyEnvelope {
        envelope,
        market_expires_at: market.expires_at,
    })
}

/// The successful output of `prepare_maker_only_envelope`. Carries the
/// envelope plus the market-end expiry that drove it (so the caller's
/// `persist_expected_venue_order_id` step doesn't have to re-derive the
/// same value from a second market lookup).
struct MakerOnlyEnvelope {
    envelope: PreparedOrderEnvelope,
    #[allow(dead_code)]
    market_expires_at: DateTime<Utc>,
}

/// Why a maker-only preparation failed. Every variant maps to a distinct
/// caller action: market-spec and best-ask errors become a durable
/// `reject_pre_submit_intent`; `Expired` becomes `cancel_expired_intent`;
/// `Prepare` errors also become `reject_pre_submit_intent`. Keeping the
/// classification in one place keeps the caller's control flow simple.
#[derive(Debug)]
enum MakerOnlyError {
    MarketSpec(String),
    BestAsk(String),
    Prepare(String),
    Expired,
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

    let envelope = if decision.maker_only {
        match prepare_maker_only_envelope(envelopes, &claimed.condition_id, &decision, now).await {
            Ok(maker) => {
                let attempt_number = next_attempt_number(pool, intent_id).await?;
                load_or_prepare_attempt(pool, intent_id, attempt_number, &maker.envelope)
                    .await?;
                let attempt = load_latest_attempt(pool, intent_id).await?.ok_or_else(|| {
                    OrchestrateError::Execute(ExecuteError::Database(
                        "missing attempt after maker-only preparation".into(),
                    ))
                })?;
                persist_expected_venue_order_id(
                    pool,
                    intent_id,
                    attempt.id,
                    &attempt.envelope,
                )
                .await?;
                return submit_prepared(pool, execution, marker, intent_id, attempt, now).await;
            }
            Err(MakerOnlyError::Expired) => {
                cancel_expired_intent(pool, claimed.intent_id).await?;
                return Ok(OrchestrateOutcome::Expired);
            }
            Err(MakerOnlyError::MarketSpec(detail)) => {
                reject_pre_submit_intent(
                    pool,
                    intent_id,
                    &format!("maker-only GTD market-end lookup failed: {detail}"),
                )
                .await?;
                return Ok(OrchestrateOutcome::Rejected);
            }
            Err(MakerOnlyError::BestAsk(detail)) => {
                reject_pre_submit_intent(
                    pool,
                    intent_id,
                    &format!("maker-only best-ask lookup failed: {detail}"),
                )
                .await?;
                return Ok(OrchestrateOutcome::Rejected);
            }
            Err(MakerOnlyError::Prepare(detail)) => {
                reject_pre_submit_intent(
                    pool,
                    intent_id,
                    &format!("maker-only envelope preparation failed: {detail}"),
                )
                .await?;
                return Ok(OrchestrateOutcome::Rejected);
            }
        }
    } else {
        match envelopes.prepare(&decision).await {
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
    let outcome = submit_prepared(pool, execution, marker, intent_id, attempt.clone(), now).await?;
    maybe_retry_no_fak_once(
        pool,
        balance_reader,
        execution,
        envelopes,
        marker,
        &claimed,
        &attempt,
        outcome,
        now,
    )
    .await
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
    if attempt.envelope.order_type == "GTD" && attempt.status == "accepted" {
        return poll_resting_gtd(pool, execution, claimed.intent_id, attempt).await;
    }
    let window_count = attempts_in_window(pool, claimed.intent_id).await?;
    match permitted_recovery_action(&attempt.status, true, window_count) {
        RecoveryAction::MarkSubmittingThenSubmit => {
            let outcome = submit_prepared(
                pool,
                execution,
                marker,
                claimed.intent_id,
                attempt.clone(),
                now,
            )
            .await?;
            maybe_retry_no_fak_once(
                pool,
                balance_reader,
                execution,
                envelopes,
                marker,
                claimed,
                &attempt,
                outcome,
                now,
            )
            .await
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
            if is_explicit_no_fak_rejection(pool, attempt.id).await? {
                return maybe_retry_no_fak_once(
                    pool,
                    balance_reader,
                    execution,
                    envelopes,
                    marker,
                    claimed,
                    &attempt,
                    OrchestrateOutcome::Rejected,
                    now,
                )
                .await;
            }
            match prepare_new_attempt(pool, balance_reader, envelopes, claimed, now).await? {
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

async fn poll_resting_gtd<E>(
    pool: &SqlitePool,
    execution: &E,
    intent_id: i64,
    attempt: AttemptRow,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    E: CopyExecution,
{
    // A failed GTD lookup leaves venue truth unknown, whether the error is
    // a transient 5xx or a 404. The venue documents that /data/order/{id}
    // can also return canceled and fully matched orders; a 404 does not
    // establish zero fill. Move the attempt and tracking case together into
    // reconciliation rather than repeating the same lookup on every startup.
    // Resolving whether it actually filled is a separate step: an operator
    // running `persistent_control reconcile-uncertain <attempt-id>`, which
    // routes a GTD envelope to the maker-side trade-history matcher
    // (`venue::trade_history_recovery::recover_gtd_maker_order_from_trades`).
    let state = match execution
        .order_for_receipt(&crate::venue::types::OrderId(
            attempt.envelope.expected_taker_order_id.clone(),
        ))
        .await
    {
        Ok(state) => state,
        Err(detail) => {
            // Preserve the lookup error for the operator; this error alone
            // does not establish a GTD fill one way or the other -- that
            // determination happens later via `reconcile-uncertain`.
            let failure_detail = format!(
                "GTD live-order lookup failed and could not be retried here: {detail}"
            );
            let transitioned = mark_attempt_gtd_lookup_failed(
                pool,
                intent_id,
                attempt.id,
                &failure_detail,
            )
            .await
            .map_err(OrchestrateError::Reconcile)?;
            if !transitioned {
                return Ok(OrchestrateOutcome::NeedsReconcile(
                    "GTD lookup failed; attempt already transitioned",
                ));
            }
            // The helper atomically marks the attempt uncertain and opens
            // a strict_query_failure case. The runner may still latch its
            // fuse, but startup cannot silently retry or resubmit this order.
            // Do not resume until a GTD-maker-aware reconciliation is done.
            return Ok(OrchestrateOutcome::Uncertain);
        }
    };
    if !is_terminal_gtd_status(&state.status) {
        sqlx::query(
            "UPDATE order_attempts SET venue_status = ?, filled_qty = ?, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND intent_id = ?",
        )
        .bind(&state.status)
        .bind(state.size_matched.to_string())
        .bind(attempt.id)
        .bind(intent_id)
        .execute(pool)
        .await
        .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
        return Ok(OrchestrateOutcome::Resting);
    }
    if state.size_matched > Decimal::ZERO {
        let requested: Decimal = attempt
            .envelope
            .size
            .parse()
            .map_err(|_| OrchestrateError::Receipt("invalid GTD envelope size".to_owned()))?;
        let receipt = OrderReceipt::new(requested, requested, state.size_matched, Decimal::ZERO)
            .map_err(|error| OrchestrateError::Receipt(error.to_string()))?;
        finalize_receipt(pool, intent_id, attempt.id, &receipt).await?;
        return Ok(OrchestrateOutcome::Filled {
            filled_qty: state.size_matched,
        });
    }
    // A terminal zero-fill GTD has reached its venue expiry/cancellation.
    // No submission ambiguity remains, so release its reservation atomically
    // with the terminal local state rather than treating it as a failed FAK.
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    sqlx::query(
        "UPDATE order_attempts SET status = 'finalized', venue_status = ?, filled_qty = '0', \
         failure_detail = 'GTD expired or cancelled without a fill', \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND intent_id = ?",
    )
    .bind(&state.status)
    .bind(attempt.id)
    .bind(intent_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    sqlx::query(
        "UPDATE copy_intents SET status = 'cancelled', reserved_qty = '0', \
         rejection_reason = 'post-only GTD expired or cancelled without a fill', \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND status = 'in_progress'",
    )
    .bind(intent_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    tx.commit()
        .await
        .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    Ok(OrchestrateOutcome::Expired)
}

fn is_terminal_gtd_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "matched" | "filled" | "confirmed" | "mined" | "cancelled" | "canceled" | "expired"
    )
}

enum RetryPreparation {
    Prepared,
    Expired,
    Rejected,
}

async fn is_explicit_no_fak_rejection(
    pool: &SqlitePool,
    attempt_id: i64,
) -> Result<bool, OrchestrateError> {
    let matched: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM order_attempts \
         WHERE id = ? AND status = 'rejected' \
           AND failure_detail LIKE '%no orders found to match with FAK order%')",
    )
    .bind(attempt_id)
    .fetch_one(pool)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))?;
    Ok(matched != 0)
}

async fn no_fak_rejection_count(
    pool: &SqlitePool,
    intent_id: i64,
) -> Result<i64, OrchestrateError> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM order_attempts WHERE intent_id = ? AND status = 'rejected' \
         AND failure_detail LIKE '%no orders found to match with FAK order%'",
    )
    .bind(intent_id)
    .fetch_one(pool)
    .await
    .map_err(|error| OrchestrateError::Execute(ExecuteError::Database(error.to_string())))
}

#[allow(clippy::too_many_arguments)]
async fn maybe_retry_no_fak_once<B, E, F, M>(
    pool: &SqlitePool,
    balance_reader: &B,
    execution: &E,
    envelopes: &F,
    marker: &M,
    claimed: &ClaimedIntent,
    failed_attempt: &AttemptRow,
    outcome: OrchestrateOutcome,
    now: DateTime<Utc>,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    B: StrictAccountBalanceReader,
    E: CopyExecution,
    F: EnvelopeFactory,
    M: SubmitAttemptMarker,
{
    if outcome != OrchestrateOutcome::Rejected
        || !is_explicit_no_fak_rejection(pool, failed_attempt.id).await?
    {
        return Ok(outcome);
    }

    let no_match_count = no_fak_rejection_count(pool, claimed.intent_id).await?;
    // A definitive zero-fill on the initial FAK is the only trigger for the
    // maker fallback.  Do not fetch a new book or submit a second taker
    // order: that best-ask retry is intentionally disabled because it can
    // introduce materially worse slippage than the leader's own execution.
    if no_match_count == 1 {
        return submit_post_only_gtd_after_initial_no_fak(
            pool,
            balance_reader,
            execution,
            envelopes,
            marker,
            claimed,
            now,
        )
        .await;
    }
    if no_match_count != 1 {
        reject_pre_submit_intent(
            pool,
            claimed.intent_id,
            "unexpected repeated no-FAK rejection after maker fallback selection",
        )
        .await?;
        return Ok(OrchestrateOutcome::Rejected);
    }
    Ok(OrchestrateOutcome::Rejected)
}

/// The final path after one definitive initial FAK no-match: do not chase
/// another ask. Reuse the immutable fixed quantity, reset the reservation to
/// the leader's own price, and submit one post-only GTD order that expires at
/// the venue-provided market end.
#[allow(clippy::too_many_arguments)]
async fn submit_post_only_gtd_after_initial_no_fak<B, E, F, M>(
    pool: &SqlitePool,
    balance_reader: &B,
    execution: &E,
    envelopes: &F,
    marker: &M,
    claimed: &ClaimedIntent,
    now: DateTime<Utc>,
) -> Result<OrchestrateOutcome, OrchestrateError>
where
    B: StrictAccountBalanceReader,
    E: CopyExecution,
    F: EnvelopeFactory,
    M: SubmitAttemptMarker,
{
    let Some(retry_claimed) = claim_or_resume_intent(pool, claimed.intent_id).await? else {
        return Ok(OrchestrateOutcome::NotClaimed);
    };
    let Some((target_qty, _, _)) = retry_claimed.existing_decision else {
        open_execute_case(pool, claimed, "post-only GTD retry is missing its persisted fixed-share decision").await?;
        return Ok(OrchestrateOutcome::NeedsReconcile(
            "post-only GTD retry is missing its persisted fixed-share decision",
        ));
    };
    let market = match envelopes.market_spec_for_gtd(&retry_claimed.condition_id).await {
        Ok(market) => market,
        Err(detail) => {
            reject_pre_submit_intent(
                pool,
                claimed.intent_id,
                &format!("post-only GTD market-end lookup failed: {detail}"),
            )
            .await?;
            return Ok(OrchestrateOutcome::Rejected);
        }
    };
    if market.expires_at <= now {
        cancel_expired_intent(pool, claimed.intent_id).await?;
        return Ok(OrchestrateOutcome::Expired);
    }
    let maker_price = floor_to_valid_maker_price(
        retry_claimed.leader_price,
        market.tick_size,
        target_qty,
    )?;
    let decision = match reprice_fixed_share_buy_after_no_fak(
        pool,
        balance_reader,
        &retry_claimed,
        maker_price,
    )
    .await?
    {
        SizingOutcome::Decision(decision) => decision,
        SizingOutcome::NeedsReconcile(reason) => {
            open_execute_case(pool, claimed, reason).await?;
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
    let envelope = match envelopes.prepare_post_only_gtd_buy(&decision, market.expires_at).await {
        Ok(envelope) => envelope,
        Err(detail) => {
            reject_pre_submit_intent(
                pool,
                claimed.intent_id,
                &format!("post-only GTD envelope preparation failed: {detail}"),
            )
            .await?;
            return Ok(OrchestrateOutcome::Rejected);
        }
    };
    let attempt_number = next_attempt_number(pool, claimed.intent_id).await?;
    load_or_prepare_attempt(pool, claimed.intent_id, attempt_number, &envelope).await?;
    let attempt = load_latest_attempt(pool, claimed.intent_id).await?.ok_or_else(|| {
        OrchestrateError::Execute(ExecuteError::Database(
            "missing attempt after post-only GTD preparation".into(),
        ))
    })?;
    persist_expected_venue_order_id(pool, claimed.intent_id, attempt.id, &attempt.envelope)
        .await?;
    submit_prepared(pool, execution, marker, claimed.intent_id, attempt, now).await
}

fn floor_to_valid_maker_price(
    leader_price: Decimal,
    tick_size: Decimal,
    qty: Decimal,
) -> Result<Decimal, OrchestrateError> {
    if leader_price <= Decimal::ZERO || leader_price >= Decimal::ONE || tick_size <= Decimal::ZERO {
        return Err(OrchestrateError::Prepare("invalid leader price or market tick for GTD maker order".to_owned()));
    }
    let mut price = (leader_price / tick_size).floor() * tick_size;
    // The CLOB's maker USDC amount must remain cent-denominated. Keep moving
    // down by valid ticks, never above the leader's price, until that is true.
    while price > Decimal::ZERO && (price * qty).normalize().scale() > 2 {
        price -= tick_size;
    }
    if price <= Decimal::ZERO || price >= Decimal::ONE {
        return Err(OrchestrateError::Prepare("no positive cent-valid maker price at or below leader price".to_owned()));
    }
    Ok(price.normalize())
}

async fn prepare_new_attempt<B, F>(
    pool: &SqlitePool,
    balance_reader: &B,
    envelopes: &F,
    claimed: &ClaimedIntent,
    now: DateTime<Utc>,
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
    let envelope = if decision.maker_only {
        // `prepare_new_attempt` is only reached for a resumed intent that
        // already cleared the planning-time expiry check (via
        // `claim_or_resume_intent`'s decision_deadline_at gate). Re-running
        // a wall-clock comparison here would either double-gate the same
        // condition or, worse, race the deadline by the few hundred
        // microseconds between resume and prepare. The fresh-path check
        // stands; this resume path inherits it.
        match prepare_maker_only_envelope(envelopes, &claimed.condition_id, &decision, now).await {
            Ok(maker) => {
                let attempt_number = next_attempt_number(pool, claimed.intent_id).await?;
                load_or_prepare_attempt(
                    pool,
                    claimed.intent_id,
                    attempt_number,
                    &maker.envelope,
                )
                .await?;
                return Ok(RetryPreparation::Prepared);
            }
            Err(MakerOnlyError::Expired) => {
                cancel_expired_intent(pool, claimed.intent_id).await?;
                return Ok(RetryPreparation::Expired);
            }
            Err(MakerOnlyError::MarketSpec(detail)) => {
                reject_pre_submit_intent(
                    pool,
                    claimed.intent_id,
                    &format!("maker-only GTD market-end lookup failed: {detail}"),
                )
                .await?;
                return Ok(RetryPreparation::Rejected);
            }
            Err(MakerOnlyError::BestAsk(detail)) => {
                reject_pre_submit_intent(
                    pool,
                    claimed.intent_id,
                    &format!("maker-only best-ask lookup failed: {detail}"),
                )
                .await?;
                return Ok(RetryPreparation::Rejected);
            }
            Err(MakerOnlyError::Prepare(detail)) => {
                reject_pre_submit_intent(
                    pool,
                    claimed.intent_id,
                    &format!("maker-only envelope preparation failed: {detail}"),
                )
                .await?;
                return Ok(RetryPreparation::Rejected);
            }
        }
    } else {
        match envelopes.prepare(&decision).await {
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
            if attempt.envelope.order_type == "GTD" {
                // A post-only GTD may remain on book. Never finalize from the
                // submit response: every later fill is accounted from a
                // strict order-status poll against this immutable envelope.
                return Ok(OrchestrateOutcome::Resting);
            }
            finalize_receipt(pool, intent_id, attempt.id, &receipt).await?;
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
            // An explicit no-FAK response is a definitive zero-fill result.
            // The caller may perform exactly one fresh-book retry with a new
            // envelope; all other definitive rejections remain ordinary
            // rejected attempts. Transport errors stay query-first above.
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
            finalize_receipt(pool, intent_id, attempt.id, &receipt).await?;
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

    fn sweep_quote_for_no_fak_retry<'a>(
        &'a self,
        token_id: &'a str,
        target_shares: Decimal,
    ) -> Pin<Box<dyn Future<Output = Result<NoFakSweepQuote, String>> + Send + 'a>> {
        let client = self.client().clone();
        let token_id = token_id.to_owned();
        Box::pin(async move {
            let token_id = token_id
                .parse::<alloy::primitives::U256>()
                .map_err(|_| "invalid outcome token ID for no-FAK best-ask retry".to_owned())?;
            let request = polymarket_client_sdk_v2::clob::types::request::OrderBookSummaryRequest::builder()
                .token_id(token_id)
                .build();
            let book = client
                .order_book(&request)
                .await
                .map_err(|error| format!("fresh order-book read failed: {error}"))?;
            no_fak_sweep_quote(
                book.asks.into_iter().map(|level| (level.price, level.size)),
                target_shares,
            )
        })
    }

    fn market_spec_for_gtd<'a>(
        &'a self,
        condition_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<GtdMarketSpec, String>> + Send + 'a>> {
        let client = self.client().clone();
        let condition_id = condition_id.to_owned();
        Box::pin(async move {
            let market = client
                .market(&condition_id)
                .await
                .map_err(|error| format!("market end lookup failed: {error}"))?;
            derive_gtd_market_spec(&market, Utc::now())
        })
    }

    fn prepare_post_only_gtd_buy<'a>(
        &'a self,
        decision: &'a SizedDecision,
        expires_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<PreparedOrderEnvelope, String>> + Send + 'a>> {
        let client = self.client().clone();
        let signer = self.signer().clone();
        let decision = decision.clone();
        Box::pin(async move {
            let resolver = crate::copytrading::prepare::SdkNegRiskResolver { client: &client };
            let preparer = crate::copytrading::prepare::EnvelopePreparer {
                client: &client,
                signer: &signer,
                neg_risk_resolver: &resolver,
            };
            preparer
                .prepare_post_only_gtd_buy(&decision, expires_at)
                .await
                .map(|prepared| prepared.envelope)
                .map_err(|error| error.to_string())
        })
    }

    fn fetch_best_ask_for_maker_only<'a>(
        &'a self,
        token_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Decimal, String>> + Send + 'a>> {
        let client = self.client().clone();
        let token_id = token_id.to_owned();
        Box::pin(async move {
            let token_id = token_id
                .parse::<alloy::primitives::U256>()
                .map_err(|_| "invalid outcome token ID for best-ask lookup".to_owned())?;
            let request = polymarket_client_sdk_v2::clob::types::request::OrderBookSummaryRequest::builder()
                .token_id(token_id)
                .build();
            let book = client
                .order_book(&request)
                .await
                .map_err(|error| format!("fresh order-book read failed: {error}"))?;
            match best_ask_price(book.asks.into_iter().map(|level| (level.price, level.size))) {
                Ok(Some(price)) => Ok(price),
                Ok(None) => Err("order book has no asks".to_owned()),
                Err(detail) => Err(detail),
            }
        })
    }
}

#[cfg(test)]
mod tests;
