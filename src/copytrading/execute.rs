//! Phase 4: fixed-lane account/token executor and virtual lots. See
//! `docs/COPY_ENGINE_BLUEPRINT.md` section 9.
//!
//! Only this module's functions may read or write `position_lots` and
//! reservations (`copy_intents.reserved_qty`). Order submission itself is
//! Phase 5, which does not exist yet -- [`OrderSubmitter`] is a generic
//! seam so this phase's claim/size/reserve/finalize logic can be built and
//! tested now, against a fake submitter in tests, without any code that
//! could place a live order. There is no implementation of this trait
//! anywhere in this crate outside test code.

use std::{fmt, str::FromStr as _};

use rust_decimal::{Decimal, RoundingStrategy};
use sqlx::SqlitePool;

use crate::{
    copytrading::plan::PolicySnapshot,
    venue::{
        intl_clob::{OutcomeTokenId, StrictAccountBalanceReader},
        OrderReceipt,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }

    pub(crate) fn from_str(raw: &str) -> Option<Self> {
        match raw {
            "BUY" => Some(Self::Buy),
            "SELL" => Some(Self::Sell),
            _ => None,
        }
    }
}

/// One already-sized, already-priced decision ready to submit. Everything
/// on this struct is already durably persisted on `copy_intents` by the
/// time a caller has one -- Phase 5's real submitter reads it back from
/// there, not from an in-memory value that a crash could lose.
#[derive(Debug, Clone)]
pub struct SizedDecision {
    pub intent_id: i64,
    pub token_id: String,
    pub side: Side,
    pub qty: Decimal,
    pub limit_price: Decimal,
}

/// What Phase 4 needs from Phase 5: submit one already-decided order and
/// return its receipt, or fail. **No implementation of this trait exists
/// in this crate's non-test code.** A real implementation would construct,
/// sign, and submit a live order -- exactly the action this project's
/// assistant will never perform; that code is Phase 5's, written and run
/// only by the account owner when they choose to.
pub trait OrderSubmitter {
    fn submit(
        &self,
        decision: &SizedDecision,
    ) -> impl std::future::Future<Output = Result<OrderReceipt, String>> + Send;
}

/// Runs one intent all the way through: claim (or resume), size and
/// reserve, submit via `submitter`, then finalize from the receipt.
/// Returns `Ok(None)` if the intent could not be claimed (already claimed
/// by another lane, not pending, or does not exist) -- not an error, since
/// that is the expected outcome of the "single-in-flight" race this
/// function is itself part of protecting.
pub async fn execute_intent<B, S>(
    pool: &SqlitePool,
    balance_reader: &B,
    submitter: &S,
    intent_id: i64,
) -> Result<Option<ExecutionOutcome>, ExecuteError>
where
    B: StrictAccountBalanceReader,
    S: OrderSubmitter,
{
    let Some(claimed) = claim_or_resume_intent(pool, intent_id).await? else {
        return Ok(None);
    };

    let decision = match size_and_reserve(pool, balance_reader, &claimed).await? {
        SizingOutcome::Decision(decision) => decision,
        SizingOutcome::NeedsReconcile(reason) => {
            open_reconciliation_case(pool, &claimed, reason).await?;
            return Ok(Some(ExecutionOutcome::NeedsReconcile(reason)));
        }
        SizingOutcome::Expired => {
            cancel_expired_intent(pool, claimed.intent_id).await?;
            return Ok(Some(ExecutionOutcome::Expired));
        }
        SizingOutcome::Rejected(reason) => {
            reject_pre_submit_intent(pool, claimed.intent_id, reason).await?;
            return Ok(Some(ExecutionOutcome::Rejected(reason)));
        }
    };

    let receipt = submitter
        .submit(&decision)
        .await
        .map_err(ExecuteError::Submission)?;

    let next_attempt_number = next_attempt_number(pool, decision.intent_id).await?;
    let attempt_id = record_attempt(pool, &decision, next_attempt_number, &receipt).await?;
    finalize_receipt(pool, decision.intent_id, attempt_id, &receipt).await?;

    Ok(Some(ExecutionOutcome::Filled {
        filled_qty: receipt.filled_qty(),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionOutcome {
    Filled { filled_qty: Decimal },
    NeedsReconcile(&'static str),
    Expired,
    Rejected(&'static str),
}

pub struct ClaimedIntent {
    pub intent_id: i64,
    pub account_id: i64,
    pub leader_id: i64,
    pub token_id: String,
    pub side: Side,
    pub decision_deadline_at: Option<String>,
    /// Set only when this claim is resuming an intent that already has a
    /// persisted decision from an earlier attempt (a crash-recovery case,
    /// blueprint: "A recovery never recalculates a persisted decision").
    pub existing_decision: Option<(Decimal, Decimal, Decimal)>,
}

/// Claims a `pending` intent (compare-and-set to `in_progress`, the
/// single-in-flight guarantee), or -- if it is already `in_progress` --
/// resumes it as-is rather than claiming it again. Either way returns the
/// row needed to size or reuse a decision; `None` if the intent does not
/// exist or is in a terminal state.
/// (account_id, leader_id, token_id, side, decision_deadline_at,
/// planned_qty, planned_price, planned_notional_usdc)
type IntentRow = (
    i64,
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

pub async fn claim_or_resume_intent(
    pool: &SqlitePool,
    intent_id: i64,
) -> Result<Option<ClaimedIntent>, ExecuteError> {
    let claimed: Option<IntentRow> = sqlx::query_as(
        "UPDATE copy_intents SET status = 'in_progress', \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND status = 'pending' \
         RETURNING account_id, leader_id, token_id, side, decision_deadline_at, \
         planned_qty, planned_price, planned_notional_usdc",
    )
    .bind(intent_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    let row = match claimed {
        Some(row) => row,
        None => {
            // Not pending: either already in_progress (resume) or a
            // terminal/nonexistent id (nothing to do).
            let resumable: Option<IntentRow> = sqlx::query_as(
                "SELECT account_id, leader_id, token_id, side, decision_deadline_at, \
                     planned_qty, planned_price, planned_notional_usdc \
                     FROM copy_intents WHERE id = ? AND status = 'in_progress'",
            )
            .bind(intent_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| ExecuteError::Database(error.to_string()))?;
            match resumable {
                Some(row) => row,
                None => return Ok(None),
            }
        }
    };

    let (
        account_id,
        leader_id,
        token_id,
        side,
        decision_deadline_at,
        planned_qty,
        planned_price,
        planned_notional_usdc,
    ) = row;
    let side = Side::from_str(&side).ok_or(ExecuteError::InvalidSide)?;
    let existing_decision = match (planned_qty, planned_price, planned_notional_usdc) {
        (Some(qty), Some(price), Some(notional)) => Some((
            qty.parse()
                .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.planned_qty"))?,
            price
                .parse()
                .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.planned_price"))?,
            notional
                .parse()
                .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.planned_notional_usdc"))?,
        )),
        (None, None, None) => None,
        _ => {
            return Err(ExecuteError::InvalidDecimal(
                "incomplete persisted decision",
            ))
        }
    };

    Ok(Some(ClaimedIntent {
        intent_id,
        account_id,
        leader_id,
        token_id,
        side,
        decision_deadline_at,
        existing_decision,
    }))
}

pub enum SizingOutcome {
    Decision(SizedDecision),
    NeedsReconcile(&'static str),
    Expired,
    Rejected(&'static str),
}

/// Sizes (or reuses an already-persisted decision for) one claimed intent.
/// Strict venue data is read before any write transaction begins (the
/// executor must not hold an SQLite write transaction across network I/O);
/// the reservation itself is one short transaction.
pub async fn size_and_reserve<B: StrictAccountBalanceReader>(
    pool: &SqlitePool,
    balance_reader: &B,
    claimed: &ClaimedIntent,
) -> Result<SizingOutcome, ExecuteError> {
    // A persisted decision is immutable, but it is not exempt from the FAK
    // deadline. A restart must never turn an expired decision into a late
    // submission.
    if let Some(deadline) = &claimed.decision_deadline_at {
        if let Ok(deadline) = chrono::DateTime::parse_from_rfc3339(deadline) {
            if chrono::Utc::now() > deadline {
                return Ok(SizingOutcome::Expired);
            }
        }
    }

    if let Some((qty, price, _notional)) = claimed.existing_decision {
        return Ok(SizingOutcome::Decision(SizedDecision {
            intent_id: claimed.intent_id,
            token_id: claimed.token_id.clone(),
            side: claimed.side,
            qty,
            limit_price: price,
        }));
    }

    let policy = load_policy_snapshot(pool, claimed.intent_id).await?;
    let tick_size: Decimal = policy
        .tick_size
        .parse()
        .map_err(|_| ExecuteError::InvalidDecimal("leader_policy.tick_size"))?;

    let (qty, limit_price) = match claimed.side {
        Side::Sell => {
            let leader_lot = load_position_lot(
                pool,
                claimed.account_id,
                claimed.leader_id,
                &claimed.token_id,
            )
            .await?;
            // A leader can sell a token that this follower never mirrored.
            // That is an expected no-op, not evidence that a strict venue
            // balance read returned a false zero. Do not issue a balance read
            // or open a reconciliation case when no virtual lot exists.
            if leader_lot <= Decimal::ZERO {
                return Ok(SizingOutcome::Rejected(
                    "no tracked virtual lot for leader sell",
                ));
            }
            let token = OutcomeTokenId::from_str(&claimed.token_id)
                .map_err(|_| ExecuteError::InvalidTokenId)?;
            let strict_available = match balance_reader.position_for_token_strict(&token).await {
                Ok(balance) => balance,
                Err(_) => {
                    return Ok(SizingOutcome::NeedsReconcile(
                        "strict token balance query failed",
                    ))
                }
            };
            let other_reservations = sum_other_active_reservations(
                pool,
                claimed.account_id,
                &claimed.token_id,
                claimed.intent_id,
            )
            .await?;
            let account_sellable = (strict_available - other_reservations).max(Decimal::ZERO);
            let sell_qty =
                round_order_qty_down(leader_lot.min(account_sellable).max(Decimal::ZERO));
            if sell_qty <= Decimal::ZERO {
                // A nonzero tracked lot but no strict sellable balance is a
                // genuine discrepancy. Unlike a missing virtual lot above,
                // it must remain blocked for reconciliation.
                return Ok(SizingOutcome::NeedsReconcile(
                    "computed sell quantity is not positive",
                ));
            }
            let event_price = load_event_price(pool, claimed.intent_id).await?;
            let price = round_price(
                apply_tolerance(event_price, policy.price_tolerance_bps, claimed.side),
                tick_size,
                claimed.side,
            );
            (sell_qty, price)
        }
        Side::Buy => {
            let event_size = load_event_size(pool, claimed.intent_id).await?;
            let event_price = load_event_price(pool, claimed.intent_id).await?;
            let limit_price = round_price(
                apply_tolerance(event_price, policy.price_tolerance_bps, claimed.side),
                tick_size,
                claimed.side,
            );
            let strict_collateral = match balance_reader.collateral_balance_strict().await {
                Ok(balance) => balance,
                Err(_) => {
                    return Ok(SizingOutcome::NeedsReconcile(
                        "strict collateral balance query failed",
                    ))
                }
            };
            let strict_allowance = match balance_reader.collateral_allowance_strict().await {
                Ok(allowance) => allowance,
                Err(_) => {
                    return Ok(SizingOutcome::NeedsReconcile(
                        "strict collateral allowance query failed",
                    ))
                }
            };
            let other_buy_notional = sum_other_active_buy_reservation_notional(
                pool,
                claimed.account_id,
                claimed.intent_id,
            )
            .await?;
            let available_collateral =
                (strict_collateral.min(strict_allowance) - other_buy_notional).max(Decimal::ZERO);
            let max_notional: Decimal = policy
                .max_order_notional
                .parse()
                .map_err(|_| ExecuteError::InvalidDecimal("leader_policy.max_order_notional"))?;
            let order_notional = max_notional.min(available_collateral);
            let notional_capped_qty = if limit_price > Decimal::ZERO {
                order_notional / limit_price
            } else {
                Decimal::ZERO
            };
            let qty = round_order_qty_down(event_size.min(notional_capped_qty));
            if qty <= Decimal::ZERO {
                return Ok(SizingOutcome::NeedsReconcile(
                    "computed buy quantity is not positive",
                ));
            }
            (qty, limit_price)
        }
    };

    let planned_notional = qty * limit_price;
    // The CLOB refuses a marketable BUY below one USDC. With the configured
    // cap at exactly one USDC, quantity truncation can make a candidate such
    // as 1.72 * 0.58 = 0.9976. It is a deterministic local policy rejection,
    // never a reason to cross the order-submission boundary.
    if claimed.side == Side::Buy && planned_notional < Decimal::ONE {
        return Ok(SizingOutcome::Rejected(
            "computed buy notional is below the CLOB minimum of 1 USDC",
        ));
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))?;
    let updated = sqlx::query(
        "UPDATE copy_intents SET planned_qty = ?, planned_price = ?, tick_size = ?, \
         time_in_force = 'FAK', reserved_qty = ?, planned_notional_usdc = ?, \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND status = 'in_progress'",
    )
    .bind(qty.to_string())
    .bind(limit_price.to_string())
    .bind(policy.tick_size.clone())
    .bind(qty.to_string())
    .bind(planned_notional.to_string())
    .bind(claimed.intent_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    tx.commit()
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))?;

    if updated.rows_affected() == 0 {
        // Revalidation failed: the intent moved out of in_progress under us
        // (e.g. a concurrent cancellation). Treat as nothing to do rather
        // than proceeding on a stale claim.
        return Ok(SizingOutcome::NeedsReconcile(
            "intent state changed during sizing",
        ));
    }

    Ok(SizingOutcome::Decision(SizedDecision {
        intent_id: claimed.intent_id,
        token_id: claimed.token_id.clone(),
        side: claimed.side,
        qty,
        limit_price,
    }))
}

fn apply_tolerance(event_price: Decimal, tolerance_bps: i64, side: Side) -> Decimal {
    let tolerance = event_price * Decimal::new(tolerance_bps, 4); // bps / 10_000
    match side {
        // Willing to pay slightly more than the leader did, to raise the
        // odds an FAK buy actually crosses the spread.
        Side::Buy => event_price + tolerance,
        // Willing to accept slightly less than the leader did.
        Side::Sell => (event_price - tolerance).max(Decimal::ZERO),
    }
}

/// CLOB orders accept at most two decimal places of outcome-token shares.
/// Always truncate a positive candidate instead of rounding it up: a BUY
/// must never exceed its already-persisted notional cap, and a SELL must
/// never exceed its confirmed available position.
fn round_order_qty_down(qty: Decimal) -> Decimal {
    qty.round_dp_with_strategy(2, RoundingStrategy::ToZero)
}

fn round_price(price: Decimal, tick_size: Decimal, side: Side) -> Decimal {
    if tick_size <= Decimal::ZERO {
        return price;
    }
    let ticks = price / tick_size;
    let rounded_ticks = match side {
        // Round toward a price this side is still willing to accept: up
        // for a BUY's ceiling, down for a SELL's floor.
        Side::Buy => ticks.ceil(),
        Side::Sell => ticks.floor(),
    };
    rounded_ticks * tick_size
}

async fn load_policy_snapshot(
    pool: &SqlitePool,
    intent_id: i64,
) -> Result<PolicySnapshot, ExecuteError> {
    let json: String =
        sqlx::query_scalar("SELECT config_snapshot_json FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(pool)
            .await
            .map_err(|error| ExecuteError::Database(error.to_string()))?;
    serde_json::from_str(&json)
        .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.config_snapshot_json"))
}

async fn load_position_lot(
    pool: &SqlitePool,
    account_id: i64,
    leader_id: i64,
    token_id: &str,
) -> Result<Decimal, ExecuteError> {
    let qty: Option<String> = sqlx::query_scalar(
        "SELECT qty FROM position_lots WHERE account_id = ? AND leader_id = ? AND token_id = ?",
    )
    .bind(account_id)
    .bind(leader_id)
    .bind(token_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    match qty {
        Some(qty) => qty
            .parse()
            .map_err(|_| ExecuteError::InvalidDecimal("position_lots.qty")),
        None => Ok(Decimal::ZERO),
    }
}

/// Sums other active BUY reservations as account-level collateral notional,
/// excluding the current intent so crash recovery does not shrink an already
/// persisted decision. `reserved_qty` remains the order size; BUY collateral
/// usage is computed from `reserved_qty * planned_price`.
async fn sum_other_active_buy_reservation_notional(
    pool: &SqlitePool,
    account_id: i64,
    exclude_intent_id: i64,
) -> Result<Decimal, ExecuteError> {
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT reserved_qty, planned_price FROM copy_intents \
         WHERE account_id = ? AND id != ? AND side = 'BUY' \
         AND status IN ('in_progress', 'partially_filled')",
    )
    .bind(account_id)
    .bind(exclude_intent_id)
    .fetch_all(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    let mut total = Decimal::ZERO;
    for (reserved_qty, planned_price) in rows {
        let reserved_qty = reserved_qty
            .parse::<Decimal>()
            .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.reserved_qty"))?;
        let planned_price = planned_price
            .ok_or(ExecuteError::InvalidDecimal("copy_intents.planned_price"))?
            .parse::<Decimal>()
            .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.planned_price"))?;
        total += reserved_qty * planned_price;
    }
    Ok(total)
}

/// Sums other *currently active* intents' reservations for this
/// account/token, excluding this intent's own reservation (so recovery
/// does not shrink its original sale). Summed in Rust from exact decimal
/// text, never via SQL SUM, which would round through floating point.
async fn sum_other_active_reservations(
    pool: &SqlitePool,
    account_id: i64,
    token_id: &str,
    exclude_intent_id: i64,
) -> Result<Decimal, ExecuteError> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT reserved_qty FROM copy_intents \
         WHERE account_id = ? AND token_id = ? AND id != ? \
         AND status IN ('in_progress', 'partially_filled')",
    )
    .bind(account_id)
    .bind(token_id)
    .bind(exclude_intent_id)
    .fetch_all(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    let mut total = Decimal::ZERO;
    for row in rows {
        total += row
            .parse::<Decimal>()
            .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.reserved_qty"))?;
    }
    Ok(total)
}

async fn load_event_price(pool: &SqlitePool, intent_id: i64) -> Result<Decimal, ExecuteError> {
    let price: String = sqlx::query_scalar(
        "SELECT le.price FROM leader_events le JOIN copy_intents ci ON ci.event_id = le.id WHERE ci.id = ?",
    )
    .bind(intent_id)
    .fetch_one(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    price
        .parse()
        .map_err(|_| ExecuteError::InvalidDecimal("leader_events.price"))
}

async fn load_event_size(pool: &SqlitePool, intent_id: i64) -> Result<Decimal, ExecuteError> {
    let size: String = sqlx::query_scalar(
        "SELECT le.size FROM leader_events le JOIN copy_intents ci ON ci.event_id = le.id WHERE ci.id = ?",
    )
    .bind(intent_id)
    .fetch_one(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    size.parse()
        .map_err(|_| ExecuteError::InvalidDecimal("leader_events.size"))
}

pub async fn open_reconciliation_case(
    pool: &SqlitePool,
    claimed: &ClaimedIntent,
    reason: &'static str,
) -> Result<(), ExecuteError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))?;
    sqlx::query(
        "UPDATE copy_intents SET status = 'needs_reconcile', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
    )
    .bind(claimed.intent_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    sqlx::query(
        "INSERT INTO reconciliation_cases (account_id, token_id, intent_id, case_type, detail) \
         VALUES (?, ?, ?, 'balance_drift', ?)",
    )
    .bind(claimed.account_id)
    .bind(&claimed.token_id)
    .bind(claimed.intent_id)
    .bind(reason)
    .execute(&mut *tx)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    tx.commit()
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))
}

pub async fn cancel_expired_intent(pool: &SqlitePool, intent_id: i64) -> Result<(), ExecuteError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))?;

    // A prepared attempt without the durable submit marker has not crossed
    // the order-submission boundary, so expiry may close it safely. Attempts
    // marked submitting or later remain for reconciliation.
    sqlx::query(
        "UPDATE order_attempts SET status = 'rejected', \
         failure_detail = 'decision deadline expired before submission', \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE intent_id = ? AND status = 'prepared' AND submission_started_at IS NULL",
    )
    .bind(intent_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    sqlx::query(
        "UPDATE copy_intents SET status = 'cancelled', rejection_reason = 'decision deadline expired', \
         reserved_qty = '0', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
    )
    .bind(intent_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    tx.commit()
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))
}

pub async fn reject_pre_submit_intent(
    pool: &SqlitePool,
    intent_id: i64,
    reason: &str,
) -> Result<(), ExecuteError> {
    sqlx::query(
        "UPDATE copy_intents SET status = 'rejected', rejection_reason = ?, reserved_qty = '0', \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND status = 'in_progress'",
    )
    .bind(reason)
    .bind(intent_id)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| ExecuteError::Database(error.to_string()))
}

/// Closes one already-overdue intent only when the database proves that no
/// order crossed the submission boundary. This is an operator recovery for a
/// process that stopped after sizing but before it could observe its own FAK
/// deadline; it performs no venue I/O.
pub async fn cancel_overdue_pre_submit_intent(
    pool: &SqlitePool,
    account_id: i64,
    intent_id: i64,
) -> Result<(), ExecuteError> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT status, decision_deadline_at FROM copy_intents WHERE id = ? AND account_id = ?",
    )
    .bind(intent_id)
    .bind(account_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    let Some((status, deadline)) = row else {
        return Err(ExecuteError::Database(
            "intent does not belong to this account".to_owned(),
        ));
    };
    if status != "in_progress" {
        return Err(ExecuteError::Database(
            "intent is not an in-progress pre-submit intent".to_owned(),
        ));
    }
    let overdue = deadline
        .as_deref()
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .is_some_and(|value| chrono::Utc::now() > value);
    if !overdue {
        return Err(ExecuteError::Database(
            "intent deadline is absent, malformed, or not yet expired".to_owned(),
        ));
    }
    let unsafe_attempt: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM order_attempts WHERE intent_id = ? \
         AND (status <> 'prepared' OR submission_started_at IS NOT NULL))",
    )
    .bind(intent_id)
    .fetch_one(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    if unsafe_attempt != 0 {
        return Err(ExecuteError::Database(
            "intent has an attempt that may have crossed the submission boundary".to_owned(),
        ));
    }
    cancel_expired_intent(pool, intent_id).await
}

pub async fn next_attempt_number(pool: &SqlitePool, intent_id: i64) -> Result<i64, ExecuteError> {
    let max: Option<i64> =
        sqlx::query_scalar("SELECT MAX(attempt_number) FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id)
            .fetch_one(pool)
            .await
            .map_err(|error| ExecuteError::Database(error.to_string()))?;
    Ok(max.unwrap_or(0) + 1)
}

async fn record_attempt(
    pool: &SqlitePool,
    decision: &SizedDecision,
    attempt_number: i64,
    receipt: &OrderReceipt,
) -> Result<i64, ExecuteError> {
    let envelope_json = format!(
        "{{\"token_id\":\"{}\",\"side\":\"{}\",\"qty\":\"{}\",\"limit_price\":\"{}\"}}",
        decision.token_id,
        decision.side.as_str(),
        decision.qty,
        decision.limit_price
    );
    sqlx::query_scalar(
        "INSERT INTO order_attempts \
         (intent_id, attempt_number, envelope_json, status, requested_qty, accepted_qty, \
          filled_qty, remaining_qty) \
         VALUES (?, ?, ?, 'finalized', ?, ?, ?, ?) RETURNING id",
    )
    .bind(decision.intent_id)
    .bind(attempt_number)
    .bind(envelope_json)
    .bind(receipt.requested_qty().to_string())
    .bind(receipt.accepted_qty().to_string())
    .bind(receipt.filled_qty().to_string())
    .bind(receipt.remaining_qty().to_string())
    .fetch_one(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))
}

/// Applies only the *newly confirmed* fill delta to `position_lots`,
/// updates `accounted_filled_qty`, releases the reservation, and finalizes
/// the intent -- in one short transaction. Idempotent: replaying the same
/// receipt (including after a crash between the external response and this
/// commit) leaves the lot unchanged on the second pass, because the delta
/// is computed against the already-accounted amount, not applied blindly.
pub async fn finalize_receipt(
    pool: &SqlitePool,
    intent_id: i64,
    attempt_id: i64,
    receipt: &OrderReceipt,
) -> Result<(), ExecuteError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))?;

    let row: (i64, i64, String, String, String) = sqlx::query_as(
        "SELECT ci.account_id, ci.leader_id, ci.token_id, ci.side, oa.accounted_filled_qty \
         FROM copy_intents ci JOIN order_attempts oa ON oa.id = ? WHERE ci.id = ?",
    )
    .bind(attempt_id)
    .bind(intent_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    let (account_id, leader_id, token_id, side, accounted_so_far) = row;
    let side = Side::from_str(&side).ok_or(ExecuteError::InvalidSide)?;
    let accounted_so_far: Decimal = accounted_so_far
        .parse()
        .map_err(|_| ExecuteError::InvalidDecimal("order_attempts.accounted_filled_qty"))?;

    let delta = (receipt.filled_qty() - accounted_so_far).max(Decimal::ZERO);
    let new_accounted = accounted_so_far + delta;

    if delta > Decimal::ZERO {
        let lot_delta = match side {
            Side::Buy => delta,
            Side::Sell => -delta,
        };
        // SQLite has no native decimal arithmetic, so the += happens in
        // Rust: read the current lot (if any) within this same short
        // transaction, then write the exact computed sum back.
        let current_qty: Option<String> = sqlx::query_scalar(
            "SELECT qty FROM position_lots WHERE account_id = ? AND leader_id = ? AND token_id = ?",
        )
        .bind(account_id)
        .bind(leader_id)
        .bind(&token_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))?;
        let current_qty: Decimal = match current_qty {
            Some(qty) => qty
                .parse()
                .map_err(|_| ExecuteError::InvalidDecimal("position_lots.qty"))?,
            None => Decimal::ZERO,
        };
        let new_qty = current_qty + lot_delta;

        sqlx::query(
            "INSERT INTO position_lots (account_id, leader_id, token_id, qty, updated_at) \
             VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
             ON CONFLICT(account_id, leader_id, token_id) DO UPDATE SET \
             qty = excluded.qty, updated_at = excluded.updated_at",
        )
        .bind(account_id)
        .bind(leader_id)
        .bind(&token_id)
        .bind(new_qty.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))?;
    }

    sqlx::query(
        "UPDATE order_attempts SET accounted_filled_qty = ?, receipt_json = ? WHERE id = ?",
    )
    .bind(new_accounted.to_string())
    .bind(format!(
        "{{\"requested\":\"{}\",\"accepted\":\"{}\",\"filled\":\"{}\",\"remaining\":\"{}\"}}",
        receipt.requested_qty(),
        receipt.accepted_qty(),
        receipt.filled_qty(),
        receipt.remaining_qty()
    ))
    .bind(attempt_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    let intent_status = if receipt.remaining_qty() > Decimal::ZERO && delta > Decimal::ZERO {
        "partially_filled"
    } else {
        "completed"
    };
    sqlx::query(
        "UPDATE copy_intents SET status = ?, reserved_qty = '0', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
    )
    .bind(intent_status)
    .bind(intent_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    tx.commit()
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))
}

#[derive(Debug)]
pub enum ExecuteError {
    Database(String),
    Submission(String),
    InvalidSide,
    InvalidTokenId,
    InvalidDecimal(&'static str),
}

impl fmt::Display for ExecuteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::Submission(error) => write!(formatter, "submission error: {error}"),
            Self::InvalidSide => write!(formatter, "invalid side stored on copy_intents"),
            Self::InvalidTokenId => write!(formatter, "invalid token id"),
            Self::InvalidDecimal(field) => write!(formatter, "invalid decimal value in {field}"),
        }
    }
}

impl std::error::Error for ExecuteError {}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use polymarket_client_sdk_v2::error::Error as SdkError;
    use sqlx::Row as _;

    use super::*;

    #[test]
    fn order_quantity_is_truncated_to_two_decimals_without_exceeding_buy_budget() {
        let price = Decimal::new(3, 1); // 0.3
        let capped = round_order_qty_down(Decimal::ONE / price);
        assert_eq!(capped, Decimal::new(333, 2));
        assert!(capped * price <= Decimal::ONE);
        assert_eq!(
            round_order_qty_down(Decimal::new(1999, 3)),
            Decimal::new(199, 2),
            "a sell quantity is also truncated, never rounded above availability"
        );
    }
    use crate::{
        copytrading::db::open_and_migrate,
        venue::intl_clob::{StrictCollateralError, StrictPositionError, StrictTokenBalanceReader},
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
                "polycopy-engine-execute-test-{}-{nonce}-{counter}.sqlite",
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

    struct FixedBalanceReader {
        token: Decimal,
        collateral: Decimal,
        allowance: Decimal,
    }

    impl FixedBalanceReader {
        fn new(token: Decimal, collateral: Decimal) -> Self {
            Self {
                token,
                collateral,
                allowance: collateral,
            }
        }

        fn with_allowance(token: Decimal, collateral: Decimal, allowance: Decimal) -> Self {
            Self {
                token,
                collateral,
                allowance,
            }
        }
    }

    #[async_trait]
    impl StrictTokenBalanceReader for FixedBalanceReader {
        async fn position_for_token_strict(
            &self,
            _token_id: &OutcomeTokenId,
        ) -> Result<Decimal, StrictPositionError> {
            Ok(self.token)
        }
    }

    #[async_trait]
    impl StrictAccountBalanceReader for FixedBalanceReader {
        async fn collateral_balance_strict(&self) -> Result<Decimal, StrictCollateralError> {
            Ok(self.collateral)
        }

        async fn collateral_allowance_strict(&self) -> Result<Decimal, StrictCollateralError> {
            Ok(self.allowance)
        }
    }

    struct FailingBalanceReader;

    #[async_trait]
    impl StrictTokenBalanceReader for FailingBalanceReader {
        async fn position_for_token_strict(
            &self,
            token_id: &OutcomeTokenId,
        ) -> Result<Decimal, StrictPositionError> {
            Err(StrictPositionError::Query {
                token_id: token_id.clone(),
                source: SdkError::validation("mock balance query failure"),
            })
        }
    }

    #[async_trait]
    impl StrictAccountBalanceReader for FailingBalanceReader {
        async fn collateral_balance_strict(&self) -> Result<Decimal, StrictCollateralError> {
            Err(StrictCollateralError::Query {
                source: SdkError::validation("mock collateral query failure"),
            })
        }

        async fn collateral_allowance_strict(&self) -> Result<Decimal, StrictCollateralError> {
            Err(StrictCollateralError::Query {
                source: SdkError::validation("mock collateral allowance query failure"),
            })
        }
    }

    /// **No implementation of `OrderSubmitter` exists in this crate outside
    /// this test module.** This fills orders exactly as requested; it is
    /// never wired to a live venue.
    struct FullFillSubmitter;

    impl OrderSubmitter for FullFillSubmitter {
        async fn submit(&self, decision: &SizedDecision) -> Result<OrderReceipt, String> {
            match decision.side {
                Side::Buy => {
                    OrderReceipt::from_fak_buy_budget(decision.qty, decision.qty, decision.qty)
                }
                Side::Sell => {
                    OrderReceipt::from_fak_sell_shares(decision.qty, decision.qty, decision.qty)
                }
            }
            .map_err(|error| error.to_string())
        }
    }

    async fn seed_account_and_schedule(db: &TestDb) {
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&**db)
        .await
        .expect("account must insert");
        sqlx::query("INSERT INTO execution_schedule (id, shard_scheme_version, shard_algorithm, lane_count) VALUES (1, 1, 'hash_mod_lane_count', 1)")
            .execute(&**db)
            .await
            .expect("execution_schedule must insert");
    }

    async fn seed_leader(db: &TestDb, leader_id: i64) {
        sqlx::query("INSERT INTO leader_config (id, label, enabled) VALUES (?, ?, 1)")
            .bind(leader_id)
            .bind(format!("leader-{leader_id}"))
            .execute(&**db)
            .await
            .expect("leader must insert");
    }

    /// Inserts one already-`pending` copy_intent directly (bypassing the
    /// planner, which is tested separately) with a real leader_event and
    /// policy snapshot behind it, so the executor has everything it needs.
    async fn seed_pending_intent(
        db: &TestDb,
        leader_id: i64,
        token_id: &str,
        side: &str,
        event_size: &str,
        event_price: &str,
    ) -> i64 {
        let event_id: i64 = sqlx::query_scalar(
            "INSERT INTO leader_events \
             (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, price, occurred_at, observed_at) \
             VALUES (?, ?, '0xcond', ?, 0, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now')) \
             RETURNING id",
        )
        .bind(format!("activity:{leader_id}:{token_id}:{side}:{event_size}:{}", uuid_like()))
        .bind(leader_id)
        .bind(token_id)
        .bind(side)
        .bind(event_size)
        .bind(event_price)
        .fetch_one(&**db)
        .await
        .expect("event must insert");

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
        let snapshot_json = serde_json::to_string(&snapshot).unwrap();

        sqlx::query_scalar(
            "INSERT INTO copy_intents \
             (event_id, account_id, leader_id, token_id, side, config_snapshot_json, config_snapshot_hash, \
              shard_scheme_version, lane_count, shard_id, status, decision_deadline_at) \
             VALUES (?, 1, ?, ?, ?, ?, 'hash', 1, 1, 0, 'pending', ?) RETURNING id",
        )
        .bind(event_id)
        .bind(leader_id)
        .bind(token_id)
        .bind(side)
        .bind(snapshot_json)
        .bind((chrono::Utc::now() + chrono::Duration::seconds(300)).to_rfc3339())
        .fetch_one(&**db)
        .await
        .expect("intent must insert")
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

    async fn lot_qty(db: &TestDb, leader_id: i64, token_id: &str) -> Decimal {
        let qty: Option<String> = sqlx::query_scalar(
            "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = ? AND token_id = ?",
        )
        .bind(leader_id)
        .bind(token_id)
        .fetch_optional(&**db)
        .await
        .expect("query must succeed");
        qty.map(|q| q.parse().unwrap()).unwrap_or(Decimal::ZERO)
    }

    #[tokio::test]
    async fn two_leaders_buying_one_token_retain_distinct_virtual_lots() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        seed_leader(&db, 2).await;
        let intent_1 = seed_pending_intent(&db, 1, "123456", "BUY", "5", "0.50").await;
        let intent_2 = seed_pending_intent(&db, 2, "123456", "BUY", "3", "0.50").await;

        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
        let submitter = FullFillSubmitter;
        execute_intent(&db, &balance_reader, &submitter, intent_1)
            .await
            .unwrap();
        execute_intent(&db, &balance_reader, &submitter, intent_2)
            .await
            .unwrap();

        assert_eq!(lot_qty(&db, 1, "123456").await, Decimal::new(5, 0));
        assert_eq!(lot_qty(&db, 2, "123456").await, Decimal::new(3, 0));
    }

    #[tokio::test]
    async fn multiple_leaders_interleaved_buy_and_sell_keep_exact_per_leader_lot_attribution() {
        // Phase 7 required test: same account/token, multiple leaders,
        // interleaved BUY/SELL, exact virtual-lot attribution. Every SELL
        // here fully exits the leader's own lot (sell_all_on_exit --
        // `sell_qty = min(leader_lot, account_sellable)`, not the leader
        // event's own size), which this test also locks in: leader 1's
        // SELL events below request "1" but must exit the leader's whole
        // lot regardless.
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        seed_leader(&db, 2).await;
        let submitter = FullFillSubmitter;
        let generous_balance = FixedBalanceReader::new(Decimal::new(100, 0), Decimal::new(100, 0));

        let l1_buy_1 = seed_pending_intent(&db, 1, "123456", "BUY", "5", "0.50").await;
        execute_intent(&db, &generous_balance, &submitter, l1_buy_1)
            .await
            .unwrap();
        assert_eq!(lot_qty(&db, 1, "123456").await, Decimal::new(5, 0));
        assert_eq!(lot_qty(&db, 2, "123456").await, Decimal::ZERO);

        let l2_buy_1 = seed_pending_intent(&db, 2, "123456", "BUY", "3", "0.50").await;
        execute_intent(&db, &generous_balance, &submitter, l2_buy_1)
            .await
            .unwrap();
        assert_eq!(
            lot_qty(&db, 1, "123456").await,
            Decimal::new(5, 0),
            "leader 2's buy must never touch leader 1's lot"
        );
        assert_eq!(lot_qty(&db, 2, "123456").await, Decimal::new(3, 0));

        let l1_sell_1 = seed_pending_intent(&db, 1, "123456", "SELL", "1", "0.50").await;
        execute_intent(&db, &generous_balance, &submitter, l1_sell_1)
            .await
            .unwrap();
        assert_eq!(
            lot_qty(&db, 1, "123456").await,
            Decimal::ZERO,
            "sell_all_on_exit: leader 1's SELL fully exits its 5-share lot, not just 1"
        );
        assert_eq!(
            lot_qty(&db, 2, "123456").await,
            Decimal::new(3, 0),
            "leader 1's sell must never touch leader 2's lot"
        );

        let l2_buy_2 = seed_pending_intent(&db, 2, "123456", "BUY", "2", "0.50").await;
        execute_intent(&db, &generous_balance, &submitter, l2_buy_2)
            .await
            .unwrap();
        assert_eq!(lot_qty(&db, 1, "123456").await, Decimal::ZERO);
        assert_eq!(lot_qty(&db, 2, "123456").await, Decimal::new(5, 0));

        let l2_sell_1 = seed_pending_intent(&db, 2, "123456", "SELL", "1", "0.50").await;
        execute_intent(&db, &generous_balance, &submitter, l2_sell_1)
            .await
            .unwrap();
        assert_eq!(
            lot_qty(&db, 1, "123456").await,
            Decimal::ZERO,
            "leader 2's sell must never touch leader 1's (already-zero) lot"
        );
        assert_eq!(lot_qty(&db, 2, "123456").await, Decimal::ZERO);
    }

    #[tokio::test]
    async fn a_second_leaders_sell_cannot_oversell_while_the_first_is_still_uncertain() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        seed_leader(&db, 2).await;
        // Both leaders' followers hold a virtual lot of 10 in this token
        // (e.g. both were mirrored in earlier, already-finalized buys).
        sqlx::query("INSERT INTO position_lots (account_id, leader_id, token_id, qty) VALUES (1, 1, '123456', '10'), (1, 2, '123456', '10')")
            .execute(&*db)
            .await
            .expect("lots must insert");
        let intent_1 = seed_pending_intent(&db, 1, "123456", "SELL", "10", "0.50").await;
        let intent_2 = seed_pending_intent(&db, 2, "123456", "SELL", "10", "0.50").await;

        // The account strictly holds only 12 real tokens total -- not
        // enough to cover both leaders' full 10+10 if summed naively.
        let balance_reader = FixedBalanceReader::new(Decimal::new(12, 0), Decimal::new(100, 0));

        // Leader 1's sell claims and reserves, but is deliberately left
        // "uncertain" (reserved, not yet finalized) rather than calling
        // execute_intent, which would also submit+finalize it.
        let claimed_1 = claim_or_resume_intent(&db, intent_1)
            .await
            .unwrap()
            .unwrap();
        let SizingOutcome::Decision(decision_1) =
            size_and_reserve(&db, &balance_reader, &claimed_1)
                .await
                .unwrap()
        else {
            panic!("leader 1's sell must size successfully");
        };
        assert_eq!(
            decision_1.qty,
            Decimal::new(10, 0),
            "leader 1 sells its full lot: min(10 leader, 12 account)"
        );

        // Leader 2 sizes next, while leader 1's reservation of 10 is still
        // outstanding: only 12 - 10 = 2 of the strict balance remains.
        let claimed_2 = claim_or_resume_intent(&db, intent_2)
            .await
            .unwrap()
            .unwrap();
        let SizingOutcome::Decision(decision_2) =
            size_and_reserve(&db, &balance_reader, &claimed_2)
                .await
                .unwrap()
        else {
            panic!("leader 2's sell must still size successfully, just smaller");
        };
        assert_eq!(
            decision_2.qty,
            Decimal::new(2, 0),
            "leader 2 must not oversell past what leader 1's outstanding reservation leaves available"
        );
    }

    #[tokio::test]
    async fn a_sell_without_a_tracked_virtual_lot_is_rejected_without_balance_reconciliation() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        // No position_lots row at all: leader_virtual_lot is 0.
        let intent = seed_pending_intent(&db, 1, "123456", "SELL", "5", "0.50").await;

        let balance_reader = FixedBalanceReader::new(Decimal::new(100, 0), Decimal::new(100, 0));
        let submitter = FullFillSubmitter;
        let outcome = execute_intent(&db, &balance_reader, &submitter, intent)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            outcome,
            ExecutionOutcome::Rejected("no tracked virtual lot for leader sell")
        );
        let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
            .bind(intent)
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(status, "rejected");
    }

    #[tokio::test]
    async fn a_tracked_sell_lot_with_zero_strict_balance_still_needs_reconciliation() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        sqlx::query(
            "INSERT INTO position_lots (account_id, leader_id, token_id, qty) VALUES (1, 1, '123456', '5')",
        )
        .execute(&*db)
        .await
        .unwrap();
        let intent = seed_pending_intent(&db, 1, "123456", "SELL", "5", "0.50").await;
        let outcome = execute_intent(
            &db,
            &FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0)),
            &FullFillSubmitter,
            intent,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            outcome,
            ExecutionOutcome::NeedsReconcile("computed sell quantity is not positive")
        );
    }

    #[tokio::test]
    async fn a_strict_balance_query_failure_becomes_needs_reconcile_never_a_zero_balance() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        sqlx::query("INSERT INTO position_lots (account_id, leader_id, token_id, qty) VALUES (1, 1, '123456', '10')")
            .execute(&*db)
            .await
            .unwrap();
        let intent = seed_pending_intent(&db, 1, "123456", "SELL", "5", "0.50").await;

        let outcome = execute_intent(&db, &FailingBalanceReader, &FullFillSubmitter, intent)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome,
            ExecutionOutcome::NeedsReconcile("strict token balance query failed")
        );
    }

    #[tokio::test]
    async fn a_strict_collateral_query_failure_becomes_needs_reconcile_never_a_zero_balance() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "5", "0.50").await;

        let outcome = execute_intent(&db, &FailingBalanceReader, &FullFillSubmitter, intent)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome,
            ExecutionOutcome::NeedsReconcile("strict collateral balance query failed")
        );

        let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
            .bind(intent)
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(status, "needs_reconcile");
    }

    #[tokio::test]
    async fn a_buy_is_capped_by_confirmed_allowance_not_only_collateral_balance() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "20", "0.50").await;
        let balance_reader = FixedBalanceReader::with_allowance(
            Decimal::ZERO,
            Decimal::new(100, 0),
            Decimal::new(3, 0),
        );

        let claimed = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();
        let SizingOutcome::Decision(decision) = size_and_reserve(&db, &balance_reader, &claimed)
            .await
            .unwrap()
        else {
            panic!("positive confirmed allowance must produce a capped buy");
        };

        assert_eq!(decision.qty, Decimal::new(6, 0));
        assert_eq!(decision.qty * decision.limit_price, Decimal::new(3, 0));
    }

    #[tokio::test]
    async fn a_second_token_buy_cannot_overspend_collateral_reserved_by_the_first_buy() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        seed_leader(&db, 2).await;
        let intent_1 = seed_pending_intent(&db, 1, "111111", "BUY", "10", "0.50").await;
        let intent_2 = seed_pending_intent(&db, 2, "222222", "BUY", "10", "0.50").await;
        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(6, 0));

        let claimed_1 = claim_or_resume_intent(&db, intent_1)
            .await
            .unwrap()
            .unwrap();
        let SizingOutcome::Decision(decision_1) =
            size_and_reserve(&db, &balance_reader, &claimed_1)
                .await
                .unwrap()
        else {
            panic!("first buy must size successfully");
        };
        assert_eq!(decision_1.qty, Decimal::new(10, 0));
        assert_eq!(decision_1.qty * decision_1.limit_price, Decimal::new(5, 0));

        let claimed_2 = claim_or_resume_intent(&db, intent_2)
            .await
            .unwrap()
            .unwrap();
        let SizingOutcome::Decision(decision_2) =
            size_and_reserve(&db, &balance_reader, &claimed_2)
                .await
                .unwrap()
        else {
            panic!("second buy must size down to remaining collateral");
        };

        assert_eq!(
            decision_2.qty,
            Decimal::new(2, 0),
            "only 1 USDC remains after the first active 5 USDC buy reservation"
        );
        assert!(
            decision_2.qty * decision_2.limit_price <= Decimal::new(1, 0),
            "second buy notional must stay within remaining collateral"
        );
    }

    #[tokio::test]
    async fn replaying_the_same_receipt_after_a_simulated_crash_leaves_the_lot_unchanged_on_the_second_pass(
    ) {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "5", "0.50").await;

        let claimed = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();
        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
        let SizingOutcome::Decision(decision) = size_and_reserve(&db, &balance_reader, &claimed)
            .await
            .unwrap()
        else {
            panic!("must size successfully");
        };
        let receipt =
            OrderReceipt::from_fak_buy_budget(decision.qty, decision.qty, decision.qty).unwrap();
        let attempt_id = record_attempt(&db, &decision, 1, &receipt).await.unwrap();

        // First pass: applies the fill.
        finalize_receipt(&db, intent, attempt_id, &receipt)
            .await
            .unwrap();
        assert_eq!(lot_qty(&db, 1, "123456").await, Decimal::new(5, 0));

        // Second pass over the identical receipt (as if the process crashed
        // between the venue response and the first commit, and this is a
        // recovery replay): must not double-apply.
        finalize_receipt(&db, intent, attempt_id, &receipt)
            .await
            .unwrap();
        assert_eq!(
            lot_qty(&db, 1, "123456").await,
            Decimal::new(5, 0),
            "a replayed receipt must not double-apply"
        );
    }

    #[tokio::test]
    async fn a_single_pending_intent_can_only_be_claimed_once() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "5", "0.50").await;

        let first_claim = claim_or_resume_intent(&db, intent).await.unwrap();
        assert!(
            first_claim.is_some(),
            "the first claim of a pending intent must succeed"
        );

        // A second, independent claim attempt (simulating a second lane, or
        // a misrouted duplicate dispatch) against the now-in_progress
        // intent must resume the same decision, never claim it as fresh
        // and never fail outright -- but critically, size_and_reserve must
        // not recompute a second, possibly-different reservation.
        let second_claim = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();

        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
        let SizingOutcome::Decision(decision) =
            size_and_reserve(&db, &balance_reader, &second_claim)
                .await
                .unwrap()
        else {
            panic!("resuming an in_progress intent must size successfully");
        };

        let reserved: String = sqlx::query("SELECT reserved_qty FROM copy_intents WHERE id = ?")
            .bind(intent)
            .fetch_one(&*db)
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            reserved, "5",
            "resuming must reuse the one persisted decision, not compute a second reservation"
        );
        assert_eq!(decision.qty, Decimal::new(5, 0));
    }

    #[tokio::test]
    async fn an_expired_persisted_decision_is_never_resumed_or_left_reserved() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "5", "0.50").await;
        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
        let claimed = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();
        assert!(matches!(
            size_and_reserve(&db, &balance_reader, &claimed)
                .await
                .unwrap(),
            SizingOutcome::Decision(_)
        ));
        sqlx::query(
            "UPDATE copy_intents SET decision_deadline_at = '2000-01-01T00:00:00Z' WHERE id = ?",
        )
        .bind(intent)
        .execute(&*db)
        .await
        .unwrap();

        let resumed = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();
        assert!(matches!(
            size_and_reserve(&db, &balance_reader, &resumed)
                .await
                .unwrap(),
            SizingOutcome::Expired
        ));
        cancel_overdue_pre_submit_intent(&db, 1, intent)
            .await
            .unwrap();

        let (status, reserved): (String, String) =
            sqlx::query_as("SELECT status, reserved_qty FROM copy_intents WHERE id = ?")
                .bind(intent)
                .fetch_one(&*db)
                .await
                .unwrap();
        assert_eq!(status, "cancelled");
        assert_eq!(reserved, "0");
    }

    #[tokio::test]
    async fn a_buy_below_the_clob_minimum_is_rejected_before_any_attempt_exists() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "100", "0.58").await;
        let snapshot = PolicySnapshot {
            max_signal_age_seconds: 3600,
            decision_window_seconds: 300,
            price_tolerance_bps: 0,
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            max_order_notional: "1".to_owned(),
            min_leader_trade_size: "0".to_owned(),
        };
        sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&snapshot).unwrap())
            .bind(intent)
            .execute(&*db)
            .await
            .unwrap();
        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
        let claimed = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();
        assert!(matches!(
            size_and_reserve(&db, &balance_reader, &claimed)
                .await
                .unwrap(),
            SizingOutcome::Rejected("computed buy notional is below the CLOB minimum of 1 USDC")
        ));
        reject_pre_submit_intent(
            &db,
            intent,
            "computed buy notional is below the CLOB minimum of 1 USDC",
        )
        .await
        .unwrap();
        let (status, attempts): (String, i64) = sqlx::query_as(
            "SELECT ci.status, COUNT(oa.id) FROM copy_intents ci LEFT JOIN order_attempts oa \
             ON oa.intent_id = ci.id WHERE ci.id = ? GROUP BY ci.id",
        )
        .bind(intent)
        .fetch_one(&*db)
        .await
        .unwrap();
        assert_eq!(status, "rejected");
        assert_eq!(attempts, 0);
    }

    #[tokio::test]
    async fn a_partial_persisted_decision_is_rejected_not_resized() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "5", "0.50").await;
        sqlx::query(
            "UPDATE copy_intents SET status = 'in_progress', planned_qty = '5', planned_price = '0.50' \
             WHERE id = ?",
        )
        .bind(intent)
        .execute(&*db)
        .await
        .unwrap();

        assert!(matches!(
            claim_or_resume_intent(&db, intent).await,
            Err(ExecuteError::InvalidDecimal(
                "incomplete persisted decision"
            ))
        ));
    }

    #[tokio::test]
    async fn a_leader_with_no_reservations_from_others_gets_its_full_strict_available_balance() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        sqlx::query("INSERT INTO position_lots (account_id, leader_id, token_id, qty) VALUES (1, 1, '123456', '20')")
            .execute(&*db)
            .await
            .unwrap();
        let intent = seed_pending_intent(&db, 1, "123456", "SELL", "20", "0.50").await;

        let balance_reader = FixedBalanceReader::new(Decimal::new(7, 0), Decimal::new(100, 0));
        let outcome = execute_intent(&db, &balance_reader, &FullFillSubmitter, intent)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            outcome,
            ExecutionOutcome::Filled {
                filled_qty: Decimal::new(7, 0)
            }
        );
    }
}
