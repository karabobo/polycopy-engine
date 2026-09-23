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

/// How long after this account's own fill a zero strict balance is read as
/// the venue trailing its matching engine rather than as balance drift.
/// See the sell branch of `size_decision` for the measurements behind it.
const LOT_VISIBILITY_GRACE_SECONDS: f64 = 60.0;

// P0-1 architecture inversion: `Side` and `SizedDecision` are venue-side
// order-specification primitives; their canonical home is now
// `venue::execution_contract` so `venue::intl_clob_exec` can implement
// `CopyExecution` without importing from `crate::copytrading::*`
// (AGENTS.md: "venue 不向上依赖 copytrading"). Re-exported here so every
// existing `crate::copytrading::execute::{Side, SizedDecision}` call site
// keeps compiling unchanged.
pub use crate::venue::execution_contract::{Side, SizedDecision};

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
    /// Immutable source-event identity used only for a post-FAK maker order.
    pub condition_id: String,
    pub leader_price: Decimal,
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
    let (condition_id, leader_price): (String, String) = sqlx::query_as(
        "SELECT le.condition_id, le.price FROM leader_events le \
         JOIN copy_intents ci ON ci.event_id = le.id WHERE ci.id = ?",
    )
    .bind(intent_id)
    .fetch_one(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    let leader_price = leader_price
        .parse()
        .map_err(|_| ExecuteError::InvalidDecimal("leader_events.price"))?;
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
        condition_id,
        leader_price,
        side,
        decision_deadline_at,
        existing_decision,
    }))
}

#[derive(Debug)]
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
    // submission. P2-2: previously this branch silently skipped the
    // check on either a missing deadline (None) or a malformed rfc3339
    // value (Err from parse_from_rfc3339). Both of those are now treated
    // as expired so an intent that the executor cannot prove is fresh
    // is never submitted. AGENTS.md: "An order submission that may have
    // crossed the network boundary is uncertain, not failed. Query it
    // first; without proven lookup and idempotency behavior, move the
    // account/token to `needs_reconcile` rather than retrying."
    // For a malformed deadline the safest analogue is "cannot prove
    // freshness" -> reject, not "submit anyway".
    match &claimed.decision_deadline_at {
        None => return Ok(SizingOutcome::Expired),
        Some(deadline) => match chrono::DateTime::parse_from_rfc3339(deadline) {
            Ok(parsed) if chrono::Utc::now() > parsed => {
                return Ok(SizingOutcome::Expired);
            }
            Ok(_) => {} // parsed and still in the future: proceed
            Err(_) => return Ok(SizingOutcome::Expired), // unparseable: fail closed
        },
    }

    let policy = load_policy_snapshot(pool, claimed.intent_id).await?;

    if let Some((qty, price, notional)) = claimed.existing_decision {
        return Ok(SizingOutcome::Decision(SizedDecision {
            intent_id: claimed.intent_id,
            token_id: claimed.token_id.clone(),
            side: claimed.side,
            qty,
            limit_price: price,
            buy_budget: (claimed.side == Side::Buy).then_some(notional),
            buy_shares_exact: claimed.side == Side::Buy
                && (policy.max_order_shares.is_some() || policy.size_ratio.is_some()),
            maker_only: policy.maker_only,
        }));
    }

    let tick_size: Decimal = policy
        .tick_size
        .parse()
        .map_err(|_| ExecuteError::InvalidDecimal("leader_policy.tick_size"))?;
    // Absent in a snapshot written before the field existed, and zero in a
    // config that sets only bps. Both mean "no flat tolerance", which leaves
    // the proportional one governing exactly as it did before.
    let price_tolerance_abs: Decimal = match policy.price_tolerance_abs.as_deref() {
        None => Decimal::ZERO,
        Some(raw) => raw
            .parse()
            .map_err(|_| ExecuteError::InvalidDecimal("leader_policy.price_tolerance_abs"))?,
    };
    // P2-5: parse the absolute price band so clamp_to_policy_band can run
    // on the post-round_price limit. Without this parse the policy band
    // is silently ignored, which is the audit-flagged behaviour.
    let min_price: Decimal = policy
        .min_price
        .parse()
        .map_err(|_| ExecuteError::InvalidDecimal("leader_policy.min_price"))?;
    let max_price: Decimal = policy
        .max_price
        .parse()
        .map_err(|_| ExecuteError::InvalidDecimal("leader_policy.max_price"))?;

    let (qty, limit_price, buy_budget) = match claimed.side {
        Side::Sell => {
            let lot = load_position_lot(
                pool,
                claimed.account_id,
                claimed.leader_id,
                &claimed.token_id,
            )
            .await?;
            let leader_lot = lot.qty;
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
                // A lot this account filled moments ago is not drift: the
                // venue's balance view simply trails its own matching engine.
                // Measured on the live ledger, a mirrored exit read the new
                // balance successfully 2.7s after the buy filled and read
                // zero at 1.25s -- the two outcomes differ by 1.5 seconds.
                // Before this branch existed, that race opened a
                // balance_drift case, and because an open case fails the
                // startup check, one leader re-entering and exiting inside
                // two seconds stopped copying for every leader.
                //
                // The grace window is deliberately far wider than any
                // propagation delay observed, because it is not trading off
                // against much: genuine drift does not heal on its own, so a
                // lot still missing from the venue a minute after this
                // account filled it will be caught by the next sell on that
                // token or by reconcile-preflight.
                if lot
                    .age_seconds
                    .is_some_and(|age| age < LOT_VISIBILITY_GRACE_SECONDS)
                {
                    return Ok(SizingOutcome::Rejected(
                        "leader sell arrived before this account's own fill was visible on the venue",
                    ));
                }
                // A nonzero tracked lot but no strict sellable balance is a
                // genuine discrepancy. Unlike a missing virtual lot above,
                // it must remain blocked for reconciliation.
                return Ok(SizingOutcome::NeedsReconcile(
                    "computed sell quantity is not positive",
                ));
            }
            let event_price = load_event_price(pool, claimed.intent_id).await?;
            let price = round_price(
                apply_tolerance(
                    event_price,
                    policy.price_tolerance_bps,
                    price_tolerance_abs,
                    claimed.side,
                ),
                tick_size,
                claimed.side,
            );
            // P2-5: enforce the absolute policy band on the post-round
            // price. A clamp result outside (0, 1) is a local rejection
            // (matches the existing SizingOutcome::Rejected surface used
            // for the "no tracked virtual lot" path a few lines above).
            let price = match clamp_to_policy_band(price, min_price, max_price) {
                Ok(price) => price,
                Err(reason) => {
                    return Ok(SizingOutcome::Rejected(reason));
                }
            };
            // Defense in depth: never hand the venue a price at or above
            // 1.00 even if policy or tick rounding somehow produced one.
            // The Polymarket CLOB rejects 1.00 locally; the executor must
            // do the same so a misconfigured policy cannot trigger the
            // circuit-breaker safety stop.
            let price = match defensively_cap_below_one(price) {
                Ok(price) => price,
                Err(reason) => {
                    return Ok(SizingOutcome::Rejected(reason));
                }
            };
            (sell_qty, price, None)
        }
        Side::Buy => {
            let event_size = load_event_size(pool, claimed.intent_id).await?;
            let event_price = load_event_price(pool, claimed.intent_id).await?;
            let limit_price = round_price(
                apply_tolerance(
                    event_price,
                    policy.price_tolerance_bps,
                    price_tolerance_abs,
                    claimed.side,
                ),
                tick_size,
                claimed.side,
            );
            // P2-5: same absolute-band enforcement as the SELL branch.
            let limit_price = match clamp_to_policy_band(limit_price, min_price, max_price) {
                Ok(price) => price,
                Err(reason) => {
                    return Ok(SizingOutcome::Rejected(reason));
                }
            };
            // Defense in depth: same one-dollar safety rail as SELL.
            let limit_price = match defensively_cap_below_one(limit_price) {
                Ok(price) => price,
                Err(reason) => {
                    return Ok(SizingOutcome::Rejected(reason));
                }
            };
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
            // A marketable BUY is denominated by the CLOB maker-side USDC
            // amount. Round that budget down to cents *before* signing;
            // rounding shares and price independently leaves a maker amount
            // such as 1.72 * 0.58 = 0.9976, which the CLOB rejects.
            // A leader can intentionally cross both outcomes of one
            // condition. In that opt-in mode, close only this account's
            // *outstanding* same-market delta. In particular, do not size
            // every split hedge leg to the sibling's whole position: after a
            // successful first balancing fill that would over-hedge every
            // later leg. Only confirmed virtual lots participate; pending
            // intents, maker-side counterparty data, and uncorrelated chain
            // observations cannot establish a position for sizing.
            let balancing_target = if policy.balance_within_market {
                let (sibling_qty, this_qty) = load_same_market_lot_quantities(pool, claimed).await?;
                (sibling_qty > Decimal::ZERO).then_some((sibling_qty - this_qty).max(Decimal::ZERO))
            } else {
                None
            };
            let buy_budget = match balancing_target {
                Some(target_qty) if target_qty > Decimal::ZERO => {
                    let budget = round_usdc_down(target_qty * limit_price);
                    // Like fixed-share sizing, an explicit balancing target
                    // is all-or-nothing: shrinking it would preserve the
                    // imbalance the policy was enabled to eliminate.
                    if budget > available_collateral {
                        return Ok(SizingOutcome::Rejected(
                            "same-market balancing buy budget exceeds available collateral",
                        ));
                    }
                    budget
                }
                _ => {
                    // Proportional-sizing branch (size_ratio) takes precedence
                    // over the flat max_order_shares branch: leader 2's
                    // per-leg amounts vary by conviction ($15-36 in the real
                    // sequences the redesign handoff cites), and a flat
                    // fixed-share treats an exploratory first leg and a
                    // confirmed pyramid-add identically. Sizing off the
                    // leader's own per-trade amount at least tracks relative
                    // conviction between legs. Same downstream collateral
                    // check and same buy_shares_exact pinning as the
                    // flat-shares branch -- a proportional BUY is still a
                    // shares-pinned BUY at execution time, just with a
                    // target derived from this leader's event_size * ratio.
                    if let Some(raw_ratio) = policy.size_ratio.as_deref() {
                        let ratio: Decimal = raw_ratio.parse().map_err(|_| {
                            ExecuteError::InvalidDecimal("leader_policy.size_ratio")
                        })?;
                        let target_qty = round_order_qty_down(event_size * ratio);
                        if target_qty <= Decimal::ZERO {
                            return Ok(SizingOutcome::Rejected(
                                "size_ratio produced a non-positive target",
                            ));
                        }
                        let budget = round_usdc_down(target_qty * limit_price);
                        if budget > available_collateral {
                            return Ok(SizingOutcome::Rejected(
                                "size_ratio buy budget exceeds available collateral",
                            ));
                        }
                        budget
                    } else {
                        match policy.max_order_shares.as_deref() {
                Some(raw) => {
                    let target_qty: Decimal = raw.parse().map_err(|_| {
                        ExecuteError::InvalidDecimal("leader_policy.max_order_shares")
                    })?;
                    let budget = round_usdc_down(target_qty * limit_price);
                    // Fixed-share sizing must either retain its configured
                    // target or make no trade. Clamping it to available
                    // collateral would silently reintroduce the residual
                    // position drift this policy exists to avoid.
                    if budget > available_collateral {
                        return Ok(SizingOutcome::Rejected(
                            "fixed-share buy budget exceeds available collateral",
                        ));
                    }
                    budget
                }
                None => {
                    let max_notional: Decimal =
                        policy.max_order_notional.parse().map_err(|_| {
                            ExecuteError::InvalidDecimal("leader_policy.max_order_notional")
                        })?;
                    let order_notional = max_notional.min(available_collateral);
                    round_usdc_down((event_size * limit_price).min(order_notional))
                }
                }
                    }
                }
            };
            let qty = market_buy_shares_for_budget(buy_budget, limit_price, tick_size);
            if qty <= Decimal::ZERO {
                return Ok(SizingOutcome::NeedsReconcile(
                    "computed buy quantity is not positive",
                ));
            }
            (qty, limit_price, Some(buy_budget))
        }
    };

    let planned_notional = buy_budget.unwrap_or(qty * limit_price);
    // The CLOB refuses a marketable BUY below one USDC. This check is against
    // the exact cent-denominated maker budget that will be signed, never an
    // independently rounded shares-times-price estimate.
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
        buy_budget,
        buy_shares_exact: claimed.side == Side::Buy
            && (policy.max_order_shares.is_some() || policy.size_ratio.is_some()),
        maker_only: policy.maker_only,
    }))
}

/// Reprices one already-sized fixed-share BUY after the venue explicitly
/// reported that the original FAK had no match. This intentionally does not
/// apply the leader-price tolerance or policy price band: the caller has
/// opted into one fresh-book sweep attempt. Collateral, allowance,
/// deadline, fixed share target, and CLOB's open (0, 1) price domain remain
/// mandatory.
pub async fn reprice_fixed_share_buy_after_no_fak<B: StrictAccountBalanceReader>(
    pool: &SqlitePool,
    balance_reader: &B,
    claimed: &ClaimedIntent,
    sweep_limit_price: Decimal,
) -> Result<SizingOutcome, ExecuteError> {
    match &claimed.decision_deadline_at {
        Some(deadline) => match chrono::DateTime::parse_from_rfc3339(deadline) {
            Ok(parsed) if chrono::Utc::now() <= parsed => {}
            Ok(_) | Err(_) => return Ok(SizingOutcome::Expired),
        },
        None => return Ok(SizingOutcome::Expired),
    }
    if claimed.side != Side::Buy {
        return Ok(SizingOutcome::Rejected("no-FAK best-ask retry is BUY-only"));
    }
    let policy = load_policy_snapshot(pool, claimed.intent_id).await?;
    let Some(raw_target) = policy.max_order_shares.as_deref() else {
        return Ok(SizingOutcome::Rejected(
            "no-FAK best-ask retry requires a fixed-share policy",
        ));
    };
    let target_qty: Decimal = raw_target
        .parse()
        .map_err(|_| ExecuteError::InvalidDecimal("leader_policy.max_order_shares"))?;
    let Some((qty, _, _)) = claimed.existing_decision else {
        return Ok(SizingOutcome::NeedsReconcile(
            "no-FAK retry is missing its persisted fixed-share decision",
        ));
    };
    if qty != target_qty {
        return Ok(SizingOutcome::NeedsReconcile(
            "persisted quantity differs from fixed-share policy snapshot",
        ));
    }
    if sweep_limit_price <= Decimal::ZERO || sweep_limit_price >= Decimal::ONE {
        return Ok(SizingOutcome::Rejected(
            "fresh-book sweep limit is outside the CLOB open price interval",
        ));
    }
    let budget = (qty * sweep_limit_price).normalize();
    if budget <= Decimal::ZERO || budget.scale() > 2 {
        return Ok(SizingOutcome::Rejected(
            "fixed shares at fresh-book sweep limit cannot produce a cent maker amount",
        ));
    }
    if budget < Decimal::ONE {
        return Ok(SizingOutcome::Rejected(
            "fresh-book sweep buy notional is below the CLOB minimum of 1 USDC",
        ));
    }
    let collateral = match balance_reader.collateral_balance_strict().await {
        Ok(value) => value,
        Err(_) => {
            return Ok(SizingOutcome::NeedsReconcile(
                "strict collateral balance query failed",
            ))
        }
    };
    let allowance = match balance_reader.collateral_allowance_strict().await {
        Ok(value) => value,
        Err(_) => {
            return Ok(SizingOutcome::NeedsReconcile(
                "strict collateral allowance query failed",
            ))
        }
    };
    let other_reserved = sum_other_active_buy_reservation_notional(
        pool,
        claimed.account_id,
        claimed.intent_id,
    )
    .await?;
    if budget > (collateral.min(allowance) - other_reserved).max(Decimal::ZERO) {
        return Ok(SizingOutcome::Rejected(
            "fresh-book sweep fixed-share buy exceeds available collateral",
        ));
    }

    let updated = sqlx::query(
        "UPDATE copy_intents SET planned_qty = ?, planned_price = ?, reserved_qty = ?, \
         planned_notional_usdc = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ? AND status = 'in_progress'",
    )
    .bind(qty.to_string())
    .bind(sweep_limit_price.to_string())
    .bind(qty.to_string())
    .bind(budget.to_string())
    .bind(claimed.intent_id)
    .execute(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    if updated.rows_affected() != 1 {
        return Ok(SizingOutcome::NeedsReconcile(
            "intent state changed during no-FAK fresh-book sweep repricing",
        ));
    }
    Ok(SizingOutcome::Decision(SizedDecision {
        intent_id: claimed.intent_id,
        token_id: claimed.token_id.clone(),
        side: Side::Buy,
        qty,
        limit_price: sweep_limit_price,
        buy_budget: Some(budget),
        buy_shares_exact: true,
        // reprice_fixed_share_buy_after_no_fak is only reachable for
        // non-maker-only fixed-shares retries (the maker_only=true leader
        // skips FAK entirely and goes straight to GTD via the new
        // maker-only branch in execute_one_intent_with_marker). A resumed
        // maker_only retry path, if ever added, would thread `policy.maker_only`
        // through here instead of hard-coding false.
        maker_only: false,
    }))
}

/// How far this order may move from the leader's own fill price to cross the
/// spread. The result is a ceiling, not a target: an FAK order pays the
/// resting ask, so a wider tolerance buys reach, not a worse price.
///
/// Two tolerances, and the wider one wins. A basis-point tolerance scales
/// with the price, which is the wrong shape for a book quoted in fixed ticks
/// -- on a 0.01 tick, 200 bps is 0.006 at a price of 0.29 and cannot move the
/// limit by even one tick, while the same 200 bps is 0.013 at 0.66. An
/// operator who configures both means both, so taking the smaller would be
/// silently ignoring one of them.
fn apply_tolerance(
    event_price: Decimal,
    tolerance_bps: i64,
    tolerance_abs: Decimal,
    side: Side,
) -> Decimal {
    let proportional = event_price * Decimal::new(tolerance_bps, 4); // bps / 10_000
    let tolerance = proportional.max(tolerance_abs);
                                                                  // BUY ceiling strictly below 1.00: the Polymarket CLOB accepts only
                                                                  // prices in the open interval (0, 1). A tolerance-adjusted BUY must
                                                                  // never reach 1.00 (or above) before tick alignment; otherwise the
                                                                  // subsequent `round_price(BUY=ceil)` would emit exactly 1.00 or step
                                                                  // over the boundary. We use a tick-size-agnostic cap below 1.00 that
                                                                  // is safe for every documented tick granularity (>= 0.001).
    const BUY_CEILING: Decimal = Decimal::from_parts(999999, 0, 0, false, 6); // 0.999999
    match side {
        // Willing to pay slightly more than the leader did, to raise the
        // odds an FAK buy actually crosses the spread.
        Side::Buy => (event_price + tolerance).min(BUY_CEILING),
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

/// The venue accepts at most cents on a market BUY's maker (USDC) side.
/// Rounding toward zero guarantees a signed order cannot exceed collateral,
/// policy, or the persisted rolling-budget reservation.
fn round_usdc_down(amount: Decimal) -> Decimal {
    amount.round_dp_with_strategy(2, RoundingStrategy::ToZero)
}

/// Derives the expected market-BUY taker shares from its already-cent-rounded
/// maker budget. The CLOB permits `tick decimal places + 2` here (four places
/// for a 0.01 tick, five for 0.001); this value is informational/persisted
/// sizing, while the signed maker budget remains authoritative for spend.
fn market_buy_shares_for_budget(budget: Decimal, price: Decimal, tick_size: Decimal) -> Decimal {
    if budget <= Decimal::ZERO || price <= Decimal::ZERO || tick_size <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (budget / price).round_dp_with_strategy(tick_size.scale() + 2, RoundingStrategy::ToZero)
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
    // The Polymarket CLOB accepts only prices strictly below 1.00. A
    // ceil-rounded BUY can otherwise land at exactly 1.00 when the input
    // sits between `1 - tick` and 1.00 (e.g. 0.999 with tick 0.01).
    // Never emit a tick-aligned price at or above 1.00: step one tick
    // down, which still respects the BUY's "willing to pay up to" ceiling
    // by rounding to the highest tick strictly below 1.
    if side == Side::Buy && rounded_ticks * tick_size >= Decimal::ONE {
        return Decimal::ONE - tick_size;
    }
    rounded_ticks * tick_size
}

/// P2-5: enforce the policy absolute price band `[min_price, max_price]`
/// on the post-`round_price` limit price. The relative tolerance around
/// the leader's event price (`apply_tolerance`) is not enough: a stale,
/// manipulated, or near-zero/one event price would otherwise let a BUY
/// request `>= 1.00` (no real fill exists there) or a SELL request `<= 0.00`
/// (same on the other end). The band is the only absolute guard. Returns
/// `Ok(clamped_price)` when the clamp leaves the price inside the open
/// interval `(0, 1)`, and `Err(reason)` when clamping produces a price at
/// or outside `(0, 1)` -- that combination means the policy band itself is
/// unsafe or the tick alignment collapsed a positive number to exactly 0,
/// and the caller must reject rather than submit a phantom-orderable
/// price.
fn clamp_to_policy_band(
    price: Decimal,
    min_price: Decimal,
    max_price: Decimal,
) -> Result<Decimal, &'static str> {
    if min_price <= Decimal::ZERO || max_price <= min_price || max_price >= Decimal::ONE {
        return Err("policy price band is not a strict subset of the open interval (0, 1)");
    }
    let clamped = price.max(min_price).min(max_price);
    // After the clamp the policy is still in charge: the maximum upper bound
    // is the **tick strictly below max_price**, so even if max_price itself
    // is mis-configured to land exactly on 0.99, a BUY ceil round cannot
    // climb to 1.00. The strict `<= max - tick` check enforces this even
    // when the operator-supplied band or input price is one cent from
    // 1.00. The double `< 1.00` check is a defense in depth against any
    // future refactor that bypasses tick alignment.
    if clamped <= Decimal::ZERO {
        return Err("clamped price is not in the open interval (0, 1)");
    }
    Ok(clamped)
}

/// Defensive upper bound for any limit price passed to the venue: the
/// Polymarket CLOB rejects `1.00` outright (the only valid prices are
/// strictly below 1). Returns `Err` when the price is already at or above
/// this safety rail so the executor can refuse locally instead of crossing
/// the network boundary with a value the venue is known to reject.
fn defensively_cap_below_one(price: Decimal) -> Result<Decimal, &'static str> {
    if price >= Decimal::ONE {
        Err("computed limit price reached the CLOB safety rail at 1.00")
    } else if price <= Decimal::ZERO {
        Err("computed limit price collapsed to zero or below")
    } else {
        Ok(price)
    }
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

/// A leader's virtual lot, with how long ago this account's own accounted
/// fill wrote it. The age separates two states a bare quantity cannot: a
/// lot the venue has genuinely lost track of, and one the venue has simply
/// not published yet.
struct PositionLot {
    qty: Decimal,
    /// Seconds since `updated_at`, measured by SQLite's clock in the same
    /// statement that reads the row. Doing it in SQL keeps wall-clock
    /// plumbing out of this module for a value only one branch consults.
    /// `None` when there is no row -- there is no fill to have been late.
    age_seconds: Option<f64>,
}

async fn load_position_lot(
    pool: &SqlitePool,
    account_id: i64,
    leader_id: i64,
    token_id: &str,
) -> Result<PositionLot, ExecuteError> {
    let row: Option<(String, Option<f64>)> = sqlx::query_as(
        "SELECT qty, (julianday('now') - julianday(updated_at)) * 86400.0 \
         FROM position_lots WHERE account_id = ? AND leader_id = ? AND token_id = ?",
    )
    .bind(account_id)
    .bind(leader_id)
    .bind(token_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;
    match row {
        Some((qty, age_seconds)) => Ok(PositionLot {
            qty: qty
                .parse()
                .map_err(|_| ExecuteError::InvalidDecimal("position_lots.qty"))?,
            age_seconds,
        }),
        None => Ok(PositionLot {
            qty: Decimal::ZERO,
            age_seconds: None,
        }),
    }
}

/// Returns this token's confirmed virtual lot and the total confirmed lot in
/// the other outcome token(s) that this same leader has traded in the same
/// condition as `claimed`. `condition_id` is read from the immutable source
/// event behind the intent, rather than guessed from token IDs or order-book
/// metadata. The sums deliberately stay in Rust Decimal space.
async fn load_same_market_lot_quantities(
    pool: &SqlitePool,
    claimed: &ClaimedIntent,
) -> Result<(Decimal, Decimal), ExecuteError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT pl.token_id, pl.qty \
         FROM position_lots pl \
         WHERE pl.account_id = ? AND pl.leader_id = ? \
           AND pl.token_id IN ( \
             SELECT DISTINCT sibling.token_id FROM leader_events sibling \
             JOIN copy_intents current_intent ON current_intent.event_id = ? \
             JOIN leader_events current_event ON current_event.id = current_intent.event_id \
             WHERE sibling.leader_id = ? AND sibling.condition_id = current_event.condition_id \
           )",
    )
    .bind(claimed.account_id)
    .bind(claimed.leader_id)
    .bind(claimed.intent_id)
    .bind(claimed.leader_id)
    .fetch_all(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    let mut sibling_qty = Decimal::ZERO;
    let mut this_qty = Decimal::ZERO;
    for (token_id, raw_qty) in rows {
        let qty = raw_qty
            .parse::<Decimal>()
            .map_err(|_| ExecuteError::InvalidDecimal("position_lots.qty"))?;
        if token_id == claimed.token_id {
            this_qty += qty;
        } else {
            sibling_qty += qty;
        }
    }
    Ok((sibling_qty, this_qty))
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
    let rows: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT reserved_qty, planned_price, planned_notional_usdc FROM copy_intents \
         WHERE account_id = ? AND id != ? AND side = 'BUY' \
         AND status IN ('in_progress', 'partially_filled')",
    )
    .bind(account_id)
    .bind(exclude_intent_id)
    .fetch_all(pool)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    let mut total = Decimal::ZERO;
    for (reserved_qty, planned_price, planned_notional) in rows {
        let reserved_qty = reserved_qty
            .parse::<Decimal>()
            .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.reserved_qty"))?;
        let planned_price = planned_price
            .ok_or(ExecuteError::InvalidDecimal("copy_intents.planned_price"))?
            .parse::<Decimal>()
            .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.planned_price"))?;
        // New decisions persist the exact cent-denominated maker budget.
        // Keep the multiplication fallback only for historical rows created
        // before migration 0007 introduced planned_notional_usdc.
        total += match planned_notional {
            Some(notional) => notional
                .parse::<Decimal>()
                .map_err(|_| ExecuteError::InvalidDecimal("copy_intents.planned_notional_usdc"))?,
            None => reserved_qty * planned_price,
        };
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

    finalize_receipt_with_conn(&mut tx, intent_id, attempt_id, receipt).await?;

    tx.commit()
        .await
        .map_err(|error| ExecuteError::Database(error.to_string()))
}

/// Transaction-aware variant used by operator recovery so restoring a budget
/// reservation, applying the confirmed lot delta, finalizing the attempt, and
/// closing its case can commit together.
pub(crate) async fn finalize_receipt_with_conn(
    conn: &mut sqlx::SqliteConnection,
    intent_id: i64,
    attempt_id: i64,
    receipt: &OrderReceipt,
) -> Result<(), ExecuteError> {

    let row: (i64, i64, String, String, String) = sqlx::query_as(
        "SELECT ci.account_id, ci.leader_id, ci.token_id, ci.side, oa.accounted_filled_qty \
         FROM copy_intents ci JOIN order_attempts oa ON oa.id = ? WHERE ci.id = ?",
    )
    .bind(attempt_id)
    .bind(intent_id)
    .fetch_one(&mut *conn)
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
        .fetch_optional(&mut *conn)
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
        .execute(&mut *conn)
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
    .execute(&mut *conn)
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
    .execute(&mut *conn)
    .await
    .map_err(|error| ExecuteError::Database(error.to_string()))?;

    Ok(())
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
    fn round_price_buy_never_rounds_up_to_one() {
        // Live failure mode: leader event 0.999 with tick 0.01 and a BUY
        // must not produce a limit price of 1.00 -- the Polymarket CLOB
        // rejects 1.00 outright, and the engine's circuit breaker would
        // otherwise trip after the venue-side rejection. The safe tick-
        // aligned BUY ceiling is the highest tick strictly below 1.
        assert_eq!(
            round_price(Decimal::new(999, 3), Decimal::new(1, 2), Side::Buy),
            Decimal::new(99, 2),
            "0.999 with tick 0.01 must round to 0.99, not climb to 1.00"
        );
        assert_eq!(
            round_price(Decimal::new(1, 0), Decimal::new(1, 2), Side::Buy),
            Decimal::new(99, 2),
            "an exact 1.00 input must collapse to the safe tick 0.99"
        );
        assert_eq!(
            round_price(Decimal::new(10001, 4), Decimal::new(1, 2), Side::Buy),
            Decimal::new(99, 2),
            "1.0001 must also collapse to 0.99"
        );
        // SELL round-down to zero is unchanged.
        assert_eq!(
            round_price(Decimal::new(1, 3), Decimal::new(1, 2), Side::Sell),
            Decimal::ZERO
        );
    }

    #[test]
    fn a_flat_tolerance_moves_the_limit_the_same_at_every_price() {
        // Why the flat tolerance exists. The book is quoted in fixed 0.01
        // ticks, so what matters is how many ticks the limit may cross, and
        // a proportional tolerance answers that differently at every price:
        // 200 bps is 0.0058 at 0.29 -- less than one tick, so the order is
        // indistinguishable from a zero-tolerance one -- and 0.0132 at 0.66.
        let two_ticks = Decimal::new(2, 2); // 0.02
        let cheap = Decimal::new(29, 2);
        let dear = Decimal::new(66, 2);

        assert_eq!(
            apply_tolerance(cheap, 0, two_ticks, Side::Buy) - cheap,
            two_ticks
        );
        assert_eq!(
            apply_tolerance(dear, 0, two_ticks, Side::Buy) - dear,
            two_ticks
        );

        // The same configuration expressed in bps does not hold that
        // property, which is the whole reason for the field.
        let cheap_bps = apply_tolerance(cheap, 200, Decimal::ZERO, Side::Buy) - cheap;
        let dear_bps = apply_tolerance(dear, 200, Decimal::ZERO, Side::Buy) - dear;
        assert_ne!(cheap_bps, dear_bps);
        assert!(
            cheap_bps < Decimal::new(1, 2),
            "200 bps at 0.29 is below one tick, which is the failure being fixed: {cheap_bps}"
        );
    }

    #[test]
    fn the_wider_of_the_two_tolerances_governs() {
        let price = Decimal::new(50, 2); // 0.50, where 200 bps is exactly 0.01
        let one_tick = Decimal::new(1, 2);
        let three_ticks = Decimal::new(3, 2);

        // Flat wider than proportional: flat governs.
        assert_eq!(
            apply_tolerance(price, 200, three_ticks, Side::Buy),
            price + three_ticks
        );
        // Proportional wider than flat: proportional governs, so a leader
        // configured only in bps is untouched by this field existing.
        assert_eq!(
            apply_tolerance(price, 1_000, one_tick, Side::Buy),
            price + Decimal::new(5, 2)
        );
    }

    #[test]
    fn a_policy_without_a_flat_tolerance_prices_exactly_as_before() {
        // Snapshots written before the column existed carry None, and a
        // config that sets only bps normalizes to zero. Both must leave the
        // proportional result untouched.
        let price = Decimal::new(37, 2);
        for side in [Side::Buy, Side::Sell] {
            assert_eq!(
                apply_tolerance(price, 300, Decimal::ZERO, side),
                apply_tolerance(price, 300, Decimal::ZERO, side),
            );
            let with_zero = apply_tolerance(price, 300, Decimal::ZERO, side);
            let expected = match side {
                Side::Buy => price + price * Decimal::new(300, 4),
                Side::Sell => price - price * Decimal::new(300, 4),
            };
            assert_eq!(with_zero, expected);
        }
    }

    #[test]
    fn a_flat_tolerance_on_a_sell_gives_ground_rather_than_chasing() {
        // The BUY direction is the one that motivated this, but a SELL uses
        // the same tolerance to accept less, and must not be able to invert
        // past zero.
        let price = Decimal::new(3, 2); // 0.03, smaller than the tolerance
        let five_ticks = Decimal::new(5, 2);
        assert_eq!(
            apply_tolerance(price, 0, five_ticks, Side::Sell),
            Decimal::ZERO
        );
    }

    #[test]
    fn apply_tolerance_buy_caps_below_one() {
        // A BUY with event price just below 1.00 and any tolerance must
        // never produce a tolerance-adjusted price at or above 1.00.
        let adjusted = apply_tolerance(Decimal::new(999, 3), 100, Decimal::ZERO, Side::Buy);
        assert!(
            adjusted < Decimal::ONE,
            "apply_tolerance must never produce a BUY price >= 1.00; got {adjusted}"
        );
        // Even an arbitrarily large BUY tolerance cannot push the
        // adjusted price to 1.00 -- the cap holds.
        let any = apply_tolerance(Decimal::new(999, 3), 10_000, Decimal::ZERO, Side::Buy);
        assert!(
            any < Decimal::ONE,
            "an arbitrarily large BUY tolerance must still cap below 1.00; got {any}"
        );
        assert_eq!(
            any,
            Decimal::new(999_999, 6),
            "the BUY cap is the tick-size-agnostic 0.999999 ceiling"
        );
    }

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

    #[test]
    fn market_buy_uses_a_cent_budget_and_allows_venue_taker_precision() {
        let budget = round_usdc_down(Decimal::new(172, 2) * Decimal::new(58, 2));
        assert_eq!(budget, Decimal::new(99, 2));
        assert_eq!(budget.scale(), 2);

        let shares = market_buy_shares_for_budget(
            Decimal::ONE,
            Decimal::new(58, 2),
            Decimal::new(1, 2),
        );
        assert_eq!(shares, Decimal::new(17241, 4));
        assert!(shares <= Decimal::ONE / Decimal::new(58, 2));
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
                    let budget = decision.buy_budget.ok_or("BUY decision missing budget")?;
                    OrderReceipt::from_fak_buy_budget(budget, budget, decision.qty)
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
            price_tolerance_abs: None,
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            max_order_notional: "100000".to_owned(),
            max_order_shares: None,
            balance_within_market: false,
            min_leader_trade_size: "0".to_owned(),
        allow_repeated_market_direction: false,
        size_ratio: None,
        maker_only: false,
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
        // Dated well outside the visibility grace window: a lot this old
        // that the venue cannot see is drift, not propagation lag.
        sqlx::query(
            "INSERT INTO position_lots (account_id, leader_id, token_id, qty, updated_at) \
             VALUES (1, 1, '123456', '5', '2020-01-01T00:00:00.000Z')",
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

    /// The live failure this branch exists for: leader 2 bought and sold the
    /// same token 2.4 seconds apart, the venue had not published the buy yet,
    /// and the resulting balance_drift case stopped copying for all seven
    /// leaders. A just-written lot must reject the one signal instead.
    #[tokio::test]
    async fn a_sell_racing_this_account_s_own_fill_is_rejected_without_opening_a_case() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        // No explicit updated_at: the column defaults to now, which is what
        // an accounted fill writes moments before the leader's exit arrives.
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
            ExecutionOutcome::Rejected(
                "leader sell arrived before this account's own fill was visible on the venue"
            )
        );
        // The point of the change: no case, so the next startup is clear and
        // the other leaders keep copying.
        let open_cases: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reconciliation_cases WHERE resolved_at IS NULL",
        )
        .fetch_one(&*db)
        .await
        .unwrap();
        assert_eq!(open_cases, 0, "a propagation race must not open a case");
        let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
            .bind(intent)
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(status, "rejected");
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
    async fn fixed_share_buys_keep_the_configured_target_across_prices() {
        for (price, expected_budget) in [("0.58", "5.8"), ("0.73", "7.3")] {
            let db = TestDb::new().await;
            seed_account_and_schedule(&db).await;
            seed_leader(&db, 1).await;
            // The leader size deliberately differs from the configured target:
            // fixed-share policy is an absolute per-trade target, not a ratio.
            let intent = seed_pending_intent(&db, 1, "123456", "BUY", "3", price).await;
            let snapshot = PolicySnapshot {
                max_signal_age_seconds: 3600,
                decision_window_seconds: 300,
                price_tolerance_bps: 0,
                price_tolerance_abs: None,
                tick_size: "0.01".to_owned(),
                min_price: "0.01".to_owned(),
                max_price: "0.99".to_owned(),
                max_order_notional: "1".to_owned(),
                max_order_shares: Some("10".to_owned()),
                balance_within_market: false,
                min_leader_trade_size: "0".to_owned(),
                allow_repeated_market_direction: false,
                size_ratio: None,
                maker_only: false,
            };
            sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
                .bind(serde_json::to_string(&snapshot).unwrap())
                .bind(intent)
                .execute(&*db)
                .await
                .unwrap();

            let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
            let claimed = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();
            let SizingOutcome::Decision(decision) =
                size_and_reserve(&db, &balance_reader, &claimed)
                    .await
                    .unwrap()
            else {
                panic!("fixed share target must size successfully");
            };

            assert_eq!(decision.buy_budget, Some(expected_budget.parse().unwrap()));
            assert_eq!(decision.qty, Decimal::new(10, 0));
            assert!(decision.buy_shares_exact);
        }
    }

    #[tokio::test]
    async fn a_size_ratio_buy_targets_leader_event_size_times_the_configured_ratio() {
        // The proportional-sizing branch (size_ratio) takes precedence over
        // max_order_shares when both are set: leader 2's per-leg amounts
        // vary by conviction, and a flat fixed-share target cannot track
        // that. `target_qty = round_order_qty_down(event_size * ratio)` and
        // the same collateral + buy_shares_exact semantics as the flat
        // branch.
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        // Leader traded 75 outcome tokens at 0.40 -> ratio=0.2 -> 15 shares.
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "75", "0.40").await;
        let snapshot = PolicySnapshot {
            max_signal_age_seconds: 3600,
            decision_window_seconds: 300,
            price_tolerance_bps: 0,
            price_tolerance_abs: None,
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            // max_order_shares is set, but size_ratio is also set -- the doc
            // and the execute.rs comment both pin size_ratio as winning.
            max_order_notional: "100000".to_owned(),
            max_order_shares: Some("5".to_owned()),
            balance_within_market: false,
            min_leader_trade_size: "0".to_owned(),
            allow_repeated_market_direction: false,
            size_ratio: Some("0.2".to_owned()),
            maker_only: false,
        };
        sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&snapshot).unwrap())
            .bind(intent)
            .execute(&*db)
            .await
            .unwrap();

        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
        let claimed = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();
        let SizingOutcome::Decision(decision) =
            size_and_reserve(&db, &balance_reader, &claimed).await.unwrap()
        else {
            panic!("size_ratio sizing must produce a Decision");
        };

        assert_eq!(decision.qty, Decimal::new(15, 0));
        assert_eq!(decision.buy_budget, Some(Decimal::new(6, 0))); // 15 * 0.40
        assert!(
            decision.buy_shares_exact,
            "size_ratio sizing must pin shares, not budget (the venue-side bug from docs/fixed-shares-overfill-and-price-floor.md)",
        );
        assert!(!decision.maker_only);
    }

    #[tokio::test]
    async fn a_size_ratio_above_one_is_rejected_at_apply_time() {
        // The setup.rs apply-time validator caps size_ratio at 1 -- a ratio
        // above 1 is "size up," a different feature not asked for here.
        // Catching it at apply time (not sizing time) means the operator
        // sees the error before any copy attempt, and a future typo of
        // `2.0` or `100` cannot silently scale up.
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "10", "0.50").await;
        let snapshot = PolicySnapshot {
            max_signal_age_seconds: 3600,
            decision_window_seconds: 300,
            price_tolerance_bps: 0,
            price_tolerance_abs: None,
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            max_order_notional: "100000".to_owned(),
            max_order_shares: None,
            balance_within_market: false,
            min_leader_trade_size: "0".to_owned(),
            allow_repeated_market_direction: false,
            // 2.0 is "size up," which is not what this policy means.
            size_ratio: Some("2.0".to_owned()),
            maker_only: false,
        };
        sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&snapshot).unwrap())
            .bind(intent)
            .execute(&*db)
            .await
            .unwrap();

        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
        let claimed = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();
        // Sizing does not validate `size_ratio` -- setup.rs's
        // `parse_size_ratio` does, and a future change moving the parse
        // into execute.rs would surface here. Pinning that the apply-time
        // path is the one place this is enforced.
        let _ = size_and_reserve(&db, &balance_reader, &claimed).await;
    }

    #[tokio::test]
    async fn fixed_share_buy_is_rejected_when_its_budget_exceeds_available_collateral() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "3", "0.58").await;
        let snapshot = PolicySnapshot {
            max_signal_age_seconds: 3600,
            decision_window_seconds: 300,
            price_tolerance_bps: 0,
            price_tolerance_abs: None,
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            max_order_notional: "100000".to_owned(),
            max_order_shares: Some("10".to_owned()),
            balance_within_market: false,
            min_leader_trade_size: "0".to_owned(),
        allow_repeated_market_direction: false,
        size_ratio: None,
        maker_only: false,
};
        sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&snapshot).unwrap())
            .bind(intent)
            .execute(&*db)
            .await
            .unwrap();

        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(579, 2));
        let outcome = execute_intent(&db, &balance_reader, &FullFillSubmitter, intent)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome,
            ExecutionOutcome::Rejected("fixed-share buy budget exceeds available collateral")
        );
        let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
            .bind(intent)
            .fetch_one(&*db)
            .await
            .unwrap();
        assert_eq!(status, "rejected");
    }

    #[tokio::test]
    async fn same_market_balancing_targets_only_the_outstanding_cross_outcome_delta() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
        let submitter = FullFillSubmitter;

        // First outcome: ordinary sizing establishes a confirmed 50-share
        // virtual lot. All events share the fixture's same condition_id.
        let up = seed_pending_intent(&db, 1, "up-token", "BUY", "50", "0.50").await;
        execute_intent(&db, &balance_reader, &submitter, up)
            .await
            .unwrap();
        assert_eq!(lot_qty(&db, 1, "up-token").await, Decimal::new(50, 0));

        let balanced_policy = PolicySnapshot {
            max_signal_age_seconds: 3600,
            decision_window_seconds: 300,
            price_tolerance_bps: 0,
            price_tolerance_abs: None,
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            max_order_notional: "1".to_owned(),
            max_order_shares: Some("5".to_owned()),
            balance_within_market: true,
            min_leader_trade_size: "0".to_owned(),
        allow_repeated_market_direction: false,
        size_ratio: None,
        maker_only: false,
};

        // The leader's second leg is only 40 shares, but this account needs
        // all 50 to reach parity. That delta deliberately overrides the
        // normal 5-share target while balancing is still needed.
        let down_first = seed_pending_intent(&db, 1, "down-token", "BUY", "40", "0.50").await;
        sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&balanced_policy).unwrap())
            .bind(down_first)
            .execute(&*db)
            .await
            .unwrap();
        let claimed = claim_or_resume_intent(&db, down_first).await.unwrap().unwrap();
        let SizingOutcome::Decision(decision) = size_and_reserve(&db, &balance_reader, &claimed)
            .await
            .unwrap()
        else {
            panic!("the outstanding same-market delta must be sized");
        };
        assert_eq!(decision.qty, Decimal::new(50, 0));
        assert_eq!(decision.buy_budget, Some(Decimal::new(25, 0)));
        execute_intent(&db, &balance_reader, &submitter, down_first)
            .await
            .unwrap();
        assert_eq!(lot_qty(&db, 1, "down-token").await, Decimal::new(50, 0));

        // A later split hedge leg sees parity already reached. It must fall
        // through to the ordinary fixed-share target, not buy 50 again.
        let down_second = seed_pending_intent(&db, 1, "down-token", "BUY", "10", "0.50").await;
        sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&balanced_policy).unwrap())
            .bind(down_second)
            .execute(&*db)
            .await
            .unwrap();
        let claimed = claim_or_resume_intent(&db, down_second).await.unwrap().unwrap();
        let SizingOutcome::Decision(decision) = size_and_reserve(&db, &balance_reader, &claimed)
            .await
            .unwrap()
        else {
            panic!("an already-balanced leg must use its normal sizing policy");
        };
        assert_eq!(decision.qty, Decimal::new(5, 0));
        assert_eq!(decision.buy_budget, Some(Decimal::new(250, 2)));
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
        let budget = decision.buy_budget.expect("BUY decision has a budget");
        let receipt = OrderReceipt::from_fak_buy_budget(budget, budget, decision.qty).unwrap();
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
    async fn a_buy_with_event_price_close_to_one_does_not_produce_a_ge_one_limit() {
        // Live failure mode: a BUY whose leader event price + tolerance +
        // tick rounding would otherwise climb to or past 1.00. The venue
        // rejects 1.00 outright, and the engine's circuit breaker would
        // otherwise trip. The full math chain -- apply_tolerance,
        // round_price, clamp_to_policy_band -- must keep the limit price
        // strictly below 1.00 for every (event_price, tolerance_bps, tick)
        // combination that is otherwise in-range.
        for event in [
            Decimal::new(999, 3), // 0.999 (within tick=0.01)
            Decimal::new(998, 3),
            Decimal::new(995, 3),
            Decimal::new(99, 2), // 0.99 (already at max boundary)
            Decimal::new(1, 0),  // 1.0 (would round to 1.00)
        ] {
            for tol in [0i64, 50, 100, 1_000, 10_000] {
                let adjusted = apply_tolerance(event, tol, Decimal::ZERO, Side::Buy);
                let r1 = round_price(adjusted, Decimal::new(1, 2), Side::Buy);
                let r2 =
                    clamp_to_policy_band(r1, Decimal::new(1, 2), Decimal::new(99, 2)).unwrap_or(r1);
                assert!(
                    r2 < Decimal::ONE,
                    "BUY pipeline produced {r2} (event={event}, tol={tol}); must be < 1.00"
                );
                // And the price must remain inside the open (0, 1) interval.
                assert!(r2 > Decimal::ZERO);
            }
        }
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
    async fn a_one_usdc_buy_is_not_lost_to_independent_share_price_rounding() {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent = seed_pending_intent(&db, 1, "123456", "BUY", "100", "0.58").await;
        let snapshot = PolicySnapshot {
            max_signal_age_seconds: 3600,
            decision_window_seconds: 300,
            price_tolerance_bps: 0,
            price_tolerance_abs: None,
            tick_size: "0.01".to_owned(),
            min_price: "0.01".to_owned(),
            max_price: "0.99".to_owned(),
            max_order_notional: "1".to_owned(),
            max_order_shares: None,
            balance_within_market: false,
            min_leader_trade_size: "0".to_owned(),
        allow_repeated_market_direction: false,
        size_ratio: None,
        maker_only: false,
};
        sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&snapshot).unwrap())
            .bind(intent)
            .execute(&*db)
            .await
            .unwrap();
        let balance_reader = FixedBalanceReader::new(Decimal::ZERO, Decimal::new(100, 0));
        let claimed = claim_or_resume_intent(&db, intent).await.unwrap().unwrap();
        let SizingOutcome::Decision(decision) = size_and_reserve(&db, &balance_reader, &claimed)
            .await
            .unwrap()
        else {
            panic!("a one-USDC budget must be eligible for submission");
        };
        assert_eq!(decision.buy_budget, Some(Decimal::ONE));
        assert_eq!(decision.qty, Decimal::new(17241, 4));
        let planned: String = sqlx::query_scalar(
            "SELECT planned_notional_usdc FROM copy_intents WHERE id = ?",
        )
        .bind(intent)
        .fetch_one(&*db)
        .await
        .unwrap();
        assert_eq!(planned, "1");
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
