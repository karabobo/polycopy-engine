# Fixed-shares BUY sizing, and same-market balancing: handoff

## Part 1: fixed-shares sizing -- done

`leader_policy.max_order_shares` (migration `0012`), its config/validation
layer (`setup.rs`), the `PolicySnapshot` wiring (`plan.rs`), and the sizing
branch itself (`execute.rs`, inside `Side::Buy`) are all implemented and
tested. When set, a BUY sizes to exactly that many shares regardless of
price, and is rejected outright (not silently downsized) if the resulting
budget exceeds available collateral --
`fixed_share_buys_keep_the_configured_target_across_prices` and
`fixed_share_buy_is_rejected_when_its_budget_exceeds_available_collateral`
in `execute.rs` cover both cases. No further work needed here.

## Part 2: same-market balancing -- config layer done, sizing branch is not

> Completion update (2026-09-15): the sizing branch is now implemented.
> `PolicySnapshot` persists `balance_within_market`; the executor calculates
> the confirmed same-leader/same-condition delta from `position_lots`, and
> only overrides ordinary sizing while that delta is positive. The regression
> suite covers a 50 Up / 40 Down / 10 Down split sequence: the first Down
> buy targets 50 shares to close the gap, while the next falls back to the
> configured fixed-share policy rather than buying 50 again. The setting
> remains opt-in per leader and defaults to `false`.

### Why

Some Leaders trade both outcomes of the same market (buy "Up", later buy
"Down" in the same condition) rather than a clean single-token buy/sell
cycle. Account owner's own description of the wanted behavior: "类似我们现在跟
leader买了10shares up，那如果下次leader买同市场down时，我们就也买入10shares
down，单市场保持1个均衡的控制" -- i.e. when we already hold shares of this
market's *other* outcome (from copying this same Leader), size the new BUY
to match that held quantity, not a fresh notional/fixed-shares amount, so
the account's own two-sided exposure in that market stays numerically
balanced.

### What's already done

- `migrations/0013_leader_balance_within_market.sql`:
  `leader_policy.balance_within_market`, `INTEGER NOT NULL DEFAULT 0`
  (boolean), additive -- every existing Leader is unaffected until set.
- `src/copytrading/setup.rs`: `LeaderPolicyInput.balance_within_market: bool`
  (`#[serde(default)]`, so omitting it in a Trading Config leaves it `false`),
  persisted through `insert_policy`/`update_policy_if_changed`. Covered by
  `balance_within_market_defaults_to_false_and_can_be_enabled`.

Nothing reads this flag back yet. Same shape as Part 1 was before its
sizing branch landed: configurable and persisted, not yet acted on.

### What's left

**1. Carry it through the intent snapshot** (`src/copytrading/plan.rs`),
mirroring exactly how `max_order_shares` was just added: add
`balance_within_market: bool` to `PolicySnapshot` (plain `bool`, not
`Option` -- the column has a `NOT NULL DEFAULT 0`, so there is no "absent"
state to model) and to the `SELECT` in `evaluate_event`
(`src/copytrading/plan.rs:261-264`, right where `max_order_shares` was just
added next to it).

**2. Target the outstanding *delta*, not the sibling's full quantity** --
important correction from the first draft of this doc, found from a real
observed pattern: some Leaders worth copying build the second side of a
market across *several* trades (e.g. buy 50 "Up" in one trade, then 40
"Down", then another 10 "Down" -- ending flat at 50/50, but never in one
matching trade). Sizing every one of those "Down" legs to match the full
50-share "Up" position (the first draft's rule) would buy 50 *again* on
each leg -- 100+ total, not 50. The target must be **how much more is
needed to reach parity, given what this side already holds**, so it
naturally shrinks to zero once this account is actually balanced,
regardless of how many trades the Leader split it into:

```
target_qty = max(0, held_qty(sibling_token) - held_qty(this_token))
```

Both are the same kind of lookup, against `position_lots`:

```sql
-- sibling_qty: this account's holding of the other outcome in this market,
-- for this Leader (same "find a token this Leader has traded in the same
-- condition_id" join as the first draft)
SELECT pl.qty FROM position_lots pl
WHERE pl.account_id = ? AND pl.leader_id = ? AND pl.token_id != ?
  AND pl.token_id IN (
      SELECT DISTINCT token_id FROM leader_events
      WHERE leader_id = ? AND condition_id = ?
  )

-- this_qty: this account's current holding of the token being bought right now
SELECT qty FROM position_lots
WHERE account_id = ? AND leader_id = ? AND token_id = ?
```

(`condition_id` isn't on `copy_intents` today -- it has to come from
`leader_events.condition_id` via the intent's `event_id`, same as the first
draft noted.)

Priority order, once both lookups resolve:

```rust
let buy_budget = if policy.balance_within_market {
    let sibling_qty = held_sibling_qty(...).await?.unwrap_or(Decimal::ZERO);
    let this_qty = held_this_token_qty(...).await?.unwrap_or(Decimal::ZERO);
    let target_qty = (sibling_qty - this_qty).max(Decimal::ZERO);
    if target_qty > Decimal::ZERO {
        round_usdc_down(target_qty * limit_price)
        // same available_collateral / rejection handling as Part 1's
        // fixed-shares branch -- an exact delta target deserves the same
        // reject-rather-than-downsize treatment a configured share count
        // gets, for the same reason.
    } else {
        /* already balanced (or overweight) on this side: fall through to
           max_order_shares / max_order_notional */
    }
} else {
    /* existing max_order_shares / max_order_notional branch */
};
```

Worked example matching the observed pattern, Leader configured with
`balance_within_market = true`:

1. Leader buys 50 "Up". No "Down" holding exists yet for this Leader+market
   -> `target_qty` branch not applicable (nothing to balance against) ->
   falls through to this Leader's `max_order_shares`/`max_order_notional`.
   Say that sizes us to 50 "Up" too.
2. Leader buys 40 "Down". `sibling_qty` (Up) = 50, `this_qty` (Down) = 0 ->
   `target_qty` = 50. We buy 50 "Down" in this one trade -- deliberately
   *not* capped to the Leader's own 40, since the goal is this account's own
   balance, not replaying the Leader's per-trade pacing.
3. Leader buys another 10 "Down". `sibling_qty` = 50, `this_qty` = 50 now ->
   `target_qty` = 0 -> falls through to normal sizing for this leg (which
   may itself reject or size small, depending on `max_order_shares`/
   `max_order_notional` -- this account is already balanced, so it isn't
   this rule's concern any more).

i.e. `balance_within_market` only *overrides* sizing while there's a real
gap to close; once closed (or if there was never a sibling position), it
gets out of the way of whatever this Leader's other sizing config says.

**3. Nothing else changes**, for the same reason Part 1 didn't touch
`prepare.rs`/`orchestrate.rs`/the venue layer: this only changes how
`buy_budget` is computed, still consumed generically downstream through the
same `Amount::usdc(buy_budget)` construction path.

### Verification

- `cargo test --all-features --locked` stays green.
- New coverage, mirroring Part 1's tests:
  - A BUY into a market where the account holds N shares of the sibling
    outcome and none yet of this one sizes to exactly N.
  - **The multi-leg case that motivated the delta rule**: after that first
    balancing buy lands (this side now also at N), a second BUY signal in
    the same market sizes to the delta (0, so it falls through to
    `max_order_shares`/`max_order_notional`) rather than buying N again --
    this is the regression test for the bug the "match the total" version
    of this rule would have had.
  - A first-ever trade in a market with no held sibling falls through to
    the existing sizing untouched.
- `tests/canary_production_construction_contract.rs` still passes
  unmodified.
