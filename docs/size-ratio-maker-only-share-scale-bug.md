# size_ratio sizing produces 4-decimal share counts that every maker-only GTD order rejects

## Severity

**Live: every leader 2 signal that reaches the venue-preparation step is rejected before submission.** No money is spent and no order reaches the venue: the failure is inside the SDK's local order builder, `reject_pre_submit_intent` handles it, the execution fuse stays closed, and no reconciliation case opens. The fail-closed design worked. The problem is that leader 2 currently cannot place a single trade unless the leader's share count happens to make the arithmetic below land on a 2-decimal number, which is rare.

First observed: `copy_intents.id = 726` (leader event 1295), 2026-09-24 02:40 CST, first leader 2 signal large enough to pass the sizing checks after the crash-loop fix (`e163300`) and the GTD maker fill recovery (`916d06c`/`eea86a1`) were deployed. Its `rejection_reason`:

```
maker-only envelope preparation failed: order building failed: Validation: invalid:
Unable to build Order: Size 7.6981 has 4 decimal places. Maximum lot size is 2
```

The missed trade is terminal (intent status `rejected`); nothing needs to be recovered for it.

There is also a second, latent failure one step further along (see Finding 3) that would **open the fuse** rather than reject cleanly. It should be fixed in the same change.

## Root cause

### Finding 1 (the reported failure): shares are re-derived from a cent budget, discarding the 2-decimal target

Production evidence, `copy_intents` row 726 and `leader_events` row 1295:

| field | value |
|---|---|
| leader event `size` | `38.547059` |
| leader event `price` | `0.5058751693611697` |
| `limit_price` after tolerance (0.02 abs) and tick ceiling | `0.53` |
| policy snapshot | `size_ratio: "0.2"`, `maker_only: true`, `allow_repeated_market_direction: true` |
| persisted `planned_qty` | **`7.6981`** |
| persisted `planned_notional_usdc` | `4.08` |

How `7.6981` is produced, in `src/copytrading/execute.rs` (size_ratio branch, lines 516-533, then line 563):

1. `target_qty = round_order_qty_down(38.547059 × 0.2)` = `round_order_qty_down(7.7094118)` = **`7.70`**. Correct: two decimals, as the CLOB requires (`round_order_qty_down` at line 800 exists for exactly this reason).
2. `budget = round_usdc_down(target_qty × limit_price)` = `round_usdc_down(7.70 × 0.53)` = `round_usdc_down(4.081)` = **`4.08`**.
3. The branch returns only `budget`. The shared tail then does `qty = market_buy_shares_for_budget(budget, limit_price, tick_size)` (line 563) = `4.08 / 0.53` truncated to `tick.scale() + 2` = 4 places = **`7.6981`**. The correct `target_qty` from step 1 is never used again.

Step 3 is the right shape for a FAK market BUY: the signed maker amount is the USDC budget, and the share figure is only informational, so four decimals are legal there (`market_buy_shares_for_budget`'s own doc comment says so, line 812). It is the wrong shape for a maker-only GTD limit order, where **the share count is the signed quantity**:

- `src/copytrading/prepare.rs:292` builds the order with `.size(decision.qty)`.
- The SDK rejects any size with more than 2 decimals: `vendor/polymarket_client_sdk_v2_source/src/clob/order_builder.rs:25` (`LOT_SIZE_SCALE = 2`) and `:311-314`. That is the exact error text above.

The flat `max_order_shares` branch never showed this because a whole-number share count times a 2-decimal price is always cent-aligned, so `budget / price` returns the same whole number back. `size_ratio` produces fractional targets, so it almost never round-trips.

### Why the tests did not catch it

- `src/copytrading/execute.rs:2248-2300` (`a_size_ratio_buy_targets_leader_event_size_times_the_configured_ratio`) uses leader size 75, price 0.40, ratio 0.2: target `15`, budget `6.00`, `6.00 / 0.40 = 15.0000`. That is a coincidental round trip. It also has `maker_only: false`, so it never exercises the maker-only path.
- The maker-only orchestrate tests (`src/copytrading/orchestrate/tests.rs:806-833`, `set_maker_only_policy`) deliberately use `max_order_shares: "5"` with `size_ratio: None` (a whole-number target), so they never touch the size_ratio arithmetic either.
- The fake envelope factory's `prepare_post_only_gtd_buy` (`orchestrate/tests.rs:491-512`) copies `decision.qty.to_string()` into the envelope without running the SDK builder, so the SDK's lot-size validation is unreachable from any orchestrate test.

Net effect: sizing is tested with round numbers, GTD preparation is tested with a fake, and the one place the two meet with real arithmetic is only reachable in production.

## Fix

The live execution path belongs to the implementing agent; this section is the specification, not a patch.

### F1 (required): pin the share count to `target_qty` for size_ratio BUYs

In the size_ratio branch, carry `target_qty` out of the branch (for example as a `pinned_qty: Option<Decimal>` alongside `buy_budget`) and use it as `SizedDecision.qty` instead of calling `market_buy_shares_for_budget` at line 563. Invariant to enforce and test: **for `maker_only` decisions, `qty.scale() <= 2 && qty > 0`.**

Leave the derivation at line 563 unchanged for the branches that legitimately want it (the budget-based branches).

### F2 (required, decide together with F1): the reserved budget must be an upper bound of what gets signed

After F1, the signed maker amount for a GTD BUY is `qty × maker_price`, computed by the SDK as `(size × price).trunc_with_scale(tick_decimals + LOT_SIZE_SCALE)` (`order_builder.rs:343`), so four decimals. For intent 726 at the exact limit price: `7.70 × 0.53 = 4.0810`. The persisted `planned_notional_usdc` (the amount `reserve_budget_and_mark_submitting` reserves, `persistent.rs:498-560`) is `round_usdc_down(...)` = `4.08`. The signed order is one tenth of a cent above the reservation when the maker price equals the limit price (it is at or below the limit price in every case, because `maker_price = min(limit_price, best_ask - tick)`).

The gap is at most one cent per order and no code currently compares the two on the GTD path (`prepare_post_only_gtd_buy` does not call `construction_amount`, `prepare.rs:184` is the FAK builder only). But the comment at `execute.rs:805-808` states the invariant the rounding exists to protect ("a signed order cannot exceed ... the persisted rolling-budget reservation"), and for a shares-pinned order rounding **down** breaks it. Recommended: for maker-only size_ratio BUYs, reserve `ceil_to_cent(qty × limit_price)` and use that as `buy_budget` / `planned_notional_usdc`. Check that this still satisfies the `budget > available_collateral` check and `max_order_notional` at the extremes.

### F3 (required, separate root cause): pre-submit minimum order size check

Verified on 2026-09-24 against the public CLOB (`GET /markets/{condition_id}` for the market of intent 726): **`minimum_order_size: 5`** (shares), `minimum_tick_size: 0.01`, `accepting_orders: true`.

With `size_ratio = 0.2`, any leader BUY under 25 shares produces a target under 5 shares. The existing pre-check (`execute.rs:573-580`, "computed buy notional is below the CLOB minimum of 1 USDC") only rejects targets under 1 USDC, so a 2-5 share target (about 1-2.5 USDC) passes it. Intents 724 and 725 were rejected by that check; a slightly larger signal would pass it and reach the venue.

I have not observed a venue rejection for this in this system, so the exact venue error text and status are unverified. The likely consequence, by analogy with intent 721 (`docs/maker-only-post-only-crosses-book-bug.md`), is a definitive 400 on each of the five retry attempts, retry budget exhausted, fuse opened, service exit 21. That is a full outage from one small leader trade, which is why this belongs in the same change rather than a later one.

Fix shape: `GtdMarketSpec` (`orchestrate/mod.rs:46-49`) currently carries only `expires_at` and `tick_size`. Add the market's minimum order size, populated in `derive_gtd_market_spec` (`orchestrate/mod.rs:72-90`) from `market.minimum_order_size` (the field exists on `MarketResponse`, `vendor/.../response.rs:205`; it is already fetched, so no extra network call). In `prepare_maker_only_envelope` (`orchestrate/mod.rs:388-426`), after the market spec is fetched, reject when `decision.qty < min_order_size` via a new `MakerOnlyError` variant that goes through `reject_pre_submit_intent` (a clean pre-submit rejection, no attempt row, no fuse), with a reason that names both numbers.

### Adjacent finding, not live: the FAK path has the same shape problem (by code reading, not executed)

`construction_amount` (`prepare.rs:55-73`) requires, for every shares-pinned BUY on the FAK path, that `qty × limit_price` has at most 2 decimals and equals `buy_budget`. Neither `7.6981 × 0.53` nor `7.70 × 0.53` satisfies that, so a size_ratio FAK BUY with a fractional target would fail there with `NonCentFixedShareMakerAmount`/`FixedShareBudgetMismatch`. No enabled leader is affected (leader 1, the only non-maker-only leader, is disabled and has no `size_ratio`). Recommended minimal handling: either reject `size_ratio` on non-`maker_only` leaders at apply time (`setup.rs`), or reject it at sizing with an explicit reason. Do not silently change FAK sizing in the same commit.

## Verification

Tests to add (all offline, none contact the venue):

1. **Sizing, the actual production numbers.** size_ratio `0.2`, `maker_only: true`, leader event size `38.547059`, event price `0.5058751693611697`, abs tolerance `0.02`, tick `0.01`. Assert `decision.qty == 7.70`, `decision.qty.scale() <= 2`, `decision.limit_price == 0.53`, and (per F2) `decision.buy_budget >= qty × limit_price`.
2. **Table-driven scale invariant.** A loop over at least 20 (event size, price) pairs with awkward fractions (for example 38.547059, 27.3, 91.91, 25.0, 33.333333, prices 0.07 to 0.93). For every `maker_only` size_ratio decision assert `qty > 0 && qty.scale() <= 2 && qty × limit_price` has at most 4 decimals. This is the test that would have caught 726, because the fake factory cannot run the SDK's check.
3. **Full pipeline through orchestrate.** A `size_ratio: Some("0.2")`, `maker_only: true` fixture (the existing `set_maker_only_policy` only covers `max_order_shares`) driven through `execute_one_intent_with_marker` to a persisted GTD envelope. Assert the envelope `size` has at most 2 decimals and the persisted `planned_qty` equals it.
4. **Minimum size (F3).** Fake market spec with minimum `5`: qty `4.99` is rejected pre-submit (intent `rejected`, no `order_attempts` row, fuse empty, reason names 4.99 and 5); qty `5.00` proceeds.
5. Update the existing size_ratio test (`execute.rs:2248-2300`) or add a sibling with a non-round leader size, so its coincidental `15` no longer stands as the only coverage.

Suite gates: `cargo test --all-features --locked` and `cargo clippy --all-targets --all-features --locked -- -D warnings` clean.

Production verification after deploy, on the next leader 2 signal of at least 25 shares (read-only; on the remote host use `python3` with `sqlite3` and `mode=ro`):

```sql
SELECT i.id, i.status, i.planned_qty, i.planned_price, i.planned_notional_usdc, i.rejection_reason,
       a.status AS attempt_status, a.requested_qty, a.venue_status, a.failure_detail
FROM copy_intents i LEFT JOIN order_attempts a ON a.intent_id = i.id
WHERE i.id > 726 ORDER BY i.id DESC, a.attempt_number;
```

Expected: `planned_qty` and `requested_qty` at most 2 decimals; envelope `order_type = GTD`, `post_only = true`; price equal to `min(limit_price, best_ask - tick)`; attempt `accepted` (resting) or terminal without `order crosses book` or lot-size text; `persistent_execution_fuse` empty. Also expect signals under 25 shares to be rejected with the new minimum-size reason instead of reaching the venue.

## Not part of this problem

- Intents 724/725 (`computed buy notional is below the CLOB minimum of 1 USDC`) are correct rejections, not bugs. They are what the existing pre-check is for; F3 adds the stricter share-count floor on top.
- The crossing fix (`min(limit_price, best_ask - tick)`), the `condition_id` lookup, and the GTD lookup/reconciliation work are untouched and behaved correctly on intent 726: preparation failed before any submission.
- The uncommitted working-tree changes currently in `execute.rs`/`orchestrate`/`reconcile.rs` (`finalize_receipt` attempt-state finalization, no-FAK maker fallback for size_ratio) are a separate change; F1-F3 should be reviewed against them, since both touch `execute.rs` sizing and `orchestrate/mod.rs` maker-only preparation.
