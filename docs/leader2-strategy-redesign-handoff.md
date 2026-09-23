# Leader 2 strategy redesign: proportional sizing, no per-direction cap, maker-only execution

**Note**: an earlier version of this doc was written for leader 1 --
address mix-up on my part (account owner confirmed the target address,
`<leader-2-address>`, is leader 2, not leader 1).
The mechanisms below are unchanged (all per-leader-configurable), but the
*why* is corrected: this is leader 2's own confirmed behavior, not a
statistic borrowed from leader 1's unrelated rejection profile.

**Note 2**: as of this session, leader 1 (`leader-fixed-5-shares`,
`<leader-1-address>`) has been **disabled entirely**
by account-owner request (`enabled: false`, applied via `copy_config_apply`,
verified directly against the live DB: `leader_config` shows
`(1, 'leader-fixed-5-shares', 0)`). This is a config-only change, already
live. Leader 2 (`leader2-fixed-5-shares`) remains the only active leader on
this account; everything below is scoped to leader 2 only.

**Note 3**: this version replaces Part 3's earlier "FOK-first, GTD-maker
fallback only if FOK fails" proposal. The account owner's latest,
more complete instruction is simpler and supersedes it: leader 2 should
submit **only** a GTD/maker order (no taker attempt at all), sized off
Part 1, with a new rule for choosing its price. See Part 3 below; the old
FOK idea is kept as a non-blocking side note at the end of that section.

Account owner's request, informed directly by leader 2's own real trading
pattern found this session: of leader 2's ~1758 markets traded in
September, **400 (22.8%) had both outcome tokens bought** -- a first
directional bet, then (when the trend reversed) a flip to the opposite
side, sometimes followed by adding more as that new side's price confirmed
(see the session's leader2 hedge/pyramid analysis for full trade sequences).
Today's copy setup can only ever capture the *first* leg of that pattern:
the account-wide `market_direction_order_locks` mechanism permanently
blocks any later copy order in the same market+direction, so the flip and
any subsequent add-on never get copied at all.

Five changes requested across this and prior sessions; two already applied
(config only), three need code.

## Done: `max_signal_age_seconds` 3s -> 30s (leader 2)

Applied via `copy_config_apply` against the live DB. No code change.
Config verified: `leader_policy.max_signal_age_seconds = 30` for
`leader_id = 2` (leader 1 was also `30`, set in an earlier, separately
-justified pass, before leader 1 was disabled entirely -- see Note 2 above).

## Done: leader 1 disabled entirely

Applied via `copy_config_apply` against the live DB (`enabled: false`,
leader 2's policy passed through unchanged in the same payload -- the
declarative apply requires every leader in the JSON, see
`docs/adr/0001-trading-config-apply-is-declarative.md`). No code change.
Verified directly: `SELECT id, label, enabled FROM leader_config` ->
`[(1, 'leader-fixed-5-shares', 0), (2, 'leader2-fixed-5-shares', 1)]`.

## 1. Proportional sizing: size = leader's own trade size x 1/5

**Not implementable as a config-only change under today's schema.**
`leader_policy` currently has exactly two BUY sizing modes
(`docs/fixed-shares-sizing-handoff.md`): `max_order_shares` (fixed target)
and `max_order_notional` (budget cap, `qty` derived by division). Neither
is "a ratio of whatever the leader just did." This is a third mode.

**Schema**: new nullable `leader_policy.size_ratio` column (`TEXT`, decimal
string, e.g. `"0.2"` for 1/5), same additive-migration shape as
`max_order_shares` (`migrations/0012_leader_fixed_shares_sizing.sql` is the
template). Validate as a positive decimal, probably capped at `1` (a ratio
above 1 is "size up," a different feature not asked for here) --
`setup.rs`'s `parse_positive_decimal` already does the parse-and-validate
half of this.

**Sizing** (`execute.rs`, the `Side::Buy` branch, same spot the
`max_order_shares` arm lives): if `policy.size_ratio` is set,
`target_qty = event_size * ratio` (rounded to the market's tick/lot
convention the same way `max_order_shares` already is), then the exact same
downstream treatment `max_order_shares` gets today -- collateral check,
reject-not-downsize on insufficient funds. Pick one explicit precedence if
a leader somehow has both `max_order_shares` and `size_ratio` set
(shouldn't happen given how config is applied here, but `execute.rs`'s doc
comment should say which one wins so a future misconfiguration fails
loudly instead of silently picking whichever branch is checked first).

**Why proportional, specifically for this leader**: leader 2's own leg
sizes vary a lot by conviction ($15-36 in the real sequences found this
session, not a flat constant) -- a fixed 10-share copy treats an
exploratory first leg and a confirmed pyramid-add identically. Sizing off
the leader's own per-trade amount at least tracks *relative* conviction
between legs, which flat fixed-shares structurally cannot.

**Interaction with the overfill finding -- now moot for leader 2.**
`docs/fixed-shares-overfill-and-price-floor.md` established that the FAK
overfill mechanism (venue spends the maker USDC budget regardless of
construction method, so a favorable price returns more shares than
signed) applies to *taker* (FAK) orders. Part 3 below removes leader 2's
taker attempt entirely -- every leader 2 BUY becomes a GTD/maker order,
which this session separately confirmed (via leader 1's own
`order_attempts`, `attempt_number=2` GTD fills) lands at *exactly* the
signed share target, with none of the FAK variance. So for leader 2
specifically, `target_qty` from this section is what actually fills, not
just a nominal baseline to measure overfill against -- the overfill doc's
Fix 2 (symmetric taker price floor) remains relevant only if a future
leader is ever configured taker-primary again.

## 2. Remove the one-copy-order-per-market-direction limit -- for leader 2 only

**Where it lives**: `src/copytrading/plan.rs:102-151`, backed by
`market_direction_order_locks` (`migrations/0016_one_copy_order_per_market_direction.sql`).
It's account-wide, not per-leader, and permanent -- an `INSERT OR IGNORE`
keyed on `(account_id, condition_id, token_id)`, never released. First copy
order for a given market+outcome, ever, wins; every later signal for that
exact direction becomes an auditable rejection. This is exactly the
mechanism that stops us from ever copying leader 2's second and third legs
in the 400-market hedge/pyramid pattern above -- the migration's own stated
purpose ("a copy of YES must not silently prohibit a later copy of NO")
already lets the *opposite*-side flip through; what's blocked is a *second*
add on the side already held (e.g. the third line of the `0x33a11cce...`
example this session found: Up at 0.462, Up again at 0.420, Up again at
0.586 -- only the first of those three would ever become a copy order
today).

**Scope this to leader 2 specifically, not a global removal.** Add a new
`leader_policy.allow_repeated_market_direction` boolean (same pattern as
`balance_within_market`: default `false`, every existing leader
unaffected) and skip the `market_direction_order_locks` check in
`plan.rs` when the triggering event's leader has it set, rather than
deleting the mechanism. Leader 1 is disabled entirely (Note 2 above), so
this is moot for it either way, but the flag stays leader-scoped rather
than global in case leader 1 (or a future leader) is ever re-enabled.

**This removes a real safety backstop -- flag the consequence explicitly,
not just the mechanism.** Once removed for leader 2, every repeated
same-direction signal becomes a new, separately-sized copy order (at
whatever Part 1 computes). The account-level `rolling_budget_usdc` /
`budget_window_seconds` cap still bounds total spend over that leader's
budget window, so this isn't unbounded -- but it's a real shift from "at
most one bite" to "as many bites as the budget window allows," and
combined with proportional sizing, a leader who fires several same-direction
legs in one market now consumes roughly (legs) x (their own size x 1/5) of
the budget window on a single market, not one fixed bite. Worth explicitly
re-confirming `rolling_budget_usdc` / `budget_window_seconds` for leader 2
still make sense once this ships, rather than assuming the existing numbers
fit the new behavior.

## 3. Maker-only execution: skip the taker attempt, GTD-maker as the only order, price capped at real-time best ask

**Currently**: `execute_one_intent_with_marker` (`orchestrate/mod.rs:324-400`)
always builds and submits a FAK order first (`envelopes.prepare(&decision)`
at line 382, which internally uses `OrderType::FAK` -- `prepare.rs:192/203/214`).
The GTD/maker order (`prepare_post_only_gtd_buy`, `prepare.rs:276-...`,
using `decision.limit_price` and `decision.qty` unchanged) is reachable
**only** via `maybe_retry_no_fak_once` (`orchestrate/mod.rs:648-698`),
gated on the FAK attempt having been submitted, rejected, and its rejection
text matching `%no orders found to match with FAK order%`
(`is_explicit_no_fak_rejection`, lines 617-631), and only on the first such
rejection (`no_match_count == 1`, line 676).

**Change 1 -- new per-leader flag, `leader_policy.maker_only`.** Same
pattern as `balance_within_market` (already precedented end-to-end):
- Migration: add `maker_only INTEGER NOT NULL DEFAULT 0 CHECK (maker_only IN (0,1))`
  to `leader_policy`, same shape as
  `migrations/0013_leader_balance_within_market.sql:17`.
- `LeaderPolicyInput` (`setup.rs:92`, add a field next to
  `balance_within_market` at `setup.rs:118-119`):
  `#[serde(default)] pub maker_only: bool,`
- Thread it through `NormalizedPolicy` (`setup.rs:625-639`),
  `insert_policy`'s bind (`setup.rs:503-534`, alongside the
  `balance_within_market` bind at line 528), `StoredPolicy` +
  `update_policy_if_changed` (`setup.rs:540-613`, alongside line 613).
- `PolicySnapshot` (`plan.rs:386-411`): add
  `#[serde(default)] pub maker_only: bool,` next to the existing
  `balance_within_market` field -- same "old snapshots still deserialize"
  reasoning already documented there for that field.

**Change 2 -- have `SizedDecision` carry the flag so `orchestrate/mod.rs`
can branch on it without a second policy read.** `size_and_reserve`
(`execute.rs:247`) already loads `PolicySnapshot` internally (`execute.rs:275`,
`policy = load_policy_snapshot(...)`) and has `policy` in scope at all
three `SizedDecision` construction sites (`execute.rs:278-286`, `580`,
`694` -- the same places `policy.balance_within_market` is already read,
e.g. line 482). Add `pub maker_only: bool` to `SizedDecision` itself
(`venue/execution_contract.rs:85-98`) and set it from `policy.maker_only`
at each of those three construction sites. This keeps the branch point in
`orchestrate/mod.rs` a single already-in-scope field read, not a second DB
round-trip.

*(Alternative considered and rejected: query `leader_policy` directly by
`claimed.leader_id` inside `orchestrate/mod.rs` instead of extending
`SizedDecision`. Simpler to review in isolation, but duplicates the policy
read `size_and_reserve` already did one line earlier for no benefit --
prefer the field addition.)*

**Change 3 -- branch in `execute_one_intent_with_marker` before the FAK
prepare.** At `orchestrate/mod.rs:366-382`, after `decision` is bound and
before `envelopes.prepare(&decision)` is called:

```rust
let envelope = if decision.maker_only {
    let expires_at = /* same market-close lookup submit_post_only_gtd_after_initial_no_fak
                         already does via market_spec_for_gtd, orchestrate/mod.rs:148-153 / 1264-1277 */;
    let priced_decision = apply_best_ask_price_cap(&envelopes, &decision).await?; // Change 4
    envelopes.prepare_post_only_gtd_buy(&priced_decision, expires_at).await
} else {
    envelopes.prepare(&decision).await
};
```

(pseudocode -- match the existing `match ... { Ok/Err }` error-handling
shape already at lines 382-398, don't drop the `reject_pre_submit_intent`
local-failure path). The second, structurally identical
`size_and_reserve` -> `envelopes.prepare` pair further down the same file
(`orchestrate/mod.rs:826`, `843`, inside the resume/retry path) needs the
same branch -- a `maker_only` leader whose intent is resumed after a
restart must not fall through to the FAK path there either.

**Change 4 -- price rule: use the real-time best ask instead of the
leader-derived price, when the ask is lower.** `decision.limit_price` is
`event_price` widened by `price_tolerance_bps`/`price_tolerance_abs` as a
BUY *ceiling* (`execute.rs:419-431`, `apply_tolerance` at
`execute.rs:715-737` -- confirmed this only ever raises the price above
`event_price`, never lowers it; the symmetric floor proposed as "Fix 2" in
`docs/fixed-shares-overfill-and-price-floor.md` is still unimplemented and,
per Part 1's note above, no longer needed for leader 2 once this ships).
For a maker-only leader, fetch the current top-of-book ask immediately
before submission and use it instead when it's more favorable:

```
best_ask = fetch current lowest ask for decision.token_id   // new small helper, see below
maker_price = if best_ask < decision.limit_price { best_ask } else { decision.limit_price }
```

Then build the GTD envelope with `maker_price` in place of
`decision.limit_price` (clone `decision` with `limit_price: maker_price` --
`prepare_post_only_gtd_buy`'s signature doesn't need to change).

*Order-book fetch: don't reuse `no_fak_sweep_quote` as-is.* The machinery
for fetching a fresh order book already exists and is precedented --
`sweep_quote_for_no_fak_retry` (`orchestrate/mod.rs:1239-1262`) calls
`client.order_book(&request)` via the SDK's `OrderBookSummaryRequest`, and
`EnvelopeFactory` (`orchestrate/mod.rs:136-142`) already declares the trait
method. But `no_fak_sweep_quote` (`orchestrate/mod.rs:94-117`) answers a
different question -- it sweeps multiple ask levels to find the VWAP that
would fill a *target size*, for the (currently disabled, dead-code)
FAK-retry idea. What's wanted here is simpler: the single best (lowest) ask
price, full stop, regardless of size at that level. Add a small sibling
helper (e.g. `best_ask_price(asks) -> Option<Decimal>`, a `min_by_key` over
`book.asks`) rather than routing through the sweep logic -- reuse the SDK
request/`order_book()` call, not the VWAP helper.

*Why this direction, mechanically*: `prepare_post_only_gtd_buy` builds a
`post_only` GTD order (`prepare.rs:294-296`). A BUY resting order that
prices at or above the current best ask would cross the book on arrival,
which a post-only order cannot do -- so if `decision.limit_price` (the
leader-derived ceiling) is stale and now sits above the live best ask,
submitting at `decision.limit_price` risks an immediate post-only
rejection rather than a resting order at all. Capping at `best_ask` keeps
the order valid *and* gets a better price when the market has moved in our
favor since the leader's own fill.

**Open question worth an empirical check before shipping**: whether the
venue requires strictly-below-best-ask (`price < best_ask`, by at least
one tick) or accepts exactly-at-best-ask for a post-only BUY -- the account
owner's instruction is literally "用 best ask 价格" (use the best-ask price
itself). If the venue rejects an exact match as still crossing, the
implementor should adjust to `best_ask - tick_size` and note that
deviation explicitly in the commit, rather than silently guessing here.

**Failure mode to pick explicitly**: if the order-book fetch itself fails
(network error, empty book, no asks), decide whether to reject the intent
or fall back to `decision.limit_price` -- silently falling back risks
submitting at a stale/crossing price with no safety net. Failing closed
(treat it as a local prepare failure via the existing
`reject_pre_submit_intent` path already used at `orchestrate/mod.rs:390-396`)
is the safer default; call it out as a decision for the implementor to
confirm rather than assuming.

**Interaction with Part 1 (proportional sizing) and Part 2 (no
per-direction cap)**: both are unaffected by this change --
`decision.qty` (from `size_ratio`) and whether a given signal survives the
`market_direction_order_locks` check both resolve before this point in
`execute_one_intent_with_marker`; maker-only execution only changes *how*
the already-sized, already-admitted decision gets submitted.

**FOK, still an open side note, no longer blocking**: an earlier version
of this doc proposed swapping the first attempt's `OrderType::FAK` to
`OrderType::FOK`, keeping GTD as a fallback. That's superseded for leader 2
by this maker-only design -- there's no "first attempt" left to swap. The
underlying empirical question (does a FOK fill ever land above its signed
share amount, the way confirmed FAK behavior does) is still unanswered and
might be worth checking some day if a *taker*-primary leader is ever
configured again, but it's no longer part of leader 2's design and
shouldn't block shipping this.

## Verification

- New tests for `size_ratio` sizing (mirroring
  `fixed_share_buys_keep_the_configured_target_across_prices`, but with
  the target computed from a mocked leader event size, not a config
  constant).
- New test confirming `allow_repeated_market_direction` on one leader
  doesn't affect another leader's lock behavior in the same market (moot
  today with leader 1 disabled, but keep the test leader-scoped rather
  than assuming only one leader will ever be active).
- New test confirming `maker_only` leaders skip the FAK path entirely and
  go straight to `prepare_post_only_gtd_buy`, on both the fresh-intent
  path (`orchestrate/mod.rs:366-382`) and the resume/retry path
  (`orchestrate/mod.rs:826/843`).
- New tests for the `best_ask_price` helper (multiple levels, empty book,
  single level) and for the `maker_price = min(decision.limit_price,
  best_ask)` selection logic, including the order-book-fetch-failure case
  (should fail closed via `reject_pre_submit_intent`, per the decision
  above -- test that it does, not that it silently falls back).
- `cargo test --all-features --locked` and
  `cargo clippy --all-targets --all-features --locked -- -D warnings` stay
  green throughout.
