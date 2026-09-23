# maker-only GTD orders always cross the book at exact best_ask, halting the whole service

## Severity

**Live production outage, service currently down.** After the market-
identifier fix (`0f58d10`) deployed and the service restarted cleanly at
2026-09-23 19:29:34 CST, the very first leader 2 signal that reached the
venue (`copy_intents.id` 721, 20:03:xx CST) was rejected by Polymarket on
**every one of its 5 retry attempts**, exhausted the retry budget, and
caused `copy_persistent` to open its execution fuse and exit (status 21,
"persistent execution fuse is open"). The fuse is a persisted DB row
(`persistent_execution_fuse`), not in-memory state: the next
`systemctl restart` will fail immediately at the startup fuse check
(`persistent.rs:453-463`) without even reaching this bug again. **Do not
restart until the fix below lands** -- a plain restart will not recover
service, and once the fuse is manually cleared without the fix, the very
next leader 2 signal will very likely repeat this exact crash.

This is a different, follow-on bug from `0f58d10` (the market-identifier
fix) -- that fix worked (the order legitimately reached the venue this
time, proving the `condition_id` lookup now succeeds) and immediately
surfaced this next issue.

## Root cause

Real production evidence, `order_attempts` for `copy_intents.id = 721`
(all 5 attempts, ~7 seconds apart):

| attempt | price | order_type | post_only | venue response |
|---|---|---|---|---|
| 1 | 0.22 | GTD | true | 400: `invalid post-only order: order crosses book` |
| 2 | 0.21 | GTD | true | 400: `invalid post-only order: order crosses book` |
| 3 | 0.22 | GTD | true | 400: `invalid post-only order: order crosses book` |
| 4 | 0.22 | GTD | true | 400: `invalid post-only order: order crosses book` |
| 5 | 0.22 | GTD | true | 400: `invalid post-only order: order crosses book` |

This settles, with real venue evidence, the question flagged as open in
`docs/leader2-strategy-redesign-handoff.md`'s Change 4 and repeated in
`0f58d10`'s commit message ("if post-deploy monitoring shows post-only
rejections due to crossing, the next commit switches to `best_ask -
tick_size`"): **the venue rejects a post-only BUY priced exactly at the
current best ask as crossing, unconditionally.** This isn't intermittent --
5/5 attempts failed identically, at two different prices as the book moved
between retries, so it isn't one stale quote either.

`src/copytrading/orchestrate/mod.rs:411-415`, inside
`prepare_maker_only_envelope`:

```rust
let maker_price = if best_ask < decision.limit_price {
    best_ask                    // exact venue best ask -- always crosses
} else {
    decision.limit_price
};
```

The `else` branch (using the leader-derived `decision.limit_price`
unchanged) is only safe when `decision.limit_price` is *strictly* below
the live best ask; if they're ever exactly equal it has the identical
crossing problem, just not yet observed in production.

## Consequence chain (why the whole service went down, not just this order)

1. All 5 attempts for intent 721 rejected -> `blocked_recovery` retry
   budget exhausted -> `reconciliation_cases` id 3 opened (`detail: "retry
   budget exhausted"`, unresolved).
2. `copy_persistent` opens `persistent_execution_fuse` for account 1
   (`reason: "retry budget exhausted"`, `actor_source: "copy_persistent"`)
   and exits with `PersistentError::FuseOpen` (exit code 21).
3. The fuse check at startup (`persistent.rs:453-463`,
   `assert_startup_clear`-adjacent path) means **every subsequent start
   attempt fails immediately**, before reaching any leader/order logic,
   until an operator runs `persistent_control resume <reason>` (per the
   command list in `src/bin/persistent_control.rs`) to clear the fuse row
   -- this is deliberate, not a bug: a repeated-failure halt is supposed to
   require a human to look at it before resuming.

## Fix

`GtdMarketSpec` already carries `tick_size: Decimal`
(`orchestrate/mod.rs:45-48`, populated from `market.minimum_tick_size` in
`derive_gtd_market_spec`), and `prepare_maker_only_envelope` already binds
`market: GtdMarketSpec` from the `market_spec_for_gtd` call two lines above
this logic (`orchestrate/mod.rs:394-397`) -- no new parameter or plumbing
needed, just use what's already in scope:

```rust
let maker_price = std::cmp::min(decision.limit_price, best_ask - market.tick_size);
```

This replaces the `if/else` entirely: it keeps the "use the leader price
when it's already better" behavior from the `else` branch, keeps the
"use the market when it's better" intent from the `if` branch, and
guarantees the final price is strictly below the live best ask by at least
one tick in both cases -- closing the exact-equality gap the old `else`
branch left open too.

Deviation from the account owner's literal 2026-09-23 instruction ("用
best ask 价格", not "best ask minus a tick") is now justified by direct
venue evidence rather than speculation -- the literal reading is provably
unsubmittable, not just theoretically risky. Worth a one-line confirmation
back to the account owner when this ships, same as `0f58d10`'s commit
documented why it deviated from the original design doc's ordering.

## Verification

- Update `a_maker_only_buy_uses_min_of_leader_price_and_real_time_best_ask`
  (added in `1bb25e6`) to assert the resulting price is `best_ask -
  tick_size`, not `best_ask` -- if that test currently asserts exact
  `best_ask`, it was passing against the same wrong assumption the real
  venue just rejected, and needs its expected value corrected, not just a
  new test added alongside it.
- Add a case at exact equality (`decision.limit_price == best_ask`) proving
  the new `min(...)` formula also prices strictly below the ask there --
  the old `else` branch had this same gap unobserved in production; worth
  covering explicitly now that the mechanism is understood.
- `cargo test --all-features --locked` and
  `cargo clippy --all-targets --all-features --locked -- -D warnings`
  green, matching every prior change this session.

## Recovery sequence (after the fix is verified, not before)

**The existing `resolve-exhausted-fak-no-match` command does not apply to
this case -- checked, not assumed.** Its implementation
(`persistent.rs:805-840`) requires every `order_attempts` row for the
intent to match `failure_detail LIKE '%no orders found to match with FAK
order%'` before it will resolve the case. Intent 721's five attempts all
have `failure_detail = 'invalid post-only order: order crosses book'` --
none match that pattern, so this command will fail closed (most likely
`PersistentError::UnresolvedRecovery`) rather than resolve
`reconciliation_cases.id = 3`. There is currently no existing operator
command that resolves a `blocked_recovery` / "retry budget exhausted" case
caused by post-only crossing rejections instead of FAK no-match
rejections.

1. Deploy the price fix above (`deploy/remote-release.sh`, same as the
   last two rounds).
2. Add a small sibling to `resolve_exhausted_fak_no_match` --
   e.g. `resolve_exhausted_maker_only_crossing(pool, account_id,
   intent_id)` in `persistent.rs`, same shape (same `blocked_recovery` /
   `'retry budget exhausted'` case lookup at lines 817-829) but checking
   `failure_detail LIKE '%order crosses book%'` instead of the FAK
   no-match string at line 838, wired to a new `persistent_control`
   subcommand the same way `resolve-exhausted-fak-no-match` is wired
   (`src/bin/persistent_control.rs:155-175`). Confirm the "all attempts
   verified" invariant that function enforces before writing the new one
   -- don't just relax the WHERE clause without understanding why it
   requires 100% of attempts to match, not just the majority.
3. Run the new resolve command for intent 721, then
   `persistent_control resume "fixed post-only crossing at exact
   best_ask, see docs/maker-only-post-only-crosses-book-bug.md"` to clear
   the fuse.
4. Restart the service.
5. Watch the next real leader 2 signal end-to-end: confirm the resulting
   `order_attempts` row prices strictly below the best ask at submission
   time and does not repeat the crossing rejection.
