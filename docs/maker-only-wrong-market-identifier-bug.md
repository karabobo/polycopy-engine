# maker_only path calls market_spec_for_gtd with token_id instead of condition_id

## Severity

**Live production bug, currently 100% blocking.** Since the leader 2
maker-only restart (`852595d`, service restarted 2026-09-23 17:25:51 CST),
every leader 2 signal that reaches `prepare_maker_only_envelope` has been
rejected at the market-lookup step. Two real signals confirmed so far
(`copy_intents.id` 705 and 706), both leader 2, both rejected with the
identical failure shape within the first 35 minutes of the new config being
live.

**Not a safety risk.** Both rejections are pre-submit: no request reached
the venue, no order was signed or sent. The fail-closed path added this
session (`MakerOnlyError::MarketSpec` -> `reject_pre_submit_intent`) did
exactly what it was built to do. The bug's actual cost is "leader 2 copies
nothing right now," not "leader 2 copies incorrectly."

## Root cause

`src/copytrading/orchestrate/mod.rs:395`, inside `prepare_maker_only_envelope`:

```rust
let market = envelopes
    .market_spec_for_gtd(&decision.token_id)   // wrong identifier
    .await
    .map_err(MakerOnlyError::MarketSpec)?;
```

`market_spec_for_gtd`'s own parameter is named and documented as
`condition_id` (`orchestrate/mod.rs:1501-1503`), and its implementation
calls `client.market(&condition_id)` (`orchestrate/mod.rs:1508-1509`) --
the CLOB's `/markets/{condition_id}` endpoint, keyed on the market's
condition id (a `0x`-prefixed 66-char hex string), not the ERC1155 outcome
token id (a large decimal `U256`) that `decision.token_id` actually holds.
`SizedDecision` (`src/venue/execution_contract.rs:85-98`) has no
`condition_id` field at all -- only `token_id` -- so `&decision.token_id`
was the only thing in scope to reach for at that call site, and it's the
wrong value.

The correct precedent already exists in the same file, in the pre-existing
(pre-this-session) FAK -> GTD fallback path:

```rust
// orchestrate/mod.rs:911, inside maybe_retry_no_fak_once
let market = match envelopes.market_spec_for_gtd(&retry_claimed.condition_id).await {
```

That call site reaches for `.condition_id` off a `ClaimedIntent`, not off
the `SizedDecision`. `ClaimedIntent` (`src/copytrading/execute.rs:116`)
does carry `pub condition_id: String`, and it's already in scope, unused,
at both of the new maker-only call sites.

## Real production evidence

Queried directly against the live DB (`copy_intents` joined to
`leader_events` for the real `condition_id`):

| intent | token_id (what was sent) | real condition_id (what should have been sent) | rejected at |
|---|---|---|---|
| 705 | `15073637138447290028304609420179554936209527168880576155933099898241884819058` | `0xd7f4ab6d1e6a956fa503ea4b225d6e77e243bc99f37d02a68e2bf8b7bbeae052` | 2026-09-23T09:28:36.504Z |
| 706 | `23684839897644981601818975725677117822377087932883349168330945633710396370056` | `0xb0f69527d6a07ac237f73a6bd2f563dd08bcb040576af16e4248c59239c0b096` | 2026-09-23T09:59:00.674Z |

Both `copy_intents.rejection_reason` read:

```
maker-only GTD market-end lookup failed: market end lookup failed: Status:
error(404 Not Found) making GET call to /markets/<token_id> with
{"error":"market not found"}
```

The venue 404s because a decimal token id was sent to a path that expects
a hex condition id -- this is not a flaky-network repeat of the earlier
`intl_clob` test flake, and not the same class of bug as either of the two
GTD fixes shipped earlier this session (`docs/gtd-market-end-lookup-bug.md`,
`docs/gtd-maker-expiry-too-short.md` -- both of those were about *which
fields of a correctly-identified market* to trust, not about *which
identifier* to look the market up by).

## Fix

`prepare_maker_only_envelope` needs the intent's real `condition_id`, and
the only place that has it is the caller. Add a parameter:

```rust
async fn prepare_maker_only_envelope<F>(
    envelopes: &F,
    condition_id: &str,          // new
    decision: &SizedDecision,
    now: DateTime<Utc>,
) -> Result<MakerOnlyEnvelope, MakerOnlyError>
where
    F: EnvelopeFactory,
{
    let market = envelopes
        .market_spec_for_gtd(condition_id)   // was &decision.token_id
        .await
        .map_err(MakerOnlyError::MarketSpec)?;
    ...
```

Both call sites already have a `ClaimedIntent` named `claimed` in scope and
just need to pass `&claimed.condition_id` through:

- `orchestrate/mod.rs:511`, inside `execute_one_intent_with_marker`:
  `prepare_maker_only_envelope(envelopes, &decision, now).await` ->
  `prepare_maker_only_envelope(envelopes, &claimed.condition_id, &decision, now).await`
  (`claimed: ClaimedIntent` is already bound earlier in this function, from
  `claim_or_resume_intent`).
- `orchestrate/mod.rs:1034`, inside `prepare_new_attempt`:
  same change. This function's own parameter is already named `claimed:
  &ClaimedIntent` (`orchestrate/mod.rs:1002`), so it's `&claimed.condition_id`
  here too, no new plumbing needed to get it into scope.

No other call site of `prepare_maker_only_envelope` exists (grep confirms
exactly two, matching the two listed above).

## Verification

- The existing maker-only tests from `1bb25e6`
  (`a_maker_only_buy_skips_the_fak_path_on_the_fresh_intent_path`,
  `a_maker_only_buy_on_the_retry_path_also_skips_the_fak_path`, etc., in
  `orchestrate/tests.rs`) evidently didn't catch this -- worth checking
  whether their fake `EnvelopeFactory::market_spec_for_gtd` implementation
  asserts *which* identifier string it was called with, or just returns a
  fixed `GtdMarketSpec` regardless of input. If it ignores its argument,
  add an assertion there (e.g. the fake records the last identifier it was
  called with, and the test asserts it equals the seeded `condition_id`,
  not the seeded `token_id`) so this class of bug can't reappear silently.
- Add a small regression test that seeds a `ClaimedIntent` whose
  `condition_id` and `token_id` are deliberately different-looking strings
  (like the real production pair above -- one hex, one decimal) and asserts
  `prepare_maker_only_envelope` is invoked with the `condition_id` one.
- After the fix, `cargo test --all-features --locked` and
  `cargo clippy --all-targets --all-features --locked -- -D warnings` stay
  green, same as `1bb25e6`'s own bar.
- Post-deploy: reconcile the two already-rejected intents (705, 706) are
  expected to stay rejected -- they're not retried automatically -- and
  confirm the *next* real leader 2 signal after the fix lands produces a
  `copy_intents` row that reaches `order_attempts` with `order_type = GTD`
  instead of failing at the market-lookup step.
