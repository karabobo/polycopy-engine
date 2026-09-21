# GTD maker fallback is now 100% non-functional: `GTD_MAKER_EXPIRY` violates the venue's real minimum

**Urgent correction to `docs/gtd-market-end-lookup-bug.md`**, which this repo
already implemented and deployed (commit `ca3186f`, live on production as of
2026-09-21 15:59 CST). That fix corrected the `end_date_iso` misuse but chose
a expiry duration that the venue rejects outright. Net effect: the post-only
GTD maker fallback, which used to succeed *some* of the time (whenever
`end_date_iso` happened to still be in the future), now fails **every single
time**, unconditionally.

## What happened (production evidence, post-deploy)

`copy_intents` id `536` (leader 2, BUY, created `2026-09-21T08:32:26Z` --
well after the fix went live at `07:59:34Z`), final status: `rejected`,
`"unexpected repeated no-FAK rejection after maker fallback selection"`.

Its three `order_attempts`:

1. `id=373`: FAK submitted, `400`, `"no orders found to match with FAK
   order..."` -- a normal, expected first-leg miss.
2. `id=374`, same `submission_started_at`: the post-only GTD maker attempt,
   `400`, **`"expiration is less than 180 seconds in the future"`** -- the
   venue's own validation, rejecting our order outright before it ever
   reaches the book.
3. `id=375`, ~3s later: a *second* FAK attempt, also `400` "no orders found
   to match."

## Root cause

`GTD_MAKER_EXPIRY` (`src/copytrading/orchestrate/mod.rs`) is currently:

```rust
pub const GTD_MAKER_EXPIRY: chrono::Duration = chrono::Duration::seconds(30);
```

The venue requires a GTD order's `expiration` to be **at least 180 seconds
in the future**. `derive_gtd_market_spec` computes `expires_at = now +
GTD_MAKER_EXPIRY`, i.e. `now + 30s` -- always short of the venue's floor, so
the submission is rejected before matching is ever attempted. This constant
was chosen as the docs' own illustrative "short window" example when the
original bug was diagnosed; nobody had checked it against the venue's actual
minimum-expiration rule at the time. My mistake to flag specifically: I
reviewed and endorsed the `closed`/`accepting_orders` logic fix and its
tests, but did not check the `30`-second value itself against real venue
behavior before signing off on it.

Consequence: every post-only GTD attempt now fails with the same
`"expiration is less than 180 seconds"` venue error, 100% of the time --
worse in practice than the bug it replaced, which at least sometimes
succeeded by chance.

## Why the second FAK attempt (and the confusing final rejection) happens too

`handle_post_only_gtd_selection` (or whichever orchestration entry point
wraps this -- see `src/copytrading/orchestrate/mod.rs:655-691`) expects
*exactly one* prior no-FAK rejection when it reaches this branch
(`no_match_count == 1` routes to the GTD attempt; anything else is treated
as an invariant violation and produces the `"unexpected repeated no-FAK
rejection after maker fallback selection"` message). Because the GTD
attempt fails venue-side (not through the no-FAK path) and something in the
retry flow falls back to submitting another FAK, a second no-FAK rejection
lands on the same intent, and the invariant check -- correctly, given its
assumptions -- flags the state as unexpected and rejects the whole intent.
Fixing `GTD_MAKER_EXPIRY` removes the trigger; the fallback-to-a-second-FAK
behavior on a failed GTD *submission* (as opposed to a failed GTD *market
lookup*, which is a separate, already-handled path) may be worth a second
look once the primary fix lands, but isn't this doc's main claim -- I have
one data point, not a characterized pattern.

## The fix

```rust
pub const GTD_MAKER_EXPIRY: chrono::Duration = chrono::Duration::seconds(200);
```

200s, not exactly 180s: a safety margin against clock skew and the time
between computing `expires_at` and the venue receiving the request landing
right at the boundary. Still short relative to a market's actual lifetime
(these are 5/15-minute crypto slots), so the original design goal --
bounded resting exposure, decoupled from the unreliable `end_date_iso`
field -- is unaffected.

## Verification

- Update `gtd_maker_expiry_constant_is_a_short_relative_window`
  (`src/copytrading/orchestrate/tests.rs`) to also assert `GTD_MAKER_EXPIRY
  >= chrono::Duration::seconds(180)` -- pin the venue's real floor, not just
  an arbitrary lower bound, so this specific regression can't recur silently.
- After deploying, confirm on the real venue: a post-only GTD maker order
  actually gets accepted (not rejected with an expiration error) the next
  time a leader's FAK misses. Watch `order_attempts.failure_detail` for
  leader 2 (or any leader routed through this path) for the string
  `"expiration is less than 180 seconds"` -- it should stop appearing
  entirely.
- Also watch `copy_intents.rejection_reason` for `"unexpected repeated
  no-FAK rejection after maker fallback selection"` -- per the mechanism
  above, this should also stop once the GTD attempt can actually succeed.
