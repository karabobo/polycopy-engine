# Phase 0.5 CLOB submission-safety canary

Status: **in progress — one real canary order placed and observed; not every
gate box below is checked yet, so this does not authorize automated retry or
a live-order path.**

This report is required by
[`COPY_ENGINE_BLUEPRINT.md`](COPY_ENGINE_BLUEPRINT.md) before any automatic
resubmission of a `submitting` or `uncertain` attempt is enabled.

Never commit private keys, API credentials, full signed envelopes, raw request
bodies, or unredacted wallet addresses here. Put any raw operational material
under the ignored `canary-artifacts/` directory and retain it only according to
the operational security policy.

The command is `cargo run --locked --features intl_clob --bin canary_probe`;
see `README.md`'s "Phase 0.5 canary probe" section for its full environment
variable list. It is a dry run — it builds and signs but never submits —
unless `POLYCOPY_CANARY_CONFIRM_SUBMIT=yes` is set by the operator in their
own shell. Nothing else can set that variable. Set
`POLYCOPY_CANARY_CONFIRM_DUPLICATE=yes` in the same run to also submit a
second, independently-signed copy of the identical order for Result 2 below.

## Authorization and bounds

- Date and operator: 2026-08-31, account owner, run from a remote host (local
  network could not reach the venue that day)
- Account identifier (redacted): `gnosis_safe-...255E`
- Market / outcome token (redacted): "Will the Fed increase interest rates by
  25 bps after the September 2026 meeting?" — Yes outcome
- Maximum notional and quantity: requested `size=5`, limit `price=0.55`
  (intended cap ≈ $2.75)
- Time-in-force (must be explicit): FAK
- Approval record: account owner set `POLYCOPY_CANARY_CONFIRM_SUBMIT=yes`
  themselves and ran `canary_probe` on their own remote server

## Result 1: lookup after interrupted response

- Deliberately interrupted client-side response method: not deliberately
  interrupted this run. The submission response was received normally and
  its `order_id` persisted. The immediately-following `GET /data/order/{id}`
  lookup then failed on its own (see below), which is real, unprompted
  evidence for the same class of problem a genuinely lost response would
  present.
- Persisted envelope fields used for deterministic lookup: `token_id`,
  `side`, `price`, `size` were persisted to `canary-artifacts/<label>/spec.json`
  before signing or submission, per the "never rebuild an attempt" rule.
- Lookup endpoint / filters: `GET /data/order/{order_id}` (by known ID),
  re-queried both immediately after submission and again roughly six hours
  later; `GET /data/orders?asset_id=...` (blind field-based listing),
  queried at the same later time.
- Result and returned order identifier: order id
  `0xc6190561e9d5908ce5c1c3a0fc11e8f6bd140329dd41a33413fc1409204f6be5` is
  known from the original successful submission response.
  - Immediately after submission: `GET /data/order/{id}` returned
    `404 Not Found — Unable to find requested resource`.
  - ~6 hours later: the **same** `GET /data/order/{id}` call **succeeded**,
    returning the full matched order (`status: Matched`,
    `size_matched: 5.28846`, `price: 0.55`, `original_size: 5`, ...) — i.e.
    this is an **indexing/consistency delay**, not a permanent gap. The
    order became findable by ID some time after it matched.
  - At that same later time, `GET /data/orders?asset_id=<token>` (no
    `order_id` filter — the "I don't know the order_id" case) returned
    **zero orders**, even though the order above unambiguously exists and
    is matched. This endpoint appears scoped to currently-open/resting
    orders only, not historical/matched ones, regardless of how long has
    passed.
- Is the lookup deterministic? **Only if the order ID is already known, and
  only after waiting out an unknown indexing delay (confirmed to clear
  within ~6 hours; the actual minimum delay is unmeasured).** The blind,
  field-based path this project would need if the order ID itself were lost
  (e.g. a crash before the submission response was ever read) **does not
  recover a matched order** through this endpoint — it only lists open
  orders. Practical implication for later phases: persisting the order ID
  the moment it is known (before any further processing) is load-bearing,
  because the asset_id-filtered listing is not a viable fallback for a
  truly lost order ID once the order has matched. An `uncertain` attempt
  whose response was genuinely never received cannot currently be resolved
  by polling either lookup endpoint with only the persisted pre-submission
  envelope fields; a different recovery mechanism (e.g. a trade-history
  endpoint or on-chain confirmation) would need to be identified before
  that gap can be closed.

## Result 2: byte-identical duplicate submission

**Not tested live.** The necessary precondition — that two independent
`sign()` calls over one built `SignableOrder` (same salt, cloned before
signing) produce byte-identical `SignedOrder` JSON — was confirmed in
multiple dry runs, both locally and on the remote host, before any live
attempt. The actual venue-side behavior when the same signed bytes are
submitted twice (idempotent response, deterministic rejection, or two
independent fills) has not yet been observed.

Three further live attempts were made specifically to test this (labels
`fed-25bps-dup-test-2026-08-31`, `fed-25bps-dup-test-2-2026-08-31`, and
`mancity-win-diagnostic-2026-08-31` — the last on a completely unrelated
market: an EPL soccer moneyline, different neg-risk group, ~250x lower 24h
volume than the Fed market). All three had their *first* submission rejected
before any order was created:
`503 Service Unavailable, {"error":"trading is disabled"}`. No `order_id`
was ever returned in any of them, so each is a clean, unambiguous rejection —
not an `uncertain` case per invariant #6 — and the tool correctly did not
proceed to the duplicate-submission step in any of the three.

Root cause, confirmed via Polymarket's own public status page
(status.polymarket.com): a **platform-wide CLOB trading outage**, unrelated
to this account or either market. Incident timeline (times as posted, UTC):
14:23 investigating, 14:30 trading paused platform-wide pending fix (with a
cancel-only period before any resumption), 16:29 ETA revised to ~10:00 next
resumption target, 17:40 ETA revised again to 11:00 for full resumption with
"at least 15 minutes" of cancel-only immediately before that. All three
canary attempts fall inside this window. This supersedes an earlier, incorrect
working theory in this report (that the rejection might reflect a
post-first-trade account review or a market-specific hold) — the account
was independently confirmed not to be in `closed_only` mode
(`BanStatusResponse { closed_only: false }`) and a second, unrelated market
also failed identically, which is only consistent with a venue-wide cause.
This question remains open and requires a further live attempt made after
Polymarket confirms the incident is resolved (not merely during its
cancel-only window) — a decision for the account owner, not something to
retry automatically or on a timer.

## Result 3: receipt fields

- Stable request, accepted, filled, remaining, and venue-status fields:
  `status` (`Matched`) and `success` (`true`) correctly reflected a full FAK
  fill. `making_amount` (`2.749999`, USDC spent) and `taking_amount`
  (`5.28846`, shares received) were both present and precise; `taking_amount`
  matched the independently observed strict token balance exactly
  (`5288460` raw units ÷ 10^6 = `5.28846`), with no observed settlement lag.
- Cumulative-versus-delta fill semantics: not distinguished yet — this was
  one single, immediately-resolved FAK fill, not a partial-fill-then-later-fill
  sequence, so cumulative-vs-delta accounting across multiple receipts for one
  attempt remains untested.
- Any contradictory or missing response fields: **`size` is not a literal
  share count for a BUY limit order.** The order requested `size=5` at limit
  `price=0.55`; it matched at a better price and returned `taking_amount=5.28846`
  shares for `making_amount=2.749999` — i.e. `size` combined with `price`
  behaved as a notional/budget cap (≈ size × price), not a fixed quantity of
  shares to acquire. A future lot-accounting implementation must read
  `filled_qty` from the venue's actual matched-shares field
  (`taking_amount` for a BUY) and must never assume it equals the requested
  `size`.

## Gate decision

- [ ] Lookup is deterministic from persisted envelope data. **Not proven —
      and now partially disproven.** By-ID lookup works, but only after an
      unmeasured indexing delay (confirmed clear by ~6 hours; confirmed
      absent immediately after matching). The field-based fallback for a
      truly lost order ID (asset_id-filtered listing) does **not** find a
      matched order at all — it only lists open orders. A crash before the
      order ID is durably recorded is not currently recoverable by either
      lookup path alone.
- [ ] Byte-identical duplicate behavior is safe for the intended retry policy.
      **Not tested live yet.**
- [x] Receipt fields are sufficient for idempotent fill-delta accounting —
      `making_amount`/`taking_amount` are precise and matched the observed
      balance change exactly, **provided `filled_qty` is read from
      `taking_amount`, not from the requested `size`** (see Result 3).
- [~] The exact lookup and recovery rule has a regression test. **Partial.**
      `CanaryLookupRecord::{found, not_found, query_failed}` in `canary.rs`
      now encode the three outcomes discovered live (found; completed but
      empty; the call itself failed), with unit tests
      (`canary::tests::a_lookup_query_failure_is_recorded_not_a_reason_to_abort`
      and its siblings) pinning that a lookup failure always produces a
      persistable record instead of aborting the probe. This locks in the
      *safety property* (never crash-and-lose-the-report on a failed lookup).
      It does **not** replay the actual discovered venue sequence
      (404-then-succeeds-later, empty-listing-for-a-matched-order) against a
      mock server — doing that would need the CLOB host and authentication to
      be injectable for tests, which is not yet built. That remains open.

Decision: **not passed until every box is checked and independently reviewed.**
One question is now answered in the negative rather than merely open: the
field-based fallback lookup does not recover a matched order, so a lost order
ID before durable persistence is currently an unrecoverable gap, not just an
untested one — later phases must close it with a different mechanism before
relying on automatic recovery. The duplicate-submission question remains
fully open and requires a further live order to resolve; that is a decision
for the account owner, not something this project runs automatically.
