# Phase 0.5 CLOB submission-safety canary

Status: **in progress — three real canary orders placed and observed across
2026-08-31 and 2026-09-01 (one full fill, one killed-for-no-match, one full
fill plus a rejected duplicate resubmission); two of the four gate boxes
below are now checked, two are not, so this still does not authorize
automated retry or a live-order path.**

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

**Tested live and answered on 2026-09-01** (see "Live result" below, after
the earlier attempts that could not reach this step).

The necessary precondition — that two independent
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
This question remained open and required a further live attempt made after
Polymarket confirmed the incident was resolved (not merely during its
cancel-only window) — a decision for the account owner, not something to
retry automatically or on a timer.

### Live result (2026-09-01)

- Label: `order-id-verify-2-2026-09-01`. Market: "Will Arsenal FC win on
  2026-08-31?" (EPL, Aston Villa vs. Arsenal), a different market from the
  original Fed canary and from all three outage-blocked attempts above.
  BUY, `price=0.66`, `size=5`, FAK.
- First submission: **matched in full.** `order_id
  0x828d2fc088f1c10cb5723b77a59cdf9a53673520b1984fa0309dabe9cc90489e`,
  `status: Matched`, `success: true`, `making_amount=3.3`, `taking_amount=5`
  (filled exactly at the limit price this time — no better-price improvement
  like the original Fed run).
- Second submission (the byte-identical duplicate, same salt, independently
  signed): rejected with `400 Bad Request`:
  `{"error":"order
  0x828d2fc088f1c10cb5723b77a59cdf9a53673520b1984fa0309dabe9cc90489e is
  invalid. Duplicated."}` — the venue recognized the resubmitted order by its
  own ID and refused it outright.
- Classification: **deterministic rejection, not an independent second
  fill, and not an ambiguous/idempotent-echo response.** The venue holds
  enough state to recognize "this exact order already exists" and reject a
  literal resubmission of it — it does not silently accept, silently ignore,
  or execute it a second time. This is the safe outcome for the retry
  question this canary exists to answer: resubmitting the *exact same
  signed bytes* cannot double-fill.
- Scope of what this does and does not prove: this specific test resent the
  **identical signed envelope** (same salt) for an order that had already
  fully matched. It does not by itself prove what happens on a resubmission
  of an order that is still open/partially filled, nor what happens if the
  *content* differs from a prior attempt (a new salt for "the same" logical
  intent) — those remain distinct questions the current recovery design
  (query-first, never blind-resubmit) is built to avoid needing an answer
  to.

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

## Result 4: offline order-ID hash equivalence

- Date and operator: 2026-09-01, account owner, run from the same remote host
  as the original canary.
- Label: `order-id-verify-2026-09-01`. Same market/token, side, price, and
  size as the original successful run (`fed-25bps-live-2026-08-31`): BUY,
  `price=0.55`, `size=5`, FAK, token_id
  `63842529068710005716169325380315470359047749786610778647370693404952498013178`.
- What was compared: this project's own offline prediction
  (`src/venue/order_hash.rs`'s `expected_order_id`, computed from the signed
  order before any network call, by mirroring the SDK's internal EIP-712
  signing digest and the on-chain CTF Exchange contract's public `hashOrder`
  formula) against the ID the venue itself assigned to the order.
- Result: the market had moved since 2026-08-31 (best bid `$0.57` / best ask
  `$0.99` at time of check — a much wider spread than the original run), so
  the identical `price=0.55` no longer matched. The venue rejected the
  submission with `400 Bad Request`:
  `{"error":"no orders found to match with FAK order. FAK orders are
  partially filled or killed if no match is found.","orderID":
  "0x76480b8e5a6d653f7b7b15aa51504eb959c64ddcc1ea6f3f1bb6bfdd539afad7"}`.
  This project's `expected_order_id`, printed **before** submission, was:
  `0x76480b8e5a6d653f7b7b15aa51504eb959c64ddcc1ea6f3f1bb6bfdd539afad7`.
  **These are byte-identical.**
- Significance: this is the first live confirmation that this project's
  precomputed order-ID formula equals the venue's own assigned ID for a real
  order — and it held even for a rejected, zero-fill FAK (the venue clearly
  computes/assigns the ID before or independent of matching, not only after a
  fill). This directly answers, in the positive, the open question in
  `docs/INTL_CLOB_SDK_BOUNDARY.md` about whether a locally computed
  signed-envelope identifier matches the venue's own field.
- What remains open: this run proved equivalence against the
  order-submission response's `orderID` field for a rejected order. Result 2
  above's `order-id-verify-2-2026-09-01` run (2026-09-01, same day)
  additionally proved it for a **matched** fill: `expected_order_id` again
  matched `order_id` exactly, this time with `status: Matched, success:
  true`. That closes the practical question of whether this project's
  offline computation identifies a real order regardless of whether it
  matches or is killed.
  What remains technically unobserved is narrower: neither run called the
  authenticated `GET /data/trades` endpoint itself, so the equivalence of
  its specific `taker_order_id` field (rather than the order-submission
  response's `order_id` field) to this same value has not been directly
  witnessed — only inferred, since both fields should identify the same
  order. `reconcile.rs`'s `recover_fak_taker_order_from_trades` reads that
  endpoint, not the submission response, so a fully rigorous close of this
  gap would query it once for a known-matched order and confirm the field
  name and value both match.

## Gate decision

### Local recovery implementation (partially live-proven)

The engine now persists a precomputed expected `taker_order_id`, the exact
signed-order JSON, and the durable instant an attempt enters `submitting`. For
a response lost after the HTTP boundary, it performs a paginated authenticated
`GET /data/trades` read for the bounded token/time window. It accepts only an
exact `taker_order_id` match from a successful taker-side trade, records that
ID, and still requires the normal strict by-ID receipt query before lot
accounting. An empty or delayed history, error, unknown status, malformed
fingerprint, or contradictory duplicate data opens `needs_reconcile`; no path
resubmits an uncertain order.

Result 4 above live-proved that the precomputed identifier equals the
venue's own assigned order ID, for the order-submission response, for both a
rejected and a matched order. What remains **not directly observed** is the
specific field `recover_fak_taker_order_from_trades` actually reads
(`GET /data/trades`'s `taker_order_id`) — inferred to carry the same value,
but not yet queried and checked in a live run.

- [ ] Lookup is deterministic from persisted envelope data. **Not proven —
      and now partially disproven, partially strengthened.** By-ID lookup
      works, but only after an unmeasured indexing delay (confirmed clear by
      ~6 hours; confirmed absent immediately after matching, and reconfirmed
      on 2026-09-01). The field-based fallback for a truly lost order ID
      (asset_id-filtered listing) does **not** find a matched order at all —
      it only lists open orders. A crash before the order ID is durably
      recorded is not currently recoverable by either lookup path alone. On
      the other hand, Result 4 now proves the order ID itself *can* be
      computed deterministically offline, before any submission, for both a
      rejected and a matched order — the remaining gap is specifically
      whether `GET /data/trades`'s `taker_order_id` field carries the same
      value, not the ID computation itself.
- [x] Byte-identical duplicate behavior is safe for the intended retry
      policy. **Tested live on 2026-09-01** (Result 2, "Live result"):
      resubmitting the identical signed bytes for an already-matched order
      was rejected outright (`400`, `"invalid. Duplicated."`) — a
      deterministic rejection, never a second fill and never a silent
      idempotent echo. This specific test covered only a full resubmission
      of an already-fully-matched order; it does not by itself cover a
      resubmission while an order is still open/partially filled.
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

The probe now also has a separate, read-only
`POLYCOPY_CANARY_VERIFY_TRADE_LOOKUP=yes` mode. Given a persisted expected
order ID and the token, it queries authenticated trade history over a bounded
24-hour window and writes an exact `taker_order_id` match/miss/failure record.
It never builds, signs, submits, retries, or duplicates an order. Running this
against a known matched canary is the remaining live-only evidence required by
the first checkbox; until its record exists and is independently reviewed, the
gate remains not passed.

Decision: **not passed until every box is checked and independently reviewed.**
Three of the four boxes now have a live-tested answer; two of those three are
positive. Confirmed in the negative: the field-based fallback lookup does not
recover a matched order, so a lost order ID before durable persistence is
currently an unrecoverable gap, not just an untested one — later phases must
close it with a different mechanism before relying on automatic recovery.
Confirmed in the positive: Result 4 proves this project's offline order-ID
computation matches the venue's own assigned ID, for both a rejected and a
matched order, and Result 2's live result proves a byte-identical duplicate
submission is rejected deterministically, never silently double-filled. What
remains open is narrower than before: whether `GET /data/trades`'s
`taker_order_id` field specifically (not just the submission response's
`order_id`) carries the same value, and the regression-test-against-a-mock
item, which was already only partial. Those, and independent review of the
two now-positive results, are what this gate still needs before it can be
marked passed.
