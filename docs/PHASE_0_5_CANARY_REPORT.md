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
- Lookup endpoint / filters: `GET /data/order/{order_id}` (by known ID); the
  asset_id-filtered `GET /data/orders` blind-match lookup exists in the tool
  but was not exercised on this live attempt — the run ended (see below)
  before reaching that step, and it has not yet been re-run live since the
  fix.
- Result and returned order identifier: order id
  `0xc6190561e9d5908ce5c1c3a0fc11e8f6bd140329dd41a33413fc1409204f6be5` is
  known from the original successful submission response. The follow-up
  `GET /data/order/{id}` call for that same id returned
  `404 Not Found — Unable to find requested resource`.
- Is the lookup deterministic? **no, not by order ID** — a fully matched
  order was not findable by `GET /data/order/{id}` immediately after
  matching. Balance confirms the fill happened and settled instantly (see
  Result 3); the order simply is not visible through this specific
  lookup endpoint once matched. Whether the asset_id-filtered listing
  endpoint would have found it is still unknown and remains open.

## Result 2: byte-identical duplicate submission

**Not tested live.** `POLYCOPY_CANARY_CONFIRM_DUPLICATE` was not set on this
run. The necessary precondition — that two independent `sign()` calls over
one built `SignableOrder` (same salt, cloned before signing) produce
byte-identical `SignedOrder` JSON — was confirmed in multiple dry runs, both
locally and on the remote host, before this live attempt. The actual
venue-side behavior when the same signed bytes are submitted twice (409/idempotent
response, deterministic rejection, or two independent fills) has not yet been
observed.

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

- [ ] Lookup is deterministic from persisted envelope data. **Not proven** —
      by-ID lookup fails for an already-matched order; the alternative
      field-based lookup has not yet been exercised live.
- [ ] Byte-identical duplicate behavior is safe for the intended retry policy.
      **Not tested live yet.**
- [x] Receipt fields are sufficient for idempotent fill-delta accounting —
      `making_amount`/`taking_amount` are precise and matched the observed
      balance change exactly, **provided `filled_qty` is read from
      `taking_amount`, not from the requested `size`** (see Result 3).
- [ ] The exact lookup and recovery rule has a regression test. **Not yet** —
      this was a live, manually observed investigation; it is not yet encoded
      as an automated regression test.

Decision: **not passed until every box is checked and independently reviewed.**
Two questions remain open: whether an asset_id-filtered listing lookup can
recover an order the by-ID lookup cannot, and how the venue handles a
byte-identical duplicate submission. Both require a further live order to
resolve and are a decision for the account owner, not something to run
automatically.
