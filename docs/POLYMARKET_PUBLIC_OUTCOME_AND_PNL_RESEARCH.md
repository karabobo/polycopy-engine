# Polymarket public outcome, trade, and PnL research

**Purpose.** This note records a read-only, reproducible way to collect the
public evidence needed to compare a copied account with its leaders: individual
trades, market outcome, price path, and position-level PnL.  It is not an
execution integration and it does not authorize a trading decision.

## Authoritative public interfaces

| Question | Public interface | What to retain |
| --- | --- | --- |
| What did a wallet do? | [`GET /v2/trades` and `GET /v2/activity`](https://data-api.polymarket.com/v2/docs) on `https://data-api.polymarket.com` | Raw trade/activity rows, wallet, condition/token, side, price, size, time, and cursor used. `activity` is broader than trades, so filter `type` deliberately. |
| What is the position result? | [`GET /v2/positions`](https://data-api.polymarket.com/v2/docs) | The full position row: `entry_cost_usdc`, fees, `total_pnl`, `unrealized_pnl`, `realized_pnl`, percentage fields, `status`, `redeemable`, condition/token/outcome and opposite-outcome identifiers. |
| What is the market's resolution lifecycle? | [`GET /v2/resolutions`](https://data-api.polymarket.com/v2/docs) | Query by `condition` and retain the complete resolution response alongside the position/activity evidence. This is the public resolution-lifecycle interface; do not derive a winner solely from a price mark. |
| How did the token trade over time? | [CLOB public price and history endpoints](https://institute.polymarket.com/data) | `GET /price?token_id=...&side=BUY` is a point quote. `GET /prices-history?market=...&interval=...&fidelity=...` returns `{history:[{t,p}]}` with Unix-second time and price. |
| Which binary tokens belong to a market? | [`GET /markets-by-token/{token_id}`](https://docs.polymarket.com/api-reference/markets/get-market-by-token) | The returned `condition_id`, primary token ID and secondary token ID; retain it to prevent mixing YES and NO. |
| What metadata corroborates the outcome? | [Gamma/market data guide](https://institute.polymarket.com/data) | A Gamma market record includes `conditionId`, `outcomes`, index-aligned `outcomePrices`, `clobTokenIds`, `active`, `closed`, `resolutionSource`, `resolvedBy`, and `umaResolutionStatuses`. Treat it as metadata/corroboration, not the sole settlement authority. |
| What does a resolved share pay? | [Polymarket FAQ](https://docs.polymarket.com/faq) and [resolution concepts](https://docs.polymarket.com/concepts/resolution) | At final resolution, shares of the correct outcome pay $1 USDC per share and the losing outcome is worthless. A holder can instead sell before resolution. |

The Data API v2 documentation describes a response envelope containing `data`.
For the cursor-paged routes above, use the opaque `pagination.next_cursor` until
it is null, and use `pagination.has_more` as documented.  Do **not** synthesize
or increment a cursor and do **not** send `offset`: v2 explicitly rejects
offset.  The documented maximum `limit` is 1,000; on HTTP 429 or 503 honor
`Retry-After` before a retry.

Gamma is also public, but has different pagination: the official guide documents
`limit` plus `offset` for its active-market listing and a maximum `limit` of
500.  This is not interchangeable with v2's cursor contract.  The guide
documents `GET /markets/{id}` for a known Gamma market ID; it does not justify
assuming an unverified `condition_id` query spelling.  Prefer the documented
v2 `condition` parameter for condition-keyed research.

## Reproducible read-only collection

Use the public wallet address that Polymarket exposes (which can be a proxy
wallet), normalize it to lowercase for local joins, preserve the API spelling
in the raw capture, and never place credentials in these calls.

```bash
# First page: repeat with the opaque pagination.next_cursor returned by the API.
curl -fsS -G 'https://data-api.polymarket.com/v2/trades' \
  --data-urlencode 'user=0xWALLET' \
  --data-urlencode 'limit=1000' \
  --data-urlencode 'start=UNIX_START' \
  --data-urlencode 'end=UNIX_END' \
  -o leader-trades-page-001.json

# Activity is useful for REDEEM / MERGE / SPLIT context; restrict its type and
# time window instead of treating all activity records as executable trades.
curl -fsS -G 'https://data-api.polymarket.com/v2/activity' \
  --data-urlencode 'user=0xWALLET' \
  --data-urlencode 'type=TRADE' \
  --data-urlencode 'start=UNIX_START' \
  --data-urlencode 'end=UNIX_END' \
  --data-urlencode 'limit=1000' \
  -o leader-trade-activity-page-001.json

# Capture open, closed, and redeemable views separately.  A position response
# is a mark/position report, not a transaction ledger.
curl -fsS -G 'https://data-api.polymarket.com/v2/positions' \
  --data-urlencode 'user=0xWALLET' \
  --data-urlencode 'status=CLOSED' \
  --data-urlencode 'limit=1000' \
  -o wallet-closed-positions-page-001.json
```

`/v2/activity` supports, among others, `user`, `condition`, `event_id`, `side`,
`start`, `end`, `type`, `sort_by`, `sort_direction`, and
`exclude_deposits_withdrawals`.  `/v2/positions` requires at least `user` or
`condition`; it accepts at most 20 condition IDs in one request.  Record the
request URL (excluding any future authenticated material), retrieval time,
HTTP status, response hash, and every cursor transition.  This makes a later
comparison auditable despite concurrent new trading.

## Outcome and PnL interpretation rules

1. Join on **token ID first**.  Resolve its parent condition and the two token
   IDs before grouping by market.  A condition-level join alone can accidentally
   net opposing outcomes.
2. `CLOSED` is not synonymous with “won at resolution”: a position may be
   closed by a sale.  Use `/v2/resolutions` as the market-resolution lifecycle
   evidence, and treat `REDEEMABLE` plus recorded REDEEM activity as settlement
   evidence for an outstanding/redeemed winner.  Gamma `outcomePrices` can
   corroborate the result but is not enough on its own.  Keep redeemed
   historical rows rather than inferring a result from an absent open position.
3. Calculate the comparison in two layers.  (a) **Realized cashflow from the
   selected trade/activity rows**: buys, sells, redemptions, fees and any
   merge/split effect.  (b) **Position mark** from `/v2/positions`: explicitly
   sum the reported position PnL fields only after deduplicating token rows.
   The latter can include inventory and marks from outside the selected copy
   window, so it is not proof that a leader's recent signals caused the PnL.
4. Align each copied intent to the leader fill by token, side and a bounded
   timestamp window.  Compare leader price, follower receipt VWAP, follower
   filled size, and whether the follower was rejected.  Use the historical CLOB
   series only for a post-trade mark/path; it cannot reconstruct the executable
   order book or prove that an unfilled FAK would have filled at that price.

## Testing the “rejected orders contain alpha” hypothesis

Do not test this by comparing all rejected signals with all fills: that mixes
liquidity, price, market, and arrival-time selection.  Build one row per leader
signal with its eventual **settled-or-exit** PnL at equal notional, then compare
accepted and rejected rows within bins of token/condition, side, leader price,
signal time, and signal age.  Report counts, mean and median PnL, win rate, and
confidence intervals; preserve rejected rows that never had executable depth as
their own class.  A higher realized result for rejections would show selection
in the current fill policy, not by itself an instruction to chase them—those
same rows may have been impossible to fill at the leader price.

## Boundaries

- These are public read endpoints.  They do not validate the engine's private
  receipt ledger, CLOB authentication, or collateral balance.
- No single account-wide PnL endpoint is relied upon here.  `/v2/positions`
  exposes position-level PnL fields; any account or strategy total must state
  exactly which rows, time window, fee treatment, and position marks it sums.
- Public endpoints and limits can change.  Recheck the linked official v2 docs
  immediately before automating a collection job.
