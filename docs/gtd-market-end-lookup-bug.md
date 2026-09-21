# Post-only GTD maker retry: `end_date_iso` does not mean what the code assumes

## Symptom

Leader 2's BUY intents intermittently reject with:

```
post-only GTD market-end lookup failed: market end is not in the future; refusing GTD maker order
```

Confirmed via `copy_intents.rejection_reason` on the production DB (`intent
423, 520, 529, 533` — 2026-09-21, spread across 00:18, 04:56, 06:00, 07:07
UTC), each preceded by two rejected FAK `order_attempts` (normal "no orders
found to match" misses) before the flow falls through to the post-only GTD
retry in `market_spec_for_gtd`
(`src/copytrading/orchestrate/mod.rs:1222-1247`).

## Root cause (verified against the live venue, not guessed)

`market_spec_for_gtd` treats `MarketResponse.end_date_iso` as this specific
15-minute (or 5-minute) market window's real resolution time:

```rust
let end = market.end_date_iso.ok_or_else(|| {
    "market end lookup returned no end_date_iso; refusing GTD maker order".to_owned()
})?;
if end <= Utc::now() {
    return Err("market end is not in the future; refusing GTD maker order".to_owned());
}
```

It isn't. I pulled the real `GET /markets/{condition_id}` response for all
four failing intents' `condition_id`s directly from `clob.polymarket.com`:

| Market (from the question text) | Real state right now | `end_date_iso` |
|---|---|---|
| Solana Up or Down, Sep 20 8:15–8:20PM ET | long since resolved | `2026-09-21T00:00:00Z` |
| Bitcoin Up or Down, Sep 21 12:45–1:00AM ET | resolved | `2026-09-21T00:00:00Z` |
| Bitcoin Up or Down, Sep 21 2:00–2:15AM ET | resolved | `2026-09-21T00:00:00Z` |
| Bitcoin Up or Down, Sep 21 3:00–3:15AM ET | **`closed: false`, `accepting_orders: true` — still tradeable when checked** | `2026-09-21T00:00:00Z` |

Four different 5/15-minute windows, spanning two calendar days of real
resolution times, all report the identical `end_date_iso`: midnight UTC of
the day the market instance belongs to. It's a day-level placeholder these
auto-generated recurring crypto markets share, not a per-slot end time. The
last row is the decisive evidence: that market was still genuinely open and
accepting orders at the moment I checked, yet `end_date_iso` had already
"expired" hours earlier by this check's logic — so this rejects real,
currently-tradeable opportunities, not just genuinely-closed ones.

`closed` and `accepting_orders`, by contrast, were accurate for all four in
this sample (`true`/`false` on the three resolved markets, `false`/`true` on
the one still open) — those are the fields that actually reflect whether
the venue will still accept an order for this market.

## The fix has two parts, not one

**1. Gate on `accepting_orders`/`closed`, not `end_date_iso`.** Replace the
`end_date_iso <= Utc::now()` check with something like:

```rust
if market.closed || !market.accepting_orders {
    return Err("market is closed or not accepting orders; refusing GTD maker order".to_owned());
}
```

**2. `GtdMarketSpec.expires_at` still needs a real timestamp for the GTD
order itself** — the venue needs to know when an unfilled maker order should
expire, and nothing in `MarketResponse` reliably gives "this specific
window's real resolution instant" (`end_date_iso` is the wrong field per
above; `game_start_time` was `None` in every sample market so it isn't a
substitute either). Rather than trying to reconstruct the real per-slot end
time from a field the API doesn't reliably provide, a short, fixed relative
expiry decouples correctness from that missing data entirely — e.g.
`Utc::now() + chrono::Duration::seconds(30)` (or whatever value matches the
existing GTD retry's own time budget). The order still can't linger
past a market's actual close either way: an already-closed or soon-to-close
market will simply reject the order at submission (same as the FAK legs
already do), and a genuinely-open market gets a bounded, short-lived resting
maker order instead of one anchored to a fabricated multi-hour "end time."
This also sidesteps a subtler failure the current code doesn't handle: even
if `end_date_iso` were correct, a GTD expiring exactly at market close would
leave a maker order open for the market's *entire remaining life*, which is
a much longer resting-order exposure than a short-duration crypto up/down
strategy likely wants regardless of this bug.

Both changes are local to `market_spec_for_gtd`; nothing about
`GtdMarketSpec`'s shape or its caller (`prepare_post_only_gtd_buy`) needs to
change.

## Verification

- `cargo test --all-features --locked` stays green.
- New test: a `MarketResponse` fixture with a past-midnight `end_date_iso`
  but `closed: false, accepting_orders: true` must succeed (this is the
  exact real-world shape from the fourth row above — the current code fails
  this case today).
- New test: `closed: true` (or `accepting_orders: false`) still refuses,
  regardless of what `end_date_iso` says.
- Confirm `expires_at` on the returned `GtdMarketSpec` is `Utc::now() +
  <chosen short duration>`, not derived from `end_date_iso`.
- After deploying, watch `copy_intents.rejection_reason` for leader 2 (and
  any other leader routed through the post-only GTD retry) — this exact
  rejection string should stop appearing for markets that are genuinely
  still open.
