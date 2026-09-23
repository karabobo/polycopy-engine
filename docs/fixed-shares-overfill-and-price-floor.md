# Fixed-shares BUYs can fill far above their target: one fix, one retracted

**Correction, same day as the first draft**: this doc originally proposed
two fixes. Fix 1 was wrong and is retracted below, with the evidence that
killed it, rather than deleted outright -- the reasoning is worth keeping so
nobody re-proposes it from the same stale premise. Only Fix 2 stands.

## The observation (real production data, leader 2)

Leader 2's `max_order_shares = 10`. Bucketing its 180 finalized fills by how
far `accounted_filled_qty` landed from that target:

| bucket | n | win rate | avg qty | ROI |
|---|---|---|---|---|
| under-filled (<10) | 121 | 33.9% | 5.83 | -21.6% |
| ~exact (10) | 24 | **41.7%** | 10.04 | **-18.1%** |
| slight over (10-13) | 24 | 29.2% | 11.37 | -24.0% |
| big over (>13) | 11 | **18.2%** | 15.63 | **-29.3%** |

Monotonic: the further a fill lands above the configured target, the worse
both win rate and ROI get.

## Retracted: "switch to a shares-denominated order so the venue can't overfill"

The original draft proposed constructing fixed-shares BUYs as
`limit_order().price(limit_price).size(target_qty)` instead of a
budget-denominated market order, reasoning that a signed `takerAmount`
would then be a hard venue-side cap. Two things were wrong with that:

**1. It was already done, and I hadn't re-checked.** Commit `5508aaf`
("Add bounded FAK retry and GTD maker fallback", 2026-09-21 04:44 +0800)
had already changed `src/copytrading/prepare.rs` to construct fixed-shares
BUYs as `ClobOrderAmount::BuyTakerShares(decision.qty)` →
`self.client.market_order()....amount(Amount::shares(shares)...)`. I had
re-read `execute.rs`'s sizing branch for the earlier doc but not gone back
to check how `prepare.rs` actually builds the order from that decision --
stale premise, not a live one.

**2. Even done, it doesn't cap shares -- this is venue behavior, not a
construction choice.** `src/venue/receipt.rs`'s `from_fak_buy_shares` doc
comment is explicit: *"The signed taker amount is the intended share
target, not a venue ceiling. Polymarket BUY FAK still spends the maker USDC
budget, so a better price can return more outcome shares than that
target."* There's a dedicated regression test locking this in,
`fixed_share_buy_fak_accounts_a_fill_above_its_signed_size`
(`tests/receipt.rs`), asserting a fill above the signed size must be kept,
not rejected. Polymarket's BUY FAK settles against the USDC budget
regardless of which SDK amount type the client signs with -- a
shares-denominated signature does not make shares the hard limit.

**Confirmed against real post-switch production data, not just the code
and the test.** Most of the 180 fills analyzed here predate `5508aaf` (134
of 180 are from before 2026-09-20 20:44 UTC, when the switch landed) and so
used the old budget-denominated construction -- expected overfill there
under either theory. But `order_attempts` id `314` (leader 2,
`submission_started_at` `2026-09-20T21:02:03Z`, after the switch) shows
signed `takerAmount = 10.0` shares and `accounted_filled_qty = 15.555556`
-- overfill under the *new*, already-shares-denominated construction too.
That's direct evidence the mechanism is venue-side, not something our
construction method controls.

**Net: there is no order-construction change that caps a Polymarket BUY
FAK's fill at a target share count.** The only lever available is deciding,
before submission, whether the *conditions* look bad enough to skip the
trade entirely -- which is Fix 2.

## Why the overfill happens

Polymarket's BUY FAK matches against the maker-side USDC budget: the limit
price is the worst acceptable price, and a fill at a materially better
price legitimately buys more shares for the same money, regardless of
whether the client signed a budget or a share count. In these fast
5/15-minute crypto slots, a price that has moved far below the leader's
reference price in the seconds between decision and submission is
plausibly not luck -- often the book is pricing in information that this
side is now less likely to win, and every current construction path
mechanically buys *more* of exactly that when it happens.

## Fix 2 (the only one left): a symmetric price floor

Account owner's own framing: keep it consistent with
`price_tolerance_bps`, which today only bounds how much *worse* (higher,
for a BUY) than the leader's reference price we'll accept. Add a symmetric
floor: if the real-time price at submission time is *below* the leader's
reference price by more than the same tolerance, refuse the order rather
than fill it. Rationale: a price that has already moved this favorably,
this fast, more likely means the market is pricing in something adverse for
this side than that we got lucky -- decline the trade rather than lean into
it.

This is now the only available backstop, for every sizing mode (fixed-share
and `max_order_notional` alike) -- there is no structural alternative per
the section above. It needs a fresh best-ask read immediately before
submission: real added latency (a network round-trip, on top of the
~1-1.85s per-order budget already measured), not a free addition.

## Verification

- A BUY whose fresh best-ask is more than `price_tolerance_bps` below the
  leader's reference price is rejected before any order is constructed or
  signed; one exactly at the boundary is accepted (mirror the existing
  ceiling-side tests for symmetry).
- Regression coverage for the retraction itself: a test asserting a
  fixed-shares BUY signed via `Amount::shares(target_qty)` still accounts a
  fill above `target_qty` as a real, kept fill (already covered by
  `fixed_share_buy_fak_accounts_a_fill_above_its_signed_size` -- this is a
  reminder not to "fix" that test when implementing Fix 2, since Fix 2
  prevents the trade beforehand rather than rejecting the fill after).
- `cargo test --all-features --locked` and
  `cargo clippy --all-targets --all-features --locked -- -D warnings` stay
  green throughout.
