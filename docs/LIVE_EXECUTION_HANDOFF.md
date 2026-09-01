# Live execution handoff spec

> Historical handoff note: this document described the pre-writer state at
> commit `7dfaf29`. It is retained for design history only. The current
> implementation includes `IntlClobCopyAdapter` and bounded `copy_run`; the
> authoritative operating boundary is now
> [`LIVE_PROGRESSION_RUNBOOK.md`](LIVE_PROGRESSION_RUNBOOK.md). Do not use the
> statements below that say the writer is missing as current deployment advice.

## Why this document exists

This document is the handoff: what exists, what is missing, and exactly what
"done" looks like, so the account owner can verify delivered work without
reading Rust.

## What already exists (do not rebuild this)

- **Full schema** (`migrations/0001`-`0006`): accounts, leaders, leader
  events, copy intents, order attempts, position lots, reconciliation cases,
  execution schedule.
- **Activity ingestion** (`src/copytrading/ingest/`): live-verified
  WebSocket firehose plus REST backfill, writing into `leader_events`.
- **Intent planning** (`src/copytrading/plan.rs`): turns a recorded event
  into a durably accepted or rejected `copy_intent`, with an immutable
  policy snapshot per intent.
- **Sizing and reservation** (`src/copytrading/execute.rs`): claims an
  intent, computes the exact quantity/price to submit, reserves it so a
  second lane cannot oversell, and finalizes an idempotent receipt into
  `position_lots`.
- **The submission data model and recovery logic**
  (`src/copytrading/reconcile.rs`):
  - `PreparedOrderEnvelope` — the plain, serializable fields of one signed
    order attempt (`reconcile.rs:49`).
  - `CopyExecution` trait — the four methods a live venue adapter must
    implement (`reconcile.rs:304`). **This is the trait you are
    implementing.**
  - `load_or_prepare_attempt` — persists exactly one envelope per attempt
    under a `BEGIN IMMEDIATE` critical section (`reconcile.rs:336`).
  - `mark_attempt_submitting` / `mark_attempt_uncertain_after_submission_error`
    — the durable state transitions around the one HTTP call that can cross
    the network boundary (`reconcile.rs:392`, `reconcile.rs:420`).
  - `recover_lost_submission_response` /
    `recover_fak_taker_order_from_trades` — read-only recovery via
    authenticated trade history when a submission response is lost
    (`reconcile.rs:523`, `reconcile.rs:220`).
  - `permitted_recovery_action` — the full submission recovery matrix as
    pure code (`reconcile.rs:625`).
  - `open_reconciliation_case` — opens a visible, blocking case on any
    unresolved or ambiguous state (`reconcile.rs:670`).
- **Offline order-ID computation** (`src/venue/order_hash.rs`): computes the
  CLOB's order ID from a signed order before it is ever submitted, live-proven
  on 2026-09-01 against both a rejected and a matched real order (see
  `docs/PHASE_0_5_CANARY_REPORT.md`, Result 4 and Result 2's "Live result").
- **Read-only venue adapters** (`src/venue/intl_clob.rs`):
  `StrictTokenBalanceReader`, `StrictAccountBalanceReader`,
  `StrictTradeHistoryReader` — all already implemented, tested, and
  live-verified against the real CLOB API.
- **Status/traceability** (`src/copytrading/control_tower.rs`): read-only
  views over all of the above.
- **112+ tests** across this logic, all passing, all against fakes/an
  in-memory or temp-file SQLite database — none of them contact the venue.

## What is missing — four distinct pieces

Do not scope this as "implement one function." It is four pieces of
different sizes. Read all four before estimating.

### 1. `submit_exact_envelope` (the actual gap — order-writing code)

Signature (`reconcile.rs:320`):

```rust
fn submit_exact_envelope(
    &self,
    envelope: &PreparedOrderEnvelope,
) -> impl Future<Output = Result<OrderReceipt, String>> + Send;
```

The venue SDK's own submission method is:

```rust
// polymarket_client_sdk_v2::clob::Client<Authenticated<Normal>>
pub async fn post_order(&self, order: SignedOrder) -> Result<PostOrderResponse>
```

**The open design problem**: `SignedOrder` has a manual `Serialize` impl
(`polymarket_client_sdk_v2` source, `clob/types/mod.rs:811`) but **no
`Deserialize` impl at all**. `PreparedOrderEnvelope.signed_order_json` is
`serde_json::to_string(&signed_order)`'s output (a plain string, stored so
the envelope can survive a process restart), but there is no SDK-provided
way to parse that string back into a `SignedOrder` to hand to `post_order`.
Two ways to close this gap:

- **(a) Recommended — manually reconstruct a typed `SignedOrder`.** Parse
  the stored JSON's `order`/`orderType`/`owner`/`postOnly`/`deferExec`
  fields by hand into a `SignedOrder` struct literal (every field on
  `SignedOrder`, `OrderV1`, `OrderV2`, and `OrderPayload` is `pub`).
  `OrderSignature` has `From<Signature>` and `From<String>` constructors
  (`clob/types/mod.rs:712`); parse the stored hex signature string into an
  `alloy::primitives::Signature` for the normal (non-Poly1271) case. The
  exact wire shape to parse is in the SDK source at
  `clob/types/mod.rs:770`-`860` (the `Serialize` impl this must invert) —
  read it directly; do not guess field names or casing. Then call
  `client.post_order(reconstructed)` and get the SDK's normal response
  handling (including `resolve_transaction_hashes`) for free.
- **(b) Rejected — replay the raw JSON as a hand-authenticated HTTP POST.**
  Bypasses the `Deserialize` gap entirely, but `create_headers`
  (`clob/client.rs:2802`, the method that signs a request with the L2 HMAC
  scheme) is a **private** method on the SDK's client — this path would
  require independently reimplementing Polymarket's L2 authentication
  scheme by hand, which is strictly more risk than (a) for no real benefit.

Map `PostOrderResponse` (`error_msg`, `making_amount`, `taking_amount`,
`order_id`, `status`, `success`, `transaction_hashes`) into an
`OrderReceipt` via `OrderReceipt::from_fak_buy_budget` or
`from_fak_sell_shares` (`src/venue/receipt.rs`) — **read `taking_amount`
for the filled quantity, never the requested `size`**; Phase 0.5 proved live
that a BUY's `size` is a notional budget, not a literal share count.

A submission that returns a network/transport error (not a clean venue
rejection) must call `mark_attempt_uncertain_after_submission_error`, never
retry itself. A submission that returns a clean rejection before any
`order_id` exists is a definitive rejection, not `uncertain` — see
`docs/PHASE_0_5_CANARY_REPORT.md`'s Result 2 "Live result" for a real
example (`400`, `"invalid. Duplicated."`).

### 2. The other three `CopyExecution` methods (small — reuse what exists)

- `position_for_token_strict` — same name and same signature shape as
  `StrictTokenBalanceReader::position_for_token_strict`
  (`venue/intl_clob.rs:76`), already implemented on `IntlClobReadAdapter`.
- `order_for_receipt` — thin wrapper over the same by-ID lookup
  `canary_run.rs::lookup_by_id` already exercises live
  (`client.order(order_id)`, SDK `clob/client.rs:2025`).
- `query_prepared_envelope` — thin wrapper over the already-built
  `lookup_prepared_fak_in_trade_history` (`reconcile.rs:193`), converting
  its `TradeHistoryLookup::Recovered` case into an `OrderReceipt`.

These three are read-only, low-risk, and mostly plumbing around code that is
already tested and live-verified.

### 3. Envelope construction (does not exist yet)

Nothing in this repository currently builds a real `PreparedOrderEnvelope`
from an actual signing operation — only test code does
(`reconcile.rs:851`'s `envelope(salt)` helper). A real implementation needs
a function that: builds a `SignableOrder` (`canary_run.rs::build_signable_order`
already does this), signs it once, computes `expected_taker_order_id` via
`src/venue/order_hash.rs::expected_order_id`, serializes the signed order to
`signed_order_json`, and returns a `PreparedOrderEnvelope` ready for
`load_or_prepare_attempt`. **The signed order must never be rebuilt** —
signing twice for "the same" attempt produces a different salt-derived
order unless the exact same `SignableOrder` is reused, which breaks the
whole point of `load_or_prepare_attempt`'s idempotency guarantee.

### 4. The orchestration loop (does not exist yet)

There is currently no binary that runs the pipeline end to end. `src/bin/`
has only `canary_probe`, `ghost_verify`, and `lock_probe` — no
"run continuously" service. A real deployment needs something that: runs
Phase 2 ingestion, periodically calls `plan_next_batch`, and for each
`prepared`/resumable intent walks `permitted_recovery_action`'s matrix
(claim → prepare envelope → `mark_attempt_submitting` → `submit_exact_envelope`
→ finalize, or query-first/recover/block as the matrix dictates). This is
a genuinely separate task from writing `submit_exact_envelope` itself —
budget for it separately.

## Acceptance criteria (verifiable without reading Rust)

Ask for all of the following before accepting delivered work:

1. `cargo build --release --all-features --locked` succeeds with **zero**
   new warnings.
2. `cargo test --all-features --locked` — every existing test (112+ as of
   this document) still passes, plus new tests for every new function.
3. `cargo clippy --all-targets --all-features --locked -- -D warnings` is
   clean.
4. New tests demonstrate, without contacting the venue (fakes only):
   - `submit_exact_envelope` never rebuilds/re-signs an envelope that
     already exists for `(intent_id, attempt_number)`.
   - A transport/network error transitions the attempt to `uncertain`, not
     back to `prepared` and never auto-resubmitted.
   - `filled_qty` for a BUY comes from the response's matched-shares field,
     never from the requested size.
   - `submit_exact_envelope` is exercised against a fake venue only in
     tests — it must still be true after this work that no automated test
     in this repository contacts the real venue.
5. One final, real live test, run by the account owner only, exactly as the
   existing canary tooling does it (an explicit operator-set confirmation
   before any real order): a full run through claim → prepare → submit →
   finalize against one deliberately tiny real order, with the resulting
   receipt and lot compared by hand against the venue's own UI.

## Non-negotiable properties (from `docs/COPY_ENGINE_BLUEPRINT.md`)

A reviewer — technical or not — can sanity-check delivered code against
this list by asking the developer to point to where each one is enforced:

- A network-boundary-crossing submission is `uncertain`, never `failed`,
  until proven otherwise by a strict venue query.
- A strict venue query failure is never treated as "zero" or "not found" —
  it always opens a visible `reconciliation_cases` row and blocks further
  automatic action on that account/token.
- An `uncertain` attempt is never resubmitted automatically. Ever.
- `filled_qty` is applied to `position_lots` as a delta over
  `accounted_filled_qty`, never blindly re-applied — a duplicate read of
  the same receipt must change nothing on the second pass.
- The retry budget (`MAX_ATTEMPTS_PER_WINDOW` / `RETRY_WINDOW_SECONDS`,
  `reconcile.rs:39`) is enforced before any new attempt is prepared for an
  intent.

## Suggested engagement scope

Sequence the work as: (2) the three read-only trait methods, (3) envelope
construction, (1) `submit_exact_envelope` itself, (4) the orchestration
loop — in that order, since each step is directly testable against fakes
before the next depends on it. Only the very last step of the whole
engagement (acceptance criterion 5 above) should touch the real venue, and
only the account owner should set the confirmation that allows it to.
