# polycopy-engine

`polycopy-engine` is the standalone implementation project for a Polymarket copy engine.
It begins with the financial-correctness blueprint in
[`docs/COPY_ENGINE_BLUEPRINT.md`](docs/COPY_ENGINE_BLUEPRINT.md).

The project uses DRADIS as a read-only reference for selected venue concepts. It
does not vendor DRADIS source code or preserve DRADIS commit history.

The exact audit reference and this independent-implementation decision are
recorded in [`docs/DRADIS_REFERENCE_BASELINE.md`](docs/DRADIS_REFERENCE_BASELINE.md).

No automated copy-trading client is enabled. The only order-writing code is
the narrowly scoped Phase 0.5 canary probe below, which is a dry run unless
the operator explicitly opts in. The broader copy-execution path remains
blocked until the Phase 0.5 and Phase 7 gates in the blueprint are completed.

## Current development status

Phase 0 is closed (2026-08-30); Phase 0.5 is in progress (three real canary
orders placed and observed across 2026-08-31 and 2026-09-01; two of four gate
boxes checked, two still open — see
[`docs/PHASE_0_5_CANARY_REPORT.md`](docs/PHASE_0_5_CANARY_REPORT.md)). The
first implemented primitive is a cross-process database ownership lock: a
second engine instance fails instead of sharing a database and sending
concurrently for the same account/token.

Phase 1 (durable schema, blueprint section 6) through Phase 6 (Squadron,
CAG, and Control Tower, section 11) have started, ahead of Phase 0.5's gate
formally closing, at the account owner's explicit direction while Phase
0.5's remaining live tests wait on further live confirmation. Phase 1's
schema is complete (every table section 6 lists). Phase 2 has a working,
live-verified connection to the leader-trade firehose plus REST backfill,
both writing into that schema. Phase 3 turns a recorded event into a
durably accepted or explicitly rejected `copy_intent`. Phase 4 claims an
intent, sizes and reserves it, and finalizes an idempotent receipt into
`position_lots`. Phase 5 adds the durably persisted order envelope, the
submission recovery matrix, and the retry budget that govern how a claimed
intent is actually submitted — but `submit_exact_envelope`, the one call
that would write a live order, has **no implementation anywhere in this
crate**, and this project's assistant will never write or run that code.
Phase 6 adds a read-only leader/intent/lot/reconciliation status layer and
full attempt-to-event-to-account traceability; it writes nothing. See
"Database (Phase 1)", "Activity ingestion (Phase 2)", "Intent planning
(Phase 3)", "Fixed-lane executor (Phase 4)", "Prepared submission and
reconciliation (Phase 5)", and "Squadron, CAG, and Control Tower (Phase 6)"
below.

Run the complete local checks with:

```sh
cargo test --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo build --release --all-features --locked
```

The project's only live-order path is the `canary_probe` command below,
gated behind an explicit operator-set environment variable and used solely to
produce the evidence required by the Phase 0.5 gate. See
[`docs/PHASE_0_STATUS.md`](docs/PHASE_0_STATUS.md) for Phase 0's closed
status and [`docs/PHASE_0_5_CANARY_REPORT.md`](docs/PHASE_0_5_CANARY_REPORT.md)
for the required real-order safety evidence template.

The execution host's release, GHOST-only systemd unit, secret boundary, and
production-promotion gates are documented in
[`docs/SERVER_DEPLOYMENT.md`](docs/SERVER_DEPLOYMENT.md). The deployment
scripts do not install or start a copy-trading service.

The optional `intl_clob` feature enables **read-only**, strict per-outcome
token balances and authenticated account-trade-history reads. It has no
order-writing API; see
[`docs/INTL_CLOB_SDK_BOUNDARY.md`](docs/INTL_CLOB_SDK_BOUNDARY.md).

## Authenticated GHOST verification

`ghost_verify` performs only authenticated balance reads; it has no order,
cancel, approval, deposit, or credential-creation path. By default it uses the
signing key to derive an **existing** CLOB L2 credential; a failed derivation
does not create one. It needs a timestamped manual snapshot and these local
process-environment variables, never a committed file:

```text
POLYCOPY_CLOB_PRIVATE_KEY=[signing key]
POLYCOPY_CLOB_SIGNATURE_TYPE=eoa|proxy|gnosis_safe|poly1271
POLYCOPY_CLOB_FUNDER=[optional for proxy/gnosis_safe; required for poly1271]
POLYCOPY_GHOST_SNAPSHOT_AT_UTC=2026-08-30T00:00:00Z
POLYCOPY_GHOST_EXPECTED_COLLATERAL=[decimal]
POLYCOPY_GHOST_EXPECTED_TOKEN_BALANCES=123456789=1.5,987654321=0
```

If the existing API credential was created with a non-default nonce, set
`POLYCOPY_CLOB_L2_NONCE=[u32]`. Alternatively, provide all three existing L2
credential variables (`POLYCOPY_CLOB_L2_API_KEY`,
`POLYCOPY_CLOB_L2_API_SECRET`, and `POLYCOPY_CLOB_L2_API_PASSPHRASE`) to skip
derivation. Partial credentials are rejected, and a nonce cannot be combined
with supplied credentials.

The derive-only request uses the signing EOA and optional nonce. `funder` and
`signature_type` configure the subsequently authenticated CLOB client; they do
not cause a different L2 credential to be created or selected.

For Proxy or Gnosis Safe wallets, explicitly set `POLYCOPY_CLOB_FUNDER` to the
funded address shown in Polymarket before GHOST verification. The current SDK
can derive a proxy/Safe address from the signing EOA, but the real balance check
must prove it selects the intended funded account. `poly1271` requires an
explicit funder and is GHOST-only until Phase 0.5 proves the venue behavior.

Then run `cargo run --locked --features intl_clob --bin ghost_verify`. The
command prints only redacted per-row status, returns exit code `3` for a
mismatch or query failure, and never treats a clean result as trading approval.
Record the result using
[`docs/PHASE_0_GHOST_REPORT.md`](docs/PHASE_0_GHOST_REPORT.md).

## Phase 0.5 canary probe (dry run by default)

`canary_probe` builds and signs one FAK order and, by default, stops there —
it never calls the venue's order-writing endpoint unless the operator
explicitly sets `POLYCOPY_CANARY_CONFIRM_SUBMIT=yes` in their own shell. No
other mechanism, and no automated tool, can set that variable. A dry run still
authenticates, builds, and signs the order twice against the live venue, which
is enough to confirm the whole pipeline works and that two independent
signatures over the same built order are byte-identical (they are: EIP-712
signing is deterministic for a fixed key and message).

It reuses the same `POLYCOPY_CLOB_*` credential variables as `ghost_verify`,
plus:

```text
POLYCOPY_CANARY_LABEL=[short stable label, used under canary-artifacts/<label>/]
POLYCOPY_CANARY_TOKEN_ID=[decimal outcome token ID]
POLYCOPY_CANARY_SIDE=BUY|SELL
POLYCOPY_CANARY_PRICE=[decimal, strictly between 0 and 1]
POLYCOPY_CANARY_SIZE=[decimal, positive]
POLYCOPY_CANARY_CONFIRM_SUBMIT=yes      # only this exact value submits the first order
POLYCOPY_CANARY_CONFIRM_DUPLICATE=yes   # only this exact value also submits a second, independently-signed copy
```

Choose `POLYCOPY_CANARY_PRICE` away from the current market and
`POLYCOPY_CANARY_SIZE` at the venue's minimum: the order is always
Fill-And-Kill (this project has no cancel-order client, so a resting GTC
canary could be filled later with nothing able to close it), and a price far
from the market keeps an accidental match astronomically unlikely.

Run `cargo run --locked --features intl_clob --bin canary_probe`. Every
persisted record lands under the gitignored `canary-artifacts/<label>/`
directory (spec, submission, and lookup records only — never a private key,
full envelope, or raw response body). Record the redacted result using
[`docs/PHASE_0_5_CANARY_REPORT.md`](docs/PHASE_0_5_CANARY_REPORT.md).

## Database (Phase 1)

The optional `db` feature (`polycopy_engine::copytrading`) opens a pooled
SQLite connection and applies every migration under `migrations/`. Every
connection is configured exactly as blueprint section 6 requires: foreign
keys on, WAL journal mode, a 5 second busy timeout, and `synchronous=FULL`.
Migrations run through sqlx's built-in migrator, which keeps its own
checksum-protected ledger (an `_sqlx_migrations` table) and refuses to run if
an already-applied migration's file content no longer matches what was
recorded — the same protection the blueprint's own
`copy_schema_migrations(version, name, checksum, applied_at)` ledger
describes, provided here by sqlx's existing implementation rather than a
hand-rolled equivalent.

```rust
let pool = polycopy_engine::copytrading::open_and_migrate("./copy.sqlite").await?;
```

Schema so far covers:

- `accounts`, `leader_config`, `leader_wallet_aliases` — one copied account
  and its enabled leader address aliases. Addresses are stored lowercase and
  a partial unique index enforces that the same address can never be enabled
  under two leaders at once (or twice under one). This does **not** limit how
  many leaders one account follows — v1's whole scope is "one account,
  multiple leaders" (blueprint section 1), and multiple simultaneously
  enabled leaders is a directly tested case.
- `leader_events`, `leader_event_observations` — the canonical, immutable
  leader-trade ledger and its raw source observations. `canonical_event_key`
  (`'activity:' || activity_trade_id`) is the sole dedup key: replaying the
  same Activity WS message or re-running backfill over an overlapping window
  is safe by construction (`INSERT OR IGNORE`), never creates a second event.
  An on-chain observation may stay unlinked (`leader_event_id = NULL`) rather
  than being fuzzy-matched onto a canonical event it can't be explicitly
  related to.
- `copy_intents`, `order_attempts`, `position_lots`, `reconciliation_cases`,
  `execution_schedule` — the planning/execution state machine, virtual lots,
  and reconciliation history. `copy_intents` is unique on `(event_id,
  account_id)` (replaying an event creates no second intent);
  `order_attempts` is unique on `(intent_id, attempt_number)`; `position_lots`
  is keyed by `(account_id, leader_id, token_id)` so two leaders holding the
  same token keep distinct lots. `order_attempts.accounted_filled_qty` exists
  because Phase 0.5's canary confirmed a BUY's requested `size` behaves as a
  notional cap, not a literal share count — filled quantity must always be
  read from the venue's actual matched-quantity field, and that same finding
  (order-by-id lookup 404s immediately after a match, with no working
  field-based fallback yet) is why `order_attempts.status` has an `uncertain`
  state that can only be queried and parked, never auto-resubmitted. The
  current recovery code can additionally query authenticated trade history,
  but accepts only an exact precomputed `taker_order_id` match from the stored
  signed envelope; this still needs the Phase 0.5 live canary to prove that
  the precomputed ID and the venue's field are identical for a real FAK.
  All decimal quantities are stored as exact text, never `REAL`, to avoid
  reintroducing the rounding-error class [`OrderReceipt`](src/venue/receipt.rs)
  exists to prevent.

The database file must live on local block storage, not NFS/SMB — this is a
single-host, single-writer deployment; see [`EngineLock`](src/engine_lock.rs)
for the process-level enforcement of "single writer". This covers every table
blueprint section 6 lists. The Phase 3–5 logic described below has since been
implemented, but its real order-writing seam remains intentionally unimplemented.

## Activity ingestion (Phase 2)

The optional `ingest` feature (`polycopy_engine::copytrading::ingest`, implies
`db`) is the first slice of blueprint section 7: a live connection to
Polymarket's real-time activity firehose, filtered to watched leader
addresses and written into `leader_events`/`leader_event_observations`.

**This is not documented by Polymarket and not in the official Rust SDK.**
`docs.polymarket.com`'s WebSocket page and `polymarket_client_sdk_v2` both
only expose a `Market` channel (subscribed by token ID, not by wallet) and an
authenticated `User` channel (your own account only) — neither can watch an
arbitrary leader's trades. The mechanism this project actually uses was
confirmed by reading a working reference implementation
(`PolymarketWebSocketClient`/`PolymarketActivityWsService` in this project's
predecessor, PolyHermes) and verified live against production data (934 real
trades correctly parsed from one 15-second connection): `wss://ws-live-data.polymarket.com`
(Polymarket's RTDS), subscribed with
`{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"},{"topic":"activity","type":"orders_matched"}]}`.
This is a global, unfiltered firehose of every trade on the platform — there
is no server-side per-wallet filter, so [`address_resolver`](src/copytrading/ingest/address_resolver.rs)
filters client-side. Both `trades` and `orders_matched` are subscribed
because the same trade can arrive under either or both.

There is no dedicated trade-ID field in either this payload or the REST
backfill response below. The reference implementation uses
`transaction_hash` alone as the trade identity; **this project does not**,
because a single settlement transaction was confirmed live (2026-08-31,
against real Data API results) to sometimes contain more than one of a
leader's trades — two distinct fills, same token/side/price/timestamp,
different sizes, one shared transaction hash. `transaction_hash` alone
silently dropped the second fill. [`apply.rs`](src/copytrading/ingest/apply.rs)'s
`canonical_event_key` instead composes transaction hash with token, side,
price, and size — the most specific disambiguator either payload exposes;
two fills sharing all four of those too would still collide, and no
available field rules that out entirely.

- [`normalize.rs`](src/copytrading/ingest/normalize.rs) — pure, fully
  unit-tested parsing of one raw WebSocket frame into a trade or a reason it
  isn't one. No networking, no database.
- [`address_resolver.rs`](src/copytrading/ingest/address_resolver.rs) — the
  blueprint's `ArcSwap<HashMap<normalized_address, leader_id>>`: a reload
  builds the full replacement map before publishing it, so a concurrent
  reader never observes a temporary empty map.
- [`apply.rs`](src/copytrading/ingest/apply.rs) — `apply_trade`: resolve,
  check `leader_config.activation_at` (a leader with no `activation_at` yet
  rejects every event, never treated as "no lower bound"), then durably
  record. The one function both `activity_ws` and `backfill` funnel every
  decision through, so the activation rule and the insert path can't drift
  between sources; testable against a real (temp-file) database without a
  live WebSocket or REST call.
- [`activity_ws.rs`](src/copytrading/ingest/activity_ws.rs) — connects,
  subscribes, sends an application-level `"ping"` every 10s (the venue's
  convention, not a WebSocket protocol ping), and reconnects with
  exponential backoff (3s → 6s → ... → 60s) on any error or on 30 seconds
  without an activity-topic message. The connection loop itself isn't
  unit-tested, matching this project's existing precedent for
  network-touching code.
- [`backfill.rs`](src/copytrading/ingest/backfill.rs) — `backfill_leader`:
  catches up a watched leader via the *documented, typed*
  `polymarket_client_sdk_v2::data::Client` (`data-api.polymarket.com`,
  public, unauthenticated), using each leader's latest recorded
  `occurred_at` as a high-water mark (minus a 30s overlap; canonical-event
  uniqueness absorbs the overlap). Verified live: correctly ingested 499
  distinct events from 500 fetched real activity rows for a real address
  (the 500th was the duplicate-transaction-hash fill described above,
  ingested as its own event once the fix above landed).

One easy-to-miss setup requirement: `rustls` 0.23+ does not select a default
crypto backend on its own, and without installing one every TLS connect
(including this WebSocket client's) hangs or panics depending on where the
missing-provider error surfaces. `activity_ws.rs` calls
`rustls::crypto::aws_lc_rs::default_provider().install_default()` before its
first connection attempt (idempotent on every reconnect).

## Intent planning (Phase 3)

[`plan.rs`](src/copytrading/plan.rs)'s `plan_next_batch` reads
`leader_events` past a durable per-account cursor (`planner_cursor`) and,
for each event, either records a `copy_intents` row with `status='pending'`
or one with `status='rejected'` and a specific reason — it never silently
skips an event, and it never mutates `position_lots` or sends an order.
Per blueprint section 8, it deliberately does **not** compute
`planned_qty`/`planned_price`/tick-rounded limit price/TIF: those depend on
live account state and market data at execution time, so they are the
lane's job (Phase 4, not built).

For one event, insert-or-ignore into `copy_intents` and advancing
`planner_cursor` happen in a single transaction, so a crash between the two
rolls back both — replaying the same event afterward creates no second
intent (`copy_intents` is unique on `(event_id, account_id)`). A rejection
records why: no `leader_policy` configured for the leader, the leader
disabled, the trade below `leader_policy.min_leader_trade_size`, or the
event older than `leader_policy.max_signal_age_seconds` — a delayed
WebSocket or backfill event past its signal age becomes a durable
rejection, never a current-market order. An accepted intent's
`decision_deadline_at` is `observed_at + leader_policy.decision_window_seconds`,
and its `config_snapshot_json`/`config_snapshot_hash` freeze the policy
values the decision was made under, immune to a later policy change.
`plan_next_batch` refuses to run at all if no `execution_schedule` row
exists yet, rather than guessing a lane count.

`AddressResolver::reload_from_db` (added alongside this) queries every
currently-enabled leader alias of an enabled leader and reloads the
resolver from it in one call — the piece that was previously only
exercised with hand-built test data, not a real query, is now wired up.

## Fixed-lane executor (Phase 4)

The optional `execute` feature (`polycopy_engine::copytrading::execute`,
implies `db` and `intl_clob`) is `execute_intent`: claim a pending intent
(compare-and-set to `in_progress` — the single-in-flight guarantee two
lanes racing the same intent rely on), size and reserve it, submit via a
generic [`OrderSubmitter`](src/copytrading/execute.rs), then finalize the
receipt into `position_lots`. **`OrderSubmitter` has no real implementation
anywhere in this crate.** Per this project's own operating rule, the
assistant that has been building it will never write or run the code that
places a live order — that boundary is why this trait exists as a seam:
it lets Phase 4's own logic be built and fully tested now, against a fake
submitter, without touching anything that could trade. Phase 5 (below)
extends this same seam to the actual submission and reconciliation layer.

Sizing follows blueprint section 9's `sell_all_on_exit` algorithm exactly
for `SELL`: `sell_qty = max(0, min(leader_virtual_lot, strict_actual_available
- reservations_of_other_active_intents))`, with the intent's own reservation
excluded from that subtraction so a recovery doesn't shrink its original
sale. A strict balance-query failure or a non-positive result is always
`needs_reconcile`, never treated as a zero balance or a silent no-op.
`BUY` sizing (not specified by the blueprint in the same detail) mirrors
the leader's traded size, capped by `leader_policy.max_order_notional /
limit_price`; both sides price at the leader's trade price plus/minus
`leader_policy.price_tolerance_bps` and round to `leader_policy.tick_size`
(ceiling for a BUY's ceiling, floor for a SELL's floor) — implementation
choices, not blueprint mandates, since the blueprint leaves the exact price
formula to the implementation.

The executor never holds an SQLite write transaction across network I/O:
the strict balance read happens before the reservation transaction begins.
Resuming an already-`in_progress` intent (a crash-recovery case) reuses its
already-persisted `planned_qty`/`planned_price` rather than recomputing a
second, possibly-different decision. `finalize_receipt` applies only the
newly confirmed fill delta (computed against `order_attempts.accounted_filled_qty`,
not applied blindly), so replaying the identical receipt — including after
a simulated crash between the venue response and the commit — leaves the
lot unchanged on the second pass. Seven tests cover blueprint's four stated
Phase 4 acceptance criteria directly: distinct lots for two leaders buying
one token, no overselling while a first SELL is still uncertain, idempotent
receipt replay, and single-in-flight claiming — plus the needs_reconcile
paths for a failed balance query and a non-positive sell result.

## Prepared submission and reconciliation (Phase 5)

The `execute` feature also gates `polycopy_engine::copytrading::reconcile`
(blueprint section 10): the layer between a sized, reserved intent and the
venue. It defines a generic `CopyExecution` trait —
`position_for_token_strict`, `order_for_receipt`,
`query_prepared_envelope`, and `submit_exact_envelope` — and, exactly like
Phase 4's `OrderSubmitter`, **`CopyExecution` has no implementation
anywhere in this crate outside test code.** `submit_exact_envelope` is the
one call that would write a live order; this project's assistant will
never write or run it.

What Phase 5 does implement, and test without ever calling a real venue,
is the logic around that seam:

- `load_or_prepare_attempt` persists exactly one envelope per
  `(intent_id, attempt_number)` inside a `BEGIN IMMEDIATE` critical
  section, so two concurrent callers racing to prepare the same attempt
  both observe the single envelope that actually won, never two different
  signed payloads for the same attempt.
- `permitted_recovery_action` is the blueprint's submission recovery
  matrix as pure code: `prepared` may submit; `submitting`/`uncertain`
  (a crash after the request may have crossed the network boundary) must
  query the venue first and can never be resubmitted directly; `accepted`/
  `finalized` route to reconciliation or finalization; `rejected` may
  prepare a new attempt only if the rejection was definitive *and* the
  retry budget below still allows it; every other status, including an
  indefinite rejection or one the matrix doesn't recognize, is `Blocked`.
- `attempts_in_window` and the `MAX_ATTEMPTS_PER_WINDOW` /
  `RETRY_WINDOW_SECONDS` constants (5 attempts per 600 seconds) cap how
  many attempts one intent may accumulate, closing off unbounded retry
  loops.
- `open_reconciliation_case` moves an intent to `needs_reconcile` and
  records a `reconciliation_cases` row in one transaction, so a strict
  venue-query failure or an unresolved submission always produces a
  visible, blocking case rather than silently stalling.
- Before a future order writer can cross the HTTP boundary, it must call
  `mark_attempt_submitting`, which durably stores the submission timestamp.
  `recover_lost_submission_response` then issues only authenticated
  trade-history reads, accepts an exact persisted `taker_order_id` match, and
  stores that ID while leaving the attempt `uncertain` for the normal strict
  by-ID receipt lookup. Empty/delayed history, malformed fingerprints,
  conflicting observations, and query errors open an idempotent visible case
  and freeze the account/token; none permits a resubmit.

The reconciliation tests cover concurrent envelope preparation, every
documented recovery-matrix state and its anti-resubmit property, finite retry
accounting, strict-query case opening, exact ID recovery, delayed/empty
history, and exact partial-fill lot accounting.

## Squadron, CAG, and Control Tower (Phase 6)

`polycopy_engine::copytrading::control_tower` (blueprint section 11,
gated by `db` alone — it needs no venue feature) is a read-only status and
traceability layer. It writes nothing: enabling/disabling a leader and
updating its policy remain plain `UPDATE` statements a caller runs
directly against `leader_config`/`leader_policy`; this module only reads
back what already happened.

- `CopyStrategyStatusShim` always reports `SignalStatus::NoSignal`. DRADIS's
  own Squadron/CAG objects are configuration and read-only status consumers
  that this crate does not link against, vendor, or depend on (see
  `docs/DRADIS_REFERENCE_BASELINE.md`); this shim is this project's own
  implementation of that contract, and it is always `NoSignal` because the
  copy pipeline's ingest/plan/execute/reconcile modules own signal
  generation and execution end to end.
- `leader_status`, `leader_intents`, `leader_lots`, and
  `leader_reconciliation_cases` read one leader's current configuration,
  every intent it has ever produced, every virtual lot its copied trades
  hold, and every reconciliation case attributable to it. Disabling a
  leader stops new intents from being planned for it (Phase 3); it never
  deletes an intent, erases a lot, or hides a case that already exists.
- `trace_attempt` joins one `order_attempts` row back through its
  `copy_intents` row (including the immutable `config_snapshot_json` it was
  planned under), the `leader_events` row that triggered it, the leader and
  account, and every `reconciliation_cases` row attributable to that
  intent — one read that answers the blueprint's Control Tower acceptance
  criterion ("trace any attempt to event, leader, account, configuration
  snapshot, reservation, receipt, and reconciliation state") without a
  caller having to assemble the joins itself.

Tests cover that updating one leader's config/status never touches another
leader's already-planned intent snapshot or status row, that disabling a
leader leaves its existing lots fully visible, that a reconciliation case
with no `intent_id` is correctly excluded from any leader's view rather than
silently misattributed, and a full `trace_attempt` round trip through every
linked table.
