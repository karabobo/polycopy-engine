# DRADIS Copy Engine Blueprint

**Status:** implementation specification, not yet approved for automated live trading
**Scope:** one account, multiple leaders, leader-specific policy, Polymarket Intl CLOB
**Out of scope:** pm-robot integration, automated leader import, paper trading, multi-instance HA,
generic multi-venue support, and proportional leader exits in the first release.

This document is the repository source of truth for the PolyHermes-to-DRADIS copy
engine rebuild. It replaces external planning artifacts. `pm-robot` remains a
research product; it neither starts this engine nor changes its configuration.

## 1. Release Position and Non-Negotiable Invariants

The engine may be built in GHOST mode immediately. It may not place live orders
until the Phase 0.5 canary and every Phase 7 gate pass.

1. **Leader events are durable facts.** A WebSocket, `broadcast`, or `mpsc`
   message is only a notification. The durable event ledger is the only input to
   trading decisions.
2. **Activity data is the sole execution trigger.** Activity WS and Activity REST
   backfill create canonical events. On-chain data confirms or audits events but
   never creates an additional tradable event.
3. **The execution unit is `(account_id, token_id)`.** A leader owns attribution
   and policy, not the account's physical token balance. All position-number
   reads and writes for one account/token are serial.
4. **The database is the durable queue.** Creating a `copy_intent` and advancing
   the event consumer cursor happen in one transaction. A lane wakeup carries no
   business data and can be lost safely.
5. **A submitted logical order has one immutable envelope per attempt.** Salt,
   timestamp, expiration, payload, and signature are generated once, persisted,
   and never rebuilt for that attempt.
6. **Unknown external submission is fail-closed.** A timeout or process crash
   after the HTTP boundary is not evidence that no order exists. It must be
   queried first; absent a proven lookup and duplicate-submission guarantee, the
   account/token moves to `needs_reconcile` rather than being retried.
7. **Only confirmed fills change virtual lots.** A requested quantity, accepted
   quantity, and filled quantity are distinct. Lot accounting applies a durable,
   idempotent filled-quantity delta exactly once.
8. **Strict venue truth never degrades to an empty position list.** A token query
   error is an error, not a zero balance. It blocks new work for that account/token.
9. **Exactly one copy-engine process may own a database.** Fixed lanes serialize
   work only inside one process; a deployment lock prevents two processes from
   sending different intents for the same key concurrently.
10. **A persisted shard belongs to a persisted schedule.** The shard algorithm,
    version, and lane count are durable. A deployment cannot silently change lane
    count while non-terminal intents exist.
11. **A copied order has a deadline and bounded price.** Activation prevents old
    history from entering the ledger; signal age and a persisted decision deadline
    prevent delayed data from becoming a current-market order.

## 2. Facts Verified in the DRADIS Baseline

These are implementation constraints, not hypotheses.

- `src/venues/intl/orders.rs` builds order salt and timestamp from wall-clock
  time, then signs the full EIP-712 order. Rebuilding a logical order therefore
  creates another signed order hash.
- `src/venues/intl/mod.rs` calculates `matched_shares` but currently returns
  `intent.quantity` in `Fill.filled`. A FAK zero-fill or partial-fill can be
  reported as a full fill.
- `IntlClobVenue::positions()` reads only its mutable `active_tokens` set and
  skips token-query failures. This cannot be used as a strict copy-engine truth
  source.
- `MarketId` is the Polymarket outcome-token identifier in the Intl venue. It is
  not the market-wide condition ID.
- The current `Execution` trait does not expose strict per-token positions,
  prepared-envelope lookup, or exact-envelope submission. The copy engine needs
  a dedicated trait rather than a permissive fallback.
- The installed CLOB SDK exposes order lookup by order ID and list filters by
  order ID, market, or asset. It has no verified lookup-by-envelope/salt API.

Phase 0 fixes the `Fill.filled` defect before copy-engine code depends on the
venue. The exact CLOB behavior for repeated submission of an identical signed
envelope remains unverified and is a Phase 0.5 release gate.

## 3. State Model

### 3.1 Event and intent states

- `leader_events` is immutable canonical input.
- `copy_intents.status` is one of `pending`, `in_progress`,
  `partially_filled`, `completed`, `rejected`, `cancelled`,
  `needs_reconcile`, or `dead_letter`.
- `order_attempts.status` is one of `prepared`, `submitting`, `uncertain`,
  `accepted`, `rejected`, `finalized`, or `error`.

`submitting` is deliberately treated as **uncertain after a restart**. The
process can die after bytes leave the host and before it records the response.
It must never be treated as a proven non-submission.

### 3.2 Receipt and reservation rules

`OrderReceipt` has `requested_qty`, `accepted_qty`, `filled_qty`,
`remaining_qty`, and `venue_status`. Only a positive, newly accounted
`filled_qty` delta changes `position_lots`.

`copy_intents.reserved_qty` is a durable reservation, not a display field:

1. A lane calculates a sell quantity from its leader lot, strict account balance,
   and reservations owned by other active intents.
2. Before HTTP submission, it writes its own reservation in a database
   transaction.
3. A final receipt transaction applies the fill delta, adjusts/releases that
   reservation, records the receipt, and changes the intent state together.
4. An unknown order preserves its reservation and blocks further work for the
   account/token until reconciled.

An aggregate account balance cannot identify which leader owns a missing token.
Therefore `actual_balance == 0` is not proof that a particular SELL completed.
Only a venue receipt can finalize that intent automatically; otherwise it is
`needs_reconcile`.

## 4. Phase 0: Fork Baseline and Venue Repair

### Work

- Fork DRADIS at the selected commit and pin that commit. Do not track upstream
  `main` during this implementation.
- Feature-gate or unregister unused legacy Vipers, Raptors, and LLM advisor
  modules. Do not physically delete them until Phase 7 passes.
- Fix `IntlClobVenue` so `Fill.filled` returns actual `matched_shares`, not the
  requested quantity.
- Add a regression test for FAK zero-fill and partial-fill responses.
- Build and run the existing GHOST path to verify CLOB connection, authentication,
  collateral, and a manually checked read-only balance.
- Add an exclusive local process lock (for example a `flock` lock file next to the
  copy database). Startup must fail if another copy-engine instance holds it.
  Compose/systemd must run exactly one replica and mount the database only from
  that host.

### Acceptance

- `cargo build --release --features intl_clob` succeeds.
- `cargo test` succeeds, including a test that would fail before the
  `matched_shares` correction.
- A second process cannot acquire the copy-engine lock.
- GHOST balances agree with one manually checked wallet snapshot.

## 5. Phase 0.5: CLOB Submission-Safety Canary

This gate occurs before any automatic retry is implemented or enabled.

### Questions to establish with a deliberately tiny real order

1. Can an accepted order be located deterministically from fields available in a
   persisted envelope when the HTTP response is intentionally interrupted?
2. Does submitting byte-identical signed order data twice yield an idempotent
   result, a deterministic duplicate rejection, or two independently executable
   FAK orders?
3. Which response fields are stable enough to populate `OrderReceipt` and its
   cumulative filled quantity?

### Gate

- Record the request/response behavior and the exact lookup method in a test
  report committed with the code.
- Until the result proves both lookup and duplicate behavior safe,
  `submitting`/`uncertain` attempts may only query then enter
  `needs_reconcile`; they may not automatically submit again.
- A GHOST run cannot satisfy this gate because it does not cross the order HTTP
  boundary.

## 6. Phase 1: Account Model, Connection Policy, and Schema

Create `src/copytrading/` with `leader.rs`, `event.rs`, and a standalone
versioned migration module. It must not reuse DRADIS's error-suppressed
`ALTER TABLE` pattern.

### Connection and migration rules

- Use a `copy_schema_migrations(version, name, checksum, applied_at)` ledger.
  Refuse startup when an already-applied migration checksum differs.
- Configure every pooled SQLite connection through `SqliteConnectOptions`:
  `foreign_keys=true`, WAL journal mode, `busy_timeout=5000`, and
  `synchronous=FULL`.
- The first release is a single-host, single-writer SQLite deployment. The
  database must reside on local block storage, not NFS/SMB. SQLite is acceptable
  for the current event rate, not a claim of multi-writer scalability.

### Core schema requirements

`accounts`, `leader_config`, and `leader_wallet_aliases` define one copied account
and enabled leader aliases. Alias writes must reject an address that belongs to
two enabled leaders before persisting the configuration.

`leader_events` stores one canonical Activity trade with:

- `canonical_event_key = 'activity:' + activity_trade_id`
- `leader_id`, `condition_id`, `token_id`, `outcome_index`, `side`, `size`,
  `price`, `tx_hash`
- distinct `occurred_at` and `observed_at`
- `onchain_confirmed`

`leader_event_observations` retains all source payloads. Activity WS and Activity
backfill may produce two observations for one canonical event. An on-chain
observation may stay unlinked if it cannot be related by an explicit source
identifier; it must not use fuzzy field matching or create another executable
event.

`copy_intents` includes at least:

- event, account, leader, token, side, immutable configuration snapshot/hash
- `planned_qty`, `planned_price`, tick size, TIF, `decision_deadline_at`
- durable `reserved_qty`
- `shard_scheme_version`, `lane_count`, and `shard_id`
- lifecycle status and timestamps

`order_attempts` includes at least:

- intent and attempt number
- exact serialized `envelope_json`
- attempt state, `receipt_json`, venue order ID/status
- requested, accepted, filled, remaining, and `accounted_filled_qty`
- timestamps and failure detail

`position_lots` is keyed by `(account_id, leader_id, token_id)`. It changes only
from an idempotent receipt-accounting transaction. `reconciliation_cases` records
drift, unknown submission, and manual resolution history.

Persist the scheduler configuration in an `execution_schedule` record. A startup
with a different lane count, shard algorithm, or scheme version must refuse to
run while any older non-terminal intent exists. Draining those intents or an
explicit schedule migration is required before changing lanes.

### Acceptance

- Migrations are idempotent and checksum-protected.
- Foreign-key enforcement is verified on a freshly borrowed pool connection.
- Attempting a duplicate enabled alias fails without replacing the active address
  index.
- Changing `lane_count` with pending work refuses startup.

## 7. Phase 2: Activity-Led Ingestion and Auditing

Implement `copytrading/ingest/{activity_ws,onchain_ws,backfill,normalize}.rs`.

### Rules

- Activity WS is the only realtime event trigger. Activity REST backfill uses the
  same Activity trade ID and is equivalent to WS for canonical event creation.
- On-chain WS writes audit observations and confirms existing events only. It
  never creates an additional `leader_event`.
- Address resolution uses an `ArcSwap<HashMap<normalized_address, leader_id>>`.
  Build and validate the replacement map fully, then atomically publish it.
- All paths reject `occurred_at < activation_at`. This is distinct from rejecting
  stale signals at execution time.
- Backfill uses a high-water mark `(occurred_at, activity_trade_id)` plus a small
  overlap. Canonical event uniqueness absorbs overlap.

### Acceptance

- A maker-order counterparty address does not match a watched leader.
- Reloading aliases never exposes a temporary empty map.
- A historical event observed after activation is still rejected if it occurred
  before activation.
- Activity WS and Activity REST create one canonical event; uncorrelated on-chain
  data creates audit-only data.

## 8. Phase 3: Transactional Intent Planning

`CopyPlanner` reads the event ledger by cursor. It does not mutate lots or send
orders.

For each executable Activity event it validates the enabled leader configuration,
market/category policy, leader-trade minimum, event age, and explicit leader
filters. Rejections are durable and explainable.

For a forwarded event, one transaction must:

1. insert-or-ignore `copy_intents(event_id, account_id)` with the full policy
   snapshot, scheduler identity, and decision deadline;
2. update the planner cursor; and
3. commit before signaling the target lane.

The lane computes live sizing and the executable limit price, because those depend
on current account state and market data. Before preparing an order it persists
the resulting `planned_qty`, side-specific price bound, tick-rounded limit price,
TIF, and deadline. A recovery never recalculates a persisted decision; it either
continues that exact decision or cancels it after expiry.

Every policy must define:

- maximum signal age and decision deadline;
- price formula and side-specific tolerance bound;
- tick rounding direction, valid price range, and FAK-only behavior in v1;
- maximum order notional and leader minimum trade size; and
- market closed/resolved/invalid handling.

### Acceptance

- A crash between intent insertion and cursor update rolls back both.
- Replaying an event creates no second intent.
- A delayed WS or backfill event that exceeds its signal age becomes a durable
  rejection, never a current-market order.
- Price and quantity chosen once remain unchanged after a restart.

## 9. Phase 4: Fixed-Lane Account/Token Executor and Lots

Use a fixed number of lanes, initially selected by load testing. Route an intent
only by its already-persisted schedule and `shard_id`; recovery scans only rows
for that lane's schedule identity and shard. There is no unscoped pending-intent
scan.

Only the lane executor may read or write `position_lots` and reservations. It
claims work with a compare-and-set transition from `pending` to `in_progress`.

### Sell algorithm

For `sell_all_on_exit`, calculate:

```text
leader_sellable = leader_virtual_lot
account_sellable = strict_actual_available - reservations_of_other_active_intents
sell_qty = max(0, min(leader_sellable, account_sellable))
```

The current intent's own reservation is excluded from the second line so recovery
does not reduce its original sale. A strict token-balance error, a non-positive
result, or an already-blocked key becomes `needs_reconcile`; it is never treated
as a zero balance.

`proportional_exit` is rejected in v1. It requires a reliable leader-holdings
ledger and cannot be inferred from follower lots alone.

### Reservation and receipt transaction

The executor must not hold an SQLite write transaction across network I/O.

1. Obtain strict venue data outside the transaction.
2. In a short transaction, revalidate the claimed intent, record the immutable
   decision, and reserve the intended quantity.
3. Submit through Phase 5.
4. In one short finalization transaction, apply only the newly confirmed fill
   delta, update the lot, update `accounted_filled_qty`, release/adjust the
   reservation, store the receipt, and update the intent state.

### Acceptance

- Two leaders buying one token retain distinct virtual lots.
- Two leaders attempting SELL while the first is uncertain cannot over-reserve or
  oversell the account balance.
- Replaying the same receipt, including after a crash between external response
  and database commit, leaves lots unchanged on the second pass.
- A deliberately incorrect cross-lane route is caught by a single-in-flight
  account/token test.

## 10. Phase 5: Prepared Submission, Strict Venue API, and Reconciliation

Introduce a copy-specific `CopyExecution` trait. Only `IntlClobVenue` implements
it in v1. There is no fallback to `Execution::positions()`.

```rust
#[async_trait]
pub trait CopyExecution {
    async fn position_for_token_strict(&self, token: &MarketId) -> Result<Decimal>;
    async fn order_for_receipt(&self, order_id: &OrderId) -> Result<VenueOrderState>;
    async fn query_prepared_envelope(
        &self,
        envelope: &PreparedOrderEnvelope,
    ) -> Result<Option<OrderReceipt>>;
    async fn submit_exact_envelope(
        &self,
        envelope: &PreparedOrderEnvelope,
    ) -> Result<OrderReceipt>;
}
```

`load_or_prepare_attempt` serializes envelope creation with a short
`BEGIN IMMEDIATE` critical section. It reads an existing serialized envelope or
inserts exactly one new one. It must roll back reliably on every error and must
not hold the write lock across order HTTP calls.

### Submission recovery matrix

| Persisted attempt state | Permitted recovery action |
| --- | --- |
| `prepared` | Mark `submitting`, then submit once. |
| `submitting` after restart | Treat as `uncertain`; query first. |
| `uncertain` | Query first. Resubmit only when the Phase 0.5 canary has proven this safe. |
| `accepted` / `finalized` | Reconcile or finalize the receipt delta; never submit again. |
| `rejected` | A new attempt may be prepared only if the rejection is definitive and policy/deadline/retry budget permit it. |

When lookup is unavailable, returns no result, or returns contradictory data, the
intent becomes `needs_reconcile` and blocks the account/token. It is not
auto-resolved from an aggregate token balance.

Retries are finite: default maximum five attempts within ten minutes. Exhaustion
opens a visible reconciliation case. A strict venue query error also opens a case;
it never becomes an empty balance.

### Acceptance

- Concurrent calls for one `(intent_id, attempt_no)` persist one identical
  envelope.
- FAK zero-fill and partial-fill produce the correct receipt and no phantom lot.
- A crash after the request may have crossed the network boundary never causes a
  direct resubmission on restart.
- Receipt finalization is idempotent under duplicate API reads and crash recovery.
- A token query error blocks the key and produces a visible case.

## 11. Phase 6: Squadron, CAG, and Control Tower

The copy pipeline owns execution. DRADIS Squadron/CAG objects provide configuration
and read-only status only.

- A `CopyStrategyStatusShim` always returns `NoSignal`; it displays leader status,
  intents, attempts, lots, reservations, and reconciliation cases.
- Enabling/disabling a leader affects new intent planning only. It does not erase
  lots or silently close positions.
- Configuration updates affect the next event. Existing intents retain their
  stored policy snapshot and immutable decision.

### Acceptance

- Updating one leader does not affect another leader's snapshot.
- Disabling a leader produces durable planner rejections while retaining its
  existing lot visibility.
- Control Tower can trace any attempt to event, leader, account, configuration
  snapshot, reservation, receipt, and reconciliation state.

## 12. Phase 7: End-to-End Verification and Live Gates

### Historical replay

Label historical PolyHermes rows before using them:

- Correct rejections, such as price or daily-limit policy, remain negative tests.
- Aggregated events that lost outcome direction are positive regressions: the new
  Activity event parser must preserve a real outcome token.
- Sell retries that expired after hundreds of attempts are regressions: they must
  converge to a receipt-backed result or visible `needs_reconcile` case.
- Combination bets remain an explicit v1 exclusion.

### Required tests

1. Event delivery, cursor crash injection, duplicate Activity WS/backfill, and
   alias reload atomicity.
2. Same account/token, multiple leaders, interleaved BUY/SELL replay with exact
   virtual-lot attribution.
3. Reservation collision where a first SELL is uncertain and a second SELL arrives.
4. Process death after request dispatch but before receipt persistence, followed by
   recovery without a duplicate lot or unsafe resubmission.
5. Duplicate receipt delivery and partial fill accounting with exactly-once lot
   deltas.
6. Token query failure, stale signal, changed lane count, and second-process lock
   acquisition failures.
7. A 72-hour GHOST run that reconciles ledger, intent, and strict venue reads
   without unexplained event loss.

### Live progression

1. Complete Phase 0.5 with one deliberately tiny canary order.
2. Run one leader with an explicitly bounded amount for seven days.
3. Require zero unresolved, over-tolerance drift cases beyond the configured SLA.
4. Any unresolved drift or submission uncertainty freezes its account/token until
   a human resolves the recorded case.
5. Only then add leaders one at a time. Multi-account and HA require a separate
   architecture review; they are not enabled by the v1 schema alone.

## 13. Dependency Order

```text
Phase 0    Fork baseline, venue fix, single-instance lock
    |
Phase 0.5  Tiny CLOB submission-safety canary
    |
Phase 1    Connection policy, migrations, durable state
    |
Phase 2    Activity-led ingest and audit
    |
Phase 3    Transactional intent planning
    |
Phase 4    Fixed lanes, reservations, virtual lots
    |
Phase 5    Prepared submission, strict queries, reconciliation
    |
Phase 6    Squadron/CAG status integration
    |
Phase 7    Replay, GHOST, small live progression
```

Phase 0.5, Phase 4, and Phase 5 are financial-correctness gates. A passing build,
green unit tests, or a healthy WebSocket alone is not permission to bypass them.
