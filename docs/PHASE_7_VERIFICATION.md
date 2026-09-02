# Phase 7: end-to-end verification

See `docs/COPY_ENGINE_BLUEPRINT.md` section 12. This covers the "Historical
replay" and "Required tests" portions only. "Live progression" (running one
leader with a bounded live amount for seven days) remains a separate, later
gate. A bounded writer now exists, but it is default-disabled and cannot
satisfy the live gate without independent Phase 0.5 and 12-hour GHOST
evidence; see `docs/LIVE_PROGRESSION_RUNBOOK.md`.

## Required tests: status

| # | Requirement | Status | Where |
| --- | --- | --- | --- |
| 1 | Event delivery, cursor crash injection, duplicate Activity WS/backfill, alias reload atomicity | Done | `plan.rs::a_crash_between_intent_insert_and_cursor_advance_leaves_neither_persisted`; duplicate/alias coverage already existed in `ingest/apply.rs` and `ingest/address_resolver.rs` |
| 2 | Multi-leader interleaved BUY/SELL, exact virtual-lot attribution | Done | `execute.rs::multiple_leaders_interleaved_buy_and_sell_keep_exact_per_leader_lot_attribution` |
| 3 | Reservation collision: first SELL uncertain, second SELL arrives | Already done | `execute.rs::a_second_leaders_sell_cannot_oversell_while_the_first_is_still_uncertain` (pre-existing) |
| 4 | Process death after dispatch, before receipt; recovery without a duplicate lot or unsafe resubmission | Done | `reconcile.rs::a_crash_between_dispatch_and_receipt_recovers_to_exactly_one_lot_no_resubmission` — chains real recovery + real finalize end to end, not just the pieces each already covered alone |
| 5 | Duplicate receipt delivery, partial fill, exactly-once lot deltas | Already substantially done | `execute.rs`/`reconcile.rs`'s existing idempotent-finalize tests; also re-exercised by #4 above |
| 6 | Token query failure, stale signal, changed lane count, second-process lock failure | Done | Token query failure and stale signal were already covered; second-process lock was already covered (`process_lock.rs`); **changed lane count was a genuine gap** — closed by the new `plan::verify_schedule_compatible_with_pending_work` (a real startup check, not just a test — see below) |
| 7 | 12-hour GHOST run reconciling ledger/intent/strict venue reads with no unexplained event loss | Tooling built; the run itself is the account owner's operation | See "12-hour GHOST run" below |
| — | Historical replay | Done — 305 real PolyHermes production rows replayed; combination-bet exclusion implemented, grounded in PolyHermes's own source | See "Historical replay" below |

## New: the lane-count/shard-scheme startup check

`copytrading::plan::verify_schedule_compatible_with_pending_work` did not
exist before this work — the blueprint calls for it ("a startup with a
different lane count, shard algorithm, or scheme version must refuse to run
while any older non-terminal intent exists") but nothing enforced it. An
intent's `shard_id` is computed once, at planning time, from the
`lane_count` active then. If the operator changes `lane_count` while a
`pending`/`in_progress`/`partially_filled`/`needs_reconcile` intent still
carries the old value, that intent's shard assignment no longer means what a
lane worker under the new scheme would assume. This function is read-only
(it changes nothing); a process's own startup sequence is expected to call
it and refuse to proceed on `Err`. `copy_run` invokes this check before it
loads any runnable intent, so a changed schedule blocks the bounded runtime
instead of merely documenting a required manual check.

## 12-hour GHOST run

`ghost_verify` now also prints one `GHOST_RECORD: {json}` line per run (in
addition to its existing human-readable output) — a redacted,
JSON-serializable summary (`ghost::GhostRunRecord`) of that run's
collateral/token-balance comparison results. Running `ghost_verify`
repeatedly over 12 hours and appending its output to one log file is enough
to produce the evidence this required test asks for:

```sh
# Every 5 minutes, for 12 hours: run ghost_verify with the operator's own
# credentials and snapshot values (see README.md's "Authenticated GHOST
# verification" section), append its output to one log.
ghost_verify >> ghost-12h.log 2>&1
```

The repository supplies a default-disabled `polycopy-engine-ghost.timer` for
the designated server; it must be installed and enabled only after the
read-only GHOST configuration has been independently reviewed. After the
window, summarize the whole log:

```sh
ghost_drift_report ghost-12h.log            # default 1200s (20 min) gap tolerance
ghost_drift_report ghost-12h.log 900        # or pick your own tolerance
```

`ghost_drift_report` (new binary, `src/bin/ghost_drift_report.rs`, logic in
`src/ghost_drift.rs`) parses every `GHOST_RECORD:` line, sorts by
`checked_at_utc`, and reports: total runs, how many were clean, every
unclean run's detail, and every gap between consecutive runs wider than the
tolerance — a gap is exactly "unexplained event loss": either the scheduler
missed a run or the process producing the log was down, and a string of
individually-clean runs on either side of a gap must never hide it. Exit
code 0 only when every run was clean, no gap exceeded tolerance, and no log
line failed to parse, and the observed window covers at least 12 hours; exit code 3 otherwise. Neither this binary nor
`src/ghost_drift.rs` contacts the venue, opens a database, or reads a
credential — it only parses a local text file.

## Bounded writer safety checks

The executable `copy_run` is intentionally not a daemon or a timer target.
It starts only with an exact runtime guard and rejects any configuration other
than one leader, one attempt, and a maximum 5-USDC notional. Before it can
prepare a buy it requires a strict CLOB collateral response **and** strict
allowance data; it uses the smaller of the two, subtracts in-progress BUY
reservations for every token in the account, and moves any query uncertainty
to `needs_reconcile`. The Activity WebSocket and REST backfill run under one
supervisor; either stream ending or failing ends the bounded process before a
later intent can be executed. These are implementation gates, not evidence
that a real order is safe: the remaining evidence gates are listed in the
live-progression runbook.

## Historical replay

`tests/historical_replay.rs` has two tests.

**`replays_a_real_production_batch_without_loss_or_a_panic`** uses real
data: `tests/fixtures/leader_activity_slim.json`, 305 rows extracted
read-only from a PolyHermes production database backup
(`/srv/polyhermes/backups/mysql-final-cutover.sql` on the account owner's
own execution server — the same host this project already deploys to —
`leader_activity_event` table, 2026-07-21 through 2026-07-23, 9 distinct
leader wallets, 105 distinct outcome tokens), downloaded into this repo with
the account owner's explicit permission. Only trade-shape fields survived
the extraction (wallet, token, condition, side, outcome index, price, size,
event time, source event ID) — no credential, internal ID, or
PolyHermes-internal processing-status field was carried over. The test
replays every one of the 305 rows through the real `apply_trade` →
`plan_next_batch` pipeline and asserts nothing is lost, duplicated, or
causes a panic or planning error: exactly 305 events, exactly 305 intents,
every one landing as `pending` or `rejected` and nothing else.

**`a_replayed_historical_batch_preserves_aggregated_outcome_direction_and_rejects_correctly`**
replays a small, hand-built batch of `NormalizedTrade` rows through the same
pipeline (the real dataset above happens not to contain a
naturally-occurring duplicate-transaction-hash case, so this pattern is
still exercised synthetically) and pins:

- **Aggregated events losing outcome direction** (a real, already-fixed
  Phase 2 defect: one `transaction_hash` can carry two distinct fills with
  different token/side/price/size; an earlier `canonical_event_key` design
  keyed on `transaction_hash` alone and silently dropped one). The replay
  proves this end to end through the real pipeline, not just `apply.rs`'s
  own unit test.
- **Correct rejections remain negative tests**: a trade below the leader's
  policy minimum ingests as an event but plans as a durable rejection, never
  a dropped event and never a live order.

**Sell retries expiring after hundreds of attempts** is covered structurally
by the retry budget (`reconcile::MAX_ATTEMPTS_PER_WINDOW` /
`RETRY_WINDOW_SECONDS`, already tested in `reconcile.rs`) rather than
repeated here — the budget is exactly the mechanism that makes "hundreds of
attempts" impossible.

**Combination bets remain an explicit v1 exclusion** is **now implemented**,
in `copytrading::ingest::normalize::parse` (`src/copytrading/ingest/normalize.rs`).
No official Polymarket documentation names this concept, and the real
305-row replay dataset happens not to contain an example — but PolyHermes's
own source, read directly (not vendored; see
`docs/DRADIS_REFERENCE_BASELINE.md` for why reading a predecessor's source
for a behavior it already discovered is a different thing than copying its
code), settles what it means operationally:
`CopyOrderTrackingService.kt`'s buy and sell paths both check
`if (trade.isCombo == true) { recordUnsupportedComboDecisions(...); return }`
before ever building a copy order, fed from an `isCombo: Boolean?` field
PolyHermes reads directly off the real activity/trade API response
(`PolymarketClobApi.kt`'s `Activity` model, JSON key `isCombo`, no rename).
The official Rust SDK this project uses (`polymarket_client_sdk_v2` 0.7.0)
does not expose that field on any of its typed response structs, so
`normalize.rs` reads it directly off the raw WS payload instead of going
through the SDK, the same way every other field in that parser already
does. A trade with `"isCombo":true` is rejected before any other field is
even checked (`ParseResult::Rejected`, mirroring how every other
unsupported/malformed message is handled — no `leader_events` row, no
intent, no order); `isCombo: false` or the field's absence parses exactly
as before. Three new tests in `normalize.rs` cover all three cases.
