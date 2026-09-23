# poll_resting_gtd's unhandled 404 permanently blocks service startup, with no existing recovery path

## Severity

**Live production outage, and unlike the two prior bugs this session, a
plain resume+restart cycle does not recover it -- it will crash again
identically on every future startup attempt until this is fixed in code.**

## Timeline

1. Intent 723's GTD order (attempt 537, `venue_order_id =
   0x0000000000000000000000000000000000000000000000000000000000000537`)
   was accepted by the venue at 2026-09-23T13:01:12.909Z, `expires_at =
   2026-09-23T13:04:32Z` (the standard ~200s `GTD_MAKER_EXPIRY` window).
2. ~1.5 seconds later (13:01:14.441Z), `poll_resting_gtd`'s call to
   `execution.order_for_receipt` -- looking up the order by its
   `expected_taker_order_id`, which matches the real `venue_order_id`
   exactly (checked, not assumed) -- returned 404. This propagated as an
   unhandled `OrchestrateError::Receipt`, which `copy_persistent` treated
   as fuse-worthy: it opened `persistent_execution_fuse` and exited
   (status 21), with **no `reconciliation_cases` row created** -- a
   different mechanism than the "retry budget exhausted" path from
   `0f58d10`/`0aa8c79`.
3. First recovery attempt: cleared the fuse via `persistent_control
   resume`, restarted. Hypothesis at the time was a transient
   venue-side read/write indexing race (1.5s is short). **Wrong** --
   the service crashed again, same reason, same order id, at
   2026-09-23T13:23:58.822Z -- **22 minutes later**, long past both the
   order's 3-minute expiry and any plausible indexing delay.
4. The order's `expires_at` (13:04:32) had passed nearly 20 minutes
   before this second lookup, so a stale/expired order was one hypothesis
   for the 404. **That hypothesis is wrong -- checked, not assumed.**
   Polymarket's own docs for `GET /data/order/{id}`
   (https://docs.polymarket.com/developers/CLOB/orders/get-order) state
   the endpoint "can also return a canceled or fully matched order," i.e.
   it is *not* limited to currently-resting orders. **The real cause of
   this specific 404 is unknown.** An earlier version of this doc
   asserted the opposite as fact; that assertion was never verified
   against the venue's own documentation and has been retracted. Whatever
   the true cause, the practical problem is unchanged: `poll_resting_gtd`
   has no fallback for a lookup failure of any kind.

## Root cause

`orchestrate/mod.rs:709-723`, `poll_resting_gtd`:

```rust
async fn poll_resting_gtd<E>(...) -> Result<OrchestrateOutcome, OrchestrateError>
{
    let state = execution
        .order_for_receipt(&OrderId(attempt.envelope.expected_taker_order_id.clone()))
        .await
        .map_err(|detail| OrchestrateError::Receipt(format!("GTD order lookup failed: {detail}")))?;
    ...
}
```

Any error from `order_for_receipt` -- including a 404 whose cause is
unknown -- propagates as a hard `OrchestrateError`. A failed lookup
cannot establish whether the GTD maker order filled, expired, or remains
open. Do not interpret a 404 as proof of zero fill.

`copy_persistent`'s top level treats *any* unhandled error from this path
as fuse-worthy and halts the whole process -- reasonable as a
fail-closed default for a money-moving service, but it means **the very
next startup re-runs `walk_existing_attempt` for the same stuck
`'accepted'` GTD attempt first, hits the identical permanent 404, and
crashes again** (confirmed: attempt 537 is still `status = 'accepted'`
after two crash cycles -- the status update inside `poll_resting_gtd`
that would move it to a resolvable state is never reached, because the
lookup itself is what fails, before any DB write). This is a genuine
startup deadlock, not a one-off.

## Why the existing manual recovery tooling doesn't apply

`persistent_control reconcile-uncertain <attempt-id>` is exactly the tool
built for "an attempt whose venue state is unknown, resolve it via a
strict authenticated trade-history lookup instead of the (broken) live
lookup" -- but `inspect_uncertain_attempt_for_operator`
(`reconcile.rs:311-331`) requires `oa.status = 'uncertain'` in its own
query (line 324). Attempt 537 is `'accepted'`, not `'uncertain'` -- the
crash happens *inside* the lookup that would normally be followed by a
status transition, so that transition never gets written. There is
currently no tool that moves a stuck `'accepted'` GTD attempt into the
`'uncertain'` state so the existing strict-lookup machinery can take over.

## Fix (two parts -- and a third, load-bearing gap this doc originally missed)

**Part 1 -- make `poll_resting_gtd` degrade instead of crash.** On a
lookup failure, persist the attempt as `'uncertain'` (the same status
`persist_recovered_order_id`, reconcile.rs:352-378, already writes to in
the FAK-recovery path -- reuse that shape) instead of propagating a raw
`OrchestrateError`, and open a tracking reconciliation case **in the same
transaction as the status write** -- not two separate calls. A crash
between "mark uncertain" and "open the case" leaves an attempt that no
longer loops into `poll_resting_gtd` (good) but that no operator dashboard
knows needs attention (bad); the existing precedent for this exact shape
of multi-step state change is `resolve_exhausted_maker_only_crossing`
(persistent.rs, `0aa8c79`'s sibling commit `84862b5`), which wraps its
UPDATE + case-resolution in `BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK`.
Follow that pattern here.

**This stops the crash loop. It does NOT, by itself, determine whether
any given attempt actually filled -- see the gap below before treating
Part 1 as sufficient to resume and move on.**

**Part 2 -- one-time manual recovery for attempt 537 specifically.**
Since this instance already crashed past the point where Part 1 would
help, attempt 537 needs a one-time state fix before the service can start
at all again. Two ways to get there, in order of preference:

1. Ship Part 1, then use a guarded operator command (or reuse Part 1's
   atomic transition primitive) to move attempt 537 from `'accepted'`
   to `'uncertain'` and open its tracking case in one transaction. This
   prevents repeat polling but does **not** determine whether it filled.
   Do not run the FAK-only `reconcile-uncertain 537` as if it could
   resolve this GTD maker order.
2. If that's not fast enough operationally, a `persistent_control`
   subcommand that does exactly "mark this one `'accepted'` GTD attempt
   `'uncertain'`" is a reasonable narrow addition even before Part 1 is
   fully designed -- but Part 1 should still land so this class of event
   doesn't require a bespoke unblock every time it recurs (and it will
   recur -- every GTD order that terminates between its acceptance and
   the first poll after that termination hits this same gap).

**Do not hand-edit `order_attempts.status` via direct SQL.** This
codebase is explicit elsewhere (`reconfigure_config`'s doc comment,
`persistent.rs:227-231`) that state transitions like this are "a code
path, not a manual SQLite edit" -- there may be invariants (linked
reservations, case bookkeeping) a bare UPDATE would silently violate.

## The load-bearing gap: there is currently no way to verify a GTD maker fill

**Checked, not assumed -- and this is the part an earlier draft of this
doc got wrong by not checking:** `reconcile-uncertain <attempt-id>`
routes through `inspect_uncertain_attempt_for_operator` ->
`lookup_prepared_fak_in_trade_history` ->
`recover_fak_taker_order_from_trades` (`src/venue/trade_history_recovery.rs`).
That function's first two checks are:

```rust
if envelope.order_type != "FAK" {
    return Err(TradeHistoryRecoveryError::UnsupportedOrderType);
}
```

and, deeper in the match loop, `trade.role != AccountTradeRole::Taker`
excludes any trade where the account was the maker. A GTD post-only
order is *never* a taker fill by construction -- that is what "post-only"
means. So running `reconcile-uncertain 537` (or any GTD attempt) does not
inspect trade history at all; it fails immediately with
`UnsupportedOrderType`, before it gets anywhere near determining a real
answer.

**Consequence: Part 1 (above) stops the crash loop, but does not, on its
own, let anyone determine whether attempt 537 -- or any future GTD
maker-only attempt that lands in `'uncertain'` -- actually filled before
terminating.** After Part 1 ships and the service resumes, intent 723
will sit in `needs_reconcile` indefinitely, with its reservation still
held, until one of:

1. Someone extends the strict trade-history matcher to also handle a
   maker-side fill (matching on `role == Maker` and the *accepted*
   `venue_order_id`/`maker_order_id` rather than
   `expected_taker_order_id`, since a maker order's fill isn't keyed the
   same way a FAK taker fill is) -- this is real, unscoped design and
   implementation work, not a small addition to the existing matcher.
2. Someone determines attempt 537's true state through some other
   channel (the account owner checking Polymarket's own UI/trade history
   for this wallet and this specific order id directly, for instance) and
   manually resolves the case through whatever tooling fits that finding.

**Do not resume the persistent service on the premise that `Part 2 +
reconcile-uncertain` closes this out.** It does not, today.

**Decision (2026-09-23, account owner): ship Part 1 alone now; track
building a GTD-maker-aware strict trade-history matcher as a separate,
later follow-up, not part of this fix.** Consequence, explicit: after
this deploys and the service is running again, intent 723 / attempt 537
stays in `needs_reconcile` with its reservation held until that follow-up
lands (or the account owner determines the order's true state through
some other channel and it gets resolved manually). This is a deliberate,
acknowledged tradeoff, not an oversight -- do not "helpfully" build (1)
later without checking whether it's still wanted, and do not let this
gap block deploying Part 1, which is what actually stops the crash loop.

## Verification

- A regression test for `poll_resting_gtd` (or whatever replaces its
  error-propagation shape) asserting that a lookup failure results in the
  attempt moving to `'uncertain'` **and** the tracking case existing
  atomically with it, not an unhandled error reaching the caller and not
  a status-changed-but-uncased half-state.
- **This does NOT include a test proving `reconcile-uncertain` can
  resolve a GTD attempt** -- it currently can't (see the gap above). A
  test asserting `a_maker_only`-shaped attempt promoted to `'uncertain'`
  by this fix, then run through `reconcile-uncertain`, correctly reports
  `UnsupportedOrderType` (not a false "resolved") is the honest version of
  this check until the matcher itself is extended.
- If the matcher is extended (see "load-bearing gap" above): a test
  confirming it can correctly identify a maker-side fill for a GTD/
  post-only envelope, separate from and in addition to its existing FAK
  taker-fill coverage.
- Manually confirm, once attempt 537's true state is established by
  *some* working method, that intent 723's final state (filled/cancelled/
  expired) and any reserved funds are reconciled accordingly -- this is
  the actual ground truth this whole investigation has been trying to
  establish, and Part 1 alone does not reach it.
- `cargo test --all-features --locked` and
  `cargo clippy --all-targets --all-features --locked -- -D warnings`
  clean, as with every prior change this session.
