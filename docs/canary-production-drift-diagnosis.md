# Canary/production BUY-path drift diagnosis

Status: **B and C mitigations implemented; current Phase 0.5 evidence still
requires a new attested BUY canary.** This document preserves the diagnosis
that motivated the change. Full construction-path unification remains a
separate design decision.

## The problem

`canary_run.rs` (Phase 0.5's verification tool) and `prepare.rs`
(`EnvelopePreparer`, the real execution path's order-construction code)
are two **independently hand-written** implementations of "build and sign
a Polymarket order." This was a deliberate choice — `canary_run.rs`'s own
module doc says so directly:

> "Credential handling mirrors `ghost_run.rs`... but is kept as a
> separate copy rather than a shared refactor, so this new, higher-risk
> code path cannot regress the already-verified Phase 0 GHOST tool."

That isolation had a real cost: **the two paths diverged**, and originally
nothing detected it.

- `prepare.rs`'s `EnvelopePreparer::prepare` (the BUY-precision fix) now
  constructs a BUY via
  `client.market_order()....amount(Amount::usdc(budget))....build()` —
  a cent-denominated USDC maker budget, specifically to avoid the CLOB's
  maker-amount-precision rejection that a `shares * price` product
  routinely triggers.
- At diagnosis time, `canary_run.rs::build_signable_order()` still
  unconditionally constructed **every** order, BUY or SELL, via
  `client.limit_order()....price(spec.price())....size(spec.size())....build()`.
  It now independently constructs BUYs as
  `client.market_order()....amount(Amount::usdc(budget))....build()`.

**Consequence at diagnosis time: a prior Phase 0.5 pass did not prove the
BUY-precision fix.** Historical canary records remain evidence for their
then-built envelope, lookup, duplicate, and receipt behavior, but cannot
cover the current construction version without a new attested canary.

## Why this keeps happening, not just this once

Originally, nothing tracked "which commit was the last passing canary run
validated against," nor whether signing/order construction had changed:

- `docs/PHASE_0_5_CANARY_REPORT.md` records dates (2026-08-31 through
  2026-09-02) and one unrelated commit hash (cited for a different patch,
  not the gate decision itself). Its "Decision: passed" line is not tied
  to any commit SHA.
- No file, DB row, or CI check then recorded "last successful canary run's
  commit." The BUY-precision fix was not exercised against the canary path.

The canary/production divergence was therefore not a one-off mistake —
it's the predictable result of "verify tool A, ship code B" with no
mechanism forcing them to be reconciled. Whatever changes next in the
signing/order-construction path will silently drift from the canary the
same way, again, unless something closes this gap structurally.

## Resolution and remaining option

**A. Unify the construction path.** Make `canary_run.rs` call through
the same order-construction logic `prepare.rs` uses (once the BUY fix
lands and stabilizes), instead of hand-duplicating it. This is the
strongest fix — the canary would then, by construction, exercise exactly
what production ships — but it's the biggest change, and it reintroduces
some of the coupling the original separate-copy decision was trying to
avoid (a bug in the shared path would now affect both the "proven safe"
tool and the thing it's proving). That coupling risk is arguably the
correct tradeoff for a *verification* tool specifically — the whole point
is to catch exactly this class of bug — but it's a real design
discussion, not a given.

**B. Automated drift check — implemented.**
`canary_production_construction_contract` independently evaluates the two
paths' amount contracts: BUY must be a cent-denominated maker-side USDC
budget, and SELL remains maker-side shares. It covers the former
independent-rounding failure and rejects non-cent BUY budgets before an SDK
builder is reached.

**C. Track what a canary covers — implemented for new runs.** Each new
`spec.json` stores the build Git commit and a construction-source fingerprint.
`PHASE_0_5_CANARY_REPORT.md` now requires both values for a new live BUY
canary. Historical records deserialize explicitly as `unknown`, so they cannot
silently appear current.

B and C provide an immediate drift alarm and provenance boundary without
committing to A's larger refactor. A remains the stronger long-term option once
the BUY-construction rewrite has settled.

## What this means for "package Phase 0.5 into a one-click tool"

Building a friendlier wrapper around `canary_probe` (streamlining the
env-var setup / dry-run / confirm-submit / confirm-duplicate sequence,
which is legitimately tedious today — see `canary_probe.rs`'s and
`canary_run.rs`'s current env-var-only, no-TTY-prompt design) is good,
worthwhile tooling work and doesn't touch signing/submission logic at all. It
must continue to preserve the independent construction contract, provenance
record, and explicit human submission gate implemented above.
