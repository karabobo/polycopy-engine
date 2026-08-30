# polycopy-engine

`polycopy-engine` is a standalone Polymarket copy-execution engine. It is not a
DRADIS fork, not PolyHermes, and not part of the `pm-robot` wallet-research
pipeline. DRADIS may be read as a reference for selected venue concepts, but its
source and history must not be copied into this repository without an explicit
decision.

## Source of Truth

- The implementation specification is `docs/COPY_ENGINE_BLUEPRINT.md`.
- This repository starts as a planning project. No automated trading code exists
  yet.
- The first release scope is one account, multiple leaders, leader-specific
  policies, and Polymarket Intl CLOB only.

## Financial-Correctness Gates

Do not enable real automated orders until every gate in the blueprint has passed.
In particular:

- Phase 0 repairs the upstream `Fill.filled` defect and establishes a single
  active engine process.
- Phase 0.5 must prove the real CLOB behavior of a repeated identical signed
  envelope using a deliberately tiny canary order.
- An order submission that may have crossed the network boundary is uncertain,
  not failed. Query it first; without proven lookup and idempotency behavior,
  move the account/token to `needs_reconcile` rather than retrying.
- Only confirmed `filled_qty` changes virtual lots. Receipt accounting,
  reservation release, and intent finalization must be idempotent and atomic.
- A token-query error is never a zero position. It blocks further work for that
  account/token until reconciliation.

## Architecture Boundaries

- Activity WS and Activity REST backfill create canonical leader events. On-chain
  data is confirmation/audit only and must not create a second executable event.
- The durable database ledger is the input to execution. In-memory channels are
  wakeups only.
- Execution serializes by `(account_id, token_id)`, not leader. Leader identity
  is used for attribution and policy.
- Persist shard algorithm/version/lane count. Do not change schedule while
  non-terminal work exists.
- Use a dedicated strict copy-execution API. Do not silently fall back to a
  generic position method that can omit unregistered tokens.
- Persist every order decision's quantity, price, tick rule, TIF, and deadline
  before preparing a signed order. Do not execute stale events.

## Repository Discipline

- Keep secrets, private keys, environment files, databases, and order artifacts
  out of Git.
- Do not vendor DRADIS or carry its commit history into this repository.
- Make schema changes through versioned migrations; do not edit runtime database
  files by hand.
- Add failure-injection and replay tests before implementing money-moving paths.
- A green build or healthy WebSocket is not proof of safe execution. Verify the
  behavior specified in Phase 7 before stating that a phase is complete.
