# Phase 0 status

Status: **in progress; not approved for automated trading.**

This document records evidence for Phase 0 of
[`COPY_ENGINE_BLUEPRINT.md`](COPY_ENGINE_BLUEPRINT.md). A build, a passing
lock test, or a healthy connection is not permission to place an order.

## Completed local safeguards

- The project has an explicit Rust baseline and a versioned lockfile.
- `EngineLock` acquires a non-blocking, exclusive advisory lock next to the
  configured copy database. A second process fails at startup instead of
  sharing fixed-lane ownership of an account/token.
- The cross-process regression test proves both rejection while the first owner
  holds the guard and release after that guard is dropped.
- A pure receipt boundary keeps requested, accepted, filled, and remaining
  quantities distinct. Its FAK zero-fill and partial-fill regressions prevent
  a requested quantity from becoming a phantom filled quantity.
- The strict read-only Intl CLOB boundary uses the official SDK's single-token
  conditional-balance query. A query failure remains typed failure, never zero
  position; it has no order-writing method.
- The GHOST verifier compares the same strict CLOB reads with a manual snapshot
  of collateral and configured outcome-token balances. It accepts only
  non-negative, non-duplicated snapshot entries, applies exact comparisons, and
  marks any mismatch or query error unclean. It has no order, credential,
  persistence, or retry surface.
- The `ghost_verify` command reads existing L2 API credentials only from its
  local process environment and refuses incomplete configuration before it
  builds a client. It uses the supplied credentials to avoid the SDK's
  API-key-creation path, prints only row status, and exits non-zero for an
  unclean verification.
- The complete optional SDK feature compiles and its safety regressions pass
  locally with `cargo test --all-features` and `cargo build --release --all-features`.
- Git excludes credentials, local database files, raw wallet evidence, and
  signed-order artifacts. Redacted Markdown reports remain reviewable.

## Required before Phase 0 can close

1. Complete a GHOST-only, read-only CLOB check with authenticated collateral
   and strict token balances matched against a timestamped manual wallet
   snapshot. Record the redacted result using
   [`PHASE_0_GHOST_REPORT.md`](PHASE_0_GHOST_REPORT.md).

## Hard stop

No order-submission client exists in this project. The Phase 0.5 canary report
must be complete before any automatic retry or live-order path is introduced.
