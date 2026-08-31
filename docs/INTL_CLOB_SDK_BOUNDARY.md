# Polymarket Intl CLOB SDK boundary

## Chosen dependency

Phase 0 uses the official Polymarket Rust SDK
[`polymarket_client_sdk_v2` version `0.7.0`](https://crates.io/crates/polymarket_client_sdk_v2/0.7.0),
with its `clob` feature only. The project pins the exact release so the adapter
does not silently change when a compatible semver range resolves differently.

The SDK's documented CLOB module exposes authenticated balance/allowance,
single-order, filtered-order-list, and authenticated trade-history operations.
The official order-management docs confirm lookup by order ID and open-order
filters by order ID, condition ID, or outcome token. Trade history exposes a
`taker_order_id`, but no official source yet proves that it is equal to a
locally computed signed-envelope identifier for the FAK path; that remains a
Phase 0.5 canary question.

Sources:

- [Official Rust SDK repository](https://github.com/Polymarket/rs-clob-client-v2)
- [SDK 0.7.0 CLOB module documentation](https://docs.rs/polymarket_client_sdk_v2/0.7.0/polymarket_client_sdk_v2/clob/)
- [Official order-management documentation](https://docs.polymarket.com/trading/manage-orders)
- [Official trading quickstart](https://docs.polymarket.com/trading/quickstart)

## Phase 0 adapter surface

`IntlClobReadAdapter` accepts an already-authenticated SDK client and calls the
SDK's balance-and-allowance endpoint for collateral or exactly one conditional
outcome token. It returns the venue balance or a typed error. It never converts
an error into `0`, and it cannot list a mutable active-token set as a substitute
for a strict query.

`GhostVerifier` is a pure orchestration layer over those strict reads. It
compares each response with a timestamped manual snapshot using exact decimal
equality; a mismatch or a query failure makes the report unclean. It continues
only to collect a complete read-only diagnostic report, never to retry or change
venue state. The redacted evidence template is
[`PHASE_0_GHOST_REPORT.md`](PHASE_0_GHOST_REPORT.md).

The `ghost_verify` command either supplies pre-existing L2 API credentials or
uses the SDK's explicit derive-only endpoint to recover an existing credential
from the signing key. It never uses the SDK's create-or-derive path, which can
create an API key before falling back to derivation. Phase 0 may authenticate
using existing credentials and make balance reads, but it must not create keys,
update balance allowances, or place orders.

`IntlClobReadAdapter` also exposes `GET /data/trades` through a strict,
paginated authenticated read for one outcome token and a bounded time window.
It returns raw `AccountTrade` observations including `taker_order_id`, role,
side, status, price, size, and match time. A query or pagination failure remains
an error and cannot be treated as empty history. The adapter does not choose an
order: the reconciliation layer accepts only a precomputed, stored
`taker_order_id` that exactly equals the returned field, and otherwise opens
`needs_reconcile`.

This surface intentionally has no API for order construction, signing,
submission, cancellation, retries, automatic envelope lookup, or credential
persistence. No test contacts Polymarket; the tests use an in-memory strict
reader to lock the error-versus-zero and fail-closed recovery contracts.

## Remaining gate

Before a write-capable adapter exists, Phase 0.5 must prove deterministic
post-boundary lookup and byte-identical duplicate-envelope behavior with an
explicitly authorized tiny canary order. Until then, unknown submissions must
only query and then enter `needs_reconcile`.
