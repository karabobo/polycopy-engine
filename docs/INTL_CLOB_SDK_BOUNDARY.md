# Polymarket Intl CLOB SDK boundary

## Chosen dependency

Phase 0 uses the official Polymarket Rust SDK
[`polymarket_client_sdk_v2` version `0.7.0`](https://crates.io/crates/polymarket_client_sdk_v2/0.7.0),
with its `clob` feature only. The project pins the exact release so the adapter
does not silently change when a compatible semver range resolves differently.

The SDK's documented CLOB module exposes authenticated balance/allowance,
single-order, and filtered-order-list operations. The official order-management
docs confirm lookup by order ID and open-order filters by order ID, condition ID,
or outcome token. They do **not** establish lookup by a signed-envelope hash or
salt; that remains a Phase 0.5 canary question.

Sources:

- [Official Rust SDK repository](https://github.com/Polymarket/rs-clob-client-v2)
- [SDK 0.7.0 CLOB module documentation](https://docs.rs/polymarket_client_sdk_v2/0.7.0/polymarket_client_sdk_v2/clob/)
- [Official order-management documentation](https://docs.polymarket.com/trading/manage-orders)

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

The `ghost_verify` command supplies the adapter only with pre-existing L2 API
credentials. This is intentional: the SDK's default credential path can create
an API key before falling back to derivation. Phase 0 may authenticate using
existing credentials and make balance reads, but it must not create keys,
update balance allowances, or place orders.

This surface intentionally has no API for order construction, signing,
submission, cancellation, retries, envelope lookup, or credential persistence.
No test contacts Polymarket; the tests use a failing in-memory reader to lock
the error-versus-zero contract.

## Remaining gate

Before a write-capable adapter exists, Phase 0.5 must prove deterministic
post-boundary lookup and byte-identical duplicate-envelope behavior with an
explicitly authorized tiny canary order. Until then, unknown submissions must
only query and then enter `needs_reconcile`.
