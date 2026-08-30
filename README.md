# polycopy-engine

`polycopy-engine` is the standalone implementation project for a Polymarket copy engine.
It begins with the financial-correctness blueprint in
[`docs/COPY_ENGINE_BLUEPRINT.md`](docs/COPY_ENGINE_BLUEPRINT.md).

The project uses DRADIS as a read-only reference for selected venue concepts. It
does not vendor DRADIS source code or preserve DRADIS commit history.

The exact audit reference and this independent-implementation decision are
recorded in [`docs/DRADIS_REFERENCE_BASELINE.md`](docs/DRADIS_REFERENCE_BASELINE.md).

No automated trading code is included yet. The implementation starts only after
the Phase 0 and Phase 0.5 gates in the blueprint are completed.

## Current development status

Phase 0 is closed (2026-08-30); Phase 0.5 has not started. The first
implemented primitive is a cross-process database ownership lock: a second
engine instance fails instead of sharing a database and sending concurrently
for the same account/token.

Run the complete local checks with:

```sh
cargo test --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo build --release --all-features --locked
```

The project contains no CLOB write client and no live-order path. See
[`docs/PHASE_0_STATUS.md`](docs/PHASE_0_STATUS.md) for the remaining Phase 0
gates and [`docs/PHASE_0_5_CANARY_REPORT.md`](docs/PHASE_0_5_CANARY_REPORT.md)
for the required real-order safety evidence template.

The optional `intl_clob` feature enables a **read-only**, strict per-outcome
token balance adapter. It has no order-writing API; see
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
