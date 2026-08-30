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

Phase 0 is in progress. The first implemented primitive is a cross-process
database ownership lock: a second engine instance fails instead of sharing a
database and sending concurrently for the same account/token.

Run the local checks with:

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release
```

The project contains no CLOB write client and no live-order path. See
[`docs/PHASE_0_STATUS.md`](docs/PHASE_0_STATUS.md) for the remaining Phase 0
gates and [`docs/PHASE_0_5_CANARY_REPORT.md`](docs/PHASE_0_5_CANARY_REPORT.md)
for the required real-order safety evidence template.

The optional `intl_clob` feature enables a **read-only**, strict per-outcome
token balance adapter. It has no order-writing API; see
[`docs/INTL_CLOB_SDK_BOUNDARY.md`](docs/INTL_CLOB_SDK_BOUNDARY.md).
