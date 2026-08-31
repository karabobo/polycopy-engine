# polycopy-engine

`polycopy-engine` is the standalone implementation project for a Polymarket copy engine.
It begins with the financial-correctness blueprint in
[`docs/COPY_ENGINE_BLUEPRINT.md`](docs/COPY_ENGINE_BLUEPRINT.md).

The project uses DRADIS as a read-only reference for selected venue concepts. It
does not vendor DRADIS source code or preserve DRADIS commit history.

The exact audit reference and this independent-implementation decision are
recorded in [`docs/DRADIS_REFERENCE_BASELINE.md`](docs/DRADIS_REFERENCE_BASELINE.md).

No automated trading code is included yet. The implementation starts only after
the Phase 0 and Phase 0.5 gates in the blueprint are completed. The only
order-writing code that exists is the narrowly scoped Phase 0.5 canary probe
below, which is a dry run unless the operator explicitly opts in.

## Current development status

Phase 0 is closed (2026-08-30); Phase 0.5 is in progress (one real canary
order placed and observed on 2026-08-31; not every gate box is checked yet —
see [`docs/PHASE_0_5_CANARY_REPORT.md`](docs/PHASE_0_5_CANARY_REPORT.md)). The
first implemented primitive is a cross-process database ownership lock: a
second engine instance fails instead of sharing a database and sending
concurrently for the same account/token.

Phase 1 groundwork (the durable account/leader schema from blueprint section
6) has also started, ahead of Phase 0.5's gate formally closing, at the
account owner's explicit direction while Phase 0.5's remaining live tests
wait on a Polymarket-side CLOB outage to clear. So far this covers only
connection setup and the `accounts`/`leader_config`/`leader_wallet_aliases`
tables (see "Database (Phase 1)" below); the event ledger, intent planner,
and executor are not built.

Run the complete local checks with:

```sh
cargo test --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo build --release --all-features --locked
```

The project's only live-order path is the `canary_probe` command below,
gated behind an explicit operator-set environment variable and used solely to
produce the evidence required by the Phase 0.5 gate. See
[`docs/PHASE_0_STATUS.md`](docs/PHASE_0_STATUS.md) for Phase 0's closed
status and [`docs/PHASE_0_5_CANARY_REPORT.md`](docs/PHASE_0_5_CANARY_REPORT.md)
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

## Phase 0.5 canary probe (dry run by default)

`canary_probe` builds and signs one FAK order and, by default, stops there —
it never calls the venue's order-writing endpoint unless the operator
explicitly sets `POLYCOPY_CANARY_CONFIRM_SUBMIT=yes` in their own shell. No
other mechanism, and no automated tool, can set that variable. A dry run still
authenticates, builds, and signs the order twice against the live venue, which
is enough to confirm the whole pipeline works and that two independent
signatures over the same built order are byte-identical (they are: EIP-712
signing is deterministic for a fixed key and message).

It reuses the same `POLYCOPY_CLOB_*` credential variables as `ghost_verify`,
plus:

```text
POLYCOPY_CANARY_LABEL=[short stable label, used under canary-artifacts/<label>/]
POLYCOPY_CANARY_TOKEN_ID=[decimal outcome token ID]
POLYCOPY_CANARY_SIDE=BUY|SELL
POLYCOPY_CANARY_PRICE=[decimal, strictly between 0 and 1]
POLYCOPY_CANARY_SIZE=[decimal, positive]
POLYCOPY_CANARY_CONFIRM_SUBMIT=yes      # only this exact value submits the first order
POLYCOPY_CANARY_CONFIRM_DUPLICATE=yes   # only this exact value also submits a second, independently-signed copy
```

Choose `POLYCOPY_CANARY_PRICE` away from the current market and
`POLYCOPY_CANARY_SIZE` at the venue's minimum: the order is always
Fill-And-Kill (this project has no cancel-order client, so a resting GTC
canary could be filled later with nothing able to close it), and a price far
from the market keeps an accidental match astronomically unlikely.

Run `cargo run --locked --features intl_clob --bin canary_probe`. Every
persisted record lands under the gitignored `canary-artifacts/<label>/`
directory (spec, submission, and lookup records only — never a private key,
full envelope, or raw response body). Record the redacted result using
[`docs/PHASE_0_5_CANARY_REPORT.md`](docs/PHASE_0_5_CANARY_REPORT.md).

## Database (Phase 1)

The optional `db` feature (`polycopy_engine::copytrading`) opens a pooled
SQLite connection and applies every migration under `migrations/`. Every
connection is configured exactly as blueprint section 6 requires: foreign
keys on, WAL journal mode, a 5 second busy timeout, and `synchronous=FULL`.
Migrations run through sqlx's built-in migrator, which keeps its own
checksum-protected ledger (an `_sqlx_migrations` table) and refuses to run if
an already-applied migration's file content no longer matches what was
recorded — the same protection the blueprint's own
`copy_schema_migrations(version, name, checksum, applied_at)` ledger
describes, provided here by sqlx's existing implementation rather than a
hand-rolled equivalent.

```rust
let pool = polycopy_engine::copytrading::open_and_migrate("./copy.sqlite").await?;
```

Schema so far covers:

- `accounts`, `leader_config`, `leader_wallet_aliases` — one copied account
  and its enabled leader address aliases. Addresses are stored lowercase and
  a partial unique index enforces that the same address can never be enabled
  under two leaders at once (or twice under one). This does **not** limit how
  many leaders one account follows — v1's whole scope is "one account,
  multiple leaders" (blueprint section 1), and multiple simultaneously
  enabled leaders is a directly tested case.
- `leader_events`, `leader_event_observations` — the canonical, immutable
  leader-trade ledger and its raw source observations. `canonical_event_key`
  (`'activity:' || activity_trade_id`) is the sole dedup key: replaying the
  same Activity WS message or re-running backfill over an overlapping window
  is safe by construction (`INSERT OR IGNORE`), never creates a second event.
  An on-chain observation may stay unlinked (`leader_event_id = NULL`) rather
  than being fuzzy-matched onto a canonical event it can't be explicitly
  related to.
- `copy_intents`, `order_attempts`, `position_lots`, `reconciliation_cases`,
  `execution_schedule` — the planning/execution state machine, virtual lots,
  and reconciliation history. `copy_intents` is unique on `(event_id,
  account_id)` (replaying an event creates no second intent);
  `order_attempts` is unique on `(intent_id, attempt_number)`; `position_lots`
  is keyed by `(account_id, leader_id, token_id)` so two leaders holding the
  same token keep distinct lots. `order_attempts.accounted_filled_qty` exists
  because Phase 0.5's canary confirmed a BUY's requested `size` behaves as a
  notional cap, not a literal share count — filled quantity must always be
  read from the venue's actual matched-quantity field, and that same finding
  (order-by-id lookup 404s immediately after a match, with no working
  field-based fallback yet) is why `order_attempts.status` has an `uncertain`
  state that can only be queried and parked, never auto-resubmitted.
  All decimal quantities are stored as exact text, never `REAL`, to avoid
  reintroducing the rounding-error class [`OrderReceipt`](src/venue/receipt.rs)
  exists to prevent.

The database file must live on local block storage, not NFS/SMB — this is a
single-host, single-writer deployment; see [`EngineLock`](src/engine_lock.rs)
for the process-level enforcement of "single writer". This covers every
table blueprint section 6 lists, but only the schema: the ingestion pipeline,
intent planner, fixed-lane executor, and the startup check that refuses a
lane-count change while non-terminal intents exist (Phases 2–5) are not
built yet.
