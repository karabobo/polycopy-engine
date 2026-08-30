# Phase 0 GHOST verification report

Status: **template only; it does not authorize trading.**

Use one report per authenticated, read-only verification attempt. Do not store
private keys, API credentials, full wallet addresses, signed requests, raw API
responses, or database files in this repository.

The command is `cargo run --locked --features intl_clob --bin ghost_verify`.
It derives an existing L2 credential from the supplied signing key when one is
not supplied, but never calls API-key creation. Provide its configuration only
through the local process environment; see the README for the exact variable
names.

## Scope

- Account label: `gnosis_safe-...255E` (Gnosis Safe, address redacted to last 4 hex chars)
- Timestamp in UTC: `2026-08-30T11:28:54Z`
- SDK version and Git commit: `polymarket_client_sdk_v2 v0.7.0`; polycopy-engine at the commit that introduces this report, which also corrects `CLOB_HOST` from the nonexistent `clob-v2.polymarket.com` to the real production host `clob.polymarket.com`
- Snapshot source and capture time: manual read from the Polymarket web UI, `2026-08-30T11:28:54Z`
- Verification mode: `GHOST / read-only`
- Order, cancellation, approval, deposit, and update calls issued: `none`

## Exact comparison

The current implementation applies no rounding or tolerance. The manually
recorded values must use the same units and exact decimal representation as the
strict CLOB balance query.

| Asset | Expected manual snapshot | Observed strict CLOB balance | Result |
| --- | ---: | ---: | --- |
| Collateral | `0` | `0` | `match` |
| Outcome token `"Will Gavin Newsom win the 2028 Democratic presidential nomination?" - YES` | `0` | `0` | `match` |

Include every token relevant to the future account configuration. A query error
is not a zero balance, and any `mismatch` or `query_failed` result leaves the
account in GHOST / reconciliation-required status.

## Decision

- All rows matched exactly: `yes`
- Evidence is redacted and retained outside Git where required: `yes` (full
  funder address and outcome token ID live only in the local, gitignored
  `.env`; this report carries only a redacted label and the human-readable
  market name)
- Approved to close Phase 0: `pending — requires your sign-off per the
  financial-correctness gate review; this report only records that the strict
  CLOB reads matched a manual snapshot exactly`

## Note on this verification run

The first two attempts at this run failed before reaching Polymarket at all,
with `Internal: error sending request` for `clob-v2.polymarket.com`. Root
cause: that hostname does not exist in public DNS (confirmed via an
independent Cloudflare DoH query returning no `A` record, only the zone's SOA)
— it was copied from the SDK's own example files, which appear to reference a
non-public/incorrect host. The real production CLOB REST host is
`clob.polymarket.com`, confirmed live via `GET /time`. This was a code defect,
not a network, proxy, or wallet-configuration problem; see the `CLOB_HOST` fix
in `src/ghost_run.rs`.
