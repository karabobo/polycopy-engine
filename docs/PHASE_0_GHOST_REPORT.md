# Phase 0 GHOST verification report

Status: **template only; it does not authorize trading.**

Use one report per authenticated, read-only verification attempt. Do not store
private keys, API credentials, full wallet addresses, signed requests, raw API
responses, or database files in this repository.

The command is `cargo run --locked --features intl_clob --bin ghost_verify`.
It refuses to start without existing L2 credentials, so it cannot take the
SDK's automatic API-key creation path. Provide its configuration only through
the local process environment; see the README for the exact variable names.

## Scope

- Account label: `[redacted stable label]`
- Timestamp in UTC: `[YYYY-MM-DDTHH:MM:SSZ]`
- SDK version and Git commit: `[exact values]`
- Snapshot source and capture time: `[manual wallet/UI source, UTC timestamp]`
- Verification mode: `GHOST / read-only`
- Order, cancellation, approval, deposit, and update calls issued: `none`

## Exact comparison

The current implementation applies no rounding or tolerance. The manually
recorded values must use the same units and exact decimal representation as the
strict CLOB balance query.

| Asset | Expected manual snapshot | Observed strict CLOB balance | Result |
| --- | ---: | ---: | --- |
| Collateral | `[decimal]` | `[decimal or query error]` | `[match/mismatch/query_failed]` |
| Outcome token `[redacted token label]` | `[decimal]` | `[decimal or query error]` | `[match/mismatch/query_failed]` |

Include every token relevant to the future account configuration. A query error
is not a zero balance, and any `mismatch` or `query_failed` result leaves the
account in GHOST / reconciliation-required status.

## Decision

- All rows matched exactly: `[yes/no]`
- Evidence is redacted and retained outside Git where required: `[yes/no]`
- Approved to close Phase 0: `[yes/no; requires the financial-correctness gate review]`
