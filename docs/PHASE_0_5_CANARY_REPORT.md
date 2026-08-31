# Phase 0.5 CLOB submission-safety canary

Status: **template only — no canary has been authorized or run.**

This report is required by
[`COPY_ENGINE_BLUEPRINT.md`](COPY_ENGINE_BLUEPRINT.md) before any automatic
resubmission of a `submitting` or `uncertain` attempt is enabled.

Never commit private keys, API credentials, full signed envelopes, raw request
bodies, or unredacted wallet addresses here. Put any raw operational material
under the ignored `canary-artifacts/` directory and retain it only according to
the operational security policy.

The command is `cargo run --locked --features intl_clob --bin canary_probe`;
see `README.md`'s "Phase 0.5 canary probe" section for its full environment
variable list. It is a dry run — it builds and signs but never submits —
unless `POLYCOPY_CANARY_CONFIRM_SUBMIT=yes` is set by the operator in their
own shell. Nothing else can set that variable. Set
`POLYCOPY_CANARY_CONFIRM_DUPLICATE=yes` in the same run to also submit a
second, independently-signed copy of the identical order for Result 2 below.

## Authorization and bounds

- Date and operator:
- Account identifier (redacted):
- Market / outcome token (redacted if required):
- Maximum notional and quantity:
- Time-in-force (must be explicit):
- Approval record:

## Result 1: lookup after interrupted response

- Deliberately interrupted client-side response method:
- Persisted envelope fields used for deterministic lookup:
- Lookup endpoint / filters:
- Result and returned order identifier:
- Is the lookup deterministic? `yes` / `no`

## Result 2: byte-identical duplicate submission

- Duplicate response classification: idempotent / deterministic rejection /
  independently executable / inconclusive
- Evidence that the signed payload bytes were identical (digest only):
- Venue order identifiers and cumulative fills:
- Safety conclusion:

## Result 3: receipt fields

- Stable request, accepted, filled, remaining, and venue-status fields:
- Cumulative-versus-delta fill semantics:
- Any contradictory or missing response fields:

## Gate decision

- [ ] Lookup is deterministic from persisted envelope data.
- [ ] Byte-identical duplicate behavior is safe for the intended retry policy.
- [ ] Receipt fields are sufficient for idempotent fill-delta accounting.
- [ ] The exact lookup and recovery rule has a regression test.

Decision: **not passed until every box is checked and independently reviewed.**
