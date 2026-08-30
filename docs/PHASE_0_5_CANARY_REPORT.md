# Phase 0.5 CLOB submission-safety canary

Status: **template only — no canary has been authorized or run.**

This report is required by
[`COPY_ENGINE_BLUEPRINT.md`](COPY_ENGINE_BLUEPRINT.md) before any automatic
resubmission of a `submitting` or `uncertain` attempt is enabled.

Never commit private keys, API credentials, full signed envelopes, raw request
bodies, or unredacted wallet addresses here. Put any raw operational material
under the ignored `canary-artifacts/` directory and retain it only according to
the operational security policy.

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
