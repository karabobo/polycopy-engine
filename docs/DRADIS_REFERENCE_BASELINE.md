# DRADIS reference baseline

## Decision

`polycopy-engine` is an independent implementation. It does **not** vendor,
fork, link to, or inherit Git history from DRADIS. It will implement its own
strict Polymarket Intl CLOB adapter using official venue documentation and SDK
evidence.

DRADIS is retained only as an auditable reference for venue behaviors and known
failure modes. No DRADIS source file may be copied into this repository.

## Fixed reference

| Field | Value |
| --- | --- |
| Repository | `https://github.com/mbordash/DRADIS.git` |
| Commit | `b2dfd04f87e2dbaf6eef833d9cfb2ebaaea81437` |
| Commit timestamp | 2026-08-29T10:28:24-04:00 |
| Selection date | 2026-08-30 |
| Purpose | Audit reference only; not a build or runtime dependency |

This revision is selected because it is the exact local reference in which the
blueprint's `Fill.filled` defect was inspected: immediate FAK zero- and
partial-fill handling computed `matched_shares` but returned the requested
quantity instead. The independent adapter must retain requested, accepted, and
filled quantities as separate values and prove the behavior with its own
tests before it can submit any order.

## Change control

- Never update this reference merely because upstream `main` moves.
- A reference upgrade requires a reviewed change to this file, the reason for
  the upgrade, and revalidation of every recorded venue behavior that affects
  financial correctness.
- A DRADIS reference may inform a test case, but official Polymarket
  documentation and observed canary evidence control the implementation.
