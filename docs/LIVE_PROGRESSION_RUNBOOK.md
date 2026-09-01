# Bounded live-progression runbook

This is an operational safety contract, not permission to start trading. The
copy unit must remain static and stopped until every prerequisite below is
independently reviewed.

## Preconditions

1. Phase 0.5 is marked passed only after a real matched canary proves that
   `GET /data/trades.taker_order_id` equals the persisted precomputed ID.
2. The current commit is reviewed, pushed, built remotely, and backed by a
   verified SQLite `.backup` copy.
3. A 72-hour GHOST timer window has produced a clean drift report: no bad
   records, no gaps over fifteen minutes, and no unresolved mismatch.
4. The signing key is a new server-local credential, not a key ever pasted in
   chat. The credential file is root-owned mode `0600`; it contains only
   `POLYCOPY_CLOB_PRIVATE_KEY` and optional complete L2 fields.

## Default-disabled files

`copy-public.env` is public service configuration, never a secret. During the
first seven-day progression it must contain all of the following values:

```text
POLYCOPY_ENGINE_EXECUTE=yes
POLYCOPY_DB_PATH=/var/lib/polycopy-engine/polycopy.sqlite
POLYCOPY_ACCOUNT_ID=1
POLYCOPY_ALLOWED_LEADER_IDS=[one enabled leader id]
POLYCOPY_MAX_ATTEMPTS_PER_RUN=1
POLYCOPY_MAX_ORDER_NOTIONAL=[positive decimal no greater than 5]
POLYCOPY_MAX_RUNTIME_SECONDS=[positive duration]
POLYCOPY_TICK_SECONDS=5
POLYCOPY_BACKFILL_EVERY_SECONDS=60
POLYCOPY_CLOB_SIGNATURE_TYPE=eoa|proxy|gnosis_safe|poly1271
POLYCOPY_CLOB_FUNDER=[only when the selected wallet type requires it]
```

The service rejects more than one enabled leader, more than one attempt, a
policy snapshot above the 5-USDC ceiling, an incompatible shard schedule, an
open reconciliation case, or a stopped Activity/backfill supervisor.

## Evidence sequence

1. Install units only with `deploy/install-production-units.sh`; confirm the
   copy unit is `static/inactive` and the GHOST timer is `disabled/inactive`.
2. Configure and enable only the read-only GHOST timer. After 72 hours, run
   `ghost_drift_report` against the corresponding journal window and retain its
   redacted result.
3. After Phase 0.5 and GHOST pass, start the static copy unit manually for one
   reviewed intent. Check its journal, Control Tower trace, lot, reservation,
   strict balance and reconciliation state before any later start.
4. Repeat this bounded, one-order operation with the same one leader for seven
   days. Any uncertainty or drift freezes that account/token; resolve it before
   another run.

Never enable the copy unit, attach a timer to it, or increase the 5-USDC
ceiling under this runbook. Those changes require a new architecture and risk
review.
