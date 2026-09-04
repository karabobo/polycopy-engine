# Bounded live-progression runbook

This is an operational safety contract, not permission to start trading. The
copy unit must remain static and stopped until every prerequisite below is
independently reviewed.

## Preconditions

1. Phase 0.5 is marked passed only after a real matched canary proves that
   `GET /data/trades.taker_order_id` equals the persisted precomputed ID.
2. The current commit is reviewed, pushed, built remotely, and backed by a
   verified SQLite `.backup` copy.
3. A 12-hour GHOST timer window has produced a clean drift report: no bad
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

## One-time test database setup

`copy_setup` is the only supported initializer for a fresh bounded test-copy
database. It first performs derive-only CLOB authentication using the protected
server credential, then records one Safe account, one enabled leader, its
policy, and the one-lane schedule in a single database transaction. It never
creates credentials or prepares, signs, submits, cancels, or changes an order.
It refuses to run if any copy account, leader, schedule, or intent already
exists, so it cannot overwrite an initialized ledger. For the first controlled
test, the initializer accepts a maximum order notional of no more than 1 USDC.

## Evidence sequence

1. Install units only with `deploy/install-production-units.sh`; confirm the
   copy unit is `static/inactive` and the GHOST timer is `disabled/inactive`.
2. Configure and enable only the read-only GHOST timer. After 12 hours, run
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

## Persistent 5-USDC/24h Test Mode

`copy_persistent` is the separate always-on runner for the current controlled
production test. Do not emulate persistence by restarting `copy_run`.

The public persistent file is `/etc/polycopy-engine/persistent-public.env`.
For the current test wallet it must keep the same one account, one enabled
leader, one-USDC order cap, and five-USDC rolling 24-hour budget:

```text
POLYCOPY_ENGINE_EXECUTE=yes
POLYCOPY_PERSISTENT_EXECUTE=yes
POLYCOPY_DB_PATH=/var/lib/polycopy-engine/polycopy.sqlite
POLYCOPY_PERSISTENT_ACCOUNT_ID=1
POLYCOPY_PERSISTENT_ALLOWED_LEADER_IDS=1
POLYCOPY_PERSISTENT_MAX_ORDER_NOTIONAL=1
POLYCOPY_PERSISTENT_ROLLING_BUDGET_USDC=5
POLYCOPY_PERSISTENT_BUDGET_WINDOW_SECONDS=86400
POLYCOPY_PERSISTENT_TICK_SECONDS=1
POLYCOPY_PERSISTENT_BACKFILL_EVERY_SECONDS=60
POLYCOPY_CLOB_SIGNATURE_TYPE=gnosis_safe
POLYCOPY_CLOB_FUNDER=[configured funder address]
```

Initialize the database-owned persistent config once, while the service is
stopped:

```sh
/opt/polycopy-engine/current/target/release/persistent_control init-config
```

The runner startup then requires the runtime file to exactly match that
database row. It refuses to start on config drift, an open account fuse, an
unresolved reconciliation case, or any `submitting`/`uncertain` attempt. Before
each order submit, it atomically reserves the persisted `planned_notional_usdc`
and marks the attempt `submitting`; definitive rejections and uncertain
submissions still count against the rolling budget until their 24-hour
timestamp ages out.

Operator controls:

```sh
/opt/polycopy-engine/current/target/release/persistent_control status
/opt/polycopy-engine/current/target/release/persistent_control pause "operator reason"
/opt/polycopy-engine/current/target/release/persistent_control resume "operator reason"
```

`resume` is manual and refuses while account-wide reconciliation or uncertain
submission state remains. The static service uses exit code `20` for lock
collision, `21` for fuse-open safe stop, `22` for config refusal, `23` for
unresolved recovery state, and `24` for malformed budget state/budget refusal;
systemd does not restart on those codes.

### Proven pre-submission allowance failure

When an intent fails before any `order_attempt` exists, do not resume the
fuse directly. Run the static `polycopy-engine-persistent-reconcile.service`.
It receives the same service-private credential, performs fresh strict
collateral and allowance reads, requires usable collateral of at least the
configured per-order cap, and closes exactly one matching pre-submission case.
It cannot construct, submit, retry, or recover an order and does not resume
the fuse. The stale signal is recorded as rejected and is never replayed.

Only after that unit succeeds may the operator use the explicit `resume`
control and start the persistent service.
