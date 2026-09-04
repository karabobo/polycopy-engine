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

## Persistent production-test mode

`copy_persistent` is the separate always-on runner for the current controlled
production test. Do not emulate persistence by restarting `copy_run`.

The public persistent file is `/etc/polycopy-engine/persistent-public.env`.
The file defines the complete enabled leader set, a one-USDC per-order cap,
and the operator-selected rolling 24-hour budget. The budget is an explicit
positive decimal, persisted in the database, and is not silently capped by
the binary. Raising it is a risk decision; it does not bypass the separate
strict balance/allowance check before every order.

```text
POLYCOPY_ENGINE_EXECUTE=yes
POLYCOPY_PERSISTENT_EXECUTE=yes
POLYCOPY_DB_PATH=/var/lib/polycopy-engine/polycopy.sqlite
POLYCOPY_PERSISTENT_ACCOUNT_ID=1
POLYCOPY_PERSISTENT_ALLOWED_LEADER_IDS=1,2,3
POLYCOPY_PERSISTENT_MAX_ORDER_NOTIONAL=1
POLYCOPY_PERSISTENT_ROLLING_BUDGET_USDC=[positive decimal, for example 50]
POLYCOPY_PERSISTENT_BUDGET_WINDOW_SECONDS=86400
POLYCOPY_PERSISTENT_TICK_SECONDS=1
POLYCOPY_PERSISTENT_BACKFILL_EVERY_SECONDS=60
POLYCOPY_CLOB_SIGNATURE_TYPE=gnosis_safe
POLYCOPY_CLOB_FUNDER=[configured funder address]
```

`copy-public.env`, `copy_run`, and the removed `copy_setup` command are legacy
bounded-run surfaces. They are not the initializer or configuration source for
this persistent production-test workflow. Do not create a `copy-public.env` or
mix its old scattered `POLYCOPY_ACCOUNT_ID` / `POLYCOPY_MAX_*` values into the
persistent service.

## Database configuration

With `polycopy-engine-persistent.service` stopped, use
`copy_config_apply` for both a fresh database and later safe configuration
reconciliation. It replaces the former `copy_setup` and `copy_policy_setup`
tools. The command is derive-only with respect to CLOB authentication: it
derives the existing signer address but cannot prepare, sign, submit, cancel,
or modify a venue order. It takes the engine lock, so it refuses to run while
either execution runner owns the database.

Store a root-owned, non-secret JSON file outside Git, for example
`/etc/polycopy-engine/trading-config.json`:

```json
{
  "account": {
    "label": "controlled-test-account",
    "signature_type": "gnosis_safe",
    "funder_address": "0x<funded-safe-address>"
  },
  "leaders": [
    {
      "label": "leader-one",
      "enabled": true,
      "addresses": ["0x<leader-address>"],
      "policy": {
        "max_signal_age_seconds": 3,
        "decision_window_seconds": 3,
        "price_tolerance_bps": 0,
        "tick_size": "0.01",
        "min_price": "0.01",
        "max_price": "0.99",
        "max_order_notional": "1",
        "min_leader_trade_size": "0"
      }
    }
  ]
}
```

Run `copy_config_apply` only through a root systemd scope that supplies the
same protected credential file as the persistent service. Do not source the
private-key file into an interactive shell. The invocation needs the database
path, the JSON path, and an explicit maximum configuration ceiling:

```sh
systemd-run --wait --collect --pipe \
  --unit=polycopy-engine-config-apply \
  --property='EnvironmentFile=/etc/polycopy-engine/persistent-public.env' \
  --property='LoadCredential=polycopy-copy-secrets:/etc/polycopy-engine/credentials/copy-secrets.env' \
  --setenv=POLYCOPY_SETUP_CONFIG=/etc/polycopy-engine/trading-config.json \
  --setenv=POLYCOPY_CONFIG_MAX_NOTIONAL_CEILING=1 \
  /opt/polycopy-engine/current/target/release/copy_config_apply
```

The command prints `CONFIG_APPLIED:` followed by an auditable summary. It may
add or disable leader aliases and update leader policy. Once activity exists,
it refuses to change the account's `funder_address` or `signature_type`; add a
new account instead. It never deletes historical configuration or ledger rows.

After `copy_config_apply` succeeds, initialize the database-owned persistent
runtime configuration once, using exactly the values in
`persistent-public.env`:

```sh
/opt/polycopy-engine/current/target/release/persistent_control init-config
```

For a later leader-set or budget change, stop the persistent service, back up
the database, update both the Trading Config and `persistent-public.env`, then
run the code-owned reconfiguration command:

```sh
/opt/polycopy-engine/current/target/release/persistent_control reconfigure
```

`reconfigure` takes the engine lock, verifies that the enabled leader set
exactly matches the requested IDs, refuses unresolved reconciliation or
uncertain submissions, and refuses a lower budget when existing rolling
reservations already exceed it. It is the only supported way to alter the
database-owned persistent runtime configuration; never edit SQLite directly.

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
