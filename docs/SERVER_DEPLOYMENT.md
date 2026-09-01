# Server-first deployment boundary

## Purpose and scope

The designated execution server is the only environment allowed to contact
Polymarket for this project. Developer machines are for source editing and
offline tests only. This is a deployment boundary, not authorization to trade:
until Phase 0.5 and Phase 7 pass, the server may run only authenticated
read-only GHOST verification or an explicitly operator-confirmed Phase 0.5
canary.

The repository now contains a bounded copy-execution binary, including a real
`submit_exact_envelope` adapter. It is intentionally **not deployed or
enabled** by this document: the binary refuses to write unless all runtime
gates are present, and the systemd unit is static. Do not create an automatic
timer for copy execution.

## Layout and ownership

All paths below are on the execution server's local block storage:

```text
/opt/polycopy-engine/
  releases/<immutable-git-commit>/   source archive and verified binaries
  current -> releases/<commit>       atomically selected release
/etc/polycopy-engine/                root-only configuration; never in Git
/var/lib/polycopy-engine/             future SQLite database and lock file
/var/log/polycopy-engine/             future non-secret operational logs
```

The database must never be stored on NFS, SMB, a shared folder, or another
host. Only one copy-engine process may use it; the database-side `EngineLock`
is an additional guard, not a substitute for one systemd owner.

## Release procedure

The execution server needs an isolated Rust stable toolchain once, before its
first source build. On Ubuntu 24.04, the distribution owns `/usr/bin/rustup`
and its proxy launchers; the script verifies that the resolved Cargo binary is
under the project-owned toolchain and keeps its build cache there rather than
under root's home directory. It also forces Cargo and every build script to use
that same resolved `rustc`, rather than an unrelated compiler that may appear
earlier in root's `PATH`:

```sh
deploy/bootstrap-rust-toolchain.sh
```

It installs no service and does not receive source code, credentials, database
files, or runtime data. The toolchain setup is separate from a release so a
failed compiler download cannot change `current`.

From a clean local Git worktree, set the SSH host alias only in the invoking
shell and run:

```sh
deploy/remote-release.sh
```

The release script refuses a dirty worktree, exports exactly one committed
source tree with `git archive`, creates a new immutable remote directory, and
builds all features remotely. It never copies `.git`, credentials, databases,
runtime evidence, or artifacts. It does not start, restart, enable, or install
a service. A failed build remains non-current for diagnosis; do not delete it
until the cause has been recorded.

The `current` symlink is changed only after the remote build succeeds. It does
not start, restart, enable, or reload a unit, so changing it cannot send an
order.

## Read-only GHOST unit

After one remote release has built successfully, install the intentionally
disabled unit:

```sh
deploy/install-ghost-unit.sh
```

This only installs `polycopy-engine-ghost.service`, reloads systemd metadata,
and creates root-only empty configuration/state directories. It refuses to
overwrite a pre-existing unit. It never creates a configuration or credential
file and never starts or enables the unit.

Before a manual GHOST run, the account owner creates two root-only files
directly on the server (each mode `0600`). Do not paste any private key, L2
secret, passphrase, signed order, or full wallet address into chat, Git, shell
history, or deployment logs.

`/etc/polycopy-engine/ghost-public.env` contains only non-secret runtime
settings: signature type, optional funder address, timestamp, collateral, and
expected token balances. It is loaded with `EnvironmentFile`.

`/etc/polycopy-engine/credentials/ghost-secrets.env` contains only the signing
key plus an optional complete set of existing L2 fields. It is supplied via
systemd `LoadCredential`, so the process reads the service-private credential
file through `CREDENTIALS_DIRECTORY`; none of its values become systemd
environment variables. The GHOST process rejects snapshot or other runtime
values in this credential file. Leaving the three L2 values out keeps the
derive-only existing-credential path.

Run GHOST manually only after the snapshot is ready:

```sh
systemctl start polycopy-engine-ghost.service
systemctl status --no-pager polycopy-engine-ghost.service
journalctl -u polycopy-engine-ghost.service --since '-10 min' --no-pager
```

The unit has no `[Install]` section and therefore its systemd state is
`static`, not enabled. A passing GHOST check is evidence only of a matching
read-only snapshot; it never authorizes trading.

## Disabled execution and GHOST operations

After a release, `deploy/install-production-units.sh` may install the static
copy unit and the disabled GHOST timer. It refuses to replace an existing unit,
never creates either configuration file, never creates credentials, and never
starts or enables anything. See
[`LIVE_PROGRESSION_RUNBOOK.md`](LIVE_PROGRESSION_RUNBOOK.md) for the only
permitted order of operations.

The copy unit reads public, non-secret bounds from
`/etc/polycopy-engine/copy-public.env` and signing/L2 material only through
`LoadCredential=polycopy-copy-secrets`. In its bounded-progression mode it
requires exactly one enabled leader, exactly one attempted order per process,
and a per-order maximum no greater than 5 USDC. Any schedule mismatch, open
reconciliation case, Activity WS failure, or REST backfill failure stops it.

All deployment scripts are hard-locked to the SSH alias
`aliyun-8-220-180-39` and use key-only, strict-host-key SSH options. Supplying
a different `POLYCOPY_DEPLOY_HOST` is rejected.

## Evidence, backups, and production promotion

Keep raw canary artifacts and the future SQLite database only on the server,
with root-only permissions. Before each migration or release promotion, make a
timestamped, SQLite-consistent backup using SQLite's `.backup` command; verify
the copied database opens and run an integrity check before relying on it.
Do not copy a live WAL database with a plain file copy.

Production promotion remains blocked until all of the following are recorded
and independently reviewed:

1. Phase 0.5 proves the exact `taker_order_id` recovery lookup after a truly
   lost response, not merely the submission response's order ID.
2. The live order writer, strict collateral preflight, market/allowance checks,
   and its failure-injection tests are implemented and independently reviewed.
3. Phase 7 historical replay, 72-hour GHOST verification, and bounded
   single-leader seven-day live progression complete with no unresolved drift.

Any query failure, uncertain submission, balance mismatch, or unresolved
reconciliation case freezes the affected account/token. It is never repaired
by restarting a service or by automatic resubmission.
