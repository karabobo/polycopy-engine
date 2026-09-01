# Server-first deployment boundary

## Purpose and scope

The designated execution server is the only environment allowed to contact
Polymarket for this project. Developer machines are for source editing and
offline tests only. This is a deployment boundary, not authorization to trade:
until Phase 0.5 and Phase 7 pass, the server may run only authenticated
read-only GHOST verification or an explicitly operator-confirmed Phase 0.5
canary.

There is no production copy-execution service yet. In particular,
`CopyExecution::submit_exact_envelope` has no non-test implementation. Do not
create, enable, or start a service that claims to perform automatic copying.

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
first source build. On Ubuntu 24.04 the bootstrap script uses the distribution
`rustup` launcher and stores the actual toolchain and Cargo cache under the
project root rather than root's home directory:

```sh
POLYCOPY_DEPLOY_HOST=[server SSH alias] deploy/bootstrap-rust-toolchain.sh
```

It installs no service and does not receive source code, credentials, database
files, or runtime data. The toolchain setup is separate from a release so a
failed compiler download cannot change `current`.

From a clean local Git worktree, set the SSH host alias only in the invoking
shell and run:

```sh
POLYCOPY_DEPLOY_HOST=[server SSH alias] deploy/remote-release.sh
```

The release script refuses a dirty worktree, exports exactly one committed
source tree with `git archive`, creates a new immutable remote directory, and
builds all features remotely. It never copies `.git`, credentials, databases,
runtime evidence, or artifacts. It does not start, restart, enable, or install
a service. A failed build remains non-current for diagnosis; do not delete it
until the cause has been recorded.

The `current` symlink is changed only after the remote build succeeds. Since
this repository has no live execution unit, changing that symlink cannot send
an order.

## Read-only GHOST unit

After one remote release has built successfully, install the intentionally
disabled unit:

```sh
POLYCOPY_DEPLOY_HOST=[server SSH alias] deploy/install-ghost-unit.sh
```

This only installs `polycopy-engine-ghost.service`, reloads systemd metadata,
and creates root-only empty configuration/state directories. It refuses to
overwrite a pre-existing unit. It never creates `/etc/polycopy-engine/ghost.env`
and never starts or enables the unit.

Before a manual GHOST run, the account owner creates the root-only environment
file directly on the server (mode `0600`) and enters a **new, non-exposed**
signing credential there. It must contain a newly captured timestamped manual
balance snapshot and the normal `POLYCOPY_GHOST_*` values. Do not paste any
private key, L2 secret, passphrase, signed order, or full wallet address into
chat, Git, shell history, or deployment logs.

Run GHOST manually only after the snapshot is ready:

```sh
systemctl start polycopy-engine-ghost.service
systemctl status --no-pager polycopy-engine-ghost.service
journalctl -u polycopy-engine-ghost.service --since '-10 min' --no-pager
```

The unit has no `[Install]` section and therefore cannot be enabled for
automatic execution. A passing GHOST check is evidence only of a matching
read-only snapshot; it never authorizes trading.

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
2. The actual live order writer, current market/allowance checks, and its
   failure-injection tests are implemented and reviewed separately.
3. Phase 7 historical replay, 72-hour GHOST verification, and bounded
   single-leader seven-day live progression complete with no unresolved drift.

Any query failure, uncertain submission, balance mismatch, or unresolved
reconciliation case freezes the affected account/token. It is never repaired
by restarting a service or by automatic resubmission.
