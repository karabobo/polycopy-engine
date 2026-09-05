#!/usr/bin/env bash
# Install disabled operational units after a verified release. This script
# never writes credentials or public configuration and never starts/enables a
# service or timer.

set -Eeuo pipefail

readonly approved_host="aliyun-8-220-180-39"
host="${POLYCOPY_DEPLOY_HOST:-$approved_host}"
if [[ "$host" != "$approved_host" ]]; then
    echo "POLYCOPY_DEPLOY_HOST must remain $approved_host" >&2
    exit 2
fi
readonly remote_root="/opt/polycopy-engine"

ssh_args=(
    ssh
    -o BatchMode=yes
    -o PasswordAuthentication=no
    -o KbdInteractiveAuthentication=no
    -o StrictHostKeyChecking=yes
    -o ConnectTimeout=15
    "$host"
)

"${ssh_args[@]}" "set -eu
    test \"\$(id -u)\" -eq 0 || { echo 'remote installer must run as root' >&2; exit 1; }
    test -x '$remote_root/current/target/release/ghost_verify'
    test -x '$remote_root/current/target/release/ghost_drift_report'
    test -x '$remote_root/current/target/release/copy_run'
    test -x '$remote_root/current/target/release/copy_persistent'
    test -x '$remote_root/current/target/release/persistent_control'
    for unit in polycopy-engine-copy.service polycopy-engine-persistent.service polycopy-engine-persistent-reconcile.service polycopy-engine-ghost.timer; do
        if test -e /etc/systemd/system/\"\$unit\"; then
            echo \"refusing to replace existing \$unit\" >&2
            exit 17
        fi
    done
    # Fixed unprivileged service identity. Keep source configuration and
    # credentials root-only; systemd's LoadCredential supplies the private
    # per-service copy without making /etc readable to this account.
    if ! getent group polycopy-engine >/dev/null; then
        groupadd --system polycopy-engine
    fi
    if ! id -u polycopy-engine >/dev/null 2>&1; then
        useradd --system --gid polycopy-engine --home-dir /nonexistent \
            --shell /usr/sbin/nologin polycopy-engine
    fi
    install -d -m 0700 /etc/polycopy-engine /etc/polycopy-engine/credentials
    install -d -o polycopy-engine -g polycopy-engine -m 0750 \
        /var/lib/polycopy-engine /var/log/polycopy-engine
    install -m 0644 '$remote_root/current/deploy/systemd/polycopy-engine-copy.service' /etc/systemd/system/polycopy-engine-copy.service
    install -m 0644 '$remote_root/current/deploy/systemd/polycopy-engine-persistent.service' /etc/systemd/system/polycopy-engine-persistent.service
    install -m 0644 '$remote_root/current/deploy/systemd/polycopy-engine-persistent-reconcile.service' /etc/systemd/system/polycopy-engine-persistent-reconcile.service
    install -m 0644 '$remote_root/current/deploy/systemd/polycopy-engine-ghost.timer' /etc/systemd/system/polycopy-engine-ghost.timer
    systemctl daemon-reload
    systemctl is-active --quiet polycopy-engine-copy && { echo 'copy unit unexpectedly active' >&2; exit 18; } || true
    systemctl is-active --quiet polycopy-engine-persistent && { echo 'persistent unit unexpectedly active' >&2; exit 18; } || true
    systemctl is-active --quiet polycopy-engine-persistent-reconcile && { echo 'persistent reconciliation unit unexpectedly active' >&2; exit 18; } || true
    systemctl is-active --quiet polycopy-engine-ghost.timer && { echo 'ghost timer unexpectedly active' >&2; exit 19; } || true
    test \"\$(systemctl show polycopy-engine-copy --property=UnitFileState --value)\" = static
    test \"\$(systemctl show polycopy-engine-persistent --property=UnitFileState --value)\" = static
    systemctl show polycopy-engine-copy polycopy-engine-persistent polycopy-engine-ghost.timer --property=LoadState --property=UnitFileState --property=ActiveState --no-pager"

echo "disabled copy/persistent/GHOST operational units installed; no configuration, credential, process, or order was created"
