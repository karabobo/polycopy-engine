#!/usr/bin/env bash
# Install the disabled, read-only GHOST systemd unit after a release has been
# built remotely. This script deliberately refuses to replace an existing unit
# or touch any credential file.

set -Eeuo pipefail

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: deploy/install-ghost-unit.sh

Installs a disabled polycopy-engine-ghost.service only when no unit with that
name already exists. It never writes configuration or credential files and
never starts the unit.
EOF
    exit 0
fi

readonly approved_host="aliyun-8-220-180-39"
host="${POLYCOPY_DEPLOY_HOST:-$approved_host}"
if [[ "$host" != "$approved_host" ]]; then
    echo "POLYCOPY_DEPLOY_HOST must remain $approved_host" >&2
    exit 2
fi
remote_root="${POLYCOPY_REMOTE_ROOT:-/opt/polycopy-engine}"
if [[ "$remote_root" != "/opt/polycopy-engine" ]]; then
    echo "POLYCOPY_REMOTE_ROOT must remain /opt/polycopy-engine" >&2
    exit 2
fi

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
    test -x '$remote_root/current/target/release/ghost_verify'
    if test -e /etc/systemd/system/polycopy-engine-ghost.service; then
        echo 'refusing to replace existing polycopy-engine-ghost.service' >&2
        exit 17
    fi
    install -d -m 0700 /etc/polycopy-engine /etc/polycopy-engine/credentials
    install -d -m 0750 /var/lib/polycopy-engine /var/log/polycopy-engine
    install -m 0644 '$remote_root/current/deploy/systemd/polycopy-engine-ghost.service' \\
        /etc/systemd/system/polycopy-engine-ghost.service
    systemctl daemon-reload
    systemctl is-active --quiet polycopy-engine-ghost && {
        echo 'unexpected active ghost unit after installation' >&2
        exit 18
    } || true
    systemctl is-enabled --quiet polycopy-engine-ghost && {
        echo 'unexpected enabled ghost unit after installation' >&2
        exit 19
    } || true
    systemctl show polycopy-engine-ghost --property=LoadState --property=UnitFileState --no-pager"

echo "disabled GHOST unit installed; no credentials were created and no command was started"
