#!/usr/bin/env bash
# Install the disabled, read-only GHOST systemd unit after a release has been
# built remotely. This script deliberately refuses to replace an existing unit
# or touch any credential file.

set -Eeuo pipefail

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: POLYCOPY_DEPLOY_HOST=<ssh-host> deploy/install-ghost-unit.sh

Installs a disabled polycopy-engine-ghost.service only when no unit with that
name already exists. It never writes /etc/polycopy-engine/ghost.env and never
starts the unit.
EOF
    exit 0
fi

host="${POLYCOPY_DEPLOY_HOST:?set POLYCOPY_DEPLOY_HOST to the dedicated execution host}"
remote_root="${POLYCOPY_REMOTE_ROOT:-/opt/polycopy-engine}"
if [[ "$remote_root" != "/opt/polycopy-engine" ]]; then
    echo "POLYCOPY_REMOTE_ROOT must remain /opt/polycopy-engine" >&2
    exit 2
fi

ssh "$host" "set -eu
    test -x '$remote_root/current/target/release/ghost_verify'
    if test -e /etc/systemd/system/polycopy-engine-ghost.service; then
        echo 'refusing to replace existing polycopy-engine-ghost.service' >&2
        exit 17
    fi
    install -d -m 0700 /etc/polycopy-engine
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
