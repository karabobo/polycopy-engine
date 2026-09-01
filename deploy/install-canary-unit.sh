#!/usr/bin/env bash
# Install the static Phase 0.5 canary unit after a release has been built
# remotely. This script never writes public config or credentials, and never
# starts the unit.

set -Eeuo pipefail

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: deploy/install-canary-unit.sh

Installs a disabled polycopy-engine-canary.service only when no unit with that
name exists. It never writes configuration or credential files and never
starts the unit. The canary binary is dry-run-only unless its separate exact
confirmation environment variable is deliberately supplied by the operator.
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
    test -x '$remote_root/current/target/release/canary_probe'
    if test -e /etc/systemd/system/polycopy-engine-canary.service; then
        echo 'refusing to replace existing polycopy-engine-canary.service' >&2
        exit 17
    fi
    install -d -m 0700 /etc/polycopy-engine /etc/polycopy-engine/credentials
    install -d -m 0750 /var/lib/polycopy-engine /var/log/polycopy-engine
    install -m 0644 '$remote_root/current/deploy/systemd/polycopy-engine-canary.service' \\
        /etc/systemd/system/polycopy-engine-canary.service
    systemctl daemon-reload
    systemctl is-active --quiet polycopy-engine-canary && {
        echo 'unexpected active canary unit after installation' >&2
        exit 18
    } || true
    unit_file_state=\$(systemctl show polycopy-engine-canary --property=UnitFileState --value)
    if test \"\$unit_file_state\" != static; then
        echo \"unexpected canary unit file state: \$unit_file_state\" >&2
        exit 19
    fi
    systemctl show polycopy-engine-canary --property=LoadState --property=UnitFileState --no-pager"

echo "static canary unit installed; no credentials were created and no command was started"
