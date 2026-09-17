#!/usr/bin/env bash
# Install the Feishu notifier and its unit. Never writes the webhook
# credential, never starts or enables the service.
#
# The notifier ships outside the release tree on purpose. It is a read-only
# sidecar written in Python: coupling it to the Rust release would mean a
# fifteen-minute rebuild to reword a notification, and would tie a cosmetic
# change to the cadence of the trading binary.

set -Eeuo pipefail

readonly approved_host="aliyun-8-220-180-39"
host="${POLYCOPY_DEPLOY_HOST:-$approved_host}"
if [[ "$host" != "$approved_host" ]]; then
    echo "POLYCOPY_DEPLOY_HOST must remain $approved_host" >&2
    exit 2
fi
readonly remote_root="/opt/polycopy-engine"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test -f "$here/tools/notify/notify.py"
test -f "$here/deploy/systemd/polycopy-engine-notify.service"

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
    install -d -m 0755 '$remote_root/notify'"

scp -q -o BatchMode=yes -o StrictHostKeyChecking=yes \
    "$here/tools/notify/notify.py" "$host:$remote_root/notify/notify.py.incoming"
scp -q -o BatchMode=yes -o StrictHostKeyChecking=yes \
    "$here/deploy/systemd/polycopy-engine-notify.service" \
    "$host:/tmp/polycopy-engine-notify.service.incoming"

"${ssh_args[@]}" "set -eu
    python3 -m py_compile '$remote_root/notify/notify.py.incoming'
    install -m 0755 -o root -g root \
        '$remote_root/notify/notify.py.incoming' '$remote_root/notify/notify.py'
    rm -f '$remote_root/notify/notify.py.incoming'
    find '$remote_root/notify' -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
    install -m 0644 -o root -g root \
        /tmp/polycopy-engine-notify.service.incoming \
        /etc/systemd/system/polycopy-engine-notify.service
    rm -f /tmp/polycopy-engine-notify.service.incoming
    systemctl daemon-reload
    systemd-analyze verify /etc/systemd/system/polycopy-engine-notify.service
    echo 'notifier installed (not started)'"
