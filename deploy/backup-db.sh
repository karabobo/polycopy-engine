#!/usr/bin/env bash
# Make one SQLite-consistent, root-only copy-engine backup on the designated
# host. It does not stop a service or alter the source database.

set -Eeuo pipefail

readonly approved_host="aliyun-8-220-180-39"
host="${POLYCOPY_DEPLOY_HOST:-$approved_host}"
if [[ "$host" != "$approved_host" ]]; then
    echo "POLYCOPY_DEPLOY_HOST must remain $approved_host" >&2
    exit 2
fi

readonly database_path="/var/lib/polycopy-engine/polycopy.sqlite"
readonly backup_dir="/var/lib/polycopy-engine/backups"

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
    test -f '$database_path'
    command -v sqlite3 >/dev/null
    umask 077
    install -d -m 0700 '$backup_dir'
    stamp=\$(date -u +%Y%m%dT%H%M%SZ)
    target='$backup_dir/polycopy-'\"\$stamp\"'.sqlite'
    sqlite3 '$database_path' \".backup '\$target'\"
    test -s \"\$target\"
    sqlite3 \"\$target\" 'PRAGMA integrity_check;' | grep -Fx 'ok' >/dev/null
    stat -c '%a %s %n' \"\$target\""
