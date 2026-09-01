#!/usr/bin/env bash
# Build one immutable, source-only release on the designated execution host.
#
# This script never copies credentials, databases, canary artifacts, or Git
# history. It does not install or start a systemd unit, and it cannot submit an
# order. A failed remote build deliberately leaves its non-current release
# directory in place for diagnosis; it never removes a previous release.

set -Eeuo pipefail

usage() {
    cat <<'EOF'
Usage: deploy/remote-release.sh [git-commit]

Requires a clean local worktree. The selected commit is archived locally and
streamed to /opt/polycopy-engine/releases/<full-commit> on the remote host.
The SSH target is hard-locked to the designated execution-server alias.
EOF
}

if [[ "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "refusing to release a dirty worktree" >&2
    exit 2
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

commit="${1:-$(git rev-parse HEAD)}"
commit="$(git rev-parse --verify "${commit}^{commit}")"
release_dir="$remote_root/releases/$commit"

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
    umask 077
    install -d -m 0755 '$remote_root/releases'
    if test -e '$release_dir'; then
        echo 'remote release already exists: $release_dir' >&2
        exit 17
    fi
    install -d -m 0755 '$release_dir'"

git archive --format=tar "$commit" | "${ssh_args[@]}" "tar -x -C '$release_dir'"

"${ssh_args[@]}" "set -eu
    cd '$release_dir'
    export CARGO_HOME='$remote_root/toolchain/cargo'
    export RUSTUP_HOME='$remote_root/toolchain/rustup'
    test -x /usr/bin/rustup || {
        echo 'Rust toolchain missing; run deploy/bootstrap-rust-toolchain.sh first' >&2
        exit 20
    }
    cargo_bin=\$(/usr/bin/rustup which cargo)
    case \"\$cargo_bin\" in
        '$remote_root'/toolchain/rustup/toolchains/*/bin/cargo) ;;
        *) echo \"unexpected cargo toolchain path: \$cargo_bin\" >&2; exit 21 ;;
    esac
    \"\$cargo_bin\" build --release --all-features --locked
    test -x target/release/ghost_verify
    test -x target/release/canary_probe
    test -x target/release/lock_probe"

# A symlink replacement is atomic on the same filesystem. No running process
# is restarted here, so making a release current cannot itself change venue
# state.
"${ssh_args[@]}" "set -eu
    ln -s '$release_dir' '$remote_root/current.new'
    mv -Tf '$remote_root/current.new' '$remote_root/current'
    readlink -f '$remote_root/current'"

echo "remote build verified; release is current but no service was installed or started"
