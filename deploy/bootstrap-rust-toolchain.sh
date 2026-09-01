#!/usr/bin/env bash
# Install the minimum Rust build prerequisite for isolated release builds.
#
# Ubuntu 24.04 ships rustup-managed /usr/bin proxy launchers. The actual stable
# toolchain and Cargo cache live below /opt/polycopy-engine, not under root's
# home and not on the system-wide PATH.

set -Eeuo pipefail

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: deploy/bootstrap-rust-toolchain.sh

Installs Ubuntu's rustup package if absent, then installs Rust stable under
/opt/polycopy-engine/toolchain. It does not touch project releases, systemd,
credentials, databases, or Polymarket.
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
    if ! test -x /usr/bin/rustup; then
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends rustup
    fi
    install -d -m 0755 '$remote_root/toolchain/cargo' '$remote_root/toolchain/rustup'
    export RUSTUP_HOME='$remote_root/toolchain/rustup'
    # Ubuntu's packaged rustup binary validates its own location against the
    # default CARGO_HOME. Keep that launcher at its package-managed path while
    # placing the actual toolchain and later Cargo caches under the project.
    unset CARGO_HOME
    RUSTUP_NO_SELF_UPDATE=1 /usr/bin/rustup toolchain install stable --profile minimal
    export CARGO_HOME='$remote_root/toolchain/cargo'
    cargo_bin=\$(/usr/bin/rustup which cargo)
    case \"\$cargo_bin\" in
        '$remote_root'/toolchain/rustup/toolchains/*/bin/cargo) ;;
        *) echo \"unexpected cargo toolchain path: \$cargo_bin\" >&2; exit 21 ;;
    esac
    \"\$cargo_bin\" --version
    /usr/bin/rustc --version"

echo "isolated Rust stable toolchain is ready; no release or service changed"
