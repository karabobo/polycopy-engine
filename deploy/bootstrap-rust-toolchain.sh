#!/usr/bin/env bash
# Install the minimum Rust build prerequisite for isolated release builds.
#
# Ubuntu 24.04 ships a rustup package. The package supplies the rustup launcher;
# the actual stable toolchain and Cargo cache live below /opt/polycopy-engine,
# not under root's home and not on the system-wide PATH.

set -Eeuo pipefail

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: POLYCOPY_DEPLOY_HOST=<ssh-host> deploy/bootstrap-rust-toolchain.sh

Installs Ubuntu's rustup package if absent, then installs Rust stable under
/opt/polycopy-engine/toolchain. It does not touch project releases, systemd,
credentials, databases, or Polymarket.
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
    if ! command -v rustup >/dev/null 2>&1; then
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends rustup
    fi
    install -d -m 0755 '$remote_root/toolchain/cargo' '$remote_root/toolchain/rustup'
    export CARGO_HOME='$remote_root/toolchain/cargo'
    export RUSTUP_HOME='$remote_root/toolchain/rustup'
    export PATH=\"\$CARGO_HOME/bin:\$PATH\"
    rustup toolchain install stable --profile minimal
    rustup default stable
    cargo --version
    rustc --version"

echo "isolated Rust stable toolchain is ready; no release or service changed"
