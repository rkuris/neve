#!/usr/bin/env bash
# Provision and launch neve as a systemd service on a fresh Ubuntu host.
#
# Invoked by cloud-init (see deploy/cloud-init.yaml), but also safe to run by
# hand:  sudo bash /opt/neve/deploy/bootstrap.sh
#
# Idempotent: re-running rebuilds the binary and restarts the service without
# clobbering an edited /etc/neve/neve.env. Both neve and its blockstore
# dependency are public repos, so no SSH keys or git credentials are needed.
set -euo pipefail

REPO_DIR="${REPO_DIR:-/opt/neve}"   # where cloud-init cloned the repo
RUST_HOME=/opt/rust                 # build-time toolchain (not needed at runtime)
BIN=/usr/local/bin/neve
SERVICE_USER=neve

# rustup proxies locate the toolchain via these. Export unconditionally (not
# just when installing below) so a re-run — which skips the install block
# because cargo already exists — still points cargo at /opt/rust and builds.
export RUSTUP_HOME="$RUST_HOME" CARGO_HOME="$RUST_HOME"

echo "== neve bootstrap starting =="

# 1. Swap. A release build of neve + its dependency tree can exceed RAM on the
#    small burstable instances this is meant for (t3.micro/t4g.small). 2 GiB of
#    swap keeps the linker from OOM-killing the build; harmless once built.
if ! swapon --show | grep -q '/swapfile'; then
  fallocate -l 2G /swapfile || dd if=/dev/zero of=/swapfile bs=1M count=2048
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# 2. Dedicated unprivileged service account (no login). systemd's
#    StateDirectory provides /var/lib/neve, so this user needs no home.
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --shell /usr/sbin/nologin "$SERVICE_USER"
fi

# 3. Rust toolchain via rustup (build-time only; the binary is statically
#    self-contained as far as Rust is concerned).
if [ ! -x "$RUST_HOME/bin/cargo" ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --default-toolchain stable
fi

# 4. Build the release binary and install it.
( cd "$REPO_DIR" && "$RUST_HOME/bin/cargo" build --release --locked )
install -m 0755 "$REPO_DIR/target/release/neve" "$BIN"

# 5. Install the unit + default env. Don't clobber an env the operator edited.
install -d /etc/neve
if [ ! -f /etc/neve/neve.env ]; then
  install -m 0644 "$REPO_DIR/deploy/neve.env" /etc/neve/neve.env
fi
install -m 0644 "$REPO_DIR/deploy/neve.service" /etc/systemd/system/neve.service

# 6. Launch.
systemctl daemon-reload
systemctl enable --now neve.service

echo "== neve bootstrap done =="
echo "   status: systemctl status neve"
echo "   logs:   journalctl -u neve -f"
echo "   health: curl -s http://127.0.0.1:8545/health"
