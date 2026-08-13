#!/usr/bin/env bash
# Provision and launch neve as a systemd service on a fresh Ubuntu host.
#
# Invoked by cloud-init (see deploy/cloud-init.yaml), but also safe to run by
# hand:  sudo bash /opt/neve/deploy/bootstrap.sh
#
# Idempotent: re-running rebuilds the binary and restarts the service without
# clobbering an edited /etc/neve/config.toml or /etc/neve/neve.env. Both neve
# and its blockstore dependency are public repos, so no SSH keys or git
# credentials are needed.
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

# 5. Install the unit, the config, and the env file. Don't clobber anything the
#    operator edited.
install -d /etc/neve

# The annotated example is documentation, not configuration — always refresh it
# so the reference on the box matches the binary that was just built. Guarded
# because a checkout predating it must still bootstrap.
if [ -f "$REPO_DIR/deploy/config.toml.example" ]; then
  install -m 0644 "$REPO_DIR/deploy/config.toml.example" /etc/neve/config.toml.example
fi

# The live config is written once, and kept minimal: presence of a [chains.<x>]
# table is what enables that chain, and everything else comes from neve's
# built-in defaults, so there is no second copy of the defaults to drift. An
# operator who wants the full annotated set copies config.toml.example over it.
# cloud-init writes this file before bootstrap runs (binding 0.0.0.0), so the
# 127.0.0.1 default below only applies to a bootstrap run by hand.
if [ ! -f /etc/neve/config.toml ]; then
  cat >/etc/neve/config.toml <<'TOML'
[server]
addr = "127.0.0.1:8545"

[defaults]
summary_period = "1m"

[chains.c]
[chains.p]

# Rate-limit bypass token, if this host has one. See /etc/neve/neve.env.
# [upstream]
# token_file = "/etc/neve/token"
TOML
  chmod 0644 /etc/neve/config.toml
fi

# neve.env now holds at most NEVE_UPSTREAM_TOKEN and an emergency $NEVE_ARGS, so
# it can carry a secret: root-owned and readable only by the service user. Fix
# the mode on an existing one too — a host bootstrapped before the token moved
# here still has the old world-readable 0644.
if [ ! -f /etc/neve/neve.env ]; then
  install -m 0640 -o root -g "$SERVICE_USER" "$REPO_DIR/deploy/neve.env" /etc/neve/neve.env
else
  chown "root:$SERVICE_USER" /etc/neve/neve.env
  chmod 0640 /etc/neve/neve.env
fi

install -m 0644 "$REPO_DIR/deploy/neve.service" /etc/systemd/system/neve.service

# 5b. Login MOTD: install neve's /health status fragment and quiet the stock
#     Ubuntu MOTD so it stands alone. Shared with update.sh.
bash "$REPO_DIR/deploy/setup-motd.sh" "$REPO_DIR"

# 6. Launch.
systemctl daemon-reload
systemctl enable --now neve.service

echo "== neve bootstrap done =="
echo "   status: systemctl status neve"
echo "   logs:   journalctl -u neve -f"
echo "   health: curl -s http://127.0.0.1:8545/health"
echo "   config: /etc/neve/config.toml  (reference: /etc/neve/config.toml.example)"
echo "   both the C-chain and the P-chain are enabled; drop [chains.p] for C only"
