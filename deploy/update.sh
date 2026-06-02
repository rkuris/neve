#!/usr/bin/env bash
# Update an already-bootstrapped neve host to the latest code and restart.
#
# For boxes first provisioned by deploy/bootstrap.sh (via cloud-init). Pulls the
# newest commit, rebuilds the release binary *while the old one keeps serving*,
# then swaps it in with a brief restart and checks the service came back. Your
# /etc/neve/neve.env and the blockstore in /var/lib/neve are left untouched.
#
# Usage:  sudo bash /opt/neve/deploy/update.sh [BRANCH]   # BRANCH defaults to main
set -euo pipefail

REPO_DIR="${REPO_DIR:-/opt/neve}"   # where cloud-init cloned the repo
RUST_HOME="${RUST_HOME:-/opt/rust}" # build-time toolchain from bootstrap.sh
BIN=/usr/local/bin/neve
SERVICE=neve.service
BRANCH="${1:-${BRANCH:-main}}"

# Re-exec under sudo if needed — we write to /usr/local/bin and drive systemd.
if [ "$(id -u)" -ne 0 ]; then
  exec sudo --preserve-env=REPO_DIR,RUST_HOME,BRANCH bash "$0" "$@"
fi

# rustup proxies need these to find the toolchain bootstrap installed under
# /opt/rust; without them they fall back to ~/.rustup and fail to build.
export RUSTUP_HOME="$RUST_HOME" CARGO_HOME="$RUST_HOME"

if [ ! -x "$RUST_HOME/bin/cargo" ]; then
  echo "error: no toolchain at $RUST_HOME — run deploy/bootstrap.sh first" >&2
  exit 1
fi

echo "== neve update starting (branch: $BRANCH) =="

# 1. Update the source. The clone is shallow (--depth 1), so fetch the current
#    tip and hard-reset onto it rather than a merge that shallow history rejects.
cd "$REPO_DIR"
before="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
git fetch --depth 1 origin "$BRANCH"
git reset --hard FETCH_HEAD
after="$(git rev-parse --short HEAD)"
if [ "$before" = "$after" ]; then
  echo "already at $after — rebuilding and restarting anyway"
else
  echo "updating $before -> $after"
fi

# 1b. Re-exec the freshly checked-out copy of this script. bash holds the old
#     file open, so without this the rest of *this* run is the pre-update
#     logic — which breaks when the update renames a file it installs (the
#     20- -> 99- MOTD rename did exactly that). The guard runs the re-exec
#     once; the second pass redoes the fetch/reset as a cheap no-op.
if [ -z "${NEVE_UPDATE_REEXEC:-}" ]; then
  export NEVE_UPDATE_REEXEC=1
  echo "re-exec'ing updated update.sh"
  exec bash "$REPO_DIR/deploy/update.sh" "$@"
fi

# 2. Build first, with the old binary still serving. Only the swap below
#    interrupts requests, so build time is not downtime.
"$RUST_HOME/bin/cargo" build --release --locked

# 3. Refresh the unit in case it changed; never clobber an edited neve.env.
install -m 0644 "$REPO_DIR/deploy/neve.service" /etc/systemd/system/neve.service
systemctl daemon-reload

# 3b. Refresh the login MOTD status script, but only when its contents
#     actually changed — so an unchanged update leaves its mtime alone.
if [ -d /etc/update-motd.d ] \
   && ! cmp -s "$REPO_DIR/deploy/99-neve-status" /etc/update-motd.d/99-neve-status; then
  install -m 0755 "$REPO_DIR/deploy/99-neve-status" /etc/update-motd.d/99-neve-status
fi

# 4. Swap the binary and restart. Stop first: Linux refuses to overwrite a
#    running executable (ETXTBSY).
echo "restarting service (brief downtime)…"
systemctl stop "$SERVICE"
install -m 0755 "$REPO_DIR/target/release/neve" "$BIN"
systemctl start "$SERVICE"

# 5. Verify it came back and is answering.
printf 'waiting for health'
for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8545/health >/dev/null 2>&1; then
    printf ' ok\n'
    break
  fi
  printf '.'
  sleep 1
done

health="$(curl -fsS http://127.0.0.1:8545/health 2>/dev/null || echo '<no response>')"
chainid="$(curl -fsS -X POST http://127.0.0.1:8545 \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' 2>/dev/null \
  || echo '<no response>')"

echo "== neve update done: $before -> $after =="
echo "   health:  $health"
echo "   chainId: $chainid"
echo "   status:  systemctl status neve"
echo "   logs:    journalctl -u neve -f"
