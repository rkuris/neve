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
  exec sudo --preserve-env=REPO_DIR,RUST_HOME,BRANCH,HEALTH_TIMEOUT bash "$0" "$@"
fi

# rustup proxies need these to find the toolchain bootstrap installed under
# /opt/rust; without them they fall back to ~/.rustup and fail to build.
export RUSTUP_HOME="$RUST_HOME" CARGO_HOME="$RUST_HOME"

if [ ! -x "$RUST_HOME/bin/cargo" ]; then
  echo "error: no toolchain at $RUST_HOME — run deploy/bootstrap.sh first" >&2
  exit 1
fi

echo "== neve update starting (branch: $BRANCH) =="

# 1. Update the source and re-exec the fresh copy — first pass only. The clone
#    is shallow (--depth 1), so fetch the tip and hard-reset onto it rather than
#    a merge shallow history rejects. bash holds the *old* update.sh open, so we
#    re-exec the freshly checked-out copy before building — otherwise the rest of
#    this run is pre-update logic, which breaks when an update renames a file it
#    installs (the 20- -> 99- MOTD rename did exactly that). Carry the before/
#    after SHAs across the exec so the second pass reports the real transition
#    rather than re-fetching — a re-fetch finds HEAD already moved and would
#    print a misleading "already at <new>".
cd "$REPO_DIR"
if [ -z "${NEVE_UPDATE_REEXEC:-}" ]; then
  before="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  git fetch --depth 1 origin "$BRANCH"
  git reset --hard FETCH_HEAD
  after="$(git rev-parse --short HEAD)"
  export NEVE_UPDATE_REEXEC=1 NEVE_UPDATE_BEFORE="$before" NEVE_UPDATE_AFTER="$after"
  echo "re-exec'ing updated update.sh"
  exec bash "$REPO_DIR/deploy/update.sh" "$@"
fi

# Post-re-exec: the working tree is already at the new tip; report the
# transition the first pass recorded. Default both under `set -u` so a re-exec
# from an *older* update.sh — which set NEVE_UPDATE_REEXEC but not these SHAs —
# degrades to a sane report instead of aborting on an unbound variable.
before="${NEVE_UPDATE_BEFORE:-unknown}"
after="${NEVE_UPDATE_AFTER:-$(git rev-parse --short HEAD)}"
if [ "$before" = "$after" ]; then
  echo "already up to date at $after — rebuilding and restarting anyway"
else
  echo "updating $before -> $after"
fi

# 2. Build first, with the old binary still serving. Only the swap below
#    interrupts requests, so build time is not downtime.
"$RUST_HOME/bin/cargo" build --release --locked

# 3. Refresh the unit in case it changed; never clobber an edited neve.env.
install -m 0644 "$REPO_DIR/deploy/neve.service" /etc/systemd/system/neve.service
systemctl daemon-reload

# 3b. Refresh the login MOTD (status fragment + stock-MOTD quieting). Shared
#     with bootstrap.sh; only re-installs the fragment when it changed.
bash "$REPO_DIR/deploy/setup-motd.sh" "$REPO_DIR"

# 4. Swap the binary and restart. Stop first: Linux refuses to overwrite a
#    running executable (ETXTBSY).
echo "restarting service (brief downtime)…"
systemctl stop "$SERVICE"
install -m 0755 "$REPO_DIR/target/release/neve" "$BIN"
systemctl start "$SERVICE"

# 5. Verify it came back and is answering.
#
# The wait has to outlast *store recovery*, not just process start: neve opens
# the blockstore and recovers the fjall index before it binds the RPC port, and
# that scales with store size. On the mainnet host (2026-08-11, ~5 GiB index)
# recovery took 43s, so the previous 30s wait reported "down" 13s before the
# service was actually up — a false alarm on every upgrade, which is worse than
# no check because it teaches you to ignore the real thing.
#
# Bail out early if the unit dies, so a genuine failure surfaces immediately
# instead of sitting out the whole window.
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-180}"
healthy=0
started=$SECONDS
printf 'waiting for health (up to %ss; a large store recovers before the port binds)' "$HEALTH_TIMEOUT"
while [ $((SECONDS - started)) -lt "$HEALTH_TIMEOUT" ]; do
  if curl -fsS http://127.0.0.1:8545/health >/dev/null 2>&1; then
    healthy=1
    break
  fi
  if ! systemctl is-active --quiet "$SERVICE"; then
    printf '\n'
    echo "error: $SERVICE stopped after $((SECONDS - started))s — it did not survive the restart" >&2
    break
  fi
  printf '.'
  sleep 1
done
elapsed=$((SECONDS - started))

if [ "$healthy" -eq 1 ]; then
  printf ' ok (%ss)\n' "$elapsed"
else
  printf '\n'
  echo "error: no healthy response after ${elapsed}s" >&2
  echo "  logs:   journalctl -u $SERVICE -e" >&2
  echo "  status: systemctl status $SERVICE" >&2
  echo "  if it is still recovering a large store, re-run with HEALTH_TIMEOUT=600" >&2
fi

# 6. Show the operator the same formatted status block they see at login,
#    instead of dumping raw JSON — reuse the MOTD fragment we just installed (it
#    reads /health and formats it, including the now-current version line). If
#    /health never came up, the fragment prints "status: down" with a hint.
echo
if [ -x /etc/update-motd.d/99-neve-status ]; then
  /etc/update-motd.d/99-neve-status
else
  echo "neve updated $before -> $after.  status: systemctl status neve  ·  logs: journalctl -u neve -f"
fi

# Exit non-zero if it never answered, so an unattended run fails loudly rather
# than looking like a success with a "down" line buried in its output.
[ "$healthy" -eq 1 ] || exit 1
