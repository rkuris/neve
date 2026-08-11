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
BRANCH="${1:-${BRANCH:-main}}"

# Binary-archive layout and the health wait live here, shared with
# deploy/rollback.sh so the two cannot disagree about where binaries go or how
# long a restart is allowed to take. Sourced after the re-exec below, from the
# freshly checked-out copy.
# shellcheck source=deploy/deploy-lib.sh
. "$(dirname "$0")/deploy-lib.sh"

# Re-exec under sudo if needed — we write to /usr/local/bin and drive systemd.
if [ "$(id -u)" -ne 0 ]; then
  exec sudo --preserve-env=REPO_DIR,RUST_HOME,BRANCH,HEALTH_TIMEOUT,ARCHIVE_DIR,ARCHIVE_KEEP \
    bash "$0" "$@"
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

# 4. Archive the binary about to be replaced, so a bad deploy can be undone by
#    copying a file rather than by rebuilding. Named by timestamp and by the SHA
#    it was built from: the SHA is what you would roll back *to*, and the
#    timestamp gives a total order for pruning that survives two deploys in one
#    day. Copy before the stop — reading a running executable is fine, only
#    overwriting it is not.
if [ -x "$BIN" ]; then
  mkdir -p "$ARCHIVE_DIR"
  archived="$ARCHIVE_DIR/neve-$(date -u +%Y%m%dT%H%M%SZ)-$before"
  cp -p "$BIN" "$archived"
  echo "archived current binary to $archived"
  # Drop all but the newest $ARCHIVE_KEEP. Ordered by mtime rather than by name
  # because `cp -p` preserves the original build time, which is the more useful
  # order when a deploy re-runs without the binary having changed.
  total="$(archive_count)"
  if [ "$total" -gt "$ARCHIVE_KEEP" ]; then
    archive_list_oldest_first \
      | head -z -n "$((total - ARCHIVE_KEEP))" \
      | cut -z -f2- \
      | xargs -0r rm -f --
  fi
  echo "kept $(archive_count) archived binaries in $ARCHIVE_DIR"
fi

# 5. Swap the binary and restart. Stop first: Linux refuses to overwrite a
#    running executable (ETXTBSY).
echo "restarting service (brief downtime)…"
systemctl stop "$SERVICE"
install -m 0755 "$REPO_DIR/target/release/neve" "$BIN"
systemctl start "$SERVICE"

# 6. Verify it came back and is answering. The rollback recipe is printed on
#    failure, when it is wanted, rather than left in a doc nobody opens then.
healthy=1
wait_for_health || { healthy=0; archive_rollback_hint; }

# 7. Show the operator the same formatted status block they see at login,
#    instead of dumping raw JSON — reuse the MOTD fragment we just installed (it
#    reads /health and formats it, including the now-current version line). If
#    /health never came up, the fragment prints "status: down" with a hint.
echo
if [ -x /etc/update-motd.d/99-neve-status ]; then
  /etc/update-motd.d/99-neve-status
else
  echo "neve updated $before -> $after.  status: systemctl status neve  ·  logs: journalctl -u neve -f"
fi
echo
echo "previous binary kept in $ARCHIVE_DIR ($(archive_count) retained)"
echo "to go back:  sudo bash $REPO_DIR/deploy/rollback.sh"

# Exit non-zero if it never answered, so an unattended run fails loudly rather
# than looking like a success with a "down" line buried in its output.
[ "$healthy" -eq 1 ] || exit 1
