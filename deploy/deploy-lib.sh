#!/usr/bin/env bash
# Shared by deploy/update.sh and deploy/rollback.sh. Sourced, not executed.
#
# Holds the two things both need and that must not drift between them: the
# binary-archive layout (where superseded binaries go, how they are named, how
# many survive) and the post-restart health wait.

BIN="${BIN:-/usr/local/bin/neve}"
SERVICE="${SERVICE:-neve.service}"

# Superseded binaries, kept so a bad deploy can be undone without a rebuild —
# which otherwise means waiting out a compile on a small instance while the
# service is down. /var/backups is the Debian convention for displaced files and
# has two properties the obvious alternatives lack: it is not on PATH, so an
# archived binary can't be run by accident or by tab-completion, and it is
# outside the repo checkout, so nothing needs gitignoring and `git reset --hard`
# cannot reach it.
ARCHIVE_DIR="${ARCHIVE_DIR:-/var/backups/neve}"
# How many to keep. Each is ~10-20 MB, so this is single-digit MB per rollback
# step — cheap next to a multi-GB block store on the same filesystem.
ARCHIVE_KEEP="${ARCHIVE_KEEP:-5}"

# Ordered by *name*, which sorts chronologically because archive_current stamps
# each file with `YYYYmmddTHHMMSSZ`. That avoids `find -printf` and the NUL-aware
# `head -z`/`cut -z`/`tac`, all of which are GNU-only: on a BSD userland they fail,
# and under `set -o pipefail` they take the calling script down with them. It is
# also the more honest ordering for pruning, which keeps the most recently
# *archived* binaries.
#
# Names are written only by archive_current, from a timestamp and a git SHA, so
# they contain no newline, tab or space and line-oriented handling is safe.
#
# The missing-directory case is handled by the guard rather than by discarding
# stderr, so a real `find` failure is still visible.
archive_paths_newest_first() {
  [ -d "$ARCHIVE_DIR" ] || return 0
  find "$ARCHIVE_DIR" -maxdepth 1 -type f -name 'neve-*' | sort -r
}

archive_count() {
  # `tr` strips the padding `wc` adds, so the result is usable in an arithmetic
  # comparison.
  archive_paths_newest_first | wc -l | tr -d '[:space:]'
}

archive_newest() {
  archive_paths_newest_first | head -n 1
}

# Copy the currently-installed binary into the archive, labelled with `$1` (the
# SHA it was built from, where known). Copying a running executable is fine —
# only overwriting one is not — so this is safe to call before stopping.
archive_current() {
  local label="${1:-unknown}"
  [ -x "$BIN" ] || return 0
  mkdir -p "$ARCHIVE_DIR"
  local stamp dest
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  dest="$ARCHIVE_DIR/neve-$stamp-$label"
  cp -p "$BIN" "$dest"
  echo "archived current binary to $dest"
}

# Drop all but the newest $ARCHIVE_KEEP. A read loop rather than `xargs -r`,
# which is another GNU-only flag; with no input the loop simply does nothing,
# where a bare `xargs rm` would run `rm` with no arguments.
archive_prune() {
  local path
  archive_paths_newest_first | tail -n "+$((ARCHIVE_KEEP + 1))" | while IFS= read -r path; do
    rm -f -- "$path"
  done
  echo "kept $(archive_count) archived binaries in $ARCHIVE_DIR"
}

# Print the rollback recipe. Called at the moment it is needed rather than left
# in a doc nobody opens mid-incident. Writes to stderr so it survives being
# piped alongside an error.
archive_rollback_hint() {
  local newest
  newest="$(archive_newest || true)"
  [ -n "$newest" ] || return 0
  {
    echo "  roll back to the previous binary (no rebuild):"
    echo "    sudo bash $(dirname "${BASH_SOURCE[0]}")/rollback.sh"
    echo "  or by hand:"
    echo "    sudo systemctl stop $SERVICE"
    echo "    sudo install -m 0755 $newest $BIN"
    echo "    sudo systemctl start $SERVICE"
  } >&2
}

# Wait for /health after a restart, returning non-zero if it never answers.
#
# The timeout has to outlast *store recovery*, not just process start: neve opens
# the blockstore and recovers the fjall index before binding the RPC port, and
# that scales with store size — 43s for the mainnet host's ~5 GiB index.
#
# Bails out early if the unit dies, so a genuine failure surfaces immediately
# instead of sitting out the whole window.
wait_for_health() {
  local timeout="${HEALTH_TIMEOUT:-180}"
  local started=$SECONDS
  printf 'waiting for health (up to %ss; a large store recovers before the port binds)' "$timeout"
  while [ $((SECONDS - started)) -lt "$timeout" ]; do
    if curl -fsS http://127.0.0.1:8545/health >/dev/null 2>&1; then
      printf ' ok (%ss)\n' "$((SECONDS - started))"
      return 0
    fi
    if ! systemctl is-active --quiet "$SERVICE"; then
      printf '\n'
      echo "error: $SERVICE stopped after $((SECONDS - started))s — it did not survive the restart" >&2
      return 1
    fi
    printf '.'
    sleep 1
  done
  printf '\n'
  echo "error: no healthy response after $((SECONDS - started))s" >&2
  echo "  logs:   journalctl -u $SERVICE -e" >&2
  echo "  status: systemctl status $SERVICE" >&2
  echo "  if it is still recovering a large store, re-run with HEALTH_TIMEOUT=600" >&2
  return 1
}
