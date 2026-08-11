#!/usr/bin/env bash
# Put a previously-deployed neve binary back, without rebuilding.
#
# deploy/update.sh archives the binary it is about to replace (see
# deploy/deploy-lib.sh), so recovering from a bad deploy is a file copy rather
# than a compile — which matters on a small instance, where rebuilding means
# minutes of downtime you are trying to end, not start.
#
# Usage:
#   sudo bash /opt/neve/deploy/rollback.sh              # list what is available
#   sudo bash /opt/neve/deploy/rollback.sh 2            # roll back to choice 2
#   sudo bash /opt/neve/deploy/rollback.sh 724acbd      # roll back to a SHA
#
# Listing is the default because rolling back is destructive and the choice is
# rarely obvious — you usually want to see which one is running first.
set -euo pipefail

REPO_DIR="${REPO_DIR:-/opt/neve}"
# shellcheck source=deploy/deploy-lib.sh
. "$(dirname "$0")/deploy-lib.sh"

# Cheap identity check so the listing can mark what is currently installed.
# Comparing bytes rather than trusting the label: the running binary may have
# been replaced by hand, and a wrong "<- running" marker would send someone
# rolling back to what they are already on.
same_as_installed() {
  [ -x "$BIN" ] && cmp -s "$1" "$BIN"
}

mapfile -t archives < <(archive_paths_newest_first)

if [ "${#archives[@]}" -eq 0 ]; then
  echo "no archived binaries in $ARCHIVE_DIR" >&2
  echo "one is written each time deploy/update.sh replaces the binary." >&2
  exit 1
fi

# Ask the binary what it is. `neve --version` reports both crate version and the
# commit it was built from, so the listing does not have to trust the filename —
# which is only as good as the checkout state when it was written, and says
# nothing at all about a binary installed by hand. Guarded because an archive from
# a foreign architecture, or a truncated copy, will simply fail to run.
binary_identity() {
  "$1" --version 2>/dev/null | head -1 || echo "unreadable"
}

# No separate timestamp column: the filename already carries the archive time, and
# reading the file's mtime portably is not worth it — `date -r FILE` means "mtime
# of file" only to GNU date; BSD reads the argument as an epoch and silently prints
# the wrong thing.
list_archives() {
  echo "archived binaries in $ARCHIVE_DIR (newest first):"
  local i=1 path size
  for path in "${archives[@]}"; do
    size="$(du -h "$path" | cut -f1)"
    printf '  %d) %-32s %-24s %6s' \
      "$i" "$(basename "$path")" "$(binary_identity "$path")" "$size"
    if same_as_installed "$path"; then
      printf '  <- currently installed'
    fi
    printf '\n'
    i=$((i + 1))
  done
}

# Listing is read-only, so it does not need root — deliberately checked before the
# privilege escalation below, so "what do I have?" never needs a password.
if [ "$#" -eq 0 ]; then
  list_archives
  echo
  echo "to roll back:  sudo bash $0 <number|sha>"
  exit 0
fi

# Resolve the selector to exactly one archive. A number indexes the listing; a
# SHA (or any other substring) matches on filename, and an ambiguous match is an
# error rather than a guess — picking one silently is how you roll back to the
# wrong build.
selector="$1"
target=""
if [[ "$selector" =~ ^[0-9]+$ ]] && [ "$selector" -ge 1 ] && [ "$selector" -le "${#archives[@]}" ]; then
  target="${archives[$((selector - 1))]}"
else
  matches=()
  for path in "${archives[@]}"; do
    case "$(basename "$path")" in
      *"$selector"*) matches+=("$path") ;;
    esac
  done
  if [ "${#matches[@]}" -eq 1 ]; then
    target="${matches[0]}"
  elif [ "${#matches[@]}" -gt 1 ]; then
    echo "error: '$selector' matches ${#matches[@]} archives:" >&2
    printf '  %s\n' "${matches[@]##*/}" >&2
    echo "be more specific, or use the number from the listing." >&2
    exit 1
  fi
fi

if [ -z "$target" ]; then
  echo "error: no archive matches '$selector'" >&2
  echo >&2
  list_archives >&2
  exit 1
fi

if same_as_installed "$target"; then
  echo "$(basename "$target") is already the installed binary — nothing to do."
  exit 0
fi

# Everything past here writes to $BIN and drives systemd. Escalating only now
# means a mistyped selector, or a target that is already installed, is reported
# without asking for a password first.
if [ "$(id -u)" -ne 0 ]; then
  exec sudo --preserve-env=REPO_DIR,ARCHIVE_DIR,ARCHIVE_KEEP,HEALTH_TIMEOUT bash "$0" "$@"
fi

echo "rolling back to $(basename "$target")"

# Archive what is being displaced, so a rollback is itself reversible. Labelled
# from the running binary's own reported commit rather than from the checkout,
# which may have moved on: /health is authoritative about what is actually
# serving. Falls back to a marker when neve is down and cannot be asked.
current_commit="$(curl -fsS http://127.0.0.1:8545/health 2>/dev/null \
  | sed -n 's/.*"commit"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
archive_current "${current_commit:-preroll}"
archive_prune

systemctl stop "$SERVICE"
install -m 0755 "$target" "$BIN"
systemctl start "$SERVICE"

if wait_for_health; then
  echo
  if [ -x /etc/update-motd.d/99-neve-status ]; then
    /etc/update-motd.d/99-neve-status
  fi
  echo "rolled back to $(basename "$target")."
  echo "note: the next deploy/update.sh will rebuild from $REPO_DIR and replace this."
else
  echo "error: rolled-back binary did not come up healthy" >&2
  archive_rollback_hint
  exit 1
fi
