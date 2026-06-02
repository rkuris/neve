#!/usr/bin/env bash
# Install neve's login MOTD and quiet the stock Ubuntu MOTD so the neve status
# block stands alone. Shared by bootstrap.sh (provisioning) and update.sh
# (refresh) so both apply the same MOTD policy; safe to run by hand too:
#
#   sudo bash /opt/neve/deploy/setup-motd.sh [REPO_DIR]   # REPO_DIR defaults to /opt/neve
#
# Idempotent: re-running re-installs the fragment only when its contents
# changed and re-applies the (idempotent) disabling of the stock scripts.
set -euo pipefail

REPO_DIR="${1:-${REPO_DIR:-/opt/neve}}"
MOTD_DIR=/etc/update-motd.d
SRC="$REPO_DIR/deploy/99-neve-status"

# update-motd.d isn't present on every distro; nothing to do if it's absent.
[ -d "$MOTD_DIR" ] || exit 0

# jq powers the detail block; 99-neve-status still reports up/down without it.
command -v jq >/dev/null || (apt-get update -qq && apt-get install -y -qq jq) || true

# Install the status fragment only when its contents changed, so an unchanged
# refresh leaves its mtime alone. cmp -s exits non-zero (and silent) when the
# destination is missing, so a fresh host installs.
if ! cmp -s "$SRC" "$MOTD_DIR/99-neve-status"; then
  install -m 0755 "$SRC" "$MOTD_DIR/99-neve-status"
fi

# Quiet the stock fragments so only the neve block (and the Ubuntu welcome
# line) render: the system-info dump, the help/docs links, the news fetcher,
# and the updates/ESM nags. Each chmod is guarded on existence (releases vary)
# and idempotent.
for f in 50-landscape-sysinfo 10-help-text 50-motd-news \
         90-updates-available 91-contract-ua-esm-status; do
  [ -e "$MOTD_DIR/$f" ] && chmod -x "$MOTD_DIR/$f"
done

# 50-motd-news also pre-renders its cache from a systemd timer; mask it too —
# dropping the x-bit alone leaves the timer running.
systemctl disable --now motd-news.timer 2>/dev/null || true
