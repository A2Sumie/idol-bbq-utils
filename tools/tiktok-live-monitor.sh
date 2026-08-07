#!/usr/bin/env bash
set -Eeuo pipefail

# Self-healing monitor for the TikTok live capture watcher.
#
# Runs detached on the remote host (3020e). Every 5 minutes it ensures the
# dedicated `tiktok-live-watch` container is running and that a watcher process
# is alive inside it, relaunching either whenever missing, up to the window
# deadline (next HH:MM from now). Because the watcher runs in the dedicated
# container, routine forwarder deploys never kill it; this monitor only guards
# against container-level crashes or manual kills.
#
# Usage (on the remote host):
#   bash tools/tiktok-live-monitor.sh --handle emma_tsukishiro --until 01:00
#
# Or from the Mac:
#   scp tools/tiktok-live-monitor.sh 3020e:/tmp/ && ssh 3020e 'nohup bash /tmp/tiktok-live-monitor.sh --handle emma_tsukishiro --until 01:00 >/tmp/tt-monitor.log 2>&1 &'

CONTAINER_NAME="${CONTAINER_NAME:-tiktok-live-watch}"
REPO_DIR="${REPO_DIR:-$HOME/idol-bbq-utils}"
WATCHER_LOCAL="${WATCHER_LOCAL:-/tmp/tiktok-live-watch.ts}"
UNTIL="${UNTIL:-01:00}"
HANDLE=""
POLL="${POLL:-15}"
MAX_MINUTES="${MAX_MINUTES:-600}"
CHECK_EVERY="${CHECK_EVERY:-300}"

usage() {
  cat <<'HELP'
Usage: tiktok-live-monitor.sh --handle <handle> [--until HH:MM] [--poll SEC] [--max-minutes N] [--check-every SEC]
HELP
}

while [ $# -gt 0 ]; do
  case "$1" in
    --handle) HANDLE="${2#@}"; shift 2 ;;
    --until) UNTIL="$2"; shift 2 ;;
    --poll) POLL="$2"; shift 2 ;;
    --max-minutes) MAX_MINUTES="$2"; shift 2 ;;
    --check-every) CHECK_EVERY="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ -n "$HANDLE" ] || { usage >&2; exit 2; }

deadline() {
  local target now
  target="$(date -d "$UNTIL" +%s)"
  now="$(date +%s)"
  if [ "$target" -le "$now" ]; then
    target="$(date -d "tomorrow $UNTIL" +%s)"
  fi
  echo "$target"
}

DEADLINE="$(deadline)"

ensure_container() {
  if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "[monitor $(date '+%F %T %Z')] container $CONTAINER_NAME missing; starting"
    (cd "$REPO_DIR" && docker compose up -d "$CONTAINER_NAME") || true
    sleep 5
  fi
}

ensure_watcher_file() {
  docker exec "$CONTAINER_NAME" sh -lc 'test -f /app/tiktok-live-watch.ts' 2>/dev/null || {
    echo "[monitor $(date '+%F %T %Z')] deploying watcher into $CONTAINER_NAME"
    docker cp "$WATCHER_LOCAL" "$CONTAINER_NAME":/app/tiktok-live-watch.ts || true
  }
}

relaunch() {
  ensure_container
  ensure_watcher_file
  docker exec "$CONTAINER_NAME" sh -lc \
    "rm -f /app/archive/tiktok-live/watch-$HANDLE.lock; mkdir -p /app/archive/tiktok-live; \
     nohup bun /app/tiktok-live-watch.ts $HANDLE --until $UNTIL --poll $POLL --max-minutes $MAX_MINUTES \
       --cookie /app/assets/cookies/tiktok_cookies.txt \
       >> /app/archive/tiktok-live/watch-$HANDLE.log 2>&1 & echo started=\$!"
}

watcher_alive() {
  docker exec "$CONTAINER_NAME" sh -lc '
    for d in /proc/[0-9]*; do
      if tr "\0" " " < "$d/cmdline" 2>/dev/null | grep -q "tiktok-live-watch"; then
        exit 0
      fi
    done
    exit 1
  ' 2>/dev/null
}

echo "[monitor] start handle=$HANDLE until=$UNTIL deadline=$(date -d @"$DEADLINE" '+%F %T %Z') check_every=${CHECK_EVERY}s"
if watcher_alive; then
  echo "[monitor] watcher already alive; not relaunching"
else
  relaunch || echo "[monitor] initial launch failed" >&2
fi

while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  sleep "$CHECK_EVERY"
  if ! watcher_alive; then
    echo "[monitor $(date '+%F %T %Z')] watcher dead; relaunching"
    relaunch || echo "[monitor] relaunch failed" >&2
  fi
done
echo "[monitor] window ended $(date '+%F %T %Z')"
