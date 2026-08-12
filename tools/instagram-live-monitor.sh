#!/usr/bin/env bash
set -Eeuo pipefail

# Self-healing monitor for the Instagram Live watcher (tools/instagram-live-watch.ts).
#
# Runs detached on the remote host (3020e). Every CHECK_EVERY seconds it ensures the
# dedicated `tiktok-live-watch` container is running and that an IG live watcher
# process is alive inside it, relaunching either whenever missing, up to the window
# deadline. The watcher lives in the deployment-independent container, so routine
# forwarder deploys never kill it; this monitor guards against crashes/manual kills.
#
# Usage (on the remote host):
#   bash tools/instagram-live-monitor.sh --handle nao_aikawa227 --until 01:00

CONTAINER_NAME="${CONTAINER_NAME:-tiktok-live-watch}"
REPO_DIR="${REPO_DIR:-$HOME/idol-bbq-utils}"
WATCHER_LOCAL="${WATCHER_LOCAL:-/tmp/instagram-live-watch.ts}"
UNTIL="${UNTIL:-01:00}"
HANDLE=""
POLL="${POLL:-30}"
CHECK_EVERY="${CHECK_EVERY:-15}"
PLAYER_ID="${PLAYER_ID:-relay}"
PLAYER_NAME=""
LIVE_PLAYER_URL="${LIVE_PLAYER_URL:-https://tv.n2nj.moe}"
AUTH_USERNAME="${AUTH_USERNAME:-sumie}"
AUTH_PASSWORD="${AUTH_PASSWORD:-}"
WAF_HEADER="${WAF_HEADER:-N2NJ_SUPER_SECRET_PASS_2026_7684}"
ARCHIVE="${ARCHIVE:-0}"

usage() {
  cat <<'HELP'
Usage: instagram-live-monitor.sh --handle <handle> [--until HH:MM] [--player-id ID]
       [--player-name "名称"] [--poll SEC] [--check-every SEC] [--archive]
HELP
}

while [ $# -gt 0 ]; do
  case "$1" in
    --handle) HANDLE="${2#@}"; shift 2 ;;
    --until) UNTIL="$2"; shift 2 ;;
    --player-id) PLAYER_ID="$2"; shift 2 ;;
    --player-name) PLAYER_NAME="$2"; shift 2 ;;
    --poll) POLL="$2"; shift 2 ;;
    --check-every) CHECK_EVERY="$2"; shift 2 ;;
    --archive) ARCHIVE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ -n "$HANDLE" ] || { usage >&2; exit 2; }
[ -n "$AUTH_PASSWORD" ] || { echo "AUTH_PASSWORD is required" >&2; exit 2; }
[ -n "$PLAYER_NAME" ] || PLAYER_NAME="【IG Live】${HANDLE}"

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

ARCHIVE_ARGS=()
if [ "$ARCHIVE" = "1" ]; then ARCHIVE_ARGS=(--archive); fi

ensure_container() {
  if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "[monitor $(date '+%F %T %Z')] container $CONTAINER_NAME missing; starting"
    (cd "$REPO_DIR" && docker compose up -d "$CONTAINER_NAME") || true
    sleep 5
  fi
}

ensure_watcher_file() {
  docker exec "$CONTAINER_NAME" sh -lc 'test -f /app/instagram-live-watch.ts' 2>/dev/null || {
    echo "[monitor $(date '+%F %T %Z')] deploying watcher into $CONTAINER_NAME"
    docker cp "$WATCHER_LOCAL" "$CONTAINER_NAME":/app/instagram-live-watch.ts || true
  }
}

relaunch() {
  ensure_container
  ensure_watcher_file
  # Do not remove the lock here: the watcher's acquireLock already reclaims stale
  # locks via PID liveness, and an unconditional rm can race a healthy watcher
  # into running two instances for the same handle.
  docker exec -e AUTH_PASSWORD="$AUTH_PASSWORD" "$CONTAINER_NAME" sh -lc \
    "mkdir -p /app/archive/instagram-live; \
     nohup bun /app/instagram-live-watch.ts $HANDLE --until $UNTIL --poll $POLL \
       --player-id \"$PLAYER_ID\" --player-name \"$PLAYER_NAME\" --live-player-url $LIVE_PLAYER_URL \
       --auth-username $AUTH_USERNAME --auth-password \"\$AUTH_PASSWORD\" --waf-header \"$WAF_HEADER\" \
       --cookie /app/assets/cookies/inscks0318.txt ${ARCHIVE_ARGS[*]:-} \
       >> /app/archive/instagram-live/watch-$HANDLE.log 2>&1 & echo started=\$!"
}

watcher_alive() {
  docker exec "$CONTAINER_NAME" sh -lc '
    for d in /proc/[0-9]*; do
      if tr "\0" " " < "$d/cmdline" 2>/dev/null | grep -q "instagram-live-watch.ts '"$HANDLE"'"; then
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
