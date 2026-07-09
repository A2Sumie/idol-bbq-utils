#!/usr/bin/env bash
set -Eeuo pipefail

# Schedule/launch a no-upload TikTok Live capture on the remote forwarder host.
# It deploys the self-contained watcher (tiktok-live-watch.ts) into the container
# and runs it detached. The watcher detects live via the authoritative webcast
# room-info API, captures to MKV with ffmpeg copy, and never uploads.
#
# Examples:
#   tools/schedule-tiktok-live.sh --handle mao_asaoka --once
#   tools/schedule-tiktok-live.sh --handle mao_asaoka --until 23:59
#   tools/schedule-tiktok-live.sh --handle mao_asaoka --start 21:00 --until 23:59   # cron-style: register only
#   tools/schedule-tiktok-live.sh --handle mao_asaoka --status
#
# For truly unattended scheduling, put a crontab entry on the remote host that
# invokes this with --once (or --until) at the desired JST time.

REMOTE_HOST="${REMOTE_HOST:-3020e}"
CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
WATCHER_LOCAL="${WATCHER_LOCAL:-}"
HANDLE=""
UNTIL=""
START=""
MAX_MINUTES="${MAX_MINUTES:-240}"
POLL="${POLL:-20}"
ONCE=0
STATUS=0
DRY_RUN=0

usage() {
  cat <<'HELP'
Usage:
  tools/schedule-tiktok-live.sh --handle <handle> [--once | --until HH:MM] [--poll SEC] [--max-minutes N]
  tools/schedule-tiktok-live.sh --handle <handle> --start HH:MM --until HH:MM   # prints a cron line to register
  tools/schedule-tiktok-live.sh --handle <handle> --status
  tools/schedule-tiktok-live.sh --handle <handle> --once --dry-run

Notes:
  - No upload. Captures to /app/archive/tiktok-live/<handle>-<ts>/<handle>-<ts>.mkv on the remote host.
  - 麻丘真央's current live handle is: mao_asaoka  (NOT mao_asaoka_227).
  - --once: check now; capture until the live ends; then exit.
  - --until HH:MM (JST): poll until that time, capturing whenever live.
HELP
}

while [ $# -gt 0 ]; do
  case "$1" in
    --handle) HANDLE="${2#@}"; shift 2 ;;
    --until) UNTIL="$2"; shift 2 ;;
    --start) START="$2"; shift 2 ;;
    --poll) POLL="$2"; shift 2 ;;
    --max-minutes) MAX_MINUTES="$2"; shift 2 ;;
    --once) ONCE=1; shift ;;
    --status) STATUS=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ -n "$HANDLE" ] || { usage >&2; exit 2; }

# Locate the watcher source next to this script (or via WATCHER_LOCAL override).
if [ -z "$WATCHER_LOCAL" ]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  WATCHER_LOCAL="$SCRIPT_DIR/tiktok-live-watch.ts"
fi

# Cron-registration mode: just print a ready-to-install crontab line.
if [ -n "$START" ]; then
  read -r ch cm <<<"$(echo "$START" | awk -F: '{print $1, $2}')"
  self="tools/schedule-tiktok-live.sh"
  line="$cm $ch * * * cd $(pwd) && REMOTE_HOST=$REMOTE_HOST CONTAINER_NAME=$CONTAINER_NAME $self --handle $HANDLE${UNTIL:+ --until $UNTIL} >> /tmp/tt-$HANDLE.cron.log 2>&1"
  echo "# Add this to your (local) crontab to auto-run at $START JST-equivalent local time:"
  echo "$line"
  exit 0
fi

if [ "$DRY_RUN" = 1 ]; then
  echo "would deploy: $WATCHER_LOCAL -> $CONTAINER_NAME:/app/tiktok-live-watch.ts"
  echo "would run: bun /app/tiktok-live-watch.ts $HANDLE $([ "$ONCE" = 1 ] && echo --once)${UNTIL:+ --until $UNTIL} --poll $POLL --max-minutes $MAX_MINUTES"
  exit 0
fi

[ -f "$WATCHER_LOCAL" ] || { echo "watcher not found: $WATCHER_LOCAL" >&2; exit 3; }

# Status mode: show current watcher/capture state.
if [ "$STATUS" = 1 ]; then
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" "CONTAINER_NAME=$(printf %q "$CONTAINER_NAME") HANDLE=$(printf %q "$HANDLE") bash -s" <<'REMOTE'
set -Eeuo pipefail
docker exec "$CONTAINER_NAME" sh -lc '
  echo "=== watcher procs ==="
  for d in /proc/[0-9]*; do
    if grep -qa "tiktok-live-watch.ts" "$d/cmdline" 2>/dev/null; then
      echo "watch pid=$(basename $d): $(tr "\0" " " < $d/cmdline)"
    fi
  done
  echo "=== recent captures ==="
  find /app/archive/tiktok-live -maxdepth 2 -name "*.mkv" -printf "%s\t%p\n" 2>/dev/null | sort -rn | head -10 || echo none
'
REMOTE
  exit 0
fi

# Deploy watcher into the container and launch detached.
scp -q -o BatchMode=yes -o ConnectTimeout=10 "$WATCHER_LOCAL" "$REMOTE_HOST:/tmp/tiktok-live-watch.ts"
RUN_ARGS="$HANDLE"
[ "$ONCE" = 1 ] && RUN_ARGS="$RUN_ARGS --once"
[ -n "$UNTIL" ] && RUN_ARGS="$RUN_ARGS --until $UNTIL"
RUN_ARGS="$RUN_ARGS --poll $POLL --max-minutes $MAX_MINUTES"

ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" \
  "CONTAINER_NAME=$(printf %q "$CONTAINER_NAME") HANDLE=$(printf %q "$HANDLE") RUN_ARGS=$(printf %q "$RUN_ARGS") bash -s" <<'REMOTE'
set -Eeuo pipefail
docker cp /tmp/tiktok-live-watch.ts "$CONTAINER_NAME":/app/tiktok-live-watch.ts
log="/app/archive/tiktok-live/watch-$HANDLE.log"
docker exec "$CONTAINER_NAME" sh -lc "mkdir -p /app/archive/tiktok-live && nohup bun /app/tiktok-live-watch.ts $RUN_ARGS >> $log 2>&1 & echo started-pid=\$!"
echo "watcher launched; log: $log (inside container)"
docker exec "$CONTAINER_NAME" sh -lc "sleep 3; tail -n 20 $log 2>/dev/null || true"
REMOTE
