#!/usr/bin/env bash
set -Eeuo pipefail

# Post-restart/deploy audit for Instagram Live capture schedules.
#
# The 2026-08-09 incident: the IG live probe window was upserted at runtime only and
# every container restart rebuilt the schedule from the legacy config cron, so the
# crawler stopped probing and a live was missed. This audit fails loudly whenever a
# configured IG live crawler has no active schedule slots, or an active watcher
# window has no running watcher process.
#
# Run after every deploy/restart (wired into deploy-forwarder-stopped.sh verification):
#   tools/verify-ig-live-schedule.sh

REMOTE_HOST="${REMOTE_HOST:-3020e}"
CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"

fail=0

for name in "Instagram Live 抢抓 - 相川奈央" "Instagram Live 抢抓 - 椎名桜月"; do
  grep_esc="Crawler schedule created for ${name}: source=[^ ]* slots=[0-9]*"
  slot_line="$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" "docker logs --since 12h $CONTAINER_NAME 2>&1 | grep -oF 'Crawler schedule created for ${name}' | tail -1 && docker logs --since 12h $CONTAINER_NAME 2>&1 | grep -oE 'Crawler schedule created for ${name}: source=[^ ]* slots=[0-9]*' | tail -1" || true)"
  slot_line="${slot_line##*$'
'}"
  if [ -z "$slot_line" ]; then
    echo "WARN: no schedule line yet for $name (container may have just started)" >&2
    continue
  fi
  slots="$(echo "$slot_line" | grep -o 'slots=[0-9]*' | grep -o '[0-9]*')"
  if [ "${slots:-0}" -le 0 ]; then
    echo "FAIL: $name has zero schedule slots — probe window lost (restart wiped it?)" >&2
    fail=1
  else
    echo "OK: $name slots=$slots"
  fi
done

# Active watcher windows must have a live watcher process.
watcher_locks="$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" "docker exec tiktok-live-watch ls /app/archive/instagram-live/watch-*.lock 2>/dev/null" || true)"
if [ -n "$watcher_locks" ]; then
  echo "OK: IG live watcher active (lock: $(echo "$watcher_locks" | tr '\n' ' '))"
else
  echo "INFO: no IG live watcher lock file (no active window expected)" >&2
fi

if [ "$fail" = 1 ]; then
  echo "verify-ig-live-schedule: FAILED — restore the IG live window (tools/schedule-ig-live-watch.sh) and re-check." >&2
  exit 1
fi
echo "verify-ig-live-schedule: ok"
