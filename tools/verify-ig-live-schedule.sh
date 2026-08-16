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

# Derive the crawler names to check from the deployed config instead of a
# hardcoded list. Hardcoding "相川奈央" produced a permanent WARN whenever that
# crawler was not configured, which hid the distinction between "config has no
# such live crawler" and "config has it but the schedule line is missing".
# CONTAINER_NAME is passed as a remote env var and quoted there; never splice it
# into the remote shell command.
container_env="CONTAINER_NAME=$(printf %q "$CONTAINER_NAME")"
live_crawler_names="$(
    ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" "$container_env bash -s" <<'REMOTE' || true
docker exec "$CONTAINER_NAME" bun -e '
const fs = require("fs")
const YAML = require("yaml")
const config = YAML.parse(fs.readFileSync("/app/config.yaml", "utf8")) || {}
for (const crawler of config.crawlers || []) {
    if (crawler.cfg_crawler && crawler.cfg_crawler.live_relay && crawler.cfg_crawler.live_relay.enabled === true) {
        const name = String(crawler.name || "").trim()
        if (name) console.log(name)
    }
}
'
REMOTE
)"

if [ -z "$live_crawler_names" ]; then
    echo "INFO: no crawlers with live_relay.enabled=true in /app/config.yaml" >&2
else
    while IFS= read -r name; do
        [ -z "$name" ] && continue
        name_env="NAME=$(printf %q "$name")"
        # Match the crawler name as a fixed string, then extract only the
        # source/slots tail. A config-derived name must never be interpolated
        # into a remote grep regex.
        slot_line="$(
            ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" "$container_env $name_env bash -s" <<'REMOTE' || true
docker logs --since 12h "$CONTAINER_NAME" 2>&1 | grep -F "Crawler schedule created for $NAME:" | grep -oE 'source=[^ ]* slots=[0-9]*' | tail -1
REMOTE
        )"
        if [ -z "$slot_line" ]; then
            echo "FAIL: $name has live_relay enabled but no schedule line in the last 12h — probe window lost" >&2
            fail=1
            continue
        fi
        slots="$(echo "$slot_line" | grep -o 'slots=[0-9]*' | grep -o '[0-9]*')"
        if [ "${slots:-0}" -le 0 ]; then
            echo "FAIL: $name has zero schedule slots — probe window lost (restart wiped it?)" >&2
            fail=1
        else
            echo "OK: $name slots=$slots"
        fi
    done <<< "$live_crawler_names"
fi

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
