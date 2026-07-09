#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_HOST="${REMOTE_HOST:-3020e}"
CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
API_HOST="${API_HOST:-127.0.0.1}"
CRAWLER_NAME="${CRAWLER_NAME:-Instagram Live 自動保存发布 - 22/7}"
TIMEZONE="${TIMEZONE:-Asia/Tokyo}"
START=""
END=""
WINDOW_BEFORE_MINUTES="${WINDOW_BEFORE_MINUTES:-10}"
WINDOW_AFTER_MINUTES="${WINDOW_AFTER_MINUTES:-180}"
HANDLE=""
DRY_RUN=0

usage() {
  cat <<'HELP'
Usage:
  tools/schedule-ig-live.sh --handle <instagram_handle> --start HH:MM [--end HH:MM]
  tools/schedule-ig-live.sh --handle <instagram_handle> --start 20:55 --window-before 10 --window-after 180

Defaults:
  CRAWLER_NAME="Instagram Live 自動保存发布 - 22/7"
  REMOTE_HOST=3020e CONTAINER_NAME=forwarder-new API_HOST=127.0.0.1

Notes:
  - The target crawler must already exist in /app/config.yaml and runtime config.
  - This hot-upserts the crawler schedule and queues an immediate run for the handle.
  - It does not enable upload; publish is controlled by crawler config.
HELP
}

while [ $# -gt 0 ]; do
  case "$1" in
    --handle) HANDLE="${2:-}"; shift 2 ;;
    --start) START="${2:-}"; shift 2 ;;
    --end) END="${2:-}"; shift 2 ;;
    --window-before) WINDOW_BEFORE_MINUTES="${2:-}"; shift 2 ;;
    --window-after) WINDOW_AFTER_MINUTES="${2:-}"; shift 2 ;;
    --crawler) CRAWLER_NAME="${2:-}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ -z "$HANDLE" ] || [ -z "$START" ]; then
  usage >&2
  exit 2
fi

payload_json="$(python3 - "$HANDLE" "$START" "$END" "$WINDOW_BEFORE_MINUTES" "$WINDOW_AFTER_MINUTES" "$TIMEZONE" <<'PY'
import datetime as dt
import json
import sys
from zoneinfo import ZoneInfo
handle, start, end, before, after, timezone = sys.argv[1:7]
before = int(before)
after = int(after)
tz = ZoneInfo(timezone)
now = dt.datetime.now(tz)
hour, minute = map(int, start.split(':'))
start_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
if end:
    eh, em = map(int, end.split(':'))
    end_dt = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    if end_dt <= start_dt:
        end_dt += dt.timedelta(days=1)
else:
    end_dt = start_dt + dt.timedelta(minutes=after)
window_start = start_dt - dt.timedelta(minutes=before)
print(json.dumps({
    'handle': handle,
    'website': f'https://www.instagram.com/{handle}',
    'schedule': {
        'timezone': timezone,
        'windows': [{
            'start': window_start.strftime('%H:%M'),
            'end': end_dt.strftime('%H:%M'),
            'every_minutes': 1,
        }],
        'min_gap_seconds': 45,
        'tick_seconds': 10,
    },
    'start_jst': start_dt.isoformat(),
    'window_start': window_start.strftime('%H:%M'),
    'window_end': end_dt.strftime('%H:%M'),
}, ensure_ascii=False))
PY
)"

if [ "$DRY_RUN" = 1 ]; then
  printf '%s\n' "$payload_json" | python3 -m json.tool
  exit 0
fi

ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" \
  "CONTAINER_NAME=$(printf %q "$CONTAINER_NAME") API_HOST=$(printf %q "$API_HOST") CRAWLER_NAME=$(printf %q "$CRAWLER_NAME") PAYLOAD=$(printf %q "$payload_json") bash -s" <<'REMOTE'
set -Eeuo pipefail
read_api_config() {
  docker exec "$CONTAINER_NAME" bun -e '
    const fs = require("fs")
    const YAML = require("yaml")
    const config = YAML.parse(fs.readFileSync("/app/config.yaml", "utf8")) || {}
    const port = Number(config.api?.port || 3000)
    const secret = String(config.api?.secret || process.env.API_SECRET || "")
    if (!secret) process.exit(7)
    console.log(JSON.stringify({ port, secret }))
  '
}
api_config="$(read_api_config)"
api_port="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["port"])' <<<"$api_config")"
api_secret="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["secret"])' <<<"$api_config")"
api_base="http://${API_HOST}:${api_port}"
auth="Authorization: Bearer ${api_secret}"

crawler_exists="$(docker exec "$CONTAINER_NAME" bun -e '
  const fs=require("fs"), YAML=require("yaml")
  const cfg=YAML.parse(fs.readFileSync("/app/config.yaml","utf8"))||{}
  const name=process.argv[2]
  console.log((cfg.crawlers||[]).some(c=>c.name===name)?"yes":"no")
' -- "$CRAWLER_NAME" 2>/dev/null)"
if [ "$crawler_exists" != "yes" ]; then
  echo "crawler not present in runtime config: $CRAWLER_NAME" >&2
  echo "deploy/reload config with the IG live crawler before relying on schedule." >&2
  exit 3
fi

schedule_body="$(python3 - <<'PY'
import json, os
p=json.loads(os.environ['PAYLOAD'])
print(json.dumps({'name': os.environ['CRAWLER_NAME'], 'schedule': p['schedule']}, ensure_ascii=False))
PY
)"
run_body="$(python3 - <<'PY'
import json, os
p=json.loads(os.environ['PAYLOAD'])
print(json.dumps({'name': os.environ['CRAWLER_NAME'], 'websites': [p['website']]}, ensure_ascii=False))
PY
)"

echo "upsert schedule:"
curl -sS -X POST -H "$auth" -H 'Content-Type: application/json' --data-binary "$schedule_body" "$api_base/api/schedules/crawlers/upsert" | python3 -m json.tool

echo "queue immediate probe:"
curl -sS -X POST -H "$auth" -H 'Content-Type: application/json' --data-binary "$run_body" "$api_base/api/actions/crawlers/run" | python3 -m json.tool
REMOTE
