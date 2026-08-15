#!/usr/bin/env bash
set -Eeuo pipefail

# Persistent Instagram Live capture scheduler.
#
# Unlike the old runtime-only upsert (which vanished on container restart and caused
# a missed live), this wrapper:
#   1. PERSISTS the probe window into the remote config.yaml (crawler schedule) so any
#      restart keeps the 1-minute probing window — restarts cannot lose it again.
#   2. Deploys the standalone watcher (tools/instagram-live-watch.ts) into the
#      deployment-independent `tiktok-live-watch` container and launches it for the
#      window (until HH:MM).
#   3. Launches the 15s self-heal monitor (tools/instagram-live-monitor.sh).
#   4. Restarts forwarder-new so the persisted schedule takes effect.
#
# Usage:
#   tools/schedule-ig-live-watch.sh --handle nao_aikawa227 --start 21:50 --until 01:00 \
#     --player-id relay-nao --player-name "【IG Live】相川奈央" [--archive]

REMOTE_HOST="${REMOTE_HOST:-3020e}"
CONTAINER_NAME="${CONTAINER_NAME:-tiktok-live-watch}"
HANDLE=""
START=""
UNTIL=""
WINDOW_BEFORE_MINUTES="${WINDOW_BEFORE_MINUTES:-10}"
PLAYER_ID=""
PLAYER_NAME=""
AUTH_USERNAME="${AUTH_USERNAME:-sumie}"
AUTH_PASSWORD="${AUTH_PASSWORD:-}"
WAF_HEADER="${WAF_HEADER:-${LIVE_PLAYER_SCHEDULE_WAF_BYPASS_HEADER:-}}"
if [ -z "$WAF_HEADER" ]; then
  echo "LIVE_PLAYER_SCHEDULE_WAF_BYPASS_HEADER (or WAF_HEADER) is required" >&2
  exit 1
fi
ARCHIVE=0
DRY_RUN=0

usage() {
  cat <<'HELP'
Usage:
  tools/schedule-ig-live-watch.sh --handle <handle> --start HH:MM --until HH:MM \
    --player-id <id> --player-name "名称" [--window-before N] [--archive] [--dry-run]
HELP
}

while [ $# -gt 0 ]; do
  case "$1" in
    --handle) HANDLE="${2#@}"; shift 2 ;;
    --start) START="$2"; shift 2 ;;
    --until) UNTIL="$2"; shift 2 ;;
    --window-before) WINDOW_BEFORE_MINUTES="$2"; shift 2 ;;
    --player-id) PLAYER_ID="$2"; shift 2 ;;
    --player-name) PLAYER_NAME="$2"; shift 2 ;;
    --auth-password) AUTH_PASSWORD="$2"; shift 2 ;;
    --archive) ARCHIVE=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ -n "$HANDLE" ] && [ -n "$START" ] && [ -n "$UNTIL" ] || { usage >&2; exit 2; }
[ -n "$PLAYER_ID" ] || { echo "--player-id is required" >&2; exit 2; }
[ -n "$PLAYER_NAME" ] || PLAYER_NAME="【IG Live】${HANDLE}"
[ -n "$AUTH_PASSWORD" ] || { echo "AUTH_PASSWORD is required (or --auth-password)" >&2; exit 2; }

WINDOW_START="$(python3 - "$START" "$WINDOW_BEFORE_MINUTES" <<'PY'
import sys
h, m = map(int, sys.argv[1].split(':'))
before = int(sys.argv[2])
total = (h * 60 + m - before) % (24 * 60)
print(f'{total // 60:02d}:{total % 60:02d}')
PY
)"

echo "== schedule IG live capture =="
echo "  handle=$HANDLE window=$WINDOW_START..$UNTIL player=$PLAYER_ID ($PLAYER_NAME)"

if [ "$DRY_RUN" = 1 ]; then
  echo "(dry-run) would persist window $WINDOW_START..$UNTIL in remote config; deploy watcher; launch watcher+monitor"
  exit 0
fi

# 1) Persist the probe window into the remote config (string HH:MM — verified safe against
#    the YAML sexagesimal trap: '10:00'-style values are emitted quoted).
ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" \
  "HANDLE=$(printf %q "$HANDLE") WINDOW_START=$(printf %q "$WINDOW_START") UNTIL=$(printf %q "$UNTIL") bash -s" <<'REMOTE'
set -Eeuo pipefail
python3 - "$HANDLE" "$WINDOW_START" "$UNTIL" <<'PY'
import os
import sys
import tempfile
import yaml

handle, start, end = sys.argv[1:4]
p = os.path.expanduser("~/idol-bbq-utils/assets/config.yaml")
cfg = yaml.safe_load(open(p, encoding="utf-8"))
crawler = next((c for c in cfg["crawlers"] if c.get("name") == "Instagram Live 抢抓 - " + {"nao_aikawa227": "相川奈央", "shiina_satsuki227": "椎名桜月"}.get(handle, handle)), None)
if crawler is None:
    raise SystemExit(f"crawler not found for handle {handle}")
crawler.setdefault("cfg_crawler", {})["schedule"] = {
    "timezone": "Asia/Tokyo",
    "windows": [{"start": start, "end": end, "every_minutes": 3}],
    "min_gap_seconds": 120,
    "tick_seconds": 10,
}
fd, tmp_path = tempfile.mkstemp(prefix="config.yaml.", dir=os.path.dirname(p))
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle_file:
        yaml.safe_dump(cfg, handle_file, allow_unicode=True, default_flow_style=False, sort_keys=False)
    os.replace(tmp_path, p)
except BaseException:
    try:
        os.unlink(tmp_path)
    except OSError:
        pass
    raise
print(f"persisted window {start}..{end} for {handle}")
PY
REMOTE

# 2) Deploy the watcher into the dedicated container + stage the monitor script.
WATCHER_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/instagram-live-watch.ts"
scp -q -o BatchMode=yes -o ConnectTimeout=10 "$WATCHER_SRC" "$REMOTE_HOST:/tmp/instagram-live-watch.ts"
scp -q -o BatchMode=yes -o ConnectTimeout=10 "$(dirname "$WATCHER_SRC")/instagram-live-monitor.sh" "$REMOTE_HOST:/tmp/instagram-live-monitor.sh"
ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" "docker cp /tmp/instagram-live-watch.ts $CONTAINER_NAME:/app/instagram-live-watch.ts"

# 3) Launch the watcher + monitor for the window (starts immediately; probes at poll
#    interval; the persisted config window additionally drives the crawler-side probe).
#    Secrets travel via stdin assignments (not ssh argv or remote argv) and into the
#    container only via `docker exec -e VAR` passthrough (no secret values in argv).
{
  printf 'AUTH_PASSWORD=%s\n' "$(printf %q "$AUTH_PASSWORD")"
  printf 'WAF_HEADER=%s\n' "$(printf %q "$WAF_HEADER")"
  cat <<'REMOTE'
set -Eeuo pipefail
export AUTH_PASSWORD WAF_HEADER
ARCHIVE_ARG=""
if [ "$ARCHIVE" = "1" ]; then ARCHIVE_ARG="--archive"; fi
docker exec -e AUTH_PASSWORD -e WAF_HEADER "$CONTAINER_NAME" sh -c '
  rm -f "/app/archive/instagram-live/watch-$1.lock"
  mkdir -p /app/archive/instagram-live
  archive_args=""
  if [ "$5" = "1" ]; then archive_args="--archive"; fi
  nohup bun /app/instagram-live-watch.ts "$1" --until "$2" --poll 90 \
    --player-id "$3" --player-name "$4" --live-player-url https://tv.n2nj.moe \
    --auth-username sumie --auth-password "$AUTH_PASSWORD" --waf-header "$WAF_HEADER" \
    --cookie /app/assets/cookies/inscks0318.txt $archive_args \
    >> "/app/archive/instagram-live/watch-$1.log" 2>&1 & echo "watcher-started=$!"
' _ "$HANDLE" "$UNTIL" "$PLAYER_ID" "$PLAYER_NAME" "$ARCHIVE"
AUTH_PASSWORD="$AUTH_PASSWORD" WAF_HEADER="$WAF_HEADER" nohup bash /tmp/instagram-live-monitor.sh --handle "$HANDLE" --until "$UNTIL" \
  --player-id "$PLAYER_ID" --player-name "$PLAYER_NAME" --check-every 15 \
  $ARCHIVE_ARG >> /tmp/ig-live-monitor.log 2>&1 &
echo "monitor-started=$!"
REMOTE
} | ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" \
  "CONTAINER_NAME=$(printf %q "$CONTAINER_NAME") HANDLE=$(printf %q "$HANDLE") UNTIL=$(printf %q "$UNTIL") PLAYER_ID=$(printf %q "$PLAYER_ID") PLAYER_NAME=$(printf %q "$PLAYER_NAME") ARCHIVE=$ARCHIVE bash -s"

# 4) Restart forwarder-new so the persisted schedule takes effect.
ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" 'docker restart forwarder-new >/dev/null && echo forwarder-restarted'

echo "== done: watcher+monitor launched, schedule persisted (restart-proof) =="
