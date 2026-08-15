#!/usr/bin/env bash
set -Eeuo pipefail

# Migrate X570 StreamServ showroom on-demand events (relay_schedule.json) into the
# unified forwarder live-capture-plan system. Reads the schedule from X570 (or a
# local copy), converts scheduled/pending events into showroom plans and POSTs them
# to the forwarder API; the forwarder's live-capture executor picks them up.
#
# Usage:
#   tools/migrate-x570-showroom-ondemand.sh [--dry-run] [--schedule /path/to/relay_schedule.json]

REMOTE_HOST="${REMOTE_HOST:-3020e}"
FORWARDER_URL="${FORWARDER_URL:-http://3020e:3000}"
WIN_REMOTE="${WIN_REMOTE:-/Users/zou/ytdlp/subPrep/livestr/windows-remote-executor/bin/win-remote}"
SCHEDULE_FILE=""
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --schedule) SCHEDULE_FILE="$2"; shift 2 ;;
    -h|--help) echo "usage: $0 [--dry-run] [--schedule FILE]"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

LOCAL_SCHEDULE="${SCHEDULE_FILE:-/tmp/relay_schedule.json}"
if [ -z "$SCHEDULE_FILE" ]; then
  echo "== fetching relay_schedule.json from X570 =="
  "$WIN_REMOTE" exec x570 --stdin <<'EOF' > "$LOCAL_SCHEDULE"
type D:\StreamServ\relay_schedule.json
EOF
fi

[ -s "$LOCAL_SCHEDULE" ] || { echo "schedule file empty: $LOCAL_SCHEDULE" >&2; exit 1; }

echo "== resolving forwarder API secret from remote config =="
SECRET="$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_HOST" \
  'python3 -c "import yaml,sys; c=yaml.safe_load(open(\"/home/sumie/idol-bbq-utils/assets/config.yaml\")); print(c.get(\"api\",{}).get(\"secret\",\"\"))"')"
[ -n "$SECRET" ] || { echo "no api.secret in remote config" >&2; exit 1; }

echo "== migrating showroom on-demand events =="
ARGS=(--schedule "$LOCAL_SCHEDULE" --forwarder "$FORWARDER_URL")
if [ "$DRY_RUN" = 1 ]; then ARGS+=(--dry-run); fi
FORWARDER_API_SECRET="$SECRET" node "$(dirname "${BASH_SOURCE[0]}")/migrate-x570-showroom-plans.mjs" "${ARGS[@]}"
echo "== done =="
