#!/usr/bin/env bash
set -Eeuo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-3000}"
# Notification target for SESSION_BROKEN alerts (falls back to the first QQ
# forward target's group_id from the container config when unset).
NOTIFY_GROUP_ID="${NOTIFY_GROUP_ID:-}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
YOUTUBE_KEEPALIVE_SCRIPT="${YOUTUBE_KEEPALIVE_SCRIPT:-${SCRIPT_DIR}/youtube-cookie-keepalive.sh}"

api_secret="$(docker exec "$CONTAINER_NAME" bun -e 'const fs=require("fs"); const YAML=require("yaml"); const c=YAML.parse(fs.readFileSync("/app/config.yaml","utf8"))||{}; process.stdout.write(String(c.api?.secret||process.env.API_SECRET||""))')"
if [ -z "$api_secret" ]; then
    printf 'cookie-maintenance: API secret unavailable\n' >&2
    exit 1
fi

# Keep the bearer secret out of curl argv (ps-visible on this machine).
auth_header="$(mktemp)"
trap 'rm -f "$auth_header"' EXIT
chmod 600 "$auth_header"
printf 'Authorization: Bearer %s\n' "$api_secret" > "$auth_header"

# First QQ forward target's group_id from the container config — the
# notification channel for SESSION_BROKEN alerts.
default_notify_group() {
    docker exec "$CONTAINER_NAME" bun -e '
        const fs = require("fs")
        const YAML = require("yaml")
        const config = YAML.parse(fs.readFileSync("/app/config.yaml", "utf8")) || {}
        const target = (config.forward_targets || []).find((t) => t && t.platform === "qq")
        const groupId = target?.cfg_platform?.group_id
        process.stdout.write(groupId ? String(groupId) : "")
    ' 2>/dev/null || true
}

notify_session_broken() {
    local summary="$1"
    # Always emit the loud marker line first — it lands in the maintenance log
    # (which docker/host log greps can see) regardless of QQ availability.
    printf 'SESSION_BROKEN %s\n' "$summary" >&2

    local group_id="${NOTIFY_GROUP_ID:-$(default_notify_group)}"
    if [ -z "$group_id" ]; then
        printf 'SESSION_BROKEN notify skipped: no QQ group_id configured\n' >&2
        return 0
    fi
    local response status
    response="$(curl -sS --connect-timeout 5 --max-time 30 -w '\n%{http_code}' -X POST \
        "http://${API_HOST}:${API_PORT}/api/actions/qq/send" \
        -H @"$auth_header" -H 'Content-Type: application/json' \
        --data-binary "$(python3 -c 'import json,sys; print(json.dumps({"group_id": sys.argv[1], "message": sys.argv[2]}))' "$group_id" "SESSION_BROKEN: $summary")" 2>/dev/null || printf '\n000')"
    status="${response##*$'\n'}"
    if [ "$status" != "200" ]; then
        printf 'SESSION_BROKEN notify failed: http=%s\n' "$status" >&2
    fi
}

discover_crawlers() {
    docker exec "$CONTAINER_NAME" bun -e '
        const fs = require("fs")
        const YAML = require("yaml")
        const config = YAML.parse(fs.readFileSync("/app/config.yaml", "utf8")) || {}
        const crawlers = Array.isArray(config.crawlers) ? config.crawlers : []
        const defaults = config.cfg_crawler || {}
        const seen = new Set()
        for (const crawler of crawlers) {
            const name = crawler && crawler.name
            if (!name) continue
            const cfg = crawler.cfg_crawler || {}
            const cookieFile = cfg.cookie_file || defaults.cookie_file
            const sessionProfile = cfg.session_profile || defaults.session_profile
            if (!cookieFile || !sessionProfile) continue
            if (String(cookieFile).endsWith("/ycookies.txt")) continue
            const jarKey = `${cookieFile}\u0000${sessionProfile}`
            if (seen.has(jarKey)) continue
            seen.add(jarKey)
            process.stdout.write(`${name}\n`)
        }
    '
}

sync() {
    local crawler="$1"
    local response status
    response="$(curl -sS --connect-timeout 10 --max-time 300 -w '\n%{http_code}' -X POST "http://${API_HOST}:${API_PORT}/api/cookies/sync" \
        -H @"$auth_header" -H 'Content-Type: application/json' \
        --data-binary "$(python3 -c 'import json,sys; print(json.dumps({"crawlerName": sys.argv[1]}))' "$crawler")")"
    status="${response##*$'\n'}"
    response="${response%$'\n'*}"
    printf '%s http=%s %s\n' "$crawler" "$status" "$response"
    # 409 "missing sessionid"-style failures used to exit quietly for days.
    # Surface every failed sync through the notification channel + a loud
    # SESSION_BROKEN marker line in this log.
    if [ "$status" != "200" ] || printf '%s' "$response" | grep -qi 'missing'; then
        notify_session_broken "cookie sync failed for ${crawler}: http=${status} ${response:0:200}"
    fi
    [ "$status" = "200" ]
}

crawlers=()
while IFS= read -r name; do
    [ -n "$name" ] && crawlers+=("$name")
done < <(discover_crawlers)

if [ "${#crawlers[@]}" -eq 0 ]; then
    printf 'cookie-maintenance: no browser-profile cookie jars found\n' >&2
    exit 1
fi

failures=0
if ! "$YOUTUBE_KEEPALIVE_SCRIPT"; then
    printf 'cookie-maintenance: YouTube cookie keepalive failed\n' >&2
    failures=$((failures + 1))
fi

for crawler in "${crawlers[@]}"; do
    if ! sync "$crawler"; then
        printf 'cookie-maintenance: sync failed for %s\n' "$crawler" >&2
        failures=$((failures + 1))
    fi
done

printf 'cookie-maintenance: attempted=%s failures=%s\n' "${#crawlers[@]}" "$failures"
if [ "$failures" -gt 0 ]; then
    exit 1
fi
