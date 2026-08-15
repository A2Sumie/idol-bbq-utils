#!/usr/bin/env bash
set -Eeuo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-3000}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
YOUTUBE_KEEPALIVE_SCRIPT="${YOUTUBE_KEEPALIVE_SCRIPT:-${SCRIPT_DIR}/youtube-cookie-keepalive.sh}"

api_secret="$(docker exec "$CONTAINER_NAME" bun -e 'const fs=require("fs"); const YAML=require("yaml"); const c=YAML.parse(fs.readFileSync("/app/config.yaml","utf8"))||{}; process.stdout.write(String(c.api?.secret||process.env.API_SECRET||""))')"
if [ -z "$api_secret" ]; then
    printf 'cookie-maintenance: API secret unavailable\n' >&2
    exit 1
fi

auth="Authorization: Bearer $api_secret"

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
        -H "$auth" -H 'Content-Type: application/json' \
        --data-binary "$(python3 -c 'import json,sys; print(json.dumps({"crawlerName": sys.argv[1]}))' "$crawler")")"
    status="${response##*$'\n'}"
    response="${response%$'\n'*}"
    printf '%s http=%s %s\n' "$crawler" "$status" "$response"
    [ "$status" = 200 ]
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
