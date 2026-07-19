#!/usr/bin/env bash
set -Eeuo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-3000}"

api_secret="$(docker exec "$CONTAINER_NAME" bun -e 'const fs=require("fs"); const YAML=require("yaml"); const c=YAML.parse(fs.readFileSync("/app/config.yaml","utf8"))||{}; process.stdout.write(String(c.api?.secret||process.env.API_SECRET||""))')"
if [ -z "$api_secret" ]; then
    printf 'cookie-maintenance: API secret unavailable\n' >&2
    exit 1
fi

auth="Authorization: Bearer $api_secret"
sync() {
    local crawler="$1"
    local response status
    response="$(curl -sS -w '\n%{http_code}' -X POST "http://${API_HOST}:${API_PORT}/api/cookies/sync" \
        -H "$auth" -H 'Content-Type: application/json' \
        --data-binary "$(python3 -c 'import json,sys; print(json.dumps({"crawlerName": sys.argv[1]}))' "$crawler")")"
    status="${response##*$'\n'}"
    response="${response%$'\n'*}"
    printf '%s http=%s %s\n' "$crawler" "$status" "$response"
    [ "$status" = 200 ]
}

sync 'Instagram抓取 - 高频时段'
sync 'Tiktok抓取'
