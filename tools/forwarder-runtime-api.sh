#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_HOST="${REMOTE_HOST:-3020e}"
CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
API_HOST="${API_HOST:-127.0.0.1}"

if [ -n "${SSH_OPTS:-}" ]; then
    # shellcheck disable=SC2206
    SSH_ARGS=(${SSH_OPTS})
else
    SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout=10)
fi

usage() {
    cat <<'HELP'
Usage:
  tools/forwarder-runtime-api.sh status
  tools/forwarder-runtime-api.sh manifest
  tools/forwarder-runtime-api.sh reload
  tools/forwarder-runtime-api.sh run-crawler <crawler-name> [--website URL ...]

Environment:
  REMOTE_HOST=3020e
  CONTAINER_NAME=forwarder-new
  API_HOST=127.0.0.1

The script reads the API port and secret from /app/config.yaml inside the
container and never prints the secret.
HELP
}

if [ "${1:-}" = "--help" ] || [ $# -lt 1 ]; then
    usage
    exit 0
fi

command_name="$1"
shift

remote_env_prefix() {
    local name value
    for name in CONTAINER_NAME API_HOST COMMAND_NAME CRAWLER_NAME WEBSITES_JSON; do
        value="${!name:-}"
        if [ -n "$value" ]; then
            printf '%s=%q ' "$name" "$value"
        fi
    done
}

WEBSITES_JSON=""
CRAWLER_NAME=""
case "$command_name" in
    status|manifest|reload)
        if [ $# -ne 0 ]; then
            usage >&2
            exit 2
        fi
        ;;
    run-crawler)
        if [ $# -lt 1 ]; then
            usage >&2
            exit 2
        fi
        CRAWLER_NAME="$1"
        shift
        websites=()
        while [ $# -gt 0 ]; do
            case "$1" in
                --website)
                    [ $# -ge 2 ] || {
                        printf 'missing URL after --website\n' >&2
                        exit 2
                    }
                    websites+=("$2")
                    shift 2
                    ;;
                *)
                    printf 'unknown argument: %s\n' "$1" >&2
                    exit 2
                    ;;
            esac
        done
        if [ "${#websites[@]}" -gt 0 ]; then
            WEBSITES_JSON="$(python3 - "${websites[@]}" <<'PY'
import json
import sys
print(json.dumps(sys.argv[1:], ensure_ascii=False))
PY
)"
        fi
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac

COMMAND_NAME="$command_name"
env_prefix="$(remote_env_prefix)"
ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s" <<'REMOTE'
set -Eeuo pipefail

read_api_config() {
    docker exec "$CONTAINER_NAME" bun -e '
        const fs = require("fs")
        const YAML = require("yaml")
        const config = YAML.parse(fs.readFileSync("/app/config.yaml", "utf8")) || {}
        const port = Number(config.api?.port || 3000)
        const secret = String(config.api?.secret || process.env.API_SECRET || "")
        if (!secret) {
            process.exit(7)
        }
        console.log(JSON.stringify({ port, secret }))
    '
}

api_config="$(read_api_config)" || {
    printf 'failed to read API config from container %s\n' "$CONTAINER_NAME" >&2
    exit 1
}
api_port="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["port"])' <<<"$api_config")"
api_secret="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["secret"])' <<<"$api_config")"
api_base="http://${API_HOST}:${api_port}"
auth_header="$(mktemp)"
body_file=""
response_file="$(mktemp)"
cleanup() {
    rm -f "$auth_header" "$response_file"
    if [ -n "$body_file" ]; then
        rm -f "$body_file"
    fi
}
trap cleanup EXIT
chmod 600 "$auth_header"
printf 'Authorization: Bearer %s\n' "$api_secret" > "$auth_header"

pretty_json() {
    python3 -m json.tool 2>/dev/null || cat
}

api_request() {
    local method="$1"
    local path="$2"
    local body="${3:-}"
    local curl_args=(-sS -o "$response_file" -w '%{http_code}' -X "$method" -H @"$auth_header")
    if [ -n "$body" ]; then
        body_file="$(mktemp)"
        chmod 600 "$body_file"
        printf '%s' "$body" > "$body_file"
        curl_args+=(-H 'Content-Type: application/json' --data-binary @"$body_file")
    fi
    local http_code
    http_code="$(curl "${curl_args[@]}" "${api_base}${path}" 2>/dev/null || true)"
    printf 'http_code=%s\n' "$http_code" >&2
    if ! [[ "$http_code" =~ ^[0-9]+$ ]]; then
        printf 'API request failed before receiving an HTTP status\n' >&2
        exit 1
    fi
    if [ "$http_code" -lt 200 ] || [ "$http_code" -ge 300 ]; then
        cat "$response_file" >&2
        printf '\n' >&2
        exit 1
    fi
    cat "$response_file" | pretty_json
}

case "$COMMAND_NAME" in
    status)
        api_request GET /api/runtime/status
        ;;
    manifest)
        api_request GET /api/runtime/manifest
        ;;
    reload)
        printf 'reload:\n' >&2
        api_request POST /api/runtime/reload
        printf '\nstatus:\n' >&2
        api_request GET /api/runtime/status
        ;;
    run-crawler)
        body="$(python3 - "$CRAWLER_NAME" "$WEBSITES_JSON" <<'PY'
import json
import sys
crawler = sys.argv[1]
websites = json.loads(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else []
payload = {"name": crawler}
if websites:
    payload["websites"] = websites
print(json.dumps(payload, ensure_ascii=False))
PY
)"
        api_request POST /api/actions/crawlers/run "$body"
        ;;
esac
REMOTE
