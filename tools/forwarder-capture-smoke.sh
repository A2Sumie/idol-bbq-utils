#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_HOST="${REMOTE_HOST:-3020e}"
REMOTE_REPO="${REMOTE_REPO:-}"
IMAGE_NAME="${IMAGE_NAME:-idol-bbq-utils-spider:latest}"
CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
CAPTURE_SMOKE_API_PORT="${CAPTURE_SMOKE_API_PORT:-39321}"
CAPTURE_SMOKE_CRAWLER_NAME="${CAPTURE_SMOKE_CRAWLER_NAME:-22/7-角色账号统一列表}"
CAPTURE_SMOKE_PLATFORM="${CAPTURE_SMOKE_PLATFORM:-x}"
CAPTURE_SMOKE_TIMEOUT_SECONDS="${CAPTURE_SMOKE_TIMEOUT_SECONDS:-240}"
CAPTURE_SMOKE_KEEP_ROOT="${CAPTURE_SMOKE_KEEP_ROOT:-1}"
CAPTURE_SMOKE_REQUIRE_PRODUCTION_STOPPED="${CAPTURE_SMOKE_REQUIRE_PRODUCTION_STOPPED:-1}"
CAPTURE_SMOKE_CONTAINER_PREFIX="${CAPTURE_SMOKE_CONTAINER_PREFIX:-idol-bbq-capture-smoke}"
CAPTURE_SMOKE_ROOT="${CAPTURE_SMOKE_ROOT:-}"
CAPTURE_SMOKE_TARGET_IDS="${CAPTURE_SMOKE_TARGET_IDS:-}"

if [ -n "${SSH_OPTS:-}" ]; then
    # shellcheck disable=SC2206
    SSH_ARGS=(${SSH_OPTS})
else
    SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout=10)
fi

remote_env_prefix() {
    local name value
    for name in REMOTE_REPO IMAGE_NAME CONTAINER_NAME CAPTURE_SMOKE_API_PORT CAPTURE_SMOKE_CRAWLER_NAME CAPTURE_SMOKE_PLATFORM CAPTURE_SMOKE_TIMEOUT_SECONDS CAPTURE_SMOKE_KEEP_ROOT CAPTURE_SMOKE_REQUIRE_PRODUCTION_STOPPED CAPTURE_SMOKE_CONTAINER_PREFIX CAPTURE_SMOKE_ROOT CAPTURE_SMOKE_TARGET_IDS; do
        value="${!name:-}"
        if [ -n "$value" ]; then
            printf '%s=%q ' "$name" "$value"
        fi
    done
}

main() {
    if [ "${1:-}" = "--help" ]; then
        cat <<'HELP'
Usage: tools/forwarder-capture-smoke.sh

Runs a remote capture-mode smoke trial without starting the production
forwarder container. The script creates a one-shot container from the deployed
image, copies the production DB to a temporary DB, mounts production config and
cookies read-only, rewrites only the temporary API secret/port and schedules,
simulates one fresh article, and asserts that all attempted outbound sends are
captured/dry-run records.

Environment:
  REMOTE_HOST=3020e
  REMOTE_REPO=                       # defaults remotely to $HOME/idol-bbq-utils
  IMAGE_NAME=idol-bbq-utils-spider:latest
  CONTAINER_NAME=forwarder-new
  CAPTURE_SMOKE_API_PORT=39321
  CAPTURE_SMOKE_CRAWLER_NAME=22/7-角色账号统一列表
  CAPTURE_SMOKE_PLATFORM=x
  CAPTURE_SMOKE_TIMEOUT_SECONDS=240
  CAPTURE_SMOKE_KEEP_ROOT=1          # keep no-secret evidence root
  CAPTURE_SMOKE_REQUIRE_PRODUCTION_STOPPED=1
  CAPTURE_SMOKE_CONTAINER_PREFIX=idol-bbq-capture-smoke
  CAPTURE_SMOKE_ROOT=                # optional remote evidence root
  CAPTURE_SMOKE_TARGET_IDS=          # optional comma-separated target ids/names/group ids

The temporary API secret is never printed. Secret-bearing temporary files are
removed before exit. Production sends remain blocked by
IDOL_BBQ_OUTBOUND_SEND_MODE=capture inside the one-shot container.
HELP
        return
    fi

    git rev-parse --is-inside-work-tree >/dev/null

    local env_prefix
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s" <<'REMOTE'
set -Eeuo pipefail

die() {
    printf 'forwarder-capture-smoke: %s\n' "$*" >&2
    exit 1
}

bool_lower() {
    case "${1:-}" in
        true | True | TRUE | 1 | yes | YES)
            printf 'true\n'
            ;;
        *)
            printf 'false\n'
            ;;
    esac
}

sqlite_backup() {
    local source_path="$1" backup_path="$2"
    python3 - "$source_path" "$backup_path" <<'PY'
import sqlite3
import sys
from urllib.request import pathname2url

source_path, backup_path = sys.argv[1:3]
source_uri = "file:" + pathname2url(source_path) + "?mode=ro"
source = sqlite3.connect(source_uri, uri=True)
try:
    backup = sqlite3.connect(backup_path)
    try:
        source.backup(backup)
    finally:
        backup.close()
finally:
    source.close()
PY
}

prepare_temp_db() {
    local db_path="$1" summary_path="$2"
    python3 - "$db_path" "$summary_path" <<'PY'
import sqlite3
import sys
import time

db_path, summary_path = sys.argv[1:3]
now = int(time.time())
connection = sqlite3.connect(db_path)
try:
    quick = connection.execute("PRAGMA quick_check").fetchone()
    if not quick or quick[0] != "ok":
        raise SystemExit(f"quick_check before prepare failed: {quick[0] if quick else 'no result'}")

    tables = {
        row[0]
        for row in connection.execute("select name from sqlite_master where type='table'").fetchall()
    }
    suppressed_tasks = 0
    suppressed_windows = 0
    if "task_queue" in tables:
        cursor = connection.execute(
            """
            update task_queue
               set status='cancelled',
                   updated_at=?,
                   finished_at=?,
                   last_error=coalesce(last_error, 'capture smoke temp db suppression'),
                   result_summary='capture smoke temp db suppression'
             where status in ('pending', 'processing')
            """,
            (now, now),
        )
        suppressed_tasks = cursor.rowcount
    if "aggregation_windows" in tables:
        cursor = connection.execute(
            """
            update aggregation_windows
               set status='cancelled',
                   updated_at=?,
                   finished_at=?,
                   payload_hash=coalesce(payload_hash, 'capture-smoke-suppressed-open-window')
             where status='open'
            """,
            (now, now),
        )
        suppressed_windows = cursor.rowcount
    connection.commit()

    quick = connection.execute("PRAGMA quick_check").fetchone()
    if not quick or quick[0] != "ok":
        raise SystemExit(f"quick_check after prepare failed: {quick[0] if quick else 'no result'}")
finally:
    connection.close()

with open(summary_path, "w", encoding="utf-8") as handle:
    handle.write(f"suppressed_task_count={suppressed_tasks}\n")
    handle.write(f"suppressed_open_aggregation_window_count={suppressed_windows}\n")
    handle.write("temp_db_quick_check=ok\n")
PY
}

write_temp_config() {
    local repo="$1" root="$2" image="$3" api_port="$4" api_secret="$5" target_filter="$6"
    docker run --rm --pull never \
        --entrypoint bun \
        --user "$(id -u):$(id -g)" \
        -e HOME=/tmp \
        -e TEMP_API_PORT="$api_port" \
        -e TEMP_API_SECRET="$api_secret" \
        -e TEMP_TARGET_IDS="$target_filter" \
        -v "$repo/assets/config.yaml:/app/config.yaml:ro" \
        -v "$root:/out" \
        "$image" \
        --eval '
import { readFileSync, writeFileSync, chmodSync } from "node:fs"
import YAML from "yaml"

const quietCron = "0 0 0 1 1 *"
const config = YAML.parse(readFileSync("/app/config.yaml", "utf8"))
const targetFilter = new Set(
    String(process.env.TEMP_TARGET_IDS || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
)
config.api = {
    ...(config.api || {}),
    port: Number(process.env.TEMP_API_PORT),
    secret: process.env.TEMP_API_SECRET,
}

function quietAggregation(value) {
    if (value && typeof value === "object") {
        value.cron = quietCron
    }
}

config.cfg_crawler = {
    ...(config.cfg_crawler || {}),
    cron: quietCron,
}
quietAggregation(config.cfg_crawler.aggregation)
for (const crawler of config.crawlers || []) {
    crawler.cfg_crawler = {
        ...(crawler.cfg_crawler || {}),
        cron: quietCron,
    }
    quietAggregation(crawler.cfg_crawler.aggregation)
}

config.cfg_forwarder = {
    ...(config.cfg_forwarder || {}),
    cron: quietCron,
    aggregation_cron: quietCron,
    batch_cron: quietCron,
}
for (const forwarder of config.forwarders || []) {
    forwarder.cfg_forwarder = {
        ...(forwarder.cfg_forwarder || {}),
        cron: quietCron,
        aggregation_cron: quietCron,
        batch_cron: quietCron,
    }
}

function targetIdentityValues(target) {
    const cfg = target?.cfg_platform || {}
    return [
        target?.id,
        target?.name,
        target?.group,
        cfg?.group_id,
        cfg?.group,
        cfg?.target_id,
        cfg?.id,
    ]
        .map((value) => String(value ?? "").trim())
        .filter(Boolean)
}

function subscriberId(subscriber) {
    if (typeof subscriber === "string") return subscriber
    if (subscriber && typeof subscriber === "object") return String(subscriber.id || subscriber.name || "").trim()
    return ""
}

let allowedTargetIds = []
if (targetFilter.size > 0) {
    const keptTargets = (config.forward_targets || []).filter((target) =>
        targetIdentityValues(target).some((identity) => targetFilter.has(identity)),
    )
    if (keptTargets.length === 0) {
        throw new Error("CAPTURE_SMOKE_TARGET_IDS did not match any forward target")
    }

    allowedTargetIds = keptTargets.map((target, index) => String(target.id || target.name || `target-${index}`).trim())
    const allowedSet = new Set(allowedTargetIds)
    config.forward_targets = keptTargets

    const connections = config.connections || {}
    for (const connectionName of ["formatter-target", "forwarder-target"]) {
        const connection = connections[connectionName]
        if (!connection || typeof connection !== "object") continue
        for (const [nodeId, subscribers] of Object.entries(connection)) {
            if (Array.isArray(subscribers)) {
                connection[nodeId] = subscribers.filter((subscriber) => allowedSet.has(subscriberId(subscriber)))
            }
        }
    }

    for (const forwarder of config.forwarders || []) {
        if (!Array.isArray(forwarder.subscribers)) continue
        forwarder.subscribers = forwarder.subscribers.filter((subscriber) => allowedSet.has(subscriberId(subscriber)))
    }
}

writeFileSync("/out/allowed-targets.txt", `${allowedTargetIds.join("\n")}${allowedTargetIds.length ? "\n" : ""}`, {
    mode: 0o600,
})
writeFileSync("/out/config.yaml", YAML.stringify(config), { mode: 0o600 })
chmodSync("/out/config.yaml", 0o600)
chmodSync("/out/allowed-targets.txt", 0o600)
'
}

probe_port_free() {
    local port="$1"
    python3 - "$port" <<'PY'
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", port))
finally:
    sock.close()
PY
}

count_capture_and_outbound() {
    local capture_file="$1" db_path="$2" article_id="$3" allowed_target_ids="$4" metrics_path="$5"
    python3 - "$capture_file" "$db_path" "$article_id" "$allowed_target_ids" "$metrics_path" <<'PY'
import json
import os
import sqlite3
import sys

capture_file, db_path, article_id, allowed_target_ids_raw, metrics_path = sys.argv[1:6]
allowed_target_ids = {item.strip() for item in allowed_target_ids_raw.split(",") if item.strip()}

capture_count = 0
matching_capture_count = 0
malformed_capture_count = 0
disallowed_capture_count = 0
captured_target_ids = set()
if os.path.exists(capture_file):
    with open(capture_file, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            capture_count += 1
            try:
                payload = json.loads(line)
            except Exception:
                malformed_capture_count += 1
                continue
            article = payload.get("article") if isinstance(payload, dict) else None
            article_key = str(payload.get("article_key") or "") if isinstance(payload, dict) else ""
            outbound_key = str(payload.get("outbound_key") or "") if isinstance(payload, dict) else ""
            if (
                isinstance(article, dict)
                and str(article.get("a_id") or "") == article_id
                or article_id in article_key
                or article_id in outbound_key
            ):
                matching_capture_count += 1
                target_id = str(payload.get("target_id") or "") if isinstance(payload, dict) else ""
                if target_id:
                    captured_target_ids.add(target_id)
                if allowed_target_ids and target_id not in allowed_target_ids:
                    disallowed_capture_count += 1

status_counts = {}
matching_outbound_target_ids = set()
disallowed_outbound_count = 0
dry_run_count = 0
matching_outbound_count = 0
connection = sqlite3.connect(db_path)
try:
    rows = connection.execute(
        """
        select coalesce(target_id, ''), status, count(*)
          from outbound_messages
         where idempotency_key like ?
            or coalesce(article_key, '') like ?
            or coalesce(synthetic_key, '') like ?
         group by target_id, status
         order by target_id, status
        """,
        (f"%{article_id}%", f"%{article_id}%", f"%{article_id}%"),
    ).fetchall()
finally:
    connection.close()

for target_id, status, count in rows:
    target_id = str(target_id or "")
    if target_id:
        matching_outbound_target_ids.add(target_id)
    if allowed_target_ids and target_id not in allowed_target_ids:
        disallowed_outbound_count += int(count)
    status_counts[str(status)] = status_counts.get(str(status), 0) + int(count)
    matching_outbound_count += int(count)
dry_run_count = int(status_counts.get("dry_run", 0))

with open(metrics_path, "w", encoding="utf-8") as handle:
    handle.write(f"capture_count={capture_count}\n")
    handle.write(f"matching_capture_count={matching_capture_count}\n")
    handle.write(f"unmatched_capture_count={capture_count - matching_capture_count}\n")
    handle.write(f"malformed_capture_count={malformed_capture_count}\n")
    handle.write(f"matching_outbound_count={matching_outbound_count}\n")
    handle.write(f"dry_run_count={dry_run_count}\n")
    handle.write(f"disallowed_capture_count={disallowed_capture_count}\n")
    handle.write(f"disallowed_outbound_count={disallowed_outbound_count}\n")
    handle.write(f"captured_target_ids={','.join(sorted(captured_target_ids))}\n")
    handle.write(f"matching_outbound_target_ids={','.join(sorted(matching_outbound_target_ids))}\n")
    handle.write(
        "outbound_status_counts="
        + ",".join(f"{key}:{value}" for key, value in sorted(status_counts.items()))
        + "\n"
    )
PY
}

repo="${REMOTE_REPO:-$HOME/idol-bbq-utils}"
image="${IMAGE_NAME:-idol-bbq-utils-spider:latest}"
prod_container="${CONTAINER_NAME:-forwarder-new}"
api_port="${CAPTURE_SMOKE_API_PORT:-39321}"
crawler_name="${CAPTURE_SMOKE_CRAWLER_NAME:-22/7-角色账号统一列表}"
platform="${CAPTURE_SMOKE_PLATFORM:-x}"
timeout_seconds="${CAPTURE_SMOKE_TIMEOUT_SECONDS:-240}"
keep_root="$(bool_lower "${CAPTURE_SMOKE_KEEP_ROOT:-1}")"
require_production_stopped="$(bool_lower "${CAPTURE_SMOKE_REQUIRE_PRODUCTION_STOPPED:-1}")"
prefix="${CAPTURE_SMOKE_CONTAINER_PREFIX:-idol-bbq-capture-smoke}"
target_filter="${CAPTURE_SMOKE_TARGET_IDS:-}"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
root="${CAPTURE_SMOKE_ROOT:-/tmp/idol-bbq-capture-smoke-$stamp}"
smoke_container="$prefix-$stamp"
tmp_root="$root/tmp"
tmp_config="$root/config.yaml"
allowed_targets_file="$root/allowed-targets.txt"
tmp_db="$root/refactor.db"
capture_file="$tmp_root/outbound-capture.jsonl"
log_file="$root/container.log"
auth_header="$root/api-auth-header"
payload_file="$root/simulate-payload.json"
response_file="$root/simulate-response.json"
status_file="$root/runtime-status.json"
metrics_file="$root/metrics.env"
db_prepare_file="$root/db-prepare.env"
article_id="capture-smoke-$stamp"
api_secret=""
allowed_target_ids=""
cleanup_started="false"

cleanup() {
    local status="$?"
    if [ "$cleanup_started" = "false" ]; then
        cleanup_started="true"
        if docker inspect "$smoke_container" >/dev/null 2>&1; then
            docker logs "$smoke_container" > "$log_file" 2>&1 || true
            docker rm -f "$smoke_container" >/dev/null 2>&1 || true
        fi
        rm -f "$tmp_config" "$auth_header"
        if [ "$keep_root" != "true" ] && [ "$status" = "0" ]; then
            rm -rf "$root"
        fi
    fi
    exit "$status"
}
trap cleanup EXIT

[ -d "$repo" ] || die "remote repo missing: $repo"
[ -f "$repo/assets/config.yaml" ] || die "remote config missing: $repo/assets/config.yaml"
[ -f "$repo/assets/refactor.db" ] || die "remote DB missing: $repo/assets/refactor.db"
docker image inspect "$image" >/dev/null || die "docker image missing: $image"
probe_port_free "$api_port" || die "api port is not free on 127.0.0.1:$api_port"

prod_before_status="$(docker inspect "$prod_container" --format '{{.State.Status}}' 2>/dev/null || printf 'missing')"
prod_before_running="$(docker inspect "$prod_container" --format '{{.State.Running}}' 2>/dev/null || printf 'missing')"
prod_before_restart="$(docker inspect "$prod_container" --format '{{.HostConfig.RestartPolicy.Name}}' 2>/dev/null || printf 'missing')"
if [ "$require_production_stopped" = "true" ] && [ "$prod_before_running" != "false" ]; then
    die "production container is not stopped before smoke: status=$prod_before_status running=$prod_before_running"
fi

mkdir -p "$root" "$tmp_root"
chmod 700 "$root" "$tmp_root"
sqlite_backup "$repo/assets/refactor.db" "$tmp_db"
prepare_temp_db "$tmp_db" "$db_prepare_file"

api_secret="$(python3 - <<'PY'
import secrets
print("capture-smoke-" + secrets.token_urlsafe(24))
PY
)"
write_temp_config "$repo" "$root" "$image" "$api_port" "$api_secret" "$target_filter"
if [ -f "$allowed_targets_file" ]; then
    allowed_target_ids="$(tr '\n' ',' < "$allowed_targets_file" | sed 's/,$//')"
fi
if [ -n "$target_filter" ] && [ -z "$allowed_target_ids" ]; then
    die "target filter matched no allowed target ids"
fi
printf 'Authorization: Bearer %s\n' "$api_secret" > "$auth_header"
chmod 600 "$auth_header"

mkdir -p "$tmp_root/logs" "$tmp_root/media" "$tmp_root/browser-profiles" "$tmp_root/instagram-live" "$root/backups"
docker run -d --pull never \
    --name "$smoke_container" \
    --network host \
    -e TZ=Asia/Tokyo \
    -e LOG_LEVEL=info \
    -e IDOL_BBQ_RUNTIME_MODE=online \
    -e IDOL_BBQ_OUTBOUND_SEND_MODE=capture \
    -e IDOL_BBQ_OUTBOUND_CAPTURE_FILE=/tmp/tweet-forwarder/outbound-capture.jsonl \
    -e IDOL_BBQ_RUN_MIGRATIONS=0 \
    -e IDOL_BBQ_REFRESH_MEDIA_TOOLS=0 \
    -e IDOL_BBQ_REQUIRE_EXISTING_DB_FOR_MIGRATION=1 \
    -e BROWSER_PROFILE_DIR=/tmp/tweet-forwarder/browser-profiles \
    -e CACHE_DIR=/tmp/tweet-forwarder \
    -e NO_SANDBOX=1 \
    -v "$tmp_config:/app/config.yaml:ro" \
    -v "$tmp_db:/app/data.db:rw" \
    -v "$repo/assets/cookies:/app/assets/cookies:ro" \
    -v "$tmp_root:/tmp/tweet-forwarder:rw" \
    -v "$root/backups:/app/backups:rw" \
    "$image" >/dev/null

smoke_send_mode="$(docker inspect "$smoke_container" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' | awk -F= '$1 == "IDOL_BBQ_OUTBOUND_SEND_MODE" { print $2; found=1 } END { if (!found) print "" }')"
smoke_runtime_mode="$(docker inspect "$smoke_container" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' | awk -F= '$1 == "IDOL_BBQ_RUNTIME_MODE" { print $2; found=1 } END { if (!found) print "" }')"
[ "$smoke_runtime_mode" = "online" ] || die "smoke runtime mode mismatch: $smoke_runtime_mode"
[ "$smoke_send_mode" = "capture" ] || die "smoke outbound send mode mismatch: $smoke_send_mode"

ready="false"
for _ in $(seq 1 "$timeout_seconds"); do
    http_code="$(curl -sS -o "$status_file" -w '%{http_code}' -H @"$auth_header" "http://127.0.0.1:$api_port/api/runtime/status" 2>/dev/null || true)"
    if [ "$http_code" = "200" ] && python3 - "$status_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
if payload.get("runtime", {}).get("mode") != "online":
    raise SystemExit(1)
PY
    then
        ready="true"
        break
    fi
    sleep 1
done
[ "$ready" = "true" ] || die "temporary capture smoke API did not become ready"

python3 - "$payload_file" "$crawler_name" "$platform" "$article_id" <<'PY'
import json
import sys
import time

payload_path, crawler_name, platform, article_id = sys.argv[1:5]
payload = {
    "crawlerName": crawler_name,
    "platform": platform,
    "a_id": article_id,
    "u_id": "capture_smoke",
    "username": "capture_smoke",
    "content": f"[capture smoke] {article_id}",
    "url": f"https://x.com/capture_smoke/status/{article_id}",
    "created_at": int(time.time()),
    "forwardAfterSave": True,
}
with open(payload_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, ensure_ascii=False)
PY

simulate_http_code="$(
    curl -sS -o "$response_file" -w '%{http_code}' \
        -H @"$auth_header" \
        -H 'Content-Type: application/json' \
        --data-binary @"$payload_file" \
        "http://127.0.0.1:$api_port/api/actions/articles/simulate" 2>/dev/null || true
)"
if [ "$simulate_http_code" != "200" ]; then
    die "article simulate failed: http_code=$simulate_http_code response_file=$response_file"
fi
python3 - "$response_file" "$article_id" <<'PY'
import json
import sys

response_path, article_id = sys.argv[1:3]
with open(response_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
if payload.get("success") is not True or payload.get("forwarded") is not True:
    raise SystemExit("simulate response did not report success+forwarded")
article = payload.get("article") or {}
if str(article.get("a_id") or "") != article_id:
    raise SystemExit("simulate response article id mismatch")
PY

for _ in $(seq 1 "$timeout_seconds"); do
    count_capture_and_outbound "$capture_file" "$tmp_db" "$article_id" "$allowed_target_ids" "$metrics_file"
    # shellcheck disable=SC1090
    . "$metrics_file"
    if [ "${matching_capture_count:-0}" -gt 0 ] && [ "${dry_run_count:-0}" -gt 0 ]; then
        break
    fi
    sleep 1
done

# shellcheck disable=SC1090
. "$metrics_file"
[ "${capture_count:-0}" -gt 0 ] || die "no capture records were written"
[ "${matching_capture_count:-0}" -gt 0 ] || die "no capture record matched smoke article"
[ "${unmatched_capture_count:-0}" = "0" ] || die "unexpected non-smoke capture records: $unmatched_capture_count"
[ "${malformed_capture_count:-0}" = "0" ] || die "malformed capture records: $malformed_capture_count"
[ "${dry_run_count:-0}" -gt 0 ] || die "no matching dry_run outbound records were written"
if [ -n "$allowed_target_ids" ]; then
    [ "${captured_target_ids:-}" != "" ] || die "target allowlist produced no captured target ids"
    [ "${disallowed_capture_count:-0}" = "0" ] || die "capture records escaped target allowlist: $disallowed_capture_count"
    [ "${disallowed_outbound_count:-0}" = "0" ] || die "outbound records escaped target allowlist: $disallowed_outbound_count"
fi

docker logs "$smoke_container" > "$log_file" 2>&1 || true
docker stop -t 30 "$smoke_container" >/dev/null || true
docker rm "$smoke_container" >/dev/null || true
rm -f "$tmp_config" "$auth_header"

prod_after_status="$(docker inspect "$prod_container" --format '{{.State.Status}}' 2>/dev/null || printf 'missing')"
prod_after_running="$(docker inspect "$prod_container" --format '{{.State.Running}}' 2>/dev/null || printf 'missing')"
prod_after_restart="$(docker inspect "$prod_container" --format '{{.HostConfig.RestartPolicy.Name}}' 2>/dev/null || printf 'missing')"
if [ "$require_production_stopped" = "true" ] && [ "$prod_after_running" != "false" ]; then
    die "production container is not stopped after smoke: status=$prod_after_status running=$prod_after_running"
fi

leftover_current_count="$(docker ps -a --filter "name=^/$smoke_container$" --format '{{.Names}}' | wc -l | tr -d ' ')"
[ "$leftover_current_count" = "0" ] || die "temporary smoke container was not removed: $smoke_container"
secret_artifacts_removed="false"
if [ ! -f "$tmp_config" ] && [ ! -f "$auth_header" ]; then
    secret_artifacts_removed="true"
fi

printf 'capture_smoke_ok=true\n'
printf 'production_before_status=%s\n' "$prod_before_status"
printf 'production_before_running=%s\n' "$prod_before_running"
printf 'production_before_restart=%s\n' "$prod_before_restart"
printf 'production_after_status=%s\n' "$prod_after_status"
printf 'production_after_running=%s\n' "$prod_after_running"
printf 'production_after_restart=%s\n' "$prod_after_restart"
printf 'smoke_container=%s\n' "$smoke_container"
printf 'smoke_runtime_mode=%s\n' "$smoke_runtime_mode"
printf 'smoke_outbound_send_mode=%s\n' "$smoke_send_mode"
printf 'smoke_container_removed=true\n'
printf 'secret_artifacts_removed=%s\n' "$secret_artifacts_removed"
printf 'evidence_root=%s\n' "$root"
printf 'article_id=%s\n' "$article_id"
printf 'crawler_name=%s\n' "$crawler_name"
printf 'target_filter=%s\n' "$target_filter"
printf 'allowed_target_ids=%s\n' "$allowed_target_ids"
printf 'simulate_http_code=%s\n' "$simulate_http_code"
printf 'capture_file=%s\n' "$capture_file"
printf 'capture_count=%s\n' "$capture_count"
printf 'matching_capture_count=%s\n' "$matching_capture_count"
printf 'unmatched_capture_count=%s\n' "$unmatched_capture_count"
printf 'matching_outbound_count=%s\n' "$matching_outbound_count"
printf 'dry_run_count=%s\n' "$dry_run_count"
printf 'captured_target_ids=%s\n' "${captured_target_ids:-}"
printf 'matching_outbound_target_ids=%s\n' "${matching_outbound_target_ids:-}"
printf 'disallowed_capture_count=%s\n' "${disallowed_capture_count:-0}"
printf 'disallowed_outbound_count=%s\n' "${disallowed_outbound_count:-0}"
printf 'outbound_status_counts=%s\n' "${outbound_status_counts:-}"
printf 'db_prepare_summary=%s\n' "$db_prepare_file"
printf 'container_log=%s\n' "$log_file"
REMOTE
}

main "$@"
