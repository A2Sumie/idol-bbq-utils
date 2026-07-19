#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_HOST="${REMOTE_HOST:-3020e}"
REMOTE_REPO="${REMOTE_REPO:-}"
IMAGE_NAME="${IMAGE_NAME:-idol-bbq-utils-spider:latest}"
CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
EXPECTED_COMMIT="${EXPECTED_COMMIT:-$(git rev-parse HEAD 2>/dev/null || true)}"
EXPECTED_RUNTIME_MODE="${EXPECTED_RUNTIME_MODE:-offline}"
EXPECTED_OUTBOUND_SEND_MODE="${EXPECTED_OUTBOUND_SEND_MODE:-}"
EXPECTED_RUNNING="${EXPECTED_RUNNING:-false}"
EXPECTED_RESTART_POLICY="${EXPECTED_RESTART_POLICY:-no}"
EXPECTED_STOP_TIMEOUT_SECONDS="${EXPECTED_STOP_TIMEOUT_SECONDS:-90}"
STRICT_MIGRATIONS="${STRICT_MIGRATIONS:-0}"
STRICT_COMMIT="${STRICT_COMMIT:-0}"
STRICT_CONFIG_SHA256="${STRICT_CONFIG_SHA256:-1}"
STRICT_PROCESSOR_ENV="${STRICT_PROCESSOR_ENV:-1}"
LOCAL_CONFIG_PATH="${LOCAL_CONFIG_PATH:-assets/config.yaml}"
EXPECTED_CONFIG_SHA256="${EXPECTED_CONFIG_SHA256:-}"

die() {
    printf 'forwarder-preflight: %s\n' "$*" >&2
    exit 1
}

if [ -n "${SSH_OPTS:-}" ]; then
    # shellcheck disable=SC2206
    SSH_ARGS=(${SSH_OPTS})
else
    SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout=10)
fi

remote_env_prefix() {
    local name value
    for name in REMOTE_REPO IMAGE_NAME CONTAINER_NAME EXPECTED_COMMIT EXPECTED_RUNTIME_MODE EXPECTED_OUTBOUND_SEND_MODE EXPECTED_RUNNING EXPECTED_RESTART_POLICY EXPECTED_STOP_TIMEOUT_SECONDS STRICT_MIGRATIONS STRICT_COMMIT STRICT_CONFIG_SHA256 STRICT_PROCESSOR_ENV EXPECTED_CONFIG_SHA256; do
        value="${!name:-}"
        if [ -n "$value" ]; then
            printf '%s=%q ' "$name" "$value"
        fi
    done
}

main() {
    if [ "${1:-}" = "--help" ]; then
        cat <<'HELP'
Usage: tools/forwarder-preflight.sh

Prints a no-secret remote preflight summary for the stopped forwarder container:
container status, restart policy, image id, image build commit, config audit
hashes/counts, migration head, and remote dirty worktree counts.

Environment:
  REMOTE_HOST=3020e
  REMOTE_REPO=            # defaults remotely to $HOME/idol-bbq-utils
  IMAGE_NAME=idol-bbq-utils-spider:latest
  CONTAINER_NAME=forwarder-new
  EXPECTED_COMMIT=<local HEAD>
  EXPECTED_RUNTIME_MODE=offline
  EXPECTED_OUTBOUND_SEND_MODE=  # set to blocked/live to assert the send exit guard
  EXPECTED_RUNNING=false
  EXPECTED_RESTART_POLICY=no
  EXPECTED_STOP_TIMEOUT_SECONDS=90
  STRICT_MIGRATIONS=0      # set 1 to fail when DB migrations are pending/failed
  STRICT_COMMIT=0         # set 1 to fail when image commit != expected commit
  STRICT_CONFIG_SHA256=1   # fail when remote mounted config differs from local/expected config
  STRICT_PROCESSOR_ENV=1   # fail when env: processor keys are absent from the container
  LOCAL_CONFIG_PATH=assets/config.yaml
  EXPECTED_CONFIG_SHA256=  # overrides the local config hash when set
HELP
        return
    fi

    if [ -z "$EXPECTED_CONFIG_SHA256" ]; then
        if [ ! -f "$LOCAL_CONFIG_PATH" ]; then
            die "local config not found: $LOCAL_CONFIG_PATH"
        fi
        EXPECTED_CONFIG_SHA256="$(shasum -a 256 "$LOCAL_CONFIG_PATH" | awk '{ print $1 }')"
    fi

    local env_prefix
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s" <<'REMOTE'
set -euo pipefail
repo="${REMOTE_REPO:-$HOME/idol-bbq-utils}"
config_path="$repo/assets/config.yaml"
remote_config_sha256=""
if [ -f "$config_path" ]; then
    remote_config_sha256="$(sha256sum "$config_path" | awk '{ print $1 }')"
fi

container_status="$(docker inspect "$CONTAINER_NAME" --format '{{.State.Status}}')"
container_running="$(docker inspect "$CONTAINER_NAME" --format '{{.State.Running}}')"
restart_policy="$(docker inspect "$CONTAINER_NAME" --format '{{.HostConfig.RestartPolicy.Name}}')"
stop_timeout="$(docker inspect "$CONTAINER_NAME" --format '{{.Config.StopTimeout}}')"
container_image="$(docker inspect "$CONTAINER_NAME" --format '{{.Image}}')"
image_build_commit="$(docker image inspect "$container_image" --format '{{ index .Config.Labels "moe.n2nj.idol-bbq.build-commit" }}' 2>/dev/null || true)"
image_oci_revision="$(docker image inspect "$container_image" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' 2>/dev/null || true)"
image_created_label="$(docker image inspect "$container_image" --format '{{ index .Config.Labels "org.opencontainers.image.created" }}' 2>/dev/null || true)"
runtime_mode="$(docker inspect "$CONTAINER_NAME" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' | awk -F= '$1 == "IDOL_BBQ_RUNTIME_MODE" { print $2; found=1 } END { if (!found) print "" }')"
outbound_send_mode="$(docker inspect "$CONTAINER_NAME" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' | awk -F= '$1 == "IDOL_BBQ_OUTBOUND_SEND_MODE" { print $2; found=1 } END { if (!found) print "live" }')"
backup_container_dir="$(docker inspect "$CONTAINER_NAME" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' | awk -F= '$1 == "IDOL_BBQ_DB_BACKUP_DIR" { print $2; found=1 } END { if (!found) print "/app/backups/db-migrations" }')"
binds_tmp="$(mktemp)"
docker inspect "$CONTAINER_NAME" --format '{{ range .HostConfig.Binds }}{{ println . }}{{ end }}' > "$binds_tmp"
container_env_tmp="$(mktemp)"
docker inspect "$CONTAINER_NAME" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' > "$container_env_tmp"
mount_source() {
    awk -v target="$1" -F ':' '$2 == target { print $1; found=1; exit } END { if (!found) print "" }' "$binds_tmp"
}
mount_exists() {
    awk -v target="$1" -F ':' '$2 == target { found=1; exit } END { print found ? "true" : "false" }' "$binds_tmp"
}
mount_config_yaml="$(mount_source /app/config.yaml)"
mount_data_db="$(mount_source /app/data.db)"
mount_app_backups="$(mount_source /app/backups)"
resolve_backup_host_dir() {
    case "$backup_container_dir" in
        /app/backups)
            printf '%s\n' "$mount_app_backups"
            ;;
        /app/backups/*)
            if [ -n "$mount_app_backups" ]; then
                printf '%s/%s\n' "$mount_app_backups" "${backup_container_dir#/app/backups/}"
            fi
            ;;
        *)
            printf '%s\n' "$backup_container_dir"
            ;;
    esac
}
backup_host_dir="$(resolve_backup_host_dir)"
backup_parent_dir="$(dirname "$backup_host_dir")"
build_commit_file="$(docker run --rm --entrypoint cat "$container_image" /app/build-commit 2>/dev/null || true)"
migration_names="$(docker run --rm --entrypoint sh "$container_image" -lc 'find /app/prisma/migrations -mindepth 1 -maxdepth 1 -type d -printf "%f\n" | sort')"
migration_head="$(printf '%s\n' "$migration_names" | tail -1)"

audit_json="$(docker run --rm --entrypoint bun -v "$config_path:/app/config.yaml:ro" "$container_image" /app/tools/config-audit.js --config /app/config.yaml --fail-on-diagnostics)"
audit_tmp="$(mktemp)"
printf '%s\n' "$audit_json" > "$audit_tmp"
processor_env_tmp="$(mktemp)"
python3 - "$config_path" "$container_env_tmp" > "$processor_env_tmp" <<'PY'
import sys
import yaml

config_path, env_path = sys.argv[1:3]
with open(config_path, "r", encoding="utf-8") as handle:
    config = yaml.safe_load(handle) or {}
env = {}
with open(env_path, "r", encoding="utf-8") as handle:
    for line in handle:
        key, _, value = line.rstrip("\n").partition("=")
        if key:
            env[key] = value

required = []
for processor in config.get("processors") or []:
    api_key = str(processor.get("api_key") or "").strip()
    if api_key.startswith("env:"):
        name = api_key[len("env:"):].strip()
        if name:
            required.append(name)

required = sorted(set(required))
missing = [name for name in required if not env.get(name)]
print(f'processor_env_required={",".join(required)}')
print(f'processor_env_missing={",".join(missing)}')
print(f'processor_env_status={"missing" if missing else "ok"}')
PY

cd "$repo"
remote_dirty_tracked="$(git status --porcelain=v1 --untracked-files=no | wc -l | tr -d ' ')"
remote_dirty_untracked="$(git status --porcelain=v1 --untracked-files=normal | awk '$1 == "??" { count += 1 } END { print count + 0 }')"
db_path="$repo/assets/refactor.db"
backup_dir="$backup_host_dir"
backup_count=0
latest_backup=""
if [ -d "$backup_dir" ]; then
    backup_count="$(find "$backup_dir" -maxdepth 1 -type f -name 'refactor.db.*' ! -name '*.manifest' ! -name '*-wal' ! -name '*-shm' | wc -l | tr -d ' ')"
    latest_backup="$(find "$backup_dir" -maxdepth 1 -type f -name 'refactor.db.*' ! -name '*.manifest' ! -name '*-wal' ! -name '*-shm' -printf '%T@ %p\n' | sort -nr | awk 'NR == 1 { $1=""; sub(/^ /, ""); print }')"
fi
migration_names_tmp="$(mktemp)"
printf '%s\n' "$migration_names" > "$migration_names_tmp"
db_status_tmp="$(mktemp)"
db_read_path="$db_path"
db_container_tmp=""
if [ "$container_running" = "true" ]; then
    db_container_tmp="/tmp/forwarder-preflight-db-$$.sqlite"
    docker exec "$CONTAINER_NAME" python3 -c "import sqlite3; source=sqlite3.connect('/app/data.db'); target=sqlite3.connect('$db_container_tmp'); source.backup(target); target.close(); source.close()"
    db_read_path="$(mktemp)"
    docker cp "$CONTAINER_NAME:$db_container_tmp" "$db_read_path" >/dev/null
    docker exec "$CONTAINER_NAME" rm -f "$db_container_tmp"
fi
python3 - "$db_read_path" "$migration_names_tmp" "$backup_container_dir" "$backup_host_dir" "$backup_parent_dir" "$backup_count" "$latest_backup" > "$db_status_tmp" <<'PY'
import os
import sqlite3
import sys
from urllib.request import pathname2url

db_path, migrations_file, backup_container_dir, backup_host_dir, backup_parent_dir, backup_count, latest_backup = sys.argv[1:8]
with open(migrations_file, "r", encoding="utf-8") as handle:
    migration_names = [line.strip() for line in handle if line.strip()]

def emit(key, value):
    text = str(value).replace("\n", "\\n")
    print(f"{key}={text}")

def open_readonly_sqlite(path):
    uri = "file:" + pathname2url(path) + "?mode=ro"
    return sqlite3.connect(uri, uri=True)

def emit_db_error(reason):
    emit("db_quick_check_ok", "false")
    emit("db_quick_check", reason)
    emit("db_prisma_migrations_table", "false")
    emit("db_applied_migration_count", 0)
    emit("db_applied_migration_head", "")
    emit("db_failed_migration_count", 0)
    emit("db_rolled_back_migration_count", 0)
    emit("db_unknown_migration_count", 0)
    emit("db_unknown_migrations", "")
    emit("db_pending_migration_count", len(migration_names))
    emit("db_pending_migrations", ",".join(migration_names))
    emit("migration_status", "db_error")

emit("db_path", db_path)
emit("db_exists", str(os.path.isfile(db_path)).lower())
emit("db_backup_dir", backup_container_dir)
emit("db_backup_host_dir", backup_host_dir)
emit("db_backup_dir_resolved", str(bool(backup_host_dir)).lower())
emit("db_backup_dir_exists", str(os.path.isdir(backup_host_dir)).lower() if backup_host_dir else "false")
emit("db_backup_parent_dir", backup_parent_dir)
emit("db_backup_parent_exists", str(os.path.isdir(backup_parent_dir)).lower() if backup_parent_dir else "false")
emit("db_backup_count", backup_count)
emit("db_latest_backup", latest_backup)

if not os.path.isfile(db_path):
    emit("db_size", 0)
    emit("db_quick_check_ok", "false")
    emit("db_quick_check", "missing_db")
    emit("db_prisma_migrations_table", "false")
    emit("db_applied_migration_count", 0)
    emit("db_applied_migration_head", "")
    emit("db_failed_migration_count", 0)
    emit("db_rolled_back_migration_count", 0)
    emit("db_unknown_migration_count", 0)
    emit("db_unknown_migrations", "")
    emit("db_pending_migration_count", len(migration_names))
    emit("db_pending_migrations", ",".join(migration_names))
    emit("migration_status", "missing_db")
    raise SystemExit(0)

emit("db_size", os.path.getsize(db_path))
try:
    connection = open_readonly_sqlite(db_path)
except Exception as exc:
    emit_db_error(f"open_error:{exc.__class__.__name__}:{exc}")
    raise SystemExit(0)

quick_check_ok = False
try:
    quick_row = connection.execute("PRAGMA quick_check").fetchone()
    quick_check = quick_row[0] if quick_row else "no result"
    quick_check_ok = quick_check == "ok"
    emit("db_quick_check_ok", str(quick_check_ok).lower())
    emit("db_quick_check", quick_check)
    row = connection.execute(
        "select name from sqlite_master where type='table' and name='_prisma_migrations'"
    ).fetchone()
    has_table = row is not None
    emit("db_prisma_migrations_table", str(has_table).lower())
    if not has_table:
        applied = []
        failed = 0
        rolled_back = 0
    else:
        rows = connection.execute(
            "select migration_name, finished_at, rolled_back_at from _prisma_migrations order by migration_name"
        ).fetchall()
        applied = [name for name, finished_at, rolled_back_at in rows if finished_at and not rolled_back_at]
        failed = sum(1 for _name, finished_at, rolled_back_at in rows if not finished_at and not rolled_back_at)
        rolled_back = sum(1 for _name, _finished_at, rolled_back_at in rows if rolled_back_at)
except Exception as exc:
    emit_db_error(f"query_error:{exc.__class__.__name__}:{exc}")
    raise SystemExit(0)
finally:
    connection.close()

applied_set = set(applied)
known_set = set(migration_names)
pending = [name for name in migration_names if name not in applied_set]
unknown = [name for name in applied if name not in known_set]
emit("db_applied_migration_count", len(applied))
emit("db_applied_migration_head", applied[-1] if applied else "")
emit("db_failed_migration_count", failed)
emit("db_rolled_back_migration_count", rolled_back)
emit("db_unknown_migration_count", len(unknown))
emit("db_unknown_migrations", ",".join(unknown))
emit("db_pending_migration_count", len(pending))
emit("db_pending_migrations", ",".join(pending))
if not quick_check_ok:
    status = "db_error"
elif failed:
    status = "failed"
elif unknown:
    status = "drift"
elif pending:
    status = "pending"
elif not has_table:
    status = "missing_migration_table"
else:
    status = "up-to-date"
emit("migration_status", status)
PY
migration_status="$(awk -F= '$1 == "migration_status" { print $2 }' "$db_status_tmp")"

printf 'container_status=%s\n' "$container_status"
printf 'container_running=%s\n' "$container_running"
printf 'restart_policy=%s\n' "$restart_policy"
printf 'stop_timeout=%s\n' "$stop_timeout"
printf 'runtime_mode=%s\n' "$runtime_mode"
printf 'outbound_send_mode=%s\n' "$outbound_send_mode"
printf 'container_image=%s\n' "$container_image"
printf 'image_build_commit=%s\n' "$image_build_commit"
printf 'image_oci_revision=%s\n' "$image_oci_revision"
printf 'image_created=%s\n' "$image_created_label"
printf 'build_commit_file=%s\n' "$build_commit_file"
printf 'mount_config_yaml_exists=%s\n' "$(mount_exists /app/config.yaml)"
printf 'mount_config_yaml_source=%s\n' "$mount_config_yaml"
printf 'expected_config_sha256=%s\n' "${EXPECTED_CONFIG_SHA256:-}"
printf 'remote_config_sha256=%s\n' "$remote_config_sha256"
if [ -n "${EXPECTED_CONFIG_SHA256:-}" ] && [ "$remote_config_sha256" = "$EXPECTED_CONFIG_SHA256" ]; then
    printf 'config_sha256_match=true\n'
else
    printf 'config_sha256_match=false\n'
fi
printf 'mount_data_db_exists=%s\n' "$(mount_exists /app/data.db)"
printf 'mount_data_db_source=%s\n' "$mount_data_db"
printf 'mount_app_backups_exists=%s\n' "$(mount_exists /app/backups)"
printf 'mount_app_backups_source=%s\n' "$mount_app_backups"
printf 'expected_commit=%s\n' "${EXPECTED_COMMIT:-}"
printf 'expected_runtime_mode=%s\n' "${EXPECTED_RUNTIME_MODE:-}"
printf 'expected_outbound_send_mode=%s\n' "${EXPECTED_OUTBOUND_SEND_MODE:-}"
printf 'expected_running=%s\n' "${EXPECTED_RUNNING:-}"
printf 'expected_restart_policy=%s\n' "${EXPECTED_RESTART_POLICY:-}"
printf 'expected_stop_timeout_seconds=%s\n' "${EXPECTED_STOP_TIMEOUT_SECONDS:-}"
if [ -n "${EXPECTED_COMMIT:-}" ] && [ "$image_build_commit" = "$EXPECTED_COMMIT" ] && [ "$build_commit_file" = "$EXPECTED_COMMIT" ]; then
    printf 'commit_match=true\n'
else
    printf 'commit_match=false\n'
fi
python3 - "$audit_tmp" <<'PY'
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as handle:
    data = json.load(handle)

counts = data["route_graph"]["counts"]
print(f'audit_ok={str(data["ok"]).lower()}')
print(f'redacted_config_hash={data["redacted_config_hash"]}')
print(f'policy_hash={data["policy_hash"]}')
print(f'secret_field_count={data["secret_field_count"]}')
print(f'route_count={counts["routes"]}')
print(f'route_errors={counts["errors"]}')
print(f'route_warnings={counts["warnings"]}')
print(f'operational_crawlers={counts["operational_crawlers"]}')
print(f'summary_card_routes={data["route_graph"]["summary_card_routes"]}')
PY
cat "$processor_env_tmp"
processor_env_status="$(awk -F= '$1 == "processor_env_status" { print $2 }' "$processor_env_tmp")"
rm -f "$audit_tmp"
rm -f "$binds_tmp" "$container_env_tmp" "$processor_env_tmp"
printf 'migration_head=%s\n' "$migration_head"
cat "$db_status_tmp"
rm -f "$migration_names_tmp" "$db_status_tmp"
if [ -n "$db_container_tmp" ]; then
    rm -f "$db_read_path"
fi
printf 'remote_dirty_tracked=%s\n' "$remote_dirty_tracked"
printf 'remote_dirty_untracked=%s\n' "$remote_dirty_untracked"

if [ -n "${EXPECTED_RUNNING:-}" ] && [ "$container_running" != "$EXPECTED_RUNNING" ]; then
    printf 'preflight failed: running mismatch expected=%s actual=%s\n' "$EXPECTED_RUNNING" "$container_running" >&2
    exit 1
fi
if [ -n "${EXPECTED_RESTART_POLICY:-}" ] && [ "$restart_policy" != "$EXPECTED_RESTART_POLICY" ]; then
    printf 'preflight failed: restart policy mismatch expected=%s actual=%s\n' "$EXPECTED_RESTART_POLICY" "$restart_policy" >&2
    exit 1
fi
if [ -n "${EXPECTED_STOP_TIMEOUT_SECONDS:-}" ] && [ "$stop_timeout" != "$EXPECTED_STOP_TIMEOUT_SECONDS" ]; then
    printf 'preflight failed: stop timeout mismatch expected=%s actual=%s\n' "$EXPECTED_STOP_TIMEOUT_SECONDS" "$stop_timeout" >&2
    exit 1
fi
if [ -n "${EXPECTED_RUNTIME_MODE:-}" ] && [ "$runtime_mode" != "$EXPECTED_RUNTIME_MODE" ]; then
    printf 'preflight failed: runtime mode mismatch expected=%s actual=%s\n' "$EXPECTED_RUNTIME_MODE" "$runtime_mode" >&2
    exit 1
fi
if [ -n "${EXPECTED_OUTBOUND_SEND_MODE:-}" ] && [ "$outbound_send_mode" != "$EXPECTED_OUTBOUND_SEND_MODE" ]; then
    printf 'preflight failed: outbound send mode mismatch expected=%s actual=%s\n' "$EXPECTED_OUTBOUND_SEND_MODE" "$outbound_send_mode" >&2
    exit 1
fi
if [ "$STRICT_MIGRATIONS" = "1" ] && [ "$migration_status" != "up-to-date" ]; then
    printf 'preflight failed: migration status is %s\n' "$migration_status" >&2
    exit 1
fi
if [ "$STRICT_MIGRATIONS" = "1" ] && { [ -z "$backup_host_dir" ] || [ ! -d "$backup_parent_dir" ]; }; then
    printf 'preflight failed: migration backup directory is not resolvable or its parent is missing\n' >&2
    exit 1
fi
if [ "$STRICT_COMMIT" = "1" ] && { [ -z "${EXPECTED_COMMIT:-}" ] || [ "$image_build_commit" != "$EXPECTED_COMMIT" ] || [ "$build_commit_file" != "$EXPECTED_COMMIT" ]; }; then
    printf 'preflight failed: image commit does not match expected commit\n' >&2
    exit 1
fi
if [ "$STRICT_CONFIG_SHA256" = "1" ] && { [ -z "${EXPECTED_CONFIG_SHA256:-}" ] || [ "$remote_config_sha256" != "$EXPECTED_CONFIG_SHA256" ]; }; then
    printf 'preflight failed: runtime config sha256 mismatch\n' >&2
    exit 1
fi
if [ "$STRICT_PROCESSOR_ENV" = "1" ] && [ "$processor_env_status" != "ok" ]; then
    printf 'preflight failed: processor env keys are missing\n' >&2
    exit 1
fi
REMOTE
}

main "$@"
