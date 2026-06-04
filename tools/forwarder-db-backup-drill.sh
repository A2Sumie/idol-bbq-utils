#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_HOST="${REMOTE_HOST:-3020e}"
REMOTE_REPO="${REMOTE_REPO:-}"
CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
DRILL_ROOT="${DRILL_ROOT:-/tmp/tweet-forwarder/backup-drills}"
KEEP_DRILL="${KEEP_DRILL:-0}"

if [ -n "${SSH_OPTS:-}" ]; then
    # shellcheck disable=SC2206
    SSH_ARGS=(${SSH_OPTS})
else
    SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout=10)
fi

remote_env_prefix() {
    local name value
    for name in REMOTE_REPO CONTAINER_NAME DRILL_ROOT KEEP_DRILL; do
        value="${!name:-}"
        if [ -n "$value" ]; then
            printf '%s=%q ' "$name" "$value"
        fi
    done
}

main() {
    if [ "${1:-}" = "--help" ]; then
        cat <<'HELP'
Usage: tools/forwarder-db-backup-drill.sh

Runs a no-start, no-migration remote restore drill against a temporary SQLite
copy. The production DB is opened read-only through SQLite's backup API. The
temporary copy is checked with PRAGMA quick_check and the current stopped image's
Prisma migrations are checked with `prisma migrate status`.

Environment:
  REMOTE_HOST=3020e
  REMOTE_REPO=            # defaults remotely to $HOME/idol-bbq-utils
  CONTAINER_NAME=forwarder-new
  DRILL_ROOT=/tmp/tweet-forwarder/backup-drills
  KEEP_DRILL=0            # set 1 to keep the temporary drill directory
HELP
        return
    fi

    if [ -n "${1:-}" ]; then
        printf 'forwarder-db-backup-drill: unknown argument: %s\n' "$1" >&2
        return 2
    fi

    local env_prefix
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s" <<'REMOTE'
set -euo pipefail

repo="${REMOTE_REPO:-$HOME/idol-bbq-utils}"
db_path="$repo/assets/refactor.db"
container_image="$(docker inspect "$CONTAINER_NAME" --format '{{.Image}}')"
image_build_commit="$(docker image inspect "$container_image" --format '{{ index .Config.Labels "moe.n2nj.idol-bbq.build-commit" }}' 2>/dev/null || true)"
image_created_label="$(docker image inspect "$container_image" --format '{{ index .Config.Labels "org.opencontainers.image.created" }}' 2>/dev/null || true)"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
safe_commit="${image_build_commit:-unknown}"
safe_commit="${safe_commit%%[!A-Za-z0-9_.-]*}"
if [ -z "$safe_commit" ]; then
    safe_commit="unknown"
fi
drill_dir="$DRILL_ROOT/$timestamp-$safe_commit"
drill_db="$drill_dir/refactor.db"
status_file="$drill_dir/prisma-migrate-status.txt"

cleanup() {
    if [ "${KEEP_DRILL:-0}" != "1" ] && [ -n "${drill_dir:-}" ] && [ -d "$drill_dir" ]; then
        rm -rf "$drill_dir"
    fi
}
trap cleanup EXIT

if [ ! -f "$db_path" ]; then
    printf 'db_backup_drill_failed=missing_db\n' >&2
    exit 65
fi

mkdir -p "$drill_dir"
python3 - "$db_path" "$drill_db" <<'PY'
import os
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

connection = sqlite3.connect(backup_path)
try:
    result = connection.execute("PRAGMA quick_check").fetchone()
finally:
    connection.close()

if not result or result[0] != "ok":
    raise SystemExit(f"backup quick_check failed: {result[0] if result else 'no result'}")

os.chmod(backup_path, 0o666)
PY

set +e
docker run --rm \
    --entrypoint sh \
    -e DATABASE_URL=file:/app/data.db \
    -v "$drill_db:/app/data.db:rw" \
    "$container_image" \
    -lc 'cd /app && bunx prisma migrate status --schema /app/prisma/schema.prisma' \
    > "$status_file" 2>&1
prisma_status_exit="$?"
set -e

printf 'drill_repo=%s\n' "$repo"
printf 'drill_db_source=%s\n' "$db_path"
printf 'drill_dir=%s\n' "$drill_dir"
printf 'drill_keep=%s\n' "${KEEP_DRILL:-0}"
printf 'container_image=%s\n' "$container_image"
printf 'image_build_commit=%s\n' "$image_build_commit"
printf 'image_created=%s\n' "$image_created_label"
printf 'drill_db_size=%s\n' "$(stat -c '%s' "$drill_db")"
printf 'drill_db_quick_check=ok\n'
printf 'prisma_migrate_status_exit=%s\n' "$prisma_status_exit"
printf 'prisma_migrate_status_begin\n'
sed -n '1,80p' "$status_file"
printf 'prisma_migrate_status_end\n'

if [ "$prisma_status_exit" != "0" ]; then
    exit "$prisma_status_exit"
fi

if [ "${KEEP_DRILL:-0}" != "1" ]; then
    printf 'drill_dir_removed=true\n'
else
    printf 'drill_dir_removed=false\n'
fi
REMOTE
}

main "$@"
