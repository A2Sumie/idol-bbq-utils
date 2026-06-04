#!/bin/sh
set -e

# Ensure database file exists (Prisma might complain if the file is missing, but sqlite provider usually creates it)
# However, since we mount it, it might be created by docker as a directory if not exists.
# The user mapped ./assets/refactor.db:/app/data.db.

export PATH="/app/tools/bin:$PATH"
export BROWSER_PROFILE_DIR="${BROWSER_PROFILE_DIR:-/app/assets/cookies/browser-profiles}"

XVFB_PID=""
APP_PID=""
HOLD_PID=""
MIGRATION_LOCK_DIR=""
release_migration_lock() {
    if [ -n "$MIGRATION_LOCK_DIR" ] && [ -d "$MIGRATION_LOCK_DIR" ]; then
        rmdir "$MIGRATION_LOCK_DIR" >/dev/null 2>&1 || true
    fi
    MIGRATION_LOCK_DIR=""
}

cleanup() {
    release_migration_lock
    if [ -n "$HOLD_PID" ]; then
        kill "$HOLD_PID" >/dev/null 2>&1 || true
        wait "$HOLD_PID" >/dev/null 2>&1 || true
    fi
    if [ -n "$APP_PID" ]; then
        kill "$APP_PID" >/dev/null 2>&1 || true
        wait "$APP_PID" >/dev/null 2>&1 || true
    fi
    if [ -n "$XVFB_PID" ]; then
        kill "$XVFB_PID" >/dev/null 2>&1 || true
        wait "$XVFB_PID" >/dev/null 2>&1 || true
    fi
}

terminate() {
    cleanup
    exit 0
}

trap cleanup EXIT
trap terminate INT TERM

sqlite_quick_check() {
    db_path="$1"
python3 - "$db_path" <<'PY'
import sqlite3
import sys
from urllib.request import pathname2url

db_path = sys.argv[1]
db_uri = "file:" + pathname2url(db_path) + "?mode=ro"
connection = sqlite3.connect(db_uri, uri=True)
try:
    result = connection.execute("PRAGMA quick_check").fetchone()
finally:
    connection.close()

if not result or result[0] != "ok":
    raise SystemExit(f"SQLite quick_check failed for {db_path}: {result[0] if result else 'no result'}")
PY
}

sqlite_backup() {
    source_path="$1"
    backup_path="$2"
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

resolve_sqlite_db_path() {
    database_url="${DATABASE_URL:-file:/app/data.db}"
    case "$database_url" in
        file:*)
            db_path="${database_url#file:}"
            db_path="${db_path%%\?*}"
            ;;
        *)
            echo "Unsupported DATABASE_URL for migration backup: $database_url" >&2
            exit 65
            ;;
    esac

    if [ -z "$db_path" ]; then
        echo "DATABASE_URL does not contain a SQLite file path." >&2
        exit 65
    fi
    printf '%s\n' "$db_path"
}

prepare_migration_backup() {
    db_path="$1"
    if [ -d "$db_path" ]; then
        echo "Database path is a directory, refusing migration: $db_path" >&2
        exit 65
    fi

    if [ ! -f "$db_path" ]; then
        if [ "${IDOL_BBQ_REQUIRE_EXISTING_DB_FOR_MIGRATION:-1}" = "1" ]; then
            echo "Database file is missing, refusing migration: $db_path" >&2
            echo "Set IDOL_BBQ_REQUIRE_EXISTING_DB_FOR_MIGRATION=0 only for a deliberate first-run database." >&2
            exit 65
        fi
        echo "Database file is missing; migration backup skipped by explicit first-run override: $db_path" >&2
        return
    fi

    backup_dir="${IDOL_BBQ_DB_BACKUP_DIR:-/tmp/tweet-forwarder/logs/db-migrations}"
    mkdir -p "$backup_dir"
    if [ ! -w "$backup_dir" ]; then
        echo "Migration backup directory is not writable: $backup_dir" >&2
        exit 65
    fi

    lock_parent="$backup_dir"
    MIGRATION_LOCK_DIR="${IDOL_BBQ_DB_MIGRATION_LOCK_DIR:-$lock_parent/migration.lock}"
    if ! mkdir "$MIGRATION_LOCK_DIR" 2>/dev/null; then
        echo "Migration lock already exists: $MIGRATION_LOCK_DIR" >&2
        exit 75
    fi

    sqlite_quick_check "$db_path"

    timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
    build_commit="${IDOL_BBQ_BUILD_COMMIT:-unknown}"
    backup_base="$backup_dir/refactor.db.$timestamp.$build_commit"
    echo "Creating migration backup: $backup_base"
    sqlite_backup "$db_path" "$backup_base"
    sqlite_quick_check "$backup_base"
    for suffix in -wal -shm; do
        if [ -f "$db_path$suffix" ]; then
            cp -p "$db_path$suffix" "$backup_base$suffix"
        fi
    done

    {
        printf 'created_at=%s\n' "$timestamp"
        printf 'build_commit=%s\n' "$build_commit"
        printf 'database_path=%s\n' "$db_path"
        printf 'database_url=%s\n' "${DATABASE_URL:-file:/app/data.db}"
        printf 'backup_method=%s\n' "sqlite_backup_api"
        printf 'migration_head=%s\n' "$(find /app/prisma/migrations -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null | sort | tail -1)"
    } > "$backup_base.manifest"
}

runtime_mode="${IDOL_BBQ_RUNTIME_MODE:-offline}"
runtime_mode="$(printf '%s' "$runtime_mode" | tr '[:upper:]' '[:lower:]' | tr '_' '-')"
case "$runtime_mode" in
    online | api-only | offline)
        ;;
    *)
        echo "Invalid IDOL_BBQ_RUNTIME_MODE: $runtime_mode (expected online, api-only, or offline)" >&2
        exit 64
        ;;
esac
export IDOL_BBQ_RUNTIME_MODE="$runtime_mode"

mkdir -p /tmp/tweet-forwarder /tmp/tweet-forwarder/logs /tmp/tweet-forwarder/media "$BROWSER_PROFILE_DIR"

if [ "$runtime_mode" = "offline" ]; then
    echo "IDOL_BBQ_RUNTIME_MODE=offline: skipping media refresh, database migration, and application startup."
    echo "Container is intentionally idle; set IDOL_BBQ_RUNTIME_MODE=online for production."
    while :; do
        sleep 3600 &
        HOLD_PID=$!
        wait "$HOLD_PID" || true
        HOLD_PID=""
    done
fi

if [ "$runtime_mode" = "online" ] && [ "${ENABLE_XVFB:-1}" != "0" ] && command -v Xvfb >/dev/null 2>&1 && [ -z "${DISPLAY:-}" ]; then
    export DISPLAY="${XVFB_DISPLAY:-:99}"
    XVFB_DISPLAY_NUM="$(printf '%s' "$DISPLAY" | sed 's/^://')"
    XVFB_LOCK_FILE="/tmp/.X${XVFB_DISPLAY_NUM}-lock"
    XVFB_SOCKET_FILE="/tmp/.X11-unix/X${XVFB_DISPLAY_NUM}"
    if [ -f "$XVFB_LOCK_FILE" ]; then
        XVFB_LOCK_PID="$(tr -dc '0-9' < "$XVFB_LOCK_FILE")"
        if [ -z "$XVFB_LOCK_PID" ] || ! kill -0 "$XVFB_LOCK_PID" >/dev/null 2>&1; then
            rm -f "$XVFB_LOCK_FILE" "$XVFB_SOCKET_FILE"
        fi
    fi
    XVFB_SCREEN_SPEC="${XVFB_SCREEN:-0 1600x1200x24}"
    set -- $XVFB_SCREEN_SPEC
    echo "Starting Xvfb on ${DISPLAY} with screen ${XVFB_SCREEN_SPEC}..."
    Xvfb "$DISPLAY" -screen "$1" "$2" -ac +extension RANDR -nolisten tcp >/tmp/tweet-forwarder-xvfb.log 2>&1 &
    XVFB_PID=$!
    sleep 1
fi

refresh_media_tools="${IDOL_BBQ_REFRESH_MEDIA_TOOLS:-auto}"
if [ "$refresh_media_tools" = "auto" ]; then
    if [ "$runtime_mode" = "online" ]; then
        refresh_media_tools="1"
    else
        refresh_media_tools="0"
    fi
fi
if [ "$refresh_media_tools" = "1" ]; then
    echo "Refreshing media tools..."
    if ! /app/tools/update-media-tools.sh; then
        echo "Media tools refresh failed, continuing with bundled versions." >&2
    fi
else
    echo "Skipping media tools refresh for runtime mode: $runtime_mode"
fi

run_migrations="${IDOL_BBQ_RUN_MIGRATIONS:-auto}"
if [ "$run_migrations" = "auto" ]; then
    if [ "$runtime_mode" = "online" ]; then
        run_migrations="1"
    else
        run_migrations="0"
    fi
fi
case "$run_migrations" in
    true | yes)
        run_migrations="1"
        ;;
    false | no)
        run_migrations="0"
        ;;
esac
if [ "$run_migrations" = "1" ]; then
    if [ "$runtime_mode" != "online" ] && [ "${IDOL_BBQ_ALLOW_NON_ONLINE_MIGRATIONS:-0}" != "1" ]; then
        echo "Refusing to run database migrations while runtime mode is $runtime_mode." >&2
        echo "Set IDOL_BBQ_ALLOW_NON_ONLINE_MIGRATIONS=1 only for an explicit maintenance migration." >&2
        exit 65
    fi
    db_path="$(resolve_sqlite_db_path)"
    prepare_migration_backup "$db_path"
    echo "Migrating database..."
    # Use the installed prisma CLI
    bunx prisma migrate deploy
    if [ -f "$db_path" ]; then
        sqlite_quick_check "$db_path"
    fi
    release_migration_lock
else
    echo "Skipping database migration for runtime mode: $runtime_mode"
fi

echo "Starting application in runtime mode: $runtime_mode"
bun /app/bin.js &
APP_PID=$!
wait "$APP_PID"
APP_STATUS=$?
APP_PID=""
exit "$APP_STATUS"
