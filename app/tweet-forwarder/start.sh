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
    quick_check_db_path="$1"
python3 - "$quick_check_db_path" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
connection = sqlite3.connect(db_path)
try:
    result = connection.execute("PRAGMA quick_check").fetchone()
finally:
    connection.close()

if not result or result[0] != "ok":
    raise SystemExit(f"SQLite quick_check failed for {db_path}: {result[0] if result else 'no result'}")
PY
}

sqlite_backup() {
    backup_source_path="$1"
    backup_target_path="$2"
    python3 - "$backup_source_path" "$backup_target_path" <<'PY'
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

find_latest_healthy_sqlite_backup() {
    healthy_backup_dir="$1"
    python3 - "$healthy_backup_dir" <<'PY'
import sqlite3
import sys
from pathlib import Path

backup_dir = Path(sys.argv[1])
if not backup_dir.is_dir():
    raise SystemExit(1)

candidates = [
    path for path in backup_dir.iterdir()
    if path.is_file()
    and path.name.startswith('refactor.db.')
    and not path.name.endswith(('.manifest', '-wal', '-shm'))
]
candidates.sort(key=lambda path: (path.stat().st_mtime, path.name), reverse=True)

for path in candidates:
    try:
        connection = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
        try:
            result = connection.execute('PRAGMA quick_check').fetchone()
        finally:
            connection.close()
    except Exception:
        continue
    if result and result[0] == 'ok':
        print(path)
        raise SystemExit(0)

raise SystemExit(1)
PY
}

recover_sqlite_db_if_needed() {
    recovery_db_path="$1"
    recovery_backup_dir="$2"

    if [ ! -f "$recovery_db_path" ]; then
        return 0
    fi

    if sqlite_quick_check "$recovery_db_path"; then
        return 0
    fi
    recovery_check_status=$?

    if [ "${IDOL_BBQ_AUTO_RESTORE_DB_BACKUP:-1}" != "1" ]; then
        echo "SQLite quick_check failed and automatic backup restore is disabled: $recovery_db_path" >&2
        return "$recovery_check_status"
    fi

    echo "SQLite quick_check failed for $recovery_db_path; attempting automatic restore from backups." >&2
    recovery_latest_backup="$(find_latest_healthy_sqlite_backup "$recovery_backup_dir")" || {
        echo "No healthy SQLite backup found in $recovery_backup_dir; refusing startup." >&2
        return "$recovery_check_status"
    }

    recovery_dir="${IDOL_BBQ_DB_RECOVERY_DIR:-/app/backups/db-recovery}"
    mkdir -p "$recovery_dir"
    recovery_timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
    recovery_corrupt_base="$recovery_dir/refactor.db.$recovery_timestamp.corrupt"

    cp -p "$recovery_db_path" "$recovery_corrupt_base"
    for suffix in -wal -shm; do
        if [ -f "$recovery_db_path$suffix" ]; then
            cp -p "$recovery_db_path$suffix" "$recovery_corrupt_base$suffix"
            rm -f "$recovery_db_path$suffix"
        fi
    done

    cp "$recovery_latest_backup" "$recovery_db_path"
    for suffix in -wal -shm; do
        if [ -f "$recovery_latest_backup$suffix" ]; then
            cp "$recovery_latest_backup$suffix" "$recovery_db_path$suffix"
        else
            rm -f "$recovery_db_path$suffix"
        fi
    done

    sqlite_quick_check "$recovery_db_path"
    {
        printf 'recovered_at=%s\n' "$recovery_timestamp"
        printf 'database_path=%s\n' "$recovery_db_path"
        printf 'source_backup=%s\n' "$recovery_latest_backup"
        printf 'corrupt_copy=%s\n' "$recovery_corrupt_base"
        printf 'build_commit=%s\n' "${IDOL_BBQ_BUILD_COMMIT:-unknown}"
    } > "$recovery_corrupt_base.manifest"

    # Emit a one-time marker so the app can reconcile external send state (e.g. read the
    # Bilibili submission list) after a restore that may have lost recent sent markers.
    recovery_marker="${IDOL_BBQ_DB_RECOVERY_MARKER:-/tmp/tweet-forwarder/db-recovered.json}"
    mkdir -p "$(dirname "$recovery_marker")"
    {
        printf '{'
        printf '"recovered_at":"%s",' "$recovery_timestamp"
        printf '"source_backup":"%s",' "$recovery_latest_backup"
        printf '"corrupt_copy":"%s"' "$recovery_corrupt_base"
        printf '}\n'
    } > "$recovery_marker"

    echo "Restored SQLite database from backup: $recovery_latest_backup" >&2
}

resolve_sqlite_db_path() {
    database_url="${DATABASE_URL:-file:/app/data.db}"
    case "$database_url" in
        file:*)
            resolved_db_path="${database_url#file:}"
            resolved_db_path="${resolved_db_path%%\?*}"
            ;;
        *)
            echo "Unsupported DATABASE_URL for migration backup: $database_url" >&2
            exit 65
            ;;
    esac

    if [ -z "$resolved_db_path" ]; then
        echo "DATABASE_URL does not contain a SQLite file path." >&2
        exit 65
    fi
    printf '%s\n' "$resolved_db_path"
}

prepare_migration_backup() {
    migration_db_path="$1"
    if [ -d "$migration_db_path" ]; then
        echo "Database path is a directory, refusing migration: $migration_db_path" >&2
        exit 65
    fi

    if [ ! -f "$migration_db_path" ]; then
        if [ "${IDOL_BBQ_REQUIRE_EXISTING_DB_FOR_MIGRATION:-1}" = "1" ]; then
            echo "Database file is missing, refusing migration: $migration_db_path" >&2
            echo "Set IDOL_BBQ_REQUIRE_EXISTING_DB_FOR_MIGRATION=0 only for a deliberate first-run database." >&2
            exit 65
        fi
        echo "Database file is missing; migration backup skipped by explicit first-run override: $migration_db_path" >&2
        return
    fi

    backup_dir="${IDOL_BBQ_DB_BACKUP_DIR:-/app/backups/db-migrations}"
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

    sqlite_quick_check "$migration_db_path"

    timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
    build_commit="${IDOL_BBQ_BUILD_COMMIT:-unknown}"
    backup_base="$backup_dir/refactor.db.$timestamp.$build_commit"
    echo "Creating migration backup: $backup_base"
    sqlite_backup "$migration_db_path" "$backup_base"
    sqlite_quick_check "$backup_base"
    for suffix in -wal -shm; do
        if [ -f "$migration_db_path$suffix" ]; then
            cp -p "$migration_db_path$suffix" "$backup_base$suffix"
        fi
    done

    {
        printf 'created_at=%s\n' "$timestamp"
        printf 'build_commit=%s\n' "$build_commit"
        printf 'database_path=%s\n' "$migration_db_path"
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

outbound_send_mode="${IDOL_BBQ_OUTBOUND_SEND_MODE:-live}"
outbound_send_mode="$(printf '%s' "$outbound_send_mode" | tr '[:upper:]' '[:lower:]' | tr '_' '-')"
case "$outbound_send_mode" in
    live | online)
        outbound_send_mode="live"
        ;;
    blocked | block | dry-run | dryrun | disabled | off | no-send | nosend)
        outbound_send_mode="blocked"
        ;;
    capture | captured | test-receiver | testreceiver | receiver | fake-receiver | fakereceiver | fake | sink)
        outbound_send_mode="capture"
        ;;
    *)
        echo "Invalid IDOL_BBQ_OUTBOUND_SEND_MODE: $outbound_send_mode (expected live, blocked, or capture)" >&2
        exit 64
        ;;
esac
export IDOL_BBQ_OUTBOUND_SEND_MODE="$outbound_send_mode"
if [ "$runtime_mode" = "online" ] && [ "$outbound_send_mode" = "blocked" ]; then
    echo "IDOL_BBQ_OUTBOUND_SEND_MODE=blocked: runtime will crawl/process but external send APIs are disabled."
fi
if [ "$runtime_mode" = "online" ] && [ "$outbound_send_mode" = "capture" ]; then
    echo "IDOL_BBQ_OUTBOUND_SEND_MODE=capture: runtime will crawl/process but external send APIs are replaced by an internal capture receiver."
fi

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
    migration_backup_dir="${IDOL_BBQ_DB_BACKUP_DIR:-/app/backups/db-migrations}"
    recover_sqlite_db_if_needed "$db_path" "$migration_backup_dir"
    prepare_migration_backup "$db_path"
    echo "Migrating database..."
    # Use the installed prisma CLI
    bunx prisma migrate deploy
    if [ -f "$db_path" ]; then
        if ! sqlite_quick_check "$db_path"; then
            recover_sqlite_db_if_needed "$db_path" "$migration_backup_dir"
            sqlite_quick_check "$db_path"
            # The restored backup predates the migration; re-apply migrations so the
            # runtime schema matches the generated Prisma client.
            echo "Re-running migrations after post-migration backup restore..."
            bunx prisma migrate deploy
            sqlite_quick_check "$db_path"
        fi
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
