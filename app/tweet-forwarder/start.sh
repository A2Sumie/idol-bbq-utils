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
cleanup() {
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
if [ "$run_migrations" = "1" ]; then
    echo "Migrating database..."
    # Use the installed prisma CLI
    bunx prisma migrate deploy
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
