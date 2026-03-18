#!/bin/sh
set -e

# Ensure database file exists (Prisma might complain if the file is missing, but sqlite provider usually creates it)
# However, since we mount it, it might be created by docker as a directory if not exists.
# The user mapped ./assets/refactor.db:/app/data.db.

export PATH="/app/tools/bin:$PATH"
export BROWSER_PROFILE_DIR="${BROWSER_PROFILE_DIR:-/app/assets/cookies/browser-profiles}"

XVFB_PID=""
APP_PID=""
cleanup() {
    if [ -n "$APP_PID" ]; then
        kill "$APP_PID" >/dev/null 2>&1 || true
        wait "$APP_PID" >/dev/null 2>&1 || true
    fi
    if [ -n "$XVFB_PID" ]; then
        kill "$XVFB_PID" >/dev/null 2>&1 || true
        wait "$XVFB_PID" >/dev/null 2>&1 || true
    fi
}

trap cleanup EXIT INT TERM

mkdir -p /tmp/tweet-forwarder /tmp/tweet-forwarder/logs /tmp/tweet-forwarder/media "$BROWSER_PROFILE_DIR"

if [ "${ENABLE_XVFB:-1}" != "0" ] && command -v Xvfb >/dev/null 2>&1 && [ -z "${DISPLAY:-}" ]; then
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

echo "Refreshing media tools..."
if ! /app/tools/update-media-tools.sh; then
    echo "Media tools refresh failed, continuing with bundled versions." >&2
fi

echo "Migrating database..."
# Use the installed prisma CLI
bunx prisma migrate deploy

echo "Starting application..."
bun /app/bin.js &
APP_PID=$!
wait "$APP_PID"
APP_STATUS=$?
APP_PID=""
exit "$APP_STATUS"
