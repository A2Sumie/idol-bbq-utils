#!/usr/bin/env bash
set -Eeuo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
COOKIE_FILE="${COOKIE_FILE:-/app/assets/cookies/ycookies.txt}"
YT_DLP_PATH="${YT_DLP_PATH:-/app/tools/bin/yt-dlp}"
KEEPALIVE_URL="${KEEPALIVE_URL:-https://www.youtube.com/@sallyamakiofficial}"
MEMBERSHIP_PROBE_URL="${MEMBERSHIP_PROBE_URL:-https://www.youtube.com/watch?v=dpNbrZJqwqg}"

CONTAINER_SCRIPT="$(cat <<'REMOTE'
set -Eeuo pipefail

if [ ! -s "$COOKIE_FILE" ]; then
    printf 'youtube-cookie-keepalive: cookie jar missing or empty: %s\n' "$COOKIE_FILE" >&2
    exit 1
fi
if [ ! -x "$YT_DLP_PATH" ]; then
    printf 'youtube-cookie-keepalive: yt-dlp unavailable: %s\n' "$YT_DLP_PATH" >&2
    exit 1
fi

temporary_cookie_file="$(mktemp "${COOKIE_FILE}.tmp-keepalive.XXXXXX")"
cleanup() {
    rm -f "$temporary_cookie_file"
}
trap cleanup EXIT
cp "$COOKIE_FILE" "$temporary_cookie_file"
chmod 600 "$temporary_cookie_file"

"$YT_DLP_PATH" \
    --cookies "$temporary_cookie_file" \
    --simulate \
    --playlist-items 1 \
    --print '%(id)s' \
    "$KEEPALIVE_URL" >/dev/null

membership_result="$("$YT_DLP_PATH" \
    --cookies "$temporary_cookie_file" \
    --simulate \
    --no-playlist \
    --print '%(id)s|%(availability)s|%(duration)s' \
    "$MEMBERSHIP_PROBE_URL")"

case "$membership_result" in
    *'|subscriber_only|'*) ;;
    *)
        printf 'youtube-cookie-keepalive: membership probe did not confirm subscriber access\n' >&2
        exit 1
        ;;
esac

cp -p "$COOKIE_FILE" "${COOKIE_FILE}.bak-keepalive"
mv "$temporary_cookie_file" "$COOKIE_FILE"
chmod 600 "$COOKIE_FILE"
trap - EXIT
printf 'youtube-cookie-keepalive: ok %s\n' "$membership_result"
REMOTE
)"

exec docker exec \
    -e COOKIE_FILE="$COOKIE_FILE" \
    -e YT_DLP_PATH="$YT_DLP_PATH" \
    -e KEEPALIVE_URL="$KEEPALIVE_URL" \
    -e MEMBERSHIP_PROBE_URL="$MEMBERSHIP_PROBE_URL" \
    "$CONTAINER_NAME" bash -c "$CONTAINER_SCRIPT"
