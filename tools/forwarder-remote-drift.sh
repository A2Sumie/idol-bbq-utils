#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_HOST="${REMOTE_HOST:-3020e}"
REMOTE_REPO="${REMOTE_REPO:-}"

if [ -n "${SSH_OPTS:-}" ]; then
    # shellcheck disable=SC2206
    SSH_ARGS=(${SSH_OPTS})
else
    SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout=10)
fi

remote_env_prefix() {
    if [ -n "$REMOTE_REPO" ]; then
        printf 'REMOTE_REPO=%q ' "$REMOTE_REPO"
    fi
}

main() {
    if [ "${1:-}" = "--help" ]; then
        cat <<'HELP'
Usage: tools/forwarder-remote-drift.sh

Prints a no-content inventory of the remote idol-bbq-utils dirty worktree.
Only git status codes and paths are shown; file contents are never read.
HELP
        return
    fi

    local env_prefix
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s" <<'REMOTE'
set -euo pipefail
repo="${REMOTE_REPO:-$HOME/idol-bbq-utils}"
cd "$repo"
printf 'remote_repo=%s\n' "$repo"
printf 'remote_head=%s\n' "$(git rev-parse HEAD)"
printf 'remote_branch=%s\n' "$(git rev-parse --abbrev-ref HEAD)"
git status --porcelain=v1 --untracked-files=normal | awk '
function bucket(path) {
    if (path ~ /^app\/tweet-forwarder\/prisma\//) return "prisma"
    if (path ~ /^app\/tweet-forwarder\/src\/db\//) return "db"
    if (path ~ /^app\/tweet-forwarder\/src\/managers\//) return "managers"
    if (path ~ /^app\/tweet-forwarder\/src\/middleware\/forwarder\//) return "forwarder-middleware"
    if (path ~ /^app\/tweet-forwarder\/src\/services\//) return "services"
    if (path ~ /^app\/tweet-forwarder\/src\/types\//) return "types"
    if (path ~ /^app\/tweet-forwarder\/src\//) return "app-src"
    if (path ~ /^core\/spider\//) return "core-spider"
    if (path ~ /^assets\/backups\//) return "runtime-backups"
    if (path ~ /^assets\//) return "runtime-assets"
    return "other"
}
{
    status=$1
    path=$2
    if (status != "??" && $2 == "->") {
        path=$3
    }
    b=bucket(path)
    counts[b] += 1
    total += 1
    if (status == "??") untracked += 1
    else tracked += 1
    rows[++row_count] = status "\t" b "\t" path
}
END {
    printf "tracked=%d\n", tracked + 0
    printf "untracked=%d\n", untracked + 0
    printf "total=%d\n", total + 0
    printf "buckets_begin\n"
    for (name in counts) {
        printf "%s\t%d\n", name, counts[name]
    }
    printf "buckets_end\n"
    printf "paths_begin\n"
    for (i = 1; i <= row_count; i += 1) {
        print rows[i]
    }
    printf "paths_end\n"
}'
REMOTE
}

main "$@"
