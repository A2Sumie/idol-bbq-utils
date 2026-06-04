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
Usage: tools/forwarder-remote-drift.sh [--compare-local-head]

Prints a no-content inventory of the remote idol-bbq-utils dirty worktree.
Only git status codes and paths are shown; file contents are never read.

Options:
  --compare-local-head  Expand untracked source files and compare remote
                        worktree blob hashes against local HEAD. Runtime
                        backup/config artifacts are classified without hashing.
HELP
        return
    fi

    if [ "${1:-}" = "--compare-local-head" ]; then
        compare_local_head
        return
    fi

    if [ -n "${1:-}" ]; then
        printf 'forwarder-remote-drift: unknown argument: %s\n' "$1" >&2
        return 2
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

compare_local_head() {
    git rev-parse --is-inside-work-tree >/dev/null

    local expected_json remote_json env_prefix expected_commit
    expected_json="$(mktemp)"
    remote_json="$(mktemp)"
    trap 'rm -f "${expected_json:-}" "${remote_json:-}"' RETURN

    expected_commit="$(git rev-parse HEAD)"
    python3 - "$expected_commit" > "$expected_json" <<'PY'
import json
import subprocess
import sys

commit = sys.argv[1]
raw = subprocess.check_output(
    ["git", "ls-tree", "-r", "-z", "--format=%(objectname)%x09%(path)", "HEAD"]
)
files = {}
for record in raw.split(b"\0"):
    if not record:
        continue
    sha, path = record.decode("utf-8", "surrogateescape").split("\t", 1)
    files[path] = sha

json.dump({"expected_commit": commit, "files": files}, sys.stdout, sort_keys=True)
PY

    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}python3 -" > "$remote_json" <<'REMOTE'
import json
import os
import subprocess
import sys


def run(args):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
    except subprocess.CalledProcessError:
        return ""


def bucket(path):
    if path.startswith("app/tweet-forwarder/prisma/"):
        return "prisma"
    if path.startswith("app/tweet-forwarder/src/db/"):
        return "db"
    if path.startswith("app/tweet-forwarder/src/managers/"):
        return "managers"
    if path.startswith("app/tweet-forwarder/src/middleware/forwarder/"):
        return "forwarder-middleware"
    if path.startswith("app/tweet-forwarder/src/services/"):
        return "services"
    if path.startswith("app/tweet-forwarder/src/types/"):
        return "types"
    if path.startswith("app/tweet-forwarder/src/"):
        return "app-src"
    if path.startswith("core/spider/"):
        return "core-spider"
    if path.startswith("assets/backups/"):
        return "runtime-backups"
    if path.startswith("assets/"):
        return "runtime-assets"
    return "other"


repo = os.environ.get("REMOTE_REPO") or os.path.join(os.path.expanduser("~"), "idol-bbq-utils")
os.chdir(repo)
raw_status = subprocess.check_output(
    ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"]
)
entries = [entry for entry in raw_status.split(b"\0") if entry]
rows = []
i = 0
while i < len(entries):
    entry = entries[i]
    code = entry[:2].decode("ascii", "replace")
    path = entry[3:].decode("utf-8", "surrogateescape")
    # Porcelain v1 -z stores rename/copy source as the next NUL record.
    if code[:1] in {"R", "C"} or code[1:2] in {"R", "C"}:
        i += 1
    i += 1

    clean_code = code.strip() or code
    path_bucket = bucket(path)
    worktree_hash = ""
    remote_head_hash = ""
    if not path_bucket.startswith("runtime-"):
        if os.path.isfile(path):
            worktree_hash = run(["git", "hash-object", f"--path={path}", "--", path])
        remote_head_hash = run(["git", "rev-parse", f"HEAD:{path}"])
    rows.append(
        {
            "status": clean_code,
            "bucket": path_bucket,
            "path": path,
            "worktree_hash": worktree_hash,
            "remote_head_hash": remote_head_hash,
        }
    )

json.dump(
    {
        "remote_repo": repo,
        "remote_head": run(["git", "rev-parse", "HEAD"]),
        "remote_branch": run(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "expanded_untracked_files": True,
        "rows": rows,
    },
    sys.stdout,
    ensure_ascii=False,
    sort_keys=True,
)
REMOTE

    python3 - "$expected_json" "$remote_json" <<'PY'
import json
import sys
from collections import Counter

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    expected = json.load(handle)
with open(sys.argv[2], "r", encoding="utf-8") as handle:
    remote = json.load(handle)

files = expected["files"]
relation_counts = Counter()
bucket_counts = Counter()
tracked = 0
untracked = 0

def relation(row):
    path = row["path"]
    expected_hash = files.get(path, "")
    if row["bucket"].startswith("runtime-"):
        return "runtime_artifact"
    if not expected_hash:
        return "absent_in_local_head"
    if row["worktree_hash"] == expected_hash:
        return "matches_local_head"
    if row["remote_head_hash"] == expected_hash:
        return "local_equals_remote_head_worktree_differs"
    if not row["worktree_hash"]:
        return "no_worktree_hash"
    return "differs_from_local_head"

rows = []
for row in remote["rows"]:
    item = dict(row)
    item["relation"] = relation(row)
    rows.append(item)
    relation_counts[item["relation"]] += 1
    bucket_counts[item["bucket"]] += 1
    if item["status"] == "??":
        untracked += 1
    else:
        tracked += 1

print(f'remote_repo={remote["remote_repo"]}')
print(f'remote_head={remote["remote_head"]}')
print(f'remote_branch={remote["remote_branch"]}')
print(f'expected_commit={expected["expected_commit"]}')
print('expanded_untracked_files=true')
print(f'tracked={tracked}')
print(f'untracked={untracked}')
print(f'total={tracked + untracked}')
print('relations_begin')
for name, count in sorted(relation_counts.items()):
    print(f'{name}\t{count}')
print('relations_end')
print('buckets_begin')
for name, count in sorted(bucket_counts.items()):
    print(f'{name}\t{count}')
print('buckets_end')
print('paths_begin')
for row in rows:
    print(f'{row["status"]}\t{row["bucket"]}\t{row["relation"]}\t{row["path"]}')
print('paths_end')
PY
    rm -f "$expected_json" "$remote_json"
    trap - RETURN
}

main "$@"
