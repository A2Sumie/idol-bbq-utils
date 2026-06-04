#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_HOST="${REMOTE_HOST:-3020e}"
REMOTE_REPO="${REMOTE_REPO:-}"
REMOTE_ARCHIVE_ROOT="${REMOTE_ARCHIVE_ROOT:-}"
SKIP_UPSTREAM_CHECK="${SKIP_UPSTREAM_CHECK:-0}"

MODE="dry-run"
YES="0"

if [ -n "${SSH_OPTS:-}" ]; then
    # shellcheck disable=SC2206
    SSH_ARGS=(${SSH_OPTS})
else
    SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout=10)
fi

die() {
    printf 'forwarder-remote-converge: %s\n' "$*" >&2
    exit 1
}

remote_env_prefix() {
    local name value
    for name in REMOTE_REPO REMOTE_ARCHIVE_ROOT EXPECTED_COMMIT EXPECTED_SHORT REMOTE_PATH_FILE REMOTE_TAR_FILE; do
        value="${!name:-}"
        if [ -n "$value" ]; then
            printf '%s=%q ' "$name" "$value"
        fi
    done
}

remote_mktemp() {
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" 'mktemp'
}

remote_rm_f() {
    local path="$1"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "rm -f $(printf '%q' "$path")" >/dev/null
}

require_clean_local_worktree() {
    local status
    status="$(git status --porcelain=v1 --untracked-files=normal)"
    if [ -n "$status" ]; then
        printf '%s\n' "$status" >&2
        die "local worktree is dirty; commit or stash before applying remote source convergence"
    fi
}

require_pushed_head() {
    if [ "$SKIP_UPSTREAM_CHECK" = "1" ]; then
        return
    fi

    local upstream ahead
    upstream="$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || true)"
    if [ -z "$upstream" ]; then
        die "current branch has no upstream; push first or set SKIP_UPSTREAM_CHECK=1"
    fi
    ahead="$(git rev-list --count "$upstream..HEAD")"
    if [ "$ahead" != "0" ]; then
        die "HEAD is $ahead commit(s) ahead of $upstream; push before remote convergence for traceability"
    fi
}

build_plan() {
    local drift_json="$1" replace_file="$2" replace_nul_file="$3" delete_file="$4" delete_nul_file="$5" archive_nul_file="$6" protected_file="$7" manual_file="$8"
    tools/forwarder-remote-drift.sh --compare-local-head --json > "$drift_json"
    python3 - "$drift_json" "$replace_file" "$replace_nul_file" "$delete_file" "$delete_nul_file" "$archive_nul_file" "$protected_file" "$manual_file" "$MODE" <<'PY'
import json
import subprocess
import sys

(
    drift_path,
    replace_path,
    replace_nul_path,
    delete_path,
    delete_nul_path,
    archive_nul_path,
    protected_path,
    manual_path,
    mode,
) = sys.argv[1:]

with open(drift_path, "r", encoding="utf-8") as handle:
    drift = json.load(handle)

protected_prefixes = ("assets/",)
protected_exact = {
    "assets/config.yaml",
    "assets/refactor.db",
    "assets/refactor.db-shm",
    "assets/refactor.db-wal",
}
source_prefixes = (
    "app/tweet-forwarder/src/",
    "app/tweet-forwarder/prisma/",
    "app/tweet-forwarder/scripts/",
    "core/",
    "tools/",
)
source_exact = {
    "package.json",
    "bun.lock",
    "tsconfig.json",
    "docker-compose.yaml",
    "app/tweet-forwarder/package.json",
    "app/tweet-forwarder/Dockerfile",
    "app/tweet-forwarder/start.sh",
    "app/tweet-forwarder/update-media-tools.sh",
}
candidate_relations = {
    "matches_local_head",
    "differs_from_local_head",
    "local_equals_remote_head_worktree_differs",
    "no_worktree_hash",
    "remote_head_differs_from_local_head",
    "absent_on_remote_head",
}
delete_relations = {
    "absent_in_local_head",
}
no_action_relations = {
    "source_absent_as_desired",
}


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
    if path.startswith("app/tweet-forwarder/scripts/"):
        return "app-scripts"
    if path.startswith("core/spider/"):
        return "core-spider"
    if path.startswith("core/"):
        return "core"
    if path.startswith("tools/"):
        return "tools"
    if path in source_exact:
        return "repo-source"
    if path.startswith("assets/backups/"):
        return "runtime-backups"
    if path.startswith("assets/"):
        return "runtime-assets"
    return "other"


def unsafe_path(path):
    return path.startswith("/") or "\x00" in path or any(part == ".." for part in path.split("/"))


def is_local_source_path(path):
    if path.startswith("assets/"):
        return False
    return path in source_exact or path.startswith(source_prefixes)


def is_runtime_path(row):
    path = row["path"]
    return (
        row["bucket"].startswith("runtime-")
        or row["relation"] == "runtime_artifact"
        or path.startswith(protected_prefixes)
        or path in protected_exact
    )


candidates = []
deletions = []
no_actions = []
protected = []
manual = []
candidate_paths = set()

for row in drift["rows"]:
    path = row["path"]
    item = {
        "status": row["status"],
        "bucket": row["bucket"],
        "relation": row["relation"],
        "path": path,
    }
    if unsafe_path(path):
        item["reason"] = "unsafe_path"
        manual.append(item)
    elif is_runtime_path(row):
        item["reason"] = "runtime_protected"
        protected.append(item)
    elif row["relation"] in no_action_relations:
        item["action"] = "already_matches_local_head"
        no_actions.append(item)
    elif row["relation"] in delete_relations and is_local_source_path(path):
        item["action"] = "delete_remote_source"
        deletions.append(item)
    elif row["relation"] in candidate_relations:
        item["action"] = "replace_from_local_head"
        candidates.append(item)
        candidate_paths.add(path)
    else:
        item["reason"] = "manual_review_required"
        manual.append(item)

local_head_source_paths = subprocess.check_output(
    ["git", "ls-tree", "-r", "--name-only", "HEAD"],
    text=True,
).splitlines()
for path in local_head_source_paths:
    if path in candidate_paths or not is_local_source_path(path) or unsafe_path(path):
        continue
    candidates.append(
        {
            "status": "HEAD",
            "bucket": bucket(path),
            "relation": "local_head_source",
            "action": "replace_from_local_head",
            "path": path,
        }
    )
    candidate_paths.add(path)

with open(replace_path, "w", encoding="utf-8") as handle:
    for item in candidates:
        handle.write(item["path"] + "\n")
with open(replace_nul_path, "wb") as handle:
    for item in candidates:
        handle.write(item["path"].encode("utf-8", "surrogateescape") + b"\0")
with open(delete_path, "w", encoding="utf-8") as handle:
    for item in deletions:
        handle.write(item["path"] + "\n")
with open(delete_nul_path, "wb") as handle:
    for item in deletions:
        handle.write(item["path"].encode("utf-8", "surrogateescape") + b"\0")
with open(archive_nul_path, "wb") as handle:
    archived = set()
    for item in [*candidates, *deletions]:
        path = item["path"]
        if path in archived:
            continue
        handle.write(path.encode("utf-8", "surrogateescape") + b"\0")
        archived.add(path)
with open(protected_path, "w", encoding="utf-8") as handle:
    for item in protected:
        handle.write(f'{item["status"]}\t{item["bucket"]}\t{item["relation"]}\t{item["reason"]}\t{item["path"]}\n')
with open(manual_path, "w", encoding="utf-8") as handle:
    for item in manual:
        handle.write(f'{item["status"]}\t{item["bucket"]}\t{item["relation"]}\t{item["reason"]}\t{item["path"]}\n')

relations = dict(drift.get("relations", {}))
buckets = dict(drift.get("buckets", {}))
for item in candidates:
    if item["relation"] == "local_head_source":
        relations["local_head_source"] = relations.get("local_head_source", 0) + 1
        buckets[item["bucket"]] = buckets.get(item["bucket"], 0) + 1
print(f'mode={mode}')
print(f'remote_repo={drift["remote_repo"]}')
print(f'remote_head={drift["remote_head"]}')
print(f'remote_branch={drift["remote_branch"]}')
print(f'expected_commit={drift["expected_commit"]}')
print(f'expanded_untracked_files={str(drift["expanded_untracked_files"]).lower()}')
print(f'total={drift["counts"]["total"]}')
print(f'tracked={drift["counts"]["tracked"]}')
print(f'untracked={drift["counts"]["untracked"]}')
print(f'synthetic_source_drift={drift["counts"].get("synthetic_source_drift", 0)}')
print(f'candidate_source_paths={len(candidates)}')
print(f'delete_source_paths={len(deletions)}')
print(f'no_action_source_paths={len(no_actions)}')
print(f'archive_source_paths={len(set([item["path"] for item in [*candidates, *deletions]]))}')
print(f'protected_runtime_paths={len(protected)}')
print(f'manual_review_paths={len(manual)}')
print("relations_begin")
for name, count in sorted(relations.items()):
    print(f"{name}\t{count}")
print("relations_end")
print("buckets_begin")
for name, count in sorted(buckets.items()):
    print(f"{name}\t{count}")
print("buckets_end")
print("candidate_paths_begin")
for item in [*candidates, *deletions]:
    action = item.get("action", "review")
    print(f'{item["status"]}\t{item["bucket"]}\t{item["relation"]}\t{action}\t{item["path"]}')
print("candidate_paths_end")
print("protected_paths_begin")
for item in protected:
    print(f'{item["status"]}\t{item["bucket"]}\t{item["relation"]}\t{item["reason"]}\t{item["path"]}')
print("protected_paths_end")
if manual:
    print("manual_review_paths_begin")
    for item in manual:
        print(f'{item["status"]}\t{item["bucket"]}\t{item["relation"]}\t{item["reason"]}\t{item["path"]}')
    print("manual_review_paths_end")
PY
}

create_remote_archive() {
    local candidate_nul_file="$1" expected_commit="$2" expected_short="$3"
    local remote_path_file env_prefix

    remote_path_file="$(remote_mktemp)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "cat > $(printf '%q' "$remote_path_file")" < "$candidate_nul_file"

    EXPECTED_COMMIT="$expected_commit"
    EXPECTED_SHORT="$expected_short"
    REMOTE_PATH_FILE="$remote_path_file"
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s" <<'REMOTE'
set -euo pipefail
repo="${REMOTE_REPO:-$HOME/idol-bbq-utils}"
archive_root="${REMOTE_ARCHIVE_ROOT:-$HOME/idol-bbq-utils-archives/remote-convergence}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
archive_dir="$archive_root/${timestamp}-${EXPECTED_SHORT:-unknown}"
mkdir -p "$archive_dir"
cd "$repo"
python3 - "$REMOTE_PATH_FILE" "$archive_dir/manifest.json" "$repo" "${EXPECTED_COMMIT:-}" <<'PY'
import json
import os
import sys

path_file, manifest_path, repo, expected_commit = sys.argv[1:]
with open(path_file, "rb") as handle:
    paths = [item.decode("utf-8", "surrogateescape") for item in handle.read().split(b"\0") if item]

protected_exact = {
    "assets/config.yaml",
    "assets/refactor.db",
    "assets/refactor.db-shm",
    "assets/refactor.db-wal",
}
bad = [
    path
    for path in paths
    if path.startswith("/")
    or "\x00" in path
    or path.startswith("assets/")
    or path in protected_exact
    or any(part == ".." for part in path.split("/"))
]
if bad:
    raise SystemExit(f"refusing protected or unsafe archive path: {bad[0]}")

entries = []
for path in paths:
    full_path = os.path.join(repo, path)
    entries.append(
        {
            "path": path,
            "exists": os.path.exists(full_path),
            "is_file": os.path.isfile(full_path),
            "size": os.path.getsize(full_path) if os.path.isfile(full_path) else None,
        }
    )

with open(manifest_path, "w", encoding="utf-8") as handle:
    json.dump(
        {
            "expected_commit": expected_commit,
            "repo": repo,
            "path_count": len(entries),
            "paths": entries,
        },
        handle,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    handle.write("\n")
PY
tar -czf "$archive_dir/source-worktree.tgz" --null -T "$REMOTE_PATH_FILE" --ignore-failed-read
printf 'archive_dir=%s\n' "$archive_dir"
printf 'archive=%s\n' "$archive_dir/source-worktree.tgz"
printf 'manifest=%s\n' "$archive_dir/manifest.json"
REMOTE
    remote_rm_f "$remote_path_file"
}

apply_local_head() {
    local candidate_file="$1" expected_commit="$2"
    local desired_tar remote_tar env_prefix
    local candidate_paths=()
    local candidate_path
    while IFS= read -r candidate_path || [ -n "$candidate_path" ]; do
        candidate_paths+=("$candidate_path")
    done < "$candidate_file"
    if [ "${#candidate_paths[@]}" -eq 0 ]; then
        return
    fi

    desired_tar="$(mktemp)"
    trap 'rm -f "${desired_tar:-}"' RETURN
    git archive --format=tar "$expected_commit" -- "${candidate_paths[@]}" > "$desired_tar"

    remote_tar="$(remote_mktemp)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "cat > $(printf '%q' "$remote_tar")" < "$desired_tar"

    REMOTE_TAR_FILE="$remote_tar"
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s" <<'REMOTE'
set -euo pipefail
repo="${REMOTE_REPO:-$HOME/idol-bbq-utils}"
cd "$repo"
tar -xf "$REMOTE_TAR_FILE"
rm -f "$REMOTE_TAR_FILE"
REMOTE
    rm -f "$desired_tar"
    trap - RETURN
}

delete_remote_source_paths() {
    local delete_nul_file="$1"
    local remote_path_file env_prefix

    remote_path_file="$(remote_mktemp)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "cat > $(printf '%q' "$remote_path_file")" < "$delete_nul_file"

    REMOTE_PATH_FILE="$remote_path_file"
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}python3 -" <<'REMOTE'
import os

repo = os.environ.get("REMOTE_REPO") or os.path.join(os.path.expanduser("~"), "idol-bbq-utils")
path_file = os.environ["REMOTE_PATH_FILE"]

with open(path_file, "rb") as handle:
    paths = [item.decode("utf-8", "surrogateescape") for item in handle.read().split(b"\0") if item]

protected_exact = {
    "assets/config.yaml",
    "assets/refactor.db",
    "assets/refactor.db-shm",
    "assets/refactor.db-wal",
}


def unsafe(path):
    return (
        path.startswith("/")
        or "\x00" in path
        or path.startswith("assets/")
        or path in protected_exact
        or any(part == ".." for part in path.split("/"))
    )


for path in paths:
    if unsafe(path):
        raise SystemExit(f"refusing protected or unsafe delete path: {path}")
    full_path = os.path.join(repo, path)
    if not os.path.lexists(full_path):
        continue
    if os.path.isdir(full_path) and not os.path.islink(full_path):
        raise SystemExit(f"refusing to delete directory source path: {path}")
    os.remove(full_path)
REMOTE
    remote_rm_f "$remote_path_file"
}

usage() {
    cat <<'HELP'
Usage: tools/forwarder-remote-converge.sh [--dry-run|--archive-only|--apply --yes]

Builds a guarded convergence plan for the dirty remote idol-bbq-utils worktree.
The desired source state is the current local HEAD. Runtime assets such as
assets/config.yaml, database files, and assets/backups are always protected.

Modes:
  --dry-run       Print the plan only. This is the default and performs no
                  remote writes.
  --archive-only  Archive the remote source candidate files and stop.
  --apply         Archive remote source candidates, replace source paths with
                  the current local HEAD, and delete remote source paths absent
                  from local HEAD. Requires --yes, a clean local worktree, and
                  a pushed HEAD unless
                  SKIP_UPSTREAM_CHECK=1 is set.

Environment:
  REMOTE_HOST=3020e
  REMOTE_REPO=                  # defaults remotely to $HOME/idol-bbq-utils
  REMOTE_ARCHIVE_ROOT=          # defaults remotely to
                                # $HOME/idol-bbq-utils-archives/remote-convergence
  SKIP_UPSTREAM_CHECK=0
HELP
}

main() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --help)
                usage
                return
                ;;
            --dry-run)
                MODE="dry-run"
                ;;
            --archive-only)
                MODE="archive-only"
                ;;
            --apply)
                MODE="apply"
                ;;
            --yes)
                YES="1"
                ;;
            *)
                die "unknown argument: $1"
                ;;
        esac
        shift
    done

    git rev-parse --is-inside-work-tree >/dev/null || die "must run inside a git worktree"

    local expected_commit expected_short drift_json replace_file replace_nul_file delete_file delete_nul_file archive_nul_file protected_file manual_file replace_count delete_count archive_count manual_count
    expected_commit="$(git rev-parse HEAD)"
    expected_short="$(git rev-parse --short=7 HEAD)"
    drift_json="$(mktemp)"
    replace_file="$(mktemp)"
    replace_nul_file="$(mktemp)"
    delete_file="$(mktemp)"
    delete_nul_file="$(mktemp)"
    archive_nul_file="$(mktemp)"
    protected_file="$(mktemp)"
    manual_file="$(mktemp)"
    trap 'rm -f "${drift_json:-}" "${replace_file:-}" "${replace_nul_file:-}" "${delete_file:-}" "${delete_nul_file:-}" "${archive_nul_file:-}" "${protected_file:-}" "${manual_file:-}"' EXIT

    build_plan "$drift_json" "$replace_file" "$replace_nul_file" "$delete_file" "$delete_nul_file" "$archive_nul_file" "$protected_file" "$manual_file"

    replace_count="$(wc -l < "$replace_file" | tr -d ' ')"
    delete_count="$(wc -l < "$delete_file" | tr -d ' ')"
    archive_count="$(
        python3 - "$archive_nul_file" <<'PY'
import sys
with open(sys.argv[1], "rb") as handle:
    print(sum(1 for item in handle.read().split(b"\0") if item))
PY
    )"
    manual_count="$(wc -l < "$manual_file" | tr -d ' ')"

    if [ "$manual_count" != "0" ] && [ "$MODE" = "apply" ]; then
        die "manual-review paths are present; refusing --apply"
    fi

    case "$MODE" in
        dry-run)
            printf 'would_archive_source_paths=%s\n' "$archive_count"
            printf 'would_delete_remote_source_paths=%s\n' "$delete_count"
            printf 'would_replace_from_local_head=false\n'
            ;;
        archive-only)
            if [ "$archive_count" = "0" ]; then
                printf 'archive_skipped=no_candidate_paths\n'
            else
                create_remote_archive "$archive_nul_file" "$expected_commit" "$expected_short"
            fi
            printf 'replace_from_local_head=false\n'
            ;;
        apply)
            if [ "$YES" != "1" ]; then
                die "--apply requires --yes"
            fi
            require_clean_local_worktree
            require_pushed_head
            if [ "$archive_count" = "0" ]; then
                printf 'archive_skipped=no_candidate_paths\n'
            else
                create_remote_archive "$archive_nul_file" "$expected_commit" "$expected_short"
            fi
            if [ "$delete_count" != "0" ]; then
                delete_remote_source_paths "$delete_nul_file"
            fi
            if [ "$replace_count" != "0" ]; then
                apply_local_head "$replace_file" "$expected_commit"
            fi
            printf 'replace_from_local_head=true\n'
            printf 'replaced_source_paths=%s\n' "$replace_count"
            printf 'deleted_remote_source_paths=%s\n' "$delete_count"
            ;;
        *)
            die "invalid mode: $MODE"
            ;;
    esac
}

main "$@"
