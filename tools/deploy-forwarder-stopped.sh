#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_HOST="${REMOTE_HOST:-3020e}"
REMOTE_REPO="${REMOTE_REPO:-}"
IMAGE_NAME="${IMAGE_NAME:-idol-bbq-utils-spider:latest}"
COMPOSE_SERVICE="${COMPOSE_SERVICE:-spider}"
CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
DEPLOY_RUNTIME_MODE="${DEPLOY_RUNTIME_MODE:-offline}"
DEPLOY_OUTBOUND_SEND_MODE="${DEPLOY_OUTBOUND_SEND_MODE:-live}"
BUILD_DIR_PREFIX="${BUILD_DIR_PREFIX:-/tmp/idol-bbq-utils-build}"
SKIP_UPSTREAM_CHECK="${SKIP_UPSTREAM_CHECK:-0}"
LOCAL_CONFIG_PATH="${LOCAL_CONFIG_PATH:-assets/config.yaml}"
EXPECTED_CONFIG_SHA256="${EXPECTED_CONFIG_SHA256:-}"

if [ -n "${SSH_OPTS:-}" ]; then
    # shellcheck disable=SC2206
    SSH_ARGS=(${SSH_OPTS})
else
    SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout=10)
fi

die() {
    printf 'deploy-forwarder-stopped: %s\n' "$*" >&2
    exit 1
}

run() {
    printf '+ %s\n' "$*" >&2
    "$@"
}

require_clean_local_worktree() {
    local status
    status="$(git status --porcelain=v1 --untracked-files=normal)"
    if [ -n "$status" ]; then
        printf '%s\n' "$status" >&2
        die "local worktree is dirty; commit or stash changes before building a deployable image"
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
        die "HEAD is $ahead commit(s) ahead of $upstream; push before deploy for traceability"
    fi
}

require_local_config_hash() {
    if [ -z "$EXPECTED_CONFIG_SHA256" ]; then
        if [ ! -f "$LOCAL_CONFIG_PATH" ]; then
            die "local config not found: $LOCAL_CONFIG_PATH"
        fi
        EXPECTED_CONFIG_SHA256="$(shasum -a 256 "$LOCAL_CONFIG_PATH" | awk '{ print $1 }')"
    fi
}

remote_env_prefix() {
    local name value
    for name in REMOTE_REPO BUILD_DIR DEPLOY_COMMIT IMAGE_NAME COMPOSE_SERVICE CONTAINER_NAME DEPLOY_RUNTIME_MODE DEPLOY_OUTBOUND_SEND_MODE EXPECTED_CONFIG_SHA256; do
        value="${!name:-}"
        if [ -n "$value" ]; then
            printf '%s=%q ' "$name" "$value"
        fi
    done
}

ssh_remote() {
    local env_prefix
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s"
}

remote_dirty_summary() {
    local env_prefix
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s" <<'REMOTE' || true
set -euo pipefail
repo="${REMOTE_REPO:-$HOME/idol-bbq-utils}"
cd "$repo"
tracked_count="$(git status --porcelain=v1 --untracked-files=no | wc -l | tr -d ' ')"
untracked_count="$(git status --porcelain=v1 --untracked-files=normal | awk '$1 == "??" { count += 1 } END { print count + 0 }')"
printf 'remote_dirty_tracked=%s remote_dirty_untracked=%s\n' "$tracked_count" "$untracked_count"
REMOTE
}

main() {
    if [ "${1:-}" = "--help" ]; then
        cat <<'HELP'
Usage: tools/deploy-forwarder-stopped.sh

Builds idol-bbq-utils from the current committed HEAD using a git archive,
pushes the archive to a clean remote /tmp build directory, rebuilds the Docker
image on the remote host, recreates the compose service with --no-start, and
verifies the forwarder container remains stopped with restart=no.

Environment:
  REMOTE_HOST=3020e
  REMOTE_REPO=            # defaults remotely to $HOME/idol-bbq-utils
  IMAGE_NAME=idol-bbq-utils-spider:latest
  COMPOSE_SERVICE=spider
  CONTAINER_NAME=forwarder-new
  DEPLOY_RUNTIME_MODE=offline
  DEPLOY_OUTBOUND_SEND_MODE=live
  BUILD_DIR_PREFIX=/tmp/idol-bbq-utils-build
  SKIP_UPSTREAM_CHECK=0
  LOCAL_CONFIG_PATH=assets/config.yaml
  EXPECTED_CONFIG_SHA256=  # overrides the local config hash when set

This script refuses to run from a dirty local worktree, an unpushed HEAD, a
remote runtime config that differs from the local intended config, or a recreated
container missing processor env keys declared in the runtime config.
It never builds from the remote tracked worktree.
HELP
        return
    fi

    git rev-parse --is-inside-work-tree >/dev/null || die "must run inside a git worktree"
    require_clean_local_worktree
    require_pushed_head
    require_local_config_hash

    local commit short build_dir
    commit="$(git rev-parse HEAD)"
    short="$(git rev-parse --short=7 HEAD)"
    build_dir="${BUILD_DIR_PREFIX}-${short}"

    printf 'deploy_commit=%s\n' "$commit"
    printf 'expected_config_sha256=%s\n' "$EXPECTED_CONFIG_SHA256"
    remote_dirty_summary

    run ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "rm -rf $(printf '%q' "$build_dir") && mkdir -p $(printf '%q' "$build_dir")"
    git archive --format=tar HEAD | ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "tar -xf - -C $(printf '%q' "$build_dir")"

    BUILD_DIR="$build_dir" \
    DEPLOY_COMMIT="$commit" \
    IMAGE_NAME="$IMAGE_NAME" \
    COMPOSE_SERVICE="$COMPOSE_SERVICE" \
    CONTAINER_NAME="$CONTAINER_NAME" \
    DEPLOY_RUNTIME_MODE="$DEPLOY_RUNTIME_MODE" \
    DEPLOY_OUTBOUND_SEND_MODE="$DEPLOY_OUTBOUND_SEND_MODE" \
        ssh_remote <<'REMOTE'
set -euo pipefail
repo="${REMOTE_REPO:-$HOME/idol-bbq-utils}"
config_path="$repo/assets/config.yaml"
remote_config_sha256="$(sha256sum "$config_path" | awk '{ print $1 }')"
if [ "$remote_config_sha256" != "$EXPECTED_CONFIG_SHA256" ]; then
    printf 'runtime config sha256 mismatch: expected=%s actual=%s path=%s\n' "$EXPECTED_CONFIG_SHA256" "$remote_config_sha256" "$config_path" >&2
    exit 1
fi
cd "$BUILD_DIR"
printf '%s\n' "$DEPLOY_COMMIT" > .codex-build-commit
build_date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
docker build \
    --build-arg "BUILD_COMMIT=$DEPLOY_COMMIT" \
    --build-arg "BUILD_DATE=$build_date" \
    -t "$IMAGE_NAME" \
    -f app/tweet-forwarder/Dockerfile .
image_id="$(docker image inspect "$IMAGE_NAME" --format '{{.Id}}')"
compose_override="$(mktemp "$BUILD_DIR/compose-stopped-override.XXXXXX.yaml")"
cat > "$compose_override" <<OVERRIDE
services:
  $COMPOSE_SERVICE:
    image: "$IMAGE_NAME"
    restart: "no"
    environment:
      IDOL_BBQ_RUNTIME_MODE: "$DEPLOY_RUNTIME_MODE"
      IDOL_BBQ_OUTBOUND_SEND_MODE: "$DEPLOY_OUTBOUND_SEND_MODE"
OVERRIDE
cd "$repo"
IDOL_BBQ_RUNTIME_MODE="$DEPLOY_RUNTIME_MODE" IDOL_BBQ_OUTBOUND_SEND_MODE="$DEPLOY_OUTBOUND_SEND_MODE" IDOL_BBQ_RESTART_POLICY=no \
    docker compose \
        --project-directory "$repo" \
        -f "$BUILD_DIR/docker-compose.yaml" \
        -f "$compose_override" \
        up --no-start --force-recreate --no-build "$COMPOSE_SERVICE"
docker update --restart=no "$CONTAINER_NAME" >/dev/null
status="$(docker inspect "$CONTAINER_NAME" --format 'status={{.State.Status}} running={{.State.Running}} restart={{.HostConfig.RestartPolicy.Name}} image={{.Image}}')"
stop_timeout="$(docker inspect "$CONTAINER_NAME" --format '{{.Config.StopTimeout}}')"
runtime_mode="$(docker inspect "$CONTAINER_NAME" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' | awk -F= '$1 == "IDOL_BBQ_RUNTIME_MODE" { print $2; found=1 } END { if (!found) print "" }')"
outbound_send_mode="$(docker inspect "$CONTAINER_NAME" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' | awk -F= '$1 == "IDOL_BBQ_OUTBOUND_SEND_MODE" { print $2; found=1 } END { if (!found) print "" }')"
container_env_tmp="$(mktemp)"
docker inspect "$CONTAINER_NAME" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' > "$container_env_tmp"
processor_env_status="$(python3 - "$config_path" "$container_env_tmp" <<'PY'
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
missing = [name for name in sorted(set(required)) if not env.get(name)]
print("missing:" + ",".join(missing) if missing else "ok")
PY
)"
rm -f "$container_env_tmp"
printf '%s\n' "$status"
printf 'stop_timeout=%s\n' "$stop_timeout"
printf 'runtime_mode=%s\n' "$runtime_mode"
printf 'outbound_send_mode=%s\n' "$outbound_send_mode"
printf 'processor_env_status=%s\n' "$processor_env_status"
case "$status" in
    *'running=false'*'restart=no'*)
        ;;
    *)
        printf 'container is not safely stopped after recreate: %s\n' "$status" >&2
        exit 1
        ;;
esac
if [ "$runtime_mode" != "$DEPLOY_RUNTIME_MODE" ]; then
    printf 'container runtime mode mismatch after recreate: expected=%s actual=%s\n' "$DEPLOY_RUNTIME_MODE" "$runtime_mode" >&2
    exit 1
fi
if [ "$outbound_send_mode" != "$DEPLOY_OUTBOUND_SEND_MODE" ]; then
    printf 'container outbound send mode mismatch after recreate: expected=%s actual=%s\n' "$DEPLOY_OUTBOUND_SEND_MODE" "$outbound_send_mode" >&2
    exit 1
fi
if [ "$processor_env_status" != "ok" ]; then
    printf 'container processor env keys missing after recreate: %s\n' "$processor_env_status" >&2
    exit 1
fi
printf 'commit=%s\n' "$DEPLOY_COMMIT"
printf 'image=%s\n' "$image_id"
printf 'build_date=%s\n' "$build_date"
printf 'build_dir=%s\n' "$BUILD_DIR"
printf 'compose_file=%s\n' "$BUILD_DIR/docker-compose.yaml"
printf 'compose_project_directory=%s\n' "$repo"
REMOTE
}

main "$@"
