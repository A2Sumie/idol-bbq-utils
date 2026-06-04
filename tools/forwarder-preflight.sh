#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_HOST="${REMOTE_HOST:-3020e}"
REMOTE_REPO="${REMOTE_REPO:-}"
IMAGE_NAME="${IMAGE_NAME:-idol-bbq-utils-spider:latest}"
CONTAINER_NAME="${CONTAINER_NAME:-forwarder-new}"
EXPECTED_COMMIT="${EXPECTED_COMMIT:-$(git rev-parse HEAD 2>/dev/null || true)}"
STRICT_COMMIT="${STRICT_COMMIT:-0}"

if [ -n "${SSH_OPTS:-}" ]; then
    # shellcheck disable=SC2206
    SSH_ARGS=(${SSH_OPTS})
else
    SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout=10)
fi

remote_env_prefix() {
    local name value
    for name in REMOTE_REPO IMAGE_NAME CONTAINER_NAME EXPECTED_COMMIT STRICT_COMMIT; do
        value="${!name:-}"
        if [ -n "$value" ]; then
            printf '%s=%q ' "$name" "$value"
        fi
    done
}

main() {
    if [ "${1:-}" = "--help" ]; then
        cat <<'HELP'
Usage: tools/forwarder-preflight.sh

Prints a no-secret remote preflight summary for the stopped forwarder container:
container status, restart policy, image id, image build commit, config audit
hashes/counts, migration head, and remote dirty worktree counts.

Environment:
  REMOTE_HOST=3020e
  REMOTE_REPO=            # defaults remotely to $HOME/idol-bbq-utils
  IMAGE_NAME=idol-bbq-utils-spider:latest
  CONTAINER_NAME=forwarder-new
  EXPECTED_COMMIT=<local HEAD>
  STRICT_COMMIT=0         # set 1 to fail when image commit != expected commit
HELP
        return
    fi

    local env_prefix
    env_prefix="$(remote_env_prefix)"
    ssh "${SSH_ARGS[@]}" "$REMOTE_HOST" "${env_prefix}bash -s" <<'REMOTE'
set -euo pipefail
repo="${REMOTE_REPO:-$HOME/idol-bbq-utils}"
config_path="$repo/assets/config.yaml"

container_status="$(docker inspect "$CONTAINER_NAME" --format '{{.State.Status}}')"
container_running="$(docker inspect "$CONTAINER_NAME" --format '{{.State.Running}}')"
restart_policy="$(docker inspect "$CONTAINER_NAME" --format '{{.HostConfig.RestartPolicy.Name}}')"
container_image="$(docker inspect "$CONTAINER_NAME" --format '{{.Image}}')"
image_build_commit="$(docker image inspect "$container_image" --format '{{ index .Config.Labels "moe.n2nj.idol-bbq.build-commit" }}' 2>/dev/null || true)"
image_oci_revision="$(docker image inspect "$container_image" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' 2>/dev/null || true)"
image_created_label="$(docker image inspect "$container_image" --format '{{ index .Config.Labels "org.opencontainers.image.created" }}' 2>/dev/null || true)"
runtime_mode="$(docker inspect "$CONTAINER_NAME" --format '{{ range .Config.Env }}{{ println . }}{{ end }}' | awk -F= '$1 == "IDOL_BBQ_RUNTIME_MODE" { print $2; found=1 } END { if (!found) print "" }')"
build_commit_file="$(docker run --rm --entrypoint cat "$container_image" /app/build-commit 2>/dev/null || true)"
migration_head="$(docker run --rm --entrypoint sh "$container_image" -lc 'find /app/prisma/migrations -mindepth 1 -maxdepth 1 -type d -printf "%f\n" | sort | tail -1')"

audit_json="$(docker run --rm --entrypoint bun -v "$config_path:/app/config.yaml:ro" "$container_image" /app/tools/config-audit.js --config /app/config.yaml --fail-on-diagnostics)"
audit_tmp="$(mktemp)"
printf '%s\n' "$audit_json" > "$audit_tmp"

cd "$repo"
remote_dirty_tracked="$(git status --porcelain=v1 --untracked-files=no | wc -l | tr -d ' ')"
remote_dirty_untracked="$(git status --porcelain=v1 --untracked-files=normal | awk '$1 == "??" { count += 1 } END { print count + 0 }')"

printf 'container_status=%s\n' "$container_status"
printf 'container_running=%s\n' "$container_running"
printf 'restart_policy=%s\n' "$restart_policy"
printf 'runtime_mode=%s\n' "$runtime_mode"
printf 'container_image=%s\n' "$container_image"
printf 'image_build_commit=%s\n' "$image_build_commit"
printf 'image_oci_revision=%s\n' "$image_oci_revision"
printf 'image_created=%s\n' "$image_created_label"
printf 'build_commit_file=%s\n' "$build_commit_file"
printf 'expected_commit=%s\n' "${EXPECTED_COMMIT:-}"
if [ -n "${EXPECTED_COMMIT:-}" ] && [ "$image_build_commit" = "$EXPECTED_COMMIT" ] && [ "$build_commit_file" = "$EXPECTED_COMMIT" ]; then
    printf 'commit_match=true\n'
else
    printf 'commit_match=false\n'
fi
python3 - "$audit_tmp" <<'PY'
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as handle:
    data = json.load(handle)

counts = data["route_graph"]["counts"]
print(f'audit_ok={str(data["ok"]).lower()}')
print(f'redacted_config_hash={data["redacted_config_hash"]}')
print(f'policy_hash={data["policy_hash"]}')
print(f'secret_field_count={data["secret_field_count"]}')
print(f'route_count={counts["routes"]}')
print(f'route_errors={counts["errors"]}')
print(f'route_warnings={counts["warnings"]}')
print(f'operational_crawlers={counts["operational_crawlers"]}')
print(f'summary_card_routes={data["route_graph"]["summary_card_routes"]}')
PY
rm -f "$audit_tmp"
printf 'migration_head=%s\n' "$migration_head"
printf 'remote_dirty_tracked=%s\n' "$remote_dirty_tracked"
printf 'remote_dirty_untracked=%s\n' "$remote_dirty_untracked"

if [ "$container_running" != "false" ] || [ "$restart_policy" != "no" ]; then
    printf 'preflight failed: container is not safely stopped\n' >&2
    exit 1
fi
if [ "$STRICT_COMMIT" = "1" ] && { [ -z "${EXPECTED_COMMIT:-}" ] || [ "$image_build_commit" != "$EXPECTED_COMMIT" ] || [ "$build_commit_file" != "$EXPECTED_COMMIT" ]; }; then
    printf 'preflight failed: image commit does not match expected commit\n' >&2
    exit 1
fi
REMOTE
}

main "$@"
