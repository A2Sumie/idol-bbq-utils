#!/usr/bin/env bash
set -Eeuo pipefail

die() {
    printf 'forwarder-operator-contracts: %s\n' "$*" >&2
    exit 1
}

require_contains() {
    local file="$1"
    local needle="$2"
    local label="$3"

    if ! grep -Fq -- "$needle" "$file"; then
        die "$label missing in $file"
    fi
}

require_not_contains() {
    local file="$1"
    local needle="$2"
    local label="$3"

    if grep -Fq -- "$needle" "$file"; then
        die "$label unexpectedly present in $file"
    fi
}

main() {
    if [ "${1:-}" = "--help" ]; then
        cat <<'HELP'
Usage: tools/forwarder-operator-contracts.sh

Runs local, no-network contract checks for forwarder operator scripts. These
checks pin safety invariants that are easy to regress in shell scripts and hard
to catch with TypeScript unit tests.
HELP
        return
    fi

    git rev-parse --is-inside-work-tree >/dev/null

    local deploy preflight start
    deploy="tools/deploy-forwarder-stopped.sh"
    preflight="tools/forwarder-preflight.sh"
    start="app/tweet-forwarder/start.sh"

    require_contains "$deploy" 'require_clean_local_worktree' \
        'local clean-worktree deploy guard'
    require_contains "$deploy" 'require_pushed_head' \
        'pushed-head deploy guard'
    require_contains "$deploy" 'git archive --format=tar HEAD' \
        'local HEAD archive deploy source'
    require_contains "$deploy" 'cd "$BUILD_DIR"' \
        'remote docker build from build archive'
    require_contains "$deploy" '-f "$BUILD_DIR/docker-compose.yaml"' \
        'stopped deploy compose from build archive'
    require_contains "$deploy" '--project-directory "$repo"' \
        'stopped deploy remote project directory'
    require_contains "$deploy" 'up --no-start --force-recreate --no-build "$COMPOSE_SERVICE"' \
        'stopped deploy no-start recreate'
    require_contains "$deploy" 'docker update --restart=no "$CONTAINER_NAME"' \
        'post-recreate restart guard'
    require_contains "$deploy" 'IDOL_BBQ_RUNTIME_MODE="$DEPLOY_RUNTIME_MODE"' \
        'stopped deploy runtime-mode override'
    require_not_contains "$deploy" '-f docker-compose.yaml' \
        'remote checkout compose fallback'

    require_contains "$preflight" '.HostConfig.Binds' \
        'created-container bind inspection'
    require_contains "$preflight" 'mount_app_backups_exists' \
        'backup bind preflight visibility'
    require_contains "$preflight" 'EXPECTED_RUNTIME_MODE' \
        'runtime-mode preflight expectation'
    require_contains "$preflight" 'STRICT_COMMIT' \
        'strict commit preflight gate'
    require_contains "$preflight" 'STRICT_MIGRATIONS' \
        'strict migration preflight gate'
    require_contains "$preflight" '?mode=ro' \
        'read-only sqlite preflight'
    require_contains "$preflight" 'PRAGMA quick_check' \
        'sqlite quick_check preflight'

    require_contains "$start" 'IDOL_BBQ_RUN_MIGRATIONS' \
        'startup migration feature flag'
    require_contains "$start" 'IDOL_BBQ_ALLOW_NON_ONLINE_MIGRATIONS' \
        'non-online migration refusal guard'
    require_contains "$start" 'IDOL_BBQ_REQUIRE_EXISTING_DB_FOR_MIGRATION' \
        'existing DB migration guard'
    require_contains "$start" 'PRAGMA quick_check' \
        'startup sqlite quick_check'
    require_contains "$start" 'backup_method=%s' \
        'startup sqlite backup manifest field'
    require_contains "$start" 'sqlite_backup_api' \
        'startup sqlite backup manifest method'

    printf 'operator_contracts_ok=true\n'
}

main "$@"
