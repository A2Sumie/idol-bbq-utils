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

    local deploy preflight start dockerfile drill drift converge capture_smoke health_audit
    deploy="tools/deploy-forwarder-stopped.sh"
    preflight="tools/forwarder-preflight.sh"
    start="app/tweet-forwarder/start.sh"
    dockerfile="app/tweet-forwarder/Dockerfile"
    drill="tools/forwarder-db-backup-drill.sh"
    drift="tools/forwarder-remote-drift.sh"
    converge="tools/forwarder-remote-converge.sh"
    capture_smoke="tools/forwarder-capture-smoke.sh"
    health_audit="app/tweet-forwarder/scripts/crawler-health-audit.ts"

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
    require_contains "$deploy" 'IDOL_BBQ_OUTBOUND_SEND_MODE="$DEPLOY_OUTBOUND_SEND_MODE"' \
        'stopped deploy outbound send-mode override'
    require_not_contains "$deploy" '-f docker-compose.yaml' \
        'remote checkout compose fallback'

    require_contains "$preflight" '.HostConfig.Binds' \
        'created-container bind inspection'
    require_contains "$preflight" 'mount_app_backups_exists' \
        'backup bind preflight visibility'
    require_contains "$preflight" 'EXPECTED_RUNTIME_MODE' \
        'runtime-mode preflight expectation'
    require_contains "$preflight" 'EXPECTED_OUTBOUND_SEND_MODE' \
        'outbound send-mode preflight expectation'
    require_contains "$preflight" 'EXPECTED_RUNNING' \
        'container running-state preflight expectation'
    require_contains "$preflight" 'EXPECTED_RESTART_POLICY' \
        'container restart-policy preflight expectation'
    require_contains "$preflight" 'EXPECTED_STOP_TIMEOUT_SECONDS' \
        'container stop-timeout preflight expectation'
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
    require_contains "$start" 'IDOL_BBQ_DB_BACKUP_DIR:-/app/backups/db-migrations' \
        'persistent startup migration backup default'
    require_contains "$start" 'IDOL_BBQ_OUTBOUND_SEND_MODE' \
        'startup outbound send-mode guard'
    require_contains "$start" 'outbound_send_mode="capture"' \
        'startup outbound capture mode guard'
    require_contains "$start" 'PRAGMA quick_check' \
        'startup sqlite quick_check'
    require_contains "$start" 'backup_method=%s' \
        'startup sqlite backup manifest field'
    require_contains "$start" 'sqlite_backup_api' \
        'startup sqlite backup manifest method'

    require_contains "docker-compose.yaml" 'IDOL_BBQ_DB_BACKUP_DIR=${IDOL_BBQ_DB_BACKUP_DIR:-/app/backups/db-migrations}' \
        'compose persistent migration backup env'
    require_contains "docker-compose.yaml" 'IDOL_BBQ_OUTBOUND_SEND_MODE=${IDOL_BBQ_OUTBOUND_SEND_MODE:-live}' \
        'compose outbound send-mode env'
    require_contains "docker-compose.yaml" 'IDOL_BBQ_OUTBOUND_CAPTURE_URL=${IDOL_BBQ_OUTBOUND_CAPTURE_URL:-}' \
        'compose outbound capture receiver env'
    require_contains "docker-compose.yaml" 'IDOL_BBQ_OUTBOUND_CAPTURE_FILE=${IDOL_BBQ_OUTBOUND_CAPTURE_FILE:-}' \
        'compose outbound capture file env'
    require_contains "docker-compose.yaml" "stop_grace_period: '90s'" \
        'compose stop grace period'
    require_contains "$preflight" 'backup_container_dir' \
        'preflight backup container env resolution'

    require_contains "app/tweet-forwarder/update-media-tools.sh" 'BILIUP_PACKAGE="${BILIUP_PACKAGE:-biliup}"' \
        'upstream biliup package selector'
    require_contains "app/tweet-forwarder/update-media-tools.sh" 'from biliup.plugins.bili_webup import BiliBili, BiliWeb, Data' \
        'biliup helper import self-test'
    require_contains "$preflight" 'db_backup_host_dir' \
        'preflight backup host path visibility'
    require_contains "$drill" '?mode=ro' \
        'backup drill read-only source DB'
    require_contains "$drill" 'source.backup(backup)' \
        'backup drill sqlite backup API'
    require_contains "$drill" 'PRAGMA quick_check' \
        'backup drill sqlite quick_check'
    require_contains "$drill" 'prisma migrate status' \
        'backup drill migration status check'

    require_contains "$drift" 'remote_head_source_files' \
        'remote drift clean-source inventory'
    require_contains "$drift" 'remote_head_differs_from_local_head' \
        'remote drift clean changed-source relation'
    require_contains "$drift" 'absent_on_remote_head' \
        'remote drift local-only source relation'
    require_contains "$drift" 'absent_in_local_head' \
        'remote drift remote-only source relation'
    require_contains "$drift" 'source_absent_as_desired' \
        'remote drift deleted source idempotence relation'
    require_contains "$converge" 'local_head_source' \
        'remote convergence local HEAD source expansion'
    require_contains "$converge" 'delete_remote_source_paths' \
        'remote convergence remote-only source deletion'
    require_contains "$converge" 'delete_source_paths' \
        'remote convergence delete count visibility'
    require_contains "$converge" 'no_action_source_paths' \
        'remote convergence no-action count visibility'
    require_contains "$converge" 'archive_source_paths' \
        'remote convergence archive count visibility'
    require_contains "$converge" 'import subprocess' \
        'remote convergence local HEAD source scan import'

    require_contains "$capture_smoke" 'CAPTURE_SMOKE_REQUIRE_PRODUCTION_STOPPED' \
        'capture smoke production-stopped guard'
    require_contains "$capture_smoke" 'CAPTURE_SMOKE_TARGET_IDS' \
        'capture smoke target allowlist option'
    require_contains "$capture_smoke" 'TEMP_TARGET_IDS' \
        'capture smoke temporary config target filter'
    require_contains "$capture_smoke" 'allowed-targets.txt' \
        'capture smoke allowed target evidence file'
    require_contains "$capture_smoke" 'IDOL_BBQ_OUTBOUND_SEND_MODE=capture' \
        'capture smoke outbound send-mode override'
    require_contains "$capture_smoke" 'sqlite_backup "$repo/assets/refactor.db" "$tmp_db"' \
        'capture smoke temporary DB copy'
    require_contains "$capture_smoke" 'prepare_temp_db "$tmp_db" "$db_prepare_file"' \
        'capture smoke temp DB background task suppression'
    require_contains "$capture_smoke" '"$tmp_config:/app/config.yaml:ro"' \
        'capture smoke read-only temporary config mount'
    require_contains "$capture_smoke" '"$repo/assets/cookies:/app/assets/cookies:ro"' \
        'capture smoke read-only production cookies mount'
    require_contains "$capture_smoke" 'rm -f "$tmp_config" "$auth_header"' \
        'capture smoke secret-bearing artifact cleanup'
    require_contains "$capture_smoke" 'production container is not stopped' \
        'capture smoke production stop refusal'
    require_contains "$capture_smoke" 'unmatched_capture_count' \
        'capture smoke no unrelated capture assertion'
    require_contains "$capture_smoke" 'disallowed_capture_count' \
        'capture smoke target allowlist capture assertion'
    require_contains "$capture_smoke" 'disallowed_outbound_count' \
        'capture smoke target allowlist outbound assertion'
    require_contains "$health_audit" '--no-live-probe' \
        'crawler health audit static-only option'
    require_contains "$health_audit" 'liveProbe: args.liveProbe' \
        'crawler health audit live-probe toggle wiring'

    python3 - "$dockerfile" <<'PY'
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    lines = handle.readlines()


def find(needle):
    for index, line in enumerate(lines, start=1):
        if needle in line:
            return index
    raise SystemExit(f"forwarder-operator-contracts: missing Dockerfile invariant: {needle}")


chown_line = find("chown -R bun:bun /app/")
build_arg_line = find("ARG BUILD_COMMIT=unknown")
label_line = find('LABEL moe.n2nj.idol-bbq.build-commit="${BUILD_COMMIT}"')
build_file_line = find("/app/build-commit")

if build_arg_line < chown_line:
    raise SystemExit(
        "forwarder-operator-contracts: BUILD_COMMIT arg must stay after heavy runner layers"
    )
if label_line < chown_line:
    raise SystemExit(
        "forwarder-operator-contracts: build labels must stay after heavy runner layers"
    )
if build_file_line < chown_line:
    raise SystemExit(
        "forwarder-operator-contracts: build metadata files must stay after heavy runner layers"
    )
PY

    printf 'operator_contracts_ok=true\n'
}

main "$@"
