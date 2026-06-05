#!/usr/bin/env bash
set -Eeuo pipefail

RUN_REMOTE_PREFLIGHT="${RUN_REMOTE_PREFLIGHT:-0}"
RUN_REMOTE_DRIFT_COMPARE="${RUN_REMOTE_DRIFT_COMPARE:-0}"

run() {
    printf '+ %s\n' "$*" >&2
    "$@"
}

main() {
    if [ "${1:-}" = "--help" ]; then
        cat <<'HELP'
Usage: tools/forwarder-green-gate.sh

Runs the practical local verification gate for idol-bbq forwarder remediation.
It intentionally does not start production services.

Environment:
  RUN_REMOTE_PREFLIGHT=0       # set 1 to run STRICT_COMMIT preflight on 3020e
  RUN_REMOTE_DRIFT_COMPARE=0   # set 1 to compare remote dirty hashes to local HEAD
HELP
        return
    fi

    git rev-parse --is-inside-work-tree >/dev/null

    run git diff --check
    run bash -n \
        tools/deploy-forwarder-stopped.sh \
        tools/forwarder-capture-smoke.sh \
        tools/forwarder-db-backup-drill.sh \
        tools/forwarder-operator-contracts.sh \
        tools/forwarder-preflight.sh \
        tools/forwarder-remote-drift.sh \
        tools/forwarder-remote-converge.sh \
        tools/forwarder-green-gate.sh
    run sh -n app/tweet-forwarder/start.sh
    run bash tools/forwarder-operator-contracts.sh
    run bun run audit:config -- --fail-on-diagnostics
    run bun test
    run bun --filter @idol-bbq-utils/tweet-forwarder build
    run bun build app/tweet-forwarder/scripts/config-audit.ts \
        --outdir=/tmp/idol-bbq-audit-build \
        --target=bun \
        --minify
    run bun build app/tweet-forwarder/scripts/crawler-health-audit.ts \
        --outdir=/tmp/idol-bbq-crawler-health-audit-build \
        --target=bun \
        --minify
    run bun build app/tweet-forwarder/scripts/crawler-cookie-export-audit.ts \
        --outdir=/tmp/idol-bbq-crawler-cookie-export-audit-build \
        --target=bun \
        --minify

    if [ "$RUN_REMOTE_PREFLIGHT" = "1" ]; then
        run env STRICT_COMMIT=1 bun run preflight:forwarder
    fi

    if [ "$RUN_REMOTE_DRIFT_COMPARE" = "1" ]; then
        run tools/forwarder-remote-drift.sh --compare-local-head
    fi
}

main "$@"
