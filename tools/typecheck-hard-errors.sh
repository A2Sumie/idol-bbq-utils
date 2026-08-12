#!/usr/bin/env bash
# Catches structural TypeScript errors that `bun build` silently accepts
# (duplicate object keys, redeclarations, unreachable-case collisions) without
# requiring the whole tree to be type-clean yet. The full 300+ pre-existing
# errors are tracked separately; this gate only fails on error codes that have
# caused real production bugs (e.g. the duplicate `articleStateLookup` key that
# silently disabled the Website TTL dedup).
set -Eeuo pipefail

output="$(bunx tsc --noEmit -p app/tweet-forwarder 2>&1 || true)"

if echo "$output" | grep -qE 'error TS(1117|2451|2300|2440|2454|2462|2783)'; then
    echo "$output" | grep -E 'error TS(1117|2451|2300|2440|2454|2462|2783)'
    echo 'typecheck-hard-errors: structural TS errors found (see above)' >&2
    exit 1
fi

echo 'typecheck-hard-errors: ok'
