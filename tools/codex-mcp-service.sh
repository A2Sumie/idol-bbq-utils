#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Intended for MCP clients or the idol-bbq host bridge that need Codex Pro as
# an enhancement service. Keep defaults non-interactive; override through env
# when a tighter sandbox is desired.
codex_approval="${CODEX_MCP_APPROVAL:-never}"
codex_sandbox="${CODEX_MCP_SANDBOX:-workspace-write}"
codex_model="${CODEX_MCP_MODEL:-}"

args=(
    -c "approval_policy=\"${codex_approval}\""
    -c "sandbox_mode=\"${codex_sandbox}\""
)

if [ -n "$codex_model" ]; then
    args+=(-c "model=\"${codex_model}\"")
fi

cd "$repo_root"
exec codex mcp-server "${args[@]}"
