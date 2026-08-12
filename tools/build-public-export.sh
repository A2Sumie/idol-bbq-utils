#!/usr/bin/env bash
# Builds a credentials-free, migratable copy of idol-bbq-utils from the current
# committed HEAD:
#   - exports only git-tracked files (git archive, so no history, no gitignored
#     secrets, no cookies/db/env files)
#   - removes deployment/ops-specific files (hostnames, tunnels, monitors)
#   - writes .env.example + MIGRATION.md
#   - scans the result for credential patterns and fails loudly if any remain
#   - initializes a fresh git repository with a single commit and a neutral
#     author, so no private history ever leaves this machine
#
# Usage:
#   tools/build-public-export.sh [TARGET_DIR]
#   (default TARGET_DIR: /tmp/idol-bbq-utils-public)
#
# The output directory is intentionally NOT pushed anywhere by this script.
set -Eeuo pipefail

TARGET_DIR="${1:-/tmp/idol-bbq-utils-public}"
REPO_ROOT="$(git rev-parse --show-toplevel)"

# Paths that must not appear in a public copy (deployment/ops-specific or
# containing real credentials/identifiers). Everything else stays.
EXCLUDE_PATHS=(
    DEPLOYMENT.md
    'app/tweet-forwarder/OPERATIONS.md'
    rewrite_history.sh
    'assets/tweet-forwarder/x.cookies'
    'assets/tweet-forwarder/data.db'
    tools/deploy-forwarder-stopped.sh
    tools/forwarder-preflight.sh
    tools/forwarder-remote-drift.sh
    tools/forwarder-remote-converge.sh
    tools/forwarder-green-gate.sh
    tools/forwarder-capture-smoke.sh
    tools/forwarder-db-backup-drill.sh
    tools/forwarder-cookie-maintenance.sh
    tools/youtube-cookie-keepalive.sh
    tools/forwarder-runtime-api.sh
    tools/schedule-ig-live.sh
    tools/schedule-ig-live-watch.sh
    tools/schedule-tiktok-live.sh
    tools/verify-ig-live-schedule.sh
    tools/instagram-live-monitor.sh
    tools/tiktok-live-monitor.sh
    tools/migrate-x570-showroom-ondemand.sh
    tools/migrate-x570-showroom-plans.mjs
    tools/sync-22-7-x570-prompt-knowledge.mjs
    tools/refresh-22-7-knowledge.mjs
    tools/instagram-mpd-probe.py
)

# Patterns that must never appear in the public copy. Value-shaped patterns only:
# field declarations like `sessdata?: string` are legitimate source code.
SECRET_PATTERNS=(
    'sk-[A-Za-z0-9]{12,}'
    'sessdata[:=][[:space:]]*["'"'"']?[A-Za-z0-9%]{8,}'
    'bili_jct[:=][[:space:]]*["'"'"']?[0-9a-fA-F]{8,}'
    'N2NJ_SUPER_SECRET'
    '12qwaszx'
    'stpuie'
    '3020e'
    'sumie@'
    'n2nj\.moe'
    'cloudflared'
    'ZOUdeMacBook'
    'idol-bbq-internal'
)

NEUTRAL_AUTHOR='idol-bbq-utils <idol-bbq-utils@users.noreply.github.com>'

rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"

git archive --format=tar HEAD | tar -xf - -C "$TARGET_DIR"

for path in "${EXCLUDE_PATHS[@]}"; do
    rm -rf "$TARGET_DIR/$path"
done

# The bundled example configs reference a sample cookie path that no longer
# ships with the repo; point them at the gitignored cookie directory instead.
if [ -d "$TARGET_DIR/assets/tweet-forwarder" ]; then
    find "$TARGET_DIR/assets/tweet-forwarder" -maxdepth 1 -name 'config.example*.yaml' -print0 \
        | xargs -0 sed -i '' 's|./assets/tweet-forwarder/x.cookies|./assets/cookies/x.cookies.txt|g' 2>/dev/null || true
fi

# Replace deployment-specific defaults (domains/UA/host comments) with neutral
# placeholders in the exported copy. Production source keeps its own defaults;
# the export is scrubbed at build time.
apply_scrub() {
    find "$TARGET_DIR" -type f \( \
        -name '*.ts' -o -name '*.js' -o -name '*.tsx' -o -name '*.md' -o -name '*.yaml' -o -name '*.yml' \
        -o -name '*.json' -o -name '*.py' -o -name '*.sh' -o -name 'Dockerfile' -o -name '*.mjs' \) \
        -print0 | xargs -0 sed -i '' \
        -e 's|cic\.n2nj\.moe|cic.example.com|g' \
        -e 's|drop\.n2nj\.moe|drop.example.com|g' \
        -e 's|tv\.n2nj\.moe|live.example.com|g' \
        -e 's|stream\.n2nj\.moe|stream.example.com|g' \
        -e 's|N2NJ-Stream-Bot/1\.0|IdolBBQ-RelayBot/1.0|g' \
        -e 's|actual host (3020e)|deployment host|g' \
        -e 's|3020e production DB|production DB|g' \
        2>/dev/null || true
}
apply_scrub

failures="$(grep -rInE "$(IFS='|'; echo "${SECRET_PATTERNS[*]}")" "$TARGET_DIR" 2>/dev/null || true)"
if [ -n "$failures" ]; then
    printf 'public-export: credential patterns found, aborting:\n%s\n' "$failures" >&2
    exit 1
fi

cat > "$TARGET_DIR/.env.example" <<'ENV'
# Copy to .env (or export in the shell) and fill in your own values.
# Every value is optional unless marked required.

# Runtime
IDOL_BBQ_RUNTIME_MODE=offline        # offline | online | api-only
IDOL_BBQ_OUTBOUND_SEND_MODE=live     # live | blocked | dry_run
IDOL_BBQ_RESTART_POLICY=no           # docker restart policy

# API control plane (required to enable the management API)
API_SECRET=

# Processor providers (any you actually use)
DEEPSEEK_API_KEY=
TENCENT_HUNYUAN_API_KEY=
OPENCODE_GO_API_KEY=

# X web client constants (defaults are the public web-client values)
X_PUBLIC_TOKEN=
X_GRAPHQL_PUBLIC_TOKEN=
X_GUEST_TOKEN=

# Message board (optional; disabled unless enabled)
IDOL_BBQ_MESSAGEBOARD_ENABLED=
UIE_PASSWORD=

# Live player relay (only needed by the live-relay services)
LIVE_PLAYER_SCHEDULE_WEBHOOK_URL=
LIVE_PLAYER_SCHEDULE_WEBHOOK_API_KEY=
LIVE_PLAYER_SCHEDULE_WEBHOOK_USER_AGENT=
LIVE_PLAYER_SCHEDULE_WAF_BYPASS_HEADER=

# Browser
PUPPETEER_EXECUTABLE_PATH=
ENV

cat > "$TARGET_DIR/MIGRATION.md" <<'MD'
# Migrating to your own deployment

idol-bbq-utils is a multi-platform article crawler + formatter + forwarder
(X/Twitter, Instagram, TikTok, YouTube, custom websites, message board),
with live-relay capture, card rendering, and summary-card aggregation.

## Requirements

- [bun](https://bun.sh) (runtime and package manager)
- Chrome/Chromium for browser-assisted crawling
  (`PUPPETEER_EXECUTABLE_PATH` or the default `chrome` channel)
- Optional: `yt-dlp` / `gallery-dl` (media fallback tools), `ffmpeg`
  (video/live capture)

## Setup

1. `bun install`
2. `cp .env.example .env` and fill in the values you need.
3. `cp assets/tweet-forwarder/config.example.yaml assets/config.yaml` and edit
   it for your crawlers/forwarders.
4. Put cookie files under `assets/cookies/` (Netscape cookie-jar format) and
   reference them via `cookie_file` in the config.
5. Run:
   ```sh
   bun run start:forwarder
   ```
   or build a container image with `app/tweet-forwarder/Dockerfile`.

## Cookie maintenance

Cookie jars expire. The forwarder exposes `POST /api/cookies/sync` (see
`app/tweet-forwarder/scripts/startup-cookie-maintenance.ts` for an example
client) which visits the target site in a real browser session and re-exports
the jar.

## License

The code is MIT-licensed (see `LICENSE`).

Third-party notices:
- `satori` / `@resvg/resvg-js` are MPL-2.0 (file-level copyleft; keep their
  license headers intact if you modify those packages).
- The media pipeline shells out to `ffmpeg`/`yt-dlp` binaries at runtime. If
  you redistribute a container image that bundles ffmpeg, the ffmpeg binary is
  GPL and you must comply with its terms (most images install the distro
  package; check your base image's packaging).
MD

cd "$TARGET_DIR"
git init -q
git add -A
git -c user.name=idol-bbq-utils -c user.email=idol-bbq-utils@users.noreply.github.com \
    commit -q -m "idol-bbq-utils (migratable snapshot, credentials-free)"

commit="$(git rev-parse --short HEAD)"
file_count="$(git ls-files | wc -l | tr -d ' ')"
printf 'public export ready at %s (commit %s, %s files)\n' "$TARGET_DIR" "$commit" "$file_count"
printf 'Review it and push to a new repository when ready; nothing was uploaded.\n'
