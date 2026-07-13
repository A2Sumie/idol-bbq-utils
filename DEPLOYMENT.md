# Deployment Guide

## Server Information
- **Host**: `sumie@3020e`
- **Path**: `~/idol-bbq-utils`
- **Runtime database**: SQLite mounted as `./assets/refactor.db:/app/data.db`
- **Runtime config**: host-mounted `./assets/config.yaml:/app/config.yaml`
- **Migration backups**: default container path `/app/backups/db-migrations`,
  backed by the host `./assets/backups:/app/backups` compose volume.

## Standard Workflow

Production code changes are GitHub-first. Do not patch the 3020e tracked
source tree as the primary implementation path. Land the source change locally,
verify it, commit it, and push it to GitHub before changing containers, images,
or remote working trees. If a machine-side hot patch is unavoidable for an
incident, treat it as temporary: record it, back-port it into Git, push it, and
only then converge the machine from the committed state.

### 1. Development (Local)
1.  Make changes in `idol-bbq-utils` (this repository).
2.  If local Chrome is used for automation or mobile emulation, launch it with an isolated profile and restore normal desktop Chrome behavior before finishing by closing orphaned headless processes and reopening a regular GUI window if needed.
3.  If database schema changed:
    ```bash
    cd app/tweet-forwarder
    npx prisma migrate dev --name <migration_name>
    ```
    This generates a migration file in `prisma/migrations`.
4.  Commit all changes, including `prisma/migrations`.
    ```bash
    git add .
    git commit -m "feat: your feature"
    git push
    ```
5.  Run the local forwarder gate:
    ```bash
    bun run verify:forwarder
    ```
6.  Run the no-secret config audit when config/route policy changed:
    ```bash
    bun run audit:config -- --fail-on-diagnostics
    ```
7.  Check the current remote stopped deployment without printing secrets:
    ```bash
    bun run preflight:forwarder
    ```

### 2. Stopped Deployment (Default)
Use the guarded stopped deploy path unless the user explicitly asks to bring production online:

```bash
bun run deploy:forwarder:stopped
```

The script:
- refuses a dirty local worktree;
- refuses an unpushed `HEAD` unless `SKIP_UPSTREAM_CHECK=1` is set;
- builds from a local `git archive` in `/tmp/idol-bbq-utils-build-<commit>`, not from the remote tracked worktree;
- uses the compose file from that build archive with `--project-directory`
  pointing at the remote runtime repo, so service definitions come from the
  deployed commit while relative runtime assets still resolve to host `assets/`;
- recreates the compose service with `--no-start`;
- forces `forwarder-new` to `restart=no`;
- verifies `running=false` before returning success.
- writes the deployed commit into image labels and `/app/build-commit`.

Do not use `docker compose up -d --build` as the default remediation deploy path. It starts the service and runs startup migrations via `app/tweet-forwarder/start.sh`.

### 3. Production Start
Production start is a separate deliberate operation after stopped deploy verification, migration review, and an explicit user request.

The compose file defaults to `IDOL_BBQ_RUNTIME_MODE=offline` and `restart=no`. A
plain `docker compose up -d` must not activate crawlers, migrations, or senders.
For an intentional production start, set the runtime mode explicitly:

```bash
IDOL_BBQ_RUNTIME_MODE=online IDOL_BBQ_RESTART_POLICY=always docker compose up -d spider
```

`IDOL_BBQ_RUNTIME_MODE=api-only` starts only the API surface and does not create
crawler schedulers, forwarder schedulers, task queue polling, or sender pools.
High-risk action endpoints return `503` in non-`online` modes before enqueueing
or sending.

Startup migrations run automatically only in `online` mode. Before `prisma
migrate deploy`, startup now:

- refuses a missing DB by default (`IDOL_BBQ_REQUIRE_EXISTING_DB_FOR_MIGRATION=1`);
- creates an atomic migration lock;
- runs SQLite `PRAGMA quick_check`;
- creates a consistent SQLite backup snapshot, then copies `-wal`/`-shm` sidecars
  when present for forensic context;
- writes a small backup manifest with the build commit and migration head;
- runs another quick check after migration.

The default backup directory is `/app/backups/db-migrations`, backed by the
host `assets/backups` mount. Running migrations in `api-only` or `offline` is
refused even when `IDOL_BBQ_RUN_MIGRATIONS=1` is set, unless
`IDOL_BBQ_ALLOW_NON_ONLINE_MIGRATIONS=1` is also set for a deliberate maintenance
migration.

## Feature Configuration
### Agent Access
The forwarder exposes an authenticated agent-safe API subset on the configured
runtime API port:

- `GET /api/agent/status` returns compact runtime, queue, route-count, endpoint,
  and model status.
- `GET /api/agent/models` returns redacted processor model capability metadata.
- `POST /api/agent/probe-model` performs a bounded live model probe and reports
  latency plus output speed.
- `GET /api/agent/codex/status` checks the idol-bbq -> MCP -> Codex bridge.
- `POST /api/agent/codex/run` and `/api/agent/codex/reply` call Codex through
  MCP when `IDOL_BBQ_CODEX_MCP_ENABLED=1`.

The existing runtime API token is used. Do not put it on command lines; use the
container config reader or environment variables.

From the local operator machine:

```bash
tools/forwarder-runtime-api.sh agent-status
tools/forwarder-runtime-api.sh codex-status
tools/forwarder-runtime-api.sh model-capabilities
tools/forwarder-runtime-api.sh probe-model 22_7-social-ja-zh --text "Reply OK"
```

OpenCode Go is used only as the stable DeepSeek API provider for configured
processors. Do not use OpenCode's agent tooling in this deployment path.

For `idol-bbq -> MCP -> Codex` on `3020e`, run the host-side Codex bridge from
the same user account that is logged into Codex CLI:

```bash
cd /home/sumie/idol-bbq-utils
read -rs IDOL_BBQ_CODEX_BRIDGE_TOKEN
export IDOL_BBQ_CODEX_BRIDGE_TOKEN
export IDOL_BBQ_CODEX_MCP_ENABLED=1
bun tools/codex-mcp-bridge-server.ts
```

Then start the forwarder with:

```bash
export IDOL_BBQ_CODEX_MCP_ENABLED=1
export IDOL_BBQ_CODEX_MCP_BRIDGE_URL=http://127.0.0.1:3099
export IDOL_BBQ_CODEX_MCP_BRIDGE_TOKEN="$IDOL_BBQ_CODEX_BRIDGE_TOKEN"
IDOL_BBQ_RUNTIME_MODE=api-only docker compose up -d spider
```

The bridge speaks MCP to `codex mcp-server` over stdio and exposes only the
local `/status`, `/run`, and `/reply` HTTP endpoints. For non-container local
testing, omit `IDOL_BBQ_CODEX_MCP_BRIDGE_URL` and the API will spawn
`codex mcp-server` directly.

For Codex CLI on `3020e`, add the project MCP server after deploying this
commit:

```bash
codex mcp add idol-bbq -- bash -lc 'cd /home/sumie/idol-bbq-utils && exec bun tools/forwarder-mcp-server.ts'
codex mcp list
```

This is the `Codex -> MCP -> idol-bbq` path. The project MCP server reads
`IDOL_BBQ_AGENT_API_TOKEN`/`API_SECRET` first, then falls back to the project
config's `api.secret`; it never prints the token.

### Batch Sending & Deduplication
- **Media Deduplication**: Enabled by default. Duplicates are checked via SHA-256 hash against `media_hashes` table.
- **Biliup Video Upload**: Uploads no longer append a collision-placeholder
  P2. Legacy `video_upload.collision_placeholder_part` config is ignored so
  Bilibili submissions use the real source video part(s) only.
- **Route Policy Audit**:
    -   `/api/config/audit` exposes redacted config hash, route policy hash, route graph counts, diagnostics, and sensitive field paths only.
    -   `bun run audit:config` provides the same no-secret audit from the host config file.

## Verification
To verify changes:
1.  **Preflight summary**:
    ```bash
    STRICT_COMMIT=1 bun run preflight:forwarder
    ```
    Add `STRICT_MIGRATIONS=1` when validating readiness for an intentional
    online start; it fails if the production DB fails SQLite `quick_check` or
    has pending, failed, or image/DB-drifted Prisma migrations.

    Runtime-config drift is now gated by default. `STRICT_CONFIG_SHA256=1`
    (default) fails preflight when the remote mounted `assets/config.yaml`
    sha256 differs from the local `LOCAL_CONFIG_PATH` (default
    `assets/config.yaml`), or set `EXPECTED_CONFIG_SHA256` to pin an exact
    hash. `STRICT_PROCESSOR_ENV=1` (default) fails when any `api_key: env:NAME`
    referenced by the runtime config is missing from the container environment.
    Together these refuse the two silent drift classes: a runtime config that no
    longer matches the intended committed config, and a config that names a
    provider key (for example `TENCENT_HUNYUAN_API_KEY` for Tencent `hy3`) that
    the machine does not actually have. Set either to `0` only for a deliberate,
    documented exception.
2.  **Stopped state**:
    ```bash
    ssh 3020e 'docker inspect forwarder-new --format "status={{.State.Status}} running={{.State.Running}} restart={{.HostConfig.RestartPolicy.Name}} image={{.Image}}"'
    ```
3.  **Image audit command**:
    ```bash
    ssh 3020e 'docker run --rm --entrypoint bun -v "$HOME/idol-bbq-utils/assets/config.yaml:/app/config.yaml:ro" idol-bbq-utils-spider:latest /app/tools/config-audit.js --config /app/config.yaml --fail-on-diagnostics'
    ```
4.  **Remote dirty worktree inventory**:
    ```bash
    bun run audit:remote-drift
    ```
5.  **Remote dirty worktree comparison against local HEAD**:
    ```bash
    bun run audit:remote-drift -- --compare-local-head
    ```
    Runtime backup/config artifacts are classified without hashing their contents.
    For automation, use the no-content JSON form:
    ```bash
    bun run audit:remote-drift -- --compare-local-head --json
    ```
6.  **Remote source convergence plan**:
    ```bash
    bun run converge:remote-source
    ```
    This defaults to dry-run and does not write to the remote host. Runtime config,
    database files, and runtime backups are protected. To write a source archive
    without replacing files, use:
    ```bash
    bun run converge:remote-source -- --archive-only
    ```
    Replacing remote source candidates from local `HEAD` is deliberately explicit:
    ```bash
    bun run converge:remote-source -- --apply --yes
    ```
7.  **Full local gate plus remote proof**:
    ```bash
    RUN_REMOTE_PREFLIGHT=1 RUN_REMOTE_DRIFT_COMPARE=1 bun run verify:forwarder
    ```
8.  **Temporary DB backup/restore drill**:
    ```bash
    bun run drill:db-backup
    ```
    This opens the production SQLite DB read-only, creates a temporary backup
    copy, runs SQLite `quick_check`, and runs `prisma migrate status` against
    the copy using the stopped image. It does not start the service and does not
    run production migrations.
9.  **Logs after an explicit production start**:
    ```bash
    docker logs -f forwarder-new
    ```
