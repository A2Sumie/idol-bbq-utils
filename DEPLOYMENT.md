# Deployment Guide

## Server Information
- **Host**: `sumie@3020e`
- **Path**: `~/idol-bbq-utils`
- **Runtime database**: SQLite mounted as `./assets/refactor.db:/app/data.db`
- **Runtime config**: host-mounted `./assets/config.yaml:/app/config.yaml`

## Standard Workflow

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
5.  Run the no-secret config audit when config/route policy changed:
    ```bash
    bun run audit:config -- --fail-on-diagnostics
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
- recreates the compose service with `--no-start`;
- forces `forwarder-new` to `restart=no`;
- verifies `running=false` before returning success.

Do not use `docker compose up -d --build` as the default remediation deploy path. It starts the service and runs startup migrations via `app/tweet-forwarder/start.sh`.

### 3. Production Start
Production start is a separate deliberate operation after stopped deploy verification, migration review, and an explicit user request.

## Feature Configuration
### Batch Sending & Deduplication
- **Media Deduplication**: Enabled by default. Duplicates are checked via SHA-256 hash against `media_hashes` table.
- **Route Policy Audit**:
    -   `/api/config/audit` exposes redacted config hash, route policy hash, route graph counts, diagnostics, and sensitive field paths only.
    -   `bun run audit:config` provides the same no-secret audit from the host config file.

## Verification
To verify changes:
1.  **Stopped state**:
    ```bash
    ssh 3020e 'docker inspect forwarder-new --format "status={{.State.Status}} running={{.State.Running}} restart={{.HostConfig.RestartPolicy.Name}} image={{.Image}}"'
    ```
2.  **Image audit command**:
    ```bash
    ssh 3020e 'docker run --rm --entrypoint bun -v "$HOME/idol-bbq-utils/assets/config.yaml:/app/config.yaml:ro" idol-bbq-utils-spider:latest /app/tools/config-audit.js --config /app/config.yaml --fail-on-diagnostics'
    ```
3.  **Logs after an explicit production start**:
    ```bash
    docker logs -f forwarder-new
    ```
