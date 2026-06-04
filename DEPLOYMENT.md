# Deployment Guide

## Server Information
- **Host**: `sumie@3020e`
- **Path**: `~/idol-bbq-utils`
- **Runtime database**: SQLite mounted as `./assets/refactor.db:/app/data.db`
- **Runtime config**: host-mounted `./assets/config.yaml:/app/config.yaml`
- **Migration backups**: default container path `/tmp/tweet-forwarder/logs/db-migrations`
  (host path with the current compose volume). The checked-in compose file also
  maps `./assets/backups:/app/backups` for explicit backup-dir overrides.

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

The default backup directory is `/tmp/tweet-forwarder/logs/db-migrations`.
Override with `IDOL_BBQ_DB_BACKUP_DIR=/app/backups/db-migrations` only after
confirming the host `assets/backups` mount is present and writable by the
container user. Running migrations in `api-only` or `offline` is refused even
when `IDOL_BBQ_RUN_MIGRATIONS=1` is set, unless
`IDOL_BBQ_ALLOW_NON_ONLINE_MIGRATIONS=1` is also set for a deliberate maintenance
migration.

## Feature Configuration
### Batch Sending & Deduplication
- **Media Deduplication**: Enabled by default. Duplicates are checked via SHA-256 hash against `media_hashes` table.
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
8.  **Logs after an explicit production start**:
    ```bash
    docker logs -f forwarder-new
    ```
