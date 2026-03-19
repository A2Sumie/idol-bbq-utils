# Tweet Forwarder Operations

## Paths

- Local repo: `/Users/zou/ytdlp/subPrep/livestr/idol-bbq-utils`
- Service repo on `3020e`: `~/idol-bbq-utils`
- Runtime config on server: `~/idol-bbq-utils/assets/config.yaml`
- Container mount target: `/app/config.yaml`
- Container name: `forwarder-new`
- Compose service: `spider`

## Direct API Control

Set the target and auth header first:

```bash
export TF_HOST=3020e
export TF_API="http://${TF_HOST}:3000"
export API_SECRET='<read from assets/config.yaml api.secret>'
export TF_AUTH="Authorization: Bearer ${API_SECRET}"
```

Check runtime status:

```bash
curl -s -H "$TF_AUTH" "$TF_API/api/runtime/status" | jq
```

Trigger a true in-process hot reload:

```bash
curl -s -X POST -H "$TF_AUTH" "$TF_API/api/runtime/reload" | jq
```

Trigger a hard restart:

```bash
curl -s -X POST -H "$TF_AUTH" "$TF_API/api/server/restart" | jq
```

Run a crawler immediately:

```bash
curl -s -X POST -H "$TF_AUTH" -H 'Content-Type: application/json' \
  "$TF_API/api/actions/crawlers/run" \
  -d '{"crawler":"22/7-cast-成员统一列表"}' | jq
```

Simulate a captured article:

```bash
curl -s -X POST -H "$TF_AUTH" -H 'Content-Type: application/json' \
  "$TF_API/api/actions/articles/simulate" \
  -d '{
    "crawlerName":"YouTube抓取",
    "content":"[simulated] smoke test",
    "forwardAfterSave":false
  }' | jq
```

Sync cookies back from a browser session:

```bash
curl -s -X POST -H "$TF_AUTH" -H 'Content-Type: application/json' \
  "$TF_API/api/cookies/sync" \
  -d '{"crawlerName":"22/7-cast-成员统一列表"}' | jq
```

## Config Update Workflow

Edit the mounted config on the server:

```bash
ssh 3020e 'cd ~/idol-bbq-utils && ${EDITOR:-vi} assets/config.yaml'
```

Apply the edited config without restarting the process:

```bash
curl -s -X POST -H "$TF_AUTH" "$TF_API/api/runtime/reload" | jq
```

Push a full config object through the API:

```bash
yq -o=json /Users/zou/ytdlp/subPrep/livestr/idol-bbq-utils/assets/config.yaml | \
  curl -s -X POST -H "$TF_AUTH" -H 'Content-Type: application/json' \
    "$TF_API/api/config/update" \
    --data-binary @- | jq
```

`/api/config/update` now saves and hot reloads in one step. If hot reload fails, the config file is restored automatically.

## Verifying True Hot Reload

Record runtime generation and container start time:

```bash
curl -s -H "$TF_AUTH" "$TF_API/api/runtime/status" | jq '.runtime'
ssh 3020e 'docker inspect -f "{{.State.StartedAt}}" forwarder-new'
```

Run hot reload, then check again:

```bash
curl -s -X POST -H "$TF_AUTH" "$TF_API/api/runtime/reload" | jq '.runtime'
curl -s -H "$TF_AUTH" "$TF_API/api/runtime/status" | jq '.runtime'
ssh 3020e 'docker inspect -f "{{.State.StartedAt}}" forwarder-new'
```

Expected result:

- `runtime.generation` increments.
- `runtime.lastReloadedAt` changes.
- `docker inspect ... StartedAt` stays the same.

If the container start time changed, that was a hard restart, not a hot reload.

## Logs And Deployment

Tail live logs:

```bash
ssh 3020e 'docker logs --tail 200 -f forwarder-new'
```

Redeploy backend:

```bash
cd /Users/zou/ytdlp/subPrep/livestr/idol-bbq-utils
git push origin main
ssh 3020e 'cd ~/idol-bbq-utils && git pull --ff-only && docker compose up -d --build spider'
```

Check container state after deploy:

```bash
ssh 3020e 'docker ps --filter name=forwarder-new'
ssh 3020e 'docker logs --tail 120 forwarder-new'
```
