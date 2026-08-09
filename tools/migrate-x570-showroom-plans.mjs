// Migrate X570 StreamServ relay_schedule.json events into the unified on-demand
// live capture plan system (forwarder /api/live-capture-plans).
//
// The X570 SHOWROOM on-demand mechanism (relay_schedule.json + StreamServ
// auto_stream.py 30s/5s/1s polling) is being folded into the forwarder executor
// (live-capture-executor-service): each scheduled event becomes a showroom plan
// that the executor probes (native status API) and records (yt-dlp) in-window.
//
// Usage:
//   node tools/migrate-x570-showroom-plans.mjs \
//     --schedule /tmp/relay_schedule.json \
//     --forwarder http://3020e:3000 \
//     --secret <api-secret> [--dry-run]
//
// The script only migrates events whose status is still pending scheduling
// ('scheduled' | 'pending'); applied/cancelled/abandoned events are skipped.
import fs from 'fs'
import { execFileSync } from 'child_process'

function arg(name, dflt) {
  const i = process.argv.indexOf(name)
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : dflt
}
const schedulePath = arg('--schedule', '')
const forwarderUrl = (arg('--forwarder', 'http://3020e:3000') || '').replace(/\/+$/, '')
const secret = arg('--secret', process.env.FORWARDER_API_SECRET || '')
const dryRun = process.argv.includes('--dry-run')
const winRemotePath = arg('--win-remote', '/Users/zou/ytdlp/subPrep/livestr/windows-remote-executor/bin/win-remote')

let raw = ''
if (schedulePath && fs.existsSync(schedulePath)) {
  raw = fs.readFileSync(schedulePath, 'utf8')
} else if (winRemotePath && fs.existsSync(winRemotePath)) {
  // Fall back to reading the schedule straight off X570 via the windows remote executor.
  raw = execFileSync(winRemotePath, ['exec', 'x570', '--stdin'], {
    input: 'type D:\\StreamServ\\relay_schedule.json',
    encoding: 'utf8',
  })
} else {
  console.error('no schedule source: pass --schedule or a working --win-remote')
  process.exit(2)
}

let payload
try {
  payload = JSON.parse(raw)
} catch (error) {
  // The win-remote channel can mangle UTF-8 into raw control bytes inside strings
  // (including embedded newlines inside string literals), which can corrupt the
  // title strings so badly that the file is not valid JSON. Strip control chars,
  // then fall back to a field-level extractor over the ASCII fields we need.
  const sanitized = raw.replace(/[\u0000-\u001f\u007f]/g, '')
  try {
    payload = JSON.parse(sanitized)
  } catch {
    const events = []
    const eventBlocks = sanitized.split(/\{\s*"id":\s*"/).slice(1)
    for (const block of eventBlocks) {
      const take = (pattern) => {
        const m = block.match(pattern)
        return m?.[1] || ''
      }
      const id = take(/^([^"]+)/)
      if (!id) continue
      const status = take(/"status":\s*"([^"]*)"/)
      const applyAt = take(/"apply_at":\s*(\d+)/)
      const pageUrl = take(/"page_url":\s*"([^"]+)"/)
      const scheduledStart = take(/"scheduled_start_at":\s*(\d+)/)
      const title = take(/"title":\s*"([^"]{0,300})/)
      const members = take(/"members":\s*\[([^\]]{0,300})\]/)
        .split(',')
        .map((part) => part.replace(/["\s]/g, ''))
        .filter(Boolean)
      events.push({
        id,
        status,
        apply_at: Number(applyAt) || 0,
        streamConfig: {
          page_url: pageUrl,
          scheduled_start_at: Number(scheduledStart) || 0,
          upload_metadata: { members },
        },
        metadata: { title },
      })
    }
    if (events.length === 0) {
      console.error(`relay_schedule.json is not valid JSON: ${error.message}`)
      process.exit(1)
    }
    console.warn(`relay_schedule.json was malformed; recovered ${events.length} events field-wise`)
    payload = { events }
  }
}

const events = Array.isArray(payload?.events) ? payload.events : []
const migratable = events.filter((event) => ['scheduled', 'pending'].includes(String(event?.status || '').toLowerCase()))
console.log(`events=${events.length} migratable=${migratable.length} (scheduled/pending)`)

for (const event of migratable) {
  const pageUrl = String(event?.streamConfig?.page_url || '')
  const match = pageUrl.match(/showroom-live\.com\/(?:r|lite)\/([^/?]+)/)
  const handle = match?.[1]
  const applyAt = Number(event?.apply_at)
  const scheduledStart = Number(event?.streamConfig?.scheduled_start_at || 0)
  const startsAt = scheduledStart > 0 ? scheduledStart : applyAt
  const title = String(event?.metadata?.title || event?.streamConfig?.upload_metadata?.upload_title || '').trim()

  if (!handle || !Number.isFinite(applyAt) || applyAt <= 0) {
    console.warn(`skip ${event?.id}: missing room key or apply_at (page_url=${pageUrl})`)
    continue
  }

  const plan = {
    schema_version: 1,
    target: { platform: 'showroom', handle, url: pageUrl },
    event: {
      starts_at: startsAt,
      timezone: 'Asia/Tokyo',
      ...(title ? { title: title.slice(0, 500) } : {}),
    },
    window: { before_minutes: 10, after_minutes: 240 },
    capture: { poll_seconds: 15, first_byte_timeout_seconds: 30, quality_order: ['origin_rtmp', 'hd_flv', 'hd_hls'] },
    source: { kind: 'llm_extraction', ref: `x570-relay:${event?.id}`.slice(0, 500), url: pageUrl },
    tags: ['showroom', 'x570-migrated'],
    notes: 'migrated from X570 StreamServ relay_schedule.json',
  }

  console.log(`-> ${event?.id} room=${handle} starts_at=${startsAt}${title ? ` title="${title.slice(0, 60)}"` : ''}`)
  if (dryRun) {
    continue
  }

  const response = await fetch(`${forwarderUrl}/api/live-capture-plans`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${secret}`,
    },
    body: JSON.stringify({ plan, idempotency_key: `x570-${event?.id}`.slice(0, 200) }),
  })
  const body = await response.text()
  if (response.status === 201 || response.status === 200) {
    let parsed
    try {
      parsed = JSON.parse(body)
    } catch {}
    console.log(`  ok ${response.status} created=${parsed?.created ?? '?'} id=${parsed?.id ?? '?'}`)
  } else {
    console.error(`  FAIL ${response.status}: ${body.slice(0, 300)}`)
  }
}
