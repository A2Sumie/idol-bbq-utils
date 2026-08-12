// Instagram Live watcher/relayer (standalone, deployment-independent).
//
// Self-contained IG live capture that does NOT depend on the crawler scheduler:
// it probes the handle's IG live page, captures the stream manifests (HLS/DASH),
// relays them to the live-player (tv.n2nj.moe) and optionally archives locally with
// ffmpeg. The schedule lives in the repo config (persisted, survives restarts) and
// the watcher runs in the deployment-independent tiktok-live-watch container,
// supervised by tools/instagram-live-monitor.sh (15s self-heal).
//
// Usage (inside the watcher container):
//   bun /app/instagram-live-watch.ts nao_aikawa227 --until 01:00 --player-id relay-nao \
//     --player-name "【IG Live】相川奈央" --live-player-url https://<live-player-host> \
//     --auth-username <user> --auth-password '...' --waf-header '<secret>' \
//     --cookie /app/assets/cookies/inscks0318.txt [--archive]
import fs from 'fs'
import path from 'path'
import { spawn, spawnSync } from 'child_process'
import puppeteer from 'puppeteer-core'

function arg(name: string, dflt?: string) {
  const i = process.argv.indexOf(name)
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : dflt
}
const handle = (process.argv[2] || '').replace(/^@/, '')
if (!handle) {
  console.error('usage: bun instagram-live-watch.ts <handle> [--until HH:MM] [--poll SEC] [--player-id ..] ...')
  process.exit(2)
}
const untilHHMM = arg('--until')
const pollSeconds = Number(arg('--poll', '90'))
const playerId = arg('--player-id', 'relay')
const playerName = arg('--player-name', `【IG Live】${handle}`)
const livePlayerUrl = (arg('--live-player-url', 'https://tv.n2nj.moe') || '').replace(/\/+$/, '')
const authUsername = arg('--auth-username', '')
const authPassword = arg('--auth-password', '')
const wafHeader = arg('--waf-header', '')
const cookiePath = arg('--cookie', '/app/assets/cookies/inscks0318.txt')!
const archiveRoot = arg('--archive-root', '/app/archive/instagram-live')!
const ffmpegBin = arg('--ffmpeg', '/usr/bin/ffmpeg')!
const archiveEnabled = process.argv.includes('--archive')
const profileUrl = `https://www.instagram.com/${handle}`
const liveUrl = `${profileUrl}/live/`
const requestUa =
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'

const deadlineMs = (() => {
  const byDefault = Date.now() + 12 * 60 * 60 * 1000
  if (!untilHHMM) return byDefault
  const [h, m] = untilHHMM.split(':').map(Number)
  const jstNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }))
  const target = new Date(jstNow)
  target.setHours(h, m, 0, 0)
  if (target <= jstNow) target.setDate(target.getDate() + 1)
  return Date.now() + (target.getTime() - jstNow.getTime())
})()

function log(msg: string) {
  process.stdout.write(`[ig-watch ${new Date().toISOString()}] ${msg}\n`)
}

function parseCookies(fp: string): Array<{ name: string; value: string }> {
  const out: Array<{ name: string; value: string }> = []
  if (!fs.existsSync(fp)) return out
  for (const raw of fs.readFileSync(fp, 'utf8').split(/\r?\n/)) {
    const line = raw.trim()
    if (!line || line.startsWith('#')) continue
    const p = line.split('\t')
    if (p.length !== 7) continue
    out.push({ name: p[5], value: p[6] })
  }
  return out
}
const cookieHeader = parseCookies(cookiePath)
  .map((c) => `${c.name}=${c.value}`)
  .join('; ')

function wafHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    'User-Agent': 'N2NJ-Stream-Bot/1.0',
    Accept: 'application/json',
  }
  if (wafHeader) {
    const idx = wafHeader.indexOf(':')
    if (idx > 0) headers[wafHeader.slice(0, idx).trim()] = wafHeader.slice(idx + 1).trim()
    else headers['x-bypass-waf'] = wafHeader
  }
  return headers
}

async function fetchWithTimeout(url: string, init: RequestInit = {}, timeoutMs = 15000) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await fetch(url, { ...init, signal: controller.signal })
  } finally {
    clearTimeout(timer)
  }
}

type CapturedStream = { source: string; headers: Record<string, string> }
const capturedStreams = new Map<string, CapturedStream>()

function captureStream(url: string, headers: Record<string, string>) {
  const normalized = url.split('?')[0]
  if (!capturedStreams.has(normalized)) {
    capturedStreams.set(normalized, { source: url, headers })
  }
}

function captureStreamsFromWebInfo(json: any) {
  const payload = [json?.broadcast, json?.data, json].find((c) => {
    return c?.dash_abr_playback_url || c?.dash_playback_url || c?.broadcast_status
  }) || json
  const urls = [payload?.dash_abr_playback_url, payload?.dash_playback_url, payload?.hls_playback_url]
    .map((v) => String(v || '').replace(/\\u0026/g, '&').trim())
    .filter(Boolean)
  for (const url of urls) captureStream(url, { Referer: liveUrl, 'User-Agent': requestUa })
  return payload?.broadcast_status ? String(payload.broadcast_status) : null
}

const persistentProfileDir = `/tmp/ig-watch-${process.pid}-profile`
const uidCachePath = `${archiveRoot}/uids.json`
let browser: Awaited<ReturnType<typeof puppeteer.launch>> | null = null
let page: any = null

// IG request budget (mirrors the RE-community approach: private API XHR is cheap,
// full browser navigations are the 429 driver — use one only when needed):
//   probe = 1 web_info XHR (no navigation) once the numeric uid is cached;
//   full page load happens only on first probe (uid lookup) or when live is detected.
const DEFAULT_IG_APP_ID = '936619743392459'
const WEB_INFO_RATE_LIMITED_MS = 10 * 60 * 1000

function loadUidCache(): Record<string, string> {
  try {
    return JSON.parse(fs.readFileSync(uidCachePath, 'utf8'))
  } catch {
    return {}
  }
}

function saveUidCache(cache: Record<string, string>) {
  try {
    fs.mkdirSync(path.dirname(uidCachePath), { recursive: true })
    fs.writeFileSync(uidCachePath, JSON.stringify(cache))
  } catch {}
}

async function fetchWebInfoInPage(uid: string): Promise<{ ok: boolean; status: number; json: any } | null> {
  if (!page) return null
  const result = await page
    .evaluate(
      async (targetUserId: string, igAppId: string) => {
        const r = await fetch(`/api/v1/live/web_info/?target_user_id=${encodeURIComponent(targetUserId)}`, {
          credentials: 'include',
          headers: { accept: '*/*', 'x-requested-with': 'XMLHttpRequest', 'x-ig-app-id': igAppId },
        })
        return { ok: r.ok, status: r.status, text: await r.text() }
      },
      uid,
      DEFAULT_IG_APP_ID,
    )
    .catch(() => null)
  if (!result) return null
  try {
    return { ok: result.ok, status: result.status, json: JSON.parse(result.text) }
  } catch {
    return { ok: result.ok, status: result.status, json: null }
  }
}

async function ensureBrowser() {
  if (browser && browser.isConnected()) return
  if (browser) {
    await browser.close().catch(() => {})
    browser = null
    page = null
  }
  browser = await puppeteer.launch({
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/google-chrome-stable',
    headless: true,
    userDataDir: persistentProfileDir,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  })
  page = await browser.newPage()
  await page.setExtraHTTPHeaders({ 'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7' })
  const cookies = parseCookies(cookiePath).map((c) => ({ ...c, domain: '.instagram.com', path: '/' }))
  if (cookies.length) await page.setCookie(...cookies)
}

async function probeLive(): Promise<{ live: boolean; status: string | null; userId: string | null; rateLimited: boolean }> {
  capturedStreams.clear()
  await ensureBrowser()

  const uids = loadUidCache()
  let userId = uids[handle] || null

  if (!userId) {
    const listener = async (res: any) => {
      const url = String(res.url() || '')
      if (url.includes('.m3u8') || url.includes('.mpd')) {
        captureStream(url, {
          Referer: liveUrl,
          'User-Agent': requestUa,
          Cookie: cookieHeader,
        })
        return
      }
      if (url.includes('/api/v1/live/web_info/')) {
        try {
          const json = await res.json()
          captureStreamsFromWebInfo(json)
        } catch {}
      }
    }
    page.on('response', listener)
    try {
      await page.goto(liveUrl, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {})
      await new Promise((r) => setTimeout(r, 3500))
    } catch {}
    page.off('response', listener)

    try {
      const html = await page.content()
      const pkMatch = html.match(/instagram:\/\/user\?id=(\d+)/) || html.match(/"pk":"(\d{6,})"/)
      userId = pkMatch?.[1] || null
      if (userId) {
        uids[handle] = userId
        saveUidCache(uids)
      }
    } catch {}
  }

  let status: string | null = null
  if (userId) {
    const wid = await fetchWebInfoInPage(userId)
    if (wid?.status === 429) {
      log(`web_info 429 for handle=${handle} — backing off ${WEB_INFO_RATE_LIMITED_MS / 60000} min`)
      return { live: false, status: null, userId, rateLimited: true }
    }
    if (wid?.ok && wid.json) {
      status = captureStreamsFromWebInfo(wid.json)
      const liveStates = ['live', 'post_live']
      if (capturedStreams.size === 0 && (status && liveStates.includes(status))) {
        // Live confirmed but no manifest surfaced in the XHR — a full page load is
        // justified now (the page emits the manifest network requests).
        const listener = async (res: any) => {
          const url = String(res.url() || '')
          if (url.includes('.m3u8') || url.includes('.mpd')) {
            captureStream(url, {
              Referer: liveUrl,
              'User-Agent': requestUa,
              Cookie: cookieHeader,
            })
          }
        }
        page.on('response', listener)
        try {
          await page.goto(liveUrl, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {})
          await new Promise((r) => setTimeout(r, 3500))
        } catch {}
        page.off('response', listener)
      }
    }
  }

  const live = capturedStreams.size > 0
  log(
    `probe handle=${handle} live=${live} status=${status || 'unknown'} streams=${capturedStreams.size} userId=${userId || '?'} navigations=${userId ? 'xhr-only' : 'first-load'}`,
  )
  return { live, status, userId, rateLimited: false }
}

async function login(): Promise<string> {
  if (!authUsername || !authPassword) throw new Error('missing live-player credentials')
  const res = await fetchWithTimeout(`${livePlayerUrl}/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...wafHeaders() },
    body: JSON.stringify({ username: authUsername, password: authPassword }),
  })
  const setCookie = res.headers.get('set-cookie') || ''
  const body = await res.text()
  if (!res.ok) throw new Error(`live-player login failed: ${res.status} ${body}`)
  const token = setCookie
    .split(/,(?=\s*[A-Za-z0-9_.-]+=)/)
    .map((v) => v.trim())
    .find((v) => v.startsWith('auth-token='))
  const pair = (token || setCookie).split(';')[0]?.trim()
  if (!pair) throw new Error('live-player login missing auth-token cookie')
  return pair
}

async function postRelayAction(authCookie: string, body: Record<string, unknown>) {
  const res = await fetchWithTimeout(
    `${livePlayerUrl}/api/players/${encodeURIComponent(playerId)}/relay`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Cookie: authCookie, ...wafHeaders() },
      body: JSON.stringify(body),
    },
  )
  const text = await res.text()
  return { ok: res.ok, status: res.status, body: (() => { try { return JSON.parse(text) } catch { return text } })() }
}

async function ensurePlayer(authCookie: string, title: string) {
  await fetchWithTimeout(`${livePlayerUrl}/api/players`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Cookie: authCookie, ...wafHeaders() },
    body: JSON.stringify({
      name: title,
      pId: playerId,
      url: `https://stream.n2nj.moe/${playerId}.m3u8`,
      description: `Instagram Live relay for ${handle}`,
      coverUrl: null,
      announcement: null,
      streamConfig: { mode: 'echo' },
    }),
  })
}

function buildEchoPackage(): Record<string, unknown> {
  return {
    mode: 'echo',
    page_url: liveUrl,
    timestamp: Date.now(),
    cookies_b64: Buffer.from(cookieHeader).toString('base64'),
    streams_detected: capturedStreams.size,
    streams: Array.from(capturedStreams.values()).map((s) => ({
      source: s.source,
      type: s.source.includes('.mpd') ? 'DASH' : 'HLS',
      headers: s.headers,
      mediaInfo: { size: 0, variants_count: 0, variants: [], encrypted: false, pssh: null },
    })),
    licenses: [],
    keys: [],
  }
}

let sessionActive = false
let lastSyncAt = 0
let ffmpegChild: ReturnType<typeof spawn> | null = null
const archiveDir = path.join(archiveRoot, handle)

function startArchive() {
  if (!archiveEnabled || ffmpegChild) return
  fs.mkdirSync(archiveDir, { recursive: true })
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
  const out = path.join(archiveDir, `${handle}-${stamp}.mkv`)
  const first = capturedStreams.values().next().value as CapturedStream | undefined
  if (!first) return
  const headerLines = Object.entries(first.headers).map(([k, v]) => `${k}: ${v}`).join('\r\n')
  const args = [
    ffmpegBin,
    '-y',
    '-headers',
    headerLines + '\r\n',
    '-i',
    first.source,
    '-c',
    'copy',
    out,
  ]
  const child = spawn('/usr/bin/python3', ['-c', 'import ctypes,signal,sys,subprocess\nchild=None\ndef stop(s,f):\n    if child and child.poll() is None: child.send_signal(signal.SIGINT)\nsignal.signal(signal.SIGINT, stop)\nsignal.signal(signal.SIGTERM, stop)\nctypes.CDLL(None).prctl(1, signal.SIGTERM)\nchild=subprocess.Popen(sys.argv[1:])\ntry:\n    sys.exit(child.wait())\nexcept BaseException:\n    if child.poll() is None:\n        child.send_signal(signal.SIGINT)\n        try: child.wait(timeout=10)\n        except subprocess.TimeoutExpired: child.kill()\n    raise', ...args], { stdio: 'ignore' })
  ffmpegChild = child
  log(`archive start -> ${out}`)
}

async function syncRelay() {
  try {
    const authCookie = await login()
    let res = await postRelayAction(authCookie, {
      action: 'sync',
      streamConfig: buildEchoPackage(),
      metadata: { title: playerName },
    })
    if (res.status === 404) {
      await ensurePlayer(authCookie, playerName)
      res = await postRelayAction(authCookie, {
        action: 'sync',
        streamConfig: buildEchoPackage(),
        metadata: { title: playerName },
      })
    }
    if (!res.ok) throw new Error(`relay sync failed: ${res.status} ${JSON.stringify(res.body)}`)
    log(`relay synced streams=${capturedStreams.size}`)
  } catch (e) {
    log(`relay sync error: ${e instanceof Error ? e.message : String(e)}`)
  }
}

async function stopRelay() {
  if (!sessionActive) return
  try {
    const authCookie = await login()
    const res = await postRelayAction(authCookie, { action: 'stop' })
    if (!res.ok && res.status !== 404) log(`relay stop warn: ${res.status}`)
    log('relay stopped')
  } catch (e) {
    log(`relay stop error: ${e instanceof Error ? e.message : String(e)}`)
  }
  sessionActive = false
  if (ffmpegChild) {
    try { ffmpegChild.kill('SIGINT') } catch {}
    ffmpegChild = null
  }
}

const lockPath = path.join(archiveRoot, `watch-${handle}.lock`)
fs.mkdirSync(archiveRoot, { recursive: true })
try {
  const fd = fs.openSync(lockPath, 'wx')
  fs.writeSync(fd, JSON.stringify({ pid: process.pid, handle, startedAt: new Date().toISOString() }))
  fs.closeSync(fd)
} catch {
  log(`another watcher holds ${lockPath}; exiting`)
  process.exit(0)
}

process.on('exit', () => {
  try { fs.rmSync(lockPath, { force: true }) } catch {}
  if (browser) { try { browser.close() } catch {} }
  if (ffmpegChild) { try { ffmpegChild.kill('SIGINT') } catch {} }
})

log(`watch start handle=${handle} until=${untilHHMM || '(12h)'} deadline=${new Date(deadlineMs).toISOString()} poll=${pollSeconds}s`)
let idleStreak = 0
let rateLimitUntil = 0
while (Date.now() < deadlineMs) {
  if (Date.now() < rateLimitUntil) {
    const pause = Math.min(60_000, rateLimitUntil - Date.now())
    await new Promise((r) => setTimeout(r, pause))
    continue
  }
  let probe: { live: boolean; status: string | null; userId: string | null; rateLimited: boolean }
  try {
    probe = await probeLive()
  } catch (e) {
    log(`probe error: ${e instanceof Error ? e.message : String(e)}`)
    await new Promise((r) => setTimeout(r, pollSeconds * 1000))
    continue
  }
  if (probe.rateLimited) {
    rateLimitUntil = Date.now() + WEB_INFO_RATE_LIMITED_MS
    continue
  }
  if (probe.live) {
    idleStreak = 0
    sessionActive = true
    startArchive()
    if (Date.now() - lastSyncAt > 15_000) {
      await syncRelay()
      lastSyncAt = Date.now()
    }
    await new Promise((r) => setTimeout(r, pollSeconds * 1000))
  } else {
    if (sessionActive) {
      await stopRelay()
      idleStreak = 0
    } else {
      idleStreak += 1
    }
    // Idle backoff: after 3 consecutive idle probes, stretch the gap to 3-5 min
    // (jittered) — a quiet window does not need dense probing.
    let wait = pollSeconds * 1000
    if (idleStreak >= 3) {
      wait = 180_000 + Math.floor(Math.random() * 120_000)
    } else if (idleStreak >= 1) {
      wait += Math.floor(Math.random() * 30_000)
    }
    await new Promise((r) => setTimeout(r, wait))
  }
}
await stopRelay()
log('watch end')
process.exit(0)
