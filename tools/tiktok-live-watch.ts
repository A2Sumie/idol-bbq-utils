// TikTok Live watcher/capturer (no upload).
// Polls a target handle within an optional time window, detects live via the
// authoritative webcast room-info API (status===2 means living), extracts pull
// URLs from the same API payload, and captures with ffmpeg copy to MKV.
// Safe to run from cron/schedule: it self-exits on end-of-window or after the
// live ends. It never uploads.
//
// Usage (inside container):
//   bun /app/tiktok-live-watch.ts <handle> [--until HH:MM] [--max-minutes N] [--poll 20] [--once]
import fs from 'fs'
import path from 'path'
import { spawn, spawnSync } from 'child_process'
import puppeteer from 'puppeteer-core'

function arg(name: string, dflt?: string) {
  const i = process.argv.indexOf(name)
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : dflt
}
const handle = (process.argv[2] || 'mao_asaoka').replace(/^@/, '')
const untilHHMM = arg('--until')
const maxMinutes = Number(arg('--max-minutes', '240'))
const pollSeconds = Number(arg('--poll', '15'))
const once = process.argv.includes('--once')
const archiveRoot = arg('--archive-root', '/app/archive/tiktok-live')!
const cookiePath = arg('--cookie', '/app/assets/cookies/tcookies.txt')!
const ffmpegBin = arg('--ffmpeg', '/usr/bin/ffmpeg')!
const ffprobeBin = arg('--ffprobe', '/usr/bin/ffprobe')!
const firstByteTimeoutMs = Number(arg('--first-byte-timeout', '30')) * 1000
const minValidDuration = Number(arg('--min-valid-duration', '2'))
const ffmpegMonitor = `
import ctypes, os, signal, subprocess, sys
child = None
def stop(sig, frame):
    if child and child.poll() is None:
        child.send_signal(signal.SIGINT)
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
parent = os.getppid()
ctypes.CDLL(None).prctl(1, signal.SIGTERM)
if os.getppid() != parent:
    sys.exit(143)
child = subprocess.Popen(sys.argv[1:])
try:
    sys.exit(child.wait())
except BaseException:
    if child.poll() is None:
        child.send_signal(signal.SIGINT)
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.kill()
    raise
`
const cleanupMonitor = `
import ctypes, json, os, pathlib, shutil, signal, sys, time
owner = int(sys.argv[1])
lock = pathlib.Path(sys.argv[2])
prefix = sys.argv[3]
def cleanup(sig=None, frame=None):
    targets = []
    for proc in pathlib.Path('/proc').glob('[0-9]*'):
        try:
            comm = (proc / 'comm').read_text().strip()
            cmd = (proc / 'cmdline').read_bytes().replace(b'\\0', b' ').decode(errors='replace')
        except Exception:
            continue
        if comm in {'chrome', 'google-chrome'} and prefix in cmd:
            targets.append(int(proc.name))
    for pid in targets:
        try: os.kill(pid, signal.SIGTERM)
        except ProcessLookupError: pass
    time.sleep(2)
    for pid in targets:
        if pathlib.Path(f'/proc/{pid}').exists():
            try: os.kill(pid, signal.SIGKILL)
            except ProcessLookupError: pass
    for profile in pathlib.Path('/tmp').glob(prefix + '*'):
        shutil.rmtree(profile, ignore_errors=True)
    try:
        if json.loads(lock.read_text()).get('pid') == owner:
            lock.unlink(missing_ok=True)
    except Exception:
        pass
    sys.exit(0)
signal.signal(signal.SIGTERM, cleanup)
parent = os.getppid()
ctypes.CDLL(None).prctl(1, signal.SIGTERM)
if os.getppid() != parent:
    cleanup()
while os.getppid() == parent:
    time.sleep(1)
cleanup()
`

const deadlineMs = (() => {
  const byMax = Date.now() + maxMinutes * 60_000
  if (!untilHHMM) return byMax
  const [h, m] = untilHHMM.split(':').map(Number)
  const d = new Date()
  const jstNow = new Date(d.toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }))
  const target = new Date(jstNow); target.setHours(h, m, 0, 0)
  if (target <= jstNow) target.setDate(target.getDate() + 1)
  const untilMs = Date.now() + (target.getTime() - jstNow.getTime())
  return Math.min(byMax, untilMs)
})()

function log(msg: string) { process.stdout.write(`[tt-watch ${new Date().toISOString()}] ${msg}\n`) }

const lockPath = path.join(archiveRoot, `watch-${handle}.lock`)
let lockHeld = false
function acquireLock(): boolean {
  fs.mkdirSync(archiveRoot, { recursive: true })
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const fd = fs.openSync(lockPath, 'wx')
      fs.writeSync(fd, JSON.stringify({ pid: process.pid, handle, startedAt: new Date().toISOString() }))
      fs.closeSync(fd)
      lockHeld = true
      return true
    } catch (e: any) {
      if (e?.code !== 'EEXIST') throw e
      let stale = true
      try {
        const prev = JSON.parse(fs.readFileSync(lockPath, 'utf8'))
        if (prev?.pid) { try { process.kill(prev.pid, 0); stale = false } catch { stale = true } }
      } catch { stale = true }
      if (!stale) return false
      log(`removing stale lock ${lockPath}`)
      try { fs.rmSync(lockPath) } catch {}
    }
  }
  return false
}
function releaseLock() {
  if (!lockHeld) return
  try {
    const prev = JSON.parse(fs.readFileSync(lockPath, 'utf8'))
    if (prev?.pid === process.pid) fs.rmSync(lockPath)
  } catch {}
  lockHeld = false
}
function startCleanupMonitor() {
  cleanupChild = spawn('/usr/bin/python3', ['-c', cleanupMonitor, String(process.pid), lockPath, `tt-watch-${handle}-${process.pid}-`], { stdio: 'ignore' })
}

let stopping = false
let activeChild: ReturnType<typeof spawn> | null = null
let cleanupChild: ReturnType<typeof spawn> | null = null
const activeProfileDirs = new Set<string>()

function cleanupProfiles() {
  for (const dir of activeProfileDirs) { try { fs.rmSync(dir, { recursive: true, force: true }) } catch {} }
  activeProfileDirs.clear()
}
function onSignal(sig: NodeJS.Signals) {
  if (stopping) return
  stopping = true
  log(`received ${sig}; finalizing current capture then exiting`)
  if (activeChild && !activeChild.killed) { try { activeChild.kill('SIGINT') } catch {} }
}
process.on('SIGINT', () => onSignal('SIGINT'))
process.on('SIGTERM', () => onSignal('SIGTERM'))
process.on('exit', () => {
  cleanupProfiles()
  releaseLock()
  if (cleanupChild && !cleanupChild.killed) { try { cleanupChild.kill('SIGTERM') } catch {} }
  if (persistentBrowser) { try { persistentBrowser.close() } catch {} }
})

function parseCookies(fp: string) {
  const cookies: any[] = []
  if (!fs.existsSync(fp)) return cookies
  for (let raw of fs.readFileSync(fp, 'utf8').split(/\r?\n/)) {
    raw = raw.trim(); let httpOnly = false
    if (raw.startsWith('#HttpOnly_')) { httpOnly = true; raw = raw.slice(10) }
    if (!raw || raw.startsWith('#')) continue
    const p = raw.split('\t')
    if (p.length !== 7 || !p[0].includes('tiktok.com')) continue
    cookies.push({ name: p[5], value: p[6], domain: p[0].startsWith('.') ? p[0] : `.${p[0]}`, path: p[2] || '/', secure: String(p[3]).toUpperCase() === 'TRUE', httpOnly })
  }
  return cookies
}
function pickPullUrls(roomData: any): Array<{ quality: string; kind: string; url: string }> {
  const out: Array<{ quality: string; kind: string; url: string }> = []
  const su = roomData?.stream_url
  if (!su) return out
  // Preferred: sdk pull data with quality ladder
  const raw = su?.live_core_sdk_data?.pull_data?.stream_data
  let sd: any = null
  if (typeof raw === 'string') { try { sd = JSON.parse(raw) } catch {} }
  if (sd?.data) {
    for (const [quality, q] of Object.entries<any>(sd.data)) {
      if (q?.main?.flv) out.push({ quality, kind: 'flv', url: q.main.flv })
      if (q?.main?.hls) out.push({ quality, kind: 'hls', url: q.main.hls })
    }
  }
  // Fallback: flat maps
  const flv = su?.flv_pull_url
  if (flv && typeof flv === 'object') for (const [k, v] of Object.entries<any>(flv)) if (typeof v === 'string') out.push({ quality: `flv:${k}`, kind: 'flv', url: v })
  const rtmp = su?.rtmp_pull_url
  if (typeof rtmp === 'string') out.push({ quality: 'origin', kind: 'rtmp', url: rtmp })
  return out
}
function rank(c: { quality: string; kind: string }) {
  const q = c.quality.toLowerCase()
  let s = 0
  if (q.includes('origin')) s += 200
  else if (q.includes('full_hd') || q.includes('uhd')) s += 100
  else if (q.includes('hd')) s += 80
  else if (q.includes('sd') || q.includes('ld')) s += 40
  if (c.kind === 'flv') s += 5 // flv tends to be more capture-stable here
  return s
}

type Candidate = { quality: string; kind: string; url: string }
type Probe = { handle: string; roomId: string; status: number; candidates: Candidate[] } | { handle: string; roomId: null; status: number | null; candidates: [] }

// Persistent browser session reused across probes: the WAF challenge is passed once
// and its cookies survive in the fixed profile, so every later probe is a cheap
// navigation instead of a fresh browser launch + challenge. Falls back to a fresh
// browser when the persistent one crashes.
const persistentProfileDir = `/tmp/tt-watch-${process.pid}-profile`
let persistentBrowser: Awaited<ReturnType<typeof puppeteer.launch>> | null = null
let persistentPage: any = null
async function ensurePersistentBrowser() {
  if (persistentBrowser && persistentBrowser.isConnected()) {
    return persistentBrowser
  }
  if (persistentBrowser) {
    await persistentBrowser.close().catch(() => {})
    persistentBrowser = null
    persistentPage = null
  }
  persistentBrowser = await puppeteer.launch({
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/google-chrome-stable',
    headless: true,
    userDataDir: persistentProfileDir,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  })
  persistentPage = await persistentBrowser.newPage()
  await persistentPage.setExtraHTTPHeaders({ 'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7' })
  const cookies = parseCookies(cookiePath)
  if (cookies.length) await persistentPage.setCookie(...cookies)
  return persistentBrowser
}

async function persistentBrowserProbe(): Promise<Probe> {
  try {
    await ensurePersistentBrowser()
    await persistentPage
      .goto(`https://www.tiktok.com/@${handle}/live`, { waitUntil: 'domcontentloaded', timeout: 45000 })
      .catch(() => {})
    await new Promise((r) => setTimeout(r, 2000))
    const room = await persistentPage
      .evaluate(() => {
        const s = (window as any).SIGI_STATE
        const liveRoom = s?.LiveRoom?.liveRoomUserInfo?.liveRoom
        const user = s?.LiveRoom?.liveRoomUserInfo?.user
        const roomId = user?.roomId || liveRoom?.roomId || null
        const status = liveRoom?.status ?? null
        return { roomId, status }
      })
      .catch(() => null)
    const roomId = room?.roomId || null
    if (!roomId) {
      log('no roomId found (user not live / not found)')
      return { handle, roomId: null, status: null, candidates: [] }
    }
    // Skip the webcast API call when SIGI_STATE already says the room is not live.
    if (room.status !== null && room.status !== 2) {
      return { handle, roomId, status: Number(room.status), candidates: [] }
    }
    const api = await persistentPage
      .evaluate(async (rid) => {
        const url = `https://webcast.tiktok.com/webcast/room/info/?aid=1988&app_language=ja&room_id=${rid}`
        const r = await fetch(url, { credentials: 'include' })
        return { status: r.status, text: await r.text() }
      }, roomId)
      .catch((e) => ({ status: 0, text: String(e) }))
    let roomData: any = null
    try {
      roomData = JSON.parse(api.text)?.data
    } catch {}
    const status = roomData?.status ?? null
    log(`roomId=${roomId} api=${api.status} room.status=${status} (2=living,4=ended)`)
    if (status !== 2) return { handle, roomId, status, candidates: [] }
    const candidates = pickPullUrls(roomData).sort((a, b) => rank(b) - rank(a))
    return { handle, roomId, status, candidates }
  } catch (e) {
    log(`persistent probe error: ${e instanceof Error ? e.message : String(e)}`)
    await persistentBrowser?.close().catch(() => {})
    persistentBrowser = null
    persistentPage = null
    return { handle, roomId: null, status: null, candidates: [] }
  }
}

async function probeLive(): Promise<Probe> {
  const userDataDir = `/tmp/tt-watch-${handle}-${process.pid}-${Date.now()}`
  const browser = await puppeteer.launch({
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/google-chrome-stable',
    headless: true,
    userDataDir,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  })
  activeProfileDirs.add(userDataDir)
  try {
    const page = await browser.newPage()
    await page.setExtraHTTPHeaders({ 'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7' })
    const cookies = parseCookies(cookiePath)
    if (cookies.length) await page.setCookie(...cookies)
    await page.goto(`https://www.tiktok.com/@${handle}/live`, { waitUntil: 'networkidle2', timeout: 45000 }).catch(() => {})
    await new Promise((r) => setTimeout(r, 4000))
    const roomId = await page.evaluate(() => {
      const s = (window as any).SIGI_STATE
      const room = s?.LiveRoom?.liveRoomUserInfo?.liveRoom
      const user = s?.LiveRoom?.liveRoomUserInfo?.user
      return user?.roomId || room?.roomId || null
    }).catch(() => null)
    if (!roomId) { log('no roomId found (user not live / not found)'); return { handle, roomId: null, status: null, candidates: [] } }
    const api = await page.evaluate(async (rid) => {
      const url = `https://webcast.tiktok.com/webcast/room/info/?aid=1988&app_language=ja&room_id=${rid}`
      const r = await fetch(url, { credentials: 'include' })
      return { status: r.status, text: await r.text() }
    }, roomId).catch((e) => ({ status: 0, text: String(e) }))
    let roomData: any = null
    try { roomData = JSON.parse(api.text)?.data } catch {}
    const status = roomData?.status ?? null
    log(`roomId=${roomId} api=${api.status} room.status=${status} (2=living,4=ended)`)
    if (status !== 2) return { handle, roomId, status, candidates: [] }
    const candidates = pickPullUrls(roomData).sort((a, b) => rank(b) - rank(a))
    return { handle, roomId, status, candidates }
  } catch (e) {
    log(`probe error: ${e instanceof Error ? e.message : String(e)}`)
    return { handle, roomId: null, status: null, candidates: [] }
  } finally {
    await browser.close().catch(() => {})
    try { fs.rmSync(userDataDir, { recursive: true, force: true }) } catch {}
    activeProfileDirs.delete(userDataDir)
  }
}

function buildHeaders(): string {
  const cookies = parseCookies(cookiePath)
  const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ')
  const ua = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
  return `User-Agent: ${ua}\r\nReferer: https://www.tiktok.com/@${handle}/live\r\nOrigin: https://www.tiktok.com\r\nCookie: ${cookieHeader}\r\n`
}

function buildFetchHeaders(): Record<string, string> {
  const cookies = parseCookies(cookiePath)
  const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ')
  return {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    Referer: `https://www.tiktok.com/@${handle}/live`,
    Origin: 'https://www.tiktok.com',
    'Accept-Language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
    Cookie: cookieHeader,
  }
}

function probeMedia(file: string): Record<string, any> {
  try {
    const res = spawnSync(ffprobeBin, ['-v', 'error', '-show_entries', 'format=duration,size:stream=index,codec_name,codec_type,width,height,avg_frame_rate,sample_rate,channels', '-of', 'json', file], { encoding: 'utf8' })
    if (res.status === 0 && res.stdout) return JSON.parse(res.stdout)
  } catch {}
  return {}
}

function captureCandidate(cand: Candidate, mediaPath: string, ffLog: string, capSeconds: number): Promise<{ ok: boolean; bytes: number }> {
  const headers = buildHeaders()
  return new Promise((resolve) => {
    const fd = fs.openSync(ffLog, 'a')
    const ffmpegArgs = [ffmpegBin, '-y', '-headers', headers, '-i', cand.url, '-t', String(capSeconds), '-c', 'copy', mediaPath]
    const child = spawn('/usr/bin/python3', ['-c', ffmpegMonitor, ...ffmpegArgs], { stdio: ['ignore', fd, fd] })
    activeChild = child
    let settled = false
    let forceTimer: ReturnType<typeof setTimeout> | null = null
    const finish = (ok: boolean) => {
      if (settled) return
      settled = true
      clearTimeout(watchdog)
      clearTimeout(wallTimer)
      if (forceTimer) clearTimeout(forceTimer)
      try { fs.closeSync(fd) } catch {}
      if (activeChild === child) activeChild = null
      const bytes = fs.existsSync(mediaPath) ? fs.statSync(mediaPath).size : 0
      resolve({ ok: ok && bytes > 0, bytes })
    }
    const watchdog = setTimeout(() => {
      const bytes = fs.existsSync(mediaPath) ? fs.statSync(mediaPath).size : 0
      if (bytes === 0 && !stopping) {
        log(`no bytes within ${firstByteTimeoutMs / 1000}s on quality=${cand.quality} kind=${cand.kind}; dropping`)
        try { child.kill('SIGKILL') } catch {}
      }
    }, firstByteTimeoutMs)
    const wallTimer = setTimeout(() => {
      log(`capture wall-clock limit reached after ${capSeconds}s; finalizing`)
      try { child.kill('SIGINT') } catch {}
      forceTimer = setTimeout(() => { try { child.kill('SIGKILL') } catch {} }, 10_000)
    }, capSeconds * 1000)
    child.on('close', () => finish(true))
    child.on('error', () => finish(false))
  })
}

type SessionState = { roomId: string; dir: string; part: number; manifest: any }
let session: SessionState | null = null

function openSession(roomId: string): SessionState {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
  const dir = path.join(archiveRoot, `${handle}-${stamp}`)
  fs.mkdirSync(dir, { recursive: true })
  const manifest = { handle, roomId, startedAt: new Date().toISOString(), parts: [] as any[] }
  return { roomId, dir, part: 0, manifest }
}
function writeManifest(s: SessionState) {
  try { fs.writeFileSync(path.join(s.dir, 'manifest.json'), JSON.stringify(s.manifest, null, 2)) } catch {}
}

async function captureSegment(probe: Extract<Probe, { candidates: Candidate[] }>): Promise<boolean> {
  if (!probe.roomId || probe.status !== 2 || !probe.candidates.length) return false
  if (!session || session.roomId !== probe.roomId) {
    if (session) writeManifest(session)
    session = openSession(probe.roomId)
    log(`new session room=${probe.roomId} dir=${session.dir}`)
  }
  session.part += 1
  const partName = `part${String(session.part).padStart(2, '0')}`
  const mediaPath = path.join(session.dir, `${partName}.mkv`)
  const ffLog = path.join(session.dir, `${partName}.ffmpeg.log`)

  let cands = probe.candidates
  const maxAttempts = cands.length + 2
  for (let attempt = 0, idx = 0; attempt < maxAttempts && idx < cands.length; attempt++, idx++) {
    if (stopping || Date.now() >= deadlineMs) break
    const cand = cands[idx]
    const capSeconds = Math.max(1, Math.floor((deadlineMs - Date.now()) / 1000))
    log(`capturing ${partName} attempt=${attempt + 1} quality=${cand.quality} kind=${cand.kind} -> ${mediaPath}`)
    const startedAt = new Date().toISOString()
    const { ok, bytes } = await captureCandidate(cand, mediaPath, ffLog, capSeconds)
    const media = ok ? probeMedia(mediaPath) : {}
    const v = (media.streams || []).find((s: any) => s.codec_type === 'video')
    const a = (media.streams || []).find((s: any) => s.codec_type === 'audio')
    const duration = Number(media?.format?.duration || 0)
    if (ok && v && duration >= minValidDuration) {
      session.manifest.parts.push({
        part: partName, file: `${partName}.mkv`, quality: cand.quality, kind: cand.kind,
        bytes, duration: media?.format?.duration ?? null,
        video: { codec: v.codec_name, width: v.width, height: v.height, fps: v.avg_frame_rate },
        audio: a ? { codec: a.codec_name, sampleRate: a.sample_rate, channels: a.channels } : null,
        startedAt, endedAt: new Date().toISOString(),
      })
      writeManifest(session)
      log(`${partName} ok bytes=${bytes} dur=${duration}s`)
      return true
    }
    log(`candidate failed (bytes=${bytes} dur=${duration}s video=${Boolean(v)}); advancing to next candidate`)
    try { if (fs.existsSync(mediaPath)) fs.rmSync(mediaPath) } catch {}
    if (stopping || Date.now() >= deadlineMs) break
    const fresh = await probeLive()
    if (!fresh.roomId || fresh.status !== 2 || fresh.candidates.length === 0) { log('room no longer live during fallback'); return false }
    if (fresh.roomId !== session.roomId) { log('room changed during fallback; deferring to outer loop'); return false }
    cands = fresh.candidates
    if (idx + 1 >= cands.length) { log('exhausted candidate ladder'); return false }
  }
  return false
}

// Probe cadence: tight with jitter so the live start is always caught within a few
// seconds, while the random walk keeps the pattern human-like. Idle probes are served
// by the lightweight HTTP path (probeLiveHttp) instead of a full browser page load,
// so frequent probing never hammers the shared IP the regular TikTok crawler uses.
function nextProbeDelayMs() {
    const base = Math.max(1, pollSeconds) * 1000
    return base + Math.floor(Math.random() * Math.min(base, 8_000))
}

async function probeLiveHttp(): Promise<Probe | null> {
    const headers = buildFetchHeaders()
    let res: Response
    try {
        res = await fetch(`https://www.tiktok.com/@${handle}/live`, {
            headers,
            redirect: 'follow',
            signal: AbortSignal.timeout(15_000),
        })
    } catch {
        return null
    }
    if (!res.ok) {
        return null
    }
    const html = await res.text()
    if (html.length < 5_000 || !html.includes('liveRoomUserInfo')) {
        return null
    }
    const roomId = extractRoomIdFromHtml(html)
    if (!roomId) {
        return { handle, roomId: null, status: null, candidates: [] }
    }
    // The page embeds the current room status; skip the webcast API call entirely when
    // the room is not live (status 4/null) to halve idle probe pressure.
    const htmlStatus = extractRoomStatusFromHtml(html)
    if (htmlStatus !== null && htmlStatus !== 2) {
        return { handle, roomId, status: htmlStatus, candidates: [] }
    }
    let api: Response
    try {
        api = await fetch(
            `https://webcast.tiktok.com/webcast/room/info/?aid=1988&app_language=ja&room_id=${roomId}`,
            { headers, signal: AbortSignal.timeout(10_000) },
        )
    } catch {
        return { handle, roomId, status: null, candidates: [] }
    }
    let roomData: any = null
    try {
        roomData = JSON.parse(await api.text())?.data
    } catch {
        return { handle, roomId, status: null, candidates: [] }
    }
    const status = roomData?.status ?? null
    if (status !== 2) {
        return { handle, roomId, status, candidates: [] }
    }
    return { handle, roomId, status, candidates: pickPullUrls(roomData) }
}

function extractRoomStatusFromHtml(html: string): number | null {
    const section = html.slice(html.indexOf('liveRoomUserInfo'), html.indexOf('liveRoomUserInfo') + 20_000)
    const match = section.match(/"status"\s*:\s*(\d+)/)
    if (!match) {
        return null
    }
    const value = Number(match[1])
    return Number.isFinite(value) ? value : null
}

function extractRoomIdFromHtml(html: string): string | null {
    const section = html.slice(html.indexOf('liveRoomUserInfo'), html.indexOf('liveRoomUserInfo') + 20_000)
    const candidates = [
        /(?:roomId|room_id)["']?\s*[:=]\s*["']?(\d+)/g,
        /"roomId":"(\d+)"/g,
    ]
    for (const pattern of candidates) {
        const match = section.match(pattern)
        if (match) {
            const first = match[0]
            const number = first.match(/(\d+)/)?.[1]
            if (number) {
                return number
            }
        }
    }
    return null
}

;(async () => {
  if (!acquireLock()) { log(`another watcher holds ${lockPath}; exiting`); process.exit(0) }
  startCleanupMonitor()
  log(`watch start handle=${handle} until=${untilHHMM || '(max-minutes)'} deadline=${new Date(deadlineMs).toISOString()} poll=${pollSeconds}s once=${once}`)
  let endedStreak = 0
  let firstEndedAt = 0
  let httpProbes = 0
  let browserProbes = 0
  while (Date.now() < deadlineMs && !stopping) {
    let probe = await probeLiveHttp()
    if (probe) {
      httpProbes += 1
    }
    if (!probe || (probe.status === 2 && probe.candidates.length === 0)) {
      probe = await persistentBrowserProbe()
      browserProbes += 1
    }
    if (probe.status === 2 && probe.roomId && probe.candidates.length) {
      endedStreak = 0
      firstEndedAt = 0
      const wrote = await captureSegment(probe as any)
      if (wrote) {
        log('segment ended; reconnect recheck in 10s')
        await new Promise((r) => setTimeout(r, 10_000))
        continue
      }
    } else if (session && probe.status === 4) {
      endedStreak += 1
      if (!firstEndedAt) firstEndedAt = Date.now()
      log(`ended confirmation ${endedStreak}/3; elapsed=${Math.floor((Date.now() - firstEndedAt) / 1000)}s/300s`)
    } else {
      endedStreak = 0
      firstEndedAt = 0
    }
    const activeSession = session as SessionState | null
    if (activeSession && endedStreak >= 3 && Date.now() - firstEndedAt >= 300_000) {
      writeManifest(activeSession)
      log(`session finalized room=${activeSession.roomId} parts=${activeSession.manifest.parts.length}`)
      session = null
      if (once) { log('once mode: session done, exiting'); break }
    }
    if (once && !session) { log('once mode: not live now, exiting'); break }
    if (stopping) break
    await new Promise((r) => setTimeout(r, nextProbeDelayMs()))
  }
  if (session) writeManifest(session)
  log(`watch end (${stopping ? 'stopped' : 'deadline reached or once'})`)
  process.exit(0)
})()
