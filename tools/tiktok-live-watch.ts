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
import { spawn } from 'child_process'
import puppeteer from 'puppeteer-core'

function arg(name: string, dflt?: string) {
  const i = process.argv.indexOf(name)
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : dflt
}
const handle = (process.argv[2] || 'mao_asaoka').replace(/^@/, '')
const untilHHMM = arg('--until')
const maxMinutes = Number(arg('--max-minutes', '240'))
const pollSeconds = Number(arg('--poll', '20'))
const once = process.argv.includes('--once')
const archiveRoot = arg('--archive-root', '/app/archive/tiktok-live')!
const cookiePath = arg('--cookie', '/app/assets/cookies/tcookies.txt')!

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
  if (q.includes('origin') || q.includes('full_hd') || q.includes('uhd')) s += 100
  else if (q.includes('hd')) s += 80
  else if (q.includes('sd') || q.includes('ld')) s += 40
  if (c.kind === 'flv') s += 5 // flv tends to be more capture-stable here
  return s
}

async function checkLiveAndCapture(): Promise<'captured-until-end' | 'not-live' | 'error'> {
  const browser = await puppeteer.launch({
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/google-chrome-stable',
    headless: true,
    userDataDir: `/tmp/tt-watch-${handle}-${Date.now()}`,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  })
  try {
    const page = await browser.newPage()
    const ua = await browser.userAgent()
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
    if (!roomId) { log('no roomId found (user not live / not found)'); return 'not-live' }
    const api = await page.evaluate(async (rid) => {
      const url = `https://webcast.tiktok.com/webcast/room/info/?aid=1988&app_language=ja&room_id=${rid}`
      const r = await fetch(url, { credentials: 'include' })
      return { status: r.status, text: await r.text() }
    }, roomId).catch((e) => ({ status: 0, text: String(e) }))
    let roomData: any = null
    try { roomData = JSON.parse(api.text)?.data } catch {}
    const status = roomData?.status
    log(`roomId=${roomId} api=${api.status} room.status=${status} (2=living,4=ended)`)
    if (status !== 2) return 'not-live'

    const candidates = pickPullUrls(roomData).sort((a, b) => rank(b) - rank(a))
    if (!candidates.length) { log('live but no pull urls in payload'); return 'not-live' }

    const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
    const outDir = path.join(archiveRoot, `${handle}-${stamp}`)
    fs.mkdirSync(outDir, { recursive: true })
    const mediaPath = path.join(outDir, `${handle}-${stamp}.mkv`)
    const ffLog = path.join(outDir, 'ffmpeg.log')
    fs.writeFileSync(path.join(outDir, 'stream.json'), JSON.stringify({ handle, roomId, chosen: candidates[0].quality, kind: candidates[0].kind, candidates: candidates.map(c => ({ quality: c.quality, kind: c.kind })) }, null, 2))
    const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ')
    const headers = `User-Agent: ${ua}\r\nReferer: https://www.tiktok.com/@${handle}/live\r\nOrigin: https://www.tiktok.com\r\nCookie: ${cookieHeader}\r\n`
    const chosen = candidates[0]
    log(`capturing quality=${chosen.quality} kind=${chosen.kind} -> ${mediaPath}`)
    // Foreground ffmpeg: it exits when the live ends (stream closes).
    const capSeconds = Math.max(30, Math.floor((deadlineMs - Date.now()) / 1000))
    await new Promise<void>((resolve) => {
      const fd = fs.openSync(ffLog, 'a')
      const child = spawn('/usr/bin/ffmpeg', ['-y', '-headers', headers, '-i', chosen.url, '-t', String(capSeconds), '-c', 'copy', mediaPath], { stdio: ['ignore', fd, fd] })
      child.on('close', () => resolve())
      child.on('error', () => resolve())
    })
    const size = fs.existsSync(mediaPath) ? fs.statSync(mediaPath).size : 0
    log(`capture ended size=${size} bytes file=${mediaPath}`)
    return 'captured-until-end'
  } catch (e) {
    log(`error: ${e instanceof Error ? e.message : String(e)}`)
    return 'error'
  } finally {
    await browser.close().catch(() => {})
  }
}

;(async () => {
  log(`watch start handle=${handle} until=${untilHHMM || '(max-minutes)'} deadline=${new Date(deadlineMs).toISOString()} poll=${pollSeconds}s once=${once}`)
  while (Date.now() < deadlineMs) {
    const res = await checkLiveAndCapture()
    if (res === 'captured-until-end') {
      // After a capture ends, re-check shortly in case of reconnect; then continue polling.
      log('post-capture recheck in 15s')
      await new Promise((r) => setTimeout(r, 15_000))
      continue
    }
    if (once) { log('once mode: not live now, exiting'); break }
    await new Promise((r) => setTimeout(r, pollSeconds * 1000))
  }
  log('watch end (deadline reached or once)')
})()
