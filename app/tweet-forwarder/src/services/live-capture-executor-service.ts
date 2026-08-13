// On-demand live capture executor.
//
// Consumes live_capture_plan tasks (task_queue, type live_capture_plan) that the
// plan API stores with status=planned. When a plan window opens it claims the task
// (planned -> pending), probes the platform at capture.poll_seconds, and records the
// live with yt-dlp until the window closes (pending -> completed).
//
// Supported platforms:
//   - showroom: probe = showroom-live status API (the endpoint StreamServ used on
//     X570 — one cheap request), capture = yt-dlp showroomlive extractor.
//   - twitch:   probe = yt-dlp --dump-single-json (is_live), capture = yt-dlp
//     --live-from-start (full VOD from the start of the stream).
//
// Request budget mirrors the reference projects (yt-dlp / DouyinLiveRecorder /
// TikTok-Live-Connector / instagrapi): one probe request per poll interval while a
// window is open, capture only while actually live, no redundant fallback chains.
import { spawn, type ChildProcess } from 'child_process'
import fs from 'fs'
import path from 'path'
import DB from '@/db'
import type { Logger } from '@idol-bbq-utils/log'
import type { LiveCapturePlanPayload } from './live-capture-plan-service'

export interface LiveCaptureConfig {
    enabled?: boolean
    archive_root?: string
    yt_dlp_path?: string
    scan_interval_seconds?: number
    max_concurrent_sessions?: number
    capture_grace_seconds?: number
    probe_timeout_seconds?: number
}

export interface LiveCaptureExecutorOptions {
    config: LiveCaptureConfig
    log?: Logger
    sessionFactory?: (task: { id: number; payload: any }, options: LiveCaptureExecutorOptions) => CaptureSessionLike
}

export interface CaptureSessionLike {
    readonly id: number
    finished: boolean
    run(): void
    stop(): Promise<void>
}

const DEFAULT_ARCHIVE_ROOT = '/app/archive/live-capture'
const DEFAULT_YT_DLP_PATH = 'yt-dlp'
const DEFAULT_SCAN_INTERVAL_SECONDS = 15
const DEFAULT_MAX_CONCURRENT_SESSIONS = 4
const DEFAULT_CAPTURE_GRACE_SECONDS = 120
const DEFAULT_PROBE_TIMEOUT_SECONDS = 30

const SHOWROOM_STATUS_URL = 'https://www.showroom-live.com/api/room/status'
const SHOWROOM_STREAMING_URL = 'https://www.showroom-live.com/api/live/streaming_url'
const SHOWROOM_UA =
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36'

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

function boundedPositive(value: number | undefined, fallback: number) {
    const normalized = Number(value)
    return Number.isFinite(normalized) && normalized > 0 ? normalized : fallback
}

type ProbeStatus = 'live' | 'offline' | 'unknown'

/** Probe a showroom room via the native status API (single request, StreamServ-proven). */
export async function probeShowroomLive(
    handle: string,
    fetchImpl: typeof fetch = fetch,
    timeoutMs = DEFAULT_PROBE_TIMEOUT_SECONDS * 1000,
): Promise<boolean> {
    const detail = await probeShowroomDetail(handle, fetchImpl, timeoutMs)
    return detail.status === 'live'
}

/**
 * Probe a showroom room and return its numeric room id (for streaming_url).
 * status distinguishes a confirmed offline (`is_live:false`) from a probe failure
 * (`unknown`): a 429/timeout/network error must not be treated as "stream ended",
 * otherwise a live broadcast gets stopped mid-way during a transient API blip.
 */
export async function probeShowroomDetail(
    handle: string,
    fetchImpl: typeof fetch = fetch,
    timeoutMs = DEFAULT_PROBE_TIMEOUT_SECONDS * 1000,
): Promise<{ status: ProbeStatus; roomId: number | null }> {
    const url = `${SHOWROOM_STATUS_URL}?room_url_key=${encodeURIComponent(handle)}`
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    try {
        const response = await fetchImpl(url, {
            signal: controller.signal,
            headers: {
                accept: 'application/json',
                'user-agent': SHOWROOM_UA,
            },
        })
        if (!response.ok) {
            return { status: 'unknown', roomId: null }
        }
        const json = (await response.json()) as { is_live?: unknown; room_id?: unknown }
        const roomId = Number(json?.room_id) > 0 ? Number(json.room_id) : null
        return { status: json?.is_live === true ? 'live' : 'offline', roomId }
    } catch {
        return { status: 'unknown', roomId: null }
    } finally {
        clearTimeout(timer)
    }
}

/**
 * Resolve the master HLS playlist for a live showroom room via the native
 * streaming_url API (same endpoint StreamServ uses on X570). Returns null when
 * the room is not streaming.
 */
export async function fetchShowroomStreamingUrl(
    roomId: number,
    fetchImpl: typeof fetch = fetch,
    timeoutMs = DEFAULT_PROBE_TIMEOUT_SECONDS * 1000,
): Promise<string | null> {
    const url = `${SHOWROOM_STREAMING_URL}?room_id=${encodeURIComponent(roomId)}&_=${Date.now()}&abr_available=1`
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    try {
        const response = await fetchImpl(url, {
            signal: controller.signal,
            headers: {
                accept: 'application/json',
                'user-agent': SHOWROOM_UA,
            },
        })
        if (!response.ok) {
            return null
        }
        const json = (await response.json()) as {
            streaming_url_list?: Array<{ type?: string; url?: string }>
        }
        if (json?.streaming_url_list === false) {
            return null
        }
        const list = Array.isArray(json?.streaming_url_list) ? json.streaming_url_list : []
        // Prefer the master playlist (hls_all), then plain hls — same order as StreamServ.
        const entry =
            list.find((item) => item?.type === 'hls_all') || list.find((item) => item?.type === 'hls') || null
        const hls = entry?.url || null
        return hls?.replace(/\\u0026/g, '&') || null
    } catch {
        return null
    } finally {
        clearTimeout(timer)
    }
}

/** Build the ffmpeg HLS-copy capture command for a showroom room (native, mirrors StreamServ). */
export function buildShowroomCaptureCommand(
    archiveDir: string,
    m3u8Url: string,
    stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19),
): { bin: string; args: Array<string> } {
    const output = path.join(archiveDir, `${stamp}.mkv`)
    return {
        bin: 'ffmpeg',
        args: [
            '-y',
            '-headers',
            `User-Agent: ${SHOWROOM_UA}\r\n`,
            '-i',
            m3u8Url,
            '-c',
            'copy',
            output,
        ],
    }
}

/** Parse a yt-dlp --dump-single-json payload for a live status. */
export function parseTwitchLiveDump(json: any): boolean {
    if (!json || typeof json !== 'object') {
        return false
    }
    if (json.is_live === true) {
        return true
    }
    return json.live_status === 'is_live'
}

export function targetUrl(plan: LiveCapturePlanPayload): string {
    if (plan.target.url) {
        return plan.target.url
    }
    if (plan.target.platform === 'twitch') {
        return `https://www.twitch.tv/${plan.target.handle}`
    }
    return `https://www.showroom-live.com/${plan.target.handle}`
}

/** Build the yt-dlp capture command for a plan. */
export function buildCaptureCommand(
    plan: LiveCapturePlanPayload,
    archiveDir: string,
    ytDlpPath = DEFAULT_YT_DLP_PATH,
    socketTimeoutSeconds = DEFAULT_PROBE_TIMEOUT_SECONDS,
): { bin: string; args: Array<string> } {
    const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
    const outputTemplate = path.join(archiveDir, `${stamp}.%(ext)s`)
    const args = ['--no-playlist', '--socket-timeout', String(socketTimeoutSeconds), '-f', 'best']
    if (plan.target.platform === 'twitch') {
        // Full capture from the stream start (yt-dlp's documented Twitch mode).
        args.push('--live-from-start')
    }
    args.push('-o', outputTemplate, targetUrl(plan))
    return { bin: ytDlpPath, args }
}

function stopChildGracefully(child: ChildProcess, timeoutMs = 10_000) {
    return new Promise<void>((resolve) => {
        if (!child.pid) {
            resolve()
            return
        }
        child.once('exit', () => resolve())
        child.kill('SIGINT')
        const killer = setTimeout(() => {
            try {
                child.kill('SIGKILL')
            } catch {}
            resolve()
        }, timeoutMs)
        child.once('exit', () => clearTimeout(killer))
    })
}

interface CaptureSessionDeps {
    fetchImpl?: typeof fetch
    probeShowroom?: typeof probeShowroomDetail
}

class CaptureSession {
    readonly id: number
    readonly plan: LiveCapturePlanPayload
    finished = false
    private readonly archiveDir: string
    private readonly ytDlpPath: string
    private readonly graceMs: number
    private readonly pollMs: number
    private readonly log?: Logger
    private readonly deps: CaptureSessionDeps
    private capturing: { child: ChildProcess; startedAt: number; captureCount: number } | null = null
    private captureStarting = false
    private stopping = false
    private shuttingDown = false
    private lastRoomId: number | null = null
    private result: { captured: boolean; files: Array<string>; duration_seconds: number; last_error?: string } = {
        captured: false,
        files: [],
        duration_seconds: 0,
    }

    constructor(task: { id: number; payload: any }, options: LiveCaptureExecutorOptions) {
        this.id = task.id
        this.plan = task.payload as LiveCapturePlanPayload
        this.log = options.log
        this.deps = {}
        this.archiveDir = path.join(
            options.config.archive_root || DEFAULT_ARCHIVE_ROOT,
            this.plan.target.platform,
            this.plan.target.handle,
            String(this.plan.event.starts_at),
        )
        this.ytDlpPath = options.config.yt_dlp_path || DEFAULT_YT_DLP_PATH
        this.graceMs = boundedPositive(options.config.capture_grace_seconds, DEFAULT_CAPTURE_GRACE_SECONDS) * 1000
        this.pollMs = Math.max(5, this.plan.capture.poll_seconds || 15) * 1000
    }

    /** Fire-and-forget session loop; tracks completion via this.finished. */
    run() {
        this.loop().catch((error) => {
            this.log?.warn(`[live-capture ${this.id}] session error: ${error instanceof Error ? error.message : String(error)}`)
        })
    }

    async stop() {
        this.stopping = true
        this.shuttingDown = true
        if (this.capturing) {
            await this.recordStop('shutdown')
        }
    }

    private async loop() {
        const opensAt = this.plan.window.opens_at
        const closesAt = this.plan.window.closes_at
        const now = Math.floor(Date.now() / 1000)
        this.log?.info(
            `[live-capture ${this.id}] session start platform=${this.plan.target.platform} handle=${this.plan.target.handle} window=${opensAt}..${closesAt} poll=${this.pollMs / 1000}s`,
        )
        fs.mkdirSync(this.archiveDir, { recursive: true })
        let liveStreak = 0
        while (!this.stopping) {
            const current = Math.floor(Date.now() / 1000)
            if (current > closesAt) {
                break
            }
            let status: ProbeStatus = 'unknown'
            try {
                const probe = await this.probeOnce()
                status = probe.status
            } catch (error) {
                this.log?.warn(
                    `[live-capture ${this.id}] probe error: ${error instanceof Error ? error.message : String(error)}`,
                )
            }
            if (status === 'live') {
                liveStreak = 0
                if (!this.capturing && !this.captureStarting) {
                    await this.startCapture()
                }
            } else if (status === 'offline') {
                liveStreak += 1
                if (this.capturing && liveStreak >= 3) {
                    await this.recordStop('offline')
                }
            }
            // Unknown probe results do not advance the offline streak.
            await sleep(this.pollMs)
        }
        if (this.capturing) {
            await this.recordStop(this.shuttingDown ? 'shutdown' : 'window-end')
        }
        if (this.shuttingDown) {
            // Preserve pending status so the next process reclaims the still-open
            // window instead of treating a deploy/restart as successful completion.
            this.finished = true
            return
        }
        await this.finish()
    }

    private async probeOnce(): Promise<{ status: ProbeStatus }> {
        if (this.plan.target.platform === 'showroom') {
            const probe = this.deps.probeShowroom || probeShowroomDetail
            const detail = await probe(this.plan.target.handle, this.deps.fetchImpl)
            if (detail.status === 'live' && detail.roomId) {
                this.lastRoomId = detail.roomId
            }
            return { status: detail.status }
        }
        if (this.plan.target.platform === 'twitch') {
            // yt-dlp --dump-single-json is the probe: one lightweight run that reports
            // live state without downloading (caching + no redundant fallbacks inside).
            const dumpArgs = [
                '--no-playlist',
                '--socket-timeout',
                String(DEFAULT_PROBE_TIMEOUT_SECONDS),
                '-f',
                'best',
                '--dump-single-json',
                '--skip-download',
                targetUrl(this.plan),
            ]
            const text = await new Promise<string>((resolve, reject) => {
                const child = spawn(this.ytDlpPath, dumpArgs, {
                    stdio: ['ignore', 'pipe', 'pipe'],
                })
                let out = ''
                let err = ''
                child.stdout.on('data', (chunk) => (out += String(chunk)))
                child.stderr.on('data', (chunk) => (err += String(chunk)))
                const killer = setTimeout(() => {
                    child.kill('SIGKILL')
                    reject(new Error(`yt-dlp probe timeout: ${err.slice(0, 200)}`))
                }, DEFAULT_PROBE_TIMEOUT_SECONDS * 1000)
                child.on('error', reject)
                child.on('exit', (code) => {
                    clearTimeout(killer)
                    if (code === 0 && out) {
                        resolve(out)
                    } else {
                        reject(new Error(`yt-dlp probe exit=${code} ${err.slice(0, 200)}`))
                    }
                })
            })
            const lastLine = text.trim().split('\n').pop() || '{}'
            try {
                return { status: parseTwitchLiveDump(JSON.parse(lastLine)) ? 'live' : 'offline' }
            } catch {
                return { status: 'unknown' }
            }
        }
        this.log?.warn(`[live-capture ${this.id}] unsupported platform ${this.plan.target.platform}`)
        return { status: 'unknown' }
    }

    private async startCapture() {
        if (this.capturing || this.captureStarting || this.stopping) {
            return
        }
        this.captureStarting = true
        try {
            fs.mkdirSync(this.archiveDir, { recursive: true })
            if (this.plan.target.platform === 'showroom') {
                // Native path (mirrors StreamServ on X570): streaming_url API -> HLS -> ffmpeg copy.
                // yt-dlp's ShowRoomLive extractor is marked broken upstream, so it is not used.
                if (!this.lastRoomId) {
                    this.log?.warn(`[live-capture ${this.id}] showroom live without a room id; skip capture`)
                    return
                }
                const m3u8 = await fetchShowroomStreamingUrl(this.lastRoomId, this.deps.fetchImpl)
                if (!m3u8) {
                    this.log?.warn(`[live-capture ${this.id}] showroom streaming_url returned no HLS; will retry`)
                    return
                }
                const { bin, args } = buildShowroomCaptureCommand(this.archiveDir, m3u8)
                this.log?.info(`[live-capture ${this.id}] capturing showroom room=${this.lastRoomId} ${bin} -> mkv`)
                const child = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] })
                this.attachCaptureProcess(child)
                return
            }
            const { bin, args } = buildCaptureCommand(this.plan, this.archiveDir, this.ytDlpPath)
            this.log?.info(`[live-capture ${this.id}] capturing: ${bin} ${args.slice(0, 3).join(' ')} ...`)
            const child = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] })
            this.attachCaptureProcess(child)
        } finally {
            this.captureStarting = false
        }
    }

    private attachCaptureProcess(child: ChildProcess) {
        let stderrTail = ''
        child.stderr.on('data', (chunk) => {
            stderrTail = (stderrTail + String(chunk)).slice(-400)
        })
        child.once('exit', (code) => {
            const active = this.capturing
            if (!active || active.child !== child) {
                return
            }
            this.log?.info(
                `[live-capture ${this.id}] capture process exit=${code} ${code === 0 ? '' : `stderr=${stderrTail}`}`,
            )
            this.result.duration_seconds += Math.round((Date.now() - active.startedAt) / 1000)
            // Let the poll loop decide whether to restart; clear the slot.
            this.capturing = null
        })
        this.capturing = { child, startedAt: Date.now(), captureCount: 1 }
    }

    private async recordStop(reason: string) {
        if (!this.capturing) {
            return
        }
        const { child, startedAt } = this.capturing
        this.capturing = null
        await stopChildGracefully(child)
        this.result.duration_seconds += Math.round((Date.now() - startedAt) / 1000)
        this.log?.info(`[live-capture ${this.id}] capture stop reason=${reason}`)
    }

    private async finish() {
        const files: Array<string> = []
        try {
            for (const entry of fs.readdirSync(this.archiveDir)) {
                if (/\.(mp4|mkv|ts|webm)$/.test(entry)) {
                    files.push(entry)
                }
            }
        } catch {}
        this.result.captured = files.length > 0
        this.result.files = files
        if (files.length === 0) {
            this.result.last_error = 'no live detected in window'
        }
        const status = 'completed'
        const payload = {
            ...this.plan,
            capture_result: this.result,
            capture_result_iso: new Date().toISOString(),
        }
        try {
            await DB.TaskQueue.updateTaskStatus(this.id, {
                status,
                payload,
                last_error: this.result.last_error || null,
            })
        } catch (error) {
            this.log?.warn(`[live-capture ${this.id}] status update failed: ${error instanceof Error ? error.message : String(error)}`)
        }
        this.log?.info(
            `[live-capture ${this.id}] session finished captured=${this.result.captured} files=${files.length} duration=${this.result.duration_seconds}s`,
        )
        this.finished = true
    }
}

export class LiveCaptureExecutor {
    readonly NAME = 'LiveCaptureExecutor'
    enabled: boolean
    private readonly options: LiveCaptureExecutorOptions
    private readonly log?: Logger
    private readonly sessions = new Map<number, CaptureSessionLike>()
    private timer: ReturnType<typeof setInterval> | null = null
    private stopping = false

    constructor(options: LiveCaptureExecutorOptions) {
        this.options = options
        this.log = options.log
        this.enabled = options.config.enabled === true
    }

    async init() {
        if (!this.enabled) {
            this.log?.info('[live-capture] executor disabled')
            return
        }
        this.log?.info('[live-capture] executor enabled, scanning every ' +
            `${boundedPositive(this.options.config.scan_interval_seconds, DEFAULT_SCAN_INTERVAL_SECONDS)}s`)
        const scanMs = boundedPositive(this.options.config.scan_interval_seconds, DEFAULT_SCAN_INTERVAL_SECONDS) * 1000
        this.timer = setInterval(() => {
            this.tick().catch((error) => {
                this.log?.warn(`[live-capture] scan error: ${error instanceof Error ? error.message : String(error)}`)
            })
        }, scanMs)
        await this.tick()
    }

    async tick() {
        if (this.stopping) {
            return
        }
        const now = Math.floor(Date.now() / 1000)
        const maxConcurrent = Math.max(
            0,
            Math.floor(
                this.options.config.max_concurrent_sessions === undefined
                    ? DEFAULT_MAX_CONCURRENT_SESSIONS
                    : Number(this.options.config.max_concurrent_sessions),
            ),
        )
        const due = await DB.TaskQueue.getDue(DB.TaskQueue.TYPE.LiveCapturePlan, ['planned', 'pending'], now)
        for (const task of due) {
            if (this.stopping) {
                break
            }
            const plan = task.payload as LiveCapturePlanPayload
            if (!plan?.window?.closes_at) {
                continue
            }
            if (now > plan.window.closes_at && !this.sessions.has(task.id)) {
                await DB.TaskQueue.updateTaskStatus(task.id, {
                    status: 'completed',
                    last_error: 'window closed before executor could open it',
                    payload: { ...plan, capture_result: { captured: false, files: [], duration_seconds: 0 } },
                })
                continue
            }
            if (maxConcurrent <= 0 || this.sessions.has(task.id) || this.sessions.size >= maxConcurrent) {
                continue
            }
            if (task.status === 'planned') {
                // CAS-claim so a second executor instance (hot migration window)
                // cannot double-capture the same plan: only the claimer proceeds.
                const claimedTask = await DB.TaskQueue.claimPlanned(task.id).catch(() => null)
                if (!claimedTask) {
                    continue
                }
            }
            const sessionFactory = this.options.sessionFactory || ((t, o) => new CaptureSession(t, o))
            const session = sessionFactory(task, this.options)
            this.sessions.set(task.id, session)
            session.run()
        }
        for (const [id, session] of this.sessions) {
            if (session.finished) {
                this.sessions.delete(id)
            }
        }
    }

    async stop() {
        this.stopping = true
        if (this.timer) {
            clearInterval(this.timer)
            this.timer = null
        }
        await Promise.all(Array.from(this.sessions.values()).map((session) => session.stop()))
        this.sessions.clear()
    }

    async drop() {
        await this.stop()
    }
}

export { DEFAULT_ARCHIVE_ROOT, DEFAULT_YT_DLP_PATH }
