import fs from 'fs'
import path from 'path'

const DEFAULT_HEARTBEAT_PATH = '/app/backups/runtime-heartbeat.json'
const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000
const DEFAULT_SOFT_START_THRESHOLD_MS = 15 * 60 * 1000
const DEFAULT_SOFT_START_DURATION_MS = 2 * 60 * 1000

interface RuntimeHeartbeatState {
    lastSeenAt: number | null
    downtimeMs: number | null
    softStart: boolean
    warmupUntilMs: number | null
}

interface RuntimeHeartbeatJob {
    state: RuntimeHeartbeatState
    start(): void
    stop(): void
}

function readHeartbeat(filePath: string): number | null {
    try {
        const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'))
        const value = Number(parsed?.at)
        return Number.isFinite(value) && value > 0 ? value : null
    } catch {
        return null
    }
}

function writeHeartbeat(filePath: string, at = Date.now()) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true })
    const tmp = `${filePath}.tmp`
    fs.writeFileSync(tmp, JSON.stringify({ at }))
    fs.renameSync(tmp, filePath)
}

function evaluateRuntimeHeartbeat(
    lastSeenAt: number | null,
    now = Date.now(),
    options: { thresholdMs?: number; durationMs?: number } = {},
): RuntimeHeartbeatState {
    if (!lastSeenAt || lastSeenAt > now) {
        return { lastSeenAt, downtimeMs: null, softStart: false, warmupUntilMs: null }
    }
    const downtimeMs = now - lastSeenAt
    const thresholdMs = Math.max(60_000, Number(options.thresholdMs) || DEFAULT_SOFT_START_THRESHOLD_MS)
    const durationMs = Math.max(30_000, Number(options.durationMs) || DEFAULT_SOFT_START_DURATION_MS)
    const softStart = downtimeMs >= thresholdMs
    return {
        lastSeenAt,
        downtimeMs,
        softStart,
        warmupUntilMs: softStart ? now + durationMs : null,
    }
}

function startRuntimeHeartbeatJob(options: {
    filePath?: string
    intervalMs?: number
    thresholdMs?: number
    durationMs?: number
    now?: number
    log?: { info?: (message: string) => void; warn?: (message: string) => void }
    deferWrites?: boolean
} = {}): RuntimeHeartbeatJob {
    const filePath = options.filePath || process.env.IDOL_BBQ_RUNTIME_HEARTBEAT_PATH || DEFAULT_HEARTBEAT_PATH
    const now = options.now || Date.now()
    const lastSeenAt = readHeartbeat(filePath)
    const state = evaluateRuntimeHeartbeat(lastSeenAt, now, {
        thresholdMs: options.thresholdMs,
        durationMs: options.durationMs,
    })
    if (state.softStart) {
        options.log?.warn?.(
            `[runtime-heartbeat] long downtime ${Math.round((state.downtimeMs || 0) / 1000)}s; soft-start until ${new Date(state.warmupUntilMs!).toISOString()}`,
        )
    } else {
        options.log?.info?.(
            `[runtime-heartbeat] startup gap ${state.downtimeMs === null ? 'unknown' : `${Math.round(state.downtimeMs / 1000)}s`}; normal start`,
        )
    }
    const intervalMs = Math.max(10_000, Number(options.intervalMs) || DEFAULT_HEARTBEAT_INTERVAL_MS)
    let timer: ReturnType<typeof setInterval> | undefined
    const beginWrites = () => {
        if (timer) return
        // The first write only happens once the runtime is actually ready. Writing at
        // process entry would mask a real outage as a short restart if init then fails
        // and a supervisor restarts within the threshold.
        try {
            writeHeartbeat(filePath, options.now ?? Date.now())
        } catch (error) {
            options.log?.warn?.(`[runtime-heartbeat] initial write failed: ${error instanceof Error ? error.message : String(error)}`)
        }
        timer = setInterval(() => {
            try {
                writeHeartbeat(filePath)
            } catch (error) {
                options.log?.warn?.(`[runtime-heartbeat] write failed: ${error instanceof Error ? error.message : String(error)}`)
            }
        }, intervalMs)
        timer.unref?.()
    }
    if (!options.deferWrites) {
        beginWrites()
    }
    return {
        state,
        start: beginWrites,
        stop() {
            if (timer) clearInterval(timer)
            try {
                writeHeartbeat(filePath)
            } catch {}
        },
    }
}

export {
    DEFAULT_HEARTBEAT_PATH,
    DEFAULT_SOFT_START_DURATION_MS,
    DEFAULT_SOFT_START_THRESHOLD_MS,
    evaluateRuntimeHeartbeat,
    readHeartbeat,
    startRuntimeHeartbeatJob,
    writeHeartbeat,
}
export type { RuntimeHeartbeatJob, RuntimeHeartbeatState }
