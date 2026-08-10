import { afterEach, expect, test } from 'bun:test'
import fs from 'fs'
import os from 'os'
import path from 'path'
import {
    evaluateRuntimeHeartbeat,
    readHeartbeat,
    startRuntimeHeartbeatJob,
    writeHeartbeat,
} from './runtime-heartbeat-service'

const dirs: string[] = []
afterEach(() => {
    for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true })
})

function tempFile() {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'runtime-heartbeat-'))
    dirs.push(dir)
    return path.join(dir, 'heartbeat.json')
}

test('runtime heartbeat distinguishes deploy restart from long outage', () => {
    const now = 1_800_000_000_000
    const deploy = evaluateRuntimeHeartbeat(now - 4 * 60 * 1000, now)
    expect(deploy.softStart).toBe(false)
    expect(deploy.warmupUntilMs).toBeNull()

    const outage = evaluateRuntimeHeartbeat(now - 40 * 60 * 1000, now)
    expect(outage.softStart).toBe(true)
    expect(outage.warmupUntilMs).toBe(now + 2 * 60 * 1000)
})

test('runtime heartbeat missing marker is normal first start', () => {
    expect(evaluateRuntimeHeartbeat(null, 1_800_000_000_000)).toEqual({
        lastSeenAt: null,
        downtimeMs: null,
        softStart: false,
        warmupUntilMs: null,
    })
})

test('runtime heartbeat persists atomically', () => {
    const file = tempFile()
    writeHeartbeat(file, 123456)
    expect(readHeartbeat(file)).toBe(123456)
})

test('runtime heartbeat job exposes soft-start state and writes marker', () => {
    const file = tempFile()
    writeHeartbeat(file, 1_000)
    const job = startRuntimeHeartbeatJob({ filePath: file, now: 2_000_000, thresholdMs: 60_000, durationMs: 30_000 })
    expect(job.state.softStart).toBe(true)
    expect(readHeartbeat(file)).toBe(2_000_000)
    job.stop()
})
