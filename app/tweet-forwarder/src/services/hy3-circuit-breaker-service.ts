import fs from 'fs'
import path from 'path'
import { CACHE_DIR_ROOT } from '@/config'
import { ensureDirectoryExists } from '@/utils/directories'
import { Logger } from '@idol-bbq-utils/log'

interface Hy3BreakerState {
    frozen: boolean
    consecutiveFailures: number
    frozenAt: string | null
    lastError: string | null
    totalFailures: number
    totalFallbacks: number
    totalSuccesses: number
}

const DEFAULT_STATE: Hy3BreakerState = {
    frozen: false,
    consecutiveFailures: 0,
    frozenAt: null,
    lastError: null,
    totalFailures: 0,
    totalFallbacks: 0,
    totalSuccesses: 0,
}

function resolveThreshold(): number {
    const raw = Number(process.env.HY3_FAILURE_THRESHOLD)
    return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 10
}

function resolveFreezeDurationMs(): number | null {
    const raw = Number(process.env.HY3_FREEZE_DURATION_MS)
    return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : null
}

function resolveStatePath(): string {
    return process.env.HY3_BREAKER_STATE_PATH || path.join(CACHE_DIR_ROOT, 'hy3-breaker.json')
}

class Hy3CircuitBreaker {
    private state: Hy3BreakerState = { ...DEFAULT_STATE }
    private statePath: string
    private threshold: number
    private freezeDurationMs: number | null
    private loaded = false
    log?: Logger

    constructor(log?: Logger) {
        this.log = log?.child({ label: 'Hy3Breaker', subservice: 'circuit-breaker' })
        this.statePath = resolveStatePath()
        this.threshold = resolveThreshold()
        this.freezeDurationMs = resolveFreezeDurationMs()
    }

    private load(): void {
        if (this.loaded) {
            return
        }
        this.loaded = true
        try {
            if (fs.existsSync(this.statePath)) {
                const raw = fs.readFileSync(this.statePath, 'utf8')
                const parsed = JSON.parse(raw) as Partial<Hy3BreakerState>
                this.state = { ...DEFAULT_STATE, ...parsed }
            }
        } catch (error) {
            this.log?.warn(`Failed to load hy3 breaker state: ${error}`)
        }
    }

    private persist(): void {
        try {
            ensureDirectoryExists(path.dirname(this.statePath))
            fs.writeFileSync(this.statePath, JSON.stringify(this.state, null, 2), 'utf8')
        } catch (error) {
            this.log?.warn(`Failed to persist hy3 breaker state: ${error}`)
        }
    }

    private maybeAutoUnfreeze(): void {
        if (!this.state.frozen || this.freezeDurationMs === null || !this.state.frozenAt) {
            return
        }
        const frozenAtMs = Date.parse(this.state.frozenAt)
        if (!Number.isFinite(frozenAtMs)) {
            return
        }
        if (Date.now() - frozenAtMs >= this.freezeDurationMs) {
            this.log?.info('HY3 UNFROZEN (auto half-open retry window)')
            this.state.frozen = false
            this.state.consecutiveFailures = 0
            this.state.frozenAt = null
            this.persist()
        }
    }

    isFrozen(): boolean {
        this.load()
        this.maybeAutoUnfreeze()
        return this.state.frozen
    }

    recordSuccess(): void {
        this.load()
        if (this.state.consecutiveFailures > 0 || this.state.frozen) {
            this.log?.info('HY3 recovered — consecutive failures reset')
        }
        this.state.consecutiveFailures = 0
        this.state.frozen = false
        this.state.frozenAt = null
        this.state.lastError = null
        this.state.totalSuccesses += 1
        this.persist()
    }

    recordFailure(error: unknown): void {
        this.load()
        const message = error instanceof Error ? error.message : String(error)
        this.state.consecutiveFailures += 1
        this.state.totalFailures += 1
        this.state.lastError = message
        if (!this.state.frozen && this.state.consecutiveFailures >= this.threshold) {
            this.state.frozen = true
            this.state.frozenAt = new Date().toISOString()
            this.log?.warn(
                `HY3 FROZEN after ${this.state.consecutiveFailures} consecutive failures — falling back to v4-pro ` +
                    `(${this.state.totalFailures} total). Unfreeze via /api/agent/hy3/unfreeze`,
            )
        } else {
            this.log?.warn(
                `HY3 failure ${this.state.consecutiveFailures}/${this.threshold}: ${message.slice(0, 200)}`,
            )
        }
        this.persist()
    }

    recordFallback(): void {
        this.load()
        this.state.totalFallbacks += 1
        this.persist()
    }

    unfreeze(): Hy3BreakerState {
        this.load()
        this.state.frozen = false
        this.state.consecutiveFailures = 0
        this.state.frozenAt = null
        this.state.lastError = null
        this.persist()
        this.log?.info('HY3 UNFROZEN (manual)')
        return this.getStatus()
    }

    getStatus(): Hy3BreakerState {
        this.load()
        this.maybeAutoUnfreeze()
        return { ...this.state }
    }

    getDetailedStatus(): Hy3BreakerStatus {
        this.load()
        this.maybeAutoUnfreeze()
        return {
            ...this.state,
            threshold: this.threshold,
            freeze_duration_ms: this.freezeDurationMs,
            state_path: this.statePath,
        }
    }
}

export interface Hy3BreakerStatus extends Hy3BreakerState {
    threshold: number
    freeze_duration_ms: number | null
    state_path: string
}

let singleton: Hy3CircuitBreaker | null = null

function getHy3CircuitBreaker(log?: Logger): Hy3CircuitBreaker {
    if (!singleton) {
        singleton = new Hy3CircuitBreaker(log)
    }
    return singleton
}

function resetHy3CircuitBreakerForTest(): void {
    singleton = null
}

export { Hy3CircuitBreaker, getHy3CircuitBreaker, resetHy3CircuitBreakerForTest }
