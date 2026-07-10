import { expect, test, beforeEach, afterEach } from 'bun:test'
import fs from 'fs'
import path from 'path'
import os from 'os'
import { Hy3CircuitBreaker, resetHy3CircuitBreakerForTest } from '@/services/hy3-circuit-breaker-service'

let statePath: string
let origThreshold: string | undefined
let origStatePath: string | undefined
let origFreezeDuration: string | undefined

beforeEach(() => {
    statePath = path.join(os.tmpdir(), `hy3-breaker-test-${Date.now()}-${Math.random().toString(36).slice(2)}.json`)
    origThreshold = process.env.HY3_FAILURE_THRESHOLD
    origStatePath = process.env.HY3_BREAKER_STATE_PATH
    origFreezeDuration = process.env.HY3_FREEZE_DURATION_MS
    process.env.HY3_FAILURE_THRESHOLD = '3'
    process.env.HY3_BREAKER_STATE_PATH = statePath
    process.env.HY3_FREEZE_DURATION_MS = ''
    resetHy3CircuitBreakerForTest()
})

afterEach(() => {
    if (origThreshold === undefined) delete process.env.HY3_FAILURE_THRESHOLD
    else process.env.HY3_FAILURE_THRESHOLD = origThreshold
    if (origStatePath === undefined) delete process.env.HY3_BREAKER_STATE_PATH
    else process.env.HY3_BREAKER_STATE_PATH = origStatePath
    if (origFreezeDuration === undefined) delete process.env.HY3_FREEZE_DURATION_MS
    else process.env.HY3_FREEZE_DURATION_MS = origFreezeDuration
    try {
        if (fs.existsSync(statePath)) fs.unlinkSync(statePath)
    } catch {
        // ignore
    }
    resetHy3CircuitBreakerForTest()
})

test('does not freeze below threshold', () => {
    const breaker = new Hy3CircuitBreaker()
    expect(breaker.isFrozen()).toBe(false)
    breaker.recordFailure(new Error('boom-1'))
    breaker.recordFailure(new Error('boom-2'))
    expect(breaker.isFrozen()).toBe(false)
    expect(breaker.getStatus().consecutiveFailures).toBe(2)
})

test('freezes at threshold', () => {
    const breaker = new Hy3CircuitBreaker()
    breaker.recordFailure(new Error('boom-1'))
    breaker.recordFailure(new Error('boom-2'))
    breaker.recordFailure(new Error('boom-3'))
    expect(breaker.isFrozen()).toBe(true)
    const status = breaker.getDetailedStatus()
    expect(status.frozen).toBe(true)
    expect(status.consecutiveFailures).toBe(3)
    expect(status.frozenAt).toBeTruthy()
    expect(status.lastError).toBe('boom-3')
    expect(status.threshold).toBe(3)
    expect(status.totalFailures).toBe(3)
})

test('recordSuccess resets consecutive failures and unfreezes', () => {
    const breaker = new Hy3CircuitBreaker()
    breaker.recordFailure(new Error('boom-1'))
    breaker.recordFailure(new Error('boom-2'))
    breaker.recordFailure(new Error('boom-3'))
    expect(breaker.isFrozen()).toBe(true)
    breaker.recordSuccess()
    expect(breaker.isFrozen()).toBe(false)
    expect(breaker.getStatus().consecutiveFailures).toBe(0)
    expect(breaker.getStatus().totalSuccesses).toBe(1)
})

test('manual unfreeze clears frozen state', () => {
    const breaker = new Hy3CircuitBreaker()
    for (let i = 0; i < 3; i++) breaker.recordFailure(new Error(`boom-${i}`))
    expect(breaker.isFrozen()).toBe(true)
    breaker.unfreeze()
    expect(breaker.isFrozen()).toBe(false)
    expect(breaker.getStatus().consecutiveFailures).toBe(0)
})

test('persists state across instances', () => {
    const breaker1 = new Hy3CircuitBreaker()
    for (let i = 0; i < 3; i++) breaker1.recordFailure(new Error(`boom-${i}`))
    expect(breaker1.isFrozen()).toBe(true)

    resetHy3CircuitBreakerForTest()
    const breaker2 = new Hy3CircuitBreaker()
    expect(breaker2.isFrozen()).toBe(true)
    expect(breaker2.getStatus().consecutiveFailures).toBe(3)
    expect(breaker2.getStatus().totalFailures).toBe(3)
})

test('auto-unfreezes after freeze duration elapses', () => {
    process.env.HY3_FREEZE_DURATION_MS = '50'
    resetHy3CircuitBreakerForTest()
    const breaker = new Hy3CircuitBreaker()
    for (let i = 0; i < 3; i++) breaker.recordFailure(new Error(`boom-${i}`))
    expect(breaker.isFrozen()).toBe(true)
    // wait for freeze duration to pass
    const start = Date.now()
    while (Date.now() - start < 80) {
        // busy wait
    }
    expect(breaker.isFrozen()).toBe(false)
})

test('recordFallback increments counter', () => {
    const breaker = new Hy3CircuitBreaker()
    breaker.recordFallback()
    breaker.recordFallback()
    expect(breaker.getStatus().totalFallbacks).toBe(2)
})

test('default threshold is 10 when env not set', () => {
    delete process.env.HY3_FAILURE_THRESHOLD
    resetHy3CircuitBreakerForTest()
    const breaker = new Hy3CircuitBreaker()
    for (let i = 0; i < 9; i++) breaker.recordFailure(new Error(`boom-${i}`))
    expect(breaker.isFrozen()).toBe(false)
    breaker.recordFailure(new Error('boom-10'))
    expect(breaker.isFrozen()).toBe(true)
    expect(breaker.getDetailedStatus().threshold).toBe(10)
})
