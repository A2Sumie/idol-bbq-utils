import { expect, test } from 'bun:test'
import { EventDedupeCache, HandlerRateLimiter } from './onebot-event-dedupe'

function fakeClock(start = 1_000_000) {
    let now = start
    return {
        now: () => now,
        advance: (ms: number) => {
            now += ms
        },
    }
}

test('EventDedupeCache marks first delivery and rejects redelivery inside the TTL', () => {
    const clock = fakeClock()
    const cache = new EventDedupeCache({ ttlMs: 60_000, maxEntries: 128, now: clock.now })
    expect(cache.mark('msg:1:100')).toBe(true)
    expect(cache.mark('msg:1:100')).toBe(false)
    expect(cache.isDuplicate('msg:1:100')).toBe(true)
    expect(cache.mark('msg:1:101')).toBe(true)

    clock.advance(61_000)
    expect(cache.isDuplicate('msg:1:100')).toBe(false)
    // Same key is accepted again after the dedupe window (legit re-delivery
    // after a bridge outage is a new reaction opportunity).
    expect(cache.mark('msg:1:100')).toBe(true)
})

test('EventDedupeCache evicts beyond maxEntries instead of growing forever', () => {
    const clock = fakeClock()
    const cache = new EventDedupeCache({ ttlMs: 600_000, maxEntries: 16, now: clock.now })
    for (let i = 0; i < 100; i += 1) {
        cache.mark(`key-${i}`)
    }
    expect(cache.size()).toBeLessThanOrEqual(16)
    expect(cache.isDuplicate('key-0')).toBe(false)
    expect(cache.isDuplicate('key-99')).toBe(true)
})

test('EventDedupeCache survives concurrent-looking duplicate bursts', () => {
    const clock = fakeClock()
    const cache = new EventDedupeCache({ ttlMs: 60_000, maxEntries: 16, now: clock.now })
    const results = [cache.mark('k'), cache.mark('k'), cache.mark('k')]
    expect(results).toEqual([true, false, false])
})

test('HandlerRateLimiter enforces per-scope cooldown', () => {
    const clock = fakeClock()
    const limiter = new HandlerRateLimiter(clock.now)
    const rule = { cooldownMs: 30_000 }
    expect(limiter.consume('h1:g1', rule).allowed).toBe(true)
    const blocked = limiter.consume('h1:g1', rule)
    expect(blocked.allowed).toBe(false)
    expect(blocked.reason).toBe('cooldown')
    // A different scope is independent.
    expect(limiter.consume('h1:g2', rule).allowed).toBe(true)
    clock.advance(31_000)
    expect(limiter.consume('h1:g1', rule).allowed).toBe(true)
})

test('HandlerRateLimiter enforces hourly quota per handler+scope', () => {
    const clock = fakeClock()
    const limiter = new HandlerRateLimiter(clock.now)
    const rule = { maxPerWindow: 3, windowMs: 3_600_000 }
    expect(limiter.consume('h1:g1', rule).allowed).toBe(true)
    expect(limiter.consume('h1:g1', rule).allowed).toBe(true)
    expect(limiter.consume('h1:g1', rule).allowed).toBe(true)
    const over = limiter.consume('h1:g1', rule)
    expect(over.allowed).toBe(false)
    expect(over.reason).toBe('quota')
    clock.advance(3_600_001)
    expect(limiter.consume('h1:g1', rule).allowed).toBe(true)
})

test('HandlerRateLimiter cooldown and quota compose', () => {
    const clock = fakeClock()
    const limiter = new HandlerRateLimiter(clock.now)
    const rule = { cooldownMs: 1_000, maxPerWindow: 2, windowMs: 3_600_000 }
    expect(limiter.consume('h:g', rule).allowed).toBe(true)
    clock.advance(1_500)
    expect(limiter.consume('h:g', rule).allowed).toBe(true)
    clock.advance(1_500)
    const limited = limiter.consume('h:g', rule)
    expect(limited.allowed).toBe(false)
    expect(limited.reason).toBe('quota')
})
