import { test, expect } from 'bun:test'
import { HostPacer } from '../src/utils/host-pacer'

function makeClock(start = 1_000_000) {
    let now = start
    const sleeps: Array<number> = []
    return {
        now: () => now,
        advance: (ms: number) => {
            now += ms
        },
        sleep: async (ms: number) => {
            sleeps.push(ms)
            now += ms
        },
        sleeps,
    }
}

test('HostPacer: first request on a host is allowed immediately', async () => {
    const clock = makeClock()
    const pacer = new HostPacer(8_000, clock.now, clock.sleep)
    await pacer.waitTurn('www.tiktok.com')
    expect(clock.sleeps).toEqual([])
})

test('HostPacer: second request inside the interval sleeps for the remainder', async () => {
    const clock = makeClock()
    const pacer = new HostPacer(8_000, clock.now, clock.sleep)
    await pacer.waitTurn('www.tiktok.com')
    clock.advance(3_000)
    await pacer.waitTurn('www.tiktok.com')
    expect(clock.sleeps).toEqual([5_000])
})

test('HostPacer: requests past the interval are not delayed', async () => {
    const clock = makeClock()
    const pacer = new HostPacer(8_000, clock.now, clock.sleep)
    await pacer.waitTurn('www.tiktok.com')
    clock.advance(9_000)
    await pacer.waitTurn('www.tiktok.com')
    expect(clock.sleeps).toEqual([])
})

test('HostPacer: budgets are per host', async () => {
    const clock = makeClock()
    const pacer = new HostPacer(8_000, clock.now, clock.sleep)
    await pacer.waitTurn('www.tiktok.com')
    await pacer.waitTurn('www.showroom-live.com')
    expect(clock.sleeps).toEqual([])
})

test('HostPacer: concurrent callers on one host serialize onto the interval', async () => {
    const clock = makeClock()
    const pacer = new HostPacer(8_000, clock.now, clock.sleep)
    await Promise.all([pacer.waitTurn('www.tiktok.com'), pacer.waitTurn('www.tiktok.com'), pacer.waitTurn('www.tiktok.com')])
    // first immediate, then two serialized 8s waits
    expect(clock.sleeps).toEqual([8_000, 8_000])
})

test('HostPacer: a faulted turn does not poison the queue', async () => {
    const clock = makeClock()
    let failOnce = true
    const pacer = new HostPacer(8_000, clock.now, async (ms) => {
        if (failOnce) {
            failOnce = false
            throw new Error('boom')
        }
        clock.advance(ms)
    })
    await pacer.waitTurn('www.tiktok.com')
    await pacer.waitTurn('www.tiktok.com').catch(() => {})
    await pacer.waitTurn('www.tiktok.com')
    // the third call still gets its turn after the faulted second one
    expect(clock.now()).toBeGreaterThan(1_000_000)
})
