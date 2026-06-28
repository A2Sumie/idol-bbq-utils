import { afterEach, beforeEach, expect, test } from 'bun:test'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { BrowserSessionPool } from './browser-session-pool'

const originalProfileDir = process.env.BROWSER_PROFILE_DIR
const tmpRoots = new Set<string>()

beforeEach(() => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'browser-pool-test-'))
    tmpRoots.add(dir)
    process.env.BROWSER_PROFILE_DIR = dir
})

afterEach(() => {
    if (originalProfileDir === undefined) {
        delete process.env.BROWSER_PROFILE_DIR
    } else {
        process.env.BROWSER_PROFILE_DIR = originalProfileDir
    }
    for (const dir of tmpRoots) {
        fs.rmSync(dir, { recursive: true, force: true })
    }
    tmpRoots.clear()
})

function makeFakePage() {
    return {
        setUserAgent: async () => {},
        setViewport: async () => {},
        setExtraHTTPHeaders: async () => {},
        emulateTimezone: async () => {},
        setBypassCSP: async () => {},
        evaluateOnNewDocument: async () => {},
        close: async () => {},
    } as any
}

interface FakeBrowserOptions {
    connected?: boolean
    newPage?: () => Promise<any>
}

function makeFakeBrowser(options: FakeBrowserOptions = {}) {
    const listeners = new Map<string, Array<() => void>>()
    const state = {
        connected: options.connected ?? true,
        closeCalls: 0,
    }
    const browser: any = {
        get connected() {
            return state.connected
        },
        newPage: options.newPage ?? (async () => makeFakePage()),
        once(event: string, handler: () => void) {
            const handlers = listeners.get(event) ?? []
            handlers.push(handler)
            listeners.set(event, handlers)
            return browser
        },
        async close() {
            state.closeCalls += 1
            state.connected = false
        },
        // Test helpers
        _state: state,
        _setConnected(value: boolean) {
            state.connected = value
        },
        _emit(event: string) {
            for (const handler of listeners.get(event) ?? []) {
                handler()
            }
        },
    }
    return browser
}

/**
 * Stub the private launchBrowser so no real Chrome is spawned. Returns queued fake browsers in order,
 * falling back to a fresh live fake browser once the queue is drained.
 */
function stubLaunch(pool: BrowserSessionPool, queue: Array<any>) {
    const launched: Array<any> = []
    ;(pool as any).launchBrowser = async () => {
        const browser = queue.shift() ?? makeFakeBrowser()
        launched.push(browser)
        return browser
    }
    return launched
}

test('BrowserSessionPool reuses a live pooled browser across createPage calls', async () => {
    const pool = new BrowserSessionPool(path.join(os.tmpdir(), 'unused'))
    const browser = makeFakeBrowser({ connected: true })
    const launched = stubLaunch(pool, [browser])

    await pool.createPage({ session_profile: 'x-main', browser_mode: 'headed-xvfb' })
    await pool.createPage({ session_profile: 'x-main', browser_mode: 'headed-xvfb' })

    expect(launched).toHaveLength(1)
    expect(browser._state.closeCalls).toBe(0)
})

test('BrowserSessionPool relaunches when the cached browser is no longer connected', async () => {
    const pool = new BrowserSessionPool(path.join(os.tmpdir(), 'unused'))
    const dead = makeFakeBrowser({ connected: true })
    const fresh = makeFakeBrowser({ connected: true })
    const launched = stubLaunch(pool, [dead, fresh])

    await pool.createPage({ session_profile: 'x-main', browser_mode: 'headed-xvfb' })
    // Simulate the pooled Chrome dying between crawls (crash / OOM-kill).
    dead._setConnected(false)
    const page = await pool.createPage({ session_profile: 'x-main', browser_mode: 'headed-xvfb' })

    expect(page).toBeDefined()
    expect(launched).toHaveLength(2)
    expect(dead._state.closeCalls).toBeGreaterThanOrEqual(1)
})

test('BrowserSessionPool recovers when newPage fails with a closed connection', async () => {
    const pool = new BrowserSessionPool(path.join(os.tmpdir(), 'unused'))
    const broken = makeFakeBrowser({
        connected: true,
        newPage: async () => {
            throw new Error('Protocol error: Connection closed.')
        },
    })
    const healthy = makeFakeBrowser({ connected: true })
    const launched = stubLaunch(pool, [broken, healthy])

    const page = await pool.createPage({ session_profile: 'x-main', browser_mode: 'headed-xvfb' })

    expect(page).toBeDefined()
    expect(launched).toHaveLength(2)
    expect(broken._state.closeCalls).toBeGreaterThanOrEqual(1)
})

test('BrowserSessionPool surfaces the error when every relaunch keeps failing to open a page', async () => {
    const pool = new BrowserSessionPool(path.join(os.tmpdir(), 'unused'))
    const makeBroken = () =>
        makeFakeBrowser({
            connected: true,
            newPage: async () => {
                throw new Error('Protocol error: Connection closed.')
            },
        })
    stubLaunch(pool, [makeBroken(), makeBroken(), makeBroken()])

    await expect(
        pool.createPage({ session_profile: 'x-main', browser_mode: 'headed-xvfb' }),
    ).rejects.toThrow('Connection closed')
})

test('BrowserSessionPool evicts a session when the browser disconnects', async () => {
    const pool = new BrowserSessionPool(path.join(os.tmpdir(), 'unused'))
    const first = makeFakeBrowser({ connected: true })
    const second = makeFakeBrowser({ connected: true })
    const launched = stubLaunch(pool, [first, second])

    await pool.createPage({ session_profile: 'x-main', browser_mode: 'headed-xvfb' })
    expect((pool as any).sessions.size).toBe(1)

    // The browser process exits asynchronously; the pool must drop the dead entry.
    first._emit('disconnected')
    expect((pool as any).sessions.size).toBe(0)

    await pool.createPage({ session_profile: 'x-main', browser_mode: 'headed-xvfb' })
    expect(launched).toHaveLength(2)
})

test('BrowserSessionPool keeps the browser pooled but closes the page when profile setup fails', async () => {
    const pool = new BrowserSessionPool(path.join(os.tmpdir(), 'unused'))
    let pageClosed = 0
    const browser = makeFakeBrowser({
        connected: true,
        newPage: async () => {
            const page = makeFakePage()
            page.setUserAgent = async () => {
                throw new Error('profile boom')
            }
            page.close = async () => {
                pageClosed += 1
            }
            return page
        },
    })
    const launched = stubLaunch(pool, [browser])

    await expect(
        pool.createPage({ session_profile: 'x-main', browser_mode: 'headed-xvfb' }),
    ).rejects.toThrow('profile boom')

    expect(pageClosed).toBe(1)
    // A profile-apply failure does not mean the browser is dead, so it stays pooled.
    expect(launched).toHaveLength(1)
    expect((pool as any).sessions.size).toBe(1)
    expect(browser._state.closeCalls).toBe(0)
})
