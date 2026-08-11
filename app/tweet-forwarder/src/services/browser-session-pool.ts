import path from 'path'
import puppeteer, { Browser, Page } from 'puppeteer-core'
import { Logger } from '@idol-bbq-utils/log'
import {
    applyBrowserProfile,
    resolveBrowserProfile,
    type BrowserMode,
    type BrowserProfileConfig,
    type DeviceProfile,
    type ProfileViewport,
} from '@idol-bbq-utils/spider'
import { ensureDirectoryExists, getBrowserProfileRoot } from '@/utils/directories'

interface BrowserPageRequest {
    browser_mode?: BrowserMode
    device_profile?: DeviceProfile
    session_profile?: string
    extra_headers?: Record<string, string>
    viewport?: Partial<ProfileViewport>
    user_agent?: string
    locale?: string
    timezone?: string
}

interface BrowserRuntimeSession {
    browser: Browser
    mode: BrowserMode
    sessionId: string
    userDataDir: string
}

/**
 * How many times {@link BrowserSessionPool.createPage} relaunches a browser when opening a page
 * fails because the pooled browser is dead (e.g. Chrome crashed or was OOM-killed). One relaunch is
 * enough to recover from a stale handle without risking an infinite crash loop.
 */
const CREATE_PAGE_MAX_ATTEMPTS = 2
const BROWSER_CLOSE_TIMEOUT_MS = 5_000

function sanitizeSessionId(value?: string) {
    return (value || 'default').replace(/[^a-zA-Z0-9._-]+/g, '-').replace(/^-+|-+$/g, '') || 'default'
}

export class BrowserSessionPool {
    private readonly sessions = new Map<string, BrowserRuntimeSession>()
    // In-flight launches keyed by sessionKey. Concurrent createPage() calls for the
    // same profile (common at cold start when several crawlers share e.g. x-main)
    // must await one launch instead of each racing to spawn a browser and orphaning
    // the losers of the Map.set() race — that leak stacked dozens of Chrome instances
    // per profile and drove the host into OOM.
    private readonly pendingLaunches = new Map<string, Promise<BrowserRuntimeSession>>()
    private readonly browserRoot: string
    private readonly log?: Logger
    private closing = false

    constructor(cacheRoot: string, log?: Logger) {
        this.browserRoot = getBrowserProfileRoot(cacheRoot)
        this.log = log?.child({ subservice: 'BrowserSessionPool' })
        ensureDirectoryExists(this.browserRoot)
    }

    async createPage(request: BrowserPageRequest = {}): Promise<Page> {
        if (this.closing) {
            throw new Error('Browser session pool is closing')
        }
        const resolvedProfile = resolveBrowserProfile(request.device_profile, {
            extraHeaders: request.extra_headers,
            locale: request.locale,
            timezone: request.timezone,
            userAgent: request.user_agent,
            viewport: request.viewport,
        })
        const defaultBrowserMode: BrowserMode =
            process.env.DISPLAY || process.env.ENABLE_XVFB === '1' ? 'headed-xvfb' : 'headless'
        const browserMode =
            request.browser_mode || ((process.env.BROWSER_MODE as BrowserMode | undefined) ?? defaultBrowserMode)
        const sessionId = sanitizeSessionId(request.session_profile || request.device_profile || 'default')
        const sessionKey = `${sessionId}:${browserMode}`

        let lastError: unknown
        for (let attempt = 1; attempt <= CREATE_PAGE_MAX_ATTEMPTS; attempt += 1) {
            const session = await this.getOrCreateSession(sessionKey, sessionId, browserMode, resolvedProfile)
            let page: Page
            try {
                page = await session.browser.newPage()
            } catch (error) {
                lastError = error
                if (this.isSessionAlive(session) && !this.isBrowserConnectionError(error)) {
                    throw error
                }
                await this.evictSession(sessionKey, session)
                this.log?.warn(
                    `Browser session ${sessionId} (${browserMode}) could not open a page on attempt ${attempt}/${CREATE_PAGE_MAX_ATTEMPTS}; recreating: ${error}`,
                )
                continue
            }

            try {
                await applyBrowserProfile(page, resolvedProfile.deviceProfile, {
                    userAgent: resolvedProfile.userAgent,
                    viewport: resolvedProfile.viewport,
                    extraHeaders: resolvedProfile.extraHeaders,
                    locale: resolvedProfile.locale,
                    timezone: resolvedProfile.timezone,
                })
            } catch (error) {
                // The browser opened a page but profile setup failed. The browser itself is still
                // usable, so close just this page and surface the error rather than evicting the pool.
                await page.close().catch(() => null)
                throw error
            }
            return page
        }

        throw lastError instanceof Error ? lastError : new Error(`Failed to create browser page: ${String(lastError)}`)
    }

    async closeAll() {
        this.closing = true
        await Promise.allSettled(Array.from(this.pendingLaunches.values()))
        await Promise.all(
            Array.from(this.sessions.values()).map(async (session) => {
                await this.closeBrowser(session, `closeAll:${session.sessionId}`)
            }),
        )
        this.sessions.clear()
    }

    private async getOrCreateSession(
        sessionKey: string,
        sessionId: string,
        browserMode: BrowserMode,
        profile: BrowserProfileConfig,
    ) {
        const existing = this.sessions.get(sessionKey)
        if (existing) {
            if (this.isSessionAlive(existing)) {
                return existing
            }
            // A previously cached browser is no longer connected; drop it before relaunching so we
            // never hand back a dead handle that would fail on the next newPage() call.
            this.log?.warn(`Browser session ${sessionId} (${browserMode}) is no longer connected; recreating`)
            await this.evictSession(sessionKey, existing)
        }

        // Another caller may already be launching this exact profile. Await it; do not
        // race a second Chrome against the same userDataDir and leak the first process.
        const inFlight = this.pendingLaunches.get(sessionKey)
        if (inFlight) {
            return await inFlight
        }
        const replacement = this.sessions.get(sessionKey)
        if (replacement && this.isSessionAlive(replacement)) {
            return replacement
        }

        const launchPromise = this.launchSession(sessionKey, sessionId, browserMode, profile).finally(() => {
            if (this.pendingLaunches.get(sessionKey) === launchPromise) {
                this.pendingLaunches.delete(sessionKey)
            }
        })
        this.pendingLaunches.set(sessionKey, launchPromise)
        return await launchPromise
    }

    private async launchSession(
        sessionKey: string,
        sessionId: string,
        browserMode: BrowserMode,
        profile: BrowserProfileConfig,
    ): Promise<BrowserRuntimeSession> {
        const userDataDir = path.join(this.browserRoot, `${sessionId}-${browserMode}`)
        ensureDirectoryExists(userDataDir)
        const browser = await this.launchBrowser(browserMode, userDataDir, profile)
        const runtimeSession: BrowserRuntimeSession = {
            browser,
            mode: browserMode,
            sessionId,
            userDataDir,
        }
        // Self-heal the pool: if this browser crashes or disconnects later, remove it from the cache
        // so the next request transparently relaunches a fresh browser. The identity check avoids
        // evicting a replacement session that may already have taken this key.
        browser.once('disconnected', () => {
            if (this.sessions.get(sessionKey) === runtimeSession) {
                this.sessions.delete(sessionKey)
                this.log?.warn(`Browser session ${sessionId} (${browserMode}) disconnected; evicted from pool`)
                void this.closeBrowser(runtimeSession, `disconnect:${sessionId}`)
            }
        })
        this.sessions.set(sessionKey, runtimeSession)
        this.log?.info(`Browser session ready: ${sessionId} (${browserMode})`)
        return runtimeSession
    }

    private isSessionAlive(session: BrowserRuntimeSession): boolean {
        try {
            return session.browser.connected
        } catch {
            return false
        }
    }

    private isBrowserConnectionError(error: unknown) {
        return /connection (?:closed|lost)|target closed|browser has disconnected|session closed|protocol error/i.test(
            error instanceof Error ? error.message : String(error),
        )
    }

    private async evictSession(sessionKey: string, session: BrowserRuntimeSession) {
        if (this.sessions.get(sessionKey) === session) {
            this.sessions.delete(sessionKey)
        }
        await this.closeBrowser(session, `evict:${session.sessionId}`)
    }

    private async closeBrowser(session: BrowserRuntimeSession, reason: string) {
        const process = session.browser.process?.()
        let timer: ReturnType<typeof setTimeout> | undefined
        try {
            await Promise.race([
                session.browser.close(),
                new Promise<void>((_, reject) => {
                    timer = setTimeout(() => reject(new Error(`browser close timed out after ${BROWSER_CLOSE_TIMEOUT_MS}ms`)), BROWSER_CLOSE_TIMEOUT_MS)
                }),
            ])
        } catch (error) {
            this.log?.warn(`Failed to close browser session ${session.sessionId} (${reason}): ${error}`)
            // CDP may already be disconnected while the OS process remains alive.
            // Kill only this pooled browser's root process; its zygote children follow.
            try {
                process?.kill('SIGKILL')
            } catch (killError) {
                this.log?.warn(`Failed to SIGKILL browser session ${session.sessionId}: ${killError}`)
            }
        } finally {
            if (timer) clearTimeout(timer)
        }
    }

    private async launchBrowser(
        browserMode: BrowserMode,
        userDataDir: string,
        profile: BrowserProfileConfig,
    ) {
        const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH
        const lang = process.env.BROWSER_LANG || 'ja-JP'
        const extraArgs = (process.env.BROWSER_EXTRA_ARGS || '')
            .split(/\s+/)
            .map((arg) => arg.trim())
            .filter(Boolean)
        const args = [
            process.env.NO_SANDBOX ? '--no-sandbox' : '',
            process.env.NO_SANDBOX ? '--disable-setuid-sandbox' : '',
            '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled',
            '--disable-features=Translate,BackForwardCache,AcceptCHFrame,MediaRouter',
            '--disable-popup-blocking',
            '--disable-renderer-backgrounding',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-infobars',
            '--window-position=0,0',
            `--window-size=${profile.windowSize.width},${profile.windowSize.height}`,
            `--lang=${lang}`,
            ...extraArgs,
        ].filter(Boolean)

        return puppeteer.launch({
            headless: browserMode === 'headless',
            handleSIGINT: false,
            handleSIGHUP: false,
            handleSIGTERM: false,
            args,
            defaultViewport: null,
            ignoreDefaultArgs: ['--enable-automation'],
            userDataDir,
            ...(executablePath ? { executablePath } : { channel: 'chrome' as const }),
        })
    }
}

export type { BrowserMode, BrowserPageRequest, DeviceProfile, ProfileViewport }
