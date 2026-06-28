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

function sanitizeSessionId(value?: string) {
    return (value || 'default').replace(/[^a-zA-Z0-9._-]+/g, '-').replace(/^-+|-+$/g, '') || 'default'
}

export class BrowserSessionPool {
    private readonly sessions = new Map<string, BrowserRuntimeSession>()
    private readonly browserRoot: string
    private readonly log?: Logger

    constructor(cacheRoot: string, log?: Logger) {
        this.browserRoot = getBrowserProfileRoot(cacheRoot)
        this.log = log?.child({ subservice: 'BrowserSessionPool' })
        ensureDirectoryExists(this.browserRoot)
    }

    async createPage(request: BrowserPageRequest = {}): Promise<Page> {
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
                // The pooled browser is most likely dead/disconnected (Chrome crashed or was killed).
                // Evict the dead handle so the next attempt relaunches a fresh browser instead of
                // repeatedly failing with "Protocol error: Connection closed." on every reuse.
                lastError = error
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
        await Promise.all(
            Array.from(this.sessions.values()).map(async (session) => {
                try {
                    await session.browser.close()
                } catch (error) {
                    this.log?.warn(`Failed to close browser session ${session.sessionId}: ${error}`)
                }
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

        const userDataDir = path.join(this.browserRoot, sessionId)
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

    private async evictSession(sessionKey: string, session: BrowserRuntimeSession) {
        if (this.sessions.get(sessionKey) === session) {
            this.sessions.delete(sessionKey)
        }
        // Best-effort teardown of the (likely dead) browser to release its resources. Closing a
        // browser whose connection is already gone can reject, so failures are intentionally ignored.
        await session.browser.close().catch(() => null)
    }

    private async launchBrowser(
        browserMode: BrowserMode,
        userDataDir: string,
        profile: BrowserProfileConfig,
    ) {
        const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH
        const lang = process.env.BROWSER_LANG || 'ja-JP'
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
