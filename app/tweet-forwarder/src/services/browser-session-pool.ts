import path from 'path'
import puppeteer, { Browser, Page } from 'puppeteer-core'
import { Logger } from '@idol-bbq-utils/log'
import {
    applyBrowserProfile,
    resolveBrowserProfile,
    type BrowserMode,
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
        const browserMode = request.browser_mode || ((process.env.BROWSER_MODE as BrowserMode | undefined) ?? 'headless')
        const sessionId = sanitizeSessionId(request.session_profile || request.device_profile || 'default')
        const sessionKey = `${sessionId}:${browserMode}`
        const session = await this.getOrCreateSession(sessionKey, sessionId, browserMode)
        const page = await session.browser.newPage()
        await applyBrowserProfile(page, resolvedProfile.deviceProfile, {
            userAgent: resolvedProfile.userAgent,
            viewport: resolvedProfile.viewport,
            extraHeaders: resolvedProfile.extraHeaders,
            locale: resolvedProfile.locale,
            timezone: resolvedProfile.timezone,
        })
        return page
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

    private async getOrCreateSession(sessionKey: string, sessionId: string, browserMode: BrowserMode) {
        const existing = this.sessions.get(sessionKey)
        if (existing) {
            return existing
        }

        const userDataDir = path.join(this.browserRoot, sessionId)
        ensureDirectoryExists(userDataDir)
        const browser = await this.launchBrowser(browserMode, userDataDir)
        const runtimeSession: BrowserRuntimeSession = {
            browser,
            mode: browserMode,
            sessionId,
            userDataDir,
        }
        this.sessions.set(sessionKey, runtimeSession)
        this.log?.info(`Browser session ready: ${sessionId} (${browserMode})`)
        return runtimeSession
    }

    private async launchBrowser(browserMode: BrowserMode, userDataDir: string) {
        const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH
        const lang = process.env.BROWSER_LANG || 'ja-JP'
        const args = [
            process.env.NO_SANDBOX ? '--no-sandbox' : '',
            '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled',
            '--disable-features=Translate,BackForwardCache',
            '--window-size=430,1200',
            `--lang=${lang}`,
        ].filter(Boolean)

        return puppeteer.launch({
            headless: browserMode === 'headless',
            handleSIGINT: false,
            handleSIGHUP: false,
            handleSIGTERM: false,
            args,
            userDataDir,
            ...(executablePath ? { executablePath } : { channel: 'chrome' as const }),
        })
    }
}

export type { BrowserMode, BrowserPageRequest, DeviceProfile, ProfileViewport }
