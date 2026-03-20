#!/usr/bin/env bun

import fs from 'fs'
import path from 'path'
import puppeteer from 'puppeteer-core'

type BrowserMode = 'headless' | 'headed' | 'headed-xvfb'

interface Args {
    sessionProfile: string
    output: string
    url: string
    browserMode: BrowserMode
    userAgent?: string
    locale?: string
    timezone?: string
}

function parseArgs(argv: string[]): Args {
    const options = new Map<string, string>()

    for (let i = 0; i < argv.length; i += 1) {
        const key = argv[i]
        if (!key.startsWith('--')) {
            throw new Error(`Unexpected argument: ${key}`)
        }
        const value = argv[i + 1]
        if (!value || value.startsWith('--')) {
            throw new Error(`Missing value for ${key}`)
        }
        options.set(key.slice(2), value)
        i += 1
    }

    const sessionProfile = String(options.get('session-profile') || '').trim()
    const output = String(options.get('output') || '').trim()
    const url = String(options.get('url') || 'https://www.bilibili.com').trim()
    const rawBrowserMode = String(options.get('browser-mode') || 'headless').trim() as BrowserMode

    if (!sessionProfile) {
        throw new Error('--session-profile is required')
    }
    if (!output) {
        throw new Error('--output is required')
    }
    if (!['headless', 'headed', 'headed-xvfb'].includes(rawBrowserMode)) {
        throw new Error(`Unsupported browser mode: ${rawBrowserMode}`)
    }

    return {
        sessionProfile,
        output,
        url,
        browserMode: rawBrowserMode,
        userAgent: options.get('user-agent') || undefined,
        locale: options.get('locale') || undefined,
        timezone: options.get('timezone') || undefined,
    }
}

function sanitizeSessionId(value: string) {
    return value.replace(/[^a-zA-Z0-9._-]+/g, '-').replace(/^-+|-+$/g, '') || 'default'
}

function ensureDirectoryExists(dirPath: string) {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true })
    }
}

function getBrowserProfileRoot() {
    return process.env.BROWSER_PROFILE_DIR || path.join(process.cwd(), 'assets', 'cookies', 'browser-profiles')
}

function getExecutablePath() {
    return process.env.PUPPETEER_EXECUTABLE_PATH || undefined
}

function filterBilibiliCookies(
    cookies: Array<{
        name: string
        value: string
        domain?: string
        path?: string
        expires?: number
        secure?: boolean
        httpOnly?: boolean
    }>,
) {
    return cookies.filter((cookie) => {
        const domain = String(cookie.domain || '').replace(/^\./, '').toLowerCase()
        return (
            domain.endsWith('bilibili.com')
            || domain.endsWith('hdslb.com')
            || domain.endsWith('bilivideo.com')
        )
    })
}

function buildCookieDocument(
    cookies: Array<{
        name: string
        value: string
        domain?: string
        path?: string
        expires?: number
        secure?: boolean
        httpOnly?: boolean
    }>,
) {
    return {
        cookie_info: {
            cookies: cookies.map((cookie) => ({
                name: cookie.name,
                value: cookie.value,
                domain: cookie.domain || '.bilibili.com',
                path: cookie.path || '/',
                expires: typeof cookie.expires === 'number' ? cookie.expires : 0,
                http_only: cookie.httpOnly ? 1 : 0,
                secure: cookie.secure ? 1 : 0,
            })),
        },
        sso: [],
        token_info: {
            access_token: '',
            expires_in: 0,
            mid: 0,
            refresh_token: '',
        },
        platform: null,
    }
}

function assertUsableLogin(cookies: Array<{ name: string }>) {
    const names = new Set(cookies.map((cookie) => cookie.name))
    const missing = ['SESSDATA', 'bili_jct'].filter((name) => !names.has(name))
    if (missing.length > 0) {
        throw new Error(`Browser session does not have required Bilibili cookies: ${missing.join(', ')}`)
    }
}

async function main() {
    const args = parseArgs(process.argv.slice(2))
    const browserProfileRoot = getBrowserProfileRoot()
    const userDataDir = path.join(browserProfileRoot, sanitizeSessionId(args.sessionProfile))
    ensureDirectoryExists(userDataDir)
    ensureDirectoryExists(path.dirname(args.output))

    const lang = args.locale || process.env.BROWSER_LANG || 'ja-JP'
    const browser = await puppeteer.launch({
        headless: args.browserMode === 'headless',
        handleSIGINT: false,
        handleSIGHUP: false,
        handleSIGTERM: false,
        defaultViewport: null,
        ignoreDefaultArgs: ['--enable-automation'],
        userDataDir,
        args: [
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
            '--window-size=1440,1024',
            `--lang=${lang}`,
        ].filter(Boolean),
        ...(getExecutablePath() ? { executablePath: getExecutablePath() } : { channel: 'chrome' as const }),
    })

    try {
        const page = await browser.newPage()
        if (args.userAgent) {
            await page.setUserAgent(args.userAgent)
        }
        if (args.timezone) {
            await page.emulateTimezone(args.timezone)
        }

        await page.goto(args.url, {
            waitUntil: 'domcontentloaded',
            timeout: 20000,
        })
        await page.waitForFunction(() => document.readyState === 'interactive' || document.readyState === 'complete', {
            timeout: 5000,
        }).catch(() => null)

        const cookies = filterBilibiliCookies(
            (await page.browserContext().cookies()).map((cookie) => ({
                name: cookie.name,
                value: cookie.value,
                domain: cookie.domain,
                path: cookie.path,
                expires: cookie.expires,
                secure: cookie.secure,
                httpOnly: cookie.httpOnly,
            })),
        )

        assertUsableLogin(cookies)

        const document = buildCookieDocument(cookies)
        fs.writeFileSync(args.output, JSON.stringify(document, null, 2), 'utf8')

        process.stdout.write(
            `${JSON.stringify(
                {
                    ok: true,
                    sessionProfile: args.sessionProfile,
                    output: args.output,
                    cookieCount: cookies.length,
                    hostname: new URL(args.url).hostname,
                    generatedAt: new Date().toISOString(),
                },
                null,
                2,
            )}\n`,
        )
    } finally {
        await browser.close().catch(() => null)
    }
}

main().catch((error) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exit(1)
})
