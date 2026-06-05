import type { CookieData } from 'puppeteer-core'
import fs from 'fs'

type NetscapeCookieParseOptions = {
    includeExpired?: boolean
    now?: number
}

function splitNetscapeCookieLine(line: string) {
    if (line.includes('\t')) {
        const fields = line.split('\t')
        return {
            fields: fields.slice(0, 6).concat(fields.slice(6).join('\t')),
        }
    }

    const fields = line.trim().split(/[ ]+/)
    return {
        fields: fields.slice(0, 6).concat(fields.slice(6).join(' ')),
    }
}

function isExpiredCookie(expires: number, now: number) {
    return Number.isFinite(expires) && expires > 0 && expires <= now
}

/**
 * @description convert netscape cookie file to puppeteer cookie like https://chromewebstore.google.com/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc?hl=en
 * @param cookie_file path to cookie file
 * @returns CookieParam[]
 */
function parseNetscapeCookieToPuppeteerCookie(
    cookie_file: string,
    options: NetscapeCookieParseOptions = {},
): Array<CookieData> {
    const now = options.now ?? Math.floor(Date.now() / 1000)
    const lines = fs.readFileSync(cookie_file, 'utf8').split('\n')
    const cookies = []
    for (let line of lines) {
        line = line.trimEnd()
        if (!line.trim()) {
            continue
        }

        //  ref: https://github.com/Moustachauve/cookie-editor
        const trimmedLine = line.trimStart()
        const httpOnly = trimmedLine.startsWith('#HttpOnly_')
        if (httpOnly) {
            line = trimmedLine.replace('#HttpOnly_', '')
        }
        if (line.trimStart().startsWith('#')) {
            continue
        }

        const { fields } = splitNetscapeCookieLine(line)
        const [domain = '', _includeSubdomain, path, secure, expiresRaw, name = '', value = ''] = fields
        if (!domain || !path || !name || !expiresRaw) {
            continue
        }

        const expires = Number(expiresRaw)
        if (!options.includeExpired && isExpiredCookie(expires, now)) {
            continue
        }

        cookies.push({
            name,
            value: value.trim(),
            domain,
            path,
            expires,
            httpOnly,
            secure: secure === 'TRUE',
        })
    }
    return cookies
}

function getCookieString(cookies: Array<CookieData>): string {
    return cookies
        .map((cookie) => {
            return `${cookie.name}=${cookie.value}`.trim()
        })
        .join(';')
}

class SimpleExpiringCache {
    cache: Map<string, any>
    constructor() {
        this.cache = new Map()
    }

    /**
     * @param ttl seconds
     */
    set(key: string, value: any, ttl: number) {
        const ttlMs = Math.max(0, ttl * 1000)
        const expiresAt = Date.now() + ttlMs
        this.cache.set(key, { value, expiresAt })

        if (ttlMs > 0) {
            setTimeout(() => {
                if (this.cache.get(key)?.expiresAt <= Date.now()) {
                    this.cache.delete(key)
                }
            }, Math.min(ttlMs, 2_147_483_647))
        }
    }

    get(key: string) {
        const item = this.cache.get(key)
        if (!item) return null

        if (Date.now() > item.expiresAt) {
            this.cache.delete(key)
            return null
        }

        return item.value
    }
}

export { parseNetscapeCookieToPuppeteerCookie, getCookieString, SimpleExpiringCache }
export * from './http'
export * from './browser'
