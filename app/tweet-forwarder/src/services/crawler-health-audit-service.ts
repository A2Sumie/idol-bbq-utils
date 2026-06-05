import fs from 'fs'
import {
    auditNetscapeCookieFile,
    getCookieString,
    parseNetscapeCookieToPuppeteerCookie,
    type NetscapeCookieFileAudit,
} from '@idol-bbq-utils/spider'
import type { CookieData } from 'puppeteer-core'
import type { AppConfig } from '@/types'

type CrawlerHealthPlatform = 'x' | 'instagram' | 'tiktok' | 'youtube' | 'unknown'
type CrawlerHealthStatus = 'ok' | 'warn' | 'fail' | 'skipped'

type CrawlerHealthResult = {
    crawler_id: string
    crawler_name: string
    platform: CrawlerHealthPlatform
    status: CrawlerHealthStatus
    diagnostic_codes: Array<string>
    static_cookie: {
        exists: boolean
        usable_cookie_count: number
        expired_cookie_count: number
        required_cookie_names: {
            present: Array<string>
            missing: Array<string>
        }
    }
    live_probe: {
        checked: boolean
        status: CrawlerHealthStatus
        http_status: number | null
    }
}

type CrawlerHealthAudit = {
    generated_at: string
    counts: {
        checked: number
        ok: number
        warn: number
        fail: number
        skipped: number
    }
    results: Array<CrawlerHealthResult>
}

type LiveProbeResult = {
    status: CrawlerHealthStatus
    diagnostic_codes: Array<string>
    http_status: number | null
}

type CrawlerHealthAuditOptions = {
    now?: number
    timeoutMs?: number
    platforms?: Array<CrawlerHealthPlatform>
    resolveCookieFile?: (cookieFile: string) => string | null | undefined
    fetch?: typeof fetch
}

const REQUIRED_COOKIE_NAMES: Record<CrawlerHealthPlatform, Array<string>> = {
    x: ['auth_token', 'ct0'],
    instagram: ['sessionid', 'csrftoken'],
    tiktok: ['sessionid'],
    youtube: [],
    unknown: [],
}

const LIVE_PROBE_PLATFORMS = new Set<CrawlerHealthPlatform>(['x', 'instagram', 'tiktok'])
const X_PUBLIC_BEARER =
    'Bearer AAAAAAAAAAAAAAAAAAAAAFQODgEAAAAAVHTp76lzh3rFzcHbmHVvQxYYpTw%3DckAlMINMjmCwxUcaXbAN4XqJVdgMJaHqNOFgPMK0zN1qLqLQCF'

function emptyCookieMetadata(): NetscapeCookieFileAudit {
    return {
        total_cookie_rows: 0,
        usable_cookie_count: 0,
        expired_cookie_count: 0,
        session_cookie_count: 0,
        malformed_cookie_count: 0,
        http_only_cookie_count: 0,
        domains: [],
        cookie_names: [],
    }
}

function nodeId(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.id || value?.name || fallback).trim()
}

function nodeName(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.name || value?.id || fallback).trim()
}

function inferCrawlerPlatform(crawler: any): CrawlerHealthPlatform {
    const candidates = [crawler?.origin, ...(Array.isArray(crawler?.websites) ? crawler.websites : [])]

    for (const candidate of candidates) {
        try {
            const hostname = new URL(candidate).hostname.replace(/^www\./, '').toLowerCase()
            if (hostname === 'x.com' || hostname === 'twitter.com') return 'x'
            if (hostname === 'instagram.com') return 'instagram'
            if (hostname === 'tiktok.com') return 'tiktok'
            if (hostname === 'youtube.com' || hostname === 'youtu.be') return 'youtube'
        } catch {
            continue
        }
    }

    return 'unknown'
}

function cookieValue(cookies: Array<CookieData>, name: string) {
    return cookies.find((cookie) => cookie.name === name)?.value || ''
}

function mergeStatus(staticStatus: CrawlerHealthStatus, liveStatus: CrawlerHealthStatus): CrawlerHealthStatus {
    if (staticStatus === 'fail' || liveStatus === 'fail') return 'fail'
    if (staticStatus === 'warn' || liveStatus === 'warn') return 'warn'
    if (staticStatus === 'skipped' || liveStatus === 'skipped') return 'skipped'
    return 'ok'
}

async function fetchWithTimeout(
    fetchImpl: typeof fetch,
    url: string,
    init: RequestInit,
    timeoutMs: number,
): Promise<Response> {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    try {
        return await fetchImpl(url, {
            ...init,
            signal: init.signal || controller.signal,
        })
    } catch (error) {
        if (controller.signal.aborted) {
            throw new Error(`probe timed out after ${timeoutMs}ms`)
        }
        throw error
    } finally {
        clearTimeout(timer)
    }
}

function statusFromAuthResponse(
    platform: CrawlerHealthPlatform,
    response: Response,
    okCode: string,
    rejectedCode: string,
    rateLimitedCode: string,
    unexpectedCode: string,
): LiveProbeResult {
    if (response.status >= 200 && response.status < 300) {
        return {
            status: 'ok',
            diagnostic_codes: [okCode],
            http_status: response.status,
        }
    }
    if (response.status === 401 || response.status === 403) {
        return {
            status: 'fail',
            diagnostic_codes: [rejectedCode],
            http_status: response.status,
        }
    }
    if (response.status === 429) {
        return {
            status: 'warn',
            diagnostic_codes: [rateLimitedCode],
            http_status: response.status,
        }
    }
    return {
        status: platform === 'tiktok' && response.status >= 500 ? 'warn' : 'fail',
        diagnostic_codes: [unexpectedCode],
        http_status: response.status,
    }
}

async function probeX(cookies: Array<CookieData>, fetchImpl: typeof fetch, timeoutMs: number): Promise<LiveProbeResult> {
    const cookie = getCookieString(cookies)
    const ct0 = cookieValue(cookies, 'ct0')
    const response = await fetchWithTimeout(
        fetchImpl,
        'https://x.com/i/api/1.1/account/settings.json',
        {
            method: 'GET',
            headers: {
                authorization: X_PUBLIC_BEARER,
                cookie,
                'x-csrf-token': ct0,
                'x-twitter-active-user': 'yes',
                'x-twitter-auth-type': 'OAuth2Session',
                'user-agent':
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36',
                'accept-language': 'en-US,en;q=0.9',
            },
        },
        timeoutMs,
    )
    return statusFromAuthResponse(
        'x',
        response,
        'x_live_probe_ok',
        'x_live_auth_rejected',
        'x_live_rate_limited',
        'x_live_unexpected_status',
    )
}

async function probeInstagram(
    cookies: Array<CookieData>,
    fetchImpl: typeof fetch,
    timeoutMs: number,
): Promise<LiveProbeResult> {
    const cookie = getCookieString(cookies)
    const csrf = cookieValue(cookies, 'csrftoken')
    const response = await fetchWithTimeout(
        fetchImpl,
        'https://www.instagram.com/api/v1/users/web_profile_info/?username=instagram',
        {
            method: 'GET',
            headers: {
                cookie,
                'x-csrftoken': csrf,
                'x-ig-app-id': '936619743392459',
                referer: 'https://www.instagram.com/instagram/',
                'user-agent':
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36',
                'accept-language': 'en-US,en;q=0.9',
            },
        },
        timeoutMs,
    )
    const result = statusFromAuthResponse(
        'instagram',
        response,
        'instagram_live_probe_ok',
        'instagram_live_auth_rejected',
        'instagram_live_rate_limited',
        'instagram_live_unexpected_status',
    )
    if (result.status !== 'ok') {
        return result
    }
    try {
        const json = await response.json()
        if (!json?.data?.user?.username) {
            return {
                status: 'warn',
                diagnostic_codes: ['instagram_live_payload_missing_user'],
                http_status: response.status,
            }
        }
    } catch {
        return {
            status: 'warn',
            diagnostic_codes: ['instagram_live_payload_not_json'],
            http_status: response.status,
        }
    }
    return result
}

async function probeTikTok(
    cookies: Array<CookieData>,
    fetchImpl: typeof fetch,
    timeoutMs: number,
): Promise<LiveProbeResult> {
    const response = await fetchWithTimeout(
        fetchImpl,
        'https://www.tiktok.com/@tiktok',
        {
            method: 'GET',
            headers: {
                cookie: getCookieString(cookies),
                referer: 'https://www.tiktok.com/',
                'user-agent':
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36',
                'accept-language': 'en-US,en;q=0.9',
            },
        },
        timeoutMs,
    )
    const result = statusFromAuthResponse(
        'tiktok',
        response,
        'tiktok_live_probe_ok',
        'tiktok_live_auth_rejected',
        'tiktok_live_rate_limited',
        'tiktok_live_unexpected_status',
    )
    if (result.status !== 'ok') {
        return result
    }
    const text = await response.text()
    if (!text.includes('__UNIVERSAL_DATA_FOR_REHYDRATION__')) {
        return {
            status: 'warn',
            diagnostic_codes: ['tiktok_live_payload_missing_universal_data'],
            http_status: response.status,
        }
    }
    return result
}

async function runLiveProbe(
    platform: CrawlerHealthPlatform,
    cookies: Array<CookieData>,
    fetchImpl: typeof fetch,
    timeoutMs: number,
): Promise<LiveProbeResult> {
    try {
        if (platform === 'x') return await probeX(cookies, fetchImpl, timeoutMs)
        if (platform === 'instagram') return await probeInstagram(cookies, fetchImpl, timeoutMs)
        if (platform === 'tiktok') return await probeTikTok(cookies, fetchImpl, timeoutMs)
        return {
            status: 'skipped',
            diagnostic_codes: ['live_probe_unsupported_platform'],
            http_status: null,
        }
    } catch (error) {
        return {
            status: 'fail',
            diagnostic_codes: [`${platform}_live_probe_failed`],
            http_status: null,
        }
    }
}

async function buildCrawlerLiveHealthAudit(
    config: AppConfig,
    options: CrawlerHealthAuditOptions = {},
): Promise<CrawlerHealthAudit> {
    const fetchImpl = options.fetch || fetch
    const timeoutMs = Math.max(1000, Math.floor(Number(options.timeoutMs || 15_000)))
    const platformFilter = new Set(options.platforms || Array.from(LIVE_PROBE_PLATFORMS))
    const results: Array<CrawlerHealthResult> = []

    for (const [index, crawler] of (config.crawlers || []).entries()) {
        const platform = inferCrawlerPlatform(crawler)
        if (!platformFilter.has(platform)) {
            continue
        }

        const crawlerId = nodeId(crawler, `crawler-${index}`)
        const crawlerName = nodeName(crawler, crawlerId)
        const cookieFile = crawler.cfg_crawler?.cookie_file
        const requiredNames = REQUIRED_COOKIE_NAMES[platform]
        let exists = false
        let metadata = emptyCookieMetadata()
        let cookies: Array<CookieData> = []
        const diagnosticCodes = [] as Array<string>

        if (!cookieFile) {
            diagnosticCodes.push('cookie_file_not_configured')
        } else {
            try {
                const resolved = options.resolveCookieFile?.(cookieFile) ?? cookieFile
                exists = Boolean(resolved && fs.existsSync(resolved))
                if (resolved && exists) {
                    metadata = auditNetscapeCookieFile(resolved, { now: options.now })
                    cookies = parseNetscapeCookieToPuppeteerCookie(resolved, { now: options.now })
                } else {
                    diagnosticCodes.push('cookie_file_missing')
                }
            } catch {
                diagnosticCodes.push('cookie_file_unreadable')
            }
        }

        if (metadata.usable_cookie_count === 0) {
            diagnosticCodes.push('cookie_file_has_no_usable_rows')
        }
        if (metadata.malformed_cookie_count > 0) {
            diagnosticCodes.push('cookie_file_has_malformed_rows')
        }
        const present = requiredNames.filter((name) => metadata.cookie_names.includes(name))
        const missing = requiredNames.filter((name) => !metadata.cookie_names.includes(name))
        if (missing.length > 0) {
            diagnosticCodes.push('cookie_required_names_missing')
        }

        const staticStatus: CrawlerHealthStatus =
            diagnosticCodes.length > 0 ? (missing.length > 0 || metadata.usable_cookie_count === 0 ? 'fail' : 'warn') : 'ok'

        let liveProbe: LiveProbeResult = {
            status: 'skipped',
            diagnostic_codes: ['live_probe_static_cookie_unhealthy'],
            http_status: null,
        }
        if (staticStatus !== 'fail' && LIVE_PROBE_PLATFORMS.has(platform)) {
            liveProbe = await runLiveProbe(platform, cookies, fetchImpl, timeoutMs)
        }

        results.push({
            crawler_id: crawlerId,
            crawler_name: crawlerName,
            platform,
            status: mergeStatus(staticStatus, liveProbe.status),
            diagnostic_codes: Array.from(new Set([...diagnosticCodes, ...liveProbe.diagnostic_codes])).sort(),
            static_cookie: {
                exists,
                usable_cookie_count: metadata.usable_cookie_count,
                expired_cookie_count: metadata.expired_cookie_count,
                required_cookie_names: {
                    present,
                    missing,
                },
            },
            live_probe: {
                checked: liveProbe.status !== 'skipped',
                status: liveProbe.status,
                http_status: liveProbe.http_status,
            },
        })
    }

    return {
        generated_at: new Date().toISOString(),
        counts: {
            checked: results.length,
            ok: results.filter((result) => result.status === 'ok').length,
            warn: results.filter((result) => result.status === 'warn').length,
            fail: results.filter((result) => result.status === 'fail').length,
            skipped: results.filter((result) => result.status === 'skipped').length,
        },
        results,
    }
}

export {
    buildCrawlerLiveHealthAudit,
    inferCrawlerPlatform,
    type CrawlerHealthAudit,
    type CrawlerHealthAuditOptions,
    type CrawlerHealthPlatform,
    type CrawlerHealthResult,
    type CrawlerHealthStatus,
}
