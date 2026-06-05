import fs from 'fs'
import {
    auditNetscapeCookieFile,
    getCookieString,
    parseNetscapeCookieToPuppeteerCookie,
    type NetscapeCookieFileAudit,
} from '@idol-bbq-utils/spider'
import type { CookieData } from 'puppeteer-core'
import type { AppConfig } from '@/types'
import {
    inferCookieHealthPlatform,
    summarizeRequiredCookieNames,
    type CookieHealthPlatform,
} from './crawler-cookie-policy'

type CrawlerHealthPlatform = Exclude<CookieHealthPlatform, 'website'>
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

type CrawlerCookieLiveProbeResult = {
    status: CrawlerHealthStatus
    diagnostic_codes: Array<string>
    http_status: number | null
}

type XLiveProbeTarget =
    | {
          kind: 'list'
          id: string
      }
    | {
          kind: 'user'
          screen_name: string
      }

type CrawlerHealthAuditOptions = {
    now?: number
    timeoutMs?: number
    platforms?: Array<CrawlerHealthPlatform>
    resolveCookieFile?: (cookieFile: string) => string | null | undefined
    fetch?: typeof fetch
    xProbeTarget?: XLiveProbeTarget
}

const LIVE_PROBE_PLATFORMS = new Set<CrawlerHealthPlatform>(['x', 'instagram', 'tiktok'])
const X_PUBLIC_BEARER =
    'Bearer AAAAAAAAAAAAAAAAAAAAAFQODgEAAAAAVHTp76lzh3rFzcHbmHVvQxYYpTw%3DckAlMINMjmCwxUcaXbAN4XqJVdgMJaHqNOFgPMK0zN1qLqLQCF'
const X_GRAPHQL_PUBLIC_BEARER =
    'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'
const X_LIST_LATEST_TIMELINE_QUERY_ID = 'NRigOCel0QKiWs_GuBgOzw'
const X_USER_BY_SCREEN_NAME_QUERY_ID = '32pL5BWe9WKeSK1MoPvFQQ'
const X_GUEST_TOKEN = '1918915913551839395'

const X_RESERVED_PATHS = new Set([
    '',
    'compose',
    'explore',
    'home',
    'i',
    'jobs',
    'login',
    'messages',
    'notifications',
    'privacy',
    'search',
    'settings',
    'signup',
    'tos',
])

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
    const platform = inferCookieHealthPlatform(crawler)
    return platform === 'website' ? 'unknown' : platform
}

function cookieValue(cookies: Array<CookieData>, name: string) {
    return cookies.find((cookie) => cookie.name === name)?.value || ''
}

function normalizeCrawlerTargets(crawler: any): Array<string> {
    if (Array.isArray(crawler?.websites)) {
        return crawler.websites.map((entry: unknown) => String(entry || '').trim()).filter(Boolean)
    }

    const origin = String(crawler?.origin || '').trim()
    const paths = Array.isArray(crawler?.paths) ? crawler.paths : []
    if (!origin || paths.length === 0) {
        return origin ? [origin] : []
    }

    return paths
        .map((path: unknown) => String(path || '').trim())
        .filter(Boolean)
        .map((path: string) => {
            if (/^https?:\/\//i.test(path)) {
                return path
            }
            try {
                const base = origin.endsWith('/') ? origin : `${origin}/`
                return new URL(path.replace(/^\/+/, ''), base).toString()
            } catch {
                return `${origin.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`
            }
        })
}

function inferXProbeTarget(crawler: any): XLiveProbeTarget | undefined {
    const targets = normalizeCrawlerTargets(crawler)
    for (const target of targets) {
        try {
            const url = new URL(target)
            if (url.hostname !== 'x.com' && url.hostname !== 'twitter.com' && url.hostname !== 'www.x.com') {
                continue
            }
            const listMatch = url.pathname.match(/^\/i\/lists\/(\d+)/)
            if (listMatch?.[1]) {
                return { kind: 'list', id: listMatch[1] }
            }
        } catch {
            continue
        }
    }

    for (const target of targets) {
        try {
            const url = new URL(target)
            if (url.hostname !== 'x.com' && url.hostname !== 'twitter.com' && url.hostname !== 'www.x.com') {
                continue
            }
            const screenName = decodeURIComponent(url.pathname.replace(/^\/+|\/+$/g, '')).trim()
            if (!screenName || screenName.includes('/') || X_RESERVED_PATHS.has(screenName.toLowerCase())) {
                continue
            }
            return { kind: 'user', screen_name: screenName.replace(/^@+/, '') }
        } catch {
            continue
        }
    }

    return undefined
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
): CrawlerCookieLiveProbeResult {
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

function graphqlStatusFromResponse(
    response: Response,
    okCode: string,
    rejectedCode: string,
    rateLimitedCode: string,
    unexpectedCode: string,
): CrawlerCookieLiveProbeResult {
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
    if (response.status < 200 || response.status >= 300) {
        return {
            status: 'fail',
            diagnostic_codes: [unexpectedCode],
            http_status: response.status,
        }
    }
    return {
        status: 'ok',
        diagnostic_codes: [okCode],
        http_status: response.status,
    }
}

async function graphqlProbeResult(
    response: Response,
    codes: {
        ok: string
        rejected: string
        rateLimited: string
        unexpected: string
        payloadErrors: string
        payloadMissingData: string
        payloadNotJson: string
    },
): Promise<CrawlerCookieLiveProbeResult> {
    const status = graphqlStatusFromResponse(
        response,
        codes.ok,
        codes.rejected,
        codes.rateLimited,
        codes.unexpected,
    )
    if (status.status !== 'ok') {
        return status
    }

    try {
        const json = await response.json()
        if (json?.errors?.length) {
            return {
                status: 'fail',
                diagnostic_codes: [codes.payloadErrors],
                http_status: response.status,
            }
        }
        if (!json?.data) {
            return {
                status: 'warn',
                diagnostic_codes: [codes.payloadMissingData],
                http_status: response.status,
            }
        }
        return status
    } catch {
        return {
            status: 'warn',
            diagnostic_codes: [codes.payloadNotJson],
            http_status: response.status,
        }
    }
}

function xGraphqlHeaders(cookies: Array<CookieData>, referer: string, extraHeaders: Record<string, string> = {}) {
    const cookie = getCookieString(cookies)
    const ct0 = cookieValue(cookies, 'ct0')
    return {
        authorization: X_GRAPHQL_PUBLIC_BEARER,
        cookie,
        'x-csrf-token': ct0,
        'x-twitter-active-user': 'yes',
        'x-twitter-auth-type': 'OAuth2Session',
        'user-agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36',
        'accept-language': 'en-US,en;q=0.9',
        origin: 'https://x.com',
        referer,
        ...extraHeaders,
    }
}

function buildXListTimelineProbeUrl(listId: string) {
    const query = new URLSearchParams()
    query.append('variables', JSON.stringify({ listId, count: 1 }))
    query.append(
        'features',
        JSON.stringify({
            rweb_video_screen_enabled: false,
            profile_label_improvements_pcf_label_in_post_enabled: true,
            responsive_web_profile_redirect_enabled: false,
            rweb_tipjar_consumption_enabled: true,
            verified_phone_label_enabled: false,
            creator_subscriptions_tweet_preview_api_enabled: true,
            responsive_web_graphql_timeline_navigation_enabled: true,
            responsive_web_graphql_skip_user_profile_image_extensions_enabled: false,
            premium_content_api_read_enabled: false,
            communities_web_enable_tweet_community_results_fetch: true,
            c9s_tweet_anatomy_moderator_badge_enabled: true,
            responsive_web_grok_analyze_button_fetch_trends_enabled: false,
            responsive_web_grok_analyze_post_followups_enabled: true,
            responsive_web_jetfuel_frame: true,
            responsive_web_grok_share_attachment_enabled: true,
            responsive_web_grok_annotations_enabled: false,
            articles_preview_enabled: true,
            responsive_web_edit_tweet_api_enabled: true,
            graphql_is_translatable_rweb_tweet_is_translatable_enabled: true,
            view_counts_everywhere_api_enabled: true,
            longform_notetweets_consumption_enabled: true,
            responsive_web_twitter_article_tweet_consumption_enabled: true,
            tweet_awards_web_tipping_enabled: false,
            responsive_web_grok_show_grok_translated_post: false,
            responsive_web_grok_analysis_button_from_backend: true,
            post_ctas_fetch_enabled: false,
            creator_subscriptions_quote_tweet_preview_enabled: false,
            freedom_of_speech_not_reach_fetch_enabled: true,
            standardized_nudges_misinfo: true,
            tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled: true,
            longform_notetweets_rich_text_read_enabled: true,
            longform_notetweets_inline_media_enabled: true,
            responsive_web_grok_image_annotation_enabled: true,
            responsive_web_grok_imagine_annotation_enabled: true,
            responsive_web_grok_community_note_auto_translation_is_enabled: false,
            responsive_web_enhance_cards_enabled: false,
        }),
    )
    return `https://x.com/i/api/graphql/${X_LIST_LATEST_TIMELINE_QUERY_ID}/ListLatestTweetsTimeline?${query.toString()}`
}

function buildXUserProbeUrl(screenName: string) {
    const query = new URLSearchParams()
    query.append('variables', JSON.stringify({ screen_name: screenName, withGrokTranslatedBio: false }))
    query.append(
        'features',
        JSON.stringify({
            hidden_profile_subscriptions_enabled: true,
            profile_label_improvements_pcf_label_in_post_enabled: true,
            responsive_web_profile_redirect_enabled: false,
            rweb_tipjar_consumption_enabled: true,
            verified_phone_label_enabled: false,
            subscriptions_verification_info_is_identity_verified_enabled: true,
            subscriptions_verification_info_verified_since_enabled: true,
            highlights_tweets_tab_ui_enabled: true,
            responsive_web_twitter_article_notes_tab_enabled: true,
            subscriptions_feature_can_gift_premium: true,
            creator_subscriptions_tweet_preview_api_enabled: true,
            responsive_web_graphql_skip_user_profile_image_extensions_enabled: false,
            responsive_web_graphql_timeline_navigation_enabled: true,
        }),
    )
    query.append('fieldToggles', JSON.stringify({ withPayments: false, withAuxiliaryUserLabels: true }))
    return `https://x.com/i/api/graphql/${X_USER_BY_SCREEN_NAME_QUERY_ID}/UserByScreenName?${query.toString()}`
}

async function probeXGraphql(
    cookies: Array<CookieData>,
    fetchImpl: typeof fetch,
    timeoutMs: number,
    target: XLiveProbeTarget,
): Promise<CrawlerCookieLiveProbeResult> {
    if (target.kind === 'list') {
        const response = await fetchWithTimeout(
            fetchImpl,
            buildXListTimelineProbeUrl(target.id),
            {
                method: 'GET',
                headers: xGraphqlHeaders(cookies, `https://x.com/i/lists/${target.id}`),
            },
            timeoutMs,
        )
        return graphqlProbeResult(response, {
            ok: 'x_list_timeline_probe_ok',
            rejected: 'x_list_timeline_auth_rejected',
            rateLimited: 'x_list_timeline_rate_limited',
            unexpected: 'x_list_timeline_unexpected_status',
            payloadErrors: 'x_list_timeline_payload_errors',
            payloadMissingData: 'x_list_timeline_payload_missing_data',
            payloadNotJson: 'x_list_timeline_payload_not_json',
        })
    }

    const response = await fetchWithTimeout(
        fetchImpl,
        buildXUserProbeUrl(target.screen_name),
        {
            method: 'GET',
            headers: xGraphqlHeaders(cookies, `https://x.com/${target.screen_name}`, {
                'x-guest-token': X_GUEST_TOKEN,
            }),
        },
        timeoutMs,
    )
    return graphqlProbeResult(response, {
        ok: 'x_user_lookup_probe_ok',
        rejected: 'x_user_lookup_auth_rejected',
        rateLimited: 'x_user_lookup_rate_limited',
        unexpected: 'x_user_lookup_unexpected_status',
        payloadErrors: 'x_user_lookup_payload_errors',
        payloadMissingData: 'x_user_lookup_payload_missing_data',
        payloadNotJson: 'x_user_lookup_payload_not_json',
    })
}

async function probeX(
    cookies: Array<CookieData>,
    fetchImpl: typeof fetch,
    timeoutMs: number,
    target?: XLiveProbeTarget,
): Promise<CrawlerCookieLiveProbeResult> {
    if (target) {
        return probeXGraphql(cookies, fetchImpl, timeoutMs, target)
    }

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
): Promise<CrawlerCookieLiveProbeResult> {
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
): Promise<CrawlerCookieLiveProbeResult> {
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
    options: Pick<CrawlerHealthAuditOptions, 'xProbeTarget'> = {},
): Promise<CrawlerCookieLiveProbeResult> {
    try {
        if (platform === 'x') return await probeX(cookies, fetchImpl, timeoutMs, options.xProbeTarget)
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

async function probeCrawlerCookieLiveHealth(
    platform: CookieHealthPlatform,
    cookies: Array<CookieData>,
    options: Pick<CrawlerHealthAuditOptions, 'fetch' | 'timeoutMs'> = {},
): Promise<CrawlerCookieLiveProbeResult> {
    const auditPlatform: CrawlerHealthPlatform = platform === 'website' ? 'unknown' : platform
    const timeoutMs = Math.max(1000, Math.floor(Number(options.timeoutMs || 15_000)))
    if (!LIVE_PROBE_PLATFORMS.has(auditPlatform)) {
        return {
            status: 'skipped',
            diagnostic_codes: ['live_probe_unsupported_platform'],
            http_status: null,
        }
    }
    return runLiveProbe(auditPlatform, cookies, options.fetch || fetch, timeoutMs, {
        xProbeTarget: options.xProbeTarget,
    })
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
        const { present, missing } = summarizeRequiredCookieNames(platform, metadata.cookie_names)
        if (missing.length > 0) {
            diagnosticCodes.push('cookie_required_names_missing')
        }

        const staticStatus: CrawlerHealthStatus =
            diagnosticCodes.length > 0 ? (missing.length > 0 || metadata.usable_cookie_count === 0 ? 'fail' : 'warn') : 'ok'

        let liveProbe: CrawlerCookieLiveProbeResult = {
            status: 'skipped',
            diagnostic_codes: ['live_probe_static_cookie_unhealthy'],
            http_status: null,
        }
        if (staticStatus !== 'fail' && LIVE_PROBE_PLATFORMS.has(platform)) {
            liveProbe = await runLiveProbe(platform, cookies, fetchImpl, timeoutMs, {
                xProbeTarget: platform === 'x' ? inferXProbeTarget(crawler) : undefined,
            })
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
    inferXProbeTarget,
    probeCrawlerCookieLiveHealth,
    type CrawlerHealthAudit,
    type CrawlerHealthAuditOptions,
    type CrawlerHealthPlatform,
    type CrawlerHealthResult,
    type CrawlerHealthStatus,
    type CrawlerCookieLiveProbeResult,
    type XLiveProbeTarget,
}
