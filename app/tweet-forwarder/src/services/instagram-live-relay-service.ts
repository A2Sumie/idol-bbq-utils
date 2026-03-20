import fs from 'fs'
import path from 'path'
import type { Logger } from '@idol-bbq-utils/log'
import { Instagram } from '@idol-bbq-utils/spider'
import type { Page } from 'puppeteer-core'
import { ensureDirectoryExists } from '@/utils/directories'
import type { CrawlerConfig, LiveRelayConfig, LiveRelayTargetConfig } from '@/types/crawler'

const DEFAULT_LIVE_PLAYER_URL = 'https://tv.n2nj.moe'
const DEFAULT_LIVE_PLAYER_PLAYER_ID = 'relay'
const DEFAULT_LIVE_PLAYER_STREAM_URL = 'https://stream.n2nj.moe/relay.m3u8'
const DEFAULT_SYNC_INTERVAL_SECONDS = 300
const STREAM_CAPTURE_TIMEOUT_MS = 15000

const ALLOWED_HEADER_KEYS = new Set([
    'user-agent',
    'referer',
    'origin',
    'cookie',
    'authorization',
    'x-requested-with',
    'accept',
    'accept-language',
    'accept-encoding',
    'content-type',
    'sec-ch-ua',
    'sec-ch-ua-mobile',
    'sec-ch-ua-platform',
    'sec-fetch-dest',
    'sec-fetch-mode',
    'sec-fetch-site',
])

interface StreamMediaInfo {
    size: number
    variants_count: number
    variants: Array<{
        url: string
        bandwidth: number
        resolution: string
    }>
    encrypted: boolean
    pssh: string | null
}

interface EchoStreamRecord {
    source: string
    type: 'HLS' | 'DASH'
    headers: Record<string, string>
    mediaInfo: StreamMediaInfo
}

interface EchoPackage {
    mode: 'echo'
    page_url: string
    timestamp: number
    cookies_b64: string
    streams_detected: number
    streams: Array<EchoStreamRecord>
    licenses: Array<{
        url: string
        headers: Record<string, string>
        timestamp: number
    }>
    keys: Array<{
        kid: string
        key: string
        session?: string
    }>
}

interface LiveRelayResolution extends LiveRelayTargetConfig {
    live_player_url: string
    player_id: string
    player_name: string
    player_url: string
    auth_username?: string
    auth_password?: string
    waf_bypass_header?: string
    sync_interval_seconds: number
    stop_offline: boolean
}

interface InstagramLiveCacheEntry {
    handle: string
    profileUrl: string
    checkedAt: string
    isLive: boolean
    liveBroadcastId: string | null
    liveBroadcastVisibility: string | null
    liveUrl: string | null
    displayName: string
    avatarUrl: string | null
    package: EchoPackage | null
    lastError?: string | null
    syncedAt?: string | null
    relay?: {
        baseUrl: string
        playerId: string
        active: boolean
        status?: number
        body?: unknown
    }
}

interface SyncInstagramLiveOptions {
    handle: string
    profileUrl: string
    page: Page
    crawlerConfig?: CrawlerConfig
    cookieString?: string
    requestHeaders?: Record<string, string>
    log?: Logger
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

function normalizeBaseUrl(url?: string) {
    return String(url || DEFAULT_LIVE_PLAYER_URL).replace(/\/+$/, '')
}

function parseCookieString(cookieString?: string) {
    const cookies: Record<string, string> = {}
    for (const entry of String(cookieString || '').split(';')) {
        const [rawName, ...rawValue] = entry.split('=')
        const name = rawName?.trim()
        if (!name) {
            continue
        }
        cookies[name] = rawValue.join('=').trim()
    }
    return cookies
}

function filterRelayHeaders(headers?: Record<string, string>) {
    return Object.fromEntries(
        Object.entries(headers || {}).filter(([key, value]) => {
            return typeof value === 'string' && value.trim().length > 0 && ALLOWED_HEADER_KEYS.has(key.toLowerCase())
        }),
    )
}

function parseM3u8Variants(content: string, baseUrl: string) {
    const variants: Array<{ url: string; bandwidth: number; resolution: string }> = []
    const lines = content.split('\n')
    let currentInfo: { bandwidth?: number; resolution?: string } = {}

    for (const rawLine of lines) {
        const line = rawLine.trim()
        if (line.startsWith('#EXT-X-STREAM-INF:')) {
            const bandwidthMatch = line.match(/BANDWIDTH=(\d+)/)
            const resolutionMatch = line.match(/RESOLUTION=(\d+x\d+)/)
            currentInfo = {
                bandwidth: bandwidthMatch ? Number(bandwidthMatch[1]) : 0,
                resolution: resolutionMatch?.[1] || 'Unknown',
            }
            continue
        }
        if (!line || line.startsWith('#')) {
            continue
        }
        if (currentInfo.bandwidth || currentInfo.resolution) {
            const url = line.startsWith('http') ? line : new URL(line, baseUrl).toString()
            variants.push({
                url,
                bandwidth: currentInfo.bandwidth || 0,
                resolution: currentInfo.resolution || 'Unknown',
            })
            currentInfo = {}
        }
    }

    return variants.sort((a, b) => b.bandwidth - a.bandwidth)
}

function analyzeManifestText(url: string, text: string): StreamMediaInfo {
    const info: StreamMediaInfo = {
        size: text.length,
        variants_count: 0,
        variants: [],
        encrypted: false,
        pssh: null,
    }

    if (url.includes('.m3u8')) {
        info.encrypted = text.includes('#EXT-X-KEY')
        info.variants = parseM3u8Variants(text, url)
        info.variants_count = info.variants.length
        return info
    }

    if (url.includes('.mpd')) {
        info.variants_count = (text.match(/<Representation/g) || []).length
        info.encrypted = text.includes('ContentProtection') || text.includes('cenc:default_KID')
        if (info.encrypted) {
            const psshMatch = text.match(/<cenc:pssh>([A-Za-z0-9+/=]+)<\/cenc:pssh>/)
            const altPsshMatch = text.match(/<mspr:pro>([A-Za-z0-9+/=]+)<\/mspr:pro>/)
            info.pssh = psshMatch?.[1] || altPsshMatch?.[1] || null
        }
    }

    return info
}

function isStreamManifest(url: string) {
    return url.includes('.m3u8') || url.includes('.mpd')
}

function buildPlayerUrl(playerId: string, configuredUrl?: string) {
    if (configuredUrl?.trim()) {
        return configuredUrl.trim()
    }
    if (playerId === DEFAULT_LIVE_PLAYER_PLAYER_ID) {
        return DEFAULT_LIVE_PLAYER_STREAM_URL
    }
    return `https://stream.n2nj.moe/${playerId}.m3u8`
}

function applyWafBypassHeader(headers: Headers, rawHeader?: string) {
    const normalized = rawHeader?.trim()
    if (!normalized) {
        return
    }
    const separatorIndex = normalized.indexOf(':')
    if (separatorIndex > 0) {
        const name = normalized.slice(0, separatorIndex).trim()
        const value = normalized.slice(separatorIndex + 1).trim()
        if (name && value) {
            headers.set(name, value)
        }
        return
    }
    headers.set('x-bypass-waf', normalized)
}

class InstagramLiveRelayService {
    private readonly cacheDir: string
    private readonly log?: Logger

    constructor(cacheRoot: string, log?: Logger) {
        this.cacheDir = path.join(cacheRoot, 'instagram-live')
        this.log = log?.child({ label: 'InstagramLiveRelayService' })
        ensureDirectoryExists(this.cacheDir)
    }

    async syncProfile(options: SyncInstagramLiveOptions) {
        const relayConfig = this.resolveTargetConfig(options.handle, options.crawlerConfig?.live_relay)
        if (!relayConfig) {
            return
        }

        const scopedLog = options.log || this.log
        const now = new Date().toISOString()
        const previousCache = this.readCache(options.handle)

        try {
            const status = await Instagram.InsApiJsonParser.grabProfileStatus(options.page, options.profileUrl)
            let nextCache: InstagramLiveCacheEntry = {
                handle: options.handle,
                profileUrl: options.profileUrl,
                checkedAt: now,
                isLive: status.is_live,
                liveBroadcastId: status.live_broadcast_id,
                liveBroadcastVisibility: status.live_broadcast_visibility,
                liveUrl: status.live_url,
                displayName: status.username,
                avatarUrl: status.u_avatar,
                package: previousCache?.package || null,
                syncedAt: previousCache?.syncedAt || null,
                relay: previousCache?.relay,
                lastError: null,
            }

            if (!status.is_live || !status.live_url) {
                if (relayConfig.stop_offline && previousCache?.relay?.active) {
                    const relayResponse = await this.stopRelay(relayConfig)
                    nextCache.relay = {
                        baseUrl: relayConfig.live_player_url,
                        playerId: relayConfig.player_id,
                        active: false,
                        status: relayResponse.status,
                        body: relayResponse.body,
                    }
                }
                nextCache.package = null
                this.writeCache(options.handle, nextCache)
                return nextCache
            }

            const shouldCapture =
                previousCache?.liveBroadcastId !== status.live_broadcast_id
                || !previousCache?.package
                || (previousCache.package.streams_detected || 0) === 0

            if (shouldCapture) {
                nextCache.package = await this.captureEchoPackage({
                    page: options.page,
                    profileUrl: options.profileUrl,
                    liveUrl: status.live_url,
                    cookieString: options.cookieString,
                    requestHeaders: options.requestHeaders,
                    log: scopedLog,
                })
            }

            const packageToSync = nextCache.package || previousCache?.package
            nextCache.package = packageToSync || null

            if (!packageToSync || packageToSync.streams_detected === 0) {
                nextCache.lastError = 'Live detected but no stream manifests were captured.'
                this.writeCache(options.handle, nextCache)
                return nextCache
            }

            const shouldSync =
                shouldCapture
                || !previousCache?.relay?.active
                || !previousCache?.syncedAt
                || Date.now() - new Date(previousCache.syncedAt).getTime() >= relayConfig.sync_interval_seconds * 1000

            if (shouldSync) {
                const relayResponse = await this.syncRelay(relayConfig, packageToSync, {
                    title: relayConfig.player_name || `【IG Live】${status.username || options.handle}`,
                    coverUrl: status.u_avatar,
                    description: `Instagram Live relay for ${status.username || options.handle}`,
                })
                nextCache.syncedAt = now
                nextCache.relay = {
                    baseUrl: relayConfig.live_player_url,
                    playerId: relayConfig.player_id,
                    active: true,
                    status: relayResponse.status,
                    body: relayResponse.body,
                }
            }

            this.writeCache(options.handle, nextCache)
            return nextCache
        } catch (error) {
            const failedCache: InstagramLiveCacheEntry = {
                handle: options.handle,
                profileUrl: options.profileUrl,
                checkedAt: now,
                isLive: previousCache?.isLive || false,
                liveBroadcastId: previousCache?.liveBroadcastId || null,
                liveBroadcastVisibility: previousCache?.liveBroadcastVisibility || null,
                liveUrl: previousCache?.liveUrl || null,
                displayName: previousCache?.displayName || options.handle,
                avatarUrl: previousCache?.avatarUrl || null,
                package: previousCache?.package || null,
                syncedAt: previousCache?.syncedAt || null,
                relay: previousCache?.relay,
                lastError: error instanceof Error ? error.message : String(error),
            }
            this.writeCache(options.handle, failedCache)
            throw error
        }
    }

    private resolveTargetConfig(handle: string, liveRelay?: LiveRelayConfig): LiveRelayResolution | null {
        if (!liveRelay) {
            return null
        }
        const handleConfig = liveRelay.targets?.[handle]
        const enabled =
            handleConfig?.enabled
            ?? liveRelay.enabled
            ?? Boolean(handleConfig || liveRelay.player_id || liveRelay.live_player_url)

        if (!enabled) {
            return null
        }

        const merged: LiveRelayResolution = {
            ...liveRelay,
            ...handleConfig,
            live_player_url: normalizeBaseUrl(handleConfig?.live_player_url || liveRelay.live_player_url || process.env.LIVE_PLAYER_URL),
            player_id: handleConfig?.player_id || liveRelay.player_id || process.env.LIVE_PLAYER_PLAYER_ID || DEFAULT_LIVE_PLAYER_PLAYER_ID,
            player_name: handleConfig?.player_name || liveRelay.player_name || `【Relay】${handle}`,
            player_url: buildPlayerUrl(
                handleConfig?.player_id || liveRelay.player_id || process.env.LIVE_PLAYER_PLAYER_ID || DEFAULT_LIVE_PLAYER_PLAYER_ID,
                handleConfig?.player_url || liveRelay.player_url,
            ),
            auth_username:
                handleConfig?.auth_username
                || liveRelay.auth_username
                || process.env.LIVE_PLAYER_ADMIN_ACCOUNT,
            auth_password:
                handleConfig?.auth_password
                || liveRelay.auth_password
                || process.env.LIVE_PLAYER_ADMIN_PASSWORD,
            waf_bypass_header:
                handleConfig?.waf_bypass_header
                || liveRelay.waf_bypass_header
                || process.env.LIVE_PLAYER_WAF_BYPASS_HEADER,
            sync_interval_seconds: Math.max(
                0,
                Number(
                    handleConfig?.sync_interval_seconds
                    || liveRelay.sync_interval_seconds
                    || process.env.LIVE_PLAYER_SYNC_INTERVAL_SECONDS
                    || DEFAULT_SYNC_INTERVAL_SECONDS,
                ),
            ),
            stop_offline: Boolean(handleConfig?.stop_offline ?? liveRelay.stop_offline),
        }

        return merged
    }

    private readCache(handle: string) {
        const cacheFile = this.getCacheFile(handle)
        if (!fs.existsSync(cacheFile)) {
            return null
        }
        try {
            return JSON.parse(fs.readFileSync(cacheFile, 'utf8')) as InstagramLiveCacheEntry
        } catch {
            return null
        }
    }

    private writeCache(handle: string, cache: InstagramLiveCacheEntry) {
        fs.writeFileSync(this.getCacheFile(handle), JSON.stringify(cache, null, 2), 'utf8')
    }

    private getCacheFile(handle: string) {
        const safeHandle = handle.replace(/[^A-Za-z0-9._-]/g, '_')
        return path.join(this.cacheDir, `${safeHandle}.json`)
    }

    private async captureEchoPackage({
        page,
        profileUrl,
        liveUrl,
        cookieString,
        requestHeaders,
        log,
    }: {
        page: Page
        profileUrl: string
        liveUrl: string
        cookieString?: string
        requestHeaders?: Record<string, string>
        log?: Logger
    }): Promise<EchoPackage> {
        const capturedStreams = new Map<string, EchoStreamRecord>()
        const analysisTasks: Array<Promise<void>> = []
        const baseHeaders = filterRelayHeaders(requestHeaders)
        const cookieEntries = await this.collectCookies(page, cookieString)

        const responseListener = (response: any) => {
            const source = response.url()
            if (!isStreamManifest(source)) {
                return
            }
            const responseRequestHeaders = filterRelayHeaders(response.request().headers())
            const headers = {
                ...baseHeaders,
                ...responseRequestHeaders,
            }
            if (!headers.cookie) {
                const cookieHeader = Object.entries(cookieEntries)
                    .map(([name, value]) => `${name}=${value}`)
                    .join('; ')
                if (cookieHeader) {
                    headers.cookie = cookieHeader
                }
            }
            const record: EchoStreamRecord = {
                source,
                type: source.includes('.mpd') ? 'DASH' : 'HLS',
                headers,
                mediaInfo: {
                    size: 0,
                    variants_count: 0,
                    variants: [],
                    encrypted: false,
                    pssh: null,
                },
            }
            capturedStreams.set(source, record)
            analysisTasks.push(
                (async () => {
                    try {
                        const text = await response.text()
                        record.mediaInfo = analyzeManifestText(source, text)
                    } catch (error) {
                        log?.warn(`Failed to analyze live manifest ${source}: ${error}`)
                    }
                })(),
            )
        }

        page.on('response', responseListener)

        try {
            for (const targetUrl of [liveUrl, profileUrl]) {
                try {
                    await page.goto(targetUrl, {
                        waitUntil: 'domcontentloaded',
                        timeout: 30000,
                    })
                } catch (error) {
                    log?.warn(`Instagram live capture navigation failed for ${targetUrl}: ${error}`)
                    continue
                }

                await page.waitForSelector('video', { timeout: 5000 }).catch(() => null)

                const deadline = Date.now() + STREAM_CAPTURE_TIMEOUT_MS
                while (Date.now() < deadline) {
                    if (capturedStreams.size > 0) {
                        break
                    }
                    await sleep(500)
                }
                if (capturedStreams.size > 0) {
                    break
                }
            }
        } finally {
            page.off('response', responseListener)
        }

        await Promise.allSettled(analysisTasks)

        const streams = Array.from(capturedStreams.values()).sort((a, b) => {
            return (b.mediaInfo?.variants_count || 0) - (a.mediaInfo?.variants_count || 0)
        })

        return {
            mode: 'echo',
            page_url: liveUrl,
            timestamp: Date.now(),
            cookies_b64: Buffer.from(JSON.stringify(cookieEntries), 'utf8').toString('base64'),
            streams_detected: streams.length,
            streams,
            licenses: [],
            keys: [],
        }
    }

    private async collectCookies(page: Page, cookieString?: string) {
        const cookies = parseCookieString(cookieString)
        for (const cookie of await page.browserContext().cookies().catch(() => [] as Array<any>)) {
            const domain = String(cookie.domain || '').replace(/^\./, '').toLowerCase()
            if (!domain.includes('instagram.com')) {
                continue
            }
            if (!cookie.name) {
                continue
            }
            cookies[cookie.name] = cookie.value
        }
        return cookies
    }

    private async syncRelay(
        relayConfig: LiveRelayResolution,
        streamConfig: EchoPackage,
        metadata: {
            title: string
            coverUrl?: string | null
            description?: string
        },
    ) {
        const authCookie = await this.login(relayConfig)
        let response = await this.postRelayAction(relayConfig, authCookie, {
            action: 'sync',
            streamConfig,
            metadata: {
                title: metadata.title,
                coverUrl: metadata.coverUrl || undefined,
            },
        })

        if (response.status === 404) {
            await this.ensurePlayer(relayConfig, authCookie, metadata)
            response = await this.postRelayAction(relayConfig, authCookie, {
                action: 'sync',
                streamConfig,
                metadata: {
                    title: metadata.title,
                    coverUrl: metadata.coverUrl || undefined,
                },
            })
        }

        if (!response.ok) {
            throw new Error(`Failed to sync relay ${relayConfig.player_id}: ${response.status} ${JSON.stringify(response.body)}`)
        }

        return response
    }

    private async stopRelay(relayConfig: LiveRelayResolution) {
        const authCookie = await this.login(relayConfig)
        const response = await this.postRelayAction(relayConfig, authCookie, {
            action: 'stop',
        })
        if (!response.ok && response.status !== 404) {
            throw new Error(`Failed to stop relay ${relayConfig.player_id}: ${response.status} ${JSON.stringify(response.body)}`)
        }
        return response
    }

    private async ensurePlayer(
        relayConfig: LiveRelayResolution,
        authCookie: string,
        metadata: {
            title: string
            coverUrl?: string | null
            description?: string
        },
    ) {
        const headers = new Headers({
            'Content-Type': 'application/json',
            Cookie: authCookie,
        })
        applyWafBypassHeader(headers, relayConfig.waf_bypass_header)

        const response = await fetch(`${relayConfig.live_player_url}/api/players`, {
            method: 'POST',
            headers,
            body: JSON.stringify({
                name: metadata.title,
                pId: relayConfig.player_id,
                url: relayConfig.player_url,
                description: metadata.description || null,
                coverUrl: metadata.coverUrl || null,
                announcement: null,
                streamConfig: { mode: 'echo' },
            }),
        })
        const text = await response.text()
        if (!response.ok && response.status !== 400) {
            throw new Error(`Failed to create relay player ${relayConfig.player_id}: ${response.status} ${text}`)
        }
    }

    private async login(relayConfig: LiveRelayResolution) {
        if (!relayConfig.auth_username || !relayConfig.auth_password) {
            throw new Error(`Missing live-player credentials for ${relayConfig.player_id}`)
        }

        const headers = new Headers({
            'Content-Type': 'application/json',
        })
        applyWafBypassHeader(headers, relayConfig.waf_bypass_header)

        const response = await fetch(`${relayConfig.live_player_url}/api/auth/login`, {
            method: 'POST',
            headers,
            body: JSON.stringify({
                username: relayConfig.auth_username,
                password: relayConfig.auth_password,
            }),
        })
        const setCookie = response.headers.get('set-cookie') || ''
        const bodyText = await response.text()
        if (!response.ok) {
            throw new Error(`live-player login failed: ${response.status} ${bodyText}`)
        }
        const authTokenCookie = setCookie
            .split(/,(?=\s*[A-Za-z0-9_.-]+=)/)
            .map((value) => value.trim())
            .find((value) => value.startsWith('auth-token='))
        const cookiePair = (authTokenCookie || setCookie).split(';')[0]?.trim()
        if (!cookiePair) {
            throw new Error('live-player login missing auth-token cookie')
        }
        return cookiePair
    }

    private async postRelayAction(
        relayConfig: LiveRelayResolution,
        authCookie: string,
        body: Record<string, unknown>,
    ) {
        const headers = new Headers({
            'Content-Type': 'application/json',
            Cookie: authCookie,
        })
        applyWafBypassHeader(headers, relayConfig.waf_bypass_header)

        const response = await fetch(
            `${relayConfig.live_player_url}/api/players/${encodeURIComponent(relayConfig.player_id)}/relay`,
            {
                method: 'POST',
                headers,
                body: JSON.stringify(body),
            },
        )
        const text = await response.text()
        return {
            ok: response.ok,
            status: response.status,
            body: (() => {
                try {
                    return JSON.parse(text)
                } catch {
                    return text
                }
            })(),
        }
    }
}

export {
    DEFAULT_LIVE_PLAYER_PLAYER_ID,
    DEFAULT_LIVE_PLAYER_STREAM_URL,
    DEFAULT_LIVE_PLAYER_URL,
    InstagramLiveRelayService,
    analyzeManifestText,
    buildPlayerUrl,
    filterRelayHeaders,
    parseCookieString,
}
export type { EchoPackage, InstagramLiveCacheEntry }
