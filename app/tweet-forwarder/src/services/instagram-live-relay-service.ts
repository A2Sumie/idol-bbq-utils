import fs from 'fs'
import path from 'path'
import { execFileSync, spawn, type ChildProcessWithoutNullStreams } from 'child_process'
import { createHash } from 'crypto'
import type { Logger } from '@idol-bbq-utils/log'
import { Instagram } from '@idol-bbq-utils/spider'
import { Platform } from '@idol-bbq-utils/spider/types'
import type { Page } from 'puppeteer-core'
import type { Article } from '@/db'
import { buildBiliupUploadCandidate, completeBiliupUploadCandidateTags, runBiliupUpload, type BiliupUploadCandidate } from '@/middleware/forwarder/biliup'
import { ensureDirectoryExists } from '@/utils/directories'
import { CACHE_DIR_ROOT } from '@/config'
import type { CrawlerConfig, InstagramLiveArchiveConfig, InstagramLivePublishConfig, LiveRelayConfig, LiveRelayTargetConfig } from '@/types/crawler'

const DEFAULT_LIVE_PLAYER_URL = 'https://tv.n2nj.moe'
const DEFAULT_LIVE_PLAYER_PLAYER_ID = 'relay'
const DEFAULT_LIVE_PLAYER_STREAM_URL = 'https://stream.n2nj.moe/relay.m3u8'
const DEFAULT_SYNC_INTERVAL_SECONDS = 300
const DEFAULT_POST_LIVE_GRACE_SECONDS = 6 * 60 * 60
const STREAM_CAPTURE_TIMEOUT_MS = 15000
const MANIFEST_FETCH_TIMEOUT_MS = 15000
const LIVE_PLAYER_API_TIMEOUT_MS = 15000
const DEFAULT_INSTAGRAM_ARCHIVE_EXTENSION = 'mkv'
const DEFAULT_INSTAGRAM_ARCHIVE_MIN_PUBLISH_DURATION_SECONDS = 60
const DEFAULT_INSTAGRAM_ARCHIVE_MAX_DURATION_SECONDS = 4 * 60 * 60
const INSTAGRAM_ORIGIN = 'https://www.instagram.com'
const LIVE_WEB_INFO_PATH = '/api/v1/live/web_info/'
const N2NJ_REQUEST_USER_AGENT = 'N2NJ-Stream-Bot/1.0'

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

interface ResolvedInstagramLiveArchiveConfig {
    enabled: true
    root_dir: string
    ffmpeg_path: string
    extension: 'mp4' | 'mkv' | 'ts'
    max_duration_seconds: number
    min_publish_duration_seconds: number
    stop_at_epoch?: number
}

interface ResolvedInstagramLivePublishConfig {
    enabled: true
    sessdata?: string
    bili_jct?: string
    video_upload: NonNullable<InstagramLivePublishConfig['video_upload']>
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
    post_live_grace_seconds: number
    stop_offline: boolean
    relay_enabled: boolean
    archive?: ResolvedInstagramLiveArchiveConfig
    publish?: ResolvedInstagramLivePublishConfig
}

interface InstagramLiveArchiveState {
    active: boolean
    broadcastId: string | null
    mediaPath: string | null
    manifestPath: string | null
    diagnosticsPath?: string | null
    stderrLogPath?: string | null
    stdoutLogPath?: string | null
    startedAt: string | null
    completedAt?: string | null
    durationSeconds?: number | null
    sizeBytes?: number | null
    publishedAt?: string | null
    publishResult?: unknown
    lastError?: string | null
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
    lastLiveAt?: string | null
    package: EchoPackage | null
    archive?: InstagramLiveArchiveState | null
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

interface InstagramLiveRecordingSession {
    handle: string
    broadcastId: string | null
    liveUrl: string
    profileUrl: string
    displayName: string
    avatarUrl: string | null
    startedAt: string
    mediaPath: string
    manifestPath: string
    diagnosticsPath: string
    stderrLogPath: string
    stdoutLogPath: string
    stream: EchoStreamRecord
    process: ChildProcessWithoutNullStreams
    stderr: string[]
    stdout: string[]
    archiveConfig: ResolvedInstagramLiveArchiveConfig
    publishConfig?: ResolvedInstagramLivePublishConfig
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

function resolveNonNegativeSeconds(value: unknown, fallback: number) {
    const numeric = Number(value ?? fallback)
    return Number.isFinite(numeric) && numeric >= 0 ? numeric : fallback
}

function resolvePositiveSeconds(value: unknown, fallback: number) {
    const numeric = Number(value ?? fallback)
    return Number.isFinite(numeric) && numeric > 0 ? numeric : fallback
}

function normalizeArchiveExtension(value: unknown) {
    const normalized = String(value || DEFAULT_INSTAGRAM_ARCHIVE_EXTENSION).replace(/^\./, '').toLowerCase()
    return normalized === 'mkv' || normalized === 'ts' || normalized === 'mp4'
        ? normalized
        : DEFAULT_INSTAGRAM_ARCHIVE_EXTENSION
}

function resolveArchiveStopAtEpoch(value: unknown) {
    const raw = String(value || '').trim()
    if (!raw) {
        return undefined
    }
    const parsed = Date.parse(raw)
    if (Number.isFinite(parsed)) {
        return Math.floor(parsed / 1000)
    }
    const match = raw.match(/^(\d{1,2}):(\d{2})$/)
    if (!match) {
        return undefined
    }
    const now = new Date()
    const localDate = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Tokyo',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
    }).format(now)
    const parsedJst = Date.parse(`${localDate}T${match[1]!.padStart(2, '0')}:${match[2]}:00+09:00`)
    return Number.isFinite(parsedJst) ? Math.floor(parsedJst / 1000) : undefined
}

function resolveInstagramArchiveConfig(
    handleConfig?: Pick<LiveRelayTargetConfig, 'archive'>,
    liveRelay?: Pick<LiveRelayTargetConfig, 'archive'>,
): ResolvedInstagramLiveArchiveConfig | undefined {
    const raw = handleConfig?.archive ?? liveRelay?.archive
    const enabled = raw?.enabled ?? false
    if (!enabled) {
        return undefined
    }
    return {
        enabled: true,
        root_dir: path.resolve(String(raw?.root_dir || process.env.INSTAGRAM_LIVE_ARCHIVE_ROOT || path.join(process.cwd(), 'archive', 'instagram-live'))),
        ffmpeg_path: String(raw?.ffmpeg_path || process.env.FFMPEG_PATH || 'ffmpeg'),
        extension: normalizeArchiveExtension(raw?.extension),
        max_duration_seconds: resolvePositiveSeconds(raw?.max_duration_seconds, DEFAULT_INSTAGRAM_ARCHIVE_MAX_DURATION_SECONDS),
        min_publish_duration_seconds: resolveNonNegativeSeconds(
            raw?.min_publish_duration_seconds,
            DEFAULT_INSTAGRAM_ARCHIVE_MIN_PUBLISH_DURATION_SECONDS,
        ),
        stop_at_epoch: resolveArchiveStopAtEpoch(raw?.stop_at),
    }
}

function resolveInstagramPublishConfig(
    handleConfig?: Pick<LiveRelayTargetConfig, 'publish'>,
    liveRelay?: Pick<LiveRelayTargetConfig, 'publish'>,
): ResolvedInstagramLivePublishConfig | undefined {
    const raw = handleConfig?.publish ?? liveRelay?.publish
    if (!raw?.enabled || !raw.video_upload?.enabled) {
        return undefined
    }
    return {
        enabled: true,
        sessdata: raw.sessdata,
        bili_jct: raw.bili_jct,
        video_upload: raw.video_upload,
    }
}

async function fetchWithTimeout(url: string, init: RequestInit = {}, timeoutMs = LIVE_PLAYER_API_TIMEOUT_MS) {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), timeoutMs)
    try {
        return await fetch(url, {
            ...init,
            signal: init.signal || controller.signal,
        })
    } finally {
        clearTimeout(timeout)
    }
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
        }).map(([key, value]) => [key.toLowerCase(), value.trim()]),
    )
}

function normalizeUrlValue(value: unknown) {
    if (typeof value !== 'string') {
        return null
    }
    const trimmed = value.trim()
    if (!trimmed) {
        return null
    }
    return trimmed.replace(/\\u0026/g, '&')
}

function isLiveWebInfoResponse(url: string) {
    return url.includes(LIVE_WEB_INFO_PATH)
}

function parseInstagramLiveWebInfo(json: any) {
    const payload = [json?.broadcast, json?.data, json].find((candidate) => {
        return candidate?.dash_abr_playback_url || candidate?.dash_playback_url || candidate?.broadcast_status
    }) || json
    const streamUrls = Array.from(
        new Set(
            [payload?.dash_abr_playback_url, payload?.dash_playback_url, payload?.hls_playback_url]
                .map((value) => normalizeUrlValue(value))
                .filter((value): value is string => Boolean(value)),
        ),
    )

    return {
        broadcastStatus: payload?.broadcast_status ? String(payload.broadcast_status) : null,
        coverUrl: normalizeUrlValue(payload?.cover_frame_url),
        streamUrls,
    }
}

function createEmptyMediaInfo(): StreamMediaInfo {
    return {
        size: 0,
        variants_count: 0,
        variants: [],
        encrypted: false,
        pssh: null,
    }
}

function buildCookieHeader(cookieEntries: Record<string, string>) {
    return Object.entries(cookieEntries)
        .map(([name, value]) => `${name}=${value}`)
        .join('; ')
}

function isPostLiveGraceActive(lastLiveAt?: string | null, graceSeconds: number = DEFAULT_POST_LIVE_GRACE_SECONDS, now = Date.now()) {
    if (!lastLiveAt || graceSeconds <= 0) {
        return false
    }
    const lastLiveMs = new Date(lastLiveAt).getTime()
    if (!Number.isFinite(lastLiveMs)) {
        return false
    }
    return now - lastLiveMs <= graceSeconds * 1000
}

function resolveLastLiveAt(cache?: InstagramLiveCacheEntry | null) {
    if (!cache) {
        return null
    }
    return cache.lastLiveAt || (cache.isLive ? cache.checkedAt : null) || cache.syncedAt || null
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

function applyN2njRequestIdentity(headers: Headers) {
    if (!headers.has('User-Agent')) {
        headers.set('User-Agent', process.env.LIVE_PLAYER_REQUEST_USER_AGENT || N2NJ_REQUEST_USER_AGENT)
    }
    if (!headers.has('Accept')) {
        headers.set('Accept', 'application/json')
    }
}

function extractInstagramAppIdFromHtml(html: string) {
    return html.match(/"APP_ID"\s*:\s*"(\d+)"/)?.[1] || '936619743392459'
}

function sanitizeFileSegment(value: string, fallback: string) {
    const normalized = String(value || '')
        .replace(/[<>:"/\\|?*\u0000-\u001F]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
    return (normalized || fallback).slice(0, 80)
}

function formatDateParts(timestampMs: number) {
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Tokyo',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hourCycle: 'h23',
    })
        .formatToParts(new Date(timestampMs))
        .reduce<Record<string, string>>((acc, part) => {
            if (part.type !== 'literal') {
                acc[part.type] = part.value
            }
            return acc
        }, {})
    return {
        date: `${parts.year}-${parts.month}-${parts.day}`,
        shortDate: `${String(parts.year || '').slice(-2)}.${parts.month}.${parts.day}`,
        time: `${parts.hour}${parts.minute}`,
    }
}

function safeJsonParse<T>(value: string): T | null {
    try {
        return JSON.parse(value) as T
    } catch {
        return null
    }
}

function streamHeaderArgs(headers: Record<string, string>) {
    const lines = Object.entries(headers)
        .filter(([, value]) => String(value || '').trim())
        .map(([key, value]) => `${key}: ${value}`)
    return lines.length > 0 ? ['-headers', `${lines.join('\r\n')}\r\n`] : []
}

function archiveIdFor(mediaPath: string, startedAt: string) {
    return createHash('sha1').update(`instagram-live\n${mediaPath}\n${startedAt}`).digest('hex')
}

class InstagramLiveRelayService {
    private readonly cacheDir: string
    private readonly log?: Logger
    private readonly recordingSessions = new Map<string, InstagramLiveRecordingSession>()
    private readonly publishedArchives = new Set<string>()
    private publishedArchivesLoaded = false

    private get publishedArchivesPath() {
        return path.join(CACHE_DIR_ROOT, 'instagram-live-published.json')
    }

    private loadPublishedArchives() {
        if (this.publishedArchivesLoaded) {
            return
        }
        this.publishedArchivesLoaded = true
        try {
            if (fs.existsSync(this.publishedArchivesPath)) {
                const parsed = JSON.parse(fs.readFileSync(this.publishedArchivesPath, 'utf8'))
                for (const key of Array.isArray(parsed?.published) ? parsed.published : []) {
                    if (typeof key === 'string') {
                        this.publishedArchives.add(key)
                    }
                }
            }
        } catch {
            // A corrupt ledger must not block publishing; it only risks a duplicate upload.
        }
    }

    private persistPublishedArchives() {
        try {
            ensureDirectoryExists(path.dirname(this.publishedArchivesPath))
            fs.writeFileSync(this.publishedArchivesPath, JSON.stringify({ published: [...this.publishedArchives] }, null, 2), 'utf8')
        } catch {
            // Non-fatal: the in-memory set still dedupes this process.
        }
    }

    private archivePublishKey(session: Pick<InstagramLiveRecordingSession, 'broadcastId' | 'mediaPath' | 'startedAt'>) {
        // Dedup by broadcast: a mid-live recorder restart produces a new mediaPath/startedAt for the same
        // broadcast and must not publish a second partial video.
        return session.broadcastId ? `broadcast:${session.broadcastId}` : `${session.mediaPath}:${session.startedAt}`
    }

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
        const previousLastLiveAt = resolveLastLiveAt(previousCache)

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
                lastLiveAt: status.is_live ? now : previousLastLiveAt,
                package: previousCache?.package || null,
                archive: previousCache?.archive || null,
                syncedAt: previousCache?.syncedAt || null,
                relay: previousCache?.relay,
                lastError: null,
            }

            const recoveredArchive = await this.recoverStaleActiveArchive(options.handle, previousCache, relayConfig, scopedLog)
            if (recoveredArchive) {
                nextCache.archive = recoveredArchive
            }

            if (!status.is_live || !status.live_url) {
                const finishedArchive = await this.finishRecordingIfNeeded(options.handle, previousCache, scopedLog)
                if (finishedArchive) {
                    nextCache.archive = finishedArchive
                }

                const postLivePackage = await this.refreshPostLivePackage(previousCache, relayConfig, scopedLog)
                if (postLivePackage) {
                    nextCache.package = postLivePackage

                    const shouldSyncPostLive =
                        !previousCache?.relay?.active
                        || !previousCache?.syncedAt
                        || Date.now() - new Date(previousCache.syncedAt).getTime() >= relayConfig.sync_interval_seconds * 1000

                    if (shouldSyncPostLive && relayConfig.relay_enabled) {
                        const relayResponse = await this.syncRelay(relayConfig, postLivePackage, {
                            title: relayConfig.player_name || `【IG Live】${status.username || previousCache?.displayName || options.handle}`,
                            coverUrl: status.u_avatar || previousCache?.avatarUrl || undefined,
                            description: `Instagram Live relay for ${status.username || previousCache?.displayName || options.handle}`,
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
                }

                if (relayConfig.relay_enabled && relayConfig.stop_offline && previousCache?.relay?.active) {
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
                    userId: status.numeric_id,
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

            const recordingState = this.ensureRecording({
                handle: options.handle,
                profileUrl: options.profileUrl,
                liveUrl: status.live_url,
                broadcastId: status.live_broadcast_id,
                displayName: status.username || options.handle,
                avatarUrl: status.u_avatar,
                package: packageToSync,
                relayConfig,
                log: scopedLog,
            })
            if (recordingState) {
                nextCache.archive = recordingState
            }

            const shouldSync =
                shouldCapture
                || !previousCache?.relay?.active
                || !previousCache?.syncedAt
                || Date.now() - new Date(previousCache.syncedAt).getTime() >= relayConfig.sync_interval_seconds * 1000

            if (shouldSync && relayConfig.relay_enabled) {
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
                lastLiveAt: previousLastLiveAt,
                package: previousCache?.package || null,
                archive: previousCache?.archive || null,
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

        const archiveConfig = resolveInstagramArchiveConfig(handleConfig, liveRelay)
        const publishConfig = resolveInstagramPublishConfig(handleConfig, liveRelay)
        const merged: LiveRelayResolution = {
            ...liveRelay,
            ...handleConfig,
            archive: archiveConfig,
            publish: publishConfig,
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
                resolveNonNegativeSeconds(
                    handleConfig?.sync_interval_seconds
                    ?? liveRelay.sync_interval_seconds
                    ?? process.env.LIVE_PLAYER_SYNC_INTERVAL_SECONDS,
                    DEFAULT_SYNC_INTERVAL_SECONDS,
                ),
            ),
            post_live_grace_seconds: Math.max(
                0,
                resolveNonNegativeSeconds(
                    handleConfig?.post_live_grace_seconds
                    ?? liveRelay.post_live_grace_seconds
                    ?? process.env.LIVE_PLAYER_POST_LIVE_GRACE_SECONDS,
                    DEFAULT_POST_LIVE_GRACE_SECONDS,
                ),
            ),
            stop_offline: Boolean(handleConfig?.stop_offline ?? liveRelay.stop_offline),
            relay_enabled: Boolean(handleConfig?.relay_enabled ?? liveRelay.relay_enabled ?? true),
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

    private getRecordingKey(handle: string, broadcastId?: string | null) {
        return `${handle}:${broadcastId || 'live'}`
    }

    private selectArchiveStream(pkg: EchoPackage) {
        return pkg.streams.find((stream) => stream.type === 'HLS' && !stream.mediaInfo.encrypted) || pkg.streams.find((stream) => !stream.mediaInfo.encrypted) || null
    }

    private buildArchivePaths(options: {
        handle: string
        displayName: string
        broadcastId: string | null
        archiveConfig: ResolvedInstagramLiveArchiveConfig
    }) {
        const now = formatDateParts(Date.now())
        const safeHandle = sanitizeFileSegment(options.handle, 'instagram')
        const safeName = sanitizeFileSegment(options.displayName, safeHandle)
        const dir = path.join(options.archiveConfig.root_dir, now.date, safeHandle)
        ensureDirectoryExists(dir)
        const idSuffix = options.broadcastId ? `-${sanitizeFileSegment(options.broadcastId, 'live').slice(0, 24)}` : ''
        const stem = sanitizeFileSegment(`${now.time}-${safeName}${idSuffix}`, `${now.time}-${safeHandle}`)
        return {
            dir,
            mediaPath: path.join(dir, `${stem}.${options.archiveConfig.extension}`),
            manifestPath: path.join(dir, `${stem}.archive.json`),
            diagnosticsPath: path.join(dir, `${stem}.diagnostics.jsonl`),
            stderrLogPath: path.join(dir, `${stem}.ffmpeg.stderr.log`),
            stdoutLogPath: path.join(dir, `${stem}.ffmpeg.stdout.log`),
        }
    }

    private ensureRecording(options: {
        handle: string
        profileUrl: string
        liveUrl: string
        broadcastId: string | null
        displayName: string
        avatarUrl: string | null
        package: EchoPackage
        relayConfig: LiveRelayResolution
        log?: Logger
    }): InstagramLiveArchiveState | null {
        const archiveConfig = options.relayConfig.archive
        if (!archiveConfig) {
            return null
        }
        const stream = this.selectArchiveStream(options.package)
        if (!stream) {
            return {
                active: false,
                broadcastId: options.broadcastId,
                mediaPath: null,
                manifestPath: null,
                startedAt: null,
                lastError: 'No unencrypted Instagram live stream was available for archiving.',
            }
        }
        const key = this.getRecordingKey(options.handle, options.broadcastId)
        const existing = this.recordingSessions.get(key)
        if (existing) {
            return {
                active: true,
                broadcastId: existing.broadcastId,
                mediaPath: existing.mediaPath,
                manifestPath: existing.manifestPath,
                diagnosticsPath: existing.diagnosticsPath,
                stderrLogPath: existing.stderrLogPath,
                stdoutLogPath: existing.stdoutLogPath,
                startedAt: existing.startedAt,
            }
        }

        const paths = this.buildArchivePaths({
            handle: options.handle,
            displayName: options.displayName,
            broadcastId: options.broadcastId,
            archiveConfig,
        })
        const startedAt = new Date().toISOString()
        const stopAtLimit = archiveConfig.stop_at_epoch
            ? Math.max(1, archiveConfig.stop_at_epoch - Math.floor(Date.now() / 1000))
            : archiveConfig.max_duration_seconds
        const captureDurationSeconds = Math.min(archiveConfig.max_duration_seconds, stopAtLimit)
        const args = [
            '-y',
            ...streamHeaderArgs(stream.headers),
            '-i',
            stream.source,
            '-t',
            String(captureDurationSeconds),
            '-map',
            '0',
            '-c',
            'copy',
        ]
        if (archiveConfig.extension === 'mp4') {
            args.push('-movflags', '+faststart')
        }
        args.push(paths.mediaPath)
        const child = spawn(archiveConfig.ffmpeg_path, args, {
            env: {
                ...process.env,
            },
        })
        const session: InstagramLiveRecordingSession = {
            handle: options.handle,
            broadcastId: options.broadcastId,
            liveUrl: options.liveUrl,
            profileUrl: options.profileUrl,
            displayName: options.displayName,
            avatarUrl: options.avatarUrl,
            startedAt,
            mediaPath: paths.mediaPath,
            manifestPath: paths.manifestPath,
            diagnosticsPath: paths.diagnosticsPath,
            stderrLogPath: paths.stderrLogPath,
            stdoutLogPath: paths.stdoutLogPath,
            stream,
            process: child,
            stderr: [],
            stdout: [],
            archiveConfig,
            publishConfig: options.relayConfig.publish,
        }
        this.recordingSessions.set(key, session)
        this.appendArchiveDiagnostic(session, 'recorder_started', {
            handle: options.handle,
            broadcastId: options.broadcastId,
            liveUrl: options.liveUrl,
            mediaPath: paths.mediaPath,
            manifestPath: paths.manifestPath,
            streamType: stream.type,
            streamSourceHash: createHash('sha256').update(stream.source).digest('hex').slice(0, 16),
            variants: stream.mediaInfo.variants_count,
            encrypted: stream.mediaInfo.encrypted,
            captureDurationSeconds,
            stopAtEpoch: archiveConfig.stop_at_epoch || null,
        })
        child.stdout.on('data', (chunk) => {
            const text = chunk.toString()
            session.stdout.push(text)
            fs.appendFileSync(session.stdoutLogPath, text, 'utf8')
        })
        child.stderr.on('data', (chunk) => {
            const text = chunk.toString()
            session.stderr.push(text)
            fs.appendFileSync(session.stderrLogPath, text, 'utf8')
        })
        child.on('close', () => {
            this.finalizeRecordingSession(key, session, options.log).catch((error) => {
                options.log?.warn(`Instagram live archive finalize failed for ${options.handle}: ${error}`)
            })
        })
        child.on('error', (error) => {
            options.log?.warn(`Instagram live archive recorder failed for ${options.handle}: ${error}`)
        })
        options.log?.info(`Instagram live archive recorder started for ${options.handle}: ${paths.mediaPath}`)
        return {
            active: true,
            broadcastId: options.broadcastId,
            mediaPath: paths.mediaPath,
            manifestPath: paths.manifestPath,
            diagnosticsPath: paths.diagnosticsPath,
            stderrLogPath: paths.stderrLogPath,
            stdoutLogPath: paths.stdoutLogPath,
            startedAt,
        }
    }

    private async finishRecordingIfNeeded(
        handle: string,
        previousCache: InstagramLiveCacheEntry | null,
        log?: Logger,
    ): Promise<InstagramLiveArchiveState | null> {
        const candidates = Array.from(this.recordingSessions.entries()).filter(([key]) => key.startsWith(`${handle}:`))
        if (candidates.length === 0) {
            return previousCache?.archive || null
        }
        const [, session] = candidates[0]!
        if (!session.process.killed) {
            session.process.kill('SIGINT')
        }
        return {
            active: true,
            broadcastId: session.broadcastId,
            mediaPath: session.mediaPath,
            manifestPath: session.manifestPath,
            diagnosticsPath: session.diagnosticsPath,
            stderrLogPath: session.stderrLogPath,
            stdoutLogPath: session.stdoutLogPath,
            startedAt: session.startedAt,
        }
    }

    private async recoverStaleActiveArchive(
        handle: string,
        previousCache: InstagramLiveCacheEntry | null,
        relayConfig: LiveRelayResolution,
        log?: Logger,
    ): Promise<InstagramLiveArchiveState | null> {
        const stale = previousCache?.archive
        if (!stale?.active) {
            return null
        }
        // A live in-memory session owns this archive; only recover after a process restart orphaned it.
        const hasLiveSession = Array.from(this.recordingSessions.keys()).some((key) => key.startsWith(`${handle}:`))
        if (hasLiveSession) {
            return null
        }
        const mediaPath = stale.mediaPath
        const exists = mediaPath ? fs.existsSync(mediaPath) : false
        const durationSeconds = exists && mediaPath ? this.probeDurationSeconds(mediaPath) : null
        const state: InstagramLiveArchiveState = {
            ...stale,
            active: false,
            completedAt: stale.completedAt || new Date().toISOString(),
            durationSeconds,
            lastError: 'Recorder lost to a process restart; archive finalized from disk state.',
        }
        log?.warn(`Recovering stale Instagram live archive for ${handle}: ${mediaPath || 'no media path'}`)
        if (exists && mediaPath && relayConfig.publish) {
            const pseudoSession = {
                handle,
                broadcastId: stale.broadcastId ?? null,
                liveUrl: previousCache?.liveUrl || '',
                profileUrl: previousCache?.profileUrl || '',
                displayName: previousCache?.displayName || handle,
                avatarUrl: previousCache?.avatarUrl || null,
                startedAt: stale.startedAt || new Date().toISOString(),
                mediaPath,
                publishConfig: relayConfig.publish,
                archiveConfig: relayConfig.archive,
            } as unknown as InstagramLiveRecordingSession
            try {
                const publishResult = await this.publishArchiveIfEnabled(pseudoSession, durationSeconds, log)
                if (publishResult) {
                    state.publishedAt = new Date().toISOString()
                    state.publishResult = publishResult
                }
            } catch (error) {
                state.lastError = `publish failed after stale recovery: ${error instanceof Error ? error.message : String(error)}`
                log?.warn(`Instagram live stale archive publish failed for ${handle}: ${error}`)
            }
        }
        return state
    }

    private probeDurationSeconds(filePath: string) {
        try {
            const output = execFileSync('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', filePath], {
                encoding: 'utf8',
            })
            const duration = Number(output.trim())
            return Number.isFinite(duration) && duration > 0 ? duration : null
        } catch {
            return null
        }
    }

    private appendArchiveDiagnostic(session: InstagramLiveRecordingSession, event: string, payload: Record<string, unknown>) {
        fs.appendFileSync(
            session.diagnosticsPath,
            `${JSON.stringify({ ts: new Date().toISOString(), event, ...payload })}\n`,
            'utf8',
        )
    }

    private writeArchiveManifest(session: InstagramLiveRecordingSession, state: InstagramLiveArchiveState) {
        const stats = fs.existsSync(session.mediaPath) ? fs.statSync(session.mediaPath) : null
        const title = `IG Live ${session.displayName} ${formatDateParts(Date.parse(session.startedAt)).shortDate}`
        const manifest = {
            version: 1,
            visible: true,
            id: archiveIdFor(session.mediaPath, session.startedAt),
            kind: 'relay-session',
            title,
            fileName: path.basename(session.mediaPath),
            fileExtension: path.extname(session.mediaPath).toLowerCase(),
            localPath: session.mediaPath,
            mediaPath: session.mediaPath,
            containerPath: path.dirname(session.mediaPath),
            manifestPath: session.manifestPath,
            diagnosticsPath: session.diagnosticsPath,
            stderrLogPath: session.stderrLogPath,
            stdoutLogPath: session.stdoutLogPath,
            sizeBytes: stats?.size || 0,
            modifiedAt: stats?.mtime.toISOString() || new Date().toISOString(),
            createdAt: stats?.birthtime.toISOString() || session.startedAt,
            category: 'instagram-live',
            rootLabel: 'Instagram Live',
            session: {
                pid: session.handle,
                name: `IG Live | ${session.displayName}`,
                reason: 'instagram-live-archive',
                archived_at: state.completedAt || new Date().toISOString(),
                session_started_at: session.startedAt,
                source: session.stream.source,
                page_url: session.liveUrl || session.profileUrl,
            },
        }
        fs.writeFileSync(session.manifestPath, JSON.stringify(manifest, null, 2), 'utf8')
    }

    private buildArchiveArticle(session: InstagramLiveRecordingSession, durationSeconds: number | null): Article {
        const date = formatDateParts(Date.parse(session.startedAt))
        const durationLine = durationSeconds ? `录制时长约 ${Math.round(durationSeconds / 60)} 分钟` : 'Instagram Live 存档'
        return {
            platform: Platform.Instagram,
            a_id: `ig-live-${session.handle}-${Date.parse(session.startedAt)}`,
            u_id: session.handle,
            username: session.displayName,
            created_at: Math.floor(Date.parse(session.startedAt) / 1000),
            content: `Instagram Live ${session.displayName} ${date.shortDate}\n${durationLine}`,
            url: session.liveUrl || session.profileUrl,
            type: 'story' as any,
            ref: null,
            has_media: true,
            media: [{ type: 'video', url: session.mediaPath }],
            extra: {
                data: {
                    kind: 'instagram_live_archive',
                    broadcast_id: session.broadcastId,
                    archive_path: session.mediaPath,
                    manifest_path: session.manifestPath,
                },
            } as any,
            u_avatar: session.avatarUrl,
        }
    }

    private async publishArchiveIfEnabled(session: InstagramLiveRecordingSession, durationSeconds: number | null, log?: Logger) {
        if (!session.publishConfig || durationSeconds === null || durationSeconds < session.archiveConfig.min_publish_duration_seconds) {
            return null
        }
        this.loadPublishedArchives()
        const publishKey = this.archivePublishKey(session)
        if (this.publishedArchives.has(publishKey)) {
            return null
        }
        const article = this.buildArchiveArticle(session, durationSeconds)
        const candidate = buildBiliupUploadCandidate(
            article,
            [article.content || ''],
            [{ media_type: 'video', path: session.mediaPath }],
            session.publishConfig.video_upload,
        ) as BiliupUploadCandidate | null
        if (!candidate) {
            return null
        }
        await completeBiliupUploadCandidateTags(article, [article.content || ''], candidate, log)
        const result = await runBiliupUpload(
            article,
            candidate,
            {
                sessdata: session.publishConfig.sessdata,
                bili_jct: session.publishConfig.bili_jct,
            },
            log,
        )
        // Mark published only after a confirmed upload so a failed attempt stays retryable; persist so a
        // process restart cannot re-publish the same broadcast.
        this.publishedArchives.add(publishKey)
        this.persistPublishedArchives()
        return result
    }

    private async finalizeRecordingSession(key: string, session: InstagramLiveRecordingSession, log?: Logger) {
        this.recordingSessions.delete(key)
        const exists = fs.existsSync(session.mediaPath)
        const stats = exists ? fs.statSync(session.mediaPath) : null
        const durationSeconds = exists ? this.probeDurationSeconds(session.mediaPath) : null
        const completedAt = new Date().toISOString()
        const state: InstagramLiveArchiveState = {
            active: false,
            broadcastId: session.broadcastId,
            mediaPath: session.mediaPath,
            manifestPath: session.manifestPath,
            startedAt: session.startedAt,
            completedAt,
            durationSeconds,
            sizeBytes: stats?.size || 0,
            lastError: exists && stats && stats.size > 0 ? null : 'Instagram live archive did not produce a media file.',
        }
        this.appendArchiveDiagnostic(session, 'recorder_finished', {
            mediaPath: session.mediaPath,
            manifestPath: session.manifestPath,
            exists,
            sizeBytes: stats?.size || 0,
            durationSeconds,
            stderrBytes: session.stderr.join('').length,
            stdoutBytes: session.stdout.join('').length,
            lastError: state.lastError || null,
        })
        this.writeArchiveManifest(session, state)
        try {
            const publishResult = await this.publishArchiveIfEnabled(session, durationSeconds, log)
            if (publishResult) {
                state.publishedAt = new Date().toISOString()
                state.publishResult = publishResult
                this.appendArchiveDiagnostic(session, 'publish_finished', {
                    publishedAt: state.publishedAt,
                    stdoutBytes: String((publishResult as any).stdout || '').length,
                })
                this.writeArchiveManifest(session, state)
            }
        } catch (error) {
            state.lastError = `publish failed: ${error instanceof Error ? error.message : String(error)}`
            this.appendArchiveDiagnostic(session, 'publish_failed', {
                error: error instanceof Error ? error.message : String(error),
            })
            this.writeArchiveManifest(session, state)
            log?.warn(`Instagram live archive publish failed for ${session.handle}: ${error}`)
        }
        const cache = this.readCache(session.handle)
        if (cache) {
            cache.archive = state
            this.writeCache(session.handle, cache)
        }
        log?.info(`Instagram live archive finalized for ${session.handle}: ${session.mediaPath}`)
    }

    private async captureEchoPackage({
        page,
        profileUrl,
        liveUrl,
        userId,
        cookieString,
        requestHeaders,
        log,
    }: {
        page: Page
        profileUrl: string
        liveUrl: string
        userId?: string | null
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
            const responseRequestHeaders = filterRelayHeaders(response.request().headers())
            const headers = this.mergeCaptureHeaders(baseHeaders, cookieEntries, responseRequestHeaders, liveUrl)

            if (isStreamManifest(source)) {
                analysisTasks.push(this.registerStream(capturedStreams, source, headers, log, response.text()))
                return
            }

            if (isLiveWebInfoResponse(source)) {
                analysisTasks.push(
                    (async () => {
                        try {
                            const text = await response.text()
                            const liveWebInfo = JSON.parse(text)
                            await this.captureStreamsFromLiveWebInfo(capturedStreams, liveWebInfo, headers, log)
                        } catch (error) {
                            log?.warn(`Failed to parse Instagram live web_info ${source}: ${error}`)
                        }
                    })(),
                )
            }
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

            if (capturedStreams.size === 0 && userId) {
                try {
                    const liveWebInfo = await this.fetchLiveWebInfo(page, liveUrl, userId)
                    const directHeaders = this.mergeCaptureHeaders(baseHeaders, cookieEntries, {
                        accept: '*/*',
                        'x-requested-with': 'XMLHttpRequest',
                    }, liveUrl)
                    analysisTasks.push(this.captureStreamsFromLiveWebInfo(capturedStreams, liveWebInfo, directHeaders, log))
                } catch (error) {
                    log?.warn(`Instagram live web_info fallback failed for ${userId}: ${error}`)
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

    private mergeCaptureHeaders(
        baseHeaders: Record<string, string>,
        cookieEntries: Record<string, string>,
        responseHeaders?: Record<string, string>,
        referer?: string,
    ) {
        const headers = {
            ...baseHeaders,
            ...(responseHeaders || {}),
        }
        if (!headers.referer && referer) {
            headers.referer = referer
        }
        if (!headers.origin) {
            headers.origin = INSTAGRAM_ORIGIN
        }
        if (!headers.cookie) {
            const cookieHeader = buildCookieHeader(cookieEntries)
            if (cookieHeader) {
                headers.cookie = cookieHeader
            }
        }
        return headers
    }

    private async registerStream(
        capturedStreams: Map<string, EchoStreamRecord>,
        source: string,
        headers: Record<string, string>,
        log?: Logger,
        manifestTextPromise?: Promise<string>,
    ) {
        if (capturedStreams.has(source)) {
            return
        }

        const record: EchoStreamRecord = {
            source,
            type: source.includes('.mpd') ? 'DASH' : 'HLS',
            headers: { ...headers },
            mediaInfo: createEmptyMediaInfo(),
        }
        capturedStreams.set(source, record)

        try {
            const text = manifestTextPromise ? await manifestTextPromise : await this.fetchManifestText(source, record.headers)
            record.mediaInfo = analyzeManifestText(source, text)
        } catch (error) {
            log?.warn(`Failed to analyze live manifest ${source}: ${error}`)
        }
    }

    private async captureStreamsFromLiveWebInfo(
        capturedStreams: Map<string, EchoStreamRecord>,
        liveWebInfo: any,
        headers: Record<string, string>,
        log?: Logger,
    ) {
        const parsed = parseInstagramLiveWebInfo(liveWebInfo)
        if (parsed.streamUrls.length === 0) {
            return
        }

        await Promise.all(
            parsed.streamUrls.map((streamUrl) => this.registerStream(capturedStreams, streamUrl, headers, log)),
        )
    }

    private async fetchLiveWebInfo(page: Page, liveUrl: string, userId: string) {
        await page.goto(liveUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 30000,
        })
        const html = await page.content().catch(() => '')
        const appId = extractInstagramAppIdFromHtml(html)

        const result = await page.evaluate(async (targetUserId, igAppId) => {
            const response = await fetch(`/api/v1/live/web_info/?target_user_id=${encodeURIComponent(targetUserId)}`, {
                credentials: 'include',
                headers: {
                    accept: '*/*',
                    'x-requested-with': 'XMLHttpRequest',
                    'x-ig-app-id': igAppId,
                },
            })
            return {
                ok: response.ok,
                status: response.status,
                text: await response.text(),
            }
        }, userId, appId)

        if (!result.ok) {
            throw new Error(`HTTP ${result.status}: ${result.text}`)
        }

        return JSON.parse(result.text)
    }

    private async fetchManifestText(source: string, headers: Record<string, string>) {
        const response = await fetchWithTimeout(source, {
            headers,
        }, MANIFEST_FETCH_TIMEOUT_MS)
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`)
        }
        return await response.text()
    }

    private async refreshPostLivePackage(
        previousCache: InstagramLiveCacheEntry | null,
        relayConfig: LiveRelayResolution,
        log?: Logger,
    ) {
        const lastLiveAt = resolveLastLiveAt(previousCache)
        const previousPackage = previousCache?.package
        if (!previousPackage || previousPackage.streams_detected === 0) {
            return null
        }
        if (!isPostLiveGraceActive(lastLiveAt, relayConfig.post_live_grace_seconds)) {
            return null
        }

        const refreshedStreams = (
            await Promise.all(
                previousPackage.streams.map(async (stream) => {
                    try {
                        const text = await this.fetchManifestText(stream.source, stream.headers)
                        return {
                            ...stream,
                            mediaInfo: analyzeManifestText(stream.source, text),
                        }
                    } catch (error) {
                        log?.warn(`Post-live relay manifest expired for ${stream.source}: ${error}`)
                        return null
                    }
                }),
            )
        ).filter((stream): stream is EchoStreamRecord => Boolean(stream))

        if (refreshedStreams.length === 0) {
            return null
        }

        return {
            ...previousPackage,
            timestamp: Date.now(),
            streams_detected: refreshedStreams.length,
            streams: refreshedStreams.sort((a, b) => {
                return (b.mediaInfo?.variants_count || 0) - (a.mediaInfo?.variants_count || 0)
            }),
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
        applyN2njRequestIdentity(headers)
        applyWafBypassHeader(headers, relayConfig.waf_bypass_header)

        const response = await fetchWithTimeout(`${relayConfig.live_player_url}/api/players`, {
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
        applyN2njRequestIdentity(headers)
        applyWafBypassHeader(headers, relayConfig.waf_bypass_header)

        const response = await fetchWithTimeout(`${relayConfig.live_player_url}/api/auth/login`, {
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
        applyN2njRequestIdentity(headers)
        applyWafBypassHeader(headers, relayConfig.waf_bypass_header)

        const response = await fetchWithTimeout(
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
    N2NJ_REQUEST_USER_AGENT,
    InstagramLiveRelayService,
    analyzeManifestText,
    buildPlayerUrl,
    filterRelayHeaders,
    isPostLiveGraceActive,
    parseInstagramLiveWebInfo,
    parseCookieString,
}
export type { EchoPackage, InstagramLiveCacheEntry }
