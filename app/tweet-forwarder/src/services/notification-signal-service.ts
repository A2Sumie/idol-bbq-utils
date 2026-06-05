import crypto from 'crypto'
import type { AppConfig, Crawler } from '@/types'
import { sanitizeWebsites } from '@/utils/base'
import { spiderRegistry } from '@idol-bbq-utils/spider'
import { Platform } from '@idol-bbq-utils/spider/types'

type NotificationSignalMode = 'shadow' | 'disabled'
type NotificationSignalPlatform = 'x' | 'instagram' | 'tiktok' | 'bilibili' | 'youtube' | 'website' | 'unknown'

type NotificationSignalInput = {
    platform?: string
    type?: string
    eventKey?: string
    notificationId?: string
    postId?: string
    url?: string
    u_id?: string
    userId?: string
    sourceUserId?: string
    username?: string
    screenName?: string
    crawlerName?: string
    crawlerId?: string
    crawlerNames?: Array<string>
    crawlerIds?: Array<string>
    title?: string
    body?: string
    text?: string
    received_at?: number | string
}

type NotificationSignalCrawlerMatch = {
    crawler_id: string
    crawler_name: string
    reason: 'explicit' | 'url' | 'identity'
}

type NotificationSignalRecord = {
    schema_version: 1
    mode: NotificationSignalMode
    platform: NotificationSignalPlatform
    event_key: string
    source_ref: string
    received_at: number
    notification: {
        type?: string
        notification_id?: string
        post_id?: string
        url?: string
        source_user_id?: string
        username?: string
        title_hash?: string
        title_length?: number
        body_hash?: string
        body_length?: number
        text_hash?: string
        text_length?: number
    }
    matched_crawlers: Array<NotificationSignalCrawlerMatch>
    would_trigger_crawlers: false
}

type NotificationSignalBuildOptions = {
    now?: number
}

function stableSerialize(value: unknown): string {
    if (value === null) {
        return 'null'
    }
    if (Array.isArray(value)) {
        return `[${value.map((item) => stableSerialize(item)).join(',')}]`
    }
    if (typeof value === 'object') {
        const objectValue = value as Record<string, unknown>
        return `{${Object.keys(objectValue)
            .sort()
            .filter((key) => objectValue[key] !== undefined)
            .map((key) => `${JSON.stringify(key)}:${stableSerialize(objectValue[key])}`)
            .join(',')}}`
    }
    return JSON.stringify(value) ?? 'undefined'
}

function hashStable(value: unknown) {
    return crypto.createHash('sha256').update(stableSerialize(value)).digest('hex')
}

function hashText(value: string) {
    return crypto.createHash('sha256').update(value).digest('hex')
}

function cleanString(value: unknown) {
    const text = String(value ?? '').trim()
    return text || undefined
}

function normalizeIdentity(value: unknown) {
    return cleanString(value)
        ?.replace(/^@+/, '')
        .replace(/^\/+|\/+$/g, '')
        .toLowerCase()
}

function normalizeSignalPlatform(value: unknown): NotificationSignalPlatform {
    const normalized = String(value || '')
        .trim()
        .toLowerCase()
        .replace(/^twitter$/, 'x')
        .replace(/^ig$/, 'instagram')
        .replace(/^ins$/, 'instagram')
        .replace(/^tt$/, 'tiktok')
        .replace(/^bilibili$/, 'bilibili')
        .replace(/^b站$/, 'bilibili')
    if (['x', 'instagram', 'tiktok', 'bilibili', 'youtube', 'website'].includes(normalized)) {
        return normalized as NotificationSignalPlatform
    }
    return 'unknown'
}

function platformFromSpider(platform?: Platform | null): NotificationSignalPlatform {
    switch (platform) {
        case Platform.X:
            return 'x'
        case Platform.Instagram:
            return 'instagram'
        case Platform.TikTok:
            return 'tiktok'
        case Platform.YouTube:
            return 'youtube'
        case Platform.Website:
            return 'website'
        default:
            return 'unknown'
    }
}

function safeNotificationUrl(rawUrl: unknown) {
    const value = cleanString(rawUrl)
    if (!value) {
        return undefined
    }
    try {
        const parsed = new URL(value)
        parsed.username = ''
        parsed.password = ''
        parsed.search = ''
        parsed.hash = ''
        return parsed.toString().replace(/\/$/, '')
    } catch {
        return undefined
    }
}

function resolveReceivedAt(value: unknown, now: number) {
    if (typeof value === 'number' && Number.isFinite(value)) {
        return Math.floor(value > 1_000_000_000_000 ? value / 1000 : value)
    }
    const text = cleanString(value)
    if (!text) {
        return now
    }
    const numeric = Number(text)
    if (Number.isFinite(numeric)) {
        return Math.floor(numeric > 1_000_000_000_000 ? numeric / 1000 : numeric)
    }
    const parsed = Date.parse(text)
    return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : now
}

function textMetadata(key: 'title' | 'body' | 'text', value: unknown) {
    const text = cleanString(value)
    if (!text) {
        return {}
    }
    return {
        [`${key}_hash`]: hashText(text),
        [`${key}_length`]: text.length,
    }
}

function nodeId(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.id || value?.name || fallback).trim()
}

function nodeName(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.name || value?.id || fallback).trim()
}

function explicitCrawlerKeys(input: NotificationSignalInput) {
    return new Set(
        [
            input.crawlerId,
            input.crawlerName,
            ...(Array.isArray(input.crawlerIds) ? input.crawlerIds : []),
            ...(Array.isArray(input.crawlerNames) ? input.crawlerNames : []),
        ]
            .map((value) => cleanString(value))
            .filter((value): value is string => Boolean(value)),
    )
}

function collectCrawlerTargets(crawler: Crawler) {
    return sanitizeWebsites({
        websites: crawler.websites,
        origin: crawler.origin,
        paths: crawler.paths,
    })
        .map((url) => {
            const info = spiderRegistry.extractBasicInfo(url)
            return {
                url: safeNotificationUrl(url),
                platform: platformFromSpider(info?.platform ?? null),
                identity: normalizeIdentity(info?.u_id),
            }
        })
        .filter((target) => target.url || target.identity)
}

function addMatch(
    matches: Map<string, NotificationSignalCrawlerMatch>,
    crawler: Crawler,
    fallback: string,
    reason: NotificationSignalCrawlerMatch['reason'],
) {
    const crawlerId = nodeId(crawler, fallback)
    const existing = matches.get(crawlerId)
    if (existing && existing.reason === 'explicit') {
        return
    }
    matches.set(crawlerId, {
        crawler_id: crawlerId,
        crawler_name: nodeName(crawler, crawlerId),
        reason,
    })
}

function matchNotificationSignalCrawlers(config: AppConfig, input: NotificationSignalInput) {
    const explicitKeys = explicitCrawlerKeys(input)
    const signalPlatform = normalizeSignalPlatform(input.platform)
    const signalUrl = safeNotificationUrl(input.url)
    const signalUrlInfo = signalUrl ? spiderRegistry.extractBasicInfo(signalUrl) : null
    const identities = new Set(
        [
            normalizeIdentity(input.u_id),
            normalizeIdentity(input.userId),
            normalizeIdentity(input.sourceUserId),
            normalizeIdentity(input.username),
            normalizeIdentity(input.screenName),
            normalizeIdentity(signalUrlInfo?.u_id),
        ].filter((value): value is string => Boolean(value)),
    )
    const matches = new Map<string, NotificationSignalCrawlerMatch>()

    for (const [index, crawler] of (config.crawlers || []).entries()) {
        const fallback = `crawler-${index}`
        const crawlerId = nodeId(crawler, fallback)
        const crawlerName = nodeName(crawler, crawlerId)
        if (explicitKeys.has(crawlerId) || explicitKeys.has(crawlerName)) {
            addMatch(matches, crawler, fallback, 'explicit')
            continue
        }

        for (const target of collectCrawlerTargets(crawler)) {
            if (signalUrl && target.url === signalUrl) {
                addMatch(matches, crawler, fallback, 'url')
                break
            }
            if (
                target.identity &&
                identities.has(target.identity) &&
                (signalPlatform === 'unknown' || target.platform === signalPlatform)
            ) {
                addMatch(matches, crawler, fallback, 'identity')
                break
            }
        }
    }

    return Array.from(matches.values()).sort(
        (left, right) =>
            left.reason.localeCompare(right.reason) || left.crawler_id.localeCompare(right.crawler_id),
    )
}

function resolveNotificationSignalMode(config: AppConfig): NotificationSignalMode {
    const raw = String((config as any).notification_signals?.mode || 'shadow')
        .trim()
        .toLowerCase()
        .replace(/_/g, '-')
    if (raw === 'disabled' || raw === 'off') {
        return 'disabled'
    }
    return 'shadow'
}

function buildNotificationEventKey(
    input: NotificationSignalInput,
    normalized: {
        platform: NotificationSignalPlatform
        type?: string
        notificationId?: string
        postId?: string
        url?: string
        sourceUserId?: string
        username?: string
        receivedAt: number
    },
) {
    const explicit = cleanString(input.eventKey)
    if (explicit) {
        return explicit.slice(0, 160)
    }
    const contentIdentity =
        normalized.notificationId ||
        normalized.postId ||
        normalized.url ||
        [normalized.sourceUserId, normalized.username, normalized.type].filter(Boolean).join(':')
    const fallbackBucket = Math.floor(normalized.receivedAt / 300)
    return hashStable({
        platform: normalized.platform,
        type: normalized.type || '',
        contentIdentity: contentIdentity || '',
        fallbackBucket,
    })
}

function buildNotificationSignalRecord(
    config: AppConfig,
    input: NotificationSignalInput,
    options: NotificationSignalBuildOptions = {},
): NotificationSignalRecord {
    const now = options.now ?? Math.floor(Date.now() / 1000)
    const platform = normalizeSignalPlatform(input.platform)
    const receivedAt = resolveReceivedAt(input.received_at, now)
    const url = safeNotificationUrl(input.url)
    const notificationId = cleanString(input.notificationId)
    const postId = cleanString(input.postId)
    const sourceUserId = normalizeIdentity(input.sourceUserId || input.userId || input.u_id)
    const username = normalizeIdentity(input.username || input.screenName)
    const type = cleanString(input.type)
    const eventKey = buildNotificationEventKey(input, {
        platform,
        type,
        notificationId,
        postId,
        url,
        sourceUserId,
        username,
        receivedAt,
    })
    const sourceRefIdentity = sourceUserId || username || postId || notificationId || eventKey.slice(0, 24)

    return {
        schema_version: 1,
        mode: resolveNotificationSignalMode(config),
        platform,
        event_key: eventKey,
        source_ref: `notification:${platform}:${sourceRefIdentity}`,
        received_at: receivedAt,
        notification: {
            ...(type ? { type } : {}),
            ...(notificationId ? { notification_id: notificationId } : {}),
            ...(postId ? { post_id: postId } : {}),
            ...(url ? { url } : {}),
            ...(sourceUserId ? { source_user_id: sourceUserId } : {}),
            ...(username ? { username } : {}),
            ...textMetadata('title', input.title),
            ...textMetadata('body', input.body),
            ...textMetadata('text', input.text),
        },
        matched_crawlers: matchNotificationSignalCrawlers(config, input),
        would_trigger_crawlers: false,
    }
}

export {
    buildNotificationSignalRecord,
    matchNotificationSignalCrawlers,
    normalizeSignalPlatform,
    type NotificationSignalInput,
    type NotificationSignalMode,
    type NotificationSignalPlatform,
    type NotificationSignalRecord,
}
