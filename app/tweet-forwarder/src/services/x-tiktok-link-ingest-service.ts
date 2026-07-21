import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import type { Article } from '@/db'
import type { CrawlerConfig } from '@/types'

type MinimalLog = {
    info?: (...args: any[]) => void
    warn?: (...args: any[]) => void
}

type TikTokLinkInfo = {
    originalUrl: string
    resolvedUrl: string
    videoId?: string
    username?: string
    profileUrl?: string
}

type YouTubeLinkInfo = {
    originalUrl: string
    resolvedUrl: string
    videoId?: string
    watchUrl?: string
}

type EnqueueOptions = {
    crawlerConfig?: CrawlerConfig
    log?: MinimalLog
    fetchImpl?: typeof fetch
    now?: number
}

const TIKTOK_URL_RE = /https?:\/\/(?:www\.|vm\.|vt\.)?tiktok\.com\/[^\s<>"'，。！？、）)\]}]+/gi
const YOUTUBE_URL_RE = /https?:\/\/(?:(?:www|m)\.)?(?:youtube\.com|youtu\.be)\/[^\s<>"'，。！？、）)\]}]+/gi
const DEFAULT_TIKTOK_CRAWLER_NAME = 'Tiktok抓取'
const DEFAULT_YOUTUBE_CRAWLER_NAME = 'YouTube抓取'
const MAX_TIKTOK_LINKS_PER_X_ARTICLE = 5
const MAX_YOUTUBE_LINKS_PER_X_ARTICLE = 5
const TIKTOK_LINK_RESOLVE_TIMEOUT_MS = 10_000
const YOUTUBE_VIDEO_ID_RE = /^[A-Za-z0-9_-]{6,128}$/

function cleanExternalUrl(value: string) {
    return value.replace(/[.,!?;:，。！？、）)\]}]+$/g, '')
}

function cleanTikTokUrl(value: string) {
    return cleanExternalUrl(value)
}

function extractTikTokLinksFromText(text?: string | null): Array<string> {
    if (!text) {
        return []
    }
    return Array.from(new Set(Array.from(text.matchAll(TIKTOK_URL_RE)).map((match) => cleanTikTokUrl(match[0])))).slice(
        0,
        MAX_TIKTOK_LINKS_PER_X_ARTICLE,
    )
}

function extractYouTubeLinksFromText(text?: string | null): Array<string> {
    if (!text) {
        return []
    }
    return Array.from(new Set(Array.from(text.matchAll(YOUTUBE_URL_RE)).map((match) => cleanExternalUrl(match[0])))).slice(
        0,
        MAX_YOUTUBE_LINKS_PER_X_ARTICLE,
    )
}

function parseTikTokUrl(rawUrl: string): TikTokLinkInfo | null {
    let url: URL
    try {
        url = new URL(rawUrl)
    } catch {
        return null
    }

    const hostname = url.hostname.toLowerCase()
    if (hostname !== 'tiktok.com' && !hostname.endsWith('.tiktok.com')) {
        return null
    }

    const parts = url.pathname.split('/').filter(Boolean)
    const usernamePart = parts.find((part) => part.startsWith('@'))
    const username = usernamePart?.replace(/^@+/, '')
    const videoIndex = parts.findIndex((part) => part === 'video')
    const videoId = videoIndex >= 0 ? parts[videoIndex + 1]?.match(/^\d+$/)?.[0] : undefined
    const profileUrl = username ? `https://www.tiktok.com/@${username}` : undefined
    const resolvedUrl = videoId && username ? `https://www.tiktok.com/@${username}/video/${videoId}` : rawUrl

    return {
        originalUrl: rawUrl,
        resolvedUrl,
        videoId,
        username,
        profileUrl,
    }
}

function normalizeYouTubeVideoId(value?: string | null) {
    const videoId = String(value || '').trim()
    if (!YOUTUBE_VIDEO_ID_RE.test(videoId)) {
        return undefined
    }
    return videoId
}

function parseYouTubeUrl(rawUrl: string): YouTubeLinkInfo | null {
    let url: URL
    try {
        url = new URL(rawUrl)
    } catch {
        return null
    }

    const hostname = url.hostname.toLowerCase()
    if (hostname === 'youtu.be') {
        const videoId = normalizeYouTubeVideoId(url.pathname.split('/').filter(Boolean)[0])
        return {
            originalUrl: rawUrl,
            resolvedUrl: videoId ? `https://www.youtube.com/watch?v=${videoId}` : rawUrl,
            videoId,
            watchUrl: videoId ? `https://www.youtube.com/watch?v=${videoId}` : undefined,
        }
    }
    if (hostname !== 'youtube.com' && hostname !== 'www.youtube.com' && hostname !== 'm.youtube.com') {
        return null
    }

    const parts = url.pathname.split('/').filter(Boolean)
    let videoId = normalizeYouTubeVideoId(url.searchParams.get('v'))
    if (!videoId && ['shorts', 'live', 'embed'].includes(parts[0] || '')) {
        videoId = normalizeYouTubeVideoId(parts[1])
    }

    return {
        originalUrl: rawUrl,
        resolvedUrl: videoId ? `https://www.youtube.com/watch?v=${videoId}` : rawUrl,
        videoId,
        watchUrl: videoId ? `https://www.youtube.com/watch?v=${videoId}` : undefined,
    }
}

async function resolveTikTokLink(rawUrl: string, fetchImpl: typeof fetch = fetch): Promise<TikTokLinkInfo | null> {
    const direct = parseTikTokUrl(rawUrl)
    if (direct?.videoId && direct.profileUrl) {
        return direct
    }

    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), TIKTOK_LINK_RESOLVE_TIMEOUT_MS)
    try {
        const response = await fetchImpl(rawUrl, {
            method: 'GET',
            redirect: 'follow',
            signal: controller.signal,
            headers: {
                'user-agent':
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36',
            },
        })
        return parseTikTokUrl(response.url || rawUrl)
    } catch {
        return direct
    } finally {
        clearTimeout(timeout)
    }
}

function resolveTikTokCrawlerName(crawlerConfig?: CrawlerConfig) {
    const config = (crawlerConfig as any)?.x_tiktok_link_ingest
    if (config === false || config?.enabled === false) {
        return null
    }
    return String(config?.crawler || (crawlerConfig as any)?.tiktok_link_ingest_crawler || DEFAULT_TIKTOK_CRAWLER_NAME)
        .trim()
}

function resolveYouTubeCrawlerName(crawlerConfig?: CrawlerConfig) {
    const config = (crawlerConfig as any)?.x_youtube_link_ingest
    if (config === false || config?.enabled === false) {
        return null
    }
    return String(config?.crawler || (crawlerConfig as any)?.youtube_link_ingest_crawler || DEFAULT_YOUTUBE_CRAWLER_NAME)
        .trim()
}

async function enqueueMissingTikTokLinksFromXArticle(article: Article, options: EnqueueOptions = {}) {
    if (article.platform !== Platform.X && article.platform !== Platform.Twitter) {
        return []
    }

    const crawlerName = resolveTikTokCrawlerName(options.crawlerConfig)
    if (!crawlerName) {
        return []
    }

    const links = extractTikTokLinksFromText(article.content || '')
    if (links.length === 0) {
        return []
    }

    const now = options.now || Math.floor(Date.now() / 1000)
    const queued: Array<{ videoId?: string; profileUrl: string; taskQueueId: number; status: string }> = []
    for (const link of links) {
        const resolved = await resolveTikTokLink(link, options.fetchImpl)
        if (!resolved?.profileUrl) {
            options.log?.warn?.(`X TikTok link ingest skipped unresolved link ${link}`)
            continue
        }

        if (resolved.videoId) {
            const existing = await DB.Article.getByArticleCode(resolved.videoId, Platform.TikTok)
            if (existing) {
                continue
            }
        }

        const taskType = DB.TaskQueue.TYPE.ScheduledCrawlerRun
        const payload = {
            crawler: crawlerName,
            websites: [resolved.videoId ? resolved.resolvedUrl : resolved.profileUrl],
            reason: `x tiktok link ${article.a_id || article.id || ''}`.trim().slice(0, 200),
        }
        const task = await DB.TaskQueue.add(taskType, payload, now, {
            source_ref: `x-tiktok-link:${article.a_id || article.id || resolved.profileUrl}`,
            action_type: 'x_tiktok_link_ingest',
            idempotency_key: DB.TaskQueue.buildIdempotencyKey(taskType, {
                crawler: crawlerName,
                profileUrl: resolved.profileUrl,
                videoId: resolved.videoId || null,
                sourcePlatform: 'x',
                sourceArticleId: article.a_id || article.id || null,
            }),
        })
        queued.push({
            videoId: resolved.videoId,
            profileUrl: resolved.profileUrl,
            taskQueueId: task.id,
            status: task.status,
        })
    }

    if (queued.length > 0) {
        options.log?.info?.(
            `Queued ${queued.length} TikTok ingest task(s) from X article ${article.a_id || article.id || '(unknown)'}`,
        )
    }
    return queued
}

async function enqueueMissingYouTubeLinksFromXArticle(article: Article, options: EnqueueOptions = {}) {
    if (article.platform !== Platform.X && article.platform !== Platform.Twitter) {
        return []
    }

    const crawlerName = resolveYouTubeCrawlerName(options.crawlerConfig)
    if (!crawlerName) {
        return []
    }

    const links = extractYouTubeLinksFromText(article.content || '')
    if (links.length === 0) {
        return []
    }

    const now = options.now || Math.floor(Date.now() / 1000)
    const queued: Array<{ videoId?: string; watchUrl: string; taskQueueId: number; status: string }> = []
    for (const link of links) {
        const resolved = parseYouTubeUrl(link)
        if (!resolved?.watchUrl) {
            options.log?.warn?.(`X YouTube link ingest skipped unresolved link ${link}`)
            continue
        }

        if (resolved.videoId) {
            const existing = await DB.Article.getByArticleCode(resolved.videoId, Platform.YouTube)
            if (existing) {
                continue
            }
        }

        const taskType = DB.TaskQueue.TYPE.ScheduledCrawlerRun
        const payload = {
            crawler: crawlerName,
            reason: `x youtube link ${article.a_id || article.id || ''}`.trim().slice(0, 200),
        }
        const task = await DB.TaskQueue.add(taskType, payload, now, {
            source_ref: `x-youtube-link:${article.a_id || article.id || resolved.watchUrl}`,
            action_type: 'x_youtube_link_ingest',
            idempotency_key: DB.TaskQueue.buildIdempotencyKey(taskType, {
                crawler: crawlerName,
                videoId: resolved.videoId || null,
                watchUrl: resolved.watchUrl,
                sourcePlatform: 'x',
                sourceArticleId: article.a_id || article.id || null,
            }),
        })
        queued.push({
            videoId: resolved.videoId,
            watchUrl: resolved.watchUrl,
            taskQueueId: task.id,
            status: task.status,
        })
    }

    if (queued.length > 0) {
        options.log?.info?.(
            `Queued ${queued.length} YouTube ingest task(s) from X article ${article.a_id || article.id || '(unknown)'}`,
        )
    }
    return queued
}

const GENERIC_URL_RE = /https?:\/\/[^\s<>"'，。！？、）)\]}]+/gi
const KNOWN_PLATFORM_HOSTS = [
    'x.com',
    'twitter.com',
    't.co',
    'tiktok.com',
    'youtube.com',
    'youtu.be',
    'instagram.com',
]
const DEFAULT_WEBSITE_CRAWLER_NAME = '22/7官网Blog抓取 - 高频'
const DEFAULT_WEBSITE_INGEST_HOSTS = ['nanabunnonijyuuni-mobile.com']
const MAX_WEBSITE_LINKS_PER_X_ARTICLE = 3

function hostMatches(host: string, rule: string) {
    const normalized = rule.toLowerCase()
    return host === normalized || host.endsWith(`.${normalized}`)
}

function extractWebsiteLinksFromText(text: string | null | undefined, hosts: Array<string>): Array<string> {
    if (!text) {
        return []
    }
    const out: Array<string> = []
    for (const match of text.matchAll(GENERIC_URL_RE)) {
        const cleaned = cleanExternalUrl(match[0])
        let url: URL
        try {
            url = new URL(cleaned)
        } catch {
            continue
        }
        const host = url.hostname.toLowerCase()
        if (KNOWN_PLATFORM_HOSTS.some((known) => hostMatches(host, known))) {
            continue
        }
        if (!hosts.some((rule) => hostMatches(host, rule))) {
            continue
        }
        if (hostMatches(host, 'nanabunnonijyuuni-mobile.com') && !url.pathname.startsWith('/s/n110/')) {
            continue
        }
        if (!out.includes(cleaned)) {
            out.push(cleaned)
        }
    }
    return out.slice(0, MAX_WEBSITE_LINKS_PER_X_ARTICLE)
}

function resolveWebsiteIngestConfig(crawlerConfig?: CrawlerConfig): { crawler: string; hosts: Array<string> } | null {
    const config = (crawlerConfig as any)?.x_website_link_ingest
    if (config === false || config?.enabled === false) {
        return null
    }
    const hosts =
        Array.isArray(config?.hosts) && config.hosts.length > 0
            ? config.hosts.map((host: unknown) => String(host).trim().toLowerCase()).filter(Boolean)
            : DEFAULT_WEBSITE_INGEST_HOSTS
    return {
        crawler: String(config?.crawler || DEFAULT_WEBSITE_CRAWLER_NAME).trim(),
        hosts,
    }
}

async function enqueueMissingWebsiteLinksFromXArticle(article: Article, options: EnqueueOptions = {}) {
    if (article.platform !== Platform.X && article.platform !== Platform.Twitter) {
        return []
    }

    const ingestConfig = resolveWebsiteIngestConfig(options.crawlerConfig)
    if (!ingestConfig) {
        return []
    }

    const links = extractWebsiteLinksFromText(article.content || '', ingestConfig.hosts)
    if (links.length === 0) {
        return []
    }

    const now = options.now || Math.floor(Date.now() / 1000)
    const queued: Array<{ url: string; taskQueueId: number; status: string }> = []
    for (const link of links) {
        const existing = await DB.Article.findByUrl(link).catch(() => null)
        if (existing) {
            continue
        }
        const taskType = DB.TaskQueue.TYPE.ScheduledCrawlerRun
        const payload = {
            crawler: ingestConfig.crawler,
            websites: [link],
            reason: `x website link ${article.a_id || article.id || ''}`.trim().slice(0, 200),
        }
        const task = await DB.TaskQueue.add(taskType, payload, now, {
            source_ref: `x-website-link:${article.a_id || article.id || link}`,
            action_type: 'x_website_link_ingest',
            idempotency_key: DB.TaskQueue.buildIdempotencyKey(taskType, {
                crawler: ingestConfig.crawler,
                url: link,
                sourcePlatform: 'x',
                sourceArticleId: article.a_id || article.id || null,
            }),
        })
        queued.push({ url: link, taskQueueId: task.id, status: task.status })
    }

    if (queued.length > 0) {
        options.log?.info?.(
            `Queued ${queued.length} website ingest task(s) from X article ${article.a_id || article.id || '(unknown)'}`,
        )
    }
    return queued
}

async function enqueueMissingExternalMediaLinksFromXArticle(article: Article, options: EnqueueOptions = {}) {
    const tiktok = await enqueueMissingTikTokLinksFromXArticle(article, options)
    const youtube = await enqueueMissingYouTubeLinksFromXArticle(article, options)
    const website = await enqueueMissingWebsiteLinksFromXArticle(article, options)
    return { tiktok, youtube, website }
}

export {
    DEFAULT_WEBSITE_CRAWLER_NAME,
    DEFAULT_WEBSITE_INGEST_HOSTS,
    DEFAULT_YOUTUBE_CRAWLER_NAME,
    DEFAULT_TIKTOK_CRAWLER_NAME,
    enqueueMissingExternalMediaLinksFromXArticle,
    enqueueMissingTikTokLinksFromXArticle,
    enqueueMissingWebsiteLinksFromXArticle,
    enqueueMissingYouTubeLinksFromXArticle,
    extractTikTokLinksFromText,
    extractWebsiteLinksFromText,
    extractYouTubeLinksFromText,
    parseTikTokUrl,
    parseYouTubeUrl,
    resolveTikTokLink,
}
