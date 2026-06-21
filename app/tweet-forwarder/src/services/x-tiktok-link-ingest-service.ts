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

type EnqueueOptions = {
    crawlerConfig?: CrawlerConfig
    log?: MinimalLog
    fetchImpl?: typeof fetch
    now?: number
}

const TIKTOK_URL_RE = /https?:\/\/(?:www\.|vm\.|vt\.)?tiktok\.com\/[^\s<>"'，。！？、）)\]}]+/gi
const DEFAULT_TIKTOK_CRAWLER_NAME = 'Tiktok抓取'
const MAX_TIKTOK_LINKS_PER_X_ARTICLE = 5
const TIKTOK_LINK_RESOLVE_TIMEOUT_MS = 10_000

function cleanTikTokUrl(value: string) {
    return value.replace(/[.,!?;:，。！？、）)\]}]+$/g, '')
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

function parseTikTokUrl(rawUrl: string): TikTokLinkInfo | null {
    let url: URL
    try {
        url = new URL(rawUrl)
    } catch {
        return null
    }

    const hostname = url.hostname.toLowerCase()
    if (!hostname.endsWith('tiktok.com')) {
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
            websites: [resolved.profileUrl],
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

export {
    DEFAULT_TIKTOK_CRAWLER_NAME,
    enqueueMissingTikTokLinksFromXArticle,
    extractTikTokLinksFromText,
    parseTikTokUrl,
    resolveTikTokLink,
}
