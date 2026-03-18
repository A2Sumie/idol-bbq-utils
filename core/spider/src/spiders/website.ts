import dayjs from 'dayjs'
import { Page } from 'puppeteer-core'
import { Platform } from '@/types'
import type { CrawlEngine, GenericArticle, GenericMediaInfo, TaskType, TaskTypeResult } from '@/types'
import { BaseSpider } from './base'

export enum ArticleTypeEnum {
    ARTICLE = 'article',
}

type FeedKind = 'official-news' | 'official-blog'

interface FeedConfig {
    feed: FeedKind
    u_id: string
    label: string
    listMatcher: RegExp
    detailMatcher: RegExp
}

interface WebsiteListItem {
    detailUrl: string
    title: string
    dateText: string
    summary?: string | null
    member?: string | null
    thumbnail?: string | null
}

interface WebsiteDetailPayload {
    title: string
    dateText: string
    bodyText: string
    bodyHtml: string
    member?: string | null
    media: Array<GenericMediaInfo>
}

const FEED_CONFIGS: Record<FeedKind, FeedConfig> = {
    'official-news': {
        feed: 'official-news',
        u_id: '22/7:official-news',
        label: '22/7 Official News',
        listMatcher: /^https?:\/\/nanabunnonijyuuni-mobile\.com\/s\/n110\/news\/list\b/i,
        detailMatcher: /^https?:\/\/nanabunnonijyuuni-mobile\.com\/s\/n110\/news\/detail\/(?<id>\d+)\b/i,
    },
    'official-blog': {
        feed: 'official-blog',
        u_id: '22/7:official-blog',
        label: '22/7 Official Blog',
        listMatcher: /^https?:\/\/nanabunnonijyuuni-mobile\.com\/s\/n110\/diary\/official_blog\/list\b/i,
        detailMatcher: /^https?:\/\/nanabunnonijyuuni-mobile\.com\/s\/n110\/diary\/detail\/(?<id>\d+)\b/i,
    },
}

const MOBILE_227_HOST = 'nanabunnonijyuuni-mobile.com'
const MAX_LIST_PAGES = 3
const MAX_DETAIL_COUNT = 20

function cleanText(value?: string | null): string {
    return (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
}

function resolveAbsoluteUrl(url: string, value?: string | null): string | null {
    if (!value) {
        return null
    }
    try {
        return new URL(value, url).href
    } catch {
        return null
    }
}

function parseDateToUnix(dateText?: string | null): number {
    const normalized = cleanText(dateText).replace(/\./g, '-')
    const parsed = dayjs(normalized)
    if (parsed.isValid()) {
        return parsed.startOf('day').unix()
    }
    return Math.floor(Date.now() / 1000)
}

function extractArticleId(config: FeedConfig, detailUrl: string) {
    return (
        config.detailMatcher.exec(detailUrl)?.groups?.id
        || `${config.feed}:${Buffer.from(detailUrl).toString('base64url')}`
    )
}

function getDetailKey(config: FeedConfig, detailUrl: string) {
    return extractArticleId(config, detailUrl)
}

function buildMedia(detailMedia: Array<GenericMediaInfo>, fallbackThumbnail?: string | null): Array<GenericMediaInfo> | null {
    const dedup = new Map<string, GenericMediaInfo>()
    for (const media of detailMedia) {
        if (media.url) {
            dedup.set(`${media.type}:${media.url}`, media)
        }
    }
    if (fallbackThumbnail) {
        dedup.set(`photo:${fallbackThumbnail}`, {
            type: 'photo',
            url: fallbackThumbnail,
        })
    }
    return dedup.size > 0 ? Array.from(dedup.values()) : null
}

function buildWebsiteArticle(
    config: FeedConfig,
    detailUrl: string,
    listItem: WebsiteListItem,
    detail: WebsiteDetailPayload,
): GenericArticle<Platform.Website> {
    const articleId = extractArticleId(config, detailUrl)
    const title = cleanText(detail.title || listItem.title)
    const summary = cleanText(listItem.summary)
    const bodyText = cleanText(detail.bodyText)
    const content = [title ? `【${title}】` : '', bodyText].filter(Boolean).join('\n\n') || title || summary || null
    const media = buildMedia(detail.media, listItem.thumbnail)
    const member = cleanText(detail.member || listItem.member) || null
    return {
        platform: Platform.Website,
        a_id: articleId,
        u_id: config.u_id,
        username: member || config.label,
        created_at: parseDateToUnix(detail.dateText || listItem.dateText),
        content,
        url: detailUrl,
        type: ArticleTypeEnum.ARTICLE,
        ref: null,
        has_media: Boolean(media && media.length > 0),
        media,
        extra: {
            data: {
                site: '22/7',
                host: MOBILE_227_HOST,
                feed: config.feed,
                title,
                member,
                summary: summary || null,
                raw_html: detail.bodyHtml,
            },
            content: summary || title || undefined,
            media: media || undefined,
            extra_type: 'website_meta',
        },
        u_avatar: null,
    }
}

async function extractNewsList(page: Page, url: string) {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.news_box', { timeout: 15000 })
    return page.evaluate((currentUrl) => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const absolute = (value?: string | null) => {
            if (!value) {
                return null
            }
            try {
                return new URL(value, currentUrl).href
            } catch {
                return null
            }
        }
        const items = Array.from(document.querySelectorAll('.news_box'))
            .map((node) => {
                const detailUrl =
                    absolute(node.querySelector('.news_box_title a')?.getAttribute('href'))
                    || absolute(node.querySelector('.viewmore a')?.getAttribute('href'))
                if (!detailUrl) {
                    return null
                }
                return {
                    detailUrl,
                    title: clean(node.querySelector('.news_box_title')?.textContent),
                    dateText: clean(node.querySelector('.news_box_date')?.textContent),
                    summary: clean(node.querySelector('.news_box_description')?.textContent),
                    member: null,
                    thumbnail: null,
                }
            })
            .filter(Boolean)
        const nextUrl = absolute(document.querySelector('.pager .next a')?.getAttribute('href'))
        return {
            items,
            nextUrl,
        }
    }, url) as Promise<{ items: Array<WebsiteListItem>; nextUrl?: string | null }>
}

async function extractBlogList(page: Page, url: string) {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('a[href*="/diary/detail/"]', { timeout: 15000 })
    return page.evaluate((currentUrl) => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const absolute = (value?: string | null) => {
            if (!value) {
                return null
            }
            try {
                return new URL(value, currentUrl).href
            } catch {
                return null
            }
        }
        const parseBackground = (value?: string | null) => {
            const match = value?.match(/url\((['"]?)(.*?)\1\)/)
            return match?.[2] || null
        }
        const items = Array.from(document.querySelectorAll('a[href*="/diary/detail/"]'))
            .map((anchor) => {
                const detailUrl = absolute(anchor.getAttribute('href'))
                if (!detailUrl) {
                    return null
                }
                const thumbNode = anchor.querySelector<HTMLElement>('.blog-entry-list__thumb img, .blog-list__thumb img')
                const thumbFromStyle = parseBackground(thumbNode?.getAttribute('style'))
                return {
                    detailUrl,
                    title: clean(
                        anchor.querySelector('.blog-list__title, .blog-entry-list__title .title')?.textContent,
                    ),
                    dateText: clean(anchor.querySelector('.date')?.textContent),
                    summary: clean(anchor.querySelector('.blog-list__txt')?.textContent),
                    member: clean(anchor.querySelector('.name')?.textContent) || null,
                    thumbnail: absolute(thumbFromStyle || thumbNode?.getAttribute('src')),
                }
            })
            .filter(Boolean)
        const dedup = Array.from(new Map(items.map((item) => [item.detailUrl, item])).values())
        const nextUrl = absolute(document.querySelector('.pager .next a')?.getAttribute('href'))
        return {
            items: dedup,
            nextUrl,
        }
    }, url) as Promise<{ items: Array<WebsiteListItem>; nextUrl?: string | null }>
}

async function extractNewsDetail(page: Page, url: string): Promise<WebsiteDetailPayload> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('#infoDetailTitle, #infoDetail', { timeout: 15000 })
    return page.evaluate((currentUrl) => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const absolute = (value?: string | null) => {
            if (!value) {
                return null
            }
            try {
                return new URL(value, currentUrl).href
            } catch {
                return null
            }
        }
        const body = document.querySelector<HTMLElement>('#infoDetail')
        const media = Array.from(body?.querySelectorAll('img') || [])
            .map((img) => {
                const src = absolute(img.getAttribute('src'))
                if (!src) {
                    return null
                }
                return {
                    type: 'photo' as const,
                    url: src,
                    alt: clean(img.getAttribute('alt')) || undefined,
                }
            })
            .filter(Boolean)
        return {
            title: clean(document.querySelector('#infoCaption')?.textContent),
            dateText: clean(document.querySelector('.infoDate')?.textContent),
            bodyText: clean(body?.innerText || body?.textContent),
            bodyHtml: body?.innerHTML || '',
            member: null,
            media,
        }
    }, url)
}

async function extractBlogDetail(page: Page, url: string): Promise<WebsiteDetailPayload> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.blog_detail__title, .blog_detail__main', { timeout: 15000 })
    return page.evaluate((currentUrl) => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const absolute = (value?: string | null) => {
            if (!value) {
                return null
            }
            try {
                return new URL(value, currentUrl).href
            } catch {
                return null
            }
        }
        const body = document.querySelector<HTMLElement>('.blog_detail__main')
        const media = Array.from(body?.querySelectorAll('img') || [])
            .map((img) => {
                const src = absolute(img.getAttribute('src'))
                if (!src) {
                    return null
                }
                return {
                    type: 'photo' as const,
                    url: src,
                    alt: clean(img.getAttribute('alt')) || undefined,
                }
            })
            .filter(Boolean)
        return {
            title: clean(document.querySelector('.blog_detail__title')?.textContent),
            dateText: clean(document.querySelector('.blog_detail__date .date')?.textContent),
            bodyText: clean(body?.innerText || body?.textContent),
            bodyHtml: body?.innerHTML || '',
            member: clean(document.querySelector('.blog_detail__date .name')?.textContent) || null,
            media,
        }
    }, url)
}

class NanabunnonijyuuniWebsiteSpider extends BaseSpider {
    static _VALID_URL =
        /^https?:\/\/nanabunnonijyuuni-mobile\.com\/s\/n110\/(?:(?:news\/(?:list|detail\/\d+))|(?:diary\/(?:official_blog\/list|detail\/\d+)))(?:\?.*)?$/i
    static _PLATFORM = Platform.Website
    BASE_URL = `https://${MOBILE_227_HOST}/`
    NAME = '22/7 Website Spider'

    static resolveFeed(url: string): FeedConfig | null {
        return (
            Object.values(FEED_CONFIGS).find((config) => {
                return config.listMatcher.test(url) || config.detailMatcher.test(url)
            }) || null
        )
    }

    static extractBasicInfo(url: string) {
        const config = NanabunnonijyuuniWebsiteSpider.resolveFeed(url)
        if (!config) {
            return undefined
        }
        return {
            u_id: config.u_id,
            platform: Platform.Website,
        }
    }

    async _crawl<T extends TaskType>(
        url: string,
        page: Page | undefined,
        config: {
            task_type: T
            crawl_engine: CrawlEngine
            sub_task_type?: Array<string>
            cookieString?: string
        },
    ): Promise<TaskTypeResult<T, Platform.Website>> {
        if (config.task_type !== 'article') {
            throw new Error('Website spider only supports article tasks')
        }
        if (!page) {
            throw new Error('Website spider requires a browser page in mobile mode')
        }

        const feedConfig = NanabunnonijyuuniWebsiteSpider.resolveFeed(url)
        if (!feedConfig) {
            throw new Error(`Unsupported website url: ${url}`)
        }

        if (feedConfig.detailMatcher.test(url)) {
            const article = await this.crawlSingleDetail(page, feedConfig, {
                detailUrl: url,
                title: '',
                dateText: '',
            })
            return [article] as TaskTypeResult<T, Platform.Website>
        }

        const articles = await this.crawlFeed(page, feedConfig, url)
        return articles as TaskTypeResult<T, Platform.Website>
    }

    private async crawlFeed(page: Page, feedConfig: FeedConfig, url: string) {
        const discovered = new Map<string, WebsiteListItem>()
        let currentUrl: string | null = url
        let pageCount = 0

        while (currentUrl && pageCount < MAX_LIST_PAGES) {
            const result =
                feedConfig.feed === 'official-news'
                    ? await extractNewsList(page, currentUrl)
                    : await extractBlogList(page, currentUrl)
            result.items.forEach((item) => {
                const detailKey = getDetailKey(feedConfig, item.detailUrl)
                if (!discovered.has(detailKey)) {
                    discovered.set(detailKey, item)
                }
            })
            currentUrl = result.nextUrl || null
            pageCount += 1
        }

        const listItems = Array.from(discovered.values()).slice(0, MAX_DETAIL_COUNT)
        const articles: Array<GenericArticle<Platform.Website>> = []

        for (const item of listItems) {
            articles.push(await this.crawlSingleDetail(page, feedConfig, item))
        }

        return articles.sort((a, b) => b.created_at - a.created_at)
    }

    private async crawlSingleDetail(page: Page, feedConfig: FeedConfig, listItem: WebsiteListItem) {
        const detailPayload =
            feedConfig.feed === 'official-news'
                ? await extractNewsDetail(page, listItem.detailUrl)
                : await extractBlogDetail(page, listItem.detailUrl)

        return buildWebsiteArticle(feedConfig, listItem.detailUrl, listItem, detailPayload)
    }
}

export { NanabunnonijyuuniWebsiteSpider }
