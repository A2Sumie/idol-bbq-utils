import dayjs from 'dayjs'
import { Page } from 'puppeteer-core'
import { Platform } from '@/types'
import type { CrawlEngine, GenericArticle, GenericMediaInfo, TaskType, TaskTypeResult } from '@/types'
import { BaseSpider } from './base'

export enum ArticleTypeEnum {
    ARTICLE = 'article',
}

type FeedKind =
    | 'fc-news'
    | 'official-news'
    | 'official-blog'
    | 'ticket'
    | 'radio'
    | 'movie'
    | 'photo'
    | 'live-report'

export interface FeedConfig {
    feed: FeedKind
    u_id: string
    label: string
}

export interface WebsiteListItem {
    detailUrl: string
    title: string
    dateText: string
    summary?: string | null
    member?: string | null
    thumbnail?: string | null
    uAvatar?: string | null
}

interface WebsiteDetailPayload {
    title: string
    dateText: string
    bodyText: string
    bodyHtml: string
    member?: string | null
    media: Array<GenericMediaInfo>
    uAvatar?: string | null
    extraData?: Record<string, any>
}

interface WebsiteListPageResult {
    items: Array<WebsiteListItem>
    nextUrl?: string | null
}

interface WebsiteBuildOptions {
    articleId?: string
    detailUrl?: string
}

export interface WebsitePhotoEntry {
    modalId: string
    dataCode?: string | null
    detailUrl: string
    title: string
    theme?: string | null
    dateText: string
    member?: string | null
    bodyText: string
    bodyHtml: string
    media: Array<GenericMediaInfo>
    uAvatar?: string | null
    extraData?: Record<string, any>
}

export interface WebsitePhotoAlbumPayload {
    currentUrl: string
    albumId: string
    pageTheme?: string | null
    entries: Array<WebsitePhotoEntry>
}

interface StandardEntryListOptions {
    waitForSelector: string
    itemSelector: string
    detailSelector: string
    titleSelector: string
    dateSelector: string
    summarySelector?: string
    thumbnailSelector?: string
    memberSelector?: string
}

const FEED_CONFIGS: Record<FeedKind, FeedConfig> = {
    'fc-news': {
        feed: 'fc-news',
        u_id: '22/7:fc-news',
        label: '22/7 FC News',
    },
    'official-news': {
        feed: 'official-news',
        u_id: '22/7:official-news',
        label: '22/7 Official News',
    },
    'official-blog': {
        feed: 'official-blog',
        u_id: '22/7:official-blog',
        label: '22/7 Official Blog',
    },
    ticket: {
        feed: 'ticket',
        u_id: '22/7:ticket',
        label: '22/7 Ticket',
    },
    radio: {
        feed: 'radio',
        u_id: '22/7:radio',
        label: '22/7 Radio',
    },
    movie: {
        feed: 'movie',
        u_id: '22/7:movie',
        label: '22/7 Movie',
    },
    photo: {
        feed: 'photo',
        u_id: '22/7:photo',
        label: '22/7 Photo',
    },
    'live-report': {
        feed: 'live-report',
        u_id: '22/7:live-report',
        label: '22/7 Live Report',
    },
}

const MOBILE_227_HOST = 'nanabunnonijyuuni-mobile.com'
const MAX_LIST_PAGES = 3
const MAX_DETAIL_COUNT = 20

function cleanText(value?: string | null): string {
    return (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
}

function cleanMultilineText(value?: string | null): string {
    const lines = (value || '')
        .replace(/\u00a0/g, ' ')
        .replace(/\r/g, '')
        .split('\n')
        .map((line) => line.replace(/[ \t]+/g, ' ').trim())

    const collapsed = lines.reduce<Array<string>>((acc, line) => {
        if (!line) {
            if (acc[acc.length - 1] !== '') {
                acc.push('')
            }
            return acc
        }
        acc.push(line)
        return acc
    }, [])

    return collapsed.join('\n').replace(/\n{3,}/g, '\n\n').trim()
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
    const normalized = cleanText(dateText).replace(/[./]/g, '-')
    const parsed = dayjs(normalized)
    if (parsed.isValid()) {
        return parsed.startOf('day').unix()
    }
    return Math.floor(Date.now() / 1000)
}

function tryParseWebsiteUrl(url: string): URL | null {
    try {
        return new URL(url)
    } catch {
        return null
    }
}

function isNewsDetail(pathname: string) {
    return /^\/s\/n110\/news\/detail\/[^/?#]+$/i.test(pathname)
}

function isTicketDetail(pathname: string) {
    return /^\/s\/n110\/ticket\/detail\/[^/?#]+$/i.test(pathname)
}

function isDiaryDetail(pathname: string) {
    return /^\/s\/n110\/diary\/detail\/\d+$/i.test(pathname)
}

function isRadioDetail(pathname: string) {
    return /^\/s\/n110\/contents\/[^/?#]+$/i.test(pathname)
}

function isPhotoDetail(url: URL) {
    return (
        /^\/s\/n110\/gallery\/[^/?#]+$/i.test(url.pathname)
        || (url.pathname === '/s/n110/contents_list' && (url.searchParams.get('ct') || '').startsWith('member_photo_'))
    )
}

function isPhotoList(url: URL) {
    return url.pathname === '/s/n110/gallery' && url.searchParams.get('ct') === 'photoga'
}

function isRadioList(url: URL) {
    return url.pathname === '/s/n110/contents_list' && url.searchParams.get('ct') === 'radio'
}

function isMovieList(url: URL) {
    return /^\/s\/n110\/diary\/nananiji_movie(?:\/list)?$/i.test(url.pathname)
}

function isLiveReportList(url: URL) {
    return url.pathname === '/s/n110/diary/special/list'
}

function isDetailUrl(feed: FeedKind, url: string) {
    const parsed = tryParseWebsiteUrl(url)
    if (!parsed || parsed.hostname !== MOBILE_227_HOST) {
        return false
    }

    switch (feed) {
        case 'fc-news':
        case 'official-news':
            return isNewsDetail(parsed.pathname)
        case 'official-blog':
            return isDiaryDetail(parsed.pathname) && parsed.searchParams.get('cd') !== 'nananiji_movie' && parsed.searchParams.get('cd') !== 'special'
        case 'ticket':
            return isTicketDetail(parsed.pathname)
        case 'radio':
            return isRadioDetail(parsed.pathname)
        case 'movie':
            return isDiaryDetail(parsed.pathname) && parsed.searchParams.get('cd') === 'nananiji_movie'
        case 'photo':
            return isPhotoDetail(parsed)
        case 'live-report':
            return isDiaryDetail(parsed.pathname) && parsed.searchParams.get('cd') === 'special'
        default:
            return false
    }
}

function extractArticleId(config: FeedConfig, detailUrl: string) {
    const parsed = tryParseWebsiteUrl(detailUrl)
    if (parsed) {
        switch (config.feed) {
            case 'fc-news':
            case 'official-news':
            case 'ticket':
            case 'radio':
            case 'movie':
            case 'official-blog':
            case 'live-report': {
                const id = parsed.pathname.split('/').filter(Boolean).pop()
                if (id) {
                    return id
                }
                break
            }
            case 'photo': {
                const id = parsed.searchParams.get('ct') || parsed.pathname.split('/').filter(Boolean).pop()
                if (id) {
                    return id
                }
                break
            }
        }
    }

    return `${config.feed}:${Buffer.from(detailUrl).toString('base64url')}`
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

export function buildWebsiteArticle(
    config: FeedConfig,
    detailUrl: string,
    listItem: WebsiteListItem,
    detail: WebsiteDetailPayload,
    options?: WebsiteBuildOptions,
): GenericArticle<Platform.Website> {
    const articleId = options?.articleId || extractArticleId(config, options?.detailUrl || detailUrl)
    const finalUrl = options?.detailUrl || detailUrl
    const title = cleanText(detail.title || listItem.title)
    const summary = cleanText(listItem.summary)
    const bodyText = cleanMultilineText(detail.bodyText)
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
        url: finalUrl,
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
                ...(detail.extraData || {}),
            },
            content: summary || title || undefined,
            media: media || undefined,
            extra_type: 'website_meta',
        },
        u_avatar: detail.uAvatar || listItem.uAvatar || null,
    }
}

function resolvePhotoAlbumAnchor(payload: WebsitePhotoAlbumPayload) {
    const candidate =
        payload.entries.map((entry) => cleanText(entry.dataCode)).find(Boolean)
        || payload.entries.map((entry) => cleanText(entry.modalId)).find(Boolean)

    if (candidate) {
        return candidate
    }

    return Buffer.from(`${payload.albumId}:${payload.currentUrl}`).toString('base64url').slice(0, 16)
}

export function buildPhotoAlbumArticle(
    config: FeedConfig,
    listItem: WebsiteListItem,
    payload: WebsitePhotoAlbumPayload,
): Array<GenericArticle<Platform.Website>> {
    if (payload.entries.length === 0) {
        return []
    }

    const title =
        cleanText(payload.pageTheme)
        || cleanText(listItem.title)
        || cleanText(payload.entries[0]?.theme)
        || cleanText(payload.entries[0]?.title)
    const dateText = payload.entries.map((entry) => cleanText(entry.dateText)).find(Boolean) || cleanText(listItem.dateText)
    const media = payload.entries.flatMap((entry) => entry.media || [])
    const bodyText = payload.entries
        .map((entry) => {
            const heading = cleanText(entry.member) || cleanText(entry.title)
            const message = cleanMultilineText(entry.bodyText)
            return [heading ? `【${heading}】` : '', message].filter(Boolean).join('\n')
        })
        .filter(Boolean)
        .join('\n\n')
    const bodyHtml = payload.entries
        .map((entry) => entry.bodyHtml)
        .filter(Boolean)
        .join('\n<hr />\n')
    const members = Array.from(
        new Set(payload.entries.map((entry) => cleanText(entry.member)).filter(Boolean)),
    )
    const albumAnchor = resolvePhotoAlbumAnchor(payload)
    const firstAvatar = payload.entries.map((entry) => entry.uAvatar).find(Boolean) || listItem.uAvatar || null

    return [
        buildWebsiteArticle(
            config,
            payload.currentUrl,
            {
                ...listItem,
                title: title || listItem.title,
                dateText: dateText || listItem.dateText,
                member: null,
                thumbnail: media[0]?.url || listItem.thumbnail,
                uAvatar: firstAvatar,
            },
            {
                title: title || listItem.title,
                dateText: dateText || listItem.dateText,
                bodyText,
                bodyHtml,
                member: null,
                media,
                uAvatar: firstAvatar,
                extraData: {
                    album_id: payload.albumId,
                    album_anchor: albumAnchor,
                    entry_count: payload.entries.length,
                    members: members.length > 0 ? members : null,
                    entries: payload.entries.map((entry) => ({
                        ...entry,
                        bodyText: cleanMultilineText(entry.bodyText),
                    })),
                },
            },
            {
                articleId: `${config.feed}:album:${payload.albumId}:${albumAnchor}`,
                detailUrl: payload.currentUrl,
            },
        ),
    ]
}

async function extractStandardEntryList(page: Page, url: string, options: StandardEntryListOptions): Promise<WebsiteListPageResult> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector(options.waitForSelector, { timeout: 15000 })
    return page.evaluate(
        (currentUrl, selectors) => {
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
            const items = Array.from(document.querySelectorAll(selectors.itemSelector))
                .map((node) => {
                    const detailUrl = absolute(node.querySelector(selectors.detailSelector)?.getAttribute('href'))
                    if (!detailUrl) {
                        return null
                    }
                    const thumbnailSrc = selectors.thumbnailSelector
                        ? absolute(node.querySelector(selectors.thumbnailSelector)?.getAttribute('src'))
                        : null
                    return {
                        detailUrl,
                        title: clean(node.querySelector(selectors.titleSelector)?.textContent),
                        dateText: clean(node.querySelector(selectors.dateSelector)?.textContent),
                        summary: selectors.summarySelector ? clean(node.querySelector(selectors.summarySelector)?.textContent) : null,
                        member: selectors.memberSelector ? clean(node.querySelector(selectors.memberSelector)?.textContent) || null : null,
                        thumbnail: thumbnailSrc,
                    }
                })
                .filter(Boolean)

            const nextUrl = absolute(document.querySelector('.pager .next a')?.getAttribute('href'))
            return {
                items,
                nextUrl,
            }
        },
        url,
        options,
    ) as Promise<WebsiteListPageResult>
}

async function extractNewsList(page: Page, url: string) {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.news_box, .entry-list .entry-item', { timeout: 15000 })
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
        const legacyItems = Array.from(document.querySelectorAll('.news_box'))
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
        const entryItems = Array.from(document.querySelectorAll('.entry-list .entry-item'))
            .map((node) => {
                const detailUrl =
                    absolute(node.querySelector('a.panel')?.getAttribute('href'))
                    || absolute(node.querySelector('.entry__title a')?.getAttribute('href'))
                if (!detailUrl) {
                    return null
                }
                return {
                    detailUrl,
                    title: clean(node.querySelector('.entry__title')?.textContent),
                    dateText: clean(node.querySelector('.entry__posted')?.textContent),
                    summary: clean(node.querySelector('.entry__text, .entry__description')?.textContent),
                    member: null,
                    thumbnail: null,
                }
            })
            .filter(Boolean)
        const items = legacyItems.length > 0 ? legacyItems : entryItems
        const nextUrl = absolute(document.querySelector('.pager .next a')?.getAttribute('href'))
        return {
            items,
            nextUrl,
        }
    }, url) as Promise<WebsiteListPageResult>
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
        const dedup = Array.from(new Map(items.map((item: any) => [item.detailUrl, item])).values())
        const nextUrl = absolute(document.querySelector('.pager .next a')?.getAttribute('href'))
        return {
            items: dedup,
            nextUrl,
        }
    }, url) as Promise<WebsiteListPageResult>
}

async function extractRadioList(page: Page, url: string): Promise<WebsiteListPageResult> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.section-radio .radio', { timeout: 15000 })
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
        const items = Array.from(document.querySelectorAll('.section-radio .radio'))
            .map((node) => {
                const detailUrl = absolute(
                    node.querySelector('.radio-img')?.getAttribute('href')
                    || node.querySelector('.radio-btn.radio')?.getAttribute('href'),
                )
                if (!detailUrl) {
                    return null
                }
                return {
                    detailUrl,
                    title: clean(node.querySelector('.radio__title')?.textContent),
                    dateText: clean(node.querySelector('.radio__posted')?.textContent),
                    summary: clean(node.querySelector('.radio__text')?.textContent) || null,
                    member: null,
                    thumbnail: absolute(node.querySelector('.radio-img img')?.getAttribute('src')),
                }
            })
            .filter(Boolean)

        return {
            items: Array.from(new Map(items.map((item: any) => [item.detailUrl, item])).values()),
            nextUrl: null,
        }
    }, url)
}

async function extractMovieList(page: Page, url: string): Promise<WebsiteListPageResult> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.section-movie .movie, .archive-list .archive-item', { timeout: 15000 })
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
        const featured = Array.from(document.querySelectorAll('.section-movie .movie'))
            .map((node) => {
                const detailUrl = absolute(node.querySelector('.movie-img')?.getAttribute('href'))
                if (!detailUrl) {
                    return null
                }
                return {
                    detailUrl,
                    title: clean(node.querySelector('.movie__title')?.textContent),
                    dateText: clean(node.querySelector('.movie__posted')?.textContent),
                    summary: null,
                    member: null,
                    thumbnail: absolute(node.querySelector('.movie-img img')?.getAttribute('src')),
                }
            })
            .filter(Boolean)

        const archive = Array.from(document.querySelectorAll('.archive-list .archive-item'))
            .map((node) => {
                const detailUrl = absolute(node.querySelector('.archive-inner')?.getAttribute('href'))
                if (!detailUrl) {
                    return null
                }
                return {
                    detailUrl,
                    title: clean(node.querySelector('.archive__title')?.textContent),
                    dateText: clean(node.querySelector('.archive__posted')?.textContent),
                    summary: null,
                    member: null,
                    thumbnail: absolute(node.querySelector('.archive-thumb img')?.getAttribute('src')),
                }
            })
            .filter(Boolean)

        return {
            items: Array.from(new Map([...featured, ...archive].map((item: any) => [item.detailUrl, item])).values()),
            nextUrl: absolute(document.querySelector('.pager .next a')?.getAttribute('href')),
        }
    }, url)
}

async function extractPhotoList(page: Page, url: string): Promise<WebsiteListPageResult> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.section-photo .headline, .archive-list .archive-item', { timeout: 15000 })
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

        const sectionPhoto = document.querySelector('.section-photo')
        const currentTheme = clean(sectionPhoto?.querySelector('.headline__title')?.textContent)
        const currentDates = Array.from(sectionPhoto?.querySelectorAll('.photo__posted') || []).map((node) => clean(node.textContent))
        const currentThumbnail = absolute(sectionPhoto?.querySelector('.photo__img img')?.getAttribute('src'))

        const items: Array<WebsiteListItem> = []
        if (currentTheme && sectionPhoto?.querySelector('.photo-modal, .photo-block')) {
            items.push({
                detailUrl: currentUrl,
                title: currentTheme,
                dateText: currentDates[currentDates.length - 1] || currentDates[0] || '',
                summary: currentTheme,
                member: null,
                thumbnail: currentThumbnail,
            })
        }

        const archiveItems = Array.from(document.querySelectorAll('.archive-list .archive-item'))
            .map((node) => {
                const detailUrl = absolute(node.querySelector('.archive-inner')?.getAttribute('href'))
                if (!detailUrl) {
                    return null
                }
                return {
                    detailUrl,
                    title: clean(node.querySelector('.archive__title')?.textContent),
                    dateText: clean(node.querySelector('.archive__posted')?.textContent),
                    summary: clean(node.querySelector('.archive__label')?.textContent),
                    member: null,
                    thumbnail: absolute(node.querySelector('.archive-thumb img')?.getAttribute('src')),
                }
            })
            .filter(Boolean)

        return {
            items: Array.from(new Map([...items, ...archiveItems].map((item: any) => [item.detailUrl, item])).values()),
            nextUrl: absolute(document.querySelector('.pager .next a')?.getAttribute('href')),
        }
    }, url)
}

async function extractLiveReportList(page: Page, url: string): Promise<WebsiteListPageResult> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.special_box', { timeout: 15000 })
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

        const items = Array.from(document.querySelectorAll('.special_box'))
            .map((node) => {
                const detailUrl = absolute(node.querySelector('.special_title a')?.getAttribute('href'))
                if (!detailUrl) {
                    return null
                }
                return {
                    detailUrl,
                    title: clean(node.querySelector('.special_title')?.textContent),
                    dateText: clean(node.querySelector('.special_date')?.textContent),
                    summary: null,
                    member: null,
                    thumbnail: absolute(node.querySelector('.special_thumb img')?.getAttribute('src')),
                }
            })
            .filter(Boolean)

        return {
            items,
            nextUrl: absolute(document.querySelector('.pager .next a')?.getAttribute('href')),
        }
    }, url)
}

async function extractNewsDetail(page: Page, url: string): Promise<WebsiteDetailPayload> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('#infoDetailTitle, #infoDetail, #infoCaption, .section-article .article__title, .section-article .article-content', {
        timeout: 15000,
    })
    return page.evaluate((currentUrl) => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const cleanMultiline = (value?: string | null) =>
            (value || '')
                .replace(/\u00a0/g, ' ')
                .replace(/\r/g, '')
                .split('\n')
                .map((line) => line.replace(/[ \t]+/g, ' ').trim())
                .filter((line, index, arr) => Boolean(line) || (arr[index - 1] && arr[index + 1]))
                .join('\n')
                .replace(/\n{3,}/g, '\n\n')
                .trim()
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
        const body =
            document.querySelector<HTMLElement>('#infoDetail')
            || document.querySelector<HTMLElement>('.section-article .article-content')
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
            title: clean(document.querySelector('#infoCaption')?.textContent || document.querySelector('.section-article .article__title')?.textContent),
            dateText: clean(document.querySelector('.infoDate')?.textContent || document.querySelector('.section-article .article__posted')?.textContent),
            bodyText: cleanMultiline(body?.innerText || body?.textContent),
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
        const cleanMultiline = (value?: string | null) =>
            (value || '')
                .replace(/\u00a0/g, ' ')
                .replace(/\r/g, '')
                .split('\n')
                .map((line) => line.replace(/[ \t]+/g, ' ').trim())
                .filter((line, index, arr) => Boolean(line) || (arr[index - 1] && arr[index + 1]))
                .join('\n')
                .replace(/\n{3,}/g, '\n\n')
                .trim()
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
            bodyText: cleanMultiline(body?.innerText || body?.textContent),
            bodyHtml: body?.innerHTML || '',
            member: clean(document.querySelector('.blog_detail__date .name')?.textContent) || null,
            media,
        }
    }, url)
}

async function extractTicketDetail(page: Page, url: string): Promise<WebsiteDetailPayload> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.article__title, .article-content', { timeout: 15000 })
    return page.evaluate((currentUrl) => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const cleanMultiline = (value?: string | null) =>
            (value || '')
                .replace(/\u00a0/g, ' ')
                .replace(/\r/g, '')
                .split('\n')
                .map((line) => line.replace(/[ \t]+/g, ' ').trim())
                .filter((line, index, arr) => Boolean(line) || (arr[index - 1] && arr[index + 1]))
                .join('\n')
                .replace(/\n{3,}/g, '\n\n')
                .trim()
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

        const body = document.querySelector<HTMLElement>('.article-content')
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

        const applyUrl =
            document.querySelector<HTMLFormElement>('.article-btn form')?.getAttribute('action')
            || document.querySelector<HTMLAnchorElement>('.article-btn a')?.getAttribute('href')

        return {
            title: clean(document.querySelector('.article__title')?.textContent),
            dateText: clean(document.querySelector('.article__posted')?.textContent),
            bodyText: cleanMultiline(body?.innerText || body?.textContent),
            bodyHtml: body?.innerHTML || '',
            member: null,
            media,
            extraData: {
                apply_url: absolute(applyUrl),
            },
        }
    }, url)
}

async function extractRadioDetail(page: Page, url: string): Promise<WebsiteDetailPayload> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.radio__title, #modal-radio, #modal-movie', { timeout: 15000 })
    return page.evaluate((currentUrl) => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const cleanMultiline = (value?: string | null) =>
            (value || '')
                .replace(/\u00a0/g, ' ')
                .replace(/\r/g, '')
                .split('\n')
                .map((line) => line.replace(/[ \t]+/g, ' ').trim())
                .filter((line, index, arr) => Boolean(line) || (arr[index - 1] && arr[index + 1]))
                .join('\n')
                .replace(/\n{3,}/g, '\n\n')
                .trim()
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

        const thumb = absolute(document.querySelector('.radio__thumb img')?.getAttribute('src'))
        const streamMap = new Map<string, Record<string, any>>()
        Array.from(document.querySelectorAll<HTMLElement>('#modal-radio [data-video-id], #modal-movie [data-video-id]')).forEach((node) => {
            const videoId = clean(node.getAttribute('data-video-id'))
            const kind = node.closest('#modal-movie') ? 'movie' : 'radio'
            const playerRoot = node.closest<HTMLElement>('.video-js')
            const poster =
                absolute(node.getAttribute('poster'))
                || absolute(parseBackground(playerRoot?.querySelector<HTMLElement>('.vjs-poster')?.getAttribute('style')))
            const src = absolute(node.getAttribute('src'))
            if (!videoId && !src && !poster) {
                return
            }
            const key = `${kind}:${videoId || src || poster}`
            if (!streamMap.has(key)) {
                streamMap.set(key, {
                    kind,
                    url: src,
                    poster,
                    video_id: videoId || null,
                })
            }
        })
        const streams = Array.from(streamMap.values())
            .map((stream) => {
                if (!stream.url && !stream.poster && !stream.video_id) {
                    return null
                }
                return stream
            })
            .filter(Boolean)

        const media = [
            ...(thumb
                ? [
                      {
                          type: 'photo' as const,
                          url: thumb,
                      },
                  ]
                : []),
            ...streams
                .map((stream: any) => {
                    if (!stream.poster) {
                        return null
                    }
                    return {
                        type: 'video_thumbnail' as const,
                        url: stream.poster,
                    }
                })
                .filter(Boolean),
        ]

        const notes = clean(document.querySelector('.radio__notes')?.textContent)
        const accessNote = clean(document.querySelector('#modal-msg .msg')?.textContent)
        const bodyText = [cleanMultiline(document.querySelector('.radio__text')?.textContent), notes]
            .filter(Boolean)
            .join('\n\n')

        return {
            title: clean(document.querySelector('.radio__title')?.textContent),
            dateText: clean(document.querySelector('.radio__posted')?.textContent),
            bodyText,
            bodyHtml: document.querySelector<HTMLElement>('.section-radio-content .content')?.innerHTML || '',
            member: null,
            media,
            extraData: {
                access_note: accessNote || null,
                notes: notes || null,
                streams,
            },
        }
    }, url)
}

async function extractMovieDetail(page: Page, url: string): Promise<WebsiteDetailPayload> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.movie__title, .movie-player video', { timeout: 15000 })
    return page.evaluate((currentUrl) => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const cleanMultiline = (value?: string | null) =>
            (value || '')
                .replace(/\u00a0/g, ' ')
                .replace(/\r/g, '')
                .split('\n')
                .map((line) => line.replace(/[ \t]+/g, ' ').trim())
                .filter((line, index, arr) => Boolean(line) || (arr[index - 1] && arr[index + 1]))
                .join('\n')
                .replace(/\n{3,}/g, '\n\n')
                .trim()
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

        const videoMap = new Map<string, Record<string, any>>()
        Array.from(document.querySelectorAll<HTMLElement>('.movie-player [data-video-id]')).forEach((node) => {
            const videoId = clean(node.getAttribute('data-video-id'))
            const playerRoot = node.closest<HTMLElement>('.video-js')
            const poster =
                absolute(node.getAttribute('poster'))
                || absolute(parseBackground(playerRoot?.querySelector<HTMLElement>('.vjs-poster')?.getAttribute('style')))
            const src = absolute(node.getAttribute('src'))
            if (!videoId && !src && !poster) {
                return
            }
            const key = videoId || src || poster || String(videoMap.size)
            if (!videoMap.has(key)) {
                videoMap.set(key, {
                    url: src,
                    poster,
                    video_id: videoId || null,
                })
            }
        })
        const videos = Array.from(videoMap.values())
            .map((video) => {
                if (!video.url && !video.poster && !video.video_id) {
                    return null
                }
                return video
            })
            .filter(Boolean)

        const tags = Array.from(document.querySelectorAll('.movie-tag-list.artist .movie-tag-item'))
            .map((node) => clean(node.textContent))
            .filter(Boolean)

        const notes = clean(document.querySelector('.movie__notes')?.textContent)
        const bodyText = [tags.length > 0 ? tags.join(' ') : '', notes].filter(Boolean).join('\n\n')

        return {
            title: clean(document.querySelector('.movie__title')?.textContent),
            dateText: clean(document.querySelector('.movie__posted')?.textContent),
            bodyText: cleanMultiline(bodyText),
            bodyHtml: document.querySelector<HTMLElement>('.section-movie-content .content')?.innerHTML || '',
            member: null,
            media: videos
                .map((video: any) => {
                    if (!video.poster) {
                        return null
                    }
                    return {
                        type: 'video_thumbnail' as const,
                        url: video.poster,
                    }
                })
                .filter(Boolean),
            extraData: {
                notes: notes || null,
                tags,
                streams: videos,
            },
        }
    }, url)
}

async function extractLiveReportDetail(page: Page, url: string): Promise<WebsiteDetailPayload> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.regular-concert-content, .headline__text, .special .regular-concert', { timeout: 15000 })
    return page.evaluate((currentUrl) => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const cleanMultiline = (value?: string | null) =>
            (value || '')
                .replace(/\u00a0/g, ' ')
                .replace(/\r/g, '')
                .split('\n')
                .map((line) => line.replace(/[ \t]+/g, ' ').trim())
                .filter((line, index, arr) => Boolean(line) || (arr[index - 1] && arr[index + 1]))
                .join('\n')
                .replace(/\n{3,}/g, '\n\n')
                .trim()
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
        const body =
            document.querySelector<HTMLElement>('.regular-concert-content')
            || document.querySelector<HTMLElement>('.special .regular-concert')
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

        const headline =
            document.querySelector<HTMLElement>('.regular-concert-headline .headline__text')
            || document.querySelector<HTMLElement>('.regular-concert-headline')

        return {
            title: cleanMultiline(headline?.innerText || headline?.textContent)
                .replace(/\n+/g, ' ')
                .replace(/[<>]/g, ' ')
                .replace(/\s+/g, ' ')
                .trim(),
            dateText: '',
            bodyText: cleanMultiline(body?.innerText || body?.textContent),
            bodyHtml: body?.innerHTML || '',
            member: null,
            media,
        }
    }, url)
}

async function extractPhotoDetailArticles(
    page: Page,
    url: string,
    config: FeedConfig,
    listItem: WebsiteListItem,
): Promise<Array<GenericArticle<Platform.Website>>> {
    await page.goto(url, { waitUntil: 'domcontentloaded' })
    await page.waitForSelector('.photo-block, .photo-modal', { timeout: 15000 })

    const payload = await page.evaluate(() => {
        const clean = (value?: string | null) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim()
        const cleanMultiline = (value?: string | null) =>
            (value || '')
                .replace(/\u00a0/g, ' ')
                .replace(/\r/g, '')
                .split('\n')
                .map((line) => line.replace(/[ \t]+/g, ' ').trim())
                .filter((line, index, arr) => Boolean(line) || (arr[index - 1] && arr[index + 1]))
                .join('\n')
                .replace(/\n{3,}/g, '\n\n')
                .trim()
        const absolute = (value?: string | null) => {
            if (!value) {
                return null
            }
            try {
                return new URL(value, location.href).href
            } catch {
                return null
            }
        }

        const currentUrl = location.href
        const parsed = new URL(currentUrl)
        const albumId = parsed.searchParams.get('ct') || parsed.pathname.split('/').filter(Boolean).pop() || 'photo'
        const pageTheme = clean(document.querySelector('.headline__title')?.textContent)
        const modalDateMap = new Map<string, string>()

        for (const block of Array.from(document.querySelectorAll('.photo-block'))) {
            const dateText = clean(block.querySelector('.photo__posted')?.textContent)
            const modalIds = Array.from(block.querySelectorAll('.photo__img'))
                .map((anchor) => (anchor.getAttribute('href') || '').replace(/^#/, ''))
                .filter(Boolean)
            modalIds.forEach((modalId) => modalDateMap.set(modalId, dateText))
        }

        const entries = Array.from(document.querySelectorAll<HTMLElement>('.photo-modal'))
            .map((modal) => {
                const modalId = modal.id
                const theme = clean(modal.querySelector('.photo-modal-thema__title')?.textContent) || pageTheme
                const member = clean(modal.querySelector('.photo-modal__artiname')?.textContent)
                const photoUrl = absolute(modal.querySelector('.photo-modal__img img')?.getAttribute('src'))
                const avatarUrl = absolute(modal.querySelector('.photo-modal__artiimag img')?.getAttribute('src'))
                const text = cleanMultiline(modal.querySelector('.photo-modal__text')?.textContent)
                const dataCode = clean(modal.querySelector('.photo-modal-favorite__icon')?.getAttribute('data-code'))
                const title = [theme, member].filter(Boolean).join(' - ')
                if (!modalId) {
                    return null
                }
                return {
                    modalId,
                    dataCode: dataCode || null,
                    articleId: `${albumId}:${dataCode || modalId}`,
                    detailUrl: `${currentUrl}#${modalId}`,
                    title,
                    dateText: modalDateMap.get(modalId) || '',
                    member: member || null,
                    bodyText: text,
                    bodyHtml: modal.innerHTML,
                    media: photoUrl
                        ? [
                              {
                                  type: 'photo' as const,
                                  url: photoUrl,
                                  alt: member || undefined,
                              },
                          ]
                        : [],
                    uAvatar: avatarUrl,
                    extraData: {
                        album_id: albumId,
                        theme,
                        modal_id: modalId,
                        photo_code: dataCode || null,
                    },
                }
            })
            .filter(Boolean)

        return {
            currentUrl,
            albumId,
            pageTheme,
            entries,
        }
    }) as WebsitePhotoAlbumPayload

    return buildPhotoAlbumArticle(config, listItem, payload)
}

function extractListPage(page: Page, feedConfig: FeedConfig, url: string): Promise<WebsiteListPageResult> {
    switch (feedConfig.feed) {
        case 'official-news':
            return extractNewsList(page, url)
        case 'fc-news':
        case 'ticket':
            return extractStandardEntryList(page, url, {
                waitForSelector: '.entry-list .entry-item',
                itemSelector: '.entry-list .entry-item',
                detailSelector: 'a.panel',
                titleSelector: '.entry__title',
                dateSelector: '.entry__posted',
            })
        case 'official-blog':
            return extractBlogList(page, url)
        case 'radio':
            return extractRadioList(page, url)
        case 'movie':
            return extractMovieList(page, url)
        case 'photo':
            return extractPhotoList(page, url)
        case 'live-report':
            return extractLiveReportList(page, url)
        default:
            throw new Error(`Unsupported website feed: ${feedConfig.feed}`)
    }
}

function extractDetailPayload(page: Page, feedConfig: FeedConfig, url: string): Promise<WebsiteDetailPayload> {
    switch (feedConfig.feed) {
        case 'official-news':
        case 'fc-news':
            return extractNewsDetail(page, url)
        case 'official-blog':
            return extractBlogDetail(page, url)
        case 'ticket':
            return extractTicketDetail(page, url)
        case 'radio':
            return extractRadioDetail(page, url)
        case 'movie':
            return extractMovieDetail(page, url)
        case 'live-report':
            return extractLiveReportDetail(page, url)
        default:
            throw new Error(`Unsupported website detail feed: ${feedConfig.feed}`)
    }
}

class NanabunnonijyuuniWebsiteSpider extends BaseSpider {
    static _VALID_URL =
        /^https?:\/\/nanabunnonijyuuni-mobile\.com\/s\/n110\/(?:(?:news\/(?:list|detail\/[^/?#]+))|(?:ticket\/(?:list|detail\/[^/?#]+))|(?:diary\/(?:official_blog\/list|nananiji_movie(?:\/list)?|special\/list|detail\/\d+))|(?:contents_list)|(?:contents\/[^/?#]+)|(?:gallery(?:\/[^/?#]+)?))(?:\?.*)?$/i
    static _PLATFORM = Platform.Website
    BASE_URL = `https://${MOBILE_227_HOST}/`
    NAME = '22/7 Website Spider'

    static resolveFeed(url: string): FeedConfig | null {
        const parsed = tryParseWebsiteUrl(url)
        if (!parsed || parsed.hostname !== MOBILE_227_HOST) {
            return null
        }

        if (parsed.pathname === '/s/n110/news/list') {
            return parsed.searchParams.get('ct') === 'news' ? FEED_CONFIGS['fc-news'] : FEED_CONFIGS['official-news']
        }

        if (isNewsDetail(parsed.pathname)) {
            return FEED_CONFIGS['fc-news']
        }

        if (parsed.pathname === '/s/n110/diary/official_blog/list') {
            return FEED_CONFIGS['official-blog']
        }

        if (parsed.pathname === '/s/n110/ticket/list' || isTicketDetail(parsed.pathname)) {
            return FEED_CONFIGS.ticket
        }

        if (isRadioList(parsed) || isRadioDetail(parsed.pathname)) {
            return FEED_CONFIGS.radio
        }

        if (isMovieList(parsed) || (isDiaryDetail(parsed.pathname) && parsed.searchParams.get('cd') === 'nananiji_movie')) {
            return FEED_CONFIGS.movie
        }

        if (isPhotoList(parsed) || isPhotoDetail(parsed)) {
            return FEED_CONFIGS.photo
        }

        if (isLiveReportList(parsed) || (isDiaryDetail(parsed.pathname) && parsed.searchParams.get('cd') === 'special')) {
            return FEED_CONFIGS['live-report']
        }

        if (isDiaryDetail(parsed.pathname)) {
            return FEED_CONFIGS['official-blog']
        }

        return null
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

        if (isDetailUrl(feedConfig.feed, url)) {
            const articles = await this.crawlSingleDetail(page, feedConfig, {
                detailUrl: url,
                title: '',
                dateText: '',
            })
            return articles as TaskTypeResult<T, Platform.Website>
        }

        const articles = await this.crawlFeed(page, feedConfig, url)
        return articles as TaskTypeResult<T, Platform.Website>
    }

    private async crawlFeed(page: Page, feedConfig: FeedConfig, url: string) {
        const discovered = new Map<string, WebsiteListItem>()
        let currentUrl: string | null = url
        let pageCount = 0

        while (currentUrl && pageCount < MAX_LIST_PAGES) {
            const result = await extractListPage(page, feedConfig, currentUrl)
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
            articles.push(...(await this.crawlSingleDetail(page, feedConfig, item)))
        }

        return articles.sort((a, b) => b.created_at - a.created_at)
    }

    private async crawlSingleDetail(page: Page, feedConfig: FeedConfig, listItem: WebsiteListItem) {
        if (feedConfig.feed === 'photo') {
            return extractPhotoDetailArticles(page, listItem.detailUrl, feedConfig, listItem)
        }

        const detailPayload = await extractDetailPayload(page, feedConfig, listItem.detailUrl)
        return [buildWebsiteArticle(feedConfig, listItem.detailUrl, listItem, detailPayload)]
    }
}

export { NanabunnonijyuuniWebsiteSpider }
