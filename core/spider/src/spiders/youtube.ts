import { Platform } from '@/types'
import type { GenericMediaInfo, GenericArticle, TaskType, TaskTypeResult, CrawlEngine } from '@/types'
import { BaseSpider } from './base'
import { Page } from 'puppeteer-core'

import { JSONPath } from 'jsonpath-plus'
import { getCookieString, HTTPClient } from '@/utils'
import dayjs, { type ManipulateType } from 'dayjs'

enum ArticleTypeEnum {
    /**
     * https://www.youtube.com/@username/videos
     */
    VIDEO = 'video',
    /**
     * https://www.youtube.com/@username/shorts
     */
    SHORTS = 'shorts',
}

class YoutubeSpider extends BaseSpider {
    // extends from XBaseSpider regex
    static _VALID_URL = /(https:\/\/)?(www\.)?youtube\.com\/@(?<id>[^/?#]+)/
    static _PLATFORM = Platform.YouTube
    BASE_URL: string = 'https://www.youtube.com/'
    NAME: string = 'Youtube Generic Spider'

    async _crawl<T extends TaskType>(
        url: string,
        page: Page | undefined,
        config: {
            task_type: T
            crawl_engine: CrawlEngine
            sub_task_type?: Array<string>
            cookieString?: string
        },
    ): Promise<TaskTypeResult<T, Platform.YouTube>> {
        const result = super._match_valid_url(url, YoutubeSpider)?.groups
        if (!result) {
            throw new Error(`Invalid URL: ${url}`)
        }
        const { id } = result
        const _url = `${this.BASE_URL}@${id}`
        const { task_type } = config

        if (!page) {
            throw new Error('YouTube spider requires a Page instance')
        }

        if (task_type === 'article') {
            this.log?.info('Trying to grab videos and shorts.')
            const res = await YoutubeApiJsonParser.grabArticles(page, _url)

            return res as TaskTypeResult<T, Platform.YouTube>
        }

        throw new Error('Invalid task type')
    }
}

namespace YoutubeApiJsonParser {
    type YoutubeArticle = GenericArticle<Platform.YouTube>

    interface ChannelMeta {
        handle: string
        title: string
        avatar: string | null
    }

    interface YoutubeDetail {
        created_at: number
        title: string | null
        description: string | null
        thumbnail: string | null
    }

    const LOCALE_QUERY = 'hl=en&persist_hl=1&gl=US'

    function normalizeUrl(url?: string | null): string | null {
        if (!url) {
            return null
        }
        if (url.startsWith('//')) {
            return `https:${url}`
        }
        return url.replaceAll('\\u0026', '&')
    }

    function stripHandlePrefix(handle: string): string {
        return handle.replace(/^@/, '')
    }

    function addLocaleQuery(url: string): string {
        const _url = new URL(url)
        _url.searchParams.set('hl', 'en')
        _url.searchParams.set('persist_hl', '1')
        _url.searchParams.set('gl', 'US')
        return _url.toString()
    }

    function textParser(node: any): string {
        if (!node) {
            return ''
        }
        if (typeof node === 'string') {
            return node
        }
        if (typeof node?.simpleText === 'string') {
            return node.simpleText
        }
        if (typeof node?.content === 'string') {
            return node.content
        }
        if (Array.isArray(node?.runs)) {
            return node.runs.map((item: any) => textParser(item)).join('')
        }
        if (Array.isArray(node)) {
            return node.map((item) => textParser(item)).join('')
        }
        if (typeof node?.text === 'string') {
            return node.text
        }
        return ''
    }

    function pickLargestThumbnail(thumbnails?: Array<{ url?: string; width?: number }>): string | null {
        if (!Array.isArray(thumbnails) || thumbnails.length === 0) {
            return null
        }
        const sorted = [...thumbnails].sort((a, b) => (b?.width || 0) - (a?.width || 0))
        return normalizeUrl(sorted[0]?.url)
    }

    function thumbnailParser(node: any): string | null {
        return (
            pickLargestThumbnail(node?.thumbnails)
            || pickLargestThumbnail(node?.image?.thumbnails)
            || pickLargestThumbnail(node?.sources)
        )
    }

    function extractAssignedObject<T>(text: string, variableName: string): T | null {
        const assignmentIndex = text.indexOf(variableName)
        if (assignmentIndex === -1) {
            return null
        }
        const startIndex = text.indexOf('{', assignmentIndex)
        if (startIndex === -1) {
            return null
        }

        let depth = 0
        let inString = false
        let escaped = false
        for (let i = startIndex; i < text.length; i++) {
            const char = text[i]
            if (inString) {
                if (escaped) {
                    escaped = false
                } else if (char === '\\') {
                    escaped = true
                } else if (char === '"') {
                    inString = false
                }
                continue
            }

            if (char === '"') {
                inString = true
                continue
            }
            if (char === '{') {
                depth += 1
                continue
            }
            if (char === '}') {
                depth -= 1
                if (depth === 0) {
                    return JSON.parse(text.slice(startIndex, i + 1)) as T
                }
            }
        }
        return null
    }

    function buildContent(title?: string | null, description?: string | null): string | null {
        const parts = [title?.trim(), description?.trim()].filter((part): part is string => Boolean(part))
        if (parts.length === 0) {
            return null
        }
        if (parts.length === 2 && parts[0] === parts[1]) {
            return parts[0]
        }
        return parts.join('\n\n')
    }

    function mediaParser(url: string | null): Array<GenericMediaInfo> {
        if (!url) {
            return []
        }
        return [{
            type: 'video_thumbnail',
            url,
        }]
    }

    /**
     *
     * @param relativeTime like "1 hour ago", "2 days ago"
     * @description parse relative time to timestamp
     * @returns timestamp
     */
    function relativeTimeParser(relativeTime?: string | null): number {
        if (!relativeTime || !/ago/i.test(relativeTime)) {
            return 0
        }
        const matched = relativeTime.match(/(\d+)\s+(\w+)/)
        if (!matched) {
            return 0
        }
        const [, number, unit] = matched
        return dayjs()
            .subtract(parseInt(number || '0'), unit as ManipulateType)
            .unix()
    }

    export function channelMetaParser(json: any, fallbackHandle: string): ChannelMeta {
        if (!json) {
            return {
                handle: stripHandlePrefix(fallbackHandle),
                title: stripHandlePrefix(fallbackHandle),
                avatar: null,
            }
        }
        const header = JSONPath({
            path: '$..c4TabbedHeaderRenderer',
            json,
        })[0]
        const metadata = JSONPath({
            path: '$..channelMetadataRenderer',
            json,
        })[0]
        const handleText = textParser(header?.channelHandleText) || metadata?.vanityChannelUrl?.split('/').pop() || fallbackHandle
        return {
            handle: stripHandlePrefix(handleText || fallbackHandle),
            title: textParser(header?.title) || metadata?.title || stripHandlePrefix(fallbackHandle),
            avatar: pickLargestThumbnail(header?.avatar?.thumbnails) || pickLargestThumbnail(metadata?.avatar?.thumbnails),
        }
    }

    function videoParser(item: any, channelMeta: ChannelMeta): YoutubeArticle | null {
        const videoId = item?.videoId
        if (!videoId) {
            return null
        }
        const title = textParser(item?.title)
        const description = textParser(item?.descriptionSnippet)
        const thumbnail = thumbnailParser(item?.thumbnail)
        const media = mediaParser(thumbnail)
        return {
            platform: Platform.YouTube,
            a_id: videoId,
            u_id: channelMeta.handle,
            username: channelMeta.title,
            created_at: relativeTimeParser(textParser(item?.publishedTimeText)),
            content: buildContent(title, description),
            url: `https://www.youtube.com/watch?v=${videoId}`,
            type: ArticleTypeEnum.VIDEO,
            ref: null,
            has_media: media.length > 0,
            media,
            extra: null,
            u_avatar: channelMeta.avatar,
        }
    }

    function shortsParserItem(item: any, channelMeta: ChannelMeta): YoutubeArticle | null {
        const videoId = item?.onTap?.innertubeCommand?.reelWatchEndpoint?.videoId
            || item?.navigationEndpoint?.reelWatchEndpoint?.videoId
        if (!videoId) {
            return null
        }
        const title = textParser(item?.overlayMetadata?.primaryText) || textParser(item?.accessibilityText)
        const thumbnail = thumbnailParser(item?.thumbnail)
        const media = mediaParser(thumbnail)
        return {
            platform: Platform.YouTube,
            a_id: videoId,
            u_id: channelMeta.handle,
            username: channelMeta.title,
            created_at: relativeTimeParser(
                textParser(item?.timestampText)
                || textParser(item?.overlayMetadata?.secondaryText),
            ),
            content: buildContent(title, null),
            url: `https://www.youtube.com/shorts/${videoId}`,
            type: ArticleTypeEnum.SHORTS,
            ref: null,
            has_media: media.length > 0,
            media,
            extra: null,
            u_avatar: channelMeta.avatar,
        }
    }

    export function videosParser(json: any, channelMeta: ChannelMeta): Array<YoutubeArticle> {
        if (!json) {
            return []
        }
        const items = JSONPath({
            path: '$..videoRenderer',
            json,
        })
        return items
            .map((item: any) => videoParser(item, channelMeta))
            .filter((item): item is YoutubeArticle => Boolean(item))
    }

    export function shortsParser(json: any, channelMeta: ChannelMeta): Array<YoutubeArticle> {
        if (!json) {
            return []
        }
        const items = JSONPath({
            path: '$..shortsLockupViewModel',
            json,
        })
        return items
            .map((item: any) => shortsParserItem(item, channelMeta))
            .filter((item): item is YoutubeArticle => Boolean(item))
    }

    export function detailParser(text: string): YoutubeDetail {
        const initialPlayerResponse = extractAssignedObject<any>(text, 'ytInitialPlayerResponse')
        const microformat = initialPlayerResponse?.microformat?.playerMicroformatRenderer
        const videoDetails = initialPlayerResponse?.videoDetails
        const thumbnail = pickLargestThumbnail(microformat?.thumbnail?.thumbnails)
            || pickLargestThumbnail(videoDetails?.thumbnail?.thumbnails)
        const publishedAt = microformat?.publishDate || microformat?.uploadDate
        const created_at = publishedAt ? dayjs(publishedAt).unix() : 0
        return {
            created_at,
            title: videoDetails?.title || textParser(microformat?.title),
            description: videoDetails?.shortDescription || textParser(microformat?.description),
            thumbnail,
        }
    }

    async function hydrateArticle(article: YoutubeArticle, headers: Record<string, string>): Promise<YoutubeArticle> {
        const webpage = await HTTPClient.download_webpage(addLocaleQuery(article.url), headers)
        const detail = detailParser(await webpage.text())
        const media = detail.thumbnail ? mediaParser(detail.thumbnail) : article.media
        return {
            ...article,
            created_at: detail.created_at || article.created_at,
            content: buildContent(detail.title, detail.description) || article.content,
            has_media: Boolean(media && media.length > 0),
            media,
        }
    }

    /**
     * @param url https://www.youtube.com/@username
     * @description grab videos and shorts from html
     */
    export async function grabArticles(page: Page, url: string): Promise<Array<YoutubeArticle>> {
        const cookies = await page.browserContext().cookies()
        const headers = {
            'accept-language': 'en-US,en;q=0.9',
            cookie: getCookieString(cookies),
        }
        const fallbackHandle = stripHandlePrefix(url.split('/').pop() || '')
        const [videosPage, shortsPage] = await Promise.all([
            HTTPClient.download_webpage(`${url}/videos?${LOCALE_QUERY}`, headers),
            HTTPClient.download_webpage(`${url}/shorts?${LOCALE_QUERY}`, headers),
        ])
        const [videosText, shortsText] = await Promise.all([videosPage.text(), shortsPage.text()])
        const videosJson = extractAssignedObject<any>(videosText, 'ytInitialData')
        const shortsJson = extractAssignedObject<any>(shortsText, 'ytInitialData')
        if (!videosJson && !shortsJson) {
            throw new Error('Cannot find YouTube initial data')
        }

        const channelMeta = channelMetaParser(videosJson || shortsJson, fallbackHandle)
        const baseArticles = [...videosParser(videosJson, channelMeta), ...shortsParser(shortsJson, channelMeta)]

        const articles = (await Promise.allSettled(baseArticles.map((article) => hydrateArticle(article, headers))))
            .map((result, index) => (result.status === 'fulfilled' ? result.value : baseArticles[index]))

        const dedup = new Map<string, YoutubeArticle>()
        for (const article of articles) {
            dedup.set(article.a_id, article)
        }
        return Array.from(dedup.values()).sort((a, b) => b.created_at - a.created_at)
    }
}

export { ArticleTypeEnum, YoutubeApiJsonParser }
export { YoutubeSpider }
