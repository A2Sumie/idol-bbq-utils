import { Platform } from '@/types'
import type { GenericMediaInfo, GenericArticle, GenericFollows, TaskType, TaskTypeResult, CrawlEngine } from '@/types'
import { BaseSpider } from './base'
import { Page } from 'puppeteer-core'

import { JSONPath } from 'jsonpath-plus'
import { getCookieString, HTTPClient, SimpleExpiringCache } from '@/utils'

const TIKTOK_HTTP_TIMEOUT_MS = 15000

enum ArticleTypeEnum {
    /**
     * basic page: https://www.tiktok.com/api/post/item_list/
     */
    POST = 'post',
}

class TiktokSpider extends BaseSpider {
    // extends from XBaseSpider regex
    static _VALID_URL = /^(https:\/\/)?(www\.)?tiktok\.com\/@(?<id>[A-Za-z0-9._]+)(?:\/video\/(?<videoId>\d+)\/?)?(?:\?.*)?$/i
    static _PLATFORM = Platform.TikTok
    BASE_URL: string = 'https://www.tiktok.com/'
    NAME: string = 'Tiktok Generic Spider'

    private cache: SimpleExpiringCache = new SimpleExpiringCache()
    private expire: number = 60 * 3 // 3 minutes

    async _crawl<T extends TaskType>(
        url: string,
        page: Page | undefined,
        config: {
            task_type: T
            crawl_engine: CrawlEngine
            sub_task_type?: Array<string>
            cookieString?: string
        },
    ): Promise<TaskTypeResult<T, Platform.TikTok>> {
        const result = super._match_valid_url(url, TiktokSpider)?.groups
        if (!result) {
            throw new Error(`Invalid URL: ${url}`)
        }
        let random_hex7 = this.cache.get('random_hex7')
        if (!random_hex7) {
            random_hex7 = TiktokApiJsonParser.randomHexString(7)
            this.cache.set('random_hex7', random_hex7, this.expire)
        }
        let device_id = this.cache.get('device_id')
        if (!device_id) {
            device_id = TiktokApiJsonParser.randomDeviceId().toString()
            this.cache.set('device_id', device_id, this.expire)
        }
        const { id, videoId } = result
        const _url = `${this.BASE_URL}@${id}`
        const videoUrl = videoId ? `${_url}/video/${videoId}/` : null
        const cookieString =
            config.cookieString || (page ? getCookieString(await page.browserContext().cookies()) : undefined)
        const { task_type } = config
        if (task_type === 'article') {
            this.log?.info(videoUrl ? 'Trying to grab video.' : 'Trying to grab posts.')
            const res = videoUrl
                ? await TiktokApiJsonParser.grabVideo(videoUrl, page, cookieString)
                : await TiktokApiJsonParser.grabPosts(_url, random_hex7, Number(device_id), page, cookieString)
            return res as TaskTypeResult<T, Platform.TikTok>
        }

        if (task_type === 'follows') {
            this.log?.info('Trying to grab follows.')
            return [
                await TiktokApiJsonParser.grabFollowsNumber(_url, random_hex7, Number(device_id), page, cookieString),
            ] as TaskTypeResult<T, Platform.TikTok>
        }

        throw new Error('Invalid task type')
    }
}

namespace TiktokApiJsonParser {
    const BelowRange = 7250000000000000000
    const AboveRange = 7351147085025500000
    const _API_BASE_URL = 'https://www.tiktok.com/api/creator/item_list/'

    const hex_digits = '0123456789abcdefABCDEF'

    export function randomHexString(length: number): string {
        return Array.from({ length }, () => hex_digits[Math.floor(Math.random() * hex_digits.length)]).join('')
    }

    export function randomDeviceId(): number {
        return Math.floor(Math.random() * (AboveRange - BelowRange + 1) + BelowRange)
    }

    async function checkLogin(page: Page) {
        const login_form = await page.waitForSelector('form[id="loginForm"]', { timeout: 1000 }).catch(() => null)
        if (login_form) {
            throw new Error('You need to login first, check your cookies')
        }
    }

    async function checkSomethingWrong(page: Page) {
        const main_frame_error = await page
            .waitForSelector('div[id="main-frame-error"]', { timeout: 1000 })
            .catch(() => null)
        if (main_frame_error) {
            const error_content = (await main_frame_error.evaluate((e) => e.textContent))?.replace(/\s+/g, ' ')
            throw new Error(`Something wrong on the page: ${error_content}`)
        }
    }

    function buildHeaders(url: string, cookieString?: string): Record<string, string> {
        const headers: Record<string, string> = {
            'accept-language': 'en-US,en;q=0.9',
            referer: url,
        }
        if (cookieString?.trim()) {
            headers.cookie = cookieString
        }
        return headers
    }

    function extractUniversalData(text: string): string | null {
        return text.match(/<script\s*id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)<\/script>/)?.[1] || null
    }

    function pickUrl(value: any): string | null {
        if (!value) {
            return null
        }
        if (typeof value === 'string') {
            return value
        }
        if (Array.isArray(value)) {
            return (
                value.find((item) => typeof item === 'string' && item.includes('aweme/v1/play')) ||
                value.find((item) => typeof item === 'string') ||
                null
            )
        }
        return pickUrl(
            value.UrlList ||
                value.url_list ||
                value.PlayAddr?.UrlList ||
                value.playAddr?.url_list ||
                value.Data ||
                value.src,
        )
    }

    async function loadUniversalData(url: string, page?: Page, cookieString?: string): Promise<string> {
        const headers = buildHeaders(url, cookieString)
        const webpage = await HTTPClient.download_webpage(url, headers, { timeout: TIKTOK_HTTP_TIMEOUT_MS })
        const text = await webpage.text()
        const content = extractUniversalData(text)
        if (content) {
            return content
        }

        if (!page) {
            throw new Error('Cannot find user data (fetch blocked, no browser fallback available)')
        }

        await page.goto(url, {
            waitUntil: 'domcontentloaded',
        })
        await checkLogin(page)
        await checkSomethingWrong(page)
        await page
            .waitForSelector('script[id="__UNIVERSAL_DATA_FOR_REHYDRATION__"]', { timeout: 5000 })
            .catch(() => null)
        const browserContent = extractUniversalData(await page.content())
        if (browserContent) {
            return browserContent
        }

        throw new Error('Cannot find user data (browser hydration missing)')
    }

    function mediaParser(item: any): Array<GenericMediaInfo> {
        const video = item?.video
        if (!video) {
            return []
        }

        const arr = [] as Array<GenericMediaInfo>
        const pushMedia = (type: GenericMediaInfo['type'], value?: unknown) => {
            const url = pickUrl(value)
            if (!url) {
                return
            }
            arr.push({
                type,
                url: url.replaceAll('\\u0026', '&'),
            })
        }

        // cover
        pushMedia('video_thumbnail', video.cover)
        pushMedia('video_thumbnail', video.originCover)
        pushMedia('video_thumbnail', video.dynamicCover)

        // Prefer the best playable address, but never fail the whole post if bitrate metadata is missing.
        const bitrateInfo = Array.isArray(video.bitrateInfo) ? [...video.bitrateInfo] : []
        const bestBitrate = bitrateInfo.sort(
            (a: any, b: any) => (b?.Bitrate || b?.bitrate || 0) - (a?.Bitrate || a?.bitrate || 0),
        )[0]
        pushMedia('video', bestBitrate?.PlayAddr || bestBitrate?.playAddr)
        pushMedia('video', video.playAddr)
        pushMedia('video', video.downloadAddr)

        const dedup = new Map<string, GenericMediaInfo>()
        for (const media of arr) {
            dedup.set(`${media.type}:${media.url}`, media)
        }
        return Array.from(dedup.values())
    }

    function postParser(item: any): GenericArticle<Platform.TikTok> {
        const author = item?.author
        const media = mediaParser(item)
        return {
            platform: Platform.TikTok,
            a_id: item?.id,
            u_id: author?.uniqueId,
            username: author?.nickname,
            created_at: item?.createTime,
            content: item?.desc,
            url: `https://www.tiktok.com/@${author?.uniqueId}/video/${item?.id}/`,
            type: ArticleTypeEnum.POST,
            ref: null,
            has_media: media.length > 0,
            media,
            extra: null,
            u_avatar: pickUrl(author?.avatarLarger)?.replace('\\u0026', '&') || null,
        }
    }

    export function postsParser(json: any): Array<GenericArticle<Platform.TikTok>> {
        let items = json?.itemList
        if (!Array.isArray(items)) {
            return []
        }
        return items.map(postParser).filter((item: GenericArticle<Platform.TikTok>) => item.a_id && item.u_id)
    }

    function mergePostsById(
        primary: Array<GenericArticle<Platform.TikTok>>,
        secondary: Array<GenericArticle<Platform.TikTok>>,
    ): Array<GenericArticle<Platform.TikTok>> {
        const merged = [...primary]
        const seen = new Set(primary.map((post) => post.a_id))
        for (const post of secondary) {
            if (post.a_id && !seen.has(post.a_id)) {
                seen.add(post.a_id)
                merged.push(post)
            }
        }
        return merged
    }

    function normalizeHandle(value?: string | null) {
        return String(value || '')
            .trim()
            .replace(/^@+/, '')
            .toLowerCase()
    }

    function findUserInfoForHandle(json: any, handle: string) {
        const target = normalizeHandle(handle)
        const candidates = JSONPath({
            path: '$..userInfo',
            json,
            resultType: 'value',
        }) as Array<any>
        return (
            candidates.find((candidate) => normalizeHandle(candidate?.user?.uniqueId || candidate?.user?.username) === target) ||
            null
        )
    }

    function itemModuleValues(json: any): Array<any> {
        const modules = JSONPath({
            path: '$..ItemModule',
            json,
            resultType: 'value',
        }) as Array<any>
        const first = modules.find((module) => module && typeof module === 'object' && !Array.isArray(module))
        return first ? Object.values(first) : []
    }

    function universalScope(json: any) {
        return json?.__DEFAULT_SCOPE__ || json
    }

    export function videoParser(json: any): Array<GenericArticle<Platform.TikTok>> {
        const scope = universalScope(json)
        const item = scope?.['webapp.video-detail']?.itemInfo?.itemStruct || json?.itemInfo?.itemStruct || json?.itemStruct
        return item ? postsParser({ itemList: [item] }) : []
    }

    export function followsParser(json: any): GenericFollows {
        if (!json) {
            throw new Error('Profile format may have changed')
        }
        const userInfo = JSONPath({
            path: "$..['webapp.user-detail'].userInfo",
            json,
            resultType: 'value',
        })[0]
        const user =
            userInfo?.user || json?.data?.userInfo?.user || json?.userInfo?.user || json?.data?.user || json?.user
        const stats =
            userInfo?.stats || json?.data?.userInfo?.stats || json?.userInfo?.stats || json?.data?.stats || json?.stats
        return {
            platform: Platform.TikTok,
            username: user?.nickname || user?.full_name || user?.uniqueId || user?.username || '',
            u_id: user?.uniqueId || user?.username || '',
            followers: stats?.followerCount ?? user?.follower_count ?? 0,
        }
    }

    /**
     *  // ref: https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/extractor/tiktok.py
     */
    function _build_web_query(sec_uid: string, cursor: number, device_id: number, random7: string) {
        return {
            aid: '1988',
            app_language: 'en',
            app_name: 'tiktok_web',
            browser_language: 'en-US',
            browser_name: 'Mozilla',
            browser_online: 'true',
            browser_platform: 'Win32',
            browser_version: '5.0 (Windows)',
            channel: 'tiktok_web',
            cookie_enabled: 'true',
            count: '15',
            cursor: cursor,
            device_id: device_id,
            device_platform: 'web_pc',
            focus_state: 'true',
            from_page: 'user',
            history_len: '2',
            is_fullscreen: 'false',
            is_page_visible: 'true',
            language: 'en',
            os: 'windows',
            priority_region: '',
            referer: '',
            region: 'US',
            screen_height: '1080',
            screen_width: '1920',
            secUid: sec_uid,
            type: '1',
            tz_name: 'UTC',
            verifyFp: `verify_${random7}`,
            webcast_language: 'en',
        }
    }

    /**
     * @param url https://www.tiktok.com/@username
     * @description grab common posts from api
     */
    export async function grabPosts(
        url: string,
        random_hex7: string,
        device_id: number,
        page?: Page,
        cookieString?: string,
    ): Promise<Array<GenericArticle<Platform.TikTok>>> {
        // const { cleanup, promise: waitForTweets } = waitForResponse(page, async (response, { done, fail }) => {
        //     const url = response.url()
        //     const request = response.request()
        //     if (url.includes('/api/post/item_list') && request.method() === 'GET') {
        //         if (response.status() >= 400) {
        //             fail(new Error(`Error: ${response.status()}`))
        //             return
        //         }
        //         // will get empty response from api
        //         response
        //             .json()
        //             .then((json) => {
        //                 done(json)
        //             })
        //             .catch((error) => {
        //                 fail(error)
        //             })
        //     }
        // })
        // await page.setViewport(config.viewport ?? defaultViewport)
        // await page.goto(url)
        // try {
        //     // await checkLogin(page)
        //     // await checkSomethingWrong(page)
        // } catch (error) {
        //     cleanup()
        //     throw error
        // }
        // return postsParser(posts_json)
        /**
         * Use api query instead of headless browser
         */
        // ref: https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/extractor/tiktok.py
        const content = await loadUniversalData(url, page, cookieString)
        const universalData = JSON.parse(content)
        const handle = url.match(/\/\@([^/?]+)/)?.[1] || ''
        const userInfo = findUserInfoForHandle(universalData, handle)
        const userItems = Array.isArray(userInfo?.itemList) ? userInfo.itemList : []
        const fallbackItems = userItems.length > 0 ? [] : itemModuleValues(universalData)
        const pagePosts = postsParser({ itemList: userItems.length > 0 ? userItems : fallbackItems })
        const secUid = userInfo?.user?.secUid
        if (!secUid) {
            return pagePosts
        }
        let apiFailure: unknown
        let apiPosts: Array<GenericArticle<Platform.TikTok>> = []
        try {
            const query_obj = _build_web_query(secUid, Date.now(), device_id, random_hex7)
            // @ts-ignore
            const query = new URLSearchParams(query_obj)
            const res = await HTTPClient.download_webpage(
                `${_API_BASE_URL}?${query.toString()}`,
                buildHeaders(url, cookieString),
                { timeout: TIKTOK_HTTP_TIMEOUT_MS },
            )
            const json = await res.json()
            if (Array.isArray(json?.itemList)) {
                apiPosts = postsParser(json)
            } else {
                apiFailure = new Error(
                    `TikTok creator API returned no itemList (statusCode=${json?.statusCode ?? 'unknown'}); the unsigned API likely rejected the request`,
                )
            }
        } catch (error) {
            apiFailure = error
        }
        if (apiPosts.length === 0) {
            if (pagePosts.length === 0 && apiFailure) {
                throw apiFailure
            }
            return pagePosts
        }
        return mergePostsById(pagePosts, apiPosts)
    }

    export async function grabVideo(
        url: string,
        page?: Page,
        cookieString?: string,
    ): Promise<Array<GenericArticle<Platform.TikTok>>> {
        const content = await loadUniversalData(url, page, cookieString)
        return videoParser(JSON.parse(content))
    }

    export async function grabFollowsNumber(
        url: string,
        random_hex7: string,
        device_id: number,
        page?: Page,
        cookieString?: string,
    ): Promise<GenericFollows> {
        const content = await loadUniversalData(url, page, cookieString)
        const userInfo = JSONPath({
            path: "$..['webapp.user-detail'].userInfo",
            json: JSON.parse(content),
            resultType: 'value',
        })[0]
        return {
            followers: userInfo?.stats?.followerCount,
            platform: Platform.TikTok,
            username: userInfo?.user?.nickname,
            u_id: userInfo?.user?.uniqueId,
        }
    }
}

export { ArticleTypeEnum, TiktokApiJsonParser }
export { TiktokSpider }
