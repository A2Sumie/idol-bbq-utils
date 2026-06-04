import { Platform } from '@/types'
import type {
    ArticleExtractType,
    GenericMediaInfo,
    GenericArticle,
    GenericFollows,
    TaskType,
    TaskTypeResult,
    CrawlEngine,
} from '@/types'
import { BaseSpider, waitForResponse } from './base'
import { Page } from 'puppeteer-core'

import { JSONPath } from 'jsonpath-plus'

enum ArticleTypeEnum {
    /**
     * basic page
     */
    POST = 'post',
    /**
     * https://www.instagram.com/stories/username
     */
    STORY = 'story',
    /**
     * https://www.instagram.com/stories/highlights/username
     */
    // HIGHLIGHTS = 'highlights',
    /**
     * TODO
     *
     * reels page
     */
    // REEL = 'reel',
}

interface InstagramProfileStatus {
    platform: Platform.Instagram
    u_id: string
    numeric_id: string | null
    username: string
    u_avatar: string | null
    live_broadcast_id: string | null
    live_broadcast_visibility: string | null
    is_live: boolean
    live_url: string | null
}

interface InstagramProfileContext {
    u_id: string
    username: string
    u_avatar: string | null
}

const INSTAGRAM_PROFILE_ID_PATTERN = /^[A-Za-z0-9._]+$/i
const RESERVED_INSTAGRAM_PATHS = new Set(['p', 'reel', 'reels', 'stories', 'explore', 'accounts', 'direct'])
const INSTAGRAM_AUTO_MEDIA_SUMMARY_PATTERNS = [
    /^(?:\d+\.\s*)?(?:may be (?:an?\s+|the\s+)?(?:image|photo|picture|video|reel|story|selfie|screenshot|meme|poster|text|closeup|one or more people)\b|(?:this\s+)?image may contain\b|no (?:photo|video) description available\b)/i,
    /^(?:\d+\.\s*)?(?:photo|video|image|reel|story) (?:by|shared by) .{1,160}? on .{3,80}\.\s*(?:may be\b|(?:this\s+)?image may contain\b|no (?:photo|video) description available\b)/i,
]

function sanitizeInstagramGeneratedText(text: unknown): string | null {
    if (typeof text !== 'string') {
        return null
    }

    const normalized = text.replace(/\s+/g, ' ').trim()
    if (!normalized) {
        return null
    }

    return INSTAGRAM_AUTO_MEDIA_SUMMARY_PATTERNS.some((pattern) => pattern.test(normalized)) ? null : normalized
}

function extractStoryAccessibilityText(caption: unknown): string | null {
    const normalized = sanitizeInstagramGeneratedText(caption)
    if (!normalized) {
        return null
    }

    const numberedText = normalized.match(/^\d+\.\s*(?<text>.*)$/)?.groups?.text
    return sanitizeInstagramGeneratedText(numberedText || normalized)
}

class InstagramSpider extends BaseSpider {
    // extends from XBaseSpider regex
    static _VALID_URL = /^(https:\/\/)?(www\.)?instagram\.com\/(?<id>[A-Za-z0-9._]+)(?:\/)?(?:\?.*)?$/i
    static _PLATFORM = Platform.Instagram
    BASE_URL: string = 'https://www.instagram.com/'
    NAME: string = 'Instagram Generic Spider'

    static extractBasicInfo(url: string) {
        try {
            const parsed = new URL(url)
            if (!/(^|\.)instagram\.com$/i.test(parsed.hostname)) {
                return undefined
            }

            const id = parsed.pathname.split('/').filter(Boolean)[0]
            if (!id || RESERVED_INSTAGRAM_PATHS.has(id.toLowerCase()) || !INSTAGRAM_PROFILE_ID_PATTERN.test(id)) {
                return undefined
            }

            return {
                u_id: id,
                platform: Platform.Instagram,
            }
        } catch {
            return undefined
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
    ): Promise<TaskTypeResult<T, Platform.Instagram>> {
        const result = super._match_valid_url(url, InstagramSpider)?.groups
        if (!result) {
            throw new Error(`Invalid URL: ${url}`)
        }
        const { id } = result
        const _url = `${this.BASE_URL}${id}`
        const { task_type } = config

        if (!page) {
            throw new Error('Instagram spider requires a Page instance')
        }

        if (task_type === 'article') {
            this.log?.info('Trying to grab posts.')
            const res = await InsApiJsonParser.grabPosts(page, _url)
            this.log?.info(`Trying to grab stories.`)
            const stories = await InsApiJsonParser.grabStories(page, `${this.BASE_URL}stories/${id}/`)
            return res.concat(stories) as TaskTypeResult<T, Platform.Instagram>
        }

        if (task_type === 'follows') {
            this.log?.info('Trying to grab follows.')
            return [await InsApiJsonParser.grabFollowsNumber(page, _url)] as TaskTypeResult<T, Platform.Instagram>
        }

        throw new Error('Invalid task type')
    }
}

namespace InsApiJsonParser {
    const GRAPHQL_FORM_QUERY_KEY = 'fb_api_req_friendly_name'

    const PROFILE_POSTS_KEY = 'PolarisProfilePostsQuery'
    const PROFILE_USER_KEY = 'PolarisProfilePageContentQuery'
    const PROFILE_HIGHLIGHTS_KEY = 'PolarisProfileStoryHighlightsTrayContentQuery'

    export function graphQLFriendlyNameFromRequest(
        url: string,
        method: string,
        postData: string | null | undefined,
    ): string | null {
        if (method !== 'POST' || !postData) {
            return null
        }

        const parseFriendlyName = (data: string) => {
            try {
                const friendlyName = new URLSearchParams(data).get(GRAPHQL_FORM_QUERY_KEY)
                return friendlyName?.trim() || null
            } catch {
                return null
            }
        }
        const decodePostData = (data: string) => {
            try {
                return decodeURIComponent(data)
            } catch {
                return data
            }
        }

        const friendlyName = parseFriendlyName(postData) || parseFriendlyName(decodePostData(postData))
        if (!friendlyName) {
            return null
        }

        return url.includes('/graphql/query') ||
            url.includes('/api/graphql') ||
            postData.includes(GRAPHQL_FORM_QUERY_KEY)
            ? friendlyName
            : null
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

    function parseEdges(json: any): any {
        const edges_json = JSONPath({ path: '$..edges', json })[0]
        if (!edges_json) {
            throw new Error('Edges json format may have changed')
        }
        return edges_json
    }

    function fallbackUsername(...candidates: Array<string | null | undefined>) {
        for (const candidate of candidates) {
            const normalized = candidate?.trim()
            if (normalized) {
                return normalized
            }
        }
        return ''
    }

    function normalizeInstagramUrl(url: unknown) {
        return typeof url === 'string' && url.trim() ? url.replace('\\u0026', '&') : null
    }

    function profileContextFromUser(user: any): InstagramProfileContext | null {
        const handle = fallbackUsername(user?.username)
        if (!handle) {
            return null
        }
        return {
            u_id: handle,
            username: fallbackUsername(user?.full_name, handle),
            u_avatar: normalizeInstagramUrl(
                user?.hd_profile_pic_url_info?.url || user?.profile_pic_url_hd || user?.profile_pic_url,
            ),
        }
    }

    function postProfileContext(node: any, crawledProfile: InstagramProfileContext | null) {
        const owner = profileContextFromUser(node?.user) || profileContextFromUser(node?.owner)
        if (!crawledProfile || !owner || crawledProfile.u_id === owner.u_id) {
            return null
        }

        return {
            data: {
                crawled_profile: crawledProfile,
                post_owner: owner,
            },
            extra_type: 'instagram_profile_context',
        } as ArticleExtractType<Platform.Instagram>
    }

    function mediaParser(edge: any): Array<GenericMediaInfo> {
        let arr = [] as Array<GenericMediaInfo>
        const pickBestCandidateUrl = (candidates: any): string | null => {
            if (!Array.isArray(candidates) || candidates.length === 0) {
                return null
            }
            return [...candidates].sort((a: any, b: any) => (b?.width || 0) - (a?.width || 0))[0]?.url || null
        }
        const pushMedia = (type: GenericMediaInfo['type'], url: string | null) => {
            if (!url) {
                return
            }
            arr.push({
                type,
                url,
            })
        }
        const pushNodeMedia = (node: any) => {
            const imageUrl = pickBestCandidateUrl(node?.image_versions2?.candidates)
            const videoUrl = pickBestCandidateUrl(node?.video_versions)
            if (videoUrl) {
                pushMedia('video_thumbnail', imageUrl)
                pushMedia('video', videoUrl)
                return
            }
            pushMedia('photo', imageUrl)
        }
        // cover
        const cover_candidates = edge?.image_versions2?.candidates
        if (cover_candidates) {
            pushNodeMedia(edge)
        }
        // video
        const video_candidates = edge?.video_versions
        if (video_candidates && !arr.some((item) => item.type === 'video')) {
            pushMedia('video', pickBestCandidateUrl(video_candidates))
        }
        // carousel
        const carousel_media = edge?.carousel_media
        if (carousel_media) {
            // If carousel exists, the top-level cover/video is only a preview for the carousel.
            arr = []
            carousel_media.forEach((media: any) => {
                pushNodeMedia(media)
            })
        }
        const dedup = new Map<string, GenericMediaInfo>()
        for (const media of arr) {
            if (!media.url) {
                continue
            }
            const normalizedUrl = media.url.replace('\\u0026', '&')
            dedup.set(`${media.type}:${normalizedUrl}`, {
                ...media,
                url: normalizedUrl,
            })
        }
        return Array.from(dedup.values())
    }

    function postParser(edge: any, crawledProfile: InstagramProfileContext | null): GenericArticle<Platform.Instagram> {
        const node = edge.node
        const owner = profileContextFromUser(node?.user) || profileContextFromUser(node?.owner)
        const handle = fallbackUsername(owner?.u_id, crawledProfile?.u_id)
        const displayName = fallbackUsername(owner?.username, crawledProfile?.username, handle)
        const avatarUrl = normalizeInstagramUrl(owner?.u_avatar || crawledProfile?.u_avatar)
        return {
            platform: Platform.Instagram,
            a_id: node?.code,
            u_id: handle,
            username: displayName,
            created_at: node?.taken_at,
            content: sanitizeInstagramGeneratedText(node?.caption?.text),
            url: `https://www.instagram.com/p/${node?.code}/`,
            type: ArticleTypeEnum.POST,
            ref: null,
            has_media: true,
            media: mediaParser(node),
            extra: postProfileContext(node, crawledProfile),
            u_avatar: avatarUrl,
        }
    }

    // function highlightParser(edge: any): GenericArticle<Platform.Instagram> {
    //     const node = edge.node
    //     const id = /\w+[:,](?<id>\d+)/.exec(node?.id)?.groups?.id ?? ''
    //     return {
    //         platform: Platform.Instagram,
    //         a_id: id,
    //         u_id: node?.user?.username,
    //         username: '',
    //         /**
    //          * TODO: notify when highlight updates
    //          */
    //         created_at: 0,
    //         content: node?.title,
    //         url: `https://www.instagram.com/stories/highlights/${id}/`,
    //         type: ArticleTypeEnum.HIGHLIGHTS,
    //         ref: null,
    //         has_media: true,
    //         media: null,
    //         extra: null,
    //         u_avatar: null,
    //     }
    // }
    function storyParser(item: any): GenericArticle<Platform.Instagram> {
        return {
            platform: Platform.Instagram,
            a_id: item?.id?.split('_')[0] || '',
            u_id: '',
            username: '',
            created_at: item?.taken_at,
            content: extractStoryAccessibilityText(item?.accessibility_caption),
            url: '',
            type: ArticleTypeEnum.STORY,
            ref: null,
            has_media: true,
            media: mediaParser(item),
            extra: null,
            u_avatar: '',
        }
    }

    // export function highlightsParser(json: any): Array<GenericArticle<Platform.Instagram>> {
    //     let edges = parseEdges(json)
    //     return edges.map(highlightParser)
    // }

    export function postsParser(json: any): Array<GenericArticle<Platform.Instagram>> {
        let edges = parseEdges(json)
        const crawledProfile = profileContextFromUser(json?.data?.user)
        return edges.map((edge: any) => postParser(edge, crawledProfile))
    }

    export function followsParser(json: any): GenericFollows {
        if (!json) {
            throw new Error('Profile format may have changed')
        }
        let user = json?.data?.user
        return {
            platform: Platform.Instagram,
            username: fallbackUsername(user?.full_name, user?.username),
            u_id: fallbackUsername(user?.username),
            followers: user?.follower_count,
        }
    }

    export function profileStatusParser(json: any): InstagramProfileStatus {
        if (!json) {
            throw new Error('Profile format may have changed')
        }
        const user = json?.data?.user
        const handle = fallbackUsername(user?.username)
        const displayName = fallbackUsername(user?.full_name, handle)
        const liveBroadcastId = user?.live_broadcast_id ? String(user.live_broadcast_id) : null
        const visibility = user?.live_broadcast_visibility ? String(user.live_broadcast_visibility) : null
        const avatar = user?.hd_profile_pic_url_info?.url || user?.profile_pic_url_hd || user?.profile_pic_url || null

        return {
            platform: Platform.Instagram,
            u_id: handle,
            numeric_id: user?.id ? String(user.id) : null,
            username: displayName,
            u_avatar: avatar ? String(avatar).replace('\\u0026', '&') : null,
            live_broadcast_id: liveBroadcastId,
            live_broadcast_visibility: visibility,
            is_live: Boolean(liveBroadcastId),
            live_url: handle && liveBroadcastId ? `https://www.instagram.com/${handle}/live/` : null,
        }
    }

    const USERNAME_REGEX_FROM_OG_TITLE =
        /(?:趁\s*(?<username>.*?)\s*的这条快拍|Watch this story by (?<username>.*?) on Instagram)/i
    async function storiesParser(json: any, page: Page): Promise<Array<GenericArticle<Platform.Instagram>>> {
        const reels_media = JSONPath({ path: '$..reels_media', json })[0]
        if (!Array.isArray(reels_media) || reels_media.length === 0) {
            return []
        }
        const res = reels_media
            .map((i: any) => {
                const ownerHandle = fallbackUsername(i.user?.username)
                const ownerName = fallbackUsername(i.user?.full_name, ownerHandle)
                const stories = (Array.isArray(i.items) ? i.items : [])
                    .map((item: any) => storyParser(item))
                    .map((item: any) => {
                        return {
                            ...item,
                            u_id: ownerHandle,
                            username: ownerName,
                            url: `https://www.instagram.com/stories/${ownerHandle}/${item.a_id}`,
                            u_avatar: i.user?.profile_pic_url,
                        }
                    })
                return stories
            })
            .flat()
        const og_title = await page.$('meta[property="og:title"]')
        const title = await og_title?.evaluate((el) => el.getAttribute('content'))
        const username = fallbackUsername(title?.match(USERNAME_REGEX_FROM_OG_TITLE)?.groups?.username)
        for (const item of res) {
            item.username = fallbackUsername(username, item.username, item.u_id)
        }
        return res
    }

    /**
     * @param url https://www.instagram.com/username
     * @description grab common posts from user page
     */
    export async function grabPosts(
        page: Page,
        url: string,
        config: {
            viewport?: {
                width: number
                height: number
            }
        } = {},
    ): Promise<Array<GenericArticle<Platform.Instagram>>> {
        const { cleanup, promise: waitForTweets } = waitForResponse(page, async (response, { done, fail }) => {
            const url = response.url()
            const request = response.request()
            const friendlyName = graphQLFriendlyNameFromRequest(url, request.method(), request.postData())
            if (friendlyName !== PROFILE_POSTS_KEY) {
                return
            }
            if (response.status() >= 400) {
                fail(new Error(`Error: ${response.status()}`))
                return
            }
            try {
                done(await response.json())
            } catch (e) {
                fail(e)
            }
        })
        if (config.viewport) {
            await page.setViewport(config.viewport)
        }
        await page.goto(url)
        try {
            await checkLogin(page)
            await checkSomethingWrong(page)
        } catch (error) {
            cleanup()
            throw error
        }

        const data = await waitForTweets
        if (!data.success) {
            throw data.error
        }
        const posts = postsParser(data.data)
        // const highlights = highlightsParser(reasonable_jsons[PROFILE_HIGHLIGHTS_KEY]).map((h) => {
        //     h.username = posts[0]?.username ?? ''
        //     h.u_avatar = posts[0]?.u_avatar ?? ''
        //     return h
        // })
        return posts
    }

    /** 由于使用了bun做运行时，无法使用xpath做内容筛选
     *
     * https://github.com/puppeteer/puppeteer/issues/12570
     *
     * https://github.com/oven-sh/bun/issues/13853
     */
    export async function grabStories(
        page: Page,
        url: string,
        config: {
            viewport?: {
                width: number
                height: number
            }
        } = {},
    ): Promise<Array<GenericArticle<Platform.Instagram>>> {
        if (config.viewport) {
            await page.setViewport(config.viewport)
        }
        await page.goto(url)
        try {
            await checkLogin(page)
            await checkSomethingWrong(page)
        } catch (error) {
            throw error
        }
        /**
         * Xpath selector for stories json, but not working in bun with puppeteer version after 22.10+
         */
        // const stores_json = await page.$('::-p-xpath(//script[@type="application/json"])')
        const json_script_tags = await page.$$('script[type="application/json"]')
        for (const json_script_tag of json_script_tags) {
            const text = await json_script_tag.evaluate((el) => el.innerText)
            if (text.includes('xdt_api__v1__feed__reels_media')) {
                return await storiesParser(JSON.parse(text), page)
            }
        }
        return []
    }

    export async function grabFollowsNumber(page: Page, url: string): Promise<GenericFollows> {
        const follows_json = await grabProfileUserPayload(page, url)
        return followsParser(follows_json)
    }

    export async function grabProfileStatus(page: Page, url: string): Promise<InstagramProfileStatus> {
        const profile_json = await grabProfileUserPayload(page, url)
        return profileStatusParser(profile_json)
    }

    async function grabProfileUserPayload(page: Page, url: string) {
        const { cleanup, promise: waitForTweets } = waitForResponse(page, async (response, { done, fail }) => {
            const url = response.url()
            const request = response.request()
            const friendlyName = graphQLFriendlyNameFromRequest(url, request.method(), request.postData())
            if (friendlyName !== PROFILE_USER_KEY) {
                return
            }
            if (response.status() >= 400) {
                fail(new Error(`Error: ${response.status()}`))
                return
            }
            try {
                done(await response.json())
            } catch (e) {
                fail(e)
            }
        })
        await page.goto(url)
        try {
            await checkLogin(page)
            await checkSomethingWrong(page)
        } catch (error) {
            cleanup()
            throw error
        }
        const data = await waitForTweets
        if (!data.success) {
            throw data.error
        }
        return data.data
    }
}

export { ArticleTypeEnum, InsApiJsonParser }
export type { InstagramProfileStatus }
export { InstagramSpider }
