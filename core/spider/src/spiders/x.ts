import { Platform } from '@/types'
import type {
    ArticleExtractType,
    CrawlEngine,
    GenericArticle,
    GenericArticleRef,
    GenericFollows,
    GenericMediaInfo,
    TaskType,
    TaskTypeResult,
} from '@/types'
import { BaseSpider } from './base'
import { Page } from 'puppeteer-core'
import { JSONPath } from 'jsonpath-plus'
import type { Logger } from '@idol-bbq-utils/log'
import { waitForResponse } from '@/spiders/base'
import { defaultViewport } from './base'
import { UserAgent } from '@/utils'
import { v4 as uuidv4 } from 'uuid'
import { noop } from 'puppeteer-core/lib/esm/third_party/rxjs/rxjs.js'

type XListApiEngine = 'api-statuses' | 'api-member' | 'api-graphql' | 'api-unified'

enum ArticleTypeEnum {
    /**
     *
     */
    TWEET = 'tweet',
    RETWEET = 'retweet',
    QUOTED = 'quoted',
    CONVERSATION = 'conversation',
}

const X_BASE_VALID_URL = /(https:\/\/)?(www\.)?x\.com\//

enum XApis {
    UserTweets = 'UserTweets',
    UserTweetsAndReplies = 'UserTweetsAndReplies',
    UserByScreenName = 'UserByScreenName',
    ListLatestTweetsTimeline = 'ListLatestTweetsTimeline',
    ListMembers = 'ListMembers',
}

const DEFAULT_QUERY_APIS = [
    XApis.UserTweets,
    XApis.UserTweetsAndReplies,
    XApis.UserByScreenName,
    XApis.ListLatestTweetsTimeline,
] as Array<XApis>

const CAPTURED_HEADER_KEYS = new Set([
    'accept-language',
    'authorization',
    'content-type',
    'referer',
    'sec-ch-ua',
    'sec-ch-ua-mobile',
    'sec-ch-ua-platform',
    'user-agent',
    'x-client-transaction-id',
    'x-csrf-token',
    'x-twitter-active-user',
    'x-twitter-auth-type',
    'x-twitter-client-language',
])

enum XTweetsTaskType {
    tweets = 'tweets',
    replies = 'replies',
}

const X_UNIFIED_LIST_MAX_HYDRATED_USERS = 10
const X_UNIFIED_LIST_CONCURRENCY = 4
const X_UNIFIED_LIST_MEMBER_CURSORS = new Map<string, number>()

interface XOperationProfile {
    queryId: string
    url: string
    headers: Record<string, string>
    capturedAt: number
}

function normalizeRequestHeaders(headers?: Record<string, string>) {
    return Object.fromEntries(
        Object.entries(headers || {}).filter(
            ([key, value]) => typeof key === 'string' && key.trim() && typeof value === 'string' && value.trim(),
        ),
    )
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

class XUserTimeLineSpider extends BaseSpider {
    // extends from XBaseSpider regex
    static _VALID_URL = new RegExp(X_BASE_VALID_URL.source + /(?<id>\w+)$/.source)
    static _PLATFORM = Platform.X
    BASE_URL: string = 'https://x.com/'
    NAME: string = 'X TimeLine Spider'

    init(): this {
        super.init()
        return this
    }

    async _crawl<T extends TaskType>(
        url: string,
        page: Page | undefined,
        config: {
            crawl_engine: CrawlEngine
            task_type: T
            sub_task_type?: Array<string>
            hydrate_users?: Array<string>
            hydrate_limit?: number
            cookieString?: string
            requestHeaders?: Record<string, string>
        },
    ): Promise<TaskTypeResult<T, Platform.X>> {
        const result = super._match_valid_url(url, XUserTimeLineSpider)?.groups
        if (!result) {
            throw new Error(`Invalid URL: ${url}`)
        }
        const { id } = result
        if (!id) {
            throw new Error(`Invalid URL: ${url}, id not found`)
        }

        const { crawl_engine, task_type, sub_task_type, cookieString, requestHeaders } = config
        const apiClient = new XApiClient(requestHeaders, page, this.log)

        if (crawl_engine === 'api') {
            this.log?.warn(`[Engine Api] API engine will be banned by X if you use it too much`)
            try {
                let cookie_string = cookieString
                if (!cookie_string && page) {
                    const cookie = await page.browserContext().cookies()
                    cookie_string = cookie.map((c) => `${c.name}=${c.value}`).join('; ')
                }
                if (!cookie_string) {
                    throw new Error('Cookie string is required for API mode')
                }

                await apiClient.prepareUserOperations(id, {
                    needTweets:
                        task_type === 'article' &&
                        (!sub_task_type ||
                            sub_task_type.length === 0 ||
                            sub_task_type.includes(XTweetsTaskType.tweets)),
                    needReplies:
                        task_type === 'article' &&
                        (!sub_task_type ||
                            sub_task_type.length === 0 ||
                            sub_task_type.includes(XTweetsTaskType.replies)),
                })

                if (task_type === 'article') {
                    let res = []
                    if (
                        !sub_task_type ||
                        sub_task_type.length === 0 ||
                        sub_task_type.includes(XTweetsTaskType.tweets)
                    ) {
                        this.log?.info(`Trying to grab tweets for ${id}.`)
                        const tweets = await apiClient.grabTweets(id, cookie_string)
                        res.push(...tweets)
                    }
                    if (
                        !sub_task_type ||
                        sub_task_type.length === 0 ||
                        sub_task_type.includes(XTweetsTaskType.replies)
                    ) {
                        this.log?.info(`Trying to grab replies for ${id}.`)
                        const replies = await apiClient.grabReplies(id, cookie_string)
                        res.push(...replies)
                    }
                    return res as TaskTypeResult<T, Platform.X>
                }

                if (task_type === 'follows') {
                    this.log?.info(`Trying to grab follows for ${id}.`)
                    return [await apiClient.grabFollowsNumber(id, cookie_string)] as TaskTypeResult<T, Platform.X>
                }
            } catch (e) {
                this.log?.error(`[Engine Api] Failed to crawl with for ${id}: ${e}, fallback to browser`)
            } finally {
                noop()
            }
        }

        if (!page) {
            throw new Error('Browser mode requires a Page instance')
        }

        const _url = `${this.BASE_URL}${id}`
        if (task_type === 'article') {
            let res = []
            if (!sub_task_type || sub_task_type.length === 0 || sub_task_type.includes(XTweetsTaskType.tweets)) {
                this.log?.info(`Trying to grab tweets for ${id}.`)
                const tweets = await XApiJsonParser.grabTweets(page, _url)
                res.push(...tweets)
            }
            if (!sub_task_type || sub_task_type.length === 0 || sub_task_type.includes(XTweetsTaskType.replies)) {
                this.log?.info(`Trying to grab replies for ${id}.`)
                const replies = await XApiJsonParser.grabReplies(page, _url + '/with_replies')
                res.push(...replies)
            }
            return res as TaskTypeResult<T, Platform.X>
        }

        if (task_type === 'follows') {
            this.log?.info(`Trying to grab follows for ${id}.`)
            return [await XApiJsonParser.grabFollowsNumber(page, _url)] as TaskTypeResult<T, Platform.X>
        }

        throw new Error('Invalid task type')
    }
}

class XListSpider extends BaseSpider {
    static _VALID_URL = new RegExp(X_BASE_VALID_URL.source + /\i\/lists\/(?<id>\d+)/.source)
    static _PLATFORM = Platform.X
    BASE_URL: string = 'https://x.com/'
    NAME: string = 'X OldApi Spider'
    API_PREFIX = 'https://api.twitter.com'

    PUBLIC_TOKEN =
        'Bearer AAAAAAAAAAAAAAAAAAAAAFQODgEAAAAAVHTp76lzh3rFzcHbmHVvQxYYpTw%3DckAlMINMjmCwxUcaXbAN4XqJVdgMJaHqNOFgPMK0zN1qLqLQCF'

    async _crawl<T extends TaskType>(
        url: string,
        page: Page | undefined,
        config: {
            crawl_engine: CrawlEngine
            task_type: T
            sub_task_type?: Array<string>
            hydrate_users?: Array<string>
            hydrate_limit?: number
            cookieString?: string
            requestHeaders?: Record<string, string>
        },
    ): Promise<TaskTypeResult<T, Platform.X>> {
        const result = super._match_valid_url(url, XListSpider)?.groups
        if (!result) {
            throw new Error(`Invalid URL: ${url}`)
        }
        const { id } = result
        if (!id) {
            throw new Error(`Invalid URL: ${url}, id not found`)
        }

        const { task_type, cookieString, requestHeaders, sub_task_type, hydrate_users, hydrate_limit } = config
        const graphqlClient = new XApiClient(requestHeaders, page, this.log)
        let cookie_string = cookieString
        if (!cookie_string && page) {
            cookie_string = (await page.browserContext().cookies()).map((c) => `${c.name}=${c.value}`).join('; ')
        }
        if (!cookie_string) {
            throw new Error('Cookie string is required for X List Spider')
        }
        const normalizedEngine = config.crawl_engine === 'api-graphql' ? 'browser' : config.crawl_engine
        const fetchTweets =
            !sub_task_type || sub_task_type.length === 0 || sub_task_type.includes(XTweetsTaskType.tweets)
        const fetchReplies =
            !sub_task_type || sub_task_type.length === 0 || sub_task_type.includes(XTweetsTaskType.replies)
        if (task_type === 'article') {
            this.log?.info(`Trying to grab tweets for ${id}.`)
            let res = [] as Array<GenericArticle<Platform.X>>
            if (normalizedEngine === 'api-statuses') {
                this.log?.warn('Replies are not supported in api-statuses mode for now.')
                this.log?.debug('Using api-statuses engine')
                res = await this.grabTweets(id, cookie_string, requestHeaders)
            } else if (normalizedEngine === 'api-member') {
                this.log?.warn('Replies are not supported in api-member mode for now.')
                this.log?.debug('Using api-member engine')
                res = await this.grabTweetsPoor(id, cookie_string, requestHeaders)
            } else if (normalizedEngine === 'api-unified') {
                this.log?.debug('Using api-unified engine')
                res = await this.grabTweetsUnified(id, cookie_string, graphqlClient, {
                    fetchTweets,
                    fetchReplies,
                    hydrateUsers: hydrate_users,
                    hydrateLimit: hydrate_limit,
                })
            } else {
                if (config.crawl_engine === 'api-graphql') {
                    this.log?.warn('api-graphql is treated as a legacy alias for browser-assisted list graphql mode')
                }
                if (fetchReplies) {
                    this.log?.warn('Replies are not supported in browser-assisted list graphql mode for now.')
                }
                this.log?.debug('Using browser-assisted graphql list engine')
                res = fetchTweets ? await graphqlClient.grabTweetsFromList(id, cookie_string) : []
            }
            return res as TaskTypeResult<T, Platform.X>
        }

        if (task_type === 'follows') {
            this.log?.info(`Trying to grab follows for ${id}.`)
            let res = [] as Array<GenericFollows>
            if (normalizedEngine !== 'api-statuses' && normalizedEngine !== 'api-member') {
                res = await graphqlClient.grabFollowsFromList(id, cookie_string)
            } else {
                res = await this.grabFollows(id, cookie_string, requestHeaders)
            }
            return res as TaskTypeResult<T, Platform.X>
        }

        throw new Error('Invalid task type')
    }

    private async grabTweetsUnified(
        list_id: string,
        cookie: string,
        client: XApiClient,
        options: {
            fetchTweets: boolean
            fetchReplies: boolean
            hydrateUsers?: Array<string>
            hydrateLimit?: number
        },
    ): Promise<Array<GenericArticle<Platform.X>>> {
        const discoveryTweets = await client.grabTweetsFromList(list_id, cookie)
        const configuredUsers = this.sanitizeUserIds(options.hydrateUsers)
        const sampledViewportUsers = client.getSampledListUsers(list_id)
        const activeUserIds = this.sanitizeUserIds([
            ...(discoveryTweets.map((tweet) => tweet?.u_id?.trim()).filter(Boolean) as Array<string>),
            ...sampledViewportUsers,
        ])
        const listMemberUserIds = await client
            .grabFollowsFromList(list_id, cookie)
            .then((follows) => this.sanitizeUserIds(follows.map((follow) => follow?.u_id)))
            .catch((error) => {
                this.log?.warn(`Unified list crawl failed to expand list members for ${list_id}: ${error}`)
                return [] as Array<string>
            })
        const selectedUserIds = this.selectHydrationUsers({
            listId: list_id,
            configuredUsers,
            activeUserIds,
            listMemberUserIds,
            hydrateLimit: options.hydrateLimit,
        })

        this.log?.info(
            `Unified list crawl prepared ${selectedUserIds.length} accounts for ${list_id} (configured=${configuredUsers.length}, active=${activeUserIds.length}, sampled=${sampledViewportUsers.length}, members=${listMemberUserIds.length}).`,
        )
        if (configuredUsers.length + activeUserIds.length + listMemberUserIds.length > selectedUserIds.length) {
            this.log?.warn(
                `Unified list crawl truncated hydration candidates to ${selectedUserIds.length} for ${list_id} to limit request pressure.`,
            )
        }

        if (selectedUserIds[0]) {
            await client.prepareUserOperations(selectedUserIds[0], {
                needTweets: options.fetchTweets,
                needReplies: options.fetchReplies,
            })
        }

        const hydratedArticles = await this.hydrateUsersFromListActivity(selectedUserIds, client, cookie, options)
        return this.mergeArticles(options.fetchTweets ? discoveryTweets : [], hydratedArticles)
    }

    private async hydrateUsersFromListActivity(
        userIds: Array<string>,
        client: XApiClient,
        cookie: string,
        options: {
            fetchTweets: boolean
            fetchReplies: boolean
        },
    ) {
        const articles = [] as Array<GenericArticle<Platform.X>>

        for (let index = 0; index < userIds.length; index += X_UNIFIED_LIST_CONCURRENCY) {
            const chunk = userIds.slice(index, index + X_UNIFIED_LIST_CONCURRENCY)
            const chunkResults = await Promise.allSettled(
                chunk.map(async (userId) => {
                    const userArticles = [] as Array<GenericArticle<Platform.X>>
                    if (options.fetchTweets) {
                        userArticles.push(...(await client.grabTweets(userId, cookie)))
                    }
                    if (options.fetchReplies) {
                        userArticles.push(...(await client.grabReplies(userId, cookie)))
                    }
                    return userArticles
                }),
            )

            chunkResults.forEach((result, chunkIndex) => {
                const userId = chunk[chunkIndex]
                if (result.status === 'fulfilled') {
                    articles.push(...result.value)
                    return
                }
                this.log?.warn(`Unified list hydration failed for @${userId}: ${result.reason}`)
            })
        }

        return articles
    }

    private mergeArticles(...articleGroups: Array<Array<GenericArticle<Platform.X>>>) {
        const merged = new Map<string, GenericArticle<Platform.X>>()

        for (const article of articleGroups.flat()) {
            if (!article?.a_id) {
                continue
            }
            const existing = merged.get(article.a_id)
            if (!existing || this.scoreArticle(article) >= this.scoreArticle(existing)) {
                merged.set(article.a_id, article)
            }
        }

        return Array.from(merged.values()).sort((left, right) => (right.created_at || 0) - (left.created_at || 0))
    }

    private sanitizeUserIds(userIds?: Array<string | null | undefined>) {
        return Array.from(
            new Set(
                (userIds || [])
                    .map((userId) => String(userId || '').trim())
                    .filter(Boolean)
                    .map((userId) => userId.replace(/^@+/, '')),
            ),
        )
    }

    private selectHydrationUsers(options: {
        listId: string
        configuredUsers: Array<string>
        activeUserIds: Array<string>
        listMemberUserIds: Array<string>
        hydrateLimit?: number
    }) {
        const effectiveLimit = Math.max(
            options.configuredUsers.length,
            options.hydrateLimit || X_UNIFIED_LIST_MAX_HYDRATED_USERS,
        )
        const priorityUsers = this.sanitizeUserIds([...options.configuredUsers, ...options.activeUserIds])
        if (priorityUsers.length >= effectiveLimit) {
            return priorityUsers.slice(0, effectiveLimit)
        }

        const prioritySet = new Set(priorityUsers)
        const memberPool = this.sanitizeUserIds(options.listMemberUserIds).filter((userId) => !prioritySet.has(userId))
        const rotatedMembers = this.rotateMemberPool(options.listId, memberPool, effectiveLimit - priorityUsers.length)

        return [...priorityUsers, ...rotatedMembers].slice(0, effectiveLimit)
    }

    private rotateMemberPool(listId: string, userIds: Array<string>, take: number) {
        if (take <= 0 || userIds.length === 0) {
            return [] as Array<string>
        }

        const offset = X_UNIFIED_LIST_MEMBER_CURSORS.get(listId) || 0
        const normalizedOffset = offset % userIds.length
        const rotated = userIds.slice(normalizedOffset).concat(userIds.slice(0, normalizedOffset))
        const selected = rotated.slice(0, take)
        const advance = selected.length > 0 ? selected.length : 1
        X_UNIFIED_LIST_MEMBER_CURSORS.set(listId, (normalizedOffset + advance) % userIds.length)
        return selected
    }

    private scoreArticle(article: GenericArticle<Platform.X>) {
        let score = 0
        if (article.content?.trim()) score += 2
        if (article.media?.length) score += 1
        if (article.extra?.content) score += 1
        if (article.ref && typeof article.ref === 'object') score += 2
        if (article.type === ArticleTypeEnum.CONVERSATION) score += 1
        return score
    }

    getCsrfToken(cookie: string) {
        const match = cookie.match(/(?:^|;\s*)ct0=([0-9a-f]+)\s*(?:;|$)/)
        if (match) {
            return match[1]
        }
        return null
    }

    /**
     * @deprecated This api endpoint was 404 not found at 2025-07-19 00:00 UTC.
     */
    async grabTweets(
        id: string,
        cookie_string: string,
        requestHeaders?: Record<string, string>,
    ): Promise<Array<GenericArticle<Platform.X>>> {
        const url = `${this.API_PREFIX}/1.1/lists/statuses.json`
        const params = new URLSearchParams({
            count: '20',
            include_my_retweet: '1',
            include_rts: '1',
            list_id: id,
            cards_platform: 'Web-13',
            include_entities: '1',
            include_user_entities: '1',
            include_cards: '1',
            send_error_codes: '1',
            tweet_mode: 'extended',
            include_ext_alt_text: 'true',
            include_reply_count: 'true',
            ext: 'mediaStats%2ChighlightedLabel%2CvoiceInfo%2CsuperFollowMetadata',
            include_ext_has_nft_avatar: 'true',
            include_ext_is_blue_verified: 'true',
            include_ext_verified_type: 'true',
            include_ext_sensitive_media_warning: 'true',
            include_ext_media_color: 'true',
        })
        // TODO: keep http header case sensitive
        const res = await fetch(`${url}?${params.toString()}`, {
            headers: {
                ...normalizeRequestHeaders(requestHeaders),
                authorization: this.PUBLIC_TOKEN,
                cookie: cookie_string,
                'x-csrf-token': this.getCsrfToken(cookie_string) || '',
            },
        })

        if (!res.ok) {
            throw new Error(`Failed to fetch tweets: ${res.statusText}`)
        }

        const json = await res.json()
        if (!json) {
            throw new Error('Failed to fetch tweets with empty json')
        }

        return json.map(XApiJsonParser.oldTweetParser).filter(Boolean) as Array<GenericArticle<Platform.X>>
    }

    async grabTweetsPoor(
        id: string,
        cookie_string: string,
        requestHeaders?: Record<string, string>,
    ): Promise<Array<GenericArticle<Platform.X>>> {
        const url = `${this.API_PREFIX}/1.1/lists/members.json`
        const params = new URLSearchParams({
            list_id: id,
            cards_platform: 'Web-13',
            include_entities: '1',
            include_user_entities: '1',
            include_cards: '1',
            tweet_mode: 'extended',
            include_ext_alt_text: 'true',
            include_ext_media_color: 'true',
        })
        const res = await fetch(`${url}?${params.toString()}`, {
            headers: {
                authorization: this.PUBLIC_TOKEN,
                'user-agent': UserAgent.CHROME,
                ...normalizeRequestHeaders(requestHeaders),
                cookie: cookie_string,
            },
        })

        if (!res.ok) {
            throw new Error(`Failed to fetch follows: ${res.statusText}`)
        }
        const json = await res.json()
        if (!json) {
            throw new Error('Failed to fetch follows with empty json')
        }

        return json?.users?.map(XApiJsonParser.oldTweetMemeberParser).filter(Boolean) as Array<
            GenericArticle<Platform.X>
        >
    }

    async grabFollows(
        id: string,
        cookie: string,
        requestHeaders?: Record<string, string>,
    ): Promise<Array<GenericFollows>> {
        const url = `${this.API_PREFIX}/1.1/lists/members.json`
        const params = new URLSearchParams({
            list_id: id,
            count: '99',
        })
        const res = await fetch(`${url}?${params.toString()}`, {
            headers: {
                authorization: this.PUBLIC_TOKEN,
                'user-agent': UserAgent.CHROME,
                ...normalizeRequestHeaders(requestHeaders),
                cookie: cookie,
                'x-csrf-token': this.getCsrfToken(cookie) || '',
            },
        })

        if (!res.ok) {
            throw new Error(`Failed to fetch follows: ${res.statusText}`)
        }
        const json = await res.json()
        if (!json) {
            throw new Error('Failed to fetch follows with empty json')
        }

        return json?.users?.map(XApiJsonParser.oldFollowsParser).filter(Boolean) as Array<GenericFollows>
    }
}

/**
 * This is dangerous, because it will be banned by X if you use it too much
 */
class XApiClient {
    guest_token = '1918915913551839395'
    PUBLIC_TOKEN =
        'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'
    /**
     * 'https://x.com'
     *
     * Notice there is no trailing slash
     */
    BASE_URL = 'https://x.com'
    ASSETS_BASE_URL = 'https://abs.twimg.com/responsive-web/client-web'
    API_PREFIX = '/i/api/graphql'
    BASE_HEADER: Record<string, string>

    api_with_queryid: Partial<Record<XApis, string>>
    name_to_rest_id: Record<string, string>
    operationProfiles: Partial<Record<XApis, XOperationProfile>>
    listViewportUsers: Map<string, Array<string>>
    page?: Page
    log?: Logger

    constructor(requestHeaders?: Record<string, string>, page?: Page, log?: Logger) {
        this.api_with_queryid = {}
        this.name_to_rest_id = {}
        this.operationProfiles = {}
        this.listViewportUsers = new Map()
        this.page = page
        this.log = log?.child({ subservice: 'XApiClient' })
        this.BASE_HEADER = {
            'user-agent': UserAgent.CHROME,
            referer: 'https://x.com/',
            origin: 'https://x.com',
            ...normalizeRequestHeaders(requestHeaders),
            authorization: this.PUBLIC_TOKEN,
        }
    }

    async prepareUserOperations(
        screenName: string,
        options: {
            needTweets: boolean
            needReplies: boolean
        },
    ) {
        await this.captureOperationsFromPage(`${this.BASE_URL}/${screenName}`, [
            XApis.UserByScreenName,
            ...(options.needTweets ? [XApis.UserTweets] : []),
        ])
        if (options.needReplies) {
            await this.captureOperationsFromPage(`${this.BASE_URL}/${screenName}/with_replies`, [
                XApis.UserTweetsAndReplies,
            ])
        }
    }

    async prepareListOperations(listId: string) {
        await this.captureOperationsFromPage(`${this.BASE_URL}/i/lists/${listId}`, [XApis.ListLatestTweetsTimeline])
        await this.captureListViewportUsers(listId)
    }

    getSampledListUsers(listId: string) {
        return this.listViewportUsers.get(listId) || []
    }

    // 获取graphql query id, 备份用
    async getGraphqlQueryId(html?: string) {
        let resolvedHtml = html
        if (!resolvedHtml) {
            resolvedHtml = await this.fetchBaseHtml()
        }
        // extract "": "md5/hash"
        {
            // List
            const lists_graphql_js_pattern = /"([^"]*AudioSpacebarScr)"\s*:\s*"(\w+)"/
            const match = resolvedHtml.match(lists_graphql_js_pattern)
            if (match) {
                const js_url = `${this.ASSETS_BASE_URL}/${match[1]}.${match[2]}a.js`
                const js_code = await (await fetch(js_url, { headers: this.BASE_HEADER })).text()
                const queryId = this.getQueryId(js_code, XApis.ListLatestTweetsTimeline)
                if (queryId) {
                    this.api_with_queryid[XApis.ListLatestTweetsTimeline] = queryId
                }
            }
        }
    }

    private async captureOperationsFromPage(targetUrl: string, expectedOperations: Array<XApis>) {
        const missingOperations = expectedOperations.filter((operation) => !this.operationProfiles[operation])
        if (!this.page || missingOperations.length === 0) {
            return
        }

        const requestedOperations = new Set(missingOperations)
        const onRequest = (request: { url: () => string; headers: () => Record<string, string> }) => {
            const parsed = this.parseCapturedOperation(request.url())
            if (!parsed || !requestedOperations.has(parsed.operationName)) {
                return
            }
            this.storeOperationProfile(request.url(), request.headers())
        }

        this.page.on('request', onRequest)
        try {
            await this.navigateForCapture(targetUrl)

            const deadline = Date.now() + 8000
            while (Date.now() < deadline) {
                if (missingOperations.every((operation) => Boolean(this.operationProfiles[operation]))) {
                    return
                }
                await sleep(150)
            }

            const unresolved = missingOperations.filter((operation) => !this.operationProfiles[operation])
            if (unresolved.length > 0) {
                this.log?.debug(`Browser capture missed operations for ${targetUrl}: ${unresolved.join(', ')}`)
            }
        } finally {
            this.page.off('request', onRequest)
        }
    }

    private async navigateForCapture(targetUrl: string) {
        if (!this.page) {
            return
        }

        try {
            const currentUrl = this.page.url().split('#')[0]
            if (currentUrl === targetUrl) {
                await this.page.reload({
                    waitUntil: 'domcontentloaded',
                    timeout: 15000,
                })
            } else {
                await this.page.goto(targetUrl, {
                    waitUntil: 'domcontentloaded',
                    timeout: 15000,
                })
            }
            await sleep(1200)
        } catch (error) {
            this.log?.warn(`Browser capture navigation failed for ${targetUrl}: ${error}`)
        }
    }

    private async captureListViewportUsers(listId: string) {
        if (!this.page) {
            return
        }

        const sampledUsers = new Set<string>()
        const collectVisibleUsers = async () => {
            try {
                const usernames = await this.page!.evaluate(() => {
                    const reserved = new Set([
                        '',
                        'compose',
                        'explore',
                        'home',
                        'i',
                        'jobs',
                        'login',
                        'messages',
                        'notifications',
                        'privacy',
                        'search',
                        'settings',
                        'signup',
                        'tos',
                    ])
                    const links = Array.from(document.querySelectorAll<HTMLAnchorElement>('article a[href*="/status/"]'))
                    const users = new Set<string>()
                    for (const link of links) {
                        const href = link.getAttribute('href') || ''
                        const match = href.match(/^\/([^/?#]+)\/status\//)
                        if (!match) {
                            continue
                        }
                        const user = match[1]?.trim().replace(/^@+/, '')
                        if (!user || reserved.has(user.toLowerCase())) {
                            continue
                        }
                        users.add(user)
                    }
                    return Array.from(users)
                })
                usernames.forEach((username) => sampledUsers.add(username))
            } catch (error) {
                this.log?.debug(`List viewport sampling failed for ${listId}: ${error}`)
            }
        }

        await collectVisibleUsers()

        const viewport = this.page.viewport()
        for (let index = 0; index < 3; index += 1) {
            if (viewport) {
                const targetX = Math.floor(viewport.width * (0.25 + Math.random() * 0.5))
                const targetY = Math.floor(viewport.height * (0.2 + Math.random() * 0.55))
                await this.page.mouse.move(targetX, targetY, { steps: 10 }).catch(() => null)
            }

            const scrollAmount = 500 + Math.floor(Math.random() * 900)
            await this.page.mouse.wheel(0, scrollAmount).catch(() => null)
            await this.page
                .evaluate((amount) => {
                    const primaryColumn = document.querySelector('[data-testid="primaryColumn"]') as HTMLElement | null
                    const scroller = (primaryColumn?.querySelector('section')?.parentElement as HTMLElement | null) || null
                    if (scroller && typeof scroller.scrollBy === 'function') {
                        scroller.scrollBy({ top: amount, behavior: 'instant' })
                        return
                    }
                    window.scrollBy({ top: amount, behavior: 'instant' })
                }, scrollAmount)
                .catch(() => null)
            await sleep(450 + Math.floor(Math.random() * 900))
            await collectVisibleUsers()
        }

        if (sampledUsers.size > 0) {
            const users = Array.from(sampledUsers)
            this.listViewportUsers.set(listId, users)
            this.log?.debug(`List viewport sampled ${users.length} accounts for ${listId}: ${users.join(', ')}`)
        }
    }

    private storeOperationProfile(url: string, headers: Record<string, string>) {
        const parsed = this.parseCapturedOperation(url)
        if (!parsed) {
            return
        }

        const filteredHeaders = this.filterCapturedHeaders(headers)
        this.operationProfiles[parsed.operationName] = {
            queryId: parsed.queryId,
            url,
            headers: filteredHeaders,
            capturedAt: Date.now(),
        }
        this.api_with_queryid[parsed.operationName] = parsed.queryId
    }

    private parseCapturedOperation(url: string) {
        const match = url.match(/\/i\/api\/graphql\/([^/]+)\/([^/?#]+)/)
        if (!match) {
            return null
        }

        const queryId = match[1]
        const operationName = match[2] as XApis
        if (!Object.values(XApis).includes(operationName)) {
            return null
        }

        return {
            queryId,
            operationName,
        }
    }

    private filterCapturedHeaders(headers: Record<string, string>) {
        return Object.fromEntries(
            Object.entries(headers || {}).filter(([key, value]) => {
                const normalizedKey = String(key || '').toLowerCase()
                return CAPTURED_HEADER_KEYS.has(normalizedKey) && typeof value === 'string' && value.trim()
            }),
        )
    }

    private getOperationProfile(operation: XApis, fallbackOperations: Array<XApis> = []) {
        return this.operationProfiles[operation] || fallbackOperations.map((entry) => this.operationProfiles[entry]).find(Boolean)
    }

    private async ensureQueryIds(requiredApis: Array<XApis> = DEFAULT_QUERY_APIS) {
        const targets = Array.from(new Set(requiredApis))
        const missingBefore = targets.filter((api) => !this.api_with_queryid[api])
        if (missingBefore.length === 0) {
            return
        }

        const html = await this.fetchBaseHtml()

        const jsUrls = this.extractJavascriptUrls(html)
        for (const jsUrl of jsUrls) {
            const jsCode = await fetch(jsUrl, { headers: this.BASE_HEADER })
                .then((res) => res.text())
                .catch(() => '')
            if (!jsCode) {
                continue
            }

            for (const api of targets) {
                if (this.api_with_queryid[api]) {
                    continue
                }
                const queryId = this.getQueryId(jsCode, api)
                if (queryId) {
                    this.api_with_queryid[api] = queryId
                }
            }

            if (targets.every((api) => Boolean(this.api_with_queryid[api]))) {
                return
            }
        }

        if (targets.includes(XApis.ListLatestTweetsTimeline) && !this.api_with_queryid[XApis.ListLatestTweetsTimeline]) {
            await this.getGraphqlQueryId(html)
        }

        const missingAfter = targets.filter((api) => !this.api_with_queryid[api])
        if (missingAfter.length > 0) {
            throw new Error(`Missing query ids: ${missingAfter.join(', ')}`)
        }
    }

    private async fetchBaseHtml() {
        if (this.page) {
            const html = await this.page.content().catch(() => '')
            if (html) {
                return html
            }
        }

        const webpage = await fetch(this.BASE_URL, {
            headers: this.BASE_HEADER,
        })
        return await webpage.text()
    }

    private extractJavascriptUrls(html: string) {
        const urls = Array.from(html.matchAll(/(?:src|href)="([^"]+\.js)"/g))
            .map((match) => {
                try {
                    return new URL(match[1], this.BASE_URL).toString()
                } catch {
                    return null
                }
            })
            .filter((url): url is string => Boolean(url))

        return Array.from(new Set(urls)).sort((left, right) => {
            const leftMain = /\/main\./.test(left) ? 0 : 1
            const rightMain = /\/main\./.test(right) ? 0 : 1
            return leftMain - rightMain
        })
    }

    private async resolveQueryId(operation: XApis) {
        if (!this.api_with_queryid[operation]) {
            await this.ensureQueryIds([operation])
        }

        const queryId = this.api_with_queryid[operation]
        if (!queryId) {
            throw new Error(`Missing query id for ${operation}`)
        }
        return queryId
    }

    private buildOperationHeaders(
        operation: XApis,
        cookie: string,
        options?: {
            extraHeaders?: Record<string, string>
            fallbackOperations?: Array<XApis>
            includeGuestToken?: boolean
            referer?: string
        },
    ) {
        const profile = this.getOperationProfile(operation, options?.fallbackOperations)
        const csrfToken = this.getCsrfToken(cookie)
        const headers = {
            ...this.BASE_HEADER,
            ...(profile?.headers || {}),
            ...(options?.referer ? { referer: options.referer } : {}),
            cookie,
            'x-csrf-token': csrfToken || profile?.headers['x-csrf-token'] || '',
            'x-twitter-active-user': profile?.headers['x-twitter-active-user'] || 'yes',
            'x-twitter-auth-type': profile?.headers['x-twitter-auth-type'] || 'OAuth2Session',
            ...(options?.includeGuestToken ? { 'x-guest-token': this.guest_token } : {}),
            ...(options?.extraHeaders || {}),
        }
        if (!headers.authorization) {
            headers.authorization = this.PUBLIC_TOKEN
        }
        if (!headers.origin) {
            headers.origin = 'https://x.com'
        }
        if (!headers.referer) {
            headers.referer = options?.referer || `${this.BASE_URL}/`
        }
        return normalizeRequestHeaders(headers)
    }

    /**
     * UserByScreenName
     */
    async getRawUserInfo(id: string, cookie: string) {
        await this.prepareUserOperations(id, {
            needTweets: false,
            needReplies: false,
        })
        const query_id = await this.resolveQueryId(XApis.UserByScreenName)
        const query_path = `${this.API_PREFIX}/${query_id}/${XApis.UserByScreenName}`
        const variables = {
            screen_name: id,
            withGrokTranslatedBio: false,
        }
        const features = {
            hidden_profile_subscriptions_enabled: true,
            profile_label_improvements_pcf_label_in_post_enabled: true,
            responsive_web_profile_redirect_enabled: false,
            rweb_tipjar_consumption_enabled: true,
            verified_phone_label_enabled: false,
            subscriptions_verification_info_is_identity_verified_enabled: true,
            subscriptions_verification_info_verified_since_enabled: true,
            highlights_tweets_tab_ui_enabled: true,
            responsive_web_twitter_article_notes_tab_enabled: true,
            subscriptions_feature_can_gift_premium: true,
            creator_subscriptions_tweet_preview_api_enabled: true,
            responsive_web_graphql_skip_user_profile_image_extensions_enabled: false,
            responsive_web_graphql_timeline_navigation_enabled: true,
        }
        const fieldToggles = { withPayments: false, withAuxiliaryUserLabels: true }

        const query = this.generateParams(features, variables, fieldToggles)
        const url = `${this.BASE_URL}${query_path}?${query.toString()}`
        const res = await fetch(url, {
            headers: this.buildOperationHeaders(XApis.UserByScreenName, cookie, {
                includeGuestToken: true,
                referer: `${this.BASE_URL}/${id}`,
            }),
        })
        if (!res.ok) {
            throw new Error(`Failed to fetch user info (${id}): ${res.statusText}`)
        }
        const json = await res.json()
        return json
    }

    async getRestId(id: string, cookie: string) {
        if (this.name_to_rest_id[id]) {
            return this.name_to_rest_id[id]
        }
        const user_info = await this.getRawUserInfo(id, cookie)
        if (!user_info) {
            throw new Error(`Failed to fetch user info for ${id}`)
        }
        const rest_id = user_info?.data?.user?.result?.rest_id
        if (!rest_id) {
            throw new Error(`Failed to fetch rest id for ${id}`)
        }
        this.name_to_rest_id[id] = String(rest_id)
        return String(rest_id)
    }

    getQueryId(js: string, targetOperationName: string) {
        const escapedOperationName = targetOperationName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        const regex = new RegExp(`queryId:"([^"]+)",operationName:"${escapedOperationName}"`, 's')
        const match = js.match(regex)
        return match ? match[1] : null
    }

    generateParams(
        features: Record<string, any>,
        variables: Record<string, any>,
        fieldToggles?: Record<string, any>,
    ): URLSearchParams {
        let params = new URLSearchParams()
        params.append('variables', JSON.stringify(variables))
        params.append('features', JSON.stringify(features))
        if (fieldToggles) params.append('fieldToggles', JSON.stringify(fieldToggles))

        return params
    }

    getCsrfToken(cookie: string) {
        const match = cookie.match(/(?:^|;\s*)ct0=([0-9a-f]+)\s*(?:;|$)/)
        if (match) {
            return match[1]
        }
        return null
    }

    async grabTweets(id: string, cookie: string) {
        await this.prepareUserOperations(id, {
            needTweets: true,
            needReplies: false,
        })
        const rest_id = await this.getRestId(id, cookie)
        const query_id = await this.resolveQueryId(XApis.UserTweets)
        const query_path = `${this.API_PREFIX}/${query_id}/${XApis.UserTweets}`
        const uuid = uuidv4({
            rng: cookie ? () => Buffer.from(cookie.padEnd(16, '0')) : undefined,
        })
        const variables = {
            userId: rest_id,
            // TODO: configurable
            count: 5,
            includePromotedContent: true,
            withQuickPromoteEligibilityTweetFields: true,
            withVoice: true,
        }
        const features = {
            rweb_video_screen_enabled: false,
            profile_label_improvements_pcf_label_in_post_enabled: true,
            rweb_tipjar_consumption_enabled: true,
            verified_phone_label_enabled: false,
            creator_subscriptions_tweet_preview_api_enabled: true,
            responsive_web_graphql_timeline_navigation_enabled: true,
            responsive_web_graphql_skip_user_profile_image_extensions_enabled: false,
            premium_content_api_read_enabled: false,
            communities_web_enable_tweet_community_results_fetch: true,
            c9s_tweet_anatomy_moderator_badge_enabled: true,
            responsive_web_grok_analyze_button_fetch_trends_enabled: false,
            responsive_web_grok_analyze_post_followups_enabled: true,
            responsive_web_jetfuel_frame: false,
            responsive_web_grok_share_attachment_enabled: true,
            articles_preview_enabled: true,
            responsive_web_edit_tweet_api_enabled: true,
            graphql_is_translatable_rweb_tweet_is_translatable_enabled: true,
            view_counts_everywhere_api_enabled: true,
            longform_notetweets_consumption_enabled: true,
            responsive_web_twitter_article_tweet_consumption_enabled: true,
            tweet_awards_web_tipping_enabled: false,
            responsive_web_grok_show_grok_translated_post: false,
            responsive_web_grok_analysis_button_from_backend: false,
            creator_subscriptions_quote_tweet_preview_enabled: false,
            freedom_of_speech_not_reach_fetch_enabled: true,
            standardized_nudges_misinfo: true,
            tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled: true,
            longform_notetweets_rich_text_read_enabled: true,
            longform_notetweets_inline_media_enabled: true,
            responsive_web_grok_image_annotation_enabled: true,
            responsive_web_enhance_cards_enabled: false,
        }
        const fieldToggles = { withArticlePlainText: false }
        const query = this.generateParams(features, variables, fieldToggles)

        const url = `${this.BASE_URL}${query_path}?${query.toString()}`
        const res = await fetch(url, {
            headers: this.buildOperationHeaders(XApis.UserTweets, cookie, {
                extraHeaders: { 'x-client-uuid': uuid },
                referer: `${this.BASE_URL}/${id}`,
            }),
        })
        if (!res.ok) {
            throw new Error(`Failed to fetch tweets: ${res.statusText}`)
        }
        const json = await res.json()
        if (json.errors) {
            throw new Error(`Failed to fetch tweets: ${json.errors[0].message}`)
        }
        return XApiJsonParser.tweetsArticleParser(json)
    }
    async grabReplies(id: string, cookie: string) {
        await this.prepareUserOperations(id, {
            needTweets: false,
            needReplies: true,
        })
        const rest_id = await this.getRestId(id, cookie)
        const query_id = await this.resolveQueryId(XApis.UserTweetsAndReplies)
        const query_path = `${this.API_PREFIX}/${query_id}/${XApis.UserTweetsAndReplies}`
        const uuid = uuidv4({
            rng: cookie ? () => Buffer.from(cookie.padEnd(16, '0')) : undefined,
        })
        const variables = {
            userId: rest_id,
            count: 8,
            includePromotedContent: true,
            withCommunity: true,
            withVoice: true,
        }
        const features = {
            rweb_video_screen_enabled: false,
            profile_label_improvements_pcf_label_in_post_enabled: true,
            rweb_tipjar_consumption_enabled: true,
            verified_phone_label_enabled: false,
            creator_subscriptions_tweet_preview_api_enabled: true,
            responsive_web_graphql_timeline_navigation_enabled: true,
            responsive_web_graphql_skip_user_profile_image_extensions_enabled: false,
            premium_content_api_read_enabled: false,
            communities_web_enable_tweet_community_results_fetch: true,
            c9s_tweet_anatomy_moderator_badge_enabled: true,
            responsive_web_grok_analyze_button_fetch_trends_enabled: false,
            responsive_web_grok_analyze_post_followups_enabled: true,
            responsive_web_jetfuel_frame: false,
            responsive_web_grok_share_attachment_enabled: true,
            articles_preview_enabled: true,
            responsive_web_edit_tweet_api_enabled: true,
            graphql_is_translatable_rweb_tweet_is_translatable_enabled: true,
            view_counts_everywhere_api_enabled: true,
            longform_notetweets_consumption_enabled: true,
            responsive_web_twitter_article_tweet_consumption_enabled: true,
            tweet_awards_web_tipping_enabled: false,
            responsive_web_grok_show_grok_translated_post: false,
            responsive_web_grok_analysis_button_from_backend: false,
            creator_subscriptions_quote_tweet_preview_enabled: false,
            freedom_of_speech_not_reach_fetch_enabled: true,
            standardized_nudges_misinfo: true,
            tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled: true,
            longform_notetweets_rich_text_read_enabled: true,
            longform_notetweets_inline_media_enabled: true,
            responsive_web_grok_image_annotation_enabled: true,
            responsive_web_enhance_cards_enabled: false,
        }
        const fieldToggles = { withArticlePlainText: false }
        const query = this.generateParams(features, variables, fieldToggles)
        const url = `${this.BASE_URL}${query_path}?${query.toString()}`
        const res = await fetch(url, {
            headers: this.buildOperationHeaders(XApis.UserTweetsAndReplies, cookie, {
                extraHeaders: { 'x-client-uuid': uuid },
                fallbackOperations: [XApis.UserTweets],
                referer: `${this.BASE_URL}/${id}/with_replies`,
            }),
        })
        if (!res.ok) {
            throw new Error(`Failed to fetch replies: ${res.statusText}`)
        }
        const json = await res.json()
        if (json.errors) {
            throw new Error(`Failed to fetch replies: ${json.errors[0].message}`)
        }
        return XApiJsonParser.tweetsRepliesParser(json)
    }

    async grabFollowsNumber(id: string, cookie: string) {
        const user_info = await this.getRawUserInfo(id, cookie)
        if (!user_info) {
            throw new Error(`Failed to fetch user info for ${id}`)
        }
        return XApiJsonParser.tweetsFollowsParser(user_info)
    }

    async grabTweetsFromList(list_id: string, cookie: string) {
        await this.prepareListOperations(list_id)
        await this.ensureQueryIds([XApis.ListLatestTweetsTimeline]).catch(() => null)
        const query_id = this.api_with_queryid[XApis.ListLatestTweetsTimeline] ?? 'NRigOCel0QKiWs_GuBgOzw'
        const query_path = `${this.API_PREFIX}/${query_id}/ListLatestTweetsTimeline`
        const variables = { listId: list_id, count: 20 }
        const features = {
            rweb_video_screen_enabled: false,
            profile_label_improvements_pcf_label_in_post_enabled: true,
            responsive_web_profile_redirect_enabled: false,
            rweb_tipjar_consumption_enabled: true,
            verified_phone_label_enabled: false,
            creator_subscriptions_tweet_preview_api_enabled: true,
            responsive_web_graphql_timeline_navigation_enabled: true,
            responsive_web_graphql_skip_user_profile_image_extensions_enabled: false,
            premium_content_api_read_enabled: false,
            communities_web_enable_tweet_community_results_fetch: true,
            c9s_tweet_anatomy_moderator_badge_enabled: true,
            responsive_web_grok_analyze_button_fetch_trends_enabled: false,
            responsive_web_grok_analyze_post_followups_enabled: true,
            responsive_web_jetfuel_frame: true,
            responsive_web_grok_share_attachment_enabled: true,
            responsive_web_grok_annotations_enabled: false,
            articles_preview_enabled: true,
            responsive_web_edit_tweet_api_enabled: true,
            graphql_is_translatable_rweb_tweet_is_translatable_enabled: true,
            view_counts_everywhere_api_enabled: true,
            longform_notetweets_consumption_enabled: true,
            responsive_web_twitter_article_tweet_consumption_enabled: true,
            tweet_awards_web_tipping_enabled: false,
            responsive_web_grok_show_grok_translated_post: false,
            responsive_web_grok_analysis_button_from_backend: true,
            post_ctas_fetch_enabled: false,
            creator_subscriptions_quote_tweet_preview_enabled: false,
            freedom_of_speech_not_reach_fetch_enabled: true,
            standardized_nudges_misinfo: true,
            tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled: true,
            longform_notetweets_rich_text_read_enabled: true,
            longform_notetweets_inline_media_enabled: true,
            responsive_web_grok_image_annotation_enabled: true,
            responsive_web_grok_imagine_annotation_enabled: true,
            responsive_web_grok_community_note_auto_translation_is_enabled: false,
            responsive_web_enhance_cards_enabled: false,
        }
        const query = this.generateParams(features, variables)

        const url = `${this.BASE_URL}${query_path}?${query.toString()}`
        const res = await fetch(url, {
            headers: this.buildOperationHeaders(XApis.ListLatestTweetsTimeline, cookie, {
                referer: `${this.BASE_URL}/i/lists/${list_id}`,
            }),
        })
        if (!res.ok) {
            throw new Error(`Failed to fetch tweets: ${res.statusText}`)
        }
        const json = await res.json()
        if (json.errors) {
            throw new Error(`Failed to fetch tweets: ${json.errors[0].message}`)
        }
        return XApiJsonParser.tweetsArticleParser(json)
    }

    async grabFollowsFromList(list_id: string, cookie: string) {
        await this.prepareListOperations(list_id)
        await this.ensureQueryIds([XApis.ListMembers]).catch(() => null)
        const query_id = this.api_with_queryid[XApis.ListMembers] ?? '8oGwd_SHm0nGs91qI4znfA'
        const query_path = `${this.API_PREFIX}/${query_id}/ListMembers`
        const variables = { listId: list_id, count: 99 }
        const features = {
            rweb_video_screen_enabled: false,
            payments_enabled: false,
            profile_label_improvements_pcf_label_in_post_enabled: true,
            responsive_web_profile_redirect_enabled: false,
            rweb_tipjar_consumption_enabled: true,
            verified_phone_label_enabled: false,
            creator_subscriptions_tweet_preview_api_enabled: true,
            responsive_web_graphql_timeline_navigation_enabled: true,
            responsive_web_graphql_skip_user_profile_image_extensions_enabled: false,
            premium_content_api_read_enabled: false,
            communities_web_enable_tweet_community_results_fetch: true,
            c9s_tweet_anatomy_moderator_badge_enabled: true,
            responsive_web_grok_analyze_button_fetch_trends_enabled: false,
            responsive_web_grok_analyze_post_followups_enabled: true,
            responsive_web_jetfuel_frame: true,
            responsive_web_grok_share_attachment_enabled: true,
            articles_preview_enabled: true,
            responsive_web_edit_tweet_api_enabled: true,
            graphql_is_translatable_rweb_tweet_is_translatable_enabled: true,
            view_counts_everywhere_api_enabled: true,
            longform_notetweets_consumption_enabled: true,
            responsive_web_twitter_article_tweet_consumption_enabled: true,
            tweet_awards_web_tipping_enabled: false,
            responsive_web_grok_show_grok_translated_post: false,
            responsive_web_grok_analysis_button_from_backend: true,
            creator_subscriptions_quote_tweet_preview_enabled: false,
            freedom_of_speech_not_reach_fetch_enabled: true,
            standardized_nudges_misinfo: true,
            tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled: true,
            longform_notetweets_rich_text_read_enabled: true,
            longform_notetweets_inline_media_enabled: true,
            responsive_web_grok_image_annotation_enabled: true,
            responsive_web_grok_imagine_annotation_enabled: true,
            responsive_web_grok_community_note_auto_translation_is_enabled: false,
            responsive_web_enhance_cards_enabled: false,
        }
        const query = this.generateParams(features, variables)

        const url = `${this.BASE_URL}${query_path}?${query.toString()}`
        const res = await fetch(url, {
            headers: this.buildOperationHeaders(XApis.ListMembers, cookie, {
                fallbackOperations: [XApis.ListLatestTweetsTimeline],
                referer: `${this.BASE_URL}/i/lists/${list_id}`,
            }),
        })
        if (!res.ok) {
            throw new Error(`Failed to fetch tweets: ${res.statusText}`)
        }
        const json = await res.json()
        if (json.errors) {
            throw new Error(`Failed to fetch tweets: ${json.errors[0].message}`)
        }
        return XApiJsonParser.tweetsFollowsFromListParser(json)
    }
}
namespace XApiJsonParser {
    namespace Card {
        function getThumbnailUrl(
            values: Array<{
                key: string
                value: { type: string } & Record<string, any>
            }>,
        ) {
            let media = values
                .filter((v) => v.value.type === 'IMAGE')
                .map(
                    (v) =>
                        v.value.image_value as {
                            height: number
                            width: number
                            url: string
                        },
                )
            if (media.length <= 0) {
                return
            }
            media = media.sort((a, b) => b.height - a.height)
            return media[0]?.url
        }

        interface BindingValue {
            key: string
            value: {
                string_value: string
                type: string
            }
        }

        const transformPollData = (bindingValues: BindingValue[]) => {
            const resultMap = new Map<number, { name?: string; count?: string }>()

            // 使用正则表达式匹配所有choice数字编号
            const choicePattern = /^choice(\d+)_(label|count)$/

            bindingValues.forEach((item) => {
                const match = item.key.match(choicePattern)
                if (!match) return

                const [, indexStr, type] = match
                const index = parseInt(indexStr || '0', 10)

                if (!resultMap.has(index)) {
                    resultMap.set(index, {})
                }

                const current = resultMap.get(index)!
                if (type === 'label') {
                    current.name = item.value.string_value
                } else if (type === 'count') {
                    current.count = item.value.string_value
                }
            })

            // 转换为有序数组并过滤无效条目
            return Array.from(resultMap.entries())
                .sort(([a], [b]) => a - b) // 按choice数字顺序排序
                .map(([index, values]) => ({
                    name: values.name || `Unknown Choice ${index}`,
                    count: values.count || '0',
                }))
                .filter((item) => item.name && item.count) // 过滤无效条目
        }

        function extractValueByKey(
            values: Array<{
                key: string
                value: { type: string } & Record<string, any>
            }>,
            key: string,
        ) {
            if (!values) {
                return
            }
            const value = values.find((v) => v.key === key)
            if (value) {
                return value.value
            }
            return
        }

        export function cardParser(card: any): ArticleExtractType<Platform.X> | null {
            if (!card) {
                return null
            }
            let _card = {
                type: CardTypeEnum.NONE,
                card_url: card.url,
            } as Card<CardTypeEnum>
            if (card.name.includes('image')) {
                _card.type = CardTypeEnum.IMAGE
            }
            if (card.name.includes('player')) {
                _card.type = CardTypeEnum.PLAYER
            }
            if (card.name.includes('choice')) {
                _card.type = CardTypeEnum.CHOICE
            }
            if (card.name.includes('audiospace')) {
                _card.type = CardTypeEnum.SPACE
            }
            if (_card.type === CardTypeEnum.NONE) {
                return null
            }

            let binding_values = card.binding_values
            if (!Array.isArray(binding_values)) {
                binding_values = Object.entries(binding_values).map(([key, value]) => ({
                    key,
                    value,
                }))
            }

            let media: GenericMediaInfo[] = []
            let content
            if ([CardTypeEnum.IMAGE, CardTypeEnum.PLAYER].includes(_card.type)) {
                _card = {
                    ..._card,
                    title: extractValueByKey(binding_values, 'title')?.string_value,
                    description: extractValueByKey(binding_values, 'description')?.string_value,
                    domain: extractValueByKey(binding_values, 'domain')?.string_value,
                    thumbnail_url: getThumbnailUrl(binding_values),
                    player_url: extractValueByKey(binding_values, 'player_url')?.string_value,
                } as Card<CardTypeEnum.IMAGE | CardTypeEnum.PLAYER>
                const type_guard_card = _card as Card<CardTypeEnum.IMAGE | CardTypeEnum.PLAYER>
                content = [
                    type_guard_card.title ? type_guard_card.title : '',
                    type_guard_card.description ? type_guard_card.description : '',
                    type_guard_card.domain ? type_guard_card.domain : '',
                    'player_url' in type_guard_card && type_guard_card.player_url ? type_guard_card.player_url : '',
                ]
                    .filter(Boolean)
                    .join('\n')
            }
            media.push({
                type: 'photo',
                url: (_card as Card<CardTypeEnum.IMAGE | CardTypeEnum.PLAYER>).thumbnail_url || '',
            })

            if (_card.type === CardTypeEnum.CHOICE) {
                const choices = binding_values.filter((v: any) => v.key.startsWith('choice'))
                _card = {
                    ..._card,
                    choices: transformPollData(choices),
                } as Card<CardTypeEnum.CHOICE>
                content = `choices:\n${(_card as Card<CardTypeEnum.CHOICE>).choices
                    .map((choice) => `${choice.name}: ${choice.count}`)
                    .join('\n')}`
            }

            if (_card.type === CardTypeEnum.SPACE) {
                content = `space id: ${extractValueByKey(binding_values, 'id')?.string_value}`
            }
            return {
                data: _card,
                content,
                media,
                extra_type: 'card',
            } as ArticleExtractType<Platform.X>
        }
    }

    function sanitizeTweetsJson(json: any) {
        let tweets = JSONPath({ path: "$..instructions[?(@.type === 'TimelineAddEntries')].entries", json })[0]
        let pin_tweet = JSONPath({ path: "$..instructions[?(@.type === 'TimelinePinEntry')].entry", json })[0]
        if (!tweets) {
            throw new Error('Tweet json format may have changed')
        }

        if (pin_tweet) {
            tweets.unshift(pin_tweet)
        }
        return tweets
    }

    // 时间转换辅助函数
    function parseTwitterDate(dateStr: string) {
        return Date.parse(dateStr.replace(/( \+0000)/, ' UTC$1'))
    }

    function mediaParser(media: any) {
        if (!media) {
            return null
        }
        return media
            .map((m: any) => {
                const { media_url_https, video_info, type, ext_alt_text } = m
                if (type === 'photo') {
                    return {
                        type,
                        url: media_url_https,
                        alt: ext_alt_text,
                    }
                }
                if (type === 'video' || type === 'animated_gif') {
                    return [
                        {
                            type: 'video',
                            url: video_info?.variants
                                ?.filter((i: { bitrate?: number }) => i.bitrate !== undefined)
                                .sort((a: { bitrate: number }, b: { bitrate: number }) => b.bitrate - a.bitrate)[0].url,
                        },
                        {
                            type: 'video_thumbnail',
                            url: media_url_https,
                        },
                    ]
                }
            })
            .flat()
            .filter(Boolean)
    }

    function tweetParser(result: any): GenericArticle<Platform.X> | null {
        // TweetWithVisibilityResults --> result.tweet
        const legacy = result.legacy || result.tweet?.legacy
        const userResult = (result.core || result.tweet?.core)?.user_results?.result
        const userLegacy = userResult?.core
        let content = legacy?.full_text
        for (const { url } of legacy?.entities?.media || []) {
            content = content.replace(url, '')
        }

        // 主推文解析
        const tweet = {
            platform: Platform.X,
            a_id: legacy?.id_str,
            u_id: userLegacy?.screen_name,
            username: userLegacy?.name,
            created_at: Math.floor(parseTwitterDate(legacy?.created_at) / 1000),
            content: legacy?.full_text,
            url: userLegacy?.screen_name ? `https://x.com/${userLegacy.screen_name}/status/${legacy?.id_str}` : '',
            type: result.quoted_status_result?.result ? ArticleTypeEnum.QUOTED : ArticleTypeEnum.TWEET,
            ref: result.quoted_status_result?.result
                ? tweetParser(result.quoted_status_result.result)
                : result.retweeted_status_result?.result
                    ? tweetParser(result.retweeted_status_result.result)
                    : null,
            media: mediaParser(legacy?.extended_entities?.media || legacy?.entities?.media),
            has_media: !!legacy?.extended_entities?.media || !!legacy?.entities?.media,
            extra: Card.cardParser(result.card?.legacy),
            u_avatar:
                userResult?.avatar?.image_url?.replace('_normal', '') ||
                userLegacy?.profile_image_url_https?.replace('_normal', ''),
        }
        // 处理转发类型
        if (legacy?.retweeted_status_result) {
            if (!legacy.retweeted_status_result.result) {
                return null
            }
            tweet.type = ArticleTypeEnum.RETWEET
            tweet.content = ''
            tweet.ref = tweetParser(legacy.retweeted_status_result.result)
            // 转发类型推文media按照ref为准
            tweet.media = null
            tweet.has_media = false
            tweet.extra = null
        }
        let urls = legacy.entities.urls || []
        for (const u of urls) {
            if (u.expanded_url && !u.expanded_url.startsWith('https://x.com/')) {
                tweet.content = tweet.content?.replace(u.url, u.expanded_url) ?? null
            } else {
                tweet.content = tweet.content?.replace(u.url, '') ?? null
            }
        }
        let media_urls = legacy.entities.media?.map((m: { url: string }) => m.url) || []
        for (const url of media_urls) {
            tweet.content = tweet.content?.replace(url, '') ?? null
        }
        return tweet as GenericArticle<Platform.X>
    }

    export function oldTweetParser(json: any): GenericArticle<Platform.X> | null {
        const legacy = json
        const userLegacy = json?.user
        let type: ArticleTypeEnum = ArticleTypeEnum.TWEET
        let ref: GenericArticleRef<Platform.X> | null = null
        if (legacy?.retweeted_status) {
            // high priority
            type = ArticleTypeEnum.RETWEET
            ref = oldTweetParser(legacy?.retweeted_status) as GenericArticleRef<Platform.X>
        } else if (legacy?.is_quote_status) {
            type = ArticleTypeEnum.QUOTED
            ref = legacy?.quoted_status
                ? (oldTweetParser(legacy?.quoted_status) as GenericArticleRef<Platform.X>)
                : legacy?.quoted_status_id_str || null
        } else if (legacy?.in_reply_to_status_id_str) {
            type = ArticleTypeEnum.CONVERSATION
            ref = legacy?.in_reply_to_status_id_str
        }
        // 主推文解析
        const tweet = {
            platform: Platform.X,
            a_id: legacy?.id_str,
            u_id: userLegacy?.screen_name,
            username: userLegacy?.name,
            created_at: Math.floor(parseTwitterDate(legacy?.created_at) / 1000),
            content: legacy?.full_text,
            url: userLegacy?.screen_name ? `https://x.com/${userLegacy.screen_name}/status/${legacy?.id_str}` : '',
            type: type,
            ref: ref,
            // extended_entities里是video，但entities里只是图片
            media: mediaParser(legacy?.extended_entities?.media || legacy?.entities?.media),
            has_media: !!legacy?.extended_entities?.media || !!legacy?.entities?.media,
            extra: Card.cardParser(legacy.card),
            u_avatar: userLegacy?.profile_image_url_https?.replace('_normal', ''),
        } as GenericArticle<Platform.X>
        // 处理转发类型
        if (tweet.type === ArticleTypeEnum.RETWEET) {
            tweet.content = ''
            // 转发类型推文media按照ref为准
            tweet.media = null
            tweet.has_media = false
            tweet.extra = null
        }

        let urls = legacy.entities.urls || []
        for (const u of urls) {
            if (u.expanded_url && !u.expanded_url.startsWith('https://x.com/')) {
                tweet.content = tweet.content?.replace(u.url, u.expanded_url) ?? null
            } else {
                tweet.content = tweet.content?.replace(u.url, '') ?? null
            }
        }
        let media_urls = legacy.entities.media?.map((m: { url: string }) => m.url) || []
        for (const url of media_urls) {
            tweet.content = tweet.content?.replace(url, '') ?? null
        }
        return tweet as GenericArticle<Platform.X>
    }

    export function oldTweetMemeberParser(json: any): GenericArticle<Platform.X> | null {
        const legacy = json?.status
        const userLegacy = json
        let type: ArticleTypeEnum = ArticleTypeEnum.TWEET
        if (legacy?.retweeted_status) {
            // high priority
            type = ArticleTypeEnum.RETWEET
        } else if (legacy?.is_quote_status) {
            type = ArticleTypeEnum.QUOTED
        } else if (legacy?.in_reply_to_status_id_str) {
            type = ArticleTypeEnum.CONVERSATION
        }
        if (type !== ArticleTypeEnum.TWEET) {
            return null
        }
        // 主推文解析
        const tweet = {
            platform: Platform.X,
            a_id: legacy?.id_str,
            u_id: userLegacy?.screen_name,
            username: userLegacy?.name,
            created_at: Math.floor(parseTwitterDate(legacy?.created_at) / 1000),
            content: legacy?.text || legacy?.full_text,
            url: userLegacy?.screen_name ? `https://x.com/${userLegacy.screen_name}/status/${legacy?.id_str}` : '',
            type: type,
            ref: null,
            // extended_entities里是video，但entities里只是图片
            media: mediaParser(legacy?.extended_entities?.media || legacy?.entities?.media),
            has_media: !!legacy?.extended_entities?.media || !!legacy?.entities?.media,
            extra: Card.cardParser(legacy.card),
            u_avatar: userLegacy?.profile_image_url_https?.replace('_normal', ''),
        } as GenericArticle<Platform.X>

        let urls = legacy.entities.urls || []
        let media_urls = legacy.entities.media?.map((m: { url: string }) => m.url) || []
        for (const u of urls) {
            if (u.expanded_url && !u.expanded_url.startsWith('https://x.com/')) {
                tweet.content = tweet.content?.replace(u.url, u.expanded_url) ?? null
            } else {
                tweet.content = tweet.content?.replace(u.url, '') ?? null
            }
        }
        for (const url of media_urls) {
            tweet.content = tweet.content?.replace(url, '') ?? null
        }
        return tweet as GenericArticle<Platform.X>
    }

    export function tweetsArticleParser(json: any) {
        let tweets = sanitizeTweetsJson(json)
        tweets = tweets
            .filter(
                (t: { entryId: string }) =>
                    t.entryId.startsWith('tweet-') && !t.entryId.startsWith('profile-conversation'),
            )
            .map((t: { content: any }) => t.content?.itemContent?.tweet_results?.result)
            .filter(Boolean)
        return tweets.map(tweetParser).filter(Boolean) as Array<GenericArticle<Platform.X>>
    }

    export function tweetsRepliesParser(json: any) {
        const tweets = sanitizeTweetsJson(json)
        const conversations = tweets
            .filter((t: { entryId: string }) => t.entryId.startsWith('profile-conversation'))
            .map((t: { content: { items: any } }) => t.content.items)
            .map((t: any[]) =>
                t
                    .map(
                        (i) =>
                            i.item?.itemContent?.tweet_results?.result?.tweet ||
                            i.item?.itemContent?.tweet_results?.result,
                    )
                    .filter(Boolean),
            )
        return conversations
            .map((c: any[]) => c.map(tweetParser))
            .map((c: any[]) =>
                c.reduce((acc, t) => {
                    if (acc) {
                        t.ref = acc
                        t.type = ArticleTypeEnum.CONVERSATION
                    }
                    // 去除回复中的@用户名
                    if (/^@\w+ /.test(t.content)) {
                        t.content = t.content.replace(/^@\w+ /, '')
                    }
                    return t
                }, null),
            )
    }

    export function oldFollowsParser(user: any): GenericFollows {
        if (!user) {
            throw new Error('Follows json format may have changed')
        }
        return {
            platform: Platform.X,
            username: user?.name,
            u_id: user?.screen_name,
            followers: user?.followers_count,
        }
    }

    export function tweetsFollowsFromListParser(json: any): Array<GenericFollows> {
        const results = JSONPath({ path: '$..user_results.result', json })
        return results.map((r: any) => {
            return {
                platform: Platform.X,
                username: r?.core?.name,
                u_id: r?.core?.screen_name,
                followers: r?.legacy?.followers_count,
            }
        })
    }

    export function tweetsFollowsParser(json: any): GenericFollows {
        const user = JSONPath({ path: '$..user.result.legacy', json })[0]
        if (!user) {
            throw new Error('Follows json format may have changed')
        }
        return {
            platform: Platform.X,
            username: user?.name,
            u_id: user?.screen_name,
            followers: user?.followers_count,
        }
    }

    /**
     * @param url https://x.com/username
     * @description grab tweets from user page
     */
    export async function grabTweets(
        page: Page,
        url: string,
        config: {
            viewport?: {
                width: number
                height: number
            }
        } = {
                viewport: defaultViewport,
            },
    ): Promise<Array<GenericArticle<Platform.X>>> {
        const { cleanup, promise: waitForTweets } = waitForResponse(page, async (response, { done, fail }) => {
            const url = response.url()
            if (url.includes('UserTweets') && response.request().method() === 'GET') {
                if (response.status() >= 400) {
                    fail(new Error(`Error: ${response.status()}`))
                    return
                }
                response
                    .json()
                    .then((json) => {
                        done(json)
                    })
                    .catch((error) => {
                        fail(error)
                    })
            }
        })
        try {
            await page.setViewport(config.viewport ?? defaultViewport)
            await page.goto(url)
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
        const tweets_json = data.data

        return XApiJsonParser.tweetsArticleParser(tweets_json)
    }

    /**
     * @param url https://x.com/username/replies
     * @description grab replies from user page
     */
    export async function grabReplies(
        page: Page,
        url: string,
        config: {
            viewport?: {
                width: number
                height: number
            }
        } = {
                viewport: defaultViewport,
            },
    ): Promise<Array<GenericArticle<Platform.X>>> {
        const { cleanup, promise: waitForTweets } = waitForResponse(page, async (response, { done, fail }) => {
            const url = response.url()
            if (url.includes('UserTweetsAndReplies') && response.request().method() === 'GET') {
                if (response.status() >= 400) {
                    fail(new Error(`Error: ${response.status()}`))
                    return
                }
                response
                    .json()
                    .then((json) => {
                        done(json)
                    })
                    .catch((error) => {
                        fail(error)
                    })
            }
        })
        await page.setViewport(config.viewport ?? defaultViewport)
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
        const tweets_json = data.data
        return XApiJsonParser.tweetsRepliesParser(tweets_json)
    }

    /**
     * @param url https://x.com/username
     */
    export async function grabFollowsNumber(page: Page, url: string): Promise<GenericFollows> {
        const { promise: waitForTweets } = waitForResponse(page, async (response, { done, fail }) => {
            const url = response.url()
            if (url.includes('UserByScreenName') && response.request().method() === 'GET') {
                if (response.status() >= 400) {
                    fail(new Error(`Error: ${response.status()}`))
                    return
                }
                response
                    .json()
                    .then((json) => {
                        done(json)
                    })
                    .catch((error) => {
                        fail(error)
                    })
            }
        })
        await page.setViewport(defaultViewport)
        await page.goto(url)

        const data = await waitForTweets
        if (!data.success) {
            throw data.error
        }
        const user_json = data.data
        return XApiJsonParser.tweetsFollowsParser(user_json)
    }

    /**
     * Check if there is something wrong on the page of https://x.com/username
     */
    export async function checkSomethingWrong(page: Page) {
        const retry_button = await page
            .waitForSelector('nav[role="navigation"] + div > button', { timeout: 1000 })
            .catch(() => null)
        if (retry_button) {
            const error = await page.$('nav[role="navigation"] + div > div:first-child')
            throw new Error(
                `Something wrong on the page, maybe you have reached the limit or cookies are expired: ${await error?.evaluate((e) => e.textContent)}`,
            )
        }
    }

    export async function checkLogin(page: Page) {
        const login_button = await page
            .waitForSelector('a[href="/login"], [href*="/i/flow/login"]', { timeout: 1000 })
            .catch(() => null)
        if (login_button) {
            throw new Error('You need to login first, check your cookies')
        }
    }
}

enum CardTypeEnum {
    NONE = 'none',
    PLAYER = 'player',
    IMAGE = 'image',
    CHOICE = 'choice',
    SPACE = 'space',
}

type CardDataMedia = {
    title?: string
    description?: string
    domain?: string
    thumbnail_url?: string
}

type CardDataMapping = {
    [CardTypeEnum.PLAYER]: CardDataMedia & {
        player_url: string
    }
    [CardTypeEnum.IMAGE]: CardDataMedia
    [CardTypeEnum.CHOICE]: {
        choices: Array<{
            name: string
            count: string
        }>
    }
    [CardTypeEnum.SPACE]: {}
    [CardTypeEnum.NONE]: {}
}
type Card<T extends CardTypeEnum> = {
    type: T
    card_url: string
} & CardDataMapping[T]

type ExtraContentType = Card<CardTypeEnum> | null

export { ArticleTypeEnum, XApiJsonParser, XUserTimeLineSpider, XListSpider }

export type { ExtraContentType, XListApiEngine }
