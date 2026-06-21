import puppeteer from 'puppeteer-core'
import { Spider, X } from '@/.'
import { parseNetscapeCookieToPuppeteerCookie, UserAgent } from '@/utils'
import { readFileSync } from 'fs'
import { join } from 'path'
import { createLogger, winston, format } from '@idol-bbq-utils/log'
import { test, expect } from 'bun:test'
import type { GenericFollows } from '@/types'

const dataPath = (...parts: Array<string>) => join(import.meta.dir, 'data', ...parts)

test('X Spider', async () => {
    const url = 'https://x.com/X'
    const spider = Spider.getSpider(url)
    if (spider) {
        let id = await new spider()._match_valid_url(url, spider)?.groups?.id
        expect(id).toBe('X')
    }
})

test('X API mode without a browser preserves the API failure reason', async () => {
    const spider = new X.XUserTimeLineSpider().init()

    await expect(
        spider.crawl('https://x.com/X', undefined, 'api-error-regression', {
            task_type: 'follows',
            crawl_engine: 'api',
        }),
    ).rejects.toThrow('Cookie string is required for API mode')
})

/**
 * require network access & headless browser
 */
test.skip('spider', async () => {
    const url = 'https://x.com/X'
    const spider = Spider.getSpider(url)
    if (spider) {
        const spiderInstance = new spider(
            createLogger({
                defaultMeta: { service: 'tweet-forwarder' },
                level: 'debug',
                format: format.combine(
                    format.colorize(),
                    format.timestamp({ format: 'YYYY-MM-DDTHH:mm:ss.SSSZ' }),
                    format.printf(({ message, timestamp, level, label, service, childService }) => {
                        const metas = [service, childService, label, level]
                            .filter(Boolean)
                            .map((meta) => `[${meta}]`)
                            .join(' ')
                        return `${timestamp} ${metas}: ${message}`
                    }),
                ),
            }),
        ).init()
        let id = await spiderInstance._match_valid_url(url, spider)?.groups?.id
        expect(id).toBe('X')
        const browser = await puppeteer.launch({
            headless: true,
            channel: 'chrome',
        })
        const page = await browser.newPage()
        await page.setUserAgent(UserAgent.CHROME)
        await page.setCookie(...parseNetscapeCookieToPuppeteerCookie('tests/data/expire.cookies'))
        let res = []
        let follows = {} as GenericFollows
        try {
            res = await spiderInstance.crawl(url, page, 'article')
            follows = (await spiderInstance.crawl(url, page, 'follows')) as unknown as GenericFollows
        } catch (e) {
            console.error(e)
        } finally {
            await browser.close()
        }
        expect(res.length).toBeGreaterThan(0)
        expect(follows.followers).toBeGreaterThan(0)
    }
})

function buildXTimelineTweetResult(id: string, userId: string, text: string, replyToId?: string) {
    return {
        __typename: 'Tweet',
        legacy: {
            id_str: id,
            full_text: text,
            created_at: 'Tue Mar 11 20:55:07 +0000 2025',
            entities: {
                urls: [],
                media: [],
            },
            ...(replyToId ? { in_reply_to_status_id_str: replyToId } : {}),
        },
        core: {
            user_results: {
                result: {
                    core: {
                        screen_name: userId,
                        name: userId,
                    },
                    avatar: {
                        image_url: `https://example.com/${userId}_normal.jpg`,
                    },
                },
            },
        },
    }
}

test('X API JSON Parser', async () => {
    const x_json = JSON.parse(readFileSync(dataPath('x', 'x.json'), 'utf-8'))
    const x_result = JSON.parse(readFileSync(dataPath('x', 'x-result.json'), 'utf-8'))
    const x_replies_result = JSON.parse(readFileSync(dataPath('x', 'x-replies-result.json'), 'utf-8'))
    const x_follows = JSON.parse(readFileSync(dataPath('x', 'x-follows.json'), 'utf-8'))
    const x_follows_result = JSON.parse(readFileSync(dataPath('x', 'x-follows-result.json'), 'utf-8'))
    const x_response = X.XApiJsonParser.tweetsArticleParser(x_json)
    const x_replies_response = X.XApiJsonParser.tweetsRepliesParser(x_json)
    const x_follows_response = X.XApiJsonParser.tweetsFollowsParser(x_follows)
    expect(x_response).toEqual(x_result)
    expect(x_replies_response).toEqual(x_replies_result)
    expect(x_follows_response).toEqual(x_follows_result)
})

test('X replies parser reads TimelineAddToModule conversations', async () => {
    const json = {
        data: {
            user: {
                result: {
                    timeline_v2: {
                        timeline: {
                            instructions: [
                                {
                                    type: 'TimelineAddToModule',
                                    moduleItems: [
                                        {
                                            entryId: 'profile-conversation-test-tweet-100',
                                            item: {
                                                itemContent: {
                                                    tweet_results: {
                                                        result: buildXTimelineTweetResult(
                                                            '100',
                                                            'parent_member',
                                                            'parent text',
                                                        ),
                                                    },
                                                },
                                            },
                                        },
                                        {
                                            entryId: 'profile-conversation-test-tweet-101',
                                            item: {
                                                itemContent: {
                                                    tweet_results: {
                                                        result: buildXTimelineTweetResult(
                                                            '101',
                                                            'reply_member',
                                                            '@parent_member reply text',
                                                            '100',
                                                        ),
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            ],
                        },
                    },
                },
            },
        },
    }

    const replies = X.XApiJsonParser.tweetsRepliesParser(json)

    expect(replies).toHaveLength(1)
    expect(replies[0]?.a_id).toBe('101')
    expect(replies[0]?.type).toBe(X.ArticleTypeEnum.CONVERSATION)
    expect(replies[0]?.content).toBe('reply text')
    expect((replies[0]?.ref as any)?.a_id).toBe('100')
})

test('X replies parser keeps direct reply timeline entries', async () => {
    const json = {
        data: {
            user: {
                result: {
                    timeline_v2: {
                        timeline: {
                            instructions: [
                                {
                                    type: 'TimelineAddEntries',
                                    entries: [
                                        {
                                            entryId: 'tweet-201',
                                            content: {
                                                itemContent: {
                                                    tweet_results: {
                                                        result: buildXTimelineTweetResult(
                                                            '201',
                                                            'reply_member',
                                                            'direct reply',
                                                            '200',
                                                        ),
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            ],
                        },
                    },
                },
            },
        },
    }

    const replies = X.XApiJsonParser.tweetsRepliesParser(json)

    expect(replies).toHaveLength(1)
    expect(replies[0]?.a_id).toBe('201')
    expect(replies[0]?.type).toBe(X.ArticleTypeEnum.CONVERSATION)
    expect(replies[0]?.ref).toBe('200')
})

test('X tweet detail parser selects the requested status from a conversation response', async () => {
    const json = {
        data: {
            threaded_conversation_with_injections_v2: {
                instructions: [
                    {
                        type: 'TimelineAddEntries',
                        entries: [
                            {
                                entryId: 'tweet-500',
                                content: {
                                    itemContent: {
                                        tweet_results: {
                                            result: buildXTimelineTweetResult('500', 'other_member', 'thread head'),
                                        },
                                    },
                                },
                            },
                            {
                                entryId: 'tweet-501',
                                content: {
                                    itemContent: {
                                        tweet_results: {
                                            result: buildXTimelineTweetResult('501', 'target_member', 'target text'),
                                        },
                                    },
                                },
                            },
                        ],
                    },
                ],
            },
        },
    }

    const tweet = X.XApiJsonParser.tweetDetailParser(json, '501')

    expect(tweet).toMatchObject({
        a_id: '501',
        u_id: 'target_member',
        content: 'target text',
        url: 'https://x.com/target_member/status/501',
    })
})

test('X follows browser parser fails fast on login pages', async () => {
    const listeners = new Map<string, (data: any) => void>()
    const page = {
        on: (eventName: string, handler: (data: any) => void) => {
            listeners.set(eventName, handler)
        },
        off: (eventName: string, handler: (data: any) => void) => {
            if (listeners.get(eventName) === handler) {
                listeners.delete(eventName)
            }
        },
        setViewport: async () => undefined,
        goto: async () => undefined,
        waitForSelector: async () => ({}),
    } as any

    await expect(X.XApiJsonParser.grabFollowsNumber(page, 'https://x.com/expired')).rejects.toThrow(
        'You need to login first',
    )
    expect(listeners.has('response')).toBeFalse()
})

test('X parser keeps video variants without bitrate', async () => {
    const result = buildXTimelineTweetResult('301', 'video_member', 'video post https://t.co/media')
    result.legacy.entities.media = [
        {
            url: 'https://t.co/media',
            media_url_https: 'https://pbs.twimg.com/ext_tw_video_thumb/301/pu/img/thumb.jpg',
            type: 'video',
        },
    ]
    ;(result.legacy as any).extended_entities = {
        media: [
            {
                url: 'https://t.co/media',
                media_url_https: 'https://pbs.twimg.com/ext_tw_video_thumb/301/pu/img/thumb.jpg',
                type: 'video',
                video_info: {
                    variants: [
                        {
                            content_type: 'application/x-mpegURL',
                            url: 'https://video.twimg.com/ext_tw_video/301/playlist.m3u8',
                        },
                        {
                            content_type: 'video/mp4',
                            url: 'https://video.twimg.com/ext_tw_video/301/vid/720x720/video.mp4',
                        },
                    ],
                },
            },
        ],
    }
    const json = {
        data: {
            user: {
                result: {
                    timeline_v2: {
                        timeline: {
                            instructions: [
                                {
                                    type: 'TimelineAddEntries',
                                    entries: [
                                        {
                                            entryId: 'tweet-301',
                                            content: {
                                                itemContent: {
                                                    tweet_results: {
                                                        result,
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            ],
                        },
                    },
                },
            },
        },
    }

    const tweets = X.XApiJsonParser.tweetsArticleParser(json)

    expect(tweets[0]?.content).toBe('video post ')
    expect(tweets[0]?.media).toEqual([
        {
            type: 'video',
            url: 'https://video.twimg.com/ext_tw_video/301/vid/720x720/video.mp4',
        },
        {
            type: 'video_thumbnail',
            url: 'https://pbs.twimg.com/ext_tw_video_thumb/301/pu/img/thumb.jpg',
        },
    ])
})

test('X old API parser tolerates missing entities', async () => {
    const tweet = X.XApiJsonParser.oldTweetParser({
        id_str: '401',
        full_text: 'legacy post',
        created_at: 'Tue Mar 11 20:55:07 +0000 2025',
        user: {
            screen_name: 'legacy_member',
            name: 'Legacy Member',
        },
    })

    expect(tweet).toMatchObject({
        a_id: '401',
        u_id: 'legacy_member',
        content: 'legacy post',
        media: null,
        has_media: false,
    })
})

test('X unified list hydration honors configured concurrency', async () => {
    const spider = new X.XListSpider()
    let activeRequests = 0
    let maxActiveRequests = 0
    const requestedUsers: Array<string> = []
    const client = {
        grabTweets: async (userId: string) => {
            activeRequests += 1
            maxActiveRequests = Math.max(maxActiveRequests, activeRequests)
            requestedUsers.push(userId)
            await new Promise((resolve) => setTimeout(resolve, 1))
            activeRequests -= 1
            return []
        },
        grabReplies: async () => [],
    }

    await (spider as any).hydrateUsersFromListActivity(['alpha', 'beta', 'gamma'], client, 'cookie', {
        fetchTweets: true,
        fetchReplies: false,
        hydrateConcurrency: 1,
    })

    expect(maxActiveRequests).toBe(1)
    expect(requestedUsers).toEqual(['alpha', 'beta', 'gamma'])
})

test('X unified list hydration preserves tweets when replies fail', async () => {
    const spider = new X.XListSpider()
    const client = {
        grabTweets: async (userId: string) => [
            {
                platform: 0,
                a_id: `${userId}-tweet`,
                u_id: userId,
                username: userId,
                created_at: 1,
                content: 'tweet',
                url: `https://x.com/${userId}/status/1`,
                type: 'tweet',
                ref: null,
                media: null,
                has_media: false,
                extra: null,
            },
        ],
        grabReplies: async () => {
            throw new Error('Failed to fetch replies: 404 Not Found')
        },
    }

    const articles = await (spider as any).hydrateUsersFromListActivity(['alpha'], client, 'cookie', {
        fetchTweets: true,
        fetchReplies: true,
        hydrateConcurrency: 1,
    })

    expect(articles.map((article: any) => article.a_id)).toEqual(['alpha-tweet'])
})

test('X unified list hydration stops after rate limit response', async () => {
    const spider = new X.XListSpider()
    const requestedUsers: Array<string> = []
    const client = {
        grabTweets: async (userId: string) => {
            requestedUsers.push(userId)
            throw new Error('Failed to fetch tweets: 429 Too Many Requests')
        },
        grabReplies: async () => [],
    }

    const articles = await (spider as any).hydrateUsersFromListActivity(['alpha', 'beta', 'gamma'], client, 'cookie', {
        fetchTweets: true,
        fetchReplies: true,
        hydrateConcurrency: 1,
    })

    expect(articles).toEqual([])
    expect(requestedUsers).toEqual(['alpha'])
})
