import { spiderRegistry } from '../src'
import { readFileSync } from 'fs'
import type { GenericFollows } from '../src/types'
import { Platform } from '../src/types'
import { TiktokApiJsonParser, TiktokSpider } from '../src/spiders/tiktok'
import { HTTPClient } from '../src/utils'
import { test, expect } from 'bun:test'

function tiktokUniversalHtml(universalData: unknown) {
    return `<html><body><script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">${JSON.stringify(
        universalData,
    )}</script></body></html>`
}

function tiktokPost(handle: string, postId: string, desc = 'page post') {
    return {
        id: postId,
        createTime: 1710759600,
        desc,
        author: { uniqueId: handle, nickname: handle },
        video: { cover: 'https://example.com/cover.jpg' },
    }
}

function tiktokPageHtml(params: { handle: string; postId: string; secUid: string; itemList?: Array<any> }) {
    return tiktokUniversalHtml({
        __DEFAULT_SCOPE__: {
            'webapp.user-detail': {
                userInfo: {
                    user: {
                        uniqueId: params.handle,
                        nickname: params.handle,
                        secUid: params.secUid,
                    },
                    itemList: params.itemList ?? [tiktokPost(params.handle, params.postId)],
                },
            },
        },
    })
}

test('TikTok Spider URL Validation', async () => {
    const url = 'https://www.tiktok.com/@tiktok'
    const plugin = spiderRegistry.findByUrl(url)

    expect(plugin).not.toBeNull()
    expect(plugin?.id).toBe('tiktok')

    if (plugin) {
        const spider = plugin.create()
        const match = spider._match_valid_url(url, TiktokSpider)
        expect(match?.groups?.id).toBe('tiktok')
    }
})

test('TikTok Spider URL Validation supports dotted handles', async () => {
    const url = 'https://www.tiktok.com/@nananiji.official'
    const plugin = spiderRegistry.findByUrl(url)

    expect(plugin).not.toBeNull()
    expect(plugin?.id).toBe('tiktok')

    if (plugin) {
        const spider = plugin.create()
        const match = spider._match_valid_url(url, TiktokSpider)
        expect(match?.groups?.id).toBe('nananiji.official')
    }
    const videoMatch = plugin?.create()._match_valid_url('https://www.tiktok.com/@nananiji.official/video/123', TiktokSpider)
    expect(videoMatch?.groups?.id).toBe('nananiji.official')
    expect(videoMatch?.groups?.videoId).toBe('123')
})

test('TikTok Spider URL Extraction', () => {
    const url = 'https://www.tiktok.com/@nirei_nozomi'
    const info = spiderRegistry.extractBasicInfo(url)

    expect(info).not.toBeNull()
    expect(info?.u_id).toBe('nirei_nozomi')
    expect(info?.platform).toBeDefined()
})

test('TikTok Spider rotates web identity cache on a three minute ttl', () => {
    const spider = new TiktokSpider()

    expect((spider as any).expire).toBe(60 * 3)
})

/**
 * require network access & headless browser
 */
test.skip('TikTok Spider Integration', async () => {
    const url = 'https://www.tiktok.com/@tiktok'
    const plugin = spiderRegistry.findByUrl(url)

    expect(plugin).not.toBeNull()
    if (!plugin) return

    const spider = plugin.create()
    let res = []
    let follows = [] as Array<GenericFollows>

    try {
        res = await spider.crawl(url, undefined, 'test-task', { task_type: 'article' })
        follows = (await spider.crawl(url, undefined, 'test-task', {
            task_type: 'follows',
        })) as unknown as Array<GenericFollows>
    } catch (e) {
        console.error(e)
    }

    expect(res.length).toBeGreaterThan(0)
    expect(follows[0]?.followers).toBeGreaterThan(0)
})

test.skip('TikTok API JSON Parser', async () => {
    const posts_json = JSON.parse(readFileSync('test/data/tiktok/tiktok-posts.json', 'utf-8'))
    const follows_json = JSON.parse(readFileSync('test/data/tiktok/tiktok-follows.json', 'utf-8'))

    const posts_json_result = JSON.parse(readFileSync('test/data/tiktok/tiktok-posts-result.json', 'utf-8'))
    const follows_json_result = JSON.parse(readFileSync('test/data/tiktok/tiktok-follows-result.json', 'utf-8'))

    expect(TiktokApiJsonParser.postsParser(posts_json)).toEqual(posts_json_result)
    expect(TiktokApiJsonParser.followsParser(follows_json)).toEqual(follows_json_result)
})

test('TikTok API JSON Parser tolerates missing bitrateInfo', () => {
    const posts = TiktokApiJsonParser.postsParser({
        itemList: [
            {
                id: '7351147085025500001',
                createTime: 1710759600,
                desc: 'Regression case',
                author: {
                    uniqueId: 'cure_rinochi',
                    nickname: 'Cure Rinochi',
                    avatarLarger: 'https://example.com/avatar.jpg',
                },
                video: {
                    cover: 'https://example.com/cover.jpg',
                    originCover: 'https://example.com/origin-cover.jpg',
                },
            },
        ],
    })

    expect(posts).toHaveLength(1)
    expect(posts[0]?.media?.map((item) => item.type)).toEqual(['video_thumbnail', 'video_thumbnail'])
    expect(posts[0]?.media?.some((item) => item.type === 'video')).toBeFalse()
})

test('TikTok API JSON Parser normalizes structured media urls', () => {
    const posts = TiktokApiJsonParser.postsParser({
        itemList: [
            {
                id: '7351147085025500003',
                createTime: 1710759603,
                desc: 'Structured media case',
                author: {
                    uniqueId: 'nananiji.official',
                    nickname: 'Nananiji Official',
                    avatarLarger: {
                        url_list: ['https://example.com/avatar.jpg\\u0026name=large'],
                    },
                },
                video: {
                    cover: {
                        UrlList: ['https://example.com/cover.jpg\\u0026format=webp'],
                    },
                    bitrateInfo: [
                        {
                            bitrate: 100,
                            playAddr: {
                                url_list: ['https://example.com/video-low.mp4'],
                            },
                        },
                        {
                            bitrate: 1000,
                            playAddr: {
                                url_list: ['https://example.com/video-high.mp4'],
                            },
                        },
                    ],
                },
            },
        ],
    })

    expect(posts[0]).toMatchObject({
        u_id: 'nananiji.official',
        u_avatar: 'https://example.com/avatar.jpg&name=large',
        media: [
            { type: 'video_thumbnail', url: 'https://example.com/cover.jpg&format=webp' },
            { type: 'video', url: 'https://example.com/video-high.mp4' },
        ],
    })
})

test('TikTok API JSON Parser extracts single video detail pages', () => {
    const posts = TiktokApiJsonParser.videoParser({
        __DEFAULT_SCOPE__: {
            'webapp.video-detail': {
                itemInfo: {
                    itemStruct: {
                        id: '7660143895505947924',
                        createTime: 1783516238,
                        desc: 'Single video page',
                        author: {
                            uniqueId: '227official',
                            nickname: '22/7',
                        },
                        video: {
                            cover: 'https://example.com/cover.jpg',
                            playAddr: 'https://example.com/video.mp4',
                        },
                    },
                },
            },
        },
    })

    expect(posts).toHaveLength(1)
    expect(posts[0]).toMatchObject({
        a_id: '7660143895505947924',
        u_id: '227official',
        url: 'https://www.tiktok.com/@227official/video/7660143895505947924/',
    })
})

test('TikTok API JSON Parser filters malformed post items', () => {
    const posts = TiktokApiJsonParser.postsParser({
        itemList: [
            {
                id: '7351147085025500001',
                createTime: 1710759600,
                desc: 'Valid post',
                author: {
                    uniqueId: 'cure_rinochi',
                    nickname: 'Cure Rinochi',
                },
                video: {
                    cover: 'https://example.com/cover.jpg',
                },
            },
            {
                createTime: 1710759601,
                desc: 'Missing id',
                author: {
                    uniqueId: 'cure_rinochi',
                    nickname: 'Cure Rinochi',
                },
            },
            {
                id: '7351147085025500002',
                createTime: 1710759602,
                desc: 'Missing unique id',
                author: {
                    nickname: 'Cure Rinochi',
                },
            },
        ],
    })

    expect(posts).toHaveLength(1)
    expect(posts[0]?.a_id).toBe('7351147085025500001')
    expect(posts[0]?.u_id).toBe('cure_rinochi')
    expect(posts[0]?.u_avatar).toBeNull()
})

test('TikTok follows parser reports TikTok platform identity', () => {
    const follows = TiktokApiJsonParser.followsParser({
        __DEFAULT_SCOPE__: {
            'webapp.user-detail': {
                userInfo: {
                    user: {
                        uniqueId: 'cure_rinochi',
                        nickname: 'Cure Rinochi',
                    },
                    stats: {
                        followerCount: 227000,
                    },
                },
            },
        },
    })

    expect(follows).toMatchObject({
        platform: Platform.TikTok,
        u_id: 'cure_rinochi',
        username: 'Cure Rinochi',
        followers: 227000,
    })
})

test('TikTok grabPosts keeps page posts when the unsigned creator API is rejected', async () => {
    const original = HTTPClient.download_webpage
    let calls = 0
    ;(HTTPClient as any).download_webpage = async (url: string) => {
        calls += 1
        if (calls === 1) {
            return new Response(
                tiktokPageHtml({ handle: 'cure_rinochi', postId: '7351147085025500001', secUid: 'SEC_UID_ABC' }),
            )
        }
        return new Response(JSON.stringify({ statusCode: 10201, statusMsg: 'rejected' }))
    }

    try {
        const posts = await TiktokApiJsonParser.grabPosts('https://www.tiktok.com/@cure_rinochi', 'abcdef0', 12345)
        expect(calls).toBe(2)
        expect(posts).toHaveLength(1)
        expect(posts[0]?.a_id).toBe('7351147085025500001')
        expect(posts[0]?.u_id).toBe('cure_rinochi')
    } finally {
        ;(HTTPClient as any).download_webpage = original
    }
})

test('TikTok grabPosts keeps page posts when the creator API throws', async () => {
    const original = HTTPClient.download_webpage
    let calls = 0
    ;(HTTPClient as any).download_webpage = async () => {
        calls += 1
        if (calls === 1) {
            return new Response(
                tiktokPageHtml({ handle: 'cure_rinochi', postId: '7351147085025500001', secUid: 'SEC_UID_ABC' }),
            )
        }
        throw new Error('HTTP 403 for creator api')
    }

    try {
        const posts = await TiktokApiJsonParser.grabPosts('https://www.tiktok.com/@cure_rinochi', 'abcdef0', 12345)
        expect(posts).toHaveLength(1)
        expect(posts[0]?.a_id).toBe('7351147085025500001')
    } finally {
        ;(HTTPClient as any).download_webpage = original
    }
})

test('TikTok grabPosts merges creator API posts with page posts without duplicates', async () => {
    const original = HTTPClient.download_webpage
    let calls = 0
    ;(HTTPClient as any).download_webpage = async () => {
        calls += 1
        if (calls === 1) {
            return new Response(
                tiktokPageHtml({ handle: 'cure_rinochi', postId: '7351147085025500001', secUid: 'SEC_UID_ABC' }),
            )
        }
        return new Response(
            JSON.stringify({
                itemList: [
                    tiktokPost('cure_rinochi', '7351147085025500001'),
                    tiktokPost('cure_rinochi', '7351147085025500999', 'deeper api post'),
                ],
            }),
        )
    }

    try {
        const posts = await TiktokApiJsonParser.grabPosts('https://www.tiktok.com/@cure_rinochi', 'abcdef0', 12345)
        expect(posts.map((post) => post.a_id)).toEqual(['7351147085025500001', '7351147085025500999'])
    } finally {
        ;(HTTPClient as any).download_webpage = original
    }
})

test('TikTok grabPosts falls back to ItemModule when matched userInfo has an empty itemList', async () => {
    const original = HTTPClient.download_webpage
    let calls = 0
    ;(HTTPClient as any).download_webpage = async () => {
        calls += 1
        if (calls === 1) {
            return new Response(
                tiktokUniversalHtml({
                    __DEFAULT_SCOPE__: {
                        'webapp.user-detail': {
                            userInfo: {
                                user: {
                                    uniqueId: 'cure_rinochi',
                                    nickname: 'cure_rinochi',
                                    secUid: 'SEC_UID_ABC',
                                },
                                itemList: [],
                            },
                        },
                        ItemModule: {
                            '7351147085025500888': tiktokPost(
                                'cure_rinochi',
                                '7351147085025500888',
                                'item module fallback',
                            ),
                        },
                    },
                }),
            )
        }
        return new Response(JSON.stringify({ statusCode: 10201, statusMsg: 'rejected' }))
    }

    try {
        const posts = await TiktokApiJsonParser.grabPosts('https://www.tiktok.com/@cure_rinochi', 'abcdef0', 12345)
        expect(posts.map((post) => post.a_id)).toEqual(['7351147085025500888'])
    } finally {
        ;(HTTPClient as any).download_webpage = original
    }
})

test('TikTok grabPosts surfaces creator API rejection when the page has no usable posts', async () => {
    const original = HTTPClient.download_webpage
    let calls = 0
    ;(HTTPClient as any).download_webpage = async () => {
        calls += 1
        if (calls === 1) {
            return new Response(
                tiktokPageHtml({
                    handle: 'cure_rinochi',
                    postId: 'unused',
                    secUid: 'SEC_UID_ABC',
                    itemList: [],
                }),
            )
        }
        return new Response(JSON.stringify({ statusCode: 10201, statusMsg: 'rejected' }))
    }

    try {
        await expect(
            TiktokApiJsonParser.grabPosts('https://www.tiktok.com/@cure_rinochi', 'abcdef0', 12345),
        ).rejects.toThrow(/no itemList/)
    } finally {
        ;(HTTPClient as any).download_webpage = original
    }
})

test('TikTok spider routes an X-ingested /video/ URL to a single-video grab', async () => {
    const original = HTTPClient.download_webpage
    const fetchedUrls: string[] = []
    ;(HTTPClient as any).download_webpage = async (url: string) => {
        fetchedUrls.push(url)
        return new Response(
            tiktokUniversalHtml({
                __DEFAULT_SCOPE__: {
                    'webapp.video-detail': {
                        itemInfo: {
                            itemStruct: {
                                id: '7653464242506616085',
                                createTime: 1710759600,
                                desc: 'single video',
                                author: { uniqueId: 'tabesugiyaseruzo', nickname: 'tabesugiyaseruzo' },
                                video: { cover: 'https://example.com/cover.jpg' },
                            },
                        },
                    },
                },
            }),
        )
    }

    try {
        const spider = new TiktokSpider()
        const articles = await spider.crawl(
            'https://www.tiktok.com/@tabesugiyaseruzo/video/7653464242506616085',
            undefined,
            'x-ingested-video',
            { task_type: 'article', crawl_engine: 'api' as any },
        )
        expect(articles.map((a: any) => a.a_id)).toEqual(['7653464242506616085'])
        // Single-video grab hits exactly the detail page, never the creator item_list API.
        expect(fetchedUrls).toEqual(['https://www.tiktok.com/@tabesugiyaseruzo/video/7653464242506616085/'])
        expect(fetchedUrls.some((url) => url.includes('/api/creator/item_list'))).toBe(false)
    } finally {
        ;(HTTPClient as any).download_webpage = original
    }
})
