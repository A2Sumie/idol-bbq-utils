import { spiderRegistry } from '../src'
import { readFileSync } from 'fs'
import type { GenericFollows } from '../src/types'
import { TiktokApiJsonParser, TiktokSpider } from '../src/spiders/tiktok'
import { test, expect } from 'bun:test'

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

test('TikTok Spider URL Extraction', () => {
    const url = 'https://www.tiktok.com/@nirei_nozomi'
    const info = spiderRegistry.extractBasicInfo(url)

    expect(info).not.toBeNull()
    expect(info?.u_id).toBe('nirei_nozomi')
    expect(info?.platform).toBeDefined()
})

/**
 * require network access & headless browser
 */
test('TikTok Spider Integration', async () => {
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
        itemList: [{
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
        }],
    })

    expect(posts).toHaveLength(1)
    expect(posts[0]?.media?.map((item) => item.type)).toEqual(['video_thumbnail', 'video_thumbnail'])
    expect(posts[0]?.media?.some((item) => item.type === 'video')).toBeFalse()
})
