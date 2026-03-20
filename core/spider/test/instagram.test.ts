import puppeteer from 'puppeteer-core'
import { Spider } from '../src'
import { parseNetscapeCookieToPuppeteerCookie, UserAgent } from '../src/utils'
import { readFileSync } from 'fs'
import { join } from 'path'
import { createLogger, winston, format } from '@idol-bbq-utils/log'
import type { GenericFollows } from '../src/types'
import { InsApiJsonParser } from '../src/spiders/instagram'
import { test, expect } from 'bun:test'

const dataPath = (...parts: Array<string>) => join(import.meta.dir, 'data', ...parts)

/**
 * require network access & headless browser
 */
test.skip('spider', async () => {
    const url = 'https://www.instagram.com/instagram'
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
        expect(id).toBe('instagram')
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

test('Instagram API JSON Parser', async () => {
    const posts_json = JSON.parse(readFileSync(dataPath('instagram', 'instagram-posts.json'), 'utf-8'))
    const profile_json = JSON.parse(readFileSync(dataPath('instagram', 'instagram-profile.json'), 'utf-8'))

    const posts_json_result = JSON.parse(readFileSync(dataPath('instagram', 'instagram-posts-result.json'), 'utf-8'))
    const profile_json_result = JSON.parse(
        readFileSync(dataPath('instagram', 'instagram-follows-result.json'), 'utf-8'),
    )

    const posts = InsApiJsonParser.postsParser(posts_json)

    expect(posts).toHaveLength(posts_json_result.length)
    expect(posts[0]).toMatchObject({
        a_id: posts_json_result[0].a_id,
        u_id: posts_json_result[0].u_id,
        username: posts_json_result[0].username,
        url: posts_json_result[0].url,
        type: posts_json_result[0].type,
    })
    expect(posts.every((item) => item.username.length > 0)).toBeTrue()
    expect(posts.every((item) => item.u_id.length > 0)).toBeTrue()
    expect(posts.some((item) => (item.media?.length ?? 0) > 0)).toBeTrue()
    expect(InsApiJsonParser.followsParser(profile_json)).toMatchObject({
        platform: 2,
        u_id: profile_json_result.u_id,
        username: profile_json_result.username,
        followers: profile_json_result.followers,
    })
    expect(InsApiJsonParser.profileStatusParser(profile_json)).toMatchObject({
        platform: 2,
        u_id: profile_json_result.u_id,
        username: profile_json_result.username,
        is_live: false,
        live_broadcast_id: null,
        live_broadcast_visibility: null,
        live_url: null,
    })
})

test('Instagram profile status parser detects live broadcasts', () => {
    const profile_json = {
        data: {
            user: {
                username: 'shiina_satsuki227',
                full_name: '椎名桜月',
                profile_pic_url_hd: 'https://example.com/avatar.jpg',
                live_broadcast_id: '1234567890',
                live_broadcast_visibility: 'public',
            },
        },
    }

    expect(InsApiJsonParser.profileStatusParser(profile_json)).toMatchObject({
        platform: 2,
        u_id: 'shiina_satsuki227',
        username: '椎名桜月',
        is_live: true,
        live_broadcast_id: '1234567890',
        live_broadcast_visibility: 'public',
        live_url: 'https://www.instagram.com/shiina_satsuki227/live/',
    })
})

test('Instagram extractBasicInfo preserves dotted profile handles', () => {
    expect(Spider.extractBasicInfo('https://www.instagram.com/nananijigram22_7_the.3rd/')?.u_id).toBe(
        'nananijigram22_7_the.3rd',
    )
    expect(Spider.extractBasicInfo('https://www.instagram.com/p/DV0oKjQEcFT/')).toBeUndefined()
})

test('Instagram stories keep a non-empty username when og:title does not expose it', async () => {
    const page = {
        goto: async () => undefined,
        waitForSelector: async () => {
            throw new Error('not found')
        },
        $$: async () => [
            {
                evaluate: async () =>
                    JSON.stringify({
                        xdt_api__v1__feed__reels_media: true,
                        reels_media: [
                            {
                                user: {
                                    username: 'nananijigram22_7',
                                },
                                items: [
                                    {
                                        id: '36963634381048167_1',
                                        taken_at: 1773845200,
                                        accessibility_caption: '1. Story caption',
                                        image_versions2: {
                                            candidates: [{ width: 720, url: 'https://example.com/story.jpg' }],
                                        },
                                    },
                                ],
                            },
                        ],
                    }),
            },
        ],
        $: async () => ({
            evaluate: async () => 'Instagram',
        }),
    } as any

    const stories = await InsApiJsonParser.grabStories(page, 'https://www.instagram.com/stories/nananijigram22_7/')

    expect(stories).toHaveLength(1)
    expect(stories[0]?.a_id).toBe('36963634381048167')
    expect(stories[0]?.u_id).toBe('nananijigram22_7')
    expect(stories[0]?.username).toBe('nananijigram22_7')
    expect(stories[0]?.url).toBe('https://www.instagram.com/stories/nananijigram22_7/36963634381048167')
})
