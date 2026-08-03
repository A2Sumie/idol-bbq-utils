import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { TextChunkMiddleware } from './middlewares'
import type { ForwarderContext } from './types'

function buildContext(article: any, text: string): ForwarderContext {
    return {
        text,
        article,
        media: [],
        timestamp: article?.created_at,
        config: {} as any,
        metadata: new Map(),
        aborted: false,
    }
}

test('TextChunkMiddleware keeps website author and crawl time when a long blog is truncated', async () => {
    const article = {
        id: 502,
        a_id: '452614',
        u_id: '22/7:official-blog',
        username: '南伊織',
        created_at: 1785681739,
        content: `【TIF2026ありがとうございました。】\n\n${'とても長い本文。'.repeat(400)}`,
        url: 'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/452614',
        type: 'article',
        ref: null,
        has_media: false,
        media: [],
        extra: {
            extra_type: 'website_meta',
            data: {
                site: '22/7',
                feed: 'official-blog',
                member: '南伊織',
                title: 'TIF2026ありがとうございました。',
                time_source: 'crawl_observed',
            },
        },
        u_avatar: null,
        platform: Platform.Website,
    }

    const middleware = new TextChunkMiddleware(1000)
    const context = buildContext(article, article.content)

    await middleware.process(context, async () => {})

    const chunks = context.metadata.get('chunks') as string[]
    expect(chunks).toHaveLength(1)
    const chunk = chunks[0]!
    expect(chunk.length).toBeLessThanOrEqual(1000)
    expect(chunk).toContain('【22/7 博客｜南伊織】TIF2026ありがとうございました。')
    expect(chunk).toContain('22/7官网 博客 抓取于')
    expect(chunk).toContain('https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/452614')
    expect(context.metadata.get('text_truncated')).toBe(true)
})

test('TextChunkMiddleware leaves short website text untouched', async () => {
    const article = {
        id: 1,
        a_id: '1',
        u_id: '22/7:official-blog',
        username: '南伊織',
        created_at: 1785681739,
        content: '短い本文',
        url: 'https://example.com/blog',
        type: 'article',
        ref: null,
        has_media: false,
        media: [],
        extra: {
            extra_type: 'website_meta',
            data: { site: '22/7', feed: 'official-blog', member: '南伊織', time_source: 'crawl_observed' },
        },
        u_avatar: null,
        platform: Platform.Website,
    }

    const middleware = new TextChunkMiddleware(1000)
    const context = buildContext(article, '南伊織 抓取于 0100⁹ Web更新\n\n短い本文')

    await middleware.process(context, async () => {})

    expect(context.metadata.get('chunks')).toEqual(['南伊織 抓取于 0100⁹ Web更新\n\n短い本文'])
    expect(context.metadata.get('text_truncated')).toBeUndefined()
})
