import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import {
    formatArticleAttributionLine,
    formatArticleHeaderLine,
    formatTranslationPassthrough,
    PASSTHROUGH_CARD_DEFERRED_MARKER,
} from '../src/text'
import type { Article } from '../src/types'

const IORI_TS = Math.floor(Date.UTC(2026, 6, 20, 6, 56, 0) / 1000)

function xArticle(type: string): Article {
    return {
        platform: Platform.X,
        a_id: `x-${type}`,
        u_id: 'minami__iori',
        username: '南伊織【22/7】',
        created_at: IORI_TS,
        content: '',
        url: 'https://x.com/minami__iori/status/1',
        type,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }
}

function websiteArticle(timeSource: string): Article {
    return {
        platform: Platform.Website,
        a_id: `website-${timeSource}`,
        u_id: '22/7:official-news',
        username: '22/7 Official News',
        created_at: 1710000000,
        content: 'Website body',
        url: 'https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/1',
        type: 'article',
        ref: null,
        has_media: false,
        media: [],
        extra: {
            extra_type: 'website_meta',
            data: {
                site: '22/7',
                feed: 'official-news',
                time_source: timeSource,
            },
        },
        u_avatar: null,
    }
}

test('website estimated publish time is marked as EST in render metadata', () => {
    const article = websiteArticle('estimated_publish')

    expect(formatArticleHeaderLine(article)).toContain('0100 EST.')
    expect(formatArticleAttributionLine(article)).toContain('0100 EST.')
})

test('website crawl-observed time says it is a crawl timestamp', () => {
    const article = websiteArticle('crawl_observed')

    expect(formatArticleHeaderLine(article)).toContain('抓取于 0100⁹')
    expect(formatArticleAttributionLine(article)).toContain('抓取于 0100⁹（240310）')
})

test('translation passthrough uses the title/body/blank/attribution layout', () => {
    const article = xArticle('tweet')
    const text = formatTranslationPassthrough(
        article,
        '刚才比平时更kururun（轻飘飘开心）呢。注意到的人请举手！',
    )

    expect(text).toBe(
        [
            '南伊織【22/7】1556⁹(260720)',
            '刚才比平时更kururun（轻飘飘开心）呢。注意到的人请举手！',
            '',
            '@minami__iori 南伊織【22/7】 1556⁹(260720) X发推',
        ].join('\n'),
    )
})

test('translation passthrough title joins name and time with no separating space', () => {
    const text = formatTranslationPassthrough(xArticle('tweet'), '译文')
    expect(text.split('\n')[0]).toBe('南伊織【22/7】1556⁹(260720)')
    expect(text.split('\n').at(-1)).toBe('@minami__iori 南伊織【22/7】 1556⁹(260720) X发推')
})

test('translation passthrough ref-only case defers the body to the card', () => {
    const article = xArticle('retweet')
    const text = formatTranslationPassthrough(article, '')

    expect(text).toBe(
        ['', PASSTHROUGH_CARD_DEFERRED_MARKER, '@minami__iori 南伊織【22/7】 1556⁹(260720) X转推'].join('\n'),
    )
    expect(text.startsWith('\n')).toBe(true)
    expect(text).toContain('余下见卡片')
})

test('translation passthrough drops a redundant @handle when it equals the display name', () => {
    const article = { ...xArticle('tweet'), username: 'minami__iori' }
    const text = formatTranslationPassthrough(article, '译文')
    const lines = text.split('\n')
    expect(lines[0]).toBe('minami__iori1556⁹(260720)')
    expect(lines.at(-1)).toBe('@minami__iori 1556⁹(260720) X发推')
})
