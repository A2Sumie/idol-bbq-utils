import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { formatArticleAttributionLine, formatArticleHeaderLine } from '../src/text'
import type { Article } from '../src/types'

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
