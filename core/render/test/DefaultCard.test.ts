import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { resolve227WebsiteBrandKey } from '../template/img/DefaultCard'

function buildWebsiteArticle(feed: string, site: string = '22/7') {
    return {
        platform: Platform.Website,
        extra: {
            extra_type: 'website_meta',
            data: {
                site,
                feed,
            },
        },
    } as Parameters<typeof resolve227WebsiteBrandKey>[0]
}

test('resolve227WebsiteBrandKey distinguishes official and FC website feeds', () => {
    expect(resolve227WebsiteBrandKey(buildWebsiteArticle('official-news'))).toBe('official')
    expect(resolve227WebsiteBrandKey(buildWebsiteArticle('official-blog'))).toBe('official')
    expect(resolve227WebsiteBrandKey(buildWebsiteArticle('live-report'))).toBe('official')

    for (const feed of ['fc-news', 'ticket', 'radio', 'movie', 'photo']) {
        expect(resolve227WebsiteBrandKey(buildWebsiteArticle(feed))).toBe('fc')
    }
})

test('resolve227WebsiteBrandKey ignores non-22/7 website data and non-website platforms', () => {
    expect(resolve227WebsiteBrandKey(buildWebsiteArticle('official-news', 'other-site'))).toBeNull()
    expect(
        resolve227WebsiteBrandKey({
            platform: Platform.X,
            extra: {
                extra_type: 'website_meta',
                data: {
                    site: '22/7',
                    feed: 'official-news',
                },
            },
        } as Parameters<typeof resolve227WebsiteBrandKey>[0]),
    ).toBeNull()
    expect(
        resolve227WebsiteBrandKey({
            platform: Platform.Website,
            extra: null,
        } as Parameters<typeof resolve227WebsiteBrandKey>[0]),
    ).toBeNull()
})
