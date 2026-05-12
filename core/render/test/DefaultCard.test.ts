import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { CARD_FONT_FAMILY, layoutMediaRows, resolve227WebsiteBrandKey } from '../template/img/DefaultCard'

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

test('layoutMediaRows handles three and four image sets without pairing incompatible ratios', () => {
    const wide = { type: 'photo' as const, url: 'wide', width: 1600, height: 900 }
    const portrait = { type: 'photo' as const, url: 'portrait', width: 900, height: 1200 }
    const ultraWide = { type: 'photo' as const, url: 'ultra-wide', width: 1800, height: 500 }
    const ultraTall = { type: 'photo' as const, url: 'ultra-tall', width: 300, height: 1400 }

    expect(layoutMediaRows([wide, portrait, portrait], 0).map((row) => row.length)).toEqual([1, 2])
    expect(layoutMediaRows([portrait, portrait, portrait, portrait], 0).map((row) => row.length)).toEqual([2, 2])
    expect(layoutMediaRows([wide, portrait, ultraWide, ultraTall], 0).map((row) => row.length)).toEqual([1, 1, 1, 1])
})

test('card font family keeps CJK before broad fallback fonts', () => {
    const families = CARD_FONT_FAMILY.split(',').map((font) => font.trim())

    expect(families.indexOf('Noto Sans CJK JP')).toBeLessThan(families.indexOf('Noto Sans'))
    expect(families).toContain('Noto Sans JP')
    expect(families).not.toContain('Unifont')
})
