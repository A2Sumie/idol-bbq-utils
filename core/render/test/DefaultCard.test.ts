import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import {
    CARD_FONT_FAMILY,
    CARD_UI_FONT_FAMILY,
    estimateTextLinesHeight,
    layoutMediaRows,
    resolve227WebsiteBrandKey,
    sanitizeCardText,
} from '../template/img/DefaultCard'
import { languageFontMap } from '../src/img/utils/font'
import { getIconCode } from '../src/img/utils/twemoji'
import { isSupportedOpenTypeFont } from '../src/img'

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

test('layoutMediaRows gives a single portrait image a readable contained shape', () => {
    const portrait = { type: 'photo' as const, url: 'portrait', width: 900, height: 1600 }
    const [[tile]] = layoutMediaRows([portrait], 0)

    expect(tile?.width).toBeGreaterThanOrEqual(340)
    expect(tile?.height).toBeGreaterThanOrEqual(560)
    expect(tile?.height).toBeLessThanOrEqual(620)
})

test('estimateTextLinesHeight stays conservative for long mixed Japanese text', () => {
    const text =
        '【本日の東京】\n最高気温26℃/最低気温15℃\nくもり☁昼前から昼過ぎは晴れ☀\n\n' +
        '服装:日中半袖で⭕朝晩外出の方も薄手の長袖1枚で大丈夫🩵半袖の方はカーディガン等薄手のアウターあると安心。\n'.repeat(
            4,
        )

    expect(estimateTextLinesHeight(text, 16, 492)).toBeGreaterThan(280)
})

test('card font family keeps CJK before broad fallback fonts', () => {
    const families = CARD_FONT_FAMILY.split(',').map((font) => font.trim())

    expect(families.indexOf('Noto Sans CJK JP')).toBeLessThan(families.indexOf('Noto Sans'))
    expect(families).toContain('Noto Sans JP')
    expect(families).not.toContain('Unifont')
})

test('card UI metadata font prefers modern sans before simplified CJK fallback', () => {
    const families = CARD_UI_FONT_FAMILY.split(',').map((font) => font.trim())

    expect(families[0]).toBe('Noto Sans')
    expect(families.indexOf('Noto Sans CJK SC')).toBeGreaterThan(families.indexOf('Noto Sans'))
    expect(families).not.toContain('Noto Sans CJK JP')
})

test('dynamic fallback font list covers decorative lisu-shaped glyphs', () => {
    expect(languageFontMap.unknown).toContain('Noto+Sans+Lisu')
})

test('emoji icon code ignores text and emoji variation selectors', () => {
    expect(getIconCode('❤︎')).toBe('2764')
    expect(getIconCode('❤️')).toBe('2764')
})

test('sanitizeCardText removes stray selectors from rino-style decorative text', () => {
    expect(sanitizeCardText('おはりのち︎︎︎︎❤︎')).toBe('おはりのち❤️')
    expect(sanitizeCardText('今日も素敵🪄︎︎◝✩ ‌‌ ‌')).toBe('今日も素敵🪄◝✩  ')
})

test('font loader rejects TTC collections because satori cannot render them', () => {
    expect(isSupportedOpenTypeFont(Buffer.from('ttcf0000'))).toBeFalse()
    expect(isSupportedOpenTypeFont(Buffer.from('OTTO0000'))).toBeTrue()
})
