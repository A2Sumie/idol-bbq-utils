import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import {
    CARD_FONT_FAMILY,
    CARD_UI_FONT_FAMILY,
    articleParser,
    estimateTextLinesHeight,
    layoutMediaRows,
    resolve227WebsiteBrandKey,
    sanitizeCardText,
} from '../template/img/DefaultCard'
import { languageFontMap } from '../src/img/utils/font'
import { getIconCode } from '../src/img/utils/twemoji'
import { ImgConverter, isSupportedOpenTypeFont, loadDynamicAsset } from '../src/img'

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

function findReactElement(node: any, predicate: (node: any) => boolean): any {
    if (!node) {
        return null
    }
    if (Array.isArray(node)) {
        for (const child of node) {
            const match = findReactElement(child, predicate)
            if (match) {
                return match
            }
        }
        return null
    }
    if (typeof node !== 'object') {
        return null
    }
    if (predicate(node)) {
        return node
    }
    if (typeof node.type === 'function') {
        return findReactElement(node.type(node.props), predicate)
    }
    return findReactElement(node.props?.children, predicate)
}

function findReactElements(node: any, predicate: (node: any) => boolean): any[] {
    if (!node) {
        return []
    }
    if (Array.isArray(node)) {
        return node.flatMap((child) => findReactElements(child, predicate))
    }
    if (typeof node !== 'object') {
        return []
    }
    const children = typeof node.type === 'function' ? node.type(node.props) : node.props?.children
    return [...(predicate(node) ? [node] : []), ...findReactElements(children, predicate)]
}

function readPngSize(buffer: Buffer) {
    expect(buffer.subarray(0, 8).toString('hex')).toBe('89504e470d0a1a0a')
    return {
        width: buffer.readUInt32BE(16),
        height: buffer.readUInt32BE(20),
    }
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

test('translated-corner-badge feature renders sparse pink geometry watermark without text badge', () => {
    const article = {
        id: -1,
        platform: Platform.X,
        a_id: 'summary-card-test',
        u_id: 'message_pack',
        username: '聚合',
        created_at: 1710000000,
        content: '聚合',
        translation: null,
        translated_by: null,
        url: '',
        type: 'message_pack',
        ref: null,
        has_media: false,
        media: [],
        extra: {
            extra_type: 'message_pack_meta',
            data: {
                range: '1条 / 1900～2100',
                translated_badge_label: '译文',
                groups: [],
            },
        },
        u_avatar: null,
    }
    const { component } = articleParser(article as any, { features: ['translated-corner-badge'] } as any)
    const card = findReactElement(component, (node) => node.props?.style?.background === '#ffffff')
    const pinkFill = findReactElement(component, (node) => node.props?.style?.background === '#fff7fb')
    const pattern = findReactElement(component, (node) => node.props?.['data-translated-pattern'] === 'true')
    const clusters = findReactElements(
        component,
        (node) => node.props?.['data-translated-pattern-cluster'] === 'true',
    )
    const geometryShapes = findReactElements(component, (node) =>
        ['circle', 'square', 'triangle', 'diamond'].includes(node.props?.['data-translated-pattern-shape']),
    )
    const xShape = findReactElement(component, (node) => node.props?.['data-translated-pattern-shape'] === 'x')
    const visibleTextBadge = findReactElement(component, (node) => node.props?.children === '译文')

    expect(card).toBeTruthy()
    expect(pinkFill).toBeNull()
    expect(pattern).toBeTruthy()
    expect(clusters.length).toBe(0)
    expect(geometryShapes.length).toBe(3)
    expect(geometryShapes.map((shape) => shape.props.style.left)).toEqual([190, 306, 422])
    expect(geometryShapes.map((shape) => shape.props.style.top)).toEqual([34, 34, 34])
    expect(geometryShapes[0]?.props.style.width).toBe(48)
    expect(geometryShapes[0]?.props.style.height).toBe(48)
    expect(xShape).toBeNull()
    expect(visibleTextBadge).toBeNull()
})

test('translated-corner-badge watermark uses a staggered polka-dot grid on long cards', () => {
    const groups = Array.from({ length: 8 }, (_, index) => ({
        title: `${index + 1}. 消息串 1900～2100`,
        avatars: [{ name: `member-${index}` }],
        items: [
            {
                index: 1,
                text:
                    `@member_${index} 190${index}⁹ X发推\n\n` +
                    '今日はライブのお知らせと感想をまとめました。読みやすい長さの本文を保持します。\n' +
                    '引用や補足も聚合卡里は省略しません。',
            },
        ],
    }))
    const article = {
        id: -1,
        platform: Platform.X,
        a_id: 'long-summary-card-watermark-grid-test',
        u_id: 'message_pack',
        username: '聚合',
        created_at: 1710000000,
        content: '聚合',
        translation: null,
        translated_by: null,
        url: '',
        type: 'message_pack',
        ref: null,
        has_media: false,
        media: [],
        extra: {
            extra_type: 'message_pack_meta',
            data: {
                range: '8条 / 1900～2100',
                translated_badge_label: '译文',
                groups,
            },
        },
        u_avatar: null,
    }
    const { component } = articleParser(article as any, { features: ['translated-corner-badge'] } as any)
    const geometryShapes = findReactElements(component, (node) =>
        ['circle', 'square', 'triangle', 'diamond'].includes(node.props?.['data-translated-pattern-shape']),
    )
    const firstRow = geometryShapes.slice(0, 3)
    const secondRow = geometryShapes.slice(3, 5)

    expect(geometryShapes.length).toBeGreaterThan(16)
    expect(new Set(geometryShapes.map((shape) => shape.props?.['data-translated-pattern-shape'])).size).toBe(4)
    expect(firstRow.map((shape) => shape.props.style.left)).toEqual([190, 306, 422])
    expect(secondRow.map((shape) => shape.props.style.left)).toEqual([248, 364])
    expect(geometryShapes[3]?.props.style.top - geometryShapes[0]?.props.style.top).toBe(132)
})

test('long message-pack cards keep only a small height safety margin', () => {
    const groups = Array.from({ length: 8 }, (_, index) => ({
        title: `${index + 1}. 消息串 1900～2100`,
        avatars: [{ name: `member-${index}` }],
        items: [
            {
                index: 1,
                text:
                    `@member_${index} 190${index}⁹ X发推\n\n` +
                    '今日はライブのお知らせと感想をまとめました。読みやすい長さの本文を保持します。\n' +
                    '引用や補足も聚合卡里は省略しません。',
            },
        ],
    }))
    const article = {
        id: -1,
        platform: Platform.X,
        a_id: 'long-summary-card-height-test',
        u_id: 'message_pack',
        username: '聚合',
        created_at: 1710000000,
        content: '聚合',
        translation: null,
        translated_by: null,
        url: '',
        type: 'message_pack',
        ref: null,
        has_media: false,
        media: [],
        extra: {
            extra_type: 'message_pack_meta',
            data: {
                range: '8条 / 1900～2100',
                groups,
            },
        },
        u_avatar: null,
    }

    const { height } = articleParser(article as any)

    expect(height).toBeGreaterThan(1260)
    expect(height).toBeLessThan(1325)
})

test('translated-corner-badge feature renders through satori without layout errors', async () => {
    const article = {
        id: -1,
        platform: Platform.X,
        a_id: 'summary-card-render-test',
        u_id: 'message_pack',
        username: '聚合',
        created_at: 1710000000,
        content: '聚合\n1. sally_amaki发推 2. nananiji_staff转推',
        translation: null,
        translated_by: null,
        url: '',
        type: 'message_pack',
        ref: null,
        has_media: false,
        media: [],
        extra: {
            extra_type: 'message_pack_meta',
            data: {
                range: '2条 / 1900～2100',
                translated_badge_label: '译文',
                groups: [],
            },
        },
        u_avatar: null,
    }

    const img = await new ImgConverter().articleToImg(article as any, { features: ['translated-corner-badge'] })

    expect(img.subarray(0, 8).toString('hex')).toBe('89504e470d0a1a0a')
    expect(readPngSize(img).width).toBeGreaterThan(0)
})

test('font loader rejects TTC collections because satori cannot render them', () => {
    expect(isSupportedOpenTypeFont(Buffer.from('ttcf0000'))).toBeFalse()
    expect(isSupportedOpenTypeFont(Buffer.from('OTTO0000'))).toBeTrue()
})

test('dynamic asset loader can run in deterministic no-remote mode', async () => {
    const previousRemoteAssets = process.env.RENDER_REMOTE_ASSETS
    const previousFetch = globalThis.fetch
    process.env.RENDER_REMOTE_ASSETS = '0'
    globalThis.fetch = (() => {
        throw new Error('fetch should not be called when RENDER_REMOTE_ASSETS=0')
    }) as typeof fetch

    try {
        expect(await loadDynamicAsset('twemoji', 'emoji', '🧪')).toStartWith('data:image/svg+xml;base64,')
        expect(await loadDynamicAsset('twemoji', 'ja-JP', 'テスト')).toEqual([])
        const fallbackFonts = await loadDynamicAsset('twemoji', 'unknown', 'ᓚᘏᗢ')
        expect(fallbackFonts).toBeArray()
        expect((fallbackFonts as any[])[0]?.name).toBe('Unifont')
    } finally {
        if (previousRemoteAssets === undefined) {
            delete process.env.RENDER_REMOTE_ASSETS
        } else {
            process.env.RENDER_REMOTE_ASSETS = previousRemoteAssets
        }
        globalThis.fetch = previousFetch
    }
})
