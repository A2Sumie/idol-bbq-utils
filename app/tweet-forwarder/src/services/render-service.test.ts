import { describe, expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { formatArticleTimeToken, formatTime } from '@idol-bbq-utils/render'
import { formatPlatformTag, RenderService } from './render-service'
import { fileURLToPath } from 'url'
import DB from '@/db'
import { MediaToolEnum } from '@/types/media'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'fs'
import os from 'os'
import path from 'path'
import { inflateSync } from 'zlib'

process.env.FONTS_DIR = fileURLToPath(new URL('../../../../assets/fonts', import.meta.url))

const SAMPLE_PNG_DATA_URL =
    'data:image/png;base64,' +
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9s1OtS8AAAAASUVORK5CYII='

const SAMPLE_PROGRESSIVE_JPEG_DATA_URL =
    'data:image/jpeg;base64,' +
    '/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/2wBDAQMDAwQDBAgEBAgQCwkLEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBD/wgARCAB4AFADAREAAhEBAxEB/8QAFgABAQEAAAAAAAAAAAAAAAAAAAMH/8QAGAEBAQEBAQAAAAAAAAAAAAAAAAQFBgj/2gAMAwEAAhADEAAAAc44z1IAAAAJ3xAAAACehEAAAAJXwgAAACd8QAAAAnfEAAAAJaEIAAAAnfEAAAAJ3xAAAACehCAAAAJ3xAAAACV8QAAAAnfCAAAAJ6EQAAAAnfEAAAAP/8QAFxABAQEBAAAAAAAAAAAAAAAAEQAwQP/aAAgBAQABBQJmZmZmZmZmZmZmZmZmZmZmZmZmZ5WZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmc2ZmZmZmZmZmZmZmZmZmZv//EABYRAQEBAAAAAAAAAAAAAAAAAAATEv/aAAgBAwEBPwHbbbbbbbbbaiiiiiiiiiiiiiiiiiiiiiiiiiiiiijbbbbbbbbbbbbbbbbbbbaiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiijbbbbbbbbbbbbbbbbbbbaiiiiiiiiiiiiiiiiiiiiiiiiiiiiij//EABURAQEAAAAAAAAAAAAAAAAAAAAT/9oACAECAQE/AZpppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppv/8QAFBABAAAAAAAAAAAAAAAAAAAAcP/aAAgBAQAGPwIW/8QAFxABAQEBAAAAAAAAAAAAAAAAEQBQQP/aAAgBAQABPyHIAAAAAABmZmZmZmZmZmZnTAAAAAAAAGZmZmZnvAAAAA//2gAMAwEAAgADAAAAEEkkkkkkkkkgAAAALbbbbUkkkkkkkkkrbbbbdtttttttttiSSSSSSSSSdttttv8A/wD/AP7bbbbQAAAAP//EABcRAQEBAQAAAAAAAAAAAAAAAHEAUED/2gAIAQMBAT8Q0v8AwAAAAP8A3Oc5znOc8kAAAAAAHOc5znOc5znOc5znPvAAAAA//8QAFBEBAAAAAAAAAAAAAAAAAAAAcP/aAAgBAgEBPxAW/wD/AP8A/wD/AP8A/wD/AP8A/wD/AP8A/wD8AP8A/wD/AP8A/8QAGRABAQEAAwAAAAAAAAAAAAAAAHERMEBQ/9oACAEBAAE/EMMMMMMMMMMeSAAAAB/8pSlKUpSlSlKUpSlK6oAABSlKUpSlKUpSlKUpSkpSlKUpSlSlKUpSlKUpSlKUpSuMAP/Z'

function decodePngPixels(buffer: Buffer) {
    const signature = buffer.subarray(0, 8).toString('hex')
    expect(signature).toBe('89504e470d0a1a0a')

    let offset = 8
    let width = 0
    let height = 0
    let colorType = 0
    const idatChunks: Array<Buffer> = []

    while (offset < buffer.length) {
        const length = buffer.readUInt32BE(offset)
        const type = buffer.subarray(offset + 4, offset + 8).toString('ascii')
        const data = buffer.subarray(offset + 8, offset + 8 + length)
        if (type === 'IHDR') {
            width = data.readUInt32BE(0)
            height = data.readUInt32BE(4)
            expect(data[8]).toBe(8)
            colorType = data[9] || 0
        } else if (type === 'IDAT') {
            idatChunks.push(data)
        } else if (type === 'IEND') {
            break
        }
        offset += length + 12
    }

    const bytesPerPixel = colorType === 6 ? 4 : colorType === 2 ? 3 : 0
    expect(bytesPerPixel).toBeGreaterThan(0)
    const inflated = inflateSync(Buffer.concat(idatChunks))
    const stride = width * bytesPerPixel
    const rows: Array<Buffer> = []
    let sourceOffset = 0

    for (let y = 0; y < height; y += 1) {
        const filter = inflated[sourceOffset]
        sourceOffset += 1
        const row = Buffer.from(inflated.subarray(sourceOffset, sourceOffset + stride))
        const prev = rows[y - 1]
        sourceOffset += stride
        for (let x = 0; x < stride; x += 1) {
            const left = x >= bytesPerPixel ? row[x - bytesPerPixel] || 0 : 0
            const up = prev?.[x] || 0
            const upLeft = x >= bytesPerPixel ? prev?.[x - bytesPerPixel] || 0 : 0
            const paeth = (() => {
                const p = left + up - upLeft
                const pa = Math.abs(p - left)
                const pb = Math.abs(p - up)
                const pc = Math.abs(p - upLeft)
                return pa <= pb && pa <= pc ? left : pb <= pc ? up : upLeft
            })()
            const add =
                filter === 1
                    ? left
                    : filter === 2
                      ? up
                      : filter === 3
                        ? Math.floor((left + up) / 2)
                        : filter === 4
                          ? paeth
                          : 0
            row[x] = (row[x] + add) & 0xff
        }
        rows.push(row)
    }

    return rows.flatMap((row) => {
        const pixels: Array<[number, number, number, number]> = []
        for (let x = 0; x < row.length; x += bytesPerPixel) {
            pixels.push([row[x] || 0, row[x + 1] || 0, row[x + 2] || 0, bytesPerPixel === 4 ? row[x + 3] || 0 : 255])
        }
        return pixels
    })
}

describe('formatPlatformTag', () => {
    test('includes platform and display name for image-tag style labels', () => {
        expect(
            formatPlatformTag({
                a_id: '1',
                platform: Platform.X,
                username: '天城サリー',
            }),
        ).toBe('X 天城サリー')
    })

    test('avoids repeating the platform name when no useful display name exists', () => {
        expect(
            formatPlatformTag({
                a_id: '2',
                platform: Platform.Website,
                username: 'Website',
            }),
        ).toBe('Website')
    })
})

describe('RenderService text-compact', () => {
    test('shortens X quote metadata without losing the quote signal', async () => {
        const service = new RenderService()
        const expectedTime = formatArticleTimeToken(1710000000)
        const result = await service.process(
            {
                id: 3,
                a_id: 'quote123',
                u_id: 'mao_asaoka227',
                username: '麻丘真央',
                created_at: 1710000000,
                content: '引用コメント',
                translation: null,
                translated_by: null,
                url: 'https://x.com/mao_asaoka227/status/quote123',
                type: 'quoted',
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
                platform: Platform.X,
            },
            {
                taskId: 'test-x-quote-compact',
                render_type: 'text-compact',
            },
        )

        const expectedClock = expectedTime.split('(')[0]
        const expectedAttributionTime = expectedTime.replace('(', '（').replace(')', '）')
        const lines = result.text.split('\n')
        expect(lines[0]).toBe(`@mao_asaoka227 ${expectedClock} X引用`)
        expect(lines[1]).toBe('')
        expect(lines.at(-2)).toBe('')
        expect(lines.at(-1)).toBe(`麻丘真央 ${expectedAttributionTime} X 引用`)
        expect(result.text).not.toContain('发布推文')
        expect(result.text).not.toContain('引用推文')
    })

    test('keeps compact metadata on one line with @uid and short timestamp', async () => {
        const service = new RenderService()
        const expectedTime = formatArticleTimeToken(1710000000)
        const result = await service.process(
            {
                id: 1,
                a_id: 'abc123',
                u_id: 'kawase_uta',
                username: '河瀬詩',
                created_at: 1710000000,
                content: 'hello world',
                translation: null,
                translated_by: null,
                url: 'https://www.instagram.com/p/abc123/',
                type: 'post',
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
                platform: Platform.Instagram,
            },
            {
                taskId: 'test-compact',
                render_type: 'text-compact',
            },
        )

        expect(expectedTime).toContain('⁹(')
        expect(formatTime(1710000000).startsWith('240310 ')).toBeTrue()
        const expectedClock = expectedTime.split('(')[0]
        const expectedAttributionTime = expectedTime.replace('(', '（').replace(')', '）')
        const lines = result.text.split('\n')
        expect(lines[0]).toBe(`@kawase_uta ${expectedClock} IG发帖`)
        expect(lines[1]).toBe('')
        expect(lines.at(-2)).toBe('')
        expect(lines.at(-1)).toBe(`河瀬詩 ${expectedAttributionTime} IG 发帖`)
        expect(result.text).toContain('IG')
        expect(result.text).toContain('河瀬詩')
        expect(result.text).toContain('@kawase_uta')
        expect(result.text).toContain('hello world')
        expect(result.text).not.toContain('    ')
        expect(result.text).not.toContain('发布帖子')
    })

    test('keeps reference separator compact while surrounding article body with blank lines', async () => {
        const service = new RenderService()
        const quoteClock = formatArticleTimeToken(1710000600).split('(')[0]
        const refClock = formatArticleTimeToken(1710000000).split('(')[0]
        const result = await service.process(
            {
                id: 4,
                a_id: 'quote-with-ref',
                u_id: 'satsuki_shiina',
                username: '椎名桜月',
                created_at: 1710000600,
                content: '引用コメント',
                translation: null,
                translated_by: null,
                url: 'https://x.com/satsuki_shiina/status/quote-with-ref',
                type: 'quoted',
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
                platform: Platform.X,
                ref: {
                    id: 5,
                    a_id: 'quoted-ref',
                    u_id: 'needygirl_anime',
                    username: 'アニメ「NEEDY GIRL OVERDOSE」公式',
                    created_at: 1710000000,
                    content: '第6話\nTurn Around and Count 2 Ten',
                    translation: null,
                    translated_by: null,
                    url: 'https://x.com/needygirl_anime/status/quoted-ref',
                    type: 'tweet',
                    ref: null,
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                    platform: Platform.X,
                },
            } as any,
            {
                taskId: 'test-compact-ref-separator',
                render_type: 'text-compact',
            },
        )

        expect(result.text).toContain(`@satsuki_shiina ${quoteClock} X引用\n\n引用コメント\n\n椎名桜月`)
        expect(result.text).toContain(`X 引用\n------------\n@needygirl_anime ${refClock} X发推`)
        expect(result.text).not.toContain('\n\n------------')
        expect(result.text).not.toContain('------------\n\n')
    })

    test('truncates overly long YouTube descriptions in compact mode', async () => {
        const service = new RenderService()
        const longDescription = '说明段落'.repeat(120)
        const result = await service.process(
            {
                id: 2,
                a_id: 'yt001',
                u_id: '227SMEJ',
                username: '22/7',
                created_at: 1710000000,
                content: `新视频标题\n\n${longDescription}`,
                translation: null,
                translated_by: null,
                url: 'https://www.youtube.com/watch?v=yt001',
                type: 'video',
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
                platform: Platform.YouTube,
            },
            {
                taskId: 'test-youtube-compact',
                render_type: 'text-compact',
            },
        )

        expect(result.text).toContain('新视频标题')
        expect(result.text).toContain('[描述过长，已截断，完整内容请打开链接查看]')
        expect(result.text.length).toBeLessThan(520)
    })
})

describe('RenderService text-card', () => {
    test('inlines downloaded media before rendering the card', () => {
        const service = new RenderService()
        const tempDir = mkdtempSync(path.join(os.tmpdir(), 'render-card-media-'))
        const mediaPath = path.join(tempDir, 'source.png')
        const sourceUrl = 'https://pbs.twimg.com/media/card-source.jpg'
        writeFileSync(mediaPath, Buffer.from(SAMPLE_PNG_DATA_URL.split(',')[1] || '', 'base64'))

        try {
            const article = {
                id: 10,
                a_id: 'x-card-inline-media',
                u_id: 'mao_asaoka227',
                username: '麻丘真央',
                created_at: 1710000000,
                content: 'おはよう',
                translation: null,
                translated_by: null,
                url: 'https://x.com/mao_asaoka227/status/card',
                type: 'tweet',
                ref: null,
                has_media: true,
                media: [
                    {
                        type: 'photo',
                        url: sourceUrl,
                    },
                ],
                extra: null,
                u_avatar: null,
                platform: Platform.X,
            } as any

            const hydrated = (service as any).hydrateArticleMediaForCard(article, [
                {
                    path: mediaPath,
                    media_type: 'photo',
                    sourceArticleId: 'x-card-inline-media',
                    sourceUrl,
                },
            ])

            expect(String(hydrated.media[0]?.url).startsWith('data:image/png;base64,')).toBe(true)
            expect(hydrated.media[0]?.width).toBe(1)
            expect(hydrated.media[0]?.height).toBe(1)
            expect(article.media[0]?.url).toBe(sourceUrl)
        } finally {
            rmSync(tempDir, { recursive: true, force: true })
        }
    })

    test('inlines downloaded media inside website raw html blocks', () => {
        const service = new RenderService()
        const tempDir = mkdtempSync(path.join(os.tmpdir(), 'render-card-html-media-'))
        const mediaPath = path.join(tempDir, 'inline.png')
        writeFileSync(mediaPath, Buffer.from(SAMPLE_PNG_DATA_URL.split(',')[1] || '', 'base64'))

        try {
            const article = {
                id: 14,
                a_id: 'fc-blog-inline-media',
                u_id: 'fc-blog',
                username: 'FC Blog',
                created_at: 1710000000,
                content: 'ブログ本文',
                translation: null,
                translated_by: null,
                url: 'https://nananiji-fc.com/s/n129/diary/detail/1',
                type: 'article',
                ref: null,
                has_media: true,
                media: [],
                extra: {
                    extra_type: 'website_meta',
                    data: {
                        site: '22/7',
                        feed: 'fc-blog',
                        raw_html: '<p>一段目</p><img src="/images/blog-photo.jpg" alt="photo"><p>二段目</p>',
                    },
                    media: [
                        {
                            type: 'photo',
                            url: 'https://nananiji-fc.com/images/blog-photo.jpg',
                        },
                    ],
                },
                u_avatar: null,
                platform: Platform.Website,
            } as any

            const hydrated = (service as any).hydrateArticleMediaForCard(article, [
                {
                    path: mediaPath,
                    media_type: 'photo',
                    sourceArticleId: 'fc-blog-inline-media',
                    sourceUrl: 'https://nananiji-fc.com/images/blog-photo.jpg',
                },
            ])

            expect(String(hydrated.extra.data.raw_html).includes('src="data:image/png;base64,')).toBe(true)
            expect(article.extra.data.raw_html).toContain('src="/images/blog-photo.jpg"')
        } finally {
            rmSync(tempDir, { recursive: true, force: true })
        }
    })

    test('renders progressive jpeg media inside the card instead of a gray tile', async () => {
        const service = new RenderService()
        const result = await service.process(
            {
                id: 15,
                a_id: 'progressive-jpeg-card',
                u_id: 'nao_aikawa227',
                username: '22/7 相川奈央',
                created_at: 1710000000,
                content: 'おはよ〜🍓',
                translation: null,
                translated_by: null,
                url: 'https://x.com/nao_aikawa227/status/progressive',
                type: 'tweet',
                ref: null,
                has_media: true,
                media: [
                    {
                        type: 'photo',
                        url: SAMPLE_PROGRESSIVE_JPEG_DATA_URL,
                    },
                ],
                extra: null,
                u_avatar: null,
                platform: Platform.X,
            } as any,
            {
                taskId: 'test-progressive-jpeg-card',
                render_type: 'text-card',
            },
        )

        expect(result.cardMediaFiles).toHaveLength(1)
        const pixels = decodePngPixels(readFileSync(result.cardMediaFiles[0]!.path))
        const saturatedPixels = pixels.filter(([r, g, b, a]) => a > 0 && Math.max(r, g, b) - Math.min(r, g, b) > 60)
        expect(saturatedPixels.length).toBeGreaterThan(10000)

        service.cleanup(result.mediaFiles)
    })

    test('appends a rendered card after the original media', async () => {
        const service = new RenderService()
        const result = await service.process(
            {
                id: 11,
                a_id: 'ig-text-card',
                u_id: 'nananijigram22_7',
                username: '22/7',
                created_at: 1710000000,
                content: '短文标题\n\n这里是正文',
                translation: null,
                translated_by: null,
                url: 'https://www.instagram.com/p/card/',
                type: 'post',
                ref: null,
                has_media: true,
                media: [
                    {
                        type: 'photo',
                        url: SAMPLE_PNG_DATA_URL,
                    },
                ],
                extra: null,
                u_avatar: null,
                platform: Platform.Instagram,
            } as any,
            {
                taskId: 'test-text-card',
                render_type: 'text-card',
                mediaConfig: {
                    type: 'no-storage',
                    use: {
                        tool: MediaToolEnum.DEFAULT,
                    },
                },
            },
        )

        expect(result.text).toContain('短文标题')
        expect(result.cardMediaFiles).toHaveLength(1)
        expect(result.mediaFiles).toHaveLength(2)
        expect(result.mediaFiles[1]?.path).toBe(result.cardMediaFiles[0]?.path)

        service.cleanup(result.mediaFiles)
    })

    test('uses only the headline when article text is too long', async () => {
        const service = new RenderService()
        const result = await service.process(
            {
                id: 12,
                a_id: 'web-text-card-long',
                u_id: 'live-report',
                username: 'LIVE REPORT',
                created_at: 1710000000,
                content: `需要保留的标题\n\n${'很长的正文'.repeat(260)}`,
                translation: null,
                translated_by: null,
                url: 'https://www.227-official.com/live/report',
                type: 'article',
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
                platform: Platform.Website,
            } as any,
            {
                taskId: 'test-text-card-long',
                render_type: 'text-card',
            },
        )

        expect(result.text).toBe('需要保留的标题')
        expect(result.cardMediaFiles).toHaveLength(1)

        service.cleanup(result.mediaFiles)
    })

    test('renders a card when text starts with emoji', async () => {
        const service = new RenderService()
        const result = await service.process(
            {
                id: 13,
                a_id: 'emoji-leading-card',
                u_id: 'sally_amaki',
                username: '天城サリー',
                created_at: 1710000000,
                content: '💙お知らせです\n\nナナニジのライブがあります',
                translation: null,
                translated_by: null,
                url: 'https://x.com/sally/status/emoji',
                type: 'tweet',
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
                platform: Platform.X,
            } as any,
            {
                taskId: 'test-emoji-leading-card',
                render_type: 'text-card',
            },
        )

        expect(result.text).toContain('💙お知らせです')
        expect(result.cardMediaFiles).toHaveLength(1)

        service.cleanup(result.mediaFiles)
    })

    test('renders decorative fallback glyphs in text cards', async () => {
        const service = new RenderService()
        const result = await service.process(
            {
                id: 16,
                a_id: 'decorative-fallback-card',
                u_id: 'decorative_test',
                username: '໒꒱· ﾟ',
                created_at: 1710000000,
                content: '໒꒱· ﾟ ⌁ ⟡ ✦ ♡ ᜊ ᓚᘏᗢ',
                translation: null,
                translated_by: null,
                url: 'https://x.com/decorative/status/fallback',
                type: 'tweet',
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
                platform: Platform.X,
            } as any,
            {
                taskId: 'test-decorative-fallback-card',
                render_type: 'text-card',
            },
        )

        expect(result.cardMediaFiles).toHaveLength(1)

        service.cleanup(result.mediaFiles)
    })

    test('renders rino_mochizuki decorative selector-heavy text cards', async () => {
        const service = new RenderService()
        const result = await service.process(
            {
                id: 17,
                a_id: 'rino-selector-heavy-card',
                u_id: 'rino_mochizuki',
                username: '♡望月りの♡【22/7】໒꒱· ﾟ',
                created_at: 1778632285,
                content: 'おはりのち︎︎︎︎❤︎\n\nレトロな喫茶店って良いよね☕*°\n୨୧ #エスターバニー ちゃんと🐇⸒⸒ ',
                translation: null,
                translated_by: null,
                url: 'https://x.com/rino_mochizuki/status/2054358821003550783',
                type: 'tweet',
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
                platform: Platform.X,
            } as any,
            {
                taskId: 'test-rino-selector-heavy-card',
                render_type: 'text-card',
            },
        )

        expect(result.cardMediaFiles).toHaveLength(1)

        service.cleanup(result.mediaFiles)
    })
})

describe('RenderService media deduplication', () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save

    function buildMediaArticle(a_id: string) {
        return {
            id: 100,
            a_id,
            u_id: 'nananijigram22_7',
            username: '22/7',
            created_at: 1710000000,
            content: null,
            translation: null,
            translated_by: null,
            url: `https://www.instagram.com/stories/${a_id}/`,
            type: 'story',
            ref: null,
            has_media: true,
            media: [
                {
                    type: 'photo' as const,
                    url: SAMPLE_PNG_DATA_URL,
                },
            ],
            extra: null,
            u_avatar: null,
            platform: Platform.Instagram,
        }
    }

    test('keeps media for repeated processing of the same article id', async () => {
        const hashStore = new Map<string, { platform: string; hash: string; a_id: string }>()
        DB.MediaHash.checkExist = async (platform: string, hash: string) => hashStore.get(`${platform}:${hash}`) as any
        DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
            const value = { platform, hash, a_id }
            hashStore.set(`${platform}:${hash}`, value)
            return value as any
        }

        const service = new RenderService()
        const article = buildMediaArticle('story-same')
        const config = {
            taskId: 'test-story-same',
            render_type: 'text-compact',
            mediaConfig: {
                type: 'no-storage' as const,
                use: {
                    tool: MediaToolEnum.DEFAULT,
                },
            },
            deduplication: true,
        }

        const first = await service.process(article as any, config)
        const second = await service.process(article as any, { ...config, taskId: 'test-story-same-2' })

        expect(first.mediaFiles).toHaveLength(1)
        expect(second.mediaFiles).toHaveLength(1)

        service.cleanup([...first.mediaFiles, ...second.mediaFiles])
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    })

    test('still skips media reused by a different article id', async () => {
        const hashStore = new Map<string, { platform: string; hash: string; a_id: string }>()
        DB.MediaHash.checkExist = async (platform: string, hash: string) => hashStore.get(`${platform}:${hash}`) as any
        DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
            const value = { platform, hash, a_id }
            hashStore.set(`${platform}:${hash}`, value)
            return value as any
        }

        const service = new RenderService()
        const config = {
            taskId: 'test-story-cross',
            render_type: 'text-compact',
            mediaConfig: {
                type: 'no-storage' as const,
                use: {
                    tool: MediaToolEnum.DEFAULT,
                },
            },
            deduplication: true,
        }

        const first = await service.process(buildMediaArticle('story-a') as any, config)
        const second = await service.process(buildMediaArticle('story-b') as any, {
            ...config,
            taskId: 'test-story-cross-2',
        })

        expect(first.mediaFiles).toHaveLength(1)
        expect(second.mediaFiles).toHaveLength(0)
        expect(second.shouldSkipSend).toBe(true)
        expect(second.skipReason).toContain('Duplicate media hash matched')

        service.cleanup(first.mediaFiles)
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    })

    test('skips the whole article when an exact image was already sent from another platform', async () => {
        const hashStore = new Map<string, { platform: string; hash: string; a_id: string }>()
        DB.MediaHash.checkExist = async (platform: string, hash: string) => hashStore.get(`${platform}:${hash}`) as any
        DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
            const value = { platform, hash, a_id }
            hashStore.set(`${platform}:${hash}`, value)
            return value as any
        }

        const service = new RenderService()
        const config = {
            taskId: 'test-cross-platform-photo',
            render_type: 'text-compact',
            mediaConfig: {
                type: 'no-storage' as const,
                use: {
                    tool: MediaToolEnum.DEFAULT,
                },
            },
            deduplication: true,
        }

        const first = await service.process(buildMediaArticle('ig-photo-a') as any, config)
        const second = await service.process(
            {
                ...buildMediaArticle('website-photo-b'),
                platform: Platform.Website,
                type: 'article',
                u_id: '22/7:photo',
                username: '22/7 Photo',
                url: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery/detail/1',
            } as any,
            {
                ...config,
                taskId: 'test-cross-platform-photo-2',
            },
        )

        expect(first.mediaFiles).toHaveLength(1)
        expect(second.mediaFiles).toHaveLength(0)
        expect(second.shouldSkipSend).toBe(true)
        expect(second.skipReason).toContain('Cross-platform exact media duplicate matched')

        service.cleanup(first.mediaFiles)
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    })

    test('does not duplicate media when extra.media mirrors the primary media list', async () => {
        const service = new RenderService()
        const result = await service.process(
            {
                id: 200,
                a_id: 'website-photo-album',
                u_id: '22/7:photo',
                username: '22/7 Photo',
                created_at: 1710000000,
                content: '【春のかおり】\n\n【北原実咲】\n窓開けてみんなでお昼寝♪',
                translation: null,
                translated_by: null,
                url: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga',
                type: 'article',
                ref: null,
                has_media: true,
                media: [
                    {
                        type: 'photo' as const,
                        url: SAMPLE_PNG_DATA_URL,
                    },
                ],
                extra: {
                    data: {
                        site: '22/7',
                        host: 'nanabunnonijyuuni-mobile.com',
                        feed: 'photo',
                    },
                    media: [
                        {
                            type: 'photo' as const,
                            url: SAMPLE_PNG_DATA_URL,
                        },
                    ],
                    extra_type: 'website_meta',
                },
                u_avatar: null,
                platform: Platform.Website,
            },
            {
                taskId: 'test-website-photo-no-dup',
                render_type: 'text-compact',
                mediaConfig: {
                    type: 'no-storage' as const,
                    use: {
                        tool: MediaToolEnum.DEFAULT,
                    },
                },
            },
        )

        expect(result.mediaFiles).toHaveLength(1)
        service.cleanup(result.mediaFiles)
    })

    test('keeps source article metadata on downloaded media across ref chains', async () => {
        const service = new RenderService()
        const result = await service.process(
            {
                id: 300,
                a_id: 'root-post',
                u_id: 'member_a',
                username: 'Member A',
                created_at: 1710000000,
                content: 'root body',
                translation: null,
                translated_by: null,
                url: 'https://x.com/member_a/status/root-post',
                type: 'tweet',
                ref: {
                    id: 301,
                    a_id: 'ref-post',
                    u_id: 'outsider_user',
                    username: 'Outsider',
                    created_at: 1710000001,
                    content: 'ref body',
                    translation: null,
                    translated_by: null,
                    url: 'https://x.com/outsider_user/status/ref-post',
                    type: 'tweet',
                    ref: null,
                    has_media: true,
                    media: [
                        {
                            type: 'photo' as const,
                            url: SAMPLE_PNG_DATA_URL,
                        },
                    ],
                    extra: null,
                    u_avatar: null,
                    platform: Platform.X,
                } as any,
                has_media: true,
                media: [
                    {
                        type: 'photo' as const,
                        url: SAMPLE_PNG_DATA_URL,
                    },
                ],
                extra: null,
                u_avatar: null,
                platform: Platform.X,
            },
            {
                taskId: 'test-source-metadata',
                render_type: 'text-compact',
                mediaConfig: {
                    type: 'no-storage' as const,
                    use: {
                        tool: MediaToolEnum.DEFAULT,
                    },
                },
                deduplication: false,
            },
        )

        expect(result.originalMediaFiles.map((item) => item.sourceArticleId)).toEqual(['root-post', 'ref-post'])
        expect(result.originalMediaFiles.map((item) => item.sourceUserId)).toEqual(['member_a', 'outsider_user'])
        service.cleanup(result.mediaFiles)
    })
})

describe('RenderService img-tag ordering', () => {
    test('prepends the rendered card before original media files', async () => {
        const service = new RenderService()
        const pngBase64 = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9s1OtS8AAAAASUVORK5CYII='
        ;(service as any).ArticleConverter = {
            articleToImg: async () => Buffer.from(pngBase64, 'base64'),
        }

        const result = await service.process(
            {
                id: 300,
                a_id: 'website-photo-1',
                u_id: '22/7:photo',
                username: '22/7 Photo',
                created_at: 1710000000,
                content: '【春のかおり】\n\n【北原実咲】\n窓開けてみんなでお昼寝♪',
                translation: null,
                translated_by: null,
                url: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga',
                type: 'article',
                ref: null,
                has_media: true,
                media: [
                    {
                        type: 'photo' as const,
                        url: `data:image/png;base64,${pngBase64}`,
                    },
                ],
                extra: {
                    data: {
                        site: '22/7',
                        host: 'nanabunnonijyuuni-mobile.com',
                        feed: 'photo',
                    },
                    extra_type: 'website_meta',
                },
                u_avatar: null,
                platform: Platform.Website,
            },
            {
                taskId: 'test-img-tag-order',
                render_type: 'img-tag',
                mediaConfig: {
                    type: 'no-storage' as const,
                    use: {
                        tool: MediaToolEnum.DEFAULT,
                    },
                },
            },
        )

        expect(result.mediaFiles).toHaveLength(2)
        expect(result.cardMediaFiles).toHaveLength(1)
        expect(result.originalMediaFiles).toHaveLength(1)
        expect(result.mediaFiles[0]?.path).toContain('rendered.png')
        expect(result.mediaFiles[1]?.path).not.toContain('rendered.png')

        service.cleanup(result.mediaFiles)
    })
})
