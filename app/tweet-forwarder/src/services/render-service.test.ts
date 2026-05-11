import { describe, expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { formatPlatformTag, RenderService } from './render-service'
import { fileURLToPath } from 'url'
import DB from '@/db'
import { MediaToolEnum } from '@/types/media'
import { mkdtempSync, rmSync, writeFileSync } from 'fs'
import os from 'os'
import path from 'path'

process.env.FONTS_DIR = fileURLToPath(new URL('../../../../assets/fonts', import.meta.url))

const SAMPLE_PNG_DATA_URL =
    'data:image/png;base64,' +
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9s1OtS8AAAAASUVORK5CYII='

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
    test('keeps display name and source label but omits u_id', async () => {
        const service = new RenderService()
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

        expect(result.text.split('\n')[0]).toBe('河瀬詩    来自Instagram')
        expect(result.text).toContain('Instagram')
        expect(result.text).toContain('河瀬詩')
        expect(result.text).toContain('hello world')
        expect(result.text).not.toContain('kawase_uta')
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
