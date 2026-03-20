import { describe, expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { formatPlatformTag, RenderService } from './render-service'
import { fileURLToPath } from 'url'
import DB from '@/db'
import { MediaToolEnum } from '@/types/media'

process.env.FONTS_DIR = fileURLToPath(new URL('../../../../assets/fonts', import.meta.url))

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

describe('RenderService media deduplication', () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    const dataUrl =
        'data:image/png;base64,' +
        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9s1OtS8AAAAASUVORK5CYII='

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
                    url: dataUrl,
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
                        url: dataUrl,
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
                            url: dataUrl,
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
        expect(result.mediaFiles[0]?.path).toContain('rendered.png')
        expect(result.mediaFiles[1]?.path).not.toContain('rendered.png')

        service.cleanup(result.mediaFiles)
    })
})
