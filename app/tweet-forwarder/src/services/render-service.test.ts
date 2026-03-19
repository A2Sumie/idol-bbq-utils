import { describe, expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { formatPlatformTag, RenderService } from './render-service'

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
