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
    test('keeps platform and display name but omits u_id', async () => {
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

        expect(result.text).toContain('Instagram')
        expect(result.text).toContain('河瀬詩')
        expect(result.text).toContain('hello world')
        expect(result.text).not.toContain('kawase_uta')
    })
})
