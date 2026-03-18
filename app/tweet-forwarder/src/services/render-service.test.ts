import { describe, expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { formatPlatformTag } from './render-service'

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
