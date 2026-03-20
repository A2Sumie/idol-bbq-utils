import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { QQForwarder } from './qq'
import { TgForwarder } from './telegram'

test('QQForwarder treats video thumbnails as images', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
        } as any,
        'qq-test',
    )
    const payloads: any[] = []
    ;(forwarder as any).sendWithPayload = async (segments: any) => {
        payloads.push(segments)
        return { ok: true }
    }

    await (forwarder as any).realSend(['shorts update'], {
        media: [
            {
                media_type: 'video_thumbnail',
                path: '/tmp/shorts-cover.jpg',
            },
            {
                media_type: 'video',
                path: '/tmp/shorts.mp4',
            },
        ],
    })

    expect(payloads).toEqual([
        [
            {
                type: 'text',
                data: {
                    text: 'shorts update',
                },
            },
            {
                type: 'image',
                data: {
                    file: 'file:///tmp/shorts-cover.jpg',
                },
            },
        ],
        [
            {
                type: 'video',
                data: {
                    file: 'file:///tmp/shorts.mp4',
                },
            },
        ],
    ])
})

test('TgForwarder treats video thumbnails as photos in media groups', async () => {
    const forwarder = new TgForwarder(
        {
            chat_id: '-100123',
            token: 'telegram-bot-token',
        } as any,
        'tg-test',
    )
    const calls: any[] = []
    ;(forwarder as any).bot = {
        telegram: {
            sendMediaGroup: async (...args: any[]) => {
                calls.push(args)
            },
            sendMessage: async () => undefined,
        },
    }

    await (forwarder as any).realSend(['shorts update'], {
        media: [
            {
                media_type: 'video_thumbnail',
                path: '/tmp/shorts-cover.jpg',
            },
            {
                media_type: 'video',
                path: '/tmp/shorts.mp4',
            },
        ],
    })

    expect(calls).toHaveLength(1)
    expect(calls[0][0]).toBe('-100123')
    expect(calls[0][1].map((item: any) => item.type)).toEqual(['photo', 'video'])
    expect(calls[0][1][0].caption).toBe('shorts update')
    expect(calls[0][1][1].caption).toBeUndefined()
})

test('QQForwarder batches image-like units until the configured threshold is reached', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
            media_batch_threshold: 6,
        } as any,
        'qq-batch-test',
    )
    ;(forwarder as any).minInterval = 0

    const payloads: any[] = []
    ;(forwarder as any).sendWithPayload = async (segments: any) => {
        payloads.push(segments)
        return { ok: true }
    }

    for (const index of [1, 2, 3]) {
        await forwarder.send(`batch text ${index}`, {
            media: [
                {
                    media_type: 'photo',
                    path: `/tmp/batch-${index}.jpg`,
                },
            ],
            article: {
                platform: Platform.X,
            } as any,
        })
    }

    expect(payloads).toHaveLength(1)
    expect(payloads[0].filter((segment: any) => segment.type === 'image')).toHaveLength(3)
    expect(payloads[0][0]?.data?.text).toContain('batch text 1')
    expect(payloads[0][0]?.data?.text).toContain('batch text 3')
})

test('QQForwarder sends breakout image posts immediately without flushing the pending batch', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
            media_batch_threshold: 6,
        } as any,
        'qq-breakout-test',
    )
    ;(forwarder as any).minInterval = 0

    const payloads: any[] = []
    ;(forwarder as any).sendWithPayload = async (segments: any) => {
        payloads.push(segments)
        return { ok: true }
    }

    await forwarder.send('queued one', {
        media: [{ media_type: 'photo', path: '/tmp/q1.jpg' }],
        article: {
            platform: Platform.X,
        } as any,
    })
    await forwarder.send('breakout now', {
        media: [
            { media_type: 'photo', path: '/tmp/breakout-1.jpg' },
            { media_type: 'photo', path: '/tmp/breakout-2.jpg' },
            { media_type: 'photo', path: '/tmp/breakout-3.jpg' },
        ],
        article: {
            platform: Platform.X,
        } as any,
    })
    await forwarder.send('queued two', {
        media: [{ media_type: 'photo', path: '/tmp/q2.jpg' }],
        article: {
            platform: Platform.X,
        } as any,
    })
    await forwarder.send('queued three', {
        media: [{ media_type: 'photo', path: '/tmp/q3.jpg' }],
        article: {
            platform: Platform.X,
        } as any,
    })

    expect(payloads).toHaveLength(2)
    expect(payloads[0].filter((segment: any) => segment.type === 'image')).toHaveLength(3)
    expect(payloads[0][0]?.data?.text).toContain('breakout now')
    expect(payloads[1].filter((segment: any) => segment.type === 'image')).toHaveLength(3)
    expect(payloads[1][0]?.data?.text).toContain('queued one')
    expect(payloads[1][0]?.data?.text).toContain('queued three')
})

test('QQForwarder can send rendered cards separately from original media', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
            media_batch_threshold: 3,
            separate_card_media: true,
        } as any,
        'qq-split-card-test',
    )
    ;(forwarder as any).minInterval = 0

    const payloads: any[] = []
    ;(forwarder as any).sendWithPayload = async (segments: any) => {
        payloads.push(segments)
        return { ok: true }
    }

    await forwarder.send('card caption', {
        media: [
            { media_type: 'photo', path: '/tmp/card.jpg' },
            { media_type: 'photo', path: '/tmp/photo-1.jpg' },
            { media_type: 'photo', path: '/tmp/photo-2.jpg' },
        ],
        cardMedia: [{ media_type: 'photo', path: '/tmp/card.jpg' }],
        contentMedia: [
            { media_type: 'photo', path: '/tmp/photo-1.jpg' },
            { media_type: 'photo', path: '/tmp/photo-2.jpg' },
        ],
        article: {
            platform: Platform.X,
        } as any,
    })

    expect(payloads).toHaveLength(2)
    expect(payloads[0].filter((segment: any) => segment.type === 'image')).toHaveLength(1)
    expect(payloads[0][0]?.data?.text).toContain('card caption')
    expect(payloads[1].filter((segment: any) => segment.type === 'image')).toHaveLength(2)
    expect(payloads[1].some((segment: any) => segment.type === 'text')).toBe(false)
})

test('QQForwarder does not batch non-X articles even when the target has a media batch threshold', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
            media_batch_threshold: 6,
        } as any,
        'qq-non-x-test',
    )
    ;(forwarder as any).minInterval = 0

    const payloads: any[] = []
    ;(forwarder as any).sendWithPayload = async (segments: any) => {
        payloads.push(segments)
        return { ok: true }
    }

    await forwarder.send('website direct 1', {
        media: [{ media_type: 'photo', path: '/tmp/non-x-1.jpg' }],
        article: {
            platform: Platform.Website,
        } as any,
    })
    await forwarder.send('website direct 2', {
        media: [{ media_type: 'photo', path: '/tmp/non-x-2.jpg' }],
        article: {
            platform: Platform.Website,
        } as any,
    })

    expect(payloads).toHaveLength(2)
})
