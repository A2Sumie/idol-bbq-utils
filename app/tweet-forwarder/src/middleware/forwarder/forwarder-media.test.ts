import { expect, test } from 'bun:test'
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
