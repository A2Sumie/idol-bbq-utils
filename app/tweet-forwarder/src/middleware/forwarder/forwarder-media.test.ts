import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { BiliForwarder } from './bilibili'
import { QQForwarder } from './qq'
import { PartialForwarderSendError } from './base'
import { existsSync } from 'node:fs'
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { execFileSync } from 'node:child_process'

async function writeLargePpm(filePath: string, width = 900, height = 900) {
    const header = Buffer.from(`P6\n${width} ${height}\n255\n`)
    const pixels = Buffer.alloc(width * height * 3, 255)
    await writeFile(filePath, Buffer.concat([header, pixels]))
}

function probeImageSize(filePath: string) {
    const output = execFileSync(
        process.env.FFPROBE_PATH || 'ffprobe',
        [
            '-v',
            'error',
            '-select_streams',
            'v:0',
            '-show_entries',
            'stream=width,height',
            '-of',
            'csv=s=x:p=0',
            filePath,
        ],
        { encoding: 'utf8' },
    ).trim()
    const [width, height] = output.split('x').map((part) => Number(part))
    return { width, height }
}

test('QQForwarder does not send video thumbnails as standalone images', async () => {
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

test('QQForwarder compresses oversized image attachments before building image segments', async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'qq-image-compress-'))
    const sourcePath = path.join(tempRoot, 'oversized.ppm')
    await writeLargePpm(sourcePath)
    const maxImageBytes = 60_000

    try {
        const forwarder = new QQForwarder(
            {
                group_id: '123',
                url: 'http://127.0.0.1:3001',
                token: '',
                max_image_bytes: maxImageBytes,
            } as any,
            'qq-image-compress-test',
        )
        ;(forwarder as any).minInterval = 0

        let sentPath = ''
        ;(forwarder as any).sendWithPayload = async (segments: any) => {
            const imageSegment = segments.find((segment: any) => segment.type === 'image')
            sentPath = String(imageSegment?.data?.file || '').replace(/^file:\/\//, '')
            expect(sentPath).not.toBe(sourcePath)
            expect(sentPath).toContain('forwarder-compressed')
            expect((await stat(sentPath)).size).toBeLessThanOrEqual(maxImageBytes)
            return { ok: true }
        }

        await (forwarder as any).realSend(['oversized image'], {
            media: [{ media_type: 'photo', path: sourcePath }],
        })

        expect(sentPath).toBeTruthy()
        expect(existsSync(sentPath)).toBe(false)
    } finally {
        await rm(tempRoot, { recursive: true, force: true })
    }
})

test('QQForwarder preserves tall rendered card width when compressing oversized images', async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'qq-tall-card-compress-'))
    const sourcePath = path.join(tempRoot, 'tall-card.ppm')
    await writeLargePpm(sourcePath, 1200, 6000)
    const maxImageBytes = 4_000_000

    try {
        const forwarder = new QQForwarder(
            {
                group_id: '123',
                url: 'http://127.0.0.1:3001',
                token: '',
                max_image_bytes: maxImageBytes,
            } as any,
            'qq-tall-card-compress-test',
        )
        ;(forwarder as any).minInterval = 0

        let sentPath = ''
        ;(forwarder as any).sendWithPayload = async (segments: any) => {
            const imageSegment = segments.find((segment: any) => segment.type === 'image')
            sentPath = String(imageSegment?.data?.file || '').replace(/^file:\/\//, '')
            const uploadedSize = probeImageSize(sentPath)
            expect(uploadedSize.width).toBeGreaterThanOrEqual(1000)
            expect(uploadedSize.height).toBeGreaterThan(2400)
            expect((await stat(sentPath)).size).toBeLessThanOrEqual(maxImageBytes)
            return { ok: true }
        }

        await (forwarder as any).realSend(['tall card'], {
            media: [{ media_type: 'photo', path: sourcePath }],
        })

        expect(sentPath).toBeTruthy()
        expect(existsSync(sentPath)).toBe(false)
    } finally {
        await rm(tempRoot, { recursive: true, force: true })
    }
})

test('BiliForwarder compresses oversized dynamic images before upload', async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'bili-image-compress-'))
    const sourcePath = path.join(tempRoot, 'oversized.ppm')
    await writeLargePpm(sourcePath)
    const maxImageBytes = 60_000

    try {
        const forwarder = new BiliForwarder(
            {
                bili_jct: 'csrf-token',
                sessdata: 'sess-token',
                media_check_level: 'strict',
                max_image_bytes: maxImageBytes,
            } as any,
            'bili-image-compress-test',
        )
        ;(forwarder as any).minInterval = 0

        let uploadedPath = ''
        ;(forwarder as any).uploadPhoto = async (filePath: string) => {
            uploadedPath = filePath
            expect(uploadedPath).not.toBe(sourcePath)
            expect(uploadedPath).toContain('forwarder-compressed')
            const uploadedSize = (await stat(uploadedPath)).size
            expect(uploadedSize).toBeLessThanOrEqual(maxImageBytes)
            return {
                image_url: 'https://i0.hdslb.com/bfs/test/compressed.jpg',
                image_width: 900,
                image_height: 900,
                img_size: uploadedSize,
            }
        }
        ;(forwarder as any).sendTextWithPhotos = async (_text: string, pics: any[]) => {
            expect(pics).toHaveLength(1)
            expect(pics[0]?.img_size).toBeLessThanOrEqual(maxImageBytes)
            return { data: { code: 0, message: 'ok', data: { dyn_id_str: 'compressed-dynamic' } } }
        }
        ;(forwarder as any).fetchDynamicDetail = async () => {
            return {
                data: {
                    code: 0,
                    data: {
                        item: {
                            modules: {
                                module_dynamic: {
                                    major: {
                                        type: 'MAJOR_TYPE_DRAW',
                                        draw: {
                                            items: [{ src: 'https://i0.hdslb.com/bfs/test/compressed.jpg' }],
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            }
        }

        await (forwarder as any).realSend(['dynamic text'], {
            media: [{ media_type: 'photo', path: sourcePath }],
        })

        expect(uploadedPath).toBeTruthy()
        expect(existsSync(uploadedPath)).toBe(false)
    } finally {
        await rm(tempRoot, { recursive: true, force: true })
    }
})

test('BiliForwarder rejects uploaded images without size metadata in strict mode', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            media_check_level: 'strict',
            require_media: true,
        } as any,
        'bili-image-metadata-test',
    )
    ;(forwarder as any).minInterval = 0

    let sent = false
    ;(forwarder as any).uploadPhoto = async () => ({
        image_url: 'https://i0.hdslb.com/bfs/test/missing-size.jpg',
        image_width: 900,
        image_height: 1200,
    })
    ;(forwarder as any).sendTextWithPhotos = async () => {
        sent = true
        return { data: { code: 0, message: 'ok' } }
    }

    await expect(
        (forwarder as any).realSend(['dynamic text'], {
            media: [{ media_type: 'photo', path: '/tmp/source.jpg' }],
        }),
    ).rejects.toThrow('No photos uploaded')
    expect(sent).toBeFalse()
})

test('BiliForwarder suppresses pure video-thumbnail dynamics when visible media is required', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            require_media: true,
        } as any,
        'bili-thumbnail-only-test',
    )
    ;(forwarder as any).minInterval = 0

    let uploaded = false
    let sent = false
    ;(forwarder as any).uploadPhoto = async () => {
        uploaded = true
    }
    ;(forwarder as any).sendTextWithPhotos = async () => {
        sent = true
        return { data: { code: 0, message: 'ok' } }
    }

    const result = await (forwarder as any).realSend(['video update'], {
        media: [{ media_type: 'video_thumbnail', path: '/tmp/video-cover.jpg' }],
    })

    expect(result).toEqual([{ ok: true, mode: 'dynamic_media_required_suppressed' }])
    expect(uploaded).toBeFalse()
    expect(sent).toBeFalse()
})

test('BiliForwarder verifies Bilibili photo dynamic detail after posting', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            media_check_level: 'strict',
            require_media: true,
        } as any,
        'bili-photo-detail-test',
    )
    ;(forwarder as any).minInterval = 0

    const detailIds: string[] = []
    ;(forwarder as any).uploadPhoto = async () => ({
        image_url: 'https://i0.hdslb.com/bfs/test/visible.jpg',
        image_width: 900,
        image_height: 1200,
        img_size: 188,
    })
    ;(forwarder as any).sendTextWithPhotos = async () => ({
        data: {
            code: 0,
            message: 'OK',
            data: {
                dyn_id_str: 'dynamic-with-visible-photo',
            },
        },
    })
    ;(forwarder as any).fetchDynamicDetail = async (dynamicId: string) => {
        detailIds.push(dynamicId)
        return {
            data: {
                code: 0,
                data: {
                    item: {
                        modules: {
                            module_dynamic: {
                                major: {
                                    type: 'MAJOR_TYPE_DRAW',
                                    draw: {
                                        items: [{ src: 'https://i0.hdslb.com/bfs/test/visible.jpg' }],
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
    }

    await (forwarder as any).realSend(['dynamic text'], {
        media: [{ media_type: 'photo', path: '/tmp/source.jpg' }],
    })

    expect(detailIds).toEqual(['dynamic-with-visible-photo'])
})

test('BiliForwarder treats code-zero photo dynamics without visible detail media as partial failure', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            media_check_level: 'strict',
            require_media: true,
        } as any,
        'bili-photo-detail-failure-test',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).dynamicDetailValidationRetries = 0

    ;(forwarder as any).uploadPhoto = async () => ({
        image_url: 'https://i0.hdslb.com/bfs/test/missing-major.jpg',
        image_width: 900,
        image_height: 1200,
        img_size: 188,
    })
    ;(forwarder as any).sendTextWithPhotos = async () => ({
        data: {
            code: 0,
            message: 'OK',
            data: {
                dyn_id_str: 'dynamic-without-visible-photo',
            },
        },
    })
    ;(forwarder as any).fetchDynamicDetail = async () => ({
        data: {
            code: 0,
            data: {
                item: {
                    modules: {
                        module_dynamic: {
                            major: null,
                        },
                    },
                },
            },
        },
    })

    let caught: unknown
    try {
        await (forwarder as any).realSend(['dynamic text'], {
            media: [{ media_type: 'photo', path: '/tmp/source.jpg' }],
        })
    } catch (error) {
        caught = error
    }

    expect(caught).toBeInstanceOf(PartialForwarderSendError)
    expect((caught as Error).message).toContain('post-validation failed')
    expect((caught as PartialForwarderSendError).partialResults).toHaveLength(1)
})

test('QQForwarder keeps long text as a single payload instead of chunking', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
        } as any,
        'qq-text-limit-test',
    )
    ;(forwarder as any).minInterval = 0

    const payloads: any[] = []
    ;(forwarder as any).sendWithPayload = async (segments: any) => {
        payloads.push(segments)
        return { ok: true }
    }

    await forwarder.send(`保留标题\n\n${'很长的正文'.repeat(260)}`)

    expect(payloads).toHaveLength(1)
    expect(payloads[0]).toHaveLength(1)
    expect(payloads[0][0]?.data?.text).toBe('保留标题')
})

test('QQForwarder dry-run send mode blocks the actual provider exit', async () => {
    const originalMode = process.env.IDOL_BBQ_OUTBOUND_SEND_MODE
    process.env.IDOL_BBQ_OUTBOUND_SEND_MODE = 'blocked'
    try {
        const forwarder = new QQForwarder(
            {
                group_id: '123',
                url: 'http://127.0.0.1:3001',
                token: '',
                media_batch_threshold: 6,
            } as any,
            'qq-dry-run-test',
        )
        ;(forwarder as any).minInterval = 0

        const payloads: any[] = []
        ;(forwarder as any).sendWithPayload = async (segments: any) => {
            payloads.push(segments)
            return { ok: true }
        }

        const result = await forwarder.send('blocked payload', {
            media: [{ media_type: 'photo', path: '/tmp/blocked.jpg' }],
            outboundKey: 'article:qq-dry-run-test:1:blocked',
        })

        expect(result.status).toBe('dry_run')
        expect(result.status === 'dry_run' ? result.details.outbound_key : '').toBe('article:qq-dry-run-test:1:blocked')
        expect(forwarder.drainPendingMediaBatches()).toHaveLength(0)
        expect(payloads).toHaveLength(0)
    } finally {
        if (originalMode === undefined) {
            delete process.env.IDOL_BBQ_OUTBOUND_SEND_MODE
        } else {
            process.env.IDOL_BBQ_OUTBOUND_SEND_MODE = originalMode
        }
    }
})

test('QQForwarder capture send mode records a virtual receiver payload without provider exit', async () => {
    const originalMode = process.env.IDOL_BBQ_OUTBOUND_SEND_MODE
    const originalCaptureFile = process.env.IDOL_BBQ_OUTBOUND_CAPTURE_FILE
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'qq-capture-receiver-'))
    const captureFile = path.join(tempRoot, 'capture.jsonl')
    process.env.IDOL_BBQ_OUTBOUND_SEND_MODE = 'capture'
    process.env.IDOL_BBQ_OUTBOUND_CAPTURE_FILE = captureFile
    try {
        const forwarder = new QQForwarder(
            {
                group_id: '123',
                url: 'http://127.0.0.1:3001',
                token: '',
                media_batch_threshold: 6,
            } as any,
            'qq-capture-test',
        )
        ;(forwarder as any).minInterval = 0

        const payloads: any[] = []
        ;(forwarder as any).sendWithPayload = async (segments: any) => {
            payloads.push(segments)
            return { ok: true }
        }

        const result = await forwarder.send('captured payload', {
            media: [{ media_type: 'photo', path: '/tmp/captured.jpg' }],
            article: {
                platform: Platform.X,
                id: 1,
                a_id: 'capture-article',
                url: 'https://x.example/capture-article',
            } as any,
            outboundKey: 'article:qq-capture-test:1:capture-article',
        })

        expect(result.status).toBe('dry_run')
        expect(result.status === 'dry_run' ? result.details.send_mode : '').toBe('capture')
        expect(result.status === 'dry_run' ? result.details.capture_result?.ok : false).toBe(true)
        expect(forwarder.drainPendingMediaBatches()).toHaveLength(0)
        expect(payloads).toHaveLength(0)

        const captures = (await readFile(captureFile, 'utf8'))
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line))
        expect(captures).toHaveLength(1)
        expect(captures[0].target_id).toBe('qq-capture-test')
        expect(captures[0].forwarder).toBe('qq')
        expect(captures[0].texts).toEqual(['captured payload'])
        expect(captures[0].media[0]).toMatchObject({
            media_type: 'photo',
            path: '/tmp/captured.jpg',
            file_name: 'captured.jpg',
        })
        expect(captures[0].outbound_key).toBe('article:qq-capture-test:1:capture-article')
    } finally {
        if (originalMode === undefined) {
            delete process.env.IDOL_BBQ_OUTBOUND_SEND_MODE
        } else {
            process.env.IDOL_BBQ_OUTBOUND_SEND_MODE = originalMode
        }
        if (originalCaptureFile === undefined) {
            delete process.env.IDOL_BBQ_OUTBOUND_CAPTURE_FILE
        } else {
            process.env.IDOL_BBQ_OUTBOUND_CAPTURE_FILE = originalCaptureFile
        }
        await rm(tempRoot, { recursive: true, force: true })
    }
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

    const results = []
    for (const index of [1, 2, 3]) {
        results.push(
            await forwarder.send(`batch text ${index}`, {
                media: [
                    {
                        media_type: 'photo',
                        path: `/tmp/batch-${index}.jpg`,
                    },
                ],
                article: {
                    platform: Platform.X,
                    id: index,
                    a_id: `batch-${index}`,
                } as any,
            }),
        )
    }

    expect(results.map((result) => result.status)).toEqual(['queued', 'queued', 'sent'])
    expect(results[2]?.status === 'sent' ? results[2].batchArticles?.map((article: any) => article.a_id) : []).toEqual([
        'batch-1',
        'batch-2',
        'batch-3',
    ])
    expect(payloads).toHaveLength(1)
    expect(payloads[0].filter((segment: any) => segment.type === 'image')).toHaveLength(3)
    expect(payloads[0][0]?.data?.text).toContain('batch text 1')
    expect(payloads[0][0]?.data?.text).toContain('batch text 3')
})

test('QQForwarder drops pending image-like batch items without visible send', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
            media_batch_threshold: 6,
        } as any,
        'qq-batch-drop-test',
    )
    ;(forwarder as any).minInterval = 0

    const payloads: any[] = []
    ;(forwarder as any).sendWithPayload = async (segments: any) => {
        payloads.push(segments)
        return { ok: true }
    }

    const result = await forwarder.send('queued then drop', {
        media: [
            {
                media_type: 'photo',
                path: '/tmp/drop-queued.jpg',
            },
        ],
        article: {
            platform: Platform.X,
            id: 10,
            a_id: 'drop-queued',
        } as any,
        outboundKey: 'article:qq-batch-drop-test:1:drop-queued',
    })

    expect(result.status).toBe('queued')
    const discarded = forwarder.drainPendingMediaBatches()
    expect(discarded).toHaveLength(1)
    expect(discarded[0]?.items[0]?.outboundKey).toBe('article:qq-batch-drop-test:1:drop-queued')
    await forwarder.drop()
    expect(payloads).toHaveLength(0)
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

    const first = await forwarder.send('queued one', {
        media: [{ media_type: 'photo', path: '/tmp/q1.jpg' }],
        article: {
            platform: Platform.X,
        } as any,
    })
    const breakout = await forwarder.send('breakout now', {
        media: [
            { media_type: 'photo', path: '/tmp/breakout-1.jpg' },
            { media_type: 'photo', path: '/tmp/breakout-2.jpg' },
            { media_type: 'photo', path: '/tmp/breakout-3.jpg' },
        ],
        article: {
            platform: Platform.X,
        } as any,
    })
    const second = await forwarder.send('queued two', {
        media: [{ media_type: 'photo', path: '/tmp/q2.jpg' }],
        article: {
            platform: Platform.X,
        } as any,
    })
    const third = await forwarder.send('queued three', {
        media: [{ media_type: 'photo', path: '/tmp/q3.jpg' }],
        article: {
            platform: Platform.X,
        } as any,
    })

    expect([first.status, breakout.status, second.status, third.status]).toEqual(['queued', 'sent', 'queued', 'sent'])
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

    const result = await forwarder.send('card caption', {
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

    expect(result.status).toBe('sent')
    expect(payloads).toHaveLength(2)
    expect(payloads[0].filter((segment: any) => segment.type === 'image')).toHaveLength(1)
    expect(payloads[0][0]?.data?.text).toContain('card caption')
    expect(payloads[1].filter((segment: any) => segment.type === 'image')).toHaveLength(2)
    expect(payloads[1].some((segment: any) => segment.type === 'text')).toBe(false)
})

test('QQForwarder counts text as one unit even when a rendered card is present', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
            media_batch_threshold: 6,
        } as any,
        'qq-text-unit-test',
    )
    ;(forwarder as any).minInterval = 0

    const payloads: any[] = []
    ;(forwarder as any).sendWithPayload = async (segments: any) => {
        payloads.push(segments)
        return { ok: true }
    }

    const results = []
    for (const index of [1, 2]) {
        results.push(
            await forwarder.send(`card text ${index}`, {
                media: [
                    { media_type: 'photo', path: `/tmp/card-${index}.jpg`, sourceArticleId: `article-${index}` },
                    { media_type: 'photo', path: `/tmp/photo-${index}.jpg`, sourceArticleId: `article-${index}` },
                ] as any,
                cardMedia: [
                    { media_type: 'photo', path: `/tmp/card-${index}.jpg`, sourceArticleId: `article-${index}` },
                ] as any,
                contentMedia: [
                    { media_type: 'photo', path: `/tmp/photo-${index}.jpg`, sourceArticleId: `article-${index}` },
                ] as any,
                article: {
                    platform: Platform.X,
                    a_id: `article-${index}`,
                    u_id: 'member_a',
                } as any,
            }),
        )
    }

    expect(results.map((result) => result.status)).toEqual(['queued', 'sent'])
    expect(payloads).toHaveLength(1)
    expect(payloads[0].filter((segment: any) => segment.type === 'image')).toHaveLength(4)
    expect(payloads[0][0]?.data?.text).toContain('card text 1')
    expect(payloads[0][0]?.data?.text).toContain('card text 2')
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

    const first = await forwarder.send('website direct 1', {
        media: [{ media_type: 'photo', path: '/tmp/non-x-1.jpg' }],
        article: {
            platform: Platform.Website,
        } as any,
    })
    const second = await forwarder.send('website direct 2', {
        media: [{ media_type: 'photo', path: '/tmp/non-x-2.jpg' }],
        article: {
            platform: Platform.Website,
        } as any,
    })

    expect([first.status, second.status]).toEqual(['sent', 'sent'])
    expect(payloads).toHaveLength(2)
})

test('QQForwarder does not count ref media from users outside the list context', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
            media_batch_threshold: 3,
        } as any,
        'qq-ref-filter-test',
    )
    ;(forwarder as any).minInterval = 0

    const payloads: any[] = []
    ;(forwarder as any).sendWithPayload = async (segments: any) => {
        payloads.push(segments)
        return { ok: true }
    }

    const first = await forwarder.send('root text', {
        media: [
            {
                media_type: 'photo',
                path: '/tmp/root.jpg',
                sourceArticleId: 'root-1',
                sourceUserId: 'member_a',
            },
            {
                media_type: 'photo',
                path: '/tmp/ref-outsider.jpg',
                sourceArticleId: 'ref-1',
                sourceUserId: 'outsider_user',
            },
        ] as any,
        contentMedia: [
            {
                media_type: 'photo',
                path: '/tmp/root.jpg',
                sourceArticleId: 'root-1',
                sourceUserId: 'member_a',
            },
            {
                media_type: 'photo',
                path: '/tmp/ref-outsider.jpg',
                sourceArticleId: 'ref-1',
                sourceUserId: 'outsider_user',
            },
        ] as any,
        article: {
            platform: Platform.X,
            a_id: 'root-1',
            u_id: 'member_a',
            ref: {
                platform: Platform.X,
                a_id: 'ref-1',
                u_id: 'outsider_user',
            },
            extra: {
                data: {
                    list_context: {
                        list_id: 'list-1',
                        user_ids: ['member_a'],
                    },
                },
                extra_type: 'x_list_meta',
            },
        } as any,
    })

    expect(first.status).toBe('queued')
    expect(payloads).toHaveLength(0)

    const second = await forwarder.send('follow-up text', {
        media: [
            {
                media_type: 'photo',
                path: '/tmp/root-2.jpg',
                sourceArticleId: 'root-2',
                sourceUserId: 'member_a',
            },
        ] as any,
        contentMedia: [
            {
                media_type: 'photo',
                path: '/tmp/root-2.jpg',
                sourceArticleId: 'root-2',
                sourceUserId: 'member_a',
            },
        ] as any,
        article: {
            platform: Platform.X,
            a_id: 'root-2',
            u_id: 'member_a',
            extra: {
                data: {
                    list_context: {
                        list_id: 'list-1',
                        user_ids: ['member_a'],
                    },
                },
                extra_type: 'x_list_meta',
            },
        } as any,
    })

    expect(second.status).toBe('sent')
    expect(payloads).toHaveLength(1)
    expect(payloads[0].filter((segment: any) => segment.type === 'image')).toHaveLength(3)
    expect(payloads[0][0]?.data?.text).toContain('root text')
    expect(payloads[0][0]?.data?.text).toContain('follow-up text')
})
