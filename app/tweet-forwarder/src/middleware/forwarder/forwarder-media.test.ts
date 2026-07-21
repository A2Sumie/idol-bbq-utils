import { expect, test } from 'bun:test'
import axios from 'axios'
import { Platform } from '@idol-bbq-utils/spider/types'
import { BiliForwarder, BiliUploadThrottledError } from './bilibili'
import { QQForwarder } from './qq'
import { NonRetryableForwarderSendError, PartialForwarderSendError } from './base'
import { resolveForwarderImageMaxBytes } from '@/services/forwarder-image-attachment-service'
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

test('image max byte config treats small legacy values as KiB', () => {
    expect(resolveForwarderImageMaxBytes({ max_image_bytes: 3500 })).toBe(3500 * 1024)
    expect(resolveForwarderImageMaxBytes({ max_image_bytes: 4_700_000 })).toBe(4_700_000)
})

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

test('QQForwarder rejects OneBot failed JSON responses even when HTTP succeeds', () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
        } as any,
        'qq-onebot-response-test',
    )

    expect(() =>
        (forwarder as any).assertOneBotResponseOk(
            {
                statusText: 'OK',
                data: {
                    status: 'ok',
                    retcode: 0,
                },
            },
            'send_group_msg',
        ),
    ).not.toThrow()
    expect(() =>
        (forwarder as any).assertOneBotResponseOk(
            {
                statusText: 'OK',
                data: {
                    status: 'failed',
                    retcode: 200,
                    message: 'EventChecker Failed',
                },
            },
            'send_group_msg',
        ),
    ).toThrow(/status=failed retcode=200 message=EventChecker Failed/)
})

test('QQForwarder can package text and media as OneBot merged-forward nodes', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
            send_mode: 'merged_forward',
            merged_forward: {
                node_name: '七虹信标',
                node_uin: '227',
                max_segments_per_node: 2,
            },
        } as any,
        'qq-merged-forward-test',
    )
    ;(forwarder as any).minInterval = 0

    const mergedPayloads: any[] = []
    ;(forwarder as any).sendWithPayload = async () => {
        throw new Error('normal send should not be used')
    }
    ;(forwarder as any).sendMergedForwardPayload = async (segments: any, config: any) => {
        mergedPayloads.push({ segments, config })
        return { ok: true, mode: 'merged_forward' }
    }

    const result = await (forwarder as any).realSend(['translated card text', 'original text'], {
        media: [
            {
                media_type: 'photo',
                path: '/tmp/card.png',
            },
            {
                media_type: 'video',
                path: '/tmp/source.mp4',
            },
            {
                media_type: 'video_thumbnail',
                path: '/tmp/thumbnail.jpg',
            },
        ],
    })

    expect(result).toEqual([{ ok: true, mode: 'merged_forward' }])
    expect(mergedPayloads).toHaveLength(1)
    expect(mergedPayloads[0].config).toMatchObject({
        enabled: true,
        nodeName: '七虹信标',
        nodeUin: '227',
        maxSegmentsPerNode: 2,
    })
    expect(mergedPayloads[0].segments).toEqual([
        { type: 'text', data: { text: 'translated card text' } },
        { type: 'text', data: { text: 'original text' } },
        { type: 'image', data: { file: 'file:///tmp/card.png' } },
        { type: 'video', data: { file: 'file:///tmp/source.mp4' } },
    ])
})

test('QQForwarder keeps media order inside merged-forward nodes', async () => {
    const forwarder = new QQForwarder(
        {
            group_id: '123',
            url: 'http://127.0.0.1:3001',
            token: '',
            send_mode: 'merged_forward',
        } as any,
        'qq-merged-forward-order-test',
    )
    ;(forwarder as any).minInterval = 0

    const mergedPayloads: any[] = []
    ;(forwarder as any).sendMergedForwardPayload = async (segments: any, config: any) => {
        mergedPayloads.push({ segments, config })
        return { ok: true, mode: 'merged_forward' }
    }

    await (forwarder as any).realSend(['[X解析]\noriginal text'], {
        media: [
            {
                media_type: 'video',
                path: '/tmp/source.mp4',
            },
            {
                media_type: 'photo',
                path: '/tmp/card.png',
            },
        ],
    })

    expect(mergedPayloads[0].segments).toEqual([
        { type: 'text', data: { text: '[X解析]\noriginal text' } },
        { type: 'video', data: { file: 'file:///tmp/source.mp4' } },
        { type: 'image', data: { file: 'file:///tmp/card.png' } },
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

test('BiliForwarder uses blank fallback text for photo dynamics without body text', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-blank-photo-text-test',
    )
    ;(forwarder as any).minInterval = 0

    let sentText = ''
    ;(forwarder as any).uploadPhoto = async () => ({
        image_url: 'https://i0.hdslb.com/bfs/test/blank-text.jpg',
        image_width: 900,
        image_height: 1200,
        img_size: 12345,
    })
    ;(forwarder as any).sendTextWithPhotos = async (text: string) => {
        sentText = text
        return { data: { code: 0, message: 'ok', data: { dyn_id_str: 'blank-photo-dynamic' } } }
    }
    ;(forwarder as any).fetchDynamicDetail = async () => ({
        data: {
            code: 0,
            data: {
                item: {
                    modules: {
                        module_dynamic: {
                            major: {
                                type: 'MAJOR_TYPE_DRAW',
                                draw: {
                                    items: [{ src: 'https://i0.hdslb.com/bfs/test/blank-text.jpg' }],
                                },
                            },
                        },
                    },
                },
            },
        },
    })

    await (forwarder as any).sendDynamicContent([], {
        media: [{ media_type: 'photo', path: '/tmp/source.jpg' }],
    })

    expect(sentText).toBe(' ')
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

test('BiliForwarder post-validation partial failure is not retried by the whole-send layer (no re-upload/re-post)', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            media_check_level: 'strict',
            require_media: true,
        } as any,
        'bili-partial-no-retry-test',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0
    ;(forwarder as any).dynamicDetailValidationRetries = 0

    let uploadCount = 0
    let createCount = 0
    ;(forwarder as any).uploadPhoto = async () => {
        uploadCount += 1
        return {
            image_url: 'https://i0.hdslb.com/bfs/test/partial-no-retry.jpg',
            image_width: 900,
            image_height: 1200,
            img_size: 188,
        }
    }
    ;(forwarder as any).sendTextWithPhotos = async () => {
        createCount += 1
        return { data: { code: 0, message: 'OK', data: { dyn_id_str: 'dyn-partial' } } }
    }
    ;(forwarder as any).fetchDynamicDetail = async () => ({
        data: { code: 0, data: { item: { modules: { module_dynamic: { major: null } } } } },
    })

    await expect(
        (forwarder as any).sendPrepared(['dynamic text'], {
            media: [{ media_type: 'photo', path: '/tmp/source.jpg' }],
        }),
    ).rejects.toBeInstanceOf(PartialForwarderSendError)

    // The dynamic was actually created once; retrying would re-upload the photo and re-post the
    // dynamic, producing a duplicate. The whole-send pRetry must treat partial as terminal.
    expect(uploadCount).toBe(1)
    expect(createCount).toBe(1)
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

test('BiliForwarder maps upload_bfs provider codes to error classes', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-upload-code-test',
    )
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'bili-upload-code-'))
    const photoPath = path.join(tempRoot, 'photo.bin')
    await writeFile(photoPath, Buffer.alloc(16, 1))

    const originalPost = axios.post
    try {
        ;(axios as any).post = async () => ({ data: { code: -111, message: 'csrf校验失败' } })
        await expect((forwarder as any).uploadPhoto(photoPath)).rejects.toThrow(BiliUploadThrottledError)

        ;(axios as any).post = async () => ({ data: { code: -101, message: '账号未登录' } })
        await expect((forwarder as any).uploadPhoto(photoPath)).rejects.toThrow(NonRetryableForwarderSendError)

        ;(axios as any).post = async () => ({ data: { code: -412, message: 'risk control' } })
        await expect((forwarder as any).uploadPhoto(photoPath)).rejects.toThrow(/Upload photo to bilibili failed/)
    } finally {
        ;(axios as any).post = originalPost
        await rm(tempRoot, { recursive: true, force: true })
    }
})

test('BiliForwarder retries anonymous buvid fetch after an empty SPI response', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-empty-buvid-test',
    )
    const calls: string[] = []
    ;(forwarder as any).api.fetchAnonymousBuvid = async () => {
        calls.push('fetch')
        return calls.length === 1 ? null : { buvid3: 'b3', buvid4: 'b4' }
    }

    await (forwarder as any).ensureBuvidCookies()
    expect((forwarder as any).api.hasBuvid).toBe(false)
    await (forwarder as any).ensureBuvidCookies()
    expect((forwarder as any).api.hasBuvid).toBe(true)
    expect(calls).toHaveLength(2)
})

test('BiliForwarder serializes photo uploads inside one send', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-upload-pacing-test',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0

    let inFlight = 0
    let maxInFlight = 0
    const uploadOrder: string[] = []
    ;(forwarder as any).uploadPhoto = async (filePath: string) => {
        inFlight += 1
        maxInFlight = Math.max(maxInFlight, inFlight)
        await new Promise((resolve) => setTimeout(resolve, 20))
        inFlight -= 1
        uploadOrder.push(filePath)
        return {
            image_url: `https://i0.hdslb.com/bfs/test/paced-${uploadOrder.length}.jpg`,
            image_width: 100,
            image_height: 100,
            img_size: 10,
        }
    }
    let sentPicCount = 0
    ;(forwarder as any).sendTextWithPhotos = async (_text: string, pics: any[]) => {
        sentPicCount = pics.length
        return { data: { code: 0, message: 'ok', data: { dyn_id_str: 'paced-dynamic' } } }
    }
    ;(forwarder as any).fetchDynamicDetail = async () => ({
        data: {
            code: 0,
            data: {
                item: {
                    modules: {
                        module_dynamic: {
                            major: {
                                type: 'MAJOR_TYPE_DRAW',
                                draw: {
                                    items: [{ src: 'a' }, { src: 'b' }, { src: 'c' }],
                                },
                            },
                        },
                    },
                },
            },
        },
    })

    await (forwarder as any).sendDynamicContent(['paced text'], {
        media: [
            { media_type: 'photo', path: '/tmp/paced-a.jpg' },
            { media_type: 'photo', path: '/tmp/paced-b.jpg' },
            { media_type: 'photo', path: '/tmp/paced-c.jpg' },
        ],
    })

    expect(maxInFlight).toBe(1)
    expect(uploadOrder).toEqual(['/tmp/paced-a.jpg', '/tmp/paced-b.jpg', '/tmp/paced-c.jpg'])
    expect(sentPicCount).toBe(3)
})

test('BiliForwarder fails the whole send when a photo upload stays throttled', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-upload-throttle-test',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0
    ;(forwarder as any).photoUploadRetries = 0

    let sendCalled = false
    ;(forwarder as any).uploadPhoto = async () => {
        throw new BiliUploadThrottledError('throttled by test')
    }
    ;(forwarder as any).sendTextWithPhotos = async () => {
        sendCalled = true
        return { data: { code: 0, message: 'ok' } }
    }

    await expect(
        (forwarder as any).sendDynamicContent(['throttled text'], {
            media: [{ media_type: 'photo', path: '/tmp/throttled.jpg' }],
        }),
    ).rejects.toThrow(BiliUploadThrottledError)
    expect(sendCalled).toBeFalse()
})

test('BiliUploadThrottledError is non-retryable so the whole-send layer does not re-upload', async () => {
    expect(new BiliUploadThrottledError('x')).toBeInstanceOf(NonRetryableForwarderSendError)

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-upload-throttle-nonretry-test',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0
    ;(forwarder as any).photoUploadRetries = 0

    const uploadAttempts: string[] = []
    ;(forwarder as any).uploadPhoto = async (filePath: string) => {
        uploadAttempts.push(filePath)
        if (filePath.endsWith('ok.jpg')) {
            return {
                image_url: 'https://i0.hdslb.com/bfs/test/ok.jpg',
                image_width: 100,
                image_height: 100,
                img_size: 10,
            }
        }
        throw new BiliUploadThrottledError('throttled by test')
    }
    ;(forwarder as any).sendTextWithPhotos = async () => ({ data: { code: 0, message: 'ok' } })

    await expect(
        (forwarder as any).sendPrepared(['throttled text'], {
            media: [
                { media_type: 'photo', path: '/tmp/ok.jpg' },
                { media_type: 'photo', path: '/tmp/throttled.jpg' },
            ],
        }),
    ).rejects.toThrow(BiliUploadThrottledError)

    expect(uploadAttempts.filter((p) => p.endsWith('ok.jpg'))).toHaveLength(1)
})

test('BiliForwarder does not re-upload successful photos when strict upload validation fails', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            media_check_level: 'strict',
        } as any,
        'bili-upload-strict-validation-nonretry-test',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0
    ;(forwarder as any).photoUploadRetries = 0

    const uploadAttempts: string[] = []
    ;(forwarder as any).uploadPhoto = async (filePath: string) => {
        uploadAttempts.push(filePath)
        if (filePath.endsWith('ok.jpg')) {
            return {
                image_url: 'https://i0.hdslb.com/bfs/test/ok.jpg',
                image_width: 100,
                image_height: 100,
                img_size: 10,
            }
        }
        throw new Error('ordinary upload failure')
    }
    ;(forwarder as any).sendTextWithPhotos = async () => ({ data: { code: 0, message: 'ok' } })

    await expect(
        (forwarder as any).sendPrepared(['strict text'], {
            media: [
                { media_type: 'photo', path: '/tmp/ok.jpg' },
                { media_type: 'photo', path: '/tmp/fail.jpg' },
            ],
        }),
    ).rejects.toBeInstanceOf(NonRetryableForwarderSendError)

    expect(uploadAttempts.filter((p) => p.endsWith('ok.jpg'))).toHaveLength(1)
    expect(uploadAttempts.filter((p) => p.endsWith('fail.jpg'))).toHaveLength(1)
})

test('BiliForwarder does not re-upload photos when the dynamic create fails at the whole-send layer', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-create-fail-no-reupload',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0
    ;(forwarder as any).dynamicCreateRetries = 2
    ;(forwarder as any).dynamicCreateRetryMinTimeoutMs = 0

    let uploadCount = 0
    ;(forwarder as any).uploadPhoto = async () => {
        uploadCount += 1
        return {
            image_url: 'https://i0.hdslb.com/bfs/test/create-fail.jpg',
            image_width: 100,
            image_height: 100,
            img_size: 10,
        }
    }
    let createCount = 0
    ;(forwarder as any).sendTextWithPhotos = async () => {
        createCount += 1
        return { data: { code: 4100000, message: 'risk control' } }
    }

    await expect(
        (forwarder as any).sendPrepared(['card text'], {
            media: [{ media_type: 'photo', path: '/tmp/create-fail.jpg' }],
        }),
    ).rejects.toBeInstanceOf(NonRetryableForwarderSendError)

    expect(uploadCount).toBe(1)
    expect(createCount).toBe(3)
})

test('BiliForwarder retries a transient dynamic create in-band without re-uploading the photo', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-create-transient-retry',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0
    ;(forwarder as any).dynamicCreateRetryMinTimeoutMs = 0
    ;(forwarder as any).assertPhotoDynamicVisible = async () => {}

    let uploadCount = 0
    ;(forwarder as any).uploadPhoto = async () => {
        uploadCount += 1
        return {
            image_url: 'https://i0.hdslb.com/bfs/test/transient.jpg',
            image_width: 100,
            image_height: 100,
            img_size: 10,
        }
    }
    let createCount = 0
    ;(forwarder as any).sendTextWithPhotos = async () => {
        createCount += 1
        if (createCount < 2) {
            return { data: { code: 500, message: 'server error' } }
        }
        return { data: { code: 0, message: 'ok', data: { dyn_id_str: 'transient-dyn' } } }
    }

    const result = await (forwarder as any).sendPrepared(['card text'], {
        media: [{ media_type: 'photo', path: '/tmp/transient.jpg' }],
    })

    expect(uploadCount).toBe(1)
    expect(createCount).toBe(2)
    expect(result).toHaveLength(1)
})

test('BiliForwarder retries dynamic create -111 in-band without re-uploading the photo', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-create-throttle-retry',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0
    ;(forwarder as any).dynamicCreateRetryMinTimeoutMs = 0
    ;(forwarder as any).assertPhotoDynamicVisible = async () => {}

    let uploadCount = 0
    ;(forwarder as any).uploadPhoto = async () => {
        uploadCount += 1
        return {
            image_url: 'https://i0.hdslb.com/bfs/test/throttle-create.jpg',
            image_width: 100,
            image_height: 100,
            img_size: 10,
        }
    }
    let createCount = 0
    ;(forwarder as any).sendTextWithPhotos = async () => {
        createCount += 1
        if (createCount < 2) {
            return { data: { code: -111, message: 'velocity control' } }
        }
        return { data: { code: 0, message: 'ok', data: { dyn_id_str: 'throttle-create-dyn' } } }
    }

    const result = await (forwarder as any).sendPrepared(['card text'], {
        media: [{ media_type: 'photo', path: '/tmp/throttle-create.jpg' }],
    })

    expect(uploadCount).toBe(1)
    expect(createCount).toBe(2)
    expect(result).toHaveLength(1)
})

test('BiliForwarder auth rejection on create is non-retryable and does not re-upload', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-create-auth-fail',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0
    ;(forwarder as any).dynamicCreateRetryMinTimeoutMs = 0

    let uploadCount = 0
    ;(forwarder as any).uploadPhoto = async () => {
        uploadCount += 1
        return {
            image_url: 'https://i0.hdslb.com/bfs/test/auth-fail.jpg',
            image_width: 100,
            image_height: 100,
            img_size: 10,
        }
    }
    let createCount = 0
    ;(forwarder as any).sendTextWithPhotos = async () => {
        createCount += 1
        return { data: { code: -101, message: 'account not logged in' } }
    }

    await expect(
        (forwarder as any).sendPrepared(['card text'], {
            media: [{ media_type: 'photo', path: '/tmp/auth-fail.jpg' }],
        }),
    ).rejects.toBeInstanceOf(NonRetryableForwarderSendError)

    expect(uploadCount).toBe(1)
    expect(createCount).toBe(1)
})

test('BiliForwarder does not re-post an earlier chunk when a later chunk create fails', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
        } as any,
        'bili-multichunk-partial',
    )
    ;(forwarder as any).minInterval = 0
    ;(forwarder as any).photoUploadGapMs = 0
    ;(forwarder as any).dynamicCreateRetries = 0
    ;(forwarder as any).dynamicCreateRetryMinTimeoutMs = 0
    ;(forwarder as any).assertPhotoDynamicVisible = async () => {}

    ;(forwarder as any).uploadPhoto = async (filePath: string) => ({
        image_url: `https://i0.hdslb.com/bfs/test/${path.basename(filePath)}`,
        image_width: 100,
        image_height: 100,
        img_size: 10,
    })

    const createdChunks: number[] = []
    let createCount = 0
    ;(forwarder as any).sendTextWithPhotos = async (_text: string, pics: any[]) => {
        createCount += 1
        if (createCount === 1) {
            createdChunks.push(pics.length)
            return { data: { code: 0, message: 'ok', data: { dyn_id_str: `chunk-${createCount}` } } }
        }
        return { data: { code: 4100000, message: 'risk control' } }
    }

    const media = Array.from({ length: 11 }, (_, i) => ({
        media_type: 'photo' as const,
        path: `/tmp/p${i}.jpg`,
    }))

    let thrown: unknown
    await (forwarder as any).sendPrepared(['multi chunk'], { media }).catch((error: unknown) => {
        thrown = error
    })

    expect(thrown).toBeInstanceOf(PartialForwarderSendError)
    expect(createdChunks).toEqual([9])
    expect(createCount).toBe(2)
})
