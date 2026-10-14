import { expect, test } from 'bun:test'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import {
    QQ_MEDIA_DEFAULTS,
    buildQQMediaFileValue,
    resolveQQMediaDeliveryConfig,
    selectQQMediaSource,
    uploadFileViaOneBotStream,
} from './snowluma-media-source'

function config(overrides: Partial<ReturnType<typeof resolveQQMediaDeliveryConfig>> = {}) {
    return {
        mode: 'file' as const,
        urlBase: null,
        inlineMaxBytes: QQ_MEDIA_DEFAULTS.inlineMaxBytes,
        uploadMaxBytes: QQ_MEDIA_DEFAULTS.uploadMaxBytes,
        uploadChunkBytes: QQ_MEDIA_DEFAULTS.uploadChunkBytes,
        uploadRequestTimeoutMs: QQ_MEDIA_DEFAULTS.uploadRequestTimeoutMs,
        uploadDeadlineMs: QQ_MEDIA_DEFAULTS.uploadDeadlineMs,
        uploadRetentionMs: QQ_MEDIA_DEFAULTS.uploadRetentionMs,
        ...overrides,
    }
}

test('media_delivery defaults to file and keeps the legacy file:// wire shape', () => {
    const resolved = resolveQQMediaDeliveryConfig({} as any)
    expect(resolved.mode).toBe('file')
    const decision = selectQQMediaSource({ path: '/tmp/a.png', sizeBytes: 10 }, resolved)
    expect(decision).toMatchObject({ method: 'file', value: 'file:///tmp/a.png' })
})

test('env override IDOL_BBQ_QQ_MEDIA_DELIVERY wins over config', () => {
    process.env.IDOL_BBQ_QQ_MEDIA_DELIVERY = 'inline'
    try {
        const resolved = resolveQQMediaDeliveryConfig({ media_delivery: 'file' } as any)
        expect(resolved.mode).toBe('inline')
    } finally {
        delete process.env.IDOL_BBQ_QQ_MEDIA_DELIVERY
    }
})

test('auto mode decision table: url > inline > upload > file by size', () => {
    const cfg = config({ mode: 'auto' })
    // URL available (media_url_base) wins.
    expect(
        selectQQMediaSource({ path: '/cache/media/x.png', sizeBytes: 5 }, {
            ...cfg,
            urlBase: 'http://127.0.0.1:8080/media',
        }),
    ).toMatchObject({ method: 'url', value: 'http://127.0.0.1:8080/media/x.png' })
    // Small file inlines.
    expect(selectQQMediaSource({ path: '/tmp/a.png', sizeBytes: 1024 }, cfg)).toMatchObject({ method: 'inline' })
    // Medium file uploads.
    expect(selectQQMediaSource({ path: '/tmp/b.mp4', sizeBytes: 100 * 1024 * 1024 }, cfg)).toMatchObject({
        method: 'upload',
    })
    // Oversized file falls back to file:// (never a runaway message upload).
    expect(selectQQMediaSource({ path: '/tmp/huge.mp4', sizeBytes: 3.47 * 1024 * 1024 * 1024 }, cfg)).toMatchObject({
        method: 'file',
        value: 'file:///tmp/huge.mp4',
    })
    // Unknown size (missing file) keeps file://.
    expect(selectQQMediaSource({ path: '/tmp/gone.jpg', sizeBytes: null }, cfg)).toMatchObject({
        method: 'file',
        value: 'file:///tmp/gone.jpg',
    })
})

test('forced modes degrade to file:// instead of failing when a cap is exceeded', () => {
    expect(
        selectQQMediaSource({ path: '/tmp/a.png', sizeBytes: 10_000_000 }, config({ mode: 'inline' })),
    ).toMatchObject({ method: 'file' })
    expect(
        selectQQMediaSource({ path: '/tmp/a.mp4', sizeBytes: 10_000_000_000 }, config({ mode: 'upload' })),
    ).toMatchObject({ method: 'file' })
    // url mode without any resolvable URL degrades to file:// too.
    expect(selectQQMediaSource({ path: '/tmp/a.png', sizeBytes: 10 }, config({ mode: 'url' }))).toMatchObject({
        method: 'file',
    })
})

test('buildQQMediaFileValue inlines small files as base64://', async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'qq-media-inline-'))
    const filePath = path.join(tempRoot, 'small.png')
    await writeFile(filePath, Buffer.from('hello-media'))
    try {
        const built = await buildQQMediaFileValue({ path: filePath }, {
            config: config({ mode: 'inline' }),
            endpoint: 'http://127.0.0.1:3000',
            token: 'tok',
        })
        expect(built.method).toBe('inline')
        expect(built.value).toBe(`base64://${Buffer.from('hello-media').toString('base64')}`)
    } finally {
        await rm(tempRoot, { recursive: true, force: true })
    }
})

test('buildQQMediaFileValue keeps file:// when the file does not exist (NapCat regression)', async () => {
    const built = await buildQQMediaFileValue({ path: '/tmp/does-not-exist.jpg' }, {
        config: config({ mode: 'auto' }),
        endpoint: 'http://127.0.0.1:3000',
        token: 'tok',
    })
    expect(built).toMatchObject({ method: 'file', value: 'file:///tmp/does-not-exist.jpg' })
})

test('uploadFileViaOneBotStream chunks the file and returns the bot path', async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'qq-media-upload-'))
    const filePath = path.join(tempRoot, 'clip.bin')
    const payload = Buffer.alloc(300_000, 7)
    await writeFile(filePath, payload)
    const calls: Array<{ path: string; body: any; options: any }> = []
    try {
        const botPath = await uploadFileViaOneBotStream({
            endpoint: 'http://127.0.0.1:3000',
            token: 'tok',
            filePath,
            fileName: 'clip.bin',
            fileSize: payload.length,
            config: config({ mode: 'upload', uploadChunkBytes: 128 * 1024 }),
            post: async (requestPath, body, options) => {
                calls.push({ path: requestPath, body, options })
                const isLast = (body as any).is_complete === true
                return {
                    data: {
                        status: 'ok',
                        retcode: 0,
                        data: isLast
                            ? {
                                  type: 'response',
                                  status: 'file_complete',
                                  file_path: '/app/data/stream/upload/clip.bin',
                                  file_name: 'clip.bin',
                                  file_size: payload.length,
                                  sha256: 'x',
                              }
                            : { type: 'stream', status: 'chunk_received' },
                    },
                }
            },
        })
        expect(botPath).toBe('/app/data/stream/upload/clip.bin')
        expect(calls).toHaveLength(3)
        expect(calls[0].path).toBe('http://127.0.0.1:3000/upload_file_stream')
        expect(calls[0].options.headers.Authorization).toBe('Bearer tok')
        expect(calls[0].body.chunk_index).toBe(0)
        expect(calls[0].body.total_chunks).toBe(3)
        expect(calls[0].body.stream_id).toMatch(/^[A-Za-z0-9_-]+$/)
        expect(calls[2].body.chunk_index).toBe(2)
        expect(calls[2].body.is_complete).toBe(true)
        expect(calls[2].body.expected_sha256).toMatch(/^[0-9a-f]{64}$/)
        // Reassembled base64 chunks must reproduce the original bytes.
        const reassembled = Buffer.concat(
            calls.map((call) => Buffer.from(String(call.body.chunk_data), 'base64')),
        )
        expect(reassembled.equals(payload)).toBe(true)
    } finally {
        await rm(tempRoot, { recursive: true, force: true })
    }
})

test('uploadFileViaOneBotStream surfaces business failures with the shared wording shape', async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'qq-media-upload-fail-'))
    const filePath = path.join(tempRoot, 'clip.bin')
    await writeFile(filePath, Buffer.alloc(10, 1))
    try {
        await expect(
            uploadFileViaOneBotStream({
                endpoint: 'http://127.0.0.1:3000',
                token: '',
                filePath,
                fileName: 'clip.bin',
                fileSize: 10,
                config: config({ mode: 'upload', uploadChunkBytes: 1024 }),
                post: async () => ({
                    data: {
                        status: 'failed',
                        retcode: 100,
                        wording: 'send failed: 触发风控',
                    },
                }),
            }),
        ).rejects.toThrow(/retcode=100 message=send failed: 触发风控/)
    } finally {
        await rm(tempRoot, { recursive: true, force: true })
    }
})
