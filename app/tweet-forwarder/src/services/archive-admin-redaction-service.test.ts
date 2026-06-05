import { expect, test } from 'bun:test'
import {
    normalizeRedactedArchiveUploadRequest,
    redactArchiveDetailForApi,
    redactArchiveErrorMessageForApi,
    redactArchiveListForApi,
    redactArchiveUploadResultForApi,
} from './archive-admin-redaction-service'

test('archive list redaction removes host paths from summaries and defaults', () => {
    const payload = redactArchiveListForApi({
        items: [
            {
                id: 'archive-a',
                title: 'Archive A',
                localPath: '/Users/zou/private/archive-a.mp4',
                pageUrl: 'https://example.test/watch',
            },
        ],
        defaults: {
            cookieSourcePath: '/Users/zou/private/cookies.txt',
            helperPath: '/Users/zou/private/helper.py',
            pythonPath: '/usr/bin/python3',
            tid: 17,
        },
    })
    const serialized = JSON.stringify(payload)

    expect(payload.redacted).toBe(true)
    expect(payload.items[0].localPath).toBe('[redacted]')
    expect(payload.items[0].localPath_meta).toMatchObject({
        redacted_path: true,
        filename: 'archive-a.mp4',
    })
    expect(payload.defaults.cookieSourcePath).toBe('[redacted]')
    expect(payload.defaults.cookieSourcePath_meta).toMatchObject({
        redacted_path: true,
        filename: 'cookies.txt',
    })
    expect(payload.defaults.helperPath).toBe('[redacted]')
    expect(payload.defaults.pythonPath).toBe('[redacted]')
    expect(serialized).not.toContain('/Users/zou/private')
    expect(serialized).not.toContain('/usr/bin/python3')
})

test('archive detail redaction removes related file paths and suggested upload path text', () => {
    const payload = redactArchiveDetailForApi({
        id: 'archive-a',
        title: 'Archive A',
        localPath: '/home/sumie/private/archive-a.mp4',
        relatedFiles: [
            {
                name: 'archive-a.log',
                path: '/home/sumie/private/archive-a.log',
                sizeBytes: 123,
            },
        ],
        suggestedUpload: {
            title: 'Archive A',
            description: '本地文件: /home/sumie/private/archive-a.mp4',
            cookieSourcePath: '/home/sumie/private/cookies.txt',
        },
    })
    const serialized = JSON.stringify(payload)

    expect(payload.localPath).toBe('[redacted]')
    expect(payload.relatedFiles[0].path).toBe('[redacted]')
    expect(payload.suggestedUpload.cookieSourcePath).toBe('[redacted]')
    expect(payload.suggestedUpload.description).toContain('[redacted-path]')
    expect(serialized).not.toContain('/home/sumie/private')
})

test('archive upload result redaction summarizes paths stdout and accepts redacted cookie defaults', () => {
    const payload = redactArchiveUploadResultForApi({
        ok: true,
        cookieSourcePath: '/tmp/private/cookies.txt',
        uploadedPath: 'D:\\private\\archive-a.mp4',
        trimmedPath: '/tmp/private/trimmed.mp4',
        coverPath: '/tmp/private/cover.jpg',
        stdout: 'private upload stdout',
    })
    const serialized = JSON.stringify(payload)

    expect(payload.cookieSourcePath).toBe('[redacted]')
    expect(payload.uploadedPath).toBe('[redacted]')
    expect(payload.trimmedPath).toBe('[redacted]')
    expect(payload.coverPath).toBe('[redacted]')
    expect(payload.stdout).toMatchObject({
        redacted_text: true,
        text_present: true,
    })
    expect(normalizeRedactedArchiveUploadRequest({ cookieSourcePath: '[redacted]', title: 'Archive A' })).toEqual({
        title: 'Archive A',
    })
    expect(serialized).not.toContain('/tmp/private')
    expect(serialized).not.toContain('D:\\private')
    expect(serialized).not.toContain('private upload stdout')
})

test('archive error redaction removes paths and urls', () => {
    const message = redactArchiveErrorMessageForApi(
        new Error('Failed to stage remote archive media: D:\\private\\archive-a.mp4 from https://example.test/private'),
    )

    expect(message).toContain('[redacted-path]')
    expect(message).toContain('[redacted-url]')
    expect(message).not.toContain('D:\\private')
    expect(message).not.toContain('example.test/private')
})
