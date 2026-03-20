import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { BiliForwarder } from './bilibili'
import { buildBiliupUploadCandidate, buildCookieDocument, normalizeBiliupCookieDocument, resolveVideoUploadConfig } from './biliup'

test('buildBiliupUploadCandidate prepares metadata for YouTube video uploads', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.YouTube,
            u_id: '22_7_channel',
            username: '22/7 official',
            a_id: 'yt-abc',
            content: '22/7 新视频标题\n\n这里是简介',
            created_at: 1710900000,
            url: 'https://www.youtube.com/watch?v=yt-abc',
        } as any,
        ['22/7 新视频标题\n\n这里是简介'],
        [
            { media_type: 'video_thumbnail', path: '/tmp/cover.jpg' },
            { media_type: 'video', path: '/tmp/video.mp4' },
        ],
        {
            enabled: true,
            tags: ['长视频', '22/7'],
        },
    )

    expect(candidate).toBeTruthy()
    expect(candidate?.title).toBe('22/7 新视频标题')
    expect(candidate?.coverPath).toBe('/tmp/cover.jpg')
    expect(candidate?.videoPaths).toEqual(['/tmp/video.mp4'])
    expect(candidate?.config.tags).toContain('YouTube')
    expect(candidate?.config.tags).toContain('长视频')
})

test('buildBiliupUploadCandidate skips excluded FC website feeds', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.Website,
            u_id: '22/7:movie',
            username: '22/7 FC Movie',
            a_id: 'movie-1',
            content: 'movie body',
            created_at: 1710900000,
            url: 'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447178?cd=nananiji_movie',
        } as any,
        ['movie body'],
        [{ media_type: 'video', path: '/tmp/movie.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate).toBeNull()
})

test('buildCookieDocument creates a biliup-compatible cookie scaffold', () => {
    const document = buildCookieDocument('sess-token', 'csrf-token')

    expect(document.cookie_info.cookies).toEqual([
        { name: 'SESSDATA', value: 'sess-token' },
        { name: 'bili_jct', value: 'csrf-token' },
    ])
    expect(document.token_info.mid).toBe(0)
})

test('normalizeBiliupCookieDocument preserves full exported cookie documents', () => {
    const document = normalizeBiliupCookieDocument({
        cookie_info: {
            cookies: [
                { name: 'SESSDATA', value: 'sess-token', http_only: 1 },
                { name: 'bili_jct', value: 'csrf-token' },
                { name: 'DedeUserID', value: '123456' },
            ],
            domains: ['.bilibili.com'],
        },
        token_info: {
            access_token: 'token',
            expires_in: 100,
            mid: 123456,
            refresh_token: 'refresh',
        },
        platform: 'BiliTV',
    })

    expect(document.cookie_info.cookies.map((cookie) => cookie.name)).toEqual(['SESSDATA', 'bili_jct', 'DedeUserID'])
    expect(document.token_info.mid).toBe(123456)
    expect(document.platform).toBe('BiliTV')
})

test('resolveVideoUploadConfig keeps configured biliup cookie file path', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-cookie-config-'))
    const cookieFile = path.join(tempRoot, 'cookies.json')
    fs.writeFileSync(cookieFile, JSON.stringify(buildCookieDocument('sess-token', 'csrf-token')))

    const config = resolveVideoUploadConfig({
        enabled: true,
        cookie_file: cookieFile,
    })

    expect(config?.cookie_file).toBe(cookieFile)
})

test('BiliForwarder skips dynamic posting when biliup upload succeeds', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let dynamicCalls = 0
    ;(forwarder as any).tryVideoUpload = async () => true
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return []
    }

    const result = await (forwarder as any).realSend(['hello'], {})

    expect(dynamicCalls).toBe(0)
    expect(result).toEqual([{ ok: true, mode: 'biliup' }])
})

test('BiliForwarder falls back to dynamic posting when biliup upload is skipped', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let dynamicCalls = 0
    ;(forwarder as any).tryVideoUpload = async () => false
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    const result = await (forwarder as any).realSend(['hello'], {})

    expect(dynamicCalls).toBe(1)
    expect(result).toEqual([{ ok: true, mode: 'dynamic' }])
})
