import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { BiliForwarder } from './bilibili'
import {
    buildBiliupUploadCandidate,
    buildCookieDocument,
    normalizeBiliupCookieDocument,
    prepareUploadVideoParts,
    resolveBrowserCookieSyncConfig,
    resolveVideoUploadConfig,
} from './biliup'

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

test('buildBiliupUploadCandidate prepares branded metadata for Instagram uploads without text', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.Instagram,
            type: 'post',
            u_id: 'satsuki_shiina',
            username: '椎名桜月',
            a_id: 'ig-live-replay',
            content: null,
            created_at: 1773985020,
            url: 'https://www.instagram.com/p/example/',
        } as any,
        [],
        [{ media_type: 'video', path: '/tmp/replay.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate?.title).toBe('【Instagram投稿】椎名桜月 2026-03-20 14:37')
    expect(candidate?.description).toContain('来源平台: Instagram投稿')
    expect(candidate?.description).toContain('来源账号: 椎名桜月')
    expect(candidate?.description).toContain('账号标识: satsuki_shiina')
    expect(candidate?.description).toContain('原链接: https://www.instagram.com/p/example/')
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

test('resolveBrowserCookieSyncConfig keeps browser profile sync settings', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-cookie-sync-'))
    const helperScript = path.join(tempRoot, 'export-biliup-browser-cookies.ts')
    fs.writeFileSync(helperScript, 'console.log("ok")')

    const config = resolveBrowserCookieSyncConfig({
        enabled: true,
        session_profile: 'bilibili-uploader',
        script_path: helperScript,
        url: 'https://www.bilibili.com',
        browser_mode: 'headed-xvfb',
    })

    expect(config?.session_profile).toBe('bilibili-uploader')
    expect(config?.script_path).toBe(helperScript)
    expect(config?.browser_mode).toBe('headed-xvfb')
})

test('resolveVideoUploadConfig includes browser cookie sync settings', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-video-upload-sync-'))
    const helperScript = path.join(tempRoot, 'export-biliup-browser-cookies.ts')
    fs.writeFileSync(helperScript, 'console.log("ok")')

    const config = resolveVideoUploadConfig({
        enabled: true,
        cookie_file: path.join(tempRoot, 'cookies.json'),
        browser_cookie_sync: {
            enabled: true,
            session_profile: 'bilibili-uploader',
            script_path: helperScript,
        },
    })

    expect(config?.browser_cookie_sync?.session_profile).toBe('bilibili-uploader')
    expect(config?.browser_cookie_sync?.script_path).toBe(helperScript)
})

test('resolveVideoUploadConfig keeps metadata template and collision placeholder settings', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-video-upload-template-'))
    const videoPath = path.join(tempRoot, 'pad.mp4')
    fs.writeFileSync(videoPath, 'video')
    const config = resolveVideoUploadConfig({
        enabled: true,
        metadata_templates: {
            title: '【{{platform_type_label}}】{{display_name}} {{summary}}',
            description: '{{body_or_summary}}\n\n原链接: {{url}}',
        },
        collision_placeholder_part: {
            enabled: true,
            video_path: videoPath,
            image_path: path.join(tempRoot, 'logo.png'),
            title: '###',
            background_color: '#d1e5fc',
        },
    })

    expect(config?.metadata_templates?.title).toBe('【{{platform_type_label}}】{{display_name}} {{summary}}')
    expect(config?.collision_placeholder_part?.video_path).toBe(videoPath)
    expect(config?.collision_placeholder_part?.title).toBe('###')
    expect(config?.collision_placeholder_part?.background_color).toBe('#d1e5fc')
})

test('resolveVideoUploadConfig falls back from invalid numeric control values', () => {
    const config = resolveVideoUploadConfig({
        enabled: true,
        tid: 'wat' as any,
        threads: 'wat' as any,
        collision_placeholder_part: {
            enabled: true,
            duration_seconds: 'wat' as any,
            width: 'wat' as any,
            height: 'wat' as any,
            fps: 'wat' as any,
        },
    })

    expect(config?.tid).toBe(171)
    expect(config?.threads).toBe(3)
    expect(config?.collision_placeholder_part?.duration_seconds).toBe(2)
    expect(config?.collision_placeholder_part?.width).toBe(1920)
    expect(config?.collision_placeholder_part?.height).toBe(1080)
    expect(config?.collision_placeholder_part?.fps).toBe(30)
})

test('prepareUploadVideoParts reuses persistent collision placeholder video when configured', async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-video-parts-persistent-'))
    const uploadDir = path.join(tempRoot, 'upload')
    fs.mkdirSync(uploadDir, { recursive: true })

    const videoPath = path.join(tempRoot, 'source-video.mp4')
    fs.writeFileSync(videoPath, 'video')

    const placeholderPath = path.join(tempRoot, 'collision-pad.mp4')
    fs.writeFileSync(placeholderPath, 'pad')

    const parts = await prepareUploadVideoParts(
        {
            videoPaths: [videoPath],
            config: {
                enabled: true,
                python_path: 'python3',
                helper_path: '/tmp/helper.py',
                working_dir: tempRoot,
                submit_api: 'web',
                line: 'AUTO',
                tid: 171,
                threads: 3,
                copyright: 2,
                tags: [],
                exclude_uids: [],
                collision_placeholder_part: {
                    enabled: true,
                    video_path: placeholderPath,
                    image_path: path.join(tempRoot, 'unused.png'),
                    title: '###',
                    duration_seconds: 7,
                    width: 1920,
                    height: 1080,
                    fps: 30,
                    ffmpeg_path: '/usr/bin/ffmpeg',
                    background_color: '#d1e5fc',
                },
            },
        },
        uploadDir,
    )

    expect(parts.map((part) => path.basename(part.stagedPath))).toEqual(['正片.mp4', '###.mp4'])
    expect(fs.existsSync(parts[1]!.stagedPath)).toBe(true)
})

test('prepareUploadVideoParts appends collision placeholder part with clean multi-p titles', async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-video-parts-'))
    const uploadDir = path.join(tempRoot, 'upload')
    fs.mkdirSync(uploadDir, { recursive: true })

    const logoPath = path.join(tempRoot, 'logo.png')
    fs.writeFileSync(logoPath, 'logo')

    const videoPath = path.join(tempRoot, 'source-video.mp4')
    fs.writeFileSync(videoPath, 'video')

    const ffmpegPath = path.join(tempRoot, 'ffmpeg')
    fs.writeFileSync(
        ffmpegPath,
        `#!/bin/sh
out=""
for arg in "$@"; do
  out="$arg"
done
printf 'placeholder' > "$out"
`,
        { mode: 0o755 },
    )

    const parts = await prepareUploadVideoParts(
        {
            videoPaths: [videoPath],
            config: {
                enabled: true,
                python_path: 'python3',
                helper_path: '/tmp/helper.py',
                working_dir: tempRoot,
                submit_api: 'web',
                line: 'AUTO',
                tid: 171,
                threads: 3,
                copyright: 2,
                tags: [],
                exclude_uids: [],
                collision_placeholder_part: {
                    enabled: true,
                    image_path: logoPath,
                    title: '###',
                    duration_seconds: 1,
                    width: 1280,
                    height: 720,
                    fps: 24,
                    ffmpeg_path: ffmpegPath,
                    background_color: '#d1e5fc',
                },
            },
        },
        uploadDir,
    )

    expect(parts.map((part) => path.basename(part.stagedPath))).toEqual(['正片.mp4', '###.mp4'])
    expect(parts[1]?.partTitle).toBe('###')
    expect(fs.existsSync(parts[1]!.stagedPath)).toBe(true)
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

test('BiliForwarder tightens X action header spacing for Bilibili posts', async () => {
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

    let uploadTexts: string[] = []
    let dynamicTexts: string[] = []
    ;(forwarder as any).tryVideoUpload = async (texts: string[]) => {
        uploadTexts = texts
        return false
    }
    ;(forwarder as any).sendDynamicContent = async (texts: string[]) => {
        dynamicTexts = texts
        return [{ ok: true, mode: 'dynamic' }]
    }

    await (forwarder as any).realSend(
        [
            '@member 0203⁹ X发推\n\n本文\n\nmember 0203⁹（260101） X 发推',
            '@member 0204⁹ X引用\n\n引用本文\n\nmember 0204⁹（260101） X 引用',
        ],
        {},
    )

    expect(uploadTexts[0]).toContain('@member 0203⁹ X发推:\n本文')
    expect(dynamicTexts[0]).toContain('@member 0203⁹ X发推:\n本文')
    expect(dynamicTexts[0]).not.toContain('X发推\n\n本文')
    expect(uploadTexts[1]).toContain('@member 0204⁹ X引用:\n引用本文')
    expect(dynamicTexts[1]).toContain('@member 0204⁹ X引用:\n引用本文')
    expect(dynamicTexts[1]).not.toContain('X引用\n\n引用本文')
})
