import { afterEach, expect, test } from 'bun:test'
import DB from '@/db'
import { Platform } from '@idol-bbq-utils/spider/types'
import fs from 'fs'
import os from 'os'
import path from 'path'
import {
    ensureSatoriCompatibleImage,
    buildVideoFingerprintBandKeys,
    buildShortVideoDedupCandidate,
    cleanupMediaCache,
    checkShortVideoCrossPlatformDuplicate,
    checkVideoFingerprintDuplicate,
    isPersistentMediaPath,
    markShortVideoCrossPlatformSeen,
    markVideoFingerprintSeen,
    persistMediaFile,
    type VideoFingerprintCandidate,
} from './media-cache-service'

const originalCheckExist = DB.MediaHash.checkExist
const originalFindByHashPrefix = DB.MediaHash.findByHashPrefix
const originalSave = DB.MediaHash.save
const originalGetSingleArticleByArticleCode = DB.Article.getSingleArticleByArticleCode
const createdPaths = new Set<string>()

afterEach(() => {
    DB.MediaHash.checkExist = originalCheckExist
    DB.MediaHash.findByHashPrefix = originalFindByHashPrefix
    DB.MediaHash.save = originalSave
    ;(DB.Article as any).getSingleArticleByArticleCode = originalGetSingleArticleByArticleCode
    for (const targetPath of createdPaths) {
        try {
            fs.rmSync(targetPath, { recursive: true, force: true })
        } catch {}
    }
    createdPaths.clear()
})

test('persistMediaFile moves downloaded media into a stable hash store with sidecar metadata', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'media-cache-test-'))
    createdPaths.add(tmpDir)
    const sourcePath = path.join(tmpDir, 'example.png')
    fs.writeFileSync(
        sourcePath,
        Buffer.from(
            'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9s1OtS8AAAAASUVORK5CYII=',
            'base64',
        ),
    )

    const stored = persistMediaFile(sourcePath, {
        media_type: 'photo',
        article: {
            a_id: 'ig-post-1',
            platform: Platform.Instagram,
            u_id: 'nananijigram22_7',
            username: '22/7',
            created_at: 1710000000,
            url: 'https://www.instagram.com/p/abc123/',
            type: 'post',
        } as any,
        source_url: 'https://cdn.example.com/photo.png',
    })

    expect(fs.existsSync(stored.path)).toBe(true)
    expect(fs.existsSync(`${stored.path}.json`)).toBe(true)
    createdPaths.add(stored.path)
    createdPaths.add(`${stored.path}.json`)
    expect(isPersistentMediaPath(stored.path)).toBe(true)
    expect(stored.hash).toHaveLength(64)
})

test('persistMediaFile strips the credential query from persisted source urls', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'media-cache-test-'))
    createdPaths.add(tmpDir)
    const sourcePath = path.join(tmpDir, 'clip.mp4')
    fs.writeFileSync(sourcePath, Buffer.from('fake-video-bytes'))

    const stored = persistMediaFile(sourcePath, {
        media_type: 'video',
        source_url:
            'https://pull-hls-example.invalid/game/stream-1000000000000000001_or4/index.m3u8?expire=1000000000&sign=0123456789abcdef0123456789abcdef',
    })
    createdPaths.add(stored.path)
    createdPaths.add(`${stored.path}.json`)

    expect(stored.source_urls).toEqual([
        'https://pull-hls-example.invalid/game/stream-1000000000000000001_or4/index.m3u8',
    ])
    const sidecar = JSON.parse(fs.readFileSync(`${stored.path}.json`, 'utf8'))
    expect(JSON.stringify(sidecar.source_urls)).not.toContain('sign=')
})

test('cleanupMediaCache removes expired stored media and transient downloads', () => {
    const cacheRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'media-cache-cleanup-test-'))
    createdPaths.add(cacheRoot)

    const oldStoreDir = path.join(cacheRoot, 'media', 'store', 'images', 'aa')
    const freshStoreDir = path.join(cacheRoot, 'media', 'store', 'images', 'bb')
    const oldDownloadDir = path.join(cacheRoot, 'media', 'yt-dlp')
    fs.mkdirSync(oldStoreDir, { recursive: true })
    fs.mkdirSync(freshStoreDir, { recursive: true })
    fs.mkdirSync(oldDownloadDir, { recursive: true })

    const oldStoreFile = path.join(oldStoreDir, 'old.jpg')
    const oldStoreMeta = `${oldStoreFile}.json`
    const freshStoreFile = path.join(freshStoreDir, 'fresh.jpg')
    const oldDownloadFile = path.join(oldDownloadDir, 'stale.part')
    fs.writeFileSync(oldStoreFile, 'old-store')
    fs.writeFileSync(oldStoreMeta, '{}')
    fs.writeFileSync(freshStoreFile, 'fresh-store')
    fs.writeFileSync(oldDownloadFile, 'old-download')

    const now = Date.now()
    const oldDate = new Date(now - 2 * 60 * 60 * 1000)
    fs.utimesSync(oldStoreFile, oldDate, oldDate)
    fs.utimesSync(oldStoreMeta, oldDate, oldDate)
    fs.utimesSync(oldDownloadFile, oldDate, oldDate)

    const summary = cleanupMediaCache({
        cacheRoot,
        nowMs: now,
        storeRetentionMs: 60 * 60 * 1000,
        downloadRetentionMs: 60 * 60 * 1000,
    })

    expect(fs.existsSync(oldStoreFile)).toBe(false)
    expect(fs.existsSync(oldStoreMeta)).toBe(false)
    expect(fs.existsSync(oldDownloadFile)).toBe(false)
    expect(fs.existsSync(freshStoreFile)).toBe(true)
    expect(summary.storeFilesDeleted).toBe(2)
    expect(summary.downloadFilesDeleted).toBe(1)
    expect(summary.errors).toBe(0)
})

test('cross-platform short video duration buckets require meaningful text', () => {
    const candidate = buildShortVideoDedupCandidate(
        {
            platform: Platform.X,
            type: 'tweet',
            a_id: 'x-short-1',
            created_at: 1710000000,
            u_id: '227_staff',
            username: '22/7 THE 3RD',
        } as any,
        [{ media_type: 'video', duration_seconds: 15.2 }],
    )
    expect(candidate).toBeNull()
})

test('instagram/tiktok short video fallback suppresses sparse-caption cross-platform duplicates', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    const articles = new Map<string, any>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) =>
        articles.get(`${platform}:${a_id}`)

    const instagramArticleData = {
        platform: Platform.Instagram,
        type: 'post',
        a_id: 'ig-sparse-reel',
        created_at: 1780826818,
        u_id: 'nananijigram22_7_the.3rd',
        username: '22/7_the 3rd',
    }
    articles.set(`${Platform.Instagram}:${instagramArticleData.a_id}`, instagramArticleData)
    const instagramArticle = buildShortVideoDedupCandidate(instagramArticleData as any, [
        { media_type: 'video', duration_seconds: 45.787 },
    ])
    expect(instagramArticle?.group).toBe('3rd')
    expect(instagramArticle?.text.keys).toEqual([])
    await markShortVideoCrossPlatformSeen(instagramArticle!)

    const tiktokArticle = buildShortVideoDedupCandidate(
        {
            platform: Platform.TikTok,
            type: 'video',
            a_id: 'tt-sparse-repost',
            created_at: 1780827050,
            u_id: 'the3rd_tiktok',
            username: '22/7_the 3rd',
        } as any,
        [{ media_type: 'video', duration_seconds: 45.766531 }],
    )
    expect(tiktokArticle?.text.keys).toEqual([])

    const duplicate = await checkShortVideoCrossPlatformDuplicate(tiktokArticle!)
    expect(duplicate?.a_id).toBe(`${Platform.Instagram}:ig-sparse-reel`)
})

test('instagram/tiktok short video fallback does not suppress same-platform sparse videos', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    const articles = new Map<string, any>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) =>
        articles.get(`${platform}:${a_id}`)

    const firstArticleData = {
        platform: Platform.Instagram,
        type: 'post',
        a_id: 'ig-sparse-first',
        created_at: 1780826818,
        u_id: 'nananijigram22_7_the.3rd',
        username: '22/7_the 3rd',
    }
    articles.set(`${Platform.Instagram}:${firstArticleData.a_id}`, firstArticleData)
    const first = buildShortVideoDedupCandidate(firstArticleData as any, [
        { media_type: 'video', duration_seconds: 45.787 },
    ])
    await markShortVideoCrossPlatformSeen(first!)

    const second = buildShortVideoDedupCandidate(
        {
            platform: Platform.Instagram,
            type: 'story',
            a_id: 'ig-sparse-second',
            created_at: 1780827050,
            u_id: 'nananijigram22_7_the.3rd',
            username: '22/7_the 3rd',
        } as any,
        [{ media_type: 'video', duration_seconds: 45.766531 }],
    )

    const duplicate = await checkShortVideoCrossPlatformDuplicate(second!)
    expect(duplicate).toBeNull()
})

test('cross-platform short video text and duration candidates suppress same content', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    const articles = new Map<string, any>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) =>
        articles.get(`${platform}:${a_id}`)

    const xArticleData = {
        platform: Platform.X,
        type: 'tweet',
        a_id: '2063561843692716187',
        created_at: 1780826457,
        u_id: '227_staff',
        username: '22/7(ナナブンノニジュウニ)',
        content: '22/7_the 3rd\n『＃叫ぶしかない青春』\nMusic Video公開中',
    }
    articles.set(`${Platform.X}:${xArticleData.a_id}`, xArticleData)
    const xArticle = buildShortVideoDedupCandidate(xArticleData as any, [
        { media_type: 'video', duration_seconds: 45.766531 },
    ])
    expect(xArticle?.group).toBe('3rd')
    await markShortVideoCrossPlatformSeen(xArticle!)

    const instagramArticle = buildShortVideoDedupCandidate(
        {
            platform: Platform.Instagram,
            type: 'post',
            a_id: 'DZR9nGHxnvu',
            created_at: 1780826818,
            u_id: 'nananijigram22_7_the.3rd',
            username: '22/7_the 3rd',
            content: '. 22/7_the 3rd 『＃叫ぶしかない青春』 Music Video公開中',
        } as any,
        [{ media_type: 'video', duration_seconds: 45.787 }],
    )
    expect(instagramArticle?.group).toBe('3rd')

    const duplicate = await checkShortVideoCrossPlatformDuplicate(instagramArticle!)
    expect(duplicate?.a_id).toBe(`${Platform.X}:2063561843692716187`)
})

test('cross-platform short video text candidates ignore different captions in nearby buckets', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    const articles = new Map<string, any>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) =>
        articles.get(`${platform}:${a_id}`)

    const firstArticleData = {
        platform: Platform.X,
        type: 'tweet',
        a_id: 'x-mv-promo',
        created_at: 1780826457,
        u_id: '227_staff',
        username: '22/7(ナナブンノニジュウニ)',
        content: '22/7_the 3rd\n『＃叫ぶしかない青春』\nMusic Video公開中',
    }
    articles.set(`${Platform.X}:${firstArticleData.a_id}`, firstArticleData)
    const first = buildShortVideoDedupCandidate(firstArticleData as any, [
        { media_type: 'video', duration_seconds: 45.766531 },
    ])
    await markShortVideoCrossPlatformSeen(first!)

    const second = buildShortVideoDedupCandidate(
        {
            platform: Platform.Instagram,
            type: 'post',
            a_id: 'ig-making',
            created_at: 1780826818,
            u_id: 'nananijigram22_7_the.3rd',
            username: '22/7_the 3rd',
            content: '22/7_the 3rd 新衣装メイキング映像を公開しました',
        } as any,
        [{ media_type: 'video', duration_seconds: 45.787 }],
    )
    expect(second).toBeTruthy()

    const duplicate = await checkShortVideoCrossPlatformDuplicate(second!)
    expect(duplicate).toBeNull()
})

test('video fingerprint dedup matches re-encoded short videos by frame bands', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }

    const storagePlatform = 'cross-video-fingerprint:227-official'
    const firstFrameHashes = [
        '1234abcd5678ef90',
        '2345bcde6789f0a1',
        '3456cdef7890a1b2',
        '4567def08901b2c3',
        '5678ef019012c3d4',
    ]
    const first: VideoFingerprintCandidate = {
        storagePlatform,
        articleMarker: `${Platform.TikTok}:tt-short-1`,
        signature: `exact:40:${firstFrameHashes.join(':')}`,
        bandKeys: buildVideoFingerprintBandKeys(40, firstFrameHashes),
        duration_seconds: 20.1,
        group: '227-official',
    }
    await markVideoFingerprintSeen(first)

    const reencodedFrameHashes = [
        '1234abcd5678ef91',
        '2345bcde6789f0a2',
        '3456cdef7890a1b3',
        '4567def08901b2c4',
        '5678ef019012c3d5',
    ]
    const second: VideoFingerprintCandidate = {
        storagePlatform,
        articleMarker: `${Platform.YouTube}:yt-short-1`,
        signature: `exact:40:${reencodedFrameHashes.join(':')}`,
        bandKeys: buildVideoFingerprintBandKeys(40, reencodedFrameHashes),
        duration_seconds: 20.2,
        group: '227-official',
    }

    const duplicate = await checkVideoFingerprintDuplicate(second)
    expect(duplicate?.a_id).toBe(`${Platform.TikTok}:tt-short-1`)
})

test('video fingerprint dedup matches the same IG/TT video across different account groups', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }

    // IG version posted by the nijigram account group.
    const igFrameHashes = [
        'ff07a0f0780880ff',
        'a0f0780880ffc9a1',
        '780880ffc9a160c1',
        '880ffc9a160c130bf',
        'ffc9a160c130bfff1',
    ]
    const igCandidate: VideoFingerprintCandidate = {
        storagePlatform: 'cross-video-fingerprint:nijigram',
        articleMarker: `${Platform.Instagram}:ig-fancam`,
        signature: `exact:27:${igFrameHashes.join(':')}`,
        bandKeys: buildVideoFingerprintBandKeys(27, igFrameHashes),
        duration_seconds: 13.5,
        group: 'nijigram',
        crossPlatformStoragePlatform: 'cross-video-fingerprint:ig-tt',
        crossPlatformBandKeys: buildVideoFingerprintBandKeys(27, igFrameHashes),
    }
    await markVideoFingerprintSeen(igCandidate)

    // Same video on TikTok under the official account group: frame 0 identical, rest slightly re-encoded.
    const ttFrameHashes = [
        'ff07a0f0780880ff',
        'a0f0780880ff9849',
        '780880ff9849d8d9',
        '880ff9849d8d900ff',
        'ff9849d8d900fff3',
    ]
    const ttCandidate: VideoFingerprintCandidate = {
        storagePlatform: 'cross-video-fingerprint:227-official',
        articleMarker: `${Platform.TikTok}:tt-fancam`,
        signature: `exact:27:${ttFrameHashes.join(':')}`,
        bandKeys: buildVideoFingerprintBandKeys(27, ttFrameHashes),
        duration_seconds: 13.6,
        group: '227-official',
        crossPlatformStoragePlatform: 'cross-video-fingerprint:ig-tt',
        crossPlatformBandKeys: buildVideoFingerprintBandKeys(27, ttFrameHashes),
    }

    const duplicate = await checkVideoFingerprintDuplicate(ttCandidate)
    expect(duplicate?.a_id).toBe(`${Platform.Instagram}:ig-fancam`)
})

test('video fingerprint cross-platform namespace is skipped for non IG/TT platforms', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }

    const frameHashes = [
        '1234abcd5678ef90',
        '2345bcde6789f0a1',
        '3456cdef7890a1b2',
        '4567def08901b2c3',
        '5678ef019012c3d4',
    ]
    const xCandidate: VideoFingerprintCandidate = {
        storagePlatform: 'cross-video-fingerprint:227-official',
        articleMarker: `${Platform.X}:x-teaser`,
        signature: `exact:40:${frameHashes.join(':')}`,
        bandKeys: buildVideoFingerprintBandKeys(40, frameHashes),
        duration_seconds: 20.1,
        group: '227-official',
    }
    await markVideoFingerprintSeen(xCandidate)

    expect(store.has('cross-video-fingerprint:ig-tt:exact:40:1234abcd5678ef90:2345bcde6789f0a1:3456cdef7890a1b2:4567def08901b2c3:5678ef019012c3d4')).toBe(false)
})

test('video fingerprint dedup matches YouTube Shorts against the same IG video', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }

    const igFrameHashes = [
        'ff07a0f0780880ff',
        'a0f0780880ffc9a1',
        '780880ffc9a160c1',
        '880ffc9a160c130bf',
        'ffc9a160c130bfff1',
    ]
    const igCandidate: VideoFingerprintCandidate = {
        storagePlatform: 'cross-video-fingerprint:nijigram',
        articleMarker: `${Platform.Instagram}:ig-short`,
        signature: `exact:27:${igFrameHashes.join(':')}`,
        bandKeys: buildVideoFingerprintBandKeys(27, igFrameHashes),
        duration_seconds: 13.5,
        group: 'nijigram',
        crossPlatformStoragePlatform: 'cross-video-fingerprint:ig-tt',
        crossPlatformBandKeys: buildVideoFingerprintBandKeys(27, igFrameHashes),
    }
    await markVideoFingerprintSeen(igCandidate)

    const ytShortsFrameHashes = [
        'ff07a0f0780880ff',
        'a0f0780880ff9849',
        '780880ff9849d8d9',
        '880ff9849d8d900ff',
        'ff9849d8d900fff3',
    ]
    const ytCandidate: VideoFingerprintCandidate = {
        storagePlatform: 'cross-video-fingerprint:227-official',
        articleMarker: `${Platform.YouTube}:yt-short`,
        signature: `exact:27:${ytShortsFrameHashes.join(':')}`,
        bandKeys: buildVideoFingerprintBandKeys(27, ytShortsFrameHashes),
        duration_seconds: 13.6,
        group: '227-official',
        crossPlatformStoragePlatform: 'cross-video-fingerprint:ig-tt',
        crossPlatformBandKeys: buildVideoFingerprintBandKeys(27, ytShortsFrameHashes),
    }

    const duplicate = await checkVideoFingerprintDuplicate(ytCandidate)
    expect(duplicate?.a_id).toBe(`${Platform.Instagram}:ig-short`)
})

test('video fingerprint cross-platform namespace is skipped for YouTube long videos', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }

    const frameHashes = [
        '1234abcd5678ef90',
        '2345bcde6789f0a1',
        '3456cdef7890a1b2',
        '4567def08901b2c3',
        '5678ef019012c3d4',
    ]
    const ytLongCandidate: VideoFingerprintCandidate = {
        storagePlatform: 'cross-video-fingerprint:227-official',
        articleMarker: `${Platform.YouTube}:yt-long`,
        signature: `exact:40:${frameHashes.join(':')}`,
        bandKeys: buildVideoFingerprintBandKeys(40, frameHashes),
        duration_seconds: 20.1,
        group: '227-official',
    }
    await markVideoFingerprintSeen(ytLongCandidate)

    expect(store.has('cross-video-fingerprint:ig-tt:exact:40:1234abcd5678ef90:2345bcde6789f0a1:3456cdef7890a1b2:4567def08901b2c3:5678ef019012c3d4')).toBe(false)
})

test('short-video IG/TT fallback signature is shared across account groups', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    const originalGetArticle = DB.Article.getSingleArticleByArticleCode
    DB.Article.getSingleArticleByArticleCode = (async () => ({
        platform: Platform.TikTok,
        a_id: 'tt-fancam',
        u_id: '227official',
        username: '22/7(ナナブンノニジュウニ)',
        created_at: 1780826818,
        content: '#推しカメラ 何でも全力投球！ #南伊織 #小田原大合戦 #ナナニジ',
        type: 'post',
    })) as any

    const igCandidate = buildShortVideoDedupCandidate(
        {
            platform: Platform.Instagram,
            type: 'post',
            a_id: 'ig-fancam',
            created_at: 1780827000,
            u_id: 'nananijigram22_7',
            username: '22/7(ナナブンノニジュウニ)',
            content: '推しカメラ 何でも全力投球！ 南伊織 小田原大合戦 ナナニジ',
        } as any,
        [{ media_type: 'video', duration_seconds: 13.5 }],
    )
    expect(igCandidate?.crossPlatformStoragePlatform).toBe('cross-short-video:ig-tt')
    await markShortVideoCrossPlatformSeen(igCandidate!)

    const ttCandidate = buildShortVideoDedupCandidate(
        {
            platform: Platform.TikTok,
            type: 'post',
            a_id: 'tt-fancam',
            created_at: 1780827010,
            u_id: '227official',
            username: '22/7(ナナブンノニジュウニ)',
            content: '#推しカメラ 何でも全力投球！ #南伊織 #小田原大合戦 #ナナニジ',
        } as any,
        [{ media_type: 'video', duration_seconds: 13.6 }],
    )
    expect(ttCandidate?.crossPlatformStoragePlatform).toBe('cross-short-video:ig-tt')

    const duplicate = await checkShortVideoCrossPlatformDuplicate(ttCandidate!)
    DB.Article.getSingleArticleByArticleCode = originalGetArticle
    expect(duplicate?.a_id).toBe('2:ig-fancam')
})

test('short-video fallback signature dedups YouTube Shorts against an IG video', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    const originalGetArticle = DB.Article.getSingleArticleByArticleCode
    DB.Article.getSingleArticleByArticleCode = (async () => ({
        platform: Platform.Instagram,
        a_id: 'ig-fancam',
        u_id: 'nananijigram22_7',
        username: '22/7(ナナブンノニジュウニ)',
        created_at: 1780826818,
        content: '推しカメラ 何でも全力投球！ 南伊織 小田原大合戦 ナナニジ',
        type: 'post',
    })) as any

    const igCandidate = buildShortVideoDedupCandidate(
        {
            platform: Platform.Instagram,
            type: 'post',
            a_id: 'ig-fancam',
            created_at: 1780827000,
            u_id: 'nananijigram22_7',
            username: '22/7(ナナブンノニジュウニ)',
            content: '推しカメラ 何でも全力投球！ 南伊織 小田原大合戦 ナナニジ',
        } as any,
        [{ media_type: 'video', duration_seconds: 13.5 }],
    )
    await markShortVideoCrossPlatformSeen(igCandidate!)

    const ytCandidate = buildShortVideoDedupCandidate(
        {
            platform: Platform.YouTube,
            type: 'shorts',
            a_id: 'yt-fancam',
            created_at: 1780827010,
            u_id: '227SMEJ',
            username: '22/7 OFFICIAL',
            content: '推しカメラ 何でも全力投球！ 南伊織 小田原大合戦 ナナニジ',
        } as any,
        [{ media_type: 'video', duration_seconds: 13.6 }],
    )
    expect(ytCandidate?.crossPlatformStoragePlatform).toBe('cross-short-video:ig-tt')

    const duplicate = await checkShortVideoCrossPlatformDuplicate(ytCandidate!)
    DB.Article.getSingleArticleByArticleCode = originalGetArticle
    expect(duplicate?.a_id).toBe('2:ig-fancam')
})

test('video fingerprint dedup ignores low-information repeated frame bands', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }

    const storagePlatform = 'cross-video-fingerprint:3rd'
    const lowInfoFrameHashes = [
        'ffff3e0000000000',
        'ffff3e0000000000',
        'ffff3e0000000000',
        'ffff3e0000000000',
        'ffff3e0000000000',
    ]
    const staleLowInfo: VideoFingerprintCandidate = {
        storagePlatform,
        articleMarker: `${Platform.Instagram}:old-story`,
        signature: `exact:90:${lowInfoFrameHashes.join(':')}`,
        bandKeys: [
            ...buildVideoFingerprintBandKeys(90, lowInfoFrameHashes),
            'band:90:f0:b0:ffff',
            'band:90:f0:b2:0000',
        ],
        duration_seconds: 45,
        group: '3rd',
    }
    await markVideoFingerprintSeen(staleLowInfo)

    const candidateBandKeys = buildVideoFingerprintBandKeys(90, lowInfoFrameHashes)
    expect(candidateBandKeys.some((key) => key.endsWith(':ffff') || key.endsWith(':0000'))).toBe(false)
    expect(candidateBandKeys.length).toBeLessThan(8)

    const duplicate = await checkVideoFingerprintDuplicate({
        storagePlatform,
        articleMarker: `${Platform.Instagram}:new-story`,
        signature: `exact:90:different-low-info-signature`,
        bandKeys: candidateBandKeys,
        duration_seconds: 45,
        group: '3rd',
    })
    expect(duplicate).toBeNull()
})

test('ensureSatoriCompatibleImage transcodes webp content despite a .jpg extension', () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'satori-compat-'))
    const pngPath = path.join(tempDir, 'base.png')
    fs.writeFileSync(
        pngPath,
        Buffer.from(
            'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
            'base64',
        ),
    )
    const webpPath = path.join(tempDir, 'thumb.jpg')
    // The local ffmpeg build has no webp encoder; produce the webp fixture with PIL.
    const { execFileSync } = require('child_process')
    execFileSync('python3', ['-c', `from PIL import Image; Image.open(${JSON.stringify(pngPath)}).save(${JSON.stringify(webpPath)}, 'WEBP')`], {
        stdio: 'ignore',
    })
    // Sanity: the file really is RIFF/WEBP content now.
    const sniffed = fs.readFileSync(webpPath)
    expect(sniffed.toString('ascii', 0, 4)).toBe('RIFF')
    expect(sniffed.toString('ascii', 8, 12)).toBe('WEBP')

    const converted = ensureSatoriCompatibleImage(webpPath)
    expect(converted).not.toBe(webpPath)
    const out = fs.readFileSync(converted)
    expect(out.readUInt32BE(0)).toBe(0x89504e47)
    // Idempotent.
    expect(ensureSatoriCompatibleImage(webpPath)).toBe(converted)
    // PNG input passes through untouched.
    expect(ensureSatoriCompatibleImage(pngPath)).toBe(pngPath)
})

test('unbucketed token recall catches cross-posts days apart that the bucketed signature misses', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    const articles = new Map<string, any>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) =>
        articles.get(`${platform}:${a_id}`)

    // short captions: no token >= 6 chars, compact differs -> legacy text keys
    // never collide; two days apart -> even the coarse IG/TT fallback is out of
    // its ±1 x 6h bucket range
    const igData = {
        platform: Platform.Instagram,
        type: 'post',
        a_id: 'ig-recall',
        created_at: 1780000000,
        u_id: 'nananijigram22_7_the.3rd',
        username: '22/7_the 3rd',
        content: '新曲「明日の風」公開中！#ナナニジ',
    }
    articles.set(`${Platform.Instagram}:${igData.a_id}`, igData)
    const ig = buildShortVideoDedupCandidate(igData as any, [{ media_type: 'video', duration_seconds: 45.8 }])
    expect(ig).not.toBeNull()
    expect(ig!.text.keys).toEqual([]) // legacy path has nothing to key on
    await markShortVideoCrossPlatformSeen(ig!)
    // recall keys were stored unbucketed
    expect(ig!.recallKeysToStore.length).toBeGreaterThan(0)
    expect([...store.keys()].some((k) => k.includes(':ut:'))).toBe(true)

    const tt = buildShortVideoDedupCandidate(
        {
            platform: Platform.TikTok,
            type: 'video',
            a_id: 'tt-recall',
            created_at: 1780000000 + 2 * 24 * 3600, // two days later
            u_id: 'the3rd_tiktok',
            username: '22/7_the 3rd',
            content: '明日の風 新曲 みんな見てね #TikTok',
        } as any,
        [{ media_type: 'video', duration_seconds: 45.8 }],
    )
    expect(tt).not.toBeNull()
    const duplicate = await checkShortVideoCrossPlatformDuplicate(tt!)
    expect(duplicate?.a_id).toBe(`${Platform.Instagram}:ig-recall`)
})

test('unbucketed recall still rejects genuinely different videos on the IG/TT pair', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    const articles = new Map<string, any>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.findByHashPrefix = async (platform: string, prefix: string) =>
        [...store.values()].filter((v: any) => v.platform === platform && v.hash.startsWith(prefix)) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) =>
        articles.get(`${platform}:${a_id}`)

    const igData = {
        platform: Platform.Instagram,
        type: 'post',
        a_id: 'ig-cooking',
        created_at: 1780000000,
        u_id: 'nananijigram22_7_the.3rd',
        username: '22/7_the 3rd',
        content: '料理動画、カレーを作ったよ',
    }
    articles.set(`${Platform.Instagram}:${igData.a_id}`, igData)
    const ig = buildShortVideoDedupCandidate(igData as any, [{ media_type: 'video', duration_seconds: 30 }])
    await markShortVideoCrossPlatformSeen(ig!)

    const tt = buildShortVideoDedupCandidate(
        {
            platform: Platform.TikTok,
            type: 'video',
            a_id: 'tt-dance',
            created_at: 1780000100,
            u_id: 'the3rd_tiktok',
            username: '22/7_the 3rd',
            content: 'ダンス練習の動画です',
        } as any,
        [{ media_type: 'video', duration_seconds: 30 }],
    )
    expect(await checkShortVideoCrossPlatformDuplicate(tt!)).toBeNull()
})
