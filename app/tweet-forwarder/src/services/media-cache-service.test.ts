import { afterEach, expect, test } from 'bun:test'
import DB from '@/db'
import { Platform } from '@idol-bbq-utils/spider/types'
import fs from 'fs'
import os from 'os'
import path from 'path'
import {
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
const originalSave = DB.MediaHash.save
const originalGetSingleArticleByArticleCode = DB.Article.getSingleArticleByArticleCode
const createdPaths = new Set<string>()

afterEach(() => {
    DB.MediaHash.checkExist = originalCheckExist
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
            platform: Platform.Instagram,
            type: 'post',
            a_id: 'ig-short-1',
            created_at: 1710000000,
            u_id: 'nananijigram22_7_the.3rd',
            username: '22/7 THE 3RD',
        } as any,
        [{ media_type: 'video', duration_seconds: 15.2 }],
    )
    expect(candidate).toBeNull()
})

test('cross-platform short video text and duration candidates suppress same content', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    const articles = new Map<string, any>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
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

test('video fingerprint dedup ignores low-information repeated frame bands', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
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
