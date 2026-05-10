import { afterEach, expect, test } from 'bun:test'
import DB from '@/db'
import { Platform } from '@idol-bbq-utils/spider/types'
import fs from 'fs'
import os from 'os'
import path from 'path'
import {
    buildVideoFingerprintBandKeys,
    buildShortVideoDedupCandidate,
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
const createdPaths = new Set<string>()

afterEach(() => {
    DB.MediaHash.checkExist = originalCheckExist
    DB.MediaHash.save = originalSave
    for (const targetPath of createdPaths) {
        try {
            fs.rmSync(targetPath, { recursive: true, force: true })
        } catch { }
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
    expect(isPersistentMediaPath(stored.path)).toBe(true)
    expect(stored.hash).toHaveLength(64)
})

test('cross-platform short video dedup uses duration buckets for nijigram-like accounts', async () => {
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }

    const first = buildShortVideoDedupCandidate(
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
    expect(first).toBeTruthy()
    await markShortVideoCrossPlatformSeen(first!)

    const second = buildShortVideoDedupCandidate(
        {
            platform: Platform.YouTube,
            type: 'shorts',
            a_id: 'yt-short-1',
            created_at: 1710000300,
            u_id: 'nananijigram22_7_the.3rd',
            username: '22/7 THE 3RD',
        } as any,
        [{ media_type: 'video', duration_seconds: 15.6 }],
    )
    expect(second).toBeTruthy()

    const duplicate = await checkShortVideoCrossPlatformDuplicate(second!)
    expect(duplicate?.a_id).toBe(`${Platform.Instagram}:ig-short-1`)
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
        'ffff0000ffff0000',
        '0000ffff0000ffff',
        'f0f0f0f00f0f0f0f',
        'cccc3333cccc3333',
        'aaaa5555aaaa5555',
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
        'ffff0000ffff1111',
        '0000ffff0000eeee',
        'f0f0f0f00f0f2222',
        'cccc3333cccc4444',
        'aaaa5555aaaa6666',
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
