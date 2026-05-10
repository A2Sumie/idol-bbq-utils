import { CACHE_DIR_ROOT } from '@/config'
import type { Article } from '@/db'
import DB from '@/db'
import { Platform, type MediaType } from '@idol-bbq-utils/spider/types'
import { execFileSync } from 'child_process'
import crypto from 'crypto'
import fs from 'fs'
import path from 'path'

const MEDIA_STORE_ROOT = path.join(CACHE_DIR_ROOT, 'media', 'store')
const MEDIA_STORE_VIDEO_ROOT = path.join(MEDIA_STORE_ROOT, 'videos')
const MEDIA_STORE_IMAGE_ROOT = path.join(MEDIA_STORE_ROOT, 'images')
const EXACT_CROSS_PLATFORM_VIDEO_PLATFORM = 'cross-platform-video'
const EXACT_CROSS_PLATFORM_MEDIA_PREFIX = 'cross-platform-media'
const VIDEO_FINGERPRINT_PLATFORM_PREFIX = 'cross-video-fingerprint'
const SHORT_VIDEO_MAX_DURATION_SECONDS = 180
const SHORT_VIDEO_DURATION_BUCKET_MS = 500
const SHORT_VIDEO_DURATION_TOLERANCE_BUCKETS = 2
const SHORT_VIDEO_TIME_BUCKET_SECONDS = 6 * 3600
const VIDEO_FINGERPRINT_SAMPLE_RATIOS = [0.12, 0.3, 0.5, 0.7, 0.88]
const VIDEO_FINGERPRINT_BAND_SIZE = 4
const VIDEO_FINGERPRINT_MIN_BAND_MATCHES = 8

type MediaStoreArticleLike = Pick<Article, 'a_id' | 'platform' | 'u_id' | 'username' | 'created_at' | 'url' | 'type'>

interface StoredMediaMetadata {
    path: string
    hash: string
    media_type: MediaType
    size_bytes: number
    duration_seconds?: number
    source_urls: Array<string>
    article_markers: Array<string>
    article_urls: Array<string>
    platforms: Array<string>
    u_ids: Array<string>
    usernames: Array<string>
    created_at: number
    updated_at: number
}

interface PersistMediaFileOptions {
    article?: MediaStoreArticleLike
    media_type: MediaType
    source_url?: string
}

interface ShortVideoDedupCandidate {
    storagePlatform: string
    articleMarker: string
    signature: string
    signaturesToCheck: Array<string>
    duration_seconds: number
    group: string
}

interface VideoFingerprintCandidate {
    storagePlatform: string
    articleMarker: string
    signature: string
    bandKeys: Array<string>
    duration_seconds: number
    group: string
}

function ensureDirectory(dirPath: string) {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true })
    }
}

function normalizeExtension(value: string) {
    return value.replace(/^\./, '').trim().toLowerCase()
}

function dedupeStrings(values: Array<string | undefined | null>) {
    return Array.from(new Set(values.map((value) => (value || '').trim()).filter(Boolean)))
}

function buildArticleMarker(article: Pick<Article, 'platform' | 'a_id'>) {
    return `${String(article.platform)}:${article.a_id}`
}

function parseStoredMediaMetadata(metadataPath: string): StoredMediaMetadata | null {
    if (!fs.existsSync(metadataPath)) {
        return null
    }
    try {
        return JSON.parse(fs.readFileSync(metadataPath, 'utf8')) as StoredMediaMetadata
    } catch {
        return null
    }
}

function writeStoredMediaMetadata(metadataPath: string, metadata: StoredMediaMetadata) {
    const tempPath = `${metadataPath}.tmp`
    fs.writeFileSync(tempPath, JSON.stringify(metadata, null, 2))
    fs.renameSync(tempPath, metadataPath)
}

function resolveStoreRoot(mediaType: MediaType) {
    return mediaType === 'video' ? MEDIA_STORE_VIDEO_ROOT : MEDIA_STORE_IMAGE_ROOT
}

function resolveStoredMediaPath(hash: string, extension: string, mediaType: MediaType) {
    const dir = path.join(resolveStoreRoot(mediaType), hash.slice(0, 2))
    ensureDirectory(dir)
    return path.join(dir, extension ? `${hash}.${extension}` : hash)
}

function moveIntoStore(sourcePath: string, destinationPath: string) {
    if (sourcePath === destinationPath) {
        return
    }
    if (fs.existsSync(destinationPath)) {
        if (fs.existsSync(sourcePath)) {
            fs.unlinkSync(sourcePath)
        }
        return
    }
    try {
        fs.renameSync(sourcePath, destinationPath)
    } catch {
        fs.copyFileSync(sourcePath, destinationPath)
        fs.unlinkSync(sourcePath)
    }
}

function probeDurationSeconds(filePath: string) {
    try {
        const output = execFileSync(
            'ffprobe',
            [
                '-v',
                'error',
                '-show_entries',
                'format=duration',
                '-of',
                'default=noprint_wrappers=1:nokey=1',
                filePath,
            ],
            { encoding: 'utf8' },
        )
        const duration = Number(output.trim())
        if (!Number.isFinite(duration) || duration <= 0) {
            return undefined
        }
        return duration
    } catch {
        return undefined
    }
}

function normalizeShortVideoGroup(article: Pick<Article, 'u_id' | 'username'>) {
    const normalized = `${article.u_id || ''} ${article.username || ''}`.toLowerCase().replace(/[^a-z0-9]+/g, '')
    if (normalized.includes('the3rd') || normalized.includes('3rd')) {
        return '3rd'
    }
    if (normalized.includes('nananijigram')) {
        return 'nijigram'
    }
    if (normalized.includes('227smej') || normalized.includes('227official')) {
        return '227-official'
    }
    return null
}

function isSupportedShortVideoPlatform(article: Pick<Article, 'platform' | 'type'>) {
    if (article.platform === Platform.TikTok) {
        return true
    }
    if (article.platform === Platform.YouTube) {
        return article.type === 'shorts'
    }
    if (article.platform === Platform.Instagram) {
        return article.type === 'post' || article.type === 'story'
    }
    return false
}

function buildShortVideoTimeBuckets(createdAt: number) {
    const baseBucket = Math.floor(createdAt / SHORT_VIDEO_TIME_BUCKET_SECONDS)
    return [baseBucket - 1, baseBucket, baseBucket + 1]
}

function buildShortVideoDurationBuckets(durationSeconds: number) {
    const baseBucket = Math.round((durationSeconds * 1000) / SHORT_VIDEO_DURATION_BUCKET_MS)
    const buckets = [] as number[]
    for (let offset = -SHORT_VIDEO_DURATION_TOLERANCE_BUCKETS; offset <= SHORT_VIDEO_DURATION_TOLERANCE_BUCKETS; offset += 1) {
        buckets.push(baseBucket + offset)
    }
    return {
        baseBucket,
        buckets,
    }
}

function buildShortVideoDedupCandidate(
    article: Pick<Article, 'platform' | 'type' | 'a_id' | 'created_at' | 'u_id' | 'username'>,
    mediaFiles: Array<{ media_type: MediaType; duration_seconds?: number }>,
): ShortVideoDedupCandidate | null {
    if (!isSupportedShortVideoPlatform(article)) {
        return null
    }

    const group = normalizeShortVideoGroup(article)
    if (!group) {
        return null
    }

    const videoFile = mediaFiles.find(
        (item) =>
            item.media_type === 'video'
            && typeof item.duration_seconds === 'number'
            && item.duration_seconds > 0
            && item.duration_seconds <= SHORT_VIDEO_MAX_DURATION_SECONDS,
    )
    if (!videoFile || typeof videoFile.duration_seconds !== 'number') {
        return null
    }

    const articleMarker = buildArticleMarker(article as Pick<Article, 'platform' | 'a_id'>)
    const timeBuckets = buildShortVideoTimeBuckets(article.created_at)
    const { baseBucket, buckets } = buildShortVideoDurationBuckets(videoFile.duration_seconds)

    return {
        storagePlatform: `cross-short-video:${group}`,
        articleMarker,
        signature: `${Math.floor(article.created_at / SHORT_VIDEO_TIME_BUCKET_SECONDS)}:${baseBucket}`,
        signaturesToCheck: Array.from(
            new Set(timeBuckets.flatMap((timeBucket) => buckets.map((durationBucket) => `${timeBucket}:${durationBucket}`))),
        ).sort(),
        duration_seconds: videoFile.duration_seconds,
        group,
    }
}

async function checkShortVideoCrossPlatformDuplicate(candidate: ShortVideoDedupCandidate) {
    for (const signature of candidate.signaturesToCheck) {
        const existing = await DB.MediaHash.checkExist(candidate.storagePlatform, signature)
        if (existing && existing.a_id !== candidate.articleMarker) {
            return existing
        }
    }
    return null
}

async function markShortVideoCrossPlatformSeen(candidate: ShortVideoDedupCandidate) {
    return await DB.MediaHash.save(candidate.storagePlatform, candidate.signature, candidate.articleMarker)
}

function sampleVideoFrameHash(filePath: string, durationSeconds: number, ratio: number) {
    const seekSeconds = Math.max(0, Math.min(durationSeconds - 0.25, durationSeconds * ratio))
    try {
        const rawFrame = execFileSync(
            'ffmpeg',
            [
                '-v',
                'error',
                '-ss',
                seekSeconds.toFixed(3),
                '-i',
                filePath,
                '-frames:v',
                '1',
                '-vf',
                'scale=8:8:flags=area,format=gray',
                '-f',
                'rawvideo',
                'pipe:1',
            ],
            { maxBuffer: 1024 * 1024 },
        ) as Buffer

        if (rawFrame.length < 64) {
            return null
        }

        const pixels = Array.from(rawFrame.subarray(0, 64))
        const average = pixels.reduce((sum, value) => sum + value, 0) / pixels.length
        let bits = 0n
        pixels.forEach((value, index) => {
            if (value >= average) {
                bits |= 1n << BigInt(63 - index)
            }
        })
        return bits.toString(16).padStart(16, '0')
    } catch {
        return null
    }
}

function buildVideoFingerprintBandKeys(durationBucket: number, frameHashes: Array<string>) {
    const bandKeys: Array<string> = []
    frameHashes.forEach((hash, frameIndex) => {
        for (let offset = 0; offset < hash.length; offset += VIDEO_FINGERPRINT_BAND_SIZE) {
            const bandIndex = offset / VIDEO_FINGERPRINT_BAND_SIZE
            const band = hash.slice(offset, offset + VIDEO_FINGERPRINT_BAND_SIZE)
            if (band.length === VIDEO_FINGERPRINT_BAND_SIZE) {
                bandKeys.push(`band:${durationBucket}:f${frameIndex}:b${bandIndex}:${band}`)
            }
        }
    })
    return bandKeys
}

function buildVideoFingerprintCandidate(
    article: Pick<Article, 'platform' | 'type' | 'a_id' | 'created_at' | 'u_id' | 'username'>,
    mediaFile: Pick<StoredMediaMetadata, 'path' | 'media_type' | 'duration_seconds'>,
): VideoFingerprintCandidate | null {
    if (
        mediaFile.media_type !== 'video'
        || typeof mediaFile.duration_seconds !== 'number'
        || mediaFile.duration_seconds <= 0
        || mediaFile.duration_seconds > SHORT_VIDEO_MAX_DURATION_SECONDS
        || !isSupportedShortVideoPlatform(article)
    ) {
        return null
    }

    const frameHashes = VIDEO_FINGERPRINT_SAMPLE_RATIOS
        .map((ratio) => sampleVideoFrameHash(mediaFile.path, mediaFile.duration_seconds as number, ratio))
        .filter((hash): hash is string => Boolean(hash))

    if (frameHashes.length < 3) {
        return null
    }

    const group = normalizeShortVideoGroup(article) || 'global'
    const articleMarker = buildArticleMarker(article as Pick<Article, 'platform' | 'a_id'>)
    const durationBucket = Math.round((mediaFile.duration_seconds * 1000) / SHORT_VIDEO_DURATION_BUCKET_MS)

    return {
        storagePlatform: `${VIDEO_FINGERPRINT_PLATFORM_PREFIX}:${group}`,
        articleMarker,
        signature: `exact:${durationBucket}:${frameHashes.join(':')}`,
        bandKeys: buildVideoFingerprintBandKeys(durationBucket, frameHashes),
        duration_seconds: mediaFile.duration_seconds,
        group,
    }
}

async function checkVideoFingerprintDuplicate(candidate: VideoFingerprintCandidate) {
    const exact = await DB.MediaHash.checkExist(candidate.storagePlatform, candidate.signature)
    if (exact && exact.a_id !== candidate.articleMarker) {
        return exact
    }

    const hitsByArticle = new Map<string, { count: number; existing: Awaited<ReturnType<typeof DB.MediaHash.checkExist>> }>()
    for (const bandKey of candidate.bandKeys) {
        const existing = await DB.MediaHash.checkExist(candidate.storagePlatform, bandKey)
        if (!existing || existing.a_id === candidate.articleMarker) {
            continue
        }
        const current = hitsByArticle.get(existing.a_id) || { count: 0, existing }
        current.count += 1
        hitsByArticle.set(existing.a_id, current)
    }

    const threshold = Math.min(
        candidate.bandKeys.length,
        Math.max(VIDEO_FINGERPRINT_MIN_BAND_MATCHES, Math.ceil(candidate.bandKeys.length * 0.5)),
    )
    for (const hit of hitsByArticle.values()) {
        if (hit.count >= threshold) {
            return hit.existing
        }
    }

    return null
}

async function markVideoFingerprintSeen(candidate: VideoFingerprintCandidate) {
    await DB.MediaHash.save(candidate.storagePlatform, candidate.signature, candidate.articleMarker)
    await Promise.all(candidate.bandKeys.map((bandKey) => DB.MediaHash.save(candidate.storagePlatform, bandKey, candidate.articleMarker)))
}

async function checkExactCrossPlatformVideoDuplicate(hash: string, articleMarker: string) {
    const existing = await DB.MediaHash.checkExist(EXACT_CROSS_PLATFORM_VIDEO_PLATFORM, hash)
    if (existing && existing.a_id !== articleMarker) {
        return existing
    }
    return null
}

async function markExactCrossPlatformVideoSeen(hash: string, articleMarker: string) {
    return await DB.MediaHash.save(EXACT_CROSS_PLATFORM_VIDEO_PLATFORM, hash, articleMarker)
}

function resolveExactCrossPlatformMediaStorage(mediaType: MediaType) {
    if (mediaType === 'video') {
        return EXACT_CROSS_PLATFORM_VIDEO_PLATFORM
    }
    if (mediaType === 'photo') {
        return `${EXACT_CROSS_PLATFORM_MEDIA_PREFIX}:photo`
    }
    return null
}

async function checkExactCrossPlatformMediaDuplicate(mediaType: MediaType, hash: string, articleMarker: string) {
    const storagePlatform = resolveExactCrossPlatformMediaStorage(mediaType)
    if (!storagePlatform) {
        return null
    }

    const existing = await DB.MediaHash.checkExist(storagePlatform, hash)
    if (existing && existing.a_id !== articleMarker) {
        return existing
    }
    return null
}

async function markExactCrossPlatformMediaSeen(mediaType: MediaType, hash: string, articleMarker: string) {
    const storagePlatform = resolveExactCrossPlatformMediaStorage(mediaType)
    if (!storagePlatform) {
        return null
    }
    return await DB.MediaHash.save(storagePlatform, hash, articleMarker)
}

function persistMediaFile(sourcePath: string, options: PersistMediaFileOptions): StoredMediaMetadata {
    ensureDirectory(MEDIA_STORE_ROOT)
    ensureDirectory(MEDIA_STORE_VIDEO_ROOT)
    ensureDirectory(MEDIA_STORE_IMAGE_ROOT)

    const buffer = fs.readFileSync(sourcePath)
    const hash = crypto.createHash('sha256').update(buffer).digest('hex')
    const extension = normalizeExtension(path.extname(sourcePath))
    const storedPath = resolveStoredMediaPath(hash, extension, options.media_type)
    moveIntoStore(sourcePath, storedPath)

    const stats = fs.statSync(storedPath)
    const now = Math.floor(Date.now() / 1000)
    const metadataPath = `${storedPath}.json`
    const existing = parseStoredMediaMetadata(metadataPath)
    const articleMarker = options.article ? buildArticleMarker(options.article as Pick<Article, 'platform' | 'a_id'>) : ''

    const metadata: StoredMediaMetadata = {
        path: storedPath,
        hash,
        media_type: options.media_type,
        size_bytes: stats.size,
        duration_seconds:
            options.media_type === 'video'
                ? probeDurationSeconds(storedPath) || existing?.duration_seconds
                : undefined,
        source_urls: dedupeStrings([...(existing?.source_urls || []), options.source_url, options.article?.url]),
        article_markers: dedupeStrings([...(existing?.article_markers || []), articleMarker]),
        article_urls: dedupeStrings([...(existing?.article_urls || []), options.article?.url]),
        platforms: dedupeStrings([...(existing?.platforms || []), options.article ? String(options.article.platform) : '']),
        u_ids: dedupeStrings([...(existing?.u_ids || []), options.article?.u_id]),
        usernames: dedupeStrings([...(existing?.usernames || []), options.article?.username]),
        created_at: existing?.created_at || now,
        updated_at: now,
    }

    writeStoredMediaMetadata(metadataPath, metadata)
    return metadata
}

function isPersistentMediaPath(filePath: string) {
    const root = path.resolve(MEDIA_STORE_ROOT)
    const resolved = path.resolve(filePath)
    return resolved === root || resolved.startsWith(`${root}${path.sep}`)
}

export {
    buildArticleMarker,
    buildShortVideoDedupCandidate,
    buildVideoFingerprintBandKeys,
    buildVideoFingerprintCandidate,
    checkVideoFingerprintDuplicate,
    checkExactCrossPlatformMediaDuplicate,
    checkExactCrossPlatformVideoDuplicate,
    checkShortVideoCrossPlatformDuplicate,
    isPersistentMediaPath,
    markExactCrossPlatformMediaSeen,
    markExactCrossPlatformVideoSeen,
    markShortVideoCrossPlatformSeen,
    markVideoFingerprintSeen,
    persistMediaFile,
}
export type { ShortVideoDedupCandidate, StoredMediaMetadata, VideoFingerprintCandidate }
