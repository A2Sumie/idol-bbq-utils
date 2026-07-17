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
const SHORT_VIDEO_TEXT_KEY_LIMIT = 6
const SHORT_VIDEO_TEXT_MIN_COMPACT_LENGTH = 8
const SHORT_VIDEO_SHARED_PHRASE_MIN_LENGTH = 8
const SHORT_VIDEO_INSTAGRAM_TIKTOK_FALLBACK_KEY = 'p:instagram-tiktok'
const VIDEO_FINGERPRINT_SAMPLE_RATIOS = [0.12, 0.3, 0.5, 0.7, 0.88]
const VIDEO_FINGERPRINT_BAND_SIZE = 4
const VIDEO_FINGERPRINT_MIN_BAND_MATCHES = 8
const VIDEO_FINGERPRINT_MIN_DISTINCT_FRAME_HASHES = 3
const DEFAULT_MEDIA_STORE_RETENTION_DAYS = 7
const DEFAULT_MEDIA_DOWNLOAD_RETENTION_HOURS = 24
const DEFAULT_MEDIA_CLEANUP_INTERVAL_HOURS = 6
const HOUR_MS = 60 * 60 * 1000
const DAY_MS = 24 * HOUR_MS

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
    signaturesToStore: Array<string>
    signaturesToCheck: Array<string>
    coarseFallbackSignaturesToCheck: Array<string>
    duration_seconds: number
    group: string
    text: ShortVideoTextFingerprint
}

interface ShortVideoTextFingerprint {
    normalized: string
    compact: string
    distilledCompact: string
    tokens: Array<string>
    keys: Array<string>
}

interface VideoFingerprintCandidate {
    storagePlatform: string
    articleMarker: string
    signature: string
    bandKeys: Array<string>
    duration_seconds: number
    group: string
}

interface MediaCacheCleanupOptions {
    cacheRoot?: string
    nowMs?: number
    storeRetentionMs?: number
    downloadRetentionMs?: number
}

interface MediaCacheCleanupSummary {
    storeFilesDeleted: number
    downloadFilesDeleted: number
    bytesDeleted: number
    errors: number
}

interface MediaCacheCleanupJob {
    stop: () => void
}

type MediaCacheCleanupLogger = Partial<Pick<Console, 'debug' | 'info' | 'warn' | 'error'>>

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
            ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', filePath],
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

function normalizeShortVideoGroup(article: Pick<Article, 'u_id' | 'username'> & { content?: string | null }) {
    const normalized = `${article.u_id || ''} ${article.username || ''} ${article.content || ''}`
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '')
    if (normalized.includes('the3rd') || normalized.includes('3rd')) {
        return '3rd'
    }
    if (normalized.includes('nananijigram')) {
        return 'nijigram'
    }
    if (normalized.includes('227smej') || normalized.includes('227official') || normalized.includes('227staff')) {
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
    if (article.platform === Platform.X) {
        return true
    }
    return false
}

function isInstagramTikTokShortVideoPlatform(platform: Platform) {
    return platform === Platform.Instagram || platform === Platform.TikTok
}

function isInstagramTikTokShortVideoPair(left: Platform, right: Platform) {
    return (
        (left === Platform.Instagram && right === Platform.TikTok) ||
        (left === Platform.TikTok && right === Platform.Instagram)
    )
}

function buildShortVideoTimeBuckets(createdAt: number) {
    const baseBucket = Math.floor(createdAt / SHORT_VIDEO_TIME_BUCKET_SECONDS)
    return [baseBucket - 1, baseBucket, baseBucket + 1]
}

function buildShortVideoDurationBuckets(durationSeconds: number) {
    const baseBucket = Math.round((durationSeconds * 1000) / SHORT_VIDEO_DURATION_BUCKET_MS)
    const buckets = [] as number[]
    for (
        let offset = -SHORT_VIDEO_DURATION_TOLERANCE_BUCKETS;
        offset <= SHORT_VIDEO_DURATION_TOLERANCE_BUCKETS;
        offset += 1
    ) {
        buckets.push(baseBucket + offset)
    }
    return {
        baseBucket,
        buckets,
    }
}

const SHORT_VIDEO_TEXT_STOPWORDS = new Set([
    '22',
    '7',
    '227',
    '22_7',
    '22-7',
    'nananiji',
    'nananijigram',
    'nanabunnonijuuni',
    'the',
    '3rd',
    'official',
    'staff',
    'music',
    'video',
    'mv',
    'short',
    'shorts',
    'tiktok',
    'instagram',
    'youtube',
    '公開中',
    'ナナニジ',
    'ナナブンノニジュウニ',
])

const SHORT_VIDEO_COMPACT_BOILERPLATE_TERMS = [
    '227',
    '22_7',
    'nananijigram',
    'nananiji',
    'nanabunnonijuuni',
    'the3rd',
    'official',
    'staff',
    'musicvideo',
    'youtube',
    'tiktok',
    'instagram',
    'shorts',
    'short',
    '公開中',
    'ナナニジ',
    'ナナブンノニジュウニ',
]

function normalizeShortVideoTextValue(value: string) {
    return value
        .normalize('NFKC')
        .toLowerCase()
        .replace(/https?:\/\/\S+/g, ' ')
        .replace(/(?:www\.)\S+\.\S+/g, ' ')
        .replace(/[@＠][\w.-]+/g, ' ')
        .replace(/[＃#]/g, ' ')
        .replace(/[【】「」『』（）()[\]{}<>《》.,!?！？、。:：;；'"`~^*_+=|\\/]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
}

function stripShortVideoCompactBoilerplate(value: string) {
    let result = value
    for (const term of SHORT_VIDEO_COMPACT_BOILERPLATE_TERMS) {
        result = result.split(term).join('')
    }
    return result
}

function isInformativeShortVideoToken(token: string) {
    const compactToken = token.replace(/[._-]+/g, '')
    if (token.length < 2 || compactToken.length < 2) {
        return false
    }
    if (/^\d+$/.test(compactToken)) {
        return false
    }
    return !SHORT_VIDEO_TEXT_STOPWORDS.has(token) && !SHORT_VIDEO_TEXT_STOPWORDS.has(compactToken)
}

function hashShortVideoTextKey(prefix: string, value: string) {
    return `${prefix}:${crypto.createHash('sha1').update(value).digest('hex').slice(0, 16)}`
}

function buildShortVideoTextKeys(fingerprint: Omit<ShortVideoTextFingerprint, 'keys'>) {
    const keys = new Set<string>()
    if (fingerprint.distilledCompact.length >= SHORT_VIDEO_TEXT_MIN_COMPACT_LENGTH) {
        keys.add(hashShortVideoTextKey('c', fingerprint.distilledCompact))
    }

    for (const token of fingerprint.tokens.filter((item) => item.length >= 6).slice(0, 4)) {
        keys.add(hashShortVideoTextKey('t', token))
    }

    const signatureTokens = fingerprint.tokens
        .filter((item) => item.length >= 3)
        .slice(0, 8)
        .sort()
    if (signatureTokens.join('').length >= 10) {
        keys.add(hashShortVideoTextKey('s', signatureTokens.join('|')))
    }

    return Array.from(keys).slice(0, SHORT_VIDEO_TEXT_KEY_LIMIT)
}

function buildShortVideoTextFingerprint(article: { content?: string | null; translation?: string | null }) {
    const normalized = [article.content, article.translation]
        .filter((value): value is string => typeof value === 'string' && value.trim().length > 0)
        .map(normalizeShortVideoTextValue)
        .filter(Boolean)
        .join(' ')
        .replace(/\s+/g, ' ')
        .trim()
    const compact = normalized.replace(/[^\p{L}\p{N}]+/gu, '')
    const distilledCompact = stripShortVideoCompactBoilerplate(compact)
    const rawTokens = normalized.match(/[\p{L}\p{N}][\p{L}\p{N}._-]*/gu) || []
    const tokens = Array.from(
        new Set(rawTokens.map((token) => token.replace(/^[._-]+|[._-]+$/g, '')).filter(isInformativeShortVideoToken)),
    )
    const base = {
        normalized,
        compact,
        distilledCompact,
        tokens,
    }
    return {
        ...base,
        keys: buildShortVideoTextKeys(base),
    }
}

function buildShortVideoSignature(timeBucket: number, durationBucket: number, textKey: string) {
    return `${timeBucket}:${durationBucket}:${textKey}`
}

function parseArticleMarker(marker: string) {
    const separatorIndex = marker.indexOf(':')
    if (separatorIndex <= 0) {
        return null
    }
    const platform = Number(marker.slice(0, separatorIndex))
    const a_id = marker.slice(separatorIndex + 1).trim()
    if (!Number.isFinite(platform) || !a_id) {
        return null
    }
    return {
        platform: platform as Platform,
        a_id,
    }
}

function longestCommonSubstringLength(left: string, right: string) {
    if (!left || !right) {
        return 0
    }
    let previous = new Array(right.length + 1).fill(0)
    let best = 0

    for (let leftIndex = 1; leftIndex <= left.length; leftIndex += 1) {
        const current = new Array(right.length + 1).fill(0)
        for (let rightIndex = 1; rightIndex <= right.length; rightIndex += 1) {
            if (left[leftIndex - 1] !== right[rightIndex - 1]) {
                continue
            }
            const length = previous[rightIndex - 1] + 1
            current[rightIndex] = length
            if (length > best) {
                best = length
            }
        }
        previous = current
    }

    return best
}

function isLikelySameShortVideoText(left: ShortVideoTextFingerprint, right: ShortVideoTextFingerprint) {
    if (left.keys.length === 0 || right.keys.length === 0) {
        return false
    }

    const sharedPhraseLength = longestCommonSubstringLength(left.distilledCompact, right.distilledCompact)
    if (sharedPhraseLength >= SHORT_VIDEO_SHARED_PHRASE_MIN_LENGTH) {
        return true
    }

    const leftTokens = new Set(left.tokens)
    const rightTokens = new Set(right.tokens)
    const sharedTokens = Array.from(leftTokens).filter((token) => rightTokens.has(token))
    if (sharedTokens.some((token) => token.length >= SHORT_VIDEO_TEXT_MIN_COMPACT_LENGTH)) {
        return true
    }
    if (sharedTokens.length < 2) {
        return false
    }

    const unionSize = new Set([...leftTokens, ...rightTokens]).size
    const minSize = Math.min(leftTokens.size, rightTokens.size)
    if (unionSize === 0 || minSize === 0) {
        return false
    }

    const jaccard = sharedTokens.length / unionSize
    const containment = sharedTokens.length / minSize
    return jaccard >= 0.45 || containment >= 0.67
}

function buildShortVideoDedupCandidate(
    article: Pick<Article, 'platform' | 'type' | 'a_id' | 'created_at' | 'u_id' | 'username'> & {
        content?: string | null
        translation?: string | null
    },
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
            item.media_type === 'video' &&
            typeof item.duration_seconds === 'number' &&
            item.duration_seconds > 0 &&
            item.duration_seconds <= SHORT_VIDEO_MAX_DURATION_SECONDS,
    )
    if (!videoFile || typeof videoFile.duration_seconds !== 'number') {
        return null
    }

    const text = buildShortVideoTextFingerprint(article)
    const coarseFallbackKeys = isInstagramTikTokShortVideoPlatform(article.platform)
        ? [SHORT_VIDEO_INSTAGRAM_TIKTOK_FALLBACK_KEY]
        : []
    if (text.keys.length === 0 && coarseFallbackKeys.length === 0) {
        return null
    }

    const articleMarker = buildArticleMarker(article as Pick<Article, 'platform' | 'a_id'>)
    const timeBuckets = buildShortVideoTimeBuckets(article.created_at)
    const { baseBucket, buckets } = buildShortVideoDurationBuckets(videoFile.duration_seconds)
    const baseTimeBucket = Math.floor(article.created_at / SHORT_VIDEO_TIME_BUCKET_SECONDS)
    const signaturesToStore = text.keys.map((textKey) => buildShortVideoSignature(baseTimeBucket, baseBucket, textKey))
    const coarseFallbackSignaturesToStore = coarseFallbackKeys.map((textKey) =>
        buildShortVideoSignature(baseTimeBucket, baseBucket, textKey),
    )
    const textSignaturesToCheck = timeBuckets.flatMap((timeBucket) =>
        buckets.flatMap((durationBucket) =>
            text.keys.map((textKey) => buildShortVideoSignature(timeBucket, durationBucket, textKey)),
        ),
    )
    const coarseFallbackSignaturesToCheck = timeBuckets.flatMap((timeBucket) =>
        buckets.flatMap((durationBucket) =>
            coarseFallbackKeys.map((textKey) => buildShortVideoSignature(timeBucket, durationBucket, textKey)),
        ),
    )

    return {
        storagePlatform: `cross-short-video:${group}`,
        articleMarker,
        signature: signaturesToStore[0] || coarseFallbackSignaturesToStore[0],
        signaturesToStore: Array.from(new Set([...signaturesToStore, ...coarseFallbackSignaturesToStore])).sort(),
        signaturesToCheck: Array.from(new Set([...textSignaturesToCheck, ...coarseFallbackSignaturesToCheck])).sort(),
        coarseFallbackSignaturesToCheck: Array.from(new Set(coarseFallbackSignaturesToCheck)).sort(),
        duration_seconds: videoFile.duration_seconds,
        group,
        text,
    }
}

async function checkShortVideoCrossPlatformDuplicate(candidate: ShortVideoDedupCandidate) {
    const candidateMarker = parseArticleMarker(candidate.articleMarker)
    if (!candidateMarker) {
        return null
    }

    const coarseFallbackSignatures = new Set(candidate.coarseFallbackSignaturesToCheck)
    for (const signature of Array.from(new Set(candidate.signaturesToCheck))) {
        const existing = await DB.MediaHash.checkExist(candidate.storagePlatform, signature)
        if (!existing || existing.a_id === candidate.articleMarker) {
            continue
        }

        const existingMarker = parseArticleMarker(existing.a_id)
        if (!existingMarker || existingMarker.platform === candidateMarker.platform) {
            continue
        }
        const isCoarseFallbackSignature = coarseFallbackSignatures.has(signature)
        if (
            isCoarseFallbackSignature &&
            !isInstagramTikTokShortVideoPair(candidateMarker.platform, existingMarker.platform)
        ) {
            continue
        }

        const existingArticle = await DB.Article.getSingleArticleByArticleCode(
            existingMarker.a_id,
            existingMarker.platform,
        )
        if (!existingArticle) {
            continue
        }

        const existingText = buildShortVideoTextFingerprint(existingArticle as any)
        if (isCoarseFallbackSignature && (candidate.text.keys.length === 0 || existingText.keys.length === 0)) {
            return existing
        }
        if (isLikelySameShortVideoText(candidate.text, existingText)) {
            return existing
        }
    }

    return null
}

async function markShortVideoCrossPlatformSeen(candidate: ShortVideoDedupCandidate) {
    await Promise.all(
        Array.from(new Set(candidate.signaturesToStore)).map((signature) =>
            DB.MediaHash.save(candidate.storagePlatform, signature, candidate.articleMarker),
        ),
    )
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
            { maxBuffer: 1024 * 1024, stdio: ['ignore', 'pipe', 'ignore'] },
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
    const bandKeys = new Set<string>()
    frameHashes.forEach((hash, frameIndex) => {
        for (let offset = 0; offset < hash.length; offset += VIDEO_FINGERPRINT_BAND_SIZE) {
            const bandIndex = offset / VIDEO_FINGERPRINT_BAND_SIZE
            const band = hash.slice(offset, offset + VIDEO_FINGERPRINT_BAND_SIZE)
            if (band.length === VIDEO_FINGERPRINT_BAND_SIZE && !/^(?:0+|f+)$/i.test(band)) {
                bandKeys.add(`band:${durationBucket}:f${frameIndex}:b${bandIndex}:${band}`)
            }
        }
    })
    return Array.from(bandKeys)
}

function buildVideoFingerprintCandidate(
    article: Pick<Article, 'platform' | 'type' | 'a_id' | 'created_at' | 'u_id' | 'username'> & {
        content?: string | null
    },
    mediaFile: Pick<StoredMediaMetadata, 'path' | 'media_type' | 'duration_seconds'>,
): VideoFingerprintCandidate | null {
    if (
        mediaFile.media_type !== 'video' ||
        typeof mediaFile.duration_seconds !== 'number' ||
        mediaFile.duration_seconds <= 0 ||
        mediaFile.duration_seconds > SHORT_VIDEO_MAX_DURATION_SECONDS ||
        !isSupportedShortVideoPlatform(article)
    ) {
        return null
    }

    const frameHashes = VIDEO_FINGERPRINT_SAMPLE_RATIOS.map((ratio) =>
        sampleVideoFrameHash(mediaFile.path, mediaFile.duration_seconds as number, ratio),
    ).filter((hash): hash is string => Boolean(hash))

    if (frameHashes.length < 3) {
        return null
    }
    if (new Set(frameHashes).size < VIDEO_FINGERPRINT_MIN_DISTINCT_FRAME_HASHES) {
        return null
    }

    const group = normalizeShortVideoGroup(article) || 'global'
    const articleMarker = buildArticleMarker(article as Pick<Article, 'platform' | 'a_id'>)
    const durationBucket = Math.round((mediaFile.duration_seconds * 1000) / SHORT_VIDEO_DURATION_BUCKET_MS)
    const bandKeys = buildVideoFingerprintBandKeys(durationBucket, frameHashes)
    if (bandKeys.length < VIDEO_FINGERPRINT_MIN_BAND_MATCHES) {
        return null
    }

    return {
        storagePlatform: `${VIDEO_FINGERPRINT_PLATFORM_PREFIX}:${group}`,
        articleMarker,
        signature: `exact:${durationBucket}:${frameHashes.join(':')}`,
        bandKeys,
        duration_seconds: mediaFile.duration_seconds,
        group,
    }
}

async function checkVideoFingerprintDuplicate(candidate: VideoFingerprintCandidate) {
    const candidateBandKeys = Array.from(new Set(candidate.bandKeys))
    if (candidateBandKeys.length < VIDEO_FINGERPRINT_MIN_BAND_MATCHES) {
        return null
    }

    const exact = await DB.MediaHash.checkExist(candidate.storagePlatform, candidate.signature)
    if (exact && exact.a_id !== candidate.articleMarker) {
        return exact
    }

    const hitsByArticle = new Map<
        string,
        { count: number; existing: Awaited<ReturnType<typeof DB.MediaHash.checkExist>> }
    >()
    for (const bandKey of candidateBandKeys) {
        const existing = await DB.MediaHash.checkExist(candidate.storagePlatform, bandKey)
        if (!existing || existing.a_id === candidate.articleMarker) {
            continue
        }
        const current = hitsByArticle.get(existing.a_id) || { count: 0, existing }
        current.count += 1
        hitsByArticle.set(existing.a_id, current)
    }

    const threshold = Math.min(
        candidateBandKeys.length,
        Math.max(VIDEO_FINGERPRINT_MIN_BAND_MATCHES, Math.ceil(candidateBandKeys.length * 0.5)),
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
    await Promise.all(
        Array.from(new Set(candidate.bandKeys)).map((bandKey) =>
            DB.MediaHash.save(candidate.storagePlatform, bandKey, candidate.articleMarker),
        ),
    )
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

function sniffImageKind(buffer: Buffer): 'png' | 'jpeg' | 'gif' | 'webp' | null {
    if (buffer.length >= 12 && buffer.readUInt32BE(0) === 0x52494646 && buffer.toString('ascii', 8, 12) === 'WEBP') {
        return 'webp'
    }
    if (buffer.length >= 8 && buffer.readUInt32BE(0) === 0x89504e47 && buffer.readUInt32BE(4) === 0x0d0a1a0a) {
        return 'png'
    }
    if (buffer.length >= 3 && buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) {
        return 'jpeg'
    }
    if (buffer.length >= 6) {
        const sig = buffer.toString('ascii', 0, 6)
        if (sig === 'GIF87a' || sig === 'GIF89a') {
            return 'gif'
        }
    }
    return null
}

const SATORI_SAFE_IMAGE_KINDS = new Set(['png', 'jpeg', 'gif'])

function isImageLikeMediaType(mediaType: MediaType) {
    return mediaType === 'photo' || media_type_is_thumbnail(mediaType)
}

function media_type_is_thumbnail(mediaType: MediaType) {
    return mediaType === 'video_thumbnail'
}

/**
 * Satori's image loader crashes on formats it cannot parse (observed: WebP with a .jpg
 * extension, the classic i.ytimg thumbnail). Transcode such images to PNG once and reuse
 * the sibling file afterwards.
 */
function ensureSatoriCompatibleImage(filePath: string, log?: { warn?: (...args: Array<any>) => void }): string {
    let buffer: Buffer
    try {
        buffer = fs.readFileSync(filePath)
    } catch {
        return filePath
    }
    const kind = sniffImageKind(buffer)
    if (kind === null || SATORI_SAFE_IMAGE_KINDS.has(kind)) {
        return filePath
    }
    const convertedPath = `${filePath}-satori.png`
    if (fs.existsSync(convertedPath)) {
        return convertedPath
    }
    try {
        execFileSync(process.env.FFMPEG_PATH || 'ffmpeg', ['-y', '-v', 'error', '-i', filePath, convertedPath], {
            stdio: 'ignore',
            timeout: 20_000,
        })
        return convertedPath
    } catch (error) {
        log?.warn?.(`Failed to transcode ${kind} image ${filePath} for satori: ${error}`)
        return filePath
    }
}

function persistMediaFile(sourcePath: string, options: PersistMediaFileOptions): StoredMediaMetadata {
    ensureDirectory(MEDIA_STORE_ROOT)
    ensureDirectory(MEDIA_STORE_VIDEO_ROOT)
    ensureDirectory(MEDIA_STORE_IMAGE_ROOT)

    const effectiveSourcePath = isImageLikeMediaType(options.media_type)
        ? ensureSatoriCompatibleImage(sourcePath)
        : sourcePath
    const buffer = fs.readFileSync(effectiveSourcePath)
    const hash = crypto.createHash('sha256').update(buffer).digest('hex')
    const extension = normalizeExtension(path.extname(effectiveSourcePath))
    const storedPath = resolveStoredMediaPath(hash, extension, options.media_type)
    moveIntoStore(effectiveSourcePath, storedPath)

    const stats = fs.statSync(storedPath)
    const now = Math.floor(Date.now() / 1000)
    const metadataPath = `${storedPath}.json`
    const existing = parseStoredMediaMetadata(metadataPath)
    const articleMarker = options.article
        ? buildArticleMarker(options.article as Pick<Article, 'platform' | 'a_id'>)
        : ''

    const metadata: StoredMediaMetadata = {
        path: storedPath,
        hash,
        media_type: options.media_type,
        size_bytes: stats.size,
        duration_seconds:
            options.media_type === 'video' ? probeDurationSeconds(storedPath) || existing?.duration_seconds : undefined,
        source_urls: dedupeStrings([...(existing?.source_urls || []), options.source_url, options.article?.url]),
        article_markers: dedupeStrings([...(existing?.article_markers || []), articleMarker]),
        article_urls: dedupeStrings([...(existing?.article_urls || []), options.article?.url]),
        platforms: dedupeStrings([
            ...(existing?.platforms || []),
            options.article ? String(options.article.platform) : '',
        ]),
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

function parsePositiveEnvNumber(name: string, fallback: number) {
    const value = Number(process.env[name])
    return Number.isFinite(value) && value > 0 ? value : fallback
}

function resolveCleanupRoots(cacheRoot = CACHE_DIR_ROOT) {
    const mediaRoot = path.join(cacheRoot, 'media')
    return {
        storeRoot: path.join(mediaRoot, 'store'),
        downloadRoots: [
            path.join(mediaRoot, 'plain'),
            path.join(mediaRoot, 'gallery-dl'),
            path.join(mediaRoot, 'yt-dlp'),
        ],
    }
}

function createCleanupSummary(): MediaCacheCleanupSummary {
    return {
        storeFilesDeleted: 0,
        downloadFilesDeleted: 0,
        bytesDeleted: 0,
        errors: 0,
    }
}

function statMtimeMs(filePath: string) {
    try {
        return fs.statSync(filePath).mtimeMs
    } catch {
        return 0
    }
}

function deleteFile(filePath: string, summary: MediaCacheCleanupSummary, bucket: 'store' | 'download') {
    try {
        const stat = fs.statSync(filePath)
        fs.unlinkSync(filePath)
        summary.bytesDeleted += stat.size
        if (bucket === 'store') {
            summary.storeFilesDeleted += 1
        } else {
            summary.downloadFilesDeleted += 1
        }
    } catch {
        summary.errors += 1
    }
}

function cleanupEmptyDirectories(root: string) {
    if (!fs.existsSync(root)) {
        return
    }

    const visit = (dirPath: string) => {
        let entries: Array<string>
        try {
            entries = fs.readdirSync(dirPath)
        } catch {
            return false
        }

        for (const entry of entries) {
            const childPath = path.join(dirPath, entry)
            try {
                if (fs.statSync(childPath).isDirectory()) {
                    visit(childPath)
                }
            } catch {}
        }

        if (dirPath !== root) {
            try {
                if (fs.readdirSync(dirPath).length === 0) {
                    fs.rmdirSync(dirPath)
                    return true
                }
            } catch {}
        }
        return false
    }

    visit(root)
}

function walkFiles(root: string, visit: (filePath: string) => void) {
    if (!fs.existsSync(root)) {
        return
    }

    const entries = fs.readdirSync(root, { withFileTypes: true })
    for (const entry of entries) {
        const filePath = path.join(root, entry.name)
        if (entry.isDirectory()) {
            walkFiles(filePath, visit)
        } else if (entry.isFile()) {
            visit(filePath)
        }
    }
}

function cleanupStoreFiles(root: string, cutoffMs: number, summary: MediaCacheCleanupSummary) {
    const visitedSidecars = new Set<string>()

    walkFiles(root, (filePath) => {
        if (filePath.endsWith('.json')) {
            return
        }

        const sidecarPath = `${filePath}.json`
        const lastTouchedAt = Math.max(statMtimeMs(filePath), statMtimeMs(sidecarPath))
        if (lastTouchedAt >= cutoffMs) {
            return
        }

        deleteFile(filePath, summary, 'store')
        if (fs.existsSync(sidecarPath)) {
            visitedSidecars.add(sidecarPath)
            deleteFile(sidecarPath, summary, 'store')
        }
    })

    walkFiles(root, (filePath) => {
        if (!filePath.endsWith('.json') || visitedSidecars.has(filePath)) {
            return
        }
        const mediaPath = filePath.slice(0, -'.json'.length)
        if (!fs.existsSync(mediaPath) && statMtimeMs(filePath) < cutoffMs) {
            deleteFile(filePath, summary, 'store')
        }
    })
}

function cleanupDownloadFiles(root: string, cutoffMs: number, summary: MediaCacheCleanupSummary) {
    walkFiles(root, (filePath) => {
        if (statMtimeMs(filePath) < cutoffMs) {
            deleteFile(filePath, summary, 'download')
        }
    })
}

function cleanupMediaCache(options: MediaCacheCleanupOptions = {}): MediaCacheCleanupSummary {
    const nowMs = options.nowMs || Date.now()
    const storeRetentionMs =
        options.storeRetentionMs ??
        parsePositiveEnvNumber('MEDIA_STORE_RETENTION_DAYS', DEFAULT_MEDIA_STORE_RETENTION_DAYS) * DAY_MS
    const downloadRetentionMs =
        options.downloadRetentionMs ??
        parsePositiveEnvNumber('MEDIA_DOWNLOAD_RETENTION_HOURS', DEFAULT_MEDIA_DOWNLOAD_RETENTION_HOURS) * HOUR_MS
    const { storeRoot, downloadRoots } = resolveCleanupRoots(options.cacheRoot)
    const summary = createCleanupSummary()

    cleanupStoreFiles(storeRoot, nowMs - storeRetentionMs, summary)
    cleanupEmptyDirectories(storeRoot)

    for (const downloadRoot of downloadRoots) {
        cleanupDownloadFiles(downloadRoot, nowMs - downloadRetentionMs, summary)
        cleanupEmptyDirectories(downloadRoot)
    }

    return summary
}

function startMediaCacheCleanupJob(log?: MediaCacheCleanupLogger): MediaCacheCleanupJob {
    const intervalMs =
        parsePositiveEnvNumber('MEDIA_CLEANUP_INTERVAL_HOURS', DEFAULT_MEDIA_CLEANUP_INTERVAL_HOURS) * HOUR_MS
    let stopped = false

    const run = () => {
        if (stopped) {
            return
        }
        try {
            const summary = cleanupMediaCache()
            if (summary.storeFilesDeleted > 0 || summary.downloadFilesDeleted > 0 || summary.errors > 0) {
                log?.info?.(
                    `Media cache cleanup deleted ${summary.storeFilesDeleted} store file(s), ${summary.downloadFilesDeleted} download file(s), ${summary.bytesDeleted} byte(s); errors=${summary.errors}`,
                )
            } else {
                log?.debug?.('Media cache cleanup found no expired files')
            }
        } catch (error) {
            log?.warn?.(`Media cache cleanup failed: ${error instanceof Error ? error.message : String(error)}`)
        }
    }

    const initialTimer = setTimeout(run, 10_000)
    const intervalTimer = setInterval(run, intervalMs)
    ;(initialTimer as any).unref?.()
    ;(intervalTimer as any).unref?.()

    return {
        stop: () => {
            stopped = true
            clearTimeout(initialTimer)
            clearInterval(intervalTimer)
        },
    }
}

export {
    buildArticleMarker,
    buildShortVideoDedupCandidate,
    buildVideoFingerprintBandKeys,
    buildVideoFingerprintCandidate,
    cleanupMediaCache,
    checkVideoFingerprintDuplicate,
    checkExactCrossPlatformMediaDuplicate,
    checkExactCrossPlatformVideoDuplicate,
    checkShortVideoCrossPlatformDuplicate,
    ensureSatoriCompatibleImage,
    isPersistentMediaPath,
    markExactCrossPlatformMediaSeen,
    markExactCrossPlatformVideoSeen,
    markShortVideoCrossPlatformSeen,
    markVideoFingerprintSeen,
    persistMediaFile,
    startMediaCacheCleanupJob,
}
export type {
    MediaCacheCleanupJob,
    MediaCacheCleanupOptions,
    MediaCacheCleanupSummary,
    ShortVideoDedupCandidate,
    StoredMediaMetadata,
    VideoFingerprintCandidate,
}
