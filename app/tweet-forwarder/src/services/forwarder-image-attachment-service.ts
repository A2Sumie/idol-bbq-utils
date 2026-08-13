import { CACHE_DIR_ROOT } from '@/config'
import type { MediaFile } from '@/middleware/forwarder/base'
import type { Logger } from '@idol-bbq-utils/log'
import { execFileSync } from 'child_process'
import crypto from 'crypto'
import fs from 'fs'
import path from 'path'

const DEFAULT_FORWARDER_IMAGE_MAX_BYTES = 4_000_000
// 30000px cards (13-photo blog renders) exceed QQ rich media transfer limits (~25600px longest edge)
// and are rejected with -1 rich media transfer failed; 20000px keeps chunks well under the cap.
const DEFAULT_FORWARDER_IMAGE_MAX_EDGE_PX = 20_000
const DEFAULT_FORWARDER_IMAGE_MAX_PIXELS = 40_000_000
const COMPRESSED_IMAGE_DIR = path.join(CACHE_DIR_ROOT, 'media', 'forwarder-compressed')
// Bump when ffmpeg flags/output semantics change so stale cache entries never
// get reused. Old entries are reaped by the media-cache cleanup job (24h).
const COMPRESSION_CACHE_VERSION = 'v1'

type ImageAttachmentLogger = Partial<Pick<Logger, 'debug' | 'info' | 'warn'>>

interface NormalizeForwarderImageAttachmentsOptions {
    maxImageBytes?: number
    maxImageEdgePx?: number
    maxImagePixels?: number
    ffmpegPath?: string
    ffprobePath?: string
    log?: ImageAttachmentLogger
}

interface NormalizedForwarderImageAttachments {
    media: MediaFile[]
    cleanup: () => void
    compressedCount: number
}

type NormalizedImageResult = {
    path: string
    size_bytes: number
}

interface ImageDimensions {
    width: number
    height: number
}

interface CompressionAttempt {
    maxDimension: number
    quality: number
}

const TALL_IMAGE_HEIGHT_WIDTH_RATIO = 3
const COMPRESSION_ATTEMPTS: CompressionAttempt[] = [
    { maxDimension: 3200, quality: 3 },
    { maxDimension: 2880, quality: 3 },
    { maxDimension: 2560, quality: 4 },
    { maxDimension: 2400, quality: 4 },
    { maxDimension: 2400, quality: 5 },
    { maxDimension: 2200, quality: 5 },
    { maxDimension: 2048, quality: 5 },
    { maxDimension: 1800, quality: 6 },
    { maxDimension: 1600, quality: 6 },
    { maxDimension: 1440, quality: 7 },
    { maxDimension: 1280, quality: 7 },
    { maxDimension: 1080, quality: 8 },
    { maxDimension: 900, quality: 9 },
    { maxDimension: 720, quality: 10 },
    { maxDimension: 540, quality: 11 },
    { maxDimension: 420, quality: 12 },
]
const LEGACY_KIB_IMAGE_LIMIT_MAX = 10_000
const SPLIT_IMAGE_QUALITY = 4

function normalizeConfiguredImageMaxBytes(value: number) {
    if (value > 0 && value <= LEGACY_KIB_IMAGE_LIMIT_MAX) {
        return value * 1024
    }
    return value
}

function ensureDirectory(dirPath: string) {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true })
    }
}

function normalizeMaxImageBytes(value?: number) {
    if (!Number.isFinite(value) || Number(value) <= 0) {
        return DEFAULT_FORWARDER_IMAGE_MAX_BYTES
    }
    return Math.max(128_000, Math.floor(normalizeConfiguredImageMaxBytes(Number(value))))
}

function isImageLikeMedia(item: MediaFile) {
    return item.media_type === 'photo' || item.media_type === 'video_thumbnail'
}

function statSize(filePath: string) {
    try {
        return fs.statSync(filePath).size
    } catch {
        return null
    }
}

function safeEvenDimension(value: number) {
    const rounded = Math.max(2, Math.round(value))
    return rounded % 2 === 0 ? rounded : rounded - 1
}

function exceedsImageDimensionLimit(dimensions: ImageDimensions | null, maxEdgePx: number, maxPixels: number) {
    if (!dimensions || dimensions.width <= 0 || dimensions.height <= 0) {
        return false
    }
    return dimensions.width > maxEdgePx || dimensions.height > maxEdgePx || dimensions.width * dimensions.height > maxPixels
}

function fitDimensions(
    dimensions: ImageDimensions | null,
    maxDimension: number,
    limits: { maxEdgePx: number; maxPixels: number },
) {
    if (!dimensions || dimensions.width <= 0 || dimensions.height <= 0) {
        return `${maxDimension}:${maxDimension}:force_original_aspect_ratio=decrease`
    }

    const isTallImage = dimensions.height / dimensions.width >= TALL_IMAGE_HEIGHT_WIDTH_RATIO
    const constrainedDimension = isTallImage ? dimensions.width : Math.max(dimensions.width, dimensions.height)
    let ratio = Math.min(1, maxDimension / constrainedDimension)
    const longestEdge = Math.max(dimensions.width, dimensions.height)
    if (longestEdge * ratio > limits.maxEdgePx) {
        ratio = Math.min(ratio, limits.maxEdgePx / longestEdge)
    }
    const projectedPixels = dimensions.width * dimensions.height * ratio * ratio
    if (projectedPixels > limits.maxPixels) {
        ratio = Math.min(ratio, Math.sqrt(limits.maxPixels / (dimensions.width * dimensions.height)))
    }
    return `${safeEvenDimension(dimensions.width * ratio)}:${safeEvenDimension(dimensions.height * ratio)}`
}

function probeImageDimensions(filePath: string, ffprobePath: string): ImageDimensions | null {
    try {
        const output = execFileSync(
            ffprobePath,
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
            { encoding: 'utf8', timeout: 10_000 },
        )
            .trim()
            .split('\n')[0]
        const [width, height] = output.split('x').map((part) => Number(part))
        if (!Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0) {
            return null
        }
        return { width, height }
    } catch {
        return null
    }
}

function compressedOutputPath(identity: string, attempt: CompressionAttempt) {
    ensureDirectory(COMPRESSED_IMAGE_DIR)
    const hash = crypto
        .createHash('sha1')
        .update(`${COMPRESSION_CACHE_VERSION}:${identity}:${attempt.maxDimension}:${attempt.quality}`)
        .digest('hex')
        .slice(0, 20)
    const base = path.basename(identity, path.extname(identity)).replace(/[^a-zA-Z0-9._-]+/g, '_') || 'image'
    return path.join(COMPRESSED_IMAGE_DIR, `${base}-${attempt.maxDimension}-q${attempt.quality}-${hash}.jpg`)
}

function splitChunkOutputPath(identity: string, partIndex: number) {
    ensureDirectory(COMPRESSED_IMAGE_DIR)
    const hash = crypto
        .createHash('sha1')
        .update(`${COMPRESSION_CACHE_VERSION}:${identity}:split:${partIndex}`)
        .digest('hex')
        .slice(0, 20)
    const base = path.basename(identity, path.extname(identity)).replace(/[^a-zA-Z0-9._-]+/g, '_') || 'image'
    return path.join(COMPRESSED_IMAGE_DIR, `${base}-part${String(partIndex + 1).padStart(2, '0')}-${hash}.jpg`)
}

function resolveContentIdentity(
    item: MediaFile,
    sourcePath: string,
    limits: { maxEdgePx: number; maxPixels: number },
    maxImageBytes: number,
) {
    const limitsPart = `${maxImageBytes}:${limits.maxEdgePx}:${limits.maxPixels}`
    if (item.content_hash) {
        return `hash:${item.content_hash}:${limitsPart}`
    }
    const size = statSize(sourcePath)
    if (size === null) {
        return null
    }
    let mtimeMs = 0
    try {
        mtimeMs = fs.statSync(sourcePath).mtimeMs
    } catch {
        return null
    }
    return `path:${sourcePath}:${size}:${mtimeMs}:${limitsPart}`
}

function cachedOutputSizes(identity: string, attempts: CompressionAttempt[]): Array<{ path: string; size: number }> {
    const found: Array<{ path: string; size: number }> = []
    for (const attempt of attempts) {
        const outputPath = compressedOutputPath(identity, attempt)
        const size = statSize(outputPath)
        if (size !== null && size > 0) {
            found.push({ path: outputPath, size })
        }
    }
    return found
}

function splitCachedResults(
    identity: string,
    chunkCount: number,
    maxImageBytes: number,
): NormalizedImageResult[] | null {
    const results: NormalizedImageResult[] = []
    for (let index = 0; index < chunkCount; index += 1) {
        const outputPath = splitChunkOutputPath(identity, index)
        const size = statSize(outputPath)
        if (size === null || size <= 0 || size > maxImageBytes) {
            return null
        }
        results.push({ path: outputPath, size_bytes: size })
    }
    return results.length > 0 ? results : null
}

function splitTallImageUnderLimit(
    sourcePath: string,
    identity: string | null,
    dimensions: ImageDimensions | null,
    maxImageBytes: number,
    limits: { maxEdgePx: number; maxPixels: number },
    options: NormalizeForwarderImageAttachmentsOptions,
): { results: NormalizedImageResult[] | null; fromCache: boolean } {
    if (!dimensions || dimensions.width <= 0 || dimensions.height <= limits.maxEdgePx) {
        return { results: null, fromCache: false }
    }
    const ffmpegPath = options.ffmpegPath || process.env.FFMPEG_PATH || 'ffmpeg'
    const chunkHeight = Math.max(2, safeEvenDimension(Math.min(limits.maxEdgePx, Math.floor(limits.maxPixels / dimensions.width))))
    if (chunkHeight <= 0 || dimensions.height <= chunkHeight) {
        return { results: null, fromCache: false }
    }
    const chunkCount = Math.ceil(dimensions.height / chunkHeight)
    if (identity) {
        const cached = splitCachedResults(identity, chunkCount, maxImageBytes)
        if (cached) {
            return { results: cached, fromCache: true }
        }
    }
    const results: NormalizedImageResult[] = []
    try {
        for (let top = 0, index = 0; top < dimensions.height; top += chunkHeight, index += 1) {
            const height = Math.min(chunkHeight, dimensions.height - top)
            const outputPath = identity
                ? splitChunkOutputPath(identity, index)
                : path.join(
                      COMPRESSED_IMAGE_DIR,
                      `${crypto.createHash('sha1').update(`${sourcePath}:${Date.now()}:${Math.random()}:split:${index}`).digest('hex').slice(0, 12)}.jpg`,
                  )
            const tmpPath = path.join(COMPRESSED_IMAGE_DIR, `.tmp-${process.pid}-${path.basename(outputPath)}`)
            execFileSync(
                ffmpegPath,
                [
                    '-y',
                    '-v',
                    'error',
                    '-i',
                    sourcePath,
                    '-vf',
                    `crop=${dimensions.width}:${height}:0:${top}`,
                    '-frames:v',
                    '1',
                    '-q:v',
                    String(SPLIT_IMAGE_QUALITY),
                    '-pix_fmt',
                    'yuvj420p',
                    '-map_metadata',
                    '-1',
                    tmpPath,
                ],
                { stdio: 'ignore', timeout: 30_000 },
            )
            fs.renameSync(tmpPath, outputPath)
            const size = statSize(outputPath)
            if (size === null || size > maxImageBytes) {
                fs.rmSync(outputPath, { force: true })
                throw new Error(`split chunk ${index + 1} exceeded byte limit`)
            }
            results.push({ path: outputPath, size_bytes: size })
        }
        return { results: results.length > 0 ? results : null, fromCache: false }
    } catch {
        for (const result of results) {
            fs.rmSync(result.path, { force: true })
        }
        return { results: null, fromCache: false }
    }
}


function compressImageUnderLimit(
    sourcePath: string,
    maxImageBytes: number,
    limits: { maxEdgePx: number; maxPixels: number },
    options: NormalizeForwarderImageAttachmentsOptions,
    identity?: string | null,
): { results: NormalizedImageResult[] | null; fromCache: boolean } {
    const originalSize = statSize(sourcePath)
    if (originalSize === null) {
        return { results: null, fromCache: false }
    }

    const ffmpegPath = options.ffmpegPath || process.env.FFMPEG_PATH || 'ffmpeg'
    const ffprobePath = options.ffprobePath || process.env.FFPROBE_PATH || 'ffprobe'

    // Content-addressed cache fast path: previous runs with the same source
    // bytes + limits left deterministic outputs; reuse the smallest cached
    // variant under the byte limit without ffprobe or ffmpeg.
    if (identity) {
        const cached = cachedOutputSizes(identity, COMPRESSION_ATTEMPTS)
            .filter((entry) => entry.size <= maxImageBytes)
            .sort((a, b) => a.size - b.size)
        if (cached.length > 0) {
            const best = cached[0]!
            options.log?.debug?.(`Reused cached compressed attachment for ${path.basename(sourcePath)} (${best.size} bytes)`)
            return { results: [{ path: best.path, size_bytes: best.size }], fromCache: true }
        }
    }

    const dimensions = probeImageDimensions(sourcePath, ffprobePath)
    const splitOutcome = splitTallImageUnderLimit(sourcePath, identity, dimensions, maxImageBytes, limits, options)
    if (splitOutcome.results) {
        return splitOutcome
    }
    const dimensionLimited = exceedsImageDimensionLimit(dimensions, limits.maxEdgePx, limits.maxPixels)
    if (originalSize <= maxImageBytes && !dimensionLimited) {
        return { results: null, fromCache: false }
    }
    let bestPath: string | null = null
    let bestSize = Number.POSITIVE_INFINITY

    for (const attempt of COMPRESSION_ATTEMPTS) {
        const outputPath = identity
            ? compressedOutputPath(identity, attempt)
            : path.join(
                  COMPRESSED_IMAGE_DIR,
                  `${crypto.createHash('sha1').update(`${sourcePath}:${Date.now()}:${Math.random()}:${attempt.maxDimension}:${attempt.quality}`).digest('hex').slice(0, 16)}.jpg`,
              )
        // A cached attempt that still exceeds the limit is a deterministic miss:
        // skip re-running ffmpeg for it (the source and flags are unchanged).
        const existingSize = statSize(outputPath)
        if (existingSize !== null && existingSize > 0 && existingSize > maxImageBytes) {
            continue
        }
        if (existingSize !== null && existingSize > 0) {
            if (existingSize < bestSize) {
                bestPath = outputPath
                bestSize = existingSize
            }
            return { results: [{ path: outputPath, size_bytes: existingSize }], fromCache: true }
        }
        const tmpPath = path.join(COMPRESSED_IMAGE_DIR, `.tmp-${process.pid}-${path.basename(outputPath)}`)
        try {
            execFileSync(
                ffmpegPath,
                [
                    '-y',
                    '-v',
                    'error',
                    '-i',
                    sourcePath,
                    '-vf',
                    `scale=${fitDimensions(dimensions, attempt.maxDimension, limits)}`,
                    '-frames:v',
                    '1',
                    '-q:v',
                    String(attempt.quality),
                    '-pix_fmt',
                    'yuvj420p',
                    '-map_metadata',
                    '-1',
                    tmpPath,
                ],
                { stdio: 'ignore', timeout: 30_000 },
            )
            fs.renameSync(tmpPath, outputPath)
            const compressedSize = statSize(outputPath)
            if (compressedSize === null) {
                continue
            }
            if (compressedSize < bestSize) {
                bestPath = outputPath
                bestSize = compressedSize
            }
            if (compressedSize <= maxImageBytes) {
                options.log?.info?.(
                    `Compressed image attachment ${path.basename(sourcePath)} from ${originalSize} to ${compressedSize} bytes`,
                )
                return {
                    results: [
                        {
                            path: outputPath,
                            size_bytes: compressedSize,
                        },
                    ],
                    fromCache: false,
                }
            }
        } catch {
            fs.rmSync(tmpPath, { force: true })
        }
    }

    if (bestPath) {
        if (bestSize <= maxImageBytes) {
            return {
                results: [
                    {
                        path: bestPath,
                        size_bytes: bestSize,
                    },
                ],
                fromCache: false,
            }
        }
    }

    options.log?.warn?.(
        `Could not compress image attachment ${path.basename(sourcePath)} under ${maxImageBytes} bytes; keeping original`,
    )
    return { results: null, fromCache: false }
}

function normalizeForwarderImageAttachments(
    media: MediaFile[],
    options: NormalizeForwarderImageAttachmentsOptions = {},
): NormalizedForwarderImageAttachments {
    const maxImageBytes = normalizeMaxImageBytes(options.maxImageBytes)
    const limits = {
        maxEdgePx: Math.max(1, Math.floor(Number(options.maxImageEdgePx || DEFAULT_FORWARDER_IMAGE_MAX_EDGE_PX))),
        maxPixels: Math.max(1, Math.floor(Number(options.maxImagePixels || DEFAULT_FORWARDER_IMAGE_MAX_PIXELS))),
    }
    const cleanupPaths: string[] = []
    const compressedByPath = new Map<string, NormalizedImageResult[]>()
    let compressedCount = 0

    const normalized = media.flatMap((item) => {
        if (!isImageLikeMedia(item)) {
            return [item]
        }

        const existing = compressedByPath.get(item.path)
        if (existing) {
            return existing.map((result) => ({
                ...item,
                path: result.path,
                size_bytes: result.size_bytes,
            }))
        }

        const identity = resolveContentIdentity(item, item.path, limits, maxImageBytes)
        const outcome = compressImageUnderLimit(item.path, maxImageBytes, limits, options, identity)
        if (!outcome.results) {
            const size = statSize(item.path)
            return [
                size === null
                    ? item
                    : {
                          ...item,
                          size_bytes: item.size_bytes || size,
                      },
            ]
        }

        compressedByPath.set(item.path, outcome.results)
        // Only newly-created outputs are cleaned up after the send; cached
        // variants survive for reuse and are reaped by the media-cache job.
        if (!outcome.fromCache) {
            cleanupPaths.push(...outcome.results.map((entry) => entry.path))
        }
        compressedCount += outcome.results.length
        return outcome.results.map((entry) => ({
            ...item,
            path: entry.path,
            size_bytes: entry.size_bytes,
        }))
    })

    return {
        media: normalized,
        compressedCount,
        cleanup: () => {
            for (const filePath of cleanupPaths) {
                fs.rmSync(filePath, { force: true })
            }
        },
    }
}

function resolveForwarderImageMaxBytes(config?: { max_image_bytes?: number; image_max_bytes?: number }) {
    return normalizeMaxImageBytes(config?.max_image_bytes ?? config?.image_max_bytes)
}

export {
    DEFAULT_FORWARDER_IMAGE_MAX_BYTES,
    normalizeForwarderImageAttachments,
    resolveForwarderImageMaxBytes,
}
