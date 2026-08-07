import { CACHE_DIR_ROOT } from '@/config'
import type { MediaFile } from '@/middleware/forwarder/base'
import type { Logger } from '@idol-bbq-utils/log'
import { execFileSync } from 'child_process'
import crypto from 'crypto'
import fs from 'fs'
import path from 'path'

const DEFAULT_FORWARDER_IMAGE_MAX_BYTES = 4_000_000
// 30000px cards (13-photo blog renders) exceed QQ rich media transfer limits (~25600px longest edge)
// and are rejected with -1 rich media transfer failed; keep chunks well under QQ/Bilibili caps.
const DEFAULT_FORWARDER_IMAGE_MAX_EDGE_PX = 10_000
const DEFAULT_FORWARDER_IMAGE_MAX_PIXELS = 40_000_000
const COMPRESSED_IMAGE_DIR = path.join(CACHE_DIR_ROOT, 'media', 'forwarder-compressed')

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

function compressedOutputPath(sourcePath: string, attempt: CompressionAttempt) {
    ensureDirectory(COMPRESSED_IMAGE_DIR)
    const hash = crypto
        .createHash('sha1')
        .update(`${sourcePath}:${Date.now()}:${Math.random()}:${attempt.maxDimension}:${attempt.quality}`)
        .digest('hex')
        .slice(0, 16)
    const base = path.basename(sourcePath, path.extname(sourcePath)).replace(/[^a-zA-Z0-9._-]+/g, '_') || 'image'
    return path.join(COMPRESSED_IMAGE_DIR, `${base}-${attempt.maxDimension}-q${attempt.quality}-${hash}.jpg`)
}

function splitTallImageUnderLimit(
    sourcePath: string,
    dimensions: ImageDimensions | null,
    maxImageBytes: number,
    limits: { maxEdgePx: number; maxPixels: number },
    options: NormalizeForwarderImageAttachmentsOptions,
): NormalizedImageResult[] | null {
    if (!dimensions || dimensions.width <= 0 || dimensions.height <= limits.maxEdgePx) {
        return null
    }
    const ffmpegPath = options.ffmpegPath || process.env.FFMPEG_PATH || 'ffmpeg'
    const chunkHeight = Math.max(2, safeEvenDimension(Math.min(limits.maxEdgePx, Math.floor(limits.maxPixels / dimensions.width))))
    if (chunkHeight <= 0 || dimensions.height <= chunkHeight) {
        return null
    }
    const results: NormalizedImageResult[] = []
    const base = path.basename(sourcePath, path.extname(sourcePath)).replace(/[^a-zA-Z0-9._-]+/g, '_') || 'image'
    const runHash = crypto.createHash('sha1').update(`${sourcePath}:${Date.now()}:${Math.random()}:split`).digest('hex').slice(0, 12)
    try {
        for (let top = 0, index = 0; top < dimensions.height; top += chunkHeight, index += 1) {
            const height = Math.min(chunkHeight, dimensions.height - top)
            const outputPath = path.join(COMPRESSED_IMAGE_DIR, `${base}-part${String(index + 1).padStart(2, '0')}-${runHash}.jpg`)
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
                    outputPath,
                ],
                { stdio: 'ignore', timeout: 30_000 },
            )
            const size = statSize(outputPath)
            if (size === null || size > maxImageBytes) {
                fs.rmSync(outputPath, { force: true })
                throw new Error(`split chunk ${index + 1} exceeded byte limit`)
            }
            results.push({ path: outputPath, size_bytes: size })
        }
        return results.length > 0 ? results : null
    } catch {
        for (const result of results) {
            fs.rmSync(result.path, { force: true })
        }
        return null
    }
}


function compressImageUnderLimit(
    sourcePath: string,
    maxImageBytes: number,
    limits: { maxEdgePx: number; maxPixels: number },
    options: NormalizeForwarderImageAttachmentsOptions,
) {
    const originalSize = statSize(sourcePath)
    if (originalSize === null) {
        return null
    }

    const ffmpegPath = options.ffmpegPath || process.env.FFMPEG_PATH || 'ffmpeg'
    const ffprobePath = options.ffprobePath || process.env.FFPROBE_PATH || 'ffprobe'
    const dimensions = probeImageDimensions(sourcePath, ffprobePath)
    const splitResults = splitTallImageUnderLimit(sourcePath, dimensions, maxImageBytes, limits, options)
    if (splitResults) {
        return splitResults
    }
    const dimensionLimited = exceedsImageDimensionLimit(dimensions, limits.maxEdgePx, limits.maxPixels)
    if (originalSize <= maxImageBytes && !dimensionLimited) {
        return null
    }
    let bestPath: string | null = null
    let bestSize = Number.POSITIVE_INFINITY

    for (const attempt of COMPRESSION_ATTEMPTS) {
        const outputPath = compressedOutputPath(sourcePath, attempt)
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
                    outputPath,
                ],
                { stdio: 'ignore', timeout: 30_000 },
            )
            const compressedSize = statSize(outputPath)
            if (compressedSize === null) {
                continue
            }
            if (compressedSize < bestSize) {
                if (bestPath && bestPath !== outputPath) {
                    fs.rmSync(bestPath, { force: true })
                }
                bestPath = outputPath
                bestSize = compressedSize
            } else {
                fs.rmSync(outputPath, { force: true })
            }
            if (compressedSize <= maxImageBytes) {
                options.log?.info?.(
                    `Compressed image attachment ${path.basename(sourcePath)} from ${originalSize} to ${compressedSize} bytes`,
                )
                return [
                    {
                        path: outputPath,
                        size_bytes: compressedSize,
                    },
                ]
            }
        } catch {
            fs.rmSync(outputPath, { force: true })
        }
    }

    if (bestPath) {
        if (bestSize <= maxImageBytes) {
            return [
                {
                    path: bestPath,
                    size_bytes: bestSize,
                },
            ]
        }
        fs.rmSync(bestPath, { force: true })
    }

    options.log?.warn?.(
        `Could not compress image attachment ${path.basename(sourcePath)} under ${maxImageBytes} bytes; keeping original`,
    )
    return null
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

        const result = compressImageUnderLimit(item.path, maxImageBytes, limits, options)
        if (!result) {
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

        compressedByPath.set(item.path, result)
        cleanupPaths.push(...result.map((entry) => entry.path))
        compressedCount += result.length
        return result.map((entry) => ({
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
