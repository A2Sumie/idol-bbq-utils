import { CACHE_DIR_ROOT } from '@/config'
import type { MediaFile } from '@/middleware/forwarder/base'
import type { Logger } from '@idol-bbq-utils/log'
import { execFileSync } from 'child_process'
import crypto from 'crypto'
import fs from 'fs'
import path from 'path'

const DEFAULT_FORWARDER_IMAGE_MAX_BYTES = 4_000_000
const COMPRESSED_IMAGE_DIR = path.join(CACHE_DIR_ROOT, 'media', 'forwarder-compressed')

type ImageAttachmentLogger = Partial<Pick<Logger, 'debug' | 'info' | 'warn'>>

interface NormalizeForwarderImageAttachmentsOptions {
    maxImageBytes?: number
    ffmpegPath?: string
    ffprobePath?: string
    log?: ImageAttachmentLogger
}

interface NormalizedForwarderImageAttachments {
    media: MediaFile[]
    cleanup: () => void
    compressedCount: number
}

interface ImageDimensions {
    width: number
    height: number
}

interface CompressionAttempt {
    maxDimension: number
    quality: number
}

const COMPRESSION_ATTEMPTS: CompressionAttempt[] = [
    { maxDimension: 2400, quality: 3 },
    { maxDimension: 2048, quality: 4 },
    { maxDimension: 1600, quality: 5 },
    { maxDimension: 1280, quality: 6 },
    { maxDimension: 1080, quality: 7 },
    { maxDimension: 900, quality: 8 },
    { maxDimension: 720, quality: 9 },
    { maxDimension: 540, quality: 10 },
    { maxDimension: 420, quality: 12 },
]

function ensureDirectory(dirPath: string) {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true })
    }
}

function normalizeMaxImageBytes(value?: number) {
    if (!Number.isFinite(value) || Number(value) <= 0) {
        return DEFAULT_FORWARDER_IMAGE_MAX_BYTES
    }
    return Math.max(128_000, Math.floor(Number(value)))
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

function fitDimensions(dimensions: ImageDimensions | null, maxDimension: number) {
    if (!dimensions || dimensions.width <= 0 || dimensions.height <= 0) {
        return `${maxDimension}:${maxDimension}:force_original_aspect_ratio=decrease`
    }

    const ratio = Math.min(1, maxDimension / Math.max(dimensions.width, dimensions.height))
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

function compressImageUnderLimit(
    sourcePath: string,
    maxImageBytes: number,
    options: NormalizeForwarderImageAttachmentsOptions,
) {
    const originalSize = statSize(sourcePath)
    if (originalSize === null || originalSize <= maxImageBytes) {
        return null
    }

    const ffmpegPath = options.ffmpegPath || process.env.FFMPEG_PATH || 'ffmpeg'
    const ffprobePath = options.ffprobePath || process.env.FFPROBE_PATH || 'ffprobe'
    const dimensions = probeImageDimensions(sourcePath, ffprobePath)
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
                    `scale=${fitDimensions(dimensions, attempt.maxDimension)}`,
                    '-frames:v',
                    '1',
                    '-q:v',
                    String(attempt.quality),
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
                return {
                    path: outputPath,
                    size_bytes: compressedSize,
                }
            }
        } catch {
            fs.rmSync(outputPath, { force: true })
        }
    }

    if (bestPath) {
        if (bestSize <= maxImageBytes) {
            return {
                path: bestPath,
                size_bytes: bestSize,
            }
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
    const cleanupPaths: string[] = []
    const compressedByPath = new Map<string, { path: string; size_bytes: number }>()
    let compressedCount = 0

    const normalized = media.map((item) => {
        if (!isImageLikeMedia(item)) {
            return item
        }

        const existing = compressedByPath.get(item.path)
        if (existing) {
            return {
                ...item,
                path: existing.path,
                size_bytes: existing.size_bytes,
            }
        }

        const result = compressImageUnderLimit(item.path, maxImageBytes, options)
        if (!result) {
            const size = statSize(item.path)
            return size === null
                ? item
                : {
                      ...item,
                      size_bytes: item.size_bytes || size,
                  }
        }

        compressedByPath.set(item.path, result)
        cleanupPaths.push(result.path)
        compressedCount += 1
        return {
            ...item,
            path: result.path,
            size_bytes: result.size_bytes,
        }
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
