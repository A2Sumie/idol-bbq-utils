/**
 * SnowLuma-aware media source selection for the QQ forwarder.
 *
 * Legacy behavior (NapCat era) always sent `file://<local path>`, which only
 * works when the forwarder and the OneBot bridge share a filesystem (the
 * docker-compose `/tmp` mount). SnowLuma's `pickMediaSource`
 * (packages/onebot/src/element-codecs.ts) additionally accepts
 *
 *   - `base64://<...>` / `data:...;base64,<...>` inline bytes,
 *   - `http(s)://<...>` URLs which the bridge downloads itself,
 *   - `file://<...>` / plain paths on the BOT host — which is exactly what
 *     `upload_file_stream` returns after a chunked upload (packages/onebot/
 *     src/actions/stream-file.ts: base64 `chunk_data` calls keyed by
 *     `stream_id`, completion responds with `file_path`).
 *
 * `selectQQMediaSource` is a pure decision table (unit-tested) and
 * `buildQQMediaFileValue` materializes the decision (stat / read / chunked
 * upload). `media_delivery` defaults to `file` so a stock config keeps the
 * exact historical wire shape (and NapCat compatibility); `auto`/`inline`/
 * `upload`/`url` are explicit opt-ins per target (or via
 * `IDOL_BBQ_QQ_MEDIA_DELIVERY`), with `file://` retained as the fallback of
 * every other mode.
 */
import crypto from 'crypto'
import { createHash } from 'crypto'
import { createReadStream } from 'fs'
import { stat } from 'fs/promises'
import axios from 'axios'
import type { Logger } from '@idol-bbq-utils/log'
import { classifyOneBotResponse, classifyOneBotTransportError } from '@/middleware/forwarder/onebot-semantics'

export type QQMediaDeliveryMode = 'file' | 'auto' | 'inline' | 'upload' | 'url'
export type QQMediaSourceMethod = 'file' | 'inline' | 'upload' | 'url'

export const QQ_MEDIA_DELIVERY_MODES: QQMediaDeliveryMode[] = ['file', 'auto', 'inline', 'upload', 'url']

export interface QQMediaDeliveryConfig {
    mode: QQMediaDeliveryMode
    /** Public base URL that serves the media cache dir (url mode / auto). */
    urlBase: string | null
    /** Raw file size cap for inline base64 embedding. */
    inlineMaxBytes: number
    /** Raw file size cap for chunked upload (larger falls back to file://). */
    uploadMaxBytes: number
    /** Decoded bytes per upload_file_stream chunk. */
    uploadChunkBytes: number
    /** Per-request timeout for one upload chunk. */
    uploadRequestTimeoutMs: number
    /** Overall deadline for a whole chunked upload. */
    uploadDeadlineMs: number
    /** Bot-side retention of the reassembled file (0 = keep forever). */
    uploadRetentionMs: number
}

export interface QQMediaDeliveryConfigLike {
    media_delivery?: string
    media_url_base?: string
    media_inline_max_bytes?: number
    media_upload_max_bytes?: number
    media_upload_chunk_bytes?: number
    media_upload_timeout_ms?: number
    media_upload_deadline_ms?: number
    media_upload_retention_ms?: number
}

export const QQ_MEDIA_DEFAULTS = {
    inlineMaxBytes: 2 * 1024 * 1024,
    uploadMaxBytes: 512 * 1024 * 1024,
    uploadChunkBytes: 1024 * 1024,
    uploadRequestTimeoutMs: 60_000,
    uploadDeadlineMs: 600_000,
    uploadRetentionMs: 3_600_000,
} as const

function positiveInt(value: unknown, fallback: number, min = 1): number {
    const parsed = Number(value)
    if (!Number.isFinite(parsed) || parsed < min) {
        return fallback
    }
    return Math.floor(parsed)
}

function normalizeMode(value: unknown): QQMediaDeliveryMode {
    const raw = String(value || '')
        .trim()
        .toLowerCase()
    return (QQ_MEDIA_DELIVERY_MODES as string[]).includes(raw) ? (raw as QQMediaDeliveryMode) : 'file'
}

/**
 * Resolve delivery config from the target's cfg_platform with
 * `IDOL_BBQ_QQ_MEDIA_*` env overrides (handy for a whole-host bridge switch
 * without editing assets/config.yaml).
 */
export function resolveQQMediaDeliveryConfig(config?: QQMediaDeliveryConfigLike): QQMediaDeliveryConfig {
    const envMode = process.env.IDOL_BBQ_QQ_MEDIA_DELIVERY
    const envUrlBase = process.env.IDOL_BBQ_QQ_MEDIA_URL_BASE
    return {
        mode: normalizeMode(envMode || config?.media_delivery),
        urlBase: String(envUrlBase || config?.media_url_base || '').trim() || null,
        inlineMaxBytes: positiveInt(
            process.env.IDOL_BBQ_QQ_MEDIA_INLINE_MAX_BYTES ?? config?.media_inline_max_bytes,
            QQ_MEDIA_DEFAULTS.inlineMaxBytes,
        ),
        uploadMaxBytes: positiveInt(
            process.env.IDOL_BBQ_QQ_MEDIA_UPLOAD_MAX_BYTES ?? config?.media_upload_max_bytes,
            QQ_MEDIA_DEFAULTS.uploadMaxBytes,
        ),
        uploadChunkBytes: positiveInt(
            process.env.IDOL_BBQ_QQ_MEDIA_UPLOAD_CHUNK_BYTES ?? config?.media_upload_chunk_bytes,
            QQ_MEDIA_DEFAULTS.uploadChunkBytes,
        ),
        uploadRequestTimeoutMs: positiveInt(
            process.env.IDOL_BBQ_QQ_MEDIA_UPLOAD_TIMEOUT_MS ?? config?.media_upload_timeout_ms,
            QQ_MEDIA_DEFAULTS.uploadRequestTimeoutMs,
        ),
        uploadDeadlineMs: positiveInt(
            process.env.IDOL_BBQ_QQ_MEDIA_UPLOAD_DEADLINE_MS ?? config?.media_upload_deadline_ms,
            QQ_MEDIA_DEFAULTS.uploadDeadlineMs,
        ),
        uploadRetentionMs: positiveInt(
            process.env.IDOL_BBQ_QQ_MEDIA_UPLOAD_RETENTION_MS ?? config?.media_upload_retention_ms,
            QQ_MEDIA_DEFAULTS.uploadRetentionMs,
        ),
    }
}

export interface QQMediaSourceInput {
    path: string
    /** File size in bytes, or null/undefined when the probe failed / file missing. */
    sizeBytes?: number | null
    /** Remote URL when the media item already carries one. */
    url?: string | null
}

export interface QQMediaSourceDecision {
    method: QQMediaSourceMethod
    /** Ready-to-use segment value for method file/url; '' for inline/upload. */
    value: string
    /** Why this method was chosen (diagnostics). */
    reason: string
}

export function resolveQQMediaUrl(input: QQMediaSourceInput, config: QQMediaDeliveryConfig): string | null {
    const explicit = String(input.url || '').trim()
    if (/^https?:\/\//i.test(explicit)) {
        return explicit
    }
    const base = String(config.urlBase || '').trim().replace(/\/+$/, '')
    if (!base) {
        return null
    }
    const fileName = input.path.split('/').pop() || input.path.split('\\').pop() || ''
    if (!fileName) {
        return null
    }
    return `${base}/${encodeURIComponent(fileName)}`
}

/**
 * Pure decision table. `sizeBytes === null` means "cannot embed/upload safely"
 * and routes to the legacy `file://` shape.
 */
export function selectQQMediaSource(
    input: QQMediaSourceInput,
    config: QQMediaDeliveryConfig,
): QQMediaSourceDecision {
    const size = input.sizeBytes
    const fileValue = `file://${input.path}`
    const url = resolveQQMediaUrl(input, config)

    const tryInline = (reason: string): QQMediaSourceDecision => {
        if (size !== null && size <= config.inlineMaxBytes) {
            return { method: 'inline', value: '', reason }
        }
        return { method: 'file', value: fileValue, reason: `${reason}; size ${size ?? 'unknown'} > inline cap` }
    }
    const tryUpload = (reason: string): QQMediaSourceDecision => {
        if (size !== null && size <= config.uploadMaxBytes) {
            return { method: 'upload', value: '', reason }
        }
        return { method: 'file', value: fileValue, reason: `${reason}; size ${size ?? 'unknown'} > upload cap` }
    }

    switch (config.mode) {
        case 'file':
            return { method: 'file', value: fileValue, reason: 'media_delivery=file (legacy shared-filesystem mode)' }
        case 'url':
            return url
                ? { method: 'url', value: url, reason: 'media_delivery=url' }
                : { method: 'file', value: fileValue, reason: 'media_delivery=url but no resolvable URL' }
        case 'inline':
            return tryInline('media_delivery=inline')
        case 'upload':
            return tryUpload('media_delivery=upload')
        case 'auto':
        default:
            if (url) {
                return { method: 'url', value: url, reason: 'auto: bridge-pullable URL available' }
            }
            if (size !== null && size <= config.inlineMaxBytes) {
                return { method: 'inline', value: '', reason: 'auto: small enough to inline' }
            }
            return tryUpload('auto: falls back to chunked upload')
    }
}

type PostLike = (path: string, body: unknown, options: unknown) => Promise<any>

function assertActionOk(res: any, action: string) {
    const verdict = classifyOneBotResponse(res, action)
    if (!verdict.ok) {
        throw new Error(verdict.detail)
    }
    return res
}

export interface UploadStreamOptions {
    endpoint: string
    token: string
    filePath: string
    fileName: string
    fileSize: number
    config: QQMediaDeliveryConfig
    log?: Logger
    post?: PostLike
}

/**
 * Chunked `upload_file_stream` upload (works on SnowLuma and NapCat stream
 * API). Returns the bot-local path to feed back into a media segment.
 *
 * The 3.47 GiB group-file upload incident taught us two things that are
 * enforced here: a per-chunk request timeout plus an overall deadline (so a
 * stalled stream can never pin a send slot forever), and a caller-side size cap
 * (`uploadMaxBytes`, 512 MiB default) so oversized artifacts route to the
 * group-file path instead of a message-media upload.
 */
export async function uploadFileViaOneBotStream(options: UploadStreamOptions): Promise<string> {
    const post: PostLike =
        options.post ||
        ((path, body, config) => axios.post(path, body, config as any))
    const { endpoint, token, filePath, fileName, fileSize, config } = options
    const chunkBytes = config.uploadChunkBytes
    const totalChunks = Math.max(1, Math.ceil(fileSize / chunkBytes))
    const streamId = crypto.randomUUID()
    const startedAt = Date.now()
    const headers: Record<string, string> = { 'Content-Type': 'application/json' }
    if (token?.trim()) {
        headers.Authorization = `Bearer ${token}`
    }
    const deadline = startedAt + config.uploadDeadlineMs

    const callUpload = async (payload: Record<string, unknown>) => {
        if (Date.now() > deadline) {
            throw new Error(
                `QQ upload_file_stream deadline exceeded (${config.uploadDeadlineMs}ms) for ${fileName}`,
            )
        }
        try {
            const res = await post(`${endpoint}/upload_file_stream`, payload, {
                headers,
                timeout: config.uploadRequestTimeoutMs,
            })
            return assertActionOk(res, 'upload_file_stream')
        } catch (error) {
            if (error instanceof Error && error.message.startsWith('QQ OneBot send failed')) {
                throw error
            }
            const transport = classifyOneBotTransportError(error)
            throw new Error(transport.detail)
        }
    }

    const hash = createHash('sha256')
    let expectedSha256: string | undefined
    for (let index = 0; index < totalChunks; index++) {
        const start = index * chunkBytes
        const end = Math.min(fileSize, start + chunkBytes)
        const buf = await readSlice(filePath, start, end - start)
        hash.update(buf)
        const isLast = index === totalChunks - 1
        if (isLast) {
            expectedSha256 = hash.digest('hex')
        }
        const res = await callUpload({
            stream_id: streamId,
            chunk_data: buf.toString('base64'),
            chunk_index: index,
            total_chunks: totalChunks,
            file_size: fileSize,
            filename: fileName,
            ...(isLast ? { is_complete: true, expected_sha256: expectedSha256 } : {}),
            file_retention: config.uploadRetentionMs,
        })
        // Bridges that merge inline (NapCat-style: the `is_complete` data call
        // returns the completion) already carry file_path here.
        const inlinePath = extractUploadedFilePath(res)
        if (inlinePath) {
            options.log?.debug(
                `QQ media uploaded via upload_file_stream: ${fileName} -> ${inlinePath} (${totalChunks} chunk(s), inline completion)`,
            )
            return inlinePath
        }
    }

    // SnowLuma semantics (packages/onebot/src/actions/stream-file.ts): a call
    // carrying chunk_data always acks `chunk_received`; the merge + completion
    // frame (`file_path/file_name/file_size/sha256`) only arrive from a
    // data-less `is_complete` call. Send it explicitly when the chunk frames
    // did not complete inline.
    const res = await callUpload({
        stream_id: streamId,
        is_complete: true,
        total_chunks: totalChunks,
        file_size: fileSize,
        filename: fileName,
        expected_sha256: expectedSha256,
        file_retention: config.uploadRetentionMs,
    })
    const filePathOnBot = extractUploadedFilePath(res)
    if (!filePathOnBot) {
        throw new Error(`QQ upload_file_stream completed without file_path for ${fileName}`)
    }
    options.log?.debug(
        `QQ media uploaded via upload_file_stream: ${fileName} -> ${filePathOnBot} (${totalChunks} chunk(s))`,
    )
    return filePathOnBot
}

function extractUploadedFilePath(res: any): string | null {
    const data = res?.data?.data
    const value = data?.file_path
    return typeof value === 'string' && value.trim() ? value.trim() : null
}

async function readSlice(filePath: string, start: number, length: number): Promise<Buffer> {
    return await new Promise<Buffer>((resolve, reject) => {
        const chunks: Buffer[] = []
        const stream = createReadStream(filePath, {
            start,
            ...(Number.isFinite(length) && length > 0 && length < Number.MAX_SAFE_INTEGER
                ? { end: start + length - 1 }
                : {}),
        })
        stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
        stream.on('error', reject)
        stream.on('end', () => resolve(Buffer.concat(chunks)))
    })
}

export interface BuildMediaFileOptions {
    config: QQMediaDeliveryConfig
    endpoint: string
    token: string
    log?: Logger
    post?: PostLike
    /** Overridable for tests. */
    probeSize?: (path: string) => Promise<number | null>
    readInline?: (path: string) => Promise<Buffer>
}

async function defaultProbeSize(path: string): Promise<number | null> {
    try {
        const info = await stat(path)
        return info.isFile() ? info.size : null
    } catch {
        return null
    }
}

/**
 * Materialize the `file` field value of an image/video segment. Missing files
 * keep the legacy `file://` shape (the bridge reports the failure with its own
 * wording), matching pre-SnowLuma behavior.
 */
export async function buildQQMediaFileValue(
    input: QQMediaSourceInput,
    options: BuildMediaFileOptions,
): Promise<{ value: string; method: QQMediaSourceMethod; reason: string }> {
    const probeSize = options.probeSize || defaultProbeSize
    const readInline =
        options.readInline ||
        (async (path: string) => {
            return await readSlice(path, 0, Number.MAX_SAFE_INTEGER)
        })
    const sizeBytes = input.sizeBytes === undefined ? await probeSize(input.path) : input.sizeBytes
    const effective: QQMediaSourceInput = { ...input, sizeBytes: sizeBytes ?? null }
    const decision = selectQQMediaSource(effective, options.config)

    if (decision.method === 'file' || decision.method === 'url') {
        return { value: decision.value, method: decision.method, reason: decision.reason }
    }
    if (effective.sizeBytes === null) {
        return { value: `file://${input.path}`, method: 'file', reason: `${decision.reason}; file unreadable` }
    }

    if (decision.method === 'inline') {
        const buf = await readInline(input.path)
        return {
            value: `base64://${buf.toString('base64')}`,
            method: 'inline',
            reason: decision.reason,
        }
    }

    const botPath = await uploadFileViaOneBotStream({
        endpoint: options.endpoint,
        token: options.token,
        filePath: input.path,
        fileName: input.path.split('/').pop() || input.path,
        fileSize: effective.sizeBytes,
        config: options.config,
        log: options.log,
        post: options.post,
    })
    return { value: `file://${botPath}`, method: 'upload', reason: decision.reason }
}
