import path from 'path'
import crypto from 'crypto'
import { redactRuntimeLogLineForApi } from './runtime-log-redaction-service'

const REDACTED = '[redacted]'
const REDACTED_PATH = '[redacted-path]'

type JsonRecord = Record<string, unknown>

function hasValue(value: unknown) {
    return value !== null && value !== undefined && value !== ''
}

function compactHash(value: unknown) {
    if (!hasValue(value)) {
        return null
    }
    return crypto.createHash('sha256').update(String(value)).digest('hex').slice(0, 16)
}

function basenameOf(value: unknown) {
    const normalized = String(value || '').trim()
    if (!normalized) {
        return null
    }
    return path.basename(normalized.replace(/\\/g, '/'))
}

function redactPathValue(value: unknown) {
    return hasValue(value) ? REDACTED : value
}

function publicPathSummary(value: unknown) {
    return {
        redacted_path: hasValue(value),
        path_present: hasValue(value),
        filename: basenameOf(value),
        path_hash: compactHash(value),
    }
}

function redactHostPathsInText(value: unknown) {
    if (!hasValue(value)) {
        return value
    }
    return redactRuntimeLogLineForApi(value)
}

function redactArchiveSummaryForApi<T extends JsonRecord>(item: T): T {
    return {
        ...item,
        localPath: redactPathValue(item.localPath),
        localPath_meta: publicPathSummary(item.localPath),
    } as T
}

function redactArchiveUploadDefaultsForApi<T extends JsonRecord>(defaults: T): T {
    return {
        ...defaults,
        cookieSourcePath: redactPathValue(defaults.cookieSourcePath),
        helperPath: redactPathValue(defaults.helperPath),
        pythonPath: redactPathValue(defaults.pythonPath),
        cookieSourcePath_meta: publicPathSummary(defaults.cookieSourcePath),
        helperPath_meta: publicPathSummary(defaults.helperPath),
        pythonPath_meta: publicPathSummary(defaults.pythonPath),
    } as T
}

function redactRelatedFileForApi<T extends JsonRecord>(item: T): T {
    return {
        ...item,
        path: redactPathValue(item.path),
        path_meta: publicPathSummary(item.path),
    } as T
}

function redactSuggestedUploadForApi<T extends JsonRecord>(suggestedUpload: T): T {
    return {
        ...suggestedUpload,
        description: redactHostPathsInText(suggestedUpload.description),
        cookieSourcePath: redactPathValue(suggestedUpload.cookieSourcePath),
        cookieSourcePath_meta: publicPathSummary(suggestedUpload.cookieSourcePath),
    } as T
}

function redactArchiveListForApi<T extends JsonRecord>(payload: T): T {
    const items = Array.isArray(payload.items) ? payload.items.map((item) => redactArchiveSummaryForApi(item)) : payload.items
    const defaults =
        payload.defaults && typeof payload.defaults === 'object'
            ? redactArchiveUploadDefaultsForApi(payload.defaults as JsonRecord)
            : payload.defaults
    return {
        ...payload,
        items,
        defaults,
        redacted: true,
    } as T
}

function redactArchiveDetailForApi<T extends JsonRecord>(payload: T): T {
    const relatedFiles = Array.isArray(payload.relatedFiles)
        ? payload.relatedFiles.map((item) => redactRelatedFileForApi(item))
        : payload.relatedFiles
    const suggestedUpload =
        payload.suggestedUpload && typeof payload.suggestedUpload === 'object'
            ? redactSuggestedUploadForApi(payload.suggestedUpload as JsonRecord)
            : payload.suggestedUpload
    return {
        ...redactArchiveSummaryForApi(payload),
        relatedFiles,
        suggestedUpload,
        redacted: true,
    } as T
}

function redactArchiveUploadResultForApi<T extends JsonRecord>(payload: T): T {
    return {
        ...payload,
        cookieSourcePath: redactPathValue(payload.cookieSourcePath),
        uploadedPath: redactPathValue(payload.uploadedPath),
        trimmedPath: redactPathValue(payload.trimmedPath),
        coverPath: redactPathValue(payload.coverPath),
        cookieSourcePath_meta: publicPathSummary(payload.cookieSourcePath),
        uploadedPath_meta: publicPathSummary(payload.uploadedPath),
        trimmedPath_meta: publicPathSummary(payload.trimmedPath),
        coverPath_meta: publicPathSummary(payload.coverPath),
        stdout: hasValue(payload.stdout)
            ? {
                  redacted_text: true,
                  text_present: true,
                  text_length: String(payload.stdout).length,
              }
            : payload.stdout,
        redacted: true,
    } as T
}

function normalizeRedactedArchiveUploadRequest<T extends JsonRecord>(payload: T): T {
    if (payload.cookieSourcePath !== REDACTED && payload.cookieSourcePath !== REDACTED_PATH) {
        return payload
    }
    const copy = { ...payload }
    delete copy.cookieSourcePath
    return copy as T
}

function redactArchiveErrorMessageForApi(error: unknown) {
    return redactRuntimeLogLineForApi(error instanceof Error ? error.message : String(error))
}

export {
    normalizeRedactedArchiveUploadRequest,
    redactArchiveDetailForApi,
    redactArchiveErrorMessageForApi,
    redactArchiveListForApi,
    redactArchiveUploadResultForApi,
}
