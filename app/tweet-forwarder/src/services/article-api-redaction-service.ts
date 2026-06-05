import crypto from 'crypto'
import { redactSecrets, SENSITIVE_KEY_PATTERN } from './redaction-service'

const REDACTED = '[redacted]'

type JsonRecord = Record<string, unknown>

const PATH_KEY_PATTERN =
    /(^|[_-])(path|file|file_path|filepath|local_path|localpath|media_path|mediapath|cache_path|cachepath|download_path|downloadpath)([_-]|$)/i
const HOST_PATH_PATTERN =
    /^(?:\/(?:Users|home|tmp|app|var|mnt|Volumes|opt|etc)\/.+|[A-Za-z]:\\.+)$/

function hasValue(value: unknown) {
    return value !== null && value !== undefined && value !== ''
}

function compactHash(value: unknown) {
    if (!hasValue(value)) {
        return null
    }
    return crypto.createHash('sha256').update(String(value)).digest('hex').slice(0, 16)
}

function isHostPath(value: unknown) {
    return typeof value === 'string' && HOST_PATH_PATTERN.test(value.trim())
}

function pathMeta(value: unknown) {
    return {
        redacted_path: isHostPath(value),
        path_present: hasValue(value),
        path_hash: isHostPath(value) ? compactHash(value) : null,
    }
}

function sanitizeArticleValue(value: unknown, key = ''): unknown {
    if (Array.isArray(value)) {
        return value.map((item) => sanitizeArticleValue(item))
    }
    if (!value || typeof value !== 'object') {
        if (PATH_KEY_PATTERN.test(key) && isHostPath(value)) {
            return REDACTED
        }
        return value
    }

    const record = redactSecrets(value as JsonRecord) as JsonRecord
    const output: JsonRecord = {}
    for (const [entryKey, entryValue] of Object.entries(record)) {
        if (SENSITIVE_KEY_PATTERN.test(entryKey)) {
            output[entryKey] = hasValue(entryValue) ? REDACTED : entryValue
            continue
        }
        if (PATH_KEY_PATTERN.test(entryKey) && isHostPath(entryValue)) {
            output[entryKey] = REDACTED
            output[`${entryKey}_meta`] = pathMeta(entryValue)
            continue
        }
        output[entryKey] = sanitizeArticleValue(entryValue, entryKey)
    }
    return output
}

function sanitizeArticleForApi<T>(article: T): T {
    return sanitizeArticleValue(article) as T
}

function sanitizeArticlesForApi<T>(articles: Array<T>): Array<T> {
    return articles.map((article) => sanitizeArticleForApi(article))
}

export { sanitizeArticleForApi, sanitizeArticlesForApi }
