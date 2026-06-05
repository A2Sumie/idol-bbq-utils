import path from 'path'

const REDACTED = '[redacted]'
const REDACTED_PATH = '[redacted-path]'
const REDACTED_URL = '[redacted-url]'

const SENSITIVE_ASSIGNMENT_PATTERN =
    /(["']?)([A-Za-z0-9_.-]*(?:secret|token|password|passwd|cookie|sessdata|bili_jct|csrf|auth[_-]?(?:token|key)|waf_bypass|api[_-]?key|source[_-]?ref|idempotency[_-]?key|route[_-]?key|target[_-]?id|article[_-]?key|synthetic[_-]?key|payload[_-]?hash|message[_-]?id)[A-Za-z0-9_.-]*)(\1)\s*([:=])\s*(?:"[^"]*"|'[^']*'|[^\s,;}\]]+)/gi
const AUTHORIZATION_HEADER_PATTERN = /\bAuthorization\s*[:=]\s*Bearer\s+[A-Za-z0-9._~+/=-]+/gi
const COOKIE_HEADER_PATTERN = /\b(Cookie|Set-Cookie)\s*[:=]\s*[^\r\n]+/gi
const BEARER_VALUE_PATTERN = /\bBearer\s+[A-Za-z0-9._~+/=-]+/gi
const URL_PATTERN = /\bhttps?:\/\/[^\s'")\]}>,;]+/gi
const ABSOLUTE_PATH_PATTERN =
    /(^|[\s(["'=])((?:\/(?:Users|home|tmp|app|var|mnt|Volumes|opt|etc)\/[^\s'")\]}>,;]+|[A-Za-z]:\\[^\s'")\]}>,;]+))/g
const SOURCE_REF_PATTERN = /\bnotification:(?:x|twitter|instagram|tiktok|youtube|website):[^\s'")\]}>,;]+/gi

function redactAssignment(match: string, quote: string, key: string, closeQuote: string, separator: string) {
    const normalizedSeparator = separator === ':' ? ': ' : '='
    const replacementValue = separator === ':' ? `"${REDACTED}"` : REDACTED
    return `${quote}${key}${closeQuote}${normalizedSeparator}${replacementValue}`
}

function redactRuntimeLogLineForApi(line: unknown) {
    let value = String(line ?? '')
    value = value.replace(AUTHORIZATION_HEADER_PATTERN, `Authorization: Bearer ${REDACTED}`)
    value = value.replace(COOKIE_HEADER_PATTERN, (_, key) => `${key}: ${REDACTED}`)
    value = value.replace(BEARER_VALUE_PATTERN, `Bearer ${REDACTED}`)
    value = value.replace(SENSITIVE_ASSIGNMENT_PATTERN, redactAssignment)
    value = value.replace(SOURCE_REF_PATTERN, `notification:${REDACTED}`)
    value = value.replace(URL_PATTERN, REDACTED_URL)
    value = value.replace(ABSOLUTE_PATH_PATTERN, (_match, prefix) => `${prefix}${REDACTED_PATH}`)
    return value
}

function redactRuntimeLogLinesForApi(lines: Array<unknown>) {
    return lines.map((line) => redactRuntimeLogLineForApi(line))
}

function publicRuntimeLogFileMetadata(filePath?: string | null) {
    const normalized = String(filePath || '').trim()
    return {
        file: normalized ? path.basename(normalized) : null,
        file_path: normalized ? REDACTED : null,
        redacted: true,
    }
}

export { publicRuntimeLogFileMetadata, redactRuntimeLogLineForApi, redactRuntimeLogLinesForApi }
