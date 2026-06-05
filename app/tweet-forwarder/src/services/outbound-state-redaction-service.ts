import crypto from 'crypto'
import { redactSecrets } from './redaction-service'

type OutboundMessageEntryLike = Record<string, unknown> & {
    idempotency_key?: string | null
    route_key?: string | null
    target_id?: string | null
    article_key?: string | null
    synthetic_key?: string | null
    payload_hash?: string | null
    provider_message_ids?: unknown
    segment_results?: unknown
    last_error?: string | null
}

type TargetHealthEntryLike = Record<string, unknown> & {
    target_id?: string | null
    disabled_reason?: string | null
    details?: unknown
}

const PUBLIC_STATUS_STRINGS = new Set([
    'planned',
    'sending',
    'queued',
    'dry_run',
    'skipped',
    'sent',
    'partial',
    'failed',
    'failed_after_partial',
    'blocked',
    'ok',
    'error',
    'degraded',
])

function valueType(value: unknown) {
    if (value === null) return 'null'
    if (Array.isArray(value)) return 'array'
    return typeof value
}

function hasValue(value: unknown) {
    return value !== null && value !== undefined && value !== ''
}

function stringLength(value: unknown) {
    return typeof value === 'string' ? value.length : undefined
}

function publicNumber(value: unknown) {
    return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

function publicStatus(value: unknown) {
    if (typeof value === 'number' || typeof value === 'boolean') {
        return value
    }
    if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase()
        return PUBLIC_STATUS_STRINGS.has(normalized) ? normalized : null
    }
    return null
}

function publicSendMode(value: unknown) {
    if (typeof value !== 'string') {
        return null
    }
    const normalized = value.trim().toLowerCase()
    return ['live', 'blocked', 'capture'].includes(normalized) ? normalized : null
}

function publicCaptureKind(value: unknown) {
    if (typeof value !== 'string') {
        return null
    }
    const normalized = value.trim().toLowerCase()
    return ['http', 'file'].includes(normalized) ? normalized : null
}

function compactHash(value: unknown) {
    if (!hasValue(value)) {
        return null
    }
    return crypto.createHash('sha256').update(String(value)).digest('hex').slice(0, 16)
}

function redactIdentifier(value: unknown) {
    return hasValue(value) ? '[redacted]' : value
}

function publicTextSummary(value: unknown) {
    if (!hasValue(value)) {
        return value
    }
    return {
        redacted_text: true,
        text_present: true,
        text_type: valueType(value),
        ...(stringLength(value) !== undefined ? { text_length: stringLength(value) } : {}),
    }
}

function parseObject(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        return null
    }
    return value as Record<string, unknown>
}

function keyCount(value: Record<string, unknown>) {
    return Object.keys(value).length
}

function providerCodeFrom(value: unknown) {
    const object = parseObject(value)
    if (!object) {
        return publicStatus(value)
    }
    const data = parseObject(object.data)
    const directCode = object.retcode ?? object.code ?? object.status
    const dataCode = data?.retcode ?? data?.code ?? data?.status
    return publicStatus(dataCode ?? directCode)
}

function publicCaptureResult(value: unknown) {
    const record = parseObject(value)
    if (!record) {
        return {
            redacted_value: true,
            value_type: valueType(value),
        }
    }
    return {
        redacted_value: true,
        value_type: 'object',
        kind: publicCaptureKind(record.kind),
        ok: typeof record.ok === 'boolean' ? record.ok : null,
        status: publicStatus(record.status),
        destination_present: hasValue(record.destination),
        error_present: hasValue(record.error),
    }
}

function publicProviderData(value: unknown) {
    const record = parseObject(value)
    if (!record) {
        return {
            redacted_value: true,
            value_type: valueType(value),
            provider_code: providerCodeFrom(value),
        }
    }
    return {
        redacted_value: true,
        value_type: 'object',
        key_count: keyCount(record),
        provider_code: providerCodeFrom(record),
    }
}

function publicNestedIdentity(value: unknown) {
    const record = parseObject(value)
    if (!record) {
        return null
    }
    return {
        redacted_value: true,
        value_type: 'object',
        route_key_present: hasValue(record.route_key),
        route_key_hash: compactHash(record.route_key),
        target_id_present: hasValue(record.target_id),
        target_id_hash: compactHash(record.target_id),
        article_key_present: hasValue(record.article_key),
        article_key_hash: compactHash(record.article_key),
        synthetic_key_present: hasValue(record.synthetic_key),
        synthetic_key_hash: compactHash(record.synthetic_key),
        payload_hash_present: hasValue(record.payload_hash),
        status: publicStatus(record.status),
        task_kind_present: hasValue(record.task_kind),
        target_platform_present: hasValue(record.target_platform),
    }
}

function publicOperationalJson(value: unknown, depth = 0): unknown {
    if (!hasValue(value)) {
        return value ?? null
    }
    if (Array.isArray(value)) {
        return {
            redacted_value: true,
            value_type: 'array',
            item_count: value.length,
            item_types: Array.from(new Set(value.map((item) => valueType(item)))).sort(),
            ...(depth < 2 ? { items: value.slice(0, 3).map((item) => publicOperationalJson(item, depth + 1)) } : {}),
        }
    }

    const record = parseObject(redactSecrets(value))
    if (!record) {
        return {
            redacted_value: true,
            value_type: valueType(value),
        }
    }

    return {
        redacted_value: true,
        value_type: 'object',
        key_count: keyCount(record),
        send_mode: publicSendMode(record.send_mode),
        status: publicStatus(record.status),
        provider_code: providerCodeFrom(record),
        diagnostic_present: hasValue(record.diagnostic),
        reason_present: hasValue(record.reason),
        text_count: publicNumber(record.text_count),
        text_length: publicNumber(record.text_length),
        media_count: publicNumber(record.media_count),
        card_media_count: publicNumber(record.card_media_count),
        content_media_count: publicNumber(record.content_media_count),
        pending_units: publicNumber(record.pendingUnits),
        threshold: publicNumber(record.threshold),
        target_id_present: hasValue(record.target_id),
        target_id_hash: compactHash(record.target_id),
        article_key_present: hasValue(record.article_key),
        article_key_hash: compactHash(record.article_key),
        outbound_key_present: hasValue(record.outbound_key),
        outbound_key_hash: compactHash(record.outbound_key),
        batch_key_present: hasValue(record.batchKey),
        capture_result: hasValue(record.capture_result) ? publicCaptureResult(record.capture_result) : null,
        data: hasValue(record.data) ? publicProviderData(record.data) : null,
        details: depth < 2 && hasValue(record.details) ? publicOperationalJson(record.details, depth + 1) : null,
        existing: hasValue(record.existing) ? publicNestedIdentity(record.existing) : null,
        incoming: hasValue(record.incoming) ? publicNestedIdentity(record.incoming) : null,
        previous_segment_results:
            depth < 2 && hasValue(record.previous_segment_results)
                ? publicOperationalJson(record.previous_segment_results, depth + 1)
                : null,
    }
}

function redactOutboundMessageForApi<T extends OutboundMessageEntryLike>(message: T): T {
    const redacted = redactSecrets(message) as T
    return {
        ...redacted,
        idempotency_key: redactIdentifier(message.idempotency_key),
        idempotency_key_hash: compactHash(message.idempotency_key),
        route_key: redactIdentifier(message.route_key),
        route_key_hash: compactHash(message.route_key),
        target_id: redactIdentifier(message.target_id),
        target_id_hash: compactHash(message.target_id),
        article_key: redactIdentifier(message.article_key),
        article_key_hash: compactHash(message.article_key),
        synthetic_key: redactIdentifier(message.synthetic_key),
        synthetic_key_hash: compactHash(message.synthetic_key),
        payload_hash: redactIdentifier(message.payload_hash),
        payload_hash_present: hasValue(message.payload_hash),
        provider_message_ids: publicOperationalJson(message.provider_message_ids),
        segment_results: publicOperationalJson(message.segment_results),
        last_error: publicTextSummary(message.last_error),
    }
}

function redactOutboundMessagesForApi<T extends OutboundMessageEntryLike>(messages: Array<T>): Array<T> {
    return messages.map((message) => redactOutboundMessageForApi(message))
}

function redactTargetHealthForApi<T extends TargetHealthEntryLike>(entry: T): T {
    const redacted = redactSecrets(entry) as T
    return {
        ...redacted,
        target_id: redactIdentifier(entry.target_id),
        target_id_hash: compactHash(entry.target_id),
        disabled_reason: publicTextSummary(entry.disabled_reason),
        details: publicOperationalJson(entry.details),
    }
}

function redactTargetHealthEntriesForApi<T extends TargetHealthEntryLike>(entries: Array<T>): Array<T> {
    return entries.map((entry) => redactTargetHealthForApi(entry))
}

export {
    redactOutboundMessageForApi,
    redactOutboundMessagesForApi,
    redactTargetHealthEntriesForApi,
    redactTargetHealthForApi,
}
