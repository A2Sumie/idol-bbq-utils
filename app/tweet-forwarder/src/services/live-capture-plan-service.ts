const LIVE_CAPTURE_PLAN_SCHEMA_VERSION = 1
const LIVE_CAPTURE_PLATFORMS = ['tiktok', 'instagram', 'youtube', 'showroom', 'openrec', 'other'] as const
const LIVE_CAPTURE_SOURCE_KINDS = ['manual', 'social_post', 'webpage', 'llm_extraction', 'other'] as const
const LIVE_CAPTURE_QUALITY_OPTIONS = ['origin_rtmp', 'hd_flv', 'hd_hls'] as const
const LIVE_CAPTURE_PLAN_JSON_SCHEMA = {
    type: 'object',
    additionalProperties: false,
    required: ['schema_version', 'target', 'event'],
    properties: {
        schema_version: { type: 'integer', const: LIVE_CAPTURE_PLAN_SCHEMA_VERSION },
        target: {
            type: 'object',
            additionalProperties: false,
            required: ['platform', 'handle'],
            properties: {
                platform: { type: 'string', enum: LIVE_CAPTURE_PLATFORMS },
                handle: { type: 'string', minLength: 1, maxLength: 200 },
                url: { type: 'string', minLength: 1, maxLength: 2048 },
            },
        },
        event: {
            type: 'object',
            additionalProperties: false,
            required: ['starts_at'],
            properties: {
                starts_at: {
                    oneOf: [
                        { type: 'integer', minimum: 1 },
                        { type: 'string', minLength: 1, description: 'ISO-8601 with UTC offset or Z' },
                    ],
                },
                timezone: { type: 'string', default: 'Asia/Tokyo' },
                title: { type: 'string', maxLength: 500 },
                performer: { type: 'string', maxLength: 300 },
            },
        },
        window: {
            type: 'object',
            additionalProperties: false,
            properties: {
                before_minutes: { type: 'integer', minimum: 0, maximum: 180, default: 10 },
                after_minutes: { type: 'integer', minimum: 1, maximum: 1440, default: 240 },
            },
        },
        capture: {
            type: 'object',
            additionalProperties: false,
            properties: {
                poll_seconds: { type: 'integer', minimum: 5, maximum: 300, default: 15 },
                first_byte_timeout_seconds: { type: 'integer', minimum: 5, maximum: 300, default: 30 },
                quality_order: {
                    type: 'array',
                    minItems: 1,
                    maxItems: 3,
                    uniqueItems: true,
                    items: { type: 'string', enum: LIVE_CAPTURE_QUALITY_OPTIONS },
                    default: LIVE_CAPTURE_QUALITY_OPTIONS,
                },
                upload: { type: 'boolean', const: false, default: false },
            },
        },
        source: {
            type: 'object',
            additionalProperties: false,
            properties: {
                kind: { type: 'string', enum: LIVE_CAPTURE_SOURCE_KINDS, default: 'manual' },
                ref: { type: 'string', maxLength: 500 },
                url: { type: 'string', maxLength: 2048 },
                observed_at: {
                    oneOf: [
                        { type: 'integer', minimum: 1 },
                        { type: 'string', minLength: 1, description: 'ISO-8601 with UTC offset or Z' },
                    ],
                },
            },
        },
        extraction: {
            type: 'object',
            additionalProperties: false,
            properties: {
                confidence: { type: 'number', minimum: 0, maximum: 1 },
                model: { type: 'string', maxLength: 200 },
                uncertainties: {
                    type: 'array',
                    maxItems: 20,
                    items: { type: 'string', minLength: 1, maxLength: 500 },
                    default: [],
                },
            },
        },
        tags: {
            type: 'array',
            maxItems: 20,
            uniqueItems: true,
            items: { type: 'string', minLength: 1, maxLength: 100 },
            default: [],
        },
        notes: { type: 'string', maxLength: 2000 },
    },
} as const

interface LiveCapturePlanPayload {
    schema_version: 1
    target: {
        platform: (typeof LIVE_CAPTURE_PLATFORMS)[number]
        handle: string
        url?: string
    }
    event: {
        starts_at: number
        starts_at_iso: string
        timezone: string
        title?: string
        performer?: string
    }
    window: {
        before_minutes: number
        after_minutes: number
        opens_at: number
        opens_at_iso: string
        closes_at: number
        closes_at_iso: string
    }
    capture: {
        poll_seconds: number
        first_byte_timeout_seconds: number
        quality_order: Array<(typeof LIVE_CAPTURE_QUALITY_OPTIONS)[number]>
        upload: false
    }
    source: {
        kind: (typeof LIVE_CAPTURE_SOURCE_KINDS)[number]
        ref?: string
        url?: string
        observed_at?: number
        observed_at_iso?: string
    }
    extraction?: {
        confidence?: number
        model?: string
        uncertainties: Array<string>
    }
    tags: Array<string>
    notes?: string
}

type ObjectValue = Record<string, unknown>

function objectValue(value: unknown, field: string): ObjectValue {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`)
    }
    return value as ObjectValue
}

function rejectUnknownFields(value: ObjectValue, field: string, allowed: Array<string>) {
    const unknown = Object.keys(value).filter((key) => !allowed.includes(key))
    if (unknown.length > 0) throw new Error(`${field} contains unknown fields: ${unknown.join(', ')}`)
}

function requiredString(value: unknown, field: string, maxLength: number) {
    const normalized = String(value || '').trim()
    if (!normalized) throw new Error(`${field} is required`)
    if (normalized.length > maxLength) throw new Error(`${field} must be at most ${maxLength} characters`)
    return normalized
}

function optionalString(value: unknown, field: string, maxLength: number) {
    if (value === undefined || value === null || value === '') return undefined
    return requiredString(value, field, maxLength)
}

function boundedInteger(value: unknown, field: string, fallback: number, min: number, max: number) {
    if (value === undefined || value === null || value === '') return fallback
    const normalized = Number(value)
    if (!Number.isInteger(normalized) || normalized < min || normalized > max) {
        throw new Error(`${field} must be an integer between ${min} and ${max}`)
    }
    return normalized
}

function enumValue<T extends readonly string[]>(value: unknown, field: string, values: T, fallback?: T[number]) {
    const normalized = String(value ?? fallback ?? '')
        .trim()
        .toLowerCase()
    if (!values.includes(normalized as T[number])) {
        throw new Error(`${field} must be one of: ${values.join(', ')}`)
    }
    return normalized as T[number]
}

function timestampValue(value: unknown, field: string) {
    if (typeof value === 'number' && Number.isFinite(value)) {
        const seconds = value > 10_000_000_000 ? Math.floor(value / 1000) : Math.floor(value)
        if (seconds > 0) return seconds
    }
    if (typeof value === 'string' && value.trim()) {
        const normalized = value.trim()
        if (/^\d+$/.test(normalized)) return timestampValue(Number(normalized), field)
        if (!/(?:Z|[+-]\d{2}:?\d{2})$/i.test(normalized)) {
            throw new Error(`${field} must include a UTC offset or Z`)
        }
        const milliseconds = Date.parse(normalized)
        if (Number.isFinite(milliseconds)) return Math.floor(milliseconds / 1000)
    }
    throw new Error(`${field} must be a Unix timestamp or ISO-8601 timestamp with timezone offset`)
}

function optionalTimestamp(value: unknown, field: string) {
    if (value === undefined || value === null || value === '') return undefined
    return timestampValue(value, field)
}

function timezoneValue(value: unknown) {
    const timezone = optionalString(value, 'event.timezone', 100) || 'Asia/Tokyo'
    try {
        new Intl.DateTimeFormat('en-US', { timeZone: timezone }).format()
    } catch {
        throw new Error('event.timezone must be a valid IANA timezone')
    }
    return timezone
}

function stringArray(value: unknown, field: string, maxItems: number, maxLength: number) {
    if (value === undefined || value === null) return []
    if (!Array.isArray(value)) throw new Error(`${field} must be an array`)
    if (value.length > maxItems) throw new Error(`${field} must contain at most ${maxItems} items`)
    return Array.from(new Set(value.map((item, index) => requiredString(item, `${field}[${index}]`, maxLength))))
}

function normalizeLiveCapturePlanInput(input: unknown): LiveCapturePlanPayload {
    const body = objectValue(input, 'request body')
    if (body.schema_version !== undefined && Number(body.schema_version) !== LIVE_CAPTURE_PLAN_SCHEMA_VERSION) {
        throw new Error(`schema_version must be ${LIVE_CAPTURE_PLAN_SCHEMA_VERSION}`)
    }

    const targetInput = objectValue(body.target, 'target')
    const eventInput = objectValue(body.event, 'event')
    const windowInput = body.window === undefined ? {} : objectValue(body.window, 'window')
    const captureInput = body.capture === undefined ? {} : objectValue(body.capture, 'capture')
    const sourceInput = body.source === undefined ? {} : objectValue(body.source, 'source')
    const extractionInput = body.extraction === undefined ? undefined : objectValue(body.extraction, 'extraction')

    rejectUnknownFields(body, 'request body', ['schema_version', 'target', 'event', 'window', 'capture', 'source', 'extraction', 'tags', 'notes'])
    rejectUnknownFields(targetInput, 'target', ['platform', 'handle', 'url'])
    rejectUnknownFields(eventInput, 'event', ['starts_at', 'timezone', 'title', 'performer'])
    rejectUnknownFields(windowInput, 'window', ['before_minutes', 'after_minutes'])
    rejectUnknownFields(captureInput, 'capture', [
        'poll_seconds',
        'first_byte_timeout_seconds',
        'quality_order',
        'upload',
    ])
    rejectUnknownFields(sourceInput, 'source', ['kind', 'ref', 'url', 'observed_at'])
    if (extractionInput) rejectUnknownFields(extractionInput, 'extraction', ['confidence', 'model', 'uncertainties'])

    const platform = enumValue(targetInput.platform, 'target.platform', LIVE_CAPTURE_PLATFORMS)
    const handle = requiredString(targetInput.handle, 'target.handle', 200).replace(/^@/, '')
    const targetUrl = optionalString(targetInput.url, 'target.url', 2048)
    const startsAt = timestampValue(eventInput.starts_at, 'event.starts_at')
    const timezone = timezoneValue(eventInput.timezone)
    const beforeMinutes = boundedInteger(windowInput.before_minutes, 'window.before_minutes', 10, 0, 180)
    const afterMinutes = boundedInteger(windowInput.after_minutes, 'window.after_minutes', 240, 1, 1440)
    const pollSeconds = boundedInteger(captureInput.poll_seconds, 'capture.poll_seconds', 15, 5, 300)
    const firstByteTimeoutSeconds = boundedInteger(
        captureInput.first_byte_timeout_seconds,
        'capture.first_byte_timeout_seconds',
        30,
        5,
        300,
    )
    if (captureInput.upload !== undefined && captureInput.upload !== false) {
        throw new Error('capture.upload must be false; this interface does not schedule uploads')
    }

    const qualityOrder =
        captureInput.quality_order === undefined
            ? [...LIVE_CAPTURE_QUALITY_OPTIONS]
            : stringArray(captureInput.quality_order, 'capture.quality_order', 3, 32).map((quality) =>
                  enumValue(quality, 'capture.quality_order item', LIVE_CAPTURE_QUALITY_OPTIONS),
              )
    if (qualityOrder.length === 0) throw new Error('capture.quality_order must contain at least one item')

    const sourceKind = enumValue(sourceInput.kind, 'source.kind', LIVE_CAPTURE_SOURCE_KINDS, 'manual')
    const observedAt = optionalTimestamp(sourceInput.observed_at, 'source.observed_at')
    const opensAt = startsAt - beforeMinutes * 60
    const closesAt = startsAt + afterMinutes * 60
    const confidence = extractionInput?.confidence === undefined ? undefined : Number(extractionInput.confidence)
    if (confidence !== undefined && (!Number.isFinite(confidence) || confidence < 0 || confidence > 1)) {
        throw new Error('extraction.confidence must be between 0 and 1')
    }

    const source: LiveCapturePlanPayload['source'] = {
        kind: sourceKind,
        ...(optionalString(sourceInput.ref, 'source.ref', 500) ? { ref: optionalString(sourceInput.ref, 'source.ref', 500) } : {}),
        ...(optionalString(sourceInput.url, 'source.url', 2048)
            ? { url: optionalString(sourceInput.url, 'source.url', 2048) }
            : {}),
        ...(observedAt ? { observed_at: observedAt, observed_at_iso: new Date(observedAt * 1000).toISOString() } : {}),
    }

    const extraction = extractionInput
        ? {
              ...(confidence !== undefined ? { confidence } : {}),
              ...(optionalString(extractionInput.model, 'extraction.model', 200)
                  ? { model: optionalString(extractionInput.model, 'extraction.model', 200) }
                  : {}),
              uncertainties: stringArray(extractionInput.uncertainties, 'extraction.uncertainties', 20, 500),
          }
        : undefined

    return {
        schema_version: LIVE_CAPTURE_PLAN_SCHEMA_VERSION,
        target: {
            platform,
            handle,
            ...(targetUrl ? { url: targetUrl } : {}),
        },
        event: {
            starts_at: startsAt,
            starts_at_iso: new Date(startsAt * 1000).toISOString(),
            timezone,
            ...(optionalString(eventInput.title, 'event.title', 500)
                ? { title: optionalString(eventInput.title, 'event.title', 500) }
                : {}),
            ...(optionalString(eventInput.performer, 'event.performer', 300)
                ? { performer: optionalString(eventInput.performer, 'event.performer', 300) }
                : {}),
        },
        window: {
            before_minutes: beforeMinutes,
            after_minutes: afterMinutes,
            opens_at: opensAt,
            opens_at_iso: new Date(opensAt * 1000).toISOString(),
            closes_at: closesAt,
            closes_at_iso: new Date(closesAt * 1000).toISOString(),
        },
        capture: {
            poll_seconds: pollSeconds,
            first_byte_timeout_seconds: firstByteTimeoutSeconds,
            quality_order: qualityOrder,
            upload: false,
        },
        source,
        ...(extraction ? { extraction } : {}),
        tags: stringArray(body.tags, 'tags', 20, 100),
        ...(optionalString(body.notes, 'notes', 2000) ? { notes: optionalString(body.notes, 'notes', 2000) } : {}),
    }
}

export {
    LIVE_CAPTURE_PLAN_JSON_SCHEMA,
    LIVE_CAPTURE_PLAN_SCHEMA_VERSION,
    LIVE_CAPTURE_PLATFORMS,
    LIVE_CAPTURE_QUALITY_OPTIONS,
    LIVE_CAPTURE_SOURCE_KINDS,
    normalizeLiveCapturePlanInput,
}
export type { LiveCapturePlanPayload }
