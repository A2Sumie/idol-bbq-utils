/**
 * OneBot v11 inbound event ingestion: dialect-tolerant parsing/normalization.
 *
 * The QQ bridge on 3020e pushes OneBot v11 events over HTTP POST
 * (`httpClients` on SnowLuma / NapCat's httpPostEndpoints). Two dialects must
 * stay readable during the migration window:
 *
 * - message payload: `array` (segments `{type, data}`) or `string` (CQ codes)
 *   depending on the adapter's `messageFormat`;
 * - `at` segment id fields: NapCat `data.qq` vs SnowLuma `data.targetUin` /
 *   `data.uid` (see REPORT_snowluma_capabilities_2026-10-14.md §3.1);
 * - notice/request subtype fields differ in coverage (SnowLuma adds
 *   `group_msg_emoji_like`, `essence`, `notify/input_status`, `bot_offline`,
 *   ...), so unknown subtypes must normalize instead of throwing;
 * - `post_type: 'message_sent'` (SnowLuma `reportSelfMessage`) is the bot's
 *   own outbound echo and is always dropped before reactions run.
 *
 * Everything here is pure: no IO, no axios, no timers. The reaction layer and
 * the API entry point consume `NormalizedOneBotEvent` only, so a malformed
 * event degrades to `null` (caller logs + skips) instead of crashing a handler.
 */
import crypto from 'crypto'

export type OneBotEventKind = 'message' | 'notice' | 'request' | 'meta_event' | 'unknown'

export type OneBotMessageChannel = 'group' | 'private' | null

export interface NormalizedOneBotEvent {
    kind: OneBotEventKind
    postType: string
    subType: string
    /** group / private (group temp sessions normalize to private). */
    messageType: OneBotMessageChannel
    groupId: string | null
    userId: string | null
    selfId: string | null
    messageId: string | null
    /** Plain text extracted from the message (text segments / CQ text). */
    text: string
    /** raw_message as reported by the bridge (kept for rules and audit). */
    rawText: string
    atUserIds: Array<string>
    atAll: boolean
    atBot: boolean
    senderName: string | null
    /** owner / admin / member when the bridge reports it. */
    senderRole: string | null
    /** notice/request extras (operator_id / target_id / flag / comment ...). */
    operatorId: string | null
    targetId: string | null
    durationSeconds: number | null
    requestFlag: string | null
    comment: string | null
    /** 'array' | 'string' depending on how `message` arrived. */
    messageFormat: 'array' | 'string' | null
    raw: Record<string, unknown>
    /** Unique key for webhook redelivery dedupe. */
    dedupeKey: string
}

const KNOWN_KINDS: Record<string, OneBotEventKind> = {
    message: 'message',
    message_sent: 'message',
    notice: 'notice',
    request: 'request',
    meta_event: 'meta_event',
}

function asRecord(value: unknown): Record<string, unknown> | null {
    return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null
}

function asString(value: unknown): string {
    if (value === undefined || value === null) return ''
    return String(value).trim()
}

function optionalString(value: unknown): string | null {
    const text = asString(value)
    return text ? text : null
}

function optionalNumber(value: unknown): number | null {
    const num = Number(value)
    return Number.isFinite(num) ? num : null
}

function shortHash(value: unknown): string {
    return crypto.createHash('sha256').update(JSON.stringify(value) ?? '').digest('hex').slice(0, 16)
}

/** CQ code escape reversal (OneBot v11 string messages). */
export function unescapeCqText(value: string): string {
    return value
        .replace(/&#44;/g, ',')
        .replace(/&#91;/g, '[')
        .replace(/&#93;/g, ']')
        .replace(/&amp;/g, '&')
}

interface ParsedCqPiece {
    text: string
    at: string | null
}

/**
 * Minimal CQ string parser: enough to recover plain text and `at` targets.
 * Unknown CQ codes are dropped from the text (their payload is not chat text).
 */
export function parseCqMessage(value: string): Array<ParsedCqPiece> {
    const pieces: Array<ParsedCqPiece> = []
    const re = /\[CQ:([A-Za-z0-9_]+)((?:,[^\]]*)?)\]/g
    let last = 0
    let match: RegExpExecArray | null
    while ((match = re.exec(value)) !== null) {
        const chunk = value.slice(last, match.index)
        if (chunk) pieces.push({ text: unescapeCqText(chunk), at: null })
        last = match.index + match[0].length
        const type = match[1]!.toLowerCase()
        if (type === 'at') {
            const params = String(match[2] || '')
                .split(',')
                .map((part) => part.trim())
                .filter(Boolean)
            const qq = params
                .map((part) => part.split('='))
                .find(([key]) => key === 'qq' || key === 'targetUin' || key === 'user_id')
            pieces.push({ text: '', at: qq?.[1] ? unescapeCqText(qq[1]) : 'all' })
        }
    }
    const tail = value.slice(last)
    if (tail) pieces.push({ text: unescapeCqText(tail), at: null })
    return pieces
}

function segmentText(segment: Record<string, any>): string {
    if (segment.type === 'text') {
        return String(segment.data?.text ?? segment.text ?? '')
    }
    return ''
}

function segmentAt(segment: Record<string, any>): string | null {
    if (segment.type !== 'at') return null
    const data = asRecord(segment.data) || {}
    const target = data.qq ?? data.targetUin ?? data.user_id ?? data.uid
    const text = asString(target) || asString(data.text).replace(/^@/, '')
    return text ? text : null
}

/**
 * Extract chat text and @-targets from `message` / `raw_message`, accepting
 * both message formats (segment array and CQ string) and both dialects.
 */
export function extractOneBotMessageParts(value: unknown): {
    text: string
    atUserIds: Array<string>
    atAll: boolean
    messageFormat: 'array' | 'string' | null
} {
    const atUserIds: Array<string> = []
    let atAll = false
    const pushAt = (id: string | null) => {
        if (!id) return
        if (id.toLowerCase() === 'all') {
            atAll = true
            return
        }
        atUserIds.push(id)
    }

    if (Array.isArray(value)) {
        const text = value
            .map((item) => {
                const segment = asRecord(item)
                if (!segment) return typeof item === 'string' ? item : ''
                pushAt(segmentAt(segment))
                return segmentText(segment)
            })
            .join('')
        return { text, atUserIds: Array.from(new Set(atUserIds)), atAll, messageFormat: 'array' }
    }
    if (typeof value === 'string') {
        const pieces = parseCqMessage(value)
        for (const piece of pieces) pushAt(piece.at)
        return {
            text: pieces
                .map((piece) => piece.text)
                .join('')
                .trim(),
            atUserIds: Array.from(new Set(atUserIds)),
            atAll,
            messageFormat: 'string',
        }
    }
    if (value && typeof value === 'object') {
        const segment = value as Record<string, any>
        pushAt(segmentAt(segment))
        return { text: segmentText(segment), atUserIds: Array.from(new Set(atUserIds)), atAll, messageFormat: 'array' }
    }
    return { text: '', atUserIds: [], atAll: false, messageFormat: null }
}

function resolveMessageChannel(raw: Record<string, unknown>): OneBotMessageChannel {
    const messageType = asString(raw.message_type).toLowerCase()
    if (messageType === 'group') return 'group'
    if (messageType === 'private') return 'private'
    // NapCat/SnowLuma group temp sessions: message_type 'private' + group_id set,
    // or legacy 'discuss'. Both are treated as private replies keyed by user_id.
    if (messageType === 'discuss') return 'private'
    return raw.group_id !== undefined && raw.group_id !== null ? 'group' : null
}

function buildDedupeKey(kind: OneBotEventKind, raw: Record<string, unknown>, selfId: string): string {
    const messageId = optionalString(raw.message_id)
    if (kind === 'message' && messageId) {
        return `msg:${selfId}:${messageId}`
    }
    if (kind === 'notice') {
        const noticeType = asString(raw.notice_type) || 'unknown'
        const subType = asString(raw.sub_type)
        const context = [raw.group_id, raw.user_id, raw.operator_id, raw.target_id]
            .map(asString)
            .filter(Boolean)
            .join(':')
        const primary = messageId || optionalString(raw.flag) || context || shortHash(raw)
        return `notice:${selfId}:${noticeType}:${subType}:${primary}:${context}`
    }
    if (kind === 'request') {
        const requestType = asString(raw.request_type) || 'unknown'
        const flag = optionalString(raw.flag) || [raw.group_id, raw.user_id].map(asString).join(':')
        return `req:${selfId}:${requestType}:${asString(raw.sub_type)}:${flag || shortHash(raw)}`
    }
    return `${kind}:${selfId}:${shortHash(raw)}`
}

/**
 * Normalize one raw webhook body into the internal event shape. Returns null
 * when the body is not an OneBot event at all (or is unusable) — callers log
 * the reason and skip instead of failing the delivery.
 */
export function normalizeOneBotEvent(input: unknown): NormalizedOneBotEvent | null {
    const raw = asRecord(input)
    if (!raw) return null
    const postType = asString(raw.post_type).toLowerCase()
    if (!postType) return null

    const kind = KNOWN_KINDS[postType] || 'unknown'
    const selfId = optionalString(raw.self_id) || ''
    const messageValue = raw.message !== undefined && raw.message !== null ? raw.message : raw.original_message
    const parts = extractOneBotMessageParts(messageValue)
    const rawText = asString(raw.raw_message) || parts.text
    const sender = asRecord(raw.sender) || {}
    const atBot = parts.atAll || (selfId ? parts.atUserIds.includes(selfId) : false)

    return {
        kind,
        postType,
        subType: asString(raw.sub_type),
        messageType: kind === 'message' ? resolveMessageChannel(raw) : null,
        groupId: optionalString(raw.group_id),
        userId: optionalString(raw.user_id) || optionalString(sender.user_id),
        selfId: selfId || null,
        messageId: optionalString(raw.message_id),
        text: parts.text,
        rawText,
        atUserIds: parts.atUserIds,
        atAll: parts.atAll,
        atBot,
        senderName: optionalString(sender.card) || optionalString(sender.nickname) || optionalString(sender.name),
        senderRole: optionalString(sender.role),
        operatorId: optionalString(raw.operator_id),
        targetId: optionalString(raw.target_id),
        durationSeconds: optionalNumber(raw.duration),
        requestFlag: optionalString(raw.flag),
        comment: optionalString(raw.comment),
        messageFormat: parts.messageFormat,
        raw,
        dedupeKey: buildDedupeKey(kind, raw, selfId),
    }
}

/**
 * Self-echo guard: never let the bot react to its own traffic. Covers
 * `post_type: 'message_sent'` (SnowLuma reportSelfMessage), messages authored
 * by a configured bot uin, and notice/request events targeting the bot as the
 * acting party of its own actions (operator_id === self_id on recalls etc. is
 * still useful, so only message echoes are dropped here).
 */
export function isSelfEchoEvent(event: NormalizedOneBotEvent, botUserIds: Array<string> = []): boolean {
    if (event.postType === 'message_sent') return true
    // `self_id` is always the bot's own uin, so the guard compares the AUTHOR
    // (user_id) against the bot: messages the bot sent through some other echo
    // path must never re-trigger reactions.
    if (event.selfId && event.userId && event.userId === event.selfId) return true
    const bots = new Set(botUserIds.map((id) => String(id).trim()).filter(Boolean))
    if (event.userId && bots.has(event.userId)) return true
    return false
}
