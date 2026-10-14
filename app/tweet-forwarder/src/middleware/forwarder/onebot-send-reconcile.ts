/**
 * Pure helpers for reconciling an ambiguous QQ/OneBot send against the bridge
 * before any re-send.
 *
 * The 2026-10-15 duplicate-send incident: SnowLuma acks `send_group_msg` only
 * after the message is delivered, so under media load the client-side timeout
 * (15s) fired while the bridge was still delivering. The send layer treated
 * the timeout as retryable and re-posted the identical payload, so the group
 * received the message twice. A re-send of an ambiguously failed payload is
 * only safe once the bridge's own history shows the message did NOT land.
 *
 * Matching is done on the text signature of the sent payload: the bridge's
 * message store keeps the outgoing segments verbatim (text survives exactly),
 * while image/video URLs are re-signed server-side and cannot be matched.
 */

type OneBotTextSegmentLike = {
    type?: unknown
    data?: { text?: unknown } & Record<string, unknown>
}

type OneBotHistoryMessageLike = {
    message_id?: unknown
    id?: unknown
    time?: unknown
    timestamp?: unknown
    user_id?: unknown
    sender?: { user_id?: unknown } | null
    message?: unknown
}

function normalizeText(text: string): string {
    return text.replace(/\r\n/g, '\n').trim()
}

/**
 * Text signature of a prepared send: every text segment joined by a newline.
 * Empty string when the payload carries no text (e.g. pure media) — those
 * payloads cannot be reconciled and must never be blindly re-sent.
 */
export function buildSendTextSignature(segments: unknown): string {
    if (!Array.isArray(segments)) {
        return ''
    }
    const parts: string[] = []
    for (const raw of segments) {
        const segment = (raw || {}) as OneBotTextSegmentLike
        if (String(segment.type || '') !== 'text') {
            continue
        }
        const text = segment.data?.text
        if (typeof text === 'string' && text.length > 0) {
            parts.push(text.replace(/\r\n/g, '\n').trim())
        }
    }
    return normalizeText(parts.join('\n'))
}

function collectHistoryText(message: unknown): string {
    if (typeof message === 'string') {
        return message
    }
    if (!Array.isArray(message)) {
        return ''
    }
    return buildSendTextSignature(message)
}

export interface ReconcileMatchOptions {
    /** Exact text signature produced by {@link buildSendTextSignature}. */
    signature: string
    /** Only match messages at/after this unix epoch (seconds). */
    sinceEpochSec: number
    /** Bot uin when known; messages from other senders are ignored. */
    selfId?: string | number | null
    /** Message ids already adopted by concurrent reconciles. */
    takenMessageIds?: Set<string>
}

/**
 * Find the bridge-side copy of an ambiguously failed send inside a recent
 * history listing (OneBot `get_group_msg_history` / `get_friend_msg_history`
 * message objects). Returns the delivered `message_id` or null.
 */
export function findDeliveredMessageId(history: unknown, options: ReconcileMatchOptions): string | null {
    if (!Array.isArray(history)) {
        return null
    }
    const signature = normalizeText(options.signature || '')
    if (!signature) {
        return null
    }
    const taken = options.takenMessageIds || new Set<string>()
    const selfId = options.selfId === undefined || options.selfId === null ? null : String(options.selfId)
    for (const raw of history) {
        const entry = (raw || {}) as OneBotHistoryMessageLike
        const time = Number(entry.time ?? entry.timestamp ?? 0)
        if (Number.isFinite(time) && time > 0 && options.sinceEpochSec > 0 && time < options.sinceEpochSec) {
            continue
        }
        const senderId = entry.sender?.user_id ?? entry.user_id
        if (selfId !== null && senderId !== undefined && senderId !== null && String(senderId) !== selfId) {
            continue
        }
        const messageId = entry.message_id ?? entry.id
        if (messageId === undefined || messageId === null || String(messageId).length === 0) {
            continue
        }
        const id = String(messageId)
        if (taken.has(id)) {
            continue
        }
        if (normalizeText(collectHistoryText(entry.message)) !== signature) {
            continue
        }
        return id
    }
    return null
}
