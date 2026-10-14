/**
 * Explicit QQ/OneBot receipt + recall + bridge health helpers (SnowLuma
 * message-store integration).
 *
 * SnowLuma persists every message in a local SQLite message-store, so a send
 * can be verified after the fact (`get_msg` returns the durable copy with
 * `real_id` and refreshed image URLs) and undone (`delete_msg`, which works for
 * both sent and received messages). Both operations are deliberately exposed
 * only through explicit operator calls (POST /api/actions/qq/recall and
 * /api/actions/qq/receipt-check) — nothing in the send pipeline ever recalls
 * automatically.
 *
 * `probeQqBridgeHealth` maps onto the existing target-health bookkeeping:
 * SnowLuma's `get_status` reports `online` (logged in) and `good` (send/receive
 * link healthy), plus `get_version_info.app_name` which is how we tell
 * SnowLuma from legacy NapCat during the migration window.
 */
import axios from 'axios'
import type { Logger } from '@idol-bbq-utils/log'
import {
    classifyOneBotResponse,
    classifyOneBotTransportError,
    detectBridgeFlavor,
    type OneBotBridgeFlavor,
    type OneBotVerdict,
} from '@/middleware/forwarder/onebot-semantics'

const DEFAULT_REQUEST_TIMEOUT_MS = 15_000

export interface OneBotEndpointConfig {
    endpoint: string
    token: string
    timeoutMs?: number
}

type PostLike = (path: string, body: unknown, options: unknown) => Promise<any>

function buildHeaders(token: string): Record<string, string> {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' }
    if (token?.trim()) {
        headers.Authorization = `Bearer ${token}`
    }
    return headers
}

async function callOneBotAction(
    config: OneBotEndpointConfig,
    action: string,
    payload: Record<string, unknown>,
    post: PostLike = (path, body, options) => axios.post(path, body, options as any),
): Promise<{ verdict: OneBotVerdict; body: any }> {
    const timeout = config.timeoutMs && config.timeoutMs > 0 ? config.timeoutMs : DEFAULT_REQUEST_TIMEOUT_MS
    let res: any
    try {
        res = await post(`${config.endpoint.replace(/\/+$/, '')}/${action}`, payload, {
            headers: buildHeaders(config.token),
            timeout,
        })
    } catch (error) {
        const transport = classifyOneBotTransportError(error)
        return {
            verdict: {
                ok: false,
                retryable: transport.retryable,
                category: transport.category,
                retcode: null,
                status: '',
                wording: transport.detail,
                detail: transport.detail,
            },
            body: null,
        }
    }
    return { verdict: classifyOneBotResponse(res, action), body: res?.data ?? null }
}

export interface OneBotReceipt {
    ok: boolean
    messageId: string
    /** SnowLuma message-store real_id (server-side id) when available. */
    realId: string | number | null
    detail: string
    raw: unknown
}

/**
 * Receipt check: ask the bridge (SnowLuma message-store / NapCat cache) for the
 * persisted copy of a previously sent message id.
 */
export async function fetchOneBotReceipt(
    config: OneBotEndpointConfig,
    messageId: string,
    post?: PostLike,
): Promise<OneBotReceipt> {
    const id = String(messageId || '').trim()
    const { verdict, body } = await callOneBotAction(config, 'get_msg', { message_id: id }, post)
    if (!verdict.ok) {
        return { ok: false, messageId: id, realId: null, detail: verdict.detail, raw: body }
    }
    const data = body?.data
    const realId = data?.real_id ?? data?.message?.real_id ?? null
    return {
        ok: true,
        messageId: id,
        realId,
        detail: 'receipt found',
        raw: data ?? null,
    }
}

export interface OneBotRecallResult {
    success: boolean
    messageId: string
    detail: string
}

/**
 * Explicit recall (`delete_msg`). Never invoked automatically — operators call
 * POST /api/actions/qq/recall with the stored message id.
 */
export async function recallOneBotMessage(
    config: OneBotEndpointConfig,
    messageId: string,
    post?: PostLike,
): Promise<OneBotRecallResult> {
    const id = String(messageId || '').trim()
    const { verdict, body } = await callOneBotAction(config, 'delete_msg', { message_id: id }, post)
    return verdict.ok
        ? { success: true, messageId: id, detail: 'recalled' }
        : { success: false, messageId: id, detail: verdict.detail || String(body || '') }
}

export interface QqBridgeHealth {
    online: boolean
    good: boolean
    flavor: OneBotBridgeFlavor
    /** SnowLuma `get_status.time` (instance uptime seconds) when reported. */
    uptimeSeconds: number | null
    detail: string
    raw: { status?: unknown; version?: unknown }
}

/**
 * Login/health probe: `get_status` (online + good) plus `get_version_info` for
 * bridge flavor detection. Results feed the existing target-health service so
 * a dead bridge is visible without a failed send.
 */
export async function probeQqBridgeHealth(
    config: OneBotEndpointConfig,
    post?: PostLike,
): Promise<QqBridgeHealth> {
    const statusCall = await callOneBotAction(config, 'get_status', {}, post)
    const statusData = statusCall.body?.data
    const online = Boolean(statusData?.online)
    const good = Boolean(statusData?.good)
    const uptimeRaw = Number(statusData?.time)
    const uptimeSeconds = Number.isFinite(uptimeRaw) ? uptimeRaw : null

    const versionCall = await callOneBotAction(config, 'get_version_info', {}, post)
    const versionData = versionCall.body?.data
    const flavor = detectBridgeFlavor(versionData)

    const detail = statusCall.verdict.ok
        ? `online=${online} good=${good} flavor=${flavor}`
        : statusCall.verdict.detail
    return {
        online,
        good,
        flavor,
        uptimeSeconds,
        detail,
        raw: { status: statusData ?? null, version: versionData ?? null },
    }
}

export { callOneBotAction }
