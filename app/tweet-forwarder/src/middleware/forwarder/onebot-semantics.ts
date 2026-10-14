/**
 * OneBot response / failure semantics for the QQ send path.
 *
 * The 3020e bridge is being switched from legacy NapCat to SnowLuma and both
 * must stay readable during the migration window, so every failure is judged by
 * a single pure table that understands both dialects:
 *
 * - SnowLuma (`REPORT_snowluma_capabilities_2026-10-14.md` §3.3, source
 *   `packages/onebot/src/types.ts` + `api-handler.ts` "single error seam"):
 *   success = `status:"ok" && retcode:0`; business failures (QQ/OIDB/bridge
 *   errors, including send rejections) surface as `status:"failed",
 *   retcode:100` + `wording`; parameter / message-segment validation errors are
 *   `retcode:1400`; unknown action `1404`; unauthorized `1401` (HTTP 401);
 *   oversized body `1413` (HTTP 413); internal `1200` (HTTP 500). SnowLuma
 *   explicitly refuses the NapCat "retcode 200 + result 120" success-shaped
 *   rejection wire ("Reject that ambiguous wire shape instead of reporting a
 *   successful send").
 *
 * - Legacy NapCat: the EventChecker rejects sends (bot muted / risk control /
 *   ...) BEFORE the message is accepted and reports them as `retcode:200` with
 *   `result:120`-ish payloads — a success-looking envelope that must NOT be
 *   treated as retryable traffic, and must NOT be treated as success either.
 *
 * Retry policy (kept compatible with the historical NapCat behavior where only
 * the `retcode:200` rejection was non-retryable):
 *
 * - non-retryable: NapCat `retcode:200` rejections, SnowLuma `1400`/`1401`/
 *   `1404`/`1413`, and failures whose wording matches a rejection signature
 *   (risk control / muted / banned / permission / throttled). Those are
 *   "拒发/风控/参数" — re-posting the identical payload immediately only spams
 *   the bridge during an outage.
 * - retryable: transport-level failures (timeout / connection reset / 5xx),
 *   SnowLuma `1200` internal errors, and business failures whose wording looks
 *   transient (timeout / busy / try again). The bounded pRetry (RETRY_LIMIT=2)
 *   plus the outbound attempt budget keep this from becoming a retry storm.
 *
 * Everything here is a pure function so the whole table is unit-testable
 * without a bridge.
 */

export type OneBotBridgeFlavor = 'snowluma' | 'napcat' | 'unknown'

export type OneBotFailureCategory =
    /** NapCat EventChecker rejection (retcode 200 + result 120-style). */
    | 'napcat_rejected'
    /** SnowLuma ACTION_FAILED (retcode 100) / generic business failure. */
    | 'business'
    /** SnowLuma BAD_REQUEST (retcode 1400): parameter / segment validation. */
    | 'bad_request'
    /** SnowLuma UNAUTHORIZED (retcode 1401, HTTP 401): token invalid. */
    | 'unauthorized'
    /** SnowLuma NOT_FOUND (retcode 1404): unknown action. */
    | 'unknown_action'
    /** SnowLuma PAYLOAD_TOO_LARGE (retcode 1413, HTTP 413). */
    | 'payload_too_large'
    /** SnowLuma INTERNAL (retcode 1200, HTTP 500). */
    | 'internal'
    /** HTTP/socket layer (axios error), judged by code/status. */
    | 'transport'
    /** Unrecognized failure shape. */
    | 'unknown'

export type OneBotVerdict =
    | {
          ok: true
          messageId?: string | number | null
      }
    | {
          ok: false
          retryable: boolean
          category: OneBotFailureCategory
          retcode: number | null
          status: string
          wording: string
          detail: string
      }

/**
 * Wording signatures that mark a failure as a rejection (拒发/风控/参数/权限/
 * 限流) even when the retcode alone cannot say so — e.g. SnowLuma retcode 100
 * wrapping a QQ server-side risk-control message.
 */
const REJECTION_WORDING_PATTERNS: RegExp[] = [
    /风控|风险|禁言|被踢|踢出|封禁|黑名单|冻结|异常账号|安全/i,
    /muted|banned|blocked|kick|risk|frozen|forbidden|permission|not allowed|denied/i,
    /限流|频繁|太快|rate.?limit|too many|frequen|throttl/i,
    /参数|校验|不支持|无效|过期|不存在|invalid|malformed|unsupported/i,
]

/** Wording signatures that mark a failure as transient (网络/超时/繁忙). */
const TRANSIENT_WORDING_PATTERNS: RegExp[] = [
    /timeout|timed\s*out|etimedout|econnreset|econnrefused|epipe|socket hang up/i,
    /temporarily|try again|retry later|unavailable|busy|overloaded/i,
    /超时|网络|连接|繁忙|稍后|重试|暂时|不可用/i,
    /\b50[234]\b|bad gateway|gateway timeout/i,
]

function normalizeStatus(data: any): string {
    return String(data?.status || '')
        .trim()
        .toLowerCase()
}

function extractMessageId(data: any): string | number | null | undefined {
    const inner = data?.data
    const candidate = inner && typeof inner === 'object' ? inner.message_id : undefined
    const fallback = data?.message_id
    const value = candidate === undefined || candidate === null ? fallback : candidate
    return value === undefined || value === null || value === '' ? null : (value as string | number)
}

function matchAny(patterns: RegExp[], text: string): boolean {
    return patterns.some((pattern) => pattern.test(text))
}

function buildDetail(parts: {
    context: string
    status: string
    retcodeRaw: unknown
    hasRetcode: boolean
    message: string
}): string {
    return `QQ OneBot send failed (${parts.context}): status=${parts.status || 'unknown'} retcode=${
        parts.hasRetcode ? parts.retcodeRaw : 'unknown'
    } message=${parts.message}`
}

/**
 * Classify a completed OneBot HTTP response (HTTP 2xx with a JSON body).
 * `context` is only used for the human-readable detail line.
 */
export function classifyOneBotResponse(
    res: { data?: any; statusText?: string },
    context: string,
): OneBotVerdict {
    const data = res?.data
    const status = normalizeStatus(data)
    const retcodeRaw = data?.retcode
    const retcodeText = retcodeRaw === undefined || retcodeRaw === null ? '' : String(retcodeRaw).trim()
    const hasRetcode = retcodeText.length > 0
    const retcode = Number(retcodeRaw)
    const retcodeFailed = hasRetcode && (!Number.isFinite(retcode) || retcode !== 0)
    const statusFailed = Boolean(status) && status !== 'ok'

    if (!statusFailed && !retcodeFailed) {
        return { ok: true, messageId: extractMessageId(data) }
    }

    const message = String(data?.message || data?.wording || data?.msg || data?.error || res?.statusText || 'unknown')
    const wording = String(data?.wording || data?.message || data?.msg || '')
    const detail = buildDetail({ context, status, retcodeRaw, hasRetcode, message })
    const normalizedRetcode = hasRetcode && Number.isFinite(retcode) ? retcode : null

    // NapCat EventChecker rejection: success-shaped envelope (retcode 200) with
    // a rejection payload. Never success, never worth an immediate retry.
    if (normalizedRetcode === 200) {
        return {
            ok: false,
            retryable: false,
            category: 'napcat_rejected',
            retcode: normalizedRetcode,
            status,
            wording: message,
            detail,
        }
    }

    // SnowLuma fixed code table (packages/onebot/src/types.ts RETCODE +
    // api-handler.ts error seam). 1400/1401/1404/1413 are caller/config
    // problems: re-sending the identical request cannot succeed.
    const FIXED_NON_RETRYABLE: Record<number, OneBotFailureCategory> = {
        1400: 'bad_request',
        1401: 'unauthorized',
        1404: 'unknown_action',
        1413: 'payload_too_large',
    }
    if (normalizedRetcode !== null && FIXED_NON_RETRYABLE[normalizedRetcode]) {
        return {
            ok: false,
            retryable: false,
            category: FIXED_NON_RETRYABLE[normalizedRetcode],
            retcode: normalizedRetcode,
            status,
            wording: message,
            detail,
        }
    }

    // SnowLuma INTERNAL: bridge-side error, plausibly transient.
    if (normalizedRetcode === 1200) {
        return {
            ok: false,
            retryable: true,
            category: 'internal',
            retcode: normalizedRetcode,
            status,
            wording: message,
            detail,
        }
    }

    // Business failure (SnowLuma retcode 100 ACTION_FAILED, or any other
    // non-zero code both bridges use for "the action ran and failed"). Judge by
    // wording: explicit rejection signatures win, transient signatures retry,
    // anything else keeps the historical bounded-retry behavior.
    const category: OneBotFailureCategory = normalizedRetcode === 100 ? 'business' : 'unknown'
    if (matchAny(REJECTION_WORDING_PATTERNS, `${message} ${wording}`)) {
        return { ok: false, retryable: false, category, retcode: normalizedRetcode, status, wording: message, detail }
    }
    if (matchAny(TRANSIENT_WORDING_PATTERNS, `${message} ${wording}`)) {
        return { ok: false, retryable: true, category, retcode: normalizedRetcode, status, wording: message, detail }
    }
    return { ok: false, retryable: true, category, retcode: normalizedRetcode, status, wording: message, detail }
}

type TransportErrorLike = {
    message?: unknown
    code?: unknown
    response?: { status?: unknown; data?: any; statusText?: unknown }
    isAxiosError?: unknown
}

/**
 * Socket-level codes where the request provably never reached the bridge:
 * name resolution / routing / connection establishment failed before any
 * request byte could be processed. Re-sending those is safe.
 */
const NEVER_DELIVERED_CODE_PATTERN = /^(ECONNREFUSED|ENOTFOUND|EAI_AGAIN|ENETUNREACH|EHOSTUNREACH|EACCES)$/i

/**
 * Classify an error thrown by the HTTP layer (axios). Timeouts, connection
 * resets and 5xx are retryable; 4xx (bad endpoint / token / payload) are not,
 * with 408/429 kept retryable because they explicitly ask for a retry.
 *
 * `possiblyDelivered` answers the duplicate-send question: could the bridge
 * have received and delivered this message even though the client saw a
 * failure? Timeouts / connection resets mid-flight are ambiguous — SnowLuma
 * acks `send_*_msg` only after the message is actually delivered, so a client
 * timeout routinely accompanies a delivered message (2026-10-15 duplicate-send
 * incident: every observed timed-out send landed in the group). Callers must
 * reconcile against the bridge before re-sending such a payload; only
 * `possiblyDelivered: false` failures may be blindly retried.
 */
export function classifyOneBotTransportError(error: unknown): {
    retryable: boolean
    /** The request may have been delivered even though the client saw a failure. */
    possiblyDelivered: boolean
    category: OneBotFailureCategory
    detail: string
} {
    const candidate = (error || {}) as TransportErrorLike
    const httpStatusRaw = candidate.response?.status
    const httpStatus = Number(httpStatusRaw)
    const message = error instanceof Error ? error.message : String(error)
    const code = String(candidate.code || '')

    if (Number.isFinite(httpStatus) && httpStatus > 0) {
        const retryable = httpStatus >= 500 || httpStatus === 408 || httpStatus === 429
        return {
            retryable,
            // The bridge answered with an explicit verdict: the action ran and
            // failed (SnowLuma "single error seam" — HTTP 500 wraps retcode
            // 1200 internal failures), so nothing was delivered and the
            // bounded retry is safe. Only *response-less* failures below are
            // ambiguous.
            possiblyDelivered: false,
            category: 'transport',
            detail: `QQ OneBot HTTP ${httpStatus}${code ? ` code=${code}` : ''}: ${message}`,
        }
    }

    if (NEVER_DELIVERED_CODE_PATTERN.test(code)) {
        return {
            retryable: true,
            possiblyDelivered: false,
            category: 'transport',
            detail: `QQ OneBot transport error${code ? ` code=${code}` : ''}: ${message}`,
        }
    }

    // Timeout (ECONNABORTED / ETIMEDOUT / axios "timeout of Xms exceeded"),
    // mid-flight resets (ECONNRESET / EPIPE / socket hang up) and anything
    // unrecognized: the request bytes may well have been processed.
    return {
        retryable: true,
        possiblyDelivered: true,
        category: 'transport',
        detail: `QQ OneBot transport error${code ? ` code=${code}` : ''}: ${message}`,
    }
}

/**
 * Best-effort bridge flavor from `get_version_info` data. SnowLuma reports
 * `app_name: "SnowLuma"` (packages/onebot/src/actions/info.ts); anything else
 * is treated as the legacy NapCat family for semantics purposes.
 */
export function detectBridgeFlavor(versionInfo: any): OneBotBridgeFlavor {
    const appName = String(versionInfo?.app_name || versionInfo?.data?.app_name || '')
        .trim()
        .toLowerCase()
    if (!appName) return 'unknown'
    if (appName.includes('snowluma')) return 'snowluma'
    return 'napcat'
}
