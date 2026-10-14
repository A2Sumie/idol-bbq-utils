import { expect, test } from 'bun:test'
import {
    classifyOneBotResponse,
    classifyOneBotTransportError,
    detectBridgeFlavor,
} from './onebot-semantics'

test('SnowLuma success envelope is ok and carries the message_id', () => {
    const verdict = classifyOneBotResponse(
        {
            statusText: 'OK',
            data: { status: 'ok', retcode: 0, data: { message_id: 123456 }, wording: '' },
        },
        'send_group_msg',
    )
    expect(verdict).toEqual({ ok: true, messageId: 123456 })
})

test('SnowLuma business failure retcode=100 classifies by wording', () => {
    const riskControl = classifyOneBotResponse(
        {
            data: {
                status: 'failed',
                retcode: 100,
                wording: 'send group message failed: 触发风控, 消息被拒绝',
            },
        },
        'send_group_msg',
    )
    expect(riskControl.ok).toBe(false)
    if (!riskControl.ok) {
        expect(riskControl.retryable).toBe(false)
        expect(riskControl.category).toBe('business')
        expect(riskControl.retcode).toBe(100)
    }

    const transient = classifyOneBotResponse(
        {
            data: {
                status: 'failed',
                retcode: 100,
                wording: 'send group message failed: upstream timeout, try again',
            },
        },
        'send_group_msg',
    )
    expect(transient.ok).toBe(false)
    if (!transient.ok) {
        expect(transient.retryable).toBe(true)
    }
})

test('SnowLuma parameter / auth / unknown-action / oversized codes are never retried', () => {
    const cases: Array<[number, string]> = [
        [1400, 'bad_request'],
        [1401, 'unauthorized'],
        [1404, 'unknown_action'],
        [1413, 'payload_too_large'],
    ]
    for (const [retcode, category] of cases) {
        const verdict = classifyOneBotResponse(
            { data: { status: 'failed', retcode, wording: 'whatever' } },
            'send_group_msg',
        )
        expect(verdict.ok).toBe(false)
        if (!verdict.ok) {
            expect(verdict.retryable).toBe(false)
            expect(verdict.category).toBe(category)
        }
    }
})

test('SnowLuma internal error retcode=1200 stays retryable', () => {
    const verdict = classifyOneBotResponse(
        { data: { status: 'failed', retcode: 1200, wording: 'internal error' } },
        'send_group_msg',
    )
    expect(verdict.ok).toBe(false)
    if (!verdict.ok) {
        expect(verdict.retryable).toBe(true)
        expect(verdict.category).toBe('internal')
    }
})

test('NapCat EventChecker rejection retcode=200 stays non-retryable (regression pin)', () => {
    const verdict = classifyOneBotResponse(
        {
            statusText: 'OK',
            data: { status: 'failed', retcode: 200, message: 'EventChecker Failed', data: { result: 120 } },
        },
        'send_group_msg',
    )
    expect(verdict.ok).toBe(false)
    if (!verdict.ok) {
        expect(verdict.retryable).toBe(false)
        expect(verdict.category).toBe('napcat_rejected')
        // The historical error wording must survive for log/diagnostic compat.
        expect(verdict.detail).toMatch(/status=failed retcode=200 message=EventChecker Failed/)
    }
})

test('NapCat retcode=200 is not treated as success even when status reads ok', () => {
    const verdict = classifyOneBotResponse(
        { data: { status: 'ok', retcode: 200, data: { result: 120 } } },
        'send_group_msg',
    )
    expect(verdict.ok).toBe(false)
    if (!verdict.ok) {
        expect(verdict.retryable).toBe(false)
    }
})

test('status=failed without retcode is a failure and keeps the detail shape', () => {
    const verdict = classifyOneBotResponse({ statusText: 'Bad', data: { status: 'failed' } }, 'send_group_msg')
    expect(verdict.ok).toBe(false)
    if (!verdict.ok) {
        expect(verdict.detail).toMatch(/QQ OneBot send failed \(send_group_msg\): status=failed retcode=unknown message=Bad/)
    }
})

test('transport errors: timeouts and 5xx retry, 4xx does not', () => {
    const timeout = classifyOneBotTransportError(
        Object.assign(new Error('timeout of 15000ms exceeded'), {
            code: 'ECONNABORTED',
            isAxiosError: true,
        }),
    )
    expect(timeout.retryable).toBe(true)

    const serverError = classifyOneBotTransportError(
        Object.assign(new Error('Request failed with status code 502'), {
            isAxiosError: true,
            response: { status: 502 },
        }),
    )
    expect(serverError.retryable).toBe(true)

    const badRequest = classifyOneBotTransportError(
        Object.assign(new Error('Request failed with status code 400'), {
            isAxiosError: true,
            response: { status: 400 },
        }),
    )
    expect(badRequest.retryable).toBe(false)

    const tooMany = classifyOneBotTransportError(
        Object.assign(new Error('Request failed with status code 429'), {
            isAxiosError: true,
            response: { status: 429 },
        }),
    )
    expect(tooMany.retryable).toBe(true)
})

test('transport errors: only ambiguous failures are flagged possiblyDelivered (2026-10-15 dup-send pin)', () => {
    // Client timeout: the bridge may already be delivering — never blind retry.
    const timeout = classifyOneBotTransportError(
        Object.assign(new Error('timeout of 60000ms exceeded'), {
            code: 'ECONNABORTED',
            isAxiosError: true,
        }),
    )
    expect(timeout.possiblyDelivered).toBe(true)

    // Mid-flight reset: request bytes may have been processed.
    const reset = classifyOneBotTransportError(
        Object.assign(new Error('socket hang up'), {
            code: 'ECONNRESET',
            isAxiosError: true,
        }),
    )
    expect(reset.possiblyDelivered).toBe(true)

    // 5xx: the bridge answered with an explicit "the action ran and failed"
    // verdict (SnowLuma error seam wraps retcode 1200) — not delivered.
    const serverError = classifyOneBotTransportError(
        Object.assign(new Error('Request failed with status code 502'), {
            isAxiosError: true,
            response: { status: 502 },
        }),
    )
    expect(serverError.possiblyDelivered).toBe(false)

    // Connection never established: safe to retry blindly.
    const refused = classifyOneBotTransportError(
        Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:3200'), {
            code: 'ECONNREFUSED',
            isAxiosError: true,
        }),
    )
    expect(refused.possiblyDelivered).toBe(false)

    const dns = classifyOneBotTransportError(
        Object.assign(new Error('getaddrinfo ENOTFOUND bridge'), {
            code: 'ENOTFOUND',
            isAxiosError: true,
        }),
    )
    expect(dns.possiblyDelivered).toBe(false)

    // Explicit "retry later" answers: nothing was processed.
    const tooMany = classifyOneBotTransportError(
        Object.assign(new Error('Request failed with status code 429'), {
            isAxiosError: true,
            response: { status: 429 },
        }),
    )
    expect(tooMany.possiblyDelivered).toBe(false)
})

test('bridge flavor detection reads get_version_info app_name', () => {
    expect(detectBridgeFlavor({ app_name: 'SnowLuma', app_version: '1.14.22-node' })).toBe('snowluma')
    expect(detectBridgeFlavor({ data: { app_name: 'SnowLuma' } })).toBe('snowluma')
    expect(detectBridgeFlavor({ app_name: 'NapCat.Shell' })).toBe('napcat')
    expect(detectBridgeFlavor({})).toBe('unknown')
})
