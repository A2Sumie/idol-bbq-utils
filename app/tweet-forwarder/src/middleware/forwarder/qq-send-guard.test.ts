import { expect, test } from 'bun:test'
import { NonRetryableForwarderSendError } from './base'
import { QQForwarder } from './qq'

/**
 * 2026-10-15 duplicate-send incident regression pins.
 *
 * SnowLuma acks `send_group_msg` only after the message is delivered, so under
 * media load the client timeout fired while the message was actually landing
 * in the group. The old code blindly retried the timeout and the group got the
 * message twice. These tests pin the new contract: an ambiguously failed send
 * is reconciled against the bridge history before any re-send —
 *   - delivered copy found  -> exactly one message, adopted message_id;
 *   - proven absent         -> the bounded retry may re-send;
 *   - reconcile unreachable -> terminal failure, never a blind re-send.
 */

function timeoutError() {
    return Object.assign(new Error('timeout of 60000ms exceeded'), { code: 'ECONNABORTED', isAxiosError: true })
}

function refusedError() {
    return Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:3200'), { code: 'ECONNREFUSED', isAxiosError: true })
}

function okEnvelope(messageId: string | number) {
    return {
        status: 200,
        statusText: 'OK',
        data: { status: 'ok', retcode: 0, wording: '', data: { message_id: messageId } },
    }
}

function loginEnvelope(userId: number) {
    return {
        status: 200,
        statusText: 'OK',
        data: { status: 'ok', retcode: 0, wording: '', data: { user_id: userId } },
    }
}

function historyEnvelope(messages: unknown[]) {
    return {
        status: 200,
        statusText: 'OK',
        data: { status: 'ok', retcode: 0, wording: '', data: { messages } },
    }
}

function textSegment(text: string) {
    return { type: 'text', data: { text } }
}

type StubHandler = (action: string, body: any) => Promise<any>

class StubQQForwarder extends QQForwarder {
    readonly calls: Array<{ action: string; body: any }> = []
    private readonly handler: StubHandler

    constructor(handler: StubHandler) {
        super({ group_id: '742435777', url: 'http://127.0.0.1:3200', token: '' } as any, 'test-qq-target')
        this.handler = handler
    }

    protected override async postOneBot(action: string, body: unknown): Promise<any> {
        this.calls.push({ action, body })
        return await this.handler(action, body)
    }

    protected override async ambiguousReconcileSleep(_ms: number): Promise<void> {
        // keep the reconcile schedule instant in tests
    }

    callsFor(action: string) {
        return this.calls.filter((call) => call.action === action)
    }
}

test('incident pin: ambiguous timeout + delivered bridge copy sends exactly one message and adopts its id', async () => {
    const deliveredText = '🎉 Ratatan（ラタタン）本日正式リリース！'
    let sendCalls = 0
    const forwarder = new StubQQForwarder(async (action) => {
        if (action === 'send_group_msg') {
            sendCalls += 1
            throw timeoutError()
        }
        if (action === 'get_login_info') {
            return loginEnvelope(3219117363)
        }
        if (action === 'get_group_msg_history') {
            return historyEnvelope([
                {
                    message_id: -1106780317,
                    time: Math.floor(Date.now() / 1000),
                    sender: { user_id: 3219117363 },
                    message: [textSegment(deliveredText), { type: 'image', data: { url: 'https://multimedia/x' } }],
                },
            ])
        }
        throw new Error(`unexpected action ${action}`)
    })

    const res = await forwarder.sendWithPayload([textSegment(deliveredText)])
    expect(sendCalls).toBe(1)
    expect(res.data.data.message_id).toBe('-1106780317')
    expect(forwarder.callsFor('get_group_msg_history').length).toBeGreaterThan(0)
})

test('ambiguous timeout with a proven-absent bridge copy rethrows so the bounded retry may re-send once', async () => {
    let sendCalls = 0
    const forwarder = new StubQQForwarder(async (action) => {
        if (action === 'send_group_msg') {
            sendCalls += 1
            throw timeoutError()
        }
        if (action === 'get_login_info') {
            return loginEnvelope(3219117363)
        }
        if (action === 'get_group_msg_history') {
            return historyEnvelope([])
        }
        throw new Error(`unexpected action ${action}`)
    })

    await expect(forwarder.sendWithPayload([textSegment('never landed')])).rejects.toThrow('timeout of 60000ms exceeded')
    expect(sendCalls).toBe(1)
    expect(forwarder.callsFor('get_group_msg_history').length).toBeGreaterThan(0)
})

test('incident pin: retries stay bounded and every retry passes the reconcile gate', async () => {
    let sendCalls = 0
    const forwarder = new StubQQForwarder(async (action) => {
        if (action === 'send_group_msg') {
            sendCalls += 1
            throw timeoutError()
        }
        if (action === 'get_login_info') {
            return loginEnvelope(3219117363)
        }
        if (action === 'get_group_msg_history') {
            return historyEnvelope([])
        }
        throw new Error(`unexpected action ${action}`)
    })

    await expect(forwarder.send('never landed')).rejects.toThrow('timeout of 60000ms exceeded')
    // RETRY_LIMIT=2 -> at most 3 sends, and every one of them had its delivery
    // checked against the bridge first.
    expect(sendCalls).toBe(3)
    expect(forwarder.callsFor('get_group_msg_history').length).toBeGreaterThanOrEqual(sendCalls)
})

test('ambiguous timeout with unreachable history is terminal: no blind resend', async () => {
    let sendCalls = 0
    const forwarder = new StubQQForwarder(async (action) => {
        if (action === 'send_group_msg') {
            sendCalls += 1
            throw timeoutError()
        }
        if (action === 'get_login_info') {
            return loginEnvelope(3219117363)
        }
        if (action === 'get_group_msg_history') {
            throw refusedError()
        }
        throw new Error(`unexpected action ${action}`)
    })

    let caught: unknown
    try {
        await forwarder.sendWithPayload([textSegment('do not duplicate me')])
    } catch (error) {
        caught = error
    }
    expect(caught instanceof NonRetryableForwarderSendError).toBe(true)
    expect(String((caught as Error).message)).toContain('refusing blind resend')
    expect(sendCalls).toBe(1)
})

test('never-delivered connection failures retry without a history reconcile', async () => {
    let sendCalls = 0
    const forwarder = new StubQQForwarder(async (action) => {
        if (action === 'send_group_msg') {
            sendCalls += 1
            throw refusedError()
        }
        throw new Error(`unexpected action ${action}`)
    })

    await expect(forwarder.sendWithPayload([textSegment('retry me safely')])).rejects.toThrow('ECONNREFUSED')
    expect(sendCalls).toBe(1)
    expect(forwarder.callsFor('get_group_msg_history')).toHaveLength(0)
})

test('text-less media payloads cannot be reconciled, so an ambiguous failure never resends', async () => {
    let sendCalls = 0
    const forwarder = new StubQQForwarder(async (action) => {
        if (action === 'send_group_msg') {
            sendCalls += 1
            throw timeoutError()
        }
        throw new Error(`unexpected action ${action}`)
    })

    let caught: unknown
    try {
        await forwarder.sendWithPayload([{ type: 'image', data: { file: 'http://127.0.0.1:1/x.png' } } as any])
    } catch (error) {
        caught = error
    }
    expect(caught instanceof NonRetryableForwarderSendError).toBe(true)
    expect(sendCalls).toBe(1)
    expect(forwarder.callsFor('get_group_msg_history')).toHaveLength(0)
})

test('merged-forward payloads are not history-reconcilable: ambiguous failures are terminal', async () => {
    let sendCalls = 0
    const forwarder = new StubQQForwarder(async (action) => {
        if (action === 'send_group_forward_msg') {
            sendCalls += 1
            throw timeoutError()
        }
        throw new Error(`unexpected action ${action}`)
    })

    let caught: unknown
    try {
        await forwarder.sendMergedForwardPayload([textSegment('forward node text')], {
            enabled: true,
            nodeName: 'idol-bbq',
            nodeUin: '10000',
            maxSegmentsPerNode: 12,
        })
    } catch (error) {
        caught = error
    }
    expect(caught instanceof NonRetryableForwarderSendError).toBe(true)
    expect(sendCalls).toBe(1)
    expect(forwarder.callsFor('get_group_msg_history')).toHaveLength(0)
})
