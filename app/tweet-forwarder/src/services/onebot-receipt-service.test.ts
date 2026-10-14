import { expect, test } from 'bun:test'
import { extractProviderMessageIds } from '@/services/outbound-message-service'
import {
    fetchOneBotReceipt,
    probeQqBridgeHealth,
    recallOneBotMessage,
} from './onebot-receipt-service'

const endpoint = { endpoint: 'http://127.0.0.1:3000/', token: 'tok' }

function postReturning(body: any) {
    const calls: Array<{ path: string; body: any }> = []
    const post = async (path: string, payload: any) => {
        calls.push({ path, body: payload })
        return { data: body }
    }
    return { post, calls }
}

test('extractProviderMessageIds pulls OneBot data.message_id out of every send result shape', () => {
    // Single axios-style summarized response (what markSent receives).
    expect(
        extractProviderMessageIds({ status: 200, statusText: 'OK', data: { status: 'ok', retcode: 0, data: { message_id: 42 } } }),
    ).toEqual(['42'])
    // Array of per-segment responses (realSend returns one entry per visible segment).
    expect(
        extractProviderMessageIds([
            { status: 200, data: { status: 'ok', retcode: 0, data: { message_id: 1 } } },
            { status: 200, data: { status: 'ok', retcode: 0, data: { message_id: 2 } } },
        ]),
    ).toEqual(['1', '2'])
    // Partial results keep the ids of the segments that DID become visible.
    expect(extractProviderMessageIds([{ message_id: 'visible-1' }, { diagnostic: 'tail-not-sent' }])).toEqual([
        'visible-1',
    ])
    // Duplicates collapse; non-id payloads contribute nothing.
    expect(extractProviderMessageIds([{ message_id: 7 }, { message_id: 7 }, { retcode: 0 }])).toEqual(['7'])
    expect(extractProviderMessageIds(null)).toEqual([])
})

test('fetchOneBotReceipt reports the message-store copy and real_id', async () => {
    const { post, calls } = postReturning({
        status: 'ok',
        retcode: 0,
        data: { message_id: '9001', real_id: 123456, message: [{ type: 'text', data: { text: 'hi' } }] },
    })
    const receipt = await fetchOneBotReceipt(endpoint, '9001', post)
    expect(calls[0].path).toBe('http://127.0.0.1:3000/get_msg')
    expect(calls[0].body).toMatchObject({ message_id: '9001' })
    expect(receipt).toMatchObject({ ok: true, messageId: '9001', realId: 123456 })
})

test('fetchOneBotReceipt surfaces SnowLuma business failure as missing receipt', async () => {
    const { post } = postReturning({
        status: 'failed',
        retcode: 100,
        wording: 'get_msg failed: message not found',
    })
    const receipt = await fetchOneBotReceipt(endpoint, '404', post)
    expect(receipt.ok).toBe(false)
    expect(receipt.detail).toMatch(/retcode=100/)
})

test('recallOneBotMessage calls delete_msg and never retries on rejection wording', async () => {
    const okCall = postReturning({ status: 'ok', retcode: 0, data: null })
    const recalled = await recallOneBotMessage(endpoint, '9001', okCall.post)
    expect(okCall.calls[0].path).toBe('http://127.0.0.1:3000/delete_msg')
    expect(recalled).toMatchObject({ success: true, messageId: '9001' })

    const rejected = postReturning({ status: 'failed', retcode: 100, wording: '撤回失败: 消息已过期' })
    const failed = await recallOneBotMessage(endpoint, '9001', rejected.post)
    expect(failed.success).toBe(false)
    expect(failed.detail).toMatch(/消息已过期/)
})

test('probeQqBridgeHealth reads get_status and detects the SnowLuma flavor', async () => {
    const responses: Record<string, any> = {
        get_status: { status: 'ok', retcode: 0, data: { online: true, good: true, time: 3600 } },
        get_version_info: { status: 'ok', retcode: 0, data: { app_name: 'SnowLuma', app_version: '1.14.22-node' } },
    }
    const calls: string[] = []
    const health = await probeQqBridgeHealth(endpoint, async (path) => {
        const action = path.split('/').pop()!
        calls.push(action)
        return { data: responses[action] }
    })
    expect(calls).toEqual(['get_status', 'get_version_info'])
    expect(health).toMatchObject({ online: true, good: true, flavor: 'snowluma', uptimeSeconds: 3600 })
})

test('probeQqBridgeHealth reports a dead bridge without throwing', async () => {
    const health = await probeQqBridgeHealth(endpoint, async () => {
        throw Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:3000'), { code: 'ECONNREFUSED' })
    })
    expect(health.online).toBe(false)
    expect(health.good).toBe(false)
    expect(health.detail).toMatch(/transport error/)
})
