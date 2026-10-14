import { expect, test } from 'bun:test'
import axios from 'axios'
import DB from '@/db'
import { APIManager } from './api-manager'

type StubCall = { path: string; body: any; headers: any }

function buildManager() {
    return new APIManager({
        getConfig: () =>
            ({
                api: { secret: 'test-secret' },
                forward_targets: [
                    {
                        id: 'qq-group',
                        platform: 'qq',
                        cfg_platform: {
                            url: 'http://127.0.0.1:3000',
                            token: 'onebot-token',
                            group_id: '227',
                        },
                    },
                ],
            }) as any,
        getDeps: () => ({}),
    } as any)
}

function stubOneBotPost(responses: Record<string, any>) {
    const calls: StubCall[] = []
    const original = axios.post
    ;(axios as any).post = async (path: string, body: any, options: any) => {
        calls.push({ path, body, headers: options?.headers || {} })
        const action = String(path).split('/').pop()!
        const responder = responses[action]
        return { data: responder ? responder(body) : { status: 'ok', retcode: 0, data: null } }
    }
    return {
        calls,
        restore: () => {
            ;(axios as any).post = original
        },
    }
}

test('POST /api/actions/qq/recall recalls explicit message ids via delete_msg', async () => {
    const manager = buildManager()
    const stub = stubOneBotPost({
        delete_msg: (body: any) => ({ status: 'ok', retcode: 0, data: null, echo: body }),
    })
    try {
        const response = await (manager as any).handleQqRecall(
            new Request('http://localhost/api/actions/qq/recall', {
                method: 'POST',
                body: JSON.stringify({ message_ids: ['1001', '1002'] }),
            }),
        )
        expect(response.status).toBe(200)
        const payload = await response.json()
        expect(payload.success).toBe(true)
        expect(payload.results).toHaveLength(2)
        expect(stub.calls.map((call) => call.path)).toEqual([
            'http://127.0.0.1:3000/delete_msg',
            'http://127.0.0.1:3000/delete_msg',
        ])
        expect(stub.calls[0].body).toMatchObject({ message_id: '1001' })
        expect(stub.calls[0].headers.Authorization).toBe('Bearer onebot-token')
    } finally {
        stub.restore()
    }
})

test('POST /api/actions/qq/recall resolves message ids from the outbound record', async () => {
    const manager = buildManager()
    const stub = stubOneBotPost({})
    const originalGet = DB.OutboundMessage.getByIdempotencyKey
    ;(DB.OutboundMessage as any).getByIdempotencyKey = async (key: string) => ({
        idempotency_key: key,
        status: 'sent',
        message_ids: ['777'],
    })
    try {
        const response = await (manager as any).handleQqRecall(
            new Request('http://localhost/api/actions/qq/recall', {
                method: 'POST',
                body: JSON.stringify({ idempotency_key: 'article:qq-group:1:abc' }),
            }),
        )
        const payload = await response.json()
        expect(payload.success).toBe(true)
        expect(stub.calls[0].body).toMatchObject({ message_id: '777' })
    } finally {
        ;(DB.OutboundMessage as any).getByIdempotencyKey = originalGet
        stub.restore()
    }
})

test('POST /api/actions/qq/recall requires a message id', async () => {
    const manager = buildManager()
    const response = await (manager as any).handleQqRecall(
        new Request('http://localhost/api/actions/qq/recall', {
            method: 'POST',
            body: JSON.stringify({}),
        }),
    )
    expect(response.status).toBe(400)
    expect(await response.json()).toMatchObject({ error: 'message_id_required' })
})

test('POST /api/actions/qq/receipt-check verifies ids against get_msg', async () => {
    const manager = buildManager()
    const stub = stubOneBotPost({
        get_msg: () => ({
            status: 'ok',
            retcode: 0,
            data: { message_id: '9001', real_id: 555, message: [{ type: 'text', data: { text: 'hi' } }] },
        }),
    })
    try {
        const response = await (manager as any).handleQqReceiptCheck(
            new Request('http://localhost/api/actions/qq/receipt-check', {
                method: 'POST',
                body: JSON.stringify({ message_id: '9001' }),
            }),
        )
        expect(response.status).toBe(200)
        const payload = await response.json()
        expect(payload.success).toBe(true)
        expect(payload.results[0]).toMatchObject({ ok: true, messageId: '9001', realId: 555 })
        expect(stub.calls[0].path).toBe('http://127.0.0.1:3000/get_msg')
    } finally {
        stub.restore()
    }
})

test('GET /api/actions/qq/health probes get_status and records target health', async () => {
    const manager = buildManager()
    const stub = stubOneBotPost({
        get_status: () => ({ status: 'ok', retcode: 0, data: { online: true, good: true, time: 60 } }),
        get_version_info: () => ({ status: 'ok', retcode: 0, data: { app_name: 'SnowLuma' } }),
    })
    const healthMarks: any[] = []
    const originalMark = DB.TargetHealth.mark
    ;(DB.TargetHealth as any).mark = async (entry: any) => {
        healthMarks.push(entry)
    }
    try {
        const response = await (manager as any).handleQqHealth(
            new URL('http://localhost/api/actions/qq/health?target_id=qq-group'),
        )
        expect(response.status).toBe(200)
        const payload = await response.json()
        expect(payload.health).toMatchObject({ online: true, good: true, flavor: 'snowluma' })
        expect(healthMarks).toHaveLength(1)
        expect(healthMarks[0]).toMatchObject({ target_id: 'qq-group', provider: 'qq', status: 'ok' })
    } finally {
        ;(DB.TargetHealth as any).mark = originalMark
        stub.restore()
    }
})

test('QQ bridge endpoints report a rejected business failure instead of pretending success', async () => {
    const manager = buildManager()
    const stub = stubOneBotPost({
        delete_msg: () => ({ status: 'failed', retcode: 100, wording: '撤回失败: 消息已过期' }),
    })
    try {
        const response = await (manager as any).handleQqRecall(
            new Request('http://localhost/api/actions/qq/recall', {
                method: 'POST',
                body: JSON.stringify({ message_id: '1' }),
            }),
        )
        const payload = await response.json()
        expect(payload.success).toBe(false)
        expect(payload.results[0].detail).toMatch(/消息已过期/)
    } finally {
        stub.restore()
    }
})
