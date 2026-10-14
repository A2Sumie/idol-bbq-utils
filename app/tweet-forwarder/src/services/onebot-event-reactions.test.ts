import { expect, test } from 'bun:test'
import { runReactionChain, isAdminSender, type QqInboundConfig } from './onebot-event-reactions'
import { HandlerRateLimiter } from './onebot-event-dedupe'
import { normalizeOneBotEvent, type NormalizedOneBotEvent } from './onebot-event-ingest'

type Call = { path: string; body: any }

function makePost(responder?: (action: string, body: any) => any) {
    const calls: Array<Call> = []
    const post = async (path: string, body: any) => {
        calls.push({ path, body })
        const action = path.split('/').pop() || ''
        const custom = responder?.(action, body)
        if (custom instanceof Error) throw custom
        if (custom !== undefined) return { data: custom }
        return { data: { status: 'ok', retcode: 0, data: { message_id: 4242 } } }
    }
    return { post, calls }
}

function groupMessage(overrides: Partial<Record<string, unknown>> = {}): NormalizedOneBotEvent {
    const event = normalizeOneBotEvent({
        time: 1_777_777_777,
        self_id: 3020_0001,
        post_type: 'message',
        message_type: 'group',
        message_id: 99887766,
        group_id: 999,
        user_id: 12345,
        message: [{ type: 'text', data: { text: 'hello there' } }],
        raw_message: 'hello there',
        sender: { user_id: 12345, nickname: 'tester', role: 'member' },
        ...overrides,
    })
    if (!event) throw new Error('fixture did not normalize')
    return event
}

const baseCtx = () => ({
    endpoint: { endpoint: 'http://127.0.0.1:3200', token: 'tok' },
    admins: ['10001'],
    botUserIds: ['30200001'],
})

test('reply handler quotes the triggering message and answers in the same group', async () => {
    const { post, calls } = makePost()
    const event = groupMessage()
    const result = await runReactionChain(
        event,
        {
            enabled: true,
            handlers: [
                {
                    id: 'hello-reply',
                    type: 'reply',
                    match: { text_regex: 'hello' },
                    reply: { template: 'echo: {text}', quote: true, at_sender: true },
                },
            ],
        },
        { ...baseCtx(), post },
    )
    expect(result.handled).toBe(true)
    expect(result.actions[0]).toMatchObject({ handlerId: 'hello-reply', type: 'reply', status: 'ok' })
    expect(calls).toHaveLength(1)
    expect(calls[0]!.path).toContain('send_group_msg')
    expect(calls[0]!.body.group_id).toBe('999')
    const segments = calls[0]!.body.message
    expect(segments[0]).toEqual({ type: 'reply', data: { message_id: '99887766' } })
    expect(segments[1]).toEqual({ type: 'at', data: { qq: '12345' } })
    expect(segments[2]).toEqual({ type: 'text', data: { text: 'echo: hello there' } })
})

test('forward/alert handler relays to configured group + admin private chat', async () => {
    const { post, calls } = makePost()
    const result = await runReactionChain(
        groupMessage({ message: [{ type: 'text', data: { text: '紧急事件!' } }], raw_message: '紧急事件!' }),
        {
            enabled: true,
            handlers: [
                {
                    id: 'alert',
                    type: 'forward',
                    match: { text_regex: '紧急' },
                    forward: {
                        template: '[告警] {group_id}/{sender_name}: {text}',
                        group_ids: ['8888'],
                        user_ids: ['10001'],
                    },
                },
            ],
        },
        { ...baseCtx(), post },
    )
    expect(result.actions[0]).toMatchObject({ type: 'forward', status: 'ok' })
    expect(calls.map((call) => call.body.group_id ?? call.body.user_id)).toEqual(['8888', '10001'])
    expect(calls[0]!.body.message[0].data.text).toBe('[告警] 999/tester: 紧急事件!')
})

test('forward handler resolves forward_target ids through resolveTarget', async () => {
    const { post, calls } = makePost()
    await runReactionChain(
        groupMessage(),
        {
            enabled: true,
            handlers: [
                {
                    id: 'by-target',
                    type: 'forward',
                    match: { text_regex: 'hello' },
                    forward: { target_ids: ['七虹信标-群1'], template: 'x' },
                },
            ],
        },
        {
            ...baseCtx(),
            post,
            resolveTarget: (id) => (id === '七虹信标-群1' ? { group_id: '424242' } : null),
        },
    )
    expect(calls[0]!.body.group_id).toBe('424242')
})

test('rule miss skips the handler without any outbound call', async () => {
    const { post, calls } = makePost()
    const result = await runReactionChain(
        groupMessage(),
        {
            enabled: true,
            handlers: [
                { id: 'nope', type: 'reply', match: { text_regex: 'GOODBYE' }, reply: { template: 'hi' } },
                { id: 'group-guard', type: 'reply', match: { group_ids: ['1'] }, reply: { template: 'hi' } },
                { id: 'user-guard', type: 'reply', match: { user_ids: ['2'] }, reply: { template: 'hi' } },
                { id: 'at-guard', type: 'reply', match: { at_bot: true }, reply: { template: 'hi' } },
                { id: 'admin-guard', type: 'reply', match: { require_admin: true }, reply: { template: 'hi' } },
            ],
        },
        { ...baseCtx(), post },
    )
    expect(result.handled).toBe(false)
    expect(calls).toHaveLength(0)
    expect(result.actions.every((action) => action.status === 'skipped')).toBe(true)
})

test('command handler: #status works for admins only', async () => {
    const responder = (action: string) => {
        if (action === 'get_status') return { status: 'ok', retcode: 0, data: { online: true, good: true, time: 42 } }
        if (action === 'get_version_info') return { status: 'ok', retcode: 0, data: { app_name: 'SnowLuma' } }
        return undefined
    }
    const adminRun = makePost(responder)
    const adminEvent = groupMessage({
        message: [{ type: 'text', data: { text: '#status' } }],
        raw_message: '#status',
        user_id: 10001,
    })
    const adminResult = await runReactionChain(
        adminEvent,
        {
            enabled: true,
            handlers: [{ id: 'cmd', type: 'command', command: { commands: ['status'], admin_only: true } }],
        },
        { ...baseCtx(), post: adminRun.post },
    )
    expect(adminResult.actions[0]).toMatchObject({ type: 'command', status: 'ok' })
    expect(adminResult.actions[0]!.detail).toContain('command status')
    expect(adminRun.calls.map((call) => call.path.split('/').pop())).toEqual([
        'get_status',
        'get_version_info',
        'send_group_msg',
    ])
    expect(adminRun.calls[2]!.body.message[0].data.text).toContain('flavor=snowluma')

    const intruderRun = makePost(responder)
    const intruderResult = await runReactionChain(
        groupMessage({
            message: [{ type: 'text', data: { text: '#status' } }],
            raw_message: '#status',
            user_id: 555,
        }),
        {
            enabled: true,
            handlers: [{ id: 'cmd', type: 'command', command: { commands: ['status'], admin_only: true } }],
        },
        { ...baseCtx(), post: intruderRun.post },
    )
    expect(intruderResult.actions[0]).toMatchObject({ status: 'skipped' })
    expect(intruderRun.calls).toHaveLength(0)
})

test('#recall is always admin-gated and calls delete_msg', async () => {
    const nonAdmin = makePost()
    const nonAdminResult = await runReactionChain(
        groupMessage({
            message: [{ type: 'text', data: { text: '#recall 123' } }],
            raw_message: '#recall 123',
            user_id: 555,
        }),
        {
            enabled: true,
            // admin_only explicitly disabled — recall must STILL refuse.
            handlers: [{ id: 'cmd', type: 'command', command: { commands: ['recall'], admin_only: false } }],
        },
        { ...baseCtx(), post: nonAdmin.post },
    )
    expect(nonAdminResult.actions[0]).toMatchObject({ status: 'skipped' })
    expect(nonAdmin.calls).toHaveLength(0)

    const admin = makePost()
    const adminResult = await runReactionChain(
        groupMessage({
            message: [{ type: 'text', data: { text: '#recall 123' } }],
            raw_message: '#recall 123',
            user_id: 10001,
        }),
        {
            enabled: true,
            handlers: [{ id: 'cmd', type: 'command', command: { commands: ['recall'], admin_only: true } }],
        },
        { ...baseCtx(), post: admin.post },
    )
    expect(adminResult.actions[0]).toMatchObject({ type: 'command', status: 'ok' })
    expect(admin.calls[0]!.path).toContain('delete_msg')
    expect(admin.calls[0]!.body).toEqual({ message_id: '123' })
    expect(admin.calls[1]!.path).toContain('send_group_msg')
})

test('group_action stays off by default and needs allow switch + allowlist + admin', async () => {
    const config: QqInboundConfig = {
        enabled: true,
        handlers: [
            {
                id: 'ban',
                type: 'group_action',
                match: { group_ids: ['999'] },
                group_action: { action: 'ban', duration_seconds: 120 },
            },
        ],
    }
    const defaultOff = makePost()
    const offResult = await runReactionChain(
        groupMessage({ user_id: 10001 }),
        config,
        { ...baseCtx(), post: defaultOff.post },
    )
    expect(offResult.actions[0]).toMatchObject({ status: 'disabled' })
    expect(defaultOff.calls).toHaveLength(0)

    const notAdmin = makePost()
    const notAdminResult = await runReactionChain(
        groupMessage({ user_id: 555 }),
        { ...config, allow_group_actions: true },
        { ...baseCtx(), post: notAdmin.post },
    )
    expect(notAdminResult.actions[0]).toMatchObject({ status: 'skipped' })
    expect(notAdmin.calls).toHaveLength(0)

    const noAllowlist = makePost()
    const noAllowlistResult = await runReactionChain(
        groupMessage({ user_id: 10001 }),
        {
            enabled: true,
            allow_group_actions: true,
            handlers: [{ id: 'ban', type: 'group_action', group_action: { action: 'ban' } }],
        },
        { ...baseCtx(), post: noAllowlist.post },
    )
    expect(noAllowlistResult.actions[0]).toMatchObject({ status: 'disabled' })

    const ok = makePost()
    const okResult = await runReactionChain(
        groupMessage({ user_id: 10001 }),
        { ...config, allow_group_actions: true },
        { ...baseCtx(), post: ok.post },
    )
    expect(okResult.actions[0]).toMatchObject({ status: 'ok' })
    expect(ok.calls[0]!.path).toContain('set_group_ban')
    expect(ok.calls[0]!.body).toEqual({ group_id: '999', user_id: '10001', duration: 120 })
})

test('quick_operation handler returns a bridge quick-op payload', async () => {
    const { post, calls } = makePost()
    const result = await runReactionChain(
        groupMessage(),
        {
            enabled: true,
            handlers: [
                {
                    id: 'quick',
                    type: 'quick_operation',
                    match: { text_regex: 'hello' },
                    quick_operation: { reply: 'auto reply: {text}', at_sender: true },
                },
            ],
        },
        { ...baseCtx(), post },
    )
    expect(result.quickOperation).toEqual({ reply: 'auto reply: hello there', at_sender: true })
    expect(result.actions[0]).toMatchObject({ type: 'quick_operation', status: 'ok' })
    // Quick operation is answered through the HTTP response, never a second call.
    expect(calls).toHaveLength(0)
})

test('self echo (bot messages / message_sent) never reaches handlers', async () => {
    const { post, calls } = makePost()
    const own = groupMessage({ user_id: 30200001 })
    const ownResult = await runReactionChain(
        own,
        { enabled: true, handlers: [{ id: 'r', type: 'reply', match: { text_regex: 'hello' }, reply: { template: 'x' } }] },
        { ...baseCtx(), post },
    )
    expect(ownResult.handled).toBe(false)
    expect(ownResult.actions[0]!.detail).toContain('self echo')

    const echo = groupMessage({ post_type: 'message_sent' })
    const echoResult = await runReactionChain(
        echo,
        { enabled: true, handlers: [{ id: 'r', type: 'reply', reply: { template: 'x' } }] },
        { ...baseCtx(), post },
    )
    expect(echoResult.handled).toBe(false)
    expect(calls).toHaveLength(0)
})

test('failing handler is isolated: later handlers still run and nothing throws', async () => {
    const { post, calls } = makePost((action) => {
        if (action === 'send_group_msg') return new Error('bridge exploded')
        return undefined
    })
    const result = await runReactionChain(
        groupMessage(),
        {
            enabled: true,
            handlers: [
                { id: 'boom', type: 'reply', match: { text_regex: 'hello' }, reply: { template: 'x' } },
                { id: 'quick', type: 'quick_operation', quick_operation: { reply: 'still alive' } },
            ],
        },
        { ...baseCtx(), post },
    )
    expect(result.actions[0]).toMatchObject({ handlerId: 'boom', status: 'failed' })
    expect(result.actions[1]).toMatchObject({ handlerId: 'quick', status: 'ok' })
    expect(result.quickOperation).toEqual({ reply: 'still alive' })
    expect(calls).toHaveLength(1)
})

test('bridge rejection (retcode wording) is reported as failed without retry', async () => {
    const { post, calls } = makePost(() => ({ status: 'failed', retcode: 100, wording: '风控拦截' }))
    const result = await runReactionChain(
        groupMessage(),
        { enabled: true, handlers: [{ id: 'r', type: 'reply', reply: { template: 'x' } }] },
        { ...baseCtx(), post },
    )
    expect(result.actions[0]).toMatchObject({ status: 'failed' })
    expect(result.actions[0]!.detail).toContain('风控')
    expect(calls).toHaveLength(1)
})

test('per-handler cooldown + quota limit reaction storms', async () => {
    const { post, calls } = makePost()
    const clock = { t: 1_000_000 }
    const limiter = new HandlerRateLimiter(() => clock.t)
    const config: QqInboundConfig = {
        enabled: true,
        handlers: [{ id: 'r', type: 'reply', cooldown_seconds: 30, max_per_hour: 2, reply: { template: 'x' } }],
    }
    const first = await runReactionChain(groupMessage(), config, { ...baseCtx(), post, limiter })
    expect(first.actions[0]!.status).toBe('ok')
    clock.t += 60_000
    const second = await runReactionChain(groupMessage(), config, { ...baseCtx(), post, limiter })
    expect(second.actions[0]!.status).toBe('ok')
    clock.t += 60_000
    const third = await runReactionChain(groupMessage(), config, { ...baseCtx(), post, limiter })
    expect(third.actions[0]!.status).toBe('limited')
    expect(calls).toHaveLength(2)
})

test('command replies report a dead bridge as failed instead of ok', async () => {
    const { post, calls } = makePost(() => new Error('ECONNREFUSED'))
    const result = await runReactionChain(
        groupMessage({
            message: [{ type: 'text', data: { text: '#ibping' } }],
            raw_message: '#ibping',
            user_id: 10001,
        }),
        {
            enabled: true,
            handlers: [{ id: 'cmd', type: 'command', command: { commands: ['ping'], prefix: '#ib', admin_only: true } }],
        },
        { ...baseCtx(), post },
    )
    expect(result.actions[0]).toMatchObject({ type: 'command', status: 'failed' })
    expect(result.actions[0]!.calls![0]).toMatchObject({ action: 'send_msg', ok: false })
    expect(calls).toHaveLength(1)
})

test('disabled handlers and unknown command prefixes are inert', async () => {
    const { post, calls } = makePost()
    const result = await runReactionChain(
        groupMessage({ message: [{ type: 'text', data: { text: 'ping' } }], raw_message: 'ping' }),
        {
            enabled: true,
            handlers: [
                { id: 'off', type: 'reply', enabled: false, reply: { template: 'x' } },
                { id: 'cmd', type: 'command', command: { commands: ['status'] } },
            ],
        },
        { ...baseCtx(), post },
    )
    expect(result.handled).toBe(false)
    expect(calls).toHaveLength(0)
    expect(isAdminSender(groupMessage({ user_id: 10001 }), ['10001'])).toBe(true)
    expect(isAdminSender(groupMessage({ user_id: 2 }), ['10001'])).toBe(false)
})
