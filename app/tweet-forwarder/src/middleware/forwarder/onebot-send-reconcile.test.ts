import { expect, test } from 'bun:test'
import { buildSendTextSignature, findDeliveredMessageId } from './onebot-send-reconcile'

const nowSec = Math.floor(Date.now() / 1000)

test('buildSendTextSignature joins only text segments and normalizes newlines', () => {
    expect(
        buildSendTextSignature([
            { type: 'text', data: { text: 'line1\r\nline2' } },
            { type: 'image', data: { file: 'http://example/x.png' } },
            { type: 'text', data: { text: ' tail ' } },
        ]),
    ).toBe('line1\nline2\ntail')
    expect(buildSendTextSignature([{ type: 'image', data: { file: 'x' } }])).toBe('')
    expect(buildSendTextSignature('not-an-array')).toBe('')
})

test('findDeliveredMessageId matches the bridge copy of an ambiguously failed send', () => {
    const signature = buildSendTextSignature([{ type: 'text', data: { text: '🎉 Ratatan release' } }])
    const history = [
        {
            message_id: 111,
            time: nowSec - 30,
            sender: { user_id: 3219117363 },
            message: [{ type: 'text', data: { text: 'unrelated chatter' } }],
        },
        {
            message_id: 222,
            time: nowSec - 3,
            sender: { user_id: 3219117363 },
            message: [{ type: 'text', data: { text: '🎉 Ratatan release' } }, { type: 'image', data: { url: 'https://x/y' } }],
        },
    ]
    expect(
        findDeliveredMessageId(history, {
            signature,
            sinceEpochSec: nowSec - 20,
            selfId: 3219117363,
        }),
    ).toBe('222')
})

test('findDeliveredMessageId ignores other senders, stale copies and taken ids', () => {
    const signature = buildSendTextSignature([{ type: 'text', data: { text: 'same text' } }])
    const foreign = {
        message_id: 1,
        time: nowSec - 1,
        sender: { user_id: 999 },
        message: [{ type: 'text', data: { text: 'same text' } }],
    }
    const stale = {
        message_id: 2,
        time: nowSec - 600,
        sender: { user_id: 3219117363 },
        message: [{ type: 'text', data: { text: 'same text' } }],
    }
    const taken = {
        message_id: 3,
        time: nowSec - 1,
        sender: { user_id: 3219117363 },
        message: [{ type: 'text', data: { text: 'same text' } }],
    }
    expect(findDeliveredMessageId([foreign], { signature, sinceEpochSec: nowSec - 20, selfId: 3219117363 })).toBeNull()
    expect(findDeliveredMessageId([stale], { signature, sinceEpochSec: nowSec - 20, selfId: 3219117363 })).toBeNull()
    expect(
        findDeliveredMessageId([taken], {
            signature,
            sinceEpochSec: nowSec - 20,
            selfId: 3219117363,
            takenMessageIds: new Set(['3']),
        }),
    ).toBeNull()
})

test('findDeliveredMessageId never matches an empty signature or empty history', () => {
    expect(findDeliveredMessageId([], { signature: '', sinceEpochSec: nowSec - 20 })).toBeNull()
    expect(findDeliveredMessageId([{ message_id: 1, time: nowSec, message: [] }], { signature: '', sinceEpochSec: 0 })).toBeNull()
    expect(findDeliveredMessageId(undefined, { signature: 'x', sinceEpochSec: 0 })).toBeNull()
})
