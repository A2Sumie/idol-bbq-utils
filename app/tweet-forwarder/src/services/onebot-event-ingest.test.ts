import { expect, test } from 'bun:test'
import {
    extractOneBotMessageParts,
    isSelfEchoEvent,
    normalizeOneBotEvent,
    parseCqMessage,
    unescapeCqText,
} from './onebot-event-ingest'

const snowlumaArrayGroupMessage = {
    time: 1_777_777_777,
    self_id: 3020_0001,
    post_type: 'message',
    message_type: 'group',
    sub_type: 'normal',
    message_id: 99887766,
    group_id: 999,
    user_id: 12345,
    message: [
        { type: 'text', data: { text: '看看这个 ' } },
        { type: 'at', data: { qq: 3020_0001, targetUin: 3020_0001 } },
        { type: 'text', data: { text: ' https://x.com/kyu0817a/status/123' } },
    ],
    raw_message: '看看这个 [CQ:at,qq=30200001] https://x.com/kyu0817a/status/123',
    sender: { user_id: 12345, nickname: '测试', card: '群名片', role: 'member' },
}

const napcatStringGroupMessage = {
    time: 1_777_777_778,
    self_id: 3020_0001,
    post_type: 'message',
    message_type: 'group',
    sub_type: 'normal',
    message_id: 11223344,
    group_id: 999,
    user_id: 5566,
    message: '[CQ:reply,id=555][CQ:at,qq=30200001]你好 &amp; &#91;ok&#93;',
    raw_message: '[CQ:reply,id=555][CQ:at,qq=30200001]你好 &amp; &#91;ok&#93;',
    sender: { user_id: 5566, nickname: 'napcat-user', role: 'admin' },
}

test('normalizes SnowLuma array-format group messages (text + at + link)', () => {
    const event = normalizeOneBotEvent(snowlumaArrayGroupMessage)
    expect(event).not.toBeNull()
    expect(event!.kind).toBe('message')
    expect(event!.messageType).toBe('group')
    expect(event!.groupId).toBe('999')
    expect(event!.userId).toBe('12345')
    expect(event!.messageId).toBe('99887766')
    expect(event!.text).toContain('https://x.com/kyu0817a/status/123')
    expect(event!.atUserIds).toEqual(['30200001'])
    expect(event!.atBot).toBe(true)
    expect(event!.senderName).toBe('群名片')
    expect(event!.senderRole).toBe('member')
    expect(event!.messageFormat).toBe('array')
    expect(event!.dedupeKey).toBe('msg:30200001:99887766')
})

test('normalizes NapCat CQ-string group messages (reply/at codes + CQ unescape)', () => {
    const event = normalizeOneBotEvent(napcatStringGroupMessage)
    expect(event).not.toBeNull()
    expect(event!.messageFormat).toBe('string')
    expect(event!.messageId).toBe('11223344')
    expect(event!.text).toBe('你好 & [ok]')
    expect(event!.atUserIds).toEqual(['30200001'])
    expect(event!.atBot).toBe(true)
    expect(event!.senderName).toBe('napcat-user')
    expect(event!.senderRole).toBe('admin')
    expect(event!.dedupeKey).toBe('msg:30200001:11223344')
})

test('private messages, group temp sessions and at-all normalize predictably', () => {
    const privateEvent = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'message',
        message_type: 'private',
        message_id: 7,
        user_id: 42,
        message: 'hi',
        sender: { user_id: 42, nickname: 'n' },
    })
    expect(privateEvent!.messageType).toBe('private')
    expect(privateEvent!.groupId).toBeNull()

    const tempSession = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'message',
        message_type: 'private',
        message_id: 8,
        group_id: 999,
        user_id: 42,
        message: 'temp',
        sender: { user_id: 42 },
    })
    expect(tempSession!.messageType).toBe('private')
    expect(tempSession!.groupId).toBe('999')

    const atAll = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'message',
        message_type: 'group',
        message_id: 9,
        group_id: 5,
        user_id: 42,
        message: [{ type: 'at', data: { qq: 'all' } }, { type: 'text', data: { text: 'all hands' } }],
        sender: { user_id: 42 },
    })
    expect(atAll!.atAll).toBe(true)
    expect(atAll!.atBot).toBe(true)
})

test('notice and request events of both dialects normalize with dedupe keys', () => {
    const recall = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'notice',
        notice_type: 'group_recall',
        group_id: 999,
        user_id: 42,
        operator_id: 42,
        message_id: 1234,
    })
    expect(recall!.kind).toBe('notice')
    expect(recall!.operatorId).toBe('42')
    expect(recall!.dedupeKey).toBe('notice:1:group_recall::1234:999:42:42')

    const snowlumaExtra = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'notice',
        notice_type: 'notify',
        sub_type: 'input_status',
        group_id: 999,
        user_id: 42,
        status: 'typing',
    })
    expect(snowlumaExtra!.kind).toBe('notice')
    expect(snowlumaExtra!.subType).toBe('input_status')

    const groupRequest = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'request',
        request_type: 'group',
        sub_type: 'add',
        group_id: 999,
        user_id: 42,
        comment: '求通过',
        flag: 'req-flag-1',
    })
    expect(groupRequest!.kind).toBe('request')
    expect(groupRequest!.requestFlag).toBe('req-flag-1')
    expect(groupRequest!.comment).toBe('求通过')
    expect(groupRequest!.dedupeKey).toBe('req:1:group:add:req-flag-1')

    const unknownNotice = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'notice',
        notice_type: 'brand_new_thing',
        group_id: 1,
        user_id: 2,
    })
    expect(unknownNotice).not.toBeNull()
    expect(unknownNotice!.kind).toBe('notice')
})

test('meta_event and message_sent normalize but are never useful triggers', () => {
    const heartbeat = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'meta_event',
        meta_event_type: 'heartbeat',
        status: { online: true },
        interval: 5000,
    })
    expect(heartbeat!.kind).toBe('meta_event')

    const echo = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'message_sent',
        message_type: 'group',
        message_id: 3,
        group_id: 9,
        user_id: 1,
        message: 'outbound echo',
        sender: { user_id: 1 },
    })
    expect(echo!.postType).toBe('message_sent')
    expect(isSelfEchoEvent(echo!)).toBe(true)
})

test('bad events degrade to null instead of throwing', () => {
    expect(normalizeOneBotEvent(null)).toBeNull()
    expect(normalizeOneBotEvent('just text')).toBeNull()
    expect(normalizeOneBotEvent([1, 2, 3])).toBeNull()
    expect(normalizeOneBotEvent({ hello: 'world' })).toBeNull()
    expect(normalizeOneBotEvent({ post_type: '' })).toBeNull()
    // Unknown post_type still normalizes (kind unknown) instead of exploding.
    expect(normalizeOneBotEvent({ post_type: 'weird_future_type', user_id: 1 })!.kind).toBe('unknown')
})

test('self echo guard covers configured bot uins and self-authored messages', () => {
    const ownMessage = normalizeOneBotEvent({
        self_id: 1,
        post_type: 'message',
        message_type: 'group',
        message_id: 5,
        group_id: 9,
        user_id: 777,
        message: 'loop',
        sender: { user_id: 777 },
    })!
    expect(isSelfEchoEvent(ownMessage, ['777'])).toBe(true)
    expect(isSelfEchoEvent(ownMessage, ['999'])).toBe(false)
    expect(isSelfEchoEvent(ownMessage, [])).toBe(false)
})

test('CQ parsing handles escapes and unknown codes', () => {
    expect(unescapeCqText('a &amp; b &#91;c&#93; &#44;')).toBe('a & b [c] ,')
    const pieces = parseCqMessage('[CQ:image,file=a.png]hello[CQ:face,id=1]')
    expect(pieces.map((piece) => piece.text).join('')).toBe('hello')
    expect(pieces.some((piece) => piece.at)).toBe(false)
    expect(extractOneBotMessageParts('[CQ:at,qq=1]yo').atUserIds).toEqual(['1'])
    expect(extractOneBotMessageParts([{ type: 'text', data: { text: 'x' } }]).text).toBe('x')
})
