import { expect, test } from 'bun:test'
import { assertBiliResponseOk, BilibiliApiClient, BiliUploadVelocityError } from './bilibili-api'
import { NonRetryableForwarderSendError } from './base'

test('assertBiliResponseOk returns the data payload on code 0', () => {
    const payload = { dyn_id_str: '12345' }
    expect(assertBiliResponseOk({ data: { code: 0, data: payload } }, 'text dynamic')).toBe(payload)
})

test('assertBiliResponseOk maps -101 to a non-retryable auth failure', () => {
    let thrown: unknown
    try {
        assertBiliResponseOk({ data: { code: -101, message: '账号未登录' } }, 'photo upload')
    } catch (error) {
        thrown = error
    }
    expect(thrown).toBeInstanceOf(NonRetryableForwarderSendError)
    expect(thrown).not.toBeInstanceOf(BiliUploadVelocityError)
    expect((thrown as Error).message).toContain('-101')
})

test('assertBiliResponseOk maps -111 to a retryable velocity error that is still non-retryable at whole-send', () => {
    let thrown: unknown
    try {
        assertBiliResponseOk({ data: { code: -111, message: 'csrf校验失败' } }, 'photo upload')
    } catch (error) {
        thrown = error
    }
    // Velocity error extends NonRetryableForwarderSendError so the whole-send layer never re-uploads,
    // while the per-photo retry loop opts back in via an explicit instanceof check.
    expect(thrown).toBeInstanceOf(BiliUploadVelocityError)
    expect(thrown).toBeInstanceOf(NonRetryableForwarderSendError)
})

test('assertBiliResponseOk maps an unclassified code to a generic retryable error', () => {
    let thrown: unknown
    try {
        assertBiliResponseOk({ data: { code: 4100000, message: 'risk control' } }, 'photo dynamic chunk 1/1')
    } catch (error) {
        thrown = error
    }
    expect(thrown).toBeInstanceOf(Error)
    expect(thrown).not.toBeInstanceOf(NonRetryableForwarderSendError)
    expect((thrown as Error).message).toContain('4100000')
})

test('assertBiliResponseOk uses the provided generic message override when given', () => {
    let thrown: unknown
    try {
        assertBiliResponseOk({ data: { code: -412, message: 'risk' } }, 'photo upload', 'Upload photo to bilibili failed. custom')
    } catch (error) {
        thrown = error
    }
    expect((thrown as Error).message).toBe('Upload photo to bilibili failed. custom')
})

test('BilibiliApiClient builds cookie header with buvid pair once set', () => {
    const client = new BilibiliApiClient({ bili_jct: 'jct', sessdata: 'sess' })
    expect(client.hasBuvid).toBe(false)
    expect(client.cookieHeader).toBe('SESSDATA=sess; bili_jct=jct')

    client.setBuvid('b3', 'b4')
    expect(client.hasBuvid).toBe(true)
    expect(client.cookieHeader).toBe('SESSDATA=sess; bili_jct=jct; buvid3=b3; buvid4=b4')
})

test('BilibiliApiClient exposes the web dynamic headers', () => {
    const client = new BilibiliApiClient({ bili_jct: 'jct', sessdata: 'sess' })
    const headers = client.headers
    expect(headers.Referer).toBe('https://t.bilibili.com/')
    expect(headers.Origin).toBe('https://t.bilibili.com')
    expect(headers['User-Agent']).toContain('Mozilla/5.0')
})
