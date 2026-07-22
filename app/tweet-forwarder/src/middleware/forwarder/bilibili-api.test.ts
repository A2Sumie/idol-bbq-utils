import { expect, test } from 'bun:test'
import axios from 'axios'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
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

test('BilibiliApiClient builds a full WAF cookie header with volatile fields', () => {
    const client = new BilibiliApiClient({
        bili_jct: 'jct',
        sessdata: 'sess',
        cookies: {
            DedeUserID: 'mid',
            sid: 'sid-value',
            buvid_fp: 'fingerprint',
        },
    })
    expect(client.hasBuvid).toBe(false)
    expect(client.cookieHeader).toContain('SESSDATA=sess')
    expect(client.cookieHeader).toContain('bili_jct=jct')
    expect(client.cookieHeader).toContain('DedeUserID=mid')
    expect(client.cookieHeader).toContain('b_nut=')
    expect(client.cookieHeader).toContain('_uuid=')
    expect(client.cookieHeader).toContain('CURRENT_FNVAL=4048')
    expect(client.cookieHeader).toContain('b_lsid=')
    expect(client.cookieHeader).toContain('buvid_fp=fingerprint')

    client.setBuvid('b3', 'b4')
    expect(client.hasBuvid).toBe(true)
    expect(client.cookieHeader).toContain('buvid3=b3')
    expect(client.cookieHeader).toContain('buvid4=b4')
})

test('BilibiliApiClient exposes the web dynamic headers', () => {
    const client = new BilibiliApiClient({ bili_jct: 'jct', sessdata: 'sess' })
    const headers = client.headers
    expect(headers.Referer).toBe('https://t.bilibili.com/')
    expect(headers.Origin).toBe('https://t.bilibili.com')
    expect(headers['User-Agent']).toContain('Mozilla/5.0')
})

test('BilibiliApiClient sends upload_bfs with content-length and WAF cookies', async () => {
    const client = new BilibiliApiClient({
        bili_jct: 'jct',
        sessdata: 'sess',
        cookies: { DedeUserID: 'mid' },
    })
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'bili-api-upload-'))
    const photoPath = path.join(tempRoot, 'photo.jpg')
    await writeFile(photoPath, Buffer.alloc(16, 1))
    const originalPost = axios.post
    let capturedHeaders: any
    try {
        ;(axios as any).post = async (_url: string, _body: any, options: any) => {
            capturedHeaders = options.headers
            return { data: { code: 0, data: { image_url: 'ok' } } }
        }
        await client.uploadPhoto(photoPath)
    } finally {
        ;(axios as any).post = originalPost
        await rm(tempRoot, { recursive: true, force: true })
    }
    expect(Number(capturedHeaders['Content-Length'])).toBeGreaterThan(0)
    expect(capturedHeaders.Cookie).toContain('SESSDATA=sess')
    expect(capturedHeaders.Cookie).toContain('DedeUserID=mid')
    expect(capturedHeaders.Cookie).toContain('b_lsid=')
})

test('BilibiliApiClient bounds every dynamic call with a request timeout', async () => {
    const client = new BilibiliApiClient({ bili_jct: 'jct', sessdata: 'sess' })
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'bili-api-timeout-'))
    const photoPath = path.join(tempRoot, 'photo.jpg')
    await writeFile(photoPath, Buffer.alloc(16, 1))
    const originalPost = axios.post
    const originalGet = axios.get
    const postTimeouts: Array<unknown> = []
    const getTimeouts: Array<unknown> = []
    try {
        ;(axios as any).post = async (_url: string, _body: any, options: any) => {
            postTimeouts.push(options?.timeout)
            return { data: { code: 0, data: { image_url: 'ok', dyn_id_str: '1' } } }
        }
        ;(axios as any).get = async (_url: string, options: any) => {
            getTimeouts.push(options?.timeout)
            return { data: { code: 0, data: {} } }
        }
        await client.uploadPhoto(photoPath)
        await client.createTextDynamic('hello')
        await client.createPhotoDynamic('hi', [{ img_src: 'x', img_width: 1, img_height: 1, img_size: 1 }])
        await client.fetchDynamicDetail('123')
    } finally {
        ;(axios as any).post = originalPost
        ;(axios as any).get = originalGet
        await rm(tempRoot, { recursive: true, force: true })
    }
    expect(postTimeouts.length).toBe(3)
    expect(getTimeouts.length).toBe(1)
    for (const timeout of [...postTimeouts, ...getTimeouts]) {
        expect(Number(timeout)).toBeGreaterThan(0)
    }
})
