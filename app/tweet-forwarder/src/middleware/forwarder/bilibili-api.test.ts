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

type CapturedPost = { url: string; body: any; options: any }

async function capturePosts(run: (client: BilibiliApiClient) => Promise<unknown>): Promise<CapturedPost[]> {
    const captured: CapturedPost[] = []
    const originalPost = axios.post
    try {
        ;(axios as any).post = async (url: string, body: any, options: any) => {
            captured.push({ url, body, options })
            return { data: { code: 0, data: { dynamic_id_str: '777' } } }
        }
        await run(new BilibiliApiClient({ bili_jct: 'jct-value', sessdata: 'sess-value' }, { dynamicApi: 'app' }))
    } finally {
        ;(axios as any).post = originalPost
    }
    return captured
}

test('dynamic_api defaults to the web face: create/dyn JSON body, unchanged behavior', async () => {
    const captured: CapturedPost[] = []
    const originalPost = axios.post
    try {
        ;(axios as any).post = async (url: string, body: any, options: any) => {
            captured.push({ url, body, options })
            return { data: { code: 0, data: { dyn_id_str: '1' } } }
        }
        const client = new BilibiliApiClient({ bili_jct: 'jct-value', sessdata: 'sess-value' })
        expect(client.postingRoute).toBe('web')
        await client.createTextDynamic('hello')
    } finally {
        ;(axios as any).post = originalPost
    }
    expect(captured.length).toBe(1)
    expect(captured[0]!.url).toBe('https://api.bilibili.com/x/dynamic/feed/create/dyn')
    expect(JSON.stringify(captured[0]!.body)).toContain('"dyn_req"')
    expect(captured[0]!.options.params.csrf).toBe('jct-value')
})

test('app route posts text through the signed vc create endpoint (发表文字动态)', async () => {
    const captured = await capturePosts((client) => client.createTextDynamic('测试正文 [RT-B]'))
    expect(captured.length).toBe(1)
    const { url, body, options } = captured[0]!
    expect(url.startsWith('https://api.vc.bilibili.com/dynamic_svr/v1/dynamic_svr/create?')).toBe(true)
    expect(url).toContain('appkey=1d8b6e7d45233436')
    expect(url).toContain('mobi_app=android')
    expect(url).toMatch(/&sign=[0-9a-f]{32}$/)
    const form = String(body)
    expect(form).toContain('type=4')
    expect(form).toContain('rid=0')
    expect(decodeURIComponent(form.replace(/\+/g, ' '))).toContain('content=测试正文 [RT-B]')
    expect(form).toContain('ctrl=%5B%5D')
    expect(form).toContain('csrf_token=jct-value')
    expect(String(options.headers['User-Agent'])).toContain('BiliDroid')
    expect(String(options.headers.Cookie)).toContain('SESSDATA=sess-value')
})

test('app route posts draw dynamics through create_draw (发表相簿动态) with pictures JSON', async () => {
    const captured = await capturePosts((client) =>
        client.createPhotoDynamic('图文正文', [{ img_src: 'https://i0.hdslb.com/x.png', img_width: 240, img_height: 240, img_size: 8.2 }]),
    )
    expect(captured.length).toBe(1)
    const { url, body } = captured[0]!
    expect(url.startsWith('https://api.vc.bilibili.com/dynamic_svr/v1/dynamic_svr/create_draw?')).toBe(true)
    const form = String(body)
    expect(form).toContain('biz=3')
    expect(form).toContain('category=3')
    expect(form).toContain('type=0')
    expect(form).toContain('from=create.dynamic.android')
    expect(form).toContain('setting=')
    expect(decodeURIComponent(form)).toContain('"img_src":"https://i0.hdslb.com/x.png"')
})

test('access_key from the cookie export joins both the signed query and the form', async () => {
    const captured: CapturedPost[] = []
    const originalPost = axios.post
    try {
        ;(axios as any).post = async (url: string, body: any, options: any) => {
            captured.push({ url, body, options })
            return { data: { code: 0, data: { dynamic_id_str: '1' } } }
        }
        const client = new BilibiliApiClient(
            { bili_jct: 'jct-value', sessdata: 'sess-value', access_key: 'ak-token' },
            { dynamicApi: 'app' },
        )
        await client.createTextDynamic('x')
    } finally {
        ;(axios as any).post = originalPost
    }
    expect(captured[0]!.url).toContain('access_key=ak-token')
    expect(String(captured[0]!.body)).toContain('access_key=ak-token')
})
