import axios, { type AxiosResponse } from 'axios'
import FormData from 'form-data'
import fs from 'fs'
import path from 'path'
import crypto from 'crypto'
import { NonRetryableForwarderSendError } from './base'
import { BILI_APP_USER_AGENT, signAppQuery } from './bilibili-app-sign'

/**
 * Bilibili API client for the dynamic (动态) + photo-upload surface.
 *
 * This module is the single authoritative place for the Bilibili transport concerns that used to be
 * duplicated inline across the forwarder: endpoint URLs, the web UA/Referer/Origin headers, the
 * SESSDATA/bili_jct/buvid cookie header, and — most importantly — the provider response-code policy.
 *
 * Provider response-code policy (the former scattered "mitigation measures", now centralized):
 *   code === 0    -> success
 *   code === -101 -> account not logged in / CSRF identity failure. Not retryable: retrying with the
 *                    same credentials cannot recover. Surfaced as NonRetryableForwarderSendError.
 *   code === -111 -> per-account upload velocity control (WAF, csrf-flavoured). Transient: the same
 *                    credentials succeed again seconds later, so it is retryable with backoff. Surfaced
 *                    as BiliUploadVelocityError (a NonRetryableForwarderSendError subclass so the
 *                    whole-send layer does not re-upload; the per-photo retry loop opts back in).
 *   any other     -> unclassified provider failure, retryable by default (transient risk/5xx/etc.).
 */

const BILI_ENDPOINTS = {
    finger: 'https://api.bilibili.com/x/frontend/finger/spi',
    uploadPhoto: 'https://api.bilibili.com/x/dynamic/feed/draw/upload_bfs',
    createDynamic: 'https://api.bilibili.com/x/dynamic/feed/create/dyn',
    dynamicDetail: 'https://api.bilibili.com/x/polymer/web-dynamic/v1/detail',
    // App-protocol (route B, `dynamic_api: 'app'`) creation endpoints on api.vc.bilibili.com.
    // The RE case's endpoint matrix pointed at `dynamic_repost/v1/dynamic_repost/reply`
    // (shareToTimeline, comment-share flow — rejects standalone text with 4101001) and
    // `dynamic_svr/v1/dynamic_svr/create_act_draw` (HTTP 404, dead); these two are the alive
    // vc.bilibili.com equivalents documented as 发表文字动态 / 发表相簿动态 and live-proven on
    // 2026-10-02 (both created dynamics with code 0 through signed app-shaped requests).
    appCreateTextDynamic: 'https://api.vc.bilibili.com/dynamic_svr/v1/dynamic_svr/create',
    appCreateDrawDynamic: 'https://api.vc.bilibili.com/dynamic_svr/v1/dynamic_svr/create_draw',
} as const

/** Posting route switch (`dynamic_api` config). Default 'web' keeps the historical behavior. */
type BiliDynamicApiRoute = 'web' | 'app'

const BILI_WEB_USER_AGENT =
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

// Every Bilibili call must be bounded: axios has no default timeout, so a half-open/dead socket
// (observed on upload_bfs) otherwise wedges the whole send forever and leaves the outbound stuck in
// `sending`. A finite timeout lets the per-photo/whole-send retry loops recover instead.
const BILI_REQUEST_TIMEOUT_MS = 30_000

/** Bilibili provider codes with dedicated handling, named so call sites read as policy not magic numbers. */
const BILI_CODE = {
    ok: 0,
    authFailure: -101,
    velocityControl: -111,
} as const

/**
 * upload_bfs answers -111 when the account trips Bilibili's per-account upload velocity control; the
 * same credentials succeed again seconds later. It extends NonRetryableForwarderSendError so the
 * whole-send pRetry in base.sendPrepared never re-runs realSend (which would re-upload every already
 * uploaded photo and drive the throttle harder); the per-photo retry loop explicitly opts back in.
 */
class BiliUploadVelocityError extends NonRetryableForwarderSendError {
    constructor(message: string) {
        super(message)
        this.name = 'BiliUploadThrottledError'
    }
}

interface BiliProviderResponse {
    data?: {
        code?: number
        message?: string
        data?: unknown
    }
}

interface BiliClientCredentials {
    bili_jct: string
    sessdata: string
    buvid3?: string
    buvid4?: string
    cookies?: Record<string, string>
    /** App OAuth token (token_info.access_token in the biliup cookie export). Optional: the app
     *  route also works with SESSDATA cookie auth + csrf_token, which is what the export provides
     *  when the token_info block is empty. */
    access_key?: string
}

type BiliCookieDocument = {
    cookie_info?: {
        cookies?: Array<{
            name?: unknown
            value?: unknown
        }>
    }
    token_info?: {
        access_token?: unknown
    }
}

/**
 * Classify a Bilibili provider response into success / typed error, per the centralized policy above.
 * `context` describes the operation for error messages (e.g. "photo upload", "text dynamic chunk 1/2").
 * `genericMessage`, when given, is the message thrown for an unclassified non-zero code (defaults to a
 * context-derived message). Returns the successful payload's `data.data`, or throws the typed error.
 */
function readBiliCookieDocument(cookieFile?: string): Record<string, string> {
    if (!cookieFile) {
        return {}
    }
    try {
        const document = JSON.parse(fs.readFileSync(cookieFile, 'utf8')) as BiliCookieDocument
        const cookies = document.cookie_info?.cookies || []
        return Object.fromEntries(
            cookies
                .map((cookie) => [String(cookie.name || '').trim(), String(cookie.value || '').trim()] as const)
                .filter(([name, value]) => Boolean(name && value)),
        )
    } catch {
        return {}
    }
}

/**
 * App OAuth token (`token_info.access_token`) from a biliup-style cookie export. Returns '' when
 * the export carries no token (browser exports usually have an empty token_info block). Never logs.
 */
function readBiliCookieAccessToken(cookieFile?: string): string {
    if (!cookieFile) {
        return ''
    }
    try {
        const document = JSON.parse(fs.readFileSync(cookieFile, 'utf8')) as BiliCookieDocument
        return String(document.token_info?.access_token || '').trim()
    } catch {
        return ''
    }
}

function randomUpperHex(length: number) {
    return crypto.randomBytes(Math.ceil(length / 2)).toString('hex').toUpperCase().slice(0, length)
}

function buildBiliLsid(now = Date.now()) {
    return `${randomUpperHex(8)}_${Math.floor(now / 1000).toString(16).toUpperCase()}`
}

function buildBiliUuid(now = Date.now()) {
    return `${randomUpperHex(8)}-${randomUpperHex(4)}-${randomUpperHex(4)}-${randomUpperHex(4)}-${randomUpperHex(12)}${Math.floor(now / 1000)}`
}

function resolveImageContentType(filePath: string) {
    const extension = path.extname(filePath).toLowerCase()
    if (extension === '.png') return 'image/png'
    if (extension === '.webp') return 'image/webp'
    if (extension === '.gif') return 'image/gif'
    return 'image/jpeg'
}

function assertBiliResponseOk(res: BiliProviderResponse, context: string, genericMessage?: string): unknown {
    const code = Number(res.data?.code)
    if (code === BILI_CODE.ok) {
        return res.data?.data
    }
    const message = res.data?.message
    if (code === BILI_CODE.authFailure) {
        throw new NonRetryableForwarderSendError(
            `Bilibili ${context} rejected by provider (${code}): ${message || 'authentication failure'}`,
        )
    }
    if (code === BILI_CODE.velocityControl) {
        throw new BiliUploadVelocityError(
            `Bilibili ${context} throttled by provider (${code}): ${message || 'velocity control'}`,
        )
    }
    throw new Error(genericMessage || `Bilibili ${context} failed. ${message}: ${JSON.stringify(res.data)}`)
}

class BilibiliApiClient {
    private credentials: BiliClientCredentials
    private cookieJar: Map<string, string>
    private volatileCookieIssuedAt = 0
    private dynamicApi: BiliDynamicApiRoute

    constructor(credentials: BiliClientCredentials, options?: { dynamicApi?: BiliDynamicApiRoute }) {
        this.credentials = credentials
        this.dynamicApi = options?.dynamicApi === 'app' ? 'app' : 'web'
        this.cookieJar = new Map(Object.entries(credentials.cookies || {}))
        this.setCookie('SESSDATA', credentials.sessdata)
        this.setCookie('bili_jct', credentials.bili_jct)
        if (credentials.buvid3) this.setCookie('buvid3', credentials.buvid3)
        if (credentials.buvid4) this.setCookie('buvid4', credentials.buvid4)
        this.ensureStaticWafCookies()
    }

    static readCookieDocument(cookieFile?: string) {
        return readBiliCookieDocument(cookieFile)
    }

    static readAccessToken(cookieFile?: string) {
        return readBiliCookieAccessToken(cookieFile)
    }

    /** Active posting route, per the `dynamic_api` config switch (default 'web'). */
    get postingRoute(): BiliDynamicApiRoute {
        return this.dynamicApi
    }

    private setCookie(name: string, value?: string) {
        const normalized = String(value || '').trim()
        if (normalized) {
            this.cookieJar.set(name, normalized)
        }
    }

    /** Update the anonymous buvid pair once fetched, so later requests carry the WAF-required cookies. */
    setBuvid(buvid3: string, buvid4: string) {
        this.credentials.buvid3 = buvid3
        this.credentials.buvid4 = buvid4
        this.setCookie('buvid3', buvid3)
        this.setCookie('buvid4', buvid4)
        this.ensureStaticWafCookies()
    }

    get hasBuvid(): boolean {
        return Boolean(this.cookieJar.get('buvid3') && this.cookieJar.get('buvid4'))
    }

    private ensureStaticWafCookies() {
        if (!this.cookieJar.get('b_nut')) {
            this.cookieJar.set('b_nut', String(Math.floor(Date.now() / 1000)))
        }
        if (!this.cookieJar.get('_uuid')) {
            this.cookieJar.set('_uuid', buildBiliUuid())
        }
        if (!this.cookieJar.get('CURRENT_FNVAL')) {
            this.cookieJar.set('CURRENT_FNVAL', '4048')
        }
    }

    refreshVolatileWafCookies(force = false) {
        const now = Date.now()
        if (!force && this.volatileCookieIssuedAt > 0 && now - this.volatileCookieIssuedAt < 30_000) {
            return
        }
        this.cookieJar.set('b_lsid', buildBiliLsid(now))
        this.volatileCookieIssuedAt = now
    }

    get headers() {
        return {
            'User-Agent': BILI_WEB_USER_AGENT,
            Referer: 'https://t.bilibili.com/',
            Origin: 'https://t.bilibili.com',
        }
    }

    get cookieHeader(): string {
        this.ensureStaticWafCookies()
        this.refreshVolatileWafCookies()
        const preferredOrder = [
            'SESSDATA',
            'bili_jct',
            'DedeUserID',
            'DedeUserID__ckMd5',
            'sid',
            'buvid3',
            'buvid4',
            'b_nut',
            '_uuid',
            'CURRENT_FNVAL',
            'b_lsid',
        ]
        const emitted = new Set<string>()
        const parts: string[] = []
        for (const name of preferredOrder) {
            const value = this.cookieJar.get(name)
            if (value) {
                parts.push(`${name}=${value}`)
                emitted.add(name)
            }
        }
        for (const [name, value] of this.cookieJar) {
            if (!emitted.has(name) && value) {
                parts.push(`${name}=${value}`)
            }
        }
        return parts.join('; ')
    }

    /**
     * Cookie header that deliberately excludes account auth cookies (SESSDATA/bili_jct/DedeUserID/sid).
     * Used for post-send visibility checks: Bilibili's authenticated detail API still returns a draw
     * dynamic to its author even when the provider has hidden it from the public (content audit/review,
     * observed as public code 4101152 动态不可见). Querying the same endpoint without auth cookies
     * reflects what an actual viewer sees, so a hidden chunk is no longer silently marked as sent.
     */
    get publicCookieHeader(): string {
        this.ensureStaticWafCookies()
        this.refreshVolatileWafCookies()
        const authCookies = new Set(['SESSDATA', 'bili_jct', 'DedeUserID', 'DedeUserID__ckMd5', 'sid'])
        const parts: string[] = []
        for (const [name, value] of this.cookieJar) {
            if (authCookies.has(name) || !value) {
                continue
            }
            parts.push(`${name}=${value}`)
        }
        return parts.join('; ')
    }

    /** Fetch an anonymous buvid3/buvid4 pair from the SPI endpoint (no auth cookies required). */
    async fetchAnonymousBuvid(): Promise<{ buvid3: string; buvid4: string } | null> {
        const res = await axios.get(BILI_ENDPOINTS.finger, {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            timeout: 10000,
        })
        const buvid3 = String(res.data?.data?.b_3 || '')
        const buvid4 = String(res.data?.data?.b_4 || '')
        return buvid3 && buvid4 ? { buvid3, buvid4 } : null
    }

    private getFormLength(form: FormData): Promise<number | null> {
        return new Promise((resolve) => {
            form.getLength((error, length) => {
                resolve(error ? null : length)
            })
        })
    }

    /**
     * Upload one image to upload_bfs. Returns the raw provider payload (image_url/width/height/size).
     * `rawResponse` is the untouched axios response so the caller can log the exact body.
     */
    async uploadPhoto(path: string): Promise<{ rawResponse: any; data: unknown }> {
        this.refreshVolatileWafCookies(true)
        const form = new FormData()
        const fileBuffer = fs.readFileSync(path)
        form.append('file_up', fileBuffer, {
            filename: path.split(/[\\/]/).pop() || 'image.jpg',
            contentType: resolveImageContentType(path),
            knownLength: fileBuffer.length,
        })
        form.append('category', 'daily')
        form.append('csrf', this.credentials.bili_jct)
        const contentLength = await this.getFormLength(form)
        const rawResponse = await axios.post(BILI_ENDPOINTS.uploadPhoto, form, {
            headers: {
                ...form.getHeaders(),
                ...(contentLength ? { 'Content-Length': contentLength } : {}),
                ...this.headers,
                Cookie: this.cookieHeader,
            },
            timeout: BILI_REQUEST_TIMEOUT_MS,
        })
        const data = assertBiliResponseOk(
            rawResponse,
            'photo upload',
            `Upload photo to bilibili failed. ${rawResponse.data?.message}: ${JSON.stringify(rawResponse.data)}`,
        )
        return { rawResponse, data }
    }

    /**
     * Create a text-only dynamic (scene 1 on the web face). Dispatches per the `dynamic_api`
     * switch: 'web' (default) posts to x/dynamic/feed/create/dyn, 'app' posts through the
     * app-protocol vc endpoint. Returns the raw axios response for the caller to inspect.
     */
    async createTextDynamic(text: string): Promise<AxiosResponse> {
        return this.dynamicApi === 'app' ? this.createTextDynamicApp(text) : this.createTextDynamicWeb(text)
    }

    /** Create a draw dynamic with photos (scene 2). Dispatches per the `dynamic_api` switch. */
    async createPhotoDynamic(
        text: string,
        pics: Array<{ img_src: string; img_width: number; img_height: number; img_size: number }>,
    ): Promise<AxiosResponse> {
        return this.dynamicApi === 'app'
            ? this.createPhotoDynamicApp(text, pics)
            : this.createPhotoDynamicWeb(text, pics)
    }

    /** Web face (route A): JSON `dyn_req` on x/dynamic/feed/create/dyn. */
    async createTextDynamicWeb(text: string): Promise<AxiosResponse> {
        this.refreshVolatileWafCookies(true)
        return axios.post(
            BILI_ENDPOINTS.createDynamic,
            {
                dyn_req: {
                    content: { contents: [{ raw_text: text, type: 1, biz_id: '' }] },
                    scene: 1,
                },
            },
            {
                headers: { 'Content-Type': 'application/json', ...this.headers, Cookie: this.cookieHeader },
                params: { csrf: this.credentials.bili_jct },
                timeout: BILI_REQUEST_TIMEOUT_MS,
            },
        )
    }

    /** Web face (route A): JSON `dyn_req` with pics on x/dynamic/feed/create/dyn. */
    async createPhotoDynamicWeb(
        text: string,
        pics: Array<{ img_src: string; img_width: number; img_height: number; img_size: number }>,
    ): Promise<AxiosResponse> {
        this.refreshVolatileWafCookies(true)
        return axios.post(
            BILI_ENDPOINTS.createDynamic,
            {
                dyn_req: {
                    content: { contents: [{ raw_text: text, type: 1, biz_id: '' }] },
                    pics,
                    scene: 2,
                },
            },
            {
                headers: { 'Content-Type': 'application/json', ...this.headers, Cookie: this.cookieHeader },
                params: { csrf: this.credentials.bili_jct },
                timeout: BILI_REQUEST_TIMEOUT_MS,
            },
        )
    }

    /** Headers shaped like the app client for the vc.bilibili.com family (route B). */
    private get appHeaders() {
        return {
            'User-Agent': BILI_APP_USER_AGENT,
            Referer: 'https://www.bilibili.com/',
        }
    }

    /**
     * Signed common query for route B (appkey/build/mobi_app/platform/channel/app_ver/ts +
     * sign=MD5(query+appsec); pairing recorded in bilibili-app-sign.ts). `access_key` joins the
     * query when the credential carries one (DefaultRequestInterceptor behavior).
     */
    private appSignedQuery(): string {
        return signAppQuery(this.credentials.access_key ? { access_key: this.credentials.access_key } : {})
    }

    private appForm(extra: Record<string, string>): string {
        const form = new URLSearchParams(extra)
        if (this.credentials.access_key) {
            form.set('access_key', this.credentials.access_key)
        }
        form.set('csrf', this.credentials.bili_jct)
        form.set('csrf_token', this.credentials.bili_jct)
        return form.toString()
    }

    private postAppForm(url: string, form: string): Promise<AxiosResponse> {
        this.refreshVolatileWafCookies(true)
        return axios.post(`${url}?${this.appSignedQuery()}`, form, {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                ...this.appHeaders,
                Cookie: this.cookieHeader,
            },
            timeout: BILI_REQUEST_TIMEOUT_MS,
        })
    }

    /**
     * App face (route B): FormUrlEncoded 发表文字动态 on api.vc.bilibili.com
     * (`dynamic_svr/v1/dynamic_svr/create`, type=4), app-shape signed query + app UA.
     */
    async createTextDynamicApp(text: string): Promise<AxiosResponse> {
        return this.postAppForm(
            BILI_ENDPOINTS.appCreateTextDynamic,
            this.appForm({
                type: '4',
                rid: '0',
                content: text,
                extension: '{"emoji_type":1}',
                at_uids: '',
                ctrl: '[]',
            }),
        )
    }

    /**
     * App face (route B): FormUrlEncoded 发表相簿动态 on api.vc.bilibili.com
     * (`dynamic_svr/v1/dynamic_svr/create_draw`, biz=3/category=3/type=0).
     */
    async createPhotoDynamicApp(
        text: string,
        pics: Array<{ img_src: string; img_width: number; img_height: number; img_size: number }>,
    ): Promise<AxiosResponse> {
        return this.postAppForm(
            BILI_ENDPOINTS.appCreateDrawDynamic,
            this.appForm({
                biz: '3',
                category: '3',
                type: '0',
                pictures: JSON.stringify(pics),
                title: '',
                tags: '',
                description: text,
                content: text,
                setting: '{"copy_forbidden":0,"cachedTime":0}',
                from: 'create.dynamic.android',
                extension: '{"emoji_type":1}',
                at_uids: '',
                at_control: '[]',
            }),
        )
    }

    /** Fetch a dynamic's detail for post-send visibility validation. */
    async fetchDynamicDetail(dynamicId: string): Promise<AxiosResponse> {
        return axios.get(BILI_ENDPOINTS.dynamicDetail, {
            params: { id: dynamicId },
            headers: { ...this.headers, Cookie: this.cookieHeader },
            timeout: BILI_REQUEST_TIMEOUT_MS,
        })
    }

    /**
     * Fetch a dynamic's detail as an anonymous viewer sees it (no auth cookies). Bilibili's
     * authenticated detail returns the author's own hidden/audit dynamics with code 0, so this
     * anonymous variant is the source of truth for whether a posted photo chunk is actually public.
     */
    async fetchPublicDynamicDetail(dynamicId: string): Promise<AxiosResponse> {
        return axios.get(BILI_ENDPOINTS.dynamicDetail, {
            params: { id: dynamicId },
            headers: { ...this.headers, Cookie: this.publicCookieHeader },
            timeout: BILI_REQUEST_TIMEOUT_MS,
        })
    }
}

export {
    BILI_CODE,
    BILI_ENDPOINTS,
    BilibiliApiClient,
    BiliUploadVelocityError,
    assertBiliResponseOk,
    readBiliCookieAccessToken,
    type BiliClientCredentials,
    type BiliDynamicApiRoute,
    type BiliProviderResponse,
}
