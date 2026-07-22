import axios, { type AxiosResponse } from 'axios'
import FormData from 'form-data'
import fs from 'fs'
import path from 'path'
import crypto from 'crypto'
import { NonRetryableForwarderSendError } from './base'

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
} as const

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
}

type BiliCookieDocument = {
    cookie_info?: {
        cookies?: Array<{
            name?: unknown
            value?: unknown
        }>
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

    constructor(credentials: BiliClientCredentials) {
        this.credentials = credentials
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

    /** Create a text-only dynamic (scene 1). Returns the raw axios response for the caller to inspect. */
    async createTextDynamic(text: string): Promise<AxiosResponse> {
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

    /** Create a draw dynamic with photos (scene 2). Returns the raw axios response. */
    async createPhotoDynamic(
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

    /** Fetch a dynamic's detail for post-send visibility validation. */
    async fetchDynamicDetail(dynamicId: string): Promise<AxiosResponse> {
        return axios.get(BILI_ENDPOINTS.dynamicDetail, {
            params: { id: dynamicId },
            headers: { ...this.headers, Cookie: this.cookieHeader },
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
    type BiliClientCredentials,
    type BiliProviderResponse,
}
