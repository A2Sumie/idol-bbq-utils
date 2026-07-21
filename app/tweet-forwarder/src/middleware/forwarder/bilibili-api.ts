import axios, { type AxiosResponse } from 'axios'
import FormData from 'form-data'
import fs from 'fs'
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
}

/**
 * Classify a Bilibili provider response into success / typed error, per the centralized policy above.
 * `context` describes the operation for error messages (e.g. "photo upload", "text dynamic chunk 1/2").
 * `genericMessage`, when given, is the message thrown for an unclassified non-zero code (defaults to a
 * context-derived message). Returns the successful payload's `data.data`, or throws the typed error.
 */
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

    constructor(credentials: BiliClientCredentials) {
        this.credentials = credentials
    }

    /** Update the anonymous buvid pair once fetched, so later requests carry the WAF-required cookies. */
    setBuvid(buvid3: string, buvid4: string) {
        this.credentials.buvid3 = buvid3
        this.credentials.buvid4 = buvid4
    }

    get hasBuvid(): boolean {
        return Boolean(this.credentials.buvid3 && this.credentials.buvid4)
    }

    get headers() {
        return {
            'User-Agent': BILI_WEB_USER_AGENT,
            Referer: 'https://t.bilibili.com/',
            Origin: 'https://t.bilibili.com',
        }
    }

    get cookieHeader(): string {
        const { sessdata, bili_jct, buvid3, buvid4 } = this.credentials
        const parts = [`SESSDATA=${sessdata}`, `bili_jct=${bili_jct}`]
        if (buvid3) parts.push(`buvid3=${buvid3}`)
        if (buvid4) parts.push(`buvid4=${buvid4}`)
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

    /**
     * Upload one image to upload_bfs. Returns the raw provider payload (image_url/width/height/size).
     * `rawResponse` is the untouched axios response so the caller can log the exact body.
     */
    async uploadPhoto(path: string): Promise<{ rawResponse: any; data: unknown }> {
        const form = new FormData()
        form.append('file_up', fs.createReadStream(path))
        form.append('category', 'daily')
        form.append('csrf', this.credentials.bili_jct)
        const rawResponse = await axios.post(BILI_ENDPOINTS.uploadPhoto, form, {
            headers: {
                ...form.getHeaders(),
                ...this.headers,
                Cookie: this.cookieHeader,
            },
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
            },
        )
    }

    /** Create a draw dynamic with photos (scene 2). Returns the raw axios response. */
    async createPhotoDynamic(
        text: string,
        pics: Array<{ img_src: string; img_width: number; img_height: number; img_size: number }>,
    ): Promise<AxiosResponse> {
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
            },
        )
    }

    /** Fetch a dynamic's detail for post-send visibility validation. */
    async fetchDynamicDetail(dynamicId: string): Promise<AxiosResponse> {
        return axios.get(BILI_ENDPOINTS.dynamicDetail, {
            params: { id: dynamicId },
            headers: { ...this.headers, Cookie: this.cookieHeader },
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
