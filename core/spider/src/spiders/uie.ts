import { Platform } from '@/types'
import type { CrawlEngine, GenericArticle, TaskType, TaskTypeResult } from '@/types'
import { BaseSpider } from './base'

/**
 * uie encrypted message reader (X570 FileDrop internal service).
 *
 * Protocol (see tools/x570-filedrop/UIE-READ-API.md):
 *  - wss://drop.n2nj.moe/ws, full TLS via cloudflared
 *  - ECDH P-256 handshake -> HKDF-SHA256 (salt = clientPub || serverPub, info = "3f9a2c7e")
 *    -> AES-256-GCM session key; every business message is sealed flannel {radish: nonce, saffron: ct+tag}
 *  - auth: daikon {verbena: "stp<id><password>"} (password mode; no clock drift)
 *  - read: narcissus -> marmot {tangerine: [...]} (<=200, newest first); opossum marks read
 *
 * Gated by IDOL_BBQ_UIE_ENABLED=1 (off by default; idol-bbq is public). The credential comes
 * from the UIE_PASSWORD env (full verbena string) and must never be committed.
 */

export enum ArticleTypeEnum {
    ARTICLE = 'article',
}

export const UIE_ENABLED_FLAG = 'IDOL_BBQ_UIE_ENABLED'
export const UIE_PASSWORD_ENV = 'UIE_PASSWORD'
export const UIE_WS_URL = 'wss://drop.n2nj.moe/ws'
export const UIE_KDF_INFO = '3f9a2c7e'
export const UIE_AUTH_PREFIX = 'stp'
export const UIE_READ_TIMEOUT_MS = 20000
// Cloudflare edge rejects the WS upgrade without a browser-ish UA / matching Origin.
export const UIE_WS_HEADERS: Record<string, string> = {
    'User-Agent': 'N2NJ-Stream-Bot/1.0',
    Origin: 'https://drop.n2nj.moe',
}

export interface UieMessage {
    id: string
    ts: string
    to?: string
    anonymous?: boolean
    publicReply?: boolean
    read?: boolean
    name?: string | null
    contactType?: string | null
    contact?: string | null
    platform?: string
    body?: string
    remoteIp?: string
}

export interface UieReadOptions {
    wsUrl?: string
    verbena?: string
    timeoutMs?: number
    markRead?: boolean
    connectImpl?: (url: string) => WebSocket
}

type MinimalLog = {
    debug?: (...args: any[]) => void
    info?: (...args: any[]) => void
    warn?: (...args: any[]) => void
}

function base64urlEncode(value: Uint8Array | ArrayBuffer): string {
    const bytes = value instanceof Uint8Array ? value : new Uint8Array(value)
    return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString('base64url')
}

function delay(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

function base64urlDecode(value: string): Uint8Array<ArrayBuffer> {
    const bytes = new Uint8Array(Buffer.from(value, 'base64url'))
    return bytes as unknown as Uint8Array<ArrayBuffer>
}

function concatBytes(...parts: Array<Uint8Array>): Uint8Array<ArrayBuffer> {
    const total = parts.reduce((sum, part) => sum + part.length, 0)
    const out = new Uint8Array(total)
    let offset = 0
    for (const part of parts) {
        out.set(part, offset)
        offset += part.length
    }
    return out
}

async function importEcdhPublicKey(raw: Uint8Array<ArrayBuffer>): Promise<CryptoKey> {
    return crypto.subtle.importKey('raw', raw, { name: 'ECDH', namedCurve: 'P-256' }, false, [])
}

async function deriveUieSessionKey(
    clientPublicRaw: Uint8Array,
    serverPublicRaw: Uint8Array,
    sharedBits: ArrayBuffer,
): Promise<CryptoKey> {
    const hkdfKey = await crypto.subtle.importKey('raw', sharedBits, 'HKDF', false, ['deriveKey'])
    const salt = concatBytes(clientPublicRaw, serverPublicRaw)
    return crypto.subtle.deriveKey(
        { name: 'HKDF', hash: 'SHA-256', salt, info: new TextEncoder().encode(UIE_KDF_INFO) },
        hkdfKey,
        { name: 'AES-GCM', length: 256 },
        false,
        ['encrypt', 'decrypt'],
    )
}

async function sealUieMessage(sessionKey: CryptoKey, plain: unknown): Promise<string> {
    const nonce = crypto.getRandomValues(new Uint8Array(12))
    const ciphertext = await crypto.subtle.encrypt(
        { name: 'AES-GCM', iv: nonce },
        sessionKey,
        new TextEncoder().encode(JSON.stringify(plain)),
    )
    return JSON.stringify({
        type: 'flannel',
        radish: base64urlEncode(nonce),
        saffron: base64urlEncode(new Uint8Array(ciphertext)),
    })
}

async function unsealUieMessage<T>(sessionKey: CryptoKey, raw: string): Promise<T> {
    const sealed = JSON.parse(raw) as { type?: string; radish?: string; saffron?: string }
    if (sealed?.type !== 'flannel' || !sealed.radish || !sealed.saffron) {
        throw new Error(`UIE unexpected message: ${raw.slice(0, 120)}`)
    }
    const plaintext = await crypto.subtle.decrypt(
        { name: 'AES-GCM', iv: base64urlDecode(sealed.radish) },
        sessionKey,
        base64urlDecode(sealed.saffron),
    )
    return JSON.parse(new TextDecoder().decode(plaintext)) as T
}

/**
 * Connect, handshake, authenticate and pull messages. Returns the marmot list and marks
 * messages read (opossum) before closing, matching the recommended read-then-consume flow.
 */
export async function readUieMessages(options: UieReadOptions = {}): Promise<Array<UieMessage>> {
    const wsUrl = options.wsUrl || UIE_WS_URL
    const verbena = options.verbena || process.env[UIE_PASSWORD_ENV] || ''
    if (!verbena) {
        throw new Error('UIE reader requires UIE_PASSWORD')
    }
    const timeoutMs = Math.max(5000, Number(options.timeoutMs) || UIE_READ_TIMEOUT_MS)
    const connectImpl =
        options.connectImpl ||
        ((url: string) => new WebSocket(url, { headers: UIE_WS_HEADERS } as any))
    const ws = connectImpl(wsUrl)

    const messages: Array<UieMessage> = []
    let sessionKey: CryptoKey | null = null
    let handshakeDone = false
    let authed = false
    let listed = false
    let settleError: unknown = null
    let settled = false

    const settle = (error?: unknown) => {
        if (settled) {
            return
        }
        settled = true
        settleError = error || null
        try {
            ws.close()
        } catch {
            // ignore
        }
    }

    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => {
            settle(new Error(`UIE read timed out after ${timeoutMs}ms`))
            reject(new Error(`UIE read timed out after ${timeoutMs}ms`))
        }, timeoutMs)

        ws.onopen = () => {
            // server sends azalea on connect; nothing to do here
        }

        ws.onmessage = async (event: MessageEvent) => {
            const raw = typeof event.data === 'string' ? event.data : Buffer.from(event.data as any).toString('utf8')
            try {
                const frame = JSON.parse(raw) as Record<string, any>
                if (frame.type === 'azalea') {
                    const serverPublicRaw = base64urlDecode(String(frame.obsidian || ''))
                    const clientKeyPair = await crypto.subtle.generateKey(
                        { name: 'ECDH', namedCurve: 'P-256' },
                        true,
                        ['deriveBits'],
                    )
                    const clientPublicRaw = new Uint8Array(
                        await crypto.subtle.exportKey('raw', clientKeyPair.publicKey),
                    )
                    const serverKey = await importEcdhPublicKey(serverPublicRaw)
                    const sharedBits = await crypto.subtle.deriveBits(
                        { name: 'ECDH', public: serverKey },
                        clientKeyPair.privateKey,
                        256,
                    )
                    sessionKey = await deriveUieSessionKey(clientPublicRaw, serverPublicRaw, sharedBits)
                    ws.send(
                        JSON.stringify({
                            type: 'bromide',
                            obsidian: base64urlEncode(clientPublicRaw),
                        }),
                    )
                    return
                }
                if (frame.type === 'cranberry') {
                    handshakeDone = true
                    ws.send(
                        await sealUieMessage(sessionKey!, {
                            type: 'daikon',
                            verbena,
                        }),
                    )
                    return
                }
                if (frame.type === 'flannel') {
                    const plain = await unsealUieMessage<Record<string, any>>(sessionKey!, raw)
                    if (plain.type === 'lattice') {
                        settle(new Error(`UIE auth rejected: ${String(plain.albacore || 'Bad code')}`))
                        reject(new Error(`UIE auth rejected: ${String(plain.albacore || 'Bad code')}`))
                        return
                    }
                    if (plain.type === 'egret') {
                        authed = true
                        ws.send(await sealUieMessage(sessionKey!, { type: 'narcissus' }))
                        return
                    }
                    if (plain.type === 'marmot') {
                        listed = true
                        const items = Array.isArray(plain.tangerine) ? plain.tangerine : []
                        for (const item of items) {
                            if (item && typeof item === 'object' && typeof (item as any).id === 'string') {
                                messages.push(item as UieMessage)
                            }
                        }
                        if (options.markRead !== false) {
                            ws.send(await sealUieMessage(sessionKey!, { type: 'opossum' }))
                            // Let the server observe opossum before we close: wait for the server
                            // to close the socket (protocol behavior) or a short grace period.
                            await delay(500)
                        }
                        settle()
                        resolve()
                        return
                    }
                    return
                }
            } catch (error) {
                settle(error)
                reject(error)
            }
        }

        ws.onerror = (error) => {
            clearTimeout(timer)
            const message = error instanceof Error ? error.message : String(error)
            settle(new Error(`UIE websocket error: ${message}`))
            reject(new Error(`UIE websocket error: ${message}`))
        }

        ws.onclose = () => {
            clearTimeout(timer)
            if (!settled && !listed) {
                const error = new Error('UIE connection closed before marmot')
                settle(error)
                reject(error)
            }
        }
    })

    if (settleError) {
        throw settleError
    }
    return messages
}

export function buildUieArticle(message: UieMessage): GenericArticle<Platform.Website> {
    const rawBody = String(message.body || '').trim()
    const title = message.anonymous || !message.name ? '匿名留言' : String(message.name).trim()
    const createdAt = Date.parse(message.ts || '')
    const validCreatedAt = Number.isFinite(createdAt) && createdAt > 0 ? Math.floor(createdAt / 1000) : 0
    const crawledAt = Math.floor(Date.now() / 1000)
    const contactParts = [message.contactType, message.contact, message.platform]
        .map((part) => String(part || '').trim())
        .filter(Boolean)
    const content = [title ? `【${title}】` : '', rawBody, contactParts.length > 0 ? `联系方式: ${contactParts.join(' / ')}` : '']
        .filter(Boolean)
        .join('\n\n')

    return {
        platform: Platform.Website,
        a_id: String(message.id),
        u_id: 'uie:message',
        username: title,
        created_at: validCreatedAt || crawledAt,
        content: content || title || null,
        url: `https://drop.n2nj.moe/` ,
        type: ArticleTypeEnum.ARTICLE,
        ref: null,
        has_media: false,
        media: null,
        extra: {
            data: {
                site: 'UIE',
                host: 'drop.n2nj.moe',
                feed: 'uie',
                title,
                category: 'message',
                summary: null,
                raw_html: '',
                time_source: validCreatedAt ? 'explicit' : 'crawl_observed',
                date_text: message.ts || null,
                crawled_at: crawledAt,
                to: message.to || 'uie',
                anonymous: message.anonymous === true,
                public_reply: message.publicReply === true,
                read: message.read === true,
                contact_type: message.contactType || null,
                contact: message.contact || null,
                platform: message.platform || null,
            },
            content: title || undefined,
            media: undefined,
            extra_type: 'website_meta',
        },
        u_avatar: null,
    }
}

class UieSpider extends BaseSpider {
    static _VALID_URL = /^uie:\/\/read$/i
    static _PLATFORM = Platform.Website
    BASE_URL = 'uie://read'
    NAME = 'UIE Message Spider'

    static isEnabled(): boolean {
        return process.env[UIE_ENABLED_FLAG] === '1'
    }

    static extractBasicInfo(url: string) {
        if (!UieSpider._VALID_URL.test(url)) {
            return undefined
        }
        return {
            u_id: 'uie:message',
            platform: Platform.Website,
        }
    }

    async _crawl<T extends TaskType>(
        url: string,
        _page: unknown,
        config: {
            task_type: T
            crawl_engine: CrawlEngine
            sub_task_type?: Array<string>
            cookieString?: string
        },
    ): Promise<TaskTypeResult<T, Platform.Website>> {
        if (config.task_type !== 'article') {
            throw new Error('UIE spider only supports article tasks')
        }
        if (!UieSpider.isEnabled()) {
            throw new Error(
                `UIE reader disabled: set ${UIE_ENABLED_FLAG}=1 and ${UIE_PASSWORD_ENV} to enable`,
            )
        }
        const messages = await readUieMessages()
        this.log?.info?.(`UIE read ${messages.length} message(s)`)
        return messages.map(buildUieArticle) as TaskTypeResult<T, Platform.Website>
    }
}

export { UieSpider }
