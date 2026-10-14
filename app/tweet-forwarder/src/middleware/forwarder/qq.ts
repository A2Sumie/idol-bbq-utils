import axios from 'axios'
import { chunk } from 'lodash'
import { Forwarder, NonRetryableForwarderSendError, PartialForwarderSendError, type MediaFile, type SendProps } from './base'
import { classifyOneBotResponse, classifyOneBotTransportError } from './onebot-semantics'
import { type ForwardTargetPlatformConfig, ForwardTargetPlatformEnum } from '@/types/forwarder'
import {
    normalizeForwarderImageAttachments,
    resolveForwarderImageMaxBytes,
} from '@/services/forwarder-image-attachment-service'
import {
    buildQQMediaFileValue,
    resolveQQMediaDeliveryConfig,
    type QQMediaDeliveryConfigLike,
} from '@/services/snowluma-media-source'

type OneBotTextSegment = {
    type: 'text'
    data: {
        text: string
    }
}

type OneBotImageSegment = {
    type: 'image'
    data: {
        file: string
    }
}

type OneBotVideoSegment = {
    type: 'video'
    data: {
        file: string
    }
}

type OneBotMessageSegment = OneBotTextSegment | OneBotImageSegment | OneBotVideoSegment

const QQ_REQUEST_TIMEOUT_MS = 15_000

/**
 * In `url` media delivery the bridge downloads the media after the send is
 * accepted, so compressed attachment copies must outlive the send call for a
 * bit before `normalizedAttachments.cleanup()` frees them.
 */
const QQ_MEDIA_URL_CLEANUP_GRACE_MS = 5 * 60 * 1000

type MergedForwardRuntimeConfig = {
    enabled: boolean
    nodeName: string
    nodeUin: string
    maxSegmentsPerNode: number
}

class QQForwarder extends Forwarder {
    static _PLATFORM = ForwardTargetPlatformEnum.QQ
    private group_id?: string
    private user_id?: string
    private url: string
    private token: string
    private mediaDelivery: ReturnType<typeof resolveQQMediaDeliveryConfig>
    NAME = 'qq'
    protected override BASIC_TEXT_LIMIT = 4000

    constructor(...[config, ...rest]: [...ConstructorParameters<typeof Forwarder>]) {
        super(config, ...rest)
        this.minInterval = 1000 // 1s
        const { group_id, user_id, url, token } = config as ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.QQ>
        if (!url) {
            throw new Error(`forwarder ${this.NAME} url is required`)
        }
        if (Boolean(group_id) === Boolean(user_id)) {
            throw new Error(`forwarder ${this.NAME} requires exactly one of group_id or user_id`)
        }
        this.group_id = group_id ? String(group_id) : undefined
        this.user_id = user_id ? String(user_id) : undefined
        this.url = url
        this.token = token
        this.mediaDelivery = resolveQQMediaDeliveryConfig(config as QQMediaDeliveryConfigLike)
    }

    private isPrivateChat() {
        return Boolean(this.user_id)
    }

    private normalizeMergedForwardConfig(props?: SendProps): MergedForwardRuntimeConfig {
        const effectiveConfig = this.getEffectiveConfig(
            props?.runtime_config,
        ) as ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.QQ> & {
            send_mode?: string
            merged_forward?:
                | boolean
                | {
                      enabled?: boolean
                      node_name?: string
                      node_uin?: string | number
                      max_segments_per_node?: number
                  }
        }
        const raw = effectiveConfig.merged_forward
        const sendMode = String(effectiveConfig.send_mode || '').trim()
        const objectConfig = raw && typeof raw === 'object' ? raw : {}
        const rawMaxSegments = Number(objectConfig.max_segments_per_node)
        const maxSegmentsPerNode = Number.isFinite(rawMaxSegments)
            ? Math.max(1, Math.min(Math.floor(rawMaxSegments), 30))
            : 12
        const enabled =
            sendMode !== 'normal' &&
            (sendMode === 'merged_forward' || raw === true || (typeof raw === 'object' && raw.enabled !== false))

        return {
            enabled,
            nodeName: String(objectConfig.node_name || 'idol-bbq').trim() || 'idol-bbq',
            nodeUin: String(objectConfig.node_uin || '10000').trim() || '10000',
            maxSegmentsPerNode,
        }
    }

    /**
     * Unified OneBot response judgment (NapCat + SnowLuma): see
     * onebot-semantics.ts for the full retcode/wording table. Success is
     * `status=="ok" && retcode==0`; NapCat EventChecker rejections
     * (`retcode:200`) and SnowLuma `1400/1401/1404/1413` + risk-control wording
     * become NonRetryableForwarderSendError so pRetry never re-spams them.
     */
    private assertOneBotResponseOk(res: { data?: any; statusText?: string }, context: string) {
        const verdict = classifyOneBotResponse(res, context)
        if (verdict.ok) {
            return verdict
        }
        if (!verdict.retryable) {
            throw new NonRetryableForwarderSendError(verdict.detail)
        }
        throw new Error(verdict.detail)
    }

    /**
     * Transport-layer failures (timeout / reset / 5xx) stay retryable; 4xx
     * (bad endpoint, bad token, oversized body) are promoted to
     * NonRetryableForwarderSendError so they cannot start a retry storm.
     */
    private rethrowClassifiedTransportError(error: unknown): never {
        const transport = classifyOneBotTransportError(error)
        if (!transport.retryable) {
            throw new NonRetryableForwarderSendError(transport.detail)
        }
        throw error
    }

    private async resolveMediaFileValue(
        item: MediaFile,
        cache: Map<string, string>,
    ): Promise<{ value: string; usedUrl: boolean }> {
        const cached = cache.get(item.path)
        if (cached !== undefined) {
            return { value: cached, usedUrl: cached.startsWith('http://') || cached.startsWith('https://') }
        }
        const built = await buildQQMediaFileValue(
            {
                path: item.path,
                url: (item as MediaFile & { url?: string }).url,
            },
            {
                config: this.mediaDelivery,
                endpoint: this.url,
                token: this.token,
                log: this.log,
            },
        )
        cache.set(item.path, built.value)
        if (built.method !== 'file' && built.method !== 'url') {
            this.log?.debug(`QQ media source for ${item.path}: ${built.method} (${built.reason})`)
        }
        return { value: built.value, usedUrl: built.method === 'url' }
    }

    private async buildImageSegments(
        media: NonNullable<SendProps['media']>,
        cache: Map<string, string>,
    ): Promise<OneBotImageSegment[]> {
        const segments: OneBotImageSegment[] = []
        for (const item of media.filter((i) => i.media_type === 'photo')) {
            const { value } = await this.resolveMediaFileValue(item, cache)
            segments.push({ type: 'image', data: { file: value } })
        }
        return segments
    }

    private async buildVideoSegments(
        media: NonNullable<SendProps['media']>,
        cache: Map<string, string>,
    ): Promise<OneBotVideoSegment[]> {
        const segments: OneBotVideoSegment[] = []
        for (const item of media.filter((i) => i.media_type === 'video')) {
            const { value } = await this.resolveMediaFileValue(item, cache)
            segments.push({ type: 'video', data: { file: value } })
        }
        return segments
    }

    private async buildMediaSegmentsInOrder(
        media: NonNullable<SendProps['media']>,
        cache: Map<string, string>,
    ): Promise<OneBotMessageSegment[]> {
        const segments: OneBotMessageSegment[] = []
        for (const item of media) {
            if (item.media_type !== 'photo' && item.media_type !== 'video') {
                continue
            }
            const { value } = await this.resolveMediaFileValue(item, cache)
            segments.push({
                type: item.media_type === 'photo' ? ('image' as const) : ('video' as const),
                data: { file: value },
            })
        }
        return segments
    }

    private buildTextSegments(texts: string[]): OneBotTextSegment[] {
        return texts.filter(Boolean).map((text) => ({
            type: 'text',
            data: {
                text,
            },
        }))
    }

    protected async realSend(texts: string[], props?: SendProps): Promise<any> {
        let { media } = props || {}
        media = media || []
        const _log = this.log
        const normalizedAttachments = normalizeForwarderImageAttachments(media, {
            maxImageBytes: resolveForwarderImageMaxBytes(this.getEffectiveConfig(props?.runtime_config)),
            log: _log,
        })
        media = normalizedAttachments.media
        const mediaFileCache = new Map<string, string>()
        const pics = await this.buildImageSegments(media, mediaFileCache)
        const videos = await this.buildVideoSegments(media, mediaFileCache)
        if (media.length > 0) {
            _log?.debug(`Send text with photos..., media: ${media}`)
            _log?.debug(`pics: ${pics}`)
            _log?.debug(`videos: ${videos}`)
        }

        const MAX_PICS = 10
        const picChunks = chunk(pics, MAX_PICS)
        // OneBot video segments are much more expensive than images and NapCat
        // commonly rejects a single message that carries several videos. Send
        // them one per message, after text/photo messages, so thumbnail+video
        // posts keep their existing "text first, video second" shape.
        const MAX_VIDEOS = 1
        const videoChunks = chunk(videos, MAX_VIDEOS)
        const textChunks = texts.length > 0 ? texts : []
        const n = Math.max(picChunks.length, textChunks.length)

        const _res: any[] = []
        const sendSegment = async (segments: Parameters<QQForwarder['sendWithPayload']>[0], label: string) => {
            try {
                const res = await this.sendWithPayload(segments)
                _res.push(res)
                return res
            } catch (error) {
                if (_res.length > 0) {
                    throw new PartialForwarderSendError(
                        `QQ partial send failed at ${label} after ${_res.length} visible segment(s)`,
                        _res,
                        label,
                        error,
                    )
                }
                throw error
            }
        }

        try {
            const mergedForwardConfig = this.normalizeMergedForwardConfig(props)
            if (mergedForwardConfig.enabled) {
                const segments = [
                    ...this.buildTextSegments(texts),
                    ...(await this.buildMediaSegmentsInOrder(media, mediaFileCache)),
                ]
                if (segments.length > 0) {
                    const res = await this.sendMergedForwardPayload(segments, mergedForwardConfig)
                    _res.push(res)
                }
                return _res
            }

            for (let i = 0; i < n; i++) {
                const text = textChunks[i]
                const msgPics = picChunks[i] || []

                const segments = []
                if (text) {
                    segments.push({ type: 'text', data: { text } })
                }
                if (msgPics.length > 0) {
                    // Cast to any to avoid complex type reconstruction in this snippet, though structure matches.
                    segments.push(...(msgPics as any[]))
                }

                if (segments.length > 0) {
                    await sendSegment(segments as any, `message:${i + 1}/${n}`)
                }
            }

            for (const [index, msgVideos] of videoChunks.entries()) {
                if (msgVideos.length > 0) {
                    await sendSegment(msgVideos as any, `video:${index + 1}/${videoChunks.length}`)
                }
            }

            return _res
        } finally {
            // In url delivery mode the bridge may fetch the media slightly after
            // the send response, so give compressed copies a grace period.
            const usedUrlDelivery = [...mediaFileCache.values()].some(
                (value) => value.startsWith('http://') || value.startsWith('https://'),
            )
            if (usedUrlDelivery) {
                const timer = setTimeout(() => normalizedAttachments.cleanup(), QQ_MEDIA_URL_CLEANUP_GRACE_MS)
                ;(timer as unknown as { unref?: () => void }).unref?.()
            } else {
                normalizedAttachments.cleanup()
            }
        }
    }

    private buildHeaders() {
        const headers: Record<string, string> = {
            'Content-Type': 'application/json',
        }
        if (this.token?.trim()) {
            headers.Authorization = `Bearer ${this.token}`
        }
        return headers
    }

    /**
     * A hung OneBot endpoint (TCP half-open) used to freeze the whole target
     * slot forever: axios without a timeout never rejects, the pRetry never
     * fires and the outbound stays stuck in `sending`.
     */
    private buildRequestOptions() {
        return {
            headers: this.buildHeaders(),
            timeout: QQ_REQUEST_TIMEOUT_MS,
        }
    }

    async sendWithPayload(arr_of_segments: OneBotMessageSegment[]) {
        if (this.isPrivateChat()) {
            let res
            try {
                res = await axios.post(
                    `${this.url}/send_private_msg`,
                    {
                        user_id: this.user_id,
                        message: arr_of_segments,
                    },
                    this.buildRequestOptions(),
                )
            } catch (error) {
                this.rethrowClassifiedTransportError(error)
            }
            this.assertOneBotResponseOk(res, 'send_private_msg')
            return res
        }
        let res
        try {
            res = await axios.post(
                `${this.url}/send_group_msg`,
                {
                    group_id: this.group_id,
                    message: arr_of_segments,
                },
                this.buildRequestOptions(),
            )
        } catch (error) {
            this.rethrowClassifiedTransportError(error)
        }
        this.assertOneBotResponseOk(res, 'send_group_msg')
        return res
    }

    async sendMergedForwardPayload(segments: OneBotMessageSegment[], config: MergedForwardRuntimeConfig) {
        const nodes = chunk(segments, config.maxSegmentsPerNode).map((content) => ({
            type: 'node',
            data: {
                name: config.nodeName,
                uin: config.nodeUin,
                content,
            },
        }))
        if (this.isPrivateChat()) {
            let res
            try {
                res = await axios.post(
                    `${this.url}/send_private_forward_msg`,
                    {
                        user_id: this.user_id,
                        messages: nodes,
                    },
                    this.buildRequestOptions(),
                )
            } catch (error) {
                this.rethrowClassifiedTransportError(error)
            }
            this.assertOneBotResponseOk(res, 'send_private_forward_msg')
            return res
        }
        let res
        try {
            res = await axios.post(
                `${this.url}/send_group_forward_msg`,
                {
                    group_id: this.group_id,
                    messages: nodes,
                },
                this.buildRequestOptions(),
            )
        } catch (error) {
            this.rethrowClassifiedTransportError(error)
        }
        this.assertOneBotResponseOk(res, 'send_group_forward_msg')
        return res
    }
}

export { QQForwarder }
