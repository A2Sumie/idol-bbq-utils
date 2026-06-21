import axios from 'axios'
import { chunk } from 'lodash'
import { Forwarder, PartialForwarderSendError, type SendProps } from './base'
import { type ForwardTargetPlatformConfig, ForwardTargetPlatformEnum } from '@/types/forwarder'
import {
    normalizeForwarderImageAttachments,
    resolveForwarderImageMaxBytes,
} from '@/services/forwarder-image-attachment-service'

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

type MergedForwardRuntimeConfig = {
    enabled: boolean
    nodeName: string
    nodeUin: string
    maxSegmentsPerNode: number
}

class QQForwarder extends Forwarder {
    static _PLATFORM = ForwardTargetPlatformEnum.QQ
    private group_id: string
    private url: string
    private token: string
    NAME = 'qq'
    protected override BASIC_TEXT_LIMIT = 4000

    constructor(...[config, ...rest]: [...ConstructorParameters<typeof Forwarder>]) {
        super(config, ...rest)
        this.minInterval = 1000 // 1s
        const { group_id, url, token } = config as ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.QQ>
        if (!group_id || !url) {
            throw new Error(`forwarder ${this.NAME} group_id and url is required`)
        }
        this.group_id = group_id
        this.url = url
        this.token = token
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

    private assertOneBotResponseOk(res: { data?: any; statusText?: string }, context: string) {
        const data = res?.data
        const status = String(data?.status || '')
            .trim()
            .toLowerCase()
        const retcodeRaw = data?.retcode
        const retcodeText = retcodeRaw === undefined || retcodeRaw === null ? '' : String(retcodeRaw).trim()
        const hasRetcode = retcodeText.length > 0
        const retcode = Number(retcodeRaw)
        const retcodeFailed = hasRetcode && (!Number.isFinite(retcode) || retcode !== 0)
        if ((status && status !== 'ok') || retcodeFailed) {
            const message = String(
                data?.message || data?.wording || data?.msg || data?.error || res?.statusText || 'unknown',
            )
            throw new Error(
                `QQ OneBot send failed (${context}): status=${status || 'unknown'} retcode=${hasRetcode ? retcodeRaw : 'unknown'} message=${message}`,
            )
        }
    }

    private buildImageSegments(media: NonNullable<SendProps['media']>): OneBotImageSegment[] {
        return media
            .filter((i) => i.media_type === 'photo')
            .map((i) => ({
                type: 'image',
                data: {
                    file: `file://${i.path}`,
                },
            }))
    }

    private buildVideoSegments(media: NonNullable<SendProps['media']>): OneBotVideoSegment[] {
        return media
            .filter((i) => i.media_type === 'video')
            .map((i) => ({
                type: 'video',
                data: {
                    file: `file://${i.path}`,
                },
            }))
    }

    private buildMediaSegmentsInOrder(media: NonNullable<SendProps['media']>): OneBotMessageSegment[] {
        return media.flatMap((item) => {
            if (item.media_type === 'photo') {
                return [
                    {
                        type: 'image' as const,
                        data: {
                            file: `file://${item.path}`,
                        },
                    },
                ]
            }
            if (item.media_type === 'video') {
                return [
                    {
                        type: 'video' as const,
                        data: {
                            file: `file://${item.path}`,
                        },
                    },
                ]
            }
            return []
        })
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
        let pics = this.buildImageSegments(media)
        let videos = this.buildVideoSegments(media)
        if (media.length > 0) {
            _log?.debug(`Send text with photos..., media: ${media}`)
            _log?.debug(`pics: ${pics}`)
            _log?.debug(`videos: ${videos}`)
        }

        const MAX_PICS = 10
        const picChunks = chunk(pics, MAX_PICS)
        const textChunks = texts.length > 0 ? texts : []
        const n = Math.max(picChunks.length, textChunks.length)

        const _res = []
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
                const segments = [...this.buildTextSegments(texts), ...this.buildMediaSegmentsInOrder(media)]
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

            videos.length !== 0 && (await sendSegment(videos, 'video'))
            return _res
        } finally {
            normalizedAttachments.cleanup()
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

    async sendWithPayload(arr_of_segments: OneBotMessageSegment[]) {
        const res = await axios.post(
            `${this.url}/send_group_msg`,
            {
                group_id: this.group_id,
                message: arr_of_segments,
            },
            {
                headers: this.buildHeaders(),
            },
        )
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
        const res = await axios.post(
            `${this.url}/send_group_forward_msg`,
            {
                group_id: this.group_id,
                messages: nodes,
            },
            {
                headers: this.buildHeaders(),
            },
        )
        this.assertOneBotResponseOk(res, 'send_group_forward_msg')
        return res
    }
}

export { QQForwarder }
