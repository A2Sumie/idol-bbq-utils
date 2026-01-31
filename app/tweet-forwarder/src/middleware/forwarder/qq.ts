import axios from 'axios'
import { chunk } from 'lodash'
import { Forwarder, type SendProps } from './base'
import { type ForwardTargetPlatformConfig, ForwardTargetPlatformEnum } from '@/types/forwarder'

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

    protected async realSend(texts: string[], props?: SendProps): Promise<any> {
        let { media } = props || {}
        media = media || []
        const _log = this.log
        let pics: Array<{
            type: 'image'
            data: {
                file: string
            }
        }> = media
            .filter((i) => i.media_type === 'photo')
            .map((i) => ({
                type: 'image',
                data: {
                    file: `file://${i.path}`,
                },
            }))
        let videos: Array<{
            type: 'video'
            data: {
                file: string
            }
        }> = media
            .filter((i) => i.media_type === 'video')
            .map((i) => ({
                type: 'video',
                data: {
                    file: `file://${i.path}`,
                },
            }))
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
                const res = await this.sendWithPayload(segments as any)
                _res.push(res)
            }
        }

        const maybe_video_res = videos.length !== 0 && (await this.sendWithPayload(videos))
        maybe_video_res && _res.push(maybe_video_res)
        return _res
    }

    async sendWithPayload(
        arr_of_segments: Array<
            | {
                type: 'text'
                data: {
                    text: string
                }
            }
            | {
                type: 'image'
                data: {
                    file: string
                }
            }
            | {
                type: 'video'
                data: {
                    file: string
                }
            }
        >,
    ) {
        const res = await axios.post(
            `${this.url}/send_group_msg`,
            {
                group_id: this.group_id,
                message: arr_of_segments,
            },
            {
                headers: {
                    Authorization: `Bearer ${this.token}`,
                    'Content-Type': 'application/json',
                },
            },
        )
        return res
    }
}

export { QQForwarder }
