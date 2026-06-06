import axios from 'axios'
import { Forwarder, type SendProps } from './base'
import { pRetry } from '@idol-bbq-utils/utils'
import FormData from 'form-data'
import fs from 'fs'
import { chunk } from 'lodash'
import { type ForwardTargetPlatformConfig, ForwardTargetPlatformEnum } from '@/types/forwarder'
import { buildBiliupUploadCandidate, runBiliupUpload } from './biliup'
import {
    normalizeForwarderImageAttachments,
    resolveForwarderImageMaxBytes,
} from '@/services/forwarder-image-attachment-service'
import DB, { type Article } from '@/db'
import { createHash } from 'crypto'

const BILI_VIDEO_UPLOAD_HASH_NAMESPACE = 'bilibili-video-upload'

interface BiliImageUploaded {
    img_src: string
    img_width: number
    img_height: number
    img_size: number
}

type BiliVideoUploadResult = 'uploaded' | 'duplicate' | 'dedupe_blocked' | 'upload_failed'
type BiliVideoUploadHashRecord = {
    hash: string
    path: string
}

class BiliForwarder extends Forwarder {
    static _PLATFORM = ForwardTargetPlatformEnum.Bilibili
    NAME = 'bilibili'
    private bili_jct: string
    private sessdata: string
    private media_check_level: ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.Bilibili>['media_check_level']
    private video_upload: ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.Bilibili>['video_upload']
    protected override BASIC_TEXT_LIMIT = 1000

    constructor(...[config, ...rest]: [...ConstructorParameters<typeof Forwarder>]) {
        super(config, ...rest)
        this.minInterval = 10000 // 10s
        const {
            bili_jct,
            sessdata,
            media_check_level = 'none',
            video_upload,
        } = config as ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.Bilibili>
        if (!bili_jct || !sessdata) {
            throw new Error(`forwarder ${this.NAME} bili_jct and sessdata are required`)
        }
        this.bili_jct = bili_jct
        this.sessdata = sessdata
        this.media_check_level = media_check_level
        this.video_upload = video_upload
    }

    protected async realSend(texts: string[], props?: SendProps): Promise<any> {
        const normalizedTexts = this.normalizeTextsForBilibili(texts)
        const videoUploadResult = await this.tryVideoUpload(normalizedTexts, props)
        if (videoUploadResult) {
            return [
                {
                    ok: true,
                    mode:
                        videoUploadResult === true || videoUploadResult === 'uploaded'
                            ? 'biliup'
                            : `biliup_${videoUploadResult}`,
                },
            ]
        }
        if (this.shouldSuppressMediaRequiredDynamic(props)) {
            this.log?.warn(
                `Suppressing Bilibili dynamic for ${props?.article?.a_id || 'unknown'}: target requires visible media but dynamic payload has no uploadable image media`,
            )
            return [{ ok: true, mode: 'dynamic_media_required_suppressed' }]
        }
        return this.sendDynamicContent(normalizedTexts, props)
    }

    private normalizeTextsForBilibili(texts: string[]) {
        return texts.map((text) =>
            text.replace(
                /^((?:@\S+\s+)?\d{4}[\u00b9\u00b2\u00b3\u2070-\u2079\u207b]*\s+X(?:发推|引用|回复|转推))\n{2,}/mu,
                '$1:\n',
            ),
        )
    }

    private buildVideoUploadMarker(article: Article | undefined, props?: SendProps) {
        if (article) {
            return `${String(article.platform)}:${article.a_id}`
        }
        return props?.outboundKey || 'unknown'
    }

    private hashVideoFile(filePath: string) {
        const buffer = fs.readFileSync(filePath)
        return createHash('sha256').update(buffer).digest('hex')
    }

    private resolveVideoUploadHashRecords(videoPaths: string[], props?: SendProps): BiliVideoUploadHashRecord[] {
        const records = new Map<string, BiliVideoUploadHashRecord>()
        for (const videoPath of videoPaths) {
            const mediaFile = (props?.media || []).find(
                (item) => item.media_type === 'video' && item.path === videoPath,
            )
            const hash = mediaFile?.content_hash || this.hashVideoFile(videoPath)
            records.set(hash, {
                hash,
                path: videoPath,
            })
        }
        return Array.from(records.values())
    }

    private async findDuplicateBiliVideoUpload(records: BiliVideoUploadHashRecord[]) {
        for (const record of records) {
            const existing = await DB.MediaHash.checkExist(BILI_VIDEO_UPLOAD_HASH_NAMESPACE, record.hash)
            if (existing) {
                return { record, existing }
            }
        }
        return null
    }

    private async markBiliVideoUploadSeen(records: BiliVideoUploadHashRecord[], marker: string) {
        for (const record of records) {
            await DB.MediaHash.save(BILI_VIDEO_UPLOAD_HASH_NAMESPACE, record.hash, marker)
        }
    }

    private async performBiliupUpload(
        article: Article | undefined,
        candidate: NonNullable<ReturnType<typeof buildBiliupUploadCandidate>>,
    ) {
        await runBiliupUpload(
            article || ({ a_id: 'unknown' } as any),
            candidate,
            {
                sessdata: this.sessdata,
                bili_jct: this.bili_jct,
            },
            this.log,
        )
    }

    private async tryVideoUpload(texts: string[], props?: SendProps): Promise<BiliVideoUploadResult | boolean> {
        const media = props?.media || []
        const candidate = buildBiliupUploadCandidate(props?.article, texts, media, this.video_upload)
        if (!candidate) {
            return false
        }

        let hashRecords: BiliVideoUploadHashRecord[]
        try {
            hashRecords = this.resolveVideoUploadHashRecords(candidate.videoPaths, props)
            const duplicate = await this.findDuplicateBiliVideoUpload(hashRecords)
            if (duplicate) {
                this.log?.warn(
                    `Skipping duplicate Bilibili video upload for ${props?.article?.a_id || 'unknown'}: ${duplicate.record.hash.substring(0, 8)} already uploaded as ${duplicate.existing.a_id || 'previous article'}`,
                )
                return 'duplicate'
            }
        } catch (error) {
            this.log?.error(
                `Bilibili video upload dedupe check failed for ${props?.article?.a_id || 'unknown'}; suppressing upload and dynamic fallback: ${error}`,
            )
            return 'dedupe_blocked'
        }

        try {
            await this.performBiliupUpload(props?.article, candidate)
            await this.markBiliVideoUploadSeen(hashRecords, this.buildVideoUploadMarker(props?.article, props)).catch(
                (error) => {
                    this.log?.error(
                        `Failed to mark Bilibili video upload hash for ${props?.article?.a_id || 'unknown'}: ${error}`,
                    )
                },
            )
            return 'uploaded'
        } catch (error) {
            this.log?.error(
                `biliup upload failed for ${props?.article?.a_id || 'unknown'}; suppressing dynamic fallback: ${error}`,
            )
            return 'upload_failed'
        }
    }

    private getMediaCheckLevel(props?: SendProps) {
        return (
            (this.getEffectiveConfig(props?.runtime_config) as any).media_check_level ||
            this.media_check_level ||
            'none'
        )
    }

    private isDynamicImageMedia(item: NonNullable<SendProps['media']>[number]) {
        return item.media_type === 'photo' || item.media_type === 'video_thumbnail'
    }

    private shouldSuppressMediaRequiredDynamic(props?: SendProps) {
        const config = this.getEffectiveConfig(props?.runtime_config)
        if (config.require_media !== true) {
            return false
        }
        return !(props?.media || []).some((item) => this.isDynamicImageMedia(item))
    }

    private async sendDynamicContent(texts: string[], props?: SendProps): Promise<any> {
        let { media } = props || {}
        media = media || []
        const _log = this.log
        const mediaCheckLevel = this.getMediaCheckLevel(props)
        const requireMedia = this.getEffectiveConfig(props?.runtime_config).require_media === true
        const normalizedAttachments = normalizeForwarderImageAttachments(media, {
            maxImageBytes: resolveForwarderImageMaxBytes(this.getEffectiveConfig(props?.runtime_config)),
            log: _log,
        })
        media = normalizedAttachments.media
        try {
            let pics: Array<BiliImageUploaded> = (
                await Promise.all(
                    media.map(async (item) => {
                        if (item.media_type === 'photo' || item.media_type === 'video_thumbnail') {
                            try {
                                _log?.debug(`Uploading photo ${item.path}`)
                                const obj = await pRetry(() => this.uploadPhoto(item.path), {
                                    retries: 2,
                                    onFailedAttempt() {
                                        _log?.error('Upload photo failed, retrying...')
                                    },
                                })
                                return obj
                            } catch (e) {
                                _log?.error(`Upload photo ${item.path} failed, skip this photo`)
                                return
                            }
                        }
                        // video to gif
                    }),
                )
            )
                .filter((i) => i !== undefined)
                .map((i) => ({
                    img_src: i.image_url,
                    img_width: i.image_width,
                    img_height: i.image_height,
                    img_size: i.image_size,
                }))
            const dynamicImageCount = media.filter((item) => this.isDynamicImageMedia(item)).length
            if ((mediaCheckLevel === 'loose' || requireMedia) && dynamicImageCount !== 0 && pics.length === 0) {
                _log?.error(`No photos uploaded, throw error.`)
                throw new Error(`No photos uploaded, please check your bili_jct and sessdata.`)
            }
            if ((mediaCheckLevel === 'strict' || requireMedia) && dynamicImageCount !== pics.length) {
                _log?.error(`Some photos upload failed.`)
                throw new Error(`Some photos upload failed, please check your bili_jct and sessdata.`)
            }
            // TODO: more pics support
            const MAX_PICS = 9
            const picChunks = chunk(pics, MAX_PICS)

            const textChunks = texts.length > 0 ? texts : []

            const n = Math.max(picChunks.length, textChunks.length)
            const _res = []

            for (let i = 0; i < n; i++) {
                const text = textChunks[i] || (i === 0 ? 'Forwarded content' : ' ') // Fallback text. Bilibili dynamic needs text.
                const msgPics = picChunks[i] || [] // Type: BiliImageUploaded[]

                _log?.debug(`Sending chunk ${i + 1}/${n}: text length ${text.length}, pics count ${msgPics.length}`)

                let res
                if (msgPics.length > 0) {
                    res = await this.sendTextWithPhotos(text, msgPics)
                } else {
                    if (!textChunks[i]) continue; // If no text and no pics, skip (shouldn't happen due to Math.max logic unless textChunks ran out and picChunks ran out)
                    res = await this.sendText(text)
                }
                _res.push(res)
            }
            _res.forEach((res) => {
                if (res.data.code !== 0) {
                    throw new Error(
                        `Send text to ${this.NAME} failed. ${res.data.message}: ${JSON.stringify(res.data)}`,
                    )
                }
            })
            return _res
        } finally {
            normalizedAttachments.cleanup()
        }
    }

    private async uploadPhoto(path: string) {
        const form = new FormData()
        form.append('file_up', fs.createReadStream(path))
        form.append('category', 'daily')
        form.append('csrf', this.bili_jct)
        const res = await axios.post('https://api.bilibili.com/x/dynamic/feed/draw/upload_bfs', form, {
            headers: {
                ...form.getHeaders(),
                Cookie: `SESSDATA=${this.sessdata}`,
            },
        })
        this.log?.debug(`Upload photo response: ${JSON.stringify(res.data)}`)
        return res.data.data
    }

    private async sendText(text: string) {
        return axios.post(
            'https://api.bilibili.com/x/dynamic/feed/create/dyn',
            {
                dyn_req: {
                    content: {
                        contents: [
                            {
                                raw_text: text,
                                type: 1,
                                biz_id: '',
                            },
                        ],
                    },
                    scene: 1,
                },
            },
            {
                headers: {
                    'Content-Type': 'application/json',
                    Cookie: `SESSDATA=${this.sessdata}`,
                },
                params: {
                    csrf: this.bili_jct,
                },
            },
        )
    }

    private async sendTextWithPhotos(
        text: string,
        pics: Array<{
            img_src: string
            img_width: number
            img_height: number
            img_size: number
        }>,
    ) {
        return axios.post(
            'https://api.bilibili.com/x/dynamic/feed/create/dyn',
            {
                dyn_req: {
                    content: {
                        contents: [
                            {
                                raw_text: text,
                                type: 1,
                                biz_id: '',
                            },
                        ],
                    },
                    pics: pics,
                    scene: 2,
                },
            },
            {
                headers: {
                    'Content-Type': 'application/json',
                    Cookie: `SESSDATA=${this.sessdata}`,
                },
                params: {
                    csrf: this.bili_jct,
                },
            },
        )
    }
}

export { BiliForwarder }
