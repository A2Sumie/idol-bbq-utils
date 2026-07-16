import axios from 'axios'
import { Forwarder, PartialForwarderSendError, type SendProps } from './base'
import { pRetry } from '@idol-bbq-utils/utils'
import FormData from 'form-data'
import fs from 'fs'
import { chunk } from 'lodash'
import { type ForwardTargetPlatformConfig, ForwardTargetPlatformEnum } from '@/types/forwarder'
import { buildBiliupUploadCandidate, completeBiliupUploadCandidateTags, runBiliupUpload } from './biliup'
import {
    normalizeForwarderImageAttachments,
    resolveForwarderImageMaxBytes,
} from '@/services/forwarder-image-attachment-service'
import DB, { type Article } from '@/db'
import { createHash } from 'crypto'
import {
    buildShortVideoDedupCandidate,
    buildVideoFingerprintCandidate,
    checkShortVideoCrossPlatformDuplicate,
    checkVideoFingerprintDuplicate,
    markShortVideoCrossPlatformSeen,
    markVideoFingerprintSeen,
    type ShortVideoDedupCandidate,
    type VideoFingerprintCandidate,
} from '@/services/media-cache-service'
import {
    BILIBILI_VIDEO_PAIRING_HELD_MODE,
    BILIBILI_VIDEO_PAIRING_MERGED_MODE,
    deserializeTeaserMedia,
    findBilibiliPendingPairingForMainVideo,
    holdBilibiliVideoPairingTeaser,
    markExpiredVideoPairings,
    resolveVideoPairingConfig,
} from '@/services/video-pairing-service'

const BILI_VIDEO_UPLOAD_HASH_NAMESPACE = 'bilibili-video-upload'

interface BiliImageUploaded {
    img_src: string
    img_width: number
    img_height: number
    img_size: number
}

type BiliUploadPhotoResponse = {
    image_url?: string
    image_width?: number
    image_height?: number
    image_size?: number
    img_size?: number
}

type BiliCreateDynamicResponse = {
    data?: {
        code?: number
        message?: string
        data?: {
            dyn_id?: string | number
            dyn_id_str?: string | number
        }
    }
}

type BiliVideoUploadResult = 'uploaded' | 'duplicate' | 'held' | 'merged'
type BiliVideoUploadHashRecord = {
    hash: string
    path: string
}
type BiliVideoUploadDedupeRecords = {
    exact: BiliVideoUploadHashRecord[]
    article?: Article
    videoMedia: Array<NonNullable<SendProps['media']>[number]>
    shortVideos?: ShortVideoDedupCandidate[]
    fingerprints?: VideoFingerprintCandidate[]
}
type BiliVideoUploadDuplicate =
    | {
          kind: 'exact'
          record: BiliVideoUploadHashRecord
          existing: Awaited<ReturnType<typeof DB.MediaHash.checkExist>>
      }
    | {
          kind: 'fingerprint'
          existing: Awaited<ReturnType<typeof DB.MediaHash.checkExist>>
      }
    | {
          kind: 'short-video'
          existing: Awaited<ReturnType<typeof DB.MediaHash.checkExist>>
      }

class BiliForwarder extends Forwarder {
    static _PLATFORM = ForwardTargetPlatformEnum.Bilibili
    NAME = 'bilibili'
    private bili_jct: string
    private sessdata: string
    private buvid3: string
    private buvid4: string
    private media_check_level: ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.Bilibili>['media_check_level']
    private video_upload: ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.Bilibili>['video_upload']
    private dynamicDetailValidationRetries = 3
    protected override BASIC_TEXT_LIMIT = 1000

    constructor(...[config, ...rest]: [...ConstructorParameters<typeof Forwarder>]) {
        super(config, ...rest)
        this.minInterval = 10000 // 10s
        const {
            bili_jct,
            sessdata,
            buvid3 = '',
            buvid4 = '',
            media_check_level = 'none',
            video_upload,
        } = config as ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.Bilibili>
        if (!bili_jct || !sessdata) {
            throw new Error(`forwarder ${this.NAME} bili_jct and sessdata are required`)
        }
        this.bili_jct = bili_jct
        this.sessdata = sessdata
        this.buvid3 = buvid3
        this.buvid4 = buvid4
        this.media_check_level = media_check_level
        this.video_upload = video_upload
    }

    private buildBiliCookieHeader(): string {
        const parts = [`SESSDATA=${this.sessdata}`, `bili_jct=${this.bili_jct}`]
        if (this.buvid3) parts.push(`buvid3=${this.buvid3}`)
        if (this.buvid4) parts.push(`buvid4=${this.buvid4}`)
        return parts.join('; ')
    }

    private get biliApiHeaders() {
        return {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            Referer: 'https://t.bilibili.com/',
            Origin: 'https://t.bilibili.com',
        }
    }

    protected async realSend(texts: string[], props?: SendProps): Promise<any> {
        const normalizedTexts = this.normalizeTextsForBilibili(texts)
        const videoUploadResult = await this.tryVideoUpload(normalizedTexts, props)
        if (videoUploadResult) {
            return [
                {
                    ok: true,
                    mode:
                        videoUploadResult === 'held'
                            ? BILIBILI_VIDEO_PAIRING_HELD_MODE
                            : videoUploadResult === 'merged'
                              ? BILIBILI_VIDEO_PAIRING_MERGED_MODE
                              : videoUploadResult === 'duplicate'
                                ? 'biliup_duplicate'
                                : 'biliup',
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

    private isRootArticleMedia(item: NonNullable<SendProps['media']>[number], props?: SendProps) {
        const rootArticleId = props?.article?.a_id?.trim()
        return !rootArticleId || !item.sourceArticleId || item.sourceArticleId === rootArticleId
    }

    private resolveVideoUploadMedia(props?: SendProps) {
        const media = props?.media?.length ? props.media : [...(props?.contentMedia || []), ...(props?.cardMedia || [])]
        const rootMedia = media.filter((item) => this.isRootArticleMedia(item, props))
        const rootHasVideo = rootMedia.some((item) => item.media_type === 'video')
        if (!rootHasVideo) {
            return rootMedia
        }

        const seen = new Set(rootMedia.map((item) => item.path))
        const referencedVideos = media.filter((item) => {
            if (this.isRootArticleMedia(item, props) || item.media_type !== 'video' || seen.has(item.path)) {
                return false
            }
            seen.add(item.path)
            return true
        })
        return [...rootMedia, ...referencedVideos]
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

    private resolveVideoUploadExactHashRecords(videoPaths: string[], props?: SendProps): BiliVideoUploadHashRecord[] {
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

    private resolveVideoUploadDedupeRecords(videoPaths: string[], props?: SendProps): BiliVideoUploadDedupeRecords {
        const exact = this.resolveVideoUploadExactHashRecords(videoPaths, props)
        if (!props?.article) {
            return {
                exact,
                videoMedia: [],
            }
        }

        const videoMedia = videoPaths
            .map((videoPath) =>
                (props.media || []).find((item) => item.media_type === 'video' && item.path === videoPath),
            )
            .filter((item): item is NonNullable<SendProps['media']>[number] => Boolean(item))
        return {
            exact,
            article: props.article,
            videoMedia,
        }
    }

    private resolveVideoUploadFingerprints(records: BiliVideoUploadDedupeRecords) {
        if (!records.article) {
            return []
        }
        if (!records.fingerprints) {
            records.fingerprints = records.videoMedia
                .map((item) => buildVideoFingerprintCandidate(records.article as any, item as any))
                .filter((item): item is VideoFingerprintCandidate => Boolean(item))
        }
        return records.fingerprints
    }

    private resolveVideoUploadShortVideos(records: BiliVideoUploadDedupeRecords) {
        if (!records.article) {
            return []
        }
        if (!records.shortVideos) {
            records.shortVideos = records.videoMedia
                .map((item) => buildShortVideoDedupCandidate(records.article as any, [item as any]))
                .filter((item): item is ShortVideoDedupCandidate => Boolean(item))
        }
        return records.shortVideos
    }

    private async findDuplicateBiliVideoUpload(
        records: BiliVideoUploadDedupeRecords,
    ): Promise<BiliVideoUploadDuplicate | null> {
        for (const record of records.exact) {
            const existing = await DB.MediaHash.checkExist(BILI_VIDEO_UPLOAD_HASH_NAMESPACE, record.hash)
            if (existing) {
                return { kind: 'exact', record, existing }
            }
        }
        for (const fingerprint of this.resolveVideoUploadFingerprints(records)) {
            const existing = await checkVideoFingerprintDuplicate(fingerprint)
            if (existing) {
                return { kind: 'fingerprint', existing }
            }
        }
        for (const shortVideo of this.resolveVideoUploadShortVideos(records)) {
            const existing = await checkShortVideoCrossPlatformDuplicate(shortVideo)
            if (existing) {
                return { kind: 'short-video', existing }
            }
        }
        return null
    }

    private async markBiliVideoUploadSeen(records: BiliVideoUploadDedupeRecords, marker: string) {
        for (const record of records.exact) {
            await DB.MediaHash.save(BILI_VIDEO_UPLOAD_HASH_NAMESPACE, record.hash, marker)
        }
        for (const fingerprint of this.resolveVideoUploadFingerprints(records)) {
            await markVideoFingerprintSeen(fingerprint)
        }
        for (const shortVideo of this.resolveVideoUploadShortVideos(records)) {
            await markShortVideoCrossPlatformSeen(shortVideo)
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

    private formatError(error: unknown) {
        return error instanceof Error ? error.message : String(error)
    }

    private async tryHoldTeaserForPairing(
        props: SendProps | undefined,
        pairingConfig: NonNullable<ReturnType<typeof resolveVideoPairingConfig>>,
    ): Promise<BiliVideoUploadResult | false> {
        if (!props?.article || props.forceSend) {
            return false
        }
        const media = this.resolveVideoUploadMedia(props)
        const held = await holdBilibiliVideoPairingTeaser({
            targetId: this.id,
            article: props.article,
            media,
            config: pairingConfig,
            log: this.log,
        })
        return held.held ? 'held' : false
    }

    private async resolvePairedTeaserMedia(
        props: SendProps | undefined,
        pairingConfig: NonNullable<ReturnType<typeof resolveVideoPairingConfig>>,
    ) {
        if (!props?.article || props.forceSend) {
            return null
        }
        const pairing = await findBilibiliPendingPairingForMainVideo({
            targetId: this.id,
            article: props.article,
            config: pairingConfig,
        })
        if (!pairing) {
            return null
        }
        const media = deserializeTeaserMedia(pairing)
        if (media.length === 0) {
            this.log?.warn(
                `Dropping stale video pairing ${pairing.source_article_key}: no teaser media file is still available`,
            )
            await DB.VideoPairing.markStatus(pairing.id, DB.VideoPairing.STATUS.Dropped, {
                reason: 'missing_teaser_media',
            }).catch(() => undefined)
            return null
        }
        this.log?.info(
            `Merging Bilibili video ${props.article.a_id} with held teaser ${pairing.source_article_key} (${media.length} part(s))`,
        )
        return { pairing, media }
    }

    private async tryVideoUpload(texts: string[], props?: SendProps): Promise<BiliVideoUploadResult | false> {
        const effectiveConfig = this.getEffectiveConfig(props?.runtime_config) as any
        const pairingConfig = resolveVideoPairingConfig(effectiveConfig)
        if (pairingConfig) {
            await markExpiredVideoPairings(this.log).catch((error) =>
                this.log?.warn(`Video pairing expiry sweep failed: ${this.formatError(error)}`),
            )
            const held = await this.tryHoldTeaserForPairing(props, pairingConfig)
            if (held) {
                return held
            }
        }

        let media = this.resolveVideoUploadMedia(props)
        const pairedTeaserMedia = pairingConfig ? await this.resolvePairedTeaserMedia(props, pairingConfig) : null
        if (pairedTeaserMedia && pairedTeaserMedia.media.length > 0) {
            media = [...media, ...pairedTeaserMedia.media]
        }
        const videoUploadConfig = (effectiveConfig.video_upload as typeof this.video_upload) || this.video_upload
        const candidate = buildBiliupUploadCandidate(props?.article, texts, media, videoUploadConfig)
        if (!candidate) {
            return false
        }
        await completeBiliupUploadCandidateTags(props?.article, texts, candidate, this.log)

        let dedupeRecords: BiliVideoUploadDedupeRecords
        try {
            dedupeRecords = this.resolveVideoUploadDedupeRecords(candidate.videoPaths, props)
            const duplicate = await this.findDuplicateBiliVideoUpload(dedupeRecords)
            if (duplicate) {
                const detail =
                    duplicate.kind === 'exact'
                        ? `${duplicate.record.hash.substring(0, 8)} already uploaded`
                        : `${duplicate.kind} matched`
                this.log?.warn(
                    `Skipping duplicate Bilibili video upload for ${props?.article?.a_id || 'unknown'}: ${detail} as ${duplicate.existing?.a_id || 'previous article'}`,
                )
                if (pairedTeaserMedia?.pairing) {
                    await DB.VideoPairing.markStatus(pairedTeaserMedia.pairing.id, DB.VideoPairing.STATUS.Dropped, {
                        reason: 'main_video_duplicate',
                        duplicate_kind: duplicate.kind,
                        existing_a_id: duplicate.existing?.a_id || null,
                    }).catch(() => undefined)
                }
                return 'duplicate'
            }
        } catch (error) {
            const message = this.formatError(error)
            this.log?.error(
                `Bilibili video upload dedupe check failed for ${props?.article?.a_id || 'unknown'}; refusing dynamic fallback: ${message}`,
            )
            throw new Error(
                `Bilibili video upload dedupe check failed for ${props?.article?.a_id || 'unknown'}: ${message}`,
            )
        }

        try {
            await this.performBiliupUpload(props?.article, candidate)
            await this.markBiliVideoUploadSeen(dedupeRecords, this.buildVideoUploadMarker(props?.article, props)).catch(
                (error) => {
                    this.log?.error(
                        `Failed to mark Bilibili video upload hash for ${props?.article?.a_id || 'unknown'}: ${error}`,
                    )
                },
            )
            if (pairedTeaserMedia?.pairing) {
                await DB.VideoPairing.markMerged(pairedTeaserMedia.pairing.id, {
                    target_article_key: props?.article
                        ? `${String(props.article.platform)}:${props.article.a_id}`
                        : null,
                    target_article_id: (props?.article as any)?.id || null,
                    target_video_id: props?.article?.a_id || null,
                    merge_result: {
                        mode: BILIBILI_VIDEO_PAIRING_MERGED_MODE,
                        parts: candidate.videoPaths.length,
                        source_article_key: pairedTeaserMedia.pairing.source_article_key,
                    },
                }).catch((error) => {
                    this.log?.error(
                        `Failed to mark Bilibili video pairing merged for ${props?.article?.a_id}: ${error}`,
                    )
                })
                return 'merged'
            }
            return 'uploaded'
        } catch (error) {
            const message = this.formatError(error)
            this.log?.error(
                `biliup video publish failed for ${props?.article?.a_id || 'unknown'}; refusing dynamic fallback: ${message}`,
            )
            throw new Error(`biliup video publish failed for ${props?.article?.a_id || 'unknown'}: ${message}`)
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
        return item.media_type === 'photo'
    }

    private isMessagePackArticle(props?: SendProps) {
        const article = props?.article as any
        return article?.type === 'message_pack' || article?.extra?.extra_type === 'message_pack_meta'
    }

    private getRequiredSourceImageMedia(props?: SendProps) {
        if (!props) {
            return []
        }
        if (this.isMessagePackArticle(props)) {
            return [...(props.contentMedia || []), ...(props.cardMedia || [])].filter((item) =>
                this.isDynamicImageMedia(item),
            )
        }
        if (props.contentMedia) {
            return props.contentMedia.filter((item) => this.isDynamicImageMedia(item))
        }
        const cardPaths = new Set((props.cardMedia || []).map((item) => item.path).filter(Boolean))
        return (props.media || []).filter((item) => this.isDynamicImageMedia(item) && !cardPaths.has(item.path))
    }

    private shouldSuppressMediaRequiredDynamic(props?: SendProps) {
        const config = this.getEffectiveConfig(props?.runtime_config)
        if (config.require_media !== true) {
            return false
        }
        return this.getRequiredSourceImageMedia(props).length === 0
    }

    private normalizeUploadedPhoto(value: BiliUploadPhotoResponse | undefined): BiliImageUploaded | null {
        if (!value?.image_url || !value.image_width || !value.image_height) {
            return null
        }
        const imageSize = Number(value.img_size ?? value.image_size ?? 0)
        if (!Number.isFinite(imageSize) || imageSize <= 0) {
            return null
        }
        return {
            img_src: value.image_url,
            img_width: value.image_width,
            img_height: value.image_height,
            img_size: imageSize,
        }
    }

    private extractDynamicId(res: BiliCreateDynamicResponse) {
        const data = res.data?.data
        const dynId = data?.dyn_id_str ?? data?.dyn_id
        return dynId === undefined || dynId === null ? '' : String(dynId).trim()
    }

    private getDynamicDetailMajor(detail: any) {
        return detail?.data?.data?.item?.modules?.module_dynamic?.major
    }

    private countDynamicDetailImages(detail: any) {
        const major = this.getDynamicDetailMajor(detail)
        const drawItems = major?.draw?.items
        if (Array.isArray(drawItems)) {
            return drawItems.filter((item) => item?.src || item?.img_src || item?.url).length
        }
        const opusPics = major?.opus?.pics
        if (Array.isArray(opusPics)) {
            return opusPics.filter((item) => item?.url || item?.src).length
        }
        return 0
    }

    private async fetchDynamicDetail(dynamicId: string) {
        return axios.get('https://api.bilibili.com/x/polymer/web-dynamic/v1/detail', {
            params: {
                id: dynamicId,
            },
            headers: {
                ...this.biliApiHeaders,
                Cookie: this.buildBiliCookieHeader(),
            },
        })
    }

    private assertProviderResponseOk(res: BiliCreateDynamicResponse, context: string) {
        if (res.data?.code !== 0) {
            throw new Error(`Send ${context} to ${this.NAME} failed. ${res.data?.message}: ${JSON.stringify(res.data)}`)
        }
    }

    private async assertPhotoDynamicVisible(res: BiliCreateDynamicResponse, expectedPicCount: number) {
        const dynamicId = this.extractDynamicId(res)
        if (!dynamicId) {
            throw new Error(`Bilibili photo dynamic response did not include dyn_id_str.`)
        }

        await pRetry(
            async () => {
                const detail = await this.fetchDynamicDetail(dynamicId)
                if (detail.data?.code !== 0) {
                    throw new Error(`Bilibili dynamic detail failed. ${JSON.stringify(detail.data)}`)
                }
                const major = this.getDynamicDetailMajor(detail)
                const imageCount = this.countDynamicDetailImages(detail)
                if (!major || imageCount < expectedPicCount) {
                    throw new Error(
                        `Bilibili photo dynamic ${dynamicId} has invalid detail major: major=${major ? JSON.stringify(Object.keys(major)) : 'null'} image_count=${imageCount} expected=${expectedPicCount}`,
                    )
                }
            },
            {
                retries: this.dynamicDetailValidationRetries,
                minTimeout: 1500,
                maxTimeout: 3000,
                onFailedAttempt: (error) => {
                    this.log?.warn(
                        `Bilibili photo dynamic detail validation pending for ${dynamicId}: ${error.originalError.message}`,
                    )
                },
            },
        )
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
                        if (this.isDynamicImageMedia(item)) {
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
                .map((i) => this.normalizeUploadedPhoto(i))
                .filter((i): i is BiliImageUploaded => Boolean(i))
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
                const msgPics = picChunks[i] || [] // Type: BiliImageUploaded[]
                const text = textChunks[i] || (msgPics.length > 0 ? ' ' : '')

                _log?.debug(`Sending chunk ${i + 1}/${n}: text length ${text.length}, pics count ${msgPics.length}`)

                let res
                if (msgPics.length > 0) {
                    res = await this.sendTextWithPhotos(text, msgPics)
                    this.assertProviderResponseOk(res, `photo dynamic chunk ${i + 1}/${n}`)
                    _res.push(res)
                    try {
                        await this.assertPhotoDynamicVisible(res, msgPics.length)
                    } catch (error) {
                        throw new PartialForwarderSendError(
                            `Bilibili photo dynamic post-validation failed for chunk ${i + 1}/${n}`,
                            _res,
                            `photo dynamic chunk ${i + 1}/${n}`,
                            error,
                        )
                    }
                } else {
                    if (!textChunks[i]) continue // If no text and no pics, skip (shouldn't happen due to Math.max logic unless textChunks ran out and picChunks ran out)
                    res = await this.sendText(text)
                    this.assertProviderResponseOk(res, `text dynamic chunk ${i + 1}/${n}`)
                    _res.push(res)
                }
            }
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
                ...this.biliApiHeaders,
                Cookie: this.buildBiliCookieHeader(),
            },
        })
        this.log?.debug(`Upload photo response: ${JSON.stringify(res.data)}`)
        if (res.data?.code !== 0) {
            throw new Error(`Upload photo to ${this.NAME} failed. ${res.data?.message}: ${JSON.stringify(res.data)}`)
        }
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
                    ...this.biliApiHeaders,
                    Cookie: this.buildBiliCookieHeader(),
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
                    ...this.biliApiHeaders,
                    Cookie: this.buildBiliCookieHeader(),
                },
                params: {
                    csrf: this.bili_jct,
                },
            },
        )
    }
}

export { BiliForwarder }
