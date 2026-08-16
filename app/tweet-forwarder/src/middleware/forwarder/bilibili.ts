import { Forwarder, NonRetryableForwarderSendError, PartialForwarderSendError, type SendProps } from './base'
import { pRetry } from '@idol-bbq-utils/utils'
import { delay } from '@/utils/time'
import { stripUrlsFromText } from '@/utils/base'
import { chunk } from 'lodash'
import { type ForwardTargetPlatformConfig, ForwardTargetPlatformEnum } from '@/types/forwarder'
import { buildBiliupUploadCandidate, completeBiliupUploadCandidateTags, runBiliupUpload } from './biliup'
import { Platform } from '@idol-bbq-utils/spider/types'
import {
    normalizeForwarderImageAttachments,
    resolveForwarderImageMaxBytes,
} from '@/services/forwarder-image-attachment-service'
import DB, { type Article } from '@/db'
import { createHash } from 'crypto'
import fs from 'fs'
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
    isXTiktokTeaserArticle,
    markExpiredVideoPairings,
    resolveVideoPairingConfig,
} from '@/services/video-pairing-service'
import { assertBiliResponseOk, BilibiliApiClient, BiliUploadVelocityError } from './bilibili-api'

const BILI_VIDEO_UPLOAD_HASH_NAMESPACE = 'bilibili-video-upload'

/**
 * Kept as an alias of the API client's velocity error so existing imports and `instanceof` checks
 * (tests, whole-send retry policy) keep working after transport was extracted into bilibili-api.ts.
 */
const BiliUploadThrottledError = BiliUploadVelocityError

const BILI_PHOTO_UPLOAD_GAP_MS = 2000
const BILI_PHOTO_UPLOAD_COOLDOWN_MS = 15000
const BILI_PHOTO_UPLOAD_MISSING_MARKER = '[缺图]'

type BiliUploadQueueState = {
    chain: Promise<unknown>
    lastUploadAt: number
    cooldownUntil: number
}

const biliUploadQueues = new Map<string, BiliUploadQueueState>()

function isBiliUploadThrottledError(error: unknown): boolean {
    let current = error
    for (let depth = 0; depth < 4; depth++) {
        if (current instanceof BiliUploadThrottledError) {
            return true
        }
        if (!current || typeof current !== 'object' || !('originalError' in current)) {
            return false
        }
        current = (current as { originalError?: unknown }).originalError
    }
    return false
}

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
    private media_check_level: ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.Bilibili>['media_check_level']
    private video_upload: ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.Bilibili>['video_upload']
    private api: BilibiliApiClient
    private dynamicDetailValidationRetries = 3
    /** Pacing knobs as instance fields so tests can override them (same pattern as minInterval). */
    private photoUploadGapMs = BILI_PHOTO_UPLOAD_GAP_MS
    private photoUploadRetries = 3
    private photoUploadRetryMinTimeoutMs = 10000
    private photoUploadCooldownMs = BILI_PHOTO_UPLOAD_COOLDOWN_MS
    private dynamicCreateRetries = 2
    private dynamicCreateRetryMinTimeoutMs = 3000
    // Same image (content hash) re-uploaded within the TTL reuses the previous
    // upload_bfs result: retry flushes and dedup-approved repeated media used to
    // pay a full upload + serial gap every time.
    private uploadResultCache = new Map<string, { at: number; uploaded: BiliUploadPhotoResponse }>()
    private static readonly UPLOAD_RESULT_TTL_MS = 4 * 60 * 60 * 1000
    private static readonly UPLOAD_RESULT_CACHE_LIMIT = 200
    protected override BASIC_TEXT_LIMIT = 1000

    static resetUploadQueuesForTests() {
        biliUploadQueues.clear()
    }

    constructor(...[config, ...rest]: [...ConstructorParameters<typeof Forwarder>]) {
        super(config, ...rest)
        this.minInterval = 10000 // 10s
        const {
            bili_jct,
            sessdata,
            buvid3 = '',
            buvid4 = '',
            cookie_file,
            cookies,
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
        const cookieFile = cookie_file || video_upload?.cookie_file
        this.api = new BilibiliApiClient({
            bili_jct,
            sessdata,
            buvid3,
            buvid4,
            cookies: {
                ...BilibiliApiClient.readCookieDocument(cookieFile),
                ...(cookies || {}),
            },
        })
    }

    private buvidFetchPromise: Promise<void> | null = null

    /**
     * upload_bfs (and other WAF-strict endpoints) reject sessions without buvid3/buvid4 even when
     * SESSDATA/bili_jct are valid (-101). Fetch an anonymous pair from the SPI endpoint once and
     * cache it when the config does not provide them.
     */
    private ensureBuvidCookies(): Promise<void> {
        if (this.api.hasBuvid || this.buvidFetchPromise) {
            return this.buvidFetchPromise || Promise.resolve()
        }
        this.buvidFetchPromise = (async () => {
            try {
                const buvid = await this.api.fetchAnonymousBuvid()
                if (buvid) {
                    this.api.setBuvid(buvid.buvid3, buvid.buvid4)
                    this.log?.info(`Fetched anonymous buvid3/buvid4 for ${this.id} (config did not provide them)`)
                }
            } catch (error) {
                this.buvidFetchPromise = null
                this.log?.warn(`Failed to fetch buvid cookies for ${this.id}: ${error}`)
            } finally {
                if (!this.api.hasBuvid) {
                    this.buvidFetchPromise = null
                }
            }
        })()
        return this.buvidFetchPromise
    }

    protected async realSend(texts: string[], props?: SendProps): Promise<any> {
        const mediaSuppressionNotice = this.resolveMediaSuppressionNotice(texts, props)
        const normalizedTexts = this.normalizeTextsForBilibili(texts)
        // Bilibili posts must never carry source links (blog/official requests): strip URLs from
        // every text path (article sends, summary cards, digests, passthroughs) at the sender gate,
        // not only in the forwarder-manager article path.
        const urlStrippedTexts = normalizedTexts.map((text) => stripUrlsFromText(text))
        if (mediaSuppressionNotice) {
            const noticeTexts = [`【媒体未转载：${mediaSuppressionNotice}】`, ...urlStrippedTexts]
            const textOnlyProps = {
                ...props,
                media: [],
                contentMedia: [],
                cardMedia: [],
                videoUploadMedia: [],
                runtime_config: {
                    ...(props?.runtime_config || {}),
                    require_media: false,
                },
            }
            return this.sendDynamicContent(noticeTexts, textOnlyProps)
        }
        const videoUploadResult = await this.tryVideoUpload(urlStrippedTexts, props)
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
        return this.sendDynamicContent(urlStrippedTexts, props)
    }

    private countSuppressibleMedia(props?: SendProps) {
        const seen = new Set<string>()
        let photos = 0
        let videos = 0
        for (const item of [...(props?.media || []), ...(props?.videoUploadMedia || [])]) {
            const key = item.path
            if (key && seen.has(key)) {
                continue
            }
            if (key) {
                seen.add(key)
            }
            if (item.media_type === 'video' || item.media_type === 'video_thumbnail') {
                videos += 1
            } else {
                photos += 1
            }
        }
        const parts: string[] = []
        if (photos > 0) {
            parts.push(`${photos} 张图片`)
        }
        if (videos > 0) {
            parts.push(`${videos} 个视频`)
        }
        return parts.length > 0 ? `，已过滤 ${parts.join('、')}` : ''
    }

    private resolveMediaSuppressionNotice(texts: string[], props?: SendProps) {
        const article = props?.article
        if (!article) {
            return null
        }
        const config = this.getEffectiveConfig(props?.runtime_config) as any
        const suppressedUids = new Set(
            (config.suppress_media_uids || []).map((value: unknown) => String(value).trim()),
        )
        if (suppressedUids.has(article.u_id)) {
            const label = article.u_id.split(':').pop() || article.u_id
            this.log?.info(`Suppressing Bilibili media for ${article.a_id}: source ${article.u_id}`)
            return `FC ${label} 内容${this.countSuppressibleMedia(props)}`
        }
        if (!config.suppress_members_only_media) {
            return null
        }
        // Public official-site feeds (blog/news/live-report) legitimately mention
        // 会員限定 when announcing FC updates; the text heuristic must not suppress
        // their media. FC areas are covered by suppress_media_uids above, and
        // explicit members_only flags (e.g. YouTube) still apply to every platform.
        const extra = article.extra?.data as any
        if (extra?.members_only === true) {
            this.log?.info(`Suppressing Bilibili media for ${article.a_id}: members-only source`)
            return `会员限定内容${this.countSuppressibleMedia(props)}`
        }
        if (article.platform === Platform.Website) {
            return null
        }
        const haystack = [article.content || '', article.translation || '', ...texts].join('\n')
        const membersOnly =
            /会员限定|会員限定|メンバー限定|メン限|メンシプ|members?[-\s]?only|subscribers?[-\s]?only/i.test(
                haystack,
            )
        if (membersOnly) {
            this.log?.info(`Suppressing Bilibili media for ${article.a_id}: members-only source`)
            return `会员限定内容${this.countSuppressibleMedia(props)}`
        }
        return null
    }

    private normalizeTextsForBilibili(texts: string[]) {
        return texts.map((text) =>
            text.replace(
                /^((?:@\S+\s+)?\d{4}[\u00b9\u00b2\u00b3\u2070-\u2079\u207a\u207b]*\s+X(?:发推|引用|回复|转推))\n{2,}/mu,
                '$1:\n',
            ),
        )
    }

    private isRootArticleMedia(item: NonNullable<SendProps['media']>[number], props?: SendProps) {
        const rootArticleId = props?.article?.a_id?.trim()
        return !rootArticleId || !item.sourceArticleId || item.sourceArticleId === rootArticleId
    }

    private resolveVideoUploadMedia(props?: SendProps) {
        const cardPaths = new Set((props?.cardMedia || []).map((item) => item.path))
        const media = props?.videoUploadMedia || (props?.media || []).filter((item) => !cardPaths.has(item.path))
        const rootMedia = media.filter((item) => this.isRootArticleMedia(item, props))
        const rootHasVideo = rootMedia.some((item) => item.media_type === 'video')
        if (!rootHasVideo) {
            return rootMedia
        }

        const seen = new Set(rootMedia.map((item) => item.path))
        const referencedVideos = media.filter((item) => {
            if (
                this.isRootArticleMedia(item, props) ||
                !['video', 'video_thumbnail'].includes(item.media_type) ||
                seen.has(item.path)
            ) {
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

    private resolveVideoUploadExactHashRecords(
        videoPaths: string[],
        uploadMedia: NonNullable<SendProps['media']> = [],
    ): BiliVideoUploadHashRecord[] {
        const records = new Map<string, BiliVideoUploadHashRecord>()
        for (const videoPath of videoPaths) {
            const mediaFile = uploadMedia.find((item) => item.media_type === 'video' && item.path === videoPath)
            const hash = mediaFile?.content_hash || this.hashVideoFile(videoPath)
            records.set(hash, {
                hash,
                path: videoPath,
            })
        }
        return Array.from(records.values())
    }

    private resolveVideoUploadDedupeRecords(
        videoPaths: string[],
        props?: SendProps,
        uploadMedia: NonNullable<SendProps['media']> = props?.videoUploadMedia || props?.media || [],
    ): BiliVideoUploadDedupeRecords {
        const exact = this.resolveVideoUploadExactHashRecords(videoPaths, uploadMedia)
        if (!props?.article) {
            return {
                exact,
                videoMedia: [],
            }
        }

        const videoMedia = videoPaths
            .map((videoPath) => uploadMedia.find((item) => item.media_type === 'video' && item.path === videoPath))
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
        await this.ensureBuvidCookies()
        const effectiveConfig = this.getEffectiveConfig(props?.runtime_config) as any
        if (effectiveConfig.x_tiktok_teaser_mode === 'image' && isXTiktokTeaserArticle(props?.article)) {
            this.log?.info(
                `X TikTok teaser ${props?.article?.a_id} posts as a cover-image dynamic instead of a video upload for ${this.id}`,
            )
            return false
        }
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
            dedupeRecords = this.resolveVideoUploadDedupeRecords(candidate.videoPaths, props, media)
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
        const contentImages = (props.contentMedia || []).filter((item) => this.isDynamicImageMedia(item))
        if (contentImages.length > 0) {
            return contentImages
        }
        const cardImages = (props.cardMedia || []).filter((item) => this.isDynamicImageMedia(item))
        if (cardImages.length > 0) {
            return cardImages
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
        return this.api.fetchDynamicDetail(dynamicId)
    }

    private async fetchPublicDynamicDetail(dynamicId: string) {
        return this.api.fetchPublicDynamicDetail(dynamicId)
    }

    /**
     * Post-send visibility validation for a created photo dynamic.
     *
     * Bilibili's authenticated detail API returns the author's own draw dynamics with code 0 even
     * when the provider has hidden them from the public (content audit / review, observed as public
     * code 4101152 动态不可见). Without a public check, a hidden photo chunk was marked as fully
     * "sent" while viewers could not see it. This validates both the authenticated image count and
     * the anonymous (no-auth) visibility so a hidden chunk surfaces as a PartialForwarderSendError
     * instead of silently succeeding.
     */
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
                const publicDetail = await this.fetchPublicDynamicDetail(dynamicId)
                if (publicDetail.data?.code !== 0) {
                    throw new Error(
                        `Bilibili photo dynamic ${dynamicId} is not publicly visible: ` +
                            `public_code=${publicDetail.data?.code} message=${publicDetail.data?.message || 'unknown'}`,
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

    private getUploadQueueKey() {
        return `${this.bili_jct}:${this.sessdata}`
    }

    private getUploadQueueState() {
        const key = this.getUploadQueueKey()
        let state = biliUploadQueues.get(key)
        if (!state) {
            state = { chain: Promise.resolve(), lastUploadAt: 0, cooldownUntil: 0 }
            biliUploadQueues.set(key, state)
        }
        return state
    }

    private async runQueuedPhotoUpload<T>(upload: () => Promise<T>): Promise<T> {
        const state = this.getUploadQueueState()
        const run = async () => {
            const now = Date.now()
            const waitUntil = Math.max(state.lastUploadAt + this.photoUploadGapMs, state.cooldownUntil)
            if (waitUntil > now) {
                await delay(waitUntil - now)
            }
            try {
                const result = await upload()
                state.lastUploadAt = Date.now()
                return result
            } catch (error) {
                if (error instanceof BiliUploadThrottledError) {
                    state.cooldownUntil = Math.max(state.cooldownUntil, Date.now() + this.photoUploadCooldownMs)
                }
                throw error
            }
        }
        const queued = state.chain.then(run, run)
        state.chain = queued.catch(() => undefined)
        return queued
    }

    private shouldAllowMissingSummaryMedia(props?: SendProps) {
        const kind = String((props?.runtime_config as any)?.summary_card_task_kind || '')
        return kind === 'summary_card' || kind === 'summary_realtime_media' || kind === 'summary_single_native'
    }

    private markTextWithMissingMedia(texts: string[]) {
        if (texts.some((text) => text.includes(BILI_PHOTO_UPLOAD_MISSING_MARKER))) {
            return texts
        }
        if (texts.length === 0) {
            return [BILI_PHOTO_UPLOAD_MISSING_MARKER]
        }
        const [first, ...rest] = texts
        return [`${BILI_PHOTO_UPLOAD_MISSING_MARKER}\n${first}`, ...rest]
    }

    private async sendDynamicContent(texts: string[], props?: SendProps): Promise<any> {
        await this.ensureBuvidCookies()
        let { media } = props || {}
        media = media || []
        const _log = this.log
        const mediaCheckLevel = this.getMediaCheckLevel(props)
        const requireMedia = this.getEffectiveConfig(props?.runtime_config).require_media === true
        const effectiveConfig = this.getEffectiveConfig(props?.runtime_config) as any
        const normalizedAttachments = normalizeForwarderImageAttachments(media, {
            maxImageBytes: resolveForwarderImageMaxBytes(effectiveConfig),
            maxImageEdgePx: effectiveConfig.image_max_edge_px,
            maxImagePixels: effectiveConfig.image_max_pixels,
            log: _log,
        })
        media = normalizedAttachments.media
        try {
            // Upload photos one at a time with a gap: parallel upload_bfs bursts trip Bilibili's
            // per-account velocity control (-111), which previously killed the whole realtime send.
            const uploadedPhotos: Array<BiliUploadPhotoResponse | undefined> = []
            const allowMissingSummaryMedia = this.shouldAllowMissingSummaryMedia(props)
            let missingPhotoDueToThrottle = false
            for (const item of media) {
                if (!this.isDynamicImageMedia(item)) {
                    // video to gif
                    continue
                }
                try {
                    _log?.debug(`Uploading photo ${item.path}`)
                    const obj = await pRetry(
                        () => this.runQueuedPhotoUpload(() => this.uploadPhotoCached(item)),
                        {
                            retries: this.photoUploadRetries,
                            minTimeout: this.photoUploadRetryMinTimeoutMs,
                            factor: 2,
                            shouldRetry(error) {
                                if (error.originalError instanceof BiliUploadThrottledError) {
                                    return true
                                }
                                return !(error.originalError instanceof NonRetryableForwarderSendError)
                            },
                            onFailedAttempt(e) {
                                _log?.error(`Upload photo failed, retrying...: ${e.originalError.message}`)
                            },
                        },
                    )
                    uploadedPhotos.push(obj)
                } catch (e) {
                    if (isBiliUploadThrottledError(e) && allowMissingSummaryMedia) {
                        missingPhotoDueToThrottle = true
                        _log?.error(
                            `Upload photo ${item.path} throttled, sending summary text with ${BILI_PHOTO_UPLOAD_MISSING_MARKER}: ${
                                e instanceof Error ? e.message : String(e)
                            }`,
                        )
                        uploadedPhotos.push(undefined)
                        continue
                    }
                    if (e instanceof NonRetryableForwarderSendError) {
                        throw e
                    }
                    _log?.error(`Upload photo ${item.path} failed, skip this photo: ${e instanceof Error ? e.message : String(e)}`)
                    uploadedPhotos.push(undefined)
                }
            }
            let pics: Array<BiliImageUploaded> = uploadedPhotos
                .filter((i) => i !== undefined)
                .map((i) => this.normalizeUploadedPhoto(i))
                .filter((i): i is BiliImageUploaded => Boolean(i))
            const dynamicImageCount = media.filter((item) => this.isDynamicImageMedia(item)).length
            if (!allowMissingSummaryMedia || !missingPhotoDueToThrottle) {
                if ((mediaCheckLevel === 'loose' || requireMedia) && dynamicImageCount !== 0 && pics.length === 0) {
                    _log?.error(`No photos uploaded, throw error.`)
                    throw new NonRetryableForwarderSendError(`No photos uploaded, please check your bili_jct and sessdata.`)
                }
                if ((mediaCheckLevel === 'strict' || requireMedia) && dynamicImageCount !== pics.length) {
                    _log?.error(`Some photos upload failed.`)
                    throw new NonRetryableForwarderSendError(`Some photos upload failed, please check your bili_jct and sessdata.`)
                }
            }
            // TODO: more pics support
            const MAX_PICS = 9
            const picChunks = chunk(pics, MAX_PICS)

            const textChunks = missingPhotoDueToThrottle ? this.markTextWithMissingMedia(texts) : texts.length > 0 ? texts : []

            const n = Math.max(picChunks.length, textChunks.length)
            const _res = []

            for (let i = 0; i < n; i++) {
                const msgPics = picChunks[i] || [] // Type: BiliImageUploaded[]
                let text = textChunks[i] || (msgPics.length > 0 ? ' ' : '')

                // When photos are split across multiple dynamics, mark continuation so viewers
                // know more pictures follow (non-last chunk) or that this continues an earlier
                // chunk (non-first chunk). The first photo chunk carries the article text; later
                // photo chunks get the marker attached to their own text line.
                if (picChunks.length > 1 && msgPics.length > 0) {
                    const isFirstPhotoChunk = i === 0
                    const isLastPhotoChunk = i === picChunks.length - 1
                    const photoChunkText = text.trim() ? text : ''
                    if (isFirstPhotoChunk && !isLastPhotoChunk) {
                        text = photoChunkText ? `${photoChunkText}\n（图片未完，见下条）` : '（图片未完，见下条）'
                    } else if (!isFirstPhotoChunk && !isLastPhotoChunk) {
                        const prefix = photoChunkText ? `（接上条）\n${photoChunkText}` : '（接上条）'
                        text = `${prefix}\n（图片未完，见下条）`
                    } else if (!isFirstPhotoChunk && isLastPhotoChunk) {
                        text = photoChunkText ? `（接上条）\n${photoChunkText}` : '（接上条）'
                    }
                }

                _log?.debug(`Sending chunk ${i + 1}/${n}: text length ${text.length}, pics count ${msgPics.length}`)

                let res
                if (msgPics.length > 0) {
                    try {
                        res = await this.createDynamicWithRetry(
                            () => this.sendTextWithPhotos(text, msgPics),
                            `photo dynamic chunk ${i + 1}/${n}`,
                        )
                    } catch (error) {
                        throw this.buildDynamicCreateError(`photo dynamic chunk ${i + 1}/${n}`, _res, error)
                    }
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
                    try {
                        res = await this.createDynamicWithRetry(() => this.sendText(text), `text dynamic chunk ${i + 1}/${n}`)
                    } catch (error) {
                        throw this.buildDynamicCreateError(`text dynamic chunk ${i + 1}/${n}`, _res, error)
                    }
                    _res.push(res)
                }
            }
            return _res
        } finally {
            normalizedAttachments.cleanup()
        }
    }

    private async createDynamicWithRetry(
        create: () => Promise<BiliCreateDynamicResponse>,
        context: string,
    ): Promise<BiliCreateDynamicResponse> {
        return pRetry(
            async () => {
                const res = await create()
                // Centralized policy: -101 -> non-retryable auth, -111 -> throttle, else generic/ok.
                assertBiliResponseOk(res, `create ${context}`)
                return res
            },
            {
                retries: this.dynamicCreateRetries,
                minTimeout: this.dynamicCreateRetryMinTimeoutMs,
                factor: 2,
                shouldRetry(error) {
                    if (error.originalError instanceof BiliUploadVelocityError) {
                        return true
                    }
                    return !(error.originalError instanceof NonRetryableForwarderSendError)
                },
                onFailedAttempt: (error) => {
                    this.log?.warn(`Create ${context} failed, retrying...: ${error.originalError.message}`)
                },
            },
        )
    }

    private buildDynamicCreateError(context: string, partialResults: unknown[], error: unknown): Error {
        if (partialResults.length > 0) {
            return new PartialForwarderSendError(
                `Bilibili dynamic create failed for ${context} after earlier chunks posted`,
                partialResults,
                context,
                error,
            )
        }
        if (error instanceof NonRetryableForwarderSendError) {
            return error
        }
        const message = error instanceof Error ? error.message : String(error)
        return new NonRetryableForwarderSendError(`Bilibili dynamic create failed for ${context}: ${message}`)
    }

    private async uploadPhoto(path: string) {
        const { rawResponse, data } = await this.api.uploadPhoto(path)
        this.log?.debug(`Upload photo response: ${JSON.stringify(rawResponse.data)}`)
        return data as BiliUploadPhotoResponse
    }

    private async uploadPhotoCached(item: { path: string; content_hash?: string }): Promise<BiliUploadPhotoResponse> {
        const identity = item.content_hash || this.fileIdentityFallback(item.path)
        const cached = identity ? this.uploadResultCache.get(identity) : undefined
        if (cached && cached.at > Date.now() - BiliForwarder.UPLOAD_RESULT_TTL_MS) {
            this.log?.debug(`Reusing cached Bilibili upload result for ${identity}`)
            return cached.uploaded
        }
        const uploaded = await this.uploadPhoto(item.path)
        if (identity) {
            this.uploadResultCache.set(identity, { at: Date.now(), uploaded })
            if (this.uploadResultCache.size > BiliForwarder.UPLOAD_RESULT_CACHE_LIMIT) {
                const oldest = Array.from(this.uploadResultCache.entries()).sort((a, b) => a[1].at - b[1].at)[0]
                if (oldest) {
                    this.uploadResultCache.delete(oldest[0])
                }
            }
        }
        return uploaded
    }

    private fileIdentityFallback(path: string) {
        try {
            const stat = fs.statSync(path)
            return `size:${stat.size}:mtime:${stat.mtimeMs}`
        } catch {
            return null
        }
    }

    private async sendText(text: string) {
        return this.api.createTextDynamic(text)
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
        return this.api.createPhotoDynamic(text, pics)
    }
}

export { BiliForwarder, BiliUploadThrottledError }
