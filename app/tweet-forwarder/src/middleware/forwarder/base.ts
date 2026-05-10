import { RETRY_LIMIT } from '@/config'
import type { Article } from '@/db'
import {
    type ForwardTarget,
    type ForwardTargetPlatformCommonConfig,
    ForwardTargetPlatformEnum,
} from '@/types/forwarder'
import { BaseCompatibleModel } from '@/utils/base'
import { Logger } from '@idol-bbq-utils/log'
import { extractTextHeadline } from '@idol-bbq-utils/render'
import { Platform, type MediaType } from '@idol-bbq-utils/spider/types'
import { pRetry } from '@idol-bbq-utils/utils'
import {
    MiddlewarePipeline,
    TimeFilterMiddleware,
    KeywordFilterMiddleware,
    BlockRuleMiddleware,
    TextReplaceMiddleware,
    TextChunkMiddleware,
    type ForwarderContext,
    type ForwarderMiddleware,
} from './pipeline'

type MediaFile = {
    media_type: MediaType
    path: string
    sourceArticleId?: string
    sourceUserId?: string
}

interface PreparedBatchItem {
    article?: Article
    timestamp?: number
    text: string
    media: MediaFile[]
    cardMedia: MediaFile[]
    contentMedia: MediaFile[]
    unitCount: number
    sourceImageCount: number
    hasVideo: boolean
}

interface MediaBatchConfig {
    threshold: number
    breakoutImages: number
    separateCardMedia: boolean
}

export interface SendProps {
    media?: MediaFile[]
    cardMedia?: MediaFile[]
    contentMedia?: MediaFile[]
    timestamp?: number
    runtime_config?: ForwardTargetPlatformCommonConfig
    article?: Article
    forceSend?: boolean
}

abstract class BaseForwarder extends BaseCompatibleModel {
    static _PLATFORM = ForwardTargetPlatformEnum.None
    log?: Logger
    id: string
    protected config: ForwardTarget['cfg_platform']
    protected pipeline: MiddlewarePipeline
    protected readonly timeFilterMiddleware = new TimeFilterMiddleware()
    protected readonly keywordFilterMiddleware = new KeywordFilterMiddleware()
    protected readonly blockRuleMiddleware = new BlockRuleMiddleware()
    private pendingMediaBatches: Map<
        string,
        { config: MediaBatchConfig; items: PreparedBatchItem[]; unitCount: number }
    > = new Map()

    constructor(config: ForwardTarget['cfg_platform'], id: string, log?: Logger) {
        super()
        this.log = log
        this.config = config
        this.id = String(id)
        this.pipeline = this.createDefaultPipeline()
    }

    async init(): Promise<void> {
        this.log = this.log?.child({ service: 'Forwarder', subservice: this.NAME, label: this.id })
        this.log?.debug(`loaded with config ${this.config}`)
    }

    async drop(..._args: any[]): Promise<void> {
        await this.flushPendingMediaBatches()
    }

    protected createDefaultPipeline(): MiddlewarePipeline {
        return new MiddlewarePipeline()
            .use(this.timeFilterMiddleware)
            .use(this.keywordFilterMiddleware)
            .use(this.blockRuleMiddleware)
            .use(new TextReplaceMiddleware())
            .use(new TextChunkMiddleware(this.getTextLimit()))
    }

    protected createForceSendPipeline(): MiddlewarePipeline {
        return new MiddlewarePipeline()
            .use(new TextReplaceMiddleware())
            .use(new TextChunkMiddleware(this.getTextLimit()))
    }

    protected getTextLimit(): number {
        return 1000
    }

    public getEffectiveConfig(
        runtime_config?: ForwardTargetPlatformCommonConfig,
    ): ForwardTargetPlatformCommonConfig {
        return {
            ...this.config,
            ...runtime_config,
        }
    }

    public async check_blocked(text: string, props: SendProps): Promise<boolean> {
        if (props?.forceSend) {
            return false
        }
        const { timestamp, runtime_config, article } = props || {}
        const mergedConfig = this.getEffectiveConfig(runtime_config)

        const context: ForwarderContext = {
            text,
            article,
            media: props?.media,
            timestamp,
            config: mergedConfig,
            metadata: new Map(),
            aborted: false,
        }

        const blockCheckPipeline = new MiddlewarePipeline()
            .use(this.timeFilterMiddleware)
            .use(this.keywordFilterMiddleware)
            .use(this.blockRuleMiddleware)

        try {
            const result = await blockCheckPipeline.execute(context)
            return !result
        } catch {
            return true
        }
    }

    protected minInterval: number = 0
    private lastSentTime: number = 0

    public async send(text: string, props?: SendProps): Promise<any> {
        const { runtime_config } = props || {}
        const mergedConfig = this.getEffectiveConfig(runtime_config)

        const context: ForwarderContext = {
            text,
            article: props?.article,
            media: props?.media,
            timestamp: props?.timestamp,
            config: mergedConfig,
            metadata: new Map(),
            aborted: false,
        }

        const pipeline = props?.forceSend ? this.createForceSendPipeline() : this.pipeline
        const shouldSend = await pipeline.execute(context)

        if (!shouldSend) {
            this.log?.warn(context.abortReason || 'Message blocked by middleware')
            return Promise.resolve()
        }

        const chunks = (context.metadata.get('chunks') as string[]) || [context.text]
        const handledByBatch = await this.maybeHandleMediaBatch(chunks, props, mergedConfig)
        if (handledByBatch) {
            this.blockRuleMiddleware.commitPending(context)
            return
        }

        await this.sendPrepared(chunks, props)
        this.blockRuleMiddleware.commitPending(context)
    }

    protected async sendPrepared(texts: string[], props?: SendProps): Promise<any> {
        const normalizedTexts = texts.filter((item) => item !== undefined)
        const textLength = normalizedTexts.join('\n').length
        const _log = this.log

        _log?.debug(`trying to send prepared payload with text length ${textLength}`)

        if (this.minInterval > 0) {
            const now = Date.now()
            const timeSinceLastSend = now - this.lastSentTime
            if (timeSinceLastSend < this.minInterval) {
                const waitTime = this.minInterval - timeSinceLastSend
                _log?.debug(`Rate limit hit, waiting ${waitTime}ms`)
                await new Promise((resolve) => setTimeout(resolve, waitTime))
            }
        }

        this.lastSentTime = Date.now()

        await pRetry(() => this.realSend(normalizedTexts, props), {
            retries: RETRY_LIMIT,
            onFailedAttempt(e) {
                _log?.error(`send texts failed, retrying...: ${e.originalError.message}`)
            },
        })
    }

    private async flushPendingMediaBatches() {
        const batches = Array.from(this.pendingMediaBatches.values())
        this.pendingMediaBatches.clear()
        for (const batch of batches) {
            if (batch.items.length === 0) {
                continue
            }
            await this.sendPreparedBatchItems(batch.items, batch.config)
        }
    }

    private resolveMediaBatchConfig(config: ForwardTargetPlatformCommonConfig): MediaBatchConfig | null {
        const rawThreshold = Number(config.media_batch_threshold ?? 0)
        if (!Number.isFinite(rawThreshold) || rawThreshold < 2) {
            return null
        }

        const rawBreakout = Number(config.media_batch_breakout_images ?? 3)
        return {
            threshold: Math.max(2, Math.floor(rawThreshold)),
            breakoutImages: Number.isFinite(rawBreakout) && rawBreakout >= 1 ? Math.floor(rawBreakout) : 3,
            separateCardMedia: config.separate_card_media === true,
        }
    }

    private buildMediaBatchKey(config: MediaBatchConfig) {
        return JSON.stringify(config)
    }

    private isPhotoLikeMedia(item: MediaFile) {
        return item.media_type === 'photo' || item.media_type === 'video_thumbnail'
    }

    private normalizeUserId(userId: unknown) {
        const normalized = String(userId || '')
            .trim()
            .replace(/^@+/, '')
        return normalized || null
    }

    private getAllowedListUserIds(article?: Article) {
        const listContext = article?.extra?.data as Record<string, unknown> | undefined
        const rawUserIds = listContext?.list_context
        if (!rawUserIds || typeof rawUserIds !== 'object') {
            return null
        }

        const userIds = (rawUserIds as Record<string, unknown>).user_ids
        if (!Array.isArray(userIds)) {
            return null
        }

        const normalizedUserIds = userIds
            .map((userId) => this.normalizeUserId(userId))
            .filter((userId): userId is string => Boolean(userId))
        return normalizedUserIds.length > 0 ? new Set(normalizedUserIds) : null
    }

    private shouldCountSourceMedia(item: MediaFile, article?: Article, allowedListUserIds?: Set<string> | null) {
        if (!this.isPhotoLikeMedia(item)) {
            return false
        }

        const rootArticleId = article?.a_id?.trim()
        if (!rootArticleId || !item.sourceArticleId || item.sourceArticleId === rootArticleId) {
            return true
        }

        if (!allowedListUserIds || allowedListUserIds.size === 0) {
            return true
        }

        const sourceUserId = this.normalizeUserId(item.sourceUserId)
        return Boolean(sourceUserId && allowedListUserIds.has(sourceUserId))
    }

    private createPreparedBatchItem(texts: string[], props?: SendProps): PreparedBatchItem {
        const cardMedia = props?.cardMedia || []
        const media = props?.media || []
        const cardPaths = new Set(cardMedia.map((item) => item.path))
        const contentMedia = props?.contentMedia || media.filter((item) => !cardPaths.has(item.path))
        const normalizedMedia = media.length > 0 ? media : [...cardMedia, ...contentMedia]
        const text = texts.filter(Boolean).join('\n')
        const allowedListUserIds = this.getAllowedListUserIds(props?.article)
        const sourceImageCount = contentMedia.filter((item) =>
            this.shouldCountSourceMedia(item, props?.article, allowedListUserIds),
        ).length
        const cardImageCount = cardMedia.filter((item) => this.isPhotoLikeMedia(item)).length
        const hasVideo =
            normalizedMedia.some((item) => item.media_type === 'video') ||
            contentMedia.some((item) => item.media_type === 'video')
        const textUnitCount = text.trim() ? 1 : 0

        return {
            article: props?.article,
            timestamp: props?.timestamp,
            text,
            media: normalizedMedia,
            cardMedia,
            contentMedia,
            unitCount: sourceImageCount + cardImageCount + textUnitCount,
            sourceImageCount,
            hasVideo,
        }
    }

    private buildTextChunksFromItems(items: PreparedBatchItem[]) {
        const combinedText = items
            .map((item) => item.text.trim())
            .filter(Boolean)
            .join('\n\n----------\n\n')

        return this.chunkText(combinedText)
    }

    private chunkText(text: string) {
        const normalized = text.trim()
        if (!normalized) {
            return [] as string[]
        }

        const basicTextLimit = this.getTextLimit()
        if (normalized.length <= basicTextLimit) {
            return [normalized]
        }

        const fallback = extractTextHeadline(normalized, Math.min(120, basicTextLimit))
        return [fallback || normalized.slice(0, basicTextLimit).trimEnd()]
    }

    private async sendPreparedBatchItems(items: PreparedBatchItem[], config: MediaBatchConfig) {
        const firstItem = items[0]
        if (!firstItem) {
            return
        }

        const baseProps: SendProps = {
            article: firstItem.article,
            timestamp: firstItem.timestamp,
        }

        if (!config.separateCardMedia) {
            await this.sendPrepared(this.buildTextChunksFromItems(items), {
                ...baseProps,
                media: items.flatMap((item) => item.media),
            })
            return
        }

        const cardItems = items.filter((item) => item.cardMedia.length > 0)
        const normalItems = items.filter((item) => item.cardMedia.length === 0)
        const splitMediaItems = items.filter((item) => item.cardMedia.length > 0 && item.contentMedia.length > 0)

        if (cardItems.length > 0) {
            await this.sendPrepared(this.buildTextChunksFromItems(cardItems), {
                ...baseProps,
                media: cardItems.flatMap((item) => item.cardMedia),
            })
        }

        if (normalItems.length > 0) {
            await this.sendPrepared(this.buildTextChunksFromItems(normalItems), {
                ...baseProps,
                media: normalItems.flatMap((item) => item.media),
            })
        }

        if (splitMediaItems.length > 0) {
            await this.sendPrepared([], {
                ...baseProps,
                media: splitMediaItems.flatMap((item) => item.contentMedia),
            })
        }
    }

    private async maybeHandleMediaBatch(
        texts: string[],
        props: SendProps | undefined,
        mergedConfig: ForwardTargetPlatformCommonConfig,
    ) {
        if (props?.forceSend) {
            return false
        }

        const batchConfig = this.resolveMediaBatchConfig(mergedConfig)
        if (!batchConfig) {
            return false
        }

        if (!props?.article || ![Platform.X, Platform.Twitter].includes(props.article.platform)) {
            return false
        }

        const item = this.createPreparedBatchItem(texts, props)
        if (item.unitCount === 0) {
            return false
        }

        if (item.hasVideo || item.sourceImageCount >= batchConfig.breakoutImages) {
            this.log?.debug(
                `Bypassing pending media batch for ${this.id}: source images=${item.sourceImageCount}, hasVideo=${item.hasVideo}`,
            )
            await this.sendPreparedBatchItems([item], batchConfig)
            return true
        }

        const batchKey = this.buildMediaBatchKey(batchConfig)
        const batch = this.pendingMediaBatches.get(batchKey) || {
            config: batchConfig,
            items: [] as PreparedBatchItem[],
            unitCount: 0,
        }

        batch.items.push(item)
        batch.unitCount += item.unitCount
        this.pendingMediaBatches.set(batchKey, batch)
        this.log?.debug(
            `Queued media batch item for ${this.id}: +${item.unitCount}, pending=${batch.unitCount}/${batchConfig.threshold}`,
        )

        if (batch.unitCount >= batchConfig.threshold) {
            this.pendingMediaBatches.delete(batchKey)
            await this.sendPreparedBatchItems(batch.items, batch.config)
        }

        return true
    }

    protected abstract realSend(texts: string[], props?: SendProps): Promise<any>
}

abstract class Forwarder extends BaseForwarder {
    protected BASIC_TEXT_LIMIT = 1000

    constructor(config: ForwardTarget['cfg_platform'], id: string, log?: Logger) {
        super(config, id, log)
        if (this.config?.replace_regex) {
            try {
                this.log?.debug(`checking config replace_regex: ${JSON.stringify(this.config.replace_regex)}`)
                this.validateReplaceRegex(this.config.replace_regex)
            } catch (e) {
                this.log?.error(`replace regex is invalid for reason: ${e}`)
                throw e
            }
        }
    }

    protected override createDefaultPipeline(): MiddlewarePipeline {
        return new MiddlewarePipeline()
            .use(this.timeFilterMiddleware)
            .use(this.keywordFilterMiddleware)
            .use(this.blockRuleMiddleware)
            .use(new TextReplaceMiddleware())
            .use(new TextChunkMiddleware(this.BASIC_TEXT_LIMIT))
    }

    protected override getTextLimit(): number {
        return this.BASIC_TEXT_LIMIT
    }

    private validateReplaceRegex(regexps: ForwardTarget['cfg_platform']['replace_regex']): void {
        if (!regexps) return

        if (typeof regexps === 'string') {
            new RegExp(regexps, 'g')
            return
        }

        if (Array.isArray(regexps)) {
            if (regexps.length > 0 && Array.isArray(regexps[0])) {
                for (const [reg] of regexps as Array<[string, string]>) {
                    new RegExp(reg, 'g')
                }
            } else {
                new RegExp((regexps as [string, string])[0], 'g')
            }
        }
    }
}

export { BaseForwarder, Forwarder }
