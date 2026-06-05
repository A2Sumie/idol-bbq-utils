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
    OutboundSendDryRunError,
    captureOutboundSend,
    isNonLiveOutboundSendMode,
    resolveOutboundSendMode,
    type OutboundCapturePayload,
    type OutboundSendDryRunDetails,
} from '@/services/outbound-send-mode'
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

export type MediaFile = {
    media_type: MediaType
    path: string
    sourceArticleId?: string
    sourceUserId?: string
    content_hash?: string
    size_bytes?: number
}

interface PreparedBatchItem {
    article?: Article
    outboundKey?: string
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

export interface DiscardedMediaBatchItem {
    article?: Article
    outboundKey?: string
    unitCount: number
    sourceImageCount: number
    hasVideo: boolean
}

export interface DiscardedMediaBatch {
    batchKey: string
    pendingUnits: number
    threshold: number
    items: DiscardedMediaBatchItem[]
}

export interface SendProps {
    media?: MediaFile[]
    cardMedia?: MediaFile[]
    contentMedia?: MediaFile[]
    timestamp?: number
    runtime_config?: ForwardTargetPlatformCommonConfig
    article?: Article
    forceSend?: boolean
    bypassMediaBatch?: boolean
    outboundKey?: string
}

export type ForwarderSendResult =
    | {
          status: 'sent'
          providerResult?: unknown
          batchArticles?: Article[]
          batchOutbounds?: string[]
      }
    | {
          status: 'queued'
          reason: 'media_batch'
          batchKey: string
          pendingUnits: number
          threshold: number
          outboundKey?: string
      }
    | {
          status: 'blocked'
          reason: string
      }
    | {
          status: 'dry_run'
          reason: string
          details: OutboundSendDryRunDetails
}

export function isForwarderSendResult(value: unknown): value is ForwarderSendResult {
    if (!value || typeof value !== 'object') {
        return false
    }
    return ['sent', 'queued', 'blocked', 'dry_run'].includes(String((value as { status?: unknown }).status))
}

export function isForwarderSentResult(value: unknown): value is Extract<ForwarderSendResult, { status: 'sent' }> {
    return isForwarderSendResult(value) && value.status === 'sent'
}

export function getForwarderProviderResult(value: unknown) {
    return isForwarderSentResult(value) ? value.providerResult : value
}

class PartialForwarderSendError extends Error {
    readonly partialResults: unknown[]
    readonly failedSegment: string
    readonly originalError: unknown

    constructor(message: string, partialResults: unknown[], failedSegment: string, originalError: unknown) {
        super(message)
        this.name = 'PartialForwarderSendError'
        this.partialResults = partialResults
        this.failedSegment = failedSegment
        this.originalError = originalError
    }
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
        const discarded = this.drainPendingMediaBatches()
        const discardedItems = discarded.reduce((sum, batch) => sum + batch.items.length, 0)
        if (discardedItems > 0) {
            this.log?.warn(
                `Discarded ${discardedItems} pending media batch item(s) for ${this.id} during drop without visible send`,
            )
        }
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

    public getEffectiveConfig(runtime_config?: ForwardTargetPlatformCommonConfig): ForwardTargetPlatformCommonConfig {
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

    public async send(text: string, props?: SendProps): Promise<ForwarderSendResult> {
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
            const reason = context.abortReason || 'Message blocked by middleware'
            this.log?.warn(reason)
            return { status: 'blocked', reason }
        }

        try {
            const chunks = (context.metadata.get('chunks') as string[]) || [context.text]
            const batchResult = await this.maybeHandleMediaBatch(chunks, props, mergedConfig)
            if (batchResult) {
                this.blockRuleMiddleware.commitPending(context)
                return batchResult
            }

            const result = await this.sendPrepared(chunks, props)
            this.blockRuleMiddleware.commitPending(context)
            return { status: 'sent', providerResult: result }
        } catch (error) {
            if (error instanceof OutboundSendDryRunError) {
                this.log?.warn(error.message)
                return {
                    status: 'dry_run',
                    reason: error.message,
                    details: error.details,
                }
            }
            throw error
        }
    }

    protected async sendPrepared(texts: string[], props?: SendProps): Promise<any> {
        const normalizedTexts = texts.filter((item) => item !== undefined)
        const textLength = normalizedTexts.join('\n').length
        const _log = this.log

        _log?.debug(`trying to send prepared payload with text length ${textLength}`)

        await this.assertActualSendAllowed(normalizedTexts, props)

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

        return await pRetry(() => this.realSend(normalizedTexts, props), {
            retries: RETRY_LIMIT,
            shouldRetry(error) {
                return !(error.originalError instanceof PartialForwarderSendError)
            },
            onFailedAttempt(e) {
                _log?.error(`send texts failed, retrying...: ${e.originalError.message}`)
            },
        })
    }

    public drainPendingMediaBatches(): DiscardedMediaBatch[] {
        const batches = Array.from(this.pendingMediaBatches.entries()).map(([batchKey, batch]) => ({
            batchKey,
            pendingUnits: batch.unitCount,
            threshold: batch.config.threshold,
            items: batch.items.map((item) => ({
                article: item.article,
                outboundKey: item.outboundKey,
                unitCount: item.unitCount,
                sourceImageCount: item.sourceImageCount,
                hasVideo: item.hasVideo,
            })),
        }))
        this.pendingMediaBatches.clear()
        return batches
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
            outboundKey: props?.outboundKey,
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

    private async sendPreparedBatchItems(
        items: PreparedBatchItem[],
        config: MediaBatchConfig,
    ): Promise<Extract<ForwarderSendResult, { status: 'sent' }>> {
        const firstItem = items[0]
        if (!firstItem) {
            return { status: 'sent', providerResult: undefined, batchArticles: [] }
        }

        const baseProps: SendProps = {
            article: firstItem.article,
            timestamp: firstItem.timestamp,
        }
        const batchArticles = items
            .map((item) => item.article)
            .filter((article): article is Article => Boolean(article))
        const batchOutbounds = items.map((item) => item.outboundKey).filter((key): key is string => Boolean(key))
        const providerResults: unknown[] = []

        if (!config.separateCardMedia) {
            providerResults.push(
                await this.sendPrepared(this.buildTextChunksFromItems(items), {
                    ...baseProps,
                    media: items.flatMap((item) => item.media),
                }),
            )
            return {
                status: 'sent',
                providerResult: providerResults.length === 1 ? providerResults[0] : providerResults,
                batchArticles,
                batchOutbounds,
            }
        }

        const cardItems = items.filter((item) => item.cardMedia.length > 0)
        const normalItems = items.filter((item) => item.cardMedia.length === 0)
        const splitMediaItems = items.filter((item) => item.cardMedia.length > 0 && item.contentMedia.length > 0)

        if (cardItems.length > 0) {
            providerResults.push(
                await this.sendPrepared(this.buildTextChunksFromItems(cardItems), {
                    ...baseProps,
                    media: cardItems.flatMap((item) => item.cardMedia),
                }),
            )
        }

        if (normalItems.length > 0) {
            providerResults.push(
                await this.sendPrepared(this.buildTextChunksFromItems(normalItems), {
                    ...baseProps,
                    media: normalItems.flatMap((item) => item.media),
                }),
            )
        }

        if (splitMediaItems.length > 0) {
            providerResults.push(
                await this.sendPrepared([], {
                    ...baseProps,
                    media: splitMediaItems.flatMap((item) => item.contentMedia),
                }),
            )
        }
        return {
            status: 'sent',
            providerResult: providerResults.length === 1 ? providerResults[0] : providerResults,
            batchArticles,
            batchOutbounds,
        }
    }

    private async maybeHandleMediaBatch(
        texts: string[],
        props: SendProps | undefined,
        mergedConfig: ForwardTargetPlatformCommonConfig,
    ): Promise<ForwarderSendResult | null> {
        if (props?.forceSend || props?.bypassMediaBatch || isNonLiveOutboundSendMode()) {
            return null
        }

        const batchConfig = this.resolveMediaBatchConfig(mergedConfig)
        if (!batchConfig) {
            return null
        }

        if (!props?.article || ![Platform.X, Platform.Twitter].includes(props.article.platform)) {
            return null
        }

        const item = this.createPreparedBatchItem(texts, props)
        if (item.unitCount === 0) {
            return null
        }

        if (item.hasVideo || item.sourceImageCount >= batchConfig.breakoutImages) {
            this.log?.debug(
                `Bypassing pending media batch for ${this.id}: source images=${item.sourceImageCount}, hasVideo=${item.hasVideo}`,
            )
            return await this.sendPreparedBatchItems([item], batchConfig)
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
            return await this.sendPreparedBatchItems(batch.items, batch.config)
        }

        return {
            status: 'queued',
            reason: 'media_batch',
            batchKey,
            pendingUnits: batch.unitCount,
            threshold: batchConfig.threshold,
            outboundKey: item.outboundKey,
        }
    }

    private summarizeCaptureMedia(items: MediaFile[]) {
        return items.map((item) => ({
            media_type: item.media_type,
            path: item.path,
            file_name: item.path.split('/').pop() || item.path,
            ...(item.sourceArticleId ? { source_article_id: item.sourceArticleId } : {}),
            ...(item.sourceUserId ? { source_user_id: item.sourceUserId } : {}),
            ...((item as { content_hash?: string }).content_hash
                ? { content_hash: (item as { content_hash?: string }).content_hash }
                : {}),
            ...((item as { size_bytes?: number }).size_bytes
                ? { size_bytes: (item as { size_bytes?: number }).size_bytes }
                : {}),
        }))
    }

    private buildOutboundDryRunDetails(
        sendMode: ReturnType<typeof resolveOutboundSendMode>,
        texts: string[],
        props?: SendProps,
    ): OutboundSendDryRunDetails {
        const media = props?.media || []
        const cardMedia = props?.cardMedia || []
        const contentMedia = props?.contentMedia || []
        const article = props?.article
        return {
            send_mode: sendMode,
            target_id: this.id,
            forwarder: this.NAME,
            text_count: texts.length,
            text_length: texts.join('\n').length,
            media_count: media.length,
            card_media_count: cardMedia.length,
            content_media_count: contentMedia.length,
            ...(article ? { article_key: `${article.platform}:${article.a_id}` } : {}),
            ...(props?.outboundKey ? { outbound_key: props.outboundKey } : {}),
        }
    }

    private buildOutboundCapturePayload(
        details: OutboundSendDryRunDetails,
        texts: string[],
        props?: SendProps,
    ): OutboundCapturePayload {
        const media = props?.media || []
        const cardMedia = props?.cardMedia || []
        const contentMedia = props?.contentMedia || []
        const article = props?.article
        return {
            schema_version: 1,
            send_mode: 'capture',
            captured_at: new Date().toISOString(),
            target_id: this.id,
            forwarder: this.NAME,
            text_count: details.text_count,
            text_length: details.text_length,
            texts,
            media: this.summarizeCaptureMedia(media),
            card_media: this.summarizeCaptureMedia(cardMedia),
            content_media: this.summarizeCaptureMedia(contentMedia),
            ...(article
                ? {
                      article: {
                          id: article.id,
                          a_id: article.a_id,
                          platform: String(article.platform),
                          ...(article.url ? { url: article.url } : {}),
                      },
                      article_key: details.article_key,
                  }
                : {}),
            ...(details.outbound_key ? { outbound_key: details.outbound_key } : {}),
        }
    }

    private async assertActualSendAllowed(texts: string[], props?: SendProps) {
        const sendMode = resolveOutboundSendMode()
        if (sendMode === 'live') {
            return
        }

        const details = this.buildOutboundDryRunDetails(sendMode, texts, props)
        if (sendMode === 'capture') {
            details.capture_result = await captureOutboundSend(this.buildOutboundCapturePayload(details, texts, props))
        }
        throw new OutboundSendDryRunError(details)
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

export { BaseForwarder, Forwarder, OutboundSendDryRunError, PartialForwarderSendError, resolveOutboundSendMode }
