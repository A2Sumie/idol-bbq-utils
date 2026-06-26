import { Logger } from '@idol-bbq-utils/log'
import { spiderRegistry } from '@idol-bbq-utils/spider'
import { CronJob } from 'cron'
import EventEmitter from 'events'
import { BaseCompatibleModel, sanitizeWebsites, TaskScheduler } from '@/utils/base'
import type { AppConfig, Processor } from '@/types'
import { Platform, type MediaType, type TaskType } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import type { Article, ArticleWithId, DBFollows } from '@/db'
import {
    BaseForwarder,
    type DiscardedMediaBatch,
    type ForwarderSendResult,
    getForwarderProviderResult,
    isForwarderSentResult,
    PartialForwarderSendError,
} from '@/middleware/forwarder/base'
import { type Media, type MediaTool, MediaToolEnum } from '@/types/media'
import type { ForwardTargetPlatformCommonConfig, Forwarder as RealForwarder } from '@/types/forwarder'
import { getForwarder } from '@/middleware/forwarder'
import crypto from 'crypto'
import { RenderService, type RenderResult } from '@/services/render-service'
import { BaseProcessor, PROCESSOR_ERROR_FALLBACK } from '@/middleware/processor/base'
import { processorRegistry } from '@/middleware/processor'
import {
    extractArticleHeadline,
    followsToText,
    formatArticleHeaderLine,
    formatArticleSourceActionAttribution,
    formatArticleTimeToken,
    formatArticleUserId,
} from '@idol-bbq-utils/render'
import dayjs from 'dayjs'
import { cloneDeep, orderBy } from 'lodash'
import {
    getWebsitePhotoBatchKey,
    isWebsitePhotoAlbumArticle,
    normalizeWebsitePhotoArticles,
} from '@/utils/website-photo'
import { normalizeCronSecond } from '@/utils/cron'
import {
    articleKey,
    articleOutboundKey,
    hashValue,
    isOutboundSuppressedCompletionStatus,
    isOutboundVisibleCompletionStatus,
    payloadHash,
    providerCode,
    routeKey,
    summarizeProviderResult,
    syntheticOutboundKey,
    targetRouteKey,
} from '@/services/outbound-message-service'
import { resolveSummaryCardConfig, type ResolvedSummaryCardConfig } from '@/services/summary-card-policy'
import { isNonLiveOutboundSendMode } from '@/services/outbound-send-mode'
import { pRetry } from '@idol-bbq-utils/utils'
import { RETRY_LIMIT } from '@/config'

type CrawlerConfig = NonNullable<AppConfig['crawlers']>[number]
type ForwarderTemplate = NonNullable<AppConfig['forwarders']>[number]
type ArticleForwarderDispatch = {
    article: ArticleWithId
    to: Array<ForwardTargetInstanceWithRuntimeConfig>
}
type ForwardingPath = {
    routeKey: string
    formatterId?: string
    formatterConfig: Forwarder['cfg_forwarder']
    targets: Array<ForwardTargetInstanceWithRuntimeConfig>
    source: 'graph' | 'inline'
    formatterName: string
}
type TagDigestEvent = {
    timestamp: number
    authorKey: string
}
type TagDigestState = {
    events: Array<TagDigestEvent>
    digestUntil: number
    displayTag: string
}
type TagDigestGroup = {
    tag: string
    articles: Array<ArticleWithId>
}
type SummaryCardQueueItem = {
    article: ArticleWithId
    queuedAt: number
    cardSourceMediaFiles: Array<RenderResult['originalMediaFiles'][number]>
    originalMediaFiles: Array<RenderResult['originalMediaFiles'][number]>
    digestTags: Array<string>
}
type SummaryCardQueue = {
    routeKey: string
    windowId?: number
    target: BaseForwarder
    runtime_config?: ForwardTargetPlatformCommonConfig
    config: ResolvedSummaryCardConfig
    items: Map<number, SummaryCardQueueItem>
    firstQueuedAt: number
    lastQueuedAt: number
    windowStart?: number
    windowEnd?: number
}
type SummaryCardGroup = {
    kind: 'storm' | 'thread'
    label: string
    items: SummaryCardQueueItem[]
}
type SummaryCardMediaUsage = Map<string, number>
type SummaryCardTextMode = 'default' | 'original' | 'translated'
type SummaryCardSendTextItemParts = {
    actor: string
    action: string
    ref: string
}
type SummaryCardRealtimeMediaResult = {
    hadMedia: boolean
    handled: boolean
    visibleMediaSent: boolean
    skippedDuplicate: boolean
}
type RenderedMediaFile = RenderResult['originalMediaFiles'][number]
type MediaVisibilityDuplicateBehavior = 'skip' | 'text_only'
type ResolvedMediaVisibilityPolicy = {
    windowSeconds: number
    maxVisible: number
    duplicateBehavior: MediaVisibilityDuplicateBehavior
}
type MediaVisibilityResult = {
    policy: ResolvedMediaVisibilityPolicy | null
    originalCount: number
    visibleFiles: RenderedMediaFile[]
    hiddenFiles: RenderedMediaFile[]
    visibleHashes: Set<string>
    hiddenHashes: Set<string>
    visibleClaims: Array<{
        platform: string
        hash: string
        a_id: string
    }>
}
type ImmediateXLinkSendOptions = {
    crawlerName?: string
    targetIds: Array<string>
    processorId?: string
    badgeLabel?: string
}

const DEFAULT_TAG_DIGEST_THRESHOLD = 3
const DEFAULT_TAG_DIGEST_MIN_AUTHORS = 2
const DEFAULT_TAG_DIGEST_DETECTION_WINDOW_SECONDS = 5 * 60
const DEFAULT_TAG_DIGEST_WINDOW_SECONDS = 20 * 60
const DEFAULT_COLLAPSE_FORWARDED_REF_WINDOW_SECONDS = 18 * 3600
const DEFAULT_SUMMARY_CARD_MAX_EMBEDDED_MEDIA = 12
const DEFAULT_SUMMARY_CARD_STALE_GRACE_SECONDS = 3600
const DEFAULT_SUMMARY_CARD_FLUSHES_PER_TICK = 1
const DEFAULT_BATCH_AGGREGATION_CRON = '0 * * * *'
const DEFAULT_BATCH_AGGREGATION_WINDOW_SECONDS = 3600
const HIGH_REALTIME_GROUP_IDS = new Set(['742435777'])
const HASHTAG_REGEX = /[#＃][\p{L}\p{N}_ー一-龯ぁ-んァ-ヶ]+/gu

function sortUnique(values: Array<string>) {
    return Array.from(new Set(values.filter(Boolean))).sort((a, b) => a.localeCompare(b))
}

function toErrorMessage(error: unknown) {
    return error instanceof Error ? error.message : String(error)
}

function uniquePreserveOrder(values: Array<string>) {
    return Array.from(new Set(values.filter(Boolean)))
}

function articleTranslationIdentityKey(article: ArticleWithId | Article) {
    const stableId = String((article as any).id ?? '').trim()
    if (stableId) {
        return `${article.platform}:id:${stableId}`
    }
    return articleKey(article as ArticleWithId)
}

function stripArticleTranslations<T extends ArticleWithId | Article>(article: T): T {
    const cloned = cloneDeep(article)
    const visit = (currentArticle?: ArticleWithId | Article | null) => {
        if (!currentArticle) {
            return
        }

        currentArticle.translation = null
        currentArticle.translated_by = null

        if (Array.isArray(currentArticle.media)) {
            currentArticle.media = currentArticle.media.map((mediaItem) => {
                const nextMedia = { ...(mediaItem as any) }
                delete nextMedia.translation
                delete nextMedia.translated_by
                return nextMedia
            }) as any
        }

        if (currentArticle.extra && typeof currentArticle.extra === 'object') {
            const nextExtra = { ...(currentArticle.extra as any) }
            delete nextExtra.translation
            delete nextExtra.translated_by
            currentArticle.extra = nextExtra as any
        }

        if (currentArticle.ref && typeof currentArticle.ref === 'object') {
            visit(currentArticle.ref as ArticleWithId | Article)
        }
    }

    visit(cloned)
    return cloned
}

function normalizePlatformToken(value: unknown) {
    const normalized = String(value || '')
        .trim()
        .toLocaleLowerCase()
        .replace(/[_\s-]+/g, '')
    if (['2', 'instagram', 'ig', 'ins'].includes(normalized)) {
        return 'instagram'
    }
    if (['3', 'tiktok', 'tt'].includes(normalized)) {
        return 'tiktok'
    }
    if (['0', '1', 'x', 'twitter'].includes(normalized)) {
        return 'x'
    }
    if (['4', 'youtube', 'yt'].includes(normalized)) {
        return 'youtube'
    }
    if (['5', 'website', 'web'].includes(normalized)) {
        return 'website'
    }
    return normalized
}

function getArticlePlatformToken(article: Pick<ArticleWithId, 'platform'>) {
    return normalizePlatformToken(article.platform)
}

function normalizePlatformTokenList(values: unknown) {
    if (!Array.isArray(values)) {
        return []
    }
    return uniquePreserveOrder(values.map((value) => normalizePlatformToken(value)).filter(Boolean))
}

function arePlatformTokenListsEqual(left: unknown, right: unknown) {
    const normalizedLeft = normalizePlatformTokenList(left)
    const normalizedRight = normalizePlatformTokenList(right)
    return (
        normalizedLeft.length === normalizedRight.length &&
        normalizedLeft.every((value, index) => value === normalizedRight[index])
    )
}

function resolveMediaVisibilityPolicy(
    config: ForwardTargetPlatformCommonConfig | undefined,
): ResolvedMediaVisibilityPolicy | null {
    const raw = config?.media_visibility
    const enabled = raw === true || (typeof raw === 'object' && raw?.enabled !== false)
    if (!enabled) {
        return null
    }

    const objectConfig = typeof raw === 'object' && raw ? raw : {}
    const rawWindowSeconds = Math.floor(Number((objectConfig as any).window_seconds || 0))
    if (!Number.isFinite(rawWindowSeconds) || rawWindowSeconds <= 0) {
        return null
    }

    const rawMaxVisible = Math.floor(Number((objectConfig as any).max_visible || 1))
    return {
        windowSeconds: Math.max(60, rawWindowSeconds),
        maxVisible: Math.max(1, Number.isFinite(rawMaxVisible) ? rawMaxVisible : 1),
        duplicateBehavior: (objectConfig as any).duplicate_behavior === 'text_only' ? 'text_only' : 'skip',
    }
}

function mergeFeatureFlags(...values: Array<Array<string> | undefined>) {
    return uniquePreserveOrder(values.flatMap((value) => value || []))
}

function extractHashtagsFromText(text?: string | null) {
    if (!text) {
        return []
    }
    const tags = text.match(HASHTAG_REGEX) || []
    return tags.map((tag) => `#${tag.slice(1)}`)
}

function normalizeHashtagKey(tag: string) {
    return `#${tag.slice(1).toLocaleLowerCase()}`
}

function stripHashtagsFromText(text?: string | null) {
    if (!text) {
        return ''
    }
    return text
        .replace(HASHTAG_REGEX, ' ')
        .replace(/[ \t]+/g, ' ')
        .replace(/\s*\n\s*/g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim()
}

function preserveSourceHashtags(sourceText: string, translatedText: string) {
    if (!BaseProcessor.isValidResult(translatedText)) {
        return translatedText
    }
    const sourceTags = extractHashtagsFromText(sourceText)
    if (sourceTags.length === 0) {
        return translatedText
    }
    const existingTagKeys = new Set(extractHashtagsFromText(translatedText).map((tag) => normalizeHashtagKey(tag)))
    const missingTags = uniquePreserveOrder(sourceTags).filter((tag) => !existingTagKeys.has(normalizeHashtagKey(tag)))
    if (missingTags.length === 0) {
        return translatedText
    }
    return [translatedText.trim(), missingTags.join(' ')].filter(Boolean).join(' ')
}

function normalizeTranslationComparableText(text: string | null | undefined) {
    return String(text || '')
        .replace(/https?:\/\/\S+/gi, '')
        .replace(/\s+/g, '')
        .replace(/[、。，．！？!?…~～・･:：;；"'“”‘’「」『』【】（）()[\]{}<>《》〈〉＿_\-—–]+/g, '')
        .trim()
        .toLocaleLowerCase()
}

function hasJapaneseKana(text: string | null | undefined) {
    return /[\p{Script=Hiragana}\p{Script=Katakana}]/u.test(String(text || ''))
}

function truncateDigestText(text: string, maxLength: number) {
    if (text.length <= maxLength) {
        return text
    }
    return `${text.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`
}

function extractArticleHashtags(article: ArticleWithId) {
    const extra = article.extra as any
    return uniquePreserveOrder([
        ...extractHashtagsFromText(article.content),
        ...extractHashtagsFromText(article.translation),
        ...extractHashtagsFromText(extra?.content),
        ...extractHashtagsFromText(extra?.translation),
    ])
}

function extractArticleNonTagText(article: ArticleWithId, maxLength = 120) {
    const extra = article.extra as any
    const candidates = [
        article.content,
        article.translation,
        extra?.content,
        extra?.translation,
        extractArticleHeadline(article as any, maxLength),
    ]

    for (const candidate of candidates) {
        const stripped = stripHashtagsFromText(candidate)
        if (stripped) {
            return truncateDigestText(stripped, maxLength)
        }
    }
    return article.url || article.a_id
}

function getArticleAuthorKey(article: ArticleWithId) {
    return article.u_id || article.username || article.a_id
}

function lookupConnectionValues<T>(map: Record<string, T> | undefined, keys: Array<string | undefined>): T | undefined {
    if (!map) {
        return undefined
    }
    for (const key of keys) {
        if (key && Object.prototype.hasOwnProperty.call(map, key)) {
            return map[key]
        }
    }
    return undefined
}

function resolveBatchTargetIds(
    formatterIds: Array<string>,
    formatterTargetMap: Record<string, Array<string>>,
    forwardTargets?: AppConfig['forward_targets'],
) {
    const targetIds = new Set<string>()
    for (const formatterId of formatterIds) {
        const connectedTargets = formatterTargetMap[formatterId] || []
        for (const targetId of connectedTargets) {
            const targetDef = forwardTargets?.find((target) => target.id === targetId)
            if ((targetDef?.cfg_platform as any)?.bypass_batch === true) {
                continue
            }
            targetIds.add(targetId)
        }
    }
    return sortUnique(Array.from(targetIds))
}

function resolveBatchAggregationConfig(
    cfg_forwarder: Forwarder['cfg_forwarder'] | undefined,
    aggregatingFormatters: Array<Record<string, any>> = [],
) {
    const raw = cfg_forwarder as any
    const formatterCron = aggregatingFormatters.find((formatter) => formatter?.aggregation_cron)?.aggregation_cron
    const formatterWindowSeconds = aggregatingFormatters.find(
        (formatter) => formatter?.aggregation_window_seconds,
    )?.aggregation_window_seconds
    const cron = normalizeCronSecond(
        raw?.aggregation_cron || raw?.batch_cron || formatterCron || DEFAULT_BATCH_AGGREGATION_CRON,
    )
    const windowSeconds = Math.max(
        60,
        Math.floor(
            Number(
                raw?.aggregation_window_seconds ||
                    raw?.batch_window_seconds ||
                    formatterWindowSeconds ||
                    DEFAULT_BATCH_AGGREGATION_WINDOW_SECONDS,
            ),
        ),
    )
    return {
        cron,
        windowSeconds,
    }
}

function resolveMatchingForwarderTemplate(
    crawler: CrawlerConfig,
    forwarders?: AppConfig['forwarders'],
): ForwarderTemplate {
    return (
        forwarders?.find((forwarder) => {
            return forwarder.origin && crawler.origin && forwarder.origin === crawler.origin
        }) || {
            name: 'default-auto-bind',
            origin: crawler.origin,
            cfg_forwarder: {},
        }
    )
}

function buildAutoBoundForwarderTaskData(
    crawler: CrawlerConfig,
    props: Pick<AppConfig, 'cfg_forwarder' | 'forwarders'>,
) {
    const matchedForwarder = resolveMatchingForwarderTemplate(crawler, props.forwarders)
    const cfg_forwarder = {
        cron: '*/30 * * * *',
        media: {
            type: 'no-storage' as const,
            use: {
                tool: MediaToolEnum.DEFAULT,
            },
        },
        ...props.cfg_forwarder,
        ...matchedForwarder.cfg_forwarder,
        deduplication: matchedForwarder.cfg_forwarder?.deduplication ?? true,
    }

    return {
        matchedForwarder,
        forwarderTaskData: {
            ...matchedForwarder,
            name: crawler.name,
            crawler_id: (crawler as any).id,
            websites: crawler.websites,
            origin: crawler.origin,
            paths: crawler.paths,
            cfg_forwarder,
        } satisfies Forwarder,
    }
}

type Forwarder = RealForwarder<TaskType>

interface TaskResult {
    taskId: string
    result: Array<CrawlerTaskResult>
    immediate_notify?: boolean
    crawlerName?: string
}

interface CrawlerTaskResult {
    task_type: TaskType
    url: string
    data: Array<number>
}

type ArticleIdsByUrl = Record<string, Array<number>>

/**
 * 根据cronjob dispatch任务
 * 根据结果查询数据库
 */
class ForwarderTaskScheduler extends TaskScheduler.TaskScheduler {
    NAME: string = 'ForwarderTaskScheduler'
    protected log?: Logger
    private props: Pick<
        AppConfig,
        'cfg_forwarder' | 'forwarders' | 'connections' | 'crawlers' | 'formatters' | 'forward_targets'
    >
    private taskEventBindings: Array<{ eventName: string; listener: (...args: any[]) => void }> = []
    private spiderFinishedListener: (payload: unknown) => void

    constructor(
        props: Pick<
            AppConfig,
            'cfg_forwarder' | 'forwarders' | 'connections' | 'crawlers' | 'formatters' | 'forward_targets'
        >,
        emitter: EventEmitter,
        log?: Logger,
    ) {
        super(emitter)
        this.log = log?.child({ subservice: this.NAME })
        this.props = props
        this.spiderFinishedListener = this.onSpiderTaskFinished.bind(this)
    }

    async init() {
        this.log?.info('initializing...')

        // 注册基本的监听器
        this.taskEventBindings = Object.entries(this.taskHandlers).map(([eventName, listener]) => ({
            eventName: `forwarder:${eventName}`,
            listener,
        }))
        for (const binding of this.taskEventBindings) {
            this.emitter.on(binding.eventName, binding.listener)
        }
        this.emitter.on(`spider:${TaskScheduler.TaskEvent.FINISHED}`, this.spiderFinishedListener)

        // 遍历爬虫配置，为每个爬虫创建定时任务
        // Auto-Bind Logic: Iterate Crawlers -> Find Matching Forwarder -> Spawn Task
        if (this.props.crawlers && this.props.crawlers.length > 0) {
            for (const crawler of this.props.crawlers) {
                const { matchedForwarder, forwarderTaskData } = buildAutoBoundForwarderTaskData(crawler, this.props)
                const taskName = crawler.name
                const cfg_forwarder = forwarderTaskData.cfg_forwarder
                const cron = normalizeCronSecond(cfg_forwarder.cron)

                const job = new CronJob(cron, async () => {
                    const taskId = `${Math.random().toString(36).substring(2, 9)}`
                    this.log?.info(`starting to dispatch task ${taskName}...`)
                    const task: TaskScheduler.Task = {
                        id: taskId,
                        status: TaskScheduler.TaskStatus.PENDING,
                        data: {
                            ...forwarderTaskData,
                            // Inject connections into task data so pools can access it
                            connections: this.props.connections,
                        },
                    }
                    this.tasks.set(taskId, task)
                    this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, {
                        taskId,
                        task: task,
                    })
                })
                this.log?.info(`Auto-Bound Forwarder Task created: ${taskName} using template ${matchedForwarder.name}`)
                this.cronJobs.push(job)

                // --- BATCH MODE DISPATCHER ---
                // Find formatters connected to this crawler that have aggregation enabled
                const crawlerFormatterMap = (this.props.connections as any)?.['crawler-formatter'] || {}
                const crawlerProcessorMap = (this.props.connections as any)?.['crawler-processor'] || {}
                const processorFormatterMap = (this.props.connections as any)?.['processor-formatter'] || {}
                const directFormatterIds: string[] = crawlerFormatterMap[crawler.name as string] || []
                const processorId: string | undefined = crawlerProcessorMap[crawler.name as string]
                const connectedFormatterIds: string[] = sortUnique([
                    ...directFormatterIds,
                    ...((processorId ? processorFormatterMap[processorId] : []) || []),
                ])
                const aggregatingFormatters = this.props.connections
                    ? connectedFormatterIds
                          .map((fid: string) => this.props.formatters?.find((f) => f.id === fid))
                          .filter((f) => f?.aggregation)
                    : []

                if (aggregatingFormatters.length > 0 || (cfg_forwarder as any).batch_mode) {
                    const batchConfig = resolveBatchAggregationConfig(cfg_forwarder, aggregatingFormatters as any)
                    const batchJob = new CronJob(batchConfig.cron, async () => {
                        this.log?.info(`Dispatching Batch for ${taskName}`)
                        const formatterTargetMap = ((this.props.connections as any)?.['formatter-target'] ||
                            {}) as Record<string, Array<string>>
                        const targetIds = resolveBatchTargetIds(
                            aggregatingFormatters.map((formatter: any) => formatter.id).filter(Boolean),
                            formatterTargetMap,
                            this.props.forward_targets,
                        )
                        if (targetIds.length === 0) {
                            this.log?.info(`Skipping Hourly Batch for ${taskName}: no batch-enabled targets`)
                            return
                        }

                        const websites = sanitizeWebsites({
                            websites: crawler.websites || matchedForwarder?.websites,
                            origin: crawler.origin || matchedForwarder?.origin,
                            paths: crawler.paths || matchedForwarder?.paths,
                        })
                        const end = Math.floor(Date.now() / 1000)
                        const start = end - batchConfig.windowSeconds

                        for (const website of websites) {
                            const info = spiderRegistry.extractBasicInfo(website)
                            if (!info?.u_id || !info.platform) {
                                continue
                            }
                            const taskType = DB.TaskQueue.TYPE.AggregateHourly
                            const payload = {
                                platform: info.platform,
                                u_id: info.u_id,
                                start,
                                end,
                                target_ids: targetIds,
                            }
                            await DB.TaskQueue.add(taskType, payload, end, {
                                source_ref: `${info.platform}:${info.u_id}`,
                                action_type: taskType,
                                idempotency_key: DB.TaskQueue.buildIdempotencyKey(taskType, payload),
                            })
                        }
                    })
                    this.log?.info(
                        `Batch Job created for ${taskName} with cron ${batchConfig.cron}, window ${batchConfig.windowSeconds}s, to send to ${aggregatingFormatters.length} aggregating formatters`,
                    )
                    this.cronJobs.push(batchJob)
                }
                // -----------------------------
            }
        } else {
            this.log?.warn('No crawlers defined for auto-binding.')
        }
    }

    private onSpiderTaskFinished(payload: unknown) {
        if (!TaskScheduler.isTaskFinishedPayload<CrawlerTaskResult>(payload)) {
            this.log?.warn('Ignoring malformed spider finished payload')
            return
        }
        const { taskId, result, crawlerName } = payload
        if (!crawlerName || result.length === 0) {
            return
        }

        const crawler = this.props.crawlers?.find((item) => item.name === crawlerName)
        if (!crawler) {
            this.log?.warn(`Spider finished for unknown crawler ${crawlerName}, skipping immediate forward dispatch.`)
            return
        }

        const articleIdsByUrl = result.reduce((acc, item) => {
            if (item.task_type !== 'article' || item.data.length === 0) {
                return acc
            }
            acc[item.url] = item.data
            return acc
        }, {} as ArticleIdsByUrl)

        if (Object.keys(articleIdsByUrl).length === 0) {
            return
        }

        void this.enqueueCrawlerPostProcessorTasks(crawler, articleIdsByUrl).catch((error) => {
            this.log?.warn(
                `Failed to enqueue post-processor tasks for crawler ${crawlerName}: ${
                    error instanceof Error ? error.message : String(error)
                }`,
            )
        })

        const { forwarderTaskData } = buildAutoBoundForwarderTaskData(crawler, this.props)
        const resultWebsites = Object.keys(articleIdsByUrl)
        const forwardTaskId = `spider-${taskId}`
        const task: TaskScheduler.Task = {
            id: forwardTaskId,
            status: TaskScheduler.TaskStatus.PENDING,
            data: {
                ...forwarderTaskData,
                websites: resultWebsites,
                origin: undefined,
                paths: undefined,
                connections: this.props.connections,
                article_ids_by_url: articleIdsByUrl,
            },
        }

        this.log?.info(`Dispatching immediate forwarder task for crawler ${crawlerName}`)
        this.tasks.set(forwardTaskId, task)
        const dispatched = this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, {
            taskId: forwardTaskId,
            task,
        })
        if (!dispatched) {
            this.tasks.delete(forwardTaskId)
            this.log?.warn(`Forwarder dispatcher unavailable for immediate task ${forwardTaskId}`)
        }
    }

    private getCrawlerPostProcessors(crawler: CrawlerConfig) {
        return (crawler.cfg_crawler?.post_processors || []).filter(
            (processor) => processor && processor.enabled !== false && processor.processor_id,
        )
    }

    private async enqueueCrawlerPostProcessorTasks(crawler: CrawlerConfig, articleIdsByUrl: ArticleIdsByUrl) {
        const postProcessors = this.getCrawlerPostProcessors(crawler)
        if (postProcessors.length === 0) {
            return
        }

        const now = Math.floor(Date.now() / 1000)
        let scheduled = 0
        for (const [url, articleIds] of Object.entries(articleIdsByUrl)) {
            const info = spiderRegistry.extractBasicInfo(url)
            if (!info?.platform) {
                this.log?.warn(`Skipping post-processor tasks for ${url}: unable to resolve platform`)
                continue
            }
            for (const id of articleIds) {
                for (const postProcessor of postProcessors) {
                    const action = postProcessor.action || 'extract'
                    const payload = {
                        processorId: postProcessor.processor_id,
                        action,
                        platform: info.platform,
                        id,
                        scheduleUrl: postProcessor.schedule_url,
                        scheduleApiKey: postProcessor.schedule_api_key,
                        scheduleUserAgent: postProcessor.schedule_user_agent,
                        scheduleWafBypassHeader: postProcessor.schedule_waf_bypass_header,
                        resultKey: postProcessor.result_key,
                        minConfidence: postProcessor.min_confidence,
                    }
                    await DB.TaskQueue.add(DB.TaskQueue.TYPE.ArticleProcessorRun, payload, now, {
                        source_ref: `${info.platform}:${id}`,
                        action_type: action,
                        idempotency_key: DB.TaskQueue.buildIdempotencyKey(DB.TaskQueue.TYPE.ArticleProcessorRun, {
                            processorId: payload.processorId,
                            action,
                            platform: info.platform,
                            id,
                        }),
                    })
                    scheduled += 1
                }
            }
        }

        if (scheduled > 0) {
            this.log?.info(`Queued ${scheduled} post-processor task(s) for crawler ${crawler.name || '(unnamed)'}`)
        }
    }

    /**
     * 启动定时任务
     */
    async start() {
        this.log?.info(`Manager starting... [CronJobs: ${this.cronJobs.length}]`)
        this.cronJobs.forEach((job) => {
            job.start()
            this.log?.debug(`CronJob started: ${job.cronTime.source}`)
        })
    }

    /**
     * 停止定时任务管理器
     */
    async stop() {
        // force to stop all tasks

        // stop all cron jobs
        this.cronJobs.forEach((job) => {
            job.stop()
        })
        this.log?.info('All jobs stopped')
        this.log?.info('Manager stopped')
    }

    async drop() {
        // 清除所有任务
        this.tasks.clear()
        for (const binding of this.taskEventBindings) {
            this.emitter.off(binding.eventName, binding.listener)
        }
        this.emitter.off(`spider:${TaskScheduler.TaskEvent.FINISHED}`, this.spiderFinishedListener)
        this.taskEventBindings = []
        this.cronJobs = []
        this.log?.info('Manager dropped')
    }

    updateTaskStatus(payload: unknown) {
        if (!TaskScheduler.isTaskStatusPayload(payload)) {
            this.log?.warn('Ignoring malformed forwarder status payload')
            return
        }
        const { taskId, status } = payload
        const task = this.tasks.get(taskId)
        if (task) {
            task.status = status
        }

        // TODO: delete task later or manually
        if (
            status === TaskScheduler.TaskStatus.COMPLETED ||
            status === TaskScheduler.TaskStatus.FAILED ||
            status === TaskScheduler.TaskStatus.CANCELLED
        ) {
            this.tasks.delete(taskId)
        }
    }
    finishTask(payload: unknown) {
        if (!TaskScheduler.isTaskFinishedPayload<CrawlerTaskResult>(payload)) {
            this.log?.warn('Ignoring malformed forwarder finished payload')
            return
        }
        const { taskId, result, immediate_notify } = payload
        this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
            taskId,
            status: TaskScheduler.TaskStatus.COMPLETED,
        })
        this.log?.info(`[${taskId}] Task finished.`)
        if (result.length > 0 && immediate_notify) {
            // TODO: notify forwarders by emitter
        }
    }
}

type ForwardTargetIdWithRuntimeConfig = Record<string, ForwardTargetPlatformCommonConfig | undefined>
type ForwardTargetInstanceWithRuntimeConfig = {
    forwarder: BaseForwarder
    runtime_config?: ForwardTargetPlatformCommonConfig
}
type ResendArticleOptions = {
    targetIds?: Array<string>
}
class ForwarderPools extends BaseCompatibleModel {
    NAME = 'ForwarderPools'
    log?: Logger
    private emitter: EventEmitter
    /**
     * Mapping from `forwarder id` to `forwarder instance`
     *
     * ```
     * const id =  id or `${platform}-${hash(forwarderToBeHashed)}`
     * const forwarderToBeHashed =
     * {
     *     platform: Platform,
     *     id?: string,
     *     cfg_platform: {
     *         replace_regex?: ...,     // this will not be hashed
     *         block_until?: ...,       // this will not be hashed
     *         ...others
     *     }
     * }
     * ```
     */
    private forward_to: Map<string, BaseForwarder> = new Map()
    /**
     * - Article: batch id -> the forwarders subscribed to this website
     *
     * - Follows: batch id -> the forwarders subscribed to theses follows
     */
    private subscribers: Map<
        string,
        {
            /**
             * id for forward_to
             */
            to: ForwardTargetIdWithRuntimeConfig
            cfg_forwarder: Forwarder['cfg_forwarder']
        }
    > = new Map()
    private props: Pick<
        AppConfig,
        | 'forward_targets'
        | 'cfg_forward_target'
        | 'connections'
        | 'formatters'
        | 'cfg_forwarder'
        | 'forwarders'
        | 'crawlers'
        | 'processors'
    >
    private renderService: RenderService
    private processors: Processor[]

    /**
     * max allowed error count for a single article in every cycle
     */
    private MAX_ERROR_COUNT = 3
    /**
     * platform:a_id -> error count
     */
    private errorCounter = new Map<string, number>()
    private tagDigestStates = new Map<string, TagDigestState>()
    private summaryCardQueues = new Map<string, SummaryCardQueue>()
    private summaryCardLastSentAt = new Map<string, number>()
    private summaryCardTargetLastSentAt = new Map<string, number>()
    private summaryCardProcessors = new Map<string, BaseProcessor>()
    private summaryCardFlushTimer?: ReturnType<typeof setInterval>
    private dispatchListener: (payload: unknown) => Promise<void>
    private shuttingDown = false

    // private workers:
    constructor(
        props: Pick<
            AppConfig,
            | 'forward_targets'
            | 'cfg_forward_target'
            | 'connections'
            | 'formatters'
            | 'cfg_forwarder'
            | 'forwarders'
            | 'crawlers'
            | 'processors'
        >,
        emitter: EventEmitter,
        log?: Logger,
    ) {
        super()
        this.log = log?.child({ subservice: this.NAME })
        this.renderService = new RenderService(this.log)
        this.emitter = emitter
        this.props = props
        this.processors = props.processors || []
        this.dispatchListener = this.onDispatchReceived.bind(this)
    }

    async init() {
        this.log?.info('Forwarder Pools initializing...')
        this.shuttingDown = false
        this.emitter.on(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, this.dispatchListener)
        this.summaryCardFlushTimer = setInterval(() => {
            this.flushDueSummaryCardQueues().catch((error) => {
                this.log?.error(`Failed to flush due summary card queues: ${error}`)
            })
        }, 30 * 1000)
        this.summaryCardFlushTimer.unref?.()
        // create targets
        const { cfg_forward_target } = this.props
        await Promise.all(
            (this.props.forward_targets || []).map(async (t) => {
                const forwarderBuilder = getForwarder(t.platform)
                if (!forwarderBuilder) {
                    this.log?.warn(`Forwarder not found for ${t.platform}`)
                    return
                }
                t.cfg_platform = {
                    ...cfg_forward_target,
                    ...t.cfg_platform,
                }
                const { block_until, replace_regex, ...restToBeHashed } = t.cfg_platform
                const forwarderToBeHashed = {
                    ...t,
                    cfg_platform: {
                        ...restToBeHashed,
                    },
                }
                const id =
                    t.id ||
                    `${t.platform}-${crypto.createHash('md5').update(JSON.stringify(forwarderToBeHashed)).digest('hex')}`
                const forwarder = new forwarderBuilder(t.cfg_platform, id, this.log)
                await forwarder.init()
                this.forward_to.set(id, forwarder)
            }),
        )
        await this.restoreSummaryCardQueues().catch((error) => {
            this.log?.warn(
                `Failed to restore summary-card queues: ${error instanceof Error ? error.message : String(error)}`,
            )
        })
    }

    async stop(..._args: any[]): Promise<void> {
        this.shuttingDown = true
        this.emitter.off(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, this.dispatchListener)
        if (this.summaryCardFlushTimer) {
            clearInterval(this.summaryCardFlushTimer)
            this.summaryCardFlushTimer = undefined
        }
    }

    private async restoreSummaryCardQueues() {
        const windows = await DB.AggregationWindow.listOpen('summary_card')
        let restoredCount = 0
        const now = Math.floor(Date.now() / 1000)
        for (const window of windows) {
            const target = this.forward_to.get(window.target_id)
            if (!target) {
                await DB.AggregationWindow.updateStatus(window.id, DB.AggregationWindow.STATUS.Cancelled, {
                    payload_hash: 'missing-target',
                }).catch(() => undefined)
                continue
            }

            const items = await DB.AggregationWindow.listItems(window.id)
            const queueItems = new Map<number, SummaryCardQueueItem>()
            let runtime_config: ForwardTargetPlatformCommonConfig | undefined
            let persistedConfig: ResolvedSummaryCardConfig | undefined
            for (const item of items) {
                const article = await DB.Article.getSingleArticle(
                    item.article_row_id,
                    Number(item.platform) as Platform,
                )
                if (!article) {
                    continue
                }
                const payload = (item.payload || {}) as any
                runtime_config = runtime_config || payload.runtime_config || undefined
                persistedConfig = persistedConfig || payload.summaryConfig || undefined
                queueItems.set(article.id, {
                    article: cloneDeep(article),
                    queuedAt: Number(payload.queuedAt || item.created_at),
                    cardSourceMediaFiles: Array.isArray(payload.cardSourceMediaFiles)
                        ? payload.cardSourceMediaFiles
                        : [],
                    originalMediaFiles: Array.isArray(payload.originalMediaFiles) ? payload.originalMediaFiles : [],
                    digestTags: Array.isArray(payload.digestTags) ? payload.digestTags : [],
                })
            }

            if (queueItems.size === 0) {
                const baseConfig = resolveSummaryCardConfig(target.getEffectiveConfig(undefined))
                const payload_hash =
                    baseConfig && this.isSummaryCardWindowStale(window, baseConfig, now)
                        ? 'stale-window'
                        : 'empty-window'
                await DB.AggregationWindow.updateStatus(window.id, DB.AggregationWindow.STATUS.Cancelled, {
                    payload_hash,
                }).catch(() => undefined)
                continue
            }

            const restoreConfig = this.resolveSummaryCardConfigForRestore(target, runtime_config, persistedConfig)
            const config = restoreConfig.config
            if (!config) {
                await DB.AggregationWindow.updateStatus(window.id, DB.AggregationWindow.STATUS.Cancelled, {
                    payload_hash: restoreConfig.cancelReason,
                }).catch(() => undefined)
                continue
            }

            runtime_config = this.stripSummaryCardRuntimeConfig(runtime_config)

            if (this.isSummaryCardWindowStale(window, config, now)) {
                await DB.AggregationWindow.updateStatus(window.id, DB.AggregationWindow.STATUS.Cancelled, {
                    payload_hash: 'stale-window',
                }).catch(() => undefined)
                continue
            }

            const firstQueuedAt = Number(
                window.window_start || Math.min(...Array.from(queueItems.values()).map((item) => item.queuedAt)),
            )
            const lastQueuedAt = Math.max(...Array.from(queueItems.values()).map((item) => item.queuedAt))
            const windowStart = Number(window.window_start || firstQueuedAt)
            const windowEnd = Number(window.window_end || firstQueuedAt + config.intervalSeconds)
            const sharedRouteKey = this.buildSummaryCardSharedRouteKey(target.id)
            const queueKey = this.getSummaryCardQueueKey(sharedRouteKey, target.id, runtime_config, config)
            const existingQueue = this.summaryCardQueues.get(queueKey)
            if (existingQueue) {
                for (const item of queueItems.values()) {
                    existingQueue.items.set(item.article.id, item)
                    if (existingQueue.windowId && existingQueue.windowId !== window.id) {
                        await this.persistSummaryCardItem(existingQueue, item).catch((error) => {
                            this.log?.warn(
                                `Failed to merge summary-card item ${item.article.a_id} into window ${existingQueue.windowId}: ${
                                    error instanceof Error ? error.message : String(error)
                                }`,
                            )
                        })
                    }
                }
                existingQueue.firstQueuedAt = Math.min(existingQueue.firstQueuedAt, firstQueuedAt)
                existingQueue.lastQueuedAt = Math.max(existingQueue.lastQueuedAt, lastQueuedAt)
                existingQueue.windowStart = Math.min(existingQueue.windowStart || windowStart, windowStart)
                existingQueue.windowEnd = Math.max(existingQueue.windowEnd || windowEnd, windowEnd)
                if (existingQueue.windowId && existingQueue.windowId !== window.id) {
                    await DB.AggregationWindow.updateStatus(window.id, DB.AggregationWindow.STATUS.Cancelled, {
                        payload_hash: `merged-into-window-${existingQueue.windowId}`,
                    }).catch(() => undefined)
                }
            } else {
                this.summaryCardQueues.set(queueKey, {
                    routeKey: sharedRouteKey,
                    windowId: window.id,
                    target,
                    runtime_config,
                    config,
                    items: queueItems,
                    firstQueuedAt,
                    lastQueuedAt,
                    windowStart,
                    windowEnd,
                })
            }
            restoredCount += 1
        }
        if (restoredCount > 0) {
            this.log?.info(`Restored ${restoredCount} summary-card queue(s) from durable aggregation windows`)
        }
    }

    private isSummaryCardExplicitlyDisabled(config: ForwardTargetPlatformCommonConfig) {
        const raw = config.summary_card
        return raw === false || (typeof raw === 'object' && raw?.enabled === false)
    }

    private resolveSummaryCardConfigForRestore(
        target: BaseForwarder,
        runtime_config: ForwardTargetPlatformCommonConfig | undefined,
        persistedConfig: ResolvedSummaryCardConfig | undefined,
    ) {
        const baseEffectiveConfig = target.getEffectiveConfig(undefined)
        if (this.isSummaryCardExplicitlyDisabled(baseEffectiveConfig)) {
            return { config: null, cancelReason: 'summary-card-disabled' }
        }

        const candidates = uniquePreserveOrder(
            [
                resolveSummaryCardConfig(target.getEffectiveConfig(runtime_config)),
                resolveSummaryCardConfig(baseEffectiveConfig),
            ].filter((config): config is ResolvedSummaryCardConfig => Boolean(config)),
        )

        if (!persistedConfig) {
            return {
                config: candidates[0] || null,
                cancelReason: candidates[0] ? undefined : 'summary-card-disabled',
            }
        }

        const compatibleConfig = candidates.find((config) =>
            this.isSummaryCardConfigCompatibleForRestore(config, persistedConfig),
        )
        if (compatibleConfig) {
            return { config: compatibleConfig, cancelReason: undefined }
        }
        return {
            config: null,
            cancelReason: candidates.length > 0 ? 'summary-card-config-changed' : 'summary-card-disabled',
        }
    }

    private isSummaryCardConfigCompatibleForRestore(
        current: ResolvedSummaryCardConfig,
        persisted: ResolvedSummaryCardConfig,
    ) {
        return (
            current.intervalSeconds === persisted.intervalSeconds &&
            current.threshold === persisted.threshold &&
            current.maxItems === persisted.maxItems &&
            current.includeOriginalMedia === persisted.includeOriginalMedia &&
            current.sendFirstImmediately === persisted.sendFirstImmediately &&
            current.sendFirstNative === persisted.sendFirstNative &&
            current.mediaRealtime === persisted.mediaRealtime &&
            current.mediaRealtimeText === persisted.mediaRealtimeText &&
            arePlatformTokenListsEqual(
                current.mediaRealtimeDropSummaryPlatforms,
                (persisted as any).mediaRealtimeDropSummaryPlatforms,
            ) &&
            current.flushOnThreshold === persisted.flushOnThreshold &&
            current.flushDelaySeconds === persisted.flushDelaySeconds &&
            current.windowAlignment === persisted.windowAlignment &&
            current.mediaDuplicateLimit === persisted.mediaDuplicateLimit
        )
    }

    private stripSummaryCardRuntimeConfig(runtime_config?: ForwardTargetPlatformCommonConfig) {
        if (!runtime_config || !Object.prototype.hasOwnProperty.call(runtime_config, 'summary_card')) {
            return runtime_config
        }
        const restoredRuntimeConfig = { ...(runtime_config as any) }
        delete restoredRuntimeConfig.summary_card
        return restoredRuntimeConfig as ForwardTargetPlatformCommonConfig
    }

    private async onDispatchReceived(payload: unknown) {
        if (!TaskScheduler.isTaskCtx(payload)) {
            this.log?.warn('Ignoring malformed forwarder dispatch payload')
            return
        }
        const ctx = payload
        if (this.shuttingDown) {
            this.log?.warn(`Cancelling forwarder task ${ctx.taskId}: pool is shutting down`)
            this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
                taskId: ctx.taskId,
                status: TaskScheduler.TaskStatus.CANCELLED,
            })
            return
        }
        try {
            await this.onTaskReceived(ctx)
        } catch (error) {
            const taskName = (ctx.task.data as Forwarder | undefined)?.name || 'unknown'
            const message = toErrorMessage(error)
            ctx.log = ctx.log || this.log?.child({ label: taskName, trace_id: ctx.taskId })
            ctx.log?.error(`Unexpected forwarder dispatch failure: ${message}`)
            try {
                this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
                    taskId: ctx.taskId,
                    status: TaskScheduler.TaskStatus.FAILED,
                })
            } catch (emitError) {
                ctx.log?.warn(`Failed to emit forwarder failure status: ${toErrorMessage(emitError)}`)
            }
        }
    }

    // handle task received
    async onTaskReceived(ctx: TaskScheduler.TaskCtx) {
        const { taskId, task } = ctx
        let {
            websites,
            origin,
            paths,
            task_type = 'article' as TaskType,
            task_title,
            cfg_forwarder,
            name,
            subscribers,
            cfg_forward_target,
            id,
            crawler_id,
            connections,
            article_ids_by_url,
        } = task.data as Forwarder & {
            crawler_id?: string
            connections?: AppConfig['connections']
            article_ids_by_url?: ArticleIdsByUrl
        }
        ctx.log = this.log?.child({ label: name, trace_id: taskId })
        // prepare
        // maybe we will use workers in the future
        this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
            taskId,
            status: TaskScheduler.TaskStatus.RUNNING,
        })
        ctx.log?.debug(`Task received: ${JSON.stringify(task)}`)

        if (!websites && !origin && !paths) {
            ctx.log?.error(
                `[Trace] No websites or origin or paths found. Task data keys: ${Object.keys(task.data).join(',')}`,
            )
            this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
                taskId,
                status: TaskScheduler.TaskStatus.CANCELLED,
            })
            return
        }
        websites = sanitizeWebsites({
            websites,
            origin,
            paths,
        })
        ctx.log?.info(`[Trace] Sanitized websites: ${websites?.length || 0} found. List: ${websites?.join(', ')}`)

        try {
            let result: Array<CrawlerTaskResult> = []
            if (task_type === 'article') {
                await this.processArticleTask({
                    taskId,
                    log: ctx.log,
                    task: {
                        ...ctx.task,
                        data: {
                            websites,
                            cfg_forwarder,
                            subscribers,
                            cfg_forward_target,
                            id,
                            crawler_id,
                            connections,
                            name,
                            article_ids_by_url,
                        },
                    },
                })
            }

            if (task_type === 'follows') {
                /**
                 * one time task id, so we basic needn't care about the collision next run
                 */
                const batchId = crypto
                    .createHash('md5')
                    .update(`${task_type}:${websites.join(',')}`)
                    .digest('hex')
                const forwarders = this.getOrInitForwarders(
                    batchId,
                    subscribers,
                    cfg_forwarder,
                    cfg_forward_target,
                    id,
                    connections,
                )
                if (forwarders.length === 0) {
                    ctx.log?.warn(`No forwarders found for ${task_title || batchId}`)
                    return
                }
                await this.processFollowsTask(ctx, websites, forwarders)
            }

            this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.FINISHED}`, {
                taskId,
                result,
            } as TaskResult)
        } catch (error) {
            ctx.log?.error(`Error while sending: ${error}`)
            this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
                taskId,
                status: TaskScheduler.TaskStatus.FAILED,
            })
        }
    }

    private async markDiscardedMediaBatchOutboundsSkipped(target: BaseForwarder, batches: DiscardedMediaBatch[]) {
        let marked = 0
        for (const batch of batches) {
            for (const item of batch.items) {
                const article = item.article as ArticleWithId | undefined
                const outboundKey =
                    item.outboundKey ||
                    (article && article.a_id && article.platform !== undefined
                        ? articleOutboundKey(target.id, article)
                        : null)
                if (!outboundKey) {
                    continue
                }

                try {
                    await DB.OutboundMessage.markSkipped(outboundKey, 'media_batch_discarded_on_drop', {
                        skipped: 'media_batch_discarded_on_drop',
                        batchKey: batch.batchKey,
                        pendingUnits: batch.pendingUnits,
                        threshold: batch.threshold,
                        unitCount: item.unitCount,
                        articleKey: article ? articleKey(article) : null,
                    })
                    marked += 1
                } catch (error) {
                    this.log?.warn(
                        `Failed to mark discarded media batch outbound ${outboundKey} skipped for ${target.id}: ${
                            error instanceof Error ? error.message : String(error)
                        }`,
                    )
                }
            }
        }

        if (marked > 0) {
            this.log?.warn(`Marked ${marked} discarded media batch outbound(s) skipped for ${target.id}`)
        }
    }

    async drop(...args: any[]): Promise<void> {
        this.log?.info('Dropping Pools...')
        await this.stop(...args)
        if (this.summaryCardQueues.size > 0) {
            this.log?.info(
                `Keeping ${this.summaryCardQueues.size} durable summary-card queue(s) unsent during pool drop`,
            )
        }
        for (const forwarder of this.forward_to.values()) {
            const discardedMediaBatches = forwarder.drainPendingMediaBatches()
            await this.markDiscardedMediaBatchOutboundsSkipped(forwarder, discardedMediaBatches)
            await forwarder.drop().catch((error) => {
                this.log?.warn(`Failed to drop forwarder ${forwarder.id}: ${error}`)
            })
        }
        this.forward_to.clear()
        this.subscribers.clear()
        this.errorCounter.clear()
        await Promise.all(
            Array.from(this.summaryCardProcessors.values()).map((processor) =>
                processor.drop().catch((error) => {
                    this.log?.warn(`Failed to drop summary-card processor ${processor.NAME}: ${error}`)
                }),
            ),
        )
        this.summaryCardProcessors.clear()
        this.summaryCardQueues.clear()
        this.summaryCardLastSentAt.clear()
        this.summaryCardTargetLastSentAt.clear()
        this.log?.info('Pools dropped')
    }

    private shouldStopForShutdown(log?: Logger, scope = 'forwarder task') {
        if (!this.shuttingDown) {
            return false
        }
        log?.warn(`Stopping ${scope}: forwarder pool is shutting down`)
        return true
    }

    async processArticleTask(ctx: TaskScheduler.TaskCtx) {
        const {
            websites,
            subscribers,
            cfg_forwarder,
            cfg_forward_target,
            id,
            crawler_id,
            connections,
            article_ids_by_url,
        } = ctx.task.data as {
            websites: Array<string>
            subscribers: Forwarder['subscribers']
            cfg_forwarder: Forwarder['cfg_forwarder']
            cfg_forward_target: Forwarder['cfg_forward_target']
            id?: string
            crawler_id?: string
            connections?: AppConfig['connections']
            article_ids_by_url?: ArticleIdsByUrl
        }
        const batchId = crypto
            .createHash('md5')
            .update(`article:${websites.join(',')}`)
            .digest('hex')
        let attemptedPaths = 0
        let failedPaths = 0

        for (const website of websites) {
            if (this.shouldStopForShutdown(ctx.log, 'article forwarding loop')) {
                return
            }
            // 单次爬虫任务
            const url = new URL(website)

            const crawlerName = ctx.task.data.name
            const allPaths = this.resolveForwardingPaths(
                crawlerName,
                cfg_forwarder,
                cfg_forward_target,
                connections,
                ctx.log,
                {
                    crawlerId: crawler_id,
                    forwarderId: id,
                    subscribers,
                },
            )

            // 3. Execute All Paths
            if (allPaths.length === 0) {
                // Only warn if we really found nothing at all (neither graph nor inline)
                ctx.log?.debug(`[Trace] No forwarding paths (graph or inline) found for ${url}, skipping...`)
                continue
            }

            for (const path of allPaths) {
                if (this.shouldStopForShutdown(ctx.log, 'article forwarding path loop')) {
                    return
                }
                ctx.log?.info(
                    `Processing via path [${path.source}]: ${path.formatterName} for ${path.targets.length} targets`,
                )
                /**
                 * 查询当前网站下的近10篇文章并查询转发
                 */
                attemptedPaths += 1
                try {
                    await this.processSingleArticleTask(
                        ctx,
                        url.href,
                        path.targets,
                        path.formatterConfig,
                        article_ids_by_url?.[url.href],
                        path.routeKey,
                    )
                } catch (error) {
                    failedPaths += 1
                    ctx.log?.error(
                        `Article forwarding path failed for ${url.href} via ${path.formatterName}: ${toErrorMessage(error)}`,
                    )
                }
            }
        }

        if (failedPaths > 0) {
            ctx.log?.warn(
                `Article forwarding completed with ${failedPaths}/${attemptedPaths} path failure(s); remaining paths were processed.`,
            )
        }
        if (attemptedPaths > 0 && failedPaths === attemptedPaths) {
            throw new Error(`All article forwarding paths failed for ${websites.length} website(s)`)
        }
    }

    async resendArticle(
        article: ArticleWithId,
        crawlerName: string,
        cfg_forwarder?: Forwarder['cfg_forwarder'],
        cfg_forward_target?: Forwarder['cfg_forward_target'],
        options: ResendArticleOptions = {},
    ) {
        const taskLog = this.log?.child({ label: `manual-resend:${crawlerName}` })
        const crawler = this.props.crawlers?.find(
            (item) => item.name === crawlerName || (item as any).id === crawlerName,
        )
        const matchedForwarder = crawler ? resolveMatchingForwarderTemplate(crawler, this.props.forwarders) : undefined
        const baseForwarderConfig = crawler
            ? buildAutoBoundForwarderTaskData(crawler, this.props).forwarderTaskData.cfg_forwarder
            : undefined
        const effectiveForwarderConfig = {
            ...baseForwarderConfig,
            ...cfg_forwarder,
        }
        const targetFilter = new Set((options.targetIds || []).map((id) => id.trim()).filter(Boolean))
        const resolvedPaths = this.resolveForwardingPaths(
            crawlerName,
            effectiveForwarderConfig,
            cfg_forward_target,
            this.props.connections,
            taskLog,
            {
                crawlerId: (crawler as any)?.id,
                forwarderId: matchedForwarder?.id,
                subscribers: matchedForwarder?.subscribers,
            },
        )
        const paths =
            targetFilter.size > 0
                ? resolvedPaths
                      .map((path) => ({
                          ...path,
                          targets: path.targets.filter(({ forwarder }) => targetFilter.has(forwarder.id)),
                      }))
                      .filter((path) => path.targets.length > 0)
                : resolvedPaths
        if (paths.length === 0) {
            const targetSuffix = targetFilter.size > 0 ? ` and targets ${Array.from(targetFilter).join(', ')}` : ''
            throw new Error(`No forwarding paths found for crawler ${crawlerName}${targetSuffix}`)
        }

        const normalizedArticles = await this.normalizeForwardingArticles([article])
        const manualTaskId = `manual-${article.a_id}-${crypto.randomUUID()}`
        for (const path of paths) {
            if (this.shouldStopForShutdown(taskLog, 'manual resend path loop')) {
                return
            }
            await this.sendArticles(
                taskLog,
                manualTaskId,
                normalizedArticles,
                path.targets,
                path.formatterConfig,
                { forceSend: true },
                { routeKey: path.routeKey },
            )
        }
    }

    async sendImmediateXLinkArticle(article: ArticleWithId, options: ImmediateXLinkSendOptions) {
        const targetIds = uniquePreserveOrder(options.targetIds.map((id) => id.trim()))
        if (targetIds.length === 0) {
            throw new Error('No QQ target ids resolved for immediate X link send')
        }
        const taskLog = this.log?.child({ label: `qq-x-link:${article.a_id}` })
        const targets = this.resolveTargetInstances(
            targetIds.map((id) => ({ id })),
            taskLog,
        )
        if (targets.length === 0) {
            throw new Error(`No target instances found for immediate X link send: ${targetIds.join(', ')}`)
        }

        const crawler = options.crawlerName
            ? this.props.crawlers?.find(
                  (item) => item.name === options.crawlerName || (item as any).id === options.crawlerName,
              )
            : undefined
        const cfg_forwarder = crawler
            ? buildAutoBoundForwarderTaskData(crawler, this.props).forwarderTaskData.cfg_forwarder
            : this.props.cfg_forwarder
        if (options.processorId) {
            await this.prepareArticleChainTranslations(
                options.processorId,
                [article],
                `QQ X link immediate ${article.a_id}`,
            )
        }

        const refreshedArticle =
            (await DB.Article.getSingleArticle(article.id, article.platform).catch(() => null)) || article
        const translatedCardArticle = this.buildTranslatedCardArticle(refreshedArticle, options.badgeLabel || '译文')
        const cardArticle = translatedCardArticle || refreshedArticle
        const cardFeatures = ['media-contain']
        if (translatedCardArticle) {
            cardFeatures.push('translated-corner-badge')
        }
        const cardResult = await this.renderService.process(cardArticle, {
            taskId: `qq-x-link-${article.id || article.a_id}`,
            render_type: 'text-card',
            card_features: mergeFeatureFlags(cfg_forwarder?.card_features, cardFeatures),
            mediaConfig: cfg_forwarder?.media,
            deduplication: false,
        })
        cardResult.mediaFiles ||= []
        cardResult.cardMediaFiles ||= []
        cardResult.originalMediaFiles ||= []

        const originalText = this.renderService.renderText(stripArticleTranslations(refreshedArticle), {
            render_type: 'text',
        })
        const sends: Array<{
            target_id: string
            part: 'merged_forward'
            result: ForwarderSendResult
        }> = []

        try {
            for (const { forwarder: target } of targets) {
                const prefixedText = originalText.trim() ? `[X解析]\n${originalText}` : '[X解析]'
                sends.push({
                    target_id: target.id,
                    part: 'merged_forward',
                    result: await target.send(prefixedText, {
                        media: [...cardResult.originalMediaFiles, ...cardResult.cardMediaFiles],
                        contentMedia: cardResult.originalMediaFiles,
                        cardMedia: cardResult.cardMediaFiles,
                        timestamp: refreshedArticle.created_at,
                        article: cloneDeep(cardArticle),
                        forceSend: true,
                        bypassMediaBatch: true,
                        runtime_config: {
                            send_mode: 'merged_forward',
                            merged_forward: {
                                enabled: true,
                            },
                        } as any,
                    }),
                })
            }
        } finally {
            this.renderService.cleanup(cardResult.mediaFiles)
        }

        return {
            article_key: articleKey(refreshedArticle),
            translated_card: Boolean(translatedCardArticle),
            target_ids: targets.map(({ forwarder }) => forwarder.id),
            sends,
        }
    }

    async processSingleArticleTask(
        ctx: TaskScheduler.TaskCtx,
        url: string,
        forwarders: Array<ForwardTargetInstanceWithRuntimeConfig>,
        cfg_forwarder: Forwarder['cfg_forwarder'],
        articleIds?: Array<number>,
        routeKeyValue?: string,
    ) {
        const { u_id, platform } = spiderRegistry.extractBasicInfo(url) ?? {}
        if (!platform) {
            ctx.log?.error(`Invalid url: ${url}`)
            return
        }

        let articles: Array<ArticleWithId> = []
        if (articleIds && articleIds.length > 0) {
            const resolvedArticles = await Promise.all(
                articleIds.map((articleId) => DB.Article.getSingleArticle(articleId, platform)),
            )
            articles = resolvedArticles.filter((item): item is ArticleWithId => Boolean(item))
        }

        if (articles.length === 0) {
            if (!u_id) {
                ctx.log?.warn(`[Trace] No article ids or u_id found for ${url}, skipping.`)
                return
            }
            articles = await DB.Article.getArticlesByName(u_id, platform)
        }
        if (articles.length <= 0) {
            ctx.log?.warn(`[Trace] No articles found for ${url} (u_id: ${u_id}, platform: ${platform})`)
            return
        }

        articles = await this.normalizeForwardingArticles(articles)
        if (this.shouldStopForShutdown(ctx.log, 'single article forwarding task')) {
            return
        }
        ctx.log?.info(`[Trace] Found ${articles.length} articles for ${url}`)
        await this.sendArticles(ctx.log, ctx.taskId, articles, forwarders, cfg_forwarder, undefined, {
            routeKey: routeKeyValue,
        })
    }

    private async normalizeForwardingArticles(articles: Array<ArticleWithId>) {
        const batchCache = new Map<string, Array<ArticleWithId>>()
        const expanded = [...articles]

        for (const article of articles) {
            const batchKey = getWebsitePhotoBatchKey(article)
            if (!batchKey || isWebsitePhotoAlbumArticle(article)) {
                continue
            }

            if (!batchCache.has(batchKey)) {
                const sameDayArticles = await DB.Article.getArticlesByTimeRange(
                    article.u_id,
                    article.platform,
                    article.created_at,
                    article.created_at,
                )
                batchCache.set(
                    batchKey,
                    sameDayArticles.filter((candidate) => getWebsitePhotoBatchKey(candidate) === batchKey),
                )
            }

            expanded.push(...(batchCache.get(batchKey) || []))
        }

        const deduped = Array.from(new Map(expanded.map((item) => [`${item.platform}:${item.a_id}`, item])).values())
        return normalizeWebsitePhotoArticles(orderBy(deduped, ['created_at', 'id'], ['desc', 'asc']))
    }

    private resolveForwardingPaths(
        crawlerName: string | undefined,
        cfg_forwarder: Forwarder['cfg_forwarder'],
        cfg_forward_target: Forwarder['cfg_forward_target'],
        connections: AppConfig['connections'] | undefined,
        log?: Logger,
        options?: {
            crawlerId?: string
            forwarderId?: string
            subscribers?: Forwarder['subscribers']
        },
    ) {
        const allPaths: ForwardingPath[] = []
        const formatterTargetMap = connections?.['formatter-target']
        const crawlerKeys = [options?.crawlerId, crawlerName]

        if (!crawlerName || !connections || !formatterTargetMap) {
            log?.warn(
                `[Trace] Missing connections or crawler name. Name: ${crawlerName}, Connections present: ${!!connections}`,
            )
        } else {
            const directFormatterIds = lookupConnectionValues(connections['crawler-formatter'], crawlerKeys) || []
            const processorId = lookupConnectionValues(connections['crawler-processor'], crawlerKeys)
            const viaProcessorFormatterIds = processorId ? connections['processor-formatter']?.[processorId] || [] : []
            const connectedFormatterIds = sortUnique([...directFormatterIds, ...viaProcessorFormatterIds])
            log?.info(
                `[Trace] Crawler '${crawlerName}' connected formatters: ${connectedFormatterIds.length} (${connectedFormatterIds.join(', ')})`,
            )

            const { formatters } = this.props
            for (const formatterId of connectedFormatterIds) {
                const formatterConfig = formatters?.find((f) => f.id === formatterId)
                if (!formatterConfig) {
                    log?.warn(`[Trace] Formatter config NOT found for ID: ${formatterId}`)
                    continue
                }

                const targetIds = formatterTargetMap[formatterId] || []
                const validTargets = this.resolveTargetInstances(
                    targetIds.map((targetId) => ({ id: targetId, runtime_config: cfg_forward_target })),
                    log,
                )

                if (validTargets.length <= 0) {
                    log?.warn(
                        `[Trace] No valid targets found for formatter ${formatterId} (Original IDs: ${targetIds.join(', ')})`,
                    )
                    continue
                }

                allPaths.push({
                    routeKey: routeKey({
                        source: 'graph',
                        crawlerId: options?.crawlerId || crawlerName,
                        formatterId,
                    }),
                    formatterId,
                    formatterConfig: {
                        ...cfg_forwarder,
                        render_type: formatterConfig.render_type,
                        aggregation: formatterConfig.aggregation,
                        deduplication: formatterConfig.deduplication,
                        render_features: mergeFeatureFlags(
                            cfg_forwarder?.render_features,
                            formatterConfig.render_features,
                        ),
                        card_features: mergeFeatureFlags(cfg_forwarder?.card_features, formatterConfig.card_features),
                    },
                    targets: validTargets,
                    source: 'graph',
                    formatterName: formatterConfig.name || formatterId,
                })
            }
        }

        const inlineTargets = this.resolveInlineForwardingTargets(
            options?.subscribers,
            cfg_forward_target,
            options?.forwarderId,
            connections,
            allPaths.flatMap((path) => path.targets.map(({ forwarder }) => forwarder.id)),
            log,
        )

        if (inlineTargets.length > 0) {
            allPaths.push({
                routeKey: routeKey({
                    source: options?.forwarderId ? 'inline' : 'manual',
                    crawlerId: options?.crawlerId || crawlerName,
                    formatterId: options?.forwarderId || 'inline',
                }),
                formatterId: options?.forwarderId || 'inline',
                formatterConfig: cfg_forwarder,
                targets: inlineTargets,
                source: 'inline',
                formatterName: options?.forwarderId || crawlerName || 'inline',
            })
        }

        return allPaths
    }

    private resolveTargetInstances(
        targets: Array<{ id: string; runtime_config?: ForwardTargetPlatformCommonConfig }>,
        log?: Logger,
    ) {
        return targets
            .map(({ id, runtime_config }) => {
                const forwarder = this.forward_to.get(id)
                if (!forwarder) {
                    log?.warn(`[Trace] Forwarder Instance NOT found for Target ID: ${id}`)
                    return undefined
                }
                return {
                    forwarder,
                    runtime_config,
                }
            })
            .filter((item): item is ForwardTargetInstanceWithRuntimeConfig => Boolean(item))
    }

    private resolveInlineForwardingTargets(
        subscribers: Forwarder['subscribers'],
        commonConfig: Forwarder['cfg_forward_target'],
        forwarderId: string | undefined,
        connections: AppConfig['connections'] | undefined,
        graphTargetIds: Array<string>,
        log?: Logger,
    ) {
        const resolved: Array<{ id: string; runtime_config?: ForwardTargetPlatformCommonConfig }> = []
        const seen = new Set(graphTargetIds)

        const pushTarget = (id: string, runtime_config?: ForwardTargetPlatformCommonConfig) => {
            if (!id || seen.has(id)) {
                return
            }
            seen.add(id)
            resolved.push({ id, runtime_config })
        }

        for (const subscriber of subscribers || []) {
            if (typeof subscriber === 'string') {
                pushTarget(subscriber, commonConfig)
                continue
            }
            pushTarget(subscriber.id, {
                ...commonConfig,
                ...subscriber.cfg_forward_target,
            })
        }

        const connectedTargetIds = forwarderId
            ? lookupConnectionValues(connections?.['forwarder-target'], [forwarderId])
            : undefined
        for (const targetId of connectedTargetIds || []) {
            pushTarget(targetId, commonConfig)
        }

        if (resolved.length === 0 && graphTargetIds.length === 0 && !connections?.['formatter-target']) {
            for (const targetId of this.forward_to.keys()) {
                pushTarget(targetId, commonConfig)
            }
        }

        if (resolved.length > 0) {
            log?.info(`[Trace] Resolved ${resolved.length} inline forwarding targets`)
        }
        return this.resolveTargetInstances(resolved, log)
    }

    private async sendArticles(
        log: Logger | undefined,
        taskId: string,
        articles: Array<ArticleWithId>,
        forwarders: Array<ForwardTargetInstanceWithRuntimeConfig>,
        cfg_forwarder: Forwarder['cfg_forwarder'],
        options?: {
            forceSend?: boolean
        },
        context?: {
            routeKey?: string
        },
    ) {
        if (this.shouldStopForShutdown(log, 'sendArticles before dispatch')) {
            return
        }
        let articles_forwarders = [] as Array<ArticleForwarderDispatch>
        for (const article of articles) {
            if (this.shouldStopForShutdown(log, 'sendArticles prefilter')) {
                return
            }
            const to = [] as Array<ForwardTargetInstanceWithRuntimeConfig>
            for (const forwarder of forwarders) {
                if (options?.forceSend) {
                    to.push(forwarder)
                    continue
                }
                const exist = await DB.ForwardBy.checkExist(
                    article.id,
                    article.platform,
                    forwarder.forwarder.id,
                    'article',
                )
                if (!exist) {
                    to.push(forwarder)
                } else {
                    log?.debug(`[Trace] Article ${article.a_id} already exists for target ${forwarder.forwarder.id}`)
                }
            }
            if (to.length > 0) {
                articles_forwarders.push({
                    article,
                    to,
                })
            }
        }

        if (articles_forwarders.length === 0) {
            log?.debug(`[Trace] No articles need to be sent (All exist or empty)`)
            return
        }

        log?.info(`[Trace] Ready to send ${articles_forwarders.length} articles`)
        articles_forwarders = await this.applyDispatchDigests(log, articles_forwarders, options, context)
        if (this.shouldStopForShutdown(log, 'sendArticles after digest handling')) {
            return
        }
        if (articles_forwarders.length === 0) {
            log?.debug(`[Trace] No articles remain after digest handling`)
            return
        }
        for (const { article, to } of articles_forwarders) {
            if (this.shouldStopForShutdown(log, 'sendArticles article loop')) {
                return
            }
            const platform = article.platform
            const article_is_blocked = options?.forceSend
                ? false
                : (
                      await Promise.all(
                          to.map(({ forwarder: target, runtime_config }) =>
                              target.check_blocked('', {
                                  timestamp: article.created_at,
                                  runtime_config,
                                  article: cloneDeep(article),
                              }),
                          ),
                      )
                  ).every((result) => result)

            if (article_is_blocked) {
                log?.warn(`[Trace] Article ${article.a_id} is blocked by all forwarders, skipping...`)
                for (const { forwarder: target } of to) {
                    const routeKeyForTarget = targetRouteKey(
                        context?.routeKey || routeKey({ source: 'system', crawlerId: 'unknown' }),
                        target.id,
                    )
                    await this.markArticleOutboundSkipped(
                        log,
                        article,
                        target,
                        routeKeyForTarget,
                        'blocked_by_all_forwarders',
                        {
                            skipped: 'blocked_by_all_forwarders',
                        },
                    )
                    await this.claimArticleChain(article, platform, target.id)
                }
                continue
            }

            const renderResult = await this.renderService.process(article, {
                taskId,
                render_type: cfg_forwarder?.render_type,
                render_features: cfg_forwarder?.render_features,
                card_features: cfg_forwarder?.card_features,
                mediaConfig: cfg_forwarder?.media,
                deduplication: options?.forceSend ? false : cfg_forwarder?.deduplication,
            })
            renderResult.mediaFiles ||= []
            renderResult.cardMediaFiles ||= []
            renderResult.originalMediaFiles ||= []

            if (this.shouldStopForShutdown(log, 'sendArticles after render')) {
                this.renderService.cleanup(renderResult.mediaFiles)
                return
            }

            if (renderResult.shouldSkipSend) {
                log?.info(`Skipping article ${article.a_id}: ${renderResult.skipReason || 'deduplicated media'}`)
                for (const { forwarder: target } of to) {
                    const routeKeyForTarget = targetRouteKey(
                        context?.routeKey || routeKey({ source: 'system', crawlerId: 'unknown' }),
                        target.id,
                    )
                    await this.markArticleOutboundSkipped(
                        log,
                        article,
                        target,
                        routeKeyForTarget,
                        renderResult.skipReason || 'deduplicated_media',
                        {
                            skipped: renderResult.skipReason || 'deduplicated_media',
                        },
                        renderResult,
                        options?.forceSend ? taskId : undefined,
                    )
                    await this.claimArticleChain(article, platform, target.id)
                }
                this.renderService.cleanup(renderResult.mediaFiles)
                continue
            }

            let error_for_all = true
            let hadNonErrorOutcome = false
            let forceSendError: Error | null = null
            const cloned_article = cloneDeep(article)
            const errorCounterKey = `${platform}:${cloned_article.a_id}`
            const companionMediaFilesForCleanup: Array<RenderedMediaFile> = []
            await Promise.all(
                to.map(async ({ forwarder: target, runtime_config }) => {
                    let visibilityForRelease: MediaVisibilityResult | null = null
                    let targetRenderResultForCleanup: RenderResult | null = null
                    try {
                        if (this.shouldStopForShutdown(log, `sendArticles target ${target.id}`)) {
                            hadNonErrorOutcome = true
                            return
                        }
                        const routeKeyForTarget = targetRouteKey(
                            context?.routeKey || routeKey({ source: 'system', crawlerId: 'unknown' }),
                            target.id,
                        )

                        if (!options?.forceSend) {
                            const TWO_HOURS_SECONDS = 3600 * 2
                            const now = dayjs().unix()
                            if (now - article.created_at > TWO_HOURS_SECONDS) {
                                const claimed = await this.claimArticleChain(article, platform, target.id)
                                await this.markArticleOutboundSkipped(
                                    log,
                                    article,
                                    target,
                                    routeKeyForTarget,
                                    'old_article',
                                    {
                                        skipped: 'old_article',
                                        created_at: article.created_at,
                                        age_seconds: now - article.created_at,
                                    },
                                    renderResult,
                                )
                                if (claimed) {
                                    log?.info(
                                        `Skipping old article ${article.a_id} (created at ${dayjs.unix(article.created_at).format()}) for target ${target.id}`,
                                    )
                                }
                                hadNonErrorOutcome = true
                                return
                            }

                            const isAggregation = cfg_forwarder?.aggregation || (cfg_forwarder as any)?.batch_mode
                            if (isAggregation && (runtime_config as any)?.bypass_batch !== true) {
                                const claimed = await this.claimArticleChain(article, platform, target.id)
                                await this.markArticleOutboundSkipped(
                                    log,
                                    article,
                                    target,
                                    routeKeyForTarget,
                                    'aggregation_realtime_suppressed',
                                    {
                                        skipped: 'aggregation_realtime_suppressed',
                                    },
                                    renderResult,
                                )
                                if (claimed) {
                                    log?.info(
                                        `Skipping real-time send for ${article.a_id} to ${target.id} (Aggregation/Batch Mode ON)`,
                                    )
                                }
                                hadNonErrorOutcome = true
                                return
                            }

                            const keywords = cfg_forwarder?.keywords
                            if (keywords && keywords.length > 0) {
                                const content = article.content || ''
                                const hasKeyword = keywords.some((keyword) => content.includes(keyword))
                                if (!hasKeyword) {
                                    const claimed = await this.claimArticleChain(article, platform, target.id)
                                    await this.markArticleOutboundSkipped(
                                        log,
                                        article,
                                        target,
                                        routeKeyForTarget,
                                        'keyword_mismatch',
                                        {
                                            skipped: 'keyword_mismatch',
                                            keywords,
                                        },
                                        renderResult,
                                    )
                                    if (claimed) {
                                        log?.debug(
                                            `Article ${article.a_id} does not contain any required keywords, skipping for ${target.id}`,
                                        )
                                    }
                                    hadNonErrorOutcome = true
                                    return
                                }
                            }

                            const queuedForSummary = await this.maybeQueueSummaryCardArticle(
                                log,
                                article,
                                renderResult,
                                target,
                                runtime_config,
                                routeKeyForTarget,
                            )
                            if (queuedForSummary) {
                                hadNonErrorOutcome = true
                                return
                            }
                        }

                        const suppressTranslations = this.shouldSuppressTargetTranslations(target, runtime_config)
                        const stripNativeOriginalTranslations = this.shouldStripNativeOriginalCardTranslations(
                            target,
                            runtime_config,
                        )
                        const targetArticle =
                            suppressTranslations || stripNativeOriginalTranslations
                                ? stripArticleTranslations(article)
                                : article
                        let targetRenderResult = renderResult
                        if (suppressTranslations || stripNativeOriginalTranslations) {
                            targetRenderResult = await this.renderService.process(targetArticle, {
                                taskId: `${taskId}-${target.id}-no-translation`,
                                render_type: cfg_forwarder?.render_type,
                                render_features: cfg_forwarder?.render_features,
                                card_features: cfg_forwarder?.card_features,
                                mediaConfig: cfg_forwarder?.media,
                                deduplication: options?.forceSend ? false : cfg_forwarder?.deduplication,
                            })
                            targetRenderResult.mediaFiles ||= []
                            targetRenderResult.cardMediaFiles ||= []
                            targetRenderResult.originalMediaFiles ||= []
                            targetRenderResultForCleanup = targetRenderResult
                        }

                        if (targetRenderResult.shouldSkipSend) {
                            await this.markArticleOutboundSkipped(
                                log,
                                article,
                                target,
                                routeKeyForTarget,
                                targetRenderResult.skipReason || 'deduplicated_media',
                                {
                                    skipped: targetRenderResult.skipReason || 'deduplicated_media',
                                    suppress_translations: suppressTranslations,
                                    strip_native_original_translations: stripNativeOriginalTranslations,
                                },
                                targetRenderResult,
                                options?.forceSend ? taskId : undefined,
                            )
                            await this.claimArticleChain(article, platform, target.id)
                            hadNonErrorOutcome = true
                            return
                        }

                        const baseText = await this.resolveTargetTextForArticle(
                            targetArticle,
                            targetRenderResult,
                            cfg_forwarder,
                            target,
                            runtime_config,
                        )

                        const outboundIdempotencyKey = articleOutboundKey(target.id, article, {
                            forceKey: options?.forceSend ? taskId : undefined,
                        })

                        let claimed = true
                        if (!options?.forceSend) {
                            claimed = await this.claimArticleChain(article, platform, target.id)
                            if (!claimed) {
                                log?.debug(`[Trace] Article ${article.a_id} already claimed for target ${target.id}`)
                                hadNonErrorOutcome = true
                                return
                            }
                            if (this.shouldStopForShutdown(log, `sendArticles after claim ${target.id}`)) {
                                await this.releaseArticleChain(article, platform, target.id)
                                hadNonErrorOutcome = true
                                return
                            }
                        }

                        const visibility = options?.forceSend
                            ? ({
                                  policy: null,
                                  originalCount: targetRenderResult.originalMediaFiles.length,
                                  visibleFiles: [...targetRenderResult.originalMediaFiles],
                                  hiddenFiles: [],
                                  visibleHashes: new Set<string>(),
                                  hiddenHashes: new Set<string>(),
                                  visibleClaims: [],
                              } satisfies MediaVisibilityResult)
                            : await this.applyTargetMediaVisibility(
                                  article,
                                  target,
                                  runtime_config,
                                  targetRenderResult.originalMediaFiles,
                              )
                        visibilityForRelease = visibility
                        let text = baseText
                        let mediaFiles = targetRenderResult.mediaFiles
                        let cardMediaFiles = targetRenderResult.cardMediaFiles
                        let contentMediaFiles = targetRenderResult.originalMediaFiles

                        if (visibility.policy && visibility.hiddenHashes.size > 0) {
                            mediaFiles = this.filterMediaFilesByVisibility(targetRenderResult.mediaFiles, visibility)
                            contentMediaFiles = this.filterMediaFilesByVisibility(
                                targetRenderResult.originalMediaFiles,
                                visibility,
                            )
                            cardMediaFiles =
                                visibility.policy.duplicateBehavior === 'text_only'
                                    ? []
                                    : this.filterMediaFilesByVisibility(targetRenderResult.cardMediaFiles, visibility)
                            if (visibility.policy.duplicateBehavior === 'text_only') {
                                text = uniquePreserveOrder([
                                    baseText,
                                    this.buildMediaVisibilityTextNotice(visibility.policy),
                                ]).join('\n\n')
                            }
                        }

                        if (
                            visibility.policy?.duplicateBehavior === 'skip' &&
                            visibility.hiddenFiles.length > 0 &&
                            contentMediaFiles.length === 0 &&
                            cardMediaFiles.length === 0
                        ) {
                            const outboundPayloadHash = payloadHash({
                                routeKey: routeKeyForTarget,
                                targetId: target.id,
                                taskKind: options?.forceSend ? 'manual_article' : 'article',
                                text,
                                articleKeys: [articleKey(article)],
                                media: [],
                                extra: { skipped: 'media_visibility_duplicate' },
                            })
                            const outbound = await DB.OutboundMessage.claim({
                                idempotency_key: outboundIdempotencyKey,
                                route_key: routeKeyForTarget,
                                target_id: target.id,
                                target_platform: target.NAME,
                                task_kind: options?.forceSend ? 'manual_article' : 'article',
                                article_key: articleKey(article),
                                payload_hash: outboundPayloadHash,
                            })
                            if (outbound.claimed) {
                                await DB.OutboundMessage.markSkipped(
                                    outboundIdempotencyKey,
                                    'media_visibility_duplicate',
                                    {
                                        hidden_media_count: visibility.hiddenFiles.length,
                                        window_seconds: visibility.policy.windowSeconds,
                                        max_visible: visibility.policy.maxVisible,
                                    },
                                )
                            }
                            log?.debug(
                                `Skipping article ${article.a_id} for ${target.id}: target media visibility duplicate left no visible media`,
                            )
                            error_for_all = false
                            hadNonErrorOutcome = true
                            return
                        }

                        const translatedCompanionCard = suppressTranslations
                            ? null
                            : await this.buildTranslatedNativeCompanionCard(
                                  article,
                                  targetRenderResult,
                                  cfg_forwarder,
                                  target,
                                  runtime_config,
                                  taskId,
                                  cardMediaFiles,
                              )
                        if (translatedCompanionCard) {
                            companionMediaFilesForCleanup.push(...translatedCompanionCard.mediaFiles)
                            if (translatedCompanionCard.cardMediaFiles.length > 0) {
                                mediaFiles = [...mediaFiles, ...translatedCompanionCard.cardMediaFiles]
                                cardMediaFiles = [...cardMediaFiles, ...translatedCompanionCard.cardMediaFiles]
                            }
                            log?.info(
                                `Prepared native translated companion for ${target.id} ${article.a_id}: ` +
                                    `cards=${translatedCompanionCard.cardMediaFiles.length} ` +
                                    `media=${translatedCompanionCard.mediaFiles.length}`,
                            )
                        }

                        const outboundPayloadHash = payloadHash({
                            routeKey: routeKeyForTarget,
                            targetId: target.id,
                            taskKind: options?.forceSend ? 'manual_article' : 'article',
                            text,
                            articleKeys: [articleKey(article)],
                            media: [...mediaFiles, ...cardMediaFiles, ...contentMediaFiles],
                        })

                        if (this.shouldStopForShutdown(log, `sendArticles before outbound ${target.id}`)) {
                            await this.releaseTargetMediaVisibilityClaims(visibilityForRelease).catch(() => undefined)
                            hadNonErrorOutcome = true
                            return
                        }
                        const outbound = await DB.OutboundMessage.claim({
                            idempotency_key: outboundIdempotencyKey,
                            route_key: routeKeyForTarget,
                            target_id: target.id,
                            target_platform: target.NAME,
                            task_kind: options?.forceSend ? 'manual_article' : 'article',
                            article_key: articleKey(article),
                            payload_hash: outboundPayloadHash,
                        })
                        if (!outbound.claimed) {
                            log?.debug(
                                `[Trace] Outbound ${outboundIdempotencyKey} already ${outbound.record.status}; skipping ${article.a_id} for ${target.id}`,
                            )
                            if (isOutboundVisibleCompletionStatus(outbound.record.status)) {
                                await DB.ForwardBy.save(article.id, platform, target.id, 'article')
                            } else if (claimed && !options?.forceSend) {
                                await this.releaseArticleChain(article, platform, target.id)
                            }
                            await this.releaseTargetMediaVisibilityClaims(visibilityForRelease).catch(() => undefined)
                            hadNonErrorOutcome = true
                            return
                        }
                        if (this.shouldStopForShutdown(log, `sendArticles after outbound claim ${target.id}`)) {
                            await DB.OutboundMessage.markFailed(
                                outboundIdempotencyKey,
                                new Error('forwarder_pool_shutdown'),
                            ).catch(() => undefined)
                            if (claimed && !options?.forceSend) {
                                await this.releaseArticleChain(article, platform, target.id)
                            }
                            await this.releaseTargetMediaVisibilityClaims(visibilityForRelease).catch(() => undefined)
                            hadNonErrorOutcome = true
                            return
                        }

                        await DB.OutboundMessage.markSending(outboundIdempotencyKey)
                        const sendResult = await target.send(text, {
                            media: mediaFiles,
                            cardMedia: cardMediaFiles,
                            contentMedia: contentMediaFiles,
                            timestamp: article.created_at,
                            runtime_config,
                            article: cloneDeep(targetArticle),
                            forceSend: options?.forceSend,
                            outboundKey: outboundIdempotencyKey,
                        })
                        if (sendResult.status === 'queued') {
                            await DB.OutboundMessage.markQueued(outboundIdempotencyKey, {
                                reason: sendResult.reason,
                                batchKey: sendResult.batchKey,
                                pendingUnits: sendResult.pendingUnits,
                                threshold: sendResult.threshold,
                            })
                            if (claimed && !options?.forceSend) {
                                await this.releaseArticleChain(article, platform, target.id)
                            }
                            await this.releaseTargetMediaVisibilityClaims(visibilityForRelease).catch(() => undefined)
                            await DB.TargetHealth.mark({
                                target_id: target.id,
                                provider: target.NAME,
                                status: 'ok',
                                last_send_status: 'queued',
                                details: sendResult,
                            })
                            error_for_all = false
                            hadNonErrorOutcome = true
                            return
                        }
                        if (sendResult.status === 'blocked') {
                            await DB.OutboundMessage.markSkipped(outboundIdempotencyKey, sendResult.reason, sendResult)
                            await this.releaseTargetMediaVisibilityClaims(visibilityForRelease).catch(() => undefined)
                            await DB.TargetHealth.mark({
                                target_id: target.id,
                                provider: target.NAME,
                                status: 'ok',
                                last_send_status: 'blocked',
                                details: sendResult,
                            })
                            error_for_all = false
                            hadNonErrorOutcome = true
                            return
                        }
                        if (sendResult.status === 'dry_run') {
                            await DB.OutboundMessage.markDryRun(outboundIdempotencyKey, sendResult)
                            await this.releaseTargetMediaVisibilityClaims(visibilityForRelease).catch(() => undefined)
                            await DB.TargetHealth.mark({
                                target_id: target.id,
                                provider: target.NAME,
                                status: 'ok',
                                last_send_status: 'dry_run',
                                details: sendResult,
                            })
                            if (claimed && !options?.forceSend) {
                                await this.releaseArticleChain(article, platform, target.id)
                            }
                            error_for_all = false
                            hadNonErrorOutcome = true
                            return
                        }
                        const providerResult = getForwarderProviderResult(sendResult)
                        const providerSummary = summarizeProviderResult(providerResult)
                        await DB.OutboundMessage.markSent(outboundIdempotencyKey, providerSummary)
                        await this.markMediaBatchArticlesSent(target, sendResult, providerSummary)
                        await DB.TargetHealth.mark({
                            target_id: target.id,
                            provider: target.NAME,
                            status: 'ok',
                            last_send_status: 'sent',
                            last_provider_code: providerCode(providerResult),
                            details: summarizeProviderResult(providerResult),
                        })
                        if (options?.forceSend) {
                            await DB.ForwardBy.save(article.id, platform, target.id, 'article')
                        }
                        visibilityForRelease = null
                        error_for_all = false
                        hadNonErrorOutcome = true
                    } catch (error) {
                        log?.error(`Error while sending to ${target.id}: ${error}`)
                        const partialError = error instanceof PartialForwarderSendError ? error : null
                        const routeKeyForTarget = targetRouteKey(
                            context?.routeKey || routeKey({ source: 'system', crawlerId: 'unknown' }),
                            target.id,
                        )
                        const outboundIdempotencyKey = articleOutboundKey(target.id, article, {
                            forceKey: options?.forceSend ? taskId : undefined,
                        })
                        if (partialError) {
                            await DB.OutboundMessage.markPartial(
                                outboundIdempotencyKey,
                                summarizeProviderResult(partialError.partialResults),
                                partialError,
                            )
                            await DB.TargetHealth.mark({
                                target_id: target.id,
                                provider: target.NAME,
                                status: 'degraded',
                                last_send_status: 'partial',
                                last_provider_code: providerCode(partialError.partialResults),
                                disabled_reason: partialError.message,
                                details: summarizeProviderResult(partialError.partialResults),
                            })
                            error_for_all = false
                            hadNonErrorOutcome = true
                            return
                        }
                        await this.releaseTargetMediaVisibilityClaims(visibilityForRelease).catch(() => undefined)
                        await DB.OutboundMessage.markFailed(outboundIdempotencyKey, error).catch(() => undefined)
                        await DB.TargetHealth.mark({
                            target_id: target.id,
                            provider: target.NAME,
                            status: 'error',
                            last_send_status: 'failed',
                            disabled_reason: error instanceof Error ? error.message : String(error),
                            details: {
                                route_key: routeKeyForTarget,
                                article_key: articleKey(article),
                            },
                        })
                        if (!options?.forceSend) {
                            await this.releaseArticleChain(article, platform, target.id)
                        }
                    } finally {
                        if (targetRenderResultForCleanup) {
                            this.renderService.cleanup(targetRenderResultForCleanup.mediaFiles)
                        }
                    }
                }),
            )

            if (error_for_all && !hadNonErrorOutcome) {
                if (options?.forceSend) {
                    forceSendError = new Error(`Failed to send article ${cloned_article.a_id} to all targets`)
                } else {
                    let errorCount = this.errorCounter.get(errorCounterKey) || 0
                    errorCount += 1
                    if (errorCount > this.MAX_ERROR_COUNT) {
                        log?.error(
                            `Error count exceeded for ${cloned_article.a_id}, skipping this and tag forwarded...`,
                        )
                        for (const { forwarder: target } of to) {
                            let currentArticle: ArticleWithId | null = cloned_article
                            while (currentArticle && typeof currentArticle === 'object') {
                                await DB.ForwardBy.save(currentArticle.id, platform, target.id, 'article')
                                currentArticle = currentArticle.ref as ArticleWithId | null
                            }
                        }
                        this.errorCounter.delete(errorCounterKey)
                    } else {
                        this.errorCounter.set(errorCounterKey, errorCount)
                        log?.error(`Error count for ${cloned_article.a_id}: ${errorCount}`)
                    }
                }
            } else {
                this.errorCounter.delete(errorCounterKey)
            }

            this.renderService.cleanup([...renderResult.mediaFiles, ...companionMediaFilesForCleanup])
            if (forceSendError) {
                throw forceSendError
            }
        }
    }

    private async markArticleOutboundSkipped(
        log: Logger | undefined,
        article: ArticleWithId,
        target: BaseForwarder,
        routeKeyForTarget: string,
        reason: string,
        details?: unknown,
        renderResult?: Pick<RenderResult, 'text' | 'mediaFiles' | 'cardMediaFiles' | 'originalMediaFiles'>,
        forceKey?: string,
    ) {
        const outboundIdempotencyKey = articleOutboundKey(target.id, article, { forceKey })
        const outboundPayloadHash = payloadHash({
            routeKey: routeKeyForTarget,
            targetId: target.id,
            taskKind: 'article',
            text: renderResult?.text,
            articleKeys: [articleKey(article)],
            media: [
                ...(renderResult?.mediaFiles || []),
                ...(renderResult?.cardMediaFiles || []),
                ...(renderResult?.originalMediaFiles || []),
            ],
            extra: {
                skipped: reason,
                details: details || null,
            },
        })
        try {
            const outbound = await DB.OutboundMessage.claim({
                idempotency_key: outboundIdempotencyKey,
                route_key: routeKeyForTarget,
                target_id: target.id,
                target_platform: target.NAME,
                task_kind: 'article',
                article_key: articleKey(article),
                payload_hash: outboundPayloadHash,
            })
            if (!outbound.claimed) {
                log?.debug(
                    `[Trace] Outbound ${outboundIdempotencyKey} already ${outbound.record.status}; skipping durable skip marker for ${article.a_id} (${reason})`,
                )
                return
            }
            await DB.OutboundMessage.markSkipped(outboundIdempotencyKey, reason, details)
        } catch (error) {
            log?.warn(
                `Failed to mark outbound ${outboundIdempotencyKey} skipped for ${article.a_id}: ${
                    error instanceof Error ? error.message : String(error)
                }`,
            )
        }
    }

    private async resolveTargetTextForArticle(
        article: ArticleWithId,
        renderResult: Pick<RenderResult, 'text' | 'textCollapseMode'>,
        cfg_forwarder: Forwarder['cfg_forwarder'],
        target: BaseForwarder,
        runtime_config?: ForwardTargetPlatformCommonConfig,
    ) {
        const fallbackText = renderResult.text
        if (renderResult.textCollapseMode === 'none') {
            return fallbackText
        }
        if (!this.shouldCollapseForwardedRefText(article, cfg_forwarder, target, runtime_config)) {
            return fallbackText
        }

        const config = target.getEffectiveConfig(runtime_config)
        const collapsedArticleIds = await this.collectForwardedReferenceIds(
            article,
            target.id,
            this.resolvePositiveSeconds(
                config.collapse_forwarded_ref_window_seconds,
                DEFAULT_COLLAPSE_FORWARDED_REF_WINDOW_SECONDS,
            ),
        )
        if (collapsedArticleIds.size === 0) {
            return fallbackText
        }

        return this.renderService.renderText(article, {
            render_type: renderResult.textCollapseMode === 'compact-article' ? 'text-compact' : 'text',
            collapsedArticleIds,
        })
    }

    private shouldSuppressTargetTranslations(
        target: BaseForwarder,
        runtime_config?: ForwardTargetPlatformCommonConfig,
    ) {
        return target.getEffectiveConfig(runtime_config).suppress_translations === true
    }

    private shouldStripNativeOriginalCardTranslations(
        target: BaseForwarder,
        runtime_config?: ForwardTargetPlatformCommonConfig,
    ) {
        const summaryConfig = resolveSummaryCardConfig(target.getEffectiveConfig(runtime_config))
        return Boolean(summaryConfig?.sendFirstNative && summaryConfig.translatedCard)
    }

    private buildSummaryCardWindowKey(
        routeKeyValue: string,
        targetId: string,
        windowStart: number,
        intervalSeconds: number,
    ) {
        return syntheticOutboundKey(targetId, 'summary_window', `${routeKeyValue}:${windowStart}:${intervalSeconds}`)
    }

    private buildSummaryCardSharedRouteKey(targetId: string) {
        return targetRouteKey(
            routeKey({
                source: 'system',
                crawlerId: 'summary-card',
                formatterId: 'all-platforms',
                targetId,
            }),
            targetId,
        )
    }

    private resolveSummaryCardWindowStart(firstQueuedAt: number, config: ResolvedSummaryCardConfig) {
        if (config.windowAlignment === 'hour') {
            return dayjs.unix(firstQueuedAt).startOf('hour').unix()
        }
        if (config.windowAlignment === 'interval') {
            const dayStart = dayjs.unix(firstQueuedAt).startOf('day').unix()
            const offset = Math.max(0, firstQueuedAt - dayStart)
            return dayStart + Math.floor(offset / config.intervalSeconds) * config.intervalSeconds
        }
        return firstQueuedAt
    }

    private async getOrCreateSummaryCardWindow(
        routeKeyValue: string,
        target: BaseForwarder,
        config: ResolvedSummaryCardConfig,
        firstQueuedAt: number,
    ) {
        const windowStart = this.resolveSummaryCardWindowStart(firstQueuedAt, config)
        const baseIdempotencyKey = this.buildSummaryCardWindowKey(
            routeKeyValue,
            target.id,
            windowStart,
            config.intervalSeconds,
        )
        let idempotencyKey = baseIdempotencyKey
        let lastWindow: Awaited<ReturnType<typeof DB.AggregationWindow.getOrCreateOpen>> | undefined

        for (let attempt = 0; attempt < 5; attempt++) {
            const window = await DB.AggregationWindow.getOrCreateOpen({
                idempotency_key: idempotencyKey,
                route_key: routeKeyValue,
                target_id: target.id,
                mode: 'summary_card',
                window_start: windowStart,
                window_end: windowStart + config.intervalSeconds,
            })
            if (window.status === DB.AggregationWindow.STATUS.Open) {
                if (this.isSummaryCardWindowStale(window, config, Math.floor(Date.now() / 1000))) {
                    await DB.AggregationWindow.updateStatus(window.id, DB.AggregationWindow.STATUS.Cancelled, {
                        payload_hash: 'stale-window',
                    }).catch(() => undefined)
                    lastWindow = window
                    idempotencyKey = `${baseIdempotencyKey}:reopen:${window.id}:${firstQueuedAt}:${attempt + 1}`
                    continue
                }
                return window
            }

            lastWindow = window
            idempotencyKey = `${baseIdempotencyKey}:reopen:${window.id}:${firstQueuedAt}:${attempt + 1}`
        }

        this.log?.warn(
            `Unable to allocate open summary-card window for ${target.id}; reusing non-open window ${lastWindow?.id}`,
        )
        return lastWindow!
    }

    private async persistSummaryCardItem(queue: SummaryCardQueue, item: SummaryCardQueueItem) {
        if (!queue.windowId) {
            return
        }
        await DB.AggregationWindow.upsertItem({
            window_id: queue.windowId,
            article_key: articleKey(item.article),
            article_row_id: item.article.id,
            platform: item.article.platform,
            payload: {
                queuedAt: item.queuedAt,
                cardSourceMediaFiles: item.cardSourceMediaFiles,
                originalMediaFiles: item.originalMediaFiles,
                digestTags: item.digestTags,
                runtime_config: queue.runtime_config || null,
                summaryConfig: queue.config,
            },
        })
    }

    private async maybeQueueSummaryCardArticle(
        log: Logger | undefined,
        article: ArticleWithId,
        renderResult: RenderResult,
        target: BaseForwarder,
        runtime_config?: ForwardTargetPlatformCommonConfig,
        routeKeyValue?: string,
    ) {
        void routeKeyValue
        const effectiveConfig = target.getEffectiveConfig(runtime_config)
        const summaryConfig = resolveSummaryCardConfig(effectiveConfig)
        if (!summaryConfig) {
            return false
        }

        if (!this.shouldUseSummaryCardForArticle(article)) {
            return false
        }

        const blocked = await target.check_blocked('', {
            timestamp: article.created_at,
            runtime_config,
            article: cloneDeep(article),
        })
        if (blocked) {
            await this.claimArticleChain(article, article.platform, target.id)
            log?.debug(`Summary-card target ${target.id} blocked ${article.a_id}; claimed without queueing.`)
            return true
        }

        const now = Math.floor(Date.now() / 1000)
        const summaryRouteKey = this.buildSummaryCardSharedRouteKey(target.id)
        const queueKey = this.getSummaryCardQueueKey(summaryRouteKey, target.id, runtime_config, summaryConfig)
        let existingQueue = this.summaryCardQueues.get(queueKey)
        if (existingQueue && this.isSummaryCardQueueDue(existingQueue, now)) {
            log?.debug(
                `Flushing due summary-card queue for ${target.id} before queuing ${article.a_id}; keeping article in next window.`,
            )
            await this.flushSummaryCardQueue(queueKey, 'interval')
            existingQueue = this.summaryCardQueues.get(queueKey)
        }
        const item: SummaryCardQueueItem = {
            article: cloneDeep(article),
            queuedAt: now,
            cardSourceMediaFiles: [...renderResult.originalMediaFiles],
            originalMediaFiles: summaryConfig.includeOriginalMedia ? [...renderResult.originalMediaFiles] : [],
            digestTags: this.resolveActiveTagDigestsForArticle(target.id, article, effectiveConfig),
        }
        if (existingQueue && item.digestTags.length > 0) {
            this.applySummaryCardActiveDigestTags(existingQueue, item.digestTags)
        }

        if (
            !existingQueue &&
            summaryConfig.sendFirstImmediately &&
            (await this.canSendSummaryCardNow(queueKey, summaryConfig, now, target.id))
        ) {
            if (summaryConfig.sendFirstNative) {
                this.markSummaryCardVisibleSent(queueKey, target.id, now)
                log?.debug(`Sending idle-first summary-card item ${article.a_id} natively for ${target.id}.`)
                return false
            }
            log?.debug(
                `Skipping retired idle-first summary-card batch for ${article.a_id} to ${target.id}; queueing for fixed window.`,
            )
        }

        let realtimeMediaResult: SummaryCardRealtimeMediaResult = {
            hadMedia: false,
            handled: true,
            visibleMediaSent: false,
            skippedDuplicate: false,
        }
        if (summaryConfig.mediaRealtime) {
            realtimeMediaResult = await this.sendSummaryCardRealtimeMedia(
                log,
                article,
                target,
                runtime_config,
                summaryRouteKey,
                summaryConfig,
                renderResult,
            )
            if (this.shouldDropSummaryCardAfterRealtimeMedia(article, summaryConfig)) {
                if (realtimeMediaResult.hadMedia && !realtimeMediaResult.handled) {
                    log?.warn(
                        `Not queueing summary-card text for ${article.a_id} to ${target.id}: realtime media failed and platform is media-only`,
                    )
                    return true
                }
                const claimed = await this.claimArticleChain(article, article.platform, target.id)
                if (claimed) {
                    await this.markArticleOutboundSkipped(
                        log,
                        article,
                        target,
                        summaryRouteKey,
                        realtimeMediaResult.hadMedia
                            ? 'summary_realtime_media_only'
                            : 'summary_realtime_media_required_missing',
                        {
                            media_realtime_drop_summary_platforms: summaryConfig.mediaRealtimeDropSummaryPlatforms,
                            realtime_media: realtimeMediaResult,
                        },
                        renderResult,
                    )
                    log?.debug(
                        `Dropped summary-card queue item ${article.a_id} for ${target.id}: platform is realtime-media-only`,
                    )
                }
                return true
            }
        }

        let queue = existingQueue
        if (!queue) {
            const queueWindowStart = this.resolveSummaryCardWindowStart(now, summaryConfig)
            queue = {
                routeKey: summaryRouteKey,
                target,
                runtime_config,
                config: summaryConfig,
                items: new Map<number, SummaryCardQueueItem>(),
                firstQueuedAt: now,
                lastQueuedAt: now,
                windowStart: queueWindowStart,
                windowEnd: queueWindowStart + summaryConfig.intervalSeconds,
            }
        }

        if (!queue.windowId) {
            const window = await this.getOrCreateSummaryCardWindow(
                summaryRouteKey,
                target,
                summaryConfig,
                queue.firstQueuedAt,
            )
            queue.windowId = window.id
            queue.routeKey = window.route_key
            queue.windowStart = Number(window.window_start || queue.windowStart || queue.firstQueuedAt)
            queue.windowEnd = Number(
                window.window_end || queue.windowEnd || queue.firstQueuedAt + summaryConfig.intervalSeconds,
            )
        }
        queue.target = target
        queue.runtime_config = runtime_config
        queue.config = summaryConfig
        queue.lastQueuedAt = now
        queue.items.set(article.id, item)
        await this.persistSummaryCardItem(queue, item)
        this.summaryCardQueues.set(queueKey, queue)
        log?.debug(
            `Queued summary-card item ${article.a_id} for ${target.id}: ${queue.items.size}/${summaryConfig.threshold}`,
        )

        if (
            summaryConfig.flushOnThreshold &&
            queue.items.size >= summaryConfig.threshold &&
            this.canFlushSummaryCardThreshold(queueKey, queue, now)
        ) {
            await this.flushSummaryCardQueue(queueKey, 'threshold')
        }
        return true
    }

    private applySummaryCardActiveDigestTags(queue: SummaryCardQueue, activeTags: Array<string>) {
        const activeKeys = new Map(activeTags.map((tag) => [normalizeHashtagKey(tag), tag]))
        if (activeKeys.size === 0) {
            return
        }

        for (const item of queue.items.values()) {
            const matchedTags = extractArticleHashtags(item.article)
                .map((tag) => activeKeys.get(normalizeHashtagKey(tag)))
                .filter((tag): tag is string => Boolean(tag))
            if (matchedTags.length > 0) {
                item.digestTags = uniquePreserveOrder([...item.digestTags, ...matchedTags])
            }
        }
    }

    private async canSendSummaryCardNow(
        queueKey: string,
        config: ResolvedSummaryCardConfig,
        now: number,
        targetId: string,
    ) {
        const memoryLastSentAt = this.summaryCardLastSentAt.get(queueKey) || 0
        const memoryTargetLastSentAt = this.summaryCardTargetLastSentAt.get(targetId) || 0
        const latestVisibleOutbound = await DB.OutboundMessage.findLatestVisibleCompletion({
            target_id: targetId,
            task_kinds: ['summary_card', 'article'],
        }).catch((error) => {
            this.log?.warn(
                `Failed to read durable summary-card cooldown for ${targetId}: ${
                    error instanceof Error ? error.message : String(error)
                }`,
            )
            return null
        })
        const durableLastSentAt = Number(
            latestVisibleOutbound?.finished_at ||
                latestVisibleOutbound?.updated_at ||
                latestVisibleOutbound?.created_at ||
                0,
        )
        const lastSentAt = Math.max(memoryLastSentAt, memoryTargetLastSentAt, durableLastSentAt)
        if (lastSentAt > 0) {
            this.markSummaryCardVisibleSent(queueKey, targetId, lastSentAt)
        }
        return !lastSentAt || now - lastSentAt >= config.intervalSeconds
    }

    private markSummaryCardVisibleSent(queueKey: string, targetId: string, sentAt: number) {
        this.summaryCardLastSentAt.set(queueKey, sentAt)
        this.summaryCardTargetLastSentAt.set(
            targetId,
            Math.max(this.summaryCardTargetLastSentAt.get(targetId) || 0, sentAt),
        )
    }

    private canFlushSummaryCardThreshold(queueKey: string, queue: SummaryCardQueue, now: number) {
        const effectiveConfig = queue.target.getEffectiveConfig(queue.runtime_config)
        if (!this.isTagDigestEnabled(effectiveConfig)) {
            return true
        }

        const groups = this.buildSummaryCardGroups(Array.from(queue.items.values()))
        if (groups.some((group) => group.kind === 'storm')) {
            return true
        }

        if (this.hasPendingSummaryCardTagStormCandidate(queue, effectiveConfig, now)) {
            return false
        }

        return true
    }

    private buildSummaryCardRealtimeMediaText(
        article: ArticleWithId,
        renderResult: RenderResult,
        config: ResolvedSummaryCardConfig,
    ) {
        if (config.mediaRealtimeText === 'rendered') {
            return renderResult.text || ''
        }
        if (config.mediaRealtimeText === 'metadata') {
            return uniquePreserveOrder([
                formatArticleUserId(article as any).trim(),
                String(article.username || '').trim(),
                formatArticleTimeToken(article.created_at).trim(),
                formatArticleSourceActionAttribution(article as any).trim(),
            ]).join(' ')
        }
        if (config.mediaRealtimeText !== 'basic') {
            return ''
        }

        const headline = extractArticleHeadline(article as any, 160).trim()
        const header = formatArticleHeaderLine(article as any).trim()
        return uniquePreserveOrder([header, headline, article.url || '']).join('\n')
    }

    private shouldDropSummaryCardAfterRealtimeMedia(article: ArticleWithId, config: ResolvedSummaryCardConfig) {
        if (!config.mediaRealtime || config.mediaRealtimeDropSummaryPlatforms.length === 0) {
            return false
        }
        const articlePlatform = getArticlePlatformToken(article)
        return config.mediaRealtimeDropSummaryPlatforms
            .map((value) => normalizePlatformToken(value))
            .some((value) => value === '*' || value === articlePlatform)
    }

    private buildRenderedMediaIdentity(file: RenderedMediaFile) {
        return file.content_hash || file.sourceUrl || file.path
    }

    private buildRenderedMediaIdentityKeys(file: RenderedMediaFile) {
        return uniquePreserveOrder(
            [file.content_hash, file.sourceUrl, file.path].filter((value): value is string => Boolean(value)),
        )
    }

    private buildRenderedMediaIdentityList(mediaFiles: Array<RenderedMediaFile>) {
        return mediaFiles.map((file) => this.buildRenderedMediaIdentity(file)).filter(Boolean)
    }

    private buildTargetMediaVisibilityHash(file: RenderedMediaFile) {
        const identity = this.buildRenderedMediaIdentity(file)
        if (!identity) {
            return null
        }
        return hashValue({
            media_type: file.media_type || '',
            identity,
        })
    }

    private buildTargetMediaVisibilityNamespace(target: BaseForwarder) {
        return `target-media:${hashValue({ target_id: target.id }).slice(0, 32)}`
    }

    private async applyTargetMediaVisibility(
        article: ArticleWithId,
        target: BaseForwarder,
        runtime_config: ForwardTargetPlatformCommonConfig | undefined,
        mediaFiles: Array<RenderedMediaFile>,
    ): Promise<MediaVisibilityResult> {
        const effectiveConfig = target.getEffectiveConfig(runtime_config)
        const policy = resolveMediaVisibilityPolicy(effectiveConfig)
        const emptyResult = {
            policy,
            originalCount: mediaFiles.length,
            visibleFiles: [...mediaFiles],
            hiddenFiles: [],
            visibleHashes: new Set<string>(),
            hiddenHashes: new Set<string>(),
            visibleClaims: [],
        }
        if (!policy || mediaFiles.length === 0 || isNonLiveOutboundSendMode()) {
            return emptyResult
        }

        const namespace = this.buildTargetMediaVisibilityNamespace(target)
        const visibleFiles: RenderedMediaFile[] = []
        const hiddenFiles: RenderedMediaFile[] = []
        const visibleHashes = new Set<string>()
        const hiddenHashes = new Set<string>()
        const visibleClaims: MediaVisibilityResult['visibleClaims'] = []
        const a_id = articleKey(article)

        for (const file of mediaFiles) {
            const mediaHash = this.buildTargetMediaVisibilityHash(file)
            if (!mediaHash) {
                visibleFiles.push(file)
                continue
            }
            const claim = await DB.MediaHash.claimVisibleSlot({
                namespace,
                hash: mediaHash,
                a_id,
                maxVisible: policy.maxVisible,
                windowSeconds: policy.windowSeconds,
            })
            if (claim.allowed) {
                visibleFiles.push(file)
                visibleHashes.add(mediaHash)
                if (claim.slot !== undefined) {
                    visibleClaims.push({
                        platform: `${namespace}:slot:${claim.slot}`,
                        hash: mediaHash,
                        a_id,
                    })
                }
            } else {
                hiddenFiles.push(file)
                hiddenHashes.add(mediaHash)
            }
        }

        return {
            policy,
            originalCount: mediaFiles.length,
            visibleFiles,
            hiddenFiles,
            visibleHashes,
            hiddenHashes,
            visibleClaims,
        }
    }

    private filterMediaFilesByVisibility(
        files: Array<RenderedMediaFile>,
        visibility: MediaVisibilityResult,
    ): Array<RenderedMediaFile> {
        if (!visibility.policy || visibility.hiddenHashes.size === 0) {
            return files
        }
        return files.filter((file) => {
            const mediaHash = this.buildTargetMediaVisibilityHash(file)
            return !mediaHash || !visibility.hiddenHashes.has(mediaHash)
        })
    }

    private buildMediaVisibilityTextNotice(policy: ResolvedMediaVisibilityPolicy) {
        void policy
        return '[图略]'
    }

    private shouldUseSummaryCardForArticle(article: ArticleWithId) {
        if (article.platform !== Platform.Website) {
            return true
        }
        const feed = String((article.extra?.data as any)?.feed || '').trim()
        return feed !== 'official-blog' && article.u_id !== '22/7:official-blog'
    }

    private isSummaryRealtimeMediaEligible(
        target: BaseForwarder,
        file: RenderedMediaFile,
        allFiles: RenderedMediaFile[],
    ) {
        if (file.media_type === 'photo' || file.media_type === 'video') {
            return true
        }
        if (file.media_type !== 'video_thumbnail') {
            return false
        }
        return target.NAME === 'bilibili' && allFiles.some((item) => item.media_type === 'video')
    }

    private shouldAppendSummaryRealtimeCardMedia(target: BaseForwarder, mediaFiles: RenderedMediaFile[]) {
        return (
            target.NAME === 'bilibili' &&
            mediaFiles.length > 0 &&
            mediaFiles.every((file) => file.media_type === 'photo')
        )
    }

    private appendSummaryRealtimeCardMediaForTarget(
        target: BaseForwarder,
        mediaFiles: RenderedMediaFile[],
        renderResult: RenderResult,
    ) {
        if (!this.shouldAppendSummaryRealtimeCardMedia(target, mediaFiles)) {
            return mediaFiles
        }
        const cardMedia = renderResult.cardMediaFiles.find((file) => file.media_type === 'photo')
        if (!cardMedia || mediaFiles.some((file) => file.path === cardMedia.path)) {
            return mediaFiles
        }
        return [...mediaFiles, cardMedia]
    }

    private async buildSummaryRealtimeCardRenderResultForTarget(
        article: ArticleWithId,
        target: BaseForwarder,
        config: ResolvedSummaryCardConfig,
        mediaFiles: RenderedMediaFile[],
        renderResult: RenderResult,
    ) {
        if (!this.shouldAppendSummaryRealtimeCardMedia(target, mediaFiles)) {
            return renderResult
        }

        const translatedCard = config.translatedCard
        if (!translatedCard) {
            return renderResult
        }

        const hasTranslatedContent = translatedCard.processorId
            ? await this.prepareArticleChainTranslations(
                  translatedCard.processorId,
                  [article],
                  `summary realtime Bilibili card ${target.id}`,
              )
            : this.hasArticleChainTranslatedContent([article])
        if (!hasTranslatedContent) {
            this.log?.warn(
                `Using original-only summary realtime Bilibili tail card for ${article.a_id}: translated_card is enabled but no useful translated content is available`,
            )
            return renderResult
        }

        const cardResult = await this.renderService.process(article, {
            taskId: `summary-realtime-card-${target.id}-${article.id || article.a_id}`,
            render_type: 'text-card',
            card_features: ['no-translated-card-pattern', 'no-translated-corner-badge'],
            preloadedMediaFiles: renderResult.originalMediaFiles,
            deduplication: false,
        })
        cardResult.mediaFiles ||= []
        cardResult.cardMediaFiles ||= []
        cardResult.originalMediaFiles ||= []
        if (cardResult.cardMediaFiles.length === 0) {
            this.log?.warn(
                `Falling back to original-only summary realtime Bilibili tail card for ${article.a_id}: translated card render produced no media`,
            )
            return renderResult
        }
        return cardResult
    }

    private async releaseTargetMediaVisibilityClaims(visibility: MediaVisibilityResult | null | undefined) {
        if (!visibility?.policy || visibility.visibleClaims.length === 0 || isNonLiveOutboundSendMode()) {
            return
        }
        await DB.MediaHash.releaseVisibleSlots({
            claims: visibility.visibleClaims,
        })
    }

    private async sendSummaryCardRealtimeMedia(
        log: Logger | undefined,
        article: ArticleWithId,
        target: BaseForwarder,
        runtime_config: ForwardTargetPlatformCommonConfig | undefined,
        routeKeyValue: string,
        config: ResolvedSummaryCardConfig,
        renderResult: RenderResult,
    ): Promise<SummaryCardRealtimeMediaResult> {
        const rawMediaFiles = [...renderResult.originalMediaFiles]
        const mediaFiles = rawMediaFiles.filter((file) =>
            this.isSummaryRealtimeMediaEligible(target, file, rawMediaFiles),
        )
        const cardRenderResult = await this.buildSummaryRealtimeCardRenderResultForTarget(
            article,
            target,
            config,
            mediaFiles,
            renderResult,
        )
        const mediaFilesWithTargetExtras = this.appendSummaryRealtimeCardMediaForTarget(
            target,
            mediaFiles,
            cardRenderResult,
        )
        if (mediaFilesWithTargetExtras.length === 0) {
            return {
                hadMedia: false,
                handled: true,
                visibleMediaSent: false,
                skippedDuplicate: false,
            }
        }

        const mediaIdentities = this.buildRenderedMediaIdentityList(mediaFilesWithTargetExtras)
        const currentArticleKey = articleKey(article)
        const syntheticKey = `${routeKeyValue}:${currentArticleKey}`
        const outboundIdempotencyKey = syntheticOutboundKey(target.id, 'summary_realtime_media', syntheticKey)
        const targetExtraMediaFiles = mediaFilesWithTargetExtras.filter(
            (file) => !mediaFiles.some((mediaFile) => mediaFile.path === file.path),
        )
        const text = this.buildSummaryCardRealtimeMediaText(article, renderResult, config)
        const latestVisibleOutbound = await DB.OutboundMessage.findLatestVisibleCompletion({
            target_id: target.id,
            task_kinds: ['summary_realtime_media'],
            article_key: currentArticleKey,
        }).catch((error) => {
            log?.warn(
                `Failed to read summary realtime media visible completion for ${article.a_id} to ${target.id}: ${
                    error instanceof Error ? error.message : String(error)
                }`,
            )
            return null
        })
        if (latestVisibleOutbound && latestVisibleOutbound.idempotency_key !== outboundIdempotencyKey) {
            log?.debug(
                `Skipping summary realtime media for ${article.a_id} to ${target.id}: article already visibly completed as ${latestVisibleOutbound.status}`,
            )
            return {
                hadMedia: true,
                handled: true,
                visibleMediaSent: true,
                skippedDuplicate: true,
            }
        }
        const visibility = await this.applyTargetMediaVisibility(article, target, runtime_config, mediaFiles)
        const visibleMediaFiles = [...visibility.visibleFiles, ...targetExtraMediaFiles]
        if (visibility.policy?.duplicateBehavior === 'skip' && visibleMediaFiles.length === 0) {
            const outboundPayloadHash = payloadHash({
                routeKey: routeKeyValue,
                targetId: target.id,
                taskKind: 'summary_realtime_media',
                text,
                articleKeys: [currentArticleKey],
                media: [],
                extra: { skipped: 'media_visibility_duplicate', mediaIdentitiesHash: hashValue(mediaIdentities) },
            })
            const outbound = await DB.OutboundMessage.claim({
                idempotency_key: outboundIdempotencyKey,
                route_key: routeKeyValue,
                target_id: target.id,
                target_platform: target.NAME,
                task_kind: 'summary_realtime_media',
                article_key: currentArticleKey,
                synthetic_key: syntheticKey,
                payload_hash: outboundPayloadHash,
            })
            if (outbound.claimed) {
                await DB.OutboundMessage.markSkipped(outboundIdempotencyKey, 'media_visibility_duplicate', {
                    hidden_media_count: visibility.hiddenFiles.length,
                    window_seconds: visibility.policy.windowSeconds,
                    max_visible: visibility.policy.maxVisible,
                })
            }
            log?.debug(
                `Skipping summary realtime media for ${article.a_id} to ${target.id}: target media visibility duplicate`,
            )
            return {
                hadMedia: true,
                handled: true,
                visibleMediaSent: false,
                skippedDuplicate: true,
            }
        }
        if (visibleMediaFiles.length === 0) {
            return {
                hadMedia: true,
                handled: true,
                visibleMediaSent: false,
                skippedDuplicate: false,
            }
        }
        const outboundPayloadHash = payloadHash({
            routeKey: routeKeyValue,
            targetId: target.id,
            taskKind: 'summary_realtime_media',
            text,
            articleKeys: [currentArticleKey],
            media: visibleMediaFiles,
            extra: { mediaIdentitiesHash: hashValue(mediaIdentities) },
        })

        try {
            const outbound = await DB.OutboundMessage.claim({
                idempotency_key: outboundIdempotencyKey,
                route_key: routeKeyValue,
                target_id: target.id,
                target_platform: target.NAME,
                task_kind: 'summary_realtime_media',
                article_key: currentArticleKey,
                synthetic_key: syntheticKey,
                payload_hash: outboundPayloadHash,
            })
            if (!outbound.claimed) {
                log?.debug(
                    `Summary realtime media outbound ${outboundIdempotencyKey} already ${outbound.record.status}; skipping visible media send`,
                )
                await this.releaseTargetMediaVisibilityClaims(visibility).catch(() => undefined)
                const visibleCompletion = isOutboundVisibleCompletionStatus(outbound.record.status)
                return {
                    hadMedia: true,
                    handled: visibleCompletion,
                    visibleMediaSent: visibleCompletion,
                    skippedDuplicate: false,
                }
            }

            await DB.OutboundMessage.markSending(outboundIdempotencyKey)
            const sendResult = await target.send(text, {
                media: visibleMediaFiles,
                contentMedia: visibleMediaFiles,
                timestamp: article.created_at,
                runtime_config,
                article: cloneDeep(article),
                bypassMediaBatch: true,
            })
            if (sendResult.status === 'queued') {
                await DB.OutboundMessage.markQueued(outboundIdempotencyKey, sendResult)
                await this.releaseTargetMediaVisibilityClaims(visibility).catch(() => undefined)
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'ok',
                    last_send_status: 'queued',
                    details: sendResult,
                }).catch(() => undefined)
                return {
                    hadMedia: true,
                    handled: false,
                    visibleMediaSent: false,
                    skippedDuplicate: false,
                }
            }
            if (sendResult.status === 'blocked') {
                await DB.OutboundMessage.markSkipped(outboundIdempotencyKey, sendResult.reason, sendResult)
                await this.releaseTargetMediaVisibilityClaims(visibility).catch(() => undefined)
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'ok',
                    last_send_status: 'blocked',
                    details: sendResult,
                }).catch(() => undefined)
                return {
                    hadMedia: true,
                    handled: false,
                    visibleMediaSent: false,
                    skippedDuplicate: false,
                }
            }
            if (sendResult.status === 'dry_run') {
                await DB.OutboundMessage.markDryRun(outboundIdempotencyKey, sendResult)
                await this.releaseTargetMediaVisibilityClaims(visibility).catch(() => undefined)
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'ok',
                    last_send_status: 'dry_run',
                    details: sendResult,
                }).catch(() => undefined)
                return {
                    hadMedia: true,
                    handled: false,
                    visibleMediaSent: false,
                    skippedDuplicate: false,
                }
            }
            const providerResult = getForwarderProviderResult(sendResult)
            await DB.OutboundMessage.markSent(outboundIdempotencyKey, summarizeProviderResult(providerResult))
            await DB.TargetHealth.mark({
                target_id: target.id,
                provider: target.NAME,
                status: 'ok',
                last_send_status: 'sent',
                last_provider_code: providerCode(providerResult),
                details: summarizeProviderResult(providerResult),
            })
            return {
                hadMedia: true,
                handled: true,
                visibleMediaSent: true,
                skippedDuplicate: false,
            }
        } catch (error) {
            log?.error(`Failed to send summary realtime media for ${article.a_id} to ${target.id}: ${error}`)
            if (error instanceof PartialForwarderSendError) {
                await DB.OutboundMessage.markPartial(
                    outboundIdempotencyKey,
                    summarizeProviderResult(error.partialResults),
                    error,
                ).catch(() => undefined)
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'degraded',
                    last_send_status: 'partial',
                    last_provider_code: providerCode(error.partialResults),
                    disabled_reason: error.message,
                    details: summarizeProviderResult(error.partialResults),
                }).catch(() => undefined)
                return {
                    hadMedia: true,
                    handled: true,
                    visibleMediaSent: true,
                    skippedDuplicate: false,
                }
            }
            await this.releaseTargetMediaVisibilityClaims(visibility).catch(() => undefined)
            await DB.OutboundMessage.markFailed(outboundIdempotencyKey, error).catch(() => undefined)
            await DB.TargetHealth.mark({
                target_id: target.id,
                provider: target.NAME,
                status: 'error',
                last_send_status: 'failed',
                disabled_reason: error instanceof Error ? error.message : String(error),
                details: {
                    route_key: routeKeyValue,
                    task_kind: 'summary_realtime_media',
                    article_key: articleKey(article),
                },
            }).catch(() => undefined)
            return {
                hadMedia: true,
                handled: false,
                visibleMediaSent: false,
                skippedDuplicate: false,
            }
        }
    }

    private hasPendingSummaryCardTagStormCandidate(
        queue: SummaryCardQueue,
        config: ForwardTargetPlatformCommonConfig,
        now: number,
    ) {
        const threshold = Math.max(2, Math.floor(Number(config.tag_digest_threshold || DEFAULT_TAG_DIGEST_THRESHOLD)))
        const minAuthors = Math.max(
            1,
            Math.floor(Number(config.tag_digest_min_authors || DEFAULT_TAG_DIGEST_MIN_AUTHORS)),
        )
        const triggerAuthors = Math.min(minAuthors, threshold)
        const detectionWindow = this.resolvePositiveSeconds(
            config.tag_digest_detection_window_seconds,
            DEFAULT_TAG_DIGEST_DETECTION_WINDOW_SECONDS,
        )

        const candidateCounts = new Map<string, { count: number; authors: Set<string> }>()
        for (const item of queue.items.values()) {
            for (const tag of extractArticleHashtags(item.article)) {
                const state = this.tagDigestStates.get(this.getTagDigestStateKey(queue.target.id, tag))
                if (state?.digestUntil && state.digestUntil >= now) {
                    continue
                }

                const key = normalizeHashtagKey(tag)
                const existing = candidateCounts.get(key) || { count: 0, authors: new Set<string>() }
                if (state) {
                    for (const event of state.events.filter(
                        (candidate) => candidate.timestamp >= now - detectionWindow,
                    )) {
                        existing.count += 1
                        existing.authors.add(event.authorKey)
                    }
                } else {
                    existing.count += 1
                    existing.authors.add(getArticleAuthorKey(item.article))
                }
                candidateCounts.set(key, existing)
            }
        }

        return Array.from(candidateCounts.values()).some(
            (candidate) =>
                candidate.count >= threshold - 1 && candidate.authors.size >= Math.min(triggerAuthors, threshold - 1),
        )
    }

    private getSummaryCardQueueKey(
        routeKeyValue: string,
        targetId: string,
        runtime_config: ForwardTargetPlatformCommonConfig | undefined,
        config: ResolvedSummaryCardConfig,
    ) {
        void runtime_config
        return [
            'summary_card',
            targetId,
            hashValue(routeKeyValue),
            config.intervalSeconds,
            config.threshold,
            config.flushDelaySeconds,
            config.windowAlignment,
            config.flushOnThreshold ? 'threshold' : 'interval',
            config.mediaRealtime ? `media-${config.mediaRealtimeText}` : 'text',
            config.includeOriginalMedia ? 'with-original' : 'card-only',
            config.mediaDuplicateLimit || 0,
            config.translatedCard ? `translated-${config.translatedCard.badgeLabel}` : 'single',
        ].join(':')
    }

    private isSummaryCardWindowStale(
        window: { window_end?: number | null },
        config: ResolvedSummaryCardConfig,
        now: number,
    ) {
        const windowEnd = Number(window.window_end || 0)
        if (!windowEnd) {
            return false
        }
        const graceSeconds = Math.max(DEFAULT_SUMMARY_CARD_STALE_GRACE_SECONDS, config.intervalSeconds * 2)
        return now > windowEnd + config.flushDelaySeconds + graceSeconds
    }

    private async flushDueSummaryCardQueues() {
        const now = Math.floor(Date.now() / 1000)
        let flushed = 0
        for (const [queueKey, queue] of Array.from(this.summaryCardQueues.entries())) {
            if (this.isSummaryCardQueueDue(queue, now)) {
                await this.flushSummaryCardQueue(queueKey, 'interval')
                if (!this.summaryCardQueues.has(queueKey)) {
                    flushed += 1
                    if (flushed >= DEFAULT_SUMMARY_CARD_FLUSHES_PER_TICK) {
                        break
                    }
                }
            }
        }
    }

    private getSummaryCardQueueDueAt(queue: SummaryCardQueue) {
        return queue.config.windowAlignment === 'none'
            ? queue.firstQueuedAt + queue.config.intervalSeconds
            : queue.windowEnd || queue.firstQueuedAt + queue.config.intervalSeconds
    }

    private isSummaryCardQueueDue(queue: SummaryCardQueue, now: number) {
        return queue.items.size > 0 && now >= this.getSummaryCardQueueDueAt(queue) + queue.config.flushDelaySeconds
    }

    private async flushAllSummaryCardQueues() {
        for (const queueKey of Array.from(this.summaryCardQueues.keys())) {
            await this.flushSummaryCardQueue(queueKey, 'shutdown')
        }
    }

    private async flushSummaryCardQueue(queueKey: string, reason: 'threshold' | 'interval' | 'shutdown') {
        const queue = this.summaryCardQueues.get(queueKey)
        if (!queue || queue.items.size === 0) {
            this.summaryCardQueues.delete(queueKey)
            return
        }
        this.summaryCardQueues.delete(queueKey)
        const now = Math.floor(Date.now() / 1000)
        if (queue.windowId && this.isSummaryCardWindowStale({ window_end: queue.windowEnd }, queue.config, now)) {
            await DB.AggregationWindow.updateStatus(queue.windowId, DB.AggregationWindow.STATUS.Cancelled, {
                payload_hash: 'stale-window',
            }).catch((error) => {
                this.log?.warn(
                    `Failed to cancel stale summary-card window ${queue.windowId} for ${queue.target.id}: ${
                        error instanceof Error ? error.message : String(error)
                    }`,
                )
            })
            return
        }
        const claimedItems: SummaryCardQueueItem[] = []
        for (const item of orderBy(
            Array.from(queue.items.values()),
            ['article.created_at', 'article.id'],
            ['asc', 'asc'],
        )) {
            const claimed = await this.claimArticleChain(item.article, item.article.platform, queue.target.id)
            if (claimed) {
                claimedItems.push(item)
            }
        }

        if (claimedItems.length === 0) {
            if (queue.windowId) {
                await DB.AggregationWindow.updateStatus(queue.windowId, DB.AggregationWindow.STATUS.Cancelled, {
                    payload_hash: 'no-claimable-items',
                }).catch((error) => {
                    this.log?.warn(
                        `Failed to cancel empty summary-card window ${queue.windowId} for ${queue.target.id}: ${
                            error instanceof Error ? error.message : String(error)
                        }`,
                    )
                })
            }
            return
        }

        const ok = await this.sendSummaryCardBatch(queue, this.buildSummaryCardGroups(claimedItems), reason)
        if (!ok) {
            for (const item of claimedItems) {
                await this.releaseArticleChain(item.article, item.article.platform, queue.target.id)
            }
            this.summaryCardQueues.set(queueKey, queue)
            this.log?.warn(
                `Retained summary-card queue for ${queue.target.id} after non-terminal ${reason} flush failure`,
            )
        }
    }

    private buildSummaryCardGroups(items: SummaryCardQueueItem[]): SummaryCardGroup[] {
        const stormItems = items.filter((item) => item.digestTags.length > 0)
        const groups: SummaryCardGroup[] = []

        if (stormItems.length > 0) {
            groups.push({
                kind: 'storm',
                label: uniquePreserveOrder(stormItems.flatMap((item) => item.digestTags)).join(' '),
                items: stormItems,
            })
        }

        const threadGroups = new Map<string, SummaryCardQueueItem[]>()
        for (const item of items.filter((candidate) => candidate.digestTags.length === 0)) {
            const key = this.getArticleThreadKey(item.article)
            const existing = threadGroups.get(key) || []
            existing.push(item)
            threadGroups.set(key, existing)
        }

        for (const [key, groupItems] of threadGroups) {
            groups.push({
                kind: 'thread',
                label: key,
                items: groupItems,
            })
        }

        return groups
    }

    private async sendSummaryCardBatch(
        queue: SummaryCardQueue,
        groups: SummaryCardGroup[],
        reason: 'threshold' | 'interval' | 'shutdown',
    ) {
        const allItems = orderBy(
            groups.flatMap((group) => group.items),
            ['article.created_at', 'article.id'],
            ['asc', 'asc'],
        )
        if (allItems.length === 0) {
            return true
        }
        const primaryTextMode: SummaryCardTextMode = queue.config.translatedCard ? 'original' : 'default'
        const content = await this.buildSummaryCardBatchContent(queue, groups, primaryTextMode)
        const hasStorm = groups.some((group) => group.kind === 'storm')
        const totalArticles = allItems.length
        const title =
            hasStorm && groups.length === 1
                ? `话题聚合 ${groups[0]?.label || ''}`.trim()
                : `聚合 ${this.formatSummaryCardRangeForQueue(
                      queue,
                      allItems.map((item) => item.article),
                  )}`
        const now = Math.floor(Date.now() / 1000)
        const primaryCard = await this.buildSummaryCardRenderVariant(
            queue,
            groups,
            allItems,
            title,
            content,
            now,
            primaryTextMode,
        )
        const hasTranslatedContent =
            queue.config.translatedCard && (await this.prepareSummaryCardTranslations(queue, allItems))
        const translatedCard =
            queue.config.translatedCard && hasTranslatedContent
                ? await this.buildSummaryCardRenderVariant(
                      queue,
                      groups,
                      allItems,
                      title,
                      await this.buildSummaryCardBatchContent(queue, groups, 'translated'),
                      now,
                      'translated',
                      queue.config.translatedCard.badgeLabel,
                  )
                : null
        this.log?.info(
            `Prepared message pack card (${reason}) for ${queue.target.id}: ` +
                `articles=${totalArticles} groups=${groups.length} ` +
                `primary_cards=${primaryCard.cardResult.cardMediaFiles.length} ` +
                `translated_cards=${translatedCard?.cardResult.cardMediaFiles.length || 0} ` +
                `translated_enabled=${Boolean(queue.config.translatedCard)} ` +
                `translated_content=${Boolean(hasTranslatedContent)}`,
        )
        const cardResults = [primaryCard.cardResult, translatedCard?.cardResult].filter(
            (result): result is RenderResult => Boolean(result),
        )
        const cardMediaFiles = cardResults.flatMap((result) => result.cardMediaFiles)
        const originalMediaFiles = queue.config.includeOriginalMedia
            ? this.filterSummaryCardRenderedFiles(
                  allItems.flatMap((item) => item.originalMediaFiles),
                  queue.config,
                  primaryCard.mediaUsage,
              )
            : []
        const mediaFiles = [...cardMediaFiles, ...originalMediaFiles]
        const hasRenderedCard = cardMediaFiles.length > 0
        const sendText = this.buildSummaryCardSendText(queue, allItems, title)
        const articleKeys = allItems.map((item) => articleKey(item.article)).sort((a, b) => a.localeCompare(b))
        const syntheticKey = `${queue.routeKey}:${articleKeys.join('|')}`
        const outboundIdempotencyKey = syntheticOutboundKey(queue.target.id, 'summary_card', syntheticKey)
        const outboundPayloadHash = payloadHash({
            routeKey: queue.routeKey,
            targetId: queue.target.id,
            taskKind: 'summary_card',
            text: sendText,
            articleKeys,
            media: mediaFiles,
            extra: {
                reason,
                windowId: queue.windowId || null,
                groupCount: groups.length,
            },
        })
        if (!hasRenderedCard) {
            this.log?.warn(
                `Message pack card render produced no card media; sending compact title fallback to ${queue.target.id}`,
            )
        }

        try {
            const outbound = await DB.OutboundMessage.claim({
                idempotency_key: outboundIdempotencyKey,
                route_key: queue.routeKey,
                target_id: queue.target.id,
                target_platform: queue.target.NAME,
                task_kind: 'summary_card',
                synthetic_key: syntheticKey,
                payload_hash: outboundPayloadHash,
            })
            if (!outbound.claimed) {
                this.log?.debug(
                    `Summary-card outbound ${outboundIdempotencyKey} already ${outbound.record.status}; skipping visible send`,
                )
                if (isOutboundSuppressedCompletionStatus(outbound.record.status)) {
                    const visibleCompletion = isOutboundVisibleCompletionStatus(outbound.record.status)
                    const terminalStatus = visibleCompletion
                        ? DB.AggregationWindow.STATUS.Completed
                        : DB.AggregationWindow.STATUS.Cancelled
                    if (visibleCompletion) {
                        this.markSummaryCardVisibleSent(
                            this.getSummaryCardQueueKey(
                                queue.routeKey,
                                queue.target.id,
                                queue.runtime_config,
                                queue.config,
                            ),
                            queue.target.id,
                            now,
                        )
                    }
                    if (queue.windowId) {
                        await DB.AggregationWindow.updateStatus(queue.windowId, terminalStatus, {
                            payload_hash: outboundPayloadHash,
                        }).catch(() => undefined)
                    }
                    return true
                }
                return false
            }

            await DB.OutboundMessage.markSending(outboundIdempotencyKey)
            const sendResult = await queue.target.send(sendText, {
                media: mediaFiles,
                cardMedia: cardMediaFiles,
                contentMedia: originalMediaFiles,
                timestamp: now,
                runtime_config: queue.runtime_config,
                article: primaryCard.summaryArticle,
                forceSend: true,
            })
            if (sendResult.status === 'queued') {
                await DB.OutboundMessage.markQueued(outboundIdempotencyKey, sendResult)
                await DB.TargetHealth.mark({
                    target_id: queue.target.id,
                    provider: queue.target.NAME,
                    status: 'ok',
                    last_send_status: 'queued',
                    details: sendResult,
                }).catch(() => undefined)
                return false
            }
            if (sendResult.status === 'blocked') {
                await DB.OutboundMessage.markSkipped(outboundIdempotencyKey, sendResult.reason, sendResult)
                if (queue.windowId) {
                    await DB.AggregationWindow.updateStatus(queue.windowId, DB.AggregationWindow.STATUS.Cancelled, {
                        payload_hash: outboundPayloadHash,
                    }).catch(() => undefined)
                }
                await DB.TargetHealth.mark({
                    target_id: queue.target.id,
                    provider: queue.target.NAME,
                    status: 'ok',
                    last_send_status: 'blocked',
                    details: sendResult,
                }).catch(() => undefined)
                return true
            }
            if (sendResult.status === 'dry_run') {
                await DB.OutboundMessage.markDryRun(outboundIdempotencyKey, sendResult)
                await DB.TargetHealth.mark({
                    target_id: queue.target.id,
                    provider: queue.target.NAME,
                    status: 'ok',
                    last_send_status: 'dry_run',
                    details: sendResult,
                }).catch(() => undefined)
                return false
            }
            const providerResult = getForwarderProviderResult(sendResult)
            await DB.OutboundMessage.markSent(outboundIdempotencyKey, summarizeProviderResult(providerResult))
            await DB.TargetHealth.mark({
                target_id: queue.target.id,
                provider: queue.target.NAME,
                status: 'ok',
                last_send_status: 'sent',
                last_provider_code: providerCode(providerResult),
                details: summarizeProviderResult(providerResult),
            })
            if (queue.windowId) {
                await DB.AggregationWindow.updateStatus(queue.windowId, DB.AggregationWindow.STATUS.Completed, {
                    payload_hash: outboundPayloadHash,
                }).catch(() => undefined)
            }
            this.markSummaryCardVisibleSent(
                this.getSummaryCardQueueKey(queue.routeKey, queue.target.id, queue.runtime_config, queue.config),
                queue.target.id,
                now,
            )
            this.log?.info(
                `Sent message pack card (${reason}) with ${totalArticles} articles in ${groups.length} section(s) to ${queue.target.id}`,
            )
            return true
        } catch (error) {
            this.log?.error(`Failed to send message pack card to ${queue.target.id}: ${error}`)
            if (error instanceof PartialForwarderSendError) {
                await DB.OutboundMessage.markPartial(
                    outboundIdempotencyKey,
                    summarizeProviderResult(error.partialResults),
                    error,
                ).catch(() => undefined)
                await DB.TargetHealth.mark({
                    target_id: queue.target.id,
                    provider: queue.target.NAME,
                    status: 'degraded',
                    last_send_status: 'partial',
                    last_provider_code: providerCode(error.partialResults),
                    disabled_reason: error.message,
                    details: summarizeProviderResult(error.partialResults),
                }).catch(() => undefined)
                if (queue.windowId) {
                    await DB.AggregationWindow.updateStatus(queue.windowId, DB.AggregationWindow.STATUS.Completed, {
                        payload_hash: outboundPayloadHash,
                    }).catch(() => undefined)
                }
                this.markSummaryCardVisibleSent(
                    this.getSummaryCardQueueKey(queue.routeKey, queue.target.id, queue.runtime_config, queue.config),
                    queue.target.id,
                    now,
                )
                return true
            }
            await DB.OutboundMessage.markFailed(outboundIdempotencyKey, error).catch(() => undefined)
            await DB.TargetHealth.mark({
                target_id: queue.target.id,
                provider: queue.target.NAME,
                status: 'error',
                last_send_status: 'failed',
                disabled_reason: error instanceof Error ? error.message : String(error),
                details: {
                    route_key: queue.routeKey,
                    task_kind: 'summary_card',
                    article_keys: articleKeys,
                },
            }).catch(() => undefined)
            if (queue.windowId) {
                await DB.AggregationWindow.updateStatus(queue.windowId, DB.AggregationWindow.STATUS.Open, {
                    payload_hash: outboundPayloadHash,
                }).catch(() => undefined)
            }
            return false
        } finally {
            this.renderService.cleanup(cardResults.flatMap((result) => result.mediaFiles))
        }
    }

    private flattenSummaryArticleChain(article: ArticleWithId | Article) {
        const chain: Array<ArticleWithId | Article> = []
        let current: ArticleWithId | Article | null = article
        const visitedIds = new Set<string>()
        const visitedObjects = new WeakSet<object>()
        while (current && typeof current === 'object') {
            if (visitedObjects.has(current)) {
                break
            }
            visitedObjects.add(current)
            const stableId = String((current as any).id ?? current.a_id ?? '').trim()
            if (stableId) {
                const key = `${current.platform}:${stableId}`
                if (visitedIds.has(key)) {
                    break
                }
                visitedIds.add(key)
            }
            chain.push(current)
            current = current.ref && typeof current.ref === 'object' ? (current.ref as ArticleWithId | Article) : null
        }
        return chain
    }

    private resolveSummaryCardProcessorDefinition(processorId?: string) {
        const normalized = String(processorId || '').trim()
        if (!normalized) {
            return null
        }
        return this.processors.find((processor) => processor.id === normalized || processor.name === normalized) || null
    }

    private async getSummaryCardProcessor(processorId?: string) {
        const processorDef = this.resolveSummaryCardProcessorDefinition(processorId)
        if (!processorDef) {
            if (processorId) {
                this.log?.warn(`Summary-card translation processor not found: ${processorId}`)
            }
            return null
        }

        const cacheKey = processorDef.id || processorDef.name || processorId || processorDef.provider
        const cached = this.summaryCardProcessors.get(cacheKey)
        if (cached) {
            return cached
        }

        try {
            const processor = await processorRegistry.create(
                processorDef.provider,
                processorDef.api_key,
                this.log,
                processorDef.cfg_processor,
            )
            this.summaryCardProcessors.set(cacheKey, processor)
            return processor
        } catch (error) {
            this.log?.warn(
                `Failed to initialize summary-card translation processor ${cacheKey}: ${
                    error instanceof Error ? error.message : String(error)
                }`,
            )
            return null
        }
    }

    private buildOrderedTranslationInput(
        rootArticle: ArticleWithId,
        targetArticle: ArticleWithId | Article,
        sourceText: string,
        fieldLabel: string,
    ) {
        const chain = this.flattenSummaryArticleChain(rootArticle).reverse()
        const sourceHashtags = extractHashtagsFromText(sourceText)
        if (chain.length <= 1 && sourceHashtags.length === 0) {
            return sourceText
        }

        const lines = [
            '请只翻译【当前待译】段落，输出简体中文译文，不要输出原文、序号、说明或上下文。',
            '保留所有 hashtag 原文（例如 #ナナニジ），不要翻译、改写、删除或增减。',
        ]
        if (chain.length <= 1) {
            lines.push(`【当前待译字段】${fieldLabel}`, '【当前待译】', sourceText)
            return lines.join('\n\n')
        }

        const targetStableId = String((targetArticle as any).id ?? targetArticle.a_id ?? '')
        lines.push('以下按发生顺序排列：第1条最先发生，序号越大越后发生。')
        for (const [index, article] of chain.entries()) {
            const stableId = String((article as any).id ?? article.a_id ?? '')
            const isTarget = stableId === targetStableId
            const orderLabel =
                index === 0 ? '最先发生' : index === chain.length - 1 ? '最后发生' : `第${index + 1}条发生`
            const body = isTarget ? sourceText : String(article.content || '').trim()
            if (!body) {
                continue
            }
            lines.push(`【第${index + 1}条/${orderLabel}${isTarget ? '/当前待译' : '/上下文'}】`, body)
        }
        lines.push(`【当前待译字段】${fieldLabel}`)
        return lines.join('\n\n')
    }

    private async processSummaryCardTranslationText(processor: BaseProcessor, inputText: string, sourceText: string) {
        const translateOnce = async (text: string) =>
            await pRetry(() => processor.process(text), {
                retries: RETRY_LIMIT,
            })
                .then((value) => preserveSourceHashtags(sourceText, value))
                .catch((error) => {
                    this.log?.error(`Summary-card translation processor failed: ${error}`)
                    return PROCESSOR_ERROR_FALLBACK
                })

        const firstResult = await translateOnce(inputText)
        if (this.isUsefulSummaryCardTranslation(sourceText, firstResult) || !hasJapaneseKana(sourceText)) {
            return firstResult
        }

        const retryInput = [
            '上一次输出疑似没有把日文翻成简体中文。请重新翻译下面文本。',
            '只输出简体中文译文；保留 URLs、@handles、hashtags、emoji、数字、日期、时间和专有名词。',
            '如果有 hashtag，保留 hashtag 原文。',
            '',
            sourceText,
        ].join('\n')
        const retryResult = await translateOnce(retryInput)
        if (!this.isUsefulSummaryCardTranslation(sourceText, retryResult)) {
            this.log?.warn(
                `Summary-card translation looked unchanged; suppressing translated text for source: ${sourceText.slice(
                    0,
                    80,
                )}`,
            )
            return PROCESSOR_ERROR_FALLBACK
        }
        return retryResult
    }

    private isUsefulSummaryCardTranslation(
        sourceText: string | null | undefined,
        translatedText: string | null | undefined,
    ) {
        if (!BaseProcessor.isValidResult(translatedText)) {
            return false
        }
        const sourceComparable = normalizeTranslationComparableText(sourceText)
        const translatedComparable = normalizeTranslationComparableText(translatedText)
        if (!sourceComparable || !translatedComparable) {
            return true
        }
        if (sourceComparable === translatedComparable && hasJapaneseKana(sourceText)) {
            return false
        }
        return true
    }

    private async translateSummaryCardField(
        processor: BaseProcessor,
        rootArticle: ArticleWithId,
        article: ArticleWithId | Article,
        sourceText: string,
        fieldLabel: string,
    ) {
        return await this.processSummaryCardTranslationText(
            processor,
            this.buildOrderedTranslationInput(rootArticle, article, sourceText, fieldLabel),
            sourceText,
        )
    }

    private async translateSummaryCardFieldIfUseful(
        processor: BaseProcessor,
        rootArticle: ArticleWithId,
        article: ArticleWithId | Article,
        sourceText: string,
        fieldLabel: string,
    ) {
        const result = await this.translateSummaryCardField(processor, rootArticle, article, sourceText, fieldLabel)
        return this.isUsefulSummaryCardTranslation(sourceText, result) ? result : null
    }

    private collectArticleChainTranslationCoverage(articles: Array<ArticleWithId | Article>) {
        const checks: boolean[] = []
        for (const item of articles) {
            for (const article of this.flattenSummaryArticleChain(item)) {
                if (String(article.content || '').trim()) {
                    checks.push(this.isUsefulSummaryCardTranslation(article.content, article.translation))
                }
                for (const media of article.media || []) {
                    if (String(media.alt || '').trim()) {
                        checks.push(this.isUsefulSummaryCardTranslation(media.alt, (media as any).translation))
                    }
                }
                if (String((article.extra as any)?.content || '').trim()) {
                    checks.push(
                        this.isUsefulSummaryCardTranslation(
                            (article.extra as any)?.content,
                            (article.extra as any)?.translation,
                        ),
                    )
                }
            }
        }
        return {
            total: checks.length,
            translated: checks.filter(Boolean).length,
            complete: checks.length > 0 && checks.every(Boolean),
        }
    }

    private hasArticleChainTranslatedContent(articles: Array<ArticleWithId | Article>) {
        return this.collectArticleChainTranslationCoverage(articles).complete
    }

    private async prepareArticleChainTranslations(
        processorId: string | undefined,
        articles: ArticleWithId[],
        contextLabel: string,
    ) {
        if (!processorId) {
            return this.hasArticleChainTranslatedContent(articles)
        }

        const processor = await this.getSummaryCardProcessor(processorId)
        if (!processor) {
            return this.hasArticleChainTranslatedContent(articles)
        }

        const seenArticles = new Set<string>()
        let updatedArticles = 0
        for (const item of articles) {
            for (const article of this.flattenSummaryArticleChain(item).reverse()) {
                const key = articleTranslationIdentityKey(article)
                if (seenArticles.has(key)) {
                    continue
                }
                seenArticles.add(key)

                const patch: Partial<Article> = {}
                if (article.content && !this.isUsefulSummaryCardTranslation(article.content, article.translation)) {
                    const translation = await this.translateSummaryCardFieldIfUseful(
                        processor,
                        item,
                        article,
                        article.content,
                        '正文',
                    )
                    if (translation) {
                        patch.translation = translation
                        patch.translated_by = processor.NAME
                        article.translation = patch.translation
                        article.translated_by = processor.NAME
                    }
                }

                if (article.media) {
                    let changed = false
                    const updatedMedia = await Promise.all(
                        article.media.map(async (media) => {
                            if (
                                !media.alt ||
                                this.isUsefulSummaryCardTranslation(media.alt, (media as any).translation)
                            ) {
                                return media
                            }
                            const translation = await this.translateSummaryCardFieldIfUseful(
                                processor,
                                item,
                                article,
                                media.alt,
                                '图片说明',
                            )
                            if (!translation) {
                                return media
                            }
                            changed = true
                            return {
                                ...media,
                                translation,
                                translated_by: processor.NAME,
                            }
                        }),
                    )
                    if (changed) {
                        patch.media = updatedMedia as any
                        article.media = updatedMedia as any
                    }
                }

                if (
                    article.extra?.content &&
                    !this.isUsefulSummaryCardTranslation(article.extra.content, (article.extra as any).translation)
                ) {
                    const translation = await this.translateSummaryCardFieldIfUseful(
                        processor,
                        item,
                        article,
                        article.extra.content,
                        '补充内容',
                    )
                    if (translation) {
                        patch.extra = {
                            ...article.extra,
                            translation,
                            translated_by: processor.NAME,
                        } as any
                        article.extra = patch.extra as any
                    }
                }

                if (Object.keys(patch).length > 0) {
                    const articleId = Number((article as any).id)
                    if (Number.isInteger(articleId) && articleId > 0) {
                        await DB.Article.update(articleId, article.platform, patch)
                    }
                    updatedArticles += 1
                }
            }
        }

        if (updatedArticles > 0) {
            this.log?.info(`Translated ${updatedArticles} article(s) for ${contextLabel} using ${processor.NAME}`)
        }
        return this.hasArticleChainTranslatedContent(articles)
    }

    private formatSummaryCardSendTextUser(article: ArticleWithId | Article | null | undefined) {
        const value = String(article?.u_id || article?.username || article?.a_id || 'unknown').trim()
        return value.replace(/^@+/, '') || 'unknown'
    }

    private formatSummaryCardSendTextAction(article: ArticleWithId | Article) {
        if (article.platform === Platform.X) {
            const xActions: Record<string, string> = {
                tweet: 'x发推',
                retweet: 'x转推',
                reply: 'x回复',
                conversation: 'x回复',
                quoted: 'x引用',
            }
            return xActions[article.type] || 'x更新'
        }
        if (article.platform === Platform.Instagram) {
            return article.type === 'story' ? 'ig故事' : 'ig发帖'
        }
        if (article.platform === Platform.TikTok) {
            return 'tt视频'
        }
        if (article.platform === Platform.YouTube) {
            return article.type === 'shorts' ? 'yt短视频' : 'yt视频'
        }
        if (article.platform === Platform.Website) {
            return 'web更新'
        }
        return formatArticleSourceActionAttribution(article as any).replace(/^[^\s]+\s+/, '') || '更新'
    }

    private buildSummaryCardSendTextItemParts(article: ArticleWithId | Article): SummaryCardSendTextItemParts {
        const actor = this.formatSummaryCardSendTextUser(article)
        const action = this.formatSummaryCardSendTextAction(article)
        const ref =
            article.platform === Platform.X &&
            article.ref &&
            typeof article.ref === 'object' &&
            ['retweet', 'quoted', 'reply', 'conversation'].includes(String(article.type || ''))
                ? this.formatSummaryCardSendTextUser(article.ref as ArticleWithId | Article)
                : ''
        return { actor, action, ref }
    }

    private formatSummaryCardSendTextIndexList(indices: number[]) {
        const sortedIndices = indices.slice().sort((a, b) => a - b)
        const ranges: string[] = []
        let start = sortedIndices[0]
        let previous = sortedIndices[0]

        for (const index of sortedIndices.slice(1)) {
            if (index === previous + 1) {
                previous = index
                continue
            }
            ranges.push(start === previous ? String(start) : `${start}~${previous}`)
            start = index
            previous = index
        }

        if (start !== undefined && previous !== undefined) {
            ranges.push(start === previous ? String(start) : `${start}~${previous}`)
        }

        return ranges.join(',')
    }

    private formatSummaryCardSendTextDigestEntry(indices: number[], parts: SummaryCardSendTextItemParts) {
        const refSeparator = parts.ref && parts.action.includes('/') ? ' ' : ''
        return `${this.formatSummaryCardSendTextIndexList(indices)}. ${parts.actor} ${parts.action}${refSeparator}${
            parts.ref
        }`
    }

    private isSummaryCardRetweetQuoteParts(parts: SummaryCardSendTextItemParts) {
        return Boolean(parts.ref) && (parts.action === 'x转推' || parts.action === 'x引用')
    }

    private formatSummaryCardRetweetQuoteAction(actions: string[]) {
        const uniqueActions = new Set(actions)
        if (uniqueActions.has('x转推') && uniqueActions.has('x引用')) {
            return 'x转推/引用'
        }
        if (uniqueActions.has('x转推')) {
            return 'x转推'
        }
        if (uniqueActions.has('x引用')) {
            return 'x引用'
        }
        return actions[0] || 'x更新'
    }

    private buildSummaryCardSendTextDigestItems(items: SummaryCardQueueItem[]) {
        const entries = items.map((item, index) => ({
            index: index + 1,
            parts: this.buildSummaryCardSendTextItemParts(item.article),
        }))
        const consumed = new Set<number>()
        const digestEntries: Array<{ firstIndex: number; text: string }> = []

        for (let cursor = 0; cursor < entries.length; cursor += 1) {
            const first = entries[cursor]
            if (!first || !this.isSummaryCardRetweetQuoteParts(first.parts)) {
                continue
            }
            let end = cursor + 1
            while (
                end < entries.length &&
                this.isSummaryCardRetweetQuoteParts(entries[end].parts) &&
                entries[end].parts.actor === first.parts.actor &&
                entries[end].parts.ref === first.parts.ref
            ) {
                end += 1
            }

            const run = entries.slice(cursor, end)
            const actions = uniquePreserveOrder(run.map((entry) => entry.parts.action))
            if (run.length >= 2 && actions.length >= 2) {
                for (const entry of run) {
                    consumed.add(entry.index)
                }
                digestEntries.push({
                    firstIndex: first.index,
                    text: this.formatSummaryCardSendTextDigestEntry(
                        run.map((entry) => entry.index),
                        {
                            actor: first.parts.actor,
                            action: this.formatSummaryCardRetweetQuoteAction(actions),
                            ref: first.parts.ref,
                        },
                    ),
                })
                cursor = end - 1
            }
        }

        const exactGroups = new Map<
            string,
            {
                parts: SummaryCardSendTextItemParts
                indices: number[]
            }
        >()
        for (const entry of entries) {
            if (consumed.has(entry.index)) {
                continue
            }
            const key = [entry.parts.actor, entry.parts.action, entry.parts.ref].join('\u0000')
            const group = exactGroups.get(key)
            if (group) {
                group.indices.push(entry.index)
            } else {
                exactGroups.set(key, { parts: entry.parts, indices: [entry.index] })
            }
        }

        for (const group of exactGroups.values()) {
            digestEntries.push({
                firstIndex: group.indices[0],
                text: this.formatSummaryCardSendTextDigestEntry(group.indices, group.parts),
            })
        }

        return digestEntries
            .sort((a, b) => a.firstIndex - b.firstIndex)
            .map((entry) => entry.text)
            .join(' ')
    }

    private buildSummaryCardSendText(queue: SummaryCardQueue, items: SummaryCardQueueItem[], fallbackTitle: string) {
        const range = this.formatSummaryCardRangeForQueue(
            queue,
            items.map((item) => item.article),
            { spaced: true },
        )
        const itemLine = this.buildSummaryCardSendTextDigestItems(items)
        return [`聚合 ${range || fallbackTitle.replace(/^聚合\s*/, '')}`.trim(), itemLine].filter(Boolean).join('\n')
    }

    private async prepareSummaryCardTranslations(queue: SummaryCardQueue, items: SummaryCardQueueItem[]) {
        const processorId = queue.config.translatedCard?.processorId
        const articles = items.map((item) => item.article)
        if (!processorId) {
            this.log?.warn(
                `Summary-card translated companion enabled for ${queue.target.id} without processor_id; using existing translations only`,
            )
            return this.hasArticleChainTranslatedContent(articles)
        }
        return this.prepareArticleChainTranslations(processorId, articles, `summary-card companion ${queue.target.id}`)
    }

    private async buildSummaryCardRenderVariant(
        queue: SummaryCardQueue,
        groups: SummaryCardGroup[],
        allItems: SummaryCardQueueItem[],
        title: string,
        content: string,
        now: number,
        textMode: SummaryCardTextMode,
        translatedBadgeLabel?: string,
    ) {
        const mediaUsage: SummaryCardMediaUsage = new Map()
        const renderMeta = await this.buildSummaryCardRenderMeta(queue, groups, mediaUsage, {
            textMode,
            translatedBadgeLabel,
        })
        const embeddedMedia = this.collectSummaryCardEmbeddedMedia(
            allItems,
            DEFAULT_SUMMARY_CARD_MAX_EMBEDDED_MEDIA,
            queue.config,
        )
        const summaryArticle = this.buildSyntheticSummaryArticle(
            title,
            content,
            allItems[0]?.article,
            now,
            embeddedMedia,
            renderMeta,
        )
        const cardFeatures = textMode === 'translated' ? ['translated-corner-badge'] : undefined
        const cardResult = await this.renderService.process(summaryArticle, {
            taskId: `summary-card-${queue.target.id}-${now}-${textMode}`,
            render_type: 'text-card',
            card_features: cardFeatures,
            deduplication: false,
        })
        return {
            summaryArticle,
            cardResult,
            mediaUsage,
        }
    }

    private async buildSummaryCardBatchContent(
        queue: SummaryCardQueue,
        groups: SummaryCardGroup[],
        textMode: SummaryCardTextMode = 'default',
    ) {
        const sortedGroups = orderBy(groups, [(group) => group.items[0]?.article.created_at || 0], ['asc'])
        const allItems = sortedGroups.flatMap((group) => group.items)
        if (sortedGroups.length === 1) {
            const group = sortedGroups[0]
            return this.buildSummaryCardContent(queue, group.kind, group.items, textMode)
        }

        const sections = await Promise.all(
            sortedGroups.map(async (group, index) => {
                const sectionTitle =
                    group.kind === 'storm'
                        ? `【${index + 1}. 话题串】${group.label}`
                        : `【${index + 1}. 消息串】${this.formatSummaryCardRangeForQueue(
                              queue,
                              group.items.map((item) => item.article),
                          )}`
                return [
                    sectionTitle,
                    await this.buildSummaryCardContent(queue, group.kind, group.items, textMode),
                ].join('\n')
            }),
        )

        return [
            `【聚合】${this.formatSummaryCardRangeForQueue(
                queue,
                allItems.map((item) => item.article),
                { spaced: true },
            )}`,
            ...sections,
        ].join('\n\n')
    }

    private buildSyntheticSummaryArticle(
        title: string,
        content: string,
        sourceArticle: ArticleWithId | undefined,
        now: number,
        media: NonNullable<Article['media']>,
        renderMeta?: Record<string, unknown>,
    ): ArticleWithId {
        return {
            id: -now,
            platform: sourceArticle?.platform || Platform.X,
            a_id: `summary-card-${now}`,
            u_id: 'message_pack',
            username: '聚合',
            created_at: now,
            content: `${title}\n\n${content}`,
            url: sourceArticle?.url || '',
            type: 'message_pack' as any,
            ref: null,
            has_media: media.length > 0,
            media,
            extra: renderMeta
                ? ({
                      extra_type: 'message_pack_meta',
                      data: renderMeta,
                  } as any)
                : null,
            u_avatar: sourceArticle?.u_avatar || null,
        }
    }

    private async buildSummaryCardRenderMeta(
        queue: SummaryCardQueue,
        groups: SummaryCardGroup[],
        mediaUsage?: SummaryCardMediaUsage,
        options: { textMode?: SummaryCardTextMode; translatedBadgeLabel?: string } = {},
    ) {
        const sortedGroups = orderBy(groups, [(group) => group.items[0]?.article.created_at || 0], ['asc'])
        const allItems = sortedGroups.flatMap((group) => group.items)
        const groupMetas: Array<Record<string, unknown>> = []
        for (const [index, group] of sortedGroups.entries()) {
            const shown = group.items.slice(0, queue.config.maxItems)
            const tags =
                group.kind === 'storm' ? uniquePreserveOrder(group.items.flatMap((item) => item.digestTags)) : []
            const title =
                group.kind === 'storm'
                    ? `${sortedGroups.length > 1 ? `${index + 1}. ` : ''}话题串 ${group.label}`.trim()
                    : `${sortedGroups.length > 1 ? `${index + 1}. ` : ''}消息串 ${this.formatSummaryCardRangeForQueue(
                          queue,
                          group.items.map((item) => item.article),
                      )}`.trim()
            const avatars = this.collectSummaryCardGroupAvatars(group.items)
            const itemMetas: Array<Record<string, unknown>> = []
            for (const [itemIndex, item] of shown.entries()) {
                const nonStormTags =
                    group.kind === 'storm'
                        ? extractArticleHashtags(item.article).filter(
                              (tag) =>
                                  !tags.some((stormTag) => normalizeHashtagKey(stormTag) === normalizeHashtagKey(tag)),
                          )
                        : []
                const media = this.buildSummaryCardItemMedia(item, 4, queue.config, mediaUsage)
                const omittedMedia = this.countSummaryCardItemMedia(item) > media.length
                const itemText = await this.buildSummaryCardItemText(
                    queue,
                    item.article,
                    nonStormTags,
                    options.textMode || 'default',
                )
                itemMetas.push({
                    index: itemIndex + 1,
                    text: omittedMedia ? [itemText, '[图略]'].filter(Boolean).join('\n') : itemText,
                    avatar: this.buildSummaryCardAvatar(item.article),
                    media,
                    mediaLabel: media.length > 0 ? `#${itemIndex + 1} 图集` : undefined,
                })
            }
            groupMetas.push({
                kind: group.kind,
                label: group.label,
                title,
                range: this.formatSummaryCardRangeForQueue(
                    queue,
                    group.items.map((item) => item.article),
                ),
                omitted: Math.max(0, group.items.length - shown.length),
                avatars,
                items: itemMetas,
            })
        }
        return {
            total: allItems.length,
            range: this.formatSummaryCardRangeForQueue(
                queue,
                allItems.map((item) => item.article),
            ),
            groups: groupMetas,
            ...(options.textMode === 'translated'
                ? {
                      translated_badge_label: options.translatedBadgeLabel || '译文',
                  }
                : {}),
        }
    }

    private collectSummaryCardGroupAvatars(items: SummaryCardQueueItem[]) {
        const seen = new Set<string>()
        const avatars: Array<Record<string, string>> = []
        const visit = (article?: ArticleWithId | Article | null) => {
            if (!article || avatars.length >= 5) {
                return
            }
            const avatar = this.buildSummaryCardAvatar(article)
            const key = avatar.url || avatar.id || avatar.name
            if (key && !seen.has(key)) {
                seen.add(key)
                avatars.push(avatar)
            }
            if (article.ref && typeof article.ref === 'object') {
                visit(article.ref as ArticleWithId | Article)
            }
        }

        for (const item of items) {
            visit(item.article)
        }
        return avatars
    }

    private buildSummaryCardAvatar(article: ArticleWithId | Article) {
        return {
            url: article.u_avatar || '',
            name: article.username || '',
            id: article.u_id || article.a_id || '',
        }
    }

    private shouldUseSummaryCardMediaKeys(
        keys: string[],
        config?: ResolvedSummaryCardConfig,
        mediaUsage?: SummaryCardMediaUsage,
    ) {
        const normalizedKeys = uniquePreserveOrder(keys.filter(Boolean))
        if (normalizedKeys.length === 0) {
            return true
        }
        if (!config?.mediaDuplicateLimit || !mediaUsage) {
            return true
        }
        if (normalizedKeys.some((key) => (mediaUsage.get(key) || 0) >= config.mediaDuplicateLimit!)) {
            return false
        }
        for (const key of normalizedKeys) {
            mediaUsage.set(key, (mediaUsage.get(key) || 0) + 1)
        }
        return true
    }

    private filterSummaryCardRenderedFiles(
        files: Array<RenderResult['originalMediaFiles'][number]>,
        config?: ResolvedSummaryCardConfig,
        mediaUsage?: SummaryCardMediaUsage,
        options: { renderableOnly?: boolean } = {},
    ) {
        const seen = new Set<string>()
        return files.filter((file) => {
            if (options.renderableOnly && file.media_type !== 'photo' && file.media_type !== 'video_thumbnail') {
                return false
            }
            const keys = this.buildRenderedMediaIdentityKeys(file)
            if (options.renderableOnly && keys.length > 0) {
                if (keys.some((key) => seen.has(key))) {
                    return false
                }
                for (const key of keys) {
                    seen.add(key)
                }
            }
            return this.shouldUseSummaryCardMediaKeys(keys, config, mediaUsage)
        })
    }

    private collectSummaryArticleMedia(
        articles: ArticleWithId[],
        maxItems: number,
        config?: ResolvedSummaryCardConfig,
        mediaUsage?: SummaryCardMediaUsage,
    ): NonNullable<Article['media']> {
        const result: NonNullable<Article['media']> = []
        const seen = new Set<string>()
        const visit = (article?: ArticleWithId | Article | null) => {
            if (!article || result.length >= maxItems) {
                return
            }
            for (const media of article.media || []) {
                if (result.length >= maxItems) {
                    break
                }
                if (media.type !== 'photo' && media.type !== 'video_thumbnail') {
                    continue
                }
                const keys = uniquePreserveOrder([media.url, JSON.stringify(media)].filter(Boolean))
                if (keys.some((key) => seen.has(key))) {
                    continue
                }
                if (!this.shouldUseSummaryCardMediaKeys(keys, config, mediaUsage)) {
                    continue
                }
                for (const key of keys) {
                    seen.add(key)
                }
                result.push(cloneDeep(media))
            }
            if (article.ref && typeof article.ref === 'object') {
                visit(article.ref as ArticleWithId | Article)
            }
        }

        for (const article of articles) {
            visit(article)
        }
        return result
    }

    private buildSummaryCardItemMedia(
        item: SummaryCardQueueItem,
        maxItems: number = 4,
        config?: ResolvedSummaryCardConfig,
        mediaUsage?: SummaryCardMediaUsage,
    ): NonNullable<Article['media']> {
        const fromRenderedFiles = this.renderService.buildCardMediaFromRenderedFiles(
            this.filterSummaryCardRenderedFiles(item.cardSourceMediaFiles, config, mediaUsage, {
                renderableOnly: true,
            }),
            maxItems,
        )
        if (fromRenderedFiles.length > 0) {
            return fromRenderedFiles
        }
        return this.collectSummaryArticleMedia([item.article], maxItems, config, mediaUsage)
    }

    private collectSummaryCardEmbeddedMedia(
        items: SummaryCardQueueItem[],
        maxItems: number,
        config: ResolvedSummaryCardConfig,
    ): NonNullable<Article['media']> {
        if (config.mediaDuplicateLimit) {
            return []
        }
        const result: NonNullable<Article['media']> = []
        const seen = new Set<string>()
        for (const item of items) {
            if (result.length >= maxItems) {
                break
            }
            const mediaItems = this.buildSummaryCardItemMedia(item, maxItems - result.length, config)
            for (const media of mediaItems) {
                if (result.length >= maxItems) {
                    break
                }
                const keys = uniquePreserveOrder([media.url, JSON.stringify(media)].filter(Boolean))
                if (keys.some((key) => seen.has(key))) {
                    continue
                }
                for (const key of keys) {
                    seen.add(key)
                }
                result.push(cloneDeep(media))
            }
        }
        return result
    }

    private countSummaryCardItemMedia(item: SummaryCardQueueItem) {
        const renderedCount = item.cardSourceMediaFiles.filter(
            (file) => file.media_type === 'photo' || file.media_type === 'video_thumbnail',
        ).length
        if (renderedCount > 0) {
            return renderedCount
        }
        return this.collectSummaryArticleMedia([item.article], DEFAULT_SUMMARY_CARD_MAX_EMBEDDED_MEDIA).length
    }

    private buildArticleTextVariant(article: ArticleWithId, textMode: SummaryCardTextMode) {
        if (textMode === 'default') {
            return article
        }

        const cloned = cloneDeep(article)
        const visit = (currentArticle?: ArticleWithId | Article | null) => {
            if (!currentArticle) {
                return
            }
            let usedTranslatedText = false

            if (Array.isArray(currentArticle.media)) {
                currentArticle.media = currentArticle.media.map((mediaItem) => {
                    const clonedMedia = { ...(mediaItem as any) }
                    if (
                        textMode === 'translated' &&
                        this.isUsefulSummaryCardTranslation(clonedMedia.alt, clonedMedia.translation)
                    ) {
                        clonedMedia.alt = clonedMedia.translation
                    }
                    delete clonedMedia.translation
                    delete clonedMedia.translated_by
                    return clonedMedia
                }) as any
            }

            if (currentArticle.extra) {
                const nextExtra = { ...(currentArticle.extra as any) }
                if (
                    textMode === 'translated' &&
                    this.isUsefulSummaryCardTranslation(nextExtra.content, nextExtra.translation)
                ) {
                    nextExtra.content = nextExtra.translation
                    usedTranslatedText = true
                }
                delete nextExtra.translation
                currentArticle.extra = nextExtra as any
            }

            if (textMode === 'translated') {
                const translatedContent = String(currentArticle.translation || '').trim()
                if (this.isUsefulSummaryCardTranslation(currentArticle.content, translatedContent)) {
                    currentArticle.content = translatedContent
                    usedTranslatedText = true
                }
                const websiteMeta = currentArticle.extra as any
                if (
                    usedTranslatedText &&
                    currentArticle.platform === Platform.Website &&
                    websiteMeta?.extra_type === 'website_meta' &&
                    websiteMeta.data &&
                    typeof websiteMeta.data === 'object' &&
                    typeof websiteMeta.data.raw_html === 'string'
                ) {
                    const dataWithoutRawHtml = { ...websiteMeta.data }
                    delete dataWithoutRawHtml.raw_html
                    currentArticle.extra = {
                        ...websiteMeta,
                        data: dataWithoutRawHtml,
                    } as any
                }
            }

            currentArticle.translation = null
            currentArticle.translated_by = null

            if (currentArticle.ref && typeof currentArticle.ref === 'object') {
                visit(currentArticle.ref as ArticleWithId | Article)
            }
        }

        visit(cloned)
        return cloned
    }

    private attachTranslatedCardBadgeLabel<T extends ArticleWithId | Article>(article: T, badgeLabel: string): T {
        const extra = article.extra && typeof article.extra === 'object' ? (article.extra as any) : {}
        article.extra = {
            ...extra,
            data: {
                ...(extra.data && typeof extra.data === 'object' ? extra.data : {}),
                translated_badge_label: badgeLabel,
            },
        } as any
        return article
    }

    private hasVisibleTranslatedCardText(original: ArticleWithId | Article, translated: ArticleWithId | Article) {
        const visit = (
            originalArticle?: ArticleWithId | Article | null,
            translatedArticle?: ArticleWithId | Article | null,
        ): boolean => {
            if (!originalArticle || !translatedArticle) {
                return false
            }

            const originalContent = String(originalArticle.content || '').trim()
            const translatedContent = String(translatedArticle.content || '').trim()
            if (originalContent !== translatedContent && translatedContent.length > 0) {
                return true
            }

            const originalExtraContent = String((originalArticle.extra as any)?.content || '').trim()
            const translatedExtraContent = String((translatedArticle.extra as any)?.content || '').trim()
            if (originalExtraContent !== translatedExtraContent && translatedExtraContent.length > 0) {
                return true
            }

            if (originalArticle.ref && translatedArticle.ref) {
                return visit(
                    originalArticle.ref as ArticleWithId | Article,
                    translatedArticle.ref as ArticleWithId | Article,
                )
            }
            return false
        }

        return visit(original, translated)
    }

    private buildTranslatedCardArticle(article: ArticleWithId, badgeLabel: string) {
        const translatedArticle = this.buildArticleTextVariant(article, 'translated')
        if (!this.hasVisibleTranslatedCardText(article, translatedArticle)) {
            return null
        }
        return this.attachTranslatedCardBadgeLabel(translatedArticle, badgeLabel)
    }

    private async buildTranslatedNativeCompanionCard(
        article: ArticleWithId,
        renderResult: Pick<RenderResult, 'cardMediaFiles' | 'originalMediaFiles'>,
        cfg_forwarder: Forwarder['cfg_forwarder'],
        target: BaseForwarder,
        runtime_config: ForwardTargetPlatformCommonConfig | undefined,
        taskId: string,
        visibleCardMediaFiles: Array<RenderedMediaFile>,
    ) {
        if (renderResult.cardMediaFiles.length === 0 || visibleCardMediaFiles.length === 0) {
            return null
        }

        const summaryConfig = resolveSummaryCardConfig(target.getEffectiveConfig(runtime_config))
        const translatedCard = summaryConfig?.translatedCard
        if (!summaryConfig?.sendFirstNative || !translatedCard) {
            return null
        }
        if (!translatedCard.processorId) {
            this.log?.warn(
                `Native translated companion enabled for ${target.id} without processor_id; using existing translations only`,
            )
            if (!this.hasArticleChainTranslatedContent([article])) {
                return null
            }
        } else if (
            !(await this.prepareArticleChainTranslations(
                translatedCard.processorId,
                [article],
                `native translated companion ${target.id}`,
            ))
        ) {
            return null
        }

        const translatedArticle = this.buildTranslatedCardArticle(article, translatedCard.badgeLabel)
        if (!translatedArticle) {
            this.log?.debug(`Skip native translated companion for ${target.id}: no visible translated card text`)
            return null
        }
        return this.renderService.process(translatedArticle, {
            taskId: `${taskId}-${target.id}-${article.a_id}-translated-card`,
            render_type: cfg_forwarder?.render_type,
            render_features: cfg_forwarder?.render_features,
            card_features: mergeFeatureFlags(cfg_forwarder?.card_features, ['translated-corner-badge']),
            preloadedMediaFiles: renderResult.originalMediaFiles,
            deduplication: false,
        })
    }

    private async buildSummaryCardItemText(
        queue: SummaryCardQueue,
        article: ArticleWithId,
        nonStormTags: Array<string> = [],
        textMode: SummaryCardTextMode = 'default',
    ) {
        const textArticle = this.buildArticleTextVariant(article, textMode)
        const message =
            this.renderService.renderText(textArticle, { render_type: 'text-compact' }).trim() ||
            formatArticleHeaderLine(textArticle as any).trim() ||
            extractArticleHeadline(textArticle as any, 100).trim() ||
            article.url ||
            article.a_id ||
            '无正文'
        const tagLine = nonStormTags.length > 0 ? `其他标签: ${uniquePreserveOrder(nonStormTags).join(' ')}` : ''
        return [message, tagLine].filter(Boolean).join('\n')
    }

    private buildSummaryCardItemMediaLine(item: SummaryCardQueueItem) {
        const count = this.countSummaryCardItemMedia(item)
        return count > 0 ? `图集: ${count} 张` : ''
    }

    private async buildSummaryCardContent(
        queue: SummaryCardQueue,
        kind: 'storm' | 'thread',
        items: SummaryCardQueueItem[],
        textMode: SummaryCardTextMode = 'default',
    ) {
        const shown = items.slice(0, queue.config.maxItems)
        const omitted = items.length - shown.length

        if (kind === 'storm') {
            const tags = uniquePreserveOrder(items.flatMap((item) => item.digestTags))
            const lines = await Promise.all(
                shown.map(async (item, index) => {
                    const nonStormTags = extractArticleHashtags(item.article).filter(
                        (tag) => !tags.some((stormTag) => normalizeHashtagKey(stormTag) === normalizeHashtagKey(tag)),
                    )
                    return [
                        `【${index + 1}】`,
                        await this.buildSummaryCardItemText(queue, item.article, nonStormTags, textMode),
                        this.buildSummaryCardItemMediaLine(item),
                    ]
                        .filter(Boolean)
                        .join('\n')
                }),
            )
            if (omitted > 0) {
                lines.push(`另有 ${omitted} 条更新已合并`)
            }
            return [
                `【话题聚合】${tags.join(' ')} / ${items.length} 条`,
                `范围: ${this.formatSummaryCardTimeRangeForQueue(
                    queue,
                    items.map((item) => item.article),
                )}`,
                ...lines,
            ].join('\n\n')
        }

        const root = this.getArticleThreadRoot(items[0]?.article)
        const rootLine = root
            ? `串: ${root.username || root.u_id || 'unknown'} / ${extractArticleHeadline(root as any, 100)}`
            : undefined
        const lines = await Promise.all(
            shown.map(async (item, index) => {
                return [
                    `【${index + 1}】`,
                    await this.buildSummaryCardItemText(queue, item.article, [], textMode),
                    this.buildSummaryCardItemMediaLine(item),
                ]
                    .filter(Boolean)
                    .join('\n')
            }),
        )
        if (omitted > 0) {
            lines.push(`另有 ${omitted} 条更新已合并`)
        }
        return [
            `【聚合】${this.formatSummaryCardRangeForQueue(
                queue,
                items.map((item) => item.article),
                { spaced: true },
            )}`,
            rootLine,
            ...lines,
        ]
            .filter(Boolean)
            .join('\n\n')
    }

    private formatSummaryCardTimeRange(articles: ArticleWithId[]) {
        const sorted = orderBy(articles, ['created_at', 'id'], ['asc', 'asc'])
        const start = dayjs.unix(sorted[0]?.created_at || Math.floor(Date.now() / 1000)).format('HH:mm')
        const end = dayjs.unix(sorted[sorted.length - 1]?.created_at || Math.floor(Date.now() / 1000)).format('HH:mm')
        return start === end ? start : `${start}-${end}`
    }

    private formatSummaryCardWindowTimeRange(queue: SummaryCardQueue) {
        if (queue.config.windowAlignment === 'none' || !queue.windowStart || !queue.windowEnd) {
            return null
        }
        const start = dayjs.unix(queue.windowStart).format('HHmm')
        const end = dayjs.unix(queue.windowEnd).format('HHmm')
        return start === end ? start : `${start}～${end}`
    }

    private formatSummaryCardTimeRangeForQueue(queue: SummaryCardQueue, articles: ArticleWithId[]) {
        return this.formatSummaryCardWindowTimeRange(queue) || this.formatSummaryCardTimeRange(articles)
    }

    private formatSummaryCardRange(articles: ArticleWithId[], options: { spaced?: boolean } = {}) {
        const count = articles.length
        const countLabel = options.spaced ? `${count} 条` : `${count}条`
        return `${countLabel} / ${this.formatSummaryCardTimeRange(articles)}`
    }

    private formatSummaryCardRangeForQueue(
        queue: SummaryCardQueue,
        articles: ArticleWithId[],
        options: { spaced?: boolean } = {},
    ) {
        const count = articles.length
        const countLabel = options.spaced ? `${count} 条` : `${count}条`
        return `${countLabel} / ${this.formatSummaryCardTimeRangeForQueue(queue, articles)}`
    }

    private getArticleThreadRoot(article?: ArticleWithId | Article | null): ArticleWithId | Article | null {
        let current: ArticleWithId | Article | null | undefined = article
        let root: ArticleWithId | Article | null = current || null
        while (current?.ref && typeof current.ref === 'object') {
            root = current.ref as ArticleWithId | Article
            current = current.ref as ArticleWithId | Article
        }
        return root
    }

    private getArticleThreadKey(article: ArticleWithId) {
        const root = this.getArticleThreadRoot(article) || article
        return `${root.platform}:${root.a_id || root.id}`
    }

    private resolveActiveTagDigestsForArticle(
        targetId: string,
        article: ArticleWithId,
        config: ForwardTargetPlatformCommonConfig,
    ) {
        if (!this.isTagDigestEnabled(config)) {
            return []
        }

        const now = Math.floor(Date.now() / 1000)
        const detectionWindow = this.resolvePositiveSeconds(
            config.tag_digest_detection_window_seconds,
            DEFAULT_TAG_DIGEST_DETECTION_WINDOW_SECONDS,
        )
        const digestWindow = this.resolvePositiveSeconds(
            config.tag_digest_window_seconds,
            DEFAULT_TAG_DIGEST_WINDOW_SECONDS,
        )
        const threshold = Math.max(2, Math.floor(Number(config.tag_digest_threshold || DEFAULT_TAG_DIGEST_THRESHOLD)))
        const minAuthors = Math.max(
            1,
            Math.floor(Number(config.tag_digest_min_authors || DEFAULT_TAG_DIGEST_MIN_AUTHORS)),
        )
        const tags = extractArticleHashtags(article)

        for (const tag of tags) {
            const stateKey = this.getTagDigestStateKey(targetId, tag)
            const state = this.tagDigestStates.get(stateKey) || { events: [], digestUntil: 0, displayTag: tag }
            state.events = state.events.filter((event) => event.timestamp >= now - detectionWindow)
            state.events.push({
                timestamp: now,
                authorKey: getArticleAuthorKey(article),
            })
            const distinctAuthorCount = new Set(state.events.map((event) => event.authorKey)).size
            if (state.events.length >= threshold && distinctAuthorCount >= Math.min(minAuthors, threshold)) {
                state.digestUntil = Math.max(state.digestUntil, now + digestWindow)
            }
            this.tagDigestStates.set(stateKey, state)
        }

        return tags.filter((tag) => {
            const state = this.tagDigestStates.get(this.getTagDigestStateKey(targetId, tag))
            return Boolean(state && state.digestUntil >= now)
        })
    }

    private shouldCollapseForwardedRefText(
        article: ArticleWithId,
        cfg_forwarder: Forwarder['cfg_forwarder'],
        target: BaseForwarder,
        runtime_config?: ForwardTargetPlatformCommonConfig,
    ) {
        if (!article.ref || typeof article.ref !== 'object') {
            return false
        }

        const config = target.getEffectiveConfig(runtime_config)
        if (config.collapse_forwarded_ref_text === false) {
            return false
        }
        if (config.collapse_forwarded_ref_text === true) {
            return true
        }
        if (HIGH_REALTIME_GROUP_IDS.has(String((config as any).group_id || ''))) {
            return false
        }
        if (cfg_forwarder?.render_features?.includes('no-collapse-forwarded-ref-text')) {
            return false
        }
        return true
    }

    private async collectForwardedReferenceIds(article: ArticleWithId, targetId: string, windowSeconds: number) {
        const ids = new Set<string | number>()
        const now = Math.floor(Date.now() / 1000)
        let currentArticle = article.ref as ArticleWithId | null
        while (currentArticle && typeof currentArticle === 'object') {
            if (!currentArticle.created_at || now - currentArticle.created_at > windowSeconds) {
                currentArticle = currentArticle.ref as ArticleWithId | null
                continue
            }
            const primary = await DB.ForwardBy.checkExist(currentArticle.id, article.platform, targetId, 'article')
            const secondary =
                currentArticle.platform !== article.platform
                    ? await DB.ForwardBy.checkExist(currentArticle.id, currentArticle.platform, targetId, 'article')
                    : null
            if (primary || secondary) {
                ids.add(currentArticle.id)
            }
            currentArticle = currentArticle.ref as ArticleWithId | null
        }
        return ids
    }

    private async applyDispatchDigests(
        log: Logger | undefined,
        articlesForwarders: Array<ArticleForwarderDispatch>,
        options?: { forceSend?: boolean },
        context?: { routeKey?: string },
    ) {
        if (options?.forceSend) {
            return articlesForwarders
        }

        const byTarget = new Map<
            string,
            {
                target: BaseForwarder
                runtime_config?: ForwardTargetPlatformCommonConfig
                routeKey: string
                articles: Array<ArticleWithId>
            }
        >()

        for (const { article, to } of articlesForwarders) {
            for (const { forwarder: target, runtime_config } of to) {
                if (resolveSummaryCardConfig(target.getEffectiveConfig(runtime_config))) {
                    continue
                }
                const blocked = await target.check_blocked('', {
                    timestamp: article.created_at,
                    runtime_config,
                    article: cloneDeep(article),
                })
                if (blocked) {
                    continue
                }
                const existing = byTarget.get(target.id) || {
                    target,
                    runtime_config,
                    routeKey: targetRouteKey(
                        context?.routeKey || routeKey({ source: 'system', crawlerId: 'unknown' }),
                        target.id,
                    ),
                    articles: [],
                }
                existing.articles.push(article)
                byTarget.set(target.id, existing)
            }
        }

        const digestedArticleIdsByTarget = new Map<string, Set<number>>()
        for (const [targetId, group] of byTarget) {
            const config = group.target.getEffectiveConfig(group.runtime_config)
            const targetDigestedIds = digestedArticleIdsByTarget.get(targetId) || new Set<number>()

            for (const tagGroup of this.resolveTagDigestGroups(targetId, group.articles, config)) {
                const sentIds = await this.claimAndSendDigest(log, targetId, group, tagGroup.articles, config, {
                    tag: tagGroup.tag,
                })
                for (const id of sentIds) {
                    targetDigestedIds.add(id)
                }
            }

            const threshold = Math.floor(Number(config.digest_threshold || 0))
            const remainingArticles = group.articles.filter((article) => !targetDigestedIds.has(article.id))
            if (threshold < 2 || remainingArticles.length < threshold) {
                if (targetDigestedIds.size > 0) {
                    digestedArticleIdsByTarget.set(targetId, targetDigestedIds)
                }
                continue
            }

            const sentIds = await this.claimAndSendDigest(log, targetId, group, remainingArticles, config)
            for (const id of sentIds) {
                targetDigestedIds.add(id)
            }
            if (targetDigestedIds.size > 0) {
                digestedArticleIdsByTarget.set(targetId, targetDigestedIds)
            }
        }

        return articlesForwarders
            .map(({ article, to }) => ({
                article,
                to: to.filter(({ forwarder: target }) => !digestedArticleIdsByTarget.get(target.id)?.has(article.id)),
            }))
            .filter(({ to }) => to.length > 0)
    }

    private async claimAndSendDigest(
        log: Logger | undefined,
        targetId: string,
        group: {
            target: BaseForwarder
            runtime_config?: ForwardTargetPlatformCommonConfig
            routeKey: string
            articles: Array<ArticleWithId>
        },
        articles: Array<ArticleWithId>,
        config: ForwardTargetPlatformCommonConfig,
        options?: { tag?: string },
    ) {
        const claimedArticles: Array<ArticleWithId> = []
        for (const article of articles) {
            const claimed = await this.claimArticleChain(article, article.platform, targetId)
            if (claimed) {
                claimedArticles.push(article)
            }
        }

        const requiredCount = options?.tag ? 1 : Math.floor(Number(config.digest_threshold || 0))
        if (claimedArticles.length < Math.max(1, requiredCount)) {
            for (const article of claimedArticles) {
                await this.releaseArticleChain(article, article.platform, targetId)
            }
            return []
        }

        const digestText = this.buildDispatchDigestText(targetId, claimedArticles, config, options)
        const articleKeys = claimedArticles.map((article) => articleKey(article)).sort((a, b) => a.localeCompare(b))
        const taskKind = options?.tag ? 'tag_digest' : 'digest'
        const syntheticKey = `${group.routeKey}:${options?.tag || 'all'}:${articleKeys.join('|')}`
        const outboundIdempotencyKey = syntheticOutboundKey(targetId, taskKind, syntheticKey)
        const outboundPayloadHash = payloadHash({
            routeKey: group.routeKey,
            targetId,
            taskKind,
            text: digestText,
            articleKeys,
            extra: {
                tag: options?.tag || null,
            },
        })
        try {
            const outbound = await DB.OutboundMessage.claim({
                idempotency_key: outboundIdempotencyKey,
                route_key: group.routeKey,
                target_id: targetId,
                target_platform: group.target.NAME,
                task_kind: taskKind,
                synthetic_key: syntheticKey,
                payload_hash: outboundPayloadHash,
            })
            if (!outbound.claimed) {
                log?.debug(`Digest outbound ${outboundIdempotencyKey} already ${outbound.record.status}; skipping send`)
                if (isOutboundSuppressedCompletionStatus(outbound.record.status)) {
                    return claimedArticles.map((article) => article.id)
                }
                for (const article of claimedArticles) {
                    await this.releaseArticleChain(article, article.platform, targetId)
                }
                return []
            }

            await DB.OutboundMessage.markSending(outboundIdempotencyKey)
            const sendResult = await group.target.send(digestText, {
                timestamp: Math.floor(Date.now() / 1000),
                runtime_config: group.runtime_config,
            })
            if (sendResult.status === 'queued') {
                await DB.OutboundMessage.markQueued(outboundIdempotencyKey, sendResult)
                await DB.TargetHealth.mark({
                    target_id: targetId,
                    provider: group.target.NAME,
                    status: 'ok',
                    last_send_status: 'queued',
                    details: sendResult,
                }).catch(() => undefined)
                for (const article of claimedArticles) {
                    await this.releaseArticleChain(article, article.platform, targetId)
                }
                return []
            }
            if (sendResult.status === 'blocked') {
                await DB.OutboundMessage.markSkipped(outboundIdempotencyKey, sendResult.reason, sendResult)
                await DB.TargetHealth.mark({
                    target_id: targetId,
                    provider: group.target.NAME,
                    status: 'ok',
                    last_send_status: 'blocked',
                    details: sendResult,
                }).catch(() => undefined)
                return claimedArticles.map((article) => article.id)
            }
            if (sendResult.status === 'dry_run') {
                await DB.OutboundMessage.markDryRun(outboundIdempotencyKey, sendResult)
                await DB.TargetHealth.mark({
                    target_id: targetId,
                    provider: group.target.NAME,
                    status: 'ok',
                    last_send_status: 'dry_run',
                    details: sendResult,
                }).catch(() => undefined)
                for (const article of claimedArticles) {
                    await this.releaseArticleChain(article, article.platform, targetId)
                }
                return []
            }
            const providerResult = getForwarderProviderResult(sendResult)
            await DB.OutboundMessage.markSent(outboundIdempotencyKey, summarizeProviderResult(providerResult))
            await DB.TargetHealth.mark({
                target_id: targetId,
                provider: group.target.NAME,
                status: 'ok',
                last_send_status: 'sent',
                last_provider_code: providerCode(providerResult),
                details: summarizeProviderResult(providerResult),
            })
            log?.info(
                `Sent ${options?.tag ? `${options.tag} tag ` : ''}digest for ${claimedArticles.length} articles to ${targetId}`,
            )
            return claimedArticles.map((article) => article.id)
        } catch (error) {
            log?.error(`Failed to send digest to ${targetId}: ${error}`)
            if (error instanceof PartialForwarderSendError) {
                await DB.OutboundMessage.markPartial(
                    outboundIdempotencyKey,
                    summarizeProviderResult(error.partialResults),
                    error,
                ).catch(() => undefined)
                await DB.TargetHealth.mark({
                    target_id: targetId,
                    provider: group.target.NAME,
                    status: 'degraded',
                    last_send_status: 'partial',
                    last_provider_code: providerCode(error.partialResults),
                    disabled_reason: error.message,
                    details: summarizeProviderResult(error.partialResults),
                }).catch(() => undefined)
                return claimedArticles.map((article) => article.id)
            }
            await DB.OutboundMessage.markFailed(outboundIdempotencyKey, error).catch(() => undefined)
            await DB.TargetHealth.mark({
                target_id: targetId,
                provider: group.target.NAME,
                status: 'error',
                last_send_status: 'failed',
                disabled_reason: error instanceof Error ? error.message : String(error),
                details: {
                    route_key: group.routeKey,
                    task_kind: taskKind,
                    article_keys: articleKeys,
                },
            }).catch(() => undefined)
            for (const article of claimedArticles) {
                await this.releaseArticleChain(article, article.platform, targetId)
            }
            return []
        }
    }

    private resolveTagDigestGroups(
        targetId: string,
        articles: Array<ArticleWithId>,
        config: ForwardTargetPlatformCommonConfig,
    ): Array<TagDigestGroup> {
        if (!this.isTagDigestEnabled(config)) {
            return []
        }

        const now = Math.floor(Date.now() / 1000)
        const detectionWindow = this.resolvePositiveSeconds(
            config.tag_digest_detection_window_seconds,
            DEFAULT_TAG_DIGEST_DETECTION_WINDOW_SECONDS,
        )
        const digestWindow = this.resolvePositiveSeconds(
            config.tag_digest_window_seconds,
            DEFAULT_TAG_DIGEST_WINDOW_SECONDS,
        )
        const threshold = Math.max(2, Math.floor(Number(config.tag_digest_threshold || DEFAULT_TAG_DIGEST_THRESHOLD)))
        const minAuthors = Math.max(
            1,
            Math.floor(Number(config.tag_digest_min_authors || DEFAULT_TAG_DIGEST_MIN_AUTHORS)),
        )
        const tagsByArticle = new Map<number, Array<string>>()

        for (const article of articles) {
            const tags = extractArticleHashtags(article)
            tagsByArticle.set(article.id, tags)
            for (const tag of tags) {
                const stateKey = this.getTagDigestStateKey(targetId, tag)
                const state = this.tagDigestStates.get(stateKey) || { events: [], digestUntil: 0, displayTag: tag }
                state.events = state.events.filter((event) => event.timestamp >= now - detectionWindow)
                state.events.push({
                    timestamp: now,
                    authorKey: getArticleAuthorKey(article),
                })
                const distinctAuthorCount = new Set(state.events.map((event) => event.authorKey)).size
                if (state.events.length >= threshold && distinctAuthorCount >= Math.min(minAuthors, threshold)) {
                    state.digestUntil = Math.max(state.digestUntil, now + digestWindow)
                }
                this.tagDigestStates.set(stateKey, state)
            }
        }

        const groups = new Map<string, Array<ArticleWithId>>()
        for (const article of articles) {
            const activeTag = (tagsByArticle.get(article.id) || []).find((tag) => {
                const state = this.tagDigestStates.get(this.getTagDigestStateKey(targetId, tag))
                return Boolean(state && state.digestUntil >= now)
            })
            if (!activeTag) {
                continue
            }
            const state = this.tagDigestStates.get(this.getTagDigestStateKey(targetId, activeTag))
            const displayTag = state?.displayTag || activeTag
            const existing = groups.get(displayTag) || []
            existing.push(article)
            groups.set(displayTag, existing)
        }

        return Array.from(groups.entries()).map(([tag, tagArticles]) => ({
            tag,
            articles: tagArticles,
        }))
    }

    private isTagDigestEnabled(config: ForwardTargetPlatformCommonConfig) {
        const explicitThreshold = Math.floor(Number(config.tag_digest_threshold || 0))
        const targetDigestThreshold = Math.floor(Number(config.digest_threshold || 0))
        return explicitThreshold >= 2 || targetDigestThreshold >= 2
    }

    private resolvePositiveSeconds(value: unknown, fallback: number) {
        const seconds = Math.floor(Number(value || 0))
        return seconds > 0 ? seconds : fallback
    }

    private getTagDigestStateKey(targetId: string, tag: string) {
        return `${targetId}:${normalizeHashtagKey(tag)}`
    }

    private buildDispatchDigestText(
        targetId: string,
        articles: Array<ArticleWithId>,
        config: ForwardTargetPlatformCommonConfig,
        options?: { tag?: string },
    ) {
        const sorted = orderBy(articles, ['created_at', 'id'], ['asc', 'asc'])
        const maxItemsConfig = options?.tag
            ? config.tag_digest_max_items || config.digest_max_items
            : config.digest_max_items
        const maxItems = Math.max(3, Math.min(Math.floor(Number(maxItemsConfig || 8)), 20))
        const shown = sorted.slice(0, maxItems)
        const start = dayjs.unix(sorted[0]?.created_at || Math.floor(Date.now() / 1000)).format('HH:mm')
        const end = dayjs.unix(sorted[sorted.length - 1]?.created_at || Math.floor(Date.now() / 1000)).format('HH:mm')
        const lines = shown.map((article, index) => {
            const time = dayjs.unix(article.created_at).format('HH:mm')
            const replyMark = String(article.type || '').includes('reply') ? '↪ ' : ''
            const author = article.username || article.u_id || 'unknown'
            if (options?.tag) {
                const nonTagText = extractArticleNonTagText(article, 120)
                const tags = extractArticleHashtags(article).filter(
                    (tag) => normalizeHashtagKey(tag) !== normalizeHashtagKey(options.tag || ''),
                )
                const tagLine = tags.length > 0 ? `其他标签: ${tags.join(' ')}` : undefined
                return [
                    `${index + 1}. [${time}] ${replyMark}${author}`,
                    `正文: ${nonTagText}`,
                    tagLine,
                    article.url || '',
                ]
                    .filter(Boolean)
                    .join('\n')
                    .trim()
            }
            const headline = extractArticleHeadline(article as any, 96) || article.url || article.a_id
            return `${index + 1}. [${time}] ${replyMark}${author}: ${headline}\n${article.url || ''}`.trim()
        })
        const omitted = sorted.length - shown.length
        if (omitted > 0) {
            lines.push(`... 另有 ${omitted} 条更新已合并`)
        }

        const title = options?.tag
            ? `【话题更新合并】${options.tag} / ${sorted.length} 条 / ${start}-${end}`
            : `【更新合并】${sorted.length} 条 / ${start}-${end}`
        return [title, ...lines].join('\n\n')
    }

    private async claimArticleChain(article: ArticleWithId, platform: Platform, targetId: string) {
        const claimed = await DB.ForwardBy.claim(article.id, platform, targetId, 'article')
        if (!claimed) {
            return false
        }

        let currentArticle = article.ref as ArticleWithId | null
        while (currentArticle && typeof currentArticle === 'object') {
            await DB.ForwardBy.save(currentArticle.id, platform, targetId, 'article')
            currentArticle = currentArticle.ref as ArticleWithId | null
        }
        return true
    }

    private async markMediaBatchArticlesSent(target: BaseForwarder, sendResult: unknown, providerSummary: unknown) {
        if (!isForwarderSentResult(sendResult) || !sendResult.batchArticles || sendResult.batchArticles.length === 0) {
            return
        }

        const markedOutboundKeys = new Set<string>()
        for (const outboundKey of sendResult.batchOutbounds || []) {
            markedOutboundKeys.add(outboundKey)
            await DB.OutboundMessage.markSent(outboundKey, providerSummary).catch((error) => {
                this.log?.warn(
                    `Failed to mark media batch outbound ${outboundKey} sent for ${target.id}: ${
                        error instanceof Error ? error.message : String(error)
                    }`,
                )
            })
        }

        for (const batchArticle of sendResult.batchArticles) {
            const article = batchArticle as ArticleWithId
            if (!article || !Number.isFinite(Number(article.id)) || article.id < 0 || article.platform === undefined) {
                continue
            }
            const outboundKey = articleOutboundKey(target.id, article)
            if (!markedOutboundKeys.has(outboundKey)) {
                await DB.OutboundMessage.markSent(outboundKey, providerSummary).catch((error) => {
                    this.log?.warn(
                        `Failed to mark media batch outbound ${outboundKey} sent for ${target.id}: ${
                            error instanceof Error ? error.message : String(error)
                        }`,
                    )
                })
            }
            await DB.ForwardBy.save(article.id, article.platform, target.id, 'article').catch((error) => {
                this.log?.warn(
                    `Failed to mark media batch article ${article.a_id || article.id} forwarded for ${target.id}: ${
                        error instanceof Error ? error.message : String(error)
                    }`,
                )
            })
        }
    }

    private async releaseArticleChain(article: ArticleWithId, platform: Platform, targetId: string) {
        let currentArticle: ArticleWithId | null = article
        while (currentArticle && typeof currentArticle === 'object') {
            await DB.ForwardBy.deleteRecord(currentArticle.id, platform, targetId, 'article')
            currentArticle = currentArticle.ref as ArticleWithId | null
        }
    }

    /**
     * 一次性任务，并不需要保存转发状态
     */
    async processFollowsTask(
        ctx: TaskScheduler.TaskCtx,
        websites: Array<string>,
        forwarders: Array<ForwardTargetInstanceWithRuntimeConfig>,
    ) {
        if (websites.length === 0) {
            ctx.log?.error(`No websites found`)
            return
        }
        const { task_title, cfg_task } = ctx.task.data as RealForwarder<'follows'>
        const { comparison_window = '1d' } = cfg_task || {}
        const results = new Map<Platform, Array<[DBFollows, DBFollows | null]>>()
        // 我们假设websites的网页并不完全相同，所以我们需要分类
        for (const website of websites) {
            const url = new URL(website)
            const { platform, u_id } = spiderRegistry.extractBasicInfo(url.href) ?? {}
            if (!platform || !u_id) {
                ctx.log?.error(`Invalid url: ${url.href}`)
                continue
            }
            const follows = await DB.Follow.getLatestAndComparisonFollowsByName(u_id, platform, comparison_window)
            if (!follows) {
                ctx.log?.warn(`No follows found for ${url.href}`)
                continue
            }
            let result = results.get(platform)
            if (!result) {
                result = []
                results.set(platform, result)
            }
            result.push(follows)
        }

        if (results.size === 0) {
            ctx.log?.warn(`No follows need to be sent ${task_title}`)
            return
        }

        // 开始转发
        let texts_to_send = followsToText(orderBy(Array.from(results.entries()), (i) => i[0], 'asc'))
        if (task_title) {
            texts_to_send = `${task_title}\n${texts_to_send}`
        }
        for (const { forwarder: target, runtime_config } of forwarders) {
            try {
                await target.send(texts_to_send, {
                    timestamp: dayjs().unix(),
                    runtime_config,
                })
                /**
                 * 假设follows并不需要保存转发状态，因为任务基本上是一天一次
                 */
                // for (const [_, follows] of results.entries()) {
                //     for (const [cur, _] of follows) {
                //         await DB.ForwardBy.save(cur.id, target.id, 'follows')
                //     }
                // }
            } catch (e) {
                ctx.log?.error(`Error while sending to ${target.id}: ${e}`)
            }
        }
    }

    /**
     * 通过 batch id 来获取或初始化转发器
     * 如果没有找到，则新创建一个映射
     * 并注册新的订阅者
     */
    getOrInitForwarders(
        id: string,
        subscribers: Forwarder['subscribers'],
        cfg: Forwarder['cfg_forwarder'],
        cfg_forward_target?: Forwarder['cfg_forward_target'],
        forwarderId?: string,
        connections?: AppConfig['connections'],
    ): Array<ForwardTargetInstanceWithRuntimeConfig> {
        // Resolve targets from subscribers
        const subscribersList = [...(subscribers || [])]

        // Resolve targets from connections map if forwarderId is present
        if (forwarderId && connections && connections['forwarder-target']) {
            const targetIds = connections['forwarder-target'][forwarderId]
            if (targetIds && targetIds.length > 0) {
                targetIds.forEach((targetId) => {
                    // Avoid duplicates if already in subscribers
                    const exists = subscribersList.some((s) => (typeof s === 'string' ? s : s.id) === targetId)
                    if (!exists) {
                        subscribersList.push(targetId)
                    }
                })
            }
        }

        const common_cfg = cfg_forward_target
        let wrap = this.subscribers.get(id)
        if (!wrap) {
            const newWrap = {
                to:
                    subscribersList.length > 0
                        ? subscribersList.reduce((acc, s) => {
                              if (typeof s === 'string') {
                                  acc[s] = common_cfg
                              }
                              if (typeof s === 'object') {
                                  acc[s.id] = {
                                      ...common_cfg,
                                      ...s.cfg_forward_target,
                                  }
                              }
                              return acc
                          }, {} as ForwardTargetIdWithRuntimeConfig)
                        : this.forward_to.keys().reduce((acc, id) => {
                              acc[id] = undefined
                              return acc
                          }, {} as ForwardTargetIdWithRuntimeConfig),
                cfg_forwarder: cfg,
            }
            this.subscribers.set(id, newWrap)
            wrap = newWrap
        }
        const { to } = wrap
        /**
         * 注册新的订阅者
         */
        subscribersList.forEach((s) => {
            const id = typeof s === 'string' ? s : s.id
            if (!(id in to)) {
                to[id] =
                    typeof s === 'string'
                        ? common_cfg
                        : {
                              ...common_cfg,
                              ...s.cfg_forward_target,
                          }
            }
        })
        return Object.entries(to)
            .map(([id, cfg]) => {
                const forwarder = this.forward_to.get(id)
                if (!forwarder) {
                    return undefined
                }
                return {
                    forwarder,
                    runtime_config: cfg,
                }
            })
            .filter((i) => i !== undefined)
    }

    /**
     * Get a specific forward target by ID
     */
    getTarget(id: string): BaseForwarder | undefined {
        return this.forward_to.get(id)
    }
}

export { ForwarderTaskScheduler, ForwarderPools }
export {
    buildAutoBoundForwarderTaskData,
    resolveBatchAggregationConfig,
    resolveBatchTargetIds,
    resolveMatchingForwarderTemplate,
    resolveSummaryCardConfig,
}
