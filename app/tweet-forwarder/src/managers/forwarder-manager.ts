import { Logger } from '@idol-bbq-utils/log'
import { spiderRegistry } from '@idol-bbq-utils/spider'
import { CronJob } from 'cron'
import EventEmitter from 'events'
import { BaseCompatibleModel, sanitizeWebsites, TaskScheduler } from '@/utils/base'
import type { AppConfig } from '@/types'
import { Platform, type MediaType, type TaskType } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import type { Article, ArticleWithId, DBFollows } from '@/db'
import { BaseForwarder } from '@/middleware/forwarder/base'
import { type Media, type MediaTool, MediaToolEnum } from '@/types/media'
import type { ForwardTargetPlatformCommonConfig, Forwarder as RealForwarder } from '@/types/forwarder'
import { getForwarder } from '@/middleware/forwarder'
import crypto from 'crypto'
import { RenderService, type RenderResult } from '@/services/render-service'
import { extractArticleHeadline, followsToText } from '@idol-bbq-utils/render'
import dayjs from 'dayjs'
import { cloneDeep, orderBy } from 'lodash'
import {
    getWebsitePhotoBatchKey,
    isWebsitePhotoAlbumArticle,
    normalizeWebsitePhotoArticles,
} from '@/utils/website-photo'

type CrawlerConfig = NonNullable<AppConfig['crawlers']>[number]
type ForwarderTemplate = NonNullable<AppConfig['forwarders']>[number]
type ArticleForwarderDispatch = {
    article: ArticleWithId
    to: Array<ForwardTargetInstanceWithRuntimeConfig>
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
type ResolvedSummaryCardConfig = {
    intervalSeconds: number
    threshold: number
    maxItems: number
    includeOriginalMedia: boolean
}
type SummaryCardQueueItem = {
    article: ArticleWithId
    queuedAt: number
    cardSourceMediaFiles: Array<RenderResult['originalMediaFiles'][number]>
    originalMediaFiles: Array<RenderResult['originalMediaFiles'][number]>
    digestTags: Array<string>
}
type SummaryCardQueue = {
    target: BaseForwarder
    runtime_config?: ForwardTargetPlatformCommonConfig
    config: ResolvedSummaryCardConfig
    items: Map<number, SummaryCardQueueItem>
    firstQueuedAt: number
    lastQueuedAt: number
}

const DEFAULT_TAG_DIGEST_THRESHOLD = 3
const DEFAULT_TAG_DIGEST_MIN_AUTHORS = 2
const DEFAULT_TAG_DIGEST_DETECTION_WINDOW_SECONDS = 5 * 60
const DEFAULT_TAG_DIGEST_WINDOW_SECONDS = 20 * 60
const DEFAULT_COLLAPSE_FORWARDED_REF_WINDOW_SECONDS = 18 * 3600
const DEFAULT_SUMMARY_CARD_INTERVAL_SECONDS = 30 * 60
const DEFAULT_SUMMARY_CARD_THRESHOLD = 8
const DEFAULT_SUMMARY_CARD_MAX_ITEMS = 14
const DEFAULT_SUMMARY_CARD_MAX_EMBEDDED_MEDIA = 12
const HIGH_REALTIME_GROUP_IDS = new Set(['742435777'])
const HASHTAG_REGEX = /[#＃][\p{L}\p{N}_ー一-龯ぁ-んァ-ヶ]+/gu

function sortUnique(values: Array<string>) {
    return Array.from(new Set(values.filter(Boolean))).sort((a, b) => a.localeCompare(b))
}

function uniquePreserveOrder(values: Array<string>) {
    return Array.from(new Set(values.filter(Boolean)))
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

function truncateDigestText(text: string, maxLength: number) {
    if (text.length <= maxLength) {
        return text
    }
    return `${text.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`
}

function resolveSummaryCardConfig(config: ForwardTargetPlatformCommonConfig): ResolvedSummaryCardConfig | null {
    const raw = config.summary_card
    const enabled = raw === true || (typeof raw === 'object' && raw?.enabled !== false)
    if (!enabled) {
        return null
    }

    const objectConfig = typeof raw === 'object' && raw ? raw : {}
    const intervalSeconds = Math.max(
        60,
        Math.floor(Number(objectConfig.interval_seconds || DEFAULT_SUMMARY_CARD_INTERVAL_SECONDS)),
    )
    const threshold = Math.max(2, Math.floor(Number(objectConfig.threshold || DEFAULT_SUMMARY_CARD_THRESHOLD)))
    const maxItems = Math.max(
        3,
        Math.min(Math.floor(Number(objectConfig.max_items || DEFAULT_SUMMARY_CARD_MAX_ITEMS)), 30),
    )

    return {
        intervalSeconds,
        threshold,
        maxItems,
        includeOriginalMedia: objectConfig.include_original_media === true,
    }
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
    private spiderFinishedListener: (payload: TaskResult) => void

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
                const { cron } = cfg_forwarder

                const job = new CronJob(cron as string, async () => {
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
                    const batchJob = new CronJob('0 * * * *', async () => {
                        this.log?.info(`Dispatching Hourly Batch for ${taskName}`)
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
                        const start = end - 3600

                        for (const website of websites) {
                            const info = spiderRegistry.extractBasicInfo(website)
                            if (!info?.u_id || !info.platform) {
                                continue
                            }
                            await DB.TaskQueue.add(
                                'aggregate_hourly',
                                {
                                    platform: info.platform,
                                    u_id: info.u_id,
                                    start,
                                    end,
                                    target_ids: targetIds,
                                },
                                end,
                            )
                        }
                    })
                    this.log?.info(
                        `Batch Job created for ${taskName} to send to ${aggregatingFormatters.length} aggregating formatters`,
                    )
                    this.cronJobs.push(batchJob)
                }
                // -----------------------------
            }
        } else {
            this.log?.warn('No crawlers defined for auto-binding.')
        }
    }

    private onSpiderTaskFinished({ taskId, result, crawlerName }: TaskResult) {
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

        const { forwarderTaskData } = buildAutoBoundForwarderTaskData(crawler, this.props)
        const forwardTaskId = `spider-${taskId}`
        const task: TaskScheduler.Task = {
            id: forwardTaskId,
            status: TaskScheduler.TaskStatus.PENDING,
            data: {
                ...forwarderTaskData,
                connections: this.props.connections,
                article_ids_by_url: articleIdsByUrl,
            },
        }

        this.log?.info(`Dispatching immediate forwarder task for crawler ${crawlerName}`)
        this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, {
            taskId: forwardTaskId,
            task,
        })
        this.tasks.set(forwardTaskId, task)
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

    updateTaskStatus({ taskId, status }: { taskId: string; status: TaskScheduler.TaskStatus }) {
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
    finishTask({ taskId, result, immediate_notify }: TaskResult) {
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
interface ForwardingPath {
    formatterConfig: Forwarder['cfg_forwarder']
    targets: Array<ForwardTargetInstanceWithRuntimeConfig>
    source: 'graph' | 'inline'
    formatterName: string
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
    >
    private renderService: RenderService

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
    private summaryCardLastObservedAt = new Map<string, number>()
    private summaryCardFlushTimer?: ReturnType<typeof setInterval>
    private dispatchListener: (ctx: TaskScheduler.TaskCtx) => Promise<void>

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
        >,
        emitter: EventEmitter,
        log?: Logger,
    ) {
        super()
        this.log = log?.child({ subservice: this.NAME })
        this.renderService = new RenderService(this.log)
        this.emitter = emitter
        this.props = props
        this.dispatchListener = this.onTaskReceived.bind(this)
    }

    async init() {
        this.log?.info('Forwarder Pools initializing...')
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

    async drop(...args: any[]): Promise<void> {
        this.log?.info('Dropping Pools...')
        this.emitter.off(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, this.dispatchListener)
        if (this.summaryCardFlushTimer) {
            clearInterval(this.summaryCardFlushTimer)
            this.summaryCardFlushTimer = undefined
        }
        await this.flushAllSummaryCardQueues()
        for (const forwarder of this.forward_to.values()) {
            await forwarder.drop().catch((error) => {
                this.log?.warn(`Failed to drop forwarder ${forwarder.id}: ${error}`)
            })
        }
        this.forward_to.clear()
        this.subscribers.clear()
        this.errorCounter.clear()
        this.summaryCardQueues.clear()
        this.log?.info('Pools dropped')
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

        // Define a unified path structure
        interface ForwardingPath {
            formatterConfig: Forwarder['cfg_forwarder']
            targets: Array<ForwardTargetInstanceWithRuntimeConfig>
            source: 'graph' | 'inline'
            formatterName: string
        }

        for (const website of websites) {
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
                ctx.log?.info(
                    `Processing via path [${path.source}]: ${path.formatterName} for ${path.targets.length} targets`,
                )
                /**
                 * 查询当前网站下的近10篇文章并查询转发
                 */
                await this.processSingleArticleTask(
                    ctx,
                    url.href,
                    path.targets,
                    path.formatterConfig,
                    article_ids_by_url?.[url.href],
                )
            }
        }
    }

    async resendArticle(
        article: ArticleWithId,
        crawlerName: string,
        cfg_forwarder?: Forwarder['cfg_forwarder'],
        cfg_forward_target?: Forwarder['cfg_forward_target'],
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
        const paths = this.resolveForwardingPaths(
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
        if (paths.length === 0) {
            throw new Error(`No forwarding paths found for crawler ${crawlerName}`)
        }

        const normalizedArticles = await this.normalizeForwardingArticles([article])
        for (const path of paths) {
            await this.sendArticles(
                taskLog,
                `manual-${article.a_id}`,
                normalizedArticles,
                path.targets,
                path.formatterConfig,
                { forceSend: true },
            )
        }
    }

    async processSingleArticleTask(
        ctx: TaskScheduler.TaskCtx,
        url: string,
        forwarders: Array<ForwardTargetInstanceWithRuntimeConfig>,
        cfg_forwarder: Forwarder['cfg_forwarder'],
        articleIds?: Array<number>,
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
        ctx.log?.info(`[Trace] Found ${articles.length} articles for ${url}`)
        await this.sendArticles(ctx.log, ctx.taskId, articles, forwarders, cfg_forwarder)
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
    ) {
        let articles_forwarders = [] as Array<ArticleForwarderDispatch>
        for (const article of articles) {
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
        articles_forwarders = await this.applyDispatchDigests(log, articles_forwarders, options)
        if (articles_forwarders.length === 0) {
            log?.debug(`[Trace] No articles remain after digest handling`)
            return
        }
        for (const { article, to } of articles_forwarders) {
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

            if (renderResult.shouldSkipSend) {
                log?.info(`Skipping article ${article.a_id}: ${renderResult.skipReason || 'deduplicated media'}`)
                for (const { forwarder: target } of to) {
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
            await Promise.all(
                to.map(async ({ forwarder: target, runtime_config }) => {
                    try {
                        if (!options?.forceSend) {
                            const TWO_HOURS_SECONDS = 3600 * 2
                            const now = dayjs().unix()
                            if (now - article.created_at > TWO_HOURS_SECONDS) {
                                const claimed = await this.claimArticleChain(article, platform, target.id)
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
                            )
                            if (queuedForSummary) {
                                hadNonErrorOutcome = true
                                return
                            }
                        }

                        const text = await this.resolveTargetTextForArticle(
                            article,
                            renderResult,
                            cfg_forwarder,
                            target,
                            runtime_config,
                        )

                        let claimed = true
                        if (!options?.forceSend) {
                            claimed = await this.claimArticleChain(article, platform, target.id)
                            if (!claimed) {
                                log?.debug(`[Trace] Article ${article.a_id} already claimed for target ${target.id}`)
                                hadNonErrorOutcome = true
                                return
                            }
                        }

                        await target.send(text, {
                            media: renderResult.mediaFiles,
                            cardMedia: renderResult.cardMediaFiles,
                            contentMedia: renderResult.originalMediaFiles,
                            timestamp: article.created_at,
                            runtime_config,
                            article: cloned_article,
                            forceSend: options?.forceSend,
                        })
                        if (options?.forceSend) {
                            await DB.ForwardBy.save(article.id, platform, target.id, 'article')
                        }
                        error_for_all = false
                        hadNonErrorOutcome = true
                    } catch (error) {
                        log?.error(`Error while sending to ${target.id}: ${error}`)
                        if (!options?.forceSend) {
                            await this.releaseArticleChain(article, platform, target.id)
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

            this.renderService.cleanup(renderResult.mediaFiles)
            if (forceSendError) {
                throw forceSendError
            }
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

    private async maybeQueueSummaryCardArticle(
        log: Logger | undefined,
        article: ArticleWithId,
        renderResult: RenderResult,
        target: BaseForwarder,
        runtime_config?: ForwardTargetPlatformCommonConfig,
    ) {
        const effectiveConfig = target.getEffectiveConfig(runtime_config)
        const summaryConfig = resolveSummaryCardConfig(effectiveConfig)
        if (!summaryConfig) {
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
        const queueKey = this.getSummaryCardQueueKey(target.id, runtime_config, summaryConfig)
        const lastObservedAt = this.summaryCardLastObservedAt.get(queueKey)
        const existingQueue = this.summaryCardQueues.get(queueKey)
        const item: SummaryCardQueueItem = {
            article: cloneDeep(article),
            queuedAt: now,
            cardSourceMediaFiles: [...renderResult.originalMediaFiles],
            originalMediaFiles: summaryConfig.includeOriginalMedia ? [...renderResult.originalMediaFiles] : [],
            digestTags: this.resolveActiveTagDigestsForArticle(target.id, article, effectiveConfig),
        }

        if (!existingQueue && (!lastObservedAt || now - lastObservedAt >= summaryConfig.intervalSeconds)) {
            const sent = await this.sendImmediateSummaryCardItem(target, runtime_config, summaryConfig, item)
            this.summaryCardLastObservedAt.set(queueKey, now)
            if (!sent) {
                await this.releaseArticleChain(item.article, item.article.platform, target.id)
            }
            log?.debug(`Sent idle-first summary-card item ${article.a_id} for ${target.id}.`)
            return true
        }

        const queue = existingQueue || {
            target,
            runtime_config,
            config: summaryConfig,
            items: new Map<number, SummaryCardQueueItem>(),
            firstQueuedAt: lastObservedAt || now,
            lastQueuedAt: now,
        }

        queue.target = target
        queue.runtime_config = runtime_config
        queue.config = summaryConfig
        queue.lastQueuedAt = now
        queue.items.set(article.id, item)
        this.summaryCardLastObservedAt.set(queueKey, now)
        this.summaryCardQueues.set(queueKey, queue)
        log?.debug(
            `Queued summary-card item ${article.a_id} for ${target.id}: ${queue.items.size}/${summaryConfig.threshold}`,
        )

        if (queue.items.size >= summaryConfig.threshold) {
            await this.flushSummaryCardQueue(queueKey, 'threshold')
        }
        return true
    }

    private getSummaryCardQueueKey(
        targetId: string,
        runtime_config: ForwardTargetPlatformCommonConfig | undefined,
        config: ResolvedSummaryCardConfig,
    ) {
        const hash = crypto
            .createHash('md5')
            .update(JSON.stringify({ runtime_config: runtime_config || {}, config }))
            .digest('hex')
        return `${targetId}:${hash}`
    }

    private async sendImmediateSummaryCardItem(
        target: BaseForwarder,
        runtime_config: ForwardTargetPlatformCommonConfig | undefined,
        config: ResolvedSummaryCardConfig,
        item: SummaryCardQueueItem,
    ) {
        const claimed = await this.claimArticleChain(item.article, item.article.platform, target.id)
        if (!claimed) {
            return true
        }

        return this.sendSummaryCardGroup(
            {
                target,
                runtime_config,
                config,
                items: new Map([[item.article.id, item]]),
                firstQueuedAt: item.queuedAt,
                lastQueuedAt: item.queuedAt,
            },
            { kind: 'thread', label: this.getArticleThreadKey(item.article), items: [item] },
            'idle-first',
        )
    }

    private async flushDueSummaryCardQueues() {
        const now = Math.floor(Date.now() / 1000)
        for (const [queueKey, queue] of Array.from(this.summaryCardQueues.entries())) {
            if (queue.items.size > 0 && now - queue.firstQueuedAt >= queue.config.intervalSeconds) {
                await this.flushSummaryCardQueue(queueKey, 'interval')
            }
        }
    }

    private async flushAllSummaryCardQueues() {
        for (const queueKey of Array.from(this.summaryCardQueues.keys())) {
            await this.flushSummaryCardQueue(queueKey, 'shutdown')
        }
    }

    private async flushSummaryCardQueue(
        queueKey: string,
        reason: 'threshold' | 'interval' | 'shutdown' | 'idle-first',
    ) {
        const queue = this.summaryCardQueues.get(queueKey)
        if (!queue || queue.items.size === 0) {
            this.summaryCardQueues.delete(queueKey)
            return
        }
        this.summaryCardQueues.delete(queueKey)
        this.summaryCardLastObservedAt.set(queueKey, Math.floor(Date.now() / 1000))

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
            return
        }

        const groups = this.buildSummaryCardGroups(claimedItems)
        for (const group of groups) {
            const ok = await this.sendSummaryCardGroup(queue, group, reason)
            if (!ok) {
                for (const item of group.items) {
                    await this.releaseArticleChain(item.article, item.article.platform, queue.target.id)
                }
            }
        }
    }

    private buildSummaryCardGroups(items: SummaryCardQueueItem[]) {
        const stormItems = items.filter((item) => item.digestTags.length > 0)
        const groups: Array<{ kind: 'storm' | 'thread'; label: string; items: SummaryCardQueueItem[] }> = []

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

    private async sendSummaryCardGroup(
        queue: SummaryCardQueue,
        group: { kind: 'storm' | 'thread'; label: string; items: SummaryCardQueueItem[] },
        reason: 'threshold' | 'interval' | 'shutdown' | 'idle-first',
    ) {
        const sorted = orderBy(group.items, ['article.created_at', 'article.id'], ['asc', 'asc'])
        const content = this.buildSummaryCardContent(group.kind, sorted, queue.config)
        const title =
            group.kind === 'storm'
                ? `话题消息合并 ${group.label}`.trim()
                : `消息合并 ${this.formatSummaryCardRange(sorted.map((item) => item.article))}`
        const now = Math.floor(Date.now() / 1000)
        const embeddedMedia = this.buildSummaryCardEmbeddedMedia(sorted)
        const summaryArticle = this.buildSyntheticSummaryArticle(title, content, sorted[0]?.article, now, embeddedMedia)
        const cardResult = await this.renderService.process(summaryArticle, {
            taskId: `summary-card-${queue.target.id}-${now}`,
            render_type: 'text-card',
            deduplication: false,
        })
        const originalMediaFiles = queue.config.includeOriginalMedia
            ? sorted.flatMap((item) => item.originalMediaFiles)
            : []
        const mediaFiles = [...cardResult.cardMediaFiles, ...originalMediaFiles]

        try {
            await queue.target.send(title, {
                media: mediaFiles,
                cardMedia: cardResult.cardMediaFiles,
                contentMedia: originalMediaFiles,
                timestamp: now,
                runtime_config: queue.runtime_config,
                article: summaryArticle,
                forceSend: true,
            })
            this.log?.info(
                `Sent ${group.kind} message pack card (${reason}) with ${sorted.length} articles to ${queue.target.id}`,
            )
            return true
        } catch (error) {
            this.log?.error(`Failed to send ${group.kind} message pack card to ${queue.target.id}: ${error}`)
            return false
        } finally {
            this.renderService.cleanup(cardResult.mediaFiles)
        }
    }

    private buildSyntheticSummaryArticle(
        title: string,
        content: string,
        sourceArticle: ArticleWithId | undefined,
        now: number,
        media: NonNullable<Article['media']>,
    ): ArticleWithId {
        return {
            id: -now,
            platform: sourceArticle?.platform || Platform.X,
            a_id: `summary-card-${now}`,
            u_id: 'message_pack',
            username: '消息合并',
            created_at: now,
            content: `${title}\n\n${content}`,
            url: sourceArticle?.url || '',
            type: 'message_pack' as any,
            ref: null,
            has_media: media.length > 0,
            media,
            extra: null,
            u_avatar: sourceArticle?.u_avatar || null,
        }
    }

    private buildSummaryCardEmbeddedMedia(items: SummaryCardQueueItem[]): NonNullable<Article['media']> {
        const fromRenderedFiles = this.renderService.buildCardMediaFromRenderedFiles(
            items.flatMap((item) => item.cardSourceMediaFiles),
            DEFAULT_SUMMARY_CARD_MAX_EMBEDDED_MEDIA,
        )
        if (fromRenderedFiles.length > 0) {
            return fromRenderedFiles
        }

        return this.collectSummaryArticleMedia(
            items.map((item) => item.article),
            DEFAULT_SUMMARY_CARD_MAX_EMBEDDED_MEDIA,
        )
    }

    private collectSummaryArticleMedia(articles: ArticleWithId[], maxItems: number): NonNullable<Article['media']> {
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
                const key = media.url || JSON.stringify(media)
                if (seen.has(key)) {
                    continue
                }
                seen.add(key)
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

    private buildSummaryCardContent(
        kind: 'storm' | 'thread',
        items: SummaryCardQueueItem[],
        config: ResolvedSummaryCardConfig,
    ) {
        const shown = items.slice(0, config.maxItems)
        const omitted = items.length - shown.length

        if (kind === 'storm') {
            const tags = uniquePreserveOrder(items.flatMap((item) => item.digestTags))
            const lines = shown.map((item, index) => {
                const message = this.renderService.renderText(item.article, { render_type: 'text-compact' }).trim()
                const nonStormTags = extractArticleHashtags(item.article).filter(
                    (tag) => !tags.some((stormTag) => normalizeHashtagKey(stormTag) === normalizeHashtagKey(tag)),
                )
                const tagLine =
                    nonStormTags.length > 0 ? `其他标签: ${uniquePreserveOrder(nonStormTags).join(' ')}` : ''
                return [`【${index + 1}】`, message, tagLine].filter(Boolean).join('\n')
            })
            if (omitted > 0) {
                lines.push(`另有 ${omitted} 条更新已合并`)
            }
            return [
                `【话题消息合并】${tags.join(' ')} / ${items.length} 条`,
                `范围: ${this.formatSummaryCardRange(items.map((item) => item.article))}`,
                ...lines,
            ].join('\n\n')
        }

        const root = this.getArticleThreadRoot(items[0]?.article)
        const rootLine = root
            ? `串: ${root.username || root.u_id || 'unknown'} / ${extractArticleHeadline(root as any, 100)}`
            : undefined
        const lines = shown.map((item, index) => {
            const message = this.renderService.renderText(item.article, { render_type: 'text-compact' }).trim()
            return [`【${index + 1}】`, message].filter(Boolean).join('\n')
        })
        if (omitted > 0) {
            lines.push(`另有 ${omitted} 条更新已合并`)
        }
        return [
            `【消息合并】${items.length} 条 / ${this.formatSummaryCardRange(items.map((item) => item.article))}`,
            rootLine,
            ...lines,
        ]
            .filter(Boolean)
            .join('\n\n')
    }

    private formatSummaryCardRange(articles: ArticleWithId[]) {
        const sorted = orderBy(articles, ['created_at', 'id'], ['asc', 'asc'])
        const start = dayjs.unix(sorted[0]?.created_at || Math.floor(Date.now() / 1000)).format('HH:mm')
        const end = dayjs.unix(sorted[sorted.length - 1]?.created_at || Math.floor(Date.now() / 1000)).format('HH:mm')
        return `${sorted.length}条 / ${start}-${end}`
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
    ) {
        if (options?.forceSend) {
            return articlesForwarders
        }

        const byTarget = new Map<
            string,
            {
                target: BaseForwarder
                runtime_config?: ForwardTargetPlatformCommonConfig
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
        try {
            await group.target.send(digestText, {
                timestamp: Math.floor(Date.now() / 1000),
                runtime_config: group.runtime_config,
            })
            log?.info(
                `Sent ${options?.tag ? `${options.tag} tag ` : ''}digest for ${claimedArticles.length} articles to ${targetId}`,
            )
            return claimedArticles.map((article) => article.id)
        } catch (error) {
            log?.error(`Failed to send digest to ${targetId}: ${error}`)
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
    resolveBatchTargetIds,
    resolveMatchingForwarderTemplate,
    resolveSummaryCardConfig,
}
