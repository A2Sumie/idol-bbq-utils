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
import { RenderService } from '@/services/render-service'
import { followsToText } from '@idol-bbq-utils/render'
import dayjs from 'dayjs'
import { cloneDeep, orderBy } from 'lodash'
import {
    getWebsitePhotoBatchKey,
    isWebsitePhotoAlbumArticle,
    normalizeWebsitePhotoArticles,
} from '@/utils/website-photo'

type CrawlerConfig = NonNullable<AppConfig['crawlers']>[number]
type ForwarderTemplate = NonNullable<AppConfig['forwarders']>[number]

function sortUnique(values: Array<string>) {
    return Array.from(new Set(values.filter(Boolean))).sort((a, b) => a.localeCompare(b))
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
                    this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, {
                        taskId,
                        task: task,
                    })
                    this.tasks.set(taskId, task)
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
        if (status === TaskScheduler.TaskStatus.COMPLETED || status === TaskScheduler.TaskStatus.FAILED) {
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
        // create targets
        const { cfg_forward_target } = this.props
        this.props.forward_targets?.forEach(async (t) => {
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
        })
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
            connections,
            article_ids_by_url,
        } = task.data as Forwarder & {
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
        for (const forwarder of this.forward_to.values()) {
            await forwarder.drop().catch((error) => {
                this.log?.warn(`Failed to drop forwarder ${forwarder.id}: ${error}`)
            })
        }
        this.forward_to.clear()
        this.subscribers.clear()
        this.errorCounter.clear()
        this.log?.info('Pools dropped')
    }

    async processArticleTask(ctx: TaskScheduler.TaskCtx) {
        const { websites, subscribers, cfg_forwarder, cfg_forward_target, id, connections, article_ids_by_url } = ctx
            .task.data as {
            websites: Array<string>
            subscribers: Forwarder['subscribers']
            cfg_forwarder: Forwarder['cfg_forwarder']
            cfg_forward_target: Forwarder['cfg_forward_target']
            id?: string
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
        const crawler = this.props.crawlers?.find((item) => item.name === crawlerName)
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
    ) {
        const allPaths: ForwardingPath[] = []
        if (!crawlerName || !connections || !connections['formatter-target']) {
            log?.warn(
                `[Trace] Missing connections or crawler name. Name: ${crawlerName}, Connections present: ${!!connections}`,
            )
            return allPaths
        }

        const directFormatterIds = connections['crawler-formatter']?.[crawlerName] || []
        const processorId = connections['crawler-processor']?.[crawlerName]
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

            const targetIds = connections['formatter-target']?.[formatterId] || []
            const validTargets: Array<ForwardTargetInstanceWithRuntimeConfig> = []
            for (const targetId of targetIds) {
                const forwarderInstance = this.forward_to.get(targetId)
                if (forwarderInstance) {
                    validTargets.push({
                        forwarder: forwarderInstance,
                        runtime_config: cfg_forward_target,
                    })
                } else {
                    log?.warn(`[Trace] Forwarder Instance NOT found for Target ID: ${targetId}`)
                }
            }

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
                },
                targets: validTargets,
                source: 'graph',
                formatterName: formatterConfig.name || formatterId,
            })
        }

        return allPaths
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
        const articles_forwarders = [] as Array<{
            article: ArticleWithId
            to: Array<ForwardTargetInstanceWithRuntimeConfig>
        }>
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
            let forceSendError: Error | null = null
            const cloned_article = cloneDeep(article)
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
                                    return
                                }
                            }
                        }

                        let claimed = true
                        if (!options?.forceSend) {
                            claimed = await this.claimArticleChain(article, platform, target.id)
                            if (!claimed) {
                                log?.debug(`[Trace] Article ${article.a_id} already claimed for target ${target.id}`)
                                return
                            }
                        }

                        await target.send(renderResult.text, {
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
                    } catch (error) {
                        log?.error(`Error while sending to ${target.id}: ${error}`)
                        if (!options?.forceSend) {
                            await this.releaseArticleChain(article, platform, target.id)
                        }
                    }
                }),
            )

            if (error_for_all) {
                if (options?.forceSend) {
                    forceSendError = new Error(`Failed to send article ${cloned_article.a_id} to all targets`)
                } else {
                    let errorCount = this.errorCounter.get(`${platform}:${cloned_article.a_id}`) || 0
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
                        this.errorCounter.delete(`${platform}:${cloned_article.a_id}`)
                    } else {
                        this.errorCounter.set(`${platform}:${cloned_article.a_id}`, errorCount)
                        log?.error(`Error count for ${cloned_article.a_id}: ${errorCount}`)
                    }
                }
            }

            this.renderService.cleanup(renderResult.mediaFiles)
            if (forceSendError) {
                throw forceSendError
            }
        }
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
        const subscribersList = subscribers || []

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
export { buildAutoBoundForwarderTaskData, resolveBatchTargetIds, resolveMatchingForwarderTemplate }
