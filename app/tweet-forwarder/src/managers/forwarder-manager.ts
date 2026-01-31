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


type Forwarder = RealForwarder<TaskType>

interface TaskResult {
    taskId: string
    result: Array<CrawlerTaskResult>
    immediate_notify?: boolean
}

interface CrawlerTaskResult {
    task_type: TaskType
    url: string
    data: Array<number>
}

/**
 * 根据cronjob dispatch任务
 * 根据结果查询数据库
 */
class ForwarderTaskScheduler extends TaskScheduler.TaskScheduler {
    NAME: string = 'ForwarderTaskScheduler'
    protected log?: Logger
    private props: Pick<AppConfig, 'cfg_forwarder' | 'forwarders' | 'connections' | 'crawlers'>

    constructor(props: Pick<AppConfig, 'cfg_forwarder' | 'forwarders' | 'connections' | 'crawlers'>, emitter: EventEmitter, log?: Logger) {
        super(emitter)
        this.log = log?.child({ subservice: this.NAME })
        this.props = props
    }

    async init() {
        this.log?.info('initializing...')

        if (!this.props.forwarders) {
            this.log?.warn('Forwarder not found, skipping...')
            return
        }

        // 注册基本的监听器
        for (const [eventName, listener] of Object.entries(this.taskHandlers)) {
            this.emitter.on(`forwarder:${eventName}`, listener)
        }

        // 遍历爬虫配置，为每个爬虫创建定时任务
        // Auto-Bind Logic: Iterate Crawlers -> Find Matching Forwarder -> Spawn Task
        if (this.props.crawlers && this.props.crawlers.length > 0) {
            for (const crawler of this.props.crawlers) {
                // Find matching forwarder by origin
                const matchForwarder = this.props.forwarders.find(f => {
                    // Simple origin match. Could be improved with improved url matching if needed.
                    return f.origin && crawler.origin && f.origin === crawler.origin
                })

                if (!matchForwarder) {
                    this.log?.debug(`No matching forwarder template found for crawler ${crawler.name} (${crawler.origin}), skipping auto-bind...`)
                    continue
                }

                // Use Crawler's Name so connections work!
                const taskName = crawler.name
                // Use Forwarder's Config as Template
                const cfg_forwarder = {
                    cron: '*/30 * * * *',
                    media: {
                        type: 'no-storage',
                        use: {
                            tool: MediaToolEnum.DEFAULT,
                        },
                    },
                    ...this.props.cfg_forwarder,
                    ...matchForwarder.cfg_forwarder,
                }
                const { cron } = cfg_forwarder

                // Create the task using Crawler's paths/identity but Forwarder's settings
                const forwarderTaskData: Forwarder = {
                    ...matchForwarder, // Inherit base props/methods/id from template if any
                    name: taskName,    // OVERRIDE Name
                    websites: undefined, // Clear hardcoded websites
                    origin: crawler.origin,
                    paths: crawler.paths, // Use Crawler's Paths
                    cfg_forwarder: cfg_forwarder as any // Use merged config
                }

                const job = new CronJob(cron as string, async () => {
                    const taskId = `${Math.random().toString(36).substring(2, 9)}`
                    this.log?.info(`starting to dispatch task ${taskName}...`)
                    const task: TaskScheduler.Task = {
                        id: taskId,
                        status: TaskScheduler.TaskStatus.PENDING,
                        data: {
                            ...forwarderTaskData,
                            // Inject connections into task data so pools can access it
                            connections: this.props.connections
                        },
                    }
                    this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, {
                        taskId,
                        task: task,
                    })
                    this.tasks.set(taskId, task)
                })
                this.log?.info(`Auto-Bound Forwarder Task created: ${taskName} using template ${matchForwarder.name}`)
                this.cronJobs.push(job)
            }
        } else {
            this.log?.warn('No crawlers defined for auto-binding.')
        }
    }

    /**
     * 启动定时任务
     */
    async start() {
        this.log?.info('Manager starting...')
        this.cronJobs.forEach((job) => {
            job.start()
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
        this.emitter.removeAllListeners()
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
    private props: Pick<AppConfig, 'forward_targets' | 'cfg_forward_target' | 'connections' | 'formatters'>
    private renderService: RenderService

    /**
     * max allowed error count for a single article in every cycle
     */
    private MAX_ERROR_COUNT = 3
    /**
     * platform:a_id -> error count
     */
    private errorCounter = new Map<string, number>()

    // private workers:
    constructor(props: Pick<AppConfig, 'forward_targets' | 'cfg_forward_target' | 'connections' | 'formatters'>, emitter: EventEmitter, log?: Logger) {
        super()
        this.log = log?.child({ subservice: this.NAME })
        this.renderService = new RenderService(this.log)
        this.emitter = emitter
        this.props = props
    }

    async init() {
        this.log?.info('Forwarder Pools initializing...')
        this.emitter.on(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, this.onTaskReceived.bind(this))
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
            connections
        } = task.data as Forwarder & { connections?: AppConfig['connections'] }
        ctx.log = this.log?.child({ label: name, trace_id: taskId })
        // prepare
        // maybe we will use workers in the future
        this.emitter.emit(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
            taskId,
            status: TaskScheduler.TaskStatus.RUNNING,
        })
        ctx.log?.debug(`Task received: ${JSON.stringify(task)}`)

        if (!websites && !origin && !paths) {
            ctx.log?.error(`No websites or origin or paths found`)
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
                            name
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
                const forwarders = this.getOrInitForwarders(batchId, subscribers, cfg_forwarder, cfg_forward_target, id, connections)
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
        this.emitter.removeAllListeners()
        this.log?.info('Pools dropped')
    }

    async processArticleTask(ctx: TaskScheduler.TaskCtx) {
        const { websites, subscribers, cfg_forwarder, cfg_forward_target, id, connections } = ctx.task.data as {
            websites: Array<string>
            subscribers: Forwarder['subscribers']
            cfg_forwarder: Forwarder['cfg_forwarder']
            cfg_forward_target: Forwarder['cfg_forward_target']
            id?: string
            connections?: AppConfig['connections']
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

            // Collection of all paths for this website
            const allPaths: ForwardingPath[] = []

            // 1. Resolve Graph Connections (Crawler -> Formatter -> Target)
            const crawlerName = ctx.task.data.name
            if (crawlerName && connections && connections['crawler-formatter'] && connections['formatter-target']) {
                const connectedFormatterIds = connections['crawler-formatter'][crawlerName] || []
                const { formatters } = this.props

                for (const formatterId of connectedFormatterIds) {
                    const formatterConfig = formatters?.find(f => f.id === formatterId)
                    if (!formatterConfig) continue

                    const targetIds = connections['formatter-target'][formatterId] || []
                    const validTargets: Array<ForwardTargetInstanceWithRuntimeConfig> = []

                    for (const targetId of targetIds) {
                        const forwarderInstance = this.forward_to.get(targetId)
                        if (forwarderInstance) {
                            validTargets.push({
                                forwarder: forwarderInstance,
                                runtime_config: cfg_forward_target
                            })
                        }
                    }

                    if (validTargets.length > 0) {
                        allPaths.push({
                            formatterConfig: {
                                ...cfg_forwarder,
                                render_type: formatterConfig.render_type as any
                            },
                            targets: validTargets,
                            source: 'graph',
                            formatterName: formatterConfig.name || 'Graph Formatter'
                        })
                    }
                }
            }



            // 3. Execute All Paths
            if (allPaths.length === 0) {
                // Only warn if we really found nothing at all (neither graph nor inline)
                ctx.log?.debug(`No forwarding paths (graph or inline) found for ${url}, skipping...`)
                continue
            }

            for (const path of allPaths) {
                ctx.log?.info(`Processing via path [${path.source}]: ${path.formatterName} for ${path.targets.length} targets`)
                /**
                 * 查询当前网站下的近10篇文章并查询转发
                 */
                await this.processSingleArticleTask(ctx, url.href, path.targets, path.formatterConfig)
            }
        }
    }

    async processSingleArticleTask(
        ctx: TaskScheduler.TaskCtx,
        url: string,
        forwarders: Array<ForwardTargetInstanceWithRuntimeConfig>,
        cfg_forwarder: Forwarder['cfg_forwarder'],
    ) {
        const { u_id, platform } = spiderRegistry.extractBasicInfo(url) ?? {}
        if (!u_id || !platform) {
            ctx.log?.error(`Invalid url: ${url}`)
            return
        }
        const articles = await DB.Article.getArticlesByName(u_id, platform)
        if (articles.length <= 0) {
            ctx.log?.warn(`No articles found for ${url}`)
            return
        }
        /**
         * 一篇文章可能需要被转发至多个平台，先获取一篇文章与forwarder的对应关系
         */
        const articles_forwarders = [] as Array<{
            article: ArticleWithId
            to: Array<ForwardTargetInstanceWithRuntimeConfig>
        }>
        for (const article of articles) {
            const to = [] as Array<ForwardTargetInstanceWithRuntimeConfig>
            for (const forwarder of forwarders) {
                const { forwarder: f } = forwarder
                const id = f.id
                /**
                 * 同一个宏任务循环中，此时可能会有同一个网站运行了两次及以上的定时任务，此时checkExist都是false
                 */
                const exist = await DB.ForwardBy.checkExist(article.id, platform, id, 'article')
                if (!exist) {
                    to.push(forwarder)
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
            ctx.log?.debug(`No articles need to be sent for ${url}`)
            return
        }
        ctx.log?.info(`Ready to send articles for ${url}`)
        // 开始转发文章
        for (const { article, to } of articles_forwarders) {
            // check article
            const article_is_blocked = to.every(({ forwarder: target, runtime_config }) =>
                target.check_blocked('', {
                    timestamp: article.created_at,
                    runtime_config,
                    article: cloneDeep(article),
                }),
            )
            if (article_is_blocked) {
                ctx.log?.warn(`Article ${article.a_id} is blocked by all forwarders, skipping...`)
                // save forwardby
                for (const { forwarder: target } of to) {
                    let currentArticle: ArticleWithId | null = article
                    while (currentArticle && typeof currentArticle === 'object') {
                        await DB.ForwardBy.save(currentArticle.id, platform, target.id, 'article')
                        currentArticle = currentArticle.ref as ArticleWithId | null
                    }
                }
                continue
            }

            ctx.log?.debug(`Processing article ${article.a_id} for ${to.map((i) => i.forwarder.id).join(', ')}`)

            // --- Use RenderService ---
            const renderResult = await this.renderService.process(article, {
                taskId: ctx.taskId,
                render_type: cfg_forwarder?.render_type,
                mediaConfig: cfg_forwarder?.media
            })
            // -------------------------

            let error_for_all = true
            let cloned_article = cloneDeep(article)
            // 对所有订阅者进行转发
            await Promise.all(
                to.map(async ({ forwarder: target, runtime_config }) => {
                    ctx.log?.info(`Sending article ${article.a_id} from ${article.u_id} to ${target.NAME}`)
                    try {
                        const exist = await DB.ForwardBy.checkExist(article.id, platform, target.id, 'article')
                        // 运行前再检查下，因为cron的设定，可能同时会有两个同样的任务在执行
                        // 如果不存在则尝试发送
                        if (!exist) {
                            // --- NEW: No Backfill Logic ---
                            // If article is older than 2 hours, assume it's a backfill/initial bind and skip sending
                            // But mark it as sent so we don't process it again.
                            const ONE_HOUR_SECONDS = 3600 * 2
                            const now = dayjs().unix()
                            if (now - article.created_at > ONE_HOUR_SECONDS) {
                                ctx.log?.info(`Skipping old article ${article.a_id} (created at ${dayjs.unix(article.created_at).format()}) for target ${target.id}`)
                                let currentArticle: ArticleWithId | null = article
                                while (currentArticle && typeof currentArticle === 'object') {
                                    await DB.ForwardBy.save(currentArticle.id, platform, target.id, 'article')
                                    currentArticle = currentArticle.ref as ArticleWithId | null
                                }
                                return // Skip sending
                            }
                            // -----------------------------

                            // --- NEW: Keyword Filter Logic ---
                            // If keywords are defined in config, only send if content matches
                            const keywords = (cfg_forwarder as any)?.keywords as string[] | undefined
                            if (keywords && keywords.length > 0) {
                                const content = article.content || ''
                                const hasKeyword = keywords.some(k => content.includes(k))
                                if (!hasKeyword) {
                                    ctx.log?.debug(`Article ${article.a_id} does not contain any required keywords, skipping for ${target.id}`)
                                    // Mark as sent (ignored) so we don't retry endlessly
                                    let currentArticle: ArticleWithId | null = article
                                    while (currentArticle && typeof currentArticle === 'object') {
                                        await DB.ForwardBy.save(currentArticle.id, platform, target.id, 'article')
                                        currentArticle = currentArticle.ref as ArticleWithId | null
                                    }
                                    return
                                }
                            }
                            // -------------------------------

                            // 先占用发送
                            let currentArticle: ArticleWithId | null = article
                            while (currentArticle && typeof currentArticle === 'object') {
                                await DB.ForwardBy.save(currentArticle.id, platform, target.id, 'article')
                                currentArticle = currentArticle.ref as ArticleWithId | null
                            }
                            try {
                                await target.send(renderResult.text, {
                                    media: renderResult.mediaFiles,
                                    timestamp: article.created_at,
                                    runtime_config,
                                    article: cloned_article,
                                })
                                error_for_all = false
                            } catch (e) {
                                ctx.log?.error(`Error while sending to ${target.id}: ${e}`)
                                let currentArticle: ArticleWithId | null = article
                                while (currentArticle && typeof currentArticle === 'object') {
                                    await DB.ForwardBy.deleteRecord(currentArticle.id, platform, target.id, 'article')
                                    currentArticle = currentArticle.ref as ArticleWithId | null
                                }
                            }
                        }
                    } catch (e) {
                        ctx.log?.error(`DB Error ${target.id}: ${e}`)
                    }
                }),
            )

            /**
             * 如果剩下的转发平台全部都出错，并且在5个循环周期内都没有成功转发，我们认为这个文章已经无法转发了，标记为已转发
             * 比如 413: Request Entity Too Large
             */
            if (error_for_all) {
                // 记录错误次数
                let errorCount = this.errorCounter.get(`${platform}:${cloned_article.a_id}`)
                if (!errorCount) {
                    errorCount = 0
                }
                errorCount = errorCount + 1
                if (errorCount > this.MAX_ERROR_COUNT) {
                    ctx.log?.error(
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
                    ctx.log?.error(`Error count for ${cloned_article.a_id}: ${errorCount}`)
                }
            }
            /**
             * 清理媒体文件
             */
            this.renderService.cleanup(renderResult.mediaFiles)
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
        connections?: AppConfig['connections']
    ): Array<ForwardTargetInstanceWithRuntimeConfig> {
        // Resolve targets from subscribers
        const subscribersList = subscribers || []

        // Resolve targets from connections map if forwarderId is present
        if (forwarderId && connections && connections['forwarder-target']) {
            const targetIds = connections['forwarder-target'][forwarderId]
            if (targetIds && targetIds.length > 0) {
                targetIds.forEach(targetId => {
                    // Avoid duplicates if already in subscribers
                    const exists = subscribersList.some(s => (typeof s === 'string' ? s : s.id) === targetId)
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
                to: subscribersList.length > 0
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
