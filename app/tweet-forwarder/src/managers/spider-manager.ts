import { Logger } from '@idol-bbq-utils/log'
import {
    beginXOperationCapture,
    buildBrowserRequestHeaders,
    HttpStatusError,
    HttpTimeoutError,
    spiderRegistry,
    parseNetscapeCookieToPuppeteerCookie,
} from '@idol-bbq-utils/spider'
import type { BrowserMode, DeviceProfile } from '@idol-bbq-utils/spider'
import { Page } from 'puppeteer-core'
import { CronJob } from 'cron'
import { BaseProcessor, PROCESSOR_ERROR_FALLBACK } from '@/middleware/processor/base'
import EventEmitter from 'events'
import { BaseCompatibleModel, sanitizeWebsites, TaskScheduler, toErrorMessage } from '@/utils/base'
import type { Crawler } from '@/types/crawler'
import type { AppConfig } from '@/types'
import { Platform } from '@idol-bbq-utils/spider/types'
import type { TaskType, TaskTypeResult } from '@idol-bbq-utils/spider/types'
import { BaseSpider } from '@idol-bbq-utils/spider'
import { processorRegistry } from '@/middleware/processor'
import { pRetry } from '@idol-bbq-utils/utils'
import DB from '@/db'
import type { Article } from '@/db'
import { RETRY_LIMIT, CRAWL_RETRY_LIMIT } from '@/config'
import { delay } from '@/utils/time'
import { shuffle } from 'lodash'
import crypto from 'crypto'
import dayjs from 'dayjs'
import { BrowserSessionPool } from '@/services/browser-session-pool'
import { resolveConfiguredCookieFilePath } from '@/services/cookie-file-path-service'
import { InstagramLiveRelayService } from '@/services/instagram-live-relay-service'
import { normalizeCronSecond } from '@/utils/cron'
import {
    inferInstagramProbeTarget,
    inferTikTokProbeTarget,
    inferXProbeTarget,
    probeCrawlerCookieLiveHealth,
    probeInstagramBrowserLoginState,
    type CrawlerCookieLiveProbeResult,
} from '@/services/crawler-health-audit-service'
import {
    inferCookieHealthPlatform,
    summarizeRequiredCookieNames,
    toCookieHealthPlatformFromSpiderPlatform,
    toSpiderPlatformFromCookieHealthPlatform,
    type CookieHealthPlatform,
} from '@/services/crawler-cookie-policy'
import {
    DEFAULT_TICK_SECONDS,
    buildScheduleSnapshot,
    nextCrawlerRunAt,
    resolveCrawlerSchedule,
    resolveTimezoneLabel,
    type CrawlerHotScheduleConfig,
    type ResolvedCrawlerSchedule,
} from '@/services/crawler-schedule-service'
import { enqueueMissingExternalMediaLinksFromXArticle } from '@/services/x-tiktok-link-ingest-service'

/**
 * Host that only renders content for mobile clients (Fanclub). It must be crawled with a
 * phone-shaped browser profile or member content is not visible.
 */
const MOBILE_REQUIRED_HOST = 'nanabunnonijyuuni-mobile.com'
/**
 * Default mobile profile for Fanclub / mobile-only hosts: a large Samsung Android Chrome phone,
 * matching a manual Chrome DevTools emulation of a big Samsung screen.
 */
const DEFAULT_MOBILE_DEVICE_PROFILE: DeviceProfile = 'mobile_android_chrome_samsung_large'
/**
 * Device profiles accepted for mobile-required hosts. iOS Safari stays allowed for explicit
 * opt-in, but desktop profiles are rejected.
 */
const MOBILE_DEVICE_PROFILES: ReadonlySet<DeviceProfile> = new Set<DeviceProfile>([
    'mobile_android_chrome_samsung_large',
    'mobile_ios_safari_portrait',
])
const RISK_COOLDOWN_MS: Record<CrawlErrorClass, number> = {
    auth: 30 * 60 * 1000,
    rate_limit: 20 * 60 * 1000,
    timeout: 0,
    transient: 0,
    parser: 0,
    unknown: 0,
    private_unfollowed: 24 * 60 * 60 * 1000,
    invalid_handle: 24 * 60 * 60 * 1000,
}
const INSTAGRAM_TIMEOUT_COOLDOWN_MS = 5 * 60 * 1000
const INSTAGRAM_AUTH_COOLDOWN_MS = 6 * 60 * 60 * 1000
const INSTAGRAM_RATE_LIMIT_COOLDOWN_MS = 10 * 60 * 1000

type CrawlErrorClass =
    | 'auth'
    | 'rate_limit'
    | 'timeout'
    | 'transient'
    | 'parser'
    | 'unknown'
    | 'private_unfollowed'
    | 'invalid_handle'

interface CrawlTargetError {
    url: string
    classification: CrawlErrorClass
    message: string
}

interface CrawlTargetSkip {
    url: string
    reason: string
}

interface CrawlTargetContext {
    url: URL
    platform: Platform
    sessionProfile?: string
    deviceProfile?: string
}

interface CrawlRiskCooldown {
    expiresAt: number
    classification: CrawlErrorClass
    message: string
}

function sortUnique(values: Array<string>) {
    return Array.from(new Set(values.filter(Boolean))).sort((a, b) => a.localeCompare(b))
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

function articleExtraData(value: unknown) {
    if (typeof value === 'string') {
        try {
            const parsed = JSON.parse(value)
            return parsed && typeof parsed === 'object' ? (parsed as any).data || parsed : null
        } catch {
            return null
        }
    }
    return value && typeof value === 'object' ? (value as any).data || value : null
}

function premiereResolvedExtra(existing: any, next: Article, resolvedAt: number) {
    const existingPremiere = articleExtraData(existing?.extra)?.premiere || {}
    const nextData = articleExtraData(next.extra) || {}
    return {
        data: {
            ...nextData,
            premiere: {
                ...existingPremiere,
                pending: false,
                scheduled_start_at:
                    existingPremiere.scheduled_start_at || (nextData as any)?.premiere?.scheduled_start_at || null,
                resolved_at: resolvedAt,
            },
        },
    }
}

function isPremierePendingArticleLike(article: Pick<Article, 'platform' | 'content' | 'extra'>) {
    if (article.platform !== Platform.YouTube) {
        return false
    }
    const extra = articleExtraData(article.extra)
    return Boolean(extra?.premiere?.pending) || /^coming soon/i.test(String(article.content || '').trim())
}

function shouldRefreshPremiereArticle(existing: any, next: Article) {
    if (
        !isPremierePendingArticleLike({
            platform: next.platform,
            content: existing?.content || null,
            extra: existing?.extra || null,
        })
    ) {
        return false
    }
    // Only trust an explicit hydrated premiere marker: list-page articles carry `extra: null`, and treating
    // "no marker" as resolved would falsely resolve real-titled pending premieres on the next crawl.
    const nextPremiere = articleExtraData(next.extra)?.premiere
    return Boolean(nextPremiere) && !nextPremiere.pending
}

function resolveExistingArticleReusePolicy(cfg_crawler: Crawler['cfg_crawler'] | undefined) {
    const raw = cfg_crawler?.reuse_existing_for_immediate_forward
    const enabled = raw === true || (typeof raw === 'object' && raw?.enabled === true)
    if (!enabled) {
        return null
    }
    const objectConfig = typeof raw === 'object' && raw ? raw : {}
    const clampFinite = (value: unknown, fallback: number, min: number, max?: number) => {
        const numeric = Math.floor(Number(value ?? fallback))
        if (!Number.isFinite(numeric)) {
            return fallback
        }
        return Math.max(min, Math.min(max ?? Number.POSITIVE_INFINITY, numeric))
    }
    return {
        maxAgeSeconds: clampFinite(objectConfig.max_age_seconds, 5 * 60, 1),
        maxItems: clampFinite(objectConfig.max_items, 5, 1, 20),
        reason: objectConfig.reason || 'explicit backfill',
    }
}

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

interface BrowserCookieSnapshot {
    name: string
    value: string
    domain: string
    path: string
    expires?: number
    secure?: boolean
    httpOnly?: boolean
}

interface CrawlerCookieExportOptions {
    validateLiveProbe?: boolean
    seedConfiguredCookieFile?: boolean
    visit?: boolean
    browserModeOverride?: BrowserMode
    fetch?: typeof fetch
    timeoutMs?: number
}

type CrawlerRuntimeSchedule = {
    crawlerName: string
    schedule: ResolvedCrawlerSchedule
    nextRunAt: number | null
    lastRunAt: number | null
    /** Slots consumed while the previous task was still active (skipped by design). */
    deferredSlots: number
    /** Last time the active-task skip warning was emitted (once per active run). */
    lastActiveWarnAt: number | null
}

type ScheduledCrawlerRunPayload = {
    crawler?: string
    name?: string
    websites?: Array<string>
    reason?: string
}

class CrawlerCookieExportError extends Error {
    readonly statusCode = 409
    readonly publicMessage: string
    readonly code: string
    readonly publicDetails?: Record<string, unknown>

    constructor(message: string, code = 'crawler_cookie_export_failed', publicDetails?: Record<string, unknown>) {
        super(message)
        this.name = 'CrawlerCookieExportError'
        this.code = code
        this.publicMessage = message
        this.publicDetails = publicDetails
    }
}

function unwrapRetryError(error: unknown): unknown {
    let current = error
    const seen = new Set<unknown>()
    while (current && typeof current === 'object' && !seen.has(current)) {
        seen.add(current)
        const originalError = (current as { originalError?: unknown }).originalError
        if (!originalError) {
            break
        }
        current = originalError
    }
    return current
}

function statusFromMessage(message: string) {
    const match = message.match(/\b(40[1389]|429|5\d\d)\b/)
    return match ? Number(match[1]) : null
}

function retryAfterMillisFromMessage(message: string): number | null {
    const match = message.match(/\bretry_after=([^\s]+)/)
    if (!match) {
        return null
    }
    const value = match[1]
    if (!value || value === 'none') {
        return null
    }
    if (/^\d+$/.test(value)) {
        const seconds = Number(value)
        if (Number.isFinite(seconds) && seconds > 0 && seconds <= 24 * 60 * 60) {
            return seconds * 1000
        }
        return null
    }
    // X's Retry-After may be an HTTP date; spaces were encoded as '_' so the
    // value stays a single token in the error message.
    const timestamp = Date.parse(value.replace(/_/g, ' '))
    if (!Number.isFinite(timestamp)) {
        return null
    }
    return Math.max(0, timestamp - Date.now())
}

function classifyCrawlError(error: unknown): CrawlErrorClass {
    const root = unwrapRetryError(error)
    if (root instanceof HttpTimeoutError) {
        return 'timeout'
    }
    if (root instanceof HttpStatusError) {
        if (root.status === 401 || root.status === 403) {
            return 'auth'
        }
        if (root.status === 429) {
            return 'rate_limit'
        }
        if (root.status === 408) {
            return 'timeout'
        }
        if (root.status >= 500) {
            return 'transient'
        }
    }

    const message = toErrorMessage(root).toLowerCase()
    const status = statusFromMessage(message)
    if (status === 401 || status === 403) {
        return 'auth'
    }
    if (status === 429) {
        return 'rate_limit'
    }
    if (status === 408) {
        return 'timeout'
    }
    if (status && status >= 500) {
        return 'transient'
    }
    if (/\b(rate limit|rate-limit|too many requests|reached the limit|temporarily blocked)\b/.test(message)) {
        return 'rate_limit'
    }
    if (
        /\b(login_required|checkpoint_required|challenge_required|login|logged out|auth|unauthorized|forbidden|csrf|cookie|cookies expired|check your cookies|checkpoint|challenge|session expired)\b/.test(
            message,
        )
    ) {
        return 'auth'
    }
    if (/\b(timeout|timed out|navigation timeout|aborterror)\b/.test(message)) {
        return 'timeout'
    }
    if (
        /\b(econnreset|socket hang up|network|fetch failed|temporarily unavailable|bad gateway|service unavailable)\b/.test(
            message,
        )
    ) {
        return 'transient'
    }
    if (/browser hydration missing/.test(message)) {
        // TikTok serves the hydration shell inconsistently under risk control; the same account flips
        // between success and "hydration missing" across runs, so treat it as retryable, not a parser break.
        return 'transient'
    }
    if (/instagram_private_unfollowed|private and the current viewer is not following/.test(message)) {
        return 'private_unfollowed'
    }
    if (/tiktok_invalid_handle|handle .* appears to not exist/.test(message)) {
        return 'invalid_handle'
    }
    if (/\b(format may have changed|cannot find|missing initial data|parse|parser|unexpected token)\b/.test(message)) {
        return 'parser'
    }
    return 'unknown'
}

function shouldRetryCrawlError(error: unknown) {
    const classification = classifyCrawlError(error)
    return (
        classification !== 'auth' &&
        classification !== 'rate_limit' &&
        classification !== 'parser' &&
        classification !== 'private_unfollowed' &&
        classification !== 'invalid_handle'
    )
}

function shouldRetryCrawlErrorForPlatform(error: unknown, platform?: Platform) {
    const classification = classifyCrawlError(error)
    if (
        platform === Platform.Instagram &&
        (classification === 'auth' || classification === 'rate_limit' || classification === 'timeout')
    ) {
        return false
    }
    return shouldRetryCrawlError(error)
}

function summarizeCrawlerTaskResult(
    crawlerName: string | undefined,
    result: Array<CrawlerTaskResult>,
    errors: Array<CrawlTargetError> = [],
    skips: Array<CrawlTargetSkip> = [],
) {
    const articleCount = result
        .filter((item) => item.task_type === 'article')
        .reduce((count, item) => count + item.data.length, 0)
    const followsCount = result
        .filter((item) => item.task_type === 'follows')
        .reduce((count, item) => count + item.data.length, 0)
    const suffix = [
        errors.length > 0 ? `${errors.length} warning(s)` : null,
        skips.length > 0 ? `${skips.length} skipped` : null,
    ].filter(Boolean)
    return `crawler ${crawlerName || 'unknown'} completed: ${articleCount} article(s), ${followsCount} follow(s)${
        suffix.length > 0 ? `; ${suffix.join(', ')}` : ''
    }`
}

function summarizeCrawlerErrors(errors: Array<CrawlTargetError>) {
    return errors
        .slice(0, 3)
        .map((error) => `${error.url} [${error.classification}]: ${error.message}`)
        .join('; ')
}

/**
 * 根据cronjob dispatch任务
 * 根据结果查询数据库
 */
class SpiderTaskScheduler extends TaskScheduler.TaskScheduler {
    NAME: string = 'SpiderTaskScheduler'
    protected log?: Logger
    private props: Pick<
        AppConfig,
        'crawlers' | 'cfg_crawler' | 'connections' | 'formatters' | 'forward_targets' | 'processors'
    >
    private taskEventBindings: Array<{ eventName: string; listener: (...args: any[]) => void }> = []
    private crawlersByName = new Map<string, Crawler>()
    private runtimeSchedules = new Map<string, CrawlerRuntimeSchedule>()
    private scheduleTimer?: ReturnType<typeof setInterval>
    private scheduleTickSeconds = DEFAULT_TICK_SECONDS
    private runningScheduleTick = false
    private warmupUntilMs = 0
    private warmupMaxDispatchPerTick = 1

    constructor(
        props: Pick<
            AppConfig,
            'crawlers' | 'cfg_crawler' | 'connections' | 'formatters' | 'forward_targets' | 'processors'
        >,
        emitter: EventEmitter,
        log?: Logger,
        options: { warmupUntilMs?: number; warmupDurationMs?: number; warmupMaxDispatchPerTick?: number } = {},
    ) {
        super(emitter)
        this.props = props
        this.log = log?.child({ subservice: this.NAME })
        const warmupDurationMs = Math.max(0, Number(options.warmupDurationMs) || 0)
        this.warmupUntilMs =
            warmupDurationMs > 0 ? Date.now() + warmupDurationMs : Math.max(0, Number(options.warmupUntilMs) || 0)
        this.warmupMaxDispatchPerTick = Math.max(1, Math.floor(Number(options.warmupMaxDispatchPerTick) || 1))
    }

    async init() {
        this.log?.info('Manager initializing...')
        if (Date.now() < this.warmupUntilMs) {
            this.log?.warn(
                `Soft-start active until ${new Date(this.warmupUntilMs).toISOString()}; max ${this.warmupMaxDispatchPerTick} crawler dispatch(es) per tick`,
            )
        }

        if (!this.props.crawlers) {
            this.log?.warn('Crawler not found, skipping...')
            return
        }

        // 注册基本的监听器
        this.taskEventBindings = Object.entries(this.taskHandlers).map(([eventName, listener]) => ({
            eventName: `spider:${eventName}`,
            listener,
        }))
        for (const binding of this.taskEventBindings) {
            this.emitter.on(binding.eventName, binding.listener)
        }

        // Build non-Cron hot schedules for crawler dispatch. Legacy cron strings
        // are expanded into daily slots only as a compatibility source.
        // Clone before merging: the runtime config snapshot must not be mutated
        // with synthetic scheduler defaults or hot-schedule upserts.
        for (const configuredCrawler of this.props.crawlers) {
            const crawler = {
                ...configuredCrawler,
                cfg_crawler: {
                    cron: '*/30 * * * *',
                    ...this.props.cfg_crawler,
                    ...configuredCrawler.cfg_crawler,
                },
            }
            const crawlerName = crawler.name
            if (!crawlerName) {
                this.log?.warn(
                    `Skipping crawler ${crawler.id || '(unknown)'}: crawler name is required for scheduling.`,
                )
                continue
            }
            this.crawlersByName.set(crawlerName, crawler)
            const schedule = resolveCrawlerSchedule(crawler)
            if (schedule) {
                if (schedule.timezoneUnsupported) {
                    this.log?.warn(
                        `Crawler ${crawler.name} uses an unsupported timezone "${schedule.timezone}"; falling back to ${resolveTimezoneLabel()}`,
                    )
                }
                const nextRunAt = nextCrawlerRunAt(schedule, Math.floor(Date.now() / 1000), crawlerName)
                this.runtimeSchedules.set(crawlerName, {
                    crawlerName,
                    schedule,
                    nextRunAt,
                    lastRunAt: null,
                    deferredSlots: 0,
                    lastActiveWarnAt: null,
                })
                this.scheduleTickSeconds = Math.min(this.scheduleTickSeconds, schedule.tickSeconds)
                this.log?.info(
                    `Crawler schedule created for ${crawler.name}: source=${schedule.source} slots=${schedule.slots.length} next=${nextRunAt ? dayjs.unix(nextRunAt).format() : 'none'}`,
                )
            } else {
                this.log?.warn(`Crawler ${crawler.name} has no runnable schedule.`)
            }

            // Aggregation Task Scheduling
            if (crawler.cfg_crawler.aggregation && crawler.cfg_crawler.aggregation.cron) {
                const aggCron = normalizeCronSecond(crawler.cfg_crawler.aggregation.cron)
                const aggJob = new CronJob(aggCron, async () => {
                    const now = dayjs()
                    const start = now.startOf('day').unix()
                    const end = now.endOf('day').unix()
                    const targetIds = sortUnique(
                        crawler.cfg_crawler?.aggregation?.target_ids || this.resolveAggregationTargetIds(crawler),
                    )

                    const websites = crawler.websites || []
                    const paths = crawler.paths || []
                    const origin = crawler.origin || ''
                    // Combine to full URLs
                    const targets = [...websites, ...paths.map((p) => origin + p)].filter(Boolean)

                    for (const url of targets) {
                        let platform: Platform | null = null
                        let u_id: string | null = null

                        if (url.includes('twitter.com') || url.includes('x.com')) {
                            platform = Platform.Twitter
                            const parts = url.split('/')
                            u_id = parts[parts.length - 1] || null
                        } else if (url.includes('instagram.com')) {
                            platform = Platform.Instagram
                            const parts = url.split('/').filter(Boolean)
                            u_id = parts[parts.length - 1] || null
                        } else if (url.includes('tiktok.com')) {
                            platform = Platform.TikTok
                            const parts = url.split('/').filter(Boolean)
                            const userPart = parts.find((p) => p.startsWith('@'))
                            u_id = (userPart ? userPart.substring(1) : parts[parts.length - 1]) || null
                        } else if (url.includes('youtube.com')) {
                            platform = Platform.YouTube
                            const parts = url.split('/').filter(Boolean)
                            u_id = parts[parts.length - 1] || null
                        }

                        // If we found a valid target, schedule aggregation
                        if (platform && u_id) {
                            const taskType = DB.TaskQueue.TYPE.AggregateDaily
                            const payload = {
                                platform,
                                u_id,
                                start,
                                end,
                                target_ids: targetIds,
                                processorConfig: crawler.cfg_crawler?.processor,
                                processorId:
                                    crawler.cfg_crawler?.aggregation?.processor_id ||
                                    this.resolveCrawlerProcessorId(crawler),
                                prompt: crawler.cfg_crawler?.aggregation?.prompt,
                            }
                            const idempotencyPayload = {
                                platform,
                                u_id,
                                start,
                                end,
                                target_ids: targetIds,
                            }
                            await DB.TaskQueue.add(taskType, payload, now.unix(), {
                                source_ref: `${platform}:${u_id}`,
                                action_type: taskType,
                                idempotency_key: DB.TaskQueue.buildIdempotencyKey(
                                    taskType,
                                    idempotencyPayload,
                                    DB.TaskQueue.IDEMPOTENCY_FORMAT.LegacyJson,
                                ),
                            })
                            this.log?.info(`Scheduled aggregation task for ${platform} ${u_id}`)
                        }
                    }
                })
                this.cronJobs.push(aggJob)
                this.log?.info(`Aggregation schedule created for ${crawler.name} at ${aggCron}`)
            }
        }
    }

    private resolveAggregationTargetIds(crawler: Crawler) {
        const connections = this.props.connections
        const formatterTargetMap = connections?.['formatter-target']
        if (!connections || !formatterTargetMap) {
            return []
        }

        const crawlerKeys = [(crawler as any).id, crawler.name]
        const directFormatterIds = lookupConnectionValues(connections['crawler-formatter'], crawlerKeys) || []
        const processorId = lookupConnectionValues(connections['crawler-processor'], crawlerKeys)
        const viaProcessorFormatterIds = processorId ? connections['processor-formatter']?.[processorId] || [] : []
        const formatterIds = sortUnique([...directFormatterIds, ...viaProcessorFormatterIds])

        const targetIds = new Set<string>()
        for (const formatterId of formatterIds) {
            for (const targetId of formatterTargetMap[formatterId] || []) {
                const targetDef = this.props.forward_targets?.find((target) => target.id === targetId)
                if ((targetDef?.cfg_platform as any)?.bypass_batch === true) {
                    continue
                }
                targetIds.add(targetId)
            }
        }
        return sortUnique(Array.from(targetIds))
    }

    private resolveCrawlerProcessorId(crawler: Crawler) {
        const crawlerKeys = [(crawler as any).id, crawler.name]
        const configured =
            lookupConnectionValues(this.props.connections?.['crawler-processor'], crawlerKeys) ||
            crawler.cfg_crawler?.processor_id ||
            ''
        if (configured) {
            return configured
        }

        // Website articles must have a translation before the immediate forwarder dispatch. Keep
        // this default for the configured 22/7 website host even when an older config omits the map.
        const websites = Array.isArray(crawler.websites) ? crawler.websites : []
        if (websites.some((url) => String(url).toLowerCase().includes('nanabunnonijyuuni-mobile.com/s/n110/'))) {
            return '22_7-social-ja-zh'
        }
        return ''
    }

    private resolveCrawlerProcessor(crawler: Crawler) {
        if (crawler.cfg_crawler?.processor) {
            return crawler.cfg_crawler.processor
        }

        const processorId = this.resolveCrawlerProcessorId(crawler)
        if (!processorId) {
            return undefined
        }
        const processor = (this.props.processors || []).find(
            (candidate) => candidate.id === processorId || candidate.name === processorId,
        )
        if (!processor) {
            this.log?.warn(
                `Processor ${processorId} configured for crawler ${crawler.name || '(unnamed)'} was not found`,
            )
            return undefined
        }
        return processor
    }

    private buildCrawlerTaskData(crawler: Crawler): Crawler {
        const processor = this.resolveCrawlerProcessor(crawler)
        if (!processor) {
            return crawler
        }
        return {
            ...crawler,
            cfg_crawler: {
                ...crawler.cfg_crawler,
                processor,
            },
        }
    }

    /**
     * 启动爬虫管理器
     */
    async start() {
        this.log?.info('Manager starting...')
        this.cronJobs.forEach((job) => {
            job.start()
        })
        this.restartScheduleTimer()
        await this.runScheduleTick()
    }

    /**
     * 停止爬虫管理器
     */
    async stop() {
        // force to stop all tasks

        // stop all cron jobs
        this.cronJobs.forEach((job) => {
            job.stop()
            this.log?.info(`Task dispatcher stopped with cron: ${job.cronTime.source}`)
        })
        if (this.scheduleTimer) {
            clearInterval(this.scheduleTimer)
            this.scheduleTimer = undefined
        }
        this.log?.info('Manager stopped')
    }

    async drop() {
        // Stop cron/timer machinery first: clearing the arrays without
        // stopping the jobs would leave live timers dispatching into a
        // dropped manager.
        await this.stop()
        // 清除所有任务
        this.tasks.clear()
        for (const binding of this.taskEventBindings) {
            this.emitter.off(binding.eventName, binding.listener)
        }
        this.taskEventBindings = []
        this.cronJobs = []
        this.crawlersByName.clear()
        this.runtimeSchedules.clear()
        this.log?.info('Spider Manager dropped')
    }

    private createCrawlerTaskId(prefix: string) {
        return `${prefix}-${Math.random().toString(36).substring(2, 9)}`
    }

    private async dispatchCrawlerTask(
        crawler: Crawler,
        options: {
            source: string
            taskIdPrefix?: string
            scheduledAt?: number | null
            taskQueueId?: number
            taskQueueType?: string
            reason?: string
        },
    ) {
        const crawlerName = crawler.name
        if (!crawlerName) {
            this.log?.warn('Skipping crawler dispatch: crawler is missing a name.')
            return false
        }
        if (this.hasActiveCrawlerTask(crawlerName)) {
            this.log?.warn(`Skipping crawler ${crawlerName}: previous task is still active.`)
            return false
        }

        const taskId = this.createCrawlerTaskId(options.taskIdPrefix || 'schedule')
        this.log?.info(`[${taskId}] Starting to dispatch task: ${crawlerName} source=${options.source}`)
        const task: TaskScheduler.Task = {
            id: taskId,
            status: TaskScheduler.TaskStatus.PENDING,
            data: this.buildCrawlerTaskData(crawler),
            meta: {
                schedule_source: options.source,
                scheduled_at: options.scheduledAt || undefined,
                reason: options.reason || undefined,
                ...(options.taskQueueId
                    ? {
                          task_queue_id: options.taskQueueId,
                          task_queue_type: options.taskQueueType || DB.TaskQueue.TYPE.ScheduledCrawlerRun,
                      }
                    : {}),
            },
        }
        this.tasks.set(taskId, task)
        const dispatched = this.emitter.emit(`spider:${TaskScheduler.TaskEvent.DISPATCH}`, {
            taskId,
            task,
        })
        if (!dispatched) {
            this.tasks.delete(taskId)
            this.log?.error(`No spider dispatcher registered for crawler ${crawler.name}`)
            return false
        }
        return true
    }

    private async runConfiguredSchedules(
        now = Math.floor(Date.now() / 1000),
        maxDispatches = Number.POSITIVE_INFINITY,
    ) {
        const warmupActive = Date.now() < this.warmupUntilMs
        const dispatchBudget = warmupActive ? Math.min(maxDispatches, this.warmupMaxDispatchPerTick) : maxDispatches
        let dispatchedThisTick = 0
        for (const runtimeSchedule of this.runtimeSchedules.values()) {
            if (!runtimeSchedule.nextRunAt || runtimeSchedule.nextRunAt > now) {
                continue
            }
            if (dispatchedThisTick >= dispatchBudget) {
                continue
            }
            const crawler = this.crawlersByName.get(runtimeSchedule.crawlerName)
            if (!crawler) {
                this.log?.warn(`Skipping missing scheduled crawler ${runtimeSchedule.crawlerName}`)
                runtimeSchedule.nextRunAt = nextCrawlerRunAt(
                    runtimeSchedule.schedule,
                    now + runtimeSchedule.schedule.minGapSeconds,
                    runtimeSchedule.crawlerName,
                )
                continue
            }
            const scheduledCrawlerName = crawler.name || runtimeSchedule.crawlerName
            // Active-task skip: the slot is consumed (nextRunAt advances) but the
            // crawler is not dispatched — this is the intended de-throttling when
            // a round takes longer than the window spacing. Log once per active
            // run instead of once per consumed slot (X 2-min windows produced a
            // warn every ~2 minutes for the same active round).
            if (this.hasActiveCrawlerTask(scheduledCrawlerName)) {
                runtimeSchedule.deferredSlots += 1
                const lastWarn = runtimeSchedule.lastActiveWarnAt
                if (!lastWarn || now - lastWarn >= 60) {
                    runtimeSchedule.lastActiveWarnAt = now
                    this.log?.warn(
                        `Skipping crawler ${scheduledCrawlerName}: previous task is still active (${runtimeSchedule.deferredSlots} slot(s) deferred this active run).`,
                    )
                }
                runtimeSchedule.nextRunAt = nextCrawlerRunAt(
                    runtimeSchedule.schedule,
                    Math.max(now, runtimeSchedule.nextRunAt) + runtimeSchedule.schedule.minGapSeconds,
                    runtimeSchedule.crawlerName,
                )
                continue
            }
            if (runtimeSchedule.deferredSlots > 0) {
                runtimeSchedule.deferredSlots = 0
                runtimeSchedule.lastActiveWarnAt = null
            }
            const dispatched = await this.dispatchCrawlerTask(crawler, {
                source: runtimeSchedule.schedule.source,
                taskIdPrefix: 'schedule',
                scheduledAt: runtimeSchedule.nextRunAt,
            })
            if (dispatched) {
                dispatchedThisTick += 1
                runtimeSchedule.lastRunAt = now
            }
            runtimeSchedule.nextRunAt = nextCrawlerRunAt(
                runtimeSchedule.schedule,
                Math.max(now, runtimeSchedule.nextRunAt) + runtimeSchedule.schedule.minGapSeconds,
                runtimeSchedule.crawlerName,
            )
        }
    }

    private normalizeScheduledCrawlerPayload(payload: unknown): ScheduledCrawlerRunPayload {
        return payload && typeof payload === 'object' ? (payload as ScheduledCrawlerRunPayload) : {}
    }

    private async runQueuedScheduledCrawlerRuns(
        now = Math.floor(Date.now() / 1000),
        maxDispatches = Number.POSITIVE_INFINITY,
    ) {
        let dispatchedCount = 0
        await DB.TaskQueue.recoverStaleProcessing(now, 5 * 60, { types: [DB.TaskQueue.TYPE.ScheduledCrawlerRun] })
        const dueTasks = await DB.TaskQueue.getPending(now, { types: [DB.TaskQueue.TYPE.ScheduledCrawlerRun] })
        for (const dueTask of dueTasks) {
            if (dispatchedCount >= maxDispatches) break
            const claimedTask = await DB.TaskQueue.claimPending(dueTask.id)
            if (!claimedTask) {
                continue
            }
            const payload = this.normalizeScheduledCrawlerPayload(claimedTask.payload)
            const crawlerName = String(payload.crawler || payload.name || '').trim()
            const crawler = crawlerName ? this.crawlersByName.get(crawlerName) : undefined
            if (!crawler) {
                await DB.TaskQueue.updateStatus(claimedTask.id, DB.TaskQueue.STATUS.Failed, {
                    last_error: `Crawler not found: ${crawlerName || '(missing)'}`,
                    result_summary: 'scheduled crawler run failed: crawler not found',
                })
                continue
            }
            if (this.hasActiveCrawlerTask(crawlerName)) {
                // Retry on a longer horizon: an active round lasts minutes, and a
                // 60s retry re-writes the queued row on every 10-15s executor tick.
                const retryAt = now + 300
                await DB.TaskQueue.retryLater(claimedTask.id, retryAt, {
                    last_error: 'Crawler already active; delayed scheduled run',
                    result_summary: `scheduled crawler run delayed until ${dayjs.unix(retryAt).format()}`,
                })
                continue
            }
            const dispatchCrawler =
                payload.websites && payload.websites.length > 0
                    ? {
                          ...crawler,
                          websites: payload.websites,
                          origin: undefined,
                          paths: undefined,
                      }
                    : crawler
            const dispatched = await this.dispatchCrawlerTask(dispatchCrawler, {
                source: 'task_queue',
                taskIdPrefix: 'scheduled',
                scheduledAt: claimedTask.execute_at,
                taskQueueId: claimedTask.id,
                taskQueueType: claimedTask.type,
                reason: payload.reason,
            })
            if (!dispatched) {
                await DB.TaskQueue.updateStatus(claimedTask.id, DB.TaskQueue.STATUS.Failed, {
                    last_error: 'No spider dispatcher registered',
                    result_summary: 'scheduled crawler run failed: dispatcher unavailable',
                })
            } else {
                dispatchedCount += 1
            }
        }
        return dispatchedCount
    }

    private async runScheduleTick(now = Math.floor(Date.now() / 1000)) {
        if (this.runningScheduleTick) {
            return
        }
        this.runningScheduleTick = true
        try {
            const warmupActive = Date.now() < this.warmupUntilMs
            if (warmupActive) {
                const activeCrawlers = this.getActiveCrawlerTaskCount()
                const budget = activeCrawlers > 0 ? 0 : this.warmupMaxDispatchPerTick
                const usedQueued = await this.runQueuedScheduledCrawlerRuns(now, budget)
                await this.runConfiguredSchedules(now, Math.max(0, budget - usedQueued))
            } else {
                await this.runQueuedScheduledCrawlerRuns(now)
                await this.runConfiguredSchedules(now)
            }
        } catch (error) {
            this.log?.error(`Crawler schedule tick failed: ${toErrorMessage(error)}`)
        } finally {
            this.runningScheduleTick = false
        }
    }

    private restartScheduleTimer() {
        if (this.scheduleTimer) {
            clearInterval(this.scheduleTimer)
        }
        this.scheduleTimer = setInterval(() => {
            void this.runScheduleTick()
        }, this.scheduleTickSeconds * 1000)
        this.scheduleTimer.unref?.()
    }

    getScheduleSnapshot() {
        return Array.from(this.runtimeSchedules.values()).map((runtimeSchedule) =>
            buildScheduleSnapshot(
                runtimeSchedule.crawlerName,
                runtimeSchedule.schedule,
                runtimeSchedule.nextRunAt,
                runtimeSchedule.lastRunAt,
            ),
        )
    }

    async pokeSchedules() {
        await this.runScheduleTick()
    }

    upsertHotSchedule(crawlerName: string, scheduleConfig: CrawlerHotScheduleConfig) {
        const crawler = this.crawlersByName.get(crawlerName)
        if (!crawler) {
            throw new Error(`Crawler not found: ${crawlerName}`)
        }
        crawler.cfg_crawler = {
            ...crawler.cfg_crawler,
            schedule: scheduleConfig,
        }
        const schedule = resolveCrawlerSchedule(crawler)
        if (!schedule) {
            this.runtimeSchedules.delete(crawlerName)
            return buildScheduleSnapshot(crawlerName, null, null, null)
        }
        const nextRunAt = nextCrawlerRunAt(schedule, Math.floor(Date.now() / 1000), crawler.name)
        const previous = this.runtimeSchedules.get(crawlerName)
        this.runtimeSchedules.set(crawlerName, {
            crawlerName,
            schedule,
            nextRunAt,
            lastRunAt: previous?.lastRunAt || null,
            deferredSlots: previous?.deferredSlots || 0,
            lastActiveWarnAt: previous?.lastActiveWarnAt || null,
        })
        const nextTickSeconds = Math.min(this.scheduleTickSeconds, schedule.tickSeconds)
        if (nextTickSeconds !== this.scheduleTickSeconds) {
            this.scheduleTickSeconds = nextTickSeconds
            if (this.scheduleTimer) {
                this.restartScheduleTimer()
            }
        }
        return buildScheduleSnapshot(crawlerName, schedule, nextRunAt, previous?.lastRunAt || null)
    }

    private getActiveCrawlerTaskCount() {
        return Array.from(this.tasks.values()).filter(
            (task) =>
                task.status === TaskScheduler.TaskStatus.PENDING || task.status === TaskScheduler.TaskStatus.RUNNING,
        ).length
    }

    private hasActiveCrawlerTask(crawlerName: string) {
        return Array.from(this.tasks.values()).some((task) => {
            if (task.data?.name !== crawlerName) {
                return false
            }
            return task.status === TaskScheduler.TaskStatus.PENDING || task.status === TaskScheduler.TaskStatus.RUNNING
        })
    }

    updateTaskStatus(payload: unknown) {
        if (!TaskScheduler.isTaskStatusPayload(payload)) {
            this.log?.warn('Ignoring malformed spider status payload')
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
            this.log?.warn('Ignoring malformed spider finished payload')
            return
        }
        const { taskId, result, immediate_notify } = payload
        this.emitter.emit(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
            taskId,
            status: TaskScheduler.TaskStatus.COMPLETED,
        })
        this.log?.info(`[${taskId}] Task finished.`)
        if (result.length > 0 && immediate_notify) {
            // TODO: notify forwarders by emitter
        }
    }
}

const WEBSITE_MUTABLE_REFRESH_FEEDS = new Set(['radio', 'movie', 'ticket'])

function buildMutableWebsiteArticleRefreshPatch(existing: unknown, incoming: Article) {
    const feed = (incoming.extra as any)?.data?.feed
    if (incoming.platform !== Platform.Website || !feed || !WEBSITE_MUTABLE_REFRESH_FEEDS.has(String(feed))) {
        return null
    }
    const existingRecord = existing && typeof existing === 'object' ? (existing as Record<string, unknown>) : null
    const changedFields = ['content', 'has_media', 'media', 'username', 'u_avatar', 'extra'] as const
    const changed = changedFields.some(
        (field) => JSON.stringify(existingRecord?.[field]) !== JSON.stringify(incoming[field]),
    )
    if (!changed) {
        return null
    }
    return {
        content: incoming.content,
        has_media: incoming.has_media,
        media: incoming.media,
        username: incoming.username,
        u_avatar: incoming.u_avatar,
        extra: incoming.extra,
    }
}

class SpiderPools extends BaseCompatibleModel {
    NAME = 'SpiderPools'
    log?: Logger
    private emitter: EventEmitter
    private processors: Map<string, BaseProcessor> = new Map()
    private browserPool: BrowserSessionPool
    private instagramLiveRelay: InstagramLiveRelayService
    private riskCooldowns = new Map<string, CrawlRiskCooldown>()
    private cooldownEscalations = new Map<
        string,
        { classification: CrawlErrorClass; consecutive: number; lastSetAt: number }
    >()
    // Browser-instance scoped seed bookkeeping: when the pool evicts/relaunches a
    // session, the new Browser object gets a fresh Set and cookies are seeded again.
    private seededBrowserSessions = new WeakMap<object, Set<string>>()
    private warmedBrowserSessions = new Map<string, number>()
    private cookieExportProbeCache = new Map<string, number>()
    private static readonly X_WARMUP_REFRESH_MS = 30 * 60 * 1000
    /**
     * BaseSpider._VALID_URL.source
     */
    private spiders: Map<string, BaseSpider> = new Map()
    private dispatchListener: (payload: unknown) => Promise<void>
    private readonly onSchedulePoke?: () => Promise<void>
    private readonly enqueueExternalMediaLinks: typeof enqueueMissingExternalMediaLinksFromXArticle
    private readonly scheduledWebsitesByCrawler = new Map<string, Array<string>>()
    private stopping = false
    // private workers:
    constructor(
        cacheRoot: string,
        emitter: EventEmitter,
        log?: Logger,
        options?: {
            onSchedulePoke?: () => Promise<void>
            enqueueExternalMediaLinks?: typeof enqueueMissingExternalMediaLinksFromXArticle
            crawlers?: Array<Crawler>
        },
    ) {
        super()
        this.log = log?.child({ subservice: this.NAME })
        this.emitter = emitter
        this.onSchedulePoke = options?.onSchedulePoke
        this.enqueueExternalMediaLinks =
            options?.enqueueExternalMediaLinks || enqueueMissingExternalMediaLinksFromXArticle
        for (const crawler of options?.crawlers || []) {
            if (!crawler.name) {
                continue
            }
            this.scheduledWebsitesByCrawler.set(crawler.name, sanitizeWebsites(crawler))
        }
        this.browserPool = new BrowserSessionPool(cacheRoot, this.log)
        this.instagramLiveRelay = new InstagramLiveRelayService(cacheRoot, this.log)
        this.dispatchListener = this.onDispatchReceived.bind(this)
    }

    async init() {
        this.log?.info('Spider Pools initializing...')
        this.stopping = false
        this.emitter.on(`spider:${TaskScheduler.TaskEvent.DISPATCH}`, this.dispatchListener)
    }

    async hydrateArticleUrlForImmediate(
        rawUrl: string,
        crawler: Crawler,
        options: {
            contextLabel?: string
        } = {},
    ): Promise<Array<number>> {
        if (this.stopping) {
            throw new Error('Spider pool is shutting down')
        }

        const url = new URL(rawUrl)
        const spiderPlugin = spiderRegistry.findByUrl(url.href)
        if (!spiderPlugin) {
            throw new Error(`Spider not found for ${url.href}`)
        }

        let spider = this.spiders.get(spiderPlugin.id)
        if (!spider) {
            spider = spiderPlugin.create(this.log)
            this.spiders.set(spiderPlugin.id, spider)
        }

        const cfg_crawler = crawler.cfg_crawler || {}
        const taskId = `immediate-hydrate-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`
        const ctx: TaskScheduler.TaskCtx = {
            taskId,
            task: {
                id: taskId,
                status: TaskScheduler.TaskStatus.RUNNING,
                data: {
                    ...crawler,
                    websites: [url.href],
                    task_type: 'article',
                    cfg_crawler,
                } satisfies Crawler,
            },
            log: this.log?.child({
                label: options.contextLabel || crawler.name || (crawler as any).id || 'immediate-hydrate',
                trace_id: taskId,
            }),
        }

        let processor: BaseProcessor | undefined
        const processor_cfg = (cfg_crawler as any).processor || (cfg_crawler as any).translator
        if (processor_cfg) {
            const processorCacheKey = crypto
                .createHash('md5')
                .update(
                    JSON.stringify({
                        id: processor_cfg.id,
                        name: processor_cfg.name,
                        provider: processor_cfg.provider,
                        cfg_processor: processor_cfg.cfg_processor || processor_cfg.cfg_translator,
                    }),
                )
                .digest('hex')
            processor = this.processors.get(processorCacheKey)
            if (!processor) {
                processor = await processorRegistry.create(
                    processor_cfg.provider,
                    processor_cfg.api_key,
                    this.log,
                    processor_cfg.cfg_processor || processor_cfg.cfg_translator,
                )
                this.processors.set(processorCacheKey, processor)
                ctx.log?.info(`Processor instance created for ${processor_cfg.provider}`)
            }
        }

        let cookieString: string | undefined
        const cookie_file = cfg_crawler.cookie_file
        if (cookie_file) {
            const cookies = parseNetscapeCookieToPuppeteerCookie(
                resolveConfiguredCookieFilePath(cookie_file) || cookie_file,
            )
            cookieString = cookies.map((cookie) => `${cookie.name}=${cookie.value}`).join('; ')
        }

        const crawl_engine = cfg_crawler.engine
        const browserRequest = this.resolveBrowserRequest(cfg_crawler, url, spiderPlugin.platform)
        const requestHeaders = buildBrowserRequestHeaders(browserRequest.device_profile, {
            extraHeaders: browserRequest.extra_headers,
            locale: browserRequest.locale,
            timezone: browserRequest.timezone,
            userAgent: cfg_crawler.user_agent,
            viewport: browserRequest.viewport,
        })
        const needsBrowser = this.shouldUseBrowserAssist(crawl_engine, spiderPlugin.platform)
        const waitTime = this.resolveWaitTime(
            cfg_crawler.interval_time || {
                min: 0,
                max: 0,
            },
        )
        let page: Page | undefined

        try {
            if (needsBrowser) {
                page = await this.browserPool.createPage({
                    ...browserRequest,
                    user_agent: cfg_crawler.user_agent,
                })
                if (cookie_file && cfg_crawler.seed_cookie_file !== false) {
                    await page
                        .browserContext()
                        .setCookie(
                            ...parseNetscapeCookieToPuppeteerCookie(
                                resolveConfiguredCookieFilePath(cookie_file) || cookie_file,
                            ),
                        )
                }
            }

            if (page && crawl_engine?.startsWith('api')) {
                const warmupFingerprint = JSON.stringify({
                    browser_mode: browserRequest.browser_mode,
                    device_profile: browserRequest.device_profile,
                    session_profile: browserRequest.session_profile,
                    locale: browserRequest.locale,
                    timezone: browserRequest.timezone,
                    viewport: browserRequest.viewport,
                    user_agent: cfg_crawler.user_agent,
                    host: url.hostname,
                })
                await this.primeBrowserSessionIfNeeded(
                    page,
                    url,
                    warmupFingerprint,
                    {
                        url,
                        platform: spiderPlugin.platform,
                        sessionProfile: browserRequest.session_profile,
                        deviceProfile: browserRequest.device_profile,
                    },
                    ctx.log,
                )
            }

            const sessionCookieString =
                needsBrowser && page ? await this.getBrowserCookieString(page, url).catch(() => undefined) : undefined
            const effectiveCookieString = this.mergeCookieStrings(cookieString, sessionCookieString)

            if (waitTime > 0) {
                ctx.log?.info(`[${taskId}] immediate hydrate wait for ${waitTime}ms before ${url.href}`)
                await delay(waitTime)
            }

            return await this.crawlArticle(
                ctx,
                spider,
                url,
                page,
                processor,
                effectiveCookieString,
                requestHeaders,
                spiderPlugin.platform,
            )
        } finally {
            if (page) {
                await page.close().catch(() => null)
            }
        }
    }

    private getLinkedTaskQueueId(task: TaskScheduler.Task) {
        const value = task.meta?.task_queue_id
        const id = typeof value === 'number' ? value : Number(value)
        return Number.isSafeInteger(id) && id > 0 ? id : null
    }

    private async updateLinkedTaskQueue(
        ctx: TaskScheduler.TaskCtx,
        status: DB.TaskQueue.Status,
        meta?: { last_error?: string | null; result_summary?: string | null },
    ) {
        const taskQueueId = this.getLinkedTaskQueueId(ctx.task)
        if (!taskQueueId) {
            return
        }

        try {
            await DB.TaskQueue.updateStatus(taskQueueId, status, meta)
        } catch (error) {
            ctx.log?.warn(`Failed to update linked task_queue ${taskQueueId}: ${toErrorMessage(error)}`)
        }
    }

    private shouldStopForShutdown(log?: Logger, scope = 'crawler task') {
        if (!this.stopping) {
            return false
        }
        log?.warn(`Stopping ${scope}: spider pool is shutting down`)
        return true
    }

    private async cancelTaskForShutdown(ctx: TaskScheduler.TaskCtx, crawlerName?: string) {
        this.emitter.emit(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
            taskId: ctx.taskId,
            status: TaskScheduler.TaskStatus.CANCELLED,
        })
        await this.updateLinkedTaskQueue(ctx, DB.TaskQueue.STATUS.Cancelled, {
            last_error: null,
            result_summary: `crawler ${crawlerName || 'unknown'} cancelled: runtime stopping`,
        })
    }

    private async onDispatchReceived(payload: unknown) {
        if (!TaskScheduler.isTaskCtx(payload)) {
            this.log?.warn('Ignoring malformed spider dispatch payload')
            return
        }
        const ctx = payload
        if (this.stopping) {
            const taskName = (ctx.task.data as Crawler | undefined)?.name || 'unknown'
            ctx.log = ctx.log || this.log?.child({ label: taskName, trace_id: ctx.taskId })
            ctx.log?.warn(`Cancelling crawler task ${ctx.taskId}: pool is shutting down`)
            await this.cancelTaskForShutdown(ctx, taskName)
            return
        }
        try {
            await this.onTaskReceived(ctx)
        } catch (error) {
            const taskName = (ctx.task.data as Crawler | undefined)?.name || 'unknown'
            const message = toErrorMessage(error)
            ctx.log = ctx.log || this.log?.child({ label: taskName, trace_id: ctx.taskId })
            ctx.log?.error(`Unexpected spider dispatch failure: ${message}`)
            try {
                this.emitter.emit(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
                    taskId: ctx.taskId,
                    status: TaskScheduler.TaskStatus.FAILED,
                })
            } catch (emitError) {
                ctx.log?.warn(`Failed to emit spider failure status: ${toErrorMessage(emitError)}`)
            }
            await this.updateLinkedTaskQueue(ctx, DB.TaskQueue.STATUS.Failed, {
                last_error: message,
                result_summary: `crawler ${taskName} failed: unexpected dispatch error`,
            })
        }
    }

    // handle task received
    async onTaskReceived(ctx: TaskScheduler.TaskCtx) {
        const { taskId, task } = ctx
        let { websites, origin, paths, task_type = 'article', cfg_crawler, name } = task.data as Crawler
        ctx.log = this.log?.child({ label: name, trace_id: taskId })
        // prepare
        // maybe we will use workers in the future
        this.emitter.emit(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
            taskId,
            status: TaskScheduler.TaskStatus.RUNNING,
        })
        await this.updateLinkedTaskQueue(ctx, DB.TaskQueue.STATUS.Processing, {
            result_summary: `crawler ${name || 'unknown'} running`,
        })
        ctx.log?.debug(`Task received: ${JSON.stringify(task)}`)
        if (this.shouldStopForShutdown(ctx.log, 'crawler task before start')) {
            await this.cancelTaskForShutdown(ctx, name)
            return
        }
        if (!websites && !origin && !paths) {
            ctx.log?.error(`No websites or origin or paths found`)
            this.emitter.emit(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
                taskId,
                status: TaskScheduler.TaskStatus.CANCELLED,
            })
            await this.updateLinkedTaskQueue(ctx, DB.TaskQueue.STATUS.Cancelled, {
                last_error: 'No websites or origin or paths found',
                result_summary: `crawler ${name || 'unknown'} cancelled: no crawl targets`,
            })
            return
        }
        websites = sanitizeWebsites({
            websites,
            origin,
            paths,
        })
        // TODO: configurable id
        const crawler_batch_id = crypto
            .createHash('md5')
            .update(`${websites.join(',')}`)
            .digest('hex')

        // shuffle it for avoiding bot detection
        websites = shuffle(websites)
        if (websites.length === 0) {
            ctx.log?.error(`No websites found after sanitizing`)
            this.emitter.emit(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
                taskId,
                status: TaskScheduler.TaskStatus.CANCELLED,
            })
            await this.updateLinkedTaskQueue(ctx, DB.TaskQueue.STATUS.Cancelled, {
                last_error: 'No websites found after sanitizing',
                result_summary: `crawler ${name || 'unknown'} cancelled: no sanitized crawl targets`,
            })
            return
        }

        let { processor: _processor, interval_time } = cfg_crawler || {}
        // Fallback for compatibility or if type mismatch
        if (!_processor) {
            _processor = (cfg_crawler as any)?.translator
        }

        let processor: BaseProcessor | undefined = undefined
        if (_processor) {
            const processor_cfg = _processor as any
            processor = this.processors.get(crawler_batch_id)
            if (!processor) {
                try {
                    processor = await processorRegistry.create(
                        processor_cfg.provider,
                        processor_cfg.api_key,
                        this.log,
                        processor_cfg.cfg_processor || processor_cfg.cfg_translator, // compatibility
                    )
                    this.processors.set(crawler_batch_id, processor)
                    ctx.log?.info(`Processor instance created for ${processor_cfg.provider}`)
                } catch (e) {
                    ctx.log?.warn(`Processor not found for ${processor_cfg.provider}: ${e}`)
                }
            }
        }

        let cookieString: string | undefined
        let page: Page | undefined
        let pageKey: string | undefined

        const cookie_file = cfg_crawler?.cookie_file
        if (cookie_file) {
            const cookies = parseNetscapeCookieToPuppeteerCookie(
                resolveConfiguredCookieFilePath(cookie_file) || cookie_file,
            )
            cookieString = cookies.map((c) => `${c.name}=${c.value}`).join('; ')
        }

        const user_agent = cfg_crawler?.user_agent

        interval_time = {
            ...{
                max: 0,
                min: 0,
            },
            ...interval_time,
        }

        let result: Array<CrawlerTaskResult> = []
        let errors: Array<CrawlTargetError> = []
        let skips: Array<CrawlTargetSkip> = []
        let cancelledByShutdown = false

        try {
            // 开始任务
            for (const website of websites) {
                if (this.shouldStopForShutdown(ctx.log, 'crawler website loop')) {
                    cancelledByShutdown = true
                    break
                }
                let targetContext: CrawlTargetContext | undefined
                // 单次系列爬虫任务
                try {
                    const url = new URL(website)
                    const spiderPlugin = spiderRegistry.findByUrl(url.href)
                    if (!spiderPlugin) {
                        ctx.log?.warn(`Spider not found for ${url.href}`)
                        continue
                    }
                    let spider = this.spiders.get(spiderPlugin.id)
                    if (!spider) {
                        spider = spiderPlugin.create(this.log)
                        this.spiders.set(spiderPlugin.id, spider)
                        ctx.log?.info(`Spider instance created for ${url.hostname}`)
                    }

                    const crawl_engine = cfg_crawler?.engine
                    const browserRequest = this.resolveBrowserRequest(cfg_crawler, url, spiderPlugin.platform)
                    const requestHeaders = buildBrowserRequestHeaders(browserRequest.device_profile, {
                        extraHeaders: browserRequest.extra_headers,
                        locale: browserRequest.locale,
                        timezone: browserRequest.timezone,
                        userAgent: user_agent,
                        viewport: browserRequest.viewport,
                    })
                    targetContext = {
                        url,
                        platform: spiderPlugin.platform,
                        sessionProfile: browserRequest.session_profile,
                        deviceProfile: browserRequest.device_profile,
                    }
                    const activeCooldown = this.getActiveCooldown(targetContext)
                    if (activeCooldown) {
                        const reason = `${activeCooldown.classification} cooldown until ${dayjs(activeCooldown.expiresAt).format()}: ${activeCooldown.message}`
                        ctx.log?.warn(`[${url.href}] Skipping crawler target during ${reason}`)
                        skips.push({
                            url: url.href,
                            reason,
                        })
                        continue
                    }
                    const needsBrowser = this.shouldUseBrowserAssist(crawl_engine, spiderPlugin.platform)
                    const waitTime = this.resolveWaitTime(interval_time)

                    const nextPageKey = needsBrowser
                        ? JSON.stringify({
                              browser_mode: browserRequest.browser_mode,
                              device_profile: browserRequest.device_profile,
                              session_profile: browserRequest.session_profile,
                              locale: browserRequest.locale,
                              timezone: browserRequest.timezone,
                              viewport: browserRequest.viewport,
                              user_agent,
                              host: url.hostname,
                          })
                        : undefined

                    if (!needsBrowser && page) {
                        await page.close()
                        page = undefined
                        pageKey = undefined
                        ctx.log?.info('Browser page closed before non-browser crawl')
                    }

                    if (needsBrowser && page && pageKey !== nextPageKey) {
                        await page.close()
                        page = undefined
                        pageKey = undefined
                        ctx.log?.info('Browser page closed before profile switch')
                    }

                    if (needsBrowser && !page) {
                        ctx.log?.info(`Creating browser page for engine: ${crawl_engine || 'browser'} (browser-assist)`)
                        page = await this.browserPool.createPage({
                            ...browserRequest,
                            user_agent,
                        })
                        pageKey = nextPageKey

                        const seedKey = browserRequest.session_profile || browserRequest.device_profile || 'default'
                        if (cookie_file && cfg_crawler?.seed_cookie_file !== false) {
                            const browserInstance = page.browser()
                            const seededKeys = this.seededBrowserSessions.get(browserInstance) || new Set<string>()
                            if (!seededKeys.has(seedKey)) {
                                await page
                                    .browserContext()
                                    .setCookie(
                                        ...parseNetscapeCookieToPuppeteerCookie(
                                            resolveConfiguredCookieFilePath(cookie_file) || cookie_file,
                                        ),
                                    )
                                seededKeys.add(seedKey)
                                this.seededBrowserSessions.set(browserInstance, seededKeys)
                            }
                        }
                    } else if (!needsBrowser) {
                        ctx.log?.debug(`Using non-browser engine: ${crawl_engine}`)
                    }

                    if (page && crawl_engine?.startsWith('api') && targetContext) {
                        await this.primeBrowserSessionIfNeeded(page, url, nextPageKey, targetContext, ctx.log)
                    }

                    const sessionCookieString =
                        needsBrowser && page
                            ? await this.getBrowserCookieString(page, url).catch(() => undefined)
                            : undefined
                    const effectiveCookieString = this.mergeCookieStrings(cookieString, sessionCookieString)

                    ctx.log?.info(`[${taskId}] crawler wait for ${waitTime}ms before ${url.href}`)
                    await delay(waitTime)
                    if (this.shouldStopForShutdown(ctx.log, 'crawler before crawl')) {
                        cancelledByShutdown = true
                        break
                    }

                    if (task_type === 'article') {
                        let saved_article_ids = await this.crawlArticle(
                            ctx,
                            spider,
                            url,
                            page,
                            processor,
                            effectiveCookieString,
                            requestHeaders,
                            spiderPlugin.platform,
                        )

                        result.push({
                            task_type: 'article',
                            url: url.href,
                            data: saved_article_ids,
                        })
                    }

                    if (task_type === 'follows') {
                        const sub_task_type = cfg_crawler?.sub_task_type
                        const follows_res = (await pRetry(
                            () =>
                                spider.crawl(url.href, page, taskId, {
                                    task_type: 'follows',
                                    crawl_engine,
                                    sub_task_type,
                                    cookieString: effectiveCookieString,
                                    requestHeaders,
                                }),
                            {
                                retries: CRAWL_RETRY_LIMIT,
                                shouldRetry: (error) => shouldRetryCrawlErrorForPlatform(error, spiderPlugin.platform),
                                onFailedAttempt: async (error) => {
                                    const classification = classifyCrawlError(error)
                                    ctx.log?.error(
                                        `[${url.href}] Crawl follows failed (${classification}), there are ${error.retriesLeft} retries left: ${error.originalError.message}`,
                                    )
                                    if (classification === 'transient' || classification === 'timeout') {
                                        await delay(500 * error.attemptNumber)
                                    }
                                },
                            },
                        )) as TaskTypeResult<'follows', Platform>
                        for (const follows of follows_res) {
                            let saved_follows_id = (await DB.Follow.save(follows)).id
                            result.push({
                                task_type: 'follows',
                                url: url.href,
                                data: [saved_follows_id],
                            })
                        }
                    }
                } catch (error) {
                    if (this.stopping) {
                        ctx.log?.warn(`Cancelling crawler ${name || 'unknown'} after shutdown-time error: ${error}`)
                        cancelledByShutdown = true
                        break
                    }
                    ctx.log?.error(`Error while crawling for ${website}: ${error}`)
                    const classification = classifyCrawlError(error)
                    const message = toErrorMessage(unwrapRetryError(error))
                    if (targetContext) {
                        this.setCooldownForError(targetContext, classification, message)
                    }
                    errors.push({
                        url: website,
                        classification,
                        message,
                    })
                    continue
                }
            }
        } finally {
            if (page) {
                await page.close()
                ctx.log?.info('Browser page closed')
            }
        }

        if (cancelledByShutdown) {
            await this.cancelTaskForShutdown(ctx, name)
            return
        }

        if (errors.length > 0 && result.length === 0) {
            this.emitter.emit(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
                taskId,
                status: TaskScheduler.TaskStatus.FAILED,
            })
            await this.updateLinkedTaskQueue(ctx, DB.TaskQueue.STATUS.Failed, {
                last_error: summarizeCrawlerErrors(errors),
                result_summary: `crawler ${name || 'unknown'} failed: ${errors.length} error(s)`,
            })
        } else {
            await this.updateLinkedTaskQueue(ctx, DB.TaskQueue.STATUS.Completed, {
                ...(errors.length > 0 ? { last_error: summarizeCrawlerErrors(errors) } : {}),
                result_summary: summarizeCrawlerTaskResult(name, result, errors, skips),
            })
            try {
                this.emitter.emit(`spider:${TaskScheduler.TaskEvent.FINISHED}`, {
                    taskId,
                    result,
                    immediate_notify: cfg_crawler?.immediate_notify,
                    crawlerName: name,
                } as TaskResult)
            } catch (error) {
                ctx.log?.error(`Spider finished listener failed: ${toErrorMessage(error)}`)
            }
        }
    }

    async stop(..._args: any[]): Promise<void> {
        this.stopping = true
        this.emitter.off(`spider:${TaskScheduler.TaskEvent.DISPATCH}`, this.dispatchListener)
        await this.browserPool.closeAll().catch((error) => {
            this.log?.warn(`Failed to close browser sessions during spider stop: ${toErrorMessage(error)}`)
        })
    }

    async drop(...args: any[]): Promise<void> {
        this.log?.info('Dropping Spider Pools...')
        await this.stop(...args)
        this.spiders.clear()
        this.processors.clear()
        await this.browserPool.closeAll()
        this.log?.info('Browser sessions closed')
        this.log?.info('Spider Pools dropped')
    }

    async exportCrawlerCookies(
        crawler: Crawler,
        options: CrawlerCookieExportOptions = {},
    ): Promise<{
        cookies: Array<BrowserCookieSnapshot>
        visitedUrl: string
        sessionProfile: string | null
        browser: {
            session_profile: string | null
            configured_browser_mode: BrowserMode | null
            effective_browser_mode: BrowserMode | null
            device_profile: string | null
        }
        domains: Array<string>
        platformHint: CookieHealthPlatform
        requiredCookieNames: {
            present: Array<string>
            missing: Array<string>
        }
        liveProbe: {
            checked: boolean
            status: CrawlerCookieLiveProbeResult['status']
            diagnostic_codes: Array<string>
            http_status: number | null
        }
    }> {
        const websites = sanitizeWebsites({
            websites: crawler.websites,
            origin: crawler.origin,
            paths: crawler.paths,
        })
        const visitedUrl = websites[0] || crawler.origin
        if (!visitedUrl) {
            throw new Error(`Crawler ${crawler.name || 'unknown'} has no usable URL for cookie sync`)
        }

        const url = new URL(visitedUrl)
        const crawlerPlatformHint = inferCookieHealthPlatform(crawler)
        const spiderPlugin = spiderRegistry.findByUrl(url.href)
        const platform =
            spiderPlugin?.platform || toSpiderPlatformFromCookieHealthPlatform(crawlerPlatformHint) || Platform.Website
        const platformHint =
            crawlerPlatformHint === 'unknown' ? toCookieHealthPlatformFromSpiderPlatform(platform) : crawlerPlatformHint
        const browserRequest = this.resolveBrowserRequest(crawler.cfg_crawler, url, platform)
        const effectiveBrowserRequest = options.browserModeOverride
            ? {
                  ...browserRequest,
                  browser_mode: options.browserModeOverride,
              }
            : browserRequest
        const browserDetails = {
            session_profile: effectiveBrowserRequest.session_profile || null,
            configured_browser_mode: browserRequest.browser_mode || null,
            effective_browser_mode: effectiveBrowserRequest.browser_mode || null,
            device_profile: effectiveBrowserRequest.device_profile || null,
        }
        if (!browserRequest.session_profile) {
            throw new CrawlerCookieExportError(
                `Crawler ${crawler.name || visitedUrl} is missing session_profile`,
                'crawler_cookie_session_profile_missing',
                {
                    cookie_count: 0,
                    domains: [],
                    required_cookie_names: summarizeRequiredCookieNames(platformHint, []),
                    browser: browserDetails,
                    live_probe: {
                        checked: false,
                        status: 'skipped',
                        diagnostic_codes: ['browser_session_profile_missing'],
                        http_status: null,
                    },
                },
            )
        }

        const targetDomains = this.resolveCookieDomains(websites, platform)
        let page: Page
        try {
            page = await this.browserPool.createPage({
                ...effectiveBrowserRequest,
                user_agent: crawler.cfg_crawler?.user_agent,
            })
        } catch (error) {
            throw new CrawlerCookieExportError(
                `Browser session ${effectiveBrowserRequest.session_profile} failed to create a page for cookie export`,
                'crawler_cookie_browser_page_failed',
                {
                    cookie_count: 0,
                    domains: targetDomains,
                    required_cookie_names: summarizeRequiredCookieNames(platformHint, []),
                    browser: browserDetails,
                    error_name: error instanceof Error ? error.name : typeof error,
                    live_probe: {
                        checked: false,
                        status: 'skipped',
                        diagnostic_codes: ['browser_page_not_created'],
                        http_status: null,
                    },
                },
            )
        }

        try {
            const existingCookies = await page.browserContext().cookies()
            const existingRelevantCookies = existingCookies.filter((cookie) =>
                this.matchCookieDomain(cookie.domain, targetDomains),
            )
            const existingRequiredCookieNames = summarizeRequiredCookieNames(
                platformHint,
                existingRelevantCookies.map((cookie) => cookie.name),
            )
            if (
                options.seedConfiguredCookieFile !== false &&
                (existingRelevantCookies.length === 0 || existingRequiredCookieNames.missing.length > 0) &&
                crawler.cfg_crawler?.cookie_file
            ) {
                try {
                    await page
                        .browserContext()
                        .setCookie(
                            ...parseNetscapeCookieToPuppeteerCookie(
                                resolveConfiguredCookieFilePath(crawler.cfg_crawler.cookie_file) ||
                                    crawler.cfg_crawler.cookie_file,
                            ),
                        )
                    this.log?.info(
                        `Seeded browser session ${browserRequest.session_profile} from configured cookie file before cookie export.`,
                    )
                } catch (error) {
                    this.log?.warn(`Failed to seed browser session for ${crawler.name || visitedUrl}: ${error}`)
                }
            }

            // A session validated recently does not need a fresh visit + live probe
            // on every maintenance run: reuse the persistent browser jar for the
            // export and re-validate on a 30-minute TTL instead.
            const probeCacheKey = `${browserRequest.session_profile || browserRequest.device_profile || 'default'}|${platformHint}`
            const lastProbeOkAt = this.cookieExportProbeCache.get(probeCacheKey)
            const skipVisitAndProbe = typeof lastProbeOkAt === 'number' && Date.now() - lastProbeOkAt < 30 * 60 * 1000

            if (options.visit !== false && !skipVisitAndProbe) {
                await page
                    .goto(visitedUrl, {
                        waitUntil: 'domcontentloaded',
                        timeout: 15000,
                    })
                    .catch(async () => {
                        await page.goto(url.origin, {
                            waitUntil: 'domcontentloaded',
                            timeout: 15000,
                        })
                    })
            }

            const cookies = await page.browserContext().cookies()
            const filteredCookies = cookies
                .filter((cookie) => this.matchCookieDomain(cookie.domain, targetDomains))
                .map((cookie) => ({
                    name: cookie.name,
                    value: cookie.value,
                    domain: cookie.domain,
                    path: cookie.path,
                    expires: cookie.expires,
                    secure: cookie.secure,
                    httpOnly: cookie.httpOnly,
                }))
            const requiredCookieNames = summarizeRequiredCookieNames(
                platformHint,
                filteredCookies.map((cookie) => cookie.name),
            )
            if (requiredCookieNames.missing.length > 0) {
                throw new CrawlerCookieExportError(
                    `Browser session ${browserRequest.session_profile} is missing required ${platformHint} cookies: ${requiredCookieNames.missing.join(', ')}`,
                    'crawler_cookie_required_names_missing',
                    {
                        cookie_count: filteredCookies.length,
                        domains: targetDomains,
                        required_cookie_names: requiredCookieNames,
                        browser: browserDetails,
                        live_probe: {
                            checked: false,
                            status: 'skipped',
                            diagnostic_codes: ['live_probe_static_cookie_unhealthy'],
                            http_status: null,
                        },
                    },
                )
            }
            let liveProbe: CrawlerCookieLiveProbeResult = {
                status: 'skipped',
                diagnostic_codes: ['live_probe_not_requested'],
                http_status: null,
            }
            if (options.validateLiveProbe && !skipVisitAndProbe) {
                liveProbe = await probeCrawlerCookieLiveHealth(platformHint, filteredCookies, {
                    fetch: options.fetch,
                    timeoutMs: options.timeoutMs,
                    xProbeTarget: platformHint === 'x' ? inferXProbeTarget(crawler) : undefined,
                    instagramProbeTarget: platformHint === 'instagram' ? inferInstagramProbeTarget(crawler) : undefined,
                    tiktokProbeTarget: platformHint === 'tiktok' ? inferTikTokProbeTarget(crawler) : undefined,
                    instagramBrowserProbe:
                        platformHint === 'instagram' && options.visit !== false
                            ? () => probeInstagramBrowserLoginState(page)
                            : undefined,
                })
                if (liveProbe.status === 'fail') {
                    throw new CrawlerCookieExportError(
                        `Browser session ${browserRequest.session_profile} failed live ${platformHint} cookie probe: ${liveProbe.diagnostic_codes.join(', ')}`,
                        'crawler_cookie_live_probe_failed',
                        {
                            cookie_count: filteredCookies.length,
                            domains: targetDomains,
                            required_cookie_names: requiredCookieNames,
                            browser: browserDetails,
                            live_probe: {
                                checked: true,
                                status: liveProbe.status,
                                diagnostic_codes: liveProbe.diagnostic_codes,
                                http_status: liveProbe.http_status,
                            },
                        },
                    )
                }
                this.cookieExportProbeCache.set(probeCacheKey, Date.now())
            } else if (skipVisitAndProbe) {
                liveProbe = {
                    status: 'skipped',
                    diagnostic_codes: ['live_probe_recently_validated'],
                    http_status: null,
                }
            }

            return {
                cookies: filteredCookies,
                visitedUrl,
                sessionProfile: browserRequest.session_profile || null,
                browser: browserDetails,
                domains: targetDomains,
                platformHint,
                requiredCookieNames,
                liveProbe: {
                    checked: liveProbe.status !== 'skipped',
                    status: liveProbe.status,
                    diagnostic_codes: liveProbe.diagnostic_codes,
                    http_status: liveProbe.http_status,
                },
            }
        } finally {
            await page.close().catch(() => null)
        }
    }

    private resolveBrowserRequest(cfg_crawler: Crawler['cfg_crawler'] | undefined, url: URL, platform: Platform) {
        const requiresMobileProfile = platform === Platform.Website || url.hostname === MOBILE_REQUIRED_HOST
        // Fanclub / mobile-only hosts must look like a real phone, otherwise member content is not
        // rendered. Default to the large Samsung Android Chrome profile and only allow explicit
        // mobile overrides (iOS Safari is allowed but discouraged).
        const deviceProfile =
            cfg_crawler?.device_profile || (requiresMobileProfile ? DEFAULT_MOBILE_DEVICE_PROFILE : 'desktop_chrome')

        if (requiresMobileProfile && !MOBILE_DEVICE_PROFILES.has(deviceProfile)) {
            throw new Error(
                `Crawler for ${url.hostname} must use a mobile device profile (${Array.from(
                    MOBILE_DEVICE_PROFILES,
                ).join(', ')}), got ${deviceProfile}`,
            )
        }

        return {
            browser_mode: cfg_crawler?.browser_mode,
            device_profile: deviceProfile,
            session_profile:
                cfg_crawler?.session_profile ||
                (requiresMobileProfile ? `${deviceProfile}:${url.hostname}` : undefined),
            extra_headers: cfg_crawler?.extra_headers,
            viewport: cfg_crawler?.viewport,
            locale: cfg_crawler?.locale,
            timezone: cfg_crawler?.timezone,
        }
    }

    private shouldUseBrowserAssist(crawl_engine: string | undefined, platform: Platform) {
        if (platform === Platform.Website) {
            return true
        }

        if (!crawl_engine || crawl_engine === 'browser') {
            return true
        }

        return crawl_engine.startsWith('api')
    }

    private crawlCooldownKey(context: CrawlTargetContext) {
        // Instagram cooldowns include the profile path so one failing handle does
        // not cool the whole shared session (11 handles used to back off together
        // after a single 45s timeout).
        const targetScope =
            context.platform === Platform.Instagram ? context.url.pathname.split('/').filter(Boolean)[0] || 'root' : ''
        return [
            context.platform,
            context.url.hostname,
            context.sessionProfile || context.deviceProfile || 'stateless',
            targetScope,
        ]
            .filter(Boolean)
            .join(':')
    }

    private getActiveCooldown(context: CrawlTargetContext): CrawlRiskCooldown | null {
        const key = this.crawlCooldownKey(context)
        const cooldown = this.riskCooldowns.get(key)
        if (!cooldown) {
            return null
        }
        if (cooldown.expiresAt <= Date.now()) {
            this.riskCooldowns.delete(key)
            return null
        }
        return cooldown
    }

    private setCooldownForError(context: CrawlTargetContext, classification: CrawlErrorClass, message: string) {
        let duration =
            context.platform === Platform.Instagram
                ? classification === 'auth'
                    ? INSTAGRAM_AUTH_COOLDOWN_MS
                    : classification === 'rate_limit'
                      ? INSTAGRAM_RATE_LIMIT_COOLDOWN_MS
                      : classification === 'timeout'
                        ? INSTAGRAM_TIMEOUT_COOLDOWN_MS
                        : RISK_COOLDOWN_MS[classification] || 0
                : RISK_COOLDOWN_MS[classification] || 0
        if (duration <= 0 && classification !== 'rate_limit') {
            return
        }
        // Respect the platform's Retry-After hint when X embeds one in the 429
        // error. This stops the crawler from re-hitting an endpoint the platform
        // explicitly told us to leave alone for N seconds/minutes.
        if (classification === 'rate_limit') {
            const retryAfterMs = retryAfterMillisFromMessage(message)
            if (retryAfterMs !== null && retryAfterMs > duration) {
                duration = Math.min(retryAfterMs, 6 * 60 * 60 * 1000)
            }
        }
        if (duration <= 0) {
            return
        }
        const key = this.crawlCooldownKey(context)
        const now = Date.now()
        // A target that fails the same way every round is not helped by hammering:
        // escalate repeat cooldowns exponentially (up to 6h) so failing targets
        // back off instead of re-attempting at full frequency forever.
        const history = this.cooldownEscalations.get(key)
        let consecutive = 1
        if (history && history.classification === classification && now - history.lastSetAt < 2 * 60 * 60 * 1000) {
            consecutive = history.consecutive + 1
        }
        const multiplier = Math.min(Math.pow(2, consecutive - 1), 8)
        const effectiveDuration = Math.min(duration * multiplier, 6 * 60 * 60 * 1000)
        this.cooldownEscalations.set(key, {
            classification,
            consecutive,
            lastSetAt: now,
        })
        if (consecutive > 1 && effectiveDuration > duration) {
            this.log?.warn(
                `Escalating ${classification} cooldown for ${context.url.hostname} (attempt ${consecutive}): ${Math.round(duration / 60000)}m -> ${Math.round(effectiveDuration / 60000)}m`,
            )
        }
        this.riskCooldowns.set(key, {
            expiresAt: now + effectiveDuration,
            classification,
            message,
        })
    }

    private resolveWaitTime(interval_time: NonNullable<Crawler['cfg_crawler']>['interval_time']) {
        const min = Math.max(0, interval_time?.min || 0)
        const max = Math.max(min, interval_time?.max || min)

        if (max === min) {
            return min
        }

        return Math.floor(Math.random() * (max - min + 1)) + min
    }

    private pageIsAlreadyOnHost(page: Page, url: URL): boolean {
        try {
            const current = page.url()
            if (!current || current === 'about:blank') {
                return false
            }
            return new URL(current).hostname === url.hostname
        } catch {
            return false
        }
    }

    private shouldPrimeBrowserSession(
        page: Page,
        url: URL,
        sessionFingerprint: string | undefined,
        context: CrawlTargetContext,
    ): boolean {
        // A page already on this host was warmed earlier in the same task.
        if (this.pageIsAlreadyOnHost(page, url)) {
            return false
        }
        // During/right after an auth or rate-limit cooldown the warmup is the
        // cheapest opportunity to validate refreshed cookies; never skip it there.
        if (this.getActiveCooldown(context)) {
            return true
        }
        // The YouTube spider consumes only cookies, never warmup DOM; cookie
        // freshness is maintained by the dedicated keepalive tooling. Skip the
        // full page load entirely (cooldown recovery is kept above).
        if (context.platform === Platform.YouTube) {
            return false
        }
        if (!sessionFingerprint) {
            return true
        }
        const last = this.warmedBrowserSessions.get(sessionFingerprint)
        return !last || Date.now() - last >= SpiderPools.X_WARMUP_REFRESH_MS
    }

    private async primeBrowserSessionIfNeeded(
        page: Page,
        url: URL,
        sessionFingerprint: string | undefined,
        context: CrawlTargetContext,
        log?: Logger,
    ): Promise<void> {
        if (!this.shouldPrimeBrowserSession(page, url, sessionFingerprint, context)) {
            return
        }
        await this.primeBrowserSession(page, url, log)
        if (sessionFingerprint) {
            this.warmedBrowserSessions.set(sessionFingerprint, Date.now())
            if (this.warmedBrowserSessions.size > 512) {
                const oldest = Array.from(this.warmedBrowserSessions.entries()).sort((a, b) => a[1] - b[1])[0]
                if (oldest) {
                    this.warmedBrowserSessions.delete(oldest[0])
                }
            }
        }
    }

    private async primeBrowserSession(page: Page, url: URL, log?: Logger) {
        try {
            // X api engines capture GraphQL query ids/headers from a page navigation.
            // Buffering requests during warmup lets the spider consume what this
            // navigation already triggered instead of reloading the same URL right
            // after (cold-cache rounds otherwise paid two full page loads).
            if (url.hostname === 'x.com' || url.hostname === 'twitter.com') {
                beginXOperationCapture(page)
            }
            await page.goto(url.href, {
                waitUntil: 'domcontentloaded',
                timeout: 15000,
            })
            await page
                .waitForFunction(() => document.readyState === 'interactive' || document.readyState === 'complete', {
                    timeout: 5000,
                })
                .catch(() => null)

            const dwellTime = 900 + Math.floor(Math.random() * 1800)
            await delay(dwellTime)

            const viewport = page.viewport()
            if (viewport) {
                const targetX = Math.floor(viewport.width * (0.2 + Math.random() * 0.6))
                const targetY = Math.floor(viewport.height * (0.2 + Math.random() * 0.5))
                await page.mouse.move(targetX, targetY, { steps: 12 }).catch(() => null)
            }

            // Real user scrolling emits wheel events; window.scrollBy() does not and is a detectable
            // automation marker. Scroll in a few jittered wheel steps, pause, then scroll back a bit.
            const scrollAmount = 160 + Math.floor(Math.random() * 520)
            const downSteps = 2 + Math.floor(Math.random() * 2)
            for (let step = 0; step < downSteps; step += 1) {
                const stepDelta = Math.floor(scrollAmount / downSteps) * (0.85 + Math.random() * 0.3)
                await page.mouse.wheel({ deltaY: Math.floor(stepDelta) }).catch(() => null)
                await delay(90 + Math.floor(Math.random() * 260))
            }
            await delay(250 + Math.floor(Math.random() * 700))
            const upSteps = 1 + Math.floor(Math.random() * 2)
            for (let step = 0; step < upSteps; step += 1) {
                const stepDelta = Math.floor((scrollAmount * 0.4) / upSteps) * (0.85 + Math.random() * 0.3)
                await page.mouse.wheel({ deltaY: -Math.floor(stepDelta) }).catch(() => null)
                await delay(80 + Math.floor(Math.random() * 220))
            }
        } catch (error) {
            log?.warn(`Browser session warmup failed for ${url.href}: ${error}`)
        }
    }

    private async getBrowserCookieString(page: Page, url: URL) {
        const cookies = await page.browserContext().cookies()
        return cookies.map((cookie) => `${cookie.name}=${cookie.value}`).join('; ')
    }

    private resolveCookieDomains(websites: Array<string>, platform: Platform) {
        const domains = new Set<string>()
        for (const website of websites) {
            try {
                domains.add(new URL(website).hostname.replace(/^\./, '').toLowerCase())
            } catch {
                continue
            }
        }

        if (platform === Platform.X) {
            domains.add('x.com')
            domains.add('twitter.com')
            domains.add('api.x.com')
        } else if (platform === Platform.Instagram) {
            domains.add('instagram.com')
        } else if (platform === Platform.TikTok) {
            domains.add('tiktok.com')
        } else if (platform === Platform.YouTube) {
            domains.add('youtube.com')
            domains.add('youtu.be')
        }

        return Array.from(domains)
    }

    private matchCookieDomain(cookieDomain: string, targetDomains: Array<string>) {
        const normalizedCookieDomain = String(cookieDomain || '')
            .replace(/^\./, '')
            .toLowerCase()
        if (!normalizedCookieDomain || targetDomains.length === 0) {
            return true
        }

        return targetDomains.some((targetDomain) => {
            const normalizedTargetDomain = targetDomain.replace(/^\./, '').toLowerCase()
            return (
                normalizedCookieDomain === normalizedTargetDomain ||
                normalizedCookieDomain.endsWith(`.${normalizedTargetDomain}`) ||
                normalizedTargetDomain.endsWith(`.${normalizedCookieDomain}`)
            )
        })
    }

    private mergeCookieStrings(...cookieStrings: Array<string | undefined>) {
        const merged = new Map<string, string>()

        for (const cookieString of cookieStrings.filter(Boolean)) {
            for (const entry of String(cookieString).split(';')) {
                const [rawName, ...rawValue] = entry.split('=')
                const name = rawName?.trim()
                if (!name) {
                    continue
                }
                merged.set(name, rawValue.join('=').trim())
            }
        }

        return merged.size > 0
            ? Array.from(merged.entries())
                  .map(([name, value]) => `${name}=${value}`)
                  .join('; ')
            : undefined
    }

    private async crawlArticle(
        ctx: TaskScheduler.TaskCtx,
        spider: BaseSpider,
        url: URL,
        page?: Page,
        processor?: BaseProcessor,
        cookieString?: string,
        requestHeaders?: Record<string, string>,
        platform?: Platform,
    ): Promise<Array<number>> {
        const { cfg_crawler } = ctx.task.data as Crawler
        const {
            engine,
            sub_task_type,
            hydrate_users,
            hydrate_limit,
            hydrate_concurrency,
            hydrate_interval_time,
            max_list_pages,
            max_detail_count,
            detail_interval_time,
            block_resource_types,
        } = cfg_crawler || {}
        const liveRelayOnly = Boolean((cfg_crawler?.live_relay as any)?.only)
        const articles = liveRelayOnly
            ? ([] as Array<Article>)
            : await pRetry(
                  () =>
                      spider.crawl(url.href, page, ctx.taskId, {
                          task_type: 'article',
                          crawl_engine: engine,
                          sub_task_type,
                          hydrate_users,
                          hydrate_limit,
                          hydrate_concurrency,
                          hydrate_interval_time,
                          cookieString,
                          requestHeaders,
                          max_list_pages,
                          max_detail_count,
                          detail_interval_time,
                          block_resource_types,
                          isArticleKnown: platform
                              ? async (a_id: string) => Boolean(await DB.Article.getByArticleCode(a_id, platform))
                              : undefined,
                          // Single article-state lookup; the shape differs per platform:
                          // Website mutable feeds need TTL-gated re-crawls driven by the
                          // stored row's created_at, YouTube needs known + premiere-pending.
                          articleStateLookup:
                              platform === Platform.Website
                                  ? async (a_id: string) => {
                                        const existing = await DB.Article.getByArticleCode(a_id, platform)
                                        return {
                                            known: Boolean(existing),
                                            createdAt: existing?.created_at ?? null,
                                            crawledAt: Number((existing?.extra as any)?.data?.crawled_at) || null,
                                        }
                                    }
                                  : platform === Platform.YouTube
                                    ? async (a_id: string) => {
                                          const existing = await DB.Article.getByArticleCode(a_id, platform)
                                          return {
                                              known: Boolean(existing),
                                              storedPremierePending: existing
                                                  ? isPremierePendingArticleLike({
                                                        platform,
                                                        content: (existing as any)?.content || null,
                                                        extra: (existing as any)?.extra || null,
                                                    })
                                                  : false,
                                          }
                                      }
                                    : undefined,
                          articlePrefixStateLookup:
                              platform === Platform.Website
                                  ? async (prefix: string) => {
                                        const existing = await DB.Article.getLatestByArticleCodePrefix(prefix, platform)
                                        return {
                                            known: Boolean(existing),
                                            createdAt: existing?.created_at ?? null,
                                            crawledAt: Number((existing?.extra as any)?.data?.crawled_at) || null,
                                        }
                                    }
                                  : undefined,
                          isStoredPremierePending:
                              platform === Platform.YouTube
                                  ? async (a_id: string) => {
                                        const existing = await DB.Article.getByArticleCode(a_id, platform)
                                        if (!existing) {
                                            return false
                                        }
                                        return isPremierePendingArticleLike({
                                            platform,
                                            content: (existing as any)?.content || null,
                                            extra: (existing as any)?.extra || null,
                                        })
                                    }
                                  : undefined,
                      }),
                  {
                      retries: CRAWL_RETRY_LIMIT,
                      shouldRetry: (error) => shouldRetryCrawlErrorForPlatform(error, platform),
                      onFailedAttempt: async (error) => {
                          const classification = classifyCrawlError(error)
                          ctx.log?.error(
                              `[${url.href}] Crawl article failed (${classification}), there are ${error.retriesLeft} retries left: ${error.originalError.message}`,
                          )
                          if (classification === 'transient' || classification === 'timeout') {
                              await delay(500 * error.attemptNumber)
                          }
                      },
                  },
              )
        await this.maybeSyncInstagramLiveRelay(ctx, url, page, cookieString, requestHeaders)
        const existingArticleReusePolicy = resolveExistingArticleReusePolicy(cfg_crawler)
        let new_articles: Array<Article> = []
        let dispatch_article_ids: Array<number> = []
        const premiere_dispatch_ids: Array<number> = []
        let saved_articles_count = 0
        const now = Math.floor(Date.now() / 1000)
        for (const article of articles) {
            const isExist = await DB.Article.checkExist(article)
            if (!isExist) {
                new_articles.push(article)
                continue
            }
            if (shouldRefreshPremiereArticle(isExist, article)) {
                const resolvedAt = Math.floor(Date.now() / 1000)
                const updated = await DB.Article.update(isExist.id, article.platform, {
                    ...article,
                    created_at: article.created_at || resolvedAt,
                    extra: premiereResolvedExtra(isExist, article, resolvedAt) as any,
                    // DB columns are nullable; Article's render-side type is not.
                    translation: null,
                    translated_by: null,
                } as unknown as Partial<Article>)
                premiere_dispatch_ids.push((updated as any).id || isExist.id)
                ctx.log?.info(
                    `[${url.href}] Refreshed premiere placeholder article ${article.a_id} with public YouTube metadata.`,
                )
                continue
            }
            const mutableWebsiteRefreshPatch = buildMutableWebsiteArticleRefreshPatch(isExist, article)
            if (mutableWebsiteRefreshPatch) {
                await DB.Article.update(isExist.id, article.platform, mutableWebsiteRefreshPatch)
                ctx.log?.info(
                    `[${url.href}] Refreshed mutable website article ${article.a_id} metadata (${String(
                        (article.extra as any)?.data?.feed || 'website',
                    )}).`,
                )
                continue
            }
            if (
                existingArticleReusePolicy &&
                now - Number(isExist.created_at || 0) <= existingArticleReusePolicy.maxAgeSeconds
            ) {
                dispatch_article_ids.push(isExist.id)
            }
        }
        if (new_articles.length === 0) {
            const dedupedPremiereIds = Array.from(new Set(premiere_dispatch_ids))
            if (dispatch_article_ids.length > 0) {
                // Premiere-resolution dispatches are driven by stored state, not the reuse policy; they must
                // not be capped by (or require) `reuse_existing_for_immediate_forward`.
                const dedupedDispatchIds = [
                    ...dedupedPremiereIds,
                    ...Array.from(new Set(dispatch_article_ids)).slice(0, existingArticleReusePolicy?.maxItems || 0),
                ]
                ctx.log?.info(
                    `[${url.href}] No new articles found, explicitly reusing ${dedupedDispatchIds.length} existing article ids for immediate forward (${existingArticleReusePolicy?.reason}).`,
                )
                return dedupedDispatchIds
            }
            if (dedupedPremiereIds.length > 0) {
                ctx.log?.info(
                    `[${url.href}] No new articles found, dispatching ${dedupedPremiereIds.length} premiere-resolved article ids for immediate forward.`,
                )
                return dedupedPremiereIds
            }
            ctx.log?.info(`[${url.href}] No new articles found.`)
            return []
        }
        /**
         * 非常耗时，如何解决
         */
        new_articles = await Promise.all(new_articles.map((article) => this.doProcess(ctx, article, processor)))

        // 串行，防止create unique的问题
        for (const article of new_articles) {
            /**
             * TODO 这里可以尝试更新翻译
             */
            const res = await DB.Article.trySave(article)
            if (res) {
                saved_articles_count += 1
            }
            const persisted = res || (await DB.Article.checkExist(article))
            if (persisted) {
                dispatch_article_ids.push(persisted.id)
                const ingestedLinks = await this.enqueueExternalMediaLinks(article, {
                    crawlerConfig: cfg_crawler,
                    log: ctx.log,
                    getScheduledWebsitesForCrawler: (crawlerName) =>
                        this.scheduledWebsitesByCrawler.get(crawlerName) || [],
                })
                const queuedExternalLinks =
                    (ingestedLinks?.tiktok?.length || 0) +
                    (ingestedLinks?.youtube?.length || 0) +
                    (ingestedLinks?.instagram?.length || 0) +
                    (ingestedLinks?.website?.length || 0)
                if (queuedExternalLinks > 0) {
                    // SpiderPools has no scheduler of its own; the runtime wires the
                    // scheduler's poke in at construction. A missing hook must never
                    // break the article-save loop.
                    this.onSchedulePoke?.().catch((error) =>
                        ctx.log?.warn(`Failed to poke schedules after external link ingest: ${error}`),
                    )
                }
            }
        }
        ctx.log?.info(`[${url.href}] ${saved_articles_count} articles saved.`)
        return Array.from(new Set([...premiere_dispatch_ids, ...dispatch_article_ids]))
    }

    private async maybeSyncInstagramLiveRelay(
        ctx: TaskScheduler.TaskCtx,
        url: URL,
        page?: Page,
        cookieString?: string,
        requestHeaders?: Record<string, string>,
    ) {
        if (!page) {
            return
        }

        const spiderPlugin = spiderRegistry.findByUrl(url.href)
        if (spiderPlugin?.platform !== Platform.Instagram) {
            return
        }

        const handle = spiderRegistry.extractBasicInfo(url.href)?.u_id
        if (!handle) {
            return
        }

        try {
            await this.instagramLiveRelay.syncProfile({
                handle,
                profileUrl: url.href,
                page,
                crawlerConfig: (ctx.task.data as Crawler).cfg_crawler,
                cookieString,
                requestHeaders,
                log: ctx.log,
            })
        } catch (error) {
            ctx.log?.warn(`[${url.href}] Instagram live relay sync failed: ${error}`)
        }
    }

    private async doProcess(ctx: TaskScheduler.TaskCtx, article: Article, processor?: BaseProcessor): Promise<Article> {
        if (!processor) {
            return article
        }
        const { username } = article
        ctx.log?.info(`[${username}] [${article.a_id}] Processing article...`)
        let currentArticle: Article | null = article
        /**
         * 先获取所有引用文章的指针，flat为数组，对数组进行await Promise.all操作
         * 再根据是否需要更新翻译进行更新
         */
        let articleNeedTobeProcessed: Array<Article> = []
        // 获取引用文章
        while (currentArticle && typeof currentArticle === 'object') {
            articleNeedTobeProcessed.push(currentArticle)
            if (typeof currentArticle.ref !== 'string') {
                currentArticle = currentArticle.ref as Article
            } else {
                currentArticle = null
            }
        }
        /**
         * 并行处理
         * 通过文章引用来修改对应文章的翻译/处理结果
         */
        ctx.log?.info(
            `[${username}] [${article.a_id}] Starting batch processing ${articleNeedTobeProcessed.length} articles...`,
        )
        await Promise.all(
            articleNeedTobeProcessed.map(async (currentArticle) => {
                const { a_id, username, platform } = currentArticle
                // maybe the ref article translated failed
                const article_maybe_processed = await DB.Article.getByArticleCode(a_id, platform)
                const processField = async (field: string, context: string) => {
                    try {
                        return await pRetry(() => processor.process(field), {
                            retries: RETRY_LIMIT,
                            onFailedAttempt: (error) => {
                                ctx.log?.warn(
                                    `[${username}] [${a_id}] Process ${context} failed, there are ${error.retriesLeft} retries left: ${error.originalError.message}`,
                                )
                            },
                        })
                    } catch (err) {
                        ctx.log?.error(`[${username}] [${a_id}] Error while processing ${context}: ${err}`)
                        // Never persist the sentinel: a failed request keeps the
                        // field untranslated so the next cycle retries it.
                        return null
                    }
                }
                if (currentArticle.content && !BaseProcessor.isValidResult(article_maybe_processed?.translation)) {
                    const content = currentArticle.content
                    ctx.log?.info(`[${username}] [${a_id}] Starting to process...`)
                    const content_processed = await processField(content, 'content')
                    if (content_processed !== null) {
                        ctx.log?.debug(`[${username}] [${a_id}] Process result: ${content_processed}`)
                        ctx.log?.info(`[${username}] [${a_id}] Process complete.`)
                        currentArticle.translation = content_processed
                        currentArticle.translated_by = processor.NAME
                    }
                }

                if (currentArticle.media) {
                    for (const [idx, media] of currentArticle.media.entries()) {
                        // 假设图片与描述的顺序是一致的
                        if (
                            media.alt &&
                            !BaseProcessor.isValidResult(
                                (article_maybe_processed?.media as unknown as Article['media'])?.[idx]?.translation,
                            )
                        ) {
                            const alt = media.alt
                            const caption_processed = await processField(alt, 'media alt')
                            if (caption_processed !== null) {
                                media.translation = caption_processed
                                media.translated_by = processor.NAME
                            }
                        }
                    }
                }

                if (currentArticle.extra) {
                    const extra_ref = currentArticle.extra
                    let { content, translation } = extra_ref
                    if (content && !BaseProcessor.isValidResult(translation)) {
                        const content_processed = await processField(content, 'extra content')
                        if (content_processed !== null) {
                            extra_ref.translation = content_processed
                            extra_ref.translated_by = processor.NAME
                        }
                    }
                }
            }),
        )
        ctx.log?.info(`[${username}] [${article.a_id}] ${articleNeedTobeProcessed.length} Articles are processed.`)
        return article
    }
}

export {
    SpiderTaskScheduler,
    SpiderPools,
    CrawlerCookieExportError,
    classifyCrawlError,
    shouldRetryCrawlErrorForPlatform,
}
