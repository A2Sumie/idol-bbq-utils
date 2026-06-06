import crypto from 'crypto'
import { Platform } from '@idol-bbq-utils/spider/types'
import type { ArticleExtractType, GenericArticle, GenericFollows, GenericMediaInfo } from '@idol-bbq-utils/spider/types'
import { prisma, Prisma } from './client'
import { getSubtractTime } from '@/utils/time'
import type { Article } from '@idol-bbq-utils/render/types'
import { normalizeWebsitePhotoArticles } from '@/utils/website-photo'
import {
    isOutboundFailedStatus,
    isOutboundStaleRetryableStatus,
    OUTBOUND_STATUS,
    hashValue,
} from '@/services/outbound-message-service'

type ArticleWithId = Article & { id: number }

// Union of all article payloads
type DBArticle =
    | Prisma.twitter_articleGetPayload<{}>
    | Prisma.instagram_articleGetPayload<{}>
    | Prisma.tiktok_articleGetPayload<{}>
    | Prisma.youtube_articleGetPayload<{}>
    | Prisma.website_articleGetPayload<{}>

type DBFollows = Prisma.crawler_followsGetPayload<{}>
type DBTaskQueue = Prisma.task_queueGetPayload<{}>
type DBProcessorRun = Prisma.processor_runsGetPayload<{}>
type DBOutboundMessage = Prisma.outbound_messagesGetPayload<{}>
type DBAggregationWindow = Prisma.aggregation_windowsGetPayload<{}>
type DBAggregationItem = Prisma.aggregation_itemsGetPayload<{}>
type DBTargetHealth = Prisma.target_healthGetPayload<{}>

interface TaskQueueListFilters {
    status?: string
    type?: string
    source_ref?: string
    action_type?: string
    idempotency_key?: string
}

interface ArticleQueryParams {
    platform?: Platform
    u_id?: string
    a_id?: string
    q?: string
    from?: number
    to?: number
    limit?: number
}

namespace DB {
    export namespace Article {
        function getDelegate(platform: Platform): any {
            switch (platform) {
                case Platform.X:
                case Platform.Twitter:
                    return prisma.twitter_article
                case Platform.Instagram:
                    return prisma.instagram_article
                case Platform.TikTok:
                    return prisma.tiktok_article
                case Platform.YouTube:
                    return prisma.youtube_article
                case Platform.Website:
                    return prisma.website_article
                default:
                    throw new Error(`Unsupported platform: ${platform}`)
            }
        }

        function getDelegates(platform?: Platform) {
            if (platform) {
                return [{ platform, delegate: getDelegate(platform) }]
            }
            return [
                { platform: Platform.X, delegate: getDelegate(Platform.X) },
                { platform: Platform.Instagram, delegate: getDelegate(Platform.Instagram) },
                { platform: Platform.TikTok, delegate: getDelegate(Platform.TikTok) },
                { platform: Platform.YouTube, delegate: getDelegate(Platform.YouTube) },
                { platform: Platform.Website, delegate: getDelegate(Platform.Website) },
            ]
        }

        export async function checkExist(article: Article) {
            const delegate = getDelegate(article.platform)
            return await delegate.findUnique({
                where: {
                    a_id: article.a_id,
                },
            })
        }

        export async function trySave(article: Article): Promise<DBArticle | undefined> {
            let exist_one = await checkExist(article)
            if (exist_one) {
                return
            }
            return await save(article)
        }

        export async function save(article: Article): Promise<DBArticle> {
            let exist_one = await checkExist(article)
            if (exist_one) {
                return exist_one
            }
            let ref: number | undefined = undefined
            // 递归注意
            if (article.ref) {
                if (typeof article.ref === 'object') {
                    ref = (await save(article.ref)).id
                }
                if (typeof article.ref === 'string') {
                    // For ref string, we assume it's same platform
                    ref = (await getByArticleCode(article.ref, article.platform))?.id
                }
            }

            const delegate = getDelegate(article.platform)
            // Remove platform from data as it's not in the model anymore (implied by table)
            // But we need to keep other keys.
            // The model expects: a_id, u_id, username, created_at, content...
            // It does NOT have 'platform' column.
            const { platform, ...rest } = article

            const res = await delegate.create({
                data: {
                    ...rest,
                    ref: ref,
                    extra: article.extra ? (article.extra as unknown as Prisma.JsonObject) : Prisma.JsonNull,
                    media: (article.media as unknown as Prisma.JsonArray) ?? Prisma.JsonNull,
                },
            })
            return res
        }

        export async function get(id: number, platform: Platform) {
            const delegate = getDelegate(platform)
            return await delegate.findUnique({
                where: {
                    id: id,
                },
            })
        }

        export async function getByArticleCode(a_id: string, platform: Platform) {
            const delegate = getDelegate(platform)
            return await delegate.findUnique({
                where: {
                    a_id,
                },
            })
        }

        export async function getSingleArticle(id: number, platform: Platform) {
            const delegate = getDelegate(platform)
            const article = await delegate.findUnique({
                where: {
                    id: id,
                },
            })
            if (!article) {
                return
            }
            return await getFullChainArticle(article, platform)
        }

        export async function getSingleArticleByArticleCode(a_id: string, platform: Platform) {
            const delegate = getDelegate(platform)
            const article = await delegate.findUnique({
                where: {
                    a_id,
                },
            })
            if (!article) {
                return
            }
            return await getFullChainArticle(article, platform)
        }

        export async function update(id: number, platform: Platform, patch: Partial<Article>) {
            const delegate = getDelegate(platform)
            const { platform: _platform, ref, media, extra, ...rest } = patch
            return await delegate.update({
                where: { id },
                data: {
                    ...rest,
                    ...(media !== undefined
                        ? {
                              media: (media as unknown as Prisma.JsonArray) ?? Prisma.JsonNull,
                          }
                        : {}),
                    ...(extra !== undefined
                        ? {
                              extra: (extra as unknown as Prisma.JsonObject) ?? Prisma.JsonNull,
                          }
                        : {}),
                    ...(ref === null ? { ref: null } : {}),
                },
            })
        }

        async function rootWithChain(article: DBArticle, platform: Platform) {
            const root = { ...article, platform } as unknown as ArticleWithId
            let current = root
            const delegate = getDelegate(platform)
            const visitedIds = new Set<number>([Number(article.id)])
            let depth = 0
            const maxDepth = 100

            while (current.ref && typeof current.ref === 'number') {
                const refId = current.ref as unknown as number
                // Corrupt imports can create cycles; truncate instead of blocking restore/query paths.
                if (visitedIds.has(refId) || depth >= maxDepth) {
                    current.ref = null
                    break
                }
                const found = await delegate.findUnique({ where: { id: refId } })
                if (found) {
                    visitedIds.add(Number(found.id))
                    depth += 1
                    const foundWithP = { ...found, platform } as unknown as ArticleWithId
                    current.ref = foundWithP
                    current = foundWithP
                } else {
                    break
                }
            }
            return root
        }

        async function getFullChainArticle(article: DBArticle, platform: Platform) {
            return rootWithChain(article, platform)
        }

        export async function getArticlesByName(u_id: string, platform: Platform, count = 10) {
            const delegate = getDelegate(platform)
            const res = await delegate.findMany({
                where: {
                    u_id: u_id,
                },
                orderBy: {
                    created_at: 'desc',
                },
                take: count,
            })
            const articles = await Promise.all(res.map(async ({ id }: any) => getSingleArticle(id, platform)))
            return normalizeWebsitePhotoArticles(articles.filter((item) => item) as ArticleWithId[])
        }

        // New method for time-range query (User Requirement 2)
        export async function getArticlesByTimeRange(u_id: string, platform: Platform, start: number, end: number) {
            const delegate = getDelegate(platform)
            const res = await delegate.findMany({
                where: {
                    u_id: u_id,
                    created_at: {
                        gte: start,
                        lte: end,
                    },
                },
                orderBy: {
                    created_at: 'asc', // Ascending for aggregation? Or Desc?
                },
            })
            // No need for full chain probably if just for summary, but safer to get it
            const articles = await Promise.all(res.map(async ({ id }: any) => getSingleArticle(id, platform)))
            return normalizeWebsitePhotoArticles(articles.filter((item) => item) as ArticleWithId[])
        }

        export async function query(params: ArticleQueryParams = {}) {
            const limit = Math.max(1, Math.min(params.limit || 50, 200))
            const results = [] as ArticleWithId[]

            for (const { platform, delegate } of getDelegates(params.platform)) {
                const where: Record<string, any> = {}
                if (params.u_id) {
                    where.u_id = params.u_id
                }
                if (params.a_id) {
                    where.a_id = params.a_id
                }
                if (params.from || params.to) {
                    where.created_at = {}
                    if (params.from) {
                        where.created_at.gte = params.from
                    }
                    if (params.to) {
                        where.created_at.lte = params.to
                    }
                }
                if (params.q) {
                    where.OR = [
                        { content: { contains: params.q } },
                        { translation: { contains: params.q } },
                        { username: { contains: params.q } },
                        { u_id: { contains: params.q } },
                        { url: { contains: params.q } },
                    ]
                }

                const rows = await delegate.findMany({
                    where,
                    orderBy: {
                        created_at: 'desc',
                    },
                    take: limit,
                })
                for (const row of rows) {
                    const article = await getSingleArticle(row.id, platform)
                    if (article) {
                        results.push(article)
                    }
                }
            }

            return normalizeWebsitePhotoArticles(results)
                .sort((a, b) => b.created_at - a.created_at)
                .slice(0, limit)
        }
    }

    export namespace Follow {
        export async function save(follows: GenericFollows) {
            return await prisma.crawler_follows.create({
                data: {
                    ...follows,
                    created_at: Math.floor(Date.now() / 1000),
                },
            })
        }

        export async function getLatestAndComparisonFollowsByName(
            u_id: string,
            platform: Platform,
            window: string,
        ): Promise<[DBFollows, DBFollows | null] | null> {
            const latest = await prisma.crawler_follows.findFirst({
                where: {
                    platform: platform,
                    u_id: u_id,
                },
                orderBy: {
                    created_at: 'desc',
                },
            })
            if (!latest) {
                return null
            }
            const latestTime = latest.created_at
            const subtractTime = getSubtractTime(latestTime, window)
            const comparison = await prisma.crawler_follows.findFirst({
                where: {
                    platform: platform,
                    u_id: u_id,
                    created_at: {
                        lte: subtractTime,
                    },
                },
                orderBy: {
                    created_at: 'desc',
                },
            })
            return [latest, comparison]
        }
    }

    export namespace ForwardBy {
        export async function checkExist(ref_id: number, platform: string | number, bot_id: string, task_type: string) {
            return await prisma.forward_by.findUnique({
                where: {
                    ref_id_platform_bot_id_task_type: {
                        ref_id,
                        platform: String(platform),
                        bot_id,
                        task_type,
                    },
                },
            })
        }

        export async function save(ref_id: number, platform: string | number, bot_id: string, task_type: string) {
            return await prisma.forward_by.upsert({
                where: {
                    ref_id_platform_bot_id_task_type: {
                        ref_id,
                        platform: String(platform),
                        bot_id,
                        task_type,
                    },
                },
                create: {
                    ref_id,
                    platform: String(platform),
                    bot_id,
                    task_type,
                },
                update: {},
            })
        }

        export async function claim(ref_id: number, platform: string | number, bot_id: string, task_type: string) {
            try {
                await prisma.forward_by.create({
                    data: {
                        ref_id,
                        platform: String(platform),
                        bot_id,
                        task_type,
                    },
                })
                return true
            } catch (error) {
                if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2002') {
                    return false
                }
                throw error
            }
        }

        export async function deleteRecord(
            ref_id: number,
            platform: string | number,
            bot_id: string,
            task_type: string,
        ) {
            let exist_one = await checkExist(ref_id, platform, bot_id, task_type)
            if (!exist_one) {
                return
            }
            return await prisma.forward_by.delete({
                where: {
                    ref_id_platform_bot_id_task_type: {
                        ref_id,
                        platform: String(platform),
                        bot_id,
                        task_type,
                    },
                },
            })
        }
    }

    export namespace TaskQueue {
        export const TYPE = {
            AggregateDaily: 'aggregate_daily',
            AggregateHourly: 'aggregate_hourly',
            ManualCrawlerRun: 'manual_crawler_run',
            NotificationSignal: 'notification_signal',
            ArticleSimulate: 'article_simulate',
            ArticleReprocess: 'article_reprocess',
            ArticleResend: 'article_resend',
            ProcessorRun: 'processor_run',
        } as const

        export type Type = (typeof TYPE)[keyof typeof TYPE]

        export const IDEMPOTENCY_FORMAT = {
            Stable: 'stable',
            LegacyJson: 'legacy_json',
        } as const

        export type IdempotencyFormat = (typeof IDEMPOTENCY_FORMAT)[keyof typeof IDEMPOTENCY_FORMAT]

        export const WORKER_TYPES = [TYPE.AggregateDaily, TYPE.AggregateHourly] as const
        export const INLINE_API_TYPES = [
            TYPE.ManualCrawlerRun,
            TYPE.NotificationSignal,
            TYPE.ArticleSimulate,
            TYPE.ArticleReprocess,
            TYPE.ArticleResend,
            TYPE.ProcessorRun,
        ] as const

        export const STATUS = {
            Pending: 'pending',
            Processing: 'processing',
            Completed: 'completed',
            Failed: 'failed',
            Cancelled: 'cancelled',
        } as const

        export type Status = (typeof STATUS)[keyof typeof STATUS]

        export const TERMINAL_STATUSES = new Set<string>([STATUS.Completed, STATUS.Failed, STATUS.Cancelled])

        export function buildIdempotencyKey(
            type: Type | string,
            payload: unknown,
            format: IdempotencyFormat = IDEMPOTENCY_FORMAT.Stable,
        ) {
            if (format === IDEMPOTENCY_FORMAT.LegacyJson) {
                return crypto.createHash('sha256').update(JSON.stringify({ type, payload })).digest('hex')
            }
            return hashValue({ type, payload })
        }

        export function shouldReviveExistingTaskOnAdd(existing: { status: string }) {
            return existing.status === STATUS.Failed
        }

        export function clampListLimit(limit = 50) {
            const normalized = Number.isFinite(limit) ? Math.trunc(limit) : 50
            return Math.max(1, Math.min(normalized, 200))
        }

        export function isTerminalStatus(status: string) {
            return TERMINAL_STATUSES.has(status)
        }

        export function normalizeListFilters(filters?: string | TaskQueueListFilters): TaskQueueListFilters {
            if (typeof filters === 'string') {
                return filters ? { status: filters } : {}
            }
            if (!filters) {
                return {}
            }
            return Object.fromEntries(
                Object.entries(filters)
                    .map(([key, value]) => [key, typeof value === 'string' ? value.trim() : value])
                    .filter(([, value]) => Boolean(value)),
            )
        }

        export function buildListWhere(filters?: string | TaskQueueListFilters) {
            const normalized = normalizeListFilters(filters)
            return Object.keys(normalized).length > 0 ? normalized : undefined
        }

        export function summarizeStatusCounts(rows: Array<{ status: string; _count: { _all: number } }>) {
            return Object.fromEntries(rows.map((row) => [row.status, row._count._all]))
        }

        export function buildInterruptedInlineFailureData(now: number) {
            return {
                status: STATUS.Failed,
                updated_at: now,
                finished_at: now,
                last_error: 'Inline API action was interrupted by runtime restart and cannot resume',
                result_summary: 'failed interrupted inline API action',
            }
        }

        export function buildRequeueFailedTaskData(
            payload: any,
            execute_at: number,
            now: number,
            meta?: {
                source_ref?: string
                action_type?: string
                idempotency_key?: string
            },
        ) {
            return {
                payload,
                execute_at,
                updated_at: now,
                status: STATUS.Pending,
                finished_at: null,
                last_error: null,
                result_summary: 'requeued failed idempotent task',
                source_ref: meta?.source_ref,
                action_type: meta?.action_type,
            }
        }

        export async function add(
            type: string,
            payload: any,
            execute_at: number,
            meta?: {
                source_ref?: string
                action_type?: string
                idempotency_key?: string
            },
        ) {
            const now = Math.floor(Date.now() / 1000)
            if (meta?.idempotency_key) {
                const existing = await prisma.task_queue.findUnique({
                    where: {
                        type_idempotency_key: {
                            type,
                            idempotency_key: meta.idempotency_key,
                        },
                    },
                })
                if (existing) {
                    if (shouldReviveExistingTaskOnAdd(existing)) {
                        return await prisma.task_queue.update({
                            where: { id: existing.id },
                            data: buildRequeueFailedTaskData(payload, execute_at, now, meta),
                        })
                    }
                    return existing
                }
            }
            try {
                return await prisma.task_queue.create({
                    data: {
                        type,
                        payload,
                        execute_at,
                        created_at: now,
                        updated_at: now,
                        status: STATUS.Pending,
                        source_ref: meta?.source_ref,
                        action_type: meta?.action_type,
                        idempotency_key: meta?.idempotency_key,
                    },
                })
            } catch (error) {
                if (
                    meta?.idempotency_key &&
                    error instanceof Prisma.PrismaClientKnownRequestError &&
                    error.code === 'P2002'
                ) {
                    const existingRecord = await prisma.task_queue.findUniqueOrThrow({
                        where: {
                            type_idempotency_key: {
                                type,
                                idempotency_key: meta.idempotency_key,
                            },
                        },
                    })
                    if (shouldReviveExistingTaskOnAdd(existingRecord)) {
                        return await prisma.task_queue.update({
                            where: { id: existingRecord.id },
                            data: buildRequeueFailedTaskData(payload, execute_at, now, meta),
                        })
                    }
                    return existingRecord
                }
                throw error
            }
        }

        export async function getPending(now: number, options?: { types?: Array<string> }) {
            return await prisma.task_queue.findMany({
                where: {
                    status: STATUS.Pending,
                    execute_at: {
                        lte: now,
                    },
                    ...(options?.types?.length ? { type: { in: options.types } } : {}),
                },
                orderBy: {
                    execute_at: 'asc',
                },
            })
        }

        export async function recoverStaleProcessing(
            now: number,
            staleAfterSeconds: number,
            options?: { types?: Array<string> },
        ) {
            return await prisma.task_queue.updateMany({
                where: {
                    status: STATUS.Processing,
                    updated_at: {
                        lte: now - staleAfterSeconds,
                    },
                    ...(options?.types?.length ? { type: { in: options.types } } : {}),
                },
                data: {
                    status: STATUS.Pending,
                    updated_at: now,
                    last_error: 'Recovered stale processing task after runtime interruption',
                },
            })
        }

        export async function failInterruptedInlineProcessing(now = Math.floor(Date.now() / 1000)) {
            return await prisma.task_queue.updateMany({
                where: {
                    status: STATUS.Processing,
                    type: {
                        in: [...INLINE_API_TYPES],
                    },
                },
                data: buildInterruptedInlineFailureData(now),
            })
        }

        export async function claimPending(id: number) {
            const now = Math.floor(Date.now() / 1000)
            const updated = await prisma.task_queue.updateMany({
                where: {
                    id,
                    status: STATUS.Pending,
                },
                data: {
                    status: STATUS.Processing,
                    updated_at: now,
                    finished_at: null,
                    last_error: null,
                },
            })
            if (updated.count === 0) {
                return null
            }
            return await prisma.task_queue.findUnique({
                where: { id },
            })
        }

        export async function updateStatus(
            id: number,
            status: Status,
            meta?: { last_error?: string | null; result_summary?: string | null },
        ) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.task_queue.update({
                where: { id },
                data: {
                    status,
                    updated_at: now,
                    finished_at: isTerminalStatus(status) ? now : null,
                    last_error: meta?.last_error ?? undefined,
                    result_summary: meta?.result_summary ?? undefined,
                },
            })
        }

        export async function retryLater(
            id: number,
            execute_at: number,
            meta?: { last_error?: string | null; result_summary?: string | null },
        ) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.task_queue.update({
                where: { id },
                data: {
                    status: STATUS.Pending,
                    execute_at,
                    updated_at: now,
                    finished_at: null,
                    last_error: meta?.last_error ?? undefined,
                    result_summary: meta?.result_summary ?? undefined,
                },
            })
        }

        export async function list(
            limit = 50,
            filters?: string | TaskQueueListFilters,
        ): Promise<Array<DBTaskQueue>> {
            return await prisma.task_queue.findMany({
                where: buildListWhere(filters),
                orderBy: {
                    created_at: 'desc',
                },
                take: clampListLimit(limit),
            })
        }

        export async function countsByStatus(): Promise<Record<string, number>> {
            const rows = await prisma.task_queue.groupBy({
                by: ['status'],
                _count: {
                    _all: true,
                },
            })
            return summarizeStatusCounts(rows)
        }
    }

    export namespace ProcessorRun {
        export const STATUS = {
            Completed: 'completed',
            Failed: 'failed',
        } as const

        export type Status = (typeof STATUS)[keyof typeof STATUS]

        export async function create(data: {
            processor_id?: string | null
            action: string
            source_type?: string | null
            source_ref?: string | null
            status?: Status
            input?: any
            output?: any
            error?: string | null
        }) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.processor_runs.create({
                data: {
                    processor_id: data.processor_id || null,
                    action: data.action,
                    source_type: data.source_type || null,
                    source_ref: data.source_ref || null,
                    status: data.status || STATUS.Completed,
                    input: (data.input as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                    output: (data.output as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                    error: data.error || null,
                    created_at: now,
                    finished_at: now,
                },
            })
        }

        export async function list(limit = 50, source_ref?: string): Promise<Array<DBProcessorRun>> {
            return await prisma.processor_runs.findMany({
                where: source_ref ? { source_ref } : undefined,
                orderBy: {
                    created_at: 'desc',
                },
                take: Math.max(1, Math.min(limit, 200)),
            })
        }
    }

    export namespace MediaHash {
        export async function checkExist(platform: string, hash: string) {
            return await prisma.media_hashes.findUnique({
                where: {
                    platform_hash: {
                        platform,
                        hash,
                    },
                },
            })
        }

        export async function save(platform: string, hash: string, a_id: string = '') {
            return await prisma.media_hashes.upsert({
                where: {
                    platform_hash: {
                        platform,
                        hash,
                    },
                },
                create: {
                    platform,
                    hash,
                    a_id,
                    created_at: Math.floor(Date.now() / 1000),
                },
                update: {}, // No op if exists
            })
        }

        export async function claimVisibleSlot(options: {
            namespace: string
            hash: string
            a_id?: string
            maxVisible: number
            windowSeconds: number
            now?: number
        }): Promise<{ allowed: boolean; seenCount: number; slot?: number }> {
            const maxVisible = Math.max(1, Math.floor(Number(options.maxVisible || 1)))
            const windowSeconds = Math.max(1, Math.floor(Number(options.windowSeconds || 1)))
            const now = Math.floor(Number(options.now || Date.now() / 1000))
            const cutoff = now - windowSeconds
            const slots = Array.from({ length: maxVisible }, (_, index) => `${options.namespace}:slot:${index}`)
            const existing = await prisma.media_hashes.findMany({
                where: {
                    hash: options.hash,
                    platform: {
                        in: slots,
                    },
                },
            })
            const byPlatform = new Map(existing.map((record) => [record.platform, record]))
            let activeCount = 0
            let inactiveSlot: number | undefined

            for (const [index, platform] of slots.entries()) {
                const record = byPlatform.get(platform)
                if (record && record.created_at >= cutoff) {
                    activeCount += 1
                    continue
                }
                inactiveSlot ??= index
            }

            if (activeCount >= maxVisible || inactiveSlot === undefined) {
                return {
                    allowed: false,
                    seenCount: activeCount,
                }
            }

            const platform = slots[inactiveSlot]!
            await prisma.media_hashes.upsert({
                where: {
                    platform_hash: {
                        platform,
                        hash: options.hash,
                    },
                },
                create: {
                    platform,
                    hash: options.hash,
                    a_id: options.a_id || '',
                    created_at: now,
                },
                update: {
                    a_id: options.a_id || '',
                    created_at: now,
                },
            })

            return {
                allowed: true,
                seenCount: activeCount + 1,
                slot: inactiveSlot,
            }
        }

        export async function releaseVisibleSlots(options: {
            claims: Array<{
                platform: string
                hash: string
                a_id?: string
            }>
        }): Promise<number> {
            let released = 0
            for (const claim of options.claims) {
                const result = await prisma.media_hashes.deleteMany({
                    where: {
                        platform: claim.platform,
                        hash: claim.hash,
                        ...(claim.a_id ? { a_id: claim.a_id } : {}),
                    },
                })
                released += result.count
            }
            return released
        }
    }

    export namespace OutboundMessage {
        const FAILED_RETRY_LIMIT = 5
        const FAILED_RETRY_BASE_SECONDS = 60
        const FAILED_RETRY_MAX_SECONDS = 3600

        function failedBackoffSeconds(attemptCount: number) {
            const exponent = Math.max(0, Math.min(attemptCount, 6))
            return Math.min(FAILED_RETRY_MAX_SECONDS, FAILED_RETRY_BASE_SECONDS * 2 ** exponent)
        }

        function hasSuppressedPayloadDrift(
            existing: DBOutboundMessage,
            data: {
                route_key: string
                target_id: string
                target_platform?: string | null
                task_kind: string
                article_key?: string | null
                synthetic_key?: string | null
                payload_hash: string
            },
        ) {
            return (
                existing.route_key !== data.route_key ||
                existing.target_id !== data.target_id ||
                (existing.target_platform || null) !== (data.target_platform || null) ||
                existing.task_kind !== data.task_kind ||
                (existing.article_key || null) !== (data.article_key || null) ||
                (existing.synthetic_key || null) !== (data.synthetic_key || null) ||
                existing.payload_hash !== data.payload_hash
            )
        }

        function preservedSegmentResultsForDrift(segmentResults: unknown) {
            if (
                segmentResults &&
                typeof segmentResults === 'object' &&
                !Array.isArray(segmentResults) &&
                (segmentResults as Record<string, unknown>).diagnostic === 'suppressed_payload_drift'
            ) {
                return (segmentResults as Record<string, unknown>).previous_segment_results ?? null
            }
            return segmentResults ?? null
        }

        async function recordSuppressedPayloadDrift(
            existing: DBOutboundMessage,
            data: {
                route_key: string
                target_id: string
                target_platform?: string | null
                task_kind: string
                article_key?: string | null
                synthetic_key?: string | null
                payload_hash: string
            },
            now: number,
        ) {
            if (!hasSuppressedPayloadDrift(existing, data)) {
                return existing
            }
            const previousSegmentResults = preservedSegmentResultsForDrift(existing.segment_results)
            const diagnostic = {
                diagnostic: 'suppressed_payload_drift',
                observed_at: now,
                existing: {
                    route_key: existing.route_key,
                    target_id: existing.target_id,
                    target_platform: existing.target_platform || null,
                    task_kind: existing.task_kind,
                    article_key: existing.article_key || null,
                    synthetic_key: existing.synthetic_key || null,
                    payload_hash: existing.payload_hash,
                    status: existing.status,
                },
                incoming: {
                    route_key: data.route_key,
                    target_id: data.target_id,
                    target_platform: data.target_platform || null,
                    task_kind: data.task_kind,
                    article_key: data.article_key || null,
                    synthetic_key: data.synthetic_key || null,
                    payload_hash: data.payload_hash,
                },
                ...(previousSegmentResults === null
                    ? {}
                    : { previous_segment_results: previousSegmentResults as Prisma.InputJsonValue }),
            }
            return await prisma.outbound_messages.update({
                where: { idempotency_key: existing.idempotency_key },
                data: {
                    segment_results: diagnostic as Prisma.InputJsonValue,
                },
            })
        }

        export async function claim(
            data: {
                idempotency_key: string
                route_key: string
                target_id: string
                target_platform?: string | null
                task_kind: string
                article_key?: string | null
                synthetic_key?: string | null
                payload_hash: string
            },
            staleAfterSeconds = 30 * 60,
        ): Promise<{ claimed: boolean; record: DBOutboundMessage }> {
            const now = Math.floor(Date.now() / 1000)
            const existing = await prisma.outbound_messages.findUnique({
                where: { idempotency_key: data.idempotency_key },
            })
            if (existing) {
                const stale = existing.updated_at <= now - staleAfterSeconds
                const failedAttempts = existing.attempt_count || 0
                const failedRetryable =
                    isOutboundFailedStatus(existing.status) &&
                    failedAttempts < FAILED_RETRY_LIMIT &&
                    existing.updated_at <= now - failedBackoffSeconds(failedAttempts)
                const dryRunRetryable = existing.status === OUTBOUND_STATUS.DryRun
                const retryable =
                    dryRunRetryable || failedRetryable || (isOutboundStaleRetryableStatus(existing.status) && stale)
                if (!retryable) {
                    return { claimed: false, record: await recordSuppressedPayloadDrift(existing, data, now) }
                }
                const record = await prisma.outbound_messages.update({
                    where: { idempotency_key: data.idempotency_key },
                    data: {
                        route_key: data.route_key,
                        target_id: data.target_id,
                        target_platform: data.target_platform || null,
                        task_kind: data.task_kind,
                        article_key: data.article_key || null,
                        synthetic_key: data.synthetic_key || null,
                        payload_hash: data.payload_hash,
                        status: OUTBOUND_STATUS.Planned,
                        provider_message_ids: Prisma.JsonNull,
                        segment_results: Prisma.JsonNull,
                        last_error: null,
                        updated_at: now,
                        finished_at: null,
                    },
                })
                return { claimed: true, record }
            }

            try {
                const record = await prisma.outbound_messages.create({
                    data: {
                        ...data,
                        target_platform: data.target_platform || null,
                        article_key: data.article_key || null,
                        synthetic_key: data.synthetic_key || null,
                        status: OUTBOUND_STATUS.Planned,
                        created_at: now,
                        updated_at: now,
                    },
                })
                return { claimed: true, record }
            } catch (error) {
                if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2002') {
                    const existingRecord = await prisma.outbound_messages.findUniqueOrThrow({
                        where: { idempotency_key: data.idempotency_key },
                    })
                    const record = await recordSuppressedPayloadDrift(existingRecord, data, now)
                    return { claimed: false, record }
                }
                throw error
            }
        }

        export async function markSending(idempotency_key: string) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.outbound_messages.update({
                where: { idempotency_key },
                data: {
                    status: OUTBOUND_STATUS.Sending,
                    attempt_count: { increment: 1 },
                    provider_message_ids: Prisma.JsonNull,
                    segment_results: Prisma.JsonNull,
                    updated_at: now,
                    finished_at: null,
                    last_error: null,
                },
            })
        }

        export async function markQueued(idempotency_key: string, details?: unknown) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.outbound_messages.update({
                where: { idempotency_key },
                data: {
                    status: OUTBOUND_STATUS.Queued,
                    provider_message_ids: (details as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                    segment_results: Prisma.JsonNull,
                    updated_at: now,
                    finished_at: null,
                    last_error: null,
                },
            })
        }

        export async function markDryRun(idempotency_key: string, details?: unknown) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.outbound_messages.update({
                where: { idempotency_key },
                data: {
                    status: OUTBOUND_STATUS.DryRun,
                    provider_message_ids: (details as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                    segment_results: Prisma.JsonNull,
                    updated_at: now,
                    finished_at: null,
                    last_error: null,
                },
            })
        }

        export async function markSkipped(idempotency_key: string, reason: string, details?: unknown) {
            const now = Math.floor(Date.now() / 1000)
            const payload =
                details === undefined
                    ? { reason }
                    : {
                          reason,
                          details,
                      }
            return await prisma.outbound_messages.update({
                where: { idempotency_key },
                data: {
                    status: OUTBOUND_STATUS.Skipped,
                    provider_message_ids: payload as Prisma.InputJsonValue,
                    segment_results: Prisma.JsonNull,
                    updated_at: now,
                    finished_at: now,
                    last_error: null,
                },
            })
        }

        export async function markSent(idempotency_key: string, providerResult?: unknown) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.outbound_messages.update({
                where: { idempotency_key },
                data: {
                    status: OUTBOUND_STATUS.Sent,
                    provider_message_ids: (providerResult as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                    segment_results: Prisma.JsonNull,
                    updated_at: now,
                    finished_at: now,
                    last_error: null,
                },
            })
        }

        export async function markPartial(idempotency_key: string, providerResult: unknown, error: unknown) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.outbound_messages.update({
                where: { idempotency_key },
                data: {
                    status: OUTBOUND_STATUS.Partial,
                    provider_message_ids: Prisma.JsonNull,
                    segment_results: (providerResult as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                    last_error: error instanceof Error ? error.message : String(error),
                    updated_at: now,
                    finished_at: now,
                },
            })
        }

        export async function markFailed(idempotency_key: string, error: unknown) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.outbound_messages.update({
                where: { idempotency_key },
                data: {
                    status: OUTBOUND_STATUS.Failed,
                    provider_message_ids: Prisma.JsonNull,
                    segment_results: Prisma.JsonNull,
                    last_error: error instanceof Error ? error.message : String(error),
                    updated_at: now,
                    finished_at: now,
                },
            })
        }

        export async function list(limit = 50, status?: string): Promise<Array<DBOutboundMessage>> {
            return await prisma.outbound_messages.findMany({
                where: status ? { status } : undefined,
                orderBy: { updated_at: 'desc' },
                take: Math.max(1, Math.min(limit, 200)),
            })
        }

        export async function findLatestVisibleCompletion(options: {
            route_key: string
            target_id: string
            task_kinds?: Array<string>
        }): Promise<DBOutboundMessage | null> {
            return await prisma.outbound_messages.findFirst({
                where: {
                    route_key: options.route_key,
                    target_id: options.target_id,
                    task_kind:
                        options.task_kinds && options.task_kinds.length > 0
                            ? {
                                  in: options.task_kinds,
                              }
                            : undefined,
                    status: {
                        in: [
                            OUTBOUND_STATUS.Sent,
                            OUTBOUND_STATUS.Partial,
                            OUTBOUND_STATUS.FailedAfterPartial,
                        ],
                    },
                },
                orderBy: [{ finished_at: 'desc' }, { updated_at: 'desc' }, { created_at: 'desc' }],
            })
        }
    }

    export namespace AggregationWindow {
        export const STATUS = {
            Open: 'open',
            Sent: 'sent',
            Completed: 'completed',
            Failed: 'failed',
            Cancelled: 'cancelled',
        } as const

        export type Status = (typeof STATUS)[keyof typeof STATUS]

        export const TERMINAL_STATUSES = new Set<string>([
            STATUS.Sent,
            STATUS.Completed,
            STATUS.Failed,
            STATUS.Cancelled,
        ])

        export function isTerminalStatus(status: string) {
            return TERMINAL_STATUSES.has(status)
        }

        export async function getOpen(route_key: string, target_id: string, mode: string) {
            return await prisma.aggregation_windows.findFirst({
                where: {
                    route_key,
                    target_id,
                    mode,
                    status: STATUS.Open,
                },
                orderBy: { created_at: 'asc' },
            })
        }

        export async function getOrCreateOpen(data: {
            idempotency_key: string
            route_key: string
            target_id: string
            mode: string
            window_start: number
            window_end: number
        }): Promise<DBAggregationWindow> {
            const existing = await prisma.aggregation_windows.findUnique({
                where: { idempotency_key: data.idempotency_key },
            })
            if (existing) {
                return existing
            }
            const now = Math.floor(Date.now() / 1000)
            try {
                return await prisma.aggregation_windows.create({
                    data: {
                        ...data,
                        status: STATUS.Open,
                        created_at: now,
                        updated_at: now,
                    },
                })
            } catch (error) {
                if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2002') {
                    return await prisma.aggregation_windows.findUniqueOrThrow({
                        where: { idempotency_key: data.idempotency_key },
                    })
                }
                throw error
            }
        }

        export async function listOpen(mode?: string): Promise<Array<DBAggregationWindow>> {
            return await prisma.aggregation_windows.findMany({
                where: {
                    status: STATUS.Open,
                    ...(mode ? { mode } : {}),
                },
                orderBy: { created_at: 'asc' },
            })
        }

        export async function updateStatus(id: number, status: Status, meta?: { payload_hash?: string | null }) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.aggregation_windows.update({
                where: { id },
                data: {
                    status,
                    payload_hash: meta?.payload_hash ?? undefined,
                    updated_at: now,
                    finished_at: isTerminalStatus(status) ? now : null,
                },
            })
        }

        export async function upsertItem(data: {
            window_id: number
            article_key: string
            article_row_id: number
            platform: string | number
            payload?: unknown
        }): Promise<DBAggregationItem> {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.aggregation_items.upsert({
                where: {
                    window_id_article_key: {
                        window_id: data.window_id,
                        article_key: data.article_key,
                    },
                },
                create: {
                    window_id: data.window_id,
                    article_key: data.article_key,
                    article_row_id: data.article_row_id,
                    platform: String(data.platform),
                    payload: (data.payload as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                    created_at: now,
                },
                update: {
                    article_row_id: data.article_row_id,
                    platform: String(data.platform),
                    payload: (data.payload as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                },
            })
        }

        export async function listItems(window_id: number): Promise<Array<DBAggregationItem>> {
            return await prisma.aggregation_items.findMany({
                where: { window_id },
                orderBy: { created_at: 'asc' },
            })
        }
    }

    export namespace TargetHealth {
        export async function mark(data: {
            target_id: string
            provider: string
            status: string
            last_send_status?: string | null
            last_provider_code?: string | null
            disabled_reason?: string | null
            details?: unknown
        }): Promise<DBTargetHealth> {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.target_health.upsert({
                where: { target_id: data.target_id },
                create: {
                    target_id: data.target_id,
                    provider: data.provider,
                    status: data.status,
                    last_send_status: data.last_send_status || null,
                    last_provider_code: data.last_provider_code || null,
                    disabled_reason: data.disabled_reason || null,
                    details: (data.details as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                    checked_at: now,
                    updated_at: now,
                },
                update: {
                    provider: data.provider,
                    status: data.status,
                    last_send_status: data.last_send_status || null,
                    last_provider_code: data.last_provider_code || null,
                    disabled_reason: data.disabled_reason || null,
                    details: (data.details as Prisma.InputJsonValue | undefined) ?? Prisma.JsonNull,
                    checked_at: now,
                    updated_at: now,
                },
            })
        }

        export async function list(): Promise<Array<DBTargetHealth>> {
            return await prisma.target_health.findMany({
                orderBy: { updated_at: 'desc' },
            })
        }
    }
}

export default DB
export type {
    Article,
    ArticleWithId,
    DBAggregationItem,
    DBAggregationWindow,
    DBFollows,
    DBOutboundMessage,
    DBProcessorRun,
    DBTargetHealth,
    DBTaskQueue,
}
