import { Platform } from '@idol-bbq-utils/spider/types'
import type { ArticleExtractType, GenericArticle, GenericFollows, GenericMediaInfo } from '@idol-bbq-utils/spider/types'
import { prisma, Prisma } from './client'
import { getSubtractTime } from '@/utils/time'
import type { Article } from '@idol-bbq-utils/render/types'

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

        async function getFullChainArticle(article: DBArticle, platform: Platform) {
            let currentRefId = article.ref
            let currentArticle = { ...article, platform } as unknown as ArticleWithId
            const delegate = getDelegate(platform)

            while (currentRefId) {
                const foundArticle = await delegate.findUnique({
                    where: {
                        id: currentRefId,
                    },
                })
                currentRefId = foundArticle?.ref || null
                // We assume ref is also same platform
                if (foundArticle) {
                    const foundWithPlatform = { ...foundArticle, platform } as unknown as ArticleWithId
                    currentArticle.ref = foundWithPlatform
                    currentArticle = foundWithPlatform
                }
            }
            return rootWithChain(article, platform)
        }

        async function rootWithChain(article: DBArticle, platform: Platform) {
            const root = { ...article, platform } as unknown as ArticleWithId
            let current = root
            const delegate = getDelegate(platform)

            while (current.ref && typeof current.ref === 'number') {
                // The ref field in DBArticle is generic Int?
                // In transformed ArticleWithId, ref can be object.
                // But initially it's just ID from DB.
                // Wait, `article.ref` (DB) is number.
                // `current.ref` (ArticleWithId) is generic.
                // We need to look at `current.ref` as ID.
                const refId = current.ref as unknown as number
                const found = await delegate.findUnique({ where: { id: refId } })
                if (found) {
                    const foundWithP = { ...found, platform } as unknown as ArticleWithId
                    current.ref = foundWithP
                    current = foundWithP
                } else {
                    break
                }
            }
            return root
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
            return articles.filter((item) => item) as ArticleWithId[]
        }

        // New method for time-range query (User Requirement 2)
        export async function getArticlesByTimeRange(u_id: string, platform: Platform, start: number, end: number) {
            const delegate = getDelegate(platform)
            const res = await delegate.findMany({
                where: {
                    u_id: u_id,
                    created_at: {
                        gte: start,
                        lte: end
                    }
                },
                orderBy: {
                    created_at: 'asc', // Ascending for aggregation? Or Desc?
                }
            })
            // No need for full chain probably if just for summary, but safer to get it
            const articles = await Promise.all(res.map(async ({ id }: any) => getSingleArticle(id, platform)))
            return articles.filter((item) => item) as ArticleWithId[]
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

            return results.sort((a, b) => b.created_at - a.created_at).slice(0, limit)
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

        export async function deleteRecord(ref_id: number, platform: string | number, bot_id: string, task_type: string) {
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
        export async function add(
            type: string,
            payload: any,
            execute_at: number,
            meta?: {
                source_ref?: string
                action_type?: string
            },
        ) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.task_queue.create({
                data: {
                    type,
                    payload,
                    execute_at,
                    created_at: now,
                    updated_at: now,
                    status: 'pending',
                    source_ref: meta?.source_ref,
                    action_type: meta?.action_type,
                },
            })
        }

        export async function getPending(now: number) {
            return await prisma.task_queue.findMany({
                where: {
                    status: 'pending',
                    execute_at: {
                        lte: now
                    },
                },
                orderBy: {
                    execute_at: 'asc',
                },
            })
        }

        export async function updateStatus(
            id: number,
            status: string,
            meta?: { last_error?: string | null; result_summary?: string | null },
        ) {
            const now = Math.floor(Date.now() / 1000)
            return await prisma.task_queue.update({
                where: { id },
                data: {
                    status,
                    updated_at: now,
                    finished_at: ['completed', 'failed', 'cancelled'].includes(status) ? now : null,
                    last_error: meta?.last_error ?? undefined,
                    result_summary: meta?.result_summary ?? undefined,
                },
            })
        }

        export async function list(limit = 50, status?: string): Promise<Array<DBTaskQueue>> {
            return await prisma.task_queue.findMany({
                where: status ? { status } : undefined,
                orderBy: {
                    created_at: 'desc',
                },
                take: Math.max(1, Math.min(limit, 200)),
            })
        }
    }

    export namespace ProcessorRun {
        export async function create(data: {
            processor_id?: string | null
            action: string
            source_type?: string | null
            source_ref?: string | null
            status?: string
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
                    status: data.status || 'completed',
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
                        hash
                    }
                }
            })
        }

        export async function save(platform: string, hash: string, a_id: string = '') {
            return await prisma.media_hashes.upsert({
                where: {
                    platform_hash: {
                        platform,
                        hash
                    }
                },
                create: {
                    platform,
                    hash,
                    a_id,
                    created_at: Math.floor(Date.now() / 1000)
                },
                update: {} // No op if exists
            })
        }
    }
}

export default DB
export type { Article, ArticleWithId, DBFollows, DBProcessorRun, DBTaskQueue }
