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

type DBFollows = Prisma.crawler_followsGetPayload<{}>

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
                default:
                    throw new Error(`Unsupported platform: ${platform}`)
            }
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
        export async function add(type: string, payload: any, execute_at: number) {
            return await prisma.task_queue.create({
                data: {
                    type,
                    payload,
                    execute_at,
                    created_at: Math.floor(Date.now() / 1000),
                    status: 'pending'
                }
            })
        }

        export async function getPending(now: number) {
            return await prisma.task_queue.findMany({
                where: {
                    status: 'pending',
                    execute_at: {
                        lte: now
                    }
                }
            })
        }

        export async function updateStatus(id: number, status: string) {
            return await prisma.task_queue.update({
                where: { id },
                data: { status }
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
export type { Article, ArticleWithId, DBFollows }
