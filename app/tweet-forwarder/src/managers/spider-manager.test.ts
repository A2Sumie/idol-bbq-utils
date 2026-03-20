import { expect, test } from 'bun:test'
import EventEmitter from 'events'
import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import { SpiderPools } from './spider-manager'

test('SpiderPools reuses existing article ids for x list immediate forward', async () => {
    const originalCheckExist = DB.Article.checkExist
    const originalTrySave = DB.Article.trySave

    ;(DB.Article as any).checkExist = async (article: any) => {
        if (article.a_id === '2034851104853524704') {
            return { id: 1327 }
        }
        return undefined
    }
    ;(DB.Article as any).trySave = async () => undefined

    try {
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-pools', new EventEmitter())
        const result = await (pools as any).crawlArticle(
            {
                taskId: 'spider-test',
                task: {
                    id: 'spider-test',
                    status: 'running',
                    data: {
                        cfg_crawler: {
                            engine: 'api-unified',
                        },
                    },
                },
            },
            {
                crawl: async () =>
                    [
                        {
                            a_id: '2034851104853524704',
                            u_id: 'ru_ri0808',
                            username: '吉宮るり',
                            created_at: 1773981283,
                            url: 'https://x.com/ru_ri0808/status/2034851104853524704',
                            type: 'tweet',
                            has_media: true,
                            media: [],
                            platform: Platform.X,
                        },
                    ] as any,
            } as any,
            new URL('https://x.com/i/lists/1936785344072151389'),
        )

        expect(result).toEqual([1327])
    } finally {
        ;(DB.Article as any).checkExist = originalCheckExist
        ;(DB.Article as any).trySave = originalTrySave
    }
})

test('SpiderPools does not reuse existing article ids for non-list crawlers', async () => {
    const originalCheckExist = DB.Article.checkExist
    const originalTrySave = DB.Article.trySave

    ;(DB.Article as any).checkExist = async () => ({ id: 1327 })
    ;(DB.Article as any).trySave = async () => undefined

    try {
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-pools', new EventEmitter())
        const result = await (pools as any).crawlArticle(
            {
                taskId: 'spider-test',
                task: {
                    id: 'spider-test',
                    status: 'running',
                    data: {
                        cfg_crawler: {
                            engine: 'api-unified',
                        },
                    },
                },
            },
            {
                crawl: async () =>
                    [
                        {
                            a_id: '2034851104853524704',
                            u_id: 'ru_ri0808',
                            username: '吉宮るり',
                            created_at: 1773981283,
                            url: 'https://x.com/ru_ri0808/status/2034851104853524704',
                            type: 'tweet',
                            has_media: true,
                            media: [],
                            platform: Platform.X,
                        },
                    ] as any,
            } as any,
            new URL('https://x.com/ru_ri0808'),
        )

        expect(result).toEqual([])
    } finally {
        ;(DB.Article as any).checkExist = originalCheckExist
        ;(DB.Article as any).trySave = originalTrySave
    }
})
