import { expect, test } from 'bun:test'
import EventEmitter from 'events'
import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import { TaskScheduler } from '@/utils/base'
import { SpiderPools, SpiderTaskScheduler } from './spider-manager'

test('SpiderTaskScheduler treats same crawler pending or running tasks as active until completion', () => {
    const scheduler = new SpiderTaskScheduler({ crawlers: [] }, new EventEmitter())

    ;(scheduler as any).tasks.set('running-task', {
        id: 'running-task',
        status: TaskScheduler.TaskStatus.RUNNING,
        data: { name: 'Instagram Live 抢抓 - 椎名桜月' },
    })
    expect((scheduler as any).hasActiveCrawlerTask('Instagram Live 抢抓 - 椎名桜月')).toBe(true)
    expect((scheduler as any).hasActiveCrawlerTask('22/7-cast-成员统一列表')).toBe(false)

    scheduler.updateTaskStatus({
        taskId: 'running-task',
        status: TaskScheduler.TaskStatus.COMPLETED,
    })
    expect((scheduler as any).hasActiveCrawlerTask('Instagram Live 抢抓 - 椎名桜月')).toBe(false)
})

test('SpiderPools marks linked manual crawler task completed after crawl handling finishes', async () => {
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const statusUpdates: any[] = []
    const finishedEvents: any[] = []

    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }

    try {
        const emitter = new EventEmitter()
        emitter.on(`spider:${TaskScheduler.TaskEvent.FINISHED}`, (payload) => finishedEvents.push(payload))
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-pools', emitter)

        await (pools as any).onTaskReceived({
            taskId: 'manual-ok',
            task: {
                id: 'manual-ok',
                status: TaskScheduler.TaskStatus.PENDING,
                data: {
                    name: 'crawler-ok',
                    websites: ['https://unsupported.invalid/path'],
                },
                meta: {
                    task_queue_id: 123,
                },
            },
        })

        expect(statusUpdates).toEqual([
            {
                id: 123,
                status: DB.TaskQueue.STATUS.Processing,
                meta: {
                    result_summary: 'crawler crawler-ok running',
                },
            },
            {
                id: 123,
                status: DB.TaskQueue.STATUS.Completed,
                meta: {
                    result_summary: 'crawler crawler-ok completed: 0 article(s), 0 follow(s)',
                },
            },
        ])
        expect(finishedEvents).toEqual([
            {
                taskId: 'manual-ok',
                result: [],
                immediate_notify: undefined,
                crawlerName: 'crawler-ok',
            },
        ])
    } finally {
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
    }
})

test('SpiderPools marks linked manual crawler task cancelled when targets are missing', async () => {
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const statusUpdates: any[] = []

    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }

    try {
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-pools', new EventEmitter())

        await (pools as any).onTaskReceived({
            taskId: 'manual-cancel',
            task: {
                id: 'manual-cancel',
                status: TaskScheduler.TaskStatus.PENDING,
                data: {
                    name: 'crawler-cancel',
                },
                meta: {
                    task_queue_id: 124,
                },
            },
        })

        expect(statusUpdates).toEqual([
            {
                id: 124,
                status: DB.TaskQueue.STATUS.Processing,
                meta: {
                    result_summary: 'crawler crawler-cancel running',
                },
            },
            {
                id: 124,
                status: DB.TaskQueue.STATUS.Cancelled,
                meta: {
                    last_error: 'No websites or origin or paths found',
                    result_summary: 'crawler crawler-cancel cancelled: no crawl targets',
                },
            },
        ])
    } finally {
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
    }
})

test('SpiderPools marks linked manual crawler task failed when crawl target handling errors', async () => {
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const statusUpdates: any[] = []

    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }

    try {
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-pools', new EventEmitter())

        await (pools as any).onTaskReceived({
            taskId: 'manual-fail',
            task: {
                id: 'manual-fail',
                status: TaskScheduler.TaskStatus.PENDING,
                data: {
                    name: 'crawler-fail',
                    websites: ['not-a-url'],
                },
                meta: {
                    task_queue_id: 125,
                },
            },
        })

        expect(statusUpdates[0]).toEqual({
            id: 125,
            status: DB.TaskQueue.STATUS.Processing,
            meta: {
                result_summary: 'crawler crawler-fail running',
            },
        })
        expect(statusUpdates[1]).toMatchObject({
            id: 125,
            status: DB.TaskQueue.STATUS.Failed,
            meta: {
                result_summary: 'crawler crawler-fail failed: 1 error(s)',
            },
        })
        expect(String(statusUpdates[1]?.meta?.last_error || '').length).toBeGreaterThan(0)
    } finally {
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
    }
})

test('SpiderPools does not reuse existing article ids for x list immediate forward by default', async () => {
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

        expect(result).toEqual([])
    } finally {
        ;(DB.Article as any).checkExist = originalCheckExist
        ;(DB.Article as any).trySave = originalTrySave
    }
})

test('SpiderPools only reuses existing x list article ids when explicitly enabled and recent', async () => {
    const originalCheckExist = DB.Article.checkExist
    const originalTrySave = DB.Article.trySave
    const now = Math.floor(Date.now() / 1000)

    ;(DB.Article as any).checkExist = async (article: any) => {
        if (article.a_id === '2034851104853524704') {
            return { id: 1327, created_at: now - 60 }
        }
        if (article.a_id === '2034851104853524705') {
            return { id: 1328, created_at: now - 3600 }
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
                            reuse_existing_for_immediate_forward: {
                                enabled: true,
                                max_age_seconds: 300,
                                max_items: 1,
                                reason: 'test backfill',
                            },
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
                            created_at: now - 60,
                            url: 'https://x.com/ru_ri0808/status/2034851104853524704',
                            type: 'tweet',
                            has_media: true,
                            media: [],
                            platform: Platform.X,
                        },
                        {
                            a_id: '2034851104853524705',
                            u_id: 'ru_ri0808',
                            username: '吉宮るり',
                            created_at: now - 3600,
                            url: 'https://x.com/ru_ri0808/status/2034851104853524705',
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

test('SpiderPools reuses existing article ids for configured non-list crawlers', async () => {
    const originalCheckExist = DB.Article.checkExist
    const originalTrySave = DB.Article.trySave
    const now = Math.floor(Date.now() / 1000)

    ;(DB.Article as any).checkExist = async (article: any) => {
        if (article.a_id === 'COLLABPOST') {
            return { id: 227, created_at: now - 90 }
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
                            reuse_existing_for_immediate_forward: {
                                enabled: true,
                                max_age_seconds: 300,
                                max_items: 2,
                                reason: 'collaboration route fanout',
                            },
                        },
                    },
                },
            },
            {
                crawl: async () =>
                    [
                        {
                            a_id: 'COLLABPOST',
                            u_id: 'em_matcha227',
                            username: '望月りの',
                            created_at: now - 90,
                            url: 'https://www.instagram.com/p/COLLABPOST/',
                            type: 'post',
                            has_media: true,
                            media: [],
                            platform: Platform.Instagram,
                        },
                    ] as any,
            } as any,
            new URL('https://www.instagram.com/shiina_satsuki227'),
        )

        expect(result).toEqual([227])
    } finally {
        ;(DB.Article as any).checkExist = originalCheckExist
        ;(DB.Article as any).trySave = originalTrySave
    }
})
