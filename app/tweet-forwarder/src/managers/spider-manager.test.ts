import { expect, test } from 'bun:test'
import EventEmitter from 'events'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import { ProcessorProvider } from '@/types/processor'
import { TaskScheduler } from '@/utils/base'
import {
    CrawlerCookieExportError,
    SpiderPools,
    SpiderTaskScheduler,
    classifyCrawlError,
    shouldRetryCrawlErrorForPlatform,
} from './spider-manager'

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

test('SpiderTaskScheduler injects connected processor definitions into crawler tasks', () => {
    const scheduler = new SpiderTaskScheduler(
        {
            crawlers: [],
            connections: {
                'crawler-processor': {
                    'Crawler A': 'processor-v4-flash',
                },
            } as any,
            processors: [
                {
                    id: 'processor-v4-flash',
                    provider: ProcessorProvider.DeepSeekV4Flash,
                    api_key: 'env:OPENCODE_GO_API_KEY',
                    cfg_processor: {
                        action: 'translate',
                    },
                },
            ],
        },
        new EventEmitter(),
    )

    const taskData = (scheduler as any).buildCrawlerTaskData({
        name: 'Crawler A',
        websites: ['https://x.com/member'],
        cfg_crawler: {
            cron: '* * * * *',
        },
    })

    expect(taskData.cfg_crawler.processor).toMatchObject({
        id: 'processor-v4-flash',
        provider: ProcessorProvider.DeepSeekV4Flash,
        api_key: 'env:OPENCODE_GO_API_KEY',
        cfg_processor: {
            action: 'translate',
        },
    })
    expect((scheduler as any).resolveCrawlerProcessorId(taskData)).toBe('processor-v4-flash')
})

test('SpiderTaskScheduler defaults 22/7 website crawlers to the social translator', () => {
    const scheduler = new SpiderTaskScheduler(
        {
            crawlers: [],
            connections: {} as any,
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.DeepSeekV4Flash,
                    api_key: 'env:OPENCODE_GO_API_KEY',
                    cfg_processor: { action: 'translate' },
                },
            ],
        },
        new EventEmitter(),
    )

    const taskData = (scheduler as any).buildCrawlerTaskData({
        name: '22/7官网Blog抓取 - 高频',
        websites: ['https://nanabunnonijyuuni-mobile.com/s/n110/diary/official_blog/list'],
        cfg_crawler: {},
    })

    expect((scheduler as any).resolveCrawlerProcessorId(taskData)).toBe('22_7-social-ja-zh')
    expect(taskData.cfg_crawler.processor?.id).toBe('22_7-social-ja-zh')
})

test('SpiderTaskScheduler dispatches due non-Cron crawler slots', async () => {
    const originalRecover = DB.TaskQueue.recoverStaleProcessing
    const originalGetPending = DB.TaskQueue.getPending
    ;(DB.TaskQueue as any).recoverStaleProcessing = async () => ({ count: 0 })
    ;(DB.TaskQueue as any).getPending = async () => []

    try {
        const emitter = new EventEmitter()
        const dispatched: any[] = []
        emitter.on(`spider:${TaskScheduler.TaskEvent.DISPATCH}`, (payload) => {
            dispatched.push(payload)
            emitter.emit(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
                taskId: payload.taskId,
                status: TaskScheduler.TaskStatus.COMPLETED,
            })
        })
        const scheduler = new SpiderTaskScheduler(
            {
                crawlers: [
                    {
                        name: 'Hot Slot Crawler',
                        websites: ['https://example.com/feed'],
                        cfg_crawler: {
                            schedule: {
                                slots: ['18:20'],
                                timezone: 'Asia/Tokyo',
                                min_gap_seconds: 0,
                            },
                        },
                    },
                ],
            },
            emitter,
        )

        await scheduler.init()
        const now = Date.UTC(2026, 5, 12, 9, 20, 0) / 1000
        const runtimeSchedule = (scheduler as any).runtimeSchedules.get('Hot Slot Crawler')
        runtimeSchedule.nextRunAt = now
        await (scheduler as any).runScheduleTick(now)

        expect(dispatched).toHaveLength(1)
        expect(dispatched[0]?.task.data.name).toBe('Hot Slot Crawler')
        expect(dispatched[0]?.task.meta.schedule_source).toBe('hot_schedule')
    } finally {
        ;(DB.TaskQueue as any).recoverStaleProcessing = originalRecover
        ;(DB.TaskQueue as any).getPending = originalGetPending
    }
})

test('SpiderTaskScheduler claims due scheduled crawler queue tasks', async () => {
    const originalRecover = DB.TaskQueue.recoverStaleProcessing
    const originalGetPending = DB.TaskQueue.getPending
    const originalClaim = DB.TaskQueue.claimPending
    const originalUpdate = DB.TaskQueue.updateStatus
    const task = {
        id: 401,
        type: DB.TaskQueue.TYPE.ScheduledCrawlerRun,
        payload: {
            crawler: 'Queued Crawler',
            reason: 'operator insert',
        },
        status: DB.TaskQueue.STATUS.Pending,
        execute_at: 1777047600,
    }
    ;(DB.TaskQueue as any).recoverStaleProcessing = async () => ({ count: 0 })
    ;(DB.TaskQueue as any).getPending = async () => [task]
    ;(DB.TaskQueue as any).claimPending = async () => ({ ...task, status: DB.TaskQueue.STATUS.Processing })
    ;(DB.TaskQueue as any).updateStatus = async () => undefined

    try {
        const emitter = new EventEmitter()
        const dispatched: any[] = []
        emitter.on(`spider:${TaskScheduler.TaskEvent.DISPATCH}`, (payload) => dispatched.push(payload))
        const scheduler = new SpiderTaskScheduler(
            {
                crawlers: [
                    {
                        name: 'Queued Crawler',
                        websites: ['https://example.com/feed'],
                        cfg_crawler: {
                            schedule: {
                                enabled: false,
                            },
                        },
                    },
                ],
            },
            emitter,
        )

        await scheduler.init()
        ;(scheduler as any).runtimeSchedules.clear()
        await (scheduler as any).runScheduleTick(task.execute_at)

        expect(dispatched).toHaveLength(1)
        expect(dispatched[0]?.task.data.name).toBe('Queued Crawler')
        expect(dispatched[0]?.task.meta.task_queue_id).toBe(401)
        expect(dispatched[0]?.task.meta.schedule_source).toBe('task_queue')
    } finally {
        ;(DB.TaskQueue as any).recoverStaleProcessing = originalRecover
        ;(DB.TaskQueue as any).getPending = originalGetPending
        ;(DB.TaskQueue as any).claimPending = originalClaim
        ;(DB.TaskQueue as any).updateStatus = originalUpdate
    }
})

test('SpiderTaskScheduler uses queued crawler websites as one-shot target override', async () => {
    const originalRecover = DB.TaskQueue.recoverStaleProcessing
    const originalGetPending = DB.TaskQueue.getPending
    const originalClaim = DB.TaskQueue.claimPending
    const originalUpdate = DB.TaskQueue.updateStatus
    const task = {
        id: 402,
        type: DB.TaskQueue.TYPE.ScheduledCrawlerRun,
        payload: {
            crawler: 'Tiktok抓取',
            websites: ['https://www.tiktok.com/@tabesugiyaseruzo'],
            reason: 'x tiktok link',
        },
        status: DB.TaskQueue.STATUS.Pending,
        execute_at: 1777047600,
    }
    ;(DB.TaskQueue as any).recoverStaleProcessing = async () => ({ count: 0 })
    ;(DB.TaskQueue as any).getPending = async () => [task]
    ;(DB.TaskQueue as any).claimPending = async () => ({ ...task, status: DB.TaskQueue.STATUS.Processing })
    ;(DB.TaskQueue as any).updateStatus = async () => undefined

    try {
        const emitter = new EventEmitter()
        const dispatched: any[] = []
        emitter.on(`spider:${TaskScheduler.TaskEvent.DISPATCH}`, (payload) => dispatched.push(payload))
        const scheduler = new SpiderTaskScheduler(
            {
                crawlers: [
                    {
                        name: 'Tiktok抓取',
                        origin: 'https://www.tiktok.com',
                        paths: ['/@227official', '/@sally_amaki_official'],
                        cfg_crawler: {
                            schedule: {
                                enabled: false,
                            },
                        },
                    },
                ],
            },
            emitter,
        )

        await scheduler.init()
        ;(scheduler as any).runtimeSchedules.clear()
        await (scheduler as any).runScheduleTick(task.execute_at)

        expect(dispatched).toHaveLength(1)
        expect(dispatched[0]?.task.data).toMatchObject({
            name: 'Tiktok抓取',
            websites: ['https://www.tiktok.com/@tabesugiyaseruzo'],
        })
        expect(dispatched[0]?.task.data.origin).toBeUndefined()
        expect(dispatched[0]?.task.data.paths).toBeUndefined()
    } finally {
        ;(DB.TaskQueue as any).recoverStaleProcessing = originalRecover
        ;(DB.TaskQueue as any).getPending = originalGetPending
        ;(DB.TaskQueue as any).claimPending = originalClaim
        ;(DB.TaskQueue as any).updateStatus = originalUpdate
    }
})

test('SpiderPools ignores malformed dispatch payloads without status side effects', async () => {
    const emitter = new EventEmitter()
    const statusEvents: any[] = []
    emitter.on(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, (payload) => statusEvents.push(payload))
    const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-pools', emitter)

    await (pools as any).dispatchListener(undefined)

    expect(statusEvents).toEqual([])
})

test('SpiderPools dispatch listener catches unexpected async failures and fails linked task', async () => {
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const statusUpdates: any[] = []
    const statusEvents: any[] = []

    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }

    try {
        const emitter = new EventEmitter()
        emitter.on(`spider:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, (payload) => statusEvents.push(payload))
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-pools', emitter)
        ;(pools as any).onTaskReceived = async () => {
            throw new Error('outer dispatch boom')
        }

        await (pools as any).dispatchListener({
            taskId: 'manual-boom',
            task: {
                id: 'manual-boom',
                status: TaskScheduler.TaskStatus.PENDING,
                data: {
                    name: 'crawler-boom',
                    websites: ['https://unsupported.invalid/path'],
                },
                meta: {
                    task_queue_id: 126,
                },
            },
        })

        expect(statusEvents).toEqual([
            {
                taskId: 'manual-boom',
                status: TaskScheduler.TaskStatus.FAILED,
            },
        ])
        expect(statusUpdates).toEqual([
            {
                id: 126,
                status: DB.TaskQueue.STATUS.Failed,
                meta: {
                    last_error: 'outer dispatch boom',
                    result_summary: 'crawler crawler-boom failed: unexpected dispatch error',
                },
            },
        ])
    } finally {
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
    }
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

test('SpiderPools defaults Website/Fanclub browser requests to Samsung Android mobile profile', () => {
    const pools = new SpiderPools('/tmp/idol-bbq-utils-test-mobile-profile', new EventEmitter())

    const request = (pools as any).resolveBrowserRequest(
        undefined,
        new URL('https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/227'),
        Platform.Website,
    )

    expect(request).toMatchObject({
        device_profile: 'mobile_android_chrome_samsung_large',
        session_profile: 'mobile_android_chrome_samsung_large:nanabunnonijyuuni-mobile.com',
    })
})

test('SpiderPools rejects desktop browser profile for mobile-required Fanclub hosts', () => {
    const pools = new SpiderPools('/tmp/idol-bbq-utils-test-mobile-profile-reject', new EventEmitter())

    expect(() =>
        (pools as any).resolveBrowserRequest(
            { device_profile: 'desktop_chrome' },
            new URL('https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/227'),
            Platform.Website,
        ),
    ).toThrow('must use a mobile device profile')
})

test('SpiderPools completes partial-success crawler tasks and emits finished results', async () => {
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const originalCheckExist = DB.Article.checkExist
    const originalTrySave = DB.Article.trySave
    const statusUpdates: any[] = []
    const finishedEvents: any[] = []
    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }
    ;(DB.Article as any).checkExist = async () => undefined
    ;(DB.Article as any).trySave = async () => ({ id: 227 })

    try {
        const emitter = new EventEmitter()
        emitter.on(`spider:${TaskScheduler.TaskEvent.FINISHED}`, (payload) => finishedEvents.push(payload))
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-partial-success', emitter)
        ;(pools as any).spiders.set('x-timeline', {
            crawl: async (url: string) => {
                if (url.includes('fail_member')) {
                    throw new Error('Profile format may have changed')
                }
                return [
                    {
                        a_id: '2034851104853524704',
                        u_id: 'ok_member',
                        username: 'ok member',
                        created_at: 1773981283,
                        url: 'https://x.com/ok_member/status/2034851104853524704',
                        type: 'tweet',
                        has_media: false,
                        media: [],
                        platform: Platform.X,
                    },
                ]
            },
        })

        await (pools as any).onTaskReceived({
            taskId: 'manual-partial',
            task: {
                id: 'manual-partial',
                status: TaskScheduler.TaskStatus.PENDING,
                data: {
                    name: 'crawler-partial',
                    websites: ['https://x.com/ok_member', 'https://x.com/fail_member'],
                    cfg_crawler: {
                        engine: 'unit-test' as any,
                    },
                },
                meta: {
                    task_queue_id: 130,
                },
            },
        })

        expect(statusUpdates.at(-1)).toMatchObject({
            id: 130,
            status: DB.TaskQueue.STATUS.Completed,
        })
        expect(statusUpdates.at(-1)?.meta?.result_summary).toContain('1 article(s)')
        expect(statusUpdates.at(-1)?.meta?.result_summary).toContain('1 warning(s)')
        expect(statusUpdates.at(-1)?.meta?.last_error).toContain('fail_member')
        expect(finishedEvents).toHaveLength(1)
        expect(finishedEvents[0]?.result).toEqual([
            {
                task_type: 'article',
                url: 'https://x.com/ok_member',
                data: [227],
            },
        ])
    } finally {
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
        ;(DB.Article as any).checkExist = originalCheckExist
        ;(DB.Article as any).trySave = originalTrySave
    }
})

test('SpiderPools cooldown skips same auth-risk target without retrying immediately', async () => {
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const statusUpdates: any[] = []
    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }

    try {
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-risk-cooldown', new EventEmitter())
        let crawlCalls = 0
        ;(pools as any).spiders.set('x-timeline', {
            crawl: async () => {
                crawlCalls += 1
                throw new Error('You need to login first, check your cookies')
            },
        })
        const task = {
            id: 'manual-cooldown',
            status: TaskScheduler.TaskStatus.PENDING,
            data: {
                name: 'crawler-cooldown',
                websites: ['https://x.com/auth_member'],
                cfg_crawler: {
                    engine: 'unit-test' as any,
                    session_profile: 'x-main',
                },
            },
            meta: {
                task_queue_id: 131,
            },
        }

        await (pools as any).onTaskReceived({ taskId: 'manual-cooldown-1', task })
        await (pools as any).onTaskReceived({ taskId: 'manual-cooldown-2', task })

        expect(crawlCalls).toBe(1)
        expect(statusUpdates.at(-1)).toMatchObject({
            id: 131,
            status: DB.TaskQueue.STATUS.Completed,
        })
        expect(statusUpdates.at(-1)?.meta?.result_summary).toContain('1 skipped')
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

test('SpiderPools dispatches premiere-resolved articles without a reuse policy', async () => {
    const originalCheckExist = DB.Article.checkExist
    const originalTrySave = DB.Article.trySave
    const originalUpdate = DB.Article.update
    const now = Math.floor(Date.now() / 1000)

    ;(DB.Article as any).checkExist = async (article: any) => {
        if (article.a_id === 'premiere-resolved-1') {
            return {
                id: 3333,
                content: 'Coming soon',
                extra: { data: { premiere: { pending: true, scheduled_start_at: now - 3600 } } },
            }
        }
        return undefined
    }
    ;(DB.Article as any).trySave = async () => undefined
    ;(DB.Article as any).update = async (id: number) => ({ id })

    try {
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-pools', new EventEmitter())
        const result = await (pools as any).crawlArticle(
            {
                taskId: 'spider-test',
                task: {
                    id: 'spider-test',
                    status: 'running',
                    data: { cfg_crawler: {} },
                },
            },
            {
                crawl: async () =>
                    [
                        {
                            a_id: 'premiere-resolved-1',
                            u_id: '227SMEJ',
                            username: '22/7',
                            created_at: now - 60,
                            url: 'https://www.youtube.com/watch?v=premiere-resolved-1',
                            type: 'video',
                            has_media: true,
                            media: [],
                            platform: Platform.YouTube,
                            extra: {
                                data: {
                                    premiere: { pending: false, scheduled_start_at: now - 3600, resolved_at: now - 60 },
                                },
                            },
                        },
                    ] as any,
            } as any,
            new URL('https://www.youtube.com/@227SMEJ'),
        )

        expect(result).toEqual([3333])
    } finally {
        ;(DB.Article as any).checkExist = originalCheckExist
        ;(DB.Article as any).trySave = originalTrySave
        ;(DB.Article as any).update = originalUpdate
    }
})

test('SpiderPools does not resolve a pending premiere from list-page shape alone', async () => {
    const originalCheckExist = DB.Article.checkExist
    const originalTrySave = DB.Article.trySave
    const originalUpdate = DB.Article.update
    const now = Math.floor(Date.now() / 1000)
    let updateCalled = false

    ;(DB.Article as any).checkExist = async (article: any) => {
        if (article.a_id === 'premiere-real-title') {
            return {
                id: 4444,
                content: 'Coming soon',
                extra: { data: { premiere: { pending: true, scheduled_start_at: now + 86400 } } },
            }
        }
        return undefined
    }
    ;(DB.Article as any).trySave = async () => undefined
    ;(DB.Article as any).update = async () => {
        updateCalled = true
        return { id: 4444 }
    }

    try {
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-pools', new EventEmitter())
        const result = await (pools as any).crawlArticle(
            {
                taskId: 'spider-test',
                task: {
                    id: 'spider-test',
                    status: 'running',
                    data: { cfg_crawler: {} },
                },
            },
            {
                crawl: async () =>
                    [
                        {
                            a_id: 'premiere-real-title',
                            u_id: '227SMEJ',
                            username: '22/7',
                            created_at: 0,
                            url: 'https://www.youtube.com/watch?v=premiere-real-title',
                            type: 'video',
                            has_media: true,
                            media: [],
                            platform: Platform.YouTube,
                            extra: null,
                        },
                    ] as any,
            } as any,
            new URL('https://www.youtube.com/@227SMEJ'),
        )

        expect(updateCalled).toBe(false)
        expect(result).toEqual([])
    } finally {
        ;(DB.Article as any).checkExist = originalCheckExist
        ;(DB.Article as any).trySave = originalTrySave
        ;(DB.Article as any).update = originalUpdate
    }
})

function makeCookieExportPage(cookies: Array<any>) {
    return {
        browserContext: () => ({
            cookies: async () => cookies,
            setCookie: async (...seededCookies: Array<any>) => {
                cookies.push(...seededCookies)
            },
        }),
        goto: async () => undefined,
        close: async () => undefined,
    }
}

test('SpiderPools exportCrawlerCookies returns only relevant cookies and required-name metadata', async () => {
    const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-cookie-export', new EventEmitter())
    ;(pools as any).browserPool = {
        createPage: async () =>
            makeCookieExportPage([
                {
                    name: 'auth_token',
                    value: 'auth-value',
                    domain: '.x.com',
                    path: '/',
                    expires: 9999999999,
                    secure: true,
                    httpOnly: true,
                },
                {
                    name: 'ct0',
                    value: 'csrf-value',
                    domain: '.x.com',
                    path: '/',
                    expires: 9999999999,
                    secure: true,
                    httpOnly: false,
                },
                {
                    name: 'sessionid',
                    value: 'instagram-value',
                    domain: '.instagram.com',
                    path: '/',
                    expires: 9999999999,
                    secure: true,
                    httpOnly: true,
                },
            ]),
    }

    const snapshot = await pools.exportCrawlerCookies({
        name: 'x-list',
        origin: 'https://x.com',
        paths: ['/i/lists/1936785344072151389'],
        cfg_crawler: {
            session_profile: 'x-main',
        },
    })

    expect(snapshot.platformHint).toBe('x')
    expect(snapshot.requiredCookieNames).toEqual({
        present: ['auth_token', 'ct0'],
        missing: [],
    })
    expect(snapshot.cookies.map((cookie) => cookie.name).sort()).toEqual(['auth_token', 'ct0'])
    expect(snapshot.liveProbe).toEqual({
        checked: false,
        status: 'skipped',
        diagnostic_codes: ['live_probe_not_requested'],
        http_status: null,
    })
})

test('SpiderPools exportCrawlerCookies rejects sessions missing required platform cookies', async () => {
    const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-cookie-export-missing', new EventEmitter())
    const pageRequests: Array<any> = []
    ;(pools as any).browserPool = {
        createPage: async (request: any) => {
            pageRequests.push(request)
            return makeCookieExportPage([
                {
                    name: 'sessionid',
                    value: 'instagram-value',
                    domain: '.instagram.com',
                    path: '/',
                    expires: 9999999999,
                    secure: true,
                    httpOnly: true,
                },
            ])
        },
    }

    await expect(
        pools.exportCrawlerCookies({
            name: 'x-list',
            origin: 'https://x.com',
            cfg_crawler: {
                session_profile: 'x-main',
            },
        }),
    ).rejects.toThrow(CrawlerCookieExportError)
    expect(pageRequests[0]?.device_profile).toBe('desktop_chrome')
})

test('SpiderPools exportCrawlerCookies seeds configured cookies when profile is only partially authenticated', async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'idol-bbq-cookie-seed-'))
    const cookieFile = path.join(tempRoot, 'x.cookies.txt')
    fs.writeFileSync(
        cookieFile,
        [
            '# Netscape HTTP Cookie File',
            '.x.com\tTRUE\t/\tTRUE\t9999999999\tauth_token\tseed-auth-value',
            '.x.com\tTRUE\t/\tTRUE\t9999999999\tct0\tseed-csrf-value',
            '',
        ].join('\n'),
        'utf8',
    )

    try {
        const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-cookie-export-partial', new EventEmitter())
        const page = makeCookieExportPage([
            {
                name: 'guest_id',
                value: 'guest-value',
                domain: '.x.com',
                path: '/',
                expires: 9999999999,
                secure: true,
                httpOnly: false,
            },
        ])
        ;(pools as any).browserPool = {
            createPage: async () => page,
        }

        const snapshot = await pools.exportCrawlerCookies(
            {
                name: 'x-list',
                origin: 'https://x.com',
                cfg_crawler: {
                    session_profile: 'x-main',
                    cookie_file: cookieFile,
                },
            },
            {
                visit: false,
            },
        )

        expect(snapshot.requiredCookieNames).toEqual({
            present: ['auth_token', 'ct0'],
            missing: [],
        })
        expect(snapshot.cookies.map((cookie) => cookie.name).sort()).toEqual(['auth_token', 'ct0', 'guest_id'])
    } finally {
        fs.rmSync(tempRoot, { recursive: true, force: true })
    }
})

test('SpiderPools exportCrawlerCookies rejects X sessions that fail live auth validation', async () => {
    const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-cookie-export-live-fail', new EventEmitter())
    const probeUrls: Array<string> = []
    ;(pools as any).browserPool = {
        createPage: async () =>
            makeCookieExportPage([
                {
                    name: 'auth_token',
                    value: 'stale-auth-value',
                    domain: '.x.com',
                    path: '/',
                    expires: 9999999999,
                    secure: true,
                    httpOnly: true,
                },
                {
                    name: 'ct0',
                    value: 'stale-csrf-value',
                    domain: '.x.com',
                    path: '/',
                    expires: 9999999999,
                    secure: true,
                    httpOnly: false,
                },
            ]),
    }

    let exportError: any
    try {
        await pools.exportCrawlerCookies(
            {
                name: 'x-list',
                origin: 'https://x.com/i/lists',
                paths: ['1940955289840476438'],
                cfg_crawler: {
                    session_profile: 'x-main',
                },
            },
            {
                validateLiveProbe: true,
                fetch: (async (url: string) => {
                    probeUrls.push(url)
                    return new Response('', { status: 401 })
                }) as any,
            },
        )
    } catch (error) {
        exportError = error
    }
    expect(exportError).toBeInstanceOf(CrawlerCookieExportError)
    expect(exportError.code).toBe('crawler_cookie_live_probe_failed')
    expect(exportError.publicDetails).toMatchObject({
        cookie_count: 2,
        required_cookie_names: {
            present: ['auth_token', 'ct0'],
            missing: [],
        },
        live_probe: {
            checked: true,
            status: 'fail',
            diagnostic_codes: ['x_list_timeline_auth_rejected'],
            http_status: 401,
        },
    })
    expect(probeUrls).toHaveLength(1)
    expect(probeUrls[0]).toContain('/ListLatestTweetsTimeline?')
    expect(probeUrls[0]).toContain('1940955289840476438')
})

test('SpiderPools exportCrawlerCookies can audit without seeding configured cookies or visiting pages', async () => {
    const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-cookie-export-readonly', new EventEmitter())
    let setCookieCalls = 0
    let gotoCalls = 0
    const pageRequests: Array<any> = []
    ;(pools as any).browserPool = {
        createPage: async (request: any) => {
            pageRequests.push(request)
            return {
                browserContext: () => ({
                    cookies: async () => [],
                    setCookie: async () => {
                        setCookieCalls += 1
                    },
                }),
                goto: async () => {
                    gotoCalls += 1
                },
                close: async () => undefined,
            }
        },
    }

    await expect(
        pools.exportCrawlerCookies(
            {
                name: 'x-list',
                origin: 'https://x.com',
                cfg_crawler: {
                    browser_mode: 'headed-xvfb',
                    session_profile: 'x-main',
                    cookie_file: '/tmp/seed-would-be-used.txt',
                },
            },
            {
                seedConfiguredCookieFile: false,
                visit: false,
                browserModeOverride: 'headless',
            },
        ),
    ).rejects.toThrow(CrawlerCookieExportError)
    expect(pageRequests[0]?.browser_mode).toBe('headless')
    expect(setCookieCalls).toBe(0)
    expect(gotoCalls).toBe(0)
})

test('SpiderPools exportCrawlerCookies reports safe browser page creation failures', async () => {
    const pools = new SpiderPools('/tmp/idol-bbq-utils-test-spider-cookie-export-browser-fail', new EventEmitter())
    ;(pools as any).browserPool = {
        createPage: async () => {
            throw new Error('/private/profile/path is intentionally hidden')
        },
    }

    let exportError: any
    try {
        await pools.exportCrawlerCookies({
            name: 'x-backfill',
            origin: 'https://x.com',
            cfg_crawler: {
                browser_mode: 'headed-xvfb',
                session_profile: 'x-main',
            },
        })
    } catch (error) {
        exportError = error
    }

    expect(exportError).toBeInstanceOf(CrawlerCookieExportError)
    expect(exportError.code).toBe('crawler_cookie_browser_page_failed')
    expect(exportError.publicDetails).toMatchObject({
        cookie_count: 0,
        domains: ['x.com', 'twitter.com', 'api.x.com'],
        required_cookie_names: {
            present: [],
            missing: ['auth_token', 'ct0'],
        },
        browser: {
            session_profile: 'x-main',
            configured_browser_mode: 'headed-xvfb',
            effective_browser_mode: 'headed-xvfb',
            device_profile: 'desktop_chrome',
        },
        error_name: 'Error',
        live_probe: {
            checked: false,
            status: 'skipped',
            diagnostic_codes: ['browser_page_not_created'],
            http_status: null,
        },
    })
    expect(JSON.stringify(exportError.publicDetails)).not.toContain('/private/profile/path')
})

test('classifyCrawlError treats a throttle 302 redirect as rate_limit, not auth', () => {
    const throttleRedirect = new Error(
        'Error: redirect (302) to https://www.instagram.com/ - likely rate limit or challenge',
    )
    expect(classifyCrawlError(throttleRedirect)).toBe('rate_limit')
})

test('classifyCrawlError still classifies genuine login/checkpoint bounces as auth', () => {
    expect(classifyCrawlError(new Error('Error: login redirect (302): session expired or checkpoint'))).toBe('auth')
    expect(classifyCrawlError(new Error('You need to login first, check your cookies'))).toBe('auth')
    expect(classifyCrawlError(new Error('account challenge detected'))).toBe('auth')
    expect(classifyCrawlError(new Error('challenge_required'))).toBe('auth')
    expect(classifyCrawlError(new Error('checkpoint_required'))).toBe('auth')
    expect(classifyCrawlError(new Error('login_required'))).toBe('auth')
})

test('classifyCrawlError keeps explicit rate-limit and status signals', () => {
    expect(classifyCrawlError(new Error('Error: 429'))).toBe('rate_limit')
    expect(classifyCrawlError(new Error('too many requests'))).toBe('rate_limit')
    expect(classifyCrawlError(new Error('Error: 403'))).toBe('auth')
})

test('shouldRetryCrawlErrorForPlatform defers Instagram throttle instead of in-run hammering', () => {
    const throttleRedirect = new Error(
        'Error: redirect (302) to https://www.instagram.com/ - likely rate limit or challenge',
    )
    expect(shouldRetryCrawlErrorForPlatform(throttleRedirect, Platform.Instagram)).toBe(false)
    expect(shouldRetryCrawlErrorForPlatform(new Error('fetch failed'), Platform.Instagram)).toBe(true)
})
