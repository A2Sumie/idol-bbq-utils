import { expect, test } from 'bun:test'
import EventEmitter from 'events'
import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import { TaskScheduler } from '@/utils/base'
import { CrawlerCookieExportError, SpiderPools, SpiderTaskScheduler } from './spider-manager'

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
                origin: 'https://x.com',
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
            diagnostic_codes: ['x_live_auth_rejected'],
            http_status: 401,
        },
    })
    expect(probeUrls).toEqual(['https://x.com/i/api/1.1/account/settings.json'])
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
