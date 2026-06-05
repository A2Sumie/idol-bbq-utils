import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import { APIManager } from './api-manager'
import { existsSync, mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'

test('APIManager adds CIC CORS headers to control responses', () => {
    const manager = new APIManager({
        getConfig: () =>
            ({
                api: {
                    secret: 'test-secret',
                },
            }) as any,
        getDeps: () => ({}),
    })

    const corsHeaders = (manager as any).resolveCorsHeaders(
        new Request('http://localhost/api/runtime/status', {
            headers: {
                Origin: 'https://cic.n2nj.moe',
            },
        }),
    )
    const response = (manager as any).withCorsHeaders(new Response('ok'), corsHeaders)

    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('https://cic.n2nj.moe')
    expect(response.headers.get('Access-Control-Allow-Headers')).toContain('Authorization')
})

test('APIManager blocks high-risk actions in api-only mode before queue side effects', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    let taskAddCalls = 0
    ;(DB.TaskQueue as any).add = async () => {
        taskAddCalls += 1
        return { id: 1 }
    }

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    crawlers: [
                        {
                            name: 'crawler-a',
                            origin: 'https://x.com',
                        },
                    ],
                    processors: [
                        {
                            id: 'processor-a',
                            name: 'processor-a',
                            provider: 'noop',
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    emitter: { emit: () => undefined },
                    forwarderPools: { resendArticle: async () => undefined },
                    spiderPools: { exportCrawlerCookies: async () => ({ cookies: [] }) },
                }) as any,
            getRuntimeMeta: () =>
                ({
                    generation: 0,
                    configPath: 'config.yaml',
                    mode: 'api-only',
                    startedAt: new Date(0).toISOString(),
                    lastReloadedAt: new Date(0).toISOString(),
                    reloading: false,
                }) as any,
        })
        const server = { timeout: () => undefined }
        const requests = [
            ['/api/actions/crawlers/run', { name: 'crawler-a' }],
            ['/api/actions/articles/simulate', { platform: 'x', content: 'hello' }],
            ['/api/actions/articles/reprocess', { platform: 'x', id: 1 }],
            ['/api/actions/articles/resend', { platform: 'x', id: 1, crawlerName: 'crawler-a' }],
            ['/api/actions/processors/run', { text: 'hello' }],
            ['/api/cookies/sync', { finder: 'crawler-a' }],
            ['/api/archives/archive-a/upload', {}],
            ['/api/server/restart', {}],
        ] as const

        for (const [pathname, body] of requests) {
            const response = await (manager as any).dispatchApiRequest(
                new Request(`http://localhost${pathname}`, {
                    method: 'POST',
                    headers: {
                        Authorization: 'Bearer test-secret',
                    },
                    body: JSON.stringify(body),
                }),
                server,
                'test-secret',
            )
            expect(response.status).toBe(503)
            const payload = await response.json()
            expect(payload).toMatchObject({
                success: false,
                error: 'runtime_mode_disabled',
                runtime_mode: 'api-only',
            })
        }

        expect(taskAddCalls).toBe(0)
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
    }
})

test('APIManager keeps runtime reload available in api-only mode', async () => {
    let reloadCalls = 0
    const manager = new APIManager({
        getConfig: () =>
            ({
                api: {
                    secret: 'test-secret',
                },
            }) as any,
        getDeps: () => ({}),
        getRuntimeMeta: () =>
            ({
                generation: 0,
                configPath: 'config.yaml',
                mode: 'api-only',
                startedAt: new Date(0).toISOString(),
                lastReloadedAt: new Date(0).toISOString(),
                reloading: false,
            }) as any,
        reloadRuntime: async () => {
            reloadCalls += 1
            return {
                success: true,
                generation: 1,
                reloadedAt: new Date(1).toISOString(),
                configPath: 'config.yaml',
            }
        },
    })

    const response = await (manager as any).dispatchApiRequest(
        new Request('http://localhost/api/runtime/reload', {
            method: 'POST',
            headers: {
                Authorization: 'Bearer test-secret',
            },
        }),
        { timeout: () => undefined },
        'test-secret',
    )

    expect(response.status).toBe(200)
    const payload = await response.json()
    expect(payload.runtime).toMatchObject({
        success: true,
        generation: 1,
    })
    expect(reloadCalls).toBe(1)
})

test('APIManager runtime status uses full task status counts', async () => {
    const originalTaskList = DB.TaskQueue.list
    const originalTaskCountsByStatus = DB.TaskQueue.countsByStatus

    ;(DB.TaskQueue as any).list = async () => [
        { id: 1, status: 'completed' },
        { id: 2, status: 'completed' },
    ]
    ;(DB.TaskQueue as any).countsByStatus = async () => ({
        pending: 12,
        processing: 3,
        failed: 2,
        completed: 80,
    })

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    crawlers: [{ name: 'crawler-a' }],
                    processors: [{ id: 'processor-a' }],
                    formatters: [{ id: 'formatter-a' }],
                    forward_targets: [{ id: 'target-a' }],
                    forwarders: [{ id: 'forwarder-a' }],
                }) as any,
            getDeps: () => ({}),
            getRuntimeMeta: () =>
                ({
                    mode: 'online',
                    generation: 1,
                }) as any,
        })

        const response = await (manager as any).handleRuntimeStatus()
        expect(response.status).toBe(200)
        const payload = await response.json()

        expect(payload.pending_tasks).toBe(12)
        expect(payload.processing_tasks).toBe(3)
        expect(payload.failed_tasks).toBe(2)
        expect(payload.completed_tasks).toBe(80)
        expect(payload.task_counts).toEqual({
            pending: 12,
            processing: 3,
            failed: 2,
            completed: 80,
        })
        expect(payload.latest_tasks).toHaveLength(2)
    } finally {
        ;(DB.TaskQueue as any).list = originalTaskList
        ;(DB.TaskQueue as any).countsByStatus = originalTaskCountsByStatus
    }
})

test('APIManager task list forwards operator filters safely', async () => {
    const originalTaskList = DB.TaskQueue.list
    const calls: any[] = []

    ;(DB.TaskQueue as any).list = async (limit: number, filters: unknown) => {
        calls.push({ limit, filters })
        return []
    }

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                }) as any,
            getDeps: () => ({}),
        })

        const response = await (manager as any).handleTasks(
            new URL(
                'http://localhost/api/tasks?limit=abc&status=failed&type=aggregate_daily&source_ref=x%3Amember&action_type=aggregate&idempotency_key=idem-1',
            ),
        )
        expect(response.status).toBe(200)
        expect(await response.json()).toEqual([])
        expect(calls).toEqual([
            {
                limit: 50,
                filters: {
                    status: 'failed',
                    type: 'aggregate_daily',
                    source_ref: 'x:member',
                    action_type: 'aggregate',
                    idempotency_key: 'idem-1',
                },
            },
        ])
    } finally {
        ;(DB.TaskQueue as any).list = originalTaskList
    }
})

test('APIManager marks manual crawler run task failed when dispatch throws', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const statusUpdates: any[] = []

    ;(DB.TaskQueue as any).add = async () => ({ id: 88 })
    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    crawlers: [
                        {
                            name: 'crawler-a',
                            origin: 'https://x.com',
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    emitter: {
                        emit: () => {
                            throw new Error('dispatch unavailable')
                        },
                    },
                }) as any,
        })

        await expect(
            (manager as any).handleCrawlerRun(
                new Request('http://localhost/api/actions/crawlers/run', {
                    method: 'POST',
                    body: JSON.stringify({ name: 'crawler-a' }),
                }),
            ),
        ).rejects.toThrow('dispatch unavailable')

        expect(statusUpdates).toEqual([
            {
                id: 88,
                status: 'failed',
                meta: {
                    last_error: 'dispatch unavailable',
                },
            },
        ])
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
    }
})

test('APIManager queues manual crawler run without marking it completed before crawl finishes', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const dispatched: any[] = []
    const statusUpdates: any[] = []

    ;(DB.TaskQueue as any).add = async (_type: string, payload: any) => {
        return { id: 89, payload }
    }
    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    crawlers: [
                        {
                            name: 'crawler-a',
                            origin: 'https://x.com',
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    emitter: {
                        emit: (_event: string, payload: any) => {
                            dispatched.push(payload)
                            return true
                        },
                    },
                }) as any,
        })

        const response = await (manager as any).handleCrawlerRun(
            new Request('http://localhost/api/actions/crawlers/run', {
                method: 'POST',
                body: JSON.stringify({ name: 'crawler-a' }),
            }),
        )
        const payload = await response.json()

        expect(payload).toMatchObject({
            success: true,
            status: 'queued',
            crawler: 'crawler-a',
            taskQueueId: 89,
        })
        expect(String(payload.taskId).startsWith('manual-')).toBe(true)
        expect(statusUpdates).toEqual([])
        expect(dispatched).toHaveLength(1)
        expect(dispatched[0].task).toMatchObject({
            id: payload.taskId,
            status: 'pending',
            data: {
                name: 'crawler-a',
            },
            meta: {
                task_queue_id: 89,
                task_queue_type: 'manual_crawler_run',
            },
        })
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
    }
})

test('APIManager records failed processor runs when processor execution fails', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const originalProcessorRunCreate = DB.ProcessorRun.create
    const statusUpdates: any[] = []
    const processorRuns: any[] = []

    ;(DB.TaskQueue as any).add = async () => ({ id: 77 })
    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }
    ;(DB.ProcessorRun as any).create = async (data: any) => {
        processorRuns.push(data)
        return { id: 1, ...data }
    }

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    processors: [
                        {
                            id: 'broken-processor',
                            name: 'broken-processor',
                            provider: 'missing-provider',
                        },
                    ],
                }) as any,
            getDeps: () => ({}) as any,
        })

        await expect(
            (manager as any).handleProcessorRun(
                new Request('http://localhost/api/actions/processors/run', {
                    method: 'POST',
                    body: JSON.stringify({ text: 'hello' }),
                }),
            ),
        ).rejects.toThrow('Unknown processor provider')

        expect(processorRuns).toHaveLength(1)
        expect(processorRuns[0]).toMatchObject({
            processor_id: 'broken-processor',
            action: 'extract',
            source_type: 'text',
            source_ref: 'manual:text',
            status: DB.ProcessorRun.STATUS.Failed,
            input: {
                request: {
                    text: 'hello',
                },
            },
        })
        expect(processorRuns[0]?.error).toContain('Unknown processor provider')
        expect(statusUpdates).toHaveLength(2)
        expect(statusUpdates[0]).toMatchObject({
            id: 77,
            status: DB.TaskQueue.STATUS.Processing,
            meta: {
                result_summary: 'extract running',
            },
        })
        expect(statusUpdates[1]).toMatchObject({
            id: 77,
            status: DB.TaskQueue.STATUS.Failed,
        })
        expect(statusUpdates[1]?.meta?.last_error).toContain('Unknown processor provider')
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
        ;(DB.ProcessorRun as any).create = originalProcessorRunCreate
    }
})

test('APIManager resend infers website crawler platform from websites config', async () => {
    const originalGetSingleArticle = DB.Article.getSingleArticle
    const originalTaskAdd = DB.TaskQueue.add
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus

    const resendCalls: any[] = []
    const statusUpdates: any[] = []

    ;(DB.Article as any).getSingleArticle = async () =>
        ({
            id: 162,
            a_id: '11230',
            platform: Platform.Website,
        }) as any
    ;(DB.TaskQueue as any).add = async () => ({ id: 991 })
    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    crawlers: [
                        {
                            name: '22/7官网FC抓取 - 日间轮询',
                            websites: ['https://nanabunnonijyuuni-mobile.com/s/n110/news/list'],
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    forwarderPools: {
                        resendArticle: async (...args: any[]) => {
                            resendCalls.push(args)
                        },
                    },
                }) as any,
        })

        const response = await (manager as any).handleArticleResend(
            new Request('http://localhost/api/actions/articles/resend', {
                method: 'POST',
                body: JSON.stringify({
                    platform: 'website',
                    id: 162,
                    crawlerName: '22/7官网FC抓取 - 日间轮询',
                }),
            }),
        )

        expect(response.status).toBe(200)
        expect(await response.json()).toEqual({
            success: true,
            articleId: 162,
            crawlerName: '22/7官网FC抓取 - 日间轮询',
        })
        expect(resendCalls).toHaveLength(1)
        expect(resendCalls[0][0]).toMatchObject({
            id: 162,
            a_id: '11230',
            platform: Platform.Website,
        })
        expect(resendCalls[0][1]).toBe('22/7官网FC抓取 - 日间轮询')
        expect(statusUpdates).toEqual([
            {
                id: 991,
                status: DB.TaskQueue.STATUS.Processing,
                meta: {
                    result_summary: 'resending 11230',
                },
            },
            {
                id: 991,
                status: DB.TaskQueue.STATUS.Completed,
                meta: {
                    result_summary: 'resent 11230',
                },
            },
        ])
    } finally {
        ;(DB.Article as any).getSingleArticle = originalGetSingleArticle
        ;(DB.TaskQueue as any).add = originalTaskAdd
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
    }
})

test('APIManager returns redacted config for audit endpoints', async () => {
    const manager = new APIManager({
        getConfig: () =>
            ({
                api: {
                    secret: 'test-secret',
                },
                forward_targets: [
                    {
                        id: 'qq-1',
                        platform: 'qq',
                        cfg_platform: {
                            url: 'http://127.0.0.1:3001',
                            token: 'bot-token',
                            group_id: '123',
                        },
                    },
                ],
                crawlers: [
                    {
                        name: 'x-list',
                        cfg_crawler: {
                            cookie_file: '/tmp/cookies.txt',
                        },
                    },
                ],
            }) as any,
        getDeps: () => ({}),
    })

    const response = await (manager as any).handleConfigRedacted()
    expect(response.status).toBe(200)
    const config = await response.json()
    expect(config.api.secret).toBe('[redacted]')
    expect(config.forward_targets[0].cfg_platform.token).toBe('[redacted]')
    expect(config.crawlers[0].cfg_crawler.cookie_file).toBe('[redacted]')
    expect(config.forward_targets[0].cfg_platform.group_id).toBe('123')
})

test('APIManager defaults /api/config to redacted config', async () => {
    const manager = new APIManager({
        getConfig: () =>
            ({
                api: {
                    secret: 'test-secret',
                },
                crawlers: [
                    {
                        name: 'x-list',
                        cfg_crawler: {
                            cookie_file: '/tmp/private-x.cookies.txt',
                        },
                    },
                ],
                forward_targets: [
                    {
                        id: 'qq-1',
                        platform: 'qq',
                        cfg_platform: {
                            token: 'bot-token',
                        },
                    },
                ],
            }) as any,
        getDeps: () => ({}),
    })

    const response = await (manager as any).dispatchApiRequest(
        new Request('http://localhost/api/config', {
            headers: {
                Authorization: 'Bearer test-secret',
            },
        }),
        {
            timeout: () => undefined,
        },
        'test-secret',
    )

    expect(response.status).toBe(200)
    const config = await response.json()
    const serialized = JSON.stringify(config)
    expect(config.api.secret).toBe('[redacted]')
    expect(config.forward_targets[0].cfg_platform.token).toBe('[redacted]')
    expect(config.crawlers[0].cfg_crawler.cookie_file).toBe('[redacted]')
    expect(serialized).not.toContain('test-secret')
    expect(serialized).not.toContain('bot-token')
    expect(serialized).not.toContain('/tmp/private-x.cookies.txt')
})

test('APIManager crawler list exposes cookie metadata without cookie paths', async () => {
    const manager = new APIManager({
        getConfig: () =>
            ({
                api: {
                    secret: 'test-secret',
                },
                crawlers: [
                    {
                        name: 'x-list',
                        task_type: 'article',
                        cfg_crawler: {
                            cron: '5 */1 * * *',
                            cookie_file: '/tmp/private-x.cookies.txt',
                        },
                    },
                    {
                        name: 'website-list',
                        task_type: 'article',
                    },
                ],
            }) as any,
        getDeps: () => ({}),
    })

    const response = await (manager as any).dispatchApiRequest(
        new Request('http://localhost/api/config/crawlers', {
            headers: {
                Authorization: 'Bearer test-secret',
            },
        }),
        {
            timeout: () => undefined,
        },
        'test-secret',
    )

    expect(response.status).toBe(200)
    const crawlers = await response.json()
    expect(crawlers[0]).toMatchObject({
        name: 'x-list',
        cookieFile: {
            configured: true,
            filename: 'private-x.cookies.txt',
        },
    })
    expect(crawlers[1]).toMatchObject({
        name: 'website-list',
        cookieFile: {
            configured: false,
            filename: null,
        },
    })
    expect(JSON.stringify(crawlers)).not.toContain('/tmp/private-x.cookies.txt')
})

test('APIManager cookie sync response avoids returning full cookie paths', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-api-cookie-sync-'))
    const cookieFile = join(dir, 'synced.cookies.txt')
    let exportOptions: any
    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    crawlers: [
                        {
                            name: 'x-list',
                            origin: 'https://x.com',
                            cfg_crawler: {
                                cookie_file: cookieFile,
                            },
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    spiderPools: {
                        exportCrawlerCookies: async (_crawler: any, options: any) => {
                            exportOptions = options
                            return {
                                cookies: [
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
                                ],
                                sessionProfile: 'profile-a',
                                visitedUrl: 'https://x.com/X',
                                domains: ['x.com'],
                                platformHint: 'x',
                                requiredCookieNames: {
                                    present: ['auth_token', 'ct0'],
                                    missing: [],
                                },
                                liveProbe: {
                                    checked: true,
                                    status: 'ok',
                                    diagnostic_codes: ['x_live_probe_ok'],
                                    http_status: 200,
                                },
                            }
                        },
                    },
                }) as any,
            getRuntimeMeta: () =>
                ({
                    mode: 'online',
                }) as any,
        })

        const response = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/cookies/sync', {
                method: 'POST',
                headers: {
                    Authorization: 'Bearer test-secret',
                },
                body: JSON.stringify({ finder: 'x-list' }),
            }),
            {
                timeout: () => undefined,
            },
            'test-secret',
        )

        expect(response.status).toBe(200)
        expect(existsSync(cookieFile)).toBeTrue()
        const payload = await response.json()
        const serialized = JSON.stringify(payload)
        expect(payload.cookieFile).toEqual({
            configured: true,
            filename: 'synced.cookies.txt',
        })
        expect(payload.platformHint).toBe('x')
        expect(payload.requiredCookieNames).toEqual({
            present: ['auth_token', 'ct0'],
            missing: [],
        })
        expect(payload.liveProbe).toEqual({
            checked: true,
            status: 'ok',
            diagnostic_codes: ['x_live_probe_ok'],
            http_status: 200,
        })
        expect(exportOptions).toMatchObject({
            validateLiveProbe: true,
        })
        expect(serialized).not.toContain(cookieFile)
        expect(serialized).not.toContain(dir)
        expect(serialized).not.toContain('auth-value')
        expect(serialized).not.toContain('csrf-value')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('APIManager cookie sync returns safe conflict when session lacks required cookies', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-api-cookie-sync-missing-'))
    const cookieFile = join(dir, 'missing.cookies.txt')
    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    crawlers: [
                        {
                            name: 'x-list',
                            origin: 'https://x.com',
                            cfg_crawler: {
                                cookie_file: cookieFile,
                            },
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    spiderPools: {
                        exportCrawlerCookies: async () => {
                            const error = new Error('Browser session x-main is missing required x cookies: auth_token, ct0')
                            ;(error as any).statusCode = 409
                            ;(error as any).publicMessage =
                                'Browser session x-main is missing required x cookies: auth_token, ct0'
                            throw error
                        },
                    },
                }) as any,
            getRuntimeMeta: () =>
                ({
                    mode: 'online',
                }) as any,
        })

        const response = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/cookies/sync', {
                method: 'POST',
                headers: {
                    Authorization: 'Bearer test-secret',
                },
                body: JSON.stringify({ finder: 'x-list' }),
            }),
            {
                timeout: () => undefined,
            },
            'test-secret',
        )

        expect(response.status).toBe(409)
        const text = await response.text()
        expect(text).toContain('missing required x cookies')
        expect(text).not.toContain(cookieFile)
        expect(text).not.toContain(dir)
        expect(existsSync(cookieFile)).toBeFalse()
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('APIManager returns no-secret config audit for route policy checks', async () => {
    const manager = new APIManager({
        getConfig: () =>
            ({
                api: {
                    secret: 'test-secret',
                },
                crawlers: [
                    {
                        id: 'crawler-x',
                        name: 'crawler x',
                    },
                ],
                formatters: [
                    {
                        id: 'formatter-a',
                        name: 'formatter a',
                    },
                ],
                forward_targets: [
                    {
                        id: 'qq-1',
                        platform: 'qq',
                        cfg_platform: {
                            url: 'http://127.0.0.1:3001',
                            token: 'bot-token',
                            group_id: '123',
                            summary_card: {
                                enabled: true,
                                interval_seconds: 7200,
                                send_first_native: true,
                                media_realtime: true,
                                media_duplicate_limit: 2,
                                flush_on_threshold: false,
                                align_to_hour: true,
                                flush_delay_seconds: 300,
                            },
                        },
                    },
                ],
                connections: {
                    'crawler-formatter': {
                        'crawler-x': ['formatter-a'],
                    },
                    'formatter-target': {
                        'formatter-a': ['qq-1'],
                    },
                },
            }) as any,
        getDeps: () => ({}),
    })

    const response = await (manager as any).dispatchApiRequest(
        new Request('http://localhost/api/config/audit', {
            headers: {
                Authorization: 'Bearer test-secret',
            },
        }),
        {
            timeout: () => undefined,
        },
        'test-secret',
    )
    expect(response.status).toBe(200)
    const audit = await response.json()
    const serialized = JSON.stringify(audit)
    expect(audit.secret_fields.paths).toContain('api.secret')
    expect(audit.secret_fields.paths).toContain('forward_targets[0].cfg_platform.token')
    expect(audit.route_graph.summary_card_routes[0].policy.summary_card).toMatchObject({
        interval_seconds: 7200,
        send_first_native: true,
        media_realtime: true,
        flush_on_threshold: false,
        window_alignment: 'hour',
    })
    expect(audit.policy_hash).toMatch(/^[a-f0-9]{64}$/)
    expect(serialized).not.toContain('test-secret')
    expect(serialized).not.toContain('bot-token')
})
