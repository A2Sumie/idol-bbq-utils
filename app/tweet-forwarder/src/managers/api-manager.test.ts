import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import { APIManager } from './api-manager'
import { processorRegistry } from '@/middleware/processor'
import { CACHE_DIR_ROOT } from '@/config'
import { existsSync, mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from 'fs'
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
            ['/api/actions/qq/x-link', { raw_message: 'https://x.com/kyu0817a/status/2068528507651842559' }],
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
        {
            id: 1,
            type: DB.TaskQueue.TYPE.NotificationSignal,
            status: 'completed',
            source_ref: 'notification:instagram:private-member',
            idempotency_key: 'private-idempotency-key',
            payload: {
                schema_version: 1,
                mode: 'shadow',
                platform: 'instagram',
                event_key: 'private-event-key',
                source_ref: 'notification:instagram:private-member',
                received_at: 1_800_000_000,
                notification: {
                    username: 'private-member',
                    title_hash: 'private-title-hash',
                    title_length: 12,
                },
                matched_crawlers: [],
                would_trigger_crawlers: false,
            },
        },
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
        const serialized = JSON.stringify(payload.latest_tasks)
        expect(payload.latest_tasks[0].source_ref).toBe('[redacted]')
        expect(payload.latest_tasks[0].idempotency_key).toBe('[redacted]')
        expect(serialized).not.toContain('private-member')
        expect(serialized).not.toContain('private-event-key')
        expect(serialized).not.toContain('private-title-hash')
    } finally {
        ;(DB.TaskQueue as any).list = originalTaskList
        ;(DB.TaskQueue as any).countsByStatus = originalTaskCountsByStatus
    }
})

test('APIManager agent status reports model capabilities without processor secrets', async () => {
    const originalTaskCountsByStatus = DB.TaskQueue.countsByStatus
    ;(DB.TaskQueue as any).countsByStatus = async () => ({
        pending: 1,
        processing: 0,
        failed: 0,
        completed: 2,
    })

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    processors: [
                        {
                            id: 'main',
                            name: 'main model',
                            provider: 'DeepSeekV4Pro',
                            api_key: 'private-key',
                            cfg_processor: {
                                model_id: 'deepseek-v4-pro',
                                name: 'OpenCode-Go-DeepSeek-v4-pro-22/7',
                                temperature: 0.2,
                            },
                        },
                    ],
                }) as any,
            getDeps: () => ({}),
            getRuntimeMeta: () =>
                ({
                    mode: 'api-only',
                    generation: 1,
                }) as any,
        })

        const response = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/agent/status', {
                headers: {
                    Authorization: 'Bearer test-secret',
                },
            }),
            { timeout: () => undefined },
            'test-secret',
        )

        expect(response.status).toBe(200)
        const payload = await response.json()
        expect(payload.models[0]).toMatchObject({
            processor_id: 'main',
            provider: 'DeepSeekV4Pro',
            model_id: 'deepseek-v4-pro',
            capability: {
                display_name: 'DeepSeek V4 Pro',
                context_window: 1_000_000,
                reasoning: true,
            },
        })
        expect(JSON.stringify(payload)).not.toContain('private-key')
    } finally {
        ;(DB.TaskQueue as any).countsByStatus = originalTaskCountsByStatus
    }
})

test('APIManager agent model probe uses a bounded lightweight processor config', async () => {
    const originalCreate = processorRegistry.create
    const calls: any[] = []
    ;(processorRegistry as any).create = async (provider: string, apiKey: string, _log: unknown, config: any) => {
        calls.push({ provider, apiKey, config })
        return {
            NAME: 'mock-probe',
            process: async (text: string) => `OK:${text}`,
            drop: async () => undefined,
        }
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
                            id: 'main',
                            name: 'main model',
                            provider: 'DeepSeekV4Pro',
                            api_key: 'private-key',
                            cfg_processor: {
                                model_id: 'deepseek-v4-pro',
                                prompt_assets: [{ path: '/private/prompt.txt' }],
                                response_format: 'json_object',
                                max_tokens: 2048,
                            },
                        },
                    ],
                }) as any,
            getDeps: () => ({}),
        })

        const response = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/agent/probe-model', {
                method: 'POST',
                headers: {
                    Authorization: 'Bearer test-secret',
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    processor_id: 'main',
                    text: 'ping',
                    timeout_ms: 10_000,
                }),
            }),
            { timeout: () => undefined },
            'test-secret',
        )

        expect(response.status).toBe(200)
        const payload = await response.json()
        expect(payload).toMatchObject({
            success: true,
            provider: 'DeepSeekV4Pro',
            model_id: 'deepseek-v4-pro',
            output_preview: 'OK:ping',
        })
        expect(payload.latency_ms).toBeGreaterThan(0)
        expect(payload.chars_per_sec).toBeGreaterThan(0)
        expect(calls).toHaveLength(1)
        expect(calls[0].apiKey).toBe('private-key')
        expect(calls[0].config).toMatchObject({
            prompt_assets: [],
            response_format: 'none',
            max_tokens: 32,
            request_timeout_ms: 10_000,
        })
    } finally {
        ;(processorRegistry as any).create = originalCreate
    }
})

test('APIManager exposes Codex MCP bridge status and run endpoints', async () => {
    const root = mkdtempSync(join(tmpdir(), 'api-codex-mcp-fake-'))
    const script = join(root, 'fake-codex-mcp.cjs')
    writeFileSync(
        script,
        `
const readline = require('node:readline')
const rl = readline.createInterface({ input: process.stdin })
function send(id, result) {
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, result }) + '\\n')
}
rl.on('line', (line) => {
  if (!line.trim()) return
  const msg = JSON.parse(line)
  if (msg.method === 'initialize') {
    send(msg.id, { protocolVersion: '2024-11-05', capabilities: { tools: {} }, serverInfo: { name: 'fake-codex' } })
    return
  }
  if (msg.method === 'tools/list') {
    send(msg.id, { tools: [{ name: 'codex', title: 'Codex' }, { name: 'codex-reply', title: 'Codex Reply' }] })
    return
  }
  if (msg.method === 'tools/call') {
    const args = msg.params.arguments || {}
    send(msg.id, {
      structuredContent: { threadId: 'thread-api', content: 'codex:' + args.prompt },
      content: [{ type: 'text', text: JSON.stringify({ threadId: 'thread-api', content: 'codex:' + args.prompt }) }]
    })
  }
})
`,
    )

    const originalEnabled = process.env.IDOL_BBQ_CODEX_MCP_ENABLED
    const originalCommand = process.env.IDOL_BBQ_CODEX_MCP_COMMAND
    const originalArgs = process.env.IDOL_BBQ_CODEX_MCP_ARGS_JSON
    const originalBridgeUrl = process.env.IDOL_BBQ_CODEX_MCP_BRIDGE_URL
    const originalBridgeToken = process.env.IDOL_BBQ_CODEX_MCP_BRIDGE_TOKEN
    process.env.IDOL_BBQ_CODEX_MCP_ENABLED = '1'
    process.env.IDOL_BBQ_CODEX_MCP_COMMAND = process.execPath
    process.env.IDOL_BBQ_CODEX_MCP_ARGS_JSON = JSON.stringify([script])
    delete process.env.IDOL_BBQ_CODEX_MCP_BRIDGE_URL
    delete process.env.IDOL_BBQ_CODEX_MCP_BRIDGE_TOKEN

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

        const statusResponse = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/agent/codex/status', {
                headers: {
                    Authorization: 'Bearer test-secret',
                },
            }),
            { timeout: () => undefined },
            'test-secret',
        )
        expect(statusResponse.status).toBe(200)
        const status = await statusResponse.json()
        expect(status).toMatchObject({
            service: 'codex-mcp',
            enabled: true,
            available: true,
            mode: 'stdio',
        })
        expect(status.tools.map((tool: any) => tool.name)).toEqual(['codex', 'codex-reply'])

        const runResponse = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/agent/codex/run', {
                method: 'POST',
                headers: {
                    Authorization: 'Bearer test-secret',
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    prompt: 'inspect project',
                    timeout_ms: 2_000,
                }),
            }),
            { timeout: () => undefined },
            'test-secret',
        )
        expect(runResponse.status).toBe(200)
        expect(await runResponse.json()).toMatchObject({
            success: true,
            thread_id: 'thread-api',
            content: 'codex:inspect project',
        })
    } finally {
        if (originalEnabled === undefined) delete process.env.IDOL_BBQ_CODEX_MCP_ENABLED
        else process.env.IDOL_BBQ_CODEX_MCP_ENABLED = originalEnabled
        if (originalCommand === undefined) delete process.env.IDOL_BBQ_CODEX_MCP_COMMAND
        else process.env.IDOL_BBQ_CODEX_MCP_COMMAND = originalCommand
        if (originalArgs === undefined) delete process.env.IDOL_BBQ_CODEX_MCP_ARGS_JSON
        else process.env.IDOL_BBQ_CODEX_MCP_ARGS_JSON = originalArgs
        if (originalBridgeUrl === undefined) delete process.env.IDOL_BBQ_CODEX_MCP_BRIDGE_URL
        else process.env.IDOL_BBQ_CODEX_MCP_BRIDGE_URL = originalBridgeUrl
        if (originalBridgeToken === undefined) delete process.env.IDOL_BBQ_CODEX_MCP_BRIDGE_TOKEN
        else process.env.IDOL_BBQ_CODEX_MCP_BRIDGE_TOKEN = originalBridgeToken
        rmSync(root, { recursive: true, force: true })
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

test('APIManager task list redacts notification signal payload identity fields', async () => {
    const originalTaskList = DB.TaskQueue.list

    ;(DB.TaskQueue as any).list = async () => [
        {
            id: 1,
            type: DB.TaskQueue.TYPE.NotificationSignal,
            status: 'completed',
            source_ref: 'notification:instagram:private-member',
            idempotency_key: 'private-idempotency-key',
            last_error: 'private last error',
            payload: {
                schema_version: 1,
                mode: 'shadow',
                platform: 'instagram',
                event_key: 'private-event-key',
                source_ref: 'notification:instagram:private-member',
                received_at: 1_800_000_000,
                notification: {
                    notification_id: 'private-notification-id',
                    post_id: 'private-post-id',
                    url: 'https://www.instagram.com/private-member/p/private-post-id/',
                    source_user_id: 'private-source-user',
                    username: 'private-member',
                    title_hash: 'private-title-hash',
                    title_length: 12,
                },
                matched_crawlers: [
                    {
                        crawler_id: 'ig-a',
                        crawler_name: 'Instagram A',
                        reason: 'identity',
                    },
                ],
                would_trigger_crawlers: false,
            },
        },
    ]

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
            new URL('http://localhost/api/tasks?type=notification_signal'),
        )
        expect(response.status).toBe(200)
        const tasks = await response.json()
        const serialized = JSON.stringify(tasks)

        expect(tasks[0]).toMatchObject({
            source_ref: '[redacted]',
            idempotency_key: '[redacted]',
            last_error: '[redacted]',
            payload: {
                schema_version: 1,
                platform: 'instagram',
                event_key_present: true,
                source_ref_present: true,
                matched_crawler_count: 1,
                notification: {
                    has_url: true,
                    has_notification_id: true,
                    has_post_id: true,
                    has_source_user_id: true,
                    has_username: true,
                    title_length: 12,
                },
            },
        })
        expect(serialized).not.toContain('private-member')
        expect(serialized).not.toContain('private-event-key')
        expect(serialized).not.toContain('private-notification-id')
        expect(serialized).not.toContain('private-post-id')
        expect(serialized).not.toContain('private-source-user')
        expect(serialized).not.toContain('private-title-hash')
    } finally {
        ;(DB.TaskQueue as any).list = originalTaskList
    }
})

test('APIManager task list redacts non-notification payloads and metadata', async () => {
    const originalTaskList = DB.TaskQueue.list

    ;(DB.TaskQueue as any).list = async () => [
        {
            id: 1,
            type: DB.TaskQueue.TYPE.ArticleSimulate,
            status: 'completed',
            source_ref: '1:private-article',
            idempotency_key: 'private-idempotency-key',
            last_error: 'private last error',
            result_summary: 'private simulated result summary',
            payload: {
                platform: 1,
                a_id: 'private-article',
                u_id: 'private-member',
                username: 'private username',
                content: 'private simulated content',
                url: 'https://example.test/private-article',
                mediaUrls: ['https://example.test/private-media.jpg'],
                processWithCrawler: true,
                crawlerName: 'private crawler',
            },
        },
        {
            id: 2,
            type: DB.TaskQueue.TYPE.ProcessorRun,
            status: 'failed',
            source_ref: 'manual:text',
            payload: {
                processorId: 'private processor',
                action: 'plan',
                text: 'private processor text',
                scheduleUrl: 'https://scheduler.example/private',
                scheduleApiKey: 'private-schedule-api-key',
            },
        },
    ]

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

        const response = await (manager as any).handleTasks(new URL('http://localhost/api/tasks?limit=20'))
        expect(response.status).toBe(200)
        const tasks = await response.json()
        const serialized = JSON.stringify(tasks)

        expect(tasks[0]).toMatchObject({
            source_ref: '[redacted]',
            idempotency_key: '[redacted]',
            last_error: '[redacted]',
            result_summary: {
                redacted_summary: true,
                summary_present: true,
                summary_type: 'string',
                summary_length: 'private simulated result summary'.length,
            },
            payload: {
                redacted_payload: true,
                content_present: true,
                url_present: true,
                media_url_count: 1,
                crawler_name_present: true,
            },
        })
        expect(tasks[1]).toMatchObject({
            source_ref: '[redacted]',
            payload: {
                redacted_payload: true,
                processor_id_present: true,
                action: 'plan',
                text_present: true,
                schedule_url_present: true,
                schedule_api_key_present: true,
            },
        })
        expect(serialized).not.toContain('private simulated content')
        expect(serialized).not.toContain('private processor text')
        expect(serialized).not.toContain('private-schedule-api-key')
        expect(serialized).not.toContain('scheduler.example')
        expect(serialized).not.toContain('private-member')
        expect(serialized).not.toContain('private-article')
        expect(serialized).not.toContain('private crawler')
        expect(serialized).not.toContain('private processor')
        expect(serialized).not.toContain('private last error')
        expect(serialized).not.toContain('private simulated result summary')
    } finally {
        ;(DB.TaskQueue as any).list = originalTaskList
    }
})

test('APIManager processor run list redacts input output and source refs', async () => {
    const originalProcessorRunList = DB.ProcessorRun.list
    const calls: any[] = []

    ;(DB.ProcessorRun as any).list = async (limit: number, sourceRef?: string) => {
        calls.push({ limit, sourceRef })
        return [
            {
                id: 1,
                processor_id: 'processor-a',
                action: 'plan',
                source_type: 'text',
                source_ref: 'manual:private-source',
                status: 'completed',
                input: {
                    request: {
                        text: 'private request text',
                        scheduleUrl: 'https://scheduler.example/private',
                        scheduleApiKey: 'private-schedule-api-key',
                    },
                    text: 'private processor input text',
                },
                output: {
                    raw: 'private raw output',
                    parsed: {
                        title: 'private parsed title',
                    },
                    selected: {
                        payload: 'private selected payload',
                    },
                    result_key: 'plans',
                    schedules: [
                        {
                            body: 'private schedule response',
                        },
                    ],
                },
                error: 'private error',
            },
        ]
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

        const response = await (manager as any).handleProcessorRuns(
            new URL('http://localhost/api/processor-runs?limit=999&source_ref=manual%3Aprivate-source'),
        )
        expect(response.status).toBe(200)
        const runs = await response.json()
        const serialized = JSON.stringify(runs)

        expect(calls).toEqual([{ limit: 200, sourceRef: 'manual:private-source' }])
        expect(runs[0]).toMatchObject({
            source_ref: '[redacted]',
            input: {
                redacted_input: true,
                request: {
                    has_request: true,
                    text_present: true,
                    schedule_url_present: true,
                    schedule_api_key_present: true,
                },
                text_present: true,
            },
            output: {
                redacted_output: true,
                raw_present: true,
                parsed_present: true,
                selected_present: true,
                schedule_count: 1,
            },
            error: '[redacted]',
        })
        expect(serialized).not.toContain('manual:private-source')
        expect(serialized).not.toContain('private request text')
        expect(serialized).not.toContain('private processor input text')
        expect(serialized).not.toContain('private raw output')
        expect(serialized).not.toContain('private parsed title')
        expect(serialized).not.toContain('private selected payload')
        expect(serialized).not.toContain('private-schedule-api-key')
        expect(serialized).not.toContain('scheduler.example')
        expect(serialized).not.toContain('private schedule response')
        expect(serialized).not.toContain('private error')
    } finally {
        ;(DB.ProcessorRun as any).list = originalProcessorRunList
    }
})

test('APIManager outbound message list redacts delivery identifiers and provider details', async () => {
    const originalOutboundList = DB.OutboundMessage.list
    const calls: any[] = []

    ;(DB.OutboundMessage as any).list = async (limit: number, status?: string) => {
        calls.push({ limit, status })
        return [
            {
                id: 1,
                idempotency_key: 'article:remote-private-target:1:remote-private-article',
                route_key: 'graph:remote-private-crawler:formatter:remote-private-target',
                target_id: 'remote-private-target',
                target_platform: 'QQ',
                task_kind: 'article',
                article_key: '1:remote-private-article',
                synthetic_key: 'remote-private-window',
                payload_hash: 'remote-private-payload-hash',
                status: 'dry_run',
                provider_message_ids: {
                    send_mode: 'capture',
                    target_id: 'remote-private-target',
                    text_count: 1,
                    text_length: 31,
                    media_count: 1,
                    article_key: '1:remote-private-article',
                    outbound_key: 'article:remote-private-target:1:remote-private-article',
                    capture_result: {
                        kind: 'file',
                        destination: '/tmp/remote-private-capture.jsonl',
                        ok: true,
                    },
                },
                segment_results: {
                    diagnostic: 'suppressed_payload_drift',
                    previous_segment_results: [{ message_id: 'remote-private-message-id' }],
                },
                last_error: 'remote-private-send-error',
            },
        ]
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

        const response = await (manager as any).handleOutboundMessages(
            new URL('http://localhost/api/outbound-messages?limit=999&status=dry_run'),
        )
        expect(response.status).toBe(200)
        const messages = await response.json()
        const serialized = JSON.stringify(messages)

        expect(calls).toEqual([{ limit: 200, status: 'dry_run' }])
        expect(messages[0]).toMatchObject({
            idempotency_key: '[redacted]',
            route_key: '[redacted]',
            target_id: '[redacted]',
            article_key: '[redacted]',
            synthetic_key: '[redacted]',
            payload_hash: '[redacted]',
            provider_message_ids: {
                redacted_value: true,
                send_mode: 'capture',
                text_count: 1,
                text_length: 31,
                media_count: 1,
                target_id_present: true,
                article_key_present: true,
                outbound_key_present: true,
                capture_result: {
                    redacted_value: true,
                    kind: 'file',
                    ok: true,
                    destination_present: true,
                },
            },
            segment_results: {
                redacted_value: true,
                diagnostic_present: true,
            },
            last_error: {
                redacted_text: true,
                text_present: true,
                text_length: 'remote-private-send-error'.length,
            },
        })
        expect(serialized).not.toContain('remote-private-target')
        expect(serialized).not.toContain('remote-private-article')
        expect(serialized).not.toContain('remote-private-window')
        expect(serialized).not.toContain('remote-private-payload-hash')
        expect(serialized).not.toContain('remote-private-capture')
        expect(serialized).not.toContain('remote-private-message-id')
        expect(serialized).not.toContain('remote-private-send-error')
    } finally {
        ;(DB.OutboundMessage as any).list = originalOutboundList
    }
})

test('APIManager target health redacts target ids and send details', async () => {
    const originalTargetHealthList = DB.TargetHealth.list

    ;(DB.TargetHealth as any).list = async () => [
        {
            target_id: 'remote-private-target',
            provider: 'QQ',
            status: 'error',
            last_send_status: 'failed',
            last_provider_code: '500',
            disabled_reason: 'remote-private-disabled-reason',
            details: {
                route_key: 'graph:remote-private-target',
                article_key: '1:remote-private-article',
                target_id: 'remote-private-target',
                status: 'failed',
                data: {
                    code: 500,
                    message: 'remote-private-provider-message',
                },
            },
        },
    ]

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

        const response = await (manager as any).handleTargetHealth()
        expect(response.status).toBe(200)
        const health = await response.json()
        const serialized = JSON.stringify(health)

        expect(health[0]).toMatchObject({
            target_id: '[redacted]',
            provider: 'QQ',
            status: 'error',
            disabled_reason: {
                redacted_text: true,
                text_present: true,
                text_length: 'remote-private-disabled-reason'.length,
            },
            details: {
                redacted_value: true,
                status: 'failed',
                provider_code: 500,
                target_id_present: true,
                article_key_present: true,
            },
        })
        expect(serialized).not.toContain('remote-private-target')
        expect(serialized).not.toContain('remote-private-article')
        expect(serialized).not.toContain('remote-private-disabled-reason')
        expect(serialized).not.toContain('remote-private-provider-message')
    } finally {
        ;(DB.TargetHealth as any).list = originalTargetHealthList
    }
})

test('APIManager notification signal summary returns no-secret observation metrics', async () => {
    const originalTaskList = DB.TaskQueue.list
    const calls: any[] = []

    ;(DB.TaskQueue as any).list = async (limit: number, filters: unknown) => {
        calls.push({ limit, filters })
        return [
            {
                id: 1,
                status: 'completed',
                created_at: 1000,
                payload: {
                    schema_version: 1,
                    mode: 'shadow',
                    platform: 'instagram',
                    event_key: 'private-event-key',
                    source_ref: 'notification:instagram:private-member',
                    received_at: 995,
                    notification: {
                        title: 'private notification title',
                    },
                    matched_crawlers: [
                        {
                            crawler_id: 'ig-a',
                            crawler_name: 'Instagram A',
                            reason: 'identity',
                        },
                    ],
                    would_trigger_crawlers: false,
                },
            },
        ]
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

        const response = await (manager as any).handleNotificationSignalSummary(
            new URL('http://localhost/api/notification-signals/summary?limit=999'),
        )
        expect(response.status).toBe(200)
        const payload = await response.json()
        const serialized = JSON.stringify(payload)

        expect(calls).toEqual([
            {
                limit: 200,
                filters: {
                    type: 'notification_signal',
                },
            },
        ])
        expect(payload.sample).toMatchObject({
            limit: 200,
            task_count: 1,
            parsed_record_count: 1,
        })
        expect(payload.platform_counts).toEqual({ instagram: 1 })
        expect(payload.match_reason_counts).toEqual({ identity: 1 })
        expect(payload.counts.raw_text_field_count).toBe(1)
        expect(payload.matched_crawlers).toEqual([
            {
                crawler_id: 'ig-a',
                crawler_name: 'Instagram A',
                count: 1,
            },
        ])
        expect(payload.diagnostic_codes).toContain('notification_signal_raw_text_fields_present')
        expect(serialized).not.toContain('private notification title')
        expect(serialized).not.toContain('private-event-key')
        expect(serialized).not.toContain('private-member')
    } finally {
        ;(DB.TaskQueue as any).list = originalTaskList
    }
})

test('APIManager runtime logs exposes only redacted log metadata and lines', async () => {
    const logsDir = join(CACHE_DIR_ROOT, 'logs')
    mkdirSync(logsDir, { recursive: true })
    const fileName = `codex-runtime-log-redaction-${Date.now()}-${Math.random().toString(36).slice(2)}.log`
    const filePath = join(logsDir, fileName)
    writeFileSync(
        filePath,
        [
            'safe startup line',
            `Authorization: Bearer private-token api_key=private-api-key source_ref=notification:x:private-member target_id=remote-private-target url=https://example.test/private ${filePath}`,
        ].join('\n'),
        'utf8',
    )
    const future = new Date(Date.now() + 60_000)
    utimesSync(filePath, future, future)

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

        const response = await (manager as any).handleRuntimeLogs(new URL('http://localhost/api/runtime/logs?limit=5'))
        expect(response.status).toBe(200)
        const payload = await response.json()
        const serialized = JSON.stringify(payload)

        expect(payload).toMatchObject({
            file: fileName,
            file_path: '[redacted]',
            redacted: true,
        })
        expect(serialized).toContain('[redacted-url]')
        expect(serialized).toContain('[redacted-path]')
        expect(serialized).not.toContain('private-token')
        expect(serialized).not.toContain('private-api-key')
        expect(serialized).not.toContain('private-member')
        expect(serialized).not.toContain('remote-private-target')
        expect(serialized).not.toContain('example.test/private')
        expect(serialized).not.toContain(filePath)
    } finally {
        rmSync(filePath, { force: true })
    }
})

test('APIManager article endpoints preserve content while redacting host paths and secrets', async () => {
    const originalArticleQuery = DB.Article.query
    const originalGetSingleArticle = DB.Article.getSingleArticle

    ;(DB.Article as any).query = async () => [
        {
            id: 1,
            platform: Platform.X,
            a_id: 'article-a',
            content: 'visible article content',
            url: 'https://example.test/article-a',
            media: [{ type: 'photo', url: 'https://cdn.example.test/a.jpg', path: '/tmp/private-a.jpg' }],
            extra: { api_key: 'private-api-key' },
        },
    ]
    ;(DB.Article as any).getSingleArticle = async () => ({
        id: 1,
        platform: Platform.X,
        a_id: 'article-a',
        content: 'visible article content',
        url: 'https://example.test/article-a',
        media: [{ type: 'photo', url: 'https://cdn.example.test/a.jpg', path: '/tmp/private-a.jpg' }],
        extra: { nested: { cookie: 'private-cookie', localPath: '/home/sumie/private-extra.json' } },
    })

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

        const listResponse = await (manager as any).handleArticleList(new URL('http://localhost/api/articles?limit=10'))
        const detailResponse = await (manager as any).handleArticleView(new URL('http://localhost/api/articles/x/1'))
        const listPayload = await listResponse.json()
        const detailPayload = await detailResponse.json()
        const serialized = JSON.stringify({ listPayload, detailPayload })

        expect(listResponse.headers.get('Cache-Control')).toBe('no-store')
        expect(listPayload[0].content).toBe('visible article content')
        expect(listPayload[0].url).toBe('https://example.test/article-a')
        expect(listPayload[0].media[0].url).toBe('https://cdn.example.test/a.jpg')
        expect(listPayload[0].media[0].path).toBe('[redacted]')
        expect(detailPayload.media[0].path).toBe('[redacted]')
        expect(detailPayload.extra.nested.cookie).toBe('[redacted]')
        expect(detailPayload.extra.nested.localPath).toBe('[redacted]')
        expect(serialized).not.toContain('/tmp/private-a.jpg')
        expect(serialized).not.toContain('/home/sumie/private-extra.json')
        expect(serialized).not.toContain('private-api-key')
        expect(serialized).not.toContain('private-cookie')
    } finally {
        ;(DB.Article as any).query = originalArticleQuery
        ;(DB.Article as any).getSingleArticle = originalGetSingleArticle
    }
})

test('APIManager records notification signals in api-only shadow mode without dispatching crawlers', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const taskAdds: any[] = []
    const statusUpdates: any[] = []

    ;(DB.TaskQueue as any).add = async (type: string, payload: any, executeAt: number, meta: any) => {
        taskAdds.push({ type, payload, executeAt, meta })
        return { id: 91, status: 'pending' }
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
                            id: 'ig-sakura',
                            name: 'Instagram Sakura',
                            origin: 'https://www.instagram.com',
                            paths: ['/sakura.member/'],
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    emitter: {
                        emit: () => {
                            throw new Error('notification signal must not dispatch')
                        },
                    },
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

        const response = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/actions/notification-signals/ingest', {
                method: 'POST',
                headers: {
                    Authorization: 'Bearer test-secret',
                },
                body: JSON.stringify({
                    platform: 'instagram',
                    username: 'sakura.member',
                    title: 'private notification title',
                    body: 'private notification body',
                    received_at: 1_800_000_000,
                }),
            }),
            { timeout: () => undefined },
            'test-secret',
        )

        expect(response.status).toBe(200)
        const payload = await response.json()
        expect(payload).toMatchObject({
            success: true,
            mode: 'shadow',
            platform: 'instagram',
            taskQueueId: 91,
            matched_crawler_count: 1,
            matched_crawlers: [
                {
                    crawler_id: 'ig-sakura',
                    crawler_name: 'Instagram Sakura',
                    reason: 'identity',
                },
            ],
            would_trigger_crawlers: false,
        })
        const serialized = JSON.stringify(payload)
        expect(serialized).not.toContain('sakura.member')
        expect(serialized).not.toContain('notification:instagram')
        expect(serialized).not.toContain('private notification title')
        expect(serialized).not.toContain('private notification body')

        expect(taskAdds).toHaveLength(1)
        expect(taskAdds[0].type).toBe('notification_signal')
        expect(taskAdds[0].payload.notification.title).toBeUndefined()
        expect(taskAdds[0].payload.notification.body).toBeUndefined()
        expect(taskAdds[0].payload.notification.title_hash).toHaveLength(64)
        expect(taskAdds[0].meta).toMatchObject({
            source_ref: 'notification:instagram:sakura.member',
            action_type: 'notification_signal',
        })
        expect(statusUpdates).toEqual([
            {
                id: 91,
                status: 'completed',
                meta: {
                    result_summary: 'notification signal shadowed: instagram, matches=1',
                },
            },
        ])
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
    }
})

test('APIManager ignores notification signals when notification shadow intake is disabled', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const taskAdds: any[] = []
    const statusUpdates: any[] = []

    ;(DB.TaskQueue as any).add = async (type: string, payload: any, executeAt: number, meta: any) => {
        taskAdds.push({ type, payload, executeAt, meta })
        return { id: 92, status: 'pending' }
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
                    notification_signals: {
                        mode: 'disabled',
                    },
                    crawlers: [
                        {
                            id: 'x-staff',
                            name: 'X staff',
                            origin: 'https://x.com',
                            paths: ['/227_staff'],
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    emitter: {
                        emit: () => {
                            throw new Error('disabled notification signal must not dispatch')
                        },
                    },
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

        const response = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/actions/notification-signals/ingest', {
                method: 'POST',
                headers: {
                    Authorization: 'Bearer test-secret',
                },
                body: JSON.stringify({
                    platform: 'x',
                    username: '227_staff',
                    notificationId: 'x-notification-1',
                    title: 'private notification title',
                }),
            }),
            { timeout: () => undefined },
            'test-secret',
        )

        expect(response.status).toBe(202)
        expect(await response.json()).toMatchObject({
            success: false,
            mode: 'disabled',
            platform: 'x',
            matched_crawler_count: 1,
            matched_crawlers: [
                {
                    crawler_id: 'x-staff',
                    crawler_name: 'X staff',
                    reason: 'identity',
                },
            ],
            would_trigger_crawlers: false,
        })
        expect(taskAdds).toEqual([])
        expect(statusUpdates).toEqual([])
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
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

test('APIManager queues manual website crawler run with one-shot website override', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const dispatched: any[] = []
    const queuePayloads: any[] = []

    ;(DB.TaskQueue as any).add = async (_type: string, payload: any) => {
        queuePayloads.push(payload)
        return { id: 90, payload }
    }
    ;(DB.TaskQueue as any).updateStatus = async () => undefined

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    crawlers: [
                        {
                            name: 'website-crawler',
                            websites: ['https://example.com/list'],
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
                body: JSON.stringify({
                    name: 'website-crawler',
                    websites: [
                        'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/450790',
                        'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/450790',
                    ],
                }),
            }),
        )
        const payload = await response.json()

        expect(payload).toMatchObject({
            success: true,
            status: 'queued',
            crawler: 'website-crawler',
            websites: ['https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/450790'],
        })
        expect(queuePayloads[0]).toMatchObject({
            crawler: 'website-crawler',
            websites: ['https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/450790'],
        })
        expect(dispatched[0].task.data).toMatchObject({
            name: 'website-crawler',
            websites: ['https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/450790'],
        })
        expect(dispatched[0].task.meta).toMatchObject({
            task_queue_id: 90,
            task_queue_type: 'manual_crawler_run',
            manual_website_override_count: 1,
        })
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
    }
})

test('APIManager hot-upserts crawler schedules without runtime reload', async () => {
    const calls: any[] = []
    const scheduler = {
        upsertHotSchedule: (crawlerName: string, schedule: unknown) => {
            calls.push({ crawlerName, schedule })
            return {
                crawler: crawlerName,
                nextRunAt: 1777047600,
                nextRunAtIso: '2026-04-24T09:00:00.000Z',
            }
        },
        pokeSchedules: async () => {
            calls.push({ poke: true })
        },
    }
    const manager = new APIManager({
        getConfig: () =>
            ({
                api: {
                    secret: 'test-secret',
                },
            }) as any,
        getDeps: () =>
            ({
                spiderTaskScheduler: scheduler,
            }) as any,
    })

    const response = await (manager as any).handleCrawlerScheduleUpsert(
        new Request('http://localhost/api/schedules/crawlers/upsert', {
            method: 'POST',
            body: JSON.stringify({
                crawler: 'crawler-a',
                schedule: {
                    windows: [{ start: '18:05', end: '18:35', every_minutes: 15 }],
                },
            }),
        }),
    )
    const payload = await response.json()

    expect(payload).toMatchObject({
        success: true,
        schedule: {
            crawler: 'crawler-a',
            nextRunAt: 1777047600,
        },
    })
    expect(calls).toEqual([
        {
            crawlerName: 'crawler-a',
            schedule: {
                windows: [{ start: '18:05', end: '18:35', every_minutes: 15 }],
            },
        },
        { poke: true },
    ])
})

test('APIManager inserts temporary crawler schedule points into task_queue', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    const adds: any[] = []
    const scheduler = {
        pokeSchedules: async () => {
            adds.push({ poke: true })
        },
    }
    ;(DB.TaskQueue as any).add = async (type: string, payload: any, executeAt: number, meta: any) => {
        adds.push({ type, payload, executeAt, meta })
        return { id: 402, status: 'pending' }
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
                            name: 'website-crawler',
                            websites: ['https://example.com/list'],
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    spiderTaskScheduler: scheduler,
                }) as any,
        })

        const response = await (manager as any).handleCrawlerScheduleInsert(
            new Request('http://localhost/api/schedules/crawlers/insert', {
                method: 'POST',
                body: JSON.stringify({
                    crawler: 'website-crawler',
                    execute_at: 1893456000,
                    reason: 'operator spot check',
                    websites: ['https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/1'],
                    idempotency_key: 'operator-once',
                }),
            }),
        )
        const payload = await response.json()

        expect(payload).toMatchObject({
            success: true,
            status: 'pending',
            taskQueueId: 402,
            crawler: 'website-crawler',
            executeAt: 1893456000,
            websites: ['https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/1'],
            idempotencyKey: 'operator-once',
        })
        expect(adds[0]).toMatchObject({
            type: DB.TaskQueue.TYPE.ScheduledCrawlerRun,
            payload: {
                crawler: 'website-crawler',
                reason: 'operator spot check',
                websites: ['https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/1'],
            },
            executeAt: 1893456000,
            meta: {
                source_ref: 'website-crawler',
                action_type: 'crawl',
                idempotency_key: 'operator-once',
            },
        })
        expect(adds[1]).toEqual({ poke: true })
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
    }
})

test('APIManager rejects website override for non-website crawlers', async () => {
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
                    emit: () => true,
                },
            }) as any,
    })

    const response = await (manager as any).handleCrawlerRun(
        new Request('http://localhost/api/actions/crawlers/run', {
            method: 'POST',
            body: JSON.stringify({ name: 'crawler-a', website: 'https://example.com/list' }),
        }),
    )

    expect(response.status).toBe(400)
    expect(await response.text()).toBe('website override is only supported for website crawlers')
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

test('APIManager processor translate action does not process article digest twice', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus
    const originalProcessorRunCreate = DB.ProcessorRun.create
    const originalArticleGet = DB.Article.getSingleArticleByArticleCode
    const originalArticleUpdate = DB.Article.update
    const originalCreateProcessor = (processorRegistry as any).create
    const processInputs: string[] = []
    const processorRuns: any[] = []
    const articleUpdates: any[] = []

    ;(DB.TaskQueue as any).add = async () => ({ id: 78 })
    ;(DB.TaskQueue as any).updateStatus = async () => undefined
    ;(DB.ProcessorRun as any).create = async (data: any) => {
        processorRuns.push(data)
        return { id: 1, ...data }
    }
    ;(DB.Article as any).getSingleArticleByArticleCode = async () =>
        ({
            id: 12,
            a_id: 'post-1',
            platform: Platform.X,
            created_at: 1710000000,
            u_id: 'user-1',
            username: 'member',
            content: 'こんにちは',
        }) as any
    ;(DB.Article as any).update = async (id: number, platform: Platform, patch: any) => {
        articleUpdates.push({ id, platform, patch })
    }
    ;(processorRegistry as any).create = async () =>
        ({
            NAME: 'fake-translator',
            process: async (text: string) => {
                processInputs.push(text)
                return `translated:${text}`
            },
        }) as any

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    processors: [
                        {
                            id: 'processor-a',
                            name: 'processor-a',
                            provider: 'Fake',
                        },
                    ],
                }) as any,
            getDeps: () => ({}) as any,
        })

        const response = await (manager as any).handleProcessorRun(
            new Request('http://localhost/api/actions/processors/run', {
                method: 'POST',
                body: JSON.stringify({
                    processorId: 'processor-a',
                    action: 'translate',
                    platform: 'x',
                    a_id: 'post-1',
                }),
            }),
        )
        const payload = await response.json()

        expect(payload.success).toBe(true)
        expect(processInputs).toEqual(['こんにちは'])
        expect(articleUpdates).toHaveLength(1)
        expect(articleUpdates[0].patch).toMatchObject({
            translation: 'translated:こんにちは',
            translated_by: 'fake-translator',
        })
        expect(processorRuns[0].output.parsed).toMatchObject({
            action: 'translate',
            translated_by: 'fake-translator',
            chain_length: 1,
            updated: [
                {
                    id: 12,
                    a_id: 'post-1',
                    platform: Platform.X,
                    fields: ['translation', 'translated_by'],
                },
            ],
        })
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
        ;(DB.TaskQueue as any).updateStatus = originalTaskUpdateStatus
        ;(DB.ProcessorRun as any).create = originalProcessorRunCreate
        ;(DB.Article as any).getSingleArticleByArticleCode = originalArticleGet
        ;(DB.Article as any).update = originalArticleUpdate
        ;(processorRegistry as any).create = originalCreateProcessor
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
                    targetIds: ['bilibili-转帖', 'bilibili-转帖', ''],
                }),
            }),
        )

        expect(response.status).toBe(200)
        expect(await response.json()).toEqual({
            success: true,
            articleId: 162,
            crawlerName: '22/7官网FC抓取 - 日间轮询',
            targetIds: ['bilibili-转帖'],
        })
        expect(resendCalls).toHaveLength(1)
        expect(resendCalls[0][0]).toMatchObject({
            id: 162,
            a_id: '11230',
            platform: Platform.Website,
        })
        expect(resendCalls[0][1]).toBe('22/7官网FC抓取 - 日间轮询')
        expect(resendCalls[0][4]).toEqual({
            targetIds: ['bilibili-转帖'],
        })
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

test('APIManager QQ X-link endpoint sends existing X article to matching QQ group target', async () => {
    const originalArticleGet = DB.Article.getSingleArticleByArticleCode
    const sendCalls: any[] = []

    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) => {
        expect(a_id).toBe('2068528507651842559')
        expect(platform).toBe(Platform.X)
        return {
            id: 206,
            a_id,
            platform,
            content: 'original x text',
            url: 'https://x.com/kyu0817a/status/2068528507651842559',
            created_at: 1_777_777_777,
            media: [],
            extra: null,
        }
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
                            id: 'x-list',
                            name: 'x-list',
                            origin: 'https://x.com/i/lists/123',
                            paths: ['kyu0817a'],
                        },
                    ],
                    connections: {
                        'crawler-processor': {
                            'x-list': 'translator',
                        },
                    },
                    forward_targets: [
                        {
                            id: 'qq-1',
                            platform: 'qq',
                            cfg_platform: {
                                url: 'http://127.0.0.1:3001',
                                token: 'bot-token',
                                group_id: '999',
                            },
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    forwarderPools: {
                        sendImmediateXLinkArticle: async (...args: any[]) => {
                            sendCalls.push(args)
                            const providerResult: Record<string, unknown> = {}
                            providerResult.self = providerResult
                            return {
                                article_key: 'x:2068528507651842559',
                                translated_card: true,
                                target_ids: ['qq-1'],
                                sends: [
                                    {
                                        target_id: 'qq-1',
                                        part: 'card',
                                        result: {
                                            status: 'sent',
                                            providerResult,
                                        },
                                    },
                                ],
                            }
                        },
                    },
                }) as any,
        })

        const response = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/actions/qq/x-link', {
                method: 'POST',
                headers: {
                    Authorization: 'Bearer test-secret',
                },
                body: JSON.stringify({
                    post_type: 'message',
                    message_type: 'group',
                    group_id: '999',
                    message: [
                        {
                            type: 'text',
                            data: {
                                text: '看看 https://x.com/kyu0817a/status/2068528507651842559?s=46&t=abc',
                            },
                        },
                    ],
                }),
            }),
            {
                timeout: () => undefined,
            },
            'test-secret',
        )

        expect(response.status).toBe(200)
        expect(await response.json()).toMatchObject({
            success: true,
            x: {
                username: 'kyu0817a',
                statusId: '2068528507651842559',
                url: 'https://x.com/kyu0817a/status/2068528507651842559',
            },
            crawlerName: 'x-list',
            targetIds: ['qq-1'],
            result: {
                article_key: 'x:2068528507651842559',
                translated_card: true,
                target_ids: ['qq-1'],
                sends: [
                    {
                        target_id: 'qq-1',
                        part: 'card',
                        status: 'sent',
                    },
                ],
            },
        })
        expect(sendCalls).toHaveLength(1)
        expect(sendCalls[0][0]).toMatchObject({
            id: 206,
            a_id: '2068528507651842559',
            platform: Platform.X,
        })
        expect(sendCalls[0][1]).toEqual({
            crawlerName: 'x-list',
            targetIds: ['qq-1'],
            processorId: 'translator',
            badgeLabel: undefined,
        })
    } finally {
        ;(DB.Article as any).getSingleArticleByArticleCode = originalArticleGet
    }
})

test('APIManager QQ X-link endpoint hydrates missing DB article and sends immediately', async () => {
    const originalArticleGet = DB.Article.getSingleArticleByArticleCode
    const sendCalls: Array<any[]> = []
    const hydrateCalls: Array<any[]> = []
    let articleGetCalls = 0

    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) => {
        articleGetCalls += 1
        if (articleGetCalls === 1) {
            return null
        }
        return {
            id: 207,
            a_id,
            platform,
            content: 'hydrated x text',
            url: 'https://x.com/kyu0817a/status/2068528507651842559',
            created_at: 1_777_777_777,
            media: [],
            extra: null,
        }
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
                            id: 'x-list',
                            name: 'x-list',
                            origin: 'https://x.com/i/lists/123',
                            cfg_crawler: {
                                engine: 'api',
                            },
                        },
                    ],
                    processors: [
                        {
                            id: 'translator',
                            name: 'translator',
                            provider: 'noop',
                            api_key: 'test-key',
                        },
                    ],
                    connections: {
                        'crawler-processor': {
                            'x-list': 'translator',
                        },
                    },
                    forward_targets: [
                        {
                            id: 'qq-1',
                            platform: 'qq',
                            cfg_platform: {
                                url: 'http://127.0.0.1:3001',
                                token: 'bot-token',
                                group_id: '999',
                            },
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    forwarderPools: {
                        sendImmediateXLinkArticle: async (...args: any[]) => {
                            sendCalls.push(args)
                            return { sent: true }
                        },
                    },
                    spiderPools: {
                        hydrateArticleUrlForImmediate: async (...args: any[]) => {
                            hydrateCalls.push(args)
                            return [207]
                        },
                    },
                }) as any,
        })

        const response = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/actions/qq/x-link', {
                method: 'POST',
                headers: {
                    Authorization: 'Bearer test-secret',
                },
                body: JSON.stringify({
                    group_id: '999',
                    raw_message: 'https://x.com/kyu0817a/status/2068528507651842559',
                }),
            }),
            {
                timeout: () => undefined,
            },
            'test-secret',
        )

        expect(response.status).toBe(200)
        expect(await response.json()).toMatchObject({
            success: true,
            x: {
                username: 'kyu0817a',
                statusId: '2068528507651842559',
            },
            crawlerName: 'x-list',
            targetIds: ['qq-1'],
            hydrated: true,
            result: {
                sent: true,
            },
        })
        expect(hydrateCalls).toHaveLength(1)
        expect(hydrateCalls[0][0]).toBe('https://x.com/kyu0817a/status/2068528507651842559')
        expect(hydrateCalls[0][1]).toMatchObject({
            id: 'x-list',
            cfg_crawler: {
                engine: 'api',
                processor: {
                    id: 'translator',
                },
            },
        })
        expect(sendCalls).toHaveLength(1)
        expect(sendCalls[0][0]).toMatchObject({
            id: 207,
            a_id: '2068528507651842559',
            platform: Platform.X,
        })
        expect(sendCalls[0][1]).toEqual({
            crawlerName: 'x-list',
            targetIds: ['qq-1'],
            processorId: 'translator',
            badgeLabel: undefined,
        })
    } finally {
        ;(DB.Article as any).getSingleArticleByArticleCode = originalArticleGet
    }
})

test('APIManager QQ X-link endpoint reports hydrate failures as JSON', async () => {
    const originalArticleGet = DB.Article.getSingleArticleByArticleCode
    let sendCalled = false

    ;(DB.Article as any).getSingleArticleByArticleCode = async () => null

    try {
        const manager = new APIManager({
            getConfig: () =>
                ({
                    api: {
                        secret: 'test-secret',
                    },
                    crawlers: [
                        {
                            id: 'x-list',
                            name: 'x-list',
                            origin: 'https://x.com/i/lists/123',
                        },
                    ],
                    forward_targets: [
                        {
                            id: 'qq-1',
                            platform: 'qq',
                            cfg_platform: {
                                url: 'http://127.0.0.1:3001',
                                token: 'bot-token',
                                group_id: '999',
                            },
                        },
                    ],
                }) as any,
            getDeps: () =>
                ({
                    forwarderPools: {
                        sendImmediateXLinkArticle: async () => {
                            sendCalled = true
                        },
                    },
                    spiderPools: {
                        hydrateArticleUrlForImmediate: async () => {
                            throw new Error('X auth expired')
                        },
                    },
                }) as any,
        })

        const response = await (manager as any).dispatchApiRequest(
            new Request('http://localhost/api/actions/qq/x-link', {
                method: 'POST',
                headers: {
                    Authorization: 'Bearer test-secret',
                },
                body: JSON.stringify({
                    group_id: '999',
                    raw_message: 'https://x.com/kyu0817a/status/2068528507651842559',
                }),
            }),
            {
                timeout: () => undefined,
            },
            'test-secret',
        )

        expect(response.status).toBe(502)
        expect(await response.json()).toMatchObject({
            success: false,
            status: 'hydrate_failed',
            error: 'article_hydrate_failed',
            message: 'X auth expired',
            x: {
                username: 'kyu0817a',
                statusId: '2068528507651842559',
            },
            targetIds: ['qq-1'],
        })
        expect(sendCalled).toBeFalse()
    } finally {
        ;(DB.Article as any).getSingleArticleByArticleCode = originalArticleGet
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
                            const error = new Error(
                                'Browser session x-main is missing required x cookies: auth_token, ct0',
                            )
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
