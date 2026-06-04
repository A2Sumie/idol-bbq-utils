import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import { APIManager } from './api-manager'

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

test('APIManager resend infers website crawler platform from websites config', async () => {
    const originalGetSingleArticle = DB.Article.getSingleArticle
    const originalTaskAdd = DB.TaskQueue.add
    const originalTaskUpdateStatus = DB.TaskQueue.updateStatus

    const resendCalls: any[] = []

    ;(DB.Article as any).getSingleArticle = async () =>
        ({
            id: 162,
            a_id: '11230',
            platform: Platform.Website,
        }) as any
    ;(DB.TaskQueue as any).add = async () => ({ id: 991 })
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
