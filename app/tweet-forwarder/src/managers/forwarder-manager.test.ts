import { expect, test } from 'bun:test'
import EventEmitter from 'events'
import { ForwarderPools, ForwarderTaskScheduler, buildAutoBoundForwarderTaskData, resolveBatchTargetIds } from './forwarder-manager'
import { TaskScheduler } from '@/utils/base'
import { fileURLToPath } from 'url'
import { Forwarder } from '@/middleware/forwarder/base'
import DB from '@/db'

process.env.FONTS_DIR = fileURLToPath(new URL('../../../../assets/fonts', import.meta.url))

test('resolveBatchTargetIds skips targets with bypass_batch enabled', () => {
    const targetIds = resolveBatchTargetIds(
        ['formatter-a', 'formatter-b'],
        {
            'formatter-a': ['group-1', 'group-2'],
            'formatter-b': ['group-2', 'group-3'],
        },
        [
            {
                id: 'group-1',
                platform: 'qq' as any,
                cfg_platform: {
                    group_id: '1',
                    token: '',
                    url: 'http://127.0.0.1:3001',
                } as any,
            },
            {
                id: 'group-2',
                platform: 'qq' as any,
                cfg_platform: {
                    group_id: '2',
                    token: '',
                    url: 'http://127.0.0.1:3001',
                    bypass_batch: true,
                } as any,
            },
            {
                id: 'group-3',
                platform: 'qq' as any,
                cfg_platform: {
                    group_id: '3',
                    token: '',
                    url: 'http://127.0.0.1:3001',
                } as any,
            },
        ] as any,
    )

    expect(targetIds).toEqual(['group-1', 'group-3'])
})

test('buildAutoBoundForwarderTaskData keeps crawler identity and merges media config from matching template', () => {
    const { matchedForwarder, forwarderTaskData } = buildAutoBoundForwarderTaskData(
        {
            name: 'YouTube抓取',
            origin: 'https://www.youtube.com',
            paths: ['@227SMEJ'],
        } as any,
        {
            cfg_forwarder: {
                render_type: 'text',
            } as any,
            forwarders: [
                {
                    name: 'YouTube视频模板',
                    origin: 'https://www.youtube.com',
                    cfg_forwarder: {
                        render_type: 'text-compact',
                        media: {
                            type: 'no-storage',
                            use: {
                                tool: 'yt-dlp',
                                path: '/app/tools/bin/yt-dlp',
                            },
                        },
                    },
                },
            ] as any,
        },
    )

    expect(matchedForwarder.name).toBe('YouTube视频模板')
    expect(forwarderTaskData.name).toBe('YouTube抓取')
    expect(forwarderTaskData.origin).toBe('https://www.youtube.com')
    expect(forwarderTaskData.cfg_forwarder?.media).toEqual({
        type: 'no-storage',
        use: {
            tool: 'yt-dlp',
            path: '/app/tools/bin/yt-dlp',
        },
    })
})

test('ForwarderTaskScheduler dispatches immediate tasks with article_ids_by_url from spider results', () => {
    const emitter = new EventEmitter()
    const scheduler = new ForwarderTaskScheduler(
        {
            cfg_forwarder: {
                render_type: 'img-tag',
            } as any,
            forwarders: [
                {
                    name: 'X图文模板',
                    origin: 'https://x.com',
                    cfg_forwarder: {
                        media: {
                            type: 'no-storage',
                            use: {
                                tool: 'default',
                            },
                        },
                    },
                },
            ] as any,
            connections: {
                'crawler-formatter': {},
                'crawler-processor': {},
                'processor-formatter': {},
                'formatter-target': {},
            } as any,
            crawlers: [
                {
                    name: '22/7-cast-成员统一列表',
                    origin: 'https://x.com',
                    paths: ['i/lists/1936785344072151389'],
                },
            ] as any,
            formatters: [],
            forward_targets: [],
        },
        emitter,
    )
    const dispatched: any[] = []
    emitter.on(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, (payload) => {
        dispatched.push(payload)
    })

    ;(scheduler as any).onSpiderTaskFinished({
        taskId: 'spider-1',
        crawlerName: '22/7-cast-成员统一列表',
        result: [
            {
                task_type: 'article',
                url: 'https://x.com/i/lists/1936785344072151389',
                data: [101, 102],
            },
            {
                task_type: 'follows',
                url: 'https://x.com/i/lists/1936785344072151389',
                data: [999],
            },
        ],
    })

    expect(dispatched).toHaveLength(1)
    expect(dispatched[0].taskId).toBe('spider-spider-1')
    expect(dispatched[0].task.data.name).toBe('22/7-cast-成员统一列表')
    expect(dispatched[0].task.data.article_ids_by_url).toEqual({
        'https://x.com/i/lists/1936785344072151389': [101, 102],
    })
    expect(dispatched[0].task.data.cfg_forwarder?.media).toEqual({
        type: 'no-storage',
        use: {
            tool: 'default',
        },
    })
})

test('ForwarderPools resendArticle reuses crawler template media config', async () => {
    const pools = new ForwarderPools(
        {
            forward_targets: [],
            cfg_forward_target: {} as any,
            connections: {
                'crawler-formatter': {},
                'crawler-processor': {},
                'processor-formatter': {},
                'formatter-target': {},
            } as any,
            formatters: [],
            cfg_forwarder: {
                render_type: 'text',
            } as any,
            forwarders: [
                {
                    name: 'X图文模板',
                    origin: 'https://x.com',
                    cfg_forwarder: {
                        media: {
                            type: 'no-storage',
                            use: {
                                tool: 'default',
                            },
                        },
                    },
                },
            ] as any,
            crawlers: [
                {
                    name: '22/7-cast-成员统一列表',
                    origin: 'https://x.com',
                    paths: ['i/lists/1936785344072151389'],
                },
            ] as any,
        },
        new EventEmitter(),
    )

    let capturedFormatterConfig: any
    ;(pools as any).resolveForwardingPaths = (
        _crawlerName: string,
        formatterConfig: any,
    ) => {
        capturedFormatterConfig = formatterConfig
        return [
            {
                formatterConfig,
                targets: [],
                source: 'graph',
                formatterName: 'X图文模板',
            },
        ]
    }
    ;(pools as any).sendArticles = async () => undefined

    await pools.resendArticle(
        {
            id: 1,
            a_id: '1888888888888',
            platform: 1,
        } as any,
        '22/7-cast-成员统一列表',
        {
            render_type: 'img-tag',
        } as any,
    )

    expect(capturedFormatterConfig.render_type).toBe('img-tag')
    expect(capturedFormatterConfig.media).toEqual({
        type: 'no-storage',
        use: {
            tool: 'default',
        },
    })
})

test('ForwarderPools force resend bypasses block checks but still applies text transforms', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
            return
        }
    }

    const pools = new ForwarderPools(
        {
            forward_targets: [],
            cfg_forward_target: {} as any,
            connections: {} as any,
            formatters: [],
            cfg_forwarder: {
                render_type: 'text',
            } as any,
            forwarders: [],
            crawlers: [],
        },
        new EventEmitter(),
    )

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            replace_regex: [['hello', 'hi']],
        } as any,
        'target-1',
    )

    let cleanupCalled = false
    ;(pools as any).renderService = {
        process: async () => ({
            text: 'hello world',
            mediaFiles: [],
        }),
        cleanup: () => {
            cleanupCalled = true
        },
    }

    const originalSave = DB.ForwardBy.save
    ;(DB.ForwardBy as any).save = async () => undefined

    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-11230',
            [
                {
                    id: 162,
                    a_id: '11230',
                    platform: 5,
                    created_at: Math.floor(Date.now() / 1000) - 40 * 3600,
                    ref: null,
                },
            ],
            [
                {
                    forwarder: target,
                    runtime_config: undefined,
                },
            ],
            {
                render_type: 'text-compact',
            } as any,
            { forceSend: true },
        )
    } finally {
        ;(DB.ForwardBy as any).save = originalSave
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts).toEqual(['hi world'])
    expect(target.sent[0]?.props?.forceSend).toBe(true)
    expect(cleanupCalled).toBe(true)
})
