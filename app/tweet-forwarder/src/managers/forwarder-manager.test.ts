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

test('ForwarderPools resolves article subscribers as inline forwarding paths', () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        protected async realSend(): Promise<any> {
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
    const target = new RecordingForwarder({} as any, 'target-inline')
    ;(pools as any).forward_to.set('target-inline', target)

    const paths = (pools as any).resolveForwardingPaths(
        'crawler-a',
        { render_type: 'text-card' },
        { replace_regex: [['foo', 'bar']] },
        {},
        undefined,
        {
            subscribers: [
                {
                    id: 'target-inline',
                    cfg_forward_target: {
                        block_until: '1h',
                    },
                },
            ],
        },
    )

    expect(paths).toHaveLength(1)
    expect(paths[0].source).toBe('inline')
    expect(paths[0].targets[0].forwarder.id).toBe('target-inline')
    expect(paths[0].targets[0].runtime_config).toEqual({
        replace_regex: [['foo', 'bar']],
        block_until: '1h',
    })
})

test('Forwarder block_rules once.media allows the first media article then blocks matching articles in the window', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        protected async realSend(): Promise<any> {
            return
        }
    }

    const forwarder = new RecordingForwarder(
        {
            block_rules: [
                {
                    platform: 1,
                    sub_type: ['retweet'],
                    block_type: 'once.media',
                    block_until: '6h',
                },
            ],
        } as any,
        'target-block',
    )

    const firstArticle = {
        platform: 1,
        a_id: 'rt-1',
        type: 'retweet',
        has_media: true,
        created_at: Math.floor(Date.now() / 1000),
    } as any

    const first = await forwarder.check_blocked('', {
        article: firstArticle,
    })
    await forwarder.send('', {
        article: firstArticle,
    })

    const second = await forwarder.check_blocked('', {
        article: {
            platform: 1,
            a_id: 'rt-2',
            type: 'retweet',
            has_media: true,
            created_at: Math.floor(Date.now() / 1000),
        } as any,
    })
    const textOnly = await forwarder.check_blocked('', {
        article: {
            platform: 1,
            a_id: 'rt-3',
            type: 'retweet',
            has_media: false,
            created_at: Math.floor(Date.now() / 1000),
        } as any,
    })

    expect(first).toBe(false)
    expect(second).toBe(true)
    expect(textOnly).toBe(false)
})

test('ForwarderPools resendArticle groups website photo singles into a same-day album batch', async () => {
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
                render_type: 'img-tag',
            } as any,
            forwarders: [],
            crawlers: [
                {
                    name: '22/7官网FC抓取 - 日间轮询',
                    websites: ['https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga'],
                },
            ] as any,
        },
        new EventEmitter(),
    )

    ;(pools as any).resolveForwardingPaths = () => [
        {
            formatterConfig: {
                render_type: 'img-tag',
            },
            targets: [],
            source: 'graph',
            formatterName: '官网卡片',
        },
    ]

    let capturedArticles: any[] = []
    ;(pools as any).sendArticles = async (_log: any, _taskId: string, articles: any[]) => {
        capturedArticles = articles
    }

    const originalGetArticlesByTimeRange = DB.Article.getArticlesByTimeRange
    ;(DB.Article as any).getArticlesByTimeRange = async () => [
        {
            id: 51,
            a_id: 'photo:photoga:35054',
            u_id: '22/7:photo',
            username: '北原実咲',
            created_at: 1773673200,
            content: '【春のかおり - 北原実咲】\n\nメッセージ1',
            translation: null,
            translated_by: null,
            url: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga#photo-modal-a22',
            type: 'article',
            ref: null,
            has_media: true,
            media: [{ type: 'photo', url: 'https://example.com/1.jpg' }],
            extra: {
                data: {
                    site: '22/7',
                    host: 'nanabunnonijyuuni-mobile.com',
                    feed: 'photo',
                    title: '春のかおり - 北原実咲',
                    member: '北原実咲',
                    summary: '春のかおり',
                    raw_html: '<p>メッセージ1</p>',
                    album_id: 'photoga',
                    theme: '春のかおり',
                    modal_id: 'photo-modal-a22',
                    photo_code: '35054',
                },
                content: '春のかおり',
                media: [{ type: 'photo', url: 'https://example.com/1.jpg' }],
                extra_type: 'website_meta',
            },
            u_avatar: 'https://example.com/a1.jpg',
            platform: 5,
        },
        {
            id: 52,
            a_id: 'photo:photoga:35055',
            u_id: '22/7:photo',
            username: '黒崎ありす',
            created_at: 1773673200,
            content: '【春のかおり - 黒崎ありす】\n\nメッセージ2',
            translation: null,
            translated_by: null,
            url: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga#photo-modal-a23',
            type: 'article',
            ref: null,
            has_media: true,
            media: [{ type: 'photo', url: 'https://example.com/2.jpg' }],
            extra: {
                data: {
                    site: '22/7',
                    host: 'nanabunnonijyuuni-mobile.com',
                    feed: 'photo',
                    title: '春のかおり - 黒崎ありす',
                    member: '黒崎ありす',
                    summary: '春のかおり',
                    raw_html: '<p>メッセージ2</p>',
                    album_id: 'photoga',
                    theme: '春のかおり',
                    modal_id: 'photo-modal-a23',
                    photo_code: '35055',
                },
                content: '春のかおり',
                media: [{ type: 'photo', url: 'https://example.com/2.jpg' }],
                extra_type: 'website_meta',
            },
            u_avatar: 'https://example.com/a2.jpg',
            platform: 5,
        },
    ]

    try {
        await pools.resendArticle(
            {
                id: 51,
                a_id: 'photo:photoga:35054',
                u_id: '22/7:photo',
                username: '北原実咲',
                created_at: 1773673200,
                content: '【春のかおり - 北原実咲】\n\nメッセージ1',
                translation: null,
                translated_by: null,
                url: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga#photo-modal-a22',
                type: 'article',
                ref: null,
                has_media: true,
                media: [{ type: 'photo', url: 'https://example.com/1.jpg' }],
                extra: {
                    data: {
                        site: '22/7',
                        host: 'nanabunnonijyuuni-mobile.com',
                        feed: 'photo',
                        title: '春のかおり - 北原実咲',
                        member: '北原実咲',
                        summary: '春のかおり',
                        raw_html: '<p>メッセージ1</p>',
                        album_id: 'photoga',
                        theme: '春のかおり',
                        modal_id: 'photo-modal-a22',
                        photo_code: '35054',
                    },
                    content: '春のかおり',
                    media: [{ type: 'photo', url: 'https://example.com/1.jpg' }],
                    extra_type: 'website_meta',
                },
                u_avatar: 'https://example.com/a1.jpg',
                platform: 5,
            } as any,
            '22/7官网FC抓取 - 日间轮询',
            {
                render_type: 'img-tag',
            } as any,
        )
    } finally {
        ;(DB.Article as any).getArticlesByTimeRange = originalGetArticlesByTimeRange
    }

    expect(capturedArticles).toHaveLength(1)
    expect(capturedArticles[0]?.a_id).toBe('photo:album:photoga:35054')
    expect(capturedArticles[0]?.media).toHaveLength(2)
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

test('sendArticles skips delivery when render service marks a cross-platform duplicate', async () => {
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
        } as any,
        'target-duplicate',
    )

    let cleanupCalled = false
    let claimedArticleId: number | null = null
    ;(pools as any).renderService = {
        process: async () => ({
            text: 'duplicate short',
            mediaFiles: [],
            shouldSkipSend: true,
            skipReason: 'Cross-platform short video duplicate matched 2:ig-short-1',
        }),
        cleanup: () => {
            cleanupCalled = true
        },
    }
    ;(pools as any).claimArticleChain = async (article: any) => {
        claimedArticleId = article.id
        return true
    }

    await (pools as any).sendArticles(
        undefined,
        'manual-duplicate',
        [
            {
                id: 204,
                a_id: 'yt-short-dup',
                platform: 4,
                created_at: Math.floor(Date.now() / 1000),
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

    expect(target.sent).toHaveLength(0)
    expect(claimedArticleId).toBe(204)
    expect(cleanupCalled).toBe(true)
})

test('sendArticles does not increment article error count when every target is intentionally skipped as old', async () => {
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
        } as any,
        'target-old-skip',
    )

    ;(pools as any).renderService = {
        process: async () => ({
            text: 'old article',
            mediaFiles: [],
        }),
        cleanup: () => undefined,
    }
    ;(pools as any).claimArticleChain = async () => true

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => false

    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-old-skip',
            [
                {
                    id: 205,
                    a_id: 'website-old-skip',
                    platform: 5,
                    created_at: Math.floor(Date.now() / 1000) - 3 * 3600,
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
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(0)
    expect((pools as any).errorCounter.get('5:website-old-skip')).toBeUndefined()
})
