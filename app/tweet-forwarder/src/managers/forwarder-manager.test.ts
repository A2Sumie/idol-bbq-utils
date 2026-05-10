import { expect, test } from 'bun:test'
import EventEmitter from 'events'
import {
    ForwarderPools,
    ForwarderTaskScheduler,
    buildAutoBoundForwarderTaskData,
    resolveBatchTargetIds,
} from './forwarder-manager'
import { TaskScheduler } from '@/utils/base'
import { fileURLToPath } from 'url'
import { Forwarder } from '@/middleware/forwarder/base'
import DB from '@/db'
import { Platform } from '@idol-bbq-utils/spider/types'

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
    ;(pools as any).resolveForwardingPaths = (_crawlerName: string, formatterConfig: any) => {
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

test('sendArticles sends a target-level digest for lower-noise targets', async () => {
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
            digest_threshold: 3,
            digest_max_items: 3,
        } as any,
        'target-digest',
    )

    const claimed: Array<number> = []
    ;(pools as any).claimArticleChain = async (article: any) => {
        claimed.push(article.id)
        return true
    }
    ;(pools as any).renderService = {
        process: async () => {
            throw new Error('digest should bypass per-article render')
        },
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-digest',
            [0, 1, 2, 3].map((index) => ({
                id: 300 + index,
                a_id: `digest-${index}`,
                platform: Platform.X,
                username: `member-${index}`,
                u_id: `member-${index}`,
                content: `更新 ${index}`,
                url: `https://x.com/member/status/${index}`,
                type: index === 2 ? 'reply' : 'tweet',
                created_at: Math.floor(Date.now() / 1000) + index,
                ref: null,
            })),
            [
                {
                    forwarder: target,
                    runtime_config: undefined,
                },
            ],
            {
                render_type: 'text',
            } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('【更新摘要】4 条')
    expect(target.sent[0]?.texts[0]).toContain('↪ member-2')
    expect(target.sent[0]?.texts[0]).toContain('另有 1 条更新已合并')
    expect(claimed).toEqual([300, 301, 302, 303])
})

test('sendArticles keeps high-frequency hashtags digestized and extracts non-tag text', async () => {
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
            digest_threshold: 99,
            tag_digest_threshold: 3,
            tag_digest_detection_window_seconds: 300,
            tag_digest_window_seconds: 1200,
            tag_digest_max_items: 4,
        } as any,
        'target-tag-digest',
    )

    const claimed: Array<number> = []
    ;(pools as any).claimArticleChain = async (article: any) => {
        claimed.push(article.id)
        return true
    }
    ;(pools as any).renderService = {
        process: async () => {
            throw new Error('tag digest should bypass per-article render')
        },
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-tag-digest',
            [0, 1, 2].map((index) => ({
                id: 400 + index,
                a_id: `tag-digest-${index}`,
                platform: Platform.X,
                username: `member-${index}`,
                u_id: `member-${index}`,
                content: `ライブ最高 ${index} #ナナニジ #LIVE`,
                url: `https://x.com/member/status/tag-${index}`,
                type: 'tweet',
                created_at: now + index,
                ref: null,
            })),
            [
                {
                    forwarder: target,
                    runtime_config: undefined,
                },
            ],
            {
                render_type: 'text',
            } as any,
        )

        await (pools as any).sendArticles(
            undefined,
            'manual-tag-digest-followup',
            [
                {
                    id: 410,
                    a_id: 'tag-digest-followup',
                    platform: Platform.X,
                    username: 'member-next',
                    u_id: 'member-next',
                    content: '追加のお知らせ #ナナニジ',
                    url: 'https://x.com/member/status/tag-followup',
                    type: 'tweet',
                    created_at: now + 10,
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
                render_type: 'text',
            } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(2)
    expect(target.sent[0]?.texts[0]).toContain('【话题更新摘要】#ナナニジ / 3 条')
    expect(target.sent[0]?.texts[0]).not.toContain('/ target-tag-digest')
    expect(target.sent[0]?.texts[0]).toContain('正文: ライブ最高 0')
    expect(target.sent[0]?.texts[0]).not.toContain('正文: ライブ最高 0 #')
    expect(target.sent[0]?.texts[0]).toContain('其他标签: #LIVE')
    expect(target.sent[0]?.texts[0]).not.toContain('标签: #ナナニジ #LIVE')
    expect(target.sent[1]?.texts[0]).toContain('【话题更新摘要】#ナナニジ / 1 条')
    expect(target.sent[1]?.texts[0]).toContain('正文: 追加のお知らせ')
    expect(claimed).toEqual([400, 401, 402, 410])
})

test('sendArticles does not enable hashtag digest for high-tolerance targets without digest config', async () => {
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
        'target-high-tolerance',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content,
            mediaFiles: [],
        }),
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-no-tag-digest',
            [0, 1, 2].map((index) => ({
                id: 420 + index,
                a_id: `no-tag-digest-${index}`,
                platform: Platform.X,
                username: `member-${index}`,
                u_id: `member-${index}`,
                content: `高容忍 ${index} #ナナニジ`,
                url: `https://x.com/member/status/no-tag-${index}`,
                type: 'tweet',
                created_at: now + index,
                ref: null,
            })),
            [
                {
                    forwarder: target,
                    runtime_config: undefined,
                },
            ],
            {
                render_type: 'text',
            } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(3)
    expect(target.sent.map((item) => item.texts[0])).toEqual([
        '高容忍 0 #ナナニジ',
        '高容忍 1 #ナナニジ',
        '高容忍 2 #ナナニジ',
    ])
})

test('sendArticles requires multiple authors before entering hashtag storm digest', async () => {
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
            digest_threshold: 99,
            tag_digest_threshold: 3,
            tag_digest_min_authors: 2,
        } as any,
        'target-tag-author-gate',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content,
            mediaFiles: [],
        }),
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-single-author-tag',
            [0, 1, 2].map((index) => ({
                id: 430 + index,
                a_id: `single-author-tag-${index}`,
                platform: Platform.X,
                username: 'same-member',
                u_id: 'same-member',
                content: `同一作者 ${index} #ナナニジ`,
                url: `https://x.com/member/status/same-author-${index}`,
                type: 'tweet',
                created_at: now + index,
                ref: null,
            })),
            [
                {
                    forwarder: target,
                    runtime_config: undefined,
                },
            ],
            {
                render_type: 'text',
            } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(3)
    expect(target.sent.map((item) => item.texts[0])).toEqual([
        '同一作者 0 #ナナニジ',
        '同一作者 1 #ナナニジ',
        '同一作者 2 #ナナニジ',
    ])
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
