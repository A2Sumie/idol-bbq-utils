import { afterEach, beforeEach, expect, test } from 'bun:test'
import EventEmitter from 'events'
import {
    ForwarderPools,
    ForwarderTaskScheduler,
    buildAutoBoundForwarderTaskData,
    resolveBatchAggregationConfig,
    resolveBatchTargetIds,
    resolveSummaryCardConfig,
} from './forwarder-manager'
import { TaskScheduler } from '@/utils/base'
import { fileURLToPath } from 'url'
import { Forwarder, PartialForwarderSendError } from '@/middleware/forwarder/base'
import DB from '@/db'
import { Platform } from '@idol-bbq-utils/spider/types'
import { normalizeCronSecond } from '@/utils/cron'
import { articleOutboundKey } from '@/services/outbound-message-service'

process.env.FONTS_DIR = fileURLToPath(new URL('../../../../assets/fonts', import.meta.url))
process.env.RENDER_REMOTE_ASSETS = '0'

const originalOutboundMessage = { ...DB.OutboundMessage }
const originalAggregationWindow = { ...DB.AggregationWindow }
const originalTargetHealth = { ...DB.TargetHealth }
const originalForwardBy = { ...DB.ForwardBy }
const originalMediaHash = { ...DB.MediaHash }

beforeEach(() => {
    const outboundRecords = new Map<string, any>()
    const targetHealth = new Map<string, any>()
    const aggregationWindows = new Map<number, any>()
    const aggregationItems = new Map<string, any>()
    const forwardByRecords = new Map<string, any>()
    const mediaHashRecords = new Map<string, any>()
    let nextWindowId = 1
    let nextItemId = 1

    const forwardByKey = (refId: number, platform: string | number, botId: string, taskType: string) =>
        `${platform}:${refId}:${botId}:${taskType}`
    const mediaHashKey = (platform: string, hash: string) => `${platform}:${hash}`

    ;(DB.ForwardBy as any).checkExist = async (
        refId: number,
        platform: string | number,
        botId: string,
        taskType: string,
    ) => forwardByRecords.get(forwardByKey(refId, platform, botId, taskType)) || null
    ;(DB.ForwardBy as any).save = async (refId: number, platform: string | number, botId: string, taskType: string) => {
        const record = { ref_id: refId, platform, bot_id: botId, task_type: taskType }
        forwardByRecords.set(forwardByKey(refId, platform, botId, taskType), record)
        return record
    }
    ;(DB.ForwardBy as any).claim = async (
        refId: number,
        platform: string | number,
        botId: string,
        taskType: string,
    ) => {
        const key = forwardByKey(refId, platform, botId, taskType)
        if (forwardByRecords.has(key)) {
            return false
        }
        forwardByRecords.set(key, { ref_id: refId, platform, bot_id: botId, task_type: taskType })
        return true
    }
    ;(DB.ForwardBy as any).deleteRecord = async (
        refId: number,
        platform: string | number,
        botId: string,
        taskType: string,
    ) => {
        forwardByRecords.delete(forwardByKey(refId, platform, botId, taskType))
    }
    ;(DB.MediaHash as any).checkExist = async (platform: string, hash: string) =>
        mediaHashRecords.get(mediaHashKey(platform, hash)) || null
    ;(DB.MediaHash as any).save = async (platform: string, hash: string, a_id = '') => {
        const key = mediaHashKey(platform, hash)
        const record =
            mediaHashRecords.get(key) || {
                id: mediaHashRecords.size + 1,
                platform,
                hash,
                a_id,
                created_at: Math.floor(Date.now() / 1000),
            }
        mediaHashRecords.set(key, record)
        return record
    }
    ;(DB.MediaHash as any).claimVisibleSlot = async (options: any) => {
        const maxVisible = Math.max(1, Math.floor(Number(options.maxVisible || 1)))
        const windowSeconds = Math.max(1, Math.floor(Number(options.windowSeconds || 1)))
        const now = Math.floor(Number(options.now || Date.now() / 1000))
        const cutoff = now - windowSeconds
        let activeCount = 0
        let inactiveSlot: number | undefined

        for (let slot = 0; slot < maxVisible; slot += 1) {
            const platform = `${options.namespace}:slot:${slot}`
            const record = mediaHashRecords.get(mediaHashKey(platform, options.hash))
            if (record && record.created_at >= cutoff) {
                activeCount += 1
            } else {
                inactiveSlot ??= slot
            }
        }

        if (activeCount >= maxVisible || inactiveSlot === undefined) {
            return { allowed: false, seenCount: activeCount }
        }

        const platform = `${options.namespace}:slot:${inactiveSlot}`
        mediaHashRecords.set(mediaHashKey(platform, options.hash), {
            id: mediaHashRecords.size + 1,
            platform,
            hash: options.hash,
            a_id: options.a_id || '',
            created_at: now,
        })
        return { allowed: true, seenCount: activeCount + 1, slot: inactiveSlot }
    }
    ;(DB.MediaHash as any).releaseVisibleSlots = async (options: any) => {
        let released = 0
        for (const claim of options.claims || []) {
            const key = mediaHashKey(claim.platform, claim.hash)
            const record = mediaHashRecords.get(key)
            if (record && (!claim.a_id || record.a_id === claim.a_id)) {
                mediaHashRecords.delete(key)
                released += 1
            }
        }
        return released
    }
    ;(DB.MediaHash as any).__records = mediaHashRecords
    ;(DB.OutboundMessage as any).claim = async (data: any) => {
        const existing = outboundRecords.get(data.idempotency_key)
        if (existing && existing.status !== 'failed') {
            if (
                existing.route_key !== data.route_key ||
                existing.target_id !== data.target_id ||
                (existing.target_platform || null) !== (data.target_platform || null) ||
                existing.task_kind !== data.task_kind ||
                (existing.article_key || null) !== (data.article_key || null) ||
                (existing.synthetic_key || null) !== (data.synthetic_key || null) ||
                existing.payload_hash !== data.payload_hash
            ) {
                existing.segment_results = {
                    diagnostic: 'suppressed_payload_drift',
                    existing: {
                        route_key: existing.route_key,
                        payload_hash: existing.payload_hash,
                        status: existing.status,
                    },
                    incoming: {
                        route_key: data.route_key,
                        payload_hash: data.payload_hash,
                    },
                }
            }
            return { claimed: false, record: existing }
        }
        const now = Math.floor(Date.now() / 1000)
        const record = {
            id: existing?.id || outboundRecords.size + 1,
            ...existing,
            ...data,
            status: 'planned',
            created_at: existing?.created_at || now,
            updated_at: now,
            attempt_count: existing?.attempt_count || 0,
        }
        outboundRecords.set(data.idempotency_key, record)
        return { claimed: true, record }
    }
    ;(DB.OutboundMessage as any).__records = outboundRecords
    ;(DB.OutboundMessage as any).markSending = async (idempotencyKey: string) => {
        const record = outboundRecords.get(idempotencyKey)
        Object.assign(record, {
            status: 'sending',
            attempt_count: (record?.attempt_count || 0) + 1,
        })
        return record
    }
    ;(DB.OutboundMessage as any).markSent = async (idempotencyKey: string, providerResult?: unknown) => {
        const record = outboundRecords.get(idempotencyKey)
        Object.assign(record, { status: 'sent', provider_message_ids: providerResult ?? null })
        return record
    }
    ;(DB.OutboundMessage as any).markPartial = async (
        idempotencyKey: string,
        providerResult: unknown,
        error: unknown,
    ) => {
        const record = outboundRecords.get(idempotencyKey)
        Object.assign(record, {
            status: 'partial',
            segment_results: providerResult,
            last_error: error instanceof Error ? error.message : String(error),
        })
        return record
    }
    ;(DB.OutboundMessage as any).markFailed = async (idempotencyKey: string, error: unknown) => {
        const record = outboundRecords.get(idempotencyKey)
        if (record) {
            Object.assign(record, {
                status: 'failed',
                last_error: error instanceof Error ? error.message : String(error),
            })
        }
        return record
    }
    ;(DB.OutboundMessage as any).markQueued = async (idempotencyKey: string, details?: unknown) => {
        const record = outboundRecords.get(idempotencyKey)
        Object.assign(record, { status: 'queued', provider_message_ids: details ?? null })
        return record
    }
    ;(DB.OutboundMessage as any).markDryRun = async (idempotencyKey: string, details?: unknown) => {
        const record = outboundRecords.get(idempotencyKey)
        Object.assign(record, { status: 'dry_run', provider_message_ids: details ?? null })
        return record
    }
    ;(DB.OutboundMessage as any).markSkipped = async (idempotencyKey: string, reason: string, details?: unknown) => {
        const record = outboundRecords.get(idempotencyKey)
        Object.assign(record, { status: 'skipped', provider_message_ids: { reason, details } })
        return record
    }
    ;(DB.OutboundMessage as any).list = async () => Array.from(outboundRecords.values())
    ;(DB.TargetHealth as any).mark = async (data: any) => {
        const record = {
            id: targetHealth.size + 1,
            ...targetHealth.get(data.target_id),
            ...data,
        }
        targetHealth.set(data.target_id, record)
        return record
    }
    ;(DB.TargetHealth as any).list = async () => Array.from(targetHealth.values())
    ;(DB.AggregationWindow as any).getOrCreateOpen = async (data: any) => {
        const existing = Array.from(aggregationWindows.values()).find(
            (window) => window.idempotency_key === data.idempotency_key,
        )
        if (existing) {
            return existing
        }
        const window = {
            id: nextWindowId++,
            ...data,
            status: 'open',
            created_at: Math.floor(Date.now() / 1000),
            updated_at: Math.floor(Date.now() / 1000),
            finished_at: null,
            payload_hash: null,
        }
        aggregationWindows.set(window.id, window)
        return window
    }
    ;(DB.AggregationWindow as any).listOpen = async (mode?: string) =>
        Array.from(aggregationWindows.values()).filter(
            (window) => window.status === 'open' && (!mode || window.mode === mode),
        )
    ;(DB.AggregationWindow as any).updateStatus = async (id: number, status: string, meta?: any) => {
        const window = aggregationWindows.get(id)
        if (window) {
            Object.assign(window, { status, payload_hash: meta?.payload_hash ?? window.payload_hash })
        }
        return window
    }
    ;(DB.AggregationWindow as any).upsertItem = async (data: any) => {
        const key = `${data.window_id}:${data.article_key}`
        const existing = aggregationItems.get(key)
        const item = {
            id: existing?.id || nextItemId++,
            ...existing,
            ...data,
            created_at: existing?.created_at || Math.floor(Date.now() / 1000),
        }
        aggregationItems.set(key, item)
        return item
    }
    ;(DB.AggregationWindow as any).listItems = async (windowId: number) =>
        Array.from(aggregationItems.values()).filter((item) => item.window_id === windowId)
    ;(DB.AggregationWindow as any).__windows = aggregationWindows
    ;(DB.AggregationWindow as any).__items = aggregationItems
})

afterEach(() => {
    Object.assign(DB.OutboundMessage, originalOutboundMessage)
    Object.assign(DB.AggregationWindow, originalAggregationWindow)
    Object.assign(DB.TargetHealth, originalTargetHealth)
    Object.assign(DB.ForwardBy, originalForwardBy)
    Object.assign(DB.MediaHash, originalMediaHash)
})

function backdateSummaryCardQueues(pools: any, seconds: number) {
    const now = Math.floor(Date.now() / 1000)
    for (const queue of pools.summaryCardQueues.values()) {
        queue.firstQueuedAt = now - seconds
    }
}

function getSummaryCardQueueForTarget(pools: any, targetId: string) {
    return Array.from(pools.summaryCardQueues.values()).find((queue: any) => queue.target.id === targetId) as any
}

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

test('resolveBatchAggregationConfig uses configurable cron and window', () => {
    expect(resolveBatchAggregationConfig({} as any)).toEqual({
        cron: '45 0 * * * *',
        windowSeconds: 3600,
    })
    expect(
        resolveBatchAggregationConfig({
            aggregation_cron: '*/30 * * * *',
            aggregation_window_seconds: 1800,
        } as any),
    ).toEqual({
        cron: '45 */30 * * * *',
        windowSeconds: 1800,
    })
    expect(
        resolveBatchAggregationConfig({} as any, [
            {
                aggregation_cron: '17 1-23/2 * * *',
                aggregation_window_seconds: 7200,
            },
        ]),
    ).toEqual({
        cron: '45 17 1-23/2 * * *',
        windowSeconds: 7200,
    })
})

test('resolveSummaryCardConfig defaults to an eight-item summary card threshold', () => {
    expect(resolveSummaryCardConfig({ summary_card: true } as any)).toEqual({
        intervalSeconds: 1800,
        threshold: 8,
        maxItems: 14,
        includeOriginalMedia: false,
        sendFirstImmediately: true,
        sendFirstNative: false,
        mediaRealtime: false,
        mediaRealtimeText: 'none',
        flushOnThreshold: true,
        flushDelaySeconds: 0,
        windowAlignment: 'none',
        mediaDuplicateLimit: null,
    })
    expect(
        resolveSummaryCardConfig({
            summary_card: {
                enabled: true,
                interval_seconds: 900,
                threshold: 3,
                max_items: 6,
                include_original_media: true,
                send_first_immediately: false,
                send_first_native: true,
                media_realtime: true,
                media_realtime_text: 'basic',
                flush_on_threshold: false,
                flush_delay_seconds: 300,
                align_to_interval: true,
                media_duplicate_limit: 2,
            },
        } as any),
    ).toEqual({
        intervalSeconds: 900,
        threshold: 3,
        maxItems: 6,
        includeOriginalMedia: true,
        sendFirstImmediately: false,
        sendFirstNative: true,
        mediaRealtime: true,
        mediaRealtimeText: 'basic',
        flushOnThreshold: false,
        flushDelaySeconds: 300,
        windowAlignment: 'interval',
        mediaDuplicateLimit: 2,
    })
    expect(
        resolveSummaryCardConfig({
            summary_card: {
                enabled: true,
                include_original_media: true,
            },
        } as any)?.mediaDuplicateLimit,
    ).toBe(2)
    expect(
        resolveSummaryCardConfig({
            summary_card: {
                enabled: true,
                media_realtime: true,
            },
        } as any)?.mediaDuplicateLimit,
    ).toBe(2)
    expect(resolveSummaryCardConfig({ summary_card: { enabled: false } } as any)).toBeNull()
})

test('normalizeCronSecond pins five-field cron jobs to the default forty-fifth second', () => {
    expect(normalizeCronSecond('*/4 * * * *')).toBe('45 */4 * * * *')
    expect(normalizeCronSecond('10-59/30 * * * * *')).toBe('10-59/30 * * * * *')
    expect(normalizeCronSecond(' 7,27,47 * * * * ')).toBe('45 7,27,47 * * * *')
})

test('Forwarder does not retry after a partial visible send', async () => {
    class PartialRecordingForwarder extends Forwarder {
        NAME = 'partial-recording'
        attempts = 0

        protected async realSend(): Promise<any> {
            this.attempts += 1
            throw new PartialForwarderSendError(
                'partial visible send',
                [{ ok: true }],
                'message:2/2',
                new Error('tail failed'),
            )
        }
    }

    const forwarder = new PartialRecordingForwarder({} as any, 'partial-visible-test')
    let caught: unknown
    try {
        await forwarder.send('hello', { forceSend: true })
    } catch (error) {
        caught = error
    }

    expect(forwarder.attempts).toBe(1)
    expect(caught).toBeInstanceOf(PartialForwarderSendError)
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

test('ForwarderTaskScheduler registers immediate tasks before dispatch status events', async () => {
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
                    cfg_forwarder: {},
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
    await scheduler.init()
    const registeredAtDispatch: boolean[] = []
    emitter.on(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, (payload) => {
        registeredAtDispatch.push((scheduler as any).tasks.has(payload.taskId))
        emitter.emit(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, {
            taskId: payload.taskId,
            status: TaskScheduler.TaskStatus.CANCELLED,
        })
    })

    try {
        ;(scheduler as any).onSpiderTaskFinished({
            taskId: 'spider-2',
            crawlerName: '22/7-cast-成员统一列表',
            result: [
                {
                    task_type: 'article',
                    url: 'https://x.com/i/lists/1936785344072151389',
                    data: [201],
                },
            ],
        })

        expect(registeredAtDispatch).toEqual([true])
        expect((scheduler as any).tasks.has('spider-spider-2')).toBeFalse()
    } finally {
        await scheduler.drop()
    }
})

test('ForwarderTaskScheduler ignores malformed spider finished payloads', () => {
    const emitter = new EventEmitter()
    const dispatched: any[] = []
    emitter.on(`forwarder:${TaskScheduler.TaskEvent.DISPATCH}`, (payload) => dispatched.push(payload))
    const scheduler = new ForwarderTaskScheduler(
        {
            cfg_forwarder: {} as any,
            forwarders: [],
            connections: {
                'crawler-formatter': {},
                'crawler-processor': {},
                'processor-formatter': {},
                'formatter-target': {},
            } as any,
            crawlers: [],
            formatters: [],
            forward_targets: [],
        },
        emitter,
    )

    ;(scheduler as any).onSpiderTaskFinished(undefined)
    ;(scheduler as any).onSpiderTaskFinished({ taskId: 'spider-bad', crawlerName: 'bad', result: null })

    expect(dispatched).toEqual([])
})

test('ForwarderPools ignores malformed dispatch payloads without status side effects', async () => {
    const emitter = new EventEmitter()
    const statusEvents: any[] = []
    emitter.on(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, (payload) => statusEvents.push(payload))
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
            cfg_forwarder: {} as any,
            forwarders: [],
            crawlers: [],
        },
        emitter,
    )

    await (pools as any).dispatchListener(undefined)

    expect(statusEvents).toEqual([])
})

test('ForwarderPools dispatch listener catches unexpected async failures', async () => {
    const emitter = new EventEmitter()
    const statusEvents: any[] = []
    emitter.on(`forwarder:${TaskScheduler.TaskEvent.UPDATE_STATUS}`, (payload) => statusEvents.push(payload))
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
            cfg_forwarder: {} as any,
            forwarders: [],
            crawlers: [],
        },
        emitter,
    )
    ;(pools as any).onTaskReceived = async () => {
        throw new Error('forwarder dispatch boom')
    }

    await (pools as any).dispatchListener({
        taskId: 'forwarder-boom',
        task: {
            id: 'forwarder-boom',
            status: TaskScheduler.TaskStatus.PENDING,
            data: {
                name: 'forwarder-boom',
                websites: ['https://x.com'],
            },
        },
    })

    expect(statusEvents).toEqual([
        {
            taskId: 'forwarder-boom',
            status: TaskScheduler.TaskStatus.FAILED,
        },
    ])
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

test('sendArticles does not poison outbound state when ForwardBy claim loses a race', async () => {
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

    const target = new RecordingForwarder({} as any, 'target-claim-race')
    const article = {
        id: 203,
        a_id: 'claim-race-article',
        platform: 1,
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
    }

    let cleanupCalled = false
    ;(pools as any).renderService = {
        process: async () => ({
            text: 'claim race payload',
            mediaFiles: [],
            cardMediaFiles: [],
            originalMediaFiles: [],
        }),
        cleanup: () => {
            cleanupCalled = true
        },
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    const originalClaim = DB.ForwardBy.claim
    ;(DB.ForwardBy as any).checkExist = async () => null
    ;(DB.ForwardBy as any).claim = async () => false

    try {
        await (pools as any).sendArticles(
            undefined,
            'claim-race-task',
            [article],
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
        ;(DB.ForwardBy as any).claim = originalClaim
    }

    expect(target.sent).toHaveLength(0)
    expect(cleanupCalled).toBe(true)
    const outboundKey = articleOutboundKey('target-claim-race', article as any)
    expect((DB.OutboundMessage as any).__records.get(outboundKey)).toBeUndefined()
})

test('sendArticles stops after render when forwarder pool starts shutting down', async () => {
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

    let sendCalls = 0
    let renderCalls = 0
    let cleanupCalls = 0
    const target = {
        id: 'target-shutdown-stop',
        NAME: 'recording',
        getEffectiveConfig: (runtimeConfig?: any) => runtimeConfig || {},
        check_blocked: async () => false,
        send: async () => {
            sendCalls += 1
            return { status: 'sent' }
        },
    }
    const article = {
        id: 208,
        a_id: 'shutdown-stop-article',
        platform: 1,
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
    }

    ;(pools as any).renderService = {
        process: async () => {
            renderCalls += 1
            ;(pools as any).shuttingDown = true
            return {
                text: 'shutdown stop payload',
                mediaFiles: [{ media_type: 'photo', path: '/tmp/shutdown-stop.jpg' }],
                cardMediaFiles: [],
                originalMediaFiles: [],
            }
        },
        cleanup: () => {
            cleanupCalls += 1
        },
    }

    await (pools as any).sendArticles(
        undefined,
        'shutdown-stop-task',
        [article],
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

    expect(renderCalls).toBe(1)
    expect(cleanupCalls).toBe(1)
    expect(sendCalls).toBe(0)
    expect((DB.OutboundMessage as any).__records.size).toBe(0)
})

test('sendArticles keeps ForwardBy for terminal blocked target sends', async () => {
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

    const target = {
        id: 'target-blocked-send',
        NAME: 'recording',
        getEffectiveConfig: (runtimeConfig?: any) => runtimeConfig || {},
        check_blocked: async () => false,
        send: async () => ({
            status: 'blocked',
            reason: 'target_middleware_block',
        }),
    }
    const article = {
        id: 204,
        a_id: 'blocked-send-article',
        platform: 1,
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
    }

    ;(pools as any).renderService = {
        process: async () => ({
            text: 'blocked target payload',
            mediaFiles: [],
            cardMediaFiles: [],
            originalMediaFiles: [],
        }),
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'blocked-send-task',
        [article],
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

    const outboundKey = articleOutboundKey('target-blocked-send', article as any)
    const outboundRecord = (DB.OutboundMessage as any).__records.get(outboundKey)
    expect(outboundRecord?.status).toBe('skipped')
    expect(outboundRecord?.provider_message_ids?.reason).toBe('target_middleware_block')
    expect(await DB.ForwardBy.checkExist(article.id, article.platform, target.id, 'article')).not.toBeNull()
})

test('sendArticles releases ForwardBy and keeps outbound retryable on dry-run send blocking', async () => {
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

    const target = {
        id: 'target-dry-run-send',
        NAME: 'recording',
        getEffectiveConfig: (runtimeConfig?: any) => runtimeConfig || {},
        check_blocked: async () => false,
        send: async () => ({
            status: 'dry_run',
            reason: 'outbound send blocked by blocked mode for recording:target-dry-run-send',
            details: {
                send_mode: 'blocked',
                target_id: 'target-dry-run-send',
                forwarder: 'recording',
                text_count: 1,
                text_length: 22,
                media_count: 0,
                card_media_count: 0,
                content_media_count: 0,
            },
        }),
    }
    const article = {
        id: 205,
        a_id: 'dry-run-send-article',
        platform: 1,
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
    }

    ;(pools as any).renderService = {
        process: async () => ({
            text: 'dry run target payload',
            mediaFiles: [],
            cardMediaFiles: [],
            originalMediaFiles: [],
        }),
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'dry-run-send-task',
        [article],
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

    const outboundKey = articleOutboundKey('target-dry-run-send', article as any)
    const outboundRecord = (DB.OutboundMessage as any).__records.get(outboundKey)
    expect(outboundRecord?.status).toBe('dry_run')
    expect(outboundRecord?.provider_message_ids?.status).toBe('dry_run')
    expect(await DB.ForwardBy.checkExist(article.id, article.platform, target.id, 'article')).toBeNull()
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
    const outboundKey = articleOutboundKey(
        'target-duplicate',
        {
            a_id: 'yt-short-dup',
            platform: 4,
        } as any,
        { forceKey: 'manual-duplicate' },
    )
    const outboundRecord = (DB.OutboundMessage as any).__records.get(outboundKey)
    expect(outboundRecord?.status).toBe('skipped')
    expect(outboundRecord?.provider_message_ids?.reason).toBe('Cross-platform short video duplicate matched 2:ig-short-1')
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
    expect(target.sent[0]?.texts[0]).toContain('【更新合并】4 条')
    expect(target.sent[0]?.texts[0]).toContain('↪ member-2')
    expect(target.sent[0]?.texts[0]).toContain('另有 1 条更新已合并')
    expect(claimed).toEqual([300, 301, 302, 303])
})

test('sendArticles treats a blocked digest as handled instead of falling through to article sends', async () => {
    class DigestBlockingForwarder extends Forwarder {
        NAME = 'recording'
        calls: Array<{ text: string; props: any }> = []
        articleSends: Array<{ texts: string[]; props: any }> = []

        public override async send(text: string, props?: any): Promise<any> {
            this.calls.push({ text, props })
            if (!props?.article) {
                return { status: 'blocked', reason: 'digest_blocked' }
            }
            return super.send(text, props)
        }

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.articleSends.push({ texts, props })
            return { ok: true }
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

    const target = new DigestBlockingForwarder(
        {
            block_until: '32h',
            digest_threshold: 2,
            digest_max_items: 4,
        } as any,
        'target-digest-blocked',
    )

    const claimed = new Set<number>()
    const released: number[] = []
    ;(pools as any).claimArticleChain = async (article: any) => {
        claimed.add(article.id)
        return true
    }
    ;(pools as any).releaseArticleChain = async (article: any) => {
        released.push(article.id)
        claimed.delete(article.id)
    }
    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content,
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [],
            mediaFiles: [],
        }),
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-digest-blocked',
            [0, 1].map((index) => ({
                id: 320 + index,
                a_id: `digest-blocked-${index}`,
                platform: Platform.X,
                username: `blocked-member-${index}`,
                u_id: `blocked_member_${index}`,
                content: `blocked digest update ${index}`,
                url: `https://x.com/blocked_member/status/${index}`,
                type: 'tweet',
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

    const outboundRecords = Array.from(((DB.OutboundMessage as any).__records as Map<string, any>).values())
    const digestOutbound = outboundRecords.find((record: any) => record.task_kind === 'digest')
    expect(target.calls).toHaveLength(1)
    expect(target.calls[0]?.props?.article).toBeUndefined()
    expect(target.articleSends).toHaveLength(0)
    expect(released).toEqual([])
    expect(Array.from(claimed).sort()).toEqual([320, 321])
    expect(digestOutbound?.status).toBe('skipped')
    expect(digestOutbound?.provider_message_ids?.reason).toBe('digest_blocked')
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
    expect(target.sent[0]?.texts[0]).toContain('【话题更新合并】#ナナニジ / 3 条')
    expect(target.sent[0]?.texts[0]).not.toContain('/ target-tag-digest')
    expect(target.sent[0]?.texts[0]).toContain('正文: ライブ最高 0')
    expect(target.sent[0]?.texts[0]).not.toContain('正文: ライブ最高 0 #')
    expect(target.sent[0]?.texts[0]).toContain('其他标签: #LIVE')
    expect(target.sent[0]?.texts[0]).not.toContain('标签: #ナナニジ #LIVE')
    expect(target.sent[1]?.texts[0]).toContain('【话题更新合并】#ナナニジ / 1 条')
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

test('sendArticles folds already-forwarded referenced text except for the high realtime group', async () => {
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

    ;(pools as any).claimArticleChain = async () => true

    const lowNoiseTarget = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '161717573',
        } as any,
        'target-low-noise',
    )
    const highRealtimeTarget = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '742435777',
        } as any,
        'target-high-realtime',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async (refId: number) => {
        if (refId === 501 || refId === 502) {
            return { ref_id: 501 }
        }
        return null
    }

    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-fold-ref',
            [
                {
                    id: 500,
                    a_id: 'reply-post',
                    platform: Platform.X,
                    username: 'member',
                    u_id: 'member',
                    content: 'これは返信',
                    url: 'https://x.com/member/status/reply-post',
                    type: 'reply',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: {
                        id: 501,
                        a_id: 'already-forwarded',
                        platform: Platform.X,
                        username: 'member',
                        u_id: 'member',
                        content: 'これは前に流した本文',
                        url: 'https://x.com/member/status/already-forwarded',
                        type: 'tweet',
                        created_at: Math.floor(Date.now() / 1000) - 60,
                        ref: {
                            id: 502,
                            a_id: 'already-forwarded-parent',
                            platform: Platform.X,
                            username: 'member-b',
                            u_id: 'member_b',
                            content: 'これはさらに前に流した本文',
                            url: 'https://x.com/member_b/status/already-forwarded-parent',
                            type: 'tweet',
                            created_at: Math.floor(Date.now() / 1000) - 120,
                            ref: null,
                            has_media: false,
                            media: [],
                            extra: null,
                            u_avatar: null,
                        },
                        has_media: false,
                        media: [],
                        extra: null,
                        u_avatar: null,
                    },
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [
                {
                    forwarder: lowNoiseTarget,
                    runtime_config: undefined,
                },
                {
                    forwarder: highRealtimeTarget,
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

    expect(lowNoiseTarget.sent).toHaveLength(1)
    expect(lowNoiseTarget.sent[0]?.texts[0]).toContain('@member')
    expect(lowNoiseTarget.sent[0]?.texts[0]).toContain('@member_b')
    expect(lowNoiseTarget.sent[0]?.texts[0]).toContain('、')
    expect(lowNoiseTarget.sent[0]?.texts[0]).toContain('（略）')
    expect(lowNoiseTarget.sent[0]?.texts[0]).not.toContain('（引用已发过，正文略）')
    expect(lowNoiseTarget.sent[0]?.texts[0]).not.toContain('https://x.com/member/status/already-forwarded')
    expect(lowNoiseTarget.sent[0]?.texts[0]).not.toContain('这是前に流した本文')
    expect(lowNoiseTarget.sent[0]?.texts[0]).not.toContain('これは前に流した本文')
    expect(lowNoiseTarget.sent[0]?.texts[0]).not.toContain('これはさらに前に流した本文')

    expect(highRealtimeTarget.sent).toHaveLength(1)
    expect(highRealtimeTarget.sent[0]?.texts[0]).toContain('これは前に流した本文')
    expect(highRealtimeTarget.sent[0]?.texts[0]).not.toContain('（略）')
})

test('sendArticles keeps referenced text when it is first seen in this dispatch', async () => {
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

    const claimed: Array<number> = []
    ;(pools as any).claimArticleChain = async (article: any) => {
        claimed.push(article.id)
        return true
    }

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '161717573',
        } as any,
        'target-first-seen-ref',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null

    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-keep-new-ref',
            [
                {
                    id: 510,
                    a_id: 'reply-new-ref',
                    platform: Platform.X,
                    username: 'member',
                    u_id: 'member',
                    content: 'これは返信',
                    url: 'https://x.com/member/status/reply-new-ref',
                    type: 'reply',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: {
                        id: 511,
                        a_id: 'new-ref',
                        platform: Platform.X,
                        username: 'member',
                        u_id: 'member',
                        content: 'これはまだ流していない本文',
                        url: 'https://x.com/member/status/new-ref',
                        type: 'tweet',
                        created_at: Math.floor(Date.now() / 1000) - 60,
                        ref: null,
                        has_media: false,
                        media: [],
                        extra: null,
                        u_avatar: null,
                    },
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
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

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('これはまだ流していない本文')
    expect(target.sent[0]?.texts[0]).not.toContain('（略）')
    expect(claimed).toEqual([510])
})

test('sendArticles does not materialize collapsed reply text for card-only render results', async () => {
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
                render_type: 'img',
            } as any,
            forwarders: [],
            crawlers: [],
        },
        new EventEmitter(),
    )

    const forwardedIds = new Set<number>()
    ;(pools as any).claimArticleChain = async (article: any) => {
        forwardedIds.add(article.id)
        return true
    }
    ;(pools as any).renderService.process = async () => ({
        text: '',
        textCollapseMode: 'none',
        cardMediaFiles: [],
        originalMediaFiles: [],
        mediaFiles: [],
    })

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '161717573',
        } as any,
        'target-card-only-ref',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async (refId: number) => {
        if (refId === 516) {
            return { ref_id: 516 }
        }
        return null
    }

    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-card-only-ref',
            [
                {
                    id: 515,
                    a_id: 'reply-card-only',
                    platform: Platform.X,
                    username: 'member',
                    u_id: 'member',
                    content: 'カードだけで出したい返信',
                    url: 'https://x.com/member/status/reply-card-only',
                    type: 'reply',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: {
                        id: 516,
                        a_id: 'already-forwarded-card-ref',
                        platform: Platform.X,
                        username: 'member',
                        u_id: 'member',
                        content: 'カード専用モードでは突然テキスト化しない本文',
                        url: 'https://x.com/member/status/already-forwarded-card-ref',
                        type: 'tweet',
                        created_at: Math.floor(Date.now() / 1000) - 60,
                        ref: null,
                        has_media: false,
                        media: [],
                        extra: null,
                        u_avatar: null,
                    },
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [
                {
                    forwarder: target,
                    runtime_config: undefined,
                },
            ],
            {
                render_type: 'img',
            } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toBe('')
})

test('sendArticles does not fold old forwarded references outside the configured window', async () => {
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

    ;(pools as any).claimArticleChain = async () => true

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '161717573',
            collapse_forwarded_ref_window_seconds: 18 * 3600,
        } as any,
        'target-old-ref',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async (refId: number) => {
        if (refId === 521) {
            return { ref_id: 521 }
        }
        return null
    }

    try {
        await (pools as any).sendArticles(
            undefined,
            'manual-old-ref',
            [
                {
                    id: 520,
                    a_id: 'reply-old-ref',
                    platform: Platform.X,
                    username: 'member',
                    u_id: 'member',
                    content: 'これは返信',
                    url: 'https://x.com/member/status/reply-old-ref',
                    type: 'reply',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: {
                        id: 521,
                        a_id: 'old-ref',
                        platform: Platform.X,
                        username: 'member',
                        u_id: 'member',
                        content: '18時間より前の本文',
                        url: 'https://x.com/member/status/old-ref',
                        type: 'tweet',
                        created_at: Math.floor(Date.now() / 1000) - 19 * 3600,
                        ref: null,
                        has_media: false,
                        media: [],
                        extra: null,
                        u_avatar: null,
                    },
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
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

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('18時間より前の本文')
    expect(target.sent[0]?.texts[0]).not.toContain('（略）')
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
    const outboundKey = articleOutboundKey('target-old-skip', {
        a_id: 'website-old-skip',
        platform: 5,
    } as any)
    const outboundRecord = (DB.OutboundMessage as any).__records.get(outboundKey)
    expect(outboundRecord?.status).toBe('skipped')
    expect(outboundRecord?.provider_message_ids?.reason).toBe('old_article')
})

test('drop marks queued media-batch outbound rows skipped', async () => {
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
            media_batch_threshold: 6,
        } as any,
        'target-media-batch-drop',
    )
    ;(pools as any).forward_to.set(target.id, target)
    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const mediaFile = {
        media_type: 'photo',
        path: '/tmp/media-batch-drop.jpg',
        sourceArticleId: 'media-batch-drop',
        sourceUrl: 'https://example.com/media-batch-drop.jpg',
    }
    ;(pools as any).renderService = {
        process: async () => ({
            text: 'queued media batch',
            mediaFiles: [mediaFile],
            cardMediaFiles: [],
            originalMediaFiles: [mediaFile],
        }),
        cleanup: () => undefined,
    }

    const article = {
        id: 206,
        a_id: 'media-batch-drop',
        platform: Platform.X,
        created_at: Math.floor(Date.now() / 1000),
        content: 'queued media batch',
        ref: null,
    } as any

    await (pools as any).sendArticles(
        undefined,
        'media-batch-drop-task',
        [article],
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

    const outboundKey = articleOutboundKey(target.id, article)
    const outboundRecord = (DB.OutboundMessage as any).__records.get(outboundKey)
    expect(target.sent).toHaveLength(0)
    expect(outboundRecord?.status).toBe('queued')
    expect(outboundRecord?.provider_message_ids?.reason).toBe('media_batch')

    await pools.drop()

    expect(outboundRecord?.status).toBe('skipped')
    expect(outboundRecord?.provider_message_ids?.reason).toBe('media_batch_discarded_on_drop')
    expect(outboundRecord?.provider_message_ids?.details?.batchKey).toBeString()
})

test('sendArticles rate-limits summary-card sends to one card per interval', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 2,
                interval_seconds: 1800,
                include_original_media: false,
            },
        } as any,
        'target-summary-card',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                }
            }
            return {
                text: article.content,
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [
                    {
                        media_type: 'photo',
                        path: `/tmp/original-${article.id}.jpg`,
                        sourceArticleId: article.a_id,
                        sourceUrl: `https://example.com/${article.id}.jpg`,
                    },
                ],
                mediaFiles: [
                    {
                        media_type: 'photo',
                        path: `/tmp/original-${article.id}.jpg`,
                        sourceArticleId: article.a_id,
                        sourceUrl: `https://example.com/${article.id}.jpg`,
                    },
                ],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: (files: Array<any>) =>
            files.map((file) => ({
                type: 'photo',
                url: `data:image/png;base64,${Buffer.from(file.sourceArticleId || file.path).toString('base64')}`,
                alt: file.sourceArticleId,
            })),
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => false

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-threshold',
            [1, 2, 3].map((id) => ({
                id,
                a_id: `summary-${id}`,
                platform: Platform.X,
                username: `member${id}`,
                u_id: `member${id}`,
                content: `summary content ${id}`,
                url: `https://x.com/member/status/${id}`,
                type: 'tweet',
                created_at: Math.floor(Date.now() / 1000),
                ref: null,
                has_media: true,
                media: [{ type: 'photo', url: `https://example.com/${id}.jpg` }],
                extra: null,
                u_avatar: `https://example.com/avatar-${id}.jpg`,
            })),
            [
                {
                    forwarder: target,
                    runtime_config: undefined,
                },
            ],
            {
                render_type: 'text-card',
            } as any,
        )

        backdateSummaryCardQueues(pools as any, 1800)
        await (pools as any).flushDueSummaryCardQueues()
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(2)
    expect(target.sent[0]?.props?.forceSend).toBeTrue()
    expect(target.sent[0]?.props?.media).toEqual([{ media_type: 'photo', path: '/tmp/summary-card.png' }])
    expect(target.sent[0]?.texts[0]).toContain('聚合')
    expect(target.sent[0]?.texts[0]).not.toMatch(/\d{2}:\d{2}-\d{2}:\d{2}/)
    expect(packedArticles[0]?.content).toContain('【聚合】1 条')
    expect(packedArticles[0]?.content).not.toMatch(/\d{2}:\d{2}-\d{2}:\d{2}/)
    expect(packedArticles[0]?.content).toContain('summary content 1')
    expect(packedArticles[0]?.extra?.extra_type).toBe('message_pack_meta')
    expect(packedArticles[0]?.extra?.data?.groups?.[0]?.avatars?.[0]).toEqual({
        url: 'https://example.com/avatar-1.jpg',
        name: 'member1',
        id: 'member1',
    })
    expect(packedArticles[0]?.media).toEqual([
        {
            type: 'photo',
            url: `data:image/png;base64,${Buffer.from('summary-1').toString('base64')}`,
            alt: 'summary-1',
        },
    ])
    expect(packedArticles[1]?.content).toContain('【聚合】2 条')
    expect(packedArticles[1]?.content).toContain('summary content 2')
    expect(packedArticles[1]?.content).toContain('summary content 3')
    expect(packedArticles[1]?.extra?.data?.groups?.[0]?.items?.[0]?.text).toContain('summary content 2')
    expect(packedArticles[1]?.extra?.data?.groups).toHaveLength(2)
    expect(packedArticles[1]?.extra?.data?.groups?.[0]?.avatars?.[0]?.url).toBe('https://example.com/avatar-2.jpg')
    expect(packedArticles[1]?.extra?.data?.groups?.[1]?.avatars?.[0]?.url).toBe('https://example.com/avatar-3.jpg')
    expect(target.sent[1]?.props?.media).toEqual([{ media_type: 'photo', path: '/tmp/summary-card.png' }])
})

test('sendArticles does not flush a fresh summary-card queue from stale last sent time', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
            },
        } as any,
        'target-summary-card-stale-last-send',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content,
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [],
            mediaFiles: [],
        }),
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }
    ;(pools as any).summaryCardLastSentAt.set(target.id, Math.floor(Date.now() / 1000) - 7200)

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-fresh-after-idle',
            [
                {
                    id: 710,
                    a_id: 'summary-fresh-after-idle',
                    platform: Platform.X,
                    username: 'fresh member',
                    u_id: 'fresh_member',
                    content: 'fresh queue should wait',
                    url: 'https://x.com/fresh_member/status/710',
                    type: 'tweet',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: null,
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
        await (pools as any).flushDueSummaryCardQueues()
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(0)
    expect(getSummaryCardQueueForTarget(pools, target.id)?.items.size).toBe(1)
})

test('ForwarderPools drop does not visibly send a fresh summary-card queue', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
            },
        } as any,
        'target-summary-card-drop-no-send',
    )

    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content,
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [],
            mediaFiles: [],
        }),
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'summary-drop-no-send',
        [
            {
                id: 715,
                a_id: 'summary-drop-no-send',
                platform: Platform.X,
                username: 'drop member',
                u_id: 'drop_member',
                content: 'fresh queue should survive without visible send',
                url: 'https://x.com/drop_member/status/715',
                type: 'tweet',
                created_at: Math.floor(Date.now() / 1000),
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
            },
        ],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    expect(getSummaryCardQueueForTarget(pools, target.id)?.items.size).toBe(1)
    await pools.drop()
    expect(target.sent).toHaveLength(0)
})

test('flushSummaryCardQueue cancels durable windows when no queued items are claimable', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
            },
        } as any,
        'target-summary-card-no-claimable-items',
    )

    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content,
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [],
            mediaFiles: [],
        }),
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'summary-no-claimable-items',
        [
            {
                id: 716,
                a_id: 'summary-no-claimable-items',
                platform: Platform.X,
                username: 'claimed elsewhere',
                u_id: 'claimed_elsewhere',
                content: 'durable window should not stay open',
                url: 'https://x.com/claimed_elsewhere/status/716',
                type: 'tweet',
                created_at: Math.floor(Date.now() / 1000),
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
            },
        ],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    const queueKey = Array.from((pools as any).summaryCardQueues.keys())[0]
    const queue = (pools as any).summaryCardQueues.get(queueKey)
    const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
    expect(windows.get(queue.windowId)?.status).toBe('open')

    ;(pools as any).claimArticleChain = async () => false
    await (pools as any).flushSummaryCardQueue(queueKey, 'interval')

    expect(target.sent).toHaveLength(0)
    expect((pools as any).summaryCardQueues.has(queueKey)).toBeFalse()
    expect(windows.get(queue.windowId)?.status).toBe('cancelled')
    expect(windows.get(queue.windowId)?.payload_hash).toBe('no-claimable-items')
})

test('flushSummaryCardQueue treats blocked summary-card outbound as terminally suppressed', async () => {
    class BlockingSummaryForwarder extends Forwarder {
        NAME = 'recording'
        calls: Array<{ text: string; props: any }> = []

        public override async send(text: string, props?: any): Promise<any> {
            this.calls.push({ text, props })
            return { status: 'blocked', reason: 'summary_card_blocked' }
        }

        protected async realSend(): Promise<any> {
            return { ok: true }
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

    const target = new BlockingSummaryForwarder(
        {
            block_until: '32h',
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
            },
        } as any,
        'target-summary-card-blocked-terminal',
    )

    const claimed = new Set<number>()
    const released: number[] = []
    ;(pools as any).claimArticleChain = async (article: any) => {
        claimed.add(article.id)
        return true
    }
    ;(pools as any).releaseArticleChain = async (article: any) => {
        released.push(article.id)
        claimed.delete(article.id)
    }
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-blocked.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-blocked.png' }],
                }
            }
            return {
                text: article.content,
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'summary-blocked-terminal',
        [
            {
                id: 717,
                a_id: 'summary-blocked-terminal',
                platform: Platform.X,
                username: 'blocked summary',
                u_id: 'blocked_summary',
                content: 'blocked summary-card should not retry',
                url: 'https://x.com/blocked_summary/status/717',
                type: 'tweet',
                created_at: Math.floor(Date.now() / 1000),
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
            },
        ],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    const queueKey = Array.from((pools as any).summaryCardQueues.keys())[0]
    const queue = (pools as any).summaryCardQueues.get(queueKey)
    await (pools as any).flushSummaryCardQueue(queueKey, 'interval')

    const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
    const outboundRecords = Array.from(((DB.OutboundMessage as any).__records as Map<string, any>).values())
    const summaryOutbound = outboundRecords.find((record: any) => record.task_kind === 'summary_card')
    expect(target.calls).toHaveLength(1)
    expect(target.calls[0]?.props?.forceSend).toBeTrue()
    expect(released).toEqual([])
    expect(Array.from(claimed)).toEqual([717])
    expect((pools as any).summaryCardQueues.has(queueKey)).toBeFalse()
    expect(windows.get(queue.windowId)?.status).toBe('cancelled')
    expect(summaryOutbound?.status).toBe('skipped')
    expect(summaryOutbound?.provider_message_ids?.reason).toBe('summary_card_blocked')
})

test('flushSummaryCardQueue keeps summary-card windows retryable after transient send failure', async () => {
    class FlakySummaryForwarder extends Forwarder {
        NAME = 'recording'
        calls: Array<{ text: string; props: any }> = []

        public override async send(text: string, props?: any): Promise<any> {
            this.calls.push({ text, props })
            if (this.calls.length === 1) {
                throw new Error('temporary provider outage')
            }
            return { status: 'sent', providerResult: { ok: true } }
        }

        protected async realSend(): Promise<any> {
            return { ok: true }
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

    const target = new FlakySummaryForwarder(
        {
            block_until: '32h',
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
            },
        } as any,
        'target-summary-card-transient-failure',
    )

    const claimed = new Set<number>()
    const released: number[] = []
    ;(pools as any).claimArticleChain = async (article: any) => {
        if (claimed.has(article.id)) {
            return false
        }
        claimed.add(article.id)
        return true
    }
    ;(pools as any).releaseArticleChain = async (article: any) => {
        released.push(article.id)
        claimed.delete(article.id)
    }
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-retry.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-retry.png' }],
                }
            }
            return {
                text: article.content,
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'summary-transient-failure',
        [
            {
                id: 718,
                a_id: 'summary-transient-failure',
                platform: Platform.X,
                username: 'retry summary',
                u_id: 'retry_summary',
                content: 'summary-card should retry after transient failure',
                url: 'https://x.com/retry_summary/status/718',
                type: 'tweet',
                created_at: Math.floor(Date.now() / 1000),
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
            },
        ],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    const queueKey = Array.from((pools as any).summaryCardQueues.keys())[0]
    const queue = (pools as any).summaryCardQueues.get(queueKey)
    const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
    const outboundRecords = (DB.OutboundMessage as any).__records as Map<string, any>

    await (pools as any).flushSummaryCardQueue(queueKey, 'interval')

    const failedOutbound = Array.from(outboundRecords.values()).find(
        (record: any) => record.task_kind === 'summary_card',
    )
    expect(target.calls).toHaveLength(1)
    expect(released).toEqual([718])
    expect(Array.from(claimed)).toEqual([])
    expect((pools as any).summaryCardQueues.has(queueKey)).toBeTrue()
    expect(windows.get(queue.windowId)?.status).toBe('open')
    expect(failedOutbound?.status).toBe('failed')

    await (pools as any).flushSummaryCardQueue(queueKey, 'interval')

    const sentOutbound = Array.from(outboundRecords.values()).find(
        (record: any) => record.task_kind === 'summary_card',
    )
    expect(target.calls).toHaveLength(2)
    expect(released).toEqual([718])
    expect(Array.from(claimed)).toEqual([718])
    expect((pools as any).summaryCardQueues.has(queueKey)).toBeFalse()
    expect(windows.get(queue.windowId)?.status).toBe('completed')
    expect(sentOutbound?.status).toBe('sent')
})

test('summary-card queues are isolated by route for the same target', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
            },
        } as any,
        'target-summary-card-route-isolation',
    )

    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content,
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [],
            mediaFiles: [],
        }),
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const now = Math.floor(Date.now() / 1000)
    for (const route of ['route-a', 'route-b']) {
        const index = route === 'route-a' ? 0 : 1
        await (pools as any).sendArticles(
            undefined,
            `summary-route-isolation-${index}`,
            [
                {
                    id: 720 + index,
                    a_id: `summary-route-isolation-${index}`,
                    platform: Platform.X,
                    username: `route member ${index}`,
                    u_id: `route_member_${index}`,
                    content: `route isolated text ${index}`,
                    url: `https://x.com/route_member/status/${index}`,
                    type: 'tweet',
                    created_at: now + index,
                    ref: null,
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
            undefined,
            { routeKey: route },
        )
    }

    const queues = Array.from((pools as any).summaryCardQueues.values()).filter(
        (queue: any) => queue.target.id === target.id,
    )
    expect(queues).toHaveLength(2)
    expect(new Set(queues.map((queue: any) => queue.routeKey))).toEqual(
        new Set([`route-a:target:${target.id}`, `route-b:target:${target.id}`]),
    )
})

test('sendArticles records suppressed payload drift for target-once article outbox keys', async () => {
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

    const target = new RecordingForwarder({ block_until: '32h' } as any, 'target-article-payload-drift')
    const article = {
        id: 730,
        a_id: 'article-payload-drift',
        platform: Platform.X,
        username: 'drift member',
        u_id: 'drift_member',
        content: 'new payload text',
        url: 'https://x.com/drift_member/status/730',
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    } as any

    ;(pools as any).renderService = {
        process: async () => ({
            text: 'new rendered payload',
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [],
            mediaFiles: [],
        }),
        renderText: () => 'new rendered payload',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const outboundRecords = (DB.OutboundMessage as any).__records as Map<string, any>
    const outboundKey = articleOutboundKey(target.id, article)
    outboundRecords.set(outboundKey, {
        id: 1,
        idempotency_key: outboundKey,
        route_key: 'old-route',
        target_id: target.id,
        target_platform: target.NAME,
        task_kind: 'article',
        article_key: `${article.platform}:${article.a_id}`,
        synthetic_key: null,
        payload_hash: 'old-payload-hash',
        status: 'sent',
        created_at: article.created_at - 60,
        updated_at: article.created_at - 60,
        attempt_count: 1,
    })

    await (pools as any).sendArticles(
        undefined,
        'article-payload-drift-task',
        [article],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text' } as any,
        undefined,
        { routeKey: 'new-route' },
    )

    const record = outboundRecords.get(outboundKey)
    expect(target.sent).toHaveLength(0)
    expect(record.segment_results?.diagnostic).toBe('suppressed_payload_drift')
    expect(record.segment_results?.existing?.payload_hash).toBe('old-payload-hash')
    expect(record.segment_results?.incoming?.payload_hash).not.toBe('old-payload-hash')
})

test('restoreSummaryCardQueues cancels stale open windows instead of restoring them', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                send_first_immediately: false,
            },
        } as any,
        'target-summary-card-stale-window',
    )
    ;(pools as any).forward_to.set(target.id, target)

    const now = Math.floor(Date.now() / 1000)
    const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
    windows.set(1, {
        id: 1,
        idempotency_key: 'stale-summary-window',
        route_key: `route-stale:target:${target.id}`,
        target_id: target.id,
        mode: 'summary_card',
        window_start: now - 24 * 3600 - 1800,
        window_end: now - 24 * 3600,
        status: 'open',
        created_at: now - 24 * 3600,
        updated_at: now - 24 * 3600,
        finished_at: null,
        payload_hash: null,
    })

    await (pools as any).restoreSummaryCardQueues()

    expect(windows.get(1)?.status).toBe('cancelled')
    expect(windows.get(1)?.payload_hash).toBe('stale-window')
    expect((pools as any).summaryCardQueues.size).toBe(0)
    expect(target.sent).toHaveLength(0)
})

test('restoreSummaryCardQueues restores compatible open windows using current target config', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                max_items: 14,
                include_original_media: false,
                send_first_immediately: false,
            },
        } as any,
        'target-summary-card-compatible-restore',
    )
    ;(pools as any).forward_to.set(target.id, target)

    const now = Math.floor(Date.now() / 1000)
    const article = {
        id: 740,
        a_id: 'summary-compatible-restore',
        platform: Platform.X,
        username: 'restore member',
        u_id: 'restore_member',
        content: 'compatible restore should keep queue',
        url: 'https://x.com/restore_member/status/740',
        type: 'tweet',
        created_at: now - 60,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    } as any
    const summaryConfig = {
        intervalSeconds: 1800,
        threshold: 8,
        maxItems: 14,
        includeOriginalMedia: false,
        sendFirstImmediately: false,
        sendFirstNative: false,
        mediaRealtime: false,
        mediaRealtimeText: 'none',
        flushOnThreshold: true,
        flushDelaySeconds: 0,
        windowAlignment: 'none',
        mediaDuplicateLimit: null,
    }
    const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
    const items = (DB.AggregationWindow as any).__items as Map<string, any>
    windows.set(1, {
        id: 1,
        idempotency_key: 'compatible-summary-window',
        route_key: `route-compatible:target:${target.id}`,
        target_id: target.id,
        mode: 'summary_card',
        window_start: now - 300,
        window_end: now + 1500,
        status: 'open',
        created_at: now - 300,
        updated_at: now - 300,
        finished_at: null,
        payload_hash: null,
    })
    items.set('1:compatible-summary-item', {
        id: 1,
        window_id: 1,
        article_key: 'compatible-summary-item',
        article_row_id: article.id,
        platform: article.platform,
        payload: {
            queuedAt: now - 120,
            runtime_config: {
                collapse_forwarded_ref_text: false,
                summary_card: {
                    enabled: true,
                    interval_seconds: 9999,
                },
            },
            summaryConfig,
        },
        created_at: now - 120,
    })

    const originalGetSingleArticle = DB.Article.getSingleArticle
    ;(DB.Article as any).getSingleArticle = async () => article
    try {
        await (pools as any).restoreSummaryCardQueues()
    } finally {
        ;(DB.Article as any).getSingleArticle = originalGetSingleArticle
    }

    const queues = Array.from((pools as any).summaryCardQueues.values()) as Array<any>
    expect(windows.get(1)?.status).toBe('open')
    expect(queues).toHaveLength(1)
    expect(queues[0]?.config.intervalSeconds).toBe(1800)
    expect(queues[0]?.runtime_config?.collapse_forwarded_ref_text).toBeFalse()
    expect(queues[0]?.runtime_config?.summary_card).toBeUndefined()
    expect(queues[0]?.items.get(article.id)?.article.a_id).toBe(article.a_id)
    expect(target.sent).toHaveLength(0)
})

test('restoreSummaryCardQueues restores windows configured through route runtime config', async () => {
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
        'target-summary-card-runtime-restore',
    )
    ;(pools as any).forward_to.set(target.id, target)

    const now = Math.floor(Date.now() / 1000)
    const article = {
        id: 742,
        a_id: 'summary-runtime-restore',
        platform: Platform.X,
        username: 'runtime restore',
        u_id: 'runtime_restore',
        content: 'runtime-config summary-card should survive restart',
        url: 'https://x.com/runtime_restore/status/742',
        type: 'tweet',
        created_at: now - 60,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    } as any
    const runtime_config = {
        collapse_forwarded_ref_text: false,
        summary_card: {
            enabled: true,
            threshold: 8,
            interval_seconds: 1800,
            max_items: 14,
            include_original_media: false,
            send_first_immediately: false,
        },
    }
    const summaryConfig = {
        intervalSeconds: 1800,
        threshold: 8,
        maxItems: 14,
        includeOriginalMedia: false,
        sendFirstImmediately: false,
        sendFirstNative: false,
        mediaRealtime: false,
        mediaRealtimeText: 'none',
        flushOnThreshold: true,
        flushDelaySeconds: 0,
        windowAlignment: 'none',
        mediaDuplicateLimit: null,
    }
    const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
    const items = (DB.AggregationWindow as any).__items as Map<string, any>
    windows.set(1, {
        id: 1,
        idempotency_key: 'runtime-summary-window',
        route_key: `route-runtime:target:${target.id}`,
        target_id: target.id,
        mode: 'summary_card',
        window_start: now - 300,
        window_end: now + 1500,
        status: 'open',
        created_at: now - 300,
        updated_at: now - 300,
        finished_at: null,
        payload_hash: null,
    })
    items.set('1:runtime-summary-item', {
        id: 1,
        window_id: 1,
        article_key: 'runtime-summary-item',
        article_row_id: article.id,
        platform: article.platform,
        payload: {
            queuedAt: now - 120,
            runtime_config,
            summaryConfig,
        },
        created_at: now - 120,
    })

    const originalGetSingleArticle = DB.Article.getSingleArticle
    ;(DB.Article as any).getSingleArticle = async () => article
    try {
        await (pools as any).restoreSummaryCardQueues()
    } finally {
        ;(DB.Article as any).getSingleArticle = originalGetSingleArticle
    }

    const queues = Array.from((pools as any).summaryCardQueues.values()) as Array<any>
    expect(windows.get(1)?.status).toBe('open')
    expect(queues).toHaveLength(1)
    expect(queues[0]?.config.intervalSeconds).toBe(1800)
    expect(queues[0]?.runtime_config?.collapse_forwarded_ref_text).toBeFalse()
    expect(queues[0]?.runtime_config?.summary_card).toBeUndefined()
    expect(queues[0]?.items.get(article.id)?.article.a_id).toBe(article.a_id)
    expect(target.sent).toHaveLength(0)
})

test('restoreSummaryCardQueues cancels open windows when summary-card is currently disabled', async () => {
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
            summary_card: {
                enabled: false,
            },
        } as any,
        'target-summary-card-disabled-restore',
    )
    ;(pools as any).forward_to.set(target.id, target)

    const now = Math.floor(Date.now() / 1000)
    const article = {
        id: 741,
        a_id: 'summary-disabled-restore',
        platform: Platform.X,
        username: 'disabled restore',
        u_id: 'disabled_restore',
        content: 'disabled summary-card should cancel old window',
        url: 'https://x.com/disabled_restore/status/741',
        type: 'tweet',
        created_at: now - 60,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    } as any
    const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
    const items = (DB.AggregationWindow as any).__items as Map<string, any>
    windows.set(1, {
        id: 1,
        idempotency_key: 'disabled-summary-window',
        route_key: `route-disabled:target:${target.id}`,
        target_id: target.id,
        mode: 'summary_card',
        window_start: now - 300,
        window_end: now + 1500,
        status: 'open',
        created_at: now - 300,
        updated_at: now - 300,
        finished_at: null,
        payload_hash: null,
    })
    items.set('1:disabled-summary-item', {
        id: 1,
        window_id: 1,
        article_key: 'disabled-summary-item',
        article_row_id: article.id,
        platform: article.platform,
        payload: {
            queuedAt: now - 120,
            runtime_config: {
                summary_card: {
                    enabled: true,
                    interval_seconds: 1800,
                },
            },
            summaryConfig: {
                intervalSeconds: 1800,
                threshold: 8,
                maxItems: 14,
                includeOriginalMedia: false,
                sendFirstImmediately: false,
                sendFirstNative: false,
                mediaRealtime: false,
                mediaRealtimeText: 'none',
                flushOnThreshold: true,
                flushDelaySeconds: 0,
                windowAlignment: 'none',
                mediaDuplicateLimit: null,
            },
        },
        created_at: now - 120,
    })

    const originalGetSingleArticle = DB.Article.getSingleArticle
    ;(DB.Article as any).getSingleArticle = async () => article
    try {
        await (pools as any).restoreSummaryCardQueues()
    } finally {
        ;(DB.Article as any).getSingleArticle = originalGetSingleArticle
    }

    expect(windows.get(1)?.status).toBe('cancelled')
    expect(windows.get(1)?.payload_hash).toBe('summary-card-disabled')
    expect((pools as any).summaryCardQueues.size).toBe(0)
    expect(target.sent).toHaveLength(0)
})

test('restoreSummaryCardQueues cancels open windows when summary-card config changed', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 7200,
                max_items: 14,
                include_original_media: false,
                send_first_immediately: false,
            },
        } as any,
        'target-summary-card-config-changed-restore',
    )
    ;(pools as any).forward_to.set(target.id, target)

    const now = Math.floor(Date.now() / 1000)
    const article = {
        id: 742,
        a_id: 'summary-config-changed-restore',
        platform: Platform.X,
        username: 'changed restore',
        u_id: 'changed_restore',
        content: 'changed summary-card config should cancel old window',
        url: 'https://x.com/changed_restore/status/742',
        type: 'tweet',
        created_at: now - 60,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    } as any
    const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
    const items = (DB.AggregationWindow as any).__items as Map<string, any>
    windows.set(1, {
        id: 1,
        idempotency_key: 'changed-summary-window',
        route_key: `route-changed:target:${target.id}`,
        target_id: target.id,
        mode: 'summary_card',
        window_start: now - 300,
        window_end: now + 1500,
        status: 'open',
        created_at: now - 300,
        updated_at: now - 300,
        finished_at: null,
        payload_hash: null,
    })
    items.set('1:changed-summary-item', {
        id: 1,
        window_id: 1,
        article_key: 'changed-summary-item',
        article_row_id: article.id,
        platform: article.platform,
        payload: {
            queuedAt: now - 120,
            summaryConfig: {
                intervalSeconds: 1800,
                threshold: 8,
                maxItems: 14,
                includeOriginalMedia: false,
                sendFirstImmediately: false,
                sendFirstNative: false,
                mediaRealtime: false,
                mediaRealtimeText: 'none',
                flushOnThreshold: true,
                flushDelaySeconds: 0,
                windowAlignment: 'none',
                mediaDuplicateLimit: null,
            },
        },
        created_at: now - 120,
    })

    const originalGetSingleArticle = DB.Article.getSingleArticle
    ;(DB.Article as any).getSingleArticle = async () => article
    try {
        await (pools as any).restoreSummaryCardQueues()
    } finally {
        ;(DB.Article as any).getSingleArticle = originalGetSingleArticle
    }

    expect(windows.get(1)?.status).toBe('cancelled')
    expect(windows.get(1)?.payload_hash).toBe('summary-card-config-changed')
    expect((pools as any).summaryCardQueues.size).toBe(0)
    expect(target.sent).toHaveLength(0)
})

test('sendArticles folds idle-first summary-card item when it appears as a later reply reference', async () => {
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
            group_id: '161717573',
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
            },
        } as any,
        'target-summary-card-fold-ref',
    )

    const forwardedIds = new Set<number>()
    ;(pools as any).claimArticleChain = async (article: any) => {
        forwardedIds.add(article.id)
        return true
    }
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    const renderTextCalls: Array<{ article: any; collapsedArticleIds?: Set<string | number> }> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                }
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any, config?: any) => {
            renderTextCalls.push({ article, collapsedArticleIds: config?.collapsedArticleIds })
            if (article.ref && config?.collapsedArticleIds?.has(article.ref.id)) {
                return '@reply_member 2325⁹ X回复\n\nreply body\n------------\n@first_member 2320⁹（略）'
            }
            if (article.ref) {
                return `@reply_member 2325⁹ X回复\n\nreply body\n------------\n@first_member 2320⁹ X发推\n\n${article.ref.content}`
            }
            return article.content || ''
        },
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const firstCreatedAt = Math.floor(Date.now() / 1000)
    const firstArticle = {
        id: 301,
        a_id: 'idle-first-parent',
        platform: Platform.X,
        username: 'first member',
        u_id: 'first_member',
        content: 'first body should not repeat',
        url: 'https://x.com/first_member/status/301',
        type: 'tweet',
        created_at: firstCreatedAt,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }
    const replyArticle = {
        id: 302,
        a_id: 'reply-to-idle-first',
        platform: Platform.X,
        username: 'reply member',
        u_id: 'reply_member',
        content: 'reply body',
        url: 'https://x.com/reply_member/status/302',
        type: 'reply',
        created_at: firstCreatedAt + 60,
        ref: firstArticle,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async (refId: number) => {
        if (forwardedIds.has(refId)) {
            return { ref_id: refId }
        }
        return null
    }

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-idle-first-parent',
            [firstArticle],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
        await (pools as any).sendArticles(
            undefined,
            'summary-reply-after-idle-first',
            [replyArticle],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )

        backdateSummaryCardQueues(pools as any, 1800)
        await (pools as any).flushDueSummaryCardQueues()
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(2)
    expect(packedArticles).toHaveLength(2)
    expect(packedArticles[1]?.extra?.data?.groups?.[0]?.items?.[0]?.text).toContain('@first_member 2320⁹（略）')
    expect(packedArticles[1]?.extra?.data?.groups?.[0]?.items?.[0]?.text).not.toContain('first body should not repeat')
    expect(renderTextCalls.some((call) => call.article.id === 302 && call.collapsedArticleIds?.has(301))).toBeTrue()
})

test('sendArticles promotes queued summary-card hashtag items after a storm activates', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
            },
            tag_digest_threshold: 3,
            tag_digest_min_authors: 2,
            tag_digest_detection_window_seconds: 300,
            tag_digest_window_seconds: 1800,
        } as any,
        'target-summary-card-tag-storm',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                }
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        for (const index of [0, 1, 2]) {
            await (pools as any).sendArticles(
                undefined,
                `summary-tag-storm-${index}`,
                [
                    {
                        id: 510 + index,
                        a_id: `summary-tag-storm-${index}`,
                        platform: Platform.X,
                        username: `tag member ${index}`,
                        u_id: `tag_member_${index}`,
                        content: `話題 ${index} #ナナニジ`,
                        url: `https://x.com/member/status/tag-storm-${index}`,
                        type: 'tweet',
                        created_at: now + index,
                        ref: null,
                        has_media: false,
                        media: [],
                        extra: null,
                        u_avatar: null,
                    },
                ],
                [{ forwarder: target, runtime_config: undefined }],
                { render_type: 'text-card' } as any,
            )
        }

        backdateSummaryCardQueues(pools as any, 1800)
        await (pools as any).flushDueSummaryCardQueues()
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(2)
    expect(packedArticles).toHaveLength(2)
    const stormGroups = packedArticles[1]?.extra?.data?.groups || []
    expect(stormGroups).toHaveLength(1)
    expect(stormGroups[0]?.kind).toBe('storm')
    expect(stormGroups[0]?.label).toBe('#ナナニジ')
    expect(stormGroups[0]?.items).toHaveLength(2)
})

test('sendArticles keeps first queued summary-card item inside a delayed hashtag storm', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 7200,
                include_original_media: false,
                send_first_immediately: false,
            },
            tag_digest_threshold: 2,
            tag_digest_min_authors: 2,
            tag_digest_detection_window_seconds: 600,
            tag_digest_window_seconds: 7200,
        } as any,
        'target-summary-card-delayed-tag-storm',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                }
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        for (const index of [0, 1, 2]) {
            await (pools as any).sendArticles(
                undefined,
                `summary-delayed-tag-storm-${index}`,
                [
                    {
                        id: 540 + index,
                        a_id: `summary-delayed-tag-storm-${index}`,
                        platform: Platform.X,
                        username: `tag member ${index}`,
                        u_id: `tag_member_${index}`,
                        content: `話題 ${index} #ナナニジ`,
                        url: `https://x.com/member/status/delayed-tag-storm-${index}`,
                        type: 'tweet',
                        created_at: now + index,
                        ref: null,
                        has_media: false,
                        media: [],
                        extra: null,
                        u_avatar: null,
                    },
                ],
                [{ forwarder: target, runtime_config: undefined }],
                { render_type: 'text-card' } as any,
            )
        }

        await (pools as any).flushAllSummaryCardQueues()
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(packedArticles).toHaveLength(1)
    const stormGroups = packedArticles[0]?.extra?.data?.groups || []
    expect(stormGroups).toHaveLength(1)
    expect(stormGroups[0]?.kind).toBe('storm')
    expect(stormGroups[0]?.label).toBe('#ナナニジ')
    expect(stormGroups[0]?.items).toHaveLength(3)
})

test('sendArticles waits for a near-threshold summary-card hashtag storm instead of threshold-flushing early', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 2,
                interval_seconds: 7200,
                include_original_media: false,
                send_first_immediately: false,
            },
            tag_digest_threshold: 3,
            tag_digest_min_authors: 2,
            tag_digest_detection_window_seconds: 600,
            tag_digest_window_seconds: 7200,
        } as any,
        'target-summary-card-near-tag-storm',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card.png' }],
                }
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        for (const index of [0, 1]) {
            await (pools as any).sendArticles(
                undefined,
                `summary-near-tag-storm-${index}`,
                [
                    {
                        id: 550 + index,
                        a_id: `summary-near-tag-storm-${index}`,
                        platform: Platform.X,
                        username: `tag member ${index}`,
                        u_id: `tag_member_${index}`,
                        content: `話題 ${index} #ナナニジ`,
                        url: `https://x.com/member/status/near-tag-storm-${index}`,
                        type: 'tweet',
                        created_at: now + index,
                        ref: null,
                        has_media: false,
                        media: [],
                        extra: null,
                        u_avatar: null,
                    },
                ],
                [{ forwarder: target, runtime_config: undefined }],
                { render_type: 'text-card' } as any,
            )
        }

        expect(target.sent).toHaveLength(0)

        await (pools as any).sendArticles(
            undefined,
            'summary-near-tag-storm-2',
            [
                {
                    id: 552,
                    a_id: 'summary-near-tag-storm-2',
                    platform: Platform.X,
                    username: 'tag member 2',
                    u_id: 'tag_member_2',
                    content: '話題 2 #ナナニジ',
                    url: 'https://x.com/member/status/near-tag-storm-2',
                    type: 'tweet',
                    created_at: now + 2,
                    ref: null,
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(packedArticles).toHaveLength(1)
    expect(packedArticles[0]?.extra?.data?.groups?.[0]?.kind).toBe('storm')
    expect(packedArticles[0]?.extra?.data?.groups?.[0]?.items).toHaveLength(3)
})

test('sendArticles keeps summary-card fallback compact when card rendering fails', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
            },
        } as any,
        'target-summary-card-media-only',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [],
                    originalMediaFiles: [],
                    mediaFiles: [],
                }
            }
            return {
                text: '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [
                    {
                        media_type: 'photo',
                        path: '/tmp/original-media-only.jpg',
                        sourceArticleId: article.a_id,
                        sourceUrl: 'https://example.com/media-only.jpg',
                    },
                ],
                mediaFiles: [
                    {
                        media_type: 'photo',
                        path: '/tmp/original-media-only.jpg',
                        sourceArticleId: article.a_id,
                        sourceUrl: 'https://example.com/media-only.jpg',
                    },
                ],
            }
        },
        renderText: () => '',
        buildCardMediaFromRenderedFiles: (files: Array<any>) =>
            files.map((file) => ({
                type: 'photo',
                url: `data:image/png;base64,${Buffer.from(file.sourceArticleId || file.path).toString('base64')}`,
                alt: file.sourceArticleId,
            })),
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => false
    const now = Math.floor(Date.now() / 1000)

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-media-only',
            [
                {
                    id: 11,
                    a_id: 'media-only-11',
                    platform: Platform.X,
                    username: 'media member',
                    u_id: 'media_member',
                    content: null,
                    url: 'https://x.com/media_member/status/11',
                    type: 'tweet',
                    created_at: now,
                    ref: null,
                    has_media: true,
                    media: [{ type: 'photo', url: 'https://example.com/media-only.jpg' }],
                    extra: null,
                    u_avatar: 'https://example.com/avatar-media.jpg',
                },
            ],
            [
                {
                    forwarder: target,
                    runtime_config: undefined,
                },
            ],
            {
                render_type: 'text-card',
            } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('聚合 1条 /')
    expect(target.sent[0]?.texts[0]).not.toContain('@media_member')
    expect(target.sent[0]?.texts[0]).not.toContain('图集: 1 张')
    expect(target.sent[0]?.texts[0]).toBe(packedArticles[0]?.content?.split('\n')[0])
    expect(target.sent[0]?.props?.cardMedia).toEqual([])
    expect(packedArticles[0]?.extra?.data?.groups?.[0]?.items?.[0]?.text).toContain('@media_member')
    expect(packedArticles[0]?.extra?.data?.groups?.[0]?.items?.[0]?.media).toEqual([
        {
            type: 'photo',
            url: `data:image/png;base64,${Buffer.from('media-only-11').toString('base64')}`,
            alt: 'media-only-11',
        },
    ])
})

test('sendArticles sends summary-card media immediately while keeping text queued', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 2,
                interval_seconds: 7200,
                include_original_media: false,
                send_first_immediately: false,
                media_realtime: true,
                flush_on_threshold: false,
                align_to_hour: true,
                flush_delay_seconds: 300,
            },
        } as any,
        'target-summary-card-realtime-media',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content || '',
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [
                {
                    media_type: 'photo',
                    path: `/tmp/realtime-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-${article.id}.jpg`,
                },
            ],
            mediaFiles: [
                {
                    media_type: 'photo',
                    path: `/tmp/realtime-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-${article.id}.jpg`,
                },
            ],
        }),
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        for (const id of [810, 811]) {
            await (pools as any).sendArticles(
                undefined,
                `summary-realtime-media-${id}`,
                [
                    {
                        id,
                        a_id: `summary-realtime-media-${id}`,
                        platform: Platform.X,
                        username: 'media member',
                        u_id: 'media_member',
                        content: `summary text ${id}`,
                        url: `https://x.com/media_member/status/${id}`,
                        type: 'tweet',
                        created_at: now + id,
                        ref: null,
                        has_media: true,
                        media: [{ type: 'photo', url: `https://example.com/realtime-${id}.jpg` }],
                        extra: null,
                        u_avatar: null,
                    },
                ],
                [{ forwarder: target, runtime_config: undefined }],
                { render_type: 'text-card' } as any,
            )
        }
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(2)
    expect(target.sent[0]?.texts).toEqual([''])
    expect(target.sent[0]?.props?.forceSend).toBeUndefined()
    expect(target.sent[0]?.props?.bypassMediaBatch).toBeTrue()
    expect(target.sent[0]?.props?.media?.[0]?.path).toBe('/tmp/realtime-810.jpg')
    expect(getSummaryCardQueueForTarget(pools, target.id)?.items.size).toBe(2)
})

test('summary-card realtime media visibility skips repeats without dropping new media', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 7200,
                include_original_media: false,
                send_first_immediately: false,
                media_realtime: true,
                flush_on_threshold: false,
            },
            media_visibility: {
                enabled: true,
                window_seconds: 432000,
                max_visible: 1,
                duplicate_behavior: 'skip',
            },
        } as any,
        'target-summary-card-realtime-media-visibility',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = {
        process: async (article: any) => {
            const files =
                article.id === 901
                    ? [
                          {
                              media_type: 'photo',
                              path: `/tmp/realtime-same-${article.id}.jpg`,
                              sourceArticleId: article.a_id,
                              sourceUrl: 'https://example.com/realtime-same.jpg',
                              content_hash: 'realtime-same-hash',
                          },
                          {
                              media_type: 'photo',
                              path: `/tmp/realtime-fresh-${article.id}.jpg`,
                              sourceArticleId: article.a_id,
                              sourceUrl: `https://example.com/realtime-fresh-${article.id}.jpg`,
                              content_hash: `realtime-fresh-hash-${article.id}`,
                          },
                      ]
                    : [
                          {
                              media_type: 'photo',
                              path: `/tmp/realtime-same-${article.id}.jpg`,
                              sourceArticleId: article.a_id,
                              sourceUrl: 'https://example.com/realtime-same.jpg',
                              content_hash: 'realtime-same-hash',
                          },
                      ]
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: files,
                mediaFiles: files,
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const now = Math.floor(Date.now() / 1000)
    for (const id of [900, 901, 902]) {
        await (pools as any).sendArticles(
            undefined,
            `summary-realtime-media-visibility-${id}`,
            [
                {
                    id,
                    a_id: `summary-realtime-media-visibility-${id}`,
                    platform: Platform.X,
                    username: 'media member',
                    u_id: 'media_member',
                    content: `summary visibility text ${id}`,
                    url: `https://x.com/media_member/status/${id}`,
                    type: 'tweet',
                    created_at: now + id,
                    ref: null,
                    has_media: true,
                    media: [{ type: 'photo', url: 'https://example.com/realtime-same.jpg' }],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    }

    expect(target.sent).toHaveLength(2)
    expect(target.sent[0]?.props?.media?.map((file: any) => file.path)).toEqual(['/tmp/realtime-same-900.jpg'])
    expect(target.sent[1]?.props?.media?.map((file: any) => file.path)).toEqual(['/tmp/realtime-fresh-901.jpg'])
    expect(target.sent.every((send) => (send.props?.media || []).length > 0)).toBeTrue()
    expect(getSummaryCardQueueForTarget(pools, target.id)?.items.size).toBe(3)
})

test('summary-card realtime media visibility releases reservations after failed sends', async () => {
    class FlakyForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            if (props?.article?.id === 903) {
                throw new Error('simulated provider failure')
            }
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

    const target = new FlakyForwarder(
        {
            block_until: '32h',
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 7200,
                include_original_media: false,
                send_first_immediately: false,
                media_realtime: true,
                flush_on_threshold: false,
            },
            media_visibility: {
                enabled: true,
                window_seconds: 432000,
                max_visible: 1,
                duplicate_behavior: 'skip',
            },
        } as any,
        'target-summary-card-realtime-media-visibility-release',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = {
        process: async (article: any) => {
            const file = {
                media_type: 'photo',
                path: `/tmp/realtime-release-${article.id}.jpg`,
                sourceArticleId: article.a_id,
                sourceUrl: 'https://example.com/realtime-release-same.jpg',
                content_hash: 'realtime-release-same-hash',
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [file],
                mediaFiles: [file],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const now = Math.floor(Date.now() / 1000)
    for (const id of [903, 904]) {
        await (pools as any).sendArticles(
            undefined,
            `summary-realtime-media-visibility-release-${id}`,
            [
                {
                    id,
                    a_id: `summary-realtime-media-visibility-release-${id}`,
                    platform: Platform.X,
                    username: 'media member',
                    u_id: 'media_member',
                    content: `summary visibility release text ${id}`,
                    url: `https://x.com/media_member/status/${id}`,
                    type: 'tweet',
                    created_at: now + id,
                    ref: null,
                    has_media: true,
                    media: [{ type: 'photo', url: 'https://example.com/realtime-release-same.jpg' }],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.props?.media?.map((file: any) => file.path)).toEqual(['/tmp/realtime-release-904.jpg'])
    expect(getSummaryCardQueueForTarget(pools, target.id)?.items.size).toBe(2)
})

test('summary-card realtime media can include basic text for Bilibili-style targets', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'bilibili'
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
                media_realtime: true,
                media_realtime_text: 'basic',
                flush_on_threshold: false,
                align_to_interval: true,
                flush_delay_seconds: 300,
            },
        } as any,
        'target-summary-card-realtime-media-basic',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content || '',
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [
                {
                    media_type: 'video_thumbnail',
                    path: `/tmp/realtime-basic-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-basic-${article.id}.jpg`,
                },
            ],
            mediaFiles: [
                {
                    media_type: 'video_thumbnail',
                    path: `/tmp/realtime-basic-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-basic-${article.id}.jpg`,
                },
            ],
        }),
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-realtime-media-basic',
            [
                {
                    id: 812,
                    a_id: 'summary-realtime-media-basic',
                    platform: Platform.X,
                    username: 'media basic member',
                    u_id: 'media_basic_member',
                    content: 'summary basic media text',
                    url: 'https://x.com/media_basic_member/status/812',
                    type: 'tweet',
                    created_at: now,
                    ref: null,
                    has_media: true,
                    media: [{ type: 'video_thumbnail', url: 'https://example.com/realtime-basic-812.jpg' }],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('summary basic media text')
    expect(target.sent[0]?.texts[0]).toContain('https://x.com/media_basic_member/status/812')
    expect(target.sent[0]?.props?.bypassMediaBatch).toBeTrue()
    expect(target.sent[0]?.props?.media?.[0]?.media_type).toBe('video_thumbnail')
    expect(getSummaryCardQueueForTarget(pools, target.id)?.items.size).toBe(1)
})

test('summary-card realtime media metadata text stays one line without body or URL', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'bilibili'
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
                media_realtime: true,
                media_realtime_text: 'metadata',
                flush_on_threshold: false,
                align_to_interval: true,
                flush_delay_seconds: 300,
            },
        } as any,
        'target-summary-card-realtime-media-metadata',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content || '',
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [
                {
                    media_type: 'photo',
                    path: `/tmp/realtime-metadata-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-metadata-${article.id}.jpg`,
                },
            ],
            mediaFiles: [
                {
                    media_type: 'photo',
                    path: `/tmp/realtime-metadata-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-metadata-${article.id}.jpg`,
                },
            ],
        }),
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const now = Math.floor(Date.now() / 1000)
    await (pools as any).sendArticles(
        undefined,
        'summary-realtime-media-metadata',
        [
            {
                id: 813,
                a_id: 'summary-realtime-media-metadata',
                platform: Platform.X,
                username: 'Bili Nick',
                u_id: 'bili_uid',
                content: 'full body should not be in realtime metadata text',
                url: 'https://x.com/bili_uid/status/813',
                type: 'tweet',
                created_at: now,
                ref: null,
                has_media: true,
                media: [{ type: 'photo', url: 'https://example.com/realtime-metadata-813.jpg' }],
                extra: null,
                u_avatar: null,
            },
        ],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    const text = target.sent[0]?.texts[0] || ''
    expect(target.sent).toHaveLength(1)
    expect(text).toContain('@bili_uid')
    expect(text).toContain('Bili Nick')
    expect(text).not.toContain('\n')
    expect(text).not.toContain('full body should not be in realtime metadata text')
    expect(text).not.toContain('https://x.com/bili_uid/status/813')
    expect(target.sent[0]?.props?.media?.[0]?.media_type).toBe('photo')
})

test('target media visibility text-collapses media after the second visible occurrence', async () => {
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
            media_visibility: {
                enabled: true,
                window_seconds: 86400,
                max_visible: 2,
                duplicate_behavior: 'text_only',
            },
        } as any,
        'target-media-visibility-text-only',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = {
        process: async (article: any) => {
            const file = {
                media_type: 'photo',
                path: `/tmp/high-realtime-${article.id}.jpg`,
                sourceArticleId: article.a_id,
                sourceUrl: 'https://example.com/high-realtime-same.jpg',
                content_hash: 'high-realtime-same-hash',
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [{ media_type: 'photo', path: `/tmp/high-realtime-card-${article.id}.jpg` }],
                originalMediaFiles: [file],
                mediaFiles: [file],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const now = Math.floor(Date.now() / 1000)
    for (const id of [930, 931, 932]) {
        await (pools as any).sendArticles(
            undefined,
            `media-visibility-text-only-${id}`,
            [
                {
                    id,
                    a_id: `media-visibility-text-only-${id}`,
                    platform: Platform.X,
                    username: 'high realtime member',
                    u_id: 'high_realtime_member',
                    content: `high realtime text ${id}`,
                    url: `https://x.com/high_realtime_member/status/${id}`,
                    type: 'tweet',
                    created_at: now + id,
                    ref: null,
                    has_media: true,
                    media: [{ type: 'photo', url: 'https://example.com/high-realtime-same.jpg' }],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text' } as any,
        )
    }

    expect(target.sent).toHaveLength(3)
    expect(target.sent[0]?.props?.media).toHaveLength(1)
    expect(target.sent[1]?.props?.media).toHaveLength(1)
    expect(target.sent[2]?.props?.media).toEqual([])
    expect(target.sent[2]?.props?.contentMedia).toEqual([])
    expect(target.sent[2]?.props?.cardMedia).toEqual([])
    expect(target.sent[2]?.texts[0]).toContain('high realtime text 932')
    expect(target.sent[2]?.texts[0]).toContain('重复媒体已文字缩略')
    expect(target.sent[2]?.texts[0]).toContain('24小时')
})

test('summary-card aligned windows wait for the configured five-minute delay', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
                flush_on_threshold: false,
                align_to_interval: true,
                flush_delay_seconds: 300,
            },
        } as any,
        'target-summary-card-delayed-window',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-delayed.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-delayed.png' }],
                }
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-delayed-window',
            [
                {
                    id: 820,
                    a_id: 'summary-delayed-window',
                    platform: Platform.X,
                    username: 'window member',
                    u_id: 'window_member',
                    content: 'delayed window text',
                    url: 'https://x.com/window_member/status/820',
                    type: 'tweet',
                    created_at: now,
                    ref: null,
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )

        const queue = getSummaryCardQueueForTarget(pools, target.id)
        queue.windowStart = now - 2100
        queue.windowEnd = now - 299
        await (pools as any).flushDueSummaryCardQueues()
        expect(target.sent).toHaveLength(0)

        queue.windowEnd = Math.floor(Date.now() / 1000) - 301
        await (pools as any).flushDueSummaryCardQueues()
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toMatch(/\d{4}～\d{4}/)
    expect(packedArticles[0]?.content).toMatch(/【聚合】1 条 \/ \d{4}～\d{4}/)
})

test('sendArticles starts a new summary-card window instead of appending to a due queue', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
                flush_on_threshold: false,
                flush_delay_seconds: 300,
            },
        } as any,
        'target-summary-card-due-window-rollover',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-rollover.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-rollover.png' }],
                }
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-due-window-old',
            [
                {
                    id: 850,
                    a_id: 'summary-due-window-old',
                    platform: Platform.X,
                    username: 'old window member',
                    u_id: 'old_window_member',
                    content: 'old due-window text',
                    url: 'https://x.com/old_window_member/status/850',
                    type: 'tweet',
                    created_at: now,
                    ref: null,
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )

        expect(target.sent).toHaveLength(0)
        const dueQueue = getSummaryCardQueueForTarget(pools, target.id)
        expect(dueQueue?.items.size).toBe(1)
        dueQueue.firstQueuedAt = now - 2101
        dueQueue.windowStart = now - 2101
        dueQueue.windowEnd = now - 301
        const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
        const window = windows.get(dueQueue.windowId)
        if (window) {
            window.idempotency_key = 'backdated-summary-window'
            window.window_start = dueQueue.windowStart
            window.window_end = dueQueue.windowEnd
        }

        await (pools as any).sendArticles(
            undefined,
            'summary-due-window-new',
            [
                {
                    id: 851,
                    a_id: 'summary-due-window-new',
                    platform: Platform.X,
                    username: 'new window member',
                    u_id: 'new_window_member',
                    content: 'new window text must wait',
                    url: 'https://x.com/new_window_member/status/851',
                    type: 'tweet',
                    created_at: now + 1,
                    ref: null,
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(packedArticles).toHaveLength(1)
    expect(packedArticles[0]?.content).toContain('old due-window text')
    expect(packedArticles[0]?.content).not.toContain('new window text must wait')
    const queue = getSummaryCardQueueForTarget(pools, target.id)
    expect(queue?.items.size).toBe(1)
    expect(Array.from(queue?.items.values() || [])[0]?.article?.a_id).toBe('summary-due-window-new')
})

test('sendArticles allocates a replacement summary-card window after a threshold-completed aligned window', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 2,
                interval_seconds: 7200,
                include_original_media: false,
                send_first_immediately: false,
                flush_on_threshold: true,
                align_to_hour: true,
                flush_delay_seconds: 300,
            },
        } as any,
        'target-summary-card-threshold-reopen',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-threshold-reopen.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-threshold-reopen.png' }],
                }
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-threshold-reopen-first',
            [0, 1].map((index) => ({
                id: 860 + index,
                a_id: `summary-threshold-reopen-${index}`,
                platform: Platform.X,
                username: `threshold member ${index}`,
                u_id: `threshold_member_${index}`,
                content: `threshold window text ${index}`,
                url: `https://x.com/threshold_member/status/${index}`,
                type: 'tweet',
                created_at: now + index,
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
            })),
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )

        expect(target.sent).toHaveLength(1)
        expect(packedArticles[0]?.content).toContain('threshold window text 0')
        expect(packedArticles[0]?.content).toContain('threshold window text 1')

        await (pools as any).sendArticles(
            undefined,
            'summary-threshold-reopen-next',
            [
                {
                    id: 862,
                    a_id: 'summary-threshold-reopen-next',
                    platform: Platform.X,
                    username: 'threshold member next',
                    u_id: 'threshold_member_next',
                    content: 'threshold next text must survive restart',
                    url: 'https://x.com/threshold_member/status/next',
                    type: 'tweet',
                    created_at: now + 2,
                    ref: null,
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(packedArticles).toHaveLength(1)
    const queue = getSummaryCardQueueForTarget(pools, target.id)
    expect(queue?.items.size).toBe(1)
    expect(Array.from(queue?.items.values() || [])[0]?.article?.a_id).toBe('summary-threshold-reopen-next')

    const windows = Array.from(((DB.AggregationWindow as any).__windows as Map<number, any>).values()).filter(
        (window: any) => window.target_id === target.id,
    )
    expect(windows.filter((window: any) => window.status === 'completed')).toHaveLength(1)
    const openWindows = windows.filter((window: any) => window.status === 'open')
    expect(openWindows).toHaveLength(1)
    expect(queue?.windowId).toBe(openWindows[0]?.id)
    expect(openWindows[0]?.idempotency_key).toContain(':reopen:')
})

test('idle-first summary-card items can fall back to native article send', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: true,
                send_first_native: true,
            },
        } as any,
        'target-summary-card-native-first',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content,
            textCollapseMode: 'article',
            cardMediaFiles: [],
            originalMediaFiles: [],
            mediaFiles: [],
        }),
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-native-first',
            [
                {
                    id: 830,
                    a_id: 'summary-native-first',
                    platform: Platform.X,
                    username: 'native member',
                    u_id: 'native_member',
                    content: 'native first text',
                    url: 'https://x.com/native_member/status/830',
                    type: 'tweet',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: null,
                    has_media: false,
                    media: [],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toBe('native first text')
    expect(target.sent[0]?.texts[0]).not.toContain('聚合')
    expect(getSummaryCardQueueForTarget(pools, target.id)).toBeUndefined()
})

test('summary-card media duplicate budget omits the third visible occurrence', async () => {
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
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
                media_duplicate_limit: 2,
            },
        } as any,
        'target-summary-card-media-duplicate-limit',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-dup.png' }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: '/tmp/summary-card-dup.png' }],
                }
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [],
                originalMediaFiles: [
                    {
                        media_type: 'photo',
                        path: `/tmp/duplicate-${article.id}.jpg`,
                        sourceArticleId: article.a_id,
                        sourceUrl: 'https://example.com/same-media.jpg',
                        content_hash: 'same-media-hash',
                    },
                ],
                mediaFiles: [
                    {
                        media_type: 'photo',
                        path: `/tmp/duplicate-${article.id}.jpg`,
                        sourceArticleId: article.a_id,
                        sourceUrl: 'https://example.com/same-media.jpg',
                        content_hash: 'same-media-hash',
                    },
                ],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: (files: Array<any>) =>
            files.map((file) => ({
                type: 'photo',
                url: `data:image/png;base64,${Buffer.from(file.sourceArticleId || file.path).toString('base64')}`,
                alt: file.sourceArticleId,
            })),
        cleanup: () => undefined,
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    const now = Math.floor(Date.now() / 1000)
    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-media-duplicate-limit',
            [0, 1, 2].map((index) => ({
                id: 840 + index,
                a_id: `summary-media-duplicate-${index}`,
                platform: Platform.X,
                username: `duplicate member ${index}`,
                u_id: `duplicate_member_${index}`,
                content: `duplicate text ${index}`,
                url: `https://x.com/duplicate_member/status/${index}`,
                type: 'tweet',
                created_at: now + index,
                ref: null,
                has_media: true,
                media: [{ type: 'photo', url: 'https://example.com/same-media.jpg' }],
                extra: null,
                u_avatar: null,
            })),
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )

        await (pools as any).flushAllSummaryCardQueues()
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(packedArticles[0]?.media).toHaveLength(1)
    const itemMediaCounts = packedArticles[0]?.extra?.data?.groups.flatMap((group: any) =>
        group.items.map((item: any) => item.media.length),
    )
    expect(itemMediaCounts).toEqual([1, 0, 0])
})
