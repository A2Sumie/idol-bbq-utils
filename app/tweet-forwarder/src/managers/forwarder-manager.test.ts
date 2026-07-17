import { afterEach, beforeEach, expect, test } from 'bun:test'
import EventEmitter from 'events'
import path from 'path'
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
import { articleKey, articleOutboundKey, routeKey, syntheticOutboundKey, targetRouteKey } from '@/services/outbound-message-service'
import { processorRegistry } from '@/middleware/processor'
import { ProcessorProvider } from '@/types/processor'

process.env.FONTS_DIR = fileURLToPath(new URL('../../../../assets/fonts', import.meta.url))
process.env.RENDER_REMOTE_ASSETS = '0'

const originalOutboundMessage = { ...DB.OutboundMessage }
const originalAggregationWindow = { ...DB.AggregationWindow }
const originalTargetHealth = { ...DB.TargetHealth }
const originalForwardBy = { ...DB.ForwardBy }
const originalMediaHash = { ...DB.MediaHash }
const originalContentFingerprint = { ...DB.ContentFingerprint }
const originalArticle = { ...DB.Article }

beforeEach(() => {
    const outboundRecords = new Map<string, any>()
    const targetHealth = new Map<string, any>()
    const aggregationWindows = new Map<number, any>()
    const aggregationItems = new Map<string, any>()
    const forwardByRecords = new Map<string, any>()
    const mediaHashRecords = new Map<string, any>()
    const contentFingerprintRecords = new Map<string, any>()
    let nextWindowId = 1
    let nextItemId = 1

    const forwardByKey = (refId: number, platform: string | number, botId: string, taskType: string) =>
        `${platform}:${refId}:${botId}:${taskType}`
    const mediaHashKey = (platform: string, hash: string) => `${platform}:${hash}`
    const contentFingerprintKey = (scope: string, targetId: string, fingerprint: string) =>
        `${scope}:${targetId}:${fingerprint}`

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
        const record = mediaHashRecords.get(key) || {
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
    ;(DB.ContentFingerprint as any).claim = async (options: any) => {
        const key = contentFingerprintKey(options.scope, options.target_id, options.fingerprint)
        const now = Math.floor(Number(options.now || Date.now() / 1000))
        const windowSeconds = Math.max(0, Math.floor(Number(options.windowSeconds || 0)))
        const existing = contentFingerprintRecords.get(key)
        if (existing) {
            const active =
                existing.status === 'sent' &&
                (windowSeconds <= 0 || Number(existing.updated_at || existing.created_at || 0) >= now - windowSeconds)
            if (active) {
                return { allowed: false, record: existing }
            }
            Object.assign(existing, {
                article_key: options.article_key ?? null,
                platform: options.platform === undefined || options.platform === null ? null : String(options.platform),
                article_id: options.article_id ?? null,
                status: 'sent',
                updated_at: now,
            })
            return { allowed: true, record: existing }
        }
        const record = {
            id: contentFingerprintRecords.size + 1,
            scope: options.scope,
            target_id: options.target_id,
            fingerprint: options.fingerprint,
            article_key: options.article_key ?? null,
            platform: options.platform === undefined || options.platform === null ? null : String(options.platform),
            article_id: options.article_id ?? null,
            status: 'sent',
            created_at: now,
            updated_at: now,
        }
        contentFingerprintRecords.set(key, record)
        return { allowed: true, record }
    }
    ;(DB.ContentFingerprint as any).release = async (options: any) => {
        contentFingerprintRecords.delete(
            contentFingerprintKey(options.scope, options.target_id, options.fingerprint),
        )
    }
    ;(DB.ContentFingerprint as any).__records = contentFingerprintRecords
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
    ;(DB.OutboundMessage as any).getByIdempotencyKey = async (idempotencyKey: string) =>
        outboundRecords.get(idempotencyKey) || null
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
        const now = Math.floor(Date.now() / 1000)
        Object.assign(record, {
            status: 'sent',
            provider_message_ids: providerResult ?? null,
            updated_at: now,
            finished_at: now,
            last_error: null,
        })
        return record
    }
    ;(DB.OutboundMessage as any).markPartial = async (
        idempotencyKey: string,
        providerResult: unknown,
        error: unknown,
    ) => {
        const record = outboundRecords.get(idempotencyKey)
        const now = Math.floor(Date.now() / 1000)
        Object.assign(record, {
            status: 'partial',
            segment_results: providerResult,
            last_error: error instanceof Error ? error.message : String(error),
            updated_at: now,
            finished_at: now,
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
    ;(DB.OutboundMessage as any).findLatestVisibleCompletion = async (options: any) => {
        const taskKinds = new Set(options.task_kinds || [])
        const visibleStatuses = new Set(['sent', 'partial', 'failed_after_partial'])
        return (
            Array.from(outboundRecords.values())
                .filter(
                    (record: any) =>
                        (!options.route_key || record.route_key === options.route_key) &&
                        record.target_id === options.target_id &&
                        (!options.article_key || record.article_key === options.article_key) &&
                        (!options.synthetic_key || record.synthetic_key === options.synthetic_key) &&
                        (taskKinds.size === 0 || taskKinds.has(record.task_kind)) &&
                        visibleStatuses.has(record.status),
                )
                .sort(
                    (a: any, b: any) =>
                        Number(b.finished_at || b.updated_at || b.created_at || 0) -
                        Number(a.finished_at || a.updated_at || a.created_at || 0),
                )[0] || null
        )
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
    Object.assign(DB.ContentFingerprint, originalContentFingerprint)
    Object.assign(DB.Article, originalArticle)
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
        mediaRealtimeDropSummaryPlatforms: [],
        flushOnThreshold: true,
        flushDelaySeconds: 0,
        windowAlignment: 'none',
        singleItemBehavior: 'native_if_uncovered',
        mediaDuplicateLimit: null,
        translatedCard: null,
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
                media_realtime_drop_summary_platforms: ['instagram', 'tt'],
                flush_on_threshold: false,
                flush_delay_seconds: 300,
                align_to_interval: true,
                single_item_behavior: 'drop',
                media_duplicate_limit: 2,
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                    processor_id: '22_7-social-ja-zh',
                },
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
        mediaRealtimeDropSummaryPlatforms: ['instagram', 'tt'],
        flushOnThreshold: false,
        flushDelaySeconds: 300,
        windowAlignment: 'interval',
        singleItemBehavior: 'drop',
        mediaDuplicateLimit: 2,
        translatedCard: {
            badgeLabel: '译文',
            processorId: '22_7-social-ja-zh',
        },
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
    expect(dispatched[0].task.data.websites).toEqual(['https://x.com/i/lists/1936785344072151389'])
    expect(dispatched[0].task.data.origin).toBeUndefined()
    expect(dispatched[0].task.data.paths).toBeUndefined()
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

test('ForwarderTaskScheduler queues configured article post-processors from spider results', async () => {
    const originalTaskAdd = DB.TaskQueue.add
    const queuedTasks: any[] = []
    ;(DB.TaskQueue as any).add = async (type: string, payload: any, executeAt: number, meta: any) => {
        queuedTasks.push({ type, payload, executeAt, meta })
        return { id: queuedTasks.length }
    }

    try {
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
                        cfg_crawler: {
                            post_processors: [
                                {
                                    processor_id: '22_7-event-time-extract',
                                    action: 'extract',
                                    schedule_user_agent: 'N2NJ-Stream-Bot/1.0',
                                    schedule_waf_bypass_header: 'env:LIVE_PLAYER_SCHEDULE_WAF_BYPASS_HEADER',
                                    min_confidence: 0.65,
                                },
                            ],
                        },
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
            taskId: 'spider-schedule-1',
            crawlerName: '22/7-cast-成员统一列表',
            result: [
                {
                    task_type: 'article',
                    url: 'https://x.com/i/lists/1936785344072151389',
                    data: [301, 302],
                },
            ],
        })
        await new Promise((resolve) => setTimeout(resolve, 0))

        expect(dispatched).toHaveLength(1)
        expect(queuedTasks).toHaveLength(2)
        expect(queuedTasks[0]).toMatchObject({
            type: DB.TaskQueue.TYPE.ArticleProcessorRun,
            payload: {
                processorId: '22_7-event-time-extract',
                action: 'extract',
                id: 301,
                scheduleUserAgent: 'N2NJ-Stream-Bot/1.0',
                scheduleWafBypassHeader: 'env:LIVE_PLAYER_SCHEDULE_WAF_BYPASS_HEADER',
                minConfidence: 0.65,
            },
            meta: {
                action_type: 'extract',
            },
        })
        expect(queuedTasks[0].meta.idempotency_key).toBeTruthy()
    } finally {
        ;(DB.TaskQueue as any).add = originalTaskAdd
    }
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

test('ForwarderPools resendArticle can limit delivery to selected targets with fresh force keys', async () => {
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
            forwarders: [],
            crawlers: [
                {
                    name: 'Instagram抓取 - 普通时段',
                    origin: 'https://www.instagram.com',
                },
            ] as any,
        },
        new EventEmitter(),
    )

    const targetA = { forwarder: { id: '七虹信标-群3' }, runtime_config: undefined } as any
    const targetB = { forwarder: { id: 'bilibili-转帖' }, runtime_config: { require_media: true } } as any
    const capturedSends: Array<{ taskId: string; targets: any[] }> = []
    ;(pools as any).resolveForwardingPaths = () => [
        {
            formatterConfig: {
                render_type: 'text',
            },
            targets: [targetA, targetB],
            source: 'graph',
            formatterName: '图文模板',
        },
    ]
    ;(pools as any).normalizeForwardingArticles = async (articles: any[]) => articles
    ;(pools as any).sendArticles = async (_log: any, taskId: string, _articles: any[], targets: any[]) => {
        capturedSends.push({ taskId, targets })
    }

    const article = {
        id: 823,
        a_id: '3916403096331382169',
        platform: Platform.Instagram,
    } as any
    const options = {
        targetIds: ['bilibili-转帖'],
    }
    await pools.resendArticle(article, 'Instagram抓取 - 普通时段', undefined, undefined, options)
    await pools.resendArticle(article, 'Instagram抓取 - 普通时段', undefined, undefined, options)

    expect(capturedSends).toHaveLength(2)
    expect(capturedSends[0].targets.map((item) => item.forwarder.id)).toEqual(['bilibili-转帖'])
    expect(capturedSends[0].targets[0].runtime_config).toEqual({ require_media: true })
    expect(capturedSends[0].taskId).toStartWith('manual-3916403096331382169-')
    expect(capturedSends[1].taskId).toStartWith('manual-3916403096331382169-')
    expect(capturedSends[0].taskId).not.toBe(capturedSends[1].taskId)
})

test('ForwarderPools sendImmediateXLinkArticle sends one merged-forward parse in text media card order', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
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

    const target = new RecordingForwarder({ block_until: '32h' } as any, 'qq-1')
    ;(pools as any).resolveTargetInstances = () => [{ forwarder: target, runtime_config: undefined }]

    const article = {
        id: 980,
        a_id: '2068452355688083860',
        platform: Platform.X,
        username: 'X user',
        u_id: 'x_user',
        content: 'original x text',
        translation: 'translated x text',
        translated_by: 'translator',
        url: 'https://x.com/x_user/status/2068452355688083860',
        type: 'tweet',
        created_at: 1_782_019_314,
        ref: null,
        has_media: true,
        media: [],
        extra: null,
        u_avatar: null,
    }
    const sourceVideo = { media_type: 'video', path: '/tmp/x-source.mp4' }
    const sourcePhoto = { media_type: 'photo', path: '/tmp/x-source.jpg' }
    const card = { media_type: 'photo', path: '/tmp/x-card.png' }
    const cleaned: any[] = []

    ;(DB.Article as any).getSingleArticle = async () => article
    ;(pools as any).renderService = {
        process: async () => ({
            text: 'card text',
            textCollapseMode: 'article',
            cardMediaFiles: [card],
            originalMediaFiles: [sourceVideo, sourcePhoto],
            mediaFiles: [sourceVideo, sourcePhoto, card],
        }),
        renderText: (input: any) => input.content || '',
        cleanup: (files: any) => cleaned.push(files),
    }

    const result = await (pools as any).sendImmediateXLinkArticle(article, {
        targetIds: ['qq-1'],
    })

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts).toEqual(['[X解析]\noriginal x text'])
    expect(target.sent[0]?.props?.media).toEqual([sourceVideo, sourcePhoto, card])
    expect(target.sent[0]?.props?.contentMedia).toEqual([sourceVideo, sourcePhoto])
    expect(target.sent[0]?.props?.cardMedia).toEqual([card])
    expect(target.sent[0]?.props?.runtime_config).toMatchObject({
        send_mode: 'merged_forward',
        merged_forward: {
            enabled: true,
        },
    })
    expect(target.sent[0]?.props?.forceSend).toBeTrue()
    expect(target.sent[0]?.props?.bypassMediaBatch).toBeTrue()
    expect(result.sends).toMatchObject([
        {
            target_id: 'qq-1',
            part: 'merged_forward',
            result: {
                status: 'sent',
            },
        },
    ])
    expect(cleaned).toEqual([[sourceVideo, sourcePhoto, card]])
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

test('sendArticles sends a text-only translation passthrough before the main send when enabled', async () => {
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

    const sends: Array<{ texts: string[]; props: any }> = []
    const target = {
        id: 'target-passthrough',
        NAME: 'recording',
        getEffectiveConfig: (runtimeConfig?: any) => runtimeConfig || {},
        check_blocked: async () => false,
        send: async (texts: string[] | string, props?: any) => {
            sends.push({ texts: Array.isArray(texts) ? texts : [texts], props })
            return { status: 'sent' }
        },
    }
    const article = {
        id: 213,
        a_id: 'passthrough-article',
        platform: 1,
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        content: '原文です',
        translation: '这是译文',
    }

    ;(pools as any).renderService = {
        process: async () => ({
            text: 'main card payload',
            mediaFiles: [],
            cardMediaFiles: [],
            originalMediaFiles: [],
        }),
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'passthrough-task',
        [article],
        [
            {
                forwarder: target,
                runtime_config: { translation_passthrough: true },
            },
        ],
        {
            render_type: 'text',
        } as any,
    )

    expect(sends).toHaveLength(2)
    expect(sends[0]?.texts).toHaveLength(1)
    expect(sends[0]?.texts[0]).toContain('这是译文')
    expect(sends[0]?.props?.media).toEqual([])
    expect(sends[0]?.props?.outboundKey).toContain('translation_passthrough')
    // Text-only passthrough must bypass require_media suppression targets.
    expect(sends[0]?.props?.runtime_config?.require_media).toBe(false)
    expect(sends[1]?.texts).toEqual(['main card payload'])
})

test('sendArticles skips translation passthrough when there is no translation', async () => {
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

    const sends: Array<{ texts: string[]; props: any }> = []
    const target = {
        id: 'target-passthrough-empty',
        NAME: 'recording',
        getEffectiveConfig: (runtimeConfig?: any) => runtimeConfig || {},
        check_blocked: async () => false,
        send: async (texts: string[] | string, props?: any) => {
            sends.push({ texts: Array.isArray(texts) ? texts : [texts], props })
            return { status: 'sent' }
        },
    }
    const article = {
        id: 214,
        a_id: 'passthrough-empty-article',
        platform: 1,
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        content: '原文です',
    }

    ;(pools as any).renderService = {
        process: async () => ({
            text: 'main card payload',
            mediaFiles: [],
            cardMediaFiles: [],
            originalMediaFiles: [],
        }),
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'passthrough-empty-task',
        [article],
        [
            {
                forwarder: target,
                runtime_config: { translation_passthrough: true },
            },
        ],
        {
            render_type: 'text',
        } as any,
    )

    expect(sends).toHaveLength(1)
    expect(sends[0]?.texts).toEqual(['main card payload'])
})

test('sendArticles fires translation passthrough before queueing to a summary-card target', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
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
            translation_passthrough: true,
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
                send_first_native: false,
            },
        } as any,
        'target-passthrough-summary',
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
        cleanup: () => undefined,
    }

    const article = {
        id: 217,
        a_id: 'passthrough-summary-article',
        platform: 1,
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        content: '原文です',
        translation: '这是译文',
    }

    await (pools as any).sendArticles(
        undefined,
        'passthrough-summary-task',
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

    // The passthrough fires even though the article itself is queued for the summary window
    // (no direct visible send for the article text).
    expect(target.sent.length).toBe(1)
    expect(target.sent[0]?.texts).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('这是译文')
    expect(target.sent[0]?.props?.outboundKey).toContain('translation_passthrough')
})

test('sendArticles keeps already-sent targets in the prefilter when a passthrough is still owed', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
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
            translation_passthrough: true,
        } as any,
        'target-passthrough-backfill',
    )

    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content,
            cardMediaFiles: [],
            originalMediaFiles: [],
            mediaFiles: [],
        }),
        cleanup: () => undefined,
    }

    const article = {
        id: 219,
        a_id: 'passthrough-backfill-article',
        platform: 1,
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        content: '原文です',
        translation: '这是译文',
    }
    // The article was already visibly sent to this target (ForwardBy exists) but the passthrough
    // feature landed afterwards, so no passthrough record exists.
    await DB.ForwardBy.save(article.id, article.platform, target.id, 'article')

    await (pools as any).sendArticles(
        undefined,
        'passthrough-backfill-task',
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

    // Only the passthrough goes out; the main article send stays claim-blocked.
    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('这是译文')
})

test('summary realtime media skips when the translation passthrough already went out', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
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
            translation_passthrough: true,
        } as any,
        'target-rt-dedup',
    )

    const article = {
        id: 221,
        a_id: 'rt-dedup-article',
        platform: 1,
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        content: '原文です',
        translation: '这是译文',
    }
    const passthroughKey = syntheticOutboundKey(target.id, 'translation_passthrough', articleKey(article as any))
    ;((DB.OutboundMessage as any).__records as Map<string, any>).set(passthroughKey, {
        id: 555,
        idempotency_key: passthroughKey,
        route_key: 'r',
        target_id: target.id,
        target_platform: target.NAME,
        task_kind: 'translation_passthrough',
        article_key: articleKey(article as any),
        synthetic_key: null,
        payload_hash: 'h',
        status: 'sent',
        provider_message_ids: null,
        segment_results: null,
        attempt_count: 1,
        last_error: null,
        created_at: Math.floor(Date.now() / 1000) - 10,
        updated_at: Math.floor(Date.now() / 1000) - 10,
        finished_at: Math.floor(Date.now() / 1000) - 10,
    })

    const result = await (pools as any).sendSummaryCardRealtimeMedia(
        undefined,
        article,
        target,
        undefined,
        'route:key',
        {
            mediaRealtime: true,
            mediaRealtimeText: 'metadata',
        },
        {
            text: 'x',
            cardMediaFiles: [],
            originalMediaFiles: [{ media_type: 'photo', path: '/tmp/rt-dedup.jpg' }],
            mediaFiles: [],
        },
    )

    expect(result.visibleMediaSent).toBe(true)
    expect(result.skippedDuplicate).toBe(true)
    expect(target.sent).toHaveLength(0)
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
    expect(outboundRecord?.provider_message_ids?.reason).toBe(
        'Cross-platform short video duplicate matched 2:ig-short-1',
    )
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
                single_item_behavior: 'summary_card',
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
    expect(target.sent[0]?.texts[0]).toContain('更新合并')
    expect(target.sent[0]?.texts[0]).toContain('1. member1 x发推')
    expect(target.sent[0]?.texts[0]).toContain('2. member2 x发推')
    expect(target.sent[0]?.texts[0]).not.toMatch(/\d{2}:\d{2}-\d{2}:\d{2}/)
    expect(packedArticles[0]?.content).toContain('【更新合并】2 条')
    expect(packedArticles[0]?.content).not.toMatch(/\d{2}:\d{2}-\d{2}:\d{2}/)
    expect(packedArticles[0]?.content).toContain('summary content 1')
    expect(packedArticles[0]?.content).toContain('summary content 2')
    expect(packedArticles[0]?.extra?.extra_type).toBe('message_pack_meta')
    expect(packedArticles[0]?.extra?.data?.groups?.[0]?.avatars?.[0]).toEqual({
        url: 'https://example.com/avatar-1.jpg',
        name: 'member1',
        id: 'member1',
    })
    expect(packedArticles[0]?.extra?.data?.groups?.[1]?.avatars?.[0]).toEqual({
        url: 'https://example.com/avatar-2.jpg',
        name: 'member2',
        id: 'member2',
    })
    expect(packedArticles[0]?.media).toEqual([
        {
            type: 'photo',
            url: `data:image/png;base64,${Buffer.from('summary-1').toString('base64')}`,
            alt: 'summary-1',
        },
        {
            type: 'photo',
            url: `data:image/png;base64,${Buffer.from('summary-2').toString('base64')}`,
            alt: 'summary-2',
        },
    ])
    expect(packedArticles[1]?.content).toContain('【更新合并】1 条')
    expect(packedArticles[1]?.content).toContain('summary content 3')
    expect(packedArticles[1]?.extra?.data?.groups?.[0]?.items?.[0]?.text).toContain('summary content 3')
    expect(packedArticles[1]?.extra?.data?.groups).toHaveLength(1)
    expect(packedArticles[1]?.extra?.data?.groups?.[0]?.avatars?.[0]?.url).toBe('https://example.com/avatar-3.jpg')
    expect(target.sent[1]?.props?.media).toEqual([{ media_type: 'photo', path: '/tmp/summary-card.png' }])
})

test('summary-card send text lists every item in the top digest', () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'

        protected async realSend() {
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

    const target = new RecordingForwarder({ block_until: '32h' } as any, 'target-summary-card-send-text-digest')
    const now = Math.floor(Date.now() / 1000)
    const items = Array.from({ length: 7 }, (_, index) => {
        const id = index + 1
        const isInstagramStory = id === 2
        const isRetweet = id === 3
        return {
            article: {
                id,
                a_id: `summary-send-text-${id}`,
                platform: isInstagramStory ? Platform.Instagram : Platform.X,
                username: isInstagramStory ? 'rino' : isRetweet ? 'iko' : `member${id}`,
                u_id: isInstagramStory ? 'rino' : isRetweet ? 'iko' : `member${id}`,
                content: `summary content ${id}`,
                url: isInstagramStory
                    ? `https://www.instagram.com/stories/rino/${id}`
                    : `https://x.com/member/status/${id}`,
                type: isInstagramStory ? 'story' : isRetweet ? 'retweet' : 'tweet',
                created_at: now + index,
                ref: isRetweet
                    ? {
                          id: 227,
                          a_id: 'staff-source',
                          platform: Platform.X,
                          username: '227staff',
                          u_id: '227staff',
                          content: 'staff update',
                          url: 'https://x.com/227staff/status/227',
                          type: 'tweet',
                          created_at: now,
                          ref: null,
                      }
                    : null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
            },
            queuedAt: now,
            cardSourceMediaFiles: [],
            originalMediaFiles: [],
            digestTags: [],
        }
    })
    const text = (pools as any).buildSummaryCardSendText(
        {
            routeKey: 'summary-send-text-digest',
            target,
            runtime_config: undefined,
            config: {
                windowAlignment: 'none',
            },
            items: new Map(items.map((item) => [item.article.id, item])),
            firstQueuedAt: now,
            lastQueuedAt: now + 6,
        },
        items,
        '聚合 fallback',
    )

    expect(text).toContain('1. member1 x发推')
    expect(text).toContain('2. rino ig故事')
    expect(text).toContain('3. iko x转推227staff')
    expect(text).toContain('6. member6 x发推')
    expect(text).toContain('7. member7 x发推')
    expect(text).not.toContain('发故事')
    expect(text).not.toContain('另有 1 条更新已合并')
    expect(text).not.toContain('另有1条')
})

test('summary-card send text keeps Bilibili card body digest', () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'bilibili'

        protected async realSend() {
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

    const target = new RecordingForwarder({ block_until: '32h' } as any, 'target-bilibili-card-digest-text')
    const now = Math.floor(Date.now() / 1000)
    const items = [
        {
            article: {
                id: 1,
                a_id: 'summary-bili-digest-text-1',
                platform: Platform.X,
                username: 'member1',
                u_id: 'member1',
                content: 'summary content should keep a digest body',
                url: 'https://x.com/member1/status/1',
                type: 'tweet',
                created_at: now,
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null,
            },
            queuedAt: now,
            cardSourceMediaFiles: [],
            originalMediaFiles: [],
            digestTags: [],
        },
    ]

    const text = (pools as any).buildSummaryCardSendText(
        {
            routeKey: 'summary-bili-digest-text',
            target,
            runtime_config: undefined,
            config: {
                windowAlignment: 'none',
            },
            items: new Map(items.map((item) => [item.article.id, item])),
            firstQueuedAt: now,
            lastQueuedAt: now,
        },
        items,
        '聚合 fallback',
    )

    expect(text).toContain('更新合并')
    expect(text).toContain('1. member1 x发推')
})

test('summary-card send text consolidates repeated and mixed retweet digest items', () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'

        protected async realSend() {
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

    const target = new RecordingForwarder({ block_until: '32h' } as any, 'target-summary-card-compact-digest')
    const now = Math.floor(Date.now() / 1000)
    const article = (id: number, u_id: string, type: string, ref?: any, platform = Platform.X) => ({
        id,
        a_id: `summary-compact-${id}`,
        platform,
        username: u_id,
        u_id,
        content: `summary content ${id}`,
        url:
            platform === Platform.Instagram
                ? `https://www.instagram.com/stories/${u_id}/${id}`
                : `https://x.com/${u_id}/status/${id}`,
        type,
        created_at: now + id,
        ref: ref || null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    })
    const refArticle = (u_id: string) => ({
        id: 9000,
        a_id: `${u_id}-source`,
        platform: Platform.X,
        username: u_id,
        u_id,
        content: 'source update',
        url: `https://x.com/${u_id}/status/source`,
        type: 'tweet',
        created_at: now,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    })
    const queueItems = (articles: any[]) =>
        articles.map((item) => ({
            article: item,
            queuedAt: now,
            cardSourceMediaFiles: [],
            originalMediaFiles: [],
            digestTags: [],
        }))
    const buildText = (items: ReturnType<typeof queueItems>, routeKey: string) =>
        (pools as any).buildSummaryCardSendText(
            {
                routeKey,
                target,
                runtime_config: undefined,
                config: {
                    windowAlignment: 'none',
                },
                items: new Map(items.map((item) => [item.article.id, item])),
                firstQueuedAt: now,
                lastQueuedAt: now + items.length,
            },
            items,
            '聚合 fallback',
        )

    const needygirl = refArticle('needygirl_anime')
    const selfReply = refArticle('_nishiurasora')
    const firstText = buildText(
        queueItems([
            article(1, 'nao_aikawa227', 'story', null, Platform.Instagram),
            article(2, '_nishiurasora', 'reply', selfReply),
            article(3, 'satsuki_shiina', 'tweet'),
            article(4, '_fujimasakura', 'tweet'),
            article(5, '_fujimasakura', 'tweet'),
            article(6, 'satsuki_shiina', 'tweet'),
            article(7, 'satsuki_shiina', 'tweet'),
            article(8, 'satsuki_shiina', 'retweet', needygirl),
        ]),
        'summary-compact-first',
    )
    const secondText = buildText(
        queueItems([
            article(11, 'satsuki_shiina', 'quoted', needygirl),
            article(12, 'satsuki_shiina', 'retweet', needygirl),
            article(13, 'satsuki_shiina', 'retweet', needygirl),
            article(14, 'satsuki_shiina', 'retweet', needygirl),
            article(15, 'satsuki_shiina', 'quoted', needygirl),
        ]),
        'summary-compact-second',
    )

    expect(firstText).toContain('1. nao_aikawa227 ig故事')
    expect(firstText).toContain('2. _nishiurasora x回复_nishiurasora')
    expect(firstText).toContain('3,6~7. satsuki_shiina x发推')
    expect(firstText).toContain('4~5. _fujimasakura x发推')
    expect(firstText).toContain('8. satsuki_shiina x转推needygirl_anime')
    expect(firstText).not.toContain('6. satsuki_shiina x发推')
    expect(secondText).toContain('1~5. satsuki_shiina x转推/引用 needygirl_anime')
    expect(secondText).not.toContain('2. satsuki_shiina x转推needygirl_anime')
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
                single_item_behavior: 'summary_card',
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
                single_item_behavior: 'summary_card',
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
                single_item_behavior: 'summary_card',
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

test('flushSummaryCardQueue cancels stale durable windows during runtime without sending', async () => {
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
        'target-summary-card-runtime-stale-window',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
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
        'summary-runtime-stale-window',
        [
            {
                id: 719,
                a_id: 'summary-runtime-stale-window',
                platform: Platform.X,
                username: 'stale summary',
                u_id: 'stale_summary',
                content: 'runtime stale summary-card should not send',
                url: 'https://x.com/stale_summary/status/719',
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
    const staleEnd = Math.floor(Date.now() / 1000) - 3 * 3600 - 1
    queue.windowEnd = staleEnd
    const window = windows.get(queue.windowId)
    window.window_end = staleEnd

    await (pools as any).flushSummaryCardQueue(queueKey, 'interval')

    expect(target.sent).toHaveLength(0)
    expect((pools as any).summaryCardQueues.has(queueKey)).toBeFalse()
    expect(windows.get(queue.windowId)?.status).toBe('cancelled')
    expect(windows.get(queue.windowId)?.payload_hash).toBe('stale-window')
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
                single_item_behavior: 'summary_card',
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
                single_item_behavior: 'summary_card',
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

    const sentOutbound = Array.from(outboundRecords.values()).find((record: any) => record.task_kind === 'summary_card')
    expect(target.calls).toHaveLength(2)
    expect(released).toEqual([718])
    expect(Array.from(claimed)).toEqual([718])
    expect((pools as any).summaryCardQueues.has(queueKey)).toBeFalse()
    expect(windows.get(queue.windowId)?.status).toBe('completed')
    expect(sentOutbound?.status).toBe('sent')
})

test('single-item summary-card windows fall back to compact native when uncovered', async () => {
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
        'target-summary-card-single-native-default',
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
                    path: '/tmp/single-native-default.jpg',
                    sourceArticleId: article.a_id,
                },
            ],
            mediaFiles: [
                {
                    media_type: 'photo',
                    path: '/tmp/single-native-default.jpg',
                    sourceArticleId: article.a_id,
                },
            ],
        }),
        renderText: (article: any) => `${article.u_id}: ${article.content || ''}`,
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'summary-single-native-default',
        [
            {
                id: 722,
                a_id: 'summary-single-native-default',
                platform: Platform.X,
                username: 'single native',
                u_id: 'single_native',
                content: 'single uncovered text',
                url: 'https://x.com/single_native/status/722',
                type: 'tweet',
                created_at: Math.floor(Date.now() / 1000),
                ref: null,
                has_media: true,
                media: [{ type: 'photo', url: 'https://example.com/single-native.jpg' }],
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
    const outbounds = Array.from(((DB.OutboundMessage as any).__records as Map<string, any>).values())
    const fallbackOutbound = outbounds.find((record: any) => record.task_kind === 'summary_single_native')
    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('single uncovered text')
    expect(target.sent[0]?.texts[0]).not.toContain('聚合')
    expect(target.sent[0]?.texts[0]).not.toContain('更新合并 1 条')
    expect(target.sent[0]?.props?.media).toEqual([
        { media_type: 'photo', path: '/tmp/single-native-default.jpg', sourceArticleId: 'summary-single-native-default' },
    ])
    expect(fallbackOutbound?.status).toBe('sent')
    expect(windows.get(queue.windowId)?.status).toBe('completed')
    expect((pools as any).summaryCardQueues.has(queueKey)).toBeFalse()
})

test('single-item summary-card windows suppress when realtime media already carried text context', async () => {
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
                media_realtime: true,
                media_realtime_text: 'basic',
            },
        } as any,
        'target-summary-card-single-covered-default',
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
                    path: '/tmp/single-covered-default.jpg',
                    sourceArticleId: article.a_id,
                    sourceUrl: 'https://example.com/single-covered.jpg',
                },
            ],
            mediaFiles: [
                {
                    media_type: 'photo',
                    path: '/tmp/single-covered-default.jpg',
                    sourceArticleId: article.a_id,
                    sourceUrl: 'https://example.com/single-covered.jpg',
                },
            ],
        }),
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    await (pools as any).sendArticles(
        undefined,
        'summary-single-covered-default',
        [
            {
                id: 723,
                a_id: 'summary-single-covered-default',
                platform: Platform.X,
                username: 'single covered',
                u_id: 'single_covered',
                content: 'single covered text',
                url: 'https://x.com/single_covered/status/723',
                type: 'tweet',
                created_at: Math.floor(Date.now() / 1000),
                ref: null,
                has_media: true,
                media: [{ type: 'photo', url: 'https://example.com/single-covered.jpg' }],
                extra: null,
                u_avatar: null,
            },
        ],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('single covered text')
    const queueKey = Array.from((pools as any).summaryCardQueues.keys())[0]
    const queue = (pools as any).summaryCardQueues.get(queueKey)
    await (pools as any).flushSummaryCardQueue(queueKey, 'interval')

    const windows = (DB.AggregationWindow as any).__windows as Map<number, any>
    const outbounds = Array.from(((DB.OutboundMessage as any).__records as Map<string, any>).values())
    const fallbackOutbound = outbounds.find((record: any) => record.task_kind === 'summary_single_native')
    expect(target.sent).toHaveLength(1)
    expect(fallbackOutbound?.status).toBe('skipped')
    expect(fallbackOutbound?.provider_message_ids?.reason).toBe('single_item_covered')
    expect(windows.get(queue.windowId)?.status).toBe('cancelled')
    expect((pools as any).summaryCardQueues.has(queueKey)).toBeFalse()
})

test('summary-card outbound key suppresses the same article set across reopened windows', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        calls: Array<{ text: string; props: any }> = []

        public override async send(text: string, props?: any): Promise<any> {
            this.calls.push({ text, props })
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

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 1800,
                include_original_media: false,
                send_first_immediately: false,
                single_item_behavior: 'summary_card',
            },
        } as any,
        'target-summary-card-reopened-window-stable-key',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = {
        process: async (article: any) => {
            if (article.id < 0) {
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: `/tmp/${article.a_id}.png` }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: `/tmp/${article.a_id}.png` }],
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

    const now = Math.floor(Date.now() / 1000)
    const article = {
        id: 721,
        a_id: 'summary-reopened-window-same-set',
        platform: Platform.X,
        username: 'window member',
        u_id: 'window_member',
        content: 'same summary-card set should not resend after reopen',
        url: 'https://x.com/window_member/status/721',
        type: 'tweet',
        created_at: now,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    } as any

    for (const windowShift of [0, 1]) {
        await (pools as any).sendArticles(
            undefined,
            `summary-reopened-window-${windowShift}`,
            [article],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
        const queueKey = Array.from((pools as any).summaryCardQueues.keys())[0]
        const queue = (pools as any).summaryCardQueues.get(queueKey)
        queue.windowId += windowShift
        await (pools as any).flushSummaryCardQueue(queueKey, 'interval')
    }

    const outboundRecords = Array.from(((DB.OutboundMessage as any).__records as Map<string, any>).values()).filter(
        (record: any) => record.task_kind === 'summary_card' && record.target_id === target.id,
    )
    expect(target.calls).toHaveLength(1)
    expect(outboundRecords).toHaveLength(1)
    expect(outboundRecords[0]?.status).toBe('sent')
    expect(outboundRecords[0]?.segment_results?.diagnostic).toBe('suppressed_payload_drift')
})

test('summary-card queues are shared across routes for the same target', async () => {
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
    expect(queues).toHaveLength(1)
    expect(queues[0]?.items.size).toBe(2)
    expect(Array.from(queues[0]?.items.values()).map((item: any) => item.article.a_id)).toEqual([
        'summary-route-isolation-0',
        'summary-route-isolation-1',
    ])
    expect(queues[0]?.routeKey).toContain('summary-card')
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

test('restoreSummaryCardQueues keeps windows when only translated companion config changed', async () => {
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
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                },
            },
        } as any,
        'target-summary-card-translated-config-restore',
    )
    ;(pools as any).forward_to.set(target.id, target)

    const now = Math.floor(Date.now() / 1000)
    const article = {
        id: 743,
        a_id: 'summary-translated-config-restore',
        platform: Platform.X,
        username: 'translated restore',
        u_id: 'translated_restore',
        content: 'translated-card config change should keep old window',
        url: 'https://x.com/translated_restore/status/743',
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
        idempotency_key: 'translated-config-summary-window',
        route_key: `route-translated-config:target:${target.id}`,
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
    items.set('1:translated-config-summary-item', {
        id: 1,
        window_id: 1,
        article_key: 'translated-config-summary-item',
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
                translatedCard: null,
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

    expect(windows.get(1)?.status).toBe('open')
    const queue = getSummaryCardQueueForTarget(pools, target.id)
    expect(queue?.items.size).toBe(1)
    expect(queue?.config.translatedCard?.badgeLabel).toBe('译文')
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

test('sendArticles keeps forwarded reference text after retired idle-first summary-card batches wait for window', async () => {
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
                send_first_immediately: true,
                send_first_native: false,
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
        expect(target.sent).toHaveLength(0)
        expect(packedArticles).toHaveLength(0)
        expect(getSummaryCardQueueForTarget(pools, target.id)?.items.size).toBe(1)

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

    expect(target.sent).toHaveLength(1)
    expect(packedArticles).toHaveLength(1)
    const summaryText = (packedArticles[0]?.extra?.data?.groups || [])
        .flatMap((group: any) => group.items || [])
        .map((item: any) => item.text || '')
        .join('\n')
    expect(summaryText).not.toContain('（略）')
    expect(summaryText).toContain('first body should not repeat')
    expect(renderTextCalls.some((call) => call.article.id === 302 && call.collapsedArticleIds)).toBeFalse()
})

test('sendArticles renders a translated companion summary card with stable forwarded references', async () => {
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
                send_first_immediately: false,
                single_item_behavior: 'summary_card',
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                },
            },
        } as any,
        'target-summary-card-translated-companion',
    )

    const forwardedIds = new Set<number>()
    ;(pools as any).claimArticleChain = async (article: any) => {
        forwardedIds.add(article.id)
        return true
    }
    ;(pools as any).releaseArticleChain = async () => undefined

    const packedArticles: Array<any> = []
    const renderProcessCalls: Array<{ article: any; config: any }> = []
    const renderTextCalls: Array<{ article: any; collapsedArticleIds?: Set<string | number> }> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            if (article.id < 0) {
                renderProcessCalls.push({ article, config })
                packedArticles.push(article)
                const suffix = config?.card_features?.includes('translated-corner-badge') ? 'translated' : 'original'
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: `/tmp/summary-card-${suffix}.png` }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: `/tmp/summary-card-${suffix}.png` }],
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
            const body = article.content || ''
            if (article.ref && config?.collapsedArticleIds?.has(article.ref.id)) {
                return `${body}\n------------\n@first_member 2320⁹（略）`
            }
            if (article.ref) {
                return `${body}\n------------\n${article.ref.content}`
            }
            return body
        },
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const firstCreatedAt = Math.floor(Date.now() / 1000)
    const firstArticle = {
        id: 301,
        a_id: 'translated-parent',
        platform: Platform.X,
        username: 'first member',
        u_id: 'first_member',
        content: 'first body should not repeat',
        translation: '首条译文不应重复',
        translated_by: 'LLM',
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
        a_id: 'translated-reply',
        platform: Platform.X,
        username: 'reply member',
        u_id: 'reply_member',
        content: 'reply body',
        translation: '回复译文',
        translated_by: 'LLM',
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
            'summary-translated-parent',
            [firstArticle],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
        await (pools as any).sendArticles(
            undefined,
            'summary-translated-reply',
            [replyArticle],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )

        backdateSummaryCardQueues(pools as any, 1800)
        await (pools as any).flushDueSummaryCardQueues()
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.props?.cardMedia).toEqual([
        { media_type: 'photo', path: '/tmp/summary-card-original.png' },
        { media_type: 'photo', path: '/tmp/summary-card-translated.png' },
    ])
    expect(target.sent[0]?.texts[0]).toContain('1. first_member x发推')
    expect(target.sent[0]?.texts[0]).toContain('2. reply_member x回复first_member')
    expect(renderProcessCalls[0]?.config?.card_features).toBeUndefined()
    expect(renderProcessCalls[1]?.config?.card_features).toEqual(['translated-corner-badge'])
    expect(packedArticles).toHaveLength(2)
    expect(packedArticles[1]?.extra?.data?.translated_badge_label).toBe('译文')

    const originalReplyText = packedArticles[0]?.extra?.data?.groups?.[0]?.items?.[1]?.text || ''
    const translatedReplyText = packedArticles[1]?.extra?.data?.groups?.[0]?.items?.[1]?.text || ''
    expect(originalReplyText).toContain('reply body')
    expect(originalReplyText).not.toContain('回复译文')
    expect(translatedReplyText).toContain('回复译文')
    expect(translatedReplyText).not.toContain('（略）')
    expect(translatedReplyText).not.toContain('first body should not repeat')
    expect(translatedReplyText).toContain('首条译文不应重复')
    expect(
        renderTextCalls.filter((call) => call.article.id === 302).every((call) => !call.collapsedArticleIds),
    ).toBeTrue()
})

test('sendArticles suppresses stored translations for no-translation targets', async () => {
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

    const translatedTarget = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '161717573',
        } as any,
        'target-translation-allowed',
    )
    const suppressedTarget = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '742435777',
            suppress_translations: true,
        } as any,
        'target-translation-suppressed',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const renderCalls: Array<{ article: any; config: any }> = []
    const cleanupPaths: string[] = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            renderCalls.push({ article, config })
            const translated = Boolean(article.translation)
            const media = {
                media_type: 'photo',
                path: translated ? '/tmp/translated-card.png' : '/tmp/original-card.png',
            }
            return {
                text: translated ? 'translated text should stay out of 742' : 'original text only',
                textCollapseMode: 'article',
                cardMediaFiles: [media],
                originalMediaFiles: [],
                mediaFiles: [media],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: (files: Array<{ path: string }>) => cleanupPaths.push(...files.map((file) => file.path)),
    }

    const article = {
        id: 771,
        a_id: 'suppress-translation-article',
        platform: Platform.X,
        username: 'member',
        u_id: 'member',
        content: 'original text only',
        translation: 'translated text should stay out of 742',
        translated_by: 'LLM',
        url: 'https://x.com/member/status/771',
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }

    await (pools as any).sendArticles(
        undefined,
        'suppress-translation-task',
        [article],
        [
            { forwarder: translatedTarget, runtime_config: undefined },
            { forwarder: suppressedTarget, runtime_config: undefined },
        ],
        { render_type: 'text-card' } as any,
    )

    expect(translatedTarget.sent[0]?.texts[0]).toBe('translated text should stay out of 742')
    expect(translatedTarget.sent[0]?.props?.media).toEqual([{ media_type: 'photo', path: '/tmp/translated-card.png' }])
    expect(suppressedTarget.sent[0]?.texts[0]).toBe('original text only')
    expect(suppressedTarget.sent[0]?.props?.media).toEqual([{ media_type: 'photo', path: '/tmp/original-card.png' }])
    expect(suppressedTarget.sent[0]?.props?.article?.translation).toBeNull()
    expect(renderCalls).toHaveLength(2)
    expect(renderCalls[1]?.config?.taskId).toContain('target-translation-suppressed-no-translation')
    expect(cleanupPaths).toContain('/tmp/original-card.png')
})

test('sendArticles fills missing translations before rendering translated summary companion card', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
            return
        }
    }

    const processCalls: string[] = []
    const originalCreateProcessor = (processorRegistry as any).create
    const originalArticleUpdate = (DB.Article as any).update
    const articleUpdates: Array<{ id: number; platform: Platform; patch: any }> = []
    ;(processorRegistry as any).create = async () => ({
        NAME: 'fake-22/7-translator',
        process: async (text: string) => {
            processCalls.push(text)
            return `译:${text}`
        },
        drop: async () => undefined,
    })
    ;(DB.Article as any).update = async (id: number, platform: Platform, patch: any) => {
        articleUpdates.push({ id, platform, patch })
        return { id, ...patch }
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
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.Deepseek,
                    api_key: 'test-key',
                    cfg_processor: {
                        action: 'translate',
                    },
                },
            ],
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
                send_first_immediately: false,
                single_item_behavior: 'summary_card',
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                    processor_id: '22_7-social-ja-zh',
                },
            },
        } as any,
        'target-summary-card-translated-on-demand',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                const suffix = config?.card_features?.includes('translated-corner-badge') ? 'translated' : 'original'
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: `/tmp/on-demand-${suffix}.png` }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: `/tmp/on-demand-${suffix}.png` }],
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

    const article = {
        id: 915,
        a_id: 'translated-on-demand',
        platform: Platform.X,
        username: '22/7',
        u_id: '227_staff',
        content: 'この後19:00〜 バースデーSHOWROOM配信スタート',
        translation: null,
        translated_by: null,
        url: 'https://x.com/227_staff/status/915',
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-translated-on-demand',
            [article],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )

        backdateSummaryCardQueues(pools as any, 1800)
        await (pools as any).flushDueSummaryCardQueues()
    } finally {
        ;(processorRegistry as any).create = originalCreateProcessor
        ;(DB.Article as any).update = originalArticleUpdate
    }

    expect(processCalls).toEqual(['この後19:00〜 バースデーSHOWROOM配信スタート'])
    expect(articleUpdates).toEqual([
        {
            id: 915,
            platform: Platform.X,
            patch: {
                translation: '译:この後19:00〜 バースデーSHOWROOM配信スタート',
                translated_by: 'fake-22/7-translator',
            },
        },
    ])
    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.props?.cardMedia).toEqual([
        { media_type: 'photo', path: '/tmp/on-demand-original.png' },
        { media_type: 'photo', path: '/tmp/on-demand-translated.png' },
    ])
    const originalText = packedArticles[0]?.extra?.data?.groups?.[0]?.items?.[0]?.text || ''
    const translatedText = packedArticles[1]?.extra?.data?.groups?.[0]?.items?.[0]?.text || ''
    expect(packedArticles[1]?.content).toContain('译:この後19:00〜 バースデーSHOWROOM配信スタート')
    expect(originalText).toContain('この後19:00〜 バースデーSHOWROOM配信スタート')
    expect(originalText).not.toContain('译:')
    expect(translatedText).toContain('译:この後19:00〜 バースデーSHOWROOM配信スタート')
})

test('summary-card translation handles multiple referenced articles without database ids', async () => {
    const processCalls: string[] = []
    const articleUpdates: Array<{ id: number; patch: any }> = []
    const originalCreateProcessor = (processorRegistry as any).create
    const originalArticleUpdate = (DB.Article as any).update
    ;(processorRegistry as any).create = async () => ({
        NAME: 'fake-ref-translator',
        process: async (text: string) => {
            processCalls.push(text)
            const current =
                text.match(/当前待译】\n([\s\S]*?)(?:\n\n【第|\n\n【当前待译字段】|$)/)?.[1]?.trim() || text.trim()
            return `译:${current}`
        },
        drop: async () => undefined,
    })
    ;(DB.Article as any).update = async (id: number, _platform: Platform, patch: any) => {
        articleUpdates.push({ id, patch })
        return { id, ...patch }
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
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.Deepseek,
                    api_key: 'test-key',
                    cfg_processor: {
                        action: 'translate',
                    },
                },
            ],
        },
        new EventEmitter(),
    )

    const firstRef = {
        a_id: 'ref-without-id-a',
        platform: Platform.X,
        username: '227 staff',
        u_id: '227_staff',
        content: 'base A body',
        translation: null,
        translated_by: null,
        url: 'https://x.com/227_staff/status/ref-a',
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }
    const secondRef = {
        ...firstRef,
        a_id: 'ref-without-id-b',
        content: 'base B body',
        url: 'https://x.com/227_staff/status/ref-b',
    }
    const articles = [
        {
            id: 801,
            a_id: 'retweet-a',
            platform: Platform.X,
            username: 'member a',
            u_id: 'member_a',
            content: 'retweet A body',
            translation: null,
            translated_by: null,
            url: 'https://x.com/member_a/status/801',
            type: 'retweet',
            created_at: Math.floor(Date.now() / 1000) + 1,
            ref: firstRef,
            has_media: false,
            media: [],
            extra: null,
            u_avatar: null,
        },
        {
            id: 802,
            a_id: 'retweet-b',
            platform: Platform.X,
            username: 'member b',
            u_id: 'member_b',
            content: 'retweet B body',
            translation: null,
            translated_by: null,
            url: 'https://x.com/member_b/status/802',
            type: 'retweet',
            created_at: Math.floor(Date.now() / 1000) + 2,
            ref: secondRef,
            has_media: false,
            media: [],
            extra: null,
            u_avatar: null,
        },
    ] as any

    try {
        await (pools as any).prepareArticleChainTranslations('22_7-social-ja-zh', articles, 'missing-ref-id-test')
    } finally {
        ;(processorRegistry as any).create = originalCreateProcessor
        ;(DB.Article as any).update = originalArticleUpdate
    }

    expect(processCalls.length).toBe(4)
    expect(articles[0].ref.translation).toBe('译:base A body')
    expect(articles[1].ref.translation).toBe('译:base B body')
    expect(articleUpdates.map((item) => item.id).sort()).toEqual([801, 802])
})

test('summary-card translation requires complete coverage for three-layer forwarded chains', async () => {
    const processCalls: string[] = []
    const articleUpdates: Array<{ id: number; patch: any }> = []
    const originalCreateProcessor = (processorRegistry as any).create
    const originalArticleUpdate = (DB.Article as any).update
    ;(processorRegistry as any).create = async () => ({
        NAME: 'fake-three-layer-translator',
        process: async (text: string) => {
            processCalls.push(text)
            const current =
                text.match(/当前待译】\n([\s\S]*?)(?:\n\n【第|\n\n【当前待译字段】|$)/)?.[1]?.trim() || text.trim()
            return `译:${current}`
        },
        drop: async () => undefined,
    })
    ;(DB.Article as any).update = async (id: number, _platform: Platform, patch: any) => {
        articleUpdates.push({ id, patch })
        return { id, ...patch }
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
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.Deepseek,
                    api_key: 'test-key',
                    cfg_processor: {
                        action: 'translate',
                    },
                },
            ],
        },
        new EventEmitter(),
    )

    const rootArticle = {
        id: 930,
        a_id: 'three-layer-root',
        platform: Platform.X,
        username: 'root member',
        u_id: 'root_member',
        content: 'root source body',
        translation: '译:root source body',
        translated_by: 'existing',
        url: 'https://x.com/root_member/status/930',
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }
    const middleArticle = {
        id: 931,
        a_id: 'three-layer-middle',
        platform: Platform.X,
        username: 'middle member',
        u_id: 'middle_member',
        content: 'middle quote body',
        translation: null,
        translated_by: null,
        url: 'https://x.com/middle_member/status/931',
        type: 'quoted',
        created_at: rootArticle.created_at + 60,
        ref: rootArticle,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }
    const outerArticle = {
        id: 932,
        a_id: 'three-layer-outer',
        platform: Platform.X,
        username: 'outer member',
        u_id: 'outer_member',
        content: 'outer reply body',
        translation: null,
        translated_by: null,
        url: 'https://x.com/outer_member/status/932',
        type: 'reply',
        created_at: middleArticle.created_at + 60,
        ref: middleArticle,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    } as any

    expect((pools as any).hasArticleChainTranslatedContent([outerArticle])).toBeFalse()

    try {
        await (pools as any).prepareArticleChainTranslations(
            '22_7-social-ja-zh',
            [outerArticle],
            'three-layer-chain-test',
        )
    } finally {
        ;(processorRegistry as any).create = originalCreateProcessor
        ;(DB.Article as any).update = originalArticleUpdate
    }

    expect(processCalls).toHaveLength(2)
    expect(processCalls[0]).toContain('【第2条/第2条发生/当前待译】')
    expect(processCalls[1]).toContain('【第3条/最后发生/当前待译】')
    expect(middleArticle.translation).toBe('译:middle quote body')
    expect(outerArticle.translation).toBe('译:outer reply body')
    expect((pools as any).hasArticleChainTranslatedContent([outerArticle])).toBeTrue()
    expect(articleUpdates.map((item) => item.id).sort()).toEqual([931, 932])

    const translatedVariant = (pools as any).buildArticleTextVariant(outerArticle, 'translated')
    expect(translatedVariant.content).toBe('译:outer reply body')
    expect(translatedVariant.ref.content).toBe('译:middle quote body')
    expect(translatedVariant.ref.ref.content).toBe('译:root source body')
})

test('translated card article requires visible translated body text', () => {
    const pools = new ForwarderPools(
        {
            forward_targets: [],
            cfg_forward_target: {} as any,
            connections: {} as any,
            formatters: [],
            cfg_forwarder: {
                render_type: 'text-card',
            } as any,
            forwarders: [],
            crawlers: [],
        },
        new EventEmitter(),
    )

    const mediaOnlyTranslatedArticle = {
        id: 933,
        a_id: 'media-only-translated',
        platform: Platform.X,
        username: 'member',
        u_id: 'member',
        content: '',
        translation: null,
        translated_by: null,
        url: 'https://x.com/member/status/933',
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: true,
        media: [
            {
                type: 'photo',
                url: 'https://example.com/media-only-translated.jpg',
                alt: 'original alt',
                translation: 'translated alt',
            },
        ],
        extra: null,
        u_avatar: null,
    } as any
    expect((pools as any).hasArticleChainTranslatedContent([mediaOnlyTranslatedArticle])).toBeTrue()
    expect((pools as any).buildTranslatedCardArticle(mediaOnlyTranslatedArticle, '译文')).toBeNull()

    const bodyTranslatedArticle = {
        ...mediaOnlyTranslatedArticle,
        a_id: 'body-translated',
        content: 'original body',
        translation: 'translated body',
        translated_by: 'LLM',
    }
    const translatedCardArticle = (pools as any).buildTranslatedCardArticle(bodyTranslatedArticle, '译文')
    expect(translatedCardArticle.content).toBe('translated body')
    expect(translatedCardArticle.translation).toBeNull()
    expect(translatedCardArticle.extra?.data?.translated_badge_label).toBe('译文')
})

test('translated website card article drops raw html blocks that still contain original text', () => {
    const pools = new ForwarderPools(
        {
            forward_targets: [],
            cfg_forward_target: {} as any,
            connections: {} as any,
            formatters: [],
            cfg_forwarder: {
                render_type: 'text-card',
            } as any,
            forwarders: [],
            crawlers: [],
        },
        new EventEmitter(),
    )

    const websiteArticle = {
        id: 934,
        a_id: 'website-translated-raw-html',
        platform: Platform.Website,
        username: '桧山依子',
        u_id: 'hiyama-yoriko',
        content: '【今日のブログ】\n\n原文本文です',
        translation: '【今天的博客】\n\n这是译文正文',
        translated_by: 'LLM',
        url: 'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/934',
        type: 'article',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: true,
        media: [{ type: 'photo', url: 'https://example.com/blog-photo.jpg' }],
        extra: {
            content: '今日のブログ',
            translation: '今天的博客',
            translated_by: 'LLM',
            extra_type: 'website_meta',
            data: {
                site: '22/7',
                feed: 'fc-blog',
                title: '今日のブログ',
                raw_html: '<p>原文本文です</p><img src="/blog-photo.jpg" alt="photo">',
            },
        },
        u_avatar: null,
    } as any

    const translatedCardArticle = (pools as any).buildTranslatedCardArticle(websiteArticle, '译文')
    expect(translatedCardArticle.content).toBe('【今天的博客】\n\n这是译文正文')
    expect(translatedCardArticle.extra?.content).toBe('今天的博客')
    expect(translatedCardArticle.extra?.data?.raw_html).toBeUndefined()
    expect(translatedCardArticle.extra?.data?.title).toBe('今日のブログ')
    expect(translatedCardArticle.extra?.data?.translated_badge_label).toBe('译文')
    expect(websiteArticle.extra.data.raw_html).toContain('原文本文です')
})

test('summary-card translation reprocesses unchanged Japanese translations before rendering', async () => {
    const processCalls: string[] = []
    const articleUpdates: Array<{ id: number; patch: any }> = []
    const originalCreateProcessor = (processorRegistry as any).create
    const originalArticleUpdate = (DB.Article as any).update
    ;(processorRegistry as any).create = async () => ({
        NAME: 'fake-unchanged-ja-translator',
        process: async (text: string) => {
            processCalls.push(text)
            return '真的要早点见面呢'
        },
        drop: async () => undefined,
    })
    ;(DB.Article as any).update = async (id: number, _platform: Platform, patch: any) => {
        articleUpdates.push({ id, patch })
        return { id, ...patch }
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
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.Deepseek,
                    api_key: 'test-key',
                    cfg_processor: {
                        action: 'translate',
                    },
                },
            ],
        },
        new EventEmitter(),
    )

    const article = {
        id: 940,
        a_id: 'unchanged-ja-translation',
        platform: Platform.X,
        username: 'member',
        u_id: 'member',
        content: 'ほまに……はよう……',
        translation: 'ほまに……はよう……',
        translated_by: 'old',
        url: 'https://x.com/member/status/940',
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    } as any

    expect((pools as any).hasArticleChainTranslatedContent([article])).toBeFalse()

    try {
        await (pools as any).prepareArticleChainTranslations(
            '22_7-social-ja-zh',
            [article],
            'unchanged-ja-translation-test',
        )
    } finally {
        ;(processorRegistry as any).create = originalCreateProcessor
        ;(DB.Article as any).update = originalArticleUpdate
    }

    expect(processCalls).toHaveLength(1)
    expect(article.translation).toBe('真的要早点见面呢')
    expect((pools as any).hasArticleChainTranslatedContent([article])).toBeTrue()
    expect(articleUpdates).toEqual([
        {
            id: 940,
            patch: {
                translation: '真的要早点见面呢',
                translated_by: 'fake-unchanged-ja-translator',
            },
        },
    ])

    const translatedVariant = (pools as any).buildArticleTextVariant(article, 'translated')
    expect(translatedVariant.content).toBe('真的要早点见面呢')
})

test('sendArticles prompts summary-card translation with chronological chain order', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
            return
        }
    }

    const processCalls: string[] = []
    const articleUpdates: Array<{ id: number; patch: any }> = []
    const originalCreateProcessor = (processorRegistry as any).create
    const originalArticleUpdate = (DB.Article as any).update
    ;(processorRegistry as any).create = async () => ({
        NAME: 'fake-ordered-translator',
        process: async (text: string) => {
            processCalls.push(text)
            if (text.includes('【第2条/最后发生/当前待译】')) {
                return '译:後の返信'
            }
            if (text.includes('【第1条/最先发生/当前待译】')) {
                return '译:先の本文'
            }
            return '译:unknown'
        },
        drop: async () => undefined,
    })
    ;(DB.Article as any).update = async (id: number, _platform: Platform, patch: any) => {
        articleUpdates.push({ id, patch })
        return { id, ...patch }
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
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.Deepseek,
                    api_key: 'test-key',
                    cfg_processor: {
                        action: 'translate',
                    },
                },
            ],
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
                send_first_immediately: false,
                single_item_behavior: 'summary_card',
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                    processor_id: '22_7-social-ja-zh',
                },
            },
        } as any,
        'target-summary-card-ordered-translation',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            if (article.id < 0) {
                const suffix = config?.card_features?.includes('translated-corner-badge') ? 'translated' : 'original'
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: `/tmp/ordered-${suffix}.png` }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: `/tmp/ordered-${suffix}.png` }],
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

    const now = Math.floor(Date.now() / 1000)
    const parentArticle = {
        id: 920,
        a_id: 'ordered-parent',
        platform: Platform.X,
        username: 'first member',
        u_id: 'first_member',
        content: '先の本文 #ナナニジ',
        translation: null,
        translated_by: null,
        url: 'https://x.com/first_member/status/920',
        type: 'tweet',
        created_at: now,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }
    const replyArticle = {
        id: 921,
        a_id: 'ordered-reply',
        platform: Platform.X,
        username: 'reply member',
        u_id: 'reply_member',
        content: '後の返信 #出演情報',
        translation: null,
        translated_by: null,
        url: 'https://x.com/reply_member/status/921',
        type: 'reply',
        created_at: now + 60,
        ref: parentArticle,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-ordered-translation',
            [replyArticle],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )

        await (pools as any).flushAllSummaryCardQueues()
    } finally {
        ;(processorRegistry as any).create = originalCreateProcessor
        ;(DB.Article as any).update = originalArticleUpdate
    }

    expect(processCalls).toHaveLength(2)
    expect(processCalls[0]).toContain('保留所有 hashtag 原文')
    expect(processCalls[0]).toContain('以下按发生顺序排列')
    expect(processCalls[0]).toContain('【第1条/最先发生/当前待译】')
    expect(processCalls[0]).toContain('先の本文 #ナナニジ')
    expect(processCalls[0]).toContain('【第2条/最后发生/上下文】')
    expect(processCalls[0]).toContain('後の返信 #出演情報')
    expect(processCalls[1]).toContain('保留所有 hashtag 原文')
    expect(processCalls[1]).toContain('以下按发生顺序排列')
    expect(processCalls[1]).toContain('【第1条/最先发生/上下文】')
    expect(processCalls[1]).toContain('先の本文 #ナナニジ')
    expect(processCalls[1]).toContain('【第2条/最后发生/当前待译】')
    expect(processCalls[1]).toContain('後の返信 #出演情報')
    expect(articleUpdates.map((update) => ({ id: update.id, translation: update.patch.translation }))).toEqual([
        { id: 920, translation: '译:先の本文 #ナナニジ' },
        { id: 921, translation: '译:後の返信 #出演情報' },
    ])
    expect(target.sent).toHaveLength(1)
})

test('sendArticles suppresses empty translated summary companion when processor is unavailable', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
            return
        }
    }

    const originalCreateProcessor = (processorRegistry as any).create
    let createCalls = 0
    ;(processorRegistry as any).create = async () => {
        createCalls += 1
        throw new Error('Processor API key env var not set: DEEPSEEK_API_KEY')
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
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.Deepseek,
                    api_key: 'env:DEEPSEEK_API_KEY',
                    cfg_processor: {
                        action: 'translate',
                    },
                },
            ],
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
                send_first_immediately: false,
                single_item_behavior: 'summary_card',
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                    processor_id: '22_7-social-ja-zh',
                },
            },
        } as any,
        'target-summary-card-translated-missing-key',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            if (article.id < 0) {
                packedArticles.push(article)
                const suffix = config?.card_features?.includes('translated-corner-badge') ? 'translated' : 'original'
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: `/tmp/missing-key-${suffix}.png` }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: `/tmp/missing-key-${suffix}.png` }],
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

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-translated-missing-key',
            [
                {
                    id: 916,
                    a_id: 'translated-missing-key',
                    platform: Platform.X,
                    username: '22/7',
                    u_id: '227_staff',
                    content: 'この後20:00〜 配信スタート',
                    translation: null,
                    translated_by: null,
                    url: 'https://x.com/227_staff/status/916',
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

        backdateSummaryCardQueues(pools as any, 1800)
        await (pools as any).flushDueSummaryCardQueues()
    } finally {
        ;(processorRegistry as any).create = originalCreateProcessor
    }

    expect(createCalls).toBe(1)
    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.props?.cardMedia).toEqual([{ media_type: 'photo', path: '/tmp/missing-key-original.png' }])
    expect(packedArticles).toHaveLength(1)
    expect(packedArticles[0]?.extra?.data?.translated_badge_label).toBeUndefined()
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
                single_item_behavior: 'summary_card',
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

    expect(target.sent).toHaveLength(1)
    expect(packedArticles).toHaveLength(1)
    const stormGroups = packedArticles[0]?.extra?.data?.groups || []
    expect(stormGroups).toHaveLength(1)
    expect(stormGroups[0]?.kind).toBe('storm')
    expect(stormGroups[0]?.label).toBe('#ナナニジ')
    expect(stormGroups[0]?.items).toHaveLength(3)
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
                single_item_behavior: 'summary_card',
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

        backdateSummaryCardQueues(pools as any, 1800)
        await (pools as any).flushDueSummaryCardQueues()
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('更新合并 1 条 /')
    expect(target.sent[0]?.texts[0]).toContain('1. media_member x发推')
    expect(target.sent[0]?.texts[0]).not.toContain('图集: 1 张')
    expect(target.sent[0]?.texts[0]).not.toBe(packedArticles[0]?.content?.split('\n')[0])
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

test('summary-card realtime media suppresses same article when rendered media identity drifts', async () => {
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
                media_realtime_text: 'none',
                flush_on_threshold: false,
            },
        } as any,
        'target-summary-card-realtime-media-stable-key',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    let renderCount = 0
    ;(pools as any).renderService = {
        process: async (article: any) => {
            renderCount += 1
            const file = {
                media_type: 'photo',
                path: `/tmp/realtime-drift-${renderCount}.jpg`,
                sourceArticleId: article.a_id,
                sourceUrl: `https://example.com/realtime-drift-${renderCount}.jpg`,
                content_hash: `realtime-drift-hash-${renderCount}`,
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

    const article = {
        id: 910,
        a_id: 'summary-realtime-media-drift-same-article',
        platform: Platform.YouTube,
        username: 'video member',
        u_id: 'video_member',
        content: 'summary realtime media same article should not duplicate',
        url: 'https://youtube.com/watch?v=summary-realtime-media-drift-same-article',
        type: 'video',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: true,
        media: [{ type: 'photo', url: 'https://example.com/realtime-drift-source.jpg' }],
        extra: null,
        u_avatar: null,
    } as any

    for (let index = 0; index < 2; index += 1) {
        await (pools as any).sendArticles(
            undefined,
            `summary-realtime-media-drift-${index}`,
            [article],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    }

    const outboundRecords = Array.from(((DB.OutboundMessage as any).__records as Map<string, any>).values()).filter(
        (record: any) => record.task_kind === 'summary_realtime_media' && record.target_id === target.id,
    )
    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.props?.media?.[0]?.path).toBe('/tmp/realtime-drift-1.jpg')
    expect(outboundRecords).toHaveLength(1)
    expect(outboundRecords[0]?.status).toBe('sent')
    expect(outboundRecords[0]?.article_key).toBe(`${Platform.YouTube}:summary-realtime-media-drift-same-article`)
    expect(outboundRecords[0]?.synthetic_key).not.toContain('realtime-drift-hash')
    expect(outboundRecords[0]?.segment_results?.diagnostic).toBe('suppressed_payload_drift')
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

test('summary-card realtime media can include basic text for Bilibili video targets', async () => {
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
                single_item_behavior: 'summary_card',
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
                {
                    media_type: 'video',
                    path: `/tmp/realtime-basic-${article.id}.mp4`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-basic-${article.id}.mp4`,
                },
            ],
            mediaFiles: [
                {
                    media_type: 'video_thumbnail',
                    path: `/tmp/realtime-basic-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-basic-${article.id}.jpg`,
                },
                {
                    media_type: 'video',
                    path: `/tmp/realtime-basic-${article.id}.mp4`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-basic-${article.id}.mp4`,
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
                    media: [
                        { type: 'video_thumbnail', url: 'https://example.com/realtime-basic-812.jpg' },
                        { type: 'video', url: 'https://example.com/realtime-basic-812.mp4' },
                    ],
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
    expect(target.sent[0]?.props?.media?.map((file: any) => file.media_type)).toEqual(['video_thumbnail', 'video'])
    expect(getSummaryCardQueueForTarget(pools, target.id)?.items.size).toBe(1)
})

test('summary-card realtime media skips pure video thumbnails for non-Bilibili targets', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'qq'
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
                media_realtime_text: 'none',
                flush_on_threshold: false,
            },
        } as any,
        'target-summary-card-realtime-thumbnail-qq',
    )

    ;(pools as any).renderService = {
        process: async (article: any) => {
            const files = [
                {
                    media_type: 'video_thumbnail',
                    path: `/tmp/realtime-thumbnail-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-thumbnail-${article.id}.jpg`,
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
    await (pools as any).sendArticles(
        undefined,
        'summary-realtime-thumbnail-qq',
        [
            {
                id: 816,
                a_id: 'summary-realtime-thumbnail-qq',
                platform: Platform.X,
                username: 'video member',
                u_id: 'video_member',
                content: 'video thumbnail should wait for summary',
                url: 'https://x.com/video_member/status/816',
                type: 'tweet',
                created_at: now,
                ref: null,
                has_media: true,
                media: [{ type: 'video_thumbnail', url: 'https://example.com/realtime-thumbnail-816.jpg' }],
                extra: null,
                u_avatar: null,
            },
        ],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    expect(target.sent).toHaveLength(0)
    expect(getSummaryCardQueueForTarget(pools, target.id)?.items.size).toBe(1)
})

test('summary-card realtime media skips pure video thumbnails for Bilibili targets', async () => {
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
            },
        } as any,
        'target-summary-card-realtime-thumbnail-bili',
    )

    ;(pools as any).renderService = {
        process: async (article: any) => {
            const files = [
                {
                    media_type: 'video_thumbnail',
                    path: `/tmp/realtime-bili-thumbnail-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-bili-thumbnail-${article.id}.jpg`,
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
    await (pools as any).sendArticles(
        undefined,
        'summary-realtime-thumbnail-bili',
        [
            {
                id: 817,
                a_id: 'summary-realtime-thumbnail-bili',
                platform: Platform.YouTube,
                username: 'YT Channel',
                u_id: 'yt_channel',
                content: 'youtube video should wait for aggregation when only cover exists',
                url: 'https://www.youtube.com/watch?v=817',
                type: 'video',
                created_at: now,
                ref: null,
                has_media: true,
                media: [{ type: 'video_thumbnail', url: 'https://example.com/realtime-bili-thumbnail-817.jpg' }],
                extra: null,
                u_avatar: null,
            },
        ],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    expect(target.sent).toHaveLength(0)
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
                single_item_behavior: 'summary_card',
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

test('summary-card realtime media appends rendered card after Bilibili photo dynamics only', async () => {
    class RecordingForwarder extends Forwarder {
        NAME: string
        sent: Array<{ texts: string[]; props: any }> = []

        constructor(name: string, id: string) {
            super(
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
                    },
                } as any,
                id,
            )
            this.NAME = name
        }

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

    const biliTarget = new RecordingForwarder('bilibili', 'target-summary-card-realtime-photo-bili')
    const qqTarget = new RecordingForwarder('qq', 'target-summary-card-realtime-photo-qq')

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content || '',
            textCollapseMode: 'article',
            cardMediaFiles: [
                {
                    media_type: 'photo',
                    path: `/tmp/realtime-photo-card-${article.id}.png`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `card:${article.a_id}`,
                },
            ],
            originalMediaFiles: [
                {
                    media_type: 'photo',
                    path: `/tmp/realtime-photo-original-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-photo-original-${article.id}.jpg`,
                },
            ],
            mediaFiles: [
                {
                    media_type: 'photo',
                    path: `/tmp/realtime-photo-card-${article.id}.png`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `card:${article.a_id}`,
                },
                {
                    media_type: 'photo',
                    path: `/tmp/realtime-photo-original-${article.id}.jpg`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `https://example.com/realtime-photo-original-${article.id}.jpg`,
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
        'summary-realtime-photo-tail-card',
        [
            {
                id: 814,
                a_id: 'summary-realtime-photo-tail-card',
                platform: Platform.X,
                username: 'Photo Nick',
                u_id: 'photo_uid',
                content: 'photo body should be card tail on Bilibili',
                url: 'https://x.com/photo_uid/status/814',
                type: 'tweet',
                created_at: now,
                ref: null,
                has_media: true,
                media: [{ type: 'photo', url: 'https://example.com/realtime-photo-original-814.jpg' }],
                extra: null,
                u_avatar: null,
            },
        ],
        [
            { forwarder: biliTarget, runtime_config: undefined },
            { forwarder: qqTarget, runtime_config: undefined },
        ],
        { render_type: 'text-card' } as any,
    )

    expect(biliTarget.sent).toHaveLength(1)
    expect(qqTarget.sent).toHaveLength(1)
    expect(biliTarget.sent[0]?.props?.media?.map((file: any) => path.basename(file.path))).toEqual([
        'realtime-photo-original-814.jpg',
        'realtime-photo-card-814.png',
    ])
    expect(biliTarget.sent[0]?.texts[0]).toContain('@photo_uid')
    expect(biliTarget.sent[0]?.texts[0]).toContain('Photo Nick')
    expect(qqTarget.sent[0]?.props?.media?.map((file: any) => path.basename(file.path))).toEqual([
        'realtime-photo-original-814.jpg',
    ])
    expect(qqTarget.sent[0]?.texts[0]).toContain('@photo_uid')
})

test('summary-card realtime media appends translated content card for Bilibili photo dynamics', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'bilibili'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
            return
        }
    }

    const processCalls: string[] = []
    const articleUpdates: Array<{ id: number; platform: Platform; patch: any }> = []
    const originalCreateProcessor = (processorRegistry as any).create
    const originalArticleUpdate = (DB.Article as any).update
    ;(processorRegistry as any).create = async () => ({
        NAME: 'fake-realtime-translator',
        process: async (text: string) => {
            processCalls.push(text)
            return `译:${text}`
        },
        drop: async () => undefined,
    })
    ;(DB.Article as any).update = async (id: number, platform: Platform, patch: any) => {
        articleUpdates.push({ id, platform, patch })
        return { id, ...patch }
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
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.OpenAI,
                    api_key: 'test-key',
                    cfg_processor: { action: 'translate' },
                },
            ],
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
                send_first_native: false,
                media_realtime: true,
                media_realtime_text: 'metadata',
                flush_on_threshold: false,
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                    processor_id: '22_7-social-ja-zh',
                },
            },
        } as any,
        'target-summary-card-realtime-photo-bili-translated',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const sourceMedia = {
        media_type: 'photo',
        path: '/tmp/realtime-translated-original-815.jpg',
        sourceArticleId: 'summary-realtime-photo-tail-card-translated',
        sourceUrl: 'https://example.com/realtime-translated-original-815.jpg',
    }
    const originalCard = {
        media_type: 'photo',
        path: '/tmp/realtime-translated-original-card-815.png',
        sourceArticleId: 'summary-realtime-photo-tail-card-translated',
        sourceUrl: 'card:summary-realtime-photo-tail-card-translated:original',
    }
    const translatedCard = {
        media_type: 'photo',
        path: '/tmp/realtime-translated-card-815.png',
        sourceArticleId: 'summary-realtime-photo-tail-card-translated',
        sourceUrl: 'card:summary-realtime-photo-tail-card-translated:translated',
    }
    const renderProcessCalls: Array<{ article: any; config: any }> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            renderProcessCalls.push({ article, config })
            if (String(config?.taskId || '').startsWith('summary-realtime-card-')) {
                expect(article.content).toBe('photo body should be translated in Bilibili tail card')
                expect(article.translation).toBe('译:photo body should be translated in Bilibili tail card')
                expect(config?.preloadedMediaFiles).toEqual([sourceMedia])
                expect(config?.card_features || []).not.toContain('translated-card-pattern')
                expect(config?.card_features || []).not.toContain('translated-corner-badge')
                return {
                    text: article.content || '',
                    textCollapseMode: 'article',
                    cardMediaFiles: [translatedCard],
                    originalMediaFiles: config?.preloadedMediaFiles || [],
                    mediaFiles: [...(config?.preloadedMediaFiles || []), translatedCard],
                }
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [originalCard],
                originalMediaFiles: [sourceMedia],
                mediaFiles: [sourceMedia, originalCard],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-realtime-photo-tail-card-translated',
            [
                {
                    id: 815,
                    a_id: 'summary-realtime-photo-tail-card-translated',
                    platform: Platform.X,
                    username: 'Photo Nick',
                    u_id: 'photo_uid',
                    content: 'photo body should be translated in Bilibili tail card',
                    translation: null,
                    translated_by: null,
                    url: 'https://x.com/photo_uid/status/815',
                    type: 'tweet',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: null,
                    has_media: true,
                    media: [{ type: 'photo', url: 'https://example.com/realtime-translated-original-815.jpg' }],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    } finally {
        ;(processorRegistry as any).create = originalCreateProcessor
        ;(DB.Article as any).update = originalArticleUpdate
    }

    expect(processCalls).toEqual(['photo body should be translated in Bilibili tail card'])
    expect(articleUpdates).toEqual([
        {
            id: 815,
            platform: Platform.X,
            patch: {
                translation: '译:photo body should be translated in Bilibili tail card',
                translated_by: 'fake-realtime-translator',
            },
        },
    ])
    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.props?.media?.map((file: any) => path.basename(file.path))).toEqual([
        'realtime-translated-original-815.jpg',
        'realtime-translated-card-815.png',
    ])
    expect(renderProcessCalls).toHaveLength(2)
})

test('summary-card realtime Bilibili tail card survives duplicate source media visibility', async () => {
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
                send_first_native: false,
                media_realtime: true,
                media_realtime_text: 'metadata',
                flush_on_threshold: false,
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                },
            },
            media_visibility: {
                enabled: true,
                window_seconds: 432000,
                max_visible: 1,
                duplicate_behavior: 'skip',
            },
        } as any,
        'target-summary-card-realtime-photo-bili-duplicate-tail-card',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const sourceMediaFor = (id: number) => ({
        media_type: 'photo',
        path: `/tmp/realtime-duplicate-source-${id}.jpg`,
        sourceArticleId: `summary-realtime-duplicate-tail-card-${id}`,
        sourceUrl: 'https://example.com/realtime-duplicate-source.jpg',
        content_hash: 'realtime-duplicate-source-hash',
    })
    const renderProcessCalls: Array<{ article: any; config: any }> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            renderProcessCalls.push({ article, config })
            const id = Number(article.id)
            if (String(config?.taskId || '').startsWith('summary-realtime-card-')) {
                const translatedCard = {
                    media_type: 'photo',
                    path: `/tmp/realtime-duplicate-translated-card-${id}.png`,
                    sourceArticleId: article.a_id,
                    sourceUrl: `card:${article.a_id}:translated`,
                }
                return {
                    text: article.content || '',
                    textCollapseMode: 'article',
                    cardMediaFiles: [translatedCard],
                    originalMediaFiles: config?.preloadedMediaFiles || [],
                    mediaFiles: [...(config?.preloadedMediaFiles || []), translatedCard],
                }
            }
            const sourceMedia = sourceMediaFor(id)
            const originalCard = {
                media_type: 'photo',
                path: `/tmp/realtime-duplicate-original-card-${id}.png`,
                sourceArticleId: article.a_id,
                sourceUrl: `card:${article.a_id}:original`,
            }
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [originalCard],
                originalMediaFiles: [sourceMedia],
                mediaFiles: [sourceMedia, originalCard],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const now = Math.floor(Date.now() / 1000)
    for (const id of [817, 818]) {
        await (pools as any).sendArticles(
            undefined,
            `summary-realtime-duplicate-tail-card-${id}`,
            [
                {
                    id,
                    a_id: `summary-realtime-duplicate-tail-card-${id}`,
                    platform: Platform.X,
                    username: 'Photo Nick',
                    u_id: 'photo_uid',
                    content: `photo body ${id}`,
                    translation: `译:photo body ${id}`,
                    translated_by: 'LLM',
                    url: `https://x.com/photo_uid/status/${id}`,
                    type: 'tweet',
                    created_at: now + id,
                    ref: null,
                    has_media: true,
                    media: [{ type: 'photo', url: 'https://example.com/realtime-duplicate-source.jpg' }],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    }

    expect(target.sent).toHaveLength(2)
    expect(target.sent[0]?.props?.media?.map((file: any) => path.basename(file.path))).toEqual([
        'realtime-duplicate-source-817.jpg',
        'realtime-duplicate-translated-card-817.png',
    ])
    expect(target.sent[1]?.props?.media?.map((file: any) => path.basename(file.path))).toEqual([
        'realtime-duplicate-translated-card-818.png',
    ])
    expect(
        renderProcessCalls.filter((call) => String(call.config?.taskId || '').startsWith('summary-realtime-card-')),
    ).toHaveLength(2)
})

test('summary-card realtime Bilibili photo tail card keeps original-card fallback when translation is unavailable', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'bilibili'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
            return
        }
    }

    const originalCreateProcessor = (processorRegistry as any).create
    ;(processorRegistry as any).create = async () => {
        throw new Error('translator unavailable')
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
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.OpenAI,
                    api_key: 'test-key',
                    cfg_processor: { action: 'translate' },
                },
            ],
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
                send_first_native: false,
                media_realtime: true,
                media_realtime_text: 'metadata',
                flush_on_threshold: false,
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                    processor_id: '22_7-social-ja-zh',
                },
            },
        } as any,
        'target-summary-card-realtime-photo-bili-translation-missing',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const sourceMedia = {
        media_type: 'photo',
        path: '/tmp/realtime-translation-missing-original-815.jpg',
        sourceArticleId: 'summary-realtime-photo-tail-card-translation-missing',
        sourceUrl: 'https://example.com/realtime-translation-missing-original-815.jpg',
    }
    const originalOnlyCard = {
        media_type: 'photo',
        path: '/tmp/realtime-translation-missing-original-card-815.png',
        sourceArticleId: 'summary-realtime-photo-tail-card-translation-missing',
        sourceUrl: 'card:summary-realtime-photo-tail-card-translation-missing:original',
    }
    const renderProcessCalls: Array<{ article: any; config: any }> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            renderProcessCalls.push({ article, config })
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [originalOnlyCard],
                originalMediaFiles: [sourceMedia],
                mediaFiles: [sourceMedia, originalOnlyCard],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-realtime-photo-tail-card-translation-missing',
            [
                {
                    id: 816,
                    a_id: 'summary-realtime-photo-tail-card-translation-missing',
                    platform: Platform.X,
                    username: 'Photo Nick',
                    u_id: 'photo_uid',
                    content: 'photo body should not get an original-only Bilibili tail card',
                    translation: null,
                    translated_by: null,
                    url: 'https://x.com/photo_uid/status/816',
                    type: 'tweet',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: null,
                    has_media: true,
                    media: [
                        { type: 'photo', url: 'https://example.com/realtime-translation-missing-original-815.jpg' },
                    ],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    } finally {
        ;(processorRegistry as any).create = originalCreateProcessor
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.props?.media?.map((file: any) => path.basename(file.path))).toEqual([
        'realtime-translation-missing-original-815.jpg',
        'realtime-translation-missing-original-card-815.png',
    ])
    expect(renderProcessCalls).toHaveLength(1)
})

test('summary-card realtime media and later aggregation do not suppress each other', async () => {
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
            },
        } as any,
        'target-summary-card-realtime-media-only',
    )

    ;(pools as any).renderService = {
        process: async (article: any) => {
            const files =
                article.platform === Platform.Instagram
                    ? [
                          {
                              media_type: 'photo',
                              path: `/tmp/instagram-media-only-${article.id}.jpg`,
                              sourceArticleId: article.a_id,
                              sourceUrl: `https://example.com/instagram-media-only-${article.id}.jpg`,
                          },
                      ]
                    : []
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
    const instagramArticle = {
        id: 814,
        a_id: 'instagram-media-only',
        platform: Platform.Instagram,
        username: 'IG Nick',
        u_id: 'ig_member',
        content: 'instagram body should not become summary dynamic',
        url: 'https://www.instagram.com/p/media-only/',
        type: 'post',
        created_at: now,
        ref: null,
        has_media: true,
        media: [{ type: 'photo', url: 'https://example.com/instagram-media-only-814.jpg' }],
        extra: null,
        u_avatar: null,
    }
    const tiktokArticle = {
        id: 815,
        a_id: 'tiktok-no-media-only',
        platform: Platform.TikTok,
        username: 'TT Nick',
        u_id: 'tt_member',
        content: 'tiktok body should not become summary dynamic',
        url: 'https://www.tiktok.com/@tt_member/video/815',
        type: 'video',
        created_at: now + 1,
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    }

    await (pools as any).sendArticles(
        undefined,
        'summary-realtime-media-only-instagram',
        [instagramArticle],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )
    await (pools as any).sendArticles(
        undefined,
        'summary-realtime-media-only-tiktok',
        [tiktokArticle],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toContain('@ig_member')
    expect(target.sent[0]?.texts[0]).not.toContain('instagram body should not become summary dynamic')
    expect(target.sent[0]?.props?.media?.[0]?.path).toBe('/tmp/instagram-media-only-814.jpg')
    const queue = getSummaryCardQueueForTarget(pools, target.id)
    expect(queue?.items.size).toBe(2)
    expect(Array.from(queue?.items.values() || []).map((item: any) => item.article.a_id)).toEqual([
        'instagram-media-only',
        'tiktok-no-media-only',
    ])
    expect(
        await DB.ForwardBy.checkExist(instagramArticle.id, instagramArticle.platform, target.id, 'article'),
    ).toBeNull()
    expect(await DB.ForwardBy.checkExist(tiktokArticle.id, tiktokArticle.platform, target.id, 'article')).toBeNull()

    const articleSkips = Array.from(((DB.OutboundMessage as any).__records as Map<string, any>).values()).filter(
        (record: any) => record.task_kind === 'article',
    )
    expect(articleSkips).toHaveLength(0)
})

test('translated native companion is disabled when summary-card target is not native-first', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'bilibili'

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
                render_type: 'text-card',
            } as any,
            forwarders: [],
            crawlers: [],
        },
        new EventEmitter(),
    )

    const processCalls: any[] = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            processCalls.push({ article, config })
            return {
                text: article.content || '',
                textCollapseMode: 'article',
                cardMediaFiles: [{ media_type: 'photo', path: '/tmp/should-not-render.png' }],
                originalMediaFiles: [],
                mediaFiles: [],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const target = new RecordingForwarder(
        {
            summary_card: {
                enabled: true,
                send_first_native: false,
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                    processor_id: '22_7-social-ja-zh',
                },
            },
        } as any,
        'target-bili-no-native-companion',
    )
    const sourceMedia = { media_type: 'photo', path: '/tmp/source.jpg' }
    const originalCard = { media_type: 'photo', path: '/tmp/original-card.png' }

    const result = await (pools as any).buildTranslatedNativeCompanionCard(
        {
            id: 850,
            a_id: 'no-native-companion',
            platform: Platform.X,
            username: 'member',
            u_id: 'member',
            content: '原文',
            translation: '译文',
            translated_by: 'LLM',
            url: 'https://x.com/member/status/850',
            type: 'tweet',
            created_at: Math.floor(Date.now() / 1000),
            ref: null,
            has_media: true,
            media: [{ type: 'photo', url: 'https://example.com/source.jpg' }],
            extra: null,
            u_avatar: null,
        },
        {
            cardMediaFiles: [originalCard],
            originalMediaFiles: [sourceMedia],
        },
        { render_type: 'text-card' } as any,
        target,
        undefined,
        'task-no-native-companion',
        [originalCard],
    )

    expect(result).toBeNull()
    expect(processCalls).toHaveLength(0)
})

test('summary-card queues social platforms together per target instead of per route', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'qq'
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
                interval_seconds: 3600,
                include_original_media: false,
                send_first_immediately: false,
                flush_on_threshold: false,
                align_to_hour: true,
                flush_delay_seconds: 300,
            },
        } as any,
        'target-summary-card-social-shared',
    )

    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content || '',
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
    await (pools as any).sendArticles(
        undefined,
        'summary-social-x',
        [
            {
                id: 818,
                a_id: 'summary-social-x',
                platform: Platform.X,
                username: 'x member',
                u_id: 'x_member',
                content: 'x text',
                url: 'https://x.com/x_member/status/818',
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
        undefined,
        { routeKey: targetRouteKey(routeKey({ source: 'graph', crawlerId: 'x-crawler' }), target.id) },
    )
    await (pools as any).sendArticles(
        undefined,
        'summary-social-ig',
        [
            {
                id: 819,
                a_id: 'summary-social-ig',
                platform: Platform.Instagram,
                username: 'IG member',
                u_id: 'ig_member',
                content: 'ig text',
                url: 'https://www.instagram.com/p/819/',
                type: 'post',
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
        undefined,
        { routeKey: targetRouteKey(routeKey({ source: 'graph', crawlerId: 'ig-crawler' }), target.id) },
    )

    const queues = Array.from((pools as any).summaryCardQueues.values()) as Array<any>
    const windows = Array.from(((DB.AggregationWindow as any).__windows as Map<number, any>).values()).filter(
        (window: any) => window.status === 'open',
    )
    expect(target.sent).toHaveLength(0)
    expect(queues).toHaveLength(1)
    expect(queues[0]?.items.size).toBe(2)
    expect(Array.from(queues[0]?.items.values()).map((item: any) => item.article.platform)).toEqual([
        Platform.X,
        Platform.Instagram,
    ])
    expect(windows).toHaveLength(1)
})

test('summary-card targets leave official blog articles on the native send path', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'qq'
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
                interval_seconds: 3600,
                include_original_media: false,
                send_first_immediately: false,
                flush_on_threshold: false,
            },
        } as any,
        'target-summary-card-blog-native',
    )

    ;(pools as any).renderService = {
        process: async (article: any) => ({
            text: article.content || '',
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
    await (pools as any).sendArticles(
        undefined,
        'summary-blog-native',
        [
            {
                id: 821,
                a_id: 'summary-blog-native',
                platform: Platform.Website,
                username: 'Blog Member',
                u_id: '22/7:official-blog',
                content: 'official blog body',
                url: 'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/821',
                type: 'article',
                created_at: now,
                ref: null,
                has_media: false,
                media: [],
                extra: {
                    data: {
                        feed: 'official-blog',
                    },
                },
                u_avatar: null,
            },
        ],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toBe('official blog body')
    expect(getSummaryCardQueueForTarget(pools, target.id)).toBeUndefined()
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
    expect(target.sent[2]?.texts[0]).toContain('[图已发过]')
    expect(target.sent[2]?.texts[0]).not.toContain('重复媒体已文字缩略')
    expect(target.sent[2]?.texts[0]).not.toContain('24小时')
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
                single_item_behavior: 'summary_card',
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
    expect(packedArticles[0]?.content).toMatch(/【更新合并】1 条 \/ \d{4}～\d{4}/)
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
                single_item_behavior: 'summary_card',
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

test('idle-first native summary-card send respects target-wide durable recent visible sends across routes', async () => {
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
        'target-summary-card-native-first-durable-cooldown',
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

    const now = Math.floor(Date.now() / 1000)
    const durableRouteKey = targetRouteKey(
        routeKey({ source: 'graph', crawlerId: 'previous-role-route', formatterId: 'fmt-x-card-main' }),
        target.id,
    )
    ;((DB.OutboundMessage as any).__records as Map<string, any>).set('recent-summary-card', {
        id: 999,
        idempotency_key: 'recent-summary-card',
        route_key: durableRouteKey,
        target_id: target.id,
        target_platform: target.NAME,
        task_kind: 'summary_card',
        article_key: null,
        synthetic_key: 'recent-summary',
        payload_hash: 'recent-summary-hash',
        status: 'sent',
        provider_message_ids: null,
        segment_results: null,
        attempt_count: 1,
        last_error: null,
        created_at: now - 600,
        updated_at: now - 600,
        finished_at: now - 600,
    })

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-native-first-durable-cooldown',
            [
                {
                    id: 831,
                    a_id: 'summary-native-first-durable-cooldown',
                    platform: Platform.X,
                    username: 'chiharu-like member',
                    u_id: 'chiharu_okr',
                    content: '',
                    url: 'https://x.com/chiharu_okr/status/831',
                    type: 'retweet',
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
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(0)
    const queue = getSummaryCardQueueForTarget(pools, target.id)
    expect(queue?.items.size).toBe(1)
    expect(Array.from(queue?.items.values() || [])[0]?.article.a_id).toBe('summary-native-first-durable-cooldown')
})

test('idle-first translated summary-card targets append companion card to native sends', async () => {
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
                send_first_immediately: true,
                send_first_native: true,
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                },
            },
        } as any,
        'target-summary-card-native-first-translated',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const sourceMedia = {
        media_type: 'photo',
        path: '/tmp/native-source.jpg',
        sourceArticleId: 'summary-native-first-translated',
        sourceUrl: 'https://example.com/native-source.jpg',
        content_hash: 'native-source-hash',
    }
    const originalCard = { media_type: 'photo', path: '/tmp/native-original-card.png' }
    const translatedCard = { media_type: 'photo', path: '/tmp/native-translated-card.png' }
    const renderProcessCalls: Array<{ article: any; config: any }> = []
    const cleanupPaths: string[] = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            renderProcessCalls.push({ article, config })
            const translated = config?.card_features?.includes('translated-corner-badge')
            if (translated) {
                expect(article.content).toBe('native first translated text')
                expect(article.translation).toBeNull()
                expect(article.extra?.data?.translated_badge_label).toBe('译文')
                expect(config?.preloadedMediaFiles).toEqual([sourceMedia])
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [translatedCard],
                    originalMediaFiles: config?.preloadedMediaFiles || [],
                    mediaFiles: [...(config?.preloadedMediaFiles || []), translatedCard],
                }
            }
            return {
                text: article.content,
                textCollapseMode: 'article',
                cardMediaFiles: [originalCard],
                originalMediaFiles: [sourceMedia],
                mediaFiles: [sourceMedia, originalCard],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: (files: Array<{ path: string }>) => {
            cleanupPaths.push(...files.map((file) => file.path))
        },
    }

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-native-first-translated',
            [
                {
                    id: 831,
                    a_id: 'summary-native-first-translated',
                    platform: Platform.X,
                    username: 'native member',
                    u_id: 'native_member',
                    content: 'native first text',
                    translation: 'native first translated text',
                    translated_by: 'LLM',
                    url: 'https://x.com/native_member/status/831',
                    type: 'tweet',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: null,
                    has_media: true,
                    media: [
                        {
                            type: 'photo',
                            url: 'https://example.com/native-source.jpg',
                            alt: 'native alt',
                            translation: 'native translated alt',
                        },
                    ],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card', card_features: ['media-contain'] } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.texts[0]).toBe('native first text')
    expect(target.sent[0]?.props?.media).toEqual([sourceMedia, originalCard, translatedCard])
    expect(target.sent[0]?.props?.cardMedia).toEqual([originalCard, translatedCard])
    expect(target.sent[0]?.props?.contentMedia).toEqual([sourceMedia])
    expect(renderProcessCalls).toHaveLength(3)
    expect(renderProcessCalls[1]?.article?.translation).toBeNull()
    expect(renderProcessCalls[1]?.article?.media?.[0]?.translation).toBeUndefined()
    expect(renderProcessCalls[2]?.config?.card_features).toEqual(['media-contain', 'translated-corner-badge'])
    expect(cleanupPaths).toContain('/tmp/native-translated-card.png')
    expect(getSummaryCardQueueForTarget(pools, target.id)).toBeUndefined()
})

test('idle-first translated native companion translates missing article text before rendering', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[]; props: any }> = []

        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
            return
        }
    }

    const processCalls: string[] = []
    const articleUpdates: Array<{ id: number; platform: Platform; patch: any }> = []
    const originalCreateProcessor = (processorRegistry as any).create
    const originalArticleUpdate = (DB.Article as any).update
    ;(processorRegistry as any).create = async () => ({
        NAME: 'fake-native-translator',
        process: async (text: string) => {
            processCalls.push(text)
            return `译:${text}`
        },
        drop: async () => undefined,
    })
    ;(DB.Article as any).update = async (id: number, platform: Platform, patch: any) => {
        articleUpdates.push({ id, platform, patch })
        return { id, ...patch }
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
            processors: [
                {
                    id: '22_7-social-ja-zh',
                    provider: ProcessorProvider.OpenAI,
                    api_key: 'test-key',
                    cfg_processor: {
                        action: 'translate',
                    },
                },
            ],
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
                send_first_immediately: true,
                send_first_native: true,
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                    processor_id: '22_7-social-ja-zh',
                },
            },
        } as any,
        'target-summary-card-native-first-translated-on-demand',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const sourceMedia = {
        media_type: 'photo',
        path: '/tmp/native-on-demand-source.jpg',
        sourceArticleId: 'summary-native-on-demand',
        sourceUrl: 'https://example.com/native-on-demand-source.jpg',
        content_hash: 'native-on-demand-source-hash',
    }
    const originalCard = { media_type: 'photo', path: '/tmp/native-on-demand-original-card.png' }
    const translatedCard = { media_type: 'photo', path: '/tmp/native-on-demand-translated-card.png' }
    const renderProcessCalls: Array<{ article: any; config: any }> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            renderProcessCalls.push({ article, config })
            const translated = config?.card_features?.includes('translated-corner-badge')
            if (translated) {
                expect(article.content).toBe('译:native text without stored translation')
                expect(article.media?.[0]?.alt).toBe('译:native image alt')
                expect(article.translation).toBeNull()
                expect(article.extra?.data?.translated_badge_label).toBe('译文')
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [translatedCard],
                    originalMediaFiles: config?.preloadedMediaFiles || [],
                    mediaFiles: [...(config?.preloadedMediaFiles || []), translatedCard],
                }
            }
            return {
                text: article.content,
                textCollapseMode: 'article',
                cardMediaFiles: [originalCard],
                originalMediaFiles: [sourceMedia],
                mediaFiles: [sourceMedia, originalCard],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-native-on-demand',
            [
                {
                    id: 840,
                    a_id: 'summary-native-on-demand',
                    platform: Platform.X,
                    username: 'native member',
                    u_id: 'native_member',
                    content: 'native text without stored translation',
                    translation: null,
                    translated_by: null,
                    url: 'https://x.com/native_member/status/840',
                    type: 'tweet',
                    created_at: Math.floor(Date.now() / 1000),
                    ref: null,
                    has_media: true,
                    media: [
                        {
                            type: 'photo',
                            url: 'https://example.com/native-on-demand-source.jpg',
                            alt: 'native image alt',
                        },
                    ],
                    extra: null,
                    u_avatar: null,
                },
            ],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card', card_features: ['media-contain'] } as any,
        )
    } finally {
        ;(processorRegistry as any).create = originalCreateProcessor
        ;(DB.Article as any).update = originalArticleUpdate
    }

    expect(processCalls).toEqual(['native text without stored translation', 'native image alt'])
    expect(articleUpdates).toEqual([
        {
            id: 840,
            platform: Platform.X,
            patch: {
                translation: '译:native text without stored translation',
                translated_by: 'fake-native-translator',
                media: [
                    {
                        type: 'photo',
                        url: 'https://example.com/native-on-demand-source.jpg',
                        alt: 'native image alt',
                        translation: '译:native image alt',
                        translated_by: 'fake-native-translator',
                    },
                ],
            },
        },
    ])
    expect(target.sent).toHaveLength(1)
    expect(target.sent[0]?.props?.cardMedia).toEqual([originalCard, translatedCard])
    expect(renderProcessCalls).toHaveLength(3)
    expect(renderProcessCalls[1]?.article?.translation).toBeNull()
    expect(renderProcessCalls[1]?.article?.media?.[0]?.translation).toBeUndefined()
    expect(renderProcessCalls[2]?.config?.card_features).toEqual(['media-contain', 'translated-corner-badge'])
    expect(getSummaryCardQueueForTarget(pools, target.id)).toBeUndefined()
})

test('idle-first translated native companion is suppressed for text-only media visibility', async () => {
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
                translated_card: true,
            },
            media_visibility: {
                enabled: true,
                window_seconds: 86400,
                max_visible: 1,
                duplicate_behavior: 'text_only',
            },
        } as any,
        'target-summary-card-native-first-translated-text-only',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const sourceMediaFor = (id: string) => ({
        media_type: 'photo',
        path: `/tmp/${id}-source.jpg`,
        sourceArticleId: id,
        sourceUrl: 'https://example.com/same-visible-media.jpg',
        content_hash: 'same-visible-media-hash',
    })
    const renderProcessCalls: Array<{ article: any; config: any }> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            renderProcessCalls.push({ article, config })
            const translated = config?.card_features?.includes('translated-corner-badge')
            if (translated) {
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: `/tmp/${article.a_id}-translated-card.png` }],
                    originalMediaFiles: config?.preloadedMediaFiles || [],
                    mediaFiles: [
                        ...(config?.preloadedMediaFiles || []),
                        { media_type: 'photo', path: `/tmp/${article.a_id}-translated-card.png` },
                    ],
                }
            }
            const sourceMedia = sourceMediaFor(article.a_id)
            const originalCard = { media_type: 'photo', path: `/tmp/${article.a_id}-original-card.png` }
            return {
                text: article.content,
                textCollapseMode: 'article',
                cardMediaFiles: [originalCard],
                originalMediaFiles: [sourceMedia],
                mediaFiles: [sourceMedia, originalCard],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const makeArticle = (id: number) => ({
        id,
        a_id: `summary-native-visible-${id}`,
        platform: Platform.X,
        username: 'native member',
        u_id: 'native_member',
        content: `native visible text ${id}`,
        translation: `native visible translated ${id}`,
        translated_by: 'LLM',
        url: `https://x.com/native_member/status/${id}`,
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: true,
        media: [{ type: 'photo', url: 'https://example.com/same-visible-media.jpg' }],
        extra: null,
        u_avatar: null,
    })

    await (pools as any).sendArticles(
        undefined,
        'summary-native-visible-first',
        [makeArticle(832)],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )
    const expiredSentAt = Math.floor(Date.now() / 1000) - 7200
    for (const record of ((DB.OutboundMessage as any).__records as Map<string, any>).values()) {
        if (record.target_id === target.id && record.task_kind === 'article') {
            record.updated_at = expiredSentAt
            record.finished_at = expiredSentAt
        }
    }
    ;(pools as any).summaryCardLastSentAt.clear()
    ;(pools as any).summaryCardTargetLastSentAt.clear()
    await (pools as any).sendArticles(
        undefined,
        'summary-native-visible-second',
        [makeArticle(833)],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    expect(target.sent).toHaveLength(2)
    expect(target.sent[0]?.props?.cardMedia).toHaveLength(2)
    expect(target.sent[1]?.props?.cardMedia).toEqual([])
    expect(target.sent[1]?.props?.contentMedia).toEqual([])
    expect(target.sent[1]?.texts[0]).toContain('[图已发过]')
    expect(target.sent[1]?.texts[0]).not.toContain('重复媒体已文字缩略')
    expect(
        renderProcessCalls.filter((call) => call.config?.card_features?.includes('translated-corner-badge')),
    ).toHaveLength(1)
})

test('skip media visibility keeps generated native translated companion cards', async () => {
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
                translated_card: {
                    enabled: true,
                    badge_label: '译文',
                },
            },
            media_visibility: {
                enabled: true,
                window_seconds: 86400,
                max_visible: 1,
                duplicate_behavior: 'skip',
            },
        } as any,
        'target-summary-card-native-first-translated-skip-visibility',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const sourceMediaFor = (id: string) => ({
        media_type: 'photo',
        path: `/tmp/${id}-source.jpg`,
        sourceArticleId: id,
        sourceUrl: 'https://example.com/same-skip-visible-media.jpg',
        content_hash: 'same-skip-visible-media-hash',
    })
    const renderProcessCalls: Array<{ article: any; config: any }> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            renderProcessCalls.push({ article, config })
            const translated = config?.card_features?.includes('translated-corner-badge')
            if (translated) {
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: `/tmp/${article.a_id}-translated-card.png` }],
                    originalMediaFiles: config?.preloadedMediaFiles || [],
                    mediaFiles: [
                        ...(config?.preloadedMediaFiles || []),
                        { media_type: 'photo', path: `/tmp/${article.a_id}-translated-card.png` },
                    ],
                }
            }
            const sourceMedia = sourceMediaFor(article.a_id)
            const originalCard = { media_type: 'photo', path: `/tmp/${article.a_id}-original-card.png` }
            return {
                text: article.content,
                textCollapseMode: 'article',
                cardMediaFiles: [originalCard],
                originalMediaFiles: [sourceMedia],
                mediaFiles: [sourceMedia, originalCard],
            }
        },
        renderText: (article: any) => article.content || '',
        buildCardMediaFromRenderedFiles: () => [],
        cleanup: () => undefined,
    }

    const makeArticle = (id: number) => ({
        id,
        a_id: `summary-native-skip-visible-${id}`,
        platform: Platform.X,
        username: 'native member',
        u_id: 'native_member',
        content: `native skip visible text ${id}`,
        translation: `native skip visible translated ${id}`,
        translated_by: 'LLM',
        url: `https://x.com/native_member/status/${id}`,
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: true,
        media: [{ type: 'photo', url: 'https://example.com/same-skip-visible-media.jpg' }],
        extra: null,
        u_avatar: null,
    })

    await (pools as any).sendArticles(
        undefined,
        'summary-native-skip-visible-first',
        [makeArticle(834)],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )
    const expiredSentAt = Math.floor(Date.now() / 1000) - 7200
    for (const record of ((DB.OutboundMessage as any).__records as Map<string, any>).values()) {
        if (record.target_id === target.id && record.task_kind === 'article') {
            record.updated_at = expiredSentAt
            record.finished_at = expiredSentAt
        }
    }
    ;(pools as any).summaryCardLastSentAt.clear()
    ;(pools as any).summaryCardTargetLastSentAt.clear()

    await (pools as any).sendArticles(
        undefined,
        'summary-native-skip-visible-second',
        [makeArticle(835)],
        [{ forwarder: target, runtime_config: undefined }],
        { render_type: 'text-card' } as any,
    )

    expect(target.sent).toHaveLength(2)
    expect(target.sent[0]?.props?.cardMedia).toHaveLength(2)
    expect(target.sent[1]?.props?.contentMedia).toEqual([])
    expect(target.sent[1]?.props?.cardMedia).toEqual([
        { media_type: 'photo', path: '/tmp/summary-native-skip-visible-835-original-card.png' },
        { media_type: 'photo', path: '/tmp/summary-native-skip-visible-835-translated-card.png' },
    ])
    expect(target.sent[1]?.texts[0]).not.toContain('[图略]')
    expect(
        renderProcessCalls.filter((call) => call.config?.card_features?.includes('translated-corner-badge')),
    ).toHaveLength(2)
})

test('summary-card media duplicate budget keeps one in-card representative per card variant', async () => {
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
                media_duplicate_limit: 1,
                translated_card: true,
            },
        } as any,
        'target-summary-card-media-duplicate-limit',
    )

    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    const packedArticles: Array<any> = []
    ;(pools as any).renderService = {
        process: async (article: any, config?: any) => {
            if (article.id < 0) {
                const suffix = config?.card_features?.includes('translated-corner-badge') ? 'translated' : 'original'
                packedArticles.push(article)
                return {
                    text: article.content,
                    textCollapseMode: 'article',
                    cardMediaFiles: [{ media_type: 'photo', path: `/tmp/summary-card-dup-${suffix}.png` }],
                    originalMediaFiles: [],
                    mediaFiles: [{ media_type: 'photo', path: `/tmp/summary-card-dup-${suffix}.png` }],
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
                translation: `duplicate translated ${index}`,
                translated_by: 'LLM',
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
    expect(target.sent[0]?.props?.cardMedia).toEqual([
        { media_type: 'photo', path: '/tmp/summary-card-dup-original.png' },
        { media_type: 'photo', path: '/tmp/summary-card-dup-translated.png' },
    ])
    expect(packedArticles).toHaveLength(2)
    expect(packedArticles[0]?.media).toEqual([])
    expect(packedArticles[1]?.media).toEqual([])
    for (const packedArticle of packedArticles) {
        const itemMediaCounts = packedArticle?.extra?.data?.groups.flatMap((group: any) =>
            group.items.map((item: any) => item.media.length),
        )
        expect(itemMediaCounts).toEqual([1, 0, 0])
        const itemTexts = packedArticle?.extra?.data?.groups.flatMap((group: any) =>
            group.items.map((item: any) => item.text),
        )
        expect(itemTexts[0]).not.toContain('[图未列全]')
        expect(itemTexts[1]).toContain('[图未列全]')
        expect(itemTexts[2]).toContain('[图未列全]')
    }
    const originalItemTexts = packedArticles[0]?.extra?.data?.groups.flatMap((group: any) =>
        group.items.map((item: any) => item.text),
    )
    const translatedItemTexts = packedArticles[1]?.extra?.data?.groups.flatMap((group: any) =>
        group.items.map((item: any) => item.text),
    )
    expect(originalItemTexts[0]).toContain('duplicate text 0')
    expect(originalItemTexts[0]).not.toContain('duplicate translated 0')
    expect(translatedItemTexts[0]).toContain('duplicate translated 0')
})

function makeContentFingerprintArticle(id: number, aId: string, content: string) {
    return {
        id,
        a_id: aId,
        platform: Platform.X,
        username: 'member',
        u_id: 'member',
        content,
        url: `https://x.com/member/status/${aId}`,
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: false,
        media: [],
        extra: null,
        u_avatar: null,
    } as any
}

function makeContentFingerprintPools() {
    return new ForwarderPools(
        {
            forward_targets: [],
            cfg_forward_target: {} as any,
            connections: {} as any,
            formatters: [],
            cfg_forwarder: { render_type: 'text' } as any,
            forwarders: [],
            crawlers: [],
        },
        new EventEmitter(),
    )
}

test('content_fingerprint_dedup skips a same-content article that already sent to the target', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[] }> = []
        protected async realSend(texts: string[]): Promise<any> {
            this.sent.push({ texts })
            return
        }
    }

    const pools = makeContentFingerprintPools()
    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '813433032',
            content_fingerprint_dedup: true,
        } as any,
        'target-fp-dedup',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'fp-dedup-first',
            [makeContentFingerprintArticle(900, 'fp-first', '同一条内容用于指纹去重')],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text' } as any,
        )
        await (pools as any).sendArticles(
            undefined,
            'fp-dedup-second',
            [makeContentFingerprintArticle(901, 'fp-second', '同一条内容用于指纹去重')],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    // Only the first identical article reaches the provider; the second is fingerprint-suppressed.
    expect(target.sent).toHaveLength(1)
    const outboundRecords = Array.from(((DB.OutboundMessage as any).__records as Map<string, any>).values())
    expect(
        outboundRecords.some(
            (record) =>
                record.status === 'skipped' &&
                record.provider_message_ids?.reason === 'content_fingerprint_duplicate',
        ),
    ).toBeTrue()
})

test('content_fingerprint_dedup disabled lets identical content send twice', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[] }> = []
        protected async realSend(texts: string[]): Promise<any> {
            this.sent.push({ texts })
            return
        }
    }

    const pools = makeContentFingerprintPools()
    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '925668659',
            // no content_fingerprint_dedup -> default off
        } as any,
        'target-fp-off',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'fp-off-first',
            [makeContentFingerprintArticle(910, 'fp-off-first', '同一条内容但未开启指纹')],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text' } as any,
        )
        await (pools as any).sendArticles(
            undefined,
            'fp-off-second',
            [makeContentFingerprintArticle(911, 'fp-off-second', '同一条内容但未开启指纹')],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(2)
    const fingerprintRecords = (DB.ContentFingerprint as any).__records as Map<string, any>
    expect(fingerprintRecords.size).toBe(0)
})

test('content_fingerprint_dedup releases the claim when the send is blocked', async () => {
    class BlockingForwarder extends Forwarder {
        NAME = 'recording'
        attempts = 0
        protected async realSend(): Promise<any> {
            this.attempts += 1
            return
        }
        public async send(): Promise<any> {
            this.attempts += 1
            return { status: 'blocked', reason: 'simulated_block' }
        }
    }

    const pools = makeContentFingerprintPools()
    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const target = new BlockingForwarder(
        {
            block_until: '32h',
            group_id: '813433032',
            content_fingerprint_dedup: { enabled: true },
        } as any,
        'target-fp-block',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'fp-block-first',
            [makeContentFingerprintArticle(920, 'fp-block-first', '被拦截的内容应释放指纹')],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    // A blocked send is not a visible delivery, so the fingerprint must be released (not retained).
    const fingerprintRecords = (DB.ContentFingerprint as any).__records as Map<string, any>
    expect(fingerprintRecords.size).toBe(0)
})

test('content_fingerprint_dedup does not claim fingerprints in non-live outbound mode', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'recording'
        sent: Array<{ texts: string[] }> = []
        protected async realSend(texts: string[]): Promise<any> {
            this.sent.push({ texts })
            return
        }
    }

    const previousMode = process.env.IDOL_BBQ_OUTBOUND_SEND_MODE
    process.env.IDOL_BBQ_OUTBOUND_SEND_MODE = 'blocked'
    const pools = makeContentFingerprintPools()
    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '813433032',
            content_fingerprint_dedup: { enabled: true },
        } as any,
        'target-fp-non-live',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'fp-non-live',
            [makeContentFingerprintArticle(930, 'fp-non-live', '非 live 模式不应记录内容指纹')],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
        if (previousMode === undefined) {
            delete process.env.IDOL_BBQ_OUTBOUND_SEND_MODE
        } else {
            process.env.IDOL_BBQ_OUTBOUND_SEND_MODE = previousMode
        }
    }

    expect(target.sent).toHaveLength(0)
    const fingerprintRecords = (DB.ContentFingerprint as any).__records as Map<string, any>
    expect(fingerprintRecords.size).toBe(0)
})

function makeRealtimeFingerprintArticle(id: number, content: string) {
    return {
        id,
        a_id: `summary-realtime-fp-${id}`,
        platform: Platform.X,
        username: 'fp realtime member',
        u_id: 'fp_realtime_member',
        content,
        url: `https://x.com/fp_realtime_member/status/${id}`,
        type: 'tweet',
        created_at: Math.floor(Date.now() / 1000),
        ref: null,
        has_media: true,
        media: [{ type: 'photo', url: 'https://example.com/realtime-fp-source.jpg' }],
        extra: null,
        u_avatar: null,
    } as any
}

function makeRealtimeFingerprintRenderService() {
    // Stable content_hash so two different article ids carry the same media identity, mirroring a real
    // re-post of the same image under a new article id. Visibility is left disabled in these tests so only
    // the content fingerprint can suppress the second realtime media push.
    return {
        process: async (article: any) => {
            const file = {
                media_type: 'photo',
                path: `/tmp/realtime-fp-${article.id}.jpg`,
                sourceArticleId: article.a_id,
                sourceUrl: 'https://example.com/realtime-fp-source.jpg',
                content_hash: 'realtime-fp-stable-hash',
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
}

test('content_fingerprint_dedup suppresses a duplicate realtime media push without media visibility', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'qq'
        sent: Array<{ texts: string[]; props: any }> = []
        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
            return
        }
    }

    const pools = makeContentFingerprintPools()
    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = makeRealtimeFingerprintRenderService()

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '813433032',
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 7200,
                include_original_media: false,
                send_first_immediately: false,
                media_realtime: true,
                media_realtime_text: 'none',
                flush_on_threshold: false,
            },
            // No media_visibility here on purpose: isolate the content fingerprint as the only dedup gate.
            content_fingerprint_dedup: { enabled: true, window_seconds: 432000 },
        } as any,
        'target-realtime-fp-dedup',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        for (const id of [940, 941]) {
            await (pools as any).sendArticles(
                undefined,
                `summary-realtime-fp-${id}`,
                [makeRealtimeFingerprintArticle(id, '同一条实时媒体内容')],
                [{ forwarder: target, runtime_config: undefined }],
                { render_type: 'text-card' } as any,
            )
        }
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    // Only the first realtime media push reaches the provider; the second identical one is fingerprint-suppressed.
    expect(target.sent).toHaveLength(1)
    const realtimeOutbound = Array.from(((DB.OutboundMessage as any).__records as Map<string, any>).values()).filter(
        (record: any) => record.task_kind === 'summary_realtime_media' && record.target_id === target.id,
    )
    expect(
        realtimeOutbound.some(
            (record: any) =>
                record.status === 'skipped' &&
                record.provider_message_ids?.reason === 'content_fingerprint_duplicate',
        ),
    ).toBeTrue()
    const fingerprintRecords = (DB.ContentFingerprint as any).__records as Map<string, any>
    expect(fingerprintRecords.size).toBe(1)
})

test('content_fingerprint_dedup disabled lets identical realtime media push twice', async () => {
    class RecordingForwarder extends Forwarder {
        NAME = 'qq'
        sent: Array<{ texts: string[]; props: any }> = []
        protected async realSend(texts: string[], props?: any): Promise<any> {
            this.sent.push({ texts, props })
            return
        }
    }

    const pools = makeContentFingerprintPools()
    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = makeRealtimeFingerprintRenderService()

    const target = new RecordingForwarder(
        {
            block_until: '32h',
            group_id: '925668659',
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 7200,
                include_original_media: false,
                send_first_immediately: false,
                media_realtime: true,
                media_realtime_text: 'none',
                flush_on_threshold: false,
            },
            // no content_fingerprint_dedup and no media_visibility -> default off both ways.
        } as any,
        'target-realtime-fp-off',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        for (const id of [950, 951]) {
            await (pools as any).sendArticles(
                undefined,
                `summary-realtime-fp-off-${id}`,
                [makeRealtimeFingerprintArticle(id, '同一条实时媒体但未开启指纹')],
                [{ forwarder: target, runtime_config: undefined }],
                { render_type: 'text-card' } as any,
            )
        }
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    expect(target.sent).toHaveLength(2)
    const fingerprintRecords = (DB.ContentFingerprint as any).__records as Map<string, any>
    expect(fingerprintRecords.size).toBe(0)
})

test('content_fingerprint_dedup releases the claim when a realtime media send is blocked', async () => {
    class BlockingForwarder extends Forwarder {
        NAME = 'qq'
        attempts = 0
        protected async realSend(): Promise<any> {
            this.attempts += 1
            return
        }
        public async send(): Promise<any> {
            this.attempts += 1
            return { status: 'blocked', reason: 'simulated_block' }
        }
    }

    const pools = makeContentFingerprintPools()
    ;(pools as any).claimArticleChain = async () => true
    ;(pools as any).releaseArticleChain = async () => undefined
    ;(pools as any).renderService = makeRealtimeFingerprintRenderService()

    const target = new BlockingForwarder(
        {
            block_until: '32h',
            group_id: '813433032',
            summary_card: {
                enabled: true,
                threshold: 8,
                interval_seconds: 7200,
                include_original_media: false,
                send_first_immediately: false,
                media_realtime: true,
                media_realtime_text: 'none',
                flush_on_threshold: false,
            },
            content_fingerprint_dedup: { enabled: true },
        } as any,
        'target-realtime-fp-block',
    )

    const originalCheckExist = DB.ForwardBy.checkExist
    ;(DB.ForwardBy as any).checkExist = async () => null
    try {
        await (pools as any).sendArticles(
            undefined,
            'summary-realtime-fp-block',
            [makeRealtimeFingerprintArticle(960, '被拦截的实时媒体内容应释放指纹')],
            [{ forwarder: target, runtime_config: undefined }],
            { render_type: 'text-card' } as any,
        )
    } finally {
        ;(DB.ForwardBy as any).checkExist = originalCheckExist
    }

    // A blocked realtime send is not a visible delivery, so the fingerprint must be released (not retained).
    const fingerprintRecords = (DB.ContentFingerprint as any).__records as Map<string, any>
    expect(fingerprintRecords.size).toBe(0)
})
