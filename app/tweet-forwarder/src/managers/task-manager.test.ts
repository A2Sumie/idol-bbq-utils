import { afterEach, expect, test } from 'bun:test'
import DB from '@/db'
import { Platform } from '@idol-bbq-utils/spider/types'
import { TaskManager } from './task-manager'

const originalTaskQueue = { ...DB.TaskQueue }
const originalOutboundMessage = { ...DB.OutboundMessage }
const originalTargetHealth = { ...DB.TargetHealth }
const originalArticle = { ...DB.Article }

afterEach(() => {
    Object.assign(DB.TaskQueue, originalTaskQueue)
    Object.assign(DB.OutboundMessage, originalOutboundMessage)
    Object.assign(DB.TargetHealth, originalTargetHealth)
    Object.assign(DB.Article, originalArticle)
})

test('TaskManager poll skips tasks that lose the pending claim race', async () => {
    const updatedStatuses: string[] = []
    const recoverCalls: any[] = []
    const getPendingCalls: any[] = []
    ;(DB.TaskQueue as any).recoverStaleProcessing = async (now: number, staleAfterSeconds: number, options?: unknown) => {
        recoverCalls.push({ now, staleAfterSeconds, options })
        return { count: 0 }
    }
    ;(DB.TaskQueue as any).getPending = async (now: number, options?: unknown) => {
        getPendingCalls.push({ now, options })
        return [
            {
                id: 1,
                type: 'aggregate_hourly',
                payload: {
                    platform: Platform.X,
                    u_id: 'member_a',
                    start: 100,
                    end: 200,
                    target_ids: [],
                },
            },
        ]
    }
    ;(DB.TaskQueue as any).claimPending = async () => null
    ;(DB.TaskQueue as any).updateStatus = async (_id: number, status: string) => {
        updatedStatuses.push(status)
    }

    const manager = new TaskManager({ getTarget: () => null } as any)
    await (manager as any).poll()

    expect(updatedStatuses).toEqual([])
    expect(recoverCalls[0]?.options).toEqual({ types: ['aggregate_daily', 'aggregate_hourly'] })
    expect(getPendingCalls[0]?.options).toEqual({ types: ['aggregate_daily', 'aggregate_hourly'] })
})

test('TaskManager aggregate sends are claimed through outbound messages', async () => {
    const sentPayloads: any[] = []
    const statuses: string[] = []
    const health: any[] = []
    const forwarder = {
        id: 'target-a',
        NAME: 'recording',
        send: async (text: string, props: any) => {
            sentPayloads.push({ text, props })
            return { status: 'sent', providerResult: { status: 200, data: { retcode: 0 } } }
        },
    }

    ;(DB.OutboundMessage as any).claim = async (data: any) => {
        statuses.push(`claim:${data.task_kind}`)
        return { claimed: true, record: { id: 1, ...data, status: 'planned' } }
    }
    ;(DB.OutboundMessage as any).markSending = async () => {
        statuses.push('sending')
    }
    ;(DB.OutboundMessage as any).markSent = async (_key: string, providerResult: unknown) => {
        statuses.push('sent')
        return { provider_message_ids: providerResult }
    }
    ;(DB.OutboundMessage as any).markQueued = async () => {
        statuses.push('queued')
    }
    ;(DB.OutboundMessage as any).markSkipped = async () => {
        statuses.push('skipped')
    }
    ;(DB.OutboundMessage as any).markFailed = async () => {
        statuses.push('failed')
    }
    ;(DB.OutboundMessage as any).markPartial = async () => {
        statuses.push('partial')
    }
    ;(DB.TargetHealth as any).mark = async (data: any) => {
        health.push(data)
    }

    const manager = new TaskManager({ getTarget: () => forwarder } as any)
    const outcome = await (manager as any).sendAggregateToTarget(
        'aggregate_hourly',
        'target-a',
        {
            platform: Platform.X,
            u_id: 'member_a',
            start: 100,
            end: 200,
        },
        'Hourly Batch for member_a',
        [{ path: '/tmp/hourly.png', media_type: 'photo' }],
    )

    expect(statuses).toEqual(['claim:aggregate_hourly', 'sending', 'sent'])
    expect(outcome).toMatchObject({ targetId: 'target-a', status: 'sent', retryable: false })
    expect(sentPayloads).toHaveLength(1)
    expect(sentPayloads[0]?.props?.forceSend).toBeTrue()
    expect(health[0]?.last_send_status).toBe('sent')
})

test('TaskManager retries aggregate tasks when target delivery fails', async () => {
    const retryCalls: any[] = []
    const statusUpdates: any[] = []
    const outboundStatuses: string[] = []
    const health: any[] = []
    const task = {
        id: 42,
        type: 'aggregate_daily',
        payload: {
            platform: Platform.X,
            u_id: 'member_retry',
            start: 100,
            end: 200,
            target_ids: ['target-a'],
        },
        result_summary: null,
    }
    const forwarder = {
        id: 'target-a',
        NAME: 'recording',
        send: async () => {
            throw new Error('transport down')
        },
    }

    ;(DB.Article as any).getArticlesByTimeRange = async () => [
        {
            id: 1,
            a_id: 'article-1',
            created_at: 150,
            content: 'hello',
            has_media: false,
            media: [],
        },
    ]
    ;(DB.TaskQueue as any).recoverStaleProcessing = async () => ({ count: 0 })
    ;(DB.TaskQueue as any).getPending = async () => [task]
    ;(DB.TaskQueue as any).claimPending = async () => ({ ...task, status: 'processing' })
    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }
    ;(DB.TaskQueue as any).retryLater = async (id: number, executeAt: number, meta?: unknown) => {
        retryCalls.push({ id, executeAt, meta })
    }
    ;(DB.OutboundMessage as any).claim = async (data: any) => {
        outboundStatuses.push(`claim:${data.task_kind}`)
        return { claimed: true, record: { id: 1, ...data, status: 'planned' } }
    }
    ;(DB.OutboundMessage as any).markSending = async () => {
        outboundStatuses.push('sending')
    }
    ;(DB.OutboundMessage as any).markFailed = async () => {
        outboundStatuses.push('failed')
    }
    ;(DB.TargetHealth as any).mark = async (data: any) => {
        health.push(data)
    }

    const manager = new TaskManager({ getTarget: () => forwarder } as any)
    await (manager as any).poll()

    expect(outboundStatuses).toEqual(['claim:aggregate_daily', 'sending', 'failed'])
    expect(retryCalls).toHaveLength(1)
    expect(retryCalls[0]?.id).toBe(42)
    expect(retryCalls[0]?.meta?.last_error).toContain('retryable target failure')
    expect(retryCalls[0]?.meta?.result_summary).toContain('retry_attempts=1/5')
    expect(retryCalls[0]?.meta?.result_summary).toContain('failed=1')
    expect(statusUpdates).toEqual([])
    expect(health[0]?.last_send_status).toBe('failed')
})

test('TaskManager marks aggregate tasks failed after delivery retries are exhausted', async () => {
    const retryCalls: any[] = []
    const statusUpdates: any[] = []
    const task = {
        id: 43,
        type: 'aggregate_daily',
        payload: {
            platform: Platform.X,
            u_id: 'member_retry',
            start: 100,
            end: 200,
            target_ids: ['target-a'],
        },
        result_summary: 'retry_attempts=5/5 aggregate_daily targets=1 failed=1 failed_targets=target-a:failed',
    }
    const forwarder = {
        id: 'target-a',
        NAME: 'recording',
        send: async () => {
            throw new Error('transport down')
        },
    }

    ;(DB.Article as any).getArticlesByTimeRange = async () => [
        {
            id: 1,
            a_id: 'article-1',
            created_at: 150,
            content: 'hello',
            has_media: false,
            media: [],
        },
    ]
    ;(DB.TaskQueue as any).recoverStaleProcessing = async () => ({ count: 0 })
    ;(DB.TaskQueue as any).getPending = async () => [task]
    ;(DB.TaskQueue as any).claimPending = async () => ({ ...task, status: 'processing' })
    ;(DB.TaskQueue as any).updateStatus = async (id: number, status: string, meta?: unknown) => {
        statusUpdates.push({ id, status, meta })
    }
    ;(DB.TaskQueue as any).retryLater = async (id: number, executeAt: number, meta?: unknown) => {
        retryCalls.push({ id, executeAt, meta })
    }
    ;(DB.OutboundMessage as any).claim = async (data: any) => {
        return { claimed: true, record: { id: 1, ...data, status: 'planned' } }
    }
    ;(DB.OutboundMessage as any).markSending = async () => undefined
    ;(DB.OutboundMessage as any).markFailed = async () => undefined
    ;(DB.TargetHealth as any).mark = async () => undefined

    const manager = new TaskManager({ getTarget: () => forwarder } as any)
    await (manager as any).poll()

    expect(retryCalls).toEqual([])
    expect(statusUpdates).toHaveLength(1)
    expect(statusUpdates[0]).toMatchObject({ id: 43, status: 'failed' })
    expect(statusUpdates[0]?.meta?.last_error).toContain('retryable target failure')
    expect(statusUpdates[0]?.meta?.result_summary).toContain('retry_exhausted attempts=5/5')
    expect(statusUpdates[0]?.meta?.result_summary).toContain('failed=1')
})
