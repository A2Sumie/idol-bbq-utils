import { afterEach, expect, test } from 'bun:test'
import DB from '@/db'
import { Platform } from '@idol-bbq-utils/spider/types'
import { TaskManager } from './task-manager'

const originalTaskQueue = { ...DB.TaskQueue }
const originalOutboundMessage = { ...DB.OutboundMessage }
const originalTargetHealth = { ...DB.TargetHealth }

afterEach(() => {
    Object.assign(DB.TaskQueue, originalTaskQueue)
    Object.assign(DB.OutboundMessage, originalOutboundMessage)
    Object.assign(DB.TargetHealth, originalTargetHealth)
})

test('TaskManager poll skips tasks that lose the pending claim race', async () => {
    const updatedStatuses: string[] = []
    ;(DB.TaskQueue as any).recoverStaleProcessing = async () => ({ count: 0 })
    ;(DB.TaskQueue as any).getPending = async () => [
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
    ;(DB.TaskQueue as any).claimPending = async () => null
    ;(DB.TaskQueue as any).updateStatus = async (_id: number, status: string) => {
        updatedStatuses.push(status)
    }

    const manager = new TaskManager({ getTarget: () => null } as any)
    await (manager as any).poll()

    expect(updatedStatuses).toEqual([])
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
    await (manager as any).sendAggregateToTarget(
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
    expect(sentPayloads).toHaveLength(1)
    expect(sentPayloads[0]?.props?.forceSend).toBeTrue()
    expect(health[0]?.last_send_status).toBe('sent')
})
