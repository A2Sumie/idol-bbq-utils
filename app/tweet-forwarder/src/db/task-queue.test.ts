import { expect, test } from 'bun:test'
import DB from '@/db'

test('TaskQueue idempotent add only revives failed existing tasks', () => {
    expect(DB.TaskQueue.shouldReviveExistingTaskOnAdd({ status: DB.TaskQueue.STATUS.Failed })).toBeTrue()

    for (const status of [
        DB.TaskQueue.STATUS.Pending,
        DB.TaskQueue.STATUS.Processing,
        DB.TaskQueue.STATUS.Completed,
        DB.TaskQueue.STATUS.Cancelled,
    ]) {
        expect(DB.TaskQueue.shouldReviveExistingTaskOnAdd({ status })).toBeFalse()
    }
})

test('TaskQueue task type groups keep worker and inline API tasks distinct', () => {
    expect(DB.TaskQueue.WORKER_TYPES).toEqual(['aggregate_daily', 'aggregate_hourly'])
    expect(DB.TaskQueue.INLINE_API_TYPES).toEqual([
        'manual_crawler_run',
        'article_simulate',
        'article_reprocess',
        'article_resend',
        'processor_run',
    ])
    for (const inlineType of DB.TaskQueue.INLINE_API_TYPES) {
        expect(DB.TaskQueue.WORKER_TYPES.includes(inlineType as any)).toBeFalse()
    }
})

test('TaskQueue requeue data clears terminal failure fields while preserving reschedule metadata', () => {
    const data = DB.TaskQueue.buildRequeueFailedTaskData(
        { a: 1 },
        222,
        111,
        {
            source_ref: 'x:member',
            action_type: 'aggregate_hourly',
            idempotency_key: 'idem-1',
        },
    )

    expect(data).toEqual({
        payload: { a: 1 },
        execute_at: 222,
        updated_at: 111,
        status: 'pending',
        finished_at: null,
        last_error: null,
        result_summary: 'requeued failed idempotent task',
        source_ref: 'x:member',
        action_type: 'aggregate_hourly',
    })
})

test('TaskQueue interrupted inline failure data is terminal and operator-readable', () => {
    expect(DB.TaskQueue.buildInterruptedInlineFailureData(1234)).toEqual({
        status: 'failed',
        updated_at: 1234,
        finished_at: 1234,
        last_error: 'Inline API action was interrupted by runtime restart and cannot resume',
        result_summary: 'failed interrupted inline API action',
    })
})

test('TaskQueue terminal status policy is explicit', () => {
    for (const status of [DB.TaskQueue.STATUS.Completed, DB.TaskQueue.STATUS.Failed, DB.TaskQueue.STATUS.Cancelled]) {
        expect(DB.TaskQueue.isTerminalStatus(status)).toBeTrue()
    }

    for (const status of [DB.TaskQueue.STATUS.Pending, DB.TaskQueue.STATUS.Processing, 'queued', 'retrying']) {
        expect(DB.TaskQueue.isTerminalStatus(status)).toBeFalse()
    }
})

test('TaskQueue list limits are clamped for operator endpoints', () => {
    expect(DB.TaskQueue.clampListLimit(-1)).toBe(1)
    expect(DB.TaskQueue.clampListLimit(0)).toBe(1)
    expect(DB.TaskQueue.clampListLimit(50)).toBe(50)
    expect(DB.TaskQueue.clampListLimit(Number.NaN)).toBe(50)
    expect(DB.TaskQueue.clampListLimit(12.9)).toBe(12)
    expect(DB.TaskQueue.clampListLimit(999)).toBe(200)
})

test('TaskQueue list filters trim empty operator query params', () => {
    expect(DB.TaskQueue.buildListWhere()).toBeUndefined()
    expect(DB.TaskQueue.buildListWhere('pending')).toEqual({ status: 'pending' })
    expect(
        DB.TaskQueue.buildListWhere({
            status: ' failed ',
            type: ' aggregate_hourly ',
            source_ref: '',
            action_type: 'aggregate',
            idempotency_key: undefined,
        }),
    ).toEqual({
        status: 'failed',
        type: 'aggregate_hourly',
        action_type: 'aggregate',
    })
})

test('TaskQueue status counts summarize sparse groupBy rows', () => {
    expect(
        DB.TaskQueue.summarizeStatusCounts([
            { status: 'pending', _count: { _all: 3 } },
            { status: 'completed', _count: { _all: 9 } },
        ]),
    ).toEqual({
        pending: 3,
        completed: 9,
    })
})
