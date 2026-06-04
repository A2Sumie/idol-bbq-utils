import { afterEach, beforeEach, expect, test } from 'bun:test'
import { mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import DB from '@/db'
import {
    createPrismaClient,
    prisma as activePrisma,
    setPrismaForTesting,
    type PrismaClientInstance,
} from '@/db/client'

let previousPrisma: PrismaClientInstance
let testPrisma: PrismaClientInstance
let tempDir: string

async function createTaskQueueSchema(prisma: PrismaClientInstance) {
    await prisma.$executeRawUnsafe(`
        CREATE TABLE "task_queue" (
            "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            "type" TEXT NOT NULL,
            "payload" JSONB NOT NULL,
            "status" TEXT NOT NULL DEFAULT 'pending',
            "created_at" INTEGER NOT NULL,
            "updated_at" INTEGER NOT NULL DEFAULT 0,
            "execute_at" INTEGER NOT NULL,
            "finished_at" INTEGER,
            "last_error" TEXT,
            "result_summary" TEXT,
            "source_ref" TEXT,
            "action_type" TEXT,
            "idempotency_key" TEXT
        )
    `)
    await prisma.$executeRawUnsafe('CREATE INDEX "task_queue_status_execute_at_idx" ON "task_queue"("status", "execute_at")')
    await prisma.$executeRawUnsafe(
        'CREATE UNIQUE INDEX "task_queue_type_idempotency_key_key" ON "task_queue"("type", "idempotency_key")',
    )
}

async function createOutboundMessageSchema(prisma: PrismaClientInstance) {
    await prisma.$executeRawUnsafe(`
        CREATE TABLE "outbound_messages" (
            "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            "idempotency_key" TEXT NOT NULL,
            "route_key" TEXT NOT NULL,
            "target_id" TEXT NOT NULL,
            "target_platform" TEXT,
            "task_kind" TEXT NOT NULL,
            "article_key" TEXT,
            "synthetic_key" TEXT,
            "payload_hash" TEXT NOT NULL,
            "status" TEXT NOT NULL DEFAULT 'planned',
            "provider_message_ids" JSONB,
            "segment_results" JSONB,
            "attempt_count" INTEGER NOT NULL DEFAULT 0,
            "last_error" TEXT,
            "created_at" INTEGER NOT NULL,
            "updated_at" INTEGER NOT NULL,
            "finished_at" INTEGER
        )
    `)
    await prisma.$executeRawUnsafe(
        'CREATE UNIQUE INDEX "outbound_messages_idempotency_key_key" ON "outbound_messages"("idempotency_key")',
    )
    await prisma.$executeRawUnsafe(
        'CREATE INDEX "outbound_messages_target_id_status_idx" ON "outbound_messages"("target_id", "status")',
    )
    await prisma.$executeRawUnsafe(
        'CREATE INDEX "outbound_messages_route_key_task_kind_idx" ON "outbound_messages"("route_key", "task_kind")',
    )
    await prisma.$executeRawUnsafe(
        'CREATE INDEX "outbound_messages_article_key_idx" ON "outbound_messages"("article_key")',
    )
    await prisma.$executeRawUnsafe(
        'CREATE INDEX "outbound_messages_synthetic_key_idx" ON "outbound_messages"("synthetic_key")',
    )
}

function outboundData(overrides: Partial<Parameters<typeof DB.OutboundMessage.claim>[0]> = {}) {
    return {
        idempotency_key: 'outbound-key',
        route_key: 'route:target',
        target_id: 'target-a',
        target_platform: 'QQ',
        task_kind: 'summary_card',
        article_key: 'x:1',
        synthetic_key: 'window:1',
        payload_hash: 'payload-a',
        ...overrides,
    }
}

beforeEach(async () => {
    previousPrisma = activePrisma
    tempDir = mkdtempSync(join(tmpdir(), 'idol-bbq-taskqueue-'))
    testPrisma = createPrismaClient({
        datasources: {
            db: {
                url: `file:${join(tempDir, 'test.db')}`,
            },
        },
    })
    setPrismaForTesting(testPrisma)
    await createTaskQueueSchema(testPrisma)
    await createOutboundMessageSchema(testPrisma)
})

afterEach(async () => {
    setPrismaForTesting(previousPrisma)
    await testPrisma.$disconnect()
    rmSync(tempDir, { recursive: true, force: true })
})

test('TaskQueue add revives failed idempotent tasks without duplicating pending tasks', async () => {
    const first = await DB.TaskQueue.add('aggregate_hourly', { value: 1 }, 100, {
        source_ref: 'x:member',
        action_type: 'aggregate',
        idempotency_key: 'same-window',
    })
    expect(first).toMatchObject({
        status: 'pending',
        execute_at: 100,
        source_ref: 'x:member',
        action_type: 'aggregate',
        idempotency_key: 'same-window',
    })

    await DB.TaskQueue.updateStatus(first.id, 'failed', {
        last_error: 'transport down',
        result_summary: 'failed once',
    })

    const revived = await DB.TaskQueue.add('aggregate_hourly', { value: 2 }, 200, {
        source_ref: 'x:member',
        action_type: 'aggregate',
        idempotency_key: 'same-window',
    })
    expect(revived.id).toBe(first.id)
    expect(revived).toMatchObject({
        status: 'pending',
        payload: { value: 2 },
        execute_at: 200,
        finished_at: null,
        last_error: null,
        result_summary: 'requeued failed idempotent task',
    })

    const deduped = await DB.TaskQueue.add('aggregate_hourly', { value: 3 }, 300, {
        source_ref: 'x:member',
        action_type: 'aggregate',
        idempotency_key: 'same-window',
    })
    expect(deduped.id).toBe(first.id)
    expect(deduped).toMatchObject({
        status: 'pending',
        payload: { value: 2 },
        execute_at: 200,
    })

    expect(await DB.TaskQueue.countsByStatus()).toEqual({ pending: 1 })
})

test('TaskQueue claim, stale recovery, filtering, and terminal status updates work on SQLite', async () => {
    const due = await DB.TaskQueue.add('aggregate_daily', { value: 'due' }, 100, {
        source_ref: 'x:due',
        action_type: 'aggregate',
        idempotency_key: 'due',
    })
    await DB.TaskQueue.add('aggregate_daily', { value: 'future' }, 300, {
        source_ref: 'x:future',
        action_type: 'aggregate',
        idempotency_key: 'future',
    })

    const pending = await DB.TaskQueue.getPending(150)
    expect(pending.map((task) => task.id)).toEqual([due.id])

    const claimed = await DB.TaskQueue.claimPending(due.id)
    expect(claimed).toMatchObject({
        id: due.id,
        status: 'processing',
        last_error: null,
    })
    expect(await DB.TaskQueue.claimPending(due.id)).toBeNull()

    await testPrisma.task_queue.update({
        where: { id: due.id },
        data: { updated_at: 1000 },
    })
    const recovered = await DB.TaskQueue.recoverStaleProcessing(1900, 30)
    expect(recovered.count).toBe(1)
    expect(await DB.TaskQueue.countsByStatus()).toEqual({ pending: 2 })

    await DB.TaskQueue.updateStatus(due.id, 'completed', {
        result_summary: 'done',
    })
    const completed = await DB.TaskQueue.list(10, {
        status: 'completed',
        type: 'aggregate_daily',
        source_ref: 'x:due',
        action_type: 'aggregate',
        idempotency_key: 'due',
    })
    expect(completed).toHaveLength(1)
    expect(completed[0]).toMatchObject({
        id: due.id,
        status: 'completed',
        result_summary: 'done',
    })
    expect(completed[0]?.finished_at).toBeNumber()

    expect(await DB.TaskQueue.countsByStatus()).toEqual({
        completed: 1,
        pending: 1,
    })
})

test('OutboundMessage retry resets stale provider and segment fields on SQLite', async () => {
    const firstClaim = await DB.OutboundMessage.claim(outboundData())
    expect(firstClaim.claimed).toBe(true)
    expect(firstClaim.record).toMatchObject({
        status: 'planned',
        attempt_count: 0,
        provider_message_ids: null,
        segment_results: null,
    })

    const sending = await DB.OutboundMessage.markSending('outbound-key')
    expect(sending).toMatchObject({
        status: 'sending',
        attempt_count: 1,
        provider_message_ids: null,
        segment_results: null,
        finished_at: null,
    })

    const queued = await DB.OutboundMessage.markQueued('outbound-key', {
        batchKey: 'old-batch',
        pendingUnits: 1,
    })
    expect(queued).toMatchObject({
        status: 'queued',
        provider_message_ids: {
            batchKey: 'old-batch',
            pendingUnits: 1,
        },
        segment_results: null,
    })

    await testPrisma.outbound_messages.update({
        where: { idempotency_key: 'outbound-key' },
        data: {
            updated_at: Math.floor(Date.now() / 1000) - 3600,
            segment_results: {
                diagnostic: 'old-diagnostic',
            },
        },
    })

    const reclaimed = await DB.OutboundMessage.claim(
        outboundData({
            route_key: 'route:new-target',
            payload_hash: 'payload-b',
        }),
    )
    expect(reclaimed.claimed).toBe(true)
    expect(reclaimed.record).toMatchObject({
        status: 'planned',
        route_key: 'route:new-target',
        payload_hash: 'payload-b',
        attempt_count: 1,
        provider_message_ids: null,
        segment_results: null,
        last_error: null,
        finished_at: null,
    })

    const resent = await DB.OutboundMessage.markSending('outbound-key')
    expect(resent).toMatchObject({
        status: 'sending',
        attempt_count: 2,
        provider_message_ids: null,
        segment_results: null,
        finished_at: null,
    })
})

test('OutboundMessage terminal states clear unrelated stale fields on SQLite', async () => {
    await DB.OutboundMessage.claim(outboundData({ idempotency_key: 'sent-key' }))
    await DB.OutboundMessage.markSending('sent-key')
    await testPrisma.outbound_messages.update({
        where: { idempotency_key: 'sent-key' },
        data: {
            segment_results: {
                diagnostic: 'stale-before-sent',
            },
        },
    })
    const sent = await DB.OutboundMessage.markSent('sent-key', { message_id: 'msg-1' })
    expect(sent).toMatchObject({
        status: 'sent',
        provider_message_ids: {
            message_id: 'msg-1',
        },
        segment_results: null,
        last_error: null,
    })

    await DB.OutboundMessage.claim(outboundData({ idempotency_key: 'partial-key' }))
    await DB.OutboundMessage.markSending('partial-key')
    await testPrisma.outbound_messages.update({
        where: { idempotency_key: 'partial-key' },
        data: {
            provider_message_ids: {
                stale: true,
            },
        },
    })
    const partial = await DB.OutboundMessage.markPartial(
        'partial-key',
        [{ message_id: 'visible-1' }],
        new Error('tail failed'),
    )
    expect(partial).toMatchObject({
        status: 'partial',
        provider_message_ids: null,
        segment_results: [{ message_id: 'visible-1' }],
        last_error: 'tail failed',
    })

    await DB.OutboundMessage.claim(outboundData({ idempotency_key: 'failed-key' }))
    await DB.OutboundMessage.markSending('failed-key')
    await testPrisma.outbound_messages.update({
        where: { idempotency_key: 'failed-key' },
        data: {
            provider_message_ids: {
                stale: true,
            },
            segment_results: {
                diagnostic: 'stale-before-failed',
            },
        },
    })
    const failed = await DB.OutboundMessage.markFailed('failed-key', new Error('network failed'))
    expect(failed).toMatchObject({
        status: 'failed',
        provider_message_ids: null,
        segment_results: null,
        last_error: 'network failed',
    })
})

test('OutboundMessage suppresses terminal duplicates while recording payload drift on SQLite', async () => {
    await DB.OutboundMessage.claim(outboundData({ idempotency_key: 'terminal-key' }))
    await DB.OutboundMessage.markSending('terminal-key')
    await DB.OutboundMessage.markSent('terminal-key', { message_id: 'msg-1' })

    const suppressed = await DB.OutboundMessage.claim(
        outboundData({
            idempotency_key: 'terminal-key',
            payload_hash: 'payload-changed',
        }),
    )
    expect(suppressed.claimed).toBe(false)
    expect(suppressed.record.status).toBe('sent')
    expect(suppressed.record.segment_results).toMatchObject({
        diagnostic: 'suppressed_payload_drift',
        existing: {
            payload_hash: 'payload-a',
            status: 'sent',
        },
        incoming: {
            payload_hash: 'payload-changed',
        },
    })
})
