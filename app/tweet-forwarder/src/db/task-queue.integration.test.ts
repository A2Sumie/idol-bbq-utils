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
