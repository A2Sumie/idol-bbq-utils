import { afterEach, beforeEach, expect, test } from 'bun:test'
import { mkdtempSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import DB from '@/db'
import { Platform } from '@idol-bbq-utils/spider/types'
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

async function createAggregationSchema(prisma: PrismaClientInstance) {
    await prisma.$executeRawUnsafe('PRAGMA foreign_keys = ON')
    await prisma.$executeRawUnsafe(`
        CREATE TABLE "aggregation_windows" (
            "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            "idempotency_key" TEXT NOT NULL,
            "route_key" TEXT NOT NULL,
            "target_id" TEXT NOT NULL,
            "mode" TEXT NOT NULL,
            "window_start" INTEGER NOT NULL,
            "window_end" INTEGER NOT NULL,
            "status" TEXT NOT NULL DEFAULT 'open',
            "payload_hash" TEXT,
            "created_at" INTEGER NOT NULL,
            "updated_at" INTEGER NOT NULL,
            "finished_at" INTEGER
        )
    `)
    await prisma.$executeRawUnsafe(
        'CREATE UNIQUE INDEX "aggregation_windows_idempotency_key_key" ON "aggregation_windows"("idempotency_key")',
    )
    await prisma.$executeRawUnsafe(
        'CREATE INDEX "aggregation_windows_route_key_mode_status_idx" ON "aggregation_windows"("route_key", "mode", "status")',
    )
    await prisma.$executeRawUnsafe(
        'CREATE INDEX "aggregation_windows_target_id_mode_status_idx" ON "aggregation_windows"("target_id", "mode", "status")',
    )
    await prisma.$executeRawUnsafe(`
        CREATE TABLE "aggregation_items" (
            "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            "window_id" INTEGER NOT NULL,
            "article_key" TEXT NOT NULL,
            "article_row_id" INTEGER NOT NULL,
            "platform" TEXT NOT NULL,
            "payload" JSONB,
            "created_at" INTEGER NOT NULL,
            CONSTRAINT "aggregation_items_window_id_fkey" FOREIGN KEY ("window_id") REFERENCES "aggregation_windows" ("id") ON DELETE CASCADE ON UPDATE NO ACTION
        )
    `)
    await prisma.$executeRawUnsafe(
        'CREATE UNIQUE INDEX "aggregation_items_window_id_article_key_key" ON "aggregation_items"("window_id", "article_key")',
    )
    await prisma.$executeRawUnsafe(
        'CREATE INDEX "aggregation_items_article_key_idx" ON "aggregation_items"("article_key")',
    )
}

async function createArticleSchema(prisma: PrismaClientInstance) {
    for (const table of ['twitter_article', 'instagram_article']) {
        await prisma.$executeRawUnsafe(`
            CREATE TABLE "${table}" (
                "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                "a_id" TEXT NOT NULL,
                "u_id" TEXT NOT NULL,
                "username" TEXT NOT NULL,
                "created_at" INTEGER NOT NULL,
                "content" TEXT,
                "translation" TEXT,
                "translated_by" TEXT,
                "url" TEXT NOT NULL,
                "type" TEXT NOT NULL,
                "ref" INTEGER,
                "has_media" BOOLEAN NOT NULL,
                "media" JSONB,
                "extra" JSONB,
                "u_avatar" TEXT
            )
        `)
        await prisma.$executeRawUnsafe(`CREATE UNIQUE INDEX "${table}_a_id_key" ON "${table}"("a_id")`)
        await prisma.$executeRawUnsafe(`CREATE INDEX "${table}_created_at_idx" ON "${table}"("created_at" DESC)`)
    }
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
    await createAggregationSchema(testPrisma)
    await createArticleSchema(testPrisma)
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

    await DB.TaskQueue.updateStatus(first.id, DB.TaskQueue.STATUS.Failed, {
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

    await DB.TaskQueue.updateStatus(due.id, DB.TaskQueue.STATUS.Completed, {
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

test('OutboundMessage payload drift preserves partial segment evidence on SQLite', async () => {
    const partialSegments = [{ message_id: 'visible-1' }, { diagnostic: 'tail-not-sent' }]

    await DB.OutboundMessage.claim(outboundData({ idempotency_key: 'partial-drift-key' }))
    await DB.OutboundMessage.markSending('partial-drift-key')
    await DB.OutboundMessage.markPartial('partial-drift-key', partialSegments, new Error('tail failed'))

    const suppressed = await DB.OutboundMessage.claim(
        outboundData({
            idempotency_key: 'partial-drift-key',
            payload_hash: 'payload-changed',
        }),
    )

    expect(suppressed.claimed).toBe(false)
    expect(suppressed.record.status).toBe('partial')
    expect(suppressed.record.segment_results).toMatchObject({
        diagnostic: 'suppressed_payload_drift',
        existing: {
            payload_hash: 'payload-a',
            status: 'partial',
        },
        incoming: {
            payload_hash: 'payload-changed',
        },
        previous_segment_results: partialSegments,
    })

    const repeated = await DB.OutboundMessage.claim(
        outboundData({
            idempotency_key: 'partial-drift-key',
            payload_hash: 'payload-changed-again',
        }),
    )

    expect(repeated.claimed).toBe(false)
    expect(repeated.record.segment_results).toMatchObject({
        diagnostic: 'suppressed_payload_drift',
        incoming: {
            payload_hash: 'payload-changed-again',
        },
        previous_segment_results: partialSegments,
    })
})

test('AggregationWindow creates idempotent open windows and lists oldest open windows on SQLite', async () => {
    const first = await DB.AggregationWindow.getOrCreateOpen({
        idempotency_key: 'summary-window-a',
        route_key: 'route:target-a',
        target_id: 'target-a',
        mode: 'summary_card',
        window_start: 1000,
        window_end: 4600,
    })
    const same = await DB.AggregationWindow.getOrCreateOpen({
        idempotency_key: 'summary-window-a',
        route_key: 'route:target-a',
        target_id: 'target-a',
        mode: 'summary_card',
        window_start: 2000,
        window_end: 5600,
    })
    const second = await DB.AggregationWindow.getOrCreateOpen({
        idempotency_key: 'summary-window-b',
        route_key: 'route:target-b',
        target_id: 'target-b',
        mode: 'summary_card',
        window_start: 2000,
        window_end: 5600,
    })
    const realtime = await DB.AggregationWindow.getOrCreateOpen({
        idempotency_key: 'realtime-window',
        route_key: 'route:target-a',
        target_id: 'target-a',
        mode: 'realtime_media',
        window_start: 3000,
        window_end: 6600,
    })

    expect(same.id).toBe(first.id)
    expect(same.window_start).toBe(1000)

    await testPrisma.aggregation_windows.update({
        where: { id: first.id },
        data: { created_at: 300 },
    })
    await testPrisma.aggregation_windows.update({
        where: { id: second.id },
        data: { created_at: 100 },
    })
    await DB.AggregationWindow.updateStatus(realtime.id, DB.AggregationWindow.STATUS.Completed, {
        payload_hash: 'sent-realtime',
    })

    expect(await DB.AggregationWindow.getOpen('route:target-b', 'target-b', 'summary_card')).toMatchObject({
        id: second.id,
    })
    expect((await DB.AggregationWindow.listOpen('summary_card')).map((window) => window.id)).toEqual([
        second.id,
        first.id,
    ])
    expect((await DB.AggregationWindow.listOpen()).map((window) => window.id)).toEqual([second.id, first.id])
})

test('AggregationWindow status updates use explicit terminal policy on SQLite', async () => {
    const window = await DB.AggregationWindow.getOrCreateOpen({
        idempotency_key: 'status-window',
        route_key: 'route:target',
        target_id: 'target',
        mode: 'summary_card',
        window_start: 1000,
        window_end: 4600,
    })

    const completed = await DB.AggregationWindow.updateStatus(window.id, DB.AggregationWindow.STATUS.Completed, {
        payload_hash: 'hash-a',
    })
    expect(completed).toMatchObject({
        status: 'completed',
        payload_hash: 'hash-a',
    })
    expect(completed.finished_at).toBeNumber()

    const reopened = await DB.AggregationWindow.updateStatus(window.id, DB.AggregationWindow.STATUS.Open)
    expect(reopened).toMatchObject({
        status: 'open',
        payload_hash: 'hash-a',
        finished_at: null,
    })

    const sent = await DB.AggregationWindow.updateStatus(window.id, DB.AggregationWindow.STATUS.Sent)
    expect(sent.status).toBe('sent')
    expect(sent.finished_at).toBeNumber()
    expect(DB.AggregationWindow.isTerminalStatus(DB.AggregationWindow.STATUS.Sent)).toBe(true)
    expect(DB.AggregationWindow.isTerminalStatus(DB.AggregationWindow.STATUS.Open)).toBe(false)
})

test('AggregationWindow items upsert full restore identity while preserving queue order on SQLite', async () => {
    const window = await DB.AggregationWindow.getOrCreateOpen({
        idempotency_key: 'items-window',
        route_key: 'route:target',
        target_id: 'target',
        mode: 'summary_card',
        window_start: 1000,
        window_end: 4600,
    })

    const first = await DB.AggregationWindow.upsertItem({
        window_id: window.id,
        article_key: '1:article-a',
        article_row_id: 10,
        platform: 1,
        payload: {
            queuedAt: 1000,
            title: 'old',
        },
    })
    await DB.AggregationWindow.upsertItem({
        window_id: window.id,
        article_key: '1:article-b',
        article_row_id: 20,
        platform: 1,
        payload: {
            queuedAt: 1001,
            title: 'middle',
        },
    })
    const updated = await DB.AggregationWindow.upsertItem({
        window_id: window.id,
        article_key: '1:article-a',
        article_row_id: 11,
        platform: '2',
        payload: {
            queuedAt: 1002,
            title: 'new',
        },
    })

    expect(updated).toMatchObject({
        id: first.id,
        article_row_id: 11,
        platform: '2',
        payload: {
            queuedAt: 1002,
            title: 'new',
        },
        created_at: first.created_at,
    })

    const items = await DB.AggregationWindow.listItems(window.id)
    expect(items.map((item) => item.article_key)).toEqual(['1:article-a', '1:article-b'])
    expect(items[0]).toMatchObject({
        article_row_id: 11,
        platform: '2',
    })

    await testPrisma.aggregation_windows.delete({ where: { id: window.id } })
    expect(await testPrisma.aggregation_items.count()).toBe(0)
})

test('Article getSingleArticle resolves same-platform reference chains on SQLite', async () => {
    const source = await testPrisma.twitter_article.create({
        data: {
            a_id: 'source-post',
            u_id: 'source-user',
            username: 'source',
            created_at: 1000,
            content: 'source body',
            translation: null,
            translated_by: null,
            url: 'https://x.com/source/status/source-post',
            type: 'tweet',
            ref: null,
            has_media: false,
            media: [],
            extra: null,
            u_avatar: null,
        },
    })
    const main = await testPrisma.twitter_article.create({
        data: {
            a_id: 'main-post',
            u_id: 'main-user',
            username: 'main',
            created_at: 1001,
            content: 'main body',
            translation: null,
            translated_by: null,
            url: 'https://x.com/main/status/main-post',
            type: 'tweet',
            ref: source.id,
            has_media: false,
            media: [],
            extra: null,
            u_avatar: null,
        },
    })

    const article = await DB.Article.getSingleArticle(main.id, Platform.X)
    expect(article).toMatchObject({
        id: main.id,
        platform: Platform.X,
        ref: {
            id: source.id,
            platform: Platform.X,
            a_id: 'source-post',
        },
    })
})

test('Article getSingleArticle breaks cyclic reference chains on SQLite', async () => {
    const first = await testPrisma.twitter_article.create({
        data: {
            a_id: 'cycle-a',
            u_id: 'cycle-user-a',
            username: 'cycle-a',
            created_at: 1000,
            content: 'cycle a',
            translation: null,
            translated_by: null,
            url: 'https://x.com/cycle/status/a',
            type: 'tweet',
            ref: null,
            has_media: false,
            media: [],
            extra: null,
            u_avatar: null,
        },
    })
    const second = await testPrisma.twitter_article.create({
        data: {
            a_id: 'cycle-b',
            u_id: 'cycle-user-b',
            username: 'cycle-b',
            created_at: 1001,
            content: 'cycle b',
            translation: null,
            translated_by: null,
            url: 'https://x.com/cycle/status/b',
            type: 'tweet',
            ref: first.id,
            has_media: false,
            media: [],
            extra: null,
            u_avatar: null,
        },
    })
    await testPrisma.twitter_article.update({
        where: { id: first.id },
        data: { ref: second.id },
    })

    const article = await DB.Article.getSingleArticle(first.id, Platform.X)
    const ref = article?.ref as any
    expect(ref).toMatchObject({
        id: second.id,
        platform: Platform.X,
    })
    expect(ref.ref).toBeNull()
})

test('AggregationWindow item platform round-trips into Article restore lookup on SQLite', async () => {
    const articleRow = await testPrisma.instagram_article.create({
        data: {
            a_id: 'ig-post',
            u_id: 'ig-user',
            username: 'instagram user',
            created_at: 1000,
            content: 'ig body',
            translation: null,
            translated_by: null,
            url: 'https://www.instagram.com/p/ig-post/',
            type: 'post',
            ref: null,
            has_media: true,
            media: [{ type: 'photo', url: 'https://example.test/ig.jpg' }],
            extra: null,
            u_avatar: null,
        },
    })
    const window = await DB.AggregationWindow.getOrCreateOpen({
        idempotency_key: 'restore-platform-window',
        route_key: 'route:target',
        target_id: 'target',
        mode: 'summary_card',
        window_start: 1000,
        window_end: 4600,
    })

    await DB.AggregationWindow.upsertItem({
        window_id: window.id,
        article_key: `${Platform.Instagram}:ig-post`,
        article_row_id: articleRow.id,
        platform: Platform.Instagram,
        payload: { queuedAt: 1000 },
    })

    const [item] = await DB.AggregationWindow.listItems(window.id)
    const restored = await DB.Article.getSingleArticle(item.article_row_id, Number(item.platform) as Platform)
    expect(restored).toMatchObject({
        id: articleRow.id,
        platform: Platform.Instagram,
        a_id: 'ig-post',
    })
})
