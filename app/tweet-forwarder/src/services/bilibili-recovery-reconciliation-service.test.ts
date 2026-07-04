import { afterEach, beforeEach, expect, test } from 'bun:test'
import { mkdtempSync, rmSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import DB from '@/db'
import {
    createPrismaClient,
    prisma as activePrisma,
    setPrismaForTesting,
    type PrismaClientInstance,
} from '@/db/client'
import { ForwardTargetPlatformEnum } from '@/types/forwarder'
import { Platform } from '@idol-bbq-utils/spider/types'
import { reconcileBilibiliSubmissionsAfterDbRecovery } from './bilibili-recovery-reconciliation-service'

let previousPrisma: PrismaClientInstance
let testPrisma: PrismaClientInstance
let tempDir: string
let previousFetch: typeof globalThis.fetch
const previousEnv: Record<string, string | undefined> = {}

async function createArticleSchema(prisma: PrismaClientInstance) {
    await prisma.$executeRawUnsafe(`
        CREATE TABLE "youtube_article" (
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
    await prisma.$executeRawUnsafe('CREATE UNIQUE INDEX "youtube_article_a_id_key" ON "youtube_article"("a_id")')
    await prisma.$executeRawUnsafe('CREATE INDEX "youtube_article_created_at_idx" ON "youtube_article"("created_at" DESC)')
    for (const table of ['twitter_article', 'instagram_article', 'tiktok_article', 'website_article']) {
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
    }
}

async function createForwardBySchema(prisma: PrismaClientInstance) {
    await prisma.$executeRawUnsafe(`
        CREATE TABLE "forward_by" (
            "ref_id" INTEGER NOT NULL,
            "platform" TEXT NOT NULL,
            "bot_id" TEXT NOT NULL,
            "task_type" TEXT NOT NULL,
            PRIMARY KEY ("ref_id", "platform", "bot_id", "task_type")
        )
    `)
    await prisma.$executeRawUnsafe('CREATE INDEX "bot_id_index" ON "forward_by"("bot_id")')
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
    await prisma.$executeRawUnsafe('CREATE UNIQUE INDEX "outbound_messages_idempotency_key_key" ON "outbound_messages"("idempotency_key")')
    await prisma.$executeRawUnsafe('CREATE INDEX "outbound_messages_article_key_idx" ON "outbound_messages"("article_key")')
}

function setEnv(key: string, value: string | undefined) {
    if (!(key in previousEnv)) {
        previousEnv[key] = process.env[key]
    }
    if (value === undefined) {
        delete process.env[key]
    } else {
        process.env[key] = value
    }
}

beforeEach(async () => {
    previousPrisma = activePrisma
    previousFetch = globalThis.fetch
    tempDir = mkdtempSync(join(tmpdir(), 'idol-bbq-bili-recovery-'))
    testPrisma = createPrismaClient({
        datasources: {
            db: {
                url: `file:${join(tempDir, 'test.db')}`,
            },
        },
    })
    setPrismaForTesting(testPrisma)
    await createArticleSchema(testPrisma)
    await createForwardBySchema(testPrisma)
    await createOutboundMessageSchema(testPrisma)
    await DB.Article.save({
        platform: Platform.YouTube,
        a_id: 'A5JDZOovOMU',
        u_id: 'uploader',
        username: 'Uploader',
        created_at: 123,
        url: 'https://www.youtube.com/watch?v=A5JDZOovOMU',
        type: 'video',
        has_media: true,
    } as any)
})

afterEach(async () => {
    globalThis.fetch = previousFetch
    for (const [key, value] of Object.entries(previousEnv)) {
        if (value === undefined) {
            delete process.env[key]
        } else {
            process.env[key] = value
        }
    }
    setPrismaForTesting(previousPrisma)
    await testPrisma.$disconnect()
    rmSync(tempDir, { recursive: true, force: true })
})

test('reconciles Bilibili submissions after DB recovery marker and seeds sent state', async () => {
    const markerPath = join(tempDir, 'db-recovered.json')
    writeFileSync(markerPath, JSON.stringify({ recovered_at: '2026-07-05T00:00:00Z' }) + '\n')
    setEnv('IDOL_BBQ_DB_RECOVERY_MARKER', markerPath)
    setEnv('IDOL_BBQ_BILI_RECOVERY_MAX_PAGES', '1')

    globalThis.fetch = (async () =>
        new Response(
            JSON.stringify({
                code: 0,
                message: '0',
                data: {
                    page: { pn: 1, ps: 50, count: 1 },
                    arc_audits: [
                        {
                            Archive: {
                                bvid: 'BV1LVMP6bERp',
                                aid: 116862503356351,
                                title: 'uploaded video',
                                source: 'https://www.youtube.com/watch?v=A5JDZOovOMU',
                            },
                        },
                    ],
                },
            }),
        )) as typeof fetch

    const result = await reconcileBilibiliSubmissionsAfterDbRecovery({
        forward_targets: [
            {
                platform: ForwardTargetPlatformEnum.Bilibili,
                id: 'bilibili-转帖',
                cfg_platform: {
                    bili_jct: 'csrf',
                    sessdata: 'sess',
                    video_upload: { enabled: true },
                },
            },
        ],
    } as any)

    expect(result?.matched).toBe(1)
    expect(result?.seeded).toBe(1)
    expect(await DB.ForwardBy.checkExist(1, Platform.YouTube, 'bilibili-转帖', 'article')).toBeTruthy()
    const outbound = await DB.OutboundMessage.findLatestVisibleCompletion({
        target_id: 'bilibili-转帖',
        article_key: `${Platform.YouTube}:A5JDZOovOMU`,
    })
    expect(outbound?.status).toBe('sent')
    expect(outbound?.provider_message_ids).toMatchObject({ bvid: 'BV1LVMP6bERp' })
    expect(await Bun.file(markerPath).exists()).toBeFalse()
    expect(await Bun.file(`${markerPath}.bilibili-reconciled`).exists()).toBeTrue()
})
