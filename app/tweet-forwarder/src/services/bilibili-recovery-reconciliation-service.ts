import crypto from 'crypto'
import fs from 'fs'
import path from 'path'
import { Logger } from '@idol-bbq-utils/log'
import type { AppConfig } from '@/types'
import { ForwardTargetPlatformEnum, type ForwardTarget } from '@/types/forwarder'
import DB, { type ArticleWithId } from '@/db'
import { articleKey, articleOutboundKey, payloadHash } from '@/services/outbound-message-service'

const DEFAULT_MARKER_PATH = '/tmp/tweet-forwarder/db-recovered.json'
const DEFAULT_PAGE_SIZE = 50
const DEFAULT_MAX_PAGES = 50
const DEFAULT_ARCHIVES_URL = 'https://member.bilibili.com/x/web/archives'
const BILIBILI_TARGET_PLATFORM = 'bilibili'

type CookieDocument = {
    cookie_info?: {
        cookies?: Array<{ name?: unknown; value?: unknown }>
    }
}

type BilibiliArchive = {
    aid?: string | number
    bvid?: string
    title?: string
    source?: string
    ptime?: number
    state?: number
    state_desc?: string
}

type RecoveryMarker = {
    recovered_at?: string
    source_backup?: string
    corrupt_copy?: string
}

type ReconcileTarget = {
    id: string
    cookieHeader: string
}

type ReconcileResult = {
    markerPath: string
    marker?: RecoveryMarker
    archives: number
    matched: number
    seeded: number
    skippedNoSource: number
    skippedNoArticle: number
    targets: number
}

function normalizeBool(value: string | undefined, defaultValue: boolean) {
    if (value === undefined || value === '') {
        return defaultValue
    }
    const normalized = value.trim().toLowerCase()
    if (['1', 'true', 'yes', 'on'].includes(normalized)) {
        return true
    }
    if (['0', 'false', 'no', 'off'].includes(normalized)) {
        return false
    }
    return defaultValue
}

function normalizeUrl(value: unknown) {
    return String(value || '').trim()
}

function resolveConfiguredPath(candidate?: string) {
    if (!candidate) {
        return undefined
    }
    return path.isAbsolute(candidate) ? candidate : path.resolve(process.cwd(), candidate)
}

function normalizeCookieDocument(document: unknown): CookieDocument {
    if (!document || typeof document !== 'object') {
        throw new Error('Bilibili cookie file must contain a JSON object')
    }
    const cookies = (document as CookieDocument).cookie_info?.cookies
    if (!Array.isArray(cookies) || cookies.length === 0) {
        throw new Error('Bilibili cookie file must contain cookie_info.cookies')
    }
    return document as CookieDocument
}

function cookieHeaderFromDocument(document: CookieDocument) {
    const parts = (document.cookie_info?.cookies || [])
        .map((cookie) => {
            const name = typeof cookie.name === 'string' ? cookie.name.trim() : ''
            const value = typeof cookie.value === 'string' ? cookie.value : ''
            return name && value ? `${name}=${value}` : ''
        })
        .filter(Boolean)
    if (parts.length === 0) {
        throw new Error('Bilibili cookie document does not contain usable cookies')
    }
    return parts.join('; ')
}

function buildCookieHeader(target: ForwardTarget<ForwardTargetPlatformEnum.Bilibili>) {
    const videoUploadConfig = target.cfg_platform.video_upload
    const cookiePath = resolveConfiguredPath(videoUploadConfig?.cookie_file)
    if (cookiePath && fs.existsSync(cookiePath)) {
        const document = normalizeCookieDocument(JSON.parse(fs.readFileSync(cookiePath, 'utf8')))
        return cookieHeaderFromDocument(document)
    }

    const { sessdata, bili_jct } = target.cfg_platform
    if (!sessdata || !bili_jct) {
        throw new Error(`Bilibili target ${target.id || '(generated id)'} has no cookie_file or sessdata/bili_jct`)
    }
    return `SESSDATA=${sessdata}; bili_jct=${bili_jct}`
}

function targetIdForConfig(target: ForwardTarget) {
    if (target.id) {
        return target.id
    }
    const { block_until, replace_regex, ...restToBeHashed } = target.cfg_platform as any
    const forwarderToBeHashed = {
        platform: target.platform,
        cfg_platform: restToBeHashed,
    }
    return `${target.platform}-${crypto.createHash('md5').update(JSON.stringify(forwarderToBeHashed)).digest('hex')}`
}

function resolveBilibiliTargets(config: AppConfig): ReconcileTarget[] {
    return (config.forward_targets || [])
        .filter(
            (target): target is ForwardTarget<ForwardTargetPlatformEnum.Bilibili> =>
                target.platform === ForwardTargetPlatformEnum.Bilibili && Boolean(target.cfg_platform.video_upload?.enabled),
        )
        .map((target) => ({
            id: targetIdForConfig(target),
            cookieHeader: buildCookieHeader(target),
        }))
}

function readMarker(markerPath: string): RecoveryMarker | undefined {
    try {
        return JSON.parse(fs.readFileSync(markerPath, 'utf8')) as RecoveryMarker
    } catch {
        return undefined
    }
}

async function fetchBilibiliArchives(cookieHeader: string, log?: Logger) {
    const pageSize = Math.max(1, Math.min(Number(process.env.IDOL_BBQ_BILI_RECOVERY_PAGE_SIZE || DEFAULT_PAGE_SIZE), 100))
    const maxPages = Math.max(1, Math.min(Number(process.env.IDOL_BBQ_BILI_RECOVERY_MAX_PAGES || DEFAULT_MAX_PAGES), 200))
    const baseUrl = process.env.IDOL_BBQ_BILI_RECOVERY_ARCHIVES_URL || DEFAULT_ARCHIVES_URL
    const archives: BilibiliArchive[] = []

    for (let page = 1; page <= maxPages; page += 1) {
        const url = new URL(baseUrl)
        url.searchParams.set('status', 'is_pubing,pubed,not_pubed')
        url.searchParams.set('pn', String(page))
        url.searchParams.set('ps', String(pageSize))
        const response = await fetch(url, {
            headers: {
                'user-agent':
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
                referer: 'https://member.bilibili.com/platform/upload-manager/article',
                cookie: cookieHeader,
            },
        })
        if (!response.ok) {
            throw new Error(`Bilibili archives API returned HTTP ${response.status}`)
        }
        const payload = (await response.json()) as any
        if (payload?.code !== 0) {
            throw new Error(`Bilibili archives API returned code ${payload?.code}: ${payload?.message || payload?.msg || ''}`)
        }
        const data = payload?.data || {}
        const items = (data.arc_audits || data.arcs || []) as Array<any>
        for (const item of items) {
            const archive = item?.Archive || item?.archive || item
            if (archive && typeof archive === 'object') {
                archives.push(archive as BilibiliArchive)
            }
        }
        const total = Number(data.page?.count || 0)
        log?.info(`Read Bilibili submissions page ${page}: items=${items.length} total=${total || 'unknown'}`)
        if (items.length < pageSize || (total > 0 && archives.length >= total)) {
            break
        }
    }

    return archives
}

async function seedBilibiliSentState(targetId: string, article: ArticleWithId, archive: BilibiliArchive) {
    const key = articleKey(article)
    const outboundId = articleOutboundKey(targetId, article)
    const route = `system:bilibili-recovery:${targetId}`
    const providerSummary = {
        ok: true,
        mode: 'bilibili_recovery_reconcile',
        bvid: archive.bvid || null,
        aid: archive.aid || null,
        source: archive.source || null,
        title: archive.title || null,
    }
    const outbound = await DB.OutboundMessage.claim({
        idempotency_key: outboundId,
        route_key: route,
        target_id: targetId,
        target_platform: BILIBILI_TARGET_PLATFORM,
        task_kind: 'article',
        article_key: key,
        payload_hash: payloadHash({
            routeKey: route,
            targetId,
            taskKind: 'article',
            articleKeys: [key],
            extra: providerSummary,
        }),
    })
    if (outbound.claimed) {
        await DB.OutboundMessage.markSent(outboundId, providerSummary)
    }
    await DB.ForwardBy.save(article.id, article.platform, targetId, 'article')
    return outbound.claimed
}

async function consumeMarker(markerPath: string, result: ReconcileResult, log?: Logger) {
    const donePath = `${markerPath}.bilibili-reconciled`
    fs.writeFileSync(donePath, JSON.stringify({ ...result, completed_at: new Date().toISOString() }, null, 2) + '\n')
    fs.rmSync(markerPath, { force: true })
    log?.info(`Consumed DB recovery marker after Bilibili reconciliation: ${donePath}`)
}

async function reconcileBilibiliSubmissionsAfterDbRecovery(config: AppConfig, log?: Logger): Promise<ReconcileResult | null> {
    if (!normalizeBool(process.env.IDOL_BBQ_BILI_RECOVERY_RECONCILE, true)) {
        return null
    }

    const markerPath = process.env.IDOL_BBQ_DB_RECOVERY_MARKER || DEFAULT_MARKER_PATH
    if (!fs.existsSync(markerPath)) {
        return null
    }

    const marker = readMarker(markerPath)
    const targets = resolveBilibiliTargets(config)
    const result: ReconcileResult = {
        markerPath,
        marker,
        archives: 0,
        matched: 0,
        seeded: 0,
        skippedNoSource: 0,
        skippedNoArticle: 0,
        targets: targets.length,
    }

    if (targets.length === 0) {
        log?.warn('DB recovery marker present but no Bilibili video-upload target is configured; leaving marker for retry')
        return result
    }

    const bySource = new Map<string, BilibiliArchive>()
    for (const target of targets) {
        const archives = await fetchBilibiliArchives(target.cookieHeader, log)
        result.archives += archives.length
        for (const archive of archives) {
            const source = normalizeUrl(archive.source)
            if (!source) {
                result.skippedNoSource += 1
                continue
            }
            bySource.set(source, archive)
        }
    }

    for (const [source, archive] of bySource) {
        const article = await DB.Article.findByUrl(source)
        if (!article) {
            result.skippedNoArticle += 1
            continue
        }
        result.matched += 1
        for (const target of targets) {
            await seedBilibiliSentState(target.id, article, archive)
            result.seeded += 1
        }
    }

    await consumeMarker(markerPath, result, log)
    log?.warn(
        `Bilibili recovery reconciliation completed: archives=${result.archives} matched=${result.matched} seeded=${result.seeded} skipped_no_source=${result.skippedNoSource} skipped_no_article=${result.skippedNoArticle}`,
    )
    return result
}

export { reconcileBilibiliSubmissionsAfterDbRecovery }
export type { ReconcileResult as BilibiliRecoveryReconcileResult }
