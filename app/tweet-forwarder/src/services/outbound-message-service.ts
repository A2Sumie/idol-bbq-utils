import crypto from 'crypto'
import type { ArticleWithId } from '@/db'

type MediaLike = {
    media_type?: string
    path?: string
    sourceArticleId?: string
    sourceUserId?: string
}

const OUTBOUND_STATUS = {
    Planned: 'planned',
    Sending: 'sending',
    Queued: 'queued',
    DryRun: 'dry_run',
    Skipped: 'skipped',
    Sent: 'sent',
    Partial: 'partial',
    Failed: 'failed',
    FailedAfterPartial: 'failed_after_partial',
} as const

type OutboundMessageStatus = (typeof OUTBOUND_STATUS)[keyof typeof OUTBOUND_STATUS]

const OUTBOUND_STALE_RETRYABLE_STATUSES = new Set<string>([
    OUTBOUND_STATUS.Planned,
    OUTBOUND_STATUS.Sending,
    OUTBOUND_STATUS.Queued,
    OUTBOUND_STATUS.DryRun,
])
const OUTBOUND_IN_PROGRESS_STATUSES = new Set<string>([OUTBOUND_STATUS.Planned, OUTBOUND_STATUS.Sending])
const OUTBOUND_VISIBLE_COMPLETION_STATUSES = new Set<string>([
    OUTBOUND_STATUS.Sent,
    OUTBOUND_STATUS.Partial,
    OUTBOUND_STATUS.FailedAfterPartial,
])
const OUTBOUND_SUPPRESSED_COMPLETION_STATUSES = new Set<string>([
    ...OUTBOUND_VISIBLE_COMPLETION_STATUSES,
    OUTBOUND_STATUS.Skipped,
])

function isOutboundFailedStatus(status: string | null | undefined) {
    return status === OUTBOUND_STATUS.Failed
}

function isOutboundQueuedStatus(status: string | null | undefined) {
    return status === OUTBOUND_STATUS.Queued
}

function isOutboundDryRunStatus(status: string | null | undefined) {
    return status === OUTBOUND_STATUS.DryRun
}

function isOutboundInProgressStatus(status: string | null | undefined) {
    return OUTBOUND_IN_PROGRESS_STATUSES.has(String(status || ''))
}

function isOutboundStaleRetryableStatus(status: string | null | undefined) {
    return OUTBOUND_STALE_RETRYABLE_STATUSES.has(String(status || ''))
}

function isOutboundVisibleCompletionStatus(status: string | null | undefined) {
    return OUTBOUND_VISIBLE_COMPLETION_STATUSES.has(String(status || ''))
}

function isOutboundSuppressedCompletionStatus(status: string | null | undefined) {
    return OUTBOUND_SUPPRESSED_COMPLETION_STATUSES.has(String(status || ''))
}

function stableSerialize(value: unknown): string {
    if (value === null || typeof value !== 'object') {
        return JSON.stringify(value)
    }
    if (Array.isArray(value)) {
        return `[${value.map((item) => stableSerialize(item)).join(',')}]`
    }
    const record = value as Record<string, unknown>
    return `{${Object.keys(record)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${stableSerialize(record[key])}`)
        .join(',')}}`
}

function hashValue(value: unknown) {
    return crypto.createHash('sha256').update(stableSerialize(value)).digest('hex')
}

function compactHash(value: unknown, length = 16) {
    return hashValue(value).slice(0, length)
}

function articleKey(article: Pick<ArticleWithId, 'platform' | 'a_id'>) {
    return `${article.platform}:${article.a_id}`
}

function articleRowKey(article: Pick<ArticleWithId, 'platform' | 'id'>) {
    return `${article.platform}:${article.id}`
}

function normalizeRoutePart(value?: string | null) {
    return String(value || 'unknown')
        .trim()
        .replace(/[^a-zA-Z0-9_.:-]+/g, '_')
        .slice(0, 80)
}

function routeKey(parts: {
    source: 'graph' | 'inline' | 'manual' | 'batch' | 'system'
    crawlerId?: string | null
    formatterId?: string | null
    targetId?: string | null
    extra?: string | null
}) {
    const raw = [
        parts.source,
        normalizeRoutePart(parts.crawlerId),
        normalizeRoutePart(parts.formatterId),
        normalizeRoutePart(parts.targetId),
        normalizeRoutePart(parts.extra),
    ].join(':')
    return `${raw}:${compactHash(raw, 10)}`
}

function targetRouteKey(baseRouteKey: string, targetId: string) {
    return `${baseRouteKey}:target:${normalizeRoutePart(targetId)}`
}

function summarizeMedia(media?: Array<MediaLike>) {
    return (media || []).map((item) => ({
        media_type: item.media_type || '',
        path: item.path || '',
        sourceArticleId: item.sourceArticleId || '',
        sourceUserId: item.sourceUserId || '',
    }))
}

function payloadHash(data: {
    routeKey: string
    targetId: string
    taskKind: string
    text?: string
    articleKeys?: Array<string>
    media?: Array<MediaLike>
    extra?: unknown
}) {
    return hashValue({
        routeKey: data.routeKey,
        targetId: data.targetId,
        taskKind: data.taskKind,
        text: data.text || '',
        articleKeys: [...(data.articleKeys || [])].sort(),
        media: summarizeMedia(data.media),
        extra: data.extra || null,
    })
}

function articleOutboundKey(targetId: string, article: ArticleWithId, options?: { forceKey?: string }) {
    if (options?.forceKey) {
        return `manual:${normalizeRoutePart(targetId)}:${normalizeRoutePart(options.forceKey)}:${articleKey(article)}`
    }
    return `article:${normalizeRoutePart(targetId)}:${articleKey(article)}`
}

function syntheticOutboundKey(targetId: string, taskKind: string, syntheticKey: string) {
    return `${normalizeRoutePart(taskKind)}:${normalizeRoutePart(targetId)}:${compactHash(syntheticKey, 24)}`
}

function summarizeProviderResult(value: unknown): unknown {
    if (Array.isArray(value)) {
        return value.map((item) => summarizeProviderResult(item))
    }
    if (!value || typeof value !== 'object') {
        return value ?? null
    }
    const maybeResponse = value as {
        status?: unknown
        statusText?: unknown
        data?: unknown
        headers?: unknown
    }
    if ('status' in maybeResponse || 'data' in maybeResponse) {
        return {
            status: maybeResponse.status ?? null,
            statusText: maybeResponse.statusText ?? null,
            data: maybeResponse.data ?? null,
        }
    }
    return JSON.parse(JSON.stringify(value))
}

function providerCode(value: unknown): string | null {
    const summarized = summarizeProviderResult(value) as any
    if (Array.isArray(summarized)) {
        return summarized.map((item) => providerCode(item)).find(Boolean) || null
    }
    const data = summarized?.data
    const code = data?.retcode ?? data?.code ?? data?.status ?? summarized?.status
    return code === undefined || code === null ? null : String(code)
}

export {
    articleKey,
    articleOutboundKey,
    articleRowKey,
    hashValue,
    isOutboundFailedStatus,
    isOutboundDryRunStatus,
    isOutboundInProgressStatus,
    isOutboundQueuedStatus,
    isOutboundStaleRetryableStatus,
    isOutboundSuppressedCompletionStatus,
    isOutboundVisibleCompletionStatus,
    OUTBOUND_STATUS,
    payloadHash,
    providerCode,
    routeKey,
    stableSerialize,
    summarizeProviderResult,
    syntheticOutboundKey,
    targetRouteKey,
    type OutboundMessageStatus,
}
