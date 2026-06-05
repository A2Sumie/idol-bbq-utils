import type { NotificationSignalRecord } from './notification-signal-service'

type NotificationSignalTaskLike = {
    status?: string | null
    payload?: unknown
    created_at?: number | null
    updated_at?: number | null
    finished_at?: number | null
}

type NotificationSignalSummaryOptions = {
    now?: number
    limit?: number
}

type NotificationSignalSummary = {
    generated_at: string
    sample: {
        limit?: number
        task_count: number
        parsed_record_count: number
        malformed_payload_count: number
        oldest_task_created_at: number | null
        newest_task_created_at: number | null
    }
    counts: {
        unmatched_signal_count: number
        raw_text_field_count: number
        would_trigger_crawlers_count: number
        unique_event_key_count: number
        duplicate_event_key_count: number
    }
    freshness: {
        oldest_received_at: number | null
        latest_received_at: number | null
        latest_received_lag_seconds: number | null
    }
    status_counts: Record<string, number>
    platform_counts: Record<string, number>
    match_reason_counts: Record<string, number>
    matched_crawlers: Array<{
        crawler_id: string
        crawler_name: string
        count: number
    }>
    diagnostic_codes: Array<string>
}

function increment(map: Record<string, number>, key: string | undefined | null, amount = 1) {
    const normalized = String(key || 'unknown').trim() || 'unknown'
    map[normalized] = (map[normalized] || 0) + amount
}

function normalizeTimestamp(value: unknown): number | null {
    const numeric = Number(value)
    if (!Number.isFinite(numeric) || numeric <= 0) {
        return null
    }
    return Math.floor(numeric > 1_000_000_000_000 ? numeric / 1000 : numeric)
}

function parsePayload(payload: unknown): NotificationSignalRecord | null {
    if (typeof payload === 'string') {
        try {
            payload = JSON.parse(payload)
        } catch {
            return null
        }
    }
    if (!payload || typeof payload !== 'object') {
        return null
    }
    const record = payload as Partial<NotificationSignalRecord>
    if (record.schema_version !== 1 || !record.platform || !record.event_key) {
        return null
    }
    return record as NotificationSignalRecord
}

function hasRawNotificationText(record: NotificationSignalRecord) {
    const notification = record.notification as Record<string, unknown> | undefined
    if (!notification || typeof notification !== 'object') {
        return false
    }
    return ['title', 'body', 'text'].some((key) => typeof notification[key] === 'string' && notification[key])
}

function diagnosticCodes(summary: Omit<NotificationSignalSummary, 'diagnostic_codes'>) {
    const codes = [] as Array<string>
    if (summary.sample.task_count === 0) {
        codes.push('notification_signal_no_samples')
    }
    if (summary.sample.malformed_payload_count > 0) {
        codes.push('notification_signal_malformed_payload')
    }
    if (summary.counts.raw_text_field_count > 0) {
        codes.push('notification_signal_raw_text_fields_present')
    }
    if (summary.counts.would_trigger_crawlers_count > 0) {
        codes.push('notification_signal_would_trigger_not_shadow')
    }
    if (summary.counts.unmatched_signal_count > 0) {
        codes.push('notification_signal_unmatched_samples_present')
    }
    if (summary.counts.duplicate_event_key_count > 0) {
        codes.push('notification_signal_duplicate_event_keys_present')
    }
    return codes.sort()
}

function buildNotificationSignalSummary(
    tasks: Array<NotificationSignalTaskLike>,
    options: NotificationSignalSummaryOptions = {},
): NotificationSignalSummary {
    const now = Math.floor(options.now ?? Date.now() / 1000)
    const statusCounts: Record<string, number> = {}
    const platformCounts: Record<string, number> = {}
    const matchReasonCounts: Record<string, number> = {}
    const matchedCrawlerCounts = new Map<string, { crawler_id: string; crawler_name: string; count: number }>()
    const eventKeyCounts = new Map<string, number>()

    let parsedRecordCount = 0
    let malformedPayloadCount = 0
    let unmatchedSignalCount = 0
    let rawTextFieldCount = 0
    let wouldTriggerCrawlersCount = 0
    let oldestTaskCreatedAt: number | null = null
    let newestTaskCreatedAt: number | null = null
    let oldestReceivedAt: number | null = null
    let latestReceivedAt: number | null = null

    for (const task of tasks) {
        increment(statusCounts, task.status)
        const createdAt = normalizeTimestamp(task.created_at)
        if (createdAt !== null) {
            oldestTaskCreatedAt = oldestTaskCreatedAt === null ? createdAt : Math.min(oldestTaskCreatedAt, createdAt)
            newestTaskCreatedAt = newestTaskCreatedAt === null ? createdAt : Math.max(newestTaskCreatedAt, createdAt)
        }

        const record = parsePayload(task.payload)
        if (!record) {
            malformedPayloadCount += 1
            continue
        }
        parsedRecordCount += 1
        increment(platformCounts, record.platform)
        eventKeyCounts.set(record.event_key, (eventKeyCounts.get(record.event_key) || 0) + 1)

        const receivedAt = normalizeTimestamp(record.received_at)
        if (receivedAt !== null) {
            oldestReceivedAt = oldestReceivedAt === null ? receivedAt : Math.min(oldestReceivedAt, receivedAt)
            latestReceivedAt = latestReceivedAt === null ? receivedAt : Math.max(latestReceivedAt, receivedAt)
        }

        const matches = Array.isArray(record.matched_crawlers) ? record.matched_crawlers : []
        if (matches.length === 0) {
            unmatchedSignalCount += 1
        }
        if (record.would_trigger_crawlers !== false) {
            wouldTriggerCrawlersCount += 1
        }
        if (hasRawNotificationText(record)) {
            rawTextFieldCount += 1
        }

        for (const match of matches) {
            increment(matchReasonCounts, match.reason)
            const crawlerId = String(match.crawler_id || 'unknown').trim() || 'unknown'
            const crawlerName = String(match.crawler_name || crawlerId).trim() || crawlerId
            const existing = matchedCrawlerCounts.get(crawlerId)
            if (existing) {
                existing.count += 1
            } else {
                matchedCrawlerCounts.set(crawlerId, {
                    crawler_id: crawlerId,
                    crawler_name: crawlerName,
                    count: 1,
                })
            }
        }
    }

    const duplicateEventKeyCount = Array.from(eventKeyCounts.values()).filter((count) => count > 1).length
    const summaryWithoutDiagnostics = {
        generated_at: new Date(now * 1000).toISOString(),
        sample: {
            ...(options.limit ? { limit: options.limit } : {}),
            task_count: tasks.length,
            parsed_record_count: parsedRecordCount,
            malformed_payload_count: malformedPayloadCount,
            oldest_task_created_at: oldestTaskCreatedAt,
            newest_task_created_at: newestTaskCreatedAt,
        },
        counts: {
            unmatched_signal_count: unmatchedSignalCount,
            raw_text_field_count: rawTextFieldCount,
            would_trigger_crawlers_count: wouldTriggerCrawlersCount,
            unique_event_key_count: eventKeyCounts.size,
            duplicate_event_key_count: duplicateEventKeyCount,
        },
        freshness: {
            oldest_received_at: oldestReceivedAt,
            latest_received_at: latestReceivedAt,
            latest_received_lag_seconds: latestReceivedAt === null ? null : Math.max(0, now - latestReceivedAt),
        },
        status_counts: statusCounts,
        platform_counts: platformCounts,
        match_reason_counts: matchReasonCounts,
        matched_crawlers: Array.from(matchedCrawlerCounts.values()).sort(
            (left, right) => right.count - left.count || left.crawler_id.localeCompare(right.crawler_id),
        ),
    }

    return {
        ...summaryWithoutDiagnostics,
        diagnostic_codes: diagnosticCodes(summaryWithoutDiagnostics),
    }
}

export {
    buildNotificationSignalSummary,
    type NotificationSignalSummary,
    type NotificationSignalSummaryOptions,
    type NotificationSignalTaskLike,
}
