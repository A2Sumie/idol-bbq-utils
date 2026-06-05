import DB from '@/db'
import type { NotificationSignalRecord } from './notification-signal-service'
import { redactSecrets } from './redaction-service'

type TaskQueueEntryLike = Record<string, unknown> & {
    type?: string | null
    payload?: unknown
    source_ref?: string | null
    idempotency_key?: string | null
    last_error?: string | null
    result_summary?: string | null
}

function parseObjectPayload(payload: unknown): Record<string, unknown> | null {
    if (typeof payload === 'string') {
        try {
            payload = JSON.parse(payload)
        } catch {
            return null
        }
    }
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
        return null
    }
    return payload as Record<string, unknown>
}

function payloadType(value: unknown) {
    if (value === null) return 'null'
    if (Array.isArray(value)) return 'array'
    return typeof value
}

function hasValue(value: unknown) {
    return value !== null && value !== undefined && value !== ''
}

function stringLength(value: unknown) {
    return typeof value === 'string' ? value.length : undefined
}

function publicScalar(value: unknown) {
    if (!hasValue(value)) {
        return null
    }
    if (['string', 'number', 'boolean'].includes(typeof value)) {
        return value
    }
    return null
}

function booleanValue(value: unknown) {
    return typeof value === 'boolean' ? value : Boolean(value)
}

function publicTextSummary(value: unknown) {
    if (!hasValue(value)) {
        return value
    }
    return {
        redacted_summary: true,
        summary_present: true,
        summary_type: payloadType(value),
        ...(stringLength(value) !== undefined ? { summary_length: stringLength(value) } : {}),
    }
}

function publicMalformedPayload(payload: unknown) {
    return {
        redacted_payload: true,
        malformed_payload: true,
        payload_type: payloadType(payload),
    }
}

function publicNotificationFields(notification: unknown) {
    if (!notification || typeof notification !== 'object' || Array.isArray(notification)) {
        return {
            has_notification: false,
        }
    }

    const value = notification as Record<string, unknown>
    return {
        has_notification: true,
        ...(typeof value.type === 'string' && value.type ? { type: value.type } : {}),
        has_url: Boolean(value.url),
        has_notification_id: Boolean(value.notification_id),
        has_post_id: Boolean(value.post_id),
        has_source_user_id: Boolean(value.source_user_id),
        has_username: Boolean(value.username),
        ...(typeof value.title_length === 'number' ? { title_length: value.title_length } : {}),
        ...(typeof value.body_length === 'number' ? { body_length: value.body_length } : {}),
        ...(typeof value.text_length === 'number' ? { text_length: value.text_length } : {}),
    }
}

function publicAggregatePayload(payload: unknown) {
    const value = parseObjectPayload(payload)
    if (!value) {
        return publicMalformedPayload(payload)
    }
    const targetIds = Array.isArray(value.target_ids) ? value.target_ids : []
    const processorConfig = parseObjectPayload(value.processorConfig)
    return {
        redacted_payload: true,
        platform: publicScalar(value.platform),
        identity_present: hasValue(value.u_id),
        start: typeof value.start === 'number' ? value.start : null,
        end: typeof value.end === 'number' ? value.end : null,
        target_count: targetIds.length,
        legacy_bot_id_present: hasValue(value.bot_id),
        processor_id_present: hasValue(value.processorId),
        processor_config_present: Boolean(processorConfig),
        processor_config_provider_present: hasValue(processorConfig?.provider),
        processor_config_api_key_present: hasValue(processorConfig?.api_key),
        prompt_present: hasValue(value.prompt),
        ...(typeof value.prompt === 'string' ? { prompt_length: value.prompt.length } : {}),
    }
}

function publicManualCrawlerPayload(payload: unknown) {
    const value = parseObjectPayload(payload)
    if (!value) {
        return publicMalformedPayload(payload)
    }
    return {
        redacted_payload: true,
        crawler_present: hasValue(value.crawler),
        task_id_present: hasValue(value.task_id),
    }
}

function publicArticleSimulatePayload(payload: unknown) {
    const value = parseObjectPayload(payload)
    if (!value) {
        return publicMalformedPayload(payload)
    }
    return {
        redacted_payload: true,
        platform: publicScalar(value.platform),
        article_id_present: hasValue(value.a_id),
        user_id_present: hasValue(value.u_id),
        username_present: hasValue(value.username),
        content_present: hasValue(value.content),
        ...(stringLength(value.content) !== undefined ? { content_length: stringLength(value.content) } : {}),
        url_present: hasValue(value.url),
        created_at_present: hasValue(value.created_at),
        media_count: Array.isArray(value.media) ? value.media.length : 0,
        media_url_count: Array.isArray(value.mediaUrls) ? value.mediaUrls.length : 0,
        process_with_crawler: booleanValue(value.processWithCrawler),
        forward_after_save: booleanValue(value.forwardAfterSave),
        crawler_name_present: hasValue(value.crawlerName),
        processor_id_present: hasValue(value.processorId),
        simulated_article_id_present: hasValue(value.simulated_a_id),
    }
}

function publicArticleReprocessPayload(payload: unknown) {
    const value = parseObjectPayload(payload)
    if (!value) {
        return publicMalformedPayload(payload)
    }
    return {
        redacted_payload: true,
        platform: publicScalar(value.platform),
        row_id_present: hasValue(value.id),
        article_id_present: hasValue(value.a_id),
        processor_id_present: hasValue(value.processorId),
        force: booleanValue(value.force),
    }
}

function publicArticleResendPayload(payload: unknown) {
    const value = parseObjectPayload(payload)
    if (!value) {
        return publicMalformedPayload(payload)
    }
    return {
        redacted_payload: true,
        platform: publicScalar(value.platform),
        row_id_present: hasValue(value.id),
        article_id_present: hasValue(value.a_id),
        crawler_name_present: hasValue(value.crawlerName),
    }
}

function publicProcessorRunTaskPayload(payload: unknown) {
    const value = parseObjectPayload(payload)
    if (!value) {
        return publicMalformedPayload(payload)
    }
    return {
        redacted_payload: true,
        processor_id_present: hasValue(value.processorId),
        action: typeof value.action === 'string' ? value.action : null,
        platform: publicScalar(value.platform),
        row_id_present: hasValue(value.id),
        article_id_present: hasValue(value.a_id),
        user_id_present: hasValue(value.u_id),
        time_range_present: hasValue(value.start) || hasValue(value.end),
        text_present: hasValue(value.text),
        ...(stringLength(value.text) !== undefined ? { text_length: stringLength(value.text) } : {}),
        schedule_url_present: hasValue(value.scheduleUrl),
        schedule_api_key_present: hasValue(value.scheduleApiKey),
        result_key_present: hasValue(value.resultKey),
    }
}

function publicUnknownPayload(payload: unknown) {
    return {
        redacted_payload: true,
        payload_type: payloadType(payload),
    }
}

function publicNotificationSignalPayload(payload: unknown) {
    const record = parseObjectPayload(payload) as Partial<NotificationSignalRecord> | null
    if (!record || record.schema_version !== 1) {
        return {
            schema_version: null,
            malformed_payload: true,
        }
    }

    const matchedCrawlers = Array.isArray(record.matched_crawlers) ? record.matched_crawlers : []
    return {
        schema_version: 1,
        mode: record.mode || 'unknown',
        platform: record.platform || 'unknown',
        received_at: typeof record.received_at === 'number' ? record.received_at : null,
        event_key_present: Boolean(record.event_key),
        source_ref_present: Boolean(record.source_ref),
        notification: publicNotificationFields(record.notification),
        matched_crawler_count: matchedCrawlers.length,
        matched_crawlers: matchedCrawlers.map((match) => ({
            crawler_id: String(match?.crawler_id || 'unknown'),
            crawler_name: String(match?.crawler_name || match?.crawler_id || 'unknown'),
            reason: String(match?.reason || 'unknown'),
        })),
        would_trigger_crawlers: record.would_trigger_crawlers === false ? false : Boolean(record.would_trigger_crawlers),
    }
}

function publicTaskPayload(type: string | null | undefined, payload: unknown) {
    switch (type) {
        case DB.TaskQueue.TYPE.AggregateDaily:
        case DB.TaskQueue.TYPE.AggregateHourly:
            return publicAggregatePayload(payload)
        case DB.TaskQueue.TYPE.ManualCrawlerRun:
            return publicManualCrawlerPayload(payload)
        case DB.TaskQueue.TYPE.NotificationSignal:
            return publicNotificationSignalPayload(payload)
        case DB.TaskQueue.TYPE.ArticleSimulate:
            return publicArticleSimulatePayload(payload)
        case DB.TaskQueue.TYPE.ArticleReprocess:
            return publicArticleReprocessPayload(payload)
        case DB.TaskQueue.TYPE.ArticleResend:
            return publicArticleResendPayload(payload)
        case DB.TaskQueue.TYPE.ProcessorRun:
            return publicProcessorRunTaskPayload(payload)
        default:
            return publicUnknownPayload(payload)
    }
}

function redactTaskQueueEntryForApi<T extends TaskQueueEntryLike>(task: T): T {
    const redacted = redactSecrets(task) as T
    return {
        ...redacted,
        payload: publicTaskPayload(task.type, task.payload),
        source_ref: task.source_ref ? '[redacted]' : task.source_ref,
        idempotency_key: task.idempotency_key ? '[redacted]' : task.idempotency_key,
        last_error: task.last_error ? '[redacted]' : task.last_error,
        result_summary: publicTextSummary(task.result_summary),
    }
}

function redactTaskQueueEntriesForApi<T extends TaskQueueEntryLike>(tasks: Array<T>): Array<T> {
    return tasks.map((task) => redactTaskQueueEntryForApi(task))
}

export { redactTaskQueueEntriesForApi, redactTaskQueueEntryForApi }
