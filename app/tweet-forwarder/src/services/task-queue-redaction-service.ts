import DB from '@/db'
import type { NotificationSignalRecord } from './notification-signal-service'
import { redactSecrets } from './redaction-service'

type TaskQueueEntryLike = Record<string, unknown> & {
    type?: string | null
    payload?: unknown
    source_ref?: string | null
    idempotency_key?: string | null
    last_error?: string | null
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

function redactTaskQueueEntryForApi<T extends TaskQueueEntryLike>(task: T): T {
    const redacted = redactSecrets(task) as T
    if (task.type !== DB.TaskQueue.TYPE.NotificationSignal) {
        return redacted
    }

    return {
        ...redacted,
        payload: publicNotificationSignalPayload(task.payload),
        source_ref: task.source_ref ? '[redacted]' : task.source_ref,
        idempotency_key: task.idempotency_key ? '[redacted]' : task.idempotency_key,
        last_error: task.last_error ? '[redacted]' : task.last_error,
    }
}

function redactTaskQueueEntriesForApi<T extends TaskQueueEntryLike>(tasks: Array<T>): Array<T> {
    return tasks.map((task) => redactTaskQueueEntryForApi(task))
}

export { redactTaskQueueEntriesForApi, redactTaskQueueEntryForApi }
