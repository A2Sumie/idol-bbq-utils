import { expect, test } from 'bun:test'
import DB from '@/db'
import { redactTaskQueueEntryForApi } from './task-queue-redaction-service'

test('task queue API redaction hides notification signal identity fields', () => {
    const redacted = redactTaskQueueEntryForApi({
        id: 1,
        type: DB.TaskQueue.TYPE.NotificationSignal,
        status: 'completed',
        source_ref: 'notification:instagram:private-member',
        idempotency_key: 'private-idempotency-key',
        last_error: 'private last error',
        payload: {
            schema_version: 1,
            mode: 'shadow',
            platform: 'instagram',
            event_key: 'private-event-key',
            source_ref: 'notification:instagram:private-member',
            received_at: 1_800_000_000,
            notification: {
                type: 'post',
                notification_id: 'private-notification-id',
                post_id: 'private-post-id',
                url: 'https://www.instagram.com/private-member/p/private-post-id/',
                source_user_id: 'private-source-user',
                username: 'private-member',
                title_hash: 'private-title-hash',
                title_length: 12,
                body_hash: 'private-body-hash',
                body_length: 34,
            },
            matched_crawlers: [
                {
                    crawler_id: 'ig-a',
                    crawler_name: 'Instagram A',
                    reason: 'identity',
                },
            ],
            would_trigger_crawlers: false,
        },
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.source_ref).toBe('[redacted]')
    expect(redacted.idempotency_key).toBe('[redacted]')
    expect(redacted.last_error).toBe('[redacted]')
    expect(redacted.payload).toMatchObject({
        schema_version: 1,
        mode: 'shadow',
        platform: 'instagram',
        received_at: 1_800_000_000,
        event_key_present: true,
        source_ref_present: true,
        notification: {
            has_notification: true,
            type: 'post',
            has_url: true,
            has_notification_id: true,
            has_post_id: true,
            has_source_user_id: true,
            has_username: true,
            title_length: 12,
            body_length: 34,
        },
        matched_crawler_count: 1,
        matched_crawlers: [
            {
                crawler_id: 'ig-a',
                crawler_name: 'Instagram A',
                reason: 'identity',
            },
        ],
        would_trigger_crawlers: false,
    })
    expect(serialized).not.toContain('private-member')
    expect(serialized).not.toContain('private-event-key')
    expect(serialized).not.toContain('private-notification-id')
    expect(serialized).not.toContain('private-post-id')
    expect(serialized).not.toContain('private-source-user')
    expect(serialized).not.toContain('private-title-hash')
    expect(serialized).not.toContain('private-body-hash')
})

test('task queue API redaction keeps malformed notification payloads opaque', () => {
    const redacted = redactTaskQueueEntryForApi({
        id: 2,
        type: DB.TaskQueue.TYPE.NotificationSignal,
        status: 'failed',
        source_ref: 'notification:x:private-member',
        payload: '{"event_key":"private-event-key"',
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.payload).toEqual({
        schema_version: null,
        malformed_payload: true,
    })
    expect(serialized).not.toContain('private-member')
    expect(serialized).not.toContain('private-event-key')
})
