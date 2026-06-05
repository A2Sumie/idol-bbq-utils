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
        result_summary: 'private notification result summary',
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
    expect(redacted.result_summary).toEqual({
        redacted_summary: true,
        summary_present: true,
        summary_type: 'string',
        summary_length: 'private notification result summary'.length,
    })
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
    expect(serialized).not.toContain('private notification result summary')
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

test('task queue API redaction summarizes aggregate payloads without prompt or api keys', () => {
    const redacted = redactTaskQueueEntryForApi({
        id: 3,
        type: DB.TaskQueue.TYPE.AggregateDaily,
        status: 'pending',
        source_ref: '1:private-member',
        idempotency_key: 'private-aggregate-idempotency-key',
        last_error: 'private aggregate error',
        payload: {
            platform: 1,
            u_id: 'private-member',
            start: 1_800_000_000,
            end: 1_800_003_600,
            target_ids: ['private-target-a', 'private-target-b'],
            processorConfig: {
                provider: 'Google',
                api_key: 'private-model-api-key',
            },
            processorId: 'private-processor-id',
            prompt: 'private daily summary prompt',
        },
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.source_ref).toBe('[redacted]')
    expect(redacted.idempotency_key).toBe('[redacted]')
    expect(redacted.last_error).toBe('[redacted]')
    expect(redacted.payload).toMatchObject({
        redacted_payload: true,
        platform: 1,
        identity_present: true,
        start: 1_800_000_000,
        end: 1_800_003_600,
        target_count: 2,
        processor_id_present: true,
        processor_config_present: true,
        processor_config_provider_present: true,
        processor_config_api_key_present: true,
        prompt_present: true,
        prompt_length: 'private daily summary prompt'.length,
    })
    expect(serialized).not.toContain('private-member')
    expect(serialized).not.toContain('private-target-a')
    expect(serialized).not.toContain('private-processor-id')
    expect(serialized).not.toContain('private-model-api-key')
    expect(serialized).not.toContain('private daily summary prompt')
})

test('task queue API redaction summarizes simulated articles without text or urls', () => {
    const redacted = redactTaskQueueEntryForApi({
        id: 4,
        type: DB.TaskQueue.TYPE.ArticleSimulate,
        status: 'completed',
        source_ref: '1:private-article',
        payload: {
            platform: 1,
            a_id: 'private-article',
            u_id: 'private-member',
            username: 'private username',
            content: 'private simulated article body',
            url: 'https://example.test/private-article',
            mediaUrls: ['https://example.test/private-media.jpg'],
            media: [{ type: 'photo', url: 'https://example.test/private-media-2.jpg' }],
            processWithCrawler: true,
            forwardAfterSave: false,
            crawlerName: 'private crawler',
            processorId: 'private processor',
            simulated_a_id: 'private-simulated-id',
        },
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.payload).toMatchObject({
        redacted_payload: true,
        platform: 1,
        article_id_present: true,
        user_id_present: true,
        username_present: true,
        content_present: true,
        content_length: 'private simulated article body'.length,
        url_present: true,
        media_count: 1,
        media_url_count: 1,
        process_with_crawler: true,
        forward_after_save: false,
        crawler_name_present: true,
        processor_id_present: true,
        simulated_article_id_present: true,
    })
    expect(serialized).not.toContain('private simulated article body')
    expect(serialized).not.toContain('private-member')
    expect(serialized).not.toContain('private-article')
    expect(serialized).not.toContain('private-media')
    expect(serialized).not.toContain('private crawler')
    expect(serialized).not.toContain('private processor')
})

test('task queue API redaction summarizes processor task payloads without text or schedule keys', () => {
    const redacted = redactTaskQueueEntryForApi({
        id: 5,
        type: DB.TaskQueue.TYPE.ProcessorRun,
        status: 'failed',
        source_ref: 'manual:text',
        payload: {
            processorId: 'private processor',
            action: 'plan',
            platform: 'x',
            a_id: 'private-article',
            u_id: 'private-member',
            start: 1,
            end: 2,
            text: 'private processor input text',
            scheduleUrl: 'https://scheduler.example/private',
            scheduleApiKey: 'private-schedule-api-key',
            resultKey: 'plans',
        },
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.payload).toMatchObject({
        redacted_payload: true,
        processor_id_present: true,
        action: 'plan',
        platform: 'x',
        article_id_present: true,
        user_id_present: true,
        time_range_present: true,
        text_present: true,
        text_length: 'private processor input text'.length,
        schedule_url_present: true,
        schedule_api_key_present: true,
        result_key_present: true,
    })
    expect(serialized).not.toContain('private processor')
    expect(serialized).not.toContain('private processor input text')
    expect(serialized).not.toContain('private-schedule-api-key')
    expect(serialized).not.toContain('scheduler.example')
    expect(serialized).not.toContain('private-member')
    expect(serialized).not.toContain('private-article')
})

test('task queue API redaction keeps unexpected scalar fields opaque', () => {
    const redacted = redactTaskQueueEntryForApi({
        id: 6,
        type: DB.TaskQueue.TYPE.ArticleSimulate,
        status: 'completed',
        result_summary: 'private summary with article identity',
        payload: {
            platform: {
                nested: 'private platform object',
            },
            content: 'private body',
        },
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.payload).toMatchObject({
        redacted_payload: true,
        platform: null,
        content_present: true,
        content_length: 'private body'.length,
    })
    expect(redacted.result_summary).toMatchObject({
        redacted_summary: true,
        summary_present: true,
        summary_type: 'string',
        summary_length: 'private summary with article identity'.length,
    })
    expect(serialized).not.toContain('private platform object')
    expect(serialized).not.toContain('private body')
    expect(serialized).not.toContain('private summary with article identity')
})
