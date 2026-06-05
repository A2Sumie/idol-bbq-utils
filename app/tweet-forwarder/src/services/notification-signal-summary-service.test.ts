import { expect, test } from 'bun:test'
import { buildNotificationSignalSummary } from './notification-signal-summary-service'

test('notification signal summary exposes no-secret observation metrics', () => {
    const summary = buildNotificationSignalSummary(
        [
            {
                status: 'completed',
                created_at: 1000,
                payload: {
                    schema_version: 1,
                    mode: 'shadow',
                    platform: 'instagram',
                    event_key: 'event-a',
                    source_ref: 'notification:instagram:member-a',
                    received_at: 990,
                    notification: {
                        title_hash: 'hash-title',
                        title_length: 12,
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
            },
            {
                status: 'completed',
                created_at: 1010,
                payload: JSON.stringify({
                    schema_version: 1,
                    mode: 'shadow',
                    platform: 'x',
                    event_key: 'event-a',
                    source_ref: 'notification:x:member-b',
                    received_at: 1005,
                    notification: {
                        body: 'private notification body',
                    },
                    matched_crawlers: [],
                    would_trigger_crawlers: true,
                }),
            },
            {
                status: 'failed',
                created_at: 1020,
                payload: '{malformed',
            },
        ],
        { now: 1030, limit: 50 },
    )
    const serialized = JSON.stringify(summary)

    expect(summary.sample).toMatchObject({
        limit: 50,
        task_count: 3,
        parsed_record_count: 2,
        malformed_payload_count: 1,
        oldest_task_created_at: 1000,
        newest_task_created_at: 1020,
    })
    expect(summary.counts).toEqual({
        unmatched_signal_count: 1,
        raw_text_field_count: 1,
        would_trigger_crawlers_count: 1,
        unique_event_key_count: 1,
        duplicate_event_key_count: 1,
    })
    expect(summary.freshness).toEqual({
        oldest_received_at: 990,
        latest_received_at: 1005,
        latest_received_lag_seconds: 25,
    })
    expect(summary.status_counts).toEqual({
        completed: 2,
        failed: 1,
    })
    expect(summary.platform_counts).toEqual({
        instagram: 1,
        x: 1,
    })
    expect(summary.match_reason_counts).toEqual({
        identity: 1,
    })
    expect(summary.matched_crawlers).toEqual([
        {
            crawler_id: 'ig-a',
            crawler_name: 'Instagram A',
            count: 1,
        },
    ])
    expect(summary.diagnostic_codes).toEqual([
        'notification_signal_duplicate_event_keys_present',
        'notification_signal_malformed_payload',
        'notification_signal_raw_text_fields_present',
        'notification_signal_unmatched_samples_present',
        'notification_signal_would_trigger_not_shadow',
    ])
    expect(serialized).not.toContain('private notification body')
    expect(serialized).not.toContain('member-a')
    expect(serialized).not.toContain('member-b')
    expect(serialized).not.toContain('event-a')
    expect(serialized).not.toContain('hash-title')
})

test('notification signal summary diagnoses an empty sample', () => {
    const summary = buildNotificationSignalSummary([], { now: 1_800_000_000 })

    expect(summary).toMatchObject({
        generated_at: '2027-01-15T08:00:00.000Z',
        sample: {
            task_count: 0,
            parsed_record_count: 0,
            malformed_payload_count: 0,
            oldest_task_created_at: null,
            newest_task_created_at: null,
        },
        counts: {
            unmatched_signal_count: 0,
            raw_text_field_count: 0,
            would_trigger_crawlers_count: 0,
            unique_event_key_count: 0,
            duplicate_event_key_count: 0,
        },
        freshness: {
            oldest_received_at: null,
            latest_received_at: null,
            latest_received_lag_seconds: null,
        },
        diagnostic_codes: ['notification_signal_no_samples'],
    })
})
