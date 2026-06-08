import { expect, test } from 'bun:test'
import {
    buildScheduleWebhookHeaders,
    buildScheduleWebhookPayload,
    resolveConfigValue,
    writeSchedulesFromProcessorResult,
} from './processor-schedule-webhook-service'

test('schedule webhook payload maps extracted event time items to live-player schedule shape', () => {
    const payload = buildScheduleWebhookPayload(
        {
            title: '22/7 定期公演',
            event_type: 'live',
            starts_at: '2026-06-15T19:00:00+09:00',
            ends_at: null,
            timezone: 'Asia/Tokyo',
            source_time_text: '6/15(月) 19:00',
            source_url: 'https://example.com/schedule',
            confidence: 0.91,
            needs_review: false,
            notes: 'official schedule text',
        },
        'x:post-1',
        0,
        'test-key',
    )

    expect(payload).toMatchObject({
        title: '22/7 定期公演',
        description: 'official schedule text',
        scheduleType: 'reminder',
        executionTime: '2026-06-15T19:00:00+09:00',
        apiKey: 'test-key',
        payload: {
            schema_version: 1,
            type: 'idol_bbq_time_event_candidate',
            sourceRef: 'x:post-1',
            eventType: 'live',
            startsAt: '2026-06-15T19:00:00+09:00',
            timezone: 'Asia/Tokyo',
            sourceTimeText: '6/15(月) 19:00',
            sourceUrl: 'https://example.com/schedule',
            confidence: 0.91,
            needsReview: false,
        },
    })
    expect(payload?.externalKey).toStartWith('x:post-1:event:')
})

test('schedule webhook writer posts confident items and resolves env indirection', async () => {
    process.env.IDOL_BBQ_TEST_SCHEDULE_URL = 'https://live-player.example/api/webhook/schedule'
    process.env.IDOL_BBQ_TEST_SCHEDULE_KEY = 'resolved-schedule-key'
    process.env.IDOL_BBQ_TEST_SCHEDULE_WAF = 'x-bypass-waf: resolved-waf'
    const calls: any[] = []

    try {
        expect(
            buildScheduleWebhookHeaders({
                scheduleWafBypassHeader: 'env:IDOL_BBQ_TEST_SCHEDULE_WAF',
            }),
        ).toMatchObject({
            'Content-Type': 'application/json',
            'User-Agent': 'N2NJ-Stream-Bot/1.0',
            'x-bypass-waf': 'resolved-waf',
        })

        const results = await writeSchedulesFromProcessorResult(
            {
                items: [
                    {
                        title: 'low confidence',
                        starts_at: '2026-06-15T17:00:00+09:00',
                        confidence: 0.4,
                    },
                    {
                        title: 'SHOWROOM 配信',
                        event_type: 'stream',
                        starts_at: '2026-06-15T20:00:00+09:00',
                        source_time_text: '20:00頃',
                        confidence: 0.88,
                        needs_review: true,
                    },
                ],
            },
            'manual:text',
            {
                scheduleUrl: 'env:IDOL_BBQ_TEST_SCHEDULE_URL',
                scheduleApiKey: 'env:IDOL_BBQ_TEST_SCHEDULE_KEY',
                scheduleWafBypassHeader: 'env:IDOL_BBQ_TEST_SCHEDULE_WAF',
                minConfidence: 0.6,
                fetchImpl: (async (url: string, init: RequestInit) => {
                    calls.push({ url, headers: init.headers, body: JSON.parse(String(init.body)) })
                    return new Response(JSON.stringify({ success: true }), { status: 200 })
                }) as typeof fetch,
            },
        )

        expect(resolveConfigValue('env:IDOL_BBQ_TEST_SCHEDULE_KEY')).toBe('resolved-schedule-key')
        expect(calls).toHaveLength(1)
        expect(calls[0]).toMatchObject({
            url: 'https://live-player.example/api/webhook/schedule',
            headers: {
                'Content-Type': 'application/json',
                'User-Agent': 'N2NJ-Stream-Bot/1.0',
                'x-bypass-waf': 'resolved-waf',
            },
            body: {
                title: 'SHOWROOM 配信',
                scheduleType: 'reminder',
                executionTime: '2026-06-15T20:00:00+09:00',
                apiKey: 'resolved-schedule-key',
            },
        })
        expect(results).toHaveLength(1)
        expect(results[0]).toMatchObject({
            ok: true,
            status: 200,
            title: 'SHOWROOM 配信',
            executionTime: '2026-06-15T20:00:00+09:00',
        })
    } finally {
        delete process.env.IDOL_BBQ_TEST_SCHEDULE_URL
        delete process.env.IDOL_BBQ_TEST_SCHEDULE_KEY
        delete process.env.IDOL_BBQ_TEST_SCHEDULE_WAF
    }
})
