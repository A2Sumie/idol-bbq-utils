import { expect, test } from 'bun:test'
import { normalizeLiveCapturePlanInput } from './live-capture-plan-service'

test('normalizes a structured TikTok live capture plan for later scheduling', () => {
    const plan = normalizeLiveCapturePlanInput({
        schema_version: 1,
        target: {
            platform: 'tiktok',
            handle: '@mao_asaoka',
        },
        event: {
            starts_at: '2026-08-02T21:45:00+09:00',
            timezone: 'Asia/Tokyo',
            performer: '麻丘真央',
        },
        source: {
            kind: 'llm_extraction',
            ref: 'social-post-1',
            observed_at: '2026-08-02T20:00:00+09:00',
        },
        extraction: {
            confidence: 0.93,
            model: 'test-model',
            uncertainties: ['end time unknown'],
        },
        tags: ['22/7', 'mao', 'mao'],
    })

    expect(plan).toMatchObject({
        schema_version: 1,
        target: {
            platform: 'tiktok',
            handle: 'mao_asaoka',
        },
        event: {
            starts_at: 1785674700,
            starts_at_iso: '2026-08-02T12:45:00.000Z',
            timezone: 'Asia/Tokyo',
            performer: '麻丘真央',
        },
        window: {
            before_minutes: 10,
            after_minutes: 240,
            opens_at: 1785674100,
            closes_at: 1785689100,
        },
        capture: {
            poll_seconds: 15,
            first_byte_timeout_seconds: 30,
            quality_order: ['origin_rtmp', 'hd_flv', 'hd_hls'],
            upload: false,
        },
        source: {
            kind: 'llm_extraction',
            ref: 'social-post-1',
            observed_at: 1785668400,
        },
        extraction: {
            confidence: 0.93,
            model: 'test-model',
            uncertainties: ['end time unknown'],
        },
        tags: ['22/7', 'mao'],
    })
})

test('requires an explicit timezone offset in extracted start times', () => {
    expect(() =>
        normalizeLiveCapturePlanInput({
            target: { platform: 'tiktok', handle: 'mao_asaoka' },
            event: { starts_at: '2026-08-02T21:45:00', timezone: 'Asia/Tokyo' },
        }),
    ).toThrow('event.starts_at must include a UTC offset or Z')
})

test('rejects timestamps beyond the task queue Int range before persistence', () => {
    expect(() =>
        normalizeLiveCapturePlanInput({
            target: { platform: 'tiktok', handle: 'mao_asaoka' },
            event: { starts_at: '2099-01-01T00:00:00+09:00' },
        }),
    ).toThrow('event.starts_at must be a Unix timestamp or ISO-8601 timestamp with timezone offset no later than 2038-01-19T03:14:07Z')
})

test('rejects unknown fields so LLM extraction mistakes are visible', () => {
    expect(() =>
        normalizeLiveCapturePlanInput({
            target: { platform: 'tiktok', handel: 'mao_asaoka' },
            event: { starts_at: '2026-08-02T21:45:00+09:00' },
        }),
    ).toThrow('target contains unknown fields: handel')
})

test('rejects upload requests because plans are non-executable', () => {
    expect(() =>
        normalizeLiveCapturePlanInput({
            target: { platform: 'tiktok', handle: 'mao_asaoka' },
            event: { starts_at: '2026-08-02T21:45:00+09:00' },
            capture: { upload: true },
        }),
    ).toThrow('capture.upload must be false')
})
