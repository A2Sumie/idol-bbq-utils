import { expect, test } from 'bun:test'
import { redactProcessorRunForApi } from './processor-run-redaction-service'

test('processor run API redaction summarizes input and output without text or schedule keys', () => {
    const redacted = redactProcessorRunForApi({
        id: 1,
        processor_id: 'processor-a',
        action: 'plan',
        source_type: 'text',
        source_ref: 'manual:private-source',
        status: 'completed',
        input: {
            request: {
                processorId: 'private processor',
                action: 'plan',
                platform: 'x',
                a_id: 'private-article',
                u_id: 'private-member',
                start: 1,
                end: 2,
                text: 'private request text',
                scheduleUrl: 'https://scheduler.example/private',
                scheduleApiKey: 'private-schedule-api-key',
                resultKey: 'plans',
            },
            text: 'private processor input text',
        },
        output: {
            raw: 'private raw LLM output',
            parsed: {
                title: 'private parsed plan',
            },
            selected: {
                payload: 'private selected payload',
            },
            result_key: 'plans',
            schedules: [
                {
                    ok: true,
                    body: {
                        secret: 'private schedule response',
                    },
                },
            ],
        },
        error: 'private processor error',
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.source_ref).toBe('[redacted]')
    expect(redacted.error).toBe('[redacted]')
    expect(redacted.input).toMatchObject({
        redacted_input: true,
        request: {
            has_request: true,
            processor_id_present: true,
            action: 'plan',
            platform: 'x',
            article_id_present: true,
            user_id_present: true,
            time_range_present: true,
            text_present: true,
            text_length: 'private request text'.length,
            schedule_url_present: true,
            schedule_api_key_present: true,
            result_key_present: true,
        },
        text_present: true,
        text_length: 'private processor input text'.length,
    })
    expect(redacted.output).toMatchObject({
        redacted_output: true,
        raw_present: true,
        raw_length: 'private raw LLM output'.length,
        parsed_present: true,
        parsed_type: 'object',
        selected_present: true,
        selected_type: 'object',
        result_key_present: true,
        schedule_count: 1,
    })
    expect(serialized).not.toContain('manual:private-source')
    expect(serialized).not.toContain('private processor')
    expect(serialized).not.toContain('private request text')
    expect(serialized).not.toContain('private processor input text')
    expect(serialized).not.toContain('private raw LLM output')
    expect(serialized).not.toContain('private parsed plan')
    expect(serialized).not.toContain('private selected payload')
    expect(serialized).not.toContain('private-schedule-api-key')
    expect(serialized).not.toContain('scheduler.example')
    expect(serialized).not.toContain('private schedule response')
    expect(serialized).not.toContain('private processor error')
})

test('processor run API redaction keeps unexpected scalar fields opaque', () => {
    const redacted = redactProcessorRunForApi({
        id: 2,
        action: 'plan',
        input: {
            request: {
                platform: {
                    nested: 'private platform object',
                },
                text: 'private request text',
            },
        },
        output: null,
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.input).toMatchObject({
        redacted_input: true,
        request: {
            has_request: true,
            platform: null,
            text_present: true,
            text_length: 'private request text'.length,
        },
    })
    expect(serialized).not.toContain('private platform object')
    expect(serialized).not.toContain('private request text')
})
