import { expect, test } from 'bun:test'
import { collectSensitiveConfigPaths, redactSecrets } from './redaction-service'

test('redactSecrets treats api key fields as sensitive without hiding result keys', () => {
    const value = {
        api_key: 'private-api-key',
        scheduleApiKey: 'private-schedule-api-key',
        resultKey: 'result.path',
        nested: {
            schedule_api_key: 'private-nested-schedule-key',
            cookie_file: '/tmp/private.cookies.txt',
        },
    }

    const redacted = redactSecrets(value)
    const serialized = JSON.stringify(redacted)

    expect(redacted.api_key).toBe('[redacted]')
    expect(redacted.scheduleApiKey).toBe('[redacted]')
    expect(redacted.resultKey).toBe('result.path')
    expect(redacted.nested.schedule_api_key).toBe('[redacted]')
    expect(redacted.nested.cookie_file).toBe('[redacted]')
    expect(serialized).not.toContain('private-api-key')
    expect(serialized).not.toContain('private-schedule-api-key')
    expect(serialized).not.toContain('private-nested-schedule-key')
    expect(serialized).not.toContain('/tmp/private.cookies.txt')
})

test('collectSensitiveConfigPaths includes api key fields', () => {
    expect(
        collectSensitiveConfigPaths({
            processors: [
                {
                    api_key: 'private-api-key',
                    cfg_processor: {
                        schedule_api_key: 'private-schedule-api-key',
                        result_key: 'result.path',
                    },
                },
            ],
        }),
    ).toEqual(['processors[0].api_key', 'processors[0].cfg_processor.schedule_api_key'])
})
