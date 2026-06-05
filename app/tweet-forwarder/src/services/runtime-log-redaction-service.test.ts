import { expect, test } from 'bun:test'
import {
    publicRuntimeLogFileMetadata,
    redactRuntimeLogLineForApi,
    redactRuntimeLogLinesForApi,
} from './runtime-log-redaction-service'

test('runtime log redaction removes credentials urls source refs and host paths', () => {
    const line =
        'Authorization: Bearer private-token api_key=private-api-key source_ref=notification:instagram:private-member target_id=remote-private-target url=https://example.test/private /tmp/private/file.log'
    const redacted = redactRuntimeLogLineForApi(line)

    expect(redacted).toContain('Authorization: Bearer [redacted]')
    expect(redacted).toContain('api_key=[redacted]')
    expect(redacted).toContain('source_ref=[redacted]')
    expect(redacted).toContain('target_id=[redacted]')
    expect(redacted).toContain('[redacted-url]')
    expect(redacted).toContain('[redacted-path]')
    expect(redacted).not.toContain('private-token')
    expect(redacted).not.toContain('private-api-key')
    expect(redacted).not.toContain('private-member')
    expect(redacted).not.toContain('remote-private-target')
    expect(redacted).not.toContain('example.test/private')
    expect(redacted).not.toContain('/tmp/private/file.log')
})

test('runtime log redaction handles quoted json-like secret assignments', () => {
    const redacted = redactRuntimeLogLineForApi(
        '{"cookie":"private-cookie","route_key":"private-route","message_id":"private-message","ok":true}',
    )

    expect(redacted).toContain('"cookie": "[redacted]"')
    expect(redacted).toContain('"route_key": "[redacted]"')
    expect(redacted).toContain('"message_id": "[redacted]"')
    expect(redacted).not.toContain('private-cookie')
    expect(redacted).not.toContain('private-route')
    expect(redacted).not.toContain('private-message')
})

test('runtime log payload exposes only public log file metadata', () => {
    expect(publicRuntimeLogFileMetadata('/Users/zou/private/runtime.log')).toEqual({
        file: 'runtime.log',
        file_path: '[redacted]',
        redacted: true,
    })
    expect(redactRuntimeLogLinesForApi(['token=private-token'])).toEqual(['token=[redacted]'])
})
