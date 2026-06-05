import { expect, test } from 'bun:test'
import { redactOutboundMessageForApi, redactTargetHealthForApi } from './outbound-state-redaction-service'

test('outbound message API redaction summarizes provider state without identifiers or payload details', () => {
    const redacted = redactOutboundMessageForApi({
        id: 1,
        idempotency_key: 'article:remote-private-target:1:remote-private-article',
        route_key: 'graph:remote-private-crawler:formatter:remote-private-target',
        target_id: 'remote-private-target',
        target_platform: 'QQ',
        task_kind: 'article',
        article_key: '1:remote-private-article',
        synthetic_key: 'remote-private-window',
        payload_hash: 'remote-private-payload-hash',
        status: 'dry_run',
        provider_message_ids: {
            send_mode: 'capture',
            target_id: 'remote-private-target',
            forwarder: 'qq',
            text_count: 1,
            text_length: 37,
            media_count: 1,
            article_key: '1:remote-private-article',
            outbound_key: 'article:remote-private-target:1:remote-private-article',
            capture_result: {
                kind: 'file',
                destination: '/tmp/remote-private-capture.jsonl',
                ok: true,
                status: 200,
                error: 'remote-private-capture-error',
            },
            details: {
                reason: 'remote-private-reason',
                batchKey: 'remote-private-batch',
            },
        },
        segment_results: {
            diagnostic: 'suppressed_payload_drift',
            existing: {
                route_key: 'graph:old:remote-private-target',
                target_id: 'remote-private-target',
                article_key: '1:remote-private-article',
                payload_hash: 'remote-private-old-hash',
                status: 'sent',
            },
            incoming: {
                route_key: 'graph:new:remote-private-target',
                target_id: 'remote-private-target',
                article_key: '1:remote-private-article',
                payload_hash: 'remote-private-new-hash',
            },
            previous_segment_results: [{ message_id: 'remote-private-message-id' }],
        },
        last_error: 'remote-private-send-error',
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.idempotency_key).toBe('[redacted]')
    expect(redacted.route_key).toBe('[redacted]')
    expect(redacted.target_id).toBe('[redacted]')
    expect(redacted.article_key).toBe('[redacted]')
    expect(redacted.synthetic_key).toBe('[redacted]')
    expect(redacted.payload_hash).toBe('[redacted]')
    expect(redacted.target_id_hash).toHaveLength(16)
    expect(redacted.provider_message_ids).toMatchObject({
        redacted_value: true,
        value_type: 'object',
        send_mode: 'capture',
        text_count: 1,
        text_length: 37,
        media_count: 1,
        target_id_present: true,
        article_key_present: true,
        outbound_key_present: true,
        capture_result: {
            redacted_value: true,
            kind: 'file',
            ok: true,
            status: 200,
            destination_present: true,
            error_present: true,
        },
    })
    expect(redacted.segment_results).toMatchObject({
        redacted_value: true,
        value_type: 'object',
        diagnostic_present: true,
        existing: {
            redacted_value: true,
            target_id_present: true,
            article_key_present: true,
            payload_hash_present: true,
            status: 'sent',
        },
        incoming: {
            redacted_value: true,
            target_id_present: true,
            article_key_present: true,
            payload_hash_present: true,
        },
    })
    expect(redacted.last_error).toMatchObject({
        redacted_text: true,
        text_present: true,
        text_type: 'string',
        text_length: 'remote-private-send-error'.length,
    })
    expect(serialized).not.toContain('remote-private-target')
    expect(serialized).not.toContain('remote-private-article')
    expect(serialized).not.toContain('remote-private-window')
    expect(serialized).not.toContain('remote-private-payload-hash')
    expect(serialized).not.toContain('remote-private-capture')
    expect(serialized).not.toContain('remote-private-batch')
    expect(serialized).not.toContain('remote-private-message-id')
    expect(serialized).not.toContain('remote-private-send-error')
})

test('target health API redaction summarizes details without target ids or disabled reasons', () => {
    const redacted = redactTargetHealthForApi({
        target_id: 'remote-private-target',
        provider: 'QQ',
        status: 'error',
        last_send_status: 'failed',
        last_provider_code: '500',
        disabled_reason: 'remote-private-disabled-reason',
        details: {
            route_key: 'graph:remote-private-target',
            article_key: '1:remote-private-article',
            target_id: 'remote-private-target',
            status: 'failed',
            data: {
                code: 500,
                message: 'remote-private-provider-message',
            },
        },
    })
    const serialized = JSON.stringify(redacted)

    expect(redacted.target_id).toBe('[redacted]')
    expect(redacted.target_id_hash).toHaveLength(16)
    expect(redacted.disabled_reason).toMatchObject({
        redacted_text: true,
        text_present: true,
        text_type: 'string',
        text_length: 'remote-private-disabled-reason'.length,
    })
    expect(redacted.details).toMatchObject({
        redacted_value: true,
        value_type: 'object',
        status: 'failed',
        provider_code: 500,
        target_id_present: true,
        article_key_present: true,
        data: {
            redacted_value: true,
            value_type: 'object',
            provider_code: 500,
        },
    })
    expect(serialized).not.toContain('remote-private-target')
    expect(serialized).not.toContain('remote-private-article')
    expect(serialized).not.toContain('remote-private-disabled-reason')
    expect(serialized).not.toContain('remote-private-provider-message')
})
