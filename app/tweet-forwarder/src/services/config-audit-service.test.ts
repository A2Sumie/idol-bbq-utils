import { expect, test } from 'bun:test'
import { buildConfigAudit } from './config-audit-service'
import { buildRuntimeManifest } from './runtime-manifest-service'

function makeConfig(summaryCard: Record<string, unknown> = {}) {
    return {
        api: {
            secret: 'test-secret',
        },
        crawlers: [
            {
                id: 'crawler-x',
                name: 'crawler x',
                cfg_crawler: {
                    cookie_file: '/tmp/private-cookies.txt',
                },
            },
        ],
        formatters: [
            {
                id: 'formatter-a',
                name: 'formatter a',
            },
        ],
        forward_targets: [
            {
                id: 'target-qq',
                platform: 'qq',
                cfg_platform: {
                    token: 'bot-token',
                    group_id: '123',
                    summary_card: {
                        enabled: true,
                        interval_seconds: 7200,
                        send_first_immediately: true,
                        send_first_native: true,
                        media_realtime: true,
                        media_duplicate_limit: 2,
                        flush_on_threshold: false,
                        align_to_hour: true,
                        flush_delay_seconds: 300,
                        ...summaryCard,
                    },
                },
            },
        ],
        connections: {
            'crawler-formatter': {
                'crawler-x': ['formatter-a'],
            },
            'formatter-target': {
                'formatter-a': ['target-qq'],
            },
        },
    } as any
}

test('buildConfigAudit exposes policy evidence without secret values', () => {
    const audit = buildConfigAudit(makeConfig())
    const serialized = JSON.stringify(audit)

    expect(audit.redacted_config_hash).toMatch(/^[a-f0-9]{64}$/)
    expect(audit.policy_hash).toMatch(/^[a-f0-9]{64}$/)
    expect(audit.secret_fields.paths).toEqual([
        'api.secret',
        'crawlers[0].cfg_crawler.cookie_file',
        'forward_targets[0].cfg_platform.token',
    ])
    expect(audit.route_graph.counts.routes).toBe(1)
    expect(audit.route_graph.summary_card_routes[0]?.policy.summary_card).toMatchObject({
        interval_seconds: 7200,
        send_first_native: true,
        media_realtime: true,
        media_duplicate_limit: 2,
        flush_on_threshold: false,
        flush_delay_seconds: 300,
        window_alignment: 'hour',
    })

    expect(serialized).not.toContain('test-secret')
    expect(serialized).not.toContain('bot-token')
    expect(serialized).not.toContain('/tmp/private-cookies.txt')
    expect(serialized).toContain('api.secret')
})

test('buildConfigAudit policy hash is stable and changes with route policy', () => {
    const audit = buildConfigAudit(makeConfig())
    const sameAudit = buildConfigAudit(makeConfig())
    const changedAudit = buildConfigAudit(makeConfig({ flush_on_threshold: true }))

    expect(sameAudit.policy_hash).toBe(audit.policy_hash)
    expect(changedAudit.policy_hash).not.toBe(audit.policy_hash)
})

test('buildRuntimeManifest uses no-secret config hashes and route counts', () => {
    const manifest = buildRuntimeManifest('/tmp/missing-idol-bbq-config.yaml', makeConfig())
    const serialized = JSON.stringify(manifest)

    expect(manifest.config.hash).toBe(manifest.config.redacted_hash)
    expect(manifest.config.policy_hash).toMatch(/^[a-f0-9]{64}$/)
    expect(manifest.config.secret_field_count).toBe(3)
    expect(manifest.config.raw_file_hash_present).toBe(false)
    expect(manifest.config.route_graph_counts).toMatchObject({
        routes: 1,
        errors: 0,
    })
    expect(serialized).not.toContain('test-secret')
    expect(serialized).not.toContain('bot-token')
    expect(serialized).not.toContain('/tmp/private-cookies.txt')
})
