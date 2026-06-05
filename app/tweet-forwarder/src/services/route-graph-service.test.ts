import { expect, test } from 'bun:test'
import { buildRouteGraph } from './route-graph-service'

test('buildRouteGraph emits stable routes and detects duplicate target fanout', () => {
    const graph = buildRouteGraph({
        crawlers: [
            {
                id: 'crawler-x',
                name: 'crawler x',
            },
        ],
        formatters: [
            {
                id: 'formatter-a',
                name: 'formatter a',
                aggregation: false,
            },
            {
                id: 'formatter-b',
                name: 'formatter b',
                aggregation: true,
            },
        ],
        forward_targets: [
            {
                id: 'target-qq',
                platform: 'qq' as any,
                cfg_platform: {
                    summary_card: true,
                },
            },
        ],
        connections: {
            'crawler-formatter': {
                'crawler-x': ['formatter-a', 'formatter-b'],
            },
            'formatter-target': {
                'formatter-a': ['target-qq'],
                'formatter-b': ['target-qq'],
            },
        },
    } as any)

    expect(graph.counts.routes).toBe(2)
    expect(graph.routes.every((route) => route.route_key.includes('crawler-x'))).toBe(true)
    expect(graph.routes[0]?.dedup_contract).toContain('payload')
    expect(graph.routes[0]?.policy.summary_card).toMatchObject({
        enabled: true,
        interval_seconds: 1800,
        threshold: 8,
        send_first_immediately: true,
        send_first_native: false,
        media_realtime: false,
        flush_on_threshold: true,
        window_alignment: 'none',
    })
    expect(graph.routes[1]?.mode.aggregation).toBe(true)
    expect(graph.diagnostics.some((item) => item.code === 'duplicate_target_for_crawler')).toBe(true)
})

test('buildRouteGraph diagnoses fixed-window summary-card policy mismatches', () => {
    const graph = buildRouteGraph({
        crawlers: [
            {
                id: 'crawler-x',
                name: 'crawler x',
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
                platform: 'qq' as any,
                cfg_platform: {
                    summary_card: {
                        enabled: true,
                        interval_seconds: 7200,
                        send_first_immediately: false,
                        media_realtime: true,
                        flush_on_threshold: false,
                        align_to_hour: true,
                        flush_delay_seconds: 300,
                    },
                },
            },
            {
                id: 'target-bili',
                platform: 'bilibili' as any,
                cfg_platform: {
                    summary_card: {
                        enabled: true,
                        send_first_immediately: false,
                        send_first_native: true,
                        media_realtime: true,
                        media_realtime_text: 'none',
                    },
                },
            },
        ],
        connections: {
            'crawler-formatter': {
                'crawler-x': ['formatter-a'],
            },
            'formatter-target': {
                'formatter-a': ['target-qq', 'target-bili'],
            },
        },
    } as any)

    expect(graph.routes.find((route) => route.target_id === 'target-qq')?.policy.summary_card).toMatchObject({
        interval_seconds: 7200,
        send_first_immediately: false,
        send_first_native: false,
        media_realtime: true,
        flush_on_threshold: false,
        flush_delay_seconds: 300,
        window_alignment: 'hour',
    })
    expect(graph.diagnostics.some((item) => item.code === 'summary_card_no_native_idle_first')).toBe(true)
    expect(graph.diagnostics.some((item) => item.code === 'summary_card_native_first_disabled')).toBe(true)
    expect(graph.diagnostics.some((item) => item.code === 'summary_card_empty_realtime_text_non_qq')).toBe(true)
})

test('buildRouteGraph accepts metadata realtime media without idle-first native send', () => {
    const graph = buildRouteGraph({
        crawlers: [
            {
                id: 'crawler-x',
                name: 'crawler x',
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
                id: 'target-bili',
                platform: 'bilibili' as any,
                cfg_platform: {
                    summary_card: {
                        enabled: true,
                        send_first_immediately: false,
                        send_first_native: false,
                        media_realtime: true,
                        media_realtime_text: 'metadata',
                        flush_on_threshold: false,
                    },
                },
            },
        ],
        connections: {
            'crawler-formatter': {
                'crawler-x': ['formatter-a'],
            },
            'formatter-target': {
                'formatter-a': ['target-bili'],
            },
        },
    } as any)

    expect(graph.routes[0]?.policy.summary_card).toMatchObject({
        send_first_immediately: false,
        send_first_native: false,
        media_realtime: true,
        media_realtime_text: 'metadata',
    })
    expect(graph.diagnostics.some((item) => item.code === 'summary_card_no_native_idle_first')).toBe(false)
})

test('buildRouteGraph treats live relay crawlers without formatter as operational crawlers', () => {
    const graph = buildRouteGraph({
        crawlers: [
            {
                id: 'ig-live-satsuki',
                name: 'Instagram Live relay',
                cfg_crawler: {
                    live_relay: {
                        enabled: true,
                    },
                },
            },
        ],
    } as any)

    expect(graph.counts.routes).toBe(0)
    expect(graph.counts.operational_crawlers).toBe(1)
    expect(graph.operational_crawlers).toEqual([
        {
            crawler_id: 'ig-live-satsuki',
            crawler_name: 'Instagram Live relay',
            kind: 'instagram_live_relay',
        },
    ])
    expect(graph.diagnostics).toEqual([])
})
