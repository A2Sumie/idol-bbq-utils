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
    expect(graph.routes[1]?.mode.aggregation).toBe(true)
    expect(graph.diagnostics.some((item) => item.code === 'duplicate_target_for_crawler')).toBe(true)
})
