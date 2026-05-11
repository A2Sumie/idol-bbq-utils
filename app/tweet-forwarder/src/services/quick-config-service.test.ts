import { expect, test } from 'bun:test'
import { ProcessorProvider } from '@/types'
import {
    buildQuickConfigModel,
    compileConnectionsFromQuickPatch,
    exportPipelineConfigs,
    normalizePipelinesForRuntime,
} from './quick-config-service'

const baseConfig = {
    crawlers: [
        {
            id: 'crawler-x',
            name: 'X crawler',
            origin: 'https://x.com',
            paths: ['/list/test'],
            cfg_crawler: {
                cron: '*/5 * * * *',
            },
        },
    ],
    processors: [
        {
            id: 'processor-summary',
            name: 'Summary',
            provider: ProcessorProvider.Mechanical,
            api_key: '',
            cfg_processor: {
                action: 'extract',
            },
        },
    ],
    formatters: [
        {
            id: 'formatter-card',
            name: 'Card',
            render_type: 'text-card',
        },
    ],
    forward_targets: [
        {
            id: 'target-low-noise',
            platform: 'qq',
            cfg_platform: {
                url: 'http://127.0.0.1:3001',
                token: 'token',
                group_id: '161717573',
                digest_threshold: 3,
            },
        },
    ],
    connections: {
        'crawler-processor': {
            'crawler-x': 'processor-summary',
        },
        'processor-formatter': {
            'processor-summary': ['formatter-card'],
        },
        'formatter-target': {
            'formatter-card': ['target-low-noise'],
        },
    },
} as any

test('quick config model exposes reviewable routes and links', () => {
    const model = buildQuickConfigModel(baseConfig)

    expect(model.schema).toBe('idol-bbq.quick-config.v1')
    expect(model.model.primary).toBe('pipelines')
    expect(model.pipelines[0]).toMatchObject({
        id: 'pipeline:crawler-x',
        source: {
            crawler_id: 'crawler-x',
        },
        processors: [
            {
                processor_id: 'processor-summary',
                role: 'extract',
            },
        ],
        formatters: [
            {
                formatter_id: 'formatter-card',
                render_type: 'text-card',
            },
        ],
    })
    expect(model.routes[0]).toMatchObject({
        crawler_id: 'crawler-x',
        processor_id: 'processor-summary',
        formatter_ids: ['formatter-card'],
        target_ids: ['target-low-noise'],
    })
    expect(model.links.map((link) => link.kind)).toEqual([
        'crawler-processor',
        'processor-formatter',
        'formatter-target',
    ])
    expect(model.diagnostics).toEqual([])
})

test('quick config pipeline patches compile into current runtime connections', () => {
    const nextConfig = compileConnectionsFromQuickPatch(baseConfig, {
        pipelines: [
            {
                source: {
                    crawler_id: 'crawler-x',
                },
                processors: [
                    {
                        processor_id: 'processor-summary',
                    },
                ],
                formatters: [
                    {
                        formatter_id: 'formatter-card',
                    },
                ],
                delivery: [
                    {
                        formatter_id: 'formatter-card',
                        target_ids: ['target-low-noise'],
                    },
                ],
            },
        ],
    })

    expect(nextConfig.connections?.['crawler-processor']).toEqual({
        'crawler-x': 'processor-summary',
    })
    expect(nextConfig.connections?.['processor-formatter']).toEqual({
        'processor-summary': ['formatter-card'],
    })
    expect(nextConfig.connections?.['crawler-formatter']).toEqual({})
    expect(nextConfig.connections?.['formatter-target']).toEqual({
        'formatter-card': ['target-low-noise'],
    })
})

test('pipeline configs are exported as canonical migration shape and normalized for runtime', () => {
    const pipelines = exportPipelineConfigs(baseConfig)
    expect(pipelines).toEqual([
        {
            id: 'pipeline:crawler-x',
            name: 'X crawler',
            enabled: true,
            source: {
                crawler_id: 'crawler-x',
            },
            processors: [
                {
                    processor_id: 'processor-summary',
                    role: 'extract',
                },
            ],
            formatters: [
                {
                    formatter_id: 'formatter-card',
                },
            ],
            delivery: [
                {
                    formatter_id: 'formatter-card',
                    target_ids: ['target-low-noise'],
                },
            ],
        },
    ])

    const runtimeConfig = normalizePipelinesForRuntime({
        ...baseConfig,
        connections: undefined,
        pipelines,
    } as any)
    expect(runtimeConfig.connections?.['crawler-processor']).toEqual({
        'crawler-x': 'processor-summary',
    })
    expect(runtimeConfig.connections?.['formatter-target']).toEqual({
        'formatter-card': ['target-low-noise'],
    })
})

test('quick config patch compiles routes back to existing connection maps', () => {
    const nextConfig = compileConnectionsFromQuickPatch(baseConfig, {
        routes: [
            {
                crawler_id: 'crawler-x',
                formatter_ids: ['formatter-card'],
                formatter_targets: {
                    'formatter-card': ['target-low-noise'],
                },
            },
        ],
    })

    expect(nextConfig.connections?.['crawler-processor']).toEqual({})
    expect(nextConfig.connections?.['crawler-formatter']).toEqual({
        'crawler-x': ['formatter-card'],
    })
    expect(nextConfig.connections?.['formatter-target']).toEqual({
        'formatter-card': ['target-low-noise'],
    })
})

test('quick config patch rejects unknown nodes by default', () => {
    expect(() =>
        compileConnectionsFromQuickPatch(baseConfig, {
            links: [
                {
                    kind: 'formatter-target',
                    from: 'formatter-card',
                    to: 'missing-target',
                },
            ],
        }),
    ).toThrow('missing target')
})
