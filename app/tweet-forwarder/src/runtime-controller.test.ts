import { afterEach, expect, test } from 'bun:test'
import fs from 'fs'
import os from 'os'
import path from 'path'
import YAML from 'yaml'
import { RuntimeController, resolveRuntimeMode } from './runtime-controller'
import DB from './db'

const tempRoots: string[] = []
const originalTaskQueue = { ...DB.TaskQueue }

afterEach(async () => {
    Object.assign(DB.TaskQueue, originalTaskQueue)
    for (const root of tempRoots.splice(0)) {
        await fs.promises.rm(root, { recursive: true, force: true })
    }
})

function makeTempRoot() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'idol-bbq-runtime-controller-'))
    tempRoots.push(root)
    return root
}

function writeConfig(root: string, value: unknown) {
    const configPath = path.join(root, 'config.yaml')
    fs.writeFileSync(configPath, YAML.stringify(value), 'utf8')
    return configPath
}

test('resolveRuntimeMode accepts explicit safe startup modes', () => {
    expect(resolveRuntimeMode({})).toBe('online')
    expect(resolveRuntimeMode({ IDOL_BBQ_RUNTIME_MODE: 'ONLINE' })).toBe('online')
    expect(resolveRuntimeMode({ IDOL_BBQ_RUNTIME_MODE: 'api_only' })).toBe('api-only')
    expect(resolveRuntimeMode({ IDOL_BBQ_RUNTIME_MODE: 'offline' })).toBe('offline')
    expect(() => resolveRuntimeMode({ IDOL_BBQ_RUNTIME_MODE: 'surprise' })).toThrow(
        /Invalid IDOL_BBQ_RUNTIME_MODE/,
    )
})

test('RuntimeController api-only mode loads config without activating schedulers or senders', async () => {
    const root = makeTempRoot()
    const cleanupCalls: number[] = []
    ;(DB.TaskQueue as any).failInterruptedInlineProcessing = async () => {
        cleanupCalls.push(Date.now())
        return { count: 2 }
    }
    const configPath = writeConfig(root, {
        crawlers: [
            {
                id: 'crawler-a',
                name: 'Crawler A',
                websites: ['https://x.com/example'],
                cfg_crawler: {
                    cron: '* * * * *',
                    immediate_notify: true,
                },
            },
        ],
        forward_targets: [
            {
                id: 'target-a',
                platform: 'qq',
                cfg_platform: {
                    url: 'http://127.0.0.1:59999',
                    token: 'secret',
                    group_id: '123',
                },
            },
        ],
        forwarders: [
            {
                id: 'forwarder-a',
                name: 'Forwarder A',
                crawler_id: 'crawler-a',
                subscribers: ['target-a'],
                cfg_forwarder: {
                    cron: '* * * * *',
                },
            },
        ],
        connections: {
            'crawler-formatter': {},
            'formatter-target': {},
        },
    })

    const controller = new RuntimeController(configPath, path.join(root, 'cache'), undefined, {
        runtimeMode: 'api-only',
    })

    await controller.init()
    try {
        const runtime = (controller as any).runtime
        expect(controller.getRuntimeMeta().mode).toBe('api-only')
        expect(runtime.mode).toBe('api-only')
        expect(runtime.taskSchedulers).toHaveLength(0)
        expect(runtime.compatibleModels).toHaveLength(0)
        expect(runtime.forwarderPools).toBeUndefined()
        expect(runtime.spiderPools).toBeUndefined()
        expect(cleanupCalls).toHaveLength(1)
    } finally {
        await controller.shutdown()
    }
})

test('RuntimeController offline mode does not parse config or create runtime', async () => {
    const root = makeTempRoot()
    let cleanupCalls = 0
    ;(DB.TaskQueue as any).failInterruptedInlineProcessing = async () => {
        cleanupCalls += 1
        throw new Error('offline must not touch db')
    }
    const missingConfigPath = path.join(root, 'missing-config.yaml')
    const controller = new RuntimeController(missingConfigPath, path.join(root, 'cache'), undefined, {
        runtimeMode: 'offline',
    })

    await controller.init()
    try {
        expect(controller.getRuntimeMeta().mode).toBe('offline')
        expect((controller as any).runtime).toBeUndefined()
        expect(cleanupCalls).toBe(0)
    } finally {
        await controller.shutdown()
    }
})
