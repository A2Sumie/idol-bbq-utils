import { afterEach, expect, test } from 'bun:test'
import fs from 'fs'
import os from 'os'
import path from 'path'
import EventEmitter from 'events'
import YAML from 'yaml'
import { RuntimeController, resolveRuntimeMode } from './runtime-controller'
import { BaseCompatibleModel, TaskScheduler } from './utils/base'
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

test('RuntimeController signals compatible models before waiting for scheduler idle', async () => {
    const events: string[] = []
    let modelStopped = false

    class SignalAwareScheduler extends TaskScheduler.TaskScheduler {
        NAME = 'SignalAwareScheduler'
        protected log?: any

        async init() {}
        async start() {}
        async stop() {
            events.push('scheduler.stop')
        }
        async drop() {
            events.push('scheduler.drop')
        }
        updateTaskStatus() {}
        finishTask() {}
        getActiveTaskCount() {
            return modelStopped ? 0 : 1
        }
    }

    class SignalModel extends BaseCompatibleModel {
        NAME = 'SignalModel'
        protected log?: any

        async init() {}
        async stop(reason?: string) {
            events.push(`model.stop:${reason}`)
            modelStopped = true
        }
        async drop() {
            events.push('model.drop')
        }
    }

    const controller = new RuntimeController('/tmp/missing.yaml', '/tmp/idol-bbq-runtime-test', undefined, {
        runtimeMode: 'online',
    })
    const scheduler = new SignalAwareScheduler(new EventEmitter())
    const model = new SignalModel()

    await (controller as any).stopRuntime(
        {
            taskSchedulers: [scheduler],
            compatibleModels: [model],
            emitter: new EventEmitter(),
            config: {},
            mode: 'online',
            createdAt: Date.now(),
            manifest: {},
        },
        'unit-test',
    )

    expect(events).toEqual(['scheduler.stop', 'model.stop:unit-test', 'scheduler.drop', 'model.drop'])
})
