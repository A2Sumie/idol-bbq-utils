import EventEmitter from 'events'
import { Logger } from '@idol-bbq-utils/log'
import { BaseCompatibleModel, TaskScheduler } from './utils/base'
import { CACHE_DIR_ROOT, configParser, log } from './config'
import { initializeCacheDirectories } from './utils/directories'
import type { AppConfig } from './types'
import { SpiderPools, SpiderTaskScheduler } from './managers/spider-manager'
import { ForwarderPools, ForwarderTaskScheduler } from './managers/forwarder-manager'
import { TaskManager } from './managers/task-manager'
import { APIManager, type ApiRuntimeControl, type ApiRuntimeDeps, type ApiRuntimeMeta, type ApiRuntimeReloadResult } from './managers/api-manager'

interface RuntimeSnapshot {
    config: AppConfig
    emitter: EventEmitter
    taskSchedulers: Array<TaskScheduler.TaskScheduler>
    compatibleModels: Array<BaseCompatibleModel>
    spiderPools?: SpiderPools
    forwarderPools?: ForwarderPools
    createdAt: number
}

function parseConfigOrThrow(configPath: string) {
    const config = configParser(configPath)
    if (!config) {
        throw new Error(`Config file is empty or invalid: ${configPath}`)
    }
    return config
}

export class RuntimeController {
    private readonly configPath: string
    private readonly cacheRoot: string
    private readonly log: Logger
    private runtime?: RuntimeSnapshot
    private apiManager?: APIManager
    private reloadSequence = 0
    private startedAt = Date.now()
    private lastReloadedAt = Date.now()
    private reloadPromise: Promise<ApiRuntimeReloadResult> | null = null
    private shuttingDown = false

    constructor(configPath = './config.yaml', cacheRoot = CACHE_DIR_ROOT, parentLog: Logger = log) {
        this.configPath = configPath
        this.cacheRoot = cacheRoot
        this.log = parentLog.child({ subservice: 'RuntimeController' })
    }

    async init() {
        initializeCacheDirectories(this.cacheRoot)
        const config = parseConfigOrThrow(this.configPath)
        this.runtime = await this.createRuntime(config)
        this.startedAt = Date.now()
        this.lastReloadedAt = this.startedAt

        if (config.api || process.env.API_SECRET) {
            this.apiManager = new APIManager(this.createApiRuntimeControl(), this.log)
            await this.apiManager.init()
        }
    }

    async reload(nextConfig?: AppConfig, reason = 'manual') {
        if (this.reloadPromise) {
            return this.reloadPromise
        }

        this.reloadPromise = this.performReload(nextConfig, reason)
        try {
            return await this.reloadPromise
        } finally {
            this.reloadPromise = null
        }
    }

    async shutdown() {
        if (this.shuttingDown) {
            return
        }

        this.shuttingDown = true
        this.log.info('Shutting down gracefully...')

        if (this.runtime) {
            await this.stopRuntime(this.runtime, 'shutdown')
            this.runtime = undefined
        }

        if (this.apiManager) {
            await this.apiManager.drop()
            this.apiManager = undefined
        }

        this.log.info('Cleanup completed')
    }

    getRuntimeMeta(): ApiRuntimeMeta {
        return {
            generation: this.reloadSequence,
            configPath: this.configPath,
            startedAt: new Date(this.startedAt).toISOString(),
            lastReloadedAt: new Date(this.lastReloadedAt).toISOString(),
            reloading: this.reloadPromise !== null,
        }
    }

    private createApiRuntimeControl(): ApiRuntimeControl {
        return {
            getConfig: () => this.runtime?.config || parseConfigOrThrow(this.configPath),
            getDeps: () => this.getApiDeps(),
            getRuntimeMeta: () => this.getRuntimeMeta(),
            reloadRuntime: (config?: AppConfig) => this.reload(config, 'api'),
        }
    }

    private getApiDeps(): ApiRuntimeDeps {
        return {
            emitter: this.runtime?.emitter,
            forwarderPools: this.runtime?.forwarderPools,
            spiderPools: this.runtime?.spiderPools,
        }
    }

    private async performReload(nextConfig?: AppConfig, reason = 'manual'): Promise<ApiRuntimeReloadResult> {
        const previousRuntime = this.runtime
        if (!previousRuntime) {
            throw new Error('Runtime has not been initialized')
        }

        const previousConfig = previousRuntime.config
        const targetConfig = nextConfig || parseConfigOrThrow(this.configPath)
        this.log.warn(`Runtime reload requested (${reason})`)

        await this.stopRuntime(previousRuntime, `reload:${reason}`)
        this.runtime = undefined

        try {
            this.runtime = await this.createRuntime(targetConfig)
        } catch (error) {
            this.log.error(`Reload failed, attempting rollback: ${error instanceof Error ? error.message : String(error)}`)
            try {
                this.runtime = await this.createRuntime(previousConfig)
                this.log.warn('Runtime rollback completed')
            } catch (rollbackError) {
                this.log.error(
                    `Runtime rollback failed: ${rollbackError instanceof Error ? rollbackError.message : String(rollbackError)}`,
                )
            }
            throw error
        }

        this.reloadSequence += 1
        this.lastReloadedAt = Date.now()
        const meta = this.getRuntimeMeta()
        this.log.info(
            `Runtime hot reloaded. Generation=${meta.generation}, crawlers=${this.runtime.config.crawlers?.length || 0}, forwarders=${this.runtime.config.forwarders?.length || 0}`,
        )

        return {
            success: true,
            generation: meta.generation,
            reloadedAt: meta.lastReloadedAt,
            configPath: meta.configPath,
        }
    }

    private async createRuntime(config: AppConfig): Promise<RuntimeSnapshot> {
        const taskSchedulers: Array<TaskScheduler.TaskScheduler> = []
        const compatibleModels: Array<BaseCompatibleModel> = []
        const emitter = new EventEmitter()

        const {
            crawlers,
            cfg_crawler,
            forward_targets,
            cfg_forward_target,
            forwarders,
            cfg_forwarder,
            formatters,
        } = config

        this.log.info(
            `[Trace] Config loaded. Connections keys: ${config.connections ? Object.keys(config.connections).join(',') : 'UNDEFINED'}`,
        )

        let spiderPools: SpiderPools | undefined
        if (crawlers && crawlers.length > 0) {
            spiderPools = new SpiderPools(this.cacheRoot, emitter, log)
            compatibleModels.push(spiderPools)
            taskSchedulers.push(
                new SpiderTaskScheduler(
                    {
                        crawlers,
                        cfg_crawler,
                    },
                    emitter,
                    log,
                ),
            )
        }

        let forwarderPools: ForwarderPools | undefined
        if (forward_targets && forward_targets.length > 0) {
            forwarderPools = new ForwarderPools(
                {
                    forward_targets,
                    cfg_forward_target,
                    connections: config.connections,
                    formatters,
                },
                emitter,
                log,
            )
            compatibleModels.push(forwarderPools)
        }

        if (forwarderPools) {
            compatibleModels.push(new TaskManager(forwarderPools, { processors: config.processors }, log))
        }

        this.log.info(`[Trace] Check forwarders: ${forwarders?.length}, crawlers: ${crawlers?.length}`)
        if ((forwarders && forwarders.length > 0) || (crawlers && crawlers.length > 0)) {
            taskSchedulers.push(
                new ForwarderTaskScheduler(
                    {
                        forwarders,
                        cfg_forwarder,
                        connections: config.connections,
                        crawlers,
                        formatters,
                    },
                    emitter,
                    log,
                ),
            )
        }

        for (const model of compatibleModels) {
            await model.init()
        }

        for (const taskScheduler of taskSchedulers) {
            await taskScheduler.init()
            await taskScheduler.start()
        }

        return {
            config,
            emitter,
            taskSchedulers,
            compatibleModels,
            spiderPools,
            forwarderPools,
            createdAt: Date.now(),
        }
    }

    private async stopRuntime(runtime: RuntimeSnapshot, reason: string) {
        this.log.info(`Stopping runtime (${reason})`)

        for (const taskScheduler of runtime.taskSchedulers) {
            await taskScheduler.stop()
        }

        const idle = await this.waitForSchedulersIdle(runtime.taskSchedulers, 30000, 250)
        if (!idle) {
            this.log.warn(`Runtime stop timeout (${reason}); forcing teardown with active tasks still present`)
        }

        for (const taskScheduler of runtime.taskSchedulers) {
            await taskScheduler.drop()
        }

        for (const model of [...runtime.compatibleModels].reverse()) {
            await model.drop()
        }
    }

    private async waitForSchedulersIdle(
        schedulers: Array<TaskScheduler.TaskScheduler>,
        timeoutMs: number,
        pollIntervalMs: number,
    ) {
        const deadline = Date.now() + timeoutMs
        while (Date.now() < deadline) {
            const activeTasks = schedulers.reduce((sum, scheduler) => sum + scheduler.getActiveTaskCount(), 0)
            if (activeTasks === 0) {
                return true
            }
            await Bun.sleep(pollIntervalMs)
        }
        return schedulers.every((scheduler) => scheduler.getActiveTaskCount() === 0)
    }
}
