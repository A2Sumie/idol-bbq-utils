import EventEmitter from 'events'
import { Logger } from '@idol-bbq-utils/log'
import { BaseCompatibleModel, TaskScheduler } from './utils/base'
import { CACHE_DIR_ROOT, configParser, log } from './config'
import DB from './db'
import { initializeCacheDirectories } from './utils/directories'
import type { AppConfig } from './types'
import { SpiderPools, SpiderTaskScheduler } from './managers/spider-manager'
import { ForwarderPools, ForwarderTaskScheduler } from './managers/forwarder-manager'
import { TaskManager } from './managers/task-manager'
import {
    APIManager,
    type ApiRuntimeControl,
    type ApiRuntimeDeps,
    type ApiRuntimeMeta,
    type ApiRuntimeReloadResult,
} from './managers/api-manager'
import { startMediaCacheCleanupJob, type MediaCacheCleanupJob } from './services/media-cache-service'
import { buildRouteGraph } from './services/route-graph-service'
import { buildRuntimeManifest } from './services/runtime-manifest-service'

interface RuntimeSnapshot {
    mode: RuntimeMode
    config: AppConfig
    emitter: EventEmitter
    taskSchedulers: Array<TaskScheduler.TaskScheduler>
    compatibleModels: Array<BaseCompatibleModel>
    spiderPools?: SpiderPools
    spiderTaskScheduler?: SpiderTaskScheduler
    forwarderPools?: ForwarderPools
    createdAt: number
    manifest: ReturnType<typeof buildRuntimeManifest>
}

export type RuntimeMode = 'online' | 'api-only' | 'offline'

export function resolveRuntimeMode(env: Record<string, string | undefined> = process.env): RuntimeMode {
    const raw = String(env.IDOL_BBQ_RUNTIME_MODE || 'online')
        .trim()
        .toLowerCase()
        .replace(/_/g, '-')
    if (raw === 'online' || raw === 'api-only' || raw === 'offline') {
        return raw
    }
    throw new Error(`Invalid IDOL_BBQ_RUNTIME_MODE: ${raw || '(empty)'}`)
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
    private readonly runtimeMode: RuntimeMode
    private runtime?: RuntimeSnapshot
    private apiManager?: APIManager
    private reloadSequence = 0
    private startedAt = Date.now()
    private lastReloadedAt = Date.now()
    private reloadPromise: Promise<ApiRuntimeReloadResult> | null = null
    private shuttingDown = false
    private mediaCacheCleanupJob?: MediaCacheCleanupJob

    constructor(
        configPath = './config.yaml',
        cacheRoot = CACHE_DIR_ROOT,
        parentLog: Logger = log,
        options: { runtimeMode?: RuntimeMode } = {},
    ) {
        this.configPath = configPath
        this.cacheRoot = cacheRoot
        this.runtimeMode = options.runtimeMode || resolveRuntimeMode()
        this.log = parentLog.child({ subservice: 'RuntimeController' })
    }

    async init() {
        if (this.runtimeMode === 'offline') {
            this.startedAt = Date.now()
            this.lastReloadedAt = this.startedAt
            this.log.warn('Runtime mode offline: config parsing, migrations, API, schedulers, and senders are disabled')
            return
        }

        initializeCacheDirectories(this.cacheRoot)
        await this.failInterruptedInlineApiTasks()
        if (this.runtimeMode === 'online') {
            this.mediaCacheCleanupJob = startMediaCacheCleanupJob(this.log)
        } else {
            this.log.warn('Runtime mode api-only: media cache cleanup and all dispatch/send workers are disabled')
        }
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

        this.mediaCacheCleanupJob?.stop()
        this.mediaCacheCleanupJob = undefined

        this.log.info('Cleanup completed')
    }

    getRuntimeMeta(): ApiRuntimeMeta {
        return {
            generation: this.reloadSequence,
            configPath: this.configPath,
            mode: this.runtimeMode,
            startedAt: new Date(this.startedAt).toISOString(),
            lastReloadedAt: new Date(this.lastReloadedAt).toISOString(),
            reloading: this.reloadPromise !== null,
            manifest: this.runtime?.manifest || buildRuntimeManifest(this.configPath, this.runtime?.config),
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
            spiderTaskScheduler: this.runtime?.spiderTaskScheduler,
        }
    }

    private async failInterruptedInlineApiTasks() {
        try {
            const result = await DB.TaskQueue.failInterruptedInlineProcessing()
            if (result.count > 0) {
                this.log.warn(`Marked ${result.count} interrupted inline API task(s) as failed`)
            }
        } catch (error) {
            this.log.warn(
                `Failed to mark interrupted inline API tasks: ${error instanceof Error ? error.message : String(error)}`,
            )
        }
    }

    private async performReload(nextConfig?: AppConfig, reason = 'manual'): Promise<ApiRuntimeReloadResult> {
        if (this.runtimeMode === 'offline') {
            throw new Error('Runtime reload is disabled in offline mode')
        }
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
            this.log.error(
                `Reload failed, attempting rollback: ${error instanceof Error ? error.message : String(error)}`,
            )
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

        const { crawlers, cfg_crawler, forward_targets, cfg_forward_target, forwarders, cfg_forwarder, formatters } =
            config

        this.log.info(
            `[Trace] Config loaded. Connections keys: ${config.connections ? Object.keys(config.connections).join(',') : 'UNDEFINED'}`,
        )
        const routeGraph = buildRouteGraph(config)
        if (routeGraph.diagnostics.length > 0) {
            this.log.warn(
                `Route graph diagnostics: ${routeGraph.counts.errors} error(s), ${routeGraph.counts.warnings} warning(s)`,
            )
            for (const diagnostic of routeGraph.diagnostics.slice(0, 20)) {
                this.log.warn(`[route:${diagnostic.severity}] ${diagnostic.code}: ${diagnostic.message}`)
            }
        }

        if (this.runtimeMode === 'api-only') {
            this.log.warn('Runtime mode api-only: route graph loaded without crawler/forwarder schedulers or senders')
            return {
                mode: this.runtimeMode,
                config,
                emitter,
                taskSchedulers,
                compatibleModels,
                createdAt: Date.now(),
                manifest: buildRuntimeManifest(this.configPath, config),
            }
        }

        let spiderPools: SpiderPools | undefined
        let spiderTaskScheduler: SpiderTaskScheduler | undefined
        if (crawlers && crawlers.length > 0) {
            spiderPools = new SpiderPools(this.cacheRoot, emitter, log)
            compatibleModels.push(spiderPools)
            spiderTaskScheduler = new SpiderTaskScheduler(
                {
                    crawlers,
                    cfg_crawler,
                    connections: config.connections,
                    formatters,
                    forward_targets,
                    processors: config.processors,
                },
                emitter,
                log,
            )
            taskSchedulers.push(spiderTaskScheduler)
        }

        let forwarderPools: ForwarderPools | undefined
        if (forward_targets && forward_targets.length > 0) {
            forwarderPools = new ForwarderPools(
                {
                    forward_targets,
                    cfg_forward_target,
                    connections: config.connections,
                    formatters,
                    cfg_forwarder,
                    forwarders,
                    crawlers,
                    processors: config.processors,
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
        if (forwarderPools && ((forwarders && forwarders.length > 0) || (crawlers && crawlers.length > 0))) {
            taskSchedulers.push(
                new ForwarderTaskScheduler(
                    {
                        forwarders,
                        cfg_forwarder,
                        connections: config.connections,
                        crawlers,
                        formatters,
                        forward_targets,
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
            mode: this.runtimeMode,
            config,
            emitter,
            taskSchedulers,
            compatibleModels,
            spiderPools,
            spiderTaskScheduler,
            forwarderPools,
            createdAt: Date.now(),
            manifest: buildRuntimeManifest(this.configPath, config),
        }
    }

    private async stopRuntime(runtime: RuntimeSnapshot, reason: string) {
        this.log.info(`Stopping runtime (${reason})`)

        for (const taskScheduler of runtime.taskSchedulers) {
            await taskScheduler.stop()
        }

        for (const model of [...runtime.compatibleModels].reverse()) {
            if (typeof model.stop !== 'function') {
                continue
            }
            try {
                await model.stop(reason)
            } catch (error) {
                this.log.warn(
                    `Failed to signal ${model.NAME} stop: ${error instanceof Error ? error.message : String(error)}`,
                )
            }
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
