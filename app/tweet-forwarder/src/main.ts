import puppeteer from 'puppeteer-core'
import { SpiderPools, SpiderTaskScheduler } from './managers/spider-manager'
import { configParser, log, CACHE_DIR_ROOT } from './config'
import EventEmitter from 'events'
import { ForwarderPools, ForwarderTaskScheduler } from './managers/forwarder-manager'
import { BaseCompatibleModel, TaskScheduler } from './utils/base'
import tmp from 'tmp'
import { initializeCacheDirectories } from './utils/directories'

tmp.setGracefulCleanup()

async function main() {
    initializeCacheDirectories(CACHE_DIR_ROOT)

    const taskSchedulers: Array<TaskScheduler.TaskScheduler> = []
    const compatibleModels: Array<BaseCompatibleModel> = []
    const emitter = new EventEmitter()

    const config = configParser('./config.yaml')
    if (!config) {
        log.error('Config file is empty or invalid, exiting...')
        return
    }
    const { crawlers, cfg_crawler, forward_targets, cfg_forward_target, forwarders, cfg_forwarder, formatters } = config
    log.info(`[Trace] Config loaded. Connections keys: ${config.connections ? Object.keys(config.connections).join(',') : 'UNDEFINED'}`)


    if (crawlers && crawlers.length > 0) {
        const tmpDir = tmp.dirSync({
            prefix: 'puppeteer-',
            unsafeCleanup: true,
        })

        log.info(`Puppeteer userDataDir: ${tmpDir.name}`)

        const browser = await puppeteer.launch({
            headless: true,
            handleSIGINT: false,
            handleSIGHUP: false,
            handleSIGTERM: false,
            args: [process.env.NO_SANDBOX ? '--no-sandbox' : '', '--disable-dev-shm-usage'].filter(Boolean),
            channel: 'chrome',
            userDataDir: tmpDir.name,
        })
        // @ts-ignore
        const spiderPools = new SpiderPools(browser, emitter, log)
        compatibleModels.push(spiderPools)
        const spiderTaskScheduler = new SpiderTaskScheduler(
            {
                crawlers,
                cfg_crawler,
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
            },
            emitter,
            log,
        )
        compatibleModels.push(forwarderPools)
    }

    if (forwarderPools) {
        const { TaskManager } = await import('./managers/task-manager')
        const taskManager = new TaskManager(forwarderPools, log)
        compatibleModels.push(taskManager)
    }

    log.info(`[Trace] Check forwarders: ${forwarders?.length}, crawlers: ${crawlers?.length}`)
    if ((forwarders && forwarders.length > 0) || (crawlers && crawlers.length > 0)) {
        const forwarderTaskScheduler = new ForwarderTaskScheduler(
            {
                forwarders,
                cfg_forwarder,
                connections: config.connections,
                crawlers,
            },
            emitter,
            log,
        )
        taskSchedulers.push(forwarderTaskScheduler)
    }

    // Initialize APIManager (Skip in Debug Mode to avoid port conflict)
    if ((config.api || process.env.API_SECRET) && !process.env.TEST_PUSH_TARGET) {
        const { APIManager } = await import('./managers/api-manager')
        const apiManager = new APIManager(config, log)
        compatibleModels.push(apiManager)
    }

    for (const c of compatibleModels) {
        await c.init()
    }

    // --- DEBUG TRIGGER ---
    if (process.env.TEST_PUSH_TARGET && forwarderPools) {
        log.info(`Debug Trigger Detected. Target: ${process.env.TEST_PUSH_TARGET}`)
        const { runDebugPushWithPools } = await import('./utils/debug')
        await runDebugPushWithPools(forwarderPools, process.env.TEST_PUSH_TARGET, log)
        log.info('Debug sequence finished. Exiting.')
        process.exit(0)
    }
    // ---------------------

    for (const taskScheduler of taskSchedulers) {
        await taskScheduler.init()
        await taskScheduler.start()
    }

    async function exitHandler() {
        log.info('Shutting down gracefully...')

        for (const taskScheduler of taskSchedulers) {
            await taskScheduler.stop()
            await taskScheduler.drop()
        }
        for (const c of compatibleModels) {
            await c.drop()
        }

        log.info('Cleanup completed')
        process.exit(0)
    }
    process.on('SIGINT', exitHandler)
    process.on('SIGTERM', exitHandler)
    process.on('SIGHUP', exitHandler)
}
main()
