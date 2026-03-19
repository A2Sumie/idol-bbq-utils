import { log } from './config'
import { RuntimeController } from './runtime-controller'

async function main() {
    const runtime = new RuntimeController('./config.yaml')
    await runtime.init()

    async function exitHandler() {
        await runtime.shutdown()
        process.exit(0)
    }

    process.on('SIGINT', exitHandler)
    process.on('SIGTERM', exitHandler)
    process.on('SIGHUP', exitHandler)
}

main().catch(async (error) => {
    log.error(`Fatal startup error: ${error instanceof Error ? error.stack || error.message : String(error)}`)
    process.exit(1)
})
