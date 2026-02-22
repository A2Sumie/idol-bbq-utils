
import { ForwarderPools } from './managers/forwarder-manager'
import { Logger, createLogger, format, winston } from '@idol-bbq-utils/log'
import { runDebugPushWithPools } from './utils/debug'
import { configParser } from './config'
import EventEmitter from 'events'
import path from 'path'

// Setup basic logger
const log: Logger = createLogger({
    defaultMeta: { service: 'tweet-forwarder-debug' },
    level: 'info',
    format: format.combine(
        format.colorize(),
        format.simple()
    ),
    transports: [new winston.transports.Console()]
})

async function main() {
    const args = process.argv.slice(2)
    const targetGroup = args[0]

    if (!targetGroup) {
        console.error('Usage: bun simulate_push.ts <target_group_id_or_name>')
        process.exit(1)
    }

    log.info('Initializing System for Debug...')

    // Load Config
    // config.yaml is symlinked in app/tweet-forwarder/config.yaml
    const configPath = path.resolve(__dirname, '../config.yaml')
    const config = configParser(configPath)
    if (!config) {
        log.error('Config file empty or invalid.')
        process.exit(1)
    }

    const { forward_targets, cfg_forward_target, formatters } = config

    // Init Emitter
    const emitter = new EventEmitter()

    // Init Pools
    const forwarderPools = new ForwarderPools(
        {
            forward_targets,
            cfg_forward_target,
            connections: config.connections,
            formatters,
        },
        emitter,
        log,
    )

    await forwarderPools.init()

    log.info('System Initialized. Running Debug Push...')
    await runDebugPushWithPools(forwarderPools, targetGroup, log)

    // Clean exit
    process.exit(0)
}

main().catch(e => {
    console.error(e)
    process.exit(1)
})
