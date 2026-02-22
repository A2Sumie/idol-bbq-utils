
import { configParser } from './config'
import path from 'path'
import { createLogger, format, winston, Logger } from '@idol-bbq-utils/log'

// Setup basic logger
const log: Logger = createLogger({
    defaultMeta: { service: 'routing-simulator' },
    level: 'info',
    format: format.combine(
        format.colorize(),
        format.simple()
    ),
    transports: [new winston.transports.Console()]
})

async function main() {
    const args = process.argv.slice(2)
    const searchInput = args[0]

    if (!searchInput) {
        console.error('Usage: bun simulate_routing.ts <username_or_url>')
        console.error('Example: bun simulate_routing.ts ru_ri0808')
        process.exit(1)
    }

    log.info(`Analyzing routing for: ${searchInput}`)

    // Load Config
    // Attempt multiple paths for robustness (container vs local)
    let configPath = path.resolve(__dirname, '../config.yaml')
    // If not found (e.g. strict container), try standard
    let config = configParser(configPath)
    if (!config) {
        // Retry with ./config.yaml if running from root
        config = configParser('config.yaml')
    }

    if (!config) {
        log.error('Config file empty or invalid.')
        process.exit(1)
    }

    const { crawlers, formatters, connections, forward_targets } = config

    // 1. Find Matching Crawlers
    const matchingCrawlers = crawlers?.filter(c => {
        const pathsMatch = c.paths?.some(p => p.includes(searchInput)) ?? false
        const originMatch = c.origin?.includes(searchInput) ?? false
        return pathsMatch || originMatch
    }) || []

    if (matchingCrawlers.length === 0) {
        log.warn(`No crawlers found matching '${searchInput}'`)
        process.exit(0)
    }

    log.info(`Found ${matchingCrawlers.length} matching crawlers:`)
    matchingCrawlers.forEach(c => log.info(` - ${c.name} (${c.origin})`))

    console.log('\n--- ROUTING MAP ---')

    // 2. Trace Connections
    for (const crawler of matchingCrawlers) {
        if (!crawler.name) continue
        console.log(`\n[Crawler] ${crawler.name}`)

        const connectedFormatterIds = connections?.['crawler-formatter']?.[crawler.name] || []
        if (connectedFormatterIds.length === 0) {
            console.log(`  └─ (No Formatters Connected)`)
            continue
        }

        for (const fmtId of connectedFormatterIds) {
            const formatter = formatters?.find(f => f.id === fmtId)
            const fmtName = formatter ? `${formatter.name} (${formatter.render_type})` : fmtId

            console.log(`  └─ [Formatter] ${fmtName} [ID: ${fmtId}]`)

            const connectedTargetIds = connections?.['formatter-target']?.[fmtId] || []
            if (connectedTargetIds.length === 0) {
                console.log(`      └─ (No Targets Connected)`)
                continue
            }

            for (const targetId of connectedTargetIds) {
                // Verify target exists in definition
                const targetDef = forward_targets?.find(t => t.id === targetId)
                const targetInfo = targetDef ? `(Platform: ${targetDef.platform})` : '(Undefined!)'

                // Check if target is '七虹信标-群2' and alert
                let prefix = '      └─'
                if (targetId.includes('七虹信标-群2')) {
                    prefix = '      👉'
                }

                console.log(`${prefix} [Target] ${targetId} ${targetInfo}`)
            }
        }
    }
    console.log('\n-------------------')
}

main()
