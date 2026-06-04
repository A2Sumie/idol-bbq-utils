#!/usr/bin/env bun

import fs from 'fs'
import YAML from 'yaml'
import { buildConfigAudit, type ConfigAudit } from '../src/services/config-audit-service'
import { SENSITIVE_KEY_PATTERN } from '../src/services/redaction-service'
import { normalizePipelinesForRuntime } from '../src/services/quick-config-service'
import type { AppConfig } from '../src/types'

type OutputFormat = 'summary' | 'json'

type Args = {
    configPath: string
    format: OutputFormat
    failOnDiagnostics: boolean
}

function usage() {
    return `Usage: bun app/tweet-forwarder/scripts/config-audit.ts [--config <path>] [--format summary|json] [--fail-on-diagnostics]

Prints a no-secret config audit. The output includes redacted/policy hashes,
route counts, diagnostic codes, and sensitive field paths only; sensitive
values are never printed.`
}

function parseArgs(argv: Array<string>): Args {
    const args: Args = {
        configPath: 'config.yaml',
        format: 'summary',
        failOnDiagnostics: false,
    }

    for (let index = 0; index < argv.length; index += 1) {
        const key = argv[index]
        if (key === '--help' || key === '-h') {
            process.stdout.write(`${usage()}\n`)
            process.exit(0)
        }
        if (key === '--fail-on-diagnostics') {
            args.failOnDiagnostics = true
            continue
        }
        if (key === '--config') {
            const value = argv[index + 1]
            if (!value || value.startsWith('--')) {
                throw new Error('--config requires a path')
            }
            args.configPath = value
            index += 1
            continue
        }
        if (key === '--format') {
            const value = argv[index + 1]
            if (value !== 'summary' && value !== 'json') {
                throw new Error('--format must be summary or json')
            }
            args.format = value
            index += 1
            continue
        }
        throw new Error(`Unexpected argument: ${key}`)
    }

    return args
}

function readConfig(configPath: string): AppConfig {
    const text = fs.readFileSync(configPath, 'utf8')
    const parsed = YAML.parse(text) as AppConfig
    return normalizePipelinesForRuntime(parsed)
}

function collectSensitiveValues(value: unknown) {
    const values = new Set<string>()

    function visit(entry: unknown) {
        if (Array.isArray(entry)) {
            entry.forEach(visit)
            return
        }
        if (!entry || typeof entry !== 'object') {
            return
        }

        for (const [key, child] of Object.entries(entry as Record<string, unknown>)) {
            if (SENSITIVE_KEY_PATTERN.test(key)) {
                if (typeof child === 'string' && child.length >= 4) {
                    values.add(child)
                }
                continue
            }
            visit(child)
        }
    }

    visit(value)
    return Array.from(values)
}

function assertNoSensitiveValues(output: string, config: AppConfig) {
    const leaked = collectSensitiveValues(config).filter((value) => output.includes(value))
    if (leaked.length > 0) {
        throw new Error(`config audit output would leak ${leaked.length} sensitive value(s)`)
    }
}

function buildSummary(configPath: string, audit: ConfigAudit) {
    const diagnosticCodes = audit.route_graph.diagnostics.map((diagnostic) => diagnostic.code).sort()

    return {
        ok: audit.route_graph.counts.errors === 0,
        generated_at: audit.generated_at,
        config_path: configPath,
        redacted_config_hash: audit.redacted_config_hash,
        policy_hash: audit.policy_hash,
        secret_field_count: audit.secret_fields.count,
        route_graph: {
            counts: audit.route_graph.counts,
            diagnostic_codes: diagnosticCodes,
            operational_crawlers: audit.route_graph.operational_crawlers,
            summary_card_routes: audit.route_graph.summary_card_routes.length,
        },
    }
}

function main() {
    const args = parseArgs(process.argv.slice(2))
    const config = readConfig(args.configPath)
    const audit = buildConfigAudit(config)
    const payload = args.format === 'json' ? audit : buildSummary(args.configPath, audit)
    const output = `${JSON.stringify(payload, null, 2)}\n`
    assertNoSensitiveValues(output, config)
    process.stdout.write(output)

    const counts = audit.route_graph.counts
    if (counts.errors > 0 || (args.failOnDiagnostics && counts.warnings > 0)) {
        process.exit(2)
    }
}

try {
    main()
} catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exit(1)
}
