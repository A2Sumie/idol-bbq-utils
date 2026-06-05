#!/usr/bin/env bun

import fs from 'fs'
import path from 'path'
import YAML from 'yaml'
import { buildCrawlerLiveHealthAudit, type CrawlerHealthPlatform } from '../src/services/crawler-health-audit-service'
import { normalizePipelinesForRuntime } from '../src/services/quick-config-service'
import { SENSITIVE_KEY_PATTERN } from '../src/services/redaction-service'
import type { AppConfig } from '../src/types'

type OutputFormat = 'summary' | 'json'

type Args = {
    configPath: string
    format: OutputFormat
    failOnUnhealthy: boolean
    failOnWarn: boolean
    platforms: Array<CrawlerHealthPlatform>
    timeoutMs: number
    liveProbe: boolean
}

const DEFAULT_PLATFORMS: Array<CrawlerHealthPlatform> = ['x', 'instagram', 'tiktok']

function usage() {
    return `Usage: bun app/tweet-forwarder/scripts/crawler-health-audit.ts [--config <path>] [--format summary|json] [--platform x,instagram,tiktok] [--timeout-ms <ms>] [--no-live-probe] [--fail-on-unhealthy] [--fail-on-warn]

Runs a no-secret crawler cookie health audit. By default it performs low-volume
live probes for supported platforms. Use --no-live-probe or --static-only when a
platform is rate-limited and only cookie structure/required-name evidence is
needed. Output never includes cookie values or cookie file paths.`
}

function parsePlatforms(value: string): Array<CrawlerHealthPlatform> {
    const platforms = value
        .split(',')
        .map((item) => item.trim().toLowerCase())
        .filter(Boolean)
    const supported = new Set<CrawlerHealthPlatform>(['x', 'instagram', 'tiktok', 'youtube', 'unknown'])
    for (const platform of platforms) {
        if (!supported.has(platform as CrawlerHealthPlatform)) {
            throw new Error(`Unsupported platform: ${platform}`)
        }
    }
    return platforms as Array<CrawlerHealthPlatform>
}

function parseArgs(argv: Array<string>): Args {
    const args: Args = {
        configPath: 'config.yaml',
        format: 'summary',
        failOnUnhealthy: false,
        failOnWarn: false,
        platforms: DEFAULT_PLATFORMS,
        timeoutMs: 15_000,
        liveProbe: true,
    }

    for (let index = 0; index < argv.length; index += 1) {
        const key = argv[index]
        if (key === '--help' || key === '-h') {
            process.stdout.write(`${usage()}\n`)
            process.exit(0)
        }
        if (key === '--fail-on-unhealthy') {
            args.failOnUnhealthy = true
            continue
        }
        if (key === '--fail-on-warn') {
            args.failOnWarn = true
            continue
        }
        if (key === '--no-live-probe' || key === '--static-only') {
            args.liveProbe = false
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
        if (key === '--platform') {
            const value = argv[index + 1]
            if (!value || value.startsWith('--')) {
                throw new Error('--platform requires a comma-separated list')
            }
            args.platforms = parsePlatforms(value)
            index += 1
            continue
        }
        if (key === '--timeout-ms') {
            const value = Number(argv[index + 1])
            if (!Number.isFinite(value) || value <= 0) {
                throw new Error('--timeout-ms requires a positive number')
            }
            args.timeoutMs = Math.floor(value)
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

function resolveCookieFile(cookieFile: string, configPath: string) {
    if (!cookieFile.trim()) {
        return null
    }

    if (path.isAbsolute(cookieFile)) {
        if (fs.existsSync(cookieFile)) {
            return cookieFile
        }
        if (cookieFile.startsWith('/app/')) {
            return path.resolve(process.cwd(), cookieFile.slice('/app/'.length))
        }
        return cookieFile
    }

    return path.resolve(path.dirname(configPath), cookieFile)
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
        throw new Error(`crawler health audit output would leak ${leaked.length} sensitive value(s)`)
    }
}

function buildSummary(configPath: string, audit: Awaited<ReturnType<typeof buildCrawlerLiveHealthAudit>>) {
    return {
        ok: audit.counts.fail === 0,
        generated_at: audit.generated_at,
        config_path: configPath,
        counts: audit.counts,
        diagnostic_codes: Array.from(new Set(audit.results.flatMap((result) => result.diagnostic_codes))).sort(),
        results: audit.results.map((result) => ({
            crawler_id: result.crawler_id,
            crawler_name: result.crawler_name,
            platform: result.platform,
            status: result.status,
            diagnostic_codes: result.diagnostic_codes,
            static_cookie: result.static_cookie,
            live_probe: result.live_probe,
        })),
    }
}

async function main() {
    const args = parseArgs(process.argv.slice(2))
    const config = readConfig(args.configPath)
    const audit = await buildCrawlerLiveHealthAudit(config, {
        platforms: args.platforms,
        timeoutMs: args.timeoutMs,
        liveProbe: args.liveProbe,
        resolveCookieFile: (cookieFile) => resolveCookieFile(cookieFile, args.configPath),
    })
    const payload = args.format === 'json' ? audit : buildSummary(args.configPath, audit)
    const output = `${JSON.stringify(payload, null, 2)}\n`
    assertNoSensitiveValues(output, config)
    process.stdout.write(output)

    if (args.failOnUnhealthy && audit.counts.fail > 0) {
        process.exit(2)
    }
    if (args.failOnWarn && (audit.counts.fail > 0 || audit.counts.warn > 0)) {
        process.exit(2)
    }
}

main().catch((error) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exit(1)
})
