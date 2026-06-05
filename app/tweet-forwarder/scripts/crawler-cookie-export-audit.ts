#!/usr/bin/env bun

import EventEmitter from 'events'
import fs from 'fs'
import YAML from 'yaml'
import { normalizePipelinesForRuntime } from '../src/services/quick-config-service'
import { inferCookieHealthPlatform, type CookieHealthPlatform } from '../src/services/crawler-cookie-policy'
import { SENSITIVE_KEY_PATTERN } from '../src/services/redaction-service'
import { SpiderPools } from '../src/managers/spider-manager'
import type { AppConfig, Crawler } from '../src/types'

type OutputFormat = 'summary' | 'json'
type AuditStatus = 'ok' | 'warn' | 'fail' | 'skipped'

type Args = {
    configPath: string
    format: OutputFormat
    platforms: Array<CookieHealthPlatform>
    crawlerFilters: Array<string>
    validateLive: boolean
    seedConfiguredCookieFile: boolean
    visit: boolean
    timeoutMs: number
}

const DEFAULT_PLATFORMS: Array<CookieHealthPlatform> = ['x', 'instagram', 'tiktok']

function usage() {
    return `Usage: bun app/tweet-forwarder/scripts/crawler-cookie-export-audit.ts [--config <path>] [--format summary|json] [--platform x,instagram,tiktok] [--crawler <id-or-name>] [--timeout-ms <ms>] [--no-live-validation] [--seed-configured-cookie-file] [--visit]

Runs a no-secret browser-profile cookie export audit. By default it does not
write cookie files, does not seed from configured cookie files, and does not
navigate browser profiles before reading cookies. Output includes only crawler
names, platform hints, counts, required-name metadata, and live probe status.`
}

function parsePlatforms(value: string): Array<CookieHealthPlatform> {
    const platforms = value
        .split(',')
        .map((item) => item.trim().toLowerCase())
        .filter(Boolean)
    const supported = new Set<CookieHealthPlatform>(['x', 'instagram', 'tiktok', 'youtube', 'website', 'unknown'])
    for (const platform of platforms) {
        if (!supported.has(platform as CookieHealthPlatform)) {
            throw new Error(`Unsupported platform: ${platform}`)
        }
    }
    return platforms as Array<CookieHealthPlatform>
}

function parseArgs(argv: Array<string>): Args {
    const args: Args = {
        configPath: 'config.yaml',
        format: 'summary',
        platforms: DEFAULT_PLATFORMS,
        crawlerFilters: [],
        validateLive: true,
        seedConfiguredCookieFile: false,
        visit: false,
        timeoutMs: 15_000,
    }

    for (let index = 0; index < argv.length; index += 1) {
        const key = argv[index]
        if (key === '--help' || key === '-h') {
            process.stdout.write(`${usage()}\n`)
            process.exit(0)
        }
        if (key === '--no-live-validation') {
            args.validateLive = false
            continue
        }
        if (key === '--seed-configured-cookie-file') {
            args.seedConfiguredCookieFile = true
            continue
        }
        if (key === '--visit') {
            args.visit = true
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
        if (key === '--crawler') {
            const value = argv[index + 1]
            if (!value || value.startsWith('--')) {
                throw new Error('--crawler requires an id or name')
            }
            args.crawlerFilters.push(value)
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

function crawlerId(crawler: Crawler, fallback: string) {
    return String((crawler as any).id || crawler.name || fallback).trim()
}

function crawlerName(crawler: Crawler, fallback: string) {
    return String(crawler.name || (crawler as any).id || fallback).trim()
}

function matchesCrawlerFilter(crawler: Crawler, filters: Array<string>) {
    if (filters.length === 0) {
        return true
    }
    const candidates = new Set([String((crawler as any).id || ''), String(crawler.name || '')].filter(Boolean))
    return filters.some((filter) => candidates.has(filter))
}

function statusFromSnapshot(snapshot: any): AuditStatus {
    const liveStatus = snapshot.liveProbe?.status
    if (liveStatus === 'warn') return 'warn'
    if (liveStatus === 'fail') return 'fail'
    return 'ok'
}

function publicErrorCode(error: unknown) {
    const code = (error as any)?.code
    if (typeof code === 'string' && code.trim()) {
        return code
    }
    return 'crawler_cookie_export_failed'
}

function publicErrorDetails(error: unknown) {
    const details = (error as any)?.publicDetails
    return details && typeof details === 'object' ? (details as Record<string, any>) : {}
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
        throw new Error(`crawler cookie export audit output would leak ${leaked.length} sensitive value(s)`)
    }
}

async function main() {
    const args = parseArgs(process.argv.slice(2))
    const config = readConfig(args.configPath)
    const platformFilter = new Set(args.platforms)
    const results = []
    const pools = new SpiderPools('/tmp/tweet-forwarder/crawler-cookie-export-audit', new EventEmitter())

    try {
        for (const [index, crawler] of (config.crawlers || []).entries()) {
            const platform = inferCookieHealthPlatform(crawler)
            if (!platformFilter.has(platform) || !matchesCrawlerFilter(crawler, args.crawlerFilters)) {
                continue
            }

            const id = crawlerId(crawler, `crawler-${index}`)
            const name = crawlerName(crawler, id)
            try {
                const snapshot = await pools.exportCrawlerCookies(crawler, {
                    validateLiveProbe: args.validateLive,
                    seedConfiguredCookieFile: args.seedConfiguredCookieFile,
                    visit: args.visit,
                    timeoutMs: args.timeoutMs,
                })
                results.push({
                    crawler_id: id,
                    crawler_name: name,
                    platform,
                    status: statusFromSnapshot(snapshot),
                    diagnostic_codes: snapshot.liveProbe?.diagnostic_codes || [],
                    cookie_count: snapshot.cookies.length,
                    session_profile: snapshot.sessionProfile,
                    domains: snapshot.domains,
                    required_cookie_names: snapshot.requiredCookieNames,
                    live_probe: snapshot.liveProbe,
                })
            } catch (error) {
                const details = publicErrorDetails(error)
                const liveProbe = details.live_probe || {
                    checked: false,
                    status: 'skipped',
                    diagnostic_codes: ['live_probe_not_completed'],
                    http_status: null,
                }
                const diagnosticCodes = Array.from(
                    new Set([publicErrorCode(error), ...(Array.isArray(liveProbe.diagnostic_codes) ? liveProbe.diagnostic_codes : [])]),
                ).sort()
                results.push({
                    crawler_id: id,
                    crawler_name: name,
                    platform,
                    status: 'fail',
                    diagnostic_codes: diagnosticCodes,
                    cookie_count: Number(details.cookie_count || 0),
                    session_profile: crawler.cfg_crawler?.session_profile || null,
                    domains: Array.isArray(details.domains) ? details.domains : [],
                    required_cookie_names: details.required_cookie_names || {
                        present: [],
                        missing: [],
                    },
                    live_probe: liveProbe,
                })
            }
        }
    } finally {
        await pools.drop().catch(() => null)
    }

    const payload = {
        ok: results.every((result) => result.status !== 'fail'),
        generated_at: new Date().toISOString(),
        config_path: args.configPath,
        options: {
            validate_live: args.validateLive,
            seed_configured_cookie_file: args.seedConfiguredCookieFile,
            visit: args.visit,
            timeout_ms: args.timeoutMs,
        },
        counts: {
            checked: results.length,
            ok: results.filter((result) => result.status === 'ok').length,
            warn: results.filter((result) => result.status === 'warn').length,
            fail: results.filter((result) => result.status === 'fail').length,
            skipped: results.filter((result) => result.status === 'skipped').length,
        },
        diagnostic_codes: Array.from(new Set(results.flatMap((result) => result.diagnostic_codes))).sort(),
        results,
    }
    const output = `${JSON.stringify(payload, null, 2)}\n`
    assertNoSensitiveValues(output, config)
    process.stdout.write(output)
}

main().catch((error) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exit(1)
})
