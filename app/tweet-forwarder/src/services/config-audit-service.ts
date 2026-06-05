import crypto from 'crypto'
import fs from 'fs'
import { auditNetscapeCookieFile, type NetscapeCookieFileAudit } from '@idol-bbq-utils/spider'
import type { AppConfig } from '@/types'
import { buildRouteGraph, type RouteGraphDiagnostic, type RouteGraphRoute } from './route-graph-service'
import { collectSensitiveConfigPaths, redactSecrets } from './redaction-service'
import {
    inferCookieHealthPlatform,
    summarizeRequiredCookieNames,
    type CookieHealthPlatform,
} from './crawler-cookie-policy'

type ConfigAuditRoute = Pick<
    RouteGraphRoute,
    | 'route_key'
    | 'crawler_id'
    | 'formatter_id'
    | 'target_id'
    | 'target_platform'
    | 'mode'
    | 'policy'
    | 'dedup_contract'
>

type ConfigAuditDiagnostic = Pick<RouteGraphDiagnostic, 'severity' | 'code' | 'route_key'>

type ConfigAuditCookieCrawler = {
    crawler_id: string
    crawler_name: string
    platform_hint: CookieHealthPlatform
    exists: boolean
    status: 'ok' | 'warn' | 'missing' | 'unreadable'
    usable_cookie_count: number
    expired_cookie_count: number
    session_cookie_count: number
    malformed_cookie_count: number
    domains: Array<string>
    cookie_names: Array<string>
    required_cookie_names: {
        present: Array<string>
        missing: Array<string>
    }
}

type ConfigAuditCookieDiagnostic = {
    severity: 'warn'
    code: string
    crawler_id: string
}

type ConfigAuditOptions = {
    now?: number
    resolveCookieFile?: (cookieFile: string) => string | null | undefined
}

type ConfigAudit = {
    generated_at: string
    redacted_config_hash: string
    policy_hash: string
    secret_fields: {
        count: number
        paths: Array<string>
    }
    route_graph: {
        counts: ReturnType<typeof buildRouteGraph>['counts']
        diagnostics: Array<RouteGraphDiagnostic>
        operational_crawlers: ReturnType<typeof buildRouteGraph>['operational_crawlers']
        policy_routes: Array<ConfigAuditRoute>
        summary_card_routes: Array<ConfigAuditRoute>
    }
    cookie_audit: {
        counts: {
            crawlers_with_cookie_files: number
            existing_files: number
            missing_files: number
            unreadable_files: number
            unhealthy_crawlers: number
        }
        diagnostics: Array<ConfigAuditCookieDiagnostic>
        crawlers: Array<ConfigAuditCookieCrawler>
    }
}

function stableSerialize(value: unknown): string {
    if (value === null) {
        return 'null'
    }
    if (Array.isArray(value)) {
        return `[${value.map((item) => stableSerialize(item)).join(',')}]`
    }
    if (typeof value === 'object') {
        const objectValue = value as Record<string, unknown>
        const body = Object.keys(objectValue)
            .sort()
            .filter((key) => objectValue[key] !== undefined)
            .map((key) => `${JSON.stringify(key)}:${stableSerialize(objectValue[key])}`)
            .join(',')
        return `{${body}}`
    }
    return JSON.stringify(value) ?? 'undefined'
}

function hashStable(value: unknown) {
    return crypto.createHash('sha256').update(stableSerialize(value)).digest('hex')
}

function projectRoute(route: RouteGraphRoute): ConfigAuditRoute {
    return {
        route_key: route.route_key,
        crawler_id: route.crawler_id,
        formatter_id: route.formatter_id,
        target_id: route.target_id,
        target_platform: route.target_platform,
        mode: route.mode,
        policy: route.policy,
        dedup_contract: route.dedup_contract,
    }
}

function projectDiagnostic(diagnostic: RouteGraphDiagnostic): ConfigAuditDiagnostic {
    return {
        severity: diagnostic.severity,
        code: diagnostic.code,
        ...(diagnostic.route_key ? { route_key: diagnostic.route_key } : {}),
    }
}

function buildPolicyHashInput(routes: Array<ConfigAuditRoute>, diagnostics: Array<RouteGraphDiagnostic>) {
    return {
        routes: [...routes].sort((left, right) => left.route_key.localeCompare(right.route_key)),
        diagnostics: diagnostics
            .map(projectDiagnostic)
            .sort(
                (left, right) =>
                    left.severity.localeCompare(right.severity) ||
                    left.code.localeCompare(right.code) ||
                    String(left.route_key || '').localeCompare(String(right.route_key || '')),
            ),
    }
}

function nodeId(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.id || value?.name || fallback).trim()
}

function nodeName(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.name || value?.id || fallback).trim()
}

function emptyCookieMetadata(): NetscapeCookieFileAudit {
    return {
        total_cookie_rows: 0,
        usable_cookie_count: 0,
        expired_cookie_count: 0,
        session_cookie_count: 0,
        malformed_cookie_count: 0,
        http_only_cookie_count: 0,
        domains: [],
        cookie_names: [],
    }
}

function buildCookieAudit(config: AppConfig, options: ConfigAuditOptions = {}): ConfigAudit['cookie_audit'] {
    const crawlers = [] as Array<ConfigAuditCookieCrawler>
    const diagnostics = [] as Array<ConfigAuditCookieDiagnostic>

    for (const [index, crawler] of (config.crawlers || []).entries()) {
        const cookieFile = crawler.cfg_crawler?.cookie_file
        if (!cookieFile) {
            continue
        }

        const crawlerId = nodeId(crawler, `crawler-${index}`)
        const crawlerName = nodeName(crawler, crawlerId)
        const platform = inferCookieHealthPlatform(crawler)
        let metadata = emptyCookieMetadata()
        let exists = false
        let status: ConfigAuditCookieCrawler['status'] = 'missing'

        try {
            const resolvedCookieFile = options.resolveCookieFile?.(cookieFile) ?? cookieFile
            if (resolvedCookieFile) {
                exists = fs.existsSync(resolvedCookieFile)
                if (exists) {
                    metadata = auditNetscapeCookieFile(resolvedCookieFile, { now: options.now })
                    status = 'ok'
                }
            }
        } catch {
            status = 'unreadable'
        }

        const { present, missing } = summarizeRequiredCookieNames(platform, metadata.cookie_names)
        if (status === 'ok' && (metadata.usable_cookie_count === 0 || missing.length > 0 || metadata.malformed_cookie_count > 0)) {
            status = 'warn'
        }

        if (status !== 'ok') {
            const code =
                status === 'missing'
                    ? 'cookie_file_missing'
                    : status === 'unreadable'
                      ? 'cookie_file_unreadable'
                      : missing.length > 0
                        ? 'cookie_required_names_missing'
                        : metadata.usable_cookie_count === 0
                          ? 'cookie_file_has_no_usable_rows'
                          : 'cookie_file_has_malformed_rows'
            diagnostics.push({
                severity: 'warn',
                code,
                crawler_id: crawlerId,
            })
        }

        crawlers.push({
            crawler_id: crawlerId,
            crawler_name: crawlerName,
            platform_hint: platform,
            exists,
            status,
            usable_cookie_count: metadata.usable_cookie_count,
            expired_cookie_count: metadata.expired_cookie_count,
            session_cookie_count: metadata.session_cookie_count,
            malformed_cookie_count: metadata.malformed_cookie_count,
            domains: metadata.domains,
            cookie_names: metadata.cookie_names,
            required_cookie_names: {
                present,
                missing,
            },
        })
    }

    return {
        counts: {
            crawlers_with_cookie_files: crawlers.length,
            existing_files: crawlers.filter((crawler) => crawler.exists).length,
            missing_files: crawlers.filter((crawler) => crawler.status === 'missing').length,
            unreadable_files: crawlers.filter((crawler) => crawler.status === 'unreadable').length,
            unhealthy_crawlers: crawlers.filter((crawler) => crawler.status !== 'ok').length,
        },
        diagnostics,
        crawlers,
    }
}

function buildConfigAudit(config: AppConfig, options: ConfigAuditOptions = {}): ConfigAudit {
    const redactedConfig = redactSecrets(config)
    const routeGraph = buildRouteGraph(config)
    const policyRoutes = routeGraph.routes.map(projectRoute)
    const summaryCardRoutes = policyRoutes.filter((route) => route.mode.summary_card)
    const sensitivePaths = collectSensitiveConfigPaths(config)
    const cookieAudit = buildCookieAudit(config, options)

    return {
        generated_at: new Date().toISOString(),
        redacted_config_hash: hashStable(redactedConfig),
        policy_hash: hashStable(buildPolicyHashInput(policyRoutes, routeGraph.diagnostics)),
        secret_fields: {
            count: sensitivePaths.length,
            paths: sensitivePaths,
        },
        route_graph: {
            counts: routeGraph.counts,
            diagnostics: routeGraph.diagnostics,
            operational_crawlers: routeGraph.operational_crawlers,
            policy_routes: policyRoutes,
            summary_card_routes: summaryCardRoutes,
        },
        cookie_audit: cookieAudit,
    }
}

export { buildConfigAudit, hashStable, stableSerialize, type ConfigAudit }
