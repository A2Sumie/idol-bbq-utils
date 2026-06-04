import crypto from 'crypto'
import type { AppConfig } from '@/types'
import { buildRouteGraph, type RouteGraphDiagnostic, type RouteGraphRoute } from './route-graph-service'
import { collectSensitiveConfigPaths, redactSecrets } from './redaction-service'

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

function buildConfigAudit(config: AppConfig): ConfigAudit {
    const redactedConfig = redactSecrets(config)
    const routeGraph = buildRouteGraph(config)
    const policyRoutes = routeGraph.routes.map(projectRoute)
    const summaryCardRoutes = policyRoutes.filter((route) => route.mode.summary_card)
    const sensitivePaths = collectSensitiveConfigPaths(config)

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
    }
}

export { buildConfigAudit, hashStable, stableSerialize, type ConfigAudit }
