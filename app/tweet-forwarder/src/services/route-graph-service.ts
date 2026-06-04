import type { AppConfig, ForwardTargetPlatformCommonConfig } from '@/types'
import { routeKey } from './outbound-message-service'

type RouteModeFlags = {
    realtime: boolean
    digest: boolean
    summary_card: boolean
    tag_digest: boolean
    media_batch: boolean
    aggregation: boolean
}

type RouteGraphRoute = {
    route_key: string
    crawler_id: string
    crawler_name: string
    formatter_id: string
    formatter_name: string
    target_id: string
    target_name: string
    target_platform: string
    mode: RouteModeFlags
    dedup_contract: Array<'article' | 'payload' | 'window' | 'media'>
}

type RouteGraphDiagnostic = {
    severity: 'warn' | 'error'
    code: string
    message: string
    route_key?: string
}

function nodeId(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.id || value?.name || fallback).trim()
}

function nodeName(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.name || value?.id || fallback).trim()
}

function lookupArray(map: Record<string, Array<string>> | undefined, keys: Array<string | undefined>) {
    if (!map) {
        return []
    }
    for (const key of keys) {
        if (key && Object.prototype.hasOwnProperty.call(map, key)) {
            return Array.from(new Set(map[key] || []))
        }
    }
    return []
}

function lookupValue(map: Record<string, string> | undefined, keys: Array<string | undefined>) {
    if (!map) {
        return undefined
    }
    for (const key of keys) {
        if (key && Object.prototype.hasOwnProperty.call(map, key)) {
            return map[key]
        }
    }
    return undefined
}

function resolveMode(formatter: any, targetConfig: ForwardTargetPlatformCommonConfig = {}): RouteModeFlags {
    return {
        realtime: !formatter?.aggregation && !(targetConfig as any)?.batch_mode,
        digest: Number(targetConfig.digest_threshold || 0) >= 2,
        summary_card: Boolean(
            targetConfig.summary_card === true ||
                (typeof targetConfig.summary_card === 'object' && targetConfig.summary_card?.enabled !== false),
        ),
        tag_digest: Number(targetConfig.tag_digest_threshold || 0) >= 2,
        media_batch: Number(targetConfig.media_batch_threshold || 0) >= 2,
        aggregation: formatter?.aggregation === true,
    }
}

function dedupContract(mode: RouteModeFlags, formatter: any) {
    const contracts: Array<'article' | 'payload' | 'window' | 'media'> = ['article', 'payload']
    if (mode.digest || mode.summary_card || mode.tag_digest || mode.aggregation) {
        contracts.push('window')
    }
    if (mode.media_batch || formatter?.deduplication !== false) {
        contracts.push('media')
    }
    return Array.from(new Set(contracts))
}

function buildRouteGraph(config: AppConfig) {
    const diagnostics: RouteGraphDiagnostic[] = []
    const routes: RouteGraphRoute[] = []
    const crawlers = config.crawlers || []
    const formatters = config.formatters || []
    const targets = config.forward_targets || []
    const formatterTargetMap = config.connections?.['formatter-target'] || {}
    const targetById = new Map(targets.map((target, index) => [nodeId(target, `target-${index}`), target]))
    const formatterById = new Map(
        formatters.map((formatter, index) => [nodeId(formatter, `formatter-${index}`), formatter]),
    )

    for (const [crawlerIndex, crawler] of crawlers.entries()) {
        const crawlerId = nodeId(crawler, `crawler-${crawlerIndex}`)
        const crawlerName = nodeName(crawler, crawlerId)
        const keys = [(crawler as any).id, crawler.name]
        const directFormatterIds = lookupArray(config.connections?.['crawler-formatter'], keys)
        const processorId = lookupValue(config.connections?.['crawler-processor'], keys)
        const processorFormatterIds = processorId
            ? config.connections?.['processor-formatter']?.[processorId] || []
            : []
        const formatterIds = Array.from(new Set([...directFormatterIds, ...processorFormatterIds]))

        if (formatterIds.length === 0) {
            diagnostics.push({
                severity: 'warn',
                code: 'crawler_without_formatter',
                message: `${crawlerName} has no connected formatter`,
            })
        }

        const targetSeen = new Map<string, string>()
        for (const formatterId of formatterIds) {
            const formatter = formatterById.get(formatterId)
            if (!formatter) {
                diagnostics.push({
                    severity: 'error',
                    code: 'missing_formatter',
                    message: `${crawlerName} references missing formatter ${formatterId}`,
                })
                continue
            }
            const targetIds = formatterTargetMap[formatterId] || []
            if (targetIds.length === 0) {
                diagnostics.push({
                    severity: 'warn',
                    code: 'formatter_without_target',
                    message: `${formatterId} has no connected target`,
                })
            }
            for (const targetId of targetIds) {
                const target = targetById.get(targetId)
                if (!target) {
                    diagnostics.push({
                        severity: 'error',
                        code: 'missing_target',
                        message: `${formatterId} references missing target ${targetId}`,
                    })
                    continue
                }
                const targetConfig = {
                    ...config.cfg_forward_target,
                    ...target.cfg_platform,
                } as ForwardTargetPlatformCommonConfig
                const baseRouteKey = routeKey({
                    source: 'graph',
                    crawlerId,
                    formatterId,
                    targetId,
                })
                if (targetSeen.has(targetId)) {
                    diagnostics.push({
                        severity: 'error',
                        code: 'duplicate_target_for_crawler',
                        message: `${crawlerName} reaches ${targetId} through both ${targetSeen.get(targetId)} and ${formatterId}`,
                        route_key: baseRouteKey,
                    })
                }
                targetSeen.set(targetId, formatterId)
                const mode = resolveMode(formatter, targetConfig)
                routes.push({
                    route_key: baseRouteKey,
                    crawler_id: crawlerId,
                    crawler_name: crawlerName,
                    formatter_id: formatterId,
                    formatter_name: nodeName(formatter, formatterId),
                    target_id: targetId,
                    target_name: nodeName(target, targetId),
                    target_platform: String(target.platform),
                    mode,
                    dedup_contract: dedupContract(mode, formatter),
                })
            }
        }
    }

    return {
        generated_at: new Date().toISOString(),
        counts: {
            crawlers: crawlers.length,
            formatters: formatters.length,
            targets: targets.length,
            routes: routes.length,
            errors: diagnostics.filter((item) => item.severity === 'error').length,
            warnings: diagnostics.filter((item) => item.severity === 'warn').length,
        },
        routes,
        diagnostics,
    }
}

export { buildRouteGraph, type RouteGraphDiagnostic, type RouteGraphRoute }
