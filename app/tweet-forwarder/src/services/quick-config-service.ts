import type { AppConfig } from '@/types'
import { platformNameMap } from '@idol-bbq-utils/spider/const'

type QuickNodeType = 'crawler' | 'processor' | 'formatter' | 'target'
type QuickLinkKind = 'crawler-processor' | 'processor-formatter' | 'crawler-formatter' | 'formatter-target'

type QuickConfigLink = {
    id?: string
    kind: QuickLinkKind
    from_type?: QuickNodeType
    from: string
    to_type?: QuickNodeType
    to: string
    order?: number
}

type QuickConfigRoute = {
    crawler_id: string
    processor_id?: string | null
    formatter_ids?: Array<string>
    formatter_targets?: Record<string, Array<string>>
}

type QuickConfigPipelinePatch = {
    id?: string
    name?: string
    enabled?: boolean
    source: {
        crawler_id: string
    }
    processors?: Array<{
        processor_id: string
        role?: string
    }>
    formatters?: Array<{
        formatter_id: string
    }>
    delivery?: Array<{
        formatter_id: string
        target_ids: Array<string>
    }>
}

type QuickConfigPatch = {
    links?: Array<QuickConfigLink>
    routes?: Array<QuickConfigRoute>
    pipelines?: Array<QuickConfigPipelinePatch>
    strict?: boolean
}

const QUICK_CONFIG_SCHEMA = 'idol-bbq.quick-config.v1'

type QuickConfigDiagnostic = { severity: 'warn' | 'error'; code: string; message: string; link?: QuickConfigLink; pipeline_id?: string }

function emptyConnections() {
    return {
        'crawler-processor': {} as Record<string, string>,
        'processor-formatter': {} as Record<string, Array<string>>,
        'crawler-formatter': {} as Record<string, Array<string>>,
        'formatter-target': {} as Record<string, Array<string>>,
    }
}

function unique(values: Array<string | undefined | null>) {
    return Array.from(new Set(values.map((value) => String(value || '').trim()).filter(Boolean)))
}

function nodeId(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.id || value?.name || fallback).trim()
}

function nodeName(value: { id?: string; name?: string } | undefined, fallback: string) {
    return String(value?.name || value?.id || fallback).trim()
}

function mapValues(map: Record<string, Array<string>> | undefined, keys: Array<string | undefined>) {
    if (!map) {
        return []
    }
    for (const key of keys) {
        if (key && Object.prototype.hasOwnProperty.call(map, key)) {
            return unique(map[key] || [])
        }
    }
    return []
}

function mapValue(map: Record<string, string> | undefined, keys: Array<string | undefined>) {
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

function makeLink(kind: QuickLinkKind, from: string, to: string, order = 0): QuickConfigLink {
    const [fromType, toType] = kind.split('-') as [QuickNodeType, QuickNodeType]
    return {
        id: `${kind}:${from}->${to}`,
        kind,
        from_type: fromType,
        from,
        to_type: toType,
        to,
        order,
    }
}

function hasCompiledConnections(config: AppConfig) {
    const connections = config.connections || {}
    return ['crawler-processor', 'processor-formatter', 'crawler-formatter', 'formatter-target'].some(
        (key) => Object.keys((connections as any)[key] || {}).length > 0,
    )
}

function pipelineEnabled(pipeline: QuickConfigPipelinePatch) {
    return pipeline.enabled !== false
}

function collectLinks(config: AppConfig) {
    const pipelines = (config as any).pipelines
    if (!hasCompiledConnections(config) && Array.isArray(pipelines) && pipelines.length > 0) {
        return linksFromPipelines(pipelines)
    }

    const links: Array<QuickConfigLink> = []
    const connections = config.connections || {}
    for (const [crawler, processor] of Object.entries(connections['crawler-processor'] || {})) {
        if (processor) {
            links.push(makeLink('crawler-processor', crawler, processor))
        }
    }
    for (const [processor, formatterIds] of Object.entries(connections['processor-formatter'] || {})) {
        unique(formatterIds).forEach((formatter, order) => links.push(makeLink('processor-formatter', processor, formatter, order)))
    }
    for (const [crawler, formatterIds] of Object.entries(connections['crawler-formatter'] || {})) {
        unique(formatterIds).forEach((formatter, order) => links.push(makeLink('crawler-formatter', crawler, formatter, order)))
    }
    for (const [formatter, targetIds] of Object.entries(connections['formatter-target'] || {})) {
        unique(targetIds).forEach((target, order) => links.push(makeLink('formatter-target', formatter, target, order)))
    }
    return links
}

function buildNodeSets(config: AppConfig) {
    return {
        crawlers: new Set((config.crawlers || []).map((crawler, index) => nodeId(crawler, `crawler-${index}`))),
        processors: new Set((config.processors || []).map((processor, index) => nodeId(processor, `processor-${index}`))),
        formatters: new Set((config.formatters || []).map((formatter, index) => nodeId(formatter, `formatter-${index}`))),
        targets: new Set((config.forward_targets || []).map((target, index) => nodeId(target, `target-${index}`))),
    }
}

function diagnoseQuickConfig(config: AppConfig, links = collectLinks(config)) {
    const nodeSets = buildNodeSets(config)
    const diagnostics: Array<QuickConfigDiagnostic> = []
    const has = (type: QuickNodeType, id: string) => {
        if (type === 'crawler') return nodeSets.crawlers.has(id)
        if (type === 'processor') return nodeSets.processors.has(id)
        if (type === 'formatter') return nodeSets.formatters.has(id)
        return nodeSets.targets.has(id)
    }
    for (const link of links) {
        const [fromType, toType] = link.kind.split('-') as [QuickNodeType, QuickNodeType]
        if (!has(fromType, link.from)) {
            diagnostics.push({
                severity: 'error',
                code: 'unknown_from_node',
                message: `${link.kind} references missing ${fromType}: ${link.from}`,
                link,
            })
        }
        if (!has(toType, link.to)) {
            diagnostics.push({
                severity: 'error',
                code: 'unknown_to_node',
                message: `${link.kind} references missing ${toType}: ${link.to}`,
                link,
            })
        }
    }
    return diagnostics
}

function diagnosePipelines(config: AppConfig, pipelines: Array<QuickConfigPipelinePatch>) {
    const nodeSets = buildNodeSets(config)
    const diagnostics: Array<QuickConfigDiagnostic> = []

    for (const [index, pipeline] of pipelines.entries()) {
        const pipelineId = pipeline.id || pipeline.source?.crawler_id || `pipeline-${index}`
        if (!pipelineEnabled(pipeline)) {
            continue
        }
        const crawlerId = pipeline.source?.crawler_id
        if (!crawlerId) {
            diagnostics.push({
                severity: 'error',
                code: 'pipeline_missing_source',
                message: `${pipelineId} is missing source.crawler_id`,
                pipeline_id: pipelineId,
            })
        } else if (!nodeSets.crawlers.has(crawlerId)) {
            diagnostics.push({
                severity: 'error',
                code: 'pipeline_unknown_crawler',
                message: `${pipelineId} references missing crawler: ${crawlerId}`,
                pipeline_id: pipelineId,
            })
        }

        const processorIds = unique((pipeline.processors || []).map((processor) => processor.processor_id))
        for (const processorId of processorIds) {
            if (!nodeSets.processors.has(processorId)) {
                diagnostics.push({
                    severity: 'error',
                    code: 'pipeline_unknown_processor',
                    message: `${pipelineId} references missing processor: ${processorId}`,
                    pipeline_id: pipelineId,
                })
            }
        }
        if (processorIds.length > 1) {
            diagnostics.push({
                severity: 'warn',
                code: 'pipeline_extra_processors_ignored',
                message: `${pipelineId} has ${processorIds.length} processors; current runtime compiles only the first processor (${processorIds[0]})`,
                pipeline_id: pipelineId,
            })
        }

        const formatterIds = unique([
            ...(pipeline.formatters || []).map((formatter) => formatter.formatter_id),
            ...(pipeline.delivery || []).map((delivery) => delivery.formatter_id),
        ])
        for (const formatterId of formatterIds) {
            if (!nodeSets.formatters.has(formatterId)) {
                diagnostics.push({
                    severity: 'error',
                    code: 'pipeline_unknown_formatter',
                    message: `${pipelineId} references missing formatter: ${formatterId}`,
                    pipeline_id: pipelineId,
                })
            }
        }

        const targetIds = unique((pipeline.delivery || []).flatMap((delivery) => delivery.target_ids || []))
        for (const targetId of targetIds) {
            if (!nodeSets.targets.has(targetId)) {
                diagnostics.push({
                    severity: 'error',
                    code: 'pipeline_unknown_target',
                    message: `${pipelineId} references missing target: ${targetId}`,
                    pipeline_id: pipelineId,
                })
            }
        }
    }

    return diagnostics
}

function buildQuickConfigModel(config: AppConfig) {
    const configuredPipelines = Array.isArray((config as any).pipelines) ? ((config as any).pipelines as Array<QuickConfigPipelinePatch>) : []
    let runtimeConfig = config
    if (configuredPipelines.length > 0) {
        try {
            runtimeConfig = normalizePipelinesForRuntime(config)
        } catch (error) {
            if (!String(error instanceof Error ? error.message : error).includes('at least one enabled pipeline')) {
                throw error
            }
            runtimeConfig = { ...config, connections: emptyConnections() }
        }
    }
    const connections = runtimeConfig.connections || {}
    const formatterTargets = connections['formatter-target'] || {}
    const processorFormatters = connections['processor-formatter'] || {}
    const links = collectLinks(runtimeConfig)
    const routes = (runtimeConfig.crawlers || []).map((crawler, index) => {
        const crawlerId = nodeId(crawler, `crawler-${index}`)
        const crawlerKeys = [crawlerId, crawler.name]
        const processorId = mapValue(connections['crawler-processor'], crawlerKeys) || crawler.cfg_crawler?.processor_id || null
        const directFormatters = mapValues(connections['crawler-formatter'], crawlerKeys)
        const processorDrivenFormatters = processorId ? mapValues(processorFormatters, [processorId]) : []
        const formatterIds = unique([...directFormatters, ...processorDrivenFormatters])
        const routeFormatterTargets = Object.fromEntries(
            formatterIds.map((formatterId) => [formatterId, unique(formatterTargets[formatterId] || [])]),
        )
        return {
            id: crawlerId,
            crawler_id: crawlerId,
            crawler_name: crawler.name || crawlerId,
            group: crawler.group || '',
            task_type: crawler.task_type || 'article',
            cron: crawler.cfg_crawler?.cron || config.cfg_crawler?.cron || null,
            processor_id: processorId,
            formatter_ids: formatterIds,
            target_ids: unique(Object.values(routeFormatterTargets).flat()),
            formatter_targets: routeFormatterTargets,
        }
    })

    const processorsById = new Map((runtimeConfig.processors || []).map((processor, index) => [nodeId(processor, `processor-${index}`), processor]))
    const formattersById = new Map((runtimeConfig.formatters || []).map((formatter, index) => [nodeId(formatter, `formatter-${index}`), formatter]))
    const targetsById = new Map((runtimeConfig.forward_targets || []).map((target, index) => [nodeId(target, `target-${index}`), target]))
    const routePipelines = routes.map((route) => {
        const processors = route.processor_id
            ? [
                  {
                      processor_id: route.processor_id,
                      role: processorsById.get(route.processor_id)?.cfg_processor?.action || 'process',
                      model_id: processorsById.get(route.processor_id)?.cfg_processor?.model_id || null,
                  },
              ]
            : []
        return {
            id: `pipeline:${route.crawler_id}`,
            name: route.crawler_name,
            enabled: true,
            source: {
                crawler_id: route.crawler_id,
                cron: route.cron,
                task_type: route.task_type,
            },
            processors,
            formatters: route.formatter_ids.map((formatterId) => ({
                formatter_id: formatterId,
                render_type: formattersById.get(formatterId)?.render_type || 'text',
            })),
            delivery: route.formatter_ids.map((formatterId) => ({
                formatter_id: formatterId,
                target_ids: route.formatter_targets[formatterId] || [],
                targets: (route.formatter_targets[formatterId] || []).map((targetId) => {
                    const target = targetsById.get(targetId)
                    return {
                        target_id: targetId,
                        platform: target?.platform || 'unknown',
                        group_id: (target?.cfg_platform as any)?.group_id || null,
                    }
                }),
            })),
            review: {
                summary: `${route.crawler_name} -> ${processors.length} processor(s) -> ${route.formatter_ids.length} formatter(s) -> ${route.target_ids.length} target(s)`,
                warnings: route.formatter_ids.length === 0 || route.target_ids.length === 0
                    ? ['pipeline has no formatter or no delivery target']
                    : [],
            },
        }
    })
    const pipelinesByCrawler = new Map(routePipelines.map((pipeline) => [pipeline.source.crawler_id, pipeline]))
    const pipelines = configuredPipelines.length > 0
        ? configuredPipelines.map((pipeline, index) => {
              const crawlerId = pipeline.source?.crawler_id || ''
              const compiled = pipelinesByCrawler.get(crawlerId)
              const processors = (pipeline.processors || compiled?.processors || []).map((processor) => ({
                  processor_id: processor.processor_id,
                  role: processor.role || processorsById.get(processor.processor_id)?.cfg_processor?.action || 'process',
                  model_id: processorsById.get(processor.processor_id)?.cfg_processor?.model_id || null,
              }))
              const formatterIds = unique([
                  ...(pipeline.formatters || []).map((formatter) => formatter.formatter_id),
                  ...(pipeline.delivery || []).map((delivery) => delivery.formatter_id),
                  ...((compiled?.formatters || []).map((formatter) => formatter.formatter_id)),
              ])
              const deliveryByFormatter = new Map((pipeline.delivery || compiled?.delivery || []).map((delivery) => [delivery.formatter_id, delivery]))
              const targetIds = unique(Array.from(deliveryByFormatter.values()).flatMap((delivery) => delivery.target_ids || []))
              const warnings = [
                  ...(pipelineEnabled(pipeline) ? [] : ['pipeline is disabled and will not be compiled into runtime connections']),
                  ...(processors.length > 1 ? [`current runtime compiles only the first processor (${processors[0]?.processor_id || 'none'})`] : []),
                  ...(formatterIds.length === 0 || targetIds.length === 0 ? ['pipeline has no formatter or no delivery target'] : []),
              ]

              return {
                  id: pipeline.id || compiled?.id || `pipeline:${crawlerId || index}`,
                  name: pipeline.name || compiled?.name || crawlerId || `Pipeline ${index + 1}`,
                  enabled: pipelineEnabled(pipeline),
                  source: {
                      crawler_id: crawlerId,
                      cron: compiled?.source.cron || null,
                      task_type: compiled?.source.task_type || 'article',
                  },
                  processors,
                  compiled_processor_id: processors[0]?.processor_id || null,
                  formatters: formatterIds.map((formatterId) => ({
                      formatter_id: formatterId,
                      render_type: formattersById.get(formatterId)?.render_type || 'text',
                  })),
                  delivery: formatterIds.map((formatterId) => {
                      const delivery = deliveryByFormatter.get(formatterId)
                      const deliveryTargetIds = unique(delivery?.target_ids || [])
                      return {
                          formatter_id: formatterId,
                          target_ids: deliveryTargetIds,
                          targets: deliveryTargetIds.map((targetId) => {
                              const target = targetsById.get(targetId)
                              return {
                                  target_id: targetId,
                                  platform: target?.platform || 'unknown',
                                  group_id: (target?.cfg_platform as any)?.group_id || null,
                              }
                          }),
                      }
                  }),
                  review: {
                      summary: `${pipeline.name || compiled?.name || crawlerId || 'pipeline'} -> ${processors.length} processor(s) -> ${formatterIds.length} formatter(s) -> ${targetIds.length} target(s)`,
                      warnings,
                  },
              }
          })
        : routePipelines

    return {
        schema: QUICK_CONFIG_SCHEMA,
        generated_at: new Date().toISOString(),
        model: {
            primary: 'pipelines',
            compatibility: 'compiled-to-existing-connections',
            notes: [
                'Use pipelines for CIC and LLM-assisted edits.',
                'routes and links are compatibility views over the existing runtime config.',
                'Runtime behavior is unchanged until the compiled config is saved and hot reloaded.',
            ],
        },
        catalogs: {
            crawlers: (runtimeConfig.crawlers || []).map((crawler, index) => ({
                id: nodeId(crawler, `crawler-${index}`),
                name: nodeName(crawler, `crawler-${index}`),
                group: crawler.group || '',
                task_type: crawler.task_type || 'article',
                origin: crawler.origin || null,
                websites: crawler.websites || [],
                paths: crawler.paths || [],
                cron: crawler.cfg_crawler?.cron || config.cfg_crawler?.cron || null,
                browser_mode: crawler.cfg_crawler?.browser_mode || config.cfg_crawler?.browser_mode || null,
                device_profile: crawler.cfg_crawler?.device_profile || config.cfg_crawler?.device_profile || null,
                session_profile: crawler.cfg_crawler?.session_profile || config.cfg_crawler?.session_profile || null,
                interval_time: crawler.cfg_crawler?.interval_time || config.cfg_crawler?.interval_time || null,
                crawl_budget: {
                    max_list_pages: (crawler.cfg_crawler as any)?.max_list_pages ?? null,
                    max_detail_count: (crawler.cfg_crawler as any)?.max_detail_count ?? null,
                    detail_interval_time: (crawler.cfg_crawler as any)?.detail_interval_time ?? null,
                    block_resource_types: (crawler.cfg_crawler as any)?.block_resource_types || [],
                },
                low_signature: {
                    persistent_session: Boolean(crawler.cfg_crawler?.session_profile || config.cfg_crawler?.session_profile),
                    cookie_file: Boolean(crawler.cfg_crawler?.cookie_file || config.cfg_crawler?.cookie_file),
                    browser_profile: crawler.cfg_crawler?.device_profile || config.cfg_crawler?.device_profile || null,
                    resource_blocking: ((crawler.cfg_crawler as any)?.block_resource_types || []).length > 0,
                },
                enabled: true,
            })),
            processors: (runtimeConfig.processors || []).map((processor, index) => ({
                id: nodeId(processor, `processor-${index}`),
                name: nodeName(processor, `processor-${index}`),
                group: processor.group || '',
                provider: processor.provider,
                action: processor.cfg_processor?.action || null,
                model_id: processor.cfg_processor?.model_id || null,
            })),
            formatters: (runtimeConfig.formatters || []).map((formatter, index) => ({
                id: nodeId(formatter, `formatter-${index}`),
                name: nodeName(formatter, `formatter-${index}`),
                group: formatter.group || '',
                render_type: formatter.render_type,
                aggregation: formatter.aggregation === true,
                deduplication: formatter.deduplication !== false,
                render_features: (formatter as any).render_features || [],
                card_features: (formatter as any).card_features || [],
            })),
            targets: (runtimeConfig.forward_targets || []).map((target, index) => ({
                id: nodeId(target, `target-${index}`),
                name: nodeName(target, `target-${index}`),
                group: target.group || '',
                platform: target.platform,
                platform_label: platformNameMap[target.platform as any] || String(target.platform),
                group_id: (target.cfg_platform as any)?.group_id || null,
                noise_profile: (target.cfg_platform as any)?.group_id === '742435777' ? 'high-realtime' : 'normal',
                digest_threshold: (target.cfg_platform as any)?.digest_threshold ?? null,
                tag_digest_threshold: (target.cfg_platform as any)?.tag_digest_threshold ?? null,
                collapse_forwarded_ref_text: (target.cfg_platform as any)?.collapse_forwarded_ref_text ?? null,
            })),
        },
        pipelines,
        routes,
        links,
        diagnostics: [...diagnoseQuickConfig(runtimeConfig, links), ...diagnosePipelines(runtimeConfig, configuredPipelines)],
    }
}

function exportPipelineConfigs(config: AppConfig): Array<QuickConfigPipelinePatch & { id: string; name: string; enabled: boolean }> {
    return buildQuickConfigModel(config).pipelines.map((pipeline) => ({
        id: pipeline.id,
        name: pipeline.name,
        enabled: pipeline.enabled,
        source: {
            crawler_id: pipeline.source.crawler_id,
        },
        processors: pipeline.processors.map((processor) => ({
            processor_id: processor.processor_id,
            role: processor.role,
        })),
        formatters: pipeline.formatters.map((formatter) => ({
            formatter_id: formatter.formatter_id,
        })),
        delivery: pipeline.delivery.map((delivery) => ({
            formatter_id: delivery.formatter_id,
            target_ids: delivery.target_ids,
        })),
    }))
}

function pushMapArray(map: Record<string, Array<string>>, key: string, value: string) {
    if (!map[key]) {
        map[key] = []
    }
    if (!map[key].includes(value)) {
        map[key].push(value)
    }
}

function linksFromRoutes(routes: Array<QuickConfigRoute>) {
    const links: Array<QuickConfigLink> = []
    for (const route of routes) {
        if (route.processor_id) {
            links.push(makeLink('crawler-processor', route.crawler_id, route.processor_id))
        }
        for (const [order, formatterId] of (route.formatter_ids || []).entries()) {
            links.push(makeLink('crawler-formatter', route.crawler_id, formatterId, order))
        }
        for (const [formatterId, targetIds] of Object.entries(route.formatter_targets || {})) {
            unique(targetIds).forEach((targetId, order) => links.push(makeLink('formatter-target', formatterId, targetId, order)))
        }
    }
    return links
}

function linksFromPipelines(pipelines: Array<QuickConfigPipelinePatch>) {
    const links: Array<QuickConfigLink> = []
    for (const pipeline of pipelines) {
        if (!pipelineEnabled(pipeline)) {
            continue
        }
        const crawlerId = pipeline.source?.crawler_id
        if (!crawlerId) {
            continue
        }
        const processorIds = unique((pipeline.processors || []).map((processor) => processor.processor_id))
        const primaryProcessorId = processorIds[0]
        if (primaryProcessorId) {
            links.push(makeLink('crawler-processor', crawlerId, primaryProcessorId))
        }

        const formatterIds = unique([
            ...(pipeline.formatters || []).map((formatter) => formatter.formatter_id),
            ...(pipeline.delivery || []).map((entry) => entry.formatter_id),
        ])
        formatterIds.forEach((formatterId, order) => {
            links.push(makeLink(primaryProcessorId ? 'processor-formatter' : 'crawler-formatter', primaryProcessorId || crawlerId, formatterId, order))
        })
        for (const delivery of pipeline.delivery || []) {
            unique(delivery.target_ids || []).forEach((targetId, order) => {
                links.push(makeLink('formatter-target', delivery.formatter_id, targetId, order))
            })
        }
    }
    return links
}

function compileConnectionsFromQuickPatch(config: AppConfig, patch: QuickConfigPatch) {
    const links = [...(patch.links || []), ...linksFromRoutes(patch.routes || []), ...linksFromPipelines(patch.pipelines || [])]
    if (links.length === 0) {
        throw new Error('quick config update requires links, routes, or at least one enabled pipeline')
    }

    const diagnostics = [...diagnoseQuickConfig(config, links), ...diagnosePipelines(config, patch.pipelines || [])]
    if (patch.strict !== false && diagnostics.some((item) => item.severity === 'error')) {
        throw new Error(diagnostics.map((item) => item.message).join('; '))
    }

    const nextConnections = {
        ...(config.connections || {}),
        ...emptyConnections(),
    }

    const orderedLinks = links.slice().sort((a, b) => (a.order || 0) - (b.order || 0))
    for (const link of orderedLinks) {
        if (link.kind === 'crawler-processor') {
            nextConnections['crawler-processor'][link.from] = link.to
        } else if (link.kind === 'processor-formatter') {
            pushMapArray(nextConnections['processor-formatter'], link.from, link.to)
        } else if (link.kind === 'crawler-formatter') {
            pushMapArray(nextConnections['crawler-formatter'], link.from, link.to)
        } else if (link.kind === 'formatter-target') {
            pushMapArray(nextConnections['formatter-target'], link.from, link.to)
        }
    }

    return {
        ...config,
        connections: nextConnections,
    }
}

function normalizePipelinesForRuntime(config: AppConfig) {
    const pipelines = (config as any).pipelines
    if (!Array.isArray(pipelines) || pipelines.length === 0) {
        return config
    }
    return compileConnectionsFromQuickPatch(config, {
        pipelines,
        strict: true,
    })
}

export {
    QUICK_CONFIG_SCHEMA,
    buildQuickConfigModel,
    compileConnectionsFromQuickPatch,
    diagnoseQuickConfig,
    exportPipelineConfigs,
    normalizePipelinesForRuntime,
    type QuickConfigLink,
    type QuickConfigPatch,
    type QuickConfigPipelinePatch,
    type QuickConfigRoute,
}
