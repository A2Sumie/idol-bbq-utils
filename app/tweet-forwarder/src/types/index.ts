import type { TaskType } from '@idol-bbq-utils/spider/types'
import type { Crawler, CrawlerConfig } from './crawler'
import type { Forwarder, ForwarderConfig, ForwardTarget, ForwardTargetPlatformCommonConfig } from './forwarder'
import type { Formatter, ConnectionMap } from './formatter'
import type { Processor } from './processor'

export * from './processor'
export * from './crawler'
export * from './forwarder'
export * from './formatter'
export * from './common'

/**
 * only crawling or forwarding or both
 */
interface AppConfig {
    crawlers?: Array<Crawler>
    cfg_crawler?: CrawlerConfig
    processors?: Array<Processor>
    formatters?: Array<Formatter>
    pipelines?: Array<{
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
    }>
    forward_targets?: Array<ForwardTarget>
    cfg_forward_target?: ForwardTargetPlatformCommonConfig
    forwarders?: Array<Forwarder<TaskType>>
    cfg_forwarder?: ForwarderConfig
    connections?: ConnectionMap
    api?: {
        port?: number
        secret?: string
        cors_origin?: string | Array<string>
    }
}

export type { AppConfig }
