import type { TaskType } from '@idol-bbq-utils/spider/types'
import type { Crawler, CrawlerConfig } from './crawler'
import type { Forwarder, ForwarderConfig, ForwardTarget, ForwardTargetPlatformCommonConfig } from './forwarder'
import type { Translator } from './translator'
import type { Formatter, ConnectionMap } from './formatter'

/**
 * only crawling or forwarding or both
 */
interface AppConfig {
    crawlers?: Array<Crawler>
    cfg_crawler?: CrawlerConfig
    translators?: Array<Translator>
    formatters?: Array<Formatter>
    forward_targets?: Array<ForwardTarget>
    cfg_forward_target?: ForwardTargetPlatformCommonConfig
    forwarders?: Array<Forwarder<TaskType>>
    cfg_forwarder?: ForwarderConfig
    connections?: ConnectionMap
    api?: {
        port?: number
        secret?: string
    }
}

export type { AppConfig }
