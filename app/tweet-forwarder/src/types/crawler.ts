import type { BrowserMode, DeviceProfile, ProfileViewport } from '@idol-bbq-utils/spider'
import type { CrawlEngine, TaskType } from '@idol-bbq-utils/spider/types'
import type { CommonCfgConfig } from './common'
import type { Processor } from './processor'

interface AggregationConfig {
    cron?: string
    prompt?: string
    processor_id?: string
    target_ids?: Array<string>
}

interface LiveRelayTargetConfig {
    enabled?: boolean
    player_id?: string
    player_name?: string
    player_url?: string
    live_player_url?: string
    auth_username?: string
    auth_password?: string
    waf_bypass_header?: string
    sync_interval_seconds?: number
    post_live_grace_seconds?: number
    stop_offline?: boolean
}

interface LiveRelayConfig extends LiveRelayTargetConfig {
    targets?: Record<string, LiveRelayTargetConfig>
}

interface CrawlerConfig extends CommonCfgConfig {
    /**
     * crontab format, reference: https://crontab.guru/
     *
     * Default: every 1 hour
     *
     *          * 1 * * *
     *          m h d M w
     */
    cron?: string
    /**
     * Path to the cookie file
     */
    cookie_file?: string
    /**
     * Random waiting time for per crawling
     */
    interval_time?: {
        max: number
        min: number
    }
    /**
     * TODO
     *
     * Will trigger the immediate notify to subscribed forwarders after the crawling
     *
     * Only works for `task_type` = `article` for now
     */
    immediate_notify?: boolean
    user_agent?: string
    browser_mode?: BrowserMode
    device_profile?: DeviceProfile
    session_profile?: string
    extra_headers?: Record<string, string>
    viewport?: Partial<ProfileViewport>
    locale?: string
    timezone?: string

    // Processor Configuration
    processor?: Processor
    processor_id?: string

    // Aggregation (Batch Formatting) Configuration
    aggregation?: AggregationConfig
    live_relay?: LiveRelayConfig

    /**
     * Default use browser, it depends on the spider behavior.
     */
    engine?: CrawlEngine
    /**
     * 细粒度控制子任务类型
     *
     * 比如X的 article，需要分开爬取tweet和replies，具体设置依赖于爬虫的实现
     */
    sub_task_type?: Array<string>
    /**
     * Unified crawler: users that must always be hydrated in addition to discovered activity.
     */
    hydrate_users?: Array<string>
    /**
     * Unified crawler: cap the number of users to hydrate per run.
     */
    hydrate_limit?: number
}

interface Crawler {
    /**
     * Stable identifier for connection maps. Prefer this over `name` when present.
     */
    id?: string
    /**
     * Display only
     */
    name?: string
    /**
     * Group tag for UI grouping
     */
    group?: string
    /**
     * will override the origin and paths
     */
    websites?: Array<string>
    /**
     * should work with paths
     */
    origin?: string
    /**
     * should work with origin
     */
    paths?: Array<string>
    /**
     * Task type defined in `@idol-bbq-utils/spider`
     */
    task_type?: TaskType
    /**
     *
     */
    cfg_crawler?: CrawlerConfig
}

export type { Crawler, CrawlerConfig, AggregationConfig, LiveRelayConfig, LiveRelayTargetConfig }
