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

interface CrawlerPostProcessorConfig {
    enabled?: boolean
    processor_id: string
    action?: 'translate' | 'extract' | 'merge' | 'plan'
    schedule_url?: string
    schedule_api_key?: string
    schedule_user_agent?: string
    schedule_waf_bypass_header?: string
    result_key?: string
    min_confidence?: number
}

interface XTikTokLinkIngestConfig {
    enabled?: boolean
    crawler?: string
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

type CrawlerScheduleSlot =
    | string
    | {
          time: string
          days?: Array<number | string>
      }

interface CrawlerScheduleWindow {
    start: string
    end: string
    every_minutes: number
    offset_minutes?: number
    days?: Array<number | string>
}

interface CrawlerHotScheduleConfig {
    enabled?: boolean
    timezone?: string
    slots?: Array<CrawlerScheduleSlot>
    windows?: Array<CrawlerScheduleWindow>
    min_gap_seconds?: number
    jitter_seconds?: number
    tick_seconds?: number
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
     * Non-Cron hot-write crawler schedule. Supports daily slots and repeated
     * windows, and can be updated at runtime through API/MCP without reload.
     */
    schedule?: CrawlerHotScheduleConfig
    hot_schedule?: CrawlerHotScheduleConfig
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
    post_processors?: Array<CrawlerPostProcessorConfig>
    x_tiktok_link_ingest?: XTikTokLinkIngestConfig | false
    tiktok_link_ingest_crawler?: string

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
    /**
     * Unified crawler: cap concurrent account hydration requests.
     */
    hydrate_concurrency?: number
    /**
     * Unified crawler: random pause between hydration chunks.
     */
    hydrate_interval_time?: {
        max: number
        min: number
    }
    /**
     * Website crawler: cap list pagination per feed. Useful for high-frequency shallow scans.
     */
    max_list_pages?: number
    /**
     * Website crawler: cap detail pages fetched per feed.
     */
    max_detail_count?: number
    /**
     * Website crawler: random pause between detail pages.
     */
    detail_interval_time?: {
        max: number
        min: number
    }
    /**
     * Website crawler: Puppeteer resource types to abort, e.g. image/font/media/stylesheet.
     */
    block_resource_types?: Array<string>
    /**
     * Dangerous backfill-only option. When enabled, a crawler that finds already-persisted rows may dispatch
     * those article ids, bounded by age and count. Defaults to disabled.
     */
    reuse_existing_for_immediate_forward?:
        | boolean
        | {
              enabled?: boolean
              max_age_seconds?: number
              max_items?: number
              reason?: string
          }
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

export type {
    Crawler,
    CrawlerConfig,
    AggregationConfig,
    CrawlerPostProcessorConfig,
    LiveRelayConfig,
    LiveRelayTargetConfig,
}
