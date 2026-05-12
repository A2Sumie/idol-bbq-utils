import type { BrowserMode } from '@idol-bbq-utils/spider'
import type { Platform, TaskType } from '@idol-bbq-utils/spider/types'
import type { CommonCfgConfig } from './common'
import type { Media } from './media'

enum ForwardTargetPlatformEnum {
    None = 'none',
    Bilibili = 'bilibili',
    QQ = 'qq',
}

type BiliupBrowserCookieSyncConfig = {
    enabled?: boolean
    bun_path?: string
    script_path?: string
    session_profile?: string
    url?: string
    browser_mode?: BrowserMode
    user_agent?: string
    locale?: string
    timezone?: string
}

type BiliupMetadataTemplatesConfig = {
    title?: string
    description?: string
}

type BiliupCollisionPlaceholderPartConfig = {
    enabled?: boolean
    video_path?: string
    image_path?: string
    title?: string
    duration_seconds?: number
    width?: number
    height?: number
    fps?: number
    ffmpeg_path?: string
    background_color?: string
}

type BiliupVideoUploadConfig = {
    enabled?: boolean
    python_path?: string
    helper_path?: string
    working_dir?: string
    metadata_timezone?: string
    cookie_file?: string
    browser_cookie_sync?: BiliupBrowserCookieSyncConfig
    submit_api?: 'web'
    line?: 'AUTO' | 'bda' | 'bda2' | 'ws' | 'qn' | 'bldsa' | 'tx' | 'txa'
    tid?: number
    threads?: number
    copyright?: 1 | 2
    tags?: Array<string>
    exclude_uids?: Array<string>
    metadata_templates?: BiliupMetadataTemplatesConfig
    collision_placeholder_part?: BiliupCollisionPlaceholderPartConfig
}

type PlatformConfigMap = {
    [ForwardTargetPlatformEnum.None]: {}
    [ForwardTargetPlatformEnum.Bilibili]: {
        bili_jct: string
        sessdata: string
        media_check_level?: 'strict' | 'loose' | 'none'
        video_upload?: BiliupVideoUploadConfig
    }
    /**
     * one11 bot protocol
     */
    [ForwardTargetPlatformEnum.QQ]: {
        url: string
        group_id: string
        token: string
    }
}

type TaskConfigMap = {
    article: {}
    follows: {
        /**
         *
         * "7d", "1w", "30d", "2h"...
         *
         * default is `1d`
         * ```
         * export type UnitTypeShort = 'd' | 'D' | 'M' | 'y' | 'h' | 'm' | 's' | 'ms'
         * export type UnitTypeLong = 'millisecond' | 'second' | 'minute' | 'hour' | 'day' | 'month' | 'year' | 'date'
         * export type UnitTypeLongPlural = 'milliseconds' | 'seconds' | 'minutes' | 'hours' | 'days' | 'months' | 'years' | 'dates'
         * ```
         */
        comparison_window?: string
    }
}

type TaskConfig<T extends TaskType> = TaskConfigMap[T]

interface ForwardTargetPlatformCommonConfig {
    replace_regex?: string | [string, string] | Array<[string, string]>
    /**
     * Accumulate image-like message units until the threshold is reached, then send once.
     * Card images count as one unit. When no card image is generated, non-empty text counts as one unit.
     */
    media_batch_threshold?: number
    /**
     * If a single article already contains this many source images, bypass the pending batch and send immediately.
     */
    media_batch_breakout_images?: number
    /**
     * When enabled, rendered card images are sent separately from the original media set.
     */
    separate_card_media?: boolean
    /**
     * Send one concise digest instead of individual articles when a target receives at least this many articles
     * in the same dispatch path. Useful for lower-noise groups.
     */
    digest_threshold?: number
    /**
     * Maximum article lines included in one digest message.
     */
    digest_max_items?: number
    /**
     * Enable hashtag storm digesting after this many same-tag articles arrive within the detection window.
     * Defaults to 3 when digest_threshold is configured.
     */
    tag_digest_threshold?: number
    /**
     * Rolling hashtag storm detection window, in seconds. Defaults to 5 minutes.
     */
    tag_digest_detection_window_seconds?: number
    /**
     * How long to keep same-tag articles digestized after storm detection, in seconds. Defaults to 20 minutes.
     */
    tag_digest_window_seconds?: number
    /**
     * Minimum distinct authors needed before a hashtag is treated as a shared storm. Defaults to 2.
     */
    tag_digest_min_authors?: number
    /**
     * Maximum article lines included in one hashtag digest message.
     */
    tag_digest_max_items?: number
    /**
     * Accumulate eligible articles for this target and send a rendered summary card on interval or threshold.
     * When include_original_media is false, only the summary card image is sent.
     */
    summary_card?:
        | boolean
        | {
              enabled?: boolean
              interval_seconds?: number
              threshold?: number
              max_items?: number
              include_original_media?: boolean
          }
    /**
     * Collapse the text body of referenced/replied-to articles that were already forwarded to this target.
     * Defaults to enabled except for explicitly high-realtime targets.
     */
    collapse_forwarded_ref_text?: boolean
    /**
     * Only collapse previously forwarded referenced articles newer than this many seconds. Defaults to 18 hours.
     */
    collapse_forwarded_ref_window_seconds?: number
    /**
     *
     * if 1d, the forwarder will only forward the article that created within 1 day
     * "7d", "1w", "30d", "2h"...
     *
     * default is `30m`
     * ```
     * export type UnitTypeShort = 'd' | 'D' | 'M' | 'y' | 'h' | 'm' | 's' | 'ms'
     * export type UnitTypeLong = 'millisecond' | 'second' | 'minute' | 'hour' | 'day' | 'month' | 'year' | 'date'
     * export type UnitTypeLongPlural = 'milliseconds' | 'seconds' | 'minutes' | 'hours' | 'days' | 'months' | 'years' | 'dates'
     * ```
     */
    block_until?: string
    accept_keywords?: Array<string>
    filter_keywords?: Array<string>
    /**
     * Block rule for the forwarder
     *
     * For example:
     *
     * ```
     * platform: Platform.X
     * task_type: 'article'
     * sub_type: ['retweet']
     * block_type: 'once'
     * block_until: '6h'
     * ```
     *
     * This will only send once which article type is retweet from X.
     * And other retweets will be blocked until 6 hours later.
     */
    block_rules?: Array<{
        platform: Platform
        /**
         * Default is `article`
         */
        task_type?: TaskType
        /**
         * The rule will apply to the specified sub-task types included, otherwise it will block nothing
         */
        sub_type?: Array<string>
        /**
         * Default is `none`
         * if always set, block_until will be ignored
         */
        block_type?: 'always' | 'none' | 'once' | 'once.media'

        /**
         * default is `6h`
         */
        block_until?: string
    }>
}

type ForwardTargetPlatformConfig<T extends ForwardTargetPlatformEnum = ForwardTargetPlatformEnum> = PlatformConfigMap[T]

interface ForwarderConfig extends CommonCfgConfig {
    cron?: string
    media?: Media
    render_type?:
        | 'text'
        | 'text-card'
        | 'text-compact'
        | 'text-compact-card'
        | 'img'
        | 'tag'
        | 'img-tag'
        | 'img-tag-dynamic'
        | 'img-with-meta'
    /**
     * Feature flags layered on top of render_type so templates can be tuned without adding one-off render modes.
     */
    render_features?: Array<'collapse-forwarded-ref-text' | string>
    card_features?: Array<'media-contain' | 'website-inline-media' | string>
    keywords?: Array<string>
    aggregation?: boolean
    deduplication?: boolean
}

interface ForwardTarget<T extends ForwardTargetPlatformEnum = ForwardTargetPlatformEnum> {
    platform: T
    /**
     * unique id for the target
     * default is md5 hash of the platform and config
     */
    id?: string
    /**
     * Group tag for UI grouping
     */
    group?: string
    cfg_platform: ForwardTargetPlatformConfig<T> & ForwardTargetPlatformCommonConfig
}

interface Forwarder<T extends TaskType> {
    /**
     * Unique identifier for connection mapping
     */
    id?: string
    /**
     * Runtime-only stable crawler id used by auto-bound tasks.
     */
    crawler_id?: string
    /**
     * Group tag for UI grouping
     */
    group?: string
    /**
     * Display only
     */
    name?: string
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
    task_type?: T
    /**
     * Task type like follows need this
     */
    task_title?: string
    /**
     *
     */
    cfg_task?: TaskConfig<T>
    /**
     * Array of forwarder target's id or id with runtime config, if empty will use all targets
     */
    subscribers?: Array<
        | string
        | {
              id: string
              cfg_forward_target?: ForwardTargetPlatformCommonConfig
          }
    >

    cfg_forwarder?: ForwarderConfig

    cfg_forward_target?: ForwardTargetPlatformCommonConfig
}

export { ForwardTargetPlatformEnum }

export type {
    BiliupBrowserCookieSyncConfig,
    ForwardTarget,
    Forwarder,
    ForwarderConfig,
    ForwardTargetPlatformConfig,
    ForwardTargetPlatformCommonConfig,
    BiliupVideoUploadConfig,
}
