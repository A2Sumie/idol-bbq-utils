import type { ForwardTargetPlatformCommonConfig } from '@/types'

type SummaryCardWindowAlignment = 'none' | 'hour' | 'interval'
type SummaryCardSingleItemBehavior = 'native_if_uncovered' | 'summary_card' | 'drop'

type ResolvedSummaryCardConfig = {
    intervalSeconds: number
    threshold: number
    maxItems: number
    includeOriginalMedia: boolean
    sendFirstImmediately: boolean
    sendFirstNative: boolean
    mediaRealtime: boolean
    mediaRealtimeText: 'none' | 'basic' | 'metadata' | 'rendered'
    mediaRealtimeDropSummaryPlatforms: string[]
    flushOnThreshold: boolean
    flushDelaySeconds: number
    windowAlignment: SummaryCardWindowAlignment
    singleItemBehavior: SummaryCardSingleItemBehavior
    mediaDuplicateLimit: number | null
    translatedCard: {
        badgeLabel: string
        processorId?: string
    } | null
}

type SummaryCardRoutePolicy = {
    enabled: boolean
    interval_seconds: number
    threshold: number
    max_items: number
    include_original_media: boolean
    send_first_immediately: boolean
    send_first_native: boolean
    media_realtime: boolean
    media_realtime_text: 'none' | 'basic' | 'metadata' | 'rendered'
    media_realtime_drop_summary_platforms: string[]
    flush_on_threshold: boolean
    flush_delay_seconds: number
    window_alignment: SummaryCardWindowAlignment
    single_item_behavior: SummaryCardSingleItemBehavior
    media_duplicate_limit: number | null
    translated_card: {
        enabled: true
        badge_label: string
        processor_id?: string
    } | null
}

const DEFAULT_SUMMARY_CARD_INTERVAL_SECONDS = 30 * 60
const DEFAULT_SUMMARY_CARD_THRESHOLD = 8
const DEFAULT_SUMMARY_CARD_MAX_ITEMS = 14
const DEFAULT_TRANSLATED_SUMMARY_CARD_BADGE_LABEL = '译文'
const DEFAULT_SUMMARY_CARD_SINGLE_ITEM_BEHAVIOR: SummaryCardSingleItemBehavior = 'native_if_uncovered'

function normalizeTranslatedBadgeLabel(value: unknown) {
    const label = String(value || DEFAULT_TRANSLATED_SUMMARY_CARD_BADGE_LABEL).trim()
    return label.slice(0, 6) || DEFAULT_TRANSLATED_SUMMARY_CARD_BADGE_LABEL
}

function resolveTranslatedCardConfig(raw: unknown): ResolvedSummaryCardConfig['translatedCard'] {
    if (raw !== true && (typeof raw !== 'object' || !raw || (raw as any).enabled === false)) {
        return null
    }

    const processorId =
        typeof raw === 'object' && raw ? String((raw as any).processor_id || (raw as any).processorId || '').trim() : ''
    return {
        badgeLabel: normalizeTranslatedBadgeLabel(
            typeof raw === 'object' && raw ? (raw as any).badge_label : undefined,
        ),
        ...(processorId ? { processorId } : {}),
    }
}

function normalizePlatformTokens(raw: unknown) {
    if (!Array.isArray(raw)) {
        return []
    }
    return Array.from(
        new Set(
            raw
                .map((value) =>
                    String(value || '')
                        .trim()
                        .toLocaleLowerCase()
                        .replace(/[_\s-]+/g, ''),
                )
                .filter(Boolean),
        ),
    )
}

function resolveSummaryCardSingleItemBehavior(raw: unknown): SummaryCardSingleItemBehavior {
    const normalized = String(raw || '')
        .trim()
        .toLocaleLowerCase()
        .replace(/[-\s]+/g, '_')
    if (normalized === 'summary_card' || normalized === 'drop' || normalized === 'native_if_uncovered') {
        return normalized
    }
    return DEFAULT_SUMMARY_CARD_SINGLE_ITEM_BEHAVIOR
}

function resolveSummaryCardConfig(config: ForwardTargetPlatformCommonConfig): ResolvedSummaryCardConfig | null {
    const raw = config.summary_card
    const enabled = raw === true || (typeof raw === 'object' && raw?.enabled !== false)
    if (!enabled) {
        return null
    }

    const objectConfig = typeof raw === 'object' && raw ? raw : {}
    const intervalSeconds = Math.max(
        60,
        Math.floor(Number(objectConfig.interval_seconds || DEFAULT_SUMMARY_CARD_INTERVAL_SECONDS)),
    )
    const threshold = Math.max(2, Math.floor(Number(objectConfig.threshold || DEFAULT_SUMMARY_CARD_THRESHOLD)))
    const maxItems = Math.max(
        3,
        Math.min(Math.floor(Number(objectConfig.max_items || DEFAULT_SUMMARY_CARD_MAX_ITEMS)), 30),
    )
    const explicitDuplicateLimit = Math.floor(Number((objectConfig as any).media_duplicate_limit || 0))
    const includeOriginalMedia = objectConfig.include_original_media === true
    const mediaRealtime = (objectConfig as any).media_realtime === true
    const duplicateLimit =
        Number.isFinite(explicitDuplicateLimit) && explicitDuplicateLimit > 0
            ? explicitDuplicateLimit
            : includeOriginalMedia || mediaRealtime
              ? 2
              : 0
    const windowAlignment: SummaryCardWindowAlignment =
        (objectConfig as any).align_to_interval === true
            ? 'interval'
            : (objectConfig as any).align_to_hour === true
              ? 'hour'
              : 'none'
    const mediaRealtimeText = ['basic', 'metadata', 'rendered'].includes(
        String((objectConfig as any).media_realtime_text),
    )
        ? ((objectConfig as any).media_realtime_text as 'basic' | 'metadata' | 'rendered')
        : 'none'
    const translatedCard = resolveTranslatedCardConfig((objectConfig as any).translated_card)
    const mediaRealtimeDropSummaryPlatforms = normalizePlatformTokens(
        (objectConfig as any).media_realtime_drop_summary_platforms,
    )

    return {
        intervalSeconds,
        threshold,
        maxItems,
        includeOriginalMedia,
        sendFirstImmediately: objectConfig.send_first_immediately !== false,
        sendFirstNative: (objectConfig as any).send_first_native === true,
        mediaRealtime,
        mediaRealtimeText,
        mediaRealtimeDropSummaryPlatforms,
        flushOnThreshold: (objectConfig as any).flush_on_threshold !== false,
        flushDelaySeconds: Math.max(0, Math.floor(Number((objectConfig as any).flush_delay_seconds || 0))),
        windowAlignment,
        singleItemBehavior: resolveSummaryCardSingleItemBehavior((objectConfig as any).single_item_behavior),
        mediaDuplicateLimit: duplicateLimit > 0 ? duplicateLimit : null,
        translatedCard,
    }
}

function toSummaryCardRoutePolicy(config: ResolvedSummaryCardConfig): SummaryCardRoutePolicy {
    return {
        enabled: true,
        interval_seconds: config.intervalSeconds,
        threshold: config.threshold,
        max_items: config.maxItems,
        include_original_media: config.includeOriginalMedia,
        send_first_immediately: config.sendFirstImmediately,
        send_first_native: config.sendFirstNative,
        media_realtime: config.mediaRealtime,
        media_realtime_text: config.mediaRealtimeText,
        media_realtime_drop_summary_platforms: config.mediaRealtimeDropSummaryPlatforms,
        flush_on_threshold: config.flushOnThreshold,
        flush_delay_seconds: config.flushDelaySeconds,
        window_alignment: config.windowAlignment,
        single_item_behavior: config.singleItemBehavior,
        media_duplicate_limit: config.mediaDuplicateLimit,
        translated_card: config.translatedCard
            ? {
                  enabled: true,
                  badge_label: config.translatedCard.badgeLabel,
                  ...(config.translatedCard.processorId ? { processor_id: config.translatedCard.processorId } : {}),
              }
            : null,
    }
}

function resolveSummaryCardRoutePolicy(config: ForwardTargetPlatformCommonConfig): SummaryCardRoutePolicy | undefined {
    const resolved = resolveSummaryCardConfig(config)
    return resolved ? toSummaryCardRoutePolicy(resolved) : undefined
}

export {
    resolveSummaryCardConfig,
    resolveSummaryCardRoutePolicy,
    toSummaryCardRoutePolicy,
    type ResolvedSummaryCardConfig,
    type SummaryCardRoutePolicy,
    type SummaryCardSingleItemBehavior,
    type SummaryCardWindowAlignment,
}
