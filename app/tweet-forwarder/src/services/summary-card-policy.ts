import type { ForwardTargetPlatformCommonConfig } from '@/types'

type SummaryCardWindowAlignment = 'none' | 'hour' | 'interval'

type ResolvedSummaryCardConfig = {
    intervalSeconds: number
    threshold: number
    maxItems: number
    includeOriginalMedia: boolean
    sendFirstImmediately: boolean
    sendFirstNative: boolean
    mediaRealtime: boolean
    mediaRealtimeText: 'none' | 'basic' | 'metadata' | 'rendered'
    flushOnThreshold: boolean
    flushDelaySeconds: number
    windowAlignment: SummaryCardWindowAlignment
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
    flush_on_threshold: boolean
    flush_delay_seconds: number
    window_alignment: SummaryCardWindowAlignment
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

    return {
        intervalSeconds,
        threshold,
        maxItems,
        includeOriginalMedia,
        sendFirstImmediately: objectConfig.send_first_immediately !== false,
        sendFirstNative: (objectConfig as any).send_first_native === true,
        mediaRealtime,
        mediaRealtimeText,
        flushOnThreshold: (objectConfig as any).flush_on_threshold !== false,
        flushDelaySeconds: Math.max(0, Math.floor(Number((objectConfig as any).flush_delay_seconds || 0))),
        windowAlignment,
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
        flush_on_threshold: config.flushOnThreshold,
        flush_delay_seconds: config.flushDelaySeconds,
        window_alignment: config.windowAlignment,
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
    type SummaryCardWindowAlignment,
}
