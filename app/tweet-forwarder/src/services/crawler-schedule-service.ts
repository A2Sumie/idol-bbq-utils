import type { Crawler, CrawlerConfig } from '@/types/crawler'
import { prisma } from '@/db/client'

const DEFAULT_TIMEZONE = 'Asia/Tokyo'
const JST_OFFSET_MINUTES = 9 * 60
const DEFAULT_TICK_SECONDS = 15
const DEFAULT_MIN_GAP_SECONDS = 60
const DEFAULT_RECOMMENDATION_DAYS = 120

type CrawlerScheduleSlotInput =
    | string
    | {
          time: string
          days?: Array<number | string>
      }

type CrawlerScheduleWindowInput = {
    start: string
    end: string
    every_minutes: number
    offset_minutes?: number
    days?: Array<number | string>
}

type CrawlerHotScheduleConfig = {
    enabled?: boolean
    timezone?: string
    slots?: Array<CrawlerScheduleSlotInput>
    windows?: Array<CrawlerScheduleWindowInput>
    min_gap_seconds?: number
    jitter_seconds?: number
    tick_seconds?: number
}

type CrawlerScheduleSlot = {
    minuteOfDay: number
    days?: Array<number>
}

type ResolvedCrawlerSchedule = {
    source: 'hot_schedule' | 'legacy_cron'
    timezone: string
    timezoneOffsetMinutes: number
    slots: Array<CrawlerScheduleSlot>
    minGapSeconds: number
    jitterSeconds: number
    tickSeconds: number
}

type CrawlerScheduleSnapshot = {
    crawler: string
    schedule: ResolvedCrawlerSchedule | null
    nextRunAt: number | null
    nextRunAtIso: string | null
    lastRunAt?: number | null
}

type ArticleDistributionRow = {
    platform: string
    created_at: number
}

type PlatformScheduleRecommendation = {
    platform: string
    days: number
    sampleCount: number
    hourCounts: Array<{ hour: string; count: number }>
    quarterHourCounts: Array<{ time: string; count: number }>
    recommendedWindows: Array<{ start: string; end: string; every_minutes: number; reason: string }>
}

function clampInteger(value: unknown, fallback: number, min: number, max: number) {
    const normalized = Number(value)
    if (!Number.isFinite(normalized)) {
        return fallback
    }
    return Math.max(min, Math.min(Math.trunc(normalized), max))
}

function timezoneOffsetMinutes(timezone: string | undefined) {
    return String(timezone || DEFAULT_TIMEZONE) === DEFAULT_TIMEZONE ? JST_OFFSET_MINUTES : 0
}

function parseClockToMinuteOfDay(value: string): number | null {
    const match = String(value || '')
        .trim()
        .match(/^(\d{1,2}):(\d{2})$/)
    if (!match) {
        return null
    }
    const hour = Number(match[1])
    const minute = Number(match[2])
    if (!Number.isInteger(hour) || !Number.isInteger(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        return null
    }
    return hour * 60 + minute
}

function formatMinuteOfDay(minuteOfDay: number) {
    const normalized = ((minuteOfDay % 1440) + 1440) % 1440
    const hour = Math.floor(normalized / 60)
    const minute = normalized % 60
    return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`
}

function normalizeDays(days?: Array<number | string>) {
    if (!Array.isArray(days) || days.length === 0) {
        return undefined
    }
    const normalized = Array.from(
        new Set(
            days
                .map((day) => Number(day))
                .filter((day) => Number.isInteger(day) && day >= 0 && day <= 6),
        ),
    ).sort((a, b) => a - b)
    return normalized.length > 0 ? normalized : undefined
}

function expandScheduleWindow(window: CrawlerScheduleWindowInput): Array<CrawlerScheduleSlot> {
    const start = parseClockToMinuteOfDay(window.start)
    const end = parseClockToMinuteOfDay(window.end)
    const everyMinutes = clampInteger(window.every_minutes, 0, 1, 24 * 60)
    if (start === null || end === null || everyMinutes <= 0) {
        return []
    }
    const days = normalizeDays(window.days)
    const slots: Array<CrawlerScheduleSlot> = []
    const offset = clampInteger(window.offset_minutes, 0, 0, everyMinutes - 1)
    const spanEnd = end < start ? end + 24 * 60 : end
    for (let minute = start + offset; minute <= spanEnd; minute += everyMinutes) {
        slots.push({ minuteOfDay: minute % (24 * 60), days })
    }
    return slots
}

function parseNumberFieldToken(token: string, min: number, max: number): Array<number> {
    const [rangePart, stepPart] = token.split('/')
    const step = stepPart ? Number(stepPart) : 1
    if (!Number.isInteger(step) || step <= 0) {
        return []
    }
    let start = min
    let end = max
    if (rangePart !== '*') {
        const rangeMatch = rangePart.match(/^(\d+)(?:-(\d+))?$/)
        if (!rangeMatch) {
            return []
        }
        start = Number(rangeMatch[1])
        end = rangeMatch[2] === undefined ? start : Number(rangeMatch[2])
    }
    if (start < min || end > max || start > end) {
        return []
    }
    const values: Array<number> = []
    for (let value = start; value <= end; value += step) {
        values.push(value)
    }
    return values
}

function parseCronNumberField(field: string, min: number, max: number): Array<number> | null {
    const values = new Set<number>()
    for (const token of String(field || '').split(',')) {
        const parsed = parseNumberFieldToken(token.trim(), min, max)
        if (parsed.length === 0) {
            return null
        }
        for (const value of parsed) {
            values.add(value)
        }
    }
    return Array.from(values).sort((a, b) => a - b)
}

function expandLegacyCronToDailySlots(cron: string | undefined | null): Array<CrawlerScheduleSlot> {
    const parts = String(cron || '')
        .trim()
        .split(/\s+/)
    if (parts.length !== 5 && parts.length !== 6) {
        return []
    }
    const cronParts = parts.length === 6 ? parts.slice(1) : parts
    const [minuteField, hourField, dayOfMonthField, monthField, dayOfWeekField] = cronParts
    if (dayOfMonthField !== '*' || monthField !== '*' || dayOfWeekField !== '*') {
        return []
    }
    const minutes = parseCronNumberField(minuteField, 0, 59)
    const hours = parseCronNumberField(hourField, 0, 23)
    if (!minutes || !hours) {
        return []
    }
    return hours.flatMap((hour) => minutes.map((minute) => ({ minuteOfDay: hour * 60 + minute })))
}

function normalizeSlots(slots: Array<CrawlerScheduleSlot>) {
    const dedup = new Map<string, CrawlerScheduleSlot>()
    for (const slot of slots) {
        const minuteOfDay = ((slot.minuteOfDay % 1440) + 1440) % 1440
        const days = slot.days?.length ? [...slot.days].sort((a, b) => a - b) : undefined
        dedup.set(`${minuteOfDay}:${days?.join(',') || '*'}`, { minuteOfDay, days })
    }
    return Array.from(dedup.values()).sort((a, b) => a.minuteOfDay - b.minuteOfDay)
}

function getScheduleConfig(config?: CrawlerConfig): CrawlerHotScheduleConfig | null {
    const schedule = (config as any)?.schedule || (config as any)?.hot_schedule
    return schedule && typeof schedule === 'object' ? schedule : null
}

function resolveCrawlerSchedule(crawler: Crawler): ResolvedCrawlerSchedule | null {
    const cfg = crawler.cfg_crawler || {}
    const schedule = getScheduleConfig(cfg)
    let slots: Array<CrawlerScheduleSlot> = []
    let source: ResolvedCrawlerSchedule['source'] = 'legacy_cron'
    if (schedule) {
        if (schedule.enabled === false) {
            return null
        }
        source = 'hot_schedule'
        slots = [
            ...(schedule.slots || [])
                .map((slot) => {
                    if (typeof slot === 'string') {
                        const minuteOfDay = parseClockToMinuteOfDay(slot)
                        return minuteOfDay === null ? null : { minuteOfDay }
                    }
                    const minuteOfDay = parseClockToMinuteOfDay(slot.time)
                    return minuteOfDay === null ? null : { minuteOfDay, days: normalizeDays(slot.days) }
                })
                .filter((slot): slot is CrawlerScheduleSlot => Boolean(slot)),
            ...(schedule.windows || []).flatMap(expandScheduleWindow),
        ]
    } else {
        slots = expandLegacyCronToDailySlots(cfg.cron)
    }
    const normalizedSlots = normalizeSlots(slots)
    if (normalizedSlots.length === 0) {
        return null
    }
    const timezone = schedule?.timezone || cfg.timezone || DEFAULT_TIMEZONE
    return {
        source,
        timezone,
        timezoneOffsetMinutes: timezoneOffsetMinutes(timezone),
        slots: normalizedSlots,
        minGapSeconds: clampInteger(schedule?.min_gap_seconds, DEFAULT_MIN_GAP_SECONDS, 0, 24 * 60 * 60),
        jitterSeconds: clampInteger(schedule?.jitter_seconds, 0, 0, 10 * 60),
        tickSeconds: clampInteger(schedule?.tick_seconds, DEFAULT_TICK_SECONDS, 1, 60),
    }
}

function zonedDateParts(epochSeconds: number, offsetMinutes: number) {
    const date = new Date((epochSeconds + offsetMinutes * 60) * 1000)
    return {
        year: date.getUTCFullYear(),
        month: date.getUTCMonth(),
        dayOfMonth: date.getUTCDate(),
        dayOfWeek: date.getUTCDay(),
        secondsOfDay: date.getUTCHours() * 3600 + date.getUTCMinutes() * 60 + date.getUTCSeconds(),
    }
}

function localMidnightEpoch(year: number, month: number, dayOfMonth: number, offsetMinutes: number) {
    return Math.floor(Date.UTC(year, month, dayOfMonth, 0, 0, 0) / 1000) - offsetMinutes * 60
}

function stableJitterSeconds(key: string, jitterSeconds: number) {
    if (jitterSeconds <= 0) {
        return 0
    }
    let hash = 0
    for (let index = 0; index < key.length; index += 1) {
        hash = (hash * 31 + key.charCodeAt(index)) >>> 0
    }
    return (hash % (jitterSeconds * 2 + 1)) - jitterSeconds
}

function nextCrawlerRunAt(schedule: ResolvedCrawlerSchedule, afterEpochSeconds: number, crawlerName = '') {
    const offset = schedule.timezoneOffsetMinutes
    const parts = zonedDateParts(afterEpochSeconds, offset)
    for (let dayOffset = 0; dayOffset <= 8; dayOffset += 1) {
        const dayDate = new Date(Date.UTC(parts.year, parts.month, parts.dayOfMonth + dayOffset))
        const localDayOfWeek = dayDate.getUTCDay()
        const midnight = localMidnightEpoch(
            dayDate.getUTCFullYear(),
            dayDate.getUTCMonth(),
            dayDate.getUTCDate(),
            offset,
        )
        for (const slot of schedule.slots) {
            if (slot.days && !slot.days.includes(localDayOfWeek)) {
                continue
            }
            const base = midnight + slot.minuteOfDay * 60
            const jittered = base + stableJitterSeconds(`${crawlerName}:${base}`, schedule.jitterSeconds)
            if (jittered > afterEpochSeconds) {
                return jittered
            }
        }
    }
    return null
}

function isoOrNull(epochSeconds: number | null | undefined) {
    return epochSeconds ? new Date(epochSeconds * 1000).toISOString() : null
}

function buildScheduleSnapshot(
    crawler: string,
    schedule: ResolvedCrawlerSchedule | null,
    nextRunAt: number | null,
    lastRunAt?: number | null,
): CrawlerScheduleSnapshot {
    return {
        crawler,
        schedule,
        nextRunAt,
        nextRunAtIso: isoOrNull(nextRunAt),
        lastRunAt,
    }
}

function toJstMinuteOfDay(epochSeconds: number) {
    const parts = zonedDateParts(epochSeconds, JST_OFFSET_MINUTES)
    return Math.floor(parts.secondsOfDay / 60)
}

function buildCounts(rows: Array<ArticleDistributionRow>) {
    const hourCounts = new Map<number, number>()
    const quarterCounts = new Map<number, number>()
    for (const row of rows) {
        const minuteOfDay = toJstMinuteOfDay(row.created_at)
        const hour = Math.floor(minuteOfDay / 60)
        const quarter = Math.floor(minuteOfDay / 15) * 15
        hourCounts.set(hour, (hourCounts.get(hour) || 0) + 1)
        quarterCounts.set(quarter, (quarterCounts.get(quarter) || 0) + 1)
    }
    return {
        hourCounts: Array.from(hourCounts.entries())
            .map(([hour, count]) => ({ hour: String(hour).padStart(2, '0'), count }))
            .sort((a, b) => b.count - a.count || a.hour.localeCompare(b.hour)),
        quarterHourCounts: Array.from(quarterCounts.entries())
            .map(([minuteOfDay, count]) => ({ time: formatMinuteOfDay(minuteOfDay), count }))
            .sort((a, b) => b.count - a.count || a.time.localeCompare(b.time)),
    }
}

function platformEveryMinutes(platform: string, dense: boolean) {
    if (platform === 'x' || platform === 'tiktok') return dense ? 8 : 15
    if (platform === 'instagram') return dense ? 15 : 30
    if (platform === 'youtube') return dense ? 10 : 30
    if (platform === 'website') return dense ? 10 : 30
    return dense ? 15 : 30
}

function buildRecommendedWindows(platform: string, hourCounts: Array<{ hour: string; count: number }>) {
    if (hourCounts.length === 0) {
        return []
    }
    const max = Math.max(...hourCounts.map((item) => item.count))
    const threshold = Math.max(3, Math.ceil(max * 0.15))
    const active = new Set(hourCounts.filter((item) => item.count >= threshold).map((item) => Number(item.hour)))
    const windows: Array<{ start: string; end: string; every_minutes: number; reason: string }> = []
    let index = 0
    while (index < 24) {
        if (!active.has(index)) {
            index += 1
            continue
        }
        const start = index
        while (index + 1 < 24 && active.has(index + 1)) {
            index += 1
        }
        const end = index
        const span = end - start + 1
        windows.push({
            start: `${String(start).padStart(2, '0')}:00`,
            end: `${String(end).padStart(2, '0')}:59`,
            every_minutes: platformEveryMinutes(platform, span >= 2),
            reason: `hour_count>=${threshold}`,
        })
        index += 1
    }
    return windows
}

async function getArticleDistributionRows(days = DEFAULT_RECOMMENDATION_DAYS) {
    const since = Math.floor(Date.now() / 1000) - clampInteger(days, DEFAULT_RECOMMENDATION_DAYS, 1, 365) * 86400
    const [x, instagram, tiktok, youtube, website] = await Promise.all([
        prisma.twitter_article.findMany({ where: { created_at: { gte: since } }, select: { created_at: true } }),
        prisma.instagram_article.findMany({ where: { created_at: { gte: since } }, select: { created_at: true } }),
        prisma.tiktok_article.findMany({ where: { created_at: { gte: since } }, select: { created_at: true } }),
        prisma.youtube_article.findMany({ where: { created_at: { gte: since } }, select: { created_at: true } }),
        prisma.website_article.findMany({ where: { created_at: { gte: since } }, select: { created_at: true } }),
    ])
    return {
        x: x.map((row) => ({ platform: 'x', created_at: row.created_at })),
        instagram: instagram.map((row) => ({ platform: 'instagram', created_at: row.created_at })),
        tiktok: tiktok.map((row) => ({ platform: 'tiktok', created_at: row.created_at })),
        youtube: youtube.map((row) => ({ platform: 'youtube', created_at: row.created_at })),
        website: website.map((row) => ({ platform: 'website', created_at: row.created_at })),
    }
}

async function buildCrawlerScheduleRecommendations(days = DEFAULT_RECOMMENDATION_DAYS) {
    const byPlatform = await getArticleDistributionRows(days)
    const normalizedDays = clampInteger(days, DEFAULT_RECOMMENDATION_DAYS, 1, 365)
    return Object.entries(byPlatform).map(([platform, rows]): PlatformScheduleRecommendation => {
        const counts = buildCounts(rows)
        return {
            platform,
            days: normalizedDays,
            sampleCount: rows.length,
            hourCounts: counts.hourCounts,
            quarterHourCounts: counts.quarterHourCounts.slice(0, 40),
            recommendedWindows: buildRecommendedWindows(platform, counts.hourCounts),
        }
    })
}

export {
    DEFAULT_TICK_SECONDS,
    buildCrawlerScheduleRecommendations,
    buildScheduleSnapshot,
    expandLegacyCronToDailySlots,
    formatMinuteOfDay,
    nextCrawlerRunAt,
    resolveCrawlerSchedule,
}
export type {
    CrawlerHotScheduleConfig,
    CrawlerScheduleSnapshot,
    PlatformScheduleRecommendation,
    ResolvedCrawlerSchedule,
}
