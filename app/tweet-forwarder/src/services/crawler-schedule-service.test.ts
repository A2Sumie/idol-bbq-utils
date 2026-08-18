import { expect, test } from 'bun:test'
import {
    expandLegacyCronToDailySlots,
    formatMinuteOfDay,
    nextCrawlerRunAt,
    resolveCrawlerSchedule,
} from './crawler-schedule-service'

test('crawler schedule expands legacy cron subset into daily slots without CronJob', () => {
    const slots = expandLegacyCronToDailySlots('4,19,34,49 15-23 * * *')

    expect(slots).toHaveLength(36)
    expect(formatMinuteOfDay(slots[0]?.minuteOfDay || 0)).toBe('15:04')
    expect(formatMinuteOfDay(slots.at(-1)?.minuteOfDay || 0)).toBe('23:49')
})

test('crawler schedule resolves hot windows and computes the next JST slot', () => {
    const schedule = resolveCrawlerSchedule({
        name: 'hot-crawler',
        cfg_crawler: {
            schedule: {
                windows: [{ start: '18:05', end: '18:35', every_minutes: 15 }],
                timezone: 'Asia/Tokyo',
                min_gap_seconds: 0,
            },
        },
    } as any)

    expect(schedule?.source).toBe('hot_schedule')
    expect(schedule?.slots.map((slot) => formatMinuteOfDay(slot.minuteOfDay))).toEqual(['18:05', '18:20', '18:35'])
    const after = Date.UTC(2026, 5, 12, 9, 10, 0) / 1000 // 2026-06-12 18:10 JST
    expect(nextCrawlerRunAt(schedule!, after, 'hot-crawler')).toBe(Date.UTC(2026, 5, 12, 9, 20, 0) / 1000)
})

test('crawler schedule can disable a hot schedule explicitly', () => {
    expect(
        resolveCrawlerSchedule({
            name: 'disabled-crawler',
            cfg_crawler: {
                cron: '*/5 * * * *',
                schedule: {
                    enabled: false,
                },
            },
        } as any),
    ).toBeNull()
})

test('crawler schedule skips expired same-day slots and targets the next day instead of after+minGap', () => {
    const schedule = resolveCrawlerSchedule({
        name: 'expired-crawler',
        cfg_crawler: {
            schedule: {
                windows: [{ start: '10:00', end: '11:00', every_minutes: 30 }],
                timezone: 'Asia/Tokyo',
                min_gap_seconds: 120,
                jitter_seconds: 0,
            },
        },
    } as any)

    expect(schedule?.slots.map((slot) => formatMinuteOfDay(slot.minuteOfDay))).toEqual(['10:00', '10:30', '11:00'])
    // 2026-06-12 12:00 JST = 03:00 UTC — every same-day slot already expired.
    const after = Date.UTC(2026, 5, 12, 3, 0, 0) / 1000
    // Regression: the d9ebd5a max(after, after+minGap) bug resurrected the
    // expired 11:00 slot at after+120s, repeating forever at 2×minGap.
    // The next run must be the first slot of the NEXT day (10:00 JST 6/13).
    expect(nextCrawlerRunAt(schedule!, after, 'expired-crawler')).toBe(Date.UTC(2026, 5, 13, 1, 0, 0) / 1000)
})

test('crawler schedule Live-grab cron advances to the next slot instead of after+minGap', () => {
    // Mirrors the Live 抢抓 crawler: cron 11,26,41,56 14-16 * * * (legacy cron,
    // no hot_schedule object — min_gap falls back to the 60s default).
    const schedule = resolveCrawlerSchedule({
        name: 'live-crawler',
        cfg_crawler: {
            cron: '11,26,41,56 14-16 * * *',
            timezone: 'Asia/Tokyo',
        },
    } as any)

    expect(schedule?.source).toBe('legacy_cron')
    expect(schedule?.slots.length).toBe(12)
    // Dispatch fired at 14:11:30 JST = 05:11:30 UTC. The next run must be
    // 14:26 JST, not 14:12:30 (after+minGap) which previously resurrected the
    // dead 14:11 slot and re-fired every 2×minGap.
    const after = Date.UTC(2026, 7, 16, 5, 11, 30) / 1000
    expect(nextCrawlerRunAt(schedule!, after, 'live-crawler')).toBe(Date.UTC(2026, 7, 16, 5, 26, 0) / 1000)
})
