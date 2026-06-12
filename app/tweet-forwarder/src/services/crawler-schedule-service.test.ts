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
