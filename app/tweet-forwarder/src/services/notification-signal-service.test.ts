import { expect, test } from 'bun:test'
import { buildNotificationSignalRecord, matchNotificationSignalCrawlers, normalizeSignalPlatform } from './notification-signal-service'

test('notification signal records hash notification text and strip tracking from urls', () => {
    const record = buildNotificationSignalRecord(
        {
            crawlers: [
                {
                    id: 'x-list',
                    name: 'X unified list',
                    origin: 'https://x.com',
                    paths: ['/i/lists/123'],
                },
            ],
        },
        {
            platform: 'twitter',
            crawlerName: 'X unified list',
            notificationId: 'notification-1',
            url: 'https://x.com/Member/status/123?utm_source=secret#frag',
            title: 'private title',
            body: 'private body',
            received_at: 1_800_000_000,
        },
        { now: 1_800_000_001 },
    )

    expect(record.platform).toBe('x')
    expect(record.notification.url).toBe('https://x.com/Member/status/123')
    expect((record.notification as any).title).toBeUndefined()
    expect((record.notification as any).body).toBeUndefined()
    expect(record.notification.title_hash).toHaveLength(64)
    expect(record.notification.title_length).toBe('private title'.length)
    expect(record.notification.body_hash).toHaveLength(64)
    expect(record.notification.body_length).toBe('private body'.length)
    expect(record.matched_crawlers).toEqual([
        {
            crawler_id: 'x-list',
            crawler_name: 'X unified list',
            reason: 'explicit',
        },
    ])
    expect(record.would_trigger_crawlers).toBe(false)
})

test('notification signal crawler matching supports identity matches without explicit crawler names', () => {
    const matches = matchNotificationSignalCrawlers(
        {
            crawlers: [
                {
                    id: 'ig-sakura',
                    name: 'Instagram Sakura',
                    origin: 'https://www.instagram.com',
                    paths: ['/Sakura.Member/'],
                },
                {
                    id: 'tt-other',
                    name: 'TikTok other',
                    origin: 'https://www.tiktok.com',
                    paths: ['/@Sakura.Member'],
                },
            ],
        },
        {
            platform: 'instagram',
            username: '@sakura.member',
        },
    )

    expect(matches).toEqual([
        {
            crawler_id: 'ig-sakura',
            crawler_name: 'Instagram Sakura',
            reason: 'identity',
        },
    ])
})

test('notification signal platform aliases stay explicit for shadow-mode routing', () => {
    expect(normalizeSignalPlatform('X')).toBe('x')
    expect(normalizeSignalPlatform('twitter')).toBe('x')
    expect(normalizeSignalPlatform('ins')).toBe('instagram')
    expect(normalizeSignalPlatform('tt')).toBe('tiktok')
    expect(normalizeSignalPlatform('B站')).toBe('bilibili')
    expect(normalizeSignalPlatform('unknown-platform')).toBe('unknown')
})
