import { expect, test } from 'bun:test'
import { mkdtempSync, rmSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { buildCrawlerLiveHealthAudit } from './crawler-health-audit-service'

test('buildCrawlerLiveHealthAudit probes healthy X cookies without exposing secrets', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-crawler-health-'))
    try {
        const cookieFile = join(dir, 'x.cookies.txt')
        writeFileSync(
            cookieFile,
            [
                '.x.com\tTRUE\t/\tTRUE\t9999999999\tauth_token\tauth-value',
                '.x.com\tTRUE\t/\tTRUE\t9999999999\tct0\tcsrf-value',
            ].join('\n'),
            'utf8',
        )
        const requests: Array<{ url: string; headers: HeadersInit | undefined }> = []
        const audit = await buildCrawlerLiveHealthAudit(
            {
                crawlers: [
                    {
                        id: 'crawler-x',
                        name: 'crawler x',
                        origin: 'https://x.com',
                        cfg_crawler: {
                            cookie_file: cookieFile,
                        },
                    },
                ],
            } as any,
            {
                fetch: (async (url: string, init?: RequestInit) => {
                    requests.push({ url, headers: init?.headers })
                    return new Response(JSON.stringify({ screen_name: 'X' }), {
                        status: 200,
                        headers: { 'content-type': 'application/json' },
                    })
                }) as any,
            },
        )
        const serialized = JSON.stringify(audit)

        expect(audit.counts).toMatchObject({
            checked: 1,
            ok: 1,
            fail: 0,
        })
        expect(audit.results[0]).toMatchObject({
            crawler_id: 'crawler-x',
            platform: 'x',
            status: 'ok',
            diagnostic_codes: ['x_live_probe_ok'],
            static_cookie: {
                exists: true,
                usable_cookie_count: 2,
                required_cookie_names: {
                    present: ['auth_token', 'ct0'],
                    missing: [],
                },
            },
            live_probe: {
                checked: true,
                status: 'ok',
                http_status: 200,
            },
        })
        expect(requests).toHaveLength(1)
        expect(requests[0]?.url).toBe('https://x.com/i/api/1.1/account/settings.json')
        expect(serialized).not.toContain(cookieFile)
        expect(serialized).not.toContain(dir)
        expect(serialized).not.toContain('auth-value')
        expect(serialized).not.toContain('csrf-value')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('buildCrawlerLiveHealthAudit fails missing TikTok sessionid before live probe', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-crawler-health-'))
    try {
        const cookieFile = join(dir, 'tiktok.cookies.txt')
        writeFileSync(
            cookieFile,
            [
                '.tiktok.com\tTRUE\t/\tTRUE\t9999999999\ttt_chain_token\tchain-value',
                '.tiktok.com\tTRUE\t/\tTRUE\t9999999999\tttwid\twid-value',
            ].join('\n'),
            'utf8',
        )
        let fetchCalls = 0
        const audit = await buildCrawlerLiveHealthAudit(
            {
                crawlers: [
                    {
                        id: 'crawler-tiktok',
                        name: 'crawler tiktok',
                        origin: 'https://www.tiktok.com',
                        cfg_crawler: {
                            cookie_file: cookieFile,
                        },
                    },
                ],
            } as any,
            {
                fetch: (async () => {
                    fetchCalls += 1
                    return new Response('', { status: 200 })
                }) as any,
            },
        )
        const serialized = JSON.stringify(audit)

        expect(fetchCalls).toBe(0)
        expect(audit.counts).toMatchObject({
            checked: 1,
            ok: 0,
            fail: 1,
        })
        expect(audit.results[0]).toMatchObject({
            crawler_id: 'crawler-tiktok',
            platform: 'tiktok',
            status: 'fail',
            diagnostic_codes: ['cookie_required_names_missing', 'live_probe_static_cookie_unhealthy'],
            static_cookie: {
                exists: true,
                usable_cookie_count: 2,
                required_cookie_names: {
                    present: [],
                    missing: ['sessionid'],
                },
            },
            live_probe: {
                checked: false,
                status: 'skipped',
                http_status: null,
            },
        })
        expect(serialized).not.toContain(cookieFile)
        expect(serialized).not.toContain(dir)
        expect(serialized).not.toContain('chain-value')
        expect(serialized).not.toContain('wid-value')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})
