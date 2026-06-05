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
                        origin: 'https://x.com/i/lists',
                        paths: ['1940955289840476438'],
                        cfg_crawler: {
                            cookie_file: cookieFile,
                        },
                    },
                ],
            } as any,
            {
                fetch: (async (url: string, init?: RequestInit) => {
                    requests.push({ url, headers: init?.headers })
                    return new Response(JSON.stringify({ data: { list: { tweets_timeline: {} } } }), {
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
            diagnostic_codes: ['x_list_timeline_probe_ok'],
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
        expect(requests[0]?.url).toContain('/ListLatestTweetsTimeline?')
        expect(requests[0]?.url).toContain('1940955289840476438')
        expect(serialized).not.toContain(cookieFile)
        expect(serialized).not.toContain(dir)
        expect(serialized).not.toContain('auth-value')
        expect(serialized).not.toContain('csrf-value')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('buildCrawlerLiveHealthAudit probes X username crawlers through user lookup graphql', async () => {
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
                        id: 'crawler-x-user',
                        name: 'crawler x user',
                        origin: 'https://x.com',
                        paths: ['227_staff'],
                        cfg_crawler: {
                            cookie_file: cookieFile,
                        },
                    },
                ],
            } as any,
            {
                fetch: (async (url: string, init?: RequestInit) => {
                    requests.push({ url, headers: init?.headers })
                    return new Response(JSON.stringify({ data: { user: { result: { rest_id: '1' } } } }), {
                        status: 200,
                        headers: { 'content-type': 'application/json' },
                    })
                }) as any,
            },
        )

        expect(audit.counts).toMatchObject({
            checked: 1,
            ok: 1,
            fail: 0,
        })
        expect(audit.results[0]).toMatchObject({
            crawler_id: 'crawler-x-user',
            platform: 'x',
            status: 'ok',
            diagnostic_codes: ['x_user_lookup_probe_ok'],
            live_probe: {
                checked: true,
                status: 'ok',
                http_status: 200,
            },
        })
        expect(requests).toHaveLength(1)
        expect(requests[0]?.url).toContain('/UserByScreenName?')
        expect(requests[0]?.url).toContain('227_staff')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('buildCrawlerLiveHealthAudit probes Instagram configured usernames', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-crawler-health-'))
    try {
        const cookieFile = join(dir, 'instagram.cookies.txt')
        writeFileSync(
            cookieFile,
            [
                '.instagram.com\tTRUE\t/\tTRUE\t9999999999\tsessionid\tsession-value',
                '.instagram.com\tTRUE\t/\tTRUE\t9999999999\tcsrftoken\tcsrf-value',
            ].join('\n'),
            'utf8',
        )
        const requests: Array<{ url: string; headers: Record<string, string> }> = []
        const audit = await buildCrawlerLiveHealthAudit(
            {
                crawlers: [
                    {
                        id: 'instagram-targeted',
                        name: 'Instagram targeted',
                        origin: 'https://www.instagram.com',
                        paths: ['shiina_satsuki227'],
                        cfg_crawler: { cookie_file: cookieFile },
                    },
                ],
            } as any,
            {
                fetch: (async (url: string, init?: RequestInit) => {
                    requests.push({ url, headers: init?.headers as Record<string, string> })
                    return new Response(JSON.stringify({ data: { user: { username: 'shiina_satsuki227' } } }), {
                        status: 200,
                        headers: { 'content-type': 'application/json' },
                    })
                }) as any,
            },
        )

        expect(audit.counts).toMatchObject({
            checked: 1,
            ok: 1,
            fail: 0,
        })
        expect(audit.results[0]).toMatchObject({
            crawler_id: 'instagram-targeted',
            platform: 'instagram',
            status: 'ok',
            diagnostic_codes: ['instagram_live_probe_ok'],
            live_probe: {
                checked: true,
                status: 'ok',
                http_status: 200,
            },
        })
        expect(requests).toHaveLength(1)
        expect(requests[0]?.url).toContain('username=shiina_satsuki227')
        expect(requests[0]?.headers.referer).toBe('https://www.instagram.com/shiina_satsuki227/')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('buildCrawlerLiveHealthAudit fails missing TikTok ttwid before live probe', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-crawler-health-'))
    try {
        const cookieFile = join(dir, 'tiktok.cookies.txt')
        writeFileSync(
            cookieFile,
            [
                '.tiktok.com\tTRUE\t/\tTRUE\t9999999999\ttt_chain_token\tchain-value',
                '.tiktok.com\tTRUE\t/\tTRUE\t9999999999\ttt_csrf_token\tcsrf-value',
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
                    missing: ['ttwid'],
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
        expect(serialized).not.toContain('csrf-value')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('buildCrawlerLiveHealthAudit probes TikTok with crawler-compatible headers', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-crawler-health-'))
    try {
        const cookieFile = join(dir, 'tiktok.cookies.txt')
        writeFileSync(
            cookieFile,
            [
                '.tiktok.com\tTRUE\t/\tTRUE\t9999999999\tttwid\twid-value',
                '.tiktok.com\tTRUE\t/\tTRUE\t9999999999\ttt_csrf_token\tcsrf-value',
            ].join('\n'),
            'utf8',
        )
        const requests: Array<{ url: string; headers: Record<string, string> }> = []
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
                fetch: (async (url: string, init?: RequestInit) => {
                    requests.push({ url, headers: init?.headers as Record<string, string> })
                    return new Response(
                        '<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__">{"ok":true}</script>',
                        { status: 200 },
                    )
                }) as any,
            },
        )

        expect(audit.counts).toMatchObject({
            checked: 1,
            ok: 1,
            fail: 0,
        })
        expect(audit.results[0]).toMatchObject({
            crawler_id: 'crawler-tiktok',
            platform: 'tiktok',
            status: 'ok',
            diagnostic_codes: ['tiktok_live_probe_ok'],
        })
        expect(requests).toHaveLength(1)
        expect(requests[0]?.url).toBe('https://www.tiktok.com/@tiktok')
        expect(requests[0]?.headers['user-agent']).toContain('Chrome/')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('buildCrawlerLiveHealthAudit probes TikTok configured usernames', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-crawler-health-'))
    try {
        const cookieFile = join(dir, 'tiktok.cookies.txt')
        writeFileSync(
            cookieFile,
            [
                '.tiktok.com\tTRUE\t/\tTRUE\t9999999999\tttwid\twid-value',
                '.tiktok.com\tTRUE\t/\tTRUE\t9999999999\ttt_csrf_token\tcsrf-value',
            ].join('\n'),
            'utf8',
        )
        const requests: Array<{ url: string; headers: Record<string, string> }> = []
        const audit = await buildCrawlerLiveHealthAudit(
            {
                crawlers: [
                    {
                        id: 'crawler-tiktok-targeted',
                        name: 'crawler tiktok targeted',
                        origin: 'https://www.tiktok.com',
                        paths: ['@227official'],
                        cfg_crawler: {
                            cookie_file: cookieFile,
                        },
                    },
                ],
            } as any,
            {
                fetch: (async (url: string, init?: RequestInit) => {
                    requests.push({ url, headers: init?.headers as Record<string, string> })
                    return new Response(
                        '<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__">{"ok":true}</script>',
                        { status: 200 },
                    )
                }) as any,
            },
        )

        expect(audit.counts).toMatchObject({
            checked: 1,
            ok: 1,
            fail: 0,
        })
        expect(audit.results[0]).toMatchObject({
            crawler_id: 'crawler-tiktok-targeted',
            platform: 'tiktok',
            status: 'ok',
            diagnostic_codes: ['tiktok_live_probe_ok'],
        })
        expect(requests).toHaveLength(1)
        expect(requests[0]?.url).toBe('https://www.tiktok.com/@227official')
        expect(requests[0]?.headers['user-agent']).toContain('Chrome/')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('buildCrawlerLiveHealthAudit reuses live probes for shared cookie files', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-crawler-health-'))
    try {
        const cookieFile = join(dir, 'instagram.cookies.txt')
        writeFileSync(
            cookieFile,
            [
                '.instagram.com\tTRUE\t/\tTRUE\t9999999999\tsessionid\tsession-value',
                '.instagram.com\tTRUE\t/\tTRUE\t9999999999\tcsrftoken\tcsrf-value',
            ].join('\n'),
            'utf8',
        )
        let fetchCalls = 0
        const audit = await buildCrawlerLiveHealthAudit(
            {
                crawlers: [
                    {
                        id: 'instagram-a',
                        name: 'Instagram A',
                        origin: 'https://www.instagram.com',
                        cfg_crawler: { cookie_file: cookieFile },
                    },
                    {
                        id: 'instagram-b',
                        name: 'Instagram B',
                        origin: 'https://www.instagram.com',
                        cfg_crawler: { cookie_file: cookieFile },
                    },
                    {
                        id: 'instagram-c',
                        name: 'Instagram C',
                        origin: 'https://www.instagram.com',
                        cfg_crawler: { cookie_file: cookieFile },
                    },
                ],
            } as any,
            {
                fetch: (async () => {
                    fetchCalls += 1
                    return new Response(JSON.stringify({ data: { user: { username: 'instagram' } } }), {
                        status: 200,
                        headers: { 'content-type': 'application/json' },
                    })
                }) as any,
            },
        )

        expect(fetchCalls).toBe(1)
        expect(audit.counts).toMatchObject({
            checked: 3,
            ok: 3,
            fail: 0,
        })
        expect(audit.results.map((result) => result.live_probe)).toEqual([
            { checked: true, status: 'ok', http_status: 200 },
            { checked: true, status: 'ok', http_status: 200 },
            { checked: true, status: 'ok', http_status: 200 },
        ])
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('buildCrawlerLiveHealthAudit can skip live probes for rate-limited platforms', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-crawler-health-'))
    try {
        const cookieFile = join(dir, 'instagram.cookies.txt')
        writeFileSync(
            cookieFile,
            [
                '.instagram.com\tTRUE\t/\tTRUE\t9999999999\tsessionid\tsession-value',
                '.instagram.com\tTRUE\t/\tTRUE\t9999999999\tcsrftoken\tcsrf-value',
            ].join('\n'),
            'utf8',
        )
        let fetchCalls = 0
        const audit = await buildCrawlerLiveHealthAudit(
            {
                crawlers: [
                    {
                        id: 'instagram-a',
                        name: 'Instagram A',
                        origin: 'https://www.instagram.com',
                        cfg_crawler: { cookie_file: cookieFile },
                    },
                ],
            } as any,
            {
                liveProbe: false,
                fetch: (async () => {
                    fetchCalls += 1
                    return new Response('', { status: 429 })
                }) as any,
            },
        )
        const serialized = JSON.stringify(audit)

        expect(fetchCalls).toBe(0)
        expect(audit.counts).toMatchObject({
            checked: 1,
            ok: 1,
            warn: 0,
            fail: 0,
        })
        expect(audit.results[0]).toMatchObject({
            crawler_id: 'instagram-a',
            platform: 'instagram',
            status: 'ok',
            diagnostic_codes: ['live_probe_disabled'],
            static_cookie: {
                exists: true,
                usable_cookie_count: 2,
                required_cookie_names: {
                    present: ['sessionid', 'csrftoken'],
                    missing: [],
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
        expect(serialized).not.toContain('session-value')
        expect(serialized).not.toContain('csrf-value')
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})
