import { expect, test } from 'bun:test'
import {
    analyzeManifestText,
    buildPlayerUrl,
    classifyInstagramBroadcastStatus,
    filterRelayHeaders,
    InstagramLiveRelayService,
    isPostLiveGraceActive,
    N2NJ_REQUEST_USER_AGENT,
    parseInstagramLiveWebInfo,
    parseCookieString,
} from './instagram-live-relay-service'

test('instagram live relay helpers keep extension-compatible headers and cookies', () => {
    expect(
        filterRelayHeaders({
            referer: 'https://www.instagram.com/shiina_satsuki227/live/',
            cookie: 'sessionid=abc',
            host: 'edge-chat.instagram.com',
            'user-agent': 'Mozilla/5.0',
        }),
    ).toEqual({
        referer: 'https://www.instagram.com/shiina_satsuki227/live/',
        cookie: 'sessionid=abc',
        'user-agent': 'Mozilla/5.0',
    })

    expect(parseCookieString('sessionid=abc; csrftoken=def')).toEqual({
        sessionid: 'abc',
        csrftoken: 'def',
    })
})

test('instagram live relay manifest parser recognizes master playlists', () => {
    const manifest = `#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=1280000,RESOLUTION=640x360
low/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2560000,RESOLUTION=1280x720
high/index.m3u8
`
    const analyzed = analyzeManifestText('https://example.com/master.m3u8', manifest)

    expect(analyzed.encrypted).toBeFalse()
    expect(analyzed.variants_count).toBe(2)
    expect(analyzed.variants[0]).toMatchObject({
        url: 'https://example.com/high/index.m3u8',
        bandwidth: 2560000,
        resolution: '1280x720',
    })
    expect(buildPlayerUrl('relay')).toBe(process.env.LIVE_PLAYER_STREAM_URL || '')
})

test('instagram live relay parser extracts mpd urls from web_info payload', () => {
    expect(
        parseInstagramLiveWebInfo({
            broadcast_status: 'active',
            dash_abr_playback_url: 'https://example.com/live-abr.mpd?foo=1',
            dash_playback_url: 'https://example.com/live-hd.mpd?foo=1',
            cover_frame_url: 'https://example.com/cover.jpg',
        }),
    ).toEqual({
        broadcastStatus: 'active',
        coverUrl: 'https://example.com/cover.jpg',
        streamUrls: [
            'https://example.com/live-abr.mpd?foo=1',
            'https://example.com/live-hd.mpd?foo=1',
        ],
        expireAt: null,
    })
})

test('instagram live relay post-live grace window keeps recent captures only', () => {
    const now = Date.UTC(2026, 2, 20, 6, 30, 0)

    expect(isPostLiveGraceActive('2026-03-20T06:00:00.000Z', 3 * 60 * 60, now)).toBeTrue()
    expect(isPostLiveGraceActive('2026-03-19T23:00:00.000Z', 3 * 60 * 60, now)).toBeFalse()
    expect(N2NJ_REQUEST_USER_AGENT).toBe('N2NJ-Stream-Bot/1.0')
})

test('instagram live relay target config preserves zero sync interval and falls back on invalid seconds', () => {
    const service = new InstagramLiveRelayService('/tmp/instagram-live-relay-test')

    const zeroInterval = (service as any).resolveTargetConfig('shiina_satsuki227', {
        enabled: true,
        sync_interval_seconds: 0,
        post_live_grace_seconds: 0,
    })
    expect(zeroInterval.sync_interval_seconds).toBe(0)
    expect(zeroInterval.post_live_grace_seconds).toBe(0)

    const invalidInterval = (service as any).resolveTargetConfig('shiina_satsuki227', {
        enabled: true,
        sync_interval_seconds: 'wat',
        post_live_grace_seconds: 'wat',
    })
    expect(invalidInterval.sync_interval_seconds).toBe(300)
    expect(invalidInterval.post_live_grace_seconds).toBe(6 * 60 * 60)
})

// ---------------------------------------------------------------------------
// P5: broadcast_status state machine (intel §4.1) + expire_at freshness
// (intel §4.2) + held-manifest grace (intel §4.3)
// ---------------------------------------------------------------------------

test('instagram live broadcast status classifier maps the 11-value enum', () => {
    const classify = classifyInstagramBroadcastStatus
    // active family
    expect(classify('active')).toBe('active')
    expect(classify(null)).toBe('active')
    expect(classify('')).toBe('active')
    // terminal
    expect(classify('stopped')).toBe('terminal')
    expect(classify('hard_stop')).toBe('terminal')
    expect(classify('HARD_STOP')).toBe('terminal')
    // post_live family
    expect(classify('post_live')).toBe('post_live')
    expect(classify('post_live_posting')).toBe('post_live')
    expect(classify('post_live_posting_failed')).toBe('post_live')
    expect(classify('post_live_posting_initiated')).toBe('post_live')
    expect(classify('post_live_post_request_failed')).toBe('post_live')
    // non-terminal
    expect(classify('interrupted')).toBe('non_terminal')
    expect(classify('hidden')).toBe('non_terminal')
    expect(classify('unknown')).toBe('non_terminal')
    // novel values degrade non-terminal: an unrecognized status must not kill a capture
    expect(classify('some_future_status')).toBe('non_terminal')
})

test('instagram live web_info parser carries expire_at in epoch seconds', () => {
    expect(
        parseInstagramLiveWebInfo({
            broadcast_status: 'active',
            dash_abr_playback_url: 'https://example.com/live.mpd',
            expire_at: 1773845200,
        }),
    ).toMatchObject({ expireAt: 1773845200 })
    // absent / nonsense → null
    expect(parseInstagramLiveWebInfo({ broadcast_status: 'active' }).expireAt).toBeNull()
    expect(parseInstagramLiveWebInfo({ expire_at: 'nope' }).expireAt).toBeNull()
    expect(parseInstagramLiveWebInfo({ expire_at: 0 }).expireAt).toBeNull()
})

test('instagram live post-live package is held within the 300s manifest grace window', async () => {
    const service = new InstagramLiveRelayService('/tmp/instagram-live-relay-held-test') as any
    const now = Date.now()
    const previousCache = {
        handle: 'held_handle',
        profileUrl: 'https://www.instagram.com/held_handle/',
        checkedAt: new Date(now - 60_000).toISOString(),
        isLive: false,
        liveBroadcastId: '42',
        liveBroadcastVisibility: null,
        liveUrl: null,
        displayName: 'Held',
        avatarUrl: null,
        lastLiveAt: new Date(now - 60_000).toISOString(),
        package: {
            mode: 'echo',
            page_url: 'https://www.instagram.com/held_handle/live/',
            timestamp: now - 60_000,
            lastRefreshedAt: now - 60_000,
            broadcast_status: 'post_live',
            cookies_b64: '',
            streams_detected: 1,
            streams: [
                {
                    source: 'https://example.invalid/live.mpd',
                    type: 'DASH',
                    headers: {},
                    mediaInfo: { size: 1, variants_count: 1, variants: [], encrypted: false, pssh: null },
                },
            ],
            licenses: [],
            keys: [],
        },
        archive: null,
        syncedAt: new Date(now - 60_000).toISOString(),
        relay: { baseUrl: 'https://player', playerId: 'relay', active: true },
    }
    const relayConfig = {
        post_live_grace_seconds: 6 * 60 * 60,
        sync_interval_seconds: 300,
    }

    // Manifest fetch fails (example.invalid is unreachable / 4xx): within the
    // 300s held window the previous package must be kept, not dropped.
    const held = await service.refreshPostLivePackage(previousCache, relayConfig)
    expect(held).not.toBeNull()
    expect(held.streams_detected).toBe(1)
    expect(held.lastRefreshedAt).toBe(now - 60_000)

    // Outside the window (package last refreshed 400s ago): refresh failure now
    // legitimately ends the replay (returns null → caller stops the relay).
    const staleCache = {
        ...previousCache,
        package: { ...previousCache.package, lastRefreshedAt: now - 400_000, timestamp: now - 400_000 },
    }
    const dropped = await service.refreshPostLivePackage(staleCache, relayConfig)
    expect(dropped).toBeNull()
})
