import { test, expect } from 'bun:test'
import DB from '@/db'
import { HostPacer } from '@idol-bbq-utils/spider'
import {
    buildCaptureCommand,
    buildShowroomCaptureCommand,
    buildTikTokCaptureCommand,
    fetchShowroomStreamingUrl,
    parseTwitchLiveDump,
    probeShowroomDetail,
    probeShowroomLive,
    probeTikTokLive,
    stripStreamUrlQuery,
    LiveCaptureExecutor,
    type CaptureSessionLike,
} from './live-capture-executor-service'

function makePlan(overrides: Record<string, any> = {}) {
    return {
        schema_version: 1,
        target: { platform: 'twitch', handle: 'sallyamaki' },
        event: { starts_at: 1780000000, starts_at_iso: '', timezone: 'Asia/Tokyo', title: 't' },
        window: {
            before_minutes: 10,
            after_minutes: 240,
            opens_at: 1779999400,
            opens_at_iso: '',
            closes_at: 1780002400,
            closes_at_iso: '',
        },
        capture: {
            poll_seconds: 15,
            first_byte_timeout_seconds: 30,
            quality_order: ['origin_rtmp', 'hd_flv', 'hd_hls'],
            upload: false,
        },
        source: { kind: 'manual' },
        tags: [],
        ...overrides,
    } as any
}

test('parseTwitchLiveDump recognizes live states', () => {
    expect(parseTwitchLiveDump({ is_live: true })).toBe(true)
    expect(parseTwitchLiveDump({ live_status: 'is_live' })).toBe(true)
    expect(parseTwitchLiveDump({ is_live: false, live_status: 'is_upcoming' })).toBe(false)
    expect(parseTwitchLiveDump({ live_status: 'is_not_live' })).toBe(false)
    expect(parseTwitchLiveDump(null)).toBe(false)
    expect(parseTwitchLiveDump({})).toBe(false)
})

test('probeShowroomLive parses the native status API', async () => {
    const fetchMock = async (url: string) => {
        expect(url).toContain('/api/room/status?room_url_key=nao_aikawa227')
        return new Response(JSON.stringify({ is_live: true, room_id: 564018 }), { status: 200 })
    }
    expect(await probeShowroomLive('nao_aikawa227', fetchMock as any, 5000)).toBe(true)
    expect(
        await probeShowroomLive(
            'nao_aikawa227',
            (async () => new Response(JSON.stringify({ is_live: false }), { status: 200 })) as any,
            5000,
        ),
    ).toBe(false)
    expect(
        await probeShowroomLive('nao_aikawa227', (async () => new Response('', { status: 429 })) as any, 5000),
    ).toBe(false)
    expect(
        await probeShowroomLive(
            'nao_aikawa227',
            (async () => {
                throw new Error('boom')
            }) as any,
            5000,
        ),
    ).toBe(false)
})

test('probeShowroomDetail returns the room id alongside live state', async () => {
    const ok = await probeShowroomDetail(
        'nanabun3rd',
        (async () => new Response(JSON.stringify({ is_live: true, room_id: 564018 }), { status: 200 })) as any,
        5000,
    )
    expect(ok).toEqual({ status: 'live', roomId: 564018 })
    const offline = await probeShowroomDetail(
        'nanabun3rd',
        (async () => new Response(JSON.stringify({ is_live: false, room_id: 564018 }), { status: 200 })) as any,
        5000,
    )
    expect(offline).toEqual({ status: 'offline', roomId: 564018 })
    const noRoom = await probeShowroomDetail(
        'nanabun3rd',
        (async () => new Response(JSON.stringify({ is_live: true }), { status: 200 })) as any,
        5000,
    )
    expect(noRoom).toEqual({ status: 'live', roomId: null })
    const failed = await probeShowroomDetail(
        'nanabun3rd',
        (async () => new Response('', { status: 429 })) as any,
        5000,
    )
    expect(failed).toEqual({ status: 'unknown', roomId: null })
})

test('fetchShowroomStreamingUrl resolves the master playlist from streaming_url_list', async () => {
    const m3u8 = 'https://hls.example.com/master.m3u8?expire=1\\u0026token=abc'
    const url = await fetchShowroomStreamingUrl(
        564018,
        (async (u: string) => {
            expect(u).toContain('/api/live/streaming_url?room_id=564018&')
            return new Response(
                JSON.stringify({
                    streaming_url_list: [
                        { type: 'hls', url: 'https://hls.example.com/sub.m3u8' },
                        { type: 'hls_all', url: m3u8 },
                    ],
                }),
                { status: 200 },
            )
        }) as any,
        5000,
    )
    expect(url).toBe('https://hls.example.com/master.m3u8?expire=1&token=abc')
    const noList = await fetchShowroomStreamingUrl(
        564018,
        (async () => new Response(JSON.stringify({ streaming_url_list: [] }), { status: 200 })) as any,
        5000,
    )
    expect(noList).toBeNull()
    const offline = await fetchShowroomStreamingUrl(
        564018,
        (async () => new Response(JSON.stringify({ streaming_url_list: false }), { status: 200 })) as any,
        5000,
    )
    expect(offline).toBeNull()
})

test('buildShowroomCaptureCommand emits an ffmpeg HLS copy', () => {
    const { bin, args } = buildShowroomCaptureCommand('/tmp/live-capture/showroom/h/1', 'https://hls.example.com/x.m3u8', '2026-08-10T19-20-00')
    expect(bin).toBe('ffmpeg')
    const joined = args.join(' ')
    expect(joined).toContain('https://hls.example.com/x.m3u8')
    expect(joined).toContain('/tmp/live-capture/showroom/h/1/2026-08-10T19-20-00.mkv')
    expect(joined).toContain('-c copy')
})

test('buildCaptureCommand emits twitch live-from-start capture', () => {
    const { bin, args } = buildCaptureCommand(makePlan(), '/tmp/live-capture/twitch/h/1', '/usr/local/bin/yt-dlp')
    expect(bin).toBe('/usr/local/bin/yt-dlp')
    expect(args).toContain('--live-from-start')
    expect(args).toContain('--no-playlist')
    expect(args.join(' ')).toContain('/tmp/live-capture/twitch/h/1/')
    expect(args.join(' ')).toContain('https://www.twitch.tv/sallyamaki')
})

test('buildCaptureCommand uses plan url and skips live-from-start for non-twitch', () => {
    const twitchWithUrl = buildCaptureCommand(
        makePlan({ target: { platform: 'twitch', handle: 'x', url: 'https://www.twitch.tv/x/videos' } }),
        '/tmp/x',
    )
    expect(twitchWithUrl.args.join(' ')).toContain('https://www.twitch.tv/x/videos')
    expect(twitchWithUrl.args).toContain('--live-from-start')
})

/**
 * TikTok chain-B probe tests use synthetic hydration fixtures only (structure
 * mirrors the sa7 evidence page); all hosts/ids/signs are fake and no network
 * requests are made — fetchPage is always injected.
 */
const NOOP_PACER = new HostPacer(0)

function tiktokLivePageHtml(live: boolean) {
    const sigi = live
        ? {
              LiveRoom: {
                  liveRoomUserInfo: {
                      user: { uniqueId: 'examplelive', roomId: '7600000000000000001', status: 2 },
                      liveRoom: {
                          status: 2,
                          title: 'Synthetic Live Title',
                          streamData: {
                              pull_data: {
                                  options: { qualities: [{ sdk_key: 'hd', level: 3, name: '720p' }] },
                                  stream_data: JSON.stringify({
                                      data: {
                                          hd: {
                                              main: {
                                                  hls: 'https://pull-hls-example.invalid/stream-1/index.m3u8?expire=1000000000&sign=0123456789abcdef0123456789abcdef',
                                              },
                                          },
                                      },
                                  }),
                              },
                          },
                      },
                  },
              },
          }
        : { CurrentRoom: { roomInfo: null, roomId: '' } }
    return `<html><body><script id="SIGI_STATE" type="application/json">${JSON.stringify(sigi)}</script></body></html>`
}

test('probeTikTokLive maps a live hydration page to live + m3u8', async () => {
    let seenUrl = ''
    const result = await probeTikTokLive('examplelive', {
        pacer: NOOP_PACER,
        fetchPage: async (url) => {
            seenUrl = url
            return tiktokLivePageHtml(true)
        },
    })
    expect(result.status).toBe('live')
    expect(result.m3u8).toContain('index.m3u8')
    expect(result.title).toBe('Synthetic Live Title')
    expect(seenUrl).toBe('https://www.tiktok.com/@examplelive/live')
})

test('probeTikTokLive maps confirmed off-air to offline, WAF challenge to unknown', async () => {
    const offline = await probeTikTokLive('examplelive', {
        pacer: NOOP_PACER,
        fetchPage: async () => tiktokLivePageHtml(false),
    })
    expect(offline.status).toBe('offline')
    expect(offline.m3u8).toBeNull()

    const challenge = await probeTikTokLive('examplelive', {
        pacer: NOOP_PACER,
        fetchPage: async () => '<html><head><script>window._slardar_challenge=true</script></head><body></body></html>',
    })
    expect(challenge.status).toBe('unknown')
    expect(challenge.reason).toContain('SIGI_STATE')

    const failed = await probeTikTokLive('examplelive', {
        pacer: NOOP_PACER,
        fetchPage: async () => {
            throw new Error('boom')
        },
    })
    expect(failed.status).toBe('unknown')
})

test('probeTikTokLive waits on the per-host pacer before fetching', async () => {
    const events: Array<string> = []
    const pacer = new HostPacer(0, Date.now, async () => {
        events.push('paced')
    })
    await probeTikTokLive('examplelive', {
        pacer,
        fetchPage: async () => {
            events.push('fetched')
            return tiktokLivePageHtml(true)
        },
    })
    // interval is 0 so no sleep happens, but waitTurn must run first
    expect(events).toEqual(['fetched'])
    const ordered: Array<string> = []
    // fixed clock well past the interval: the first waitTurn is free, the
    // second (the probe's) must sleep before the fetch runs
    const slowPacer = new HostPacer(60_000, () => 1_000_000, async () => {
        ordered.push('slept')
    })
    await slowPacer.waitTurn('www.tiktok.com')
    await probeTikTokLive('examplelive', {
        pacer: slowPacer,
        fetchPage: async () => {
            ordered.push('fetched')
            return tiktokLivePageHtml(true)
        },
    })
    expect(ordered).toEqual(['slept', 'fetched'])
})

test('stripStreamUrlQuery removes the credential query from stream urls', () => {
    expect(stripStreamUrlQuery('https://pull.example.invalid/x/index.m3u8?expire=1&sign=abc')).toBe(
        'https://pull.example.invalid/x/index.m3u8',
    )
    expect(stripStreamUrlQuery('https://pull.example.invalid/x/index.m3u8')).toBe(
        'https://pull.example.invalid/x/index.m3u8',
    )
})

test('buildTikTokCaptureCommand emits an ffmpeg HLS copy and keeps the signed url in argv only', () => {
    const signed = 'https://pull-hls-example.invalid/stream-1/index.m3u8?expire=1000000000&sign=fake'
    const { bin, args } = buildTikTokCaptureCommand('/tmp/live-capture/tiktok/h/1', signed, '2026-08-21T19-20-00')
    expect(bin).toBe('ffmpeg')
    expect(args).toContain('-c')
    expect(args).toContain('copy')
    // ffmpeg argv must carry the full signed url…
    expect(args).toContain(signed)
    expect(args.join(' ')).toContain('/tmp/live-capture/tiktok/h/1/2026-08-21T19-20-00.mkv')
    // …while anything loggable uses the stripped form
    expect(stripStreamUrlQuery(signed)).not.toContain('sign=')
})

test('LiveCaptureExecutor completes expired plans and claims open windows', async () => {
    const now = Math.floor(Date.now() / 1000)
    const tasks: Array<{ id: number; status: string; payload: any }> = [
        { id: 1, status: 'planned', payload: makePlan({ window: { opens_at: now - 60, closes_at: now + 600 } }) },
        { id: 2, status: 'planned', payload: makePlan({ window: { opens_at: now - 7200, closes_at: now - 3600 } }) },
    ]
    const updates: Array<{ id: number; status: string }> = []
    const startedSessions: Array<number> = []

    const originalGetDue = DB.TaskQueue.getDue
    const originalUpdate = DB.TaskQueue.updateTaskStatus
    const originalClaimPlanned = DB.TaskQueue.claimPlanned
    const claimedPlans: Array<number> = []
    DB.TaskQueue.getDue = (async () => tasks) as any
    DB.TaskQueue.updateTaskStatus = (async (id: number, data: { status: string }) => {
        updates.push({ id, status: data.status })
    }) as any
    DB.TaskQueue.claimPlanned = (async (id: number) => {
        claimedPlans.push(id)
        return { id, status: 'pending' }
    }) as any

    const fakeFactory = (task: { id: number; payload: any }): CaptureSessionLike => {
        startedSessions.push(task.id)
        return {
            id: task.id,
            finished: true,
            run: () => {},
            stop: async () => {},
        }
    }

    try {
        const executor = new LiveCaptureExecutor({
            config: { enabled: true, scan_interval_seconds: 5, max_concurrent_sessions: 4 },
            log: undefined,
            sessionFactory: fakeFactory,
        })
        await executor.tick()
        expect(updates).toContainEqual({ id: 2, status: 'completed' })
        expect(claimedPlans).toEqual([1])
        expect(startedSessions).toContain(1)
        await executor.stop()
    } finally {
        DB.TaskQueue.getDue = originalGetDue
        DB.TaskQueue.updateTaskStatus = originalUpdate
        DB.TaskQueue.claimPlanned = originalClaimPlanned
    }
})

test('LiveCaptureExecutor with zero concurrency only reaps expired plans', async () => {
    const now = Math.floor(Date.now() / 1000)
    const tasks: Array<{ id: number; status: string; payload: any }> = [
        { id: 1, status: 'planned', payload: makePlan({ window: { opens_at: now - 60, closes_at: now + 600 } }) },
        { id: 2, status: 'planned', payload: makePlan({ window: { opens_at: now - 7200, closes_at: now - 3600 } }) },
    ]
    const updates: Array<{ id: number; status: string }> = []
    const startedSessions: Array<number> = []

    const originalGetDue = DB.TaskQueue.getDue
    const originalUpdate = DB.TaskQueue.updateTaskStatus
    DB.TaskQueue.getDue = (async () => tasks) as any
    DB.TaskQueue.updateTaskStatus = (async (id: number, data: { status: string }) => {
        updates.push({ id, status: data.status })
    }) as any

    try {
        const executor = new LiveCaptureExecutor({
            config: { enabled: true, scan_interval_seconds: 5, max_concurrent_sessions: 0 },
            log: undefined,
            sessionFactory: (task) => {
                startedSessions.push(task.id)
                return { id: task.id, finished: true, run: () => {}, stop: async () => {} }
            },
        })
        await executor.tick()
        expect(updates).toEqual([{ id: 2, status: 'completed' }])
        expect(startedSessions).toEqual([])
        await executor.stop()
    } finally {
        DB.TaskQueue.getDue = originalGetDue
        DB.TaskQueue.updateTaskStatus = originalUpdate
    }
})
