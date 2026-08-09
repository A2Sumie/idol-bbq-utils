import { test, expect } from 'bun:test'
import DB from '@/db'
import {
    buildCaptureCommand,
    parseTwitchLiveDump,
    probeShowroomLive,
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
        return new Response(JSON.stringify({ is_live: true }), { status: 200 })
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

test('buildCaptureCommand emits twitch live-from-start capture', () => {
    const { bin, args } = buildCaptureCommand(makePlan(), '/tmp/live-capture/twitch/h/1', '/usr/local/bin/yt-dlp')
    expect(bin).toBe('/usr/local/bin/yt-dlp')
    expect(args).toContain('--live-from-start')
    expect(args).toContain('--no-playlist')
    expect(args.join(' ')).toContain('/tmp/live-capture/twitch/h/1/')
    expect(args.join(' ')).toContain('https://www.twitch.tv/sallyamaki')
})

test('buildCaptureCommand uses plan url and skips live-from-start for showroom', () => {
    const showroom = buildCaptureCommand(
        makePlan({
            target: {
                platform: 'showroom',
                handle: 'nao_aikawa227',
                url: 'https://www.showroom-live.com/nao_aikawa227',
            },
        }),
        '/tmp/live-capture/showroom/h/1',
    )
    expect(showroom.args).not.toContain('--live-from-start')
    expect(showroom.args.join(' ')).toContain('https://www.showroom-live.com/nao_aikawa227')
    const twitchWithUrl = buildCaptureCommand(
        makePlan({ target: { platform: 'twitch', handle: 'x', url: 'https://www.twitch.tv/x/videos' } }),
        '/tmp/x',
    )
    expect(twitchWithUrl.args.join(' ')).toContain('https://www.twitch.tv/x/videos')
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
    DB.TaskQueue.getDue = (async () => tasks) as any
    DB.TaskQueue.updateTaskStatus = (async (id: number, data: { status: string }) => {
        updates.push({ id, status: data.status })
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
        expect(updates).toContainEqual({ id: 1, status: 'pending' })
        expect(startedSessions).toContain(1)
        await executor.stop()
    } finally {
        DB.TaskQueue.getDue = originalGetDue
        DB.TaskQueue.updateTaskStatus = originalUpdate
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
