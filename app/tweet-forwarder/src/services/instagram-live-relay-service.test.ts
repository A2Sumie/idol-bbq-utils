import { expect, test } from 'bun:test'
import {
    analyzeManifestText,
    buildPlayerUrl,
    filterRelayHeaders,
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
    expect(buildPlayerUrl('relay')).toBe('https://stream.n2nj.moe/relay.m3u8')
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
    })
})
