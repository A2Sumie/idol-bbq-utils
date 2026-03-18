import dayjs from 'dayjs'
import { test, expect } from 'bun:test'
import { spiderRegistry } from '../src'
import { YoutubeApiJsonParser, YoutubeSpider } from '../src/spiders/youtube'

const channelHeaderFixture = {
    c4TabbedHeaderRenderer: {
        title: 'Anime English Club',
        channelHandleText: {
            runs: [{ text: '@anime-english-club' }],
        },
        avatar: {
            thumbnails: [
                { url: '//yt3.example.com/s48.jpg', width: 48 },
                { url: '//yt3.example.com/s176.jpg', width: 176 },
            ],
        },
    },
}

const videosFixture = {
    header: channelHeaderFixture,
    richGridRenderer: {
        contents: [{
            richItemRenderer: {
                content: {
                    videoRenderer: {
                        videoId: 'bBRUMp_WNUU',
                        title: {
                            runs: [{ text: 'New music video' }],
                        },
                        descriptionSnippet: {
                            runs: [{ text: 'Official upload' }],
                        },
                        publishedTimeText: {
                            simpleText: '4 days ago',
                        },
                        thumbnail: {
                            thumbnails: [
                                { url: 'https://i.ytimg.com/vi/bBRUMp_WNUU/hqdefault.jpg', width: 480 },
                            ],
                        },
                    },
                },
            },
        }],
    },
}

const shortsFixture = {
    header: channelHeaderFixture,
    richGridRenderer: {
        contents: [{
            richItemRenderer: {
                content: {
                    shortsLockupViewModel: {
                        entityId: 'shorts-shelf-item-NYnbjoDltqA',
                        overlayMetadata: {
                            primaryText: {
                                content: 'Behind the scenes short',
                            },
                        },
                        thumbnail: {
                            sources: [
                                { url: 'https://i.ytimg.com/vi/NYnbjoDltqA/oar2.jpg', width: 720 },
                            ],
                        },
                        onTap: {
                            innertubeCommand: {
                                reelWatchEndpoint: {
                                    videoId: 'NYnbjoDltqA',
                                },
                            },
                        },
                    },
                },
            },
        }],
    },
}

test('YouTube Spider URL Validation supports hyphenated handles', () => {
    const url = 'https://www.youtube.com/@anime-english-club'
    const plugin = spiderRegistry.findByUrl(url)

    expect(plugin).not.toBeNull()
    expect(plugin?.id).toBe('youtube')

    if (plugin) {
        const spider = plugin.create()
        const match = spider._match_valid_url(url, YoutubeSpider)
        expect(match?.groups?.id).toBe('anime-english-club')
    }
})

test('YouTube videos parser extracts channel videos', () => {
    const channelMeta = YoutubeApiJsonParser.channelMetaParser(videosFixture, '@fallback')
    const videos = YoutubeApiJsonParser.videosParser(videosFixture, channelMeta)

    expect(videos).toHaveLength(1)
    expect(videos[0]?.type).toBe('video')
    expect(videos[0]?.u_id).toBe('anime-english-club')
    expect(videos[0]?.username).toBe('Anime English Club')
    expect(videos[0]?.url).toBe('https://www.youtube.com/watch?v=bBRUMp_WNUU')
    expect(videos[0]?.media?.[0]).toEqual({
        type: 'video_thumbnail',
        url: 'https://i.ytimg.com/vi/bBRUMp_WNUU/hqdefault.jpg',
    })
})

test('YouTube shorts parser extracts channel shorts', () => {
    const channelMeta = YoutubeApiJsonParser.channelMetaParser(shortsFixture, '@fallback')
    const shorts = YoutubeApiJsonParser.shortsParser(shortsFixture, channelMeta)

    expect(shorts).toHaveLength(1)
    expect(shorts[0]?.type).toBe('shorts')
    expect(shorts[0]?.url).toBe('https://www.youtube.com/shorts/NYnbjoDltqA')
    expect(shorts[0]?.content).toBe('Behind the scenes short')
    expect(shorts[0]?.media?.[0]?.type).toBe('video_thumbnail')
})

test('YouTube detail parser extracts publish date and metadata', () => {
    const detailHtml = `<script>var ytInitialPlayerResponse = ${JSON.stringify({
        videoDetails: {
            title: 'Fresh upload',
            shortDescription: 'A brand new clip',
            thumbnail: {
                thumbnails: [
                    { url: 'https://i.ytimg.com/vi/bBRUMp_WNUU/maxresdefault.jpg', width: 1280 },
                ],
            },
        },
        microformat: {
            playerMicroformatRenderer: {
                publishDate: '2026-03-17',
                uploadDate: '2026-03-17',
                thumbnail: {
                    thumbnails: [
                        { url: 'https://i.ytimg.com/vi/bBRUMp_WNUU/hqdefault.jpg', width: 480 },
                    ],
                },
            },
        },
    })};</script>`

    const detail = YoutubeApiJsonParser.detailParser(detailHtml)

    expect(detail.created_at).toBe(dayjs('2026-03-17').unix())
    expect(detail.title).toBe('Fresh upload')
    expect(detail.description).toBe('A brand new clip')
    expect(detail.thumbnail).toBe('https://i.ytimg.com/vi/bBRUMp_WNUU/hqdefault.jpg')
})
