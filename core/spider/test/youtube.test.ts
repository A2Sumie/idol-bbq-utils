import dayjs from 'dayjs'
import { test, expect } from 'bun:test'
import { spiderRegistry } from '../src'
import { ArticleTypeEnum, YoutubeApiJsonParser, YoutubeSpider } from '../src/spiders/youtube'
import { HTTPClient } from '../src/utils'

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

const officialChannelHeaderFixture = {
    c4TabbedHeaderRenderer: {
        title: '22/7 OFFICIAL YouTube CHANNEL',
        channelHandleText: {
            runs: [{ text: '@227SMEJ' }],
        },
        avatar: {
            thumbnails: [
                { url: '//yt3.example.com/s48-official.jpg', width: 48 },
                { url: '//yt3.example.com/s176-official.jpg', width: 176 },
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

const lockupVideosFixture = {
    header: officialChannelHeaderFixture,
    richGridRenderer: {
        contents: [{
            richItemRenderer: {
                content: {
                    lockupViewModel: {
                        contentId: 'X6J9TphDexM',
                        contentType: 'LOCKUP_CONTENT_TYPE_VIDEO',
                        contentImage: {
                            thumbnailViewModel: {
                                image: {
                                    sources: [
                                        { url: 'https://i.ytimg.com/vi/X6J9TphDexM/hqdefault.jpg', width: 168 },
                                        { url: 'https://i.ytimg.com/vi/X6J9TphDexM/hqdefault.jpg', width: 336 },
                                    ],
                                },
                            },
                        },
                        metadata: {
                            lockupMetadataViewModel: {
                                title: {
                                    content: '22/7_the 3rd AUDITION DOCUMENTARY -Misaki Kitahara-',
                                },
                                metadata: {
                                    contentMetadataViewModel: {
                                        metadataRows: [{
                                            metadataParts: [
                                                { text: { content: '412 views' } },
                                                { text: { content: '46 minutes ago' }, accessibilityLabel: '46 minutes ago' },
                                            ],
                                        }],
                                    },
                                },
                            },
                        },
                        rendererContext: {
                            commandContext: {
                                onTap: {
                                    innertubeCommand: {
                                        watchEndpoint: {
                                            videoId: 'X6J9TphDexM',
                                        },
                                    },
                                },
                            },
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

function buildYoutubeInitialData(json: any) {
    return `<script>var ytInitialData = ${JSON.stringify(json)};</script>`
}

function buildYoutubeDetailHtml(videoId: string) {
    return `<script>var ytInitialPlayerResponse = ${JSON.stringify({
        videoDetails: {
            title: `Hydrated ${videoId}`,
            shortDescription: `Detail for ${videoId}`,
            thumbnail: {
                thumbnails: [{ url: `https://i.ytimg.com/vi/${videoId}/maxresdefault.jpg`, width: 1280 }],
            },
        },
        microformat: {
            playerMicroformatRenderer: {
                publishDate: '2026-03-17',
                uploadDate: '2026-03-17',
            },
        },
    })};</script>`
}

function buildYoutubePage() {
    return {
        browserContext: () => ({
            cookies: async () => [],
        }),
    } as any
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
    expect(videos[0]?.type).toBe(ArticleTypeEnum.VIDEO)
    expect(videos[0]?.u_id).toBe('anime-english-club')
    expect(videos[0]?.username).toBe('Anime English Club')
    expect(videos[0]?.url).toBe('https://www.youtube.com/watch?v=bBRUMp_WNUU')
    expect(videos[0]?.media?.[0]).toEqual({
        type: 'video_thumbnail',
        url: 'https://i.ytimg.com/vi/bBRUMp_WNUU/hqdefault.jpg',
    })
})

test('YouTube videos parser extracts current lockup view model videos', () => {
    const channelMeta = YoutubeApiJsonParser.channelMetaParser(lockupVideosFixture, '@fallback')
    const videos = YoutubeApiJsonParser.videosParser(lockupVideosFixture, channelMeta)

    expect(videos).toHaveLength(1)
    expect(videos[0]?.type).toBe(ArticleTypeEnum.VIDEO)
    expect(videos[0]?.a_id).toBe('X6J9TphDexM')
    expect(videos[0]?.u_id).toBe('227SMEJ')
    expect(videos[0]?.username).toBe('22/7 OFFICIAL YouTube CHANNEL')
    expect(videos[0]?.url).toBe('https://www.youtube.com/watch?v=X6J9TphDexM')
    expect(videos[0]?.content).toBe('22/7_the 3rd AUDITION DOCUMENTARY -Misaki Kitahara-')
    expect(videos[0]?.created_at).toBeGreaterThan(0)
    expect(videos[0]?.media?.[0]).toEqual({
        type: 'video_thumbnail',
        url: 'https://i.ytimg.com/vi/X6J9TphDexM/hqdefault.jpg',
    })
})

test('YouTube shorts parser extracts channel shorts', () => {
    const channelMeta = YoutubeApiJsonParser.channelMetaParser(shortsFixture, '@fallback')
    const shorts = YoutubeApiJsonParser.shortsParser(shortsFixture, channelMeta)

    expect(shorts).toHaveLength(1)
    expect(shorts[0]?.type).toBe(ArticleTypeEnum.SHORTS)
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

test('YouTube grabArticles bounds detail hydration to the newest configured limit', async () => {
    const originalDownload = HTTPClient.download_webpage
    const requestedUrls: Array<string> = []
    ;(HTTPClient as any).download_webpage = async (url: string) => {
        requestedUrls.push(url)
        if (url.includes('/videos?')) {
            return new Response(buildYoutubeInitialData(videosFixture))
        }
        if (url.includes('/shorts?')) {
            return new Response(buildYoutubeInitialData(shortsFixture))
        }
        const videoId = new URL(url).searchParams.get('v') || url.split('/').pop() || 'unknown'
        return new Response(buildYoutubeDetailHtml(videoId))
    }

    try {
        const articles = await YoutubeApiJsonParser.grabArticles(
            buildYoutubePage(),
            'https://www.youtube.com/@anime-english-club',
            {
                hydrate_limit: 1,
                hydrate_concurrency: 1,
            },
        )

        const detailRequests = requestedUrls.filter((url) => url.includes('/watch?') || url.includes('/shorts/'))
        expect(detailRequests).toHaveLength(1)
        expect(articles).toHaveLength(2)
        expect(articles.some((article) => article.content?.startsWith('Hydrated '))).toBeTrue()
    } finally {
        ;(HTTPClient as any).download_webpage = originalDownload
    }
})

test('YouTube detail parser marks upcoming premieres and scheduled start', () => {
    const detailHtml = `<script>var ytInitialPlayerResponse = ${JSON.stringify({
        playabilityStatus: {
            status: 'LIVE_STREAM_OFFLINE',
        },
        videoDetails: {
            title: 'Coming Soon...',
            isUpcoming: true,
            shortDescription: '',
            thumbnail: {
                thumbnails: [{ url: 'https://i.ytimg.com/vi/premiere/maxresdefault.jpg', width: 1280 }],
            },
        },
        microformat: {
            playerMicroformatRenderer: {
                liveBroadcastDetails: {
                    startTimestamp: '2026-07-08T11:55:00Z',
                },
            },
        },
    })};</script>`

    const detail = YoutubeApiJsonParser.detailParser(detailHtml)

    expect(detail.is_premiere_pending).toBeTrue()
    expect(detail.scheduled_start_at).toBe(dayjs('2026-07-08T11:55:00Z').unix())
    expect(detail.created_at).toBe(dayjs('2026-07-08T11:55:00Z').unix())
})

test('YouTube grabArticles rehydrates known premiere placeholders', async () => {
    const originalDownload = HTTPClient.download_webpage
    const requestedUrls: Array<string> = []
    const premiereVideos = {
        header: officialChannelHeaderFixture,
        richGridRenderer: {
            contents: [{
                richItemRenderer: {
                    content: {
                        videoRenderer: {
                            videoId: 'premiere-known',
                            title: { runs: [{ text: 'Coming Soon...' }] },
                            publishedTimeText: { simpleText: 'Upcoming' },
                            thumbnail: { thumbnails: [{ url: 'https://i.ytimg.com/vi/premiere-known/hqdefault.jpg', width: 480 }] },
                        },
                    },
                },
            }],
        },
    }
    ;(HTTPClient as any).download_webpage = async (url: string) => {
        requestedUrls.push(url)
        if (url.includes('/videos?')) {
            return new Response(buildYoutubeInitialData(premiereVideos))
        }
        if (url.includes('/shorts?')) {
            return new Response(buildYoutubeInitialData({ header: officialChannelHeaderFixture }))
        }
        return new Response(buildYoutubeDetailHtml('premiere-known'))
    }

    try {
        const articles = await YoutubeApiJsonParser.grabArticles(buildYoutubePage(), 'https://www.youtube.com/@227SMEJ', {
            hydrate_limit: 8,
            hydrate_concurrency: 1,
            isArticleKnown: (a_id) => a_id === 'premiere-known',
        })

        expect(requestedUrls.some((url) => url.includes('/watch?') && url.includes('premiere-known'))).toBeTrue()
        expect(articles.find((article) => article.a_id === 'premiere-known')?.content).toContain('Hydrated premiere-known')
    } finally {
        ;(HTTPClient as any).download_webpage = originalDownload
    }
})

test('YouTube grabArticles skips detail hydration for already-known articles', async () => {
    const originalDownload = HTTPClient.download_webpage
    const requestedUrls: Array<string> = []
    ;(HTTPClient as any).download_webpage = async (url: string) => {
        requestedUrls.push(url)
        if (url.includes('/videos?')) {
            return new Response(buildYoutubeInitialData(videosFixture))
        }
        if (url.includes('/shorts?')) {
            return new Response(buildYoutubeInitialData(shortsFixture))
        }
        const videoId = new URL(url).searchParams.get('v') || url.split('/').pop() || 'unknown'
        return new Response(buildYoutubeDetailHtml(videoId))
    }

    try {
        await YoutubeApiJsonParser.grabArticles(buildYoutubePage(), 'https://www.youtube.com/@anime-english-club', {
            hydrate_limit: 8,
            hydrate_concurrency: 2,
            isArticleKnown: (a_id) => a_id === 'bBRUMp_WNUU',
        })

        const detailRequests = requestedUrls.filter((url) => url.includes('/watch?') || url.includes('/shorts/'))
        expect(detailRequests).toHaveLength(1)
        expect(detailRequests[0]).toContain('/shorts/NYnbjoDltqA')
        expect(detailRequests[0]).not.toContain('bBRUMp_WNUU')
    } finally {
        ;(HTTPClient as any).download_webpage = originalDownload
    }
})
