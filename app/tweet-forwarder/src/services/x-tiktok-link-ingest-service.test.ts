import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import {
    enqueueMissingYouTubeLinksFromXArticle,
    enqueueMissingTikTokLinksFromXArticle,
    extractTikTokLinksFromText,
    extractYouTubeLinksFromText,
    parseYouTubeUrl,
    parseTikTokUrl,
} from './x-tiktok-link-ingest-service'

test('extractTikTokLinksFromText finds TikTok short links without trailing punctuation', () => {
    expect(
        extractTikTokLinksFromText(
            'TikTok更新しましタオ〜\nhttps://vt.tiktok.com/ZSQEJtEu6/ 。 and https://www.tiktok.com/@u/video/7653464242506616085?x=1',
        ),
    ).toEqual(['https://vt.tiktok.com/ZSQEJtEu6/', 'https://www.tiktok.com/@u/video/7653464242506616085?x=1'])
})

test('parseTikTokUrl resolves canonical video metadata', () => {
    expect(parseTikTokUrl('https://www.tiktok.com/@tabesugiyaseruzo/video/7653464242506616085?_r=1')).toMatchObject({
        videoId: '7653464242506616085',
        username: 'tabesugiyaseruzo',
        profileUrl: 'https://www.tiktok.com/@tabesugiyaseruzo',
        resolvedUrl: 'https://www.tiktok.com/@tabesugiyaseruzo/video/7653464242506616085',
    })
})

test('extractYouTubeLinksFromText finds YouTube watch and short links without trailing punctuation', () => {
    expect(
        extractYouTubeLinksFromText(
            '新動画 https://www.youtube.com/watch?v=PWUNnCNTOLk。 shorts https://youtube.com/shorts/iYbrU0efOGw?feature=share,',
        ),
    ).toEqual([
        'https://www.youtube.com/watch?v=PWUNnCNTOLk',
        'https://youtube.com/shorts/iYbrU0efOGw?feature=share',
    ])
})

test('parseYouTubeUrl resolves canonical video metadata', () => {
    expect(parseYouTubeUrl('https://youtu.be/PWUNnCNTOLk?si=abc')).toMatchObject({
        videoId: 'PWUNnCNTOLk',
        watchUrl: 'https://www.youtube.com/watch?v=PWUNnCNTOLk',
        resolvedUrl: 'https://www.youtube.com/watch?v=PWUNnCNTOLk',
    })
    expect(parseYouTubeUrl('https://www.youtube.com/live/iYbrU0efOGw?feature=share')).toMatchObject({
        videoId: 'iYbrU0efOGw',
        watchUrl: 'https://www.youtube.com/watch?v=iYbrU0efOGw',
    })
})

test('enqueueMissingTikTokLinksFromXArticle queues missing TikTok profile crawl from X short link', async () => {
    const originalGetByArticleCode = DB.Article.getByArticleCode
    const originalTaskAdd = DB.TaskQueue.add
    const adds: any[] = []

    ;(DB.Article as any).getByArticleCode = async () => null
    ;(DB.TaskQueue as any).add = async (type: string, payload: any, executeAt: number, meta: any) => {
        adds.push({ type, payload, executeAt, meta })
        return { id: 227, status: 'pending' }
    }

    try {
        const queued = await enqueueMissingTikTokLinksFromXArticle(
            {
                id: 9506,
                platform: Platform.X,
                a_id: '2068685300046700614',
                content: 'https://vt.tiktok.com/ZSQEJtEu6/',
            } as any,
            {
                now: 1782048000,
                fetchImpl: (async () =>
                    ({
                        url: 'https://www.tiktok.com/@tabesugiyaseruzo/video/7653464242506616085?_r=1',
                    }) as Response) as any,
            },
        )

        expect(queued).toEqual([
            {
                videoId: '7653464242506616085',
                profileUrl: 'https://www.tiktok.com/@tabesugiyaseruzo',
                taskQueueId: 227,
                status: 'pending',
            },
        ])
        expect(adds[0]).toMatchObject({
            type: DB.TaskQueue.TYPE.ScheduledCrawlerRun,
            payload: {
                crawler: 'Tiktok抓取',
                websites: ['https://www.tiktok.com/@tabesugiyaseruzo/video/7653464242506616085'],
                reason: 'x tiktok link 2068685300046700614',
            },
            executeAt: 1782048000,
            meta: {
                source_ref: 'x-tiktok-link:2068685300046700614',
                action_type: 'x_tiktok_link_ingest',
            },
        })
        expect(adds[0].meta.idempotency_key).toBeTruthy()
    } finally {
        ;(DB.Article as any).getByArticleCode = originalGetByArticleCode
        ;(DB.TaskQueue as any).add = originalTaskAdd
    }
})

test('enqueueMissingYouTubeLinksFromXArticle queues missing YouTube crawler run from X link', async () => {
    const originalGetByArticleCode = DB.Article.getByArticleCode
    const originalTaskAdd = DB.TaskQueue.add
    const adds: any[] = []

    ;(DB.Article as any).getByArticleCode = async () => null
    ;(DB.TaskQueue as any).add = async (type: string, payload: any, executeAt: number, meta: any) => {
        adds.push({ type, payload, executeAt, meta })
        return { id: 438, status: 'pending' }
    }

    try {
        const queued = await enqueueMissingYouTubeLinksFromXArticle(
            {
                id: 9510,
                platform: Platform.X,
                a_id: '2069000000000000000',
                content: '莎莉新動画 https://youtu.be/PWUNnCNTOLk?si=abc',
            } as any,
            { now: 1782214209 },
        )

        expect(queued).toEqual([
            {
                videoId: 'PWUNnCNTOLk',
                watchUrl: 'https://www.youtube.com/watch?v=PWUNnCNTOLk',
                taskQueueId: 438,
                status: 'pending',
            },
        ])
        expect(adds[0]).toMatchObject({
            type: DB.TaskQueue.TYPE.ScheduledCrawlerRun,
            payload: {
                crawler: 'YouTube抓取',
                reason: 'x youtube link 2069000000000000000',
            },
            executeAt: 1782214209,
            meta: {
                source_ref: 'x-youtube-link:2069000000000000000',
                action_type: 'x_youtube_link_ingest',
            },
        })
        expect(adds[0].payload.websites).toBeUndefined()
        expect(adds[0].meta.idempotency_key).toBeTruthy()
    } finally {
        ;(DB.Article as any).getByArticleCode = originalGetByArticleCode
        ;(DB.TaskQueue as any).add = originalTaskAdd
    }
})

test('enqueueMissingYouTubeLinksFromXArticle skips YouTube videos already in DB', async () => {
    const originalGetByArticleCode = DB.Article.getByArticleCode
    const originalTaskAdd = DB.TaskQueue.add
    let addCalls = 0

    ;(DB.Article as any).getByArticleCode = async () => ({ id: 438 })
    ;(DB.TaskQueue as any).add = async () => {
        addCalls += 1
    }

    try {
        const queued = await enqueueMissingYouTubeLinksFromXArticle(
            {
                platform: Platform.X,
                a_id: 'x-youtube-1',
                content: 'https://www.youtube.com/watch?v=PWUNnCNTOLk',
            } as any,
            { now: 1782214209 },
        )

        expect(queued).toEqual([])
        expect(addCalls).toBe(0)
    } finally {
        ;(DB.Article as any).getByArticleCode = originalGetByArticleCode
        ;(DB.TaskQueue as any).add = originalTaskAdd
    }
})

test('enqueueMissingTikTokLinksFromXArticle skips TikTok videos already in DB', async () => {
    const originalGetByArticleCode = DB.Article.getByArticleCode
    const originalTaskAdd = DB.TaskQueue.add
    let addCalls = 0

    ;(DB.Article as any).getByArticleCode = async () => ({ id: 765 })
    ;(DB.TaskQueue as any).add = async () => {
        addCalls += 1
    }

    try {
        const queued = await enqueueMissingTikTokLinksFromXArticle(
            {
                platform: Platform.X,
                a_id: 'x-1',
                content: 'https://www.tiktok.com/@tabesugiyaseruzo/video/7653464242506616085',
            } as any,
            { now: 1782048000 },
        )

        expect(queued).toEqual([])
        expect(addCalls).toBe(0)
    } finally {
        ;(DB.Article as any).getByArticleCode = originalGetByArticleCode
        ;(DB.TaskQueue as any).add = originalTaskAdd
    }
})
