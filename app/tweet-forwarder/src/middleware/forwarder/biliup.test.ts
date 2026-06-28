import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { BiliForwarder } from './bilibili'
import { processorRegistry } from '@/middleware/processor'
import DB from '@/db'
import {
    buildBiliupUploadCandidate,
    buildCookieDocument,
    completeBiliupUploadCandidateTags,
    normalizeBiliupCookieDocument,
    prepareUploadVideoParts,
    resolveBrowserCookieSyncConfig,
    resolveVideoUploadConfig,
} from './biliup'

test('buildBiliupUploadCandidate prepares metadata for YouTube video uploads', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.YouTube,
            u_id: '22_7_channel',
            username: '22/7 official',
            a_id: 'yt-abc',
            content: '22/7 新视频标题\n\n这里是简介',
            created_at: 1710900000,
            url: 'https://www.youtube.com/watch?v=yt-abc',
        } as any,
        ['22/7 新视频标题\n\n这里是简介'],
        [
            { media_type: 'video_thumbnail', path: '/tmp/cover.jpg' },
            { media_type: 'video', path: '/tmp/video.mp4' },
        ],
        {
            enabled: true,
            tags: ['长视频', '22/7'],
        },
    )

    expect(candidate).toBeTruthy()
    expect(candidate?.title).toBe('【22/7】22/7 新视频标题')
    expect(candidate?.coverPath).toBe('/tmp/cover.jpg')
    expect(candidate?.videoPaths).toEqual(['/tmp/video.mp4'])
    expect(candidate?.title).not.toContain('[YT]')
    expect(candidate?.config.tags).not.toContain('YouTube')
    expect(candidate?.config.tags).not.toContain('长视频')
})

test('buildBiliupUploadCandidate keeps YouTube Shorts original title at description top', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.YouTube,
            type: 'shorts',
            u_id: '22_7_channel',
            username: '22/7 official',
            a_id: 'yt-short-abc',
            content: 'ショート動画の原題\n\nショートの説明文',
            translation: '短视频中文标题\n\n短视频说明',
            created_at: 1710900000,
            url: 'https://www.youtube.com/shorts/yt-short-abc',
        } as any,
        [],
        [
            { media_type: 'video_thumbnail', path: '/tmp/short-cover.jpg' },
            { media_type: 'video', path: '/tmp/short-video.mp4' },
        ],
        {
            enabled: true,
        },
    )

    expect(candidate).toBeTruthy()
    expect(candidate?.title).toBe('【22/7】[YT] 短视频中文标题')
    expect(candidate?.description.split('\n')[0]).toBe('原标题: ショート動画の原題')
    expect(candidate?.description).toContain('短视频说明')
    expect(candidate?.description).not.toContain('\nショート動画の原題\n')
})

test('buildBiliupUploadCandidate prefers stored translations for YouTube upload titles', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.YouTube,
            type: 'video',
            u_id: '227SMEJ',
            username: '22/7 OFFICIAL YouTube CHANNEL',
            a_id: 'yt-translated-title',
            content: '新番組のお知らせ\n\n22/7の新しい企画が始まります',
            translation: '新节目的通知\n\n22/7的新企划即将开始',
            translated_by: 'DeepSeek V4 Pro',
            created_at: 1710900000,
            url: 'https://www.youtube.com/watch?v=yt-translated-title',
        } as any,
        [],
        [
            { media_type: 'video_thumbnail', path: '/tmp/yt-translated-cover.jpg' },
            { media_type: 'video', path: '/tmp/yt-translated-video.mp4' },
        ],
        {
            enabled: true,
        },
    )

    expect(candidate).toBeTruthy()
    expect(candidate?.title).toBe('【22/7】新节目的通知')
    expect(candidate?.title).not.toContain('[YT]')
    expect(candidate?.description).toContain('22/7的新企划即将开始')
    expect(candidate?.description).toContain('来源平台: YouTube视频')
})

test('completeBiliupUploadCandidateTags formats official YouTube long video metadata', async () => {
    const originalCreate = (processorRegistry as any).create
    const originalTitle = '22/7 3期生 AUDITION DOCUMENTARY - 北原実咲 -'
    const translationTitle = '北原実咲三期生试镜纪录片'
    const generatedTitle = '三期生试镜纪录片'
    const calls: Array<{ provider: string; text: string }> = []
    ;(processorRegistry as any).create = async (provider: string) => ({
        process: async (text: string) => {
            calls.push({ provider, text })
            return JSON.stringify({
                tags: ['オーディション', '北原実咲', '三期生', 'ナナニジライブ'],
                title_zh: translationTitle,
            })
        },
        drop: async () => undefined,
    })

    const article = {
        platform: Platform.YouTube,
        type: 'video',
        u_id: '227SMEJ',
        username: '22/7 OFFICIAL YouTube CHANNEL',
        a_id: 'X6J9TphDexM',
        content: `${originalTitle}\n\n${originalTitle}　を公開\n\n22/7 3期生 定期公演「ナナニジライブ2026」Final開催決定！`,
        translation: `${translationTitle}\n\n${originalTitle} 已公开。\n\n22/7三期生定期公演「ナナニジライブ2026」Final 确定举办！`,
        translated_by: 'DeepSeek V4 Pro',
        created_at: 1781694017,
        url: 'https://www.youtube.com/watch?v=X6J9TphDexM',
    } as any
    const candidate = buildBiliupUploadCandidate(
        article,
        [],
        [
            { media_type: 'video_thumbnail', path: '/tmp/official-cover.jpg' },
            { media_type: 'video', path: '/tmp/official-video.mp4' },
        ],
        {
            enabled: true,
            tag_generation: {
                enabled: true,
                provider: 'DeepSeekV4Pro',
                api_key: 'test-key',
                target_count: 10,
            },
        },
    )

    try {
        expect(candidate).toBeTruthy()
        expect(candidate?.title).toBe(`【22/7 北原実咲】${translationTitle}`)
        expect(candidate?.title).not.toContain('[YT]')
        expect(candidate?.description.split('\n')[0]).toBe(`原标题: ${originalTitle}`)
        expect(candidate?.description).toContain('22/7三期生定期公演「ナナニジライブ2026」Final 确定举办！')
        expect(candidate?.description).toContain('来源平台: YouTube视频')

        await completeBiliupUploadCandidateTags(article, [], candidate!)
    } finally {
        ;(processorRegistry as any).create = originalCreate
    }

    expect(calls).toHaveLength(1)
    expect(calls[0]?.provider).toBe('DeepSeekV4Pro')
    expect(calls[0]?.text).toContain(originalTitle)
    const officialGenerationInput = JSON.parse(calls[0]?.text || '{}')
    expect(officialGenerationInput.title_candidates).toContainEqual(
        expect.objectContaining({
            source: 'translation_first_line',
            text: translationTitle,
            confidence: 'high',
        }),
    )
    expect(officialGenerationInput.title_candidates).toContainEqual(
        expect.objectContaining({
            source: 'original_first_line',
            text: originalTitle,
            confidence: 'high',
        }),
    )
    expect(officialGenerationInput.evidence).toMatchObject({
        platform: 'YouTube',
        source_url: 'https://www.youtube.com/watch?v=X6J9TphDexM',
        deterministic_title: `【22/7 北原実咲】${translationTitle}`,
        translation_first_line: translationTitle,
        original_first_line: originalTitle,
    })
    expect(candidate?.title).toBe(`【22/7 北原実咲】${generatedTitle}`)
    expect(candidate?.title).not.toContain('[YT]')
    expect(candidate?.config.tags).toHaveLength(10)
    expect(candidate?.config.tags).toEqual([
        '22/7',
        '秋元康',
        '偶像',
        '声优偶像',
        '七分之二十二',
        '北原実咲',
        '22/7三期生',
        'オーディション',
        '三期生',
        'ナナニジライブ',
    ])
})

test('buildBiliupUploadCandidate skips Sally member-only posts but keeps Sally YouTube videos', () => {
    const memberOnlyPost = buildBiliupUploadCandidate(
        {
            platform: Platform.X,
            type: 'tweet',
            u_id: 'sally_amaki',
            username: '天城サリー',
            a_id: 'sally-member-only-x',
            content: 'Subscribers only: thank you for watching tonight',
            created_at: 1710900000,
            url: 'https://x.com/sally_amaki/status/sally-member-only-x',
        } as any,
        ['Subscribers only: thank you for watching tonight'],
        [{ media_type: 'video', path: '/tmp/sally-member-only.mp4' }],
        {
            enabled: true,
        },
    )
    const youtubeMemberVideo = buildBiliupUploadCandidate(
        {
            platform: Platform.YouTube,
            type: 'video',
            u_id: 'sallyamakiofficial',
            username: 'Sally Amaki',
            a_id: 'sally-member-video',
            content: 'Members-only stream archive',
            created_at: 1710900000,
            url: 'https://www.youtube.com/watch?v=sally-member-video',
        } as any,
        ['Members-only stream archive'],
        [{ media_type: 'video', path: '/tmp/sally-member-video.mp4' }],
        {
            enabled: true,
        },
    )

    expect(memberOnlyPost).toBeNull()
    expect(youtubeMemberVideo).toBeTruthy()
})

test('buildBiliupUploadCandidate prepares branded metadata for Instagram uploads without text', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.Instagram,
            type: 'post',
            u_id: 'satsuki_shiina',
            username: '椎名桜月',
            a_id: 'ig-live-replay',
            content: null,
            created_at: 1773985020,
            url: 'https://www.instagram.com/p/example/',
        } as any,
        [],
        [{ media_type: 'video', path: '/tmp/replay.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate?.title).toBe('【22/7 椎名桜月】[ins] 椎名桜月 26.03.20')
    expect(candidate?.description).toContain('来源平台: Instagram投稿')
    expect(candidate?.description).toContain('来源账号: 椎名桜月')
    expect(candidate?.description).toContain('账号标识: satsuki_shiina')
    expect(candidate?.description).toContain('原链接: https://www.instagram.com/p/example/')
})

test('buildBiliupUploadCandidate prepares TikTok videos for Bilibili upload', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.TikTok,
            type: 'video',
            u_id: 'tiktok_member',
            username: 'TikTok Member',
            a_id: 'tt-video-1',
            content: 'TT短视频正文',
            created_at: 1773985020,
            url: 'https://www.tiktok.com/@tiktok_member/video/123',
        } as any,
        ['TT短视频正文'],
        [
            { media_type: 'video_thumbnail', path: '/tmp/tt-cover.jpg' },
            { media_type: 'video', path: '/tmp/tt-video.mp4' },
        ],
        {
            enabled: true,
        },
    )

    expect(candidate).toBeTruthy()
    expect(candidate?.title).toBe('【22/7 TikTok Member】[TT] TikTok Member 26.03.20 TT短视频正文')
    expect(candidate?.coverPath).toBe('/tmp/tt-cover.jpg')
    expect(candidate?.videoPaths).toEqual(['/tmp/tt-video.mp4'])
    expect(candidate?.config.tags).not.toContain('TikTok')
})

test('completeBiliupUploadCandidateTags replaces title payload without appending original text', async () => {
    const originalCreate = (processorRegistry as any).create
    const calls: Array<{ provider: string; text: string }> = []
    ;(processorRegistry as any).create = async (provider: string) => ({
        process: async (text: string) => {
            calls.push({ provider, text })
            return JSON.stringify({
                tags: ['ライブ配信', '京都出身', '三期生', '搬运', 'X'],
                title_zh: '北原実咲 26.06.13 直播后的感谢',
            })
        },
        drop: async () => undefined,
    })

    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.X,
            type: 'tweet',
            u_id: 'kitahara_misaki',
            username: '北原実咲【22/7】',
            a_id: 'x-kitahara-video',
            content: '今日は配信ありがとうございました',
            created_at: 1781320000,
            url: 'https://x.com/kitahara_misaki/status/1',
        } as any,
        ['今日は配信ありがとうございました'],
        [{ media_type: 'video', path: '/tmp/kitahara.mp4' }],
        {
            enabled: true,
            tags: ['搬运', '社媒'],
            tag_generation: {
                enabled: true,
                provider: 'DeepSeekV4Pro',
                api_key: 'test-key',
                target_count: 10,
            },
        },
    )

    try {
        expect(candidate).toBeTruthy()
        expect(candidate?.config.tags).toEqual([
            '22/7',
            '秋元康',
            '偶像',
            '声优偶像',
            '七分之二十二',
            '北原実咲',
            '22/7三期生',
        ])

        await completeBiliupUploadCandidateTags(
            {
                platform: Platform.X,
                type: 'tweet',
                u_id: 'kitahara_misaki',
                username: '北原実咲【22/7】',
                a_id: 'x-kitahara-video',
                content: '今日は配信ありがとうございました',
                created_at: 1781320000,
                url: 'https://x.com/kitahara_misaki/status/1',
            } as any,
            ['今日は配信ありがとうございました'],
            candidate!,
        )
    } finally {
        ;(processorRegistry as any).create = originalCreate
    }

    expect(calls).toHaveLength(1)
    expect(calls[0]?.provider).toBe('DeepSeekV4Pro')
    const generatedInput = JSON.parse(calls[0]?.text || '{}')
    expect(generatedInput.title_candidates).toContainEqual(
        expect.objectContaining({
            source: 'detected_member_facts',
            text: '北原実咲',
            confidence: 'high',
        }),
    )
    expect(generatedInput.evidence).toMatchObject({
        platform: 'Twitter',
        source_tag: 'X',
        source_url: 'https://x.com/kitahara_misaki/status/1',
        original_first_line: '今日は配信ありがとうございました',
    })
    expect(candidate?.title).toBe('【22/7 北原実咲】[X] 直播后的感谢')
    expect(candidate?.title).not.toContain('26.06.13 今日は')
    expect(candidate?.config.tags).toHaveLength(10)
    expect(candidate?.config.tags).toEqual([
        '22/7',
        '秋元康',
        '偶像',
        '声优偶像',
        '七分之二十二',
        '北原実咲',
        '22/7三期生',
        'ライブ配信',
        '京都出身',
        '三期生',
    ])
    expect(candidate?.config.tags).not.toContain('搬运')
    expect(candidate?.config.tags).not.toContain('社媒')
    expect(candidate?.config.tags).not.toContain('X')
})

test('buildBiliupUploadCandidate falls back from empty-shell metadata titles', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.TikTok,
            type: 'video',
            u_id: '',
            username: '',
            a_id: 'tt-empty-shell-title',
            content: null,
            created_at: 1773985020,
            url: 'https://www.tiktok.com/@unknown/video/1',
        } as any,
        [],
        [{ media_type: 'video', path: '/tmp/tt-empty.mp4' }],
        {
            enabled: true,
            metadata_templates: {
                title: '【{{missing_account}}】[{{source_tag}}] {{missing_summary}}',
            },
        },
    )

    expect(candidate?.title).toBe('【22/7 Unknown】[TT] Unknown 26.03.20')
    expect(candidate?.title).not.toBe('【】[TT]')
})

test('buildBiliupUploadCandidate uses compact 22/7 source tags for X uploads', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.X,
            type: 'tweet',
            u_id: '227_staff',
            username: '22/7(ナナブンノニジュウニ)',
            a_id: '2063561843692716187',
            content: '22/7_the 3rd\n『＃叫ぶしかない青春』\nMusic Video公開中',
            created_at: 1780826457,
            url: 'https://x.com/227_staff/status/2063561843692716187',
        } as any,
        ['22/7_the 3rd\n『＃叫ぶしかない青春』\nMusic Video公開中'],
        [{ media_type: 'video', path: '/tmp/x-video.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate?.title).toBe('【22/7】[X] 22/7 26.06.07 22/7_the 3rd')
})

test('buildBiliupUploadCandidate uses detected members instead of collection account names', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.Instagram,
            type: 'post',
            u_id: 'nananijigram22_7_the.3rd',
            username: '22/7 THE 3RD',
            a_id: 'ig-the3rd-members',
            content: '北原実咲 黒崎ありす',
            created_at: 1773985020,
            url: 'https://www.instagram.com/p/the3rd-members/',
        } as any,
        ['北原実咲 黒崎ありす'],
        [{ media_type: 'video', path: '/tmp/the3rd-members.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate?.title).toBe(
        '【22/7 北原実咲 黒崎ありす】[ins] 北原実咲 黒崎ありす 26.03.20 北原実咲 黒崎ありす',
    )
    expect(candidate?.title).not.toContain('THE 3RD')
    expect(candidate?.description).toContain('来源账号: 北原実咲 黒崎ありす')
    expect(candidate?.config.tags).toContain('北原実咲')
    expect(candidate?.config.tags).toContain('黒崎ありす')
    expect(candidate?.config.tags).toContain('22/7三期生')
    expect(candidate?.config.tags).not.toContain('22/7 THE 3RD')
    expect(candidate?.config.tags).not.toContain('北原実咲 黒崎ありす')
})

test('buildBiliupUploadCandidate resolves staff posts to mentioned members when confident', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.X,
            type: 'tweet',
            u_id: '227_staff',
            username: '22/7(ナナブンノニジュウニ)',
            a_id: 'x-staff-kitahara',
            content: '北原実咲の紹介動画',
            created_at: 1773985020,
            url: 'https://x.com/227_staff/status/x-staff-kitahara',
        } as any,
        ['北原実咲の紹介動画'],
        [{ media_type: 'video', path: '/tmp/staff-kitahara.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate?.title).toBe('【22/7 北原実咲】[X] 北原実咲 26.03.20 北原実咲の紹介動画')
    expect(candidate?.title).not.toContain('22/7(ナナブンノニジュウニ)')
    expect(candidate?.description).toContain('来源账号: 北原実咲')
    expect(candidate?.config.tags).toContain('北原実咲')
    expect(candidate?.config.tags).toContain('22/7三期生')
    expect(candidate?.config.tags).not.toContain('22/7(ナナブンノニジュウニ)')
})

test('buildBiliupUploadCandidate maps decorative X nicknames to canonical member names', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.X,
            type: 'tweet',
            u_id: 'rino_mochizuki',
            username: '♡望月りの♡【22/7】໒꒱· ﾟ',
            a_id: 'x-rino-video',
            content: '本日18:00〜 もぐもぐ配信します',
            created_at: 1773985020,
            url: 'https://x.com/rino_mochizuki/status/1',
        } as any,
        ['本日18:00〜 もぐもぐ配信します'],
        [{ media_type: 'video', path: '/tmp/x-rino.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate?.title).toBe('【22/7 望月りの】[X] 望月りの 26.03.20 本日18:00〜 もぐもぐ配信します')
    expect(candidate?.description).toContain('来源账号: 望月りの')
})

test('buildBiliupUploadCandidate maps configured Instagram handles to canonical member names', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.Instagram,
            type: 'story',
            u_id: 'shiina_satsuki227',
            username: 'shiina_satsuki227',
            a_id: 'ig-satsuki-story',
            content: null,
            created_at: 1773985020,
            url: 'https://www.instagram.com/stories/shiina_satsuki227/1/',
        } as any,
        [],
        [{ media_type: 'video', path: '/tmp/ig-satsuki.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate?.title).toBe('【22/7 椎名桜月】[ins] 椎名桜月 26.03.20')
    expect(candidate?.description).toContain('来源账号: 椎名桜月')
    expect(candidate?.description).toContain('账号标识: shiina_satsuki227')
})

test('buildBiliupUploadCandidate maps TikTok 22/7-prefixed nicknames to canonical member names', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.TikTok,
            type: 'video',
            u_id: 'emma_tsukishiro',
            username: '22/7 月城咲舞',
            a_id: 'tt-emma-video',
            content: 'TikTok本文',
            created_at: 1773985020,
            url: 'https://www.tiktok.com/@emma_tsukishiro/video/1',
        } as any,
        ['TikTok本文'],
        [
            { media_type: 'video_thumbnail', path: '/tmp/tt-emma-cover.jpg' },
            { media_type: 'video', path: '/tmp/tt-emma.mp4' },
        ],
        {
            enabled: true,
        },
    )

    expect(candidate?.title).toBe('【22/7 月城咲舞】[TT] 月城咲舞 26.03.20 TikTok本文')
})

test('buildBiliupUploadCandidate skips excluded FC website feeds', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.Website,
            u_id: '22/7:movie',
            username: '22/7 FC Movie',
            a_id: 'movie-1',
            content: 'movie body',
            created_at: 1710900000,
            url: 'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447178?cd=nananiji_movie',
        } as any,
        ['movie body'],
        [{ media_type: 'video', path: '/tmp/movie.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate).toBeNull()
})

test('buildBiliupUploadCandidate keeps website blog title out of upload description body', () => {
    const candidate = buildBiliupUploadCandidate(
        {
            platform: Platform.Website,
            type: 'article',
            u_id: '22/7:blog',
            username: '22/7 Blog',
            a_id: 'blog-video-1',
            content: '【春のかおり】\n\nブログ本文',
            created_at: 1710900000,
            url: 'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/1',
        } as any,
        ['【春のかおり】\n\nブログ本文'],
        [{ media_type: 'video', path: '/tmp/blog.mp4' }],
        {
            enabled: true,
        },
    )

    expect(candidate?.title).toContain('春のかおり')
    expect(candidate?.description).toContain('ブログ本文')
    expect(candidate?.description).not.toContain('【春のかおり】')
    expect(`${candidate?.title}\n${candidate?.description}`.match(/春のかおり/g)).toHaveLength(1)
})

test('buildCookieDocument creates a biliup-compatible cookie scaffold', () => {
    const document = buildCookieDocument('sess-token', 'csrf-token')

    expect(document.cookie_info.cookies).toEqual([
        { name: 'SESSDATA', value: 'sess-token' },
        { name: 'bili_jct', value: 'csrf-token' },
    ])
    expect(document.token_info.mid).toBe(0)
})

test('normalizeBiliupCookieDocument preserves full exported cookie documents', () => {
    const document = normalizeBiliupCookieDocument({
        cookie_info: {
            cookies: [
                { name: 'SESSDATA', value: 'sess-token', http_only: 1 },
                { name: 'bili_jct', value: 'csrf-token' },
                { name: 'DedeUserID', value: '123456' },
            ],
            domains: ['.bilibili.com'],
        },
        token_info: {
            access_token: 'token',
            expires_in: 100,
            mid: 123456,
            refresh_token: 'refresh',
        },
        platform: 'BiliTV',
    })

    expect(document.cookie_info.cookies.map((cookie) => cookie.name)).toEqual(['SESSDATA', 'bili_jct', 'DedeUserID'])
    expect(document.token_info.mid).toBe(123456)
    expect(document.platform).toBe('BiliTV')
})

test('resolveVideoUploadConfig keeps configured biliup cookie file path', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-cookie-config-'))
    const cookieFile = path.join(tempRoot, 'cookies.json')
    fs.writeFileSync(cookieFile, JSON.stringify(buildCookieDocument('sess-token', 'csrf-token')))

    const config = resolveVideoUploadConfig({
        enabled: true,
        cookie_file: cookieFile,
    })

    expect(config?.cookie_file).toBe(cookieFile)
})

test('resolveBrowserCookieSyncConfig keeps browser profile sync settings', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-cookie-sync-'))
    const helperScript = path.join(tempRoot, 'export-biliup-browser-cookies.ts')
    fs.writeFileSync(helperScript, 'console.log("ok")')

    const config = resolveBrowserCookieSyncConfig({
        enabled: true,
        session_profile: 'bilibili-uploader',
        script_path: helperScript,
        url: 'https://www.bilibili.com',
        browser_mode: 'headed-xvfb',
    })

    expect(config?.session_profile).toBe('bilibili-uploader')
    expect(config?.script_path).toBe(helperScript)
    expect(config?.browser_mode).toBe('headed-xvfb')
})

test('resolveVideoUploadConfig includes browser cookie sync settings', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-video-upload-sync-'))
    const helperScript = path.join(tempRoot, 'export-biliup-browser-cookies.ts')
    fs.writeFileSync(helperScript, 'console.log("ok")')

    const config = resolveVideoUploadConfig({
        enabled: true,
        cookie_file: path.join(tempRoot, 'cookies.json'),
        browser_cookie_sync: {
            enabled: true,
            session_profile: 'bilibili-uploader',
            script_path: helperScript,
        },
    })

    expect(config?.browser_cookie_sync?.session_profile).toBe('bilibili-uploader')
    expect(config?.browser_cookie_sync?.script_path).toBe(helperScript)
})

test('resolveVideoUploadConfig keeps metadata templates and ignores deprecated collision placeholder settings', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-video-upload-template-'))
    const videoPath = path.join(tempRoot, 'pad.mp4')
    fs.writeFileSync(videoPath, 'video')
    const config = resolveVideoUploadConfig({
        enabled: true,
        metadata_templates: {
            title: '【{{platform_type_label}}】{{display_name}} {{summary}}',
            description: '{{body_or_summary}}\n\n原链接: {{url}}',
        },
        collision_placeholder_part: {
            enabled: true,
            video_path: videoPath,
            image_path: path.join(tempRoot, 'logo.png'),
            title: 'legacy placeholder',
            background_color: '#d1e5fc',
        },
    })

    expect(config?.metadata_templates?.title).toBe('【{{platform_type_label}}】{{display_name}} {{summary}}')
    expect((config as any).collision_placeholder_part).toBeUndefined()
})

test('resolveVideoUploadConfig falls back from invalid numeric control values', () => {
    const config = resolveVideoUploadConfig({
        enabled: true,
        tid: 'wat' as any,
        threads: 'wat' as any,
        collision_placeholder_part: {
            enabled: true,
            duration_seconds: 'wat' as any,
            width: 'wat' as any,
            height: 'wat' as any,
            fps: 'wat' as any,
        },
    })

    expect(config?.tid).toBe(171)
    expect(config?.threads).toBe(3)
    expect((config as any).collision_placeholder_part).toBeUndefined()
})

test('prepareUploadVideoParts ignores deprecated collision placeholder video when configured', async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'biliup-video-parts-persistent-'))
    const uploadDir = path.join(tempRoot, 'upload')
    fs.mkdirSync(uploadDir, { recursive: true })

    const videoPath = path.join(tempRoot, 'source-video.mp4')
    fs.writeFileSync(videoPath, 'video')

    const placeholderPath = path.join(tempRoot, 'collision-pad.mp4')
    fs.writeFileSync(placeholderPath, 'pad')

    const parts = await prepareUploadVideoParts(
        {
            videoPaths: [videoPath],
            config: {
                enabled: true,
                python_path: 'python3',
                helper_path: '/tmp/helper.py',
                working_dir: tempRoot,
                submit_api: 'web',
                line: 'AUTO',
                tid: 171,
                threads: 3,
                copyright: 2,
                tags: [],
                exclude_uids: [],
                collision_placeholder_part: {
                    enabled: true,
                    video_path: placeholderPath,
                    image_path: path.join(tempRoot, 'unused.png'),
                    title: 'legacy placeholder',
                    duration_seconds: 7,
                    width: 1920,
                    height: 1080,
                    fps: 30,
                    ffmpeg_path: '/usr/bin/ffmpeg',
                    background_color: '#d1e5fc',
                },
            },
        },
        uploadDir,
    )

    expect(parts).toEqual([
        {
            sourcePath: videoPath,
            stagedPath: videoPath,
        },
    ])
    expect(fs.existsSync(path.join(uploadDir, 'legacy placeholder.mp4'))).toBe(false)
})

test('BiliForwarder skips dynamic posting when biliup upload succeeds', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let dynamicCalls = 0
    ;(forwarder as any).tryVideoUpload = async () => true
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return []
    }

    const result = await (forwarder as any).realSend(['hello'], {})

    expect(dynamicCalls).toBe(0)
    expect(result).toEqual([{ ok: true, mode: 'biliup' }])
})

test('BiliForwarder applies runtime video upload metadata overrides', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    DB.MediaHash.checkExist = async () => null
    DB.MediaHash.save = async () => ({ platform: 'test', hash: 'test', a_id: 'test' }) as any

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
                metadata_templates: {
                    title: 'OLD {{summary}}',
                },
            },
        } as any,
        'bili-test',
    )

    let uploadedTitle = ''
    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async (_article: unknown, candidate: { title: string }) => {
        uploadedTitle = candidate.title
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    try {
        const result = await (forwarder as any).realSend(['TT短视频正文'], {
            runtime_config: {
                video_upload: {
                    enabled: true,
                    metadata_templates: {
                        title: 'NEW {{source_tag}} {{upload_summary}}',
                    },
                },
            } as any,
            article: {
                platform: Platform.TikTok,
                a_id: 'tt-runtime-title',
                u_id: 'tiktok_member',
                username: 'TikTok Member',
                type: 'video',
                content: 'TT短视频正文',
                created_at: 1773985020,
                url: 'https://www.tiktok.com/@tiktok_member/video/runtime-title',
            },
            media: [{ media_type: 'video', path: '/tmp/video.mp4', content_hash: 'runtime-title-video-hash' }],
        })

        expect(result).toEqual([{ ok: true, mode: 'biliup' }])
        expect(uploadedTitle).toBe('NEW TT TikTok Member 26.03.20 TT短视频正文')
        expect(dynamicCalls).toBe(0)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    }
})

test('BiliForwarder records successful video uploads for strict Bilibili dedupe', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    const saved: Array<{ platform: string; hash: string; a_id: string }> = []
    DB.MediaHash.checkExist = async () => null
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        saved.push({ platform, hash, a_id })
        return { platform, hash, a_id } as any
    }

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let uploadCalls = 0
    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async () => {
        uploadCalls += 1
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return []
    }

    try {
        const result = await (forwarder as any).realSend(['hello'], {
            article: {
                platform: Platform.TikTok,
                a_id: 'tt-video-strict-dedupe',
                u_id: 'tt_member',
                username: 'TT Member',
                type: 'video',
                created_at: 1773985020,
                url: 'https://www.tiktok.com/@tt_member/video/1',
            },
            media: [{ media_type: 'video', path: '/tmp/video.mp4', content_hash: 'same-video-hash' }],
        })

        expect(result).toEqual([{ ok: true, mode: 'biliup' }])
        expect(uploadCalls).toBe(1)
        expect(dynamicCalls).toBe(0)
        expect(saved).toEqual([
            {
                platform: 'bilibili-video-upload',
                hash: 'same-video-hash',
                a_id: `${Platform.TikTok}:tt-video-strict-dedupe`,
            },
        ])
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    }
})

test('BiliForwarder suppresses duplicate video uploads without dynamic fallback', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    DB.MediaHash.checkExist = async (platform: string, hash: string) =>
        platform === 'bilibili-video-upload' && hash === 'same-video-hash'
            ? ({ platform, hash, a_id: `${Platform.Instagram}:ig-video-previous` } as any)
            : null
    DB.MediaHash.save = async () => {
        throw new Error('duplicate upload should not save')
    }

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let uploadCalls = 0
    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async () => {
        uploadCalls += 1
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    try {
        const result = await (forwarder as any).realSend(['hello'], {
            article: {
                platform: Platform.TikTok,
                a_id: 'tt-video-repeat',
                u_id: 'tt_member',
                username: 'TT Member',
                type: 'video',
                created_at: 1773985020,
                url: 'https://www.tiktok.com/@tt_member/video/2',
            },
            media: [{ media_type: 'video', path: '/tmp/video.mp4', content_hash: 'same-video-hash' }],
        })

        expect(result).toEqual([{ ok: true, mode: 'biliup_duplicate' }])
        expect(uploadCalls).toBe(0)
        expect(dynamicCalls).toBe(0)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    }
})

test('BiliForwarder ignores referenced videos when choosing biliup upload for quoted image posts', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    DB.MediaHash.checkExist = async () => {
        throw new Error('referenced video should not be considered for root biliup upload')
    }

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            require_media: true,
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-root-media-test',
    )

    let uploadCalls = 0
    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async () => {
        uploadCalls += 1
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    const rootPhoto = {
        media_type: 'photo',
        path: '/tmp/root-photo.jpg',
        sourceArticleId: 'x-quote-with-own-photo',
        sourceUserId: 'alice__kurosaki',
    }
    const refVideo = {
        media_type: 'video',
        path: '/tmp/ref-video.mp4',
        content_hash: 'ref-video-hash',
        sourceArticleId: 'x-referenced-video',
        sourceUserId: '227_staff',
    }
    const refThumb = {
        media_type: 'video_thumbnail',
        path: '/tmp/ref-thumb.jpg',
        sourceArticleId: 'x-referenced-video',
        sourceUserId: '227_staff',
    }

    try {
        const result = await (forwarder as any).realSend(['quote with own photo'], {
            article: {
                platform: Platform.X,
                a_id: 'x-quote-with-own-photo',
                u_id: 'alice__kurosaki',
                username: '黒崎ありす【22/7】',
                type: 'quoted',
                created_at: 1781100425,
                url: 'https://x.com/alice__kurosaki/status/x-quote-with-own-photo',
            },
            media: [rootPhoto, refVideo, refThumb],
            contentMedia: [rootPhoto],
        })

        expect(result).toEqual([{ ok: true, mode: 'dynamic' }])
        expect(uploadCalls).toBe(0)
        expect(dynamicCalls).toBe(1)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
    }
})

test('BiliForwarder appends referenced videos as later biliup parts when root post has a video', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    DB.MediaHash.checkExist = async () => null
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') =>
        ({ platform, hash, a_id }) as any

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            require_media: true,
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-root-video-parts-test',
    )

    let uploadedVideoPaths: string[] = []
    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async (_article: unknown, candidate: { videoPaths: string[] }) => {
        uploadedVideoPaths = candidate.videoPaths
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    const rootThumb = {
        media_type: 'video_thumbnail',
        path: '/tmp/root-thumb.jpg',
        sourceArticleId: 'x-root-video',
        sourceUserId: 'kitahara_misaki',
    }
    const rootVideo = {
        media_type: 'video',
        path: '/tmp/root-video.mp4',
        content_hash: 'root-video-hash',
        sourceArticleId: 'x-root-video',
        sourceUserId: 'kitahara_misaki',
    }
    const refVideo = {
        media_type: 'video',
        path: '/tmp/ref-video.mp4',
        content_hash: 'ref-video-hash',
        sourceArticleId: 'ig-main-video',
        sourceUserId: 'nananijigram22_7_the.3rd',
    }

    try {
        const result = await (forwarder as any).realSend(['root video with referenced preview'], {
            article: {
                platform: Platform.X,
                a_id: 'x-root-video',
                u_id: 'kitahara_misaki',
                username: '北原実咲【22/7】',
                type: 'quoted',
                created_at: 1781100425,
                url: 'https://x.com/kitahara_misaki/status/x-root-video',
                content: 'root video with referenced preview',
            },
            media: [rootThumb, rootVideo, refVideo],
            contentMedia: [rootThumb, rootVideo],
        })

        expect(result).toEqual([{ ok: true, mode: 'biliup' }])
        expect(uploadedVideoPaths).toEqual(['/tmp/root-video.mp4', '/tmp/ref-video.mp4'])
        expect(dynamicCalls).toBe(0)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    }
})

test('BiliForwarder does not suppress uploads from coarse short-video buckets alone', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }

    store.set('cross-short-video:3rd:82445:92', {
        platform: 'cross-short-video:3rd',
        hash: '82445:92',
        a_id: `${Platform.Instagram}:DZR9nGHxnvu`,
    })

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let uploadCalls = 0
    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async () => {
        uploadCalls += 1
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    try {
        const result = await (forwarder as any).realSend(['hello'], {
            article: {
                platform: Platform.X,
                a_id: '2063561843692716187',
                u_id: '227_staff',
                username: '22/7(ナナブンノニジュウニ)',
                type: 'tweet',
                created_at: 1780826457,
                url: 'https://x.com/227_staff/status/2063561843692716187',
                content: '22/7_the 3rd\n『＃叫ぶしかない青春』\nMusic Video公開中',
            },
            media: [
                {
                    media_type: 'video',
                    path: '/tmp/x-video.mp4',
                    content_hash: 'x-reencoded-video-hash',
                    duration_seconds: 45.766531,
                },
            ],
        })

        expect(result).toEqual([{ ok: true, mode: 'biliup' }])
        expect(uploadCalls).toBe(1)
        expect(dynamicCalls).toBe(0)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    }
})

test('BiliForwarder records text-keyed short-video dedupe keys after successful upload', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    const saved: Array<{ platform: string; hash: string; a_id: string }> = []
    DB.MediaHash.checkExist = async () => null
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        saved.push({ platform, hash, a_id })
        return { platform, hash, a_id } as any
    }

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    ;(forwarder as any).performBiliupUpload = async () => {}
    ;(forwarder as any).sendDynamicContent = async () => [{ ok: true, mode: 'dynamic' }]

    try {
        const result = await (forwarder as any).realSend(['hello'], {
            article: {
                platform: Platform.X,
                a_id: '2063561843692716187',
                u_id: '227_staff',
                username: '22/7(ナナブンノニジュウニ)',
                type: 'tweet',
                created_at: 1780826457,
                url: 'https://x.com/227_staff/status/2063561843692716187',
                content: '22/7_the 3rd\n『＃叫ぶしかない青春』\nMusic Video公開中',
            },
            media: [
                {
                    media_type: 'video',
                    path: '/tmp/x-video.mp4',
                    content_hash: 'x-reencoded-video-hash',
                    duration_seconds: 45.766531,
                },
            ],
        })

        expect(result).toEqual([{ ok: true, mode: 'biliup' }])
        expect(saved).toContainEqual({
            platform: 'bilibili-video-upload',
            hash: 'x-reencoded-video-hash',
            a_id: `${Platform.X}:2063561843692716187`,
        })
        const shortVideoRecords = saved.filter((item) => item.platform.startsWith('cross-short-video:'))
        expect(shortVideoRecords.length).toBeGreaterThan(0)
        expect(shortVideoRecords.every((item) => item.platform === 'cross-short-video:3rd')).toBe(true)
        expect(shortVideoRecords.every((item) => item.a_id === `${Platform.X}:2063561843692716187`)).toBe(true)
        expect(shortVideoRecords.every((item) => /^\d+:\d+:[cst]:[0-9a-f]{16}$/.test(item.hash))).toBe(true)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    }
})

test('BiliForwarder suppresses TT/INS semantic short-video duplicates without dynamic fallback', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    const originalGetSingleArticleByArticleCode = DB.Article.getSingleArticleByArticleCode
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    const articles = new Map<string, any>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) =>
        articles.get(`${platform}:${a_id}`)

    const instagramArticle = {
        platform: Platform.Instagram,
        a_id: 'DZR9nGHxnvu',
        u_id: 'nananijigram22_7_the.3rd',
        username: '22/7_the 3rd',
        type: 'post',
        created_at: 1780826818,
        url: 'https://www.instagram.com/p/DZR9nGHxnvu/',
        content: '. 22/7_the 3rd 『＃叫ぶしかない青春』 Music Video公開中',
    }
    const tiktokArticle = {
        platform: Platform.TikTok,
        a_id: 'tt-same-mv-promo',
        u_id: 'the3rd_tiktok',
        username: '22/7_the 3rd',
        type: 'video',
        created_at: 1780827050,
        url: 'https://www.tiktok.com/@the3rd_tiktok/video/1',
        content: '22/7_the 3rd\n#叫ぶしかない青春\nMusic Video公開中',
    }
    articles.set(`${Platform.Instagram}:${instagramArticle.a_id}`, instagramArticle)
    articles.set(`${Platform.TikTok}:${tiktokArticle.a_id}`, tiktokArticle)

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let uploadCalls = 0
    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async () => {
        uploadCalls += 1
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    try {
        const first = await (forwarder as any).realSend(['instagram text'], {
            article: instagramArticle,
            media: [
                {
                    media_type: 'video',
                    path: '/tmp/ig-video.mp4',
                    content_hash: 'ig-reencoded-video-hash',
                    duration_seconds: 45.787,
                },
            ],
        })
        const second = await (forwarder as any).realSend(['tiktok text'], {
            article: tiktokArticle,
            media: [
                {
                    media_type: 'video',
                    path: '/tmp/tt-video.mp4',
                    content_hash: 'tt-reencoded-video-hash',
                    duration_seconds: 45.766531,
                },
            ],
        })

        expect(first).toEqual([{ ok: true, mode: 'biliup' }])
        expect(second).toEqual([{ ok: true, mode: 'biliup_duplicate' }])
        expect(uploadCalls).toBe(1)
        expect(dynamicCalls).toBe(0)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
        ;(DB.Article as any).getSingleArticleByArticleCode = originalGetSingleArticleByArticleCode
    }
})

test('BiliForwarder suppresses sparse-caption TT/INS short-video duplicates without dynamic fallback', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    const originalGetSingleArticleByArticleCode = DB.Article.getSingleArticleByArticleCode
    const store = new Map<string, { platform: string; hash: string; a_id: string }>()
    const articles = new Map<string, any>()
    DB.MediaHash.checkExist = async (platform: string, hash: string) => store.get(`${platform}:${hash}`) as any
    DB.MediaHash.save = async (platform: string, hash: string, a_id: string = '') => {
        const value = { platform, hash, a_id }
        store.set(`${platform}:${hash}`, value)
        return value as any
    }
    ;(DB.Article as any).getSingleArticleByArticleCode = async (a_id: string, platform: Platform) =>
        articles.get(`${platform}:${a_id}`)

    const instagramArticle = {
        platform: Platform.Instagram,
        a_id: 'ig-sparse-reel',
        u_id: 'nananijigram22_7_the.3rd',
        username: '22/7_the 3rd',
        type: 'post',
        created_at: 1780826818,
        url: 'https://www.instagram.com/p/ig-sparse-reel/',
    }
    const tiktokArticle = {
        platform: Platform.TikTok,
        a_id: 'tt-sparse-repost',
        u_id: 'the3rd_tiktok',
        username: '22/7_the 3rd',
        type: 'video',
        created_at: 1780827050,
        url: 'https://www.tiktok.com/@the3rd_tiktok/video/2',
    }
    articles.set(`${Platform.Instagram}:${instagramArticle.a_id}`, instagramArticle)
    articles.set(`${Platform.TikTok}:${tiktokArticle.a_id}`, tiktokArticle)

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let uploadCalls = 0
    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async () => {
        uploadCalls += 1
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    try {
        const first = await (forwarder as any).realSend(['instagram text'], {
            article: instagramArticle,
            media: [
                {
                    media_type: 'video',
                    path: '/tmp/ig-sparse-video.mp4',
                    content_hash: 'ig-sparse-video-hash',
                    duration_seconds: 45.787,
                },
            ],
        })
        const second = await (forwarder as any).realSend(['tiktok text'], {
            article: tiktokArticle,
            media: [
                {
                    media_type: 'video',
                    path: '/tmp/tt-sparse-video.mp4',
                    content_hash: 'tt-sparse-video-hash',
                    duration_seconds: 45.766531,
                },
            ],
        })

        expect(first).toEqual([{ ok: true, mode: 'biliup' }])
        expect(second).toEqual([{ ok: true, mode: 'biliup_duplicate' }])
        expect(uploadCalls).toBe(1)
        expect(dynamicCalls).toBe(0)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
        ;(DB.Article as any).getSingleArticleByArticleCode = originalGetSingleArticleByArticleCode
    }
})

test('BiliForwarder suppresses dynamic fallback when biliup upload fails', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    DB.MediaHash.checkExist = async () => null
    DB.MediaHash.save = async () => {
        throw new Error('failed upload should not save')
    }

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async () => {
        throw new Error('simulated biliup failure')
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    try {
        await expect(
            (forwarder as any).realSend(['hello'], {
                article: {
                    platform: Platform.TikTok,
                    a_id: 'tt-video-upload-failed',
                    u_id: 'tt_member',
                    username: 'TT Member',
                    type: 'video',
                    created_at: 1773985020,
                    url: 'https://www.tiktok.com/@tt_member/video/3',
                },
                media: [{ media_type: 'video', path: '/tmp/video.mp4', content_hash: 'failed-video-hash' }],
            }),
        ).rejects.toThrow(/simulated biliup failure/)
        expect(dynamicCalls).toBe(0)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    }
})

test('BiliForwarder blocks dynamic fallback when video dedupe check fails', async () => {
    const originalCheckExist = DB.MediaHash.checkExist
    const originalSave = DB.MediaHash.save
    DB.MediaHash.checkExist = async () => {
        throw new Error('simulated media hash database failure')
    }
    DB.MediaHash.save = async () => {
        throw new Error('dedupe failure should not save')
    }

    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let uploadCalls = 0
    let dynamicCalls = 0
    ;(forwarder as any).performBiliupUpload = async () => {
        uploadCalls += 1
    }
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    try {
        await expect(
            (forwarder as any).realSend(['hello'], {
                article: {
                    platform: Platform.TikTok,
                    a_id: 'tt-video-dedupe-failed',
                    u_id: 'tt_member',
                    username: 'TT Member',
                    type: 'video',
                    created_at: 1773985020,
                    url: 'https://www.tiktok.com/@tt_member/video/4',
                },
                media: [{ media_type: 'video', path: '/tmp/video.mp4', content_hash: 'dedupe-failed-video-hash' }],
            }),
        ).rejects.toThrow(/simulated media hash database failure/)
        expect(uploadCalls).toBe(0)
        expect(dynamicCalls).toBe(0)
    } finally {
        DB.MediaHash.checkExist = originalCheckExist
        DB.MediaHash.save = originalSave
    }
})

test('BiliForwarder suppresses media-required dynamic posts without uploadable image media', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            require_media: true,
            video_upload: {
                enabled: false,
            },
        } as any,
        'bili-test',
    )

    let dynamicCalls = 0
    ;(forwarder as any).tryVideoUpload = async () => false
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    const result = await (forwarder as any).realSend(['metadata text'], {
        media: [{ media_type: 'video', path: '/tmp/video.mp4', content_hash: 'video-only-hash' }],
    })

    expect(result).toEqual([{ ok: true, mode: 'dynamic_media_required_suppressed' }])
    expect(dynamicCalls).toBe(0)
})

test('BiliForwarder does not let rendered card media satisfy media-required source media', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            require_media: true,
            video_upload: {
                enabled: false,
            },
        } as any,
        'bili-card-source-media-test',
    )

    let dynamicCalls = 0
    ;(forwarder as any).tryVideoUpload = async () => false
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    const card = { media_type: 'photo', path: '/tmp/rendered-summary-card.png' }
    const result = await (forwarder as any).realSend(['metadata card'], {
        media: [card],
        cardMedia: [card],
        contentMedia: [],
    })

    expect(result).toEqual([{ ok: true, mode: 'dynamic_media_required_suppressed' }])
    expect(dynamicCalls).toBe(0)
})

test('BiliForwarder lets message-pack card media satisfy media-required dynamic posts', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            require_media: true,
            video_upload: {
                enabled: false,
            },
        } as any,
        'bili-message-pack-card-test',
    )

    let dynamicCalls = 0
    ;(forwarder as any).tryVideoUpload = async () => false
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    const card = { media_type: 'photo', path: '/tmp/rendered-summary-card.png' }
    const result = await (forwarder as any).realSend(['summary digest'], {
        media: [card],
        cardMedia: [card],
        contentMedia: [],
        article: {
            a_id: 'summary-card-message-pack',
            type: 'message_pack',
            extra: {
                extra_type: 'message_pack_meta',
            },
        },
    })

    expect(result).toEqual([{ ok: true, mode: 'dynamic' }])
    expect(dynamicCalls).toBe(1)
})

test('BiliForwarder allows rendered card media when media-required source media exists', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            require_media: true,
            video_upload: {
                enabled: false,
            },
        } as any,
        'bili-card-with-source-media-test',
    )

    let dynamicCalls = 0
    ;(forwarder as any).tryVideoUpload = async () => false
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    const card = { media_type: 'photo', path: '/tmp/rendered-summary-card.png' }
    const source = { media_type: 'photo', path: '/tmp/source-photo.jpg' }
    const result = await (forwarder as any).realSend(['metadata card'], {
        media: [card],
        cardMedia: [card],
        contentMedia: [source],
    })

    expect(result).toEqual([{ ok: true, mode: 'dynamic' }])
    expect(dynamicCalls).toBe(1)
})

test('BiliForwarder falls back to dynamic posting when biliup upload is skipped', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let dynamicCalls = 0
    ;(forwarder as any).tryVideoUpload = async () => false
    ;(forwarder as any).sendDynamicContent = async () => {
        dynamicCalls += 1
        return [{ ok: true, mode: 'dynamic' }]
    }

    const result = await (forwarder as any).realSend(['hello'], {})

    expect(dynamicCalls).toBe(1)
    expect(result).toEqual([{ ok: true, mode: 'dynamic' }])
})

test('BiliForwarder tightens X action header spacing for Bilibili posts', async () => {
    const forwarder = new BiliForwarder(
        {
            bili_jct: 'csrf-token',
            sessdata: 'sess-token',
            video_upload: {
                enabled: true,
            },
        } as any,
        'bili-test',
    )

    let uploadTexts: string[] = []
    let dynamicTexts: string[] = []
    ;(forwarder as any).tryVideoUpload = async (texts: string[]) => {
        uploadTexts = texts
        return false
    }
    ;(forwarder as any).sendDynamicContent = async (texts: string[]) => {
        dynamicTexts = texts
        return [{ ok: true, mode: 'dynamic' }]
    }

    await (forwarder as any).realSend(
        [
            '@member 0203⁹ X发推\n\n本文\n\nmember 0203⁹（260101） X 发推',
            '@member 0204⁹ X引用\n\n引用本文\n\nmember 0204⁹（260101） X 引用',
        ],
        {},
    )

    expect(uploadTexts[0]).toContain('@member 0203⁹ X发推:\n本文')
    expect(dynamicTexts[0]).toContain('@member 0203⁹ X发推:\n本文')
    expect(dynamicTexts[0]).not.toContain('X发推\n\n本文')
    expect(uploadTexts[1]).toContain('@member 0204⁹ X引用:\n引用本文')
    expect(dynamicTexts[1]).toContain('@member 0204⁹ X引用:\n引用本文')
    expect(dynamicTexts[1]).not.toContain('X引用\n\n引用本文')
})

function buildYoutubeTitleGenArticle() {
    const originalTitle = '22/7 3期生 AUDITION DOCUMENTARY - 北原実咲 -'
    return {
        article: {
            platform: Platform.YouTube,
            type: 'video',
            u_id: '227SMEJ',
            username: '22/7 OFFICIAL YouTube CHANNEL',
            a_id: 'title-gen-doc',
            content: `${originalTitle}\n\n公演本文`,
            created_at: 1781694017,
            url: 'https://www.youtube.com/watch?v=title-gen-doc',
        } as any,
        originalTitle,
    }
}

test('title_generation runs independently when tag_generation is absent', async () => {
    const { article, originalTitle } = buildYoutubeTitleGenArticle()
    const originalCreate = (processorRegistry as any).create
    const calls: Array<{ provider: string; text: string }> = []
    ;(processorRegistry as any).create = async (provider: string) => ({
        process: async (text: string) => {
            calls.push({ provider, text })
            return JSON.stringify({ tags: [], title_zh: '北原実咲试镜纪录片' })
        },
        drop: async () => undefined,
    })

    const candidate = buildBiliupUploadCandidate(
        article,
        [],
        [{ media_type: 'video', path: '/tmp/title-gen-doc.mp4' }],
        {
            enabled: true,
            // No tag_generation block at all; title_generation must still be able to run with its own creds.
            title_generation: {
                enabled: true,
                provider: 'DeepSeekV4Pro',
                api_key: 'title-key',
            },
        },
    )

    try {
        expect(candidate).toBeTruthy()
        await completeBiliupUploadCandidateTags(article, [], candidate!)
    } finally {
        ;(processorRegistry as any).create = originalCreate
    }

    expect(calls).toHaveLength(1)
    expect(calls[0]?.provider).toBe('DeepSeekV4Pro')
    // The leading account name is stripped from the generated payload, leaving the distinctive Chinese title.
    expect(candidate?.title).toContain('试镜纪录片')
    // The original source title is preserved (here it still fits inside the bounded title, so no description anchor).
    expect(`${candidate?.title}\n${candidate?.description}`).toContain(originalTitle)
})

test('title_generation preserves a long original title as a description reference anchor', async () => {
    const originalCreate = (processorRegistry as any).create
    // A long original first line cannot also fit inside the bounded main title, so it must survive in the description.
    const longOriginalTitle =
        '北原実咲 三期生 定期公演 ナナニジライブ2026 Final 開催決定 スペシャル ロング ドキュメンタリー 完全版 公開記念 特別映像'
    const article = {
        platform: Platform.YouTube,
        type: 'video',
        u_id: '227SMEJ',
        username: '22/7 OFFICIAL YouTube CHANNEL',
        a_id: 'title-gen-long',
        content: `${longOriginalTitle}\n\n公演本文`,
        created_at: 1781694017,
        url: 'https://www.youtube.com/watch?v=title-gen-long',
    } as any
    ;(processorRegistry as any).create = async () => ({
        process: async () => JSON.stringify({ tags: [], title_zh: '试镜纪录片完全版' }),
        drop: async () => undefined,
    })

    const candidate = buildBiliupUploadCandidate(
        article,
        [],
        [{ media_type: 'video', path: '/tmp/title-gen-long.mp4' }],
        {
            enabled: true,
            title_generation: {
                enabled: true,
                provider: 'DeepSeekV4Pro',
                api_key: 'title-key',
            },
        },
    )

    try {
        expect(candidate).toBeTruthy()
        await completeBiliupUploadCandidateTags(article, [], candidate!)
    } finally {
        ;(processorRegistry as any).create = originalCreate
    }

    expect(candidate?.title).toContain('试镜纪录片完全版')
    // The tail of the long original is dropped from the bounded title but kept as a description anchor.
    expect(candidate?.title).not.toContain('公開記念')
    expect(candidate?.description).toContain('原标题:')
    expect(candidate?.description).toContain(longOriginalTitle)
})

test('title_generation: false keeps the deterministic title even with tag_generation creds', async () => {
    const { article } = buildYoutubeTitleGenArticle()
    const originalCreate = (processorRegistry as any).create
    const calls: Array<{ provider: string; text: string }> = []
    ;(processorRegistry as any).create = async (provider: string) => ({
        process: async (text: string) => {
            calls.push({ provider, text })
            return JSON.stringify({ tags: ['北原実咲'], title_zh: '不应被采用的标题' })
        },
        drop: async () => undefined,
    })

    const candidate = buildBiliupUploadCandidate(
        article,
        [],
        [{ media_type: 'video', path: '/tmp/title-gen-doc.mp4' }],
        {
            enabled: true,
            title_generation: false,
            tag_generation: {
                enabled: true,
                provider: 'DeepSeekV4Pro',
                api_key: 'tag-key',
                target_count: 10,
            },
        },
    )
    const deterministicTitle = candidate?.title

    try {
        expect(candidate).toBeTruthy()
        await completeBiliupUploadCandidateTags(article, [], candidate!)
    } finally {
        ;(processorRegistry as any).create = originalCreate
    }

    // Tag generation still ran (one combined call), but the generated title must be ignored.
    expect(calls).toHaveLength(1)
    expect(candidate?.title).toBe(deterministicTitle)
    expect(candidate?.title).not.toContain('不应被采用的标题')
    expect(candidate?.config.tags).toContain('北原実咲')
})

test('title_generation guard failure falls back to the deterministic title', async () => {
    const { article } = buildYoutubeTitleGenArticle()
    const originalCreate = (processorRegistry as any).create
    ;(processorRegistry as any).create = async () => ({
        // Latin-only output fails the Chinese-title correctness guard and must not overwrite the title.
        process: async () => JSON.stringify({ tags: [], title_zh: 'audition documentary' }),
        drop: async () => undefined,
    })

    const candidate = buildBiliupUploadCandidate(
        article,
        [],
        [{ media_type: 'video', path: '/tmp/title-gen-doc.mp4' }],
        {
            enabled: true,
            title_generation: {
                enabled: true,
                provider: 'DeepSeekV4Pro',
                api_key: 'title-key',
            },
        },
    )
    const deterministicTitle = candidate?.title

    try {
        expect(candidate).toBeTruthy()
        await completeBiliupUploadCandidateTags(article, [], candidate!)
    } finally {
        ;(processorRegistry as any).create = originalCreate
    }

    expect(candidate?.title).toBe(deterministicTitle)
    expect(candidate?.title).not.toContain('audition documentary')
})

test('title_generation with separate credentials runs separately from tag_generation', async () => {
    const { article } = buildYoutubeTitleGenArticle()
    const originalCreate = (processorRegistry as any).create
    const calls: Array<{ provider: string; text: string }> = []
    ;(processorRegistry as any).create = async (provider: string) => ({
        process: async (text: string) => {
            calls.push({ provider, text })
            if (provider === 'TagProvider') {
                return JSON.stringify({ tags: ['北原実咲'], title_zh: '不应使用标签模型标题' })
            }
            return JSON.stringify({ title_zh: '北原実咲试镜纪录片' })
        },
        drop: async () => undefined,
    })

    const candidate = buildBiliupUploadCandidate(
        article,
        [],
        [{ media_type: 'video', path: '/tmp/title-gen-doc.mp4' }],
        {
            enabled: true,
            tag_generation: {
                enabled: true,
                provider: 'TagProvider' as any,
                api_key: 'tag-key',
                target_count: 10,
            },
            title_generation: {
                enabled: true,
                provider: 'TitleProvider',
                api_key: 'title-key',
            },
        },
    )

    try {
        expect(candidate).toBeTruthy()
        await completeBiliupUploadCandidateTags(article, [], candidate!)
    } finally {
        ;(processorRegistry as any).create = originalCreate
    }

    expect(calls.map((call) => call.provider)).toEqual(['TagProvider', 'TitleProvider'])
    expect(candidate?.config.tags).toContain('北原実咲')
    expect(candidate?.title).toContain('试镜纪录片')
    expect(candidate?.title).not.toContain('不应使用标签模型标题')
})

test('resolveVideoUploadConfig reuses tag_generation creds for title_generation by default', () => {
    const resolved = resolveVideoUploadConfig({
        enabled: true,
        tag_generation: {
            enabled: true,
            provider: 'DeepSeekV4Pro',
            api_key: 'shared-key',
            target_count: 10,
        },
    })
    expect(resolved?.title_generation).toMatchObject({
        enabled: true,
        provider: 'DeepSeekV4Pro',
        api_key: 'shared-key',
    })

    const disabled = resolveVideoUploadConfig({
        enabled: true,
        title_generation: false,
        tag_generation: {
            enabled: true,
            provider: 'DeepSeekV4Pro',
            api_key: 'shared-key',
            target_count: 10,
        },
    })
    expect(disabled?.title_generation).toBeUndefined()

    const noCreds = resolveVideoUploadConfig({ enabled: true })
    expect(noCreds?.title_generation).toBeUndefined()
})
