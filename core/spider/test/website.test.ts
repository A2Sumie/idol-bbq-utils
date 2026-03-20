import { describe, expect, test } from 'bun:test'
import { buildPhotoAlbumArticle, buildWebsiteArticle, NanabunnonijyuuniWebsiteSpider, type FeedConfig } from '../src/spiders/website'

describe('NanabunnonijyuuniWebsiteSpider.resolveFeed', () => {
    test('matches supported 22/7 FC and live-report routes', () => {
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/ticket/list?ima=2101')?.feed).toBe('ticket')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/news/list?ima=2148')?.feed).toBe('official-news')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/news/list?ima=2149&ct=news')?.feed).toBe('fc-news')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/diary/official_blog/list?ima=2201')?.feed).toBe('official-blog')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/contents_list?ima=2217&cd=133&ct=radio')?.feed).toBe('radio')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/diary/nananiji_movie?ima=2246')?.feed).toBe('movie')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ima=2342&ct=photoga')?.feed).toBe('photo')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/diary/special/list?ima=2638')?.feed).toBe('live-report')
    })

    test('treats matching detail urls as their feed family', () => {
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447001?ima=3201')?.feed).toBe('official-blog')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/contents/6390467233112?ima=3253')?.feed).toBe('radio')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447178?ima=3255&cd=nananiji_movie')?.feed).toBe('movie')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447239?ima=3300&cd=special')?.feed).toBe('live-report')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/gallery/p10053?ima=3257')?.feed).toBe('photo')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/contents_list?ima=3546&cd=122&ct=member_photo_053')?.feed).toBe('photo')
    })

    test('extractBasicInfo follows the resolved feed id', () => {
        expect(
            NanabunnonijyuuniWebsiteSpider.extractBasicInfo(
                'https://nanabunnonijyuuni-mobile.com/s/n110/news/list?ima=2149&ct=news',
            )?.u_id,
        ).toBe('22/7:fc-news')
        expect(
            NanabunnonijyuuniWebsiteSpider.extractBasicInfo(
                'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ima=2342&ct=photoga',
            )?.u_id,
        ).toBe('22/7:photo')
    })
})

describe('buildPhotoAlbumArticle', () => {
    test('groups a photoga page into one album article with all media and member notes', () => {
        const [article] = buildPhotoAlbumArticle(
            {
                feed: 'photo',
                u_id: '22/7:photo',
                label: '22/7 Photo',
            },
            {
                detailUrl: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga',
                title: '3rd Anniversary',
                dateText: '2026.03.19',
                summary: '3rd Anniversary',
                member: null,
                thumbnail: null,
            },
            {
                currentUrl: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga',
                albumId: 'photoga',
                pageTheme: '3rd Anniversary',
                entries: [
                    {
                        modalId: 'modal-1',
                        dataCode: '35054',
                        detailUrl: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga#modal-1',
                        title: '3rd Anniversary - 北原実咲',
                        theme: '3rd Anniversary',
                        dateText: '2026.03.19',
                        member: '北原実咲',
                        bodyText: '最初のメッセージ',
                        bodyHtml: '<p>最初のメッセージ</p>',
                        media: [{ type: 'photo', url: 'https://example.com/1.jpg', alt: '北原実咲' }],
                        uAvatar: 'https://example.com/a1.jpg',
                        extraData: {
                            modal_id: 'modal-1',
                            photo_code: '35054',
                        },
                    },
                    {
                        modalId: 'modal-2',
                        dataCode: '35055',
                        detailUrl: 'https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga#modal-2',
                        title: '3rd Anniversary - 黒崎ありす',
                        theme: '3rd Anniversary',
                        dateText: '2026.03.19',
                        member: '黒崎ありす',
                        bodyText: '次のメッセージ',
                        bodyHtml: '<p>次のメッセージ</p>',
                        media: [{ type: 'photo', url: 'https://example.com/2.jpg', alt: '黒崎ありす' }],
                        uAvatar: 'https://example.com/a2.jpg',
                        extraData: {
                            modal_id: 'modal-2',
                            photo_code: '35055',
                        },
                    },
                ],
            },
        )

        expect(article.a_id).toBe('photo:album:photoga:35054')
        expect(article.username).toBe('22/7 Photo')
        expect(article.url).toBe('https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga')
        expect(article.has_media).toBe(true)
        expect(article.media?.map((media) => media.url)).toEqual([
            'https://example.com/1.jpg',
            'https://example.com/2.jpg',
        ])
        expect(article.content).toContain('【3rd Anniversary】')
        expect(article.content).toContain('【北原実咲】')
        expect(article.content).toContain('【黒崎ありす】')
        expect(article.extra?.data?.album_id).toBe('photoga')
        expect(article.extra?.data?.members).toEqual(['北原実咲', '黒崎ありす'])
        expect(article.extra?.data?.entries).toHaveLength(2)
    })
})

describe('buildWebsiteArticle', () => {
    const radioConfig: FeedConfig = {
        feed: 'radio',
        u_id: '22/7:radio',
        label: '22/7 Radio',
    }
    const movieConfig: FeedConfig = {
        feed: 'movie',
        u_id: '22/7:movie',
        label: '22/7 Movie',
    }

    test('keeps radio title and falls back to list cover when detail media is empty', () => {
        const article = buildWebsiteArticle(
            radioConfig,
            'https://nanabunnonijyuuni-mobile.com/s/n110/contents/6390467233112',
            {
                detailUrl: 'https://nanabunnonijyuuni-mobile.com/s/n110/contents/6390467233112',
                title: 'Radio Title',
                dateText: '2026.03.20',
                summary: null,
                member: null,
                thumbnail: 'https://example.com/radio-cover.jpg',
            },
            {
                title: '',
                dateText: '2026.03.20',
                bodyText: 'Radio body',
                bodyHtml: '<p>Radio body</p>',
                member: null,
                media: [],
            },
        )

        expect(article.content).toContain('【Radio Title】')
        expect(article.has_media).toBe(true)
        expect(article.media).toEqual([
            {
                type: 'photo',
                url: 'https://example.com/radio-cover.jpg',
            },
        ])
        expect(article.extra?.data?.title).toBe('Radio Title')
    })

    test('keeps movie title and passes through extracted poster thumbnails', () => {
        const article = buildWebsiteArticle(
            movieConfig,
            'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447178?cd=nananiji_movie',
            {
                detailUrl: 'https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447178?cd=nananiji_movie',
                title: 'Movie List Title',
                dateText: '2026.03.20',
                summary: null,
                member: null,
                thumbnail: null,
            },
            {
                title: 'Movie Detail Title',
                dateText: '2026.03.20',
                bodyText: '',
                bodyHtml: '',
                member: null,
                media: [
                    {
                        type: 'video_thumbnail',
                        url: 'https://example.com/movie-poster.jpg',
                    },
                ],
            },
        )

        expect(article.content).toContain('【Movie Detail Title】')
        expect(article.has_media).toBe(true)
        expect(article.media).toEqual([
            {
                type: 'video_thumbnail',
                url: 'https://example.com/movie-poster.jpg',
            },
        ])
        expect(article.extra?.data?.title).toBe('Movie Detail Title')
    })
})
