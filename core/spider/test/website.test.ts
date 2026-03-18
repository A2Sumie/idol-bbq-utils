import { describe, expect, test } from 'bun:test'
import { NanabunnonijyuuniWebsiteSpider } from '../src/spiders/website'

describe('NanabunnonijyuuniWebsiteSpider.resolveFeed', () => {
    test('matches supported 22/7 FC and live-report routes', () => {
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/ticket/list?ima=2101')?.feed).toBe('ticket')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/news/list?ima=2149&ct=news')?.feed).toBe('fc-news')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/contents_list?ima=2217&cd=133&ct=radio')?.feed).toBe('radio')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/diary/nananiji_movie?ima=2246')?.feed).toBe('movie')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ima=2342&ct=photoga')?.feed).toBe('photo')
        expect(NanabunnonijyuuniWebsiteSpider.resolveFeed('https://nanabunnonijyuuni-mobile.com/s/n110/diary/special/list?ima=2638')?.feed).toBe('live-report')
    })

    test('treats matching detail urls as their feed family', () => {
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
