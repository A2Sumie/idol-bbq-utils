import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import { buildWebsitePhotoAlbumArticle, normalizeWebsitePhotoArticles } from './website-photo'

function buildSinglePhotoArticle(id: number, code: string, createdAt = 1773673200) {
    return {
        id,
        a_id: `photo:photoga:${code}`,
        u_id: '22/7:photo',
        username: id % 2 === 0 ? '黒崎ありす' : '北原実咲',
        created_at: createdAt,
        content: `【春のかおり - ${id % 2 === 0 ? '黒崎ありす' : '北原実咲'}】\n\nメッセージ${id}`,
        translation: null,
        translated_by: null,
        url: `https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga#photo-modal-${code}`,
        type: 'article',
        ref: null,
        has_media: true,
        media: [
            {
                type: 'photo' as const,
                url: `https://example.com/${code}.jpg`,
            },
        ],
        extra: {
            data: {
                site: '22/7',
                host: 'nanabunnonijyuuni-mobile.com',
                feed: 'photo',
                title: `春のかおり - ${id % 2 === 0 ? '黒崎ありす' : '北原実咲'}`,
                member: id % 2 === 0 ? '黒崎ありす' : '北原実咲',
                summary: '春のかおり',
                raw_html: `<p>メッセージ${id}</p>`,
                album_id: 'photoga',
                theme: '春のかおり',
                modal_id: `photo-modal-${code}`,
                photo_code: code,
            },
            content: '春のかおり',
            media: [
                {
                    type: 'photo' as const,
                    url: `https://example.com/${code}.jpg`,
                },
            ],
            extra_type: 'website_meta',
        },
        u_avatar: `https://example.com/avatar-${code}.jpg`,
        platform: Platform.Website,
    }
}

test('buildWebsitePhotoAlbumArticle merges same-day single photo entries into one batch article', () => {
    const article = buildWebsitePhotoAlbumArticle([
        buildSinglePhotoArticle(51, '35054'),
        buildSinglePhotoArticle(52, '35055'),
    ])

    expect(article?.a_id).toBe('photo:album:photoga:35054')
    expect(article?.username).toBe('22/7 Photo')
    expect(article?.url).toBe('https://nanabunnonijyuuni-mobile.com/s/n110/gallery?ct=photoga')
    expect(article?.media).toHaveLength(2)
    expect(article?.content).toContain('【春のかおり】')
    expect(article?.content).toContain('【北原実咲】')
    expect(article?.content).toContain('【黒崎ありす】')
    expect(article?.extra?.data?.entry_count).toBe(2)
})

test('normalizeWebsitePhotoArticles prefers an album article over matching single entries', () => {
    const singles = [
        buildSinglePhotoArticle(51, '35054'),
        buildSinglePhotoArticle(52, '35055'),
    ]
    const album = buildWebsitePhotoAlbumArticle(singles)

    const normalized = normalizeWebsitePhotoArticles([
        singles[0],
        album!,
        singles[1],
    ])

    expect(normalized).toHaveLength(1)
    expect(normalized[0]?.a_id).toBe('photo:album:photoga:35054')
    expect(normalized[0]?.media).toHaveLength(2)
})
