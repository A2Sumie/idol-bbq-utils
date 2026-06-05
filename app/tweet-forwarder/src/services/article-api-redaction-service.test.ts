import { expect, test } from 'bun:test'
import { sanitizeArticleForApi, sanitizeArticlesForApi } from './article-api-redaction-service'

test('article API redaction preserves content urls while removing host paths and secrets', () => {
    const article = sanitizeArticleForApi({
        id: 1,
        content: 'keep visible article content with /app/ as ordinary text',
        url: 'https://example.test/post/private-id',
        media: [
            {
                type: 'photo',
                url: 'https://cdn.example.test/media.jpg',
                path: '/tmp/private-media.jpg',
                api_key: 'private-api-key',
            },
        ],
        extra: {
            raw: {
                localPath: '/home/sumie/private-extra.json',
                nested: {
                    cookie: 'private-cookie',
                },
            },
        },
        ref: {
            id: 2,
            media: [
                {
                    type: 'video',
                    url: 'https://cdn.example.test/video.mp4',
                    file_path: 'D:\\private\\video.mp4',
                },
            ],
        },
    })
    const serialized = JSON.stringify(article)

    expect(article.content).toBe('keep visible article content with /app/ as ordinary text')
    expect(article.url).toBe('https://example.test/post/private-id')
    expect(article.media[0].url).toBe('https://cdn.example.test/media.jpg')
    expect(article.media[0].path).toBe('[redacted]')
    expect(article.media[0].path_meta).toMatchObject({
        redacted_path: true,
        path_present: true,
    })
    expect(article.media[0].api_key).toBe('[redacted]')
    expect(article.extra.raw.localPath).toBe('[redacted]')
    expect(article.extra.raw.nested.cookie).toBe('[redacted]')
    expect(article.ref.media[0].file_path).toBe('[redacted]')
    expect(serialized).not.toContain('/tmp/private-media.jpg')
    expect(serialized).not.toContain('/home/sumie/private-extra.json')
    expect(serialized).not.toContain('D:\\private\\video.mp4')
    expect(serialized).not.toContain('private-api-key')
    expect(serialized).not.toContain('private-cookie')
})

test('article API redaction maps list payloads', () => {
    const articles = sanitizeArticlesForApi([
        {
            id: 1,
            media: [{ type: 'photo', url: 'https://cdn.example.test/a.jpg', path: '/tmp/a.jpg' }],
        },
    ])

    expect(articles[0].media[0].path).toBe('[redacted]')
})
