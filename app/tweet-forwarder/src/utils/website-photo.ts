import { Platform, type MediaType } from '@idol-bbq-utils/spider/types'
import type { Article } from '@idol-bbq-utils/render/types'

export type ArticleWithIdLike = Article & { id: number }

type WebsitePhotoExtraData = Record<string, unknown> & {
    feed?: unknown
    site?: unknown
    host?: unknown
    title?: unknown
    summary?: unknown
    member?: unknown
    raw_html?: unknown
    album_id?: unknown
    theme?: unknown
    modal_id?: unknown
    photo_code?: unknown
    entries?: unknown
    entry_count?: unknown
}

function asString(value: unknown): string | null {
    return typeof value === 'string' && value.trim() ? value.trim() : null
}

function getWebsitePhotoData(article: Pick<ArticleWithIdLike, 'platform' | 'u_id' | 'extra'>): WebsitePhotoExtraData | null {
    if (article.platform !== Platform.Website || article.u_id !== '22/7:photo') {
        return null
    }
    if (!article.extra || article.extra.extra_type !== 'website_meta') {
        return null
    }

    const data = article.extra.data as WebsitePhotoExtraData | undefined
    if (!data || data.feed !== 'photo') {
        return null
    }

    return data
}

function stripHash(url: string | null | undefined) {
    if (!url) {
        return null
    }
    try {
        const parsed = new URL(url)
        parsed.hash = ''
        return parsed.href
    } catch {
        return url
    }
}

function extractEntryBody(content: string | null) {
    const text = content?.trim()
    if (!text) {
        return ''
    }

    const lines = text.split('\n')
    if (/^【.+】$/.test(lines[0] || '')) {
        lines.shift()
    }
    while (lines[0] === '') {
        lines.shift()
    }
    return lines.join('\n').trim()
}

function dedupeMedia(media: Array<{ type: MediaType; url: string; alt?: string }> | null | undefined) {
    const dedup = new Map<string, { type: MediaType; url: string; alt?: string }>()
    for (const item of media || []) {
        if (!item?.url) {
            continue
        }
        dedup.set(`${item.type}:${item.url}`, item)
    }
    return Array.from(dedup.values())
}

export function isWebsitePhotoArticle(article: Pick<ArticleWithIdLike, 'platform' | 'u_id' | 'extra'>) {
    return Boolean(getWebsitePhotoData(article))
}

export function isWebsitePhotoAlbumArticle(article: Pick<ArticleWithIdLike, 'platform' | 'u_id' | 'extra' | 'a_id'>) {
    return isWebsitePhotoArticle(article) && article.a_id.startsWith('photo:album:')
}

export function getWebsitePhotoBatchKey(article: Pick<ArticleWithIdLike, 'platform' | 'u_id' | 'extra' | 'created_at'>) {
    const data = getWebsitePhotoData(article)
    const albumId = asString(data?.album_id)
    if (!albumId) {
        return null
    }
    return `${albumId}:${article.created_at}`
}

export function buildWebsitePhotoAlbumArticle(articles: Array<ArticleWithIdLike>): ArticleWithIdLike | null {
    if (articles.length === 0) {
        return null
    }

    const ordered = [...articles].sort((a, b) => a.id - b.id)
    const first = ordered[0]
    const firstData = getWebsitePhotoData(first)
    const albumId = asString(firstData?.album_id)
    if (!albumId) {
        return null
    }

    const theme =
        asString(firstData?.theme)
        || asString(firstData?.summary)
        || asString(firstData?.title)?.split(' - ')[0]
        || asString(first.content)?.match(/^【(.+?)】/)?.[1]
        || '22/7 Photo'

    const entries = ordered.map((article) => {
        const data = getWebsitePhotoData(article)
        return {
            articleId: article.a_id,
            detailUrl: article.url,
            title: asString(data?.title) || null,
            dateText: '',
            member: article.username || asString(data?.member) || null,
            bodyText: extractEntryBody(article.content),
            bodyHtml: asString(data?.raw_html) || '',
            media: (article.media || []) as Array<{ type: MediaType; url: string; alt?: string }>,
            uAvatar: article.u_avatar || null,
            extraData: {
                album_id: albumId,
                theme: asString(data?.theme) || theme,
                modal_id: asString(data?.modal_id),
                photo_code: asString(data?.photo_code),
            },
        }
    })

    const contentBody = entries
        .map((entry) => {
            const section = [entry.member ? `【${entry.member}】` : '', entry.bodyText].filter(Boolean).join('\n')
            return section
        })
        .filter(Boolean)
        .join('\n\n')

    const mergedMedia = dedupeMedia(entries.flatMap((entry) => entry.media || []))
    const members = Array.from(new Set(entries.map((entry) => entry.member).filter(Boolean))) as string[]
    const albumAnchor =
        asString(entries[0]?.extraData?.photo_code)
        || asString(firstData?.photo_code)
        || first.a_id.split(':').pop()
        || String(first.id)
    const baseUrl = stripHash(first.url) || first.url

    return {
        ...first,
        a_id: `photo:album:${albumId}:${albumAnchor}`,
        username: '22/7 Photo',
        url: baseUrl,
        content: [`【${theme}】`, contentBody].filter(Boolean).join('\n\n'),
        ref: null,
        has_media: mergedMedia.length > 0,
        media: mergedMedia,
        extra: {
            data: {
                site: '22/7',
                host: 'nanabunnonijyuuni-mobile.com',
                feed: 'photo',
                title: theme,
                member: null,
                summary: theme,
                raw_html: entries.map((entry) => entry.bodyHtml).filter(Boolean).join('\n<hr />\n'),
                album_id: albumId,
                album_anchor: albumAnchor,
                entry_count: entries.length,
                members: members.length > 0 ? members : null,
                entries,
            },
            content: theme,
            media: mergedMedia,
            extra_type: 'website_meta',
        },
    }
}

export function normalizeWebsitePhotoArticles<T extends ArticleWithIdLike>(articles: Array<T>): Array<T> {
    const grouped = new Map<string, Array<T>>()
    const seen = new Set<string>()
    const normalized: Array<T> = []

    for (const article of articles) {
        const key = getWebsitePhotoBatchKey(article)
        if (key) {
            const group = grouped.get(key) || []
            group.push(article)
            grouped.set(key, group)
        }
    }

    for (const article of articles) {
        const key = getWebsitePhotoBatchKey(article)
        if (!key) {
            normalized.push(article)
            continue
        }
        if (seen.has(key)) {
            continue
        }
        seen.add(key)

        const group = grouped.get(key) || []
        const album = group
            .filter((item) => isWebsitePhotoAlbumArticle(item))
            .sort((a, b) => {
                const aCount = Number((getWebsitePhotoData(a)?.entry_count as number | undefined) || 0)
                const bCount = Number((getWebsitePhotoData(b)?.entry_count as number | undefined) || 0)
                return bCount - aCount || b.id - a.id
            })[0]

        if (album) {
            normalized.push(album)
            continue
        }

        const syntheticAlbum = buildWebsitePhotoAlbumArticle(group)
        if (syntheticAlbum) {
            normalized.push(syntheticAlbum as T)
            continue
        }

        normalized.push(article)
    }

    return normalized
}
