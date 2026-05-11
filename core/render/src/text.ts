import { platformArticleMapToActionText, platformNameMap } from '@idol-bbq-utils/spider/const'
import type { Article } from '@/types'
import dayjs from 'dayjs'
import { type GenericFollows, Platform } from '@idol-bbq-utils/spider/types'
import { orderBy } from 'lodash'

type Follows = GenericFollows & {
    created_at: number
}

type ArticleTextOptions = {
    collapsedArticleIds?: Set<string | number>
}

const TAB = ' '.repeat(4)
function formatTime(unix_timestamp: number) {
    return dayjs.unix(unix_timestamp).format('YYYY-MM-DD HH:mmZ')
}

function parseTranslationContent(article: Article) {
    /***** 翻译原文 *****/
    let content = article.translation || ''
    /***** 翻译原文结束 *****/

    /***** 图片描述翻译 *****/
    let media_translations: Array<string> = []
    for (const [idx, media] of (article.media || []).entries()) {
        if (media.type === 'photo' && media.translation) {
            media_translations.push(`图片${idx + 1} alt: ${media.translation as string}`)
        }
    }
    if (media_translations.length > 0) {
        content = `${content}\n\n${media_translations.join(`\n---\n`)}`
    }
    /***** 图片描述结束 *****/

    /***** extra描述 *****/
    if (article.extra) {
        const extra = article.extra
        if (extra.translation) {
            content = `${content}\n~~~\n${extra.translation}`
        }
    }
    /***** extra描述结束 *****/
    return content
}

function parseRawContent(article: Article) {
    let content = article.content ?? ''
    let raw_alts = []
    for (const [idx, media] of (article.media || []).entries()) {
        if (media.type === 'photo' && media.alt) {
            raw_alts.push(`photo${idx + 1} alt: ${media.alt as string}`)
        }
    }
    if (raw_alts.length > 0) {
        content = `${content}\n\n${raw_alts.join(`\n---\n`)}`
    }
    if (article.extra) {
        const extra = article.extra
        // card parser
        if (extra.content) {
            content = `${content}\n~~~\n${extra.content}`
        }
    }
    return content
}

function truncateCompactText(text: string, maxLength: number) {
    const normalized = text.trim()
    if (!normalized || normalized.length <= maxLength) {
        return normalized
    }

    const slice = normalized.slice(0, maxLength)
    const softCut = Math.max(
        slice.lastIndexOf('\n'),
        slice.lastIndexOf('。'),
        slice.lastIndexOf('！'),
        slice.lastIndexOf('？'),
    )
    const cutIndex = softCut > Math.floor(maxLength * 0.6) ? softCut + 1 : maxLength
    return `${slice.slice(0, cutIndex).trimEnd()}……`
}

function extractTextHeadline(text: string, maxLength: number = 80) {
    const lines = text
        .replace(/\r\n/g, '\n')
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean)

    if (lines.length === 0) {
        return ''
    }

    const headline =
        lines.find((line) => !/^[-~～—─＿=]+$/.test(line) && !/^(photo|图片)\d+\s+alt:/i.test(line)) || lines[0] || ''

    return truncateCompactText(headline.replace(/\s+/g, ' '), maxLength)
}

function extractArticleHeadline(article: Article, maxLength: number = 80) {
    const candidates = [
        article.content,
        article.translation,
        article.extra?.content,
        article.extra?.translation,
        `${article.username || ''} ${platformNameMap[article.platform] || ''}`.trim(),
    ]

    for (const candidate of candidates) {
        const headline = extractTextHeadline(String(candidate || ''), maxLength)
        if (headline) {
            return headline
        }
    }

    return truncateCompactText(formatCompactMetaline(article).replace(/\s+/g, ' '), maxLength)
}

function parseCompactRawContent(article: Article) {
    const raw = parseRawContent(article)
    if (article.platform !== Platform.YouTube) {
        return raw
    }

    const [title = '', ...rest] = raw.split(/\n{2,}/)
    const description = rest.join('\n\n').trim()
    if (!description) {
        return raw
    }

    const compactDescription = truncateCompactText(description, 360)
    if (compactDescription === description) {
        return raw
    }

    return `${title.trim()}\n\n${compactDescription}\n\n[描述过长，已截断，完整内容请打开链接查看]`
}

/**
 * 原文 -> 媒体文件alt -> extra
 */
function getArticleStableId(article: Article) {
    return (article as any).id ?? `${article.platform}:${article.a_id}`
}

function shouldCollapseArticle(article: Article, options?: ArticleTextOptions) {
    return options?.collapsedArticleIds?.has(getArticleStableId(article)) === true
}

function formatCollapsedReferenceId(article: Article) {
    const id = String(article.u_id || article.username || article.a_id || '').trim()
    if (!id) {
        return 'ref'
    }
    if (id.startsWith('@') || id.includes(':')) {
        return id
    }
    return `@${id}`
}

function formatCollapsedReferenceTime(article: Article, rootArticle: Article) {
    if (!article.created_at) {
        return ''
    }
    const createdAt = dayjs.unix(article.created_at)
    const rootCreatedAt = rootArticle.created_at ? dayjs.unix(rootArticle.created_at) : null
    return rootCreatedAt && createdAt.isSame(rootCreatedAt, 'day')
        ? createdAt.format('HH:mm')
        : createdAt.format('MM-DD HH:mm')
}

function formatCollapsedReferenceToken(article: Article, rootArticle: Article) {
    return [formatCollapsedReferenceId(article), formatCollapsedReferenceTime(article, rootArticle)]
        .filter(Boolean)
        .join(' ')
}

function formatCollapsedReferenceChain(article: Article, rootArticle: Article, options?: ArticleTextOptions) {
    const tokens: Array<string> = []
    let currentArticle: Article | null = article
    while (currentArticle && shouldCollapseArticle(currentArticle, options)) {
        tokens.push(formatCollapsedReferenceToken(currentArticle, rootArticle))
        currentArticle = currentArticle.ref && typeof currentArticle.ref === 'object' ? currentArticle.ref : null
    }

    return `${tokens.join('、')}（略）`
}

function articleToText(article: Article, options?: ArticleTextOptions) {
    let currentArticle: Article | null = article
    const rootArticle = article
    let format_article = ''
    let isRoot = true
    while (currentArticle) {
        if (!isRoot && shouldCollapseArticle(currentArticle, options)) {
            format_article += formatCollapsedReferenceChain(currentArticle, rootArticle, options)
            break
        }
        const metaline = formatMetaline(currentArticle)
        format_article += `${metaline}`
        if (currentArticle.content) {
            format_article += '\n\n'
        }
        if (currentArticle.translated_by) {
            let translation = parseTranslationContent(currentArticle)
            format_article += `${translation}\n${'-'.repeat(6)}↑${(currentArticle.translated_by || '大模型') + '渣翻'}--↓原文${'-'.repeat(6)}\n`
        }

        /* 原文 */
        let raw_article = parseRawContent(currentArticle)
        format_article += `${raw_article}`
        if (currentArticle.ref) {
            format_article += `\n\n${'-'.repeat(12)}\n\n`
        }
        // get ready for next run
        if (currentArticle.ref && typeof currentArticle.ref === 'object') {
            currentArticle = currentArticle.ref
            isRoot = false
        } else {
            currentArticle = null
        }
    }
    return format_article
}

function compactArticleToText(article: Article, options?: ArticleTextOptions) {
    let currentArticle: Article | null = article
    const rootArticle = article
    let format_article = ''
    let isRoot = true
    while (currentArticle) {
        if (!isRoot && shouldCollapseArticle(currentArticle, options)) {
            format_article += formatCollapsedReferenceChain(currentArticle, rootArticle, options)
            break
        }
        const metaline = formatCompactMetaline(currentArticle)
        format_article += `${metaline}`
        if (currentArticle.content) {
            format_article += '\n\n'
        }
        if (currentArticle.translated_by) {
            const translation = parseTranslationContent(currentArticle)
            format_article += `${translation}\n${'-'.repeat(6)}↑${(currentArticle.translated_by || '大模型') + '渣翻'}--↓原文${'-'.repeat(6)}\n`
        }

        const raw_article = parseCompactRawContent(currentArticle)
        format_article += `${raw_article}`
        if (currentArticle.ref) {
            format_article += `\n\n${'-'.repeat(12)}\n\n`
        }
        if (currentArticle.ref && typeof currentArticle.ref === 'object') {
            currentArticle = currentArticle.ref
            isRoot = false
        } else {
            currentArticle = null
        }
    }
    return format_article
}

function followsToText(data: Array<[Platform, Array<[Follows, Follows | null]>]>) {
    // follows to texts
    const texts = [] as Array<string>
    // convert to string
    for (let [platform, follows] of data) {
        if (follows.length === 0) {
            continue
        }
        // 按粉丝数量大的排序
        follows = orderBy(follows, (f) => f[0].followers, 'desc')
        const follow = follows[0]
        if (!follow) {
            continue
        }
        const [cur, pre] = follow
        let text_to_send =
            `${platformNameMap[platform]}:\n${pre?.created_at ? `${formatTime(pre.created_at)}\n⬇️\n` : ''}${formatTime(cur.created_at)}\n\n` +
            follows
                .map(([cur, pre]) => {
                    let text = `${cur.username}\n${' '.repeat(4)}`
                    if (pre?.followers) {
                        text += `${pre.followers.toString().padStart(2)}  --->  `
                    }
                    if (cur.followers) {
                        text += `${cur.followers.toString().padEnd(2)}`
                    }
                    const offset = (cur.followers || 0) - (pre?.followers || 0)
                    text += `${TAB}${offset >= 0 ? '+' : ''}${offset.toString()}`
                    return text
                })
                .join('\n')
        texts.push(text_to_send)
    }
    return texts.join('\n\n')
}

function formatMetaline(article: Article) {
    let metaline =
        [article.username, article.u_id, `来自${platformNameMap[article.platform]}`].filter(Boolean).join(TAB) + '\n'
    const action = platformArticleMapToActionText[article.platform][article.type]
    metaline += [formatTime(article.created_at), `${action}：`].join(TAB)
    return metaline
}

function formatCompactMetaline(article: Article) {
    const header = [article.username, `来自${platformNameMap[article.platform]}`].filter(Boolean).join(TAB)
    const action = platformArticleMapToActionText[article.platform][article.type]
    return `${header}\n${[formatTime(article.created_at), `${action}：`].join(TAB)}`
}

export {
    articleToText,
    compactArticleToText,
    type ArticleTextOptions,
    extractArticleHeadline,
    extractTextHeadline,
    followsToText,
    formatCompactMetaline,
    formatMetaline,
    parseRawContent,
    parseTranslationContent,
}
