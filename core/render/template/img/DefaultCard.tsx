import { parseRawContent, parseTranslationContent } from '@/text'
import type { Article } from '@/types'
import { X } from '@idol-bbq-utils/spider'
import { Platform } from '@idol-bbq-utils/spider/types'
import { platformArticleMapToActionText } from '@idol-bbq-utils/spider/const'
import clsx from 'clsx'
import dayjs from 'dayjs'
import _, { reduce } from 'lodash'
import type { JSX } from 'react/jsx-runtime'
import SVG, { Website227FC, Website227Official } from '@/img/assets/svg'
import { KOZUE } from '@/img/assets/img'
import type { RenderParserOptions } from '@/registry'

const CARD_WIDTH = 600
const CONTENT_WIDTH = CARD_WIDTH - 16 * 2 - 64 - 12
const BASE_FONT_SIZE = 16

type WebsiteBrandKey = 'official' | 'fc'

const OFFICIAL_227_WEBSITE_FEEDS = new Set(['official-news', 'official-blog', 'live-report'])
const FC_227_WEBSITE_FEEDS = new Set(['fc-news', 'ticket', 'radio', 'movie', 'photo'])
const DEFAULT_PLATFORM_BADGE_WIDTH = 32
const DEFAULT_CARD_FEATURES = new Set(['media-contain', 'website-inline-media'])

type CardRenderFeatures = Set<string>
type InlineContentBlock =
    | {
          type: 'text'
          text: string
      }
    | {
          type: 'image'
          url: string
          alt?: string
      }

function resolveCardFeatures(options?: RenderParserOptions): CardRenderFeatures {
    return new Set([...DEFAULT_CARD_FEATURES, ...(options?.features || [])])
}

function hasFeature(features: CardRenderFeatures, feature: string) {
    return features.has(feature)
}

const WEBSITE_BRAND_CONFIG = {
    official: {
        badgeIcon: Website227Official,
        badgeRatio: 54.615 / 80,
        badgeWidth: 42,
        badgeOpacity: 0.5,
        avatarBackground: 'linear-gradient(135deg, #f8fdff 0%, #e0f6ff 100%)',
        avatarBorderColor: '#b6e4f8',
        avatarText: 'HP',
        avatarTextColor: '#008fd0',
        avatarFontSizeAt64: 22,
        avatarLetterSpacing: -0.4,
    },
    fc: {
        badgeIcon: Website227FC,
        badgeRatio: 71.39 / 505.05,
        badgeWidth: 96,
        badgeOpacity: 0.62,
        backdropIcon: Website227Official,
        backdropRatio: 54.615 / 80,
        backdropWidth: 54,
        backdropOpacity: 0.26,
        avatarBackground: 'linear-gradient(135deg, #fff6fb 0%, #f3f5ff 52%, #f5f9e7 100%)',
        avatarBorderColor: '#dccce9',
        avatarText: 'FC',
        avatarTextColor: '#8b67aa',
        avatarFontSizeAt64: 22,
        avatarLetterSpacing: -0.4,
    },
} as const

function getContentWidth(level: number) {
    if (level === 0) {
        return CONTENT_WIDTH
    }
    return CONTENT_WIDTH - 16 * 2 * level
}

function getImageWidth(level: number) {
    if (level === 0) {
        return (CONTENT_WIDTH - 4) / 2
    }
    return (CONTENT_WIDTH - 4 - 16 * 2 * level) / 2
}

function decodeHtmlEntities(text: string) {
    return text
        .replace(/&nbsp;/gi, ' ')
        .replace(/&amp;/gi, '&')
        .replace(/&lt;/gi, '<')
        .replace(/&gt;/gi, '>')
        .replace(/&quot;/gi, '"')
        .replace(/&#39;/gi, "'")
}

function htmlToPlainText(html: string) {
    return decodeHtmlEntities(
        html
            .replace(/<script[\s\S]*?<\/script>/gi, '')
            .replace(/<style[\s\S]*?<\/style>/gi, '')
            .replace(/<(br|\/p|\/div|\/li|\/h[1-6])\b[^>]*>/gi, '\n')
            .replace(/<[^>]+>/g, '')
            .replace(/\u00a0/g, ' '),
    )
        .split('\n')
        .map((line) => line.replace(/[ \t]+/g, ' ').trim())
        .filter(Boolean)
        .join('\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim()
}

function absoluteUrl(value: string, baseUrl?: string | null) {
    try {
        return new URL(value, baseUrl || undefined).href
    } catch {
        return value
    }
}

function getWebsiteTitle(article: Article) {
    const title = (article.extra?.data as any)?.title
    if (typeof title === 'string' && title.trim()) {
        return `【${title.trim()}】`
    }
    return article.content?.match(/^【.+?】/)?.[0] || ''
}

function getWebsiteInlineBlocks(article: Article, features: CardRenderFeatures): Array<InlineContentBlock> {
    if (!hasFeature(features, 'website-inline-media')) {
        return []
    }
    if (article.platform !== Platform.Website || article.extra?.extra_type !== 'website_meta') {
        return []
    }
    const rawHtml = (article.extra.data as any)?.raw_html
    if (typeof rawHtml !== 'string' || !rawHtml.includes('<img')) {
        return []
    }

    const blocks: Array<InlineContentBlock> = []
    const imageRegex = /<img\b[^>]*\bsrc=(["']?)([^"'\s>]+)\1[^>]*>/gi
    let cursor = 0
    let match: RegExpExecArray | null
    while ((match = imageRegex.exec(rawHtml))) {
        const text = htmlToPlainText(rawHtml.slice(cursor, match.index))
        if (text) {
            blocks.push({ type: 'text', text })
        }
        const src = match[2]
        if (src) {
            const alt = match[0].match(/\balt=(["']?)(.*?)\1(?:\s|>|$)/i)?.[2]
            blocks.push({
                type: 'image',
                url: absoluteUrl(src, article.url),
                alt: alt ? decodeHtmlEntities(alt).trim() : undefined,
            })
        }
        cursor = match.index + match[0].length
    }
    const tailText = htmlToPlainText(rawHtml.slice(cursor))
    if (tailText) {
        blocks.push({ type: 'text', text: tailText })
    }

    return blocks.some((block) => block.type === 'image') ? blocks : []
}

export function resolve227WebsiteBrandKey(article: Pick<Article, 'platform' | 'extra'>): WebsiteBrandKey | null {
    if (article.platform !== Platform.Website || !article.extra || article.extra.extra_type !== 'website_meta') {
        return null
    }

    const data = article.extra.data as Record<string, unknown> | undefined
    if (!data || data.site !== '22/7' || typeof data.feed !== 'string') {
        return null
    }

    if (OFFICIAL_227_WEBSITE_FEEDS.has(data.feed)) {
        return 'official'
    }

    if (FC_227_WEBSITE_FEEDS.has(data.feed)) {
        return 'fc'
    }

    return null
}

function getPlatformBadge(article: Article) {
    const websiteBrandKey = resolve227WebsiteBrandKey(article)
    if (!websiteBrandKey) {
        return {
            layers: [
                {
                    width: DEFAULT_PLATFORM_BADGE_WIDTH,
                    ratio: SVG[article.platform].ratio,
                    icon: SVG[article.platform].icon,
                    opacity: 0.2,
                    right: 16,
                    top: 16,
                    rotate: 6,
                },
            ],
        }
    }

    const brand = WEBSITE_BRAND_CONFIG[websiteBrandKey]
    const fcBrand = websiteBrandKey === 'fc' ? WEBSITE_BRAND_CONFIG.fc : null
    return {
        layers: [
            ...(fcBrand
                ? [
                      {
                          width: fcBrand.backdropWidth,
                          ratio: fcBrand.backdropRatio,
                          icon: fcBrand.backdropIcon,
                          opacity: fcBrand.backdropOpacity,
                          right: 54,
                          top: 10,
                          rotate: -10,
                      },
                  ]
                : []),
            {
                width: brand.badgeWidth,
                ratio: brand.badgeRatio,
                icon: brand.badgeIcon,
                opacity: brand.badgeOpacity,
                right: 16,
                top: 16,
                rotate: 6,
            },
        ],
    }
}

function Avatar({ article, size }: { article: Article; size: 32 | 64 }) {
    const websiteBrandKey = resolve227WebsiteBrandKey(article)
    if (!websiteBrandKey && article.u_avatar) {
        return (
            <img
                tw="rounded-full flex-none"
                style={{
                    width: size,
                    height: size,
                    objectFit: 'cover',
                }}
                src={article.u_avatar}
                alt={article.username}
            />
        )
    }

    if (!websiteBrandKey) {
        return <div tw="rounded-full bg-gray-200 flex-none" style={{ width: size, height: size }} />
    }

    const brand = WEBSITE_BRAND_CONFIG[websiteBrandKey]
    return (
        <div
            tw="rounded-full flex-none overflow-hidden flex items-center justify-center"
            style={{
                width: size,
                height: size,
                background: brand.avatarBackground,
                border: `1px solid ${brand.avatarBorderColor}`,
                boxShadow: '0 2px 8px rgba(15, 23, 42, 0.08)',
            }}
        >
            <span
                tw="font-bold leading-none"
                style={{
                    color: brand.avatarTextColor,
                    fontSize: (brand.avatarFontSizeAt64 * size) / 64,
                    letterSpacing: brand.avatarLetterSpacing,
                }}
            >
                {brand.avatarText}
            </span>
        </div>
    )
}

function Metaline({ article }: { article: Article }) {
    return (
        <div
            tw="flex flex-wrap text-base leading-tight items-baseline"
            style={{
                columnGap: '8px',
            }}
        >
            <span tw="font-bold" lang="zh-CN" style={{ fontWeight: 700 }}>
                {article.username}
            </span>
            <span tw="font-normal text-[#46556a]" lang="zh-CN" style={{ fontWeight: 500 }}>
                @{article.u_id} · {dayjs.unix(article.created_at).format('YY年MM月DD日 HH:mmZ')}
            </span>
            <span tw="text-xs text-[#46556a]" lang="zh-CN" style={{ fontWeight: 600 }}>
                {platformArticleMapToActionText[article.platform][article.type]}
            </span>
        </div>
    )
}

function Divider({ text, dash }: { text?: string; dash?: boolean }) {
    return (
        <div tw="flex items-center px-5 h-3 text-xs leading-tight">
            <div
                tw="border-t border-idol-tertiary flex-grow"
                style={{
                    borderTopStyle: dash ? 'dashed' : 'solid',
                }}
            />
            {text && (
                <span tw="mx-2 text-idol-tertiary" lang="zh-CN">
                    {text}
                </span>
            )}
            {text && (
                <div
                    tw="border-t border-idol-tertiary flex-grow"
                    style={{
                        borderTopStyle: dash ? 'dashed' : 'solid',
                    }}
                />
            )}
        </div>
    )
}

function ImageTile({ url, alt, width, contain }: { url: string; alt?: string; width: number; contain: boolean }) {
    return (
        <div tw="flex overflow-hidden" style={{ flexBasis: `${width}px` }}>
            <div
                tw="flex relative w-full bg-[#f7f9fc]"
                style={{
                    paddingTop: '56.25%',
                }}
            >
                <img
                    src={url}
                    tw="left-0 right-0 top-0 bottom-0 absolute"
                    style={{
                        width: '100%',
                        height: '100%',
                        objectFit: contain ? 'contain' : 'cover',
                    }}
                    alt={alt}
                />
            </div>
        </div>
    )
}

function MediaGroup({
    media: _media,
    level,
    features,
}: {
    media: Exclude<Article['media'], null>
    level: number
    features: CardRenderFeatures
}) {
    const media = _media.filter((m) => m.type === 'photo' || m.type === 'video_thumbnail')
    const pairedMedia = media.slice(0, media.length % 2 === 1 ? -1 : media.length)
    const lastMedia = media.length % 2 === 1 ? media[media.length - 1] : null
    const contain = hasFeature(features, 'media-contain')
    return (
        <div
            tw="flex rounded-lg overflow-hidden shadow-sm flex-wrap"
            style={{
                gap: '4px',
            }}
        >
            {pairedMedia.map((m, i) => (
                <ImageTile key={i} url={m.url} alt={m.alt} width={getImageWidth(level)} contain={contain} />
            ))}

            {lastMedia && (
                <ImageTile url={lastMedia.url} alt={lastMedia.alt} width={getContentWidth(level)} contain={contain} />
            )}
        </div>
    )
}

function InlineWebsiteContent({
    article,
    blocks,
    level,
    features,
}: {
    article: Article
    blocks: Array<InlineContentBlock>
    level: number
    features: CardRenderFeatures
}) {
    const title = getWebsiteTitle(article)
    return (
        <div tw="flex flex-col" style={{ rowGap: '4px' }}>
            {title && (
                <pre
                    tw="w-full text-[#202733] my-0 text-base leading-snug"
                    style={{
                        whiteSpace: 'pre-wrap',
                        fontWeight: 600,
                    }}
                >
                    {title}
                </pre>
            )}
            {blocks.map((block, index) =>
                block.type === 'text' ? (
                    <pre
                        key={index}
                        tw="w-full text-[#202733] my-0 text-base leading-snug"
                        style={{
                            whiteSpace: 'pre-wrap',
                            fontWeight: 500,
                        }}
                    >
                        {block.text}
                    </pre>
                ) : (
                    <MediaGroup
                        key={index}
                        media={[{ type: 'photo', url: block.url, alt: block.alt }]}
                        level={level}
                        features={features}
                    />
                ),
            )}
        </div>
    )
}

/**
 * 在Node.js环境中估算文本在指定容器宽度和字体大小下的行数
 * @param {string} text - 要计算的文本内容
 * @param {number} fontSize - 字体大小(px)
 * @param {number} containerWidth - 容器宽度(px)
 * @return {number} 估算的文本高度
 */
function estimateTextLinesHeight(text: string, fontSize: number, containerWidth: number) {
    text = text.trim()
    if (!text) {
        return 0
    }
    // 1. 处理硬换行符 - 分割文本成行
    const paragraphs = text.split('\n')

    // 2. 估算每个字符的平均宽度 - 一个粗略的估计
    // 英文字符约为字体大小的0.6倍，中日韩字符约为字体大小的1.0倍
    const avgCharWidthLatin = fontSize * 0.6 // 拉丁字符(英文、数字等)
    const avgCharWidthCJK = fontSize * 0.95 // 中日韩字符、magic number

    let totalLines = 0

    // 3. 处理每个段落
    for (const paragraph of paragraphs) {
        if (paragraph.length === 0) {
            // 空行计为一行
            totalLines += 1
            continue
        }

        // 估算这个段落的总宽度
        let paragraphWidth = 0

        for (const char of paragraph) {
            // 判断字符是拉丁字符还是CJK字符
            // 这是一个简化的判断，实际情况可能更复杂
            const charCode = char.charCodeAt(0)
            if (charCode > 0x3000) {
                // 粗略判断是否为CJK字符
                paragraphWidth += avgCharWidthCJK
            } else {
                paragraphWidth += avgCharWidthLatin
            }
        }
        // 计算此段落需要的行数
        const linesNeeded = Math.max(1, Math.ceil(paragraphWidth / containerWidth))
        totalLines += linesNeeded
    }
    return totalLines * fontSize * 1.25 // 1.25是行高的倍数
}

function estimateImagesHeight(media: Exclude<Article['media'], null>, level: number = 0) {
    if (!media || media.length === 0) {
        return 0
    }
    const imageCount = media.filter((m) => m.type === 'photo' || m.type === 'video_thumbnail').length
    return (
        ((imageCount % 2) * getContentWidth(level) + Math.floor(imageCount / 2) * getImageWidth(level)) * (9 / 16) +
        (Math.ceil(imageCount / 2) - 1) * 4
    )
}

function estimateInlineWebsiteHeight(article: Article, level: number, features: CardRenderFeatures) {
    const blocks = getWebsiteInlineBlocks(article, features)
    if (blocks.length === 0) {
        return null
    }
    const title = getWebsiteTitle(article)
    const textHeight = blocks.reduce(
        (sum, block) => {
            if (block.type === 'image') {
                return sum + getContentWidth(level) * (9 / 16)
            }
            return sum + estimateTextLinesHeight(block.text, BASE_FONT_SIZE, getContentWidth(level))
        },
        title ? estimateTextLinesHeight(title, BASE_FONT_SIZE, getContentWidth(level)) : 0,
    )
    return textHeight + Math.max(0, blocks.length - 1) * 4
}

function ArticleContent({
    article,
    level = 0,
    features,
}: {
    article: Article
    level: number
    features: CardRenderFeatures
}) {
    const inlineWebsiteBlocks = getWebsiteInlineBlocks(article, features)
    const useInlineWebsiteBlocks = inlineWebsiteBlocks.length > 0
    const shouldRenderMedia = Boolean(article.media && article.media.length > 0 && !useInlineWebsiteBlocks)
    function Content() {
        return (
            <div
                tw={clsx('flex flex-col', {
                    'pb-6': level === 0 && isConversationType(article.type),
                })}
                style={{
                    rowGap: '4px',
                    width: `${level === 0 ? CONTENT_WIDTH : CONTENT_WIDTH - 2 * 16 * level}px`,
                }}
            >
                {level === 0 && <Metaline article={article} />}
                {level !== 0 && (
                    <div
                        tw="flex flex-row"
                        style={{
                            columnGap: '4px',
                        }}
                    >
                        <Avatar article={article} size={32} />
                        <div tw="flex flex-shrink">
                            <Metaline article={article} />
                        </div>
                    </div>
                )}
                {article.translation && (
                    <pre
                        tw="w-full my-0 text-base leading-snug text-[#1f2937]"
                        style={{
                            whiteSpace: 'pre-wrap',
                            fontWeight: 500,
                        }}
                    >
                        {parseTranslationContent(article)}
                    </pre>
                )}
                {article.translation && (
                    <Divider text={article.translated_by ? `由${article.translated_by}提供翻译` : ''} />
                )}
                {article.content && !useInlineWebsiteBlocks && (
                    <pre
                        tw="w-full text-[#202733] my-0 text-base leading-snug"
                        style={{
                            whiteSpace: 'pre-wrap',
                            fontWeight: 500,
                        }}
                    >
                        {parseRawContent(article)}
                    </pre>
                )}
                {useInlineWebsiteBlocks && (
                    <InlineWebsiteContent
                        article={article}
                        blocks={inlineWebsiteBlocks}
                        level={level}
                        features={features}
                    />
                )}
                {shouldRenderMedia && <Divider dash />}
                {shouldRenderMedia && article.media && (
                    <MediaGroup media={article.media} level={level} features={features} />
                )}
                {article.ref && typeof article.ref === 'object' && (
                    <ArticleContent article={article.ref} level={level + 1} features={features} />
                )}
            </div>
        )
    }
    return level === 0 ? (
        <div
            tw="flex flex-row"
            style={{
                columnGap: '12px',
            }}
        >
            <div tw="flex flex-col items-center" style={{ rowGap: '6px' }}>
                <Avatar article={article} size={64} />
                {isConversationType(article.type) && <div tw="flex-grow bg-idol-tertiary w-[2px] rounded-full"></div>}
            </div>
            <Content />
        </div>
    ) : (
        <div tw="flex border border-idol-tertiary rounded-lg p-4 shadow-md">
            <Content />
        </div>
    )
}

function isConversationType(type: Article['type']): boolean {
    return ([X.ArticleTypeEnum.CONVERSATION] as Array<Article['type']>).includes(type)
}

function flatArticle(article: Article): Array<Article> {
    const articles: Array<Article> = []
    let currentArticle: Article | null = article
    while (currentArticle && isConversationType(currentArticle.type)) {
        articles.push({
            ...currentArticle,
            ref: null,
        })
        if (currentArticle.ref && typeof currentArticle.ref === 'object') {
            currentArticle = currentArticle.ref
        } else {
            currentArticle = null
        }
    }
    currentArticle && articles.push(currentArticle)
    return articles
}

function articleHasVisualMedia(article: Article) {
    return flatArticle(article).some((item) =>
        item.media?.some((media) => ['photo', 'video', 'video_thumbnail'].includes(media.type)),
    )
}

function BaseCard({
    article,
    paddingHeight,
    features,
}: {
    article: Article
    paddingHeight: number
    features: CardRenderFeatures
}) {
    const flattedArticle = flatArticle(article)
    const badge = getPlatformBadge(article)
    const hasVisualMedia = articleHasVisualMedia(article)
    return (
        <div
            tw={clsx('p-4 bg-white rounded-2xl shadow-sm h-full w-full flex flex-col relative', {
                'pb-5': hasVisualMedia,
                'pb-3': !hasVisualMedia,
            })}
            style={{
                rowGap: '6px',
            }}
        >
            {badge.layers.map((layer, index) => (
                <img
                    key={`${layer.icon}-${index}`}
                    tw="absolute"
                    style={{
                        right: `${layer.right}px`,
                        top: `${layer.top}px`,
                        transform: `rotate(${layer.rotate}deg)`,
                        opacity: layer.opacity,
                    }}
                    width={layer.width}
                    height={layer.width * layer.ratio}
                    src={layer.icon}
                />
            ))}
            {flattedArticle.map((item, index) => (
                <ArticleContent key={index} article={item} level={0} features={features} />
            ))}
            {/* {paddingHeight > 0 && (
                <div tw="flex justify-center items-center opacity-20">
                    <img src={KOZUE} width={paddingHeight}/>
                </div>
            )} */}
        </div>
    )
}

function estimatedArticleHeight(article: Article, level: number = 0, features: CardRenderFeatures): number {
    const basePadding = 16 * 2
    const inlineWebsiteHeight = estimateInlineWebsiteHeight(article, level, features)
    const articleHeightArray = [
        estimateTextLinesHeight(
            `${article.username} @${article.u_id} · ${dayjs.unix(article.created_at).format('YY年MM月DD日 HH:mmZ')} ${platformArticleMapToActionText[article.platform][article.type]}`,
            BASE_FONT_SIZE,
            getContentWidth(level) - (level === 0 ? 0 : 32), // maybe subtract the avatar width
        ), // metaline
        estimateTextLinesHeight(parseTranslationContent(article) ?? '', BASE_FONT_SIZE, getContentWidth(level)), // translation
        article.translation ? 12 : 0, // translation divider
        inlineWebsiteHeight ??
            estimateTextLinesHeight(parseRawContent(article) ?? '', BASE_FONT_SIZE, getContentWidth(level)), // content
        article.has_media ? 12 : 0, // media or extra divider
        inlineWebsiteHeight === null ? estimateImagesHeight(article.media ?? [], level) : 0, // media
        article.ref && typeof article.ref === 'object'
            ? estimatedArticleHeight(article.ref, level + 1, features) + basePadding * (level + 1)
            : 0, // ref
    ]
    return _(articleHeightArray)
        .filter((item) => item > 0)
        .flatMap((item) => [item, 4])
        .dropRight(1)
        .reduce((a, b) => a + b, 0)
}

function articleParser(
    article: Article,
    options?: RenderParserOptions,
): {
    component: JSX.Element
    height: number
} {
    const features = resolveCardFeatures(options)
    const hasVisualMedia = articleHasVisualMedia(article)
    let flattedArticleHeightArray = flatArticle(article).map((item) => estimatedArticleHeight(item, 0, features))
    let estimatedHeight = [
        16, // padding top
        _(flattedArticleHeightArray)
            .filter((item) => item > 0)
            .flatMap((item) => [item, 24 + 6])
            .dropRight(1) // content
            .reduce((a, b) => a + b, 0),
        hasVisualMedia ? 20 : 12, // padding bottom
    ]
        .flat()
        .reduce((a, b) => a + b, 0)

    let paddingHeight = 0
    const minimumCardRatio = hasVisualMedia ? 1 / 3 : 0.27
    if (estimatedHeight / CARD_WIDTH < minimumCardRatio) {
        paddingHeight = CARD_WIDTH * minimumCardRatio - estimatedHeight
    }
    return {
        component: <BaseCard article={article} paddingHeight={paddingHeight} features={features} />,
        height: estimatedHeight + paddingHeight,
    }
}

export { estimateImagesHeight, estimateTextLinesHeight, BaseCard, articleParser }
export { BASE_FONT_SIZE, CARD_WIDTH, CONTENT_WIDTH }
