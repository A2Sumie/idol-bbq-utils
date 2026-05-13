import { Logger } from '@idol-bbq-utils/log'
import { Platform } from '@idol-bbq-utils/spider/types'
import type { Article } from '@/db'
import { type Media, type MediaTool, MediaToolEnum } from '@/types/media'
import {
    galleryDownloadMediaFile,
    getMediaType,
    plainDownloadMediaFile,
    tryGetCookie,
    ytDlpDownloadMediaFile,
    writeImgToFile,
    extToMime,
} from '@/middleware/media'
import {
    articleToText,
    compactArticleToText,
    extractArticleHeadline,
    formatMetaline,
    ImgConverter,
    type ArticleTextOptions,
} from '@idol-bbq-utils/render'
import { existsSync, readFileSync, unlinkSync } from 'fs'
import path from 'path'
import { cloneDeep } from 'lodash'
import { platformPresetHeadersMap, platformNameMap } from '@idol-bbq-utils/spider/const'
import type { MediaType } from '@idol-bbq-utils/spider/types'
import {
    buildArticleMarker,
    buildShortVideoDedupCandidate,
    buildVideoFingerprintCandidate,
    checkExactCrossPlatformMediaDuplicate,
    checkShortVideoCrossPlatformDuplicate,
    checkVideoFingerprintDuplicate,
    isPersistentMediaPath,
    markExactCrossPlatformMediaSeen,
    markShortVideoCrossPlatformSeen,
    markVideoFingerprintSeen,
    persistMediaFile,
} from './media-cache-service'

export interface RenderedMediaFile {
    path: string
    media_type: MediaType
    sourceArticleId?: string
    sourceUserId?: string
    content_hash?: string
    size_bytes?: number
    duration_seconds?: number
    persistent?: boolean
    sourceUrl?: string
    width?: number
    height?: number
}

export interface RenderResult {
    text: string
    textCollapseMode?: 'article' | 'compact-article' | 'none'
    cardMediaFiles: Array<RenderedMediaFile>
    originalMediaFiles: Array<RenderedMediaFile>
    mediaFiles: Array<RenderedMediaFile>
    shouldSkipSend?: boolean
    skipReason?: string
}

const CARD_TEXT_TITLE_THRESHOLD = 1000
const LONG_TEXT_CARD_TYPES = new Set(['message_pack', 'summary'])

function formatPlatformTag(
    article: Pick<Article, 'platform' | 'username' | 'a_id'>,
    log?: Pick<Logger, 'warn'>,
): string {
    const platformName = platformNameMap[article.platform] || 'Unknown'
    if (platformName === 'Unknown') {
        log?.warn(`Unknown platform for article ${article.a_id}: ${article.platform}`)
    }

    const username = article.username?.trim()
    if (!username || username === platformName) {
        return platformName
    }

    return `${platformName} ${username}`
}

export class RenderService {
    private log?: Logger
    private ArticleConverter = new ImgConverter()

    constructor(log?: Logger) {
        this.log = log?.child({ subservice: 'RenderService' })
    }

    /**
     * Process an article into a ready-to-send payload (text + media).
     */
    async process(
        article: Article,
        config: {
            taskId: string
            render_type?: string
            render_features?: Array<string>
            card_features?: Array<string>
            collapsedArticleIds?: Set<string | number>
            mediaConfig?: Media
            deduplication?: boolean
        },
    ): Promise<RenderResult> {
        const { taskId, render_type, mediaConfig, deduplication } = config
        const cloned_article = cloneDeep(article)

        let maybe_media_files: Array<RenderedMediaFile> = []
        let card_media_files: Array<RenderedMediaFile> = []
        let skipReason: string | undefined

        // 1. Download/Handle Media Files
        if (mediaConfig) {
            const mediaResult = await this.handleMedia(taskId, cloned_article, mediaConfig, deduplication)
            maybe_media_files = mediaResult.files
            skipReason = mediaResult.skipReason
        }

        let text = ''
        let textCollapseMode: RenderResult['textCollapseMode'] = 'none'

        // Helper: Generate Rendered Image
        const generateRenderedImage = async () => {
            try {
                this.log?.debug(`Converting article ${article.a_id} to img...`)
                const imgBuffer = await this.ArticleConverter.articleToImg(
                    this.hydrateArticleMediaForCard(article, maybe_media_files),
                    {
                        features: this.resolveCardFeatures(config.card_features),
                    },
                )
                const path = writeImgToFile(imgBuffer, `${taskId}-${article.a_id}-rendered.png`)
                this.log?.debug(`Generated rendered image at ${path}`)
                return path
            } catch (e) {
                this.log?.error(`Error while converting article to img: ${e}`)
                return null
            }
        }

        const appendRenderedCardToMedia = async (position: 'start' | 'end' = 'start') => {
            const renderedPath = await generateRenderedImage()
            if (!renderedPath) {
                return
            }

            const cardMedia = {
                path: renderedPath,
                media_type: 'photo' as MediaType,
                sourceArticleId: article.a_id,
                sourceUserId: article.u_id,
            }
            card_media_files.push(cardMedia)
            if (position === 'start') {
                maybe_media_files.unshift(cardMedia)
            } else {
                maybe_media_files.push(cardMedia)
            }
        }

        if (render_type === 'tag') {
            // Case 1: tag (was source)
            // Output: "From [Platform]"
            // Skip if no media
            if (maybe_media_files.length === 0) {
                this.log?.debug(`Skipping 'tag' mode for text-only article ${article.a_id}`)
                return {
                    text: '',
                    cardMediaFiles: [],
                    originalMediaFiles: [],
                    mediaFiles: [],
                    shouldSkipSend: Boolean(skipReason),
                    skipReason,
                }
            }
            text = this.formatPlatformFrom(article)
            textCollapseMode = 'none'
        } else if (render_type === 'img-tag' || render_type === 'img-tag-dynamic') {
            // Case 2 & 3: img-tag family
            // Check Exemption Logic
            const isVideoPlatform = [Platform.TikTok, Platform.YouTube].includes(article.platform)

            const isVideoType =
                article.media?.some((m) => m.type === 'video') ||
                article.media?.some((m) => m.type === 'video_thumbnail')

            // User requested Bilibili (future) and Video types NOT to be merged
            if (isVideoPlatform || isVideoType) {
                this.log?.info(
                    `Exemption triggered: Forcing text mode for Video/Platform ${article.platform} ${article.a_id}`,
                )
                // Fallback to standard text + media
                text = this.renderText(article, config)
                textCollapseMode = this.resolveArticleTextCollapseMode(render_type)
            } else {
                // Standard Card Logic
                text = this.formatPlatformFrom(article)
                textCollapseMode = 'none'
                await appendRenderedCardToMedia('start')
            }
        } else if (render_type?.startsWith('img')) {
            // Case 4: Other img-based types (e.g. 'img', 'img-with-meta')
            // Concept: Rendered Image (at start) + Metaline/Empty Text.

            // Check Exemption Logic
            const isVideoPlatform = [Platform.TikTok, Platform.YouTube].includes(article.platform)
            if (isVideoPlatform) {
                this.log?.info(
                    `Exemption triggered: Forcing text mode for Video/Platform ${article.platform} ${article.a_id} in img mode`,
                )
                text = this.renderText(article, config)
                textCollapseMode = this.resolveArticleTextCollapseMode(render_type)
            } else {
                let articleToImgSuccess = false
                const originalMediaCount = maybe_media_files.length
                await appendRenderedCardToMedia('start')
                if (maybe_media_files.length > originalMediaCount) {
                    articleToImgSuccess = true
                }

                const fullText = this.renderText(article, config)
                // If converted to image, usually only want the metaline
                text = articleToImgSuccess ? formatMetaline(article) : fullText
                textCollapseMode = articleToImgSuccess ? 'none' : this.resolveArticleTextCollapseMode(render_type)

                if (render_type === 'img') {
                    text = '' // No text for pure img mode
                    textCollapseMode = 'none'
                }
            }
        } else if (render_type === 'text-card' || render_type === 'text-compact-card') {
            text = this.renderText(article, config)
            textCollapseMode = this.resolveArticleTextCollapseMode(render_type)
            if (!LONG_TEXT_CARD_TYPES.has(String(article.type || '')) && text.length > CARD_TEXT_TITLE_THRESHOLD) {
                text = extractArticleHeadline(article)
                textCollapseMode = 'none'
            }
            await appendRenderedCardToMedia('end')
        } else if (render_type === 'text-compact') {
            text = this.renderText(article, config)
            textCollapseMode = 'compact-article'
        } else {
            // Case 5: Standard Text
            text = this.renderText(article, config)
            textCollapseMode = 'article'
        }

        if (!skipReason && deduplication) {
            const shortVideoCandidate = buildShortVideoDedupCandidate(article as any, maybe_media_files)
            if (shortVideoCandidate) {
                const existing = await checkShortVideoCrossPlatformDuplicate(shortVideoCandidate)
                if (existing) {
                    skipReason = `Cross-platform short video duplicate matched ${existing.a_id}`
                    this.log?.info(
                        `Skipping cross-platform short video duplicate for ${article.a_id} (${shortVideoCandidate.group}, ${shortVideoCandidate.duration_seconds.toFixed(2)}s).`,
                    )
                } else {
                    await markShortVideoCrossPlatformSeen(shortVideoCandidate)
                }
            }
        }

        return {
            text,
            textCollapseMode,
            cardMediaFiles: card_media_files,
            originalMediaFiles: maybe_media_files.filter(
                (item) => !card_media_files.some((card) => card.path === item.path),
            ),
            mediaFiles: maybe_media_files,
            shouldSkipSend: Boolean(skipReason),
            skipReason,
        }
    }

    /**
     * Clean up temporary files generated during processing
     */
    cleanup(mediaFiles: Array<{ path: string }>) {
        mediaFiles
            .map((i) => i.path)
            .forEach((path) => {
                try {
                    if (existsSync(path) && !isPersistentMediaPath(path)) {
                        unlinkSync(path)
                    }
                } catch (e) {
                    this.log?.error(`Error while unlinking file ${path}: ${e}`)
                }
            })
    }

    private formatPlatformFrom(article: Article): string {
        return formatPlatformTag(article, this.log)
    }

    renderText(
        article: Article,
        config: {
            render_type?: string
            collapsedArticleIds?: Set<string | number>
        } = {},
    ) {
        const textOptions: ArticleTextOptions = {
            collapsedArticleIds: config.collapsedArticleIds,
        }
        return config.render_type === 'text-compact' || config.render_type === 'text-compact-card'
            ? compactArticleToText(article, textOptions)
            : articleToText(article, textOptions)
    }

    private resolveCardFeatures(features?: Array<string>) {
        return Array.from(new Set(features || []))
    }

    private resolveArticleTextCollapseMode(renderType?: string): 'article' | 'compact-article' {
        return renderType === 'text-compact' || renderType === 'text-compact-card' ? 'compact-article' : 'article'
    }

    private hydrateArticleMediaForCard(article: Article, mediaFiles: Array<RenderedMediaFile>) {
        const cloned = cloneDeep(article)
        const hydrate = (currentArticle: Article | null) => {
            if (!currentArticle) {
                return
            }

            const candidateFiles = mediaFiles.filter((file) => {
                if (file.media_type !== 'photo' && file.media_type !== 'video_thumbnail') {
                    return false
                }
                return !file.sourceArticleId || file.sourceArticleId === currentArticle.a_id
            })
            const bySourceUrl = new Map(
                candidateFiles
                    .map((file) => [file.sourceUrl, file] as const)
                    .filter(([sourceUrl]) => Boolean(sourceUrl)),
            )
            let fallbackIndex = 0
            currentArticle.media = (currentArticle.media || []).map((mediaItem) => {
                if (mediaItem.type !== 'photo' && mediaItem.type !== 'video_thumbnail') {
                    return mediaItem
                }

                const file = bySourceUrl.get(mediaItem.url) || candidateFiles[fallbackIndex++]
                const dataUrl = file ? this.mediaFileToDataUrl(file.path) : null
                const dimensions = file ? this.mediaFileDimensions(file.path) : null
                return dataUrl
                    ? {
                          ...mediaItem,
                          url: dataUrl,
                          ...dimensions,
                      }
                    : mediaItem
            })
            this.hydrateInlineHtmlMedia(currentArticle, bySourceUrl)

            if (currentArticle.ref && typeof currentArticle.ref === 'object') {
                hydrate(currentArticle.ref as Article)
            }
        }

        hydrate(cloned)
        return cloned
    }

    private hydrateInlineHtmlMedia(article: Article, mediaBySourceUrl: Map<string | undefined, RenderedMediaFile>) {
        const rawHtml = (article.extra?.data as any)?.raw_html
        if (typeof rawHtml !== 'string' || !/<img\b/i.test(rawHtml)) {
            return
        }

        const nextHtml = rawHtml.replace(
            /(<img\b[^>]*\bsrc=)(["']?)([^"'\s>]+)(\2)/gi,
            (match, prefix: string, quote: string, src: string) => {
                const file =
                    mediaBySourceUrl.get(src) || mediaBySourceUrl.get(this.resolveCardMediaSourceUrl(src, article.url))
                const dataUrl = file ? this.mediaFileToDataUrl(file.path) : null
                if (!dataUrl) {
                    return match
                }
                const srcQuote = quote || '"'
                return `${prefix}${srcQuote}${dataUrl}${srcQuote}`
            },
        )

        if (nextHtml !== rawHtml && article.extra?.data) {
            article.extra.data = {
                ...(article.extra.data as any),
                raw_html: nextHtml,
            }
        }
    }

    private resolveCardMediaSourceUrl(src: string, baseUrl?: string | null) {
        try {
            return new URL(src, baseUrl || undefined).href
        } catch {
            return src
        }
    }

    private mediaFileToDataUrl(filePath: string) {
        try {
            const ext = path.extname(filePath).slice(1).toLowerCase()
            const mime = extToMime[ext as keyof typeof extToMime] || 'image/png'
            return `data:${mime};base64,${readFileSync(filePath).toString('base64')}`
        } catch (e) {
            this.log?.warn(`Failed to inline media for rendered card ${filePath}: ${e}`)
            return null
        }
    }

    private mediaFileDimensions(filePath: string): { width: number; height: number } | null {
        try {
            const buffer = readFileSync(filePath)
            return this.parsePngDimensions(buffer) || this.parseJpegDimensions(buffer)
        } catch {
            return null
        }
    }

    private parsePngDimensions(buffer: Buffer): { width: number; height: number } | null {
        if (buffer.length < 24 || buffer.subarray(0, 8).toString('hex') !== '89504e470d0a1a0a') {
            return null
        }
        return {
            width: buffer.readUInt32BE(16),
            height: buffer.readUInt32BE(20),
        }
    }

    private parseJpegDimensions(buffer: Buffer): { width: number; height: number } | null {
        if (buffer.length < 4 || buffer[0] !== 0xff || buffer[1] !== 0xd8) {
            return null
        }

        let offset = 2
        while (offset < buffer.length - 9) {
            if (buffer[offset] !== 0xff) {
                offset += 1
                continue
            }
            const marker = buffer[offset + 1]
            offset += 2
            if (marker === 0xd8 || marker === 0xd9 || (marker >= 0xd0 && marker <= 0xd7)) {
                continue
            }
            const length = buffer.readUInt16BE(offset)
            if (length < 2 || offset + length > buffer.length) {
                return null
            }
            if (
                (marker >= 0xc0 && marker <= 0xc3) ||
                (marker >= 0xc5 && marker <= 0xc7) ||
                (marker >= 0xc9 && marker <= 0xcb) ||
                (marker >= 0xcd && marker <= 0xcf)
            ) {
                return {
                    height: buffer.readUInt16BE(offset + 3),
                    width: buffer.readUInt16BE(offset + 5),
                }
            }
            offset += length
        }
        return null
    }

    private async handleMedia(
        taskId: string,
        article: Article,
        media: Media,
        deduplication?: boolean,
    ): Promise<{
        files: Array<RenderedMediaFile>
        skipReason?: string
    }> {
        let maybe_media_files = [] as Array<RenderedMediaFile>
        let currentArticle: Article | null = article
        let skipReason: string | undefined
        let processedMediaCount = 0
        let duplicateMediaCount = 0
        let duplicateMediaReason: string | undefined

        // Dynamic imports to avoid top-level issues during hot-reload or circular deps, and purely for this logic
        const DB = (await import('@/db')).default

        while (currentArticle) {
            let new_files = [] as Array<RenderedMediaFile | undefined>
            if (currentArticle.has_media) {
                this.log?.debug(`Downloading media files for ${currentArticle.a_id}`)
                let cookie: string | undefined = undefined
                if ([Platform.TikTok].includes(currentArticle.platform)) {
                    cookie = await tryGetCookie(currentArticle.url)
                }

                const finalizeDownloadedFile = async (path: string, sourceUrl?: string, preferredType?: MediaType) => {
                    const resolvedMediaType = preferredType || getMediaType(path)
                    const persisted = persistMediaFile(path, {
                        article: currentArticle as any,
                        media_type: resolvedMediaType,
                        source_url: sourceUrl,
                    })
                    processedMediaCount += 1
                    if (deduplication) {
                        try {
                            const platformStr = currentArticle?.platform ? String(currentArticle.platform) : '0'
                            const articleId = currentArticle?.a_id || ''
                            const articleMarker = buildArticleMarker(currentArticle as any)
                            const hash = persisted.hash

                            const exists = await DB.MediaHash.checkExist(platformStr, hash)
                            if (exists) {
                                if (articleId && exists.a_id === articleId) {
                                    this.log?.debug(
                                        `Media hash ${hash.substring(0, 8)} already recorded for article ${articleId}, keeping file for another formatter/target.`,
                                    )
                                } else {
                                    this.log?.info(
                                        `Duplicate media detected (Hash: ${hash.substring(0, 8)}...), skipping.`,
                                    )
                                    duplicateMediaCount += 1
                                    duplicateMediaReason = `Duplicate media hash matched ${exists.a_id || 'previous article'}`
                                    return undefined
                                }
                            } else {
                                await DB.MediaHash.save(platformStr, hash, articleId)
                            }

                            const exactMediaDuplicate = await checkExactCrossPlatformMediaDuplicate(
                                resolvedMediaType,
                                hash,
                                articleMarker,
                            )
                            if (exactMediaDuplicate) {
                                duplicateMediaCount += 1
                                const duplicateKind = resolvedMediaType === 'video' ? 'video' : 'media'
                                skipReason = `Cross-platform exact ${duplicateKind} duplicate matched ${exactMediaDuplicate.a_id}`
                                this.log?.info(
                                    `Skipping ${articleMarker} because ${duplicateKind} hash ${hash.substring(0, 8)} matches ${exactMediaDuplicate.a_id}.`,
                                )
                                return undefined
                            }
                            await markExactCrossPlatformMediaSeen(resolvedMediaType, hash, articleMarker)

                            if (resolvedMediaType === 'video') {
                                const videoFingerprintCandidate = buildVideoFingerprintCandidate(
                                    currentArticle as any,
                                    persisted,
                                )
                                if (videoFingerprintCandidate) {
                                    const fingerprintDuplicate =
                                        await checkVideoFingerprintDuplicate(videoFingerprintCandidate)
                                    if (fingerprintDuplicate) {
                                        duplicateMediaCount += 1
                                        skipReason = `Cross-platform video fingerprint duplicate matched ${fingerprintDuplicate.a_id}`
                                        this.log?.info(
                                            `Skipping ${articleMarker} because video fingerprint (${videoFingerprintCandidate.group}, ${videoFingerprintCandidate.duration_seconds.toFixed(2)}s) matches ${fingerprintDuplicate.a_id}.`,
                                        )
                                        return undefined
                                    }
                                    await markVideoFingerprintSeen(videoFingerprintCandidate)
                                }
                            }
                        } catch (e) {
                            this.log?.error(`Error during duplicate check: ${e}`)
                        }
                    }
                    const dimensions =
                        resolvedMediaType === 'photo' || resolvedMediaType === 'video_thumbnail'
                            ? this.mediaFileDimensions(persisted.path)
                            : null
                    return {
                        path: persisted.path,
                        media_type: resolvedMediaType,
                        sourceArticleId: currentArticle?.a_id || undefined,
                        sourceUserId: currentArticle?.u_id || undefined,
                        sourceUrl,
                        ...dimensions,
                        content_hash: persisted.hash,
                        size_bytes: persisted.size_bytes,
                        duration_seconds: persisted.duration_seconds,
                        persistent: true,
                    }
                }

                // Helper to download a list of media items
                const _handleMedia = async (
                    mediaList: Array<{ url: string; type: MediaType }>,
                    overrideType?: boolean,
                ) => {
                    return Promise.all(
                        mediaList.map(async ({ url, type }) => {
                            try {
                                const path = await plainDownloadMediaFile(url, taskId, {
                                    cookie: cookie || '',
                                    ...(currentArticle?.platform
                                        ? platformPresetHeadersMap[currentArticle.platform]
                                        : {}),
                                })

                                return finalizeDownloadedFile(path, url, overrideType ? undefined : type)
                            } catch (e) {
                                this.log?.error(`Error while downloading media file: ${e}, skipping ${url}`)
                            }
                            return undefined
                        }),
                    )
                }

                const getUniqueExtraMedia = () => {
                    if (!currentArticle?.extra?.media) {
                        return []
                    }

                    const seenUrls = new Set((currentArticle.media || []).map((item) => item.url))
                    return currentArticle.extra.media.filter((item) => {
                        if (!item?.url || seenUrls.has(item.url)) {
                            return false
                        }
                        seenUrls.add(item.url)
                        return true
                    })
                }

                // Tool: Default HTTP Downloader
                if (media.use.tool === MediaToolEnum.DEFAULT && currentArticle.media) {
                    this.log?.debug(`Downloading media with http downloader`)
                    new_files = await _handleMedia(currentArticle.media)

                    const uniqueExtraMedia = getUniqueExtraMedia()
                    if (uniqueExtraMedia.length > 0) {
                        const extra_files = await _handleMedia(uniqueExtraMedia, true)
                        new_files = new_files.concat(extra_files)
                    }
                }

                // Tool: Gallery-DL
                if (media.use.tool === MediaToolEnum.GALLERY_DL) {
                    this.log?.debug(`Downloading media with gallery-dl`)
                    // galleryDownloadMediaFile returns string[] (paths)
                    const paths = await galleryDownloadMediaFile(
                        currentArticle.url,
                        media.use as MediaTool<MediaToolEnum.GALLERY_DL>,
                    )

                    new_files = await Promise.all(
                        paths.map((path) => finalizeDownloadedFile(path, currentArticle?.url)),
                    )
                    new_files = new_files.filter((f) => f !== undefined)

                    const uniqueExtraMedia = getUniqueExtraMedia()
                    if (uniqueExtraMedia.length > 0) {
                        const extra_files = await _handleMedia(uniqueExtraMedia, true)
                        new_files = new_files.concat(extra_files)
                    }
                }

                // Tool: yt-dlp
                if (media.use.tool === MediaToolEnum.YT_DLP) {
                    this.log?.debug(`Downloading media with yt-dlp`)

                    if (currentArticle.media) {
                        new_files = await _handleMedia(currentArticle.media)
                    }

                    const videoPaths = await ytDlpDownloadMediaFile(
                        currentArticle.url,
                        media.use as MediaTool<MediaToolEnum.YT_DLP>,
                        `${taskId}-${currentArticle.a_id}`,
                    )
                    const videoFiles = await Promise.all(
                        videoPaths.map((path) => finalizeDownloadedFile(path, currentArticle?.url)),
                    )
                    new_files = new_files.concat(videoFiles)

                    const uniqueExtraMedia = getUniqueExtraMedia()
                    if (uniqueExtraMedia.length > 0) {
                        const extra_files = await _handleMedia(uniqueExtraMedia, true)
                        new_files = new_files.concat(extra_files)
                    }
                }

                if (new_files.length > 0) {
                    // Filter defined
                    const validFiles = new_files.filter((i): i is RenderedMediaFile => i !== undefined)
                    this.log?.debug(`Downloaded media files: ${validFiles.map((f) => f.path).join(', ')}`)
                    maybe_media_files = maybe_media_files.concat(validFiles)
                }
            }

            // Traverse references
            if (currentArticle.ref && typeof currentArticle.ref === 'object') {
                currentArticle = currentArticle.ref as Article
            } else {
                currentArticle = null
            }
        }
        if (!skipReason && deduplication && processedMediaCount > 0 && duplicateMediaCount === processedMediaCount) {
            skipReason = duplicateMediaReason || 'All downloaded media were duplicates'
        }
        return {
            files: maybe_media_files,
            skipReason,
        }
    }
}

export { formatPlatformTag }
