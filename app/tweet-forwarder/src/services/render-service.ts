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
} from '@/middleware/media'
import { articleToText, compactArticleToText, formatMetaline, ImgConverter } from '@idol-bbq-utils/render'
import { existsSync, unlinkSync } from 'fs'
import { cloneDeep } from 'lodash'
import { platformPresetHeadersMap, platformNameMap } from '@idol-bbq-utils/spider/const'
import type { MediaType } from '@idol-bbq-utils/spider/types'

export interface RenderedMediaFile {
    path: string
    media_type: MediaType
    sourceArticleId?: string
    sourceUserId?: string
}

export interface RenderResult {
    text: string
    cardMediaFiles: Array<RenderedMediaFile>
    originalMediaFiles: Array<RenderedMediaFile>
    mediaFiles: Array<RenderedMediaFile>
}

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
            mediaConfig?: Media
            deduplication?: boolean
        },
    ): Promise<RenderResult> {
        const { taskId, render_type, mediaConfig, deduplication } = config
        const cloned_article = cloneDeep(article)

        let maybe_media_files: Array<RenderedMediaFile> = []
        let card_media_files: Array<RenderedMediaFile> = []

        // 1. Download/Handle Media Files
        if (mediaConfig) {
            maybe_media_files = await this.handleMedia(taskId, cloned_article, mediaConfig, deduplication)
        }

        let text = ''

        // Helper: Generate Rendered Image
        const generateRenderedImage = async () => {
            try {
                this.log?.debug(`Converting article ${article.a_id} to img...`)
                const imgBuffer = await this.ArticleConverter.articleToImg(cloneDeep(article))
                const path = writeImgToFile(imgBuffer, `${taskId}-${article.a_id}-rendered.png`)
                this.log?.debug(`Generated rendered image at ${path}`)
                return path
            } catch (e) {
                this.log?.error(`Error while converting article to img: ${e}`)
                return null
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
                }
            }
            text = this.formatPlatformFrom(article)
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
                text = articleToText(article)
            } else {
                // Standard Card Logic
                text = this.formatPlatformFrom(article)
                const renderedPath = await generateRenderedImage()
                if (renderedPath) {
                    const cardMedia = {
                        path: renderedPath,
                        media_type: 'photo' as MediaType,
                        sourceArticleId: article.a_id,
                        sourceUserId: article.u_id,
                    }
                    card_media_files.push(cardMedia)
                    maybe_media_files.unshift(cardMedia)
                }
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
                text = articleToText(article)
            } else {
                const renderedPath = await generateRenderedImage()
                let articleToImgSuccess = false
                if (renderedPath) {
                    const cardMedia = {
                        path: renderedPath,
                        media_type: 'photo' as MediaType,
                        sourceArticleId: article.a_id,
                        sourceUserId: article.u_id,
                    }
                    card_media_files.push(cardMedia)
                    maybe_media_files.unshift(cardMedia)
                    articleToImgSuccess = true
                }

                const fullText = articleToText(article)
                // If converted to image, usually only want the metaline
                text = articleToImgSuccess ? formatMetaline(article) : fullText

                if (render_type === 'img') {
                    text = '' // No text for pure img mode
                }
            }
        } else if (render_type === 'text-compact') {
            text = compactArticleToText(article)
        } else {
            // Case 5: Standard Text
            text = articleToText(article)
        }

        return {
            text,
            cardMediaFiles: card_media_files,
            originalMediaFiles: maybe_media_files.filter(
                (item) => !card_media_files.some((card) => card.path === item.path),
            ),
            mediaFiles: maybe_media_files,
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
                    if (existsSync(path)) {
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

    private async handleMedia(
        taskId: string,
        article: Article,
        media: Media,
        deduplication?: boolean,
    ): Promise<Array<RenderedMediaFile>> {
        let maybe_media_files = [] as Array<RenderedMediaFile>
        let currentArticle: Article | null = article

        // Dynamic imports to avoid top-level issues during hot-reload or circular deps, and purely for this logic
        const fs = await import('fs')
        const crypto = await import('crypto')
        const DB = (await import('@/db')).default

        while (currentArticle) {
            let new_files = [] as Array<RenderedMediaFile | undefined>
            if (currentArticle.has_media) {
                this.log?.debug(`Downloading media files for ${currentArticle.a_id}`)
                let cookie: string | undefined = undefined
                if ([Platform.TikTok].includes(currentArticle.platform)) {
                    cookie = await tryGetCookie(currentArticle.url)
                }

                const finalizeDownloadedFile = async (path: string, preferredType?: MediaType) => {
                    if (deduplication) {
                        try {
                            const buffer = fs.readFileSync(path)
                            const hash = crypto.createHash('sha256').update(buffer).digest('hex')
                            const platformStr = currentArticle?.platform ? String(currentArticle.platform) : '0'
                            const articleId = currentArticle?.a_id || ''

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
                                    fs.unlinkSync(path)
                                    return undefined
                                }
                            } else {
                                await DB.MediaHash.save(platformStr, hash, articleId)
                            }
                        } catch (e) {
                            this.log?.error(`Error during duplicate check: ${e}`)
                        }
                    }
                    return {
                        path,
                        media_type: preferredType || getMediaType(path),
                        sourceArticleId: currentArticle?.a_id || undefined,
                        sourceUserId: currentArticle?.u_id || undefined,
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

                                return finalizeDownloadedFile(path, overrideType ? undefined : type)
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

                    new_files = await Promise.all(paths.map((path) => finalizeDownloadedFile(path)))
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
                    const videoFiles = await Promise.all(videoPaths.map((path) => finalizeDownloadedFile(path)))
                    new_files = new_files.concat(videoFiles)

                    const uniqueExtraMedia = getUniqueExtraMedia()
                    if (uniqueExtraMedia.length > 0) {
                        const extra_files = await _handleMedia(uniqueExtraMedia, true)
                        new_files = new_files.concat(extra_files)
                    }
                }

                if (new_files.length > 0) {
                    // Filter defined
                    const validFiles = new_files.filter(
                        (i): i is RenderedMediaFile => i !== undefined,
                    )
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
        return maybe_media_files
    }
}

export { formatPlatformTag }
