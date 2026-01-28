import { Logger } from '@idol-bbq-utils/log'
import { Platform } from '@idol-bbq-utils/spider/types'
import type { Article } from '@/db'
import { type Media, type MediaTool, MediaToolEnum } from '@/types/media'
import {
    galleryDownloadMediaFile,
    getMediaType,
    plainDownloadMediaFile,
    tryGetCookie,
    writeImgToFile,
} from '@/middleware/media'
import { articleToText, formatMetaline, ImgConverter } from '@idol-bbq-utils/render'
import { existsSync, unlinkSync } from 'fs'
import { cloneDeep } from 'lodash'
import { platformPresetHeadersMap } from '@idol-bbq-utils/spider/const'
import type { MediaType } from '@idol-bbq-utils/spider/types'

export interface RenderResult {
    text: string
    mediaFiles: Array<{
        path: string
        media_type: MediaType
    }>
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
        }
    ): Promise<RenderResult> {
        const { taskId, render_type, mediaConfig } = config
        const cloned_article = cloneDeep(article)

        let maybe_media_files: Array<{ path: string; media_type: MediaType }> = []

        // 1. Download/Handle Media Files
        if (mediaConfig) {
            maybe_media_files = await this.handleMedia(taskId, cloned_article, mediaConfig)
        }

        let text = ''

        // Helper: Generate Rendered Image
        const generateRenderedImage = async () => {
            try {
                this.log?.debug(`Converting article ${article.a_id} to img...`)
                const imgBuffer = await this.ArticleConverter.articleToImg(cloneDeep(article))
                return writeImgToFile(imgBuffer, `${taskId}-${article.a_id}-rendered.png`)
            } catch (e) {
                this.log?.error(`Error while converting article to img: ${e}`)
                return null
            }
        }

        if (render_type === 'img-with-source') {
            // Case 1: img+source
            // Concept: Source (Platform Tag) + Original Media. NO Rendered Image.
            text = this.extractPlatformLabel(article)
        } else if (render_type === 'img-with-source-summary') {
            // Case 2: img+source w/ summary
            // Concept: Source (Platform Tag) + Original Media + Rendered Image (at end).
            text = this.extractPlatformLabel(article)
            const renderedPath = await generateRenderedImage()
            if (renderedPath) {
                maybe_media_files.push({
                    path: renderedPath,
                    media_type: 'photo' as MediaType,
                })
            }
        } else if (render_type?.startsWith('img')) {
            // Case 3: Other img-based types (e.g. 'img')
            // Concept: Rendered Image (at start) + Metaline/Empty Text.
            const renderedPath = await generateRenderedImage()
            let articleToImgSuccess = false
            if (renderedPath) {
                maybe_media_files.unshift({
                    path: renderedPath,
                    media_type: 'photo' as MediaType,
                })
                articleToImgSuccess = true
            }

            const fullText = articleToText(article)
            // If converted to image, usually only want the metaline
            text = articleToImgSuccess ? formatMetaline(article) : fullText

            if (render_type === 'img') {
                text = '' // No text for pure img mode
            }
        } else {
            // Case 4: Standard Text
            text = articleToText(article)
        }

        return {
            text,
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

    private extractPlatformLabel(article: Article): string {
        const platformName = Platform[article.platform]?.toLowerCase()
        const platformMap: Record<string, string> = {
            'x': 'X',
            'twitter': 'X',
            'tiktok': 'TikTok',
            'instagram': 'Instagram',
            'youtube': 'YouTube',
            'bilibili': 'Bilibili'
        }
        return platformMap[platformName || ''] || Platform[article.platform] || 'Unknown'
    }

    private async handleMedia(
        taskId: string,
        article: Article,
        media: Media,
    ): Promise<Array<{ path: string; media_type: MediaType }>> {
        let maybe_media_files = [] as Array<{
            path: string
            media_type: MediaType
        }>
        let currentArticle: Article | null = article

        while (currentArticle) {
            let new_files = [] as Array<
                | {
                    path: string
                    media_type: MediaType
                }
                | undefined
            >
            if (currentArticle.has_media) {
                this.log?.debug(`Downloading media files for ${currentArticle.a_id}`)
                let cookie: string | undefined = undefined
                if ([Platform.TikTok].includes(currentArticle.platform)) {
                    cookie = await tryGetCookie(currentArticle.url)
                }

                // Helper to download a list of media items
                const _handleMedia = async (mediaList: Array<{ url: string; type: MediaType }>, overrideType?: boolean) => {
                    return Promise.all(
                        mediaList.map(async ({ url, type }) => {
                            try {
                                const path = await plainDownloadMediaFile(url, taskId, {
                                    cookie: cookie || '',
                                    ...(currentArticle?.platform
                                        ? platformPresetHeadersMap[currentArticle.platform]
                                        : {}),
                                })
                                return {
                                    path,
                                    media_type: overrideType ? getMediaType(path) : type,
                                }
                            } catch (e) {
                                this.log?.error(`Error while downloading media file: ${e}, skipping ${url}`)
                            }
                            return undefined
                        }),
                    )
                }

                // Tool: Default HTTP Downloader
                if (media.use.tool === MediaToolEnum.DEFAULT && currentArticle.media) {
                    this.log?.debug(`Downloading media with http downloader`)
                    new_files = await _handleMedia(currentArticle.media)

                    if (currentArticle.extra?.media) {
                        const extra_files = await _handleMedia(currentArticle.extra.media, true)
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

                    new_files = paths.map((path) => ({
                        path,
                        media_type: getMediaType(path),
                    }))

                    if (currentArticle.extra?.media) {
                        const extra_files = await _handleMedia(currentArticle.extra.media, true)
                        new_files = new_files.concat(extra_files)
                    }
                }

                if (new_files.length > 0) {
                    // Filter defined
                    const validFiles = new_files.filter((i): i is { path: string; media_type: MediaType } => i !== undefined)
                    this.log?.debug(`Downloaded media files: ${validFiles.map(f => f.path).join(', ')}`)
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
