
import { ForwarderPools } from '../managers/forwarder-manager'
import { Platform } from '@idol-bbq-utils/spider/types'
import { Logger } from '@idol-bbq-utils/log'
import type { Article } from '@/db'
import dayjs from 'dayjs'
import { X } from '@idol-bbq-utils/spider'
import { MediaToolEnum } from '@/types/media'
import EventEmitter from 'events'

export async function runDebugPush(targetGroup: string, log: Logger) {
    log.info(`Starting Debug Push to target: ${targetGroup}`)
}

export async function runDebugPushWithPools(forwarderPools: ForwarderPools, targetGroup: string, log: Logger) {
    log.info(`Starting Debug Push to target: ${targetGroup} using existing pools`)

    // 1. Fake X Article (Text Only) - Use correct Enum
    const xArticleText: Article = {
        id: 1001,
        platform: Platform.X,
        a_id: 'fake-x-text-1',
        u_id: 'debug_user',
        username: 'Debug User',
        created_at: dayjs().unix(),
        content: 'This is a debug text tweet from X.',
        translation: '',
        translated_by: '',
        url: 'https://twitter.com/debug_user/status/1',
        type: X.ArticleTypeEnum.TWEET,
        ref: null,
        has_media: false,
        media: [],
        extra: { data: X.ExtraContentType.TWEET },
        u_avatar: 'https://abs.twimg.com/sticky/default_profile_images/default_profile_400x400.png'
    }

    // 2. Fake X Article (Image - Should Card)
    const xArticleImg: Article = {
        ...xArticleText,
        id: 1002,
        a_id: 'fake-x-img-1',
        content: 'This is a debug tweet with image.',
        has_media: true,
        media: [{
            type: 'photo',
            url: 'https://picsum.photos/200/300',
            alt: 'Random Image'
        }]
    }

    // 3. Fake Video Article (YouTube - Should Exempt/Text Only)
    const ytArticle: any = {
        ...xArticleText,
        id: 1003,
        platform: Platform.YouTube,
        a_id: 'fake-yt-1',
        content: 'New Video Uploaded!',
        type: 'post', // Generic fallback
        has_media: false,
        media: [{
            type: 'video_thumbnail',
            url: 'https://picsum.photos/300/200',
            alt: 'Thumbnail'
        }]
    }

    const { RenderService } = await import('../services/render-service')
    const renderService = new RenderService(log)

    const send = async (article: Article, tag: string) => {
        log.info(`Processing ${tag}...`)

        // Mock Media Config
        const mockMediaConfig: any = {
            type: 'no-storage',
            use: { tool: MediaToolEnum.DEFAULT }
        }

        const result = await renderService.process(article, {
            taskId: 'debug-task',
            render_type: 'img-tag',
            mediaConfig: mockMediaConfig
        })

        log.info(`Processed Result for ${tag}:`)
        log.info(`Text Preview: ${result.text.substring(0, 50)}...`)
        log.info(`Media Files: ${result.mediaFiles.length}`)
        result.mediaFiles.forEach(f => log.info(` - ${f.path}`))

        // Access private forward_to map using cast
        const poolsMap = (forwarderPools as any).forward_to as Map<string, any>

        let sent = false
        for (const [botId, forwarder] of poolsMap.entries()) {
            if (forwarder.type === 'qq') {
                log.info(`Attempting to send via bot ${botId} to group ${targetGroup}`)
                try {
                    const client = forwarder.client
                    if (client && client.sendGroupMsg) {
                        const mediaElements = result.mediaFiles.map(f => {
                            return {
                                type: 'image',
                                file: f.path
                            }
                        })
                        const message = [
                            result.text,
                            ...mediaElements
                        ]
                        await client.sendGroupMsg(Number(targetGroup), message)
                        log.info(`Sent successfully to ${targetGroup}.`)
                        sent = true
                        break;
                    } else {
                        log.warn(`Bot ${botId} client does not have sendGroupMsg or is not exposed.`)
                    }
                } catch (e) {
                    log.error(`Failed to send via bot ${botId}: ${e}`)
                }
            }
        }
        if (!sent) {
            log.warn('Could not find suitable QQ bot or failed to send.')
        }
    }

    try {
        await send(xArticleText, 'X Text')
        await send(xArticleImg, 'X Image')
        await send(ytArticle, 'YouTube Video')
    } catch (e) {
        log.error(`Error in debug loop: ${e}`)
    }
}
