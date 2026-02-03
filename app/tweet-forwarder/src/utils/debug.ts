
import { ForwarderPools } from '../managers/forwarder-manager'
import { Platform } from '@idol-bbq-utils/spider/types'
import { Logger } from '@idol-bbq-utils/log'
import type { Article } from '@/db'
import dayjs from 'dayjs'
import { MediaToolEnum } from '@/types/media'

export async function runDebugPush(targetGroup: string, log: Logger) {
    // Unused
}

export async function runDebugPushWithPools(forwarderPools: ForwarderPools, targetIdentifier: string, log: Logger) {
    log.info(`Starting Debug Push. Looking for target matching: ${targetIdentifier}`)

    // Access private forward_to map using cast
    const poolsMap = (forwarderPools as any).forward_to as Map<string, any>
    let targetForwarder: any = null

    // 1. Try match by ID (exact match)
    if (poolsMap.has(targetIdentifier)) {
        targetForwarder = poolsMap.get(targetIdentifier)
        log.info(`Found target by ID: ${targetIdentifier}`)
    } else {
        // 2. Try match by group_id in config
        for (const [id, forwarder] of poolsMap.entries()) {
            if (forwarder.cfg_platform?.group_id === targetIdentifier || forwarder.cfg_platform?.group_id === Number(targetIdentifier)) {
                targetForwarder = forwarder
                log.info(`Found target by Group ID: ${targetIdentifier} -> ${id}`)
                break
            }
        }
    }

    if (!targetForwarder) {
        log.error(`Target ${targetIdentifier} NOT found in loaded pools! Available IDs: ${Array.from(poolsMap.keys()).join(', ')}`)
        return
    }

    // 1. Fake X Article (Text Only)
    const xArticleText: Article = {
        id: 1001,
        platform: Platform.X,
        a_id: 'fake-x-text-1',
        u_id: 'debug_user',
        username: 'Debug User',
        created_at: dayjs().unix(),
        content: '[Debug] Text only tweet (Should be just text)',
        translation: '',
        translated_by: '',
        url: 'https://twitter.com/debug_user/status/1',
        type: 'tweet' as any,
        ref: null,
        has_media: false,
        media: [],
        extra: { data: 'tweet' as any },
        u_avatar: 'https://abs.twimg.com/sticky/default_profile_images/default_profile_400x400.png'
    }

    // 2. Fake X Article (Image - Should Card)
    const xArticleImg: Article = {
        ...xArticleText,
        id: 1002,
        a_id: 'fake-x-img-1',
        content: '[Debug] Image tweet (Should generate Card)',
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
        content: '[Debug] YouTube Video (Should be EXEMPTED -> Text Only + Link)',
        type: 'post',
        has_media: false,
        media: [{
            type: 'video_thumbnail',
            url: 'https://picsum.photos/300/200',
            alt: 'Thumbnail'
        }]
    }

    // 4. Fake TikTok (Video Type - Should be Exempted)
    const tiktokArticle: any = {
        ...xArticleText,
        id: 1004,
        platform: Platform.TikTok,
        a_id: 'fake-tk-1',
        content: '[Debug] TikTok Video (Should be EXEMPTED -> Text Only)',
        type: 'video',
        has_media: true,
        media: [{
            type: 'video',
            url: 'http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4', // public sample
            alt: 'Video'
        }]
    }


    const { RenderService } = await import('../services/render-service')
    const renderService = new RenderService(log)

    const send = async (article: Article, tag: string) => {
        log.info(`--- Processing ${tag} ---`)

        // Mock Media Config
        const mockMediaConfig: any = {
            type: 'no-storage',
            use: { tool: MediaToolEnum.DEFAULT }
        }

        // We assume 'img-tag' is what the user uses for X/General
        const result = await renderService.process(article, {
            taskId: 'debug-task',
            render_type: 'img-tag',
            mediaConfig: mockMediaConfig
        })

        log.info(`Render Result: Text length=${result.text.length}, MediaFiles=${result.mediaFiles.length}`)
        if (result.mediaFiles.length > 0) {
            log.info(`Media: ${result.mediaFiles.map(f => f.path).join(', ')}`)
        }

        try {
            log.info(`Sending to ${targetForwarder.id}...`)
            // Use the Forwarder's standard send method
            await targetForwarder.send(result.text, {
                media: result.mediaFiles,
                timestamp: article.created_at,
                runtime_config: {}, // empty runtime config
                article: article
            })
            log.info(`Successfully sent ${tag}`)
        } catch (e) {
            log.error(`Failed to send ${tag}: ${e}`)
        }
    }

    try {
        await send(xArticleText, 'X (Text)')
        await send(xArticleImg, 'X (Image)')
        await send(ytArticle, 'YouTube (Video)')
        await send(tiktokArticle, 'TikTok (Video)')
    } catch (e) {
        log.error(`Error in debug loop: ${e}`)
    }
}
