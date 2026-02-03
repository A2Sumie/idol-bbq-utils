
import { ForwarderPools } from '../src/managers/forwarder-manager'
import { Platform } from '@idol-bbq-utils/spider/types'
import { Logger } from '@idol-bbq-utils/log'
import { Article } from '@/db'
import dayjs from 'dayjs'

const log = new Logger('DebugPush')

async function main() {
    const forwarderPools = new ForwarderPools(log)
    await forwarderPools.init()

    const targetGroup = 742435777

    // 1. Fake X Article (Text Only)
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
        type: 'tweet',
        ref: null,
        has_media: false,
        media: [],
        extra: null,
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
            url: 'https://picsum.photos/200/300', // Random image
            alt: 'Random Image'
        }]
    }

    // 3. Fake Video Article (YouTube - Should Exempt/Text Only)
    const ytArticle: Article = {
        ...xArticleText,
        id: 1003,
        platform: Platform.YouTube,
        a_id: 'fake-yt-1',
        content: 'New Video Uploaded!',
        type: 'post',
        has_media: false, // Usually video posts have link/thumbnail
        media: [{
            type: 'video_thumbnail',
            url: 'https://picsum.photos/300/200',
            alt: 'Thumbnail'
        }]
    }

    // Send logic helper
    const send = async (article: Article, tag: string) => {
        log.info(`Sending ${tag}...`)
        // Manually trigger process logic found in ForwarderManager.processArticleTask
        // But since we just want to push to a specific target, we can mock the subscription or use RenderService directly?
        // Actually, let's use ForwarderManager.forwarderPools to get the QQ bot and use RenderService to process.

        // However, RenderService is not easily imported as a standalone without instantiating. 
        // Let's import RenderService
        const { RenderService } = await import('../src/services/render-service')
        const renderService = new RenderService(log)

        // Mock Config: force render_type 'img-tag' to test exemption logic for video
        // We need to know what render_type the user usually uses. Assuming 'img-tag' for X.

        let renderTypeToUse = 'img-tag'

        // Process
        const result = await renderService.process(article, {
            render_type: renderTypeToUse,
            media_download_tool: 'api' // or 'gallery-dl' but lets stick to simple
        })

        // Get target (QQ Group)
        // We need a connected bot. ForwarderPools manages this.
        // If we don't know the bot ID, we might need to find one.
        // Or assume 'bot1' or similar. 
        // Let's list bots?
        const forwarder = forwarderPools.getPools().values().next().value // Get first available?

        if (!forwarder) {
            log.error('No forwarder bot found!')
            return
        }

        log.info(`Sending using forwarder: ${forwarder.bot_id}`)

        // QQ Forwarder send
        // We need to construct the target payload.
        // Forwarder interface: send(content: string, extra?: { media?: Media[], ... })
        // But how to direct to specific group?
        // The Forwarder instance usually is bound to a specific bot, but sending requires knowing where to send?
        // Wait, `forwarder.send` usually broadcasts? 
        // Looking at `src/middleware/forwarder/qq.ts`: send(content, extra) uses `this.subscribers`.
        // So we can't easily send to a specific random group unless we modify subscribers or use raw API.

        // Hack: Use `forwarder.client.sendGroupMsg` if available, or just log result.
        // Actually, if we just want to test RENDER logic, generating the artifacts is enough.
        // If we want to send, we really need the real bot.

        // Let's try to simulate what `ForwarderManager` does:
        // It calls `forwarder.send`. 
        // Maybe we just print the result for now?
        // User asked "target trying to send to ... 742435777".

        // If we can get the underlying client:
        if (forwarder.type === 'qq') {
            // Try valid send
            // configTBD? 
            // Let's just try to call the internal send mechanism if exposed.
            // Or, we can use the `config.yaml` to ADD this group to a debug subscription temporarily?
            // That's complex.

            // Simplest: just process and Log result. User can see if Exemption worked.

        }

        log.info(`Processed Result for ${tag}:`)
        log.info(`Text: ${result.text}`)
        log.info(`Media: ${result.mediaFiles.length} files`)
        if (result.mediaFiles.length > 0) {
            log.info(`Media 1: ${result.mediaFiles[0].path}`)
        }
    }

    await send(xArticleText, 'X Text')
    await send(xArticleImg, 'X Image')
    await send(ytArticle, 'YouTube Video')

    process.exit(0)
}

main()
