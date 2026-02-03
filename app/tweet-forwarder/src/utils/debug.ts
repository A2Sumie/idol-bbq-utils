
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
    log.info(`\n=== STARING ADVANCED DEBUG SUITE ===\nTarget: ${targetIdentifier}`)

    // 1. Resolve Target
    const poolsMap = (forwarderPools as any).forward_to as Map<string, any>
    let targetForwarder: any = null

    if (poolsMap.has(targetIdentifier)) {
        targetForwarder = poolsMap.get(targetIdentifier)
    } else {
        for (const [id, forwarder] of poolsMap.entries()) {
            if (forwarder.cfg_platform?.group_id === targetIdentifier || forwarder.cfg_platform?.group_id === Number(targetIdentifier)) {
                targetForwarder = forwarder
                break
            }
        }
    }

    if (!targetForwarder) {
        log.error(`Target ${targetIdentifier} NOT found! Available: ${Array.from(poolsMap.keys()).join(', ')}`)
        return
    }
    log.info(`Target Resolved: ${targetForwarder.id}`)

    // 2. Define Test Cases
    const baseArticle: Article = {
        id: 0,
        platform: Platform.X,
        a_id: '',
        u_id: 'debug_user',
        username: 'Debug User',
        created_at: dayjs().unix(),
        content: '',
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

    const testCases: Array<{ name: string; article: Article; formatters: string[] }> = [
        // Case A: Pure Text
        {
            name: 'X (Text Only)',
            article: {
                ...baseArticle,
                id: 2001,
                a_id: 'case-text-only',
                content: 'Case A: This is a pure text tweet.',
                created_at: dayjs().subtract(10, 'minute').unix()
            },
            formatters: ['text', 'img-tag']
        },
        // Case B: Text + Image
        {
            name: 'X (Image)',
            article: {
                ...baseArticle,
                id: 2002,
                a_id: 'case-image',
                content: 'Case B: Ensure proper image card generation.',
                has_media: true,
                media: [{ type: 'photo', url: 'https://picsum.photos/200/300', alt: 'Img 1' }]
            },
            formatters: ['img-tag']
        },
        // Case C: Mixed Media (Image + Video) 
        // Logic Check: Does video presence force text-only?
        {
            name: 'X (Mixed: Img + Vid)',
            article: {
                ...baseArticle,
                id: 2003,
                a_id: 'case-mixed',
                content: 'Case C: Mixed Media. Should this trigger exemption?',
                has_media: true,
                media: [
                    { type: 'photo', url: 'https://picsum.photos/300/300', alt: 'Img 2' },
                    { type: 'video', url: 'http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4', alt: 'Vid 1' }
                ]
            },
            formatters: ['img-tag']
        },
        // Case D: Retweet (Nested Article)
        {
            name: 'X (Retweet)',
            article: {
                ...baseArticle,
                id: 2004,
                a_id: 'case-retweet',
                content: 'RT @original: This is a retweet body.',
                type: 'retweet' as any,
                ref: {
                    ...baseArticle,
                    id: 20040,
                    a_id: 'case-retweet-original',
                    username: 'Original User',
                    content: 'This is the original tweet content.',
                } as any
            },
            formatters: ['text', 'img-tag']
        },
        // Case E: Duplicate Media/Merging Test
        // Sending 2 articles with SAME images close in time
        {
            name: 'X (Duplicate Sim 1)',
            article: {
                ...baseArticle,
                id: 2005,
                a_id: 'case-dup-1',
                content: 'Case E1: Tweet 1 with Image A.',
                has_media: true,
                media: [{ type: 'photo', url: 'https://picsum.photos/200/200', alt: 'Shared Img' }]
            },
            formatters: ['img-tag']
        },
        {
            name: 'X (Duplicate Sim 2)',
            article: {
                ...baseArticle,
                id: 2006,
                a_id: 'case-dup-2',
                content: 'Case E2: Tweet 2 with SAME Image A (Check duplication).',
                created_at: dayjs().unix(), // Now
                has_media: true,
                media: [{ type: 'photo', url: 'https://picsum.photos/200/200', alt: 'Shared Img' }]
            },
            formatters: ['img-tag']
        }
    ]

    const { RenderService } = await import('../services/render-service')
    const renderService = new RenderService(log)

    // 3. Execution Loop
    for (const test of testCases) {
        log.info(`\n>>> TEST SCENARIO: ${test.name} <<<`)
        for (const fmt of test.formatters) {
            log.info(`   Formatter: [${fmt}]`)

            // Mock Media Config
            const mockMediaConfig: any = { type: 'no-storage', use: { tool: MediaToolEnum.DEFAULT } }

            try {
                // RENDER
                const result = await renderService.process(test.article, {
                    taskId: `debug-${test.article.a_id}-${fmt}`,
                    render_type: fmt,
                    mediaConfig: mockMediaConfig
                })

                log.info(`   [Render] Text Len: ${result.text.length} | Media Files: ${result.mediaFiles.length}`)
                result.mediaFiles.forEach(f => log.info(`       - ${f.path} (${f.media_type})`))

                // SEND
                // Note: We use the Forwarder's send method which might handle some wrapping
                // But ForwarderManager usually checks DB for duplicates. Here we bypass DB check by calling .send directly.
                // This confirms "Can we send it?" but not "Does Manager filter it?"
                // To test Manager logic properly we'd need to mock DB, which is hard here.
                // We rely on visual verification in QQ.

                log.info(`   [Sending]...`)
                await targetForwarder.send(result.text, {
                    media: result.mediaFiles,
                    timestamp: test.article.created_at,
                    runtime_config: {}, // Defaults
                    article: test.article
                })
                log.info(`   [Sent] OK`)

                // Small delay to ensure order
                await new Promise(r => setTimeout(r, 1000))

            } catch (e) {
                log.error(`   [Error] ${e}`)
            }
        }
    }

    log.info(`\n=== DEBUG SUITE COMPLETE ===`)
}
