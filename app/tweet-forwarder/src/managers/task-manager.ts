import { Logger } from '@idol-bbq-utils/log'
import { BaseCompatibleModel } from '@/utils/base'
import DB from '@/db'
import { processorRegistry } from '@/middleware/processor'
import { ForwarderPools } from '@/managers/forwarder-manager'
import { Platform } from '@idol-bbq-utils/spider/types'
import { CronJob } from 'cron'
import dayjs from 'dayjs'
import type { ProcessorConfig } from '@/types/processor'

interface AggregatePayload {
    platform: Platform
    u_id: string
    start: number
    end: number
    bot_id: string
    processorConfig?: ProcessorConfig & { provider: string; api_key?: string }
    prompt?: string
}

export class TaskManager extends BaseCompatibleModel {
    NAME = 'TaskManager'
    log?: Logger
    private forwarderPools: ForwarderPools
    private pollingJob: CronJob

    constructor(forwarderPools: ForwarderPools, log?: Logger) {
        super()
        this.forwarderPools = forwarderPools
        this.log = log?.child({ subservice: this.NAME })
        // Poll every minute
        this.pollingJob = new CronJob('*/1 * * * *', this.poll.bind(this))
    }

    async init() {
        this.log?.info('TaskManager initialized')
        this.pollingJob.start()
    }

    async stop() {
        this.pollingJob.stop()
    }

    async drop() {
        await this.stop()
    }

    private async poll() {
        const now = Math.floor(Date.now() / 1000)
        try {
            const tasks = await DB.TaskQueue.getPending(now)
            if (tasks.length > 0) {
                this.log?.info(`Found ${tasks.length} pending tasks`)
            }
            for (const task of tasks) {
                await DB.TaskQueue.updateStatus(task.id, 'processing')
                try {
                    if (task.type === 'aggregate_daily') {
                        // Cast payload safely
                        const payload = task.payload as unknown as AggregatePayload
                        await this.handleDailyAggregation(payload)
                    } else if (task.type === 'aggregate_hourly') {
                        const payload = task.payload as unknown as AggregatePayload
                        await this.handleHourlyAggregation(payload)
                    }
                    await DB.TaskQueue.updateStatus(task.id, 'completed')
                } catch (e) {
                    this.log?.error(`Task ${task.id} failed: ${e}`)
                    await DB.TaskQueue.updateStatus(task.id, 'failed')
                }
            }
        } catch (e) {
            this.log?.error(`Polling error: ${e}`)
        }
    }

    private async handleHourlyAggregation(payload: AggregatePayload) {
        const { platform, u_id, start, end, bot_id } = payload
        this.log?.info(`Processing HOURLY batch for ${u_id} on ${platform}`)

        const articles = await DB.Article.getArticlesByTimeRange(u_id, platform, start, end)
        if (articles.length === 0) {
            this.log?.info(`No articles found for hourly batch.`)
            return
        }

        // 1. Text Summary
        const contentLines = articles.map(a => {
            const time = dayjs.unix(a.created_at).format('HH:mm')
            return `[${time}] ${a.content ?? '(No Text)'}`
        })
        const summaryText = contentLines.join('\n\n')

        // 2. Generate Summary Image
        let mediaFiles: { path: string, media_type: 'photo' | 'video' }[] = []
        const fs = await import('fs')

        try {
            const { ImgConverter } = await import('@idol-bbq-utils/render')
            const { writeImgToFile } = await import('@/middleware/media')

            const fakeArticle: any = {
                id: 0,
                platform: platform,
                a_id: `batch-${start}-${end}`,
                u_id: u_id,
                username: `Hourly Batch: ${u_id}`,
                created_at: end,
                content: summaryText,
                url: `https://${platform}.com`,
                type: 'post',
                ref: null,
                has_media: false,
                media: [],
                extra: null
            }
            const converter = new ImgConverter()
            const imgBuffer = await converter.articleToImg(fakeArticle as any, 'default')
            const path = writeImgToFile(imgBuffer, `batch-${start}-${end}.png`)
            mediaFiles.push({ path, media_type: 'photo' })
        } catch (e) {
            this.log?.error(`Failed to generate batch summary image: ${e}`)
        }

        // 3. Collect Media from Articles
        const { RenderService } = await import('@/services/render-service')
        const renderService = new RenderService(this.log)

        for (const article of articles) {
            if (article.has_media || article.media) {
                try {
                    // We use a dummy media config to trigger download
                    const dummyMediaConfig = { type: 'no-storage', use: { tool: 'default' } } as any
                    const result = await renderService.process(article, {
                        taskId: `batch-sub-${article.a_id}`,
                        render_type: 'text', // We only want media files
                        mediaConfig: dummyMediaConfig
                    })
                    if (result.mediaFiles.length > 0) {
                        mediaFiles = mediaFiles.concat(result.mediaFiles as any)
                    }
                } catch (e) {
                    this.log?.error(`Error fetching media for article ${article.a_id}: ${e}`)
                }
            }
        }

        // 4. Send
        const forwarder = this.forwarderPools.getTarget(bot_id)
        if (forwarder) {
            await forwarder.send(`Hourly Batch for ${u_id}`, {
                timestamp: Math.floor(Date.now() / 1000),
                media: mediaFiles.length > 0 ? mediaFiles : undefined
            })

            // Cleanup
            if (mediaFiles.length > 0) {
                setTimeout(() => {
                    mediaFiles.forEach(f => {
                        try { fs.unlinkSync(f.path) } catch (e) { }
                    })
                }, 60000) // Delayed cleanup 1 minute
            }
        } else {
            this.log?.warn(`Forwarder ${bot_id} not found for hourly batch`)
        }
    }

    private async handleDailyAggregation(payload: AggregatePayload) {
        const { platform, u_id, start, end, bot_id, processorConfig, prompt } = payload

        this.log?.info(`Processing aggregation for ${u_id} on ${platform} (${dayjs.unix(start).format()} - ${dayjs.unix(end).format()})`)

        const articles = await DB.Article.getArticlesByTimeRange(u_id, platform, start, end)
        if (articles.length === 0) {
            this.log?.info(`No articles found for aggregation.`)
            return
        }

        const reversedArticles = articles.reverse() // created_at asc usually better for chronological summary

        const contentLines = reversedArticles.map(a => {
            const time = dayjs.unix(a.created_at).format('YYYY-MM-DD HH:mm:ss')
            return `[${time}] ${a.content ?? '(No Text)'}\n`
        })
        const textToProcess = contentLines.join('\n')

        const provider = processorConfig?.provider || 'Google'
        const apiKey = processorConfig?.api_key || process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || ''

        const summaryPrompt = prompt || `You are a summarizer. Please summarize the following social media posts from today for a daily report. Format it nicely.`

        let summary = ''
        try {
            const configWithPrompt = { ...processorConfig, prompt: summaryPrompt }
            const processor = await processorRegistry.create(provider, apiKey, this.log, configWithPrompt)
            summary = await processor.process(textToProcess)
        } catch (e) {
            this.log?.error(`Summarization failed: ${e}`)
            summary = `Summarization failed. Raw content count: ${articles.length}`
        }

        // --- NEW: Generate Image Card for Summary ---
        // Exemption: Do not generate card for video platforms (TikTok, YouTube)
        // because "merging" video summaries into a card usually doesn't make sense or look good.
        const isVideoPlatform = [Platform.TikTok, Platform.YouTube].includes(platform)

        let mediaFiles: { path: string, media_type: 'photo' }[] = []

        const fs = await import('fs')
        if (!isVideoPlatform) {
            const { ImgConverter } = await import('@idol-bbq-utils/render')
            const { writeImgToFile } = await import('@/middleware/media')

            const fakeArticle: any = {
                id: 0,
                platform: platform,
                a_id: `summary-${start}-${end}`,
                u_id: u_id,
                username: `Daily Report: ${u_id}`,
                created_at: end,
                content: summary,
                translation: '',
                translated_by: '',
                url: `https://${platform}.com`,
                type: 'post', // Generic type
                ref: null,
                has_media: false,
                media: [],
                extra: null,
                u_avatar: null
            }

            try {
                const converter = new ImgConverter()
                // Use 'default' template for now. Future: select based on config?
                const imgBuffer = await converter.articleToImg(fakeArticle as any, 'default')
                const path = writeImgToFile(imgBuffer, `summary-${start}-${end}.png`)
                mediaFiles.push({ path, media_type: 'photo' })
            } catch (e) {
                this.log?.error(`Failed to generate summary image: ${e}`)
            }
        } else {
            this.log?.info(`Skipping summary image generation for video platform ${platform}`)
        }
        // --------------------------------------------

        const forwarder = this.forwarderPools.getTarget(bot_id)
        if (forwarder) {
            await forwarder.send(`Daily Report for ${u_id}:\n\n${summary}`, {
                timestamp: Math.floor(Date.now() / 1000),
                media: mediaFiles.length > 0 ? mediaFiles : undefined
            })

            // Cleanup
            if (mediaFiles.length > 0) {
                mediaFiles.forEach(f => {
                    try { fs.unlinkSync(f.path) } catch (e) { }
                })
            }
        } else {
            this.log?.warn(`Forwarder ${bot_id} not found for aggregation result`)
        }
    }
}
