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
        const { ImgConverter } = await import('@idol-bbq-utils/render')
        const { writeImgToFile } = await import('@/middleware/media')
        const fs = await import('fs')

        const fakeArticle: any = {
            id: 0,
            a_id: `summary-${start}-${end}`,
            u_id: u_id, // Title of the card
            platform: platform,
            content: summary,
            created_at: end,
            url: `https://${platform}.com`,
            has_media: false,
            timestamp: end,
            author: {
                name: `Daily Report: ${u_id}`,
                username: u_id,
                url: '',
                avatar: ''
            }
        }

        let mediaFiles: { path: string, media_type: 'photo' }[] = []
        try {
            const converter = new ImgConverter()
            const imgBuffer = await converter.articleToImg(fakeArticle)
            const path = writeImgToFile(imgBuffer, `summary-${start}-${end}.png`)
            mediaFiles.push({ path, media_type: 'photo' })
        } catch (e) {
            this.log?.error(`Failed to generate summary image: ${e}`)
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
