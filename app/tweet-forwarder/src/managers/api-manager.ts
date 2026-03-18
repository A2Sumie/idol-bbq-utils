import { BaseCompatibleModel, TaskScheduler } from '@/utils/base'
import type { AppConfig, Processor } from '@/types'
import { Logger } from '@idol-bbq-utils/log'
import fs from 'fs'
import path from 'path'
import YAML from 'yaml'
import DB from '@/db'
import EventEmitter from 'events'
import type { ForwarderPools } from './forwarder-manager'
import { Platform } from '@idol-bbq-utils/spider/types'
import { platformNameMap } from '@idol-bbq-utils/spider/const'
import { BaseProcessor, PROCESSOR_ERROR_FALLBACK } from '@/middleware/processor/base'
import { processorRegistry } from '@/middleware/processor'
import type { Article } from '@idol-bbq-utils/render/types'
import dayjs from 'dayjs'
import { CACHE_DIR_ROOT, RETRY_LIMIT } from '@/config'
import { getCookiesRoot } from '@/utils/directories'
import { pRetry } from '@idol-bbq-utils/utils'

interface ApiConfig {
    port?: number
    secret?: string
}

interface ApiRuntimeDeps {
    emitter?: EventEmitter
    forwarderPools?: ForwarderPools
}

function jsonResponse(payload: unknown, status = 200) {
    return new Response(JSON.stringify(payload), {
        status,
        headers: { 'Content-Type': 'application/json' },
    })
}

function resolvePlatform(value?: string | null): Platform | null {
    if (!value) {
        return null
    }
    if (/^\d+$/.test(value)) {
        return Number(value) as Platform
    }
    const normalized = value.toLowerCase()
    if (['x', 'twitter'].includes(normalized)) return Platform.X
    if (normalized === 'instagram') return Platform.Instagram
    if (['tiktok', 'tik_tok'].includes(normalized)) return Platform.TikTok
    if (['youtube', 'yt'].includes(normalized)) return Platform.YouTube
    if (['website', 'web'].includes(normalized)) return Platform.Website
    return null
}

function resolvePlatformFromOrigin(origin?: string | null): Platform | null {
    if (!origin) {
        return null
    }
    const normalized = origin.toLowerCase()
    if (normalized.includes('x.com') || normalized.includes('twitter.com')) return Platform.X
    if (normalized.includes('instagram.com')) return Platform.Instagram
    if (normalized.includes('tiktok.com')) return Platform.TikTok
    if (normalized.includes('youtube.com') || normalized.includes('youtu.be')) return Platform.YouTube
    return Platform.Website
}

function flattenArticleChain(article: Article & { id: number }) {
    const chain: Array<Article & { id: number }> = []
    let current: (Article & { id: number }) | null = article
    while (current && typeof current === 'object') {
        chain.push(current)
        current = typeof current.ref === 'object' ? ((current.ref as any) || null) : null
    }
    return chain
}

function tryParseJson(value: string) {
    try {
        return JSON.parse(value)
    } catch {
        return null
    }
}

function selectProcessorResult(value: any, resultKey?: string | null) {
    if (!resultKey) {
        return value
    }
    return resultKey.split('.').filter(Boolean).reduce((current, key) => {
        if (current && typeof current === 'object' && key in current) {
            return current[key]
        }
        return undefined
    }, value)
}

function latestLogFile() {
    const logsDir = path.join(CACHE_DIR_ROOT, 'logs')
    if (!fs.existsSync(logsDir)) {
        return null
    }
    const files = fs
        .readdirSync(logsDir)
        .filter((file) => file.endsWith('.log'))
        .map((file) => {
            const fullPath = path.join(logsDir, file)
            return {
                fullPath,
                mtime: fs.statSync(fullPath).mtimeMs,
            }
        })
        .sort((a, b) => b.mtime - a.mtime)
    return files[0]?.fullPath || null
}

function tailLines(text: string, limit: number) {
    return text.split(/\r?\n/).filter(Boolean).slice(-limit)
}

export class APIManager extends BaseCompatibleModel {
    NAME = 'APIManager'
    log?: Logger
    private config: AppConfig
    private server?: any
    private deps: ApiRuntimeDeps

    constructor(config: AppConfig, deps: ApiRuntimeDeps, log?: Logger) {
        super()
        this.config = config
        this.deps = deps
        this.log = log?.child({ subservice: this.NAME })
    }

    async init() {
        this.log?.info('APIManager initializing...')
        const apiConfig = this.config.api || {}
        const port = apiConfig.port || 3000
        const secret = apiConfig.secret || process.env.API_SECRET

        if (!secret) {
            this.log?.warn('No API secret configured, skipping APIManager start for security.')
            return
        }

        this.server = Bun.serve({
            port,
            fetch: async (req) => {
                const url = new URL(req.url)

                const authHeader = req.headers.get('Authorization')
                if (!authHeader || authHeader !== `Bearer ${secret}`) {
                    return new Response('Unauthorized', { status: 401 })
                }

                if (req.method === 'POST' && url.pathname === '/api/cookies') return this.handleCookieUpdate(req)
                if (req.method === 'DELETE' && url.pathname === '/api/cookies') return this.handleCookieDelete(req)
                if (req.method === 'POST' && url.pathname === '/api/cookies/delete') return this.handleCookieDelete(req)
                if (req.method === 'GET' && url.pathname === '/api/cookies') return this.handleCookieList()
                if (req.method === 'GET' && url.pathname.startsWith('/api/cookies/')) {
                    const finder = url.pathname.split('/api/cookies/')[1]
                    return this.handleCookieView(finder)
                }

                if (req.method === 'GET' && url.pathname === '/api/config') return this.handleConfigGet()
                if (req.method === 'GET' && url.pathname === '/api/config/crawlers') return this.handleConfigList()
                if (req.method === 'POST' && url.pathname === '/api/config/update') return this.handleConfigUpdate(req)
                if (req.method === 'POST' && (url.pathname === '/api/server/restart' || url.pathname === '/api/runtime/reload')) {
                    return this.handleServerRestart()
                }

                if (req.method === 'GET' && url.pathname === '/api/runtime/status') return this.handleRuntimeStatus()
                if (req.method === 'GET' && url.pathname === '/api/runtime/logs') return this.handleRuntimeLogs(url)
                if (req.method === 'GET' && url.pathname === '/api/articles') return this.handleArticleList(url)
                if (req.method === 'GET' && url.pathname.startsWith('/api/articles/')) return this.handleArticleView(url)
                if (req.method === 'GET' && url.pathname === '/api/tasks') return this.handleTasks(url)
                if (req.method === 'GET' && url.pathname === '/api/processor-runs') return this.handleProcessorRuns(url)

                if (req.method === 'POST' && url.pathname === '/api/actions/crawlers/run') return this.handleCrawlerRun(req)
                if (req.method === 'POST' && url.pathname === '/api/actions/articles/reprocess') return this.handleArticleReprocess(req)
                if (req.method === 'POST' && url.pathname === '/api/actions/articles/resend') return this.handleArticleResend(req)
                if (req.method === 'POST' && url.pathname === '/api/actions/processors/run') return this.handleProcessorRun(req)

                return new Response(`Not Found: ${req.method} ${url.pathname}`, { status: 404 })
            },
        })

        this.log?.info(`APIManager listening on port ${port}`)
    }

    async drop() {
        if (this.server) {
            this.server.stop()
            this.log?.info('APIManager stopped')
        }
    }

    private async handleCookieUpdate(req: Request): Promise<Response> {
        try {
            const body = (await req.json()) as { finder: string; cookie: string }
            const { finder, cookie } = body

            if (!finder || !cookie) {
                return new Response('Missing finder or cookie', { status: 400 })
            }

            const crawlers = this.config.crawlers
            if (!crawlers) {
                return new Response('No crawlers configured', { status: 500 })
            }

            let cookieFile: string
            const crawler = crawlers.find((c) => {
                if (c.name === finder) return true
                if (c.websites?.some((w) => w.includes(finder))) return true
                return false
            })

            if (crawler && crawler.cfg_crawler?.cookie_file) {
                cookieFile = crawler.cfg_crawler.cookie_file
            } else {
                const safeName = path.basename(finder).replace(/\.txt$/, '')
                cookieFile = path.join(getCookiesRoot(), `${safeName}.txt`)
            }

            const dir = path.dirname(cookieFile)
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir, { recursive: true })
            }

            fs.writeFileSync(cookieFile, cookie)
            this.log?.info(`Cookie updated for ${finder} at ${cookieFile}`)
            return new Response('Cookie updated successfully', { status: 200 })
        } catch (error) {
            this.log?.error('Cookie update error:', error)
            return new Response('Failed to update cookie', { status: 500 })
        }
    }

    private async handleCookieList(): Promise<Response> {
        try {
            const cookiesDir = getCookiesRoot()

            if (!fs.existsSync(cookiesDir)) {
                return jsonResponse([])
            }

            const files = fs.readdirSync(cookiesDir)
            const cookieFiles = files.filter((file) => file.endsWith('.txt')).map((file) => {
                const filePath = path.join(cookiesDir, file)
                const stats = fs.statSync(filePath)
                return {
                    name: file.replace('.txt', ''),
                    filename: file,
                    lastModified: stats.mtime.toISOString(),
                    size: stats.size,
                }
            })

            return jsonResponse(cookieFiles)
        } catch (error) {
            this.log?.error('Cookie list error:', error)
            return new Response('Failed to list cookies', { status: 500 })
        }
    }

    private async handleCookieView(finder: string): Promise<Response> {
        try {
            const cookiesDir = getCookiesRoot()
            const cookieFile = path.join(cookiesDir, `${finder}.txt`)

            if (!fs.existsSync(cookieFile)) {
                return new Response('Cookie file not found', { status: 404 })
            }

            const content = fs.readFileSync(cookieFile, 'utf-8')
            const stats = fs.statSync(cookieFile)

            return jsonResponse({
                name: finder,
                content,
                lastModified: stats.mtime.toISOString(),
                size: stats.size,
            })
        } catch (error) {
            this.log?.error('Cookie view error:', error)
            return new Response('Failed to read cookie', { status: 500 })
        }
    }

    private async handleCookieDelete(req: Request): Promise<Response> {
        try {
            const body = (await req.json()) as { filenames: string[] }
            const { filenames } = body

            if (!filenames || !Array.isArray(filenames) || filenames.length === 0) {
                return new Response('No filenames provided', { status: 400 })
            }

            const cookiesDir = getCookiesRoot()
            let deletedCount = 0
            const errors: string[] = []

            for (const filename of filenames) {
                const safeName = path.basename(filename)
                if (safeName !== filename) {
                    errors.push(`Invalid filename: ${filename}`)
                    continue
                }

                const filePath = path.join(cookiesDir, safeName)
                if (fs.existsSync(filePath)) {
                    try {
                        fs.unlinkSync(filePath)
                        deletedCount++
                    } catch (error: any) {
                        errors.push(`Failed to delete ${filename}: ${error.message}`)
                    }
                }
            }

            return jsonResponse({
                success: true,
                deleted: deletedCount,
                errors,
            })
        } catch (error) {
            this.log?.error('Cookie delete error:', error)
            return new Response('Failed to delete cookies', { status: 500 })
        }
    }

    private async handleConfigList(): Promise<Response> {
        try {
            const crawlers = this.config.crawlers || []
            const crawlerInfo = crawlers.map((crawler) => ({
                name: crawler.name,
                type: crawler.task_type,
                schedule: crawler.cfg_crawler?.cron || null,
                cookieFile: crawler.cfg_crawler?.cookie_file || null,
                deviceProfile: crawler.cfg_crawler?.device_profile || null,
                sessionProfile: crawler.cfg_crawler?.session_profile || null,
                enabled: true,
            }))

            return jsonResponse(crawlerInfo)
        } catch (error) {
            this.log?.error('Config list error:', error)
            return new Response('Failed to list crawlers', { status: 500 })
        }
    }

    private async handleConfigUpdate(req: Request): Promise<Response> {
        try {
            const body = (await req.json()) as AppConfig
            if (!body || typeof body !== 'object') {
                return new Response('Invalid config format', { status: 400 })
            }

            const configPath = path.join(process.cwd(), 'config.yaml')
            if (fs.existsSync(configPath)) {
                fs.copyFileSync(configPath, `${configPath}.bak`)
            }

            fs.writeFileSync(configPath, YAML.stringify(body), 'utf8')
            this.config = body

            return jsonResponse({
                success: true,
                message: 'Configuration saved. Restart server to apply changes.',
            })
        } catch (error) {
            this.log?.error('Config update error:', error)
            return new Response(`Failed to update config: ${error instanceof Error ? error.message : String(error)}`, {
                status: 500,
            })
        }
    }

    private async handleServerRestart(): Promise<Response> {
        this.log?.warn('Server restart requested via API')
        setTimeout(() => {
            this.log?.info('Exiting process for restart...')
            process.exit(0)
        }, 1000)

        return jsonResponse({ success: true, message: 'Server restarting...' })
    }

    private async handleConfigGet(): Promise<Response> {
        return jsonResponse(this.config)
    }

    private async handleRuntimeStatus(): Promise<Response> {
        const tasks = await DB.TaskQueue.list(50)
        return jsonResponse({
            uptime_sec: Math.floor(process.uptime()),
            crawlers: this.config.crawlers?.length || 0,
            processors: this.config.processors?.length || 0,
            formatters: this.config.formatters?.length || 0,
            forward_targets: this.config.forward_targets?.length || 0,
            forwarders: this.config.forwarders?.length || 0,
            pending_tasks: tasks.filter((task) => task.status === 'pending').length,
            processing_tasks: tasks.filter((task) => task.status === 'processing').length,
            latest_tasks: tasks.slice(0, 10),
        })
    }

    private async handleRuntimeLogs(url: URL): Promise<Response> {
        const limit = Math.max(1, Math.min(Number(url.searchParams.get('limit') || '200'), 1000))
        const file = latestLogFile()
        if (!file) {
            return jsonResponse({ lines: [], file: null })
        }
        const text = fs.readFileSync(file, 'utf8')
        return jsonResponse({
            file,
            lines: tailLines(text, limit),
        })
    }

    private async handleArticleList(url: URL): Promise<Response> {
        const platform = resolvePlatform(url.searchParams.get('platform'))
        const limit = Number(url.searchParams.get('limit') || '50')
        const u_id = url.searchParams.get('u_id') || undefined
        const a_id = url.searchParams.get('a_id') || undefined
        const q = url.searchParams.get('q') || undefined
        const from = url.searchParams.get('from')
        const to = url.searchParams.get('to')
        const articles = await DB.Article.query({
            platform: platform || undefined,
            u_id,
            a_id,
            q,
            from: from ? Number(from) : undefined,
            to: to ? Number(to) : undefined,
            limit,
        })
        return jsonResponse(articles)
    }

    private async handleArticleView(url: URL): Promise<Response> {
        const segments = url.pathname.split('/').filter(Boolean)
        if (segments.length < 4) {
            return new Response('Missing platform or article id', { status: 400 })
        }
        const platform = resolvePlatform(segments[2])
        if (!platform) {
            return new Response('Invalid platform', { status: 400 })
        }
        const idOrCode = decodeURIComponent(segments.slice(3).join('/'))
        const article = /^\d+$/.test(idOrCode)
            ? await DB.Article.getSingleArticle(Number(idOrCode), platform)
            : await DB.Article.getSingleArticleByArticleCode(idOrCode, platform)
        if (!article) {
            return new Response('Article not found', { status: 404 })
        }
        return jsonResponse(article)
    }

    private async handleTasks(url: URL): Promise<Response> {
        const limit = Math.max(1, Math.min(Number(url.searchParams.get('limit') || '100'), 200))
        const status = url.searchParams.get('status') || undefined
        return jsonResponse(await DB.TaskQueue.list(limit, status))
    }

    private async handleProcessorRuns(url: URL): Promise<Response> {
        const limit = Math.max(1, Math.min(Number(url.searchParams.get('limit') || '100'), 200))
        const source_ref = url.searchParams.get('source_ref') || undefined
        return jsonResponse(await DB.ProcessorRun.list(limit, source_ref))
    }

    private async handleCrawlerRun(req: Request): Promise<Response> {
        if (!this.deps.emitter) {
            return new Response('Emitter unavailable', { status: 500 })
        }
        const body = (await req.json()) as { crawler?: string; name?: string }
        const crawlerName = body.crawler || body.name
        const crawler = this.config.crawlers?.find((item) => item.name === crawlerName)
        if (!crawler || !crawler.name) {
            return new Response('Crawler not found', { status: 404 })
        }

        const now = Math.floor(Date.now() / 1000)
        const queueTask = await DB.TaskQueue.add(
            'manual_crawler_run',
            { crawler: crawler.name },
            now,
            { source_ref: crawler.name, action_type: 'crawl' },
        )
        const taskId = `manual-${Math.random().toString(36).slice(2, 9)}`
        const task: TaskScheduler.Task = {
            id: taskId,
            status: TaskScheduler.TaskStatus.PENDING,
            data: crawler,
        }
        this.deps.emitter.emit(`spider:${TaskScheduler.TaskEvent.DISPATCH}`, {
            taskId,
            task,
        })
        await DB.TaskQueue.updateStatus(queueTask.id, 'completed', {
            result_summary: `crawler ${crawler.name} dispatched`,
        })
        return jsonResponse({ success: true, taskId, crawler: crawler.name })
    }

    private async handleArticleReprocess(req: Request): Promise<Response> {
        const body = (await req.json()) as {
            platform?: string
            id?: number
            a_id?: string
            processorId?: string
            force?: boolean
        }
        const platform = resolvePlatform(body.platform)
        if (!platform) {
            return new Response('Invalid platform', { status: 400 })
        }
        const article = await this.loadArticle(platform, body.id, body.a_id)
        if (!article) {
            return new Response('Article not found', { status: 404 })
        }

        const task = await DB.TaskQueue.add(
            'article_reprocess',
            body,
            Math.floor(Date.now() / 1000),
            {
                source_ref: `${platform}:${article.a_id}`,
                action_type: 'reprocess',
            },
        )

        try {
            const processorDef = this.resolveProcessorDefinition(body.processorId)
            const processor = await this.createProcessor(processorDef)
            await this.translateArticleChain(article, processor, body.force === true)
            const updated = await DB.Article.getSingleArticle(article.id, article.platform)
            await DB.TaskQueue.updateStatus(task.id, 'completed', {
                result_summary: `reprocessed ${article.a_id}`,
            })
            return jsonResponse({
                success: true,
                article: updated,
            })
        } catch (error) {
            await DB.TaskQueue.updateStatus(task.id, 'failed', {
                last_error: error instanceof Error ? error.message : String(error),
            })
            throw error
        }
    }

    private async handleArticleResend(req: Request): Promise<Response> {
        if (!this.deps.forwarderPools) {
            return new Response('Forwarder runtime unavailable', { status: 500 })
        }
        const body = (await req.json()) as {
            platform?: string
            id?: number
            a_id?: string
            crawlerName?: string
        }
        const platform = resolvePlatform(body.platform)
        if (!platform) {
            return new Response('Invalid platform', { status: 400 })
        }
        if (!body.crawlerName) {
            return new Response('crawlerName is required', { status: 400 })
        }
        const article = await this.loadArticle(platform, body.id, body.a_id)
        if (!article) {
            return new Response('Article not found', { status: 404 })
        }
        const crawler = this.config.crawlers?.find((item) => item.name === body.crawlerName)
        if (!crawler) {
            return new Response('Crawler not found', { status: 404 })
        }
        const crawlerPlatform = resolvePlatformFromOrigin(crawler.origin)
        if (!crawlerPlatform) {
            return new Response('Unable to determine crawler platform', { status: 400 })
        }
        if (crawlerPlatform !== article.platform) {
            return new Response(
                `Crawler platform mismatch: ${body.crawlerName} is ${platformNameMap[crawlerPlatform]}, article is ${platformNameMap[article.platform]}`,
                { status: 400 },
            )
        }

        const task = await DB.TaskQueue.add(
            'article_resend',
            body,
            Math.floor(Date.now() / 1000),
            {
                source_ref: `${platform}:${article.a_id}`,
                action_type: 'resend',
            },
        )

        try {
            await this.deps.forwarderPools.resendArticle(article as any, body.crawlerName)
            await DB.TaskQueue.updateStatus(task.id, 'completed', {
                result_summary: `resent ${article.a_id}`,
            })
            return jsonResponse({ success: true, articleId: article.id, crawlerName: body.crawlerName })
        } catch (error) {
            await DB.TaskQueue.updateStatus(task.id, 'failed', {
                last_error: error instanceof Error ? error.message : String(error),
            })
            throw error
        }
    }

    private async handleProcessorRun(req: Request): Promise<Response> {
        const body = (await req.json()) as {
            processorId?: string
            action?: 'translate' | 'extract' | 'merge' | 'plan'
            platform?: string
            id?: number
            a_id?: string
            u_id?: string
            start?: number
            end?: number
            text?: string
            scheduleUrl?: string
            scheduleApiKey?: string
            resultKey?: string
        }
        const processorDef = this.resolveProcessorDefinition(body.processorId)
        const action = body.action || processorDef.cfg_processor?.action || 'extract'
        const task = await DB.TaskQueue.add(
            'processor_run',
            body,
            Math.floor(Date.now() / 1000),
            {
                source_ref: body.a_id || body.u_id || body.id?.toString() || null || undefined,
                action_type: action,
            },
        )

        try {
            const processor = await this.createProcessor(processorDef)
            const input = await this.buildProcessorInput(body)
            if (action === 'translate' && input.article) {
                await this.translateArticleChain(input.article, processor, true)
            }

            const rawResult = await processor.process(input.text)
            const parsed = tryParseJson(rawResult)
            const selected = selectProcessorResult(
                parsed,
                body.resultKey || processorDef.cfg_processor?.result_key,
            )
            const scheduleResults =
                action === 'plan'
                    ? await this.writeSchedulesFromPlan(
                          selected ?? parsed,
                          input.sourceRef,
                          body.scheduleUrl || processorDef.cfg_processor?.schedule_url,
                          body.scheduleApiKey || processorDef.cfg_processor?.schedule_api_key,
                      )
                    : []

            const run = await DB.ProcessorRun.create({
                processor_id: processorDef.id || processorDef.name || processor.NAME,
                action,
                source_type: input.sourceType,
                source_ref: input.sourceRef,
                input: {
                    request: body,
                    text: input.text,
                },
                output: {
                    raw: rawResult,
                    parsed,
                    selected,
                    result_key: body.resultKey || processorDef.cfg_processor?.result_key || null,
                    schedules: scheduleResults,
                },
            })

            await DB.TaskQueue.updateStatus(task.id, 'completed', {
                result_summary: `${action} completed`,
            })

            return jsonResponse({
                success: true,
                run,
                result: {
                    raw: rawResult,
                    parsed,
                    selected,
                    schedules: scheduleResults,
                },
            })
        } catch (error) {
            await DB.TaskQueue.updateStatus(task.id, 'failed', {
                last_error: error instanceof Error ? error.message : String(error),
            })
            throw error
        }
    }

    private async buildProcessorInput(body: {
        platform?: string
        id?: number
        a_id?: string
        u_id?: string
        start?: number
        end?: number
        text?: string
    }) {
        const platform = resolvePlatform(body.platform)
        if (platform && (body.id || body.a_id)) {
            const article = await this.loadArticle(platform, body.id, body.a_id)
            if (!article) {
                throw new Error('Article not found')
            }
            return {
                article,
                sourceType: 'article',
                sourceRef: `${platform}:${article.a_id}`,
                text: this.buildArticleDigest([article as any]),
            }
        }

        if (platform && body.u_id && body.start && body.end) {
            const articles = await DB.Article.getArticlesByTimeRange(body.u_id, platform, body.start, body.end)
            return {
                article: null,
                sourceType: 'window',
                sourceRef: `${platform}:${body.u_id}:${body.start}-${body.end}`,
                text: this.buildArticleDigest(articles),
            }
        }

        if (body.text) {
            return {
                article: null,
                sourceType: 'text',
                sourceRef: 'manual:text',
                text: body.text,
            }
        }

        throw new Error('No valid processor input provided')
    }

    private buildArticleDigest(articles: Array<Article & { id: number }>) {
        return articles
            .slice()
            .sort((a, b) => a.created_at - b.created_at)
            .map((article) => {
                const extraContent = typeof article.extra?.content === 'string' ? article.extra.content.trim() : ''
                const mediaUrls = Array.isArray(article.media)
                    ? article.media
                          .map((media) => media?.url?.trim())
                          .filter((url): url is string => Boolean(url))
                    : []
                return [
                    `[${dayjs.unix(article.created_at).format('YYYY-MM-DD HH:mm:ss')}]`,
                    `Article DB ID: ${article.id}`,
                    `Article ID: ${article.a_id}`,
                    `Platform: ${String(Platform[article.platform] || article.platform).toLowerCase()}`,
                    `User ID: ${article.u_id}`,
                    `Username: ${article.username}`,
                    `URL: ${article.url}`,
                    'Content:',
                    article.content || '(empty)',
                    extraContent ? ['Extra Content:', extraContent].join('\n') : null,
                    mediaUrls.length > 0 ? ['Media URLs:', ...mediaUrls.map((url) => `- ${url}`)].join('\n') : null,
                ]
                    .filter((line): line is string => Boolean(line))
                    .join('\n')
            })
            .join('\n\n---\n\n')
    }

    private async translateArticleChain(article: Article & { id: number }, processor: BaseProcessor, force = false) {
        const chain = flattenArticleChain(article)
        for (const current of chain) {
            const patch: Partial<Article> = {}

            if (current.content && (force || !BaseProcessor.isValidResult(current.translation))) {
                patch.translation = await this.processText(processor, current.content)
                patch.translated_by = processor.NAME
            }

            if (current.media) {
                let changed = false
                const updatedMedia = await Promise.all(
                    current.media.map(async (media) => {
                        if (!media.alt || (!force && BaseProcessor.isValidResult((media as any).translation))) {
                            return media
                        }
                        changed = true
                        return {
                            ...media,
                            translation: await this.processText(processor, media.alt),
                            translated_by: processor.NAME,
                        }
                    }),
                )
                if (changed) {
                    patch.media = updatedMedia as any
                }
            }

            if (current.extra?.content && (force || !BaseProcessor.isValidResult((current.extra as any).translation))) {
                patch.extra = {
                    ...current.extra,
                    translation: await this.processText(processor, current.extra.content),
                    translated_by: processor.NAME,
                } as any
            }

            if (Object.keys(patch).length > 0) {
                await DB.Article.update(current.id, current.platform, patch)
            }
        }
    }

    private async processText(processor: BaseProcessor, text: string) {
        return await pRetry(() => processor.process(text), {
            retries: RETRY_LIMIT,
        })
            .then((value) => value)
            .catch((error) => {
                this.log?.error(`Processor failed: ${error}`)
                return PROCESSOR_ERROR_FALLBACK
            })
    }

    private resolveProcessorDefinition(processorId?: string) {
        const processors = this.config.processors || []
        const processorDef = processorId
            ? processors.find((processor) => processor.id === processorId || processor.name === processorId)
            : processors[0]
        if (!processorDef) {
            throw new Error(`Processor not found: ${processorId || 'default'}`)
        }
        return processorDef
    }

    private async createProcessor(processorDef: Processor) {
        return await processorRegistry.create(
            processorDef.provider,
            processorDef.api_key,
            this.log,
            processorDef.cfg_processor,
        )
    }

    private async loadArticle(platform: Platform, id?: number, a_id?: string) {
        if (id) {
            return await DB.Article.getSingleArticle(id, platform)
        }
        if (a_id) {
            return await DB.Article.getSingleArticleByArticleCode(a_id, platform)
        }
        return null
    }

    private async writeSchedulesFromPlan(
        parsed: any,
        sourceRef: string,
        scheduleUrl?: string,
        scheduleApiKey?: string,
    ) {
        const targetUrl = scheduleUrl || process.env.SCHEDULE_WEBHOOK_URL
        if (!targetUrl) {
            return []
        }
        const candidates = Array.isArray(parsed)
            ? parsed
            : parsed?.plans || parsed?.items || parsed?.tasks || (parsed?.title ? [parsed] : [])
        if (!Array.isArray(candidates)) {
            return []
        }

        const results = []
        for (const [index, candidate] of candidates.entries()) {
            if (!candidate?.title || !candidate?.executionTime) {
                continue
            }

            const payload = {
                title: candidate.title,
                description: candidate.description || null,
                scheduleType: candidate.scheduleType || 'workflow',
                executionTime: candidate.executionTime,
                recurrence: candidate.recurrence || null,
                payload: candidate.payload || null,
                externalKey: candidate.externalKey || `${sourceRef}:${index}`,
                apiKey: scheduleApiKey || process.env.SCHEDULE_WEBHOOK_API_KEY,
            }

            const response = await fetch(targetUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            })
            const text = await response.text()
            results.push({
                ok: response.ok,
                status: response.status,
                body: tryParseJson(text) || text,
            })
        }
        return results
    }
}
