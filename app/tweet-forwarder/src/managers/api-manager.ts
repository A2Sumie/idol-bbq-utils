import { BaseCompatibleModel } from '@/utils/base'
import type { AppConfig } from '@/types'
import { Logger } from '@idol-bbq-utils/log'
import { spiderRegistry } from '@idol-bbq-utils/spider'
import fs from 'fs'
import path from 'path'

interface ApiConfig {
    port?: number
    secret?: string
}

export class APIManager extends BaseCompatibleModel {
    NAME = 'APIManager'
    log?: Logger
    private config: AppConfig
    private server?: any

    constructor(config: AppConfig, log?: Logger) {
        super()
        this.config = config
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

                // Auth check
                const authHeader = req.headers.get('Authorization')
                if (!authHeader || authHeader !== `Bearer ${secret}`) {
                    return new Response('Unauthorized', { status: 401 })
                }

                // Cookie management endpoints
                if (req.method === 'POST' && url.pathname === '/api/cookie') {
                    return this.handleCookieUpdate(req)
                }

                if (req.method === 'GET' && url.pathname === '/api/cookies') {
                    return this.handleCookieList(req)
                }

                if (req.method === 'GET' && url.pathname.startsWith('/api/cookie/')) {
                    const finder = url.pathname.split('/api/cookie/')[1]
                    return this.handleCookieView(req, finder)
                }

                // Config management endpoints
                if (req.method === 'GET' && url.pathname === '/api/config/crawlers') {
                    return this.handleConfigList(req)
                }

                return new Response(`Not Found: ${req.method} ${url.pathname} (Full: ${req.url})`, { status: 404 })

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
            const body = await req.json() as { finder: string; cookie: string }
            const { finder, cookie } = body

            if (!finder || !cookie) {
                return new Response('Missing finder or cookie', { status: 400 })
            }

            const crawlers = this.config.crawlers
            if (!crawlers) {
                return new Response('No crawlers configured', { status: 500 })
            }

            // Find crawler by name or website
            const crawler = crawlers.find(c => {
                if (c.name === finder) return true
                if (c.websites?.some(w => w.includes(finder))) return true
                return false
            })

            if (!crawler) {
                return new Response('Crawler not found', { status: 404 })
            }

            const cookieFile = crawler.cfg_crawler?.cookie_file
            if (!cookieFile) {
                return new Response('Crawler has no cookie_file configured', { status: 400 })
            }

            // Ensure directory exists
            const dir = path.dirname(cookieFile)
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir, { recursive: true })
            }

            fs.writeFileSync(cookieFile, cookie)
            this.log?.info(`Cookie updated for ${finder}`)
            return new Response('Cookie updated successfully', { status: 200 })
        } catch (error) {
            this.log?.error('Cookie update error:', error)
            return new Response('Failed to update cookie', { status: 500 })
        }
    }

    private async handleCookieList(req: Request): Promise<Response> {
        try {
            const cookiesDir = path.join(process.cwd(), 'assets', 'cookies')

            if (!fs.existsSync(cookiesDir)) {
                return new Response(JSON.stringify([]), {
                    status: 200,
                    headers: { 'Content-Type': 'application/json' }
                })
            }

            const files = fs.readdirSync(cookiesDir)
            const cookieFiles = files
                .filter(f => f.endsWith('.txt'))
                .map(f => {
                    const filePath = path.join(cookiesDir, f)
                    const stats = fs.statSync(filePath)
                    return {
                        name: f.replace('.txt', ''),
                        filename: f,
                        lastModified: stats.mtime.toISOString(),
                        size: stats.size
                    }
                })

            return new Response(JSON.stringify(cookieFiles), {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            })
        } catch (error) {
            this.log?.error('Cookie list error:', error)
            return new Response('Failed to list cookies', { status: 500 })
        }
    }

    private async handleCookieView(req: Request, finder: string): Promise<Response> {
        try {
            const cookiesDir = path.join(process.cwd(), 'assets', 'cookies')
            const cookieFile = path.join(cookiesDir, `${finder}.txt`)

            if (!fs.existsSync(cookieFile)) {
                return new Response('Cookie file not found', { status: 404 })
            }

            const content = fs.readFileSync(cookieFile, 'utf-8')
            const stats = fs.statSync(cookieFile)

            return new Response(JSON.stringify({
                name: finder,
                content: content,
                lastModified: stats.mtime.toISOString(),
                size: stats.size
            }), {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            })
        } catch (error) {
            this.log?.error('Cookie view error:', error)
            return new Response('Failed to read cookie', { status: 500 })
        }
    }

    private async handleConfigList(req: Request): Promise<Response> {
        try {
            const crawlers = this.config.crawlers || []

            const crawlerInfo = crawlers.map(crawler => ({
                name: crawler.name,
                type: crawler.type,
                schedule: crawler.cfg_crawler?.cron || null,
                cookieFile: crawler.cfg_crawler?.cookie_file || null,
                enabled: true // 所有配置的crawler默认启用
            }))

            return new Response(JSON.stringify(crawlerInfo), {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            })
        } catch (error) {
            this.log?.error('Config list error:', error)
            return new Response('Failed to list crawlers', { status: 500 })
        }
    }
}
