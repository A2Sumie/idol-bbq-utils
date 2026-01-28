import { BaseCompatibleModel } from '@/utils/base'
import type { AppConfig } from '@/types'
import { Logger } from '@idol-bbq-utils/log'
import { spiderRegistry } from '@idol-bbq-utils/spider'
import fs from 'fs'
import path from 'path'
import YAML from 'yaml'

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
                // Standardizing to plural /api/cookies to match RESTful practices and frontend
                if (req.method === 'POST' && url.pathname === '/api/cookies') {
                    return this.handleCookieUpdate(req)
                }

                if (req.method === 'DELETE' && url.pathname === '/api/cookies') {
                    return this.handleCookieDelete(req)
                }

                // Add POST delete to alias DELETE method for proxy compatibility
                if (req.method === 'POST' && url.pathname === '/api/cookies/delete') {
                    return this.handleCookieDelete(req)
                }

                if (req.method === 'GET' && url.pathname === '/api/cookies') {
                    return this.handleCookieList(req)
                }

                if (req.method === 'GET' && url.pathname.startsWith('/api/cookies/')) {
                    const finder = url.pathname.split('/api/cookies/')[1]
                    return this.handleCookieView(req, finder)
                }

                // Config management endpoints
                if (req.method === 'GET' && url.pathname === '/api/config') {
                    return this.handleConfigGet(req)
                }

                if (req.method === 'GET' && url.pathname === '/api/config/crawlers') {
                    return this.handleConfigList(req)
                }

                if (req.method === 'POST' && url.pathname === '/api/config/update') {
                    return this.handleConfigUpdate(req)
                }

                if (req.method === 'POST' && url.pathname === '/api/server/restart') {
                    return this.handleServerRestart(req)
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
                .filter(f => f.endsWith('.txt') && !f.startsWith('.'))
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

    private async handleCookieDelete(req: Request): Promise<Response> {
        try {
            const body = await req.json() as { filenames: string[] }
            const { filenames } = body

            if (!filenames || !Array.isArray(filenames) || filenames.length === 0) {
                return new Response('No filenames provided', { status: 400 })
            }

            const cookiesDir = path.join(process.cwd(), 'assets', 'cookies')
            let deletedCount = 0
            let errors: string[] = []

            for (const filename of filenames) {
                // Security check: prevent directory traversal
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
                    } catch (e: any) {
                        errors.push(`Failed to delete ${filename}: ${e.message}`)
                    }
                }
            }

            this.log?.info(`Deleted ${deletedCount} cookies. Errors: ${errors.length}`)

            return new Response(JSON.stringify({
                success: true,
                deleted: deletedCount,
                errors
            }), {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            })
        } catch (error) {
            this.log?.error('Cookie delete error:', error)
            return new Response('Failed to delete cookies', { status: 500 })
        }
    }

    private async handleConfigList(req: Request): Promise<Response> {
        try {
            const crawlers = this.config.crawlers || []

            const crawlerInfo = crawlers.map(crawler => ({
                name: crawler.name,
                type: crawler.task_type,
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

    private async handleConfigUpdate(req: Request): Promise<Response> {
        try {
            const body = await req.json() as AppConfig
            this.log?.info('Received config update request', { keys: Object.keys(body) })

            // Basic validation
            if (!body || typeof body !== 'object') {
                return new Response('Invalid config format', { status: 400 })
            }

            const configPath = path.join(process.cwd(), 'config.yaml')

            // Read existing config first to preserve comments/structure if possible (though generic YAML stringify might lose comments)
            // For now, we just overwrite because maintaining comments in YAML via JS is hard without specialized libs.
            // But we should backup!
            if (fs.existsSync(configPath)) {
                fs.copyFileSync(configPath, `${configPath}.bak`)
            }

            const yamlStr = YAML.stringify(body)
            fs.writeFileSync(configPath, yamlStr, 'utf8')

            this.log?.info('Configuration updated via API')

            // Update internal config state reference? 
            // Might be dangerous if other components hold references. 
            // Restart is safest, but we can update this.config slightly for read-after-write consistency if needed.
            this.config = body

            return new Response(JSON.stringify({ success: true, message: 'Configuration saved. Restart server to apply changes.' }), {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            })
        } catch (error) {
            this.log?.error('Config update error:', error)
            return new Response(`Failed to update config: ${error instanceof Error ? error.message : String(error)}`, { status: 500 })
        }
    }

    private async handleServerRestart(req: Request): Promise<Response> {
        this.log?.warn('Server restart requested via API')

        // Respond first, then exit
        setTimeout(() => {
            this.log?.info('Exiting process for restart...')
            process.exit(0) // Docker/PM2 should restart it
        }, 1000)

        return new Response(JSON.stringify({ success: true, message: 'Server restarting...' }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
        })
    }

    private async handleConfigGet(req: Request): Promise<Response> {
        try {
            this.log?.debug('Serving config', { keys: Object.keys(this.config) })
            return new Response(JSON.stringify(this.config), {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            })
        } catch (error) {
            this.log?.error('Config get error:', error)
            return new Response('Failed to get config', { status: 500 })
        }
    }
}

