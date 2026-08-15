import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'
import YAML from 'yaml'

const CONFIG_PATH = process.env.IDOL_BBQ_CONFIG_PATH || '/app/config.yaml'
const API_BASE = process.env.IDOL_BBQ_API_BASE || 'http://127.0.0.1:3000'
const YT_DLP_PATH = process.env.YT_DLP_PATH || '/app/tools/bin/yt-dlp'
const YT_COOKIE_FILE = process.env.YT_COOKIE_FILE || '/app/assets/cookies/ycookies.txt'
const STARTUP_COOKIE_MAINTENANCE_ENABLED = String(process.env.IDOL_BBQ_STARTUP_COOKIE_MAINTENANCE || '1').trim() !== '0'

function log(message: string) {
    const line = `[startup-cookie-maintenance] ${new Date().toISOString()} ${message}`
    process.stdout.write(line + '\n')
}

function loadConfig() {
    const config = YAML.parse(fs.readFileSync(CONFIG_PATH, 'utf8')) || {}
    return {
        crawlers: Array.isArray(config.crawlers) ? config.crawlers : [],
        api: config.api || {},
        cfgCrawler: config.cfg_crawler || {},
    }
}

async function waitForApiReady(timeoutMs: number) {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
        try {
            const response = await fetch(`${API_BASE}/api/runtime/status`, {
                signal: AbortSignal.timeout(8000),
            })
            // Any HTTP response (401/500) is not "ready": the API must answer 2xx.
            if (response.ok) {
                return true
            }
        } catch {
            // not up yet
        }
        await new Promise((resolve) => setTimeout(resolve, 3000))
    }
    return false
}

function discoverSyncCrawlers(config: ReturnType<typeof loadConfig>): Array<string> {
    const seen = new Set<string>()
    const names: Array<string> = []
    for (const crawler of config.crawlers) {
        const name = crawler?.name
        if (!name) {
            continue
        }
        const cfg = crawler?.cfg_crawler || {}
        const cookieFile = cfg.cookie_file || config.cfgCrawler.cookie_file
        const sessionProfile = cfg.session_profile || config.cfgCrawler.session_profile
        if (!cookieFile || !sessionProfile) {
            continue
        }
        if (String(cookieFile).endsWith('/ycookies.txt')) {
            continue
        }
        const key = `${cookieFile}\u0000${sessionProfile}`
        if (seen.has(key)) {
            continue
        }
        seen.add(key)
        names.push(name)
    }
    return names
}

async function syncCrawlerCookies(name: string, secret: string) {
    const response = await fetch(`${API_BASE}/api/cookies/sync`, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${secret}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ crawlerName: name }),
        signal: AbortSignal.timeout(60000),
    })
    if (!response.ok) {
        throw new Error(`sync failed http=${response.status}: ${(await response.text()).slice(0, 200)}`)
    }
    const body = await response.json()
    log(`synced ${name}: count=${body.count} profile=${body.sessionProfile}`)
}

function runYoutubeKeepalive() {
    return new Promise<boolean>((resolve) => {
        if (!fs.existsSync(YT_COOKIE_FILE) || !fs.existsSync(YT_DLP_PATH)) {
            log('YouTube keepalive skipped: jar or yt-dlp missing')
            resolve(false)
            return
        }
        const args = [
            '--cookies',
            YT_COOKIE_FILE,
            '--simulate',
            '--playlist-items',
            '1',
            '--print',
            '%(id)s',
            'https://www.youtube.com/@sallyamakiofficial',
        ]
        const child = spawn(YT_DLP_PATH, args, { stdio: 'ignore' })
        const timer = setTimeout(() => child.kill('SIGKILL'), 120000)
        child.on('close', (code) => {
            clearTimeout(timer)
            log(`YouTube keepalive finished rc=${code}`)
            resolve(code === 0)
        })
        child.on('error', (error) => {
            clearTimeout(timer)
            log(`YouTube keepalive spawn error: ${error.message}`)
            resolve(false)
        })
    })
}

async function main() {
    if (!STARTUP_COOKIE_MAINTENANCE_ENABLED) {
        log('disabled by IDOL_BBQ_STARTUP_COOKIE_MAINTENANCE=0')
        return
    }
    const config = loadConfig()
    const secret = String(config.api.secret || process.env.API_SECRET || '').trim()
    if (!secret) {
        log('API secret unavailable; skipping startup cookie maintenance')
        return
    }
    const apiReady = await waitForApiReady(90000)
    if (!apiReady) {
        log('API did not become ready in time; skipping startup cookie maintenance')
        return
    }
    log('API ready; starting startup cookie maintenance')
    const crawlers = discoverSyncCrawlers(config)
    let failures = 0
    for (const name of crawlers) {
        try {
            await syncCrawlerCookies(name, secret)
        } catch (error) {
            failures += 1
            log(`sync failed for ${name}: ${error instanceof Error ? error.message : String(error)}`)
        }
    }
    const youtubeKeepaliveOk = await runYoutubeKeepalive()
    if (!youtubeKeepaliveOk) {
        failures += 1
    }
    log(`startup cookie maintenance done: synced=${crawlers.length} failures=${failures}`)
    process.exit(failures > 0 ? 1 : 0)
}

main().catch((error) => {
    log(`startup cookie maintenance crashed: ${error instanceof Error ? error.message : String(error)}`)
    process.exit(1)
})
