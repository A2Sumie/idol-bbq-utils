import fs from 'fs'
import path from 'path'
import os from 'os'

export function ensureDirectoryExists(dirPath: string): void {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true })
    }
}

export function initializeCacheDirectories(cacheRoot: string): void {
    const logsDir = path.join(cacheRoot, 'logs')
    const mediaDir = path.join(cacheRoot, 'media')

    ensureDirectoryExists(cacheRoot)
    ensureDirectoryExists(logsDir)
    ensureDirectoryExists(mediaDir)
    ensureDirectoryExists(getBrowserProfileRoot(cacheRoot))
}

export function getCacheRoot(): string {
    return process.env.CACHE_DIR || path.join(os.tmpdir(), 'tweet-forwarder')
}

export function getCookiesRoot(): string {
    return process.env.COOKIES_DIR || path.join(process.cwd(), 'assets', 'cookies')
}

export function getBrowserProfileRoot(cacheRoot?: string): string {
    return process.env.BROWSER_PROFILE_DIR || path.join(getCookiesRoot(), 'browser-profiles')
}
