import { Platform } from '@idol-bbq-utils/spider/types'

type CookieHealthPlatform = 'x' | 'instagram' | 'tiktok' | 'youtube' | 'website' | 'unknown'

const REQUIRED_COOKIE_NAMES: Record<CookieHealthPlatform, Array<string>> = {
    x: ['auth_token', 'ct0'],
    instagram: ['sessionid', 'csrftoken'],
    tiktok: ['ttwid'],
    youtube: [],
    website: [],
    unknown: [],
}

function getRequiredCookieNamesForPlatform(platform: CookieHealthPlatform) {
    return REQUIRED_COOKIE_NAMES[platform] || []
}

function summarizeRequiredCookieNames(platform: CookieHealthPlatform, cookieNames: Array<string>) {
    const cookieNameSet = new Set(cookieNames.filter(Boolean))
    const requiredNames = getRequiredCookieNamesForPlatform(platform)
    return {
        present: requiredNames.filter((name) => cookieNameSet.has(name)),
        missing: requiredNames.filter((name) => !cookieNameSet.has(name)),
    }
}

function inferCookieHealthPlatform(crawler: any): CookieHealthPlatform {
    const candidates = [crawler?.origin, ...(Array.isArray(crawler?.websites) ? crawler.websites : [])]

    for (const candidate of candidates) {
        try {
            const hostname = new URL(candidate).hostname.replace(/^www\./, '').toLowerCase()
            if (hostname === 'x.com' || hostname === 'twitter.com') return 'x'
            if (hostname === 'instagram.com') return 'instagram'
            if (hostname === 'tiktok.com') return 'tiktok'
            if (hostname === 'youtube.com' || hostname === 'youtu.be') return 'youtube'
        } catch {
            continue
        }
    }

    return 'unknown'
}

function toCookieHealthPlatformFromSpiderPlatform(platform: Platform): CookieHealthPlatform {
    if (platform === Platform.X || platform === Platform.Twitter) return 'x'
    if (platform === Platform.Instagram) return 'instagram'
    if (platform === Platform.TikTok) return 'tiktok'
    if (platform === Platform.YouTube) return 'youtube'
    if (platform === Platform.Website) return 'website'
    return 'unknown'
}

function toSpiderPlatformFromCookieHealthPlatform(platform: CookieHealthPlatform): Platform | null {
    if (platform === 'x') return Platform.X
    if (platform === 'instagram') return Platform.Instagram
    if (platform === 'tiktok') return Platform.TikTok
    if (platform === 'youtube') return Platform.YouTube
    if (platform === 'website') return Platform.Website
    return null
}

export {
    getRequiredCookieNamesForPlatform,
    inferCookieHealthPlatform,
    summarizeRequiredCookieNames,
    toCookieHealthPlatformFromSpiderPlatform,
    toSpiderPlatformFromCookieHealthPlatform,
    type CookieHealthPlatform,
}
