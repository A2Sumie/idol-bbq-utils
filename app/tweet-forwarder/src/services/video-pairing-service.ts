import { Platform, type MediaType } from '@idol-bbq-utils/spider/types'
import DB, { type Article, type DBVideoPairing } from '@/db'
import { articleKey, hashValue } from '@/services/outbound-message-service'
import { parseTikTokUrl, resolveTikTokLink } from '@/services/x-tiktok-link-ingest-service'
import fs from 'fs'

const INSTAGRAM_URL_RE = /https?:\/\/(?:www\.)?instagram\.com\/[^\s<>"'，。！？、）)\]}]+/gi
const TIKTOK_URL_RE = /https?:\/\/(?:www\.|vm\.|vt\.)?tiktok\.com\/[^\s<>"'，。！？、）)\]}]+/gi
const DEFAULT_VIDEO_PAIRING_WINDOW_SECONDS = 90 * 60
const DEFAULT_VIDEO_PAIRING_ON_EXPIRY = 'drop'
const BILIBILI_VIDEO_PAIRING_HELD_MODE = 'biliup_pairing_held'
const BILIBILI_VIDEO_PAIRING_MERGED_MODE = 'biliup_pairing_merged'

type MinimalLog = {
    debug?: (...args: any[]) => void
    info?: (...args: any[]) => void
    warn?: (...args: any[]) => void
}

type VideoPairingExpiryAction = 'drop'

type ResolvedVideoPairingConfig = {
    enabled: true
    joinPlatforms: Array<'tiktok' | 'instagram'>
    windowSeconds: number
    onExpiry: VideoPairingExpiryAction
}

type PairingMediaFile = {
    media_type: MediaType
    path: string
    sourceArticleId?: string
    sourceUserId?: string
    content_hash?: string
    size_bytes?: number
    duration_seconds?: number
    sourceUrl?: string
}

type HoldVideoPairingResult = {
    held: boolean
    reason: string
    pairing?: DBVideoPairing
}

type InstagramLinkInfo = {
    originalUrl: string
    resolvedUrl: string
    shortcode?: string
    username?: string
    profileUrl?: string
}

function normalizePlatformName(value: unknown) {
    const normalized = String(value || '')
        .trim()
        .toLowerCase()
        .replace(/[_\s-]+/g, '')
    if (normalized === 'tt') {
        return 'tiktok'
    }
    if (normalized === 'ig' || normalized === 'ins') {
        return 'instagram'
    }
    return normalized
}

function resolveVideoPairingConfig(rawConfig: unknown): ResolvedVideoPairingConfig | null {
    const raw = (rawConfig as any)?.video_pairing
    if (raw === false || !raw || (typeof raw === 'object' && raw.enabled === false)) {
        return null
    }
    const objectConfig = typeof raw === 'object' ? raw : {}
    const platforms = Array.isArray(objectConfig.join_platforms)
        ? objectConfig.join_platforms
              .map((value: unknown) => normalizePlatformName(value))
              .filter((value: string) => value === 'tiktok' || value === 'instagram')
        : ['tiktok', 'instagram']
    const joinPlatforms = Array.from(new Set(platforms)) as Array<'tiktok' | 'instagram'>
    if (joinPlatforms.length === 0) {
        return null
    }
    const windowSeconds = Math.max(
        60,
        Math.floor(Number(objectConfig.window_seconds || DEFAULT_VIDEO_PAIRING_WINDOW_SECONDS)),
    )
    const onExpiry = DEFAULT_VIDEO_PAIRING_ON_EXPIRY
    return {
        enabled: true,
        joinPlatforms,
        windowSeconds,
        onExpiry,
    }
}

function cleanExternalUrl(value: string) {
    return value.replace(/[.,!?;:，。！？、）)\]}]+$/g, '')
}

function extractInstagramLinksFromText(text?: string | null): Array<string> {
    if (!text) {
        return []
    }
    return Array.from(new Set(Array.from(text.matchAll(INSTAGRAM_URL_RE)).map((match) => cleanExternalUrl(match[0]))))
}

function parseInstagramUrl(rawUrl: string): InstagramLinkInfo | null {
    let url: URL
    try {
        url = new URL(rawUrl)
    } catch {
        return null
    }
    const hostname = url.hostname.toLowerCase()
    if (hostname !== 'instagram.com' && hostname !== 'www.instagram.com') {
        return null
    }
    const parts = url.pathname.split('/').filter(Boolean)
    const type = parts[0]
    const shortcode = ['p', 'reel', 'tv'].includes(type || '') ? parts[1] : undefined
    const username =
        type === 'stories' ? parts[1] : !['p', 'reel', 'tv', 'stories'].includes(type || '') ? type : undefined
    const profileUrl = username ? `https://www.instagram.com/${username}` : undefined
    return {
        originalUrl: rawUrl,
        resolvedUrl: rawUrl,
        shortcode,
        username,
        profileUrl,
    }
}

function resolveArticleJoinPlatform(article: Pick<Article, 'platform'>): 'tiktok' | 'instagram' | null {
    if (article.platform === Platform.TikTok) {
        return 'tiktok'
    }
    if (article.platform === Platform.Instagram) {
        return 'instagram'
    }
    return null
}

function isBilibiliVideoPairingHeldResult(value: unknown): boolean {
    if (Array.isArray(value)) {
        return value.some((item) => isBilibiliVideoPairingHeldResult(item))
    }
    return Boolean(value && typeof value === 'object' && (value as any).mode === BILIBILI_VIDEO_PAIRING_HELD_MODE)
}

function normalizeAuthorKey(value?: string | null) {
    return String(value || '')
        .trim()
        .replace(/^@+/, '')
        .toLowerCase()
}

function buildPairingKey(targetId: string, sourceArticleKey: string, joinPlatform: string, targetHint?: string | null) {
    return hashValue({ targetId, sourceArticleKey, joinPlatform, targetHint: targetHint || null })
}

function serializeTeaserMedia(media: Array<PairingMediaFile>) {
    return media
        .filter((item) => item.media_type === 'video' && item.path && fs.existsSync(item.path))
        .map((item) => ({
            media_type: item.media_type,
            path: item.path,
            sourceArticleId: item.sourceArticleId,
            sourceUserId: item.sourceUserId,
            content_hash: item.content_hash,
            size_bytes: item.size_bytes,
            duration_seconds: item.duration_seconds,
            sourceUrl: item.sourceUrl,
        }))
}

function deserializeTeaserMedia(pairing: Pick<DBVideoPairing, 'teaser_media'>): Array<PairingMediaFile> {
    const raw = pairing.teaser_media
    if (!Array.isArray(raw)) {
        return []
    }
    return raw
        .map((item: any) => ({
            media_type: String(item?.media_type || '') as MediaType,
            path: String(item?.path || ''),
            sourceArticleId: item?.sourceArticleId ? String(item.sourceArticleId) : undefined,
            sourceUserId: item?.sourceUserId ? String(item.sourceUserId) : undefined,
            content_hash: item?.content_hash ? String(item.content_hash) : undefined,
            size_bytes: Number.isFinite(Number(item?.size_bytes)) ? Number(item.size_bytes) : undefined,
            duration_seconds: Number.isFinite(Number(item?.duration_seconds))
                ? Number(item.duration_seconds)
                : undefined,
            sourceUrl: item?.sourceUrl ? String(item.sourceUrl) : undefined,
        }))
        .filter((item) => item.media_type === 'video' && item.path && fs.existsSync(item.path))
}

async function resolveFirstPairingTarget(
    article: Article,
    config: ResolvedVideoPairingConfig,
    log?: MinimalLog,
): Promise<
    | {
          joinPlatform: 'tiktok'
          targetVideoId?: string
          targetProfileUrl?: string
          targetUserId?: string
          targetUsername?: string
      }
    | {
          joinPlatform: 'instagram'
          targetVideoId?: string
          targetProfileUrl?: string
          targetUserId?: string
          targetUsername?: string
      }
    | null
> {
    const content = article.content || ''
    if (config.joinPlatforms.includes('tiktok')) {
        const tiktokLinks = Array.from(content.matchAll(TIKTOK_URL_RE)).map((match) => cleanExternalUrl(match[0]))
        for (const link of tiktokLinks) {
            const resolved = await resolveTikTokLink(link).catch((error) => {
                log?.warn?.(`Video pairing TikTok link resolve failed for ${article.a_id}: ${error}`)
                return parseTikTokUrl(link)
            })
            if (resolved?.profileUrl || resolved?.videoId) {
                return {
                    joinPlatform: 'tiktok',
                    targetVideoId: resolved.videoId,
                    targetProfileUrl: resolved.profileUrl,
                    targetUserId: resolved.username,
                    targetUsername: resolved.username,
                }
            }
        }
    }
    if (config.joinPlatforms.includes('instagram')) {
        for (const link of extractInstagramLinksFromText(content)) {
            const resolved = parseInstagramUrl(link)
            if (resolved?.profileUrl || resolved?.shortcode) {
                return {
                    joinPlatform: 'instagram',
                    targetVideoId: resolved.shortcode,
                    targetProfileUrl: resolved.profileUrl,
                    targetUserId: resolved.username,
                    targetUsername: resolved.username,
                }
            }
        }
    }
    return null
}

async function holdBilibiliVideoPairingTeaser(options: {
    targetId: string
    article: Article
    media: Array<PairingMediaFile>
    config: ResolvedVideoPairingConfig | null
    log?: MinimalLog
}): Promise<HoldVideoPairingResult> {
    const { targetId, article, media, config, log } = options
    if (!config || article.platform !== Platform.X) {
        return { held: false, reason: 'not_eligible' }
    }
    const teaserMedia = serializeTeaserMedia(media)
    if (teaserMedia.length === 0) {
        return { held: false, reason: 'no_teaser_video' }
    }
    const target = await resolveFirstPairingTarget(article, config, log)
    if (!target) {
        return { held: false, reason: 'no_join_link' }
    }
    const sourceArticleKey = articleKey(article as any)
    const expiresAt = Math.floor(Date.now() / 1000) + config.windowSeconds
    const pairing = await DB.VideoPairing.upsertPending({
        pairing_key: buildPairingKey(
            targetId,
            sourceArticleKey,
            target.joinPlatform,
            target.targetVideoId || target.targetProfileUrl,
        ),
        target_id: targetId,
        source_article_key: sourceArticleKey,
        source_article_id: (article as any).id || null,
        source_platform: String(article.platform),
        source_a_id: article.a_id,
        source_u_id: article.u_id,
        source_username: article.username,
        source_created_at: article.created_at,
        join_platform: target.joinPlatform,
        target_video_id: target.targetVideoId,
        target_profile_url: target.targetProfileUrl,
        target_u_id: normalizeAuthorKey(target.targetUserId),
        target_username: target.targetUsername,
        teaser_media: teaserMedia,
        expires_at: expiresAt,
    })
    if (DB.VideoPairing.isTerminalStatus(pairing.record.status)) {
        return { held: true, reason: `pairing_${pairing.record.status}`, pairing: pairing.record }
    }
    log?.info?.(
        `Holding Bilibili teaser ${article.a_id} for ${target.joinPlatform} pairing until ${new Date(expiresAt * 1000).toISOString()}`,
    )
    return { held: true, reason: pairing.created ? 'created' : 'refreshed', pairing: pairing.record }
}

async function findBilibiliPendingPairingForMainVideo(options: {
    targetId: string
    article: Article
    config: ResolvedVideoPairingConfig | null
}): Promise<DBVideoPairing | null> {
    const joinPlatform = resolveArticleJoinPlatform(options.article)
    if (!options.config || !joinPlatform || !options.config.joinPlatforms.includes(joinPlatform)) {
        return null
    }
    return await DB.VideoPairing.findPendingForMainVideo({
        target_id: options.targetId,
        join_platform: joinPlatform,
        video_id: options.article.a_id,
        u_id: normalizeAuthorKey(options.article.u_id),
        username: normalizeAuthorKey(options.article.username),
    })
}

async function markExpiredVideoPairings(log?: MinimalLog) {
    const expired = await DB.VideoPairing.listExpired()
    for (const pairing of expired) {
        await DB.VideoPairing.markStatus(pairing.id, DB.VideoPairing.STATUS.Dropped, {
            reason: 'expired_drop',
            on_expiry: DEFAULT_VIDEO_PAIRING_ON_EXPIRY,
        })
        log?.info?.(`Dropped expired Bilibili video pairing ${pairing.source_article_key} for ${pairing.target_id}`)
    }
    return expired.length
}

export {
    BILIBILI_VIDEO_PAIRING_HELD_MODE,
    BILIBILI_VIDEO_PAIRING_MERGED_MODE,
    deserializeTeaserMedia,
    findBilibiliPendingPairingForMainVideo,
    holdBilibiliVideoPairingTeaser,
    isBilibiliVideoPairingHeldResult,
    markExpiredVideoPairings,
    resolveVideoPairingConfig,
    type ResolvedVideoPairingConfig,
}
