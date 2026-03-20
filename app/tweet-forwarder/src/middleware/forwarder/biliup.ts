import { CACHE_DIR_ROOT } from '@/config'
import type { Article } from '@/db'
import { formatPlatformTag } from '@/services/render-service'
import type { BiliupVideoUploadConfig } from '@/types/forwarder'
import type { Logger } from '@idol-bbq-utils/log'
import type { BrowserMode } from '@idol-bbq-utils/spider'
import { Platform, type MediaType } from '@idol-bbq-utils/spider/types'
import { spawn } from 'child_process'
import { createHash } from 'crypto'
import fs from 'fs'
import path from 'path'

const DEFAULT_BILIUP_TID = 171
const DEFAULT_BILIUP_THREADS = 3
const DEFAULT_BILIUP_SUBMIT_API = 'web'
const DEFAULT_BILIUP_LINE = 'AUTO'
const DEFAULT_BILIUP_WORKING_DIR = path.join(CACHE_DIR_ROOT, 'media', 'biliup')
const DEFAULT_BILIUP_EXCLUDED_UIDS = ['22/7:radio', '22/7:movie']
const DEFAULT_BILIUP_COOKIE_SYNC_URL = 'https://www.bilibili.com'
const DEFAULT_BILIUP_COLLISION_PART_TITLE = '###'
const DEFAULT_BILIUP_COLLISION_MAIN_PART_TITLE = '正片'
const DEFAULT_BILIUP_COLLISION_PLACEHOLDER_DURATION_SECONDS = 2
const DEFAULT_BILIUP_COLLISION_PLACEHOLDER_WIDTH = 1920
const DEFAULT_BILIUP_COLLISION_PLACEHOLDER_HEIGHT = 1080
const DEFAULT_BILIUP_COLLISION_PLACEHOLDER_FPS = 30
const DEFAULT_BILIUP_COLLISION_PLACEHOLDER_BACKGROUND_COLOR = '#d1e5fc'
const DEFAULT_BILIUP_COLLISION_PLACEHOLDER_VIDEO = path.resolve(
    process.cwd(),
    'assets',
    'branding',
    'live-player-background-collision-pad-7s.mp4',
)
const DEFAULT_BILIUP_COLLISION_PLACEHOLDER_IMAGE = path.resolve(
    process.cwd(),
    'assets',
    'branding',
    'live-player-background.png',
)
const DEFAULT_BILIUP_METADATA_TIMEZONE = 'Asia/Tokyo'

type TemplateContext = Record<string, string>

type MediaFile = {
    media_type: MediaType
    path: string
}

type PreparedUploadVideoPart = {
    sourcePath: string
    stagedPath: string
    partTitle?: string
}

interface ResolvedBiliupBrowserCookieSyncConfig {
    enabled: true
    bun_path: string
    script_path: string
    session_profile: string
    url: string
    browser_mode: BrowserMode
    user_agent?: string
    locale?: string
    timezone?: string
}

interface ResolvedBiliupMetadataTemplatesConfig {
    title?: string
    description?: string
}

interface ResolvedBiliupCollisionPlaceholderPartConfig {
    enabled: true
    video_path?: string
    image_path: string
    title: string
    duration_seconds: number
    width: number
    height: number
    fps: number
    ffmpeg_path: string
    background_color: string
}

interface ResolvedBiliupVideoUploadConfig {
    enabled: boolean
    python_path: string
    helper_path: string
    working_dir: string
    metadata_timezone: string
    cookie_file?: string
    browser_cookie_sync?: ResolvedBiliupBrowserCookieSyncConfig
    submit_api: 'web'
    line: 'AUTO' | 'bda' | 'bda2' | 'ws' | 'qn' | 'bldsa' | 'tx' | 'txa'
    tid: number
    threads: number
    copyright: 1 | 2
    tags: Array<string>
    exclude_uids: Array<string>
    metadata_templates?: ResolvedBiliupMetadataTemplatesConfig
    collision_placeholder_part?: ResolvedBiliupCollisionPlaceholderPartConfig
}

interface BiliupUploadCandidate {
    title: string
    description: string
    sourceUrl: string
    coverPath?: string
    videoPaths: Array<string>
    config: ResolvedBiliupVideoUploadConfig
}

type BiliupCookieDocument = {
    cookie_info: Record<string, unknown> & {
        cookies: Array<Record<string, unknown> & { name: string; value: string }>
    }
    sso: Array<unknown>
    token_info: Record<string, unknown>
    platform: unknown
} & Record<string, unknown>

function resolveExistingPath(candidates: Array<string | undefined>, fallback: string) {
    for (const candidate of candidates) {
        if (candidate && fs.existsSync(candidate)) {
            return candidate
        }
    }
    return fallback
}

function defaultPythonPath() {
    return resolveExistingPath(
        [process.env.BILIUP_PYTHON_PATH, '/app/tools/bin/biliup-python', '/usr/bin/python3', 'python3'],
        'python3',
    )
}

function defaultHelperPath() {
    return resolveExistingPath(
        [
            process.env.BILIUP_HELPER_PATH,
            '/app/tools/biliup-upload.py',
            path.resolve(process.cwd(), 'app/tweet-forwarder/scripts/biliup-upload.py'),
        ],
        '/app/tools/biliup-upload.py',
    )
}

function defaultBunPath() {
    return resolveExistingPath([process.env.BUN_PATH, '/usr/local/bin/bun', '/usr/bin/bun', 'bun'], 'bun')
}

function defaultBrowserCookieSyncScriptPath() {
    return resolveExistingPath(
        [
            process.env.BILIUP_BROWSER_COOKIE_SYNC_SCRIPT,
            '/app/tools/export-biliup-browser-cookies.js',
            path.resolve(process.cwd(), 'app/tweet-forwarder/scripts/export-biliup-browser-cookies.ts'),
        ],
        '/app/tools/export-biliup-browser-cookies.js',
    )
}

function normalizeTag(tag: string) {
    return tag.replace(/[\r\n,]+/g, ' ').replace(/\s+/g, ' ').trim()
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function uniqueStrings(values: Array<string>) {
    return Array.from(new Set(values.filter(Boolean)))
}

function truncateText(value: string, maxChars: number) {
    const chars = Array.from(value)
    if (chars.length <= maxChars) {
        return value
    }
    return `${chars.slice(0, Math.max(0, maxChars - 3)).join('')}...`
}

function defaultFfmpegPath() {
    return resolveExistingPath([process.env.FFMPEG_PATH, '/usr/bin/ffmpeg', '/usr/local/bin/ffmpeg', 'ffmpeg'], 'ffmpeg')
}

function normalizeTextBlock(value: string | null | undefined) {
    return String(value || '')
        .replace(/\r\n/g, '\n')
        .replace(/\u00a0/g, ' ')
        .replace(/[ \t]+\n/g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim()
}

function collectTextBlocks(article: Pick<Article, 'content'>, texts: string[]) {
    const seen = new Set<string>()
    const blocks: string[] = []
    for (const candidate of [article.content, ...texts]) {
        const normalized = normalizeTextBlock(candidate)
        if (!normalized || seen.has(normalized)) {
            continue
        }
        seen.add(normalized)
        blocks.push(normalized)
    }
    return blocks
}

function resolveDisplayName(article: Pick<Article, 'username' | 'u_id'>) {
    return String(article.username || article.u_id || '').trim() || 'Unknown'
}

function resolveTypeLabel(article: Pick<Article, 'platform' | 'type'>) {
    if (article.platform === Platform.Instagram) {
        return article.type === 'story' ? 'Story' : '投稿'
    }
    if (article.platform === Platform.TikTok) {
        return '视频'
    }
    if (article.platform === Platform.YouTube) {
        return article.type === 'shorts' ? 'Shorts' : '视频'
    }
    if (article.platform === Platform.X) {
        return '视频'
    }
    if (article.platform === Platform.Website) {
        return '内容'
    }
    return ''
}

function resolvePlatformLabel(article: Pick<Article, 'platform' | 'username' | 'a_id'>) {
    return formatPlatformTag(article).split(' ')[0] || 'Unknown'
}

function resolvePlatformTypeLabel(article: Pick<Article, 'platform' | 'type' | 'username' | 'a_id'>) {
    const platformLabel = resolvePlatformLabel(article)
    const typeLabel = resolveTypeLabel(article)
    if (!typeLabel) {
        return platformLabel
    }
    if (/^[A-Za-z]/.test(typeLabel)) {
        return `${platformLabel} ${typeLabel}`
    }
    return `${platformLabel}${typeLabel}`
}

function formatDateTimeParts(timestampSeconds: number, timeZone: string) {
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hourCycle: 'h23',
    })
        .formatToParts(new Date(timestampSeconds * 1000))
        .reduce<Record<string, string>>((acc, part) => {
            if (part.type !== 'literal') {
                acc[part.type] = part.value
            }
            return acc
        }, {})

    const date = `${parts.year}-${parts.month}-${parts.day}`
    const time = `${parts.hour}:${parts.minute}`
    return {
        date,
        time,
        datetime: `${date} ${time}`,
    }
}

function buildTemplateContext(
    article: Pick<Article, 'content' | 'platform' | 'username' | 'u_id' | 'a_id' | 'created_at' | 'url' | 'type'>,
    texts: string[],
    timeZone: string,
): TemplateContext {
    const blocks = collectTextBlocks(article, texts)
    const primaryLine = blocks
        .flatMap((value) => value.split('\n'))
        .map((line) => line.trim())
        .find(Boolean)
        || ''
    const dateTime = formatDateTimeParts(article.created_at, timeZone)
    const displayName = resolveDisplayName(article)
    const summary = primaryLine || dateTime.datetime
    const body = blocks[0] || ''

    return {
        article_id: article.a_id,
        body,
        body_or_summary: body || summary,
        date: dateTime.date,
        datetime: dateTime.datetime,
        display_name: displayName,
        platform_label: resolvePlatformLabel(article),
        platform_type_label: resolvePlatformTypeLabel(article),
        summary,
        time: dateTime.time,
        type_label: resolveTypeLabel(article),
        url: String(article.url || '').trim(),
        user_id: String(article.u_id || '').trim(),
        username: String(article.username || '').trim(),
    }
}

function renderTemplate(template: string, context: TemplateContext) {
    return template.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_, key: string) => context[key] || '')
}

function cleanupTemplateOutput(value: string) {
    return value
        .replace(/[ \t]+\n/g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim()
}

function resolveDefaultTitleTemplate(article: Pick<Article, 'platform' | 'type'>) {
    if (article.platform === Platform.YouTube && article.type !== 'shorts') {
        return '{{summary}}'
    }
    return '【{{platform_type_label}}】{{display_name}} {{summary}}'
}

function deriveTitle(
    article: Pick<Article, 'content' | 'platform' | 'username' | 'u_id' | 'a_id' | 'created_at' | 'url' | 'type'>,
    texts: string[],
    timeZone: string,
    template?: string,
) {
    const context = buildTemplateContext(article, texts, timeZone)
    const rendered = cleanupTemplateOutput(renderTemplate(template || resolveDefaultTitleTemplate(article), context))
    const fallback = context.summary || `${formatPlatformTag(article)} ${context.datetime}` || article.a_id
    return truncateText(rendered || fallback, 80)
}

function deriveDescription(
    article: Pick<Article, 'content' | 'platform' | 'username' | 'u_id' | 'a_id' | 'created_at' | 'url' | 'type'>,
    texts: string[],
    timeZone: string,
    template?: string,
) {
    const context = buildTemplateContext(article, texts, timeZone)
    if (template) {
        return cleanupTemplateOutput(renderTemplate(template, context))
    }

    const sections = [
        context.body_or_summary,
        `来源平台: ${context.platform_type_label}`,
        `来源账号: ${context.display_name}`,
        context.user_id ? `账号标识: ${context.user_id}` : '',
        `发布时间: ${context.datetime}`,
        context.url ? `原链接: ${context.url}` : '',
    ]
    return cleanupTemplateOutput(sections.filter(Boolean).join('\n'))
}

function sanitizeFileStem(value: string, fallback: string) {
    const normalized = value
        .replace(/[<>:"/\\|?*\u0000-\u001F]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
    return truncateText(normalized || fallback, 64)
}

function resolveCollisionPlaceholderImagePath(candidate?: string) {
    return resolveConfiguredPath(candidate) || DEFAULT_BILIUP_COLLISION_PLACEHOLDER_IMAGE
}

function resolveCollisionPlaceholderVideoPath(candidate?: string) {
    const configured = resolveConfiguredPath(candidate)
    if (configured) {
        return configured
    }
    return fs.existsSync(DEFAULT_BILIUP_COLLISION_PLACEHOLDER_VIDEO) ? DEFAULT_BILIUP_COLLISION_PLACEHOLDER_VIDEO : undefined
}

function resolveMetadataTemplatesConfig(
    config?: NonNullable<BiliupVideoUploadConfig['metadata_templates']>,
): ResolvedBiliupMetadataTemplatesConfig | undefined {
    if (!config) {
        return undefined
    }

    const title = normalizeTextBlock(config.title)
    const description = normalizeTextBlock(config.description)
    if (!title && !description) {
        return undefined
    }

    return {
        title: title || undefined,
        description: description || undefined,
    }
}

function resolveCollisionPlaceholderPartConfig(
    config?: NonNullable<BiliupVideoUploadConfig['collision_placeholder_part']>,
): ResolvedBiliupCollisionPlaceholderPartConfig | undefined {
    if (!config?.enabled) {
        return undefined
    }

    return {
        enabled: true,
        video_path: resolveCollisionPlaceholderVideoPath(config.video_path),
        image_path: resolveCollisionPlaceholderImagePath(config.image_path),
        title: normalizeTextBlock(config.title) || DEFAULT_BILIUP_COLLISION_PART_TITLE,
        duration_seconds: Math.max(1, Number(config.duration_seconds || DEFAULT_BILIUP_COLLISION_PLACEHOLDER_DURATION_SECONDS)),
        width: Math.max(320, Math.floor(Number(config.width || DEFAULT_BILIUP_COLLISION_PLACEHOLDER_WIDTH))),
        height: Math.max(240, Math.floor(Number(config.height || DEFAULT_BILIUP_COLLISION_PLACEHOLDER_HEIGHT))),
        fps: Math.max(1, Math.floor(Number(config.fps || DEFAULT_BILIUP_COLLISION_PLACEHOLDER_FPS))),
        ffmpeg_path: config.ffmpeg_path || defaultFfmpegPath(),
        background_color: normalizeTextBlock(config.background_color) || DEFAULT_BILIUP_COLLISION_PLACEHOLDER_BACKGROUND_COLOR,
    }
}

function deriveTags(
    article: Pick<Article, 'platform' | 'username'>,
    configuredTags: Array<string>,
) {
    const platformTag = article.platform === Platform.YouTube
        ? 'YouTube'
        : article.platform === Platform.Instagram
            ? 'Instagram'
            : article.platform === Platform.TikTok
                ? 'TikTok'
                : article.platform === Platform.Website
                    ? '官网'
                    : '社媒'
    return uniqueStrings(['22/7', platformTag, article.username || '', ...configuredTags].map(normalizeTag)).filter(Boolean)
}

function normalizeBiliupCookieDocument(document: unknown): BiliupCookieDocument {
    if (!isRecord(document)) {
        throw new Error('biliup cookie document must be a JSON object')
    }
    if (!isRecord(document.cookie_info) || !Array.isArray(document.cookie_info.cookies)) {
        throw new Error('biliup cookie document must contain cookie_info.cookies')
    }

    const cookies = document.cookie_info.cookies
        .map((cookie) => {
            if (!isRecord(cookie)) {
                return null
            }
            const name = typeof cookie.name === 'string' ? cookie.name.trim() : ''
            const value = typeof cookie.value === 'string' ? cookie.value : ''
            if (!name || !value) {
                return null
            }
            return {
                ...cookie,
                name,
                value,
            }
        })
        .filter((cookie): cookie is Record<string, unknown> & { name: string; value: string } => Boolean(cookie))

    if (cookies.length === 0) {
        throw new Error('biliup cookie document does not contain any usable cookies')
    }

    return {
        ...document,
        cookie_info: {
            ...document.cookie_info,
            cookies,
        },
        sso: Array.isArray(document.sso) ? document.sso : [],
        token_info: isRecord(document.token_info)
            ? document.token_info
            : {
                  access_token: '',
                  expires_in: 0,
                  mid: 0,
                  refresh_token: '',
              },
        platform: document.platform ?? null,
    }
}

function buildCookieDocument(sessdata: string, bili_jct: string) {
    return normalizeBiliupCookieDocument({
        cookie_info: {
            cookies: [
                {
                    name: 'SESSDATA',
                    value: sessdata,
                },
                {
                    name: 'bili_jct',
                    value: bili_jct,
                },
            ],
        },
        sso: [],
        token_info: {
            access_token: '',
            expires_in: 0,
            mid: 0,
            refresh_token: '',
        },
        platform: null,
    })
}

function resolveConfiguredPath(candidate?: string) {
    if (!candidate) {
        return undefined
    }
    return path.isAbsolute(candidate) ? candidate : path.resolve(process.cwd(), candidate)
}

function normalizeBrowserMode(value?: BrowserMode) {
    return value === 'headed' || value === 'headed-xvfb' || value === 'headless' ? value : 'headless'
}

function resolveBrowserCookieSyncConfig(
    config?: NonNullable<BiliupVideoUploadConfig['browser_cookie_sync']>,
): ResolvedBiliupBrowserCookieSyncConfig | undefined {
    if (!config?.enabled) {
        return undefined
    }

    const session_profile = String(config.session_profile || '').trim()
    if (!session_profile) {
        throw new Error('biliup video_upload.browser_cookie_sync.session_profile is required when enabled')
    }

    return {
        enabled: true,
        bun_path: config.bun_path || defaultBunPath(),
        script_path: resolveConfiguredPath(config.script_path) || defaultBrowserCookieSyncScriptPath(),
        session_profile,
        url: config.url || DEFAULT_BILIUP_COOKIE_SYNC_URL,
        browser_mode: normalizeBrowserMode(config.browser_mode),
        user_agent: config.user_agent,
        locale: config.locale,
        timezone: config.timezone,
    }
}

function resolveVideoUploadConfig(config?: BiliupVideoUploadConfig): ResolvedBiliupVideoUploadConfig | null {
    if (!config?.enabled) {
        return null
    }
    return {
        enabled: true,
        python_path: config.python_path || defaultPythonPath(),
        helper_path: config.helper_path || defaultHelperPath(),
        working_dir: config.working_dir || DEFAULT_BILIUP_WORKING_DIR,
        metadata_timezone: normalizeTextBlock(config.metadata_timezone) || DEFAULT_BILIUP_METADATA_TIMEZONE,
        cookie_file: resolveConfiguredPath(config.cookie_file),
        browser_cookie_sync: resolveBrowserCookieSyncConfig(config.browser_cookie_sync),
        submit_api: config.submit_api === 'web' ? config.submit_api : DEFAULT_BILIUP_SUBMIT_API,
        line: config.line || DEFAULT_BILIUP_LINE,
        tid: Number(config.tid || DEFAULT_BILIUP_TID),
        threads: Math.max(1, Number(config.threads || DEFAULT_BILIUP_THREADS)),
        copyright: config.copyright === 1 ? 1 : 2,
        tags: uniqueStrings((config.tags || []).map(normalizeTag)),
        exclude_uids: uniqueStrings([...(config.exclude_uids || []), ...DEFAULT_BILIUP_EXCLUDED_UIDS]),
        metadata_templates: resolveMetadataTemplatesConfig(config.metadata_templates),
        collision_placeholder_part: resolveCollisionPlaceholderPartConfig(config.collision_placeholder_part),
    }
}

function buildBiliupUploadCandidate(
    article: Article | undefined,
    texts: string[],
    media: Array<MediaFile>,
    config?: BiliupVideoUploadConfig,
): BiliupUploadCandidate | null {
    const resolvedConfig = resolveVideoUploadConfig(config)
    if (!resolvedConfig || !article) {
        return null
    }
    if (article.platform === Platform.Website && resolvedConfig.exclude_uids.includes(article.u_id)) {
        return null
    }

    const videoPaths = uniqueStrings(
        media.filter((item) => item.media_type === 'video').map((item) => item.path),
    )
    if (videoPaths.length === 0) {
        return null
    }

    const coverPath = media.find((item) => item.media_type === 'photo' || item.media_type === 'video_thumbnail')?.path
    return {
        title: deriveTitle(article, texts, resolvedConfig.metadata_timezone, resolvedConfig.metadata_templates?.title),
        description: deriveDescription(article, texts, resolvedConfig.metadata_timezone, resolvedConfig.metadata_templates?.description),
        sourceUrl: article.url,
        coverPath,
        videoPaths,
        config: {
            ...resolvedConfig,
            tags: deriveTags(article, resolvedConfig.tags),
        },
    }
}

async function runBrowserCookieSync(config: ResolvedBiliupVideoUploadConfig, log?: Logger) {
    const syncConfig = config.browser_cookie_sync
    if (!syncConfig || !config.cookie_file) {
        return
    }

    if (!fs.existsSync(syncConfig.script_path)) {
        throw new Error(`biliup browser cookie sync helper not found: ${syncConfig.script_path}`)
    }

    fs.mkdirSync(path.dirname(config.cookie_file), { recursive: true })

    const args = [
        syncConfig.script_path,
        '--session-profile',
        syncConfig.session_profile,
        '--output',
        config.cookie_file,
        '--url',
        syncConfig.url,
        '--browser-mode',
        syncConfig.browser_mode,
    ]

    if (syncConfig.user_agent) {
        args.push('--user-agent', syncConfig.user_agent)
    }
    if (syncConfig.locale) {
        args.push('--locale', syncConfig.locale)
    }
    if (syncConfig.timezone) {
        args.push('--timezone', syncConfig.timezone)
    }

    const stdoutChunks: string[] = []
    const stderrChunks: string[] = []

    await new Promise<void>((resolve, reject) => {
        const child = spawn(syncConfig.bun_path, args, {
            cwd: config.working_dir,
            env: {
                ...process.env,
                BROWSER_PROFILE_DIR:
                    process.env.BROWSER_PROFILE_DIR || path.join(process.cwd(), 'assets', 'cookies', 'browser-profiles'),
            },
        })

        child.stdout.on('data', (chunk) => {
            const text = chunk.toString()
            stdoutChunks.push(text)
            text.trim() && log?.debug(`[biliup-cookie-sync] ${text.trim()}`)
        })
        child.stderr.on('data', (chunk) => {
            const text = chunk.toString()
            stderrChunks.push(text)
            text.trim() && log?.warn(`[biliup-cookie-sync] ${text.trim()}`)
        })
        child.on('error', (error) => reject(error))
        child.on('close', (code) => {
            if (code === 0) {
                resolve()
                return
            }
            reject(
                new Error(
                    `biliup browser cookie sync exited with code ${code}: ${stderrChunks.join('').trim() || stdoutChunks.join('').trim()}`,
                ),
            )
        })
    })
}

async function runSpawn(command: string, args: string[], cwd: string, logPrefix: string, log?: Logger) {
    const stdoutChunks: string[] = []
    const stderrChunks: string[] = []

    await new Promise<void>((resolve, reject) => {
        const child = spawn(command, args, {
            cwd,
            env: process.env,
        })

        child.stdout.on('data', (chunk) => {
            const text = chunk.toString()
            stdoutChunks.push(text)
            text.trim() && log?.debug(`${logPrefix} ${text.trim()}`)
        })
        child.stderr.on('data', (chunk) => {
            const text = chunk.toString()
            stderrChunks.push(text)
            text.trim() && log?.warn(`${logPrefix} ${text.trim()}`)
        })
        child.on('error', (error) => reject(error))
        child.on('close', (code) => {
            if (code === 0) {
                resolve()
                return
            }
            reject(new Error(`${command} exited with code ${code}: ${stderrChunks.join('').trim() || stdoutChunks.join('').trim()}`))
        })
    })
}

async function ensureCollisionPlaceholderVideo(
    config: ResolvedBiliupCollisionPlaceholderPartConfig,
    workingDir: string,
    log?: Logger,
) {
    if (config.video_path) {
        if (!fs.existsSync(config.video_path)) {
            throw new Error(`biliup collision placeholder video not found: ${config.video_path}`)
        }
        return config.video_path
    }

    if (!fs.existsSync(config.image_path)) {
        throw new Error(`biliup collision placeholder image not found: ${config.image_path}`)
    }

    const stat = fs.statSync(config.image_path)
    const cacheKey = createHash('sha1')
        .update(
            JSON.stringify({
                image_path: config.image_path,
                mtime_ms: stat.mtimeMs,
                size: stat.size,
                title: config.title,
                duration_seconds: config.duration_seconds,
                width: config.width,
                height: config.height,
                fps: config.fps,
                background_color: config.background_color,
            }),
        )
        .digest('hex')

    const outputDir = path.join(workingDir, 'collision-placeholder-cache')
    const outputPath = path.join(outputDir, `${cacheKey}.mp4`)
    if (fs.existsSync(outputPath)) {
        return outputPath
    }

    fs.mkdirSync(outputDir, { recursive: true })

    const fadeOutStart = Math.max(0, config.duration_seconds - 0.3)
    await runSpawn(
        config.ffmpeg_path,
        [
            '-y',
            '-loop',
            '1',
            '-i',
            config.image_path,
            '-f',
            'lavfi',
            '-i',
            'anullsrc=channel_layout=stereo:sample_rate=48000',
            '-t',
            String(config.duration_seconds),
            '-vf',
            [
                `scale=${config.width}:${config.height}:force_original_aspect_ratio=decrease`,
                `pad=${config.width}:${config.height}:(ow-iw)/2:(oh-ih)/2:${config.background_color}`,
                'format=yuv420p',
                'fade=t=in:st=0:d=0.25',
                `fade=t=out:st=${fadeOutStart}:d=0.25`,
            ].join(','),
            '-r',
            String(config.fps),
            '-c:v',
            'libx264',
            '-preset',
            'veryfast',
            '-pix_fmt',
            'yuv420p',
            '-c:a',
            'aac',
            '-b:a',
            '128k',
            '-shortest',
            outputPath,
        ],
        outputDir,
        '[biliup-collision-part]',
        log,
    )

    return outputPath
}

function stageUploadVideoPart(sourcePath: string, stagedPath: string) {
    try {
        fs.symlinkSync(sourcePath, stagedPath)
        return
    } catch { }

    try {
        fs.linkSync(sourcePath, stagedPath)
        return
    } catch { }

    fs.copyFileSync(sourcePath, stagedPath)
}

async function prepareUploadVideoParts(
    candidate: Pick<BiliupUploadCandidate, 'videoPaths' | 'config'>,
    uploadDir: string,
    log?: Logger,
): Promise<Array<PreparedUploadVideoPart>> {
    let parts = candidate.videoPaths.map((videoPath) => ({
        sourcePath: videoPath,
    }))

    if (candidate.config.collision_placeholder_part && candidate.videoPaths.length === 1) {
        try {
            const placeholderPath = await ensureCollisionPlaceholderVideo(
                candidate.config.collision_placeholder_part,
                candidate.config.working_dir,
                log,
            )
            parts = [
                {
                    sourcePath: candidate.videoPaths[0],
                    partTitle: DEFAULT_BILIUP_COLLISION_MAIN_PART_TITLE,
                },
                {
                    sourcePath: placeholderPath,
                    partTitle: candidate.config.collision_placeholder_part.title,
                },
            ]
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            log?.warn(`Failed to prepare biliup collision placeholder part, continuing without it: ${message}`)
        }
    }

    return parts.map((part, index) => {
        if (!part.partTitle) {
            return {
                ...part,
                stagedPath: part.sourcePath,
            }
        }

        const extension = path.extname(part.sourcePath) || '.mp4'
        const stagedPath = path.join(uploadDir, `${sanitizeFileStem(part.partTitle, `part-${index + 1}`)}${extension}`)
        if (!fs.existsSync(stagedPath)) {
            stageUploadVideoPart(part.sourcePath, stagedPath)
        }
        return {
            ...part,
            stagedPath,
        }
    })
}

async function runBiliupUpload(
    article: Pick<Article, 'a_id'>,
    candidate: BiliupUploadCandidate,
    credentials: Partial<Pick<{ sessdata: string; bili_jct: string }, 'sessdata' | 'bili_jct'>>,
    log?: Logger,
) {
    if (!fs.existsSync(candidate.config.helper_path)) {
        throw new Error(`biliup helper not found: ${candidate.config.helper_path}`)
    }

    fs.mkdirSync(candidate.config.working_dir, { recursive: true })
    const uploadDir = fs.mkdtempSync(path.join(candidate.config.working_dir, `${article.a_id}-`))
    const cookieFile = path.join(uploadDir, 'cookies.json')
    let browserCookieSyncError: Error | null = null

    if (candidate.config.browser_cookie_sync && candidate.config.cookie_file) {
        try {
            await runBrowserCookieSync(candidate.config, log)
        } catch (error) {
            browserCookieSyncError = error instanceof Error ? error : new Error(String(error))
            log?.warn(`Biliup browser cookie sync failed, will try fallback credentials: ${browserCookieSyncError.message}`)
        }
    }

    let cookieDocument: BiliupCookieDocument | null = null
    if (candidate.config.cookie_file && fs.existsSync(candidate.config.cookie_file)) {
        try {
            cookieDocument = normalizeBiliupCookieDocument(
                JSON.parse(fs.readFileSync(candidate.config.cookie_file, 'utf8')),
            )
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            log?.warn(`Invalid biliup cookie file ${candidate.config.cookie_file}, falling back if possible: ${message}`)
        }
    }

    if (!cookieDocument) {
        if (!credentials.sessdata || !credentials.bili_jct) {
            if (candidate.config.cookie_file && !fs.existsSync(candidate.config.cookie_file)) {
                throw new Error(
                    `biliup cookie file not found: ${candidate.config.cookie_file}${browserCookieSyncError ? ` (${browserCookieSyncError.message})` : ''}`,
                )
            }
            throw new Error('biliup upload requires video_upload.cookie_file or both sessdata and bili_jct')
        }
        cookieDocument = buildCookieDocument(credentials.sessdata, credentials.bili_jct)
    }

    fs.writeFileSync(cookieFile, JSON.stringify(cookieDocument, null, 2))
    const preparedVideoParts = await prepareUploadVideoParts(candidate, uploadDir, log)

    const args = [
        candidate.config.helper_path,
        '--cookie-file',
        cookieFile,
        '--title',
        candidate.title,
        '--desc',
        candidate.description,
        '--source-url',
        candidate.sourceUrl,
        '--tid',
        String(candidate.config.tid),
        '--threads',
        String(candidate.config.threads),
        '--submit-api',
        candidate.config.submit_api,
        '--line',
        candidate.config.line,
        '--copyright',
        String(candidate.config.copyright),
    ]

    for (const tag of candidate.config.tags) {
        args.push('--tag', tag)
    }
    if (candidate.coverPath) {
        args.push('--cover', candidate.coverPath)
    }
    args.push('--')
    args.push(...preparedVideoParts.map((part) => part.stagedPath))

    log?.info(`Uploading video with biliup for ${article.a_id}: ${candidate.videoPaths.length} file(s)`)

    const stdoutChunks: string[] = []
    const stderrChunks: string[] = []

    await new Promise<void>((resolve, reject) => {
        const child = spawn(candidate.config.python_path, args, {
            cwd: uploadDir,
            env: {
                ...process.env,
                PYTHONUNBUFFERED: '1',
            },
        })

        child.stdout.on('data', (chunk) => {
            const text = chunk.toString()
            stdoutChunks.push(text)
            text.trim() && log?.debug(`[biliup] ${text.trim()}`)
        })
        child.stderr.on('data', (chunk) => {
            const text = chunk.toString()
            stderrChunks.push(text)
            text.trim() && log?.warn(`[biliup] ${text.trim()}`)
        })
        child.on('error', (error) => reject(error))
        child.on('close', (code) => {
            if (code === 0) {
                resolve()
                return
            }
            reject(new Error(`biliup exited with code ${code}: ${stderrChunks.join('').trim() || stdoutChunks.join('').trim()}`))
        })
    }).finally(() => {
        try {
            fs.rmSync(cookieFile, { force: true })
        } catch { }
    })

    return {
        stdout: stdoutChunks.join(''),
        stderr: stderrChunks.join(''),
    }
}

export {
    DEFAULT_BILIUP_EXCLUDED_UIDS,
    buildBiliupUploadCandidate,
    buildCookieDocument,
    normalizeBiliupCookieDocument,
        resolveBrowserCookieSyncConfig,
        resolveVideoUploadConfig,
        runBiliupUpload,
        runBrowserCookieSync,
        prepareUploadVideoParts,
    }
export type { BiliupUploadCandidate, ResolvedBiliupVideoUploadConfig }
