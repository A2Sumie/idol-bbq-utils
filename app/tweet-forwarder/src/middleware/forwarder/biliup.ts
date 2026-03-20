import { CACHE_DIR_ROOT } from '@/config'
import type { Article } from '@/db'
import { formatPlatformTag } from '@/services/render-service'
import type { BiliupVideoUploadConfig } from '@/types/forwarder'
import type { Logger } from '@idol-bbq-utils/log'
import type { BrowserMode } from '@idol-bbq-utils/spider'
import { Platform, type MediaType } from '@idol-bbq-utils/spider/types'
import { spawn } from 'child_process'
import dayjs from 'dayjs'
import fs from 'fs'
import path from 'path'

const DEFAULT_BILIUP_TID = 171
const DEFAULT_BILIUP_THREADS = 3
const DEFAULT_BILIUP_SUBMIT_API = 'web'
const DEFAULT_BILIUP_LINE = 'AUTO'
const DEFAULT_BILIUP_WORKING_DIR = path.join(CACHE_DIR_ROOT, 'media', 'biliup')
const DEFAULT_BILIUP_EXCLUDED_UIDS = ['22/7:radio', '22/7:movie']
const DEFAULT_BILIUP_COOKIE_SYNC_URL = 'https://www.bilibili.com'

type MediaFile = {
    media_type: MediaType
    path: string
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

interface ResolvedBiliupVideoUploadConfig {
    enabled: boolean
    python_path: string
    helper_path: string
    working_dir: string
    cookie_file?: string
    browser_cookie_sync?: ResolvedBiliupBrowserCookieSyncConfig
    submit_api: 'web'
    line: 'AUTO' | 'bda' | 'bda2' | 'ws' | 'qn' | 'bldsa' | 'tx' | 'txa'
    tid: number
    threads: number
    copyright: 1 | 2
    tags: Array<string>
    exclude_uids: Array<string>
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
            '/app/tools/export-biliup-browser-cookies.ts',
            path.resolve(process.cwd(), 'app/tweet-forwarder/scripts/export-biliup-browser-cookies.ts'),
        ],
        '/app/tools/export-biliup-browser-cookies.ts',
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

function deriveTitle(article: Pick<Article, 'content' | 'platform' | 'username' | 'a_id' | 'created_at'>, texts: string[]) {
    const candidates = [article.content, ...texts]
        .flatMap((value) => (value || '').split(/\r?\n/))
        .map((line) => line.trim())
        .filter(Boolean)
    const fallback = `${formatPlatformTag(article)} ${dayjs.unix(article.created_at).format('YYYY-MM-DD HH:mm')}`
    return truncateText(candidates[0] || fallback || article.a_id, 80)
}

function deriveDescription(article: Pick<Article, 'url'>, texts: string[]) {
    const body = texts.join('\n\n').trim()
    const sections = [body]
    if (article.url) {
        sections.push(`原链接: ${article.url}`)
    }
    return sections.filter(Boolean).join('\n\n')
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
        cookie_file: resolveConfiguredPath(config.cookie_file),
        browser_cookie_sync: resolveBrowserCookieSyncConfig(config.browser_cookie_sync),
        submit_api: config.submit_api === 'web' ? config.submit_api : DEFAULT_BILIUP_SUBMIT_API,
        line: config.line || DEFAULT_BILIUP_LINE,
        tid: Number(config.tid || DEFAULT_BILIUP_TID),
        threads: Math.max(1, Number(config.threads || DEFAULT_BILIUP_THREADS)),
        copyright: config.copyright === 1 ? 1 : 2,
        tags: uniqueStrings((config.tags || []).map(normalizeTag)),
        exclude_uids: uniqueStrings([...(config.exclude_uids || []), ...DEFAULT_BILIUP_EXCLUDED_UIDS]),
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
        title: deriveTitle(article, texts),
        description: deriveDescription(article, texts),
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
    args.push(...candidate.videoPaths)

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
}
export type { BiliupUploadCandidate, ResolvedBiliupVideoUploadConfig }
