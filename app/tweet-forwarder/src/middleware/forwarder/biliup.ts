import { CACHE_DIR_ROOT } from '@/config'
import type { Article } from '@/db'
import { formatPlatformTag } from '@/services/render-service'
import type { BiliupVideoUploadConfig } from '@/types/forwarder'
import type { Logger } from '@idol-bbq-utils/log'
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

type MediaFile = {
    media_type: MediaType
    path: string
}

interface ResolvedBiliupVideoUploadConfig {
    enabled: boolean
    python_path: string
    helper_path: string
    working_dir: string
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

function normalizeTag(tag: string) {
    return tag.replace(/[\r\n,]+/g, ' ').replace(/\s+/g, ' ').trim()
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

function buildCookieDocument(sessdata: string, bili_jct: string) {
    return {
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

async function runBiliupUpload(
    article: Pick<Article, 'a_id'>,
    candidate: BiliupUploadCandidate,
    credentials: Pick<{ sessdata: string; bili_jct: string }, 'sessdata' | 'bili_jct'>,
    log?: Logger,
) {
    if (!fs.existsSync(candidate.config.helper_path)) {
        throw new Error(`biliup helper not found: ${candidate.config.helper_path}`)
    }

    fs.mkdirSync(candidate.config.working_dir, { recursive: true })
    const uploadDir = fs.mkdtempSync(path.join(candidate.config.working_dir, `${article.a_id}-`))
    const cookieFile = path.join(uploadDir, 'cookies.json')
    fs.writeFileSync(cookieFile, JSON.stringify(buildCookieDocument(credentials.sessdata, credentials.bili_jct), null, 2))

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
    resolveVideoUploadConfig,
    runBiliupUpload,
}
export type { BiliupUploadCandidate, ResolvedBiliupVideoUploadConfig }
