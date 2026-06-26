import { CACHE_DIR_ROOT } from '@/config'
import type { Article } from '@/db'
import { processorRegistry } from '@/middleware/processor'
import { formatPlatformTag } from '@/services/render-service'
import type { BiliupVideoUploadConfig } from '@/types/forwarder'
import type { ProcessorConfig, ProcessorProvider } from '@/types/processor'
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
const DEFAULT_BILIUP_METADATA_TIMEZONE = 'Asia/Tokyo'
const DEFAULT_BILIUP_TAG_TARGET_COUNT = 10
const MAX_BILIUP_TAG_COUNT = 10
const MAX_BILIUP_TAG_CHARS = 20
const MAX_BILIUP_TITLE_CHARS = 80
const DEFAULT_BILIUP_TITLE_MIN_CHARS = 4
const DEFAULT_BILIUP_TITLE_MAX_CHARS = 32
const BILIUP_COMMON_TAGS = ['22/7', '秋元康', '偶像', '声优偶像', '七分之二十二']
const BILIUP_FALLBACK_TOPIC_TAGS = ['ナナニジ', '日本偶像', '声优', '日系偶像', '偶像团体', '二次元偶像']
const BILIUP_FORBIDDEN_TITLE_TERMS = [
    '震惊',
    '惊呆',
    '爆料',
    '爆炸',
    '炸裂',
    '离谱',
    '必看',
    '神回',
    '全网',
    '独家',
    '首发',
    '实锤',
    '塌房',
    '翻车',
    '泪目',
    '杀疯',
    '燃爆',
    '逆天',
    '不得不看',
]
const BILIUP_FORBIDDEN_TAGS = new Set(
    [
        '搬运',
        '转载',
        '转帖',
        '社媒',
        '社交媒体',
        'social media',
        'x',
        'twitter',
        '推特',
        'instagram',
        'ins',
        'ig',
        'tiktok',
        'tt',
        'youtube',
        'yt',
        '官网',
        'blog',
        '视频',
        '短视频',
        '长视频',
        '投稿',
        'story',
        'shorts',
    ].map((tag) => tag.toLocaleLowerCase()),
)
const BILIUP_ACCOUNT_DISPLAY_NAME_MAP: Record<string, string> = {
    '22_7_channel': '22/7',
    '227smej': '22/7',
    '227_staff': '22/7',
    '227official': '22/7',
    '227keisanchu': '22/7 計算外',
    nananijigram22_7: '22/7',
    'nananijigram22_7_the.3rd': '22/7 THE 3RD',
    _fujimasakura: '藤間桜',
    _nishiurasora: '西浦そら',
    _saitonicole: '斎藤ニコル',
    _takigawamiu: '滝川みう',
    _yagamitoa: '八神叶愛',
    alice__kurosaki: '黒崎ありす',
    asaoka_mao__: '麻丘真央',
    chiharu_okr: '千春',
    cure_rinochi: '望月りの',
    em_matcha227: '月城咲舞',
    emma_tsukishiro: '月城咲舞',
    hikari_kabashima: '椛島光',
    iko_hiyama: '桧山依子',
    kawase_uta: '河瀬詩',
    kitahara_misaki: '北原実咲',
    luna: '四条月',
    'luna.shijo': '四条月',
    luna_shijo: '四条月',
    mana__tachibana: '橘茉奈',
    mao_asaoka227: '麻丘真央',
    mao_asaoka_227: '麻丘真央',
    mikumo_haruka: '三雲遥加',
    minami__iori: '南伊織',
    mirei_orimoto: '折本美玲',
    nagomi_saijo_227: '西條和',
    nao_aikawa227: '相川奈央',
    rino_mochizuki: '望月りの',
    ruri_yoshizawa: '吉沢珠璃',
    sally_amaki: '天城サリー',
    sally_amaki_official: '天城サリー',
    sallyamaki: '天城サリー',
    sallyamakiofficial: '天城サリー',
    satsuki_shiina: '椎名桜月',
    shiina_satsuki227: '椎名桜月',
    shiina_satsuki_: '椎名桜月',
    tabesugiyaseruo: '蒼乃音',
    ui_sakura_0526: '佐倉初',
    yoshizawa_ruri: '吉沢珠璃',
}
const SALLY_MEMBER_ONLY_BILIUP_HANDLES = new Set(
    ['sally_amaki', 'sallyamaki', 'sally_amaki_official', 'sallyamakiofficial'].map((value) =>
        value.toLocaleLowerCase(),
    ),
)
const SALLY_MEMBER_ONLY_POST_PATTERNS = [
    /付[费費]会员|会员限定|会员动态|訂閱者限定|訂閱者專用|訂閱者专用/u,
    /有料会員|会員限定|メンバー限定|メン限|サブスク限定|サブスク向け|支援者限定/u,
    /subscribers?[-\s]?only|subscriber[-\s]?exclusive|members?[-\s]?only|paid\s+subscribers?|creator\s+subscriptions?|super\s+followers?/i,
]

type TemplateContext = Record<string, string>

type MediaFile = {
    media_type: MediaType
    path: string
}

type PreparedUploadVideoPart = {
    sourcePath: string
    stagedPath: string
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

interface ResolvedBiliupTagGenerationConfig {
    enabled: true
    provider: ProcessorProvider | string
    api_key: string
    target_count: number
    cfg_processor?: ProcessorConfig
}

interface ResolvedBiliupTitleGenerationConfig {
    enabled: true
    provider: ProcessorProvider | string
    api_key: string
    target_min_chars: number
    target_max_chars: number
    cfg_processor?: ProcessorConfig
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
    tag_generation?: ResolvedBiliupTagGenerationConfig
    title_generation?: ResolvedBiliupTitleGenerationConfig
    exclude_uids: Array<string>
    metadata_templates?: ResolvedBiliupMetadataTemplatesConfig
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

type BiliupMemberFact = {
    official_section?: string
    names?: {
        ja?: string
        kana?: string
    }
    sns?: Array<{
        platform?: string
        url?: string
    }>
}

type BiliupMemberFactIndex = {
    facts: Array<BiliupMemberFact>
    byHandle: Map<string, BiliupMemberFact>
    byName: Map<string, BiliupMemberFact>
}

let biliupMemberFactIndexCache: BiliupMemberFactIndex | null | undefined

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
    return tag
        .replace(/[\r\n,]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
}

function normalizeBiliupUploadTag(tag: string) {
    const normalized = normalizeTag(tag)
        .replace(/^[#＃]+/, '')
        .replace(/[#＃]/g, '')
        .trim()
    return truncateText(normalized, MAX_BILIUP_TAG_CHARS)
}

function normalizeBiliupTagKey(tag: string) {
    return normalizeBiliupUploadTag(tag).toLocaleLowerCase()
}

function isUsefulBiliupUploadTag(tag: string) {
    const normalized = normalizeBiliupUploadTag(tag)
    if (!normalized) {
        return false
    }
    const key = normalized.toLocaleLowerCase()
    if (BILIUP_FORBIDDEN_TAGS.has(key)) {
        return false
    }
    if (/^https?:\/\//i.test(normalized) || normalized.startsWith('@')) {
        return false
    }
    return /[A-Za-z0-9\u3040-\u30ff\u3400-\u9fff]/.test(normalized)
}

function uniqueBiliupTags(values: Array<string>, limit = MAX_BILIUP_TAG_COUNT) {
    const seen = new Set<string>()
    const tags: string[] = []
    for (const value of values) {
        const normalized = normalizeBiliupUploadTag(value)
        if (!isUsefulBiliupUploadTag(normalized)) {
            continue
        }
        const key = normalizeBiliupTagKey(normalized)
        if (seen.has(key)) {
            continue
        }
        seen.add(key)
        tags.push(normalized)
        if (tags.length >= limit) {
            break
        }
    }
    return tags
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function uniqueStrings(values: Array<string>) {
    return Array.from(new Set(values.filter(Boolean)))
}

function resolveMinNumber(value: unknown, fallback: number, min: number) {
    const numeric = Number(value ?? fallback)
    return Number.isFinite(numeric) ? Math.max(min, numeric) : fallback
}

function resolveMinInteger(value: unknown, fallback: number, min: number) {
    return Math.floor(resolveMinNumber(value, fallback, min))
}

function truncateText(value: string, maxChars: number) {
    const chars = Array.from(value)
    if (chars.length <= maxChars) {
        return value
    }
    return `${chars.slice(0, Math.max(0, maxChars - 3)).join('')}...`
}

function normalizeTextBlock(value: string | null | undefined) {
    return String(value || '')
        .replace(/\r\n/g, '\n')
        .replace(/\u00a0/g, ' ')
        .replace(/[ \t]+\n/g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim()
}

function hasBiliupTitleText(value: string) {
    return /[A-Za-z0-9\u3040-\u30ff\u3400-\u9fff]/.test(value)
}

function normalizeBiliupTitleText(value: string | null | undefined, fallback: string) {
    const normalized = normalizeTextBlock(value)
        .replace(/[<>:"/\\|?*\u0000-\u001F]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
    return normalized && hasBiliupTitleText(normalized) ? normalized : fallback
}

function normalizeBiliupMainTitleText(value: string | null | undefined, fallback: string) {
    const normalized = normalizeTextBlock(value).replace(/\s+/g, ' ').trim()
    return normalized && hasBiliupTitleText(normalized) ? normalized : fallback
}

function hasRenderedTitlePayload(value: string, context: TemplateContext) {
    const ignoredTokens = new Set(
        [context.source_tag, context.platform_label, context.type_label, 'TT', 'YT', 'X', 'ins', 'blog', '社媒']
            .map((token) =>
                String(token || '')
                    .trim()
                    .toLocaleLowerCase(),
            )
            .filter(Boolean),
    )
    const payload = normalizeTextBlock(value)
        .replace(/[【】\[\]()（）{}<>《》「」『』]/g, ' ')
        .replace(/[|｜:：,，.。/_-]+/g, ' ')
        .split(/\s+/)
        .filter((token) => token && !ignoredTokens.has(token.toLocaleLowerCase()))
        .join(' ')
    return hasBiliupTitleText(payload)
}

function collectTextBlocks(article: Pick<Article, 'content'> & { translation?: string | null }, texts: string[]) {
    const seen = new Set<string>()
    const blocks: string[] = []
    for (const candidate of [article.translation, article.content, ...texts]) {
        const normalized = normalizeTextBlock(candidate)
        if (!normalized || seen.has(normalized)) {
            continue
        }
        seen.add(normalized)
        blocks.push(normalized)
    }
    return blocks
}

function normalizeComparableText(value: string | null | undefined) {
    return String(value || '')
        .replace(/[【】「」『』"'“”‘’\[\]()（）]/g, '')
        .replace(/\s+/g, ' ')
        .trim()
        .toLocaleLowerCase()
}

function compactComparableText(value: string | null | undefined) {
    return normalizeTextBlock(value)
        .replace(/[^\p{L}\p{N}]/gu, '')
        .toLocaleLowerCase()
}

function firstNonEmptyLine(value: string | null | undefined) {
    return (
        normalizeTextBlock(value)
            .split('\n')
            .map((line) => line.trim())
            .find(Boolean) || ''
    )
}

function stripDuplicateLeadingSummary(block: string, summary: string) {
    const normalizedSummary = normalizeComparableText(summary)
    if (!block || !normalizedSummary) {
        return block
    }

    const lines = block.split('\n')
    while (lines[0] !== undefined && !lines[0]!.trim()) {
        lines.shift()
    }
    const first = lines[0]?.trim() || ''
    if (!first || normalizeComparableText(first) !== normalizedSummary) {
        return block
    }

    lines.shift()
    while (lines[0] !== undefined && !lines[0]!.trim()) {
        lines.shift()
    }
    return lines.join('\n').trim()
}

function normalizeBiliupAccountKey(value: string | null | undefined) {
    return String(value || '')
        .trim()
        .replace(/^@+/, '')
        .toLocaleLowerCase()
}

function extractHandleFromSocialUrl(url: string | null | undefined) {
    const value = String(url || '').trim()
    if (!value) {
        return ''
    }
    try {
        const parsed = new URL(value)
        return normalizeBiliupAccountKey(parsed.pathname.split('/').filter(Boolean)[0])
    } catch {
        return normalizeBiliupAccountKey(value.split('/').filter(Boolean).pop())
    }
}

function isSallyMemberOnlyBiliupHandle(article: Pick<Article, 'u_id' | 'username' | 'url'>) {
    const candidates = [article.u_id, extractHandleFromSocialUrl(article.url)]
        .map((value) => normalizeBiliupAccountKey(value))
        .filter(Boolean)
    return candidates.some((value) => SALLY_MEMBER_ONLY_BILIUP_HANDLES.has(value))
}

function shouldSkipSallyMemberOnlyBiliupUpload(
    article: Pick<Article, 'platform' | 'u_id' | 'username' | 'url' | 'content' | 'extra'> & {
        translation?: string | null
    },
    texts: string[] = [],
) {
    if (article.platform === Platform.YouTube || !isSallyMemberOnlyBiliupHandle(article)) {
        return false
    }
    const extraText = article.extra ? JSON.stringify(article.extra) : ''
    const haystack = collectTextBlocks(article, [...texts, extraText]).join('\n')
    return SALLY_MEMBER_ONLY_POST_PATTERNS.some((pattern) => pattern.test(haystack))
}

function resolveBiliupMemberFactIndex(): BiliupMemberFactIndex | null {
    if (biliupMemberFactIndexCache !== undefined) {
        return biliupMemberFactIndexCache
    }

    const candidates = [
        path.join(process.cwd(), 'assets/knowledge/22_7/facts/members.json'),
        '/app/assets/knowledge/22_7/facts/members.json',
    ]
    const sourcePath = candidates.find((candidate) => fs.existsSync(candidate))
    if (!sourcePath) {
        biliupMemberFactIndexCache = null
        return null
    }

    try {
        const payload = JSON.parse(fs.readFileSync(sourcePath, 'utf8')) as Array<BiliupMemberFact> | { items?: unknown }
        const facts = Array.isArray(payload)
            ? payload
            : Array.isArray(payload?.items)
              ? (payload.items as Array<BiliupMemberFact>)
              : []
        const byHandle = new Map<string, BiliupMemberFact>()
        const byName = new Map<string, BiliupMemberFact>()
        for (const fact of facts) {
            const jaName = normalizeBiliupUploadTag(fact.names?.ja || '')
            if (jaName) {
                byName.set(normalizeBiliupTagKey(jaName), fact)
            }
            for (const sns of fact.sns || []) {
                const handle = extractHandleFromSocialUrl(sns.url)
                if (handle) {
                    byHandle.set(handle, fact)
                }
            }
        }
        biliupMemberFactIndexCache = { facts, byHandle, byName }
    } catch {
        biliupMemberFactIndexCache = null
    }
    return biliupMemberFactIndexCache
}

function resolveBiliupMemberFact(article: Pick<Article, 'username' | 'u_id'>) {
    const index = resolveBiliupMemberFactIndex()
    if (!index) {
        return null
    }

    for (const candidate of [article.u_id, article.username]) {
        const handle = normalizeBiliupAccountKey(candidate)
        const fact = handle ? index.byHandle.get(handle) : null
        if (fact) {
            return fact
        }
    }

    const displayName = normalizeBiliupUploadTag(resolveFallbackDisplayName(article))
    return displayName ? index.byName.get(normalizeBiliupTagKey(displayName)) || null : null
}

function uniqueBiliupMemberFacts(facts: Array<BiliupMemberFact | null | undefined>) {
    const seen = new Set<string>()
    const result: BiliupMemberFact[] = []
    for (const fact of facts) {
        const name = normalizeBiliupUploadTag(fact?.names?.ja || '')
        if (!fact || !name) {
            continue
        }
        const key = normalizeBiliupTagKey(name)
        if (seen.has(key)) {
            continue
        }
        seen.add(key)
        result.push(fact)
    }
    return result
}

function resolveMentionedBiliupMemberFacts(
    article: Pick<Article, 'content' | 'username' | 'u_id'> & { translation?: string | null },
    texts: string[] = [],
) {
    const index = resolveBiliupMemberFactIndex()
    if (!index) {
        return []
    }

    const haystack = collectTextBlocks(article, [article.username || '', article.u_id || '', ...texts]).join('\n')
    if (!haystack) {
        return []
    }

    return index.facts
        .map((fact, order) => {
            const name = normalizeBiliupUploadTag(fact.names?.ja || '')
            const indexOf = name ? haystack.indexOf(name) : -1
            return { fact, indexOf, order }
        })
        .filter((item) => item.indexOf >= 0)
        .sort((a, b) => a.indexOf - b.indexOf || a.order - b.order)
        .map((item) => item.fact)
}

function resolveDetectedBiliupMemberFacts(
    article: Pick<Article, 'content' | 'username' | 'u_id'> & { translation?: string | null },
    texts: string[] = [],
) {
    return uniqueBiliupMemberFacts([resolveBiliupMemberFact(article), ...resolveMentionedBiliupMemberFacts(article, texts)])
}

function cleanupBiliupDisplayName(value: string | null | undefined) {
    return String(value || '')
        .replace(/【\s*22\/7\s*】/gi, '')
        .replace(/[（(]\s*22\/7\s*[)）]/gi, '')
        .replace(/^22\/7[\s:：-]+/i, '')
        .replace(/[\s:：-]+22\/7$/i, '')
        .replace(/\s+/g, ' ')
        .trim()
}

function resolveMappedBiliupDisplayName(article: Pick<Article, 'username' | 'u_id'>) {
    for (const candidate of [article.u_id, article.username]) {
        const key = normalizeBiliupAccountKey(candidate)
        const mapped = key ? BILIUP_ACCOUNT_DISPLAY_NAME_MAP[key] : undefined
        if (mapped) {
            return mapped
        }
    }
    return ''
}

function resolveFallbackDisplayName(article: Pick<Article, 'username' | 'u_id'>) {
    return (
        resolveMappedBiliupDisplayName(article) ||
        cleanupBiliupDisplayName(article.username) ||
        cleanupBiliupDisplayName(article.u_id) ||
        'Unknown'
    )
}

function resolveDisplayName(
    article: Pick<Article, 'content' | 'username' | 'u_id'> & { translation?: string | null },
    texts: string[] = [],
) {
    const memberNames = resolveDetectedBiliupMemberFacts(article, texts)
        .map((fact) => normalizeBiliupUploadTag(fact.names?.ja || ''))
        .filter(Boolean)
    if (memberNames.length > 0) {
        return memberNames.join(' ')
    }
    return resolveFallbackDisplayName(article)
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

function resolveBiliupSourceTag(article: Pick<Article, 'platform'>) {
    if (article.platform === Platform.X) {
        return 'X'
    }
    if (article.platform === Platform.TikTok) {
        return 'TT'
    }
    if (article.platform === Platform.Instagram) {
        return 'ins'
    }
    if (article.platform === Platform.YouTube) {
        return 'YT'
    }
    if (article.platform === Platform.Website) {
        return 'blog'
    }
    return '社媒'
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
    const dateShort = `${String(parts.year || '').slice(-2)}.${parts.month}.${parts.day}`
    const time = `${parts.hour}:${parts.minute}`
    return {
        date,
        date_short: dateShort,
        time,
        datetime: `${date} ${time}`,
    }
}

function resolveUploadSummary(
    article: Pick<Article, 'platform'>,
    primaryLine: string,
    displayName: string,
    dateTime: ReturnType<typeof formatDateTimeParts>,
) {
    const summary = primaryLine.trim()
    if ([Platform.Instagram, Platform.TikTok, Platform.X].includes(article.platform)) {
        const prefix = [displayName, dateTime.date_short].filter(Boolean).join(' ').trim()
        return [prefix, summary].filter(Boolean).join(' ').trim() || `${displayName} ${dateTime.datetime}`.trim()
    }
    return summary || `${displayName} ${dateTime.datetime}`.trim()
}

function resolveBiliupAccountTitle(displayName: string) {
    return displayName.startsWith('22/7') ? displayName : `22/7 ${displayName}`
}

function isBiliupCollectionDisplayName(value: string) {
    const normalized = normalizeBiliupTagKey(value).replace(/[^a-z0-9]+/g, '')
    return (
        normalized === '227' ||
        normalized === '227the3rd' ||
        normalized === '227staff' ||
        normalized === '227official' ||
        normalized === '227nananijigram'
    )
}

function buildTemplateContext(
    article: Pick<Article, 'content' | 'platform' | 'username' | 'u_id' | 'a_id' | 'created_at' | 'url' | 'type'> & {
        translation?: string | null
    },
    texts: string[],
    timeZone: string,
): TemplateContext {
    const blocks = collectTextBlocks(article, texts)
    const primaryLine =
        blocks
            .flatMap((value) => value.split('\n'))
            .map((line) => line.trim())
            .find(Boolean) || ''
    const dateTime = formatDateTimeParts(article.created_at, timeZone)
    const displayName = resolveDisplayName(article, texts)
    const summary = primaryLine || dateTime.datetime
    const bodyWithoutRepeatedSummary = stripDuplicateLeadingSummary(blocks[0] || '', summary)
    const body = bodyWithoutRepeatedSummary || blocks[0] || ''
    const uploadSummary = resolveUploadSummary(article, primaryLine, displayName, dateTime)

    return {
        account_title: resolveBiliupAccountTitle(displayName),
        article_id: article.a_id,
        body,
        body_or_summary: body || summary,
        date: dateTime.date,
        date_short: dateTime.date_short,
        datetime: dateTime.datetime,
        display_name: displayName,
        platform_label: resolvePlatformLabel(article),
        platform_type_label: resolvePlatformTypeLabel(article),
        source_tag: resolveBiliupSourceTag(article),
        summary,
        time: dateTime.time,
        type_label: resolveTypeLabel(article),
        upload_summary: uploadSummary,
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

function isYoutubeLongVideo(article: Pick<Article, 'platform' | 'type'>) {
    return article.platform === Platform.YouTube && article.type !== 'shorts'
}

function resolveDefaultTitleTemplate(article: Pick<Article, 'platform' | 'type'>) {
    if (isYoutubeLongVideo(article)) {
        return '【{{account_title}}】{{upload_summary}}'
    }
    return '【{{account_title}}】[{{source_tag}}] {{upload_summary}}'
}

function buildTitleFallback(context: TemplateContext, article: Pick<Article, 'platform' | 'username' | 'a_id'>) {
    const hardFallback = normalizeBiliupMainTitleText(
        [formatPlatformTag(article), context.datetime, context.article_id].filter(Boolean).join(' '),
        context.article_id || 'Bilibili upload',
    )
    return normalizeBiliupMainTitleText(
        `【${context.account_title}】${context.source_tag ? `[${context.source_tag}]` : ''} ${context.upload_summary}`,
        hardFallback,
    )
}

function deriveTitle(
    article: Pick<Article, 'content' | 'platform' | 'username' | 'u_id' | 'a_id' | 'created_at' | 'url' | 'type'> & {
        translation?: string | null
    },
    texts: string[],
    timeZone: string,
    template?: string,
) {
    const context = buildTemplateContext(article, texts, timeZone)
    const rendered = cleanupTemplateOutput(renderTemplate(template || resolveDefaultTitleTemplate(article), context))
    const fallback = buildTitleFallback(context, article)
    const title = hasRenderedTitlePayload(rendered, context) ? rendered : fallback
    return truncateText(normalizeBiliupMainTitleText(title, fallback), MAX_BILIUP_TITLE_CHARS)
}

function extractOriginalBiliupTitleLine(article: Pick<Article, 'content'> | undefined) {
    return normalizeBiliupMainTitleText(firstNonEmptyLine(article?.content), '')
}

function isYoutubeTitleAnnouncementLine(line: string, originalTitle: string, summary: string) {
    const text = normalizeTextBlock(line)
    if (!text) {
        return false
    }

    const lowerText = text.toLocaleLowerCase()
    const hasAnnouncement =
        /公開|已公开|已發布|已发布|发布|發布|公開しました|released|is out|now available/i.test(text)
    if (!hasAnnouncement) {
        return false
    }

    const compactLine = compactComparableText(text)
    const compactOriginalTitle = compactComparableText(originalTitle)
    const compactSummary = compactComparableText(summary)
    return (
        Boolean(compactOriginalTitle && compactLine.includes(compactOriginalTitle)) ||
        Boolean(compactSummary && compactLine.includes(compactSummary)) ||
        lowerText.includes('audition documentary')
    )
}

function stripLeadingYoutubeTitleAnnouncements(body: string, originalTitle: string, summary: string) {
    const lines = normalizeTextBlock(body).split('\n')
    let changed = false
    while (lines[0] !== undefined && !lines[0]!.trim()) {
        lines.shift()
        changed = true
    }
    while (lines[0] !== undefined && isYoutubeTitleAnnouncementLine(lines[0]!, originalTitle, summary)) {
        lines.shift()
        changed = true
        while (lines[0] !== undefined && !lines[0]!.trim()) {
            lines.shift()
        }
    }
    return changed ? lines.join('\n').trim() : body
}

function resolveDescriptionBody(
    article: Pick<Article, 'content' | 'platform' | 'type'>,
    context: TemplateContext,
) {
    const body = context.body_or_summary
    if (!isYoutubeLongVideo(article)) {
        return body
    }
    return stripLeadingYoutubeTitleAnnouncements(body, extractOriginalBiliupTitleLine(article), context.summary) || body
}

function deriveDescription(
    article: Pick<Article, 'content' | 'platform' | 'username' | 'u_id' | 'a_id' | 'created_at' | 'url' | 'type'> & {
        translation?: string | null
    },
    texts: string[],
    timeZone: string,
    template?: string,
) {
    const context = buildTemplateContext(article, texts, timeZone)
    if (template) {
        return cleanupTemplateOutput(renderTemplate(template, context))
    }

    const body = resolveDescriptionBody(article, context)
    const sections = [
        body,
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

function resolveTagGenerationConfig(
    config?: NonNullable<BiliupVideoUploadConfig['tag_generation']>,
): ResolvedBiliupTagGenerationConfig | undefined {
    if (!config?.enabled) {
        return undefined
    }
    const provider = String(config.provider || '').trim()
    const api_key = String(config.api_key || '').trim()
    if (!provider || !api_key) {
        return undefined
    }
    return {
        enabled: true,
        provider,
        api_key,
        target_count: Math.min(
            MAX_BILIUP_TAG_COUNT,
            Math.max(1, resolveMinInteger(config.target_count, DEFAULT_BILIUP_TAG_TARGET_COUNT, 1)),
        ),
        cfg_processor: config.cfg_processor,
    }
}

/**
 * Resolve a first-class title-generation config. Title generation is decoupled from tag generation:
 * when `title_generation` is omitted it defaults to ON and reuses the tag-generation provider/key
 * (preserving historical default-on behavior); an explicit `false` disables it; an object can point at
 * its own provider/key. Without a usable provider/api_key it resolves to undefined (deterministic only).
 */
function resolveTitleGenerationConfig(
    config?: BiliupVideoUploadConfig,
): ResolvedBiliupTitleGenerationConfig | undefined {
    if (!config) {
        return undefined
    }
    const raw = config.title_generation
    if (raw === false) {
        return undefined
    }
    const objectConfig = typeof raw === 'object' && raw ? raw : {}
    if (typeof raw === 'object' && raw && raw.enabled === false) {
        return undefined
    }
    // Fall back to tag-generation credentials so existing tag_generation-only configs keep generating titles.
    const provider = String(objectConfig.provider || config.tag_generation?.provider || '').trim()
    const api_key = String(objectConfig.api_key || config.tag_generation?.api_key || '').trim()
    if (!provider || !api_key) {
        return undefined
    }
    const minChars = Math.max(1, resolveMinInteger(objectConfig.target_min_chars, DEFAULT_BILIUP_TITLE_MIN_CHARS, 1))
    const maxChars = Math.max(
        minChars,
        resolveMinInteger(objectConfig.target_max_chars, DEFAULT_BILIUP_TITLE_MAX_CHARS, minChars),
    )
    return {
        enabled: true,
        provider,
        api_key,
        target_min_chars: minChars,
        target_max_chars: maxChars,
        cfg_processor: objectConfig.cfg_processor || config.tag_generation?.cfg_processor,
    }
}

function deriveMemberTags(
    article: Pick<Article, 'content' | 'username' | 'u_id'> & { translation?: string | null },
    texts: string[] = [],
) {
    const facts = resolveDetectedBiliupMemberFacts(article, texts)
    const displayName = normalizeBiliupUploadTag(resolveDisplayName(article, texts))
    const tags = facts.length > 0 || isBiliupCollectionDisplayName(displayName) ? facts.map((fact) => fact.names?.ja || '') : [displayName]
    if (facts.some((fact) => fact.official_section === '22/7_the_3rd')) {
        tags.push('22/7三期生')
    }
    return uniqueBiliupTags(tags)
}

function deriveTags(
    article: Pick<Article, 'content' | 'username' | 'u_id'> & { translation?: string | null },
    texts: string[],
    configuredTags: Array<string>,
) {
    return uniqueBiliupTags([...BILIUP_COMMON_TAGS, ...deriveMemberTags(article, texts), ...configuredTags])
}

function completeTagsWithFallback(tags: Array<string>, targetCount = DEFAULT_BILIUP_TAG_TARGET_COUNT) {
    return uniqueBiliupTags([...tags, ...BILIUP_FALLBACK_TOPIC_TAGS], Math.min(MAX_BILIUP_TAG_COUNT, targetCount))
}

function buildBiliupTitleCandidates(
    article: Pick<Article, 'content' | 'platform' | 'username' | 'u_id' | 'a_id' | 'created_at' | 'url' | 'type'> & {
        translation?: string | null
    },
    texts: string[],
    candidate: BiliupUploadCandidate,
) {
    const blocks = collectTextBlocks(article, texts)
    const firstOriginalLine = firstNonEmptyLine(article.content)
    const firstTranslationLine = firstNonEmptyLine(article.translation)
    const memberNames = resolveDetectedBiliupMemberFacts(article, texts)
        .map((fact) => normalizeBiliupUploadTag(fact.names?.ja || ''))
        .filter(Boolean)
    const fallbackDisplayName = resolveFallbackDisplayName(article)

    return [
        {
            source: 'deterministic_title',
            text: candidate.title,
            confidence: 'high',
            role: 'current_upload_title',
        },
        {
            source: 'translation_first_line',
            text: firstTranslationLine,
            confidence: firstTranslationLine ? 'high' : 'none',
            role: 'translated_reference',
        },
        {
            source: 'original_first_line',
            text: firstOriginalLine,
            confidence: firstOriginalLine ? 'high' : 'none',
            role: 'original_reference',
        },
        {
            source: 'detected_member_facts',
            text: memberNames.join(' '),
            confidence: memberNames.length > 0 ? 'high' : 'none',
            role: 'member_reference',
        },
        {
            source: 'fallback_display_name',
            text: fallbackDisplayName,
            confidence: fallbackDisplayName ? 'medium' : 'none',
            role: 'account_reference',
        },
        {
            source: 'text_blocks',
            text: blocks.join('\n\n'),
            confidence: blocks.length > 0 ? 'medium' : 'none',
            role: 'body_reference',
        },
    ].filter((item) => item.text)
}

function buildBiliupTagGenerationInput(
    article: Pick<Article, 'platform' | 'username' | 'u_id' | 'a_id' | 'content' | 'url' | 'type' | 'created_at'> & {
        translation?: string | null
    },
    texts: string[],
    candidate: BiliupUploadCandidate,
    targetCount: number,
) {
    const platform = Platform[article.platform] || String(article.platform)
    const dateTime = formatDateTimeParts(article.created_at, candidate.config.metadata_timezone)
    const textBlocks = collectTextBlocks(article, texts)
    const context = {
        title: candidate.title,
        description: candidate.description,
        source_url: candidate.sourceUrl,
        platform,
        source_tag: resolveBiliupSourceTag(article),
        type: article.type || '',
        user_id: article.u_id || '',
        username: article.username || '',
        article_id: article.a_id || '',
        current_tags: candidate.config.tags,
        target_count: targetCount,
        text: textBlocks.join('\n\n'),
        title_candidates: buildBiliupTitleCandidates(article, texts, candidate),
        evidence: {
            platform,
            source_tag: resolveBiliupSourceTag(article),
            type: article.type || '',
            source_url: candidate.sourceUrl,
            article_id: article.a_id || '',
            username: article.username || '',
            user_id: article.u_id || '',
            created_at: article.created_at || 0,
            date: dateTime.date,
            time: dateTime.time,
            deterministic_title: candidate.title,
            original_first_line: firstNonEmptyLine(article.content),
            translation_first_line: firstNonEmptyLine(article.translation),
            text_blocks: textBlocks,
        },
    }
    return JSON.stringify(context, null, 2)
}

type GeneratedBiliupMetadata = {
    tags: string[]
    titleZh?: string
}

type BiliupMetadataProcessorConfig = {
    provider: ProcessorProvider | string
    api_key: string
    cfg_processor?: ProcessorConfig
}

function parseGeneratedBiliupMetadata(value: string): GeneratedBiliupMetadata {
    const trimmed = String(value || '')
        .trim()
        .replace(/^```(?:json)?/i, '')
        .replace(/```$/i, '')
        .trim()
    if (!trimmed) {
        return { tags: [] }
    }
    try {
        const parsed = JSON.parse(trimmed)
        if (Array.isArray(parsed)) {
            return { tags: parsed.map(String) }
        }
        const titleZh =
            typeof parsed?.title_zh === 'string'
                ? parsed.title_zh
                : typeof parsed?.zh_title === 'string'
                  ? parsed.zh_title
                  : typeof parsed?.chinese_title === 'string'
                    ? parsed.chinese_title
                    : undefined
        if (Array.isArray(parsed?.tags) || titleZh) {
            return {
                tags: Array.isArray(parsed?.tags) ? parsed.tags.map(String) : [],
                titleZh,
            }
        }
    } catch {
        // Fall through to a lenient comma/newline split for non-JSON model output.
    }
    return {
        tags: trimmed
            .split(/[,\n，、]/)
            .map((tag) => tag.trim())
            .filter(Boolean),
    }
}

function escapeRegExpLiteral(value: string) {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function looseLiteralPattern(value: string) {
    return Array.from(value).map((char) => escapeRegExpLiteral(char)).join('\\s*')
}

function collectGeneratedTitleContextTerms(currentTitle: string) {
    const terms: string[] = []
    const structuredMatch = currentTitle.match(/^【([^】]+)】(?:\[[^\]]+\])?\s*(.*)$/)
    const accountTitle = normalizeTextBlock(structuredMatch?.[1] || '')
    const payload = normalizeTextBlock(structuredMatch?.[2] || currentTitle)

    if (accountTitle) {
        terms.push(accountTitle)
        terms.push(accountTitle.replace(/^22\/7\s+/i, ''))
        terms.push(...accountTitle.split(/\s+/))
    }

    const forcedNameMatch = payload.match(/^(.+?)\s+\d{2}[./-]\d{2}[./-]\d{2}\b/)
    if (forcedNameMatch?.[1]) {
        terms.push(forcedNameMatch[1])
        terms.push(...forcedNameMatch[1].split(/\s+/))
    }

    return Array.from(new Set(terms.map((term) => normalizeTextBlock(term)).filter((term) => Array.from(term).length >= 2)))
        .sort((a, b) => Array.from(b).length - Array.from(a).length)
}

function stripLeadingGeneratedTitleContext(value: string, currentTitle: string) {
    let title = value.trim()
    const terms = collectGeneratedTitleContextTerms(currentTitle)

    for (let index = 0; index < 8; index += 1) {
        const before = title
        title = title
            .replace(/^\s*(?:\d{2}[./-]\d{2}[./-]\d{2}|20\d{2}[./-]\d{1,2}[./-]\d{1,2})\s*(?:[:：,，、\-ー－—–|｜/／・])?\s*/u, '')
            .trim()

        for (const term of terms) {
            const pattern = new RegExp(
                `^\\s*${looseLiteralPattern(term)}\\s*(?:さん|ちゃん|様|氏)?\\s*(?:[:：,，、\\-ー－—–|｜/／・の])?\\s*`,
                'u',
            )
            const next = title.replace(pattern, '').trim()
            if (next !== title) {
                title = next
                break
            }
        }

        if (title === before) {
            break
        }
    }

    return title
}

function normalizeGeneratedBiliupTitle(
    value: string | null | undefined,
    currentTitle: string,
    bounds?: { minChars?: number; maxChars?: number },
) {
    const minChars = Math.max(1, Math.floor(bounds?.minChars ?? DEFAULT_BILIUP_TITLE_MIN_CHARS))
    const maxChars = Math.max(minChars, Math.floor(bounds?.maxChars ?? DEFAULT_BILIUP_TITLE_MAX_CHARS))
    const title = normalizeTextBlock(value)
        .replace(/[|｜\r\n]+/g, ' ')
        .replace(/\s+/g, ' ')
        .replace(/^[「『【《\[\]()（）\s]+|[」』】》\[\]()（）\s]+$/g, '')
        .trim()
    const strippedTitle = stripLeadingGeneratedTitleContext(title, currentTitle)
    if (!strippedTitle || Array.from(strippedTitle).length < minChars || Array.from(strippedTitle).length > maxChars) {
        return ''
    }
    if (!/[\u3400-\u9fff]/.test(strippedTitle) || /[\u3040-\u30ff]/.test(strippedTitle)) {
        return ''
    }
    const lowercaseTitle = strippedTitle.toLocaleLowerCase()
    if (BILIUP_FORBIDDEN_TITLE_TERMS.some((term) => lowercaseTitle.includes(term.toLocaleLowerCase()))) {
        return ''
    }
    const compactTitle = compactComparableText(strippedTitle)
    if (!compactTitle) {
        return ''
    }
    return strippedTitle
}

function appendOriginalPayloadToGeneratedTitle(generatedTitle: string, originalTitle: string) {
    const originalPayload = normalizeBiliupMainTitleText(originalTitle, '')
    if (!originalPayload) {
        return generatedTitle
    }

    const compactGenerated = compactComparableText(generatedTitle)
    const compactOriginal = compactComparableText(originalPayload)
    if (!compactOriginal || compactGenerated.includes(compactOriginal) || compactOriginal.includes(compactGenerated)) {
        return generatedTitle
    }
    return `${generatedTitle} | ${originalPayload}`
}

function buildGeneratedBiliupTitlePayload(generatedTitle: string, originalTitle: string, maxChars: number) {
    const combined = appendOriginalPayloadToGeneratedTitle(generatedTitle, originalTitle)
    if (Array.from(combined).length <= maxChars) {
        return combined
    }

    const originalPayload = normalizeBiliupMainTitleText(originalTitle, '')
    if (!originalPayload || combined === generatedTitle) {
        return truncateText(generatedTitle, maxChars)
    }

    const separator = ' | '
    const originalChars = Array.from(originalPayload)
    const minOriginalLength = Math.min(originalChars.length, 24)
    const generatedLimit = Math.max(4, maxChars - Array.from(separator).length - minOriginalLength)
    const fittedGenerated = truncateText(generatedTitle, generatedLimit)
    const originalLimit = maxChars - Array.from(fittedGenerated).length - Array.from(separator).length
    if (originalLimit >= 4) {
        return `${fittedGenerated}${separator}${truncateText(originalPayload, originalLimit)}`
    }
    return truncateText(combined, maxChars)
}

function replaceBiliupTitlePayloadWithGeneratedChinese(
    candidate: BiliupUploadCandidate,
    titleZh: string | undefined,
    originalTitle?: string,
    bounds?: { minChars?: number; maxChars?: number },
) {
    // Preserve the source title as a description-level reference anchor whenever the generated Chinese title
    // replaces it and the original would otherwise be lost (truncated or dropped from the bounded title).
    // Skip when the original is already fully visible in the title or description so we never duplicate it.
    function appendOriginalTitleReferenceToDescription(target: BiliupUploadCandidate, original?: string) {
        const originalNormalized = normalizeBiliupMainTitleText(original, '')
        if (!originalNormalized) {
            return
        }
        const compactOriginal = compactComparableText(originalNormalized)
        if (!compactOriginal) {
            return
        }
        const compactTitle = compactComparableText(target.title)
        const compactDescription = compactComparableText(target.description)
        if (compactTitle.includes(compactOriginal) || compactDescription.includes(compactOriginal)) {
            return
        }
        target.description = cleanupTemplateOutput(
            [target.description, `原标题: ${originalNormalized}`].filter(Boolean).join('\n'),
        )
    }

    const currentTitle = normalizeBiliupMainTitleText(candidate.title, candidate.title)
    const generatedTitle = normalizeGeneratedBiliupTitle(titleZh, currentTitle, bounds)
    if (!generatedTitle) {
        return false
    }

    const structuredMatch = currentTitle.match(/^(【[^】]+】(?:\[[^\]]+\])?\s*)(.+)$/)
    const fixedPrefix = structuredMatch?.[1] || ''
    const currentPayload = structuredMatch?.[2] || currentTitle
    const originalPayload = normalizeBiliupMainTitleText(originalTitle, '') || currentPayload
    if (!fixedPrefix) {
        candidate.title = buildGeneratedBiliupTitlePayload(generatedTitle, originalPayload, MAX_BILIUP_TITLE_CHARS)
        appendOriginalTitleReferenceToDescription(candidate, originalTitle)
        return true
    }

    const availableTitleLength = MAX_BILIUP_TITLE_CHARS - Array.from(fixedPrefix).length
    if (availableTitleLength < 4) {
        return false
    }
    candidate.title = `${fixedPrefix}${buildGeneratedBiliupTitlePayload(generatedTitle, originalPayload, availableTitleLength)}`
    appendOriginalTitleReferenceToDescription(candidate, originalTitle)
    return true
}

function stableJsonStringify(value: unknown): string {
    if (value === null || typeof value !== 'object') {
        return JSON.stringify(value)
    }
    if (Array.isArray(value)) {
        return `[${value.map((item) => stableJsonStringify(item)).join(',')}]`
    }
    const record = value as Record<string, unknown>
    return `{${Object.keys(record)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${stableJsonStringify(record[key])}`)
        .join(',')}}`
}

function stableProcessorConfigFingerprint(config?: ProcessorConfig) {
    return config ? stableJsonStringify(config) : ''
}

function isSameBiliupMetadataProcessor(
    left?: BiliupMetadataProcessorConfig,
    right?: BiliupMetadataProcessorConfig,
) {
    return Boolean(
        left &&
            right &&
            String(left.provider) === String(right.provider) &&
            String(left.api_key) === String(right.api_key) &&
            stableProcessorConfigFingerprint(left.cfg_processor) === stableProcessorConfigFingerprint(right.cfg_processor),
    )
}

function buildBiliupMetadataGenerationPrompt(
    cfgProcessor: ProcessorConfig | undefined,
    titleMinChars: number,
    titleMaxChars: number,
) {
    return (
        cfgProcessor?.prompt ||
        [
            '你是B站投稿元数据助手。请为22/7相关视频补充搜索友好的中文/日文标签，并在信息足够时给出克制的中文标题。',
            `固定已有标签必须保留；只输出JSON：{"tags":["标签1","标签2"],"title_zh":"中文标题或空字符串"}。`,
            '不要输出“搬运、转载、转帖、社媒、社交媒体、X、Twitter、Instagram、TikTok、YouTube、视频、短视频、投稿”等平台或搬运属性词。',
            '优先选择成员、22/7相关称呼、声优偶像、日系偶像、活动/内容主题。每个标签20字以内。',
            '输入JSON中的title_candidates和evidence是标题依据；优先使用source=translation_first_line/original_first_line/detected_member_facts的高置信事实。',
            `title_zh应为${titleMinChars}到${titleMaxChars}个中文字符，基于原文事实，不夸张、不偏颇、不脑补；信息不足则返回空字符串。`,
            'title_zh会放在固定账号/来源前缀之后，并与原标题用分隔符组合；不要重复账号名、平台名、日期、原标题或搬运属性词。',
        ].join('\n')
    )
}

async function runBiliupMetadataGeneration(
    article: Article,
    texts: string[],
    candidate: BiliupUploadCandidate,
    targetCount: number,
    processorConfig: BiliupMetadataProcessorConfig,
    titleBounds: { minChars: number; maxChars: number },
    log: Logger | undefined,
    label: string,
) {
    const prompt = buildBiliupMetadataGenerationPrompt(
        processorConfig.cfg_processor,
        titleBounds.minChars,
        titleBounds.maxChars,
    )
    try {
        const processor = await processorRegistry.create(processorConfig.provider, processorConfig.api_key, log, {
            ...(processorConfig.cfg_processor || {}),
            prompt,
        })
        try {
            const raw = await processor.process(buildBiliupTagGenerationInput(article, texts, candidate, targetCount))
            return parseGeneratedBiliupMetadata(raw)
        } finally {
            await processor.drop().catch(() => undefined)
        }
    } catch (error) {
        log?.warn(
            `Biliup ${label} generation failed for ${article.a_id || 'unknown'}; using deterministic fallback: ${
                error instanceof Error ? error.message : String(error)
            }`,
        )
        return null
    }
}

async function completeBiliupUploadCandidateTags(
    article: Article | undefined,
    texts: string[],
    candidate: BiliupUploadCandidate,
    log?: Logger,
) {
    const tagGeneration = candidate.config.tag_generation
    const titleGeneration = candidate.config.title_generation
    const targetCount = tagGeneration?.target_count || DEFAULT_BILIUP_TAG_TARGET_COUNT
    candidate.config.tags = uniqueBiliupTags(candidate.config.tags, targetCount)

    const needTags = Boolean(article && tagGeneration && candidate.config.tags.length < targetCount)
    const needTitle = Boolean(article && titleGeneration)

    if (article && (needTags || needTitle)) {
        const tagProcessor = tagGeneration
            ? {
                  provider: tagGeneration.provider,
                  api_key: tagGeneration.api_key,
                  cfg_processor: tagGeneration.cfg_processor,
              }
            : undefined
        const titleProcessor = titleGeneration
            ? {
                  provider: titleGeneration.provider,
                  api_key: titleGeneration.api_key,
                  cfg_processor: titleGeneration.cfg_processor,
              }
            : undefined
        const titleMinChars = titleGeneration?.target_min_chars ?? DEFAULT_BILIUP_TITLE_MIN_CHARS
        const titleMaxChars = titleGeneration?.target_max_chars ?? DEFAULT_BILIUP_TITLE_MAX_CHARS
        const titleBounds = { minChars: titleMinChars, maxChars: titleMaxChars }
        const applyGeneratedTags = (generated: GeneratedBiliupMetadata | null) => {
            if (!needTags || !generated) {
                return
            }
            candidate.config.tags = uniqueBiliupTags([...candidate.config.tags, ...generated.tags], targetCount)
        }
        const applyGeneratedTitle = (generated: GeneratedBiliupMetadata | null) => {
            if (!needTitle || !generated) {
                return
            }
            replaceBiliupTitlePayloadWithGeneratedChinese(
                candidate,
                generated.titleZh,
                extractOriginalBiliupTitleLine(article),
                titleBounds,
            )
        }

        if (needTags && needTitle && isSameBiliupMetadataProcessor(tagProcessor, titleProcessor)) {
            const generated = await runBiliupMetadataGeneration(
                article,
                texts,
                candidate,
                targetCount,
                tagProcessor!,
                titleBounds,
                log,
                'metadata',
            )
            applyGeneratedTags(generated)
            applyGeneratedTitle(generated)
        } else {
            if (needTags && tagProcessor) {
                applyGeneratedTags(
                    await runBiliupMetadataGeneration(
                        article,
                        texts,
                        candidate,
                        targetCount,
                        tagProcessor,
                        titleBounds,
                        log,
                        'tag',
                    ),
                )
            }
            if (needTitle && titleProcessor) {
                applyGeneratedTitle(
                    await runBiliupMetadataGeneration(
                        article,
                        texts,
                        candidate,
                        targetCount,
                        titleProcessor,
                        titleBounds,
                        log,
                        'title',
                    ),
                )
            }
        }
    }

    candidate.config.tags = completeTagsWithFallback(candidate.config.tags, targetCount)
    return candidate
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
        tid: resolveMinInteger(config.tid, DEFAULT_BILIUP_TID, 1),
        threads: resolveMinInteger(config.threads, DEFAULT_BILIUP_THREADS, 1),
        copyright: config.copyright === 1 ? 1 : 2,
        tags: uniqueBiliupTags(config.tags || []),
        tag_generation: resolveTagGenerationConfig(config.tag_generation),
        title_generation: resolveTitleGenerationConfig(config),
        exclude_uids: uniqueStrings([...(config.exclude_uids || []), ...DEFAULT_BILIUP_EXCLUDED_UIDS]),
        metadata_templates: resolveMetadataTemplatesConfig(config.metadata_templates),
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
    if (shouldSkipSallyMemberOnlyBiliupUpload(article, texts)) {
        return null
    }

    const videoPaths = uniqueStrings(media.filter((item) => item.media_type === 'video').map((item) => item.path))
    if (videoPaths.length === 0) {
        return null
    }

    const coverPath = media.find((item) => item.media_type === 'photo' || item.media_type === 'video_thumbnail')?.path
    return {
        title: deriveTitle(article, texts, resolvedConfig.metadata_timezone, resolvedConfig.metadata_templates?.title),
        description: deriveDescription(
            article,
            texts,
            resolvedConfig.metadata_timezone,
            resolvedConfig.metadata_templates?.description,
        ),
        sourceUrl: article.url,
        coverPath,
        videoPaths,
        config: {
            ...resolvedConfig,
            tags: deriveTags(article, texts, resolvedConfig.tags),
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
                    process.env.BROWSER_PROFILE_DIR ||
                    path.join(process.cwd(), 'assets', 'cookies', 'browser-profiles'),
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

async function prepareUploadVideoParts(
    candidate: Pick<BiliupUploadCandidate, 'videoPaths' | 'config'>,
    _uploadDir: string,
    log?: Logger,
): Promise<Array<PreparedUploadVideoPart>> {
    if ((candidate.config as any).collision_placeholder_part) {
        log?.warn('Ignoring deprecated biliup collision_placeholder_part; uploading original video part(s) only.')
    }
    return candidate.videoPaths.map((videoPath) => ({
        sourcePath: videoPath,
        stagedPath: videoPath,
    }))
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
            log?.warn(
                `Biliup browser cookie sync failed, will try fallback credentials: ${browserCookieSyncError.message}`,
            )
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
            log?.warn(
                `Invalid biliup cookie file ${candidate.config.cookie_file}, falling back if possible: ${message}`,
            )
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
    log?.info(
        `Prepared biliup metadata for ${article.a_id}: title_length=${candidate.title.length} title_hash=${createHash(
            'sha256',
        )
            .update(candidate.title)
            .digest('hex')
            .slice(0, 12)}`,
    )
    log?.info(
        `Prepared biliup video parts for ${article.a_id}: requested=${candidate.videoPaths.length} actual=${
            preparedVideoParts.length
        } titles=${preparedVideoParts
            .map((part, index) => path.basename(part.stagedPath) || `part-${index + 1}`)
            .join(',')}`,
    )

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
            reject(
                new Error(
                    `biliup exited with code ${code}: ${stderrChunks.join('').trim() || stdoutChunks.join('').trim()}`,
                ),
            )
        })
    }).finally(() => {
        try {
            fs.rmSync(cookieFile, { force: true })
        } catch {}
    })

    const stdout = stdoutChunks.join('')
    const stderr = stderrChunks.join('')
    const lastJsonLine = stdout
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line.startsWith('{') && line.endsWith('}'))
        .at(-1)
    if (lastJsonLine) {
        try {
            const payload = JSON.parse(lastJsonLine) as Record<string, any>
            const submitData = payload.submit_result?.data || payload.submit_result || {}
            log?.info(
                `Biliup upload completed for ${article.a_id}: parts=${preparedVideoParts.length} bvid=${
                    submitData.bvid || ''
                } aid=${submitData.aid || ''}`,
            )
        } catch {
            log?.info(`Biliup upload completed for ${article.a_id}: parts=${preparedVideoParts.length}`)
        }
    } else {
        log?.info(`Biliup upload completed for ${article.a_id}: parts=${preparedVideoParts.length}`)
    }

    return {
        stdout,
        stderr,
    }
}

export {
    DEFAULT_BILIUP_EXCLUDED_UIDS,
    buildBiliupUploadCandidate,
    buildCookieDocument,
    completeBiliupUploadCandidateTags,
    normalizeBiliupCookieDocument,
    resolveBrowserCookieSyncConfig,
    resolveVideoUploadConfig,
    runBiliupUpload,
    runBrowserCookieSync,
    prepareUploadVideoParts,
}
export type { BiliupUploadCandidate, ResolvedBiliupVideoUploadConfig }
