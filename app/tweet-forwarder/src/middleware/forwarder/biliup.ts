import { CACHE_DIR_ROOT } from '@/config'
import type { Article } from '@/db'
import { processorRegistry } from '@/middleware/processor'
import { formatPlatformTag } from '@/services/render-service'
import { convertXHashtagsToBiliFormat } from '@/utils/bili-hashtag'
import type { BiliupVideoUploadConfig } from '@/types/forwarder'
import type { ProcessorConfig, ProcessorProvider } from '@/types/processor'
import type { Logger } from '@idol-bbq-utils/log'
import type { BrowserMode } from '@idol-bbq-utils/spider'
import { Platform, type MediaType } from '@idol-bbq-utils/spider/types'
import { spawn } from 'child_process'
import { createHash } from 'crypto'
import fs from 'fs'
import path from 'path'

const DEFAULT_BILIUP_TID = 160
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
const BILIUP_ORIGINAL_TITLE_LABEL = '原标题:'
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
// Platform/repost words that must never appear in a Bilibili upload title: they carry no
// search value and read as 搬运 markers. Long forms are removed anywhere in the payload;
// short abbreviations only when standalone (so fixtures like "TT短视频正文" keep their text).
const BILIUP_TITLE_SCRUB_LONG_WORDS = [
    'youtube',
    'youtubeチャンネル',
    'twitter',
    'tiktok',
    'instagram',
    'ユーチューブ',
    'ツイッター',
    'ツイート',
    'インスタ',
    'ティックタック',
    'ティックトック',
    '推特',
    '搬运',
    '转载',
    '转帖',
    '転載',
    'リポスト',
    '社媒',
    '社交媒体',
]
const BILIUP_TITLE_SCRUB_SHORT_TOKENS = ['yt', 'tt', 'ins', 'ig', 'x', 'rt']
const BILIUP_TITLE_BREAK_CHARS = new Set([
    ' ',
    '\u3000',
    '·',
    '・',
    ':',
    '：',
    '|',
    '｜',
    '、',
    '，',
    ',',
    '/',
    '／',
    '—',
    '–',
    '-',
    '~',
    '～',
])
const BILIUP_TITLE_CLOSING_BRACKETS = '】」』）)]〉》'
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

/**
 * Title-generation task mode.
 * - `translate_original_title`: the source carries a genuine creator-authored title (YouTube). The
 *   Bilibili main title is the faithful Chinese translation of that original title; the original
 *   title is the only translation material and the model must not invent a different title.
 * - `summarize_post_title`: no real title exists (X/Instagram/TikTok captions); the model condenses
 *   the post body into a Chinese title, staying faithful to the source facts.
 */
type BiliupTitleTask = 'translate_original_title' | 'summarize_post_title'

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

/**
 * Title-safe truncation: cut at the last semantic boundary (separator or closing bracket)
 * inside the budget so we never split a word/name mid-token, and mark the cut with a single
 * ellipsis. Falls back to a hard cut when no boundary exists in the lower half of the window.
 */
function truncateBiliupTitleText(value: string, maxChars: number) {
    const chars = Array.from(value)
    if (chars.length <= maxChars) {
        return value
    }
    const budget = Math.max(2, maxChars - 1)
    let cut = -1
    for (let index = budget - 1; index >= Math.floor(budget * 0.5); index -= 1) {
        const char = chars[index]!
        if (BILIUP_TITLE_CLOSING_BRACKETS.includes(char)) {
            cut = index + 1
            break
        }
        if (BILIUP_TITLE_BREAK_CHARS.has(char)) {
            cut = index
            break
        }
    }
    const end = cut >= 0 ? cut : budget
    const head = chars
        .slice(0, end)
        .join('')
        .replace(/[\s·・:：|｜、，,／/—–\-~～]+$/u, '')
        .trim()
    return `${head || chars.slice(0, budget).join('')}…`
}

function escapeTitleScrubWord(word: string) {
    return word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

/**
 * Deterministic title payload cleanup: drop platform/repost words and tame emoji floods
 * (3+ pictographs in a row collapse to 2) while keeping reasonable symbols intact.
 */
function scrubBiliupTitlePayload(value: string) {
    let payload = normalizeTextBlock(value).replace(/\s+/g, ' ').trim()
    for (const word of BILIUP_TITLE_SCRUB_LONG_WORDS) {
        payload = payload.replace(new RegExp(escapeTitleScrubWord(word), 'giu'), ' ')
    }
    for (const token of BILIUP_TITLE_SCRUB_SHORT_TOKENS) {
        payload = payload.replace(
            new RegExp(`(?<![A-Za-z0-9\\u3400-\\u9fff\\u3040-\\u30ff])${escapeTitleScrubWord(token)}(?![A-Za-z0-9\\u3400-\\u9fff\\u3040-\\u30ff])`, 'giu'),
            ' ',
        )
    }
    payload = payload.replace(
        /(?:\p{Extended_Pictographic}(?:\ufe0f|[\u{1f3fb}-\u{1f3ff}]|\u200d\p{Extended_Pictographic}(?:\ufe0f|[\u{1f3fb}-\u{1f3ff}])*)+)/gu,
        (run) => {
            const units = run.split('\u200d')
            return (run.match(/\p{Extended_Pictographic}/gu) || []).length <= 2 ? run : units.slice(0, 2).join('\u200d')
        },
    )
    return payload.replace(/\s+/g, ' ').trim()
}

function countTitleScriptChars(value: string) {
    return {
        han: (value.match(/[\u3400-\u9fff]/g) || []).length,
        kana: (value.match(/[\u3040-\u30ff]/g) || []).length,
    }
}

/**
 * A title is "untranslated" when it carries no Chinese at all, or is still mostly Japanese
 * kana (the model echoed the source). Kana inside otherwise-Chinese titles is legitimate:
 * glossary policy keeps some song/program titles in Japanese (e.g. 「命の続き」).
 */
function looksUntranslatedBiliupTitle(value: string) {
    const { han, kana } = countTitleScriptChars(value)
    if (han === 0) {
        return true
    }
    return kana >= 4 && kana > han * 2
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
        // Repo-root assets when running from app/tweet-forwarder (local tests, host tooling).
        path.join(process.cwd(), '../../assets/knowledge/22_7/facts/members.json'),
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
    return uniqueBiliupMemberFacts([
        resolveBiliupMemberFact(article),
        ...resolveMentionedBiliupMemberFacts(article, texts),
    ])
}

type BiliupTranslationGlossaryEntry = {
    source: string
    target: string
    note?: string
}

let biliupTranslationGlossaryCache: Array<BiliupTranslationGlossaryEntry> | null | undefined

/**
 * 22/7 ja→zh glossary (`assets/knowledge/22_7/translation/translation_terms_22_7_ja_zh.json`).
 * Shipped into the title-translation prompt so member/character/work names render exactly like
 * the rest of the pipeline's translations.
 */
function resolveBiliupTranslationGlossary(): Array<BiliupTranslationGlossaryEntry> {
    if (biliupTranslationGlossaryCache !== undefined) {
        return biliupTranslationGlossaryCache
    }

    const candidates = [
        path.join(process.cwd(), 'assets/knowledge/22_7/translation/translation_terms_22_7_ja_zh.json'),
        // Repo-root assets when running from app/tweet-forwarder (local tests, host tooling).
        path.join(process.cwd(), '../../assets/knowledge/22_7/translation/translation_terms_22_7_ja_zh.json'),
        '/app/assets/knowledge/22_7/translation/translation_terms_22_7_ja_zh.json',
    ]
    const sourcePath = candidates.find((candidate) => fs.existsSync(candidate))
    if (!sourcePath) {
        biliupTranslationGlossaryCache = []
        return biliupTranslationGlossaryCache
    }

    try {
        const payload = JSON.parse(fs.readFileSync(sourcePath, 'utf8')) as Array<Record<string, unknown>>
        biliupTranslationGlossaryCache = (Array.isArray(payload) ? payload : [])
            .map((entry) => ({
                source: String(entry?.source || '').trim(),
                target: String(entry?.target || '').trim(),
                note: String(entry?.note || '').trim() || undefined,
            }))
            .filter((entry) => entry.source && entry.target)
    } catch {
        biliupTranslationGlossaryCache = []
    }
    return biliupTranslationGlossaryCache
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
        ''
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

/**
 * Conditional title source tag: `{{source_tag}}` renders to the bare tag or to ''.
 *
 * The tag is kept whenever the title cannot identify its own source — X/Instagram/TikTok
 * (summarized caption titles), YouTube Shorts, YouTube entries without a creator original
 * title, and the date-shell fallback (`【22/7】MM.DD_YY`, zero content signal). It is
 * dropped only for YouTube long videos whose title IS the translation of the creator's
 * original title: that title self-identifies its source. website/blog is not in the omit
 * list and keeps its tag.
 */
function resolveTitleSourceTag(
    article: Pick<Article, 'content' | 'platform' | 'type'>,
    hasTitlePayload: boolean,
) {
    const titleIsTranslatedOriginalTitle =
        article.platform === Platform.YouTube &&
        article.type !== 'shorts' &&
        Boolean(extractOriginalBiliupTitleLine(article)) &&
        hasTitlePayload
    return titleIsTranslatedOriginalTitle ? '' : resolveBiliupSourceTag(article)
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
        date_code: `${parts.month}.${parts.day}_${String(parts.year || '').slice(-2)}`,
        time,
        datetime: `${date} ${time}`,
    }
}

function resolveBiliupTitleHeadline(primaryLine: string) {
    // Headline is capped at 40 chars (code points, surrogate-safe) so the fixed
    // account/source prefix keeps most of the 80-char title budget. The cut is
    // semantic (word/segment boundary) and platform/repost words are scrubbed first.
    return truncateBiliupTitleText(scrubBiliupTitlePayload(primaryLine), 40)
}

function resolveUploadSummary(
    article: Pick<Article, 'platform'>,
    summary: string,
    displayName: string,
    dateTime: ReturnType<typeof formatDateTimeParts>,
) {
    if ([Platform.Instagram, Platform.TikTok, Platform.X].includes(article.platform)) {
        // upload_summary drops the display name on purpose: account_title already
        // carries it, repeating it after [TT]/[X] reads as a stutter.
        return [dateTime.date_code, summary].filter(Boolean).join(' ').trim() || dateTime.date_code
    }
    return [dateTime.date_code, summary].filter(Boolean).join(' ').trim() || `${displayName} ${dateTime.datetime}`.trim()
}

function resolveBiliupAccountTitle(displayName: string) {
    // The uploader's Bilibili channel is the 22/7 fan channel: the brand prefix is always
    // present so titles stay searchable and consistent even for unknown accounts.
    if (!displayName) {
        return '22/7'
    }
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
    // Website articles (official blog/news/live-report) carry author-written
    // Japanese titles; their display headline stays original, never translated.
    const primaryLine =
        (article.platform === Platform.Website
            ? firstNonEmptyLine(article.content)
            : blocks
                  .flatMap((value) => value.split('\n'))
                  .map((line) => line.trim())
                  .find(Boolean)) || ''
    const dateTime = formatDateTimeParts(article.created_at, timeZone)
    const displayName = resolveDisplayName(article, texts)
    const summary = primaryLine || dateTime.datetime
    const bodyWithoutRepeatedSummary = stripDuplicateLeadingSummary(blocks[0] || '', summary)
    const body = bodyWithoutRepeatedSummary || blocks[0] || ''
    const headline = resolveBiliupTitleHeadline(primaryLine)
    const uploadSummary = resolveUploadSummary(article, headline, displayName, dateTime)

    return {
        account_title: resolveBiliupAccountTitle(displayName),
        article_id: article.a_id,
        body,
        body_or_summary: body || summary,
        date: dateTime.date,
        date_short: dateTime.date_short,
        date_code: dateTime.date_code,
        datetime: dateTime.datetime,
        display_name: displayName,
        platform_label: resolvePlatformLabel(article),
        platform_type_label: resolvePlatformTypeLabel(article),
        source_tag: resolveTitleSourceTag(article, Boolean(headline)),
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
    const rendered = template.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_, key: string) => context[key] || '')
    return stripEmptyRenderedBrackets(rendered)
}

function stripEmptyRenderedBrackets(value: string) {
    return value
        .replace(/【\s*】/g, '')
        .replace(/「\s*」/g, '')
        .replace(/『\s*』/g, '')
        .replace(/（\s*）/g, '')
        .replace(/\(\s*\)/g, '')
        // An empty `[...]` source slot disappears together with one trailing horizontal
        // space, so an omitted source tag renders exactly like the tag-less template
        // (`【acc】MM.DD_YY …`, no stray space after `】`).
        .replace(/\[[ \t]*\][ \t]?/g, '')
        .replace(/\[\s*\]/g, '')
        .replace(/\s{2,}/g, ' ')
        .trim()
}

function cleanupTemplateOutput(value: string) {
    return value
        .replace(/[ \t]+\n/g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim()
}

/**
 * Uniform default title template for every source: `【account】[source] MM.DD_YY <title payload>`.
 * The `[{{source_tag}}]` slot is *conditional* (see `resolveTitleSourceTag`): it renders to the
 * bare tag (`[YT]`/`[X]`/`[ins]`/`[TT]`/`[blog]`) when the title cannot identify its own source,
 * and to nothing for YouTube long videos whose title is the original title's translation.
 * The `【22/7 …】` brand prefix stays as the search anchor and the `MM.DD_YY` date code keeps
 * chronological scan/dedupe value; the source is always credited in the description (`来源平台:`
 * line) regardless of the title slot.
 */
function resolveDefaultTitleTemplate(_article: Pick<Article, 'platform' | 'type'>) {
    return '【{{account_title}}】[{{source_tag}}] {{upload_summary}}'
}

function buildTitleFallback(context: TemplateContext, article: Pick<Article, 'platform' | 'username' | 'a_id'>) {
    const hardFallback = normalizeBiliupMainTitleText(
        [formatPlatformTag(article), context.datetime, context.article_id].filter(Boolean).join(' '),
        context.article_id || 'Bilibili upload',
    )
    return normalizeBiliupMainTitleText(
        stripEmptyRenderedBrackets(`【${context.account_title}】[${context.source_tag}] ${context.upload_summary}`),
        hardFallback,
    )
}

/**
 * Video sources that carry a genuine creator-authored title (YouTube videos and Shorts).
 * For these the original title is the translation material for the Bilibili main title and
 * stays verbatim as a leading `原标题:` line in the description. Social captions
 * (X/Instagram/TikTok) and website articles are excluded: their first line is body/caption
 * text, not a real title.
 */
function hasOriginalVideoTitle(article: Pick<Article, 'platform'>) {
    return article.platform === Platform.YouTube
}

function resolveBiliupTitleTask(
    article: Pick<Article, 'content' | 'platform'>,
): BiliupTitleTask {
    return hasOriginalVideoTitle(article) && extractOriginalBiliupTitleLine(article)
        ? 'translate_original_title'
        : 'summarize_post_title'
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
    return truncateBiliupTitleText(normalizeBiliupMainTitleText(title, fallback), MAX_BILIUP_TITLE_CHARS)
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
    const hasAnnouncement = /公開|已公开|已發布|已发布|发布|發布|公開しました|released|is out|now available/i.test(text)
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

function resolveDescriptionBody(article: Pick<Article, 'content' | 'platform' | 'type'>, context: TemplateContext) {
    const body = context.body_or_summary || ''
    if (!hasOriginalVideoTitle(article)) {
        return body
    }
    return (
        stripLeadingYoutubeTitleAnnouncements(
            body,
            extractOriginalBiliupTitleLine(article),
            String(context.summary || ''),
        ) || body
    )
}

function prependOriginalBiliupTitle(description: string, originalTitle: string) {
    const originalNormalized = normalizeBiliupMainTitleText(originalTitle, '')
    if (!originalNormalized) {
        return cleanupTemplateOutput(description)
    }

    const compactOriginal = compactComparableText(originalNormalized)
    const lines = cleanupTemplateOutput(description)
        .split('\n')
        .filter((line) => !line.trim().startsWith(BILIUP_ORIGINAL_TITLE_LABEL))
    while (lines[0] !== undefined && !lines[0]!.trim()) {
        lines.shift()
    }
    if (compactOriginal && compactComparableText(lines[0] || '') === compactOriginal) {
        lines.shift()
        while (lines[0] !== undefined && !lines[0]!.trim()) {
            lines.shift()
        }
    }

    const body = cleanupTemplateOutput(lines.join('\n'))
    return cleanupTemplateOutput(
        [`${BILIUP_ORIGINAL_TITLE_LABEL} ${originalNormalized}`, body].filter(Boolean).join('\n\n'),
    )
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
    const originalTitle = hasOriginalVideoTitle(article) ? extractOriginalBiliupTitleLine(article) : ''
    if (template) {
        return prependOriginalBiliupTitle(renderTemplate(template, context), originalTitle)
    }

    const body = resolveDescriptionBody(article, context)
    const sections = [
        body,
        `来源平台: ${context.platform_type_label}`,
        context.display_name ? `来源账号: ${context.display_name}` : '',
        context.user_id ? `账号标识: ${context.user_id}` : '',
        `发布时间: ${context.datetime}`,
        context.url ? `原链接: ${context.url}` : '',
    ]
    return prependOriginalBiliupTitle(sections.filter(Boolean).join('\n'), originalTitle)
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
    const tags =
        facts.length > 0 || isBiliupCollectionDisplayName(displayName)
            ? facts.map((fact) => fact.names?.ja || '')
            : [displayName]
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

function buildBiliupMetadataGenerationInput(
    article: Pick<Article, 'platform' | 'username' | 'u_id' | 'a_id' | 'content' | 'url' | 'type' | 'created_at'> & {
        translation?: string | null
    },
    texts: string[],
    candidate: BiliupUploadCandidate,
    targetCount: number,
    task: BiliupTitleTask | null,
    titleBounds: { minChars: number; maxChars: number },
) {
    const platform = Platform[article.platform] || String(article.platform)
    const dateTime = formatDateTimeParts(article.created_at, candidate.config.metadata_timezone)
    const textBlocks = collectTextBlocks(article, texts)
    const context: Record<string, unknown> = {
        task: task || 'generate_tags_only',
        text: textBlocks.join('\n\n'),
        glossary: resolveBiliupTranslationGlossary(),
        current_tags: candidate.config.tags,
        target_count: targetCount,
        source_context: {
            platform,
            source_tag: resolveBiliupSourceTag(article),
            type: article.type || '',
            username: article.username || '',
            user_id: article.u_id || '',
            article_id: article.a_id || '',
            source_url: candidate.sourceUrl,
            date: dateTime.date,
            time: dateTime.time,
        },
    }
    if (task) {
        context.title_min_chars = titleBounds.minChars
        context.title_max_chars = titleBounds.maxChars
    }
    if (task === 'translate_original_title') {
        // The only translation material: the model must translate this title, nothing else.
        context.original_title = extractOriginalBiliupTitleLine(article)
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

function parseGeneratedBiliupMetadata(value: string, allowPlainTextTitle = false): GeneratedBiliupMetadata {
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
                    : typeof parsed?.title === 'string' && typeof parsed?.tags !== 'object'
                      ? parsed.title
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
    // A translation model answering with the plain translated title (no JSON) is still useful:
    // when title output was requested and the text is a single line, treat it as the title.
    if (allowPlainTextTitle && !/[\n,，、]/.test(trimmed) && Array.from(trimmed).length <= MAX_BILIUP_TITLE_CHARS) {
        return { tags: [], titleZh: trimmed }
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
    return Array.from(value)
        .map((char) => escapeRegExpLiteral(char))
        .join('\\s*')
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

    return Array.from(
        new Set(terms.map((term) => normalizeTextBlock(term)).filter((term) => Array.from(term).length >= 2)),
    ).sort((a, b) => Array.from(b).length - Array.from(a).length)
}

function stripLeadingGeneratedTitleContext(value: string, currentTitle: string) {
    let title = value.trim()
    const terms = collectGeneratedTitleContextTerms(currentTitle)

    for (let index = 0; index < 8; index += 1) {
        const before = title
        title = title
            .replace(
                /^\s*(?:\d{2}[./-]\d{2}[./-]\d{2}|20\d{2}[./-]\d{1,2}[./-]\d{1,2})\s*(?:[:：,，、\-ー－—–|｜/／・])?\s*/u,
                '',
            )
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
    bounds: { minChars?: number; maxChars?: number },
    task: BiliupTitleTask,
) {
    // Translations may legitimately be short (a faithful short title beats an invented long one),
    // so translate mode only enforces a minimal sanity floor; summarize mode keeps the configured
    // target minimum to push the model toward informative titles.
    const minChars = task === 'translate_original_title' ? 2 : Math.max(1, Math.floor(bounds?.minChars ?? DEFAULT_BILIUP_TITLE_MIN_CHARS))
    const title = scrubBiliupTitlePayload(
        normalizeTextBlock(value)
            .replace(/[|｜\r\n]+/g, ' ')
            .replace(/\s+/g, ' ')
            .replace(/^[「『【《\[\]()（）\s]+|[」』】》\[\]()（）\s]+$/g, '')
            .trim(),
    )
    // Translate mode keeps the translation verbatim (faithful translation, no second editing);
    // summarize mode may strip model-echoed context (account name + posting date).
    const strippedTitle =
        task === 'translate_original_title' ? title : stripLeadingGeneratedTitleContext(title, currentTitle)
    if (!strippedTitle || Array.from(strippedTitle).length < minChars) {
        return ''
    }
    if (looksUntranslatedBiliupTitle(strippedTitle)) {
        return ''
    }
    const lowercaseTitle = strippedTitle.toLocaleLowerCase()
    if (BILIUP_FORBIDDEN_TITLE_TERMS.some((term) => lowercaseTitle.includes(term.toLocaleLowerCase()))) {
        return ''
    }
    if (!compactComparableText(strippedTitle)) {
        return ''
    }
    return strippedTitle
}

function splitBiliupTitleStructure(title: string) {
    const normalized = normalizeBiliupMainTitleText(title, title)
    // Fixed part keeps the exact rendered spacing: `【account】` + optional `[source]` +
    // optional `MM.DD_YY` posting-date code. Only the trailing payload is replaceable.
    const match = normalized.match(/^(\s*【[^】]+】(?:\s*\[[^\]]+\])?\s*(?:\d{2}\.\d{2}_\d{2})?\s*)(.*)$/)
    return {
        head: match?.[1] || '',
        dateCode: match?.[1]?.match(/(\d{2}\.\d{2}_\d{2})/)?.[1] || '',
        payload: (match?.[2] || normalized).trim(),
    }
}

function replaceBiliupTitlePayloadWithGeneratedChinese(
    candidate: BiliupUploadCandidate,
    titleZh: string | undefined,
    options: {
        task: BiliupTitleTask
        bounds?: { minChars?: number; maxChars?: number }
    },
) {
    const currentTitle = normalizeBiliupMainTitleText(candidate.title, candidate.title)
    const generatedTitle = normalizeGeneratedBiliupTitle(titleZh, currentTitle, options.bounds || {}, options.task)
    if (!generatedTitle) {
        return false
    }

    const { head, dateCode, payload: _oldPayload } = splitBiliupTitleStructure(currentTitle)
    let payload = generatedTitle
    // The fixed part already carries the MM.DD_YY posting-date code; never duplicate it
    // when the model echoes it back inside the title payload.
    const echoedDate = dateCode ? payload.match(/^\d{2}\.\d{2}_\d{2}[\s:：,，、\-ー－—–|｜/／・]+/)?.[0] : undefined
    if (echoedDate && echoedDate.includes(dateCode)) {
        payload = payload.slice(echoedDate.length).trim()
    }
    if (!head) {
        // Unstructured custom template: replace the whole title, keeping the date code alive.
        const looseDateCode = currentTitle.match(/(\d{2}\.\d{2}_\d{2})/)?.[1] || ''
        if (looseDateCode && !payload.includes(looseDateCode)) {
            payload = `${looseDateCode} ${payload}`.trim()
        }
        candidate.title = truncateBiliupTitleText(payload, MAX_BILIUP_TITLE_CHARS)
        return true
    }

    const availableTitleLength = MAX_BILIUP_TITLE_CHARS - Array.from(head).length
    if (availableTitleLength < 4 || !payload) {
        return false
    }
    candidate.title = `${head}${truncateBiliupTitleText(payload, availableTitleLength)}`
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

function isSameBiliupMetadataProcessor(left?: BiliupMetadataProcessorConfig, right?: BiliupMetadataProcessorConfig) {
    return Boolean(
        left &&
            right &&
            String(left.provider) === String(right.provider) &&
            String(left.api_key) === String(right.api_key) &&
            stableProcessorConfigFingerprint(left.cfg_processor) ===
                stableProcessorConfigFingerprint(right.cfg_processor),
    )
}

const BILIUP_TAG_GENERATION_RULES = [
    '你是B站投稿元数据助手。只输出一个JSON对象：{"tags":["标签1","标签2"],"title_zh":"中文标题"}。',
    'tags规则：保留输入current_tags里的固定标签；优先 成员名/作品名/节目名/活动主题/声优偶像/日系偶像；每个标签20字以内。',
    '不要输出“搬运、转载、转帖、社媒、社交媒体、X、Twitter、Instagram、TikTok、YouTube、视频、短视频、投稿”等平台或搬运属性词。',
]

const BILIUP_TITLE_TRANSLATION_RULES = (titleMinChars: number, titleMaxChars: number) => [
    'title_zh规则（本次任务：翻译原标题）：',
    '- title_zh必须是输入original_title的忠实简体中文翻译：original_title是唯一翻译素材，不得改写、扩写、概括或另拟标题，不得补写原文没有的时间、地点、人物、评价。',
    '- 保留成员名、角色名、作品名、曲名、节目名、数字、日期、期数、序号、记号与原文结构；不截断、不漏译关键名词。',
    '- 专有名词按输入glossary的target写法翻译并保持一致（含note里的“avoid”警告）；glossary未收录的专有名词保守处理，可保留原文。',
    '- 用自然简体中文，不逐词硬译、不带翻译腔；不使用“震惊、必看、神回”等标题党词；不加解释、译注、引号装饰或表情符号堆砌。',
    `- 长度目标${titleMinChars}到${titleMaxChars}字：宁可完整传达原标题信息，超长时省略次要修饰语，绝不截断半个词。`,
    '示例：',
    'original_title: 【ナナコミ】第12回 配信開始 → title_zh: 《NANA-COMI》第12回 开始直播',
    'original_title: 天城サリー『推しカメラ』#3 を公開 → title_zh: 天城莎莉《推し相机》#3 公开',
    'original_title: 北原実咲のうさぎの話 → title_zh: 北原实咲的兔子话题',
]

const BILIUP_TITLE_SUMMARIZE_RULES = (titleMinChars: number, titleMaxChars: number) => [
    'title_zh规则（本次任务：概括标题，来源没有独立标题）：',
    '- 把输入text开头的主要信息忠实翻译并浓缩成简体中文标题：先逐句翻译正文开头的事实，只在原文过长时压缩次要修饰；不得脑补原文没有的信息。',
    `title_zh应为${titleMinChars}到${titleMaxChars}个中文字符，具体描述视频内容或事件本身（例如“北原実咲介绍自己养的兔子”“推し相机：强肩偶像把签名球送到2层观众席”），不夸张、不偏颇、不脑补。`,
    '禁止把成员名、日期、时间、账号名或平台名作为标题主体（例如“北原実咲 26.08.06”这类仅成员名加日期的标题）；只要输入包含可描述的事实就必须给出能反映内容的标题，不得用空字符串或缩略概括替代。',
    '- 保留成员名、作品名、数字、日期；专有名词按输入glossary的target写法；不使用标题党词，不加解释或译注。',
    '- title_zh会放在固定账号/日期前缀之后作为主标题；不要重复账号名、平台名、日期、原标题或搬运属性词。',
]

function buildBiliupMetadataGenerationPrompt(
    cfgProcessor: ProcessorConfig | undefined,
    titleMinChars: number,
    titleMaxChars: number,
    task: BiliupTitleTask | null,
) {
    return (
        cfgProcessor?.prompt ||
        [
            ...BILIUP_TAG_GENERATION_RULES,
            ...(task === 'translate_original_title'
                ? BILIUP_TITLE_TRANSLATION_RULES(titleMinChars, titleMaxChars)
                : task === 'summarize_post_title'
                  ? BILIUP_TITLE_SUMMARIZE_RULES(titleMinChars, titleMaxChars)
                  : []),
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
    task: BiliupTitleTask | null,
    log: Logger | undefined,
    label: string,
) {
    const prompt = buildBiliupMetadataGenerationPrompt(
        processorConfig.cfg_processor,
        titleBounds.minChars,
        titleBounds.maxChars,
        task,
    )
    try {
        const processor = await processorRegistry.create(processorConfig.provider, processorConfig.api_key, log, {
            ...(processorConfig.cfg_processor || {}),
            prompt,
        })
        try {
            const raw = await processor.process(
                buildBiliupMetadataGenerationInput(article, texts, candidate, targetCount, task, titleBounds),
            )
            return parseGeneratedBiliupMetadata(raw, Boolean(task))
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
    const hasTranslatableMetadata = article ? collectTextBlocks(article, texts).length > 0 : false
    // Website articles already carry author-written display titles; never run
    // them through LLM title translation/rewrite.
    const needTitle = Boolean(
        article && article.platform !== Platform.Website && titleGeneration && hasTranslatableMetadata,
    )
    const titleTask = article ? resolveBiliupTitleTask(article) : null

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
            if (!needTitle || !generated || !titleTask) {
                return
            }
            replaceBiliupTitlePayloadWithGeneratedChinese(candidate, generated.titleZh, {
                task: titleTask,
                bounds: titleBounds,
            })
        }

        if (needTags && needTitle && isSameBiliupMetadataProcessor(tagProcessor, titleProcessor)) {
            const generated = await runBiliupMetadataGeneration(
                article,
                texts,
                candidate,
                targetCount,
                tagProcessor!,
                titleBounds,
                titleTask,
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
                        null,
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
                        titleTask,
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

function normalizeBrowserMode(value?: BrowserMode): BrowserMode {
    return value === 'headed-xvfb' || value === 'headless' ? value : 'headless'
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

    const coverPath =
        media.find((item) => item.media_type === 'video_thumbnail')?.path ||
        media.find((item) => item.media_type === 'photo')?.path
    // X hashtags inside the source caption must reach Bilibili in the paired `#tag#`
    // form. Title/description are derived from article.content/translation as well as
    // the outbound texts, so convert at this output boundary instead of per-input.
    return {
        title: convertXHashtagsToBiliFormat(
            deriveTitle(article, texts, resolvedConfig.metadata_timezone, resolvedConfig.metadata_templates?.title),
        ),
        description: convertXHashtagsToBiliFormat(
            deriveDescription(
                article,
                texts,
                resolvedConfig.metadata_timezone,
                resolvedConfig.metadata_templates?.description,
            ),
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
    // Article ids can be URL-derived or otherwise contain path separators; a raw
    // id in mkdtempSync would create a nested path and fail with ENOENT.
    const uploadPrefix = String(article.a_id || 'article')
        .replace(/[^A-Za-z0-9._-]/g, '_')
        .slice(0, 80)
    const uploadDir = fs.mkdtempSync(path.join(candidate.config.working_dir, `${uploadPrefix}-`))
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
            fs.rmSync(uploadDir, { recursive: true, force: true })
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
    isSallyMemberOnlyBiliupHandle,
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
