import dayjs from 'dayjs'
import { BaseProcessor } from './base'
import { type ProcessorConfig, ProcessorProvider } from '@/types/processor'
import { Logger } from '@idol-bbq-utils/log'

type MechanicalAction = 'extract' | 'merge'
type DigestSection = 'content' | 'extra_content' | 'media_urls' | null

interface MechanicalOptions {
    merge_window_minutes?: number
    merge_window_seconds?: number
    group_by_user?: boolean
    group_by_platform?: boolean
    min_group_size?: number
    include_source_url?: boolean
    url_allow_patterns?: string[] | string
    url_block_patterns?: string[] | string
    max_results?: number
}

interface DigestArticle {
    index: number
    raw: string
    timestamp: string | null
    created_at: number | null
    db_id: number | null
    article_id: string | null
    platform: string | null
    user_id: string | null
    username: string | null
    url: string | null
    content: string
    extra_content: string | null
    media_urls: string[]
}

interface UrlCandidate {
    url: string
    normalized_url: string
    domain: string
    matched_from: Array<'content' | 'extra_content' | 'article_url'>
    source_kinds: Array<'linked' | 'source'>
    article_ids: string[]
    db_ids: number[]
    platforms: string[]
    user_ids: string[]
    usernames: string[]
    first_seen_at: number | null
    last_seen_at: number | null
    occurrences: number
}

interface MergeGroup {
    key: string
    start_at: number | null
    end_at: number | null
    start_label: string | null
    end_label: string | null
    count: number
    platforms: string[]
    user_ids: string[]
    usernames: string[]
    article_ids: string[]
    db_ids: number[]
    webpage_candidates: UrlCandidate[]
    combined_text: string
    items: Array<{
        index: number
        timestamp: string | null
        created_at: number | null
        db_id: number | null
        article_id: string | null
        platform: string | null
        user_id: string | null
        username: string | null
        url: string | null
        content: string
        extra_content: string | null
        linked_urls: string[]
    }>
}

class MechanicalProcessor extends BaseProcessor {
    static _PROVIDER = ProcessorProvider.Mechanical
    protected BASE_URL = ''
    NAME = 'Mechanical'

    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super(api_key, log, config)
        this.NAME = config?.name || this.NAME
    }

    public async process(text: string): Promise<string> {
        const action = (this.config?.action || 'extract').toLowerCase() as MechanicalAction
        if (action === 'extract') {
            return JSON.stringify(this.runExtract(text), null, 2)
        }
        if (action === 'merge') {
            return JSON.stringify(this.runMerge(text), null, 2)
        }
        throw new Error(`Mechanical processor does not support action: ${this.config?.action || action}`)
    }

    private runExtract(text: string) {
        const articles = this.parseDigestArticles(text)
        const webpages = this.collectWebpageCandidates(articles)
        return {
            action: 'extract',
            generated_at: dayjs().toISOString(),
            total_articles: articles.length,
            webpages: webpages.map((candidate) => ({
                ...candidate,
            })),
        }
    }

    private runMerge(text: string) {
        const articles = this.parseDigestArticles(text)
        const options = this.getOptions()
        const mergeWindowSeconds = this.getMergeWindowSeconds(options)
        const minGroupSize = Math.max(1, options.min_group_size || 1)
        const groups: MergeGroup[] = []

        const sorted = articles.slice().sort((a, b) => {
            if (a.created_at !== null && b.created_at !== null) {
                return a.created_at - b.created_at
            }
            if (a.created_at !== null) {
                return -1
            }
            if (b.created_at !== null) {
                return 1
            }
            return a.index - b.index
        })

        let current: DigestArticle[] = []
        for (const article of sorted) {
            if (!current.length) {
                current = [article]
                continue
            }
            if (this.shouldMerge(current[current.length - 1], article, mergeWindowSeconds, options)) {
                current.push(article)
                continue
            }
            if (current.length >= minGroupSize) {
                groups.push(this.buildMergeGroup(current))
            }
            current = [article]
        }
        if (current.length >= minGroupSize) {
            groups.push(this.buildMergeGroup(current))
        }

        return {
            action: 'merge',
            generated_at: dayjs().toISOString(),
            merge_window_seconds: mergeWindowSeconds,
            total_articles: articles.length,
            groups,
        }
    }

    private getOptions(): MechanicalOptions {
        const payload = this.config?.extended_payload
        if (!payload || typeof payload !== 'object') {
            return {}
        }
        return payload as MechanicalOptions
    }

    private getMergeWindowSeconds(options: MechanicalOptions) {
        const seconds = Number(options.merge_window_seconds)
        if (Number.isFinite(seconds) && seconds > 0) {
            return seconds
        }
        const minutes = Number(options.merge_window_minutes)
        if (Number.isFinite(minutes) && minutes > 0) {
            return Math.round(minutes * 60)
        }
        return 15 * 60
    }

    private shouldMerge(
        previous: DigestArticle,
        next: DigestArticle,
        mergeWindowSeconds: number,
        options: MechanicalOptions,
    ) {
        if (options.group_by_user !== false && previous.user_id && next.user_id && previous.user_id !== next.user_id) {
            return false
        }
        if (options.group_by_platform !== false && previous.platform && next.platform && previous.platform !== next.platform) {
            return false
        }
        if (previous.created_at !== null && next.created_at !== null) {
            return next.created_at - previous.created_at <= mergeWindowSeconds
        }
        if (previous.created_at === null && next.created_at === null) {
            return true
        }
        return false
    }

    private buildMergeGroup(items: DigestArticle[]): MergeGroup {
        const sorted = items.slice().sort((a, b) => {
            if (a.created_at !== null && b.created_at !== null) {
                return a.created_at - b.created_at
            }
            return a.index - b.index
        })
        const start = sorted[0]
        const end = sorted[sorted.length - 1]
        const webpages = this.collectWebpageCandidates(sorted)
        const combinedText = sorted
            .map((item) => {
                const label = item.timestamp || `item-${item.index + 1}`
                const name = item.username || item.user_id || 'unknown'
                return [`[${label}] ${name}`, item.content || '(empty)'].join('\n')
            })
            .join('\n\n---\n\n')

        return {
            key: this.buildGroupKey(sorted),
            start_at: start.created_at,
            end_at: end.created_at,
            start_label: start.timestamp,
            end_label: end.timestamp,
            count: sorted.length,
            platforms: this.unique(sorted.map((item) => item.platform)),
            user_ids: this.unique(sorted.map((item) => item.user_id)),
            usernames: this.unique(sorted.map((item) => item.username)),
            article_ids: this.unique(sorted.map((item) => item.article_id)),
            db_ids: this.uniqueNumbers(sorted.map((item) => item.db_id)),
            webpage_candidates: webpages,
            combined_text: combinedText,
            items: sorted.map((item) => ({
                index: item.index,
                timestamp: item.timestamp,
                created_at: item.created_at,
                db_id: item.db_id,
                article_id: item.article_id,
                platform: item.platform,
                user_id: item.user_id,
                username: item.username,
                url: item.url,
                content: item.content,
                extra_content: item.extra_content,
                linked_urls: this.collectArticleUrls(item)
                    .filter((candidate) => !candidate.source_kinds.includes('source'))
                    .map((candidate) => candidate.normalized_url),
            })),
        }
    }

    private buildGroupKey(items: DigestArticle[]) {
        const first = items[0]
        const last = items[items.length - 1]
        return [
            first.platform || 'unknown',
            first.user_id || 'unknown',
            first.created_at || first.index,
            last.created_at || last.index,
            items.length,
        ].join(':')
    }

    private parseDigestArticles(text: string): DigestArticle[] {
        const normalized = text.replace(/\r\n/g, '\n').trim()
        if (!normalized) {
            return []
        }
        const blocks = normalized
            .split(/\n\s*---\s*\n/g)
            .map((block) => block.trim())
            .filter(Boolean)

        if (!blocks.length) {
            return [this.createFallbackArticle(normalized, 0)]
        }

        const parsed = blocks.map((block, index) => this.parseDigestBlock(block, index))
        if (parsed.every((item) => !item.timestamp && !item.article_id && !item.user_id && !item.url)) {
            return [this.createFallbackArticle(normalized, 0)]
        }
        return parsed
    }

    private parseDigestBlock(block: string, index: number): DigestArticle {
        const lines = block.split('\n')
        const article: DigestArticle = {
            index,
            raw: block,
            timestamp: null,
            created_at: null,
            db_id: null,
            article_id: null,
            platform: null,
            user_id: null,
            username: null,
            url: null,
            content: '',
            extra_content: null,
            media_urls: [],
        }

        let section: DigestSection = null
        const fallbackContent: string[] = []
        const contentLines: string[] = []
        const extraLines: string[] = []

        for (const [lineIndex, rawLine] of lines.entries()) {
            const line = rawLine.trimEnd()

            if (lineIndex === 0) {
                const timestampMatch = line.match(/^\[(.+)\]$/)
                if (timestampMatch) {
                    article.timestamp = timestampMatch[1]
                    const parsed = dayjs(timestampMatch[1])
                    article.created_at = parsed.isValid() ? parsed.unix() : null
                    continue
                }
            }

            if (line === 'Content:') {
                section = 'content'
                continue
            }
            if (line === 'Extra Content:') {
                section = 'extra_content'
                continue
            }
            if (line === 'Media URLs:') {
                section = 'media_urls'
                continue
            }

            const dbIdMatch = line.match(/^Article DB ID:\s*(\d+)$/)
            if (dbIdMatch) {
                article.db_id = Number(dbIdMatch[1])
                section = null
                continue
            }
            const articleIdMatch = line.match(/^Article ID:\s*(.+)$/)
            if (articleIdMatch) {
                article.article_id = articleIdMatch[1].trim()
                section = null
                continue
            }
            const platformMatch = line.match(/^Platform:\s*(.+)$/)
            if (platformMatch) {
                article.platform = platformMatch[1].trim()
                section = null
                continue
            }
            const userIdMatch = line.match(/^User ID:\s*(.+)$/)
            if (userIdMatch) {
                article.user_id = userIdMatch[1].trim()
                section = null
                continue
            }
            const usernameMatch = line.match(/^Username:\s*(.+)$/)
            if (usernameMatch) {
                article.username = usernameMatch[1].trim()
                section = null
                continue
            }
            const urlMatch = line.match(/^URL:\s*(.+)$/)
            if (urlMatch) {
                article.url = urlMatch[1].trim()
                section = null
                continue
            }

            if (!section && !article.username && !article.user_id) {
                const legacyAuthor = line.match(/^(.+?)\s+\(([^()]+)\)$/)
                if (legacyAuthor) {
                    article.username = legacyAuthor[1].trim()
                    article.user_id = legacyAuthor[2].trim()
                    continue
                }
            }

            if (section === 'content') {
                contentLines.push(rawLine)
                continue
            }
            if (section === 'extra_content') {
                extraLines.push(rawLine)
                continue
            }
            if (section === 'media_urls') {
                const mediaLine = line.replace(/^- /, '').trim()
                if (mediaLine) {
                    article.media_urls.push(mediaLine)
                }
                continue
            }

            if (line) {
                fallbackContent.push(rawLine)
            }
        }

        article.content = this.cleanMultilineText(contentLines.join('\n') || fallbackContent.join('\n'))
        article.extra_content = this.cleanMultilineText(extraLines.join('\n')) || null
        return article
    }

    private createFallbackArticle(text: string, index: number): DigestArticle {
        return {
            index,
            raw: text,
            timestamp: null,
            created_at: null,
            db_id: null,
            article_id: null,
            platform: null,
            user_id: null,
            username: null,
            url: null,
            content: this.cleanMultilineText(text),
            extra_content: null,
            media_urls: [],
        }
    }

    private cleanMultilineText(text: string) {
        return text.trim()
    }

    private collectWebpageCandidates(articles: DigestArticle[]) {
        const maxResults = Number(this.getOptions().max_results)
        const map = new Map<string, UrlCandidate>()

        for (const article of articles) {
            for (const candidate of this.collectArticleUrls(article)) {
                const current = map.get(candidate.normalized_url)
                if (!current) {
                    map.set(candidate.normalized_url, candidate)
                    continue
                }
                current.occurrences += candidate.occurrences
                current.matched_from = this.unique([...current.matched_from, ...candidate.matched_from]) as Array<'content' | 'extra_content' | 'article_url'>
                current.source_kinds = this.unique([...current.source_kinds, ...candidate.source_kinds]) as Array<'linked' | 'source'>
                current.article_ids = this.unique([...current.article_ids, ...candidate.article_ids])
                current.db_ids = this.uniqueNumbers([...current.db_ids, ...candidate.db_ids])
                current.platforms = this.unique([...current.platforms, ...candidate.platforms])
                current.user_ids = this.unique([...current.user_ids, ...candidate.user_ids])
                current.usernames = this.unique([...current.usernames, ...candidate.usernames])
                current.first_seen_at = this.pickMinTime(current.first_seen_at, candidate.first_seen_at)
                current.last_seen_at = this.pickMaxTime(current.last_seen_at, candidate.last_seen_at)
            }
        }

        const webpages = Array.from(map.values()).sort((a, b) => {
            const linkedDiff = Number(b.source_kinds.includes('linked')) - Number(a.source_kinds.includes('linked'))
            if (linkedDiff !== 0) {
                return linkedDiff
            }
            if (b.occurrences !== a.occurrences) {
                return b.occurrences - a.occurrences
            }
            return (b.last_seen_at || 0) - (a.last_seen_at || 0)
        })

        if (Number.isFinite(maxResults) && maxResults > 0) {
            return webpages.slice(0, maxResults)
        }
        return webpages
    }

    private collectArticleUrls(article: DigestArticle): UrlCandidate[] {
        const options = this.getOptions()
        const allowPatterns = this.compilePatterns(options.url_allow_patterns)
        const blockPatterns = this.compilePatterns(options.url_block_patterns)
        const map = new Map<string, UrlCandidate>()

        const register = (
            rawUrl: string | null | undefined,
            matchedFrom: 'content' | 'extra_content' | 'article_url',
            sourceKind: 'linked' | 'source',
        ) => {
            if (!rawUrl) {
                return
            }
            const normalized = this.normalizeUrl(rawUrl)
            if (!normalized || !this.isLikelyWebPage(normalized, allowPatterns, blockPatterns)) {
                return
            }
            const domain = this.getDomain(normalized)
            if (!domain) {
                return
            }
            const current = map.get(normalized)
            if (!current) {
                map.set(normalized, {
                    url: rawUrl,
                    normalized_url: normalized,
                    domain,
                    matched_from: [matchedFrom],
                    source_kinds: [sourceKind],
                    article_ids: article.article_id ? [article.article_id] : [],
                    db_ids: article.db_id !== null ? [article.db_id] : [],
                    platforms: article.platform ? [article.platform] : [],
                    user_ids: article.user_id ? [article.user_id] : [],
                    usernames: article.username ? [article.username] : [],
                    first_seen_at: article.created_at,
                    last_seen_at: article.created_at,
                    occurrences: 1,
                })
                return
            }
            current.occurrences += 1
            current.matched_from = this.unique([...current.matched_from, matchedFrom]) as Array<'content' | 'extra_content' | 'article_url'>
            current.source_kinds = this.unique([...current.source_kinds, sourceKind]) as Array<'linked' | 'source'>
            current.article_ids = this.unique(
                article.article_id ? [...current.article_ids, article.article_id] : current.article_ids,
            )
            current.db_ids = this.uniqueNumbers(
                article.db_id !== null ? [...current.db_ids, article.db_id] : current.db_ids,
            )
            current.platforms = this.unique(
                article.platform ? [...current.platforms, article.platform] : current.platforms,
            )
            current.user_ids = this.unique(
                article.user_id ? [...current.user_ids, article.user_id] : current.user_ids,
            )
            current.usernames = this.unique(
                article.username ? [...current.usernames, article.username] : current.usernames,
            )
            current.first_seen_at = this.pickMinTime(current.first_seen_at, article.created_at)
            current.last_seen_at = this.pickMaxTime(current.last_seen_at, article.created_at)
        }

        if (options.include_source_url !== false) {
            register(article.url, 'article_url', 'source')
        }
        for (const url of this.extractUrls(article.content)) {
            register(url, 'content', 'linked')
        }
        for (const url of this.extractUrls(article.extra_content || '')) {
            register(url, 'extra_content', 'linked')
        }

        return Array.from(map.values())
    }

    private extractUrls(text: string) {
        const matches = text.match(/https?:\/\/[^\s<>"']+/g) || []
        return matches
            .map((item) => item.replace(/[)\],.;!?]+$/g, ''))
            .filter(Boolean)
    }

    private normalizeUrl(rawUrl: string) {
        try {
            const url = new URL(rawUrl)
            if (!['http:', 'https:'].includes(url.protocol)) {
                return null
            }
            url.hash = ''
            url.hostname = url.hostname.toLowerCase()
            const search = new URLSearchParams()
            for (const [key, value] of url.searchParams.entries()) {
                if (/^utm_/i.test(key)) {
                    continue
                }
                if (['fbclid', 'gclid', 'igshid'].includes(key.toLowerCase())) {
                    continue
                }
                search.append(key, value)
            }
            url.search = search.toString() ? `?${search.toString()}` : ''
            return url.toString()
        } catch {
            return null
        }
    }

    private isLikelyWebPage(urlString: string, allowPatterns: RegExp[], blockPatterns: RegExp[]) {
        if (allowPatterns.some((pattern) => pattern.test(urlString))) {
            return true
        }
        if (blockPatterns.some((pattern) => pattern.test(urlString))) {
            return false
        }
        try {
            const url = new URL(urlString)
            if (!url.hostname) {
                return false
            }
            return !/\.(?:jpg|jpeg|png|webp|gif|svg|mp4|mov|m4v|webm|m3u8|ts|mp3|wav|ogg|flac|zip|rar|7z|tar|gz|css|js|map|woff2?|ttf|otf|ico)(?:$|[?#])/i.test(
                url.pathname,
            )
        } catch {
            return false
        }
    }

    private compilePatterns(value: MechanicalOptions['url_allow_patterns']) {
        const patterns = Array.isArray(value) ? value : typeof value === 'string' ? [value] : []
        return patterns
            .map((pattern) => {
                try {
                    return new RegExp(pattern)
                } catch (error) {
                    this.log?.warn(`Invalid mechanical processor regex: ${pattern} (${error})`)
                    return null
                }
            })
            .filter((pattern): pattern is RegExp => Boolean(pattern))
    }

    private getDomain(urlString: string) {
        try {
            return new URL(urlString).hostname
        } catch {
            return ''
        }
    }

    private unique(values: Array<string | null | undefined>) {
        return Array.from(new Set(values.filter((value): value is string => Boolean(value))))
    }

    private uniqueNumbers(values: Array<number | null | undefined>) {
        return Array.from(new Set(values.filter((value): value is number => typeof value === 'number')))
    }

    private pickMinTime(current: number | null, next: number | null) {
        if (current === null) {
            return next
        }
        if (next === null) {
            return current
        }
        return Math.min(current, next)
    }

    private pickMaxTime(current: number | null, next: number | null) {
        if (current === null) {
            return next
        }
        if (next === null) {
            return current
        }
        return Math.max(current, next)
    }
}

export { MechanicalProcessor }
