import type { ForwarderContext, ForwarderMiddleware } from './types'
import { formatTime, getSubtractTime } from '@/utils/time'
import { isStringArrayArray } from '@/utils/typeguards'
import { articleToText, extractArticleHeadline, extractTextHeadline } from '@idol-bbq-utils/render'
import { SimpleExpiringCache } from '@idol-bbq-utils/spider'
import type { Article } from '@/db'
import dayjs, { type ManipulateType } from 'dayjs'

const BLOCK_RULE_PENDING_COMMITS_KEY = 'block_rule_pending_commits'

type BlockRuleCommit = {
    cacheKey: string
    value: string
    ttlSeconds: number
}

function parseDurationSeconds(value: string, fallbackSeconds: number) {
    if (value === 'once') {
        return 365 * 24 * 3600
    }

    const match = value.match(/^(\d+)([a-zA-Z]+)$/)
    if (!match || !match[1] || !match[2]) {
        return fallbackSeconds
    }

    const start = dayjs.unix(0)
    const end = start.add(Number(match[1]), match[2] as ManipulateType)
    const seconds = end.diff(start, 'second')
    return Number.isFinite(seconds) && seconds > 0 ? seconds : fallbackSeconds
}

export class TimeFilterMiddleware implements ForwarderMiddleware {
    readonly name = 'TimeFilter'

    async process(context: ForwarderContext, next: () => Promise<void>): Promise<boolean> {
        const { timestamp, config } = context
        const { block_until } = config

        if (!timestamp) {
            await next()
            return true
        }

        const block_until_date = getSubtractTime(dayjs().unix(), block_until || '30m')

        if (timestamp < block_until_date) {
            context.abortReason = `blocked: can not send before ${formatTime(block_until_date)}`
            return false
        }

        await next()
        return true
    }
}

export class KeywordFilterMiddleware implements ForwarderMiddleware {
    readonly name = 'KeywordFilter'

    async process(context: ForwarderContext, next: () => Promise<void>): Promise<boolean> {
        const { text, article, config } = context
        const { accept_keywords, filter_keywords } = config

        const original_text = accept_keywords || filter_keywords ? articleToText(article) : undefined

        if (accept_keywords && accept_keywords.length > 0) {
            const regex = new RegExp(accept_keywords.join('|'), 'i')
            let blocked = !regex.test(text)
            blocked = original_text ? !regex.test(original_text) : blocked

            if (blocked) {
                context.abortReason = 'blocked: accept keywords not matched'
                return false
            }
        }

        if (filter_keywords && filter_keywords.length > 0) {
            const regex = new RegExp(filter_keywords.join('|'), 'i')
            let blocked = regex.test(text)
            blocked = original_text ? regex.test(original_text) : blocked

            if (blocked) {
                context.abortReason = 'blocked: filter keywords matched'
                return false
            }
        }

        await next()
        return true
    }
}

export class BlockRuleMiddleware implements ForwarderMiddleware {
    readonly name = 'BlockRule'
    private cache: SimpleExpiringCache = new SimpleExpiringCache()

    commitPending(context: ForwarderContext): number {
        const pendingCommits = context.metadata.get(BLOCK_RULE_PENDING_COMMITS_KEY) as
            | BlockRuleCommit[]
            | undefined

        if (!pendingCommits || pendingCommits.length === 0) {
            return 0
        }

        for (const commit of pendingCommits) {
            this.cache.set(commit.cacheKey, commit.value, commit.ttlSeconds)
        }
        context.metadata.delete(BLOCK_RULE_PENDING_COMMITS_KEY)
        return pendingCommits.length
    }

    async process(context: ForwarderContext, next: () => Promise<void>): Promise<boolean> {
        const { article, config } = context
        const { block_rules } = config

        if (!block_rules || block_rules.length === 0 || !article) {
            await next()
            return true
        }

        const pendingCommits: BlockRuleCommit[] = []
        const blocked = block_rules.some((rule) => this.shouldBlock(article, rule, pendingCommits))

        if (blocked) {
            context.abortReason = 'blocked: block rules matched'
            return false
        }

        if (pendingCommits.length > 0) {
            const existingCommits =
                (context.metadata.get(BLOCK_RULE_PENDING_COMMITS_KEY) as BlockRuleCommit[] | undefined) || []
            context.metadata.set(BLOCK_RULE_PENDING_COMMITS_KEY, [...existingCommits, ...pendingCommits])
        }

        await next()
        return true
    }

    private shouldBlock(
        article: Article,
        rule: NonNullable<ForwarderContext['config']['block_rules']>[number],
        pendingCommits: BlockRuleCommit[],
    ): boolean {
        const { platform, task_type = 'article', sub_type = [], block_type = 'none', block_until = '6h' } = rule

        if (platform !== article.platform) {
            return false
        }

        if (task_type !== 'article') {
            return false
        }

        if (block_type === 'none') {
            return false
        }

        if (sub_type.length > 0 && !sub_type.includes(article.type)) {
            return false
        }

        if (block_type === 'always') {
            return true
        }

        if (block_type === 'once.media') {
            let currentArticle: Article | null = article
            let has_media = false
            while (currentArticle) {
                if (currentArticle.has_media) {
                    has_media = true
                    break
                }
                if (currentArticle.ref && typeof currentArticle.ref === 'object') {
                    currentArticle = currentArticle.ref
                } else {
                    currentArticle = null
                }
            }
            if (!has_media) {
                return false
            }
        }

        const cache_key = [
            article.platform,
            task_type,
            sub_type.length > 0 ? sub_type.slice().sort().join(',') : '*',
            block_type,
        ].join('::')
        const cached = this.cache.get(cache_key)
        if (cached) {
            return true
        }

        if (block_type.startsWith('once')) {
            pendingCommits.push({
                cacheKey: cache_key,
                value: block_type,
                ttlSeconds: parseDurationSeconds(block_until, 6 * 3600),
            })
            return false
        }

        return false
    }
}

export class TextReplaceMiddleware implements ForwarderMiddleware {
    readonly name = 'TextReplace'

    async process(context: ForwarderContext, next: () => Promise<void>): Promise<boolean> {
        const { config } = context
        const { replace_regex } = config

        if (replace_regex) {
            context.text = this.applyReplacements(context.text, replace_regex)
        }

        await next()
        return true
    }

    private applyReplacements(text: string, regexps: string | [string, string] | Array<[string, string]>): string {
        if (typeof regexps === 'string') {
            return text.replace(new RegExp(regexps, 'g'), '')
        }

        if (isStringArrayArray(regexps)) {
            return regexps.reduce((acc, [reg, replace]) => acc.replace(new RegExp(reg, 'g'), replace || ''), text)
        }

        return text.replace(new RegExp(regexps[0], 'g'), regexps.length > 1 ? regexps[1] : '')
    }
}

export class TextChunkMiddleware implements ForwarderMiddleware {
    readonly name = 'TextChunk'

    constructor(private basicTextLimit: number = 1000) {}

    async process(context: ForwarderContext, next: () => Promise<void>): Promise<boolean> {
        const { text } = context

        if (text.length <= this.basicTextLimit) {
            context.metadata.set('chunks', [text])
            await next()
            return true
        }

        const fallbackText = context.article
            ? extractArticleHeadline(context.article, Math.min(120, this.basicTextLimit))
            : extractTextHeadline(text, Math.min(120, this.basicTextLimit))

        const singleChunk = fallbackText || text.slice(0, this.basicTextLimit).trimEnd()
        context.text = singleChunk
        context.metadata.set('chunks', [singleChunk])
        context.metadata.set('text_truncated', true)

        await next()
        return true
    }
}
