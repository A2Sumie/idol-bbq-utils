import dayjs from 'dayjs'
import { Logger } from '@idol-bbq-utils/log'
import type { AppConfig } from '@/types'
import DB from '@/db'
import { Platform } from '@idol-bbq-utils/spider/types'

const SHOWROOM_URL_RE = /https?:\/\/(?:www\.)?showroom-live\.com\/r\/([a-z0-9_]+)/i
const SHOWROOM_KEYWORD_RE = /SHOWROOM|ショールーム/i
const POST_TIME_RE = /(?:🎥|🖤|⏰|この後|このあと)?\s*(\d{1,2}:\d{2})/
const DATE_REF_RE = /📅(明日|今日|本日)\s*(\d{1,2})\/(\d{1,2})/
const MEMBER_NAMES = [
    '折本美玲',
    '橘茉奈',
    '南伊織',
    '黒崎ありす',
    '三雲遥加',
    '吉沢珠璃',
    '麻丘真央',
    '望月りの',
    '西條和',
    '月城咲舞',
    '椎名桜月',
    '河瀬詩',
    '天城サリー',
    '西浦そら',
    '桧山依子',
    '相川奈央',
    '佐倉初',
    '北原実咲',
    '青木萌',
    '倉丸莉子',
]

type ShowroomEvent = {
    starts_at: number
    date_label: string
    time_label: string
    slug: string
    url: string
    members: string[]
    confidence: number
    uncertainties: string[]
}

type ShowroomScheduleOptions = {
    scanWindowStartHour?: number
    scanWindowEndHour?: number
    fastScanSeconds?: number
    slowScanSeconds?: number
    minConfidence?: number
    processorId?: string
}

const DEFAULT_SCAN_WINDOW_START_HOUR = 22
const DEFAULT_SCAN_WINDOW_END_HOUR = 24
const DEFAULT_FAST_SCAN_SECONDS = 120
const DEFAULT_SLOW_SCAN_SECONDS = 600
const DEFAULT_MIN_CONFIDENCE = 0.6
const DEFAULT_PROCESSOR_ID = '22_7-showroom-schedule'

function nowJstClock() {
    return Math.floor(Date.now() / 1000) + 9 * 3600
}

function jstClockHour() {
    return Math.floor(nowJstClock() / 3600) % 24
}

function jstClockDateString(unix: number) {
    const d = new Date(unix * 1000)
    return d.toISOString().slice(0, 10)
}

function jstClockIso(unix: number) {
    return dayjs.unix(unix).utcOffset(9 * 60).format('YYYY-MM-DDTHH:mm:ssZ')
}

export function isShowroomCandidatePost(content: string) {
    const text = String(content || '')
    return SHOWROOM_KEYWORD_RE.test(text) && SHOWROOM_URL_RE.test(text)
}

function extractMembers(text: string): string[] {
    return MEMBER_NAMES.filter((name) => text.includes(name))
}

function parseRelativeDate(dateRef: string, month: string, day: string, timeLabel: string) {
    const jstNow = nowJstClock()
    const [hour, minute] = timeLabel.split(':').map((value) => Number(value))
    const baseDay = dateRef === '明日' ? jstNow + 86400 : jstNow
    const baseDate = new Date(baseDay * 1000)
    let year = baseDate.getUTCFullYear()
    let start = Math.floor(Date.UTC(year, Number(month) - 1, Number(day), hour, minute, 0) / 1000)
    if (start < jstNow) {
        year += 1
        start = Math.floor(Date.UTC(year, Number(month) - 1, Number(day), hour, minute, 0) / 1000)
    }
    return start - 9 * 3600
}

export function extractShowroomEventsByRule(content: string): ShowroomEvent[] {
    const text = String(content || '')
    const events: ShowroomEvent[] = []
    const dateMatch = DATE_REF_RE.exec(text)
    if (!dateMatch) {
        return events
    }
    const lines = text.split(/\r?\n/)
    for (let index = 0; index < lines.length; index += 1) {
        const line = lines[index]
        const urlMatch = SHOWROOM_URL_RE.exec(line)
        if (!urlMatch) {
            continue
        }
        const slug = urlMatch[1]
        const windowStart = Math.max(0, index - 3)
        const context = lines.slice(windowStart, index + 1).join('\n')
        const timeMatch = /(\d{1,2}:\d{2})/.exec(context)
        if (!timeMatch) {
            continue
        }
        const timeLabel = timeMatch[1]
        events.push({
            starts_at: parseRelativeDate(dateMatch[1], dateMatch[2], dateMatch[3], timeLabel),
            date_label: `${dateMatch[2]}/${dateMatch[3]}`,
            time_label: timeLabel,
            slug,
            url: urlMatch[0],
            members: extractMembers(context),
            confidence: 0.85,
            uncertainties: [],
        })
    }
    return events
}

export function normalizeLlmEvents(raw: unknown, fallback: ShowroomEvent[]): ShowroomEvent[] {
    const events: ShowroomEvent[] = []
    const candidates = Array.isArray(raw) ? raw : (raw as any)?.showroom_events
    if (Array.isArray(candidates)) {
        for (const item of candidates) {
            const timeLabel = String(item?.time || item?.time_label || '').trim()
            const slug = String(item?.slug || item?.room || '').trim()
            const url = String(item?.url || '').trim()
            const dateLabel = String(item?.date || item?.date_label || '').trim()
            const members = Array.isArray(item?.members)
                ? item.members.map(String).filter(Boolean)
                : String(item?.members || '').split(/[・、,，\s]+/).filter(Boolean)
            if (!timeLabel || (!slug && !url)) {
                continue
            }
            const resolvedSlug = slug || (SHOWROOM_URL_RE.exec(url)?.[1] ?? '')
            if (!resolvedSlug) {
                continue
            }
            const startsAt = Number(item?.starts_at || 0)
            events.push({
                starts_at: Number.isFinite(startsAt) && startsAt > 0 ? startsAt : 0,
                date_label: dateLabel,
                time_label: timeLabel,
                slug: resolvedSlug,
                url: url || `https://www.showroom-live.com/r/${resolvedSlug}`,
                members,
                confidence: Math.min(1, Math.max(0, Number(item?.confidence ?? 0.9))),
                uncertainties: Array.isArray(item?.uncertainties) ? item.uncertainties.map(String) : [],
            })
        }
    }
    if (events.length === 0 && fallback.length > 0) {
        return fallback
    }
    return events
}

export class ShowroomScheduleService {
    private readonly config: AppConfig
    private readonly log?: Logger
    private readonly options: Required<
        Pick<
            ShowroomScheduleOptions,
            'scanWindowStartHour' | 'scanWindowEndHour' | 'fastScanSeconds' | 'slowScanSeconds' | 'minConfidence' | 'processorId'
        >
    >
    private timer: ReturnType<typeof setInterval> | null = null
    private running = false
    private seenIds = new Set<number>()
    private lastScanFrom = Math.floor(Date.now() / 1000) - 60 * 60

    constructor(config: AppConfig, log?: Logger, options: ShowroomScheduleOptions = {}) {
        this.config = config
        this.log = log?.child({ subservice: 'ShowroomSchedule' })
        this.options = {
            scanWindowStartHour: options.scanWindowStartHour ?? DEFAULT_SCAN_WINDOW_START_HOUR,
            scanWindowEndHour: options.scanWindowEndHour ?? DEFAULT_SCAN_WINDOW_END_HOUR,
            fastScanSeconds: options.fastScanSeconds ?? DEFAULT_FAST_SCAN_SECONDS,
            slowScanSeconds: options.slowScanSeconds ?? DEFAULT_SLOW_SCAN_SECONDS,
            minConfidence: options.minConfidence ?? DEFAULT_MIN_CONFIDENCE,
            processorId: options.processorId ?? DEFAULT_PROCESSOR_ID,
        }
    }

    async start() {
        if (this.timer) {
            return
        }
        const intervalMs = this.resolveScanIntervalSeconds() * 1000
        this.log?.info(`Showroom schedule service started (interval=${intervalMs}ms)`)
        this.timer = setInterval(() => {
            void this.runScan().catch((error) => {
                this.log?.warn(`Showroom schedule scan failed: ${error instanceof Error ? error.message : String(error)}`)
            })
        }, intervalMs)
        void this.runScan().catch((error) => {
            this.log?.warn(`Showroom schedule initial scan failed: ${error instanceof Error ? error.message : String(error)}`)
        })
    }

    async stop() {
        if (this.timer) {
            clearInterval(this.timer)
            this.timer = null
        }
    }

    private resolveScanIntervalSeconds() {
        const hour = jstClockHour()
        const inWindow =
            hour >= this.options.scanWindowStartHour && hour < this.options.scanWindowEndHour
        return inWindow ? this.options.fastScanSeconds : this.options.slowScanSeconds
    }

    async runScan() {
        if (this.running) {
            return
        }
        this.running = true
        try {
            const from = this.lastScanFrom
            const to = Math.floor(Date.now() / 1000)
            this.lastScanFrom = to
            const posts = await this.findNewStaffPosts(from, to)
            if (posts.length === 0) {
                return
            }
            for (const post of posts) {
                if (!post?.id || this.seenIds.has(post.id)) {
                    continue
                }
                this.seenIds.add(post.id)
                await this.processStaffPost(post)
            }
            if (this.seenIds.size > 500) {
                this.seenIds.clear()
            }
        } finally {
            this.running = false
        }
    }

    private async findNewStaffPosts(from: number, to: number) {
        const posts = await DB.Article.query({
            platform: Platform.X,
            u_id: '227_staff',
            from,
            to,
            limit: 50,
        })
        return posts
    }

    private async processStaffPost(post: any) {
        const content = String(post.content || '')
        if (!isShowroomCandidatePost(content)) {
            return
        }
        this.log?.info(`Showroom schedule: candidate post ${post.a_id} (${post.id})`)
        const ruleEvents = extractShowroomEventsByRule(content)
        let events = ruleEvents
        try {
            const llmEvents = await this.extractWithLlm(post)
            events = normalizeLlmEvents(llmEvents, ruleEvents)
        } catch (error) {
            this.log?.warn(
                `Showroom LLM extraction failed for ${post.a_id}; using rule extraction: ${
                    error instanceof Error ? error.message : String(error)
                }`,
            )
            events = ruleEvents
        }
        if (events.length === 0) {
            this.log?.info(`Showroom schedule: no events in post ${post.a_id}`)
            return
        }
        this.log?.info(
            `Showroom schedule: post ${post.a_id} -> ${events.length} event(s): ${events
                .map((event) => `${event.slug}@${event.time_label}`)
                .join(', ')}`,
        )
        for (const event of events) {
            if (event.confidence < this.options.minConfidence) {
                this.log?.warn(
                    `Showroom schedule: skip ${post.a_id} ${event.slug}@${event.time_label} confidence=${event.confidence}`,
                )
                continue
            }
            if (!event.starts_at) {
                this.log?.warn(`Showroom schedule: skip ${post.a_id} ${event.slug}: no resolved starts_at`)
                continue
            }
            await this.createPlan(event, post)
        }
    }

    private async extractWithLlm(post: any): Promise<unknown> {
        const cfg = this.resolveLlmConfig()
        const response = await fetch(`${cfg.base_url}/chat/completions`, {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${cfg.api_key}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                model: cfg.model_id,
                temperature: cfg.temperature,
                max_tokens: cfg.max_tokens,
                response_format: { type: 'json_object' },
                ...cfg.extended_payload,
                messages: [
                    {
                        role: 'system',
                        content: cfg.prompt,
                    },
                    {
                        role: 'user',
                        content: `Post metadata:\nuser_id: ${post.u_id}\npost_id: ${post.a_id}\nposted_at: ${jstClockIso(post.created_at)}\n\nPost text:\n${post.content}`,
                    },
                ],
            }),
            signal: AbortSignal.timeout(60000),
        })
        if (!response.ok) {
            throw new Error(`LLM request failed http=${response.status}: ${(await response.text()).slice(0, 200)}`)
        }
        const body = await response.json()
        const content = body?.choices?.[0]?.message?.content
        if (typeof content !== 'string' || !content.trim()) {
            throw new Error('LLM returned empty content')
        }
        return JSON.parse(content.trim())
    }

    private resolveLlmConfig() {
        const processor = (this.config.processors || []).find(
            (item: any) => String(item?.id || '') === this.options.processorId,
        )
        if (!processor) {
            throw new Error(`showroom schedule processor ${this.options.processorId} is not configured`)
        }
        const cfg = processor.cfg_processor || {}
        const rawKey = String(cfg.api_key || processor.api_key || '')
        const apiKey = rawKey.startsWith('env:') ? String(process.env[rawKey.slice(4)] || '') : rawKey
        let prompt = String(cfg.prompt || '')
        if (!prompt && Array.isArray(cfg.prompt_assets)) {
            for (const asset of cfg.prompt_assets) {
                const assetPath = String(asset?.path || '').trim()
                if (!assetPath) {
                    continue
                }
                try {
                    prompt += require('fs').readFileSync(assetPath, 'utf8')
                    break
                } catch {
                    // try next asset
                }
            }
        }
        return {
            base_url: String(cfg.base_url || 'https://opencode.ai/zen/go/v1').replace(/\/+$/, ''),
            api_key: apiKey,
            model_id: String(cfg.model_id || 'deepseek-v4-pro'),
            temperature: Number(cfg.temperature ?? 0.2),
            max_tokens: Number(cfg.max_tokens ?? 2048),
            prompt,
            extended_payload: cfg.extended_payload || {},
        }
    }

    private async createPlan(event: ShowroomEvent, post: any) {
        const secret = String(this.config.api?.secret || process.env.API_SECRET || '').trim()
        if (!secret) {
            this.log?.warn('Showroom schedule: API secret unavailable; skipping plan creation')
            return
        }
        const port = Number(this.config.api?.port || 3000)
        const startsAtIso = jstClockIso(event.starts_at)
        const payload = {
            schema_version: 1,
            target: {
                platform: 'showroom',
                handle: event.slug,
                url: event.url,
            },
            event: {
                starts_at: startsAtIso,
                timezone: 'Asia/Tokyo',
                title: 'SHOWROOM配信',
                performer: event.members.join('・'),
            },
            window: {
                before_minutes: 10,
                after_minutes: 240,
            },
            capture: {
                poll_seconds: 15,
                first_byte_timeout_seconds: 30,
                quality_order: ['origin_rtmp', 'hd_flv', 'hd_hls'],
                upload: false,
            },
            source: {
                kind: 'social_post',
                ref: String(post.a_id || ''),
                url: String(post.url || ''),
                observed_at: jstClockIso(post.created_at),
            },
            extraction: {
                confidence: event.confidence,
                model: 'deepseek-v4-pro',
                uncertainties: event.uncertainties,
            },
            tags: ['showroom', '22/7', event.slug],
            notes: `出演情報 ${event.date_label} ${event.time_label} ${event.members.join('・')} SHOWROOM配信`,
        }
        const response = await fetch(`http://127.0.0.1:${port}/api/live-capture-plans`, {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${secret}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload),
            signal: AbortSignal.timeout(30000),
        })
        if (!response.ok) {
            throw new Error(`plan create failed http=${response.status}: ${(await response.text()).slice(0, 200)}`)
        }
        const result = await response.json()
        this.log?.info(
            `Showroom plan ${result.created ? 'created' : 'exists'} for ${event.slug}@${event.time_label} (post ${post.a_id})`,
        )
    }
}
