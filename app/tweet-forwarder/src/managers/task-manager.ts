import { Logger } from '@idol-bbq-utils/log'
import { BaseCompatibleModel } from '@/utils/base'
import DB from '@/db'
import { processorRegistry } from '@/middleware/processor'
import { ForwarderPools } from '@/managers/forwarder-manager'
import { Platform } from '@idol-bbq-utils/spider/types'
import { CronJob } from 'cron'
import dayjs from 'dayjs'
import type { ProcessorConfig } from '@/types/processor'
import type { Processor } from '@/types'
import { isPersistentMediaPath } from '@/services/media-cache-service'
import { normalizeCronSecond } from '@/utils/cron'
import { getForwarderProviderResult, PartialForwarderSendError } from '@/middleware/forwarder/base'
import {
    isOutboundFailedStatus,
    isOutboundInProgressStatus,
    isOutboundQueuedStatus,
    isOutboundSuppressedCompletionStatus,
    payloadHash,
    providerCode,
    routeKey,
    summarizeProviderResult,
    syntheticOutboundKey,
    targetRouteKey,
} from '@/services/outbound-message-service'

interface AggregatePayload {
    platform: Platform
    u_id: string
    start: number
    end: number
    target_ids?: string[]
    bot_id?: string // legacy
    processorConfig?: ProcessorConfig & { provider: string; api_key?: string }
    processorId?: string
    prompt?: string
}

type AggregateSendStatus =
    | 'sent'
    | 'queued'
    | 'blocked'
    | 'partial'
    | 'already_completed'
    | 'in_progress'
    | 'missing_target'
    | 'failed'

interface AggregateSendOutcome {
    targetId: string
    status: AggregateSendStatus
    retryable: boolean
    outboundStatus?: string
    error?: string
}

class TaskDeliveryError extends Error {
    readonly retryable: boolean
    readonly resultSummary?: string

    constructor(message: string, options: { retryable: boolean; resultSummary?: string }) {
        super(message)
        this.name = 'TaskDeliveryError'
        this.retryable = options.retryable
        this.resultSummary = options.resultSummary
    }
}

function toErrorMessage(error: unknown) {
    return error instanceof Error ? error.message : String(error)
}

export class TaskManager extends BaseCompatibleModel {
    NAME = 'TaskManager'
    log?: Logger
    private forwarderPools: ForwarderPools
    private pollingJob: CronJob
    private processors: Processor[]
    private readonly staleProcessingSeconds = 30 * 60
    private readonly taskRetryLimit = 5
    private readonly taskRetryBaseSeconds = 120
    private readonly taskRetryMaxSeconds = 3600

    constructor(forwarderPools: ForwarderPools, options: { processors?: Processor[] } = {}, log?: Logger) {
        super()
        this.forwarderPools = forwarderPools
        this.log = log?.child({ subservice: this.NAME })
        this.processors = options.processors || []
        // Poll every minute, desynchronized from top-of-minute crawler/forwarder work.
        this.pollingJob = new CronJob(normalizeCronSecond('*/1 * * * *'), this.poll.bind(this))
    }

    async init() {
        this.log?.info('TaskManager initialized')
        this.pollingJob.start()
    }

    async stop() {
        this.pollingJob.stop()
    }

    async drop() {
        await this.stop()
    }

    private async poll() {
        const now = Math.floor(Date.now() / 1000)
        try {
            const recovered = await DB.TaskQueue.recoverStaleProcessing(now, this.staleProcessingSeconds)
            if (recovered.count > 0) {
                this.log?.warn(`Recovered ${recovered.count} stale processing task(s)`)
            }
            const tasks = await DB.TaskQueue.getPending(now)
            if (tasks.length > 0) {
                this.log?.info(`Found ${tasks.length} pending tasks`)
            }
            for (const task of tasks) {
                const claimedTask = await DB.TaskQueue.claimPending(task.id)
                if (!claimedTask) {
                    this.log?.debug(`Task ${task.id} was already claimed by another worker`)
                    continue
                }
                try {
                    let resultSummary: string | undefined
                    if (claimedTask.type === 'aggregate_daily') {
                        // Cast payload safely
                        const payload = claimedTask.payload as unknown as AggregatePayload
                        resultSummary = await this.handleDailyAggregation(payload)
                    } else if (claimedTask.type === 'aggregate_hourly') {
                        const payload = claimedTask.payload as unknown as AggregatePayload
                        resultSummary = await this.handleHourlyAggregation(payload)
                    } else {
                        throw new Error(`Unsupported task type: ${claimedTask.type}`)
                    }
                    await DB.TaskQueue.updateStatus(
                        claimedTask.id,
                        'completed',
                        resultSummary ? { result_summary: resultSummary } : undefined,
                    )
                } catch (e) {
                    const errorMessage = toErrorMessage(e)
                    if (e instanceof TaskDeliveryError && e.retryable) {
                        const nextAttempt = this.getTaskRetryAttempts(claimedTask) + 1
                        if (nextAttempt <= this.taskRetryLimit) {
                            const retryAt = Math.floor(Date.now() / 1000) + this.taskRetryDelaySeconds(nextAttempt)
                            this.log?.warn(
                                `Task ${claimedTask.id} will retry after delivery failure (${nextAttempt}/${this.taskRetryLimit}): ${errorMessage}`,
                            )
                            await DB.TaskQueue.retryLater(claimedTask.id, retryAt, {
                                last_error: errorMessage,
                                result_summary: this.formatTaskRetrySummary(nextAttempt, e.resultSummary),
                            })
                            continue
                        }
                    }

                    this.log?.error(`Task ${claimedTask.id} failed: ${errorMessage}`)
                    await DB.TaskQueue.updateStatus(claimedTask.id, 'failed', {
                        last_error: errorMessage,
                        result_summary:
                            e instanceof TaskDeliveryError && e.retryable
                                ? this.formatTaskRetryExhaustedSummary(claimedTask, e.resultSummary)
                                : e instanceof TaskDeliveryError
                                  ? e.resultSummary
                                  : undefined,
                    })
                }
            }
        } catch (e) {
            this.log?.error(`Polling error: ${e}`)
        }
    }

    private async handleHourlyAggregation(payload: AggregatePayload): Promise<string | undefined> {
        const { platform, u_id, start, end, bot_id, target_ids } = payload
        this.log?.info(`Processing HOURLY batch for ${u_id} on ${platform}`)

        const articles = await DB.Article.getArticlesByTimeRange(u_id, platform, start, end)
        if (articles.length === 0) {
            this.log?.info(`No articles found for hourly batch.`)
            return 'aggregate_hourly no_articles'
        }

        // 1. Text Summary
        const contentLines = articles.map((a) => {
            const time = dayjs.unix(a.created_at).format('HH:mm')
            return `[${time}] ${a.content ?? '(No Text)'}`
        })
        const summaryText = contentLines.join('\n\n')

        // 2. Generate Summary Image
        let mediaFiles: { path: string; media_type: 'photo' | 'video' }[] = []
        const fs = await import('fs')

        try {
            const { ImgConverter } = await import('@idol-bbq-utils/render')
            const { writeImgToFile } = await import('@/middleware/media')

            const fakeArticle: any = {
                id: 0,
                platform: platform,
                a_id: `batch-${start}-${end}`,
                u_id: u_id,
                username: `Hourly Batch: ${u_id}`,
                created_at: end,
                content: summaryText,
                url: `https://${platform}.com`,
                type: 'post',
                ref: null,
                has_media: false,
                media: [],
                extra: null,
            }
            const converter = new ImgConverter()
            const imgBuffer = await converter.articleToImg(fakeArticle as any, 'default')
            const path = writeImgToFile(imgBuffer, `batch-${start}-${end}.png`)
            mediaFiles.push({ path, media_type: 'photo' })
        } catch (e) {
            this.log?.error(`Failed to generate batch summary image: ${e}`)
        }

        // 3. Collect Media from Articles
        const { RenderService } = await import('@/services/render-service')
        const renderService = new RenderService(this.log)

        for (const article of articles) {
            if (article.has_media || article.media) {
                try {
                    // We use a dummy media config to trigger download
                    const dummyMediaConfig = { type: 'no-storage', use: { tool: 'default' } } as any
                    const result = await renderService.process(article, {
                        taskId: `batch-sub-${article.a_id}`,
                        render_type: 'text', // We only want media files
                        mediaConfig: dummyMediaConfig,
                    })
                    if (result.mediaFiles.length > 0) {
                        mediaFiles = mediaFiles.concat(result.mediaFiles as any)
                    }
                } catch (e) {
                    this.log?.error(`Error fetching media for article ${article.a_id}: ${e}`)
                }
            }
        }

        // 4. Send
        try {
            return await this.sendAggregateToTargets(
                'aggregate_hourly',
                payload,
                target_ids && target_ids.length > 0 ? target_ids : bot_id ? [bot_id] : [],
                `Hourly Batch for ${u_id}`,
                mediaFiles,
            )
        } finally {
            // Cleanup
            if (mediaFiles.length > 0) {
                setTimeout(() => {
                    mediaFiles.forEach((f) => {
                        try {
                            if (!isPersistentMediaPath(f.path)) {
                                fs.unlinkSync(f.path)
                            }
                        } catch (e) {}
                    })
                }, 60000) // Delayed cleanup 1 minute
            }
        }
    }

    private async handleDailyAggregation(payload: AggregatePayload): Promise<string | undefined> {
        const { platform, u_id, start, end, bot_id, target_ids, processorConfig, processorId, prompt } = payload

        this.log?.info(
            `Processing aggregation for ${u_id} on ${platform} (${dayjs.unix(start).format()} - ${dayjs.unix(end).format()})`,
        )

        const articles = await DB.Article.getArticlesByTimeRange(u_id, platform, start, end)
        if (articles.length === 0) {
            this.log?.info(`No articles found for aggregation.`)
            return 'aggregate_daily no_articles'
        }

        const reversedArticles = articles.reverse() // created_at asc usually better for chronological summary

        const contentLines = reversedArticles.map((a) => {
            const time = dayjs.unix(a.created_at).format('YYYY-MM-DD HH:mm:ss')
            return `[${time}] ${a.content ?? '(No Text)'}\n`
        })
        const textToProcess = contentLines.join('\n')

        const configuredProcessor = processorId
            ? this.processors.find((processor) => processor.id === processorId || processor.name === processorId)
            : null
        const provider = configuredProcessor?.provider || processorConfig?.provider || 'Google'
        const apiKey =
            configuredProcessor?.api_key ||
            processorConfig?.api_key ||
            process.env.GEMINI_API_KEY ||
            process.env.GOOGLE_API_KEY ||
            ''

        const summaryPrompt =
            prompt ||
            `You are a summarizer. Please summarize the following social media posts from today for a daily report. Format it nicely.`

        let summary = ''
        try {
            const configWithPrompt = {
                ...(configuredProcessor?.cfg_processor || {}),
                ...(processorConfig || {}),
                prompt: summaryPrompt,
            }
            const processor = await processorRegistry.create(provider, apiKey, this.log, configWithPrompt)
            summary = await processor.process(textToProcess)
        } catch (e) {
            this.log?.error(`Summarization failed: ${e}`)
            summary = `Summarization failed. Raw content count: ${articles.length}`
        }

        let mediaFiles: { path: string; media_type: 'photo' }[] = []
        const fs = await import('fs')

        try {
            return await this.sendAggregateToTargets(
                'aggregate_daily',
                payload,
                target_ids && target_ids.length > 0 ? target_ids : bot_id ? [bot_id] : [],
                `Daily Report for ${u_id}:\n\n${summary}`,
                mediaFiles,
            )
        } finally {
            // Cleanup
            if (mediaFiles.length > 0) {
                mediaFiles.forEach((f) => {
                    try {
                        if (!isPersistentMediaPath(f.path)) {
                            fs.unlinkSync(f.path)
                        }
                    } catch (e) {}
                })
            }
        }
    }

    private async sendAggregateToTargets(
        taskKind: 'aggregate_hourly' | 'aggregate_daily',
        payload: AggregatePayload,
        targetIds: string[],
        text: string,
        mediaFiles: Array<{ path: string; media_type: 'photo' | 'video' }>,
    ) {
        if (targetIds.length === 0) {
            const message = `No target IDs provided for ${taskKind} of ${payload.u_id}`
            this.log?.warn(message)
            throw new TaskDeliveryError(message, {
                retryable: false,
                resultSummary: `${taskKind} targets=0`,
            })
        }

        const outcomes: AggregateSendOutcome[] = []
        for (const targetId of targetIds) {
            outcomes.push(await this.sendAggregateToTarget(taskKind, targetId, payload, text, mediaFiles))
        }

        return this.finalizeAggregateOutcomes(taskKind, payload, outcomes)
    }

    private finalizeAggregateOutcomes(
        taskKind: 'aggregate_hourly' | 'aggregate_daily',
        payload: AggregatePayload,
        outcomes: AggregateSendOutcome[],
    ) {
        const resultSummary = this.formatAggregateOutcomeSummary(taskKind, outcomes)
        const retryableFailures = outcomes.filter((outcome) => outcome.retryable)
        if (retryableFailures.length > 0) {
            const failureDetails = this.formatAggregateFailureDetails(retryableFailures)
            throw new TaskDeliveryError(
                `${taskKind} for ${payload.u_id} has ${retryableFailures.length} retryable target failure(s)${
                    failureDetails ? `: ${failureDetails}` : ''
                }`,
                {
                    retryable: true,
                    resultSummary,
                },
            )
        }

        const completedTarget = outcomes.some((outcome) => this.isCompletedAggregateOutcome(outcome))
        const failedTargets = outcomes.filter((outcome) => !this.isCompletedAggregateOutcome(outcome))
        if (!completedTarget && failedTargets.length > 0) {
            const failureDetails = this.formatAggregateFailureDetails(failedTargets)
            throw new TaskDeliveryError(
                `${taskKind} for ${payload.u_id} has no completed target delivery${
                    failureDetails ? `: ${failureDetails}` : ''
                }`,
                {
                    retryable: false,
                    resultSummary,
                },
            )
        }

        return resultSummary
    }

    private isCompletedAggregateOutcome(outcome: AggregateSendOutcome) {
        return ['sent', 'queued', 'blocked', 'partial', 'already_completed'].includes(outcome.status)
    }

    private formatAggregateOutcomeSummary(
        taskKind: 'aggregate_hourly' | 'aggregate_daily',
        outcomes: AggregateSendOutcome[],
    ) {
        const counts = outcomes.reduce<Record<string, number>>((acc, outcome) => {
            acc[outcome.status] = (acc[outcome.status] || 0) + 1
            return acc
        }, {})
        const countPart = Object.entries(counts)
            .sort(([left], [right]) => left.localeCompare(right))
            .map(([status, count]) => `${status}=${count}`)
            .join(' ')
        const failedTargets = outcomes
            .filter((outcome) => !this.isCompletedAggregateOutcome(outcome))
            .map((outcome) => `${outcome.targetId}:${outcome.status}`)
            .join(',')
        return `${taskKind} targets=${outcomes.length}${countPart ? ` ${countPart}` : ''}${
            failedTargets ? ` failed_targets=${failedTargets}` : ''
        }`
    }

    private formatAggregateFailureDetails(outcomes: AggregateSendOutcome[]) {
        return outcomes
            .map((outcome) => `${outcome.targetId}:${outcome.status}${outcome.error ? `:${outcome.error}` : ''}`)
            .join(';')
    }

    private getTaskRetryAttempts(task: { result_summary?: string | null }) {
        const match = /retry_attempts=(\d+)/.exec(task.result_summary || '')
        if (!match) {
            return 0
        }
        const parsed = Number.parseInt(match[1] || '0', 10)
        return Number.isFinite(parsed) && parsed > 0 ? parsed : 0
    }

    private taskRetryDelaySeconds(attempt: number) {
        const exponent = Math.max(0, Math.min(attempt - 1, 6))
        return Math.min(this.taskRetryMaxSeconds, this.taskRetryBaseSeconds * 2 ** exponent)
    }

    private formatTaskRetrySummary(attempt: number, resultSummary?: string) {
        return `retry_attempts=${attempt}/${this.taskRetryLimit}${resultSummary ? ` ${resultSummary}` : ''}`
    }

    private formatTaskRetryExhaustedSummary(task: { result_summary?: string | null }, resultSummary?: string) {
        const attempts = Math.max(this.getTaskRetryAttempts(task), this.taskRetryLimit)
        return `retry_exhausted attempts=${attempts}/${this.taskRetryLimit}${resultSummary ? ` ${resultSummary}` : ''}`
    }

    private classifySuppressedOutbound(targetId: string, status: string): AggregateSendOutcome {
        if (isOutboundSuppressedCompletionStatus(status)) {
            return { targetId, status: 'already_completed', retryable: false, outboundStatus: status }
        }
        if (isOutboundQueuedStatus(status)) {
            return { targetId, status: 'queued', retryable: false, outboundStatus: status }
        }
        if (isOutboundInProgressStatus(status)) {
            return { targetId, status: 'in_progress', retryable: true, outboundStatus: status }
        }
        if (isOutboundFailedStatus(status)) {
            return { targetId, status: 'failed', retryable: true, outboundStatus: status }
        }
        return {
            targetId,
            status: 'failed',
            retryable: true,
            outboundStatus: status,
            error: `Unexpected suppressed outbound status: ${status}`,
        }
    }

    private async sendAggregateToTarget(
        taskKind: 'aggregate_hourly' | 'aggregate_daily',
        targetId: string,
        payload: AggregatePayload,
        text: string,
        mediaFiles: Array<{ path: string; media_type: 'photo' | 'video' }>,
    ): Promise<AggregateSendOutcome> {
        const forwarder = this.forwarderPools.getTarget(targetId)
        if (!forwarder) {
            this.log?.warn(`Target ${targetId} not found for ${taskKind}`)
            return { targetId, status: 'missing_target', retryable: false, error: 'target not found' }
        }

        const routeKeyValue = targetRouteKey(
            routeKey({
                source: 'batch',
                crawlerId: payload.u_id,
                extra: taskKind,
            }),
            targetId,
        )
        const syntheticKey = `${payload.platform}:${payload.u_id}:${payload.start}:${payload.end}:${targetId}:${taskKind}`
        const outboundIdempotencyKey = syntheticOutboundKey(targetId, taskKind, syntheticKey)
        const outboundPayloadHash = payloadHash({
            routeKey: routeKeyValue,
            targetId,
            taskKind,
            text,
            media: mediaFiles,
            extra: {
                platform: payload.platform,
                u_id: payload.u_id,
                start: payload.start,
                end: payload.end,
            },
        })

        try {
            const outbound = await DB.OutboundMessage.claim({
                idempotency_key: outboundIdempotencyKey,
                route_key: routeKeyValue,
                target_id: targetId,
                target_platform: forwarder.NAME,
                task_kind: taskKind,
                synthetic_key: syntheticKey,
                payload_hash: outboundPayloadHash,
            })
            if (!outbound.claimed) {
                const status = String(outbound.record.status)
                this.log?.debug(
                    `${taskKind} outbound ${outboundIdempotencyKey} already ${status}; skipping ${targetId}`,
                )
                return this.classifySuppressedOutbound(targetId, status)
            }

            await DB.OutboundMessage.markSending(outboundIdempotencyKey)
            const sendResult = await forwarder.send(text, {
                timestamp: Math.floor(Date.now() / 1000),
                media: mediaFiles.length > 0 ? mediaFiles : undefined,
                forceSend: true,
            })
            if (sendResult.status === 'queued') {
                await DB.OutboundMessage.markQueued(outboundIdempotencyKey, sendResult)
                await DB.TargetHealth.mark({
                    target_id: targetId,
                    provider: forwarder.NAME,
                    status: 'ok',
                    last_send_status: 'queued',
                    details: sendResult,
                })
                return { targetId, status: 'queued', retryable: false }
            }
            if (sendResult.status === 'blocked') {
                await DB.OutboundMessage.markSkipped(outboundIdempotencyKey, sendResult.reason, sendResult)
                await DB.TargetHealth.mark({
                    target_id: targetId,
                    provider: forwarder.NAME,
                    status: 'ok',
                    last_send_status: 'blocked',
                    details: sendResult,
                })
                return { targetId, status: 'blocked', retryable: false }
            }

            const providerResult = getForwarderProviderResult(sendResult)
            await DB.OutboundMessage.markSent(outboundIdempotencyKey, summarizeProviderResult(providerResult))
            await DB.TargetHealth.mark({
                target_id: targetId,
                provider: forwarder.NAME,
                status: 'ok',
                last_send_status: 'sent',
                last_provider_code: providerCode(providerResult),
                details: summarizeProviderResult(providerResult),
            })
            return { targetId, status: 'sent', retryable: false }
        } catch (error) {
            this.log?.error(`Failed to send ${taskKind} for ${payload.u_id} to ${targetId}: ${error}`)
            if (error instanceof PartialForwarderSendError) {
                await DB.OutboundMessage.markPartial(
                    outboundIdempotencyKey,
                    summarizeProviderResult(error.partialResults),
                    error,
                ).catch(() => undefined)
                await DB.TargetHealth.mark({
                    target_id: targetId,
                    provider: forwarder.NAME,
                    status: 'degraded',
                    last_send_status: 'partial',
                    last_provider_code: providerCode(error.partialResults),
                    disabled_reason: error.message,
                    details: summarizeProviderResult(error.partialResults),
                }).catch(() => undefined)
                return { targetId, status: 'partial', retryable: false, error: error.message }
            }
            await DB.OutboundMessage.markFailed(outboundIdempotencyKey, error).catch(() => undefined)
            await DB.TargetHealth.mark({
                target_id: targetId,
                provider: forwarder.NAME,
                status: 'error',
                last_send_status: 'failed',
                disabled_reason: toErrorMessage(error),
                details: {
                    route_key: routeKeyValue,
                    task_kind: taskKind,
                },
            }).catch(() => undefined)
            return { targetId, status: 'failed', retryable: true, error: toErrorMessage(error) }
        }
    }
}
