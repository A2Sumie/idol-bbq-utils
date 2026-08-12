import DB from '@/db'
import type { Logger } from '@idol-bbq-utils/log'
import { providerCode, summarizeProviderResult } from '@/services/outbound-message-service'
import type { BaseForwarder, NonRetryableForwarderSendError, PartialForwarderSendError } from '@/middleware/forwarder/base'

/**
 * Health bookkeeping for a send attempt against one forward target. Every send
 * path used to inline this mapping (37 call sites) with inconsistent error
 * handling; this helper centralizes the outcome -> TargetHealth row mapping and
 * never throws (health records must not break a send path).
 */
export type SendHealthOutcome =
    | { kind: 'sent'; providerResult?: unknown }
    | { kind: 'queued'; result?: unknown }
    | { kind: 'blocked'; result?: unknown }
    | { kind: 'dry_run'; result?: unknown }
    | { kind: 'partial'; partialResults: unknown[]; message: string }
    | { kind: 'failed'; message: string; details?: Record<string, unknown> }

export async function markTargetHealthForSendOutcome(
    target: Pick<BaseForwarder, 'id' | 'NAME'>,
    outcome: SendHealthOutcome,
    log?: Logger,
): Promise<void> {
    try {
        switch (outcome.kind) {
            case 'sent':
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'ok',
                    last_send_status: 'sent',
                    last_provider_code: providerCode(outcome.providerResult),
                    details: summarizeProviderResult(outcome.providerResult),
                })
                return
            case 'queued':
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'ok',
                    last_send_status: 'queued',
                    details: outcome.result,
                })
                return
            case 'blocked':
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'ok',
                    last_send_status: 'blocked',
                    details: outcome.result,
                })
                return
            case 'dry_run':
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'ok',
                    last_send_status: 'dry_run',
                    details: outcome.result,
                })
                return
            case 'partial':
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'degraded',
                    last_send_status: 'partial',
                    last_provider_code: providerCode(outcome.partialResults),
                    disabled_reason: outcome.message,
                    details: summarizeProviderResult(outcome.partialResults),
                })
                return
            case 'failed':
                await DB.TargetHealth.mark({
                    target_id: target.id,
                    provider: target.NAME,
                    status: 'error',
                    last_send_status: 'failed',
                    disabled_reason: outcome.message,
                    details: outcome.details,
                })
                return
        }
    } catch (error) {
        log?.warn(
            `Failed to record target health for ${target.id}: ${error instanceof Error ? error.message : String(error)}`,
        )
    }
}

/**
 * Shared failure bookkeeping for send paths. Every path used to inline these
 * three blocks (with subtle divergence in mark order and error handling);
 * these helpers fix the order (mark outbound first, health second, never throw).
 */
export async function applyPartialSendFailure(
    target: Pick<BaseForwarder, 'id' | 'NAME'>,
    outboundIdempotencyKey: string,
    error: PartialForwarderSendError,
    log?: Logger,
): Promise<void> {
    await DB.OutboundMessage.markPartial(
        outboundIdempotencyKey,
        summarizeProviderResult(error.partialResults),
        error,
    ).catch(() => undefined)
    await markTargetHealthForSendOutcome(
        target,
        { kind: 'partial', partialResults: error.partialResults, message: error.message },
        log,
    )
}

export async function applyFailedSendFailure(
    target: Pick<BaseForwarder, 'id' | 'NAME'>,
    outboundIdempotencyKey: string,
    error: unknown,
    details: Record<string, unknown> | undefined,
    log?: Logger,
): Promise<void> {
    await DB.OutboundMessage.markFailed(outboundIdempotencyKey, error).catch(() => undefined)
    await markTargetHealthForSendOutcome(
        target,
        { kind: 'failed', message: error instanceof Error ? error.message : String(error), details },
        log,
    )
}

export async function applyNonRetryableSendFailure(
    target: Pick<BaseForwarder, 'id' | 'NAME'>,
    outboundIdempotencyKey: string,
    error: NonRetryableForwarderSendError,
    details: Record<string, unknown> | undefined,
    log?: Logger,
): Promise<void> {
    await DB.OutboundMessage.markFailed(outboundIdempotencyKey, error).catch(() => undefined)
    await markTargetHealthForSendOutcome(target, { kind: 'failed', message: error.message, details }, log)
}
