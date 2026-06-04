import { expect, test } from 'bun:test'
import {
    isOutboundFailedStatus,
    isOutboundInProgressStatus,
    isOutboundQueuedStatus,
    isOutboundStaleRetryableStatus,
    isOutboundSuppressedCompletionStatus,
    isOutboundVisibleCompletionStatus,
    OUTBOUND_STATUS,
} from './outbound-message-service'

test('outbound status helpers keep visible completion distinct from suppressed completion', () => {
    expect(isOutboundVisibleCompletionStatus(OUTBOUND_STATUS.Sent)).toBeTrue()
    expect(isOutboundVisibleCompletionStatus(OUTBOUND_STATUS.Partial)).toBeTrue()
    expect(isOutboundVisibleCompletionStatus(OUTBOUND_STATUS.FailedAfterPartial)).toBeTrue()
    expect(isOutboundVisibleCompletionStatus(OUTBOUND_STATUS.Skipped)).toBeFalse()

    expect(isOutboundSuppressedCompletionStatus(OUTBOUND_STATUS.Sent)).toBeTrue()
    expect(isOutboundSuppressedCompletionStatus(OUTBOUND_STATUS.Partial)).toBeTrue()
    expect(isOutboundSuppressedCompletionStatus(OUTBOUND_STATUS.FailedAfterPartial)).toBeTrue()
    expect(isOutboundSuppressedCompletionStatus(OUTBOUND_STATUS.Skipped)).toBeTrue()
    expect(isOutboundSuppressedCompletionStatus(OUTBOUND_STATUS.Failed)).toBeFalse()
})

test('outbound status helpers classify retryable in-flight and failed states', () => {
    expect(isOutboundStaleRetryableStatus(OUTBOUND_STATUS.Planned)).toBeTrue()
    expect(isOutboundStaleRetryableStatus(OUTBOUND_STATUS.Sending)).toBeTrue()
    expect(isOutboundStaleRetryableStatus(OUTBOUND_STATUS.Queued)).toBeTrue()
    expect(isOutboundStaleRetryableStatus(OUTBOUND_STATUS.Sent)).toBeFalse()

    expect(isOutboundInProgressStatus(OUTBOUND_STATUS.Planned)).toBeTrue()
    expect(isOutboundInProgressStatus(OUTBOUND_STATUS.Sending)).toBeTrue()
    expect(isOutboundInProgressStatus(OUTBOUND_STATUS.Queued)).toBeFalse()

    expect(isOutboundQueuedStatus(OUTBOUND_STATUS.Queued)).toBeTrue()
    expect(isOutboundQueuedStatus(OUTBOUND_STATUS.Sending)).toBeFalse()
    expect(isOutboundFailedStatus(OUTBOUND_STATUS.Failed)).toBeTrue()
    expect(isOutboundFailedStatus(OUTBOUND_STATUS.Partial)).toBeFalse()
})
