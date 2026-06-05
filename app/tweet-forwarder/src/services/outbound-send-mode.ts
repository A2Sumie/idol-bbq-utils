import { appendFile, mkdir } from 'node:fs/promises'
import nodePath from 'node:path'

type OutboundSendMode = 'live' | 'blocked' | 'capture'

interface OutboundCaptureResult {
    kind: 'http' | 'file'
    destination: string
    ok: boolean
    status?: number
    error?: string
}

interface OutboundCapturePayload {
    schema_version: 1
    send_mode: 'capture'
    captured_at: string
    target_id: string
    forwarder: string
    text_count: number
    text_length: number
    texts: string[]
    media: Array<Record<string, unknown>>
    card_media: Array<Record<string, unknown>>
    content_media: Array<Record<string, unknown>>
    article?: Record<string, unknown>
    article_key?: string
    outbound_key?: string
}

interface OutboundSendDryRunDetails {
    send_mode: OutboundSendMode
    target_id: string
    forwarder: string
    text_count: number
    text_length: number
    media_count: number
    card_media_count: number
    content_media_count: number
    article_key?: string
    outbound_key?: string
    capture_result?: OutboundCaptureResult
}

const DEFAULT_CAPTURE_FILE = '/tmp/tweet-forwarder/outbound-capture.jsonl'

function normalizeOutboundSendModeValue(value: string | undefined): OutboundSendMode {
    const normalized = String(value || 'live')
        .trim()
        .toLowerCase()
        .replace(/_/g, '-')
    if (!normalized || normalized === 'live' || normalized === 'online') {
        return 'live'
    }
    if (['blocked', 'block', 'dry-run', 'dryrun', 'disabled', 'off', 'no-send', 'nosend'].includes(normalized)) {
        return 'blocked'
    }
    if (
        [
            'capture',
            'captured',
            'test-receiver',
            'testreceiver',
            'receiver',
            'fake-receiver',
            'fakereceiver',
            'fake',
            'sink',
        ].includes(normalized)
    ) {
        return 'capture'
    }
    throw new Error(`Invalid IDOL_BBQ_OUTBOUND_SEND_MODE: ${value}`)
}

function resolveOutboundSendMode(env: Record<string, string | undefined> = process.env): OutboundSendMode {
    return normalizeOutboundSendModeValue(env.IDOL_BBQ_OUTBOUND_SEND_MODE || env.IDOL_BBQ_SEND_MODE)
}

function isNonLiveOutboundSendMode(mode: OutboundSendMode = resolveOutboundSendMode()) {
    return mode !== 'live'
}

function safeCaptureHttpDestination(rawUrl: string) {
    try {
        const parsed = new URL(rawUrl)
        parsed.username = ''
        parsed.password = ''
        parsed.search = ''
        parsed.hash = ''
        return parsed.toString()
    } catch {
        return 'invalid-url'
    }
}

function resolveOutboundCaptureUrl(env: Record<string, string | undefined> = process.env) {
    const value = env.IDOL_BBQ_OUTBOUND_CAPTURE_URL || env.IDOL_BBQ_TEST_RECEIVER_URL
    const normalized = value?.trim()
    return normalized || undefined
}

function resolveOutboundCaptureFile(env: Record<string, string | undefined> = process.env) {
    const value = env.IDOL_BBQ_OUTBOUND_CAPTURE_FILE?.trim()
    return value || DEFAULT_CAPTURE_FILE
}

async function captureOutboundSend(
    payload: OutboundCapturePayload,
    env: Record<string, string | undefined> = process.env,
): Promise<OutboundCaptureResult> {
    const captureUrl = resolveOutboundCaptureUrl(env)
    if (captureUrl) {
        try {
            const response = await fetch(captureUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
            })
            return {
                kind: 'http',
                destination: safeCaptureHttpDestination(captureUrl),
                ok: response.ok,
                status: response.status,
            }
        } catch (error) {
            return {
                kind: 'http',
                destination: safeCaptureHttpDestination(captureUrl),
                ok: false,
                error: error instanceof Error ? error.message : String(error),
            }
        }
    }

    const captureFile = resolveOutboundCaptureFile(env)
    try {
        await mkdir(nodePath.dirname(captureFile), { recursive: true })
        await appendFile(captureFile, `${JSON.stringify(payload)}\n`, 'utf8')
        return {
            kind: 'file',
            destination: captureFile,
            ok: true,
        }
    } catch (error) {
        return {
            kind: 'file',
            destination: captureFile,
            ok: false,
            error: error instanceof Error ? error.message : String(error),
        }
    }
}

class OutboundSendDryRunError extends Error {
    readonly details: OutboundSendDryRunDetails

    constructor(details: OutboundSendDryRunDetails) {
        const verb = details.send_mode === 'capture' ? 'captured' : 'blocked'
        super(`outbound send ${verb} by ${details.send_mode} mode for ${details.forwarder}:${details.target_id}`)
        this.name = 'OutboundSendDryRunError'
        this.details = details
    }
}

export {
    OutboundSendDryRunError,
    captureOutboundSend,
    isNonLiveOutboundSendMode,
    normalizeOutboundSendModeValue,
    resolveOutboundCaptureFile,
    resolveOutboundCaptureUrl,
    resolveOutboundSendMode,
    type OutboundCapturePayload,
    type OutboundCaptureResult,
    type OutboundSendDryRunDetails,
    type OutboundSendMode,
}
