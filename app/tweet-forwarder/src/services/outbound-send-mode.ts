type OutboundSendMode = 'live' | 'blocked'

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
}

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
    throw new Error(`Invalid IDOL_BBQ_OUTBOUND_SEND_MODE: ${value}`)
}

function resolveOutboundSendMode(env: Record<string, string | undefined> = process.env): OutboundSendMode {
    return normalizeOutboundSendModeValue(env.IDOL_BBQ_OUTBOUND_SEND_MODE || env.IDOL_BBQ_SEND_MODE)
}

class OutboundSendDryRunError extends Error {
    readonly details: OutboundSendDryRunDetails

    constructor(details: OutboundSendDryRunDetails) {
        super(`outbound send blocked by ${details.send_mode} mode for ${details.forwarder}:${details.target_id}`)
        this.name = 'OutboundSendDryRunError'
        this.details = details
    }
}

export {
    OutboundSendDryRunError,
    normalizeOutboundSendModeValue,
    resolveOutboundSendMode,
    type OutboundSendDryRunDetails,
    type OutboundSendMode,
}
