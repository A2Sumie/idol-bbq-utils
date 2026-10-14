/**
 * Pluggable inbound reaction framework for QQ/OneBot events.
 *
 * Chain: raw webhook body → `normalizeOneBotEvent` (ingest) → self-echo guard →
 * handler chain (config-driven, ordered) → outbound through the existing
 * OneBot call path (`callOneBotAction`, judged by `onebot-semantics`) or a
 * SnowLuma quick-operation response.
 *
 * Deliberate guard rails (docs/snowluma.md "入站事件与反应"):
 * - every handler runs single-shot inside try/catch: one failing handler never
 *   takes the chain (or the HTTP entry point) down, and there is no retry loop,
 *   so a dead bridge cannot turn into a retry storm;
 * - self messages / outbound echoes are dropped before matching (anti
 *   self-trigger loop);
 * - per-scope cooldown + hourly quota (`HandlerRateLimiter`) per handler;
 * - `group_action` (ban/kick/notice) needs THREE explicit opt-ins: master
 *   switch `allow_group_actions`, handler `enabled: true`, non-empty group
 *   allowlist, and only admins may trigger it;
 * - admin-gated commands (`#status` / `#recall` / `#ping`) require the sender
 *   to be in the configured admin whitelist (`#recall` is always admin-only).
 */
import {
    callOneBotAction,
    probeQqBridgeHealth,
    recallOneBotMessage,
    type OneBotEndpointConfig,
} from '@/services/onebot-receipt-service'
import {
    buildQQMediaFileValue,
    resolveQQMediaDeliveryConfig,
    type QQMediaDeliveryConfigLike,
} from '@/services/snowluma-media-source'
import { HandlerRateLimiter } from '@/services/onebot-event-dedupe'
import { isSelfEchoEvent, type NormalizedOneBotEvent } from '@/services/onebot-event-ingest'

export type QqInboundHandlerType = 'reply' | 'forward' | 'command' | 'group_action' | 'quick_operation'

type PostLike = (path: string, body: unknown, options: unknown) => Promise<any>

export interface QqInboundMatchConfig {
    /** Default ['message']. */
    post_types?: Array<string>
    /** Default any of group/private. */
    message_types?: Array<'group' | 'private'>
    group_ids?: Array<string>
    user_ids?: Array<string>
    /** JavaScript regex source tested against the extracted text. */
    text_regex?: string
    /** JavaScript regex source tested against raw_message. */
    raw_regex?: string
    /** Require the bot to be @-mentioned. */
    at_bot?: boolean
    /** Require the sender to be in the admin whitelist. */
    require_admin?: boolean
}

export interface QqInboundReplyConfig {
    template?: string
    /** Attach a `reply` segment with the triggering message_id (default true). */
    quote?: boolean
    at_sender?: boolean
    /** Extra raw OneBot segments appended after the text. */
    segments?: Array<Record<string, unknown>>
}

export interface QqInboundForwardConfig {
    template?: string
    /** Forward-target ids from `forward_targets` (resolved to group/user ids). */
    target_ids?: Array<string>
    group_ids?: Array<string>
    user_ids?: Array<string>
    segments?: Array<Record<string, unknown>>
    /** Local media files delivered through the target's media_delivery policy. */
    media?: Array<{ path: string; type?: 'image' | 'video' }>
}

export interface QqInboundCommandConfig {
    /** Subset of ['status', 'ping', 'recall']; default all three. */
    commands?: Array<string>
    /** Command prefix, default '#'. */
    prefix?: string
    /** Default true — commands are admin-only unless explicitly opened. */
    admin_only?: boolean
}

export interface QqInboundGroupActionConfig {
    action: 'ban' | 'kick' | 'notice'
    /** Ban duration in seconds (default 60, 0 = unmute). */
    duration_seconds?: number
    notice_text?: string
}

export interface QqInboundQuickOperationConfig {
    reply?: string
    at_sender?: boolean
    delete?: boolean
    /** Ban duration in seconds (SnowLuma quick operation `ban`). */
    ban?: number
}

export interface QqInboundHandlerConfig {
    id?: string
    type: QqInboundHandlerType
    enabled?: boolean
    match?: QqInboundMatchConfig
    /** Per-scope cooldown in seconds (default from qq_inbound.default_cooldown_seconds). */
    cooldown_seconds?: number
    /** Per-scope hourly quota (default from qq_inbound.default_max_per_hour). */
    max_per_hour?: number
    reply?: QqInboundReplyConfig
    forward?: QqInboundForwardConfig
    command?: QqInboundCommandConfig
    group_action?: QqInboundGroupActionConfig
    quick_operation?: QqInboundQuickOperationConfig
    /** Optional endpoint override (defaults to the first QQ target's endpoint). */
    url?: string
    token?: string
}

export interface QqInboundConfig {
    enabled?: boolean
    /** uins whose messages must never trigger reactions (self-echo guard). */
    bot_user_ids?: Array<string>
    /** Whitelist for admin-gated commands / group_action triggers. */
    admins?: Array<string>
    /** Master switch for group_action handlers (default false). */
    allow_group_actions?: boolean
    /** Dedupe window and cache size for webhook redeliveries. */
    dedupe_ttl_seconds?: number
    dedupe_max_entries?: number
    default_cooldown_seconds?: number
    default_max_per_hour?: number
    handlers?: Array<QqInboundHandlerConfig>
}

export interface ReactionContext {
    endpoint: OneBotEndpointConfig
    /** Resolve a forward_target id to its OneBot group/user destination. */
    resolveTarget?: (targetId: string) => { group_id?: string; user_id?: string } | null
    mediaConfig?: QQMediaDeliveryConfigLike
    admins?: Array<string>
    botUserIds?: Array<string>
    allowGroupActions?: boolean
    log?: { info?: (msg: string) => void; warn?: (msg: string) => void; debug?: (msg: string) => void }
    post?: PostLike
    limiter?: HandlerRateLimiter
    now?: () => number
}

export interface ReactionActionResult {
    handlerId: string
    type: QqInboundHandlerType
    status: 'ok' | 'skipped' | 'limited' | 'failed' | 'disabled'
    detail: string
    calls?: Array<{ action: string; ok: boolean; detail: string }>
    quickOperation?: Record<string, unknown>
}

export interface ReactionChainResult {
    handled: boolean
    actions: Array<ReactionActionResult>
    quickOperation: Record<string, unknown> | null
}

function templateReplace(template: string, event: NormalizedOneBotEvent, handlerId: string): string {
    return String(template).replace(/\{(\w+)\}/g, (_match, key: string) => {
        switch (key) {
            case 'text':
                return event.text
            case 'raw_text':
                return event.rawText
            case 'user_id':
                return event.userId || ''
            case 'group_id':
                return event.groupId || ''
            case 'sender_name':
                return event.senderName || event.userId || ''
            case 'message_id':
                return event.messageId || ''
            case 'handler_id':
                return handlerId
            default:
                return ''
        }
    })
}

function asIdList(value: unknown): Array<string> {
    return Array.isArray(value) ? value.map((item) => String(item || '').trim()).filter(Boolean) : []
}

export function isAdminSender(event: NormalizedOneBotEvent, admins: Array<string> = []): boolean {
    const whitelist = new Set(asIdList(admins))
    return Boolean(event.userId && whitelist.has(event.userId))
}

function matchHandler(event: NormalizedOneBotEvent, handler: QqInboundHandlerConfig, admins: Array<string>): string | null {
    const match = handler.match || {}
    const postTypes = asIdList(match.post_types).map((value) => value.toLowerCase())
    if (postTypes.length > 0 && !postTypes.includes(event.postType)) return `post_type ${event.postType} not in ${postTypes.join(',')}`
    if (postTypes.length === 0 && event.kind !== 'message') return `post_type ${event.postType} unmatched (default message-only)`
    if (match.message_types?.length) {
        const allowed = match.message_types.map((value) => String(value))
        if (!event.messageType || !allowed.includes(event.messageType)) {
            return `message_type ${event.messageType || 'unknown'} not in ${allowed.join(',')}`
        }
    }
    const groupIds = asIdList(match.group_ids)
    if (groupIds.length > 0 && (!event.groupId || !groupIds.includes(event.groupId))) return 'group_id not allowlisted'
    const userIds = asIdList(match.user_ids)
    if (userIds.length > 0 && (!event.userId || !userIds.includes(event.userId))) return 'user_id not allowlisted'
    if (match.text_regex) {
        let re: RegExp
        try {
            re = new RegExp(match.text_regex)
        } catch {
            return 'invalid text_regex'
        }
        if (!re.test(event.text)) return 'text_regex unmatched'
    }
    if (match.raw_regex) {
        let re: RegExp
        try {
            re = new RegExp(match.raw_regex)
        } catch {
            return 'invalid raw_regex'
        }
        if (!re.test(event.rawText)) return 'raw_regex unmatched'
    }
    if (match.at_bot && !event.atBot) return 'bot not mentioned'
    if (match.require_admin && !isAdminSender(event, admins)) return 'sender not admin'
    return null
}

function resolveHandlerEndpoint(handler: QqInboundHandlerConfig, ctx: ReactionContext): OneBotEndpointConfig {
    if (!handler.url && !handler.token) return ctx.endpoint
    return {
        endpoint: (handler.url || ctx.endpoint.endpoint).replace(/\/+$/, ''),
        token: (handler.token ?? ctx.endpoint.token).trim(),
    }
}

async function buildForwardSegments(
    config: QqInboundForwardConfig,
    event: NormalizedOneBotEvent,
    handlerId: string,
    ctx: ReactionContext,
): Promise<Array<Record<string, unknown>>> {
    const segments: Array<Record<string, unknown>> = []
    const template = config.template ?? '[入站事件] {handler_id} 群{group_id} 用户{user_id}({sender_name}): {text}'
    segments.push({ type: 'text', data: { text: templateReplace(template, event, handlerId) } })
    for (const media of config.media || []) {
        const mediaType = media.type === 'video' ? 'video' : 'image'
        if (!media.path) continue
        try {
            const built = await buildQQMediaFileValue(
                { path: media.path },
                {
                    config: resolveQQMediaDeliveryConfig(ctx.mediaConfig),
                    endpoint: ctx.endpoint.endpoint,
                    token: ctx.endpoint.token,
                    log: ctx.log as any,
                    post: ctx.post as any,
                },
            )
            segments.push({ type: mediaType, data: { file: built.value } })
        } catch (error) {
            ctx.log?.warn?.(
                `[qq-inbound] handler ${handlerId} media segment skipped (${media.path}): ${
                    error instanceof Error ? error.message : String(error)
                }`,
            )
        }
    }
    for (const segment of config.segments || []) {
        if (segment && typeof segment === 'object') segments.push(segment as Record<string, unknown>)
    }
    return segments
}

function buildReplySegments(
    config: QqInboundReplyConfig,
    event: NormalizedOneBotEvent,
    handlerId: string,
): Array<Record<string, unknown>> {
    const segments: Array<Record<string, unknown>> = []
    const quote = config.quote !== false
    if (quote && event.messageId) {
        segments.push({ type: 'reply', data: { message_id: event.messageId } })
    }
    if (config.at_sender && event.userId) {
        segments.push({ type: 'at', data: { qq: event.userId } })
    }
    const template = config.template ?? '收到'
    segments.push({ type: 'text', data: { text: templateReplace(template, event, handlerId) } })
    for (const segment of config.segments || []) {
        if (segment && typeof segment === 'object') segments.push(segment as Record<string, unknown>)
    }
    return segments
}

function resolveDestinations(
    config: { target_ids?: Array<string>; group_ids?: Array<string>; user_ids?: Array<string> },
    event: NormalizedOneBotEvent,
    ctx: ReactionContext,
): Array<{ kind: 'group' | 'private'; id: string }> {
    const destinations: Array<{ kind: 'group' | 'private'; id: string }> = []
    for (const groupId of asIdList(config.group_ids)) destinations.push({ kind: 'group', id: groupId })
    for (const userId of asIdList(config.user_ids)) destinations.push({ kind: 'private', id: userId })
    for (const targetId of asIdList(config.target_ids)) {
        const resolved = ctx.resolveTarget?.(targetId)
        if (!resolved) {
            ctx.log?.warn?.(`[qq-inbound] forward target ${targetId} not resolvable; skipped`)
            continue
        }
        if (resolved.group_id) destinations.push({ kind: 'group', id: String(resolved.group_id) })
        else if (resolved.user_id) destinations.push({ kind: 'private', id: String(resolved.user_id) })
    }
    if (destinations.length === 0 && !config.target_ids?.length && !config.group_ids?.length && !config.user_ids?.length) {
        // Default: answer in the same conversation the event came from.
        if (event.groupId) destinations.push({ kind: 'group', id: event.groupId })
        else if (event.userId) destinations.push({ kind: 'private', id: event.userId })
    }
    return destinations
}

async function runCommand(
    config: QqInboundCommandConfig,
    event: NormalizedOneBotEvent,
    handler: QqInboundHandlerConfig,
    ctx: ReactionContext,
    handlerId: string,
): Promise<ReactionActionResult> {
    const prefix = config.prefix || '#'
    const text = event.text.trim()
    if (!text.startsWith(prefix)) {
        return { handlerId, type: 'command', status: 'skipped', detail: 'no command prefix' }
    }
    const [rawName, ...args] = text.slice(prefix.length).trim().split(/\s+/)
    const name = String(rawName || '').toLowerCase()
    const allowed = asIdList(config.commands).map((value) => value.toLowerCase())
    const commands = allowed.length > 0 ? allowed : ['status', 'ping', 'recall']
    if (!commands.includes(name)) {
        return { handlerId, type: 'command', status: 'skipped', detail: `unknown command ${name || '(empty)'}` }
    }
    const adminOnly = config.admin_only !== false
    const admin = isAdminSender(event, asIdList(ctx.admins))
    // `recall` is destructive: ALWAYS admin-only, regardless of admin_only.
    if ((adminOnly || name === 'recall') && !admin) {
        return { handlerId, type: 'command', status: 'skipped', detail: 'command requires admin whitelist' }
    }

    const endpoint = resolveHandlerEndpoint(handler, ctx)

    // The command's reply goes back through the normal outbound path, so its
    // send result is reported honestly (a dead bridge must not read as 'ok').
    const sendText = async (text: string) => {
        const { verdict } = await callOneBotAction(
            endpoint,
            event.groupId ? 'send_group_msg' : 'send_private_msg',
            event.groupId
                ? { group_id: event.groupId, message: [{ type: 'text', data: { text } }] }
                : { user_id: event.userId, message: [{ type: 'text', data: { text } }] },
            ctx.post,
        )
        return { ok: verdict.ok, detail: verdict.ok ? 'ok' : verdict.detail }
    }
    const replyTo = (text: string, sent: { ok: boolean; detail: string }): ReactionActionResult => ({
        handlerId,
        type: 'command',
        status: sent.ok ? 'ok' : 'failed',
        detail: `command ${name}: ${text} (reply ${sent.detail})`,
        calls: [{ action: 'send_msg', ok: sent.ok, detail: sent.detail }],
    })

    if (name === 'ping') {
        return replyTo('pong', await sendText('pong'))
    }

    if (name === 'status') {
        const health = await probeQqBridgeHealth(endpoint, ctx.post)
        const line = `桥状态: online=${health.online} good=${health.good} flavor=${health.flavor} uptime=${health.uptimeSeconds ?? '?'}s`
        return replyTo(line, await sendText(line))
    }

    // recall <message_id>  (or the replied message id)
    const targetMessageId = (args[0] || '').trim() || (event.messageId ? String(event.messageId) : '')
    if (!targetMessageId) {
        return { handlerId, type: 'command', status: 'skipped', detail: 'recall requires a message id' }
    }
    const result = await recallOneBotMessage(endpoint, targetMessageId, ctx.post)
    const line = result.success ? `已撤回 ${targetMessageId}` : `撤回失败 ${targetMessageId}: ${result.detail}`
    const sent = await sendText(line)
    return {
        handlerId,
        type: 'command',
        status: result.success && sent.ok ? 'ok' : 'failed',
        detail: line,
        calls: [
            { action: 'delete_msg', ok: result.success, detail: result.detail },
            { action: 'send_msg', ok: sent.ok, detail: sent.detail },
        ],
    }
}

async function runGroupAction(
    config: QqInboundGroupActionConfig,
    event: NormalizedOneBotEvent,
    handler: QqInboundHandlerConfig,
    ctx: ReactionContext,
    handlerId: string,
): Promise<ReactionActionResult> {
    const allow = ctx.allowGroupActions === true
    const allowGroups = asIdList(handler.match?.group_ids)
    const admin = isAdminSender(event, asIdList(ctx.admins))
    if (!allow) {
        return { handlerId, type: 'group_action', status: 'disabled', detail: 'qq_inbound.allow_group_actions is off' }
    }
    if (allowGroups.length === 0) {
        return { handlerId, type: 'group_action', status: 'disabled', detail: 'group_action needs an explicit group allowlist' }
    }
    if (!admin) {
        return { handlerId, type: 'group_action', status: 'skipped', detail: 'group_action requires admin whitelist' }
    }
    if (!event.groupId || !event.userId) {
        return { handlerId, type: 'group_action', status: 'skipped', detail: 'group_action needs a group message with sender' }
    }

    const endpoint = resolveHandlerEndpoint(handler, ctx)

    let action = 'set_group_ban'
    let payload: Record<string, unknown> = {}
    switch (config.action) {
        case 'kick':
            action = 'set_group_kick'
            payload = { group_id: event.groupId, user_id: event.userId, reject_add_request: false }
            break
        case 'notice':
            action = '_send_group_notice'
            payload = {
                group_id: event.groupId,
                content: templateReplace(config.notice_text || '{text}', event, handlerId),
            }
            break
        case 'ban':
        default:
            action = 'set_group_ban'
            payload = {
                group_id: event.groupId,
                user_id: event.userId,
                duration: Math.max(0, Number(config.duration_seconds ?? 60)),
            }
            break
    }
    const { verdict } = await callOneBotAction(endpoint, action, payload, ctx.post)
    return {
        handlerId,
        type: 'group_action',
        status: verdict.ok ? 'ok' : 'failed',
        detail: verdict.ok ? `${action} ok` : verdict.detail,
        calls: [{ action, ok: verdict.ok, detail: verdict.ok ? 'ok' : verdict.detail }],
    }
}

function buildQuickOperation(
    config: QqInboundQuickOperationConfig,
    event: NormalizedOneBotEvent,
    handlerId: string,
): Record<string, unknown> {
    const quick: Record<string, unknown> = {}
    if (config.reply) quick.reply = templateReplace(config.reply, event, handlerId)
    if (config.at_sender) quick.at_sender = true
    if (config.delete) quick.delete = true
    if (typeof config.ban === 'number') quick.ban = Math.max(0, Math.floor(config.ban))
    return quick
}

/**
 * Run the configured handler chain against one normalized event. Never throws:
 * handler failures are reported in `actions` and isolated.
 */
export async function runReactionChain(
    event: NormalizedOneBotEvent,
    config: QqInboundConfig,
    ctx: ReactionContext,
): Promise<ReactionChainResult> {
    const actions: Array<ReactionActionResult> = []
    const botUserIds = [...asIdList(config.bot_user_ids), ...asIdList(ctx.botUserIds)]
    const admins = [...asIdList(config.admins), ...asIdList(ctx.admins)]
    const handlerContext: ReactionContext = {
        ...ctx,
        admins,
        botUserIds,
        allowGroupActions: ctx.allowGroupActions ?? config.allow_group_actions === true,
    }
    const limiter = ctx.limiter || new HandlerRateLimiter(ctx.now)

    if (isSelfEchoEvent(event, botUserIds)) {
        return {
            handled: false,
            actions: [
                {
                    handlerId: '(chain)',
                    type: 'reply',
                    status: 'skipped',
                    detail: 'self echo dropped (bot message / message_sent)',
                },
            ],
            quickOperation: null,
        }
    }

    let quickOperation: Record<string, unknown> | null = null
    for (const [index, handler] of (config.handlers || []).entries()) {
        const handlerId = String(handler.id || `${handler.type}-${index}`)
        if (handler.enabled === false) {
            actions.push({ handlerId, type: handler.type, status: 'disabled', detail: 'handler disabled' })
            continue
        }
        const mismatch = matchHandler(event, handler, admins)
        if (mismatch) {
            actions.push({ handlerId, type: handler.type, status: 'skipped', detail: mismatch })
            continue
        }
        const scope = event.groupId ? `g:${event.groupId}` : `u:${event.userId || 'unknown'}`
        const decision = limiter.consume(`${handlerId}:${scope}`, {
            cooldownMs: Math.max(0, Number(handler.cooldown_seconds ?? config.default_cooldown_seconds ?? 0)) * 1000,
            maxPerWindow: Number(handler.max_per_hour ?? config.default_max_per_hour ?? 0),
            windowMs: 60 * 60 * 1000,
        })
        if (!decision.allowed) {
            actions.push({
                handlerId,
                type: handler.type,
                status: 'limited',
                detail: `rate limited (${decision.reason}, retry in ${Math.ceil((decision.retryAfterMs || 0) / 1000)}s)`,
            })
            continue
        }

        try {
            switch (handler.type) {
                case 'reply': {
                    const replyConfig = handler.reply || {}
                    const segments = buildReplySegments(replyConfig, event, handlerId)
                    const endpoint = resolveHandlerEndpoint(handler, handlerContext)
                    const destinations = resolveDestinations(
                        { group_ids: event.groupId ? [event.groupId] : [], user_ids: event.groupId ? [] : [event.userId || ''] },
                        event,
                        handlerContext,
                    )
                    const calls: Array<{ action: string; ok: boolean; detail: string }> = []
                    for (const destination of destinations) {
                        if (!destination.id) continue
                        const action = destination.kind === 'group' ? 'send_group_msg' : 'send_private_msg'
                        const payload =
                            destination.kind === 'group'
                                ? { group_id: destination.id, message: segments }
                                : { user_id: destination.id, message: segments }
                        const { verdict } = await callOneBotAction(endpoint, action, payload, ctx.post)
                        calls.push({ action, ok: verdict.ok, detail: verdict.ok ? 'ok' : verdict.detail })
                    }
                    actions.push({
                        handlerId,
                        type: 'reply',
                        status: calls.some((call) => call.ok) ? 'ok' : 'failed',
                        detail: calls.map((call) => `${call.action}:${call.ok ? 'ok' : call.detail}`).join('; '),
                        calls,
                    })
                    break
                }
                case 'forward': {
                    const forwardConfig = handler.forward || {}
                    const segments = await buildForwardSegments(forwardConfig, event, handlerId, handlerContext)
                    const endpoint = resolveHandlerEndpoint(handler, handlerContext)
                    const destinations = resolveDestinations(forwardConfig, event, handlerContext)
                    const calls: Array<{ action: string; ok: boolean; detail: string }> = []
                    for (const destination of destinations) {
                        if (!destination.id) continue
                        const action = destination.kind === 'group' ? 'send_group_msg' : 'send_private_msg'
                        const payload =
                            destination.kind === 'group'
                                ? { group_id: destination.id, message: segments }
                                : { user_id: destination.id, message: segments }
                        const { verdict } = await callOneBotAction(endpoint, action, payload, ctx.post)
                        calls.push({ action, ok: verdict.ok, detail: verdict.ok ? 'ok' : verdict.detail })
                    }
                    actions.push({
                        handlerId,
                        type: 'forward',
                        status: calls.length > 0 && calls.some((call) => call.ok) ? 'ok' : 'failed',
                        detail: calls.map((call) => `${call.action}:${call.ok ? 'ok' : call.detail}`).join('; '),
                        calls,
                    })
                    break
                }
                case 'command': {
                    actions.push(await runCommand(handler.command || {}, event, handler, handlerContext, handlerId))
                    break
                }
                case 'group_action': {
                    actions.push(await runGroupAction(handler.group_action || { action: 'ban' }, event, handler, handlerContext, handlerId))
                    break
                }
                case 'quick_operation': {
                    const quick = buildQuickOperation(handler.quick_operation || {}, event, handlerId)
                    if (Object.keys(quick).length === 0) {
                        actions.push({ handlerId, type: 'quick_operation', status: 'skipped', detail: 'empty quick operation' })
                        break
                    }
                    quickOperation = quick
                    actions.push({ handlerId, type: 'quick_operation', status: 'ok', detail: 'quick operation returned to bridge', quickOperation: quick })
                    break
                }
                default: {
                    actions.push({
                        handlerId,
                        type: (handler.type as QqInboundHandlerType) || 'reply',
                        status: 'skipped',
                        detail: `unknown handler type ${String(handler.type)}`,
                    })
                }
            }
        } catch (error) {
            const detail = error instanceof Error ? error.message : String(error)
            ctx.log?.warn?.(`[qq-inbound] handler ${handlerId} failed: ${detail}`)
            actions.push({ handlerId, type: handler.type, status: 'failed', detail })
        }
    }

    return {
        handled: actions.some((action) => action.status === 'ok'),
        actions,
        quickOperation,
    }
}
