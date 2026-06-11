#!/usr/bin/env bun
import {
    CodexMcpDisabledError,
    createLocalCodexMcpClientServiceFromEnv,
    type CodexMcpReplyRequest,
    type CodexMcpRunRequest,
} from '../app/tweet-forwarder/src/services/codex-mcp-client-service'

const DEFAULT_HOST = '127.0.0.1'
const DEFAULT_PORT = 3099
const MAX_PROMPT_CHARS = 20_000
const MAX_TIMEOUT_MS = 240_000

const host = process.env.IDOL_BBQ_CODEX_BRIDGE_HOST || DEFAULT_HOST
const port = Number(process.env.IDOL_BBQ_CODEX_BRIDGE_PORT || DEFAULT_PORT)
const token = process.env.IDOL_BBQ_CODEX_BRIDGE_TOKEN || ''

if (!token && !['127.0.0.1', 'localhost', '::1'].includes(host)) {
    throw new Error('IDOL_BBQ_CODEX_BRIDGE_TOKEN is required when binding outside localhost')
}

function jsonResponse(payload: unknown, status = 200) {
    return new Response(JSON.stringify(payload), {
        status,
        headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-store',
        },
    })
}

function authorize(req: Request) {
    if (!token) {
        return true
    }
    return req.headers.get('Authorization') === `Bearer ${token}`
}

function clampNumber(value: unknown, min: number, max: number, fallback: number) {
    const numeric = typeof value === 'number' ? value : Number(value)
    if (!Number.isFinite(numeric)) {
        return fallback
    }
    return Math.max(min, Math.min(max, Math.floor(numeric)))
}

function normalizeRunBody(body: any): CodexMcpRunRequest {
    const prompt = String(body?.prompt || '')
        .slice(0, MAX_PROMPT_CHARS)
        .trim()
    if (!prompt) {
        throw new Error('prompt is required')
    }
    return {
        prompt,
        cwd: body?.cwd ? String(body.cwd) : undefined,
        model: body?.model ? String(body.model) : undefined,
        sandbox: body?.sandbox,
        approval_policy: body?.approval_policy || body?.approvalPolicy,
        developer_instructions: body?.developer_instructions || body?.developerInstructions,
        base_instructions: body?.base_instructions || body?.baseInstructions,
        config: typeof body?.config === 'object' && body.config ? body.config : undefined,
        timeout_ms: clampNumber(body?.timeout_ms || body?.timeoutMs, 1_000, MAX_TIMEOUT_MS, 120_000),
    }
}

function normalizeReplyBody(body: any): CodexMcpReplyRequest {
    const thread_id = String(body?.thread_id || body?.threadId || '').trim()
    const prompt = String(body?.prompt || '')
        .slice(0, MAX_PROMPT_CHARS)
        .trim()
    if (!thread_id) {
        throw new Error('thread_id is required')
    }
    if (!prompt) {
        throw new Error('prompt is required')
    }
    return {
        thread_id,
        prompt,
        timeout_ms: clampNumber(body?.timeout_ms || body?.timeoutMs, 1_000, MAX_TIMEOUT_MS, 120_000),
    }
}

async function parseJson(req: Request) {
    return req.json().catch(() => ({}))
}

function errorResponse(error: unknown, fallbackStatus = 500) {
    const status = error instanceof CodexMcpDisabledError ? 403 : fallbackStatus
    return jsonResponse(
        {
            success: false,
            error: error instanceof CodexMcpDisabledError ? 'codex_mcp_disabled' : 'codex_mcp_bridge_error',
            message: error instanceof Error ? error.message : String(error),
        },
        status,
    )
}

const server = Bun.serve({
    hostname: host,
    port,
    idleTimeout: 255,
    fetch: async (req, server) => {
        const url = new URL(req.url)
        if (!authorize(req)) {
            return new Response('Unauthorized', { status: 401 })
        }
        if (url.pathname === '/run' || url.pathname === '/reply') {
            server.timeout(req, 255)
        }

        const service = createLocalCodexMcpClientServiceFromEnv(process.env, process.cwd())
        try {
            if (req.method === 'GET' && url.pathname === '/status') {
                return jsonResponse(await service.status())
            }
            if (req.method === 'POST' && url.pathname === '/run') {
                return jsonResponse(await service.run(normalizeRunBody(await parseJson(req))))
            }
            if (req.method === 'POST' && url.pathname === '/reply') {
                return jsonResponse(await service.reply(normalizeReplyBody(await parseJson(req))))
            }
            return new Response(`Not Found: ${req.method} ${url.pathname}`, { status: 404 })
        } catch (error) {
            return errorResponse(error, url.pathname === '/run' || url.pathname === '/reply' ? 502 : 500)
        }
    },
})

console.error(`idol-bbq Codex MCP bridge listening on http://${server.hostname}:${server.port}`)
