#!/usr/bin/env bun
import fs from 'fs'
import path from 'path'
import YAML from 'yaml'

const SERVER_NAME = 'idol-bbq-forwarder'
const SERVER_VERSION = '0.1.0'
const DEFAULT_API_PORT = 3000

type JsonRpcId = string | number | null

interface JsonRpcMessage {
    jsonrpc?: '2.0'
    id?: JsonRpcId
    method?: string
    params?: any
}

interface RuntimeApiConfig {
    baseUrl: string
    token: string
}

const TOOLS = [
    {
        name: 'idol_bbq_agent_status',
        description: 'Return compact idol-bbq runtime, queue, endpoint, and configured-model status.',
        inputSchema: {
            type: 'object',
            properties: {},
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_model_capabilities',
        description: 'Return redacted configured processor models and known capability metadata.',
        inputSchema: {
            type: 'object',
            properties: {},
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_model_probe',
        description: 'Run a bounded live processor probe and report latency and output speed.',
        inputSchema: {
            type: 'object',
            properties: {
                processor_id: {
                    type: 'string',
                    description: 'Optional processor id or name. Defaults to the first configured processor.',
                },
                text: {
                    type: 'string',
                    description: 'Probe text, capped server-side.',
                },
                timeout_ms: {
                    type: 'number',
                    description: 'Request timeout in milliseconds, capped server-side.',
                },
                max_tokens: {
                    type: 'number',
                    description: 'Max output tokens, capped server-side.',
                },
                temperature: {
                    type: 'number',
                    description: 'Probe temperature.',
                },
            },
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_codex_bridge_status',
        description: 'Return idol-bbq -> MCP -> Codex bridge status and available Codex MCP tools.',
        inputSchema: {
            type: 'object',
            properties: {},
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_codex_bridge_run',
        description:
            'Ask Codex through the idol-bbq Codex MCP bridge. Intended for non-Codex MCP clients; requires server-side enablement.',
        inputSchema: {
            type: 'object',
            properties: {
                prompt: {
                    type: 'string',
                    description: 'Initial user prompt for Codex.',
                },
                cwd: {
                    type: 'string',
                    description: 'Optional working directory for Codex.',
                },
                model: {
                    type: 'string',
                    description: 'Optional Codex model override.',
                },
                sandbox: {
                    type: 'string',
                    enum: ['read-only', 'workspace-write', 'danger-full-access'],
                },
                approval_policy: {
                    type: 'string',
                    enum: ['untrusted', 'on-failure', 'on-request', 'never'],
                },
                developer_instructions: {
                    type: 'string',
                    description: 'Optional developer instructions for the Codex run.',
                },
                timeout_ms: {
                    type: 'number',
                    description: 'Request timeout in milliseconds, capped server-side.',
                },
            },
            required: ['prompt'],
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_codex_bridge_reply',
        description: 'Continue a Codex bridge conversation by thread id. Requires server-side enablement.',
        inputSchema: {
            type: 'object',
            properties: {
                thread_id: {
                    type: 'string',
                    description: 'Codex thread id returned by idol_bbq_codex_bridge_run.',
                },
                prompt: {
                    type: 'string',
                    description: 'Next user prompt for Codex.',
                },
                timeout_ms: {
                    type: 'number',
                    description: 'Request timeout in milliseconds, capped server-side.',
                },
            },
            required: ['thread_id', 'prompt'],
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_runtime_status',
        description: 'Return the fuller existing /api/runtime/status payload.',
        inputSchema: {
            type: 'object',
            properties: {},
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_runtime_reload',
        description: 'Reload the runtime config. Requires confirm="reload".',
        inputSchema: {
            type: 'object',
            properties: {
                confirm: {
                    type: 'string',
                    description: 'Must be exactly "reload".',
                },
            },
            required: ['confirm'],
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_crawler_schedule_status',
        description: 'Return the hot non-Cron crawler schedule snapshot.',
        inputSchema: {
            type: 'object',
            properties: {},
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_crawler_schedule_recommendations',
        description: 'Return DB-derived dense stable crawler schedule recommendations.',
        inputSchema: {
            type: 'object',
            properties: {
                days: {
                    type: 'number',
                    description: 'History window in days. Defaults to 120.',
                },
            },
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_crawler_schedule_insert',
        description: 'Temporarily insert a crawler run time into the hot schedule.',
        inputSchema: {
            type: 'object',
            properties: {
                crawler: { type: 'string' },
                execute_at: { type: ['number', 'string'] },
                delay_seconds: { type: 'number' },
                reason: { type: 'string' },
                websites: {
                    type: 'array',
                    items: { type: 'string' },
                },
                idempotency_key: { type: 'string' },
            },
            required: ['crawler'],
            additionalProperties: false,
        },
    },
    {
        name: 'idol_bbq_crawler_schedule_upsert',
        description: 'Hot-update one crawler schedule without runtime reload.',
        inputSchema: {
            type: 'object',
            properties: {
                crawler: { type: 'string' },
                schedule: {
                    type: 'object',
                    additionalProperties: true,
                },
            },
            required: ['crawler', 'schedule'],
            additionalProperties: false,
        },
    },
]

function parseArgs(argv: Array<string>) {
    const options: Record<string, string> = {}
    for (let index = 0; index < argv.length; index += 1) {
        const arg = argv[index]
        if (arg === '--help' || arg === '-h') {
            process.stderr.write(
                [
                    'Usage: bun tools/forwarder-mcp-server.ts [--config PATH] [--api-base URL]',
                    '',
                    'Environment:',
                    '  IDOL_BBQ_AGENT_CONFIG=/path/to/assets/config.yaml',
                    '  IDOL_BBQ_AGENT_API_BASE_URL=http://127.0.0.1:3000',
                    '  IDOL_BBQ_AGENT_API_TOKEN=<bearer token>',
                    '  API_SECRET=<bearer token fallback>',
                    '',
                ].join('\n'),
            )
            process.exit(0)
        }
        if (arg === '--config' || arg === '--api-base') {
            const value = argv[index + 1]
            if (!value) {
                throw new Error(`Missing value for ${arg}`)
            }
            options[arg.slice(2)] = value
            index += 1
        }
    }
    return options
}

const CLI_OPTIONS = parseArgs(process.argv.slice(2))

function findConfigPath() {
    const candidates = [
        CLI_OPTIONS.config,
        process.env.IDOL_BBQ_AGENT_CONFIG,
        path.join(process.cwd(), 'assets/config.yaml'),
        path.join(process.cwd(), 'config.yaml'),
        '/app/config.yaml',
    ].filter(Boolean) as Array<string>

    return candidates.find((candidate) => fs.existsSync(candidate)) || null
}

function readRuntimeConfig() {
    const configPath = findConfigPath()
    if (!configPath) {
        return {}
    }
    const text = fs.readFileSync(configPath, 'utf8')
    return (YAML.parse(text) || {}) as { api?: { port?: number; secret?: string } }
}

function resolveRuntimeApiConfig(): RuntimeApiConfig {
    const fileConfig = readRuntimeConfig()
    const configuredPort = Number(fileConfig.api?.port || DEFAULT_API_PORT)
    const baseUrl =
        CLI_OPTIONS['api-base'] ||
        process.env.IDOL_BBQ_AGENT_API_BASE_URL ||
        `http://127.0.0.1:${Number.isFinite(configuredPort) ? configuredPort : DEFAULT_API_PORT}`
    const token = process.env.IDOL_BBQ_AGENT_API_TOKEN || process.env.API_SECRET || fileConfig.api?.secret || ''

    if (!token) {
        throw new Error('Missing idol-bbq API token. Set IDOL_BBQ_AGENT_API_TOKEN or provide api.secret in config.')
    }
    return {
        baseUrl,
        token,
    }
}

function textToolResult(payload: unknown, isError = false) {
    return {
        content: [
            {
                type: 'text',
                text: typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2),
            },
        ],
        isError,
    }
}

async function runtimeApiRequest(method: string, pathname: string, body?: unknown) {
    const config = resolveRuntimeApiConfig()
    const response = await fetch(new URL(pathname, config.baseUrl), {
        method,
        headers: {
            Authorization: `Bearer ${config.token}`,
            ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
    })
    const text = await response.text()
    const payload = text ? (tryParseJson(text) ?? text) : null
    if (!response.ok) {
        const message = typeof payload === 'string' ? payload.slice(0, 500) : JSON.stringify(payload)
        throw new Error(`idol-bbq API ${method} ${pathname} failed: HTTP ${response.status}: ${message}`)
    }
    return payload
}

function tryParseJson(text: string) {
    try {
        return JSON.parse(text)
    } catch {
        return null
    }
}

async function callTool(name: string, args: Record<string, unknown>) {
    switch (name) {
        case 'idol_bbq_agent_status':
            return textToolResult(await runtimeApiRequest('GET', '/api/agent/status'))
        case 'idol_bbq_model_capabilities':
            return textToolResult(await runtimeApiRequest('GET', '/api/agent/models'))
        case 'idol_bbq_model_probe':
            return textToolResult(await runtimeApiRequest('POST', '/api/agent/probe-model', args || {}))
        case 'idol_bbq_codex_bridge_status':
            return textToolResult(await runtimeApiRequest('GET', '/api/agent/codex/status'))
        case 'idol_bbq_codex_bridge_run':
            return textToolResult(await runtimeApiRequest('POST', '/api/agent/codex/run', args || {}))
        case 'idol_bbq_codex_bridge_reply':
            return textToolResult(await runtimeApiRequest('POST', '/api/agent/codex/reply', args || {}))
        case 'idol_bbq_runtime_status':
            return textToolResult(await runtimeApiRequest('GET', '/api/runtime/status'))
        case 'idol_bbq_runtime_reload':
            if (args?.confirm !== 'reload') {
                return textToolResult({ success: false, error: 'confirm must be exactly "reload"' }, true)
            }
            return textToolResult(await runtimeApiRequest('POST', '/api/runtime/reload'))
        case 'idol_bbq_crawler_schedule_status':
            return textToolResult(await runtimeApiRequest('GET', '/api/schedules/crawlers'))
        case 'idol_bbq_crawler_schedule_recommendations': {
            const days = Number(args?.days || 120)
            const suffix = Number.isFinite(days) ? `?days=${encodeURIComponent(String(Math.trunc(days)))}` : ''
            return textToolResult(await runtimeApiRequest('GET', `/api/schedules/crawlers/recommendations${suffix}`))
        }
        case 'idol_bbq_crawler_schedule_insert':
            return textToolResult(await runtimeApiRequest('POST', '/api/schedules/crawlers/insert', args || {}))
        case 'idol_bbq_crawler_schedule_upsert':
            return textToolResult(await runtimeApiRequest('POST', '/api/schedules/crawlers/upsert', args || {}))
        default:
            return textToolResult({ success: false, error: `unknown tool: ${name}` }, true)
    }
}

function sendMessage(message: Record<string, unknown>) {
    const json = JSON.stringify(message)
    process.stdout.write(`Content-Length: ${Buffer.byteLength(json, 'utf8')}\r\n\r\n${json}`)
}

function sendResult(id: JsonRpcId | undefined, result: unknown) {
    if (id === undefined) {
        return
    }
    sendMessage({
        jsonrpc: '2.0',
        id,
        result,
    })
}

function sendError(id: JsonRpcId | undefined, code: number, message: string) {
    if (id === undefined) {
        return
    }
    sendMessage({
        jsonrpc: '2.0',
        id,
        error: {
            code,
            message,
        },
    })
}

async function handleMessage(message: JsonRpcMessage) {
    const id = message.id
    const method = message.method

    try {
        if (!method) {
            sendError(id, -32600, 'Invalid request')
            return
        }
        if (method.startsWith('notifications/')) {
            return
        }
        if (method === 'initialize') {
            sendResult(id, {
                protocolVersion: message.params?.protocolVersion || '2024-11-05',
                capabilities: {
                    tools: {},
                },
                serverInfo: {
                    name: SERVER_NAME,
                    version: SERVER_VERSION,
                },
            })
            return
        }
        if (method === 'ping') {
            sendResult(id, {})
            return
        }
        if (method === 'tools/list') {
            sendResult(id, { tools: TOOLS })
            return
        }
        if (method === 'tools/call') {
            const result = await callTool(String(message.params?.name || ''), message.params?.arguments || {})
            sendResult(id, result)
            return
        }
        if (method === 'resources/list') {
            sendResult(id, { resources: [] })
            return
        }
        if (method === 'prompts/list') {
            sendResult(id, { prompts: [] })
            return
        }
        sendError(id, -32601, `Method not found: ${method}`)
    } catch (error) {
        sendError(id, -32000, error instanceof Error ? error.message : String(error))
    }
}

let inputBuffer = Buffer.alloc(0)

function takeNextMessage() {
    if (inputBuffer.length === 0) {
        return null
    }

    const text = inputBuffer.toString('utf8')
    if (/^Content-Length:/i.test(text)) {
        const crlfEnd = text.indexOf('\r\n\r\n')
        const lfEnd = text.indexOf('\n\n')
        const headerEnd = crlfEnd >= 0 ? crlfEnd : lfEnd
        const delimiter = crlfEnd >= 0 ? '\r\n\r\n' : '\n\n'
        if (headerEnd < 0) {
            return null
        }
        const header = text.slice(0, headerEnd)
        const match = header.match(/Content-Length:\s*(\d+)/i)
        if (!match) {
            throw new Error('Invalid MCP frame: missing Content-Length')
        }
        const contentLength = Number(match[1])
        const bodyStart = Buffer.byteLength(text.slice(0, headerEnd) + delimiter, 'utf8')
        const frameLength = bodyStart + contentLength
        if (inputBuffer.length < frameLength) {
            return null
        }
        const body = inputBuffer.subarray(bodyStart, frameLength).toString('utf8')
        inputBuffer = inputBuffer.subarray(frameLength)
        return JSON.parse(body) as JsonRpcMessage
    }

    const newline = inputBuffer.indexOf(0x0a)
    if (newline < 0) {
        return null
    }
    const line = inputBuffer.subarray(0, newline).toString('utf8').trim()
    inputBuffer = inputBuffer.subarray(newline + 1)
    if (!line) {
        return null
    }
    return JSON.parse(line) as JsonRpcMessage
}

function pumpMessages() {
    while (true) {
        const message = takeNextMessage()
        if (!message) {
            break
        }
        void handleMessage(message)
    }
}

process.stdin.on('data', (chunk) => {
    inputBuffer = Buffer.concat([inputBuffer, chunk])
    try {
        pumpMessages()
    } catch (error) {
        process.stderr.write(`idol-bbq MCP protocol error: ${error instanceof Error ? error.message : String(error)}\n`)
    }
})

process.stdin.on('end', () => {
    process.exit(0)
})

process.stdin.resume()
