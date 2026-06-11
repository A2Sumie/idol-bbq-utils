import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from 'node:child_process'

type JsonRpcId = string | number | null

interface JsonRpcMessage {
    jsonrpc?: '2.0'
    id?: JsonRpcId
    method?: string
    params?: unknown
    result?: unknown
    error?: {
        code?: number
        message?: string
        data?: unknown
    }
}

interface PendingRequest {
    resolve: (value: JsonRpcMessage) => void
    reject: (error: Error) => void
    timer: ReturnType<typeof setTimeout>
}

interface CodexMcpCommandConfig {
    command: string
    args: string[]
    cwd: string
    env?: Record<string, string | undefined>
}

interface CodexMcpBridgeConfig {
    url: string
    token?: string
}

interface CodexMcpClientConfig {
    enabled: boolean
    command: CodexMcpCommandConfig
    bridge?: CodexMcpBridgeConfig
}

interface CodexMcpStatus {
    service: 'codex-mcp'
    enabled: boolean
    mode: 'stdio' | 'http-bridge'
    available: boolean
    command?: string
    args?: string[]
    cwd?: string
    bridge_url?: string
    version?: string
    server_info?: unknown
    tools?: Array<{ name: string; title?: string; description?: string }>
    latency_ms?: number
    error?: string
}

interface CodexMcpRunRequest {
    prompt: string
    cwd?: string
    model?: string
    sandbox?: 'read-only' | 'workspace-write' | 'danger-full-access'
    approval_policy?: 'untrusted' | 'on-failure' | 'on-request' | 'never'
    developer_instructions?: string
    base_instructions?: string
    config?: Record<string, unknown>
    timeout_ms?: number
}

interface CodexMcpReplyRequest {
    thread_id: string
    prompt: string
    timeout_ms?: number
}

interface CodexMcpToolResult {
    success: true
    thread_id: string | null
    content: string
    latency_ms: number
    raw_result_type: string
}

const DEFAULT_CODEX_MCP_ARGS = ['mcp-server', '-c', 'approval_policy="never"', '-c', 'sandbox_mode="workspace-write"']
const DEFAULT_CODEX_MCP_TIMEOUT_MS = 120_000
const STATUS_TIMEOUT_MS = 15_000

class CodexMcpDisabledError extends Error {
    constructor() {
        super('Codex MCP bridge is disabled. Set IDOL_BBQ_CODEX_MCP_ENABLED=1 to allow calls.')
        this.name = 'CodexMcpDisabledError'
    }
}

class JsonRpcStdioClient {
    private child?: ChildProcessWithoutNullStreams
    private inputBuffer = Buffer.alloc(0)
    private nextId = 1
    private pending = new Map<JsonRpcId, PendingRequest>()
    private stderr = ''

    constructor(private readonly config: CodexMcpCommandConfig) {}

    start() {
        this.child = spawn(this.config.command, this.config.args, {
            cwd: this.config.cwd,
            env: {
                ...process.env,
                ...this.config.env,
            },
            stdio: ['pipe', 'pipe', 'pipe'],
        })

        this.child.stdout.on('data', (chunk) => {
            this.inputBuffer = Buffer.concat([this.inputBuffer, chunk])
            this.pumpMessages()
        })
        this.child.stderr.on('data', (chunk) => {
            this.stderr = `${this.stderr}${chunk.toString('utf8')}`.slice(-4000)
        })
        this.child.on('error', (error) => {
            this.rejectAll(error)
        })
        this.child.on('exit', (code, signal) => {
            if (this.pending.size > 0) {
                this.rejectAll(new Error(`Codex MCP process exited before response: code=${code} signal=${signal}`))
            }
        })
    }

    getStderr() {
        return this.stderr.trim()
    }

    notify(method: string, params: unknown = {}) {
        this.writeMessage({
            jsonrpc: '2.0',
            method,
            params,
        })
    }

    request(method: string, params: unknown = {}, timeoutMs = DEFAULT_CODEX_MCP_TIMEOUT_MS) {
        const id = this.nextId++
        this.writeMessage({
            jsonrpc: '2.0',
            id,
            method,
            params,
        })
        return new Promise<JsonRpcMessage>((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pending.delete(id)
                reject(new Error(`Codex MCP request timed out: ${method}`))
            }, timeoutMs)
            this.pending.set(id, { resolve, reject, timer })
        }).then((message) => {
            if (message.error) {
                throw new Error(message.error.message || `Codex MCP error ${message.error.code || 'unknown'}`)
            }
            return message.result
        })
    }

    close() {
        if (!this.child || this.child.killed) {
            return
        }
        this.child.kill('SIGTERM')
        setTimeout(() => {
            if (this.child && !this.child.killed) {
                this.child.kill('SIGKILL')
            }
        }, 500).unref()
    }

    private writeMessage(message: Record<string, unknown>) {
        if (!this.child?.stdin.writable) {
            throw new Error('Codex MCP process is not writable')
        }
        this.child.stdin.write(`${JSON.stringify(message)}\n`)
    }

    private pumpMessages() {
        while (true) {
            const message = this.takeNextMessage()
            if (!message) {
                break
            }
            if (message.id !== undefined && this.pending.has(message.id)) {
                const pending = this.pending.get(message.id)!
                clearTimeout(pending.timer)
                this.pending.delete(message.id)
                pending.resolve(message)
            }
        }
    }

    private takeNextMessage(): JsonRpcMessage | null {
        if (this.inputBuffer.length === 0) {
            return null
        }

        const text = this.inputBuffer.toString('utf8')
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
            if (this.inputBuffer.length < frameLength) {
                return null
            }
            const body = this.inputBuffer.subarray(bodyStart, frameLength).toString('utf8')
            this.inputBuffer = this.inputBuffer.subarray(frameLength)
            return JSON.parse(body) as JsonRpcMessage
        }

        const newline = this.inputBuffer.indexOf(0x0a)
        if (newline < 0) {
            return null
        }
        const line = this.inputBuffer.subarray(0, newline).toString('utf8').trim()
        this.inputBuffer = this.inputBuffer.subarray(newline + 1)
        if (!line) {
            return null
        }
        return JSON.parse(line) as JsonRpcMessage
    }

    private rejectAll(error: Error) {
        for (const [id, pending] of this.pending.entries()) {
            clearTimeout(pending.timer)
            pending.reject(error)
            this.pending.delete(id)
        }
    }
}

class CodexMcpClientService {
    constructor(private readonly config: CodexMcpClientConfig) {}

    isEnabled() {
        return this.config.enabled
    }

    async status(timeoutMs = STATUS_TIMEOUT_MS): Promise<CodexMcpStatus> {
        if (this.config.bridge?.url) {
            const status = await this.bridgeRequest<CodexMcpStatus>('GET', '/status', undefined, timeoutMs)
            return {
                ...status,
                mode: 'http-bridge',
                bridge_url: this.config.bridge.url,
                enabled: this.config.enabled,
            }
        }

        const started = performance.now()
        const client = new JsonRpcStdioClient(this.config.command)
        try {
            const version = readCommandVersion(this.config.command.command)
            client.start()
            const initialize = (await client.request(
                'initialize',
                {
                    protocolVersion: '2024-11-05',
                    capabilities: {},
                    clientInfo: {
                        name: 'idol-bbq-codex-bridge',
                        version: '0.1.0',
                    },
                },
                timeoutMs,
            )) as { serverInfo?: unknown }
            client.notify('notifications/initialized')
            const toolsResult = (await client.request('tools/list', {}, timeoutMs)) as {
                tools?: Array<{ name: string; title?: string; description?: string }>
            }
            return {
                service: 'codex-mcp',
                enabled: this.config.enabled,
                mode: 'stdio',
                available: true,
                command: this.config.command.command,
                args: this.config.command.args,
                cwd: this.config.command.cwd,
                version,
                server_info: initialize.serverInfo,
                tools: toolsResult.tools || [],
                latency_ms: elapsedMs(started),
            }
        } catch (error) {
            const stderr = client.getStderr()
            return {
                service: 'codex-mcp',
                enabled: this.config.enabled,
                mode: 'stdio',
                available: false,
                command: this.config.command.command,
                args: this.config.command.args,
                cwd: this.config.command.cwd,
                latency_ms: elapsedMs(started),
                error: [error instanceof Error ? error.message : String(error), stderr].filter(Boolean).join(': '),
            }
        } finally {
            client.close()
        }
    }

    async run(request: CodexMcpRunRequest): Promise<CodexMcpToolResult> {
        this.assertEnabled()
        if (this.config.bridge?.url) {
            return this.bridgeRequest<CodexMcpToolResult>(
                'POST',
                '/run',
                request,
                request.timeout_ms || DEFAULT_CODEX_MCP_TIMEOUT_MS,
            )
        }
        return this.callCodexTool('codex', buildCodexToolArgs(request), request.timeout_ms)
    }

    async reply(request: CodexMcpReplyRequest): Promise<CodexMcpToolResult> {
        this.assertEnabled()
        if (this.config.bridge?.url) {
            return this.bridgeRequest<CodexMcpToolResult>(
                'POST',
                '/reply',
                request,
                request.timeout_ms || DEFAULT_CODEX_MCP_TIMEOUT_MS,
            )
        }
        return this.callCodexTool(
            'codex-reply',
            {
                threadId: request.thread_id,
                prompt: request.prompt,
            },
            request.timeout_ms,
        )
    }

    private async callCodexTool(name: string, args: Record<string, unknown>, timeoutMs = DEFAULT_CODEX_MCP_TIMEOUT_MS) {
        const started = performance.now()
        const client = new JsonRpcStdioClient(this.config.command)
        try {
            client.start()
            await client.request(
                'initialize',
                {
                    protocolVersion: '2024-11-05',
                    capabilities: {},
                    clientInfo: {
                        name: 'idol-bbq-codex-bridge',
                        version: '0.1.0',
                    },
                },
                Math.min(timeoutMs, STATUS_TIMEOUT_MS),
            )
            client.notify('notifications/initialized')
            const result = await client.request(
                'tools/call',
                {
                    name,
                    arguments: args,
                },
                timeoutMs,
            )
            return normalizeToolResult(result, elapsedMs(started))
        } finally {
            client.close()
        }
    }

    private async bridgeRequest<T>(
        method: string,
        pathname: string,
        body?: unknown,
        timeoutMs = DEFAULT_CODEX_MCP_TIMEOUT_MS,
    ) {
        if (!this.config.bridge?.url) {
            throw new Error('Codex MCP bridge URL is not configured')
        }
        const controller = new AbortController()
        const timer = setTimeout(() => controller.abort(), timeoutMs)
        try {
            const response = await fetch(new URL(pathname, this.config.bridge.url), {
                method,
                headers: {
                    ...(this.config.bridge.token ? { Authorization: `Bearer ${this.config.bridge.token}` } : {}),
                    ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
                },
                body: body === undefined ? undefined : JSON.stringify(body),
                signal: controller.signal,
            })
            const text = await response.text()
            const payload = text ? (tryParseJson(text) ?? text) : null
            if (!response.ok) {
                const message = typeof payload === 'string' ? payload : JSON.stringify(payload)
                throw new Error(`Codex MCP bridge ${method} ${pathname} failed: HTTP ${response.status}: ${message}`)
            }
            return payload as T
        } finally {
            clearTimeout(timer)
        }
    }

    private assertEnabled() {
        if (!this.config.enabled) {
            throw new CodexMcpDisabledError()
        }
    }
}

function buildCodexToolArgs(request: CodexMcpRunRequest) {
    return stripUndefined({
        prompt: request.prompt,
        cwd: request.cwd,
        model: request.model,
        sandbox: request.sandbox,
        'approval-policy': request.approval_policy,
        'developer-instructions': request.developer_instructions,
        'base-instructions': request.base_instructions,
        config: request.config,
    })
}

function normalizeToolResult(result: unknown, latencyMs: number): CodexMcpToolResult {
    const typed = (result || {}) as any
    const structured = typed.structuredContent || typed.structured_content
    const contentText = Array.isArray(typed.content)
        ? typed.content
              .filter((item: any) => item?.type === 'text' && typeof item.text === 'string')
              .map((item: any) => item.text)
              .join('\n')
        : typeof typed.content === 'string'
          ? typed.content
          : ''
    const parsedContent = contentText ? tryParseJson(contentText) : null
    const payload = structured || parsedContent || typed
    const threadId = payload?.threadId || payload?.thread_id || payload?.conversationId || null
    const content = payload?.content || payload?.text || contentText || JSON.stringify(result)
    return {
        success: true,
        thread_id: threadId ? String(threadId) : null,
        content: String(content || ''),
        latency_ms: latencyMs,
        raw_result_type: structured ? 'structuredContent' : contentText ? 'content' : typeof result,
    }
}

function readCommandVersion(command: string) {
    const result = spawnSync(command, ['--version'], { encoding: 'utf8', timeout: 5000 })
    if (result.error) {
        return undefined
    }
    return (
        String(result.stdout || result.stderr || '')
            .trim()
            .split('\n')[0] || undefined
    )
}

function elapsedMs(started: number) {
    return Math.max(1, Math.round(performance.now() - started))
}

function stripUndefined<T extends Record<string, unknown>>(value: T) {
    const result: Record<string, unknown> = {}
    for (const [key, item] of Object.entries(value)) {
        if (item !== undefined && item !== null && item !== '') {
            result[key] = item
        }
    }
    return result
}

function tryParseJson(text: string) {
    try {
        return JSON.parse(text)
    } catch {
        return null
    }
}

function parseBoolean(value?: string | null) {
    return /^(1|true|yes|on)$/i.test(String(value || '').trim())
}

function parseArgsJson(value?: string | null) {
    const text = String(value || '').trim()
    if (!text) {
        return null
    }
    const parsed = JSON.parse(text)
    if (!Array.isArray(parsed) || parsed.some((item) => typeof item !== 'string')) {
        throw new Error('IDOL_BBQ_CODEX_MCP_ARGS_JSON must be a JSON string array')
    }
    return parsed
}

function redactBridgeUrl(url: string) {
    try {
        const parsed = new URL(url)
        parsed.username = ''
        parsed.password = ''
        parsed.search = ''
        parsed.hash = ''
        return parsed.toString()
    } catch {
        return 'invalid-url'
    }
}

function createLocalCodexMcpClientServiceFromEnv(env: NodeJS.ProcessEnv = process.env, cwd = process.cwd()) {
    return new CodexMcpClientService({
        enabled: parseBoolean(env.IDOL_BBQ_CODEX_MCP_ENABLED),
        command: {
            command: env.IDOL_BBQ_CODEX_MCP_COMMAND || 'codex',
            args: parseArgsJson(env.IDOL_BBQ_CODEX_MCP_ARGS_JSON) || DEFAULT_CODEX_MCP_ARGS,
            cwd: env.IDOL_BBQ_CODEX_MCP_CWD || cwd,
        },
    })
}

function createCodexMcpClientServiceFromEnv(env: NodeJS.ProcessEnv = process.env, cwd = process.cwd()) {
    const bridgeUrl = String(env.IDOL_BBQ_CODEX_MCP_BRIDGE_URL || '').trim()
    if (bridgeUrl) {
        return new CodexMcpClientService({
            enabled: parseBoolean(env.IDOL_BBQ_CODEX_MCP_ENABLED),
            command: {
                command: env.IDOL_BBQ_CODEX_MCP_COMMAND || 'codex',
                args: parseArgsJson(env.IDOL_BBQ_CODEX_MCP_ARGS_JSON) || DEFAULT_CODEX_MCP_ARGS,
                cwd: env.IDOL_BBQ_CODEX_MCP_CWD || cwd,
            },
            bridge: {
                url: redactBridgeUrl(bridgeUrl),
                token: env.IDOL_BBQ_CODEX_MCP_BRIDGE_TOKEN,
            },
        })
    }
    return createLocalCodexMcpClientServiceFromEnv(env, cwd)
}

export {
    CodexMcpClientService,
    CodexMcpDisabledError,
    createCodexMcpClientServiceFromEnv,
    createLocalCodexMcpClientServiceFromEnv,
}
export type { CodexMcpReplyRequest, CodexMcpRunRequest, CodexMcpStatus, CodexMcpToolResult }
