import { expect, test } from 'bun:test'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { CodexMcpClientService, CodexMcpDisabledError } from './codex-mcp-client-service'

function writeFakeCodexMcpServer(root: string) {
    const script = join(root, 'fake-codex-mcp.cjs')
    writeFileSync(
        script,
        `
const readline = require('node:readline')
const rl = readline.createInterface({ input: process.stdin })
function send(id, result) {
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, result }) + '\\n')
}
rl.on('line', (line) => {
  if (!line.trim()) return
  const msg = JSON.parse(line)
  if (msg.method === 'initialize') {
    send(msg.id, { protocolVersion: '2024-11-05', capabilities: { tools: {} }, serverInfo: { name: 'fake-codex' } })
    return
  }
  if (msg.method === 'tools/list') {
    send(msg.id, { tools: [{ name: 'codex', title: 'Codex' }, { name: 'codex-reply', title: 'Codex Reply' }] })
    return
  }
  if (msg.method === 'tools/call') {
    const args = msg.params.arguments || {}
    const threadId = args.threadId || 'thread-1'
    const content = msg.params.name === 'codex-reply' ? 'reply:' + args.prompt : 'run:' + args.prompt
    send(msg.id, {
      structuredContent: { threadId, content },
      content: [{ type: 'text', text: JSON.stringify({ threadId, content }) }]
    })
  }
})
`,
    )
    return script
}

test('CodexMcpClientService handshakes with newline JSON-RPC and calls Codex tools', async () => {
    const root = mkdtempSync(join(tmpdir(), 'codex-mcp-fake-'))
    try {
        const script = writeFakeCodexMcpServer(root)
        const service = new CodexMcpClientService({
            enabled: true,
            command: {
                command: process.execPath,
                args: [script],
                cwd: root,
            },
        })

        const status = await service.status(2_000)
        expect(status).toMatchObject({
            service: 'codex-mcp',
            enabled: true,
            mode: 'stdio',
            available: true,
        })
        expect(status.tools?.map((tool) => tool.name)).toEqual(['codex', 'codex-reply'])

        const run = await service.run({ prompt: 'hello', timeout_ms: 2_000 })
        expect(run).toMatchObject({
            success: true,
            thread_id: 'thread-1',
            content: 'run:hello',
        })

        const reply = await service.reply({ thread_id: 'thread-1', prompt: 'again', timeout_ms: 2_000 })
        expect(reply).toMatchObject({
            success: true,
            thread_id: 'thread-1',
            content: 'reply:again',
        })
    } finally {
        rmSync(root, { recursive: true, force: true })
    }
})

test('CodexMcpClientService gates Codex calls behind explicit enablement', async () => {
    const service = new CodexMcpClientService({
        enabled: false,
        command: {
            command: process.execPath,
            args: [],
            cwd: process.cwd(),
        },
    })

    await expect(service.run({ prompt: 'hello' })).rejects.toBeInstanceOf(CodexMcpDisabledError)
})
