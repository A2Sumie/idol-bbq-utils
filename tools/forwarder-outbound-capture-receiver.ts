#!/usr/bin/env bun

import { appendFile, mkdir } from 'node:fs/promises'
import nodePath from 'node:path'

const host = process.env.HOST || '127.0.0.1'
const port = Number(process.env.PORT || 3999)
const outputFile = process.env.OUTBOUND_CAPTURE_FILE || '/tmp/tweet-forwarder/outbound-capture-receiver.jsonl'

async function appendRecord(record: Record<string, unknown>) {
    await mkdir(nodePath.dirname(outputFile), { recursive: true })
    await appendFile(outputFile, `${JSON.stringify(record)}\n`, 'utf8')
}

const server = Bun.serve({
    hostname: host,
    port,
    async fetch(request) {
        const url = new URL(request.url)
        if (request.method === 'GET' && url.pathname === '/health') {
            return Response.json({
                ok: true,
                output_file: outputFile,
            })
        }

        if (request.method !== 'POST' || !['/', '/capture'].includes(url.pathname)) {
            return new Response('not found\n', { status: 404 })
        }

        const bodyText = await request.text()
        let body: unknown = bodyText
        try {
            body = JSON.parse(bodyText)
        } catch {
            body = bodyText
        }

        await appendRecord({
            received_at: new Date().toISOString(),
            method: request.method,
            path: url.pathname,
            body,
        })

        return Response.json({ ok: true, output_file: outputFile }, { status: 202 })
    },
})

console.log(`forwarder outbound capture receiver listening on http://${host}:${server.port}/capture`)
console.log(`writing captures to ${outputFile}`)
