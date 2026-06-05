import { expect, test } from 'bun:test'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import {
    captureOutboundSend,
    isNonLiveOutboundSendMode,
    normalizeOutboundSendModeValue,
    resolveOutboundCaptureFile,
} from './outbound-send-mode'

test('outbound send mode normalization accepts live, blocked, and capture aliases', () => {
    expect(normalizeOutboundSendModeValue(undefined)).toBe('live')
    expect(normalizeOutboundSendModeValue('online')).toBe('live')
    expect(normalizeOutboundSendModeValue('dry_run')).toBe('blocked')
    expect(normalizeOutboundSendModeValue('test-receiver')).toBe('capture')
    expect(normalizeOutboundSendModeValue('sink')).toBe('capture')
    expect(isNonLiveOutboundSendMode('live')).toBe(false)
    expect(isNonLiveOutboundSendMode('blocked')).toBe(true)
    expect(isNonLiveOutboundSendMode('capture')).toBe(true)
})

test('captureOutboundSend appends payloads to the configured capture file', async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), 'outbound-capture-mode-'))
    const captureFile = path.join(tempRoot, 'capture.jsonl')
    try {
        const result = await captureOutboundSend(
            {
                schema_version: 1,
                send_mode: 'capture',
                captured_at: '2026-06-05T00:00:00.000Z',
                target_id: 'target-a',
                forwarder: 'qq',
                text_count: 1,
                text_length: 5,
                texts: ['hello'],
                media: [],
                card_media: [],
                content_media: [],
            },
            {
                IDOL_BBQ_OUTBOUND_CAPTURE_FILE: captureFile,
            },
        )

        expect(result).toEqual({
            kind: 'file',
            destination: captureFile,
            ok: true,
        })
        expect(resolveOutboundCaptureFile({ IDOL_BBQ_OUTBOUND_CAPTURE_FILE: captureFile })).toBe(captureFile)
        const lines = (await readFile(captureFile, 'utf8')).trim().split('\n')
        expect(JSON.parse(lines[0]).target_id).toBe('target-a')
    } finally {
        await rm(tempRoot, { recursive: true, force: true })
    }
})
