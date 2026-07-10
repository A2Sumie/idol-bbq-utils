#!/usr/bin/env bun
/**
 * switch-processor-model.ts — toggle idol-bbq processors between HY3-free (Zen)
 * and DeepSeek V4 Pro (Go) without touching source code.
 *
 * Usage:
 *   bun tools/switch-processor-model.ts hy3     # switch all v4-pro processors to hy3-free
 *   bun tools/switch-processor-model.ts v4pro   # switch back to v4-pro (quick revert)
 *   bun tools/switch-processor-model.ts status  # show current mode per processor
 *
 * The script:
 *   1. Reads assets/config.yaml (single runtime source of truth).
 *   2. Rewrites every processor block (processors[] + inline tag_generation/title_generation).
 *   3. Writes the file with a .bak backup.
 *   4. Hot-reloads via POST /api/config/update if the API is reachable.
 *   5. On reload failure, restores the backup and exits non-zero.
 *
 * Guard: before switching to hy3, probes /api/agent/models. If the runtime image
 * does not report hy3-free capability, it warns and requires --force.
 */

import fs from 'fs'
import path from 'path'
import YAML from 'yaml'

const V4PRO_PROVIDER = 'DeepSeekV4Pro'
const HY3_PROVIDER = 'Hy3Free'
const GO_BASE_URL = 'https://opencode.ai/zen/go/v1/chat/completions'
const ZEN_BASE_URL = 'https://opencode.ai/zen/v1/chat/completions'

const MODES = ['hy3', 'v4pro', 'status'] as const
type Mode = (typeof MODES)[number]

interface ProcessorLikeBlock {
    id?: string
    name?: string
    provider?: string
    api_key?: string
    cfg_processor?: Record<string, unknown> & {
        model_id?: string
        base_url?: string
        temperature?: number
        fallback?: Record<string, unknown>
        extended_payload?: Record<string, unknown>
    }
}

function parseArgs(argv: string[]): { mode: Mode; force: boolean; noReload: boolean; configPath: string } {
    const args = argv.slice(2)
    const force = args.includes('--force')
    const noReload = args.includes('--no-reload')
    const modeArg = args.find((a) => !a.startsWith('--')) as Mode | undefined
    if (!modeArg || !MODES.includes(modeArg)) {
        console.error(`Usage: bun tools/switch-processor-model.ts <hy3|v4pro|status> [--force] [--no-reload]`)
        process.exit(2)
    }
    const configPath = path.join(process.cwd(), 'assets/config.yaml')
    if (!fs.existsSync(configPath)) {
        console.error(`Config not found: ${configPath}`)
        process.exit(2)
    }
    return { mode: modeArg, force, noReload, configPath }
}

function readConfig(configPath: string): unknown {
    const raw = fs.readFileSync(configPath, 'utf8')
    return YAML.parse(raw)
}

function isProcessorBlock(value: unknown): value is ProcessorLikeBlock {
    return Boolean(
        value &&
            typeof value === 'object' &&
            'provider' in value &&
            'cfg_processor' in value,
    )
}

function switchBlock(block: ProcessorLikeBlock, mode: 'hy3' | 'v4pro'): boolean {
    const provider = String(block.provider || '')
    if (provider !== V4PRO_PROVIDER && provider !== HY3_PROVIDER) {
        return false
    }
    const cfg = block.cfg_processor
    if (!cfg) {
        return false
    }
    const originalTemp = typeof cfg.temperature === 'number' ? cfg.temperature : 1.0

    if (mode === 'hy3') {
        block.provider = HY3_PROVIDER
        cfg.model_id = 'hy3-free'
        cfg.base_url = ZEN_BASE_URL
        cfg.temperature = originalTemp
        delete cfg.extended_payload
        cfg.fallback = {
            provider: V4PRO_PROVIDER,
            model_id: 'deepseek-v4-pro',
            base_url: GO_BASE_URL,
            temperature: originalTemp,
            extended_payload: { thinking: { type: 'disabled' } },
        }
    } else {
        block.provider = V4PRO_PROVIDER
        cfg.model_id = 'deepseek-v4-pro'
        delete cfg.base_url
        cfg.temperature = originalTemp
        delete cfg.fallback
        delete cfg.extended_payload
    }
    return true
}

function collectProcessorBlocks(config: any): Array<{ label: string; block: ProcessorLikeBlock }> {
    const found: Array<{ label: string; block: ProcessorLikeBlock }> = []

    for (const proc of config?.processors || []) {
        if (isProcessorBlock(proc)) {
            found.push({ label: `processors[${proc.id || proc.name || '?'}]`, block: proc })
        }
    }

    for (const target of config?.forward_targets || []) {
        const cfgPlatform = target?.cfg_platform
        if (!cfgPlatform) continue
        const videoUpload = cfgPlatform.video_upload
        if (!videoUpload) continue
        const tg = videoUpload.tag_generation
        if (tg && isProcessorBlock(tg)) {
            found.push({ label: `forward_targets[${target.id || target.name || '?'}].tag_generation`, block: tg })
        }
        const titleGen = videoUpload.title_generation
        if (titleGen && typeof titleGen === 'object' && isProcessorBlock(titleGen)) {
            found.push({
                label: `forward_targets[${target.id || target.name || '?'}].title_generation`,
                block: titleGen,
            })
        }
    }

    return found
}

async function probeRuntimeSupportsHy3(): Promise<{ reachable: boolean; supportsHy3: boolean }> {
    const baseUrl = process.env.IDOL_BBQ_API_BASE_URL || 'http://localhost:3000'
    const secret = process.env.IDOL_BBQ_AGENT_API_TOKEN || process.env.API_SECRET
    try {
        const res = await fetch(`${baseUrl}/api/agent/models`, {
            headers: { Authorization: `Bearer ${secret || ''}` },
            signal: AbortSignal.timeout(5000),
        })
        if (!res.ok) return { reachable: true, supportsHy3: false }
        const data = (await res.json()) as { models?: Array<{ model_id?: string; hy3?: { frozen: boolean } }> }
        const supportsHy3 = Boolean(data.models?.some((m) => m.model_id === 'hy3-free'))
        return { reachable: true, supportsHy3 }
    } catch {
        return { reachable: false, supportsHy3: false }
    }
}

async function hotReload(
    config: unknown,
): Promise<{ ok: boolean; unreachable: boolean; message: string }> {
    const baseUrl = process.env.IDOL_BBQ_API_BASE_URL || 'http://localhost:3000'
    const secret = process.env.IDOL_BBQ_AGENT_API_TOKEN || process.env.API_SECRET
    if (!secret) {
        return { ok: false, unreachable: true, message: 'API secret not set (IDOL_BBQ_AGENT_API_TOKEN/API_SECRET) — file written, restart server to apply' }
    }
    try {
        const res = await fetch(`${baseUrl}/api/config/update`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${secret}`, 'Content-Type': 'application/json' },
            body: JSON.stringify(config),
            signal: AbortSignal.timeout(15000),
        })
        if (!res.ok) {
            const text = await res.text().catch(() => 'unknown')
            return { ok: false, unreachable: false, message: `reload HTTP ${res.status}: ${text.slice(0, 300)}` }
        }
        const data = (await res.json()) as { success?: boolean; message?: string }
        return { ok: true, unreachable: false, message: data.message || 'reloaded' }
    } catch (error) {
        return {
            ok: false,
            unreachable: true,
            message: `reload unreachable: ${error instanceof Error ? error.message : String(error)}`,
        }
    }
}

async function main() {
    const { mode, force, noReload, configPath } = parseArgs(process.argv)
    const config = readConfig(configPath) as any
    const blocks = collectProcessorBlocks(config)

    if (blocks.length === 0) {
        console.error('No DeepSeekV4Pro/Hy3Free processor blocks found in config.')
        process.exit(1)
    }

    if (mode === 'status') {
        console.log('Processor model status:')
        for (const { label, block } of blocks) {
            const modelId = block.cfg_processor?.model_id || '(default)'
            const hasFallback = Boolean(block.cfg_processor?.fallback)
            console.log(`  ${label}: provider=${block.provider} model=${modelId} fallback=${hasFallback ? 'yes' : 'no'}`)
        }
        return
    }

    if (mode === 'hy3' && !force) {
        const probe = await probeRuntimeSupportsHy3()
        if (probe.reachable && !probe.supportsHy3) {
            console.error(
                'Runtime is reachable but does not report hy3-free capability.\n' +
                    'Deploy the Hy3Free code first, or use --force to switch config anyway.',
            )
            process.exit(1)
        }
        if (!probe.reachable) {
            console.warn('Warning: runtime API not reachable — cannot verify Hy3Free support. Proceeding (file will be written, reload will be attempted).')
        }
    }

    let changed = 0
    let skipped = 0
    for (const { label, block } of blocks) {
        if (mode === 'hy3') {
            const rf = block.cfg_processor?.response_format
            if (rf === 'json_object' || rf === 'json_schema') {
                console.log(`  skipped ${label}: response_format=${rf} not supported by hy3-free, keeping v4-pro`)
                skipped++
                continue
            }
            const maxTokens = block.cfg_processor?.max_tokens
            if (typeof maxTokens === 'number' && maxTokens < 1024) {
                block.cfg_processor!.max_tokens = 2048
                console.log(`  bumped ${label} max_tokens ${maxTokens} -> 2048 (hy3 reasoning needs headroom)`)
            }
        }
        if (switchBlock(block, mode)) {
            changed++
            console.log(`  switched ${label} -> ${mode}`)
        }
    }

    if (changed === 0) {
        console.log(`No blocks needed switching. (${skipped} skipped due to json_object incompatibility)`)
        return
    }

    const backupPath = `${configPath}.bak-switch`
    fs.copyFileSync(configPath, backupPath)
    console.log(`backup: ${backupPath}`)

    fs.writeFileSync(configPath, YAML.stringify(config), 'utf8')
    console.log(`written: ${configPath} (${changed} block${changed > 1 ? 's' : ''})`)

    if (noReload) {
        console.log('skipped hot-reload (--no-reload). Restart the server to apply.')
        return
    }

    const reload = await hotReload(config)
    if (reload.ok) {
        console.log(`hot-reload: ${reload.message}`)
    } else if (reload.unreachable) {
        console.warn(`warning: ${reload.message}`)
        console.warn('Config file is written. Restart the server to apply.')
    } else {
        console.error(`hot-reload rejected: ${reload.message}`)
        fs.copyFileSync(backupPath, configPath)
        console.error(`Restored backup. Fix the issue and re-run.`)
        process.exit(1)
    }
}

main().catch((error) => {
    console.error(error)
    process.exit(1)
})
