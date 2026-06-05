import fs from 'fs'
import path from 'path'
import type { ProcessorPromptAssetConfig } from '@/types/processor'

type PromptAssetInput = string | ProcessorPromptAssetConfig

type NormalizedPromptAsset = ProcessorPromptAssetConfig & {
    path: string
}

type TranslationTerm = {
    source?: unknown
    target?: unknown
    note?: unknown
}

function normalizePromptAsset(asset: PromptAssetInput): NormalizedPromptAsset {
    if (typeof asset === 'string') {
        return { path: asset }
    }
    return asset
}

function resolveConfiguredPromptAssetPath(assetPath: string, cwd = process.cwd()) {
    const value = String(assetPath || '').trim()
    if (!value) {
        return null
    }

    if (path.isAbsolute(value)) {
        if (fs.existsSync(value)) {
            return value
        }
        if (value.startsWith('/app/')) {
            const localMirror = path.resolve(cwd, value.slice('/app/'.length))
            if (fs.existsSync(localMirror)) {
                return localMirror
            }
        }
        return value
    }

    return path.resolve(cwd, value)
}

function limitText(text: string, maxChars?: number) {
    const limit = Number(maxChars)
    if (!Number.isFinite(limit) || limit <= 0 || text.length <= limit) {
        return text
    }
    return text.slice(0, Math.floor(limit)).trimEnd()
}

function isTranslationTerm(value: TranslationTerm): value is { source: string; target: string; note?: string } {
    return typeof value.source === 'string' && typeof value.target === 'string'
}

function renderTranslationTermsJson(raw: string, asset: NormalizedPromptAsset) {
    const parsed = JSON.parse(raw) as unknown
    if (!Array.isArray(parsed)) {
        throw new Error(`Prompt asset is not a translation term array: ${asset.path}`)
    }

    const maxItems = Math.max(0, Math.floor(Number(asset.max_items ?? parsed.length)))
    const lines = parsed
        .filter((item): item is TranslationTerm => Boolean(item) && typeof item === 'object')
        .filter(isTranslationTerm)
        .slice(0, maxItems)
        .map((term) => {
            const note = typeof term.note === 'string' && term.note ? ` (${term.note})` : ''
            return `- ${term.source} => ${term.target}${note}`
        })

    if (!lines.length) {
        return ''
    }
    return [`${asset.label || 'Terminology glossary'}:`, ...lines].join('\n')
}

function renderTextAsset(raw: string, asset: NormalizedPromptAsset, resolvedPath: string) {
    const label = asset.label || path.basename(resolvedPath)
    const body = limitText(raw.trim(), asset.max_chars)
    if (!body) {
        return ''
    }
    return `${label}:\n${body}`
}

function inferPromptAssetFormat(asset: NormalizedPromptAsset, resolvedPath: string, raw: string) {
    if (asset.format) {
        return asset.format
    }
    if (resolvedPath.endsWith('.json')) {
        try {
            const parsed = JSON.parse(raw) as unknown
            if (
                Array.isArray(parsed) &&
                parsed.some(
                    (item) =>
                        item &&
                        typeof item === 'object' &&
                        typeof (item as TranslationTerm).source === 'string' &&
                        typeof (item as TranslationTerm).target === 'string',
                )
            ) {
                return 'translation_terms_json'
            }
        } catch {
            return 'text'
        }
    }
    return 'text'
}

function renderPromptAsset(assetInput: PromptAssetInput, cwd = process.cwd()) {
    const asset = normalizePromptAsset(assetInput)
    const resolvedPath = resolveConfiguredPromptAssetPath(asset.path, cwd)
    if (!resolvedPath) {
        if (asset.optional) {
            return ''
        }
        throw new Error('Prompt asset path is empty')
    }
    if (!fs.existsSync(resolvedPath)) {
        if (asset.optional) {
            return ''
        }
        throw new Error(`Prompt asset not found: ${asset.path}`)
    }

    const raw = fs.readFileSync(resolvedPath, 'utf8')
    const format = inferPromptAssetFormat(asset, resolvedPath, raw)
    if (format === 'translation_terms_json') {
        return limitText(renderTranslationTermsJson(raw, asset), asset.max_chars)
    }
    return renderTextAsset(raw, asset, resolvedPath)
}

function buildProcessorPrompt(basePrompt: string, assets?: Array<PromptAssetInput>, cwd = process.cwd()) {
    const parts = [String(basePrompt || '').trim()]
    for (const asset of assets || []) {
        const rendered = renderPromptAsset(asset, cwd).trim()
        if (rendered) {
            parts.push(rendered)
        }
    }
    return parts.filter(Boolean).join('\n\n')
}

function loadProcessorJsonAsset(assetPath: string | null | undefined, cwd = process.cwd()) {
    const resolvedPath = resolveConfiguredPromptAssetPath(assetPath || '', cwd)
    if (!resolvedPath) {
        return null
    }
    if (!fs.existsSync(resolvedPath)) {
        throw new Error(`Processor JSON asset not found: ${assetPath}`)
    }
    return JSON.parse(fs.readFileSync(resolvedPath, 'utf8')) as Record<string, unknown>
}

function summarizePromptAssets(assets?: Array<PromptAssetInput>) {
    return (assets || []).map((asset) => {
        const normalized = normalizePromptAsset(asset)
        return {
            path: normalized.path,
            label: normalized.label || null,
            format: normalized.format || null,
            optional: normalized.optional === true,
        }
    })
}

export {
    buildProcessorPrompt,
    loadProcessorJsonAsset,
    renderPromptAsset,
    resolveConfiguredPromptAssetPath,
    summarizePromptAssets,
}
