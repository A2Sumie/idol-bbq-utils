import { expect, test } from 'bun:test'
import fs from 'fs'
import os from 'os'
import path from 'path'
import {
    buildProcessorPrompt,
    loadProcessorJsonAsset,
    renderPromptAsset,
    resolveConfiguredPromptAssetPath,
} from './processor-prompt-asset-service'
import { BaseProcessor, resolveProcessorApiKey } from '@/middleware/processor/base'

test('buildProcessorPrompt appends text and translation term assets', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'processor-prompt-assets-'))
    try {
        const textPath = path.join(root, 'style.txt')
        const termsPath = path.join(root, 'terms.json')
        fs.writeFileSync(textPath, 'Keep idol names stable.', 'utf8')
        fs.writeFileSync(
            termsPath,
            JSON.stringify([
                { source: 'ナナニジ', target: '22/7', note: 'group nickname' },
                { source: '計算中', target: '计算中' },
            ]),
            'utf8',
        )

        const prompt = buildProcessorPrompt('Base prompt.', [
            { path: textPath, label: 'Style notes' },
            { path: termsPath, label: 'Terms', format: 'translation_terms_json', max_items: 1 },
        ])

        expect(prompt).toContain('Base prompt.')
        expect(prompt).toContain('Style notes:\nKeep idol names stable.')
        expect(prompt).toContain('Terms:\n- ナナニジ => 22/7 (group nickname)')
        expect(prompt).not.toContain('計算中')
    } finally {
        fs.rmSync(root, { recursive: true, force: true })
    }
})

test('resolveConfiguredPromptAssetPath maps container /app paths to local cwd when present', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'processor-prompt-app-path-'))
    try {
        const localPath = path.join(root, 'assets', 'knowledge', 'terms.json')
        fs.mkdirSync(path.dirname(localPath), { recursive: true })
        fs.writeFileSync(localPath, '[]', 'utf8')

        expect(resolveConfiguredPromptAssetPath('/app/assets/knowledge/terms.json', root)).toBe(localPath)
    } finally {
        fs.rmSync(root, { recursive: true, force: true })
    }
})

test('renderPromptAsset skips optional missing files and rejects required missing files', () => {
    expect(renderPromptAsset({ path: '/tmp/idol-bbq-missing-prompt-asset.txt', optional: true })).toBe('')
    expect(() => renderPromptAsset('/tmp/idol-bbq-missing-prompt-asset.txt')).toThrow(/Prompt asset not found/)
})

test('resolveProcessorApiKey supports env indirection', () => {
    process.env.IDOL_BBQ_TEST_PROCESSOR_KEY = 'resolved-key'
    try {
        expect(resolveProcessorApiKey('env:IDOL_BBQ_TEST_PROCESSOR_KEY')).toBe('resolved-key')
        expect(resolveProcessorApiKey('literal-key')).toBe('literal-key')
        expect(() => resolveProcessorApiKey('env:IDOL_BBQ_MISSING_PROCESSOR_KEY')).toThrow(
            /Processor API key env var not set/,
        )
    } finally {
        delete process.env.IDOL_BBQ_TEST_PROCESSOR_KEY
    }
})

test('default processor prompt explicitly targets Simplified Chinese', () => {
    class TestProcessor extends BaseProcessor {
        NAME = 'Prompt Probe'
        protected BASE_URL = ''
        public async process() {
            return ''
        }
        public prompt() {
            return this.getPrompt()
        }
    }

    const prompt = new TestProcessor('').prompt()
    expect(prompt).toContain('目标语言必须是简体中文（zh-CN）')
    expect(prompt).toContain('不要输出原文、繁体中文、日文或英文解释')
})

test('22/7 social translation prompt explicitly targets Simplified Chinese', () => {
    const promptPath = path.resolve(process.cwd(), 'assets/knowledge/22_7/translation/social-ja-zh.prompt.txt')
    const prompt = fs.readFileSync(promptPath, 'utf8')
    expect(prompt).toContain('Target language: Simplified Chinese (zh-CN).')
    expect(prompt).toContain('Return only the translated text in Simplified Chinese (zh-CN).')
})

test('output schema can be loaded from a processor JSON asset', () => {
    class TestProcessor extends BaseProcessor {
        NAME = 'Schema Probe'
        protected BASE_URL = ''
        public async process() {
            return ''
        }
        public requestConfig() {
            return this.buildOpenAICompatibleRequestConfig()
        }
    }

    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'processor-schema-asset-'))
    try {
        const schemaPath = path.join(root, 'schema.json')
        fs.writeFileSync(
            schemaPath,
            JSON.stringify({
                type: 'object',
                properties: {
                    items: { type: 'array' },
                },
                required: ['items'],
            }),
            'utf8',
        )

        expect(loadProcessorJsonAsset(schemaPath)).toMatchObject({
            type: 'object',
            required: ['items'],
        })
        const processor = new TestProcessor('', undefined, {
            output_schema_file: schemaPath,
        })
        expect(processor.requestConfig()).toMatchObject({
            response_format: {
                type: 'json_schema',
                json_schema: {
                    name: 'schema_probe',
                    schema: {
                        type: 'object',
                        required: ['items'],
                    },
                },
            },
        })
    } finally {
        fs.rmSync(root, { recursive: true, force: true })
    }
})
