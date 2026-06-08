import { type ProcessorConfig, ProcessorProvider } from '@/types/processor'
import {
    buildProcessorPrompt,
    loadProcessorJsonAsset,
    summarizePromptAssets,
} from '@/services/processor-prompt-asset-service'
import { BaseCompatibleModel } from '@/utils/base'
import { Logger } from '@idol-bbq-utils/log'
import { noop } from 'lodash'

const PROCESSOR_ERROR_FALLBACK = '╮(╯-╰)╭非常抱歉无法处理'

function resolveProcessorApiKey(apiKey: string) {
    const value = String(apiKey || '').trim()
    if (!value.startsWith('env:')) {
        return value
    }

    const envName = value.slice('env:'.length).trim()
    if (!envName) {
        throw new Error('Processor API key env var name is empty')
    }
    const resolved = process.env[envName]
    if (!resolved) {
        throw new Error(`Processor API key env var not set: ${envName}`)
    }
    return resolved.trim()
}

abstract class BaseProcessor extends BaseCompatibleModel {
    static _PROVIDER = ProcessorProvider.None
    protected abstract BASE_URL: string
    protected api_key: string
    log?: Logger
    config?: ProcessorConfig
    private promptCache?: string
    private outputSchemaCache?: Record<string, any> | null
    protected PROCESS_PROMPT = `现在你是一个翻译，接下来会给你日语或英语，请翻译以下日语或英语为简体中文，只输出译文，不要输出原文。如果是带有# hash tag的标签，不需要翻译。如果无法翻译请输出："${PROCESSOR_ERROR_FALLBACK}"`

    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super()
        this.api_key = api_key
        this.log = log
        this.config = config
    }

    async init(): Promise<void> {
        this.log = this.log?.child({ label: this.NAME, subservice: 'processor' })
        const prompt = this.getPrompt()
        this.log?.info(
            `loaded prompt chars=${prompt.length} assets=${JSON.stringify(summarizePromptAssets(this.config?.prompt_assets))}`,
        )
        this.log?.debug(
            `loaded with config ${JSON.stringify({
                ...this.config,
                prompt: this.config?.prompt ? `[inline:${this.config.prompt.length} chars]` : undefined,
            })}`,
        )
    }

    async drop(..._args: any[]): Promise<void> {
        noop()
    }

    public abstract process(text: string): Promise<string>

    protected getPrompt() {
        if (!this.promptCache) {
            this.promptCache = buildProcessorPrompt(
                this.config?.prompt || this.PROCESS_PROMPT,
                this.config?.prompt_assets,
            )
        }
        return this.promptCache
    }

    protected buildOpenAICompatibleRequestConfig(defaults: Record<string, unknown> = {}) {
        const payload: Record<string, unknown> = {
            ...defaults,
        }
        if (typeof this.config?.temperature === 'number') {
            payload.temperature = this.config.temperature
        }
        if (typeof this.config?.max_tokens === 'number') {
            payload.max_tokens = this.config.max_tokens
        }
        const outputSchema = this.getOutputSchema()
        if (outputSchema) {
            payload.response_format = {
                type: 'json_schema',
                json_schema: {
                    name: (this.NAME || 'processor').replace(/[^a-zA-Z0-9_-]+/g, '_').toLowerCase(),
                    schema: outputSchema,
                },
            }
        }
        return payload
    }

    protected getOutputSchema() {
        if (this.outputSchemaCache !== undefined) {
            return this.outputSchemaCache
        }
        this.outputSchemaCache = (this.config?.output_schema ||
            loadProcessorJsonAsset(this.config?.output_schema_file)) as Record<string, any> | null
        return this.outputSchemaCache
    }

    static isValidResult(text?: string | null): boolean {
        return Boolean(text) && text !== PROCESSOR_ERROR_FALLBACK
    }
}

export interface ProcessorPlugin {
    provider: ProcessorProvider
    aliases?: Array<string>
    create: (apiKey: string, log?: Logger, config?: ProcessorConfig) => BaseProcessor
}

class ProcessorRegistry {
    private static instance: ProcessorRegistry
    private plugins: Map<string, ProcessorPlugin> = new Map()

    private constructor() { }

    static getInstance(): ProcessorRegistry {
        if (!ProcessorRegistry.instance) {
            ProcessorRegistry.instance = new ProcessorRegistry()
        }
        return ProcessorRegistry.instance
    }

    register(plugin: ProcessorPlugin): this {
        const keys = [plugin.provider, ...(plugin.aliases || [])].map((value) => value.toLowerCase())
        for (const key of keys) {
            if (this.plugins.has(key)) {
                throw new Error(`Processor plugin ${plugin.provider} already registered for key ${key}`)
            }
        }
        for (const key of keys) {
            this.plugins.set(key, plugin)
        }
        return this
    }

    find(provider: ProcessorProvider | string): ProcessorPlugin | null {
        return this.plugins.get(provider.toLowerCase()) || null
    }

    async create(
        provider: ProcessorProvider | string,
        apiKey: string,
        log?: Logger,
        config?: ProcessorConfig,
    ): Promise<BaseProcessor> {
        const plugin = this.find(provider)
        if (!plugin) {
            throw new Error(`Unknown processor provider: ${provider}`)
        }

        const processor = plugin.create(resolveProcessorApiKey(apiKey), log, config)
        await processor.init()
        return processor
    }

    getRegisteredProviders(): string[] {
        return Array.from(this.plugins.keys())
    }
}

export { BaseProcessor, PROCESSOR_ERROR_FALLBACK, ProcessorRegistry, resolveProcessorApiKey }
